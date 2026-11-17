//! **Layer: Execution** — MongoDB source engine (document store).

use std::sync::Arc;

use arrow::array::{ArrayRef, StringBuilder};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures_util::TryStreamExt;
use mongodb::Client;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::{ClientOptions, CollectionOptions, ReadConcern};
use tokio::runtime::Runtime;

use crate::config::{MongoConfig, MongoJsonMode, MongoReadConcern, TlsConfig};
use crate::error::Result;
use crate::source::{BatchSink, ExportRequest, Source};
use crate::types::{ColumnOverrides, RivetType, SourceColumn, TypeMapping};

pub(crate) mod cdc;
pub(crate) mod column_grants;
pub(crate) mod id_types;

/// The byte budget at which a document batch flushes.
fn mongo_batch_byte_cap(max_batch_memory_mb: Option<usize>) -> usize {
    const DEFAULT: usize = 256 * 1024 * 1024;
    const MAX: usize = 1024 * 1024 * 1024;
    max_batch_memory_mb
        .map(|mb| mb.saturating_mul(1024 * 1024).min(MAX))
        .filter(|&b| b > 0)
        .unwrap_or(DEFAULT)
}

/// Map rivet's [`TlsConfig`] onto the mongodb driver's `Tls`, mirroring the SQL engines' posture.
fn tls_to_mongo(cfg: &TlsConfig) -> mongodb::options::Tls {
    use crate::config::TlsMode;
    use mongodb::options::{Tls, TlsOptions};
    if matches!(cfg.mode, TlsMode::Disable) {
        return Tls::Disabled;
    }
    let mode_invalid_cert = matches!(cfg.mode, TlsMode::Require);
    let mut o = TlsOptions::default();
    o.ca_file_path = cfg.ca_file.as_ref().map(std::path::PathBuf::from);
    o.allow_invalid_certificates = Some(mode_invalid_cert || cfg.accept_invalid_certs);
    Tls::Enabled(o)
}

/// A connected MongoDB session: the async client plus the tokio runtime that drives it, so the sync `Source` trait can `block_on` every driver call (ADR-0011, mirrors MSSQL/tiberius).
pub struct MongoSession {
    rt: Runtime,
    client: Client,
    /// Database resolved from the connection URL path (`mongodb://…/<db>`).
    db: String,
}

impl MongoSession {
    /// Connect + `ping`.
    pub fn connect(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        crate::source::require_tls_or_loopback(url, tls)?;
        let rt = crate::source::driver_runtime(
            "mongodb",
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build(),
        )?;
        let (client, db) = rt
            .block_on(async {
                let mut opts = ClientOptions::parse(url).await?;
                default_server_selection_timeout(&mut opts);
                if let Some(cfg) = tls {
                    opts.tls = Some(tls_to_mongo(cfg));
                }
                let db = opts.default_database.clone().ok_or_else(|| {
                    crate::error::coded(
                        crate::error::codes::CONFIG_SOURCE_URL_INVALID,
                        "mongodb url must include a database: mongodb://user:pass@host:port/<db>"
                            .to_string(),
                    )
                })?;
                let client = Client::with_options(opts)?;
                client.database(&db).run_command(doc! { "ping": 1 }).await?;
                Ok::<_, anyhow::Error>((client, db))
            })
            .map_err(|e| crate::source::describe_connect_error(url, e))?;
        Ok(Self { rt, client, db })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }
    pub fn db(&self) -> &str {
        &self.db
    }
    /// Drive a future to completion on the owned runtime.
    pub fn block_on<T>(&self, fut: impl std::future::Future<Output = T>) -> T {
        self.rt.block_on(fut)
    }
}

/// Cap server selection at 10 s unless the URL sets `serverSelectionTimeoutMS` (the driver's 30 s default made an unreachable or TLS-refusing server take ~90 s to fail).
fn default_server_selection_timeout(opts: &mut ClientOptions) {
    opts.server_selection_timeout
        .get_or_insert(std::time::Duration::from_secs(10));
}

/// Whether the server refused the command for want of the privilege (code 13, `Unauthorized`).
fn not_allowed(e: &mongodb::error::Error) -> bool {
    matches!(&*e.kind, mongodb::error::ErrorKind::Command(c) if c.code == 13)
}

/// The UUIDs (empty for a view) the database lists under `collection`.
fn list_collection_uuids(session: &MongoSession, collection: &str) -> Result<Option<Vec<Vec<u8>>>> {
    let listed = session.block_on(async {
        let mut cursor = session
            .client()
            .database(session.db())
            .list_collections()
            .filter(doc! { "name": collection })
            .await?
            .with_type::<Document>();
        let mut rows = Vec::new();
        while let Some(row) = cursor.try_next().await? {
            rows.push(uuid_of_listing(&row));
        }
        Ok::<_, mongodb::error::Error>(rows)
    });
    match listed {
        Ok(uuids) => Ok(Some(uuids)),
        Err(e) if seeded!("mongo-listing-failure-fails-open", true, not_allowed(&e)) => Ok(None),
        Err(e) => Err(anyhow::Error::new(e).context(format!(
            "listing MongoDB collection `{}.{collection}` failed — the run stops rather than \
             read a collection it cannot confirm is the one it started on; run again once the \
             server answers",
            session.db()
        ))),
    }
}

/// The UUID bytes a `listCollections` row carries in `info.uuid`; empty for a view, which lists none.
fn uuid_of_listing(listed: &Document) -> Vec<u8> {
    match listed.get_document("info").ok().and_then(|i| i.get("uuid")) {
        Some(Bson::Binary(b)) => b.bytes.clone(),
        _ => Vec::new(),
    }
}

/// The bytes `text` spells in hex; `None` for anything else.
fn bytes_of_hex(text: &str) -> Option<Vec<u8>> {
    let digits = text.as_bytes();
    digits.len().is_multiple_of(2).then_some(())?;
    digits
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
        .collect()
}

/// Refuse a collection the database does not list, or lists under another UUID than the run `started` on.
fn refuse_a_missing_collection(
    db: &str,
    collection: &str,
    listed: Option<&[Vec<u8>]>,
    started: Option<&[u8]>,
) -> Result<()> {
    let Some(listed) = listed else {
        return Ok(());
    };
    match (listed.first(), started) {
        (None, None) => crate::rivet_bail!(
            crate::error::codes::SOURCE_COLLECTION_NOT_FOUND,
            "MongoDB collection `{db}.{collection}` does not exist — an export of it would \
             succeed with 0 rows and a full load would then empty the warehouse table. Check \
             the `table:` name (a dotted collection name is written whole)."
        ),
        (None, Some(_)) => crate::rivet_bail!(
            crate::error::codes::SOURCE_COLLECTION_NOT_FOUND,
            "MongoDB collection `{db}.{collection}` was dropped while the run read it — the \
             documents read so far are not the collection, and a run that ended here would be \
             marked complete. Create the collection again with its documents, then run again."
        ),
        (Some(now), Some(then)) if now.as_slice() != then => crate::rivet_bail!(
            crate::error::codes::SOURCE_COLLECTION_NOT_FOUND,
            "MongoDB collection `{db}.{collection}` was dropped and created again while the \
             run read it — the documents read so far are not the collection it holds now. Run \
             again."
        ),
        _ => Ok(()),
    }
}

impl MongoSession {
    /// Refuse a collection the database does not list, by the rule [`MongoSource::export`] refuses it by.
    pub(crate) fn require_collection(&self, collection: &str) -> Result<()> {
        let listed = list_collection_uuids(self, collection)?;
        refuse_a_missing_collection(self.db(), collection, listed.as_deref(), None)
    }

    /// The incarnation of `collection` as the database lists it now.
    pub(crate) fn collection_incarnation(&self, collection: &str) -> Result<Option<String>> {
        let listed = list_collection_uuids(self, collection)?;
        Ok(listed
            .as_deref()
            .and_then(<[_]>::first)
            .filter(|uuid| !uuid.is_empty())
            .map(|uuid| crate::types::hex_bytes(uuid)))
    }
}

/// MongoDB source over a [`MongoSession`], carrying the resolved `source.mongo:` read options `export` applies.
pub struct MongoSource {
    session: MongoSession,
    /// Render the `document` column as canonical (type-tagged) extended JSON.
    canonical_json: bool,
    /// Scan under `readConcern: snapshot` (point-in-time; 5.0+ replica set).
    snapshot: bool,
    /// Set `noCursorTimeout` on the scan cursor.
    no_cursor_timeout: bool,
    /// A disjoint `_id` slice `[lo, hi)` for ONE parallel worker.
    id_range: Option<(Bson, Bson)>,
    /// Collections already vetted by [`Self::ensure_uniform_id_type`] this run.
    id_guard_checked: std::collections::HashSet<String>,
    /// The UUID each collection was listed under when this source first read it.
    started_as: std::collections::HashMap<String, Vec<u8>>,
    /// The collections whose last listing this source confirmed and no read has used since.
    confirmed: std::collections::HashSet<String>,
}

impl MongoSource {
    /// Connect, resolving the optional `source.mongo:` read options (absent ⇒ relaxed JSON, server read concern, cursor kept alive).
    pub fn connect(url: &str, tls: Option<&TlsConfig>, cfg: Option<&MongoConfig>) -> Result<Self> {
        let session = MongoSession::connect(url, tls)?;
        let (canonical_json, snapshot, no_cursor_timeout) = match cfg {
            None => (false, false, true),
            Some(c) => (
                matches!(c.json, MongoJsonMode::Canonical),
                matches!(c.read_concern, MongoReadConcern::Snapshot),
                c.no_cursor_timeout,
            ),
        };
        Ok(Self {
            session,
            canonical_json,
            snapshot,
            no_cursor_timeout,
            id_range: None,
            id_guard_checked: std::collections::HashSet::new(),
            started_as: std::collections::HashMap::new(),
            confirmed: std::collections::HashSet::new(),
        })
    }

    /// Refuse `collection` when it is gone or listed under another UUID than this source first saw.
    pub(crate) fn confirm_collection(&mut self, collection: &str) -> Result<()> {
        let listed = list_collection_uuids(&self.session, collection)?;
        let refusal = refuse_a_missing_collection(
            self.session.db(),
            collection,
            listed.as_deref(),
            seeded!(
                "mongo-collection-uuid-not-compared",
                None,
                self.started_as.get(collection).map(Vec::as_slice)
            ),
        );
        seeded!(
            "collection-refusal-uncoded-on-run",
            refusal.map_err(|e| anyhow::Error::msg(e.to_string())),
            refusal
        )?;
        if let Some(uuid) = listed.as_deref().and_then(<[_]>::first) {
            self.started_as
                .entry(collection.to_string())
                .or_insert_with(|| uuid.clone());
        }
        self.confirmed.insert(collection.to_string());
        Ok(())
    }

    /// Confirm `collection` before a read, unless the listing that ended the last read already did.
    fn confirm_before_read(&mut self, collection: &str) -> Result<()> {
        if self.confirmed.remove(collection) {
            return Ok(());
        }
        self.confirm_collection(collection)
    }

    /// Read `collection` only as the incarnation (UUID in hex) a stored cursor was written against.
    pub(crate) fn pin_incarnation(&mut self, collection: &str, incarnation: &str) {
        if let Some(uuid) = bytes_of_hex(incarnation) {
            self.started_as
                .entry(collection.to_string())
                .or_insert(uuid);
        }
    }

    /// The UUIDs this source has pinned, by collection.
    pub(crate) fn pinned(&self) -> std::collections::HashMap<String, Vec<u8>> {
        self.started_as.clone()
    }

    /// Read under the UUIDs another source pinned, so a worker never pins a collection created after its run began.
    pub(crate) fn with_pinned(
        mut self,
        pinned: std::collections::HashMap<String, Vec<u8>>,
    ) -> Self {
        self.started_as = pinned;
        self
    }

    /// Bind this reader to one disjoint `_id` slice `[lo, hi)` (a parallel worker).
    pub fn with_id_range(mut self, lo: Bson, hi: Bson) -> Self {
        self.id_range = Some((lo, hi));
        self
    }

    /// The min and max `_id` of `collection` by BSON sort order, or `None` if it is empty.
    fn id_span(&self, collection: &str) -> Result<Option<(Document, Document)>> {
        id_types::id_span_on(&self.session, collection)
    }

    /// Guard against silent loss on a heterogeneous-`_id` collection.
    pub(crate) fn ensure_uniform_id_type(&self, collection: &str) -> Result<()> {
        if let Some((lo, hi)) = self.id_span(collection)?
            && let (Some(lo_id), Some(hi_id)) = (lo.get("_id"), hi.get("_id"))
        {
            seekable_id_span(collection, lo_id, hi_id)?;
        }
        Ok(())
    }

    /// Advisory sibling of [`Self::ensure_uniform_id_type`] for the **full-scan** path (no `page_size`, no `parallel`).
    pub(crate) fn warn_if_heterogeneous_id(&self, collection: &str) {
        let Ok(Some((lo, hi))) = self.id_span(collection) else {
            return;
        };
        if let (Some(lo_id), Some(hi_id)) = (lo.get("_id"), hi.get("_id"))
            && id_types_collide(lo_id, hi_id)
        {
            log::warn!(
                "collection '{collection}' has heterogeneous `_id` types ({:?} … {:?}): {}",
                lo_id.element_type(),
                hi_id.element_type(),
                hetero_id_guidance()
            );
        }
    }

    /// Split the collection's `_id` space into `n` disjoint, size-balanced ranges `[lo, hi)` whose union tiles the whole BSON key space (so the union read is the complete collection, no overlap).
    pub fn sample_id_ranges(&self, collection: &str, n: usize) -> Result<Vec<(Bson, Bson)>> {
        self.ensure_uniform_id_type(collection)?;
        let n = n.max(1);
        let full_range = || vec![(Bson::MinKey, Bson::MaxKey)];
        if n == 1 {
            return Ok(full_range());
        }
        let sample_size = (n * 250).clamp(2000, 50_000);
        let coll = self
            .session
            .client()
            .database(self.session.db())
            .collection::<Document>(collection);
        let ids: Vec<Bson> = self.session.block_on(async move {
            let mut cursor = coll
                .aggregate(vec![
                    doc! { "$sample": { "size": sample_size as i64 } },
                    doc! { "$sort": { "_id": 1 } },
                    doc! { "$project": { "_id": 1 } },
                ])
                .await?;
            let mut ids = Vec::new();
            while let Some(d) = cursor.try_next().await? {
                if let Some(id) = d.get("_id") {
                    ids.push(id.clone());
                }
            }
            Ok::<_, anyhow::Error>(ids)
        })?;
        if ids.len() < 2 {
            return Ok(full_range());
        }
        let mut bounds = vec![Bson::MinKey];
        for i in 1..n {
            bounds.push(ids[i * ids.len() / n].clone());
        }
        bounds.push(Bson::MaxKey);
        Ok(bounds
            .windows(2)
            .filter(|w| w[0] != w[1])
            .map(|w| (w[0].clone(), w[1].clone()))
            .collect())
    }
}

/// The two fixed columns of the JSON-blob model, each retyped by its `columns:` override where the override table reads that pair.
fn blob_mappings(overrides: &ColumnOverrides) -> Result<Vec<TypeMapping>> {
    [
        ("_id", "objectid", false, RivetType::String),
        ("document", "document", true, RivetType::Json),
    ]
    .into_iter()
    .map(|(name, native, nullable, detected)| {
        let t = crate::types::override_table::resolve_column(
            crate::types::override_table::Engine::Mongo,
            overrides,
            name,
            native,
            || detected,
        )?;
        Ok(TypeMapping::from_source(
            &SourceColumn::simple(name, native, nullable),
            t,
        ))
    })
    .collect()
}

/// Arrow schema for the JSON-blob model (`_id: Utf8`, `document: Utf8 + json`) under `overrides`.
fn blob_schema(overrides: &ColumnOverrides) -> Result<SchemaRef> {
    let fields = blob_mappings(overrides)?
        .iter()
        .map(|m| {
            crate::types::build_arrow_field(m)
                .expect("blob columns (String/Json) always have an Arrow mapping")
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

/// Stringify a document `_id` for the `_id` column.
fn id_to_string(id: Option<&Bson>) -> String {
    match id {
        Some(Bson::ObjectId(oid)) => oid.to_hex(),
        Some(Bson::String(s)) => s.clone(),
        Some(other) => other.clone().into_relaxed_extjson().to_string(),
        None => String::new(),
    }
}

/// Encode a BSON `_id` as a lossless, engine-decodable keyset token.
fn seekable_id_span(collection: &str, lo_id: &Bson, hi_id: &Bson) -> Result<()> {
    for id in [lo_id, hi_id] {
        if is_id_nan(id) {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_KEYSET_KEY_UNORDERED,
                "collection '{collection}' has a NaN `_id`: MongoDB range \
                 operators never match NaN, so keyset paging / parallel \
                 ranges would silently skip documents. Remove `page_size`, \
                 `resume` and `parallel` to use a full ordered scan (a single \
                 cursor reads every document), or fix the `_id`."
            );
        }
    }
    if id_types_collide(lo_id, hi_id) {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_KEYSET_KEY_UNORDERED,
            "collection '{collection}' has heterogeneous `_id` types \
             ({:?} … {:?}): MongoDB keyset paging seeks with `$gt`, which is \
             BSON-type-bracketed and would silently skip every `_id` type but \
             one. Remove `page_size`, `resume` and `parallel` to use a full ordered \
             scan (a single cursor crosses BSON types), or normalise `_id` to one type.",
            lo_id.element_type(),
            hi_id.element_type(),
        );
    }
    Ok(())
}

fn is_id_nan(id: &Bson) -> bool {
    match id {
        Bson::Double(f) => f.is_nan(),
        Bson::Decimal128(d) => d.to_string().to_ascii_lowercase().contains("nan"),
        _ => false,
    }
}

fn id_bracket(v: &Bson) -> Option<u8> {
    Some(match v {
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_) => 0,
        Bson::Null => 1,
        Bson::String(_) | Bson::Symbol(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::ObjectId(_) => 6,
        Bson::Boolean(_) => 7,
        Bson::DateTime(_) => 8,
        Bson::Timestamp(_) => 9,
        Bson::RegularExpression(_) => 10,
        _ => return None,
    })
}

/// Whether two `_id` values fall in DIFFERENT keyset brackets — i.e.
fn id_types_collide(lo: &Bson, hi: &Bson) -> bool {
    match (id_bracket(lo), id_bracket(hi)) {
        (Some(a), Some(b)) => a != b,
        _ => true,
    }
}

/// The shared tail of the heterogeneous-`_id` warning (batch full scan and CDC both emit it).
pub(super) fn hetero_id_guidance() -> &'static str {
    "the flat `_id` column renders different BSON types to possibly-colliding text \
     (int 1001 and string \"1001\" both become \"1001\"), so a merge keyed on `_id` \
     conflates distinct documents. In YOUR OWN queries over the change log, key on the \
     typed `document._id` (see docs/reference/mongodb.md#consuming-in-the-warehouse). \
     RIVET'S OWN compaction cannot: `load.pk` takes plain identifiers only \
     ([A-Za-z_][A-Za-z0-9_]*), so `document._id` is refused both when the plan is built \
     and again at load time, and a `layout: base_buffer` load of this collection would \
     MERGE on the lossy flat `_id` and then drop the buffer that distinguished the \
     documents. Until the `_id` types are uniform, load this collection with \
     `mode: full` — a whole-table pass runs no MERGE at all"
}

fn encode_id_cursor(id: &Bson) -> String {
    let mut buf = Vec::new();
    doc! { "_id": id.clone() }
        .to_writer(&mut buf)
        .expect("a one-field BSON document always serializes");
    bytes_to_hex(&buf)
}

/// Lower-case hex of raw bytes — the string half of the BSON-token round-trip (paired with [`hex_to_bytes`]), used to carry a lossless BSON value across a string-typed seam (the keyset cursor and the CDC resume token).
pub(super) fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Inverse of [`encode_id_cursor`].
fn decode_id_cursor(token: &str) -> Result<Bson> {
    if let Ok(bytes) = hex_to_bytes(token)
        && let Ok(doc) = Document::from_reader(&bytes[..])
        && let Some(id) = doc.get("_id")
    {
        return Ok(id.clone());
    }
    ObjectId::parse_str(token).map(Bson::ObjectId).map_err(|_| {
        crate::error::coded(
            crate::error::codes::STATE_CURSOR_INCOMPARABLE,
            format!("MongoDB keyset cursor '{token}' is neither a typed token nor ObjectId hex"),
        )
    })
}

fn hex_to_bytes(s: &str) -> Result<Vec<u8>> {
    if !s.is_ascii() {
        crate::rivet_bail!(
            crate::error::codes::STATE_CURSOR_INCOMPARABLE,
            "invalid hex cursor token (non-ASCII)"
        );
    }
    if !s.len().is_multiple_of(2) {
        crate::rivet_bail!(
            crate::error::codes::STATE_CURSOR_INCOMPARABLE,
            "odd-length hex cursor token"
        );
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| {
                crate::error::coded(
                    crate::error::codes::STATE_CURSOR_INCOMPARABLE,
                    e.to_string(),
                )
            })
        })
        .collect()
}

/// Render a whole BSON document as an extended-JSON string.
fn document_to_json(doc: &Document, canonical: bool) -> Result<String> {
    let bson = Bson::Document(doc.clone());
    let value = if canonical {
        bson.into_canonical_extjson()
    } else {
        bson.into_relaxed_extjson()
    };
    Ok(serde_json::to_string(&value)?)
}

/// Finish the accumulated builders into a `RecordBatch` and hand it to the sink.
fn flush(
    schema: &SchemaRef,
    ids: &mut StringBuilder,
    docs: &mut StringBuilder,
    sink: &mut dyn BatchSink,
) -> Result<()> {
    let columns: Vec<ArrayRef> = vec![Arc::new(ids.finish()), Arc::new(docs.finish())];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    sink.on_batch(&batch)?;
    Ok(())
}

impl Source for MongoSource {
    fn pin_incarnation(&mut self, collection: &str, incarnation: &str) {
        MongoSource::pin_incarnation(self, collection, incarnation);
    }

    fn readable_columns(
        &mut self,
        table: &str,
    ) -> Result<Option<crate::source::cdc::ReadableColumns>> {
        column_grants::readable_columns_for_batch(&self.session, table).map(Some)
    }

    fn harm_counters(&mut self) -> Option<Vec<(String, i64)>> {
        let session = &self.session;
        session.block_on(async {
            let status = session
                .client()
                .database(session.db())
                .run_command(doc! { "serverStatus": 1 })
                .await
                .ok()?;
            Some(harm_from_server_status(&status))
        })
    }

    fn export(&mut self, request: &ExportRequest<'_>, sink: &mut dyn BatchSink) -> Result<()> {
        let coll_name = request.base_relation.ok_or_else(|| {
            crate::error::coded(
                crate::error::codes::CONFIG_SOURCE_OPTION_UNSUPPORTED,
                format!(
                    "MongoDB source supports only `table: <collection>` with `mode: full` — a \
                 hand-written `query:` or a filtered/wrapped form has no MongoDB equivalent. \
                 Got query: {}",
                    request.query
                ),
            )
        })?;
        let db = self.session.client().database(self.session.db());
        self.confirm_before_read(coll_name)?;
        let schema = blob_schema(request.column_overrides)?;
        sink.on_schema(schema.clone())?;

        let coll = if self.snapshot {
            db.collection_with_options::<Document>(
                coll_name,
                CollectionOptions::builder()
                    .read_concern(ReadConcern::snapshot())
                    .build(),
            )
        } else {
            db.collection::<Document>(coll_name)
        };
        let canonical = self.canonical_json;
        let no_cursor_timeout = self.no_cursor_timeout
            && seeded!(
                "mongo-view-read-sets-no-cursor-timeout",
                true,
                !self.started_as.get(coll_name).is_some_and(Vec::is_empty)
            );
        let id_range = self.id_range.clone();
        let batch_rows = request.tuning.effective_batch_size(Some(&schema)).max(1);
        let batch_byte_cap = mongo_batch_byte_cap(request.tuning.max_batch_memory_mb);

        let page_limit = request.page_limit;
        let after_id: Option<Bson> =
            match request.cursor.and_then(|c| c.last_cursor_value.as_deref()) {
                Some(token) => Some(decode_id_cursor(token)?),
                None => None,
            };

        if page_limit.is_some() && id_range.is_none() && !self.id_guard_checked.contains(coll_name)
        {
            self.ensure_uniform_id_type(coll_name)?;
            self.id_guard_checked.insert(coll_name.to_string());
        }

        if page_limit.is_none() && id_range.is_none() {
            self.warn_if_heterogeneous_id(coll_name);
        }

        let total = self.session.block_on(async {
            let filter = {
                let mut cond = Document::new();
                match &after_id {
                    Some(id) => {
                        cond.insert("$gt", id.clone());
                    }
                    None => {
                        if let Some((lo, _)) = &id_range {
                            cond.insert("$gte", lo.clone());
                        }
                    }
                }
                if let Some((_, hi)) = &id_range {
                    cond.insert("$lt", hi.clone());
                }
                if cond.is_empty() {
                    doc! {}
                } else {
                    doc! { "_id": cond }
                }
            };
            let mut find = coll.find(filter);
            if let Some(page) = page_limit {
                find = find.sort(doc! { "_id": 1 }).limit(page as i64);
            }
            if no_cursor_timeout {
                find = find.no_cursor_timeout(true);
            }
            let mut cursor = find.await?;
            let mut ids = StringBuilder::new();
            let mut docs = StringBuilder::new();
            let mut in_batch = 0usize;
            let mut batch_bytes = 0usize;
            let mut total = 0usize;
            let mut max_id: Option<Bson> = None;
            while let Some(d) = cursor.try_next().await? {
                let id = id_to_string(d.get("_id"));
                let json = document_to_json(&d, canonical)?;
                batch_bytes += id.len() + json.len();
                ids.append_value(&id);
                docs.append_value(&json);
                if page_limit.is_some() {
                    max_id = d.get("_id").cloned();
                }
                in_batch += 1;
                total += 1;
                if in_batch >= batch_rows || batch_bytes >= batch_byte_cap {
                    flush(&schema, &mut ids, &mut docs, sink)?;
                    in_batch = 0;
                    batch_bytes = 0;
                }
            }
            if in_batch > 0 {
                flush(&schema, &mut ids, &mut docs, sink)?;
            }
            if let Some(id) = &max_id {
                sink.set_source_cursor(encode_id_cursor(id));
            }
            Ok::<usize, anyhow::Error>(total)
        })?;

        seeded!(
            "mongo-empty-read-after-drop",
            Ok(()),
            self.confirm_collection(coll_name)
        )?;

        log::info!("total: {total} documents (collection '{coll_name}')");
        Ok(())
    }

    /// The JSON-blob model has a fixed two-column schema regardless of the query, so `check --type-report` shows it without touching the server.
    fn type_mappings(
        &mut self,
        _query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>> {
        blob_mappings(column_overrides)
    }

    fn primary_key(&mut self, _table: &str) -> Result<Option<Vec<String>>> {
        Ok(Some(vec!["_id".to_string()]))
    }

    /// The only scalar rivet asks a `mode: full` source is the reconcile row count.
    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        if !sql.to_ascii_lowercase().contains("count(") {
            return Ok(None);
        }
        let Some(coll_name) = last_from_identifier(sql) else {
            return Ok(None);
        };
        let coll = self
            .session
            .client()
            .database(self.session.db())
            .collection::<Document>(coll_name);
        let n = self
            .session
            .block_on(async move { coll.count_documents(doc! {}).await })?;
        Ok(Some(n.to_string()))
    }

    fn server_context(&mut self) -> Option<String> {
        let session = &self.session;
        let (version, current, available, uptime) = session.block_on(async {
            let db = session.client().database(session.db());
            let build = db.run_command(doc! { "buildInfo": 1 }).await.ok();
            let status = db.run_command(doc! { "serverStatus": 1 }).await.ok();
            let version = build
                .as_ref()
                .and_then(|d| d.get_str("version").ok().map(str::to_string));
            let current = status
                .as_ref()
                .and_then(|d| nested_i64(d, &["connections", "current"]));
            let available = status
                .as_ref()
                .and_then(|d| nested_i64(d, &["connections", "available"]));
            let uptime = status.as_ref().and_then(|d| nested_i64(d, &["uptime"]));
            (version, current, available, uptime)
        });
        Some(
            serde_json::json!({
                "engine": "mongodb",
                "version": version,
                "connections_current": current,
                "connections_available": available,
                "uptime_s": uptime,
            })
            .to_string(),
        )
    }
}

/// Pull the collection name out of the innermost `… FROM <ident>` of a SQL string.
fn last_from_identifier(sql: &str) -> Option<&str> {
    const MARKER: &str = "from ";
    let start = sql.to_ascii_lowercase().rfind(MARKER)? + MARKER.len();
    let ident = sql[start..]
        .trim_start()
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
        .next()?;
    (!ident.is_empty()).then_some(ident)
}

/// Read a possibly-nested numeric field out of a `serverStatus` document, coercing Int32 / Int64 / Double to `i64`.
fn nested_i64(doc: &Document, path: &[&str]) -> Option<i64> {
    let (leaf, parents) = path.split_last()?;
    let mut cur = doc;
    for key in parents {
        cur = cur.get_document(key).ok()?;
    }
    match cur.get(leaf) {
        Some(Bson::Int64(v)) => Some(*v),
        Some(Bson::Int32(v)) => Some(*v as i64),
        Some(Bson::Double(v)) => Some(*v as i64),
        _ => None,
    }
}

/// Pull the source-harm counters out of a `serverStatus` response.
fn harm_from_server_status(status: &Document) -> Vec<(String, i64)> {
    let probes: [(&str, &[&str]); 6] = [
        (
            "mongo_docs_scanned",
            &["metrics", "queryExecutor", "scannedObjects"],
        ),
        (
            "mongo_keys_scanned",
            &["metrics", "queryExecutor", "scanned"],
        ),
        ("mongo_docs_returned", &["metrics", "document", "returned"]),
        ("mongo_op_query", &["opcounters", "query"]),
        ("mongo_op_getmore", &["opcounters", "getmore"]),
        (
            "mongo_wt_cache_bytes_read",
            &["wiredTiger", "cache", "bytes read into cache"],
        ),
    ];
    probes
        .iter()
        .filter_map(|(name, path)| nested_i64(status, path).map(|v| ((*name).to_string(), v)))
        .collect()
}

/// Snapshot MongoDB's source-harm counters via `serverStatus`.
pub(crate) fn max_connections(url: &str, tls: Option<&TlsConfig>) -> Option<u32> {
    let session = MongoSession::connect(url, tls).ok()?;
    session.block_on(async {
        let status = session
            .client()
            .database("admin")
            .run_command(mongodb::bson::doc! { "serverStatus": 1 })
            .await
            .ok()?;
        let conns = status.get_document("connections").ok()?;
        let avail = conns
            .get_i32("available")
            .ok()
            .map(|v| v as i64)
            .or_else(|| conns.get_i64("available").ok())?;
        u32::try_from(avail).ok()
    })
}

pub(crate) fn estimated_count(url: &str, tls: Option<&TlsConfig>, collection: &str) -> Option<i64> {
    let session = MongoSession::connect(url, tls).ok()?;
    session.block_on(async {
        let n = session
            .client()
            .database(session.db())
            .collection::<Document>(collection)
            .estimated_document_count()
            .await
            .ok()?;
        i64::try_from(n).ok()
    })
}

#[cfg(test)]
mod fake_server;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_of_hex_reads_hex_pairs_and_nothing_else() {
        assert_eq!(bytes_of_hex("ab01"), Some(vec![0xab, 0x01]));
        assert_eq!(bytes_of_hex("AB"), Some(vec![0xab]));
        assert_eq!(bytes_of_hex(""), Some(Vec::new()));
        assert_eq!(bytes_of_hex("abc"), None);
        assert_eq!(bytes_of_hex("zz"), None);
        assert_eq!(bytes_of_hex("a\u{e9}b"), None);
        assert_eq!(bytes_of_hex("\u{e9}12"), None);
    }

    #[test]
    fn server_selection_defaults_to_ten_seconds_but_the_url_wins() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let sel = |url: &str| {
            let mut o = rt
                .block_on(async { ClientOptions::parse(url).await })
                .unwrap();
            default_server_selection_timeout(&mut o);
            o.server_selection_timeout
        };
        assert_eq!(
            sel("mongodb://h/db"),
            Some(std::time::Duration::from_secs(10))
        );
        assert_eq!(
            sel("mongodb://h/db?serverSelectionTimeoutMS=45000"),
            Some(std::time::Duration::from_secs(45))
        );
    }

    #[test]
    fn only_a_collection_the_database_does_not_list_is_refused() {
        let refusal = refuse_a_missing_collection("rivet", "orderz", Some(&[]), None)
            .expect_err("an empty listing for the exact name means the collection is missing");
        assert_eq!(
            crate::error::error_code(&refusal),
            Some("RIVET_SOURCE_COLLECTION_NOT_FOUND")
        );
        assert!(
            format!("{refusal:#}").contains("`rivet.orderz` does not exist"),
            "{refusal:#}"
        );
        assert!(refuse_a_missing_collection("rivet", "orders", Some(&[vec![1]]), None).is_ok());
        assert!(
            refuse_a_missing_collection("rivet", "orders", None, None).is_ok(),
            "a role without listCollections is not evidence of absence"
        );
    }

    #[test]
    fn a_listing_row_carries_its_uuid_and_a_view_has_none() {
        use mongodb::bson::{Binary, spec::BinarySubtype};
        let uuid = Binary {
            subtype: BinarySubtype::Uuid,
            bytes: vec![7, 8, 9],
        };
        assert_eq!(
            uuid_of_listing(&doc! { "name": "t", "info": { "uuid": uuid } }),
            vec![7, 8, 9]
        );
        assert_eq!(
            uuid_of_listing(&doc! { "name": "v", "info": { "readOnly": true } }),
            Vec::<u8>::new(),
            "a view lists no uuid"
        );
        assert_eq!(uuid_of_listing(&doc! { "name": "t" }), Vec::<u8>::new());
    }

    #[test]
    fn a_collection_gone_or_replaced_since_the_run_started_is_refused_by_code() {
        let started: &[u8] = &[1, 2];
        let dropped = refuse_a_missing_collection("rivet", "orders", Some(&[]), Some(started))
            .expect_err("a collection the run started on, now unlisted, was dropped");
        assert_eq!(
            crate::error::error_code(&dropped),
            Some("RIVET_SOURCE_COLLECTION_NOT_FOUND")
        );
        assert!(
            format!("{dropped:#}").contains("`rivet.orders` was dropped while the run read it"),
            "{dropped:#}"
        );
        let replaced =
            refuse_a_missing_collection("rivet", "orders", Some(&[vec![3, 4]]), Some(started))
                .expect_err("another UUID under the same name is another collection");
        assert_eq!(
            crate::error::error_code(&replaced),
            Some("RIVET_SOURCE_COLLECTION_NOT_FOUND")
        );
        assert!(
            format!("{replaced:#}")
                .contains("`rivet.orders` was dropped and created again while the run read it"),
            "{replaced:#}"
        );
        assert!(
            refuse_a_missing_collection("rivet", "orders", Some(&[vec![1, 2]]), Some(started))
                .is_ok(),
            "the same UUID is the same collection"
        );
        assert!(
            refuse_a_missing_collection("rivet", "orders", None, Some(started)).is_ok(),
            "a listing that was not allowed refuses nothing, started or not"
        );
        assert!(
            refuse_a_missing_collection("rivet", "v", Some(&[vec![]]), Some(&[])).is_ok(),
            "a view has no UUID and is the same view"
        );
    }
    use mongodb::bson::oid::ObjectId;

    #[test]
    fn batch_byte_cap_is_exact_mib() {
        assert_eq!(super::mongo_batch_byte_cap(Some(10)), 10 * 1024 * 1024);
        assert_eq!(super::mongo_batch_byte_cap(None), 256 * 1024 * 1024);
        assert_eq!(super::mongo_batch_byte_cap(Some(0)), 256 * 1024 * 1024);
    }

    #[test]
    fn batch_byte_cap_is_clamped_below_the_i32_offset_ceiling() {
        let ceiling = i32::MAX as usize;
        for mb in [2048usize, 4096, 65_536, usize::MAX / (1024 * 1024)] {
            let cap = super::mongo_batch_byte_cap(Some(mb));
            assert!(
                cap < ceiling,
                "max_batch_memory_mb={mb} yields cap {cap} >= i32::MAX ({ceiling}) — would overflow the Utf8 offset builder"
            );
        }
        assert_eq!(super::mongo_batch_byte_cap(Some(256)), 256 * 1024 * 1024);
    }

    #[test]
    fn nan_id_detected_for_double_and_decimal128() {
        use mongodb::bson::Decimal128;
        assert!(is_id_nan(&Bson::Double(f64::NAN)));
        let dec_nan: Decimal128 = "NaN".parse().expect("bson parses NumberDecimal NaN");
        assert!(
            is_id_nan(&Bson::Decimal128(dec_nan)),
            "a Decimal128 NaN _id must be caught, not silently keyset-dropped"
        );
        assert!(!is_id_nan(&Bson::Double(1.5)));
        assert!(!is_id_nan(&Bson::Int64(1001)));
        let dec_finite: Decimal128 = "1234.5".parse().unwrap();
        assert!(!is_id_nan(&Bson::Decimal128(dec_finite)));
    }

    #[test]
    fn id_bracket_map_matches_the_server_sort_bands() {
        use mongodb::bson::spec::BinarySubtype;
        use mongodb::bson::{Binary, DateTime, Decimal128, Regex, Timestamp, doc};
        let b = |v: &Bson| id_bracket(v);

        assert_eq!(b(&Bson::Int32(1)), Some(0));
        assert_eq!(b(&Bson::Int64(1)), Some(0));
        assert_eq!(b(&Bson::Double(1.0)), Some(0));
        assert_eq!(
            b(&Bson::Decimal128("1".parse::<Decimal128>().unwrap())),
            Some(0)
        );
        assert_eq!(b(&Bson::Null), Some(1));
        assert_eq!(b(&Bson::String("s".into())), Some(2));
        assert_eq!(b(&Bson::Document(doc! {"k": 1})), Some(3));
        assert_eq!(b(&Bson::Array(vec![Bson::Int32(1)])), Some(4));
        assert_eq!(
            b(&Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![1],
            })),
            Some(5)
        );
        assert_eq!(b(&Bson::ObjectId(ObjectId::new())), Some(6));
        assert_eq!(b(&Bson::Boolean(true)), Some(7));
        assert_eq!(b(&Bson::DateTime(DateTime::from_millis(0))), Some(8));
        assert_eq!(
            b(&Bson::Timestamp(Timestamp {
                time: 0,
                increment: 0
            })),
            Some(9)
        );
        assert_eq!(
            b(&Bson::RegularExpression(Regex {
                pattern: "a".into(),
                options: String::new(),
            })),
            Some(10)
        );
        assert_eq!(b(&Bson::MaxKey), None);

        assert!(!id_types_collide(&Bson::Int32(1), &Bson::Double(2.0)));
        assert!(id_types_collide(
            &Bson::Int64(2000),
            &Bson::String("id-1".into())
        ));
        assert!(id_types_collide(&Bson::Null, &Bson::Document(doc! {})));
        assert!(id_types_collide(&Bson::MaxKey, &Bson::MaxKey));
    }

    #[test]
    fn id_string_covers_common_key_types() {
        let oid = ObjectId::parse_str("64b8f0000000000000000000").unwrap();
        assert_eq!(
            id_to_string(Some(&Bson::ObjectId(oid))),
            "64b8f0000000000000000000"
        );
        assert_eq!(id_to_string(Some(&Bson::String("abc".to_string()))), "abc");
        assert_eq!(id_to_string(Some(&Bson::Int64(42))), "42");
        assert_eq!(id_to_string(Some(&Bson::Int32(7))), "7");
        assert_eq!(id_to_string(None), "");
    }

    #[test]
    fn heterogeneous_id_detection_is_bracket_based() {
        let int1001 = Bson::Int32(1001);
        let str1001 = Bson::String("1001".to_string());
        let oid = Bson::ObjectId(ObjectId::parse_str("64b8f0000000000000000000").unwrap());
        assert!(id_types_collide(&int1001, &str1001));
        assert!(id_types_collide(&oid, &str1001));
        assert!(!id_types_collide(&int1001, &Bson::Int32(2002)));
        assert!(!id_types_collide(&int1001, &Bson::Int64(9_000_000_000)));
        assert!(!id_types_collide(&oid, &oid));
        assert!(!id_types_collide(
            &str1001,
            &Bson::String("abc".to_string())
        ));
    }

    #[test]
    fn roast_tls_config_is_honored_not_ignored() {
        use crate::config::{TlsConfig, TlsMode};
        use mongodb::options::Tls;
        let cfg = |mode, ca: Option<&str>| TlsConfig {
            mode,
            ca_file: ca.map(String::from),
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
        };
        match tls_to_mongo(&cfg(TlsMode::VerifyFull, Some("/ca.pem"))) {
            Tls::Enabled(o) => {
                assert_eq!(
                    o.ca_file_path.as_deref(),
                    Some(std::path::Path::new("/ca.pem"))
                );
                assert_eq!(o.allow_invalid_certificates, Some(false));
            }
            Tls::Disabled => panic!("verify-full must ENABLE tls, not ignore it"),
        }
        match tls_to_mongo(&cfg(TlsMode::Require, None)) {
            Tls::Enabled(o) => assert_eq!(o.allow_invalid_certificates, Some(true)),
            Tls::Disabled => panic!("require must enable tls"),
        }
        assert!(matches!(
            tls_to_mongo(&cfg(TlsMode::Disable, None)),
            Tls::Disabled
        ));
    }

    #[test]
    fn roast_default_batch_byte_cap_is_bounded_not_unlimited() {
        let cap = mongo_batch_byte_cap(None);
        assert!(
            cap > 0 && cap < (i32::MAX as usize),
            "cap must be finite + sub-2GiB"
        );
        assert_eq!(mongo_batch_byte_cap(Some(64)), 64 * 1024 * 1024);
        assert_eq!(mongo_batch_byte_cap(Some(0)), mongo_batch_byte_cap(None));
    }

    #[test]
    fn roast_non_ascii_cursor_token_is_a_clean_error_not_a_panic() {
        assert!(decode_id_cursor("€€").is_err());
    }

    #[test]
    fn id_cursor_token_roundtrips_every_bson_type_losslessly() {
        let oid = Bson::ObjectId(ObjectId::parse_str("64b8f0000000000000000000").unwrap());
        for id in [
            oid.clone(),
            Bson::Int32(1001),
            Bson::Int64(9_000_000_000),
            Bson::String("1001".to_string()),
            Bson::String("sku-00042".to_string()),
        ] {
            let token = encode_id_cursor(&id);
            assert_eq!(
                decode_id_cursor(&token).unwrap(),
                id,
                "typed cursor round-trip lost the type for {id:?}"
            );
        }
        assert_ne!(
            encode_id_cursor(&Bson::Int32(1001)),
            encode_id_cursor(&Bson::String("1001".to_string()))
        );
        assert_eq!(decode_id_cursor("64b8f0000000000000000000").unwrap(), oid);
    }

    #[test]
    fn document_renders_relaxed_and_canonical() {
        let d = doc! {
            "a": 1i32,
            "b": "x",
            "big": 9_223_372_036_854_775_807i64,
            "nested": doc! { "d": true },
        };
        let relaxed = document_to_json(&d, false).unwrap();
        assert!(relaxed.contains("\"a\":1"), "got: {relaxed}");
        assert!(relaxed.contains("\"b\":\"x\""), "got: {relaxed}");
        assert!(
            relaxed.contains("\"nested\":{\"d\":true}"),
            "got: {relaxed}"
        );
        assert!(
            relaxed.contains("\"big\":9223372036854775807"),
            "int64 is a bare number in relaxed: {relaxed}"
        );
        let canonical = document_to_json(&d, true).unwrap();
        assert!(
            canonical.contains("\"$numberLong\":\"9223372036854775807\""),
            "int64 is type-tagged in canonical: {canonical}"
        );
    }

    #[test]
    fn blob_schema_is_two_columns_id_and_document_json() {
        let schema = blob_schema(&ColumnOverrides::default()).unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["_id", "document"]);
        let doc_field = schema.field_with_name("document").unwrap();
        assert_eq!(
            doc_field.metadata().get("ARROW:extension:name"),
            Some(&"arrow.json".to_string()),
            "document column must carry the arrow.json extension type"
        );
    }

    #[test]
    fn a_columns_override_on_the_blob_columns_is_refused_or_read_never_ignored() {
        let one = |c: &str, t: RivetType| ColumnOverrides::from([(c.to_string(), t)]);
        for (c, t) in [("_id", RivetType::Int64), ("document", RivetType::Int64)] {
            let err = blob_schema(&one(c, t)).unwrap_err();
            assert_eq!(
                crate::error::code_of(&err).map(|c| c.id),
                Some(crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH.id),
                "{c}"
            );
        }
        let doc = blob_schema(&one("document", RivetType::String)).unwrap();
        assert!(
            doc.field_with_name("document")
                .unwrap()
                .metadata()
                .get("ARROW:extension:name")
                .is_none()
        );
    }

    #[test]
    fn harm_counters_extracted_from_server_status() {
        let status = doc! {
            "metrics": {
                "queryExecutor": { "scanned": 10i64, "scannedObjects": 500i64 },
                "document": { "returned": 480i64 },
            },
            "opcounters": { "query": 7i32, "getmore": 12i32 },
            "wiredTiger": { "cache": { "bytes read into cache": 1_048_576i64 } },
        };
        let harm: std::collections::HashMap<String, i64> =
            harm_from_server_status(&status).into_iter().collect();
        assert_eq!(harm.get("mongo_docs_scanned"), Some(&500));
        assert_eq!(harm.get("mongo_keys_scanned"), Some(&10));
        assert_eq!(harm.get("mongo_docs_returned"), Some(&480));
        assert_eq!(harm.get("mongo_op_query"), Some(&7));
        assert_eq!(harm.get("mongo_op_getmore"), Some(&12));
        assert_eq!(harm.get("mongo_wt_cache_bytes_read"), Some(&1_048_576));
    }

    #[test]
    fn reconcile_count_query_names_the_collection() {
        assert_eq!(
            last_from_identifier("SELECT COUNT(*) FROM (SELECT * FROM orders) AS _rivet_reconcile"),
            Some("orders")
        );
        assert_eq!(
            last_from_identifier("select count(*) from (select * from shop_events) as _r"),
            Some("shop_events")
        );
        assert_eq!(last_from_identifier("SELECT 1"), None);
    }

    #[test]
    fn harm_skips_missing_sections_gracefully() {
        let status = doc! { "opcounters": { "query": 3i64 } };
        let harm: std::collections::HashMap<String, i64> =
            harm_from_server_status(&status).into_iter().collect();
        assert_eq!(harm.get("mongo_op_query"), Some(&3));
        assert!(!harm.contains_key("mongo_wt_cache_bytes_read"));
        assert!(!harm.contains_key("mongo_docs_scanned"));
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    /// A NaN or two-bracket `_id` span is a refusal (exit 5).
    #[test]
    fn an_unseekable_id_span_and_an_undecodable_cursor_token_are_refused_by_code() {
        let (nan, one, text) = (
            Bson::Double(f64::NAN),
            Bson::Int32(1),
            Bson::String("1".into()),
        );
        let err = seekable_id_span("c", &nan, &one).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_KEYSET_KEY_UNORDERED", 5);
        let err = seekable_id_span("c", &one, &text).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_KEYSET_KEY_UNORDERED", 5);
        assert!(seekable_id_span("c", &one, &Bson::Int64(2)).is_ok());
        for token in ["€€", "abc", "zz"] {
            let err = decode_id_cursor(token).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_STATE_CURSOR_INCOMPARABLE", 5);
        }
    }
}

#[cfg(test)]
mod collection_identity_tests {
    use super::fake_server::FakeMongo;
    use super::*;
    use crate::tuning::SourceTuning;

    const FIRST: [u8; 16] = [1; 16];
    const SECOND: [u8; 16] = [2; 16];

    #[derive(Default)]
    struct Rows {
        rows: usize,
    }

    impl BatchSink for Rows {
        fn on_schema(&mut self, _schema: SchemaRef) -> Result<()> {
            Ok(())
        }
        fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
            self.rows += batch.num_rows();
            Ok(())
        }
    }

    fn documents(n: i32) -> Vec<Document> {
        (0..n).map(|i| doc! { "_id": i, "v": i }).collect()
    }

    fn source(fake: &FakeMongo) -> MongoSource {
        MongoSource::connect(&fake.url("shop"), None, None).expect("connect to the fake server")
    }

    fn read(source: &mut MongoSource, page_limit: Option<usize>) -> (Result<()>, usize) {
        let tuning = SourceTuning::from_config(None);
        let overrides = ColumnOverrides::default();
        let mut request = ExportRequest::unwrapped("SELECT * FROM t", &tuning, &overrides);
        request.page_limit = page_limit;
        let mut sink = Rows::default();
        let outcome = source.export(&request, &mut sink);
        (outcome, sink.rows)
    }

    fn code(outcome: Result<()>) -> Option<&'static str> {
        crate::error::error_code(&outcome.expect_err("the read is refused"))
    }

    #[test]
    fn a_collection_that_stays_the_same_is_read_whole() {
        let fake = FakeMongo::serving(&FIRST, documents(5));
        let (outcome, rows) = read(&mut source(&fake), None);
        outcome.expect("an unchanged collection reads");
        assert_eq!(rows, 5);
    }

    #[test]
    fn a_missing_collection_is_refused_before_any_read() {
        let fake = FakeMongo::serving(&FIRST, documents(5));
        fake.shelf.lock().unwrap().uuid = None;
        let (outcome, rows) = read(&mut source(&fake), None);
        assert_eq!(code(outcome), Some("RIVET_SOURCE_COLLECTION_NOT_FOUND"));
        assert_eq!((rows, fake.shelf.lock().unwrap().finds), (0, 0));
    }

    #[test]
    fn a_collection_created_again_under_a_read_is_refused_though_the_read_returned_documents() {
        let fake = FakeMongo::serving(&FIRST, documents(5));
        fake.shelf.lock().unwrap().on_find = Some(Box::new(|shelf, _| {
            shelf.uuid = Some(SECOND.to_vec());
        }));
        let (outcome, rows) = read(&mut source(&fake), None);
        assert_eq!(code(outcome), Some("RIVET_SOURCE_COLLECTION_NOT_FOUND"));
        assert_eq!(rows, 5);
    }

    #[test]
    fn a_collection_dropped_under_a_read_that_returned_documents_is_refused() {
        let fake = FakeMongo::serving(&FIRST, documents(5));
        fake.shelf.lock().unwrap().on_find = Some(Box::new(|shelf, _| shelf.uuid = None));
        let (outcome, rows) = read(&mut source(&fake), None);
        assert_eq!(code(outcome), Some("RIVET_SOURCE_COLLECTION_NOT_FOUND"));
        assert_eq!(rows, 5);
    }

    #[test]
    fn a_collection_created_again_between_two_pages_is_refused_by_the_second_page_listing() {
        let fake = FakeMongo::serving(&FIRST, documents(3));
        let mut keyset = source(&fake);
        let (first, _) = read(&mut keyset, None);
        first.expect("the first read pins the collection");
        fake.shelf.lock().unwrap().uuid = Some(SECOND.to_vec());
        let (second, _) = read(&mut keyset, None);
        assert_eq!(code(second), Some("RIVET_SOURCE_COLLECTION_NOT_FOUND"));
    }

    #[test]
    fn a_page_lists_the_collection_once_and_the_first_page_lists_it_twice() {
        let fake = FakeMongo::serving(&FIRST, documents(3));
        let mut keyset = source(&fake);
        read(&mut keyset, None).0.expect("page 1");
        assert_eq!(fake.shelf.lock().unwrap().listed, 2);
        read(&mut keyset, None).0.expect("page 2");
        read(&mut keyset, None).0.expect("page 3");
        assert_eq!(fake.shelf.lock().unwrap().listed, 4);
    }

    #[test]
    fn a_listing_that_fails_for_any_reason_but_the_privilege_stops_the_run() {
        let fake = FakeMongo::serving(&FIRST, documents(4));
        fake.shelf.lock().unwrap().listing_fails = true;
        let (outcome, rows) = read(&mut source(&fake), None);
        let said = format!("{:#}", outcome.expect_err("an unanswered listing"));
        assert!(
            said.contains("listing MongoDB collection `shop.t` failed"),
            "{said}"
        );
        assert_eq!((rows, fake.shelf.lock().unwrap().finds), (0, 0));
    }

    #[test]
    fn a_collection_pinned_by_a_stored_cursor_refuses_one_created_again_before_any_read() {
        let fake = FakeMongo::serving(&SECOND, documents(4));
        let mut reader = source(&fake);
        reader.pin_incarnation("t", &crate::types::hex_bytes(&FIRST));
        let (outcome, rows) = read(&mut reader, None);
        assert_eq!(code(outcome), Some("RIVET_SOURCE_COLLECTION_NOT_FOUND"));
        assert_eq!((rows, fake.shelf.lock().unwrap().finds), (0, 0));
    }

    #[test]
    fn a_collection_pinned_through_the_source_trait_refuses_one_created_again() {
        let fake = FakeMongo::serving(&SECOND, documents(4));
        let mut reader = source(&fake);
        Source::pin_incarnation(&mut reader, "t", &crate::types::hex_bytes(&FIRST));
        let (outcome, rows) = read(&mut reader, None);
        assert_eq!(code(outcome), Some("RIVET_SOURCE_COLLECTION_NOT_FOUND"));
        assert_eq!((rows, fake.shelf.lock().unwrap().finds), (0, 0));
    }

    #[test]
    fn the_incarnation_of_a_collection_is_its_uuid_in_hex_and_a_view_has_none() {
        let fake = FakeMongo::serving(&FIRST, documents(1));
        let session = MongoSession::connect(&fake.url("shop"), None).expect("connect");
        assert_eq!(
            session.collection_incarnation("t").unwrap(),
            Some("01010101010101010101010101010101".to_string())
        );
        fake.shelf.lock().unwrap().view = true;
        assert_eq!(session.collection_incarnation("t").unwrap(), None);
        fake.shelf.lock().unwrap().uuid = None;
        assert_eq!(session.collection_incarnation("t").unwrap(), None);
    }

    #[test]
    fn a_worker_reads_under_the_uuid_its_run_pinned_not_the_one_it_finds() {
        let fake = FakeMongo::serving(&FIRST, documents(3));
        let mut probe = source(&fake);
        probe.confirm_collection("t").expect("the probe pins");
        assert_eq!(probe.pinned().get("t").map(Vec::as_slice), Some(&FIRST[..]));
        fake.shelf.lock().unwrap().uuid = Some(SECOND.to_vec());
        let mut worker = source(&fake).with_pinned(probe.pinned());
        let (outcome, rows) = read(&mut worker, None);
        assert_eq!(code(outcome), Some("RIVET_SOURCE_COLLECTION_NOT_FOUND"));
        assert_eq!((rows, fake.shelf.lock().unwrap().finds), (0, 0));
    }

    #[test]
    fn a_source_without_a_pin_pins_the_collection_it_first_lists() {
        let fake = FakeMongo::serving(&FIRST, documents(1));
        let mut reader = source(&fake);
        assert!(reader.pinned().is_empty());
        reader
            .confirm_collection("t")
            .expect("the collection is listed");
        assert_eq!(
            reader.pinned().get("t").map(Vec::as_slice),
            Some(&FIRST[..])
        );
        fake.shelf.lock().unwrap().uuid = Some(SECOND.to_vec());
        reader
            .confirm_collection("t")
            .expect_err("the second listing differs from the pin");
        assert_eq!(
            reader.pinned().get("t").map(Vec::as_slice),
            Some(&FIRST[..])
        );
    }

    #[test]
    fn a_listing_the_role_may_not_read_refuses_nothing() {
        let fake = FakeMongo::serving(&FIRST, documents(4));
        fake.shelf.lock().unwrap().listing_denied = true;
        let (outcome, rows) = read(&mut source(&fake), None);
        outcome.expect("a denied listing fails open");
        assert_eq!(rows, 4);
    }

    #[test]
    fn a_view_listed_without_a_uuid_is_read() {
        let fake = FakeMongo::serving(&FIRST, documents(2));
        fake.shelf.lock().unwrap().view = true;
        let (outcome, rows) = read(&mut source(&fake), None);
        outcome.expect("a view has an empty UUID and stays readable");
        assert_eq!(rows, 2);
    }

    #[test]
    fn the_listing_names_the_uuid_the_server_holds() {
        let fake = FakeMongo::serving(&FIRST, documents(1));
        let session = MongoSession::connect(&fake.url("shop"), None).expect("connect");
        assert_eq!(
            list_collection_uuids(&session, "t").unwrap(),
            Some(vec![FIRST.to_vec()])
        );
        fake.shelf.lock().unwrap().uuid = None;
        assert_eq!(
            list_collection_uuids(&session, "t").unwrap(),
            Some(Vec::new())
        );
        fake.shelf.lock().unwrap().listing_denied = true;
        assert_eq!(list_collection_uuids(&session, "t").unwrap(), None);
    }

    #[test]
    fn the_session_check_refuses_a_missing_collection_and_passes_a_listed_one() {
        let fake = FakeMongo::serving(&FIRST, documents(1));
        let session = MongoSession::connect(&fake.url("shop"), None).expect("connect");
        session.require_collection("t").expect("listed");
        fake.shelf.lock().unwrap().uuid = None;
        assert_eq!(
            crate::error::error_code(&session.require_collection("t").expect_err("missing")),
            Some("RIVET_SOURCE_COLLECTION_NOT_FOUND")
        );
    }
}
