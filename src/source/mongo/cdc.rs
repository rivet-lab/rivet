//! **Layer: Execution** — MongoDB CDC via change streams.

use futures_util::StreamExt;
use mongodb::action::Action;
use mongodb::bson::{Document, doc};
use mongodb::change_stream::ChangeStream as DriverStream;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::options::{FullDocumentBeforeChangeType, FullDocumentType};

use super::{MongoSession, document_to_json, id_to_string};
use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{
    ChangeEvent, ChangeOp, ChangeStream, DrainMode, Position, ReadableColumns,
};
use std::collections::HashMap;

/// The two fixed image columns, matching the batch JSON-blob schema.
static IMAGE_NAMES: std::sync::LazyLock<std::sync::Arc<[String]>> =
    std::sync::LazyLock::new(|| {
        std::sync::Arc::from(vec!["_id".to_string(), "document".to_string()])
    });

pub(crate) struct MongoChangeStream {
    session: MongoSession,
    stream: DriverStream<ChangeStreamEvent<Document>>,
    /// Render `document` as canonical (type-tagged) extended JSON when set.
    canonical: bool,
    /// The watched database — the `schema` field of every emitted change.
    db_name: String,
    /// Bounded "catch up to the current oplog end and exit" run (the scheduler model).
    until_current: bool,
    /// The stream's resume-token `_data` at open — the "current end" a bounded run drains up to.
    target_data: Option<String>,
    /// Cluster time at open — the UPPER time bound for a bounded run.
    until_current_ts: Option<mongodb::bson::Timestamp>,
    /// The resume-token clusterTime a bounded drain must reach before it stops; see `token_floor`.
    token_floor: Option<mongodb::bson::Timestamp>,
    /// Heterogeneous-`_id` warn state, mirroring the batch full-scan warning ([`super::MongoSource::warn_if_heterogeneous_id`]).
    hetero_first: Option<Option<u8>>,
    hetero_warned: bool,
    /// The collections this export captures, so a DDL event can be told apart from one on another collection.
    configured_tables: Vec<String>,
    /// The collections a keyed `load:` merges by `_id`; a second `_id` type in one is refused.
    keyed_tables: Vec<String>,
    /// The `_id` types delivered per collection, persisted in the checkpoint.
    id_types: super::id_types::IdTypes,
    /// Orders events that share a clusterTime.
    clock: OrdinalClock,
    /// The collections whose events the server filters to a view's fields (`columns_from: grants`).
    projected: Vec<String>,
}

/// Counts events sharing one clusterTime from 0; a resume restores it from the checkpoint's `ct` and `ord`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct OrdinalClock {
    ct: u64,
    ord: u64,
}

impl OrdinalClock {
    /// The clock a checkpoint left behind; a checkpoint without `ct` and `ord` starts at zero.
    pub(crate) fn from_checkpoint(v: &serde_json::Value) -> Self {
        let n = |k: &str| v.get(k).and_then(serde_json::Value::as_u64).unwrap_or(0);
        Self {
            ct: n("ct"),
            ord: n("ord"),
        }
    }

    /// The `(ct, ord)` of the next event at this clusterTime.
    pub(crate) fn stamp(&mut self, ct: u64) -> (u64, u64) {
        self.ord = if ct == self.ct {
            self.ord.saturating_add(1)
        } else {
            0
        };
        self.ct = ct;
        (ct, self.ord)
    }
}

/// The `(ct, ord)` of an event, or the refusal for an event the server sent without a clusterTime.
fn stamp_event(
    clock: &mut OrdinalClock,
    cluster_time: Option<mongodb::bson::Timestamp>,
) -> Result<(u64, u64)> {
    match cluster_time {
        Some(t) => Ok(clock.stamp(cluster_time_u64(t))),
        None => crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            "mongodb cdc: the server sent a change event with no clusterTime, which every \
             change event of MongoDB 4.4 and later carries; rivet cannot order that event \
             against the ones around it, so nothing after it was read. Point `source.url` \
             at a MongoDB 4.4 or later replica set or sharded cluster, then re-run"
        ),
    }
}

/// A clusterTime as one integer, seconds in the high half and the increment in the low.
fn cluster_time_u64(t: mongodb::bson::Timestamp) -> u64 {
    (u64::from(t.time) << 32) + u64::from(t.increment)
}

/// How a whole-db change-stream operation is handled.
#[derive(Debug, PartialEq, Eq)]
enum OpClass {
    Row,
    Skip,
    Invalidate,
}

/// Whether a DDL event drops or renames a captured collection — every one of them, for `dropDatabase`.
fn ddl_removes_a_capture(
    op: &OperationType,
    coll: Option<&str>,
    db: &str,
    configured: &[String],
) -> bool {
    match (op, coll) {
        (OperationType::DropDatabase, _) => true,
        (OperationType::Drop | OperationType::Rename, None) => true,
        (OperationType::Drop | OperationType::Rename, Some(c)) => {
            configured.is_empty()
                || configured.iter().any(|t| {
                    crate::source::cdc::sink::table_matches(
                        crate::source::cdc::CdcEngine::Mongo,
                        t,
                        db,
                        c,
                    )
                })
        }
        _ => false,
    }
}

/// Refuse a captured collection that left the source without per-document deletes.
fn dropped_capture_message(op: &OperationType, db: &str, coll: Option<&str>) -> String {
    let what = coll.map_or_else(
        || format!("database `{db}`"),
        |c| format!("collection `{db}.{c}`"),
    );
    format!(
        "mongodb cdc: captured {what} was removed by `{op:?}`, and a change stream carries no \
         per-document deletes for that — skipping it would leave every document it held live in \
         the destination. {}",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

fn classify_op(op: &OperationType) -> OpClass {
    match op {
        OperationType::Insert
        | OperationType::Update
        | OperationType::Replace
        | OperationType::Delete => OpClass::Row,
        OperationType::Invalidate => OpClass::Invalidate,
        _ => OpClass::Skip,
    }
}

/// The `_data` keystring of a resume token, whose lexical order is oplog order.
fn token_data(v: &serde_json::Value) -> Option<String> {
    crate::source::cdc::order::MONGO.text(v, "_data")
}

/// `token_data` is the resume token's identity — the string every bound and resume decision compares.
#[cfg(test)]
mod token_data_tests {
    use super::*;

    #[test]
    fn token_data_plucks_the_data_string_and_nothing_else() {
        assert_eq!(
            token_data(&serde_json::json!({ "_data": "82ABCD" })),
            Some("82ABCD".to_string())
        );
        assert_eq!(
            token_data(&serde_json::json!({ "_data": "" })),
            Some(String::new()),
            "an EMPTY _data is still a token — collapsing it to None would make \
             an empty-token checkpoint re-anchor instead of resuming"
        );
        for absent in [
            serde_json::json!({}),
            serde_json::json!({ "_data": 7 }),
            serde_json::json!({ "other": "82ABCD" }),
            serde_json::Value::Null,
        ] {
            assert_eq!(
                token_data(&absent),
                None,
                "{absent}: no string _data means NO token — inventing one would \
                 compare bounds against a fiction"
            );
        }
    }
}

/// How long a drain waits, once its token is past the open-time target, for the token to reach the open-time clusterTime bound.
const BOUND_WAIT_CEILING: std::time::Duration = std::time::Duration::from_secs(120);

/// An empty poll ends the drain once the token is past the open-time target and has reached the open-time clusterTime bound, or has waited `BOUND_WAIT_CEILING` for it, or when there is no target.
fn idle_poll_stops(
    advanced: Option<&str>,
    target: Option<&str>,
    bound: Option<mongodb::bson::Timestamp>,
    waited: std::time::Duration,
) -> bool {
    match (advanced, target) {
        (Some(cur), Some(tgt)) => {
            cur > tgt
                && (token_reached_bound(
                    cur,
                    seeded!("mongo-cdc-stops-before-the-open-time-bound", None, bound),
                ) || seeded!(
                    "mongo-cdc-bound-wait-has-no-ceiling",
                    waited > std::time::Duration::MAX,
                    waited >= BOUND_WAIT_CEILING
                ))
        }
        (_, None) => true,
        _ => false,
    }
}

/// Whether the token is past the open-time target while still below the open-time clusterTime bound, the one state `BOUND_WAIT_CEILING` limits.
fn bound_unmet(
    advanced: Option<&str>,
    target: Option<&str>,
    bound: Option<mongodb::bson::Timestamp>,
) -> bool {
    advanced
        .zip(target)
        .is_some_and(|(cur, tgt)| cur > tgt && !token_reached_bound(cur, bound))
}

/// Whether a `hello` answer comes from a `mongos` router, which names itself `isdbgrid` in `msg`.
fn is_mongos(hello: &mongodb::bson::Document) -> bool {
    hello.get_str("msg") == Ok("isdbgrid")
}

/// The clusterTime a resume token must reach before a bounded drain stops.
fn token_floor(
    bound: Option<mongodb::bson::Timestamp>,
    sharded: bool,
) -> Option<mongodb::bson::Timestamp> {
    bound.map(|b| {
        match (
            seeded!(
                "mongo-cdc-sharded-drain-stops-at-the-bound-tick",
                false,
                sharded
            ),
            b.increment.checked_add(1),
        ) {
            (true, Some(increment)) => mongodb::bson::Timestamp {
                time: b.time,
                increment,
            },
            (true, None) => mongodb::bson::Timestamp {
                time: b.time.saturating_add(1),
                increment: 0,
            },
            (false, _) => b,
        }
    })
}

/// Whether the clusterTime a resume token's `_data` opens with (`82` + seconds + increment, big-endian hex) is at or past `bound`.
fn token_reached_bound(data: &str, bound: Option<mongodb::bson::Timestamp>) -> bool {
    let Some(b) = bound else { return true };
    let parsed = data
        .strip_prefix("82")
        .filter(|rest| rest.len() >= 16)
        .and_then(|rest| {
            let time = u32::from_str_radix(rest.get(..8)?, 16).ok()?;
            let increment = u32::from_str_radix(rest.get(8..16)?, 16).ok()?;
            Some((time, increment))
        });
    parsed.is_none_or(|ct| ct >= (b.time, b.increment))
}

/// The TIME bound: an event whose `cluster_time` is past the open-time cluster time arrived AFTER we opened.
fn past_time_bound(
    until_current: bool,
    cluster_time: Option<mongodb::bson::Timestamp>,
    bound: Option<mongodb::bson::Timestamp>,
) -> bool {
    until_current && matches!((cluster_time, bound), (Some(ct), Some(b)) if ct > b)
}

/// Run the server-identity command, tolerating servers that predate `hello`.
async fn server_identity(
    client: &mongodb::Client,
    db: &str,
) -> std::result::Result<mongodb::bson::Document, mongodb::error::Error> {
    match client.database(db).run_command(doc! { "hello": 1 }).await {
        Ok(d) => Ok(d),
        Err(hello_err) => client
            .database(db)
            .run_command(doc! { "isMaster": 1 })
            .await
            .map_err(|_| hello_err),
    }
}

/// The open-time cluster-time bound a `until_current` run MUST have, or a loud refusal explaining why the run cannot be bounded.
pub(crate) fn until_current_bound(
    until_current: bool,
    operation_time: Option<mongodb::bson::Timestamp>,
    probe_error: Option<String>,
) -> Result<Option<mongodb::bson::Timestamp>> {
    if !until_current {
        return Ok(None);
    }
    operation_time.map(Some).ok_or_else(|| {
        let cause = match probe_error {
            Some(e) => format!("the server answered: {e}"),
            None => "the server answered without an `operationTime`".to_string(),
        };
        anyhow::Error::new(crate::error::CodedError::new(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            format!(
                "mongo cdc: `until_current: true` needs the cluster time at open, and {cause}. \
             Unlike the SQL engines, a MongoDB change stream has no catch-up exit to fall \
             back on: without this bound the run would keep draining under any ongoing \
             writes and NEVER terminate, so continuing would hang rather than finish late. \
             Check the connection's permissions and that the source is a replica set / \
             sharded cluster (a standalone reports no operationTime and cannot serve change \
             streams at all), or set `until_current: false` to stream continuously."
            ),
        ))
    })
}

/// Persist a resume token as a [`Position`] losslessly, stamped with its `(clusterTime, ordinal)`.
fn encode_resume_token(
    token: &mongodb::change_stream::event::ResumeToken,
    (ct, ord): (u64, u64),
) -> Result<Position> {
    let bson = mongodb::bson::to_bson(token)?;
    let doc = bson.as_document().ok_or_else(|| {
        anyhow::Error::new(crate::error::CodedError::new(
            crate::error::codes::INTERNAL_SOURCE_INVARIANT,
            "mongodb cdc: resume token is not a BSON document".to_string(),
        ))
    })?;
    let mut buf = Vec::new();
    doc.to_writer(&mut buf)?;
    let hex = super::bytes_to_hex(&buf);
    let data = doc.get_str("_data").ok();
    if data.is_none() && doc.get("_data").is_some() {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            "mongodb cdc: resume token `_data` is not a string (a pre-4.2 BinData \
             token?) — refusing to emit an unorderable __pos; every token from \
             this server has the same shape, so the remedy is upgrading the \
             server (support floor is MongoDB 4.4)"
        );
    }
    Ok(Position::new(
        serde_json::json!({ "_data": data, "rt": hex, "ct": ct, "ord": ord }),
    ))
}

/// Inverse of [`encode_resume_token`], with a fallback to the pre-lossless `serde_json` form so an older checkpoint still resolves.
pub(crate) fn decode_resume_token(
    v: &serde_json::Value,
) -> Result<mongodb::change_stream::event::ResumeToken> {
    if let Some(hex) = v.get("rt").and_then(|x| x.as_str()) {
        let bytes = super::hex_to_bytes(hex)?;
        let doc = Document::from_reader(&bytes[..])?;
        return Ok(mongodb::bson::from_bson(mongodb::bson::Bson::Document(
            doc,
        ))?);
    }
    if let Some(data) = v.get("_data").and_then(|x| x.as_str()) {
        return Ok(serde_json::from_value(
            serde_json::json!({ "_data": data }),
        )?);
    }
    Err(crate::source::cdc::checkpoint_identity::checkpoint_invalid(
        format!(
            "mongodb cdc: unrecognized resume-token checkpoint shape \
         (expected an `rt` hex string or a `_data` string): {v}"
        ),
    ))
}

impl MongoChangeStream {
    /// Open a database-wide change stream, resuming from `checkpoint` when one exists (else starting at the current oplog position).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open(
        url: &str,
        tls: Option<&TlsConfig>,
        checkpoint: Option<&std::path::Path>,
        canonical: bool,
        mode: DrainMode,
        configured_tables: &[String],
        keyed_tables: &[String],
        grants: Option<&HashMap<String, ReadableColumns>>,
    ) -> Result<Self> {
        let until_current = mode.is_bounded();
        let session = MongoSession::connect(url, tls)?;
        let db_name = session.db().to_string();
        let loaded = match checkpoint {
            Some(p) => Position::load(p)?,
            None => None,
        };
        let clock = loaded
            .as_ref()
            .map(|pos| {
                seeded!(
                    "mongo-cdc-ordinal-restarts-on-resume",
                    OrdinalClock::default(),
                    OrdinalClock::from_checkpoint(&pos.0)
                )
            })
            .unwrap_or_default();
        let known = loaded
            .as_ref()
            .map(|pos| super::id_types::IdTypes::from_checkpoint(&pos.0))
            .unwrap_or_default();
        let id_types = super::id_types::guard_keyed(known, keyed_tables, |c| {
            super::id_types::id_span_on(&session, c)
        })?;
        let resume = loaded.map(|pos| decode_resume_token(&pos.0)).transpose()?;
        let is_fresh = resume.is_none();
        if !configured_tables.is_empty() {
            let present: Vec<String> = session
                .block_on(async {
                    session
                        .client()
                        .database(&db_name)
                        .list_collection_names()
                        .await
                })
                .unwrap_or_default();
            if !present.is_empty() {
                let missing: Vec<&String> = configured_tables
                    .iter()
                    .filter(|c| {
                        !present.iter().any(|p| {
                            crate::source::cdc::sink::table_matches(
                                crate::source::cdc::CdcEngine::Mongo,
                                c,
                                &db_name,
                                p,
                            )
                        })
                    })
                    .collect();
                if !missing.is_empty() {
                    log::warn!(
                        "mongodb cdc: could not find {missing:?} in database `{db_name}` — it \
                         holds {present:?}. A collection is created by its first write, so \
                         this is fine if the app has not written yet; if it is a typo the \
                         capture will report success over zero rows forever."
                    );
                }
            }
        }
        let cap = probe_capability_on(&session);
        if cap.major >= 6 {
            log::info!(
                "mongodb cdc: server {} — capture tier: {}",
                cap.server_version,
                cap.tier()
            );
        } else {
            log::warn!(
                "mongodb cdc: server {} — capture tier: {}",
                cap.server_version,
                cap.tier()
            );
        }
        let stream = session
            .block_on(async {
                session
                    .client()
                    .database(&db_name)
                    .watch()
                    .full_document(FullDocumentType::UpdateLookup)
                    .optional(cap.before_image_request(), |w, v| {
                        w.full_document_before_change(v)
                    })
                    .optional(super::column_grants::stream_pipeline(grants), |w, p| {
                        w.pipeline(p)
                    })
                    .max_await_time(std::time::Duration::from_millis(500))
                    .resume_after(resume)
                    .await
            })
            .map_err(diagnose_stream_error)?;
        let target_data = if until_current {
            stream
                .resume_token()
                .and_then(|t| serde_json::to_value(&t).ok())
                .as_ref()
                .and_then(token_data)
        } else {
            None
        };
        let (operation_time, sharded, probe_err) = if until_current {
            session.block_on(async {
                match server_identity(session.client(), &db_name).await {
                    Ok(d) => (d.get_timestamp("operationTime").ok(), is_mongos(&d), None),
                    Err(e) => (None, false, Some(e.to_string())),
                }
            })
        } else {
            (None, false, None)
        };
        let until_current_ts = until_current_bound(until_current, operation_time, probe_err)?;
        let token_floor = token_floor(until_current_ts, sharded);
        let this = Self {
            session,
            stream,
            canonical,
            db_name,
            until_current,
            target_data,
            until_current_ts,
            token_floor,
            hetero_first: None,
            hetero_warned: false,
            configured_tables: configured_tables.to_vec(),
            keyed_tables: keyed_tables.to_vec(),
            id_types,
            clock,
            projected: super::column_grants::projected_collections(grants),
        };
        if is_fresh
            && let Some(ckpt) = checkpoint
            && let Some(pos) = this.anchor_position()
        {
            this.checkpoint_of(&pos).save(ckpt)?;
            log::warn!(
                "mongodb cdc: no checkpoint at `{}` — anchoring the resume token at the \
                 CURRENT oplog position, so anything written before now is NOT captured. \
                 On a first run that is expected. If this checkpoint existed before, it \
                 was deleted or the config moved: a RELATIVE `cdc.checkpoint:` is \
                 resolved against the CONFIG FILE's directory, so the path above is \
                 where rivet looked, and the changes since it was written are gone from \
                 this stream. {}",
                ckpt.display(),
                crate::source::cdc::checkpoint_identity::RECOVER
            );
        }
        Ok(this)
    }

    /// The resume token to open from right now — used to pin a client-side anchor before any change arrives (the MySQL model.
    pub(crate) fn anchor_position(&self) -> Option<Position> {
        self.stream
            .resume_token()
            .and_then(|t| encode_resume_token(&t, (0, 0)).ok())
    }
}

/// Pin the resume anchor at the CURRENT oplog position (a first-run open with no prior checkpoint).
pub(crate) fn pin_checkpoint_at_current(
    url: &str,
    tls: Option<&TlsConfig>,
    checkpoint: &std::path::Path,
) -> Result<()> {
    MongoChangeStream::open(
        url,
        tls,
        Some(checkpoint),
        false,
        DrainMode::Continuous,
        &[],
        &[],
        None,
    )
    .map(|_| ())
}

/// What a MongoDB CDC run actually delivers.
pub(crate) struct MongoCdcCapability {
    pub(crate) server_version: String,
    pub(crate) major: u32,
    pub(crate) is_replica_set: bool,
}

impl MongoCdcCapability {
    /// The pre-image request the stream may carry.
    pub(crate) fn before_image_request(&self) -> Option<FullDocumentBeforeChangeType> {
        seeded!(
            "mongo-sharded-cdc-requests-pre-images-below-6-0",
            Some(FullDocumentBeforeChangeType::WhenAvailable),
            (self.major == 0 || self.major >= 6)
                .then_some(FullDocumentBeforeChangeType::WhenAvailable)
        )
    }

    /// One-line fidelity declaration for the log / `doctor`.
    pub(crate) fn tier(&self) -> &'static str {
        if self.major >= 6 {
            "full-image-capable (6.0+) — DELETE pre-images ride when \
             changeStreamPreAndPostImages is enabled; an UPDATE's document is a \
             current-state lookup, so a document deleted after the change has none \
             and rivet refuses that event rather than writing NULL"
        } else {
            "current-state (UpdateLookup) — update post-images are current-state \
             (not point-in-time) and deletes carry _id only; upgrade to 6.0+ for pre/post-images"
        }
    }
}

/// Probe server version + replica-set membership on an existing session.
fn probe_capability_on(session: &MongoSession) -> MongoCdcCapability {
    session.block_on(async {
        let db = session.client().database(session.db());
        let version = db
            .run_command(doc! { "buildInfo": 1 })
            .await
            .ok()
            .and_then(|d| d.get_str("version").ok().map(str::to_string))
            .unwrap_or_else(|| "unknown".to_string());
        let major = version
            .split('.')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let is_replica_set = server_identity(session.client(), session.db())
            .await
            .is_ok_and(|d| d.get_str("setName").is_ok());
        MongoCdcCapability {
            server_version: version,
            major,
            is_replica_set,
        }
    })
}

/// Connect + probe (for `rivet doctor` — a fresh connection).
pub(crate) fn probe_capability(url: &str, tls: Option<&TlsConfig>) -> Result<MongoCdcCapability> {
    let session = MongoSession::connect(url, tls)?;
    Ok(probe_capability_on(&session))
}

/// Map one driver change event to the canonical [`ChangeEvent`] (JSON-blob image).
fn to_change_event(
    cse: ChangeStreamEvent<Document>,
    canonical: bool,
    db_name: &str,
    stamp: (u64, u64),
) -> Result<ChangeEvent> {
    let op = match cse.operation_type {
        OperationType::Insert => ChangeOp::Insert,
        OperationType::Update | OperationType::Replace => ChangeOp::Update,
        OperationType::Delete => ChangeOp::Delete,
        other => crate::rivet_bail!(
            crate::error::codes::INTERNAL_SOURCE_INVARIANT,
            "mongodb cdc: change operation {other:?} is collection/cluster level (drop, rename, \
             invalidate), not a row change — rivet streams document changes only"
        ),
    };
    let table = cse
        .ns
        .as_ref()
        .and_then(|n| n.coll.clone())
        .unwrap_or_default();

    let id_str = cse
        .document_key
        .as_ref()
        .map(|dk| id_to_string(dk.get("_id")))
        .unwrap_or_default();
    let id_val = RivetValue::Bytes(id_str.into_bytes());

    let doc_source = if op.values_live_in_before() {
        cse.full_document_before_change.as_ref()
    } else {
        cse.full_document.as_ref()
    };
    let doc_val = match doc_source {
        Some(d) => RivetValue::Bytes(document_to_json(d, canonical)?.into_bytes()),
        None => RivetValue::Null,
    };

    let image = vec![id_val, doc_val];
    let (before, after) = if op.values_live_in_before() {
        (Some(image), None)
    } else {
        (None, Some(image))
    };

    let mut ev = ChangeEvent {
        op,
        schema: db_name.to_string(),
        table,
        before,
        after,
        position: encode_resume_token(&cse.id, stamp)?,
        committed: false,
        image_names: Some(std::sync::Arc::clone(&IMAGE_NAMES)),
        seq: 0,
        poison: None,
        row_id: None,
        before_names: None,
        before_poison: None,
    };
    crate::source::cdc::TxnFramer::single_event_commit(&mut ev);
    Ok(ev)
}

/// Turn a change-stream error into one an operator can act on.
pub(crate) fn error_names_its_own_cause(e: &anyhow::Error) -> bool {
    let text = format!("{e:#}");
    text.contains("16 MB BSON limit")
}

/// What an operator reads when the server is a standalone, which serves no change stream.
const STANDALONE_MESSAGE: &str = "mongodb cdc: this MongoDB server is a standalone, and MongoDB serves \
     change streams only on a replica set or a sharded cluster (server error 40573). Point `url:` at \
     a replica-set member or a mongos, or run this server as a one-member replica set (`--replSet` \
     and `rs.initiate()`); a standalone can still be exported with `mode: full`.";

/// Does this driver error text say the server is not a replica set (error 40573)?
fn names_standalone(text: &str) -> bool {
    text.contains("Location40573") || text.contains("only supported on replica sets")
}

/// Does this driver error text name an OVERSIZED change event?
fn names_oversize_event(text: &str) -> bool {
    text.contains("BSONObjectTooLarge") || text.contains("BSONObj size")
}

/// Whether this driver error text names a resume token the oplog no longer holds (server error 286).
fn names_history_lost(text: &str) -> bool {
    text.contains("ChangeStreamHistoryLost")
}

/// Refuse a resume token the oplog rolled past.
pub(crate) fn history_lost_message(server_said: &str) -> String {
    format!(
        "mongodb cdc: the oplog no longer holds the checkpoint's resume token — it rolled past \
         the checkpoint, and the changes between the token and the oldest oplog entry are no \
         longer in the log (server error: {server_said}). {}",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

fn diagnose_stream_error(e: mongodb::error::Error) -> anyhow::Error {
    let text = e.to_string();
    if names_history_lost(&text) {
        return crate::source::cdc::checkpoint_identity::log_gap(history_lost_message(&text));
    }
    if names_standalone(&text) {
        return seeded!(
            "mongo-standalone-uncoded",
            anyhow::Error::from(e),
            anyhow::Error::new(crate::error::CodedError::new(
                crate::error::codes::SOURCE_CDC_PREREQUISITE,
                STANDALONE_MESSAGE,
            ))
        );
    }
    if names_oversize_event(&text) {
        return crate::source::cdc::checkpoint_identity::undecodable(format!(
            "mongodb cdc: a single change event exceeds MongoDB's 16 MB BSON limit — \
             the event carries the post-image, the pre-image and the envelope in ONE \
             document, so a document over roughly 8 MB can cross it on an update. \
             This is not a setup problem and it does not clear on retry: the run stops \
             at the same event every time. Recovery is a re-baseline, whose snapshot \
             reads the document rather than an event carrying two copies of it \
             (verified: a batch read of the same 9 MB document succeeds). {} Deleting \
             the checkpoint WITHOUT the baseline re-anchors at NOW and drops the change \
             with nothing standing in for it. Turning `changeStreamPreAndPostImages` off \
             does NOT unstick THIS event either — the pre-image was already recorded when \
             the change happened — though it does prevent the next one. Server error: {text}",
            crate::source::cdc::checkpoint_identity::RECOVER
        ));
    }
    anyhow::Error::from(e)
}

/// Record the first `_id` bracket of the stream and report true when a later event differs and no warning was given yet.
fn hetero_should_warn(first: &mut Option<Option<u8>>, warned: bool, bracket: Option<u8>) -> bool {
    if warned {
        return false;
    }
    match *first {
        None => {
            *first = Some(bracket);
            false
        }
        Some(seen) => seen != bracket,
    }
}

/// An event that routes to no table and carries no row, whose position moves the checkpoint past `cse`.
fn boundary_marker(cse: &ChangeStreamEvent<Document>, stamp: (u64, u64)) -> Result<ChangeEvent> {
    let mut ev = ChangeEvent {
        op: ChangeOp::Update,
        schema: String::new(),
        table: String::new(),
        before: None,
        after: None,
        position: encode_resume_token(&cse.id, stamp)?,
        committed: false,
        image_names: None,
        seq: 0,
        poison: None,
        row_id: None,
        before_names: None,
        before_poison: None,
    };
    crate::source::cdc::TxnFramer::single_event_commit(&mut ev);
    Ok(ev)
}

impl ChangeStream for MongoChangeStream {
    fn engine(&self) -> crate::source::cdc::CdcEngine {
        crate::source::cdc::CdcEngine::Mongo
    }

    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        let canonical = self.canonical;
        let until_current = self.until_current;
        let target = self.target_data.clone();
        let bound_ts = self.until_current_ts;
        let floor_ts = self.token_floor;
        let session = &self.session;
        let stream = &mut self.stream;
        let db_name = &self.db_name;
        let hetero_first = &mut self.hetero_first;
        let hetero_warned = &mut self.hetero_warned;
        let configured_tables = &self.configured_tables;
        let keyed_tables = &self.keyed_tables;
        let id_types = &mut self.id_types;
        let clock = &mut self.clock;
        let projected = &self.projected;
        let mut bound_wait: Option<std::time::Instant> = None;
        loop {
            let cse = if until_current {
                match session.block_on(async { stream.next_if_any().await }) {
                    Ok(Some(cse)) => cse,
                    Ok(None) => {
                        let advanced = stream
                            .resume_token()
                            .and_then(|t| serde_json::to_value(&t).ok())
                            .as_ref()
                            .and_then(token_data);
                        let unmet = bound_unmet(advanced.as_deref(), target.as_deref(), floor_ts);
                        if unmet {
                            bound_wait.get_or_insert_with(std::time::Instant::now);
                        }
                        let waited = bound_wait.map_or(std::time::Duration::ZERO, |t| t.elapsed());
                        if idle_poll_stops(advanced.as_deref(), target.as_deref(), floor_ts, waited)
                        {
                            if unmet {
                                log::warn!(
                                    "mongodb cdc: the change stream did not reach the cluster time \
                                     the run opened at within {}s (a shard is not advancing its \
                                     resume token); the run stops here and the writes still \
                                     pending arrive on the next run from the checkpoint",
                                    BOUND_WAIT_CEILING.as_secs()
                                );
                            }
                            return None;
                        }
                        continue;
                    }
                    Err(e) => return Some(Err(diagnose_stream_error(e))),
                }
            } else {
                match session.block_on(async { stream.next().await }) {
                    Some(Ok(cse)) => cse,
                    Some(Err(e)) => return Some(Err(diagnose_stream_error(e))),
                    None => return None,
                }
            };

            if past_time_bound(until_current, cse.cluster_time, bound_ts) {
                return None;
            }

            match classify_op(&cse.operation_type) {
                OpClass::Row => {
                    if let Err(e) = super::id_types::admit_change(
                        id_types,
                        keyed_tables,
                        cse.ns.as_ref().and_then(|n| n.coll.as_deref()),
                        cse.document_key.as_ref(),
                    ) {
                        return Some(Err(e));
                    }
                    let bracket = cse
                        .document_key
                        .as_ref()
                        .and_then(|dk| dk.get("_id"))
                        .and_then(super::id_bracket);
                    if hetero_should_warn(hetero_first, *hetero_warned, bracket) {
                        log::warn!(
                            "mongodb cdc: heterogeneous `_id` types across the \
                             change stream: {}",
                            super::hetero_id_guidance()
                        );
                        *hetero_warned = true;
                    }
                    let hidden_only = seeded!(
                        "mongo-cdc-hidden-only-update-delivered",
                        false,
                        super::column_grants::is_hidden_only_update(
                            projected,
                            cse.ns.as_ref().and_then(|n| n.coll.as_deref()),
                            &cse.operation_type,
                            cse.update_description.as_ref(),
                        )
                    );
                    return Some(stamp_event(clock, cse.cluster_time).and_then(|stamp| {
                        if hidden_only {
                            boundary_marker(&cse, stamp)
                        } else {
                            to_change_event(cse, canonical, db_name, stamp)
                        }
                    }));
                }
                OpClass::Skip => {
                    let coll = cse.ns.as_ref().and_then(|n| n.coll.as_deref());
                    if ddl_removes_a_capture(&cse.operation_type, coll, db_name, configured_tables)
                    {
                        return Some(Err(crate::source::cdc::checkpoint_identity::truncated(
                            dropped_capture_message(&cse.operation_type, db_name, coll),
                        )));
                    }
                    continue;
                }
                OpClass::Invalidate => {
                    return Some(Err(crate::source::cdc::checkpoint_identity::truncated(
                        "mongodb cdc: the change stream was INVALIDATED (the watched \
                         database was dropped, or a captured collection dropped/renamed). \
                         The resume token is no longer usable — re-create the capture from \
                         a fresh checkpoint after confirming the source state.",
                    )));
                }
            }
        }
    }

    fn checkpoint_of(&self, position: &Position) -> Position {
        Position::new(self.id_types.stamp_checkpoint(position.json().clone()))
    }
}

#[cfg(test)]
mod tests {
    /// Three pure decisions a mutation run over this file found ungraded.
    #[test]
    fn only_a_ddl_on_a_captured_collection_is_refused() {
        let cap = ["orders".to_string()];
        let r = |op, coll| ddl_removes_a_capture(&op, coll, "shop", &cap);
        assert!(r(OperationType::Drop, Some("orders")));
        assert!(r(OperationType::Rename, Some("orders")));
        assert!(
            !r(OperationType::Drop, Some("audit_log")),
            "another collection's drop is skipped"
        );
        assert!(r(OperationType::DropDatabase, None));
        assert!(
            !r(OperationType::Insert, Some("orders")),
            "a row change is not DDL"
        );
        assert!(
            ddl_removes_a_capture(&OperationType::Drop, Some("x"), "shop", &[]),
            "capture-all owns every collection"
        );
    }

    #[test]
    fn op_classification_delete_framing_and_the_diagnosis_check_all_answer() {
        for op in [
            OperationType::Insert,
            OperationType::Update,
            OperationType::Replace,
            OperationType::Delete,
        ] {
            assert_eq!(
                classify_op(&op),
                OpClass::Row,
                "{op:?} carries a row — classifying it Skip drops the change while \
                 the run still exits 0"
            );
        }
        assert_eq!(
            classify_op(&OperationType::Invalidate),
            OpClass::Invalidate,
            "an invalidate ends the stream's validity; treating it as Skip keeps \
             reading from a cursor the server has abandoned"
        );
        assert_eq!(classify_op(&OperationType::Drop), OpClass::Skip);
        assert_eq!(classify_op(&OperationType::DropDatabase), OpClass::Skip);
        assert_eq!(classify_op(&OperationType::Rename), OpClass::Skip);

        assert!(
            error_names_its_own_cause(&anyhow::anyhow!(
                "change stream event exceeded the 16 MB BSON limit"
            )),
            "an error that already names its own cause must suppress the generic \
             hint — burying it is what sent an operator to check a topology that \
             was fine"
        );
        assert!(
            !error_names_its_own_cause(&anyhow::anyhow!("connection refused")),
            "an opaque driver error needs the setup hint; suppressing it there \
             leaves the operator with nothing actionable"
        );
    }

    /// The oversize predicate recognises EITHER spelling, and the capability string tells the truth about the version it describes.
    #[test]
    fn a_standalone_server_is_named_by_either_spelling_of_40573() {
        assert!(super::names_standalone(
            "Kind: Command failed: Error code 40573 (Location40573): The $changeStream stage \
             is only supported on replica sets, labels: {}"
        ));
        assert!(super::names_standalone("Error code 40573 (Location40573)"));
        assert!(super::names_standalone(
            "The $changeStream stage is only supported on replica sets"
        ));
        assert!(!super::names_standalone(
            "Kind: Command failed: Error code 286 (ChangeStreamHistoryLost)"
        ));
        assert!(super::STANDALONE_MESSAGE.contains("`mode: full`"));
    }

    /// Error 286 by its code name is a log gap, worded as one and ending in the re-baseline remedy.
    #[test]
    fn a_lost_change_stream_history_is_named_and_worded_as_a_log_gap() {
        const LOST: &str = "Kind: Command failed: Error code 286 (ChangeStreamHistoryLost): \
                            Resume of change stream was not possible, as the resume point may \
                            no longer be in the oplog., labels: {}";
        assert!(super::names_history_lost(LOST));
        assert!(!super::names_history_lost(
            "Kind: Command failed: Error code 40573 (Location40573): The $changeStream stage \
             is only supported on replica sets"
        ));
        assert_eq!(
            super::history_lost_message(LOST),
            format!(
                "mongodb cdc: the oplog no longer holds the checkpoint's resume token — it \
                 rolled past the checkpoint, and the changes between the token and the oldest \
                 oplog entry are no longer in the log (server error: {LOST}). {}",
                crate::source::cdc::checkpoint_identity::RECOVER
            )
        );
    }

    #[test]
    fn the_oversize_predicate_and_the_capability_string_say_what_they_mean() {
        assert!(super::names_oversize_event(
            "Executor error during getMore :: caused by :: BSONObj size: 18874897 is invalid"
        ));
        assert!(super::names_oversize_event(
            "PlanExecutor error during aggregation :: BSONObjectTooLarge"
        ));
        assert!(
            !super::names_oversize_event("not authorized on db to execute command"),
            "an unrelated error must NOT be diagnosed as oversize — that would send \
             an operator to shrink documents over a permissions problem"
        );

        let cap = |major: u32| super::MongoCdcCapability {
            server_version: format!("{major}.0.0"),
            major,
            is_replica_set: true,
        };
        let six = cap(6);
        let five = cap(5);
        assert!(
            six.tier().contains("6.0+") && six.tier().to_uppercase().contains("DELETE"),
            "6.0+ must declare what actually rides — DELETE pre-images, not update \
             ones, which the decode never consults: {}",
            six.tier()
        );
        assert!(
            five.tier().contains("UpdateLookup"),
            "and below 6.0 it must say the post-images are current-state: {}",
            five.tier()
        );
        assert_ne!(
            six.tier(),
            five.tier(),
            "the two tiers must not render identically, or the declaration carries \
             no information about the server it describes"
        );
    }

    /// The pre-image request goes out from 6.0 and when the version is unprobed, never below 6.0.
    #[test]
    fn the_pre_image_request_follows_the_server_version() {
        let req = |version: &str, major: u32| {
            super::MongoCdcCapability {
                server_version: version.to_string(),
                major,
                is_replica_set: true,
            }
            .before_image_request()
        };
        for (version, major) in [("4.4.30", 4), ("5.0.33", 5)] {
            assert!(
                req(version, major).is_none(),
                "{version} has no pre-image to ask for"
            );
        }
        for (version, major) in [("6.0.0", 6), ("7.0.37", 7), ("8.0.13", 8), ("unknown", 0)] {
            assert!(
                matches!(
                    req(version, major),
                    Some(mongodb::options::FullDocumentBeforeChangeType::WhenAvailable)
                ),
                "{version} keeps the WhenAvailable request"
            );
        }
    }

    use super::*;
    use serde_json::json;

    /// The exact input libFuzzer crashed on (nightly Fuzz, run 31998520492, 2026-08-17).
    #[test]
    fn decode_resume_token_survives_foreign_keys_beside_data() {
        let raw = r##"{"rz~tt":"t'" ,    "/":{"":{"":{"":{"t":{}}}}},   " t":"t'" ,    "RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR   ~vrtt":"t'" ,    "_data":"8" ,    " jjjjjjjjjjjr~tt":    "_data'" ,    "   rt":" E" }"##;
        let v: serde_json::Value = serde_json::from_str(raw).expect("corpus input is valid JSON");
        let token = decode_resume_token(&v)
            .expect("a `_data` string is a resume token whatever else the file carries");
        let round: serde_json::Value = serde_json::to_value(&token).unwrap();
        assert_eq!(round.get("_data").and_then(|d| d.as_str()), Some("8"));
    }

    /// The `rt` branch is the SIBLING of the `_data` one and was never fuzzed past the hex gate.
    #[test]
    fn decode_resume_token_does_not_panic_on_wellformed_bson_of_the_wrong_shape() {
        let mut doc = Document::new();
        doc.insert("x", mongodb::bson::doc! { "y": 1i32 });
        let mut buf = Vec::new();
        doc.to_writer(&mut buf).unwrap();
        let hex: String = buf.iter().map(|b| format!("{b:02x}")).collect();
        let _ = decode_resume_token(&json!({ "rt": hex }));
    }

    #[test]
    fn decode_resume_token_rejects_malformed_shapes_without_panicking() {
        for bad in [
            json!({"rt": {}}),
            json!({"rt": 5}),
            json!({"_data": {}}),
            json!({"_data": 7}),
            json!({}),
            json!([]),
            json!("scalar"),
        ] {
            assert!(
                decode_resume_token(&bad).is_err(),
                "malformed resume token must be a clean Err, not a panic: {bad}"
            );
        }
    }

    /// #161: Mongo's until_current stop rules as pure transitions, both directions each.
    #[test]
    fn idle_poll_stops_both_directions() {
        let at = |time, increment| Some(mongodb::bson::Timestamp { time, increment });
        let now = std::time::Duration::ZERO;
        let past = "82000000640000000A";
        let bound_met = at(100, 10);
        assert!(idle_poll_stops(Some(past), Some("81"), bound_met, now));
        assert!(idle_poll_stops(Some(past), Some("81"), at(100, 9), now));
        assert!(idle_poll_stops(Some(past), Some("81"), None, now));
        assert!(idle_poll_stops(None, None, bound_met, now));
        assert!(idle_poll_stops(Some(past), None, bound_met, now));
        assert!(!idle_poll_stops(Some(past), Some("81"), at(100, 11), now));
        assert!(!idle_poll_stops(Some(past), Some("81"), at(101, 0), now));
        assert!(!idle_poll_stops(Some("81"), Some(past), bound_met, now));
        assert!(!idle_poll_stops(Some(past), Some(past), bound_met, now));
        assert!(!idle_poll_stops(None, Some(past), bound_met, now));
    }

    /// A token that never reaches the bound ends the drain once the ceiling has passed, and never before.
    #[test]
    fn a_token_that_never_reaches_the_bound_stops_the_drain_at_the_ceiling() {
        let at = |time, increment| Some(mongodb::bson::Timestamp { time, increment });
        let secs = std::time::Duration::from_secs;
        let past = "82000000640000000A";
        let never = at(101, 0);
        assert!(!idle_poll_stops(Some(past), Some("81"), never, secs(0)));
        assert!(!idle_poll_stops(Some(past), Some("81"), never, secs(119)));
        assert!(idle_poll_stops(
            Some(past),
            Some("81"),
            never,
            BOUND_WAIT_CEILING
        ));
        assert!(idle_poll_stops(Some(past), Some("81"), never, secs(3600)));
        assert!(!idle_poll_stops(Some("81"), Some(past), never, secs(3600)));
        assert!(!idle_poll_stops(None, Some(past), never, secs(3600)));
        assert_eq!(BOUND_WAIT_CEILING, secs(120));
        assert!(bound_unmet(Some(past), Some("81"), never));
        assert!(!bound_unmet(Some(past), Some("81"), at(100, 10)));
        assert!(!bound_unmet(Some(past), Some("81"), None));
        assert!(!bound_unmet(Some("81"), Some(past), never));
        assert!(!bound_unmet(Some(past), Some(past), never));
        assert!(!bound_unmet(None, Some(past), never));
        assert!(!bound_unmet(Some(past), None, never));
    }

    /// Only a `mongos` answer marks the connection as sharded.
    #[test]
    fn only_a_mongos_hello_marks_the_connection_sharded() {
        use mongodb::bson::doc;
        assert!(is_mongos(&doc! { "msg": "isdbgrid", "ok": 1 }));
        assert!(!is_mongos(
            &doc! { "isWritablePrimary": true, "setName": "rs0", "ok": 1 }
        ));
        assert!(!is_mongos(&doc! { "msg": "other" }));
        assert!(!is_mongos(&doc! {}));
    }

    /// A sharded drain stops only on a token past the bound's own tick; a replica set stops at it.
    #[test]
    fn a_sharded_drain_needs_a_token_past_the_bound_tick() {
        let at = |time, increment| Some(mongodb::bson::Timestamp { time, increment });
        let bound = at(100, 10);
        assert_eq!(token_floor(bound, false), bound);
        assert_eq!(token_floor(bound, true), at(100, 11));
        assert_eq!(token_floor(at(100, u32::MAX), true), at(101, 0));
        assert_eq!(token_floor(at(u32::MAX, u32::MAX), true), at(u32::MAX, 0));
        assert_eq!(token_floor(None, true), None);
        assert_eq!(token_floor(None, false), None);
        let at_bound = "82000000640000000A";
        assert!(idle_poll_stops(
            Some(at_bound),
            Some("81"),
            token_floor(bound, false),
            std::time::Duration::ZERO
        ));
        assert!(!idle_poll_stops(
            Some(at_bound),
            Some("81"),
            token_floor(bound, true),
            std::time::Duration::ZERO
        ));
        assert!(idle_poll_stops(
            Some("82000000640000000B"),
            Some("81"),
            token_floor(bound, true),
            std::time::Duration::ZERO
        ));
        assert!(bound_unmet(
            Some(at_bound),
            Some("81"),
            token_floor(bound, true)
        ));
    }

    /// A token whose clusterTime is below the open-time bound has not drained the backlog, whatever it compares to the target.
    #[test]
    fn a_token_below_the_open_time_bound_has_not_reached_it() {
        let at = |time, increment| Some(mongodb::bson::Timestamp { time, increment });
        let token = "826AE5B2FD000000EC2B042C01002B026E5A1004";
        assert!(token_reached_bound(token, at(0x6AE5B2FD, 0xEC)));
        assert!(!token_reached_bound(token, at(0x6AE5B2FD, 0xED)));
        assert!(!token_reached_bound(token, at(0x6AE5B2FE, 0)));
        assert!(token_reached_bound(token, None));
        assert!(token_reached_bound("not-a-token", at(1, 0)));
        assert!(token_reached_bound("82", at(1, 0)));
    }

    /// `until_current` must REFUSE without its bound, and only then.
    #[test]
    fn until_current_refuses_when_the_cluster_time_bound_is_unavailable() {
        use mongodb::bson::Timestamp;
        let ts = Timestamp {
            time: 1_700_000_000,
            increment: 3,
        };

        assert_eq!(
            until_current_bound(true, Some(ts), None).unwrap(),
            Some(ts),
            "the ordinary case must pass the bound through untouched"
        );
        assert_eq!(
            until_current_bound(false, None, None).unwrap(),
            None,
            "`until_current: false` streams forever by design and needs no ceiling"
        );
        assert_eq!(
            until_current_bound(false, Some(ts), None).unwrap(),
            None,
            "the daemon must not carry a bound even when one is available — a stray \
             ceiling would end a stream that is supposed to run continuously"
        );

        let err = until_current_bound(true, None, None)
            .expect_err("a bounded run with no ceiling must refuse, not run forever")
            .to_string();
        assert!(
            err.to_lowercase().contains("never terminate") && err.contains("until_current: false"),
            "the refusal must say what would happen (the run does not end — it does \
             not merely finish late, the way the SQL engines would) AND name the \
             setting that accepts continuous streaming instead: {err}"
        );

        let with_cause = until_current_bound(
            true,
            None,
            Some("CommandNotFound (59): no such command: 'hello'".to_string()),
        )
        .expect_err("still a refusal")
        .to_string();
        assert!(
            with_cause.contains("no such command: 'hello'"),
            "the driver's own error must reach the operator — it names the cause in \
             one line where a generic message sends them to check permissions they \
             never had a problem with: {with_cause}"
        );
    }

    #[test]
    fn past_time_bound_both_directions() {
        use mongodb::bson::Timestamp;
        let t = |time: u32| Some(Timestamp { time, increment: 0 });
        assert!(past_time_bound(true, t(101), t(100)));
        assert!(!past_time_bound(true, t(100), t(100)));
        assert!(!past_time_bound(true, t(99), t(100)));
        assert!(!past_time_bound(false, t(101), t(100)));
        assert!(!past_time_bound(true, t(101), None));
        assert!(!past_time_bound(true, None, t(100)));
    }
}

#[cfg(test)]
mod refusal_text_tests {
    use super::*;
    use crate::source::cdc::checkpoint_identity::RECOVER;

    /// A removed capture names what was removed and how, says why skipping is wrong, and ends in the recovery steps.
    #[test]
    fn a_removed_capture_names_what_was_removed_the_harm_and_the_recovery() {
        let said = dropped_capture_message(&OperationType::Drop, "shop", Some("orders"));
        assert!(
            said.starts_with(
                "mongodb cdc: captured collection `shop.orders` was removed by `Drop`"
            ),
            "{said}"
        );
        assert!(
            said.contains("leave every document it held live in the destination"),
            "{said}"
        );
        assert!(said.ends_with(RECOVER), "{said}");
        let whole = dropped_capture_message(&OperationType::DropDatabase, "shop", None);
        assert!(
            whole
                .starts_with("mongodb cdc: captured database `shop` was removed by `DropDatabase`"),
            "{whole}"
        );
        assert!(whole.ends_with(RECOVER), "{whole}");
    }
}

#[cfg(test)]
mod ordinal_tests {
    use super::*;
    use serde_json::json;

    fn run(clock: &mut OrdinalClock, cts: &[u64]) -> Vec<(u64, u64)> {
        cts.iter().map(|ct| clock.stamp(*ct)).collect()
    }

    /// Events sharing a clusterTime are numbered from zero; a new clusterTime restarts the count.
    #[test]
    fn the_clock_counts_events_sharing_a_cluster_time_from_zero() {
        let mut c = OrdinalClock::default();
        assert_eq!(
            run(&mut c, &[5, 5, 5, 6, 7, 7]),
            vec![(5, 0), (5, 1), (5, 2), (6, 0), (7, 0), (7, 1)]
        );
    }

    /// A restart from any event's persisted position stamps the rest exactly as the unbroken stream did.
    #[test]
    fn a_clock_restored_from_any_checkpoint_stamps_the_rest_as_the_unbroken_stream_did() {
        let cts = [5u64, 5, 5, 6, 6, 7, 8, 8, 8, 8];
        let whole = run(&mut OrdinalClock::default(), &cts);
        for cut in 1..cts.len() {
            let (ct, ord) = whole[cut - 1];
            let mut resumed = OrdinalClock::from_checkpoint(&json!({ "ct": ct, "ord": ord }));
            assert_eq!(run(&mut resumed, &cts[cut..]), whole[cut..], "cut {cut}");
        }
    }

    /// A checkpoint written before the position carried `ct` and `ord` starts the clock at zero.
    #[test]
    fn a_checkpoint_without_ct_and_ord_starts_the_clock_at_zero() {
        for old in [
            json!({ "_data": "826A" }),
            json!({ "_data": "826A", "rt": "0e" }),
        ] {
            let mut c = OrdinalClock::from_checkpoint(&old);
            assert_eq!(c, OrdinalClock::default());
            assert_eq!(c.stamp(9), (9, 0));
        }
        let mut anchor = OrdinalClock::from_checkpoint(&json!({ "ct": 0, "ord": 0 }));
        assert_eq!(anchor.stamp(1), (1, 0));
    }

    /// A clusterTime is its seconds in the high half and its increment in the low half.
    #[test]
    fn an_event_without_a_cluster_time_is_refused_with_the_server_side_cause() {
        let mut clock = OrdinalClock::default();
        let err = stamp_event(&mut clock, None).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_SOURCE_CDC_PREREQUISITE")
        );
        assert_eq!(
            err.to_string(),
            "mongodb cdc: the server sent a change event with no clusterTime, which every change event of MongoDB 4.4 and later carries; rivet cannot order that event against the ones around it, so nothing after it was read. Point `source.url` at a MongoDB 4.4 or later replica set or sharded cluster, then re-run"
        );
        let t = |time, increment| mongodb::bson::Timestamp { time, increment };
        assert_eq!(
            stamp_event(&mut clock, Some(t(1, 2))).unwrap(),
            (cluster_time_u64(t(1, 2)), 0)
        );
        assert_eq!(
            stamp_event(&mut clock, Some(t(1, 2))).unwrap(),
            (cluster_time_u64(t(1, 2)), 1)
        );
    }

    #[test]
    fn a_cluster_time_packs_seconds_over_increment() {
        let t = |time, increment| mongodb::bson::Timestamp { time, increment };
        assert_eq!(cluster_time_u64(t(1, 2)), (1 << 32) | 2);
        assert_eq!(cluster_time_u64(t(0, u32::MAX)), u64::from(u32::MAX));
        assert_eq!(cluster_time_u64(t(u32::MAX, u32::MAX)), u64::MAX);
    }

    /// The largest clusterTime survives the checkpoint's JSON as an unsigned integer.
    #[test]
    fn the_largest_cluster_time_survives_the_checkpoint_json() {
        let token = decode_resume_token(&json!({ "_data": "826A" })).unwrap();
        let pos = encode_resume_token(&token, (u64::MAX, u64::MAX - 1)).unwrap();
        let text = pos.json().to_string();
        let back: serde_json::Value = serde_json::from_str(&text).unwrap();
        let c = OrdinalClock::from_checkpoint(&back);
        assert_eq!(
            c,
            OrdinalClock {
                ct: u64::MAX,
                ord: u64::MAX - 1
            }
        );
    }

    /// The persisted shapes: the old pair, the new quadruple, and an anchor at zero; each still decodes to its token.
    #[test]
    fn the_position_shapes_a_checkpoint_may_hold_all_decode_to_the_same_token() {
        let token = decode_resume_token(&json!({ "_data": "826A4E" })).unwrap();
        let stamped = encode_resume_token(&token, (7, 3)).unwrap();
        let anchor = encode_resume_token(&token, (0, 0)).unwrap();
        let mut unstamped = stamped.json().clone();
        unstamped
            .as_object_mut()
            .unwrap()
            .retain(|k, _| k != "ct" && k != "ord");
        let old = Position::new(unstamped);
        let keys = |p: &Position| {
            let mut k: Vec<String> = p.json().as_object().unwrap().keys().cloned().collect();
            k.sort();
            k
        };
        assert_eq!(keys(&old), ["_data", "rt"]);
        assert_eq!(keys(&stamped), ["_data", "ct", "ord", "rt"]);
        assert_eq!(stamped.json()["ct"], 7);
        assert_eq!(stamped.json()["ord"], 3);
        assert_eq!(anchor.json()["ct"], 0);
        assert_eq!(anchor.json()["ord"], 0);
        let want = serde_json::to_value(&token).unwrap();
        for p in [&old, &stamped, &anchor] {
            let got = decode_resume_token(p.json()).unwrap();
            assert_eq!(serde_json::to_value(&got).unwrap(), want);
        }
        let legacy = decode_resume_token(&json!({ "_data": "826A4E" })).unwrap();
        assert_eq!(serde_json::to_value(&legacy).unwrap(), want);
    }
}

#[cfg(test)]
mod error_code_tests {
    /// A checkpoint of a shape no rivet wrote is the checkpoint refusal (exit 5).
    #[test]
    fn an_unrecognised_resume_token_checkpoint_is_refused_by_code() {
        let err = super::decode_resume_token(&serde_json::json!({ "foo": 1 })).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_CHECKPOINT_INVALID", 5);
    }

    /// The first event fixes the bracket; a different one warns once and never again.
    #[test]
    fn a_second_id_bracket_warns_once() {
        let mut first = None;
        assert!(!super::hetero_should_warn(&mut first, false, Some(0)));
        assert_eq!(first, Some(Some(0)));
        assert!(!super::hetero_should_warn(&mut first, false, Some(0)));
        assert!(super::hetero_should_warn(&mut first, false, Some(2)));
        assert!(!super::hetero_should_warn(&mut first, true, Some(2)));
        let mut unrecorded = None;
        assert!(!super::hetero_should_warn(&mut unrecorded, true, Some(0)));
        assert_eq!(unrecorded, None);
    }

    /// Anchoring against a server that cannot be reached fails and pins nothing.
    #[test]
    fn anchoring_an_unreachable_server_fails_and_writes_no_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("o.ckpt");
        let url = "mongodb://u:p@127.0.0.1:1/app?serverSelectionTimeoutMS=300";
        super::pin_checkpoint_at_current(url, None, &ckpt).unwrap_err();
        assert!(!ckpt.exists());
    }
}
