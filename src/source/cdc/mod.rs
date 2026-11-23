//! Canonical change-data-capture types + driver, shared across engines.

#![allow(dead_code)]

pub(crate) mod checkpoint_identity;
pub(crate) mod identity;
pub(crate) mod order;
pub(crate) mod partition_guard;
/// The routing rule under the name the engines call it by; the file sink is `pipeline::cdc::sink`.
pub(crate) mod sink {
    pub(crate) use super::identity::table_matches;
}
pub(crate) mod spill;
pub(crate) mod tx_buffer;
pub(crate) mod validate;
pub(crate) mod value;

use std::path::{Path, PathBuf};

use serde_json::Value as Json;

use crate::error::Result;
use value::RivetValue;

/// The cap a `RIVET_CDC_MAX_TX_*` override resolves to — the pure half of [`max_tx_rows`] and [`max_tx_bytes`].
pub(crate) fn tx_cap_from_env(raw: Option<&str>, default: usize) -> usize {
    raw.and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// The refusal for a PARTIAL change image: `names` columns but `n` values.
pub(crate) fn partial_image_refusal(
    schema: &str,
    table: &str,
    n: usize,
    names: usize,
) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::SOURCE_CDC_PREREQUISITE,
        format!(
            "cdc: {schema}.{table}: a row image carries {n} value(s) under {names} column \
             name(s) — a PARTIAL image, which rivet cannot map without \
             fabricating the missing cells as NULL. On MySQL this is \
             `binlog_row_image` set to something other than FULL when the \
             change was WRITTEN (a session override counts, and the open-time \
             probe reads only the global). {}{}",
            seeded!(
                "partial-image-refusal-names-a-remedy-that-loops",
                "Set `binlog_row_image = FULL` and \
                 re-capture from before the affected DDL/DML; the changes are \
                 still in the binlog, so this is a delay rather than a loss.",
                "Setting `binlog_row_image = FULL` fixes only the \
                 changes written after it: this one stays in the binlog, and every run that \
                 resumes before it meets it again. Set FULL for every writer, then re-baseline: "
            ),
            seeded!(
                "partial-image-refusal-names-a-remedy-that-loops",
                "",
                checkpoint_identity::RECOVER
            )
        ),
    )
}

/// The refusal of a stream whose image does not carry every column of a captured table, at open and at the end of a drain.
pub(crate) fn open_partial_image_refusal(engine: CdcEngine, why: &str) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::SOURCE_CDC_PREREQUISITE,
        format!(
            "{} cdc: {why}. Capturing under this setting would report success over events that \
             cannot represent the row.",
            engine.label()
        ),
    )
}

/// `Some((values, names))` when the image an event prints or writes, or its before image, carries a different number of values than the names it travels under, a PARTIAL image.
pub(crate) fn named_image_arity_mismatch(ev: &ChangeEvent) -> Option<(usize, usize)> {
    let image = ev.image_names.as_deref().and_then(|names| {
        let n = ev.after.as_ref().or(ev.before.as_ref()).map_or(0, Vec::len);
        (n != names.len()).then_some((n, names.len()))
    });
    if seeded!("cdc-stdout-partial-before-image", true, false) {
        return image;
    }
    image.or_else(|| {
        let names = ev.before_names.as_deref().or(ev.image_names.as_deref())?;
        let n = ev.before.as_ref()?.len();
        (n != names.len()).then_some((n, names.len()))
    })
}

/// Default row cap — 5M rows, far above any real OLTP transaction.
pub(crate) const DEFAULT_MAX_TX_ROWS: usize = 5_000_000;
/// Default byte cap — 2 GiB, likewise.
pub(crate) const DEFAULT_MAX_TX_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// Canonical DML kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeOp {
    Insert,
    Update,
    Delete,
}

impl ChangeOp {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ChangeOp::Insert => "insert",
            ChangeOp::Update => "update",
            ChangeOp::Delete => "delete",
        }
    }
}

impl ChangeOp {
    /// Does this op carry its values in the BEFORE image?
    pub(crate) fn values_live_in_before(self) -> bool {
        match self {
            ChangeOp::Delete => true,
            ChangeOp::Insert | ChangeOp::Update => false,
        }
    }
}

/// The prior-run evidence a caller checked before a missing server-side anchor is refused.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PriorRun {
    /// The checkpoint file holds a position.
    pub(crate) checkpoint: bool,
    /// A table's snapshot is recorded done (state DB `cdc_snapshot` row or `snapshot/_SUCCESS`).
    pub(crate) snapshot: bool,
}

impl PriorRun {
    /// Whether any evidence of a prior run was found.
    pub(crate) fn any(self) -> bool {
        self.checkpoint || self.snapshot
    }
}

/// An engine-shaped resume position — MySQL `{file, pos}`, a PostgreSQL LSN, a SQL Server LSN, a Mongo resume token, Oracle SCNs.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Position(pub(crate) std::sync::Arc<Json>);

impl Position {
    /// A position holding `json`; a clone shares it.
    pub(crate) fn new(json: Json) -> Self {
        Self(std::sync::Arc::new(json))
    }

    /// The engine-shaped value.
    pub(crate) fn json(&self) -> &Json {
        &self.0
    }

    /// Load a persisted checkpoint, or `None` on first run (absent).
    pub(crate) fn load(path: &Path) -> Result<Option<Self>> {
        match std::fs::read_to_string(path) {
            Ok(s) => match serde_json::from_str(&s) {
                Ok(json) => Ok(Some(Position::new(json))),
                Err(e) => Err(checkpoint_identity::checkpoint_invalid(format!(
                    "checkpoint '{}' is corrupt or truncated (not valid JSON: {e}) — refusing to \
                     silently treat it as absent and re-anchor CDC at 'current', which would \
                     permanently skip every change since the last checkpoint. Restore the file, \
                     or{}",
                    path.display(),
                    seeded!(
                        "corrupt-checkpoint-remedy-reanchors",
                        " delete it to accept a new anchor from a fresh snapshot.".to_string(),
                        format!(": {}", checkpoint_identity::RECOVER)
                    )
                ))),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => {
                Err(anyhow::Error::new(e)
                    .context(format!("reading checkpoint '{}'", path.display())))
            }
        }
    }

    /// Persist atomically (temp file + rename) so a crash never leaves a torn checkpoint that would resume from a corrupt position.
    pub(crate) fn save(&self, path: &Path) -> Result<()> {
        use anyhow::Context as _;
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating checkpoint directory '{}'", parent.display()))?;
        }
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let tmp = path.with_extension(format!("tmp.{}.{nanos:x}", std::process::id()));
        std::fs::write(&tmp, serde_json::to_vec(&self.0)?)
            .with_context(|| format!("writing checkpoint '{}'", path.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("committing checkpoint '{}'", path.display()))?;
        Ok(())
    }
}

/// One canonical row-level change.
#[derive(Debug, Clone)]
pub(crate) struct ChangeEvent {
    pub(crate) op: ChangeOp,
    pub(crate) schema: String,
    pub(crate) table: String,
    /// Pre-image — present for `Update`/`Delete` when the engine carries it.
    pub(crate) before: Option<Vec<RivetValue>>,
    /// Post-image — present for `Insert`/`Update` when the engine carries it.
    pub(crate) after: Option<Vec<RivetValue>>,
    /// Resume position after this change.
    pub(crate) position: Position,
    /// `true` if this is the last change in its source transaction.
    pub(crate) committed: bool,
    /// Column NAMES of this event's image, when the engine carries them (PostgreSQL wire text names every column.
    pub(crate) image_names: Option<std::sync::Arc<[String]>>,
    /// Ordinal of this change **within its source transaction** (0-based), stamped by [`TxnSeq`] as the stream is consumed.
    pub(crate) seq: u64,
    /// A DEFERRED per-event decode error, surfaced by the sink **only when this event routes to a captured table**.
    pub(crate) poison: Option<String>,
    /// The changed row's identity within its transaction (Oracle's ROWID where stable); `None` elsewhere.
    pub(crate) row_id: Option<String>,
    /// Names of `before`'s cells when they are not `image_names` (PostgreSQL's key-only pre-image).
    pub(crate) before_names: Option<std::sync::Arc<[String]>>,
    /// A deferred refusal of an undecodable old-image cell, raised only where the old image is written.
    pub(crate) before_poison: Option<String>,
    /// Labels of the ENUM and SET columns whose catalog spelling cannot be trusted, read from the binlog table map (MySQL only).
    pub(crate) image_labels: Option<std::sync::Arc<[value::WireLabels]>>,
}

/// Stamps each change with its intra-transaction ordinal ([`ChangeEvent::seq`]).
#[derive(Default)]
pub(crate) struct TxnSeq {
    counter: u64,
    prev: Option<Position>,
}

impl TxnSeq {
    /// Ordinal for a change at commit `position`.
    pub(crate) fn next(&mut self, position: &Position) -> u64 {
        if self.prev.as_ref() == Some(position) {
            self.counter += 1;
        } else {
            self.counter = 0;
            self.prev = Some(position.clone());
        }
        self.counter
    }

    pub(crate) fn stamp(&mut self, ev: &mut ChangeEvent) {
        ev.seq = self.next(&ev.position);
    }
}

/// Closes a buffered source transaction (#158).
pub(crate) struct TxnFramer;

impl TxnFramer {
    /// Frame one committed source transaction: `position = commit` on all, `committed = true` on the last event only.
    pub(crate) fn close_group(events: &mut [ChangeEvent], commit: &Position) {
        Self::close_head_of_group(events, commit, 0);
    }

    /// Close the IN-MEMORY head of a transaction whose tail is still on disk.
    pub(crate) fn close_head_of_group(
        events: &mut [ChangeEvent],
        commit: &Position,
        tail_len: usize,
    ) {
        let n = events.len();
        for (i, ev) in events.iter_mut().enumerate() {
            ev.position = commit.clone();
            ev.committed = tail_len == 0 && i + 1 == n;
        }
    }

    /// Close ONE event of a spilled tail: `at` is its index within the tail, `tail_len` the tail's length.
    pub(crate) fn close_tail_event(
        event: &mut ChangeEvent,
        commit: &Position,
        at: usize,
        tail_len: usize,
    ) {
        event.position = commit.clone();
        event.committed = at + 1 == tail_len;
    }

    /// Mark a stand-alone event as its own commit boundary (#158 — a NAMED decision, not a divergence).
    pub(crate) fn single_event_commit(event: &mut ChangeEvent) {
        event.committed = true;
    }
}

/// Resident cost of a `serde_json::Value`, for the CDC memory budgets.
fn json_resident_bytes(v: &serde_json::Value) -> usize {
    use serde_json::Value;
    /// MEASURED, not derived.
    const JSON_OBJECT_NODE_BYTES: usize = 448;
    match v {
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
        Value::String(s) => s.len(),
        Value::Array(a) => {
            a.capacity() * std::mem::size_of::<Value>()
                + a.iter().map(json_resident_bytes).sum::<usize>()
        }
        Value::Object(m) if m.is_empty() => 0,
        Value::Object(m) => {
            JSON_OBJECT_NODE_BYTES
                + m.iter()
                    .map(|(k, val)| k.len() + json_resident_bytes(val))
                    .sum::<usize>()
        }
    }
}

impl ChangeEvent {
    /// Rough in-memory footprint of this buffered change.
    pub(crate) fn payload_bytes(&self) -> usize {
        let img = |v: &Option<Vec<RivetValue>>| {
            v.as_ref()
                .map_or(0, |vs| vs.iter().map(RivetValue::payload_bytes).sum())
        };
        self.schema.len() + self.table.len() + img(&self.before) + img(&self.after)
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        let img = |v: &Option<Vec<RivetValue>>| {
            v.as_ref().map_or(0, |vs| {
                vs.capacity() * std::mem::size_of::<RivetValue>()
                    + vs.iter().map(RivetValue::estimated_bytes).sum::<usize>()
            })
        };
        let names = self.image_names.as_ref().map_or(0, |a| {
            let own: usize =
                a.len() * std::mem::size_of::<String>() + a.iter().map(String::len).sum::<usize>();
            own / std::sync::Arc::strong_count(a).max(1)
        });
        let before_names = self.before_names.as_ref().map_or(0, |a| {
            let own: usize =
                a.len() * std::mem::size_of::<String>() + a.iter().map(String::len).sum::<usize>();
            own / std::sync::Arc::strong_count(a).max(1)
        });
        let labels = self.image_labels.as_ref().map_or(0, |a| {
            let own: usize = a
                .iter()
                .map(|w| {
                    std::mem::size_of::<value::WireLabels>()
                        + w.column.len()
                        + w.labels.as_ref().map_or(0, |l| {
                            l.len() * std::mem::size_of::<String>()
                                + l.iter().map(String::len).sum::<usize>()
                        })
                })
                .sum();
            own / std::sync::Arc::strong_count(a).max(1)
        });
        let text = |s: &Option<String>| s.as_ref().map_or(0, String::len);
        std::mem::size_of::<Self>()
            + self.schema.len()
            + self.table.len()
            + text(&self.poison)
            + text(&self.before_poison)
            + text(&self.row_id)
            + names
            + before_names
            + labels
            + img(&self.before)
            + img(&self.after)
    }

    /// Surface this event's DEFERRED decode error ([`poison`](Self::poison)) if it carries one.
    pub(crate) fn raise_poison(&self) -> Result<()> {
        raise_deferred(&self.poison)
    }

    /// Surface the old image's deferred decode error; for a consumer that writes the old image.
    pub(crate) fn raise_before_poison(&self) -> Result<()> {
        raise_deferred(&self.before_poison)
    }
}

/// A deferred decode error as the run's failure.
fn raise_deferred(poison: &Option<String>) -> Result<()> {
    if let Some(poison) = poison {
        crate::rivet_bail!(crate::error::codes::SOURCE_CDC_CELL_UNSUPPORTED, "{poison}");
    }
    Ok(())
}

/// The seam every engine reader satisfies: a blocking pull of canonical changes.
pub(crate) trait ChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>>;

    /// What the capture can supply for `tables`, asked on this stream's own connection.
    fn row_image(&mut self, _tables: &[String]) -> RowImage {
        RowImage::Whole
    }

    /// Log this source retains on the reader's behalf, worth saying before the run; only a PostgreSQL slot pins log.
    fn retention_warnings(&mut self) -> Vec<String> {
        Vec::new()
    }

    /// Acknowledge that every change up to and including `position` is **durably persisted** at the destination.
    fn ack(&mut self, _position: &Position) -> Result<()> {
        Ok(())
    }

    /// The relation this stream's events will actually name, when the engine knows it from a CATALOG rather than from the configured string.
    fn resolved_identity(&self, _configured: &str) -> Option<(String, String)> {
        None
    }

    /// `columns_from: grants`: the columns of `configured` this run may deliver; `None` = all.
    fn readable_columns(&self, _configured: &str) -> Option<ReadableColumns> {
        None
    }

    /// After a clean drain, a position past everything read that nothing captured can precede.
    fn drained_frontier(&self) -> Option<Position> {
        None
    }

    /// The checkpoint to persist for `position`; an engine adds what verifies a later resume.
    fn checkpoint_of(&self, position: &Position) -> Position {
        position.clone()
    }

    /// Which engine this stream speaks — required, never defaulted.
    fn engine(&self) -> CdcEngine;
}

/// The first declared key column the table does not have, matched as images match names.
#[derive(Debug, Default)]
pub(crate) struct ResidentBytes {
    bytes: usize,
    at: Option<Position>,
}

impl ResidentBytes {
    /// Charge `ev`; its position is charged only when it is not the one the previous event holds.
    pub(crate) fn add(&mut self, ev: &ChangeEvent) {
        let shared = self
            .at
            .as_ref()
            .is_some_and(|p| std::sync::Arc::ptr_eq(&p.0, &ev.position.0));
        if !shared {
            self.bytes = self
                .bytes
                .saturating_add(json_resident_bytes(ev.position.json()));
            self.at = Some(ev.position.clone());
        }
        self.bytes = self.bytes.saturating_add(ev.estimated_bytes());
    }

    /// The bytes charged since the last reset.
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Start over: nothing is buffered.
    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }
}

pub(crate) fn undeclared_key_column<'k>(
    key: &'k [String],
    columns: &[crate::types::TypeMapping],
) -> Option<&'k str> {
    key.iter()
        .find(|k| {
            !columns
                .iter()
                .any(|c| c.column_name == **k || c.column_name.eq_ignore_ascii_case(k))
        })
        .map(String::as_str)
}

/// One `rivet cdc` stdout line: the change as the engine delivered it, never split.
pub(crate) fn ndjson_line(ev: &ChangeEvent) -> serde_json::Value {
    let to_json = |img: &Option<Vec<RivetValue>>| {
        img.as_ref()
            .map(|vs| vs.iter().map(RivetValue::to_json).collect::<Vec<_>>())
    };
    let mut line = serde_json::json!({
        "op": ev.op.as_str(),
        "schema": ev.schema,
        "table": ev.table,
        "before": to_json(&ev.before),
        "after": to_json(&ev.after),
        "pos": ev.position.json(),
        "seq": ev.seq,
    });
    if let Some(names) = &ev.before_names {
        line["before_columns"] = serde_json::json!(names.as_ref());
    }
    line
}

/// `rivet cdc` driver.
pub(crate) fn run(
    stream: &mut dyn ChangeStream,
    checkpoint: Option<PathBuf>,
    tables: Vec<String>,
    max_events: Option<usize>,
) -> Result<()> {
    if max_events == Some(0) {
        return Ok(());
    }
    if let RowImage::Partial { why } = seeded!(
        "cdc-stdout-partial-image",
        RowImage::Whole,
        stream.row_image(&tables)
    ) {
        return Err(open_partial_image_refusal(stream.engine(), &why));
    }
    let mut last_commit: Option<Position> = None;
    let drained = emit_ndjson(stream, &checkpoint, &tables, max_events, &mut last_commit);
    let acked = last_commit.map_or(Ok(()), |p| {
        seeded!("stdout-drain-never-acks", Ok(()), stream.ack(&p))
    });
    drained.and(acked)
}

/// The emit loop of [`run`]; `last_commit` ends at the last commit boundary whose lines were all printed.
fn emit_ndjson(
    stream: &mut dyn ChangeStream,
    checkpoint: &Option<PathBuf>,
    tables: &[String],
    max_events: Option<usize>,
    last_commit: &mut Option<Position>,
) -> Result<()> {
    let mut emitted = 0usize;
    let eng = stream.engine();
    let mut txn_seq = TxnSeq::default();
    while let Some(ev) = stream.next_change() {
        let mut ev = ev?;
        txn_seq.stamp(&mut ev);
        let committed = ev.committed;
        let filtered = !tables.is_empty()
            && !tables
                .iter()
                .any(|t| sink::table_matches(eng, t, &ev.schema, &ev.table));
        if filtered {
            if committed {
                if let Some(p) = checkpoint {
                    stream.checkpoint_of(&ev.position).save(p)?;
                }
                *last_commit = Some(ev.position.clone());
            }
            continue;
        }
        ev.raise_poison()?;
        ev.raise_before_poison()?;
        if let Some((n, names)) = seeded!(
            "cdc-stdout-partial-image",
            None,
            named_image_arity_mismatch(&ev)
        ) {
            return Err(partial_image_refusal(&ev.schema, &ev.table, n, names));
        }
        println!("{}", ndjson_line(&ev));
        emitted += 1;
        if committed {
            if let Some(p) = checkpoint {
                stream.checkpoint_of(&ev.position).save(p)?;
            }
            *last_commit = Some(ev.position.clone());
        }
        if committed && max_events.is_some_and(|m| emitted >= m) {
            if seeded!("capped-run-skips-remainder", true, false) {
                while let Some(Ok(rest)) = stream.next_change() {
                    if rest.committed
                        && let Some(p) = &checkpoint
                    {
                        stream.checkpoint_of(&rest.position).save(p)?;
                    }
                }
            }
            break;
        }
    }
    Ok(())
}

/// The columns a `columns_from: grants` run delivers for one table, and the `SELECT` that reads them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadableColumns {
    /// The readable columns, in table order.
    pub columns: Vec<String>,
    /// A `SELECT` of exactly those columns, every identifier quoted.
    pub select: String,
}

/// The per-engine CDC knobs — each variant carries ONLY the parameters its engine reads, so the live set is obvious at a glance (a MySQL run has no `slot`, a Mongo run has no `server_id`) and a new engine adds a self-contained variant instead of a field every other engine ignores.
#[derive(Debug, Clone)]
pub(crate) enum CdcEngineOpts {
    /// MySQL replica id (the binlog `server_id`).
    Mysql {
        server_id: u32,
        /// The `table:` values the CONFIG asked for.
        configured_tables: Vec<String>,
        /// The grants the run resolved before its anchor.
        resolved_grants: Option<std::sync::Arc<std::collections::HashMap<String, ReadableColumns>>>,
    },
    /// PostgreSQL logical slot name.
    Postgres {
        slot: String,
        /// The `table:` values the CONFIG asked for.
        configured_tables: Vec<String>,
        /// The grants the run resolved before its anchor.
        resolved_grants: Option<std::sync::Arc<std::collections::HashMap<String, ReadableColumns>>>,
    },
    /// SQL Server capture instance (required for `sqlserver://`).
    Mssql {
        capture_instance: Option<String>,
        /// The `table:` values the CONFIG asked for.
        configured_tables: Vec<String>,
        /// The grants the run resolved before its anchor.
        resolved_grants: Option<std::sync::Arc<std::collections::HashMap<String, ReadableColumns>>>,
    },
    Oracle {
        configured_tables: Vec<String>,
        /// The grants the run resolved before its anchor.
        resolved_grants: Option<std::sync::Arc<std::collections::HashMap<String, ReadableColumns>>>,
    },
    Mongo {
        canonical: bool,
        /// The export's configured `table:` names.
        configured_tables: Vec<String>,
        /// The collections a keyed `load:` merges by `_id`.
        keyed_tables: Vec<String>,
        /// The views the run resolved before its anchor.
        resolved_grants: Option<std::sync::Arc<std::collections::HashMap<String, ReadableColumns>>>,
    },
}

impl CdcEngineOpts {
    /// The engine these opts configure — the identity `source.type` chose, never re-read from the URL.
    pub(crate) fn engine(&self) -> CdcEngine {
        match self {
            Self::Mysql { .. } => CdcEngine::Mysql,
            Self::Postgres { .. } => CdcEngine::Postgres,
            Self::Mssql { .. } => CdcEngine::Mssql,
            Self::Oracle { .. } => CdcEngine::Oracle,
            Self::Mongo { .. } => CdcEngine::Mongo,
        }
    }
}

/// How a capture run ends — ONE name for the concept that used to cross the adapter seam as three differently-aliased bools (`bound_at_open`, `non_block`, `until_current`), and the canonical home of the bounded run's termination contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainMode {
    BoundedAtOpen,
    Continuous,
}

impl DrainMode {
    /// The user-facing surface stays a bool (`cdc.until_current`, default true.
    pub(crate) fn from_until_current(until_current: bool) -> Self {
        if until_current {
            DrainMode::BoundedAtOpen
        } else {
            DrainMode::Continuous
        }
    }

    pub(crate) fn is_bounded(self) -> bool {
        matches!(self, DrainMode::BoundedAtOpen)
    }
}

/// Memory backstop shared by every log/poll adapter.
pub(crate) fn max_tx_rows() -> usize {
    use std::sync::OnceLock;
    static CELL: OnceLock<usize> = OnceLock::new();
    *CELL.get_or_init(|| {
        tx_cap_from_env(
            std::env::var("RIVET_CDC_MAX_TX_ROWS").ok().as_deref(),
            DEFAULT_MAX_TX_ROWS,
        )
    })
}

/// Byte sibling of [`max_tx_rows`] (round-2 audit #9).
pub(crate) fn max_tx_bytes() -> usize {
    use std::sync::OnceLock;
    static CELL: OnceLock<usize> = OnceLock::new();
    *CELL.get_or_init(|| {
        tx_cap_from_env(
            std::env::var("RIVET_CDC_MAX_TX_BYTES").ok().as_deref(),
            DEFAULT_MAX_TX_BYTES,
        )
    })
}

/// Connection + resume parameters for `rivet cdc`, across engines.
pub(crate) struct CdcConfig {
    pub url: crate::secret::Secret,
    /// MySQL checkpoint file (PG resumes via the slot; SQL Server via its LSN; MongoDB via the resume token).
    pub checkpoint: Option<PathBuf>,
    /// The CONFIG's directory — the anchor for every relative path this run resolves (checkpoint already arrives resolved.
    pub config_dir: PathBuf,
    /// How this capture run ends — see [`DrainMode`], the canonical home of the termination contract.
    pub drain: DrainMode,
    /// Transport security, applied by every adapter through the same `require_tls_or_loopback` gate the batch path uses (refuse remote plaintext / unauthenticated TLS).
    pub tls: Option<crate::config::TlsConfig>,
    /// The engine + its knobs — the CDC engine identity for dispatch.
    pub engine: CdcEngineOpts,
}

/// The sink's ACK CADENCE, handed to a poll adapter to size one peek.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PeekBound {
    /// The sink's part `rollover` — one ack cadence per peek.
    Sized(usize),
    /// One peek pulls the whole backlog (the non-acking NDJSON path).
    Unbounded,
}

impl PeekBound {
    /// Resolve to a positive `upto_nchanges`-style row cap the adapters clamp to their SQL arg width.
    pub(crate) fn rows_capped(self) -> usize {
        match self {
            PeekBound::Sized(n) => n.clamp(1, i32::MAX as usize),
            PeekBound::Unbounded => i32::MAX as usize,
        }
    }
}

/// The CDC engine, resolved ONCE — from `source.type` on a config run, from the URL scheme only for `rivet cdc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CdcEngine {
    Mysql,
    Postgres,
    Mssql,
    Mongo,
    Oracle,
}

impl From<crate::config::SourceType> for CdcEngine {
    fn from(t: crate::config::SourceType) -> Self {
        use crate::config::SourceType;
        match t {
            SourceType::Mysql => Self::Mysql,
            SourceType::Postgres => Self::Postgres,
            SourceType::Mssql => Self::Mssql,
            SourceType::Mongo => Self::Mongo,
            SourceType::Oracle => Self::Oracle,
        }
    }
}

/// What image of a row this source can actually supply per change event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RowImage {
    /// Every column, on every event.
    Whole,
    /// Inserts and updates carry the whole new row; DELETE carries only the key.
    KeyOnlyDeletes { why: String },
    /// The event cannot represent the row.
    Partial { why: String },
}
/// The native column TYPE a catalog listing gives for `column`.
pub(crate) fn native_type_for<'a>(
    catalog: &'a [(String, String)],
    column: &str,
) -> Option<&'a str> {
    catalog
        .iter()
        .find(|(name, _)| name == column)
        .map(|(_, ty)| ty.as_str())
}

impl CdcEngine {
    /// Does this engine make the SERVER retain log on the reader's behalf?
    pub(crate) fn pins_log_for_reader(self) -> bool {
        match self {
            Self::Postgres => true,
            Self::Mysql | Self::Mssql | Self::Mongo | Self::Oracle => false,
        }
    }
}

impl CdcEngine {
    /// The engine a URL's scheme names — for `rivet cdc`, which has a URL and no `source.type`.
    pub(crate) fn from_url(url: &str) -> Result<Self> {
        let Some(t) = crate::config::SourceType::from_url_scheme(url) else {
            crate::rivet_bail!(
                crate::error::codes::CONFIG_SOURCE_URL_INVALID,
                "rivet cdc: unsupported source url — expected mysql:// / postgresql:// / sqlserver:// / mongodb:// / oracle://"
            )
        };
        Ok(t.into())
    }

    /// The `source.type` this engine reads.
    pub(crate) fn source_type(self) -> crate::config::SourceType {
        use crate::config::SourceType;
        match self {
            Self::Mysql => SourceType::Mysql,
            Self::Postgres => SourceType::Postgres,
            Self::Mssql => SourceType::Mssql,
            Self::Mongo => SourceType::Mongo,
            Self::Oracle => SourceType::Oracle,
        }
    }

    /// Stable lowercase label for metrics / run records / hints.
    pub(crate) fn label(self) -> &'static str {
        self.source_type().label()
    }

    /// Refuse a source the capture open would refuse, before the anchor or any snapshot part is written.
    pub(crate) fn refuse_unmet_prerequisites(
        self,
        url: &str,
        tls: Option<&crate::config::TlsConfig>,
        tables: &[String],
        drain: DrainMode,
        columns_from_grants: bool,
    ) -> Result<()> {
        match self {
            Self::Mysql => {
                crate::source::mysql::cdc::MysqlChangeStream::refuse_unmet_prerequisites(
                    url,
                    tls,
                    tables,
                    columns_from_grants,
                )
            }
            Self::Postgres => {
                crate::source::postgres::cdc::PgChangeStream::refuse_unmet_prerequisites(
                    url,
                    tls,
                    tables,
                    drain,
                    columns_from_grants,
                )
            }
            #[cfg(feature = "oracle")]
            Self::Oracle => {
                crate::source::oracle::cdc::refuse_unmet_prerequisites(url, tls, tables)
            }
            #[cfg(not(feature = "oracle"))]
            Self::Oracle => Err(crate::source::oracle_feature_missing()),
            Self::Mongo if columns_from_grants => {
                crate::source::mongo::column_grants::resolve_for_tables(url, tls, tables)
                    .map(|_| ())
            }
            Self::Mssql | Self::Mongo => Ok(()),
        }
    }

    /// Ensure the resume anchor EXISTS — `initial.
    pub(crate) fn ensure_anchor(
        self,
        url: &str,
        slot: &str,
        capture_instance: Option<&str>,
        checkpoint: Option<&std::path::Path>,
        tls: Option<&crate::config::TlsConfig>,
        prior: PriorRun,
    ) -> Result<()> {
        match self {
            Self::Postgres => {
                drop(crate::source::postgres::cdc::PgChangeStream::open(
                    url,
                    slot,
                    prior,
                    tls,
                    PeekBound::Unbounded,
                    DrainMode::Continuous,
                    &[],
                    None,
                    None,
                    None,
                )?);
                Ok(())
            }
            Self::Mysql | Self::Mssql | Self::Mongo | Self::Oracle => {
                let ckpt = checkpoint.ok_or_else(|| {
                    crate::error::coded(
                        crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                        format!(
                            "{} cdc: an anchor needs cdc.checkpoint (no server-side anchor exists)",
                            self.label()
                        ),
                    )
                })?;
                if Position::load(ckpt)?.is_some() {
                    return Ok(());
                }
                if prior.any() {
                    return Err(checkpoint_identity::checkpoint_invalid(format!(
                        "{} cdc: checkpoint '{}' is missing but prior-run evidence exists — \
                         restore the checkpoint file, or: {}",
                        self.label(),
                        ckpt.display(),
                        checkpoint_identity::RECOVER
                    )));
                }
                match self {
                    Self::Mysql => {
                        crate::source::mysql::cdc::MysqlChangeStream::pin_checkpoint_at_current(
                            url, ckpt, tls,
                        )
                    }
                    Self::Mssql => crate::source::mssql::cdc::pin_checkpoint_at_max_lsn(
                        url,
                        capture_instance.unwrap_or_default(),
                        ckpt,
                        tls,
                    ),
                    #[cfg(feature = "oracle")]
                    Self::Oracle => {
                        crate::source::oracle::cdc::pin_checkpoint_at_current(url, tls, ckpt)
                    }
                    #[cfg(not(feature = "oracle"))]
                    Self::Oracle => Err(crate::source::oracle_feature_missing()),
                    _ => crate::source::mongo::cdc::pin_checkpoint_at_current(url, tls, ckpt),
                }
            }
        }
    }
}

/// The retained-WAL warnings for `slot` and for inactive slots nobody here owns, on `client`.
pub(crate) fn pg_retention_warnings_on(client: &mut postgres::Client, slot: &str) -> Vec<String> {
    let slot = slot.to_string();
    let mut out = Vec::new();
    if let Ok(Some(row)) = client.query_opt(
        "SELECT active, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint \
         FROM pg_replication_slots WHERE slot_name = $1",
        &[&slot],
    ) && let Some(w) =
        crate::preflight::cdc_health::pg_retained_wal_warning(&slot, row.get(1), row.get(0))
    {
        out.push(w);
    }
    let ours = vec![slot];
    if let Ok(rows) = client.query(
        "SELECT slot_name, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint, \
                slot_type \
         FROM pg_replication_slots WHERE NOT active AND slot_name <> ALL($1)",
        &[&ours],
    ) {
        let foreign: Vec<(String, i64, String)> = rows
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect();
        if let Some(w) = crate::preflight::cdc_health::pg_foreign_slots_warning(&foreign, true) {
            out.push(w);
        }
    }
    out
}

/// Add an engine's setup hint only to errors rivet did not raise itself.
fn with_setup_hint(e: anyhow::Error, hint: &'static str) -> anyhow::Error {
    let rendered = format!("{e:#}");
    if let Some(coded) = e.downcast_ref::<crate::error::CodedError>()
        && !matches!(
            coded.kind(),
            crate::error::ErrorKind::Environment(crate::error::Retry::ByCause)
        )
    {
        return e;
    }
    if e.downcast_ref::<crate::source::TlsHandshakeFailed>()
        .is_some()
        || e.downcast_ref::<crate::source::UnreachableTarget>()
            .is_some()
    {
        return e;
    }
    if crate::source::is_tls_handshake_failure(&rendered) {
        return crate::source::TlsHandshakeFailed::wrap(e, None);
    }
    if rendered.split(": ").any(|segment| {
        segment == "cdc"
            || segment
                .strip_suffix(" cdc")
                .is_some_and(|engine| !engine.is_empty() && !engine.contains(' '))
    }) {
        return e;
    }
    e.context(hint)
}

/// Setup/permission hints appended to a CDC start-up error.
pub(crate) const MYSQL_CDC_HINT: &str = "if this is a permissions/setup error: MySQL CDC needs binlog_format=ROW plus a REPLICATION SLAVE + REPLICATION CLIENT grant (and SELECT on the table) — see the 'MySQL — the binlog grants' section of docs/reference/cdc.md";
pub(crate) const PG_CDC_HINT: &str = "if this is a permissions/setup error: PostgreSQL CDC needs wal_level=logical and a role with the REPLICATION attribute — see the 'PostgreSQL — the logical slot' section of docs/reference/cdc.md";
pub(crate) const MSSQL_CDC_HINT: &str = "if this is a permissions/setup error: SQL Server CDC must be enabled on the table (sys.sp_cdc_enable_table) with SQL Server Agent running, and the reader needs SELECT on the cdc schema — see the 'SQL Server — CDC change tables' section of docs/reference/cdc.md";
pub(crate) const ORACLE_CDC_HINT: &str = "if this is a permissions/setup error: Oracle CDC mines redo with LogMiner from CDB$ROOT — the URL names the pluggable database's service, the user is a COMMON user (C##…) granted CREATE SESSION, SET CONTAINER, LOGMINING, EXECUTE_CATALOG_ROLE and SELECT on V_$DATABASE, V_$ARCHIVED_LOG, V_$LOG, V_$LOGFILE, V_$LOGMNR_CONTENTS, V_$LOGMNR_LOGS, V_$TRANSACTION with CONTAINER=ALL, plus SELECT on the captured tables — see the 'Oracle — LogMiner' section of docs/reference/cdc.md";
pub(crate) const MONGO_CDC_HINT: &str = "if this is a setup error: MongoDB change streams require a replica set (a single-node replica set is fine) — a standalone mongod cannot watch(); the reader needs a role that can run changeStream (readAnyDatabase / read on the db) (connect with directConnection=true to a port-mapped single-node set) — see the 'Prerequisites' section of docs/reference/mongodb.md";

#[cfg(feature = "oracle")]
pub(crate) use crate::config::ORACLE_CONTINUOUS_REFUSAL;
/// Where an oversized transaction spills — `None` unless the operator named a directory in `RIVET_CDC_SPILL_DIR`.
pub(crate) fn spill_dir_for(
    checkpoint: Option<&std::path::Path>,
    config_dir: &std::path::Path,
) -> Option<std::path::PathBuf> {
    let raw = std::env::var("RIVET_CDC_SPILL_DIR").ok()?;
    let named = raw.trim();
    if named.is_empty() {
        return None;
    }
    if ["0", "false", "off", "no"].contains(&named.to_ascii_lowercase().as_str()) {
        return None;
    }
    if ["1", "true", "yes", "on"].contains(&named.to_ascii_lowercase().as_str()) {
        return Some(match checkpoint.and_then(std::path::Path::parent) {
            Some(dir) if !dir.as_os_str().is_empty() => dir.join(".rivet-spill"),
            _ => config_dir.join(".rivet").join("spill"),
        });
    }
    let p = std::path::Path::new(named);
    Some(if p.is_absolute() {
        p.to_path_buf()
    } else {
        config_dir.join(p)
    })
}

/// What a PostgreSQL stream finds already recorded for this run: a checkpoint or not.
fn pg_prior_run(cfg: &CdcConfig) -> Result<PriorRun> {
    let checkpoint = match cfg.checkpoint.as_deref() {
        Some(p) => Position::load(p)?.is_some(),
        None => false,
    };
    Ok(PriorRun {
        checkpoint,
        snapshot: false,
    })
}

/// Construct the right [`ChangeStream`] adapter for the source URL's scheme.
pub(crate) fn create_change_stream(
    cfg: &CdcConfig,
    peek: PeekBound,
) -> Result<Box<dyn ChangeStream>> {
    let url = cfg.url.expose();
    crate::source::require_url_has_host(url)?;
    let tls = cfg.tls.as_ref();
    if let Some(dir) = spill_dir_for(cfg.checkpoint.as_deref(), &cfg.config_dir) {
        spill::sweep_dead_spills_now(&dir);
    }
    match &cfg.engine {
        CdcEngineOpts::Mysql {
            server_id,
            configured_tables,
            resolved_grants,
        } => {
            if let Some(p) = cfg.checkpoint.as_deref() {
                let pos = Position::load(std::path::Path::new(p))?;
                crate::source::mysql::cdc::MysqlChangeStream::resume_from_checkpoint(
                    pos.as_ref(),
                    &p.display().to_string(),
                )?;
            }
            Ok(Box::new(
                crate::source::mysql::cdc::MysqlChangeStream::open_or_resume(
                    url,
                    *server_id,
                    cfg.checkpoint.as_deref(),
                    cfg.drain,
                    tls,
                    configured_tables.clone(),
                    resolved_grants.clone(),
                    spill_dir_for(cfg.checkpoint.as_deref(), &cfg.config_dir),
                )
                .map_err(|e| with_setup_hint(e, MYSQL_CDC_HINT))?,
            ))
        }
        CdcEngineOpts::Postgres {
            slot,
            configured_tables,
            resolved_grants,
        } => {
            let prior = pg_prior_run(cfg)?;
            Ok(Box::new(
                crate::source::postgres::cdc::PgChangeStream::open(
                    url,
                    slot,
                    prior,
                    tls,
                    peek,
                    cfg.drain,
                    configured_tables,
                    spill_dir_for(cfg.checkpoint.as_deref(), &cfg.config_dir).as_deref(),
                    cfg.checkpoint.as_deref(),
                    resolved_grants.clone(),
                )
                .map_err(|e| with_setup_hint(e, PG_CDC_HINT))?,
            ))
        }
        CdcEngineOpts::Mssql {
            capture_instance,
            configured_tables,
            resolved_grants,
        } => {
            let ci = capture_instance.as_deref().ok_or_else(|| {
                crate::error::coded(
                    crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                    "sqlserver cdc requires a capture instance: set `cdc.capture_instance:` on the \
                     export (or `--capture-instance` with `rivet cdc`), e.g. dbo_orders",
                )
            })?;
            let resume = match cfg.checkpoint.as_deref() {
                Some(p) => crate::source::mssql::cdc::resume_from_checkpoint(
                    Position::load(p)?.as_ref(),
                    &p.display().to_string(),
                )?,
                None => crate::source::mssql::cdc::resume_from_checkpoint(None, "")?,
            };
            Ok(Box::new(
                crate::source::mssql::cdc::MssqlChangeStream::from_url(
                    url,
                    ci,
                    resume,
                    tls,
                    peek,
                    cfg.drain,
                    configured_tables,
                    spill_dir_for(cfg.checkpoint.as_deref(), &cfg.config_dir),
                    resolved_grants.clone(),
                )
                .map_err(|e| with_setup_hint(e, MSSQL_CDC_HINT))?,
            ))
        }
        #[cfg(feature = "oracle")]
        CdcEngineOpts::Oracle {
            configured_tables,
            resolved_grants,
        } => {
            if !cfg.drain.is_bounded() {
                crate::rivet_bail!(
                    crate::error::codes::CONFIG_CDC_CONTINUOUS_UNSUPPORTED,
                    "{ORACLE_CONTINUOUS_REFUSAL}"
                );
            }
            if let Some(p) = cfg.checkpoint.as_deref() {
                Position::load(p)?;
            }
            Ok(Box::new(
                crate::source::oracle::cdc::OracleChangeStream::open(
                    url,
                    tls,
                    cfg.checkpoint.as_deref(),
                    configured_tables,
                    resolved_grants.clone(),
                )
                .map_err(|e| with_setup_hint(e, ORACLE_CDC_HINT))?,
            ))
        }
        #[cfg(not(feature = "oracle"))]
        CdcEngineOpts::Oracle { .. } => Err(crate::source::oracle_feature_missing()),
        CdcEngineOpts::Mongo {
            canonical,
            configured_tables,
            keyed_tables,
            resolved_grants,
        } => {
            if let Some(p) = cfg.checkpoint.as_deref() {
                Position::load(std::path::Path::new(p))?;
            }
            Ok(Box::new(
                crate::source::mongo::cdc::MongoChangeStream::open(
                    url,
                    tls,
                    cfg.checkpoint.as_deref(),
                    *canonical,
                    cfg.drain,
                    configured_tables,
                    keyed_tables,
                    resolved_grants.as_deref(),
                )
                .map_err(|e| {
                    if crate::source::mongo::cdc::error_names_its_own_cause(&e) {
                        e
                    } else {
                        with_setup_hint(e, MONGO_CDC_HINT)
                    }
                })?,
            ))
        }
    }
}

/// Resolve CDC tables' column type mappings from the source.
pub(crate) struct CdcSchemaResolver<'s> {
    src: ResolverSource<'s>,
}

/// The resolver's connection: its own, or the run's metadata connection lent for the run.
enum ResolverSource<'s> {
    Own(Box<dyn crate::source::Source>),
    Lent(&'s mut (dyn crate::source::Source + 'static)),
}

impl CdcSchemaResolver<'static> {
    pub(crate) fn connect(url: &str, tls: Option<&crate::config::TlsConfig>) -> Result<Self> {
        Self::connect_as(CdcEngine::from_url(url)?, url, tls)
    }

    /// Open the resolver's own connection to `engine` (Mongo resolves its fixed `_id` + `document` schema, like batch).
    pub(crate) fn connect_as(
        engine: CdcEngine,
        url: &str,
        tls: Option<&crate::config::TlsConfig>,
    ) -> Result<Self> {
        let src = crate::source::connect(engine.source_type(), url, tls, None)?;
        Ok(Self {
            src: ResolverSource::Own(src),
        })
    }
}

impl<'s> CdcSchemaResolver<'s> {
    /// Resolve on a connection the run already holds.
    pub(crate) fn lent(src: &'s mut (dyn crate::source::Source + 'static)) -> Self {
        Self {
            src: ResolverSource::Lent(src),
        }
    }

    pub(crate) fn source(&mut self) -> &mut dyn crate::source::Source {
        match &mut self.src {
            ResolverSource::Own(s) => s.as_mut(),
            ResolverSource::Lent(s) => &mut **s,
        }
    }

    /// One table's mappings.
    pub(crate) fn resolve(
        &mut self,
        table: &str,
        overrides: &crate::types::ColumnOverrides,
        readable: Option<&ReadableColumns>,
    ) -> Result<Vec<crate::types::TypeMapping>> {
        validate_table_ident(table)?;
        let select =
            readable.map_or_else(|| format!("SELECT * FROM {table}"), |r| r.select.clone());
        let mut mappings = match self
            .source()
            .type_mappings(&select, &crate::types::ColumnOverrides::new())
        {
            Ok(m) => m,
            Err(e) => {
                let e = schema_read_error(readable.is_some(), table, e);
                return Err(match readable {
                    Some(before) => {
                        grants_moved_under_a_failed_read(self.source(), table, before, e)
                    }
                    None => e,
                });
            }
        };
        if let Some(m) = mappings.iter().find(|m| {
            matches!(
                m.column_name.as_str(),
                "__op" | "__pos" | "__seq" | crate::enrich::DELETE_FLAG_COLUMN
            )
        }) {
            crate::rivet_bail!(
                crate::error::codes::LOAD_RESERVED_COLUMN,
                "cdc: `{table}` has a column named `{}`, which is one of the names rivet \
                 adds to every part (`__op`, `__pos`, `__seq`) or to the warehouse base \
                 (`__is_deleted`). Capturing it would put two fields of that name in one \
                 part and silently redirect `_rivet_row_hash` to the sink's value instead \
                 of the source's — the drain and the snapshot leg would then disagree about \
                 the same row. Project the column to another name with a `query:`, or \
                 exclude it.",
                m.column_name
            );
        }
        let full = self.source().native_column_types(table)?;
        for m in &mut mappings {
            if let Some(ct) = native_type_for(&full, &m.column_name) {
                m.source_native_type = ct.to_string();
            }
        }
        refuse_by_override_table(self.source(), &select, &mappings, overrides)?;
        let declared = mappings.clone();
        let mut planned = mappings
            .into_iter()
            .map(|m| match overrides.contains_key(&m.column_name) {
                true => retype_for_stream(m, overrides),
                false => plan_cdc_delivery(m, false),
            })
            .collect::<Result<Vec<_>>>()?;
        crate::types::narrowing::downgrade_narrowing_overrides(
            &mut planned,
            overrides,
            || Ok(declared),
            || self.source().finer_than_microsecond_in_query(&select),
        )?;
        Ok(planned)
    }
}

/// The mapping of `m` under its `columns:` override: the declared type, refused when the CDC cell builder cannot build it.
pub(crate) fn retype_for_stream(
    m: crate::types::TypeMapping,
    overrides: &crate::types::ColumnOverrides,
) -> Result<crate::types::TypeMapping> {
    let Some(target) = overrides.get(&m.column_name) else {
        return Ok(m);
    };
    let source = crate::types::SourceColumn::simple(
        m.column_name.clone(),
        m.source_native_type.clone(),
        m.nullable,
    );
    plan_cdc_delivery(
        crate::types::TypeMapping::from_source(&source, target.clone()),
        true,
    )
}

/// `columns:` overrides of a stream export, graded by the batch override table where it decides and by the CDC cell builder after it.
pub(crate) fn stream_override_refusal(
    src: &mut dyn crate::source::Source,
    query: &str,
    overrides: &crate::types::ColumnOverrides,
) -> Result<()> {
    let Ok(mappings) = src.type_mappings(query, &crate::types::ColumnOverrides::new()) else {
        return Ok(());
    };
    refuse_by_override_table(src, query, &mappings, overrides)?;
    mappings
        .into_iter()
        .try_for_each(|m| retype_for_stream(m, overrides).map(drop))
}

/// An integer override over a numeric, float or text column is the batch table's pair, so the stream, the baseline and `check` refuse it alike before any row.
pub(crate) fn refuse_by_override_table(
    src: &mut dyn crate::source::Source,
    query: &str,
    detected: &[crate::types::TypeMapping],
    overrides: &crate::types::ColumnOverrides,
) -> Result<()> {
    use crate::types::RivetType as T;
    let routed: crate::types::ColumnOverrides = detected
        .iter()
        .filter_map(|m| {
            let target = overrides.get(&m.column_name)?;
            matches!(
                (&m.rivet_type, target),
                (
                    T::Decimal { .. } | T::Float32 | T::Float64 | T::String | T::Text,
                    T::Int16 | T::Int32 | T::Int64 | T::UInt64
                )
            )
            .then(|| (m.column_name.clone(), target.clone()))
        })
        .collect();
    if seeded!("cdc-integer-override-skips-table", true, routed.is_empty()) {
        return Ok(());
    }
    src.type_mappings(query, &routed).map(drop)
}

/// The CDC plan for one column: kept when the builder builds it, else the server's own text.
pub(crate) fn plan_cdc_delivery(
    m: crate::types::TypeMapping,
    overridden: bool,
) -> Result<crate::types::TypeMapping> {
    match &m.arrow_type {
        Some(dt) if value::is_buildable(dt) => Ok(m),
        _ if overridden => crate::rivet_bail!(
            crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
            "cdc: column '{}' has a `columns:` override to {}, which the CDC change-event \
             builder cannot produce; correct or drop the override",
            m.column_name,
            m.rivet_type.label()
        ),
        _ => Ok(m.with_text(crate::types::TextForm::ServerText)),
    }
}

/// Split a possibly-qualified `db.table` into the `(schema, bare_table)` the MySQL `information_schema.COLUMNS` enrichment query needs.
pub(crate) fn enrich_schema_and_table(table: &str, default_db: Option<&str>) -> (String, String) {
    match table.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => (
            default_db.unwrap_or_default().to_string(),
            table.to_string(),
        ),
    }
}

/// Single-table convenience over [`CdcSchemaResolver`] (CLI path + tests).
pub(crate) fn resolve_cdc_columns(
    url: &str,
    table: &str,
    tls: Option<&crate::config::TlsConfig>,
    overrides: &crate::types::ColumnOverrides,
) -> Result<Vec<crate::types::TypeMapping>> {
    validate_table_ident(table)?;
    CdcSchemaResolver::connect(url, tls)?.resolve(table, overrides, None)
}

/// The table name is interpolated into `SELECT * FROM {table}` for the schema probe.
pub(crate) fn validate_table_ident(table: &str) -> Result<()> {
    if table.is_empty()
        || !table
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        crate::rivet_bail!(
            crate::error::codes::CONFIG_EXPORT_SOURCE_INVALID,
            "rivet cdc table must be a plain [schema.]table identifier (got {table:?}); \
             refusing to interpolate it into SQL"
        );
    }
    Ok(())
}

/// The error of a schema read: a column refused while the run projects is a grant change, any other stays as it came.
fn schema_read_error(projecting: bool, table: &str, err: anyhow::Error) -> anyhow::Error {
    if !grants_moved_mid_resolve(projecting, &err) {
        return err;
    }
    crate::error::coded(
        crate::error::codes::SOURCE_CDC_GRANTS_CHANGED,
        format!(
            "mysql cdc: the account lost SELECT on a column of `{table}` between the stream's \
             read of the readable columns and its read of the schema; nothing past it was \
             delivered. Re-run: the run then reads the columns as they are now"
        ),
    )
}

/// A schema read that failed while the run projects: when the server now says another column set is readable, the grants moved.
fn grants_moved_under_a_failed_read(
    src: &mut dyn crate::source::Source,
    table: &str,
    before: &ReadableColumns,
    err: anyhow::Error,
) -> anyhow::Error {
    if seeded!("column-grants-failed-schema-read-not-reread", true, false)
        || seeded!(
            "column-grants-revoke-between-two-reads-uncoded",
            true,
            false
        )
        || crate::error::code_of(&err) == Some(crate::error::codes::SOURCE_CDC_GRANTS_CHANGED)
    {
        return err;
    }
    let Ok(Some(now)) = src.readable_columns(table) else {
        return err;
    };
    let named =
        |c: &ReadableColumns| std::collections::HashMap::from([(table.to_string(), c.clone())]);
    match crate::source::column_grants::first_grants_change(
        &[table.to_string()],
        &named(before),
        &named(&now),
    ) {
        Some(why) => crate::error::coded(
            crate::error::codes::SOURCE_CDC_GRANTS_CHANGED,
            format!(
                "cdc: {why}; the read of the schema of `{table}` failed after the grants moved and \
                 nothing was delivered. Re-run: the next run reads the grants as they are now and \
                 resumes from the checkpoint, so nothing is lost"
            ),
        ),
        None => err,
    }
}

/// A schema read refused by the server's privilege check while the run projects.
fn grants_moved_mid_resolve(projecting: bool, err: &anyhow::Error) -> bool {
    seeded!(
        "column-grants-revoke-between-two-reads-uncoded",
        false,
        projecting
    ) && crate::source::column_grants::denied_by_server(err).is_some()
}

/// The CDC probe could not read a captured table for a reason the server named; nothing was read or written.
pub(crate) fn table_unreadable(tag: &str, table: &str, said: &str) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::SOURCE_QUERY_FAILED,
        format!("{tag} cdc: cannot read `{table}` ({said}); nothing was read or written."),
    )
}

/// Resolve a configured `cdc.checkpoint:` path.
pub(crate) fn resolve_checkpoint(raw: &str, config_dir: &std::path::Path) -> PathBuf {
    let p = std::path::Path::new(raw);
    if p.is_absolute() {
        return p.to_path_buf();
    }
    let by_config: PathBuf = config_dir.join(p).components().collect();
    if !by_config.exists() && p.exists() {
        log::warn!(
            "cdc: using the existing checkpoint at `{}` (relative to the working \
             directory). rivet now resolves a relative `cdc.checkpoint:` against the \
             config's directory, so this run would otherwise have started without it \
             (MySQL/MongoDB re-anchor at the current position and skip everything since; \
             SQL Server re-reads the retained change table). Move it to `{}` — or \
             make the path absolute — so the location no longer depends on where \
             rivet is invoked from.",
            p.display(),
            by_config.display()
        );
        return p.to_path_buf();
    }
    by_config
}

#[cfg(test)]
mod mod_decisions {
    use super::*;

    /// The two buffer caps, the sequence stamp and the byte estimate.
    #[test]
    fn a_tx_cap_falls_back_to_its_default_on_anything_that_is_not_a_positive_number() {
        assert_eq!(tx_cap_from_env(Some("7"), 100), 7);
        assert_eq!(tx_cap_from_env(None, 100), 100);
        assert_eq!(tx_cap_from_env(Some("abc"), 100), 100);
        assert_eq!(tx_cap_from_env(Some("-1"), 100), 100);
        assert_eq!(
            tx_cap_from_env(Some("0"), 100),
            100,
            "`RIVET_CDC_MAX_TX_ROWS=0` must not mean `refuse every transaction` — \
             taking it literally turns a tuning knob into a kill switch"
        );
        assert_eq!(DEFAULT_MAX_TX_ROWS, 5_000_000);
        assert_eq!(
            DEFAULT_MAX_TX_BYTES,
            2 * 1024 * 1024 * 1024,
            "2 GiB — `*` -> `+` collapses it to 3074 bytes, which refuses every \
             transaction that carries more than a couple of rows"
        );
    }

    /// Every op answers where its values live, and only DELETE says "before".
    #[test]
    fn only_a_delete_carries_its_values_in_the_before_image() {
        assert!(
            ChangeOp::Delete.values_live_in_before(),
            "a delete has no post-image; reading `after` finds None and the event \
             carries nothing"
        );
        assert!(
            !ChangeOp::Insert.values_live_in_before(),
            "an insert's values are the AFTER image — framing them as `before` \
             delivers a retraction of a row that was just created"
        );
        assert!(!ChangeOp::Update.values_live_in_before());
    }

    /// A column's native type comes from ITS row in the catalog, not a neighbour's.
    #[test]
    fn cdc_delivery_keeps_buildable_types_texts_the_rest_and_refuses_an_unbuildable_override() {
        use crate::types::{
            Delivery, RivetType, SourceColumn, TextForm, TimeUnit, TypeFidelity, TypeMapping,
        };
        let col = |native: &str, t: RivetType| {
            TypeMapping::from_source(&SourceColumn::simple("c", native, true), t)
        };
        let kept = plan_cdc_delivery(col("bigint", RivetType::Int64), true).unwrap();
        assert_eq!(kept.delivery, Delivery::Native);
        assert_eq!(kept.arrow_type, Some(arrow::datatypes::DataType::Int64));

        let numeric = RivetType::Unsupported {
            native_type: "numeric".into(),
            reason: "no precision".into(),
        };
        let numeric_list = RivetType::List {
            inner: Box::new(numeric.clone()),
        };
        for t in [numeric, numeric_list] {
            let m = plan_cdc_delivery(col("numeric", t.clone()), false).unwrap();
            assert_eq!(m.delivery, Delivery::Text(TextForm::ServerText), "{t:?}");
            assert_eq!(m.arrow_type, Some(arrow::datatypes::DataType::Utf8));
            assert_eq!(m.fidelity, TypeFidelity::LogicalString);
        }
        for inner in [RivetType::Date, RivetType::Uuid, RivetType::Binary] {
            let list = RivetType::List {
                inner: Box::new(inner),
            };
            let m = plan_cdc_delivery(col("date", list.clone()), false).unwrap();
            assert_eq!(m.delivery, Delivery::Native, "{list:?}");
        }

        let ns = RivetType::Time {
            unit: TimeUnit::Millisecond,
        };
        let err = plan_cdc_delivery(col("time", ns), true).unwrap_err();
        assert_eq!(
            err.downcast_ref::<crate::error::CodedError>()
                .map(|c| c.code()),
            Some("RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH")
        );
        assert!(format!("{err:#}").contains("column 'c'"), "{err:#}");
    }

    /// A stream override is graded by the CDC cell builder, so a pair only the stream reads is delivered.
    #[test]
    fn a_stream_override_is_graded_by_the_cell_builder_not_the_batch_table() {
        use crate::types::{RivetType, SourceColumn, TimeUnit, TypeMapping};
        let time = || {
            TypeMapping::from_source(
                &SourceColumn::simple("t", "time", true),
                RivetType::Time {
                    unit: TimeUnit::Microsecond,
                },
            )
        };
        let declared = |t: RivetType| [("t".to_string(), t)].into_iter().collect();
        let kept = retype_for_stream(time(), &declared(RivetType::String)).unwrap();
        assert_eq!(kept.arrow_type, Some(arrow::datatypes::DataType::Utf8));
        assert!(!crate::types::override_table::override_is_read(
            crate::types::override_table::Engine::Mysql,
            "time",
            &RivetType::Time {
                unit: TimeUnit::Microsecond
            },
            &RivetType::String,
        ));
        let ns = RivetType::Time {
            unit: TimeUnit::Millisecond,
        };
        let err = retype_for_stream(time(), &declared(ns)).unwrap_err();
        assert_eq!(
            err.downcast_ref::<crate::error::CodedError>()
                .map(|c| c.code()),
            Some("RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH")
        );
        let untouched = retype_for_stream(time(), &crate::types::ColumnOverrides::new()).unwrap();
        assert_eq!(untouched.rivet_type, time().rivet_type);
    }

    #[test]
    fn a_columns_native_type_is_looked_up_by_its_own_name() {
        let catalog = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "varchar(64)".to_string()),
        ];
        assert_eq!(native_type_for(&catalog, "id"), Some("bigint"));
        assert_eq!(
            native_type_for(&catalog, "name"),
            Some("varchar(64)"),
            "`!=` returns the FIRST row that is not this column, so every mapping \
             is enriched with a neighbour's native type — a varchar reported as \
             bigint, in the schema every consumer downstream reads"
        );
        assert_eq!(
            native_type_for(&catalog, "absent"),
            None,
            "a column the catalog does not list has no native type; `!=` answers \
             `bigint` here, which is a type for a column that does not exist"
        );
        assert_eq!(native_type_for(&[], "id"), None);
    }

    #[test]
    fn only_a_projecting_run_reads_a_denied_schema_read_as_moved_grants() {
        let denied = || {
            anyhow::Error::from(mysql::Error::MySqlError(mysql::MySqlError {
                state: "42000".into(),
                message: "SELECT command denied".into(),
                code: 1143,
            }))
        };
        assert!(super::grants_moved_mid_resolve(true, &denied()));
        assert!(!super::grants_moved_mid_resolve(false, &denied()));
        assert!(!super::grants_moved_mid_resolve(
            true,
            &anyhow::anyhow!("other")
        ));

        let moved = super::schema_read_error(true, "d.t", denied());
        assert_eq!(
            crate::error::code_of(&moved),
            Some(crate::error::codes::SOURCE_CDC_GRANTS_CHANGED)
        );
        assert!(format!("{moved:#}").contains("lost SELECT on a column of `d.t`"));
        for (projecting, err) in [(false, denied()), (true, anyhow::anyhow!("other"))] {
            let said = format!("{err:#}");
            let kept = super::schema_read_error(projecting, "d.t", err);
            assert_eq!(crate::error::code_of(&kept), None);
            assert_eq!(format!("{kept:#}"), said);
        }
    }

    struct Rereads(anyhow::Result<Option<Vec<&'static str>>>);

    impl crate::source::Source for Rereads {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> Result<()> {
            unreachable!("a re-read exports nothing")
        }

        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>> {
            unreachable!("a re-read reads no scalar")
        }

        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            unreachable!("a re-read reads no types")
        }

        fn readable_columns(&mut self, _table: &str) -> Result<Option<ReadableColumns>> {
            match &self.0 {
                Ok(c) => Ok(c.as_ref().map(|c| readable(c))),
                Err(e) => Err(anyhow::anyhow!("{e:#}")),
            }
        }
    }

    struct Fine(crate::types::RivetType, Vec<String>);

    impl crate::source::Source for Fine {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> Result<()> {
            unreachable!("a resolve exports nothing")
        }

        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>> {
            unreachable!("a resolve reads no scalar")
        }

        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![crate::types::TypeMapping::from_source(
                &crate::types::SourceColumn::simple("c", "source", true),
                self.0.clone(),
            )])
        }

        fn finer_than_microsecond_in_query(&mut self, _query: &str) -> Result<Vec<String>> {
            Ok(self.1.clone())
        }
    }

    /// The stream resolver grades a narrowing override as `check` does, so `--strict` refuses what `check` reports lossy: a time override by the result column, a decimal override by its digits.
    #[test]
    fn the_stream_resolver_reports_a_narrowing_override_lossy_as_check_does() {
        use crate::types::TimeUnit::{Microsecond, Nanosecond};
        use crate::types::{ColumnOverrides, RivetType, TypeFidelity};
        let time = |unit| RivetType::Timestamp {
            unit,
            timezone: None,
        };
        let dec = |precision, scale| RivetType::Decimal { precision, scale };
        let fidelity = |source: RivetType, finer: &[&str], declared: Option<RivetType>| {
            let overrides: ColumnOverrides =
                declared.into_iter().map(|t| ("c".to_string(), t)).collect();
            let mut src = Fine(source, finer.iter().map(|c| c.to_string()).collect());
            CdcSchemaResolver::lent(&mut src)
                .resolve("dbo.t", &overrides, None)
                .unwrap()[0]
                .fidelity
        };
        let (lossy, exact) = (TypeFidelity::Lossy, TypeFidelity::Exact);
        let ns = || time(Nanosecond);
        assert_eq!(fidelity(ns(), &["c"], Some(time(Microsecond))), lossy);
        assert_eq!(fidelity(ns(), &[], Some(time(Microsecond))), exact);
        assert_eq!(fidelity(ns(), &["c"], Some(ns())), exact);
        assert_eq!(fidelity(ns(), &["c"], None), exact);
        assert_eq!(fidelity(dec(10, 2), &[], Some(dec(20, 0))), lossy);
        assert_eq!(fidelity(dec(10, 2), &[], Some(dec(12, 2))), exact);
    }

    fn readable(columns: &[&str]) -> ReadableColumns {
        ReadableColumns {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            select: format!("SELECT {} FROM d.t", columns.join(", ")),
        }
    }

    #[test]
    fn a_failed_schema_read_is_a_grant_change_only_when_the_server_now_reads_another_set() {
        let before = readable(&["id", "name", "pass"]);
        let failed = || anyhow::anyhow!("type probe failed");
        let run = |now: anyhow::Result<Option<Vec<&'static str>>>, err| {
            super::grants_moved_under_a_failed_read(&mut Rereads(now), "d.t", &before, err)
        };

        let moved = run(Ok(Some(vec!["id", "name"])), failed());
        assert_eq!(
            crate::error::code_of(&moved),
            Some(crate::error::codes::SOURCE_CDC_GRANTS_CHANGED)
        );
        assert!(format!("{moved:#}").contains("lost `pass`"), "{moved:#}");

        for (now, err) in [
            (Ok(Some(vec!["id", "name", "pass"])), failed()),
            (Ok(None), failed()),
            (Err(anyhow::anyhow!("connection lost")), failed()),
        ] {
            let kept = run(now, err);
            assert_eq!(crate::error::code_of(&kept), None);
            assert_eq!(format!("{kept:#}"), "type probe failed");
        }

        let coded = crate::error::coded(
            crate::error::codes::SOURCE_CDC_GRANTS_CHANGED,
            "mysql cdc: the account lost SELECT on a column of `d.t`",
        );
        let kept = run(Ok(Some(vec!["id"])), coded);
        assert!(format!("{kept:#}").contains("mysql cdc: the account lost SELECT"));
    }

    /// A checkpoint that cannot be READ is not a checkpoint that is ABSENT.
    #[test]
    fn an_unreadable_checkpoint_is_an_error_not_an_absent_one() {
        let dir = tempfile::tempdir().expect("temp dir");

        assert!(
            Position::load(&dir.path().join("nope.ckpt"))
                .expect("a missing checkpoint is not an error")
                .is_none(),
            "a file that is not there IS the first run"
        );

        let ok = dir.path().join("ok.ckpt");
        std::fs::write(&ok, br#"{"lsn":"0/10"}"#).expect("write a checkpoint");
        assert!(Position::load(&ok).expect("valid").is_some());

        let as_dir = dir.path().join("a_directory.ckpt");
        std::fs::create_dir(&as_dir).expect("create the impostor");
        let err = Position::load(&as_dir)
            .expect_err("a path that cannot be read must ERROR, not read as absent");
        let text = format!("{err:#}");
        assert!(
            text.contains("reading checkpoint") && text.contains("a_directory.ckpt"),
            "the error must name the file it failed to read, or an operator cannot \
             tell it from the corrupt-JSON case one line above: {text}"
        );
    }

    /// The log-pinning fact, EVERY variant — only a PostgreSQL slot makes the server retain log for the reader.
    #[test]
    fn only_postgres_pins_log_for_the_reader() {
        let all = [
            CdcEngine::Mysql,
            CdcEngine::Postgres,
            CdcEngine::Mssql,
            CdcEngine::Mongo,
            CdcEngine::Oracle,
        ];
        let pinning: Vec<CdcEngine> = all
            .iter()
            .copied()
            .filter(|e| e.pins_log_for_reader())
            .collect();
        assert_eq!(pinning, vec![CdcEngine::Postgres]);
    }

    /// A stream that resolves no catalog identity must say NOTHING, not a name.
    #[test]
    fn an_adapter_that_resolves_no_identity_returns_none_not_a_fabricated_pair() {
        struct Unresolving;
        impl ChangeStream for Unresolving {
            fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
                None
            }
            fn ack(&mut self, _position: &Position) -> Result<()> {
                Ok(())
            }
            fn engine(&self) -> CdcEngine {
                CdcEngine::Mongo
            }
        }
        assert_eq!(
            Unresolving.resolved_identity("anything"),
            None,
            "the default must be `no answer`. Any `Some` here — including a pair of \
             empty strings — is a claim about a catalog the adapter never read."
        );
        assert_eq!(Unresolving.resolved_identity(""), None);
    }

    /// `__seq` is the intra-transaction ordinal the load's dedup sorts by, together with `__pos`.
    #[test]
    fn the_sequence_stamp_writes_the_ordinal_onto_the_event() {
        let at = |lsn: &str| Position::new(serde_json::json!({ "lsn": lsn }));
        let ev = |lsn: &str| ChangeEvent {
            op: ChangeOp::Insert,
            schema: "s".into(),
            table: "t".into(),
            before: None,
            after: None,
            position: at(lsn),
            committed: false,
            image_names: None,
            seq: 999,
            poison: None,
            row_id: None,
            before_names: None,
            before_poison: None,
            image_labels: None,
        };
        let mut seq = TxnSeq::default();

        let mut a = ev("0/10");
        let mut b = ev("0/10");
        let mut c = ev("0/10");
        seq.stamp(&mut a);
        seq.stamp(&mut b);
        seq.stamp(&mut c);
        assert_eq!(
            (a.seq, b.seq, c.seq),
            (0, 1, 2),
            "the ordinal must be written onto the EVENT — leaving the constructor's \
             value there is what `stamp -> ()` does, and every row then sorts equal"
        );

        let mut d = ev("0/20");
        seq.stamp(&mut d);
        assert_eq!(d.seq, 0, "a new transaction restarts the ordinal");
        let mut e = ev("0/20");
        seq.stamp(&mut e);
        assert_eq!(e.seq, 1);
    }

    /// `estimated_bytes` is the SUPPLIER of the sink's byte-cap accounting, whose consumer (`should_roll`) has a full unit matrix.
    #[test]
    fn the_byte_estimate_counts_both_images_the_names_and_the_overhead() {
        let mk = |before: Option<Vec<RivetValue>>, after: Option<Vec<RivetValue>>| ChangeEvent {
            op: ChangeOp::Update,
            schema: "sch".into(),
            table: "tab".into(),
            before,
            after,
            position: Position::new(serde_json::json!({})),
            committed: false,
            image_names: None,
            seq: 0,
            poison: None,
            row_id: None,
            before_names: None,
            before_poison: None,
            image_labels: None,
        };
        let empty = mk(None, None).estimated_bytes();
        assert_eq!(
            empty,
            std::mem::size_of::<ChangeEvent>() + 3 + 3,
            "an empty event still costs the struct itself — charging only the \
             PAYLOAD is how this estimate came to under-count a real event 12.7x, \
             which made `RIVET_CDC_MAX_TX_BYTES: 2 GiB` mean ~25 GiB of memory"
        );

        let mut poisoned = mk(None, None);
        poisoned.poison = Some("x".repeat(7));
        assert_eq!(poisoned.estimated_bytes(), empty + 7);
        poisoned.before_poison = Some("y".repeat(11));
        assert_eq!(poisoned.estimated_bytes(), empty + 7 + 11);
        poisoned.row_id = Some("z".repeat(13));
        assert_eq!(poisoned.estimated_bytes(), empty + 7 + 11 + 13);

        let mut positioned = mk(None, None);
        positioned.position = Position::new(serde_json::json!({ "lsn": "0/16B2E00" }));
        let mut resident = super::ResidentBytes::default();
        resident.add(&positioned);
        assert!(
            resident.bytes() > empty + 400,
            "a position is charged where it enters the buffer: a one-key JSON object costs a whole BTreeMap node"
        );
        let after_one = resident.bytes();
        let sibling = positioned.clone();
        resident.add(&sibling);
        assert_eq!(
            resident.bytes(),
            after_one + sibling.estimated_bytes(),
            "an event sharing the previous event's position adds its own bytes only"
        );
        let mut apart = sibling.clone();
        apart.position = Position::new(serde_json::json!({ "lsn": "0/16B2E00" }));
        resident.add(&apart);
        assert!(
            resident.bytes() > after_one + 2 * sibling.estimated_bytes() + 400,
            "an equal position held in another allocation is charged again"
        );
        resident.reset();
        assert_eq!(resident.bytes(), 0);
        resident.add(&sibling);
        assert_eq!(
            resident.bytes(),
            after_one,
            "after a reset the next position is charged again"
        );

        let names: std::sync::Arc<[String]> =
            std::sync::Arc::from(["alpha", "beta"].map(String::from).to_vec());
        let mut exclusive = mk(None, None);
        exclusive.image_names = Some(names.clone());
        drop(names);
        let solo = exclusive.estimated_bytes();
        assert!(
            solo > empty + 2 * std::mem::size_of::<String>(),
            "an Arc held by ONE event is that event's memory and must be charged"
        );

        let shared: std::sync::Arc<[String]> =
            std::sync::Arc::from(["alpha", "beta"].map(String::from).to_vec());
        let many: Vec<ChangeEvent> = (0..8)
            .map(|_| {
                let mut e = mk(None, None);
                e.image_names = Some(shared.clone());
                e
            })
            .collect();
        assert!(
            many[0].estimated_bytes() < solo,
            "the SAME names shared across events must cost each of them less than \
             an exclusive copy — charging in full would over-count by the \
             transaction's length, which is the opposite error and just as wrong"
        );

        let mut slack = Vec::with_capacity(1024);
        slack.extend_from_slice(&[b'x'; 600]);
        assert!(
            slack.capacity() > slack.len(),
            "the fixture must carry slack"
        );
        let with_slack = mk(None, Some(vec![RivetValue::Bytes(slack)])).estimated_bytes();
        let exact = mk(None, Some(vec![RivetValue::Bytes(vec![b'x'; 600])])).estimated_bytes();
        assert!(
            with_slack >= exact + 300,
            "resident must charge the CAPACITY ({with_slack} vs exact {exact}) — \
             len-charging is how a 256 MiB budget held ~512 MiB of documents"
        );

        let flat = mk(None, Some(vec![RivetValue::Int(1)])).estimated_bytes();
        let arr = mk(
            None,
            Some(vec![RivetValue::Array(vec![RivetValue::Int(1); 100])]),
        )
        .estimated_bytes();
        assert!(
            arr > flat + 100 * std::mem::size_of::<RivetValue>(),
            "an array's SLOTS cost as much as any other Vec's — PostgreSQL is the \
             engine that produces them and the one whose budget was blind to them"
        );

        let one = vec![RivetValue::Int(1)];
        let b_only = mk(Some(one.clone()), None).estimated_bytes();
        let a_only = mk(None, Some(one.clone())).estimated_bytes();
        let both = mk(Some(one.clone()), Some(one.clone())).estimated_bytes();
        assert_eq!(b_only, a_only, "the two images are weighed the same way");
        assert!(b_only > empty, "a value must add to the estimate");
        assert_eq!(
            both - empty,
            2 * (b_only - empty),
            "both images are SUMMED — dropping either, or folding them with `*`, \
             makes the sink's memory ceiling describe a different event"
        );

        let two = mk(None, Some(vec![RivetValue::Int(1), RivetValue::Int(2)])).estimated_bytes();
        assert!(two > a_only, "the per-cell estimates are summed, not maxed");
    }
}

#[cfg(test)]
mod prior_run {
    use super::PriorRun;

    /// `any` is the OR of the two kinds of evidence, over all four combinations.
    #[test]
    fn any_is_true_when_either_kind_of_evidence_was_found() {
        for (checkpoint, snapshot, any) in [
            (false, false, false),
            (true, false, true),
            (false, true, true),
            (true, true, true),
        ] {
            assert_eq!(
                PriorRun {
                    checkpoint,
                    snapshot
                }
                .any(),
                any,
                "checkpoint={checkpoint} snapshot={snapshot}"
            );
        }
    }
}

#[cfg(test)]
mod setup_hint_doc_pointers {
    /// cdc.md names the transaction caps and the opt-in spill the adapters actually read.
    #[test]
    fn cdc_reference_names_the_tx_caps_and_the_opt_in_spill() {
        let doc = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/reference/cdc.md"),
        )
        .unwrap();
        for var in [
            "RIVET_CDC_MAX_TX_ROWS",
            "RIVET_CDC_MAX_TX_BYTES",
            "RIVET_CDC_SPILL_DIR",
        ] {
            assert!(doc.contains(var), "cdc.md must name {var}");
        }
        assert!(!doc.contains("Spilling oversized transactions to disk is roadmap"));
    }

    /// Every setup hint names a docs section that exists as a heading in the file it names.
    #[test]
    fn every_setup_hint_points_at_a_heading_that_exists() {
        for hint in [
            super::MYSQL_CDC_HINT,
            super::PG_CDC_HINT,
            super::MSSQL_CDC_HINT,
            super::ORACLE_CDC_HINT,
            super::MONGO_CDC_HINT,
        ] {
            let (_, tail) = hint.split_once("see the '").expect(hint);
            let (section, path) = tail.split_once("' section of ").expect(hint);
            let doc = std::fs::read_to_string(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path),
            )
            .unwrap_or_else(|e| panic!("{path}: {e}"));
            assert!(
                doc.lines()
                    .any(|l| l.starts_with('#') && l.contains(section)),
                "no heading '{section}' in {path}"
            );
        }
    }
}

#[cfg(test)]
mod setup_hint {
    /// Both directions, because only the pair says anything.
    #[test]
    fn a_setup_hint_is_added_to_driver_errors_and_withheld_from_rivets_own_verdicts() {
        let hint = super::PG_CDC_HINT;

        let driver = anyhow::anyhow!("db error: ERROR: logical decoding requires wal_level >= lo");
        let wrapped = format!("{:#}", super::with_setup_hint(driver, hint));
        assert!(
            wrapped.starts_with("if this is a permissions/setup error"),
            "an opaque driver error must keep its setup hint, or removing the wrap \
             from rivet's verdicts would silently take it from the case it exists \
             for. Got: {wrapped}"
        );

        let ours = anyhow::anyhow!(
            "pg cdc: slot 'x' is missing but a resume checkpoint exists — the changes \
             since then are no longer in the log. Re-snapshot the table (mode: full)"
        );
        let kept = format!("{:#}", super::with_setup_hint(ours, hint));
        assert!(
            kept.starts_with("pg cdc: slot 'x' is missing"),
            "rivet's own verdict must LEAD. Prefixed, the operator reads a wal_level \
             troubleshooting line while `re-snapshot, the data is gone` sits past a \
             colon at the end. Got: {kept}"
        );

        let sink =
            anyhow::anyhow!("cdc: rivet.t: a row image carries 1 value(s) under 3 column name(s)");
        assert!(
            format!("{:#}", super::with_setup_hint(sink, hint)).starts_with("cdc: rivet.t"),
            "an engine-agnostic sink refusal is rivet's verdict as much as an \
             engine-prefixed one"
        );
    }

    /// The hint is chosen by the error's code and by an engine `cdc` segment of the message, never by a `cdc:` that a driver message happens to echo inside a word.
    #[test]
    fn the_setup_hint_is_chosen_by_code_then_by_the_prefix_not_by_a_substring() {
        let hint = super::PG_CDC_HINT;
        let hinted = |e: anyhow::Error| {
            format!("{:#}", super::with_setup_hint(e, hint))
                .starts_with("if this is a permissions/setup error")
        };
        assert!(
            !hinted(crate::error::coded(
                crate::error::codes::SOURCE_CDC_LOG_GAP,
                "the stop says nothing of cdc"
            )),
            "a coded refusal is rivet's verdict whatever its words"
        );
        assert!(
            hinted(anyhow::anyhow!(
                "db error: relation \"dns_cdc: zones\" does not exist"
            )),
            "a driver message that echoes `cdc:` mid-word is still an opaque driver error"
        );
        for opaque in [
            "db error: relation the orders cdc: does not exist",
            "db error:  cdc: stray",
        ] {
            assert!(
                hinted(anyhow::anyhow!(opaque)),
                "a segment ending ` cdc` after a spaced or empty engine is not rivet's prefix: {opaque}"
            );
        }
        for rivet_own in [
            "oracle cdc: the redo log changed",
            "sqlserver cdc: capture job stopped",
            "cdc: rivet.t: a row image carries 1 value(s)",
            "opening the stream: pg cdc: slot 'x' is missing",
        ] {
            assert!(!hinted(anyhow::anyhow!(rivet_own)), "{rivet_own}");
        }
        let wrapper = crate::error::coded(
            crate::error::codes::SOURCE_QUERY_FAILED,
            "permission denied for table t",
        );
        assert!(hinted(wrapper), "a driver-failure wrapper keeps the hint");
    }

    /// A redo log that kept changing under LogMiner reaches the operator as that, never as a grants problem.
    #[cfg(feature = "oracle")]
    #[test]
    fn an_oracle_log_set_change_does_not_get_the_setup_hint() {
        let driver = || anyhow::anyhow!("oracle: ORA-01368: Redo log file header mismatch");
        let shown = format!(
            "{:#}",
            super::with_setup_hint(
                crate::source::oracle::cdc::log_set_changed_error(driver()),
                super::ORACLE_CDC_HINT
            )
        );
        assert!(
            shown.starts_with("oracle cdc: the redo log changed under LogMiner"),
            "{shown}"
        );
        assert!(
            format!(
                "{:#}",
                super::with_setup_hint(driver(), super::ORACLE_CDC_HINT)
            )
            .starts_with("if this is a permissions/setup error"),
            "the bare driver error is what got the hint before the wrap"
        );
    }

    #[test]
    fn a_tls_handshake_failure_does_not_get_the_setup_hint() {
        let tls = anyhow::anyhow!("error performing TLS handshake: server does not support TLS");
        let e = super::with_setup_hint(tls, super::PG_CDC_HINT);
        assert_eq!(
            format!("{e:#}"),
            "TLS handshake failed — the server does not speak TLS or its certificate is not \
             trusted: set `tls.ca_file` for a private CA, or `tls.mode: disable` if the server \
             has no TLS (trusted networks only); retrying will not help: error performing TLS \
             handshake: server does not support TLS"
        );
        assert!(!crate::pipeline::retry::classify_error(&e).is_transient());
    }

    /// Asserts a CDC open against a refused loopback port leads with the port, not the setup hint.
    fn assert_names_refused_port(open: anyhow::Result<()>, hint: &'static str) {
        let e = open.expect_err("nothing listens on 127.0.0.1:1");
        let shown = format!("{:#}", super::with_setup_hint(e, hint));
        assert!(
            shown.starts_with("nothing is listening on 127.0.0.1:1"),
            "a refused port must be named first, not buried behind a setup hint. Got: {shown}"
        );
    }

    #[test]
    fn pg_cdc_open_names_the_refused_port() {
        let open = crate::source::postgres::cdc::PgChangeStream::open(
            "postgresql://u:p@127.0.0.1:1/db",
            "s",
            super::PriorRun::default(),
            None,
            super::PeekBound::Unbounded,
            super::DrainMode::Continuous,
            &[],
            None,
            None,
            None,
        );
        assert_names_refused_port(open.map(drop), super::PG_CDC_HINT);
    }

    #[test]
    fn mysql_cdc_open_names_the_refused_port() {
        let open = crate::source::mysql::cdc::MysqlChangeStream::open_or_resume(
            "mysql://u:p@127.0.0.1:1/db",
            1,
            None,
            super::DrainMode::Continuous,
            None,
            vec![],
            None,
            None,
        );
        assert_names_refused_port(open.map(drop), super::MYSQL_CDC_HINT);
    }

    #[test]
    fn mssql_cdc_open_names_the_refused_port() {
        let open = crate::source::mssql::cdc::MssqlChangeStream::from_url(
            "sqlserver://u:p@127.0.0.1:1/db",
            "dbo_t",
            crate::source::mssql::cdc::resume_from_checkpoint(None, "").unwrap(),
            None,
            super::PeekBound::Unbounded,
            super::DrainMode::Continuous,
            &[],
            None,
            None,
        );
        assert_names_refused_port(open.map(drop), super::MSSQL_CDC_HINT);
    }

    #[test]
    fn tiberius_config_dials_the_host_and_port_it_was_given() {
        let c =
            crate::source::mssql::tiberius_config("db.example", 14330, "d", "u", &"p".into(), None);
        assert_eq!(c.get_addr(), "db.example:14330");
    }
}

#[cfg(test)]
mod engine_identity {
    use super::{CdcEngine, CdcEngineOpts};
    use crate::config::SourceType;

    #[test]
    fn the_opts_variant_source_type_and_url_scheme_name_one_engine() {
        let t = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        for (opts, st, url) in [
            (
                CdcEngineOpts::Mysql {
                    server_id: 1,
                    configured_tables: t(&["a"]),
                    resolved_grants: None,
                },
                SourceType::Mysql,
                "mysql://h/d",
            ),
            (
                CdcEngineOpts::Postgres {
                    slot: "s".into(),
                    configured_tables: t(&["a"]),
                    resolved_grants: None,
                },
                SourceType::Postgres,
                "postgresql://h/d",
            ),
            (
                CdcEngineOpts::Mssql {
                    capture_instance: None,
                    configured_tables: t(&["a"]),
                    resolved_grants: None,
                },
                SourceType::Mssql,
                "sqlserver://h/d",
            ),
            (
                CdcEngineOpts::Oracle {
                    configured_tables: t(&["a"]),
                    resolved_grants: None,
                },
                SourceType::Oracle,
                "ORACLE://h/d",
            ),
            (
                CdcEngineOpts::Mongo {
                    canonical: false,
                    configured_tables: t(&["a"]),
                    keyed_tables: Vec::new(),
                    resolved_grants: None,
                },
                SourceType::Mongo,
                "mongodb+srv://h/d",
            ),
        ] {
            let engine = opts.engine();
            assert_eq!(engine, CdcEngine::from(st), "{url}");
            assert_eq!(engine.source_type(), st, "{url}");
            assert_eq!(CdcEngine::from_url(url).unwrap(), engine, "{url}");
            assert_eq!(engine.label(), st.label(), "{url}");
        }
        assert!(CdcEngine::from_url("redis://h").is_err());
    }
}

#[cfg(test)]
mod tests {

    /// `--max-events` must not be able to WEDGE a checkpointed NDJSON run, and must still CAP.
    #[test]
    fn max_events_stops_at_the_first_boundary_past_the_cap_and_checkpoints_there() {
        use super::*;
        use std::collections::VecDeque;

        struct Fake(VecDeque<ChangeEvent>);
        impl ChangeStream for Fake {
            fn engine(&self) -> CdcEngine {
                CdcEngine::Postgres
            }

            fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
                self.0.pop_front().map(Ok)
            }
        }
        let ev = |id: i64, committed: bool| ChangeEvent {
            op: ChangeOp::Insert,
            schema: "s".into(),
            table: "t".into(),
            before: None,
            after: Some(vec![crate::source::cdc::value::RivetValue::Int(id)]),
            position: Position::new(serde_json::json!({ "lsn": format!("{id:08X}") })),
            committed,
            image_names: None,
            seq: 0,
            poison: None,
            row_id: None,
            before_names: None,
            before_poison: None,
            image_labels: None,
        };
        let mut stream = Fake(
            [
                ev(1, false),
                ev(2, false),
                ev(3, true),
                ev(4, false),
                ev(5, false),
                ev(6, true),
            ]
            .into_iter()
            .collect(),
        );

        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("ck");
        run(&mut stream, Some(ckpt.clone()), Vec::new(), Some(2)).expect("run");

        assert!(
            ckpt.exists(),
            "a cap landing inside a transaction must still reach the boundary and \
             checkpoint — otherwise the next run resumes at the same place and the \
             export never progresses"
        );
        let saved = Position::load(&ckpt).expect("load").expect("some");
        assert_eq!(
            saved.0["lsn"], "00000003",
            "the cap must stop at tx1's COMMIT — 00000006 means it ran to the end of \
             the stream and capped nothing, 00000001/2 would mean it saved a \
             mid-transaction position"
        );
        assert_eq!(
            stream.0.len(),
            3,
            "tx2 must be left UNCONSUMED for the next run; draining it means the cap \
             stopped nothing"
        );
    }

    #[test]
    fn enrich_uses_the_qualified_schema_not_the_connection_default() {
        use super::enrich_schema_and_table;
        assert_eq!(
            enrich_schema_and_table("otherdb.orders", Some("conndb")),
            ("otherdb".to_string(), "orders".to_string())
        );
        assert_eq!(
            enrich_schema_and_table("orders", Some("conndb")),
            ("conndb".to_string(), "orders".to_string())
        );
        assert_eq!(
            enrich_schema_and_table("orders", None),
            (String::new(), "orders".to_string())
        );
    }

    #[test]
    fn drain_mode_maps_the_config_bool_and_bounds() {
        use super::DrainMode;
        assert_eq!(
            DrainMode::from_until_current(true),
            DrainMode::BoundedAtOpen
        );
        assert_eq!(DrainMode::from_until_current(false), DrainMode::Continuous);
        assert!(DrainMode::BoundedAtOpen.is_bounded());
        assert!(!DrainMode::Continuous.is_bounded());
    }

    /// Finding #43: `rivet init --mode cdc` scaffolds `checkpoint.
    #[test]
    fn corrupt_checkpoint_fails_loud_not_silently_absent() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("ck.json");
        std::fs::write(&path, b"{not valid json").unwrap();
        let err = Position::load(&path).unwrap_err();
        assert!(
            err.to_string().contains("corrupt or truncated"),
            "a corrupt checkpoint must fail loud, not read as absent: {err}"
        );
        assert_eq!(crate::error::classify_exit(&err), 5, "{err}");
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_SOURCE_CDC_CHECKPOINT_INVALID")
        );
        assert_eq!(
            err.to_string(),
            format!(
                "checkpoint '{}' is corrupt or truncated (not valid JSON: key must be a string \
                 at line 1 column 2) — refusing to silently treat it as absent and re-anchor \
                 CDC at 'current', which would permanently skip every change since the last \
                 checkpoint. Restore the file, or: {}",
                path.display(),
                checkpoint_identity::RECOVER
            )
        );

        assert!(
            Position::load(&d.path().join("absent.json"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn mysql_corrupt_checkpoint_error_is_not_masked_by_the_grants_hint() {
        let d = tempfile::tempdir().unwrap();
        let ckpt = d.path().join("ck.json");
        std::fs::write(&ckpt, b"{not valid json").unwrap();
        let cfg = CdcConfig {
            config_dir: std::path::PathBuf::from("."),
            url: "mysql://rivet:rivet@127.0.0.1:1/rivet".into(),
            checkpoint: Some(ckpt),
            drain: DrainMode::BoundedAtOpen,
            tls: None,
            engine: CdcEngineOpts::Mysql {
                server_id: 4321,
                configured_tables: Vec::new(),
                resolved_grants: None,
            },
        };
        let err = match create_change_stream(&cfg, PeekBound::Unbounded) {
            Ok(_) => panic!("a corrupt checkpoint must error, not open a stream"),
            Err(e) => e,
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("corrupt or truncated"),
            "the error must be the clean corrupt-checkpoint message: {msg}"
        );
        assert!(
            !msg.contains("REPLICATION SLAVE") && !msg.contains("binlog_format"),
            "a checkpoint-file error must NOT carry the binlog-grants hint: {msg}"
        );
    }

    #[test]
    fn mysql_hollow_checkpoint_refusal_is_not_masked_by_the_grants_hint() {
        let d = tempfile::tempdir().unwrap();
        let ckpt = d.path().join("ck.json");
        for hollow in [&b"{\"pos\":4}"[..], &b"{\"file\":\"binlog.000001\"}"[..]] {
            std::fs::write(&ckpt, hollow).unwrap();
            let cfg = CdcConfig {
                config_dir: std::path::PathBuf::from("."),
                url: "mysql://rivet:rivet@127.0.0.1:1/rivet".into(),
                checkpoint: Some(ckpt.clone()),
                drain: DrainMode::BoundedAtOpen,
                tls: None,
                engine: CdcEngineOpts::Mysql {
                    server_id: 4321,
                    configured_tables: Vec::new(),
                    resolved_grants: None,
                },
            };
            let Err(err) = create_change_stream(&cfg, PeekBound::Unbounded) else {
                panic!("a hollow checkpoint must be refused, not open a stream");
            };
            let msg = format!("{err:#}");
            assert!(
                msg.starts_with("checkpoint '") && !msg.contains("permissions/setup"),
                "the checkpoint refusal must come first, with no grants hint: {msg}"
            );
            assert_eq!(crate::error::classify_exit(&err), 5, "{msg}");
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_SOURCE_CDC_CHECKPOINT_INVALID")
            );
        }
    }

    #[cfg(feature = "oracle")]
    #[test]
    fn the_oracle_stream_refuses_only_the_continuous_drain() {
        let d = tempfile::tempdir().unwrap();
        let ckpt = d.path().join("ck.json");
        std::fs::write(&ckpt, b"{not valid json").unwrap();
        let open = |drain| {
            let cfg = CdcConfig {
                config_dir: std::path::PathBuf::from("."),
                url: "oracle://u:p@127.0.0.1:1/FREEPDB1".into(),
                checkpoint: Some(ckpt.clone()),
                drain,
                tls: None,
                engine: CdcEngineOpts::Oracle {
                    configured_tables: Vec::new(),
                    resolved_grants: None,
                },
            };
            match create_change_stream(&cfg, PeekBound::Unbounded) {
                Ok(_) => panic!("a corrupt checkpoint never opens a stream"),
                Err(e) => format!("{e:#}"),
            }
        };
        assert!(
            open(DrainMode::Continuous).contains("Oracle CDC is always a bounded drain"),
            "the continuous drain is refused before anything is read"
        );
        let bounded = open(DrainMode::BoundedAtOpen);
        assert!(
            bounded.contains("corrupt or truncated"),
            "the bounded drain goes on to read the checkpoint: {bounded}"
        );
    }

    #[test]
    fn a_missing_mssql_capture_instance_names_the_config_key() {
        let cfg = CdcConfig {
            config_dir: std::path::PathBuf::from("."),
            url: "sqlserver://sa:x@127.0.0.1:1/rivet".into(),
            checkpoint: None,
            drain: DrainMode::BoundedAtOpen,
            tls: None,
            engine: CdcEngineOpts::Mssql {
                capture_instance: None,
                configured_tables: Vec::new(),
                resolved_grants: None,
            },
        };
        let err = match create_change_stream(&cfg, PeekBound::Unbounded) {
            Ok(_) => panic!("no capture instance must error"),
            Err(e) => format!("{e:#}"),
        };
        assert!(
            err.contains("set `cdc.capture_instance:` on the export"),
            "{err}"
        );
    }

    #[test]
    fn checkpoint_save_creates_missing_parent_directories() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("cdc").join("nested").join("orders.ckpt");
        let pos = Position::new(serde_json::json!({"file": "binlog.000001", "pos": 4}));
        pos.save(&path).expect("save must create parents");
        let loaded = Position::load(&path).unwrap().expect("roundtrip");
        assert_eq!(loaded.0["pos"], 4);
        let blocker = d.path().join("blocked");
        std::fs::write(&blocker, b"file").unwrap();
        let bad = blocker.join("x.ckpt");
        let err = pos.save(&bad).unwrap_err().to_string();
        assert!(
            err.contains("checkpoint directory"),
            "the failure must name the real cause: {err}"
        );
    }

    use super::*;

    #[test]
    fn ensure_anchor_missing_checkpoint_with_evidence_fails_loudly() {
        let d = tempfile::tempdir().unwrap();
        let missing = d.path().join("nonexistent.ckpt");
        for engine in [
            CdcEngine::Mysql,
            CdcEngine::Mssql,
            CdcEngine::Mongo,
            CdcEngine::Oracle,
        ] {
            let err = engine
                .ensure_anchor(
                    "mysql://u:p@127.0.0.1:1/db",
                    "unused",
                    None,
                    Some(&missing),
                    None,
                    PriorRun {
                        checkpoint: false,
                        snapshot: true,
                    },
                )
                .expect_err("missing checkpoint + evidence must bail, not re-pin");
            let msg = err.to_string();
            assert!(
                msg.contains("prior-run evidence"),
                "{engine:?}: must explain the evidence: {msg}"
            );
            assert!(msg.ends_with(checkpoint_identity::RECOVER), "{msg}");
            assert_eq!(crate::error::classify_exit(&err), 5, "{engine:?}");
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_SOURCE_CDC_CHECKPOINT_INVALID"),
                "{engine:?}"
            );
        }
    }

    #[test]
    fn resolve_cdc_columns_rejects_a_non_identifier_table() {
        for bad in ["orders; DROP TABLE x", "orders WHERE 1=1", "a b", "o'r", ""] {
            let err = resolve_cdc_columns(
                "mysql://u:p@127.0.0.1:3306/db",
                bad,
                None,
                &crate::types::ColumnOverrides::new(),
            )
            .expect_err(&format!("{bad:?} must be rejected"));
            assert!(
                err.to_string().contains("plain [schema.]table identifier"),
                "{bad:?} → {err}"
            );
        }
    }

    fn framer_ev(id: i64) -> super::ChangeEvent {
        super::ChangeEvent {
            op: super::ChangeOp::Insert,
            schema: "public".into(),
            table: "orders".into(),
            before: None,
            after: Some(vec![RivetValue::Int(id)]),
            position: Position::new(serde_json::json!({ "lsn": "stale" })),
            committed: false,
            image_names: None,
            seq: 0,
            poison: None,
            row_id: None,
            before_names: None,
            before_poison: None,
            image_labels: None,
        }
    }

    /// #158: the shared transaction close — commit position on ALL, committed on the LAST only.
    #[test]
    fn txn_framer_close_group_marks_only_the_last_committed() {
        let commit = Position::new(serde_json::json!({ "lsn": "COMMIT" }));

        let mut g: Vec<super::ChangeEvent> = (0..4).map(framer_ev).collect();
        super::TxnFramer::close_group(&mut g, &commit);
        assert!(g.iter().all(|e| e.position == commit), "commit on all");
        assert_eq!(
            g.iter().map(|e| e.committed).collect::<Vec<_>>(),
            vec![false, false, false, true],
            "only the last event of a transaction is committed"
        );

        let mut one = vec![framer_ev(9)];
        super::TxnFramer::close_group(&mut one, &commit);
        assert!(one[0].committed && one[0].position == commit);

        let c1 = Position::new(serde_json::json!({ "lsn": "C1" }));
        let c2 = Position::new(serde_json::json!({ "lsn": "C2" }));
        let mut a: Vec<super::ChangeEvent> = (0..2).map(framer_ev).collect();
        let mut b: Vec<super::ChangeEvent> = (0..3).map(framer_ev).collect();
        super::TxnFramer::close_group(&mut a, &c1);
        super::TxnFramer::close_group(&mut b, &c2);
        assert_eq!(a.iter().filter(|e| e.committed).count(), 1);
        assert_eq!(b.iter().filter(|e| e.committed).count(), 1);
        assert!(a.iter().all(|e| e.position == c1));
        assert!(b.iter().all(|e| e.position == c2));

        let mut empty: Vec<super::ChangeEvent> = Vec::new();
        super::TxnFramer::close_group(&mut empty, &commit);
        assert!(empty.is_empty());
    }

    /// Mongo's named single-event-commit model.
    #[test]
    fn txn_framer_single_event_commit_marks_the_event() {
        let mut e = framer_ev(1);
        assert!(!e.committed);
        super::TxnFramer::single_event_commit(&mut e);
        assert!(e.committed);
    }

    #[test]
    fn txn_seq_ordinals_reset_when_commit_position_changes() {
        let mut ts = TxnSeq::default();
        let pa = Position::new(serde_json::json!({ "lsn": "A" }));
        let pb = Position::new(serde_json::json!({ "lsn": "B" }));
        let seqs: Vec<u64> = [&pa, &pa, &pa, &pb, &pb]
            .iter()
            .map(|p| ts.next(p))
            .collect();
        assert_eq!(seqs, vec![0, 1, 2, 0, 1]);

        assert_eq!(ts.next(&pb), 2);
        assert_eq!(
            ts.next(&Position::new(serde_json::json!({ "lsn": "C" }))),
            0
        );
    }

    struct OneShot(Option<super::ChangeEvent>);
    impl super::ChangeStream for OneShot {
        fn engine(&self) -> super::CdcEngine {
            super::CdcEngine::Postgres
        }

        fn next_change(&mut self) -> Option<Result<super::ChangeEvent>> {
            self.0.take().map(Ok)
        }
    }

    fn poison_event(table: &str) -> super::ChangeEvent {
        super::ChangeEvent {
            op: super::ChangeOp::Update,
            schema: "public".into(),
            table: table.into(),
            before: None,
            after: Some(vec![RivetValue::Bytes(b"unchanged-toast-datum".to_vec())]),
            position: Position::new(serde_json::json!({ "lsn": "0/ABC" })),
            committed: true,
            image_names: None,
            seq: 0,
            poison: Some(
                "pg cdc: public.orders: column [big] unchanged-TOAST — REPLICA IDENTITY FULL"
                    .into(),
            ),
            row_id: None,
            before_names: None,
            before_poison: None,
            image_labels: None,
        }
    }

    #[test]
    fn ndjson_run_raises_poison_for_a_captured_table() {
        let mut s = OneShot(Some(poison_event("orders")));
        let err = super::run(&mut s, None, vec!["orders".into()], None)
            .expect_err("captured poison must bail");
        assert!(
            format!("{err:#}").contains("REPLICA IDENTITY FULL"),
            "got: {err:#}"
        );
    }

    /// A stream of events that records every position it is acked at.
    struct Acked(std::collections::VecDeque<super::ChangeEvent>, Vec<String>);
    impl super::ChangeStream for Acked {
        fn engine(&self) -> super::CdcEngine {
            super::CdcEngine::Postgres
        }
        fn next_change(&mut self) -> Option<Result<super::ChangeEvent>> {
            self.0.pop_front().map(Ok)
        }
        fn ack(&mut self, position: &Position) -> Result<()> {
            self.1
                .push(position.json()["lsn"].as_str().unwrap().to_string());
            Ok(())
        }
    }

    /// One clean event on `table` at `lsn`.
    fn event_at(table: &str, lsn: &str, committed: bool) -> super::ChangeEvent {
        super::ChangeEvent {
            position: Position::new(serde_json::json!({ "lsn": lsn })),
            committed,
            poison: None,
            ..poison_event(table)
        }
    }

    /// The stdout driver acks once, at the last commit boundary it passed.
    #[test]
    fn the_ndjson_driver_acks_the_last_commit_it_emitted_or_passed() {
        let drain = |events: Vec<super::ChangeEvent>, max: Option<usize>| {
            let mut s = Acked(events.into(), Vec::new());
            super::run(&mut s, None, vec!["orders".into()], max).expect("a clean drain");
            s.1
        };
        assert_eq!(
            drain(
                vec![
                    event_at("orders", "0/1", true),
                    event_at("orders", "0/2", false),
                    event_at("orders", "0/3", true),
                ],
                None
            ),
            ["0/3"]
        );
        assert_eq!(
            drain(
                vec![
                    event_at("orders", "0/1", true),
                    event_at("other", "0/2", true)
                ],
                None
            ),
            ["0/2"],
            "a commit on an unlisted table is a boundary too"
        );
        assert_eq!(
            drain(
                vec![
                    event_at("orders", "0/1", true),
                    event_at("orders", "0/2", true)
                ],
                Some(1)
            ),
            ["0/1"],
            "the cap stops the drain, and the ack, at the first commit"
        );
        assert!(
            drain(vec![event_at("orders", "0/1", false)], None).is_empty(),
            "an open transaction is never acked"
        );
        assert!(drain(vec![event_at("orders", "0/1", true)], Some(0)).is_empty());
    }

    /// A drain that fails still acks what it printed, and reports the failure.
    #[test]
    fn the_ndjson_driver_acks_what_it_printed_before_a_failure() {
        let mut s = Acked(
            vec![event_at("orders", "0/1", true), poison_event("orders")].into(),
            Vec::new(),
        );
        super::run(&mut s, None, vec!["orders".into()], None).expect_err("the poison");
        assert_eq!(s.1, ["0/1"]);
    }

    /// A stream of events with a fixed open-time image verdict.
    struct Imaged(
        std::collections::VecDeque<super::ChangeEvent>,
        super::RowImage,
    );
    impl super::ChangeStream for Imaged {
        fn engine(&self) -> super::CdcEngine {
            super::CdcEngine::Mysql
        }
        fn row_image(&mut self, _tables: &[String]) -> super::RowImage {
            self.1.clone()
        }
        fn next_change(&mut self) -> Option<Result<super::ChangeEvent>> {
            self.0.pop_front().map(Ok)
        }
    }

    /// One update on `table` naming four columns and carrying `n` values in its after image.
    fn named_update(table: &str, lsn: &str, n: usize) -> super::ChangeEvent {
        super::ChangeEvent {
            after: Some((0..n as i64).map(RivetValue::Int).collect()),
            image_names: Some(std::sync::Arc::from(
                ["id", "a", "b", "c"].map(String::from).to_vec(),
            )),
            ..event_at(table, lsn, true)
        }
    }

    /// An update on `orders` with a whole four-value after image and a before image of `n` values named by `names` (the event's own four names when `None`).
    fn update_with_before(n: usize, names: Option<&[&str]>) -> super::ChangeEvent {
        super::ChangeEvent {
            before: Some((0..n as i64).map(RivetValue::Int).collect()),
            before_names: names
                .map(|v| std::sync::Arc::from(v.iter().map(|s| s.to_string()).collect::<Vec<_>>())),
            ..named_update("orders", "0/1", 4)
        }
    }

    /// A before image short of its names is refused by code with no line and no checkpoint.
    #[test]
    fn the_ndjson_driver_refuses_a_partial_before_image_and_prints_a_named_or_whole_one() {
        let drive = |ev: super::ChangeEvent| {
            let dir = tempfile::tempdir().unwrap();
            let ck = dir.path().join("ck.json");
            let mut s = Imaged(vec![ev].into(), super::RowImage::Whole);
            let out = super::run(&mut s, Some(ck.clone()), vec!["orders".into()], None);
            (out, ck.exists(), s.0.len())
        };
        let (out, ckpt, left) = drive(update_with_before(1, None));
        let err = out.expect_err("a before image of 1 value under 4 names is partial");
        assert!(
            format!("{err:#}").contains("a row image carries 1 value(s) under 4 column name(s)"),
            "got: {err:#}"
        );
        assert_eq!(
            crate::error::code_of(&err),
            Some(crate::error::codes::SOURCE_CDC_PREREQUISITE)
        );
        assert!(!ckpt, "the checkpoint must not move past a refused event");
        assert_eq!(left, 0, "the event was consumed and refused");
        let (out, ckpt, _) = drive(update_with_before(1, Some(&["id"])));
        assert!(
            out.is_ok() && ckpt,
            "a key-only before image named by before_names prints: {out:?}"
        );
        let (out, ckpt, _) = drive(update_with_before(4, None));
        assert!(out.is_ok() && ckpt, "a whole before image prints: {out:?}");
        let (out, _, _) = drive(update_with_before(3, Some(&["id"])));
        assert!(
            out.is_err(),
            "a before image wider than its own names is refused"
        );
    }

    /// The stdout driver refuses a partial image by code, acks nothing, and leaves the checkpoint file unwritten.
    #[test]
    fn the_ndjson_driver_refuses_a_partial_image_by_code_and_keeps_the_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let ck = dir.path().join("ck.json");
        let mut s = Imaged(
            vec![named_update("orders", "0/1", 1)].into(),
            super::RowImage::Whole,
        );
        let err = super::run(&mut s, Some(ck.clone()), vec!["orders".into()], None)
            .expect_err("a partial image must stop the run");
        let text = format!("{err:#}");
        assert!(
            text.contains("a row image carries 1 value(s) under 4 column name(s)")
                && text.contains("a PARTIAL image")
                && text.contains(super::checkpoint_identity::RECOVER),
            "got: {text}"
        );
        assert_eq!(
            crate::error::code_of(&err),
            Some(crate::error::codes::SOURCE_CDC_PREREQUISITE)
        );
        assert!(
            !ck.exists(),
            "the checkpoint must not move past a refused event"
        );
    }

    /// Whole images, partial images of an unlisted table and key-only deletes print as before.
    #[test]
    fn the_ndjson_driver_still_prints_whole_unlisted_and_key_only_images() {
        let drained = |events: Vec<super::ChangeEvent>, image: super::RowImage| {
            let mut s = Imaged(events.into(), image);
            let out = super::run(&mut s, None, vec!["orders".into()], None);
            assert!(out.is_ok(), "got: {out:?}");
            s.0.len()
        };
        assert_eq!(
            drained(
                vec![named_update("orders", "0/1", 4)],
                super::RowImage::Whole
            ),
            0
        );
        assert_eq!(
            drained(
                vec![named_update("other", "0/1", 1)],
                super::RowImage::Whole
            ),
            0
        );
        assert_eq!(
            drained(
                vec![named_update("orders", "0/1", 4)],
                super::RowImage::KeyOnlyDeletes { why: "k".into() }
            ),
            0
        );
    }

    /// A partial open-time verdict stops the run before any event is read.
    #[test]
    fn the_ndjson_driver_refuses_a_partial_open_time_image_before_reading() {
        let mut s = Imaged(
            vec![named_update("orders", "0/1", 4)].into(),
            super::RowImage::Partial {
                why: "binlog_row_image = MINIMAL".into(),
            },
        );
        let err = super::run(&mut s, None, vec!["orders".into()], None).expect_err("partial");
        assert!(
            format!("{err:#}").contains("binlog_row_image = MINIMAL"),
            "got: {err:#}"
        );
        assert_eq!(
            crate::error::code_of(&err),
            Some(crate::error::codes::SOURCE_CDC_PREREQUISITE)
        );
        assert_eq!(s.0.len(), 1, "no event was consumed");
    }

    /// A declared key column is found by exact or case-folded name; the first missing one is named.
    #[test]
    fn an_undeclared_key_column_is_named_and_a_case_folded_one_is_found() {
        let cols: Vec<crate::types::TypeMapping> = ["ID", "code"]
            .iter()
            .map(|n| crate::types::TypeMapping {
                column_name: n.to_string(),
                source_native_type: "bigint".into(),
                rivet_type: crate::types::RivetType::Int64,
                arrow_type: None,
                fidelity: crate::types::TypeFidelity::Exact,
                nullable: true,
                warnings: vec![],
                delivery: crate::types::Delivery::Native,
                refusal: None,
            })
            .collect();
        let k = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            super::undeclared_key_column(&k(&["id", "code"]), &cols),
            None
        );
        assert_eq!(super::undeclared_key_column(&k(&[]), &cols), None);
        assert_eq!(
            super::undeclared_key_column(&k(&["code", "sku", "x"]), &cols),
            Some("sku")
        );
    }

    /// The resident cost counts an event's own old-image names, row id and old-image poison.
    #[test]
    fn estimated_bytes_counts_before_names_row_id_and_before_poison() {
        let bare = super::ChangeEvent {
            op: super::ChangeOp::Update,
            schema: "public".into(),
            table: "t".into(),
            before: Some(vec![RivetValue::Int(1)]),
            after: Some(vec![RivetValue::Int(2)]),
            position: Position::new(serde_json::json!({ "lsn": "0/ABC" })),
            committed: true,
            image_names: None,
            seq: 0,
            poison: None,
            row_id: None,
            before_names: None,
            before_poison: None,
            image_labels: None,
        };
        let base = bare.estimated_bytes();
        let names = super::ChangeEvent {
            before_names: Some(vec!["identifier".to_string()].into()),
            ..bare.clone()
        };
        assert_eq!(
            names.estimated_bytes(),
            base + std::mem::size_of::<String>() + "identifier".len()
        );
        let shared = names.before_names.clone();
        assert_eq!(
            names.estimated_bytes(),
            base + (std::mem::size_of::<String>() + "identifier".len()) / 2,
            "an Arc held twice charges each holder half"
        );
        drop(shared);
        let ids = super::ChangeEvent {
            row_id: Some("AAAVrg".into()),
            before_poison: Some("bad".into()),
            ..bare
        };
        assert_eq!(ids.estimated_bytes(), base + 6 + 3);
    }

    /// The resident cost counts each wire-label column's own size, its name and its labels, and shares an Arc between its holders.
    #[test]
    fn estimated_bytes_counts_image_labels_and_splits_a_shared_arc() {
        let bare = super::ChangeEvent {
            op: super::ChangeOp::Insert,
            schema: "public".into(),
            table: "t".into(),
            before: None,
            after: Some(vec![RivetValue::Int(2)]),
            position: Position::new(serde_json::json!({ "lsn": "0/ABC" })),
            committed: true,
            image_names: None,
            seq: 0,
            poison: None,
            row_id: None,
            before_names: None,
            before_poison: None,
            image_labels: None,
        };
        let base = bare.estimated_bytes();
        let labels: std::sync::Arc<[super::value::WireLabels]> = vec![
            super::value::WireLabels {
                column: "mood".into(),
                labels: Some(vec!["a".into(), "bcd".into()]),
            },
            super::value::WireLabels {
                column: "xyz".into(),
                labels: None,
            },
        ]
        .into();
        let own = 2 * std::mem::size_of::<super::value::WireLabels>()
            + "mood".len()
            + "xyz".len()
            + 2 * std::mem::size_of::<String>()
            + "a".len()
            + "bcd".len();
        let ev = super::ChangeEvent {
            image_labels: Some(labels.clone()),
            ..bare
        };
        drop(labels);
        assert_eq!(ev.estimated_bytes(), base + own);
        let sibling = ev.clone();
        assert_eq!(
            ev.estimated_bytes(),
            base + own / 2,
            "an Arc held twice charges each holder half"
        );
        drop(sibling);
    }

    /// A key-only pre-image prints as carried, named by `before_columns`; a key move stays one update.
    #[test]
    fn an_ndjson_line_names_a_key_only_pre_image_and_does_not_split() {
        let ev = super::ChangeEvent {
            op: super::ChangeOp::Update,
            schema: "public".into(),
            table: "t".into(),
            before: Some(vec![RivetValue::Int(1)]),
            after: Some(vec![RivetValue::Bytes(b"a".to_vec()), RivetValue::Int(2)]),
            position: Position::new(serde_json::json!({ "lsn": "0/ABC" })),
            committed: true,
            image_names: Some(vec!["v".to_string(), "id".to_string()].into()),
            seq: 0,
            poison: None,
            row_id: None,
            before_names: Some(vec!["id".to_string()].into()),
            before_poison: None,
            image_labels: None,
        };
        assert_eq!(
            super::ndjson_line(&ev),
            serde_json::json!({
                "op": "update", "schema": "public", "table": "t",
                "before": [1], "before_columns": ["id"], "after": ["a", 2],
                "pos": { "lsn": "0/ABC" }, "seq": 0,
            })
        );
        let full = super::ChangeEvent {
            before_names: None,
            before_poison: None,
            image_labels: None,
            before: Some(vec![RivetValue::Bytes(b"a".to_vec()), RivetValue::Int(1)]),
            ..ev
        };
        assert!(super::ndjson_line(&full).get("before_columns").is_none());
    }

    /// The NDJSON driver prints the old image, so it raises the old image's deferred refusal too.
    #[test]
    fn ndjson_run_raises_an_old_image_poison_for_a_captured_table() {
        let mut ev = poison_event("orders");
        ev.before_poison = ev.poison.take();
        let mut s = OneShot(Some(ev));
        let err = super::run(&mut s, None, vec!["orders".into()], None)
            .expect_err("an old image NDJSON would print undecodable must bail");
        assert!(
            format!("{err:#}").contains("REPLICA IDENTITY FULL"),
            "got: {err:#}"
        );
    }

    #[test]
    fn ndjson_run_drops_poison_for_an_uncaptured_table() {
        let mut s = OneShot(Some(poison_event("audit_log")));
        super::run(&mut s, None, vec!["orders".into()], None)
            .expect("uncaptured poison must not bail the NDJSON run");
    }

    struct Forbidden;
    impl super::ChangeStream for Forbidden {
        fn engine(&self) -> super::CdcEngine {
            super::CdcEngine::Postgres
        }

        fn next_change(&mut self) -> Option<Result<super::ChangeEvent>> {
            panic!("run must not poll the stream when --max-events 0");
        }
    }

    #[test]
    fn max_events_zero_is_a_true_no_op_never_polls_the_stream() {
        let mut s = Forbidden;
        super::run(&mut s, None, vec!["orders".into()], Some(0))
            .expect("--max-events 0 must be a clean no-op");
    }

    /// The metric's unit and the budget's unit are DIFFERENT, on purpose.
    #[test]
    fn payload_bytes_counts_values_and_estimated_bytes_counts_memory() {
        let mut ev = ChangeEvent {
            op: ChangeOp::Insert,
            schema: "s".into(),
            table: "t".into(),
            before: None,
            after: Some(vec![RivetValue::Int(1), RivetValue::Bytes(vec![b'x'; 100])]),
            position: Position::new(serde_json::json!({ "lsn": "0/1" })),
            committed: false,
            image_names: None,
            seq: 0,
            poison: None,
            row_id: None,
            before_names: None,
            before_poison: None,
            image_labels: None,
        };
        let payload = ev.payload_bytes();
        assert_eq!(
            payload,
            1 + 1 + 8 + 100,
            "payload = schema + table + the values' own bytes, nothing else"
        );
        assert!(
            ev.estimated_bytes() > payload,
            "an event's own resident cost is its values plus the struct that holds them"
        );
        let mut resident = ResidentBytes::default();
        resident.add(&ev);
        assert!(
            resident.bytes() > payload + 400,
            "buffered, the event costs its position too — if resident and payload \
             converge, one of them changed meaning and a metric or a budget is now lying"
        );
        let before = ev.payload_bytes();
        ev.position = Position::new(serde_json::json!({ "lsn": "0/FFFFFFFF", "extra": "x" }));
        assert_eq!(
            ev.payload_bytes(),
            before,
            "the commit position is bookkeeping, not data read from the source"
        );
    }

    /// Spilling is OPT-IN, and every resolved location is CONFIG-anchored.
    #[test]
    fn spilling_is_off_until_a_directory_is_named() {
        use std::path::Path;
        let cfg = Path::new("/etc/rivet");
        let guard = EnvGuard::unset("RIVET_CDC_SPILL_DIR");
        assert_eq!(
            spill_dir_for(Some(Path::new("/var/lib/rivet/cdc.json")), cfg),
            None,
            "with nothing named, an oversized transaction must still FAIL — a \
             silent spill would remove the OOM guard and say nothing"
        );

        for off in ["0", "false", "FALSE", "off", "no", "  "] {
            guard.set(off);
            assert_eq!(
                spill_dir_for(None, cfg),
                None,
                "`RIVET_CDC_SPILL_DIR={off:?}` must mean OFF, not a directory of \
                 that name"
            );
        }
        for on in ["1", " 1 ", "true", "yes", "ON"] {
            guard.set(on);
            assert_eq!(
                spill_dir_for(Some(Path::new("/var/lib/rivet/cdc.json")), cfg),
                Some(Path::new("/var/lib/rivet/.rivet-spill").to_path_buf()),
                "`{on:?}` is the switch, not a directory name"
            );
        }

        guard.set("1");
        assert_eq!(
            spill_dir_for(None, cfg),
            Some(Path::new("/etc/rivet/.rivet/spill").to_path_buf()),
        );

        guard.set("/mnt/big/spill");
        assert_eq!(
            spill_dir_for(Some(Path::new("/var/lib/rivet/cdc.json")), cfg),
            Some(Path::new("/mnt/big/spill").to_path_buf()),
        );
        guard.set("spill-here");
        assert_eq!(
            spill_dir_for(None, cfg),
            Some(Path::new("/etc/rivet/spill-here").to_path_buf()),
            "a relative explicit path anchors to the CONFIG's directory"
        );
    }

    /// Save/restore one env var around a test that must read it.
    struct EnvGuard {
        key: &'static str,
        prior: Option<String>,
    }

    impl EnvGuard {
        fn unset(key: &'static str) -> Self {
            let prior = std::env::var(key).ok();
            unsafe { std::env::remove_var(key) };
            Self { key, prior }
        }
        fn set(&self, v: &str) {
            unsafe { std::env::set_var(self.key, v) };
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prior {
                Some(v) => unsafe { std::env::set_var(self.key, v) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }
}

#[cfg(test)]
mod error_code_tests {
    use crate::error::assert_coded;

    /// The shared CDC seams: an unknown URL scheme, an unsafe table identifier, an unreadable table (the server's words kept), the undecodable seam, and a deferred cell poison.
    #[test]
    fn the_shared_cdc_refusals_carry_their_codes() {
        let err = super::CdcEngine::from_url("foo://x").unwrap_err();
        assert_coded(&err, "RIVET_CONFIG_SOURCE_URL_INVALID", 1);
        let err = super::validate_table_ident("a;b").unwrap_err();
        assert_coded(&err, "RIVET_CONFIG_EXPORT_SOURCE_INVALID", 1);
        let err = super::table_unreadable("pg", "t", "permission denied for table t");
        assert_coded(&err, "RIVET_SOURCE_QUERY_FAILED", 1);
        assert_eq!(
            err.to_string(),
            "pg cdc: cannot read `t` (permission denied for table t); nothing was read or written."
        );
        let err = super::checkpoint_identity::undecodable("x".into());
        assert_coded(&err, "RIVET_SOURCE_CDC_UNDECODABLE", 5);
        let err = super::raise_deferred(&Some("column(s) [c] hold a value".into())).unwrap_err();
        assert_coded(&err, "RIVET_SOURCE_CDC_CELL_UNSUPPORTED", 5);
        assert!(super::raise_deferred(&None).is_ok());
    }
}

#[cfg(test)]
mod override_table_tests {
    use crate::types::{ColumnOverrides, RivetType, SourceColumn, TypeMapping};

    struct TableSource(Vec<TypeMapping>);

    impl crate::source::Source for TableSource {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> crate::error::Result<()> {
            unreachable!("a plan-time read exports nothing")
        }

        fn query_scalar(&mut self, _sql: &str) -> crate::error::Result<Option<String>> {
            unreachable!("a plan-time read reads no scalar")
        }

        fn type_mappings(
            &mut self,
            _query: &str,
            overrides: &ColumnOverrides,
        ) -> crate::error::Result<Vec<TypeMapping>> {
            if overrides.is_empty() {
                return Ok(self.0.clone());
            }
            crate::rivet_bail!(
                crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
                "the batch table refuses this pair"
            )
        }
    }

    fn numeric() -> TypeMapping {
        TypeMapping::from_source(
            &SourceColumn::simple("v", "numeric(10,2)", true),
            RivetType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
    }

    fn declared(t: RivetType) -> ColumnOverrides {
        ColumnOverrides::from([("v".to_string(), t)])
    }

    /// An integer override over a numeric column goes to the batch table and is refused there; a string override and no override are not its pair.
    #[test]
    fn an_integer_override_over_a_numeric_is_the_batch_tables_pair_and_nothing_else_is() {
        use crate::error::assert_coded;
        let mut src = TableSource(vec![numeric()]);
        let detected = vec![numeric()];
        let err =
            super::refuse_by_override_table(&mut src, "q", &detected, &declared(RivetType::Int64))
                .unwrap_err();
        assert_coded(&err, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
        let err =
            super::stream_override_refusal(&mut src, "q", &declared(RivetType::Int64)).unwrap_err();
        assert_coded(&err, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
        super::refuse_by_override_table(&mut src, "q", &detected, &declared(RivetType::String))
            .expect("a string override is not the table's pair");
        super::refuse_by_override_table(&mut src, "q", &detected, &ColumnOverrides::new())
            .expect("no override is not the table's pair");
    }
}

#[cfg(test)]
mod grants_prerequisite_tests {
    use super::{CdcEngine, DrainMode};

    /// A MongoDB capture with the policy reads its grants before the anchor and refuses at an unreachable server; without it nothing is read.
    #[test]
    fn a_mongo_capture_reads_the_grants_only_with_the_policy() {
        let url = "mongodb://u:p@127.0.0.1:1/app?serverSelectionTimeoutMS=300";
        let tables = vec!["orders".to_string()];
        let said = CdcEngine::Mongo
            .refuse_unmet_prerequisites(url, None, &tables, DrainMode::BoundedAtOpen, true)
            .unwrap_err();
        assert!(
            format!("{said:#}")
                .to_lowercase()
                .contains("server selection"),
            "{said:#}"
        );
        CdcEngine::Mongo
            .refuse_unmet_prerequisites(url, None, &tables, DrainMode::BoundedAtOpen, false)
            .unwrap();
    }
}
