//! CDC export config: the `cdc:` block's types, the baseline pairing, and every rule that validates a `mode.

use super::quantity::Knob;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    Config, DestinationType, ExportConfig, ExportMode, SourceType, overlapping_table_pair,
};

/// Why Oracle refuses `until_current: false` / `--stream`: LogMiner here only drains to the open-time SCN.
pub(crate) const ORACLE_CONTINUOUS_REFUSAL: &str = "Oracle CDC is always a bounded drain to the SCN current at open, so `until_current: false` (`rivet cdc --stream`) would still exit on catch-up — omit it (or set `until_current: true`) and run on a schedule";

/// `until_current` defaults to `true` — the OSS model is the BOUNDED, scheduler- driven drain ("read to the log end and exit").
fn default_true() -> bool {
    true
}

/// Default PostgreSQL logical slot when `cdc.slot` is omitted.
pub const DEFAULT_PG_SLOT: &str = "rivet_slot";
/// Default MySQL replica `server_id` when `cdc.server_id` is omitted (see [`DEFAULT_PG_SLOT`] for why this is a shared const).
pub const DEFAULT_MYSQL_SERVER_ID: u32 = 4271;

/// What the FIRST CDC run does before draining changes.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CdcInitialMode {
    /// Anchor-then-snapshot: create the resume anchor (PostgreSQL slot /
    /// MySQL binlog checkpoint / SQL Server LSN checkpoint) FIRST, then run a
    /// full batch snapshot of each table into `<destination>[/<table>]/snapshot/`,
    /// then drain CDC. Because the anchor predates the snapshot read, anything
    /// changed during the snapshot also appears in the change stream — an
    /// overlap (dedupe by PK + `__op`), never a gap. The safe switch ordering,
    /// enforced by construction instead of operator discipline.
    Snapshot,
}

/// Per-export CDC settings, required when `mode: cdc`. The output `table`,
/// `destination`, and `format` come from the export itself; this carries only the
/// CDC-specific knobs (resume + per-engine stream params).
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct CdcExportConfig {
    /// First-run behaviour: `snapshot` = anchor → full snapshot → drain (see
    /// [`CdcInitialMode`]). Omitted ⇒ capture changes only, with no anchor step
    /// (the default; the operator owns the initial load).
    ///
    /// `snapshot` anchors, and on engines with no server-side anchor (MySQL, SQL
    /// Server) that makes `checkpoint:` mandatory — the checkpoint file IS the
    /// anchor there.
    ///
    /// This doc line is what the generated config reference renders, so every
    /// accepted value must be explained HERE: the reference lists the variants
    /// from the enum but describes only this sentence, so an explanation left on
    /// a variant alone documents a value the reader is told exists and never told
    /// the meaning of.
    #[serde(default)]
    pub initial: Option<CdcInitialMode>,
    /// Persist/resume the source log position to this file. Omit to tail from the
    /// current position without checkpointing.
    pub checkpoint: Option<String>,
    /// Catch up to the source's current end and exit (a bounded run), instead of
    /// streaming indefinitely — ideal for a scheduler. For MySQL this is a
    /// non-blocking binlog dump; PostgreSQL / SQL Server already drain-and-exit.
    /// **Defaults to `true`** (bounded): the OSS model is scheduler-driven, and
    /// omitting this must NOT silently start a never-terminating stream. Setting
    /// `false` opts into the continuous model, which is engine-specific: a true
    /// daemon on MySQL (blocking binlog dump) and MongoDB (the change stream
    /// blocks awaiting events; ends only if the stream is invalidated/closed);
    /// PostgreSQL / SQL Server still exit on catch-up — one unbounded pass, run
    /// it under a supervisor. Oracle refuses `false`: LogMiner is always a
    /// bounded drain to the SCN current at open.
    #[serde(default = "default_true")]
    pub until_current: bool,
    /// Stop at the first COMMIT BOUNDARY once N change events have been
    /// captured (default: until end of stream / interrupted). A soft cap, like
    /// `rollover`: a transaction is never split, so the run may overshoot N by
    /// the remainder of the transaction the cap landed in — a hard per-event
    /// stop cut transactions mid-flight and left the stream unable to advance
    /// past them.
    pub max_events: Option<usize>,
    /// Rows per output part file (default 100000). A part also rolls at a
    /// transaction boundary, so it never splits a transaction. Larger ⇒ fewer,
    /// bigger files but more drain memory — the PostgreSQL peek reads a part's
    /// worth per batch, so drain RSS is O(rollover). Tune per workload: raise it
    /// to cut file count, lower it to cap memory on a small extractor.
    pub rollover: Option<usize>,
    /// Roll a part once its buffered changes reach this many MB, whichever comes
    /// first with `rollover`. Caps the in-memory buffer and the part file size by
    /// bytes instead of a fixed row count — predictable for tables with wide
    /// (large JSON / blob) rows, mirroring the batch path's `batch_size_memory_mb`.
    /// Defaults to 256 (MiB): the row count alone is a budget for one row width,
    /// and absence must not mean "no byte budget". The bytes are the buffered
    /// changes' RESIDENT cost (struct + commit position + values), not the part
    /// file's size on disk. It bounds the buffer, not the process: a roll encodes
    /// up to 16 tables' parts at once, each a columnar copy of its buffered rows,
    /// so peak RSS sits above the budget (measured +31% on a 60-table stream).
    pub rollover_memory_mb: Option<usize>,
    /// MySQL replica server-id for the binlog connection (default 4271; must be
    /// distinct from the source's and any other replica).
    pub server_id: Option<u32>,
    /// PostgreSQL logical replication slot name (default `rivet_slot`).
    pub slot: Option<String>,
    /// PostgreSQL publication for the pgoutput stream (default: the slot name); validated now, read once the stream reads pgoutput.
    pub publication: Option<String>,
    /// SQL Server CDC capture instance, e.g. `dbo_orders` — required for
    /// `sqlserver://` sources.
    pub capture_instance: Option<String>,
    /// Which EXPORTS supply the baseline read (see [`CdcBackfill`]). Absent ⇒ no
    /// baseline: the stream captures changes only, and the operator owns the
    /// initial load.
    #[serde(default, deserialize_with = "backfill_written_means_a_value")]
    pub backfill: Option<CdcBackfill>,
}

/// The exports whose read strategy the baseline legs borrow.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum CdcBackfill {
    /// `auto` — pair each captured table with the export whose `table:` names it.
    Auto(AutoWord),
    /// An explicit list of export names, for a config where the pairing is not one-to-one by name.
    Exports(Vec<String>),
}

/// `backfill:` written with no value (YAML null) is refused, not read as absent.
fn backfill_written_means_a_value<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<CdcBackfill>, D::Error> {
    CdcBackfill::deserialize(d).map(Some).map_err(|_| {
        serde::de::Error::custom(
            "`cdc.backfill` must be `auto` or a list of export names (`[orders, customers]`); \
             leave the key out for a stream with no baseline",
        )
    })
}

/// The literal `auto`, as its own type so the untagged enum cannot swallow a mistyped word.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AutoWord {
    Auto,
}

impl schemars::JsonSchema for CdcBackfill {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CdcBackfill".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "`auto` (pair each captured table with the export that names it), \
                            or a list of export names",
            "anyOf": [
                { "type": "string", "enum": ["auto"] },
                { "type": "array", "items": { "type": "string" } }
            ]
        })
    }
}

/// The tables one `mode: cdc` export captures, in configured order.
pub fn cdc_captured_tables(export: &ExportConfig) -> Vec<String> {
    match (&export.tables, &export.table) {
        (Some(ts), _) => ts.clone(),
        (None, Some(t)) => vec![t.clone()],
        (None, None) => Vec::new(),
    }
}

/// The bare relation name — the last dotted segment.
fn bare(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

/// Whether two configured relation names name ONE table.
fn same_relation(a: &str, b: &str) -> bool {
    match (a.contains('.'), b.contains('.')) {
        (true, true) => a == b,
        (false, false) => a == b,
        _ => {
            let (qualified, plain) = if a.contains('.') { (a, b) } else { (b, a) };
            let (schema, leaf) = qualified.rsplit_once('.').unwrap_or(("", qualified));
            matches!(schema, "public" | "dbo") && leaf == plain
        }
    }
}

impl CdcExportConfig {
    /// Whether the stream has a baseline leg (`initial.
    pub fn has_baseline(&self) -> bool {
        self.initial == Some(CdcInitialMode::Snapshot) || self.backfill.is_some()
    }

    /// `until_current: false` — a continuous stream the operator stops, never a bounded drain.
    pub fn runs_until_stopped(&self) -> bool {
        !self.until_current
    }

    /// Neither `initial: snapshot` nor `backfill:` — no anchor step, no baseline legs.
    pub fn captures_changes_only(&self) -> bool {
        !self.has_baseline()
    }
}

/// Why a baseline on this engine needs `cdc.checkpoint:`, or `None` when it does not.
pub fn baseline_checkpoint_refusal(
    export: &ExportConfig,
    source_type: crate::config::SourceType,
) -> Option<String> {
    let cdc = export.cdc.as_ref()?;
    let declared = if cdc.backfill.is_some() {
        "`cdc.backfill:`"
    } else if cdc.initial.is_some() {
        "`cdc.initial:`"
    } else {
        return None;
    };
    if source_type == crate::config::SourceType::Postgres || cdc.checkpoint.is_some() {
        return None;
    }
    Some(format!(
        "export '{}': {declared} on {source_type:?} requires `cdc.checkpoint:` — this engine has \
         no server-side anchor, so the checkpoint file is the anchor: the baseline is only safe \
         because the anchor precedes it, and without it each run re-anchors at the current log \
         position and silently skips every change since the last one",
        export.name
    ))
}

/// The `columns:` a `mode: cdc` export resolves types with.
pub fn effective_columns(
    export: &ExportConfig,
    all: &[ExportConfig],
) -> std::collections::HashMap<String, String> {
    let mut merged = std::collections::HashMap::new();
    for (table, recipe) in resolve_backfill(export, all).unwrap_or_default() {
        for (k, v) in recipe.columns.iter().filter(|(k, _)| !k.contains('.')) {
            merged.insert(format!("{}.{k}", bare(&table)), v.clone());
        }
        for (k, v) in recipe.columns.iter().filter(|(k, _)| k.contains('.')) {
            merged.insert(k.clone(), v.clone());
        }
    }
    merged.extend(export.columns.clone());
    merged
}

/// Every export some `mode: cdc` export claims as a baseline recipe.
pub fn backfill_recipe_names(exports: &[ExportConfig]) -> std::collections::HashSet<String> {
    exports
        .iter()
        .filter(|e| e.mode == ExportMode::Cdc)
        .flat_map(|cdc| resolve_backfill(cdc, exports).unwrap_or_default())
        .map(|(_, recipe)| recipe.name.clone())
        .collect()
}

/// Why a whole-config `rivet plan` skips `export`, or `None` when it plans it.
pub fn batch_plan_skip_reason(
    export: &ExportConfig,
    recipes: &std::collections::HashSet<String>,
) -> Option<&'static str> {
    if export.mode == ExportMode::Cdc {
        Some("a CDC export has no batch plan; it runs with `rivet run`")
    } else if recipes.contains(&export.name) {
        Some("it is the backfill recipe of a `mode: cdc` export, which runs it after the anchor")
    } else {
        None
    }
}

/// `exports` minus the ones named in `recipes` — the whole-config run loop's view.
pub fn without_backfill_recipes<'a>(
    exports: impl IntoIterator<Item = &'a ExportConfig>,
    recipes: &std::collections::HashSet<String>,
) -> Vec<&'a ExportConfig> {
    exports
        .into_iter()
        .filter(|e| !recipes.contains(&e.name))
        .collect()
}

/// The names a whole-config invocation must not run itself: each is a `mode: cdc` export's `backfill:` recipe.
fn backfill_recipes_to_skip(exports: &[ExportConfig]) -> std::collections::HashSet<String> {
    let recipes = backfill_recipe_names(exports);
    for e in exports.iter().filter(|e| recipes.contains(&e.name)) {
        log::info!(
            "export '{}': skipped — it is the backfill recipe of a `mode: cdc` export, \
             which runs it after the anchor (run it alone with `-e {}` to export it on \
             its own)",
            e.name,
            e.name
        );
    }
    recipes
}

/// Whether any of `exports` declares a `columns:` override map.
pub fn any_declares_columns(exports: &[ExportConfig]) -> bool {
    exports.iter().any(|e| !e.columns.is_empty())
}

/// The exports an invocation was asked for: one named export, or the whole config minus the CDC backfill recipes.
pub fn declared_exports(
    config: &super::Config,
    export_name: Option<&str>,
) -> crate::error::Result<Vec<ExportConfig>> {
    match export_name {
        Some(name) => Ok(vec![
            config
                .exports
                .iter()
                .find(|e| e.name == name)
                .ok_or_else(|| {
                    crate::error::coded(
                        crate::error::codes::CLI_EXPORT_UNKNOWN,
                        format!("export '{}' not found in config", name),
                    )
                })?
                .clone(),
        ]),
        None => {
            let recipes = backfill_recipes_to_skip(&config.exports);
            Ok(without_backfill_recipes(&config.exports, &recipes)
                .into_iter()
                .cloned()
                .collect())
        }
    }
}

/// Two captured tables that share a LEAF name (`sales.orders`, `archive.orders`) while a recipe of either declares `columns:`.
pub fn same_leaf_typed_pair(pairs: &[(String, &ExportConfig)]) -> Option<(String, String)> {
    for (i, (a, ra)) in pairs.iter().enumerate() {
        for (b, rb) in pairs.iter().skip(i + 1) {
            if a != b && bare(a) == bare(b) && !(ra.columns.is_empty() && rb.columns.is_empty()) {
                return Some((a.clone(), b.clone()));
            }
        }
    }
    None
}

/// Refuse a column `table`'s recipe and the CDC export type DIFFERENTLY.
pub fn refuse_backfill_type_conflict(
    cdc_export: &ExportConfig,
    table: &str,
    recipe: &ExportConfig,
) -> anyhow::Result<()> {
    let parsed = |e: &ExportConfig| crate::types::parse_column_overrides(&e.columns, &e.name);
    let (recipe_types, cdc_types) = (parsed(recipe)?, parsed(cdc_export)?);
    let cdc_for_table = crate::types::overrides_for_unit(&cdc_types, Some(table));
    let recipe_for_table = crate::types::overrides_for_unit(&recipe_types, Some(table));
    for (col, mine) in &recipe_for_table {
        if let Some(theirs) = cdc_for_table.get(col)
            && theirs != mine
        {
            crate::config_bail!(
                crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                "export '{}': column '{col}' of table '{table}' is declared `{mine:?}` by its \
                 backfill export '{}' and `{theirs:?}` by the CDC export — the baseline and the \
                 stream write into one `<table>__changes`, so one column cannot have two types. \
                 Keep the declaration on the batch export and drop the other.",
                cdc_export.name,
                recipe.name
            );
        }
    }
    Ok(())
}

/// The recipe [`resolve_backfill`] paired with `table`, if any.
pub fn backfill_recipe_for<'a>(
    pairs: &[(String, &'a ExportConfig)],
    table: &str,
) -> Option<&'a ExportConfig> {
    pairs.iter().find(|(t, _)| t == table).map(|(_, r)| *r)
}

/// Pair every captured table with the export that supplies its baseline read.
pub fn resolve_backfill<'a>(
    cdc: &ExportConfig,
    all: &'a [ExportConfig],
) -> std::result::Result<Vec<(String, &'a ExportConfig)>, String> {
    let Some(spec) = cdc.cdc.as_ref().and_then(|c| c.backfill.as_ref()) else {
        return Ok(Vec::new());
    };
    let tables = cdc_captured_tables(cdc);
    if tables.is_empty() {
        return Err(format!(
            "export '{}': `cdc.backfill` needs `table:` or `tables:` — there is nothing to \
             pair a baseline export with",
            cdc.name
        ));
    }

    let candidate = |e: &ExportConfig| -> bool {
        e.name != cdc.name && e.mode != ExportMode::Cdc && e.table.is_some()
    };

    let mut pairs: Vec<(String, &ExportConfig)> = Vec::with_capacity(tables.len());
    match spec {
        CdcBackfill::Auto(_) => {
            for t in &tables {
                let matches: Vec<&ExportConfig> = all
                    .iter()
                    .filter(|e| {
                        candidate(e) && e.table.as_deref().is_some_and(|et| same_relation(et, t))
                    })
                    .collect();
                match matches.as_slice() {
                    [one] => pairs.push((t.clone(), *one)),
                    [] => {
                        return Err(format!(
                            "export '{}': `cdc.backfill: auto` found no export reading table \
                             '{t}' — add one (the batch export that already describes how to \
                             read it), or name the exports explicitly with \
                             `backfill: [<name>, …]`.\n  Without a baseline this table would \
                             capture changes over history nobody loaded, and the run would \
                             still report success.",
                            cdc.name
                        ));
                    }
                    many => {
                        let names: Vec<&str> = many.iter().map(|e| e.name.as_str()).collect();
                        return Err(format!(
                            "export '{}': `cdc.backfill: auto` found {} exports reading table \
                             '{t}' ({}) — the pairing is ambiguous. Name the one you mean with \
                             `backfill: [<name>, …]`.",
                            cdc.name,
                            many.len(),
                            names.join(", ")
                        ));
                    }
                }
            }
        }
        CdcBackfill::Exports(names) => {
            for n in names {
                let Some(e) = all.iter().find(|e| e.name == *n) else {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names export '{n}', which this config does \
                         not define",
                        cdc.name
                    ));
                };
                if e.name == cdc.name {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names the CDC export itself — the baseline \
                         is read by a BATCH export, not by the stream",
                        cdc.name
                    ));
                }
                if e.mode == ExportMode::Cdc {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names export '{n}', which is `mode: cdc` — \
                         a baseline reads the TABLE, not another change stream",
                        cdc.name
                    ));
                }
                let Some(t) = e.table.as_deref() else {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names export '{n}', which has no `table:` \
                         (a `query:` export describes rows, not a relation, so it cannot be a \
                         captured table's baseline)",
                        cdc.name
                    ));
                };
                let Some(captured) = tables.iter().find(|c| same_relation(c, t)) else {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names export '{n}', which reads '{t}' — a \
                         table this stream does not capture. Capture it, or drop the reference.",
                        cdc.name
                    ));
                };
                if pairs.iter().any(|(c, _)| c == captured) {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names two exports for table '{captured}' — \
                         one baseline per table",
                        cdc.name
                    ));
                }
                pairs.push((captured.clone(), e));
            }
            let missing: Vec<&str> = tables
                .iter()
                .filter(|t| !pairs.iter().any(|(c, _)| c == *t))
                .map(String::as_str)
                .collect();
            if !missing.is_empty() {
                return Err(format!(
                    "export '{}': `cdc.backfill` covers {} of {} captured tables — no baseline \
                     for {}. List every table's export, or remove the ones you do not want \
                     captured from `tables:`.",
                    cdc.name,
                    pairs.len(),
                    tables.len(),
                    missing.join(", ")
                ));
            }
        }
    }
    for (t, e) in &pairs {
        if !matches!(e.mode, ExportMode::Full | ExportMode::Chunked) {
            return Err(format!(
                "export '{}': backfill export '{}' for table '{t}' is `mode: {:?}` — a baseline \
                 must read the whole table, so only a `full` or `chunked` export can be one; an \
                 incremental read would leave every row outside its window with no baseline.",
                cdc.name, e.name, e.mode
            ));
        }
    }
    Ok(pairs)
}

impl Default for CdcExportConfig {
    fn default() -> Self {
        Self {
            initial: None,
            checkpoint: None,
            until_current: true,
            max_events: None,
            rollover: None,
            rollover_memory_mb: None,
            server_id: None,
            slot: None,
            publication: None,
            capture_instance: None,
            backfill: None,
        }
    }
}

impl Config {
    /// CDC stream resources are per-export and their **defaults collide**.
    pub(super) fn validate_cdc_resource_conflicts(&self) -> crate::error::Result<()> {
        use std::collections::HashMap;

        let mut slots: HashMap<String, &str> = HashMap::new();
        let mut publications: HashMap<String, &str> = HashMap::new();
        let mut server_ids: HashMap<u32, &str> = HashMap::new();
        let mut checkpoints: HashMap<String, &str> = HashMap::new();

        for e in self.exports.iter().filter(|e| e.mode == ExportMode::Cdc) {
            let cdc = e.cdc.as_ref();
            if let Some(c) = cdc {
                if let Some(rows) = c.rollover {
                    Knob::ROLLOVER.validate_export(&e.name, rows)?;
                }
                if c.rollover_memory_mb == Some(0) {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_CDC_ROLLOVER_INVALID,
                        "export '{}': cdc.rollover_memory_mb must be >= 1 (got 0). \
                         A zero byte budget rolls a part on every committed \
                         transaction; omit the field for the default (256 MiB).",
                        e.name
                    );
                }
                if let Some(mb) = c.rollover_memory_mb
                    && mb.checked_mul(1024 * 1024).is_none()
                {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_CDC_ROLLOVER_INVALID,
                        "export '{}': cdc.rollover_memory_mb {} overflows a byte \
                         count on this platform.",
                        e.name,
                        mb
                    );
                }
            }
            match self.source.source_type {
                SourceType::Postgres => {
                    let slot = cdc
                        .and_then(|c| c.slot.clone())
                        .unwrap_or_else(|| DEFAULT_PG_SLOT.to_string());
                    if let Some(prev) = slots.insert(slot.clone(), &e.name) {
                        crate::config_bail!(
                            crate::error::codes::CONFIG_CDC_RESOURCE_CONFLICT,
                            "exports '{prev}' and '{}': same PostgreSQL slot '{slot}' — a slot \
                             has ONE consumer; each export's ack would advance it past changes \
                             the other never read (silent data loss). Set a distinct `cdc.slot:` \
                             per export (the default is '{DEFAULT_PG_SLOT}').",
                            e.name
                        );
                    }
                    let publication = cdc
                        .and_then(|c| c.publication.clone())
                        .unwrap_or_else(|| slot.clone());
                    if let Some(prev) = publications.insert(publication.clone(), &e.name) {
                        crate::config_bail!(
                            crate::error::codes::CONFIG_CDC_RESOURCE_CONFLICT,
                            "exports '{prev}' and '{}': same PostgreSQL publication \
                             '{publication}' — a publication carries ONE export's tables, so \
                             sharing it makes the second export's check see the first's tables as \
                             drift. Set a distinct `cdc.publication:` per export (the default is \
                             the export's slot name).",
                            e.name
                        );
                    }
                }
                SourceType::Mysql => {
                    let sid = cdc
                        .and_then(|c| c.server_id)
                        .unwrap_or(DEFAULT_MYSQL_SERVER_ID);
                    if let Some(prev) = server_ids.insert(sid, &e.name) {
                        crate::config_bail!(
                            crate::error::codes::CONFIG_CDC_RESOURCE_CONFLICT,
                            "exports '{prev}' and '{}': same MySQL server_id {sid} — the server \
                             kills the older replica connection when a new one registers with \
                             the same id. Set a distinct `cdc.server_id:` per export (the \
                             default is {DEFAULT_MYSQL_SERVER_ID}).",
                            e.name
                        );
                    }
                }
                SourceType::Mssql => {}
                SourceType::Mongo | SourceType::Oracle => {}
            }
            if let Some(ckpt) = cdc.and_then(|c| c.checkpoint.as_deref())
                && let Some(prev) = checkpoints.insert(
                    std::path::Path::new(ckpt)
                        .components()
                        .filter(|c| !matches!(c, std::path::Component::CurDir))
                        .collect::<std::path::PathBuf>()
                        .to_string_lossy()
                        .to_lowercase(),
                    &e.name,
                )
            {
                crate::config_bail!(
                    crate::error::codes::CONFIG_CDC_RESOURCE_CONFLICT,
                    "exports '{prev}' and '{}': same checkpoint path '{ckpt}' (compared \
                     case-insensitively: on macOS and Windows two spellings are one file) — \
                     each export must own its resume position or they overwrite each \
                     other's. Set a distinct `cdc.checkpoint:` per export.",
                    e.name
                );
            }
        }
        Ok(())
    }

    /// A `mode: cdc` export captures `table:` or a well-formed `tables:`, never a query.
    pub(super) fn validate_export_cdc_mode(
        &self,
        export: &ExportConfig,
    ) -> crate::error::Result<()> {
        match (&export.table, &export.tables) {
            (None, None) => crate::config_bail!(
                crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                "export '{}': cdc mode requires `table:` (or `tables:` for a \
                 multi-table stream)",
                export.name
            ),
            (None, Some(ts)) => {
                if ts.is_empty() {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                        "export '{}': `tables:` must list at least one table",
                        export.name
                    );
                }
                let mut seen = std::collections::HashSet::new();
                for t in ts {
                    if !seen.insert(t.as_str()) {
                        crate::config_bail!(
                            crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                            "export '{}': duplicate table '{}' in `tables:`",
                            export.name,
                            t
                        );
                    }
                }
                if self.source.source_type != SourceType::Mongo
                    && let Some((a, b)) = overlapping_table_pair(ts)
                {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                        "export '{}': `tables:` lists both '{}' and '{}', which can name \
                         the SAME relation — a bare name matches any schema, so every \
                         event routes to whichever is listed first and the other \
                         publishes an empty, successful-looking export forever. Keep one \
                         spelling.",
                        export.name,
                        a,
                        b
                    );
                }
                if export.destination.destination_type == DestinationType::Local
                    && let Some((a, b)) = super::case_colliding_table_pair(ts)
                {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                        "export '{}': `tables:` lists '{}' and '{}', which differ only by \
                         letter case — on a local destination their directories are one \
                         directory on a case-insensitive filesystem (macOS, Windows), so \
                         their parts overwrite each other. Capture one of them in its own \
                         export with its own `destination.path`, or write to a cloud \
                         destination, whose object keys are case-sensitive.",
                        export.name,
                        a,
                        b
                    );
                }
                if export.destination.destination_type == DestinationType::Stdout {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                        "export '{}': `tables:` cannot write to `destination: \
                         {{ type: stdout }}` — a stream has no per-table \
                         sub-prefix, so all {} tables would interleave into one \
                         output with their headers and column sets mixed. Use a \
                         local or cloud destination, or capture one table per \
                         export.",
                        export.name,
                        ts.len()
                    );
                }
                if self.source.source_type == SourceType::Mssql {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                        "export '{}': `tables:` is not yet supported for SQL Server — \
                         its capture instances are per-table; use one cdc export per \
                         table (capture_instance each)",
                        export.name
                    );
                }
            }
            (Some(_), _) => {}
        }
        Ok(())
    }

    /// A `cdc:` block is refused outside `mode: cdc`.
    pub(super) fn validate_cdc_block_needs_cdc_mode(
        &self,
        export: &ExportConfig,
    ) -> crate::error::Result<()> {
        if export.cdc.is_some() && export.mode != ExportMode::Cdc {
            crate::config_bail!(
                crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                "export '{}': a `cdc:` block is only valid with `mode: cdc`",
                export.name
            );
        }
        Ok(())
    }

    /// `cdc.publication` is PostgreSQL's, one to 63 bytes with no NUL.
    pub(super) fn validate_cdc_publication(
        &self,
        export: &ExportConfig,
    ) -> crate::error::Result<()> {
        if let Some(name) = export.cdc.as_ref().and_then(|c| c.publication.as_deref()) {
            if self.source.source_type != SourceType::Postgres {
                crate::config_bail!(
                    crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                    "export '{}': `cdc.publication` names a PostgreSQL publication, and this \
                     source is not PostgreSQL — remove the key",
                    export.name
                );
            }
            if name.is_empty() || name.len() > 63 || name.contains('\0') {
                crate::config_bail!(
                    crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                    "export '{}': `cdc.publication` must be 1 to 63 bytes with no NUL, as a \
                     PostgreSQL identifier is (got {} bytes) — set a name of that length",
                    export.name,
                    name.len()
                );
            }
        }
        Ok(())
    }

    /// CDC baselines, checkpoints and resume anchors.
    pub(super) fn validate_export_cdc(&self, export: &ExportConfig) -> crate::error::Result<()> {
        if let Some(cdc) = &export.cdc
            && cdc.has_baseline()
        {
            let clashes = |t: &str| t.rsplit('.').next().unwrap_or(t) == "snapshot";
            if export.table.as_deref().is_some_and(clashes)
                || export.tables.iter().flatten().any(|t| clashes(t))
            {
                crate::config_bail!(
                    crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                    "export '{}': a table named 'snapshot' collides with the reserved \
                     `snapshot/` sub-prefix that a baseline (`cdc.initial: snapshot` or \
                     `cdc.backfill`) writes — rename the table or use a separate export \
                     without a baseline",
                    export.name
                );
            }
        }

        if let Some(why) = baseline_checkpoint_refusal(export, self.source.source_type) {
            crate::config_bail!(crate::error::codes::CONFIG_MODE_SETTING_INVALID, "{why}");
        }

        if self.source.source_type == SourceType::Oracle
            && export.cdc.as_ref().is_some_and(|c| c.runs_until_stopped())
        {
            crate::config_bail!(
                crate::error::codes::CONFIG_CDC_CONTINUOUS_UNSUPPORTED,
                "export '{}': {}",
                export.name,
                ORACLE_CONTINUOUS_REFUSAL
            );
        }

        if let Some(cdc) = &export.cdc
            && cdc.backfill.is_some()
            && cdc.initial.is_some()
        {
            crate::config_bail!(
                crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                "export '{}': `cdc.initial:` and `cdc.backfill:` both describe the FIRST run's \
                 baseline — keep one. `initial: snapshot` synthesizes a single-stream full scan; \
                 `backfill:` borrows the read strategy of the batch export that already describes \
                 the table (its key, workers, page size and resume).",
                export.name
            );
        }

        if let Some(cdc) = &export.cdc
            && cdc.backfill.is_some()
        {
            let pairs = resolve_backfill(export, &self.exports).map_err(|why| {
                crate::error::coded(crate::error::codes::CONFIG_CDC_SHAPE_INVALID, why)
            })?;
            if !self.source.source_type.is_sql() {
                for (captured, recipe) in &pairs {
                    if recipe.table.as_deref() != Some(captured.as_str()) {
                        crate::config_bail!(
                            crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                            "export '{}': `cdc.backfill` paired collection '{captured}' with export \
                             '{}', which reads '{}' — a MongoDB collection name is literal (a dot \
                             is part of the name, not a schema), so the recipe must read exactly \
                             `table: {captured}`",
                            export.name,
                            recipe.name,
                            recipe.table.as_deref().unwrap_or("")
                        );
                    }
                }
            }
            for (table, recipe) in &pairs {
                if self.source.source_type.is_sql()
                    && let Some(t) = recipe.table.as_deref()
                {
                    super::export::validate_table_shortcut_ident(&recipe.name, t)?;
                }
                crate::types::parse_column_overrides(&recipe.columns, &recipe.name)?;
                refuse_backfill_type_conflict(export, table, recipe)?;
            }
            if let Some((a, b)) = same_leaf_typed_pair(&pairs) {
                crate::config_bail!(
                    crate::error::codes::CONFIG_CDC_SHAPE_INVALID,
                    "export '{}': captured tables '{a}' and '{b}' share the leaf name and a \
                     recipe declares `columns:` — per-table column types are keyed \
                     `table.column` by the bare name, so one declaration would type both. \
                     Capture them in two `mode: cdc` exports, or drop the recipe's `columns:`.",
                    export.name
                );
            }
        }

        if export.mode == ExportMode::Cdc
            && matches!(
                self.source.source_type,
                SourceType::Mongo | SourceType::Mysql | SourceType::Oracle
            )
            && export
                .cdc
                .as_ref()
                .and_then(|c| c.checkpoint.as_ref())
                .is_none()
        {
            crate::config_bail!(
                crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                "export '{}': {:?} `mode: cdc` requires `cdc.checkpoint:` — this engine has \
                 no server-side resume anchor, so without the checkpoint file each run \
                 re-anchors at the current position and silently loses every change \
                 between runs.",
                export.name,
                self.source.source_type
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::export::sample_export;

    /// A one-export `mode: cdc` config on `source_type` with the given `until_current`.
    fn cdc_yaml(source: &str, url: &str, until_current: bool) -> String {
        format!(
            "source: {{ type: {source}, url: \"{url}\" }}\nexports:\n  - name: t\n    table: T\n    \
             mode: cdc\n    format: parquet\n    \
             cdc: {{ until_current: {until_current}, checkpoint: \"./t.ckpt\" }}\n    \
             destination: {{ type: local, path: /tmp/x }}\n"
        )
    }

    #[test]
    fn only_oracle_refuses_the_continuous_model_at_load() {
        let ora = "oracle://u:p@localhost:1521/FREEPDB1";
        let err = crate::config::Config::from_yaml(&cdc_yaml("oracle", ora, false))
            .expect_err("Oracle `until_current: false` must refuse");
        assert!(
            format!("{err:#}").contains("Oracle CDC is always a bounded drain"),
            "{err:#}"
        );
        crate::config::Config::from_yaml(&cdc_yaml("oracle", ora, true))
            .expect("the bounded default loads");
        crate::config::Config::from_yaml(&cdc_yaml(
            "postgres",
            "postgresql://u:p@localhost/db",
            false,
        ))
        .expect("PostgreSQL may opt into the continuous model");
    }

    /// A two-export PostgreSQL CDC config with the given `cdc:` flow maps.
    fn two_pg_exports(first: &str, second: &str) -> String {
        format!(
            "source: {{ type: postgres, url: \"postgresql://u:p@localhost/db\" }}\nexports:\n  \
             - name: a\n    table: a\n    mode: cdc\n    format: parquet\n    cdc: {{ {first} }}\n    \
             destination: {{ type: local, path: /tmp/x }}\n  \
             - name: b\n    table: b\n    mode: cdc\n    format: parquet\n    cdc: {{ {second} }}\n    \
             destination: {{ type: local, path: /tmp/y }}\n"
        )
    }

    /// `cdc.publication` loads on PostgreSQL, and a name two exports resolve to is refused naming both.
    #[test]
    fn a_publication_two_exports_share_is_refused_and_distinct_ones_load() {
        let ok = crate::config::Config::from_yaml(&two_pg_exports(
            "slot: sa, publication: pa, checkpoint: ./a",
            "slot: sb, checkpoint: ./b",
        ))
        .expect("distinct publications load");
        assert_eq!(
            ok.exports[0]
                .cdc
                .as_ref()
                .and_then(|c| c.publication.as_deref()),
            Some("pa")
        );
        let same = crate::config::Config::from_yaml(&two_pg_exports(
            "slot: sa, publication: shared, checkpoint: ./a",
            "slot: sb, publication: shared, checkpoint: ./b",
        ))
        .expect_err("a shared publication");
        let text = format!("{same:#}");
        assert!(
            text.contains("same PostgreSQL publication 'shared'"),
            "{text}"
        );
        assert!(text.contains("exports 'a' and 'b'"), "{text}");
        let against_default = crate::config::Config::from_yaml(&two_pg_exports(
            "slot: sa, checkpoint: ./a",
            "slot: sb, publication: sa, checkpoint: ./b",
        ))
        .expect_err("an explicit name equal to another export's default");
        assert!(format!("{against_default:#}").contains("same PostgreSQL publication 'sa'"));
    }

    /// The key is refused on a source that has no publications, and an empty or over-long name is refused.
    #[test]
    fn a_publication_key_is_refused_off_postgres_and_when_it_is_not_an_identifier() {
        let mysql = cdc_yaml("mysql", "mysql://u:p@localhost/db", true).replace(
            "until_current: true,",
            "until_current: true, publication: p,",
        );
        let err = crate::config::Config::from_yaml(&mysql).expect_err("MySQL has no publication");
        assert!(
            format!("{err:#}").contains("`cdc.publication` names a PostgreSQL publication"),
            "{err:#}"
        );
        for bad in ["\"\"", &format!("\"{}\"", "x".repeat(64))] {
            let pg = cdc_yaml("postgres", "postgresql://u:p@localhost/db", true).replace(
                "until_current: true,",
                &format!("until_current: true, publication: {bad},"),
            );
            let err = crate::config::Config::from_yaml(&pg).expect_err("a bad name");
            assert!(
                format!("{err:#}").contains("1 to 63 bytes"),
                "{bad}: {err:#}"
            );
        }
        let ok = cdc_yaml("postgres", "postgresql://u:p@localhost/db", true).replace(
            "until_current: true,",
            &format!("until_current: true, publication: {},", "x".repeat(63)),
        );
        crate::config::Config::from_yaml(&ok).expect("63 bytes is the identifier limit");
    }

    /// Only an export with a non-empty `columns:` map makes the set declare columns.
    #[test]
    fn a_set_declares_columns_only_when_one_export_has_a_columns_map() {
        let plain = sample_export("plain");
        let mut typed = sample_export("typed");
        typed.columns.insert("v".to_string(), "int64".to_string());
        assert!(!any_declares_columns(&[]));
        assert!(!any_declares_columns(std::slice::from_ref(&plain)));
        assert!(any_declares_columns(&[plain, typed]));
    }

    /// A batch export reading one table, named as a recipe candidate.
    fn recipe(name: &str, table: &str) -> ExportConfig {
        let mut e = sample_export(name);
        e.mode = ExportMode::Chunked;
        e.table = Some(table.to_string());
        e.tables = None;
        e.cdc = None;
        e
    }

    /// The stream: N tables, one binlog, a `backfill:` spec.
    fn stream(tables: &[&str], spec: CdcBackfill) -> ExportConfig {
        let mut e = sample_export("stand_cdc");
        e.mode = ExportMode::Cdc;
        e.table = None;
        e.tables = Some(tables.iter().map(|t| t.to_string()).collect());
        e.cdc = Some(CdcExportConfig {
            backfill: Some(spec),
            ..Default::default()
        });
        e
    }

    /// A candidate reads ONE table by `table:` and is not itself a stream.
    #[test]
    fn resolve_backfill_never_pairs_a_table_with_another_stream() {
        let orders = recipe("orders", "orders");
        let mut other_stream = recipe("orders_cdc", "orders");
        other_stream.mode = ExportMode::Cdc;
        let auto = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![orders, other_stream, auto.clone()];
        let pairs = resolve_backfill(&auto, &all).expect("the other stream is not a candidate");
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1.name, "orders");
    }

    /// The happy path both spellings must agree on.
    #[test]
    fn resolve_backfill_pairs_tables_with_their_recipe_exports() {
        let orders = recipe("orders", "orders");
        let mut history = recipe("ext_ref_id_history", "ext_ref_id_history");
        history.chunk_by_key = None;
        history.chunk_column = Some("ref_id".into());

        let auto = stream(
            &["orders", "ext_ref_id_history"],
            CdcBackfill::Auto(AutoWord::Auto),
        );
        let all = vec![orders.clone(), history.clone(), auto.clone()];
        let pairs = resolve_backfill(&auto, &all).expect("every table has exactly one export");
        let named: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(t, e)| (t.as_str(), e.name.as_str()))
            .collect();
        assert_eq!(
            named,
            vec![
                ("orders", "orders"),
                ("ext_ref_id_history", "ext_ref_id_history")
            ]
        );

        let listed = stream(
            &["orders", "ext_ref_id_history"],
            CdcBackfill::Exports(vec!["orders".into(), "ext_ref_id_history".into()]),
        );
        let all = vec![orders.clone(), history.clone(), listed.clone()];
        let pairs = resolve_backfill(&listed, &all).expect("both named");
        assert_eq!(pairs.len(), 2);

        let qualified = stream(&["public.orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![orders.clone(), qualified.clone()];
        assert_eq!(
            resolve_backfill(&qualified, &all).expect("bare and qualified are one table")[0]
                .1
                .name,
            "orders"
        );

        let mut plain = auto.clone();
        plain.cdc = Some(CdcExportConfig::default());
        assert!(resolve_backfill(&plain, &all).unwrap().is_empty());
    }

    /// Every refusal here is a shape that would otherwise present as SUCCESS.
    #[test]
    fn resolve_backfill_refuses_every_pairing_that_would_look_like_success() {
        let orders = recipe("orders", "orders");
        let err = |cdc: &ExportConfig, all: &[ExportConfig]| {
            resolve_backfill(cdc, all).expect_err("must refuse")
        };

        let auto = stream(&["orders", "payments"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![orders.clone(), auto.clone()];
        let e = err(&auto, &all);
        assert!(e.contains("payments"), "{e}");

        let twin = recipe("orders_copy", "orders");
        let auto1 = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![orders.clone(), twin.clone(), auto1.clone()];
        let e = err(&auto1, &all);
        assert!(e.contains("ambiguous") && e.contains("orders_copy"), "{e}");

        let listed_twice = stream(
            &["orders"],
            CdcBackfill::Exports(vec!["orders".into(), "orders_copy".into()]),
        );
        let all = vec![orders.clone(), twin.clone(), listed_twice.clone()];
        let e = err(&listed_twice, &all);
        assert!(e.contains("two exports for table 'orders'"), "{e}");

        let mut q = recipe("q", "orders");
        q.table = None;
        q.query = Some("SELECT 1".into());
        let listed = stream(&["orders"], CdcBackfill::Exports(vec!["q".into()]));
        let all = vec![q.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(e.contains("no `table:`"), "{e}");

        let other_cdc = {
            let mut c = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
            c.name = "other_cdc".into();
            c
        };
        let listed = stream(&["orders"], CdcBackfill::Exports(vec!["other_cdc".into()]));
        let all = vec![other_cdc.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(e.contains("mode: cdc"), "{e}");

        let elsewhere = recipe("elsewhere", "payments");
        let listed = stream(&["orders"], CdcBackfill::Exports(vec!["elsewhere".into()]));
        let all = vec![orders.clone(), elsewhere.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(e.contains("does not capture"), "{e}");

        let history = recipe("hist", "ext_ref_id_history");
        let listed = stream(
            &["orders", "ext_ref_id_history"],
            CdcBackfill::Exports(vec!["orders".into()]),
        );
        let all = vec![orders.clone(), history.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(
            e.contains("ext_ref_id_history") && e.contains("1 of 2"),
            "{e}"
        );

        let mut tableless = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
        tableless.tables = None;
        tableless.table = None;
        let all = vec![orders.clone(), tableless.clone()];
        let e = err(&tableless, &all);
        assert!(e.contains("nothing to pair"), "{e}");

        let mut inc = recipe("orders_inc", "orders");
        inc.mode = ExportMode::Incremental;
        let listed = stream(&["orders"], CdcBackfill::Exports(vec!["orders_inc".into()]));
        let all = vec![inc.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(
            e.contains("whole table") && e.contains("Incremental"),
            "{e}"
        );
        let mut win = recipe("orders_win", "orders");
        win.mode = ExportMode::TimeWindow;
        let listed = stream(&["orders"], CdcBackfill::Exports(vec!["orders_win".into()]));
        let all = vec![win.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(e.contains("whole table") && e.contains("TimeWindow"), "{e}");

        let foreign = recipe("sales_orders", "sales.orders");
        let auto = stream(&["public.orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![foreign.clone(), auto.clone()];
        let e = err(&auto, &all);
        assert!(
            e.contains("public.orders") && e.contains("no export"),
            "{e}"
        );

        let bare_recipe = recipe("orders", "orders");
        let all = vec![bare_recipe.clone(), auto.clone()];
        assert_eq!(
            resolve_backfill(&auto, &all)
                .expect("a bare name pairs with its qualified spelling")
                .len(),
            1
        );
        let sales = stream(&["sales.orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![bare_recipe.clone(), sales.clone()];
        let e = resolve_backfill(&sales, &all).expect_err("bare does not fold onto sales");
        assert!(e.contains("sales.orders") && e.contains("no export"), "{e}");
    }

    /// A schema-qualified capture (`dbo.orders`.
    #[test]
    fn effective_columns_reach_the_stream_for_a_schema_qualified_capture() {
        use std::collections::HashMap;
        let mut orders = recipe("orders", "dbo.orders");
        orders.columns = HashMap::from([("price".to_string(), "decimal(12,4)".to_string())]);
        let auto = stream(&["dbo.orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![orders, auto.clone()];

        let merged = effective_columns(&auto, &all);
        let parsed = crate::types::parse_column_overrides(&merged, &auto.name).unwrap();
        let for_stream = crate::types::overrides_for_unit(&parsed, Some("dbo.orders"));
        assert!(
            for_stream.contains_key("price"),
            "the recipe's type must reach the stream's unit `dbo.orders`; merged keys: {:?}",
            merged.keys().collect::<Vec<_>>()
        );
    }

    /// One map for the leg, the stream and the spec.
    #[test]
    fn effective_columns_qualifies_each_recipe_to_its_table_and_lets_the_stream_win() {
        use std::collections::HashMap;
        let mut orders = recipe("orders", "orders");
        orders.columns = HashMap::from([
            ("price".to_string(), "decimal(12,4)".to_string()),
            ("orders.note".to_string(), "string".to_string()),
        ]);
        let mut history = recipe("hist", "ext_ref_id_history");
        history.columns = HashMap::from([("price".to_string(), "decimal(8,2)".to_string())]);
        let mut auto = stream(
            &["orders", "ext_ref_id_history"],
            CdcBackfill::Auto(AutoWord::Auto),
        );
        auto.columns = HashMap::from([("flag".to_string(), "bool".to_string())]);
        let all = vec![orders.clone(), history.clone(), auto.clone()];

        let m = effective_columns(&auto, &all);
        assert_eq!(
            m.get("orders.price").map(String::as_str),
            Some("decimal(12,4)")
        );
        assert_eq!(
            m.get("ext_ref_id_history.price").map(String::as_str),
            Some("decimal(8,2)"),
            "each recipe's bare key is qualified to ITS table"
        );
        assert_eq!(
            m.get("orders.note").map(String::as_str),
            Some("string"),
            "an already-qualified recipe key is kept as written"
        );
        assert_eq!(m.get("flag").map(String::as_str), Some("bool"));
        assert!(
            !m.contains_key("price"),
            "no bare recipe key may bleed across tables: {m:?}"
        );

        auto.columns
            .insert("orders.price".to_string(), "decimal(14,6)".to_string());
        let all = vec![orders.clone(), history.clone(), auto.clone()];
        assert_eq!(
            effective_columns(&auto, &all)
                .get("orders.price")
                .map(String::as_str),
            Some("decimal(14,6)")
        );

        let mut plain = auto.clone();
        plain.cdc = None;
        assert_eq!(effective_columns(&plain, &all), plain.columns);

        let mut twin = orders.clone();
        twin.columns = std::collections::HashMap::from([
            ("price".to_string(), "decimal(8,2)".to_string()),
            ("orders.price".to_string(), "decimal(12,4)".to_string()),
        ]);
        let mut auto_twin = auto.clone();
        auto_twin.columns.clear();
        let all = vec![twin, history.clone(), auto_twin.clone()];
        for _ in 0..20 {
            assert_eq!(
                effective_columns(&auto_twin, &all)
                    .get("orders.price")
                    .map(String::as_str),
                Some("decimal(12,4)")
            );
        }
    }

    /// One predicate for "a baseline on this engine needs a checkpoint".
    #[test]
    fn baseline_checkpoint_refusal_is_one_rule_for_both_baselines() {
        use crate::config::{CdcInitialMode, SourceType};
        let with = |initial: Option<CdcInitialMode>, backfill: Option<CdcBackfill>, ckpt: bool| {
            let mut e = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
            e.cdc = Some(CdcExportConfig {
                initial,
                backfill,
                checkpoint: ckpt.then(|| "/tmp/ck".to_string()),
                ..Default::default()
            });
            e
        };
        let snap = Some(CdcInitialMode::Snapshot);
        let auto = Some(CdcBackfill::Auto(AutoWord::Auto));

        for (e, engine) in [
            (with(snap, None, false), SourceType::Mysql),
            (with(None, auto.clone(), false), SourceType::Mysql),
            (with(snap, None, false), SourceType::Mssql),
            (with(None, auto.clone(), false), SourceType::Mongo),
        ] {
            let why = baseline_checkpoint_refusal(&e, engine).expect("must refuse");
            assert!(why.contains("cdc.checkpoint"), "{why}");
        }
        assert!(
            baseline_checkpoint_refusal(&with(snap, None, false), SourceType::Postgres).is_none()
        );
        assert!(
            baseline_checkpoint_refusal(&with(None, auto.clone(), false), SourceType::Postgres)
                .is_none()
        );
        assert!(baseline_checkpoint_refusal(&with(snap, None, true), SourceType::Mysql).is_none());
        assert!(baseline_checkpoint_refusal(&with(None, auto, true), SourceType::Mssql).is_none());
        assert!(baseline_checkpoint_refusal(&with(None, None, false), SourceType::Mysql).is_none());
    }

    /// The run loop asks this to avoid reading one table twice in one invocation.
    #[test]
    fn backfill_recipe_names_lists_claimed_exports_and_tolerates_a_broken_config() {
        let orders = recipe("orders", "orders");
        let auto = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
        let names = backfill_recipe_names(&[orders.clone(), auto.clone()]);
        assert_eq!(
            names.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["orders"]
        );

        let broken = stream(&["payments"], CdcBackfill::Auto(AutoWord::Auto));
        assert!(backfill_recipe_names(&[orders, broken]).is_empty());
    }

    /// The whole-config loops decide through these two, not inline.
    #[test]
    fn whole_config_loops_skip_recipes_and_plan_skips_streams_too() {
        let orders = recipe("orders", "orders");
        let plain = recipe("payments", "payments");
        let auto = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = [orders.clone(), plain.clone(), auto.clone()];
        let recipes = backfill_recipe_names(&all);

        let kept: Vec<&str> = without_backfill_recipes(&all, &recipes)
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        assert_eq!(
            kept,
            vec!["payments", "stand_cdc"],
            "run keeps the stream, drops its recipe"
        );

        assert!(batch_plan_skip_reason(&plain, &recipes).is_none());
        assert!(
            batch_plan_skip_reason(&orders, &recipes)
                .is_some_and(|w| w.contains("backfill recipe")),
            "the recipe is skipped for being a recipe"
        );
        assert!(
            batch_plan_skip_reason(&auto, &recipes).is_some_and(|w| w.contains("CDC export")),
            "the stream is skipped for being a stream"
        );
        let mut both = auto;
        both.name = "orders".into();
        assert!(batch_plan_skip_reason(&both, &recipes).is_some_and(|w| w.contains("CDC export")));
    }

    /// The three predicates the CDC job's live-only bodies decide through (extracted from inline operators the purity gate flagged, 2026-09-17).
    #[test]
    fn cdc_job_predicates_have_their_truth_tables() {
        let orders = recipe("orders", "orders");
        let all = [
            orders.clone(),
            stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto)),
        ];
        let pairs = resolve_backfill(&all[1], &all).unwrap();
        assert_eq!(
            backfill_recipe_for(&pairs, "orders").map(|r| r.name.as_str()),
            Some("orders")
        );
        assert!(backfill_recipe_for(&pairs, "payments").is_none());
        assert!(backfill_recipe_for(&[], "orders").is_none());

        let mut cdc = CdcExportConfig::default();
        assert!(cdc.captures_changes_only(), "no initial, no backfill");
        cdc.initial = Some(CdcInitialMode::Snapshot);
        assert!(!cdc.captures_changes_only());
        cdc.initial = None;
        cdc.backfill = Some(CdcBackfill::Auto(AutoWord::Auto));
        assert!(!cdc.captures_changes_only());

        let mut cdc = CdcExportConfig::default();
        assert!(!cdc.runs_until_stopped(), "the default drain is bounded");
        cdc.until_current = false;
        assert!(cdc.runs_until_stopped());
    }
}
