use std::path::Path;

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::plan::{DiagnosticLevel, ExtractionStrategy, ResolvedRunPlan, validate_plan};
use crate::state::{ProgressClaim, StateStore};

use super::RunOptions;
use super::chunked::{self, run_chunked_parallel_checkpoint};
use super::single::{
    commit_incremental_cursor, refuse_a_cursor_past_the_manifest, run_with_reconnect,
};
use super::summary::RunSummary;
use crate::journal::RunEvent;

/// Classify a raw error message into a stable `error_class` for failure grouping.
pub(crate) fn classify_error_message(msg: &str) -> Option<&'static str> {
    let m = msg.to_ascii_lowercase();
    if m.contains("keyset could not read") || m.contains("could not read the key value") {
        Some("keyset_unreadable_key")
    } else if m.contains("parallel checkpoint worker") {
        Some("parallel_checkpoint")
    } else if m.contains("deadlock") {
        Some("deadlock")
    } else if m.contains("lock wait timeout") || m.contains("could not obtain lock") {
        Some("lock_timeout")
    } else if m.contains("3024")
        || m.contains("maximum statement execution time")
        || m.contains("statement timeout")
    {
        Some("statement_timeout")
    } else if m.contains("schema drift")
        || m.contains("schema changed")
        || m.contains("on_schema_drift")
    {
        Some("schema_drift")
    } else if m.contains("doesn't exist")
        || m.contains("does not exist")
        || m.contains("no such table")
        || m.contains("invalid object name")
        || m.contains("unknown table")
    {
        Some("relation_not_found")
    } else if m.contains("permission denied")
        || m.contains("command denied")
        || m.contains("privilege")
        || m.contains("view server state")
    {
        Some("privilege")
    } else if m.contains("access denied")
        || m.contains("authentication failed")
        || m.contains("password authentication")
    {
        Some("source_auth")
    } else if m.contains("connection reset")
        || m.contains("connection refused")
        || m.contains("connection closed")
        || m.contains("broken pipe")
        || m.contains("server has gone away")
        || m.contains("connection timed out")
    {
        Some("connection")
    } else if m.contains("certificate") || m.contains("tls handshake") || m.contains("ssl error") {
        Some("tls")
    } else if m.contains("no space left") || m.contains("disk full") || m.contains("quota exceeded")
    {
        Some("disk_full")
    } else if m.contains("out of memory") || m.contains("cannot allocate memory") {
        Some("out_of_memory")
    } else {
        None
    }
}

/// The resolved strategy's `(label, key column)` — the column the run pages by.
fn strategy_key_column(plan: &ResolvedRunPlan) -> Option<(&'static str, &str)> {
    match &plan.strategy {
        ExtractionStrategy::Keyset(k) => Some(("keyset", &k.key_column)),
        ExtractionStrategy::Chunked(c) => Some(("chunked", &c.column)),
        ExtractionStrategy::Incremental(i) => Some(("incremental", &i.primary_column)),
        _ => None,
    }
}

/// A compact JSON descriptor of the resolved strategy's KEY.
fn key_descriptor_json(plan: &ResolvedRunPlan, key_native_type: Option<&str>) -> Option<String> {
    let (strategy, key) = strategy_key_column(plan)?;
    let mut obj = serde_json::json!({ "strategy": strategy, "key": key });
    if let Some(t) = key_native_type {
        obj["db_type"] = serde_json::Value::String(t.to_string());
        if t.to_ascii_lowercase().contains("unsigned") {
            obj["unsigned"] = serde_json::Value::Bool(true);
        }
    }
    Some(obj.to_string())
}

/// Capture failure-forensics context at run OPEN, best-effort.
fn capture_open_forensics(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    summary: &mut RunSummary,
    want_key: bool,
    meta: &mut MetaConn<'_>,
) {
    let Some(src) = meta.get() else {
        log::debug!(
            "open-forensics: no source connection for '{}'",
            plan.export_name
        );
        return;
    };
    summary.server_context_json = src.server_context();
    match crate::preflight::type_report::probe_mappings(
        src,
        &plan.base_query,
        &plan.column_overrides,
    ) {
        Ok(mappings) => {
            if want_key && let Some(relation) = plan.source_table.as_deref() {
                summary.open_primary_key = src.primary_key(relation).unwrap_or_else(|e| {
                    log::warn!(
                        "export '{}': could not read the primary key of `{relation}`: {e:#}",
                        plan.export_name
                    );
                    None
                });
            }
            let cols: Vec<crate::state::SchemaColumn> = mappings
                .iter()
                .map(|m| crate::state::SchemaColumn {
                    name: m.column_name.clone(),
                    data_type: m
                        .arrow_type
                        .as_ref()
                        .map(|t| format!("{t:?}"))
                        .unwrap_or_else(|| m.source_native_type.clone()),
                })
                .collect();
            if let Some((_, key)) = strategy_key_column(plan) {
                summary.key_native_type = mappings
                    .iter()
                    .find(|m| m.column_name == key)
                    .map(|m| m.source_native_type.clone());
            }
            if let Err(e) = state.store_schema_if_absent(&summary.export_name, &cols) {
                log::debug!(
                    "open-forensics: store_schema_if_absent failed for '{}': {e}",
                    summary.export_name
                );
            }
            summary.open_mappings = Some(mappings);
        }
        Err(e) => log::debug!(
            "open-forensics: type_mappings failed for '{}': {e}",
            plan.export_name
        ),
    }
}

/// The run's column plan.
fn plan_open_columns(
    export_name: &str,
    open: &mut Option<Vec<crate::types::TypeMapping>>,
    strict: bool,
) -> Result<()> {
    let subject = format!("export '{export_name}'");
    match open.as_ref() {
        Some(m) => {
            let planned = crate::types::plan_columns(
                m.clone(),
                &crate::types::policy::TypePolicy::from_strict(strict),
                &subject,
            )?;
            *open = Some(planned);
        }
        None if strict => crate::rivet_bail!(
            crate::error::codes::TYPE_UNSAFE_MAPPING,
            "{subject}: strict mode: the column types could not be resolved at run start, so \
             the type policy cannot be checked (run `rivet check --type-report` to see why)"
        ),
        None => {}
    }
    Ok(())
}

/// A settle column must be a date/timestamp: the window compares it with the source clock.
fn settle_columns_are_temporal(
    plan: &ResolvedRunPlan,
    mappings: Option<&[crate::types::TypeMapping]>,
) -> Result<()> {
    let (Some(inc), Some(mappings)) = (plan.strategy.incremental_plan(), mappings) else {
        return Ok(());
    };
    for name in inc.settle_columns() {
        let Some(m) = mappings.iter().find(|m| m.column_name == name) else {
            continue;
        };
        if !matches!(
            m.rivet_type,
            crate::types::RivetType::Date | crate::types::RivetType::Timestamp { .. }
        ) {
            crate::rivet_bail!(
                crate::error::codes::CONFIG_SETTLE_COLUMN_NOT_TEMPORAL,
                "export '{}': settle column `{name}` is {}, not a date/timestamp — the settle \
                 window compares it against the source clock. Point `settle.column` at the \
                 row's insert/update time (the cursor is used when `settle.column` is omitted).",
                plan.export_name,
                m.source_native_type
            );
        }
    }
    Ok(())
}

/// Assemble the full `export_metrics` row (v9) from the finished run summary + plan.
pub(super) fn build_metric_row(
    summary: &RunSummary,
    plan: &ResolvedRunPlan,
    tuning_class: &str,
) -> crate::state::MetricRow {
    let (chunk_size, parallel) = match &plan.strategy {
        crate::plan::ExtractionStrategy::Chunked(cp) => {
            (Some(cp.chunk_size as i64), Some(cp.parallel as i64))
        }
        crate::plan::ExtractionStrategy::Keyset(kp) => {
            (Some(kp.chunk_size as i64), Some(kp.parallel.max(1) as i64))
        }
        _ => (None, None),
    };
    crate::state::MetricRow {
        export_name: summary.export_name.clone(),
        run_id: summary.run_id.clone(),
        duration_ms: summary.duration_ms,
        total_rows: summary.total_rows,
        peak_rss_mb: Some(summary.peak_rss_mb),
        status: summary.status.clone(),
        error_message: summary.error_message.clone(),
        tuning_profile: Some(tuning_class.to_string()),
        format: Some(summary.format.clone()),
        mode: Some(summary.mode.clone()),
        files_produced: summary.files_produced as i64,
        bytes_written: summary.bytes_written as i64,
        bytes_read: summary.bytes_read as i64,
        retries: summary.retries as i64,
        validated: summary.validated,
        schema_changed: summary.schema_changed,
        files_committed: summary.files_committed as i64,
        reconciled: summary.reconciled,
        source_count: summary.source_count,
        quality_passed: summary.quality_passed,
        pg_temp_bytes_delta: summary.pg_temp_bytes_delta,
        batch_size: summary.batch_size as i64,
        batch_size_memory_mb: summary.batch_size_memory_mb.map(|m| m as i64),
        skip_reason: summary.skip_reason.clone(),
        schema_fingerprint: summary.schema_fingerprint.clone(),
        chunk_size,
        parallel,
        source_type: Some(plan.source.source_type.ledger_label()),
        destination_type: Some(plan.destination.destination_type.label().to_string()),
        rivet_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        longest_chunk_ms: summary.journal.longest_chunk_ms(),
        chunk_key: plan.strategy.chunk_key().map(str::to_string),
        error_class: summary
            .error_message
            .as_deref()
            .and_then(classify_error_message)
            .map(str::to_string),
        cursor_min: summary.cursor_low.clone(),
        cursor_max: summary.cursor_high.clone(),
        key_descriptor_json: key_descriptor_json(plan, summary.key_native_type.as_deref()),
        offending_value: summary.offending_value.clone(),
        server_context_json: summary.server_context_json.clone(),
    }
}

/// The PostgreSQL temp-spill counter carried in a harm snapshot; `None` for other engines.
fn temp_bytes_of(harm: Option<&[(String, i64)]>) -> Option<i64> {
    harm?
        .iter()
        .find(|(k, _)| k == crate::source::PG_TEMP_BYTES_KEY)
        .map(|(_, v)| *v)
}

/// The harm deltas worth recording: all but the temp-spill, which has its own summary field.
fn recorded_harm(deltas: Vec<(String, i64)>) -> Vec<(String, i64)> {
    deltas
        .into_iter()
        .filter(|(k, _)| k != crate::source::PG_TEMP_BYTES_KEY)
        .collect()
}

/// A batch run's one metadata connection: opened at the start, reopened once if it died idle.
pub(super) struct MetaConn<'a> {
    cfg: &'a crate::config::SourceConfig,
    src: Option<Box<dyn crate::source::Source>>,
}

impl<'a> MetaConn<'a> {
    /// Connect once for the run; a failed connect leaves it empty rather than failing the run.
    pub(super) fn open(cfg: &'a crate::config::SourceConfig) -> Self {
        Self {
            cfg,
            src: crate::source::create_source(cfg).ok(),
        }
    }

    /// No connection yet; the first `require` opens one.
    #[cfg(test)]
    pub(super) fn empty(cfg: &'a crate::config::SourceConfig) -> Self {
        Self { cfg, src: None }
    }

    /// The held connection, or a fresh one (with the connect hint) when the run's open failed.
    pub(super) fn require(
        &mut self,
    ) -> crate::error::Result<&mut (dyn crate::source::Source + 'static)> {
        let src = match self.src.take() {
            Some(src) => src,
            None => crate::source::create_source(self.cfg)
                .map_err(|e| crate::pipeline::single::attach_connect_hint(e, self.cfg))?,
        };
        Ok(self.src.insert(src).as_mut())
    }

    /// The held connection, if the run has one.
    pub(super) fn get(&mut self) -> Option<&mut (dyn crate::source::Source + 'static)> {
        self.src.as_deref_mut()
    }

    /// Replace the connection with a fresh one (a server or a network may drop an idle one).
    pub(super) fn reopen(
        &mut self,
    ) -> crate::error::Result<&mut (dyn crate::source::Source + 'static)> {
        self.src = None;
        let fresh = crate::source::create_source(self.cfg)?;
        Ok(self.src.insert(fresh).as_mut())
    }
}

/// Whether to reopen the run's connection to close the harm bracket.
fn reopen_to_close_the_bracket(opened: bool, answered: bool) -> bool {
    opened && !answered
}

/// Source-harm counters taken before a run window; `close` turns them into the run's deltas.
pub(super) struct HarmBracket {
    harm: Option<Vec<(String, i64)>>,
}

impl HarmBracket {
    /// Snapshot on a connection the caller already holds, so the bracket opens none.
    pub(super) fn open_on(src: Option<&mut (dyn crate::source::Source + '_)>) -> Self {
        Self {
            harm: src.and_then(|s| s.harm_counters()),
        }
    }

    /// Close on the run's metadata connection.
    pub(super) fn close_on_meta(
        self,
        meta: &mut MetaConn<'_>,
        state: &StateStore,
        summary: &mut RunSummary,
    ) -> Vec<(String, i64)> {
        let mut after = meta.get().and_then(|s| s.harm_counters());
        if reopen_to_close_the_bracket(self.harm.is_some(), after.is_some()) {
            after = meta.reopen().ok().and_then(|s| s.harm_counters());
        }
        self.close_with(after, state, summary)
    }

    /// Close on the same window.
    pub(super) fn close_on(
        self,
        src: Option<&mut (dyn crate::source::Source + '_)>,
        state: &StateStore,
        summary: &mut RunSummary,
    ) -> Vec<(String, i64)> {
        let after = src.and_then(|s| s.harm_counters());
        self.close_with(after, state, summary)
    }

    /// The close itself, given the counters read at the end of the window.
    fn close_with(
        self,
        after: Option<Vec<(String, i64)>>,
        state: &StateStore,
        summary: &mut RunSummary,
    ) -> Vec<(String, i64)> {
        if let (Some(before), Some(now)) = (
            temp_bytes_of(self.harm.as_deref()),
            temp_bytes_of(after.as_deref()),
        ) {
            let delta = pg_temp_bytes_delta(before, now);
            summary.pg_temp_bytes_delta = Some(delta);
            if let Some(line) = pg_temp_bytes_warning(
                &summary.export_name,
                delta,
                super::run::multi_export_concurrent(),
            ) {
                log::warn!("{line}");
            }
        }
        let (Some(before), Some(after)) = (self.harm, after) else {
            return Vec::new();
        };
        let deltas = recorded_harm(harm_deltas(&before, &after));
        if let Err(e) = state.record_harm(&summary.run_id, &summary.export_name, &deltas) {
            log::debug!(
                "'{}': harm metrics write failed (informational): {:#}",
                summary.export_name,
                e
            );
        }
        deltas
    }
}

/// The temp-spill the run gets CREDITED with, from the two snapshots bracketing its window.
fn pg_temp_bytes_delta(before: i64, after: i64) -> i64 {
    (after - before).max(0)
}

/// Volume above which a `temp_bytes` delta is worth a WARN (100 MB).
const PG_TEMP_BYTES_WARN_MIN: i64 = 100 * 1024 * 1024;

/// The per-export PG temp-spill warning, pure so its wording is unit-tested.
fn pg_temp_bytes_warning(
    export_name: &str,
    delta: i64,
    concurrent_siblings: bool,
) -> Option<String> {
    if delta <= PG_TEMP_BYTES_WARN_MIN {
        return None;
    }
    let mb = delta as f64 / (1024.0 * 1024.0);
    let remedy = "Consider lowering `tuning.batch_size` or setting \
                  `tuning.batch_size_memory_mb` below PG's `work_mem`.";
    Some(if concurrent_siblings {
        format!(
            "export '{export_name}': PG temp_bytes spill +{mb:.1} MB during this export's \
             window — real source pressure, but `pg_stat_database.temp_bytes` is \
             database-wide and concurrent sibling exports share the window, so per-export \
             attribution is unknown. If it tracks this export: {remedy}"
        )
    } else {
        format!(
            "export '{export_name}': PG temp_bytes spill +{mb:.1} MB during run — \
             cursor / sort overflow. {remedy}"
        )
    })
}

/// Snapshot the broader source-harm counters for the run's source engine.
pub(super) fn harm_snapshot(source: &crate::config::SourceConfig) -> Option<Vec<(String, i64)>> {
    crate::source::create_source(source).ok()?.harm_counters()
}

/// Per-metric delta (`after - before`, floored at 0) for counters present in both snapshots.
pub(super) fn harm_deltas(before: &[(String, i64)], after: &[(String, i64)]) -> Vec<(String, i64)> {
    let bmap: std::collections::HashMap<&str, i64> =
        before.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    after
        .iter()
        .filter_map(|(k, after_v)| {
            bmap.get(k.as_str())
                .map(|b| (k.clone(), (after_v - b).max(0)))
        })
        .collect()
}

/// A one-line run-health DIAGNOSIS for the operator's log.
pub(super) const SPILL_FLAG_MIN: i64 = 100;

/// What a [`spill_total`] fold counted, so the message can name it truthfully.
pub(super) struct Spill {
    /// Summed delta across every spill counter the engine reported.
    pub total: i64,
    /// The NOUN for what `total` counts.
    pub unit: &'static str,
}

/// Sum the source-harm deltas that mean "the source had to spill to disk".
pub(super) fn spill_total(deltas: &[(String, i64)]) -> Spill {
    let sum = |needle: &str| -> i64 {
        deltas
            .iter()
            .filter(|(k, _)| k.contains(needle))
            .map(|(_, v)| *v)
            .sum()
    };
    let tmp_disk = sum("tmp_disk");
    let temp_files = sum("temp_files");
    let unit = match (tmp_disk > 0, temp_files > 0) {
        (true, true) => "disk spills (tmp-disk tables + temp files)",
        (false, true) => "temp-file spills",
        _ => "tmp-disk spills",
    };
    Spill {
        total: tmp_disk + temp_files,
        unit,
    }
}

pub(super) fn run_diagnosis(
    summary: &RunSummary,
    harm_deltas: &[(String, i64)],
    concurrent_siblings: bool,
) -> Option<String> {
    let mut flags: Vec<String> = Vec::new();
    if summary.reconnects > 0 {
        flags.push(format!(
            "{} reconnect(s) survived (flaky link)",
            summary.reconnects
        ));
    }
    if summary.resumed {
        flags.push("resumed a prior CRASHED run".to_string());
    }
    let Spill {
        total: spills,
        unit: spill_unit,
    } = spill_total(harm_deltas);
    if spills >= SPILL_FLAG_MIN {
        let remedy = match summary.mode.as_str() {
            "chunked" | "keyset" => "lower `chunk_size` (smaller pages) or `tuning.batch_size`",
            _ => "try `mode: chunked`/`chunk_by_key` or a smaller `tuning.batch_size`",
        };
        if concurrent_siblings {
            flags.push(format!(
                "{spills} {spill_unit} server-wide during this export's window — real \
                 source pressure, but the counter is server-global and concurrent \
                 sibling exports share the window, so per-export attribution is \
                 unknown (the run-level harm line carries the whole-window total); \
                 if it tracks this export, {remedy}"
            ));
        } else {
            flags.push(format!(
                "{spills} {spill_unit} server-wide during this export's window — the counter \
                 is server-global, so other sessions' work counts too; if the source is \
                 otherwise quiet this export spilled it: {remedy}"
            ));
        }
    }
    if flags.is_empty() {
        return None;
    }
    Some(format!(
        "export '{}': DIAGNOSIS — {} rows · peak RSS {} MB · {} ms [{}] · retries={} · {}",
        summary.export_name,
        summary.total_rows,
        summary.peak_rss_mb,
        summary.duration_ms,
        summary.status,
        summary.retries,
        flags.join("; "),
    ))
}

/// Run `SELECT COUNT(*) FROM ({query})` against the source and compare with exported rows.
fn resolve_final_result(
    failed: bool,
    run_result: crate::error::Result<()>,
    manifest_gap: Option<String>,
    gates: [crate::error::Result<()>; 3],
) -> crate::error::Result<()> {
    if failed {
        return run_result;
    }
    if let Some(why) = manifest_gap {
        return Err(crate::error::coded(
            crate::error::codes::DEST_MANIFEST_NOT_WRITTEN,
            why,
        ));
    }
    gates.into_iter().collect()
}

/// Exit gate for `run --validate`.
fn validate_run_gate(summary: &RunSummary) -> crate::error::Result<()> {
    use crate::error::codes::{VALIDATE_FAILED, VALIDATE_UNVERIFIED};
    if summary.validated != Some(false) {
        return Ok(());
    }
    let verdict = summary.manifest_verification.as_ref();
    let why = verdict.map_or_else(
        || "no verdict was recorded".to_string(),
        |v| {
            let fatal = v.failures.iter().filter(|f| f.is_fatal());
            fatal
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ")
        },
    );
    let code = if verdict.is_none_or(|v| v.has_verified_wrong_failure()) {
        VALIDATE_FAILED
    } else {
        VALIDATE_UNVERIFIED
    };
    crate::rivet_bail!(
        code,
        "`--validate` did not pass for '{}': {why}. The export completed and its manifest is \
         written; `rivet validate` re-checks the prefix part by part.",
        summary.export_name
    )
}

/// The failure of a run whose rows and manifest are durable and whose incremental cursor was not stored.
fn cursor_not_stored(kind: &str, export_name: &str, cause: &anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::STATE_CURSOR_NOT_STORED,
        format!(
            "{kind} '{export_name}': the rows are delivered and the manifest is written, but \
             the incremental cursor could not be stored ({cause:#}), so the next run delivers \
             the same rows again. Repair the state database before the next run."
        ),
    ))
}

fn reconcile_run_gate(
    summary: &RunSummary,
    could_not_verify: Option<&str>,
) -> crate::error::Result<()> {
    if summary.reconciled == Some(false) {
        return Err(crate::error::DataIntegrityError::new(format!(
            "reconcile MISMATCH for '{}': the exported dataset disagrees with the source \
             count {} — see the reconcile log above",
            summary.export_name,
            summary.source_count.unwrap_or(-1),
        ))
        .into());
    }
    if let Some(reason) = could_not_verify {
        crate::rivet_bail!(
            crate::error::codes::RUN_RECONCILE_UNVERIFIED,
            "reconcile could not be verified for '{}': {reason}. The export completed and is \
             durable; re-run to obtain the reconcile assurance, or drop `--reconcile`.",
            summary.export_name
        );
    }
    Ok(())
}

/// Run the reconcile COUNT(*) and record `source_count` / `reconciled` on the summary.
fn reconcile_source_count(
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    meta: &mut MetaConn<'_>,
) -> Option<String> {
    if let Some(reason) = plan.strategy.reconcile_subset_skip() {
        log::info!(
            "reconcile: skipping full-count for '{}' ({reason})",
            plan.export_name
        );
        return None;
    }

    let count_sql = format!(
        "SELECT COUNT(*) FROM ({}) {}",
        plan.base_query,
        crate::sql::derived(plan.source.source_type, "_rivet_reconcile")
    );
    log::info!(
        "reconcile: running source count query for '{}'",
        plan.export_name
    );

    let counted = match meta.get().map(|s| s.query_scalar(&count_sql)) {
        Some(Ok(v)) => Ok(v),
        _ => match meta.reopen() {
            Ok(s) => s.query_scalar(&count_sql),
            Err(e) => {
                log::warn!("reconcile: could not connect to source: {e:#}");
                return Some(format!(
                    "could not connect to the source to reconcile: {e:#}"
                ));
            }
        },
    };

    match counted {
        Ok(Some(val)) => {
            let Ok(count) = val.parse::<i64>() else {
                log::warn!("reconcile: could not parse count result '{val}' as integer");
                return Some(format!("source reconcile count '{val}' is not an integer"));
            };
            summary.source_count = Some(count);
            let committed_rows: i64 = summary.manifest_parts.iter().map(|p| p.rows).sum();
            let exported_total = if committed_rows > 0 {
                committed_rows
            } else {
                summary.total_rows
            };
            summary.reconciled = Some(exported_total == count);
            if exported_total != count {
                log::warn!(
                    "reconcile MISMATCH for '{}': committed {} rows, source has {}",
                    plan.export_name,
                    exported_total,
                    count
                );
            } else {
                log::info!(
                    "reconcile MATCH for '{}': {}/{}",
                    plan.export_name,
                    exported_total,
                    count
                );
            }
            None
        }
        Ok(None) => {
            log::warn!(
                "reconcile: COUNT(*) returned NULL for '{}'",
                plan.export_name
            );
            Some("source reconcile COUNT(*) returned NULL".to_string())
        }
        Err(e) => {
            log::warn!(
                "reconcile: count query failed for '{}': {:#}",
                plan.export_name,
                e
            );
            Some(format!("source reconcile count query failed: {e:#}"))
        }
    }
}

/// Synthesize a stand-in `RunSummary` for failures that occur **before** a real summary can be created (plan-build errors, plan-validation rejection).
pub(crate) fn synthetic_failed_summary(export_name: &str, err: &anyhow::Error) -> RunSummary {
    let run_id = super::summary::fresh_run_id(export_name);
    let journal = crate::journal::RunJournal::new(&run_id, export_name);
    RunSummary {
        bytes_read: 0,
        ledger: Default::default(),
        cursor_column: None,
        cursor_low: None,
        cursor_high: None,
        open_mappings: None,
        open_primary_key: None,
        offending_value: None,
        server_context_json: None,
        key_native_type: None,
        state_backed: false,
        run_id,
        export_name: export_name.to_string(),
        status: "failed".into(),
        total_rows: 0,
        files_produced: 0,
        bytes_written: 0,
        files_committed: 0,
        files_adopted: 0,
        duration_ms: 0,
        peak_rss_mb: 0,
        retries: 0,
        reconnects: 0,
        resumed: false,
        chunks_precomputed: false,
        validated: None,
        schema_changed: None,
        quality_passed: None,
        error_message: Some(crate::redact::redact_error(err)),
        tuning_profile: "balanced (default)".into(),
        batch_size: 0,
        batch_size_memory_mb: None,
        format: String::new(),
        mode: String::new(),
        compression: String::new(),
        destination_uri: None,
        source_count: None,
        pg_temp_bytes_delta: None,
        skip_reason: None,
        reconciled: None,
        manifest_parts: Vec::new(),
        schema_fingerprint: None,
        manifest_verification: None,
        apply_context: None,
        snapshot_legs: Vec::new(),
        column_checksums: Vec::new(),
        column_checksums_incomplete: false,
        column_checksums_short_cover: false,
        checksum_key_column: None,
        journal,
    }
}

/// Clear the keyset in-progress resume anchor after a SUCCESSFUL finalize.
pub(super) struct RunOutcome<'a> {
    /// `result.is_err()` — the export/quality verdict, computed BEFORE the manifest is written.
    pub failed: bool,
    /// The runner returned `Ok`: every row was read before any post-data gate ran.
    pub data_complete: bool,
    /// `Some(why)` when `finalize_manifest` could not write the manifest.
    pub manifest_gap: &'a Option<String>,
}

/// Did a post-data gate (schema drift, quality) refuse a run whose runner had read every row?
fn refused_by_a_gate(o: &RunOutcome<'_>) -> bool {
    o.data_complete && o.failed
}

/// Whether the keyset resume anchor outlives the run.
pub(super) fn keyset_anchor_survives(o: &RunOutcome<'_>, cursor_ran_ahead: bool) -> bool {
    if refused_by_a_gate(o) {
        return cursor_ran_ahead;
    }
    o.failed || o.manifest_gap.is_some()
}

/// Whether the stored cursor sits on one of `run_id`'s own pages.
fn cursor_ran_ahead(progress: &ProgressClaim<'_>, plan: &ResolvedRunPlan, run_id: &str) -> bool {
    let ExtractionStrategy::Keyset(kp) = &plan.strategy else {
        return false;
    };
    if !kp.incremental {
        return false;
    }
    progress.cursor_is_a_page_of(run_id).unwrap_or(true)
}

/// What a refused run says when its anchor is kept because the stored cursor already moved.
fn ran_ahead_warning(export_name: &str, run_id: &str) -> String {
    format!(
        "export '{export_name}': refused run {run_id} cannot be read again — the stored cursor \
         already sits past pages it committed (a run interrupted under rivet 0.31 or earlier, or \
         one stopped right after its cursor moved), so the next run adopts those pages without \
         grading them. To have every row read and graded again, run `rivet state reset -c \
         <config> --export {export_name}` before the next run"
    )
}

/// Whether the incremental cursor may advance: only a successful run whose manifest landed.
fn cursor_may_advance(status: &str, manifest_gap: &Option<String>) -> bool {
    status == "success" && manifest_gap.is_none()
}

/// Release or keep the keyset resume anchor of the run that just ended; returns the warning to log.
fn finalize_keyset_anchor(
    state: &StateStore,
    progress: &ProgressClaim<'_>,
    plan: &ResolvedRunPlan,
    run_id: &str,
    o: &RunOutcome<'_>,
) -> Option<String> {
    if !matches!(plan.strategy, ExtractionStrategy::Keyset(_)) {
        return None;
    }
    let ran_ahead = refused_by_a_gate(o) && cursor_ran_ahead(progress, plan, run_id);
    if !keyset_anchor_survives(o, ran_ahead) || !anchor_holds_a_part(state, progress) {
        let _ = state.clear_resume_run_id(&plan.export_name, &plan.source.state_key());
        let _ = state.clear_keyset_ranges(&plan.export_name, &plan.source.state_key());
    }
    ran_ahead.then(|| ran_ahead_warning(&plan.export_name, run_id))
}

/// Whether the stored keyset anchor names a run that recorded a part.
fn anchor_holds_a_part(state: &StateStore, progress: &ProgressClaim<'_>) -> bool {
    match progress.resume_run_id() {
        Ok(Some(anchor)) => !state
            .list_files_for_run(&anchor)
            .is_ok_and(|parts| parts.is_empty()),
        Ok(None) => false,
        Err(_) => true,
    }
}

/// Mark a run `running` at its START.
fn ledger_begin_run(
    state: &StateStore,
    plan: &ResolvedRunPlan,
    export_family: &str,
    run_id: &str,
) -> Result<()> {
    let prefix = super::finalize::destination_uri_for_manifest(&plan.destination);
    let started_at = chrono::Utc::now().to_rfc3339();
    state
        .begin_run_under_lease(
            run_id,
            &plan.export_name,
            &plan.source.state_key(),
            &prefix,
            &started_at,
        )
        .map_err(|e| state_not_writable(&plan.export_name, &e))?;
    super::finalize::write_running_manifest(plan, export_family, run_id, &started_at);
    Ok(())
}

/// The run lease of a `mode: cdc` export, refused by code while a live process holds it.
fn cdc_run_lease<'s>(
    state: &'s StateStore,
    export: &str,
    source: &str,
) -> Result<crate::state::LoadLease<'s>> {
    match chunked::try_run_lease(state, export, source) {
        Ok(Some(lease)) => Ok(lease),
        Ok(None) => crate::rivet_bail!(
            crate::error::codes::STATE_RUN_IN_PROGRESS,
            "{}",
            chunked::live_run_refusal(
                export,
                state.newest_running_run(export, source)?.as_deref(),
                false
            )
        ),
        Err(e) => Err(state_not_writable(export, &e)),
    }
}

/// The refusal of a run whose state store did not take its first write.
fn state_not_writable(export_name: &str, cause: &anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::STATE_NOT_WRITABLE,
        format!(
            "export '{export_name}': the state database did not take this run's first write \
             ({cause:#}). A run that cannot store its progress delivers the same rows again on \
             the next run, so the export did not start and nothing was written to the \
             destination. Make the state database writable and run again."
        ),
    ))
}

/// Transition a run to its terminal status in the run-status ledger at finalize.
fn ledger_finish_owned_runs(
    state: &StateStore,
    export_name: &str,
    opened_as: &str,
    summary: &RunSummary,
) {
    ledger_finish_run(state, export_name, &summary.run_id, &summary.status);
    if opened_as != summary.run_id {
        ledger_finish_run(state, export_name, opened_as, &summary.status);
    }
}

fn ledger_finish_run(state: &StateStore, export_name: &str, run_id: &str, status: &str) {
    let finished_at = chrono::Utc::now().to_rfc3339();
    if let Err(e) = state.finish_run(run_id, status, &finished_at) {
        log::warn!("export '{export_name}': run-status finish failed: {e:#}");
    }
}

/// ADR-0028 follow-up (arch roast 2026-08-21, Strong #1).
struct TailPolicy<'a> {
    /// "export" | "apply" — log prefixes, manifest kind, report kind.
    kind: &'static str,
    /// Ledger/manifest family.
    family: &'a str,
    /// Where the run report lands.
    config_path: &'a str,
    /// What the runners receive as their config-path argument.
    runner_config_path: &'a str,
    chunk_source: chunked::ChunkSource,
    /// Apply-only provenance recorded on the summary.
    apply_context: Option<crate::pipeline::summary::ApplyContext>,
    /// The reconcile leg exists only on the run path; apply folds `Ok(())`.
    allow_reconcile: bool,
    /// Run-path notifications; apply sends none.
    notifications: Option<&'a crate::config::NotificationsConfig>,
    /// Whether the config has a `load:` block.
    record_load_spec: bool,
    /// Plan (rule, message) warnings the run path journals as `PlanWarning` events.
    plan_warnings: Vec<(String, String)>,
    /// `--strict`: a lossy or unsupported column refuses the run; apply warns.
    strict: bool,
    /// The run path's `--force`: a `--resume` with no chunk run to continue runs as a plain run.
    force: bool,
    /// The snapshot leg of a change-stream export.
    cdc_leg: bool,
}

/// Does this run's reconcile leg run?
fn should_reconcile(allow_reconcile: bool, plan_reconcile: bool, failed: bool) -> bool {
    allow_reconcile && plan_reconcile && !failed
}

/// The plan-validation verdict, pure.
fn log_plan_diagnostics(
    export_name: &str,
    diags: &[crate::plan::validate::Diagnostic],
) -> Option<anyhow::Error> {
    let mut rejected: Vec<String> = Vec::new();
    for d in diags {
        match d.level {
            DiagnosticLevel::Rejected => {
                log::error!("[{}] plan validation rejected: {}", d.rule, d.message);
                rejected.push(d.message.clone());
            }
            DiagnosticLevel::Warning => {
                log::warn!("[{}] plan validation warning: {}", d.rule, d.message);
            }
            DiagnosticLevel::Degraded => {
                log::info!("[{}] plan validation degraded: {}", d.rule, d.message);
            }
        }
    }
    plan_rejection_error(export_name, &rejected)
}

fn plan_rejection_error(export_name: &str, rejected: &[String]) -> Option<anyhow::Error> {
    if rejected.is_empty() {
        return None;
    }
    Some(crate::error::coded(
        crate::error::codes::PLAN_REJECTED,
        format!(
            "export '{}': plan validation failed:\n  {}",
            export_name,
            rejected.join("\n  ")
        ),
    ))
}

/// The two `--resume` / `--force` gates, pure.
fn resume_success_gate_applies(resume: bool, force: bool) -> bool {
    resume && !force
}

/// Sibling of [`resume_success_gate_applies`].
fn rerun_warning_applies(resume: bool, force: bool) -> bool {
    !resume && !force
}

/// May a successful run promote its status to `success`?
fn promotes_to_success(current_status: &str) -> bool {
    current_status == "running"
}

/// Does the `running` marker written under `opened` survive this run with nothing replacing it?
fn marker_outlived_its_run(prefix_left_alone: bool, opened: &str, finished: &str) -> bool {
    prefix_left_alone || opened != finished
}

/// Terminal status of a run whose runner returned Ok.
fn ok_status(skip_empty: bool, total_rows: i64, parts: usize) -> &'static str {
    if skip_empty && total_rows == 0 && parts == 0 {
        "skipped"
    } else {
        "success"
    }
}

/// Terminal status of an Ok run plus, when it was skipped, why.
fn ok_outcome(
    skip_empty: bool,
    total_rows: i64,
    parts: usize,
    strategy: &ExtractionStrategy,
) -> (&'static str, Option<String>) {
    let status = ok_status(skip_empty, total_rows, parts);
    let reason = (status == "skipped").then(|| skip_reason(awaited_column(strategy)));
    (status, reason)
}

/// The column a run with nothing new was waiting on.
fn awaited_column(strategy: &ExtractionStrategy) -> Option<&str> {
    match strategy {
        ExtractionStrategy::Keyset(k) if k.incremental => Some(k.key_column.as_str()),
        other => other.cursor_column(),
    }
}

/// Why a `skip_empty` run wrote nothing, for the summary card and metrics.
fn skip_reason(cursor_column: Option<&str>) -> String {
    match cursor_column {
        Some(col) => format!("no new rows since cursor '{col}'"),
        None => "source returned 0 rows".into(),
    }
}

/// Does this export bypass the batch plan/strategy machinery for the dedicated CDC runner?
fn dispatches_to_cdc_runner(mode: crate::config::ExportMode) -> bool {
    mode == crate::config::ExportMode::Cdc
}

/// THE post-plan execution script, written once.
fn execute_resolved_plan(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    tail: TailPolicy<'_>,
    mut meta: MetaConn<'_>,
) -> (Result<()>, RunSummary) {
    if let Some(e) = unexpanded_partition_error(plan) {
        let summary = synthetic_failed_summary(&plan.export_name, &e);
        return (Err(e), summary);
    }
    let lease = chunked::claim_run(state, plan).map_err(|e| match crate::error::error_code(&e) {
        Some(_) => e,
        None => state_not_writable(&plan.export_name, &e),
    });
    let claim = lease.and_then(|lease| {
        let progress = state.claim(plan.progress_key())?;
        let recovered = chunked::recover_checkpoint_run(plan, &progress, tail.force)?;
        refuse_a_cursor_past_the_manifest(recovered.as_ref().unwrap_or(plan), &progress)?;
        Ok((progress, lease, recovered))
    });
    let (progress, _run_lease, recovered) = match claim {
        Ok(claim) => claim,
        Err(e) => {
            let summary = synthetic_failed_summary(&plan.export_name, &e);
            return (Err(e), summary);
        }
    };
    let plan = recovered.as_ref().unwrap_or(plan);
    let start = std::time::Instant::now();
    let rss_before = crate::resource::get_rss_mb();
    let rss_sampler = crate::resource::RssPeakSampler::start(rss_before, 100);
    let mut summary = RunSummary::new(plan);
    summary.apply_context = tail.apply_context;
    if let Err(e) = ledger_begin_run(state, plan, tail.family, &summary.run_id) {
        rss_sampler.stop();
        let summary = synthetic_failed_summary(&plan.export_name, &e);
        return (Err(e), summary);
    }
    let ledger_run_id = summary.run_id.clone();
    capture_open_forensics(plan, state, &mut summary, tail.record_load_spec, &mut meta);

    let harm = HarmBracket::open_on(meta.get());

    for (rule, message) in &tail.plan_warnings {
        summary.journal.record(RunEvent::PlanWarning {
            rule: rule.clone(),
            message: message.clone(),
        });
    }

    let gate = settle_columns_are_temporal(plan, summary.open_mappings.as_deref()).and_then(|()| {
        plan_open_columns(&plan.export_name, &mut summary.open_mappings, tail.strict)
    });
    let result = match gate {
        Err(e) => Err(e),
        Ok(()) if plan.strategy.requires_parallel_execution() => {
            if plan.strategy.is_resumable() {
                run_chunked_parallel_checkpoint(
                    tail.runner_config_path,
                    state,
                    plan,
                    &mut summary,
                    tail.chunk_source,
                    &mut meta,
                    &progress,
                )
            } else {
                chunked::run_chunked_parallel(
                    state,
                    plan,
                    &mut summary,
                    tail.chunk_source,
                    &mut meta,
                )
            }
        }
        Ok(()) => run_with_reconnect(
            state,
            &progress,
            plan,
            &mut summary,
            tail.runner_config_path,
            tail.chunk_source,
        ),
    };
    let data_complete = result.is_ok();
    let result = match result {
        Ok(()) => super::finalize::finalize_export(plan, Some(state), &mut summary),
        Err(e) => {
            super::finalize::finalize_export_records(&mut summary);
            let e = crate::source::code_a_revoked_column(
                e,
                &plan.export_name,
                plan.source.columns_from_grants(),
                tail.cdc_leg,
            );
            Err(crate::source::code_a_missing_relation(
                e,
                &plan.export_name,
                plan.source_table.as_deref(),
                Some(&plan.base_query),
            ))
        }
    };

    let rss_peak = rss_sampler.stop();
    let rss_after = crate::resource::get_rss_mb();
    summary.bytes_read = plan.bytes_read.load(std::sync::atomic::Ordering::Relaxed);
    summary.duration_ms = start.elapsed().as_millis() as i64;
    summary.peak_rss_mb = rss_peak.max(rss_after).max(rss_before) as i64;

    let harm_delta_vec = harm.close_on_meta(&mut meta, state, &mut summary);
    let tuning_class = plan.tuning.profile_name().to_string();
    let failed = result.is_err();
    match &result {
        Ok(()) => {
            if promotes_to_success(&summary.status) {
                let (status, reason) = ok_outcome(
                    plan.skip_empty,
                    summary.total_rows,
                    summary.manifest_parts.len(),
                    &plan.strategy,
                );
                summary.status = status.into();
                if reason.is_some() {
                    summary.skip_reason = reason;
                    log::info!(
                        "export '{}': skipped (0 rows, skip_empty=true)",
                        plan.export_name
                    );
                }
            }
        }
        Err(e) => {
            summary.status = "failed".into();
            let redacted = crate::redact::redact_error(e);
            summary.error_message = Some(redacted.clone());
            log::error!("{} '{}' failed: {}", tail.kind, plan.export_name, redacted);
        }
    }

    if let Some(line) = run_diagnosis(
        &summary,
        &harm_delta_vec,
        super::run::multi_export_concurrent(),
    ) {
        log::warn!("{line}");
    }

    let mut reconcile_gate: crate::error::Result<()> = Ok(());
    if should_reconcile(tail.allow_reconcile, plan.reconcile, failed) {
        let could_not_verify = reconcile_source_count(plan, &mut summary, &mut meta);
        if let (Some(source_count), Some(matched)) = (summary.source_count, summary.reconciled) {
            summary.journal.record(RunEvent::ReconciliationResult {
                source_count,
                exported_rows: summary.total_rows,
                matched,
            });
        }
        reconcile_gate = reconcile_run_gate(&summary, could_not_verify.as_deref());
    }

    let finalized = finalize_manifest(plan, tail.family, state, &summary, tail.kind);
    let manifest_gap = finalized.gap;
    if let Some(why) = &manifest_gap {
        summary.status = "failed".into();
        summary.error_message = Some(crate::redact::redact_secrets(why));
    }

    summary.journal.record(RunEvent::RunCompleted {
        status: summary.status.clone(),
        error_message: summary.error_message.clone(),
        duration_ms: summary.duration_ms,
    });

    if let Err(e) = state.store_journal(&summary.journal) {
        log::warn!(
            "{} '{}': journal persist failed (run history not stored): {:#}",
            tail.kind,
            summary.export_name,
            e
        );
    }

    summary.print();
    ledger_finish_owned_runs(state, &plan.export_name, &ledger_run_id, &summary);
    if marker_outlived_its_run(finalized.left_alone, &ledger_run_id, &summary.run_id) {
        super::finalize::retire_running_marker(plan, &ledger_run_id);
    }
    let mut cursor_gate: crate::error::Result<()> = Ok(());
    if !cursor_may_advance(&summary.status, &manifest_gap) {
        if summary.cursor_high.is_some() {
            log::error!(
                "{} '{}': incremental cursor NOT advanced — the run did not succeed with a \
                 manifest, so the next run must re-export this window rather than skip past it",
                tail.kind,
                summary.export_name,
            );
        }
    } else if let Err(e) = commit_incremental_cursor(state, plan, &summary) {
        cursor_gate = Err(cursor_not_stored(tail.kind, &summary.export_name, &e));
    }
    let outcome = RunOutcome {
        failed,
        data_complete,
        manifest_gap: &manifest_gap,
    };
    if let Some(warning) = finalize_keyset_anchor(state, &progress, plan, &summary.run_id, &outcome)
    {
        log::warn!("{warning}");
    }
    if plan.validate {
        finalize_validate_manifest(plan, &mut summary, tail.kind);
    }
    if let Err(e) = state.record_metric_full(&build_metric_row(&summary, plan, &tuning_class)) {
        log::warn!(
            "{} '{}': metrics write failed (run outcome not stored): {:#}",
            tail.kind,
            summary.export_name,
            e
        );
    }
    finalize_run_report(tail.config_path, &summary, tail.kind);
    crate::notify::maybe_send(tail.notifications, &summary);

    let gates = [validate_run_gate(&summary), reconcile_gate, cursor_gate];
    let final_result = resolve_final_result(failed, result, manifest_gap, gates);
    (final_result, summary)
}

/// Run one export and, when it succeeds, record the load spec `rivet load` plans from.
pub(super) fn run_export_job(
    config_path: &str,
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_dir: &Path,
    opts: &RunOptions<'_>,
) -> (Result<()>, RunSummary) {
    let (result, summary) =
        run_export_job_inner(config_path, config, export, state, config_dir, opts);
    if result.is_ok() {
        super::load_spec::record_after_run(
            config,
            export,
            state,
            config_dir,
            opts.params,
            &summary,
        );
    }
    (result, summary)
}

/// Whether a CDC export's pending snapshot legs run on the pool.
fn snapshot_legs_fan_out(allowed: bool, legs: usize) -> bool {
    allowed && legs > 1
}

/// A CDC export's outcome under its own name: a failed leg stops it, else the stream runs.
fn cdc_outcome_with_legs(
    export_name: &str,
    legs: Vec<(Result<()>, RunSummary)>,
    stream: impl FnOnce() -> (Result<()>, RunSummary),
) -> (Result<()>, RunSummary) {
    let entries = legs
        .iter()
        .map(|(_, leg)| super::aggregate::entry_from_summary(leg))
        .collect();
    let failed_leg = legs
        .into_iter()
        .find_map(|(res, leg)| res.err().map(|e| (e, leg.run_id)));
    let (result, mut summary) = match failed_leg {
        Some((e, run_id)) => {
            let mut failed = synthetic_failed_summary(export_name, &e);
            failed.mode = "cdc".into();
            failed.run_id = run_id;
            (Err(e), failed)
        }
        None => stream(),
    };
    summary.snapshot_legs = entries;
    (result, summary)
}

/// Record one finished baseline snapshot in the state DB, right after it lands; best-effort.
fn record_snapshot_done(
    state: &StateStore,
    export_name: &str,
    synth: &ExportConfig,
    summary: &RunSummary,
) {
    if let Some(table) = synth.snapshot_label.as_deref().or(synth.table.as_deref())
        && let Err(e) = state.mark_snapshot_done(
            export_name,
            table,
            &synth.destination.state_key(),
            &summary.journal.run_id,
        )
    {
        log::warn!(
            "cdc: snapshot-completion persist failed for '{}' table '{}': {:#}",
            export_name,
            table,
            e
        );
    }
}

fn run_export_job_inner(
    config_path: &str,
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_dir: &Path,
    opts: &RunOptions<'_>,
) -> (Result<()>, RunSummary) {
    if dispatches_to_cdc_runner(export.mode) {
        if let Some(msg) = super::cdc_job::cdc_ignored_run_flags_warning(
            &export.name,
            opts.validate,
            opts.reconcile,
        ) {
            log::warn!("{msg}");
        }
        let _run_lease = match cdc_run_lease(state, &export.name, &config.source.state_key()) {
            Ok(lease) => lease,
            Err(e) => {
                let summary = synthetic_failed_summary(&export.name, &e);
                return (Err(e), summary);
            }
        };
        if let Err(e) = super::cdc_job::claim_checkpoint(config_path, export) {
            let summary = synthetic_failed_summary(&export.name, &e);
            return (Err(e), summary);
        }
        let cdc_config_dir = std::path::Path::new(config_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let grants = match super::cdc_job::resolve_grants(config, export) {
            Ok(g) => g,
            Err(e) => {
                let e = super::cdc_job::grants_unreadable_at_start(
                    e,
                    config,
                    export,
                    state,
                    config_path,
                );
                let summary = synthetic_failed_summary(&export.name, &e);
                crate::notify::maybe_send(config.notifications.as_ref(), &summary);
                return (Err(e), summary);
            }
        };
        let pending = match super::cdc_job::initial_snapshot_pending(
            config,
            export,
            state,
            cdc_config_dir,
            grants.as_ref(),
        ) {
            Ok(p) => p,
            Err(e) => {
                let summary = synthetic_failed_summary(&export.name, &e);
                crate::notify::maybe_send(config.notifications.as_ref(), &summary);
                return (Err(e), summary);
            }
        };
        if let Err(e) = super::cdc_job::refuse_lost_file_anchor(config, export, state, config_path)
        {
            let summary = synthetic_failed_summary(&export.name, &e);
            crate::notify::maybe_send(config.notifications.as_ref(), &summary);
            return (Err(e), summary);
        }
        let outcomes = if snapshot_legs_fan_out(opts.parallel_snapshots, pending.len()) {
            let _flags = super::run::RenderFlags::set(super::run::multi_export_mode(), Some(true));
            let (harm, _) = super::run::RunHarmBracket::open(&config.source);
            let legs: Vec<&ExportConfig> = pending.iter().collect();
            let (outcomes, workers) =
                super::run::run_export_pool(config_path, &legs, |synth, own| {
                    let outcome = run_export_job(config_path, config, synth, own, config_dir, opts);
                    if outcome.0.is_ok() {
                        record_snapshot_done(own, &export.name, synth, &outcome.1);
                    }
                    outcome
                });
            harm.close_and_warn(super::run::HarmWindow::Parallel { exports: workers });
            outcomes
        } else {
            let mut done = Vec::with_capacity(pending.len());
            for synth in &pending {
                let outcome = run_export_job(config_path, config, synth, state, config_dir, opts);
                if outcome.0.is_ok() {
                    record_snapshot_done(state, &export.name, synth, &outcome.1);
                }
                let stop = outcome.0.is_err();
                done.push(outcome);
                if stop {
                    break;
                }
            }
            done
        };
        let outcome = cdc_outcome_with_legs(&export.name, outcomes, || {
            let outcome = super::cdc_job::run_cdc_export(
                config_path,
                config,
                export,
                state,
                opts.strict,
                grants,
            );
            super::cdc_job::record_file_anchor(config, export, state, config_path, &outcome.0);
            outcome
        });
        outcome.1.emit_finished();
        return outcome;
    }
    let owned_export;
    let export = if crate::load::plan::resolved_layout(config, export, None).log_is_disposable()
        && crate::load::plan::resolved_deleted_flag(config, export, None)
    {
        let mut e = export.clone();
        e.meta_columns.deleted_flag = true;
        owned_export = e;
        &owned_export
    } else {
        export
    };
    let mut meta = MetaConn::open(&config.source);
    let probe = if config.source.columns_from_grants() {
        match meta.require() {
            Ok(src) => Some(src),
            Err(e) => {
                let summary = synthetic_failed_summary(&export.name, &e);
                return (Err(e), summary);
            }
        }
    } else {
        meta.get()
    };
    let plan = match crate::plan::build::build_plan_on(
        probe.map(|s| s as &mut dyn crate::source::Source),
        config,
        export,
        config_dir,
        opts.validate,
        opts.reconcile,
        opts.resume,
        opts.params,
    ) {
        Ok(p) => p,
        Err(e) => {
            let e = crate::source::code_a_missing_relation(
                e,
                &export.name,
                export.table.as_deref(),
                export.query.as_deref(),
            );
            let summary = synthetic_failed_summary(&export.name, &e);
            return (Err(e), summary);
        }
    };

    let diags = validate_plan(&plan);
    if let Some(err) = log_plan_diagnostics(&plan.export_name, &diags) {
        let summary = synthetic_failed_summary(&export.name, &err);
        return (Err(err), summary);
    }

    if resume_success_gate_applies(opts.resume, opts.force)
        && let Err(e) = check_success_gate_for_resume(&plan, state)
    {
        let summary = synthetic_failed_summary(&export.name, &e);
        return (Err(e), summary);
    }

    if rerun_warning_applies(opts.resume, opts.force) {
        warn_if_prefix_has_completed_run(&plan);
    }

    log::info!(
        "starting export '{}' (effective tuning: {})",
        plan.export_name,
        plan.tuning
    );

    let plan_warnings: Vec<(String, String)> = diags
        .iter()
        .filter(|d| {
            matches!(
                d.level,
                DiagnosticLevel::Warning | DiagnosticLevel::Degraded
            )
        })
        .map(|d| (d.rule.to_string(), d.message.clone()))
        .collect();
    let family = export.family();
    execute_resolved_plan(
        &plan,
        state,
        TailPolicy {
            kind: "export",
            family: &family,
            config_path,
            runner_config_path: config_path,
            chunk_source: chunked::ChunkSource::Detect,
            apply_context: None,
            allow_reconcile: true,
            notifications: config.notifications.as_ref(),
            record_load_spec: true,
            plan_warnings,
            strict: opts.strict,
            force: opts.force,
            cdc_leg: export.snapshot_parent.is_some(),
        },
        meta,
    )
}

use super::finalize::{
    check_success_gate_for_resume, finalize_manifest, finalize_run_report,
    finalize_validate_manifest, warn_if_prefix_has_completed_run,
};

/// Execute a pre-resolved plan with a caller-supplied `ChunkSource`.
pub(crate) fn run_export_job_with_chunk_source(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    chunk_source: chunked::ChunkSource,
    config_path: &str,
    apply_context: Option<crate::pipeline::summary::ApplyContext>,
    record_load_spec: bool,
) -> (Result<()>, RunSummary) {
    if let Some(err) = log_plan_diagnostics(&plan.export_name, &validate_plan(plan)) {
        let summary = synthetic_failed_summary(&plan.export_name, &err);
        return (Err(err), summary);
    }

    log::info!(
        "apply: starting export '{}' (tuning: {})",
        plan.export_name,
        plan.tuning
    );

    execute_resolved_plan(
        plan,
        state,
        TailPolicy {
            kind: "apply",
            family: &plan.export_name,
            config_path,
            runner_config_path: "",
            chunk_source,
            apply_context,
            allow_reconcile: false,
            notifications: None,
            record_load_spec,
            plan_warnings: Vec::new(),
            strict: false,
            force: false,
            cdc_leg: false,
        },
        MetaConn::open(&plan.source),
    )
}

/// Why `plan` cannot be executed when its destination still holds the `{partition}` token.
fn unexpanded_partition_error(plan: &ResolvedRunPlan) -> Option<anyhow::Error> {
    let dest = &plan.destination;
    [dest.path.as_deref(), dest.prefix.as_deref()]
        .into_iter()
        .flatten()
        .any(|s| s.contains("{partition}"))
        .then(|| {
            crate::error::coded(
                crate::error::codes::PLAN_PARTITION_TOKEN_UNEXPANDED,
                format!(
                    "export '{}': the destination still holds the `{{partition}}` token: a \
                 `partition_by` export is expanded into one export per partition when the config \
                 runs, and this plan was built without that expansion — a sealed `rivet plan` \
                 artifact cannot carry it, and a destination that names `{{partition}}` without \
                 `partition_by:` has nothing to expand. Run the config instead: `rivet apply \
                 <config.yaml>` or `rivet run --config <config.yaml>`; without `partition_by:`, \
                 remove the token.",
                    plan.export_name
                ),
            )
        })
}

#[cfg(test)]
mod snapshot_leg_tests {
    use super::*;

    /// A plan with the `{partition}` token left in its path or prefix is refused before any state is written.
    #[test]
    fn a_plan_with_an_unexpanded_partition_token_is_refused_before_any_write() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let config = dir.path().join("rivet.yaml").to_string_lossy().into_owned();
        let expanded = crate::plan::test_support::test_plan();
        assert!(unexpanded_partition_error(&expanded).is_none());
        let mut unexpanded = expanded.clone();
        unexpanded.destination.prefix = Some("x/{partition}".into());
        let err = unexpanded_partition_error(&unexpanded).unwrap();
        crate::error::assert_coded(&err, "RIVET_PLAN_PARTITION_TOKEN_UNEXPANDED", 1);
        for in_prefix in [false, true] {
            let mut plan = expanded.clone();
            let template = format!("{}/{{partition}}", dir.path().display());
            if in_prefix {
                plan.destination.prefix = Some(template);
            } else {
                plan.destination.path = Some(template);
            }
            let (result, summary) = execute_resolved_plan(
                &plan,
                &state,
                TailPolicy {
                    kind: "apply",
                    family: "orders",
                    config_path: &config,
                    runner_config_path: "",
                    chunk_source: chunked::ChunkSource::Detect,
                    apply_context: None,
                    allow_reconcile: false,
                    notifications: None,
                    record_load_spec: false,
                    plan_warnings: Vec::new(),
                    strict: false,
                    force: false,
                    cdc_leg: false,
                },
                MetaConn::open(&plan.source),
            );
            assert_eq!(
                result.unwrap_err().to_string(),
                "export 'orders': the destination still holds the `{partition}` token: a \
                 `partition_by` export is expanded into one export per partition when the config \
                 runs, and this plan was built without that expansion — a sealed `rivet plan` \
                 artifact cannot carry it, and a destination that names `{partition}` without \
                 `partition_by:` has nothing to expand. Run the config instead: `rivet apply \
                 <config.yaml>` or `rivet run --config <config.yaml>`; without `partition_by:`, \
                 remove the token."
            );
            assert_eq!(summary.status, "failed");
            assert!(
                !dir.path().join("{partition}").exists(),
                "a refused plan creates nothing at the destination"
            );
        }
    }

    /// A run whose source cannot be reached ends FAILED.
    #[test]
    fn an_unreachable_source_fails_the_run_and_its_summary() {
        let dir = tempfile::tempdir().unwrap();
        let mut plan = crate::plan::test_support::test_plan();
        plan.destination.path = Some(dir.path().to_string_lossy().into_owned());
        let state = StateStore::open_in_memory().unwrap();
        let config = dir.path().join("rivet.yaml").to_string_lossy().into_owned();
        let (result, summary) = execute_resolved_plan(
            &plan,
            &state,
            TailPolicy {
                kind: "export",
                family: "orders",
                config_path: &config,
                runner_config_path: &config,
                chunk_source: chunked::ChunkSource::Detect,
                apply_context: None,
                allow_reconcile: false,
                notifications: None,
                record_load_spec: false,
                plan_warnings: Vec::new(),
                strict: false,
                force: false,
                cdc_leg: false,
            },
            MetaConn::open(&plan.source),
        );
        assert!(result.is_err(), "nothing listens on 127.0.0.1:9999");
        assert_eq!(summary.status, "failed");
        assert_eq!(summary.export_name, "orders");
    }

    fn cdc_export() -> crate::config::ExportConfig {
        let cfg = crate::config::Config::from_yaml(
            "source:\n  type: mysql\n  url: \"mysql://localhost/test\"\n\
             exports:\n  - name: cdc\n    mode: cdc\n    tables: [orders, items]\n\
             \x20   format: parquet\n    cdc:\n      checkpoint: /tmp/ck\n\
             \x20   destination:\n      type: local\n      path: ./out\n",
        )
        .expect("a two-table CDC config loads");
        cfg.exports[0].clone()
    }

    /// Snapshot legs go to the pool only when this run allows it AND there is more than one.
    #[test]
    fn snapshot_legs_fan_out_needs_permission_and_more_than_one_leg() {
        assert!(snapshot_legs_fan_out(true, 2));
        assert!(
            !snapshot_legs_fan_out(true, 1),
            "one leg gains nothing from a pool"
        );
        assert!(
            !snapshot_legs_fan_out(false, 5),
            "not allowed, never pooled"
        );
    }

    /// One finished run of `name` that wrote `rows` rows in `files` parts.
    fn wrote(name: &str, status: &str, rows: i64, files: usize) -> (Result<()>, RunSummary) {
        let summary = RunSummary {
            run_id: format!("{name}_run"),
            export_name: name.into(),
            status: status.into(),
            total_rows: rows,
            files_produced: files,
            bytes_written: rows as u64 * 10,
            bytes_read: rows as u64 * 20,
            mode: "cdc".into(),
            ..Default::default()
        };
        let result = if status == "failed" {
            Err(anyhow::anyhow!("{name} broke"))
        } else {
            Ok(())
        };
        (result, summary)
    }

    /// The CDC export's entry counts its legs and its stream.
    #[test]
    fn a_cdc_entry_counts_its_snapshot_legs_and_its_stream() {
        let legs = vec![
            wrote("pg_cdc__snapshot_a", "success", 1000, 1),
            wrote("pg_cdc__snapshot_b", "success", 5000, 1),
        ];
        let (result, summary) =
            cdc_outcome_with_legs("pg_cdc", legs, || wrote("pg_cdc", "success", 3, 1));
        assert!(result.is_ok());
        assert_eq!((summary.total_rows, summary.files_produced), (3, 1));
        let e = super::super::aggregate::entry_from_summary(&summary);
        assert_eq!(
            (
                e.export_name.as_str(),
                e.status.as_str(),
                e.rows,
                e.files,
                e.bytes,
                e.bytes_read
            ),
            ("pg_cdc", "success", 6003, 3, 60_030, 120_060)
        );
    }

    /// A failed leg fails the CDC export under its own name, with every leg's rows.
    #[test]
    fn a_failed_snapshot_leg_fails_the_cdc_entry_under_its_own_name() {
        let legs = vec![
            wrote("pg_cdc__snapshot_a", "success", 1000, 1),
            wrote("pg_cdc__snapshot_b", "failed", 40, 1),
        ];
        let (result, summary) = cdc_outcome_with_legs("pg_cdc", legs, || {
            panic!("the stream must not run after a failed leg")
        });
        assert_eq!(
            format!("{:#}", result.expect_err("the export fails")),
            "pg_cdc__snapshot_b broke"
        );
        let e = super::super::aggregate::entry_from_summary(&summary);
        assert_eq!(
            (
                e.export_name.as_str(),
                e.status.as_str(),
                e.mode.as_str(),
                e.rows,
                e.files
            ),
            ("pg_cdc", "failed", "cdc", 1040, 2)
        );
        assert_eq!(e.error_message.as_deref(), Some("pg_cdc__snapshot_b broke"));
        assert_eq!(
            e.run_id, "pg_cdc__snapshot_b_run",
            "the run id names the failed leg's own metric row"
        );
    }

    /// Without legs the stream's outcome is the export's, unchanged.
    #[test]
    fn a_cdc_run_without_legs_is_its_stream() {
        let (result, summary) =
            cdc_outcome_with_legs("pg_cdc", Vec::new(), || wrote("pg_cdc", "success", 7, 2));
        assert!(result.is_ok());
        let e = super::super::aggregate::entry_from_summary(&summary);
        assert_eq!((e.rows, e.files, e.bytes, e.bytes_read), (7, 2, 70, 140));
    }

    /// A finished leg is recorded under the key the next run's snapshot plan asks for.
    #[test]
    fn a_finished_snapshot_leg_is_recorded_under_the_key_the_plan_reads() {
        let export = cdc_export();
        let leg =
            crate::pipeline::cdc_job::synth_snapshot_export_for_test(&export, "orders", "orders");
        let state = StateStore::open_in_memory().unwrap();
        let key = leg.destination.state_key();
        assert!(
            !state.snapshot_done("cdc", "orders", &key).unwrap(),
            "fixture starts undone"
        );
        let mut summary = RunSummary::default();
        summary.journal.run_id = "leg-run".into();
        record_snapshot_done(&state, "cdc", &leg, &summary);
        assert!(
            state.snapshot_done("cdc", "orders", &key).unwrap(),
            "the leg must be marked done where snapshot_plan looks"
        );
        assert!(
            !state.snapshot_done("cdc", "items", &key).unwrap(),
            "only the leg that finished"
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::plan::test_support::test_plan;

    /// A resume that ADOPTS a prior run's id must still close the row the ledger was opened under — both wrappers, through one seam.
    #[test]
    fn an_adopted_run_id_does_not_strand_the_row_the_ledger_opened() {
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let export = "orders";
        let opened_as = "run_new";
        let adopted = "run_crashed";

        let prefix = "file:///out";
        for rid in [adopted, opened_as] {
            state
                .begin_run(rid, export, prefix, "2026-01-01T00:00:00Z")
                .expect("open a run-status row");
        }
        assert_eq!(
            state.active_run_ids_on_prefix(prefix).unwrap().len(),
            2,
            "fixture is inert — two open rows are the whole point"
        );

        let summary = crate::pipeline::summary::RunSummary {
            run_id: adopted.into(),
            export_name: export.into(),
            status: "success".into(),
            ..Default::default()
        };
        ledger_finish_owned_runs(&state, export, opened_as, &summary);

        let still_active = state.active_run_ids_on_prefix(prefix).unwrap();
        assert!(
            still_active.is_empty(),
            "these run(s) are still marked active after the run ended: {still_active:?} — a \
             permanently running row makes gc_orphans defer this prefix forever, with no later \
             run to supersede it"
        );
    }

    /// The keyset resume anchor must outlive a run that did not finish.
    #[test]
    fn a_failed_run_holds_the_incremental_cursor_even_when_its_manifest_landed() {
        let gap = Some("the manifest write FAILED".to_string());
        assert!(cursor_may_advance("success", &None));
        assert!(
            !cursor_may_advance("failed", &None),
            "a gate that fails AFTER the write (on_schema_drift: fail, quality) leaves only a \
             Failed manifest — advancing past its rows loses them"
        );
        assert!(!cursor_may_advance("success", &gap));
        assert!(!cursor_may_advance("failed", &gap));
    }

    /// A run that broke off mid-data.
    fn outcome(failed: bool, manifest_gap: &Option<String>) -> RunOutcome<'_> {
        RunOutcome {
            failed,
            data_complete: !failed,
            manifest_gap,
        }
    }

    #[test]
    fn a_run_whose_manifest_did_not_land_keeps_its_keyset_resume_anchor() {
        let gap = Some("the manifest write FAILED".to_string());

        assert!(
            keyset_anchor_survives(&outcome(false, &gap), false),
            "parts are durable and nothing names them — the anchor is the only way back to them"
        );
        assert!(
            keyset_anchor_survives(&outcome(true, &None), false),
            "an export failure keeps its anchor, as it always did"
        );
        assert!(
            keyset_anchor_survives(&outcome(true, &gap), false),
            "both at once is still a run that did not finish"
        );
        assert!(
            !keyset_anchor_survives(&outcome(false, &None), false),
            "a genuinely complete run MUST clear it, or the next run is misread as a resume of \
             this finished one and reuses its frozen run_id"
        );
        assert!(
            !keyset_anchor_survives(&outcome(false, &None), true),
            "a finished run's cursor sits on its own last page: that is not a refusal"
        );
    }

    /// The runner read every row and a gate then refused the result.
    fn refused(manifest_gap: &Option<String>) -> RunOutcome<'_> {
        RunOutcome {
            failed: true,
            data_complete: true,
            manifest_gap,
        }
    }

    /// A refusal must not become the evidence that lifts it.
    #[test]
    fn a_refused_run_releases_its_keyset_anchor_so_the_next_run_is_graded_again() {
        let gap = Some("the manifest write FAILED".to_string());
        for manifest_gap in [&None, &gap] {
            assert!(
                !keyset_anchor_survives(&refused(manifest_gap), false),
                "a refused run's pages are never adopted, with or without its Failed manifest"
            );
            assert!(
                keyset_anchor_survives(&refused(manifest_gap), true),
                "a cursor that ran ahead would skip the refused pages on a re-read"
            );
        }
        assert!(refused_by_a_gate(&refused(&None)));
        assert!(
            !refused_by_a_gate(&outcome(true, &None)),
            "a run that broke off mid-data is resumed, not re-read"
        );
        assert!(
            !refused_by_a_gate(&outcome(false, &None)),
            "a run that passed its gates was not refused"
        );
    }

    fn keyset_plan(incremental: bool) -> ResolvedRunPlan {
        let mut plan = chunked_plan_with_quality(None);
        plan.strategy = ExtractionStrategy::Keyset(crate::plan::KeysetPlan {
            key_column: "id".into(),
            chunk_size: 3,
            checkpoint: true,
            incremental,
            parallel: 1,
        });
        plan
    }

    /// A state store holding run `r1`'s anchor, one range and one committed page ending at key 3.
    fn anchored_state(plan: &ResolvedRunPlan) -> StateStore {
        let st = StateStore::open_in_memory().unwrap();
        let key = plan.progress_key();
        st.set_resume_run_id(&key, "r1").unwrap();
        st.persist_keyset_ranges("orders", &key.source, "r1", "id", &[(None, None)])
            .unwrap();
        st.record_file(crate::state::FilePart {
            run_id: "r1",
            export_name: "orders",
            file_name: "p0",
            rows: 3,
            bytes: 1,
            format: "parquet",
            compression: None,
            cursor_high: Some("3"),
        })
        .unwrap();
        st
    }

    /// The claim a run of `plan` holds on `st`.
    fn claimed<'s>(st: &'s StateStore, plan: &ResolvedRunPlan) -> ProgressClaim<'s> {
        st.claim(plan.progress_key()).unwrap()
    }

    #[test]
    fn a_plan_without_its_checkpoint_is_refused_beside_the_run_its_checkpointed_twin_left() {
        let checkpointed = keyset_plan(false);
        let mut plain = keyset_plan(false);
        if let ExtractionStrategy::Keyset(k) = &mut plain.strategy {
            k.checkpoint = false;
        }
        let st = anchored_state(&checkpointed);
        let refused = st.claim(plain.progress_key()).err().expect("refused");
        assert_eq!(
            crate::error::error_code(&refused),
            Some("RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH"),
            "{refused:#}"
        );
        assert!(st.claim(checkpointed.progress_key()).is_ok());
    }

    /// `(anchor, has ranges)` left for `orders`.
    fn anchor_left(st: &StateStore, plan: &ResolvedRunPlan) -> (Option<String>, bool) {
        (
            st.get_resume_run_id("orders", &plan.source.state_key())
                .unwrap(),
            st.has_keyset_ranges("orders", "r1", None).unwrap(),
        )
    }

    #[test]
    fn a_refused_or_finished_run_releases_anchor_and_ranges_and_a_broken_off_one_keeps_both() {
        let plan = keyset_plan(true);
        let kept = (Some("r1".to_string()), true);

        let st = anchored_state(&plan);
        assert_eq!(
            finalize_keyset_anchor(
                &st,
                &claimed(&st, &plan),
                &plan,
                "r1",
                &outcome(true, &None)
            ),
            None
        );
        assert_eq!(anchor_left(&st, &plan), kept, "broke off: resumed next run");

        assert_eq!(
            finalize_keyset_anchor(&st, &claimed(&st, &plan), &plan, "r1", &refused(&None)),
            None
        );
        assert_eq!(anchor_left(&st, &plan), (None, false), "refused: released");

        let st = anchored_state(&plan);
        assert_eq!(
            finalize_keyset_anchor(
                &st,
                &claimed(&st, &plan),
                &plan,
                "r1",
                &outcome(false, &None)
            ),
            None
        );
        assert_eq!(anchor_left(&st, &plan), (None, false), "finished: released");

        let st = anchored_state(&plan);
        let chunked = chunked_plan_with_quality(None);
        assert_eq!(
            finalize_keyset_anchor(&st, &claimed(&st, &plan), &chunked, "r1", &refused(&None)),
            None
        );
        assert_eq!(
            anchor_left(&st, &plan),
            kept,
            "not a keyset export: not its anchor"
        );
    }

    #[test]
    fn a_run_that_broke_off_before_its_first_part_releases_its_anchor() {
        let plan = keyset_plan(true);
        let st = StateStore::open_in_memory().unwrap();
        let key = plan.progress_key();
        st.set_resume_run_id(&key, "r1").unwrap();
        st.persist_keyset_ranges("orders", &key.source, "r1", "id", &[(None, None)])
            .unwrap();
        assert_eq!(
            finalize_keyset_anchor(
                &st,
                &claimed(&st, &plan),
                &plan,
                "r1",
                &outcome(true, &None)
            ),
            None
        );
        assert_eq!(
            anchor_left(&st, &plan),
            (None, false),
            "no part: nothing to resume"
        );

        st.set_resume_run_id(&key, "r1").unwrap();
        assert_eq!(
            finalize_keyset_anchor(
                &st,
                &claimed(&st, &plan),
                &plan,
                "r2",
                &outcome(true, &None)
            ),
            None
        );
        assert_eq!(
            anchor_left(&st, &plan),
            (None, false),
            "stopped before adopting an anchor with no part: nothing to resume"
        );
    }

    #[test]
    fn a_run_stopped_before_adopting_an_anchor_with_parts_keeps_it() {
        let plan = keyset_plan(true);
        let st = anchored_state(&plan);
        assert_eq!(
            finalize_keyset_anchor(
                &st,
                &claimed(&st, &plan),
                &plan,
                "r2",
                &outcome(true, &None)
            ),
            None
        );
        assert_eq!(
            anchor_left(&st, &plan),
            (Some("r1".to_string()), true),
            "r1's committed parts are still to be resumed"
        );
    }

    #[test]
    fn a_refused_run_whose_cursor_ran_ahead_keeps_its_anchor_and_says_so() {
        let plan = keyset_plan(true);
        let kept = (Some("r1".to_string()), true);
        let st = anchored_state(&plan);
        st.update_with_column(&plan.progress_key(), "3").unwrap();

        assert_eq!(
            finalize_keyset_anchor(&st, &claimed(&st, &plan), &plan, "r1", &refused(&None))
                .as_deref(),
            Some(
                "export 'orders': refused run r1 cannot be read again — the stored cursor already \
                 sits past pages it committed (a run interrupted under rivet 0.31 or earlier, or \
                 one stopped right after its cursor moved), so the next run adopts those pages \
                 without grading them. To have every row read and graded again, run `rivet state \
                 reset -c <config> --export orders` before the next run"
            )
        );
        assert_eq!(anchor_left(&st, &plan), kept);

        assert_eq!(
            finalize_keyset_anchor(
                &st,
                &claimed(&st, &plan),
                &plan,
                "r1",
                &outcome(true, &None)
            ),
            None,
            "a run that broke off is resumed whatever its cursor: nothing to warn about"
        );
        assert_eq!(anchor_left(&st, &plan), kept);

        assert_eq!(
            finalize_keyset_anchor(
                &st,
                &claimed(&st, &plan),
                &plan,
                "r1",
                &outcome(false, &None)
            ),
            None,
            "a finished run's cursor is on its own last page by design"
        );
        assert_eq!(anchor_left(&st, &plan), (None, false));
    }

    #[test]
    fn only_an_incremental_cursor_on_the_runs_own_page_ran_ahead() {
        let (inc, full) = (keyset_plan(true), keyset_plan(false));
        let st = anchored_state(&inc);
        let key = inc.progress_key();
        let claim = st.claim(key.clone()).unwrap();
        assert!(
            !cursor_ran_ahead(&claim, &inc, "r1"),
            "no cursor stored yet"
        );

        st.update_with_column(&key, "3").unwrap();
        assert!(
            cursor_ran_ahead(&claim, &inc, "r1"),
            "the cursor is r1's page"
        );
        assert!(
            !cursor_ran_ahead(&claim, &inc, "r2"),
            "another run's page: this run began there"
        );
        assert!(
            !cursor_ran_ahead(&claim, &full, "r1"),
            "a full pass releases its anchor itself and re-reads from the start"
        );
        assert!(
            !cursor_ran_ahead(&claim, &chunked_plan_with_quality(None), "r1"),
            "not a keyset export"
        );

        st.update_with_column(&key, "0").unwrap();
        assert!(
            !cursor_ran_ahead(&claim, &inc, "r1"),
            "the cursor is still where r1 began"
        );
    }

    use super::*;

    #[test]
    fn run_reconcile_gate_fails_on_mismatch_matches_subcommand() {
        let mut s = RunSummary {
            export_name: "orders".into(),
            source_count: Some(1033),
            reconciled: Some(false),
            ..Default::default()
        };
        let err = reconcile_run_gate(&s, None).unwrap_err();
        assert!(
            err.downcast_ref::<crate::error::DataIntegrityError>()
                .is_some(),
            "a reconcile mismatch must carry the data-integrity marker"
        );
        assert_eq!(
            crate::error::classify_exit(&err),
            3,
            "a reconcile mismatch must classify as exit 3"
        );

        s.reconciled = Some(true);
        assert!(reconcile_run_gate(&s, None).is_ok());
        s.reconciled = None;
        assert!(reconcile_run_gate(&s, None).is_ok());

        let err = reconcile_run_gate(&s, Some("could not connect to the source"))
            .expect_err("a could-not-verify reconcile must gate non-zero");
        assert!(
            err.downcast_ref::<crate::error::DataIntegrityError>()
                .is_none(),
            "could-not-verify must NOT be data-integrity (exit 3)"
        );
        assert_eq!(
            crate::error::classify_exit(&err),
            1,
            "a could-not-verify reconcile must classify as operational exit 1"
        );
        s.reconciled = Some(false);
        let err = reconcile_run_gate(&s, Some("noise")).unwrap_err();
        assert_eq!(crate::error::classify_exit(&err), 3);
    }

    /// The reconcile gate's full truth table.
    #[test]
    fn plan_rejection_error_refuses_only_a_rejected_plan_and_names_every_reason() {
        use super::plan_rejection_error;
        assert!(
            plan_rejection_error("orders", &[]).is_none(),
            "a plan with no REJECTED diagnostics must run"
        );
        let err = plan_rejection_error(
            "orders",
            &[
                "chunk key is nullable".to_string(),
                "no primary key".to_string(),
            ],
        )
        .expect("a rejected plan must not run");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("export 'orders'"),
            "names the export; got {msg}"
        );
        assert!(
            msg.contains("chunk key is nullable") && msg.contains("no primary key"),
            "every rejection reason must reach the operator; got {msg}"
        );
    }

    #[test]
    fn log_plan_diagnostics_refuses_on_every_rejection_and_passes_warnings() {
        use super::log_plan_diagnostics;
        use crate::plan::DiagnosticLevel;
        use crate::plan::validate::Diagnostic;
        let d = |level, message: &str| Diagnostic {
            level,
            rule: "r",
            message: message.into(),
        };
        let warn_only = [
            d(DiagnosticLevel::Warning, "w"),
            d(DiagnosticLevel::Degraded, "g"),
        ];
        assert!(log_plan_diagnostics("orders", &warn_only).is_none());
        let rejected = [
            d(DiagnosticLevel::Rejected, "first"),
            d(DiagnosticLevel::Warning, "w"),
            d(DiagnosticLevel::Rejected, "second"),
        ];
        let msg = format!("{:#}", log_plan_diagnostics("orders", &rejected).unwrap());
        assert!(msg.contains("first") && msg.contains("second") && !msg.contains("w\n"));
    }

    /// A successful run promotes ONLY the transient status it was built with.
    #[test]
    fn only_a_still_running_status_is_promoted_to_success() {
        use super::promotes_to_success;
        assert!(
            promotes_to_success("running"),
            "the transient status promotes"
        );
        for terminal in ["skipped", "failed", "success", "interrupted", ""] {
            assert!(
                !promotes_to_success(terminal),
                "`{terminal}` is a decided status and must not be overwritten"
            );
        }
    }

    #[test]
    fn skip_empty_skips_only_a_run_that_delivered_nothing() {
        use super::ok_status;
        assert_eq!(ok_status(true, 0, 0), "skipped");
        assert_eq!(
            ok_status(false, 0, 0),
            "success",
            "skip_empty off never skips"
        );
        assert_eq!(ok_status(true, 5, 1), "success", "rows were delivered");
        assert_eq!(
            ok_status(true, 0, 2),
            "success",
            "a resume re-adopted parts"
        );
        assert_eq!(
            ok_status(true, 3, 0),
            "success",
            "rows counted, no parts yet"
        );
    }

    #[test]
    fn only_a_skipped_run_carries_a_skip_reason() {
        use super::ok_outcome;
        let full = ExtractionStrategy::Snapshot;
        assert_eq!(
            ok_outcome(true, 0, 0, &full),
            ("skipped", Some("source returned 0 rows".to_string()))
        );
        assert_eq!(ok_outcome(true, 5, 1, &full), ("success", None));
        assert_eq!(ok_outcome(false, 0, 0, &full), ("success", None));
    }

    #[test]
    fn skip_reason_names_the_cursor_when_there_is_one() {
        use super::skip_reason;
        assert_eq!(
            skip_reason(Some("updated_at")),
            "no new rows since cursor 'updated_at'"
        );
        assert_eq!(skip_reason(None), "source returned 0 rows");
    }

    #[test]
    fn a_marker_outlives_a_run_that_left_the_prefix_alone_or_a_resume_under_another_id() {
        use super::marker_outlived_its_run;
        assert!(
            marker_outlived_its_run(true, "r2", "r2"),
            "no terminal manifest"
        );
        assert!(
            marker_outlived_its_run(false, "r2", "r1"),
            "resume wrote under r1"
        );
        assert!(
            !marker_outlived_its_run(false, "r2", "r2"),
            "terminal replaced it"
        );
    }

    #[test]
    fn a_keyset_incremental_run_waits_on_its_key_a_full_keyset_on_nothing() {
        use super::awaited_column;
        use crate::config::IncrementalCursorMode;
        use crate::plan::{ExtractionStrategy, IncrementalCursorPlan, KeysetPlan};
        let keyset = |incremental| {
            ExtractionStrategy::Keyset(KeysetPlan {
                key_column: "id".into(),
                chunk_size: 10,
                checkpoint: true,
                incremental,
                parallel: 1,
            })
        };
        let incremental = ExtractionStrategy::Incremental(IncrementalCursorPlan {
            primary_column: "updated_at".into(),
            fallback_column: None,
            mode: IncrementalCursorMode::SingleColumn,
            settle: None,
        });
        assert_eq!(
            awaited_column(&incremental),
            Some("updated_at"),
            "the cursor it waited on"
        );
        assert_eq!(awaited_column(&keyset(true)), Some("id"));
        assert_eq!(
            awaited_column(&keyset(false)),
            None,
            "a full keyset pass awaits nothing"
        );
        assert_eq!(awaited_column(&ExtractionStrategy::Snapshot), None);
    }

    /// The resume/force policy as ONE truth table.
    #[test]
    fn resume_and_rerun_gates_are_opposite_halves_disabled_by_force() {
        use super::{rerun_warning_applies, resume_success_gate_applies};
        assert!(resume_success_gate_applies(true, false));
        assert!(!rerun_warning_applies(true, false));
        assert!(!resume_success_gate_applies(false, false));
        assert!(rerun_warning_applies(false, false));
        for resume in [true, false] {
            assert!(
                !resume_success_gate_applies(resume, true),
                "--force must disable the resume refusal (resume={resume})"
            );
            assert!(
                !rerun_warning_applies(resume, true),
                "--force must disable the rerun warning (resume={resume})"
            );
        }
    }

    /// The CDC dispatch fork.
    #[test]
    fn only_cdc_mode_dispatches_to_the_cdc_runner() {
        use super::dispatches_to_cdc_runner;
        use crate::config::ExportMode;
        assert!(dispatches_to_cdc_runner(ExportMode::Cdc));
        for m in [ExportMode::Full, ExportMode::Incremental] {
            assert!(
                !dispatches_to_cdc_runner(m),
                "{m:?} is a BATCH mode and must not reach the CDC runner"
            );
        }
    }

    #[test]
    fn should_reconcile_requires_permission_request_and_success() {
        use super::should_reconcile;
        assert!(should_reconcile(true, true, false));

        assert!(
            !should_reconcile(false, true, false),
            "apply does not allow a reconcile leg — a sealed artifact has no flag"
        );
        assert!(
            !should_reconcile(true, false, false),
            "the plan did not ask for one"
        );
        assert!(
            !should_reconcile(true, true, true),
            "a FAILED export must not reconcile: comparing a partial write against \
             the source reports a mismatch that is not one"
        );

        for (a, r, f) in [
            (false, false, false),
            (false, false, true),
            (false, true, true),
            (true, false, true),
        ] {
            assert!(
                !should_reconcile(a, r, f),
                "({a},{r},{f}) must not reconcile"
            );
        }
    }

    /// Three green gates.
    fn green() -> [crate::error::Result<()>; 3] {
        [Ok(()), Ok(()), Ok(())]
    }

    #[test]
    fn resolve_final_result_surfaces_reconcile_mismatch_when_export_succeeded() {
        use crate::error::DataIntegrityError;
        let gate: crate::error::Result<()> = Err(DataIntegrityError::new("mismatch").into());
        let out = resolve_final_result(false, Ok(()), None, [Ok(()), gate, Ok(())]);
        assert!(
            out.is_err(),
            "a reconcile mismatch on a successful export must surface as the run result"
        );
        assert_eq!(crate::error::classify_exit(&out.unwrap_err()), 3);

        let qfail: crate::error::Result<()> = Err(DataIntegrityError::new("quality").into());
        let gate: crate::error::Result<()> = Err(anyhow::anyhow!("a gate"));
        let out = resolve_final_result(true, qfail, None, [gate, Ok(()), Ok(())]).unwrap_err();
        assert_eq!(out.to_string(), "quality");

        assert!(resolve_final_result(false, Ok(()), None, green()).is_ok());
        let gap = resolve_final_result(false, Ok(()), Some("no manifest".into()), green());
        assert!(
            gap.is_err(),
            "an unwritten manifest must fail the run — reporting success there is a claim the \
             artifacts do not support"
        );
    }

    #[test]
    fn every_gate_fails_the_run_and_the_first_one_names_it() {
        let gate = |n: usize| -> [crate::error::Result<()>; 3] {
            let mut gates = green();
            gates[n] = Err(anyhow::anyhow!("gate {n}"));
            gates
        };
        for n in 0..3 {
            let err = resolve_final_result(false, Ok(()), None, gate(n)).unwrap_err();
            assert_eq!(err.to_string(), format!("gate {n}"));
        }
        let all = [
            Err(anyhow::anyhow!("validate")),
            Err(anyhow::anyhow!("reconcile")),
            Err(anyhow::anyhow!("cursor")),
        ];
        let err = resolve_final_result(false, Ok(()), None, all).unwrap_err();
        assert_eq!(err.to_string(), "validate");
        let gap = resolve_final_result(false, Ok(()), Some("no manifest".into()), gate(0));
        assert_eq!(gap.unwrap_err().to_string(), "no manifest");
    }

    /// A verdict holding `failures`, as the manifest pass leaves it on the summary.
    fn verdict(
        failures: Vec<crate::pipeline::validate_manifest::Failure>,
    ) -> crate::pipeline::validate_manifest::ManifestVerification {
        crate::pipeline::validate_manifest::ManifestVerification {
            passed: false,
            manifest_found: true,
            legacy_run: false,
            parts_verified: 1,
            parts_md5_verified: 0,
            parts_failed: 0,
            success_marker_consistent: true,
            manifest_self_consistent: true,
            failures,
            depth_level: "full".into(),
        }
    }

    #[test]
    fn run_validate_gate_gives_the_exit_rivet_validate_gives() {
        use crate::pipeline::validate_manifest::Failure;
        let mut s = RunSummary {
            export_name: "orders".into(),
            ..Default::default()
        };
        assert!(validate_run_gate(&s).is_ok(), "no `--validate`, no gate");
        s.validated = Some(true);
        assert!(validate_run_gate(&s).is_ok(), "a pass is not a failure");

        s.validated = Some(false);
        s.manifest_verification = Some(verdict(vec![
            Failure::UntrackedObject {
                key: "stray.parquet".into(),
                size_bytes: 1,
            },
            Failure::ContentVerificationUnmet {
                size_only: 1,
                total: 1,
            },
        ]));
        let err = validate_run_gate(&s).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_VALIDATE_FAILED")
        );
        assert_eq!(crate::error::classify_exit(&err), 3);
        assert_eq!(
            err.to_string(),
            "`--validate` did not pass for 'orders': verify: content not met — 1 of 1 part(s) only \
             size-verified (no store checksum); lower max_file_size so parts upload as a single \
             PUT, or the backend exposes no checksum. The export completed and its manifest is \
             written; `rivet validate` re-checks the prefix part by part."
        );

        s.manifest_verification = Some(verdict(vec![Failure::ManifestReadError {
            detail: "permission denied".into(),
        }]));
        let err = validate_run_gate(&s).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_VALIDATE_UNVERIFIED")
        );
        assert_eq!(crate::error::classify_exit(&err), 1);

        s.manifest_verification = None;
        let err = validate_run_gate(&s).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_VALIDATE_FAILED")
        );
        assert!(
            err.to_string()
                .contains("'orders': no verdict was recorded. "),
            "{err}"
        );
    }

    #[test]
    fn a_cursor_that_was_not_stored_is_a_coded_failure_naming_its_cause() {
        let cause = anyhow::anyhow!("attempt to write a readonly database");
        let err = cursor_not_stored("export", "orders", &cause);
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_STATE_CURSOR_NOT_STORED")
        );
        assert_eq!(crate::error::classify_exit(&err), 1);
        assert_eq!(
            err.to_string(),
            "export 'orders': the rows are delivered and the manifest is written, but the \
             incremental cursor could not be stored (attempt to write a readonly database), so \
             the next run delivers the same rows again. Repair the state database before the \
             next run."
        );
    }

    #[test]
    fn a_run_that_begins_closes_the_running_rows_of_the_runs_that_died_before_it() {
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let plan = chunked_plan_with_quality(None);
        ledger_begin_run(&state, &plan, "orders", "run_dead").expect("a writable state");
        ledger_begin_run(&state, &plan, "orders", "run_next").expect("a writable state");
        let rows = state.recent_run_status(10, false).unwrap();
        let mut seen: Vec<(String, String)> =
            rows.into_iter().map(|r| (r.run_id, r.status)).collect();
        seen.sort();
        assert_eq!(
            seen,
            [
                ("run_dead".to_string(), "interrupted".to_string()),
                ("run_next".to_string(), "running".to_string())
            ]
        );
    }

    #[test]
    fn a_state_that_refuses_the_first_write_refuses_the_run_before_the_ledger_row() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("s.db");
        let state = crate::state::StateStore::open_at_path(&db).expect("a state DB");
        let plan = chunked_plan_with_quality(None);
        let prefix = crate::pipeline::finalize::destination_uri_for_manifest(&plan.destination);
        ledger_begin_run(&state, &plan, "orders", "run_ok").expect("a writable state");
        let open_runs = || {
            let mut ids: Vec<String> = state
                .active_run_ids_on_prefix(&prefix)
                .unwrap()
                .into_iter()
                .collect();
            ids.sort();
            ids
        };
        assert_eq!(
            open_runs(),
            ["run_ok"],
            "fixture: the first write is the run-status row"
        );

        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER no_runs BEFORE INSERT ON run_status \
                 BEGIN SELECT RAISE(ABORT, 'the state is not writable'); END;",
            )
            .unwrap();
        let err = ledger_begin_run(&state, &plan, "orders", "run_refused").unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_STATE_NOT_WRITABLE")
        );
        assert_eq!(crate::error::classify_exit(&err), 1);
        let said = err.to_string();
        assert!(
            said.starts_with(
                "export 'orders': the state database did not take this run's first write (the \
                 state is not writable"
            ),
            "{said}"
        );
        assert!(
            said.ends_with(
                "so the export did not start and nothing was written to the destination. Make \
                 the state database writable and run again."
            ),
            "{said}"
        );
        assert_eq!(open_runs(), ["run_ok"]);
    }

    #[test]
    fn run_diagnosis_flags_flaky_link_and_spill_signals_only() {
        let base = || RunSummary {
            export_name: "orders".into(),
            total_rows: 1000,
            peak_rss_mb: 50,
            duration_ms: 2000,
            status: "success".into(),
            ..Default::default()
        };
        assert!(run_diagnosis(&base(), &[], false).is_none());
        let mut s = base();
        s.reconnects = 2;
        s.retries = 3;
        let line = run_diagnosis(&s, &[], false).expect("reconnects must diagnose");
        assert!(line.contains("2 reconnect"), "got: {line}");
        assert!(line.contains("retries=3"), "got: {line}");
        assert!(
            line.contains("[success]"),
            "status must be the resolved one: {line}"
        );
        let mut s = base();
        s.resumed = true;
        assert!(
            run_diagnosis(&s, &[], false)
                .unwrap()
                .contains("resumed a prior CRASHED")
        );
        let line = run_diagnosis(&base(), &[("Created_tmp_disk_tables".into(), 2782)], false)
            .expect("spills must diagnose");
        assert!(line.contains("2782 tmp-disk spills"), "got: {line}");
        assert!(run_diagnosis(&base(), &[("Created_tmp_disk_tables".into(), 5)], false).is_none());
    }

    #[test]
    fn run_diagnosis_spill_remedy_never_suggests_the_strategy_already_running() {
        let with_mode = |mode: &str| RunSummary {
            export_name: "items".into(),
            total_rows: 1000,
            peak_rss_mb: 50,
            duration_ms: 2000,
            status: "success".into(),
            mode: mode.into(),
            ..Default::default()
        };
        let spills = [("Created_tmp_disk_tables".to_string(), 240_i64)];
        for paged in ["keyset", "chunked"] {
            let line =
                run_diagnosis(&with_mode(paged), &spills, false).expect("spills must diagnose");
            assert!(
                !line.contains("mode: chunked") && !line.contains("chunk_by_key"),
                "{paged} export must not be told to switch to a paging it already runs: {line}"
            );
            assert!(
                line.contains("chunk_size") && line.contains("batch_size"),
                "{paged} remedy must name the page/batch levers: {line}"
            );
        }
        for unpaged in ["full", "incremental", "timewindow"] {
            let line =
                run_diagnosis(&with_mode(unpaged), &spills, false).expect("spills must diagnose");
            assert!(
                line.contains("chunk_by_key"),
                "{unpaged} remedy should offer the paging escape: {line}"
            );
        }
    }

    /// Field find (2026-08-13, --pool 5): the spill counter is `SHOW GLOBAL STATUS`.
    #[test]
    fn run_diagnosis_spill_attribution_hedges_under_concurrent_siblings() {
        let s = || RunSummary {
            export_name: "items".into(),
            total_rows: 1000,
            duration_ms: 2000,
            status: "success".into(),
            mode: "keyset".into(),
            ..Default::default()
        };
        let spills = [("Created_tmp_disk_tables".to_string(), 240_i64)];
        let solo = run_diagnosis(&s(), &spills, false).expect("spills must diagnose");
        assert!(
            solo.contains("server-global")
                && solo.contains("otherwise quiet")
                && solo.contains("lower `chunk_size`"),
            "solo attribution names the counter scope and keeps the lever: {solo}"
        );
        let pooled = run_diagnosis(&s(), &spills, true).expect("spills must diagnose");
        assert!(
            pooled.contains("server-global") && pooled.contains("run-level harm"),
            "concurrent attribution must name the counter scope and point at the \
             run-level total: {pooled}"
        );
        assert!(
            !pooled.contains("— the source spilled to disk;"),
            "concurrent line must not carry the solo blame phrasing: {pooled}"
        );
    }

    /// The spill fold must cover EVERY engine that has a spill counter, and name what it counted.
    #[test]
    fn spill_total_counts_every_engines_spill_counter_and_names_the_unit() {
        let mysql = [
            ("Created_tmp_disk_tables".to_string(), 40_i64),
            ("mysql_created_tmp_disk_tables".to_string(), 60_i64),
        ];
        let s = spill_total(&mysql);
        assert_eq!(s.total, 100, "both tmp-disk keys must fold in");
        assert_eq!(s.unit, "tmp-disk spills");

        let pg = [
            ("pg_blks_read".to_string(), 900_000_i64),
            ("pg_blks_hit".to_string(), 5_000_000_i64),
            ("pg_tup_returned".to_string(), 9_000_000_i64),
            ("pg_tup_fetched".to_string(), 800_000_i64),
            ("pg_temp_files".to_string(), 137_i64),
            ("pg_deadlocks".to_string(), 3_i64),
        ];
        let s = spill_total(&pg);
        assert_eq!(s.total, 137, "PG's temp_files IS the spill counter");
        assert_eq!(
            s.unit, "temp-file spills",
            "PG counts FILES — printing MySQL's tmp-disk-table noun for it would \
             misdescribe what was measured"
        );

        let mssql = [
            ("mssql_lock_waits".to_string(), 4_000_i64),
            ("mssql_lock_wait_ms".to_string(), 900_000_i64),
        ];
        assert_eq!(spill_total(&mssql).total, 0);
        let mongo = [
            ("mongo_docs_scanned".to_string(), 10_000_000_i64),
            ("mongo_wt_cache_bytes_read".to_string(), 1_048_576_i64),
        ];
        assert_eq!(spill_total(&mongo).total, 0);
    }

    /// The per-export DIAGNOSIS is the first surface that must see the PG spill.
    #[test]
    fn run_diagnosis_flags_a_postgres_temp_file_spill_in_its_own_words() {
        let s = RunSummary {
            export_name: "events".into(),
            total_rows: 1000,
            duration_ms: 2000,
            status: "success".into(),
            mode: "chunked".into(),
            ..Default::default()
        };
        let pg = [
            ("pg_temp_files".to_string(), 150_i64),
            ("pg_tup_returned".to_string(), 9_000_000_i64),
        ];
        let line = run_diagnosis(&s, &pg, false).expect("a PG temp-file spill must diagnose");
        assert!(
            line.contains("150 temp-file spills"),
            "PG spill must be counted and named as temp FILES: {line}"
        );
        assert!(
            !line.contains("tmp-disk"),
            "must not print MySQL's unit for a PG temp_files count: {line}"
        );
        let quiet = [("pg_temp_files".to_string(), 99_i64)];
        assert!(run_diagnosis(&s, &quiet, false).is_none());
    }

    /// `pg_stat_database.temp_bytes` is DATABASE-wide.
    #[test]
    fn pg_temp_bytes_warning_hedges_attribution_under_concurrent_siblings() {
        let big = 250 * 1024 * 1024;
        let solo = pg_temp_bytes_warning("orders", big, false).expect("250 MB must warn");
        assert!(
            solo.contains("during run") && solo.contains("+250.0 MB"),
            "solo wording keeps the direct attribution: {solo}"
        );
        let concurrent = pg_temp_bytes_warning("orders", big, true).expect("250 MB must warn");
        assert!(
            concurrent.contains("+250.0 MB"),
            "the hedge must stay LOUD about the volume: {concurrent}"
        );
        assert!(
            concurrent.contains("database-wide") && concurrent.contains("attribution is unknown"),
            "concurrent wording must name the counter scope and refuse to blame one \
             export: {concurrent}"
        );
        assert!(
            !concurrent.contains("during run —"),
            "concurrent line must not carry the solo phrasing: {concurrent}"
        );
        assert!(pg_temp_bytes_warning("orders", 100 * 1024 * 1024, false).is_none());
        assert!(pg_temp_bytes_warning("orders", 100 * 1024 * 1024, true).is_none());
    }

    /// The bracket arithmetic itself — what the run is CREDITED with spilling.
    #[test]
    fn the_bracket_reopens_only_when_it_opened_and_the_held_connection_went_quiet() {
        assert!(
            reopen_to_close_the_bracket(true, false),
            "opened, then the connection died"
        );
        assert!(
            !reopen_to_close_the_bracket(true, true),
            "the held connection still answers"
        );
        assert!(
            !reopen_to_close_the_bracket(false, false),
            "nothing to close: never opened"
        );
        assert!(!reopen_to_close_the_bracket(false, true));
    }

    #[test]
    fn the_temp_spill_rides_the_harm_snapshot_and_is_not_recorded_twice() {
        let key = crate::source::PG_TEMP_BYTES_KEY;
        let snap = vec![("pg_temp_files".to_string(), 3), (key.to_string(), 7)];
        assert_eq!(super::temp_bytes_of(Some(&snap)), Some(7));
        assert_eq!(
            super::temp_bytes_of(Some(&snap[..1])),
            None,
            "another engine"
        );
        assert_eq!(super::temp_bytes_of(None), None);
        assert_eq!(
            super::recorded_harm(snap),
            vec![("pg_temp_files".to_string(), 3)],
            "the spill already has its own summary field"
        );
    }

    #[test]
    fn pg_temp_bytes_delta_is_the_windows_growth_and_never_a_counter_reset() {
        const MB: i64 = 1024 * 1024;
        assert_eq!(
            pg_temp_bytes_delta(3 * MB, 7 * MB),
            4 * MB,
            "the credited spill is what the window ADDED, not the counter's absolute value"
        );
        assert!(
            pg_temp_bytes_warning("orders", pg_temp_bytes_delta(120 * MB, 150 * MB), false)
                .is_none(),
            "a 30 MB window must stay below the warn floor"
        );
        assert!(
            pg_temp_bytes_warning("orders", pg_temp_bytes_delta(0, 150 * MB), false).is_some(),
            "activation guard: the same 150 MB counter DOES warn when the window really \
             produced it — otherwise the assertion above passes on an inert threshold"
        );
        assert_eq!(
            pg_temp_bytes_delta(9 * MB, MB),
            0,
            "a counter reset mid-run must credit nothing, not a negative spill"
        );
    }

    #[test]
    fn synthetic_failed_summary_fields() {
        let err = anyhow::anyhow!("connection refused");
        let summary = synthetic_failed_summary("my_export", &err);
        assert_eq!(summary.export_name, "my_export");
        assert_eq!(summary.status, "failed");
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
        assert_eq!(summary.bytes_written, 0);
        assert!(
            summary
                .error_message
                .as_ref()
                .unwrap()
                .contains("connection refused")
        );
    }

    #[test]
    fn synthetic_failed_summary_run_id_contains_export_name() {
        let err = anyhow::anyhow!("boom");
        let summary = synthetic_failed_summary("orders", &err);
        assert!(
            summary.run_id.starts_with("orders_"),
            "run_id was: {}",
            summary.run_id
        );
    }

    #[test]
    fn synthetic_failed_summary_journal_is_empty() {
        let err = anyhow::anyhow!("boom");
        let summary = synthetic_failed_summary("orders", &err);
        assert!(summary.journal.entries.is_empty());
    }

    #[test]
    fn synthetic_failed_summary_no_quality_or_reconcile_state() {
        let err = anyhow::anyhow!("boom");
        let summary = synthetic_failed_summary("orders", &err);
        assert!(summary.quality_passed.is_none());
        assert!(summary.reconciled.is_none());
        assert!(summary.validated.is_none());
    }

    use crate::config::QualityConfig;
    use crate::config::SourceConfig;
    use crate::plan::{ChunkedPlan, ExtractionStrategy, ResolvedRunPlan};

    #[test]
    fn a_settle_column_that_is_not_temporal_fails_before_the_query_runs() {
        use crate::plan::{ExtractionStrategy, IncrementalCursorPlan, SettlePlan};
        use crate::types::{RivetType, TypeFidelity, TypeMapping};
        let mapping = |name: &str, native: &str, rivet_type: RivetType| TypeMapping {
            column_name: name.into(),
            source_native_type: native.into(),
            arrow_type: crate::types::rivet_type_to_arrow(&rivet_type),
            rivet_type,
            fidelity: TypeFidelity::Exact,
            nullable: true,
            warnings: Vec::new(),
            delivery: crate::types::Delivery::Native,
        };
        let mappings = [
            mapping("id", "bigint", RivetType::Int64),
            mapping(
                "ts",
                "timestamptz",
                RivetType::Timestamp {
                    unit: crate::types::TimeUnit::Microsecond,
                    timezone: Some("UTC".into()),
                },
            ),
            mapping("d", "date", RivetType::Date),
            mapping("v", "text", RivetType::String),
        ];
        let mut plan = chunked_plan_with_quality(None);
        let settled = |column: Option<&str>| {
            ExtractionStrategy::Incremental(IncrementalCursorPlan {
                primary_column: "id".into(),
                fallback_column: None,
                mode: crate::config::IncrementalCursorMode::SingleColumn,
                settle: Some(SettlePlan {
                    column: column.map(String::from),
                    after_secs: 60,
                }),
            })
        };
        plan.strategy = settled(Some("ts"));
        assert!(settle_columns_are_temporal(&plan, Some(&mappings)).is_ok());
        plan.strategy = settled(Some("d"));
        assert!(settle_columns_are_temporal(&plan, Some(&mappings)).is_ok());
        plan.strategy = settled(Some("v"));
        let err = settle_columns_are_temporal(&plan, Some(&mappings))
            .unwrap_err()
            .to_string();
        assert!(err.contains("settle column `v` is text"), "{err}");
        plan.strategy = settled(None);
        let err = settle_columns_are_temporal(&plan, Some(&mappings))
            .unwrap_err()
            .to_string();
        assert!(err.contains("settle column `id` is bigint"), "{err}");
        assert!(
            settle_columns_are_temporal(&plan, None).is_ok(),
            "no probe: the sink's own check is the backstop"
        );
        plan.strategy = settled(Some("absent"));
        assert!(settle_columns_are_temporal(&plan, Some(&mappings)).is_ok());
    }

    fn chunked_plan_with_quality(quality: Option<QualityConfig>) -> ResolvedRunPlan {
        ResolvedRunPlan {
            base_query: "SELECT id FROM orders".into(),
            strategy: ExtractionStrategy::Chunked(ChunkedPlan {
                column: "id".into(),
                chunk_size: 100,
                chunk_count: None,
                parallel: 1,
                by_days: None,
                checkpoint: false,
                max_attempts: 3,
            }),
            quality,
            source: SourceConfig {
                url: Some("postgresql://nobody@127.0.0.1:9999/x".into()),
                ..test_plan().source
            },
            ..test_plan()
        }
    }

    #[test]
    fn a_finished_keyset_run_releases_its_anchor_and_the_ranges_of_its_own_source_only() {
        use crate::plan::{ExtractionStrategy, KeysetPlan};
        let chunked = chunked_plan_with_quality(None);
        let mut plan = chunked.clone();
        plan.strategy = ExtractionStrategy::Keyset(KeysetPlan {
            key_column: "id".into(),
            chunk_size: 10,
            checkpoint: true,
            incremental: false,
            parallel: 2,
        });
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let (key, name) = (plan.progress_key(), plan.export_name.as_str());
        let one = [(None, None)];
        state.set_resume_run_id(&key, "run_1").unwrap();
        state
            .persist_keyset_ranges(name, &key.source, "run_1", "id", &one)
            .unwrap();
        state
            .persist_keyset_ranges(name, "another/source", "run_2", "id", &one)
            .unwrap();
        state
            .record_file(crate::state::FilePart {
                run_id: "run_1",
                export_name: name,
                file_name: "p0",
                rows: 1,
                bytes: 1,
                format: "parquet",
                compression: None,
                cursor_high: Some("1"),
            })
            .unwrap();
        let anchored = || state.get_resume_run_id(name, &key.source).unwrap();

        let claim = state.claim(key.clone()).unwrap();
        let gap = None;
        let ended = |failed| super::RunOutcome {
            failed,
            data_complete: !failed,
            manifest_gap: &gap,
        };

        super::finalize_keyset_anchor(&state, &claim, &plan, "run_1", &ended(true));
        assert_eq!(
            anchored().as_deref(),
            Some("run_1"),
            "a run that broke off keeps it"
        );
        super::finalize_keyset_anchor(&state, &claim, &chunked, "run_1", &ended(false));
        assert_eq!(anchored().as_deref(), Some("run_1"), "not a keyset finish");
        super::finalize_keyset_anchor(&state, &claim, &plan, "run_1", &ended(false));
        assert_eq!(anchored(), None);
        assert!(!state.has_keyset_ranges(name, "run_1", None).unwrap());
        assert!(
            state.has_keyset_ranges(name, "run_2", None).unwrap(),
            "another source keeps its ranges"
        );
    }

    #[test]
    fn build_metric_row_maps_every_summary_and_plan_field() {
        let mut summary = RunSummary::stub_for_testing("run-bmr", "orders");
        summary.duration_ms = 1234;
        summary.total_rows = 50_000;
        summary.peak_rss_mb = 142;
        summary.status = "success".into();
        summary.error_message = Some("export 'x': keyset could not read the 'id' value".into());
        summary.cursor_low = Some("1".into());
        summary.cursor_high = Some("18446744073709551615".into());
        summary.offending_value = Some("9223372036854775800".into());
        summary.server_context_json =
            Some(r#"{"engine":"mysql","max_execution_time_ms":"30000"}"#.into());
        summary.key_native_type = Some("bigint unsigned".into());
        summary.format = "parquet".into();
        summary.mode = "chunked".into();
        summary.files_produced = 7;
        summary.bytes_written = 4096;
        summary.bytes_read = 65536;
        summary.retries = 2;
        summary.validated = Some(true);
        summary.schema_changed = Some(false);
        summary.files_committed = 6;
        summary.reconciled = Some(true);
        summary.source_count = Some(49_999);
        summary.quality_passed = Some(true);
        summary.pg_temp_bytes_delta = Some(1_048_576);
        summary.batch_size = 32_000;
        summary.batch_size_memory_mb = Some(256);
        summary.skip_reason = Some("manual".into());
        summary.schema_fingerprint = Some("fp-abc".into());

        summary.journal.push_test_chunk_span(0, 640);

        let mut plan = chunked_plan_with_quality(None);
        plan.strategy = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 100_000,
            chunk_count: None,
            parallel: 4,
            by_days: None,
            checkpoint: false,
            max_attempts: 3,
        });

        let crate::state::MetricRow {
            export_name,
            run_id,
            duration_ms,
            total_rows,
            peak_rss_mb,
            status,
            error_message,
            tuning_profile,
            format,
            mode,
            files_produced,
            bytes_written,
            bytes_read,
            retries,
            validated,
            schema_changed,
            files_committed,
            reconciled,
            source_count,
            quality_passed,
            pg_temp_bytes_delta,
            batch_size,
            batch_size_memory_mb,
            skip_reason,
            schema_fingerprint,
            chunk_size,
            parallel,
            source_type,
            destination_type,
            rivet_version,
            longest_chunk_ms,
            chunk_key,
            error_class,
            cursor_min,
            cursor_max,
            key_descriptor_json,
            offending_value,
            server_context_json,
        } = build_metric_row(&summary, &plan, "safe");

        assert_eq!(export_name, "orders");
        assert_eq!(run_id, "run-bmr");
        assert_eq!(duration_ms, 1234);
        assert_eq!(total_rows, 50_000);
        assert_eq!(peak_rss_mb, Some(142));
        assert_eq!(status, "success");
        assert_eq!(
            error_message.as_deref(),
            Some("export 'x': keyset could not read the 'id' value")
        );
        assert_eq!(tuning_profile.as_deref(), Some("safe"));
        assert_eq!(format.as_deref(), Some("parquet"));
        assert_eq!(mode.as_deref(), Some("chunked"));
        assert_eq!(files_produced, 7);
        assert_eq!(bytes_written, 4096);
        assert_eq!(bytes_read, 65536);
        assert_eq!(retries, 2);
        assert_eq!(validated, Some(true));
        assert_eq!(schema_changed, Some(false));
        assert_eq!(files_committed, 6);
        assert_eq!(reconciled, Some(true));
        assert_eq!(source_count, Some(49_999));
        assert_eq!(quality_passed, Some(true));
        assert_eq!(pg_temp_bytes_delta, Some(1_048_576));
        assert_eq!(batch_size, 32_000);
        assert_eq!(batch_size_memory_mb, Some(256));
        assert_eq!(skip_reason.as_deref(), Some("manual"));
        assert_eq!(schema_fingerprint.as_deref(), Some("fp-abc"));
        assert_eq!(chunk_size, Some(100_000));
        assert_eq!(parallel, Some(4));
        assert_eq!(source_type.as_deref(), Some("postgres"));
        assert_eq!(destination_type.as_deref(), Some("local"));
        assert_eq!(rivet_version.as_deref(), Some(env!("CARGO_PKG_VERSION")));
        assert_eq!(longest_chunk_ms, Some(640));
        assert_eq!(longest_chunk_ms, summary.journal.longest_chunk_ms());
        assert_eq!(chunk_key.as_deref(), Some("id"));
        assert_eq!(
            error_class.as_deref(),
            Some("keyset_unreadable_key"),
            "error_class is DERIVED from error_message, not hardcoded"
        );
        assert_eq!(cursor_min.as_deref(), Some("1"));
        assert_eq!(cursor_max.as_deref(), Some("18446744073709551615"));
        let kd = key_descriptor_json.expect("chunked strategy carries a key descriptor");
        assert!(kd.contains("\"strategy\":\"chunked\""), "{kd}");
        assert!(kd.contains("\"key\":\"id\""), "{kd}");
        assert!(kd.contains("\"db_type\":\"bigint unsigned\""), "{kd}");
        assert!(kd.contains("\"unsigned\":true"), "{kd}");
        assert_eq!(offending_value.as_deref(), Some("9223372036854775800"));
        assert!(
            server_context_json
                .as_deref()
                .is_some_and(|s| s.contains("max_execution_time_ms")),
            "server_context flows from the summary: {server_context_json:?}"
        );
    }

    #[test]
    fn build_metric_row_non_chunked_has_no_chunk_dims() {
        let mut plan = chunked_plan_with_quality(None);
        plan.strategy = ExtractionStrategy::Snapshot;
        let summary = RunSummary::stub_for_testing("run-snap", "orders");

        let row = build_metric_row(&summary, &plan, "balanced");

        assert!(row.chunk_size.is_none(), "snapshot has no chunk_size");
        assert!(row.parallel.is_none(), "snapshot has no parallel");
        assert_eq!(row.source_type.as_deref(), Some("postgres"));
        assert_eq!(row.destination_type.as_deref(), Some("local"));
    }

    #[test]
    fn classify_error_message_maps_the_field_run_failure_classes() {
        assert_eq!(
            classify_error_message(
                "export 'aa_import_advcake': keyset could not read the 'id' value from the last row of page 0"
            ),
            Some("keyset_unreadable_key")
        );
        assert_eq!(
            classify_error_message(
                "MySqlError { ERROR 3024 (HY000): maximum statement execution time exceeded }"
            ),
            Some("statement_timeout")
        );
        assert_eq!(
            classify_error_message(
                "export 'aa_payouts_version': parallel checkpoint worker errors:\nchunk 3: MySqlError { ERROR 3024 (HY000): maximum statement execution time exceeded }"
            ),
            Some("parallel_checkpoint"),
            "the 3024-embedding wrapper must not be mis-bucketed as a bare statement_timeout"
        );
        assert_eq!(
            classify_error_message(
                "MySqlError { ERROR 1146 (42S02): Table 'rivet.ext_x' doesn't exist }"
            ),
            Some("relation_not_found")
        );
        assert_eq!(
            classify_error_message("Lock wait timeout exceeded; try restarting transaction"),
            Some("lock_timeout"),
            "a lock-wait embeds 'timeout' — must not fall through to statement_timeout"
        );
        assert_eq!(
            classify_error_message("Deadlock found when trying to get lock"),
            Some("deadlock")
        );
        assert_eq!(
            classify_error_message("Connection reset by peer"),
            Some("connection")
        );
        assert_eq!(
            classify_error_message("SELECT command denied to user 'rivet'@'%' for table 't'"),
            Some("privilege")
        );
        assert_eq!(
            classify_error_message("No space left on device"),
            Some("disk_full")
        );
        assert_eq!(
            classify_error_message("some entirely novel failure with no known signature"),
            None
        );
    }

    #[test]
    fn harm_deltas_subtracts_matched_counters() {
        let before = vec![
            ("pg_tup_returned".to_string(), 100),
            ("pg_blks_read".to_string(), 5),
        ];
        let after = vec![
            ("pg_tup_returned".to_string(), 150),
            ("pg_blks_read".to_string(), 9),
        ];
        let mut got = harm_deltas(&before, &after);
        got.sort();
        assert_eq!(
            got,
            vec![
                ("pg_blks_read".to_string(), 4),
                ("pg_tup_returned".to_string(), 50)
            ]
        );
    }

    #[test]
    fn harm_deltas_floors_counter_reset_at_zero() {
        let before = vec![("pg_tup_returned".to_string(), 1_000)];
        let after = vec![("pg_tup_returned".to_string(), 40)];
        assert_eq!(
            harm_deltas(&before, &after),
            vec![("pg_tup_returned".to_string(), 0)]
        );
    }

    #[test]
    fn harm_deltas_intersects_counter_names() {
        let before = vec![("shared".to_string(), 10), ("only_before".to_string(), 1)];
        let after = vec![("shared".to_string(), 25), ("only_after".to_string(), 7)];
        assert_eq!(
            harm_deltas(&before, &after),
            vec![("shared".to_string(), 15)]
        );
    }

    /// The run's plan refuses a Lossy column only under --strict.
    #[test]
    fn plan_open_columns_refuses_only_under_strict() {
        use crate::types::{RivetType, SourceColumn, TypeFidelity, TypeMapping};
        let lossy = || {
            let mut m = TypeMapping::from_source(
                &SourceColumn::simple("ts", "timestamp(9)", true),
                RivetType::String,
            );
            m.fidelity = TypeFidelity::Lossy;
            Some(vec![m])
        };
        let mut open = lossy();
        assert!(plan_open_columns("e", &mut open, false).is_ok());
        assert_eq!(open.as_ref().map(Vec::len), Some(1), "warn keeps the plan");
        let err = plan_open_columns("e", &mut lossy(), true).unwrap_err();
        assert!(format!("{err:#}").contains("ts"), "{err:#}");
        assert!(plan_open_columns("e", &mut None, false).is_ok());
        let err = plan_open_columns("e", &mut None, true).unwrap_err();
        assert!(
            format!("{err:#}").contains("could not be resolved"),
            "{err:#}"
        );
    }

    /// A source that answers the run's metadata probes from a script.
    struct ScriptedSource {
        count: std::result::Result<Option<String>, String>,
        mappings: Vec<crate::types::TypeMapping>,
    }

    impl crate::source::Source for ScriptedSource {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> crate::error::Result<()> {
            Ok(())
        }
        fn query_scalar(&mut self, _sql: &str) -> crate::error::Result<Option<String>> {
            self.count.clone().map_err(|e| anyhow::anyhow!(e))
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> crate::error::Result<Vec<crate::types::TypeMapping>> {
            Ok(self.mappings.clone())
        }
        fn server_context(&mut self) -> Option<String> {
            Some(r#"{"engine":"scripted"}"#.into())
        }
        fn primary_key(&mut self, table: &str) -> crate::error::Result<Option<Vec<String>>> {
            Ok(Some(vec![format!("pk_of_{table}")]))
        }
    }

    /// A source config whose port refuses every connection.
    fn unreachable_source() -> SourceConfig {
        let mut source = chunked_plan_with_quality(None).source;
        source.url = Some("postgresql://nobody@127.0.0.1:1/x".into());
        source
    }

    /// The run's metadata connection, already holding `src`.
    fn meta_holding(cfg: &SourceConfig, src: ScriptedSource) -> MetaConn<'_> {
        MetaConn {
            cfg,
            src: Some(Box::new(src)),
        }
    }

    /// Every signature phrase names its class on its own, in any letter case.
    #[test]
    fn each_error_signature_alone_names_its_class() {
        for (phrase, class) in [
            ("keyset could not read", "keyset_unreadable_key"),
            ("could not read the key value", "keyset_unreadable_key"),
            ("parallel checkpoint worker", "parallel_checkpoint"),
            ("deadlock", "deadlock"),
            ("lock wait timeout", "lock_timeout"),
            ("could not obtain lock", "lock_timeout"),
            ("maximum statement execution time", "statement_timeout"),
            ("statement timeout", "statement_timeout"),
            ("schema drift", "schema_drift"),
            ("schema changed", "schema_drift"),
            ("on_schema_drift", "schema_drift"),
            ("doesn't exist", "relation_not_found"),
            ("does not exist", "relation_not_found"),
            ("no such table", "relation_not_found"),
            ("invalid object name", "relation_not_found"),
            ("unknown table", "relation_not_found"),
            ("permission denied", "privilege"),
            ("command denied", "privilege"),
            ("privilege", "privilege"),
            ("view server state", "privilege"),
            ("access denied", "source_auth"),
            ("authentication failed", "source_auth"),
            ("password authentication", "source_auth"),
            ("connection reset", "connection"),
            ("connection refused", "connection"),
            ("connection closed", "connection"),
            ("broken pipe", "connection"),
            ("server has gone away", "connection"),
            ("connection timed out", "connection"),
            ("certificate", "tls"),
            ("tls handshake", "tls"),
            ("ssl error", "tls"),
            ("no space left", "disk_full"),
            ("disk full", "disk_full"),
            ("quota exceeded", "disk_full"),
            ("out of memory", "out_of_memory"),
            ("cannot allocate memory", "out_of_memory"),
        ] {
            let message = format!("export 'e': {} (page 7)", phrase.to_uppercase());
            assert_eq!(classify_error_message(&message), Some(class), "{phrase}");
        }
    }

    /// Each keyed strategy is named with the column it pages by.
    #[test]
    fn the_key_descriptor_names_the_column_each_strategy_pages_by() {
        use crate::config::IncrementalCursorMode;
        use crate::plan::IncrementalCursorPlan;
        let mut plan = chunked_plan_with_quality(None);
        assert_eq!(strategy_key_column(&plan), Some(("chunked", "id")));
        plan.strategy = ExtractionStrategy::Keyset(crate::plan::KeysetPlan {
            key_column: "order_no".into(),
            chunk_size: 10,
            checkpoint: false,
            incremental: false,
            parallel: 1,
        });
        assert_eq!(strategy_key_column(&plan), Some(("keyset", "order_no")));
        plan.strategy = ExtractionStrategy::Incremental(IncrementalCursorPlan {
            primary_column: "updated_at".into(),
            fallback_column: None,
            mode: IncrementalCursorMode::SingleColumn,
            settle: None,
        });
        assert_eq!(
            strategy_key_column(&plan),
            Some(("incremental", "updated_at"))
        );
        let described: serde_json::Value =
            serde_json::from_str(&key_descriptor_json(&plan, None).unwrap()).unwrap();
        assert_eq!(
            described,
            serde_json::json!({ "strategy": "incremental", "key": "updated_at" })
        );
        plan.strategy = ExtractionStrategy::Snapshot;
        assert_eq!(strategy_key_column(&plan), None);
        assert_eq!(key_descriptor_json(&plan, Some("bigint")), None);
    }

    /// A keyset run's metric row carries its page size and its fan-out.
    #[test]
    fn a_keyset_runs_metric_row_carries_its_page_size_and_its_fan_out() {
        let keyset = |parallel| {
            let mut plan = chunked_plan_with_quality(None);
            plan.strategy = ExtractionStrategy::Keyset(crate::plan::KeysetPlan {
                key_column: "order_no".into(),
                chunk_size: 250,
                checkpoint: false,
                incremental: false,
                parallel,
            });
            build_metric_row(
                &RunSummary::stub_for_testing("run-keyset", "orders"),
                &plan,
                "balanced",
            )
        };
        let row = keyset(4);
        assert_eq!((row.chunk_size, row.parallel), (Some(250), Some(4)));
        assert_eq!(row.chunk_key.as_deref(), Some("order_no"));
        assert_eq!(keyset(0).parallel, Some(1), "one reader is one worker");
    }

    /// Deltas that carry both engines' spill counters are summed and named for both.
    #[test]
    fn a_mixed_spill_is_summed_and_named_for_both_counters() {
        let both = [
            ("mysql_created_tmp_disk_tables".to_string(), 40_i64),
            ("pg_temp_files".to_string(), 2_i64),
        ];
        let spill = spill_total(&both);
        assert_eq!(spill.total, 42);
        assert_eq!(spill.unit, "disk spills (tmp-disk tables + temp files)");
    }

    /// Closing the bracket credits the temp spill to the summary and records every other counter's growth; with a snapshot missing it records nothing.
    #[test]
    fn closing_the_harm_bracket_records_the_windows_growth_and_credits_the_temp_spill() {
        let snapshot = |files: i64, returned: i64, temp_bytes: i64| {
            vec![
                ("pg_temp_files".to_string(), files),
                ("pg_tup_returned".to_string(), returned),
                (crate::source::PG_TEMP_BYTES_KEY.to_string(), temp_bytes),
            ]
        };
        let state = StateStore::open_in_memory().unwrap();
        let mut summary = RunSummary::stub_for_testing("run-harm", "orders");
        let bracket = HarmBracket {
            harm: Some(snapshot(3, 100, 1000)),
        };
        let deltas = bracket.close_with(Some(snapshot(5, 150, 4000)), &state, &mut summary);
        let grew = vec![
            ("pg_temp_files".to_string(), 2),
            ("pg_tup_returned".to_string(), 50),
        ];
        assert_eq!(deltas, grew);
        assert_eq!(summary.pg_temp_bytes_delta, Some(3000));
        assert_eq!(state.harm_rows_for_test("run-harm"), grew);

        for (before, after) in [
            (None, Some(snapshot(5, 150, 4000))),
            (Some(snapshot(3, 100, 1000)), None),
        ] {
            let mut summary = RunSummary::stub_for_testing("run-half", "orders");
            let deltas = HarmBracket { harm: before }.close_with(after, &state, &mut summary);
            assert_eq!(deltas, Vec::new());
            assert_eq!(summary.pg_temp_bytes_delta, None);
            assert_eq!(state.harm_rows_for_test("run-half"), Vec::new());
        }
    }

    /// The run-level snapshot is the engine's counters under their own names.
    #[test]
    fn the_harm_snapshot_is_the_sources_named_counters_and_nothing_when_it_cannot_connect() {
        use crate::connect::postgres::wire_recorder::{Answer, answering_server};
        let (url, _queries) = answering_server(Answer::Int8(vec![11, 12, 13, 14, 15, 16, 17]));
        let mut source = unreachable_source();
        assert_eq!(harm_snapshot(&source), None);
        source.url = Some(url);
        let counters: Vec<(String, i64)> = [
            ("pg_blks_read", 11),
            ("pg_blks_hit", 12),
            ("pg_tup_returned", 13),
            ("pg_tup_fetched", 14),
            ("pg_temp_files", 15),
            ("pg_deadlocks", 16),
            (crate::source::PG_TEMP_BYTES_KEY, 17),
            ("pg_slot_spill_txns", 11),
            ("pg_slot_spill_bytes", 12),
            ("pg_slot_decoded_bytes", 13),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect();
        assert_eq!(harm_snapshot(&source), Some(counters));
    }

    /// The open probe records the key column's own native type, the server context, the mappings.
    #[test]
    fn the_open_probe_records_the_key_columns_own_native_type() {
        use crate::types::{RivetType, SourceColumn, TypeMapping};
        let mapping = |name: &str, native: &str, rivet_type: RivetType| {
            TypeMapping::from_source(&SourceColumn::simple(name, native, false), rivet_type)
        };
        let probed = |want_key: bool| {
            let mut plan = chunked_plan_with_quality(None);
            plan.source = unreachable_source();
            plan.source_table = Some("public.orders".into());
            let scripted = ScriptedSource {
                count: Ok(None),
                mappings: vec![
                    mapping("payload", "text", RivetType::String),
                    mapping("id", "bigint unsigned", RivetType::Int64),
                ],
            };
            let state = StateStore::open_in_memory().unwrap();
            let mut summary = RunSummary::stub_for_testing("run-open", "orders");
            let mut meta = meta_holding(&plan.source, scripted);
            capture_open_forensics(&plan, &state, &mut summary, want_key, &mut meta);
            let stored: Vec<String> = state
                .get_stored_schema("orders")
                .unwrap()
                .unwrap_or_default()
                .into_iter()
                .map(|c| c.name)
                .collect();
            (summary, stored)
        };
        let (summary, stored) = probed(true);
        assert_eq!(summary.key_native_type.as_deref(), Some("bigint unsigned"));
        assert_eq!(
            summary.server_context_json.as_deref(),
            Some(r#"{"engine":"scripted"}"#)
        );
        assert_eq!(summary.open_mappings.as_ref().map(Vec::len), Some(2));
        assert_eq!(
            summary.open_primary_key,
            Some(vec!["pk_of_public.orders".to_string()])
        );
        assert_eq!(stored, ["payload", "id"]);
        assert_eq!(probed(false).0.open_primary_key, None);
    }

    /// The count is read on the held connection and compared with the committed parts' rows.
    #[test]
    fn the_reconcile_count_is_read_on_the_held_connection_and_compared_with_what_was_committed() {
        use crate::pipeline::run::run_tail_tests::{
            captured_warnings_mentioning, install_warn_capture,
        };
        install_warn_capture();
        let source = unreachable_source();
        let reconciled = |export: &str,
                          count: std::result::Result<Option<&str>, &str>,
                          part_rows: &[i64],
                          total_rows: i64| {
            let mut plan = chunked_plan_with_quality(None);
            plan.export_name = export.into();
            let mut summary = RunSummary::stub_for_testing("run-reconcile", export);
            summary.total_rows = total_rows;
            for (i, rows) in part_rows.iter().enumerate() {
                summary.manifest_parts.push(crate::manifest::ManifestPart {
                    part_id: i as u32,
                    path: format!("part-{i}.parquet"),
                    rows: *rows,
                    size_bytes: 1,
                    content_fingerprint: String::new(),
                    content_md5: String::new(),
                    status: crate::manifest::PartStatus::Committed,
                });
            }
            let scripted = ScriptedSource {
                count: count.map(|v| v.map(str::to_string)).map_err(str::to_string),
                mappings: Vec::new(),
            };
            let mut meta = meta_holding(&source, scripted);
            let why = reconcile_source_count(&plan, &mut summary, &mut meta);
            (why, summary.source_count, summary.reconciled)
        };
        let mismatch_warnings = |export: &str| {
            captured_warnings_mentioning(&format!("reconcile MISMATCH for '{export}'"))
        };

        assert_eq!(
            reconciled("rec_resumed", Ok(Some("2500")), &[2000, 500], 500),
            (None, Some(2500), Some(true)),
            "a resumed run is compared by every committed part, not by the rows it re-ran"
        );
        assert_eq!(mismatch_warnings("rec_resumed"), Vec::<String>::new());
        assert_eq!(
            reconciled("rec_no_parts", Ok(Some("7")), &[], 7),
            (None, Some(7), Some(true)),
            "with no part recorded the run's own row count is what is compared"
        );
        assert_eq!(
            reconciled("rec_short", Ok(Some("2499")), &[2000, 500], 500),
            (None, Some(2499), Some(false))
        );
        let said = mismatch_warnings("rec_short");
        assert_eq!(said.len(), 1, "{said:?}");
        assert!(
            said[0].ends_with("committed 2500 rows, source has 2499"),
            "{said:?}"
        );

        assert_eq!(
            reconciled("rec_null", Ok(None), &[5], 5),
            (
                Some("source reconcile COUNT(*) returned NULL".to_string()),
                None,
                None
            )
        );
        assert_eq!(
            reconciled("rec_text", Ok(Some("n/a")), &[5], 5),
            (
                Some("source reconcile count 'n/a' is not an integer".to_string()),
                None,
                None
            )
        );
        let (why, counted, verdict) = reconciled("rec_dropped", Err("connection reset"), &[5], 5);
        assert!(
            why.as_deref()
                .is_some_and(|w| w.starts_with("could not connect to the source to reconcile: ")),
            "a count that failed on the held connection is asked again on a fresh one: {why:?}"
        );
        assert_eq!((counted, verdict), (None, None));
    }

    #[test]
    fn a_rejected_plan_a_manifest_gap_and_an_unverified_reconcile_carry_their_codes() {
        let err = plan_rejection_error("orders", &["[rule] bad".to_string()]).unwrap();
        crate::error::assert_coded(&err, "RIVET_PLAN_REJECTED", 1);
        assert!(plan_rejection_error("orders", &[]).is_none());

        let gap =
            Some("the manifest write FAILED, so the prefix has 2 durable part(s)".to_string());
        let err = resolve_final_result(false, Ok(()), gap, [Ok(()), Ok(()), Ok(())]).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_DEST_MANIFEST_NOT_WRITTEN", 1);

        let summary = RunSummary {
            export_name: "orders".into(),
            ..Default::default()
        };
        let err = reconcile_run_gate(&summary, Some("source count query failed")).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_RUN_RECONCILE_UNVERIFIED", 1);
    }
}
