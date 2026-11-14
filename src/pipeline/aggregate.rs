//! Aggregate summary for a `rivet run` invocation.

use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::error::Result;
use crate::state::{ExportMetric, RunAggregate, RunAggregateEntry, StateStore};

use super::summary::RunSummary;
use super::{format_bytes, strip_chunked_recovery_hint};

/// Machine-readable view of one `export_metrics` row for `rivet metrics --json`.
#[derive(Debug, Clone, Serialize)]
struct MetricRowJson {
    pub export_name: String,
    pub run_id: Option<String>,
    pub run_at: String,
    pub duration_ms: i64,
    pub total_rows: i64,
    pub peak_rss_mb: Option<i64>,
    pub status: String,
    pub error_message: Option<String>,
    pub tuning_profile: Option<String>,
    pub format: Option<String>,
    pub mode: Option<String>,
    pub files_produced: i64,
    pub bytes_written: i64,
    pub retries: i64,
    pub validated: Option<bool>,
    pub schema_changed: Option<bool>,
}

impl From<&ExportMetric> for MetricRowJson {
    fn from(m: &ExportMetric) -> Self {
        Self {
            export_name: m.export_name.clone(),
            run_id: m.run_id.clone(),
            run_at: m.run_at.clone(),
            duration_ms: m.duration_ms,
            total_rows: m.total_rows,
            peak_rss_mb: m.peak_rss_mb,
            status: m.status.clone(),
            error_message: m.error_message.clone(),
            tuning_profile: m.tuning_profile.clone(),
            format: m.format.clone(),
            mode: m.mode.clone(),
            files_produced: m.files_produced,
            bytes_written: m.bytes_written,
            retries: m.retries,
            validated: m.validated,
            schema_changed: m.schema_changed,
        }
    }
}

/// Render metric rows as a pretty-printed JSON array for `rivet metrics --json`.
pub(super) fn metrics_to_json(metrics: &[ExportMetric]) -> Result<String> {
    let rows: Vec<MetricRowJson> = metrics.iter().map(MetricRowJson::from).collect();
    serde_json::to_string_pretty(&rows).map_err(|e| {
        crate::error::coded(
            crate::error::codes::INTERNAL_RUN_INVARIANT,
            format!("serde_json: {:#}", e),
        )
    })
}

/// Convert a per-export summary into an aggregate row, the snapshot legs it ran included.
pub(super) fn entry_from_summary(s: &RunSummary) -> RunAggregateEntry {
    with_snapshot_legs(
        RunAggregateEntry {
            snapshot_rows: 0,
            export_name: s.export_name.clone(),
            status: s.status.clone(),
            run_id: s.run_id.clone(),
            answered_by_run: None,
            rows: s.total_rows,
            files: s.files_produced as i64,
            bytes: s.bytes_written,
            bytes_read: s.bytes_read,
            duration_ms: s.duration_ms,
            mode: s.mode.clone(),
            error_message: s.error_message.clone(),
            skip_reason: s.skip_reason.clone(),
        },
        &s.snapshot_legs,
    )
}

/// A CDC export's entry plus what its `initial: snapshot` legs wrote in the same invocation.
fn with_snapshot_legs(
    mut entry: RunAggregateEntry,
    legs: &[RunAggregateEntry],
) -> RunAggregateEntry {
    for leg in legs {
        entry.rows += leg.rows;
        entry.snapshot_rows += leg.rows;
        entry.files += leg.files;
        entry.bytes += leg.bytes;
        entry.bytes_read += leg.bytes_read;
    }
    entry
}

/// The export names of `export`'s `initial: snapshot` legs; empty when it takes no baseline.
pub(super) fn snapshot_leg_names(
    export: &crate::config::ExportConfig,
) -> std::collections::HashSet<String> {
    let baseline = export.mode == crate::config::ExportMode::Cdc
        && export
            .cdc
            .as_ref()
            .is_some_and(|c| !c.captures_changes_only());
    if !baseline {
        return Default::default();
    }
    crate::config::cdc_captured_tables(export)
        .iter()
        .map(|table| crate::manifest::snapshot_leg_name(&export.name, table))
        .collect()
}

/// A child's `Finished` event of one of `legs` as an entry; `None` for any other event.
pub(super) fn snapshot_leg_entry(
    ev: &super::ipc::ChildEvent,
    legs: &std::collections::HashSet<String>,
) -> Option<RunAggregateEntry> {
    let super::ipc::ChildEvent::Finished {
        export_name,
        run_id,
        status,
        total_rows,
        files_produced,
        bytes_written,
        duration_ms,
        error_message,
        bytes_read,
        ..
    } = ev
    else {
        return None;
    };
    legs.contains(export_name).then(|| RunAggregateEntry {
        snapshot_rows: 0,
        export_name: export_name.clone(),
        status: status.clone(),
        run_id: run_id.clone(),
        answered_by_run: None,
        rows: *total_rows,
        files: *files_produced as i64,
        bytes: *bytes_written,
        bytes_read: *bytes_read,
        duration_ms: *duration_ms,
        mode: "full".into(),
        error_message: error_message.clone(),
        skip_reason: None,
    })
}

/// Build the aggregate from per-export entries plus run-level metadata.
pub(super) fn build(
    entries: Vec<RunAggregateEntry>,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    config_path: Option<&str>,
    parallel_mode: &str,
) -> RunAggregate {
    let total_exports = entries.len();
    let success_count = entries.iter().filter(|e| e.status == "success").count();
    let failed_count = entries.iter().filter(|e| e.status == "failed").count();
    let skipped_count = total_exports
        .saturating_sub(success_count)
        .saturating_sub(failed_count);
    let total_rows = entries.iter().map(|e| e.rows).sum();
    let total_files = entries.iter().map(|e| e.files).sum();
    let total_bytes = entries.iter().map(|e| e.bytes).sum();
    let total_bytes_read = entries.iter().map(|e| e.bytes_read).sum();

    let id = format!("agg_{}", started_at.format("%Y%m%dT%H%M%S%3f"));

    RunAggregate {
        run_aggregate_id: id,
        started_at: started_at.to_rfc3339(),
        finished_at: finished_at.to_rfc3339(),
        duration_ms: (finished_at - started_at).num_milliseconds(),
        config_path: config_path.map(|s| s.to_string()),
        parallel_mode: parallel_mode.to_string(),
        total_exports,
        success_count,
        failed_count,
        skipped_count,
        total_rows,
        total_files,
        total_bytes,
        total_bytes_read,
        per_export: entries,
    }
}

/// One export's run-over-run throughput comparison input.
pub(super) struct ThroughputPair {
    pub export_name: String,
    pub cur_rows: i64,
    pub cur_ms: i64,
    pub prev_rows: i64,
    pub prev_ms: i64,
    pub cur_mode: String,
    /// `None` when the baseline row predates the `mode` column.
    pub prev_mode: Option<String>,
}

/// Floor below which a run is too small/short for a throughput comparison to mean anything (startup overhead dominates.
const REGRESSION_MIN_ROWS: i64 = 10_000;
const REGRESSION_MIN_MS: i64 = 5_000;
/// A run counts as regressed when its rows/s drop to ≤ 2/3 of the previous success (≥1.5× slower per row).
const REGRESSION_RATIO: f64 = 1.5;
/// The two runs must have moved comparable amounts of data.
const REGRESSION_MAX_SCALE: i64 = 2;

/// Why two runs of one export are NOT comparable on rows/s.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Incomparable {
    /// Either side moved too few rows for rows/s to mean anything.
    TooFewRows,
    /// Either side ran too briefly — fixed per-run cost dominates.
    TooShort,
    /// The two runs moved very different amounts of data.
    ScaleMismatch,
    /// The export ran in a different mode (a `full` backfill vs an `incremental` delta reads as a regression while both are healthy).
    ModeChanged,
}

/// The comparability rule, pure and total: `None` means the pair may be compared, `Some(reason)` names the refusal.
pub(super) fn incomparable(p: &ThroughputPair) -> Option<Incomparable> {
    if p.cur_rows < REGRESSION_MIN_ROWS || p.prev_rows < REGRESSION_MIN_ROWS {
        return Some(Incomparable::TooFewRows);
    }
    if p.cur_ms < REGRESSION_MIN_MS || p.prev_ms < REGRESSION_MIN_MS {
        return Some(Incomparable::TooShort);
    }
    let scale = REGRESSION_MAX_SCALE;
    if p.cur_rows.saturating_mul(scale) < p.prev_rows
        || p.prev_rows.saturating_mul(scale) < p.cur_rows
    {
        return Some(Incomparable::ScaleMismatch);
    }
    if let Some(prev_mode) = &p.prev_mode
        && !prev_mode.is_empty()
        && !p.cur_mode.is_empty()
        && prev_mode != &p.cur_mode
    {
        return Some(Incomparable::ModeChanged);
    }
    None
}

/// Does this run's concurrency mode make a per-export rows/s DROP expected?
fn mode_shares_the_source(run_mode: &str) -> bool {
    !matches!(
        run_mode,
        "sequential" | "wave-sequential" | "single" | "pool-serial"
    )
}

/// Pure run-over-run self-check: compare each export's throughput (rows/s) against its previous SUCCESS and name the material regressions.
pub(super) fn throughput_regressions(pairs: &[ThroughputPair], run_mode: &str) -> Vec<String> {
    let mut out = Vec::new();
    for p in pairs {
        if let Some(reason) = incomparable(p) {
            log::debug!(
                "throughput self-check: skipping '{}' — {:?}",
                p.export_name,
                reason
            );
            continue;
        }
        let cur_tp = p.cur_rows as f64 * 1000.0 / p.cur_ms as f64;
        let prev_tp = p.prev_rows as f64 * 1000.0 / p.prev_ms as f64;
        if prev_tp > 0.0 && prev_tp / cur_tp >= REGRESSION_RATIO {
            let tail = if mode_shares_the_source(run_mode) {
                format!(
                    "this run ran {run_mode}, where exports share the source and per-export \
                     rows/s falls BY DESIGN — compare the run's makespan before blaming a \
                     governor shed / adaptive batch shrink / source load"
                )
            } else {
                "check governor sheds / adaptive batch shrinks / source load".to_string()
            };
            out.push(format!(
                "export '{}': throughput {} → {} rows/s ({:.1}× slower than its last success) — \
                 {tail}",
                p.export_name,
                format_rate(prev_tp),
                format_rate(cur_tp),
                prev_tp / cur_tp,
            ));
        }
    }
    out
}

/// Read each successful export's previous success from the state DB and WARN about material throughput regressions.
pub(super) fn warn_throughput_regressions(
    state: &StateStore,
    entries: &[RunAggregateEntry],
    run_mode: &str,
) {
    let mut pairs = Vec::new();
    for e in entries.iter().filter(|e| e.status == "success") {
        let Ok(Some(prev)) = state.get_last_success_metric_excluding(&e.export_name, &e.run_id)
        else {
            continue;
        };
        pairs.push(throughput_pair(e, &prev));
    }
    for line in throughput_regressions(&pairs, run_mode) {
        log::warn!("{line}");
    }
}

/// One export's current-vs-previous pair; the current side leaves out its snapshot legs, as the baseline row does.
fn throughput_pair(e: &RunAggregateEntry, prev: &ExportMetric) -> ThroughputPair {
    ThroughputPair {
        export_name: e.export_name.clone(),
        cur_rows: e.rows - e.snapshot_rows,
        cur_ms: e.duration_ms,
        prev_rows: prev.total_rows,
        prev_ms: prev.duration_ms,
        cur_mode: e.mode.clone(),
        prev_mode: prev.mode.clone(),
    }
}

/// Pretty-print the aggregate after all per-export blocks.
pub(super) fn print(agg: &RunAggregate) {
    eprintln!();
    eprintln!("════════════════════════════════════════════════════════");
    eprintln!("  Run summary ({} exports)", agg.total_exports);
    eprintln!("════════════════════════════════════════════════════════");
    eprintln!("  id:          {}", agg.run_aggregate_id);
    let mut status_line = format!(
        "{} success · {} failed",
        agg.success_count, agg.failed_count
    );
    if agg.skipped_count > 0 {
        status_line.push_str(&format!(" · {} skipped", agg.skipped_count));
    }
    eprintln!("  status:      {}", status_line);
    eprintln!("  rows:        {}", agg.total_rows);
    eprintln!("  files:       {}", agg.total_files);
    if agg.total_bytes_read > 0 {
        eprintln!("  bytes read:  {}", format_bytes(agg.total_bytes_read));
    }
    if agg.total_bytes > 0 {
        eprintln!("  bytes writ:  {}", format_bytes(agg.total_bytes));
    }
    eprintln!(
        "  duration:    {} (wall clock)",
        format_duration(agg.duration_ms)
    );
    if let Some(v) =
        crate::destination::transient_retries_summary(crate::destination::transient_retries_total())
    {
        eprintln!("  dest retries: {v}");
    }
    if agg.duration_ms > 0 && agg.total_rows > 0 {
        let rps = agg.total_rows as f64 * 1000.0 / agg.duration_ms as f64;
        eprintln!("  throughput:  {} rows/s", format_rate(rps));
    }
    eprintln!("  mode:        {}", agg.parallel_mode);
    if let Some(cp) = &agg.config_path {
        eprintln!("  config:      {}", cp);
    }
    if agg.failed_count > 0 {
        eprintln!();
        eprintln!("  failed exports:");
        let mut chunked_recovery: Vec<&str> = Vec::new();
        for e in agg.per_export.iter().filter(|e| e.status == "failed") {
            let msg = e
                .error_message
                .as_deref()
                .unwrap_or("(no error message recorded)");
            let (cause, has_chunked_hint) = strip_chunked_recovery_hint(msg);
            if has_chunked_hint {
                chunked_recovery.push(e.export_name.as_str());
            }
            eprintln!("    - {}: {}", e.export_name, truncate(cause, 200));
        }
        if !chunked_recovery.is_empty() {
            print_chunked_recovery(&chunked_recovery, agg.config_path.as_deref());
        }
    }
}

/// Print one consolidated recovery block for the failed chunked exports.
fn print_chunked_recovery(exports: &[&str], config_path: Option<&str>) {
    write_chunked_recovery(&mut std::io::stderr(), exports, config_path);
}

/// Write one consolidated recovery block for the failed chunked exports to `w`; nothing when there are none.
fn write_chunked_recovery(w: &mut dyn std::io::Write, exports: &[&str], config_path: Option<&str>) {
    if exports.is_empty() {
        return;
    }
    let _ = std::io::Write::write_all(w, chunked_recovery_text(exports, config_path).as_bytes());
}

/// The recovery block: a plain run resumes each interrupted checkpoint.
fn chunked_recovery_text(exports: &[&str], config_path: Option<&str>) -> String {
    let cfg = match config_path {
        Some(p) if !p.is_empty() => format!("--config {p}"),
        _ => "--config <CONFIG>".to_string(),
    };
    format!(
        "\n  recovery ({n} chunked export(s)):\n    \
         resume the interrupted runs (a plain run continues each from its checkpoint):\n      \
         rivet run {cfg}\n    \
         or abandon them and start over (their parts stay under the prefix, unmanifested):\n      \
         for e in {names}; do rivet state reset-chunks {cfg} --export \"$e\"; done && rivet run {cfg}\n",
        n = exports.len(),
        names = exports.join(" "),
    )
}

fn format_duration(ms: i64) -> String {
    if ms < 1000 {
        return format!("{}ms", ms);
    }
    let total_secs = ms / 1000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    if h > 0 {
        format!("{}h {}m {}s", h, m, s)
    } else if m > 0 {
        format!("{}m {}s", m, s)
    } else {
        format!("{:.1}s", ms as f64 / 1000.0)
    }
}

fn format_rate(r: f64) -> String {
    if r >= 1_000_000.0 {
        format!("{:.1}M", r / 1_000_000.0)
    } else if r >= 1_000.0 {
        format!("{:.1}K", r / 1_000.0)
    } else {
        format!("{:.0}", r)
    }
}

fn truncate(s: &str, max_chars: usize) -> String {
    match s.char_indices().nth(max_chars) {
        None => s.to_owned(),
        Some((byte_pos, _)) => {
            let mut out = s[..byte_pos].to_owned();
            out.push('…');
            out
        }
    }
}

/// Persist to state DB and optionally write JSON.
pub(super) fn persist(state: &StateStore, agg: &RunAggregate, summary_output: Option<&Path>) {
    if let Err(e) = state.record_run_aggregate(agg) {
        log::warn!(
            "aggregate: failed to record run_aggregate (observational, ignored): {:#}",
            e
        );
    } else {
        log::info!(
            "aggregate: recorded {} ({} exports, {} success, {} failed)",
            agg.run_aggregate_id,
            agg.total_exports,
            agg.success_count,
            agg.failed_count,
        );
    }

    if let Some(path) = summary_output {
        match write_json(path, agg) {
            Ok(()) => eprintln!("  written:     {}", path.display()),
            Err(e) => log::warn!(
                "aggregate: failed to write summary JSON to {}: {:#}",
                path.display(),
                e
            ),
        }
    }
}

/// Write the aggregate as pretty JSON, creating missing parent directories.
pub(super) fn write_json(path: &Path, agg: &RunAggregate) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            let what = format!("create_dir_all({})", parent.display());
            crate::error::coded_io(crate::error::codes::CLI_OUTPUT_UNWRITABLE, what, e)
        })?;
    }
    let json = serde_json::to_string_pretty(agg).map_err(|e| {
        crate::error::coded(
            crate::error::codes::INTERNAL_RUN_INVARIANT,
            format!("serde_json: {:#}", e),
        )
    })?;
    std::fs::write(path, json).map_err(|e| {
        let what = format!("write({})", path.display());
        crate::error::coded_io(crate::error::codes::CLI_OUTPUT_UNWRITABLE, what, e)
    })?;
    Ok(())
}

/// What one child process reported over its event channel: its own run and the snapshot legs it ran.
#[derive(Debug, Default)]
pub(super) struct ChildReport {
    /// `(run_id, mode)` of the child's own export, from its last `Finished` event.
    pub run: Option<(String, String)>,
    pub legs: Vec<RunAggregateEntry>,
}

impl ChildReport {
    /// Take one event of the child that runs `export`, whose snapshot legs are named `legs`.
    pub(super) fn take(
        &mut self,
        export: &str,
        legs: &std::collections::HashSet<String>,
        ev: &super::ipc::ChildEvent,
    ) {
        match ev {
            super::ipc::ChildEvent::Finished {
                export_name,
                run_id,
                mode,
                ..
            } if export_name == export => self.run = Some((run_id.clone(), mode.clone())),
            _ => self.legs.extend(snapshot_leg_entry(ev, legs)),
        }
    }
}

/// Every child's report, by the export it ran.
pub(super) type ChildReports = HashMap<String, ChildReport>;

/// Reconstruct per-export entries for `--parallel-export-processes`, where each child wrote its own `record_metric` row and the parent had no in-memory `RunSummary`.
pub(super) fn collect_child_entries(
    state: &StateStore,
    exports: &[&crate::config::ExportConfig],
    child_failures: &HashMap<String, String>,
    reports: &ChildReports,
) -> Vec<RunAggregateEntry> {
    let mut out = Vec::with_capacity(exports.len());
    for export in exports {
        let own = reports.get(&export.name).and_then(|r| r.run.as_ref());
        let mut entry: Option<RunAggregateEntry> = None;
        match own.map(|(run_id, _)| state.get_metric_by_run_id(run_id)) {
            Some(Ok(row)) => {
                if let Some(m) = row
                    && m.status != "running"
                    && m.export_name == export.name
                {
                    entry = Some(RunAggregateEntry {
                        snapshot_rows: 0,
                        export_name: m.export_name,
                        status: m.status,
                        run_id: m.run_id.unwrap_or_default(),
                        answered_by_run: None,
                        rows: m.total_rows,
                        files: m.files_produced,
                        bytes: m.bytes_written.max(0) as u64,
                        bytes_read: m.bytes_read.max(0) as u64,
                        duration_ms: m.duration_ms,
                        mode: m.mode.unwrap_or_default(),
                        error_message: m.error_message,
                        skip_reason: m.skip_reason,
                    });
                }
            }
            Some(Err(e)) => {
                log::warn!(
                    "aggregate: metric query failed for '{}': {:#} (treating as failed)",
                    export.name,
                    e
                );
            }
            None => {}
        }

        out.push(entry.unwrap_or_else(|| {
            RunAggregateEntry {
                snapshot_rows: 0,
                export_name: export.name.clone(),
                status: "failed".into(),
                run_id: String::new(),
                answered_by_run: None,
                rows: 0,
                files: 0,
                bytes: 0,
                bytes_read: 0,
                duration_ms: 0,
                mode: String::new(),
                error_message: Some(
                    child_failures
                        .get(export.name.as_str())
                        .cloned()
                        .unwrap_or_else(|| "no metric recorded for this run".into()),
                ),
                skip_reason: None,
            }
        }));
    }
    out.into_iter()
        .map(|mut entry| {
            let report = reports.get(&entry.export_name);
            if let Some((run_id, mode)) = report.and_then(|r| r.run.as_ref())
                && entry.run_id.is_empty()
            {
                entry.run_id = run_id.clone();
                entry.mode = mode.clone();
            }
            with_snapshot_legs(entry, report.map_or(&[][..], |r| r.legs.as_slice()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn chunked_recovery_never_chains_a_reset_into_a_resume() {
        let text = chunked_recovery_text(&["a", "b"], Some("r.yaml"));
        assert!(
            text.contains("      rivet run --config r.yaml\n"),
            "a plain run resumes:\n{text}"
        );
        assert!(
            text.contains("--export \"$e\"; done && rivet run --config r.yaml\n"),
            "a reset is followed by a run without --resume:\n{text}"
        );
        assert!(!text.contains("--resume"), "{text}");
        assert!(
            chunked_recovery_text(&["a"], Some("")).contains("rivet run --config <CONFIG>\n"),
            "an empty config path prints the placeholder"
        );
        let mut written = Vec::new();
        write_chunked_recovery(&mut written, &["a", "b"], Some("r.yaml"));
        assert_eq!(
            String::from_utf8(written).unwrap(),
            text,
            "the block is what gets written"
        );
        let mut none = Vec::new();
        write_chunked_recovery(&mut none, &[], Some("r.yaml"));
        assert!(none.is_empty(), "no failed chunked export, no block");
    }

    /// A child that CRASHED must be reported as failed with its own cause.
    #[test]
    fn a_crashed_childs_in_flight_row_is_not_mistaken_for_its_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::StateStore::open_at_path(&dir.path().join("state.db")).unwrap();

        for i in 0..3 {
            state
                .record_file(crate::state::FilePart {
                    run_id: "r_crashed",
                    export_name: "orders",
                    file_name: &format!("part{i}.parquet"),
                    rows: 100,
                    bytes: 1000,
                    format: "parquet",
                    compression: None,
                    cursor_high: None,
                })
                .unwrap();
        }
        state
            .record_durable_part(crate::state::DurablePart {
                run_id: "r_crashed",
                export_name: "orders",
                file_name: "part0.parquet",
                rows: 100,
                bytes: 1000,
                format: "parquet",
                compression: None,
                mode: "chunked",
                cursor_high: None,
            })
            .unwrap();

        let latest = state.get_metrics(Some("orders"), 1).unwrap();
        assert_eq!(latest.len(), 1, "the projection must have written a row");
        assert_eq!(latest[0].status, "running");
        assert!(latest[0].total_rows > 0, "partial rows are the trap");

        let export: crate::config::ExportConfig = serde_yaml_ng::from_str(
            "name: orders\nquery: \"SELECT 1\"\nformat: parquet\ndestination:\n  type: local\n  path: /tmp\n",
        )
        .expect("parse test ExportConfig");
        let mut failures = std::collections::HashMap::new();
        failures.insert("orders".to_string(), "child killed by signal 9".to_string());
        let entries = collect_child_entries(&state, &[&export], &failures, &HashMap::new());

        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].status, "failed",
            "a child that never finished must not be reported by its in-flight row"
        );
        assert_eq!(
            entries[0].rows, 0,
            "the rows it had written when it died are not the rows it delivered"
        );
        assert_eq!(
            entries[0].error_message.as_deref(),
            Some("child killed by signal 9"),
            "the parent's own cause must survive — it is the only place it exists"
        );
    }

    /// One finished `export_metrics` row, as a run of `name` writes it.
    fn finished_metric(state: &crate::state::StateStore, name: &str, rows: i64, files: i64) {
        state
            .record_metric_full(&crate::state::MetricRow {
                export_name: name.into(),
                run_id: format!("{name}_run"),
                total_rows: rows,
                files_produced: files,
                bytes_written: rows * 10,
                bytes_read: rows * 20,
                status: "success".into(),
                mode: Some("full".into()),
                ..Default::default()
            })
            .unwrap();
    }

    /// The `initial: snapshot` CDC export `pg_cdc` over `items` and `orders`.
    fn cdc_export(cdc: &str) -> crate::config::ExportConfig {
        serde_yaml_ng::from_str(&format!(
            "name: pg_cdc\nmode: cdc\ntables: [items, orders]\nformat: parquet\n{cdc}\
             destination:\n  type: local\n  path: /tmp\n"
        ))
        .expect("parse test ExportConfig")
    }

    /// The `Finished` event a child prints for a run of `name`.
    fn finished(name: &str, rows: i64) -> super::super::ipc::ChildEvent {
        super::super::ipc::ChildEvent::Finished {
            export_name: name.into(),
            run_id: format!("{name}_run"),
            status: "success".into(),
            total_rows: rows,
            files_produced: 1,
            bytes_written: rows as u64 * 10,
            duration_ms: 5,
            peak_rss_mb: 1,
            error_message: None,
            dest_retries: 0,
            bytes_read: rows as u64 * 20,
            mode: "full".into(),
        }
    }

    /// What the child running `pg_cdc` reported, built from its events.
    fn report_of(events: &[super::super::ipc::ChildEvent]) -> ChildReports {
        let legs = snapshot_leg_names(&cdc_export("cdc: { initial: snapshot }\n"));
        let mut report = ChildReport::default();
        for ev in events {
            report.take("pg_cdc", &legs, ev);
        }
        HashMap::from([("pg_cdc".to_string(), report)])
    }

    /// A process child's CDC entry counts the snapshot legs that child reported, and only those.
    #[test]
    fn a_cdc_childs_entry_counts_the_snapshot_legs_it_ran() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::StateStore::open_at_path(&dir.path().join("state.db")).unwrap();
        finished_metric(&state, "pg_cdc", 3, 1);
        let export = cdc_export("cdc: { initial: snapshot }\n");
        let reports = report_of(&[
            finished("pg_cdc__snapshot_items", 1000),
            finished("pg_cdc", 3),
            finished("unrelated", 50),
        ]);
        let entries = collect_child_entries(&state, &[&export], &HashMap::new(), &reports);

        assert_eq!(entries.len(), 1, "the legs are not units of their own");
        let e = &entries[0];
        assert_eq!(
            (e.export_name.as_str(), e.run_id.as_str()),
            ("pg_cdc", "pg_cdc_run")
        );
        assert_eq!(
            (e.rows, e.files, e.bytes, e.bytes_read, e.snapshot_rows),
            (1003, 2, 10_030, 20_060, 1000),
            "the stream plus the leg this child reported, never another event"
        );
    }

    /// A CDC child killed after its snapshot leg finished.
    #[test]
    fn a_cdc_child_killed_after_its_leg_counts_the_leg_it_reported() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::StateStore::open_at_path(&dir.path().join("state.db")).unwrap();
        finished_metric(&state, "pg_cdc__snapshot_items", 30, 1);
        let export = cdc_export("cdc: { initial: snapshot }\n");
        let failures = HashMap::from([(
            "pg_cdc".to_string(),
            "exited with status signal".to_string(),
        )]);
        let reports = report_of(&[finished("pg_cdc__snapshot_items", 30)]);
        let entries = collect_child_entries(&state, &[&export], &failures, &reports);
        let e = &entries[0];
        assert_eq!(
            (e.status.as_str(), e.run_id.as_str(), e.mode.as_str()),
            ("failed", "", ""),
            "the child reported no run of its own"
        );
        assert_eq!(
            (
                e.rows,
                e.files,
                e.bytes,
                e.bytes_read,
                e.snapshot_rows,
                e.duration_ms
            ),
            (30, 1, 300, 600, 30, 0),
            "the finished leg's rows were written and are counted; duration stays the drain's, none"
        );
        assert_eq!(
            e.error_message.as_deref(),
            Some("exited with status signal")
        );
    }

    /// A leg row another holder of the export name wrote on the shared state is not this child's.
    #[test]
    fn another_holders_leg_row_is_not_folded_into_this_childs_entry() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::StateStore::open_at_path(&dir.path().join("state.db")).unwrap();
        finished_metric(&state, "pg_cdc", 3, 1);
        finished_metric(&state, "pg_cdc__snapshot_items", 70, 1);
        let export = cdc_export("cdc: { initial: snapshot }\n");
        let reports = report_of(&[finished("pg_cdc", 3)]);
        let entries = collect_child_entries(&state, &[&export], &HashMap::new(), &reports);
        assert_eq!(
            (entries[0].rows, entries[0].files),
            (3, 1),
            "this child reported no leg, so its entry is its stream alone"
        );
    }

    /// Only a CDC export that takes a baseline has legs, so a user export of a leg's name is never one.
    #[test]
    fn only_a_baseline_cdc_export_has_snapshot_legs() {
        let legs = snapshot_leg_names(&cdc_export("cdc: { initial: snapshot }\n"));
        assert_eq!(
            legs,
            ["pg_cdc__snapshot_items", "pg_cdc__snapshot_orders"]
                .map(String::from)
                .into()
        );
        assert!(snapshot_leg_names(&cdc_export("")).is_empty());
        assert!(
            snapshot_leg_entry(&finished("pg_cdc__snapshot_items", 1), &Default::default())
                .is_none()
        );
    }

    /// Another holder's newer row of the export's name is not this failed child's entry.
    #[test]
    fn another_holders_row_of_the_same_name_is_not_this_childs_entry() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::StateStore::open_at_path(&dir.path().join("state.db")).unwrap();
        finished_metric(&state, "pg_cdc", 3, 1);
        let export = cdc_export("cdc: { initial: snapshot }\n");
        let failures = HashMap::from([("pg_cdc".to_string(), "exited with status 1".to_string())]);
        let entries = collect_child_entries(&state, &[&export], &failures, &HashMap::new());
        assert_eq!(
            (entries[0].status.as_str(), entries[0].run_id.as_str()),
            ("failed", ""),
            "a child that reported no run of its own is failed, never another holder's success"
        );
    }

    /// A child's entry is the row of the run it reported, even when another holder wrote a newer row of the name.
    #[test]
    fn a_childs_entry_is_the_row_of_the_run_it_reported() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::StateStore::open_at_path(&dir.path().join("state.db")).unwrap();
        let row = |run_id: &str, status: &str| crate::state::MetricRow {
            export_name: "pg_cdc".into(),
            run_id: run_id.into(),
            status: status.into(),
            mode: Some("cdc".into()),
            ..Default::default()
        };
        state.record_metric_full(&row("mine", "failed")).unwrap();
        state.record_metric_full(&row("theirs", "success")).unwrap();
        let export = cdc_export("cdc: { initial: snapshot }\n");
        let mut ev = finished("pg_cdc", 0);
        if let super::super::ipc::ChildEvent::Finished { run_id, .. } = &mut ev {
            *run_id = "mine".into();
        }
        let entries = collect_child_entries(&state, &[&export], &HashMap::new(), &report_of(&[ev]));
        assert_eq!(
            (entries[0].status.as_str(), entries[0].run_id.as_str()),
            ("failed", "mine")
        );
    }

    /// A child whose reported run left no row of the export keeps the fallback, with the run id and mode it reported.
    #[test]
    fn a_reported_run_without_a_row_fails_under_the_reported_run_id() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::StateStore::open_at_path(&dir.path().join("state.db")).unwrap();
        finished_metric(&state, "pg_cdc__snapshot_items", 40, 1);
        let export = cdc_export("cdc: { initial: snapshot }\n");
        let mut ev = finished("pg_cdc", 0);
        if let super::super::ipc::ChildEvent::Finished {
            run_id,
            mode,
            status,
            ..
        } = &mut ev
        {
            *run_id = "pg_cdc__snapshot_items_run".into();
            *mode = "cdc".into();
            *status = "failed".into();
        }
        let failures = HashMap::from([("pg_cdc".to_string(), "exited with status 1".to_string())]);
        let entries = collect_child_entries(&state, &[&export], &failures, &report_of(&[ev]));
        let e = &entries[0];
        assert_eq!(
            (
                e.export_name.as_str(),
                e.status.as_str(),
                e.run_id.as_str(),
                e.mode.as_str(),
                e.rows
            ),
            ("pg_cdc", "failed", "pg_cdc__snapshot_items_run", "cdc", 0),
            "a row of another export is never this export's entry"
        );
    }

    /// A `backfill:` baseline runs through the same legs as `initial: snapshot`, so it has their names.
    #[test]
    fn a_backfill_cdc_export_has_snapshot_legs() {
        let legs = snapshot_leg_names(&cdc_export("cdc: { backfill: [items_base] }\n"));
        assert_eq!(
            legs,
            ["pg_cdc__snapshot_items", "pg_cdc__snapshot_orders"]
                .map(String::from)
                .into()
        );
    }

    /// The throughput self-check compares the drain alone, as its baseline metric row holds it.
    #[test]
    fn the_throughput_pair_leaves_the_snapshot_legs_out() {
        let mut e = entry("pg_cdc", "success", 6003, 3, 0);
        e.snapshot_rows = 6000;
        let prev = metric("pg_cdc", "success");
        let p = throughput_pair(&e, &prev);
        assert_eq!((p.cur_rows, p.prev_rows), (3, prev.total_rows));
    }
    use super::*;
    use chrono::Duration;

    /// A comparable pair by default — same export mode both sides, so each test names only the dimension it is about.
    fn pair(
        name: &str,
        cur_rows: i64,
        cur_ms: i64,
        prev_rows: i64,
        prev_ms: i64,
    ) -> ThroughputPair {
        ThroughputPair {
            export_name: name.into(),
            cur_rows,
            cur_ms,
            prev_rows,
            prev_ms,
            cur_mode: "full".into(),
            prev_mode: Some("full".into()),
        }
    }

    /// The honest output for a run that exported nothing is an empty-but-VALID document, never silence.
    #[test]
    fn a_zero_export_run_still_builds_a_valid_document() {
        let now = Utc::now();
        let agg = build(
            vec![],
            now - Duration::seconds(3),
            now,
            Some("c.yaml"),
            "sequential",
        );
        let json = serde_json::to_string_pretty(&agg).expect("must serialize");
        let round: serde_json::Value = serde_json::from_str(&json).expect("must parse");
        assert_eq!(round["total_exports"], 0);
        assert_eq!(round["success_count"], 0);
        assert_eq!(round["failed_count"], 0);
        assert_eq!(round["total_rows"], 0);
        assert!(
            round["per_export"].as_array().is_some_and(|a| a.is_empty()),
            "per_export must be present and empty, not absent: {json}"
        );
        assert_eq!(round["duration_ms"], 3000, "the window is still measured");
        assert_eq!(round["config_path"], "c.yaml");
    }

    fn entry(name: &str, status: &str, rows: i64, files: i64, bytes: u64) -> RunAggregateEntry {
        RunAggregateEntry {
            snapshot_rows: 0,
            bytes_read: 0,
            export_name: name.into(),
            status: status.into(),
            run_id: format!("{name}_run"),
            answered_by_run: None,
            rows,
            files,
            bytes,
            duration_ms: 1000,
            mode: "full".into(),
            error_message: if status == "failed" {
                Some("boom".into())
            } else {
                None
            },
            skip_reason: None,
        }
    }

    #[test]
    fn build_aggregates_counts_and_totals() {
        let started = Utc::now();
        let finished = started + Duration::seconds(120);
        let agg = build(
            vec![
                entry("a", "success", 100, 1, 1024),
                entry("b", "failed", 0, 0, 0),
                entry("c", "success", 50, 2, 2048),
            ],
            started,
            finished,
            Some("conf.yaml"),
            "sequential",
        );

        assert_eq!(agg.total_exports, 3);
        assert_eq!(agg.success_count, 2);
        assert_eq!(agg.failed_count, 1);
        assert_eq!(agg.skipped_count, 0);
        assert_eq!(agg.total_rows, 150);
        assert_eq!(agg.total_files, 3);
        assert_eq!(agg.total_bytes, 3072);
        assert_eq!(agg.duration_ms, 120_000);
        assert_eq!(agg.parallel_mode, "sequential");
        assert_eq!(agg.config_path.as_deref(), Some("conf.yaml"));
        assert!(
            agg.run_aggregate_id.starts_with("agg_"),
            "id should start with `agg_`, got {}",
            agg.run_aggregate_id
        );
    }

    #[test]
    fn build_handles_unknown_status_as_skipped() {
        let started = Utc::now();
        let finished = started + Duration::seconds(1);
        let agg = build(
            vec![
                entry("a", "success", 1, 0, 0),
                entry("b", "running", 0, 0, 0),
            ],
            started,
            finished,
            None,
            "sequential",
        );
        assert_eq!(agg.success_count, 1);
        assert_eq!(agg.failed_count, 0);
        assert_eq!(agg.skipped_count, 1);
    }

    #[test]
    fn build_with_zero_exports_is_well_formed() {
        let now = Utc::now();
        let agg = build(vec![], now, now, None, "sequential");
        assert_eq!(agg.total_exports, 0);
        assert_eq!(agg.total_rows, 0);
        assert_eq!(agg.success_count, 0);
        assert_eq!(agg.failed_count, 0);
        assert_eq!(agg.skipped_count, 0);
    }

    #[test]
    fn format_duration_picks_unit() {
        assert_eq!(format_duration(500), "500ms");
        assert_eq!(format_duration(1500), "1.5s");
        assert_eq!(format_duration(65_000), "1m 5s");
        assert_eq!(format_duration(3_725_000), "1h 2m 5s");
    }

    /// The run-over-run self-check that answers "prove the next run is not strangling itself".
    #[test]
    fn throughput_regressions_flag_real_slowdowns_only() {
        let out = throughput_regressions(
            &[pair("big", 1_000_000, 24_000, 1_000_000, 10_000)],
            "sequential",
        );
        assert_eq!(out.len(), 1, "a 2.4× slowdown must be flagged: {out:?}");
        assert!(
            out[0].contains("big") && out[0].contains("slower"),
            "{out:?}"
        );
        assert!(
            throughput_regressions(
                &[pair("ok", 1_000_000, 12_000, 1_000_000, 10_000)],
                "sequential"
            )
            .is_empty()
        );
        assert!(
            throughput_regressions(
                &[pair("fast", 1_000_000, 8_000, 1_000_000, 10_000)],
                "sequential"
            )
            .is_empty()
        );
        assert!(
            throughput_regressions(
                &[pair("grew", 1_800_000, 18_000, 1_000_000, 10_000)],
                "sequential"
            )
            .is_empty()
        );
        assert!(
            throughput_regressions(&[pair("tiny", 500, 5_000, 500, 500)], "sequential").is_empty()
        );
        assert!(
            throughput_regressions(&[pair("short", 20_000, 4_000, 20_000, 400)], "sequential")
                .is_empty(),
            "prev under the min-duration floor must not baseline"
        );
    }

    /// The comparability rule is the load-bearing half of the self-check.
    #[test]
    fn throughput_regressions_refuse_pairs_of_incomparable_shape() {
        let backfill = pair("orders", 40_000, 6_000, 2_000_000, 60_000);
        assert_eq!(incomparable(&backfill), Some(Incomparable::ScaleMismatch));
        let short_cur = pair("short-cur", 40_000, 4_000, 80_000, 5_000);
        assert_eq!(incomparable(&short_cur), Some(Incomparable::TooShort));
        let mut mode_flip = pair("switched", 100_000, 40_000, 100_000, 10_000);
        mode_flip.cur_mode = "incremental".into();
        mode_flip.prev_mode = Some("full".into());
        assert_eq!(incomparable(&mode_flip), Some(Incomparable::ModeChanged));
        let real = pair("real", 1_000_000, 24_000, 1_000_000, 10_000);
        assert_eq!(incomparable(&real), None);

        let out = throughput_regressions(&[backfill, short_cur, mode_flip, real], "sequential");
        assert_eq!(
            out.len(),
            1,
            "only the comparable pair may be reported: {out:?}"
        );
        assert!(out[0].contains("real"), "{out:?}");
    }

    /// A concurrency mode is not a regression: under `--pool` every export shares the source, so per-export rows/s falls while the makespan improves.
    #[test]
    fn throughput_regression_text_names_a_source_sharing_mode() {
        let p = || pair("orders", 1_000_000, 24_000, 1_000_000, 10_000);
        let pooled = throughput_regressions(&[p()], "pool");
        assert_eq!(pooled.len(), 1, "the line must still print under a pool");
        assert!(
            pooled[0].contains("pool") && pooled[0].contains("makespan"),
            "a source-sharing run must name its mode instead of blaming a shed: {pooled:?}"
        );
        let serial = throughput_regressions(&[p()], "sequential");
        assert!(
            serial[0].contains("check governor sheds"),
            "a serial run keeps the direct attribution: {serial:?}"
        );
        for m in [
            "parallel-threads",
            "parallel-processes",
            "wave-parallel-processes",
            "pool",
        ] {
            assert!(mode_shares_the_source(m), "{m} shares the source");
        }
        for m in ["sequential", "wave-sequential", "single"] {
            assert!(!mode_shares_the_source(m), "{m} is serial");
        }
        let serial_pool = throughput_regressions(&[p()], "pool-serial");
        assert!(
            serial_pool[0].contains("check governor sheds"),
            "a serialized pool must not excuse a regression as source sharing: {serial_pool:?}"
        );
        assert!(mode_shares_the_source("a-mode-added-next-release"));
    }

    /// Drop `//` comments so a doc's ``[`wave_mode_label`]`` cannot be read as a definition, and a doc's `"pool-serial"` cannot be read as a label a function returns.
    fn code_only(src: &str) -> String {
        src.lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The string literals a function body can return, in source order.
    fn string_literals(body: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut chars = body.chars();
        while let Some(c) = chars.next() {
            if c != '"' {
                continue;
            }
            let mut lit = String::new();
            for c in chars.by_ref() {
                if c == '"' {
                    break;
                }
                lit.push(c);
            }
            out.push(lit);
        }
        out
    }

    /// Every function `pipeline::run` DEFINES that can PRODUCE a label, with the labels it can return.
    fn mode_label_producers() -> Vec<(String, Vec<String>)> {
        let whole = include_str!("run.rs");
        let product = &whole[..whole
            .find("\n#[cfg(test)]")
            .expect("run.rs has test modules")];
        let code = code_only(product);
        let mut consts: Vec<(String, String)> = Vec::new();
        for line in code.lines().filter(|l| !l.starts_with(char::is_whitespace)) {
            let Some(rest) = line.split_once("const ").map(|(_, r)| r) else {
                continue;
            };
            let Some((name, value)) = rest.split_once(':') else {
                continue;
            };
            if let Some(lit) = string_literals(value).first() {
                consts.push((name.trim().to_string(), lit.clone()));
            }
        }
        let mut out: Vec<(String, Vec<String>)> = Vec::new();
        for (at, line) in code
            .match_indices('\n')
            .map(|(i, _)| (i + 1, code[i + 1..].lines().next().unwrap_or("")))
        {
            // module is indented, and rustfmt keeps it that way.
            let decl = line
                .strip_prefix("pub ")
                .or_else(|| {
                    line.starts_with("pub(")
                        .then(|| line.split_once(") ").map(|(_, r)| r))
                        .flatten()
                })
                .unwrap_or(line);
            if !decl.starts_with("fn ") {
                continue;
            }
            let sig_end = at + code[at..].find('{').expect("a fn has a body");
            let signature = &code[at..sig_end];
            let returns_label = signature
                .split_once("->")
                .is_some_and(|(_, ret)| ret.contains("&'static str"));
            if !returns_label {
                continue;
            }
            let name = decl["fn ".len()..]
                .split(['(', '<'])
                .next()
                .unwrap_or_default()
                .to_string();
            let body_end = code[sig_end..]
                .find("\n}")
                .map(|i| sig_end + i)
                .unwrap_or_else(|| panic!("no top-level close found for `{name}`"));
            let body = &code[sig_end..body_end];
            let mut labels = string_literals(body);
            labels.extend(
                consts
                    .iter()
                    .filter(|(c, _)| body.contains(c.as_str()))
                    .map(|(_, v)| v.clone()),
            );
            if labels.is_empty() {
                assert!(
                    body.contains("_mode_label("),
                    "`{name}` returns a label but this parser found none — it \
                     neither spells one as a literal (nor as a top-level const) \
                     nor delegates to a `*_mode_label` decider, so nothing here \
                     can grade what it emits. Give it literal arms, make it \
                     delegate, or teach this parser the shape."
                );
            }
            out.push((name, labels));
        }
        out
    }

    /// Derive the classified dimension from the PRODUCERS, never re-type it.
    #[test]
    fn every_run_mode_label_a_runner_can_emit_is_classified() {
        use crate::pipeline::run::{pool_mode_label, run_mode_label, wave_mode_label};
        let mut classified: std::collections::BTreeSet<&'static str> = Default::default();
        for m in [
            wave_mode_label(2),
            wave_mode_label(9),
            pool_mode_label(true),
            run_mode_label(2, true),
            run_mode_label(9, false),
        ] {
            classified.insert(m);
            assert!(
                mode_shares_the_source(m),
                "{m} is emitted for a run that overlapped exports"
            );
        }
        for m in [
            wave_mode_label(0),
            wave_mode_label(1),
            pool_mode_label(false),
            run_mode_label(0, true),
            run_mode_label(1, true),
            run_mode_label(0, false),
            run_mode_label(1, false),
        ] {
            classified.insert(m);
            assert!(
                !mode_shares_the_source(m),
                "{m} is emitted for a run whose exports ran one at a time — it must \
                 keep the actionable attribution"
            );
        }

        let producers = mode_label_producers();
        assert!(
            producers.len() >= 5,
            "the producer parser found {} label producers — expected at least the \
             three run-mode deciders plus the wave's and the pool's per-export \
             producers, so the parser has gone blind: {producers:?}",
            producers.len()
        );
        for (name, labels) in &producers {
            for label in labels {
                assert!(
                    classified.contains(label.as_str()),
                    "`{name}` can emit {label:?}, which no case above classifies — a new \
                     label must be added to the concurrent or the serial arm here, or it \
                     falls through `mode_shares_the_source`'s unknown arm and a serial run \
                     silently loses the actionable attribution"
                );
            }
        }

        const RETIRED: [&str; 1] = ["single"];
        let producible: std::collections::BTreeSet<&str> = producers
            .iter()
            .flat_map(|(_, l)| l.iter().map(String::as_str))
            .collect();
        let mine = code_only(include_str!("aggregate.rs"));
        let at = mine
            .find("fn mode_shares_the_source(")
            .expect("mode_shares_the_source moved — update the anchor");
        let end = mine[at..].find("\n}").map(|i| at + i).unwrap();
        for label in string_literals(&mine[at..end]) {
            assert!(
                producible.contains(label.as_str()) || RETIRED.contains(&label.as_str()),
                "`mode_shares_the_source` recognises {label:?}, which no producer emits — \
                 delete it or add it to RETIRED with the reason"
            );
        }
        for label in RETIRED {
            assert!(
                !producible.contains(label),
                "{label:?} has a producer now — drop it from RETIRED and classify it above"
            );
        }
    }

    #[test]
    fn format_rate_scales() {
        assert_eq!(format_rate(42.0), "42");
        assert_eq!(format_rate(1500.0), "1.5K");
        assert_eq!(format_rate(2_500_000.0), "2.5M");
    }

    #[test]
    fn truncate_respects_char_boundary_with_unicode() {
        let s = "αβγδ".repeat(100);
        let t = truncate(&s, 10);
        assert_eq!(t.chars().count(), 11);
    }

    /// A skipped run's entry carries its reason into `per_export[]`; any other keeps `null`.
    #[test]
    fn a_skipped_entry_carries_the_reason_the_run_skipped() {
        let mut s = RunSummary {
            export_name: "e".into(),
            status: "skipped".into(),
            skip_reason: Some("no new rows since cursor 'updated_at'".into()),
            ..Default::default()
        };
        let json = serde_json::to_value(entry_from_summary(&s)).unwrap();
        assert_eq!(json["skip_reason"], "no new rows since cursor 'updated_at'");
        s.status = "success".into();
        s.skip_reason = None;
        assert!(serde_json::to_value(entry_from_summary(&s)).unwrap()["skip_reason"].is_null());
    }

    fn metric(name: &str, status: &str) -> ExportMetric {
        ExportMetric {
            bytes_read: 0,
            source_type: None,
            skip_reason: None,
            export_name: name.into(),
            run_id: Some(format!("{name}_run")),
            run_at: "2026-06-09T12:00:00+00:00".into(),
            duration_ms: 1500,
            total_rows: 42,
            peak_rss_mb: Some(64),
            status: status.into(),
            error_message: if status == "failed" {
                Some("boom".into())
            } else {
                None
            },
            tuning_profile: Some("balanced".into()),
            format: Some("parquet".into()),
            mode: Some("full".into()),
            files_produced: 2,
            bytes_written: 4096,
            retries: 1,
            validated: Some(true),
            schema_changed: Some(false),
        }
    }

    #[test]
    fn metrics_to_json_empty_is_valid_array() {
        let json = metrics_to_json(&[]).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_array(), "empty metrics must serialize as []");
        assert_eq!(parsed.as_array().unwrap().len(), 0);
        assert!(!json.contains("No metrics recorded yet"));
    }

    #[test]
    fn metrics_to_json_carries_all_fields() {
        let json =
            metrics_to_json(&[metric("orders", "success"), metric("users", "failed")]).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let rows = parsed.as_array().unwrap();
        assert_eq!(rows.len(), 2);

        let first = &rows[0];
        for key in [
            "export_name",
            "run_id",
            "run_at",
            "duration_ms",
            "total_rows",
            "peak_rss_mb",
            "status",
            "error_message",
            "tuning_profile",
            "format",
            "mode",
            "files_produced",
            "bytes_written",
            "retries",
            "validated",
            "schema_changed",
        ] {
            assert!(
                first.get(key).is_some(),
                "metrics JSON row must carry `{key}`; got {first}"
            );
        }
        assert_eq!(first["export_name"], "orders");
        assert_eq!(first["status"], "success");
        assert_eq!(first["total_rows"], 42);
        assert_eq!(first["files_produced"], 2);
        assert_eq!(first["validated"], true);
        assert!(first["error_message"].is_null());
        assert_eq!(rows[1]["status"], "failed");
        assert_eq!(rows[1]["error_message"], "boom");
    }

    #[test]
    fn persist_records_to_state_and_writes_file() {
        use crate::state::StateStore;
        let s = StateStore::open_in_memory().unwrap();
        let now = Utc::now();
        let agg = build(
            vec![entry("a", "success", 10, 1, 100)],
            now - Duration::seconds(5),
            now,
            Some("test.yaml"),
            "sequential",
        );

        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("nested").join("summary.json");

        persist(&s, &agg, Some(&out));

        let rows = s.get_recent_run_aggregates(1).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].run_aggregate_id, agg.run_aggregate_id);
        assert_eq!(rows[0].total_rows, 10);

        let body = std::fs::read_to_string(&out).unwrap();
        let round: RunAggregate = serde_json::from_str(&body).unwrap();
        assert_eq!(round.run_aggregate_id, agg.run_aggregate_id);
        assert_eq!(round.per_export.len(), 1);
        assert_eq!(round.per_export[0].export_name, "a");
    }

    #[test]
    fn an_unwritable_aggregate_path_is_refused_by_code() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let agg = build(Vec::new(), Utc::now(), Utc::now(), None, "sequential");
        let err = write_json(&file.join("sub").join("agg.json"), &agg).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_CLI_OUTPUT_UNWRITABLE", 1);
    }

    /// A path whose text reads like a network word (`dns`, `reset`) is still a file error.
    #[test]
    fn an_unwritable_path_named_like_a_network_failure_is_not_transient() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["dns", "connection reset", "broken pipe"] {
            let file = dir.path().join(name);
            std::fs::write(&file, b"x").unwrap();
            let agg = build(Vec::new(), Utc::now(), Utc::now(), None, "sequential");
            let err = write_json(&file.join("sub").join("agg.json"), &agg).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_CLI_OUTPUT_UNWRITABLE", 1);
        }
    }
}
