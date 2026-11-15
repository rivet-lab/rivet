//! **Layer: Observability**

use super::ipc::{self, ChildEvent};
use super::{format_bytes, multi_export_mode, strip_chunked_recovery_hint};
use crate::journal::{PlanSnapshot, RunEvent, RunJournal};
use crate::manifest::ManifestPart;
use crate::plan::ResolvedRunPlan;

/// Build a `PlanSnapshot` from a `ResolvedRunPlan`.
fn plan_snapshot_from(plan: &ResolvedRunPlan) -> PlanSnapshot {
    PlanSnapshot {
        export_name: plan.export_name.clone(),
        base_query: plan.base_query.clone(),
        strategy: plan.strategy.mode_label().to_string(),
        format: plan.format.label().to_string(),
        compression: plan.compression.label().to_string(),
        destination_type: plan.destination.destination_type.label().to_string(),
        tuning_profile: plan.tuning_profile_label.clone(),
        batch_size: plan.tuning.batch_size,
        validate: plan.validate,
        reconcile: plan.reconcile,
        resume: plan.resume,
        chunk_key: plan.strategy.chunk_key().map(|c| c.to_string()),
        resumable: plan.strategy.is_resumable(),
        row_hash: crate::enrich::RowHashContract::of(&plan.meta_columns.row_hash),
    }
}

/// Context recorded when a run was launched via `rivet apply` rather than `rivet run`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ApplyContext {
    /// `plan_id` from the applied `PlanArtifact`.
    pub plan_id: String,
    /// Was `--force` passed to `rivet apply`?
    pub forced: bool,
    /// Names of preflight checks that `--force` actually overrode for this run.
    pub force_bypassed: Vec<String>,
}

/// Accumulates operational data during a pipeline run for summary and metrics.
#[derive(Debug, Clone, Default)]
pub struct RunSummary {
    pub run_id: String,
    pub export_name: String,
    pub status: String,
    pub total_rows: i64,
    pub files_produced: usize,
    pub bytes_written: u64,
    /// Decoded bytes READ from the source this run (harvested once from the plan's shared counter by `run_export_job` after the runner returns.
    pub bytes_read: u64,
    /// Per column, source cells written as NULL because the target type cannot hold them (MySQL zero dates).
    pub nulled_cells: Vec<(String, u64)>,
    /// Incremented after each successful `dest.write()`.
    pub files_committed: usize,
    /// Parts adopted from a prior run on resume (included in `files_committed`).
    pub files_adopted: usize,
    pub duration_ms: i64,
    pub peak_rss_mb: i64,
    pub retries: u32,
    /// Of `retries`, how many opened a FRESH source connection (a reconnect-class error.
    pub reconnects: u32,
    /// True when this run picked up a prior CRASHED run instead of starting fresh (keyset via the in-progress run_id, chunked via `--resume`).
    pub resumed: bool,
    /// True when this run's chunk boundaries came from a plan artifact (`ChunkSource::Precomputed`), i.e.
    pub chunks_precomputed: bool,
    /// True once this run entered `run_export` — the shared batch-runner dispatch choke point, which always has a `StateStore`.
    pub state_backed: bool,
    pub validated: Option<bool>,
    pub schema_changed: Option<bool>,
    pub quality_passed: Option<bool>,
    /// Form B per-column value checksums (name-keyed), harvested from the sink.
    pub column_checksums: Vec<crate::manifest::ColumnChecksum>,
    /// The column the Form B checksum is keyed to (cursor/key column); `None` = un-keyed.
    pub checksum_key_column: Option<String>,
    /// Set when a checkpoint RESUME hydrated pre-crash parts into `manifest_parts` whose per-column checksum contribution is unrecoverable (file_log stores no per-column checksums, and the prior manifest keeps only the run-wide XOR, not per-part).
    pub column_checksums_incomplete: bool,
    /// ADR-0029: WHY the coverage was short, when it was.
    pub column_checksums_short_cover: bool,
    /// ADR-0028: the run-wide tail ledger the runners FEED as they commit (schema seen, per-part checksums, shape bytes) and that `finalize::finalize_export`.
    pub(crate) ledger: super::commit::CommitLedger,
    /// Cursor range this run covered (incremental strategies).
    pub cursor_column: Option<String>,
    pub cursor_low: Option<String>,
    pub cursor_high: Option<String>,
    /// The source columns as the run resolved them at OPEN — what the load spec records.
    pub open_mappings: Option<Vec<crate::types::TypeMapping>>,
    /// The source primary key of the run's table, read at OPEN with the mappings.
    pub open_primary_key: Option<Vec<String>>,
    /// The columns the source declares finer than a microsecond, read at OPEN with the mappings.
    pub open_finer_than_microsecond: Vec<String>,
    pub error_message: Option<String>,
    /// v18 failure forensics.
    pub offending_value: Option<String>,
    pub server_context_json: Option<String>,
    /// The keyset/chunk key's SOURCE native type (`bigint unsigned`, `numeric(11,4)`), resolved by the open probe.
    pub key_native_type: Option<String>,
    /// `profile` from YAML, or `balanced (default)` if omitted.
    pub tuning_profile: String,
    /// Configured `batch_size` from YAML/profile (FETCH cap before `batch_size_memory_mb` override).
    pub batch_size: usize,
    /// When set, actual FETCH size is derived from schema (see logs).
    pub batch_size_memory_mb: Option<usize>,
    pub format: String,
    pub mode: String,
    pub compression: String,
    /// Where the files were written, as an operator would type it to find them again (`./output`, `s3://bucket/prefix`, …).
    pub destination_uri: Option<String>,
    /// Postgres `pg_stat_database.temp_bytes` delta around the run.
    pub pg_temp_bytes_delta: Option<i64>,
    /// Human-readable parenthetical attached to `status.
    pub skip_reason: Option<String>,
    /// Source COUNT(*) result for reconciliation (None = not requested or not applicable).
    pub source_count: Option<i64>,
    /// Whether reconciliation passed (Some(true) = match, Some(false) = mismatch, None = skipped).
    pub reconciled: Option<bool>,
    /// Committed parts accumulated during the run, in commit order.
    pub manifest_parts: Vec<ManifestPart>,
    /// xxh3 fingerprint of the dest-facing column schema for this run, in the canonical `xxh3:<16-hex>` form produced by [`crate::state::schema_fingerprint`].
    pub schema_fingerprint: Option<String>,
    /// Result of the manifest-aware `--validate` pass (ADR-0012 M5/M6, ADR-0013).
    pub manifest_verification: Option<crate::pipeline::validate_manifest::ManifestVerification>,
    /// Apply-time context (plan_id, --force usage, bypassed checks).
    pub apply_context: Option<ApplyContext>,
    /// Structured event log for this run.
    pub journal: RunJournal,
    /// The `initial: snapshot` legs a CDC run ran in this invocation; folded into its aggregate entry only.
    pub snapshot_legs: Vec<crate::state::RunAggregateEntry>,
}

/// One `(label, value)` line in the rendered summary block.
type Row = (&'static str, String);

/// A run id: `<export>_<UTC ms>_<pid>`.
pub(crate) fn fresh_run_id(export_name: &str) -> String {
    format!(
        "{}_{}_{}",
        export_name,
        chrono::Utc::now().format("%Y%m%dT%H%M%S%.3f"),
        std::process::id()
    )
}

/// `s` as one part-name segment: anything outside `[A-Za-z0-9_-]` becomes `_`.
fn sanitize_run_id(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// The run-unique segment of a keyset part name: the sanitized run id without its redundant `<export>_` prefix.
pub(crate) fn run_scoped_tag(run_id: &str, export_name: &str) -> String {
    let tag = sanitize_run_id(run_id);
    let prefix = format!("{}_", sanitize_run_id(export_name));
    tag.strip_prefix(&prefix).unwrap_or(&tag).to_string()
}

/// The run-unique stamp of a single-runner or Mongo-parallel part name, `<YYYYMMDD>_<HHMMSS>_<mmm>_<pid>_<nonce>`.
pub(crate) fn run_scoped_stamp(run_id: &str, export_name: &str) -> String {
    use rand::RngExt;
    let tag = run_scoped_tag(run_id, export_name);
    let stamp = match tag.split_once('T') {
        Some((date, rest)) if date.len() == 8 && date.bytes().all(|b| b.is_ascii_digit()) => {
            format!("{date}_{rest}")
        }
        _ => tag,
    };
    format!("{stamp}_{:016x}", rand::rng().random::<u64>())
}

impl RunSummary {
    /// Parts this run committed itself — not the ones a resume adopted.
    pub(super) fn files_committed_here(&self) -> usize {
        self.files_committed.saturating_sub(self.files_adopted)
    }

    pub(super) fn new(plan: &ResolvedRunPlan) -> Self {
        let run_id = fresh_run_id(&plan.export_name);
        let mut journal = RunJournal::new(&run_id, &plan.export_name);
        journal.record(RunEvent::PlanResolved(Box::new(plan_snapshot_from(plan))));

        ipc::emit_event(&ChildEvent::Started {
            export_name: plan.export_name.clone(),
            run_id: run_id.clone(),
            mode: plan.strategy.mode_label().to_string(),
            tuning_profile: plan.tuning_profile_label.clone(),
            batch_size: plan.tuning.batch_size,
        });

        Self {
            run_id,
            export_name: plan.export_name.clone(),
            status: "running".into(),
            total_rows: 0,
            files_produced: 0,
            bytes_written: 0,
            bytes_read: 0,
            nulled_cells: Vec::new(),
            files_committed: 0,
            files_adopted: 0,
            duration_ms: 0,
            peak_rss_mb: 0,
            retries: 0,
            reconnects: 0,
            resumed: false,
            chunks_precomputed: false,
            state_backed: false,
            validated: None,
            schema_changed: None,
            quality_passed: None,
            error_message: None,
            offending_value: None,
            server_context_json: None,
            key_native_type: None,
            tuning_profile: plan.tuning_profile_label.clone(),
            batch_size: plan.tuning.batch_size,
            batch_size_memory_mb: plan.tuning.batch_size_memory_mb,
            format: plan.format.label().to_string(),
            mode: plan.strategy.mode_label().to_string(),
            compression: plan.compression.label().to_string(),
            destination_uri: (!matches!(
                plan.destination.destination_type,
                crate::config::DestinationType::Stdout
            ))
            .then(|| crate::pipeline::finalize::destination_uri_for_manifest(&plan.destination)),
            pg_temp_bytes_delta: None,
            skip_reason: None,
            source_count: None,
            reconciled: None,
            manifest_parts: Vec::new(),
            schema_fingerprint: None,
            manifest_verification: None,
            apply_context: None,
            snapshot_legs: Vec::new(),
            column_checksums: Vec::new(),
            column_checksums_incomplete: false,
            column_checksums_short_cover: false,
            ledger: Default::default(),
            checksum_key_column: None,
            cursor_column: None,
            cursor_low: None,
            cursor_high: None,
            open_mappings: None,
            open_primary_key: None,
            open_finer_than_microsecond: Vec::new(),
            journal,
        }
    }

    /// One canonical builder for tests across the crate + integration suite.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn stub_for_testing(run_id: impl Into<String>, export_name: impl Into<String>) -> Self {
        let run_id = run_id.into();
        let export_name = export_name.into();
        let journal = RunJournal::new(&run_id, &export_name);
        Self {
            run_id,
            export_name,
            status: "running".into(),
            total_rows: 0,
            files_produced: 0,
            bytes_written: 0,
            bytes_read: 0,
            nulled_cells: Vec::new(),
            files_committed: 0,
            files_adopted: 0,
            duration_ms: 0,
            peak_rss_mb: 0,
            retries: 0,
            reconnects: 0,
            resumed: false,
            chunks_precomputed: false,
            state_backed: false,
            validated: None,
            schema_changed: None,
            quality_passed: None,
            error_message: None,
            offending_value: None,
            server_context_json: None,
            key_native_type: None,
            tuning_profile: "balanced".into(),
            batch_size: 1000,
            batch_size_memory_mb: None,
            format: "parquet".into(),
            mode: "snapshot".into(),
            compression: "zstd".into(),
            destination_uri: None,
            pg_temp_bytes_delta: None,
            skip_reason: None,
            source_count: None,
            reconciled: None,
            manifest_parts: Vec::new(),
            schema_fingerprint: None,
            manifest_verification: None,
            apply_context: None,
            snapshot_legs: Vec::new(),
            column_checksums: Vec::new(),
            column_checksums_incomplete: false,
            column_checksums_short_cover: false,
            ledger: Default::default(),
            checksum_key_column: None,
            cursor_column: None,
            cursor_low: None,
            cursor_high: None,
            open_mappings: None,
            open_primary_key: None,
            open_finer_than_microsecond: Vec::new(),
            journal,
        }
    }

    /// Test-only chainable setter for the run's status field.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_status(mut self, status: impl Into<String>) -> Self {
        let s = status.into();
        if (s == "success" || s == "failed") && self.journal.final_outcome().is_none() {
            self.journal.record(RunEvent::RunCompleted {
                status: s.clone(),
                error_message: self.error_message.clone(),
                duration_ms: self.duration_ms,
            });
        }
        self.status = s;
        self
    }

    /// Test-only setter — replace the recorded manifest parts (and adjust `total_rows` / `bytes_written` / `files_produced` to keep them consistent with the parts list, the way real production code does).
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_manifest_parts(mut self, parts: Vec<crate::manifest::ManifestPart>) -> Self {
        self.total_rows = parts.iter().map(|p| p.rows).sum();
        self.bytes_written = parts.iter().map(|p| p.size_bytes).sum();
        self.files_produced = parts.len();
        self.files_committed = parts.len();
        self.manifest_parts = parts;
        self
    }

    /// Test-only setter — error_message + (optionally) status.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_error(mut self, msg: impl Into<String>) -> Self {
        self.error_message = Some(msg.into());
        self
    }

    /// Test-only setter — record a PlanResolved event in the journal so downstream observability paths (`journal.plan_snapshot()`, `RunReport::from_summary` plan_origin lookup) see the same shape they would on a real run.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_plan_snapshot(mut self, snap: PlanSnapshot) -> Self {
        self.journal.record(RunEvent::PlanResolved(Box::new(snap)));
        self
    }

    pub(super) fn print(&self) {
        if ipc::capturing_events() {
            self.emit_finished();
            return;
        }

        self.print_stderr_block();
    }

    /// Report this run's outcome as a `Finished` event; a no-op outside capturing mode.
    pub(super) fn emit_finished(&self) {
        ipc::emit_event(&ChildEvent::Finished {
            export_name: self.export_name.clone(),
            run_id: self.run_id.clone(),
            status: self.status.clone(),
            total_rows: self.total_rows,
            files_produced: self.files_produced as u64,
            bytes_written: self.bytes_written,
            duration_ms: self.duration_ms,
            peak_rss_mb: self.peak_rss_mb,
            error_message: self.error_message.clone(),
            dest_retries: crate::destination::transient_retries_total(),
            bytes_read: self.bytes_read,
            mode: self.mode.clone(),
        });
    }

    /// Write the per-export summary block to stderr, bypassing IPC capture.
    pub(super) fn print_stderr_block(&self) {
        let block = if multi_export_mode() {
            self.render_compact()
        } else {
            self.render().trim_end_matches('\n').to_string()
        };

        use std::io::Write;
        let mut buf = super::parent_ui::sanitize_terminal(&block);
        buf.push('\n');
        let stderr = std::io::stderr();
        let mut handle = stderr.lock();
        let _ = handle.write_all(buf.as_bytes());
        let _ = handle.flush();
    }

    /// Compact one-line summary used when several exports run in the same invocation.
    fn render_compact(&self) -> String {
        const NAME_COL: usize = 22;
        const MODE_COL: usize = 8;
        let icon = match self.status.as_str() {
            "success" => "✓",
            "failed" => "✗",
            _ => "•",
        };
        let body = if self.status == "failed" {
            let err = self
                .error_message
                .as_deref()
                .unwrap_or("(no error message recorded)");
            let (cause, _) = strip_chunked_recovery_hint(err);
            compact_error(cause)
        } else {
            let rss = if self.peak_rss_mb > 0 {
                format!("  RSS {} MB", fmt_thousands(self.peak_rss_mb))
            } else {
                String::new()
            };
            format!(
                "{} rows  {} files  {}  {}{}",
                fmt_thousands(self.total_rows),
                fmt_thousands(self.files_produced as i64),
                format_bytes(self.bytes_written),
                fmt_duration_ms(self.duration_ms),
                rss
            )
        };
        format!(
            "{} {:<name$}  {:<mode$}  {}",
            icon,
            self.export_name,
            self.mode,
            body,
            name = NAME_COL,
            mode = MODE_COL,
        )
    }

    /// Build the block as a string.
    fn render(&self) -> String {
        let mut rows: Vec<Row> = Vec::with_capacity(16);
        rows.push(("run_id", self.run_id.clone()));
        rows.push(self.status_row());
        rows.push(self.tuning_row());
        rows.push(("rows", fmt_thousands(self.total_rows)));
        rows.push(("files", fmt_thousands(self.files_produced as i64)));
        rows.extend(self.output_row());
        rows.extend(self.position_row());
        rows.extend(self.bytes_read_row());
        rows.extend(self.fidelity_row());
        rows.extend(self.bytes_row());
        rows.push(("duration", fmt_duration_ms(self.duration_ms)));
        rows.extend(self.peak_rss_row());
        rows.extend(self.pg_temp_spill_row());
        rows.extend(self.compression_row());
        rows.extend(self.retries_row());
        rows.extend(dest_retries_row());
        rows.extend(self.outcome_rows());
        rows.extend(self.error_row());
        format_block(&self.export_name, &rows)
    }

    /// `status`, annotated with the skip reason when the run was skipped.
    fn status_row(&self) -> Row {
        let value = match (&self.status, &self.skip_reason) {
            (s, Some(reason)) if s == "skipped" => format!("{s} ({reason})"),
            (s, _) => s.clone(),
        };
        ("status", value)
    }

    /// `tuning` — profile + configured batch_size, noting the memory-derived FETCH override when `batch_size_memory_mb` is set.
    fn tuning_row(&self) -> Row {
        let value = match self.batch_size_memory_mb {
            Some(mem) => format!(
                "profile={}, batch_size={} (batch_size_memory_mb={}MiB → effective FETCH in logs)",
                self.tuning_profile,
                fmt_thousands(self.batch_size as i64),
                mem
            ),
            None => format!(
                "profile={}, batch_size={}",
                self.tuning_profile,
                fmt_thousands(self.batch_size as i64)
            ),
        };
        ("tuning", value)
    }

    /// `output` — where the files landed, so a newcomer isn't left guessing after a successful export.
    fn output_row(&self) -> Option<Row> {
        if self.files_produced > 0 {
            self.destination_uri.clone().map(|uri| ("output", uri))
        } else {
            None
        }
    }

    /// `cursor` (incremental) xor `window` (time_window): the position line for a 0-row run.
    fn position_row(&self) -> Option<Row> {
        if let Some(pos) = incremental_position_line(self.skip_reason.as_deref()) {
            Some(("cursor", pos))
        } else {
            time_window_skip_line(&self.mode, self.skip_reason.as_deref()).map(|w| ("window", w))
        }
    }

    /// `bytes read` / `bytes written` — SEPARATE rows (read is the source leg's decoded volume, written the parquet leg's.
    fn bytes_row(&self) -> Option<Row> {
        if self.bytes_written > 0 {
            Some(("bytes written", format_bytes(self.bytes_written)))
        } else {
            None
        }
    }

    /// `fidelity` — the cells written as NULL because the target type cannot hold them, per column.
    fn fidelity_row(&self) -> Option<Row> {
        if self.nulled_cells.is_empty() {
            return None;
        }
        let cols: Vec<String> = self
            .nulled_cells
            .iter()
            .map(|(c, n)| format!("{c}={}", fmt_thousands(*n as i64)))
            .collect();
        Some((
            "fidelity",
            format!("zero or partial dates written as NULL: {}", cols.join(", ")),
        ))
    }

    fn bytes_read_row(&self) -> Option<Row> {
        if self.bytes_read > 0 {
            Some(("bytes read", format_bytes(self.bytes_read)))
        } else {
            None
        }
    }

    /// `peak RSS` — only when sampled during the run.
    fn peak_rss_row(&self) -> Option<Row> {
        if self.peak_rss_mb > 0 {
            Some((
                "peak RSS",
                format!(
                    "{} MB (sampled during run)",
                    fmt_thousands(self.peak_rss_mb)
                ),
            ))
        } else {
            None
        }
    }

    /// `pg temp spill` — PostgreSQL temp-file spill around the run.
    fn pg_temp_spill_row(&self) -> Option<Row> {
        let temp = self.pg_temp_bytes_delta?;
        if temp <= 0 {
            return None;
        }
        let temp_mb = temp as f64 / (1024.0 * 1024.0);
        let label = if temp > 100 * 1024 * 1024 {
            format!(
                "{:.1} MB ⚠ shrink tuning.batch_size or set batch_size_memory_mb",
                temp_mb
            )
        } else {
            format!("{:.1} MB", temp_mb)
        };
        Some(("pg temp spill", label))
    }

    /// `compression` — only when it differs from the parquet default (zstd).
    fn compression_row(&self) -> Option<Row> {
        if self.format == "parquet" && self.compression != "zstd" {
            Some(("compression", self.compression.clone()))
        } else {
            None
        }
    }

    /// `retries` — only when the run had to retry.
    fn retries_row(&self) -> Option<Row> {
        if self.retries > 0 {
            Some(("retries", self.retries.to_string()))
        } else {
            None
        }
    }

    /// Post-extraction check outcomes: `--validate`, schema-drift, the quality gate, `--reconcile`, plus the advisory verify nudge.
    fn outcome_rows(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = Vec::new();
        if let Some(v) = self.validated {
            rows.push(("validated", if v { "pass".into() } else { "FAIL".into() }));
        }
        if let Some(sc) = self.schema_changed {
            rows.push((
                "schema",
                if sc {
                    "CHANGED".into()
                } else {
                    "unchanged".into()
                },
            ));
        }
        if let Some(q) = self.quality_passed {
            rows.push(("quality", if q { "pass".into() } else { "FAIL".into() }));
        }
        if let Some(reconciled) = self.reconciled {
            let src = self
                .source_count
                .map(fmt_thousands)
                .unwrap_or_else(|| "?".into());
            let exported = fmt_thousands(self.total_rows);
            let value = if reconciled {
                format!("MATCH ({exported}/{src})")
            } else {
                format!("MISMATCH (exported {exported} vs source {src})")
            };
            rows.push(("reconcile", value));
        }
        if self.status == "success"
            && self.files_produced > 0
            && self.validated.is_none()
            && self.reconciled.is_none()
        {
            rows.push((
                "verify",
                "not run — add `--reconcile` (count vs source) or `rivet validate` (re-read outputs)"
                    .into(),
            ));
        }
        rows
    }

    /// `error` — the failure message, with its own multi-line structure preserved (the detailed block indents continuation lines under the value column.
    fn error_row(&self) -> Option<Row> {
        self.error_message
            .as_ref()
            .map(|err| ("error", err.trim_end().to_string()))
    }

    /// Sanity-check the post-run summary ↔ manifest_parts coherence.
    pub fn check_post_run_invariants(&self, is_resume_run: bool) -> Result<(), String> {
        let parts_bytes: u64 = self.manifest_parts.iter().map(|p| p.size_bytes).sum();

        if self.files_committed > self.manifest_parts.len() {
            return Err(format!(
                "summary.files_committed ({}) > manifest_parts.len() ({}) — \
                 a runner bumped files_committed without commit::record_part. \
                 With an EMPTY part list this is also the ADR-0012 M1 gap: the \
                 cloud manifest would ship with no parts (what parallel_checkpoint \
                 did before e9b0796)",
                self.files_committed,
                self.manifest_parts.len()
            ));
        }
        if self.files_produced > self.manifest_parts.len() {
            return Err(format!(
                "summary.files_produced ({}) > manifest_parts.len() ({}) — \
                 a runner bumped files_produced without commit::record_part",
                self.files_produced,
                self.manifest_parts.len()
            ));
        }
        if self.bytes_written > parts_bytes {
            return Err(format!(
                "summary.bytes_written ({}) > sum(manifest_parts.size_bytes) ({}) — \
                 a runner bumped bytes_written without commit::record_part",
                self.bytes_written, parts_bytes
            ));
        }
        if self.status == "success" && self.total_rows > 0 && self.files_committed == 0 {
            return Err(format!(
                "summary.total_rows={} but files_committed=0 — rows extracted from \
                 source but no files committed (no output reached the destination)",
                self.total_rows
            ));
        }
        if self.state_backed && self.status == "success" && self.files_committed > 0 {
            if !self.resumed && !is_resume_run && self.schema_changed.is_none() {
                return Err(
                    "state_backed success committed parts but schema_changed is None — \
                     the on_schema_drift gate was never applied (no runner called \
                     check_from_sink_schema / check_from_type_mappings). This is the \
                     runner-bypass class: a runner owning its loop skipped the facade."
                        .to_string(),
                );
            }
            if self.format == "parquet"
                && self.column_checksums.is_empty()
                && !self.column_checksums_incomplete
            {
                return Err(
                    "state_backed success committed parts but column_checksums is empty \
                     and not flagged incomplete — Form-B was never harvested (no runner \
                     called harvest_column_checksums). Runner-bypass class."
                        .to_string(),
                );
            }
            if self.column_checksums_short_cover {
                return Err(
                    "state_backed success committed parts whose commit unit contributed NO \
                     Form-B checksums, so the harvest suppressed the record (ADR-0029 \
                     computed coverage). On a successful run every unit that recorded a part \
                     also committed it, so this is a UnitId mismatch between \
                     commit::record_part and CommitLedger::contribute_checksums in the \
                     runner — pair them on the same unit. Suppressing here is fail-safe but \
                     SILENT: `validate --depth full` would stop re-reading values for every \
                     run of this runner."
                        .to_string(),
                );
            }
        }
        Ok(())
    }
}

/// Reduce a possibly-multi-line execution error to a single-line, bounded- length cause suitable for the per-export summary block and the compact one-liner.
fn compact_error(raw: &str) -> String {
    const MAX_CHARS: usize = 240;
    if let Some(summary) = summarize_parallel_chunk_errors(raw) {
        return clamp_chars(&summary, MAX_CHARS);
    }
    let collapsed: String = raw
        .lines()
        .map(str::trim_end)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("; ");
    clamp_chars(&collapsed, MAX_CHARS)
}

/// Derive the summary block's `cursor:` line from a `skip_reason`.
fn incremental_position_line(skip_reason: Option<&str>) -> Option<String> {
    let col = skip_reason?
        .strip_prefix("no new rows since cursor '")?
        .strip_suffix('\'')?;
    Some(format!("'{col}' unchanged (no new rows this run)"))
}

/// Derive the summary block's `window:` line for a time_window run that returned nothing.
fn time_window_skip_line(mode: &str, skip_reason: Option<&str>) -> Option<String> {
    skip_reason?;
    if mode != "timewindow" {
        return None;
    }
    Some("rolling time window matched no rows — check `time_column`/`days_window`".to_string())
}

/// The summary block's `dest retries:` row — transient destination-side retry attempts the OpenDAL `RetryLayer` scheduled this process, or `None` when there were none.
fn dest_retries_row() -> Option<Row> {
    crate::destination::transient_retries_summary(crate::destination::transient_retries_total())
        .map(|v| ("dest retries", v))
}

fn summarize_parallel_chunk_errors(raw: &str) -> Option<String> {
    let header_pos = raw.find("parallel checkpoint worker errors:")?;
    let prefix = raw[..header_pos].trim_end_matches(": ").trim_end();
    let tail = &raw[header_pos + "parallel checkpoint worker errors:".len()..];

    let chunk_lines: Vec<&str> = tail
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("chunk "))
        .collect();
    if chunk_lines.is_empty() {
        return None;
    }
    let first_chunk_full = chunk_lines[0];
    let first_chunk_short = clamp_chars(first_chunk_full, 140);
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{}: ", prefix)
    };
    Some(format!(
        "{}parallel checkpoint workers failed: {} chunk(s) ({}); see stderr for full payloads",
        prefix,
        chunk_lines.len(),
        first_chunk_short
    ))
}

fn clamp_chars(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let keep = max_chars.saturating_sub(1);
    let mut out: String = s.chars().take(keep).collect();
    out.push('…');
    out
}

/// Render a `── name ─────…─` header plus one indented `label.
fn format_block(name: &str, rows: &[(&str, String)]) -> String {
    const HEADER_WIDTH: usize = 60;
    let label_w = rows.iter().map(|(l, _)| l.len()).max().unwrap_or(0);

    let prefix = format!("── {} ", name);
    let prefix_chars = prefix.chars().count();
    let dashes = HEADER_WIDTH.saturating_sub(prefix_chars);
    let mut out = String::with_capacity(HEADER_WIDTH * (rows.len() + 3));
    out.push('\n');
    out.push_str(&prefix);
    for _ in 0..dashes {
        out.push('─');
    }
    out.push('\n');
    let value_indent = " ".repeat(2 + (label_w + 1) + 2);
    for (label, value) in rows {
        let mut lines = value.split('\n');
        let first = lines.next().unwrap_or("");
        out.push_str(&format!(
            "  {:<width$}  {}\n",
            format!("{label}:"),
            first,
            width = label_w + 1
        ));
        for cont in lines {
            out.push_str(&value_indent);
            out.push_str(cont);
            out.push('\n');
        }
    }
    out
}

fn fmt_duration_ms(ms: i64) -> String {
    if ms < 1000 {
        return format!("{}ms", ms);
    }
    let total_secs = ms / 1000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s_frac = (ms % 60_000) as f64 / 1000.0;
    if h > 0 {
        format!("{}h {:02}m {:04.1}s", h, m, s_frac)
    } else if m > 0 {
        format!("{}m {:04.1}s", m, s_frac)
    } else {
        format!("{:.1}s", ms as f64 / 1000.0)
    }
}

/// Format integers with a comma every three digits.
fn fmt_thousands(n: i64) -> String {
    let abs = n.unsigned_abs();
    let s = abs.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3 + 1);
    if n < 0 {
        out.push('-');
    }
    for (i, b) in bytes.iter().enumerate() {
        let from_end = bytes.len() - i;
        if i > 0 && from_end.is_multiple_of(3) {
            out.push(',');
        }
        out.push(*b as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::test_support::test_plan;

    #[test]
    fn run_scoped_tag_strips_a_redundant_export_prefix_but_keeps_a_bare_run_id() {
        assert_eq!(
            run_scoped_tag(
                "aa_bonus_conversions_usd_20260820T104554088",
                "aa_bonus_conversions_usd"
            ),
            "20260820T104554088",
            "the leading <export>_ must be stripped so the part name is not <export>_<export>_<stamp>"
        );
        assert_eq!(run_scoped_tag("run-1", "exp"), "run-1");
        assert_ne!(
            run_scoped_tag("e_20260820T104554088", "e"),
            run_scoped_tag("e_20260820T104554090", "e")
        );
    }

    #[test]
    fn sanitize_run_id_keeps_safe_chars_and_replaces_the_rest() {
        assert_eq!(sanitize_run_id("run-2026_01A9"), "run-2026_01A9");
        assert_eq!(sanitize_run_id("a/b c:d.e"), "a_b_c_d_e");
        assert_eq!(sanitize_run_id("../etc"), "___etc");
        assert_eq!(sanitize_run_id("ABCabc012"), "ABCabc012");
    }

    /// The stamp without its nonce, which must be 16 hex digits.
    fn sans_nonce(stamp: &str) -> &str {
        let (head, nonce) = stamp.rsplit_once('_').expect("a nonce field");
        assert!(
            nonce.len() == 16 && nonce.bytes().all(|b| b.is_ascii_hexdigit()),
            "{stamp}"
        );
        head
    }

    #[test]
    fn a_part_stamp_is_the_run_id_in_the_documented_shape_then_a_nonce() {
        let id = "orders_20261008T174117.653_33972";
        assert_eq!(
            sans_nonce(&run_scoped_stamp(id, "orders")),
            "20261008_174117_653_33972"
        );
        let fresh = fresh_run_id("orders");
        let stamp = run_scoped_stamp(&fresh, "orders");
        assert!(
            sans_nonce(&stamp).ends_with(&format!("_{}", std::process::id()))
                && !stamp.contains(['T', '.']),
            "{fresh} -> {stamp}"
        );
        for other in ["run-1", "abcdefghT1", "123T4", "Test_run"] {
            assert_eq!(
                sans_nonce(&run_scoped_stamp(other, "orders")),
                other,
                "not a run stamp"
            );
        }
    }

    #[test]
    fn two_part_stamps_of_one_millisecond_and_one_pid_differ() {
        let id = "orders_20261008T174117.653_1";
        let stamps: std::collections::BTreeSet<String> =
            (0..64).map(|_| run_scoped_stamp(id, "orders")).collect();
        assert_eq!(
            stamps.len(),
            64,
            "two containers each run pid 1: the run id is the same, the part name must not be"
        );
    }

    #[test]
    fn fmt_thousands_handles_small_and_large() {
        assert_eq!(fmt_thousands(0), "0");
        assert_eq!(fmt_thousands(7), "7");
        assert_eq!(fmt_thousands(999), "999");
        assert_eq!(fmt_thousands(1_000), "1,000");
        assert_eq!(fmt_thousands(1_000_908), "1,000,908");
        assert_eq!(fmt_thousands(39_990_376), "39,990,376");
        assert_eq!(fmt_thousands(-1_234), "-1,234");
        assert_eq!(fmt_thousands(i64::MAX), "9,223,372,036,854,775,807");
    }

    #[test]
    fn fmt_duration_picks_unit() {
        assert_eq!(fmt_duration_ms(0), "0ms");
        assert_eq!(fmt_duration_ms(800), "800ms");
        assert_eq!(fmt_duration_ms(1_500), "1.5s");
        assert_eq!(fmt_duration_ms(68_400), "1m 08.4s");
        assert_eq!(fmt_duration_ms(3_725_300), "1h 02m 05.3s");
    }

    #[test]
    fn format_block_pads_labels_uniformly() {
        let rows = vec![
            ("run_id", "abc".to_string()),
            ("rows", "42".to_string()),
            ("compression", "zstd".to_string()),
        ];
        let out = format_block("orders", &rows);

        let lines: Vec<&str> = out.lines().filter(|l| l.contains(':')).collect();
        assert_eq!(lines.len(), 3);
        let value_starts: Vec<usize> = lines
            .iter()
            .map(|l| l.find(':').unwrap() + l[l.find(':').unwrap()..].find(' ').unwrap())
            .collect();
        let value_col = lines[0].rfind("abc").unwrap();
        assert_eq!(lines[1].rfind("42").unwrap(), value_col);
        assert_eq!(lines[2].rfind("zstd").unwrap(), value_col);
        let _ = value_starts;
    }

    #[test]
    fn format_block_header_has_consistent_width() {
        let block_a = format_block("a", &[("rows", "1".into())]);
        let block_b = format_block("orders_table_xyz", &[("rows", "1".into())]);
        let header_a = block_a.lines().nth(1).unwrap();
        let header_b = block_b.lines().nth(1).unwrap();
        assert_eq!(
            header_a.chars().count(),
            header_b.chars().count(),
            "headers must be the same width regardless of name length: {:?} vs {:?}",
            header_a,
            header_b
        );
    }

    #[test]
    fn render_produces_a_single_string_with_trailing_newline() {
        use crate::plan::{CompressionType, DestinationConfig, DestinationType, ResolvedRunPlan};
        let plan = ResolvedRunPlan {
            compression: CompressionType::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("./out".into()),
                ..Default::default()
            },
            tuning_profile_label: "balanced (default)".into(),
            source: crate::config::SourceConfig {
                url: Some("postgresql://localhost/test".into()),
                ..test_plan().source
            },
            shape_drift_warn_factor: 2.0,
            ..test_plan()
        };
        let mut s = RunSummary::new(&plan);
        s.status = "success".into();
        s.total_rows = 1_000_908;
        s.files_produced = 11;
        s.bytes_written = 32 * 1024 * 1024 + 400 * 1024;
        s.duration_ms = 68_400;
        s.peak_rss_mb = 884;

        let block = s.render();
        assert!(
            block.starts_with('\n'),
            "block should start with a blank line"
        );
        assert!(block.ends_with('\n'), "block should end with a newline");
        assert!(block.contains("── orders "));
        assert!(
            block.contains("1,000,908"),
            "rows should be formatted with thousands separator: {}",
            block
        );
        assert!(block.contains("1m 08.4s"), "duration formatting: {}", block);
        assert!(!block.contains('\r'));

        let line = s.render_compact();
        assert!(line.starts_with("✓ "), "success icon present: {:?}", line);
        assert!(line.contains("orders"), "export name present: {:?}", line);
        assert!(line.contains("1,000,908 rows"), "rows present: {:?}", line);
        assert!(line.contains("32.4 MB"), "bytes present: {:?}", line);
        assert!(line.contains("1m 08.4s"), "duration present: {:?}", line);
        assert!(line.contains("RSS 884 MB"), "rss present: {:?}", line);
        assert!(!line.contains('\n'), "single line: {:?}", line);
    }

    /// A single-export run must SEE the destination-retry signal.
    #[test]
    fn single_export_summary_card_surfaces_destination_retries() {
        let before = crate::destination::transient_retries_total();
        crate::destination::add_transient_retries(2);

        let s = RunSummary::stub_for_testing("run-dest-retries", "orders");
        let block = s.render();

        let line = block
            .lines()
            .find(|l| l.trim_start().starts_with("dest retries:"))
            .unwrap_or_else(|| {
                panic!("single-export summary card must carry a dest-retries row:\n{block}")
            });
        let value = line.split("dest retries:").nth(1).unwrap().trim();
        let n: u64 = value
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap_or_else(|e| panic!("row must lead with the count, got {value:?}: {e}"));
        assert!(
            n >= before + 2,
            "row must report the accumulated total (>= {} after two more), got {n}: {line:?}",
            before + 2
        );
        assert!(
            value.contains("transient retry attempts"),
            "row must say ATTEMPTS, not claim recovery: {line:?}"
        );
    }

    #[test]
    fn compact_error_summarises_parallel_chunk_errors() {
        let raw = "export 'page_views': parallel checkpoint worker errors:\n\
                   chunk 4: Unexpected (temporary) at write, context: { url: https://storage.googleapis.com/rivet_data_test/exports%2Fpage_views%2Fpage_views_20260430_202442_chunk4.parquet?partNumber=1&uploadId=ABPnzm7RqplA, called: http_util::Client::send } => send http request, source: error sending request: client error (SendRequest): dispatch task is gone\n\
                   chunk 5: Unexpected (temporary) at write, context: { url: https://storage.googleapis.com/rivet_data_test/exports%2Fpage_views%2Fpage_views_20260430_202443_chunk5.parquet?partNumber=1&uploadId=ABPnzm6q, called: http_util::Client::send } => send http request, source: dispatch task is gone";
        let out = compact_error(raw);
        assert!(
            out.contains("2 chunk(s)"),
            "should report number of failed chunks: {:?}",
            out
        );
        assert!(
            out.starts_with("export 'page_views': parallel checkpoint workers failed:"),
            "should keep export prefix and use compact phrasing: {:?}",
            out
        );
        assert!(
            out.contains("chunk 4:"),
            "should include the first chunk as an example: {:?}",
            out
        );
        assert!(!out.contains('\n'), "single line output: {:?}", out);
        assert!(
            out.chars().count() <= 240,
            "must be clamped to <=240 chars, got {}: {:?}",
            out.chars().count(),
            out
        );
    }

    #[test]
    fn compact_error_collapses_generic_multiline() {
        let raw = "first line of trouble\nsecond line with detail\n\nthird line\n";
        let out = compact_error(raw);
        assert_eq!(
            out, "first line of trouble; second line with detail; third line",
            "newlines should collapse to '; ' and blanks dropped"
        );
    }

    #[test]
    fn compact_error_clamps_excessively_long_lines() {
        let raw = "x".repeat(1_000);
        let out = compact_error(&raw);
        assert_eq!(out.chars().count(), 240);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn render_compact_strips_chunked_recovery_hint_for_failed() {
        use crate::plan::{CompressionType, DestinationConfig, DestinationType, ResolvedRunPlan};
        let plan = ResolvedRunPlan {
            export_name: "events".into(),
            compression: CompressionType::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("./out".into()),
                ..Default::default()
            },
            tuning_profile_label: "balanced (default)".into(),
            source: crate::config::SourceConfig {
                url: Some("postgresql://localhost/test".into()),
                ..test_plan().source
            },
            shape_drift_warn_factor: 2.0,
            ..test_plan()
        };
        let mut s = RunSummary::new(&plan);
        s.status = "failed".into();
        s.error_message = Some(
            "export 'events': --resume but no in-progress chunk checkpoint; \
             run without --resume first or `rivet state reset-chunks --config x.yaml --export events`"
                .to_string(),
        );

        let line = s.render_compact();
        assert!(line.starts_with("✗ "), "failure icon: {:?}", line);
        assert!(line.contains("events"), "name present: {:?}", line);
        assert!(
            line.contains("--resume but no in-progress chunk checkpoint"),
            "cause kept: {:?}",
            line
        );
        assert!(
            !line.contains("rivet state reset-chunks"),
            "recovery hint should be stripped from per-export line: {:?}",
            line
        );
        assert!(!line.contains('\n'), "single line: {:?}", line);
    }

    fn plan_for(export_name: &str) -> crate::plan::ResolvedRunPlan {
        use crate::plan::{CompressionType, DestinationConfig, DestinationType, ResolvedRunPlan};
        ResolvedRunPlan {
            export_name: export_name.into(),
            compression: CompressionType::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("./out".into()),
                ..Default::default()
            },
            tuning_profile_label: "balanced (default)".into(),
            source: crate::config::SourceConfig {
                url: Some("postgresql://localhost/test".into()),
                ..test_plan().source
            },
            shape_drift_warn_factor: 2.0,
            ..test_plan()
        }
    }

    #[test]
    fn plan_snapshot_records_chunk_key_and_resumable_for_post_mortem() {
        use crate::plan::{ExtractionStrategy, KeysetPlan};
        let mut plan = plan_for("statistic_lifetime");
        plan.strategy = ExtractionStrategy::Keyset(KeysetPlan {
            key_column: "id".into(),
            chunk_size: 1_000_000,
            checkpoint: true,
            incremental: false,
            parallel: 1,
            key_rest: Vec::new(),
        });
        let s = RunSummary::new(&plan);
        let snap = s.journal.plan_snapshot().expect("PlanResolved recorded");
        assert_eq!(snap.chunk_key.as_deref(), Some("id"));
        assert!(
            snap.resumable,
            "chunk_checkpoint on → resumable must be recorded"
        );

        let full = RunSummary::new(&plan_for("small"));
        let fsnap = full.journal.plan_snapshot().unwrap();
        assert_eq!(fsnap.chunk_key, None);
        assert!(!fsnap.resumable);
    }

    /// The snapshot is where the row-hash contract enters the trust artifacts.
    #[test]
    fn plan_snapshot_records_the_row_hash_contract_the_run_applies() {
        let mut plan = plan_for("orders");
        let bare = RunSummary::new(&plan);
        assert_eq!(
            bare.journal.plan_snapshot().unwrap().row_hash,
            None,
            "no hash column written ⇒ nothing to advertise"
        );

        plan.meta_columns.row_hash =
            crate::config::RowHash::Columns(vec!["id".into(), "status".into()]);
        let hashed = RunSummary::new(&plan);
        let snap = hashed.journal.plan_snapshot().unwrap();
        let c = snap.row_hash.clone().expect("a declared set is recorded");
        assert_eq!(c.column, crate::enrich::COL_ROW_HASH);
        assert_eq!(c.covered, plan.meta_columns.row_hash);
        assert_eq!(c.render, crate::enrich::ROW_HASH_RENDER_ID);
    }

    #[test]
    fn plan_snapshot_deserializes_pre_field_journal_as_defaults() {
        let legacy = r#"{
            "export_name":"orders","base_query":"SELECT 1","strategy":"keyset",
            "format":"parquet","compression":"zstd","destination_type":"gcs",
            "tuning_profile":"balanced","batch_size":10000,
            "validate":false,"reconcile":false,"resume":false
        }"#;
        let snap: crate::journal::PlanSnapshot = serde_json::from_str(legacy).unwrap();
        assert_eq!(snap.chunk_key, None);
        assert!(!snap.resumable);
        assert_eq!(snap.strategy, "keyset");
    }

    #[test]
    fn render_preserves_multiline_error_block() {
        let mut s = RunSummary::new(&plan_for("orders"));
        s.status = "failed".into();
        s.error_message = Some(
            "export 'orders': 1 quality check(s) failed:\n  \
             - row_count 10 below minimum 999999\n  \
             Fix the source data, or adjust the thresholds under `quality:` in your config."
                .to_string(),
        );

        let block = s.render();
        assert!(
            !block.contains("failed:;"),
            "error must not be '; '-flattened in the detailed block: {block}"
        );
        assert!(
            block.contains("- row_count 10 below minimum 999999"),
            "failing check line present: {block}"
        );
        let err_lines: Vec<&str> = block
            .lines()
            .filter(|l| {
                l.contains("quality check(s) failed")
                    || l.contains("row_count 10 below minimum")
                    || l.contains("Fix the source data")
            })
            .collect();
        assert_eq!(
            err_lines.len(),
            3,
            "all three error lines should render on separate lines: {block}"
        );
        for l in &err_lines {
            assert!(l.starts_with(' '), "error line should be indented: {l:?}");
        }
    }

    #[test]
    fn render_nudges_verification_when_unverified_success() {
        let mut s = RunSummary::new(&plan_for("orders"));
        s.status = "success".into();
        s.files_produced = 3;
        s.total_rows = 1_000;
        let block = s.render();
        assert!(
            block.lines().any(|l| l.trim_start().starts_with("verify:")),
            "expected a verify nudge on an unverified success: {block}"
        );

        let mut s2 = RunSummary::new(&plan_for("orders"));
        s2.status = "success".into();
        s2.files_produced = 3;
        s2.validated = Some(true);
        let block2 = s2.render();
        assert!(
            !block2
                .lines()
                .any(|l| l.trim_start().starts_with("verify:")),
            "a verified run must not nudge: {block2}"
        );
    }

    #[test]
    fn pg_temp_spill_row_only_for_real_spill_and_annotates_large() {
        let mut s = RunSummary::stub_for_testing("r", "orders");
        assert_eq!(s.pg_temp_spill_row(), None, "no delta → no row");
        s.pg_temp_bytes_delta = Some(0);
        assert_eq!(s.pg_temp_spill_row(), None, "zero spill → no row");
        s.pg_temp_bytes_delta = Some(-5);
        assert_eq!(s.pg_temp_spill_row(), None, "negative delta → no row");

        s.pg_temp_bytes_delta = Some(50 * 1024 * 1024);
        let (label, value) = s.pg_temp_spill_row().expect("50MB spill → row");
        assert_eq!(label, "pg temp spill");
        assert!(
            value.contains("50.0 MB") && !value.contains('⚠'),
            "small spill is plain info: {value:?}"
        );

        s.pg_temp_bytes_delta = Some(200 * 1024 * 1024);
        let (_, value) = s.pg_temp_spill_row().expect("200MB spill → row");
        assert!(
            value.contains('⚠') && value.contains("batch_size"),
            "spill over 100 MB carries the tuning hint: {value:?}"
        );
    }

    /// The fidelity row appears only when a zero or partial date was written as NULL, with each column and its count.
    #[test]
    fn fidelity_row_names_each_nulled_column_with_its_count() {
        let mut s = RunSummary::stub_for_testing("r", "orders");
        assert_eq!(s.fidelity_row(), None);
        s.nulled_cells = vec![("created".to_string(), 1234), ("shipped".to_string(), 2)];
        assert_eq!(
            s.fidelity_row(),
            Some((
                "fidelity",
                "zero or partial dates written as NULL: created=1,234, shipped=2".to_string()
            ))
        );
    }

    #[test]
    fn post_run_invariants_flag_a_runner_that_skipped_a_facade() {
        use crate::manifest::{ColumnChecksum, ManifestPart, PartStatus};
        let base = || {
            let mut s = RunSummary::stub_for_testing("r", "orders");
            s.status = "success".into();
            s.state_backed = true;
            s.files_committed = 1;
            s.files_produced = 1;
            s.bytes_written = 10;
            s.total_rows = 5;
            s.manifest_parts.push(ManifestPart {
                part_id: 1,
                path: "p.parquet".into(),
                rows: 5,
                size_bytes: 10,
                content_fingerprint: String::new(),
                content_md5: String::new(),
                status: PartStatus::Committed,
            });
            s
        };
        let ck = || ColumnChecksum {
            name: "id".into(),
            checksum: "1".into(),
        };

        let mut ok = base();
        ok.schema_changed = Some(false);
        ok.column_checksums.push(ck());
        assert!(ok.check_post_run_invariants(false).is_ok());

        let mut no_drift = base();
        no_drift.column_checksums.push(ck());
        let e = no_drift.check_post_run_invariants(false).unwrap_err();
        assert!(e.contains("on_schema_drift gate was never applied"), "{e}");

        let mut no_formb = base();
        no_formb.schema_changed = Some(false);
        let e = no_formb.check_post_run_invariants(false).unwrap_err();
        assert!(e.contains("Form-B was never harvested"), "{e}");

        let mut suppressed = base();
        suppressed.schema_changed = Some(false);
        suppressed.column_checksums_incomplete = true;
        assert!(suppressed.check_post_run_invariants(false).is_ok());

        let mut cdc = base();
        cdc.state_backed = false;
        assert!(
            cdc.check_post_run_invariants(false).is_ok(),
            "a non-run_export run must not be held to the facade contract"
        );

        let mut applied = base();
        applied.chunks_precomputed = true;
        applied.column_checksums.push(ck());
        applied.schema_changed = Some(false);
        assert!(
            applied.check_post_run_invariants(false).is_ok(),
            "an apply replaying precomputed chunks must not trip the drift-gate branch"
        );

        let mut not_applied = base();
        not_applied.chunks_precomputed = false;
        not_applied.column_checksums.push(ck());
        assert!(
            not_applied.check_post_run_invariants(false).is_err(),
            "the precomputed exemption must not weaken the guard for ordinary runs"
        );

        let mut nothing_committed = base();
        nothing_committed.files_committed = 0;
        nothing_committed.files_produced = 0;
        nothing_committed.bytes_written = 0;
        nothing_committed.total_rows = 0;
        nothing_committed.manifest_parts.clear();
        assert!(
            nothing_committed.check_post_run_invariants(false).is_ok(),
            "a success that committed no files owes no facades"
        );

        let mut rows_but_no_files = base();
        rows_but_no_files.files_committed = 0;
        rows_but_no_files.files_produced = 0;
        rows_but_no_files.manifest_parts.clear();
        rows_but_no_files.bytes_written = 0;
        rows_but_no_files.total_rows = 5;
        let e = rows_but_no_files
            .check_post_run_invariants(false)
            .unwrap_err();
        assert!(e.contains("no files committed"), "{e}");

        let mut empty_run = base();
        empty_run.files_committed = 0;
        empty_run.files_produced = 0;
        empty_run.total_rows = 0;
        empty_run.bytes_written = 0;
        empty_run.manifest_parts.clear();
        assert!(
            empty_run.check_post_run_invariants(false).is_ok(),
            "an empty run must not read as rows-extracted-but-nothing-landed"
        );

        let mut failed = base();
        failed.status = "failed".into();
        failed.schema_changed = Some(false);
        failed.column_checksums.push(ck());
        assert!(
            failed.check_post_run_invariants(false).is_ok(),
            "a failed run must not be graded against the success-only invariants"
        );

        let mut no_parts = base();
        no_parts.schema_changed = Some(false);
        no_parts.column_checksums.push(ck());
        no_parts.manifest_parts.clear();
        let e = no_parts.check_post_run_invariants(false).unwrap_err();
        assert!(e.contains("ADR-0012 M1"), "{e}");

        let mut resumed = base();
        resumed.resumed = true;
        resumed.column_checksums.push(ck());
        assert!(
            resumed.check_post_run_invariants(false).is_ok(),
            "a resume must not trip the drift-gate branch"
        );

        let mut resume_flag_no_adopt = base();
        resume_flag_no_adopt.resumed = false;
        resume_flag_no_adopt.column_checksums.push(ck());
        assert!(
            resume_flag_no_adopt.check_post_run_invariants(true).is_ok(),
            "a --resume run that adopted no prior work (crash before first commit) must be exempt"
        );
        assert!(
            resume_flag_no_adopt
                .check_post_run_invariants(false)
                .unwrap_err()
                .contains("on_schema_drift gate was never applied"),
            "a fresh (non-resume) run with the gate skipped must still be caught"
        );

        let mut csv = base();
        csv.schema_changed = Some(false);
        csv.format = "csv".into();
        assert!(
            csv.check_post_run_invariants(false).is_ok(),
            "a non-Parquet export must not trip the Form-B branch"
        );
    }

    #[test]
    fn outcome_rows_format_reconcile_and_suppress_nudge_when_checked() {
        let mut s = RunSummary::stub_for_testing("r", "orders");
        s.reconciled = Some(true);
        s.source_count = Some(1_000);
        s.total_rows = 1_000;
        assert!(
            s.outcome_rows()
                .iter()
                .any(|(l, v)| *l == "reconcile" && v == "MATCH (1,000/1,000)"),
            "match wording: {:?}",
            s.outcome_rows()
        );

        s.reconciled = Some(false);
        s.source_count = Some(1_200);
        let rows = s.outcome_rows();
        let recon = rows
            .iter()
            .find(|(l, _)| *l == "reconcile")
            .expect("reconcile row");
        assert!(
            recon.1.contains("MISMATCH") && recon.1.contains("1,000") && recon.1.contains("1,200"),
            "mismatch names both sides: {:?}",
            recon
        );

        s.status = "success".into();
        s.files_produced = 2;
        assert!(
            !s.outcome_rows().iter().any(|(l, _)| *l == "verify"),
            "a reconciled run must not also nudge"
        );
    }

    #[test]
    fn render_surfaces_cursor_position_on_zero_new_incremental() {
        let mut s = RunSummary::new(&plan_for("orders"));
        s.status = "skipped".into();
        s.skip_reason = Some("no new rows since cursor 'updated_at'".into());

        let block = s.render();
        let cursor_line = block
            .lines()
            .find(|l| l.trim_start().starts_with("cursor:"))
            .unwrap_or_else(|| panic!("expected a cursor: line in block: {block}"));
        assert!(
            cursor_line.contains("'updated_at'"),
            "cursor line names the column: {cursor_line:?}"
        );
        assert!(
            cursor_line.contains("unchanged"),
            "cursor line reports the position held: {cursor_line:?}"
        );
    }

    #[test]
    fn incremental_position_line_only_for_cursor_skips() {
        assert_eq!(
            incremental_position_line(Some("no new rows since cursor 'ts'")),
            Some("'ts' unchanged (no new rows this run)".into())
        );
        assert_eq!(
            incremental_position_line(Some("source returned 0 rows")),
            None
        );
        assert_eq!(incremental_position_line(None), None);
    }

    #[test]
    fn render_surfaces_window_position_on_zero_row_time_window() {
        let mut s = RunSummary::new(&plan_for("events"));
        s.status = "skipped".into();
        s.mode = "timewindow".into();
        s.skip_reason = Some("source returned 0 rows".into());

        let block = s.render();
        let window_line = block
            .lines()
            .find(|l| l.trim_start().starts_with("window:"))
            .unwrap_or_else(|| panic!("expected a window: line in block: {block}"));
        assert!(
            window_line.contains("matched no rows"),
            "window line reports the empty window: {window_line:?}"
        );
        assert!(
            window_line.contains("time_column") && window_line.contains("days_window"),
            "window line points at the window config to check: {window_line:?}"
        );
        assert!(
            !block.lines().any(|l| l.trim_start().starts_with("cursor:")),
            "no cursor line for a non-cursor strategy: {block}"
        );
    }

    #[test]
    fn time_window_skip_line_only_for_skipped_time_window() {
        assert_eq!(
            time_window_skip_line("timewindow", Some("source returned 0 rows")),
            Some("rolling time window matched no rows — check `time_column`/`days_window`".into())
        );
        assert_eq!(
            time_window_skip_line("incremental", Some("source returned 0 rows")),
            None
        );
        assert_eq!(
            time_window_skip_line("full", Some("source returned 0 rows")),
            None
        );
        assert_eq!(time_window_skip_line("timewindow", None), None);
    }
}
