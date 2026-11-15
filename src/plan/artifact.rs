//! Plan artifact — the serializable result of `rivet plan`.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::plan::ResolvedRunPlan;

use super::prioritization::{CampaignRecommendation, ExportRecommendation};

/// Fully self-contained plan artifact written by `rivet plan` and consumed by `rivet apply`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanArtifact {
    /// Rivet version that produced this artifact.
    pub rivet_version: String,
    /// Random hex identifier for this plan instance.
    pub plan_id: String,
    /// Wall-clock time when the plan was generated.
    pub created_at: DateTime<Utc>,
    /// Wall-clock time after which `rivet apply` will refuse to execute without `--force`.
    pub expires_at: DateTime<Utc>,
    /// Export this plan targets.
    pub export_name: String,
    /// Human-readable mode label: "full", "incremental", "chunked", "timewindow".
    pub strategy: String,
    /// Deterministic fingerprint of the chunked plan inputs (empty for non-chunked).
    pub plan_fingerprint: String,
    /// Accidental-corruption checksum over [`Self::resolved_plan`] (ADR-0005 PA10, finding #16).
    #[serde(default)]
    pub integrity: String,
    /// The fully-resolved execution plan embedded verbatim.
    pub resolved_plan: ResolvedRunPlan,
    /// Data computed at plan time that can be replayed at apply time.
    pub computed: ComputedPlanData,
    /// Preflight diagnostics captured at plan time.
    pub diagnostics: PlanDiagnostics,
    /// Advisory source-aware prioritization (ADR-0006).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prioritization: Option<PlanPrioritizationSnapshot>,
    /// Absolute path to the config file that produced this plan, captured at `rivet plan` time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_path: Option<String>,
}

/// Per-export recommendation and optional multi-export campaign view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanPrioritizationSnapshot {
    pub export_recommendation: ExportRecommendation,
    /// Present when `rivet plan` runs on the full config with multiple exports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign: Option<CampaignRecommendation>,
}

/// Data computed during `rivet plan` that is reused at `rivet apply` time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputedPlanData {
    /// Pre-computed chunk ranges `(start, end)` for Chunked exports.
    pub chunk_ranges: Vec<(i64, i64)>,
    /// Number of chunk windows (convenience; equals `chunk_ranges.len()`).
    pub chunk_count: usize,
    /// Last cursor value from `StateStore` at plan time, for Incremental exports.
    pub cursor_snapshot: Option<String>,
    /// Row estimate returned by preflight (may be `None` if unavailable).
    pub row_estimate: Option<i64>,
}

/// Preflight diagnostics captured at plan time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanDiagnostics {
    /// Health verdict: "Efficient", "Acceptable", "Degraded", or "Unsafe".
    pub verdict: String,
    /// Human-readable warnings (sparsity, memory risk, missing index, …).
    pub warnings: Vec<String>,
    /// Recommended tuning profile: "fast", "balanced", or "safe".
    pub recommended_profile: String,
    /// Narrative explaining *why* this export's strategy (mode + chunk geometry + parallelism) was chosen, plus its risk profile (resumable?
    #[serde(default)]
    pub strategy_rationale: String,
}

/// Result of a staleness check against the current wall clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StalenessCheck {
    /// Plan is recent enough to apply without warnings.
    Fresh,
    /// Plan is older than the warn threshold but younger than the error threshold.
    StaleWarn(Duration),
    /// Plan is older than the error threshold; `--force` is required to apply.
    StaleError(Duration),
}

impl PlanArtifact {
    /// Build a new artifact with default TTL of 24 hours.
    pub fn new(
        export_name: String,
        strategy: String,
        plan_fingerprint: String,
        mut resolved_plan: ResolvedRunPlan,
        computed: ComputedPlanData,
        diagnostics: PlanDiagnostics,
    ) -> Self {
        let (safe_source, was_redacted) = resolved_plan.source.redact_for_artifact();
        resolved_plan.source = safe_source;
        if was_redacted {
            log::warn!(
                "plan '{}': plaintext credentials stripped from artifact — \
                 apply time must have equivalent env/file-based auth available",
                export_name
            );
        }

        let integrity = resolved_plan_integrity(&resolved_plan);

        let created_at = Utc::now();
        Self {
            rivet_version: env!("CARGO_PKG_VERSION").to_string(),
            plan_id: new_plan_id(),
            created_at,
            expires_at: created_at + Duration::hours(24),
            export_name,
            strategy,
            plan_fingerprint,
            integrity,
            resolved_plan,
            computed,
            diagnostics,
            prioritization: None,
            config_path: None,
        }
    }

    /// Re-check the resolved-plan corruption checksum (ADR-0005 PA10, finding #16).
    pub fn verify_integrity(&self) -> Result<()> {
        if self.integrity.is_empty() {
            log::warn!(
                "plan '{}': artifact predates the corruption checksum (no `integrity` field); \
                 cannot detect an accidental edit after planning. Re-run `rivet plan` to \
                 produce a checksummed artifact.",
                self.export_name,
            );
            return Ok(());
        }
        let actual = resolved_plan_integrity(&self.resolved_plan);
        if actual != self.integrity {
            crate::rivet_bail!(
                crate::error::codes::PLAN_ARTIFACT_INVALID,
                "plan artifact integrity check failed: resolved_plan was modified after planning \
                 (export '{}', sealed {}, recomputed {}). \
                 Do not hand-edit plan files — re-run `rivet plan` to regenerate.",
                self.export_name,
                self.integrity,
                actual,
            );
        }
        Ok(())
    }

    /// Serialize to pretty-printed JSON.
    pub fn to_json_pretty(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// Deserialize from a JSON string.
    pub fn from_json(s: &str) -> Result<Self> {
        Ok(serde_json::from_str(s)?)
    }

    /// Load from a JSON file on disk.
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            crate::error::coded(
                crate::error::codes::CLI_INPUT_UNREADABLE,
                format!("cannot read plan file '{}': {}", path, e),
            )
        })?;
        Self::from_json(&content).map_err(|e| {
            crate::error::coded(
                crate::error::codes::PLAN_ARTIFACT_INVALID,
                format!(
                    "plan file '{}' is not a valid Rivet plan artifact.\n  \
                 Hint: only files generated by `rivet plan` are accepted. \
                 Parser detail: {}",
                    path, e
                ),
            )
        })
    }

    /// Check whether the plan is still fresh enough to apply.
    pub fn staleness(&self, warn_after: Duration, error_after: Duration) -> StalenessCheck {
        let age = Utc::now().signed_duration_since(self.created_at);
        if age >= error_after {
            StalenessCheck::StaleError(age)
        } else if age >= warn_after {
            StalenessCheck::StaleWarn(age)
        } else {
            StalenessCheck::Fresh
        }
    }

    /// Returns `true` when the current cursor value in the state store equals the cursor snapshot recorded at plan time.
    pub fn cursor_matches(&self, current_cursor: Option<&str>) -> bool {
        match &self.computed.cursor_snapshot {
            None => true,
            Some(snap) => current_cursor == Some(snap.as_str()),
        }
    }

    /// When this plan stops being applicable without `--force`: its `expires_at`, or never for a reusable plan.
    fn expiry_text(&self) -> String {
        if self.is_reusable() {
            "never (an incremental plan is reusable)".into()
        } else {
            self.expires_at.format("%Y-%m-%d %H:%M:%S UTC").to_string()
        }
    }

    /// Whether this plan is a reusable description whose every apply continues the stored cursor (incremental, `keyset_incremental`), not a point-in-time snapshot (full, chunked, keyset, time window).
    pub fn is_reusable(&self) -> bool {
        self.resolved_plan.strategy.continues_stored_cursor()
    }

    /// One compact row for the multi-export plan table.
    pub fn summary_line(&self, name_width: usize, apply_wave: Option<u32>) -> String {
        let wave = apply_wave.map_or_else(|| "—".to_string(), |w| w.to_string());
        let rec = self
            .prioritization
            .as_ref()
            .map(|p| p.export_recommendation.recommended_wave.to_string())
            .unwrap_or_else(|| "—".to_string());
        let rows = self
            .computed
            .row_estimate
            .map(|e| format!("~{}", format_rows(e)))
            .unwrap_or_else(|| "—".to_string());
        let name = if self.export_name.chars().count() > name_width {
            let head: String = self.export_name.chars().take(name_width - 1).collect();
            format!("{head}…")
        } else {
            self.export_name.clone()
        };
        Self::compact_row(
            &wave,
            &rec,
            &name,
            &self.strategy.to_string(),
            &rows,
            &self.diagnostics.verdict,
            name_width,
        )
    }

    /// Header row for the compact multi-export plan table, matching [`PlanArtifact::summary_line`]'s columns.
    pub fn summary_header(name_width: usize) -> String {
        Self::compact_row(
            "Wave", "Rec", "Export", "Strategy", "Rows", "Verdict", name_width,
        )
    }

    /// The shared column geometry of the compact plan table (header + rows).
    fn compact_row(
        wave: &str,
        rec: &str,
        export: &str,
        strategy: &str,
        rows: &str,
        verdict: &str,
        name_width: usize,
    ) -> String {
        format!("  {wave:<5} {rec:<4} {export:<name_width$}  {strategy:<11}  {rows:<12}  {verdict}")
    }

    /// Pretty-print a human-readable plan summary to stdout.
    pub fn print_summary(&self) {
        println!();
        println!("  Plan ID  : {}", self.plan_id);
        println!(
            "  Created  : {}",
            self.created_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        println!("  Expires  : {}", self.expiry_text());
        println!("  Export   : {}", self.export_name);
        println!("  Strategy : {}", self.strategy);
        if !self.plan_fingerprint.is_empty() {
            println!("  Fingerprint : {}", self.plan_fingerprint);
        }

        if self.computed.chunk_count > 0 {
            println!("  Chunks   : {}", self.computed.chunk_count);
        }
        if let Some(est) = self.computed.row_estimate {
            println!("  Row est. : ~{}", format_rows(est));
        }
        if let Some(ref cur) = self.computed.cursor_snapshot {
            println!("  Cursor   : {}", cur);
        }

        println!("  Verdict  : {}", self.diagnostics.verdict);
        println!("  Profile  : {}", self.diagnostics.recommended_profile);
        if !self.diagnostics.strategy_rationale.is_empty() {
            println!("  Strategy :");
            println!("    Why: {}", self.diagnostics.strategy_rationale);
        }
        if !self.diagnostics.warnings.is_empty() {
            println!("  Warnings :");
            for w in &self.diagnostics.warnings {
                println!("    • {}", w);
            }
        }

        if let Some(ref p) = self.prioritization {
            println!(
                "  Priority   : score {} — {:?} (wave {})",
                p.export_recommendation.priority_score,
                p.export_recommendation.priority_class,
                p.export_recommendation.recommended_wave
            );
            if p.export_recommendation.isolate_on_source {
                println!("  Isolate    : yes (shared source advisory)");
            }
            if !p.export_recommendation.reasons.is_empty() {
                println!("  Prioritize :");
                for r in &p.export_recommendation.reasons {
                    println!("    • [{}] {}", r.kind.as_str(), r.message);
                }
            }
            if let Some(ref c) = p.campaign {
                if c.source_group_warnings.is_empty() {
                    println!(
                        "  Campaign   : {} export(s) ordered by advisory score",
                        c.ordered_exports.len()
                    );
                } else {
                    println!("  Campaign   :");
                    for w in &c.source_group_warnings {
                        println!("    • {}", w);
                    }
                }
            }
        }

        let res = self.resolved_plan.tuning.resource_summary();
        println!("  Resources:");
        if let Some(mem_mb) = res.batch_size_memory_mb {
            println!("    Batch size   : adaptive (target {} MB/batch)", mem_mb);
        } else {
            println!(
                "    Batch size   : {:>7} rows",
                format_number(res.batch_size)
            );
        }
        println!(
            "    Batch memory : ~{:.0} MB (narrow) – ~{:.0} MB (wide)",
            res.batch_narrow_mb, res.batch_wide_mb
        );
        if res.memory_threshold_mb > 0 {
            println!(
                "    RSS guard    : {} MB",
                format_number(res.memory_threshold_mb)
            );
        }
        if res.throttle_ms > 0 {
            println!("    Throttle     : {} ms between batches", res.throttle_ms);
        }
        if res.wide_table_risk {
            println!(
                "    ⚠ Wide tables may use up to {:.0} MB/batch — consider batch_size_memory_mb or a lower batch_size",
                res.batch_wide_mb
            );
        }

        let dest = &self.resolved_plan.destination;
        let dest_label = dest.destination_type.label();
        let dest_location = dest
            .path
            .as_deref()
            .or(dest.bucket.as_deref())
            .unwrap_or("(default)");
        println!("  Output   : {} → {}", dest_label, dest_location);

        let fmt = self.resolved_plan.format.label();
        let comp = self.resolved_plan.compression.label();
        if let Some(level) = self.resolved_plan.compression_level {
            println!("  Format   : {} + {} (level {})", fmt, comp, level);
        } else {
            println!("  Format   : {} + {}", fmt, comp);
        }
        if let (crate::config::FormatType::Parquet, Some(pc)) =
            (self.resolved_plan.format, &self.resolved_plan.parquet)
        {
            let strategy = pc.row_group_strategy.unwrap_or_default();
            match strategy {
                crate::config::RowGroupStrategy::FixedRows => {
                    if let Some(rows) = pc.row_group_rows {
                        println!("  Row group: fixed {} rows", format_number(rows));
                    }
                }
                _ => {
                    let target_mb = pc
                        .target_row_group_mb
                        .unwrap_or(crate::config::ParquetConfig::DEFAULT_TARGET_ROW_GROUP_MB);
                    println!(
                        "  Row group: {:?} (target {} MB{})",
                        strategy,
                        target_mb,
                        pc.max_row_group_mb
                            .map(|m| format!(", max {} MB", m))
                            .unwrap_or_default(),
                    );
                }
            }
        }
        println!();
    }
}

/// Compute the accidental-corruption checksum over a resolved plan (ADR-0005 PA10, #16).
fn resolved_plan_integrity(plan: &ResolvedRunPlan) -> String {
    use xxhash_rust::xxh3::xxh3_64;
    let encoded = serde_json::to_value(plan).and_then(|mut v| {
        if let Some(fields) = v.as_object_mut() {
            fields.remove("single_pass_of");
        }
        canonicalize_value(&mut v);
        serde_json::to_vec(&v)
    });
    match encoded {
        Ok(bytes) => format!("xxh3:{:016x}", xxh3_64(&bytes)),
        Err(e) => {
            log::error!(
                "plan integrity checksum: failed to canonicalize resolved_plan ({e}); \
                 emitting a non-matching value so apply rejects this artifact"
            );
            "xxh3:unserializable".to_string()
        }
    }
}

/// Recursively rewrite every JSON object in `value` into key-sorted order so the serialized byte stream is independent of map iteration order.
fn canonicalize_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(String, serde_json::Value)> =
                map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            map.clear();
            for (k, mut v) in entries {
                canonicalize_value(&mut v);
                map.insert(k, v);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                canonicalize_value(item);
            }
        }
        _ => {}
    }
}

/// Generate a random 32-char lowercase hex plan identifier.
fn new_plan_id() -> String {
    let hi = rand::random::<u64>();
    let lo = rand::random::<u64>();
    format!("{:016x}{:016x}", hi, lo)
}

/// Format an integer with thousands separators.
fn format_number(n: usize) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

/// Format a row count with thousands separators (US locale style).
fn format_rows(n: i64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

#[cfg(test)]
mod legacy_wire_compat {
    //! Every frozen plan artifact must still DESERIALIZE through the current `PlanArtifact`.

    use super::PlanArtifact;

    #[test]
    fn every_frozen_plan_artifact_still_deserializes() {
        for (label, body) in [
            (
                "v0_7_5_plan_full",
                include_str!("../../tests/fixtures/artifacts_legacy/v0_7_5_plan_full.json"),
            ),
            (
                "v0_7_5_plan_incremental",
                include_str!("../../tests/fixtures/artifacts_legacy/v0_7_5_plan_incremental.json"),
            ),
            (
                "v0_7_5_plan_chunked",
                include_str!("../../tests/fixtures/artifacts_legacy/v0_7_5_plan_chunked.json"),
            ),
        ] {
            let parsed = PlanArtifact::from_json(body);
            assert!(
                parsed.is_ok(),
                "{label}: a plan artifact written by an older rivet must still parse — \
                 `rivet apply` reads it straight off a user's disk. A new field on the \
                 resolved plan needs `#[serde(default)]`, like its siblings. Error: {}",
                parsed.err().map(|e| e.to_string()).unwrap_or_default()
            );
        }
    }

    /// A plan SEALED by a released rivet must still verify.
    #[test]
    fn every_sealed_frozen_plan_artifact_still_verifies() {
        for (label, body) in [
            (
                "v0_28_0_plan_keyset_sealed",
                include_str!(
                    "../../tests/fixtures/artifacts_legacy/v0_28_0_plan_keyset_sealed.json"
                ),
            ),
            (
                "v0_28_0_plan_full_sealed",
                include_str!("../../tests/fixtures/artifacts_legacy/v0_28_0_plan_full_sealed.json"),
            ),
        ] {
            let artifact = PlanArtifact::from_json(body).expect(label);
            assert!(
                !artifact.integrity.is_empty(),
                "{label}: fixture must carry a seal"
            );
            if let Err(e) = artifact.verify_integrity() {
                panic!(
                    "{label}: `rivet apply` would reject this user's plan.json. A new field \
                     on the resolved plan needs `skip_serializing_if` so an unset value \
                     serializes as before: {e}"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompressionType, DestinationConfig, DestinationType, SourceConfig};
    use crate::plan::test_support::test_plan;
    use crate::plan::{ChunkedPlan, ExtractionStrategy};

    fn minimal_plan() -> ResolvedRunPlan {
        ResolvedRunPlan {
            base_query: "SELECT * FROM orders".into(),
            compression: CompressionType::Zstd,
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("./out".into()),
                ..Default::default()
            },
            tuning_profile_label: "balanced (default)".into(),
            source: SourceConfig {
                url: Some("postgresql://localhost/test".into()),
                ..test_plan().source
            },
            shape_drift_warn_factor: 2.0,
            ..test_plan()
        }
    }

    fn minimal_artifact() -> PlanArtifact {
        PlanArtifact::new(
            "orders".into(),
            "full".into(),
            String::new(),
            minimal_plan(),
            ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot: None,
                row_estimate: Some(42),
            },
            PlanDiagnostics {
                verdict: "Efficient".into(),
                warnings: vec![],
                recommended_profile: "balanced".into(),
                strategy_rationale: String::new(),
            },
        )
    }

    #[test]
    fn round_trip_json() {
        let artifact = minimal_artifact();
        let json = artifact.to_json_pretty().unwrap();
        let restored: PlanArtifact = PlanArtifact::from_json(&json).unwrap();
        assert_eq!(restored.export_name, artifact.export_name);
        assert_eq!(restored.plan_id, artifact.plan_id);
        assert_eq!(restored.computed.row_estimate, Some(42));
        assert_eq!(restored.diagnostics.verdict, "Efficient");
    }

    #[test]
    fn strategy_rationale_serializes_and_round_trips() {
        let mut artifact = minimal_artifact();
        let why = "Mode full: ~42 rows, below the threshold. Risk — resumable: no.";
        artifact.diagnostics.strategy_rationale = why.to_string();

        let json = artifact.to_json_pretty().unwrap();
        assert!(
            json.contains("strategy_rationale"),
            "the rationale field must appear in the JSON: {json}"
        );
        assert!(
            json.contains("Mode full"),
            "the rationale text must appear in the JSON: {json}"
        );

        let restored = PlanArtifact::from_json(&json).unwrap();
        assert_eq!(
            restored.diagnostics.strategy_rationale, why,
            "rationale must survive the round trip unchanged"
        );
    }

    #[test]
    fn legacy_artifact_without_rationale_deserializes_to_empty() {
        let mut artifact = minimal_artifact();
        artifact.diagnostics.strategy_rationale = "should be dropped".into();
        let mut value: serde_json::Value =
            serde_json::from_str(&artifact.to_json_pretty().unwrap()).unwrap();
        value["diagnostics"]
            .as_object_mut()
            .unwrap()
            .remove("strategy_rationale");
        let legacy_json = serde_json::to_string(&value).unwrap();
        assert!(!legacy_json.contains("strategy_rationale"));

        let restored = PlanArtifact::from_json(&legacy_json)
            .expect("legacy artifact without the rationale field must deserialize");
        assert_eq!(
            restored.diagnostics.strategy_rationale, "",
            "missing rationale must default to empty, not fail"
        );
    }

    #[test]
    fn round_trip_chunked() {
        let mut plan = minimal_plan();
        plan.strategy = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 50_000,
            chunk_count: None,
            parallel: 4,
            by_days: None,
            checkpoint: true,
            max_attempts: 3,
        });
        let artifact = PlanArtifact::new(
            "orders".into(),
            "chunked".into(),
            "abc123".into(),
            plan,
            ComputedPlanData {
                chunk_ranges: vec![(1, 50000), (50001, 100000)],
                chunk_count: 2,
                cursor_snapshot: None,
                row_estimate: Some(100000),
            },
            PlanDiagnostics {
                verdict: "Acceptable".into(),
                warnings: vec!["sparse range".into()],
                recommended_profile: "balanced".into(),
                strategy_rationale: String::new(),
            },
        );
        let json = serde_json::to_string(&artifact).unwrap();
        let restored = PlanArtifact::from_json(&json).unwrap();
        assert_eq!(
            restored.computed.chunk_ranges,
            vec![(1, 50000), (50001, 100000)]
        );
        assert_eq!(restored.computed.chunk_count, 2);
        assert_eq!(restored.plan_fingerprint, "abc123");
    }

    #[test]
    fn staleness_fresh() {
        let artifact = minimal_artifact();
        let result = artifact.staleness(Duration::hours(1), Duration::hours(24));
        assert_eq!(result, StalenessCheck::Fresh);
    }

    #[test]
    fn staleness_warn_between_thresholds() {
        let mut artifact = minimal_artifact();
        artifact.created_at = Utc::now() - Duration::hours(2);
        let result = artifact.staleness(Duration::hours(1), Duration::hours(24));
        assert!(
            matches!(result, StalenessCheck::StaleWarn(_)),
            "expected StaleWarn, got {result:?}"
        );
    }

    #[test]
    fn staleness_expired_artifact() {
        let mut artifact = minimal_artifact();
        artifact.created_at = Utc::now() - Duration::hours(25);
        let result = artifact.staleness(Duration::hours(1), Duration::hours(24));
        assert!(matches!(result, StalenessCheck::StaleError(_)));
    }

    #[test]
    fn staleness_exactly_at_warn_threshold() {
        let mut artifact = minimal_artifact();
        artifact.created_at = Utc::now() - Duration::hours(1) - Duration::seconds(1);
        let result = artifact.staleness(Duration::hours(1), Duration::hours(24));
        assert!(matches!(result, StalenessCheck::StaleWarn(_)));
    }

    #[test]
    fn staleness_exactly_at_error_threshold() {
        let mut artifact = minimal_artifact();
        artifact.created_at = Utc::now() - Duration::hours(24) - Duration::seconds(1);
        let result = artifact.staleness(Duration::hours(1), Duration::hours(24));
        assert!(matches!(result, StalenessCheck::StaleError(_)));
    }

    #[test]
    fn cursor_matches_none_snapshot() {
        let artifact = minimal_artifact();
        assert!(artifact.cursor_matches(None));
        assert!(artifact.cursor_matches(Some("anything")));
    }

    #[test]
    fn cursor_matches_incremental() {
        let mut artifact = minimal_artifact();
        artifact.computed.cursor_snapshot = Some("2026-01-01T00:00:00Z".into());
        assert!(artifact.cursor_matches(Some("2026-01-01T00:00:00Z")));
        assert!(!artifact.cursor_matches(Some("2026-01-02T00:00:00Z")));
        assert!(!artifact.cursor_matches(None));
    }

    /// A point-in-time plan shows its `expires_at`; an incremental plan, reusable, never expires.
    #[test]
    fn only_a_point_in_time_plan_shows_an_expiry() {
        let mut artifact = minimal_artifact();
        assert!(!artifact.is_reusable());
        assert_eq!(
            artifact.expiry_text(),
            artifact
                .expires_at
                .format("%Y-%m-%d %H:%M:%S UTC")
                .to_string()
        );
        artifact.resolved_plan.strategy =
            crate::plan::ExtractionStrategy::Incremental(crate::plan::IncrementalCursorPlan {
                primary_column: "updated_at".into(),
                fallback_column: None,
                mode: crate::config::IncrementalCursorMode::SingleColumn,
                settle: None,
            });
        assert!(artifact.is_reusable());
        assert_eq!(
            artifact.expiry_text(),
            "never (an incremental plan is reusable)"
        );
    }

    #[test]
    fn format_rows_basic() {
        assert_eq!(super::format_rows(1000), "1,000");
        assert_eq!(super::format_rows(1_000_000), "1,000,000");
        assert_eq!(super::format_rows(42), "42");
    }

    #[test]
    fn artifact_strips_plaintext_password_from_source() {
        let mut plan = minimal_plan();
        plan.source.password = Some("s3cret!".into());
        let artifact = PlanArtifact::new(
            "orders".into(),
            "full".into(),
            String::new(),
            plan,
            ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot: None,
                row_estimate: None,
            },
            PlanDiagnostics {
                verdict: "Efficient".into(),
                warnings: vec![],
                recommended_profile: "balanced".into(),
                strategy_rationale: String::new(),
            },
        );
        assert_eq!(
            artifact.resolved_plan.source.password, None,
            "plaintext password must never leave the process"
        );
        let json = artifact.to_json_pretty().unwrap();
        assert!(
            !json.contains("s3cret"),
            "secret must not appear anywhere in the JSON"
        );
    }

    #[test]
    fn artifact_strips_credentials_from_url() {
        let mut plan = minimal_plan();
        plan.source.url = Some("postgresql://rivet:s3cret@db.example.com/prod".into());
        let artifact = PlanArtifact::new(
            "orders".into(),
            "full".into(),
            String::new(),
            plan,
            ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot: None,
                row_estimate: None,
            },
            PlanDiagnostics {
                verdict: "Efficient".into(),
                warnings: vec![],
                recommended_profile: "balanced".into(),
                strategy_rationale: String::new(),
            },
        );
        let url = artifact.resolved_plan.source.url.as_ref().unwrap().expose();
        assert!(
            !url.contains("s3cret"),
            "password must not remain in URL: {url}"
        );
        assert!(url.contains("REDACTED@db.example.com/prod"));
    }

    #[test]
    fn an_edited_unreadable_or_malformed_plan_file_is_refused_by_code() {
        let mut artifact = minimal_artifact();
        artifact.resolved_plan.base_query = "SELECT 1".into();
        let err = artifact.verify_integrity().unwrap_err();
        crate::error::assert_coded(&err, "RIVET_PLAN_ARTIFACT_INVALID", 1);
        let err = PlanArtifact::from_file("/nonexistent/rivet-plan-test.json").unwrap_err();
        crate::error::assert_coded(&err, "RIVET_CLI_INPUT_UNREADABLE", 1);
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(f.path(), b"{\"not\": \"a plan\"}").unwrap();
        let err = PlanArtifact::from_file(f.path().to_str().unwrap()).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_PLAN_ARTIFACT_INVALID", 1);
    }

    #[test]
    fn integrity_seal_accepts_untouched_artifact() {
        let artifact = minimal_artifact();
        assert!(
            !artifact.integrity.is_empty(),
            "new artifact must be sealed"
        );
        artifact
            .verify_integrity()
            .expect("untouched artifact must pass integrity check");

        let json = artifact.to_json_pretty().unwrap();
        let restored = PlanArtifact::from_json(&json).unwrap();
        restored
            .verify_integrity()
            .expect("round-tripped artifact must still verify");
    }

    #[test]
    fn a_single_pass_plan_verifies_under_a_rivet_that_drops_the_field() {
        use crate::plan::contract::PagedMode;
        let mut plan = minimal_plan();
        plan.single_pass_of = Some(PagedMode::Chunked);
        let mut artifact = minimal_artifact();
        artifact.resolved_plan = plan;
        artifact.integrity = resolved_plan_integrity(&artifact.resolved_plan);
        let mut json: serde_json::Value =
            serde_json::from_str(&artifact.to_json_pretty().unwrap()).unwrap();

        let sealed = PlanArtifact::from_json(&json.to_string()).unwrap();
        assert_eq!(
            sealed.resolved_plan.single_pass_of,
            Some(PagedMode::Chunked),
            "the plan file carries the field to `rivet apply`"
        );
        sealed.verify_integrity().expect("as sealed");

        let dropped = json["resolved_plan"]
            .as_object_mut()
            .unwrap()
            .remove("single_pass_of");
        assert_eq!(dropped, Some(serde_json::json!("Chunked")));
        let older = PlanArtifact::from_json(&json.to_string()).unwrap();
        assert_eq!(older.resolved_plan.single_pass_of, None);
        older
            .verify_integrity()
            .expect("the plan as a rivet without the field re-serializes it");
    }

    #[test]
    fn integrity_seal_rejects_tampered_base_query() {
        let mut artifact = minimal_artifact();
        artifact.resolved_plan.base_query = "SELECT * FROM users".into();
        let err = artifact.verify_integrity().unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("integrity check failed") && msg.contains("modified after planning"),
            "tamper must be reported with the PA10 message, got: {msg}"
        );
    }

    #[test]
    fn integrity_seal_rejects_tamper_in_any_execution_field() {
        let mut artifact = minimal_artifact();
        artifact.resolved_plan.destination.path = Some("/etc/evil".into());
        assert!(
            artifact.verify_integrity().is_err(),
            "editing the destination path must be detected as tampering"
        );
    }

    #[test]
    fn integrity_seal_legacy_empty_is_accepted_with_warning() {
        let mut artifact = minimal_artifact();
        artifact.integrity = String::new();
        artifact
            .verify_integrity()
            .expect("unsealed (legacy) artifact must be accepted, not failed closed");
    }

    #[test]
    fn integrity_checksum_is_not_tamper_protection() {
        let mut artifact = minimal_artifact();
        artifact.resolved_plan.base_query = "SELECT * FROM users".into();
        artifact.integrity = resolved_plan_integrity(&artifact.resolved_plan);
        artifact.verify_integrity().expect(
            "an unkeyed checksum cannot detect a deliberate edit + recompute — \
             this is accidental-corruption detection, not tamper protection",
        );
    }

    #[test]
    fn integrity_seal_distinguishes_a_real_override_change() {
        let mut plan_a = minimal_plan();
        plan_a.column_overrides = [("alpha".to_string(), crate::types::RivetType::String)]
            .into_iter()
            .collect();
        let mut plan_b = minimal_plan();
        plan_b.column_overrides = [("alpha".to_string(), crate::types::RivetType::Int64)]
            .into_iter()
            .collect();
        assert_ne!(
            resolved_plan_integrity(&plan_a),
            resolved_plan_integrity(&plan_b),
            "a real override type change must change the seal"
        );
    }

    #[test]
    fn canonicalize_makes_seal_independent_of_object_key_order() {
        let mut a = serde_json::json!({});
        let mut b = serde_json::json!({});
        if let (serde_json::Value::Object(ma), serde_json::Value::Object(mb)) = (&mut a, &mut b) {
            for k in ["zeta", "alpha", "mu"] {
                ma.insert(k.to_string(), serde_json::json!(k));
            }
            for k in ["mu", "zeta", "alpha"] {
                mb.insert(k.to_string(), serde_json::json!(k));
            }
        }
        canonicalize_value(&mut a);
        canonicalize_value(&mut b);
        assert_eq!(
            serde_json::to_vec(&a).unwrap(),
            serde_json::to_vec(&b).unwrap(),
            "canonicalized bytes must not depend on object key insertion order"
        );
    }

    #[test]
    fn canonicalize_value_sorts_nested_keys_preserves_arrays() {
        let mut v = serde_json::json!({
            "b": 1,
            "a": { "y": [3, 2, 1], "x": 0 },
        });
        canonicalize_value(&mut v);
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, r#"{"a":{"x":0,"y":[3,2,1]},"b":1}"#);
    }
}
