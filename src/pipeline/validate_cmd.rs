//! **Layer: Coordinator** (config → destination → verification → render)

use std::path::Path;

use chrono::NaiveDate;

use crate::config::{Config, ExportConfig};
use crate::destination::placeholder::PlaceholderContext;
use crate::error::Result;
use crate::pipeline::run_set::RunSet;
use crate::pipeline::validate_manifest::ManifestVerification;
use crate::pipeline::validate_manifest::{ValidateDepth, verify_at_destination};

/// Output format mirroring the `rivet reconcile` / `rivet repair` pattern.
pub enum ValidateOutputFormat {
    /// Human-readable summary printed to stdout.
    Pretty,
    /// JSON to the given path or stdout if `None`.
    Json(Option<String>),
}

/// Re-targeting overrides for `rivet validate`.
#[derive(Debug, Default, Clone)]
pub struct ValidateTarget {
    /// `--date YYYY-MM-DD` — override the date used for `{date}`.
    pub date: Option<NaiveDate>,
    /// `--run-id RID` — substitute `{run_id}` in the destination template.
    pub run_id: Option<String>,
    /// `--prefix STRING` — bypass placeholder resolution entirely and verify exactly this prefix.
    pub prefix_override: Option<String>,
    /// `--depth light|sample|full` — the graded verify layer (see [`ValidateDepth`]).
    pub depth: ValidateDepth,
}

impl ValidateTarget {
    fn placeholder_context(&self, export_name: &str) -> PlaceholderContext {
        let mut ctx = match self.date {
            Some(d) => PlaceholderContext::for_date(d, export_name),
            None => PlaceholderContext::for_today(export_name),
        };
        if let Some(rid) = &self.run_id {
            ctx = ctx.with_run_id(rid.clone());
        }
        ctx
    }
}

/// Driver for `rivet validate <export>` (or every export when `export_name` is `None`).
pub fn run_validate_command(
    config_path: &str,
    export_name: Option<&str>,
    format: ValidateOutputFormat,
    target: ValidateTarget,
) -> Result<()> {
    let config = Config::load_with_params(config_path, None)?;

    let config_dir = Path::new(config_path).parent().unwrap_or(Path::new("."));
    let run_set = RunSet::select(&config, export_name)?;
    let exports: Vec<ExportConfig> = if target.prefix_override.is_some() {
        run_set.declared().to_vec()
    } else {
        run_set
            .in_process(&config.source, config_dir, None)
            .map_err(|e| {
                crate::error::recoded(
                    &e,
                    crate::error::codes::SOURCE_PROBE_UNREADABLE,
                    format!(
                        "validate could not list the partitions of a `partition_by` export: the \
                         source holds the partition list and was not readable.\n  {e:#}\n  \
                         Validate one partition without the source with \
                         `rivet validate --export <name> --prefix <partition directory>`."
                    ),
                )
            })?
    };

    if run_set.declared().is_empty() {
        crate::rivet_bail!(
            crate::error::codes::CONFIG_NO_EXPORTS,
            "no exports defined in config — nothing to validate"
        );
    }
    if exports.is_empty() {
        println!("partition_by found no rows in the source: no partition to validate");
        return Ok(());
    }

    if target.prefix_override.is_some() && exports.len() > 1 {
        crate::rivet_bail!(
            crate::error::codes::CLI_OPTION_UNSUPPORTED,
            "--prefix requires --export <name>: cannot apply one override to {} exports",
            exports.len()
        );
    }

    let mut all_results: Vec<ExportVerdict> = Vec::with_capacity(exports.len());
    let mut hard_failures: Vec<String> = Vec::new();

    for export in &exports {
        let ctx = target.placeholder_context(&export.name);
        let mut expanded_dest =
            crate::destination::placeholder::expand_destination(export.destination.clone(), &ctx);
        if let Some(p) = &target.prefix_override {
            expanded_dest.path = Some(p.clone());
            expanded_dest.prefix = Some(p.clone());
        }
        let multiplex = export.multiplex_tables();
        let has_snapshot = export.mode == crate::config::ExportMode::Cdc
            && export.cdc.as_ref().is_some_and(|c| c.has_baseline());
        match multiplex {
            Some(tables) => {
                for table in tables {
                    verify_cdc_table(
                        crate::pipeline::cdc_job::dest_for_table(&expanded_dest, table),
                        format!("{}/{}", export.name, table),
                        export,
                        &target,
                        has_snapshot,
                        &mut all_results,
                        &mut hard_failures,
                    );
                }
            }
            None if export.mode == crate::config::ExportMode::Cdc => {
                verify_cdc_table(
                    expanded_dest,
                    export.name.clone(),
                    export,
                    &target,
                    has_snapshot,
                    &mut all_results,
                    &mut hard_failures,
                );
            }
            None => {
                verify_one_prefix(
                    expanded_dest,
                    export.name.clone(),
                    export,
                    &target,
                    false,
                    false,
                    &mut all_results,
                    &mut hard_failures,
                );
            }
        }
    }

    settle_recorded_deliveries(config_path, &mut all_results)?;

    match format {
        ValidateOutputFormat::Pretty => {
            render_pretty(&mut std::io::stdout().lock(), &all_results, &hard_failures)
        }
        ValidateOutputFormat::Json(out_path) => {
            render_json(&all_results, &hard_failures, out_path)?
        }
    }

    let verified_wrong = all_results
        .iter()
        .filter(|r| {
            verdict_fails_exit(&r.verification) && r.verification.has_verified_wrong_failure()
        })
        .count();
    let could_not_verify_verdicts = all_results
        .iter()
        .filter(|r| {
            verdict_fails_exit(&r.verification) && !r.verification.has_verified_wrong_failure()
        })
        .count();
    if verified_wrong > 0 {
        return Err(crate::error::DataIntegrityError::new(format!(
            "rivet validate: {} export(s) failed verification",
            hard_failures.len() + verified_wrong + could_not_verify_verdicts
        ))
        .into());
    }
    let could_not_verify = hard_failures.len() + could_not_verify_verdicts;
    if could_not_verify > 0 {
        crate::rivet_bail!(
            crate::error::codes::VALIDATE_UNVERIFIED,
            "rivet validate: {could_not_verify} export(s) could not be verified"
        );
    }
    Ok(())
}

/// Turn each bare absent-manifest verdict of a batch export into a lost delivery when the state records a successful run of the export at that destination.
fn settle_recorded_deliveries(config_path: &str, results: &mut [ExportVerdict]) -> Result<()> {
    if !results
        .iter()
        .any(|r| r.recorded_as.is_some() && r.verification.is_bare_legacy())
    {
        return Ok(());
    }
    let Some(state) = crate::state::StateStore::open_if_present(config_path)? else {
        return Ok(());
    };
    for r in results.iter_mut() {
        let ExportVerdict {
            resolved_prefix,
            verification,
            recorded_as: Some((export, uri)),
            ..
        } = r
        else {
            continue;
        };
        if verification.is_bare_legacy() && state.has_recorded_success(export, uri)? {
            verification.require_recorded_delivery(resolved_prefix, export);
        }
    }
    Ok(())
}

/// Exit-code predicate for one export's verdict.
fn verdict_fails_exit(v: &ManifestVerification) -> bool {
    !v.passed && v.has_failures()
}

/// Per-export verdict plus the resolved physical prefix the verifier looked at.
struct ExportVerdict {
    name: String,
    resolved_prefix: String,
    verification: ManifestVerification,
    recorded_as: Option<(String, String)>,
}

/// Render the destination's resolved prefix for human/JSON output.
fn resolved_prefix_for_display(dest: &crate::config::DestinationConfig) -> String {
    dest.prefix
        .clone()
        .or_else(|| dest.path.clone())
        .unwrap_or_else(|| "<unresolved>".into())
}

/// Verify one CDC table's output at `table_dest`.
#[allow(clippy::too_many_arguments)]
fn verify_cdc_table(
    table_dest: crate::config::DestinationConfig,
    display_name: String,
    export: &crate::config::ExportConfig,
    target: &ValidateTarget,
    has_snapshot: bool,
    all_results: &mut Vec<ExportVerdict>,
    hard_failures: &mut Vec<String>,
) {
    if has_snapshot {
        let snap = crate::pipeline::cdc_job::dest_for_table(&table_dest, "snapshot");
        verify_one_prefix(
            snap,
            format!("{display_name}/snapshot"),
            export,
            target,
            false,
            false,
            all_results,
            hard_failures,
        );
    }
    verify_one_prefix(
        table_dest,
        display_name,
        export,
        target,
        true,
        has_snapshot,
        all_results,
        hard_failures,
    );
}

/// Verify ONE resolved prefix (an export's whole destination, or one table's sub-prefix of a `tables:` CDC stream) and append its verdict / failures.
#[allow(clippy::too_many_arguments)]
fn verify_one_prefix(
    expanded_dest: crate::config::DestinationConfig,
    display_name: String,
    export: &crate::config::ExportConfig,
    target: &ValidateTarget,
    run_cdc_pos_check: bool,
    drop_snapshot_untracked: bool,
    all_results: &mut Vec<ExportVerdict>,
    hard_failures: &mut Vec<String>,
) {
    let resolved_prefix = resolved_prefix_for_display(&expanded_dest);
    let recorded_as = (export.mode != crate::config::ExportMode::Cdc
        && target.prefix_override.is_none())
    .then(|| (export.name.clone(), expanded_dest.uri()));
    let dest = match crate::destination::create_destination(&expanded_dest) {
        Ok(d) => d,
        Err(e) => {
            hard_failures.push(format!(
                "export '{}' (prefix: {}): could not open destination: {:#}",
                display_name, resolved_prefix, e
            ));
            return;
        }
    };
    if !dest.capabilities().commit_protocol.leaves_objects_at_rest() {
        log::info!(
            "export '{}': streaming destination, skipping (nothing to verify)",
            display_name
        );
        return;
    }
    match verify_at_destination(&*dest, "", target.depth) {
        Ok(mut v) => {
            v.enforce_content_policy(export.verify.requires_content());
            if drop_snapshot_untracked {
                v.failures.retain(|f| {
                    !matches!(
                        f,
                        crate::pipeline::validate_manifest::Failure::UntrackedObject { key, .. }
                            if key.starts_with("snapshot/")
                    )
                });
            }
            if target.prefix_override.is_some() {
                v.require_manifest_present(&resolved_prefix);
            } else if let Some(window) = &export.partition_window {
                v.require_partition_delivered(&resolved_prefix, &window.parent);
            }
            let manifest_verified = v.manifest_found && v.passed;
            all_results.push(ExportVerdict {
                name: display_name.clone(),
                resolved_prefix,
                verification: v,
                recorded_as,
            });
            if target.depth.runs_part_download()
                && run_cdc_pos_check
                && export.format == crate::config::FormatType::Parquet
            {
                match crate::source::cdc::validate::check_positions(&*dest, "") {
                    Ok(pc) if pc.is_ok() => log::info!(
                        "export '{}': cdc __pos continuity OK — {} changes across {} parts, range {:?}..{:?}",
                        display_name,
                        pc.rows,
                        pc.parts,
                        pc.first,
                        pc.last
                    ),
                    Ok(pc) => {
                        if let Some(ev) = all_results.last_mut() {
                            ev.verification.passed = false;
                            for viol in &pc.violations {
                                ev.verification.failures.push(
                                    crate::pipeline::validate_manifest::Failure::CdcPositionViolation {
                                        detail: format!("export '{}': {}", display_name, viol),
                                    },
                                );
                            }
                        }
                    }
                    Err(e) => hard_failures.push(format!(
                        "export '{}': cdc __pos check could not complete: {:#}",
                        display_name, e
                    )),
                }
            }
            if target.depth.runs_part_download() && manifest_verified {
                if export.format == crate::config::FormatType::Csv
                    && let Some(ev) = all_results.last_mut()
                {
                    ev.verification.failures.push(
                        crate::pipeline::validate_manifest::Failure::ValueCheckNotAvailable {
                            format: export.format.label().into(),
                        },
                    );
                }
                match crate::source::value_checksum::validate_manifest_checksums(&*dest, "") {
                    Ok(Some(fault)) => {
                        use crate::pipeline::validate_manifest::Failure;
                        use crate::source::value_checksum::ReadbackFault;
                        if let Some(ev) = all_results.last_mut() {
                            ev.verification.passed = false;
                            ev.verification.failures.push(match fault {
                                ReadbackFault::ValueChecksum(d) => Failure::ValueChecksumMismatch {
                                    detail: format!("export '{}': {}", display_name, d),
                                },
                                ReadbackFault::PartRowCount(d) => Failure::PartRowCountMismatch {
                                    detail: format!("export '{}': {}", display_name, d),
                                },
                            });
                        }
                    }
                    Err(e) => hard_failures.push(format!(
                        "export '{}': value-checksum re-read could not complete: {:#}",
                        display_name, e
                    )),
                    Ok(None) => {}
                }
            }
        }
        Err(e) => {
            hard_failures.push(format!(
                "export '{}' (prefix: {}): verify_at_destination failed: {:#}",
                display_name, resolved_prefix, e
            ));
        }
    }
}

/// Render the text verdicts to `h`.
fn render_pretty(h: &mut impl std::io::Write, results: &[ExportVerdict], hard_failures: &[String]) {
    for r in results {
        let _ = writeln!(h, "── {} ──", r.name);
        let _ = writeln!(h, "  prefix:    {}", r.resolved_prefix);
        let v = &r.verification;
        let _ = writeln!(h, "  depth:     {}", v.depth_level);
        if v.legacy_run {
            let _ = writeln!(
                h,
                "  status:    legacy_run (no manifest at this prefix — a pre-0.7.0 export, \
                 or nothing was ever written here)"
            );
            continue;
        }
        if !v.manifest_found {
            let _ = writeln!(h, "  status:    NO MANIFEST");
            for failure in &v.failures {
                let _ = writeln!(h, "  failure:   [{}] {}", failure.error_code(), failure);
            }
            continue;
        }
        let _ = writeln!(
            h,
            "  status:    {}",
            if v.passed { "PASSED" } else { "FAILED" }
        );
        let _ = writeln!(
            h,
            "  parts:     {} verified ({} md5, {} size-only), {} failed",
            v.parts_verified,
            v.parts_md5_verified,
            v.parts_verified.saturating_sub(v.parts_md5_verified),
            v.parts_failed
        );
        let _ = writeln!(
            h,
            "  _SUCCESS:  {}",
            if v.success_marker_consistent {
                "consistent"
            } else if v.failures.iter().any(|f| matches!(
                f,
                crate::pipeline::validate_manifest::Failure::SuccessMarkerStale { .. }
                    | crate::pipeline::validate_manifest::Failure::SuccessMarkerMalformed { .. }
                    | crate::pipeline::validate_manifest::Failure::SuccessMarkerReadError { .. }
            )) {
                "INCONSISTENT (see failures)"
            } else {
                "absent (no signal)"
            }
        );
        let _ = writeln!(
            h,
            "  manifest:  {}",
            if v.manifest_self_consistent {
                "self-consistent"
            } else {
                "INCONSISTENT (see failures)"
            }
        );
        for failure in &v.failures {
            let label = if failure.is_fatal() {
                "failure:"
            } else {
                "warning:"
            };
            let _ = writeln!(h, "  {}   [{}] {}", label, failure.error_code(), failure);
        }
    }

    if !hard_failures.is_empty() {
        let _ = writeln!(h);
        let _ = writeln!(h, "── errors ──");
        for e in hard_failures {
            let _ = writeln!(h, "  {}", e);
        }
    }
    let _ = h.flush();
}

/// Serialize one [`ManifestVerificationFailure`] to JSON with its stable `RIVET_VERIFY_*` code injected next to `kind`.
fn failure_json(f: &crate::pipeline::validate_manifest::Failure) -> serde_json::Value {
    let mut value = serde_json::json!(f);
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "code".to_string(),
            serde_json::Value::String(f.error_code().to_string()),
        );
    }
    value
}

/// Serialize a [`ManifestVerification`] to JSON, replacing the derive's plain `failures` array with one whose entries each carry their `RIVET_VERIFY_*` `code`.
fn verification_json(v: &ManifestVerification) -> serde_json::Value {
    let mut value = serde_json::json!(v);
    if let Some(obj) = value.as_object_mut() {
        let failures: Vec<serde_json::Value> = v.failures.iter().map(failure_json).collect();
        obj.insert("failures".to_string(), serde_json::Value::Array(failures));
    }
    value
}

fn render_json(
    results: &[ExportVerdict],
    hard_failures: &[String],
    out_path: Option<String>,
) -> Result<()> {
    let warnings: Vec<serde_json::Value> = results
        .iter()
        .flat_map(|r| {
            r.verification
                .failures
                .iter()
                .filter(|f| !f.is_fatal())
                .map(move |f| {
                    serde_json::json!({
                        "export_name": r.name,
                        "warning": failure_json(f),
                    })
                })
        })
        .collect();

    let payload = serde_json::json!({
        "exports": results
            .iter()
            .map(|r| {
                serde_json::json!({
                    "export_name": r.name,
                    "resolved_prefix": r.resolved_prefix,
                    "verification": verification_json(&r.verification),
                })
            })
            .collect::<Vec<_>>(),
        "warnings": warnings,
        "errors": hard_failures,
    });
    let serialized = serde_json::to_string_pretty(&payload)?;
    match out_path {
        Some(p) => {
            std::fs::write(Path::new(&p), &serialized)?;
            log::info!("rivet validate: wrote JSON report to {}", p);
        }
        None => {
            println!("{}", serialized);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_default_uses_today() {
        let target = ValidateTarget::default();
        let ctx = target.placeholder_context("orders");
        assert_eq!(ctx.date, Some(chrono::Utc::now().date_naive()));
        assert_eq!(ctx.export_name, "orders");
        assert!(ctx.run_id.is_none());
    }

    #[test]
    fn target_with_date_overrides_today() {
        let target = ValidateTarget {
            date: Some(NaiveDate::from_ymd_opt(2026, 5, 21).unwrap()),
            ..Default::default()
        };
        let ctx = target.placeholder_context("orders");
        assert_eq!(ctx.date, NaiveDate::from_ymd_opt(2026, 5, 21));
        assert!(ctx.run_id.is_none());
    }

    #[test]
    fn target_composes_date_and_run_id() {
        let target = ValidateTarget {
            date: Some(NaiveDate::from_ymd_opt(2026, 5, 21).unwrap()),
            run_id: Some("r-abc123".into()),
            prefix_override: None,
            ..Default::default()
        };
        let ctx = target.placeholder_context("orders");
        assert_eq!(ctx.date, NaiveDate::from_ymd_opt(2026, 5, 21));
        assert_eq!(ctx.run_id.as_deref(), Some("r-abc123"));
    }

    #[test]
    fn resolved_prefix_prefers_cloud_prefix_over_path() {
        let dest = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::S3,
            prefix: Some("exports/2026-05-21/orders/".into()),
            path: Some("/scratch".into()),
            ..Default::default()
        };
        assert_eq!(
            resolved_prefix_for_display(&dest),
            "exports/2026-05-21/orders/",
        );
    }

    #[test]
    fn resolved_prefix_falls_back_to_path_when_prefix_missing() {
        let dest = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            prefix: None,
            path: Some("/data/out".into()),
            ..Default::default()
        };
        assert_eq!(resolved_prefix_for_display(&dest), "/data/out");
    }

    use crate::pipeline::validate_manifest::Failure as VFailure;

    /// Verdict shape `verify_at_destination` returns when `manifest.json` exists but cannot be read.
    fn read_error_verdict() -> ManifestVerification {
        ManifestVerification {
            legacy_run: false,
            failures: vec![VFailure::ManifestReadError {
                detail: "permission denied".into(),
            }],
            ..ManifestVerification::legacy()
        }
    }

    #[test]
    fn exit_gate_counts_manifest_read_error_as_failure() {
        assert!(verdict_fails_exit(&read_error_verdict()));
    }

    #[test]
    fn exit_gate_keeps_legacy_run_at_zero() {
        assert!(!verdict_fails_exit(&ManifestVerification::legacy()));
    }

    /// The legacy verdict says what the check ESTABLISHED.
    #[test]
    fn the_legacy_verdict_names_both_causes_and_asserts_neither() {
        let verdict = ExportVerdict {
            name: "orders".into(),
            resolved_prefix: "./output/orders/".into(),
            verification: ManifestVerification::legacy(),
            recorded_as: None,
        };
        let mut out: Vec<u8> = Vec::new();
        render_pretty(&mut out, std::slice::from_ref(&verdict), &[]);
        let text = String::from_utf8(out).expect("render is utf8");

        assert!(
            text.contains("no manifest at this prefix"),
            "the line must state what was OBSERVED: {text}"
        );
        assert!(
            text.contains("or nothing was ever written here"),
            "…and the cause the old wording denied by omission: {text}"
        );
    }

    #[test]
    fn exit_gate_keeps_advisory_untracked_at_zero() {
        let v = ManifestVerification {
            manifest_found: true,
            legacy_run: false,
            passed: true,
            parts_verified: 1,
            failures: vec![VFailure::UntrackedObject {
                key: "stray.parquet".into(),
                size_bytes: 9,
            }],
            ..ManifestVerification::legacy()
        };
        assert!(!verdict_fails_exit(&v));
    }

    #[test]
    fn exit_gate_counts_fatal_failure_on_found_manifest() {
        let v = ManifestVerification {
            manifest_found: true,
            legacy_run: false,
            failures: vec![VFailure::PartMissing {
                part_id: 1,
                path: "part-000001.parquet".into(),
            }],
            ..ManifestVerification::legacy()
        };
        assert!(verdict_fails_exit(&v));
    }

    #[test]
    fn value_checksum_mismatch_flips_verdict_and_fails_exit_gate() {
        let reclassified = ManifestVerification {
            manifest_found: true,
            legacy_run: false,
            passed: false,
            failures: vec![VFailure::ValueChecksumMismatch {
                detail: "export 'e': column 'id' checksum differs".into(),
            }],
            ..ManifestVerification::legacy()
        };
        assert!(
            verdict_fails_exit(&reclassified),
            "a value-checksum mismatch must fail the exit gate (exit 3), not pass silently"
        );

        let not_reclassified = ManifestVerification {
            manifest_found: true,
            legacy_run: false,
            passed: true,
            failures: vec![VFailure::ValueChecksumMismatch {
                detail: "same corruption, left in hard_failures".into(),
            }],
            ..ManifestVerification::legacy()
        };
        assert!(
            !verdict_fails_exit(&not_reclassified),
            "with passed still true the gate wrongly passes — this is exactly bug #104"
        );
    }

    use crate::manifest::{
        MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, ManifestStatus,
        PartStatus, RunManifest,
    };

    fn success_manifest(parts: Vec<ManifestPart>) -> RunManifest {
        let row_count: i64 = parts.iter().map(|p| p.rows).sum();
        let part_count = parts.len() as u32;
        RunManifest {
            split_window: None,
            checksum_render: None,
            row_hash: None,
            mode: "batch".to_string(),
            manifest_version: MANIFEST_VERSION,
            run_id: "r-validate-cmd".into(),
            export_name: "orders".into(),
            export_family: String::new(),
            started_at: "2026-06-09T12:00:00Z".into(),
            finished_at: "2026-06-09T12:01:00Z".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: Some("public".into()),
                table: Some("orders".into()),
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///tmp/out".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0123456789abcdef".into(),
            row_count,
            part_count,
            parts,
            column_checksums: None,
            checksum_key_column: None,
        }
    }

    /// Land `manifest.json` + `_SUCCESS` at `prefix` via the public writer surface.
    fn stage_dataset(prefix: &Path, m: &RunManifest) {
        std::fs::create_dir_all(prefix).unwrap();
        let dest = crate::destination::create_destination(&crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(prefix.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap();
        crate::pipeline::manifest_writer::write_manifest(&*dest, m).unwrap();
    }

    /// Config with a single export pointing at `prefix`.
    fn write_cfg(dir: &Path, prefix: &Path) -> std::path::PathBuf {
        let cfg = dir.join("rivet.yaml");
        let yaml = format!(
            "source:\n  type: postgres\n  url: postgresql://nobody@localhost/nope\nexports:\n  - name: orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: \"{}\"\n",
            prefix.to_string_lossy()
        );
        std::fs::write(&cfg, yaml).unwrap();
        cfg
    }

    /// In-process twin of the live roast test (tests/roast_validate_exit.rs).
    #[cfg(unix)]
    #[test]
    fn unreadable_manifest_fails_the_command() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset(&prefix, &success_manifest(Vec::new()));
        let cfg = write_cfg(dir.path(), &prefix);

        let manifest_path = prefix.join(crate::manifest::MANIFEST_FILENAME);
        std::fs::set_permissions(&manifest_path, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read(&manifest_path).is_ok() {
            return crate::test_hook::skip_live("running as root: file modes are ignored");
        }

        let report = dir.path().join("report.json");
        let err = run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget::default(),
        )
        .expect_err("an unreadable manifest is an explicit failure, not exit 0");
        assert!(
            format!("{err:#}").contains("could not be verified"),
            "got: {err:#}"
        );
        assert!(
            err.downcast_ref::<crate::error::DataIntegrityError>()
                .is_none(),
            "a read error must not be classed as data-integrity (exit 3): {err:#}"
        );

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let verification = &json["exports"][0]["verification"];
        assert_eq!(verification["manifest_found"], false);
        assert_eq!(verification["legacy_run"], false);
        assert_eq!(verification["failures"][0]["kind"], "manifest_read_error");
    }

    #[test]
    fn untracked_surplus_alone_keeps_exit_zero() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset(&prefix, &success_manifest(Vec::new()));
        std::fs::write(prefix.join("rogue.parquet"), b"XX").unwrap();
        let cfg = write_cfg(dir.path(), &prefix);

        let report = dir.path().join("report.json");
        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget::default(),
        )
        .expect("advisory untracked surplus must not flip the exit code");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let verification = &json["exports"][0]["verification"];
        assert_eq!(verification["passed"], true);
        assert_eq!(verification["failures"][0]["kind"], "untracked_object");

        let warnings = json["warnings"].as_array().expect("warnings array present");
        assert_eq!(warnings.len(), 1, "the untracked surplus is one warning");
        assert_eq!(warnings[0]["export_name"], "orders");
        assert_eq!(warnings[0]["warning"]["kind"], "untracked_object");
        assert_eq!(warnings[0]["warning"]["key"], "rogue.parquet");
    }

    #[test]
    fn json_warnings_array_is_empty_when_no_advisory_failures() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset(&prefix, &success_manifest(Vec::new()));
        let cfg = write_cfg(dir.path(), &prefix);

        let report = dir.path().join("report.json");
        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget::default(),
        )
        .expect("a clean dataset must pass");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        assert_eq!(
            json["warnings"]
                .as_array()
                .expect("warnings array present")
                .len(),
            0,
            "no surplus → no warnings"
        );
    }

    #[test]
    fn multiplex_cdc_validates_each_table_sub_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("cdc");
        stage_dataset(&base.join("alpha"), &success_manifest(Vec::new()));
        stage_dataset(&base.join("beta"), &success_manifest(Vec::new()));
        let cfg = write_multiplex_cfg(dir.path(), &base);

        let report = dir.path().join("report.json");
        run_validate_command(
            cfg.to_str().unwrap(),
            None,
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget {
                depth: ValidateDepth::Sample,
                ..Default::default()
            },
        )
        .expect("both table sub-prefixes are complete — the stream must validate");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let names: Vec<&str> = json["exports"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["export_name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["cdc/alpha", "cdc/beta"], "per-table descent");
        for e in json["exports"].as_array().unwrap() {
            assert_eq!(e["verification"]["passed"], true, "each table passes");
        }
    }

    fn write_multiplex_cfg(dir: &Path, base: &Path) -> std::path::PathBuf {
        let cfg = dir.join("rivet-cdc.yaml");
        let yaml = format!(
            "source:\n  type: mysql\n  url: mysql://nobody@localhost/nope\nexports:\n  - name: cdc\n    tables: [alpha, beta]\n    mode: cdc\n    format: parquet\n    cdc:\n      server_id: 1\n      checkpoint: /tmp/ck_mx\n    destination:\n      type: local\n      path: \"{}\"\n",
            base.to_string_lossy()
        );
        std::fs::write(&cfg, yaml).unwrap();
        cfg
    }

    #[test]
    fn single_table_cdc_certifies_snapshot_and_hides_its_untracked() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("cdc");
        stage_dataset(&base, &success_manifest(Vec::new()));
        stage_dataset(&base.join("snapshot"), &success_manifest(Vec::new()));
        let cfg = write_single_cdc_cfg(dir.path(), &base);

        let report = dir.path().join("report.json");
        run_validate_command(
            cfg.to_str().unwrap(),
            None,
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget {
                depth: ValidateDepth::Sample,
                ..Default::default()
            },
        )
        .expect("snapshot + change datasets are complete");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let exports = json["exports"].as_array().unwrap();
        let names: Vec<&str> = exports
            .iter()
            .map(|e| e["export_name"].as_str().unwrap())
            .collect();
        assert!(
            names.contains(&"cdc/snapshot"),
            "snapshot certified: {names:?}"
        );
        assert!(names.contains(&"cdc"), "change-prefix verdict: {names:?}");
        let change = exports.iter().find(|e| e["export_name"] == "cdc").unwrap();
        let failures = change["verification"]["failures"].as_array().unwrap();
        assert!(
            failures.iter().all(|f| f["kind"] != "untracked_object"),
            "snapshot files must not read as untracked surplus: {failures:?}"
        );
    }

    fn write_single_cdc_cfg(dir: &Path, base: &Path) -> std::path::PathBuf {
        let cfg = dir.join("rivet-single-cdc.yaml");
        let yaml = format!(
            "source:\n  type: mysql\n  url: mysql://nobody@localhost/nope\nexports:\n  - name: cdc\n    table: t\n    mode: cdc\n    format: parquet\n    cdc:\n      initial: snapshot\n      server_id: 1\n      checkpoint: ./ck\n    destination:\n      type: local\n      path: \"{}\"\n",
            base.to_string_lossy()
        );
        std::fs::write(&cfg, yaml).unwrap();
        cfg
    }

    #[test]
    fn missing_part_fails_the_command() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        let m = success_manifest(vec![ManifestPart {
            part_id: 1,
            path: "part-000001.parquet".into(),
            rows: 10,
            size_bytes: 4,
            content_fingerprint: "xxh3:1111111111111111".into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }]);
        stage_dataset(&prefix, &m);
        let cfg = write_cfg(dir.path(), &prefix);

        let err = run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(None),
            ValidateTarget::default(),
        )
        .expect_err("a missing committed part must fail verification");
        assert!(
            format!("{err:#}").contains("1 export(s) failed verification"),
            "got: {err:#}"
        );
    }

    /// `--prefix` at a real, complete dataset still passes.
    #[test]
    fn prefix_override_with_real_manifest_passes() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset(&prefix, &success_manifest(Vec::new()));
        let cfg = write_cfg(dir.path(), &prefix);

        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(None),
            ValidateTarget {
                prefix_override: Some(prefix.to_string_lossy().into_owned()),
                ..Default::default()
            },
        )
        .expect("a real dataset under a pinned --prefix must pass");
    }

    /// `--prefix` at a never-written directory FAILS (exit non-zero).
    #[test]
    fn prefix_override_at_absent_manifest_fails() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_prefix = dir.path().join("cfg_dest");
        std::fs::create_dir_all(&cfg_prefix).unwrap();
        let cfg = write_cfg(dir.path(), &cfg_prefix);
        let empty_prefix = dir.path().join("never_written");
        std::fs::create_dir_all(&empty_prefix).unwrap();

        let report = dir.path().join("report.json");
        let err = run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget {
                prefix_override: Some(empty_prefix.to_string_lossy().into_owned()),
                ..Default::default()
            },
        )
        .expect_err("a never-written prefix pinned via --prefix must fail, not legacy-pass");
        assert!(
            format!("{err:#}").contains("1 export(s) failed verification"),
            "got: {err:#}"
        );

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let verification = &json["exports"][0]["verification"];
        assert_eq!(verification["manifest_found"], false);
        assert_eq!(verification["legacy_run"], false);
        assert_eq!(
            verification["failures"][0]["kind"],
            "manifest_required_but_absent"
        );
    }

    /// A `partition_by` export whose source cannot be read is refused with its cause and a remedy that works without the source.
    #[test]
    fn a_partitioned_export_with_an_unreadable_source_is_refused_and_its_remedy_works() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("out");
        let yaml = format!(
            "source:\n  type: postgres\n  url: postgresql://nobody@127.0.0.1:1/nope\nexports:\n  - name: events\n    table: events\n    mode: full\n    format: parquet\n    partition_by: created_at\n    destination:\n      type: local\n      path: \"{}/{{partition}}\"\n",
            root.to_string_lossy()
        );
        let cfg = dir.path().join("rivet.yaml");
        std::fs::write(&cfg, yaml).unwrap();

        let err = run_validate_command(
            cfg.to_str().unwrap(),
            Some("events"),
            ValidateOutputFormat::Json(None),
            ValidateTarget::default(),
        )
        .expect_err("an unreadable source cannot list the partitions");
        let text = format!("{err:#}");
        assert!(
            text.contains("validate could not list the partitions of a `partition_by` export")
                && text.contains("rivet validate --export <name> --prefix <partition directory>"),
            "the refusal names its cause and its remedy: {text}"
        );
        assert!(
            crate::error::error_code(&err).is_some(),
            "the refusal is coded: {text}"
        );

        let partition = root.join("created_at=2026-03-01");
        stage_dataset(&partition, &success_manifest(Vec::new()));
        run_validate_command(
            cfg.to_str().unwrap(),
            Some("events"),
            ValidateOutputFormat::Json(None),
            ValidateTarget {
                prefix_override: Some(partition.to_string_lossy().into_owned()),
                ..Default::default()
            },
        )
        .expect("the remedy verifies one partition with the source unreachable");
    }

    /// Without `--prefix`, an absent manifest stays the benign M6 legacy-run pass (exit 0).
    #[test]
    fn absent_manifest_without_prefix_override_stays_legacy_pass() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        std::fs::create_dir_all(&prefix).unwrap();
        let cfg = write_cfg(dir.path(), &prefix);

        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(None),
            ValidateTarget::default(),
        )
        .expect("an absent manifest with no pinned --prefix is a legacy pass (exit 0)");
    }

    /// Stage a dataset that passes sections 1-5 (manifest reads + is self-consistent, the single part is present at the recorded size, `_SUCCESS` matches) **but** records a non-empty `column_checksums`, so the Form B re-read is *reachable*.
    fn stage_dataset_form_b_would_fail(prefix: &Path) {
        std::fs::create_dir_all(prefix).unwrap();
        let part_body: &[u8] = b"AAAA";
        std::fs::write(prefix.join("part-000001.parquet"), part_body).unwrap();

        let mut m = success_manifest(vec![ManifestPart {
            part_id: 1,
            path: "part-000001.parquet".into(),
            rows: 1,
            size_bytes: part_body.len() as u64,
            content_fingerprint: "xxh3:1111111111111111".into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }]);
        m.column_checksums = Some(vec![crate::manifest::ColumnChecksum {
            name: "id".into(),
            checksum: "0".into(),
        }]);
        stage_dataset(prefix, &m);
    }

    #[test]
    fn sample_depth_does_not_run_form_b() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset_form_b_would_fail(&prefix);
        let cfg = write_cfg(dir.path(), &prefix);

        let report = dir.path().join("report.json");
        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget {
                depth: ValidateDepth::Sample,
                ..Default::default()
            },
        )
        .expect("sample depth skips Form B, so a non-Parquet part still passes");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let verification = &json["exports"][0]["verification"];
        assert_eq!(verification["passed"], true);
        assert_eq!(verification["parts_verified"], 1, "sample reconciles parts");
        assert_eq!(verification["depth_level"], "sample");
    }

    #[test]
    fn full_depth_runs_form_b() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset_form_b_would_fail(&prefix);
        let cfg = write_cfg(dir.path(), &prefix);

        let err = run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(None),
            ValidateTarget {
                depth: ValidateDepth::Full,
                ..Default::default()
            },
        )
        .expect_err("full depth runs Form B, which fails on a non-Parquet part");
        assert!(
            format!("{err:#}").contains("1 export(s) failed verification"),
            "got: {err:#}"
        );
    }

    #[test]
    fn json_report_carries_failure_code_and_depth_level() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        let m = success_manifest(vec![ManifestPart {
            part_id: 1,
            path: "part-000001.parquet".into(),
            rows: 10,
            size_bytes: 4,
            content_fingerprint: "xxh3:1111111111111111".into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }]);
        stage_dataset(&prefix, &m);
        let cfg = write_cfg(dir.path(), &prefix);

        let report = dir.path().join("report.json");
        let _ = run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget {
                depth: ValidateDepth::Sample,
                ..Default::default()
            },
        )
        .expect_err("a missing part fails the command");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let verification = &json["exports"][0]["verification"];
        assert_eq!(verification["depth_level"], "sample");
        let failure = &verification["failures"][0];
        assert_eq!(failure["kind"], "part_missing");
        assert_eq!(failure["code"], "RIVET_VERIFY_PART_MISSING");
        assert_eq!(failure["part_id"], 1);
    }

    #[test]
    fn json_warning_entry_also_carries_its_code() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset(&prefix, &success_manifest(Vec::new()));
        std::fs::write(prefix.join("rogue.parquet"), b"XX").unwrap();
        let cfg = write_cfg(dir.path(), &prefix);

        let report = dir.path().join("report.json");
        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget::default(),
        )
        .expect("advisory untracked surplus must not flip the exit code");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let warning = &json["warnings"][0]["warning"];
        assert_eq!(warning["kind"], "untracked_object");
        assert_eq!(warning["code"], "RIVET_VERIFY_UNTRACKED_OBJECT");
        assert_eq!(json["exports"][0]["verification"]["depth_level"], "full");
    }

    /// A bare verdict for `export` at `uri`, recorded or not.
    fn bare_verdict(export: &str, uri: &str, recorded: bool) -> ExportVerdict {
        ExportVerdict {
            name: export.into(),
            resolved_prefix: "out".into(),
            verification: ManifestVerification::legacy(),
            recorded_as: recorded.then(|| (export.to_string(), uri.to_string())),
        }
    }

    /// One finished run of `export` at `uri` with `status`, written into the state beside `cfg`.
    fn record_run(cfg: &Path, export: &str, run_id: &str, uri: &str, status: &str) {
        let s = crate::state::StateStore::open(cfg.to_str().unwrap()).unwrap();
        s.begin_run(run_id, export, uri, "2026-01-01T00:00:00Z")
            .unwrap();
        s.finish_run(run_id, status, "2026-01-01T00:01:00Z")
            .unwrap();
    }

    /// A bare verdict whose export the state records as delivered there becomes a fatal failure that fails the exit gate.
    #[test]
    fn a_bare_verdict_of_a_recorded_delivery_becomes_a_fatal_failure() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = write_cfg(dir.path(), &dir.path().join("out"));
        record_run(&cfg, "orders", "r1", "file:///data/out", "success");
        let mut results = vec![bare_verdict("orders", "file:///data/out", true)];
        settle_recorded_deliveries(cfg.to_str().unwrap(), &mut results).unwrap();
        let v = &results[0].verification;
        assert!(v.has_failures() && !v.legacy_run && verdict_fails_exit(v));
    }

    /// A bare verdict with no recorded success for its export at its prefix stays the legacy pass.
    #[test]
    fn a_bare_verdict_without_a_recorded_success_stays_legacy() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = write_cfg(dir.path(), &dir.path().join("out"));
        record_run(&cfg, "orders", "r1", "file:///data/out", "failed");
        record_run(&cfg, "users", "r2", "file:///data/out", "success");
        record_run(&cfg, "orders", "r3", "file:///data/elsewhere", "success");
        let mut results = vec![bare_verdict("orders", "file:///data/out", true)];
        settle_recorded_deliveries(cfg.to_str().unwrap(), &mut results).unwrap();
        let v = &results[0].verification;
        assert!(v.is_bare_legacy() && !verdict_fails_exit(v));
    }

    /// With no state file the settle step opens nothing, creates nothing and changes no verdict.
    #[test]
    fn settling_without_a_state_file_creates_none_and_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = write_cfg(dir.path(), &dir.path().join("out"));
        let mut results = vec![bare_verdict("orders", "file:///data/out", true)];
        settle_recorded_deliveries(cfg.to_str().unwrap(), &mut results).unwrap();
        assert!(results[0].verification.is_bare_legacy());
        assert!(!dir.path().join(".rivet_state.db").exists());
    }

    /// An unreadable state is read only for a bare recorded verdict.
    #[test]
    fn an_unreadable_state_is_read_only_for_a_bare_recorded_verdict() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = write_cfg(dir.path(), &dir.path().join("out"));
        std::fs::write(
            dir.path().join(".rivet_state.db"),
            "not a sqlite database ".repeat(60),
        )
        .unwrap();
        let decided = ManifestVerification {
            manifest_found: true,
            legacy_run: false,
            ..ManifestVerification::legacy()
        };
        let mut skipped = vec![
            bare_verdict("orders", "file:///data/out", false),
            ExportVerdict {
                verification: decided,
                ..bare_verdict("orders", "file:///data/out", true)
            },
        ];
        settle_recorded_deliveries(cfg.to_str().unwrap(), &mut skipped).unwrap();
        let mut read = vec![bare_verdict("orders", "file:///data/out", true)];
        let err = settle_recorded_deliveries(cfg.to_str().unwrap(), &mut read).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_STATE_FILE_CORRUPT"),
            "{err:#}"
        );
    }

    /// The recorded identity `verify_one_prefix` hands to the settle step.
    #[test]
    fn only_a_batch_export_without_a_pinned_prefix_carries_a_recorded_identity() {
        let dir = tempfile::tempdir().unwrap();
        let batch = write_cfg(dir.path(), &dir.path().join("out"));
        let cdc = write_single_cdc_cfg(dir.path(), &dir.path().join("cdc-out"));
        let recorded = |cfg: &Path, prefix: Option<&str>| {
            let export = Config::load_with_params(cfg.to_str().unwrap(), None)
                .unwrap()
                .exports
                .remove(0);
            let target = ValidateTarget {
                prefix_override: prefix.map(str::to_string),
                ..ValidateTarget::default()
            };
            let (mut results, mut hard) = (Vec::new(), Vec::new());
            verify_one_prefix(
                export.destination.clone(),
                export.name.clone(),
                &export,
                &target,
                false,
                false,
                &mut results,
                &mut hard,
            );
            assert_eq!(results.len(), 1, "{hard:?}");
            results.remove(0).recorded_as
        };
        let (name, uri) = recorded(&batch, None).expect("a batch export is settled");
        assert!(name == "orders" && uri.starts_with("file://") && uri.ends_with("/out"));
        assert!(recorded(&batch, Some("/elsewhere")).is_none());
        assert!(recorded(&cdc, None).is_none());
    }
}
