//! **Layer: Coordinator** (post-run finalization steps)

use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::state::StateStore;

use super::summary::RunSummary;

/// ADR-0028: THE export tail — the one place the per-export post-write features are applied, called by the dispatcher (`job.rs`) on runner success, before `finalize_manifest`.
pub(super) fn finalize_export(
    plan: &ResolvedRunPlan,
    state: Option<&StateStore>,
    summary: &mut RunSummary,
) -> Result<()> {
    let ledger = std::mem::take(&mut summary.ledger);

    if let Some(schema) = &ledger.observed.drift_schema {
        super::manifest_writer::record_run_schema_fingerprint(summary, schema);
    }
    super::commit::harvest_column_checksums(summary, ledger.integrity);

    if let (Some(schema), Some(st)) = (&ledger.observed.drift_schema, state) {
        super::schema_drift::check_from_sink_schema(
            st,
            &plan.export_name,
            schema,
            plan.schema_drift_policy,
            summary,
        )?;
    }

    if plan.shape_drift_warn_factor > 0.0
        && !ledger.observed.column_max_bytes.is_empty()
        && let Some(st) = state
    {
        match st.detect_shape_drift(
            &plan.export_name,
            &ledger.observed.column_max_bytes,
            plan.shape_drift_warn_factor,
        ) {
            Ok(warnings) => {
                for w in &warnings {
                    log::warn!(
                        "export '{}': shape drift in column '{}' — \
                         max byte length grew {:.1}× ({} → {} bytes); \
                         set `shape_drift_warn_factor` to a higher value to suppress",
                        plan.export_name,
                        w.column,
                        w.growth_factor,
                        w.stored_max_bytes,
                        w.current_max_bytes,
                    );
                    summary.journal.record(crate::journal::RunEvent::Warning {
                        context: format!("shape_drift:{}", w.column),
                        message: format!(
                            "column '{}' max byte length grew {:.1}× ({} → {} bytes)",
                            w.column, w.growth_factor, w.stored_max_bytes, w.current_max_bytes
                        ),
                    });
                }
            }
            Err(e) => log::warn!(
                "export '{}': shape tracking error: {:#}",
                plan.export_name,
                e
            ),
        }
    }

    apply_quality_gate(plan, &ledger.observed.quality, summary)
}

/// Grade the run's merged quality measurements against `plan.quality`.
fn apply_quality_gate(
    plan: &ResolvedRunPlan,
    measured: &crate::quality::QualityTracker,
    summary: &mut RunSummary,
) -> Result<()> {
    let Some(qc) = &plan.quality else {
        return Ok(());
    };
    let issues = measured.issues_for(qc, summary.total_rows as usize);
    let mut fails = Vec::new();
    for issue in &issues {
        let level = match issue.severity {
            crate::quality::Severity::Fail => "FAIL",
            crate::quality::Severity::Warn => "WARN",
        };
        log::warn!("quality {}: {}", level, issue.message);
        summary
            .journal
            .record(crate::journal::RunEvent::QualityIssue {
                severity: level.to_string(),
                message: issue.message.clone(),
            });
        if issue.severity == crate::quality::Severity::Fail {
            fails.push(issue.message.as_str());
        }
    }
    summary.quality_passed = Some(fails.is_empty());
    if fails.is_empty() {
        return Ok(());
    }
    Err(
        crate::error::DataIntegrityError::new(crate::quality::failure_message(
            &plan.export_name,
            &fails,
        ))
        .into(),
    )
}

/// ADR-0028, the FAILURE half of the seam (bughunt 2026-08-21, MED).
pub(super) fn finalize_export_records(summary: &mut RunSummary) {
    let ledger = std::mem::take(&mut summary.ledger);
    if let Some(schema) = &ledger.observed.drift_schema {
        super::manifest_writer::record_run_schema_fingerprint(summary, schema);
    }
    super::commit::harvest_column_checksums(summary, ledger.integrity);
}

/// Write `.rivet/runs/<run_id>/{summary.md,summary.json}` and surface a stderr hint pointing at the report (plus a resume command, when applicable).
pub(super) fn finalize_run_report(config_path: &str, summary: &RunSummary, kind: &str) {
    use std::io::Write;

    let dir = crate::pipeline::report::report_dir(config_path, &summary.run_id);
    let written = match crate::pipeline::report::write_run_report(config_path, summary, kind) {
        Ok(_) => true,
        Err(e) => {
            log::warn!(
                "{} '{}': run report write failed (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
            false
        }
    };

    if crate::pipeline::ipc::capturing_events() {
        return;
    }

    let stderr = std::io::stderr();
    let mut h = stderr.lock();
    if written && !crate::pipeline::multi_export_mode() {
        let _ = writeln!(h, "report:    {}", dir.join("summary.md").display());
    }
    if let Some(cmd) = crate::pipeline::report::resume_command(summary, config_path, kind) {
        let _ = writeln!(h, "resume:    {cmd}");
    }
    let _ = h.flush();
}

/// Build the cloud-output manifest from the run's accumulated parts and write it (plus `_SUCCESS` for clean runs) to the destination.
pub(super) fn run_put_nothing_at_the_destination(summary: &RunSummary, landed: u64) -> bool {
    summary.status != "success" && landed == 0
}

/// What the end of a run did to its prefix: why it is not consumable (`gap`), and whether the run wrote nothing there at all.
pub(super) struct Finalized {
    pub gap: Option<String>,
    pub left_alone: bool,
}

/// Whether the prefix already holds a canonical manifest, an earlier run's record of what is there.
fn prefix_holds_a_manifest(dest: &dyn crate::destination::Destination) -> bool {
    !matches!(dest.head(crate::manifest::MANIFEST_FILENAME), Ok(None))
}

pub(super) fn finalize_manifest(
    plan: &ResolvedRunPlan,
    export_family: &str,
    state: &StateStore,
    summary: &RunSummary,
    kind: &str,
) -> Finalized {
    let mut left_alone = false;
    let gap = write_run_manifest(plan, export_family, state, summary, kind, &mut left_alone);
    Finalized { gap, left_alone }
}

fn write_run_manifest(
    plan: &ResolvedRunPlan,
    export_family: &str,
    state: &StateStore,
    summary: &RunSummary,
    kind: &str,
    left_alone: &mut bool,
) -> Option<String> {
    use crate::manifest::ManifestStatus;
    use crate::pipeline::manifest_writer::{
        ManifestBuilder, WriteOutcome, write_manifest, write_manifest_keep_canonical_no_success,
    };

    if let Err(e) = summary.check_post_run_invariants(plan.resume) {
        if cfg!(debug_assertions) {
            panic!(
                "summary↔manifest coherence violated at finalize_manifest \
                 for {} '{}': {}",
                kind, summary.export_name, e
            );
        }
        log::warn!(
            "{} '{}': run-integrity invariant violated — {}. The run's data is \
             committed, but its integrity records are incomplete; treat this \
             run's manifest as unverified and report it.",
            kind,
            summary.export_name,
            e
        );
    }

    let snapshot = match summary.journal.plan_snapshot() {
        Some(s) => s,
        None => {
            log::debug!(
                "{} '{}': no plan snapshot, manifest skipped",
                kind,
                summary.export_name
            );
            return None;
        }
    };

    let landed = plan.parts_landed.load(std::sync::atomic::Ordering::Relaxed);
    let nothing = run_put_nothing_at_the_destination(summary, landed);
    if nothing && summary.status == "skipped" {
        *left_alone = true;
        return None;
    }

    let status = match summary.status.as_str() {
        "success" => ManifestStatus::Success,
        "failed" => ManifestStatus::Failed,
        _ => ManifestStatus::Interrupted,
    };

    let schema_fingerprint = summary
        .schema_fingerprint
        .clone()
        .or_else(|| {
            state
                .get_stored_schema(&summary.export_name)
                .ok()
                .flatten()
                .map(|cols| crate::state::schema_fingerprint(&cols))
        })
        .unwrap_or_else(|| crate::manifest::SCHEMA_FINGERPRINT_UNAVAILABLE.to_string());

    let source_engine = plan.source.source_type.label();

    let mongo = plan.source.source_type == crate::config::SourceType::Mongo;
    let (source_schema, source_table) = match plan.source_table.as_deref() {
        Some(t) if mongo => (None, Some(t.to_string())),
        Some(t) => match t.split_once('.') {
            Some((s, tbl)) if !s.is_empty() && !tbl.is_empty() => {
                (Some(s.to_string()), Some(tbl.to_string()))
            }
            _ => (None, Some(t.to_string())),
        },
        None if mongo => (None, None),
        None => match summary.export_name.split_once('.') {
            Some((s, t)) if !s.is_empty() && !t.is_empty() => {
                (Some(s.to_string()), Some(t.to_string()))
            }
            _ => (None, None),
        },
    };

    let started_at = summary
        .journal
        .entries
        .first()
        .map(|e| e.recorded_at)
        .unwrap_or_else(chrono::Utc::now);

    let mut builder = ManifestBuilder::new(
        snapshot,
        export_family,
        &summary.run_id,
        started_at,
        schema_fingerprint,
        source_engine,
        source_schema,
        source_table,
        destination_uri_for_manifest(&plan.destination),
    );
    builder.set_split_window(plan.split_window.clone());
    for part in &summary.manifest_parts {
        builder.record_part(
            part.part_id,
            part.path.clone(),
            part.rows,
            part.size_bytes,
            part.content_fingerprint.clone(),
            part.content_md5.clone(),
        );
    }
    if !summary.column_checksums.is_empty() {
        builder.set_column_checksums(
            summary.column_checksums.clone(),
            summary.checksum_key_column.clone(),
        );
    }
    if summary.cursor_column.is_some() || summary.cursor_high.is_some() {
        builder.set_cursor_range(
            summary.cursor_column.clone(),
            None,
            summary.cursor_low.clone(),
            summary.cursor_high.clone(),
            None,
        );
    }
    builder.set_source_row_count(summary.source_count);
    let manifest = builder.finalize(status);

    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            let why = format!("could not create the destination for the manifest write: {e:#}");
            log::error!("{} '{}': {}", kind, summary.export_name, why);
            return Some(why);
        }
    };

    if nothing && prefix_holds_a_manifest(&*dest) {
        *left_alone = true;
        log::info!(
            "{} '{}': the run ({}) put no part at the destination; the manifest and _SUCCESS an earlier run left there stay as they were",
            kind,
            summary.export_name,
            summary.status
        );
        return None;
    }

    let write_result = if plan.is_split_unit {
        write_manifest_keep_canonical_no_success(&*dest, &manifest)
    } else {
        write_manifest(&*dest, &manifest)
    };
    match write_result {
        Ok(WriteOutcome::Written { success_marker }) => {
            log::info!(
                "{} '{}': manifest.json written ({} parts, {} rows){}",
                kind,
                summary.export_name,
                manifest.part_count,
                manifest.row_count,
                if success_marker { " + _SUCCESS" } else { "" },
            );
            None
        }
        Ok(WriteOutcome::SkippedStreaming) => {
            log::info!(
                "{} '{}': manifest skipped (streaming destination)",
                kind,
                summary.export_name,
            );
            None
        }
        Err(e) => {
            let why = format!(
                "the manifest write FAILED, so the prefix has {} durable part(s) that no manifest \
                 names — a manifest-authoritative `rivet load` will not read them: {e:#}",
                manifest.part_count
            );
            log::error!("{} '{}': {}", kind, summary.export_name, why);
            Some(why)
        }
    }
}

/// Run the manifest-aware `--validate` pass against the destination prefix (ADR-0012 M5/M6, ADR-0013).
pub(super) fn should_downgrade_validated(
    v: &crate::pipeline::validate_manifest::ManifestVerification,
    current: Option<bool>,
) -> bool {
    !v.passed && v.manifest_found && current == Some(true)
}

pub(super) fn finalize_validate_manifest(
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    kind: &str,
) {
    use crate::pipeline::validate_manifest::{ValidateDepth, verify_at_destination};

    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            log::warn!(
                "{} '{}': could not create destination for --validate manifest pass (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
            return;
        }
    };
    if !dest.capabilities().commit_protocol.leaves_objects_at_rest() {
        log::debug!(
            "{} '{}': streaming destination — skipping manifest-aware --validate",
            kind,
            summary.export_name
        );
        return;
    }

    match verify_at_destination(&*dest, "", ValidateDepth::Full) {
        Ok(mut v) => {
            v.enforce_content_policy(plan.verify.requires_content());
            if should_downgrade_validated(&v, summary.validated) {
                summary.validated = Some(false);
            }
            log::info!(
                "{} '{}': --validate manifest pass: {} parts verified, {} failed{}{}",
                kind,
                summary.export_name,
                v.parts_verified,
                v.parts_failed,
                if v.success_marker_consistent {
                    " (_SUCCESS consistent)"
                } else if v.manifest_found {
                    ""
                } else {
                    " (legacy_run: no manifest)"
                },
                if v.has_failures() {
                    format!(" — {} issue(s)", v.failures.len())
                } else {
                    String::new()
                },
            );
            summary.manifest_verification = Some(v);
        }
        Err(e) => {
            log::warn!(
                "{} '{}': --validate manifest pass failed (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
        }
    }
}

/// ADR-0012 M8 — refuse to start a `--resume` run against a destination prefix whose `_SUCCESS` marker is already present, unless the operator passed `--force`.
pub(crate) fn write_split_success_marker(
    dest_config: &crate::config::DestinationConfig,
) -> Result<()> {
    use crate::manifest::{MANIFEST_FILENAME, SUCCESS_FILENAME, success_marker_body};
    let dest = crate::destination::create_destination(dest_config)?;
    if !dest.capabilities().commit_protocol.leaves_objects_at_rest() {
        return Ok(());
    }
    let manifest_bytes = dest.read(MANIFEST_FILENAME)?;
    let body = success_marker_body(&manifest_bytes);
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), body.as_bytes())?;
    dest.write(tmp.path(), SUCCESS_FILENAME)?;
    Ok(())
}

/// Whether the export's newest journaled run failed.
pub(crate) fn last_run_failed(state: &StateStore, export_name: &str) -> Result<bool> {
    let newest = state.get_metrics(Some(export_name), 1)?;
    Ok(newest.first().is_some_and(|m| m.status == "failed"))
}

pub(super) fn check_success_gate_for_resume(
    plan: &ResolvedRunPlan,
    state: &StateStore,
) -> Result<()> {
    use crate::manifest::SUCCESS_FILENAME;

    if plan.is_split_unit {
        return Ok(());
    }

    let dest = crate::destination::create_destination(&plan.destination)?;
    if !dest.capabilities().commit_protocol.leaves_objects_at_rest() {
        log::debug!(
            "resume: streaming destination for export '{}' has no prefix; gate skipped",
            plan.export_name
        );
        return Ok(());
    }
    match dest.head(SUCCESS_FILENAME)? {
        Some(_) if last_run_failed(state, &plan.export_name)? => Ok(()),
        Some(_) => crate::rivet_bail!(
            crate::error::codes::DEST_ALREADY_COMPLETE,
            "export '{}': --resume refused: the destination prefix already holds a complete \
             export (_SUCCESS present). Pass --force to go on: the run continues an interrupted \
             run of this export if there is one, else it runs as a plain run does. What it \
             exports lands in new parts beside the old ones (nothing is overwritten) and \
             manifest.json then describes only the new run. Or use a different destination \
             prefix.",
            plan.export_name
        ),
        None => Ok(()),
    }
}

/// Footgun guard for a *fresh* (non-`--resume`) run into a destination prefix that already carries a completed export.
pub(super) fn prefix_guard_applies(commit: crate::destination::WriteCommitProtocol) -> bool {
    commit.leaves_objects_at_rest()
}

pub(super) fn warn_if_prefix_has_completed_run(plan: &ResolvedRunPlan) {
    use crate::manifest::{MANIFEST_FILENAME, SUCCESS_FILENAME};

    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            log::debug!(
                "rerun-guard: could not create destination for export '{}' (skipping pre-run check): {:#}",
                plan.export_name,
                e
            );
            return;
        }
    };
    if !prefix_guard_applies(dest.capabilities().commit_protocol) {
        return;
    }

    let marker = match dest.head(SUCCESS_FILENAME) {
        Ok(Some(_)) => Some(SUCCESS_FILENAME),
        Ok(None) => match dest.head(MANIFEST_FILENAME) {
            Ok(Some(_)) => dest
                .read(MANIFEST_FILENAME)
                .map_or(true, |b| manifest_describes_completed_parts(&b))
                .then_some(MANIFEST_FILENAME),
            Ok(None) => None,
            Err(e) => {
                log::debug!(
                    "rerun-guard: stat {} failed for export '{}' (skipping pre-run check): {:#}",
                    MANIFEST_FILENAME,
                    plan.export_name,
                    e
                );
                return;
            }
        },
        Err(e) => {
            log::debug!(
                "rerun-guard: stat {} failed for export '{}' (skipping pre-run check): {:#}",
                SUCCESS_FILENAME,
                plan.export_name,
                e
            );
            return;
        }
    };

    if let Some(marker) = marker {
        log::warn!(
            "export '{}': {}",
            plan.export_name,
            rerun_warning_message(&destination_uri_for_manifest(&plan.destination), marker),
        );
    }
}

/// Repair the prefix `_SUCCESS` a `--split --resume` found MISSING with every unit already complete.
pub(crate) fn repair_missing_split_marker(
    dest_config: &crate::config::DestinationConfig,
    family: &str,
) {
    let ctx = crate::destination::placeholder::PlaceholderContext::for_today(family);
    let expanded = crate::destination::placeholder::expand_destination(dest_config.clone(), &ctx);
    if destination_has_success(&expanded) {
        return;
    }
    if !crate::pipeline::split::completed_units_tile(dest_config, family) {
        log::warn!(
            "apply --pool --split: prefix _SUCCESS for '{family}' is missing, but the \
             completed unit windows do NOT tile one partition (mixed split generations, \
             or unreadable manifests) — refusing to write a completion marker over a \
             possible gap. Re-run the giant into a fresh prefix, or clear the stale \
             generation's manifests."
        );
        return;
    }
    match write_split_success_marker(&expanded) {
        Ok(()) => log::info!(
            "apply --pool --split: every unit of '{family}' is complete but the prefix \
             _SUCCESS was missing (a crash between the last unit and the marker) — wrote it"
        ),
        Err(e) => log::warn!(
            "apply --pool --split: prefix _SUCCESS for '{family}' is missing and could \
             not be repaired: {e:#}"
        ),
    }
}

/// False when `resume` is set, the export's destination, expanded for today, already holds `_SUCCESS`, and the export's last journaled run did not fail (logged as a skip by `who`).
pub(crate) fn needs_run(
    export: &crate::config::ExportConfig,
    resume: bool,
    who: &str,
    state: &StateStore,
) -> bool {
    if !resume {
        return true;
    }
    let ctx = crate::destination::placeholder::PlaceholderContext::for_today(&export.name);
    let expanded =
        crate::destination::placeholder::expand_destination(export.destination.clone(), &ctx);
    if destination_has_success(&expanded) && !last_run_failed(state, &export.name).unwrap_or(true) {
        log::info!(
            "{who}: skipping '{}' — destination already complete (_SUCCESS)",
            export.name
        );
        return false;
    }
    true
}

/// Whether this destination already holds a completed export (`_SUCCESS`).
pub(crate) fn destination_has_success(dest: &crate::config::DestinationConfig) -> bool {
    use crate::manifest::SUCCESS_FILENAME;
    let Ok(d) = crate::destination::create_destination(dest) else {
        return false;
    };
    if !d.capabilities().commit_protocol.leaves_objects_at_rest() {
        return false;
    }
    matches!(d.head(SUCCESS_FILENAME), Ok(Some(_)))
}

/// The `cursor_high` of the success manifest `export` left at `dest`.
pub(super) fn last_success_cursor_high(
    dest: &crate::config::DestinationConfig,
    export: &str,
) -> Option<String> {
    use crate::manifest::{MANIFEST_FILENAME, ManifestStatus, RunManifest};
    let d = crate::destination::create_destination(dest).ok()?;
    if !d.capabilities().commit_protocol.leaves_objects_at_rest() {
        return None;
    }
    let bytes = super::validate_manifest::read_capped(
        &*d,
        MANIFEST_FILENAME,
        super::validate_manifest::MANIFEST_MAX_BYTES,
    )
    .ok()?;
    let m: RunManifest = serde_json::from_slice(&bytes).ok()?;
    (m.status == ManifestStatus::Success && m.export_name == export)
        .then_some(m.source.extraction?.cursor_high?)
}

/// Whether a prior `manifest.json` names a completed run with parts; unparseable counts as yes.
fn manifest_describes_completed_parts(bytes: &[u8]) -> bool {
    serde_json::from_slice::<crate::manifest::RunManifest>(bytes)
        .map_or(true, |m| m.committed_part_count() > 0)
}

/// The operator-facing body of the rerun-accumulation warning.
fn rerun_warning_message(uri: &str, marker: &str) -> String {
    let way_out = if marker == crate::manifest::SUCCESS_FILENAME {
        "The prior run completed: clear the prefix (or use a new one) before re-running."
    } else {
        "Use --resume to continue the prior run, or clear the prefix first."
    };
    format!(
        "destination prefix '{uri}' already has parts from a prior run ({marker} present) — \
         re-running WITHOUT --resume appends fresh timestamp-named parts alongside the old ones \
         (nothing is overwritten) and rewrites manifest.json to describe only this run, so a glob \
         reader over the prefix will double-count / orphan the old parts. {way_out}"
    )
}

/// Delete the `running` marker written under `run_id` from a cloud prefix.
pub(super) fn retire_running_marker(plan: &ResolvedRunPlan, run_id: &str) {
    use crate::config::DestinationType;
    if matches!(
        plan.destination.destination_type,
        DestinationType::Local | DestinationType::Stdout
    ) {
        return;
    }
    let key = crate::manifest::run_unique_manifest_name(run_id);
    let removed = crate::destination::create_destination(&plan.destination)
        .and_then(|dest| dest.remove(&key));
    if let Err(e) = removed {
        log::debug!(
            "export '{}': could not retire the running marker {key} (not fatal; the ledger \
             says the run ended): {e:#}",
            plan.export_name
        );
    }
}

/// Project the `run_status` ledger's `running` row into the bucket as a schema-less MARKER manifest at run START.
pub(super) fn write_running_manifest(
    plan: &ResolvedRunPlan,
    export_family: &str,
    run_id: &str,
    started_at: &str,
) {
    use crate::config::DestinationType;
    use crate::manifest::{
        MANIFEST_VERSION, ManifestDestination, ManifestSource, ManifestStatus, RunManifest,
    };
    use crate::pipeline::manifest_writer::write_manifest_without_success_marker;

    let kind = match plan.destination.destination_type {
        DestinationType::Gcs => "gcs",
        DestinationType::S3 => "s3",
        DestinationType::Azure => "azure",
        DestinationType::Local | DestinationType::Stdout => return,
    };
    let engine = plan.source.source_type.label();
    let manifest = RunManifest {
        row_hash: None,
        split_window: None,
        manifest_version: MANIFEST_VERSION,
        run_id: run_id.to_string(),
        export_family: export_family.to_string(),
        export_name: plan.export_name.clone(),
        mode: "batch".to_string(),
        started_at: started_at.to_string(),
        finished_at: String::new(),
        status: ManifestStatus::Running,
        source: ManifestSource {
            engine: engine.to_string(),
            schema: None,
            table: None,
            extraction: None,
        },
        destination: ManifestDestination {
            kind: kind.to_string(),
            uri: destination_uri_for_manifest(&plan.destination),
        },
        format: String::new(),
        compression: String::new(),
        schema_fingerprint: String::new(),
        row_count: 0,
        part_count: 0,
        parts: Vec::new(),
        checksum_render: None,
        column_checksums: None,
        checksum_key_column: None,
    };
    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            log::debug!(
                "export '{}': running-manifest destination unavailable (not fatal): {e:#}",
                plan.export_name
            );
            return;
        }
    };
    if let Err(e) = write_manifest_without_success_marker(&*dest, &manifest) {
        log::debug!(
            "export '{}': running-manifest write failed (not fatal; gc uses the ledger): {e:#}",
            plan.export_name
        );
    }
}

pub(crate) use crate::manifest::destination_uri_for_manifest;

#[cfg(test)]
mod tests {
    use crate::plan::test_support::test_plan;

    /// A cloud run PUTs its `running` marker at start; checked against a stub S3 endpoint.
    #[test]
    fn a_cloud_run_writes_its_running_marker_to_the_prefix() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a stub port");
        let addr = listener.local_addr().expect("the stub's address");
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let log = std::sync::Arc::clone(&seen);
        std::thread::spawn(move || {
            while let Ok((mut stream, _)) = listener.accept() {
                let mut req = Vec::new();
                let mut buf = [0u8; 8192];
                let head_end = loop {
                    let Ok(n) = stream.read(&mut buf) else {
                        break None;
                    };
                    if n == 0 {
                        break None;
                    }
                    req.extend_from_slice(&buf[..n]);
                    if let Some(i) = req.windows(4).position(|w| w == b"\r\n\r\n") {
                        break Some(i + 4);
                    }
                };
                let Some(head_end) = head_end else { continue };
                let head = String::from_utf8_lossy(&req[..head_end]).to_string();
                let len: usize = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse().ok())?
                    })
                    .unwrap_or(0);
                while req.len() < head_end + len {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => req.extend_from_slice(&buf[..n]),
                    }
                }
                let line = head.lines().next().unwrap_or_default().to_string();
                let status = if line.starts_with("PUT ") {
                    "200 OK\r\nETag: \"e\""
                } else {
                    "404 Not Found"
                };
                log.lock().unwrap().push(line);
                let _ = stream.write_all(
                    format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .as_bytes(),
                );
            }
        });

        let mut plan = test_plan();
        plan.destination = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::S3,
            bucket: Some("b".into()),
            prefix: Some("p/".into()),
            region: Some("us-east-1".into()),
            endpoint: Some(format!("http://{addr}")),
            access_key_env: Some("PATH".into()),
            secret_key_env: Some("PATH".into()),
            ..Default::default()
        };
        super::write_running_manifest(&plan, "orders", "run_mark_1", "2026-09-29T00:00:00Z");
        let seen = seen.lock().unwrap().clone();
        assert!(
            seen.iter()
                .any(|l| l.starts_with("PUT /b/p/manifest-run_mark_1.json")),
            "no running marker was written: {seen:?}"
        );
    }

    #[test]
    fn the_run_report_lands_beside_the_config() {
        let d = tempfile::tempdir().unwrap();
        let cfg = d.path().join("rivet.yaml");
        let summary = crate::pipeline::RunSummary::stub_for_testing("run_fin_1", "orders");
        super::finalize_run_report(cfg.to_str().unwrap(), &summary, "export");
        let dir = d.path().join(".rivet/runs/run_fin_1");
        assert!(dir.join("summary.json").is_file());
        assert!(dir.join("summary.md").is_file());
    }

    /// The rerun guard must apply to every destination that can actually hold a previous run's parts.
    #[test]
    fn the_rerun_guard_applies_to_every_destination_that_can_hold_a_prior_run() {
        use crate::config::{DestinationConfig, DestinationType};

        let protocol_of = |t: DestinationType, path: Option<String>| {
            let cfg = DestinationConfig {
                destination_type: t,
                path,
                ..Default::default()
            };
            crate::destination::create_destination(&cfg)
                .expect("destination builds")
                .capabilities()
                .commit_protocol
        };

        let tmp = tempfile::tempdir().expect("tempdir");
        let local = protocol_of(
            DestinationType::Local,
            Some(tmp.path().to_string_lossy().into_owned()),
        );
        assert!(
            prefix_guard_applies(local),
            "a local prefix is precisely where a second export overwrites the first"
        );

        let stdout = protocol_of(DestinationType::Stdout, None);
        assert!(
            !prefix_guard_applies(stdout),
            "stdout has no prefix to collide on — probing it would stat nothing"
        );

        assert_ne!(
            local, stdout,
            "if both destinations reported one protocol the assertions above would be vacuous"
        );
    }

    /// The manifest pass overturns a per-file pass ONLY when all three conditions hold.
    #[test]
    fn the_manifest_pass_downgrades_only_a_fatal_verdict_on_a_run_that_had_passed() {
        use crate::pipeline::validate_manifest::ManifestVerification;
        let v = |passed: bool, manifest_found: bool| ManifestVerification {
            passed,
            manifest_found,
            legacy_run: !manifest_found,
            parts_verified: 0,
            parts_md5_verified: 0,
            parts_failed: 0,
            success_marker_consistent: true,
            manifest_self_consistent: true,
            failures: Vec::new(),
            depth_level: "full".into(),
        };

        assert!(
            should_downgrade_validated(&v(false, true), Some(true)),
            "a FATAL manifest verdict on a run that passed its file check is the whole point"
        );
        assert!(
            !should_downgrade_validated(&v(true, true), Some(true)),
            "a passing manifest pass must not fail a run — `delete !` makes it do exactly that"
        );
        assert!(
            !should_downgrade_validated(&v(false, false), Some(true)),
            "a LEGACY run has no manifest to be judged by and keeps the verdict it earned"
        );
        assert!(
            !should_downgrade_validated(&v(false, true), None),
            "there is nothing to downgrade from — writing Some(false) would claim a verdict \
             the run never reached"
        );
        assert!(
            !should_downgrade_validated(&v(false, true), Some(false)),
            "already failed; the downgrade is not a second opinion"
        );
    }

    use super::*;
    use crate::config::{DestinationConfig, DestinationType};

    fn cfg_local(path: Option<&str>, prefix: Option<&str>) -> DestinationConfig {
        DestinationConfig {
            destination_type: DestinationType::Local,
            prefix: prefix.map(str::to_string),
            path: path.map(str::to_string),
            ..Default::default()
        }
    }

    /// ADR-0028: the seam applies the ledger — Form-B checksums land on the summary (keyed), and the ledger is TAKEN (emptied) so a hypothetical second application cannot double-record.
    #[test]
    fn finalize_export_records_applies_pin_and_harvest_on_the_failure_path() {
        use arrow::datatypes::{DataType, Field, Schema};
        let mut summary = crate::pipeline::summary::RunSummary::default();
        summary
            .ledger
            .observe(crate::pipeline::commit::Observations {
                drift_schema: Some(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                ..Default::default()
            });
        summary.ledger.contribute_checksums(
            crate::pipeline::commit::UnitId::Run,
            &[("id".to_string(), 5u64)].into(),
            Some("id".into()),
        );

        finalize_export_records(&mut summary);

        assert!(
            summary.schema_fingerprint.is_some(),
            "a FAILED run must record the fingerprint it OBSERVED — the Failed              manifest otherwise falls back to the stale open baseline"
        );
        assert_eq!(
            summary
                .column_checksums
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id"],
            "a FAILED run must still harvest Form-B for its durable parts"
        );
        assert!(
            summary.ledger.integrity.column_checksums.is_empty(),
            "ledger is taken"
        );
    }

    #[test]
    fn finalize_export_harvests_the_ledger_and_takes_it() {
        let dir = tempfile::tempdir().unwrap();
        let plan = fin_plan(dir.path());
        let mut summary = crate::pipeline::summary::RunSummary::default();
        summary.ledger.contribute_checksums(
            crate::pipeline::commit::UnitId::Run,
            &[("v".to_string(), 9u64)].into(),
            Some("id".into()),
        );

        finalize_export(&plan, None, &mut summary).expect("state-free seam must succeed");

        assert_eq!(
            summary
                .column_checksums
                .iter()
                .map(|c| (c.name.as_str(), c.checksum.as_str()))
                .collect::<Vec<_>>(),
            vec![("v", "9")],
            "the seam must harvest the ledger's Form-B checksums into the summary"
        );
        assert_eq!(summary.checksum_key_column.as_deref(), Some("id"));
        assert!(
            summary.ledger.integrity.column_checksums.is_empty(),
            "the seam must TAKE the ledger — a second application must find nothing"
        );

        finalize_export(&plan, None, &mut summary).expect("idempotent on an empty ledger");
        assert_eq!(summary.column_checksums.len(), 1);
    }

    /// ADR-0028: the seam's shape-drift arm — the ONE guard deciding whether the advisory warn runs.
    #[test]
    fn finalize_export_shape_warn_fires_only_when_enabled_and_fed() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::StateStore::open_in_memory().unwrap();

        let shape_of = |bytes: u64| -> std::collections::HashMap<String, u64> {
            [("payload".to_string(), bytes)].into()
        };
        let warned = |summary: &crate::pipeline::summary::RunSummary| {
            summary.journal.warnings().iter().any(|e| {
                matches!(&e.event, crate::journal::RunEvent::Warning { context, .. }
                    if context.starts_with("shape_drift:"))
            })
        };

        state
            .detect_shape_drift("public.orders", &shape_of(10), 2.0)
            .unwrap();

        let mut plan = fin_plan(dir.path());
        plan.shape_drift_warn_factor = 2.0;
        let mut summary = crate::pipeline::summary::RunSummary::default();
        summary
            .ledger
            .observe(crate::pipeline::commit::Observations {
                column_max_bytes: shape_of(100),
                ..Default::default()
            });
        finalize_export(&plan, Some(&state), &mut summary).unwrap();
        assert!(
            warned(&summary),
            "a 10× column growth with warn_factor 2.0 must journal a shape_drift warning"
        );

        let mut plan = fin_plan(dir.path());
        plan.shape_drift_warn_factor = 0.0;
        let mut summary = crate::pipeline::summary::RunSummary::default();
        summary
            .ledger
            .observe(crate::pipeline::commit::Observations {
                column_max_bytes: shape_of(100_000),
                ..Default::default()
            });
        finalize_export(&plan, Some(&state), &mut summary).unwrap();
        assert!(
            !warned(&summary),
            "shape_drift_warn_factor 0.0 means DISABLED — the seam must not warn"
        );
    }

    fn quality(
        rows: Option<usize>,
        nulls: &[(&str, f64)],
        unique: &[&str],
    ) -> crate::config::QualityConfig {
        crate::config::QualityConfig {
            row_count_min: rows,
            row_count_max: None,
            null_ratio_max: nulls.iter().map(|(c, r)| (c.to_string(), *r)).collect(),
            unique_columns: unique.iter().map(|c| c.to_string()).collect(),
            unique_max_entries: Some(10),
        }
    }

    /// One observation carrying `nulls` NULLs in `name` and `ids` hashed under `id`.
    fn measured(nulls: usize, ids: &[u64]) -> crate::pipeline::commit::Observations {
        let mut quality = crate::quality::QualityTracker::default();
        quality.null_counts.insert("name".into(), nulls);
        quality
            .unique_sets
            .insert("id".into(), ids.iter().copied().collect());
        quality
            .unique_non_null_counts
            .insert("id".into(), ids.len());
        crate::pipeline::commit::Observations {
            quality,
            ..Default::default()
        }
    }

    /// The seam is the quality gate's one home: with no rules it decides nothing, with rules it records the verdict, and a `Fail` issue is a typed exit-3 error whose text is the shared failure contract.
    #[test]
    fn finalize_export_grades_the_quality_rules_once_for_every_runner() {
        use crate::pipeline::summary::RunSummary;
        let dir = tempfile::tempdir().unwrap();

        let plan = fin_plan(dir.path());
        let mut summary = RunSummary {
            total_rows: 3,
            ..Default::default()
        };
        finalize_export(&plan, None, &mut summary).unwrap();
        assert_eq!(summary.quality_passed, None, "no rules, no verdict");

        let mut plan = fin_plan(dir.path());
        plan.quality = Some(quality(Some(3), &[], &[]));
        let mut summary = RunSummary {
            total_rows: 3,
            ..Default::default()
        };
        finalize_export(&plan, None, &mut summary).unwrap();
        assert_eq!(summary.quality_passed, Some(true));

        plan.quality = Some(quality(Some(100), &[], &[]));
        let mut summary = RunSummary::default();
        let err = finalize_export(&plan, None, &mut summary).expect_err("3 < 100 must fail");
        assert!(
            err.downcast_ref::<crate::error::DataIntegrityError>()
                .is_some(),
            "a quality failure is a typed data-integrity error"
        );
        assert_eq!(crate::error::classify_exit(&err), 3);
        let msg = err.to_string();
        assert!(
            msg.contains("export 'public.orders': 1 quality check(s) failed:\n  - row_count 0 below minimum 100"),
            "{msg}"
        );
        assert_eq!(summary.quality_passed, Some(false));
        assert!(
            summary.journal.quality_issues().iter().any(|e| matches!(
                &e.event,
                crate::journal::RunEvent::QualityIssue { severity, .. } if severity == "FAIL"
            )),
            "the failing check is journaled"
        );
    }

    /// The gate reads the MERGED measurements: two sinks that each saw `id = 7` once are a duplicate together, and their NULL counts add into one ratio.
    #[test]
    fn finalize_export_quality_gate_merges_every_sinks_measurements() {
        use crate::pipeline::summary::RunSummary;
        let dir = tempfile::tempdir().unwrap();
        let mut plan = fin_plan(dir.path());
        plan.quality = Some(quality(None, &[("name", 0.5)], &["id"]));

        let mut summary = RunSummary {
            total_rows: 4,
            ..Default::default()
        };
        summary.ledger.observe(measured(1, &[7, 8]));
        summary.ledger.observe(measured(2, &[7, 9]));
        let err = finalize_export(&plan, None, &mut summary).expect_err("merged must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("2 quality check(s) failed:\n  - column 'name': null ratio 0.7500 exceeds threshold 0.5000\n  - column 'id': 1 duplicate values out of 4 rows"),
            "{msg}"
        );

        let mut summary = RunSummary {
            total_rows: 4,
            ..Default::default()
        };
        let mut capped = measured(0, &[1, 2, 3, 4]);
        capped.quality.unique_capped.insert("id".into());
        summary.ledger.observe(capped);
        finalize_export(&plan, None, &mut summary).unwrap();
        assert_eq!(summary.quality_passed, Some(true));
        assert!(
            summary.journal.quality_issues().iter().any(|e| matches!(
                &e.event,
                crate::journal::RunEvent::QualityIssue { severity, message }
                    if severity == "WARN" && message.contains("capped at 10 entries")
            )),
            "the cap warning is journaled"
        );
    }

    fn fin_plan(dest: &std::path::Path) -> crate::plan::ResolvedRunPlan {
        crate::plan::ResolvedRunPlan {
            export_name: "public.orders".into(),
            destination: cfg_local(Some(&dest.to_string_lossy()), None),
            ..test_plan()
        }
    }

    /// Every corner of "did the run put anything at the destination".
    #[test]
    fn only_an_unsuccessful_run_with_no_part_at_the_destination_leaves_the_prefix_alone() {
        let with_part = |status: &str| {
            let mut s = RunSummary::stub_for_testing("r", String::from("e"));
            s.status = status.into();
            s.manifest_parts.push(crate::manifest::ManifestPart {
                part_id: 1,
                path: "part-000001.parquet".into(),
                rows: 5,
                size_bytes: 7,
                content_fingerprint: "xxh3:1".into(),
                content_md5: String::new(),
                status: crate::manifest::PartStatus::Committed,
            });
            s
        };
        let bare = |status: &str| {
            let mut s = RunSummary::stub_for_testing("r", String::from("e"));
            s.status = status.into();
            s
        };

        for status in ["skipped", "failed", "interrupted"] {
            assert!(
                run_put_nothing_at_the_destination(&bare(status), 0),
                "a {status} run wrote no part"
            );
            assert!(
                run_put_nothing_at_the_destination(&with_part(status), 0),
                "a {status} run that only adopted the parts of the run it resumes wrote none"
            );
            assert!(
                !run_put_nothing_at_the_destination(&bare(status), 1),
                "a {status} run whose part landed, in a manifest or not, wrote"
            );
        }
        assert!(
            !run_put_nothing_at_the_destination(&bare("success"), 0),
            "a successful run always writes its manifest, parts or not"
        );
        assert!(!run_put_nothing_at_the_destination(
            &with_part("success"),
            1
        ));
    }

    #[test]
    fn the_only_run_of_a_prefix_records_its_failure_and_a_skipped_one_records_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let plan = fin_plan(dir.path());
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let bare = |status: &str| {
            let mut s = fin_summary(&plan, status);
            s.manifest_parts.clear();
            s
        };

        let skipped = finalize_manifest(&plan, "e", &state, &bare("skipped"), "export");
        assert!(skipped.left_alone && skipped.gap.is_none());
        assert!(
            std::fs::read_dir(dir.path()).unwrap().next().is_none(),
            "a skipped run with no parts writes nothing, manifest or not"
        );

        let failed = finalize_manifest(&plan, "e", &state, &bare("failed"), "export");
        assert!(!failed.left_alone && failed.gap.is_none());
        assert_eq!(
            read_manifest(dir.path()).status,
            crate::manifest::ManifestStatus::Failed,
            "the prefix held no manifest: the failed run's is the one record there"
        );
        assert!(!dir.path().join("_SUCCESS").exists());

        let before = std::fs::read(dir.path().join("manifest.json")).unwrap();
        let mut again = bare("failed");
        again.run_id = "finrun2".into();
        assert!(finalize_manifest(&plan, "e", &state, &again, "export").left_alone);
        assert_eq!(
            std::fs::read(dir.path().join("manifest.json")).unwrap(),
            before,
            "the next failed run leaves the earlier record as it is"
        );
    }

    #[test]
    fn a_run_that_failed_before_its_first_write_leaves_the_complete_export_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let plan = fin_plan(dir.path());
        let state = crate::state::StateStore::open_in_memory().unwrap();
        finalize_manifest(&plan, "e", &state, &fin_summary(&plan, "success"), "export");
        let before = |name: &str| std::fs::read(dir.path().join(name)).unwrap();
        let (marker, manifest) = (before("_SUCCESS"), before("manifest.json"));

        let mut never_connected = fin_summary(&plan, "failed");
        never_connected.run_id = "finrun2".into();
        never_connected.manifest_parts.clear();
        let done = finalize_manifest(&plan, "e", &state, &never_connected, "export");

        assert_eq!(
            (done.gap, done.left_alone),
            (None, true),
            "an untouched prefix is not a manifest gap"
        );
        assert_eq!(before("_SUCCESS"), marker);
        assert_eq!(before("manifest.json"), manifest);
        let mut resumed = fin_summary(&plan, "failed");
        resumed.run_id = "finrun3".into();
        assert!(
            finalize_manifest(&plan, "e", &state, &resumed, "export").left_alone,
            "a resume that adopted parts and wrote none of its own leaves the record too"
        );
        assert_eq!(before("manifest.json"), manifest);
        assert!(
            !dir.path().join("manifest-finrun2.json").exists(),
            "the failed run leaves no per-run manifest either"
        );

        plan.parts_landed
            .store(1, std::sync::atomic::Ordering::Relaxed);
        finalize_manifest(&plan, "e", &state, &never_connected, "export");
        assert_eq!(
            read_manifest(dir.path()).status,
            crate::manifest::ManifestStatus::Failed,
            "a part landed: the prefix is no longer the export the marker described"
        );
        assert!(!dir.path().join("_SUCCESS").exists());
    }

    /// A summary that carries everything finalize_manifest is supposed to surface into the manifest.
    fn fin_summary(plan: &crate::plan::ResolvedRunPlan, status: &str) -> RunSummary {
        let mut s = RunSummary::stub_for_testing("finrun", plan.export_name.clone());
        s.status = status.into();
        s.schema_fingerprint = Some("xxh3:00000000feedface".into());
        s.manifest_parts.push(crate::manifest::ManifestPart {
            part_id: 1,
            path: "part-000001.parquet".into(),
            rows: 5,
            size_bytes: 7,
            content_fingerprint: "xxh3:1".into(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        });
        s.column_checksums = vec![crate::manifest::ColumnChecksum {
            name: "id".into(),
            checksum: "42".into(),
        }];
        s.checksum_key_column = Some("id".into());
        s.cursor_column = Some("updated_at".into());
        s.cursor_low = Some("2026-01-01".into());
        s.cursor_high = Some("2026-02-01".into());
        s.journal
            .record(crate::journal::RunEvent::PlanResolved(Box::new(
                crate::journal::PlanSnapshot {
                    row_hash: None,
                    export_name: plan.export_name.clone(),
                    base_query: plan.base_query.clone(),
                    strategy: "snapshot".into(),
                    format: "parquet".into(),
                    compression: "none".into(),
                    destination_type: "local".into(),
                    tuning_profile: "balanced".into(),
                    batch_size: 1000,
                    validate: false,
                    reconcile: false,
                    resume: false,
                    chunk_key: None,
                    resumable: false,
                },
            )));
        s
    }

    fn read_manifest(dir: &std::path::Path) -> crate::manifest::RunManifest {
        serde_json::from_slice(&std::fs::read(dir.join("manifest.json")).unwrap()).unwrap()
    }

    #[test]
    fn the_rerun_warning_counts_only_a_successful_manifest_with_parts_as_a_prior_run() {
        let bytes_for = |status: &str, keep_parts: bool| {
            let dir = tempfile::tempdir().unwrap();
            let plan = fin_plan(dir.path());
            let state = crate::state::StateStore::open_in_memory().unwrap();
            let mut summary = fin_summary(&plan, status);
            if !keep_parts {
                summary.manifest_parts.clear();
                summary.total_rows = 0;
            }
            finalize_manifest(&plan, "e", &state, &summary, "export");
            std::fs::read(dir.path().join("manifest.json")).unwrap()
        };
        assert!(manifest_describes_completed_parts(&bytes_for(
            "success", true
        )));
        assert!(
            !manifest_describes_completed_parts(&bytes_for("failed", false)),
            "a failed run with no parts left nothing a re-run could double-count"
        );
        assert!(
            manifest_describes_completed_parts(&bytes_for("failed", true)),
            "a failed run's durable parts double-count under a re-run too"
        );
        assert!(!manifest_describes_completed_parts(&bytes_for(
            "success", false
        )));
        assert!(manifest_describes_completed_parts(b"not json"));
    }

    #[test]
    fn finalize_manifest_success_writes_full_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let plan = fin_plan(dir.path());
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let summary = fin_summary(&plan, "success");

        finalize_manifest(&plan, "e", &state, &summary, "export");

        let m = read_manifest(dir.path());
        assert_eq!(m.status, crate::manifest::ManifestStatus::Success);
        assert_eq!(m.run_id, "finrun");
        assert_eq!(m.parts.len(), 1);
        assert_eq!(m.row_count, 5);
        assert_eq!(m.schema_fingerprint, "xxh3:00000000feedface");
        assert_eq!(
            m.column_checksums.as_ref().map(|c| c.len()),
            Some(1),
            "Form B checksums must land in the manifest"
        );
        assert_eq!(m.checksum_key_column.as_deref(), Some("id"));
        let ex = m.source.extraction.as_ref().expect("extraction section");
        assert_eq!(ex.cursor_low.as_deref(), Some("2026-01-01"));
        assert_eq!(ex.cursor_high.as_deref(), Some("2026-02-01"));
        assert_eq!(m.source.engine, "postgres");
        assert_eq!(m.source.schema.as_deref(), Some("public"));
        assert_eq!(m.source.table.as_deref(), Some("orders"));
        assert!(
            dir.path().join("_SUCCESS").exists(),
            "success run gets _SUCCESS"
        );
        assert!(
            dir.path().join("manifest-finrun.json").exists(),
            "run-unique manifest copy beside the canonical"
        );
    }

    #[test]
    fn finalize_manifest_failed_run_records_failed_and_no_success_marker() {
        let dir = tempfile::tempdir().unwrap();
        let plan = fin_plan(dir.path());
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let summary = fin_summary(&plan, "failed");

        finalize_manifest(&plan, "e", &state, &summary, "export");

        let m = read_manifest(dir.path());
        assert_eq!(m.status, crate::manifest::ManifestStatus::Failed);
        assert!(
            !dir.path().join("_SUCCESS").exists(),
            "a failed run must never leave a _SUCCESS marker"
        );
    }

    #[test]
    fn finalize_manifest_splits_schema_table_only_on_a_real_dot() {
        for name in ["orders", "public.", ".orders"] {
            let dir = tempfile::tempdir().unwrap();
            let plan = fin_plan(dir.path());
            let state = crate::state::StateStore::open_in_memory().unwrap();
            let mut summary = fin_summary(&plan, "success");
            summary.export_name = name.into();

            finalize_manifest(&plan, "e", &state, &summary, "export");

            let m = read_manifest(dir.path());
            assert_eq!(m.source.schema, None, "export_name {name:?}");
            assert_eq!(m.source.table, None, "export_name {name:?}");
        }
    }

    /// The declared table is the source's identity.
    #[test]
    fn a_declared_table_is_split_per_engine_and_a_mongo_name_is_never_split() {
        use crate::config::SourceType::{Mongo, Postgres};
        for (engine, declared, schema, table) in [
            (
                Postgres,
                Some("public.orders"),
                Some("public"),
                Some("orders"),
            ),
            (Postgres, Some("orders"), None, Some("orders")),
            (Postgres, Some(".orders"), None, Some(".orders")),
            (Postgres, Some("public."), None, Some("public.")),
            (Mongo, Some("bh4.orders"), None, Some("bh4.orders")),
            (Mongo, Some("orders"), None, Some("orders")),
            (Mongo, None, None, None),
        ] {
            let case = format!("{engine:?} table {declared:?}");
            let dir = tempfile::tempdir().unwrap();
            let mut plan = fin_plan(dir.path());
            plan.source.source_type = engine;
            plan.source_table = declared.map(str::to_string);
            let state = crate::state::StateStore::open_in_memory().unwrap();
            let summary = fin_summary(&plan, "success");
            assert_eq!(summary.export_name, "public.orders");

            finalize_manifest(&plan, "e", &state, &summary, "export");

            let m = read_manifest(dir.path());
            assert_eq!(m.source.schema.as_deref(), schema, "{case}");
            assert_eq!(m.source.table.as_deref(), table, "{case}");
        }
    }

    /// The cursor range is the run's incremental evidence.
    #[test]
    fn a_cursor_range_is_recorded_when_the_run_knows_the_column_or_the_high_bound() {
        for (column, high) in [
            (Some("updated_at"), None),
            (None, Some("2026-02-01")),
            (None, None),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let plan = fin_plan(dir.path());
            let state = crate::state::StateStore::open_in_memory().unwrap();
            let mut summary = fin_summary(&plan, "success");
            summary.cursor_column = column.map(str::to_string);
            summary.cursor_high = high.map(str::to_string);

            finalize_manifest(&plan, "e", &state, &summary, "export");

            let m = read_manifest(dir.path());
            let ex = m.source.extraction.as_ref().expect("extraction section");
            let case = format!("column {column:?} high {high:?}");
            assert_eq!(ex.cursor_column.as_deref(), column, "{case}");
            assert_eq!(ex.cursor_high.as_deref(), high, "{case}");
            let low = (column.is_some() || high.is_some()).then_some("2026-01-01");
            assert_eq!(ex.cursor_low.as_deref(), low, "{case}");
        }
    }

    /// The end-of-run manifest pass reads the destination.
    #[test]
    fn the_manifest_pass_withdraws_validated_when_a_declared_part_is_not_at_the_destination() {
        let dir = tempfile::tempdir().unwrap();
        let plan = fin_plan(dir.path());
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let mut summary = fin_summary(&plan, "success");
        summary.validated = Some(true);

        finalize_validate_manifest(&plan, &mut summary, "export");
        assert_eq!(
            summary.validated,
            Some(true),
            "a prefix with no manifest keeps the file-row verdict"
        );

        finalize_manifest(&plan, "e", &state, &summary, "export");
        assert!(dir.path().join("manifest.json").is_file());
        assert!(!dir.path().join("part-000001.parquet").exists());
        finalize_validate_manifest(&plan, &mut summary, "export");
        assert_eq!(
            summary.validated,
            Some(false),
            "the manifest declares a part the destination does not hold"
        );
    }

    /// A run into a prefix that already holds a completed run is warned before it writes; an empty prefix is silent.
    #[test]
    fn a_run_into_a_prefix_holding_a_completed_run_is_warned_and_an_empty_prefix_is_not() {
        use crate::pipeline::run::run_tail_tests::{
            captured_warnings_mentioning, install_warn_capture,
        };
        install_warn_capture();
        let warned = |export: &str, marker: Option<&str>| {
            let dir = tempfile::tempdir().unwrap();
            if let Some(marker) = marker {
                std::fs::write(dir.path().join(marker), b"").unwrap();
            }
            let mut plan = fin_plan(dir.path());
            plan.export_name = export.into();
            warn_if_prefix_has_completed_run(&plan);
            captured_warnings_mentioning(&format!("export '{export}'"))
        };
        assert_eq!(warned("rerun_guard_empty", None), Vec::<String>::new());
        let said = warned("rerun_guard_done", Some(crate::manifest::SUCCESS_FILENAME));
        assert_eq!(said.len(), 1, "{said:?}");
        assert!(
            said[0].contains("already has parts from a prior run (_SUCCESS present)"),
            "{said:?}"
        );
    }

    #[test]
    fn success_gate_refuses_resume_over_a_completed_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let plan = fin_plan(dir.path());
        check_success_gate_for_resume(&plan, &crate::state::StateStore::open_in_memory().unwrap())
            .expect("no _SUCCESS -> gate passes");
        std::fs::write(dir.path().join("_SUCCESS"), b"xxh3:0\n").unwrap();
        let err = check_success_gate_for_resume(
            &plan,
            &crate::state::StateStore::open_in_memory().unwrap(),
        )
        .expect_err("_SUCCESS present -> resume must be refused");
        assert_eq!(
            err.downcast_ref::<crate::error::CodedError>()
                .map(|c| c.code()),
            Some("RIVET_DEST_ALREADY_COMPLETE")
        );
        assert_eq!(
            err.to_string(),
            "export 'public.orders': --resume refused: the destination prefix already holds a complete \
             export (_SUCCESS present). Pass --force to go on: the run continues an interrupted \
             run of this export if there is one, else it runs as a plain run does. What it \
             exports lands in new parts beside the old ones (nothing is overwritten) and \
             manifest.json then describes only the new run. Or use a different destination \
             prefix."
        );
    }

    #[test]
    fn destination_has_success_probes_the_marker() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_local(Some(&dir.path().to_string_lossy()), None);
        assert!(!destination_has_success(&cfg), "no marker -> false");
        std::fs::write(dir.path().join("_SUCCESS"), b"xxh3:0\n").unwrap();
        assert!(destination_has_success(&cfg), "marker present -> true");
        let bad = cfg_local(Some("/nonexistent/definitely/missing"), None);
        assert!(!destination_has_success(&bad));
    }

    #[test]
    fn needs_run_skips_only_a_resumed_export_whose_expanded_destination_is_complete() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            "name: orders\nquery: \"SELECT 1\"\nformat: parquet\ndestination:\n  type: local\n  path: {}/{{export}}\n",
            dir.path().display()
        );
        let export: crate::config::ExportConfig = serde_yaml_ng::from_str(&yaml).unwrap();
        let state = crate::state::StateStore::open_in_memory().unwrap();
        assert!(needs_run(&export, true, "t", &state), "no marker -> runs");
        std::fs::create_dir_all(dir.path().join("orders")).unwrap();
        std::fs::write(dir.path().join("orders/_SUCCESS"), b"xxh3:0\n").unwrap();
        assert!(
            !needs_run(&export, true, "t", &state),
            "marker under the expanded path -> skipped"
        );
        assert!(
            needs_run(&export, false, "t", &state),
            "without --resume -> runs"
        );
    }

    /// Journal one run of `orders` with `status`.
    fn journal_a_run(state: &crate::state::StateStore, run_id: &str, status: &str) {
        let plan = fin_plan(std::path::Path::new("/unused"));
        let mut summary = RunSummary::stub_for_testing(run_id, String::from("orders"));
        summary.status = status.into();
        state
            .record_metric_full(&crate::pipeline::job::build_metric_row(
                &summary, &plan, "balanced",
            ))
            .unwrap();
    }

    #[test]
    fn a_resume_over_a_marker_an_earlier_run_left_redoes_the_run_that_failed() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            "name: orders\nquery: \"SELECT 1\"\nformat: parquet\ndestination:\n  type: local\n  path: {}\n",
            dir.path().display()
        );
        let export: crate::config::ExportConfig = serde_yaml_ng::from_str(&yaml).unwrap();
        let mut plan = fin_plan(dir.path());
        plan.export_name = "orders".into();
        let state = crate::state::StateStore::open_in_memory().unwrap();
        std::fs::write(dir.path().join("_SUCCESS"), b"xxh3:0\n").unwrap();
        let settled = |state: &crate::state::StateStore| {
            (
                !needs_run(&export, true, "t", state),
                check_success_gate_for_resume(&plan, state).is_err(),
                last_run_failed(state, "orders").unwrap(),
            )
        };

        assert_eq!(settled(&state), (true, true, false), "no run journaled");
        journal_a_run(&state, "r1", "success");
        assert_eq!(settled(&state), (true, true, false), "the run succeeded");
        journal_a_run(&state, "r2", "failed");
        assert_eq!(
            settled(&state),
            (false, false, true),
            "the marker is r1's; r2 failed before it wrote and is to be done again"
        );
        journal_a_run(&state, "other", "skipped");
        assert_eq!(
            settled(&state),
            (true, true, false),
            "a later run settled it"
        );
    }

    /// The crash-in-[last unit → marker] window repair.
    #[test]
    fn repair_missing_split_marker_writes_only_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_local(Some(&dir.path().to_string_lossy()), None);
        std::fs::write(dir.path().join("manifest.json"), b"{\"parts\":[]}").unwrap();
        for (run, name, lo, hi) in [
            ("u0", "orders#0", None, Some("m")),
            ("u1", "orders#1", Some("m"), None),
        ] {
            let mut m = split_unit_success(run, name);
            m.split_window = Some(crate::manifest::SplitWindow {
                key_column: "id".into(),
                lo: lo.map(str::to_string),
                hi: hi.map(str::to_string),
            });
            std::fs::write(
                dir.path().join(format!("manifest-{run}.json")),
                serde_json::to_vec(&m).unwrap(),
            )
            .unwrap();
        }
        assert!(!destination_has_success(&cfg), "fixture: marker absent");
        repair_missing_split_marker(&cfg, "orders");
        assert!(
            destination_has_success(&cfg),
            "every unit complete + no marker -> the resume path must repair it"
        );
        let body = std::fs::read_to_string(dir.path().join("_SUCCESS")).unwrap();
        std::fs::write(dir.path().join("manifest.json"), b"{\"parts\":[1]}").unwrap();
        repair_missing_split_marker(&cfg, "orders");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("_SUCCESS")).unwrap(),
            body,
            "an existing marker is never clobbered by a later repair pass"
        );

        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_local(Some(&dir.path().to_string_lossy()), None);
        std::fs::write(dir.path().join("manifest.json"), b"{\"parts\":[]}").unwrap();
        for (run, name, lo, hi) in [
            ("u0", "orders#0", None, Some("b1")),
            ("u1", "orders#1", Some("c1"), None),
        ] {
            let mut m = split_unit_success(run, name);
            m.split_window = Some(crate::manifest::SplitWindow {
                key_column: "id".into(),
                lo: lo.map(str::to_string),
                hi: hi.map(str::to_string),
            });
            std::fs::write(
                dir.path().join(format!("manifest-{run}.json")),
                serde_json::to_vec(&m).unwrap(),
            )
            .unwrap();
        }
        repair_missing_split_marker(&cfg, "orders");
        assert!(
            !destination_has_success(&cfg),
            "non-tiling completed windows must never mint a _SUCCESS"
        );
    }

    /// A minimal Success split-unit manifest for the repair fixtures.
    fn split_unit_success(run: &str, name: &str) -> crate::manifest::RunManifest {
        use crate::manifest::*;
        RunManifest {
            manifest_version: MANIFEST_VERSION,
            run_id: run.into(),
            export_name: name.into(),
            export_family: "orders".into(),
            mode: "batch".into(),
            started_at: "2026-08-21T00:00:00Z".into(),
            finished_at: "2026-08-21T00:01:00Z".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: None,
                table: Some("orders".into()),
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///out".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0123456789abcdef".into(),
            row_count: 0,
            part_count: 0,
            parts: vec![],
            column_checksums: None,
            checksum_render: None,
            checksum_key_column: None,
            row_hash: None,
            split_window: None,
        }
    }

    fn cfg_s3(bucket: &str, prefix: Option<&str>) -> DestinationConfig {
        DestinationConfig {
            destination_type: DestinationType::S3,
            bucket: Some(bucket.into()),
            prefix: prefix.map(str::to_string),
            ..Default::default()
        }
    }

    fn cfg_gcs(bucket: &str, prefix: Option<&str>) -> DestinationConfig {
        let mut c = cfg_s3(bucket, prefix);
        c.destination_type = DestinationType::Gcs;
        c
    }

    fn cfg_azure(container: &str, prefix: Option<&str>) -> DestinationConfig {
        let mut c = cfg_s3(container, prefix);
        c.destination_type = DestinationType::Azure;
        c
    }

    #[test]
    fn destination_uri_local_uses_path() {
        assert_eq!(
            destination_uri_for_manifest(&cfg_local(Some("/tmp/out"), None)),
            "file:///tmp/out"
        );
    }

    #[test]
    fn destination_uri_local_falls_back_to_prefix_then_dot() {
        assert_eq!(
            destination_uri_for_manifest(&cfg_local(None, Some("/var/data"))),
            "file:///var/data"
        );
        assert_eq!(
            destination_uri_for_manifest(&cfg_local(None, None)),
            "file://."
        );
    }

    #[test]
    fn destination_uri_s3_with_and_without_prefix() {
        assert_eq!(destination_uri_for_manifest(&cfg_s3("b", None)), "s3://b/");
        assert_eq!(
            destination_uri_for_manifest(&cfg_s3("b", Some("k/"))),
            "s3://b/k/"
        );
    }

    #[test]
    fn destination_uri_gcs_with_and_without_prefix() {
        assert_eq!(destination_uri_for_manifest(&cfg_gcs("b", None)), "gs://b/");
        assert_eq!(
            destination_uri_for_manifest(&cfg_gcs("b", Some("k/"))),
            "gs://b/k/"
        );
    }

    #[test]
    fn destination_uri_azure_with_and_without_prefix() {
        assert_eq!(
            destination_uri_for_manifest(&cfg_azure("c", None)),
            "az://c/"
        );
        assert_eq!(
            destination_uri_for_manifest(&cfg_azure("c", Some("runs/2026/"))),
            "az://c/runs/2026/"
        );
    }

    #[test]
    fn destination_uri_stdout_is_stable() {
        let mut c = cfg_local(None, None);
        c.destination_type = DestinationType::Stdout;
        assert_eq!(destination_uri_for_manifest(&c), "stdout");
    }

    /// Mirrors `tests/audit_rerun.rs::warned_about_existing_prefix`.
    fn audit_matcher_accepts(s: &str) -> bool {
        let s = s.to_lowercase();
        s.contains("_success")
            || s.contains("already has")
            || s.contains("prior completed run")
            || s.contains("would overwrite")
            || s.contains("orphan")
    }

    #[test]
    fn rerun_warning_message_matches_live_audit_matcher_for_success_marker() {
        let msg = rerun_warning_message("file:///tmp/out", "_SUCCESS");
        assert!(
            audit_matcher_accepts(&msg),
            "rerun warning must trip the live audit matcher; message was: {msg}"
        );
        assert!(
            msg.contains("file:///tmp/out"),
            "must name the prefix: {msg}"
        );
        assert!(
            msg.contains("The prior run completed: clear the prefix (or use a new one)")
                && !msg.contains("Use --resume"),
            "a completed prefix points at clearing it, never at --resume (which refuses it): {msg}"
        );
    }

    #[test]
    fn rerun_warning_message_matches_live_audit_matcher_for_manifest_marker() {
        let msg = rerun_warning_message("file:///tmp/out", "manifest.json");
        assert!(
            msg.contains("Use --resume to continue the prior run"),
            "an uncompleted prior run can be resumed: {msg}"
        );
        assert!(
            audit_matcher_accepts(&msg),
            "manifest-only rerun warning must still trip the live audit matcher; message was: {msg}"
        );
    }
}
