//! **Layer: Coordinator**

use std::collections::HashMap;

use crate::destination::Destination;
use crate::error::Result;
use crate::manifest::{MANIFEST_FILENAME, QUARANTINE_PREFIX, RunManifest};
use crate::pipeline::RunSummary;
use crate::pipeline::resume_decisions::{ResumeDecision, build_resume_plan};
use crate::pipeline::validate_manifest::{MANIFEST_MAX_BYTES, read_capped};
use crate::plan::ResolvedRunPlan;
use crate::state::StateStore;

/// Aggregate counters from one M8 reconciliation pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct M8Stats {
    /// Manifest parts whose state row stays `completed` — work the resume run skips entirely.
    pub skipped: usize,
    /// Manifest parts whose state row was reset to `pending` — chunks the resume run will re-export.
    pub reset_for_rewrite: usize,
    /// Manifest parts the matrix flagged for quarantine (size mismatch, fingerprint drift).
    pub reset_for_quarantine: usize,
    /// Quarantine-move successes — count of objects relocated to `_quarantine/<run_id>/` so the next write doesn't share the prefix with stale data.
    pub quarantined_moved: usize,
    /// Quarantine-move failures.
    pub quarantine_move_failures: usize,
    /// Manifest parts whose `path` did not match any chunk_task's `file_name`.
    pub orphan_parts: usize,
}

impl M8Stats {
    /// Whether this resume actually ADOPTED prior-run state.
    pub fn adopted_prior_work(&self) -> bool {
        self.skipped > 0 || self.reset_for_rewrite > 0 || self.reset_for_quarantine > 0
    }
}

/// Apply ADR-0012 M8's decision matrix to the destination state.
fn attempt_key(file_name: &str) -> &str {
    let stem = file_name.rsplit_once('.').map_or(file_name, |(s, _)| s);
    match stem.rsplit_once("_p") {
        Some((head, n)) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => head,
        _ => stem,
    }
}

/// Completed chunk index → the attempt key of the part that completed it.
fn surviving_attempts(
    tasks: &[crate::state::ChunkTaskInfo],
) -> std::collections::BTreeMap<String, String> {
    tasks
        .iter()
        .filter(|t| t.status == "completed")
        .filter_map(|t| {
            t.file_name
                .as_deref()
                .map(|f| (t.chunk_index.to_string(), attempt_key(f).to_string()))
        })
        .collect()
}

/// A chunk part counts for the run only if it is the attempt that COMPLETED its chunk.
fn is_surviving_attempt(
    file_name: &str,
    surviving: &std::collections::BTreeMap<String, String>,
) -> bool {
    match super::chunk_index_of(file_name) {
        Some(idx) => surviving
            .get(idx)
            .is_some_and(|key| attempt_key(file_name) == key),
        None => true,
    }
}

/// Drop from a run's manifest the parts that did not complete their chunk, with their counters, and return their paths.
pub(crate) fn prune_superseded_attempts(
    summary: &mut RunSummary,
    tasks: &[crate::state::ChunkTaskInfo],
) -> Vec<String> {
    let surviving = surviving_attempts(tasks);
    let (kept, pruned): (Vec<_>, Vec<_>) = std::mem::take(&mut summary.manifest_parts)
        .into_iter()
        .partition(|p| is_surviving_attempt(&p.path, &surviving));
    summary.manifest_parts = kept;
    for p in &pruned {
        log::warn!(
            "export '{}': part '{}' is not the part that completed its chunk (a later attempt did, or the chunk is read again) — \
             left out of the manifest",
            summary.export_name,
            p.path
        );
        summary.total_rows -= p.rows;
        summary.bytes_written = summary.bytes_written.saturating_sub(p.size_bytes);
        summary.files_produced = summary.files_produced.saturating_sub(1);
        summary.files_committed = summary.files_committed.saturating_sub(1);
    }
    pruned.into_iter().map(|p| p.path).collect()
}

/// Whether `t` is marked completed with no part and no record of having read zero rows.
fn completed_without_a_part(t: &crate::state::ChunkTaskInfo) -> bool {
    t.status == "completed" && t.file_name.is_none() && t.rows_written != Some(0)
}

/// Completed chunks whose surviving attempt's `file_log` rows are absent or short of the chunk's rows, declared by a manifest or not.
fn under_logged_chunks(
    tasks: &[crate::state::ChunkTaskInfo],
    files: &[crate::state::FileRecord],
) -> Vec<(i64, String)> {
    tasks
        .iter()
        .filter(|t| t.status == "completed")
        .filter_map(|t| t.file_name.as_deref().map(|f| (t, f)))
        .filter(|(_, f)| super::chunk_index_of(f).is_some())
        .filter(|(t, f)| {
            let logged: Vec<i64> = files
                .iter()
                .filter(|r| attempt_key(&r.file_name) == attempt_key(f))
                .map(|r| r.row_count)
                .collect();
            logged.is_empty()
                || t.rows_written
                    .is_some_and(|rows| rows != logged.iter().sum::<i64>())
        })
        .map(|(t, f)| (t.chunk_index, f.to_string()))
        .collect()
}

#[cfg(test)]
pub(crate) fn rehydrate_manifest_parts_from_file_log(
    state: &StateStore,
    run_id: &str,
    summary: &mut RunSummary,
) -> Result<usize> {
    Ok(rehydrate_manifest_parts_probed(state, run_id, summary, None)?.0)
}

/// [`rehydrate_manifest_parts_from_file_log`] with a DESTINATION PRESENCE probe.
pub(crate) type Listed = std::collections::HashMap<String, u64>;

/// One part the probe found MISSING at the destination.
pub(crate) type MissingPart = (Option<u32>, String);

pub(crate) fn rehydrate_manifest_parts_probed(
    state: &StateStore,
    run_id: &str,
    summary: &mut RunSummary,
    present: Option<&Listed>,
) -> Result<(usize, Vec<MissingPart>)> {
    let files = state.list_files_for_run(run_id)?;

    let tasks = state.list_chunk_tasks_for_run(run_id)?;
    let mut surviving = surviving_attempts(&tasks);
    let mut missing: Vec<MissingPart> = Vec::new();
    for (idx, name) in under_logged_chunks(&tasks, &files) {
        surviving.remove(&idx.to_string());
        missing.push((u32::try_from(idx).ok(), name));
    }
    for t in tasks.iter().filter(|t| completed_without_a_part(t)) {
        missing.push((
            u32::try_from(t.chunk_index).ok(),
            format!(
                "(chunk {}: marked completed, no part recorded)",
                t.chunk_index
            ),
        ));
    }

    let mut next_id = summary
        .manifest_parts
        .iter()
        .map(|p| p.part_id)
        .max()
        .unwrap_or(0);
    let mut rehydrated = 0usize;
    for f in files {
        if summary.manifest_parts.iter().any(|p| p.path == f.file_name) {
            continue;
        }
        if !is_surviving_attempt(&f.file_name, &surviving) {
            continue;
        }
        if let Some(present) = present {
            let base = f.file_name.rsplit('/').next().unwrap_or(&f.file_name);
            if present.get(base).copied() != u64::try_from(f.bytes).ok() {
                let idx = super::chunk_index_of(&f.file_name).and_then(|i| i.parse::<u32>().ok());
                surviving.remove(super::chunk_index_of(&f.file_name).unwrap_or_default());
                missing.push((idx, f.file_name.clone()));
                continue;
            }
        }
        next_id += 1;
        let (rows, bytes) = (f.row_count, f.bytes.max(0) as u64);
        let adopted = super::super::commit::adopt_part(
            summary,
            crate::manifest::ManifestPart {
                part_id: next_id,
                path: f.file_name,
                rows,
                size_bytes: bytes,
                content_fingerprint: String::new(),
                content_md5: String::new(),
                status: crate::manifest::PartStatus::Committed,
            },
        );
        if adopted {
            rehydrated += 1;
        }
    }
    if rehydrated > 0 {
        log::info!(
            "resume: reconstructed {rehydrated} committed part(s) into the manifest from the \
             state DB file_log (no \
             destination manifest to hydrate from) — the finalize manifest now covers every \
             committed part, rotation siblings included"
        );
    }
    Ok((rehydrated, missing))
}

pub(crate) fn apply_m8_resume_decisions(
    state: &StateStore,
    run_id: &str,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
) -> Result<M8Stats> {
    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            log::warn!(
                "M8 resume preamble: cannot open destination for export '{}' (not fatal — \
                 falling back to state-only resume): {:#}",
                plan.export_name,
                e
            );
            return Ok(M8Stats::default());
        }
    };
    if !dest.capabilities().commit_protocol.leaves_objects_at_rest() {
        log::debug!(
            "M8 resume preamble: streaming destination for export '{}'; skipped",
            plan.export_name
        );
        return Ok(M8Stats::default());
    }

    let manifest_bytes = match dest.head(MANIFEST_FILENAME) {
        Ok(Some(_)) => match read_capped(&*dest, MANIFEST_FILENAME, MANIFEST_MAX_BYTES) {
            Ok(b) => b,
            Err(e) => {
                log::warn!(
                    "M8 resume preamble: manifest.json present but unreadable on destination \
                     for export '{}' (not fatal): {:#}",
                    plan.export_name,
                    e
                );
                return Ok(M8Stats::default());
            }
        },
        Ok(None) => {
            log::info!(
                "M8 resume preamble: no manifest at destination for export '{}'; \
                 falling back to state-only resume (legacy / fresh prefix)",
                plan.export_name
            );
            let mut stats = M8Stats::default();
            let present = present_basenames(&*dest);
            let content = super::part_keys::ContentCheck::of(plan, &*dest);
            rehydrate_and_reset(
                state,
                run_id,
                &*dest,
                summary,
                present.as_ref(),
                content.as_ref(),
                &mut stats,
            )?;
            return Ok(stats);
        }
        Err(e) => {
            log::warn!(
                "M8 resume preamble: manifest.json head failed for export '{}' (not fatal): {:#}",
                plan.export_name,
                e
            );
            return Ok(M8Stats::default());
        }
    };

    let manifest: RunManifest = match serde_json::from_slice(&manifest_bytes) {
        Ok(m) => m,
        Err(e) => {
            log::warn!(
                "M8 resume preamble: manifest.json at destination did not parse for export '{}' \
                 (not fatal — manifest may be from a future schema version): {:#}",
                plan.export_name,
                e
            );
            return Ok(M8Stats::default());
        }
    };

    if manifest.run_id != run_id {
        log::info!(
            "M8 resume preamble: destination manifest run_id '{}' differs from current resume \
             run_id '{}' for export '{}'; skipping reconciliation (foreign manifest)",
            manifest.run_id,
            run_id,
            plan.export_name
        );
        let mut stats = M8Stats::default();
        let present = present_basenames(&*dest);
        let content = super::part_keys::ContentCheck::of(plan, &*dest);
        rehydrate_and_reset(
            state,
            run_id,
            &*dest,
            summary,
            present.as_ref(),
            content.as_ref(),
            &mut stats,
        )?;
        return Ok(stats);
    }

    let listing = match dest.list_prefix("") {
        Ok(l) => l,
        Err(e) => {
            log::warn!(
                "M8 resume preamble: list_prefix failed for export '{}' (not fatal): {:#}",
                plan.export_name,
                e
            );
            return Ok(M8Stats::default());
        }
    };

    let resume_plan = build_resume_plan(&manifest, &listing);
    let tasks = state.list_chunk_tasks_for_run(run_id)?;
    let surviving = surviving_attempts(&tasks);

    let by_file: HashMap<&str, &crate::state::ChunkTaskInfo> = tasks
        .iter()
        .filter_map(|t| t.file_name.as_deref().map(|n| (n, t)))
        .collect();

    let mut stats = M8Stats::default();

    let manifest_part_by_path: HashMap<&str, &crate::manifest::ManifestPart> = manifest
        .parts
        .iter()
        .filter(|p| p.status == crate::manifest::PartStatus::Committed)
        .map(|p| (p.path.as_str(), p))
        .collect();

    if summary.schema_fingerprint.is_none()
        && manifest.schema_fingerprint != crate::manifest::SCHEMA_FINGERPRINT_UNAVAILABLE
    {
        summary.schema_fingerprint = Some(manifest.schema_fingerprint.clone());
    }

    for (path, decision) in &resume_plan.per_part {
        match decision.decision {
            ResumeDecision::Skip if !is_surviving_attempt(path, &surviving) => {
                quarantine_move(&*dest, path, run_id, &plan.export_name, &mut stats);
            }
            ResumeDecision::Skip => {
                stats.skipped += 1;
                if let Some(p) = manifest_part_by_path.get(path.as_str()) {
                    super::super::commit::adopt_part(summary, (*p).clone());
                }
            }
            ResumeDecision::Rewrite => {
                let Some(task) = by_file.get(path.as_str()) else {
                    stats.orphan_parts += 1;
                    continue;
                };
                let n = state.reset_chunk_task_for_re_export(
                    run_id,
                    task.chunk_index,
                    "M8 reset: manifest part missing at destination",
                )?;
                if n > 0 {
                    stats.reset_for_rewrite += 1;
                }
            }
            ResumeDecision::Quarantine { reason } => {
                let Some(task) = by_file.get(path.as_str()) else {
                    stats.orphan_parts += 1;
                    continue;
                };
                let n = state.reset_chunk_task_for_re_export(
                    run_id,
                    task.chunk_index,
                    &format!("M8 reset: destination part diverged ({:?})", reason),
                )?;
                if n > 0 {
                    stats.reset_for_quarantine += 1;
                }
                quarantine_move(&*dest, path, run_id, &plan.export_name, &mut stats);
            }
        }
    }

    let committed_parts: std::collections::HashSet<String> = state
        .list_files_for_run(run_id)?
        .into_iter()
        .map(|f| f.file_name)
        .collect();
    let mut untracked_surplus = 0usize;
    let own_part_prefix = plan.is_split_unit.then(|| format!("{}_", plan.export_name));
    for key in resume_plan.untracked.keys() {
        if committed_parts.contains(key.as_str()) {
            continue;
        }
        if let Some(prefix) = &own_part_prefix {
            let base = key.rsplit('/').next().unwrap_or(key);
            if !base.starts_with(prefix) {
                continue;
            }
        }
        untracked_surplus += 1;
        quarantine_move(&*dest, key, run_id, &plan.export_name, &mut stats);
    }

    let rehydrated = rehydrate_and_reset(
        state,
        run_id,
        &*dest,
        summary,
        Some(&listing_basenames(&listing)),
        super::part_keys::ContentCheck::of(plan, &*dest).as_ref(),
        &mut stats,
    )?;

    log::info!(
        "M8 resume preamble: export '{}' run_id '{}' — {} skipped, {} reset for rewrite, \
         {} reset for quarantine, {} quarantined ({} move failure(s)), {} orphan part(s), \
         {} untracked surplus object(s), {} part(s) rehydrated from file_log",
        plan.export_name,
        run_id,
        stats.skipped,
        stats.reset_for_rewrite,
        stats.reset_for_quarantine,
        stats.quarantined_moved,
        stats.quarantine_move_failures,
        stats.orphan_parts,
        untracked_surplus,
        rehydrated,
    );

    Ok(stats)
}

/// The basenames of a prefix listing, each with its size.
fn listing_basenames(listing: &[crate::destination::ObjectMeta]) -> Listed {
    listing
        .iter()
        .map(|m| {
            let base = m.key.rsplit('/').next().unwrap_or(&m.key).to_string();
            (base, m.size_bytes)
        })
        .collect()
}

/// The basenames under the destination prefix, or `None` (said out loud) when it cannot be listed.
fn present_basenames(dest: &dyn Destination) -> Option<Listed> {
    match dest.list_prefix("") {
        Ok(listing) => Some(listing_basenames(&listing)),
        Err(e) => {
            log::warn!(
                "resume: cannot list the destination to verify committed parts \
                 ({e:#}) — declaring from the state DB unverified"
            );
            None
        }
    }
}

/// Rehydrate this run's committed parts (probed when `present` is known, read back under `content`) and re-export every chunk whose part is gone, unlogged or not its chunk's.
fn committed_part_gone(name: &str) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::DEST_COMMITTED_PART_GONE,
        format!(
            "resume: committed part '{name}' is GONE from the \
             destination and carries no chunk index to re-export — \
             refusing to finalize a manifest naming a deleted file. \
             Re-run without the checkpoint (`rivet state reset-chunks`) \
             or into a fresh prefix."
        ),
    )
}

fn rehydrate_and_reset(
    state: &StateStore,
    run_id: &str,
    dest: &dyn Destination,
    summary: &mut RunSummary,
    present: Option<&Listed>,
    content: Option<&super::part_keys::ContentCheck<'_>>,
    stats: &mut M8Stats,
) -> Result<usize> {
    let listed = present;
    let checked = match (present, content) {
        (Some(p), Some(c)) => Some(c.keep_parts_of_their_chunk(state, run_id, p)?),
        _ => None,
    };
    let present = checked.as_ref().or(present);
    let (rehydrated, missing) = rehydrate_manifest_parts_probed(state, run_id, summary, present)?;
    let reset = !missing.is_empty();
    for (idx, name) in missing {
        let Some(idx) = idx else {
            return Err(committed_part_gone(&name));
        };
        let n = state.reset_chunk_task_for_re_export(
            run_id,
            idx as i64,
            "M8 reset: committed part missing at destination or from file_log",
        )?;
        if n > 0 {
            stats.reset_for_rewrite += 1;
            log::warn!(
                "resume: committed part '{name}' is gone from the destination, is not the size \
                 the run committed, or was never logged in the state DB — its chunk is \
                 re-exported this run"
            );
        }
    }
    if reset {
        for path in prune_superseded_attempts(summary, &state.list_chunk_tasks_for_run(run_id)?) {
            let base = path.rsplit('/').next().unwrap_or(&path);
            if listed.is_none_or(|l| l.contains_key(base)) {
                quarantine_move(dest, &path, run_id, &summary.export_name, stats);
            }
        }
    }
    Ok(rehydrated)
}

/// ADR-0012 M9 — best-effort move of a destination object to the quarantine prefix.
fn quarantine_move(
    dest: &dyn Destination,
    src_key: &str,
    run_id: &str,
    export_name: &str,
    stats: &mut M8Stats,
) {
    let quarantine_key = format!("{}/{}/{}", QUARANTINE_PREFIX, run_id, src_key);
    match dest.r#move(src_key, &quarantine_key) {
        Ok(()) => {
            stats.quarantined_moved += 1;
            log::info!(
                "M9 quarantine: export '{}' moved '{}' → '{}'",
                export_name,
                src_key,
                quarantine_key,
            );
        }
        Err(e) => {
            stats.quarantine_move_failures += 1;
            log::warn!(
                "M9 quarantine: export '{}' could not move '{}' → '{}' (not fatal — \
                 next resume will re-trip M9 on this object): {:#}",
                export_name,
                src_key,
                quarantine_key,
                e,
            );
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::plan::test_support::test_plan;
    use crate::state::FilePart;

    use super::*;
    use crate::state::ChunkTaskInfo;

    #[test]
    fn only_the_attempt_that_completed_a_chunk_survives() {
        let task = |idx: i64, status: &str, file: Option<&str>| ChunkTaskInfo {
            chunk_index: idx,
            start_key: String::new(),
            end_key: String::new(),
            status: status.into(),
            attempts: 1,
            last_error: None,
            rows_written: None,
            file_name: file.map(Into::into),
        };
        let won = "t_20260101_000000_chunk0_00000000000000aa.parquet";
        let lost = "t_20260101_000000_chunk0_00000000000000bb.parquet";
        let debris = "t_20260101_000000_chunk1_00000000000000cc_p0.parquet";
        let s = surviving_attempts(&[task(0, "completed", Some(won)), task(1, "failed", None)]);
        assert!(is_surviving_attempt(won, &s));
        assert!(is_surviving_attempt(
            "t_20260101_000000_chunk0_00000000000000aa_p1.parquet",
            &s
        ));
        assert!(!is_surviving_attempt(lost, &s), "a superseded attempt");
        assert!(!is_surviving_attempt(debris, &s), "a failed chunk's debris");
        assert!(
            is_surviving_attempt("t_single.parquet", &s),
            "not a chunk part"
        );
    }

    fn m8_manifest(run_id: &str, parts: Vec<crate::manifest::ManifestPart>) -> RunManifest {
        use crate::manifest::{ManifestDestination, ManifestSource, ManifestStatus};
        let row_count = parts.iter().map(|p| p.rows).sum();
        let part_count = parts.len() as u32;
        RunManifest {
            split_window: None,
            checksum_render: None,
            row_hash: None,
            mode: "batch".to_string(),
            manifest_version: crate::manifest::MANIFEST_VERSION,
            run_id: run_id.into(),
            export_name: "orders".into(),
            export_family: String::new(),
            started_at: "2026-07-14T00:00:00Z".into(),
            finished_at: "2026-07-14T00:01:00Z".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: None,
                table: Some("orders".into()),
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///tmp/out/".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:00000000deadbeef".into(),
            row_count,
            part_count,
            parts,
            column_checksums: None,
            checksum_key_column: None,
        }
    }

    fn m8_part(path: &str, rows: i64, size: u64) -> crate::manifest::ManifestPart {
        crate::manifest::ManifestPart {
            part_id: 0,
            path: path.into(),
            rows,
            size_bytes: size,
            content_fingerprint: "xxh3:1".into(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        }
    }

    pub(crate) fn m8_plan(dest: &std::path::Path) -> crate::plan::ResolvedRunPlan {
        use crate::config::{DestinationConfig, DestinationType};
        crate::plan::ResolvedRunPlan {
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some(dest.to_string_lossy().into_owned()),
                ..Default::default()
            },
            resume: true,
            ..test_plan()
        }
    }

    /// A part the destination holds at another size than the run committed is not declared and its chunk is read again.
    #[test]
    fn a_part_of_another_size_and_a_completion_without_a_part_are_read_again() {
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        let run_id = "r_sizes";
        state
            .insert_chunk_tasks(run_id, &[(0, 9), (10, 19), (20, 29), (30, 39)])
            .unwrap();
        let (whole, cut) = (
            "orders_chunk0_00000000000000aa.parquet",
            "orders_chunk1_00000000000000bb.parquet",
        );
        seed_logged_chunk(&state, run_id, 0, &[(whole, 10)]);
        seed_logged_chunk(&state, run_id, 1, &[(cut, 10)]);
        state.complete_chunk_task(run_id, 2, 0, None).unwrap();
        state.exec_for_test(
            "UPDATE chunk_task SET status = 'completed' WHERE run_id = 'r_sizes' AND chunk_index = 3",
        );
        let present: Listed = [(whole.to_string(), 5), (cut.to_string(), 4)].into();
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));

        let (n, mut missing) =
            rehydrate_manifest_parts_probed(&state, run_id, &mut summary, Some(&present)).unwrap();
        missing.sort();
        assert_eq!(
            n, 1,
            "only the part of the size the run committed is declared"
        );
        assert_eq!(
            summary
                .manifest_parts
                .iter()
                .map(|p| p.path.as_str())
                .collect::<Vec<_>>(),
            [whole]
        );
        assert_eq!(
            missing,
            [
                (Some(1), cut.to_string()),
                (
                    Some(3),
                    "(chunk 3: marked completed, no part recorded)".to_string()
                ),
            ],
            "the cut part and the completion nothing wrote; the empty chunk 2 is not among them"
        );
    }

    #[test]
    fn a_probed_rehydration_declares_only_the_parts_the_destination_still_holds() {
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        let run_id = "r_probe";
        state.insert_chunk_tasks(run_id, &[(1, 100)]).unwrap();
        state
            .complete_chunk_task(run_id, 0, 50, Some("orders_chunk0_p0.parquet"))
            .unwrap();
        for (file_name, rows) in [
            ("orders_chunk0_p0.parquet", 30),
            ("orders_chunk0_p1.parquet", 20),
        ] {
            state
                .record_file(FilePart {
                    run_id,
                    export_name: "orders",
                    file_name,
                    rows,
                    bytes: 1024,
                    format: "parquet",
                    compression: None,
                    cursor_high: None,
                })
                .unwrap();
        }
        let present: Listed = [("orders_chunk0_p0.parquet".to_string(), 1024)].into();
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));

        let (n, missing) =
            rehydrate_manifest_parts_probed(&state, run_id, &mut summary, Some(&present)).unwrap();

        assert_eq!(n, 1);
        let declared: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        assert_eq!(declared, ["orders_chunk0_p0.parquet"]);
        let lost: Vec<&str> = missing.iter().map(|(_, f)| f.as_str()).collect();
        assert_eq!(lost, ["orders_chunk0_p1.parquet"]);
    }

    #[test]
    fn rehydration_recovers_all_rotation_siblings_from_file_log() {
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        let run_id = "r_rot";
        state.insert_chunk_tasks(run_id, &[(1, 100)]).unwrap();
        state
            .complete_chunk_task(run_id, 0, 50, Some("orders_chunk0_p0.parquet"))
            .unwrap();
        state
            .record_file(FilePart {
                run_id,
                export_name: "orders",
                file_name: "orders_chunk0_p0.parquet",
                rows: 30,
                bytes: 4096,
                format: "parquet",
                compression: None,
                cursor_high: None,
            })
            .unwrap();
        state
            .record_file(FilePart {
                run_id,
                export_name: "orders",
                file_name: "orders_chunk0_p1.parquet",
                rows: 20,
                bytes: 2048,
                format: "parquet",
                compression: None,
                cursor_high: None,
            })
            .unwrap();
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        let rows_before = summary.total_rows;
        let committed_before = summary.files_committed;
        let produced_before = summary.files_produced;
        let bytes_before = summary.bytes_written;

        let n = rehydrate_manifest_parts_from_file_log(&state, run_id, &mut summary).unwrap();

        assert_eq!(
            n, 2,
            "BOTH rotation siblings must be reconstructed, not just the first"
        );
        let paths: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        assert!(
            paths.contains(&"orders_chunk0_p0.parquet")
                && paths.contains(&"orders_chunk0_p1.parquet"),
            "both sibling files must be declared: {paths:?}"
        );
        assert_eq!(
            summary.manifest_parts.iter().map(|p| p.rows).sum::<i64>(),
            50,
            "all 50 rows across both siblings declared"
        );
        assert!(
            summary.manifest_parts.iter().all(|p| p.size_bytes > 0),
            "parts carry their REAL byte size (not 0) so validate's size check can't lie"
        );
        let mut ids: Vec<u32> = summary.manifest_parts.iter().map(|p| p.part_id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 2, "each reconstructed part gets its own part_id");
        assert_eq!(
            summary.total_rows - rows_before,
            50,
            "row aggregate bumped by the union"
        );
        assert_eq!(
            summary.files_committed - committed_before,
            2,
            "both reconstructed siblings count as committed files"
        );
        assert_eq!(
            summary.bytes_written - bytes_before,
            6144,
            "byte aggregate is the SUM of both siblings' real sizes — a manifest \
             whose declared parts and whose byte total disagree misreports every \
             throughput number downstream"
        );
        assert_eq!(
            summary.files_produced - produced_before,
            2,
            "…and as produced files — the two counters move together, or the \
             manifest disagrees with its own aggregates"
        );
        let mut integrity = crate::pipeline::commit::Integrity::default();
        integrity
            .column_checksums
            .insert("id".to_string(), 0xdead_beef_u64);
        crate::pipeline::commit::harvest_column_checksums(&mut summary, integrity);
        assert!(
            summary.column_checksums.is_empty(),
            "rehydrated pre-crash parts have no commit unit, so the harvest must SUPPRESS \
             Form B rather than publish a record covering only the re-exported parts"
        );
        assert!(
            summary.column_checksums_incomplete,
            "and the suppression must be RECORDED, so the Form-B telltale in \
             check_post_run_invariants can tell it from a runner that harvested nothing"
        );
        assert!(
            !summary.column_checksums_short_cover,
            "a hydration is not a runner defect — short_cover must stay clear or every \
             crash-recovery resume fails the ADR-0029 invariant"
        );
    }

    #[test]
    fn parallel_resume_accumulates_rows_onto_rehydrated_base_not_clobbers() {
        use crate::pipeline::commit::{PartKind, PartRecord, UnitId, record_part};

        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        let dest_dir = tempfile::tempdir().unwrap();
        let plan = m8_plan(dest_dir.path());
        let run_id = "r_par_resume";
        state
            .record_file(FilePart {
                run_id,
                export_name: "orders",
                file_name: "orders_chunk0.parquet",
                rows: 30,
                bytes: 4096,
                format: "parquet",
                compression: None,
                cursor_high: None,
            })
            .unwrap();
        state
            .record_file(FilePart {
                run_id,
                export_name: "orders",
                file_name: "orders_chunk1.parquet",
                rows: 20,
                bytes: 2048,
                format: "parquet",
                compression: None,
                cursor_high: None,
            })
            .unwrap();
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));

        rehydrate_manifest_parts_from_file_log(&state, run_id, &mut summary).unwrap();
        assert_eq!(
            summary.total_rows, 50,
            "rehydrated base is the 50 pre-crash rows"
        );

        record_part(
            &plan,
            &mut summary,
            None,
            &PartRecord {
                file_name: "orders_chunk2.parquet".into(),
                rows: 30,
                bytes: 4096,
                fingerprint: "xxh3:abc".into(),
                md5: String::new(),
            },
            PartKind::Chunk { chunk_index: 2 },
            UnitId::Chunk(2),
        );

        let parts_rows: i64 = summary.manifest_parts.iter().map(|p| p.rows).sum();
        assert_eq!(
            parts_rows, 80,
            "manifest lists all 80 rows (50 rehydrated + 30 new)"
        );
        assert_eq!(
            summary.total_rows, parts_rows,
            "resume must accumulate onto the rehydrated base — total_rows must equal \
             sum(manifest_parts.rows), never under-report only this invocation's rows"
        );
    }

    /// A chunk re-run after a crash must contribute ONE part, not its abandoned attempt as well.
    #[test]
    fn an_interrupted_chunks_rotated_parts_are_not_declared_by_the_resume() {
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        let run_id = "r_rot";
        state
            .insert_chunk_tasks(run_id, &[(1, 100), (101, 200)])
            .unwrap();

        let done_base = super::super::chunk_part_filename("orders", 0, "parquet");
        let lost_base = super::super::chunk_part_filename("orders", 1, "parquet");
        let done: Vec<String> = (0..2)
            .map(|i| crate::pipeline::commit::part_indexed_name(&done_base, i, 2))
            .collect();
        let lost: Vec<String> = (0..2)
            .map(|i| crate::pipeline::commit::part_indexed_name(&lost_base, i, 2))
            .collect();
        assert!(
            done[0].ends_with("_p0.parquet") && lost[1].ends_with("_p1.parquet"),
            "fixture is inert — these must really be ROTATED names: {done:?} {lost:?}"
        );
        for f in done.iter().chain(lost.iter()) {
            state
                .record_file(FilePart {
                    run_id,
                    export_name: "orders",
                    file_name: f,
                    rows: 50,
                    bytes: 4096,
                    format: "parquet",
                    compression: None,
                    cursor_high: None,
                })
                .unwrap();
        }
        state
            .complete_chunk_task(run_id, 0, 100, Some(&done[0]))
            .unwrap();

        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        let n = rehydrate_manifest_parts_from_file_log(&state, run_id, &mut summary).unwrap();

        let mut paths: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        paths.sort_unstable();
        let mut want: Vec<&str> = done.iter().map(String::as_str).collect();
        want.sort_unstable();
        assert_eq!(
            paths, want,
            "only the COMPLETED chunk's rotation siblings may be declared; the interrupted \
             chunk's are about to be re-written and would be counted twice"
        );
        assert_eq!(n, 2);
        assert_eq!(
            summary.total_rows, 100,
            "declaring the abandoned siblings doubles the run's rows in a manifest that still \
             validates, because it is self-consistent with the files it lists"
        );
    }

    #[test]
    fn a_chunk_re_run_after_a_crash_contributes_one_part_not_its_abandoned_attempt() {
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        let run_id = "r_twice";
        state.insert_chunk_tasks(run_id, &[(1, 100)]).unwrap();

        let abandoned = super::super::chunk_part_filename("orders", 0, "parquet");
        let survivor = super::super::chunk_part_filename("orders", 0, "parquet");
        assert_ne!(
            abandoned, survivor,
            "fixture is inert — two attempts must produce DIFFERENT names, which is what the \
             nonce is for"
        );
        for f in [&abandoned, &survivor] {
            state
                .record_file(FilePart {
                    run_id,
                    export_name: "orders",
                    file_name: f,
                    rows: 50,
                    bytes: 4096,
                    format: "parquet",
                    compression: None,
                    cursor_high: None,
                })
                .unwrap();
        }
        state
            .complete_chunk_task(run_id, 0, 50, Some(&survivor))
            .unwrap();

        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        let n = rehydrate_manifest_parts_from_file_log(&state, run_id, &mut summary).unwrap();

        assert_eq!(
            n, 1,
            "chunk 0 completed ONCE and contributes ONE part; its abandoned attempt is not a \
             second part of the same chunk"
        );
        let paths: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        assert_eq!(
            paths,
            vec![survivor.as_str()],
            "the part kept must be the one `chunk_task` recorded as completing the chunk"
        );
        assert_eq!(
            summary.total_rows, 50,
            "counting both attempts doubles the chunk's rows in a manifest that still \
             validates, because it is self-consistent with the files it lists"
        );
    }

    #[test]
    fn rehydration_skips_the_part_of_a_chunk_that_did_not_complete() {
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        let run_id = "r_partial";
        let done_part = super::super::chunk_part_filename("orders", 0, "parquet");
        let abandoned_part = super::super::chunk_part_filename("orders", 1, "parquet");
        state
            .insert_chunk_tasks(run_id, &[(1, 100), (101, 200)])
            .unwrap();
        state
            .complete_chunk_task(run_id, 0, 50, Some(&done_part))
            .unwrap();
        state
            .record_file(FilePart {
                run_id,
                export_name: "orders",
                file_name: &done_part,
                rows: 50,
                bytes: 4096,
                format: "parquet",
                compression: None,
                cursor_high: None,
            })
            .unwrap();
        state
            .record_file(FilePart {
                run_id,
                export_name: "orders",
                file_name: &abandoned_part,
                rows: 50,
                bytes: 4096,
                format: "parquet",
                compression: None,
                cursor_high: None,
            })
            .unwrap();

        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        let n = rehydrate_manifest_parts_from_file_log(&state, run_id, &mut summary).unwrap();

        assert_eq!(n, 1, "only the COMPLETED chunk's part may be reconstructed");
        let paths: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        assert_eq!(paths, vec![done_part.as_str()]);
        assert!(
            !paths.iter().any(|p| p.contains("abandoned")),
            "the interrupted chunk's part must not be declared — the resume rewrites \
             that chunk, and declaring both duplicates its rows in the manifest"
        );
    }

    /// Full-matrix flow: part A intact → Skip (+ summary hydration), part B missing → Rewrite (task reset), part C size-diverged → Quarantine (task reset + object moved), part D with no task → orphan.
    #[test]
    fn apply_m8_full_matrix_skip_rewrite_quarantine_orphan() {
        let run_id = "m8run";
        let dir = tempfile::tempdir().unwrap();

        std::fs::write(dir.path().join("part-a.parquet"), b"AAAAA").unwrap();
        std::fs::write(dir.path().join("part-c.parquet"), b"CCC").unwrap();
        let mut parts = vec![
            m8_part("part-a.parquet", 10, 5),
            m8_part("part-b.parquet", 10, 7),
            m8_part("part-c.parquet", 10, 9),
            m8_part("part-d.parquet", 10, 1),
        ];
        for (i, p) in parts.iter_mut().enumerate() {
            p.part_id = (i + 1) as u32;
        }
        let manifest = m8_manifest(run_id, parts);
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state
            .insert_chunk_tasks(run_id, &[(0, 10), (10, 20), (20, 30)])
            .unwrap();
        for (idx, name) in [
            (0, "part-a.parquet"),
            (1, "part-b.parquet"),
            (2, "part-c.parquet"),
        ] {
            state.claim_next_chunk_task(run_id).unwrap();
            state
                .complete_chunk_task(run_id, idx, 10, Some(name))
                .unwrap();
        }

        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        assert!(summary.schema_fingerprint.is_none(), "fixture precondition");

        let stats = apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

        assert_eq!(stats.skipped, 1, "part A (intact) is skipped");
        assert_eq!(
            stats.reset_for_rewrite, 1,
            "part B (missing) resets its task"
        );
        assert_eq!(
            stats.reset_for_quarantine, 1,
            "part C (size-diverged) resets its task"
        );
        assert_eq!(
            stats.quarantined_moved, 1,
            "part C's divergent object is moved to _quarantine/<run_id>/"
        );
        assert_eq!(stats.quarantine_move_failures, 0);
        assert_eq!(stats.orphan_parts, 1, "part D has no chunk_task");
        assert!(
            dir.path()
                .join(format!("{QUARANTINE_PREFIX}/{run_id}/part-c.parquet"))
                .exists(),
            "quarantined object relocated"
        );

        assert_eq!(
            summary
                .manifest_parts
                .iter()
                .map(|p| p.path.as_str())
                .collect::<Vec<_>>(),
            vec!["part-a.parquet"],
            "skipped parts hydrate into the resume summary (M4 cumulative manifest)"
        );
        assert_eq!(
            summary.schema_fingerprint.as_deref(),
            Some("xxh3:00000000deadbeef"),
            "schema fingerprint hydrates from the prior manifest"
        );

        let tasks = state.list_chunk_tasks_for_run(run_id).unwrap();
        let status_of = |i: i64| {
            tasks
                .iter()
                .find(|t| t.chunk_index == i)
                .unwrap()
                .status
                .clone()
        };
        assert_eq!(status_of(0), "completed");
        assert_eq!(status_of(1), "pending", "rewrite resets the task");
        assert_eq!(status_of(2), "pending", "quarantine resets the task");
    }

    #[test]
    fn apply_m8_skip_hydrates_rotation_siblings_without_a_chunk_task() {
        let run_id = "m8rot";
        let dir = tempfile::tempdir().unwrap();

        std::fs::write(dir.path().join("chunk0-p0.parquet"), b"AAAAA").unwrap();
        std::fs::write(dir.path().join("chunk0-p1.parquet"), b"BBBBBBB").unwrap();
        let mut parts = vec![
            m8_part("chunk0-p0.parquet", 30, 5),
            m8_part("chunk0-p1.parquet", 20, 7),
        ];
        for (i, p) in parts.iter_mut().enumerate() {
            p.part_id = (i + 1) as u32;
        }
        let manifest = m8_manifest(run_id, parts);
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state.insert_chunk_tasks(run_id, &[(0, 50)]).unwrap();
        state.claim_next_chunk_task(run_id).unwrap();
        state
            .complete_chunk_task(run_id, 0, 50, Some("chunk0-p0.parquet"))
            .unwrap();

        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));

        let stats = apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

        assert_eq!(stats.skipped, 2, "BOTH siblings are intact → both skipped");
        assert_eq!(
            stats.orphan_parts, 0,
            "a rotation sibling is NOT an orphan just because chunk_task holds one name"
        );
        let mut paths: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        paths.sort_unstable();
        assert_eq!(
            paths,
            vec!["chunk0-p0.parquet", "chunk0-p1.parquet"],
            "both siblings must hydrate into the resume manifest — else load drops the second's rows"
        );
        assert_eq!(
            summary.manifest_parts.iter().map(|p| p.rows).sum::<i64>(),
            50,
            "all 50 rows across both siblings are carried into the finalize manifest"
        );
        assert_eq!(
            (
                summary.total_rows,
                summary.files_committed,
                summary.files_adopted
            ),
            (50, 2, 2),
            "a Skip adopts the prior parts into every counter, like file_log rehydration"
        );
    }

    /// A STALE destination manifest must not hide a later attempt's durable parts.
    #[test]
    fn a_stale_manifest_must_not_hide_a_later_attempts_committed_part() {
        let run_id = "m8stale";
        let dir = tempfile::tempdir().unwrap();

        let p0 = super::super::chunk_part_filename("orders", 0, "parquet");
        let p1 = super::super::chunk_part_filename("orders", 1, "parquet");
        std::fs::write(dir.path().join(&p0), b"AAAAA").unwrap();
        std::fs::write(dir.path().join(&p1), b"BBBBBBB").unwrap();

        let mut parts = vec![m8_part(&p0, 10, 5)];
        parts[0].part_id = 1;
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&m8_manifest(run_id, parts)).unwrap(),
        )
        .unwrap();

        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state
            .insert_chunk_tasks(run_id, &[(0, 10), (11, 20)])
            .unwrap();
        for (idx, name) in [(0i64, &p0), (1, &p1)] {
            state.claim_next_chunk_task(run_id).unwrap();
            state
                .complete_chunk_task(run_id, idx, 10, Some(name.as_str()))
                .unwrap();
            state
                .record_file(FilePart {
                    run_id,
                    export_name: "orders",
                    file_name: name.as_str(),
                    rows: 10,
                    bytes: if idx == 0 { 5 } else { 7 },
                    format: "parquet",
                    compression: Some("zstd"),
                    cursor_high: None,
                })
                .unwrap();
        }

        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

        let declared: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        assert!(
            declared.contains(&p1.as_str()),
            "chunk 1 completed and its part is durable per file_log, but the stale \
             manifest omits it — finalize would ship a manifest without it, so the rows \
             exist on the destination and no manifest reader can see them. declared: {declared:?}"
        );
        assert_eq!(
            declared.iter().filter(|d| **d == p1.as_str()).count(),
            1,
            "and exactly once — rehydrating must not double-declare: {declared:?}"
        );
    }

    #[test]
    fn m8_does_not_quarantine_a_part_recorded_in_the_state_file_log() {
        let run_id = "m8run";
        let dir = tempfile::tempdir().unwrap();

        std::fs::write(dir.path().join("part-a.parquet"), b"AAAAA").unwrap();
        std::fs::write(dir.path().join("part-x.parquet"), b"XXXXXXX").unwrap();
        let mut parts = vec![m8_part("part-a.parquet", 10, 5)];
        parts[0].part_id = 1;
        let manifest = m8_manifest(run_id, parts);
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state.insert_chunk_tasks(run_id, &[(0, 10)]).unwrap();
        state.claim_next_chunk_task(run_id).unwrap();
        state
            .complete_chunk_task(run_id, 0, 10, Some("part-a.parquet"))
            .unwrap();
        state
            .record_file(FilePart {
                run_id,
                export_name: "orders",
                file_name: "part-a.parquet",
                rows: 10,
                bytes: 5,
                format: "parquet",
                compression: Some("zstd"),
                cursor_high: None,
            })
            .unwrap();
        state
            .record_file(FilePart {
                run_id,
                export_name: "orders",
                file_name: "part-x.parquet",
                rows: 10,
                bytes: 7,
                format: "parquet",
                compression: Some("zstd"),
                cursor_high: None,
            })
            .unwrap();

        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

        assert!(
            dir.path().join("part-x.parquet").exists(),
            "a file_log-committed part must stay at its path"
        );
        assert!(
            !dir.path()
                .join(format!("{QUARANTINE_PREFIX}/{run_id}/part-x.parquet"))
                .exists(),
            "a file_log-committed part must NOT be moved to _quarantine (that would be silent loss)"
        );
    }

    /// The fingerprint hydration guard is `summary is None AND manifest is not the placeholder`.
    #[test]
    fn apply_m8_never_overwrites_a_live_schema_fingerprint() {
        let run_id = "m8run";
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("part-a.parquet"), b"AAAAA").unwrap();
        let mut parts = vec![m8_part("part-a.parquet", 10, 5)];
        parts[0].part_id = 1;
        let manifest = m8_manifest(run_id, parts);
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state.insert_chunk_tasks(run_id, &[(0, 10)]).unwrap();
        state.claim_next_chunk_task(run_id).unwrap();
        state
            .complete_chunk_task(run_id, 0, 10, Some("part-a.parquet"))
            .unwrap();

        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        summary.schema_fingerprint = Some("xxh3:1111111111111111".into());
        apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();
        assert_eq!(
            summary.schema_fingerprint.as_deref(),
            Some("xxh3:1111111111111111"),
            "a live fingerprint must never be overwritten by the prior manifest's"
        );
    }

    /// A manifest from a DIFFERENT run_id must be ignored wholesale.
    #[test]
    fn apply_m8_ignores_foreign_manifest() {
        let run_id = "m8run";
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("part-a.parquet"), b"AAAAA").unwrap();
        let mut parts = vec![m8_part("part-a.parquet", 10, 5)];
        parts[0].part_id = 1;
        let manifest = m8_manifest("SOMEONE_ELSES_RUN", parts);
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state.insert_chunk_tasks(run_id, &[(0, 10)]).unwrap();
        state.claim_next_chunk_task(run_id).unwrap();
        state
            .complete_chunk_task(run_id, 0, 10, Some("part-a.parquet"))
            .unwrap();

        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        let stats = apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

        assert_eq!(
            stats,
            M8Stats::default(),
            "foreign manifest: no action at all"
        );
        assert!(
            summary.manifest_parts.is_empty(),
            "no hydration from a foreign manifest"
        );
        assert_eq!(
            state.list_chunk_tasks_for_run(run_id).unwrap()[0].status,
            "completed",
            "no task reset off a foreign manifest"
        );
    }

    /// A Parquet part holding the `id` values `ids`.
    fn id_part(ids: std::ops::RangeInclusive<i64>) -> Vec<u8> {
        let a: std::sync::Arc<dyn arrow::array::Array> =
            std::sync::Arc::new(arrow::array::Int64Array::from_iter_values(ids));
        let batch = arrow::record_batch::RecordBatch::try_from_iter([("id", a)]).unwrap();
        let mut out = Vec::new();
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut out, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        out
    }

    /// Under `verify: content` a committed part at its logged size whose keys are another chunk's is not declared and its chunk is read again, while its sibling is declared.
    #[test]
    fn under_verify_content_a_part_with_another_chunks_keys_is_read_again() {
        use crate::destination::local::LocalDestination;
        let dir = tempfile::tempdir().unwrap();
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        let run_id = "r_content";
        state
            .insert_chunk_tasks(run_id, &[(0, 9), (10, 19)])
            .unwrap();
        let (own, foreign) = (
            "orders_chunk0_00000000000000aa.parquet",
            "orders_chunk1_00000000000000bb.parquet",
        );
        for (chunk, name, bytes) in [(0, own, id_part(0..=9)), (1, foreign, id_part(20..=29))] {
            std::fs::write(dir.path().join(name), &bytes).unwrap();
            state.claim_next_chunk_task(run_id).unwrap();
            state
                .complete_chunk_task(run_id, chunk, 10, Some(name))
                .unwrap();
            state
                .record_file(FilePart {
                    run_id,
                    export_name: "orders",
                    file_name: name,
                    rows: 10,
                    bytes: bytes.len() as i64,
                    format: "parquet",
                    compression: None,
                    cursor_high: None,
                })
                .unwrap();
        }
        let mut plan = m8_plan(dir.path());
        plan.verify = crate::config::VerifyMode::Content;
        plan.strategy = crate::plan::ExtractionStrategy::Chunked(crate::plan::ChunkedPlan {
            column: "id".into(),
            chunk_size: 10,
            chunk_count: None,
            parallel: 1,
            by_days: None,
            checkpoint: true,
            max_attempts: 3,
        });
        let dest = LocalDestination::new(&plan.destination).unwrap();
        let by_size = crate::plan::ResolvedRunPlan {
            verify: crate::config::VerifyMode::Size,
            ..plan.clone()
        };
        let snapshot = crate::plan::ResolvedRunPlan {
            strategy: crate::plan::ExtractionStrategy::Snapshot,
            ..plan.clone()
        };
        assert!(super::super::part_keys::ContentCheck::of(&by_size, &dest).is_none());
        assert!(super::super::part_keys::ContentCheck::of(&snapshot, &dest).is_none());
        let content = super::super::part_keys::ContentCheck::of(&plan, &dest);
        assert!(content.is_some());

        let present = listing_basenames(&dest.list_prefix("").unwrap());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        let mut stats = M8Stats::default();
        let n = rehydrate_and_reset(
            &state,
            run_id,
            &dest,
            &mut summary,
            Some(&present),
            content.as_ref(),
            &mut stats,
        )
        .unwrap();
        assert_eq!(n, 1, "only the part of its own chunk is declared");
        assert_eq!(
            summary
                .manifest_parts
                .iter()
                .map(|p| p.path.as_str())
                .collect::<Vec<_>>(),
            [own]
        );
        assert_eq!(
            stats.reset_for_rewrite, 1,
            "the foreign part's chunk is read again"
        );
        let status = |i: i64| {
            state
                .list_chunk_tasks_for_run(run_id)
                .unwrap()
                .into_iter()
                .find(|t| t.chunk_index == i)
                .unwrap()
                .status
        };
        assert_eq!(status(0), "completed");
        assert_ne!(status(1), "completed");
    }

    /// Seed a completed chunk whose part is logged in `file_log` (and optionally on disk).
    fn seed_logged_chunk(
        state: &crate::state::StateStore,
        run_id: &str,
        chunk: i64,
        parts: &[(&str, i64)],
    ) {
        state.claim_next_chunk_task(run_id).unwrap();
        let rows = parts.iter().map(|(_, r)| r).sum();
        state
            .complete_chunk_task(run_id, chunk, rows, Some(parts[0].0))
            .unwrap();
        for (file_name, rows) in parts {
            state
                .record_file(FilePart {
                    run_id,
                    export_name: "orders",
                    file_name,
                    rows: *rows,
                    bytes: 5,
                    format: "parquet",
                    compression: None,
                    cursor_high: None,
                })
                .unwrap();
        }
    }

    /// Resume against a FOREIGN canonical manifest (a reused prefix after a hard crash) and against this run's own stale manifest must probe file_log parts like the no-manifest branch.
    #[test]
    fn a_resume_with_any_manifest_re_exports_a_chunk_whose_part_is_gone() {
        for manifest_run in ["SOMEONE_ELSES_RUN", "m8run"] {
            let run_id = "m8run";
            let gone = "orders_chunk0_00000000000000aa.parquet";
            let dir = tempfile::tempdir().unwrap();
            let manifest = m8_manifest(manifest_run, vec![]);
            std::fs::write(
                dir.path().join(MANIFEST_FILENAME),
                serde_json::to_vec_pretty(&manifest).unwrap(),
            )
            .unwrap();
            let state_dir = tempfile::tempdir().unwrap();
            let state =
                crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
            state.insert_chunk_tasks(run_id, &[(0, 10)]).unwrap();
            seed_logged_chunk(&state, run_id, 0, &[(gone, 10)]);

            let plan = m8_plan(dir.path());
            let mut summary = crate::pipeline::summary::RunSummary::stub_for_testing(
                run_id,
                String::from("orders"),
            );
            let stats = apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

            assert!(
                summary.manifest_parts.iter().all(|p| p.path != gone),
                "{manifest_run}: a part missing at the destination must not be declared"
            );
            assert_eq!(
                state.list_chunk_tasks_for_run(run_id).unwrap()[0].status,
                "pending",
                "{manifest_run}: its chunk is re-exported"
            );
            assert_eq!(stats.reset_for_rewrite, 1, "{manifest_run}");
        }
    }

    /// A completed chunk whose file_log write was swallowed (warn-only, ADR-0001 I7) has its part at the destination but no file_log row, or fewer rows than the chunk recorded.
    #[test]
    fn a_completed_chunk_missing_from_file_log_is_re_exported_not_dropped() {
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        let run_id = "r_unlogged";
        state
            .insert_chunk_tasks(run_id, &[(0, 10), (10, 20), (20, 30)])
            .unwrap();
        let logged = "orders_chunk0_00000000000000aa.parquet";
        let unlogged = "orders_chunk1_00000000000000bb.parquet";
        let short = "orders_chunk2_00000000000000cc_p0.parquet";
        seed_logged_chunk(&state, run_id, 0, &[(logged, 10)]);
        state.claim_next_chunk_task(run_id).unwrap();
        state
            .complete_chunk_task(run_id, 1, 10, Some(unlogged))
            .unwrap();
        seed_logged_chunk(&state, run_id, 2, &[(short, 6)]);
        state
            .complete_chunk_task(run_id, 2, 10, Some(short))
            .unwrap();
        let present: Listed = [logged, unlogged, short]
            .into_iter()
            .map(|name| (name.to_string(), 5))
            .collect();
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));

        let (n, missing) =
            rehydrate_manifest_parts_probed(&state, run_id, &mut summary, Some(&present)).unwrap();

        assert_eq!(n, 1);
        let declared: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        assert_eq!(declared, [logged]);
        assert_eq!(
            missing,
            [
                (Some(1), unlogged.to_string()),
                (Some(2), short.to_string())
            ]
        );
    }

    /// A resume over this run's own manifest that declares a completed chunk whose `file_log` row was never written, or is short of the chunk's rows, re-exports that chunk and drops the adopted part.
    #[test]
    fn a_declared_chunk_missing_from_file_log_is_re_exported_by_a_resume_over_its_manifest() {
        let run_id = "m8run";
        let (logged, unlogged, short) = (
            "orders_chunk0_00000000000000aa.parquet",
            "orders_chunk1_00000000000000bb.parquet",
            "orders_chunk2_00000000000000cc.parquet",
        );
        let (rot0, rot1) = (
            "orders_chunk3_00000000000000dd_p0.parquet",
            "orders_chunk3_00000000000000dd_p1.parquet",
        );
        let dir = tempfile::tempdir().unwrap();
        for name in [logged, unlogged, short, rot0, rot1] {
            std::fs::write(dir.path().join(name), b"12345").unwrap();
        }
        let manifest = m8_manifest(
            run_id,
            vec![
                m8_part(logged, 10, 5),
                m8_part(unlogged, 10, 5),
                m8_part(short, 10, 5),
                m8_part(rot0, 6, 5),
                m8_part(rot1, 4, 5),
            ],
        );
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state
            .insert_chunk_tasks(run_id, &[(0, 9), (10, 19), (20, 29), (30, 39)])
            .unwrap();
        seed_logged_chunk(&state, run_id, 0, &[(logged, 10)]);
        state.claim_next_chunk_task(run_id).unwrap();
        state
            .complete_chunk_task(run_id, 1, 10, Some(unlogged))
            .unwrap();
        seed_logged_chunk(&state, run_id, 2, &[(short, 6)]);
        state
            .complete_chunk_task(run_id, 2, 10, Some(short))
            .unwrap();
        seed_logged_chunk(&state, run_id, 3, &[(rot0, 6), (rot1, 4)]);
        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));

        let stats = apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

        let mut declared: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        declared.sort_unstable();
        assert_eq!(declared, [logged, rot0, rot1]);
        let status: Vec<String> = state
            .list_chunk_tasks_for_run(run_id)
            .unwrap()
            .into_iter()
            .map(|t| t.status)
            .collect();
        assert_eq!(status, ["completed", "pending", "pending", "completed"]);
        assert_eq!(stats.reset_for_rewrite, 2);
        assert_eq!(summary.total_rows, 20);
        assert_eq!(prefix_files(dir.path()), [logged, rot0, rot1]);
        assert_eq!(
            prefix_files(&dir.path().join(QUARANTINE_PREFIX).join(run_id)),
            [unlogged, short]
        );
        assert_eq!(stats.quarantined_moved, 2);
        assert_eq!(stats.quarantine_move_failures, 0);
    }

    /// The part files directly under `dir`, sorted, without the manifest.
    fn prefix_files(dir: &std::path::Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n != MANIFEST_FILENAME)
            .collect();
        names.sort_unstable();
        names
    }

    /// A chunk a `repair` re-exported has its `file_log` row under the repair's own run id, so a resume over the repaired run reads that chunk again.
    #[test]
    fn a_chunk_repaired_under_another_run_id_is_re_exported_and_its_part_moved_aside() {
        let run_id = "m8run";
        let repaired = "orders_chunk0_00000000000000bb.parquet";
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(repaired), b"12345").unwrap();
        let manifest = m8_manifest(run_id, vec![m8_part(repaired, 10, 5)]);
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state.insert_chunk_tasks(run_id, &[(0, 9)]).unwrap();
        state.claim_next_chunk_task(run_id).unwrap();
        state
            .complete_chunk_task(run_id, 0, 10, Some(repaired))
            .unwrap();
        state
            .record_file(FilePart {
                run_id: "repair-run",
                export_name: "orders",
                file_name: repaired,
                rows: 10,
                bytes: 5,
                format: "parquet",
                compression: None,
                cursor_high: None,
            })
            .unwrap();
        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));

        let stats = apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

        assert!(summary.manifest_parts.is_empty());
        assert_eq!(stats.reset_for_rewrite, 1);
        assert!(prefix_files(dir.path()).is_empty());
        assert_eq!(
            prefix_files(&dir.path().join(QUARANTINE_PREFIX).join(run_id)),
            [repaired]
        );
    }

    /// A resume with no manifest or a foreign one, whose summary already declares a part of a chunk the ledger does not hold, re-exports that chunk and moves the part to `_quarantine/<run_id>/`, as the own-manifest resume does.
    #[test]
    fn a_pruned_part_is_quarantined_by_the_no_manifest_and_foreign_manifest_resumes() {
        for manifest_run in [None, Some("SOMEONE_ELSES_RUN")] {
            let run_id = "m8run";
            let unlogged = "orders_chunk0_00000000000000aa.parquet";
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join(unlogged), b"12345").unwrap();
            if let Some(other) = manifest_run {
                std::fs::write(
                    dir.path().join(MANIFEST_FILENAME),
                    serde_json::to_vec_pretty(&m8_manifest(other, vec![])).unwrap(),
                )
                .unwrap();
            }
            let state_dir = tempfile::tempdir().unwrap();
            let state =
                crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
            state.insert_chunk_tasks(run_id, &[(0, 9)]).unwrap();
            state.claim_next_chunk_task(run_id).unwrap();
            state
                .complete_chunk_task(run_id, 0, 10, Some(unlogged))
                .unwrap();
            let plan = m8_plan(dir.path());
            let mut summary = crate::pipeline::summary::RunSummary::stub_for_testing(
                run_id,
                String::from("orders"),
            );
            summary.manifest_parts.push(m8_part(unlogged, 10, 5));
            summary.total_rows = 10;

            let stats = apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

            assert!(summary.manifest_parts.is_empty(), "{manifest_run:?}");
            assert_eq!(summary.total_rows, 0, "{manifest_run:?}");
            assert_eq!(stats.reset_for_rewrite, 1, "{manifest_run:?}");
            assert_eq!(stats.quarantined_moved, 1, "{manifest_run:?}");
            assert!(prefix_files(dir.path()).is_empty(), "{manifest_run:?}");
            assert_eq!(
                prefix_files(&dir.path().join(QUARANTINE_PREFIX).join(run_id)),
                [unlogged],
                "{manifest_run:?}"
            );
        }
    }

    /// A chunk that failed after writing parts, was re-claimed and completed in the SAME run leaves its failed attempt's parts in `manifest_parts`.
    #[test]
    fn a_completing_run_declares_only_the_attempt_that_completed_each_chunk() {
        let won = "orders_chunk0_00000000000000bb_p0.parquet";
        let won_sibling = "orders_chunk0_00000000000000bb_p1.parquet";
        let debris = "orders_chunk0_00000000000000aa_p0.parquet";
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing("r", String::from("orders"));
        for (path, rows) in [
            (debris, 7),
            (won, 7),
            (won_sibling, 3),
            ("other.parquet", 1),
        ] {
            summary.manifest_parts.push(m8_part(path, rows, 100));
            summary.total_rows += rows;
            summary.bytes_written += 100;
            summary.files_produced += 1;
            summary.files_committed += 1;
        }
        let tasks = [ChunkTaskInfo {
            chunk_index: 0,
            start_key: String::new(),
            end_key: String::new(),
            status: "completed".into(),
            attempts: 2,
            last_error: None,
            rows_written: Some(10),
            file_name: Some(won.into()),
        }];

        assert_eq!(prune_superseded_attempts(&mut summary, &tasks).len(), 1);

        let declared: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        assert_eq!(declared, [won, won_sibling, "other.parquet"]);
        assert_eq!(summary.total_rows, 11);
        assert_eq!(summary.bytes_written, 300);
        assert_eq!(summary.files_produced, 3);
        assert_eq!(summary.files_committed, 3);
    }

    #[test]
    fn m8stats_aggregates_default_to_zero() {
        let s = M8Stats::default();
        assert_eq!(s.skipped, 0);
        assert_eq!(s.reset_for_rewrite, 0);
        assert_eq!(s.reset_for_quarantine, 0);
        assert_eq!(s.orphan_parts, 0);
    }

    /// ADR-0012 M9 — `quarantine_move` is best-effort at the executor site.
    #[test]
    fn quarantine_move_is_best_effort_on_success_and_failure() {
        use crate::destination::{
            Destination, DestinationCapabilities, WriteCommitProtocol, WriteOutcome,
        };
        use std::path::Path;
        use std::sync::Mutex;

        struct MoveMock {
            fail: bool,
            calls: Mutex<Vec<(String, String)>>,
        }
        impl Destination for MoveMock {
            fn write(&self, _p: &Path, _k: &str) -> Result<WriteOutcome> {
                unreachable!("quarantine_move must never call write")
            }
            fn capabilities(&self) -> DestinationCapabilities {
                DestinationCapabilities {
                    commit_protocol: WriteCommitProtocol::Atomic,
                    idempotent_overwrite: true,
                    retry_safe: false,
                    partial_write_risk: true,
                }
            }
            fn r#move(&self, from: &str, to: &str) -> Result<()> {
                self.calls
                    .lock()
                    .unwrap()
                    .push((from.to_string(), to.to_string()));
                if self.fail {
                    anyhow::bail!("simulated move failure")
                }
                Ok(())
            }
        }

        let ok = MoveMock {
            fail: false,
            calls: Mutex::new(Vec::new()),
        };
        let mut stats = M8Stats::default();
        quarantine_move(
            &ok,
            "part-000003.parquet",
            "run_xyz",
            "public.orders",
            &mut stats,
        );
        assert_eq!(stats.quarantined_moved, 1);
        assert_eq!(stats.quarantine_move_failures, 0);
        assert_eq!(
            ok.calls.lock().unwrap().as_slice(),
            &[(
                "part-000003.parquet".to_string(),
                format!("{QUARANTINE_PREFIX}/run_xyz/part-000003.parquet"),
            )],
            "successful move targets _quarantine/<run_id>/<original-key>"
        );

        let bad = MoveMock {
            fail: true,
            calls: Mutex::new(Vec::new()),
        };
        let mut stats = M8Stats::default();
        quarantine_move(
            &bad,
            "orphan.parquet",
            "run_xyz",
            "public.orders",
            &mut stats,
        );
        assert_eq!(stats.quarantined_moved, 0);
        assert_eq!(stats.quarantine_move_failures, 1);
    }

    #[test]
    fn sec_manifest_read_rejects_oversized() {
        use crate::config::{DestinationConfig, DestinationType};
        use crate::destination::local::LocalDestination;

        const MANIFEST_MAX_BYTES: u64 = 64 * 1024 * 1024;

        let dir = tempfile::tempdir().unwrap();
        let dest = LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::File::create(dir.path().join(MANIFEST_FILENAME)).unwrap();
            f.seek(SeekFrom::Start(MANIFEST_MAX_BYTES + 4096)).unwrap();
            f.write_all(b"}").unwrap();
            f.flush().unwrap();
        }

        let meta = dest.head(MANIFEST_FILENAME).unwrap().unwrap();
        assert!(
            meta.size_bytes > MANIFEST_MAX_BYTES,
            "precondition: planted manifest must exceed the cap"
        );

        let result = read_capped(&dest, MANIFEST_FILENAME, MANIFEST_MAX_BYTES);
        assert!(
            result.is_err(),
            "oversized manifest.json must be rejected by the capped read path, \
             not materialised into memory (V21 DoS)"
        );
    }

    /// Cross-check: the in-memory `by_file` index this module builds is what makes the matrix → chunk_task lookup O(1).
    #[test]
    fn by_file_index_skips_chunks_without_file_name() {
        let tasks = [
            ChunkTaskInfo {
                chunk_index: 0,
                start_key: "0".into(),
                end_key: "100".into(),
                status: "completed".into(),
                attempts: 1,
                last_error: None,
                rows_written: Some(100),
                file_name: Some("part-000001.parquet".into()),
            },
            ChunkTaskInfo {
                chunk_index: 1,
                start_key: "100".into(),
                end_key: "200".into(),
                status: "pending".into(),
                attempts: 0,
                last_error: None,
                rows_written: None,
                file_name: None,
            },
            ChunkTaskInfo {
                chunk_index: 2,
                start_key: "200".into(),
                end_key: "300".into(),
                status: "completed".into(),
                attempts: 1,
                last_error: None,
                rows_written: Some(50),
                file_name: Some("part-000003.parquet".into()),
            },
        ];
        let by_file: HashMap<&str, &ChunkTaskInfo> = tasks
            .iter()
            .filter_map(|t| t.file_name.as_deref().map(|n| (n, t)))
            .collect();
        assert_eq!(by_file.len(), 2);
        assert_eq!(by_file["part-000001.parquet"].chunk_index, 0);
        assert_eq!(by_file["part-000003.parquet"].chunk_index, 2);
        assert!(!by_file.contains_key("part-000002.parquet"));
    }

    /// The gone-part refusal carries its code; the resume walk that reaches it is driven by the live suite.
    #[test]
    fn a_committed_part_gone_without_a_chunk_index_is_refused_by_code() {
        let err = committed_part_gone("orders_chunk7.parquet");
        crate::error::assert_coded(&err, "RIVET_DEST_COMMITTED_PART_GONE", 5);
        assert!(
            format!("{err}").contains("'orders_chunk7.parquet'"),
            "{err}"
        );
    }
}
