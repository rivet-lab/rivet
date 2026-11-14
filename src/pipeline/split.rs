//! Range sub-export synthesis for `apply --pool --split` (#167).

use std::path::Path;

use std::collections::HashMap;

use crate::config::{Config, ExportConfig, ExportMode, SourceType, SplitSynth};
use crate::error::Result;
use crate::pipeline::pool::{PoolItem, PredictedFrom};
use crate::pipeline::summary::RunSummary;
use crate::state::StateStore;

/// The N half-open key windows `(lo, hi]` that partition the whole key span, given `n - 1` interior boundary values (ascending).
fn windows(bounds: &[String]) -> Vec<(Option<String>, Option<String>)> {
    let mut out = Vec::with_capacity(bounds.len() + 1);
    let mut prev: Option<String> = None;
    for b in bounds {
        out.push((prev.clone(), Some(b.clone())));
        prev = Some(b.clone());
    }
    out.push((prev, None));
    out
}

/// Whether a source can be RANGE-SPLIT into `WHERE key > lo AND key <= hi` sub-exports.
pub(crate) fn source_type_supports_split(st: SourceType) -> bool {
    !matches!(st, SourceType::Mongo)
}

/// Whether an export may be split into range sub-exports, and on which key.
pub(crate) fn splittable_key(export: &ExportConfig) -> Option<String> {
    if export.mode == ExportMode::Cdc {
        return None;
    }
    if export.cursor_column.is_some() {
        return None;
    }
    if export.keyset_incremental {
        return None;
    }
    if let Some(key) = &export.chunk_by_key {
        return (!key.is_composite()).then(|| key.lead().to_string());
    }
    if export.mode == ExportMode::Chunked
        && let Some(col) = &export.chunk_column
    {
        return Some(col.clone());
    }
    None
}

/// Realize a split: the dominating `base` export becomes `bounds.len() + 1` range sub-exports named `<base>#0..#N-1`, each bounded to its key window and folding to the parent family.
pub(crate) fn synthesize(
    base: &ExportConfig,
    key_column: &str,
    bounds: &[String],
) -> Vec<ExportConfig> {
    let wins = windows(bounds);
    if wins.len() < 2 {
        return vec![base.clone()];
    }
    let parent = base.family();
    wins.into_iter()
        .enumerate()
        .map(|(i, (lo, hi))| {
            let mut e = base.clone();
            e.name = format!("{}#{i}", base.name);
            e.chunk_checkpoint = true;
            e.skip_empty = false;
            e.split = Some(SplitSynth {
                parent: parent.clone(),
                key_column: key_column.to_string(),
                lo,
                hi,
            });
            e
        })
        .collect()
}

/// Probe the giant's key span and realize the split into `n` range sub-exports.
pub(crate) fn probe_and_synthesize(
    config: &Config,
    giant: &ExportConfig,
    config_dir: &Path,
    n: usize,
) -> Result<Option<Vec<ExportConfig>>> {
    if !source_type_supports_split(config.source.source_type) {
        return Ok(None);
    }
    let Some(key) = splittable_key(giant) else {
        return Ok(None);
    };
    if n < 2 {
        return Ok(None);
    }
    let plan = crate::plan::build_plan(config, giant, config_dir, false, false, false, None)?;
    let mut src = crate::source::create_source(&plan.source)?;
    if giant.chunk_by_key.is_none() && giant.chunk_column.is_some() {
        let probe =
            crate::sql::null_key_probe_sql(config.source.source_type, &key, &plan.base_query);
        if src.as_mut().query_scalar(&probe)?.is_some() {
            crate::rivet_bail!(
                crate::error::codes::PLAN_CHUNK_KEY_NULL,
                "export '{}': `--pool --split` over chunk_column '{}' would SILENTLY DROP \
                 NULL-keyed rows — each range sub-export's window excludes NULL, and unlike the \
                 un-split path (which bails) the split has no way to carry them. Fix one of: use a NOT NULL column; add `WHERE {} IS NOT NULL` \
                 to drop them explicitly; or run this export with `mode: full` (unsplit).",
                giant.name,
                key,
                key
            );
        }
    }
    let bounds = super::keyset::sample_key_boundaries(src.as_mut(), &plan, &key, n, None, None)?;
    if bounds.is_empty() {
        return Ok(None);
    }
    Ok(Some(synthesize(giant, &key, &bounds)))
}

/// Read every run-unique manifest copy under the split prefix of `family`, keyed by object key.
fn list_split_prefix(
    dest_config: &crate::config::DestinationConfig,
    family: &str,
) -> anyhow::Result<Vec<(String, crate::manifest::RunManifest)>> {
    use crate::manifest::RunManifest;
    let ctx = crate::destination::placeholder::PlaceholderContext::for_today(family);
    let expanded = crate::destination::placeholder::expand_destination(dest_config.clone(), &ctx);
    let dest = crate::destination::create_destination(&expanded)?;
    let mut out = Vec::new();
    for meta in dest.list_prefix("")? {
        if !crate::manifest::census::is_copy_key(&meta.key) {
            continue;
        }
        if let Ok(bytes) = dest.read(&meta.key)
            && let Ok(m) = serde_json::from_slice::<RunManifest>(&bytes)
        {
            out.push((meta.key, m));
        }
    }
    Ok(out)
}

/// Terminal-stamp the LEDGER rows of split units a reconstruction CEASED — ordinals at/past `kept`.
pub(crate) fn stamp_ceased_units(
    dest_config: &crate::config::DestinationConfig,
    family: &str,
    giant_name: &str,
    kept: usize,
    state: &crate::state::StateStore,
) -> usize {
    let ctx = crate::destination::placeholder::PlaceholderContext::for_today(family);
    let expanded = crate::destination::placeholder::expand_destination(dest_config.clone(), &ctx);
    let prefix = crate::pipeline::finalize::destination_uri_for_manifest(&expanded);
    let now = chrono::Utc::now().to_rfc3339();
    if seeded!("ceased-split-units-left-running", true, false) {
        return 0;
    }
    match state.interrupt_ceased_split_units(giant_name, &prefix, kept, &now) {
        Ok(names) => {
            if !names.is_empty() {
                log::info!(
                    "apply --pool --split: stamped {} ceased unit ledger row(s) interrupted \
                     ({}) — the reconstructed partition has {kept} unit(s); their bucket \
                     markers are retired by the next gc pass's terminal-ledger sweep",
                    names.len(),
                    names.join(", ")
                );
            }
            names.len()
        }
        Err(e) => {
            log::warn!(
                "apply --pool --split: could not stamp ceased unit ledger rows for \
                 '{giant_name}': {e:#}"
            );
            0
        }
    }
}

/// Do the prefix's completed unit windows TILE the key space.
pub(crate) fn completed_units_tile(
    dest_config: &crate::config::DestinationConfig,
    family: &str,
) -> bool {
    let keyed = list_split_prefix(dest_config, family).unwrap_or_default();
    let census = crate::manifest::census::ManifestCensus::new(&keyed);
    let mut latest: std::collections::BTreeMap<usize, &crate::manifest::RunManifest> =
        Default::default();
    for run in census.completed_split_units(family) {
        let m = run.manifest;
        if m.split_window.is_none() {
            continue;
        }
        let Some(ord) = run.unit_index().and_then(|s| s.parse::<usize>().ok()) else {
            continue;
        };
        match latest.get(&ord) {
            Some(cur)
                if seeded!(
                    "split-tile-keeps-the-oldest-unit",
                    true,
                    cur.finished_at >= m.finished_at
                ) => {}
            _ => {
                latest.insert(ord, m);
            }
        }
    }
    if latest.is_empty() {
        return false;
    }
    let n = latest.len();
    if !latest.keys().copied().eq(0..n) {
        return false;
    }
    let windows: Vec<_> = latest
        .values()
        .map(|m| m.split_window.clone().expect("filtered Some"))
        .collect();
    if windows[0].lo.is_some() || windows[n - 1].hi.is_some() {
        return false;
    }
    windows
        .windows(2)
        .all(|w| w[0].hi.is_some() && w[0].hi == w[1].lo)
}

/// The Success unit names under the split prefix — the per-unit resume skip set.
pub(crate) fn completed_units_in_prefix(
    dest_config: &crate::config::DestinationConfig,
    family: &str,
) -> std::collections::BTreeSet<String> {
    let keyed = list_split_prefix(dest_config, family).unwrap_or_default();
    crate::manifest::census::ManifestCensus::new(&keyed)
        .completed(family)
        .into_iter()
        .map(|run| run.manifest.export_name.clone())
        .collect()
}

/// Reconstruct the split into the EXACT partition a PRIOR run used, from the windows its units persisted in the shared prefix.
pub(crate) fn split_unsafe_placeholder(
    dest: &crate::config::DestinationConfig,
) -> Option<&'static str> {
    for field in [dest.prefix.as_deref(), dest.path.as_deref()] {
        let Some(text) = field else { continue };
        for token in ["{date}", "{run_id}"] {
            if text.contains(token) {
                return Some(token);
            }
        }
    }
    None
}

pub(crate) fn reconstruct_units_from_prefix(
    dest_config: &crate::config::DestinationConfig,
    family: &str,
    giant: &ExportConfig,
) -> anyhow::Result<Option<Vec<ExportConfig>>> {
    let keyed = list_split_prefix(dest_config, family).map_err(|e| {
        crate::error::coded(
            crate::error::codes::DEST_PREFIX_UNLISTABLE,
            format!(
                "apply --pool --split --resume: cannot LIST the split prefix to reconstruct \
             the prior partition ({e:#}) — refusing to re-sample fresh boundaries over an \
             unreadable prefix (that is how a resume silently gaps). Retry when the \
             destination is reachable."
            ),
        )
    })?;
    let manifests = keyed.into_iter().map(|(_, m)| m).collect();
    Ok(reconstruct_units_from_manifests(manifests, giant))
}

fn reconstruct_units_from_manifests(
    manifests: Vec<crate::manifest::RunManifest>,
    giant: &ExportConfig,
) -> Option<Vec<ExportConfig>> {
    splittable_key(giant)?;
    let prefix = format!("{}#", giant.name);
    let mut key: Option<String> = None;
    let mut bmap: std::collections::BTreeMap<usize, String> = std::collections::BTreeMap::new();
    let mut manifested: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for m in manifests {
        let (Some(w), Some(ord)) = (
            m.split_window.as_ref(),
            m.export_name
                .strip_prefix(&prefix)
                .and_then(|s| s.parse::<usize>().ok()),
        ) else {
            continue;
        };
        manifested.insert(ord);
        key.get_or_insert_with(|| w.key_column.clone());
        if let Some(h) = &w.hi {
            bmap.insert(ord, h.clone());
        }
        if let Some(l) = &w.lo
            && seeded!("split-reconstruct-underflows-at-unit-zero", true, ord > 0)
        {
            bmap.insert(ord - 1, l.clone());
        }
    }
    let key = key?;
    let &max_pos = bmap.keys().next_back()?;
    let filled: std::collections::HashSet<usize> =
        (0..=max_pos).filter(|i| !bmap.contains_key(i)).collect();
    let mut boundaries: Vec<Option<String>> =
        (0..=max_pos).map(|i| bmap.get(&i).cloned()).collect();
    let mut last: Option<String> = None;
    for slot in boundaries.iter_mut() {
        match slot.as_ref() {
            Some(v) => last = Some(v.clone()),
            None => slot.clone_from(&last),
        }
    }
    let mut next: Option<String> = None;
    for slot in boundaries.iter_mut().rev() {
        match slot.as_ref() {
            Some(v) => next = Some(v.clone()),
            None => slot.clone_from(&next),
        }
    }
    let boundaries: Vec<String> = boundaries.into_iter().flatten().collect();
    debug_assert_eq!(boundaries.len(), max_pos + 1);

    let mut units = synthesize(giant, &key, &boundaries);
    for (i, unit) in units.iter_mut().enumerate() {
        if (i > 0 && filled.contains(&(i - 1))) || filled.contains(&i) {
            unit.chunk_checkpoint = false;
        }
    }
    if !manifested.contains(&(max_pos + 1))
        && let Some(tail) = units.last_mut()
    {
        tail.chunk_checkpoint = false;
    }
    Some(units)
}

/// What `--split` realized: the giant's one stable prefix and each unit's seeded prediction.
pub(crate) struct Realized {
    pub(crate) dest: crate::config::DestinationConfig,
    pub(crate) family: String,
    pub(crate) seeds: HashMap<String, PredictedFrom>,
}

/// Drop the in-progress checkpoints (keyset anchor + chunk run) of every unit that runs over a new window.
fn forget_stale_unit_checkpoints(
    state: &StateStore,
    source: &str,
    units: &[ExportConfig],
    fresh_windows: bool,
) -> Result<()> {
    for unit in units
        .iter()
        .filter(|u| fresh_windows || !u.chunk_checkpoint)
    {
        state.clear_resume_run_id_every_scope(&unit.name)?;
        state.reset_chunk_checkpoint(&unit.name, source)?;
    }
    Ok(())
}

/// Replace the dominating `giant` in `effective` with its range units; `None` when it is not splittable.
fn unstable_split_prefix(export: &str, token: &str) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::CONFIG_SPLIT_PREFIX_UNSTABLE,
        format!(
            "apply --pool --split: export '{export}' writes to a destination with the \
             run-varying placeholder {token} — a split resume reconstructs its \
             partition FROM the prefix, so the prefix must be one stable location \
             across runs. Use a placeholder-free prefix (or {{export}}/{{table}}, \
             which are stable) for the split export."
        ),
    )
}

/// The refusal of a user export named like `clash`, a split unit `giant` would synthesize.
fn reserved_unit_name(clash: &str, giant: &str) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::CONFIG_EXPORT_NAME_RESERVED,
        format!(
            "apply --pool --split: the synthesized split unit '{clash}' collides \
             with an existing export of the same name. Rename that export — a \
             name of the form '{giant}#<n>' is reserved for split unit names."
        ),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn realize(
    giant: &str,
    n: usize,
    broken: f64,
    resume: bool,
    predicted_pre: &[(PoolItem, PredictedFrom)],
    effective: &mut Vec<ExportConfig>,
    config: &Config,
    config_dir: &Path,
    state: &StateStore,
) -> Result<Option<Realized>> {
    let base = effective
        .iter()
        .find(|e| e.name == giant)
        .expect("advise_split names an export in the set")
        .clone();
    if let Some(token) = split_unsafe_placeholder(&base.destination) {
        return Err(unstable_split_prefix(&base.name, token));
    }
    let reconstructed = match resume {
        true => reconstruct_units_from_prefix(&base.destination, &base.family(), &base)?,
        false => None,
    };
    let units_opt = match reconstructed {
        Some(u) => {
            stamp_ceased_units(
                &base.destination,
                &base.family(),
                &base.name,
                u.len(),
                state,
            );
            forget_stale_unit_checkpoints(state, &config.source.state_key(), &u, false)?;
            Some(u)
        }
        None => {
            let fresh = probe_and_synthesize(config, &base, config_dir, n)?;
            if let Some(units) = &fresh {
                forget_stale_unit_checkpoints(state, &config.source.state_key(), units, true)?;
            }
            fresh
        }
    };
    let Some(units) = units_opt else {
        log::warn!(
            "apply --pool --split: '{giant}' dominates the floor but is not splittable \
             (needs a `chunk_by_key:`/`chunk_column:`, and not incremental/CDC) — \
             running it whole."
        );
        return Ok(None);
    };
    {
        {
            let realized = units.len();
            let (giant_secs, giant_from) = predicted_pre
                .iter()
                .find(|(i, _)| i.name == giant)
                .map(|(i, f)| (i.predicted_secs, Some(f.clone())))
                .unwrap_or((0.0, None));
            let share = giant_secs / realized.max(1) as f64;
            let unit_from = match &giant_from {
                Some(f) => crate::pipeline::pool::split_unit_from(f, share),
                None => crate::pipeline::pool::PredictedFrom::SeededSplit(share),
            };
            let seeds = units
                .iter()
                .map(|u| (u.name.clone(), unit_from.clone()))
                .collect();
            effective.retain(|e| e.name != giant);
            if let Some(clash) = first_name_collision(&units, effective) {
                return Err(reserved_unit_name(clash, giant));
            }
            effective.extend(units);
            log::warn!(
                "apply --pool --split: split '{giant}' into {realized} range \
                 sub-export(s) over its key — projected wall ~{:.1} min from the \
                 pre-split predictions (was the single-export floor). The units share \
                 one prefix and fold to family '{giant}', so the load view reads them \
                 as one table. The run's own predicted makespan — reconciled against \
                 each unit's own history, and hedged when any of it rests on an \
                 unmeasured export — prints with the pool schedule on stdout, \
                 unless the `--resume` skip leaves nothing to schedule (which says \
                 so).",
                broken / 60.0,
            );
            Ok(Some(Realized {
                family: base.family(),
                dest: base.destination,
                seeds,
            }))
        }
    }
}

/// Drop the split units whose Success manifest copy is already in the shared prefix.
pub(crate) fn skip_completed(
    realized: Option<&Realized>,
    effective: &mut Vec<ExportConfig>,
    state: &crate::state::StateStore,
) {
    let completed_units = realized
        .map(|r| completed_units_in_prefix(&r.dest, &r.family))
        .unwrap_or_default();
    effective.retain(|e| match &e.split {
        Some(_) => {
            let done = completed_units.contains(&e.name);
            if done {
                log::info!(
                    "apply --pool --split: skipping unit '{}' — already complete (its \
                     manifest copy is present)",
                    e.name
                );
            }
            !done
        }
        None => crate::pipeline::finalize::needs_run(e, true, "apply --pool", state),
    });
}

/// A split unit resumes only when it has a checkpoint to resume; any other export keeps the run-wide flag.
pub(crate) fn unit_resume(export: &ExportConfig, resume: bool, state: &StateStore) -> bool {
    match export.split {
        Some(_) => {
            resume
                && state
                    .has_resumable_checkpoint(&export.name)
                    .unwrap_or(false)
        }
        None => resume,
    }
}

/// Is this a split unit (its name under `unit_prefix`) that did NOT finish its share?
pub(crate) fn unit_failed(unit_prefix: Option<&str>, summary: &RunSummary, ok: bool) -> bool {
    unit_prefix.is_some_and(|p| summary.export_name.starts_with(p))
        && !(ok && summary.status == "success")
}

/// Write the ONE prefix `_SUCCESS` the units suppressed, once every unit of the giant succeeded (best-effort).
pub(crate) fn seal<'a>(
    realized: &Realized,
    outcomes: impl IntoIterator<Item = (&'a RunSummary, bool)>,
) {
    let unit_prefix = format!("{}#", realized.family);
    if outcomes
        .into_iter()
        .any(|(s, ok)| unit_failed(Some(&unit_prefix), s, ok))
    {
        return;
    }
    let ctx = crate::destination::placeholder::PlaceholderContext::for_today(&realized.family);
    let expanded = crate::destination::placeholder::expand_destination(realized.dest.clone(), &ctx);
    if let Err(e) = crate::pipeline::finalize::write_split_success_marker(&expanded) {
        log::warn!(
            "apply --pool --split: could not write the prefix _SUCCESS for '{}': {e:#}",
            realized.family
        );
    }
}

/// The first synthesized unit name (`{giant}#i`) that collides with an existing export's name.
pub(crate) fn first_name_collision<'a>(
    units: &'a [ExportConfig],
    existing: &[ExportConfig],
) -> Option<&'a str> {
    units
        .iter()
        .find(|u| existing.iter().any(|e| e.name == u.name))
        .map(|u| u.name.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::sample_export;

    /// One unit's RunManifest artifact — enough for the census/stamp readers.
    fn unit_manifest(
        run: &str,
        export_name: &str,
        status: crate::manifest::ManifestStatus,
    ) -> crate::manifest::RunManifest {
        use crate::manifest::*;
        RunManifest {
            manifest_version: MANIFEST_VERSION,
            run_id: run.into(),
            export_name: export_name.into(),
            export_family: "orders".into(),
            mode: "batch".into(),
            started_at: "2026-08-21T00:00:00Z".into(),
            finished_at: String::new(),
            status,
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

    #[test]
    fn a_split_unit_resumes_only_with_a_checkpoint_and_a_plain_export_keeps_the_flag() {
        let state = StateStore::open_in_memory().unwrap();
        let plain = sample_export("users");
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        let unit = synthesize(&giant, "id", &["10".into()]).remove(0);
        assert!(unit_resume(&plain, true, &state));
        assert!(!unit_resume(&plain, false, &state));
        assert!(
            !unit_resume(&unit, true, &state),
            "a never-started unit runs fresh"
        );
        state.create_chunk_run("r0", &unit.name, "h", 3).unwrap();
        assert!(unit_resume(&unit, true, &state), "a crashed unit resumes");
        assert!(!unit_resume(&unit, false, &state));
    }

    /// A unit forced fresh (or re-sampled) must not leave its crashed chunk run in_progress.
    #[test]
    fn a_unit_run_over_a_new_window_forgets_its_crashed_chunk_run() {
        let state = StateStore::open_in_memory().unwrap();
        let mut giant = sample_export("orders");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        let mut units = synthesize(&giant, "id", &["10".into()]);
        units[1].chunk_checkpoint = false;
        state
            .create_chunk_run("r0", &units[0].name, "h", 3)
            .unwrap();
        state
            .create_chunk_run("r1", &units[1].name, "h", 3)
            .unwrap();

        forget_stale_unit_checkpoints(&state, "pg/a", &units, false).unwrap();
        assert!(
            state
                .find_in_progress_chunk_run(&units[0].name)
                .unwrap()
                .is_some()
        );
        assert!(
            state
                .find_in_progress_chunk_run(&units[1].name)
                .unwrap()
                .is_none()
        );

        forget_stale_unit_checkpoints(&state, "pg/a", &units, true).unwrap();
        assert!(
            state
                .find_in_progress_chunk_run(&units[0].name)
                .unwrap()
                .is_none()
        );
    }

    /// A unit's crashed chunk run goes with its own source's and with one that recorded no source.
    #[test]
    fn a_unit_over_a_new_window_forgets_only_the_chunk_runs_of_its_own_source() {
        let state = StateStore::open_in_memory().unwrap();
        let mut giant = sample_export("orders");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        let units = synthesize(&giant, "id", &["10".into(), "20".into()]);
        let open = |run: &str, unit: &ExportConfig, source: &str| {
            let key = crate::state::ProgressKey::chunked(&unit.name, source);
            state.open_chunk_run(&key, run, "h", 3, &[(1, 10)]).unwrap();
        };
        open("r_own", &units[0], "pg/a");
        open("r_other", &units[1], "pg/b");
        state
            .create_chunk_run("r_legacy", &units[2].name, "h", 3)
            .unwrap();

        forget_stale_unit_checkpoints(&state, "pg/a", &units, true).unwrap();

        let in_progress = |unit: &ExportConfig| {
            state
                .find_in_progress_chunk_run(&unit.name)
                .unwrap()
                .is_some()
        };
        assert!(!in_progress(&units[0]), "its own source's run goes");
        assert!(in_progress(&units[1]), "another source's run stays");
        assert!(
            !in_progress(&units[2]),
            "a run that recorded no source goes"
        );
    }

    #[test]
    fn seal_writes_the_prefix_marker_only_when_every_unit_succeeded() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("manifest.json"), b"{}").unwrap();
        let realized = Realized {
            dest: crate::config::DestinationConfig {
                destination_type: crate::config::DestinationType::Local,
                path: Some(dir.path().to_string_lossy().into_owned()),
                ..Default::default()
            },
            family: "orders".into(),
            seeds: HashMap::new(),
        };
        let with = |name: &str, status: &str| {
            let mut s = RunSummary::stub_for_testing("r", name);
            s.status = status.into();
            s
        };
        let (ok, bad, other) = (
            with("orders#0", "success"),
            with("orders#1", "failed"),
            with("users", "failed"),
        );
        let marker = dir.path().join("_SUCCESS");
        seal(&realized, [(&ok, true), (&bad, false), (&other, false)]);
        assert!(!marker.exists(), "a failed unit leaves the giant unsealed");
        seal(&realized, [(&ok, true), (&other, false)]);
        assert!(
            marker.exists(),
            "a failed non-unit export does not block the seal"
        );
    }

    #[test]
    fn skip_completed_drops_only_units_whose_success_copy_is_in_the_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let dest = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        let m = unit_manifest("r0", "orders#0", crate::manifest::ManifestStatus::Success);
        std::fs::write(
            dir.path().join("manifest-r0.json"),
            serde_json::to_vec(&m).unwrap(),
        )
        .unwrap();
        let mut giant = sample_export("orders");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        let mut effective = synthesize(&giant, "id", &["10".into()]);
        let realized = Realized {
            dest,
            family: "orders".into(),
            seeds: HashMap::new(),
        };
        skip_completed(
            Some(&realized),
            &mut effective,
            &crate::state::StateStore::open_in_memory().unwrap(),
        );
        let left: Vec<_> = effective.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(left, ["orders#1"]);
    }

    /// A `{date}`/`{run_id}` prefix breaks split's stable-identity model (the resume + the stamp expand `for_today` at their own moments), so the split entry refuses it.
    #[test]
    fn a_run_varying_placeholder_is_split_unsafe_and_a_stable_one_is_not() {
        let mut dest = crate::config::DestinationConfig {
            prefix: Some("exports/{date}/orders".into()),
            ..Default::default()
        };
        assert_eq!(split_unsafe_placeholder(&dest), Some("{date}"));
        dest.prefix = Some("exports/{run_id}".into());
        assert_eq!(split_unsafe_placeholder(&dest), Some("{run_id}"));
        dest.prefix = Some("exports/{export}".into());
        assert_eq!(split_unsafe_placeholder(&dest), None);
        dest.prefix = None;
        dest.path = Some("/out/{date}".into());
        assert_eq!(split_unsafe_placeholder(&dest), Some("{date}"));
        dest.path = Some("/out/orders".into());
        assert_eq!(split_unsafe_placeholder(&dest), None);
    }

    /// ROUND-4 wedge closure, revised by the round-5 refuter.
    #[test]
    fn tile_rejects_one_sided_opens_and_prefers_the_newer_success() {
        let dir = tempfile::tempdir().unwrap();
        let dest = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        let write = |run: &str, name: &str, lo: Option<&str>, hi: Option<&str>, fin: &str| {
            let mut m = unit_manifest(run, name, crate::manifest::ManifestStatus::Success);
            m.finished_at = fin.into();
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
        };
        write(
            "a0",
            "orders#0",
            Some("5"),
            Some("m"),
            "2026-08-21T00:01:00Z",
        );
        write("a1", "orders#1", Some("m"), None, "2026-08-21T00:01:00Z");
        assert!(!completed_units_tile(&dest, "orders"));

        for f in std::fs::read_dir(dir.path()).unwrap().flatten() {
            std::fs::remove_file(f.path()).unwrap();
        }
        write(
            "a_old0",
            "orders#0",
            None,
            Some("x"),
            "2026-08-21T00:00:00Z",
        );
        write(
            "z_new0",
            "orders#0",
            None,
            Some("m"),
            "2026-08-21T00:05:00Z",
        );
        write("b1", "orders#1", Some("m"), None, "2026-08-21T00:05:00Z");
        assert!(
            completed_units_tile(&dest, "orders"),
            "the NEWER #0 (hi=m) tiles with #1 (lo=m); picking the older breaks it"
        );

        for f in std::fs::read_dir(dir.path()).unwrap().flatten() {
            std::fs::remove_file(f.path()).unwrap();
        }
        write(
            "a_new0",
            "orders#0",
            None,
            Some("m"),
            "2026-08-21T00:05:00Z",
        );
        write(
            "z_old0",
            "orders#0",
            None,
            Some("x"),
            "2026-08-21T00:00:00Z",
        );
        write("b1", "orders#1", Some("m"), None, "2026-08-21T00:05:00Z");
        assert!(
            completed_units_tile(&dest, "orders"),
            "the NEWER #0 tiles whichever copy the listing returns first"
        );
    }

    /// The newer copy of every ordinal wins whatever order the destination lists them in.
    #[test]
    fn the_newer_copy_wins_for_every_ordinal_under_any_listing_order() {
        let dir = tempfile::tempdir().unwrap();
        let dest = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        let n = 12;
        let write = |run: String, ord: usize, hi: Option<String>, fin: &str| {
            let mut m = unit_manifest(
                &run,
                &format!("orders#{ord}"),
                crate::manifest::ManifestStatus::Success,
            );
            m.finished_at = fin.into();
            m.split_window = Some(crate::manifest::SplitWindow {
                key_column: "id".into(),
                lo: (ord > 0).then(|| format!("k{ord:02}")),
                hi,
            });
            std::fs::write(
                dir.path().join(format!("manifest-{run}.json")),
                serde_json::to_vec(&m).unwrap(),
            )
            .unwrap();
        };
        for ord in 0..n {
            let right = (ord + 1 < n).then(|| format!("k{:02}", ord + 1));
            write(format!("n{ord}"), ord, right, "2026-08-21T00:05:00Z");
            write(
                format!("o{ord}"),
                ord,
                Some(format!("z{ord:02}")),
                "2026-08-21T00:00:00Z",
            );
        }
        assert!(completed_units_tile(&dest, "orders"));
    }

    fn write_family_unit(
        dir: &std::path::Path,
        run: &str,
        family: &str,
        name: &str,
        window: (Option<&str>, Option<&str>),
        finished: &str,
    ) {
        let mut m = unit_manifest(run, name, crate::manifest::ManifestStatus::Success);
        m.export_family = family.into();
        m.finished_at = finished.into();
        m.split_window = Some(crate::manifest::SplitWindow {
            key_column: "id".into(),
            lo: window.0.map(str::to_string),
            hi: window.1.map(str::to_string),
        });
        std::fs::write(
            dir.join(format!("manifest-{run}.json")),
            serde_json::to_vec(&m).unwrap(),
        )
        .unwrap();
    }

    fn local_dest(dir: &std::path::Path) -> crate::config::DestinationConfig {
        crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        }
    }

    /// A unit of another family under the same prefix never fills a hole in this family's tiling.
    #[test]
    fn a_foreign_family_unit_does_not_fill_a_gap_in_the_tiling() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        write_family_unit(
            dir.path(),
            "a0",
            "orders",
            "orders#0",
            (None, Some("m")),
            "2026-08-21T00:01:00Z",
        );
        write_family_unit(
            dir.path(),
            "x1",
            "other",
            "other#1",
            (Some("m"), None),
            "2026-08-21T00:01:00Z",
        );
        assert!(
            !completed_units_tile(&dest, "orders"),
            "orders#1 never ran; the foreign other#1 must not stand in for it"
        );
    }

    /// A newer unit of another family never displaces this family's unit of the same ordinal.
    #[test]
    fn a_foreign_family_unit_does_not_break_a_whole_tiling() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        write_family_unit(
            dir.path(),
            "a0",
            "orders",
            "orders#0",
            (None, Some("m")),
            "2026-08-21T00:01:00Z",
        );
        write_family_unit(
            dir.path(),
            "a1",
            "orders",
            "orders#1",
            (Some("m"), None),
            "2026-08-21T00:01:00Z",
        );
        write_family_unit(
            dir.path(),
            "x0",
            "other",
            "other#0",
            (None, Some("q")),
            "2026-08-21T00:09:00Z",
        );
        assert!(
            completed_units_tile(&dest, "orders"),
            "the foreign other#0 finished later but is not a unit of orders"
        );
    }

    /// The resume skip set holds the successes of this family only.
    #[test]
    fn the_completed_units_of_a_prefix_are_this_familys() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        write_family_unit(
            dir.path(),
            "a0",
            "orders",
            "orders#0",
            (None, Some("m")),
            "2026-08-21T00:01:00Z",
        );
        write_family_unit(
            dir.path(),
            "x0",
            "other",
            "other#0",
            (None, Some("q")),
            "2026-08-21T00:09:00Z",
        );
        let done: Vec<String> = completed_units_in_prefix(&dest, "orders")
            .into_iter()
            .collect();
        assert_eq!(done, vec!["orders#0".to_string()]);
    }

    #[test]
    fn stamp_ceased_units_closes_both_wedge_halves() {
        let dir = tempfile::tempdir().unwrap();
        let dest = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        for (run, name) in [("r1", "orders#1"), ("r5", "orders#5"), ("r6", "orders#6")] {
            let m = unit_manifest(run, name, crate::manifest::ManifestStatus::Running);
            std::fs::write(
                dir.path().join(format!("manifest-{run}.json")),
                serde_json::to_vec(&m).unwrap(),
            )
            .unwrap();
        }
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let prefix = crate::pipeline::finalize::destination_uri_for_manifest(&dest);
        for (run, name) in [("r1", "orders#1"), ("r5", "orders#5"), ("r6", "orders#6")] {
            state
                .begin_run(run, name, &prefix, "2026-08-21T00:00:00Z")
                .unwrap();
        }

        let stamped = stamp_ceased_units(&dest, "orders", "orders", 5, &state);
        assert_eq!(stamped, 2, "two ceased ledger rows (#5, #6)");
        let active = state.active_run_ids_on_prefix(&prefix).unwrap();
        assert!(active.contains("r1"), "the live ordinal must stay live");
        assert!(!active.contains("r5") && !active.contains("r6"));

        let read = |run: &str| -> crate::manifest::ManifestStatus {
            serde_json::from_slice::<crate::manifest::RunManifest>(
                &std::fs::read(dir.path().join(format!("manifest-{run}.json"))).unwrap(),
            )
            .unwrap()
            .status
        };
        for run in ["r1", "r5", "r6"] {
            assert_eq!(read(run), crate::manifest::ManifestStatus::Running);
        }

        state
            .finish_run("r1", "success", "2026-08-21T02:00:00Z")
            .unwrap();
        let done = unit_manifest("r1", "orders#1", crate::manifest::ManifestStatus::Success);
        std::fs::write(
            dir.path().join("manifest-r1.json"),
            serde_json::to_vec(&done).unwrap(),
        )
        .unwrap();
        let gcs = crate::destination::gcs::GcsStore::open_fs(
            dir.path().parent().unwrap().to_str().unwrap(),
        )
        .unwrap();
        let base = dir.path().file_name().unwrap().to_string_lossy();
        let keyed: Vec<(String, crate::manifest::RunManifest)> = ["r1", "r5", "r6"]
            .iter()
            .map(|run| {
                (
                    format!("{base}/manifest-{run}.json"),
                    serde_json::from_slice(
                        &std::fs::read(dir.path().join(format!("manifest-{run}.json"))).unwrap(),
                    )
                    .unwrap(),
                )
            })
            .collect();
        let dead: std::collections::HashSet<String> = ["r5", "r6"]
            .iter()
            .filter(|r| matches!(state.run_status_of(r).unwrap(), Some(st) if st != "running"))
            .map(|r| r.to_string())
            .collect();
        assert_eq!(dead.len(), 2, "the stamp fed the sweep both ceased ids");
        let (removed, _) = crate::load::reconcile::gc_orphans(
            &gcs,
            &format!("gs://b/{base}"),
            &keyed,
            false,
            &dead,
        )
        .unwrap();
        assert_eq!(removed, 2, "exactly the ceased ordinals' markers are swept");
        assert!(!state.has_active_run_on_prefix(&prefix).unwrap());
        let keyed: Vec<(String, crate::manifest::RunManifest)> = ["r1"]
            .iter()
            .map(|run| {
                (
                    format!("{base}/manifest-{run}.json"),
                    serde_json::from_slice(
                        &std::fs::read(dir.path().join(format!("manifest-{run}.json"))).unwrap(),
                    )
                    .unwrap(),
                )
            })
            .collect();
        let census = crate::manifest::census::ManifestCensus::new(&keyed);
        assert!(
            !census.active_running(),
            "no marker may keep reading live once the stamp + sweep both ran"
        );
    }

    #[test]
    fn windows_partition_the_key_space_gap_free_and_overlap_free() {
        let w = windows(&["10".into(), "20".into(), "30".into()]);
        assert_eq!(
            w,
            vec![
                (None, Some("10".into())),
                (Some("10".into()), Some("20".into())),
                (Some("20".into()), Some("30".into())),
                (Some("30".into()), None),
            ],
            "4 windows for 3 boundaries: first no-floor, last no-ceil, each lo == prev hi"
        );
    }

    #[test]
    fn synthesize_makes_n_units_that_fold_to_the_parent_family() {
        let base = {
            let mut e = sample_export("daily");
            e.mode = ExportMode::Chunked;
            e.chunk_by_key = Some("id".into());
            e
        };
        let units = synthesize(&base, "id", &["1000".into(), "2000".into()]);
        assert_eq!(units.len(), 3, "2 boundaries → 3 range units");
        assert_eq!(
            units.iter().map(|u| u.name.clone()).collect::<Vec<_>>(),
            vec!["daily#0", "daily#1", "daily#2"]
        );
        for u in &units {
            assert_eq!(u.family(), "daily", "unit {} must fold to family", u.name);
        }
        let ranges: Vec<_> = units
            .iter()
            .map(|u| {
                let s = u.split.as_ref().unwrap();
                (s.lo.clone(), s.hi.clone())
            })
            .collect();
        assert_eq!(ranges[0], (None, Some("1000".into())));
        assert_eq!(ranges[1], (Some("1000".into()), Some("2000".into())));
        assert_eq!(ranges[2], (Some("2000".into()), None));
    }

    #[test]
    fn a_split_unit_never_inherits_skip_empty() {
        let base = {
            let mut e = sample_export("daily");
            e.mode = ExportMode::Full;
            e.chunk_by_key = Some("id".into());
            e.skip_empty = true;
            e
        };
        for u in synthesize(&base, "id", &["1000".into()]) {
            assert!(
                !u.skip_empty,
                "unit {} must write its window's manifest even when empty",
                u.name
            );
        }
    }

    #[test]
    fn synthesize_with_no_boundaries_leaves_the_export_whole() {
        let base = sample_export("solo");
        let units = synthesize(&base, "id", &[]);
        assert_eq!(units.len(), 1, "no boundaries → no split");
        assert_eq!(units[0].name, "solo");
        assert!(
            units[0].split.is_none(),
            "an unsplit export carries no marker"
        );
    }

    #[test]
    fn incremental_and_cdc_exports_are_not_splittable() {
        let mut chunked = sample_export("t");
        chunked.mode = ExportMode::Chunked;
        chunked.chunk_by_key = Some("id".into());
        assert_eq!(splittable_key(&chunked).as_deref(), Some("id"));

        let mut incr = chunked.clone();
        incr.cursor_column = Some("updated_at".into());
        assert!(
            splittable_key(&incr).is_none(),
            "an incremental export must not split — the cursor is per-export, not per-range"
        );

        let mut cdc = sample_export("c");
        cdc.mode = ExportMode::Cdc;
        cdc.chunk_by_key = Some("id".into());
        assert!(
            splittable_key(&cdc).is_none(),
            "CDC is a stream, not a range"
        );

        let mut nokey = sample_export("n");
        nokey.mode = ExportMode::Chunked;
        assert!(splittable_key(&nokey).is_none());

        let mut ks_incr = chunked.clone();
        ks_incr.keyset_incremental = true;
        assert!(
            splittable_key(&ks_incr).is_none(),
            "an append-only keyset_incremental export must not split — the per-unit \
             high-water mark disagrees with a re-derived partition (silent drop/dup)"
        );
    }

    fn write_unit_manifest(
        dir: &std::path::Path,
        unit_name: &str,
        run_id: &str,
        lo: Option<&str>,
        hi: Option<&str>,
    ) {
        use crate::manifest::{
            MANIFEST_VERSION, ManifestDestination, ManifestSource, ManifestStatus, RunManifest,
            SplitWindow, run_unique_manifest_name,
        };
        let m = RunManifest {
            split_window: Some(SplitWindow {
                key_column: "id".into(),
                lo: lo.map(String::from),
                hi: hi.map(String::from),
            }),
            checksum_render: None,
            row_hash: None,
            mode: "batch".into(),
            manifest_version: MANIFEST_VERSION,
            run_id: run_id.into(),
            export_name: unit_name.into(),
            export_family: "daily".into(),
            started_at: "t".into(),
            finished_at: "t".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "pg".into(),
                schema: None,
                table: None,
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///x".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0".into(),
            row_count: 1,
            part_count: 1,
            parts: vec![],
            column_checksums: None,
            checksum_key_column: None,
        };
        let name = run_unique_manifest_name(run_id);
        std::fs::write(dir.join(name), serde_json::to_vec(&m).unwrap()).unwrap();
    }

    #[test]
    fn a_closed_lower_bound_on_unit_zero_is_no_boundary_and_no_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());

        write_unit_manifest(dir.path(), "daily#0", "r0", Some("5"), Some("250"));
        write_unit_manifest(dir.path(), "daily#1", "r1", Some("250"), None);

        let units = reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
            .unwrap()
            .expect("the windows of both units name one boundary");
        assert_eq!(
            wins_of(&units),
            vec![(None, Some("250".into())), (Some("250".into()), None)],
            "unit #0 has no unit below it, so its lower bound names no boundary"
        );
    }

    #[test]
    fn reconstruct_recovers_the_exact_partition_including_a_crashed_units_window() {
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());

        write_unit_manifest(dir.path(), "daily#0", "r0", None, Some("250"));
        write_unit_manifest(dir.path(), "daily#1", "r1", Some("250"), Some("500"));
        write_unit_manifest(dir.path(), "daily#3", "r3", Some("750"), None);

        let units = reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
            .unwrap()
            .expect("reconstruct must recover the partition from the persisted windows");

        assert_eq!(
            wins_of(&units),
            vec![
                (None, Some("250".into())),
                (Some("250".into()), Some("500".into())),
                (Some("500".into()), Some("750".into())),
                (Some("750".into()), None),
            ],
            "reconstruct must rebuild the original 4-way partition (incl. the crashed \
             unit's window recovered from its neighbours), not a re-sampled one"
        );
        let names: Vec<&str> = units.iter().map(|u| u.name.as_str()).collect();
        assert_eq!(names, vec!["daily#0", "daily#1", "daily#2", "daily#3"]);
    }

    #[test]
    fn realize_on_resume_swaps_the_giant_for_its_units_seeded_with_its_share() {
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());
        write_unit_manifest(dir.path(), "daily#0", "r0", None, Some("250"));
        write_unit_manifest(dir.path(), "daily#1", "r1", Some("250"), Some("500"));
        write_unit_manifest(dir.path(), "daily#3", "r3", Some("750"), None);
        let users = sample_export("users");
        let item = |name: &str, secs: f64| PoolItem {
            name: name.into(),
            predicted_secs: secs,
            parallel_safe: true,
        };
        let pre = [
            (item("users", 10.0), PredictedFrom::Measured(10.0)),
            (item("daily", 400.0), PredictedFrom::Measured(400.0)),
        ];
        let cfg = Config::from_yaml(
            "source:\n  type: postgres\n  url: postgresql://localhost/db\nexports:\n\
             \x20 - name: users\n    table: users\n    mode: full\n    format: parquet\n\
             \x20   destination: { type: local, path: ./out }\n",
        )
        .unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let mut effective = vec![users, giant];

        let r = realize(
            "daily",
            4,
            100.0,
            true,
            &pre,
            &mut effective,
            &cfg,
            dir.path(),
            &state,
        )
        .unwrap()
        .expect("a reconstructable split is realized");

        let names: Vec<&str> = effective.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["users", "daily#0", "daily#1", "daily#2", "daily#3"]);
        assert_eq!(r.family, "daily");
        assert_eq!(r.seeds.len(), 4);
        for (unit, from) in &r.seeds {
            assert!(
                matches!(from, PredictedFrom::SeededSplit(s) if *s == 100.0),
                "{unit} must inherit the giant's 400 s / 4 as a measured seed, got {from:?}"
            );
        }
    }

    fn wins_of(units: &[ExportConfig]) -> Vec<(Option<String>, Option<String>)> {
        units
            .iter()
            .map(|u| {
                let s = u
                    .split
                    .as_ref()
                    .expect("reconstructed unit carries a split window");
                (s.lo.clone(), s.hi.clone())
            })
            .collect()
    }

    #[test]
    fn reconstruct_refuses_to_resurrect_a_now_unsplittable_export_on_resume() {
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());
        write_unit_manifest(dir.path(), "daily#0", "r0", None, Some("500"));
        write_unit_manifest(dir.path(), "daily#1", "r1", Some("500"), None);
        giant.keyset_incremental = true;
        assert!(
            reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
                .unwrap()
                .is_none(),
            "a resume must not resurrect a split for a config that is no longer splittable"
        );
    }

    #[test]
    fn reconstruct_runs_the_open_tail_fresh_after_a_trailing_adjacent_crash() {
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());

        write_unit_manifest(dir.path(), "daily#0", "r0", None, Some("250"));
        write_unit_manifest(dir.path(), "daily#1", "r1", Some("250"), Some("500"));

        let units = reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
            .unwrap()
            .expect("reconstruct");
        assert_eq!(
            wins_of(&units),
            vec![
                (None, Some("250".into())),
                (Some("250".into()), Some("500".into())),
                (Some("500".into()), None),
            ],
            "a trailing double-crash rebuilds a 3-unit partition with an open widened tail"
        );
        assert_eq!(
            units.iter().map(|u| u.chunk_checkpoint).collect::<Vec<_>>(),
            vec![true, true, false],
            "the open tail must run FRESH (chunk_checkpoint=false) so the widened region is \
             re-sampled, not resumed from stale ranges that skip the done hi=None range"
        );
    }

    #[test]
    fn reconstruct_covers_a_leading_adjacent_crash_instead_of_re_sampling() {
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());

        write_unit_manifest(dir.path(), "daily#2", "r2", Some("500"), Some("750"));
        write_unit_manifest(dir.path(), "daily#3", "r3", Some("750"), None);

        let units = reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
            .unwrap()
            .expect("a leading adjacent crash must NOT collapse to None (that re-samples)");
        assert_eq!(
            wins_of(&units),
            vec![
                (None, Some("500".into())),
                (Some("500".into()), Some("500".into())),
                (Some("500".into()), Some("750".into())),
                (Some("750".into()), None),
            ],
            "leading hole must be back-filled into a full 4-unit partition, not re-sampled"
        );
    }

    #[test]
    fn reconstruct_fills_an_interior_adjacent_crash_without_overlapping_a_survivor() {
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());

        write_unit_manifest(dir.path(), "daily#0", "r0", None, Some("250"));
        write_unit_manifest(dir.path(), "daily#3", "r3", Some("750"), None);

        let units = reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
            .unwrap()
            .expect("reconstruct");
        assert_eq!(
            wins_of(&units),
            vec![
                (None, Some("250".into())),
                (Some("250".into()), Some("250".into())),
                (Some("250".into()), Some("750".into())),
                (Some("750".into()), None),
            ],
            "the re-run unit must end at 750 (where completed #3 begins) — never an \
             open-ended window that re-covers #3 and duplicates its rows"
        );
        let overlaps_survivor = units.iter().any(|u| {
            let s = u.split.as_ref().unwrap();
            s.hi.is_none() && s.lo.as_deref() != Some("750")
        });
        assert!(
            !overlaps_survivor,
            "no re-run unit may be open-ended below the surviving #3 boundary (would dup)"
        );

        let ck: Vec<bool> = units.iter().map(|u| u.chunk_checkpoint).collect();
        assert_eq!(
            ck,
            vec![true, false, false, true],
            "fill-synthesized units (#1, #2) must run fresh; exact units (#0, #3) keep checkpoint"
        );
    }

    #[test]
    fn reconstruct_keeps_checkpoint_for_an_exactly_recovered_single_crash() {
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());
        write_unit_manifest(dir.path(), "daily#0", "r0", None, Some("250"));
        write_unit_manifest(dir.path(), "daily#1", "r1", Some("250"), Some("500"));
        write_unit_manifest(dir.path(), "daily#3", "r3", Some("750"), None);

        let units = reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
            .unwrap()
            .expect("reconstruct");
        assert!(
            units.iter().all(|u| u.chunk_checkpoint),
            "an exactly-recovered single-crash resume must keep crash-recovery on every unit"
        );
    }

    #[test]
    fn mongo_source_is_not_range_split_capable() {
        assert!(
            !source_type_supports_split(SourceType::Mongo),
            "Mongo has no inline SQL range literal — range-splitting it would panic"
        );
        assert!(source_type_supports_split(SourceType::Postgres));
        assert!(source_type_supports_split(SourceType::Mysql));
        assert!(source_type_supports_split(SourceType::Mssql));
    }

    #[test]
    fn the_split_refusals_carry_their_codes() {
        let err = unstable_split_prefix("orders", "{date}");
        crate::error::assert_coded(&err, "RIVET_CONFIG_SPLIT_PREFIX_UNSTABLE", 1);
        assert!(format!("{err}").contains("placeholder {date}"), "{err}");
        let err = reserved_unit_name("orders#1", "orders");
        crate::error::assert_coded(&err, "RIVET_CONFIG_EXPORT_NAME_RESERVED", 1);
        assert!(format!("{err}").contains("'orders#<n>'"), "{err}");
    }

    /// A resume whose prefix cannot be listed (an unreadable directory) refuses by code.
    #[cfg(unix)]
    #[test]
    fn an_unlistable_split_prefix_refuses_the_resume_by_code() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("prefix");
        std::fs::create_dir_all(&prefix).unwrap();
        std::fs::set_permissions(&prefix, std::fs::Permissions::from_mode(0o000)).unwrap();
        let dest = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(prefix.to_str().unwrap().to_string()),
            ..Default::default()
        };
        let result = reconstruct_units_from_prefix(&dest, "orders", &sample_export("orders"));
        std::fs::set_permissions(&prefix, std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = result.unwrap_err();
        crate::error::assert_coded(&err, "RIVET_DEST_PREFIX_UNLISTABLE", 1);
    }
}
