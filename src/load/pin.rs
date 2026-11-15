//! Pinning a load's plan to the spec of the run it consumes (ADR-0034 D1), and the refusals that keep one load from reading two specs.

use crate::error::Result;
use crate::load;
use crate::state::StateStore;
use anyhow::Context as _;

/// `plan` retyped from the spec of the NEWEST loadable run under its own prefix.
pub(super) fn pin_plan_to_its_run(
    plan: &load::plan::LoadPlan,
    state: Option<&StateStore>,
    cfg: &crate::config::Config,
    op: &str,
) -> Result<load::plan::LoadPlan> {
    plan.refused()?;
    let unpinned = |why: &str| {
        eprintln!(
            "  {op} [{}]: typed from the by-name load spec — {why}",
            plan.table
        );
        load::plan::check_spec_fit(plan)?;
        Ok(plan.clone())
    };
    let Some(s) = state else {
        return unpinned("no state DB, so no per-run spec to pin to");
    };
    let runs = match load::open_store(&plan.destination)
        .and_then(|store| load::reconcile::fetch_manifests_keyed(&store, &plan.gcs_prefix))
        .and_then(|keyed| {
            load::reconcile::own_manifests(keyed, &plan.gcs_prefix, &plan.export_name, plan.mode)
        })
        .map(|(keyed, _)| keyed)
        .and_then(|keyed| {
            load::reconcile::select_runs(keyed, &std::collections::HashSet::new(), plan.mode)
        }) {
        Ok(runs) => runs,
        Err(e) => return unpinned(&format!("could not list its runs ({e:#})")),
    };
    let mut newest_first: Vec<(String, String)> = runs
        .into_iter()
        .map(|(_, m)| (m.finished_at.clone(), m.run_id))
        .collect();
    if newest_first.is_empty() {
        return unpinned("no run left to load under its prefix");
    }
    newest_first.sort_by(|a, b| {
        use std::cmp::Ordering;
        if crate::manifest::census::finished_after(&a.0, &b.0) {
            Ordering::Less
        } else if crate::manifest::census::finished_after(&b.0, &a.0) {
            Ordering::Greater
        } else {
            b.1.cmp(&a.1)
        }
    });
    let mut pinned: Option<(String, crate::state::LoadSpec)> = None;
    let mut skipped: Vec<&str> = Vec::new();
    for (_, run_id) in &newest_first {
        match s.load_spec_of_run_with_init_key(
            &plan.export_name,
            plan.unit.as_deref(),
            &plan.source_key,
            run_id,
        ) {
            Ok(Some(spec)) => {
                pinned = Some((run_id.clone(), spec));
                break;
            }
            Ok(None) => {
                skipped.push(run_id);
                continue;
            }
            Err(e) => {
                return unpinned(&format!("run {run_id}'s own spec is unreadable ({e:#})"));
            }
        }
    }
    let Some((run_id, spec)) = pinned else {
        return unpinned(&format!(
            "none of its {} loadable run(s) recorded a per-run spec (runs older than this \
             release, baseline legs only, a run that crashed after its manifest and before \
             its spec write, or a continuous stream — `until_current: false` — that was \
             stopped rather than finished, which records nothing)",
            newest_first.len()
        ));
    };
    if let Some(note) = skipped_runs_note(&plan.table, &run_id, &skipped) {
        eprintln!("{note}");
    }
    let pinned_names: Vec<&str> = spec.columns.iter().map(|c| c.name.as_str()).collect();
    let loaded = match s
        .loaded_source_run_ids(&load::build_loader(plan, op, &Default::default()).fqtn(&plan.table))
    {
        Ok(loaded) => loaded,
        Err(e) => {
            return unpinned(&format!(
                "the ledger could not list the runs already loaded ({e:#}), so the \
                 column-respelling check cannot run"
            ));
        }
    };
    let mut respelled: Vec<(String, String, String)> = Vec::new();
    for older in runs_loaded_with_the_pin(op, plan.mode, &newest_first, &run_id, &loaded) {
        if let Ok(Some(o)) = s.load_spec_of_run_with_init_key(
            &plan.export_name,
            plan.unit.as_deref(),
            &plan.source_key,
            older,
        ) {
            let names: Vec<&str> = o.columns.iter().map(|c| c.name.as_str()).collect();
            for (was, now) in load::plan::lookalike_spelling_changes(&pinned_names, &names) {
                respelled.push((older.to_string(), was, now));
            }
        }
    }
    respelled_stop(&plan.table, &run_id, &respelled)?;
    let Some(target) = crate::types::target::ExportTarget::parse(plan.load.target.name()) else {
        return unpinned("unknown load target");
    };
    let mut retyped = load::plan::retype_plan(cfg, plan, &spec, target).with_context(|| {
        format!(
            "{op} [{}]: the config does not fit the columns run {run_id} recorded — if the \
             config changed after that run (a new `pk:` / `partition.column`), run `rivet run \
             -e {}` once so a run records the column, then {op} again",
            plan.table, plan.export_name
        )
    })?;
    retyped.refused()?;
    retyped.pinned_run = Some(listing_pin(&newest_first, &run_id));
    Ok(retyped)
}

/// Stop a load that would read two spellings of one column, with [`respelled_refusal`]'s text.
pub(super) fn respelled_stop(
    table: &str,
    pinned: &str,
    respelled: &[(String, String, String)],
) -> Result<()> {
    match respelled_refusal(table, pinned, respelled) {
        Some(refusal) => {
            crate::rivet_bail!(crate::error::codes::LOAD_COLUMN_RESPELLED, "{refusal}")
        }
        None => Ok(()),
    }
}

/// Stop a load whose listing holds runs newer than its pin, with [`late_runs_refusal`]'s text.
pub(super) fn late_runs_stop(
    table: &str,
    runs: &[(String, crate::manifest::RunManifest)],
    pin: Option<&(String, String)>,
) -> Result<()> {
    match late_runs_refusal(table, runs, pin) {
        Some(why) => crate::rivet_bail!(crate::error::codes::LOAD_RUN_LANDED_DURING_LOAD, "{why}"),
        None => Ok(()),
    }
}

/// The pin `late_runs_refusal` checks: the pinned run id, bounded by the newest run the listing held.
pub(super) fn listing_pin(newest_first: &[(String, String)], run_id: &str) -> (String, String) {
    let newest = newest_first
        .first()
        .map(|(at, _)| at.clone())
        .unwrap_or_default();
    (run_id.to_string(), newest)
}

/// Runs in this listing that finished AFTER the newest run the pin listed.
pub(super) fn late_runs_refusal(
    table: &str,
    runs: &[(String, crate::manifest::RunManifest)],
    pin: Option<&(String, String)>,
) -> Option<String> {
    let (pinned_id, pinned_at) = pin?;
    let late: Vec<&str> = runs
        .iter()
        .filter(|(_, m)| crate::manifest::census::finished_after(&m.finished_at, pinned_at))
        .map(|(_, m)| m.run_id.as_str())
        .collect();
    (!late.is_empty()).then(|| {
        format!(
            "load [{table}]: run(s) {} finished after run {pinned_id}, which this load was \
             typed from — a run landed between typing and listing. Run `rivet load` again to \
             type from the newest run.",
            late.join(", ")
        )
    })
}

/// The runs listed after `pinned` in a newest-first listing; none when it is absent.
pub(super) fn runs_older_than<'a>(
    newest_first: &'a [(String, String)],
    pinned: &str,
) -> &'a [(String, String)] {
    let start = newest_first
        .iter()
        .position(|(_, id)| id == pinned)
        .map_or(newest_first.len(), |i| i + 1);
    &newest_first[start..]
}

/// The older runs this load will read alongside the pinned one.
pub(super) fn runs_loaded_with_the_pin<'a>(
    op: &str,
    mode: load::plan::LoadMode,
    newest_first: &'a [(String, String)],
    pinned: &str,
    loaded: &std::collections::HashSet<String>,
) -> Vec<&'a str> {
    if op == "compact" || mode == load::plan::LoadMode::Full {
        return Vec::new();
    }
    runs_older_than(newest_first, pinned)
        .iter()
        .map(|(_, id)| id.as_str())
        .filter(|id| !loaded.contains(*id))
        .collect()
}

/// The refusal for pending runs that spell a column differently from the run the load is typed from.
pub(super) fn respelled_refusal(
    table: &str,
    pinned: &str,
    respelled: &[(String, String, String)],
) -> Option<String> {
    if respelled.is_empty() {
        return None;
    }
    let list = respelled
        .iter()
        .map(|(run, was, now)| format!("run {run} wrote `{was}` where run {pinned} writes `{now}`"))
        .collect::<Vec<_>>()
        .join("; ");
    Some(format!(
        "load [{table}]: {list}. The source column was renamed while the older run(s) were \
         still unloaded, and one load cannot read both spellings: BigQuery matches Parquet \
         columns by name, so the older files would load that column as NULL. Nothing was \
         loaded and nothing is lost — every run stays staged. rivet cannot load the two \
         spellings in one pass yet. To clear it by hand: load the older run(s)' files into a \
         scratch table declaring the column under its OLD spelling, rename it there, append \
         those rows to the warehouse table, and only then delete those runs' manifest(s)."
    ))
}

/// The stderr line naming the newer runs the pin passed over, or `None` when the pinned run is the newest.
pub(super) fn skipped_runs_note(table: &str, pinned: &str, skipped: &[&str]) -> Option<String> {
    (!skipped.is_empty()).then(|| {
        format!(
            "  load [{table}]: typed from run {pinned}; {} newer run(s) recorded no spec and \
             are loaded with its columns: {}",
            skipped.len(),
            skipped.join(", ")
        )
    })
}
