//! The staged-prefix lifecycle: which runs are writing into an export prefix, and the two deletes gated on that answer.

use crate::destination::gcs::GcsStore;
use crate::error::Result;
use crate::load;
use crate::state::StateStore;

type RunIds = std::collections::HashSet<String>;
type Keyed = [(String, crate::manifest::RunManifest)];

/// The runs the run-status ledger says are writing into `prefix`.
fn ledger_writers(state: Option<&StateStore>, prefix: &str) -> Option<Result<RunIds>> {
    state.map(|s| s.active_run_ids_on_prefix(prefix))
}

/// The ledger's writers sampled before the manifests are read.
pub(super) fn writers_before_fetch(state: Option<&StateStore>, prefix: &str) -> Option<RunIds> {
    ledger_writers(state, prefix).and_then(|r| match r {
        Ok(a) => Some(a),
        Err(e) => {
            log::warn!(
                "load: cannot tell which runs are writing into {prefix} at fetch time ({e:#}) — \
                 this cycle will not record any run as consumed, so nothing they write \
                 later is stranded"
            );
            None
        }
    })
}

/// The writers at record time: ledger plus bucket, every selected run when the ledger cannot answer, `None` when stateless.
pub(super) fn writers_at_record(
    state: Option<&StateStore>,
    prefix: &str,
    bucket: &RunIds,
    selected: &[String],
) -> Option<RunIds> {
    match ledger_writers(state, prefix)? {
        Ok(mut a) => {
            a.extend(bucket.iter().cloned());
            Some(a)
        }
        Err(e) => {
            log::warn!(
                "load: cannot tell which runs are still writing into {prefix} ({e:#}) — not \
                 recording any run as consumed this cycle, so nothing they write later is \
                 stranded. The next load re-evaluates them."
            );
            Some(selected.iter().cloned().collect())
        }
    }
}

/// The runs the bucket says may be writing: live `running` markers, and the CDC runs no ledger row accounts for.
pub(super) fn bucket_writers(state: Option<&StateStore>, keyed: &Keyed) -> RunIds {
    let mut writers = load::reconcile::active_running_run_ids(keyed);
    writers.extend(unlisted_cdc_runs(state, keyed));
    writers
}

/// CDC runs with a manifest under the prefix, no ledger row, and no newer CDC run of their family.
fn unlisted_cdc_runs(state: Option<&StateStore>, keyed: &Keyed) -> RunIds {
    let Some(state) = state else {
        return RunIds::new();
    };
    let census = crate::manifest::census::ManifestCensus::new(keyed);
    let started =
        |m: &crate::manifest::RunManifest| chrono::DateTime::parse_from_rfc3339(&m.started_at).ok();
    let cdc: Vec<_> = census
        .runs()
        .iter()
        .filter(|r| r.manifest.mode == "cdc")
        .map(|r| (r, started(r.manifest)))
        .collect();
    let mut newest = std::collections::HashMap::new();
    for (r, at) in &cdc {
        if let Some(at) = at {
            let slot = newest.entry(r.family()).or_insert(*at);
            *slot = (*slot).max(*at);
        }
    }
    cdc.iter()
        .filter(|(r, at)| {
            let followed = at.is_some_and(|at| newest.get(r.family()).is_some_and(|n| *n > at));
            !followed && !matches!(state.run_status_of(&r.manifest.run_id), Ok(Some(_)))
        })
        .map(|(r, _)| r.manifest.run_id.clone())
        .collect()
}

/// The runs that may still be writing into the prefix, or `None` when a stateful load could not tell.
pub(super) fn still_writing(
    active_at_fetch: &Option<RunIds>,
    bucket: &RunIds,
    stateful: bool,
) -> Option<RunIds> {
    match (active_at_fetch, stateful) {
        (None, true) => None,
        (at_fetch, _) => {
            let mut writing = at_fetch.clone().unwrap_or_default();
            writing.extend(bucket.iter().cloned());
            Some(writing)
        }
    }
}

/// Whether the run-status ledger says any run is writing into `prefix`, folded for the delete guards by [`ledger_says_active`].
fn ledger_says_writing(state: Option<&StateStore>, prefix: &str) -> bool {
    ledger_says_active(state.map(|s| s.has_active_run_on_prefix(prefix)))
}

/// What the run-status LEDGER says about a prefix, folded from the three answers it can give.
pub(super) fn ledger_says_active(answer: Option<Result<bool>>) -> bool {
    match answer {
        None => false,
        Some(Ok(active)) => active,
        Some(Err(_)) => true,
    }
}

/// Fold the two INDEPENDENT activity signals into one verdict.
pub(super) fn prefix_is_active(ledger_active: bool, manifest_active: bool) -> bool {
    ledger_active || manifest_active
}

/// Is a run writing into this prefix right now?
pub(super) fn prefix_has_active_run(
    store: &crate::destination::gcs::GcsStore,
    prefix: &str,
    state: Option<&StateStore>,
) -> bool {
    let ledger_active = ledger_says_writing(state, prefix);
    if ledger_active {
        return true;
    }
    let manifest_active = match load::reconcile::fetch_manifests_keyed(store, prefix) {
        Ok(keyed) => !bucket_writers(state, &keyed).is_empty(),
        Err(_) => true,
    };
    prefix_is_active(ledger_active, manifest_active)
}

/// Whether `cleanup_source` actually deletes a prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupVerdict {
    /// `cleanup_source` is off — no delete, and nothing to announce.
    NotRequested,
    /// Cleanup was requested but a run is writing into the prefix.
    RefusedRunActive,
    /// Delete the staged Parquet under the prefix.
    Delete,
}

/// The verdict, pure.
pub(super) fn cleanup_verdict(requested: bool, active: impl FnOnce() -> bool) -> CleanupVerdict {
    if !requested {
        return CleanupVerdict::NotRequested;
    }
    if active() {
        return CleanupVerdict::RefusedRunActive;
    }
    CleanupVerdict::Delete
}

/// The delete target for `cleanup_source`, or `None` when a run is writing here.
pub(super) fn cleanup_target<'a>(
    plan: &'a load::plan::LoadPlan,
    store: &'a crate::destination::gcs::GcsStore,
    state: Option<&StateStore>,
) -> Option<(&'a crate::destination::gcs::GcsStore, &'a str)> {
    match cleanup_verdict(plan.load.cleanup_source, || {
        prefix_has_active_run(store, &plan.gcs_prefix, state)
    }) {
        CleanupVerdict::NotRequested => None,
        CleanupVerdict::RefusedRunActive => {
            eprintln!(
                "  cleanup [{}]: SKIPPED — a run is writing into {} right now. Deleting the \
                 prefix would remove parts that run has already committed, and on a \
                 CDC/incremental export the source position has advanced past them. Re-run the \
                 load once the extract has finished — or, if this is a DEAD crash remnant \
                 (no extract is actually running), inspect it with `rivet state runs -c \
                 <config> --running` and close it with `rivet state finish-run -c <config> \
                 --run-id <id>`.",
                plan.table, plan.gcs_prefix
            );
            None
        }
        CleanupVerdict::Delete => Some((store, plan.gcs_prefix.as_str())),
    }
}

/// [`cleanup_target`], plus the PREFIX lease the delete has to hold while it runs.
pub(super) fn cleanup_target_leased<'a>(
    plan: &'a load::plan::LoadPlan,
    store: &'a crate::destination::gcs::GcsStore,
    state: Option<&'a StateStore>,
    runs: &[(String, crate::manifest::RunManifest)],
    writing: Option<&std::collections::HashSet<String>>,
) -> (
    Option<(&'a crate::destination::gcs::GcsStore, Cleanup)>,
    Option<crate::state::LoadLease<'a>>,
) {
    let (target, lease) = prefix_target_leased(plan, store, state);
    let Some((store, _)) = target else {
        return (None, lease);
    };
    let Some(writing) = writing else {
        eprintln!(
            "  cleanup [{}]: SKIPPED — could not tell which runs are still writing into {}; \
             nothing is deleted this cycle, and the next load with `cleanup_source` retries.",
            plan.table, plan.gcs_prefix
        );
        return (None, lease);
    };
    let keys = cleanup_keys(runs, writing, |dir| canonical_run_id(store, dir));
    (Some((store, keys)), lease)
}

/// The run a directory's canonical `manifest.json` describes, if it can be read.
fn canonical_run_id(store: &GcsStore, dir: &str) -> Option<String> {
    let bytes = store
        .read(&join_key(dir, crate::manifest::MANIFEST_FILENAME))
        .ok()?;
    serde_json::from_slice::<crate::manifest::RunManifest>(&bytes)
        .ok()
        .map(|m| m.run_id)
}

/// `dir/name`, or `name` at the bucket root.
fn join_key(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

/// What `cleanup_source` deletes: plain keys now, and a directory's canonical `manifest.json` and `_SUCCESS` only while that manifest still describes a cleaned run.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Cleanup {
    pub(super) keys: Vec<String>,
    canonical_dirs: Vec<String>,
    runs: std::collections::HashSet<String>,
}

impl Cleanup {
    /// A cleanup of exactly `keys`, with no canonical pointer to retire.
    #[cfg(test)]
    pub(super) fn plain(keys: Vec<String>) -> Self {
        Self {
            keys,
            ..Self::default()
        }
    }

    /// The directories whose canonical pointer is retired when it still names a cleaned run.
    #[cfg(test)]
    pub(super) fn canonical_dirs(&self) -> &[String] {
        &self.canonical_dirs
    }
}

/// The manifests, parts and (when they name a cleaned run) canonical pointers of every run read that nobody is still writing.
pub(super) fn cleanup_keys(
    runs: &[(String, crate::manifest::RunManifest)],
    writing: &std::collections::HashSet<String>,
    canonical_run: impl Fn(&str) -> Option<String>,
) -> Cleanup {
    let done: Vec<&(String, crate::manifest::RunManifest)> = runs
        .iter()
        .filter(|(_, m)| !writing.contains(&m.run_id))
        .collect();
    let ids: std::collections::HashSet<&str> =
        done.iter().map(|(_, m)| m.run_id.as_str()).collect();
    let mut keys = std::collections::BTreeSet::new();
    let mut canonical_dirs = std::collections::BTreeSet::new();
    for (key, m) in done {
        keys.insert(key.clone());
        keys.extend(load::reconcile::resolve_parts(key, m));
        let dir = key.rsplit_once('/').map_or("", |(d, _)| d);
        if canonical_run(dir).is_some_and(|id| ids.contains(id.as_str())) {
            canonical_dirs.insert(dir.to_string());
        }
    }
    Cleanup {
        keys: keys.into_iter().collect(),
        canonical_dirs: canonical_dirs.into_iter().collect(),
        runs: ids.into_iter().map(str::to_string).collect(),
    }
}

/// [`cleanup_target`] under the prefix lease.
fn prefix_target_leased<'a>(
    plan: &'a load::plan::LoadPlan,
    store: &'a crate::destination::gcs::GcsStore,
    state: Option<&'a StateStore>,
) -> (
    Option<(&'a crate::destination::gcs::GcsStore, &'a str)>,
    Option<crate::state::LoadLease<'a>>,
) {
    let target = cleanup_target(plan, store, state);
    if target.is_none() {
        return (None, None);
    }
    let Some(s) = state else {
        return (target, None);
    };
    match s.try_load_lease(&plan.gcs_prefix) {
        Ok(Some(lease)) => (target, Some(lease)),
        Ok(None) => {
            eprintln!(
                "{}",
                lease_skip_message(&plan.table, &plan.gcs_prefix, None)
            );
            (None, None)
        }
        Err(e) => {
            let why = format!("{e:#}");
            eprintln!(
                "{}",
                lease_skip_message(&plan.table, &plan.gcs_prefix, Some(&why))
            );
            (None, None)
        }
    }
}

/// The line printed when cleanup is skipped for want of the prefix lease — before the load runs.
fn lease_skip_message(table: &str, prefix: &str, lease_error: Option<&str>) -> String {
    match lease_error {
        None => format!(
            "  cleanup [{table}]: SKIPPED — another rivet holds {prefix} right now; the load \
             proceeds; the staged Parquet will be left in place, and the next load's cleanup \
             removes it."
        ),
        Some(e) => format!(
            "  cleanup [{table}]: SKIPPED — could not take the prefix lease on {prefix} ({e}). \
             Not deleting what cannot be confirmed idle; the load proceeds."
        ),
    }
}

/// Best-effort orphan-Parquet GC for one table's prefix (config `gc_orphans`).
pub(super) fn maybe_gc_orphans(
    store: &crate::destination::gcs::GcsStore,
    plan: &load::plan::LoadPlan,
    state: Option<&StateStore>,
) {
    let keyed = match load::reconcile::fetch_manifests_keyed(store, &plan.gcs_prefix) {
        Ok(k) => k,
        Err(e) => {
            eprintln!(
                "  gc-orphans [{}]: skipped (manifest fetch failed): {e:#}",
                plan.table
            );
            return;
        }
    };
    let ledger_active = ledger_says_writing(state, &plan.gcs_prefix);
    let active = prefix_is_active(ledger_active, !bucket_writers(state, &keyed).is_empty());
    let dead_marker_run_ids: std::collections::HashSet<String> = match state {
        Some(s) => keyed
            .iter()
            .filter(|(_, m)| m.status == crate::manifest::ManifestStatus::Running)
            .filter(|(_, m)| {
                matches!(
                    s.run_status_of(&m.run_id),
                    Ok(Some(status)) if status != "running"
                )
            })
            .map(|(_, m)| m.run_id.clone())
            .collect(),
        None => Default::default(),
    };
    match load::reconcile::gc_orphans(
        store,
        &plan.gcs_prefix,
        &keyed,
        active,
        &dead_marker_run_ids,
    ) {
        Ok((0, _)) => {}
        Ok((n, bytes)) => {
            eprintln!(
                "  gc-orphans [{}]: removed {n} orphan part(s) ({bytes} bytes)",
                plan.table
            )
        }
        Err(e) => eprintln!(
            "  gc-orphans [{}]: failed (load unaffected): {e:#}",
            plan.table
        ),
    }
}

/// Clean up iff `cleanup` is `Some`, downgrading a failure to a warning.
pub(super) fn maybe_cleanup(cleanup: Option<(&GcsStore, &Cleanup)>) -> bool {
    match cleanup {
        Some((store, cleanup)) => match remove_cleanup(store, cleanup) {
            Ok(()) => true,
            Err(e) => {
                eprintln!(
                    "warning: source cleanup failed (data is safely loaded): {}",
                    crate::redact::redact_secrets(&format!("{e:#}"))
                );
                false
            }
        },
        None => false,
    }
}

/// Remove the plain keys, and each canonical pointer only when it still names a cleaned run at the moment of its delete.
fn remove_cleanup(store: &GcsStore, cleanup: &Cleanup) -> anyhow::Result<()> {
    let names_cleaned_run = |dir: &str| {
        seeded!(
            "cleanup-removes-a-pointer-that-changed-hands",
            true,
            canonical_run_id(store, dir).is_some_and(|id| cleanup.runs.contains(&id))
        )
    };
    for dir in &cleanup.canonical_dirs {
        if names_cleaned_run(dir) {
            store.remove(&join_key(dir, crate::manifest::SUCCESS_FILENAME))?;
        }
    }
    cleanup.keys.iter().try_for_each(|k| store.remove(k))?;
    for dir in &cleanup.canonical_dirs {
        if names_cleaned_run(dir) {
            store.remove(&join_key(dir, crate::manifest::MANIFEST_FILENAME))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod lease_skip_message_tests {
    use super::lease_skip_message;

    #[test]
    fn a_lease_skip_printed_before_the_load_does_not_claim_the_load_succeeded() {
        for err in [None, Some("state db down")] {
            let msg = lease_skip_message("orders", "gs://b/p/", err);
            assert!(!msg.contains("succeeded"), "{msg}");
            assert!(msg.contains("the load proceeds"), "{msg}");
        }
    }
}
