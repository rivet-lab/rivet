//! The `rivet load` ORCHESTRATOR — evicted wholesale from `cli/dispatch.rs` (arch-roast 2026-08-21, Strong, challenge-confirmed).

use crate::error::Result;
use crate::load;
#[cfg(test)]
use crate::load::ledger::ledger_status;
use crate::load::ledger::{closing_status, ledger_load_id, ownership_of};
use crate::load::partition_budget::{budgeted_uris, partition_budget_ok, partition_label};
use crate::load::pin::{late_runs_stop, pin_plan_to_its_run};
use crate::load::result::{TableDone, Work};
use crate::load::staging::{
    bucket_writers, cleanup_target_leased, maybe_gc_orphans, still_writing, writers_at_record,
    writers_before_fetch,
};
use crate::state::LoadStatus;
use crate::state::{LoadRecord, StateRef, StateStore};
use anyhow::Context as _;

pub struct LoadArgs {
    pub config: String,
    pub run_id: Option<String>,
    /// Rebuild a change log whose partition differs from the config (ADR-0034 D5).
    pub rebuild_changelog: bool,
    /// Worker threads to load the config's tables on.
    pub pool: Option<usize>,
    /// The tables `--export` / `--table` name; every table when both are absent.
    pub selection: load::plan::Selection,
    /// Where the human lines and the `--json` / `--summary-output` document go.
    pub report: load::result::Report,
}

/// `rivet load`: config-driven warehouse load.
pub fn run_loads(args: LoadArgs) -> Result<()> {
    let plans = load::plan::plan_loads(&args.config, &args.selection)?;
    let run_id = resolve_run_id(args.run_id.clone());
    let state = open_state(
        &args.config,
        "loading without a ledger (no incremental skip / audit log)",
    );
    let ledger_errored = state.is_none();
    let tables: Vec<&str> = plans.iter().map(|p| p.table.as_str()).collect();
    eprintln!(
        "{}: resolved {} table(s) → {} [run_id={}]: {}",
        args.config,
        plans.len(),
        plans.first().map(|p| p.load.target.name()).unwrap_or("?"),
        run_id,
        tables.join(", ")
    );

    let engine = if needs_source_engine(&plans) {
        Some(load::plan::source_engine(&args.config)?)
    } else {
        None
    };
    let cfg = crate::config::Config::load(&args.config).context("parsing rivet config")?;
    let (state_ref, parent_had_state) = hand_off_state(state, args.pool, plans.len());
    let journals = journals_of(&plans);
    let outcomes = load::pool::run_workers(
        &plans,
        load::pool::effective_pool(args.pool, plans.len()),
        || reconnect(state_ref.as_ref(), "load").map(|s| (s, ledger_errored)),
        |worker, idx, plan| {
            let (state, ledger_errored) = (&worker.0, worker.1);
            let journal = &journals[idx];
            if load::pool::reconnect_failure_is_fatal(parent_had_state, state.is_some()) {
                crate::rivet_bail!(
                    crate::error::codes::LOAD_LEDGER_UNREADABLE,
                    "`{}`: this worker could not reopen the state ledger the run started \
                     with — refusing the table rather than loading it without a lease and \
                     without recording it. Lower `--pool`, or fix the state backend.",
                    plan.table
                );
            }
            let load_id = ledger_load_id(&run_id, "load", &plan.table);
            let drift = plan.load.allow_source_drift;
            let say = |line: String| args.report.say(&line);
            let outcome = (|| -> Result<TableDone> {
                let pinned = pin_plan_to_its_run(plan, state.as_ref(), &cfg, "load")?;
                let plan = &pinned;
                match plan.mode {
                    load::plan::LoadMode::Cdc => {
                        let pk = require_pk(plan, "cdc")?;
                        refuse_mixed_mongo_ids(&cfg, plan, &args.config)?;
                        match load_one_cdc(
                            plan,
                            &run_id,
                            engine.expect("engine resolved above for a cdc plan"),
                            pk,
                            drift,
                            args.rebuild_changelog,
                            state.as_ref(),
                            ledger_errored,
                            &load_id,
                            journal,
                        )? {
                            Some(report) => {
                                say(format!(
                                    "CDC LOAD OK [{}]: {}",
                                    plan.table,
                                    cdc_ok_line(&report)
                                ));
                                Ok(TableDone::Work(cdc_work(&report)))
                            }
                            None => {
                                say(format!("CDC LOAD SKIP [{}]: up to date", plan.table));
                                Ok(TableDone::Skipped(load::result::UP_TO_DATE))
                            }
                        }
                    }
                    load::plan::LoadMode::Incremental => {
                        let pk = require_pk(plan, "incremental")?;
                        match load_one_incremental(
                            plan,
                            &run_id,
                            pk,
                            drift,
                            args.rebuild_changelog,
                            state.as_ref(),
                            &load_id,
                            &args.config,
                            journal,
                        )? {
                            Some(report) => {
                                say(format!(
                                    "INCREMENTAL LOAD OK [{}]: {}",
                                    plan.table,
                                    report.summary()
                                ));
                                Ok(TableDone::Work(Work::loaded(report.rows(), report.wrote())))
                            }
                            None => {
                                say(format!(
                                    "INCREMENTAL LOAD SKIP [{}]: up to date",
                                    plan.table
                                ));
                                Ok(TableDone::Skipped(load::result::UP_TO_DATE))
                            }
                        }
                    }
                    load::plan::LoadMode::Full => {
                        match load_one(plan, &run_id, drift, state.as_ref(), &load_id, journal)? {
                            Some(report) => {
                                say(format!(
                                    "LOAD OK [{}]: {} row(s) in `{}`{}",
                                    plan.table,
                                    report.rows_loaded,
                                    report.target_table,
                                    cleaned_suffix(report.source_cleaned)
                                ));
                                Ok(TableDone::Work(Work::loaded(
                                    report.rows_loaded,
                                    &report.target_table,
                                )))
                            }
                            None => {
                                say(format!("LOAD SKIP [{}]: up to date", plan.table));
                                Ok(TableDone::Skipped(load::result::UP_TO_DATE))
                            }
                        }
                    }
                }
            })();
            if let Err(e) = &outcome {
                eprintln!(
                    "  LOAD FAILED [{}]: {}",
                    plan.table,
                    crate::redact::redact_secrets(&format!("{e:#}"))
                );
            }
            let done = outcome.map_err(|e| e.context(format!("load '{}'", plan.table)))?;
            if plan.load.gc_orphans {
                match load::open_store(&plan.destination) {
                    Ok(store) => maybe_gc_orphans(&store, plan, state.as_ref()),
                    Err(e) => eprintln!(
                        "  gc-orphans [{}]: skipped (store unavailable): {e:#}",
                        plan.table
                    ),
                }
            }
            Ok(done)
        },
        |plan| no_outcome_error("load", &plan.table),
    );
    let result = command_result(&run_id, &plans, &outcomes, &journals, "loaded");
    let tables = match aggregate_load_failures(failures_of(outcomes)) {
        Some(e) => Err(e),
        None => Ok(()),
    };
    load::result::exit_of(tables, args.report.finish(&result))
}

/// The document of one load or compact: each plan's entry, in config order.
pub(super) fn command_result(
    run_id: &str,
    plans: &[load::plan::LoadPlan],
    outcomes: &[Result<TableDone>],
    journals: &[load::result::JobJournal],
    done: &'static str,
) -> load::result::CommandResult {
    load::result::CommandResult {
        run_id: run_id.to_string(),
        per_table: plans
            .iter()
            .zip(outcomes)
            .zip(journals)
            .map(|((plan, outcome), journal)| {
                load::result::table_result(
                    &plan.export_name,
                    &plan.table,
                    &address_of(plan, run_id, done),
                    done,
                    outcome,
                    journal,
                )
            })
            .collect(),
    }
}

/// What `plan`'s table is worked on through, named by its loader's own `fqtn` (building a loader does no I/O).
pub(super) fn address_of(
    plan: &load::plan::LoadPlan,
    run_id: &str,
    done: &str,
) -> load::result::Address {
    let loader = load::build_loader(plan, run_id, &Default::default());
    let changes = loader.fqtn(&format!("{}__changes", plan.table));
    let keeps_log = plan.mode != load::plan::LoadMode::Full;
    match done {
        "loaded" => load::result::Address {
            target: if keeps_log {
                changes
            } else {
                loader.fqtn(&plan.table)
            },
            buffer: None,
        },
        _ => load::result::Address {
            target: loader.fqtn(&plan.table),
            buffer: (keeps_log && plan.layout.compacts()).then_some(changes),
        },
    }
}

/// One journal per plan, naming jobs where the plan's warehouse does.
pub(super) fn journals_of(plans: &[load::plan::LoadPlan]) -> Vec<load::result::JobJournal> {
    plans
        .iter()
        .map(|p| load::result::JobJournal::new(p.load.target.names_jobs()))
        .collect()
}

/// The typo warning for a prefix with no manifests.
fn empty_prefix_note(
    table: &str,
    prefix: &str,
    empty: bool,
    loaded: &std::collections::HashSet<String>,
) -> Option<String> {
    (empty && loaded.is_empty()).then(|| {
        format!(
            "  load [{table}]: found NO manifests under {prefix} — nothing was ever staged \
             here. If an export should have landed, check the prefix for typos \
             (a wrong prefix reads as permanently 'up to date')."
        )
    })
}

/// Whether a load whose runs resolve to no files is a no-op.
fn nothing_to_load(whole_table_pass: bool, no_files: bool) -> bool {
    no_files && !whole_table_pass
}

/// Whether `runs` hold a pass over the whole table: any full load, or a baseline leg of a base-and-buffer stream.
fn holds_whole_table_pass(
    plan: &load::plan::LoadPlan,
    runs: &[(String, crate::manifest::RunManifest)],
) -> bool {
    match plan.mode {
        load::plan::LoadMode::Full => true,
        load::plan::LoadMode::Cdc => {
            seeded!(
                "empty-baseline-builds-no-base",
                false,
                plan.layout.compacts()
            ) && runs.iter().any(|(_, m)| is_baseline_leg(m))
        }
        load::plan::LoadMode::Incremental => false,
    }
}

/// Whether the base of a base-and-buffer stream is missing.
fn base_was_never_built(
    plan: &load::plan::LoadPlan,
    state: Option<&StateStore>,
    loader: &dyn load::TargetLoader,
) -> Result<bool> {
    let Some(state) = state else {
        return Ok(false);
    };
    if !(plan.mode == load::plan::LoadMode::Cdc && plan.layout.compacts()) {
        return Ok(false);
    }
    let target = loader.fqtn(&plan.table);
    Ok(state.loaded_baseline_legs(&target)? == (1, 0)
        && !state.has_merged_rows(&target)?
        && loader.object_kind(&plan.table)? == load::ObjectKind::Absent)
}

/// The columns of a base-and-buffer base: the plan's, and the delete flag when the base carries it.
fn base_specs(
    plan: &load::plan::LoadPlan,
    loader: &dyn load::TargetLoader,
) -> Vec<crate::types::target::TargetColumnSpec> {
    let mut specs = plan.specs.clone();
    if load::plan::base_carries_delete_flag(true, plan.deleted_flag) {
        specs.push(load::cdc::flag_spec(loader.warehouse()));
    }
    specs
}

/// Create the empty base an earlier load of an empty baseline left out, so the buffered changes have a table to merge into.
fn build_missing_base(
    plan: &load::plan::LoadPlan,
    state: Option<&StateStore>,
    loader: &dyn load::TargetLoader,
) -> Result<()> {
    if !load::before_write(base_was_never_built(plan, state, loader))? {
        return Ok(());
    }
    eprintln!("{}", rebuild_note(&loader.fqtn(&plan.table)));
    load::run_load(
        loader,
        &plan.table,
        &base_specs(plan, loader),
        &[],
        Some(0),
        None,
        load::Ownership::Own,
    )?;
    Ok(())
}

/// What a load says when it builds the base an earlier load of an empty baseline left out.
fn rebuild_note(target_fqtn: &str) -> String {
    format!(
        "  note: `{target_fqtn}` does not exist although its baseline was loaded: the baseline \
         held no rows, and rivet 0.31 and older created no table for one. Creating it empty \
         now; the changes buffered since are merged into it by `rivet compact`"
    )
}

/// The refusal when another `rivet load` or `rivet compact` holds the table's lease.
fn lease_busy_message(target_fqtn: &str) -> String {
    format!(
        "another `rivet load` or `rivet compact` is writing `{target_fqtn}` right now (the \
         lease is held; it is released when that process ends, crash included). Wait for it, \
         then retry."
    )
}

/// Does this invocation have to resolve the source's `__pos` parse ENGINE?
pub(super) fn needs_source_engine(plans: &[load::plan::LoadPlan]) -> bool {
    plans.iter().any(|p| p.mode == load::plan::LoadMode::Cdc)
}

/// Why a load from `mine` into a table last loaded from `other` is refused.
pub(super) fn source_conflict_refusal(target_fqtn: &str, other: &str, mine: &str) -> String {
    format!(
        "target table `{target_fqtn}` was last loaded from `{other}` and this load carries \
         `{mine}` — loading would REPLACE the other source's rows, and both commands would \
         report success. Give this source its own target table."
    )
}

/// Why a CDC snapshot over an earlier full-load table is refused, with the ways out this warehouse takes.
pub(super) fn snapshot_over_full_table_refusal(
    fqtn: &str,
    warehouse: load::cdc::Warehouse,
) -> String {
    let keep = match warehouse {
        load::cdc::Warehouse::ClickHouse => "",
        _ => {
            ", or run the stream without `initial: snapshot` to keep the table's rows as the baseline"
        }
    };
    format!(
        "`{fqtn}` is a table from an earlier full load, and this CDC load carries an initial \
         snapshot of the same table — the snapshot is a new baseline, so keep one: drop the \
         table (the snapshot replaces it){keep}"
    )
}

/// Fold every per-plan failure into ONE error, or `None` when nothing failed.
pub(crate) fn aggregate_load_failures(mut failures: Vec<anyhow::Error>) -> Option<anyhow::Error> {
    if failures.is_empty() {
        return None;
    }
    let primary_idx = crate::error::representative_failure_idx(&failures)?;
    let primary = failures.remove(primary_idx);
    if failures.is_empty() {
        return Some(primary);
    }
    let others = failures
        .iter()
        .map(|e| format!("{e:#}"))
        .collect::<Vec<_>>()
        .join("; ");
    Some(primary.context(format!(
        "{} load(s) failed; representative error follows (also: {others})",
        failures.len() + 1
    )))
}

/// The parent's state store, or `None` — warned, naming the `fallback` — when it cannot be opened.
pub(super) fn open_state(config: &str, fallback: &str) -> Option<StateStore> {
    match StateStore::open(config) {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("  warning: state store unavailable ({e:#}); {fallback}");
            None
        }
    }
}

/// Close the parent store and warn on the pool's connection ceiling.
pub(super) fn hand_off_state(
    state: Option<StateStore>,
    pool: Option<usize>,
    tables: usize,
) -> (Option<StateRef>, bool) {
    let state_ref = state.as_ref().map(|s| s.state_ref().clone());
    let parent_had_state = state.is_some();
    drop(state);
    let ledger = match &state_ref {
        Some(StateRef::Sqlite(_)) => load::pool::LedgerKind::Sqlite,
        Some(StateRef::Postgres(_)) => load::pool::LedgerKind::Postgres,
        None => load::pool::LedgerKind::Absent,
    };
    if let Some(w) = load::pool::pool_ceiling_warning(pool, tables, ledger) {
        eprintln!("  warning: {w}");
    }
    (state_ref, parent_had_state)
}

/// A worker's reconnect to the parent's backend: `Some(None)` when stateless, `None` (warned) when it failed.
pub(super) fn reconnect(state_ref: Option<&StateRef>, verb: &str) -> Option<Option<StateStore>> {
    match state_ref {
        None => Some(None),
        Some(r) => match StateStore::open_at_ref(r) {
            Ok(s) => Some(Some(s)),
            Err(e) => {
                eprintln!(
                    "  warning: state store unavailable to this worker ({e:#}); it takes \
                     no table — the other workers {verb} the queue"
                );
                None
            }
        },
    }
}

/// The failure reported for a table that ended with no outcome of its own.
pub(super) fn no_outcome_error(op: &str, table: &str) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::INTERNAL_LOAD_INVARIANT,
        format!(
            "{op} '{table}' did not complete — it PANICKED (a bug, please report it), or \
         no worker was left to take it (each one panicked or could not reopen the \
         state ledger — see the warnings above). Reported as this table's failure \
         so every other table still aggregates"
        ),
    )
}

/// The failed outcomes, in the order the pool returned them.
pub(super) fn failures_of<T>(outcomes: Vec<Result<T>>) -> Vec<anyhow::Error> {
    outcomes.into_iter().filter_map(Result::err).collect()
}

/// The table's lease (`None` when stateless); refuses when another load or compact holds it.
pub(super) fn take_table_lease<'a>(
    state: Option<&'a StateStore>,
    target_fqtn: &str,
) -> Result<Option<crate::state::LoadLease<'a>>> {
    let what = || format!("the table lease of `{target_fqtn}`");
    match state
        .map(|s| s.take_lease_waiting(target_fqtn, &what))
        .transpose()?
    {
        Some(None) => crate::rivet_bail!(
            crate::error::codes::LOAD_TABLE_BUSY,
            "{}",
            lease_busy_message(target_fqtn)
        ),
        held => Ok(held.flatten()),
    }
}

/// Whether the table's lease is still this process's; a stateless load holds none to lose.
pub(super) fn holds(lease: &Option<crate::state::LoadLease<'_>>) -> bool {
    lease.as_ref().is_none_or(crate::state::LoadLease::is_held)
}

/// What a load or compact says when its table lease stopped being its own.
fn lease_lost_message(target_fqtn: &str, written: bool) -> String {
    let lost = format!(
        "the lease on `{target_fqtn}` is no longer this process's: it was not renewed within \
         its TTL (the process stalled, the state database was out of reach, or the lease \
         keeper stopped), so another `rivet load` or `rivet compact` may be writing the table"
    );
    match written {
        false => format!(
            "{lost}. This run wrote nothing to the warehouse and recorded nothing as loaded; \
             wait for the other process, then run again."
        ),
        true => format!(
            "{lost} at the same time as this run did. This run's warehouse write is done and \
             is recorded as failed, not as a success: check `{target_fqtn}`, then run again."
        ),
    }
}

/// Stop once the table's lease is not `held`.
pub(super) fn lease_still_held(held: bool, target_fqtn: &str, written: bool) -> Result<()> {
    if held {
        return Ok(());
    }
    let coded = |code| {
        let message = lease_lost_message(target_fqtn, written);
        Err(anyhow::Error::new(crate::error::CodedError::new(
            code, message,
        )))
    };
    match written {
        false => load::before_write(coded(crate::error::codes::STATE_LEASE_LOST)),
        true => coded(crate::error::codes::LOAD_LEASE_LOST_DURING_WRITE),
    }
}

/// Refuse a keyed MongoDB CDC table whose checkpoint records more than one `_id` type, before any append.
fn refuse_mixed_mongo_ids(
    cfg: &crate::config::Config,
    plan: &load::plan::LoadPlan,
    config_path: &str,
) -> Result<()> {
    if cfg.source.source_type != crate::config::SourceType::Mongo {
        return Ok(());
    }
    let Some(export) = cfg.exports.iter().find(|e| e.name == plan.export_name) else {
        return Ok(());
    };
    let Some(collection) = plan.unit.as_deref().or(export.table.as_deref()) else {
        return Ok(());
    };
    let dir = std::path::Path::new(config_path)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let checkpoint = export
        .cdc
        .as_ref()
        .and_then(|c| c.checkpoint.as_deref())
        .map(|raw| crate::source::cdc::resolve_checkpoint(raw, dir));
    crate::source::mongo::id_types::persisted(checkpoint.as_deref())?.refuse_if_mixed(collection)
}

/// The resolved dedup key for an append mode (`cdc` / `incremental`).
pub(super) fn require_pk<'a>(plan: &'a load::plan::LoadPlan, mode: &str) -> Result<&'a [String]> {
    if plan.pk.is_empty() {
        let what = match plan.continued_key {
            true => {
                "loads by append (each run carries only the keys past the last one)".to_string()
            }
            false => format!("is mode: {mode}"),
        };
        let why = match plan.load.pk {
            load::plan::KeyColumns::None => {
                "its `load:` block sets `pk: none`, so name the key there instead (e.g. `pk: [id]`)"
            }
            _ => {
                "`rivet run` recorded none (a `query:` export, or a table without one), so \
                 declare it in the export's `load:` block (e.g. `pk: [id]`)"
            }
        };
        crate::rivet_bail!(
            crate::error::codes::LOAD_PRIMARY_KEY_MISSING,
            "export `{}` {what} but has no primary key for the current-state dedup view — {why}",
            plan.export_name
        );
    }
    Ok(&plan.pk)
}

/// What a load will consume: the reconciled integrity, the parquet URIs to load, and the extraction run_ids covered.
struct LoadInputs {
    integrity: load::reconcile::LoadIntegrity,
    uris: Vec<String>,
    source_run_ids: Vec<String>,
    /// `engine:schema.table` of the manifests this load consumes.
    source_ident: String,
    /// Which runs were still WRITING at the moment the manifests were FETCHED.
    active_at_fetch: Option<std::collections::HashSet<String>>,
    /// Which runs the BUCKET says are still writing — non-superseded `running` markers.
    marker_active: std::collections::HashSet<String>,
    /// The selected run manifests, keyed by their bucket path.
    runs: Vec<(String, crate::manifest::RunManifest)>,
    /// The Success runs under the prefix whose files `cleanup_source` may remove.
    cleanable: Vec<(String, crate::manifest::RunManifest)>,
    /// Whether the target table, if it exists, is one rivet loaded (per the ledger).
    ownership: load::Ownership,
}

/// The prior source identity that CONFLICTS with the one this load carries, or `None` when the warehouse table may accept these rows.
fn conflicting_source_ident<'a>(mine: &str, prior: &'a [String]) -> Option<&'a String> {
    if mine.is_empty() {
        return None;
    }
    prior
        .iter()
        .find(|p| p.as_str() != mine && identity_engine(p) != identity_engine(mine))
        .or_else(|| {
            prior.iter().find(|p| {
                p.as_str() != mine
                    && p.contains(':')
                    && mine.contains(':')
                    && identity_engine(p) == identity_engine(mine)
            })
        })
}

/// The engine half of an `engine[:schema.table]` identity.
fn identity_engine(ident: &str) -> &str {
    ident.split(':').next().unwrap_or(ident)
}

/// What an empty selection means when the prefix holds manifests and not one is Success.
fn no_success_note(
    table: &str,
    prefix: &str,
    manifests_seen: usize,
    success_runs: usize,
) -> Option<String> {
    (manifests_seen > 0 && success_runs == 0).then(|| {
        format!(
            "  load [{table}]: {manifests_seen} manifest(s) under {prefix} and NOT ONE is Success — \
             every run there aborted or is still writing, so nothing has been loaded. \
             This is not 'up to date': re-run the export, then load. A crashed run's \
             parts are deliberately left unread; the retry returns them under a new \
             run id."
        )
    })
}

/// Reconcile the manifests under a load's prefix into its [`LoadInputs`], mode-aware and ledger-filtered.
fn prepare_load(
    store: &crate::destination::gcs::GcsStore,
    plan: &load::plan::LoadPlan,
    state: Option<&StateStore>,
    target_fqtn: &str,
    allow_source_drift: bool,
) -> Result<Option<LoadInputs>> {
    let active_at_fetch = writers_before_fetch(state, &plan.gcs_prefix);
    let (keyed, foreign) = load::reconcile::own_manifests(
        load::reconcile::fetch_manifests_keyed(store, &plan.gcs_prefix)?,
        &plan.gcs_prefix,
        &plan.export_name,
        plan.mode,
    )?;
    if let Some(note) = foreign {
        eprintln!("{note}");
    }
    let marker_active = bucket_writers(state, &keyed);
    let active_at_fetch = active_at_fetch.map(|mut s| {
        s.extend(marker_active.iter().cloned());
        s
    });
    load::reconcile::ensure_single_export(&keyed)?;
    let loaded = match state {
        Some(s) => match s.loaded_source_run_ids(target_fqtn) {
            Ok(set) => set,
            Err(e) if load::reconcile::ledger_read_failure_is_fatal(plan.mode) => {
                crate::rivet_bail!(
                    crate::error::codes::LOAD_LEDGER_UNREADABLE,
                    "`{}`: cannot read which extraction runs are already loaded ({e:#}) — \
                     refusing an append load rather than re-consuming every run the target \
                     may already hold. Fix the state backend and re-run; a `full` load is \
                     unaffected because it overwrites.",
                    plan.table
                );
            }
            Err(e) => {
                log::warn!(
                    "load: cannot read the already-loaded set for {target_fqtn} ({e:#}) — \
                     harmless here because this mode OVERWRITES the latest run and never \
                     consults the set"
                );
                std::collections::HashSet::new()
            }
        },
        None => std::collections::HashSet::new(),
    };
    if let Some(note) = empty_prefix_note(&plan.table, &plan.gcs_prefix, keyed.is_empty(), &loaded)
    {
        eprintln!("{note}");
    }
    let manifests_seen = keyed.len();
    let success_runs = keyed
        .iter()
        .filter(|(_, m)| m.status == crate::manifest::ManifestStatus::Success)
        .count();
    let listed = keyed.clone();
    let new = load::reconcile::select_runs(keyed, &loaded, plan.mode)?;
    if new.is_empty() {
        if let Some(note) =
            no_success_note(&plan.table, &plan.gcs_prefix, manifests_seen, success_runs)
        {
            eprintln!("{note}");
        }
        return Ok(None);
    }
    late_runs_stop(&plan.table, &new, plan.pinned_run.as_ref())?;
    if let Some(s) = state
        && let Some((_, m)) = new.first()
    {
        let mine = crate::manifest::identity_source(m);
        let prior = s.loaded_source_idents(target_fqtn).map_err(|e| {
            e.context(format!(
                "load: the ledger could not say which source last loaded `{target_fqtn}`, so this \
                 load cannot rule out replacing another source's rows — nothing was written; fix \
                 the state backend and re-run"
            ))
        })?;
        if let Some(other) = conflicting_source_ident(&mine, &prior) {
            crate::rivet_bail!(
                crate::error::codes::LOAD_SOURCES_SHARED,
                "{}",
                source_conflict_refusal(target_fqtn, other, &mine)
            );
        }
    }
    let manifests: Vec<_> = new.iter().map(|(_, m)| m.clone()).collect();
    for warning in &plan.rename_warnings {
        eprintln!("{warning}");
    }
    for note in spec_manifest_column_drift(&plan.file_column_names(), &manifests) {
        eprintln!("{note}");
    }
    let integrity = load::reconcile::reconcile(&manifests, allow_source_drift)?;
    let uris = load::reconcile::select_load_uris(store, &plan.gcs_prefix, &new)?;
    let source_run_ids: Vec<String> = new.iter().map(|(_, m)| m.run_id.clone()).collect();
    if nothing_to_load(holds_whole_table_pass(plan, &new), uris.is_empty()) {
        eprintln!(
            "  {} → {}: {} run(s) produced no files — nothing to load",
            plan.table,
            plan.load.target.name(),
            source_run_ids.len()
        );
    }
    let source_ident = new
        .first()
        .map(|(_, m)| crate::manifest::identity_source(m))
        .unwrap_or_default();
    let ownership = ownership_of(state, target_fqtn, "load");
    let cleanable = cleanable_runs(listed, &loaded, &source_run_ids, plan.mode);
    Ok(Some(LoadInputs {
        integrity,
        uris,
        source_run_ids,
        source_ident,
        active_at_fetch,
        marker_active,
        runs: new,
        cleanable,
        ownership,
    }))
}

/// The inputs every load shares; the full/incremental/CDC specifics are the three closures [`execute_load`] takes.
struct LoadJob<'a> {
    plan: &'a load::plan::LoadPlan,
    state: Option<&'a StateStore>,
    load_id: &'a str,
    allow_source_drift: bool,
    mode: load::plan::LoadMode,
    /// The warehouse adapter this load drives.
    loader: Box<dyn load::TargetLoader>,
    /// The object store this load reads its manifests and parts from.
    store: crate::destination::gcs::GcsStore,
    /// Where the load records the jobs it ran and the files and runs it consumed.
    journal: load::result::JobJournal,
}

/// The audit + skip-ledger writer for one load.
struct LoadCtx<'a> {
    state: Option<&'a StateStore>,
    load_id: &'a str,
    export_name: &'a str,
    target_fqtn: &'a str,
    warehouse: &'a str,
    mode: load::plan::LoadMode,
    /// The source prefix this load consumes — needed to ask the ledger which runs are still WRITING into it, so their (still-growing) manifests are not recorded as fully consumed.
    source_prefix: &'a str,
    /// Set once the manifests are known (after `prepare_load`), so the ledger row records WHERE the rows came from and not merely that they arrived.
    source_ident: String,
    /// [`LoadInputs::active_at_fetch`], copied beside `source_ident`.
    active_at_fetch: Option<std::collections::HashSet<String>>,
    /// [`LoadInputs::marker_active`] — the BUCKET's liveness answer, unioned into the record-time sample below.
    marker_active: std::collections::HashSet<String>,
}

/// The source runs this load may record as CONSUMED.
fn consumable_run_ids(
    source_run_ids: &[String],
    active: &std::collections::HashSet<String>,
) -> Vec<String> {
    source_run_ids
        .iter()
        .filter(|id| !active.contains(*id))
        .cloned()
        .collect()
}

/// The Success runs whose files `cleanup_source` may remove.
fn cleanable_runs(
    listed: Vec<(String, crate::manifest::RunManifest)>,
    loaded: &std::collections::HashSet<String>,
    loading: &[String],
    mode: load::plan::LoadMode,
) -> Vec<(String, crate::manifest::RunManifest)> {
    listed
        .into_iter()
        .filter(|(_, m)| m.status == crate::manifest::ManifestStatus::Success)
        .filter(|(_, m)| {
            mode == load::plan::LoadMode::Full
                || loaded.contains(&m.run_id)
                || loading.contains(&m.run_id)
        })
        .collect()
}

/// The operator note for source runs still writing into the prefix.
fn active_run_note(active_runs: usize, prefix: &str) -> Option<String> {
    if active_runs == 0 {
        return None;
    }
    Some(format!(
        "  note: {active_runs} source run(s) in {prefix} cannot be shown finished from this \
         state (still writing, or finished with no run-status row here) — loaded now, kept \
         retryable so their later parts are not skipped, and read again every cycle until a \
         newer run exists; if no extract is running, point the load at the state DB the \
         extract writes"
    ))
}

impl LoadCtx<'_> {
    /// Best-effort ledger write — a state-DB failure warns but never fails a load.
    fn record(&self, source_run_ids: &[String], rows_loaded: i64, status: &str) {
        if let Err(e) = self.try_record(source_run_ids, rows_loaded, status) {
            eprintln!(
                "  warning: load ledger write failed (load itself proceeded): {}",
                crate::redact::redact_secrets(&format!("{e:#}"))
            );
        }
    }

    /// One ledger row; the error is the caller's to judge.
    fn try_record(&self, source_run_ids: &[String], rows_loaded: i64, status: &str) -> Result<()> {
        let Some(s) = self.state else { return Ok(()) };
        let Some(mut active) = writers_at_record(
            self.state,
            self.source_prefix,
            &self.marker_active,
            source_run_ids,
        ) else {
            return Ok(());
        };
        let fetch_sample_missing = self.active_at_fetch.is_none();
        match &self.active_at_fetch {
            Some(at_fetch) => active.extend(at_fetch.iter().cloned()),
            None => active.extend(source_run_ids.iter().cloned()),
        }
        let source_run_ids = consumable_run_ids(source_run_ids, &active);
        if !fetch_sample_missing
            && let Some(note) = active_run_note(active.len(), self.source_prefix)
        {
            eprintln!("{note}");
        }
        let source_run_ids = &source_run_ids[..];
        let rec = LoadRecord {
            source_ident: self.source_ident.clone(),
            load_id: self.load_id.to_string(),
            export_name: self.export_name.to_string(),
            target_table: self.target_fqtn.to_string(),
            warehouse: self.warehouse.to_string(),
            mode: self.mode.ledger_str().to_string(),
            source_run_ids: source_run_ids.to_vec(),
            rows_loaded,
            status: status.to_string(),
            finished_at: chrono::Utc::now().to_rfc3339(),
        };
        s.store_load(&rec)
    }
    /// Nothing new to load — the ledger already covers every run.
    fn record_skip(&self) {
        self.record(&[], 0, LoadStatus::Success.as_str());
    }
    /// Runs that produced no files are consumed as a 0-row success, so no later load re-reads them.
    fn record_empty_runs(&self, run_ids: &[String]) {
        self.record(run_ids, 0, LoadStatus::Success.as_str());
    }

    /// About to touch the warehouse.
    fn record_writing(&self) -> Result<()> {
        self.try_record(&[], 0, LoadStatus::Writing.as_str())
            .context("load: the ledger cannot record that this load is about to write, so it does not write — a table written without that record reads as foreign to the next load")
    }
    /// The load errored after consuming `run_ids`.
    #[cfg(test)]
    fn record_failed(&self, run_ids: &[String]) {
        self.record(run_ids, 0, LoadStatus::Failed.as_str());
    }
    /// The load appended/loaded `rows` from `run_ids`.
    fn record_success(&self, run_ids: &[String], rows: i64) {
        self.record(run_ids, rows, LoadStatus::Success.as_str());
    }
}

/// The "up to date" line of a load with nothing to write.
fn print_up_to_date(job: &LoadJob<'_>) {
    eprintln!("{}", up_to_date_line(job));
}

/// The text of the "up to date" line, pure so its wording is graded.
fn up_to_date_line(job: &LoadJob<'_>) -> String {
    format!(
        "  {} {} → {}: up to date — every extraction run already loaded",
        up_to_date_label(job.mode),
        job.plan.table,
        job.plan.load.target.name(),
    )
}

/// How the "up to date — every extraction run already loaded" line names this load.
fn up_to_date_label(mode: load::plan::LoadMode) -> &'static str {
    match mode {
        load::plan::LoadMode::Cdc => "cdc load",
        load::plan::LoadMode::Full | load::plan::LoadMode::Incremental => "load",
    }
}

/// The checks a plan's staged parts must pass for its warehouse before any write: the partition budget, then the value ranges.
fn parts_fit_target(
    store: &crate::destination::gcs::GcsStore,
    plan: &load::plan::LoadPlan,
    budgeted: &[String],
    uris: &[String],
) -> Result<()> {
    partition_budget_ok(store, plan, budgeted)?;
    load::target_range::refuse_parts_outside_ranges(store, plan, uris)
}

/// The shared load envelope: open the store, build the loader, reconcile via the ledger, then run + record.
fn execute_load<R>(
    job: LoadJob<'_>,
    progress: impl FnOnce(&LoadInputs),
    run: impl FnOnce(
        &dyn load::TargetLoader,
        &crate::destination::gcs::GcsStore,
        &LoadInputs,
        &mut LegLedger<'_>,
    ) -> Result<(u64, R)>,
    done: impl FnOnce(&LoadInputs, &R),
) -> Result<Option<R>> {
    let store = &job.store;
    let loader = &job.loader;
    let target_fqtn = loader.fqtn(&job.plan.table);
    let lease = take_table_lease(job.state, &target_fqtn)?;
    build_missing_base(job.plan, job.state, &**loader)?;
    let mut ctx = LoadCtx {
        state: job.state,
        load_id: job.load_id,
        export_name: job.plan.table.as_str(),
        target_fqtn: target_fqtn.as_str(),
        warehouse: job.plan.load.target.name(),
        mode: job.mode,
        source_prefix: job.plan.gcs_prefix.as_str(),
        source_ident: String::new(),
        active_at_fetch: None,
        marker_active: std::collections::HashSet::new(),
    };
    let prepared = prepare_load(
        store,
        job.plan,
        job.state,
        &target_fqtn,
        job.allow_source_drift,
    );
    let inputs = match seeded!(
        "load-refusal-not-journaled",
        prepared,
        load::before_write(prepared)
    ) {
        Err(e) => {
            seeded!(
                "load-refusal-not-journaled",
                (),
                ctx.record(&[], 0, load::ledger::ledger_status(&e))
            );
            return Err(e);
        }
        Ok(Some(i))
            if nothing_to_load(holds_whole_table_pass(job.plan, &i.runs), i.uris.is_empty()) =>
        {
            ctx.active_at_fetch = i.active_at_fetch.clone();
            ctx.marker_active = i.marker_active.clone();
            print_up_to_date(&job);
            ctx.record_empty_runs(&i.source_run_ids);
            return Ok(None);
        }
        Ok(Some(i)) => {
            ctx.source_ident = i.source_ident.clone();
            ctx.active_at_fetch = i.active_at_fetch.clone();
            ctx.marker_active = i.marker_active.clone();
            i
        }
        Ok(None) => {
            print_up_to_date(&job);
            ctx.record_skip();
            return Ok(None);
        }
    };
    progress(&inputs);
    let mut legs = LegLedger {
        ctx: &ctx,
        consumed: Vec::new(),
    };
    let budgeted = budgeted_uris(job.plan.layout, &inputs.runs, &inputs.uris);
    let appends = job.mode != load::plan::LoadMode::Full;
    let (rows, report) = match load::refuse_view_as_base(
        &**loader,
        &job.plan.table,
        job.plan.layout.log_is_disposable(),
    )
    .and_then(|()| {
        seeded!(
            "load-append-writes-before-asking",
            Ok(()),
            load::before_write(load::ensure_target_own(
                &**loader,
                &job.plan.table,
                appends,
                inputs.ownership,
            ))
        )
    })
    .and_then(|()| load::before_write(parts_fit_target(store, job.plan, &budgeted, &inputs.uris)))
    .and_then(|()| {
        lease_still_held(holds(&lease), &target_fqtn, false)?;
        load::before_write(ctx.record_writing())?;
        let landed = run(&**loader, store, &inputs, &mut legs)?;
        lease_still_held(holds(&lease), &target_fqtn, true)?;
        Ok(landed)
    }) {
        Ok(v) => v,
        Err(e) => {
            let remaining = remaining_run_ids(&inputs.source_run_ids, &legs.consumed);
            ctx.record(&remaining, 0, closing_status(&e, &legs.consumed));
            return Err(e);
        }
    };
    let remaining = remaining_run_ids(&inputs.source_run_ids, &legs.consumed);
    if closing_record_applies(&remaining, &legs.consumed) {
        ctx.record_success(&remaining, rows as i64);
    }
    job.journal
        .consumed(inputs.uris.len(), inputs.source_run_ids.len(), rows);
    done(&inputs, &report);
    Ok(Some(report))
}

/// The base-and-buffer CDC load: the first baseline CREATES the physical base (`<table>`, source columns + `__is_deleted`), the stream's runs APPEND into the per-cycle buffer `<table>__changes` that `rivet compact` merges and drops.
fn load_one_cdc_base(
    job: LoadJob<'_>,
    pk: &[String],
    allow_source_drift: bool,
    state: Option<&StateStore>,
) -> Result<Option<load::CdcLoadReport>> {
    let plan = job.plan;
    execute_load(
        job,
        |inputs| {
            eprintln!(
                "  cdc load {} → {} | layout=base+buffer pk={} manifests={} parquet_files={} rows={}",
                plan.table,
                plan.load.target.name(),
                pk.join(","),
                inputs.integrity.manifests,
                inputs.uris.len(),
                inputs.integrity.file_rows,
            );
        },
        |loader, store, inputs, legs| {
            let (baseline, stream): (Vec<_>, Vec<_>) = inputs
                .runs
                .iter()
                .cloned()
                .partition(|(_, m)| is_baseline_leg(m));
            let mut landed: Vec<u64> = Vec::new();
            let mut report: Option<load::CdcLoadReport> = None;
            let mut baseline_leg: Option<load::LoadReport> = None;
            let (baseline, superseded) =
                load::before_write(load::reconcile::latest_baseline_generation(baseline))?;
            for id in &superseded {
                eprintln!(
                    "  note: baseline run {id} is superseded by a newer baseline — recorded as \
                     loaded, its files are not read"
                );
            }
            let base_kind = match baseline.as_slice() {
                [] => load::ObjectKind::Absent,
                [_, ..] => load::before_write(loader.object_kind(&plan.table))?,
            };
            let rebaseline =
                whole_table_run_joins_the_log(true, base_kind, load::ObjectKind::Absent);
            let mut buffered = stream.clone();
            if rebaseline {
                load::refuse_stale_buffer(loader, &plan.table, true)?;
                let fqtn = loader.fqtn(&plan.table);
                for (_, m) in &baseline {
                    eprintln!("{}", whole_pass_into_buffer_note(&fqtn, &m.run_id));
                }
                buffered = baseline.iter().cloned().chain(stream).collect();
            }
            let stream = buffered;
            let stream_uris = if stream.is_empty() {
                Vec::new()
            } else {
                load::before_write(load::reconcile::select_load_uris(
                    store,
                    &plan.gcs_prefix,
                    &stream,
                ))?
            };
            let stream_uris = buffer_uris(stream_uris);
            if !rebaseline && let [_, ..] = baseline.as_slice() {
                let uris = load::before_write(load::reconcile::select_load_uris(
                    store,
                    &plan.gcs_prefix,
                    &baseline,
                ))?;
                let manifests: Vec<_> = baseline.iter().map(|(_, m)| m.clone()).collect();
                let integrity =
                    load::before_write(load::reconcile::reconcile(&manifests, allow_source_drift))?;
                load::refuse_stale_buffer(loader, &plan.table, true)?;
                let (cleanup, _prefix_lease) = match stream_uris {
                    Some(_) => (None, None),
                    None => cleanup_target_leased(
                        plan,
                        store,
                        state,
                        &inputs.cleanable,
                        still_writing(
                            &inputs.active_at_fetch,
                            &inputs.marker_active,
                            state.is_some(),
                        )
                        .as_ref(),
                    ),
                };
                let r = load::run_load(
                    loader,
                    &plan.table,
                    &base_specs(plan, loader),
                    &uris,
                    Some(integrity.file_rows),
                    cleanup.as_ref().map(|(s, c)| (*s, c)),
                    inputs.ownership,
                )?;
                eprintln!(
                    "  baseline → `{}`: {} rows from {} leg(s) (source columns + `__is_deleted`)",
                    r.target_table,
                    r.rows_loaded,
                    baseline.len()
                );
                let ids: Vec<String> = baseline
                    .iter()
                    .map(|(_, m)| m.run_id.clone())
                    .chain(superseded)
                    .collect();
                legs.landed(&ids, r.rows_loaded);
                landed.push(r.rows_loaded);
                baseline_leg = Some(r);
            }
            if let Some(uris) = stream_uris {
                let manifests: Vec<_> = stream.iter().map(|(_, m)| m.clone()).collect();
                let integrity =
                    load::before_write(load::reconcile::reconcile(&manifests, allow_source_drift))?;
                let ownership = ownership_after_leg(landed.len(), inputs.ownership);
                let (cleanup, _prefix_lease) = cleanup_target_leased(
                    plan,
                    store,
                    state,
                    &inputs.cleanable,
                    still_writing(
                        &inputs.active_at_fetch,
                        &inputs.marker_active,
                        state.is_some(),
                    )
                    .as_ref(),
                );
                let r = load::run_load_buffer(
                    loader,
                    &plan.table,
                    &plan.specs,
                    &uris,
                    pk,
                    Some(integrity.file_rows),
                    cleanup.as_ref().map(|(s, c)| (*s, c)),
                    ownership,
                    "CDC",
                )?;
                landed.push(r.rows_appended);
                report = Some(r);
            }
            if rebaseline {
                flag_what_the_baseline_no_longer_holds(plan, loader, pk)?;
            }
            let mut report = report.unwrap_or_else(|| load::CdcLoadReport {
                rows_appended: 0,
                changes_table: loader.fqtn(&format!("{}__changes", plan.table)),
                target: loader.fqtn(&plan.table),
                target_kind: load::ChangelogTarget::Base,
                source_cleaned: baseline_leg.as_ref().is_some_and(|b| b.source_cleaned),
                baseline: None,
            });
            report.baseline = baseline_leg;
            Ok((landed.iter().sum(), report))
        },
        |inputs, report| eprintln!("{}", cdc_done_line(&inputs.integrity, report)),
    )
}

/// What a CDC load reports for its table (ADR-0039 D3).
fn cdc_work(report: &load::CdcLoadReport) -> Work {
    match seeded!(
        "cdc-load-reports-the-stream-leg-only",
        &None,
        &report.baseline
    ) {
        Some(base) => Work {
            buffer: (report.rows_appended > 0).then(|| report.changes_table.clone()),
            ..Work::loaded(base.rows_loaded + report.rows_appended, &base.target_table)
        },
        None => Work::loaded(report.rows_appended, &report.changes_table),
    }
}

/// Whose the append's names are once this load's own whole-table leg has landed the table.
fn ownership_after_leg(legs_landed: usize, before: load::Ownership) -> load::Ownership {
    match legs_landed {
        0 => before,
        _ => load::Ownership::Own,
    }
}

/// The one-line `CDC LOAD OK` verdict: what landed where and what the operator does next — never a struct dump.
fn cdc_ok_line(report: &load::CdcLoadReport) -> String {
    let leg = report.baseline.as_ref().map_or(String::new(), |b| {
        format!(
            "baseline: {} row(s) landed in `{}`; ",
            b.rows_loaded, b.target_table
        )
    });
    format!("{leg}{}", cdc_stream_line(report))
}

/// The stream leg's part of [`cdc_ok_line`].
fn cdc_stream_line(report: &load::CdcLoadReport) -> String {
    let cleaned = cleaned_suffix(report.source_cleaned);
    match report.target_kind {
        load::ChangelogTarget::Base if report.rows_appended == 0 => format!(
            "no changes buffered by this load into `{}`; `rivet compact` has nothing new for `{}`{cleaned}",
            report.changes_table, report.target
        ),
        load::ChangelogTarget::Base => format!(
            "{} change row(s) buffered into `{}` — `rivet compact` merges them into `{}`{cleaned}",
            report.rows_appended, report.changes_table, report.target
        ),
        load::ChangelogTarget::View => format!(
            "{} row(s) appended to `{}` | current-state view `{}`{cleaned}",
            report.rows_appended, report.changes_table, report.target
        ),
    }
}

/// After a re-baseline joined the buffer: flag each live base row the baseline does not hold as deleted, or say why the base keeps them as they are.
fn flag_what_the_baseline_no_longer_holds(
    plan: &load::plan::LoadPlan,
    loader: &dyn load::TargetLoader,
    pk: &[String],
) -> Result<()> {
    let fqtn = loader.fqtn(&plan.table);
    if !load::plan::base_carries_delete_flag(true, plan.deleted_flag) {
        eprintln!(
            "  note: `{fqtn}` carries no `__is_deleted` flag, so a row the new baseline no \
             longer holds is kept in the base as it is"
        );
        return Ok(());
    }
    let flagged = loader.flag_absent_from_buffer(&plan.table, &plan.specs, pk)?;
    eprintln!(
        "  re-baseline → `{fqtn}`: {flagged} base row(s) the baseline no longer holds are \
         buffered as deleted; `rivet compact` flags them `__is_deleted`, never drops them"
    );
    Ok(())
}

/// The buffer's Parquet, or `None` when the stream's runs produced no parts (an idle drain).
fn buffer_uris(uris: Vec<String>) -> Option<Vec<String>> {
    (!uris.is_empty()).then_some(uris)
}

/// A baseline leg is a BATCH run under the stream's prefix; the stream's own drains are `mode: cdc`.
pub(crate) fn is_baseline_leg(m: &crate::manifest::RunManifest) -> bool {
    m.mode != "cdc"
}

/// The ledger rows of one load, written per LEG.
struct LegLedger<'a> {
    ctx: &'a LoadCtx<'a>,
    consumed: Vec<String>,
}

impl LegLedger<'_> {
    /// Record `run_ids` as loaded with `rows`, ahead of the load's closing record.
    fn landed(&mut self, run_ids: &[String], rows: u64) {
        self.ctx.record_success(run_ids, rows as i64);
        self.consumed.extend(run_ids.iter().cloned());
    }
}

/// Whether the load's closing ledger row is written.
fn closing_record_applies(remaining: &[String], consumed: &[String]) -> bool {
    !remaining.is_empty() || consumed.is_empty()
}

/// The run ids a load's closing record still covers: every selected run a leg has not already recorded.
fn remaining_run_ids(all: &[String], consumed: &[String]) -> Vec<String> {
    all.iter()
        .filter(|id| !consumed.contains(id))
        .cloned()
        .collect()
}

/// The `(source cleaned)` suffix: a load that deleted its staged Parquet says so.
fn cleaned_suffix(source_cleaned: bool) -> &'static str {
    if source_cleaned {
        " (source cleaned)"
    } else {
        ""
    }
}

/// The success trace shared by the CDC + incremental loads.
fn cdc_done_line(
    integrity: &load::reconcile::LoadIntegrity,
    report: &load::CdcLoadReport,
) -> String {
    let cleaned = cleaned_suffix(report.source_cleaned);
    match report.target_kind {
        load::ChangelogTarget::View => format!(
            "  integrity ✓ {} → appended {} to {} | current-state view {}{cleaned}",
            integrity.chain_prefix(),
            report.rows_appended,
            report.changes_table,
            report.target,
        ),
        load::ChangelogTarget::Base => format!(
            "  integrity ✓ {} → base {} | buffered {} row(s) into {}{cleaned}",
            integrity.chain_prefix(),
            report.target,
            report.rows_appended,
            report.changes_table,
        ),
    }
}

/// The full-load sibling of [`cdc_done_line`] — the whole chain, now that the warehouse leg is known.
fn full_done_line(integrity: &load::reconcile::LoadIntegrity, report: &load::LoadReport) -> String {
    format!(
        "  integrity ✓ {} → warehouse {} rows in {}{}",
        integrity.chain_prefix(),
        report.rows_loaded,
        report.target_table,
        cleaned_suffix(report.source_cleaned),
    )
}

/// Load a single export's CDC change log: reconcile the run manifests, **append** the change Parquet into `<table>__changes`, and rebuild the current-state dedup view over it.
fn spec_manifest_column_drift(
    spec_columns: &[String],
    manifests: &[crate::manifest::RunManifest],
) -> Vec<String> {
    use std::collections::BTreeSet;
    let specs: BTreeSet<&str> = spec_columns.iter().map(|s| s.as_str()).collect();
    let mut notes = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for m in manifests {
        let Some(cols) = m.column_checksums.as_deref() else {
            continue;
        };
        for c in cols {
            if !specs.contains(c.name.as_str()) && seen.insert(c.name.clone()) {
                notes.push(format!(
                    "  WARNING: staged parquet carries column `{}` (recorded by run {}), \
                     but the spec this load is typed from (the newest run's recorded \
                     columns) lacks it — the load projects only those columns, so this \
                     column's data will be SILENTLY omitted from the warehouse. \
                     Re-extract after aligning the schema, or add the column back.",
                    c.name, m.run_id
                ));
            }
        }
    }
    notes
}

/// A CDC load carrying a snapshot leg over a `<table>` an earlier full load left: two baselines.
fn snapshot_over_full_table(snapshot_leg: bool, table: load::ObjectKind) -> bool {
    snapshot_leg && table == load::ObjectKind::Table
}

fn rebaseline_shape(uris: &[String], plan_prefix: &str) -> bool {
    let base = plan_prefix.trim_end_matches('/');
    uris.iter().any(|u| {
        u.strip_prefix(base)
            .map(|rest| rest.trim_start_matches('/'))
            .is_some_and(|rest| rest.starts_with("snapshot/"))
    })
}

/// What the re-baseline guard does, given the two signals it can read.
#[derive(Debug, PartialEq, Eq)]
enum RebaselineAction {
    Proceed,
    WarnStateless,
    Refuse,
}

/// `ledger` is a TRI-STATE (round-9): `state.is_some()` alone conflated a DB BLIP with absent-by-design.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LedgerSignal {
    Available,
    AbsentByDesign,
    Errored,
}

impl LedgerSignal {
    /// The round-9 tri-state decision, NAMED (arch critic.
    fn classify(errored: bool, present: bool) -> Self {
        if errored {
            LedgerSignal::Errored
        } else if present {
            LedgerSignal::Available
        } else {
            LedgerSignal::AbsentByDesign
        }
    }
}

fn rebaseline_action(warehouse_has_changes: bool, ledger: LedgerSignal) -> RebaselineAction {
    match (warehouse_has_changes, ledger) {
        (false, _) => RebaselineAction::Proceed,
        (true, LedgerSignal::Available) => RebaselineAction::Refuse,
        (true, LedgerSignal::Errored) => RebaselineAction::Refuse,
        (true, LedgerSignal::AbsentByDesign) => RebaselineAction::WarnStateless,
    }
}

/// The RE-baseline REFUSAL (round-7 refuter, HIGH).
fn rebaseline_refusal(target_fqtn: &str, warehouse: crate::load::cdc::Warehouse) -> String {
    let quoted = warehouse.quote_fqtn(&format!("{target_fqtn}__changes"));
    rebaseline_refusal_text(&quoted)
}

/// The re-baseline refusal as the stop a CDC load raises before its append.
fn rebaseline_stop(target_fqtn: &str, warehouse: crate::load::cdc::Warehouse) -> anyhow::Error {
    load::refused(
        crate::error::codes::LOAD_REBASELINE_OVER_CHANGES,
        rebaseline_refusal(target_fqtn, warehouse),
    )
}

/// The snapshot-over-a-full-table refusal as the stop a CDC load raises before its append.
fn snapshot_over_full_table_stop(fqtn: &str, warehouse: load::cdc::Warehouse) -> anyhow::Error {
    load::refused(
        crate::error::codes::LOAD_TARGET_OBJECT_CONFLICT,
        snapshot_over_full_table_refusal(fqtn, warehouse),
    )
}

fn rebaseline_refusal_text(quoted_changes: &str) -> String {
    format!(
        "refusing to append a RE-baseline: this load carries snapshot parquet, but \
         {quoted_changes} already holds real change rows — a re-baseline row \
         carries NULL `__pos` and LOSES the dedup to every prior change, so the \
         current-state view would keep serving PRE-GAP values for exactly the rows \
         this re-baseline fixed — and even an anchor-stamped baseline cannot \
         express a PK DELETED during the gap (no row, no tombstone: its stale \
         pre-gap rows would win). Nothing was consumed by this refusal. Recovery: \
         1) TRUNCATE TABLE {quoted_changes}; 2) re-run this same `rivet load` \
         — it will then append the baseline into the empty log and proceed \
         (cdc-failure-modes.md)."
    )
}

#[allow(clippy::too_many_arguments)]
fn load_one_cdc(
    plan: &load::plan::LoadPlan,
    run_id: &str,
    engine: load::cdc::SourceEngine,
    pk: &[String],
    allow_source_drift: bool,
    rebuild_changelog: bool,
    state: Option<&StateStore>,
    ledger_errored: bool,
    load_id: &str,
    journal: &load::result::JobJournal,
) -> Result<Option<load::CdcLoadReport>> {
    let job = LoadJob {
        plan,
        state,
        load_id,
        allow_source_drift,
        mode: plan.mode,
        loader: load::build_loader(plan, run_id, journal),
        store: load::open_store(&plan.destination)?,
        journal: journal.clone(),
    };
    if plan.layout.compacts() {
        return load_one_cdc_base(job, pk, allow_source_drift, state);
    }
    execute_load(
        job,
        |inputs| {
            eprintln!(
                "  cdc load {} → {} | engine={:?} pk={} manifests={} parquet_files={} expected_delta={}",
                plan.table,
                plan.load.target.name(),
                engine,
                pk.join(","),
                inputs.integrity.manifests,
                inputs.uris.len(),
                inputs.integrity.file_rows,
            );
        },
        |loader, store, inputs, _legs| {
            let shape = rebaseline_shape(&inputs.uris, &plan.gcs_prefix);
            let kind = load::before_write(loader.object_kind(&plan.table))?;
            if snapshot_over_full_table(shape, kind) {
                return Err(snapshot_over_full_table_stop(
                    &loader.fqtn(&plan.table),
                    loader.warehouse(),
                ));
            }
            if shape {
                let ledger = LedgerSignal::classify(ledger_errored, state.is_some());
                let prior = match ledger {
                    LedgerSignal::AbsentByDesign => true,
                    _ => load::before_write(loader.changes_has_prior_changes(&plan.table))?,
                };
                match rebaseline_action(prior, ledger) {
                    RebaselineAction::Refuse => {
                        return Err(rebaseline_stop(
                            &loader.fqtn(&plan.table),
                            loader.warehouse(),
                        ));
                    }
                    RebaselineAction::WarnStateless => eprintln!(
                        "  note: this STATELESS load re-selects the snapshot leg every \
                         cycle (at-least-once, absorbed by the dedup view) — the \
                         re-baseline refusal needs the ledger to tell a genuine \
                         re-baseline from a routine re-load, so it does not apply here.",
                    ),
                    RebaselineAction::Proceed => {}
                }
            }
            let (cleanup, _prefix_lease) = cleanup_target_leased(
                plan,
                store,
                state,
                &inputs.cleanable,
                still_writing(
                    &inputs.active_at_fetch,
                    &inputs.marker_active,
                    state.is_some(),
                )
                .as_ref(),
            );
            let report = load::run_load_cdc(
                loader,
                &plan.table,
                &plan.specs,
                &inputs.uris,
                pk,
                engine,
                Some(inputs.integrity.file_rows),
                cleanup.as_ref().map(|(s, c)| (*s, c)),
                inputs.ownership,
                rebuild_changelog,
            )?;
            Ok((report.rows_appended, report))
        },
        |inputs, report| eprintln!("{}", cdc_done_line(&inputs.integrity, report)),
    )
}

/// What an incremental load did: landed a whole-table run as `<table>`, or appended deltas to `<table>__changes` behind the view.
#[derive(Debug)]
pub enum IncrementalReport {
    Table(load::LoadReport),
    Changelog(load::CdcLoadReport),
}

impl IncrementalReport {
    /// The rows this load landed.
    pub fn rows(&self) -> u64 {
        match self {
            IncrementalReport::Table(r) => r.rows_loaded,
            IncrementalReport::Changelog(r) => r.rows_appended,
        }
    }

    /// The object this load wrote, as its loader named it.
    pub fn wrote(&self) -> &str {
        match self {
            IncrementalReport::Table(r) => &r.target_table,
            IncrementalReport::Changelog(r) => &r.changes_table,
        }
    }

    /// One line saying what landed where.
    pub fn summary(&self) -> String {
        match self {
            IncrementalReport::Table(r) => format!(
                "{} rows landed as table {}{}",
                r.rows_loaded,
                r.target_table,
                cleaned_suffix(r.source_cleaned)
            ),
            IncrementalReport::Changelog(r) => match r.target_kind {
                load::ChangelogTarget::View => format!(
                    "{} rows appended to {} | current-state view {}{}",
                    r.rows_appended,
                    r.changes_table,
                    r.target,
                    cleaned_suffix(r.source_cleaned)
                ),
                load::ChangelogTarget::Base => format!(
                    "{} rows buffered into {} | `rivet compact` merges them into {}{}",
                    r.rows_appended,
                    r.changes_table,
                    r.target,
                    cleaned_suffix(r.source_cleaned)
                ),
            },
        }
    }
}

/// Whether an incremental run's manifest says it re-read the whole table.
fn is_first_pass(m: &crate::manifest::RunManifest) -> bool {
    m.source
        .extraction
        .as_ref()
        .is_some_and(|e| e.cursor_column.is_some() && e.cursor_low.is_none())
}

/// The pending incremental runs, split: the latest whole-table run (if any) lands as the table, runs started after it are deltas, and runs it supersedes are only recorded.
struct SplitRuns {
    first_pass: Option<(String, crate::manifest::RunManifest)>,
    deltas: Vec<(String, crate::manifest::RunManifest)>,
    superseded: Vec<String>,
}

impl SplitRuns {
    fn has_deltas(&self) -> bool {
        !self.deltas.is_empty()
    }

    /// The whole-table run joins the deltas: appended into the change log (at least once.
    fn whole_table_run_joins_the_log(&mut self) {
        if let Some(first) = self.first_pass.take() {
            self.deltas.push(first);
            self.deltas
                .sort_by(|a, b| a.1.started_at.cmp(&b.1.started_at));
        }
    }
}

/// Whether a whole-table run joins the change log rather than landing as `<table>`.
fn whole_table_run_joins_the_log(
    base_and_buffer: bool,
    kind: load::ObjectKind,
    changes: load::ObjectKind,
) -> bool {
    match kind {
        load::ObjectKind::Table | load::ObjectKind::View => {
            seeded!("whole-pass-replaces-the-base", !base_and_buffer, true)
        }
        load::ObjectKind::Absent => !base_and_buffer && changes == load::ObjectKind::Table,
        load::ObjectKind::Other => false,
    }
}

/// Why a whole-table pass is appended to the buffer instead of replacing the base.
fn whole_pass_into_buffer_note(fqtn: &str, run_id: &str) -> String {
    format!(
        "  note: `{fqtn}` already holds the base, and run {run_id} re-read the whole table — it \
         is appended to the buffer `{fqtn}__changes` for `rivet compact` to merge by key, \
         never replacing the base: a row the pass no longer holds stays in it"
    )
}

/// Why a whole-table run is appended to the change log instead of landing as `<table>`.
fn whole_table_run_note(kind: load::ObjectKind, fqtn: &str, run_id: &str) -> String {
    match kind {
        load::ObjectKind::View => format!(
            "  note: `{fqtn}` is already the current-state view over its change log — run \
             {run_id} re-read the whole table, so it is appended to the log (at least once; the \
             view keeps the latest row per key) instead of replacing it"
        ),
        load::ObjectKind::Absent => format!(
            "  note: `{fqtn}` is missing but its change log `{fqtn}__changes` exists — a load \
             that turned the table into that log stopped before building the view — so run \
             {run_id}'s whole pass is appended to the log and the view is built"
        ),
        _ => format!(
            "  note: `{fqtn}` already holds rows from an earlier load, and an incremental load \
             never overwrites a table that exists — it becomes the change log `{fqtn}__changes` \
             and run {run_id}'s whole pass is appended to it (at least once; the view keeps the \
             latest row per key), so rows the source has since dropped stay in the log"
        ),
    }
}

fn split_runs(runs: &[(String, crate::manifest::RunManifest)]) -> SplitRuns {
    let first_pass = runs
        .iter()
        .filter(|(_, m)| is_first_pass(m))
        .max_by(|a, b| a.1.started_at.cmp(&b.1.started_at))
        .cloned();
    let mut out = SplitRuns {
        deltas: Vec::new(),
        superseded: Vec::new(),
        first_pass,
    };
    for (key, m) in runs {
        match &out.first_pass {
            Some((_, f)) if f.run_id == m.run_id => {}
            Some((_, f)) if m.started_at <= f.started_at => out.superseded.push(m.run_id.clone()),
            _ => out.deltas.push((key.clone(), m.clone())),
        }
    }
    out.deltas
        .sort_by(|a, b| a.1.started_at.cmp(&b.1.started_at));
    out
}

/// Why a continued-key export's table, last loaded (newest-first `loads`) as an overwrite, may lack rows.
fn overwritten_delta_warning(
    continued_key: bool,
    loads: &[crate::state::LoadRecord],
    fqtn: &str,
    export: &str,
    config: &str,
) -> Option<String> {
    let last = loads
        .iter()
        .find(|r| r.status == LoadStatus::Success.as_str());
    let overwrote = last.is_some_and(|r| r.mode == load::plan::LoadMode::Full.ledger_str());
    (continued_key && overwrote).then(|| {
        format!(
            "`{fqtn}` was last loaded as a whole-table overwrite, and export `{export}` now \
             loads by append because each run carries only the keys past the last one \
             (`keyset_incremental` / `source.mongo.resume`). rivet 0.31 and older \
             (0.30 and older for `keyset_incremental`) loaded such \
             an export by overwriting the table with each run's new keys, so it may lack rows \
             earlier runs delivered. To restore it: `rivet state reset -c {config} --export \
             {export}`, then `rivet run -c {config}` and `rivet load -c {config}`."
        )
    })
}

/// Load a single export's INCREMENTAL runs.
#[allow(clippy::too_many_arguments)]
fn load_one_incremental(
    plan: &load::plan::LoadPlan,
    run_id: &str,
    pk: &[String],
    allow_source_drift: bool,
    rebuild_changelog: bool,
    state: Option<&StateStore>,
    load_id: &str,
    config: &str,
    journal: &load::result::JobJournal,
) -> Result<Option<IncrementalReport>> {
    let cursor = plan.cursor_column.clone().ok_or_else(|| {
        crate::error::coded(
            crate::error::codes::CONFIG_MODE_SETTING_INVALID,
            format!(
                "incremental load of `{}` needs the export's `cursor_column:` — the current-state \
             view's latest-per-PK ordering key",
                plan.table
            ),
        )
    })?;
    let job = LoadJob {
        plan,
        state,
        load_id,
        allow_source_drift,
        mode: plan.mode,
        loader: load::build_loader(plan, run_id, journal),
        store: load::open_store(&plan.destination)?,
        journal: journal.clone(),
    };
    let fqtn = job.loader.fqtn(&plan.table);
    let loads = match state.map(|s| s.recent_loads(Some(&fqtn), 50)).transpose() {
        Ok(rows) => rows.unwrap_or_default(),
        Err(e) => {
            log::warn!(
                "could not read the load ledger for `{fqtn}`, so this load cannot tell whether an \
                 earlier load overwrote it with only one run's keys: {e:#}"
            );
            Vec::new()
        }
    };
    if let Some(warning) =
        overwritten_delta_warning(plan.continued_key, &loads, &fqtn, &plan.export_name, config)
    {
        log::warn!("{warning}");
    }
    load_incremental_job(
        job,
        pk,
        &cursor,
        allow_source_drift,
        rebuild_changelog,
        state,
    )
}

/// The incremental load of one job: a whole-table run lands (or joins the log), deltas append.
fn load_incremental_job(
    job: LoadJob<'_>,
    pk: &[String],
    cursor: &str,
    allow_source_drift: bool,
    rebuild_changelog: bool,
    state: Option<&StateStore>,
) -> Result<Option<IncrementalReport>> {
    let plan = job.plan;
    execute_load(
        job,
        |inputs| {
            eprintln!(
                "  incremental load {} → {} | pk={} cursor={} manifests={} parquet_files={} expected_delta={}",
                plan.table,
                plan.load.target.name(),
                pk.join(","),
                cursor,
                inputs.integrity.manifests,
                inputs.uris.len(),
                inputs.integrity.file_rows,
            );
        },
        |loader, store, inputs, legs| {
            let base_and_buffer = plan.layout.log_is_disposable();
            let mut split = split_runs(&inputs.runs);
            if let Some((_, first)) = split.first_pass.as_ref() {
                let kind = load::before_write(loader.object_kind(&plan.table))?;
                let changes =
                    load::before_write(loader.object_kind(&format!("{}__changes", plan.table)))?;
                if whole_table_run_joins_the_log(base_and_buffer, kind, changes) {
                    load::refuse_stale_buffer(loader, &plan.table, base_and_buffer)?;
                    let fqtn = loader.fqtn(&plan.table);
                    let note = match base_and_buffer {
                        true => whole_pass_into_buffer_note(&fqtn, &first.run_id),
                        false => whole_table_run_note(kind, &fqtn, &first.run_id),
                    };
                    eprintln!("{note}");
                    split.whole_table_run_joins_the_log();
                }
            }
            for id in &split.superseded {
                eprintln!(
                    "  note: run {id} started before the latest whole-table run and is superseded \
                     by it — recorded as loaded, its files are not read"
                );
            }
            let mut rows = 0u64;
            let mut report = None;
            let landed_table = split.first_pass.is_some();
            let delta_uris = match split.has_deltas() {
                true => buffer_uris(load::before_write(load::reconcile::select_load_uris(
                    store,
                    &plan.gcs_prefix,
                    &split.deltas,
                ))?),
                false => None,
            };
            let has_deltas = delta_uris.is_some();
            if let Some(first) = split.first_pass {
                let mut landed_ids = split.superseded.clone();
                landed_ids.push(first.1.run_id.clone());
                let uris = load::before_write(load::reconcile::select_load_uris(
                    store,
                    &plan.gcs_prefix,
                    std::slice::from_ref(&first),
                ))?;
                let integrity = load::before_write(load::reconcile::reconcile(
                    std::slice::from_ref(&first.1),
                    allow_source_drift,
                ))?;
                eprintln!(
                    "  incremental load {}: run {} holds the whole table (no cursor to resume \
                     from) — landing it as `{}`",
                    plan.table,
                    first.1.run_id,
                    loader.fqtn(&plan.table)
                );
                let (cleanup, _prefix_lease) = if has_deltas {
                    (None, None)
                } else {
                    cleanup_target_leased(
                        plan,
                        store,
                        state,
                        &inputs.cleanable,
                        still_writing(
                            &inputs.active_at_fetch,
                            &inputs.marker_active,
                            state.is_some(),
                        )
                        .as_ref(),
                    )
                };
                load::refuse_stale_buffer(loader, &plan.table, base_and_buffer)?;
                let mut base_specs = plan.specs.clone();
                if load::plan::base_carries_delete_flag(base_and_buffer, plan.deleted_flag) {
                    base_specs.push(load::cdc::flag_spec(loader.warehouse()));
                }
                let r = load::run_load(
                    loader,
                    &plan.table,
                    &base_specs,
                    &uris,
                    Some(integrity.file_rows),
                    cleanup.as_ref().map(|(s, c)| (*s, c)),
                    inputs.ownership,
                )?;
                eprintln!("{}", full_done_line(&integrity, &r));
                legs.landed(&landed_ids, r.rows_loaded);
                report = Some(IncrementalReport::Table(r));
            }
            if let Some(uris) = delta_uris {
                let manifests: Vec<_> = split.deltas.iter().map(|(_, m)| m.clone()).collect();
                let integrity =
                    load::before_write(load::reconcile::reconcile(&manifests, allow_source_drift))?;
                let ownership = ownership_after_leg(usize::from(landed_table), inputs.ownership);
                let (cleanup, _prefix_lease) = cleanup_target_leased(
                    plan,
                    store,
                    state,
                    &inputs.cleanable,
                    still_writing(
                        &inputs.active_at_fetch,
                        &inputs.marker_active,
                        state.is_some(),
                    )
                    .as_ref(),
                );
                let r = if base_and_buffer {
                    load::run_load_buffer(
                        loader,
                        &plan.table,
                        &plan.specs,
                        &uris,
                        pk,
                        Some(integrity.file_rows),
                        cleanup.as_ref().map(|(s, c)| (*s, c)),
                        ownership,
                        "incremental",
                    )?
                } else {
                    load::run_load_incremental(
                        loader,
                        &plan.table,
                        &plan.specs,
                        &uris,
                        pk,
                        cursor,
                        Some(integrity.file_rows),
                        cleanup.as_ref().map(|(s, c)| (*s, c)),
                        ownership,
                        rebuild_changelog,
                    )?
                };
                eprintln!("{}", cdc_done_line(&integrity, &r));
                rows += r.rows_appended;
                report = Some(IncrementalReport::Changelog(r));
            }
            let report = report.ok_or_else(|| {
                load::refused(
                    crate::error::codes::LOAD_RUN_NOT_LOADABLE,
                    "no loadable run among the selected manifests".to_string(),
                )
            })?;
            Ok((rows, report))
        },
        |_, _| {},
    )
}

/// Load a single resolved table into its warehouse target, reconciling **source → file → warehouse** row counts end-to-end.
fn load_one(
    plan: &load::plan::LoadPlan,
    run_id: &str,
    allow_source_drift: bool,
    state: Option<&StateStore>,
    load_id: &str,
    journal: &load::result::JobJournal,
) -> Result<Option<load::LoadReport>> {
    let job = LoadJob {
        plan,
        state,
        load_id,
        allow_source_drift,
        mode: plan.mode,
        loader: load::build_loader(plan, run_id, journal),
        store: load::open_store(&plan.destination)?,
        journal: journal.clone(),
    };
    execute_load(
        job,
        |inputs| {
            eprintln!(
                "  load {} → {} | columns={} partition={} manifests={} parquet_files={} expected_rows={}",
                plan.table,
                plan.load.target.name(),
                plan.specs.len(),
                partition_label(plan),
                inputs.integrity.manifests,
                inputs.uris.len(),
                inputs.integrity.file_rows,
            );
        },
        |loader, store, inputs, _legs| full_leg(plan, state, loader, store, inputs),
        |inputs, report| eprintln!("{}", full_done_line(&inputs.integrity, report)),
    )
}

/// The full-mode load leg: land the latest snapshot, then clean the staged prefix under its lease.
fn full_leg(
    plan: &load::plan::LoadPlan,
    state: Option<&StateStore>,
    loader: &dyn load::TargetLoader,
    store: &crate::destination::gcs::GcsStore,
    inputs: &LoadInputs,
) -> Result<(u64, load::LoadReport)> {
    let (cleanup, _prefix_lease) = cleanup_target_leased(
        plan,
        store,
        state,
        &inputs.cleanable,
        still_writing(
            &inputs.active_at_fetch,
            &inputs.marker_active,
            state.is_some(),
        )
        .as_ref(),
    );
    let report = load::run_load(
        loader,
        &plan.table,
        &plan.specs,
        &inputs.uris,
        Some(inputs.integrity.file_rows),
        cleanup.as_ref().map(|(s, c)| (*s, c)),
        inputs.ownership,
    )?;
    Ok((report.rows_loaded, report))
}

/// The correlation run-id for a load: the explicit `--run-id` / `RIVET_RUN_ID` if it carries a non-blank value, else a generated one.
pub(super) fn resolve_run_id(explicit: Option<String>) -> String {
    explicit
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(generate_run_id)
}

/// A per-invocation load-run id: microsecond-since-epoch hex + zero-padded pid hex.
fn generate_run_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    format!("{micros:x}{:08x}", std::process::id())
}

#[cfg(test)]
mod load_ledger_tests {
    use super::*;

    #[test]
    fn the_shared_pool_helpers_keep_state_leases_and_failures() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("rivet.yaml");
        let state = open_state(cfg.to_str().unwrap(), "stateless").expect("a writable dir opens");

        let lease = take_table_lease(Some(&state), "p.d.orders").unwrap();
        assert!(lease.is_some(), "the first taker holds the table");
        let Err(busy) = take_table_lease(Some(&state), "p.d.orders") else {
            panic!("a held table must refuse a second taker");
        };
        assert!(format!("{busy:#}").contains("p.d.orders"), "{busy:#}");
        drop(lease);
        assert!(take_table_lease(None, "p.d.orders").unwrap().is_none());

        let (state_ref, had) = hand_off_state(Some(state), None, 1);
        assert!(had && state_ref.is_some());
        assert!(matches!(
            reconnect(state_ref.as_ref(), "drain"),
            Some(Some(_))
        ));
        assert!(matches!(reconnect(None, "drain"), Some(None)));
        let (none_ref, none_had) = hand_off_state(None, None, 1);
        assert!(!none_had && none_ref.is_none());

        let failed = failures_of(vec![
            Ok(()),
            Err(anyhow::anyhow!("a")),
            Ok(()),
            Err(anyhow::anyhow!("b")),
        ]);
        assert_eq!(
            failed.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[test]
    fn resolve_run_id_treats_blank_as_absent() {
        assert_eq!(resolve_run_id(Some("abc".into())), "abc");
        for blank in [Some(String::new()), Some("   ".into())] {
            let id = resolve_run_id(blank.clone());
            assert!(
                !id.trim().is_empty(),
                "blank {blank:?} must yield a generated id, got {id:?}"
            );
        }
        assert!(!resolve_run_id(None).is_empty());
    }

    #[test]
    fn require_pk_error_names_the_export_not_the_table() {
        use load::plan::{CdcLayout, LoadMode, LoadPlan, LoadSection, LoadTarget};
        let plan = LoadPlan {
            deleted_flag: false,
            renames: Vec::new(),
            rename_warnings: Vec::new(),
            refusal: None,
            landings: Vec::new(),
            export_name: "c1".into(),
            unit: None,
            table: "content_items".into(),
            partition: None,
            specs: vec![],
            gcs_prefix: String::new(),
            destination: crate::config::DestinationConfig::default(),
            load: LoadSection {
                deleted_flag: None,
                layout: None,
                target: LoadTarget::Bigquery {
                    project: "p".into(),
                    dataset: "d".into(),
                },
                cleanup_source: false,
                pk: load::plan::KeyColumns::Auto,
                allow_source_drift: false,
                gc_orphans: false,
                cluster_by: load::plan::KeyColumns::Auto,
                partition: None,
            },
            mode: LoadMode::Cdc,
            cursor_column: None,
            continued_key: false,
            pk: vec![],
            clustering: load::plan::Clustering::Auto(vec![]),
            pinned_run: None,
            layout: CdcLayout::LogAndView,
            source_key: String::new(),
        };
        let err = require_pk(&plan, "cdc").unwrap_err().to_string();
        assert!(err.contains("export `c1`"), "must name the export: {err}");
        assert!(
            !err.contains("content_items"),
            "must NOT label the table as the export: {err}"
        );

        for mode in [LoadMode::Cdc, LoadMode::Incremental] {
            let err = require_pk(&plan, mode.ledger_str())
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(&format!("is mode: {}", mode.ledger_str())),
                "the refusal must name the export's OWN mode, not a fixed label: {err}"
            );
        }

        let mut keyless = plan.clone();
        keyless.continued_key = true;
        keyless.load.pk = load::plan::KeyColumns::None;
        assert_eq!(
            require_pk(&keyless, "incremental").unwrap_err().to_string(),
            "export `c1` loads by append (each run carries only the keys past the last one) but \
             has no primary key for the current-state dedup view — its `load:` block sets \
             `pk: none`, so name the key there instead (e.g. `pk: [id]`)"
        );
    }

    /// Round-7 rebuild of the round-6 guard: the SHAPE is prefix-anchored (a `snapshot` segment in the operator's own prefix, or a multiplex table named `snapshot`, must not read as a re-baseline), and the REFUSAL message prescribes the truncate-then-rerun sequence that the warehouse-probed condition makes safe.
    #[test]
    fn rebaseline_shape_is_prefix_anchored_and_the_refusal_names_the_sequence() {
        let plan_prefix = "gs://b/exports/orders";
        assert!(rebaseline_shape(
            &["gs://b/exports/orders/snapshot/part-0.parquet".into()],
            plan_prefix
        ));
        assert!(!rebaseline_shape(
            &["gs://b/exports/orders/cdc-000000.parquet".into()],
            plan_prefix
        ));
        assert!(!rebaseline_shape(
            &["gs://b/analytics/snapshot/orders/cdc-000000.parquet".into()],
            "gs://b/analytics/snapshot/orders"
        ));
        assert!(!rebaseline_shape(
            &["gs://b/exports/base/snapshot/cdc-000000.parquet".into()],
            "gs://b/exports/base/snapshot"
        ));
        use LedgerSignal::*;
        use RebaselineAction::*;
        assert_eq!(LedgerSignal::classify(true, true), Errored, "errored wins");
        assert_eq!(LedgerSignal::classify(false, true), Available);
        assert_eq!(LedgerSignal::classify(false, false), AbsentByDesign);
        assert_eq!(rebaseline_action(true, Available), Refuse);
        assert_eq!(
            rebaseline_action(true, Errored),
            Refuse,
            "a ledger BLIP must fail safe — note-and-proceed appended a doomed \
             baseline and cleanup then deleted the evidence (round-9)"
        );
        assert_eq!(rebaseline_action(true, AbsentByDesign), WarnStateless);
        for l in [Available, Errored, AbsentByDesign] {
            assert_eq!(rebaseline_action(false, l), Proceed);
        }
        let msg = rebaseline_refusal("p.d.t", crate::load::cdc::Warehouse::BigQuery);
        assert!(msg.contains("TRUNCATE TABLE `p.d.t__changes`"));
        let sf = rebaseline_refusal("d.s.t", crate::load::cdc::Warehouse::Snowflake);
        assert!(
            sf.contains("TRUNCATE TABLE d.s.t__changes;") && !sf.contains("TRUNCATE TABLE `"),
            "the pasted remedy must parse on Snowflake (bare fqtn in the STATEMENT): {sf}"
        );
        assert!(msg.contains("re-run this same"));
        assert!(msg.contains("Nothing was consumed"));
    }

    const TARGET: &str = "proj.ds.orders";

    fn ctx<'a>(state: &'a StateStore, load_id: &'a str) -> LoadCtx<'a> {
        LoadCtx {
            source_ident: String::new(),
            active_at_fetch: Some(Default::default()),
            marker_active: Default::default(),
            source_prefix: "gs://b/p/",
            state: Some(state),
            load_id,
            export_name: "orders",
            target_fqtn: TARGET,
            warehouse: "bigquery",
            mode: load::plan::LoadMode::Cdc,
        }
    }

    #[test]
    fn record_success_logs_the_load_and_marks_its_runs_loaded() {
        let s = StateStore::open_in_memory().unwrap();
        ctx(&s, "L1").record_success(&["r1".into(), "r2".into()], 5);
        let loads = s.recent_loads(Some(TARGET), 10).unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(loads[0].status, "success");
        assert_eq!(loads[0].rows_loaded, 5);
        let loaded = s.loaded_source_run_ids(TARGET).unwrap();
        assert!(
            loaded.contains("r1") && loaded.contains("r2"),
            "a successful load marks its runs so the next load skips them"
        );
    }

    #[test]
    fn record_success_marks_its_run_even_at_zero_rows() {
        let s = StateStore::open_in_memory().unwrap();
        ctx(&s, "L1").record_success(&["r_empty".into()], 0);
        let loads = s.recent_loads(Some(TARGET), 10).unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(loads[0].status, "success");
        assert_eq!(loads[0].rows_loaded, 0);
        assert!(
            s.loaded_source_run_ids(TARGET).unwrap().contains("r_empty"),
            "a 0-row successful load still marks its run — not re-processed forever"
        );
    }

    #[test]
    fn record_skip_logs_a_zero_row_success_and_marks_nothing() {
        let s = StateStore::open_in_memory().unwrap();
        ctx(&s, "L1").record_skip();
        let loads = s.recent_loads(Some(TARGET), 10).unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(loads[0].status, "success");
        assert_eq!(loads[0].rows_loaded, 0);
        assert!(
            s.loaded_source_run_ids(TARGET).unwrap().is_empty(),
            "an up-to-date no-op consumes no runs"
        );
    }

    #[test]
    fn a_same_load_id_skip_or_refusal_does_not_disown_the_table() {
        for status in ["skip", "refused"] {
            let s = StateStore::open_in_memory().unwrap();
            ctx(&s, "L1").record_success(&["r1".into()], 5);
            assert!(s.has_landed_load(TARGET).unwrap());
            if status == "skip" {
                ctx(&s, "L1").record_skip();
            } else {
                ctx(&s, "L1").record(&[], 0, "refused");
            }
            assert!(
                s.has_landed_load(TARGET).unwrap(),
                "a same-id {status} re-run must not disown the table"
            );
        }
    }

    /// A CDC load job over an fs-backed prefix rooted at `dir`, against an empty fake warehouse.
    fn cdc_job<'a>(
        dir: &tempfile::TempDir,
        state: &'a StateStore,
        plan: &'a load::plan::LoadPlan,
    ) -> LoadJob<'a> {
        LoadJob {
            plan,
            state: Some(state),
            load_id: "L1",
            allow_source_drift: false,
            mode: load::plan::LoadMode::Cdc,
            loader: Box::new(load::tests::fake_loader(0)),
            store: crate::destination::gcs::GcsStore::open_fs(&dir.path().display().to_string())
                .expect("a filesystem-backed store"),
            journal: Default::default(),
        }
    }

    /// The closed ledger row every finished run leaves behind.
    fn finished_row(state: &StateStore, run: &str) {
        state
            .begin_run_under_lease(run, "orders", "src", "gs://b/p/", "2026-08-21T00:00:00Z")
            .unwrap();
        state
            .finish_run(run, "success", "2026-08-21T00:05:00Z")
            .unwrap();
    }

    /// A CDC load plan for `table` whose prefix `gs://b/p/` is the `p/` directory under `dir`.
    fn cdc_plan(dir: &tempfile::TempDir, table: &str) -> load::plan::LoadPlan {
        use load::plan::{CdcLayout, LoadMode, LoadPlan, LoadSection, LoadTarget};
        LoadPlan {
            source_key: String::new(),
            deleted_flag: false,
            renames: Vec::new(),
            rename_warnings: Vec::new(),
            refusal: None,
            landings: Vec::new(),
            export_name: "orders".into(),
            unit: None,
            table: table.into(),
            partition: None,
            specs: vec![],
            gcs_prefix: "gs://b/p/".to_string(),
            destination: crate::config::DestinationConfig {
                destination_type: crate::config::DestinationType::Local,
                path: Some(dir.path().display().to_string()),
                ..Default::default()
            },
            load: LoadSection {
                deleted_flag: None,
                layout: None,
                target: LoadTarget::Bigquery {
                    project: "p".into(),
                    dataset: "d".into(),
                },
                cleanup_source: false,
                pk: load::plan::KeyColumns::Auto,
                allow_source_drift: false,
                gc_orphans: false,
                cluster_by: load::plan::KeyColumns::Auto,
                partition: None,
            },
            mode: LoadMode::Cdc,
            cursor_column: None,
            continued_key: false,
            pk: vec![],
            clustering: load::plan::Clustering::Auto(vec![]),
            pinned_run: None,
            layout: CdcLayout::LogAndView,
        }
    }

    /// Drive an up-to-date load: the run closure must never be reached.
    fn load_nothing(job: LoadJob<'_>) {
        let out = execute_load(
            job,
            |_| {},
            |_, _, _, _| -> Result<(u64, ())> {
                panic!("nothing is staged under the prefix — the run closure must not run")
            },
            |_, _| {},
        )
        .expect("an up-to-date load is not an error");
        assert!(out.is_none(), "an up-to-date load reports no work");
    }

    /// The load ENVELOPE, driven offline for the first time.
    #[test]
    fn the_load_envelope_records_one_skip_row_for_an_empty_prefix() {
        let dir = tempfile::tempdir().expect("a temp prefix");
        let state = StateStore::open_in_memory().unwrap();
        let plan = cdc_plan(&dir, "orders");
        load_nothing(cdc_job(&dir, &state, &plan));

        let loads = state.recent_loads(Some("db.orders"), 10).unwrap();
        assert_eq!(loads.len(), 1, "exactly one audit row per load, always");
        assert_eq!(loads[0].status, "success");
        assert_eq!(loads[0].rows_loaded, 0);
        assert!(
            state.loaded_source_run_ids("db.orders").unwrap().is_empty(),
            "an up-to-date no-op consumes no runs"
        );
    }

    /// A stop raised while the runs are selected, before anything is written, is on the record as `refused` and the next cycle refuses again by the same code.
    #[test]
    fn a_mixed_split_generation_is_journaled_refused_and_refuses_again() {
        let state = StateStore::open_in_memory().unwrap();
        mixed_split_generation_refuses_twice(&state, "orders_split");
    }

    /// The same rule on a shared Postgres ledger, where the `refused` row outlives the process and the next cycle is another process.
    #[test]
    fn pg_a_mixed_split_generation_is_journaled_refused_and_refuses_again() {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL unset");
        };
        if !url.starts_with("postgres") {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
        }
        let state = StateStore::open_at_ref(&crate::state::StateRef::Postgres(url.into())).unwrap();
        let table = format!("orders_split_{}", std::process::id());
        mixed_split_generation_refuses_twice(&state, &table);
    }

    /// Two load cycles over a prefix holding two split generations, on `state`.
    fn mixed_split_generation_refuses_twice(state: &StateStore, table: &str) {
        let dir = tempfile::tempdir().unwrap();
        let mut plan = cdc_plan(&dir, table);
        plan.mode = load::plan::LoadMode::Full;
        for (run, unit, lo, hi) in [
            ("r1", "orders#0", None, Some("500")),
            ("r2", "orders#1", Some("500"), None),
            ("r3", "orders#2", None, Some("300")),
        ] {
            let part = format!("{run}.parquet");
            let mut m = super::live_only_decisions::success_manifest(run, &part);
            m.export_name = unit.into();
            m.mode = "batch".into();
            m.split_window = Some(crate::manifest::SplitWindow {
                key_column: "id".into(),
                lo: lo.map(str::to_string),
                hi: hi.map(str::to_string),
            });
            super::live_only_decisions::write_at(&dir, &format!("p/{part}"), b"x");
            super::live_only_decisions::write_at(
                &dir,
                &format!("p/manifest-{run}.json"),
                &serde_json::to_vec(&m).unwrap(),
            );
        }
        let fqtn = format!("db.{table}");
        for cycle in 1..=2 {
            let mut job = cdc_job(&dir, state, &plan);
            job.mode = load::plan::LoadMode::Full;
            let err = execute_load(
                job,
                |_| {},
                |_, _, _, _| -> Result<(u64, ())> {
                    panic!("a mixed generation must stop before the run closure")
                },
                |_, _| {},
            )
            .err()
            .unwrap_or_else(|| panic!("cycle {cycle}: the mixed generation is refused"));
            crate::error::assert_coded(&err, "RIVET_LOAD_SPLIT_GENERATIONS_MIXED", 5);
            let loads = state.recent_loads(Some(&fqtn), 10).unwrap();
            assert_eq!(
                loads.iter().map(|l| l.status.as_str()).collect::<Vec<_>>(),
                vec!["refused"],
                "cycle {cycle}: the stop is on the record, once per load id"
            );
            assert!(
                err.is::<load::Refused>(),
                "cycle {cycle}: typed as a stop before the write: {err:#}"
            );
            assert!(
                !state.has_landed_load(&fqtn).unwrap(),
                "cycle {cycle}: a refusal is never rivet's own attempt"
            );
        }
    }

    /// Runs that produced no files (an idle CDC cycle) are consumed by the load that found nothing in them.
    #[test]
    fn a_load_consumes_the_runs_that_produced_no_files() {
        let dir = tempfile::tempdir().expect("a temp prefix");
        let state = StateStore::open_in_memory().unwrap();
        let plan = cdc_plan(&dir, "orders_idle");
        for run in ["r-idle-1", "r-idle-2"] {
            finished_row(&state, run);
            let mut m = super::live_only_decisions::success_manifest(run, "unused.parquet");
            m.parts.clear();
            m.part_count = 0;
            m.row_count = 0;
            super::live_only_decisions::write_at(
                &dir,
                &format!("p/manifest-{run}.json"),
                &serde_json::to_vec(&m).unwrap(),
            );
        }
        load_nothing(cdc_job(&dir, &state, &plan));

        let consumed = state.loaded_source_run_ids("db.orders_idle").unwrap();
        assert!(
            consumed.contains("r-idle-1") && consumed.contains("r-idle-2"),
            "both empty runs are recorded consumed: {consumed:?}"
        );
        let loads = state.recent_loads(Some("db.orders_idle"), 10).unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(
            (loads[0].status.as_str(), loads[0].rows_loaded),
            ("success", 0)
        );
    }

    /// A full export whose newest run is empty means the source is empty.
    #[test]
    fn an_empty_full_run_reaches_the_warehouse_write() {
        let dir = tempfile::tempdir().expect("a temp prefix");
        let state = StateStore::open_in_memory().unwrap();
        let mut plan = cdc_plan(&dir, "orders_emptied");
        plan.mode = load::plan::LoadMode::Full;
        let mut m = super::live_only_decisions::success_manifest("r-empty", "unused.parquet");
        m.parts.clear();
        m.part_count = 0;
        m.row_count = 0;
        super::live_only_decisions::write_at(
            &dir,
            "p/manifest-r-empty.json",
            &serde_json::to_vec(&m).unwrap(),
        );
        let mut job = cdc_job(&dir, &state, &plan);
        job.mode = load::plan::LoadMode::Full;
        let reached = std::cell::Cell::new(false);
        let out = execute_load(
            job,
            |_| {},
            |_, _, inputs, _| -> Result<(u64, ())> {
                assert!(inputs.uris.is_empty(), "an empty run hands no files");
                reached.set(true);
                Ok((0, ()))
            },
            |_, _| {},
        );
        assert!(
            reached.get(),
            "an emptied full source must reach the warehouse write: {out:?}"
        );
    }

    /// The up-to-date line names the mode, the table and the warehouse, word for word.
    #[test]
    fn the_up_to_date_line_names_the_mode_table_and_warehouse() {
        let dir = tempfile::tempdir().expect("a temp prefix");
        let state = StateStore::open_in_memory().unwrap();
        let plan = cdc_plan(&dir, "orders");
        assert_eq!(
            up_to_date_line(&cdc_job(&dir, &state, &plan)),
            "  cdc load orders → bigquery: up to date — every extraction run already loaded"
        );
    }

    /// A staged part holding a date BigQuery cannot hold is refused by code before the warehouse write, in every mode; the part one day inside the range reaches it.
    #[test]
    fn a_part_outside_bigquery_range_is_refused_before_the_warehouse_write() {
        use crate::types::target::{BqType, TargetColumnSpec, TargetStatus};
        use arrow::array::{ArrayRef, Date32Array};
        let part = |days: i32| {
            let col = std::sync::Arc::new(Date32Array::from(vec![days])) as ArrayRef;
            let batch = arrow::record_batch::RecordBatch::try_from_iter([("d", col)]).unwrap();
            let mut buf = Vec::new();
            let mut w =
                parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
            buf
        };
        for mode in [load::plan::LoadMode::Cdc, load::plan::LoadMode::Full] {
            for (days, refused) in [(-719_163, true), (-719_162, false)] {
                let dir = tempfile::tempdir().expect("a temp prefix");
                let state = StateStore::open_in_memory().unwrap();
                let mut plan = cdc_plan(&dir, "orders_dates");
                plan.mode = mode;
                plan.specs = vec![TargetColumnSpec {
                    column_name: "d".into(),
                    target_type: BqType::Date.into(),
                    autoload_type: BqType::Date.into(),
                    status: TargetStatus::Ok,
                    note: None,
                    cast_sql: None,
                }];
                let m = super::live_only_decisions::success_manifest("r-d", "part-0.parquet");
                super::live_only_decisions::write_at(
                    &dir,
                    "p/manifest-r-d.json",
                    &serde_json::to_vec(&m).unwrap(),
                );
                super::live_only_decisions::write_at(&dir, "p/part-0.parquet", &part(days));
                let reached = std::cell::Cell::new(false);
                let mut job = cdc_job(&dir, &state, &plan);
                job.mode = mode;
                let out = execute_load(
                    job,
                    |_| {},
                    |_, _, _, _| -> Result<(u64, ())> {
                        reached.set(true);
                        Ok((1, ()))
                    },
                    |_, _| {},
                );
                assert_eq!(reached.get(), !refused, "{mode:?} {days}: {out:?}");
                match (refused, out) {
                    (true, Err(e)) => assert_eq!(
                        crate::error::error_code(&e),
                        Some("RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE"),
                        "{e:#}"
                    ),
                    (false, Ok(_)) => {}
                    (_, other) => panic!("{mode:?} {days}: {other:?}"),
                }
            }
        }
    }

    /// A run that DID produce files is loaded, not consumed as empty beside the idle ones.
    #[test]
    fn a_load_with_files_reaches_the_warehouse_write() {
        let dir = tempfile::tempdir().expect("a temp prefix");
        let state = StateStore::open_in_memory().unwrap();
        let plan = cdc_plan(&dir, "orders_busy");
        let m = super::live_only_decisions::success_manifest("r-busy", "part-0.parquet");
        super::live_only_decisions::write_at(
            &dir,
            "p/manifest-r-busy.json",
            &serde_json::to_vec(&m).unwrap(),
        );
        super::live_only_decisions::write_at(&dir, "p/part-0.parquet", b"PAR1");
        let reached = std::cell::Cell::new(false);
        let out = execute_load(
            cdc_job(&dir, &state, &plan),
            |_| {},
            |_, _, _, _| -> Result<(u64, ())> {
                reached.set(true);
                Ok((1, ()))
            },
            |_, _| {},
        );
        assert!(
            reached.get(),
            "a run with a part must reach the warehouse write: {out:?}"
        );
    }

    /// A first full load that failed owns nothing.
    #[test]
    fn a_retry_after_a_failed_first_load_refuses_a_table_that_appeared_since() {
        let dir = tempfile::tempdir().expect("a temp prefix");
        let state = StateStore::open_in_memory().unwrap();
        let mut plan = cdc_plan(&dir, "orders_retry");
        plan.mode = load::plan::LoadMode::Full;
        let m = super::live_only_decisions::success_manifest("r-1", "part-0.parquet");
        super::live_only_decisions::write_at(
            &dir,
            "p/manifest-r-1.json",
            &serde_json::to_vec(&m).unwrap(),
        );
        super::live_only_decisions::write_at(&dir, "p/part-0.parquet", b"PAR1");

        let mut first = cdc_job(&dir, &state, &plan);
        first.mode = load::plan::LoadMode::Full;
        let failed = execute_load(
            first,
            |_| {},
            |_, _, _, _| -> Result<(u64, ())> {
                anyhow::bail!("the insert into the swap table failed")
            },
            |_, _| {},
        );
        assert!(failed.is_err(), "the first load fails: {failed:?}");
        assert_eq!(
            state.recent_loads(Some("db.orders_retry"), 10).unwrap()[0].status,
            "failed"
        );

        let mut retry = cdc_job(&dir, &state, &plan);
        retry.mode = load::plan::LoadMode::Full;
        retry.load_id = "L2";
        retry.loader = Box::new(
            load::tests::fake_loader(0).with_kind("orders_retry", load::ObjectKind::Table),
        );
        let err = execute_load(
            retry,
            |_| {},
            |_, _, _, _| -> Result<(u64, ())> {
                panic!("a table rivet never loaded must be refused before the write")
            },
            |_, _| {},
        )
        .expect_err("the foreign table is refused");
        crate::error::assert_coded(&err, "RIVET_LOAD_TARGET_NOT_RIVETS", 5);
        assert!(
            format!("{err:#}").contains("consumed no run"),
            "the refusal says a never-completed first load can be dropped safely: {err:#}"
        );
    }

    /// Stage one successful run with one part under the plan's `p/` prefix.
    fn stage_run(dir: &tempfile::TempDir, run: &str, mode: &str) {
        let part = format!("part-{run}.parquet");
        let mut m = super::live_only_decisions::success_manifest(run, &part);
        m.mode = mode.into();
        super::live_only_decisions::write_at(
            dir,
            &format!("p/manifest-{run}.json"),
            &serde_json::to_vec(&m).unwrap(),
        );
        super::live_only_decisions::write_at(dir, &format!("p/{part}"), b"x");
    }

    /// Stage CDC run `run` as a `Success` manifest holding exactly `parts`, the way each roll rewrites it.
    fn stage_cdc_parts(dir: &tempfile::TempDir, run: &str, started_at: &str, parts: &[&str]) {
        let mut m = super::live_only_decisions::success_manifest(run, parts[0]);
        m.mode = "cdc".into();
        m.started_at = started_at.into();
        m.finished_at = started_at.into();
        m.parts = parts
            .iter()
            .enumerate()
            .map(|(i, path)| crate::manifest::ManifestPart {
                part_id: i as u32,
                path: (*path).into(),
                rows: 1,
                size_bytes: 1,
                content_fingerprint: format!("xxh3:{i:016x}"),
                content_md5: String::new(),
                status: crate::manifest::PartStatus::Committed,
            })
            .collect();
        m.part_count = parts.len() as u32;
        m.row_count = parts.len() as i64;
        for part in parts {
            super::live_only_decisions::write_at(dir, &format!("p/{part}"), b"x");
        }
        super::live_only_decisions::write_at(
            dir,
            &format!("p/manifest-{run}.json"),
            &serde_json::to_vec(&m).unwrap(),
        );
    }

    /// One load cycle over the staged prefix: the part files it handed the warehouse.
    fn delivered_by_cycle(
        dir: &tempfile::TempDir,
        state: &StateStore,
        plan: &load::plan::LoadPlan,
        load_id: &str,
    ) -> std::collections::BTreeSet<String> {
        let delivered = std::cell::RefCell::new(std::collections::BTreeSet::new());
        let mut job = cdc_job(dir, state, plan);
        job.load_id = load_id;
        execute_load(
            job,
            |_| {},
            |_, _, inputs, _| -> Result<(u64, ())> {
                delivered.borrow_mut().extend(
                    inputs
                        .uris
                        .iter()
                        .map(|u| u.rsplit('/').next().unwrap().to_string()),
                );
                Ok((inputs.uris.len() as u64, ()))
            },
            |_, _| {},
        )
        .expect("the load cycle succeeds");
        delivered.into_inner()
    }

    /// Where the ledger row of the CDC run `r-cdc` lives, if anywhere.
    enum RowAt {
        LoadState,
        Nowhere,
        OtherState,
    }

    /// A CDC run that rolls twice while a load overlaps it; the parts every cycle delivered, unioned.
    fn parts_delivered_across_a_growing_run(table: &str, row: RowAt) -> Vec<String> {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let other = StateStore::open_in_memory().unwrap();
        let plan = cdc_plan(&dir, table);
        let begin = |s: &StateStore| {
            s.begin_run_under_lease(
                "r-cdc",
                "orders",
                "src",
                "gs://b/p/",
                "2026-08-21T00:00:00Z",
            )
            .unwrap()
        };
        match row {
            RowAt::LoadState => begin(&state),
            RowAt::OtherState => begin(&other),
            RowAt::Nowhere => {}
        }
        let mut seen = std::collections::BTreeSet::new();
        stage_cdc_parts(&dir, "r-cdc", "2026-08-21T00:00:00Z", &["part-0.parquet"]);
        seen.extend(delivered_by_cycle(&dir, &state, &plan, "L1"));
        stage_cdc_parts(
            &dir,
            "r-cdc",
            "2026-08-21T00:00:00Z",
            &["part-0.parquet", "part-1.parquet"],
        );
        seen.extend(delivered_by_cycle(&dir, &state, &plan, "L2"));
        if matches!(row, RowAt::LoadState) {
            state
                .finish_run("r-cdc", "success", "2026-08-21T00:05:00Z")
                .unwrap();
        }
        seen.extend(delivered_by_cycle(&dir, &state, &plan, "L3"));
        seen.into_iter().collect()
    }

    const BOTH_PARTS: [&str; 2] = ["part-0.parquet", "part-1.parquet"];

    /// A load that overlaps a CDC run whose ledger row is in the load's state delivers every part the run wrote.
    #[test]
    fn a_load_overlapping_a_cdc_run_with_a_row_delivers_every_part() {
        assert_eq!(
            parts_delivered_across_a_growing_run("grow_row", RowAt::LoadState),
            BOTH_PARTS
        );
    }

    /// A load that overlaps a CDC run no ledger lists delivers every part the run wrote.
    #[test]
    fn a_load_overlapping_a_cdc_run_with_no_row_delivers_every_part() {
        assert_eq!(
            parts_delivered_across_a_growing_run("grow_no_row", RowAt::Nowhere),
            BOTH_PARTS
        );
    }

    /// A load from a second state database delivers every part of a CDC run whose row is in the first.
    #[test]
    fn a_load_overlapping_a_cdc_run_over_a_foreign_state_delivers_every_part() {
        assert_eq!(
            parts_delivered_across_a_growing_run("grow_foreign", RowAt::OtherState),
            BOTH_PARTS
        );
    }

    /// With no row and a newer CDC run under the prefix, the older run is consumed and the newer stays retryable.
    #[test]
    fn a_cdc_run_with_no_row_is_consumed_once_a_newer_run_exists_and_not_before() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let plan = cdc_plan(&dir, "grow_newer");
        stage_cdc_parts(&dir, "r-old", "2026-08-21T00:00:00Z", &["old-0.parquet"]);
        stage_cdc_parts(&dir, "r-new", "2026-08-21T01:00:00Z", &["new-0.parquet"]);
        let first = delivered_by_cycle(&dir, &state, &plan, "L1");
        assert_eq!(
            first.iter().cloned().collect::<Vec<_>>(),
            ["new-0.parquet", "old-0.parquet"]
        );
        let loaded = state.loaded_source_run_ids("db.grow_newer").unwrap();
        assert!(
            loaded.contains("r-old"),
            "the superseded run is consumed: {loaded:?}"
        );
        assert!(
            !loaded.contains("r-new"),
            "the newest run may still be writing and stays retryable: {loaded:?}"
        );
        let again = delivered_by_cycle(&dir, &state, &plan, "L2");
        assert_eq!(again.iter().cloned().collect::<Vec<_>>(), ["new-0.parquet"]);
    }

    /// A manifest of `mode` for `run` in `family`, started at `started_at`, keyed as the bucket lists it.
    fn listed(
        run: &str,
        mode: &str,
        family: &str,
        started_at: &str,
        status: crate::manifest::ManifestStatus,
    ) -> (String, crate::manifest::RunManifest) {
        let mut m = super::live_only_decisions::success_manifest(run, "part.parquet");
        m.mode = mode.into();
        m.export_family = family.into();
        m.started_at = started_at.into();
        m.status = status;
        (format!("p/manifest-{run}.json"), m)
    }

    fn ids(set: &std::collections::HashSet<String>) -> Vec<&str> {
        let mut ids: Vec<&str> = set.iter().map(String::as_str).collect();
        ids.sort_unstable();
        ids
    }

    /// A CDC run no ledger row accounts for is writing unless a newer CDC run of its family exists.
    #[test]
    fn a_cdc_run_without_a_row_is_writing_until_a_newer_run_of_its_family_exists() {
        use crate::manifest::ManifestStatus::Success;
        let state = StateStore::open_in_memory().unwrap();
        let at = |run, family, started| listed(run, "cdc", family, started, Success);
        let two = [
            at("r-old", "orders", "2026-08-21T00:00:00Z"),
            at("r-new", "orders", "2026-08-21T01:00:00Z"),
        ];
        assert_eq!(ids(&bucket_writers(Some(&state), &two)), ["r-new"]);
        let tie = [
            at("r-a", "orders", "2026-08-21T00:00:00Z"),
            at("r-b", "orders", "2026-08-21T00:00:00Z"),
        ];
        assert_eq!(ids(&bucket_writers(Some(&state), &tie)), ["r-a", "r-b"]);
        let families = [
            at("r-a", "orders", "2026-08-21T00:00:00Z"),
            at("r-b", "items", "2026-08-21T01:00:00Z"),
        ];
        assert_eq!(
            ids(&bucket_writers(Some(&state), &families)),
            ["r-a", "r-b"]
        );
        finished_row(&state, "r-new");
        assert!(bucket_writers(Some(&state), &two).is_empty());
        state
            .begin_run_under_lease(
                "r-old",
                "orders",
                "src",
                "gs://b/p/",
                "2026-08-21T00:00:00Z",
            )
            .unwrap();
        assert!(
            bucket_writers(Some(&state), &two).is_empty(),
            "a running row is the ledger's to report, not the bucket's"
        );
    }

    /// Everything but a CDC run without a row reads as before.
    #[test]
    fn the_bucket_writers_of_every_other_run_are_the_running_markers() {
        use crate::manifest::ManifestStatus::{Running, Success};
        let state = StateStore::open_in_memory().unwrap();
        let batch = [
            listed(
                "r-batch",
                "batch",
                "orders",
                "2026-08-21T00:00:00Z",
                Success,
            ),
            listed("r-mark", "batch", "orders", "2026-08-21T01:00:00Z", Running),
        ];
        assert_eq!(ids(&bucket_writers(Some(&state), &batch)), ["r-mark"]);
        let cdc = [listed(
            "r-cdc",
            "cdc",
            "orders",
            "2026-08-21T00:00:00Z",
            Success,
        )];
        assert!(
            bucket_writers(None, &cdc).is_empty(),
            "a stateless load has no ledger to ask and keeps its documented degradation"
        );
        let marker = [listed(
            "r-cdc",
            "cdc",
            "orders",
            "2026-08-21T00:00:00Z",
            Running,
        )];
        assert_eq!(ids(&bucket_writers(Some(&state), &marker)), ["r-cdc"]);
        assert_eq!(
            bucket_writers(None, &marker),
            load::reconcile::active_running_run_ids(&marker)
        );
    }

    /// The statuses the ledger holds for `target`, newest first.
    fn statuses(state: &StateStore, target: &str) -> Vec<String> {
        state
            .recent_loads(Some(target), 10)
            .unwrap()
            .into_iter()
            .map(|l| l.status)
            .collect()
    }

    /// What one load cycle left: its error, the warehouse writes, the ledger rows, the next ownership.
    #[derive(Debug, PartialEq)]
    struct Cycle {
        refusal: Option<String>,
        typed: bool,
        exit: Option<(i32, Option<&'static str>)>,
        writes: Vec<String>,
        ledger: Vec<String>,
        next: load::Ownership,
    }

    impl Cycle {
        /// Read what a finished load left in the fake warehouse and in the ledger.
        fn observed(
            err: Option<anyhow::Error>,
            writes: &std::cell::RefCell<Vec<String>>,
            state: &StateStore,
            target: &str,
        ) -> Self {
            Cycle {
                refusal: err.as_ref().map(|e| format!("{e:#}")),
                typed: err.as_ref().is_some_and(|e| e.is::<load::Refused>()),
                exit: err
                    .as_ref()
                    .map(|e| (crate::error::classify_exit(e), crate::error::error_code(e))),
                writes: writes.borrow().clone(),
                ledger: statuses(state, target),
                next: ownership_of(Some(state), target, "load"),
            }
        }

        /// A stop that wrote nothing, is journaled `refused` only, and says `needle`.
        fn stopped_saying(&self, needle: &str) -> bool {
            self.typed
                && self.writes.is_empty()
                && self.ledger.iter().all(|s| s == "refused")
                && self.refusal.as_deref().is_some_and(|m| m.contains(needle))
        }
    }

    /// One base-and-buffer CDC cycle through the production closure, over `fake`.
    fn cdc_base_cycle(
        dir: &tempfile::TempDir,
        state: &StateStore,
        plan: &load::plan::LoadPlan,
        load_id: &str,
        fake: load::tests::FakeLoader,
    ) -> Cycle {
        let writes = fake.writes();
        let mut job = cdc_job(dir, state, plan);
        job.load_id = load_id;
        job.loader = Box::new(fake);
        let err = load_one_cdc_base(job, &["id".to_string()], false, Some(state)).err();
        Cycle::observed(err, &writes, state, &format!("db.{}", plan.table))
    }

    /// A first base-and-buffer load reports the baseline it landed.
    #[test]
    fn a_first_cdc_load_reports_the_baseline_it_landed_in_the_base() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let plan = base_plan(&dir, "first_base");
        stage_run(&dir, "r-base", "full");
        let mut job = cdc_job(&dir, &state, &plan);
        job.loader = Box::new(load::tests::fake_loader(1));
        let report = load_one_cdc_base(job, &["id".to_string()], false, Some(&state))
            .unwrap()
            .expect("the baseline loads");
        let work = cdc_work(&report);
        assert_eq!(
            (work.rows, work.wrote.as_deref(), work.buffer.as_deref()),
            (1, Some("db.first_base"), None)
        );
        assert!(cdc_ok_line(&report).starts_with("baseline: 1 row(s) landed in `db.first_base`"));
    }

    /// A load whose baseline landed in the base and whose stream buffered changes reports both legs' rows.
    #[test]
    fn a_cdc_load_of_a_baseline_and_changes_reports_both_legs() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let plan = base_plan(&dir, "both_legs");
        stage_run(&dir, "r-base", "full");
        stage_run(&dir, "r-cdc", "cdc");
        let mut job = cdc_job(&dir, &state, &plan);
        job.loader = Box::new(load::tests::fake_loader(1));
        let report = load_one_cdc_base(job, &["id".to_string()], false, Some(&state))
            .unwrap()
            .expect("both legs load");
        let work = cdc_work(&report);
        assert_eq!(
            (work.rows, work.wrote.as_deref(), work.buffer.as_deref()),
            (2, Some("db.both_legs"), Some("db.both_legs__changes"))
        );
    }

    /// A first base-and-buffer load whose baseline lands alone still cleans the staged baseline under `cleanup_source`.
    #[test]
    fn a_baseline_loaded_alone_is_cleaned_under_cleanup_source() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let mut plan = base_plan(&dir, "alone_cleaned");
        plan.load.cleanup_source = true;
        plan.gcs_prefix = "gs://alone-cleaned/p/".into();
        finished_row(&state, "r-base");
        stage_run(&dir, "r-base", "full");
        let mut job = cdc_job(&dir, &state, &plan);
        job.loader = Box::new(load::tests::fake_loader(1));
        let report = load_one_cdc_base(job, &["id".to_string()], false, Some(&state))
            .unwrap()
            .expect("the baseline loads");
        assert!(report.baseline.as_ref().is_some_and(|b| b.source_cleaned));
        assert!(!dir.path().join("p/part-r-base.parquet").exists());
        assert!(
            cdc_ok_line(&report).ends_with(" (source cleaned)"),
            "{}",
            cdc_ok_line(&report)
        );
    }

    /// A baseline whose stream also buffers leaves the cleanup to the buffer leg, which reports it.
    #[test]
    fn a_baseline_followed_by_a_buffer_leg_leaves_the_cleanup_to_that_leg() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let mut plan = base_plan(&dir, "buffer_cleans");
        plan.load.cleanup_source = true;
        plan.gcs_prefix = "gs://buffer-cleans/p/".into();
        finished_row(&state, "r-base");
        finished_row(&state, "r-cdc");
        stage_run(&dir, "r-base", "full");
        stage_run(&dir, "r-cdc", "cdc");
        let mut job = cdc_job(&dir, &state, &plan);
        job.loader = Box::new(load::tests::fake_loader(1));
        let report = load_one_cdc_base(job, &["id".to_string()], false, Some(&state))
            .unwrap()
            .expect("both legs load");
        assert!(report.baseline.as_ref().is_some_and(|b| !b.source_cleaned));
        assert!(report.source_cleaned);
    }

    /// Stage a successful baseline leg that exported no rows and wrote no part.
    fn stage_empty_baseline(dir: &tempfile::TempDir, run: &str) {
        let mut m = super::live_only_decisions::success_manifest(run, "unused.parquet");
        m.mode = "full".into();
        m.parts.clear();
        m.part_count = 0;
        m.row_count = 0;
        super::live_only_decisions::write_at(
            dir,
            &format!("p/manifest-{run}.json"),
            &serde_json::to_vec(&m).unwrap(),
        );
    }

    /// A base-and-buffer CDC plan over `table` with nothing staged.
    fn base_plan(dir: &tempfile::TempDir, table: &str) -> load::plan::LoadPlan {
        let mut plan = cdc_plan(dir, table);
        plan.specs = load::tests::spec_ok();
        plan.pk = vec!["id".into()];
        plan.layout = load::plan::CdcLayout::BaseAndBuffer;
        plan
    }

    /// What rivet 0.31 left for a table empty at its baseline.
    fn loaded_leg(state: &StateStore, target: &str, run: &str, mode: &str, rows: i64) {
        let mut earlier = ctx(state, "L0");
        earlier.target_fqtn = target;
        earlier.record_empty_runs(&[run.to_string()]);
        state
            .record_metric_full(&crate::state::MetricRow {
                export_name: "orders".into(),
                run_id: run.into(),
                total_rows: rows,
                status: "success".into(),
                mode: Some(mode.into()),
                ..Default::default()
            })
            .unwrap();
    }

    /// A table empty at the baseline gets an EMPTY base, in the load that reads the baseline, and the next cycle does not write it again.
    #[test]
    fn an_empty_baseline_builds_the_base_once() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let plan = base_plan(&dir, "empty_base");
        stage_empty_baseline(&dir, "r-base");
        let first = cdc_base_cycle(&dir, &state, &plan, "L1", load::tests::fake_loader(0));
        let again = cdc_base_cycle(
            &dir,
            &state,
            &plan,
            "L2",
            load::tests::fake_loader(0).with_kind("empty_base", load::ObjectKind::Table),
        );
        assert_eq!(
            (first.refusal, first.writes, again.refusal, again.writes),
            (
                None,
                vec!["materialize empty_base".to_string()],
                None,
                vec![]
            ),
            "(cycle-1 error, cycle-1 writes, cycle-2 error, cycle-2 writes)"
        );
        assert!(
            state
                .loaded_source_run_ids("db.empty_base")
                .unwrap()
                .contains("r-base")
        );
    }

    /// The state rivet 0.31 and older left: the empty baseline is journaled as loaded and its manifest cleaned, no base exists, and the buffer holds the changes made since.
    #[test]
    fn a_base_an_empty_baseline_never_built_is_built_by_the_next_load() {
        use load::ObjectKind::Table;
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let (table, log, target) = ("healed", "healed__changes", "db.healed");
        let plan = base_plan(&dir, table);
        loaded_leg(&state, target, "r-base", "keyset", 0);
        loaded_leg(&state, target, "r-stream", "cdc", 7);
        let buffered = || {
            load::tests::fake_loader(0)
                .with_kind(log, Table)
                .with_rows(log, 5)
        };
        let healed = cdc_base_cycle(&dir, &state, &plan, "L1", buffered());
        let again = cdc_base_cycle(
            &dir,
            &state,
            &plan,
            "L2",
            buffered().with_kind(table, Table),
        );
        assert_eq!(
            (healed.refusal, healed.writes, again.refusal, again.writes),
            (None, vec!["materialize healed".to_string()], None, vec![]),
            "(cycle-1 error, cycle-1 writes, cycle-2 error, cycle-2 writes)"
        );
    }

    /// A base-and-buffer base that already holds rows is never replaced by a whole-table pass.
    #[test]
    fn a_whole_table_pass_over_an_existing_base_is_buffered_and_never_replaces_it() {
        use load::ObjectKind::Table;
        use load::plan::{CdcLayout, LoadMode};
        let mut seen = Vec::new();
        for (tag, exists, flag) in [
            ("kept", true, true),
            ("fresh", false, true),
            ("noflag", true, false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let state = StateStore::open_in_memory().unwrap();
            let table = format!("{tag}_inc_base");
            let plan = plan_of(
                &dir,
                &table,
                LoadMode::Incremental,
                CdcLayout::BaseAndBuffer,
            );
            loaded_before(&state, &format!("db.{table}"));
            stage_incremental_run(&dir, "r-1", "2026-10-20T00:00:00Z", None);
            let fake = || {
                let f = load::tests::fake_loader(1);
                if exists {
                    f.with_kind(&table, Table).with_rows(&table, 5)
                } else {
                    f
                }
            };
            let first = incremental_cycle(&dir, &state, &plan, "L1", fake());
            let again = incremental_cycle(&dir, &state, &plan, "L2", fake());
            seen.push((
                "incremental",
                tag,
                first.refusal,
                first.writes,
                again.writes,
            ));

            let dir = tempfile::tempdir().unwrap();
            let state = StateStore::open_in_memory().unwrap();
            let table = format!("{tag}_cdc_base");
            let mut plan = base_plan(&dir, &table);
            plan.deleted_flag = flag;
            loaded_before(&state, &format!("db.{table}"));
            stage_run(&dir, "r-base", "keyset");
            let fake = || {
                let f = load::tests::fake_loader(1);
                if exists {
                    f.with_kind(&table, Table).with_rows(&table, 5)
                } else {
                    f
                }
            };
            let first = cdc_base_cycle(&dir, &state, &plan, "L1", fake());
            let again = cdc_base_cycle(&dir, &state, &plan, "L2", fake());
            seen.push(("cdc", tag, first.refusal, first.writes, again.writes));
        }
        let w = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            seen,
            vec![
                (
                    "incremental",
                    "kept",
                    None,
                    w(&["append kept_inc_base"]),
                    w(&[])
                ),
                (
                    "cdc",
                    "kept",
                    None,
                    w(&["append kept_cdc_base", "flag absent kept_cdc_base"]),
                    w(&[])
                ),
                (
                    "incremental",
                    "fresh",
                    None,
                    w(&["materialize fresh_inc_base"]),
                    w(&[])
                ),
                (
                    "cdc",
                    "fresh",
                    None,
                    w(&["materialize fresh_cdc_base"]),
                    w(&[])
                ),
                (
                    "incremental",
                    "noflag",
                    None,
                    w(&["append noflag_inc_base"]),
                    w(&[])
                ),
                (
                    "cdc",
                    "noflag",
                    None,
                    w(&["append noflag_cdc_base"]),
                    w(&[])
                ),
            ],
            "(mode, base, cycle-1 error, cycle-1 writes, cycle-2 writes)"
        );
    }

    /// A missing base is NOT built empty when it may once have held rows, or when this state cannot say.
    #[test]
    fn a_missing_base_that_may_have_held_rows_is_never_built_empty() {
        let merged = |state: &StateStore, target: &str| {
            state
                .store_load(&LoadRecord {
                    load_id: "C0".into(),
                    export_name: "orders".into(),
                    target_table: target.into(),
                    warehouse: "bigquery".into(),
                    mode: "compact".into(),
                    source_run_ids: Vec::new(),
                    source_ident: String::new(),
                    rows_loaded: 3,
                    status: "success".into(),
                    finished_at: "2026-08-22T00:00:00Z".into(),
                })
                .unwrap();
        };
        let cases = [
            "merged before",
            "two baselines",
            "baseline had rows",
            "no run record",
            "view layout",
        ];
        let mut wrote = Vec::new();
        for case in cases {
            let dir = tempfile::tempdir().unwrap();
            let state = StateStore::open_in_memory().unwrap();
            let table = format!("kept_{}", case.replace(' ', "_"));
            let target = format!("db.{table}");
            let mut plan = base_plan(&dir, &table);
            match case {
                "baseline had rows" => loaded_leg(&state, &target, "r-base", "keyset", 3),
                "no run record" => loaded_leg(&state, &target, "r-base", "cdc", 0),
                _ => loaded_leg(&state, &target, "r-base", "keyset", 0),
            }
            match case {
                "merged before" => merged(&state, &target),
                "two baselines" => loaded_leg(&state, &target, "r-base-2", "keyset", 0),
                "view layout" => plan.layout = load::plan::CdcLayout::LogAndView,
                _ => {}
            }
            let job = cdc_job(&dir, &state, &plan);
            let writes = load::tests::fake_loader(0);
            build_missing_base(&plan, job.state, &writes).unwrap();
            wrote.push((case, writes.writes().borrow().clone()));
        }
        assert_eq!(wrote, cases.map(|c| (c, Vec::<String>::new())));
    }

    /// Only a baseline leg of a base-and-buffer stream, or a full load, is a whole-table pass.
    #[test]
    fn a_whole_table_pass_is_a_full_load_or_a_base_and_buffer_baseline_leg() {
        use load::plan::{CdcLayout, LoadMode};
        let dir = tempfile::tempdir().unwrap();
        let run = |mode: &str| {
            let mut m = super::live_only_decisions::success_manifest("r", "p.parquet");
            m.mode = mode.into();
            vec![("k".to_string(), m)]
        };
        let plan = |mode, layout| {
            let mut p = cdc_plan(&dir, "t");
            p.mode = mode;
            p.layout = layout;
            p
        };
        let cells = [
            (LoadMode::Full, CdcLayout::LogAndView, "full", true),
            (LoadMode::Cdc, CdcLayout::BaseAndBuffer, "full", true),
            (LoadMode::Cdc, CdcLayout::BaseAndBuffer, "cdc", false),
            (LoadMode::Cdc, CdcLayout::LogAndView, "full", false),
            (
                LoadMode::Incremental,
                CdcLayout::BaseAndBuffer,
                "incremental",
                false,
            ),
        ];
        for (mode, layout, leg, want) in cells {
            assert_eq!(
                holds_whole_table_pass(&plan(mode, layout), &run(leg)),
                want,
                "{mode:?} {layout:?} leg={leg}"
            );
        }
    }

    /// The note of a rebuilt base names the table, the cause and what merges the buffer.
    #[test]
    fn the_rebuild_note_names_the_table_the_cause_and_the_next_step() {
        assert_eq!(
            rebuild_note("p.d.t"),
            "  note: `p.d.t` does not exist although its baseline was loaded: the baseline \
             held no rows, and rivet 0.31 and older created no table for one. Creating it \
             empty now; the changes buffered since are merged into it by `rivet compact`"
        );
    }

    /// One incremental cycle through the production closure, over `fake`.
    fn incremental_cycle(
        dir: &tempfile::TempDir,
        state: &StateStore,
        plan: &load::plan::LoadPlan,
        load_id: &str,
        fake: load::tests::FakeLoader,
    ) -> Cycle {
        let writes = fake.writes();
        let mut job = cdc_job(dir, state, plan);
        job.load_id = load_id;
        job.mode = load::plan::LoadMode::Incremental;
        job.loader = Box::new(fake);
        let err =
            load_incremental_job(job, &["id".to_string()], "id", false, false, Some(state)).err();
        Cycle::observed(err, &writes, state, &format!("db.{}", plan.table))
    }

    /// Stage an incremental run: a whole-table pass without `cursor_low`, a delta with one.
    fn stage_incremental_run(
        dir: &tempfile::TempDir,
        run: &str,
        at: &str,
        cursor_low: Option<&str>,
    ) {
        let part = format!("part-{run}.parquet");
        let mut m = super::live_only_decisions::success_manifest(run, &part);
        m.mode = "incremental".into();
        m.started_at = at.into();
        m.source.extraction = Some(
            serde_json::from_value(serde_json::json!({
                "strategy": "incremental",
                "cursor_column": "id",
                "cursor_low": cursor_low,
                "cursor_high": "9",
            }))
            .unwrap(),
        );
        super::live_only_decisions::write_at(
            dir,
            &format!("p/manifest-{run}.json"),
            &serde_json::to_vec(&m).unwrap(),
        );
        super::live_only_decisions::write_at(dir, &format!("p/{part}"), b"x");
    }

    /// A plan of `mode` and `layout` for `table`, with one column and nothing staged.
    fn plan_of(
        dir: &tempfile::TempDir,
        table: &str,
        mode: load::plan::LoadMode,
        layout: load::plan::CdcLayout,
    ) -> load::plan::LoadPlan {
        let mut plan = cdc_plan(dir, table);
        plan.mode = mode;
        plan.layout = layout;
        plan.specs = load::tests::spec_ok();
        plan.pk = vec!["id".into()];
        plan.cursor_column = Some("id".into());
        plan
    }

    /// Make `target` rivet's own: an earlier load of it is on the ledger.
    fn loaded_before(state: &StateStore, target: &str) {
        let mut earlier = ctx(state, "L0");
        earlier.target_fqtn = target;
        earlier.record_success(&["r-0".to_string()], 1);
    }

    /// One cycle of the load the plan's mode asks for, wired as `load_one*` wire it, over `fake`.
    fn cycle(
        dir: &tempfile::TempDir,
        state: &StateStore,
        plan: &load::plan::LoadPlan,
        load_id: &str,
        fake: load::tests::FakeLoader,
    ) -> Cycle {
        use load::plan::LoadMode;
        let writes = fake.writes();
        let target = format!("db.{}", plan.table);
        let mut job = cdc_job(dir, state, plan);
        job.load_id = load_id;
        job.mode = plan.mode;
        job.loader = Box::new(fake);
        let pk = ["id".to_string()];
        let out = execute_load(
            job,
            |_| {},
            |loader, _, inputs, _| -> Result<(u64, ())> {
                let (table, specs, uris) = (&plan.table, &plan.specs, &inputs.uris);
                let rows = match plan.mode {
                    LoadMode::Full => {
                        load::run_load(loader, table, specs, uris, None, None, inputs.ownership)?
                            .rows_loaded
                    }
                    LoadMode::Incremental => {
                        load::run_load_incremental(
                            loader,
                            table,
                            specs,
                            uris,
                            &pk,
                            "id",
                            None,
                            None,
                            inputs.ownership,
                            false,
                        )?
                        .rows_appended
                    }
                    LoadMode::Cdc => {
                        load::run_load_cdc(
                            loader,
                            table,
                            specs,
                            uris,
                            &pk,
                            load::cdc::SourceEngine::MySql,
                            None,
                            None,
                            inputs.ownership,
                            false,
                        )?
                        .rows_appended
                    }
                };
                Ok((rows, ()))
            },
            |_, _| {},
        );
        Cycle::observed(out.err(), &writes, state, &target)
    }

    /// A plan of `mode` for `table`, with one column and one staged run.
    fn staged_plan(
        dir: &tempfile::TempDir,
        table: &str,
        mode: load::plan::LoadMode,
    ) -> load::plan::LoadPlan {
        let mut plan = cdc_plan(dir, table);
        plan.mode = mode;
        plan.specs = load::tests::spec_ok();
        plan.pk = vec!["id".into()];
        plan.cursor_column = Some("id".into());
        stage_run(dir, "r-1", mode.ledger_str());
        plan
    }

    const DIALECTS: [load::cdc::Warehouse; 3] = [
        load::cdc::Warehouse::BigQuery,
        load::cdc::Warehouse::Snowflake,
        load::cdc::Warehouse::ClickHouse,
    ];

    /// A view at `<table>` or a `<table>__changes` this ledger has no record of is refused on every cycle and never written.
    #[test]
    fn an_append_refuses_a_foreign_view_or_log_on_every_cycle_and_writes_nothing() {
        use load::ObjectKind::{Table, View};
        use load::plan::LoadMode;
        let mut wrong = Vec::new();
        for (d, dialect) in DIALECTS.into_iter().enumerate() {
            for (mode, tag) in [(LoadMode::Incremental, "inc"), (LoadMode::Cdc, "cdc")] {
                for (case, at_table, at_log) in [
                    ("view", Some(View), None),
                    ("view_and_log", Some(View), Some(Table)),
                    ("log", None, Some(Table)),
                ] {
                    let dir = tempfile::tempdir().unwrap();
                    let state = StateStore::open_in_memory().unwrap();
                    let table = format!("foreign_{tag}_{case}_{d}");
                    let log = format!("{table}__changes");
                    let plan = staged_plan(&dir, &table, mode);
                    let fake = || {
                        let mut f = load::tests::fake_loader(1).speaking(dialect);
                        if let Some(kind) = at_table {
                            f = f.with_kind(&table, kind);
                        }
                        if let Some(kind) = at_log {
                            f = f.with_kind(&log, kind).with_rows(&log, 7);
                        }
                        f
                    };
                    let first = cycle(&dir, &state, &plan, "L1", fake());
                    let second = cycle(&dir, &state, &plan, "L2", fake());
                    let refused = |c: &Cycle| {
                        c.stopped_saying("no record of a successful rivet load")
                            && c.next == load::Ownership::Foreign
                            && c.exit == Some((5, Some("RIVET_LOAD_TARGET_NOT_RIVETS")))
                    };
                    let freed = load::tests::fake_loader(1).speaking(dialect);
                    let third = cycle(&dir, &state, &plan, "L3", freed);
                    let landed = [format!("append {table}"), format!("view {table}")];
                    if !(refused(&first)
                        && refused(&second)
                        && first.refusal == second.refusal
                        && third.refusal.is_none()
                        && third.writes == landed)
                    {
                        wrong.push((
                            dialect,
                            tag,
                            case,
                            [first.refusal, second.refusal, third.refusal],
                            [first.writes, second.writes, third.writes],
                        ));
                    }
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "(dialect, mode, what stands at the target, refusals of cycles 1-3, writes of cycles 1-3): {wrong:#?}"
        );
    }

    /// The production incremental closure refuses the same foreign names, cycle after cycle.
    #[test]
    fn the_incremental_load_refuses_foreign_names_under_both_layouts_on_every_cycle() {
        use load::ObjectKind::{Table, View};
        use load::plan::{CdcLayout, LoadMode};
        let mut wrong = Vec::new();
        for (layout, tag) in [
            (CdcLayout::LogAndView, "view"),
            (CdcLayout::BaseAndBuffer, "base"),
        ] {
            for (case, at_table, at_log) in [
                (
                    "object",
                    Some(if tag == "view" { View } else { Table }),
                    None,
                ),
                ("log", None, Some(Table)),
            ] {
                let dir = tempfile::tempdir().unwrap();
                let state = StateStore::open_in_memory().unwrap();
                let table = format!("inc_foreign_{tag}_{case}");
                let log = format!("{table}__changes");
                let plan = plan_of(&dir, &table, LoadMode::Incremental, layout);
                stage_incremental_run(&dir, "r-delta", "2026-08-21T00:00:00Z", Some("3"));
                let fake = || {
                    let mut f = load::tests::fake_loader(1);
                    if let Some(kind) = at_table {
                        f = f.with_kind(&table, kind);
                    }
                    if let Some(kind) = at_log {
                        f = f.with_kind(&log, kind);
                    }
                    f
                };
                let first = incremental_cycle(&dir, &state, &plan, "L1", fake());
                let second = incremental_cycle(&dir, &state, &plan, "L2", fake());
                let refused = |c: &Cycle| {
                    c.stopped_saying("no record of a successful rivet load")
                        && c.next == load::Ownership::Foreign
                        && c.exit == Some((5, Some("RIVET_LOAD_TARGET_NOT_RIVETS")))
                };
                if !(refused(&first) && refused(&second) && first.refusal == second.refusal) {
                    wrong.push((tag, case, first, second));
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "(layout, what stands there, cycles): {wrong:#?}"
        );
    }

    /// A whole-table load writes `<table>` only.
    #[test]
    fn a_whole_table_load_asks_about_the_table_and_never_about_the_log() {
        use load::ObjectKind::Table;
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let plan = staged_plan(&dir, "full_beside_log", load::plan::LoadMode::Full);
        let beside = load::tests::fake_loader(1).with_kind("full_beside_log__changes", Table);
        let landed = cycle(&dir, &state, &plan, "L1", beside);
        assert_eq!(
            (landed.refusal, landed.writes, landed.ledger, landed.next),
            (
                None,
                vec!["materialize full_beside_log".to_string()],
                vec!["success".to_string()],
                load::Ownership::Own
            ),
            "(refusal, warehouse writes, ledger, next ownership)"
        );

        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let plan = staged_plan(&dir, "full_foreign", load::plan::LoadMode::Full);
        let foreign = cycle(
            &dir,
            &state,
            &plan,
            "L1",
            load::tests::fake_loader(1).with_kind("full_foreign", Table),
        );
        assert_eq!(
            (foreign.refusal.as_deref(), foreign.exit, foreign.writes),
            (
                Some(
                    "refusing to overwrite `db.full_foreign`: it exists, and this state DB's \
                     load ledger has no record of a successful rivet load into it — it may hold \
                     someone else's data, or what a first rivet load that never completed left \
                     behind (such a load consumed no run, so the next load delivers its rows \
                     again). Drop or rename it, or load into another table"
                ),
                Some((5, Some("RIVET_LOAD_TARGET_NOT_RIVETS"))),
                vec![]
            ),
            "(refusal, (exit, code), warehouse writes)"
        );
    }

    /// Only a leg that landed in this load makes the names rivet's own for the next leg.
    #[test]
    fn a_landed_leg_and_only_that_makes_the_next_leg_rivets_own() {
        use load::Ownership::{Foreign, Own, Unreadable};
        assert_eq!(ownership_after_leg(0, Foreign), Foreign);
        assert_eq!(ownership_after_leg(0, Unreadable), Unreadable);
        assert_eq!(ownership_after_leg(1, Foreign), Own);
        assert_eq!(ownership_after_leg(2, Unreadable), Own);
    }

    /// A first load whose whole-table leg and deltas arrive together still lands both.
    #[test]
    fn a_first_load_lands_its_table_and_then_its_buffer_in_one_cycle() {
        use load::plan::{CdcLayout, LoadMode};
        let landed = |table: &str| {
            (
                None,
                vec![format!("materialize {table}"), format!("append {table}")],
                load::Ownership::Own,
            )
        };
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let plan = plan_of(
            &dir,
            "first_cdc_base",
            LoadMode::Cdc,
            CdcLayout::BaseAndBuffer,
        );
        stage_run(&dir, "r-base", "full");
        stage_run(&dir, "r-stream", "cdc");
        let c = cdc_base_cycle(&dir, &state, &plan, "L1", load::tests::fake_loader(1));
        assert_eq!((c.refusal, c.writes, c.next), landed("first_cdc_base"));

        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let plan = plan_of(
            &dir,
            "first_inc_base",
            LoadMode::Incremental,
            CdcLayout::BaseAndBuffer,
        );
        stage_incremental_run(&dir, "r-whole", "2026-08-21T00:00:00Z", None);
        stage_incremental_run(&dir, "r-delta", "2026-08-22T00:00:00Z", Some("3"));
        let c = incremental_cycle(&dir, &state, &plan, "L1", load::tests::fake_loader(1));
        assert_eq!((c.refusal, c.writes, c.next), landed("first_inc_base"));
    }

    /// The whole-table pass of an incremental base-and-buffer load stops on a stale buffer as a refusal, on every cycle.
    #[test]
    fn an_incremental_pass_over_a_stale_buffer_is_journaled_as_a_refusal_on_every_cycle() {
        use load::ObjectKind::Table;
        use load::plan::{CdcLayout, LoadMode};
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let (table, log) = ("inc_stale", "inc_stale__changes");
        let plan = plan_of(&dir, table, LoadMode::Incremental, CdcLayout::BaseAndBuffer);
        stage_incremental_run(&dir, "r-whole", "2026-08-21T00:00:00Z", None);
        loaded_before(&state, "db.inc_stale");
        let mut seen = Vec::new();
        for load_id in ["L1", "L2"] {
            let fake = load::tests::fake_loader(1)
                .with_kind(table, Table)
                .with_kind(log, Table)
                .with_rows(log, 5);
            let c = incremental_cycle(&dir, &state, &plan, load_id, fake);
            seen.push((
                c.typed,
                c.writes,
                c.ledger[0].clone(),
                c.refusal
                    .is_some_and(|m| m.contains("refusing to land a whole-table pass")),
            ));
        }
        let stop = (true, Vec::<String>::new(), "refused".to_string(), true);
        assert_eq!(seen, [stop.clone(), stop]);
    }

    /// A base name that is a VIEW stops every leg of a base-and-buffer load with the coded conflict before a write, on every cycle.
    #[test]
    fn a_view_under_the_base_name_stops_every_load_leg_with_the_coded_conflict() {
        use load::ObjectKind::View;
        use load::plan::{CdcLayout, LoadMode};
        let conflict = Some((5, Some("RIVET_LOAD_TARGET_OBJECT_CONFLICT")));
        type Leg = fn(&tempfile::TempDir, &StateStore, &load::plan::LoadPlan) -> Cycle;
        let legs: [(&str, Leg); 4] = [
            ("inc_whole", |d, s, p| {
                stage_incremental_run(d, "r1", "2026-08-21T00:00:00Z", None);
                incremental_cycle(
                    d,
                    s,
                    p,
                    "L1",
                    load::tests::fake_loader(1).with_kind(&p.table, View),
                )
            }),
            ("inc_delta", |d, s, p| {
                stage_incremental_run(d, "r1", "2026-08-22T00:00:00Z", Some("3"));
                incremental_cycle(
                    d,
                    s,
                    p,
                    "L1",
                    load::tests::fake_loader(1).with_kind(&p.table, View),
                )
            }),
            ("cdc_baseline", |d, s, p| {
                stage_run(d, "r1", "full");
                cdc_base_cycle(
                    d,
                    s,
                    p,
                    "L1",
                    load::tests::fake_loader(1).with_kind(&p.table, View),
                )
            }),
            ("cdc_stream", |d, s, p| {
                stage_run(d, "r1", "cdc");
                cdc_base_cycle(
                    d,
                    s,
                    p,
                    "L1",
                    load::tests::fake_loader(1).with_kind(&p.table, View),
                )
            }),
        ];
        for (table, run_leg) in legs {
            let mode = if table.starts_with("inc") {
                LoadMode::Incremental
            } else {
                LoadMode::Cdc
            };
            let dir = tempfile::tempdir().unwrap();
            let state = StateStore::open_in_memory().unwrap();
            let plan = plan_of(&dir, table, mode, CdcLayout::BaseAndBuffer);
            let c = run_leg(&dir, &state, &plan);
            assert!(
                c.stopped_saying("refusing to load `db.") && c.exit == conflict,
                "{table}: {c:#?}"
            );
            assert!(
                c.refusal
                    .as_deref()
                    .is_some_and(|m| m.contains("`load.layout: log_view`")),
                "{table}: the remedy is named: {c:#?}"
            );

            let dir = tempfile::tempdir().unwrap();
            let state = StateStore::open_in_memory().unwrap();
            let plan = plan_of(&dir, table, mode, CdcLayout::LogAndView);
            let c = run_leg(&dir, &state, &plan);
            assert!(
                c.exit != conflict,
                "{table}: the view of a changelog+view layout is not refused: {c:#?}"
            );
        }
    }

    /// A part that vanishes between the ledger read and the write stops the load as a refusal.
    #[test]
    fn a_stop_between_the_ledger_read_and_the_write_is_journaled_as_a_refusal() {
        use load::plan::{CdcLayout, LoadMode};
        let vanishing = |dir: &tempfile::TempDir, run: &str| {
            let part = dir.path().join(format!("p/part-{run}.parquet"));
            load::tests::fake_loader(1).probing(move |_| {
                let _ = std::fs::remove_file(&part);
            })
        };
        let mut seen = Vec::new();

        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let plan = plan_of(
            &dir,
            "gone_cdc_base",
            LoadMode::Cdc,
            CdcLayout::BaseAndBuffer,
        );
        stage_run(&dir, "r-base", "full");
        seen.push(cdc_base_cycle(
            &dir,
            &state,
            &plan,
            "L1",
            vanishing(&dir, "r-base"),
        ));

        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let plan = plan_of(
            &dir,
            "gone_cdc_stream",
            LoadMode::Cdc,
            CdcLayout::BaseAndBuffer,
        );
        stage_run(&dir, "r-stream", "cdc");
        seen.push(cdc_base_cycle(
            &dir,
            &state,
            &plan,
            "L1",
            vanishing(&dir, "r-stream"),
        ));

        for (table, run, cursor_low) in [
            ("gone_inc_whole", "r-whole", None),
            ("gone_inc_delta", "r-delta", Some("3")),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let state = StateStore::open_in_memory().unwrap();
            let plan = plan_of(&dir, table, LoadMode::Incremental, CdcLayout::BaseAndBuffer);
            stage_incremental_run(&dir, run, "2026-08-21T00:00:00Z", cursor_low);
            seen.push(incremental_cycle(
                &dir,
                &state,
                &plan,
                "L1",
                vanishing(&dir, run),
            ));
        }

        for c in &seen {
            assert!(
                c.stopped_saying("part") && c.next == load::Ownership::Foreign,
                "a stop before the write, journaled as one: {c:#?}"
            );
        }
        assert_eq!(seen.len(), 4);
    }

    /// The stale-buffer stop over a base this ledger never loaded is journaled as a refusal, and the base is still refused after the operator drops the buffer as the text says.
    #[test]
    fn a_stale_buffer_refusal_does_not_make_a_foreign_base_rivets_own() {
        use load::ObjectKind::Table;
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let (table, log, target) = (
            "stale_foreign",
            "stale_foreign__changes",
            "db.stale_foreign",
        );
        let mut plan = staged_plan(&dir, table, load::plan::LoadMode::Cdc);
        plan.layout = load::plan::CdcLayout::BaseAndBuffer;
        stage_run(&dir, "r-base", "full");
        let pk = ["id".to_string()];
        let run = |load_id: &'static str, fake: load::tests::FakeLoader| {
            let writes = fake.writes();
            let mut job = cdc_job(&dir, &state, &plan);
            job.load_id = load_id;
            job.loader = Box::new(fake);
            let err = load_one_cdc_base(job, &pk, false, Some(&state))
                .map(|_| ())
                .err();
            (
                err.as_ref().map(|e| format!("{e:#}")).unwrap_or_default(),
                writes.borrow().clone(),
                statuses(&state, target)[0].clone(),
                ownership_of(Some(&state), target, "load"),
            )
        };
        let buffered = load::tests::fake_loader(1)
            .with_kind(table, Table)
            .with_kind(log, Table)
            .with_rows(log, 5);
        let (_, writes, journaled, next) = run("L1", buffered);
        let dropped = load::tests::fake_loader(1).with_kind(table, Table);
        let (said, rewrites, _, after) = run("L2", dropped);
        let second = match said.contains("no record of a successful rivet load") {
            true => "refused as foreign".to_string(),
            false => format!("not refused as foreign: {said:?}, wrote {rewrites:?}"),
        };
        assert_eq!(
            (journaled.as_str(), next, writes, second.as_str(), after),
            (
                "refused",
                load::Ownership::Foreign,
                vec![],
                "refused as foreign",
                load::Ownership::Foreign
            ),
            "(cycle-1 ledger status, ownership after it, cycle-1 writes, cycle-2 outcome, \
             ownership after cycle 2)"
        );
    }

    /// The same stop over rivet's own base is journaled as a refusal too, and repeats.
    #[test]
    fn a_stale_buffer_over_rivets_own_base_is_journaled_as_a_refusal_on_every_cycle() {
        use load::ObjectKind::Table;
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let (table, log, target) = ("stale_own", "stale_own__changes", "db.stale_own");
        let mut plan = staged_plan(&dir, table, load::plan::LoadMode::Cdc);
        plan.layout = load::plan::CdcLayout::BaseAndBuffer;
        stage_run(&dir, "r-base", "full");
        let mut earlier = ctx(&state, "L0");
        earlier.target_fqtn = target;
        earlier.record_success(&["r-0".to_string()], 1);
        let pk = ["id".to_string()];
        let mut seen = Vec::new();
        for load_id in ["L1", "L2"] {
            let fake = load::tests::fake_loader(1)
                .with_kind(table, Table)
                .with_kind(log, Table)
                .with_rows(log, 5);
            let writes = fake.writes();
            let mut job = cdc_job(&dir, &state, &plan);
            job.load_id = load_id;
            job.loader = Box::new(fake);
            let err = load_one_cdc_base(job, &pk, false, Some(&state))
                .map(|_| ())
                .unwrap_err();
            seen.push((
                format!("{err:#}").contains("refusing to land a whole-table pass"),
                err.is::<load::Refused>(),
                writes.borrow().clone(),
                statuses(&state, target)[0].clone(),
            ));
        }
        let stop = (true, true, Vec::<String>::new(), "refused".to_string());
        assert_eq!(
            seen,
            [stop.clone(), stop],
            "per cycle (the stale-buffer text, typed, warehouse writes, newest ledger row)"
        );
    }

    /// A load that dies at the probe deciding ownership leaves no row claiming the table, so the next load still refuses it.
    #[test]
    fn a_load_killed_while_asking_about_a_foreign_table_leaves_it_foreign() {
        use load::ObjectKind::Table;
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let (table, target) = ("killed_full", "db.killed_full");
        let plan = staged_plan(&dir, table, load::plan::LoadMode::Full);
        let dying = load::tests::fake_loader(1)
            .with_kind(table, Table)
            .probing(|_| panic!("KILL: the process dies at the ownership probe"));
        let killed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cycle(&dir, &state, &plan, "L1", dying)
        }));
        let payload = killed.expect_err("the load was killed, it did not return");
        assert!(
            payload
                .downcast_ref::<&str>()
                .is_some_and(|s| s.starts_with("KILL")),
            "the kill, not another panic"
        );
        let left = (
            statuses(&state, target),
            ownership_of(Some(&state), target, "load"),
        );
        let second = cycle(
            &dir,
            &state,
            &plan,
            "L2",
            load::tests::fake_loader(1).with_kind(table, Table),
        );
        let refused_again = second
            .refusal
            .is_some_and(|m| m.contains("no record of a successful rivet load"));
        assert_eq!(
            (left, refused_again, second.writes, second.next),
            (
                (vec![], load::Ownership::Foreign),
                true,
                vec![],
                load::Ownership::Foreign
            ),
            "((ledger rows the killed load left, ownership the next load reads), cycle 2 \
             refused as foreign, cycle-2 writes, ownership after cycle 2)"
        );
    }

    /// A closing ledger write that fails after a foreign refusal leaves no row claiming the table either.
    #[test]
    fn a_failed_closing_write_after_a_foreign_refusal_leaves_the_table_foreign() {
        use load::ObjectKind::Table;
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let table = "unclosed_full";
        let plan = staged_plan(&dir, table, load::plan::LoadMode::Full);
        for when in ["INSERT", "UPDATE"] {
            state.exec_for_test(&format!(
                "CREATE TRIGGER closing_{when} BEFORE {when} ON load_run \
                 WHEN NEW.status <> 'writing' \
                 BEGIN SELECT RAISE(ABORT, 'state backend unavailable'); END"
            ));
        }
        let mut seen = Vec::new();
        for load_id in ["L1", "L2"] {
            let c = cycle(
                &dir,
                &state,
                &plan,
                load_id,
                load::tests::fake_loader(1).with_kind(table, Table),
            );
            let refused = c
                .refusal
                .is_some_and(|m| m.contains("no record of a successful rivet load"));
            seen.push((refused, c.writes, c.ledger, c.next));
        }
        let stop = (
            true,
            Vec::<String>::new(),
            Vec::<String>::new(),
            load::Ownership::Foreign,
        );
        assert_eq!(
            seen,
            [stop.clone(), stop],
            "per cycle (refused as foreign, warehouse writes, ledger rows, next ownership)"
        );
    }

    #[test]
    fn record_failed_logs_a_failed_audit_row() {
        let s = StateStore::open_in_memory().unwrap();
        ctx(&s, "L1").record_failed(&["r1".into()]);
        let loads = s.recent_loads(Some(TARGET), 10).unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(loads[0].status, "failed");
        assert_eq!(loads[0].rows_loaded, 0);
    }

    /// Per-table fault isolation.
    #[test]
    fn several_load_failures_aggregate_instead_of_stopping_at_the_first() {
        let failures: Vec<anyhow::Error> = vec![
            anyhow::anyhow!("boom alpha").context("load 'alpha'"),
            anyhow::anyhow!("boom beta").context("load 'beta'"),
            anyhow::anyhow!("boom gamma").context("load 'gamma'"),
        ];
        let text = format!(
            "{:#}",
            aggregate_load_failures(failures).expect("three failures must aggregate to an error")
        );
        assert!(
            text.contains("3 load(s) failed"),
            "the aggregate must say how many failed; got: {text}"
        );
        for t in ["alpha", "beta", "gamma"] {
            assert!(
                text.contains(t),
                "every failed table must be named — a table missing from the aggregate is one \
                 an operator never learns about; got: {text}"
            );
        }

        let one = format!(
            "{:#}",
            aggregate_load_failures(vec![anyhow::anyhow!("boom solo").context("load 'solo'")])
                .expect("one failure is still an error")
        );
        assert!(one.contains("solo"), "got: {one}");
        assert!(
            !one.contains("load(s) failed"),
            "a single failure must surface as itself, not as a 1-of-1 aggregate; got: {one}"
        );

        assert!(
            aggregate_load_failures(Vec::new()).is_none(),
            "an empty failure set must not manufacture an error"
        );
    }

    #[test]
    fn record_is_a_noop_without_a_state_store() {
        let c = LoadCtx {
            source_ident: String::new(),
            active_at_fetch: None,
            marker_active: Default::default(),
            state: None,
            load_id: "L1",
            export_name: "orders",
            target_fqtn: TARGET,
            warehouse: "bigquery",
            mode: load::plan::LoadMode::Full,
            source_prefix: "gs://b/p/",
        };
        c.record_success(&["r1".into()], 3);
        c.record_skip();
        c.record_failed(&["r2".into()]);
    }

    /// Commit `run` the way an extract does: its part, then `write_manifest` (run copy, canonical, `_SUCCESS`).
    fn commit_run(prefix: &std::path::Path, run: &str, part: &str) {
        let dest =
            crate::destination::local::LocalDestination::new(&crate::config::DestinationConfig {
                destination_type: crate::config::DestinationType::Local,
                path: Some(prefix.display().to_string()),
                ..Default::default()
            })
            .expect("a local destination");
        std::fs::create_dir_all(prefix).unwrap();
        std::fs::write(prefix.join(part), b"rows").unwrap();
        let manifest = super::live_only_decisions::success_manifest(run, part);
        crate::pipeline::manifest_writer::write_manifest(&dest, &manifest).expect("commit");
    }

    fn present(prefix: &std::path::Path, name: &str) -> bool {
        prefix.join(name).exists()
    }

    fn canonical_run(prefix: &std::path::Path) -> Option<String> {
        let bytes = std::fs::read(prefix.join("manifest.json")).ok()?;
        serde_json::from_slice::<crate::manifest::RunManifest>(&bytes)
            .ok()
            .map(|m| m.run_id)
    }

    /// An extract that commits its canonical `manifest.json` and `_SUCCESS` while the warehouse jobs run keeps both through `cleanup_source`.
    #[test]
    fn a_run_committed_during_the_jobs_keeps_its_canonical_markers_through_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let mut plan = cdc_plan(&dir, "orders_cleanup_race");
        plan.mode = load::plan::LoadMode::Full;
        plan.load.cleanup_source = true;
        plan.specs = load::tests::spec_ok();
        let prefix = dir.path().join("p");
        finished_row(&state, "r1");
        commit_run(&prefix, "r1", "a.parquet");
        let racing = prefix.clone();
        let armed = std::rc::Rc::new(std::cell::Cell::new(false));
        let fired = std::rc::Rc::new(std::cell::Cell::new(0u32));
        let (gate, seen) = (armed.clone(), fired.clone());
        let fake = load::tests::fake_loader(1).writing(move |_| {
            if gate.get() {
                seen.set(seen.get() + 1);
                commit_run(&racing, "r2", "b.parquet");
            }
        });
        let mut job = cdc_job(&dir, &state, &plan);
        job.mode = load::plan::LoadMode::Full;
        job.loader = Box::new(fake);
        execute_load(
            job,
            |_| {},
            |loader, store, inputs, _| {
                armed.set(true);
                full_leg(&plan, Some(&state), loader, store, inputs)
            },
            |_, _| {},
        )
        .expect("the load succeeds")
        .expect("r1 was staged");
        assert!(
            fired.get() > 0,
            "fixture is inert: r2 never committed during the load"
        );
        assert!(!present(&prefix, "a.parquet"), "r1's part is cleaned");
        assert!(
            !present(&prefix, "manifest-r1.json"),
            "r1's manifest copy is cleaned"
        );
        assert!(
            present(&prefix, "b.parquet"),
            "r2's part is not the load's to delete"
        );
        assert!(
            present(&prefix, "manifest-r2.json"),
            "r2's manifest copy survives"
        );
        assert_eq!(
            canonical_run(&prefix).as_deref(),
            Some("r2"),
            "r2's canonical manifest.json survives the cleanup of r1"
        );
        assert!(
            present(&prefix, "_SUCCESS"),
            "r2's _SUCCESS survives the cleanup of r1"
        );
    }

    /// `cleanup_source` over `runs` (each `(id, part)`) with `writing` excluded, the way a load's cleanup runs it.
    fn clean_prefix(dir: &tempfile::TempDir, runs: &[(&str, &str)], writing: &[&str]) -> bool {
        let mut plan = cdc_plan(dir, "orders_cleanup_neighbours");
        plan.load.cleanup_source = true;
        let store = crate::destination::gcs::GcsStore::open_fs(dir.path().to_str().unwrap())
            .expect("an fs store");
        let census: Vec<_> = runs
            .iter()
            .map(|(id, part)| {
                (
                    format!("p/manifest-{id}.json"),
                    load::tests::run_manifest(id, part),
                )
            })
            .collect();
        let writing = writing.iter().map(|w| w.to_string()).collect();
        let (cleanup, _lease) = cleanup_target_leased(&plan, &store, None, &census, Some(&writing));
        load::staging::maybe_cleanup(cleanup.as_ref().map(|(s, c)| (*s, c)))
    }

    fn left_in(prefix: &std::path::Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(prefix)
            .map(|d| {
                d.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    /// A cleaned run's own canonical pair still goes with it.
    #[test]
    fn a_cleaned_runs_own_canonical_markers_still_go() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("p");
        commit_run(&prefix, "r1", "a.parquet");
        assert!(clean_prefix(&dir, &[("r1", "a.parquet")], &[]));
        assert_eq!(left_in(&prefix), Vec::<String>::new());
    }

    /// A canonical pointer that already names another run at plan time is not the load's to delete.
    #[test]
    fn a_canonical_naming_another_run_stays_with_its_marker() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("p");
        commit_run(&prefix, "r1", "a.parquet");
        commit_run(&prefix, "r2", "b.parquet");
        assert!(clean_prefix(&dir, &[("r1", "a.parquet")], &[]));
        assert_eq!(
            left_in(&prefix),
            ["_SUCCESS", "b.parquet", "manifest-r2.json", "manifest.json"]
        );
    }

    /// A canonical manifest that turns unreadable after the plan is not told to name a cleaned run, so it and its marker stay.
    #[test]
    fn a_canonical_unreadable_at_delete_time_stays_with_its_marker() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("p");
        commit_run(&prefix, "r1", "a.parquet");
        let mut plan = cdc_plan(&dir, "orders_cleanup_unreadable");
        plan.load.cleanup_source = true;
        let store = crate::destination::gcs::GcsStore::open_fs(dir.path().to_str().unwrap())
            .expect("an fs store");
        let census = vec![(
            "p/manifest-r1.json".to_string(),
            load::tests::run_manifest("r1", "a.parquet"),
        )];
        let (cleanup, _lease) =
            cleanup_target_leased(&plan, &store, None, &census, Some(&Default::default()));
        std::fs::write(prefix.join("manifest.json"), b"not json").unwrap();
        assert!(load::staging::maybe_cleanup(
            cleanup.as_ref().map(|(s, c)| (*s, c))
        ));
        assert_eq!(left_in(&prefix), ["_SUCCESS", "manifest.json"]);
    }

    /// A run still writing keeps its parts, its copy and the canonical pair that names it.
    #[test]
    fn a_run_still_writing_keeps_everything_it_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("p");
        commit_run(&prefix, "r1", "a.parquet");
        commit_run(&prefix, "r2", "b.parquet");
        assert!(clean_prefix(
            &dir,
            &[("r1", "a.parquet"), ("r2", "b.parquet")],
            &["r2"]
        ));
        assert_eq!(
            left_in(&prefix),
            ["_SUCCESS", "b.parquet", "manifest-r2.json", "manifest.json"]
        );
    }

    /// A legacy directory whose only record is the canonical manifest is cleaned whole.
    #[test]
    fn a_legacy_canonical_only_directory_is_cleaned_whole() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("p");
        commit_run(&prefix, "r1", "a.parquet");
        std::fs::remove_file(prefix.join("manifest-r1.json")).unwrap();
        let mut plan = cdc_plan(&dir, "orders_cleanup_legacy");
        plan.load.cleanup_source = true;
        let store = crate::destination::gcs::GcsStore::open_fs(dir.path().to_str().unwrap())
            .expect("an fs store");
        let census = vec![(
            "p/manifest.json".to_string(),
            load::tests::run_manifest("r1", "a.parquet"),
        )];
        let (cleanup, _lease) =
            cleanup_target_leased(&plan, &store, None, &census, Some(&Default::default()));
        assert!(load::staging::maybe_cleanup(
            cleanup.as_ref().map(|(s, c)| (*s, c))
        ));
        assert_eq!(left_in(&prefix), Vec::<String>::new());
    }

    /// The stateless path closes the same window: a run that commits after the keys were planned keeps its pair.
    #[test]
    fn a_stateless_cleanup_keeps_a_pair_committed_after_the_plan() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("p");
        commit_run(&prefix, "r1", "a.parquet");
        let mut plan = cdc_plan(&dir, "orders_cleanup_stateless");
        plan.load.cleanup_source = true;
        let store = crate::destination::gcs::GcsStore::open_fs(dir.path().to_str().unwrap())
            .expect("an fs store");
        let census = vec![(
            "p/manifest-r1.json".to_string(),
            load::tests::run_manifest("r1", "a.parquet"),
        )];
        let (cleanup, _lease) =
            cleanup_target_leased(&plan, &store, None, &census, Some(&Default::default()));
        commit_run(&prefix, "r2", "b.parquet");
        assert!(load::staging::maybe_cleanup(
            cleanup.as_ref().map(|(s, c)| (*s, c))
        ));
        assert_eq!(
            left_in(&prefix),
            ["_SUCCESS", "b.parquet", "manifest-r2.json", "manifest.json"]
        );
    }
}

/// The DECISIONS the live-only load orchestrator makes, graded offline.
#[cfg(test)]
mod live_only_decisions {
    use super::*;
    use crate::destination::gcs::GcsStore;
    use crate::load::pin::{
        late_runs_refusal, late_runs_stop, respelled_refusal, respelled_stop,
        runs_loaded_with_the_pin, runs_older_than, skipped_runs_note,
    };
    use crate::load::staging::{
        CleanupVerdict, cleanup_target, cleanup_verdict, ledger_says_active, prefix_has_active_run,
        prefix_is_active,
    };
    use load::plan::{CdcLayout, LoadMode, LoadPlan};

    /// `ledger_status`: a `Refused` stop, however deep under context, is `refused`; anything else is `failed`.
    #[test]
    fn ledger_status_tells_a_stop_before_the_write_from_a_failure() {
        let stop = load::refused(
            crate::error::codes::LOAD_TARGET_OBJECT_CONFLICT,
            "refusing to overwrite `p.d.t`".into(),
        );
        assert_eq!(ledger_status(&stop), "refused");
        let wrapped = load::before_write::<()>(Err(anyhow::anyhow!("no partition statistics")))
            .unwrap_err()
            .context("loading orders");
        assert_eq!(ledger_status(&wrapped), "refused");
        assert_eq!(
            format!("{wrapped:#}"),
            "loading orders: no partition statistics",
            "the message keeps its chain"
        );
        assert_eq!(
            ledger_status(&anyhow::anyhow!("count validation failed")),
            "failed"
        );
    }

    /// `rivet load` and `rivet compact` cannot collide in the ledger under one run id.
    #[test]
    fn a_load_and_a_compact_of_one_table_under_one_run_id_are_two_ledger_rows() {
        let load = ledger_load_id("run-7", "load", "orders");
        let compact = ledger_load_id("run-7", "compact", "orders");
        assert_ne!(
            load, compact,
            "one key for both means the second command silently replaces the first's \
             audit row"
        );
        assert!(
            load.starts_with("run-7:") && compact.starts_with("run-7:"),
            "the run id must stay the PREFIX — the gate scopes with LIKE '<run-id>%': \
             {load} / {compact}"
        );
        assert_ne!(
            ledger_load_id("run-7", "load", "orders"),
            ledger_load_id("run-7", "load", "invoices")
        );
    }

    /// A load that failed after a leg landed is `failed`, never `refused`.
    #[test]
    fn a_stop_after_a_leg_landed_is_recorded_as_a_failure_not_a_refusal() {
        let stop = load::refused(
            crate::error::codes::LOAD_TARGET_OBJECT_CONFLICT,
            "refusing to append".into(),
        );

        assert_eq!(
            closing_status(&stop, &[]),
            "refused",
            "nothing landed: the stop is a stop, and the table stays foreign"
        );
        assert_eq!(
            closing_status(&stop, &["run-a".to_string()]),
            "failed",
            "a leg landed, so the warehouse was written"
        );
        let boom = anyhow::anyhow!("count validation failed");
        assert_eq!(closing_status(&boom, &[]), "failed");
        assert_eq!(closing_status(&boom, &["run-a".to_string()]), "failed");
    }

    /// A `LoadCtx` over an arbitrary target — the sibling helper in the other test module is pinned to one table, and the ownership question is per-table.
    fn ctx_for<'a>(state: &'a StateStore, load_id: &'a str, target: &'a str) -> LoadCtx<'a> {
        LoadCtx {
            state: Some(state),
            load_id,
            export_name: "orders",
            target_fqtn: target,
            warehouse: "bigquery",
            mode: LoadMode::Cdc,
            source_prefix: "gs://b/exports/orders/",
            source_ident: String::new(),
            active_at_fetch: Some(Default::default()),
            marker_active: Default::default(),
        }
    }

    /// A load whose crash marker the ledger cannot take stops before the write.
    #[test]
    fn an_unrecordable_writing_marker_stops_the_load_before_the_write() {
        let state = StateStore::open_in_memory().unwrap();
        let ctx = ctx_for(&state, "load-cut", "p.d.orders");
        state.exec_for_test("DROP TABLE load_run");
        assert!(
            ctx.record_writing().is_err(),
            "a marker the ledger could not take must stop the load"
        );
    }

    /// A crash marker is on the record but owns nothing; the first success makes the table rivet's own.
    #[test]
    fn a_crash_before_the_first_success_leaves_the_table_unowned() {
        let state = StateStore::open_in_memory().unwrap();
        let target = "p.d.orders";
        let ctx = ctx_for(&state, "load-crash", target);

        ctx.record_writing().unwrap();
        assert!(
            !state.has_landed_load(target).unwrap(),
            "a load that never completed consumed no run and owns nothing"
        );
        assert_eq!(
            state.recent_loads(Some(target), 10).unwrap()[0].status,
            "writing",
            "the crash is on the record"
        );

        ctx.record(&["r1".to_string()], 7, "success");
        let loads = state.recent_loads(Some(target), 10).unwrap();
        assert_eq!(loads.len(), 1, "the marker was replaced");
        assert_eq!(loads[0].status, "success");
        assert!(state.has_landed_load(target).unwrap());
    }

    /// Neither a refused nor a failed first load makes a foreign table rivet's own; a success does.
    #[test]
    fn a_refused_load_does_not_make_a_foreign_table_rivets_own() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        let prefix = "gs://b/base";
        write_at(&dir, "base/part-0.parquet", b"x");
        let m = success_manifest("run-1", "part-0.parquet");
        write_at(
            &dir,
            "base/manifest-run-1.json",
            &serde_json::to_vec(&m).unwrap(),
        );
        let state = StateStore::open_in_memory().unwrap();
        let plan = plan_at(LoadMode::Full, prefix);
        let target = "p.d.orders";
        let ownership = |state: &StateStore| {
            prepare_load(&store, &plan, Some(state), target, false)
                .unwrap()
                .expect("the run is unloaded")
                .ownership
        };
        assert_eq!(ownership(&state), load::Ownership::Foreign);

        let ctx = LoadCtx {
            state: Some(&state),
            load_id: "load-1",
            export_name: "orders",
            target_fqtn: target,
            warehouse: "bigquery",
            mode: LoadMode::Full,
            source_prefix: prefix,
            source_ident: String::new(),
            active_at_fetch: Some(Default::default()),
            marker_active: Default::default(),
        };
        let stop = load::refused(
            crate::error::codes::LOAD_TARGET_OBJECT_CONFLICT,
            "refusing to overwrite".into(),
        );
        ctx.record(&["run-1".to_string()], 0, ledger_status(&stop));
        assert_eq!(
            ownership(&state),
            load::Ownership::Foreign,
            "the second cycle refuses again"
        );
        assert_eq!(
            state.recent_loads(Some(target), 10).unwrap()[0].status,
            "refused",
            "and the stop is on the record"
        );

        ctx.record_failed(&["run-1".to_string()]);
        assert_eq!(
            ownership(&state),
            load::Ownership::Foreign,
            "a first load that failed owns nothing, so a table that appeared since is refused"
        );

        ctx.record(&["run-1".to_string()], 1, "success");
        assert_eq!(ownership(&state), load::Ownership::Own);
    }

    /// The BUCKET's liveness markers keep a still-writing run out of the consumed set when the LEDGER answers empty without erroring.
    #[test]
    fn a_run_the_bucket_says_is_live_is_not_consumed_when_the_ledger_answers_empty() {
        let state = StateStore::open_in_memory().unwrap();
        let target = "p.d.orders";
        let ctx = LoadCtx {
            state: Some(&state),
            load_id: "load-marker",
            export_name: "orders",
            target_fqtn: target,
            warehouse: "bigquery",
            mode: LoadMode::Cdc,
            source_prefix: "gs://b/exports/orders/",
            source_ident: String::new(),
            active_at_fetch: Some(Default::default()),
            marker_active: ["run-live".to_string()].into_iter().collect(),
        };
        ctx.record(
            &["run-done".to_string(), "run-live".to_string()],
            2,
            "success",
        );

        let loaded = state.loaded_source_run_ids(target).unwrap();
        assert!(
            loaded.contains("run-done"),
            "a terminal run must still be recorded, or every load re-reads it: {loaded:?}"
        );
        assert!(
            !loaded.contains("run-live"),
            "the bucket says `run-live` is still writing — recording it consumed strands \
             every part it flushes after this load: {loaded:?}"
        );
    }

    /// The identity guard reads the SAME population the recorder writes.
    #[test]
    fn an_idle_drain_leaves_nothing_to_buffer() {
        assert_eq!(super::buffer_uris(Vec::new()), None);
        assert_eq!(
            super::buffer_uris(vec!["gs://b/p/a.parquet".to_string()]),
            Some(vec!["gs://b/p/a.parquet".to_string()])
        );
    }

    /// A stream named `users` over table `t` writes `export_name = t` under family `users`.
    #[test]
    fn a_stream_drain_is_never_taken_for_a_baseline_leg() {
        let mut drain = success_manifest("r1", "cdc-000.parquet");
        drain.export_name = "t".into();
        drain.export_family = "users".into();
        assert!(!super::is_baseline_leg(&drain), "mode: cdc is the stream");
        let mut leg = drain.clone();
        leg.mode = "batch".into();
        assert!(
            super::is_baseline_leg(&leg),
            "a batch run under the prefix is a leg"
        );
    }

    #[test]
    fn only_the_runs_older_than_the_pin_are_compared() {
        let runs: Vec<(String, String)> = ["r3", "r2", "r1"]
            .iter()
            .map(|r| (String::new(), r.to_string()))
            .collect();
        let ids = |v: &[(String, String)]| v.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>();
        assert_eq!(ids(runs_older_than(&runs, "r2")), ["r1"]);
        assert_eq!(ids(runs_older_than(&runs, "r3")), ["r2", "r1"]);
        assert!(runs_older_than(&runs, "r1").is_empty());
        assert!(runs_older_than(&runs, "gone").is_empty());
    }

    #[test]
    fn an_empty_prefix_is_a_typo_warning_only_when_nothing_was_ever_loaded_from_it() {
        let none = std::collections::HashSet::new();
        let loaded: std::collections::HashSet<String> = ["r1".to_string()].into();
        assert!(
            super::empty_prefix_note("t", "gs://b/p/", true, &none)
                .is_some_and(|n| n.contains("check the prefix for typos"))
        );
        assert_eq!(
            super::empty_prefix_note("t", "gs://b/p/", true, &loaded),
            None,
            "loaded then cleaned by cleanup_source is not a typo"
        );
        assert_eq!(
            super::empty_prefix_note("t", "gs://b/p/", false, &none),
            None
        );
    }

    #[test]
    fn an_empty_newest_run_empties_a_full_load_and_is_a_no_op_for_an_append() {
        assert!(
            !super::nothing_to_load(true, true),
            "the table is now empty"
        );
        assert!(super::nothing_to_load(false, true));
        assert!(!super::nothing_to_load(false, false));
        assert!(!super::nothing_to_load(true, false));
    }

    #[test]
    fn only_unloaded_runs_of_an_append_load_are_compared_with_the_pin() {
        use load::plan::LoadMode;
        let runs: Vec<(String, String)> = ["r3", "r2", "r1"]
            .iter()
            .map(|r| (String::new(), r.to_string()))
            .collect();
        let loaded: std::collections::HashSet<String> = ["r1".to_string()].into();
        assert_eq!(
            runs_loaded_with_the_pin("load", LoadMode::Incremental, &runs, "r3", &loaded),
            ["r2"],
            "r1 already landed: its spelling cannot load NULL any more"
        );
        assert_eq!(
            runs_loaded_with_the_pin("load", LoadMode::Cdc, &runs, "r3", &Default::default()),
            ["r2", "r1"]
        );
        assert!(
            runs_loaded_with_the_pin("load", LoadMode::Full, &runs, "r3", &Default::default())
                .is_empty(),
            "a full load reads only the newest run"
        );
        assert!(
            runs_loaded_with_the_pin("compact", LoadMode::Cdc, &runs, "r3", &Default::default())
                .is_empty(),
            "compact merges the landed buffer and reads no pending run"
        );
    }

    #[test]
    fn the_respelling_refusal_names_each_run_and_both_spellings() {
        assert_eq!(respelled_refusal("orders", "r2", &[]), None);
        let msg = respelled_refusal(
            "orders",
            "r2",
            &[("r1".into(), "\u{441}ity".into(), "city".into())],
        )
        .expect("a respelled run is refused");
        assert!(
            msg.starts_with("load [orders]: run r1 wrote `\u{441}ity` where run r2 writes `city`."),
            "{msg}"
        );
        assert!(msg.contains("Nothing was loaded"), "{msg}");
    }

    /// The stops the load glue raises name their codes.
    #[test]
    fn each_load_glue_stop_carries_its_code() {
        let code = |e: anyhow::Error| {
            (
                crate::error::error_code(&e),
                crate::error::classify_exit(&e),
            )
        };
        let respelled = [("r1".into(), "\u{441}ity".into(), "city".into())];
        assert_eq!(
            code(respelled_stop("orders", "r2", &respelled).unwrap_err()),
            (Some("RIVET_LOAD_COLUMN_RESPELLED"), 5)
        );
        assert!(respelled_stop("orders", "r2", &[]).is_ok());
        let pinned = success_manifest("r2", "p2.parquet");
        let mut late = success_manifest("r3", "p3.parquet");
        late.finished_at = "2026-08-21T00:01:00.250Z".into();
        let pin = ("r2".to_string(), "2026-08-21T00:01:00Z".to_string());
        let runs: Vec<_> = [pinned, late]
            .into_iter()
            .map(|m| (m.run_id.clone(), m))
            .collect();
        assert_eq!(
            code(late_runs_stop("orders", &runs, Some(&pin)).unwrap_err()).0,
            Some("RIVET_LOAD_RUN_LANDED_DURING_LOAD")
        );
        for (e, want) in [
            (
                rebaseline_stop("p.d.t", load::cdc::Warehouse::BigQuery),
                "RIVET_LOAD_REBASELINE_OVER_CHANGES",
            ),
            (
                snapshot_over_full_table_stop("p.d.t", load::cdc::Warehouse::BigQuery),
                "RIVET_LOAD_TARGET_OBJECT_CONFLICT",
            ),
        ] {
            assert!(e.is::<load::Refused>(), "{want}");
            assert_eq!(ledger_status(&e), "refused");
            assert_eq!(code(e), (Some(want), 5));
        }
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let key = format!("p.d.busy_{}", std::process::id());
        let _held = take_table_lease(Some(&state), &key).unwrap();
        let Err(busy) = take_table_lease(Some(&state), &key) else {
            panic!("a held table refuses a second taker");
        };
        assert_eq!(code(busy), (Some("RIVET_LOAD_TABLE_BUSY"), 2));
        let mut keyless = load::plan::test_plan(LoadMode::Cdc, "gs://b/p/");
        keyless.pk.clear();
        assert_eq!(
            code(require_pk(&keyless, "cdc").unwrap_err()),
            (Some("RIVET_LOAD_PRIMARY_KEY_MISSING"), 1)
        );
        assert_eq!(
            code(no_outcome_error("load", "t")),
            (Some("RIVET_INTERNAL_LOAD_INVARIANT"), 6)
        );
    }

    #[test]
    fn the_pin_names_the_newer_runs_it_passed_over_and_stays_quiet_otherwise() {
        assert_eq!(skipped_runs_note("orders", "r1", &[]), None);
        let note = skipped_runs_note("orders", "r1", &["r3", "r2"]).expect("named");
        assert!(note.contains("orders") && note.contains("r1"), "{note}");
        assert!(
            note.contains("2 newer run(s)") && note.contains("r3, r2"),
            "{note}"
        );
    }

    /// An unreadable ledger cannot vouch that no other source loaded the table: refuse, never skip the check.
    #[test]
    fn an_unreadable_source_identity_ledger_refuses_the_load() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        let prefix = "gs://b/base";
        write_at(&dir, "base/part-1.parquet", b"x");
        let good = success_manifest("run-1", "part-1.parquet");
        write_at(
            &dir,
            "base/manifest-run-1.json",
            &serde_json::to_vec(&good).unwrap(),
        );
        let state = StateStore::open_in_memory().unwrap();
        state.exec_for_test("DROP TABLE loaded_source_run");
        let plan = plan_at(LoadMode::Full, prefix);
        let err = prepare_load(&store, &plan, Some(&state), "p.d.orders", false)
            .err()
            .expect("an unreadable ledger must refuse, not skip the cross-source check");
        assert!(
            format!("{err:#}").contains("could not say which source last loaded"),
            "{err:#}"
        );
    }

    /// The "not one is Success" note is for a prefix that holds manifests and no Success run.
    #[test]
    fn the_no_success_note_is_for_manifests_without_one_success() {
        let note = |seen, ok| no_success_note("orders", "gs://b/p", seen, ok);
        let said = note(3, 0).expect("three aborted runs");
        assert!(said.contains("3 manifest(s) under gs://b/p and NOT ONE is Success"));
        assert!(said.starts_with("  load [orders]:"));
        assert!(note(1, 0).is_some());
        assert_eq!(note(0, 0), None, "an empty prefix has its own note");
        assert_eq!(
            note(3, 1),
            None,
            "one Success run: the selection is up to date"
        );
        assert_eq!(note(1, 1), None);
    }

    /// An append load whose ledger cannot say which runs are loaded is refused for that reason, before anything else is asked of the ledger.
    #[test]
    fn an_append_load_is_refused_for_the_unreadable_loaded_set_itself() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        let prefix = "gs://b/base";
        write_at(&dir, "base/part-1.parquet", b"x");
        let good = success_manifest("run-1", "part-1.parquet");
        write_at(
            &dir,
            "base/manifest-run-1.json",
            &serde_json::to_vec(&good).unwrap(),
        );
        let state = StateStore::open_in_memory().unwrap();
        state.exec_for_test("DROP TABLE loaded_source_run");
        let said = |mode| {
            let err = prepare_load(
                &store,
                &plan_at(mode, prefix),
                Some(&state),
                "p.d.orders",
                false,
            )
            .err()
            .expect("an unreadable ledger refuses the load");
            format!("{err:#}")
        };
        for mode in [LoadMode::Incremental, LoadMode::Cdc] {
            let said = said(mode);
            assert!(
                said.contains("cannot read which extraction runs are already loaded")
                    && said.contains("refusing an append load"),
                "{mode:?}: {said}"
            );
        }
        assert!(
            !said(LoadMode::Full).contains("refusing an append load"),
            "a full load overwrites and does not consult the loaded set"
        );
    }

    #[test]
    fn a_running_marker_does_not_impersonate_the_source_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        let prefix = "gs://b/base";
        write_at(&dir, "base/part-1.parquet", b"x");
        let good = success_manifest("run-1", "part-1.parquet");
        write_at(
            &dir,
            "base/manifest-run-1.json",
            &serde_json::to_vec(&good).unwrap(),
        );
        let mut marker = success_manifest("run-0", "part-0.parquet");
        marker.status = crate::manifest::ManifestStatus::Running;
        marker.source.schema = None;
        marker.source.table = None;
        marker.parts.clear();
        marker.part_count = 0;
        marker.row_count = 0;
        write_at(
            &dir,
            "base/manifest-run-0.json",
            &serde_json::to_vec(&marker).unwrap(),
        );

        let state = StateStore::open_in_memory().unwrap();
        let target = "p.d.orders";
        LoadCtx {
            state: Some(&state),
            load_id: "load-0",
            export_name: "orders",
            target_fqtn: target,
            warehouse: "bigquery",
            mode: LoadMode::Cdc,
            source_prefix: prefix,
            source_ident: crate::manifest::identity_source(&good),
            active_at_fetch: Some(Default::default()),
            marker_active: Default::default(),
        }
        .record(&["run-9".to_string()], 1, "success");

        let plan = plan_at(LoadMode::Cdc, prefix);
        let prepared = prepare_load(&store, &plan, Some(&state), target, false)
            .expect("a live run's marker is not another source");
        assert!(prepared.is_some(), "run-1 is unloaded and must be selected");
    }

    /// A resolved plan, so a test can vary the ONE field it is about.
    fn plan_at(mode: LoadMode, gcs_prefix: &str) -> LoadPlan {
        load::plan::test_plan(mode, gcs_prefix)
    }

    /// An fs-backed store over `dir`, standing in for the bucket.
    fn fs_store(dir: &tempfile::TempDir) -> GcsStore {
        GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap()
    }

    pub(super) fn write_at(dir: &tempfile::TempDir, rel: &str, bytes: &[u8]) {
        let p = dir.path().join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, bytes).unwrap();
    }

    /// An incremental run's manifest: `cursor_low` is the value it resumed from, `None` on a whole-table pass.
    fn incremental_manifest(
        run: &str,
        started_at: &str,
        cursor_low: Option<&str>,
    ) -> (String, crate::manifest::RunManifest) {
        let mut m = success_manifest(run, "part.parquet");
        m.mode = "incremental".into();
        m.started_at = started_at.into();
        m.source.extraction = Some(
            serde_json::from_value(serde_json::json!({
                "strategy": "incremental",
                "cursor_column": "id",
                "cursor_low": cursor_low,
                "cursor_high": "9",
            }))
            .unwrap(),
        );
        (format!("base/{run}/manifest.json"), m)
    }

    fn ids(runs: &[(String, crate::manifest::RunManifest)]) -> Vec<String> {
        runs.iter().map(|(_, m)| m.run_id.clone()).collect()
    }

    #[test]
    fn the_latest_whole_table_run_lands_first_and_only_later_deltas_follow() {
        let split = split_runs(&[
            incremental_manifest("d0", "2026-09-01T00:00:00Z", Some("5")),
            incremental_manifest("f1", "2026-09-02T00:00:00Z", None),
            incremental_manifest("d3", "2026-09-04T00:00:00Z", Some("9")),
            incremental_manifest("d2", "2026-09-03T00:00:00Z", Some("7")),
        ]);
        assert_eq!(
            split.first_pass.map(|(_, m)| m.run_id).as_deref(),
            Some("f1")
        );
        assert_eq!(ids(&split.deltas), ["d2", "d3"], "deltas in start order");
        assert_eq!(split.superseded, ["d0"]);
    }

    #[test]
    fn two_whole_table_runs_keep_only_the_latest() {
        let split = split_runs(&[
            incremental_manifest("f1", "2026-09-01T00:00:00Z", None),
            incremental_manifest("d2", "2026-09-02T00:00:00Z", Some("3")),
            incremental_manifest("f3", "2026-09-03T00:00:00Z", None),
        ]);
        assert_eq!(
            split.first_pass.map(|(_, m)| m.run_id).as_deref(),
            Some("f3")
        );
        assert!(split.deltas.is_empty());
        assert_eq!(split.superseded, ["f1", "d2"]);
    }

    #[test]
    fn deltas_and_manifests_without_cursor_data_never_land_as_a_table() {
        let mut legacy = success_manifest("old", "part.parquet");
        legacy.started_at = "2026-08-01T00:00:00Z".into();
        let split = split_runs(&[
            incremental_manifest("d2", "2026-09-02T00:00:00Z", Some("3")),
            ("base/old/manifest.json".to_string(), legacy),
            incremental_manifest("d1", "2026-09-01T00:00:00Z", Some("1")),
        ]);
        assert!(split.first_pass.is_none());
        assert_eq!(ids(&split.deltas), ["old", "d1", "d2"]);
        assert!(split.superseded.is_empty());
    }

    #[test]
    fn a_landed_leg_leaves_only_the_rest_for_the_closing_record() {
        let all = ids(&[
            incremental_manifest("f1", "2026-09-01T00:00:00Z", None),
            incremental_manifest("d2", "2026-09-02T00:00:00Z", Some("3")),
            incremental_manifest("d3", "2026-09-03T00:00:00Z", Some("5")),
        ]);
        assert_eq!(remaining_run_ids(&all, &["f1".to_string()]), ["d2", "d3"]);
        assert_eq!(remaining_run_ids(&all, &[]), all);
        assert!(remaining_run_ids(&all, &all).is_empty());
        let one = ["f1".to_string()];
        assert!(closing_record_applies(&all, &[]), "one-row load");
        assert!(
            closing_record_applies(&[], &[]),
            "an up-to-date load still records"
        );
        assert!(
            closing_record_applies(&one, &one),
            "deltas after a landed leg"
        );
        assert!(
            !closing_record_applies(&[], &one),
            "the legs covered everything: no empty second row"
        );
    }

    #[test]
    fn has_deltas_reads_the_split() {
        let delta = incremental_manifest("d1", "2026-09-02T00:00:00Z", Some("1"));
        let first = incremental_manifest("f1", "2026-09-01T00:00:00Z", None);
        assert!(split_runs(std::slice::from_ref(&delta)).has_deltas());
        assert!(!split_runs(std::slice::from_ref(&first)).has_deltas());
        assert!(split_runs(&[first, delta]).has_deltas());
    }

    /// A whole-table run is landed as `<table>` ONLY when that name is free.
    #[test]
    fn a_whole_table_run_joins_the_log_whenever_the_target_already_exists() {
        use load::ObjectKind::*;
        for base_and_buffer in [false, true] {
            assert!(whole_table_run_joins_the_log(base_and_buffer, View, Absent));
            assert!(whole_table_run_joins_the_log(
                base_and_buffer,
                Table,
                Absent
            ));
            assert!(!whole_table_run_joins_the_log(
                base_and_buffer,
                Absent,
                Absent
            ));
            assert!(!whole_table_run_joins_the_log(
                base_and_buffer,
                Other,
                Table
            ));
        }
        assert!(whole_table_run_joins_the_log(false, Absent, Table));
        assert_eq!(
            whole_pass_into_buffer_note("p.d.orders", "f1"),
            "  note: `p.d.orders` already holds the base, and run f1 re-read the whole table — it \
             is appended to the buffer `p.d.orders__changes` for `rivet compact` to merge by key, \
             never replacing the base: a row the pass no longer holds stays in it"
        );
        assert!(!whole_table_run_joins_the_log(true, Absent, Table));

        let on_table = whole_table_run_note(Table, "p.d.orders", "f1");
        assert!(
            on_table.contains("never overwrites a table that exists"),
            "{on_table}"
        );
        assert!(on_table.contains("p.d.orders__changes"), "{on_table}");
        let on_view = whole_table_run_note(View, "p.d.orders", "f1");
        assert!(on_view.contains("instead of replacing it"), "{on_view}");

        let first = incremental_manifest("f1", "2026-09-02T00:00:00Z", None);
        let before = incremental_manifest("d0", "2026-09-01T00:00:00Z", Some("1"));
        let after = incremental_manifest("d2", "2026-09-03T00:00:00Z", Some("9"));
        let mut split = split_runs(&[after.clone(), first.clone(), before]);
        assert_eq!(split.superseded, ["d0"]);
        split.whole_table_run_joins_the_log();
        assert!(split.first_pass.is_none());
        let deltas: Vec<&str> = split
            .deltas
            .iter()
            .map(|(_, m)| m.run_id.as_str())
            .collect();
        assert_eq!(deltas, ["f1", "d2"], "in started-at order");
        assert_eq!(
            split.superseded,
            ["d0"],
            "a run the whole pass covers stays superseded"
        );

        let mut none = split_runs(std::slice::from_ref(&after));
        none.whole_table_run_joins_the_log();
        assert_eq!(none.deltas.len(), 1, "nothing to demote");
    }

    #[test]
    fn a_snapshot_leg_conflicts_only_with_a_full_load_table() {
        use load::ObjectKind::*;
        assert!(snapshot_over_full_table(true, Table));
        assert!(!snapshot_over_full_table(false, Table));
        assert!(!snapshot_over_full_table(true, View));
        assert!(!snapshot_over_full_table(true, Absent));
    }

    /// A minimal Success manifest whose one part exists in the store.
    #[test]
    fn a_run_that_finished_after_the_pinned_one_is_refused_this_cycle() {
        let mut older = success_manifest("r1", "p1.parquet");
        older.finished_at = "2026-08-21T00:00:30Z".into();
        let pinned = success_manifest("r2", "p2.parquet");
        let mut late = success_manifest("r3", "p3.parquet");
        late.finished_at = "2026-08-21T00:01:00.250Z".into();
        let pin = ("r2".to_string(), "2026-08-21T00:01:00Z".to_string());
        let runs = |ms: Vec<crate::manifest::RunManifest>| {
            ms.into_iter()
                .map(|m| (m.run_id.clone(), m))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            late_runs_refusal(
                "orders",
                &runs(vec![older.clone(), pinned.clone()]),
                Some(&pin)
            ),
            None
        );
        let why =
            late_runs_refusal("orders", &runs(vec![pinned, late]), Some(&pin)).expect("r3 is late");
        assert!(
            why.contains("r3") && why.contains("r2") && why.contains("orders"),
            "{why}"
        );
        assert_eq!(late_runs_refusal("orders", &runs(vec![older]), None), None);
    }

    #[test]
    fn a_newer_run_the_pin_skipped_for_want_of_a_spec_is_not_refused_as_late() {
        let mut r1 = success_manifest("r1", "p1.parquet");
        r1.finished_at = "2026-08-21T00:00:30Z".into();
        let r2 = success_manifest("r2", "p2.parquet");
        let mut r3 = success_manifest("r3", "p3.parquet");
        r3.finished_at = "2026-08-21T00:01:00.250Z".into();
        let newest_first = vec![
            (r2.finished_at.clone(), "r2".to_string()),
            (r1.finished_at.clone(), "r1".to_string()),
        ];
        let pin = crate::load::pin::listing_pin(&newest_first, "r1");
        let keyed = |ms: Vec<crate::manifest::RunManifest>| {
            ms.into_iter()
                .map(|m| (m.run_id.clone(), m))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            late_runs_refusal("orders", &keyed(vec![r1, r2.clone()]), Some(&pin)),
            None,
            "r2 was listed by the pin and loads under r1's columns"
        );
        assert!(late_runs_refusal("orders", &keyed(vec![r2, r3]), Some(&pin)).is_some());
    }

    /// The partition budget measures the files that land in the PARTITIONED target.
    #[test]
    fn the_partition_budget_measures_the_base_leg_only_under_base_and_buffer() {
        let mut baseline = success_manifest("r1", "r1-000.parquet");
        baseline.mode = "chunked".into();
        let stream = success_manifest("r2", "cdc-000.parquet");
        let runs = vec![
            ("base/manifest-r1.json".to_string(), baseline),
            ("base/manifest-r2.json".to_string(), stream.clone()),
        ];
        let uris = vec![
            "gs://b/base/r1-000.parquet".to_string(),
            "gs://b/base/cdc-000.parquet".to_string(),
        ];

        assert_eq!(
            budgeted_uris(CdcLayout::BaseAndBuffer, &runs, &uris),
            vec!["gs://b/base/r1-000.parquet".to_string()],
            "the buffer's file is never partitioned, so it is not budgeted"
        );
        assert_eq!(
            budgeted_uris(CdcLayout::LogAndView, &runs, &uris),
            uris,
            "a changelog IS partitioned like its table — every file is budgeted"
        );
        let stream_only = vec![("base/manifest-r2.json".to_string(), stream)];
        assert!(
            budgeted_uris(CdcLayout::BaseAndBuffer, &stream_only, &uris).is_empty(),
            "a cycle with no baseline leg writes nothing partitioned"
        );
    }

    pub(super) use crate::load::tests::run_manifest as success_manifest;

    /// Round-10: a column the staged parquet records but the live source lost must WARN (Snowflake silently omits it.
    #[test]
    fn a_dropped_source_column_warns_and_an_added_one_does_not() {
        use crate::manifest::ColumnChecksum;
        let mut m = success_manifest("r1", "p.parquet");
        m.column_checksums = Some(vec![
            ColumnChecksum {
                name: "id".into(),
                checksum: "0".into(),
            },
            ColumnChecksum {
                name: "legacy_col".into(),
                checksum: "0".into(),
            },
        ]);
        let notes = spec_manifest_column_drift(&["id".into(), "brand_new".into()], &[m.clone()]);
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert!(notes[0].contains("legacy_col") && notes[0].contains("SILENTLY"));
        m.column_checksums = None;
        assert!(
            spec_manifest_column_drift(&["id".into()], &[m]).is_empty(),
            "checksum-less manifests carry no column names — nothing to compare"
        );
    }

    /// THE ROUND-4 TOCTOU, at the boundary and through the real producer.
    #[test]
    fn a_run_finishing_during_the_copy_is_not_recorded_as_consumed() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        let prefix = "gs://b/base";
        for (run, part) in [("run-live", "live-0.parquet"), ("r-done", "done-0.parquet")] {
            write_at(&dir, &format!("base/{part}"), b"x");
            let m = success_manifest(run, part);
            write_at(
                &dir,
                &format!("base/manifest-{run}.json"),
                &serde_json::to_vec(&m).unwrap(),
            );
        }
        let state = state_with_active_run(prefix);
        state
            .begin_run("r-done", "orders", prefix, "2026-08-21T00:00:00Z")
            .unwrap();
        state
            .finish_run("r-done", "success", "2026-08-21T00:01:00Z")
            .unwrap();
        let plan = plan_at(LoadMode::Cdc, prefix);
        let inputs = prepare_load(&store, &plan, Some(&state), "p.d.orders", false)
            .unwrap()
            .expect("two unloaded Success runs must select");
        assert_eq!(
            inputs
                .active_at_fetch
                .as_ref()
                .map(|a| a.contains("run-live")),
            Some(true),
            "the fixture must catch the run mid-write, or this test grades nothing"
        );
        assert_eq!(inputs.source_run_ids.len(), 2);

        state
            .finish_run("run-live", "success", "2026-08-21T00:02:00Z")
            .unwrap();

        let ctx = LoadCtx {
            state: Some(&state),
            load_id: "load-1",
            export_name: "orders",
            target_fqtn: "proj.ds.orders",
            warehouse: "bigquery",
            mode: LoadMode::Cdc,
            source_prefix: prefix,
            source_ident: inputs.source_ident.clone(),
            active_at_fetch: inputs.active_at_fetch.clone(),
            marker_active: Default::default(),
        };
        ctx.record_success(&inputs.source_run_ids, 2);

        let loaded = state.loaded_source_run_ids("proj.ds.orders").unwrap();
        assert!(
            loaded.contains("r-done"),
            "positive control: the run terminal at FETCH time is consumed, got {loaded:?}"
        );
        assert!(
            !loaded.contains("run-live") && !loaded.contains("r-live"),
            "a run live at fetch time finished during the copy — consuming it strands its \
             post-fetch parts: {loaded:?}"
        );
    }

    /// A state store with `run` recorded `running` on `prefix` — the ledger's "a run is writing here right now".
    fn state_with_active_run(prefix: &str) -> StateStore {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("run-live", "orders", prefix, "2026-08-21T00:00:00Z")
            .unwrap();
        s
    }

    /// Only a `mode: cdc` table needs the `__pos` parse engine resolved.
    #[test]
    fn only_a_cdc_plan_needs_the_source_engine() {
        assert!(!needs_source_engine(&[]), "no tables, no engine");
        for batch in [LoadMode::Full, LoadMode::Incremental] {
            assert!(
                !needs_source_engine(&[plan_at(batch, "gs://b/base")]),
                "{batch:?} is a batch mode and must not open the source"
            );
        }
        assert!(needs_source_engine(&[plan_at(
            LoadMode::Cdc,
            "gs://b/base"
        )]));
        assert!(needs_source_engine(&[
            plan_at(LoadMode::Full, "gs://b/base"),
            plan_at(LoadMode::Cdc, "gs://b/base"),
        ]));
    }

    /// ClickHouse cannot adopt the table under CDC, so its refusal offers only the drop.
    #[test]
    fn a_snapshot_over_a_full_table_offers_only_the_ways_out_the_warehouse_takes() {
        use crate::load::cdc::Warehouse;
        let bq = snapshot_over_full_table_refusal("p.d.t", Warehouse::BigQuery);
        assert!(
            bq.contains("drop the table") && bq.contains("without `initial: snapshot`"),
            "{bq}"
        );
        let ch = snapshot_over_full_table_refusal("d.t", Warehouse::ClickHouse);
        assert!(
            ch.contains("drop the table") && !ch.contains("without `initial"),
            "{ch}"
        );
    }

    /// The ledger's three answers, each decisive.
    #[test]
    fn ledger_says_active_is_conservative_on_error_and_silent_when_absent() {
        assert!(
            !ledger_says_active(None),
            "no state store to ask: the manifest signal decides alone"
        );
        assert!(!ledger_says_active(Some(Ok(false))));
        assert!(ledger_says_active(Some(Ok(true))));
        assert!(
            ledger_says_active(Some(Err(anyhow::anyhow!("state db unreachable")))),
            "a ledger the load cannot read must not license a delete"
        );
    }

    /// The two-signal fold as a truth table.
    #[test]
    fn either_activity_signal_alone_spares_the_prefix() {
        assert!(!prefix_is_active(false, false), "nothing is writing here");
        assert!(
            prefix_is_active(true, false),
            "the ledger alone is enough — a foreign bucket may carry no marker"
        );
        assert!(
            prefix_is_active(false, true),
            "the marker alone is enough — a stateless load has no ledger to read"
        );
        assert!(prefix_is_active(true, true));
    }

    /// The whole guard over a real (filesystem-backed) store.
    #[test]
    fn prefix_has_active_run_reads_the_ledger_and_fails_safe() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        write_at(&dir, "base/part-000000.parquet", b"rows");
        let prefix = "gs://b/base";

        assert!(!prefix_has_active_run(&store, prefix, None));
        let idle = StateStore::open_in_memory().unwrap();
        assert!(!prefix_has_active_run(&store, prefix, Some(&idle)));

        let live = state_with_active_run(prefix);
        assert!(prefix_has_active_run(&store, prefix, Some(&live)));

        assert!(
            prefix_has_active_run(&store, "not-a-gs-uri", Some(&idle)),
            "an unreadable prefix must count as active: a delete needs proof, not silence"
        );
    }

    /// What cleanup and gc decide for a live run recorded at a base prefix and a per-table child, under path text a LIKE pattern or an escape would misread.
    fn assert_cleanup_and_gc_follow_the_literal_prefix(state: &StateStore, tag: &str) {
        for (i, (seg, twin)) in [
            ("a\\b", "ab"),
            ("p\\q", "pq"),
            ("a_b", "aXb"),
            ("a%b", "aXYb"),
        ]
        .iter()
        .enumerate()
        {
            let base = format!("gs://b/{tag}_{i}/{seg}/");
            let child = format!("gs://b/{tag}_{i}/{seg}/orders");
            let twin_child = format!("gs://b/{tag}_{i}/{twin}/orders");
            let run = format!("{tag}_run_{i}");
            state
                .begin_run(&run, "cdc", &base, "2026-08-21T00:00:00Z")
                .unwrap();
            let dir = tempfile::tempdir().unwrap();
            write_at(
                &dir,
                &format!("{tag}_{i}/{twin}/orders/part-000000.parquet"),
                b"rows",
            );
            assert!(
                prefix_has_active_run(&fs_store(&dir), &child, Some(state)),
                "cleanup must refuse the child {child:?} of a live run on {base:?}"
            );
            assert!(
                !prefix_has_active_run(&fs_store(&dir), &twin_child, Some(state)),
                "the look-alike {twin_child:?} is not under the live run's {base:?}"
            );
            if !seg.contains('\\') {
                write_at(
                    &dir,
                    &format!("{tag}_{i}/{seg}/orders/part-000000.parquet"),
                    b"rows",
                );
                maybe_gc_orphans(
                    &fs_store(&dir),
                    &plan_at(LoadMode::Full, &child),
                    Some(state),
                );
                assert!(
                    dir.path()
                        .join(format!("{tag}_{i}/{seg}/orders/part-000000.parquet"))
                        .exists(),
                    "gc must spare the unmanifested part under the live run on {base:?}"
                );
                maybe_gc_orphans(
                    &fs_store(&dir),
                    &plan_at(LoadMode::Full, &twin_child),
                    Some(state),
                );
                assert!(
                    !dir.path()
                        .join(format!("{tag}_{i}/{twin}/orders/part-000000.parquet"))
                        .exists(),
                    "gc must collect the look-alike {twin_child:?}: no run is writing under it"
                );
            }
            state
                .finish_run(&run, "success", "2026-08-21T00:00:01Z")
                .unwrap();
        }
    }

    #[test]
    fn sqlite_state_cleanup_and_gc_follow_the_literal_prefix() {
        let s = StateStore::open_in_memory().unwrap();
        assert_cleanup_and_gc_follow_the_literal_prefix(&s, "sq");
    }

    #[test]
    fn postgres_state_cleanup_and_gc_follow_the_literal_prefix() {
        let Some(s) = StateStore::postgres_for_test() else {
            return;
        };
        let tag = format!("pg{}", std::process::id());
        assert_cleanup_and_gc_follow_the_literal_prefix(&s, &tag);
    }

    /// The cleanup truth table, including the LAZINESS the `if` chain had.
    #[test]
    fn cleanup_verdict_refuses_under_a_live_run_and_never_probes_unrequested() {
        assert_eq!(
            cleanup_verdict(false, || panic!(
                "must not probe when cleanup was not requested"
            )),
            CleanupVerdict::NotRequested
        );
        assert_eq!(
            cleanup_verdict(true, || true),
            CleanupVerdict::RefusedRunActive
        );
        assert_eq!(cleanup_verdict(true, || false), CleanupVerdict::Delete);
    }

    /// The wiring: the delete target only materialises when cleanup was asked for AND the prefix is idle.
    #[test]
    fn cleanup_target_is_the_prefix_only_when_asked_and_idle() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        let prefix = "gs://b/base";
        let idle = StateStore::open_in_memory().unwrap();

        let mut plan = plan_at(LoadMode::Full, prefix);
        assert!(
            cleanup_target(&plan, &store, Some(&idle)).is_none(),
            "cleanup_source is off — nothing may be deleted"
        );

        plan.load.cleanup_source = true;
        assert_eq!(
            cleanup_target(&plan, &store, Some(&idle)).map(|(_, p)| p),
            Some(prefix),
            "asked for, and idle: the staged prefix is the delete target"
        );

        let live = state_with_active_run(prefix);
        assert!(
            cleanup_target(&plan, &store, Some(&live)).is_none(),
            "a run is writing here — the recursive delete must be refused"
        );
    }

    /// The recursive delete holds a PREFIX lease, and skips when it cannot get one.
    #[test]
    fn the_cleanup_delete_takes_a_prefix_lease_and_skips_when_another_rivet_holds_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        let prefix = "gs://b/base";
        let state = StateStore::open_in_memory().unwrap();
        let mut plan = plan_at(LoadMode::Full, prefix);
        plan.load.cleanup_source = true;

        let (target, lease) =
            cleanup_target_leased(&plan, &store, Some(&state), &[], Some(&Default::default()));
        assert_eq!(target.map(|(_, c)| c.keys), Some(Vec::new()));
        assert!(
            lease.is_some(),
            "the delete must HOLD the prefix, not merely check it"
        );

        let (blocked, no_lease) =
            cleanup_target_leased(&plan, &store, Some(&state), &[], Some(&Default::default()));
        assert!(
            blocked.is_none() && no_lease.is_none(),
            "a prefix another rivet holds must not be wiped: {:?}",
            blocked.map(|(_, k)| k)
        );

        drop(lease);
        let (again, _) =
            cleanup_target_leased(&plan, &store, Some(&state), &[], Some(&Default::default()));
        assert_eq!(
            again.map(|(_, c)| c.keys),
            Some(Vec::new()),
            "the lease is released with its holder — the next load cleans up normally"
        );

        let (stateless, none) =
            cleanup_target_leased(&plan, &store, None, &[], Some(&Default::default()));
        assert_eq!(stateless.map(|(_, c)| c.keys), Some(Vec::new()));
        assert!(none.is_none());
    }

    #[test]
    fn cleanup_may_remove_loaded_runs_and_superseded_full_runs_never_unloaded_ones() {
        let listed = || {
            vec![
                (
                    "p/manifest-old.json".to_string(),
                    success_manifest("old", "a.parquet"),
                ),
                (
                    "p/manifest-now.json".to_string(),
                    success_manifest("now", "b.parquet"),
                ),
                (
                    "p/manifest-late.json".to_string(),
                    success_manifest("late", "c.parquet"),
                ),
            ]
        };
        let ids = |v: Vec<(String, crate::manifest::RunManifest)>| -> Vec<String> {
            v.into_iter().map(|(_, m)| m.run_id).collect()
        };
        let loaded: std::collections::HashSet<String> = ["old".to_string()].into();
        let loading = ["now".to_string()];
        assert_eq!(
            ids(cleanable_runs(listed(), &loaded, &loading, LoadMode::Cdc)),
            vec!["old", "now"],
            "a run written after this load's read is not its to delete"
        );
        assert_eq!(
            ids(cleanable_runs(
                listed(),
                &Default::default(),
                &loading,
                LoadMode::Full
            )),
            vec!["old", "now", "late"],
            "a full load supersedes every earlier run"
        );
    }

    #[test]
    fn a_stateful_load_that_cannot_tell_who_is_writing_deletes_nothing() {
        let markers: std::collections::HashSet<String> = ["m".to_string()].into();
        assert_eq!(still_writing(&None, &markers, true), None);
        assert_eq!(still_writing(&None, &markers, false), Some(markers.clone()));
        let at_fetch = Some(["a".to_string()].into());
        assert_eq!(
            still_writing(&at_fetch, &markers, true),
            Some(["a".to_string(), "m".to_string()].into()),
            "the ledger's writers and the bucket's markers both spare a run"
        );
    }

    /// Orphan GC over a real store, both directions.
    #[test]
    fn gc_collects_an_unmanifested_part_only_while_no_run_is_writing() {
        let prefix = "gs://b/base";
        let orphan = "base/part-000000.parquet";

        let dir = tempfile::tempdir().unwrap();
        write_at(&dir, orphan, b"rows");
        let live = state_with_active_run(prefix);
        maybe_gc_orphans(
            &fs_store(&dir),
            &plan_at(LoadMode::Full, prefix),
            Some(&live),
        );
        assert!(
            dir.path().join(orphan).exists(),
            "an unmanifested part under a LIVE run must survive gc — deleting it loses data \
             the source side has already advanced past"
        );

        let idle = StateStore::open_in_memory().unwrap();
        maybe_gc_orphans(
            &fs_store(&dir),
            &plan_at(LoadMode::Full, prefix),
            Some(&idle),
        );
        assert!(
            !dir.path().join(orphan).exists(),
            "with no run active, an unmanifested part is crash debris and must be collected"
        );
    }

    fn cdc_manifest_at(
        dir: &tempfile::TempDir,
        run: &str,
        started: &str,
        family: &str,
        mode: &str,
    ) {
        let mut m = success_manifest(run, "base/kept.parquet");
        m.started_at = started.into();
        m.export_family = family.into();
        m.export_name = family.into();
        m.mode = mode.into();
        write_at(dir, "base/kept.parquet", b"rows");
        write_at(
            dir,
            &format!("base/manifest-{run}.json"),
            &serde_json::to_vec(&m).unwrap(),
        );
    }

    #[test]
    fn gc_and_the_prefix_gate_spare_a_cdc_run_no_ledger_row_accounts_for() {
        let prefix = "gs://b/base";
        let orphan = "base/part-000009.parquet";
        let dir = tempfile::tempdir().unwrap();
        cdc_manifest_at(&dir, "cdc-rolling", "2026-08-21T00:00:00Z", "orders", "cdc");
        write_at(&dir, orphan, b"rows");
        let foreign = StateStore::open_in_memory().unwrap();

        assert!(
            prefix_has_active_run(&fs_store(&dir), prefix, Some(&foreign)),
            "a CDC run with a manifest and no ledger row may still be writing: the prefix gate must say so"
        );
        maybe_gc_orphans(
            &fs_store(&dir),
            &plan_at(LoadMode::Full, prefix),
            Some(&foreign),
        );
        assert!(
            dir.path().join(orphan).exists(),
            "gc must spare an unmanifested part beside a CDC run no ledger row accounts for"
        );

        foreign
            .begin_run_under_lease(
                "cdc-rolling",
                "orders",
                "src",
                "gs://b/base/",
                "2026-08-21T00:00:00Z",
            )
            .unwrap();
        foreign
            .finish_run("cdc-rolling", "success", "2026-08-21T00:05:00Z")
            .unwrap();
        assert!(!prefix_has_active_run(
            &fs_store(&dir),
            prefix,
            Some(&foreign)
        ));
        maybe_gc_orphans(
            &fs_store(&dir),
            &plan_at(LoadMode::Full, prefix),
            Some(&foreign),
        );
        assert!(
            !dir.path().join(orphan).exists(),
            "with the run's terminal ledger row, the part is crash debris and is collected"
        );
    }

    #[test]
    fn gc_and_the_prefix_gate_treat_a_batch_manifest_without_a_row_as_idle() {
        let prefix = "gs://b/base";
        let orphan = "base/part-000009.parquet";
        let dir = tempfile::tempdir().unwrap();
        cdc_manifest_at(
            &dir,
            "batch-done",
            "2026-08-21T00:00:00Z",
            "orders",
            "batch",
        );
        write_at(&dir, orphan, b"rows");
        let foreign = StateStore::open_in_memory().unwrap();
        assert!(!prefix_has_active_run(
            &fs_store(&dir),
            prefix,
            Some(&foreign)
        ));
        maybe_gc_orphans(
            &fs_store(&dir),
            &plan_at(LoadMode::Full, prefix),
            Some(&foreign),
        );
        assert!(!dir.path().join(orphan).exists());
    }

    #[test]
    fn only_the_newest_cdc_run_of_a_family_counts_as_writing() {
        let dir = tempfile::tempdir().unwrap();
        cdc_manifest_at(&dir, "old", "2026-08-21T00:00:00Z", "orders", "cdc");
        cdc_manifest_at(&dir, "new", "2026-08-22T00:00:00Z", "orders", "cdc");
        cdc_manifest_at(&dir, "other", "2026-08-20T00:00:00Z", "items", "cdc");
        cdc_manifest_at(&dir, "tie-a", "2026-08-23T00:00:00Z", "tied", "cdc");
        cdc_manifest_at(&dir, "tie-b", "2026-08-23T00:00:00Z", "tied", "cdc");
        cdc_manifest_at(&dir, "bad-time", "not-a-time", "bad", "cdc");
        let keyed =
            crate::load::reconcile::fetch_manifests_keyed(&fs_store(&dir), "gs://b/base").unwrap();
        let state = StateStore::open_in_memory().unwrap();
        let mut got: Vec<String> = crate::load::staging::bucket_writers(Some(&state), &keyed)
            .into_iter()
            .collect();
        got.sort();
        assert_eq!(got, ["bad-time", "new", "other", "tie-a", "tie-b"]);
        assert!(crate::load::staging::bucket_writers(None, &keyed).is_empty());
    }

    #[test]
    fn gc_retires_a_running_marker_whose_run_the_ledger_says_is_over() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = "gs://b/base";
        let mut marker = success_manifest("run-dead", "part-0.parquet");
        marker.status = crate::manifest::ManifestStatus::Running;
        marker.parts.clear();
        marker.part_count = 0;
        marker.row_count = 0;
        let key = "base/manifest-run-dead.json";
        write_at(&dir, key, &serde_json::to_vec(&marker).unwrap());
        let state = StateStore::open_in_memory().unwrap();
        state
            .begin_run("run-dead", "orders", prefix, "2026-08-21T00:00:00Z")
            .unwrap();
        state
            .finish_run("run-dead", "failed", "2026-08-21T01:00:00Z")
            .unwrap();

        maybe_gc_orphans(
            &fs_store(&dir),
            &plan_at(LoadMode::Full, prefix),
            Some(&state),
        );

        assert!(
            !dir.path().join(key).exists(),
            "a marker nothing supersedes is still dead once the ledger closed its run"
        );
    }

    /// The warehouse table belongs to ONE source.
    #[test]
    fn conflicting_source_ident_names_a_different_source_and_only_that() {
        assert!(
            conflicting_source_ident("mysql:orders", &["mysql".to_string()]).is_none(),
            "a bare prior of the same engine is not another source"
        );
        assert!(
            conflicting_source_ident("mysql", &["mysql:orders".to_string()]).is_none(),
            "a bare carrier of the same engine is not another source"
        );
        assert_eq!(
            conflicting_source_ident("mysql:orders", &["postgres".to_string()]).map(String::as_str),
            Some("postgres"),
            "a bare identity of a DIFFERENT engine is still evidence"
        );
        assert_eq!(
            conflicting_source_ident("mysql:app.orders", &["mysql:app.payments".to_string()])
                .map(String::as_str),
            Some("mysql:app.payments"),
            "a different qualified table of the same engine is another source"
        );

        let mine = "postgres:public.orders";
        assert!(
            conflicting_source_ident(mine, &[]).is_none(),
            "a table nothing was loaded into yet accepts this source"
        );
        assert!(
            conflicting_source_ident(mine, &[mine.to_string()]).is_none(),
            "the SAME source must keep loading into its own table"
        );
        assert_eq!(
            conflicting_source_ident(mine, &[mine.to_string(), "mysql:app.orders".to_string()])
                .map(String::as_str),
            Some("mysql:app.orders"),
            "a second source must be named, not silently overwritten"
        );
        assert!(
            conflicting_source_ident("", &["mysql:app.orders".to_string()]).is_none(),
            "an artifact written before the ledger recorded an identity reads as UNKNOWN and \
             must never block — an upgrade may not start refusing yesterday's loads"
        );
    }

    /// One prefix under one export name is refused by `ensure_single_source`, and `dataset:` is a BigQuery-only key, so the refusal offers neither.
    #[test]
    fn the_cross_source_refusal_offers_only_a_remedy_the_product_accepts() {
        let msg = source_conflict_refusal("d.orders", "postgres:public.orders", "mysql:app.orders");
        assert!(
            msg.ends_with(
                "both commands would report success. Give this source its own target table."
            ),
            "{msg}"
        );
        assert!(!msg.contains("one export name and one prefix"), "{msg}");
        assert!(!msg.contains("`dataset:`"), "{msg}");
    }

    /// A run still WRITING into the prefix stays retryable.
    #[test]
    fn only_finished_runs_are_recorded_as_consumed() {
        let read = ["r1".to_string(), "r2".to_string()];
        let none: std::collections::HashSet<String> = Default::default();
        assert_eq!(consumable_run_ids(&read, &none), vec!["r1", "r2"]);
        let active: std::collections::HashSet<String> = ["r2".to_string()].into_iter().collect();
        assert_eq!(
            consumable_run_ids(&read, &active),
            vec!["r1"],
            "r2 is still writing — recording it consumed strands every part it writes later"
        );
        let all: std::collections::HashSet<String> = read.iter().cloned().collect();
        assert!(consumable_run_ids(&read, &all).is_empty());
    }

    /// The note about in-flight runs exists only when there ARE some.
    #[test]
    fn active_run_note_speaks_only_when_runs_are_still_writing() {
        assert_eq!(active_run_note(0, "gs://b/base"), None);
        let note = active_run_note(2, "gs://b/base").expect("two active runs must be announced");
        assert!(note.contains('2') && note.contains("gs://b/base"), "{note}");
        assert!(note.contains("cannot be shown finished"), "{note}");
        assert!(
            note.contains("point the load at the state DB the extract writes"),
            "{note}"
        );
    }

    /// The up-to-date line names the right load.
    #[test]
    fn up_to_date_label_names_cdc_and_only_cdc() {
        assert_eq!(up_to_date_label(LoadMode::Cdc), "cdc load");
        for batch in [LoadMode::Full, LoadMode::Incremental] {
            assert_eq!(up_to_date_label(batch), "load", "{batch:?}");
        }
    }

    /// The success traces, against HAND-WRITTEN expected strings.
    #[test]
    fn done_lines_render_the_whole_integrity_chain() {
        let inputs = LoadInputs {
            integrity: load::reconcile::LoadIntegrity {
                source_rows: Some(100),
                file_rows: 100,
                manifests: 2,
            },
            uris: vec!["gs://b/base/part-000000.parquet".into()],
            source_run_ids: vec!["r1".into()],
            source_ident: "postgres:public.orders".into(),
            active_at_fetch: Some(Default::default()),
            marker_active: Default::default(),
            runs: Vec::new(),
            cleanable: Vec::new(),
            ownership: load::Ownership::Own,
        };

        let appended = load::CdcLoadReport {
            rows_appended: 40,
            changes_table: "p.d.orders__changes".into(),
            target: "p.d.orders".into(),
            target_kind: load::ChangelogTarget::View,
            source_cleaned: false,
            baseline: None,
        };
        assert_eq!(
            cdc_done_line(&inputs.integrity, &appended),
            "  integrity ✓ source 100 → files 100 → appended 40 to p.d.orders__changes | \
             current-state view p.d.orders"
        );
        assert_eq!(
            cdc_ok_line(&appended),
            "40 row(s) appended to `p.d.orders__changes` | current-state view `p.d.orders`"
        );

        let buffered = load::CdcLoadReport {
            target_kind: load::ChangelogTarget::Base,
            ..appended.clone()
        };
        assert_eq!(
            cdc_done_line(&inputs.integrity, &buffered),
            "  integrity ✓ source 100 → files 100 → base p.d.orders | buffered 40 row(s) into \
             p.d.orders__changes",
            "the base layout names the base, never a view"
        );
        assert_eq!(
            cdc_ok_line(&buffered),
            "40 change row(s) buffered into `p.d.orders__changes` — `rivet compact` merges them \
             into `p.d.orders`"
        );
        let idle = load::CdcLoadReport {
            rows_appended: 0,
            ..buffered.clone()
        };
        assert_eq!(
            cdc_ok_line(&idle),
            "no changes buffered by this load into `p.d.orders__changes`; `rivet compact` has \
             nothing new for `p.d.orders`",
            "an idle drain must not promise a merge of nothing"
        );

        let cleaned = load::CdcLoadReport {
            source_cleaned: true,
            ..appended
        };
        assert_eq!(
            cdc_done_line(&inputs.integrity, &cleaned),
            "  integrity ✓ source 100 → files 100 → appended 40 to p.d.orders__changes | \
             current-state view p.d.orders (source cleaned)",
            "a load that deleted the staged Parquet must SAY so — the prefix is empty now"
        );

        let full = load::LoadReport {
            rows_loaded: 100,
            target_table: "p.d.orders".into(),
            source_cleaned: false,
        };
        assert_eq!(
            full_done_line(&inputs.integrity, &full),
            "  integrity ✓ source 100 → files 100 → warehouse 100 rows in p.d.orders"
        );
        assert_eq!(
            full_done_line(
                &inputs.integrity,
                &load::LoadReport {
                    source_cleaned: true,
                    ..full
                }
            ),
            "  integrity ✓ source 100 → files 100 → warehouse 100 rows in p.d.orders \
             (source cleaned)"
        );
    }

    /// The generated correlation id is pure lowercase hex ending in this process's pid.
    #[test]
    fn generated_run_id_is_lowercase_hex_ending_in_the_pid() {
        let id = generate_run_id();
        assert!(
            id.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()),
            "must survive both warehouses' tag sanitizers unchanged: {id}"
        );
        assert!(
            id.ends_with(&format!("{:08x}", std::process::id())),
            "the id carries THIS process's pid, so two concurrent loads cannot collide: {id}"
        );
        assert!(
            id.len() > 8,
            "a pid alone is not a per-invocation id — the microsecond stamp is missing: {id}"
        );
    }
}

#[cfg(test)]
mod load_message_tests {
    use super::*;

    /// The lease refusal names the table AND the remedy.
    #[test]
    fn the_lease_refusal_names_the_table_and_the_remedy() {
        let m = lease_busy_message("p.d.orders");
        assert!(m.contains("`p.d.orders`"), "{m}");
        assert!(m.contains("another `rivet load`"), "{m}");
        assert!(m.contains("Wait for it, then retry."), "{m}");
    }

    /// A held lease lets the load go on, and a stateless load has none to lose.
    #[test]
    fn a_held_lease_and_a_stateless_load_go_on() {
        assert!(lease_still_held(true, "p.d.orders", false).is_ok());
        assert!(lease_still_held(true, "p.d.orders", true).is_ok());
        assert!(holds(&None));
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let key = format!("p.d.held_{}", std::process::id());
        assert!(holds(&take_table_lease(Some(&state), &key).unwrap()));
    }

    /// A lease its keeper lost is not held, and the load stops on it by code on either side of the write.
    #[test]
    fn a_lease_its_keeper_lost_stops_the_load() {
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let lease = Some(crate::state::LoadLease::kept_for_test(&state, "p.d.orders"));
        assert!(holds(&lease));
        assert!(lease_still_held(holds(&lease), "p.d.orders", false).is_ok());
        lease.as_ref().unwrap().lose_for_test();
        assert!(!holds(&lease));
        let before = lease_still_held(holds(&lease), "p.d.orders", false).unwrap_err();
        assert!(before.is::<load::Refused>());
        assert_eq!(
            crate::error::error_code(&before),
            Some("RIVET_STATE_LEASE_LOST")
        );
        let after = lease_still_held(holds(&lease), "p.d.orders", true).unwrap_err();
        assert_eq!(
            crate::error::error_code(&after),
            Some("RIVET_LOAD_LEASE_LOST_DURING_WRITE")
        );
    }

    /// A lease lost before the warehouse write is a coded, retryable stop the ledger records as `refused`.
    #[test]
    fn a_lease_lost_before_the_write_is_a_refusal() {
        let e = lease_still_held(false, "p.d.orders", false).unwrap_err();
        assert!(e.is::<load::Refused>());
        assert_eq!(crate::error::error_code(&e), Some("RIVET_STATE_LEASE_LOST"));
        assert_eq!(crate::error::classify_exit(&e), 2);
        assert_eq!(ledger_status(&e), "refused");
        assert_eq!(
            e.to_string(),
            "the lease on `p.d.orders` is no longer this process's: it was not renewed within \
             its TTL (the process stalled, the state database was out of reach, or the lease \
             keeper stopped), so another `rivet load` or `rivet compact` may be writing the \
             table. This run wrote nothing to the warehouse and recorded nothing as loaded; \
             wait for the other process, then run again."
        );
    }

    /// A lease lost during the warehouse write fails the load, and the ledger keeps the write as rivet's own.
    #[test]
    fn a_lease_lost_during_the_write_is_a_failure() {
        let e = lease_still_held(false, "p.d.orders", true).unwrap_err();
        assert!(!e.is::<load::Refused>());
        assert_eq!(
            crate::error::error_code(&e),
            Some("RIVET_LOAD_LEASE_LOST_DURING_WRITE")
        );
        assert_eq!(crate::error::classify_exit(&e), 3);
        assert_eq!(ledger_status(&e), "failed");
        assert_eq!(
            e.to_string(),
            "the lease on `p.d.orders` is no longer this process's: it was not renewed within \
             its TTL (the process stalled, the state database was out of reach, or the lease \
             keeper stopped), so another `rivet load` or `rivet compact` may be writing the \
             table at the same time as this run did. This run's warehouse write is done and \
             is recorded as failed, not as a success: check `p.d.orders`, then run again."
        );
    }

    /// Both incremental outcomes say what landed where.
    #[test]
    fn the_incremental_summary_says_what_landed_where() {
        let table = IncrementalReport::Table(load::LoadReport {
            rows_loaded: 7,
            target_table: "p.d.orders".into(),
            source_cleaned: false,
        });
        assert_eq!(table.summary(), "7 rows landed as table p.d.orders");
        let delta = IncrementalReport::Changelog(load::CdcLoadReport {
            rows_appended: 3,
            changes_table: "p.d.orders__changes".into(),
            target: "p.d.orders".into(),
            target_kind: load::ChangelogTarget::View,
            source_cleaned: true,
            baseline: None,
        });
        assert_eq!(
            delta.summary(),
            "3 rows appended to p.d.orders__changes | current-state view p.d.orders (source cleaned)"
        );
        let buffered = IncrementalReport::Changelog(load::CdcLoadReport {
            rows_appended: 3,
            changes_table: "p.d.orders__changes".into(),
            target: "p.d.orders".into(),
            target_kind: load::ChangelogTarget::Base,
            source_cleaned: false,
            baseline: None,
        });
        assert_eq!(
            buffered.summary(),
            "3 rows buffered into p.d.orders__changes | `rivet compact` merges them into p.d.orders"
        );
    }
}

#[cfg(test)]
mod overwritten_delta_tests {
    use super::overwritten_delta_warning;
    use crate::state::LoadRecord;

    fn load(mode: &str, status: &str) -> LoadRecord {
        LoadRecord {
            load_id: "l".into(),
            export_name: "t".into(),
            target_table: "db.t".into(),
            warehouse: "clickhouse".into(),
            mode: mode.into(),
            source_run_ids: vec![],
            rows_loaded: 1,
            status: status.into(),
            finished_at: "2026-10-03T00:00:00Z".into(),
            source_ident: String::new(),
        }
    }

    /// A continued-key export whose newest successful load overwrote its table is warned once, with the remedy.
    #[test]
    fn a_continued_key_export_last_loaded_as_an_overwrite_is_warned_with_its_remedy() {
        let warn = |continued, loads: &[LoadRecord]| {
            overwritten_delta_warning(continued, loads, "db.t", "t", "rivet.yaml")
        };
        assert_eq!(
            warn(
                true,
                &[load("incremental", "failed"), load("full", "success")]
            )
            .as_deref(),
            Some(
                "`db.t` was last loaded as a whole-table overwrite, and export `t` now loads by \
                 append because each run carries only the keys past the last one \
                 (`keyset_incremental` / `source.mongo.resume`). rivet 0.31 and older \
                 (0.30 and older for `keyset_incremental`) loaded such \
                 an export by overwriting the table with each run's new keys, so it may lack rows \
                 earlier runs delivered. To restore it: `rivet state reset -c rivet.yaml --export \
                 t`, then `rivet run -c rivet.yaml` and `rivet load -c rivet.yaml`."
            )
        );
        assert_eq!(
            warn(
                true,
                &[load("incremental", "success"), load("full", "success")]
            ),
            None
        );
        assert_eq!(warn(false, &[load("full", "success")]), None);
        assert_eq!(warn(true, &[load("full", "failed")]), None);
        assert_eq!(warn(true, &[]), None);
    }

    /// An incremental report counts the rows of whichever load it was.
    #[test]
    fn an_incremental_report_counts_the_rows_its_load_landed() {
        let table = crate::load::orchestrate::IncrementalReport::Table(crate::load::LoadReport {
            rows_loaded: 7,
            target_table: "p.d.t".into(),
            source_cleaned: false,
        });
        let log =
            crate::load::orchestrate::IncrementalReport::Changelog(crate::load::CdcLoadReport {
                rows_appended: 40,
                changes_table: "p.d.t__changes".into(),
                target: "p.d.t".into(),
                target_kind: crate::load::ChangelogTarget::View,
                source_cleaned: false,
                baseline: None,
            });
        assert_eq!((table.rows(), log.rows()), (7, 40));
        assert_eq!((table.wrote(), log.wrote()), ("p.d.t", "p.d.t__changes"));
    }

    /// A load names the object it writes, a compact the base it writes and the buffer it reads, each through the loader's own `fqtn`.
    #[test]
    fn each_command_names_what_it_works_on_by_its_loaders_fqtn() {
        use crate::load::plan::{CdcLayout, LoadMode, test_plan};
        use crate::load::result::Address;
        let at = |target: &str, buffer: Option<&str>| Address {
            target: target.into(),
            buffer: buffer.map(str::to_string),
        };
        let full = test_plan(LoadMode::Full, "gs://b/p/");
        let log = test_plan(LoadMode::Cdc, "gs://b/p/");
        let mut base = test_plan(LoadMode::Incremental, "gs://b/p/");
        base.layout = CdcLayout::BaseAndBuffer;
        let address = crate::load::orchestrate::address_of;
        assert_eq!(address(&full, "r", "loaded"), at("p.d.orders", None));
        assert_eq!(
            address(&log, "r", "loaded"),
            at("p.d.orders__changes", None)
        );
        assert_eq!(
            address(&base, "r", "loaded"),
            at("p.d.orders__changes", None)
        );
        assert_eq!(address(&full, "r", "compacted"), at("p.d.orders", None));
        assert_eq!(address(&log, "r", "compacted"), at("p.d.orders", None));
        let mut snowflake = test_plan(LoadMode::Full, "gs://b/p/");
        snowflake.load.target = crate::load::plan::LoadTarget::Snowflake {
            account: "a".into(),
            user: "u".into(),
            private_key_path: "/k.p8".into(),
            role: None,
            warehouse: "w".into(),
            database: "DB".into(),
            schema: "SC".into(),
            storage_integration: "i".into(),
        };
        let journals = crate::load::orchestrate::journals_of(&[full.clone(), snowflake.clone()]);
        let ids = |j: &crate::load::result::JobJournal| {
            crate::load::result::table_result(
                "e",
                "t",
                &at("t", None),
                "loaded",
                &Ok(crate::load::result::TableDone::Skipped("up_to_date")),
                j,
            )
            .warehouse_job_ids
        };
        assert_eq!(
            journals.iter().map(ids).collect::<Vec<_>>(),
            [Some(vec![]), None],
            "BigQuery names its jobs, Snowflake none"
        );
        assert_eq!(address(&snowflake, "r", "loaded"), at("DB.SC.orders", None));
        assert_eq!(
            address(&base, "r", "compacted"),
            at("p.d.orders", Some("p.d.orders__changes"))
        );
    }
}

#[cfg(test)]
mod mixed_mongo_ids_tests {
    use super::*;

    /// A config with one CDC export `orders` of `source_type`, its checkpoint `./o.ckpt` holding `idt`.
    fn cfg_with_ckpt(
        dir: &std::path::Path,
        source_type: &str,
        idt: serde_json::Value,
    ) -> (crate::config::Config, String) {
        let yaml = format!(
            "source:\n  type: {source_type}\n  url: \"{source_type}://u:p@127.0.0.1:1/app\"\n\
             exports:\n  - name: orders\n    table: orders\n    mode: cdc\n    format: parquet\n    \
             cdc:\n      checkpoint: ./o.ckpt\n    destination: {{ type: gcs, bucket: b, prefix: \"p/\" }}\n"
        );
        let path = dir.join("rivet.yaml");
        std::fs::write(&path, yaml).unwrap();
        crate::source::cdc::Position::new(serde_json::json!({ "idt": idt }))
            .save(&dir.join("o.ckpt"))
            .unwrap();
        let path = path.display().to_string();
        (crate::config::Config::load(&path).unwrap(), path)
    }

    fn plan(export: &str, unit: Option<&str>) -> load::plan::LoadPlan {
        let mut plan = load::plan::test_plan(load::plan::LoadMode::Cdc, "gs://b/p/");
        plan.export_name = export.into();
        plan.unit = unit.map(str::to_string);
        plan
    }

    /// A MongoDB CDC load whose checkpoint records two `_id` types is refused by code before any append.
    #[test]
    fn a_mongo_load_over_a_mixed_checkpoint_is_refused_by_code() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, path) = cfg_with_ckpt(dir.path(), "mongo", serde_json::json!({"orders": [0, 2]}));
        let err = refuse_mixed_mongo_ids(&cfg, &plan("orders", None), &path).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_ID_TYPES_MIXED", 5);
    }

    /// A uniform checkpoint, another source, an unknown export and another unit's collection all load.
    #[test]
    fn a_uniform_foreign_or_unmatched_load_is_not_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (mongo, path) = cfg_with_ckpt(dir.path(), "mongo", serde_json::json!({"orders": [0]}));
        refuse_mixed_mongo_ids(&mongo, &plan("orders", None), &path).unwrap();

        let (mixed, path) =
            cfg_with_ckpt(dir.path(), "mongo", serde_json::json!({"orders": [0, 2]}));
        refuse_mixed_mongo_ids(&mixed, &plan("elsewhere", None), &path).unwrap();
        refuse_mixed_mongo_ids(&mixed, &plan("orders", Some("other")), &path).unwrap();
        let err =
            refuse_mixed_mongo_ids(&mixed, &plan("orders", Some("orders")), &path).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_ID_TYPES_MIXED", 5);

        let (mysql, path) =
            cfg_with_ckpt(dir.path(), "mysql", serde_json::json!({"orders": [0, 2]}));
        refuse_mixed_mongo_ids(&mysql, &plan("orders", None), &path).unwrap();
    }
}
