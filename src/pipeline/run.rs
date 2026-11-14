//! **Layer: Coordinator entrypoint** — the `rivet run` orchestrator.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::state::StateStore;

use super::run_set::RunSet;
use super::summary::RunSummary;
use super::{aggregate, finalize, ipc, job, parallel_children, parent_ui};

/// Per-run configuration flags passed from the CLI to the pipeline.
#[derive(Debug, Clone, Copy)]
pub struct RunOptions<'a> {
    pub validate: bool,
    pub reconcile: bool,
    pub resume: bool,
    /// Override safety gates that would otherwise refuse to start the run.
    pub force: bool,
    pub params: Option<&'a std::collections::HashMap<String, String>>,
    /// A CDC export's pending baseline snapshots run concurrently (up to the pool ceiling) — `--parallel-exports`.
    pub parallel_snapshots: bool,
    /// `--strict`: a lossy or unsupported column refuses the run instead of warning.
    pub strict: bool,
}

/// True when the current process is running more than one export in this `rivet run` invocation (sequential or `--parallel-exports`).
pub(crate) static MULTI_EXPORT_MODE: AtomicBool = AtomicBool::new(false);

/// True only when multiple exports run **concurrently** in the current process (i.e.
pub(crate) static MULTI_EXPORT_CONCURRENT: AtomicBool = AtomicBool::new(false);

pub(crate) fn multi_export_mode() -> bool {
    MULTI_EXPORT_MODE.load(AtomicOrdering::Relaxed)
}

/// Env marker a parent sets on re-exec'd children (--parallel-export-processes, apply's wave-parallel batches) so a single-export child still knows sibling processes share its server-counter windows.
pub(crate) const ENV_CONCURRENT_SIBLINGS: &str = "RIVET_CONCURRENT_SIBLINGS";

/// Env marker a parent sets on re-exec'd children to claim their run-over-run throughput self-check.
pub(crate) const ENV_PARENT_SELF_CHECK: &str = "RIVET_PARENT_OWNS_SELF_CHECK";

pub(crate) fn multi_export_concurrent() -> bool {
    MULTI_EXPORT_CONCURRENT.load(AtomicOrdering::Relaxed)
        || std::env::var_os(ENV_CONCURRENT_SIBLINGS).is_some()
}

/// How a `rivet run` fans out: `(exports_in_parallel, snapshot_legs_may_fan_out)`.
pub(crate) fn run_concurrency(
    flag: bool,
    config_flag: bool,
    one_export_named: bool,
    exports: usize,
    sibling_child: bool,
) -> (bool, bool) {
    let requested = flag || config_flag;
    let exports_parallel = requested && !one_export_named && exports > 1;
    (
        exports_parallel,
        requested && !exports_parallel && !sibling_child,
    )
}

/// One export's result and summary, as `job::run_export_job` returns them.
pub(crate) type ExportOutcome = (Result<()>, RunSummary);

/// Run every export on the shared worker pool (`workers::run_workers`), one state connection per worker.
pub(crate) fn run_export_pool(
    config_path: &str,
    exports: &[&ExportConfig],
    run: impl Fn(&ExportConfig, &StateStore) -> ExportOutcome + Sync,
) -> (Vec<ExportOutcome>, usize) {
    let workers = crate::workers::effective_pool(None, exports.len());
    let outcomes = crate::workers::run_workers(
        exports,
        workers,
        || match StateStore::open(config_path) {
            Ok(state) => Some(state),
            Err(e) => {
                log::error!("export worker: failed to open the state database: {e:#}");
                None
            }
        },
        |state, _, export| Ok(run(export, state)),
        |export| {
            let err = crate::error::coded(
                crate::error::codes::RUN_UNITS_FAILED,
                format!(
                    "export '{}' did not complete: its worker panicked or could not open the state database (the log above names which)",
                    export.name
                ),
            );
            let summary = job::synthetic_failed_summary(&export.name, &err);
            Box::new((Err(err), summary))
        },
    );
    let outcomes = outcomes
        .into_iter()
        .map(
            |o: std::result::Result<ExportOutcome, Box<ExportOutcome>>| {
                o.unwrap_or_else(|lost| *lost)
            },
        )
        .collect();
    (outcomes, workers)
}

/// One-line cards: this process renders more than one export itself (not one pinned export, not a subprocess parent).
fn compact_cards(pinned_export: bool, exports: usize, subprocess_parent: bool) -> bool {
    !pinned_export && !subprocess_parent && exports > 1
}

/// Swaps the render flags in for one run and restores the previous values on drop, panic included.
pub(crate) struct RenderFlags(bool, bool);

impl RenderFlags {
    /// Set `multi` and, when given, `concurrent`; the guard puts both back.
    pub(crate) fn set(multi: bool, concurrent: Option<bool>) -> Self {
        let prev_multi = MULTI_EXPORT_MODE.swap(multi, AtomicOrdering::Relaxed);
        let prev_concurrent = match concurrent {
            Some(c) => MULTI_EXPORT_CONCURRENT.swap(c, AtomicOrdering::Relaxed),
            None => MULTI_EXPORT_CONCURRENT.load(AtomicOrdering::Relaxed),
        };
        Self(prev_multi, prev_concurrent)
    }
}

impl Drop for RenderFlags {
    fn drop(&mut self) {
        MULTI_EXPORT_MODE.store(self.0, AtomicOrdering::Relaxed);
        MULTI_EXPORT_CONCURRENT.store(self.1, AtomicOrdering::Relaxed);
    }
}

/// The in-process card UI: installs the ipc sender and runs `parent_ui` until dropped, panic included.
struct CardUi(Option<std::thread::JoinHandle<()>>);

impl CardUi {
    /// Install the sender and spawn the UI thread.
    fn start(name_floor: usize, n_cards: usize) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<parent_ui::UiMessage>();
        ipc::install_in_process_tx(tx);
        let thread = std::thread::Builder::new()
            .name("rivet-ui".to_string())
            .spawn(move || parent_ui::run_ui(rx, name_floor, n_cards))
            .ok();
        Self(thread)
    }
}

impl Drop for CardUi {
    fn drop(&mut self) {
        ipc::clear_in_process_tx();
        if let Some(t) = self.0.take() {
            let _ = t.join();
        }
    }
}

/// Write `--summary-output` and print `--json` from `agg`, for a tail whose aggregate path did not write the file.
pub(super) fn emit_machine_document(
    agg: &crate::state::RunAggregate,
    summary_output: Option<&Path>,
    json_output: bool,
) {
    if let Some(out) = summary_output
        && let Err(e) = aggregate::write_json(out, agg)
    {
        log::warn!(
            "aggregate: failed to write summary JSON to {}: {:#}",
            out.display(),
            e
        );
    }
    if json_output {
        print_json_summary(agg);
    }
}

fn print_json_summary(agg: &crate::state::RunAggregate) {
    match serde_json::to_string_pretty(agg) {
        Ok(json) => println!("{json}"),
        Err(e) => eprintln!(
            "rivet: error: failed to serialize run summary as JSON: {:#}",
            e
        ),
    }
}

/// Emit captured child stderr from a parallel run.
fn emit_child_stderr(dump: &str, dir: &Path) {
    if dump.is_empty() {
        return;
    }
    let name = format!(
        "rivet-child-stderr-{}.log",
        chrono::Utc::now().format("%Y%m%dT%H%M%S")
    );
    let path = dir.join(name);
    match std::fs::write(&path, dump) {
        Ok(()) => eprintln!(
            "\n  child stderr (full per-export logs) → {}",
            path.display()
        ),
        Err(e) => {
            log::warn!(
                "could not write child stderr to {} ({e}); printing inline",
                path.display()
            );
            use std::io::Write;
            let mut h = std::io::stderr().lock();
            let _ = h.write_all(dump.as_bytes());
            let _ = h.flush();
        }
    }
}

/// Whether THIS process emits the run-over-run throughput self-check for the exports it just ran, or defers to the parent that spawned it.
fn owns_throughput_self_check(reexec_child: bool) -> bool {
    !reexec_child
}

/// Is this process a re-exec'd child whose PARENT owns its self-check?
fn is_reexecd_child(ipc_events: bool, parent_owns_self_check: bool) -> bool {
    ipc_events && parent_owns_self_check
}

/// The ONE call site of [`aggregate::warn_throughput_regressions`].
pub(super) fn self_check_throughput(
    state: &StateStore,
    entries: &[crate::state::RunAggregateEntry],
    modes: &RunModes<'_>,
) {
    self_check_throughput_as(
        state,
        entries,
        modes,
        is_reexecd_child(
            ipc::ipc_events_enabled(),
            std::env::var_os(ENV_PARENT_SELF_CHECK).is_some(),
        ),
    );
}

/// [`self_check_throughput`] with the "am I a re-exec'd child" answer INJECTED rather than read from the process environment.
fn self_check_throughput_as(
    state: &StateStore,
    entries: &[crate::state::RunAggregateEntry],
    modes: &RunModes<'_>,
    reexec_child: bool,
) {
    if !owns_throughput_self_check(reexec_child) {
        log::debug!(
            "throughput self-check: deferred to the parent process (this is a re-exec'd child; \
             its stderr is captured, the parent prints beside the run aggregate)"
        );
        return;
    }
    let mut by_mode: std::collections::BTreeMap<&str, Vec<crate::state::RunAggregateEntry>> =
        std::collections::BTreeMap::new();
    for e in entries {
        by_mode
            .entry(modes.for_export(&e.export_name))
            .or_default()
            .push(e.clone());
    }
    for (mode, group) in by_mode {
        aggregate::warn_throughput_regressions(state, &group, mode);
    }
}

/// Which concurrency label the throughput self-check applies to each export.
pub(super) struct RunModes<'a> {
    run: &'a str,
    per_export: std::collections::HashMap<String, &'static str>,
}

impl<'a> RunModes<'a> {
    /// Every export ran under the run's one mode.
    pub(super) fn uniform(run: &'a str) -> Self {
        Self {
            run,
            per_export: std::collections::HashMap::new(),
        }
    }

    /// Each named export ran under its OWN mode; anything unnamed under `run`.
    pub(super) fn per_export(
        run: &'a str,
        per_export: std::collections::HashMap<String, &'static str>,
    ) -> Self {
        Self { run, per_export }
    }

    /// The label to attribute ONE export's throughput to.
    fn for_export(&self, export: &str) -> &str {
        self.per_export.get(export).copied().unwrap_or(self.run)
    }
}

/// What [`run`]'s tail does with the aggregate it just built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TailPlan {
    /// Print the run-summary card and record the `run_aggregate` row.
    aggregate: bool,
    /// Write `--summary-output` from the tail itself (the aggregate path writes it through [`aggregate::persist`], so exactly one of the two does).
    machine_output: bool,
}

/// Does EITHER machine channel ask for the document?
fn machine_document_requested(summary_output: Option<&Path>, json_output: bool) -> bool {
    summary_output.is_some() || json_output
}

/// The tail's routing rule, pure because the value-level oracle needs a live source (a real `run()` needs a database, a destination and a state DB).
fn tail_plan(agg: &crate::state::RunAggregate, machine_output_requested: bool) -> TailPlan {
    TailPlan {
        aggregate: reports_run_aggregate(agg),
        machine_output: machine_output_requested && agg.total_exports <= 1,
    }
}

/// Does this run report a run AGGREGATE — the summary card on stderr and the `run_aggregate` row in the state DB?
fn reports_run_aggregate(agg: &crate::state::RunAggregate) -> bool {
    agg.total_exports > 1
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    config_path: &str,
    export_name: Option<&str>,
    validate: bool,
    reconcile: bool,
    resume: bool,
    force: bool,
    params: Option<&std::collections::HashMap<String, String>>,
    parallel_exports_cli: bool,
    parallel_export_processes_cli: bool,
    summary_output: Option<&Path>,
    json_output: bool,
    strict: bool,
) -> Result<()> {
    if force && !resume {
        log::warn!(
            "--force without --resume only silences the rerun-accumulation warning (a prefix that \
             already holds a prior run); it overrides nothing else — with --resume it overrides the \
             gate against a prefix whose _SUCCESS is already present"
        );
    }
    let config = Config::load_with_params(config_path, params)?;

    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();

    let run_set = RunSet::select(&config, export_name)?;
    let expanded = run_set.in_process(&config.source, &config_dir, params)?;
    let exports: Vec<&ExportConfig> = expanded.iter().collect();

    let (run_parallel, parallel_snapshots) = run_concurrency(
        parallel_exports_cli,
        config.parallel_exports,
        export_name.is_some(),
        exports.len(),
        multi_export_concurrent(),
    );
    let opts = RunOptions {
        validate,
        reconcile,
        resume,
        force,
        params,
        parallel_snapshots,
        strict,
    };

    let name_floor = exports
        .iter()
        .map(|e| e.name.chars().count())
        .max()
        .unwrap_or(0);
    let children_ok = run_set
        .run_child_processes(parallel_export_processes_cli)
        .unwrap_or_else(|why| {
            log::warn!("{why}");
            None
        });

    let mut started_at = chrono::Utc::now();

    if let Some(children_ok) = children_ok {
        if let Err(e) = StateStore::open(config_path) {
            return Err(e.context("state: failed to initialize state DB before spawning children"));
        }

        let (run_harm, window_start) = RunHarmBracket::open(&config.source);
        started_at = window_start;
        let (result, child_failures, stderr_dump, child_reports) =
            parallel_children::run_exports_as_child_processes(
                children_ok,
                config_path,
                &exports,
                validate,
                reconcile,
                resume,
                force,
                params,
                strict,
                name_floor,
            );
        let finished_at = chrono::Utc::now();
        run_harm.close_and_warn(HarmWindow::Parallel {
            exports: exports.len(),
        });
        match StateStore::open(config_path) {
            Ok(state) => {
                let entries = aggregate::collect_child_entries(
                    &state,
                    &exports,
                    &child_failures,
                    &child_reports,
                );
                let agg = aggregate::build(
                    entries,
                    started_at,
                    finished_at,
                    Some(config_path),
                    run_mode_label(exports.len(), true),
                );
                aggregate::print(&agg);
                self_check_throughput(
                    &state,
                    &agg.per_export,
                    &RunModes::uniform(&agg.parallel_mode),
                );
                aggregate::persist(&state, &agg, summary_output);
                if json_output {
                    print_json_summary(&agg);
                }
            }
            Err(e) => log::warn!(
                "aggregate: cannot open state DB to record run aggregate: {:#}",
                e
            ),
        }
        emit_child_stderr(&stderr_dump, &config_dir);
        return result;
    }

    let multi_export = compact_cards(export_name.is_some(), exports.len(), false);
    let _render_flags = RenderFlags::set(multi_export, Some(run_parallel));

    let mut summaries: Vec<RunSummary> = Vec::with_capacity(exports.len());
    let mut failures: Vec<anyhow::Error> = Vec::new();
    let mut window_end: Option<chrono::DateTime<chrono::Utc>> = None;
    let mut peak_concurrency = 1usize;

    if run_parallel {
        log::info!(
            "running {} exports on up to {} worker threads (one state DB connection per worker)",
            exports.len(),
            crate::workers::effective_pool(None, exports.len())
        );

        if let Err(e) = StateStore::open(config_path) {
            return Err(
                e.context("state: failed to initialize state DB before spawning export threads")
            );
        }
        let n_cards = exports.len();
        let card_ui = CardUi::start(name_floor, n_cards);

        let (run_harm, window_start) = RunHarmBracket::open(&config.source);
        started_at = window_start;
        let (collected, workers) = run_export_pool(config_path, &exports, |export, state| {
            job::run_export_job(config_path, &config, export, state, &config_dir, &opts)
        });
        peak_concurrency = peak_concurrency.max(workers);

        drop(card_ui);
        window_end = Some(chrono::Utc::now());
        run_harm.close_and_warn(HarmWindow::Parallel { exports: workers });

        for (res, summary) in collected {
            if let Err(e) = res {
                failures.push(e);
            }
            summaries.push(summary);
        }
    } else {
        let state = StateStore::open(config_path)?;

        let n_cards = exports.len();
        let card_ui = CardUi::start(name_floor, n_cards);

        for export in &exports {
            let (res, summary) =
                job::run_export_job(config_path, &config, export, &state, &config_dir, &opts);
            if let Err(e) = res {
                failures.push(e);
            }
            summaries.push(summary);
        }

        drop(card_ui);
        if exports.len() == 1
            && let Some(summary) = summaries.last()
        {
            summary.print_stderr_block();
        }
    }

    let finished_at = window_end.unwrap_or_else(chrono::Utc::now);
    let parallel_mode = run_mode_label(peak_concurrency, false);
    let entries: Vec<_> = summaries
        .iter()
        .map(aggregate::entry_from_summary)
        .collect();
    let agg = aggregate::build(
        entries,
        started_at,
        finished_at,
        Some(config_path),
        parallel_mode,
    );
    let plan = tail_plan(
        &agg,
        machine_document_requested(summary_output, json_output),
    );
    if plan.aggregate {
        aggregate::print(&agg);
    }
    match StateStore::open(config_path) {
        Ok(state) => {
            self_check_throughput(
                &state,
                &agg.per_export,
                &RunModes::uniform(&agg.parallel_mode),
            );
            if plan.aggregate {
                aggregate::persist(&state, &agg, summary_output);
            }
        }
        Err(e) => log::warn!(
            "aggregate: cannot open state DB to record run aggregate: {:#}",
            e
        ),
    }
    if plan.machine_output
        && let Some(out) = summary_output
        && let Err(e) = aggregate::write_json(out, &agg)
    {
        log::warn!(
            "aggregate: failed to write summary JSON to {}: {:#}",
            out.display(),
            e
        );
    }
    if json_output {
        print_json_summary(&agg);
    }

    fold_failures(failures, "")
}

/// `rivet apply -c config.yaml` (plan→apply cycle).
pub(crate) fn run_waves(
    config_path: &str,
    force: bool,
    parallel_cli: bool,
    resume: bool,
    summary_output: Option<&Path>,
    json_output: bool,
) -> Result<()> {
    let config = Config::load_with_params(config_path, None)?;
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let opts = RunOptions {
        validate: false,
        reconcile: false,
        resume,
        force,
        params: None,
        parallel_snapshots: false,
        strict: false,
    };

    let run_set = RunSet::select(&config, None)?;
    let children = run_set.child_processes(parallel_cli).unwrap_or_else(|why| {
        log::warn!("{why}");
        None
    });
    let runnable: Vec<ExportConfig> = match &children {
        Some((_, declared)) => declared.iter().map(|e| (*e).clone()).collect(),
        None => run_set.in_process(&config.source, &config_dir, None)?,
    };
    let children_ok = children.map(|(ok, _)| ok);
    let parallel = children_ok.is_some();
    let by_wave = group_exports_by_wave(&runnable);
    let total: usize = by_wave.iter().map(|(_, v)| v.len()).sum();
    if total == 0 {
        log::warn!("apply: config '{config_path}' defines no exports");
        return Ok(());
    }

    let _render_flags = RenderFlags::set(compact_cards(false, total, parallel), None);

    let state = StateStore::open(config_path)?;
    let (run_harm, started_at) = match parallel.then(|| RunHarmBracket::open(&config.source)) {
        Some((bracket, window_start)) => (Some(bracket), window_start),
        None => (None, chrono::Utc::now()),
    };
    let mut summaries: Vec<RunSummary> = Vec::with_capacity(total);
    let mut failures: Vec<anyhow::Error> = Vec::new();
    let mut all_exports: Vec<&ExportConfig> = Vec::with_capacity(total);
    let mut child_failures: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut child_reports = aggregate::ChildReports::new();
    let mut combined_stderr = String::new();
    let mut peak_concurrency = 0usize;
    let mut export_modes: std::collections::HashMap<String, &'static str> =
        std::collections::HashMap::new();
    let mut degenerate_parallel_warned = false;

    for (wave, exports) in &by_wave {
        let label = if *wave == u32::MAX {
            "unscheduled".to_string()
        } else {
            wave.to_string()
        };
        let pending: Vec<&ExportConfig> = exports
            .iter()
            .copied()
            .filter(|e| finalize::needs_run(e, resume, "apply", &state))
            .collect();
        if pending.is_empty() {
            continue;
        }
        if let Some(header) = wave_header(&label, total, pending.len()) {
            eprintln!("{header}");
        }
        if let Some(children_ok) = children_ok {
            let safe_n = pending.iter().filter(|e| is_parallel_safe(e)).count();
            log::info!(
                "apply: wave {} — {} parallel-safe export(s) in parallel, {} run alone",
                label,
                safe_n,
                pending.len() - safe_n
            );
            let batches = cost_gate_batches(&pending);
            if !degenerate_parallel_warned
                && let Some(line) = degenerate_parallel_warning(
                    &label,
                    &batches.iter().map(Vec::len).collect::<Vec<_>>(),
                )
            {
                log::warn!("{line}");
                degenerate_parallel_warned = true;
            }
            peak_concurrency =
                peak_concurrency.max(batches.iter().map(Vec::len).max().unwrap_or(0));
            export_modes.extend(wave_batch_modes(&batches));
            let wave_name_floor = pending
                .iter()
                .map(|e| e.name.chars().count())
                .max()
                .unwrap_or(0);
            for batch in &batches {
                let (result, cf, stderr_dump, reports) =
                    parallel_children::run_exports_as_child_processes(
                        children_ok,
                        config_path,
                        batch,
                        false,
                        false,
                        resume,
                        force,
                        None,
                        false,
                        wave_name_floor,
                    );
                child_failures.extend(cf);
                child_reports.extend(reports);
                combined_stderr.push_str(&stderr_dump);
                if let Err(e) = result {
                    failures.push(e);
                }
            }
            all_exports.extend_from_slice(&pending);
        } else {
            log::info!(
                "apply: wave {} — {} export(s), sequential",
                label,
                pending.len()
            );
            for export in &pending {
                let (res, summary) =
                    job::run_export_job(config_path, &config, export, &state, &config_dir, &opts);
                if let Err(e) = res {
                    failures.push(e);
                }
                summaries.push(summary);
            }
        }
    }

    let finished_at = chrono::Utc::now();
    if let Some(bracket) = run_harm
        && let Some(window) = wave_harm_window(peak_concurrency, all_exports.len())
    {
        bracket.close_and_warn(window);
    }

    let entries = if parallel {
        aggregate::collect_child_entries(&state, &all_exports, &child_failures, &child_reports)
    } else {
        summaries
            .iter()
            .map(aggregate::entry_from_summary)
            .collect()
    };
    let mode_label = wave_mode_label(peak_concurrency);
    let modes = RunModes::per_export(mode_label, export_modes);
    finish_run_tail(
        &state,
        entries,
        started_at,
        finished_at,
        config_path,
        mode_label,
        &modes,
        Some((&combined_stderr, &config_dir)),
        failures,
        " across waves",
        summary_output,
        json_output,
    )
}

/// Arch-roast follow-up (2026-08-21): the representative-failure fold, written once.
fn fold_failures(mut failures: Vec<anyhow::Error>, context: &str) -> crate::error::Result<()> {
    if failures.is_empty() {
        return Ok(());
    }
    let primary_idx = representative_failure_idx(&failures).unwrap();
    let primary = failures.remove(primary_idx);
    if failures.is_empty() {
        return Err(primary);
    }
    let others = failures
        .iter()
        .map(|e| format!("{e:#}"))
        .collect::<Vec<_>>()
        .join("; ");
    let class = crate::error::classify_exit(&primary);
    Err(primary
        .context(crate::error::PreclassifiedExit(class))
        .context(format!(
            "{} export(s) failed{}; representative error follows (also: {others})",
            failures.len() + 1,
            context,
        )))
}

/// The waves/pool run tail, written once: ONE aggregate, then the shared routing.
#[allow(clippy::too_many_arguments)]
fn finish_run_tail(
    state: &StateStore,
    entries: Vec<crate::state::RunAggregateEntry>,
    started_at: chrono::DateTime<chrono::Utc>,
    finished_at: chrono::DateTime<chrono::Utc>,
    config_path: &str,
    mode_label: &str,
    modes: &RunModes<'_>,
    stderr_artifact: Option<(&str, &Path)>,
    failures: Vec<anyhow::Error>,
    failure_context: &str,
    summary_output: Option<&Path>,
    json_output: bool,
) -> crate::error::Result<()> {
    let agg = aggregate::build(
        entries,
        started_at,
        finished_at,
        Some(config_path),
        mode_label,
    );
    let plan = tail_plan(
        &agg,
        machine_document_requested(summary_output, json_output),
    );
    if plan.aggregate {
        aggregate::print(&agg);
    }
    self_check_throughput(state, &agg.per_export, modes);
    if plan.aggregate {
        aggregate::persist(state, &agg, summary_output);
    }
    emit_machine_document(
        &agg,
        if plan.machine_output {
            summary_output
        } else {
            None
        },
        json_output,
    );
    if let Some((dump, dir)) = stderr_artifact {
        emit_child_stderr(dump, dir);
    }
    fold_failures(failures, failure_context)
}

/// How the run shaped its concurrency, for the run-level harm line's frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HarmWindow {
    /// `--pool m`: `exports` exports drained through `slots` slots.
    Pool { exports: usize, slots: usize },
    /// `--parallel-exports` (at most 16 at once, also CDC snapshot legs) / `--parallel-export-processes` (no cap) / `apply --parallel`.
    Parallel { exports: usize },
    /// A run that ASKED for parallelism and got none.
    Serial { exports: usize },
}

/// The window a wave run actually presented to the source.
fn wave_harm_window(peak: usize, exports: usize) -> Option<HarmWindow> {
    if exports == 0 {
        return None;
    }
    Some(if peak > 1 {
        HarmWindow::Parallel { exports: peak }
    } else {
        HarmWindow::Serial { exports }
    })
}

/// The run-mode label a wave run records on its aggregate.
pub(super) fn wave_mode_label(peak: usize) -> &'static str {
    if peak > 1 {
        "wave-parallel-processes"
    } else {
        "wave-sequential"
    }
}

/// What `apply --parallel` owes the operator when the cost gate bought it NO concurrency.
fn degenerate_parallel_warning(wave: &str, batch_widths: &[usize]) -> Option<String> {
    let exports: usize = batch_widths.iter().sum();
    if exports <= 1 || batch_widths.iter().copied().max().unwrap_or(0) > 1 {
        return None;
    }
    Some(format!(
        "apply --parallel: wave {wave} — all {exports} export(s) run ALONE, one after another, \
         so `--parallel` bought no concurrency here. The cost gate only batches exports marked \
         `parallel_safe: true`, and none of these are (an export with no `parallel_safe:` counts \
         as heavy). Set `parallel_safe: true` on the cheap ones — or regenerate the config with \
         `rivet plan`, which annotates it from the source-aware cost class — to actually overlap \
         them."
    ))
}

/// The POOL's twin of [`degenerate_parallel_warning`].
fn degenerate_pool_warning(safe: usize, heavy: usize, slots: usize) -> Option<String> {
    if slots <= 1 || safe > 0 || heavy <= 1 {
        return None;
    }
    Some(format!(
        "apply --pool: all {heavy} export(s) are heavy, so the pool runs them ALONE, one after \
         another — {slots} slots bought no concurrency here. Only exports marked \
         `parallel_safe: true` overlap EACH OTHER (an export with no `parallel_safe:` counts as \
         heavy, and `rivet init` writes none). Set `parallel_safe: true` on the cheap ones — or \
         annotate the config with `rivet plan --annotate-waves` — to actually overlap them."
    ))
}

/// The wave cost-gate's batching, pure so the SHAPE the run executes is observable without spawning child processes.
fn cost_gate_batches<'a>(pending: &[&'a ExportConfig]) -> Vec<Vec<&'a ExportConfig>> {
    let (safe, lone): (Vec<&ExportConfig>, Vec<&ExportConfig>) =
        pending.iter().copied().partition(|e| is_parallel_safe(e));
    let mut batches: Vec<Vec<&ExportConfig>> = lone.iter().map(|e| vec![*e]).collect();
    if !safe.is_empty() {
        batches.push(safe);
    }
    batches
}

/// Each export's OWN concurrency label, derived from the batch it ran in.
fn wave_batch_modes(batches: &[Vec<&ExportConfig>]) -> Vec<(String, &'static str)> {
    batches
        .iter()
        .flat_map(|batch| {
            let label = wave_mode_label(batch.len());
            batch.iter().map(move |e| (e.name.clone(), label))
        })
        .collect()
}

/// The widest overlap a pool of `slots` can reach over `safe` parallel-safe and `heavy` non-`parallel_safe` exports.
fn pool_peak_concurrency(slots: usize, safe: usize, heavy: usize) -> usize {
    slots.min(safe + heavy.min(1))
}

/// Did the pool actually OVERLAP any work?
fn pool_is_concurrent(slots: usize, safe: usize, heavy: usize) -> bool {
    pool_peak_concurrency(slots, safe, heavy) > 1
}

/// The window a POOL run actually presented to the source.
fn pool_harm_window(slots: usize, safe: usize, heavy: usize) -> Option<HarmWindow> {
    let pending = safe + heavy;
    if pending == 0 {
        return None;
    }
    Some(if pool_is_concurrent(slots, safe, heavy) {
        HarmWindow::Pool {
            exports: pending,
            slots,
        }
    } else {
        HarmWindow::Serial { exports: pending }
    })
}

/// The run-mode label a pool run records — the [`wave_mode_label`] rule on the pool.
pub(super) fn pool_mode_label(concurrent: bool) -> &'static str {
    if concurrent { "pool" } else { "pool-serial" }
}

/// One pool worker's outcome: the export's result, its `RunSummary`, and the `(start_ms, end_ms)` window it occupied relative to the run's start.
type PoolOutcome = (Result<()>, RunSummary, (i64, i64));

/// Each pool export's OWN concurrency label.
fn pool_export_modes(windows: &[(String, i64, i64)]) -> Vec<(String, &'static str)> {
    windows
        .iter()
        .enumerate()
        .map(|(i, (name, start, end))| {
            let overlapped = windows
                .iter()
                .enumerate()
                .any(|(j, (_, other_start, other_end))| {
                    j != i && start < other_end && other_start < end
                });
            (name.clone(), pool_mode_label(overlapped))
        })
        .collect()
}

/// The run-mode label the TOP-LEVEL runner ([`run`]) records.
pub(super) fn run_mode_label(peak: usize, processes: bool) -> &'static str {
    match (peak > 1, processes) {
        (false, _) => "sequential",
        (true, true) => "parallel-processes",
        (true, false) => "parallel-threads",
    }
}

/// Does this export DOMINATE the pool floor (more than its fair share of the predicted total across `m` slots) while being heavy (not `parallel_safe`)?
fn dominates_as_heavy(predicted_secs: f64, total: f64, m: usize, parallel_safe: bool) -> bool {
    predicted_secs > total / (m.max(1) as f64) && !parallel_safe
}

/// Signed error of the actual makespan against the prediction, in percent (0 when there was no prediction).
fn wave_header(label: &str, total: usize, pending: usize) -> Option<String> {
    (total > 1).then(|| format!("\n  ── wave {label} · {pending} export(s) ──"))
}

/// The pool's closing line: the actual makespan (`actual_ms`) against the predicted one, in minutes.
fn makespan_graded(actual_ms: i64, predicted_secs: f64) -> String {
    let actual_secs = actual_ms as f64 / 1000.0;
    format!(
        "  Pool: actual makespan {:.1} min vs predicted {:.1} min ({:+.0}%) — the model grades itself every run",
        actual_secs / 60.0,
        predicted_secs / 60.0,
        makespan_error_pct(actual_secs, predicted_secs),
    )
}

fn makespan_error_pct(actual_secs: f64, predicted_secs: f64) -> f64 {
    if predicted_secs > 0.0 {
        (actual_secs - predicted_secs) / predicted_secs * 100.0
    } else {
        0.0
    }
}

/// What `apply --pool` says when the `--resume` skip leaves nothing to run.
fn nothing_to_run_message(split_noticed: bool) -> String {
    let base = "apply --pool: nothing to run (no exports, or all complete)";
    if split_noticed {
        format!(
            "{base} — every split unit's work is already in the shared prefix, so this run \
             schedules nothing: the predicted-makespan line the `--split` notice above points \
             at does not print. Re-run without `--resume` (or into a fresh prefix) to schedule \
             the units again."
        )
    } else {
        base.to_string()
    }
}

/// The run-window harm verdict, pure so the threshold and wording are unit-tested (its per-export sibling in `job::run_diagnosis` always was.
fn run_harm_verdict(deltas: &[(String, i64)], window: HarmWindow) -> Option<String> {
    let job::Spill {
        total: spills,
        unit,
    } = job::spill_total(deltas);
    if spills < job::SPILL_FLAG_MIN {
        return None;
    }
    let (scope, frame, lever) = match window {
        HarmWindow::Pool { exports, slots } => (
            "pool run",
            format!("the pool window ({exports} exports, {slots} slots)"),
            Some("`--pool` slots"),
        ),
        HarmWindow::Parallel { exports } => (
            "parallel run",
            format!("the run window ({exports} concurrent exports)"),
            Some(
                "the export concurrency (`--pool N` bounds it; `--parallel-exports` is capped at 16)",
            ),
        ),
        HarmWindow::Serial { exports } => (
            "serialized run",
            format!("the run window ({exports} exports, run one at a time)"),
            None,
        ),
    };
    let levers = match lever {
        Some(l) => format!("Lower {l}, `chunk_size` pages, or `tuning.batch_size`"),
        None => "Lower `chunk_size` pages or `tuning.batch_size`".to_string(),
    };
    Some(format!(
        "{scope}: source harm — {spills} {unit} server-wide across {frame}: the source \
         spilled to disk while rivet ran. {levers} to shed pressure (foreign clients can \
         contribute, but rivet's window is the frame)."
    ))
}

/// The run-level source-harm bracket: one `before` snapshot, one `after` snapshot, one WARN verdict.
pub(crate) struct RunHarmBracket<'a> {
    source: &'a crate::config::SourceConfig,
    before: Option<Vec<(String, i64)>>,
}

/// Probe FIRST, stamp SECOND — the open-side mirror of the `finished_at` rule.
fn snapshot_then_stamp<T>(probe: impl FnOnce() -> T) -> (T, chrono::DateTime<chrono::Utc>) {
    let before = probe();
    (before, chrono::Utc::now())
}

impl<'a> RunHarmBracket<'a> {
    /// Take the `before` snapshot and return it with the window-START stamp, taken after the probe (see [`snapshot_then_stamp`]).
    pub(crate) fn open(
        source: &'a crate::config::SourceConfig,
    ) -> (Self, chrono::DateTime<chrono::Utc>) {
        let (before, window_start) = snapshot_then_stamp(|| job::harm_snapshot(source));
        (Self { source, before }, window_start)
    }

    /// Take the `after` snapshot and WARN the verdict if the window crossed the shared threshold.
    pub(crate) fn close_and_warn(self, window: HarmWindow) {
        if let (Some(before), Some(after)) = (&self.before, job::harm_snapshot(self.source))
            && let Some(line) = run_harm_verdict(&job::harm_deltas(before, &after), window)
        {
            log::warn!("{line}");
        }
    }
}

/// The pool's pick rule, pure for the mutation gate (#166): the first queued export a freeing slot may start.
fn next_eligible(safe_flags: &[bool], heavy_running: bool) -> Option<usize> {
    safe_flags.iter().position(|&safe| safe || !heavy_running)
}

/// `rivet apply <config.yaml> --pool N` (#166).
pub(crate) fn run_pool(
    config_path: &str,
    force: bool,
    resume: bool,
    m: usize,
    split: bool,
    summary_output: Option<&Path>,
    json_output: bool,
) -> Result<()> {
    let m = m.max(1);
    let config = Config::load_with_params(config_path, None)?;
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let opts = RunOptions {
        validate: false,
        reconcile: false,
        resume,
        force,
        params: None,
        parallel_snapshots: false,
        strict: false,
    };
    let state = StateStore::open(config_path)?;

    let mut effective: Vec<ExportConfig> = RunSet::select(&config, None)?
        .in_process(&config.source, &config_dir, None)?
        .into_iter()
        .filter(|e| {
            if split {
                return true;
            }
            finalize::needs_run(e, resume, "apply --pool", &state)
        })
        .collect();
    if effective.is_empty() {
        log::warn!("apply --pool: nothing to run (no exports, or all complete)");
        return Ok(());
    }

    let predicted_pre: Vec<(super::pool::PoolItem, super::pool::PredictedFrom)> =
        super::pool::predict_items(
            &state,
            effective
                .iter()
                .map(|e| (e.name.as_str(), is_parallel_safe(e))),
            &std::collections::HashMap::new(),
        );
    let mut items: Vec<super::pool::PoolItem> =
        predicted_pre.iter().map(|(i, _)| i.clone()).collect();

    let mut realized: Option<super::split::Realized> = None;
    let advise = super::pool::advise_split(&items, m, 3.0, m.max(2));
    if split {
        match &advise {
            Some((giant, n, broken)) => {
                realized = super::split::realize(
                    giant,
                    *n,
                    *broken,
                    resume,
                    &predicted_pre,
                    &mut effective,
                    &config,
                    &config_dir,
                    &state,
                )?;
            }
            None => {
                let total: f64 = items.iter().map(|i| i.predicted_secs).sum();
                match items
                    .iter()
                    .max_by(|a, b| a.predicted_secs.total_cmp(&b.predicted_secs))
                    .filter(|l| dominates_as_heavy(l.predicted_secs, total, m, l.parallel_safe))
                {
                    Some(l) => log::warn!(
                        "apply --pool --split: '{}' dominates the pool floor but is HEAVY (not \
                         parallel_safe) — its split units would inherit the heavy flag and still \
                         serialize (the C3 floor), so splitting cannot lower the wall. Mark it \
                         `parallel_safe: true` if its rows are independent; running it whole.",
                        l.name
                    ),
                    None => log::info!(
                        "apply --pool --split: no export dominates the pool floor — nothing to \
                         split."
                    ),
                }
            }
        }
    } else if let Some((giant, n, broken)) = &advise {
        let giant_secs = items
            .iter()
            .find(|i| &i.name == giant)
            .map(|i| i.predicted_secs)
            .unwrap_or(0.0);
        log::warn!(
            "apply --pool: export '{}' (~{:.1} min) dominates the pool floor — extra slots cannot \
             beat it. Re-run with `--split` to split it into {} range sub-exports over its key \
             (separate scheduler units, one shared prefix) and drop the wall to ~{:.1} min.",
            giant,
            giant_secs / 60.0,
            n,
            broken / 60.0,
        );
    }

    if per_unit_skip_applies(split, resume) {
        super::split::skip_completed(realized.as_ref(), &mut effective, &state);
        if effective.is_empty() {
            if let Some(r) = &realized {
                finalize::repair_missing_split_marker(&r.dest, &r.family);
            }
            log::warn!("{}", nothing_to_run_message(realized.is_some()));
            return Ok(());
        }
    }

    let no_seeds = std::collections::HashMap::new();
    let predicted = super::pool::predict_items(
        &state,
        effective
            .iter()
            .map(|e| (e.name.as_str(), is_parallel_safe(e))),
        realized.as_ref().map_or(&no_seeds, |r| &r.seeds),
    );
    let classified: Vec<super::pool::PredictedFrom> =
        predicted.iter().map(|(_, f)| f.clone()).collect();
    items = predicted.into_iter().map(|(i, _)| i).collect();
    let order = super::pool::pool_order(&items);
    let predicted_secs = super::pool::predicted_makespan_secs(&items, m);
    let floor_secs = super::pool::makespan_floor_secs(&items, m);
    let (measured_n, attempt_n, placeholder_n) = super::pool::classification_counts(&classified);
    eprintln!(
        "  Pool: {} export(s) × {} slot(s) — predicted makespan ~{:.1} min (floor {:.1}; {} measured, {} estimated)",
        effective.len(),
        m,
        predicted_secs / 60.0,
        floor_secs / 60.0,
        measured_n,
        attempt_n + placeholder_n,
    );
    if let Some(hedge) = super::pool::lower_bound_hedge(attempt_n, placeholder_n) {
        eprintln!("        {hedge}");
    }

    let pending: Vec<&ExportConfig> = effective.iter().collect();
    {
        let (safe_n, heavy_n) = pool_safe_heavy_split(&pending);
        if let Some(line) = degenerate_pool_warning(safe_n, heavy_n, m) {
            log::warn!("{line}");
        }
    }
    let by_name: std::collections::HashMap<&str, &ExportConfig> =
        pending.iter().map(|e| (e.name.as_str(), *e)).collect();
    let queue: std::sync::Mutex<std::collections::VecDeque<&ExportConfig>> = std::sync::Mutex::new(
        order
            .iter()
            .filter_map(|n| by_name.get(n.as_str()).copied())
            .collect(),
    );
    let heavy_running = std::sync::atomic::AtomicBool::new(false);

    let name_floor = pending
        .iter()
        .map(|e| e.name.chars().count())
        .max()
        .unwrap_or(0);
    let (safe_pending, heavy_pending) = pool_safe_heavy_split(&pending);
    let really_concurrent = pool_is_concurrent(m, safe_pending, heavy_pending);
    let _render_flags = RenderFlags::set(false, Some(really_concurrent));
    let card_ui = CardUi::start(name_floor, pending.len());

    let (run_harm, started_at) = RunHarmBracket::open(&config.source);
    let collected: std::sync::Mutex<Vec<PoolOutcome>> =
        std::sync::Mutex::new(Vec::with_capacity(pending.len()));
    std::thread::scope(|s| {
        for _ in 0..m.min(pending.len()) {
            s.spawn(|| {
                loop {
                    let export = {
                        let mut q = queue.lock().unwrap();
                        if q.is_empty() {
                            break;
                        }
                        let flags: Vec<bool> = q.iter().map(|e| is_parallel_safe(e)).collect();
                        match next_eligible(&flags, heavy_running.load(AtomicOrdering::SeqCst)) {
                            Some(i) => {
                                let e = q.remove(i).unwrap();
                                if !is_parallel_safe(e) {
                                    heavy_running.store(true, AtomicOrdering::SeqCst);
                                }
                                e
                            }
                            None => {
                                drop(q);
                                std::thread::sleep(std::time::Duration::from_millis(200));
                                continue;
                            }
                        }
                    };
                    struct HeavyGuard<'a>(&'a std::sync::atomic::AtomicBool, bool);
                    impl Drop for HeavyGuard<'_> {
                        fn drop(&mut self) {
                            if self.1 {
                                self.0.store(false, AtomicOrdering::SeqCst);
                            }
                        }
                    }
                    let _heavy = HeavyGuard(&heavy_running, !is_parallel_safe(export));
                    let claimed_at = chrono::Utc::now();
                    let pair = match StateStore::open(config_path) {
                        Ok(st) => {
                            let mut unit_opts = opts;
                            unit_opts.resume = super::split::unit_resume(export, resume, &st);
                            job::run_export_job(
                                config_path,
                                &config,
                                export,
                                &st,
                                &config_dir,
                                &unit_opts,
                            )
                        }
                        Err(e) => {
                            let err = crate::error::recoded(
                                &e,
                                crate::error::codes::STATE_NOT_WRITABLE,
                                format!(
                                    "export '{}': failed to open state database: {:#}",
                                    export.name, e
                                ),
                            );
                            let summary = job::synthetic_failed_summary(&export.name, &err);
                            (Err(err), summary)
                        }
                    };
                    let window = (
                        (claimed_at - started_at).num_milliseconds(),
                        (chrono::Utc::now() - started_at).num_milliseconds(),
                    );
                    let (res, summary) = pair;
                    collected.lock().unwrap().push((res, summary, window));
                }
            });
        }
    });
    let finished_at = chrono::Utc::now();
    drop(card_ui);
    if let Some(window) = pool_harm_window(m, safe_pending, heavy_pending) {
        run_harm.close_and_warn(window);
    }

    eprintln!(
        "{}",
        makespan_graded(
            (finished_at - started_at).num_milliseconds(),
            predicted_secs
        )
    );

    let mut summaries: Vec<RunSummary> = Vec::new();
    let mut failures: Vec<anyhow::Error> = Vec::new();
    let mut pool_windows: Vec<(String, i64, i64)> = Vec::with_capacity(summaries.capacity());
    let mut oks: Vec<bool> = Vec::new();
    for (res, summary, (start_ms, end_ms)) in collected.into_inner().unwrap() {
        oks.push(res.is_ok());
        if let Err(e) = res {
            failures.push(e);
        }
        pool_windows.push((summary.export_name.clone(), start_ms, end_ms));
        summaries.push(summary);
    }
    if let Some(r) = &realized {
        super::split::seal(r, summaries.iter().zip(oks.iter().copied()));
    }
    let entries = summaries
        .iter()
        .map(aggregate::entry_from_summary)
        .collect();
    let mode_label = pool_mode_label(really_concurrent);
    let modes = RunModes::per_export(
        mode_label,
        pool_export_modes(&pool_windows).into_iter().collect(),
    );
    finish_run_tail(
        &state,
        entries,
        started_at,
        finished_at,
        config_path,
        mode_label,
        &modes,
        None,
        failures,
        " in the pool",
        summary_output,
        json_output,
    )
}

/// Group exports by `wave:` in ascending order; an export with no `wave:` runs last (sorted as `u32::MAX`).
fn group_exports_by_wave(exports: &[ExportConfig]) -> Vec<(u32, Vec<&ExportConfig>)> {
    let mut by_wave: std::collections::BTreeMap<u32, Vec<&ExportConfig>> =
        std::collections::BTreeMap::new();
    for e in exports {
        by_wave
            .entry(e.wave.unwrap_or(u32::MAX))
            .or_default()
            .push(e);
    }
    by_wave.into_iter().collect()
}

/// Whether an export may run concurrently with its wave-mates.
fn is_parallel_safe(export: &ExportConfig) -> bool {
    export.parallel_safe.unwrap_or(false)
}

/// The pool queue's `(safe, heavy)` census — the INPUT every pool concurrency claim is computed from ([`pool_is_concurrent`], [`pool_peak_concurrency`], [`pool_harm_window`], [`pool_mode_label`]).
fn pool_safe_heavy_split(pending: &[&ExportConfig]) -> (usize, usize) {
    let heavy = pending.iter().filter(|e| !is_parallel_safe(e)).count();
    (pending.len() - heavy, heavy)
}

/// Whether the pool skips completed work per split unit: only a `--split --resume` run has units to skip.
fn per_unit_skip_applies(split: bool, resume: bool) -> bool {
    split && resume
}

#[cfg(test)]
mod render_guard_tests {
    use super::*;

    #[test]
    fn the_pool_skips_per_unit_only_under_split_and_resume() {
        assert!(per_unit_skip_applies(true, true));
        assert!(!per_unit_skip_applies(true, false));
        assert!(!per_unit_skip_applies(false, true));
        assert!(!per_unit_skip_applies(false, false));
    }

    #[test]
    fn only_a_process_rendering_several_exports_itself_uses_compact_cards() {
        assert!(compact_cards(false, 2, false));
        assert!(
            !compact_cards(false, 1, false),
            "one export gets the full card"
        );
        assert!(
            !compact_cards(true, 5, false),
            "a pinned --export gets the full card"
        );
        assert!(
            !compact_cards(false, 5, true),
            "subprocess children render their own"
        );
    }

    #[test]
    fn a_panic_under_the_card_ui_clears_the_sender_and_restores_the_flags() {
        let unwound = std::panic::catch_unwind(|| {
            let _flags = RenderFlags::set(true, Some(true));
            let _ui = CardUi::start(0, 0);
            assert!(ipc::IN_PROCESS_TX.lock().unwrap().is_some());
            panic!("a worker panicked");
        });
        assert!(unwound.is_err());
        assert!(
            ipc::IN_PROCESS_TX
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_none()
        );
        assert!(!multi_export_mode());
        assert!(!MULTI_EXPORT_CONCURRENT.load(AtomicOrdering::Relaxed));
    }
}

#[cfg(test)]
mod run_concurrency_tests {
    use super::{RunSummary, run_concurrency, run_export_pool};

    /// Every export gets its own outcome, in input order, on a pool no wider than the work.
    #[test]
    fn run_export_pool_answers_every_export_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rivet.yaml");
        let cfg = crate::config::Config::from_yaml(
            "source:\n  type: mysql\n  url: \"mysql://localhost/test\"\n\
             exports:\n\
             \x20 - {name: a, table: a, format: parquet, destination: {type: local, path: ./o}}\n\
             \x20 - {name: b, table: b, format: parquet, destination: {type: local, path: ./o}}\n\
             \x20 - {name: c, table: c, format: parquet, destination: {type: local, path: ./o}}\n",
        )
        .expect("three exports load");
        let exports: Vec<_> = cfg.exports.iter().collect();
        let (outcomes, workers) =
            run_export_pool(config_path.to_str().unwrap(), &exports, |export, _state| {
                let summary = RunSummary {
                    export_name: export.name.clone(),
                    ..Default::default()
                };
                if export.name == "b" {
                    (Err(anyhow::anyhow!("b refused")), summary)
                } else {
                    (Ok(()), summary)
                }
            });
        assert_eq!(workers, 3, "never wider than the work");
        let names: Vec<_> = outcomes
            .iter()
            .map(|(_, s)| s.export_name.as_str())
            .collect();
        assert_eq!(
            names,
            ["a", "b", "c"],
            "one outcome per export, in input order"
        );
        assert!(outcomes[0].0.is_ok() && outcomes[1].0.is_err() && outcomes[2].0.is_ok());
    }

    /// Every input that can flip either answer, each against the row that differs only in it.
    #[test]
    fn run_concurrency_fans_out_exports_or_snapshot_legs_never_both() {
        let rows = [
            ((true, false, false, 3, false), (true, false)),
            ((false, true, false, 3, false), (true, false)),
            ((false, false, false, 3, false), (false, false)),
            ((true, false, true, 3, false), (false, true)),
            ((true, false, false, 1, false), (false, true)),
            ((true, false, false, 2, false), (true, false)),
            ((true, false, false, 1, true), (false, false)),
            ((false, false, false, 1, false), (false, false)),
        ];
        for ((f, c, named, n, sib), want) in rows {
            assert_eq!(
                run_concurrency(f, c, named, n, sib),
                want,
                "flag={f} config={c} named={named} exports={n} sibling={sib}"
            );
        }
    }
}

#[cfg(test)]
mod pool_decision_tests {
    use super::super::split::unit_failed as split_unit_failed;
    use super::{RunSummary, dominates_as_heavy, makespan_error_pct, makespan_graded, wave_header};

    #[test]
    fn a_heavy_export_dominates_only_above_its_fair_share() {
        assert!(
            dominates_as_heavy(60.0, 100.0, 2, false),
            "60 > 100/2 and heavy"
        );
        assert!(
            !dominates_as_heavy(60.0, 100.0, 2, true),
            "parallel_safe never dominates"
        );
        assert!(
            !dominates_as_heavy(50.0, 100.0, 2, false),
            "exactly the fair share"
        );
        assert!(
            dominates_as_heavy(2.0, 1.5, 0, false),
            "m=0 is one slot, not a divide-by-zero"
        );
    }

    #[test]
    fn makespan_error_is_signed_and_zero_without_a_prediction() {
        assert_eq!(makespan_error_pct(150.0, 100.0), 50.0);
        assert_eq!(makespan_error_pct(50.0, 100.0), -50.0);
        assert_eq!(makespan_error_pct(50.0, 0.0), 0.0);
    }

    #[test]
    fn the_pool_lines_read_minutes_and_a_single_export_has_no_wave_header() {
        assert_eq!(
            makespan_graded(150_000, 120.0),
            "  Pool: actual makespan 2.5 min vs predicted 2.0 min (+25%) — the model grades itself every run"
        );
        assert_eq!(wave_header("1", 1, 1), None);
        assert_eq!(
            wave_header("2", 3, 2).as_deref(),
            Some("\n  ── wave 2 · 2 export(s) ──")
        );
    }

    #[test]
    fn only_a_successful_split_unit_finished_its_share() {
        let unit = |name: &str, status: &str| RunSummary {
            export_name: name.into(),
            status: status.into(),
            ..Default::default()
        };
        let p = Some("orders#");
        assert!(!split_unit_failed(p, &unit("orders#1", "success"), true));
        assert!(
            split_unit_failed(p, &unit("orders#1", "skipped"), true),
            "a skipped unit wrote no window manifest"
        );
        assert!(split_unit_failed(p, &unit("orders#1", "failed"), true));
        assert!(
            split_unit_failed(p, &unit("orders#1", "success"), false),
            "errored"
        );
        assert!(
            !split_unit_failed(p, &unit("users", "failed"), false),
            "not a unit"
        );
        assert!(
            !split_unit_failed(None, &unit("orders#1", "failed"), false),
            "no split"
        );
    }
}

#[cfg(test)]
mod pool_harm_tests {
    use super::{HarmWindow, run_harm_verdict};

    fn pool() -> HarmWindow {
        HarmWindow::Pool {
            exports: 154,
            slots: 5,
        }
    }

    /// The pool-window verdict was the untested twin of run_diagnosis's spill flag (same threshold, same filter, zero cover).
    #[test]
    fn pool_harm_verdict_fires_at_threshold_on_tmp_disk_only() {
        let at = vec![("mysql_created_tmp_disk_tables".to_string(), 100_i64)];
        let line = run_harm_verdict(&at, pool()).expect("threshold reached");
        assert!(line.contains("100 tmp-disk spills") && line.contains("154 exports"));
        let below = vec![("mysql_created_tmp_disk_tables".to_string(), 99_i64)];
        assert!(run_harm_verdict(&below, pool()).is_none());
        let other = vec![("mysql_innodb_rows_read".to_string(), 1_000_000_i64)];
        assert!(run_harm_verdict(&other, pool()).is_none());
    }

    /// The run-level verdict must see EVERY engine's spill counter, not just MySQL's.
    #[test]
    fn run_harm_verdict_sees_postgres_temp_files_with_its_own_unit() {
        let pg = vec![
            ("pg_temp_files".to_string(), 60_i64),
            ("pg_temp_files_replica".to_string(), 60_i64),
        ];
        let line = run_harm_verdict(&pg, pool()).expect("120 temp files must reach the threshold");
        assert!(
            line.contains("120 temp-file spills"),
            "PG's count must be summed and named as temp FILES: {line}"
        );
        assert!(
            !line.contains("tmp-disk"),
            "must not print MySQL's unit for a temp_files count: {line}"
        );
        let noise = vec![
            ("pg_tup_returned".to_string(), 9_000_000_i64),
            ("pg_blks_read".to_string(), 5_000_000_i64),
            ("pg_deadlocks".to_string(), 7_i64),
        ];
        assert!(run_harm_verdict(&noise, pool()).is_none());
    }

    /// The line the per-export DIAGNOSIS hedge points at is now emitted by the PARALLEL parents too, so its frame must describe THAT window (concurrent exports, no slot cap) and name a lever that path actually has.
    #[test]
    fn run_harm_verdict_frames_the_parallel_window_in_its_own_terms() {
        let spills = vec![("mysql_created_tmp_disk_tables".to_string(), 300_i64)];
        let par =
            run_harm_verdict(&spills, HarmWindow::Parallel { exports: 12 }).expect("300 >= 100");
        assert!(
            par.contains("12 concurrent exports") && !par.contains("slots"),
            "parallel frame must not claim pool slots: {par}"
        );
        assert!(
            par.contains("export concurrency"),
            "parallel lever must be the concurrency, not `--pool` slots alone: {par}"
        );
        let pooled = run_harm_verdict(&spills, pool()).expect("300 >= 100");
        assert!(
            pooled.contains("5 slots") && pooled.contains("`--pool` slots"),
            "pool frame keeps its slot lever: {pooled}"
        );
    }

    /// The per-export DIAGNOSIS hedge points at "the run-level harm line", and that line exists only where a parent BRACKETS the window.
    #[test]
    fn every_concurrent_export_parent_brackets_the_run_harm_window() {
        let whole = include_str!("run.rs");
        let src = &whole[..whole
            .find("\n#[cfg(test)]")
            .expect("run.rs has test modules")];
        let open = concat!("RunHarmBracket::", "open(&config.source)");
        let close = concat!(".close_and", "_warn(");
        let anchors = [
            ("run", "\npub fn run("),
            ("run_waves", "\npub(crate) fn run_waves("),
            ("run_pool", "\npub(crate) fn run_pool("),
        ];
        let mut starts: Vec<(&str, usize)> = anchors
            .iter()
            .map(|(name, sig)| {
                (
                    *name,
                    src.find(sig)
                        .unwrap_or_else(|| panic!("{name}'s signature moved — update the anchor")),
                )
            })
            .collect();
        starts.sort_by_key(|(_, at)| *at);
        for (i, (name, at)) in starts.iter().enumerate() {
            let end = starts.get(i + 1).map(|(_, n)| *n).unwrap_or(src.len());
            let body = &src[*at..end];
            assert!(
                body.contains(open) && body.contains(close),
                "{name} runs exports concurrently, so it must bracket the run-level \
                 harm window — the per-export DIAGNOSIS hedge points at the line it emits"
            );
        }
        assert_eq!(
            src.matches(open).count(),
            4,
            "expected one bracket per concurrent path: run/processes, run/threads, \
             run_waves/parallel, run_pool"
        );
    }

    /// A wave run's harm frame must describe what HAPPENED, not what was asked for.
    #[test]
    fn wave_harm_window_is_framed_by_the_peak_that_actually_overlapped() {
        use super::wave_harm_window;
        assert_eq!(
            wave_harm_window(1, 12),
            Some(HarmWindow::Serial { exports: 12 }),
            "a serialized run must not be framed as a concurrent one"
        );
        assert_eq!(
            wave_harm_window(3, 12),
            Some(HarmWindow::Parallel { exports: 3 }),
            "the frame must report the peak width, not the export count"
        );
        assert_eq!(wave_harm_window(0, 0), None);
    }

    /// The POOL's frame is a concurrency claim too, so it must come from the concurrency.
    #[test]
    fn pool_harm_window_is_framed_by_the_concurrency_the_pool_had() {
        use super::pool_harm_window;
        assert_eq!(
            pool_harm_window(5, 1, 0),
            Some(HarmWindow::Serial { exports: 1 }),
            "a pool with one export to drain never overlapped anything"
        );
        assert_eq!(
            pool_harm_window(1, 8, 0),
            Some(HarmWindow::Serial { exports: 8 }),
            "`--pool 1` is a serial run whatever the export count"
        );
        assert_eq!(
            pool_harm_window(5, 0, 8),
            Some(HarmWindow::Serial { exports: 8 }),
            "heavies never co-run, so an all-heavy pool overlapped nothing"
        );
        assert_eq!(
            pool_harm_window(5, 1, 7),
            Some(HarmWindow::Pool {
                exports: 8,
                slots: 5
            }),
            "one safe export can co-run with the single running heavy"
        );
        assert_eq!(
            pool_harm_window(5, 12, 0),
            Some(HarmWindow::Pool {
                exports: 12,
                slots: 5
            })
        );
        assert_eq!(
            pool_harm_window(2, 2, 0),
            Some(HarmWindow::Pool {
                exports: 2,
                slots: 2
            })
        );
        assert_eq!(pool_harm_window(5, 0, 0), None);
    }

    /// The run-mode label decides whether the throughput self-check keeps its ACTIONABLE tail ("check governor sheds / adaptive batch shrinks / source load") or excuses the drop as source sharing.
    #[test]
    fn run_mode_labels_report_the_concurrency_that_happened() {
        use super::{pool_is_concurrent, pool_mode_label, run_mode_label, wave_mode_label};
        assert_eq!(wave_mode_label(3), "wave-parallel-processes");
        assert_eq!(
            wave_mode_label(1),
            "wave-sequential",
            "a `--parallel` run the cost gate serialized did not share the source"
        );
        assert_eq!(
            wave_mode_label(0),
            "wave-sequential",
            "the non-parallel wave path never raises the peak"
        );
        assert_eq!(pool_mode_label(true), "pool");
        assert_eq!(
            pool_mode_label(false),
            "pool-serial",
            "a pool that never overlapped must not be labelled a pool run"
        );
        assert!(pool_is_concurrent(5, 12, 0) && pool_is_concurrent(2, 2, 0));
        assert!(!pool_is_concurrent(5, 1, 0), "one export cannot overlap");
        assert!(!pool_is_concurrent(1, 8, 0), "one slot cannot overlap");
        assert!(!pool_is_concurrent(4, 0, 0));
        assert!(
            !pool_is_concurrent(5, 0, 8),
            "heavies serialize against each other, so an all-heavy queue never \
             overlaps however many slots were asked for"
        );
        assert!(
            pool_is_concurrent(5, 1, 7),
            "one parallel_safe export rides alongside the running heavy"
        );
        assert_eq!(run_mode_label(4, true), "parallel-processes");
        assert_eq!(run_mode_label(4, false), "parallel-threads");
        for processes in [true, false] {
            assert_eq!(
                run_mode_label(1, processes),
                "sequential",
                "a top-level run that overlapped nothing must keep the actionable \
                 attribution, whichever path asked for concurrency"
            );
            assert_eq!(run_mode_label(0, processes), "sequential");
        }
    }

    /// The wave runner's label is a PER-EXPORT fact, not a run-level one.
    #[test]
    fn a_wave_export_is_labelled_by_the_batch_it_ran_in_not_the_runs_widest() {
        use super::{RunModes, cost_gate_batches, wave_batch_modes, wave_mode_label};
        use crate::config::{ExportConfig, sample_export};
        let safe = |n: &str| {
            let mut e = sample_export(n);
            e.parallel_safe = Some(true);
            e
        };
        let exports = [
            sample_export("orders"),
            sample_export("events"),
            safe("dim_a"),
            safe("dim_b"),
            safe("dim_c"),
        ];
        let pending: Vec<&ExportConfig> = exports.iter().collect();
        let batches = cost_gate_batches(&pending);
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![1, 1, 3],
            "the cost gate emits one single-child batch per heavy export plus \
             one concurrent batch for the parallel_safe ones"
        );
        let peak = batches.iter().map(Vec::len).max().unwrap();
        assert_eq!(wave_mode_label(peak), "wave-parallel-processes");

        let modes: std::collections::HashMap<String, &'static str> =
            wave_batch_modes(&batches).into_iter().collect();
        assert_eq!(modes.len(), exports.len(), "every export gets a label");
        for lone in ["orders", "events"] {
            assert_eq!(
                modes[lone], "wave-sequential",
                "'{lone}' ran in a single-child batch, and batches run one after \
                 another — it shared the source with nothing"
            );
        }
        for batched in ["dim_a", "dim_b", "dim_c"] {
            assert_eq!(
                modes[batched], "wave-parallel-processes",
                "'{batched}' really did run alongside its two siblings"
            );
        }

        let resolver = RunModes::per_export(wave_mode_label(peak), modes);
        assert_eq!(resolver.for_export("orders"), "wave-sequential");
        assert_eq!(resolver.for_export("dim_b"), "wave-parallel-processes");
        assert_eq!(
            resolver.for_export("an-export-no-batch-covered"),
            "wave-parallel-processes",
            "an export the map never saw falls back to the RUN label — hedged \
             text on a serial export is harmless, a false 'check governor \
             sheds' on a concurrent one is not"
        );
    }

    /// The pool's label is a PER-EXPORT fact too, and for a reason its own run-level predicate already documents.
    #[test]
    fn a_pool_export_is_labelled_by_the_overlap_it_measured_not_the_runs_widest() {
        use super::{RunModes, pool_export_modes, pool_is_concurrent, pool_mode_label};
        assert!(
            pool_is_concurrent(5, 1, 7),
            "one parallel_safe export rides alongside the running heavy"
        );
        let mut windows: Vec<(String, i64, i64)> =
            vec![("dim_small".into(), 0, 50), ("orders".into(), 0, 100)];
        for (i, start) in (100..700).step_by(100).enumerate() {
            windows.push((format!("heavy_{i}"), start, start + 100));
        }
        let modes: std::collections::HashMap<String, &'static str> =
            pool_export_modes(&windows).into_iter().collect();
        assert_eq!(modes.len(), windows.len(), "every export gets a label");
        for overlapped in ["dim_small", "orders"] {
            assert_eq!(
                modes[overlapped],
                pool_mode_label(true),
                "'{overlapped}' really did share the window with a sibling"
            );
        }
        for solo in (0..6).map(|i| format!("heavy_{i}")) {
            assert_eq!(
                modes[&solo], "pool-serial",
                "'{solo}' ran after the safe work drained, alone — it must keep \
                 the actionable pointer however concurrent the RUN was"
            );
        }
        assert_eq!(
            pool_export_modes(&[("a".into(), 0, 100), ("b".into(), 100, 200)])
                .into_iter()
                .map(|(_, m)| m)
                .collect::<Vec<_>>(),
            vec!["pool-serial", "pool-serial"],
            "back-to-back exports did not overlap"
        );

        let resolver = RunModes::per_export(pool_mode_label(true), modes);
        assert_eq!(resolver.for_export("heavy_3"), "pool-serial");
        assert_eq!(resolver.for_export("orders"), "pool");
        assert_eq!(
            resolver.for_export("an-export-no-window-covered"),
            "pool",
            "an export the map never saw falls back to the RUN label — the same \
             fail-safe direction the wave twin uses"
        );

        let whole = include_str!("run.rs");
        let code: String = whole[..whole.find("\n#[cfg(test)]").expect("test modules")]
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let pool_body = &code[code
            .find("\npub(crate) fn run_pool(")
            .expect("run_pool's signature moved — update the anchor")..];
        assert!(
            pool_body.contains("pool_export_modes(&pool_windows)")
                && pool_body.contains("RunModes::per_export("),
            "the pool tail must attribute per export from the windows it \
             measured; `RunModes::uniform` here is the defect"
        );
    }

    /// `apply --parallel` that serialises the whole run must SAY so, at a level the operator sees.
    #[test]
    fn a_degenerate_pool_apply_warns_instead_of_serialising_in_silence() {
        use super::{degenerate_pool_warning, pool_safe_heavy_split};
        use crate::config::{ExportConfig, sample_export};
        let safe = |n: &str| {
            let mut e = sample_export(n);
            e.parallel_safe = Some(true);
            e
        };

        let generated = [
            sample_export("orders"),
            sample_export("events"),
            sample_export("users"),
        ];
        let refs: Vec<&ExportConfig> = generated.iter().collect();
        let (s, h) = pool_safe_heavy_split(&refs);
        assert_eq!((s, h), (0, 3), "a generated config is all-heavy");
        let line = degenerate_pool_warning(s, h, 4)
            .expect("a `--pool` run that cannot overlap anything must say so");
        assert!(
            line.contains("no concurrency") && line.contains("3 export(s)"),
            "the warning must name the symptom: {line}"
        );
        assert!(
            line.contains("parallel_safe: true") && line.contains("--annotate-waves"),
            "…and the lever, or it is a complaint rather than a diagnostic: {line}"
        );

        let mixed = [sample_export("orders"), safe("dim_a"), safe("dim_b")];
        let mrefs: Vec<&ExportConfig> = mixed.iter().collect();
        let (ms, mh) = pool_safe_heavy_split(&mrefs);
        assert_eq!(
            degenerate_pool_warning(ms, mh, 4),
            None,
            "a pool with safe exports overlaps them — nothing to warn about"
        );
        assert_eq!(degenerate_pool_warning(0, 3, 1), None);
        assert_eq!(degenerate_pool_warning(0, 1, 4), None);
    }

    /// RED against returning `None` unconditionally (the shipped silence) and against firing when the gate DID batch something (the noise failure.
    #[test]
    fn a_degenerate_parallel_apply_warns_instead_of_serialising_in_silence() {
        use super::{cost_gate_batches, degenerate_parallel_warning};
        use crate::config::{ExportConfig, sample_export};
        let widths = |exports: &[ExportConfig]| -> Vec<usize> {
            let refs: Vec<&ExportConfig> = exports.iter().collect();
            cost_gate_batches(&refs).iter().map(Vec::len).collect()
        };
        let safe = |n: &str| {
            let mut e = sample_export(n);
            e.parallel_safe = Some(true);
            e
        };

        let hand_written = [
            sample_export("orders"),
            sample_export("events"),
            sample_export("users"),
        ];
        let w = widths(&hand_written);
        assert_eq!(w, vec![1, 1, 1], "the cost gate serialised the whole wave");
        let line = degenerate_parallel_warning("1", &w)
            .expect("a `--parallel` run with no concurrency must say so");
        assert!(
            line.contains("no concurrency") && line.contains("3 export(s)"),
            "the warning must name the symptom: {line}"
        );
        assert!(
            line.contains("parallel_safe: true") && line.contains("rivet plan"),
            "…and the lever, or it is a complaint rather than a diagnostic: {line}"
        );

        let mixed = [sample_export("orders"), safe("dim_a"), safe("dim_b")];
        assert_eq!(widths(&mixed), vec![1, 2]);
        assert_eq!(degenerate_parallel_warning("1", &widths(&mixed)), None);
        assert_eq!(
            degenerate_parallel_warning("1", &widths(&[safe("only")])),
            None
        );
        assert_eq!(degenerate_parallel_warning("1", &[]), None);

        let whole = include_str!("run.rs");
        let code: String = whole[..whole.find("\n#[cfg(test)]").expect("test modules")]
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let waves = &code[code
            .find("\npub(crate) fn run_waves(")
            .expect("run_waves' signature moved — update the anchor")..];
        let at = waves
            .find("degenerate_parallel_warning(")
            .expect("run_waves must call the diagnostic");
        let statement = &waves[at.saturating_sub(200)..waves.len().min(at + 400)];
        assert!(
            statement.contains("log::warn!"),
            "the diagnostic must be emitted at WARN — the default log filter is \
             `warn`, so an `info` line about a run being slow reaches nobody:\n\
             {statement}"
        );
        let run_batches = waves
            .find("for batch in &batches {")
            .expect("run_waves runs its batches in a loop");
        assert!(
            at < run_batches,
            "the warning must print BEFORE the wave's batches execute — a \
             diagnostic that lands after the slow run has already happened is a \
             post-mortem, not a lever"
        );
    }

    /// The peak a pool can reach is capped by BOTH the slots and the shape of the queue.
    #[test]
    fn pool_peak_counts_the_heavy_serialization_not_just_the_slots() {
        use super::{next_eligible, pool_peak_concurrency};
        assert_eq!(pool_peak_concurrency(5, 12, 0), 5);
        assert_eq!(pool_peak_concurrency(5, 3, 0), 3);
        assert_eq!(pool_peak_concurrency(1, 8, 0), 1);
        assert_eq!(pool_peak_concurrency(5, 0, 0), 0);
        assert_eq!(
            pool_peak_concurrency(5, 0, 8),
            1,
            "eight heavies through five slots still run one at a time"
        );
        assert_eq!(pool_peak_concurrency(5, 2, 6), 3);
        assert_eq!(
            pool_peak_concurrency(2, 9, 4),
            2,
            "the slot cap still binds when the queue could go wider"
        );
        assert_eq!(next_eligible(&[false, false, false], true), None);
        assert_eq!(next_eligible(&[false, true, false], true), Some(1));
        assert_eq!(next_eligible(&[false, false], false), Some(0));
    }

    /// The serialized frame must also drop the LEVER.
    #[test]
    fn run_harm_verdict_for_a_serialized_run_names_no_concurrency_lever() {
        let spills = vec![
            ("Created_tmp_disk_tables".to_string(), 40_i64),
            ("mysql_created_tmp_disk_tables".to_string(), 60_i64),
        ];
        let line = run_harm_verdict(&spills, HarmWindow::Serial { exports: 12 })
            .expect("100 reaches the shared threshold");
        assert!(
            line.contains("12 exports, run one at a time"),
            "the frame must say the exports did not overlap: {line}"
        );
        assert!(
            !line.contains("concurrent"),
            "a serialized run must claim no concurrency: {line}"
        );
        assert!(
            !line.contains("--pool") && !line.contains("export concurrency"),
            "there is no concurrency to lower on this path — naming the lever \
             sends the operator to shrink a 1: {line}"
        );
        assert!(
            line.contains("`chunk_size`") && line.contains("`tuning.batch_size`"),
            "the levers this window DOES have must still be named: {line}"
        );
    }

    /// A runner may not spell its own concurrency claim as a LITERAL.
    #[test]
    fn a_runner_never_spells_its_concurrency_claim_as_a_literal() {
        let whole = include_str!("run.rs");
        let src = &whole[..whole
            .find("\n#[cfg(test)]")
            .expect("run.rs has test modules")];
        let code: String = src
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        for (needle, owner) in [
            ("\"wave-parallel-processes\"", "wave_mode_label"),
            ("\"wave-sequential\"", "wave_mode_label"),
            ("\"pool\"", "pool_mode_label"),
            ("\"pool-serial\"", "pool_mode_label"),
            ("\"parallel-processes\"", "run_mode_label"),
            ("\"parallel-threads\"", "run_mode_label"),
            ("\"sequential\"", "run_mode_label"),
        ] {
            assert_eq!(
                code.matches(needle).count(),
                1,
                "{needle} must appear ONCE, inside {owner} — a second occurrence is a \
                 runner claiming a concurrency it did not measure"
            );
        }
        for call in [
            "wave_mode_label(peak_concurrency)",
            "pool_mode_label(really_concurrent)",
            "pool_harm_window(m, safe_pending, heavy_pending)",
            "pool_is_concurrent(m, safe_pending, heavy_pending)",
            "run_mode_label(exports.len(), true)",
            "run_mode_label(peak_concurrency, false)",
        ] {
            assert!(
                code.contains(call),
                "expected the runner to decide from what happened: `{call}`"
            );
        }
        let pool_body = &code[code
            .find("\npub(crate) fn run_pool(")
            .expect("run_pool's signature moved — update the anchor")..];
        assert!(
            !pool_body.contains("HarmWindow::"),
            "the pool must route its harm frame through `pool_harm_window`, not \
             construct a window from the invocation"
        );
    }
}

#[cfg(test)]
pub(crate) mod run_tail_tests {
    use super::{
        RunModes, fold_failures, owns_throughput_self_check, pool_safe_heavy_split,
        reports_run_aggregate, self_check_throughput_as, snapshot_then_stamp, tail_plan,
    };
    use crate::config::ExportConfig;

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 256, ..Default::default()
        })]

        /// TOTALITY over any composition: the fold errs IFF the batch holds a failure, and when a DataIntegrityError is anywhere in the mix the representative marker survives the context chain (exit 3, never 1) no matter where it sits or how much noise surrounds it.
        #[test]
        fn fold_failures_errs_iff_nonempty_and_integrity_marker_survives_anywhere(
            plain in 0usize..4,
            integrity_at in proptest::option::of(0usize..4),
        ) {
            let mut failures: Vec<anyhow::Error> =
                (0..plain).map(|i| anyhow::anyhow!("plain failure {i}")).collect();
            if let Some(at) = integrity_at {
                let at = at.min(failures.len());
                failures.insert(
                    at,
                    anyhow::Error::new(crate::error::DataIntegrityError::new(
                        "checksum mismatch",
                    )),
                );
            }
            let n = failures.len();
            let out = fold_failures(failures, "prop batch");
            proptest::prop_assert_eq!(out.is_err(), n > 0, "err IFF non-empty (n={})", n);
            if integrity_at.is_some() {
                let err = out.unwrap_err();
                proptest::prop_assert!(
                    err.chain().any(|c| c.downcast_ref::<crate::error::DataIntegrityError>().is_some()),
                    "the DataIntegrityError marker must survive the fold's context \
                     chain wherever it sat: {err:#}"
                );
            }
        }
    }

    /// `fold_failures` is the shared representative-failure fold the three orchestrator tails route through (arch-roast 2026-08-21).
    #[test]
    fn fold_failures_returns_ok_only_when_empty_and_keeps_the_typed_marker() {
        use crate::error::DataIntegrityError;

        assert!(fold_failures(Vec::new(), " in the pool").is_ok());

        let one = fold_failures(
            vec![DataIntegrityError::new("rows differ").into()],
            " across waves",
        )
        .expect_err("a failure must not fold to Ok");
        assert_eq!(
            crate::error::classify_exit(&one),
            3,
            "a single data-integrity failure must still exit 3"
        );

        let many = fold_failures(
            vec![
                anyhow::anyhow!("plain boom"),
                DataIntegrityError::new("rows differ").into(),
            ],
            " in the pool",
        )
        .expect_err("failures must not fold to Ok");
        assert_eq!(
            crate::error::classify_exit(&many),
            3,
            "the data-integrity marker must survive under the summary context, \
             or a mixed batch exits on the wrong reason"
        );
        let msg = format!("{many:#}");
        assert!(
            msg.contains("2 export(s) failed in the pool"),
            "the fold must count every failure and carry the caller's context; got: {msg}"
        );
        assert!(
            msg.contains("plain boom"),
            "the non-representative failures must still be listed; got: {msg}"
        );
    }

    #[test]
    fn fold_failures_exits_on_the_primary_class_never_on_another_exports_text() {
        use crate::error::{CodedError, classify_exit, codes};
        let permanent = vec![
            anyhow::Error::new(CodedError::new(
                codes::CONFIG_NO_EXPORTS,
                "export 'session_timeouts' has a dns_records column",
            )),
            anyhow::anyhow!("unsupported column type"),
        ];
        assert_eq!(classify_exit(&fold_failures(permanent, "").unwrap_err()), 1);
        let retryable = vec![
            anyhow::anyhow!("connection reset by peer"),
            anyhow::anyhow!("permission denied on table x"),
        ];
        assert_eq!(classify_exit(&fold_failures(retryable, "").unwrap_err()), 2);
    }

    /// Captures WARN records, because the run-over-run self-check has no other observable.
    struct WarnCapture;
    static WARN_LINES: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    static WARN_CAPTURE: WarnCapture = WarnCapture;

    impl log::Log for WarnCapture {
        fn enabled(&self, m: &log::Metadata) -> bool {
            m.level() <= log::Level::Warn
        }
        fn log(&self, r: &log::Record) {
            if r.level() <= log::Level::Warn {
                WARN_LINES
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(r.args().to_string());
            }
        }
        fn flush(&self) {}
    }

    pub(crate) fn install_warn_capture() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            let _ = log::set_logger(&WARN_CAPTURE);
            log::set_max_level(log::LevelFilter::Warn);
        });
    }

    /// The aggregate a run of `n` exports leaves its tail, built by the REAL producer (`aggregate::build`) over the per-export entries.
    fn aggregate_of(n: usize) -> crate::state::RunAggregate {
        let entries: Vec<crate::state::RunAggregateEntry> = (0..n)
            .map(|i| crate::state::RunAggregateEntry {
                snapshot_rows: 0,
                export_name: format!("export_{i}"),
                status: "success".to_string(),
                run_id: "this-run".to_string(),
                answered_by_run: None,
                rows: 10,
                files: 1,
                bytes: 0,
                bytes_read: 0,
                duration_ms: 5,
                mode: "full".to_string(),
                error_message: None,
                skip_reason: None,
            })
            .collect();
        let now = chrono::Utc::now();
        super::aggregate::build(
            entries,
            now,
            now,
            Some("cfg.yaml"),
            super::run_mode_label(n, false),
        )
    }

    pub(crate) fn captured_warnings_mentioning(needle: &str) -> Vec<String> {
        WARN_LINES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|l| l.contains(needle))
            .cloned()
            .collect()
    }

    /// `--json` / `--summary-output` must emit a document on EVERY run, including one that exports nothing.
    #[test]
    fn either_machine_channel_asks_for_the_document() {
        use super::machine_document_requested;
        let out = std::path::Path::new("summary.json");
        assert!(machine_document_requested(Some(out), false));
        assert!(machine_document_requested(None, true));
        assert!(machine_document_requested(Some(out), true));
        assert!(!machine_document_requested(None, false));
    }

    #[test]
    fn tail_plan_emits_a_machine_document_for_a_zero_export_run() {
        assert!(
            tail_plan(&aggregate_of(0), true).machine_output,
            "a zero-export run must still write --summary-output / print --json"
        );
        assert!(
            !tail_plan(&aggregate_of(0), true).aggregate,
            "no card, no run_aggregate row"
        );
        assert!(tail_plan(&aggregate_of(1), true).machine_output);
        assert!(!tail_plan(&aggregate_of(1), true).aggregate);
        assert!(tail_plan(&aggregate_of(2), true).aggregate);
        assert!(
            !tail_plan(&aggregate_of(2), true).machine_output,
            "the aggregate path writes --summary-output via persist; a second \
             write here would race it"
        );
        assert!(!tail_plan(&aggregate_of(0), false).machine_output);
        assert!(!tail_plan(&aggregate_of(1), false).machine_output);
    }

    /// Exactly once per export per run — the half that is easy to get backwards.
    #[test]
    fn a_reexecd_child_defers_the_throughput_self_check_to_its_parent() {
        assert!(
            !owns_throughput_self_check(true),
            "a re-exec'd child must defer — its parent aggregates the same row, \
             and the child's stderr is captured to a file"
        );
        assert!(
            owns_throughput_self_check(false),
            "a top-level run owns its own self-check — deferring it to a parent \
             that does not exist is how the check goes silent everywhere"
        );
    }

    /// …and the MARKER the deferral keys on must be one only a real parent can set.
    #[test]
    fn the_documented_ipc_variable_alone_does_not_silence_the_self_check() {
        use super::is_reexecd_child;
        assert!(
            !is_reexecd_child(true, false),
            "`RIVET_IPC_EVENTS` is a documented user-facing switch — a process \
             that merely emits NDJSON progress has no parent to defer to, and \
             deferring there is how the check goes silent everywhere"
        );
        assert!(
            !is_reexecd_child(false, true),
            "the internal marker without the IPC stream is not a child either"
        );
        assert!(
            is_reexecd_child(true, true),
            "a real re-exec'd child carries BOTH markers and must defer"
        );
        assert!(
            !is_reexecd_child(false, false),
            "a plain run owns its check"
        );
        let spawner = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/pipeline/parallel_children.rs"),
        )
        .expect("read the spawn site");
        let code = pipeline_product_code(&spawner);
        assert_eq!(
            code.matches(".env(ENV_IPC_EVENTS").count(),
            code.matches("ENV_PARENT_SELF_CHECK").count(),
            "every child that gets the IPC flag must also get the parent's \
             self-check claim, or the child reports a regression into a captured \
             stderr file while the parent reports it too"
        );
        assert!(
            code.contains("ENV_PARENT_SELF_CHECK"),
            "the spawn site must claim the child's self-check"
        );
    }

    /// The same contract one layer up, at the EMISSION.
    #[test]
    fn a_reexecd_child_stays_silent_while_a_parent_emits_the_regression() {
        install_warn_capture();
        let export = "run_tail_selfcheck_probe_export";
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        state
            .record_metric_full(&crate::state::MetricRow {
                export_name: export.to_string(),
                run_id: "baseline-run".to_string(),
                duration_ms: 5_000,
                total_rows: 20_000,
                status: "success".to_string(),
                mode: Some("full".to_string()),
                ..Default::default()
            })
            .expect("record the baseline success");
        let entries = vec![crate::state::RunAggregateEntry {
            snapshot_rows: 0,
            export_name: export.to_string(),
            status: "success".to_string(),
            run_id: "this-run".to_string(),
            answered_by_run: None,
            rows: 20_000,
            files: 1,
            bytes: 0,
            bytes_read: 0,
            duration_ms: 20_000,
            mode: "full".to_string(),
            error_message: None,
            skip_reason: None,
        }];

        self_check_throughput_as(&state, &entries, &RunModes::uniform("sequential"), true);
        assert!(
            captured_warnings_mentioning(export).is_empty(),
            "a re-exec'd child must emit nothing — its stderr is captured to a \
             file and its parent reports the same export: {:?}",
            captured_warnings_mentioning(export)
        );

        self_check_throughput_as(&state, &entries, &RunModes::uniform("sequential"), false);
        let lines = captured_warnings_mentioning(export);
        assert_eq!(
            lines.len(),
            1,
            "a top-level run must report its own regression exactly once (0 \
             lines also means the WARN capture never installed): {lines:?}"
        );
        assert!(
            lines[0].contains("slower than its last success"),
            "the line must be the run-over-run self-check: {}",
            lines[0]
        );

        if std::env::var_os(super::ENV_PARENT_SELF_CHECK).is_none() {
            super::self_check_throughput(&state, &entries, &RunModes::uniform("sequential"));
            assert_eq!(
                captured_warnings_mentioning(export).len(),
                2,
                "the seam every tail calls must reach the same report"
            );
        }
    }

    /// The wave runner's per-export label, at the EMISSION.
    #[test]
    fn a_lone_wave_export_keeps_the_actionable_attribution_in_a_wide_run() {
        install_warn_capture();
        let lone = "run_tail_wave_lone_probe_export";
        let batched = "run_tail_wave_batched_probe_export";
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let mut entries = Vec::new();
        for name in [lone, batched] {
            state
                .record_metric_full(&crate::state::MetricRow {
                    export_name: name.to_string(),
                    run_id: "baseline-run".to_string(),
                    duration_ms: 5_000,
                    total_rows: 20_000,
                    status: "success".to_string(),
                    mode: Some("full".to_string()),
                    ..Default::default()
                })
                .expect("record the baseline success");
            entries.push(crate::state::RunAggregateEntry {
                snapshot_rows: 0,
                export_name: name.to_string(),
                status: "success".to_string(),
                run_id: "this-run".to_string(),
                answered_by_run: None,
                rows: 20_000,
                files: 1,
                bytes: 0,
                bytes_read: 0,
                duration_ms: 20_000,
                mode: "full".to_string(),
                error_message: None,
                skip_reason: None,
            });
        }
        let mut modes: std::collections::HashMap<String, &'static str> =
            std::collections::HashMap::new();
        modes.insert(lone.to_string(), super::wave_mode_label(1));
        modes.insert(batched.to_string(), super::wave_mode_label(3));
        self_check_throughput_as(
            &state,
            &entries,
            &RunModes::per_export(super::wave_mode_label(3), modes),
            false,
        );

        let lone_lines = captured_warnings_mentioning(lone);
        assert_eq!(lone_lines.len(), 1, "one line per export: {lone_lines:?}");
        assert!(
            lone_lines[0].contains("check governor sheds"),
            "an export that ran in its own single-child batch overlapped \
             nothing — it must keep the actionable pointer: {}",
            lone_lines[0]
        );
        let batched_lines = captured_warnings_mentioning(batched);
        assert_eq!(
            batched_lines.len(),
            1,
            "one line per export: {batched_lines:?}"
        );
        assert!(
            batched_lines[0].contains("share the source")
                && batched_lines[0].contains("wave-parallel-processes"),
            "an export that really did run alongside siblings names its mode: {}",
            batched_lines[0]
        );
    }

    /// The POOL's per-export label, at the EMISSION.
    #[test]
    fn a_lone_pool_export_keeps_the_actionable_attribution_in_a_concurrent_run() {
        install_warn_capture();
        let solo = "run_tail_pool_solo_probe_export";
        let overlapped = "run_tail_pool_overlapped_probe_export";
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let mut entries = Vec::new();
        for name in [solo, overlapped] {
            state
                .record_metric_full(&crate::state::MetricRow {
                    export_name: name.to_string(),
                    run_id: "baseline-run".to_string(),
                    duration_ms: 5_000,
                    total_rows: 20_000,
                    status: "success".to_string(),
                    mode: Some("full".to_string()),
                    ..Default::default()
                })
                .expect("record the baseline success");
            entries.push(crate::state::RunAggregateEntry {
                snapshot_rows: 0,
                export_name: name.to_string(),
                status: "success".to_string(),
                run_id: "this-run".to_string(),
                answered_by_run: None,
                rows: 20_000,
                files: 1,
                bytes: 0,
                bytes_read: 0,
                duration_ms: 20_000,
                mode: "full".to_string(),
                error_message: None,
                skip_reason: None,
            });
        }
        let windows = vec![
            ("a_parallel_safe_export".to_string(), 0, 5_000),
            (overlapped.to_string(), 0, 20_000),
            (solo.to_string(), 20_000, 40_000),
        ];
        let modes: std::collections::HashMap<String, &'static str> =
            super::pool_export_modes(&windows).into_iter().collect();
        self_check_throughput_as(
            &state,
            &entries,
            &RunModes::per_export(super::pool_mode_label(true), modes),
            false,
        );

        let solo_lines = captured_warnings_mentioning(solo);
        assert_eq!(solo_lines.len(), 1, "one line per export: {solo_lines:?}");
        assert!(
            solo_lines[0].contains("check governor sheds"),
            "a pool export whose window overlapped nothing must keep the \
             actionable pointer: {}",
            solo_lines[0]
        );
        let overlapped_lines = captured_warnings_mentioning(overlapped);
        assert_eq!(
            overlapped_lines.len(),
            1,
            "one line per export: {overlapped_lines:?}"
        );
        assert!(
            overlapped_lines[0].contains("share the source")
                && overlapped_lines[0].contains("pool"),
            "an export that really did share its window names its mode: {}",
            overlapped_lines[0]
        );
    }

    /// `apply --pool --split`'s notice promises the run's own predicted makespan "prints with the pool schedule".
    #[test]
    fn the_all_complete_split_resume_return_retracts_the_makespan_promise() {
        let plain = super::nothing_to_run_message(false);
        let after_split = super::nothing_to_run_message(true);
        assert!(
            plain.starts_with("apply --pool: nothing to run"),
            "the plain case keeps its one-line message: {plain}"
        );
        assert!(
            !plain.contains("makespan"),
            "with no `--split` notice emitted there is no forward reference to \
             retract: {plain}"
        );
        assert!(
            after_split.contains("makespan") && after_split.contains("does not print"),
            "the `--split` notice pointed at the predicted-makespan line, and \
             this return is above it — say so: {after_split}"
        );
        assert!(
            after_split.contains("--resume"),
            "and name the way out, since the units are complete rather than \
             broken: {after_split}"
        );
    }

    /// The card + `run_aggregate` row are multi-export-only, and it is ONE rule for the three tails that gate it.
    #[test]
    fn only_a_multi_export_run_reports_an_aggregate() {
        assert!(
            !reports_run_aggregate(&aggregate_of(0)),
            "a zero-export run has nothing to aggregate"
        );
        assert!(
            !reports_run_aggregate(&aggregate_of(1)),
            "one export: the per-export card already said it"
        );
        assert!(
            reports_run_aggregate(&aggregate_of(2)),
            "two exports: the aggregate reports"
        );
        assert!(reports_run_aggregate(&aggregate_of(9)));
        assert_eq!(
            tail_plan(&aggregate_of(1), false).aggregate,
            reports_run_aggregate(&aggregate_of(1))
        );
        assert_eq!(
            tail_plan(&aggregate_of(2), false).aggregate,
            reports_run_aggregate(&aggregate_of(2))
        );
    }

    /// The gate's SUBJECT, not just its predicate.
    #[test]
    fn the_aggregate_gate_reads_the_exports_the_run_actually_ran() {
        let fully_skipped = aggregate_of(0);
        assert_eq!(fully_skipped.total_exports, 0);
        assert!(
            !reports_run_aggregate(&fully_skipped),
            "a run that ran nothing must print no card and write no \
             `run_aggregate` row — the pre-skip config count (5) is what said \
             otherwise"
        );
        assert!(reports_run_aggregate(&aggregate_of(5)));

        let whole = include_str!("run.rs");
        let code: String = whole[..whole.find("\n#[cfg(test)]").expect("test modules")]
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let mut sites = 0;
        let gates = [
            concat!("reports_run", "_aggregate("),
            concat!("tail", "_plan("),
        ];
        for (at, _) in gates.iter().flat_map(|g| code.match_indices(g)) {
            if code[..at].trim_end().ends_with("fn") {
                continue;
            }
            let arg = code[at..]
                .split_once('(')
                .and_then(|(_, rest)| rest.split_once(')'))
                .map(|(a, _)| a.split(',').next().unwrap_or("").trim().to_string())
                .expect("a call has arguments");
            assert!(
                arg == "&agg" || arg == "agg",
                "the aggregate gate must be asked about the aggregate the tail \
                 built, not about a count re-derived at the call site (that is \
                 how `run_waves` came to ask about exports it had already \
                 skipped) — got `{arg}`"
            );
            sites += 1;
        }
        assert!(
            sites >= 3,
            "expected the gate inside `tail_plan` plus `run`'s and \
             `finish_run_tail`'s routing through it — found {sites}"
        );
        for tail_fn in ["fn run_waves", "fn run_pool"] {
            let at = code.find(tail_fn).expect("orchestrator tail exists");
            let body = &code[at..code[at..]
                .find("\npub")
                .or_else(|| code[at..].find("\nfn "))
                .map(|o| at + o)
                .unwrap_or(code.len())];
            assert!(
                body.contains("finish_run_tail("),
                "{tail_fn} no longer routes through finish_run_tail — the \
                 shared-tail invariant (one aggregate, gated card, ungated \
                 self-check) is unenforced for it"
            );
        }
        for (at, _) in code.match_indices("total_exports") {
            let after = code[at + "total_exports".len()..].trim_start();
            assert!(
                !after.starts_with('=') || after.starts_with("=="),
                "run.rs ASSIGNS `total_exports` — a tail that overwrites the \
                 aggregate's own count re-derives the subject the type signature \
                 exists to fix, and it still type-checks: {}",
                code[at..].lines().next().unwrap_or("").trim()
            );
        }
    }

    /// The pool's `(safe, heavy)` census, which every concurrency claim is computed FROM.
    #[test]
    fn the_pool_census_counts_safe_and_heavy_exports_apart() {
        let mk = |name: &str, safe: Option<bool>| -> ExportConfig {
            let line = safe
                .map(|s| format!("parallel_safe: {s}\n"))
                .unwrap_or_default();
            serde_yaml_ng::from_str(&format!(
                "name: {name}\nquery: \"SELECT 1\"\nformat: parquet\ndestination:\n  \
                 type: local\n  path: /tmp\n{line}"
            ))
            .expect("parse test ExportConfig")
        };
        let mixed = [
            mk("s1", Some(true)),
            mk("h1", Some(false)),
            mk("s2", Some(true)),
            mk("h2", None),
            mk("h3", Some(false)),
        ];
        let refs: Vec<&ExportConfig> = mixed.iter().collect();
        assert_eq!(
            pool_safe_heavy_split(&refs),
            (2, 3),
            "two parallel_safe, three heavy (one of them by default)"
        );

        let all_safe: Vec<&ExportConfig> =
            mixed.iter().filter(|e| e.name.starts_with('s')).collect();
        assert_eq!(
            pool_safe_heavy_split(&all_safe),
            (2, 0),
            "no heavy export at all — heavy must be 0, not a divisor"
        );

        let all_heavy: Vec<&ExportConfig> =
            mixed.iter().filter(|e| e.name.starts_with('h')).collect();
        assert_eq!(
            pool_safe_heavy_split(&all_heavy),
            (0, 3),
            "a queue of heavies has ZERO safe exports — the count that decides \
             whether the pool ever overlapped anything"
        );
    }

    /// The window is stamped AFTER the source probe, never before.
    #[test]
    fn the_harm_window_is_stamped_after_the_probe_that_opens_it() {
        let probe_finished = std::cell::Cell::new(None);
        let (before, stamp) = snapshot_then_stamp(|| {
            std::thread::sleep(std::time::Duration::from_millis(25));
            probe_finished.set(Some(chrono::Utc::now()));
            vec![
                ("mysql_created_tmp_disk_tables".to_string(), 7_i64),
                ("mysql_innodb_rows_read".to_string(), 11_i64),
            ]
        });
        assert_eq!(
            before.len(),
            2,
            "the probe's value passes through unchanged"
        );
        let probe_finished = probe_finished.get().expect("the probe ran");
        assert!(
            stamp >= probe_finished,
            "the window start ({stamp}) predates the end of the probe that \
             measures it ({probe_finished}) — the run is being charged for its \
             own instrumentation"
        );
    }

    /// The product half of one `src/pipeline` source file.
    fn pipeline_product_code(text: &str) -> String {
        let mut product = String::with_capacity(text.len());
        let mut skipping = false;
        for line in text.lines() {
            if !skipping && line.starts_with("#[cfg(test)]") {
                skipping = true;
                continue;
            }
            if skipping {
                if line == "}" {
                    skipping = false;
                }
                continue;
            }
            product.push_str(line);
            product.push('\n');
        }
        product
            .lines()
            .map(|line| {
                let bytes = line.as_bytes();
                let (mut in_str, mut escaped, mut cut, mut i) = (false, false, line.len(), 0);
                while i < bytes.len() {
                    match bytes[i] {
                        _ if escaped => escaped = false,
                        b'\\' if in_str => escaped = true,
                        b'"' => in_str = !in_str,
                        b'/' if !in_str && bytes.get(i + 1) == Some(&b'/') => {
                            cut = i;
                            break;
                        }
                        _ => {}
                    }
                    i += 1;
                }
                &line[..cut]
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Every top-level `fn` in `code`, as `(name, body)`.
    fn top_level_fns(code: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for (at, line) in std::iter::once((0usize, code.lines().next().unwrap_or(""))).chain(
            code.match_indices('\n')
                .map(|(i, _)| (i + 1, code[i + 1..].lines().next().unwrap_or(""))),
        ) {
            let decl = line
                .strip_prefix("pub ")
                .or_else(|| {
                    line.starts_with("pub(")
                        .then(|| line.split_once(") ").map(|(_, r)| r))
                        .flatten()
                })
                .unwrap_or(line);
            let decl = decl.strip_prefix("async ").unwrap_or(decl);
            if !decl.starts_with("fn ") {
                continue;
            }
            let name = decl["fn ".len()..]
                .split(['(', '<'])
                .next()
                .unwrap_or_default()
                .to_string();
            let body_start = at
                + code[at..]
                    .find('{')
                    .unwrap_or_else(|| panic!("no body found for `{name}`"));
            let end = code[body_start..]
                .find("\n}")
                .map(|i| body_start + i)
                .unwrap_or_else(|| panic!("no top-level close found for `{name}`"));
            out.push((name, code[body_start..end].to_string()));
        }
        out
    }

    /// The self-check reaches every orchestrator through ONE seam.
    #[test]
    fn every_orchestrator_tail_routes_the_self_check_through_one_seam() {
        let exempt: &[(&str, &str, &str)] = &[
            (
                "job.rs",
                "run_export_job_inner",
                "a PER-EXPORT job, not a run tail: it calls itself only to dispatch a \
                 CDC initial-snapshot child. The orchestrator that drives it (run / \
                 run_waves / run_pool) owns the run's self-check, and emitting one \
                 here too would report every export twice.",
            ),
            (
                "apply_cmd.rs",
                "apply_document",
                "the DOCUMENT `rivet apply` emits on --summary-output / --json, built \
                 from one entry after `run_apply_command`'s tail has already run the \
                 self-check on that entry (or from the answering run of a replay, \
                 which ran nothing); a second check here would report it twice.",
            ),
        ];

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline");
        let mut files: Vec<(String, String)> = Vec::new();
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read src/pipeline") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let rel = path
                    .strip_prefix(&root)
                    .expect("under pipeline")
                    .to_string_lossy()
                    .replace('\\', "/");
                let text = std::fs::read_to_string(&path).expect("read source");
                files.push((rel, pipeline_product_code(&text)));
            }
        }
        assert!(
            files.len() > 10,
            "the pipeline walk found only {} files — the scanner is blind",
            files.len()
        );

        let helper = concat!("aggregate::warn_throughput", "_regressions(");
        let seam = concat!("self_check", "_throughput(");
        let helper_callers: Vec<String> = files
            .iter()
            .flat_map(|(rel, code)| {
                code.match_indices(helper)
                    .map(move |(i, _)| format!("{rel}:{}", code[..i].lines().count() + 1))
            })
            .collect();
        assert_eq!(
            helper_callers.len(),
            1,
            "the aggregate helper must have exactly ONE caller — the \
             `self_check_throughput` seam. A tail calling it directly skips the \
             re-exec'd-child deferral and reports the same export twice. Callers: \
             {helper_callers:?}"
        );

        let job_entry = concat!("run_export", "_job(");
        let apply_entry = concat!("run_export_job", "_with_chunk_source(");
        let aggregate_build = concat!("aggregate::", "build(");
        let mut candidates: Vec<String> = Vec::new();
        for (rel, code) in &files {
            for (name, body) in top_level_fns(code) {
                let builds = body.matches(aggregate_build).count();
                let drives_an_export =
                    body.contains(job_entry) || body.contains(apply_entry) || builds > 0;
                if !drives_an_export {
                    continue;
                }
                candidates.push(format!("{rel}::{name}"));
                if let Some((_, _, why)) = exempt
                    .iter()
                    .find(|(f, n, _)| *f == rel.as_str() && *n == name)
                {
                    assert!(
                        why.len() > 40,
                        "{rel}::{name} is exempt from the run-over-run self-check \
                         with no real reason given"
                    );
                    continue;
                }
                let direct = body
                    .match_indices(seam)
                    .filter(|(i, _)| !body[..*i].ends_with("fn "))
                    .count();
                let routed = body
                    .match_indices("finish_run_tail(")
                    .filter(|(i, _)| !body[..*i].ends_with("fn "))
                    .count();
                let calls = if builds == 0 { direct + routed } else { direct };
                assert!(
                    calls >= builds.max(1),
                    "{rel}::{name} ends a run (it drives an export to completion \
                     and/or builds {builds} run aggregate(s)) but calls the \
                     throughput self-check {calls} time(s). 'EVERY run \
                     self-reports degradation' is the contract, and it was false \
                     on `apply`, on `apply --pool`, and then on `apply \
                     <plan.json>` for a release each. Route the tail through \
                     `self_check_throughput`, or add it to this test's exemption \
                     list WITH the reason it must not."
                );
            }
        }
        assert!(
            candidates.len() >= 5,
            "expected at least the four orchestrator tails plus the exempt \
             per-export job; parsed {candidates:?}"
        );
        for (file, name, _) in exempt {
            assert!(
                candidates.contains(&format!("{file}::{name}")),
                "the exemption for {file}::{name} is stale — nothing by that name \
                 drives an export any more. Delete it rather than carrying a \
                 reason for code that no longer exists."
            );
        }

        for (rel, code) in files.iter().filter(|(rel, _)| rel != "run.rs") {
            for (at, _) in code.match_indices("RunModes::") {
                let stmt = &code[at..code.len().min(at + 300)];
                assert!(
                    stmt.contains("_mode_label("),
                    "{rel} builds a `RunModes` without asking a `*_mode_label` \
                     decider for the label — the claim must be derived from what \
                     the run did, never spelled at the call site:\n{}",
                    stmt.lines().take(6).collect::<Vec<_>>().join("\n")
                );
            }
        }
    }
}

#[cfg(test)]
mod wave_grouping_tests {
    use super::super::split::first_name_collision;
    use super::{group_exports_by_wave, is_parallel_safe, next_eligible};

    #[test]
    fn a_synthesized_split_unit_colliding_with_an_existing_export_is_detected() {
        use crate::config::sample_export;
        let units = vec![sample_export("orders#0"), sample_export("orders#1")];
        let existing = vec![sample_export("users"), sample_export("orders#0")];
        assert_eq!(first_name_collision(&units, &existing), Some("orders#0"));
        let clean = vec![sample_export("users"), sample_export("events")];
        assert_eq!(first_name_collision(&units, &clean), None);
    }

    /// The pool's pick rule (#166), both directions.
    #[test]
    fn pool_next_eligible_serializes_heavies_and_backfills_safe() {
        assert_eq!(next_eligible(&[false, true], false), Some(0));
        assert_eq!(next_eligible(&[true, false], false), Some(0));
        assert_eq!(next_eligible(&[false, true, false], true), Some(1));
        assert_eq!(next_eligible(&[true, false], true), Some(0));
        assert_eq!(next_eligible(&[false, false], true), None);
        assert_eq!(next_eligible(&[], false), None);
    }

    #[test]
    fn groups_ascending_with_unscheduled_last() {
        let mut a = crate::config::sample_export("a");
        a.wave = Some(3);
        let mut b = crate::config::sample_export("b");
        b.wave = None;
        let mut c = crate::config::sample_export("c");
        c.wave = Some(1);
        let mut d = crate::config::sample_export("d");
        d.wave = Some(1);

        let exports = vec![a, b, c, d];
        let grouped = group_exports_by_wave(&exports);

        let waves: Vec<u32> = grouped.iter().map(|(w, _)| *w).collect();
        assert_eq!(waves, vec![1, 3, u32::MAX], "ascending, unscheduled last");
        let wave1: Vec<&str> = grouped[0].1.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(wave1, vec!["c", "d"], "same-wave keeps input order");
        assert_eq!(grouped[2].1.len(), 1);
        assert_eq!(
            grouped[2].1[0].name, "b",
            "the no-wave export lands in the last group"
        );
    }

    #[test]
    fn parallel_safe_reads_the_plan_flag() {
        let unset = crate::config::sample_export("unset");
        assert!(!is_parallel_safe(&unset), "None is treated as not-safe");

        let mut safe = crate::config::sample_export("safe");
        safe.parallel_safe = Some(true);
        assert!(is_parallel_safe(&safe), "parallel_safe: true → concurrent");

        let mut not_safe = crate::config::sample_export("heavy");
        not_safe.parallel_safe = Some(false);
        assert!(!is_parallel_safe(&not_safe), "parallel_safe: false → alone");
    }
}

pub(crate) use crate::error::representative_failure_idx;
