//! Shared runner-side wiring for the OPT-2 adaptive concurrency governor (#152).

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::journal::RunEvent;
use crate::plan::ResolvedRunPlan;
use crate::resource::Semaphore;
use crate::source::{self, Source};
use crate::tuning::{BlindSignal, DecisionCause, Governor};

use super::summary::RunSummary;

/// The journalled reason for one parallelism transition.
fn decision_reason(cause: DecisionCause) -> &'static str {
    match cause {
        DecisionCause::PressureRising => "source pressure rising: backed off",
        DecisionCause::PressureEased => "source pressure eased: recovered",
        DecisionCause::SignalUnreadable => {
            "pressure signal unreadable: failing open toward the ceiling"
        }
    }
}

/// The operator-facing WARN for a blind span — or `None` when there is nothing new to say.
fn blind_signal_warning(
    kind: BlindSignal,
    export_name: &str,
    frozen_at: usize,
    ceiling: usize,
) -> Option<String> {
    match kind {
        BlindSignal::Lost => Some(format!(
            "export '{export_name}': governor lost its pressure signal (the monitoring \
             connection can no longer sample) — parallelism was pinned at {frozen_at} of \
             {ceiling}; stepping back toward {ceiling}. Set `adaptive: false` to disarm the \
             governor, or `min_parallel` to floor it"
        )),
        BlindSignal::NeverReadable => None,
    }
}

/// Fold the governor's UP-FRONT arm probe into `gov` and return the operator line the probe earns.
fn arm_probe(
    gov: &mut Governor,
    probe: Option<u64>,
    export_name: &str,
    ceiling: usize,
) -> Option<String> {
    gov.note_arm_probe(probe.is_some());
    probe.is_none().then(|| {
        format!(
            "export '{export_name}': governor armed, but the source provides no pressure signal \
             (engine without a foreign-pressure counter, or the monitor connection cannot sample) \
             — parallelism stays at {ceiling}"
        )
    })
}

/// Recover a poisoned lock instead of panicking.
fn recover<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// RAII permit for ONE unit of work in a POOL-shaped runner.
pub(crate) struct TaskPermit<'a>(&'a Semaphore);

impl<'a> TaskPermit<'a> {
    /// Park until a permit is free, then hold it for the rest of this scope.
    pub(crate) fn acquire(semaphore: &'a Semaphore) -> Self {
        semaphore.acquire();
        Self(semaphore)
    }
    /// Take over a permit the PARENT already acquired (the spawner shape), releasing it on every exit.
    pub(crate) fn adopt(semaphore: &'a Semaphore) -> Self {
        Self(semaphore)
    }
}

impl Drop for TaskPermit<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// RAII `finished` accounting for ONE pool worker.
pub(crate) struct WorkerFinished<'a>(&'a AtomicUsize);

impl<'a> WorkerFinished<'a> {
    /// Bind at the TOP of the worker closure — before any fallible or panicking work.
    pub(crate) fn new(finished: &'a AtomicUsize) -> Self {
        Self(finished)
    }
}

impl Drop for WorkerFinished<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// Does this plan get a governor?
fn should_arm(adaptive: bool, parallel: usize) -> bool {
    adaptive && parallel > 1
}

/// The armed governor for one parallel runner invocation.
pub(crate) struct GovernorHarness {
    floor: usize,
    ceiling: usize,
    monitor: Mutex<Option<Box<dyn Source>>>,
    log: Mutex<Vec<(usize, usize, String)>>,
}

impl GovernorHarness {
    /// Arm the governor for `plan` at `parallel` slots.
    pub(crate) fn arm(plan: &ResolvedRunPlan, parallel: usize) -> Self {
        let floor = plan
            .tuning
            .min_parallel
            .unwrap_or(1)
            .clamp(1, parallel.max(1));
        let monitor: Option<Box<dyn Source>> = if should_arm(plan.tuning.adaptive, parallel) {
            match source::create_source(&plan.source) {
                Ok(s) => {
                    log::info!(
                        "export '{}': adaptive concurrency governor active (parallel {floor}..{parallel})",
                        plan.export_name
                    );
                    Some(s)
                }
                Err(e) => {
                    log::warn!(
                        "export '{}': governor monitoring connection failed; parallelism stays \
                         static at {parallel}: {e:#}",
                        plan.export_name
                    );
                    None
                }
            }
        } else {
            None
        };
        Self {
            floor,
            ceiling: parallel,
            monitor: Mutex::new(monitor),
            log: Mutex::new(Vec::new()),
        }
    }

    /// Spawn the governor thread into `scope` (no-op when unarmed).
    pub(crate) fn spawn_into<'s>(
        &'s self,
        scope: &'s std::thread::Scope<'s, '_>,
        semaphore: &'s Semaphore,
        finished: &'s AtomicUsize,
        total: usize,
        export_name: &'s str,
    ) {
        let Some(mut monitor) = recover(&self.monitor).take() else {
            return;
        };
        let (floor, ceiling) = (self.floor, self.ceiling);
        let log = &self.log;
        scope.spawn(move || {
            let mut gov = Governor::new(ceiling, floor, ceiling);
            let probe = crate::source::Source::sample_governor_pressure(monitor.as_mut());
            if let Some(msg) = arm_probe(&mut gov, probe, export_name, ceiling) {
                log::warn!("{msg}");
            }
            gov.run(
                &mut monitor,
                || finished.load(Ordering::Relaxed) >= total,
                |from, to, cause| {
                    semaphore.resize(to);
                    let reason = decision_reason(cause);
                    if matches!(cause, DecisionCause::PressureRising) {
                        log::warn!(
                            "export '{export_name}': governor parallelism {from} → {to} ({reason}) — raise `min_parallel` to floor it, or set `adaptive: false` to disarm"
                        );
                    } else {
                        log::info!(
                            "export '{export_name}': governor parallelism {from} → {to} ({reason})"
                        );
                    }
                    recover(log).push((from, to, reason.to_string()));
                },
                |kind, frozen_at, ceiling| {
                    if let Some(msg) = blind_signal_warning(kind, export_name, frozen_at, ceiling) {
                        log::warn!("{msg}");
                    }
                },
            );
        });
    }

    /// Record the buffered governor decisions into the run journal.
    pub(crate) fn drain_into(self, summary: &mut RunSummary) {
        for (from, to, reason) in recover(&self.log).drain(..) {
            summary
                .journal
                .record(RunEvent::ParallelismAdjusted { from, to, reason });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    /// Can `n` permits be taken from `sem` within `budget`?
    fn acquires_within(sem: &Arc<Semaphore>, n: usize, budget: Duration) -> bool {
        let got = Arc::new(AtomicBool::new(false));
        let (sem_t, got_t) = (Arc::clone(sem), Arc::clone(&got));
        std::thread::spawn(move || {
            for _ in 0..n {
                sem_t.acquire();
            }
            got_t.store(true, Ordering::Relaxed);
        });
        let deadline = Instant::now() + budget;
        while Instant::now() < deadline {
            if got.load(Ordering::Relaxed) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }

    /// The spawner's permit is released when its worker UNWINDS — a leaked one stalls the spawner at `parallel = 1`.
    #[test]
    fn an_adopted_permit_is_released_by_a_panicking_worker() {
        let sem = Arc::new(Semaphore::new(1));
        sem.acquire();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _p = TaskPermit::adopt(&sem);
            panic!("rivet test: injected worker panic (expected)");
        }));
        assert!(outcome.is_err());
        assert!(
            acquires_within(&sem, 1, Duration::from_secs(5)),
            "the permit leaked: the next acquire never returned"
        );
    }

    /// The two blind trajectories differ in what the OPERATOR is told, and only one of them is an incident.
    #[test]
    fn a_blind_signal_is_only_reported_as_a_loss_when_it_was_ever_readable() {
        let lost = blind_signal_warning(BlindSignal::Lost, "orders", 2, 6)
            .expect("a signal that died mid-run must reach the operator");
        assert!(lost.contains("lost its pressure signal"), "text: {lost}");
        assert!(lost.contains("pinned at 2 of 6"), "text: {lost}");

        assert_eq!(
            blind_signal_warning(BlindSignal::NeverReadable, "orders", 6, 6),
            None,
            "the never-readable engine was already reported ONCE, correctly, by the \
             up-front arm probe — repeating it as a death notice is the false alarm"
        );
    }

    /// A transition taken while the signal is unreadable is a fail-open.
    #[test]
    fn a_blind_step_is_journalled_as_failing_open_not_as_pressure_easing() {
        assert_eq!(
            decision_reason(DecisionCause::PressureRising),
            "source pressure rising: backed off",
            "the live governor suite greps `backed off` — keep it stable"
        );
        assert_eq!(
            decision_reason(DecisionCause::PressureEased),
            "source pressure eased: recovered"
        );
        let blind = decision_reason(DecisionCause::SignalUnreadable);
        assert!(
            blind.contains("unreadable"),
            "a blind step must name the unreadable signal, not claim pressure eased: {blind}"
        );
        assert_ne!(blind, decision_reason(DecisionCause::PressureEased));
    }

    /// A sampler that can never read the pressure signal.
    struct DeadSampler(Arc<AtomicUsize>);

    impl crate::tuning::PressureSource for DeadSampler {
        fn sample(&mut self) -> Option<u64> {
            self.0.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// The arm probe → blind-report seam, driven through the REAL producer rather than a hand-supplied value.
    #[test]
    fn the_arm_probe_readability_decides_whether_a_later_blind_span_is_a_loss() {
        for (probe, want_kind) in [
            (Some(7u64), BlindSignal::Lost),
            (None, BlindSignal::NeverReadable),
        ] {
            let mut gov = Governor::with_intervals(
                4,
                2,
                4,
                Duration::from_millis(1),
                Duration::from_millis(1),
            );
            let line = arm_probe(&mut gov, probe, "orders", 4);
            assert_eq!(
                line.is_some(),
                probe.is_none(),
                "the probe's own line fires exactly when the signal is unreadable (probe={probe:?})"
            );
            if let Some(line) = &line {
                assert!(
                    line.contains("provides no pressure signal"),
                    "the live suite greps this wording: {line}"
                );
            }

            let calls = Arc::new(AtomicUsize::new(0));
            let mut source = DeadSampler(Arc::clone(&calls));
            let stop_calls = Arc::clone(&calls);
            let mut reports: Vec<BlindSignal> = Vec::new();
            gov.run(
                &mut source,
                move || stop_calls.load(Ordering::Relaxed) >= 4,
                |_, _, _| {},
                |kind, _, _| reports.push(kind),
            );

            assert_eq!(
                reports,
                vec![want_kind],
                "one report per blindness episode, and its KIND comes from the arm probe \
                 (probe={probe:?})"
            );
        }
    }

    /// The arm decision, all four ways to break it.
    #[test]
    fn the_governor_arms_only_for_adaptive_with_real_parallelism() {
        assert!(
            should_arm(true, 2),
            "adaptive + parallel 2 is the whole point of the knob"
        );
        assert!(should_arm(true, 8));
        assert!(
            !should_arm(true, 1),
            "one worker has nothing to shed — arming opens a monitoring connection to drive \
             a semaphore that cannot move"
        );
        assert!(
            !should_arm(true, 0),
            "no work at all: `arm` is still called (parallel may be 0), and must not connect"
        );
        assert!(
            !should_arm(false, 8),
            "the user did not opt in — `adaptive: false` must not open a monitoring connection"
        );
        assert!(!should_arm(false, 1));
    }

    /// The POOL shape's shed, end to end through the real guards.
    #[test]
    fn a_shrunken_permit_ceiling_sheds_pool_workers_without_stranding_a_task() {
        const WORKERS: usize = 3;
        const TASKS: usize = 12;

        for ceiling in [1usize, WORKERS] {
            let sem = Arc::new(Semaphore::new(WORKERS));
            sem.resize(ceiling);

            let next = Arc::new(AtomicUsize::new(0));
            let done = Arc::new(AtomicUsize::new(0));
            let live = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let finished = Arc::new(AtomicUsize::new(0));

            for _ in 0..WORKERS {
                let (sem, next, done, live, peak, finished) = (
                    Arc::clone(&sem),
                    Arc::clone(&next),
                    Arc::clone(&done),
                    Arc::clone(&live),
                    Arc::clone(&peak),
                    Arc::clone(&finished),
                );
                std::thread::spawn(move || {
                    let _finish = WorkerFinished::new(&finished);
                    loop {
                        let _permit = TaskPermit::acquire(&sem);
                        if next.fetch_add(1, Ordering::Relaxed) >= TASKS {
                            break;
                        }
                        let now = live.fetch_add(1, Ordering::Relaxed) + 1;
                        peak.fetch_max(now, Ordering::Relaxed);
                        std::thread::sleep(Duration::from_millis(10));
                        live.fetch_sub(1, Ordering::Relaxed);
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }

            let deadline = Instant::now() + Duration::from_secs(10);
            while done.load(Ordering::Relaxed) < TASKS && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(
                done.load(Ordering::Relaxed),
                TASKS,
                "every task must still be claimed at ceiling {ceiling} — a shed slows the run \
                 down, it must never strand work (a permit that is not returned parks the pool \
                 forever)"
            );

            let observed = peak.load(Ordering::Relaxed);
            if ceiling == 1 {
                assert_eq!(
                    observed, 1,
                    "the governor shrank the ceiling to 1, so at most one task may run at a \
                     time; observed {observed} concurrent"
                );
            } else {
                assert!(
                    observed > 1,
                    "activation guard: at the full ceiling this pool MUST overlap, or the \
                     shrunken half above proves nothing; observed {observed} concurrent"
                );
            }

            let deadline = Instant::now() + Duration::from_secs(5);
            while finished.load(Ordering::Relaxed) < WORKERS && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(
                finished.load(Ordering::Relaxed),
                WORKERS,
                "every pool worker must count itself finished — the governor thread's only \
                 exit is `finished >= total`"
            );
        }
    }

    /// A task that UNWINDS must still return its permit and, when the worker dies with it, still count as finished.
    #[test]
    fn a_panicking_pool_task_returns_its_permit_and_counts_its_worker() {
        let sem = Arc::new(Semaphore::new(2));
        let finished = AtomicUsize::new(0);

        for panics in [true, false] {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _finish = WorkerFinished::new(&finished);
                let _permit = TaskPermit::acquire(&sem);
                if panics {
                    panic!("rivet test: injected pool-task panic (expected)");
                }
            }));
            assert_eq!(outcome.is_err(), panics, "task outcome (panics={panics})");
        }

        assert_eq!(
            finished.load(Ordering::Relaxed),
            2,
            "an UNWINDING pool worker must still count as finished, or the governor loops \
             forever and `thread::scope` never joins"
        );
        assert!(
            acquires_within(&sem, 2, Duration::from_secs(5)),
            "both permits must return — a permit leaked by a panicking task parks the pool"
        );
    }

    /// A monitoring connection stand-in for the governor THREAD.
    struct ThreadProbe {
        samples: Arc<AtomicUsize>,
        exited: Arc<AtomicBool>,
    }

    impl Drop for ThreadProbe {
        fn drop(&mut self) {
            self.exited.store(true, Ordering::Relaxed);
        }
    }

    impl crate::source::Source for ThreadProbe {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> crate::error::Result<()> {
            unreachable!("the governor's monitoring connection never exports")
        }
        fn query_scalar(&mut self, _sql: &str) -> crate::error::Result<Option<String>> {
            unreachable!("the governor's monitoring connection never runs scalars")
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _column_overrides: &crate::types::ColumnOverrides,
        ) -> crate::error::Result<Vec<crate::types::TypeMapping>> {
            unreachable!("the governor's monitoring connection never maps types")
        }
        fn sample_governor_pressure(&mut self) -> Option<u64> {
            Some(self.samples.fetch_add(1, Ordering::Relaxed) as u64)
        }
    }

    /// The two halves of [`GovernorHarness::spawn_into`] that no offline test reached.
    #[test]
    fn the_governor_thread_probes_at_arm_and_runs_until_every_worker_is_finished() {
        const POLL_MS: u64 = 200;
        const TOTAL: usize = 2;

        let samples = Arc::new(AtomicUsize::new(0));
        let exited = Arc::new(AtomicBool::new(false));
        let harness = GovernorHarness {
            floor: 1,
            ceiling: 4,
            monitor: Mutex::new(Some(Box::new(ThreadProbe {
                samples: Arc::clone(&samples),
                exited: Arc::clone(&exited),
            }))),
            log: Mutex::new(Vec::new()),
        };
        let semaphore = Semaphore::new(4);
        let finished = AtomicUsize::new(0);

        let mut alive_while_a_worker_ran = false;
        std::thread::scope(|scope| {
            harness.spawn_into(scope, &semaphore, &finished, TOTAL, "orders");
            finished.store(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(2 * POLL_MS));
            alive_while_a_worker_ran = !exited.load(Ordering::Relaxed);
            finished.store(TOTAL, Ordering::Relaxed);
        });

        assert!(
            samples.load(Ordering::Relaxed) >= 1,
            "an armed harness must SPAWN the governor and probe the source up front — without \
             the probe an unsamplable monitor is never reported, and without the thread \
             `adaptive: true` is a silent no-op"
        );
        assert!(
            alive_while_a_worker_ran,
            "the governor's exit is `finished >= total`: with 1 of {TOTAL} workers finished it \
             must still be running, or every run stops adapting the moment its first chunk \
             lands"
        );
        assert!(
            exited.load(Ordering::Relaxed),
            "once `finished` reaches {TOTAL} the thread must return — `thread::scope` cannot \
             join until it does, so a missed exit hangs the run rather than failing it"
        );
        drop(harness);
    }

    /// Call-site pin: the CHECKPOINT parallel chunked runner is governed.
    #[test]
    fn the_parallel_checkpoint_runner_arms_spawns_and_drains_the_governor() {
        let src = include_str!("chunked/parallel_checkpoint.rs");
        assert!(
            src.contains("GovernorHarness::arm(plan, parallel)"),
            "the resumable chunked runner must arm the governor — without it `adaptive: true` \
             is silent on the shape `rivet init` scaffolds"
        );
        assert!(
            src.contains("governor.spawn_into(s, &semaphore, fan.finished(), parallel,"),
            "arming without spawning never resizes anything; `total` is the POOL SIZE here \
             (workers that exit), not the task count"
        );
        assert!(
            src.contains("Some(governor),"),
            "the ParallelismAdjusted events must reach the run journal — FanIn::finish drains \
             the governor it is handed, first"
        );
        assert!(
            src.contains("TaskPermit::acquire(semaphore)"),
            "a pool worker must take its permit PER TASK — one permit held for the worker's \
             whole life is a ceiling the governor can shrink with no effect"
        );
        let drain = src.find("Some(governor),").expect("drain call site");
        let bail = src
            .find("super::incomplete_run_check(")
            .expect("the incomplete-run bail");
        assert!(
            drain < bail,
            "drain BEFORE the error bail, or a failed run journals none of its governor \
             decisions — the drift the keyset copy re-introduced once"
        );
    }

    /// Call-site pin: the chunked spawner's worker takes over the parent's permit with the guard and leaves `finished` to `FanIn::spawn`.
    #[test]
    fn the_chunked_worker_accounts_for_its_exit_with_the_guard_not_tail_statements() {
        let src = include_str!("chunked/exec.rs");
        assert!(
            src.contains("TaskPermit::adopt(semaphore)"),
            "the chunked worker must adopt the spawner's permit at the top of its closure"
        );
        assert!(
            src.contains("fan.spawn("),
            "FanIn::spawn owns the finished count"
        );
        assert!(
            !src.contains("finished.fetch_add"),
            "a tail `finished.fetch_add` is skipped when the worker unwinds — the guard owns it"
        );
        assert!(
            !src.contains("semaphore.release()"),
            "a tail `semaphore.release()` is skipped when the worker unwinds — the guard owns it"
        );
    }
}
