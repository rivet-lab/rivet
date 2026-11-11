//! Wall-clock ceilings of unit tests, widened when the binary runs under coverage instrumentation.

use std::time::Duration;

const INSTRUMENTED_FACTOR: u32 = 8;

/// Bytes of the small input of a linearity test; the large one is `SCALE` times this.
pub(crate) const SMALL: usize = 1 << 14;

/// How many times the large input of a linearity test is longer than the small one.
pub(crate) const SCALE: usize = 8;

const LINEAR_SLACK: f64 = 2.5;
const RUNS: usize = 3;
const COST_FLOOR: Duration = Duration::from_millis(1);
const HANG_GUARD: Duration = Duration::from_secs(300);

/// `ceiling` widened by the instrumentation factor when `instrumented`.
fn scaled(ceiling: Duration, instrumented: bool) -> Duration {
    if instrumented {
        ceiling * INSTRUMENTED_FACTOR
    } else {
        ceiling
    }
}

/// An upper wall-clock bound for a test: as written, or widened under `cargo llvm-cov`.
pub(crate) fn ceiling(ceiling: Duration) -> Duration {
    scaled(ceiling, cfg!(coverage))
}

/// CPU time this thread has used: descheduled time under host load is not counted.
fn thread_cpu() -> Duration {
    let mut at = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut at) };
    assert_eq!(rc, 0, "no thread CPU clock");
    Duration::new(at.tv_sec as u64, at.tv_nsec as u32)
}

/// The most CPU time a large run may use for a small run's cost: `SCALE` times, with `LINEAR_SLACK`, never under the floor.
fn linear_allowance(small: Duration) -> Duration {
    small.max(COST_FLOOR).mul_f64(SCALE as f64 * LINEAR_SLACK)
}

/// What `run` returned on `input` and the thread CPU time it took.
fn costed<I, R>(run: &impl Fn(&I) -> R, input: &I) -> (Duration, R) {
    let before = thread_cpu();
    let out = run(input);
    (thread_cpu() - before, out)
}

/// `work` on its own thread; a hang or a panic fails the test, and a hang is never the verdict of a cost.
pub(crate) fn hang_guard<T: Send + 'static>(
    label: &str,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (done, result) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(work());
    });
    result
        .recv_timeout(HANG_GUARD)
        .unwrap_or_else(|why| panic!("{label}: no answer, {why}"))
}

/// The CPU cost of `run` over `small` and over `large`, fastest of `RUNS`, with the output of each.
fn cost_of_both<I: Send + 'static, R: Send + 'static>(
    label: &str,
    small: I,
    large: I,
    run: impl Fn(&I) -> R + Send + 'static,
) -> ((Duration, R), (Duration, R)) {
    hang_guard(label, move || {
        let mut small_runs: Vec<_> = (0..RUNS).map(|_| costed(&run, &small)).collect();
        small_runs.sort_by_key(|(cost, _)| *cost);
        let small_best = small_runs.swap_remove(0);
        let allowed = linear_allowance(small_best.0);
        let mut best: Option<(Duration, R)> = None;
        for _ in 0..RUNS {
            let ran = costed(&run, &large);
            let within = ran.0 <= allowed;
            if best.as_ref().is_none_or(|(kept, _)| ran.0 < *kept) {
                best = Some(ran);
            }
            if within {
                break;
            }
        }
        (small_best, best.expect("a large run"))
    })
}

/// Reads `small` and `large` (`SCALE` times longer) through `run` and fails unless the CPU cost grew about `SCALE` times; returns both outputs.
pub(crate) fn linear_cost<I: Send + 'static, R: Send + 'static>(
    label: &str,
    small: I,
    large: I,
    run: impl Fn(&I) -> R + Send + 'static,
) -> (R, R) {
    let ((small_cost, small_out), (large_cost, large_out)) = cost_of_both(label, small, large, run);
    assert!(
        large_cost <= linear_allowance(small_cost),
        "NOT LINEAR: {label}: {small_cost:?} of CPU on the small input, {large_cost:?} on one {SCALE} times larger"
    );
    (small_out, large_out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cost grown `SCALE` times passes the allowance, one grown `SCALE` squared times does not, and a zero cost is held to the floor.
    #[test]
    fn the_allowance_passes_a_linear_cost_and_refuses_a_quadratic_one() {
        let small = Duration::from_millis(10);
        let scale = SCALE as u32;
        assert!(small * scale * 2 <= linear_allowance(small));
        assert!(small * scale * scale > linear_allowance(small));
        assert!(linear_allowance(Duration::ZERO) >= COST_FLOOR * scale);
    }

    /// The measured costs of a quadratic reading overrun the allowance and those of a linear one do not.
    #[test]
    fn the_cost_of_a_quadratic_reading_overruns_the_allowance_and_a_linear_one_does_not() {
        let spin = |n: usize| (0..n).fold(0u64, |a, i| std::hint::black_box(a + i as u64));
        let quadratic = move |n: &usize| spin(*n * *n / 64);
        let linear = move |n: &usize| spin(*n * 64);
        let (n, m) = (1usize << 13, (1usize << 13) * SCALE);
        let ((small, _), (large, _)) = cost_of_both("linear", n, m, linear);
        assert!(large <= linear_allowance(small), "{small:?} {large:?}");
        let ((small, _), (large, _)) = cost_of_both("quadratic", n, m, quadratic);
        assert!(large > linear_allowance(small), "{small:?} {large:?}");
    }

    #[test]
    fn a_ceiling_is_widened_only_under_instrumentation() {
        let five = Duration::from_secs(5);
        assert_eq!(scaled(five, false), five);
        assert_eq!(scaled(five, true), five * INSTRUMENTED_FACTOR);
    }

    #[test]
    fn the_ceiling_is_widened_exactly_when_the_build_is_instrumented() {
        let five = Duration::from_secs(5);
        let want = if cfg!(coverage) {
            five * INSTRUMENTED_FACTOR
        } else {
            five
        };
        assert_eq!(ceiling(five), want);
    }
}
