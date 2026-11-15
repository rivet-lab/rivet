//! Shared adaptive-batch state machine for the per-engine export loops.

use crate::tuning::{ADAPTIVE_SAMPLE_INTERVAL, SourceTuning, next_adaptive_batch_size};

/// Initial probe size: small enough to measure per-row Arrow bytes before a memory cap exists, without a wide-row first batch blowing the budget.
pub(crate) const PROBE_BATCH_SIZE: usize = 500;

pub(crate) use crate::tuning::memory::DEFAULT_BATCH_TARGET_MB;

/// Owns the batch-size state machine shared by all engine export loops.
pub(crate) struct AdaptiveBatchController {
    effective: usize,
    base: usize,
    configured: usize,
    adaptive: bool,
    throttle_ms: u64,
    batch_count: usize,
    last_pressure: Option<u64>,
    cap_applied: bool,
}

impl AdaptiveBatchController {
    /// `configured_batch_size` is the user's resolved `batch_size` — the size is never grown past it.
    pub(crate) fn new(tuning: &SourceTuning, configured_batch_size: usize) -> Self {
        let configured = configured_batch_size.max(1);
        let effective = configured.min(PROBE_BATCH_SIZE);
        Self {
            effective,
            base: effective,
            configured,
            adaptive: tuning.adaptive,
            throttle_ms: tuning.throttle_ms,
            batch_count: 0,
            last_pressure: None,
            cap_applied: false,
        }
    }

    /// Adopt the true configured ceiling once an engine that discovers its row width *mid-stream* can finally compute its memory-driven batch size.
    pub(crate) fn raise_configured_ceiling(&mut self, new_configured: usize) {
        if !self.cap_applied {
            self.configured = self.configured.max(new_configured.max(1));
        }
    }

    /// Seed the adaptive baseline with a pre-loop pressure sample so the first adaptive decision compares against a real prior reading (matches the pre-extraction PG/MySQL behaviour).
    pub(crate) fn seed_pressure(&mut self, initial: Option<u64>) {
        if self.adaptive {
            self.last_pressure = initial;
        }
    }

    /// Rows to request for the next batch.
    pub(crate) fn target(&self) -> usize {
        self.effective
    }

    /// One-shot, applied after the first (probe) batch.
    pub(crate) fn apply_memory_cap(&mut self, memory_target: usize) -> Option<usize> {
        if self.cap_applied {
            return None;
        }
        self.cap_applied = true;
        let target = memory_target.max(1).min(self.configured);
        self.base = target;
        if target != self.effective {
            self.effective = target;
            Some(target)
        } else {
            None
        }
    }

    /// Call after emitting each batch.
    pub(crate) fn after_batch(
        &mut self,
        sample: impl FnOnce() -> Option<u64>,
    ) -> Option<(usize, bool)> {
        self.batch_count += 1;
        if !self.adaptive || !self.batch_count.is_multiple_of(ADAPTIVE_SAMPLE_INTERVAL) {
            return None;
        }
        let cur = sample()?;
        let under_pressure = self.last_pressure.is_some_and(|prev| cur > prev);
        self.last_pressure = Some(cur);
        let next = next_adaptive_batch_size(self.effective, self.base, under_pressure);
        if next != self.effective {
            self.effective = next;
            Some((next, under_pressure))
        } else {
            None
        }
    }

    /// Pace the source after emitting a batch of `rows_in_batch` rows.
    pub(crate) fn throttle(&self, rows_in_batch: usize) {
        let us = throttle_sleep_us(self.throttle_ms, rows_in_batch, self.configured);
        if us > 0 {
            std::thread::sleep(std::time::Duration::from_micros(us));
        }
    }
}

/// Row-proportional throttle, in **microseconds**.
fn throttle_sleep_us(throttle_ms: u64, rows: usize, configured: usize) -> u64 {
    if throttle_ms == 0 || rows == 0 {
        return 0;
    }
    let configured = configured.max(1) as u128;
    ((throttle_ms as u128 * 1_000 * rows as u128) / configured) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuning::SourceTuning;

    fn tuning(adaptive: bool, throttle_ms: u64) -> SourceTuning {
        let mut t = SourceTuning::from_config(None);
        t.adaptive = adaptive;
        t.throttle_ms = throttle_ms;
        t
    }

    #[test]
    fn starts_at_probe_size_capped_by_configured() {
        let c = AdaptiveBatchController::new(&tuning(false, 0), 50_000);
        assert_eq!(c.target(), PROBE_BATCH_SIZE);
        let c2 = AdaptiveBatchController::new(&tuning(false, 100), 100);
        assert_eq!(c2.target(), 100);
    }

    #[test]
    fn memory_cap_is_one_shot_and_clamped_to_configured() {
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 20_000);
        assert_eq!(c.apply_memory_cap(9_000), Some(9_000));
        assert_eq!(c.target(), 9_000);
        assert_eq!(c.apply_memory_cap(1_000), None);
        assert_eq!(c.target(), 9_000);
    }

    #[test]
    fn memory_cap_never_exceeds_configured() {
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 5_000);
        assert_eq!(c.apply_memory_cap(1_000_000), Some(5_000));
        assert_eq!(c.target(), 5_000);
    }

    #[test]
    fn raise_ceiling_lets_cap_grow_past_static_batch_size() {
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 10_000);
        c.raise_configured_ceiling(500_000);
        assert_eq!(c.apply_memory_cap(480_000), Some(480_000));
        assert_eq!(c.target(), 480_000);
    }

    #[test]
    fn raise_ceiling_only_raises_never_lowers() {
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 10_000);
        c.raise_configured_ceiling(5_000);
        assert_eq!(c.apply_memory_cap(1_000_000), Some(10_000));
        assert_eq!(c.target(), 10_000);
    }

    #[test]
    fn raise_ceiling_after_cap_is_a_no_op() {
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 10_000);
        c.apply_memory_cap(8_000);
        c.raise_configured_ceiling(500_000);
        assert_eq!(c.apply_memory_cap(400_000), None);
        assert_eq!(c.target(), 8_000);
    }

    #[test]
    fn adaptive_only_fires_every_interval() {
        let mut c = AdaptiveBatchController::new(&tuning(true, 0), 50_000);
        c.apply_memory_cap(10_000);
        c.seed_pressure(Some(0));
        for _ in 0..(ADAPTIVE_SAMPLE_INTERVAL - 1) {
            assert_eq!(c.after_batch(|| Some(100)), None);
        }
        let r = c.after_batch(|| Some(100));
        assert!(
            matches!(r, Some((_, true))),
            "expected a shrink under pressure, got {r:?}"
        );
        assert!(c.target() < 10_000);
    }

    #[test]
    fn throttle_full_batch_pauses_the_whole_budget() {
        assert_eq!(throttle_sleep_us(50, 10_000, 10_000), 50_000);
        assert_eq!(throttle_sleep_us(500, 10_000, 10_000), 500_000);
    }

    #[test]
    fn throttle_scales_with_rows_pulled() {
        assert_eq!(throttle_sleep_us(50, 5_000, 10_000), 25_000);
        assert_eq!(throttle_sleep_us(50, 420, 10_000), 2_100);
        assert_eq!(throttle_sleep_us(50, 100, 10_000), 500);
    }

    #[test]
    fn throttle_total_is_independent_of_batch_count() {
        let one = throttle_sleep_us(50, 10_000, 10_000);
        let ten: u64 = (0..10).map(|_| throttle_sleep_us(50, 1_000, 10_000)).sum();
        assert_eq!(one, ten, "10×1k must equal 1×10k");
        let mut total = 0u64;
        let mut left = 10_000usize;
        while left > 0 {
            let n = left.min(420);
            total += throttle_sleep_us(50, n, 10_000);
            left -= n;
        }
        assert_eq!(
            total, 50_000,
            "10k rows in 420-row FETCHes = same 50 ms budget"
        );
    }

    #[test]
    fn throttle_zero_or_empty_never_sleeps() {
        assert_eq!(throttle_sleep_us(0, 10_000, 10_000), 0);
        assert_eq!(throttle_sleep_us(50, 0, 10_000), 0);
    }

    #[test]
    fn adaptive_noop_when_disabled() {
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 50_000);
        c.apply_memory_cap(10_000);
        for _ in 0..(ADAPTIVE_SAMPLE_INTERVAL * 2) {
            assert_eq!(c.after_batch(|| Some(999_999)), None);
        }
        assert_eq!(c.target(), 10_000);
    }
}
