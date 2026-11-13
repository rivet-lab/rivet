//! Bounded work-stealing POOL scheduling (#166) — the makespan-optimal alternative to barrier waves.

/// One export the pool schedules, by its predicted duration.
#[derive(Debug, Clone)]
pub(crate) struct PoolItem {
    pub name: String,
    /// Predicted duration in seconds (last successful run, else an estimate — the same source the wave packer reads).
    pub predicted_secs: f64,
    /// Whether this export may run CONCURRENTLY with another heavy one.
    pub parallel_safe: bool,
}

/// Where a [`PoolItem`]'s predicted duration came from.
#[derive(Debug, Clone)]
pub(crate) enum PredictedFrom {
    /// A prior SUCCESSFUL run's wall time — the only true measurement.
    Measured(f64),
    /// No success on record, but a failed/interrupted attempt ran this long.
    FailedAttemptFloor(f64),
    /// A synthesized split unit inheriting `giant_predicted / N` from a giant that was itself MEASURED.
    SeededSplit(f64),
    /// No history at all: the placeholder.
    Placeholder(f64),
}

/// Placeholder duration for an export with no run history at all.
pub(crate) const POOL_PLACEHOLDER_SECS: f64 = 5.0;

impl PredictedFrom {
    pub(crate) fn secs(&self) -> f64 {
        match self {
            PredictedFrom::Measured(s)
            | PredictedFrom::FailedAttemptFloor(s)
            | PredictedFrom::SeededSplit(s)
            | PredictedFrom::Placeholder(s) => *s,
        }
    }
}

/// Predict one export's duration from the state store.
pub(crate) fn predict_secs(state: &crate::state::StateStore, name: &str) -> PredictedFrom {
    predict_history(state, name).0
}

/// [`predict_secs`] plus the peak RSS that belongs to the SAME history.
pub(crate) fn predict_secs_and_rss(
    state: &crate::state::StateStore,
    name: &str,
) -> (PredictedFrom, Option<i64>) {
    let (from, rss, _) = predict_history(state, name);
    (from, rss)
}

/// The one state-store read behind [`predict_secs`] and [`predict_secs_and_rss`], plus the WHEN.
fn predict_history(
    state: &crate::state::StateStore,
    name: &str,
) -> (PredictedFrom, Option<i64>, Option<String>) {
    if let Some(m) = state.get_last_success_metric(name).ok().flatten() {
        let secs = (m.duration_ms as f64 / 1000.0).max(0.001);
        return (PredictedFrom::Measured(secs), m.peak_rss_mb, Some(m.run_at));
    }
    let attempts: Vec<crate::state::ExportMetric> = state
        .get_metrics(Some(name), 25)
        .ok()
        .into_iter()
        .flatten()
        .collect();
    let longest = attempts.iter().max_by_key(|m| m.duration_ms);
    let peak_rss = attempts.iter().filter_map(|m| m.peak_rss_mb).max();
    match longest {
        Some(m) => (
            PredictedFrom::FailedAttemptFloor(
                (m.duration_ms as f64 / 1000.0)
                    .max(0.001)
                    .max(POOL_PLACEHOLDER_SECS),
            ),
            peak_rss,
            Some(m.run_at.clone()),
        ),
        None => (
            PredictedFrom::Placeholder(POOL_PLACEHOLDER_SECS),
            None,
            None,
        ),
    }
}

/// A split unit's provenance INHERITS the giant's.
pub(crate) fn split_unit_from(giant: &PredictedFrom, share_secs: f64) -> PredictedFrom {
    match giant {
        PredictedFrom::Measured(_) | PredictedFrom::SeededSplit(_) => {
            PredictedFrom::SeededSplit(share_secs)
        }
        PredictedFrom::FailedAttemptFloor(_) => PredictedFrom::FailedAttemptFloor(share_secs),
        PredictedFrom::Placeholder(_) => PredictedFrom::Placeholder(share_secs),
    }
}

/// Reconcile a split unit's inherited SEED with the unit's OWN history.
pub(crate) fn reconcile_split_seed(seed: &PredictedFrom, own: &PredictedFrom) -> PredictedFrom {
    match own {
        PredictedFrom::Placeholder(_) => seed.clone(),
        _ => own.clone(),
    }
}

/// Split a synthesized unit name back into `(giant, ordinal)`.
pub(crate) fn split_unit_name(name: &str) -> Option<(&str, usize)> {
    let (giant, ord) = name.rsplit_once('#')?;
    Some((giant, ord.parse().ok()?))
}

/// How far PAST the current partition the unit-name census probes for ordinals a bigger prior partition left behind.
const SPLIT_CENSUS_LOOKAHEAD: usize = 3;

/// Hard ceiling on the census, so a pathological state store cannot turn run start into thousands of point queries.
const SPLIT_CENSUS_CAP: usize = 256;

/// Re-derive a split cohort's per-unit prediction when the partition CHANGED under it.
pub(crate) fn repartitioned_unit_prediction(
    realized: usize,
    prior: &[UnitHistory],
) -> Option<PredictedFrom> {
    if realized == 0 || prior.is_empty() {
        return None;
    }
    let cohort = newest_generation(prior);
    let prior_n = cohort.iter().map(|u| u.ordinal + 1).max()?;
    if prior_n <= realized {
        return None;
    }
    let share = cohort.iter().map(|u| u.from.secs()).sum::<f64>() / realized as f64;
    if cohort.iter().all(|u| {
        matches!(
            u.from,
            PredictedFrom::Measured(_) | PredictedFrom::SeededSplit(_)
        )
    }) {
        Some(PredictedFrom::SeededSplit(share))
    } else {
        Some(PredictedFrom::FailedAttemptFloor(
            share.max(POOL_PLACEHOLDER_SECS),
        ))
    }
}

/// One censused `{giant}#i`: what its OWN history predicts, and WHEN that history was recorded.
#[derive(Debug, Clone)]
pub(crate) struct UnitHistory {
    pub ordinal: usize,
    pub from: PredictedFrom,
    /// `run_at` of the row the prediction came from ([`predict_history`]).
    pub recorded_at: Option<String>,
}

/// The subset of the census that belongs to the LATEST partition generation.
fn newest_generation(prior: &[UnitHistory]) -> Vec<&UnitHistory> {
    let mut by_recency: Vec<&UnitHistory> = prior.iter().collect();
    by_recency.sort_by(|a, b| {
        b.recorded_at
            .cmp(&a.recorded_at)
            .then_with(|| b.ordinal.cmp(&a.ordinal))
    });
    let mut seen: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
    for (taken, u) in by_recency.iter().enumerate() {
        seen.insert(u.ordinal);
        if seen.last() == Some(&(seen.len() - 1)) {
            return by_recency[..=taken].to_vec();
        }
    }
    by_recency
}

/// The unit-name census for one split giant.
fn split_history_census(
    state: &crate::state::StateStore,
    giant: &str,
    realized: usize,
) -> Vec<UnitHistory> {
    let mut out: Vec<UnitHistory> = Vec::new();
    let mut misses = 0usize;
    for i in 0..SPLIT_CENSUS_CAP {
        let (from, _, recorded_at) = predict_history(state, &format!("{giant}#{i}"));
        if matches!(from, PredictedFrom::Placeholder(_)) {
            if i >= realized {
                misses += 1;
                if misses >= SPLIT_CENSUS_LOOKAHEAD {
                    break;
                }
            }
            continue;
        }
        misses = 0;
        out.push(UnitHistory {
            ordinal: i,
            from,
            recorded_at,
        });
    }
    out
}

/// Fold classifications into the `(measured, failed-attempt, placeholder)` accounting the makespan print grades itself with.
pub(crate) fn classification_counts(fs: &[PredictedFrom]) -> (usize, usize, usize) {
    let (mut measured, mut attempt, mut placeholder) = (0usize, 0usize, 0usize);
    for f in fs {
        match f {
            PredictedFrom::Measured(_) | PredictedFrom::SeededSplit(_) => measured += 1,
            PredictedFrom::FailedAttemptFloor(_) => attempt += 1,
            PredictedFrom::Placeholder(_) => placeholder += 1,
        }
    }
    (measured, attempt, placeholder)
}

/// The "prediction is a LOWER BOUND" line — or `None` when every prediction in the schedule rests on a real success.
pub(crate) fn lower_bound_hedge(attempt_n: usize, placeholder_n: usize) -> Option<String> {
    let unmeasured = attempt_n + placeholder_n;
    (unmeasured > 0).then(|| {
        format!(
            "prediction is a LOWER BOUND: {unmeasured} export(s) have no successful run to \
             measure from ({attempt_n} scheduled at a failed attempt's duration, \
             {placeholder_n} at a {}s placeholder) — it tightens as runs complete",
            POOL_PLACEHOLDER_SECS as i64,
        )
    })
}

/// THE PoolItem constructor — both `rivet plan`'s preview and `apply --pool`'s schedule go through here, so the two commands cannot print different makespans for one config again.
pub(crate) fn predict_items<'a>(
    state: &crate::state::StateStore,
    exports: impl IntoIterator<Item = (&'a str, bool)>,
    split_seeds: &std::collections::HashMap<String, PredictedFrom>,
) -> Vec<(PoolItem, PredictedFrom)> {
    let mut realized_of: std::collections::BTreeMap<&str, usize> =
        std::collections::BTreeMap::new();
    for name in split_seeds.keys() {
        if let Some((giant, _)) = split_unit_name(name) {
            *realized_of.entry(giant).or_default() += 1;
        }
    }
    let recut: std::collections::HashMap<&str, PredictedFrom> = realized_of
        .iter()
        .filter_map(|(giant, realized)| {
            let prior = split_history_census(state, giant, *realized);
            repartitioned_unit_prediction(*realized, &prior).map(|f| (*giant, f))
        })
        .collect();
    exports
        .into_iter()
        .map(|(name, parallel_safe)| {
            let own = predict_secs(state, name);
            let from = match split_seeds.get(name) {
                Some(seed) => split_unit_name(name)
                    .and_then(|(giant, _)| recut.get(giant).cloned())
                    .unwrap_or_else(|| reconcile_split_seed(seed, &own)),
                None => own,
            };
            (
                PoolItem {
                    name: name.to_string(),
                    predicted_secs: from.secs(),
                    parallel_safe,
                },
                from,
            )
        })
        .collect()
}

/// LPT order: longest predicted duration first, ties broken by name so the order is deterministic (two identical `plan` runs must schedule the same).
pub(crate) fn pool_order(items: &[PoolItem]) -> Vec<String> {
    let mut v: Vec<&PoolItem> = items.iter().collect();
    v.sort_by(|a, b| {
        b.predicted_secs
            .total_cmp(&a.predicted_secs)
            .then_with(|| a.name.cmp(&b.name))
    });
    v.into_iter().map(|i| i.name.clone()).collect()
}

/// Predicted makespan (wall-clock) of running `items` through `workers` slots under LPT.
pub(crate) fn predicted_makespan_secs(items: &[PoolItem], workers: usize) -> f64 {
    let w = workers.max(1);
    let mut slots = vec![0.0f64; w];
    let order = pool_order(items);
    let by_name: std::collections::HashMap<&str, f64> = items
        .iter()
        .map(|i| (i.name.as_str(), i.predicted_secs))
        .collect();
    for name in &order {
        let d = by_name[name.as_str()];
        let i = slots
            .iter()
            .enumerate()
            .min_by(|a, b| a.1.total_cmp(b.1))
            .map(|(i, _)| i)
            .unwrap_or(0);
        slots[i] += d;
    }
    let greedy = slots.into_iter().fold(0.0, f64::max);
    let heavy: f64 = items
        .iter()
        .filter(|i| !i.parallel_safe)
        .map(|i| i.predicted_secs)
        .sum();
    greedy.max(heavy)
}

/// The floor no scheduler can beat: `max(longest single export, total/M)`.
pub(crate) fn makespan_floor_secs(items: &[PoolItem], workers: usize) -> f64 {
    let w = workers.max(1) as f64;
    let total: f64 = items.iter().map(|i| i.predicted_secs).sum();
    let longest = items.iter().map(|i| i.predicted_secs).fold(0.0, f64::max);
    let heavy: f64 = items
        .iter()
        .filter(|i| !i.parallel_safe)
        .map(|i| i.predicted_secs)
        .sum();
    longest.max(total / w).max(heavy)
}

/// #167: how many range sub-units a dominating export should split into so the pool floor drops from `longest` toward `total/M`.
pub(crate) fn split_factor(
    predicted_secs: f64,
    second_longest_secs: f64,
    r: f64,
    max_split: usize,
) -> Option<usize> {
    if second_longest_secs <= 0.0 || predicted_secs <= r * second_longest_secs {
        return None;
    }
    let n = (predicted_secs / second_longest_secs).ceil() as usize;
    Some(n.clamp(2, max_split.max(2)))
}

/// Rewrite the item set with the single dominating export split into N equal sub-units (`<name>#0..<name>#N-1`, each `predicted/N`), summing to the original.
pub(crate) fn split_dominating(items: &[PoolItem], r: f64, max_split: usize) -> Vec<PoolItem> {
    if items.len() < 2 {
        return items.to_vec();
    }
    let mut sorted: Vec<&PoolItem> = items.iter().collect();
    sorted.sort_by(|a, b| b.predicted_secs.total_cmp(&a.predicted_secs));
    let (longest, second) = (sorted[0], sorted[1]);
    let Some(n) = split_factor(longest.predicted_secs, second.predicted_secs, r, max_split) else {
        return items.to_vec();
    };
    let piece = longest.predicted_secs / n as f64;
    let mut out: Vec<PoolItem> = items
        .iter()
        .filter(|i| i.name != longest.name)
        .cloned()
        .collect();
    for i in 0..n {
        out.push(PoolItem {
            name: format!("{}#{i}", longest.name),
            predicted_secs: piece,
            parallel_safe: longest.parallel_safe,
        });
    }
    out
}

/// Should `apply --pool` advise splitting a dominating export, and if so into how many + to what wall?
pub(crate) fn advise_split(
    items: &[PoolItem],
    m: usize,
    r: f64,
    max_split: usize,
) -> Option<(String, usize, f64)> {
    let mut by: Vec<&PoolItem> = items.iter().collect();
    by.sort_by(|a, b| b.predicted_secs.total_cmp(&a.predicted_secs));
    let [longest, second, ..] = by.as_slice() else {
        return None;
    };
    let total: f64 = items.iter().map(|i| i.predicted_secs).sum();
    if longest.predicted_secs <= total / m.max(1) as f64 {
        return None;
    }
    if !longest.parallel_safe {
        return None;
    }
    let n = split_factor(longest.predicted_secs, second.predicted_secs, r, max_split)?;
    let broken = predicted_makespan_secs(&split_dominating(items, r, max_split), m);
    if broken >= predicted_makespan_secs(items, m) {
        return None;
    }
    Some((longest.name.clone(), n, broken))
}

#[cfg(test)]
mod lower_bound_tests {
    use super::lower_bound_hedge;

    /// The "LOWER BOUND" claim has ONE source, fed the reconciled counts, shared by `plan` and `apply --pool`.
    #[test]
    fn the_lower_bound_claim_has_one_source_and_reads_the_reconciled_counts() {
        assert!(
            lower_bound_hedge(0, 0).is_none(),
            "a schedule resting entirely on successes is not a lower bound"
        );
        let hedge = lower_bound_hedge(2, 3).expect("5 unmeasured exports must hedge");
        assert!(
            hedge.contains("5 export(s)")
                && hedge.contains("2 scheduled at a failed attempt")
                && hedge.contains("3 at a"),
            "the hedge must count both flavours of unmeasured: {hedge}"
        );
        let needle = concat!("LOWER ", "BOUND");
        let code = |whole: &str| -> String {
            let product = whole
                .find("\n#[cfg(test)]")
                .map_or(whole, |at| &whole[..at]);
            product
                .lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        };
        for (file, src) in [
            ("run.rs", include_str!("run.rs")),
            ("plan_cmd.rs", include_str!("plan_cmd.rs")),
            ("split.rs", include_str!("split.rs")),
        ] {
            assert_eq!(
                code(src).matches(needle).count(),
                0,
                "{file} re-states the hedge"
            );
        }
        let pool = code(include_str!("pool.rs"));
        assert_eq!(pool.matches(needle).count(), 1);
        let at = pool.find(concat!("fn lower_bound", "_hedge")).unwrap();
        assert!(pool[at..].find(needle).unwrap() < pool[at..].find("\n}\n").unwrap());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn it(name: &str, secs: f64) -> PoolItem {
        PoolItem {
            name: name.into(),
            predicted_secs: secs,
            parallel_safe: true,
        }
    }

    /// A heavy (non-parallel_safe) item, for the C3 serialization-floor tests.
    fn heavy(name: &str, secs: f64) -> PoolItem {
        PoolItem {
            name: name.into(),
            predicted_secs: secs,
            parallel_safe: false,
        }
    }

    #[test]
    fn order_is_longest_first_deterministic() {
        let items = vec![it("b", 10.0), it("giant", 900.0), it("a", 10.0)];
        assert_eq!(pool_order(&items), vec!["giant", "a", "b"]);
    }

    /// #166/#167 C3 (roast 2026-08-09): an ALL-HEAVY config cannot beat sequential.
    #[test]
    fn makespan_honors_heavy_serialization() {
        let items = vec![
            heavy("a", 600.0),
            heavy("b", 600.0),
            heavy("c", 600.0),
            heavy("d", 600.0),
        ];
        assert_eq!(
            makespan_floor_secs(&items, 4),
            2400.0,
            "floor = sum of heavies"
        );
        assert_eq!(
            predicted_makespan_secs(&items, 4),
            2400.0,
            "prediction must not advertise M-way parallelism an all-heavy set can't reach"
        );

        let mixed = {
            let mut v = vec![heavy("giant", 600.0)];
            for n in 0..6 {
                v.push(it(&format!("s{n}"), 100.0));
            }
            v
        };
        assert!(
            predicted_makespan_secs(&mixed, 4) >= 600.0,
            "the single heavy still floors the wall"
        );
        let safe = vec![
            it("a", 100.0),
            it("b", 100.0),
            it("c", 100.0),
            it("d", 100.0),
        ];
        assert_eq!(predicted_makespan_secs(&safe, 2), 200.0);
    }

    #[test]
    fn split_factor_only_fires_on_a_dominating_export() {
        assert_eq!(split_factor(3000.0, 300.0, 3.0, 8), Some(8));
        assert_eq!(split_factor(3000.0, 300.0, 3.0, 20), Some(10));
        assert_eq!(split_factor(600.0, 300.0, 3.0, 8), None);
        assert_eq!(split_factor(3000.0, 0.0, 3.0, 8), None);
        assert_eq!(split_factor(310.0, 100.0, 3.0, 8), Some(4));
    }

    /// The floor-breaker (#167), RED against the un-split floor.
    #[test]
    fn splitting_the_giant_breaks_the_pool_floor() {
        let mut items = vec![it("giant", 3060.0)];
        for n in 0..30 {
            items.push(it(&format!("s{n:02}"), 60.0));
        }
        assert_eq!(makespan_floor_secs(&items, 6), 3060.0);
        let unsplit_wall = predicted_makespan_secs(&items, 6);
        assert!(
            unsplit_wall >= 3060.0,
            "giant pins the wall: {unsplit_wall}"
        );

        let split = split_dominating(&items, 3.0, 6);
        assert_eq!(
            split.len(),
            30 + 6,
            "giant replaced by 6 sub-units alongside the 30 small"
        );
        let split_wall = predicted_makespan_secs(&split, 6);
        assert!(
            split_wall < unsplit_wall,
            "the split MUST break the floor: {split_wall} < {unsplit_wall}"
        );
        assert_eq!(split_wall, 810.0, "reaches total/6 = 4860/6");
    }

    /// #167 advisory floor-gate (roast 2026-08-09): advise a split ONLY when the giant is actually the floor.
    #[test]
    fn advise_split_needs_the_giant_to_be_the_floor() {
        let mut dominating = vec![it("giant", 3060.0)];
        for n in 0..30 {
            dominating.push(it(&format!("s{n:02}"), 60.0));
        }
        let advice = advise_split(&dominating, 6, 3.0, 6);
        assert!(advice.is_some(), "a real giant must be advised");
        let (name, n, broken) = advice.unwrap();
        assert_eq!(name, "giant");
        assert!(n >= 2 && broken < 3060.0, "the split must drop the wall");

        let mut balanced = vec![it("tall", 200.0)];
        for n in 0..20 {
            balanced.push(it(&format!("s{n:02}"), 60.0));
        }
        assert!(
            advise_split(&balanced, 2, 3.0, 2).is_none(),
            "no advice when total/m is the wall — a split would be a false alarm"
        );

        let mut all_heavy = vec![heavy("giant", 3060.0)];
        for n in 0..30 {
            all_heavy.push(heavy(&format!("s{n:02}"), 60.0));
        }
        assert!(
            advise_split(&all_heavy, 6, 3.0, 6).is_none(),
            "a heavy giant's split does not lower the wall (heavies serialize) — no advice"
        );
    }

    #[test]
    fn split_dominating_leaves_a_balanced_set_untouched() {
        let items = vec![it("a", 100.0), it("b", 120.0), it("c", 90.0)];
        assert_eq!(split_dominating(&items, 3.0, 8).len(), 3);
    }

    #[test]
    fn a_dominating_giant_is_the_floor_and_the_pool_reaches_it() {
        let mut items = vec![it("giant", 3000.0)];
        for n in 0..30 {
            items.push(it(&format!("s{n:02}"), 60.0));
        }
        assert_eq!(makespan_floor_secs(&items, 4), 3000.0);
        assert_eq!(predicted_makespan_secs(&items, 4), 3000.0);
    }

    #[test]
    fn without_a_giant_the_pool_reaches_total_over_m() {
        let items: Vec<PoolItem> = (0..12).map(|n| it(&format!("e{n:02}"), 100.0)).collect();
        assert_eq!(makespan_floor_secs(&items, 4), 300.0);
        assert_eq!(predicted_makespan_secs(&items, 4), 300.0);
    }

    #[test]
    fn pool_beats_or_matches_the_barrier_and_stays_within_the_lpt_bound() {
        use proptest::prelude::*;
        proptest!(|(durs in proptest::collection::vec(1.0f64..1000.0, 1..40), m in 1usize..8)| {
            let items: Vec<PoolItem> = durs
                .iter()
                .enumerate()
                .map(|(i, &d)| it(&format!("e{i:02}"), d))
                .collect();
            let mk = predicted_makespan_secs(&items, m);
            let floor = makespan_floor_secs(&items, m);
            prop_assert!(mk >= floor - 1e-9, "makespan {mk} below floor {floor}");
            let total: f64 = durs.iter().sum();
            prop_assert!(mk <= total + 1e-9, "makespan {mk} exceeds total {total}");
            if m == 1 { prop_assert!((mk - total).abs() < 1e-6); }
        });
    }
}

#[cfg(test)]
mod prediction_tests {
    use super::{
        POOL_PLACEHOLDER_SECS, PoolItem, PredictedFrom, SPLIT_CENSUS_LOOKAHEAD, UnitHistory,
        classification_counts, newest_generation, pool_order, predict_items, predict_secs,
        reconcile_split_seed, repartitioned_unit_prediction, split_history_census, split_unit_from,
    };
    use crate::state::{MetricRow, StateStore};

    /// One generation's census: the units a single run recorded, stamped in the order that run finished them (`run` orders the runs).
    fn generation(run: u32, units: &[(usize, PredictedFrom)]) -> Vec<UnitHistory> {
        units
            .iter()
            .map(|(ordinal, from)| UnitHistory {
                ordinal: *ordinal,
                from: from.clone(),
                recorded_at: Some(format!("2026-08-{run:02}T00:00:{ordinal:02}+00:00")),
            })
            .collect()
    }

    fn store_with(rows: &[(&str, &str, i64, &str)]) -> StateStore {
        let s = StateStore::open_in_memory().expect("in-memory store");
        for (export, run_id, duration_ms, status) in rows {
            s.record_metric_full(&MetricRow {
                export_name: (*export).into(),
                run_id: (*run_id).into(),
                duration_ms: *duration_ms,
                status: (*status).into(),
                ..Default::default()
            })
            .expect("record metric");
        }
        s
    }

    /// Field find (2026-08-13, --pool 5): an export with FAILED history but no success was scheduled at the flat 5 s placeholder, so a giant that had already demonstrated an hours-long attempt was predicted as noise and the printed makespan promised minutes for an hours-long run.
    #[test]
    fn unmeasured_export_with_failed_history_predicts_the_longest_attempt() {
        let s = store_with(&[
            ("big", "r1", 1_800_000, "failed"),
            ("big", "r2", 3_600_000, "failed"),
            ("big", "r3", 600_000, "interrupted"),
            ("big", "r4", 300_000, "failed"),
        ]);
        match predict_secs(&s, "big") {
            PredictedFrom::FailedAttemptFloor(secs) => {
                assert!(
                    (secs - 3600.0).abs() < 1.0,
                    "the LONGEST attempt's hour must survive as the floor (not the \
                     shortest, first or most recent attempt), got {secs}"
                );
            }
            _ => panic!("failed-only history must be a FailedAttemptFloor, not a placeholder"),
        }
    }

    #[test]
    fn measured_success_beats_failed_attempts_and_no_history_is_a_placeholder() {
        let mut rows: Vec<(&str, String, i64, &str)> = vec![("t", "r0".into(), 120_000, "success")];
        for i in 1..=30 {
            rows.push(("t", format!("r{i}"), 3_600_000, "failed"));
        }
        let owned: Vec<(&str, &str, i64, &str)> = rows
            .iter()
            .map(|(e, r, d, s)| (*e, r.as_str(), *d, *s))
            .collect();
        let s = store_with(&owned);
        match predict_secs(&s, "t") {
            PredictedFrom::Measured(secs) => assert!((secs - 120.0).abs() < 1.0, "got {secs}"),
            _ => panic!("a successful run must classify as Measured even past 25 failures"),
        }
        let empty = store_with(&[]);
        match predict_secs(&empty, "unknown") {
            PredictedFrom::Placeholder(secs) => assert_eq!(secs, POOL_PLACEHOLDER_SECS),
            _ => panic!("no history must classify as Placeholder"),
        }
        let quick = store_with(&[("q", "r1", 1_000, "failed")]);
        assert!(predict_secs(&quick, "q").secs() >= POOL_PLACEHOLDER_SECS);
    }

    /// The drift this module closed (walk find, 2026-08-13).
    #[test]
    fn predict_items_seeds_split_units_and_includes_history_less_exports() {
        let s = store_with(&[("giant", "r1", 900_000, "success")]);
        let seeds = std::collections::HashMap::from([(
            "giant#0".to_string(),
            PredictedFrom::SeededSplit(450.0),
        )]);
        let out = predict_items(&s, [("giant#0", false), ("fresh", true)], &seeds);
        assert_eq!(
            out.len(),
            2,
            "history-less exports are included, not dropped"
        );
        let (unit, from) = &out[0];
        assert!(matches!(from, PredictedFrom::SeededSplit(_)));
        assert_eq!(
            unit.predicted_secs, 450.0,
            "the seed bootstraps a unit with no rows of its own \
             (once it has some, see reconcile_split_seed)"
        );
        assert!(!unit.parallel_safe, "parallel_safe passes through verbatim");
        let (fresh, from) = &out[1];
        assert!(matches!(from, PredictedFrom::Placeholder(_)));
        assert_eq!(fresh.predicted_secs, POOL_PLACEHOLDER_SECS);
        assert!(fresh.parallel_safe);
    }

    /// Splitting an UNMEASURED giant must not manufacture measurement.
    #[test]
    fn split_units_of_an_unmeasured_giant_keep_the_lower_bound_hedge() {
        let s = store_with(&[
            ("giant", "r1", 3_600_000, "failed"),
            ("giant", "r2", 3_000_000, "interrupted"),
            ("small", "r3", 60_000, "success"),
        ]);
        let giant_from = predict_secs(&s, "giant");
        assert!(
            matches!(giant_from, PredictedFrom::FailedAttemptFloor(_)),
            "fixture: the giant must be unmeasured, got {giant_from:?}"
        );
        let share = giant_from.secs() / 4.0;
        let seeds: std::collections::HashMap<String, PredictedFrom> = (0..4)
            .map(|i| (format!("giant#{i}"), split_unit_from(&giant_from, share)))
            .collect();
        let names: Vec<String> = (0..4).map(|i| format!("giant#{i}")).collect();
        let exports: Vec<(&str, bool)> = names
            .iter()
            .map(|n| (n.as_str(), true))
            .chain(std::iter::once(("small", true)))
            .collect();
        let predicted = predict_items(&s, exports, &seeds);
        let classified: Vec<PredictedFrom> = predicted.iter().map(|(_, f)| f.clone()).collect();
        for (item, from) in &predicted {
            if item.name.starts_with("giant#") {
                assert!(
                    matches!(from, PredictedFrom::FailedAttemptFloor(_)),
                    "a unit of a never-succeeded giant is not measured: {from:?}"
                );
                assert_eq!(item.predicted_secs, share, "the unit still gets its share");
            }
        }
        let (measured, attempt, placeholder) = classification_counts(&classified);
        assert_eq!(
            (measured, attempt, placeholder),
            (1, 4, 0),
            "only the small export is measured; the four units are attempt-floored"
        );
        assert!(
            attempt + placeholder > 0,
            "this sum is the LOWER BOUND hedge's condition in run_pool — \
             a split must never zero it"
        );
        let measured_giant = PredictedFrom::Measured(1200.0);
        assert!(matches!(
            split_unit_from(&measured_giant, 300.0),
            PredictedFrom::SeededSplit(s) if s == 300.0
        ));
        assert!(matches!(
            split_unit_from(&PredictedFrom::Placeholder(POOL_PLACEHOLDER_SECS), 1.25),
            PredictedFrom::Placeholder(_)
        ));
    }

    /// Run 2+ of `apply --pool --split`: the units' OWN measurements must supersede the inherited seed.
    #[test]
    fn a_split_units_own_history_supersedes_the_inherited_seed() {
        let s = store_with(&[
            ("giant", "r1", 3_600_000, "failed"),
            ("giant", "r2", 3_000_000, "interrupted"),
            ("giant#0", "r3", 700_000, "success"),
            ("giant#1", "r4", 1_100_000, "success"),
        ]);
        let giant_from = predict_secs(&s, "giant");
        let share = giant_from.secs() / 3.0;
        let seeds: std::collections::HashMap<String, PredictedFrom> = (0..3)
            .map(|i| (format!("giant#{i}"), split_unit_from(&giant_from, share)))
            .collect();
        let names: Vec<String> = (0..3).map(|i| format!("giant#{i}")).collect();
        let exports: Vec<(&str, bool)> = names.iter().map(|n| (n.as_str(), true)).collect();
        let predicted = predict_items(&s, exports, &seeds);
        let secs_of = |n: &str| {
            predicted
                .iter()
                .find(|(i, _)| i.name == n)
                .map(|(i, _)| i.predicted_secs)
                .expect("unit present")
        };
        assert_eq!(secs_of("giant#0"), 700.0, "the unit's own success wins");
        assert_eq!(secs_of("giant#1"), 1100.0, "…per unit, not one shared seed");
        assert_eq!(
            secs_of("giant#2"),
            share,
            "a unit with no rows of its own still takes the seed"
        );
        let items: Vec<PoolItem> = predicted.iter().map(|(i, _)| i.clone()).collect();
        assert_eq!(
            pool_order(&items),
            vec!["giant#2", "giant#1", "giant#0"],
            "longest-first among the slices"
        );
        let classified: Vec<PredictedFrom> = predicted.iter().map(|(_, f)| f.clone()).collect();
        assert_eq!(
            classification_counts(&classified),
            (2, 1, 0),
            "the LOWER BOUND hedge shrinks to the units that really lack a success"
        );
    }

    /// Run 2 of `apply --pool --split` with a DIFFERENT unit count.
    #[test]
    fn a_shrunk_split_partition_re_cuts_the_cohort_instead_of_reusing_its_units_numbers() {
        let mut rows: Vec<(String, String, i64, String)> =
            vec![("giant".into(), "r0".into(), 1_800_000, "failed".into())];
        for (i, ms) in [300, 400, 500, 700, 300, 400, 500, 500].iter().enumerate() {
            rows.push((
                format!("giant#{i}"),
                format!("r1_{i}"),
                ms * 1000,
                "success".into(),
            ));
        }
        let owned: Vec<(&str, &str, i64, &str)> = rows
            .iter()
            .map(|(e, r, d, s)| (e.as_str(), r.as_str(), *d, s.as_str()))
            .collect();
        let s = store_with(&owned);

        let giant_from = predict_secs(&s, "giant");
        let seed_share = giant_from.secs() / 3.0;
        assert_eq!(
            seed_share, 600.0,
            "fixture: the stale seed is a third value"
        );
        fn exports_of(names: &[String]) -> Vec<(&str, bool)> {
            names.iter().map(|n| (n.as_str(), true)).collect()
        }
        let seeds: std::collections::HashMap<String, PredictedFrom> = (0..3)
            .map(|i| {
                (
                    format!("giant#{i}"),
                    split_unit_from(&giant_from, seed_share),
                )
            })
            .collect();
        let names: Vec<String> = (0..3).map(|i| format!("giant#{i}")).collect();
        let predicted = predict_items(&s, exports_of(&names), &seeds);
        for (item, _) in &predicted {
            assert_eq!(
                item.predicted_secs, 1200.0,
                "{} must be re-cut from the cohort's measured 3600 s over the THREE \
                 units running now — not its own 8-way number, and not the giant's \
                 frozen 600 s seed",
                item.name
            );
        }
        let classified: Vec<PredictedFrom> = predicted.iter().map(|(_, f)| f.clone()).collect();
        assert!(
            classified
                .iter()
                .all(|f| matches!(f, PredictedFrom::SeededSplit(_))),
            "{classified:?}"
        );

        for i in 0..3 {
            s.record_metric_full(&MetricRow {
                export_name: format!("giant#{i}"),
                run_id: format!("r2_{i}"),
                duration_ms: 1_200_000,
                status: "success".into(),
                ..Default::default()
            })
            .expect("record run-2 metric");
        }
        let run3 = predict_items(&s, exports_of(&names), &seeds);
        for (item, _) in &run3 {
            assert_eq!(
                item.predicted_secs, 1200.0,
                "{} ran at THIS partition last time — the re-cut must read the \
                 newest generation only (or stand down and let the unit's own \
                 measurement through), never sum run 2's 3-way units together \
                 with run 1's retired 8-way ordinals",
                item.name
            );
        }

        let two: Vec<String> = (0..2).map(|i| format!("giant#{i}")).collect();
        let seeds2: std::collections::HashMap<String, PredictedFrom> = two
            .iter()
            .map(|n| (n.clone(), split_unit_from(&giant_from, 900.0)))
            .collect();
        let run4 = predict_items(&s, exports_of(&two), &seeds2);
        for (item, _) in &run4 {
            assert_eq!(
                item.predicted_secs, 1800.0,
                "{} must be re-cut from the NEWEST generation's measured 3600 s \
                 over the two units running now",
                item.name
            );
        }

        let mixed = generation(
            1,
            &[
                (0usize, PredictedFrom::Measured(300.0)),
                (1, PredictedFrom::FailedAttemptFloor(900.0)),
                (2, PredictedFrom::Measured(300.0)),
                (3, PredictedFrom::Measured(300.0)),
            ],
        );
        match repartitioned_unit_prediction(2, &mixed) {
            Some(PredictedFrom::FailedAttemptFloor(secs)) => assert_eq!(secs, 900.0),
            other => panic!("a cohort holding an unmeasured unit is a floor, got {other:?}"),
        }
        let grown: Vec<UnitHistory> = generation(
            1,
            &(0..3)
                .map(|i| (i, PredictedFrom::Measured(1200.0)))
                .collect::<Vec<_>>(),
        );
        assert!(repartitioned_unit_prediction(8, &grown).is_none());
        assert!(repartitioned_unit_prediction(3, &grown).is_none());
        assert!(repartitioned_unit_prediction(3, &[]).is_none());
    }

    /// The generation boundary itself, pure: which censused ordinals the re-cut is allowed to SUM.
    #[test]
    fn newest_generation_closes_at_the_partition_boundary_not_the_whole_census() {
        let mut census = generation(
            2,
            &[
                (0usize, PredictedFrom::Measured(1200.0)),
                (1, PredictedFrom::Measured(1200.0)),
                (2, PredictedFrom::Measured(1200.0)),
            ],
        );
        census.extend(generation(
            1,
            &[
                (3usize, PredictedFrom::Measured(700.0)),
                (4, PredictedFrom::Measured(300.0)),
                (5, PredictedFrom::Measured(400.0)),
                (6, PredictedFrom::Measured(500.0)),
                (7, PredictedFrom::Measured(500.0)),
            ],
        ));
        let newest: Vec<usize> = newest_generation(&census)
            .iter()
            .map(|u| u.ordinal)
            .collect();
        assert_eq!(
            newest.len(),
            3,
            "only run 2's three units sum to the table; got ordinals {newest:?}"
        );
        assert_eq!(newest.iter().copied().max(), Some(2));
        match repartitioned_unit_prediction(2, &census) {
            Some(f) => assert_eq!(f.secs(), 1800.0, "{f:?}"),
            None => panic!("8→3→2 is still a shrink; the re-cut must fire"),
        }

        let one_gen = generation(
            1,
            &(0..8)
                .map(|i| (i, PredictedFrom::Measured(450.0)))
                .collect::<Vec<_>>(),
        );
        assert_eq!(newest_generation(&one_gen).len(), 8);

        let untimed: Vec<UnitHistory> = (0..8)
            .map(|ordinal| UnitHistory {
                ordinal,
                from: PredictedFrom::Measured(450.0),
                recorded_at: None,
            })
            .collect();
        assert_eq!(newest_generation(&untimed).len(), 8);

        let holed: Vec<UnitHistory> = census.iter().filter(|u| u.ordinal != 0).cloned().collect();
        assert_eq!(newest_generation(&holed).len(), holed.len());
    }

    /// The reconcile rule itself, in isolation: seed only while the unit is genuinely history-less, own row otherwise (success OR attempt floor).
    #[test]
    fn reconcile_split_seed_yields_to_any_history_of_the_units_own() {
        let seed = PredictedFrom::SeededSplit(1200.0);
        assert!(matches!(
            reconcile_split_seed(&seed, &PredictedFrom::Placeholder(POOL_PLACEHOLDER_SECS)),
            PredictedFrom::SeededSplit(s) if s == 1200.0
        ));
        assert!(matches!(
            reconcile_split_seed(&seed, &PredictedFrom::Measured(700.0)),
            PredictedFrom::Measured(s) if s == 700.0
        ));
        assert!(matches!(
            reconcile_split_seed(&seed, &PredictedFrom::FailedAttemptFloor(1800.0)),
            PredictedFrom::FailedAttemptFloor(s) if s == 1800.0
        ));
    }

    /// A partition of ZERO units is not a partition to re-cut into.
    #[test]
    fn a_zero_unit_partition_is_not_re_cut_and_never_divides_by_zero() {
        let cohort = generation(
            1,
            &(0..3)
                .map(|i| (i, PredictedFrom::Measured(1200.0)))
                .collect::<Vec<_>>(),
        );
        let got = repartitioned_unit_prediction(0, &cohort);
        assert!(
            got.is_none(),
            "a zero-unit partition has nothing to re-cut into; got {got:?}"
        );
        assert!(
            got.map(|f| f.secs().is_finite()).unwrap_or(true),
            "the re-cut must never emit a non-finite prediction"
        );
    }

    /// The census's TERMINATION rule, against a cohort with a hole in it.
    #[test]
    fn the_census_survives_a_hole_and_stops_before_an_ancient_ordinal() {
        assert_eq!(
            SPLIT_CENSUS_LOOKAHEAD, 3,
            "fixture: the gap at #8..#14 must be longer than the lookahead"
        );
        let mut rows: Vec<(String, String, i64, &str)> = Vec::new();
        for (ordinal, secs) in [
            (0, 300),
            (1, 400),
            (3, 500),
            (4, 700),
            (5, 600),
            (6, 500),
            (7, 600),
        ] {
            rows.push((
                format!("giant#{ordinal}"),
                format!("r1_{ordinal}"),
                secs * 1000,
                "success",
            ));
        }
        rows.push(("giant#15".into(), "r0_15".into(), 800_000, "success"));
        let owned: Vec<(&str, &str, i64, &str)> = rows
            .iter()
            .map(|(e, r, d, s)| (e.as_str(), r.as_str(), *d, *s))
            .collect();
        let s = store_with(&owned);

        let census = split_history_census(&s, "giant", 2);
        let ordinals: Vec<usize> = census.iter().map(|u| u.ordinal).collect();
        assert_eq!(
            ordinals,
            vec![0, 1, 3, 4, 5, 6, 7],
            "the census must step over the hole at #2 (a unit that recorded \
             nothing is an absence, not the end of the cohort) and stop three \
             absences later — never reaching the retired #15"
        );

        match repartitioned_unit_prediction(2, &census) {
            Some(f) => assert_eq!(
                f.secs(),
                1800.0,
                "the newest generation's measured 3600 s, cut two ways: {f:?}"
            ),
            None => {
                panic!("8 recorded ordinals against 2 units now is a shrink — the re-cut must fire")
            }
        }
    }
}
