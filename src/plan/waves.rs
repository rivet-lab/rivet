//! Wave packing from MEASURED run history (#150, the feature half).

use crate::plan::CostClass;

/// One export, as the packer sees it.
#[derive(Debug, Clone)]
pub(crate) struct PackItem {
    pub name: String,
    /// The operator's tier (the export's existing `wave:`), or the cost model's recommendation when the operator set none.
    pub tier: u32,
    pub cost_class: CostClass,
    /// Predicted duration, seconds.
    pub predicted_secs: f64,
    /// Last observed peak RSS, MB (a conservative default when unknown).
    pub peak_rss_mb: i64,
}

/// A packed wave: sequential number and its members (names).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PackedWave {
    pub number: u32,
    pub members: Vec<String>,
}

/// Concurrent-export cap per wave, by the heaviest member's cost class.
fn k_for(class: CostClass) -> usize {
    match class {
        CostClass::Low => 10,
        CostClass::Medium => 3,
        CostClass::High | CostClass::VeryHigh => 2,
    }
}

/// Pack items into waves: within each tier, duration-DESC, consecutive groups of K (K from the group's FIRST.
pub(crate) fn pack(items: &[PackItem], rss_budget_mb: i64) -> Vec<PackedWave> {
    let mut tiers: std::collections::BTreeMap<u32, Vec<&PackItem>> =
        std::collections::BTreeMap::new();
    for it in items {
        tiers.entry(it.tier).or_default().push(it);
    }

    let mut waves = Vec::new();
    let mut number = 1u32;
    for (_tier, mut members) in tiers {
        members.sort_by(|a, b| {
            b.predicted_secs
                .total_cmp(&a.predicted_secs)
                .then_with(|| a.name.cmp(&b.name))
        });
        let mut i = 0;
        while i < members.len() {
            let mut group: Vec<&PackItem> = Vec::new();
            let mut rss = 0i64;
            while i < members.len() {
                let m = members[i];
                let k = group
                    .iter()
                    .map(|g| k_for(g.cost_class))
                    .chain(std::iter::once(k_for(m.cost_class)))
                    .min()
                    .unwrap_or(1);
                if !group.is_empty() && (group.len() >= k || rss + m.peak_rss_mb > rss_budget_mb) {
                    break;
                }
                rss += m.peak_rss_mb;
                group.push(m);
                i += 1;
            }
            waves.push(PackedWave {
                number,
                members: group.iter().map(|m| m.name.clone()).collect(),
            });
            number += 1;
        }
    }
    waves
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(name: &str, tier: u32, class: CostClass, secs: f64) -> PackItem {
        PackItem {
            name: name.into(),
            tier,
            cost_class: class,
            predicted_secs: secs,
            peak_rss_mb: 100,
        }
    }

    /// Wall of a schedule = sum over waves of the max duration inside each — the barrier model, verbatim.
    fn wall(waves: &[PackedWave], secs: &std::collections::HashMap<String, f64>) -> f64 {
        waves
            .iter()
            .map(|w| w.members.iter().map(|m| secs[m]).fold(0.0f64, f64::max))
            .sum()
    }

    #[test]
    fn equals_pack_with_equals_and_heavy_goes_first() {
        let items = vec![
            item("g1", 4, CostClass::High, 900.0),
            item("q1", 4, CostClass::High, 10.0),
            item("g2", 4, CostClass::High, 880.0),
            item("q2", 4, CostClass::High, 12.0),
        ];
        let waves = pack(&items, 4096);
        assert_eq!(waves.len(), 2);
        assert_eq!(waves[0].members, vec!["g1", "g2"]);
        assert_eq!(waves[1].members, vec!["q2", "q1"]);
        let secs = items
            .iter()
            .map(|i| (i.name.clone(), i.predicted_secs))
            .collect();
        assert_eq!(wall(&waves, &secs), 900.0 + 12.0);
    }

    /// The RSS split is STRICT: `rss + next.peak_rss_mb > budget`.
    #[test]
    fn rss_split_is_strict_an_exact_fit_still_joins_the_wave() {
        let items = vec![
            item("a", 1, CostClass::Low, 10.0),
            item("b", 1, CostClass::Low, 9.0),
        ];
        let waves = pack(&items, 200);
        assert_eq!(
            waves.len(),
            1,
            "exact fit must not split — `>=` would wrongly make two waves"
        );
        assert_eq!(waves[0].members, vec!["a", "b"]);
        assert_eq!(
            pack(&items, 199).len(),
            2,
            "199 < 200 must split into two waves"
        );
    }

    #[test]
    fn tiers_are_never_crossed_and_numbering_is_sequential() {
        let items = vec![
            item("t2_a", 2, CostClass::Low, 1.0),
            item("t4_a", 4, CostClass::High, 100.0),
            item("t2_b", 2, CostClass::Low, 2.0),
        ];
        let waves = pack(&items, 4096);
        assert_eq!(waves[0].members, vec!["t2_b", "t2_a"]);
        assert_eq!(waves[1].members, vec!["t4_a"]);
        assert_eq!(
            waves.iter().map(|w| w.number).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn rss_guard_splits_a_group_and_loses_nobody() {
        let mut items = vec![
            item("a", 3, CostClass::Medium, 30.0),
            item("b", 3, CostClass::Medium, 29.0),
            item("c", 3, CostClass::Medium, 28.0),
        ];
        for it in &mut items {
            it.peak_rss_mb = 300;
        }
        let waves = pack(&items, 500);
        assert!(waves.len() >= 2, "over-budget group must split: {waves:?}");
        let mut all: Vec<_> = waves.iter().flat_map(|w| w.members.clone()).collect();
        all.sort();
        assert_eq!(all, vec!["a", "b", "c"], "the guard splits, never drops");
        assert!(
            waves.iter().all(|w| !w.members.is_empty()),
            "no empty waves"
        );
    }

    /// K is the heaviest member's, not the duration-front-runner's.
    #[test]
    fn k_is_the_heaviest_class_in_the_group_not_the_front_runner() {
        let mut items = vec![item("low_but_long", 4, CostClass::Low, 500.0)];
        for n in 0..5 {
            items.push(item(
                &format!("giant{n}"),
                4,
                CostClass::High,
                400.0 - n as f64,
            ));
        }
        let waves = pack(&items, i64::MAX);
        for w in &waves {
            let has_giant = w.members.iter().any(|m| m.starts_with("giant"));
            if has_giant {
                assert!(
                    w.members.len() <= 2,
                    "a wave with a High giant must cap at K=2, got {:?}",
                    w.members
                );
            }
        }
    }

    #[test]
    fn determinism_ties_break_by_name() {
        let items = vec![
            item("b", 2, CostClass::Low, 5.0),
            item("a", 2, CostClass::Low, 5.0),
        ];
        let w1 = pack(&items, 4096);
        let rev: Vec<PackItem> = items.iter().rev().cloned().collect();
        let w2 = pack(&rev, 4096);
        assert_eq!(w1, w2, "same schedule whatever the input order");
        assert_eq!(w1[0].members, vec!["a", "b"]);
    }

    /// The optimality claim as a PROPERTY, not an example.
    #[test]
    fn consecutive_k_never_loses_to_a_shuffle() {
        use proptest::prelude::*;
        proptest!(|(durs in proptest::collection::vec(1.0f64..1000.0, 4..24), seed in 0u64..1000)| {
            let items: Vec<PackItem> = durs
                .iter()
                .enumerate()
                .map(|(i, &d)| item(&format!("e{i:02}"), 1, CostClass::Medium, d))
                .collect();
            let secs: std::collections::HashMap<String, f64> =
                items.iter().map(|i| (i.name.clone(), i.predicted_secs)).collect();
            let ours = wall(&pack(&items, i64::MAX), &secs);

            let mut shuffled = items.clone();
            let mut s = seed;
            for i in (1..shuffled.len()).rev() {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                shuffled.swap(i, (s as usize) % (i + 1));
            }
            let theirs: f64 = shuffled
                .chunks(3)
                .map(|g| g.iter().map(|m| m.predicted_secs).fold(0.0f64, f64::max))
                .sum();
            prop_assert!(ours <= theirs + 1e-9, "ours={ours} shuffle={theirs}");
        });
    }

    /// Independent oracle from the field (2026-08-07).
    #[test]
    fn reproduces_the_field_prototypes_packing() {
        let mins = [
            ("boss", 51.1),
            ("admitads", 15.9),
            ("taobao", 12.4),
            ("cb_usd", 9.9),
            ("cb_eur", 9.5),
            ("allegro", 7.9),
            ("pay_usd", 7.0),
            ("pay_eur", 6.7),
            ("lety", 5.9),
            ("misb", 5.1),
            ("backup", 3.2),
            ("products", 2.5),
        ];
        let items: Vec<PackItem> = mins
            .iter()
            .map(|(n, m)| item(n, 4, CostClass::VeryHigh, m * 60.0))
            .collect();
        let waves = pack(&items, 4096);
        let got: Vec<Vec<String>> = waves.iter().map(|w| w.members.clone()).collect();
        let want: Vec<Vec<String>> = [
            vec!["boss", "admitads"],
            vec!["taobao", "cb_usd"],
            vec!["cb_eur", "allegro"],
            vec!["pay_usd", "pay_eur"],
            vec!["lety", "misb"],
            vec!["backup", "products"],
        ]
        .iter()
        .map(|v| v.iter().map(|s| s.to_string()).collect())
        .collect();
        assert_eq!(got, want);
        let secs: std::collections::HashMap<String, f64> = items
            .iter()
            .map(|i| (i.name.clone(), i.predicted_secs))
            .collect();
        let wall_min = wall(&waves, &secs) / 60.0;
        assert!(
            (wall_min - 89.1).abs() < 0.05,
            "hand-summed 89.1 min, got {wall_min:.2}"
        );
    }
}
