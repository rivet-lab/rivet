//! Pure chunk math — no I/O, no database access.

pub(crate) use crate::sql::{strip_select_star_from, strip_simple_projection_from};

/// Hard ceiling on range-window count.
pub const MAX_CHUNK_WINDOWS: u128 = 50_000_000;

/// The number of inclusive windows of `window` keys that cover `min..=max`, with no allocation.
pub fn expected_chunk_count(min: i64, max: i64, window: u128) -> u128 {
    if max < min || window == 0 {
        return 0;
    }
    let span = (i128::from(max) - i128::from(min)) as u128;
    span / window + 1
}

/// `generate_windows`, but refuses a plan past [`MAX_CHUNK_WINDOWS`] before the `Vec` allocates.
pub fn generate_chunks_checked(
    min: i64,
    max: i64,
    window: u128,
) -> anyhow::Result<Vec<(i64, i64)>> {
    let expected = expected_chunk_count(min, max, window);
    if expected > MAX_CHUNK_WINDOWS {
        crate::rivet_bail!(
            crate::error::codes::PLAN_CHUNK_KEY_SPARSE,
            "chunk plan would generate {expected} windows (key span {min}..={max} ÷ chunk_size \
             {window}), past the {MAX_CHUNK_WINDOWS}-window ceiling — the key is extremely \
             sparse. Raise chunk_size:, set an explicit chunk_count:, or use mode: full."
        );
    }
    Ok(generate_windows(min, max, window))
}

/// The inclusive windows of `window` keys that cover `min..=max`, in a width a full `i64` range cannot overflow.
pub fn generate_windows(min: i64, max: i64, window: u128) -> Vec<(i64, i64)> {
    if max < min || window == 0 {
        return vec![];
    }
    let reach = (window - 1).min(u128::from(u64::MAX)) as i128;
    let mut chunks = Vec::new();
    let mut start = min;
    loop {
        let end = (i128::from(start) + reach).min(i128::from(max)) as i64;
        chunks.push((start, end));
        if end == max {
            break;
        }
        start = end + 1;
    }
    chunks
}

/// The inclusive windows of `chunk_size` keys over `min..=max`; empty for a non-positive size.
pub fn generate_chunks(min: i64, max: i64, chunk_size: i64) -> Vec<(i64, i64)> {
    if chunk_size <= 0 {
        return vec![];
    }
    generate_windows(min, max, chunk_size as u128)
}

/// The pieces of the inclusive `ranges` that no window of `windows` covers, in order.
pub(crate) fn ranges_uncovered(ranges: &[(i64, i64)], windows: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for &(start, end) in ranges {
        let mut from = Some(start);
        for &(lo, hi) in windows.iter().filter(|w| w.1 >= start && w.0 <= end) {
            let Some(at) = from else { break };
            if lo > at {
                out.push((at, lo - 1));
            }
            from = hi
                .checked_add(1)
                .filter(|next| *next <= end)
                .map(|n| n.max(at));
        }
        if let Some(at) = from {
            out.push((at, end));
        }
    }
    out
}

pub(crate) fn build_chunk_query_sql(
    base_query: &str,
    order_column: &str,
    start: i64,
    end: i64,
    chunk_by_days: bool,
    source_type: crate::config::SourceType,
) -> String {
    let quoted_col = crate::sql::quote_ident(source_type, order_column);
    let date_fmt = crate::plan::partition::date_literal_format(source_type);

    if let Some(table_ident) = strip_select_star_from(base_query) {
        if chunk_by_days {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
            let start_date = epoch + chrono::Duration::days(start);
            let end_date = epoch + chrono::Duration::days(end + 1);
            return format!(
                "SELECT * FROM {table} WHERE {col} >= '{s}' AND {col} < '{e}'",
                table = table_ident,
                col = quoted_col,
                s = start_date.format(date_fmt),
                e = end_date.format(date_fmt),
            );
        }
        return format!(
            "SELECT * FROM {table} WHERE {col} BETWEEN {s} AND {e}",
            table = table_ident,
            col = quoted_col,
            s = start,
            e = end,
        );
    }

    if chunk_by_days {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
        let start_date = epoch + chrono::Duration::days(start);
        let end_date = epoch + chrono::Duration::days(end + 1);
        return format!(
            "SELECT * FROM ({base}) {d} WHERE {col} >= '{start}' AND {col} < '{end}'",
            base = base_query,
            d = crate::sql::derived(source_type, "_rivet"),
            col = quoted_col,
            start = start_date.format(date_fmt),
            end = end_date.format(date_fmt),
        );
    }

    format!(
        "SELECT * FROM ({base}) {d} WHERE {col} BETWEEN {start} AND {end}",
        base = base_query,
        d = crate::sql::derived(source_type, "_rivet"),
        col = quoted_col,
        start = start,
        end = end,
    )
}

pub(crate) fn chunk_plan_fingerprint(
    base_query: &str,
    chunk_column: &str,
    chunk_size: usize,
    chunk_count: Option<usize>,
    chunk_by_days: Option<u32>,
) -> String {
    use xxhash_rust::xxh3::xxh3_64;
    let kind = match chunk_by_days {
        Some(d) => format!("date_{d}d"),
        None if chunk_count.is_some() => format!("count_{}", chunk_count.unwrap()),
        None => format!("range_{chunk_size}"),
    };
    let mut buf: Vec<u8> =
        Vec::with_capacity(base_query.len() + chunk_column.len() + kind.len() + 24);
    for part in [base_query, chunk_column, kind.as_str()] {
        buf.extend_from_slice(&(part.len() as u64).to_le_bytes());
        buf.extend_from_slice(part.as_bytes());
    }
    format!("{:016x}", xxh3_64(&buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_chunk_count_matches_generated_len() {
        for (min, max, cs) in [(0, 100, 25), (1, 100, 25), (42, 42, 100), (10, 20, 10_000)] {
            assert_eq!(
                expected_chunk_count(min, max, cs as u128),
                generate_chunks(min, max, cs).len() as u128,
                "span {min}..={max} / {cs}"
            );
        }
        assert_eq!(expected_chunk_count(10, 5, 10), 0);
        assert_eq!(expected_chunk_count(1, 100, 0), 0);
    }

    #[test]
    fn generate_chunks_checked_bails_before_allocating_on_extreme_span() {
        let err = generate_chunks_checked(0, i64::MAX, 100_000)
            .expect_err("an extreme-sparse span must bail, not allocate ~9e13 tuples");
        let msg = err.to_string();
        assert!(msg.contains("window"), "actionable message: {msg}");
        assert!(
            msg.contains("chunk_count") || msg.contains("mode: full"),
            "names an escape hatch: {msg}"
        );
        crate::error::assert_coded(&err, "RIVET_PLAN_CHUNK_KEY_SPARSE", 1);
        let just_over = (MAX_CHUNK_WINDOWS as i64).saturating_mul(2);
        assert!(generate_chunks_checked(0, just_over, 1).is_err());
    }

    #[test]
    fn a_full_i64_range_is_planned_in_windows_without_overflow() {
        let full = generate_windows(i64::MIN, i64::MAX, 1u128 << 64);
        assert_eq!(full, vec![(i64::MIN, i64::MAX)]);
        let halves = generate_windows(i64::MIN, i64::MAX, 1u128 << 63);
        assert_eq!(halves, vec![(i64::MIN, -1), (0, i64::MAX)]);
        assert_eq!(expected_chunk_count(i64::MIN, i64::MAX, 1u128 << 63), 2);
        assert_eq!(
            generate_windows(i64::MAX - 1, i64::MAX, u128::MAX),
            vec![(i64::MAX - 1, i64::MAX)]
        );
        assert_eq!(generate_windows(5, 4, 3), vec![]);
        assert_eq!(generate_windows(5, 5, 0), vec![]);
    }

    #[test]
    fn generate_chunks_checked_allows_normal_plans() {
        let ok = generate_chunks_checked(1, 10_000_000, 100_000).expect("normal plan");
        assert_eq!(ok, generate_chunks(1, 10_000_000, 100_000));
        assert_eq!(ok.len(), 100);
    }

    #[test]
    fn chunk_by_days_windows_use_exclusive_upper_bound() {
        use crate::config::SourceType;
        let q = build_chunk_query_sql("SELECT * FROM t", "d", 0, 0, true, SourceType::Postgres);
        assert!(
            q.contains(">= '1970-01-01'") && q.contains("< '1970-01-02'"),
            "single-day window must be [day, day+1): {q}"
        );
        let q = build_chunk_query_sql(
            "SELECT a, d FROM t WHERE a > 0",
            "d",
            19000,
            19001,
            true,
            SourceType::Postgres,
        );
        assert!(
            q.contains(">= '2022-01-08'") && q.contains("< '2022-01-10'"),
            "window [19000, 19001] must render as [2022-01-08, 2022-01-10): {q}"
        );
    }

    #[test]
    fn test_generate_chunks() {
        let chunks = generate_chunks(1, 100, 30);
        assert_eq!(chunks, vec![(1, 30), (31, 60), (61, 90), (91, 100)]);
    }

    #[test]
    fn test_generate_chunks_exact() {
        let chunks = generate_chunks(0, 99, 50);
        assert_eq!(chunks, vec![(0, 49), (50, 99)]);
    }

    #[test]
    fn test_generate_chunks_single() {
        let chunks = generate_chunks(1, 10, 100);
        assert_eq!(chunks, vec![(1, 10)]);
    }

    #[test]
    fn test_generate_chunks_empty() {
        assert!(generate_chunks(10, 5, 100).is_empty());
    }

    #[test]
    fn test_build_chunk_query_range_mode() {
        let q = build_chunk_query_sql(
            "SELECT id FROM t",
            "id",
            1,
            100,
            false,
            crate::config::SourceType::Postgres,
        );
        assert!(q.contains("WHERE \"id\" BETWEEN 1 AND 100"), "got: {}", q);
    }

    #[test]
    fn chunk_query_fast_path_select_star_from_simple_table() {
        let q = build_chunk_query_sql(
            "SELECT * FROM public.users",
            "id",
            1,
            100,
            false,
            crate::config::SourceType::Postgres,
        );
        assert_eq!(
            q,
            "SELECT * FROM public.users WHERE \"id\" BETWEEN 1 AND 100"
        );
    }

    #[test]
    fn chunk_query_fast_path_unqualified_table() {
        let q = build_chunk_query_sql(
            "SELECT * FROM orders",
            "id",
            1,
            10,
            false,
            crate::config::SourceType::Postgres,
        );
        assert_eq!(q, "SELECT * FROM orders WHERE \"id\" BETWEEN 1 AND 10");
    }

    #[test]
    fn chunk_query_wraps_when_query_is_not_select_star() {
        for not_fast in [
            "SELECT id, name FROM users",
            "SELECT * FROM users u",
            "SELECT * FROM users WHERE deleted_at IS NULL",
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
            "SELECT * FROM (SELECT * FROM users) sub",
        ] {
            let q = build_chunk_query_sql(
                not_fast,
                "id",
                1,
                10,
                false,
                crate::config::SourceType::Postgres,
            );
            assert!(
                q.starts_with("SELECT * FROM ("),
                "fallback wrap expected for `{not_fast}`; got: {q}"
            );
        }
    }

    #[test]
    fn chunk_query_fast_path_by_days() {
        let q = build_chunk_query_sql(
            "SELECT * FROM public.events",
            "created_at",
            18000,
            18001,
            true,
            crate::config::SourceType::Postgres,
        );
        assert!(
            q.starts_with("SELECT * FROM public.events WHERE \"created_at\" >= '"),
            "expected fast-path date range; got: {q}"
        );
        assert!(
            !q.contains("AS _rivet"),
            "fast path should not wrap in subquery; got: {q}"
        );
    }

    #[test]
    fn test_build_chunk_query_range_mode_mysql() {
        let q = build_chunk_query_sql(
            "SELECT id FROM t",
            "id",
            1,
            100,
            false,
            crate::config::SourceType::Mysql,
        );
        assert!(q.contains("WHERE `id` BETWEEN 1 AND 100"), "got: {}", q);
    }

    #[test]
    fn test_build_chunk_query_date_mode() {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let start = (chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap() - epoch).num_days();
        let end = (chrono::NaiveDate::from_ymd_opt(2023, 1, 7).unwrap() - epoch).num_days();
        let q = build_chunk_query_sql(
            "SELECT * FROM orders",
            "created_at",
            start,
            end,
            true,
            crate::config::SourceType::Postgres,
        );
        assert!(q.contains(">= '2023-01-01'"), "got: {q}");
        assert!(q.contains("< '2023-01-08'"), "got: {q}");
        assert!(!q.contains("BETWEEN"), "should use >= AND <, got: {q}");
    }

    #[test]
    fn test_build_chunk_query_date_mode_single_day() {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let day = (chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap() - epoch).num_days();
        let q = build_chunk_query_sql(
            "SELECT * FROM t",
            "ts",
            day,
            day,
            true,
            crate::config::SourceType::Postgres,
        );
        assert!(q.contains(">= '2024-03-15'"), "got: {q}");
        assert!(q.contains("< '2024-03-16'"), "got: {q}");
    }

    fn assert_chunk_invariants(min: i64, max: i64, size: i64, chunks: &[(i64, i64)]) {
        if max < min || size <= 0 {
            assert!(chunks.is_empty(), "invalid input must yield []: {chunks:?}");
            return;
        }

        assert!(
            !chunks.is_empty(),
            "valid range must yield at least one chunk"
        );
        assert_eq!(chunks.first().unwrap().0, min, "first chunk starts at min");
        assert_eq!(chunks.last().unwrap().1, max, "last chunk ends at max");

        for (i, (s, e)) in chunks.iter().enumerate() {
            assert!(s <= e, "chunk {i} well-formed");
            if i > 0 {
                let (_, pe) = chunks[i - 1];
                assert_eq!(*s, pe + 1, "chunks are adjacent, no gap or overlap");
            }
        }

        for (i, (s, e)) in chunks.iter().enumerate() {
            let len = e - s + 1;
            if i + 1 < chunks.len() {
                assert_eq!(len, size, "non-final chunks have exactly size elements");
            } else {
                assert!(len >= 1 && len <= size, "final chunk is 1..=size");
            }
        }

        let total: i64 = chunks.iter().map(|(s, e)| e - s + 1).sum();
        assert_eq!(total, max - min + 1, "sum of chunks equals total range");
    }

    /// Any span anywhere in i64 — the edges where the arithmetic saturates included.
    #[test]
    fn generate_chunks_tiles_any_span_of_i64() {
        use proptest::prelude::*;
        let edge = 1_000_000_000_000_000i64;
        let min = prop_oneof![
            any::<i64>(),
            (i64::MAX - edge)..=i64::MAX,
            i64::MIN..=(i64::MIN + edge),
        ];
        proptest!(|(min in min, size in 1i64..=1_000_000_000_000, windows in 0i64..=200)| {
            let max = min.saturating_add(size.saturating_mul(windows)).saturating_sub(1);
            prop_assume!(max >= min);
            let chunks = generate_chunks(min, max, size);
            assert_chunk_invariants(min, max, size, &chunks);
        });
    }

    #[test]
    fn generate_chunks_invariants_over_grid() {
        let mins: [i64; 6] = [-50, -1, 0, 1, 100, 1_000_000];
        let sizes: [i64; 8] = [1, 2, 3, 7, 10, 33, 100, 10_000];
        for &min in &mins {
            for &size in &sizes {
                for delta in 0..=(4 * size) {
                    let max = min + delta - 1;
                    let chunks = generate_chunks(min, max, size);
                    assert_chunk_invariants(min, max, size, &chunks);
                }
            }
        }
    }

    #[test]
    fn generate_chunks_empty_on_invalid_inputs() {
        assert!(generate_chunks(10, 5, 10).is_empty());
        assert!(generate_chunks(1, 100, 0).is_empty());
        assert!(generate_chunks(1, 100, -5).is_empty());
    }

    #[test]
    fn the_uncovered_pieces_of_ranges_are_what_no_window_holds() {
        let ranges = [(1, 10), (11, 20), (21, 30)];
        let cut = |windows: &[(i64, i64)]| ranges_uncovered(&ranges, windows);
        assert_eq!(cut(&[(1, 30)]), vec![]);
        assert_eq!(cut(&[(1, 20)]), vec![(21, 30)]);
        assert_eq!(cut(&[(11, 30)]), vec![(1, 10)]);
        assert_eq!(cut(&[(5, 25)]), vec![(1, 4), (26, 30)]);
        assert_eq!(
            cut(&[(12, 14)]),
            vec![(1, 10), (11, 11), (15, 20), (21, 30)]
        );
        assert_eq!(cut(&[(40, 50)]), ranges.to_vec());
        assert_eq!(cut(&[(-9, 0)]), ranges.to_vec());
        assert_eq!(cut(&[]), ranges.to_vec());
        assert_eq!(cut(&[(1, 10), (11, 13), (18, 30)]), vec![(14, 17)]);
        assert_eq!(
            cut(&[(1, 4), (8, 12), (25, 30)]),
            vec![(5, 7), (13, 20), (21, 24)]
        );
        assert_eq!(ranges_uncovered(&[(0, 0)], &[(0, 0)]), vec![]);
        assert_eq!(ranges_uncovered(&[], &[(1, 2)]), vec![]);
        let all = [(i64::MIN, i64::MAX)];
        assert_eq!(ranges_uncovered(&all, &[(i64::MIN, i64::MAX)]), vec![]);
        assert_eq!(
            ranges_uncovered(&all, &[(0, 0)]),
            vec![(i64::MIN, -1), (1, i64::MAX)]
        );
        assert_eq!(
            ranges_uncovered(&all, &[(i64::MIN, 0), (5, i64::MAX)]),
            vec![(1, 4)]
        );
    }

    proptest::proptest! {
        /// Whatever a run stored and whatever the source holds now, the stored windows and the pieces planned beside them cover every key of the new plan once.
        #[test]
        fn stored_windows_and_the_uncovered_pieces_cover_the_new_plan_once(
            min0 in -500i64..500,
            span0 in 0i64..400,
            min1 in -900i64..900,
            span1 in 0i64..900,
            size in 1i64..60,
            dropped in proptest::collection::vec(proptest::prelude::any::<proptest::sample::Index>(), 0..4),
        ) {
            let mut stored = generate_chunks(min0, min0 + span0, size);
            for i in &dropped {
                if stored.len() > 1 {
                    stored.remove(i.index(stored.len()));
                }
            }
            let plan = generate_chunks(min1, min1 + span1, size);
            let added = ranges_uncovered(&plan, &stored);
            let held = |k: i64, set: &[(i64, i64)]| set.iter().filter(|(a, b)| *a <= k && k <= *b).count();
            for k in min1..=min1 + span1 {
                proptest::prop_assert_eq!(held(k, &stored) + held(k, &added), 1, "key {}", k);
            }
            for (a, b) in &added {
                proptest::prop_assert!(a <= b && *a >= min1 && *b <= min1 + span1);
            }
        }
    }

    #[test]
    fn generate_chunks_single_element_range() {
        assert_eq!(generate_chunks(42, 42, 100), vec![(42, 42)]);
        assert_eq!(generate_chunks(42, 42, 1), vec![(42, 42)]);
    }

    #[test]
    fn generate_chunks_chunk_size_larger_than_range() {
        let chunks = generate_chunks(10, 20, 10_000);
        assert_eq!(chunks, vec![(10, 20)]);
        assert_chunk_invariants(10, 20, 10_000, &chunks);
    }

    #[test]
    fn generate_chunks_boundary_exact_multiple() {
        let chunks = generate_chunks(1, 100, 25);
        assert_eq!(chunks, vec![(1, 25), (26, 50), (51, 75), (76, 100)]);
        assert_chunk_invariants(1, 100, 25, &chunks);
    }

    #[test]
    fn generate_chunks_boundary_off_by_one() {
        let chunks = generate_chunks(1, 101, 25);
        assert_eq!(chunks.last(), Some(&(101, 101)));
        assert_chunk_invariants(1, 101, 25, &chunks);
    }

    /// Regression for the i64::MAX overflow that crashed `start + chunk_size - 1` before the saturating fix.
    #[test]
    fn generate_chunks_does_not_overflow_on_near_i64_max() {
        let min = i64::MAX - 10;
        let max = i64::MAX;
        let chunks = generate_chunks(min, max, 3);
        assert_chunk_invariants(min, max, 3, &chunks);
    }

    /// Extreme boundary combinations: each triple must either succeed or terminate immediately (empty).
    #[test]
    fn generate_chunks_does_not_panic_on_extreme_boundaries() {
        let triples: &[(i64, i64, i64)] = &[
            (0, 0, 1),
            (i64::MIN, i64::MIN + 1, 1),
            (i64::MIN, i64::MIN, 1),
            (i64::MIN, i64::MAX, i64::MAX),
            (i64::MAX - 2, i64::MAX, 1),
            (i64::MAX - 2, i64::MAX, 10),
            (i64::MAX - 2, i64::MAX, i64::MAX),
            (-10, 10, 3),
            (0, 0, i64::MAX),
            (0, 0, i64::MIN),
            (5, 5, -1),
        ];
        for &(min, max, size) in triples {
            let _ = generate_chunks(min, max, size);
        }
    }

    fn fp(
        query: &str,
        col: &str,
        size: usize,
        count: Option<usize>,
        by_days: Option<u32>,
    ) -> String {
        chunk_plan_fingerprint(query, col, size, count, by_days)
    }

    /// A plan's TEXT must not be able to imitate another plan's field boundaries.
    #[test]
    fn fingerprint_cannot_be_forged_by_a_query_containing_the_old_separator() {
        let a = fp("Q\u{1f}id", "x", 10, None, None);
        let b = fp("Q", "id\u{1f}x", 10, None, None);
        assert_ne!(
            a, b,
            "a query carrying the separator must not forge the chunk_column boundary"
        );
        assert_ne!(
            fp("ab", "c", 10, None, None),
            fp("a", "bc", 10, None, None),
            "('ab','c') and ('a','bc') are different plans"
        );
    }

    #[test]
    fn fingerprint_changes_when_chunk_count_changes() {
        let base = fp("SELECT * FROM t", "id", 10_000, None, None);
        let with_count = fp("SELECT * FROM t", "id", 10_000, Some(10), None);
        assert_ne!(base, with_count, "chunk_count=None vs Some(10) must differ");
    }

    #[test]
    fn fingerprint_changes_when_chunk_count_value_changes() {
        let c5 = fp("SELECT * FROM t", "id", 10_000, Some(5), None);
        let c10 = fp("SELECT * FROM t", "id", 10_000, Some(10), None);
        assert_ne!(c5, c10, "chunk_count=5 vs chunk_count=10 must differ");
    }

    #[test]
    fn fingerprint_chunk_count_ignores_chunk_size() {
        let a = fp("SELECT * FROM t", "id", 1_000, Some(5), None);
        let b = fp("SELECT * FROM t", "id", 50_000, Some(5), None);
        assert_eq!(
            a, b,
            "chunk_size is not part of the fingerprint when chunk_count is set"
        );
    }

    #[test]
    fn fingerprint_chunk_count_differs_from_range_and_by_days() {
        let count = fp("SELECT * FROM t", "id", 100, Some(4), None);
        let range = fp("SELECT * FROM t", "id", 100, None, None);
        let by_days = fp("SELECT * FROM t", "id", 100, None, Some(7));
        assert_ne!(count, range);
        assert_ne!(count, by_days);
        assert_ne!(range, by_days);
    }

    #[test]
    fn fingerprint_deterministic_for_same_inputs() {
        let a = fp("SELECT id FROM orders", "id", 5_000, Some(8), None);
        let b = fp("SELECT id FROM orders", "id", 5_000, Some(8), None);
        assert_eq!(a, b);
    }
}
