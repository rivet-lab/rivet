//! Chunk boundary detection.

use super::math::{generate_chunks, generate_chunks_checked, strip_simple_projection_from};
use crate::config::quantity::Knob;
use crate::error::Result;
use crate::scalar::{parse_date_flexible, parse_scalar_i64};
use crate::source::Source;

/// `SELECT COUNT(*) FROM (<base>) AS _rivet_rowcnt`, with a fast path when `base_query` is the `SELECT * FROM <ident>` shape produced by the `table:` shortcut.
fn query_wrapped_row_count(
    src: &mut dyn Source,
    base_query: &str,
    source_type: crate::config::SourceType,
) -> Result<i64> {
    let sql = match strip_simple_projection_from(base_query) {
        Some(table_ident) => format!("SELECT COUNT(*) FROM {table_ident}"),
        None => format!(
            "SELECT COUNT(*) FROM ({base_query}) {}",
            crate::sql::derived(source_type, "_rivet_rowcnt")
        ),
    };
    let raw = src.query_scalar(&sql)?.ok_or_else(|| {
        crate::error::coded(
            crate::error::codes::INTERNAL_RUN_INVARIANT,
            "COUNT(*) returned no row",
        )
    })?;
    parse_scalar_i64(&raw)
}

/// The plan of a table with no row: one window that matches nothing, the same for a range and a date key.
const EMPTY_WINDOW: (i64, i64) = (0, 0);

/// The `min`/`max` text of `chunk_column` over `base_query`.
fn read_key_span(
    src: &mut dyn Source,
    base_query: &str,
    chunk_column: &str,
    export_name: &str,
    source_type: crate::config::SourceType,
) -> Result<Option<(String, String)>> {
    let min = src.query_scalar(&crate::sql::aggregate_sql(
        source_type,
        "min",
        chunk_column,
        base_query,
    ))?;
    let max = src.query_scalar(&crate::sql::aggregate_sql(
        source_type,
        "max",
        chunk_column,
        base_query,
    ))?;
    if let (Some(min), Some(max)) = (min, max) {
        return Ok(Some((min, max)));
    }
    let rows = query_wrapped_row_count(src, base_query, source_type)?;
    if rows <= 0 {
        return Ok(None);
    }
    Err(crate::error::coded(
        crate::error::codes::SOURCE_PROBE_UNREADABLE,
        format!(
            "export '{export_name}': min/max({chunk_column}) returned no readable value although \
             the table has ~{rows} row(s) — the driver cannot render this key's type, and a plan \
             on it would export NOTHING while reporting success. Use `chunk_by_key:` (keyset) or \
             a chunk_column of another type."
        ),
    ))
}

/// Refuse to range/date-chunk on a column that has NULL values.
fn holds_fractions(t: &crate::types::RivetType) -> bool {
    use crate::types::RivetType as T;
    match t {
        T::Float32 | T::Float64 => true,
        T::Decimal { scale, .. } => *scale > 0,
        _ => false,
    }
}

/// Refuse a range-sliced `chunk_column` whose result type holds fractions.
fn bail_if_fractional(
    src: &mut dyn Source,
    base_query: &str,
    chunk_column: &str,
    export_name: &str,
) -> Result<()> {
    let Ok(columns) = src.type_mappings(base_query, &crate::types::ColumnOverrides::new()) else {
        return Ok(());
    };
    let Some(col) = columns
        .iter()
        .find(|c| c.column_name.eq_ignore_ascii_case(chunk_column))
    else {
        return Ok(());
    };
    if holds_fractions(&col.rivet_type) {
        return Err(crate::plan::build::chunk_column_not_integer(
            export_name,
            chunk_column,
            &format!("(the query returns it as {})", col.source_native_type),
        ));
    }
    Ok(())
}

/// rows silently.
fn bail_if_null_keyed(
    src: &mut dyn Source,
    base_query: &str,
    chunk_column: &str,
    export_name: &str,
    source_type: crate::config::SourceType,
) -> Result<()> {
    let sql = crate::sql::null_key_probe_sql(source_type, chunk_column, base_query);
    if src.query_scalar(&sql)?.is_some() {
        crate::rivet_bail!(
            crate::error::codes::PLAN_CHUNK_KEY_NULL,
            "export '{}': found NULL in chunk_column '{}'. Range/date chunking filters \
             with `BETWEEN min AND max` (or `>= .. < ..`), which excludes NULL — those rows would \
             be silently dropped from the export. Fix one of: use a NOT NULL column for \
             chunk_column; add `WHERE {} IS NOT NULL` to the query to drop them explicitly; or use \
             `mode: full`.",
            export_name,
            chunk_column,
            chunk_column
        );
    }
    Ok(())
}

fn log_chunk_boundaries_list(export_name: &str, chunks: &[(i64, i64)]) {
    const HEAD: usize = 8;
    const TAIL: usize = 8;
    if chunks.is_empty() {
        log::info!(
            "export '{}': no BETWEEN windows (empty key range)",
            export_name
        );
        return;
    }
    if chunks.len() <= HEAD + TAIL {
        for (i, (a, b)) in chunks.iter().enumerate() {
            log::info!("export '{}':   [{:>5}] {} .. {}", export_name, i, a, b);
        }
    } else {
        for (i, (a, b)) in chunks.iter().enumerate().take(HEAD) {
            log::info!("export '{}':   [{:>5}] {} .. {}", export_name, i, a, b);
        }
        log::info!(
            "export '{}':   ... {} windows omitted ...",
            export_name,
            chunks.len() - HEAD - TAIL
        );
        for (i, (a, b)) in chunks.iter().enumerate().skip(chunks.len() - TAIL) {
            log::info!("export '{}':   [{:>5}] {} .. {}", export_name, i, a, b);
        }
    }
}

/// Sparsity diagnostic computed from a scan-free row **estimate** (catalog stats, never `COUNT(*)`).
fn log_chunk_sparsity_at_run(
    export_name: &str,
    chunk_column: &str,
    chunk_size: usize,
    min_val: i64,
    max_val: i64,
    chunks: &[(i64, i64)],
    row_estimate: i64,
) {
    let info =
        crate::preflight::chunk_sparsity_from_counts(row_estimate, min_val, max_val, chunk_size);
    if info.is_sparse {
        let fill_pct = info.density * 100.0;
        let empty_hint = (1.0 - info.density).clamp(0.0, 1.0) * 100.0;
        log::info!(
            "export '{}': sparse `{}` range — ~{:.2}% of the min..max ID band contains rows (~{:.1}% of logical windows likely empty). \
             rows≈{} (est), span≈{}, chunk_size={}, ~{} logical windows, {} BETWEEN chunks. Computed boundaries:",
            export_name,
            chunk_column,
            fill_pct,
            empty_hint,
            info.row_count,
            info.range_span,
            chunk_size,
            info.logical_windows,
            chunks.len(),
        );
        log_chunk_boundaries_list(export_name, chunks);
    } else {
        log::info!(
            "export '{}': `{}` range looks dense enough for BETWEEN chunking (rows≈{} est, span≈{}, density≈{:.6}, {} chunks); continuing",
            export_name,
            chunk_column,
            info.row_count,
            info.range_span,
            info.density,
            chunks.len(),
        );
    }
}

/// Below this many windows, per-chunk round-trips are cheap enough that a sparse key isn't worth a warning even over a slow link.
const SPARSE_WARN_MIN_CHUNKS: usize = 64;
/// With no scan-free row estimate (MySQL / curated query) we can't compute the rows-vs-windows ratio, so we only flag an *egregious* absolute window count.
const NO_ESTIMATE_WARN_CHUNKS: usize = 1000;

/// What to do about a `chunk_size` range plan whose window count far exceeds what the rows justify (a sparse / huge / gappy key).
enum SparseAction {
    /// A scan-free row estimate PROVES the blow-up (PG/MSSQL) → refuse the run so the user can't silently pay N-thousand round-trips (esp.
    Bail(String),
    /// No trustworthy estimate (MySQL — `TABLE_ROWS` too unreliable.
    Warn(String),
}

/// Decide whether a `chunk_size` range plan is pathologically sparse.
fn sparse_chunk_action(
    chunk_count: usize,
    row_estimate: Option<i64>,
    chunk_size: usize,
) -> Option<SparseAction> {
    if chunk_count < SPARSE_WARN_MIN_CHUNKS {
        return None;
    }
    let cs = chunk_size.max(1);
    match row_estimate.filter(|&r| r > 0) {
        Some(rows) => {
            let row_windows = (rows as usize).div_ceil(cs).max(1);
            if chunk_count < row_windows.saturating_mul(4) {
                return None;
            }
            let avg = (rows as usize) / chunk_count.max(1);
            Some(SparseAction::Bail(format!(
                "refusing to run a sparse range plan — {chunk_count} chunk windows for \
                 ~{rows} rows (~{avg} rows/window vs chunk_size {cs}). `chunk_size` divides \
                 the KEY RANGE, not the row count, so most windows are near-empty and each is \
                 a separate source query: {chunk_count} round-trips, very slow over a \
                 tunnel/VPN. Set one of: `chunk_by_key: <unique key>` (keyset paging — \
                 windows track rows and stay correct under concurrent writes), \
                 `chunk_count: N`, or `mode: full`."
            )))
        }
        None => {
            if chunk_count < NO_ESTIMATE_WARN_CHUNKS {
                return None;
            }
            let example_n = SPARSE_WARN_MIN_CHUNKS;
            let factor = chunk_count / example_n;
            Some(SparseAction::Warn(format!(
                "{chunk_count} chunk windows on a range key (no scan-free row estimate to \
                 confirm density). If the key is sparse (large / gappy ids), most windows are \
                 near-empty and this is {chunk_count} source round-trips — very slow over a \
                 tunnel/VPN. If so: `chunk_by_key: <unique key>` pages by keyset, tracking \
                 ROWS not the id span; or `chunk_count: {example_n}` → {example_n} \
                 windows (~{factor}× fewer round-trips); or `mode: full`."
            )))
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn detect_and_generate_chunks(
    src: &mut dyn Source,
    base_query: &str,
    chunk_column: &str,
    chunk_size: usize,
    chunk_count: Option<usize>,
    export_name: &str,
    chunk_by_days: Option<u32>,
    source_type: crate::config::SourceType,
) -> Result<Vec<(i64, i64)>> {
    if let Some(count) = chunk_count {
        Knob::CHUNK_COUNT.validate_export(export_name, count)?;
    } else if chunk_by_days.is_none() {
        Knob::CHUNK_SIZE.validate_export(export_name, chunk_size)?;
    }
    if let Some(days_per_chunk) = chunk_by_days {
        bail_if_null_keyed(src, base_query, chunk_column, export_name, source_type)?;
        let Some((min_str, max_str)) =
            read_key_span(src, base_query, chunk_column, export_name, source_type)?
        else {
            return Ok(vec![EMPTY_WINDOW]);
        };

        let min_date = parse_date_flexible(&min_str).ok_or_else(|| {
            crate::error::coded(crate::error::codes::SOURCE_PROBE_UNREADABLE, format!(
                "export '{}': could not parse min({}) = {:?} as a date (expected YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
                export_name, chunk_column, min_str
            ))
        })?;
        let max_date = parse_date_flexible(&max_str).ok_or_else(|| {
            crate::error::coded(crate::error::codes::SOURCE_PROBE_UNREADABLE, format!(
                "export '{}': could not parse max({}) = {:?} as a date (expected YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
                export_name, chunk_column, max_str
            ))
        })?;

        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
        let min_days = (min_date - epoch).num_days();
        let max_days = (max_date - epoch).num_days();
        let total_days = (max_days - min_days).max(0);
        let chunks = generate_chunks(min_days, max_days, days_per_chunk as i64);
        log::info!(
            "export '{}': date chunk_column `{}` range {} .. {} ({} days, {} days/chunk → {} chunk(s))",
            export_name,
            chunk_column,
            min_date,
            max_date,
            total_days,
            days_per_chunk,
            chunks.len()
        );
        return Ok(chunks);
    }

    bail_if_null_keyed(src, base_query, chunk_column, export_name, source_type)?;
    bail_if_fractional(src, base_query, chunk_column, export_name)?;

    let strict_i64 = |text: &str| -> Option<i64> {
        let t = text.trim();
        if let Ok(v) = t.parse::<i64>() {
            return Some(v);
        }
        match t.split_once('.') {
            Some((head, frac)) if frac.bytes().all(|b| b == b'0') => head.parse::<i64>().ok(),
            _ => None,
        }
    };
    let read_bound = |text: &str, which: &str| -> Result<i64> {
        strict_i64(text).ok_or_else(|| {
            crate::error::coded(
                crate::error::codes::CONFIG_CHUNK_COLUMN_NOT_INTEGER,
                format!(
                    "export '{}': {}({}) = {:?} is not a whole integer within the 64-bit range — \
                     range chunking slices with an integer BETWEEN, so a fractional key silently \
                     drops rows between windows and a wider one cannot be sliced. Use \
                     `chunk_by_key:` (keyset — any orderable unique indexed key) or an integer \
                     column.",
                    export_name, which, chunk_column, text
                ),
            )
        })
    };
    let (min_val, max_val) =
        match read_key_span(src, base_query, chunk_column, export_name, source_type)? {
            Some((min_text, max_text)) => {
                (read_bound(&min_text, "min")?, read_bound(&max_text, "max")?)
            }
            None => EMPTY_WINDOW,
        };

    let effective_chunk_size =
        Knob::window_size(chunk_size, chunk_count, Knob::key_span(min_val, max_val));

    log::info!(
        "export '{}': chunk_column `{}` range {} .. {} ({})",
        export_name,
        chunk_column,
        min_val,
        max_val,
        if let Some(count) = chunk_count {
            format!("chunk_count={count}, effective chunk_size={effective_chunk_size}")
        } else {
            format!("chunk_size={effective_chunk_size}")
        }
    );

    let chunks = generate_chunks_checked(min_val, max_val, effective_chunk_size).map_err(|e| {
        crate::error::recoded(
            &e,
            crate::error::codes::PLAN_CHUNK_KEY_SPARSE,
            format!("export '{export_name}': {e}"),
        )
    })?;

    let row_estimate = strip_simple_projection_from(base_query)
        .and_then(|table_ident| crate::sql::row_estimate_sql(source_type, table_ident))
        .and_then(|sql| {
            src.query_scalar(&sql)
                .ok()
                .flatten()
                .and_then(|s| s.trim().parse::<i64>().ok())
                .filter(|&n| n > 0)
        });

    if chunk_count.is_none() {
        match sparse_chunk_action(chunks.len(), row_estimate, chunk_size) {
            Some(SparseAction::Bail(msg)) => crate::rivet_bail!(
                crate::error::codes::PLAN_CHUNK_KEY_SPARSE,
                "export '{export_name}': {msg}"
            ),
            Some(SparseAction::Warn(msg)) => log::warn!("export '{export_name}': {msg}"),
            None => {}
        }
    }

    match row_estimate {
        Some(est) => log_chunk_sparsity_at_run(
            export_name,
            chunk_column,
            chunk_size,
            min_val,
            max_val,
            &chunks,
            est,
        ),
        None => {
            log::info!(
                "export '{}': {} BETWEEN window(s) from `{}` min..max (no scan-free row estimate available — sparsity check skipped, no COUNT(*))",
                export_name,
                chunks.len(),
                chunk_column,
            );
            if chunks.len() <= 24 {
                log_chunk_boundaries_list(export_name, &chunks);
            }
        }
    }

    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SourceType;
    use std::collections::VecDeque;

    /// Sniffs every SQL string that gets queried.
    struct ScriptedSource {
        replies: VecDeque<Result<Option<String>>>,
        seen_sql: Vec<String>,
        /// Drives the NULL-key guard's presence probe (`… WHERE col IS NULL`): `> 0` ⇒ a NULL exists (one row).
        null_keys: i64,
    }

    impl ScriptedSource {
        fn new(items: impl IntoIterator<Item = Result<Option<String>>>) -> Self {
            Self {
                replies: items.into_iter().collect(),
                seen_sql: Vec::new(),
                null_keys: 0,
            }
        }

        fn with_null_keys(mut self, n: i64) -> Self {
            self.null_keys = n;
            self
        }
    }

    fn ok(s: &str) -> Result<Option<String>> {
        Ok(Some(s.to_string()))
    }
    fn null() -> Result<Option<String>> {
        Ok(None)
    }

    impl crate::source::Source for ScriptedSource {
        fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
            self.seen_sql.push(sql.to_string());
            if sql.contains("IS NULL") {
                return Ok((self.null_keys > 0).then(|| "1".to_string()));
            }
            self.replies
                .pop_front()
                .expect("ScriptedSource: queue exhausted")
        }
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> Result<()> {
            unimplemented!()
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _column_overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            Ok(Vec::new())
        }
    }

    /// A source that describes one column, `k`, as `t`; every other call is unreachable here.
    struct DescribedSource(crate::types::RivetType);

    impl crate::source::Source for DescribedSource {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> Result<()> {
            unreachable!()
        }
        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>> {
            unreachable!()
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _column_overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![crate::types::TypeMapping {
                column_name: "K".into(),
                source_native_type: "double precision".into(),
                rivet_type: self.0.clone(),
                arrow_type: None,
                fidelity: crate::types::TypeFidelity::Exact,
                nullable: false,
                warnings: vec![],
                delivery: crate::types::Delivery::Native,
                refusal: None,
            }])
        }
    }

    #[test]
    fn a_fractional_chunk_column_is_refused_by_code_naming_its_type() {
        use crate::types::RivetType as T;
        let err = bail_if_fractional(
            &mut DescribedSource(T::Float64),
            "SELECT k FROM t",
            "k",
            "e",
        )
        .expect_err("a float key is refused");
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_CONFIG_CHUNK_COLUMN_NOT_INTEGER")
        );
        assert!(
            format!("{err:#}")
                .contains("chunk_column 'k' (the query returns it as double precision)"),
            "{err:#}"
        );
        assert!(
            bail_if_fractional(&mut DescribedSource(T::Int64), "SELECT k FROM t", "k", "e").is_ok()
        );
        assert!(
            bail_if_fractional(
                &mut DescribedSource(T::Float64),
                "SELECT k FROM t",
                "other",
                "e"
            )
            .is_ok(),
            "a column the query does not describe is left to the bound read"
        );
    }

    #[test]
    fn only_a_type_that_holds_fractions_is_refused_for_range_slicing() {
        use crate::types::RivetType as T;
        for t in [
            T::Float32,
            T::Float64,
            T::Decimal {
                precision: 10,
                scale: 2,
            },
        ] {
            assert!(holds_fractions(&t), "{t:?}");
        }
        for t in [
            T::Int16,
            T::Int32,
            T::Int64,
            T::UInt64,
            T::Decimal {
                precision: 19,
                scale: 0,
            },
            T::Decimal {
                precision: 38,
                scale: -2,
            },
        ] {
            assert!(!holds_fractions(&t), "{t:?}");
        }
    }

    fn detect(
        src: &mut dyn crate::source::Source,
        chunk_size: usize,
        chunk_by_days: Option<u32>,
    ) -> Result<Vec<(i64, i64)>> {
        detect_and_generate_chunks(
            src,
            "SELECT * FROM orders",
            "id",
            chunk_size,
            None,
            "orders",
            chunk_by_days,
            SourceType::Postgres,
        )
    }

    #[test]
    fn integer_range_basic_chunks_computed_correctly() {
        let mut src = ScriptedSource::new([ok("1"), ok("1000"), ok("500")]);
        let chunks = detect(&mut src, 100, None).unwrap();
        assert_eq!(chunks.len(), 10);
        assert_eq!(chunks[0], (1, 100));
        assert_eq!(chunks[9], (901, 1000));
    }

    fn detect_counted(
        src: &mut dyn crate::source::Source,
        chunk_size: usize,
        chunk_count: Option<usize>,
    ) -> Result<Vec<(i64, i64)>> {
        detect_and_generate_chunks(
            src,
            "SELECT * FROM orders",
            "id",
            chunk_size,
            chunk_count,
            "orders",
            None,
            SourceType::Postgres,
        )
    }

    #[test]
    fn a_key_range_as_wide_as_i64_is_split_by_count_and_by_size_without_overflow() {
        let wide = || {
            ScriptedSource::new([
                ok("-9223372036854775808"),
                ok("9223372036854775807"),
                ok("2"),
            ])
        };
        let one = detect_counted(&mut wide(), 100_000, Some(1)).unwrap();
        assert_eq!(one, vec![(i64::MIN, i64::MAX)]);
        let two = detect_counted(&mut wide(), 100_000, Some(2)).unwrap();
        assert_eq!(two, vec![(i64::MIN, -1), (0, i64::MAX)]);
        let size = detect_counted(&mut wide(), i64::MAX as usize, None).unwrap();
        assert_eq!(
            size,
            vec![(i64::MIN, -2), (-1, i64::MAX - 2), (i64::MAX - 1, i64::MAX)]
        );
    }

    #[test]
    fn a_sealed_plan_with_a_count_past_i64_is_refused_at_run_by_code() {
        let past = i64::MAX as usize + 1;
        for (size, count) in [(past, None), (0, None), (100, Some(past)), (100, Some(0))] {
            let mut src = ScriptedSource::new([ok("1"), ok("1000"), ok("500")]);
            let err = detect_counted(&mut src, size, count).expect_err("refused before any read");
            assert!(
                matches!(
                    crate::error::error_code(&err),
                    Some(
                        "RIVET_CONFIG_VALUE_OUT_OF_RANGE"
                            | "RIVET_CONFIG_MODE_SETTING_INVALID"
                            | "RIVET_CONFIG_CHUNK_COUNT_INVALID"
                    )
                ),
                "{err:#}"
            );
        }
    }

    /// An EMPTY table plans one empty window — the single legitimate zero, reached by a row count of 0.
    #[test]
    fn integer_range_empty_table_plans_one_empty_window() {
        let mut src = ScriptedSource::new([null(), null(), ok("0"), ok("0")]);
        let chunks = detect(&mut src, 100, None).unwrap();
        assert_eq!(chunks, vec![(0, 0)]);
    }

    /// The case that made the rename necessary: the table HAS rows and the driver cannot render the key's type, so the bound arrives as `None` for a reason that has nothing to do with emptiness.
    #[test]
    fn unrenderable_bound_on_a_non_empty_table_is_an_error() {
        for by_days in [None, Some(7)] {
            let mut src = ScriptedSource::new([null(), null(), ok("100")]);
            let err = detect(&mut src, 10, by_days)
                .expect_err("a None bound over a NON-empty table must abort, not plan (0,0)");
            let msg = format!("{err:#}");
            assert!(
                msg.contains("no readable value although the table has ~100 row(s)")
                    && msg.contains("chunk_by_key"),
                "the error must name the cause and the escape; got: {msg}"
            );
            crate::error::assert_coded(&err, "RIVET_SOURCE_PROBE_UNREADABLE", 5);
        }
    }

    /// A NON-EMPTY table whose bounds cannot be read as i64 must ERROR, not silently become the single window `BETWEEN 0 AND 0`.
    #[test]
    fn unparseable_integer_range_bounds_are_an_error_not_a_zero_window() {
        let mut src = ScriptedSource::new([ok("1.50"), ok("998.75"), ok("100")]);
        let err = detect(&mut src, 10, None).expect_err(
            "an unreadable bound must abort the plan, not collapse to (0,0) and export nothing",
        );
        let msg = format!("{err:#}");
        assert!(
            msg.contains("not a whole integer") && msg.contains("chunk_by_key"),
            "the error must name the problem and the escape; got: {msg}"
        );
        crate::error::assert_coded(&err, "RIVET_CONFIG_CHUNK_COLUMN_NOT_INTEGER", 1);
    }

    #[test]
    fn integer_range_count_failure_still_returns_chunks() {
        let mut src = ScriptedSource::new([ok("1"), ok("100"), ok("not-a-number")]);
        let chunks = detect(&mut src, 50, None).unwrap();
        assert_eq!(chunks, vec![(1, 50), (51, 100)]);
    }

    #[test]
    fn integer_range_single_chunk_when_range_smaller_than_size() {
        let mut src = ScriptedSource::new([ok("5"), ok("20"), ok("15")]);
        let chunks = detect(&mut src, 100, None).unwrap();
        assert_eq!(chunks, vec![(5, 20)]);
    }

    #[test]
    fn null_keyed_rows_in_integer_range_bail_not_silently_dropped() {
        let mut src = ScriptedSource::new([ok("1"), ok("1000"), ok("500")]).with_null_keys(3);
        let err = detect(&mut src, 100, None).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("NULL in chunk_column"), "got: {msg}");
        crate::error::assert_coded(&err, "RIVET_PLAN_CHUNK_KEY_NULL", 5);
    }

    #[test]
    fn null_keyed_rows_in_date_range_bail() {
        let mut src = ScriptedSource::new([ok("2023-01-01"), ok("2023-01-31")]).with_null_keys(7);
        let err = detect(&mut src, 10_000, Some(7)).unwrap_err();
        assert!(format!("{err:#}").contains("NULL in chunk_column"));
        crate::error::assert_coded(&err, "RIVET_PLAN_CHUNK_KEY_NULL", 5);
    }

    /// A table whose every key is NULL (min and max NULL over rows) is refused as NULL-keyed under both chunkings, never planned as empty.
    #[test]
    fn an_all_null_key_is_refused_as_null_keyed_not_planned_empty() {
        for by_days in [None, Some(7)] {
            let mut src = ScriptedSource::new([null(), null(), ok("40")]).with_null_keys(40);
            let err = detect(&mut src, 10_000, by_days).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_PLAN_CHUNK_KEY_NULL", 5);
            assert!(
                !src.seen_sql.iter().any(|s| s.starts_with("SELECT min(")),
                "the refusal comes before any bound is read: {:?}",
                src.seen_sql
            );
        }
    }

    #[test]
    fn chunk_by_days_basic_range_produces_windows() {
        let mut src = ScriptedSource::new([ok("2023-01-01"), ok("2023-01-31")]);
        let chunks = detect(&mut src, 10_000, Some(7)).unwrap();
        assert_eq!(
            chunks.len(),
            5,
            "expected 5 weekly windows, got: {chunks:?}"
        );
    }

    #[test]
    fn chunk_by_days_single_day_returns_one_chunk() {
        let mut src = ScriptedSource::new([ok("2024-03-15"), ok("2024-03-15")]);
        let chunks = detect(&mut src, 10_000, Some(7)).unwrap();
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn chunk_by_days_empty_table_plans_the_empty_window_a_range_key_plans() {
        let mut src = ScriptedSource::new([null(), null(), ok("0")]);
        assert_eq!(detect(&mut src, 10_000, Some(7)).unwrap(), vec![(0, 0)]);
        let mut range = ScriptedSource::new([null(), null(), ok("0"), ok("0")]);
        assert_eq!(detect(&mut range, 100, None).unwrap(), vec![(0, 0)]);
    }

    /// The span reader: both bounds read, no count; no row, `None`; a bound absent over rows, the coded refusal.
    #[test]
    fn read_key_span_reads_bounds_and_counts_only_when_one_is_absent() {
        let span = |replies: Vec<Result<Option<String>>>| {
            let mut src = ScriptedSource::new(replies);
            let got = read_key_span(&mut src, "SELECT * FROM t", "k", "e", SourceType::Postgres);
            (got, src.seen_sql)
        };
        let (got, seen) = span(vec![ok("1"), ok("9")]);
        assert_eq!(got.unwrap(), Some(("1".into(), "9".into())));
        assert_eq!(seen.len(), 2, "no count when both bounds read: {seen:?}");
        let (got, seen) = span(vec![null(), null(), ok("0")]);
        assert_eq!(got.unwrap(), None);
        assert_eq!(seen[2], "SELECT COUNT(*) FROM t");
        for replies in [
            vec![null(), null(), ok("3")],
            vec![ok("1"), null(), ok("3")],
            vec![null(), ok("9"), ok("3")],
        ] {
            let err = span(replies).0.unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_PROBE_UNREADABLE", 5);
            assert!(
                format!("{err:#}").contains(
                    "export 'e': min/max(k) returned no readable value although the table has ~3 row(s)"
                ),
                "{err:#}"
            );
        }
    }

    #[test]
    fn chunk_by_days_invalid_date_string_returns_error() {
        let mut src = ScriptedSource::new([ok("not-a-date"), ok("2023-12-31")]);
        let result = detect(&mut src, 10_000, Some(7));
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(msg.contains("parse") || msg.contains("date"), "got: {msg}");
    }

    #[test]
    fn chunk_by_days_datetime_format_parsed_correctly() {
        let mut src = ScriptedSource::new([ok("2023-06-01 00:00:00"), ok("2023-06-30 23:59:59")]);
        let chunks = detect(&mut src, 10_000, Some(7)).unwrap();
        assert_eq!(chunks.len(), 5, "expected 5 windows, got: {chunks:?}");
    }

    fn detect_with_count(
        src: &mut dyn crate::source::Source,
        chunk_count: usize,
    ) -> Result<Vec<(i64, i64)>> {
        detect_and_generate_chunks(
            src,
            "SELECT * FROM orders",
            "id",
            100_000,
            Some(chunk_count),
            "orders",
            None,
            SourceType::Postgres,
        )
    }

    #[test]
    fn chunk_count_divides_range_into_exact_n_chunks() {
        let mut src = ScriptedSource::new([ok("1"), ok("1000"), ok("500")]);
        let chunks = detect_with_count(&mut src, 10).unwrap();
        assert_eq!(chunks.len(), 10, "expected 10 chunks: {chunks:?}");
        assert_eq!(chunks[0].0, 1);
        assert_eq!(chunks[9].1, 1000);
    }

    #[test]
    fn chunk_count_ceiling_division_produces_at_most_n_chunks() {
        let mut src = ScriptedSource::new([ok("1"), ok("100"), ok("70")]);
        let chunks = detect_with_count(&mut src, 7).unwrap();
        assert!(
            chunks.len() <= 7,
            "must produce at most 7 chunks, got {}: {chunks:?}",
            chunks.len()
        );
        assert_eq!(chunks.first().unwrap().0, 1);
        assert_eq!(chunks.last().unwrap().1, 100);
    }

    #[test]
    fn chunk_count_1_produces_single_full_range_chunk() {
        let mut src = ScriptedSource::new([ok("1"), ok("999"), ok("500")]);
        let chunks = detect_with_count(&mut src, 1).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], (1, 999));
    }

    fn detect_with_base(
        src: &mut ScriptedSource,
        base_query: &str,
        chunk_by_days: Option<u32>,
    ) -> Result<Vec<(i64, i64)>> {
        detect_and_generate_chunks(
            src,
            base_query,
            "id",
            100,
            None,
            "tbl",
            chunk_by_days,
            SourceType::Postgres,
        )
    }

    #[test]
    fn fast_path_select_star_emits_min_max_estimate_without_subquery_wrap() {
        let mut src = ScriptedSource::new([ok("1"), ok("1000"), ok("500")]);
        let _ = detect_with_base(&mut src, "SELECT * FROM public.users", None).unwrap();
        for sql in &src.seen_sql {
            assert!(
                !sql.contains("_rivet") && !sql.contains("FROM ("),
                "fast path must not wrap; got: {sql}"
            );
        }
        assert!(src.seen_sql.iter().any(|s| s.starts_with("SELECT min(")));
        assert!(src.seen_sql.iter().any(|s| s.starts_with("SELECT max(")));
        assert!(
            src.seen_sql
                .iter()
                .any(|s| s.contains("reltuples") && s.contains("pg_class")),
            "range path must use a scan-free estimate; saw: {:?}",
            src.seen_sql
        );
        assert!(
            !src.seen_sql
                .iter()
                .any(|s| s.contains("SELECT COUNT(*) FROM")),
            "range path must NOT issue a full COUNT(*) scan; saw: {:?}",
            src.seen_sql
        );
    }

    #[test]
    fn range_path_uses_scanfree_estimate_never_full_count() {
        let mut src = ScriptedSource::new([ok("1"), ok("1000000"), ok("950000")]);
        let _ = detect_with_base(&mut src, "SELECT * FROM warranty", None).unwrap();
        assert!(
            src.seen_sql.iter().any(|s| s.contains("reltuples")
                || s.contains("sys.partitions")
                || s.contains("TABLE_ROWS")),
            "expected a scan-free row estimate; saw: {:?}",
            src.seen_sql
        );
        assert!(
            !src.seen_sql
                .iter()
                .any(|s| s.contains("SELECT COUNT(*) FROM")),
            "range path must never full-scan COUNT(*); saw: {:?}",
            src.seen_sql
        );
    }

    #[test]
    fn mysql_range_path_skips_estimate_no_count_scan() {
        let mut src = ScriptedSource::new([ok("1"), ok("1000000")]);
        let chunks = detect_and_generate_chunks(
            &mut src,
            "SELECT * FROM warranty",
            "id",
            100_000,
            None,
            "warranty",
            None,
            SourceType::Mysql,
        )
        .unwrap();
        assert!(!chunks.is_empty());
        assert!(
            !src.seen_sql
                .iter()
                .any(|s| s.contains("TABLE_ROWS") || s.contains("SELECT COUNT(*) FROM")),
            "MySQL range path must issue no row-count/estimate query: {:?}",
            src.seen_sql
        );
        assert!(
            src.seen_sql.iter().any(|s| s.starts_with("SELECT min(")),
            "min/max still issued: {:?}",
            src.seen_sql
        );
    }

    #[test]
    fn fallback_curated_query_wraps_minmax_and_skips_count_scan() {
        let mut src = ScriptedSource::new([ok("1"), ok("1000"), ok("500")]);
        let _ =
            detect_with_base(&mut src, "SELECT id FROM public.users WHERE active", None).unwrap();
        assert!(
            src.seen_sql.iter().any(|s| s.contains("AS _rivet")),
            "min/max should still wrap for a curated query: {:?}",
            src.seen_sql
        );
        assert!(
            !src.seen_sql.iter().any(|s| s.contains("_rivet_rowcnt")
                || s.contains("reltuples")
                || s.contains("SELECT COUNT(*) FROM")),
            "curated query must issue no row-count scan: {:?}",
            src.seen_sql
        );
    }

    #[test]
    fn fast_path_chunk_by_days_also_unwraps() {
        let mut src = ScriptedSource::new([ok("2024-01-01"), ok("2024-12-31")]);
        let _ = detect_with_base(&mut src, "SELECT * FROM public.events", Some(7)).unwrap();
        for sql in &src.seen_sql {
            assert!(
                !sql.contains("_rivet") && !sql.contains("FROM ("),
                "chunk_by_days fast path must not wrap; got: {sql}"
            );
        }
    }

    #[test]
    fn sparse_with_estimate_bails_and_names_keyset() {
        let msg = match sparse_chunk_action(3428, Some(520_789), 100_000).expect("571x blow-up") {
            SparseAction::Bail(m) => m,
            SparseAction::Warn(m) => panic!("proven sparse must bail, not warn: {m}"),
        };
        assert!(msg.contains("refusing"), "must be a refusal: {msg}");
        assert!(
            msg.contains("chunk_by_key: <unique key>"),
            "not actionable: {msg}"
        );
        assert!(msg.contains("3428"), "should cite the window count: {msg}");
    }

    #[test]
    fn sparse_hints_never_recommend_the_removed_chunk_dense() {
        for msg in [
            match sparse_chunk_action(3428, Some(520_789), 100_000).unwrap() {
                SparseAction::Bail(m) | SparseAction::Warn(m) => m,
            },
            match sparse_chunk_action(3428, None, 100_000).unwrap() {
                SparseAction::Bail(m) | SparseAction::Warn(m) => m,
            },
        ] {
            assert!(msg.contains("`chunk_by_key: <unique key>`"), "{msg}");
            assert!(!msg.contains("chunk_dense"), "{msg}");
        }
    }

    #[test]
    fn dense_with_estimate_is_silent() {
        assert!(sparse_chunk_action(20, Some(2_000_000), 100_000).is_none());
    }

    #[test]
    fn no_estimate_egregious_count_warns_never_bails() {
        let msg = match sparse_chunk_action(3428, None, 100_000).expect("3428 windows must flag") {
            SparseAction::Warn(m) => m,
            SparseAction::Bail(m) => panic!("unprovable suspicion must warn, not bail: {m}"),
        };
        assert!(
            msg.contains("chunk_by_key: <unique key>"),
            "not actionable: {msg}"
        );
        assert!(msg.contains("If the key is sparse"), "must hedge: {msg}");
        assert!(
            msg.contains("chunk_count: 64"),
            "should give a concrete N: {msg}"
        );
        assert!(msg.contains("53"), "should quantify the reduction: {msg}");
    }

    #[test]
    fn no_estimate_moderate_count_is_silent() {
        assert!(sparse_chunk_action(900, None, 100_000).is_none());
    }

    #[test]
    fn small_window_count_never_flags() {
        assert!(sparse_chunk_action(40, Some(10), 100_000).is_none());
        assert!(sparse_chunk_action(40, None, 100_000).is_none());
    }

    #[test]
    fn sparse_thresholds_are_exact() {
        assert!(
            sparse_chunk_action(64, Some(10), 100_000).is_some(),
            "exactly-at-floor sparse plan must flag (`<`, not `<=`)"
        );
        assert!(sparse_chunk_action(64, Some(0), 100_000).is_none());
        assert!(sparse_chunk_action(400, Some(100_000), 1_000).is_some());
        assert!(sparse_chunk_action(399, Some(100_000), 1_000).is_none());
        match sparse_chunk_action(400, Some(100_000), 1_000).unwrap() {
            SparseAction::Bail(m) => {
                assert!(m.contains("~250 rows/window"), "avg must be exact: {m}")
            }
            SparseAction::Warn(m) => panic!("provable blow-up must bail: {m}"),
        }
        assert!(sparse_chunk_action(1000, None, 100_000).is_some());
        assert!(sparse_chunk_action(999, None, 100_000).is_none());
    }

    #[test]
    fn chunk_count_larger_than_range_caps_at_range_size() {
        let mut src = ScriptedSource::new([ok("1"), ok("5"), ok("5")]);
        let chunks = detect_with_count(&mut src, 100).unwrap();
        assert_eq!(chunks.len(), 5, "got: {chunks:?}");
        assert_eq!(chunks[0], (1, 1));
        assert_eq!(chunks[4], (5, 5));
    }

    /// A span past the window ceiling keeps the sparse-key code through the export-name rewrap.
    #[test]
    fn a_sparse_key_and_an_unanswered_count_carry_their_codes() {
        let mut src = ScriptedSource::new([ok("0"), ok(&i64::MAX.to_string())]);
        let err = detect(&mut src, 100_000, None).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_PLAN_CHUNK_KEY_SPARSE", 1);
        assert!(format!("{err}").starts_with("export '"), "{err}");

        let mut src = ScriptedSource::new([null()]);
        let err = query_wrapped_row_count(
            &mut src,
            "SELECT * FROM t",
            crate::config::SourceType::Postgres,
        )
        .unwrap_err();
        crate::error::assert_coded(&err, "RIVET_INTERNAL_RUN_INVARIANT", 6);

        let mut src = ScriptedSource::new([ok("2023-01-01"), ok("not a date")]);
        let err = detect(&mut src, 10_000, Some(7)).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_PROBE_UNREADABLE", 5);
    }
}
