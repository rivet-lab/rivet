//! **Layer: Execution** — keyset (seek) pagination runner (OPT-4).
//!
//! The source-safe shape for tables without a single-integer PK. Pages the
//! table by one index-backed, NOT NULL, unique key:
//!
//! ```sql
//! SELECT * FROM (<base>) AS _rivet [WHERE <key> > <last>] ORDER BY <key> LIMIT <n>
//! ```
//!
//! Each page is a bounded, index-driven range scan (never a filesort — the key
//! is index-backed by construction, see [`crate::plan::build`]) and becomes one
//! output part file. This bounds both peak RSS (`<= chunk_size` rows in flight)
//! and longest-query time (one `LIMIT` seek), unlike a `mode: full` snapshot
//! which holds a single unbounded `SELECT` open with no MySQL server cursor.
//!
//! Reuses the incremental machinery: the driver builds the page via
//! [`crate::source::query::build_keyset_query`] with the same injection-safe
//! value handling as incremental, and [`ExportSink`] tracks the per-page max
//! key in `last_cursor_value` (its `cursor_extract_column` resolves to the
//! keyset key), which the loop reads to advance to the next page.

use super::{RunSummary, key_space::Axis, sink::ExportSink};
use crate::config::IncrementalCursorMode;
use crate::destination;
use crate::error::Result;
use crate::plan::{ExtractionStrategy, IncrementalCursorPlan, KeysetPlan, ResolvedRunPlan};
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::types::CursorState;

/// Rehydrate a keyset resume's prior committed pages WITH a destination probe.
fn rehydrate_keyset_pages_probed(
    st: &crate::state::StateStore,
    run_id: &str,
    plan: &ResolvedRunPlan,
    summary: &mut crate::pipeline::RunSummary,
) -> anyhow::Result<()> {
    let probe: Option<super::chunked::Listed> =
        match crate::destination::create_destination(&plan.destination) {
            Ok(dest) if dest.capabilities().commit_protocol.leaves_objects_at_rest() => {
                match dest.list_prefix("") {
                    Ok(listing) => Some(
                        listing
                            .iter()
                            .map(|m| {
                                let base = m.key.rsplit('/').next().unwrap_or(&m.key);
                                (base.to_string(), m.size_bytes)
                            })
                            .collect::<super::chunked::Listed>(),
                    ),
                    Err(e) => {
                        log::warn!(
                            "keyset resume: cannot list the destination to verify prior \
                             pages ({e:#}) — declaring from the state DB unverified"
                        );
                        None
                    }
                }
            }
            _ => None,
        };
    let (_, missing) =
        super::chunked::rehydrate_manifest_parts_probed(st, run_id, summary, probe.as_ref())?;
    if !missing.is_empty() {
        let names: Vec<&str> = missing.iter().map(|(_, n)| n.as_str()).collect();
        return Err(committed_pages_gone(&plan.export_name, &names));
    }
    Ok(())
}

fn keyset_plan(plan: &ResolvedRunPlan) -> &KeysetPlan {
    match &plan.strategy {
        ExtractionStrategy::Keyset(kp) => kp,
        _ => unreachable!("keyset runner called with non-keyset plan"),
    }
}

/// One keyset page produced by [`read_keyset_page`].
pub(crate) struct KeysetPage {
    pub(crate) rows: usize,
    /// What this page's sink SAW: dest schema (run fingerprint) + column max bytes.
    pub(crate) observed: super::commit::Observations,
    pub(crate) next_cursor: Option<String>,
    /// First observed key of this page (the run floor when it is page 1 of range 0).
    pub(crate) first_cursor: Option<String>,
    /// This page's sink's Form-B value checksums and their key column.
    pub(crate) checksums: super::commit::UnitChecksums,
}

/// The parts a page read made durable, and the page (`None` when empty) or the error that stopped it.
pub(crate) type PageRead = (Vec<super::commit::PartRecord>, Result<Option<KeysetPage>>);

/// Read ONE seek page: `find`-and-seek from `cursor` (or the range floor), write its parts to `dest` named by `part_base`, and report the page + the typed high-water cursor.
pub(crate) fn read_keyset_page(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key_plan: &IncrementalCursorPlan,
    page_size: usize,
    cursor: Option<&str>,
    dest: &dyn destination::Destination,
    part_base: &str,
) -> PageRead {
    read_keyset_page_bounded(
        src, plan, key_plan, page_size, cursor, None, dest, part_base,
    )
}

/// [`read_keyset_page`] with an optional INCLUSIVE upper bound on the key.
#[allow(clippy::too_many_arguments)]
pub(crate) fn read_keyset_page_bounded(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key_plan: &IncrementalCursorPlan,
    page_size: usize,
    cursor: Option<&str>,
    upper: Option<&str>,
    dest: &dyn destination::Destination,
    part_base: &str,
) -> PageRead {
    let mut sink = match fill_keyset_page(src, plan, key_plan, page_size, cursor, upper) {
        Ok(Some(sink)) => sink,
        Ok(None) => return (Vec::new(), Ok(None)),
        Err(e) => return (Vec::new(), Err(e)),
    };
    let rows = sink.total_rows;
    let observed = sink.take_observations();
    let (parts, wrote) = super::commit::write_sink_parts(
        dest,
        &mut sink,
        plan.validate.then_some(plan.format),
        |idx, count| super::commit::part_indexed_name(part_base, idx, count),
    );
    let page = wrote.map(|()| {
        Some(KeysetPage {
            rows,
            observed,
            next_cursor: sink.effective_cursor(),
            first_cursor: sink.first_cursor_value.clone(),
            checksums: sink.take_checksums(),
        })
    });
    (parts, page)
}

/// Run one seek page's query into a fresh sink; `None` when the page is empty.
fn fill_keyset_page(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key_plan: &IncrementalCursorPlan,
    page_size: usize,
    cursor: Option<&str>,
    upper: Option<&str>,
) -> Result<Option<ExportSink>> {
    let cursor_state = cursor.map(|v| CursorState {
        export_name: plan.export_name.clone(),
        last_cursor_value: Some(v.to_string()),
        last_run_at: None,
        cursor_column: None,
    });
    let (key_rest, key_range) = match &plan.strategy {
        ExtractionStrategy::Keyset(k) => (k.key_rest.as_slice(), fans_out(k.parallel)),
        _ => (&[][..], false),
    };
    let mut sink = ExportSink::new(plan)?;
    src.export(
        &source::ExportRequest::unwrapped(&plan.base_query, &plan.tuning, &plan.column_overrides)
            .with_catalog_hint_query(plan.catalog_query.as_deref())
            .with_incremental(Some(key_plan))
            .with_cursor(cursor_state.as_ref())
            .with_upper_bound(upper)
            .with_key_rest(key_rest)
            .with_key_range(key_range)
            .with_page_limit(page_size),
        &mut sink,
    )?;
    sink.finish_writer()?;
    if sink.total_rows == 0 {
        return Ok(None);
    }
    Ok(Some(sink))
}

/// The run's `cursor_high` (forensics v18): the max key of the HIGHEST-index range that produced data.
fn highest_range_max(range_maxes: Vec<Option<String>>) -> Option<String> {
    range_maxes.into_iter().rev().flatten().next()
}

/// The per-range high-water keys a run starts from.
fn seeded_range_max(total: usize, committed: Vec<(usize, Option<String>)>) -> Vec<Option<String>> {
    let mut max = vec![None; total];
    for (idx, key) in committed {
        if let Some(slot) = max.get_mut(idx) {
            *slot = seeded!(
                "resumed-run-cursor-below-its-maximum",
                key.filter(|_| false),
                key
            );
        }
    }
    max
}

/// The page's high-water key rides only on its LAST part (v25): the point the whole page is committed.
fn page_part_cursor(pi: usize, n_parts: usize, high_water: &Option<String>) -> Option<String> {
    (pi + 1 == n_parts).then(|| high_water.clone()).flatten()
}

/// One sampler read, cancelled by the run's `tuning.statement_timeout_s` (0 = no limit).
fn sampler_scalar(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    sql: &str,
) -> Result<Option<String>> {
    seeded!(
        "keyset-sampler-unbounded",
        src.query_scalar(sql),
        src.query_scalar_within(sql, plan.tuning.statement_timeout_s)
    )
}

/// "Only the first row" clause (after the `ORDER BY`), per dialect.
fn first_row_clause(st: crate::config::SourceType) -> &'static str {
    use crate::config::SourceType::*;
    match st {
        Postgres | Mysql => "LIMIT 1",
        Mssql | Oracle => "OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY",
        Mongo => unreachable!("parallel keyset sampling is a SQL path; Mongo uses $sample"),
    }
}

/// "The first `n` rows" clause (after the `ORDER BY`), per dialect.
fn first_rows_clause(st: crate::config::SourceType, n: i64) -> String {
    use crate::config::SourceType::*;
    match st {
        Postgres | Mysql => format!("LIMIT {n}"),
        Mssql | Oracle => format!("OFFSET 0 ROWS FETCH NEXT {n} ROWS ONLY"),
        Mongo => unreachable!("parallel keyset sampling is a SQL path; Mongo uses $sample"),
    }
}

/// The 0-indexed ROW offset of the i-th of `parts` percentile boundaries over `total` rows (i in 1..parts).
fn percentile_offset(total: i64, i: usize, parts: usize) -> i64 {
    total * i as i64 / parts as i64
}

/// "The single row at offset `off`" clause (after the `ORDER BY`), per dialect.
fn nth_row_clause(st: crate::config::SourceType, off: i64) -> String {
    use crate::config::SourceType::*;
    match st {
        Postgres | Mysql => format!("LIMIT 1 OFFSET {off}"),
        Mssql | Oracle => format!("OFFSET {off} ROWS FETCH NEXT 1 ROWS ONLY"),
        Mongo => unreachable!("parallel keyset sampling is a SQL path; Mongo uses $sample"),
    }
}

/// N−1 ROW-percentile boundaries of the keyset key: the key values at row offsets `total*i/N`, by one count and N−1 offset reads.
fn percentile_boundaries(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key: &str,
    parts: usize,
    floor: Option<&str>,
    ceil: Option<&str>,
) -> Result<Vec<String>> {
    let st = plan.source.source_type;
    let base = &plan.base_query;
    let k = crate::sql::quote_ident(st, key);
    let mut preds: Vec<String> = Vec::new();
    let of = crate::source::query::KeyOf { base, expr: &k };
    if let Some(lo) = floor {
        preds.push(format!(
            "{k} > {}",
            crate::source::query::inline_literal(st, lo, of)
        ));
    }
    if let Some(hi) = ceil {
        preds.push(format!(
            "{k} <= {}",
            crate::source::query::inline_literal(st, hi, of)
        ));
    }
    let where_clause = if preds.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", preds.join(" AND "))
    };
    let total: i64 = sampler_scalar(
        src,
        plan,
        &format!(
            "SELECT COUNT(*) FROM ({base}) {} {where_clause}",
            crate::sql::derived(st, "_rivet_pk_cnt")
        ),
    )?
    .as_deref()
    .and_then(|s| s.trim().parse::<i64>().ok())
    .unwrap_or(0);
    if total <= 1 {
        return Ok(vec![]);
    }
    let mut bounds: Vec<String> = Vec::with_capacity(parts.saturating_sub(1));
    for i in 1..parts {
        let off = percentile_offset(total, i, parts);
        let nth = nth_row_clause(st, off);
        let sql = format!(
            "SELECT {k} FROM ({base}) {} {where_clause} ORDER BY {k} {nth}",
            crate::sql::derived(st, "_rivet_pk")
        );
        if let Some(v) = sampler_scalar(src, plan, &sql)?
            && bounds.last().map(String::as_str) != Some(v.as_str())
        {
            bounds.push(v);
        }
    }
    Ok(bounds)
}

/// Below this many rows by the catalog's estimate a key with no axis (a string, a SQL Server uniqueidentifier) samples exact row percentiles; above it, or with no estimate, it seeks.
const ROW_PERCENTILE_ROWS: i64 = 10_000_000;

/// Key-space ranges per worker for an integer key, so a dense stretch of keys is spread over more than one range.
const RANGES_PER_WORKER: usize = 16;

/// The most key-space ranges one parallel keyset run cuts.
const MAX_RANGES: usize = 256;

/// Rows one density probe skips, so one probe examines at most this many index entries.
const PROBE_ROWS: i64 = 200;

/// Density probes taken across the key-space ranges to decide whether they are skewed.
const PROBE_POINTS: usize = 32;

/// A sampled density ratio above this means the key-space ranges are skewed.
const DENSITY_RATIO: f64 = 8.0;

/// A recut is balanced when its heaviest range holds at most this multiple of the mean.
const BALANCE_FACTOR: f64 = 2.0;

/// Most recut rounds one parallel run spends before it keeps the cuts it has.
const RECUT_ROUNDS: usize = 8;

/// One integer key range (start, end] and its probed density in rows per key.
#[derive(Clone, Copy)]
struct Span {
    start: i128,
    end: i128,
    density: f64,
}

impl Span {
    /// Estimated rows in the range: its density times its key width.
    fn rows(&self) -> f64 {
        self.density * (self.end - self.start).max(1) as f64
    }
}

/// The span cut where its density changes: the right half is probed, and the halves are split again while it differs from the left by more than [`DENSITY_RATIO`], so a dense stretch before an empty one is not spread across it.
fn refine_span(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key: (&str, Axis),
    fence: &[String],
    span: Span,
    out: &mut Vec<Span>,
) -> Result<()> {
    let mid = span.start + (span.end - span.start) / 2;
    if mid <= span.start || span.rows() < PROBE_ROWS as f64 {
        out.push(span);
        return Ok(());
    }
    let right = probe_density(src, plan, key, fence, mid, span.end)?;
    let (hi, lo) = (right.max(span.density), right.min(span.density));
    if hi / lo <= seeded!("keyset-refine-span-never-splits", f64::MAX, DENSITY_RATIO) {
        out.push(span);
        return Ok(());
    }
    let left = Span { end: mid, ..span };
    let right = Span {
        start: mid,
        density: right,
        ..span
    };
    refine_span(src, plan, key, fence, left, out)?;
    refine_span(src, plan, key, fence, right, out)
}

/// How the density check left the parallel key-space cuts.
enum Recut {
    Even,
    Held { ratio: f64 },
    Rounds(usize),
    Unbalanced { rounds: usize, ratio: f64 },
}

/// The catalog's scan-free row estimate of a `table:` export's table; `None` for a query, an unanalyzed table or a failed read.
fn catalog_rows(src: &mut dyn Source, plan: &ResolvedRunPlan) -> Option<i64> {
    let st = plan.source.source_type;
    let table = crate::sql::strip_simple_projection_from(&plan.base_query)?;
    let sql = match st {
        crate::config::SourceType::Mysql => {
            let (schema, name) = table
                .split_once('.')
                .map_or(("DATABASE()".to_string(), table), |(s, t)| {
                    (format!("'{s}'"), t)
                });
            format!(
                "SELECT TABLE_ROWS FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = '{name}'"
            )
        }
        _ => crate::sql::row_estimate_sql(st, table)?,
    };
    sampler_scalar(src, plan, &sql)
        .ok()
        .flatten()?
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|n| *n > 0)
}

/// Up to N−1 range boundaries of the keyset key: key-space cuts (`int_parts` for an integer key) snapped to existing keys by index seeks and recut by density; a key with no axis under [`ROW_PERCENTILE_ROWS`] catalog rows samples row percentiles.
pub(crate) fn sample_key_boundaries(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key: &str,
    parts: usize,
    int_parts: usize,
    floor: Option<&str>,
    ceil: Option<&str>,
) -> Result<Vec<String>> {
    let st = plan.source.source_type;
    let base = &plan.base_query;
    let k = crate::sql::quote_ident(st, key);
    let of = crate::source::query::KeyOf { base, expr: &k };
    let lit = |v: &str| crate::source::query::inline_literal(st, v, of);
    let fence: Vec<String> = [
        floor.map(|v| format!("{k} > {}", lit(v))),
        ceil.map(|v| format!("{k} <= {}", lit(v))),
    ]
    .into_iter()
    .flatten()
    .collect();
    seeded!(
        "keyset-sampler-scans-the-table",
        sampler_scalar(
            src,
            plan,
            &format!(
                "SELECT COUNT(*) FROM ({base}) {}",
                crate::sql::derived(st, "_rivet_pk_cnt")
            )
        )
        .map(drop)?,
        ()
    );
    let mut bounds: Vec<String> = Vec::with_capacity(parts.saturating_sub(1));
    let outcome = seek_bounds(src, plan, &k, &fence, (parts, int_parts), &mut bounds);
    let ends = match outcome {
        Ok(SeekOutcome::Ends(ends)) => ends,
        Ok(SeekOutcome::Percentile) => {
            log::info!(
                "export '{}': parallel keyset path: percentile (key has no axis, catalog estimate under {ROW_PERCENTILE_ROWS} rows)",
                plan.export_name
            );
            return percentile_boundaries(src, plan, key, parts, floor, ceil);
        }
        Err(e) => {
            log::warn!(
                "export '{}': parallel keyset cannot seek the key to a split point ({e:#}); \
                 the keys above {} stay in one range: data is complete, with fewer workers.",
                plan.export_name,
                bounds
                    .last()
                    .map_or("its first value".to_string(), |b| format!("{b:?}"))
            );
            return Ok(bounds);
        }
    };
    let Some((axis, lo, hi)) = ends.and_then(|(a, b)| {
        let axis = Axis::of(&a, st)?;
        if seeded!(
            "keyset-uuid-not-recut",
            matches!(axis, Axis::Uuid { .. }),
            false
        ) {
            return None;
        }
        Some((axis, axis.ord(&a)?, axis.ord(&b)?))
    }) else {
        log::info!(
            "export '{}': parallel keyset path: seek (no integer key range to balance)",
            plan.export_name
        );
        return Ok(bounds);
    };
    let seek_cut = bounds.clone();
    match balance_by_density(src, plan, (&k, axis), &fence, (lo, hi), bounds) {
        Ok((bounds, Recut::Even)) => {
            log::info!(
                "export '{}': parallel keyset path: seek (key-space cuts, densities within {DENSITY_RATIO}x)",
                plan.export_name
            );
            Ok(bounds)
        }
        Ok((bounds, Recut::Held { ratio })) => {
            log::info!(
                "export '{}': parallel keyset path: seek (key-space cuts; probed densities differ over {DENSITY_RATIO}x, estimated heaviest range {ratio:.1}x the mean)",
                plan.export_name
            );
            Ok(bounds)
        }
        Ok((bounds, Recut::Rounds(rounds))) => {
            log::info!(
                "export '{}': parallel keyset path: seek+recut ({rounds} round(s), {} range(s))",
                plan.export_name,
                bounds.len() + 1
            );
            Ok(bounds)
        }
        Ok((bounds, Recut::Unbalanced { rounds, ratio })) => {
            log::warn!(
                "export '{}': parallel keyset path: seek+recut ({rounds} round(s), not balanced: estimated heaviest range {ratio:.1}x the mean); data is complete, with uneven workers.",
                plan.export_name
            );
            Ok(bounds)
        }
        Err(e) => {
            log::warn!(
                "export '{}': parallel keyset path: seek (density probe failed: {e:#}); the key-space cuts stand.",
                plan.export_name
            );
            Ok(seek_cut)
        }
    }
}

/// The ordered key select over the window past `start` up to `end`, before its row clause.
fn window_select(
    plan: &ResolvedRunPlan,
    (k, axis): (&str, Axis),
    fence: &[String],
    (start, end): (i128, i128),
) -> String {
    let st = plan.source.source_type;
    let of = crate::source::query::KeyOf {
        base: &plan.base_query,
        expr: k,
    };
    let (past, past_at) = axis.after(start);
    let (upto, upto_at) = axis.upto(end);
    let mut preds = fence.to_vec();
    preds.push(format!(
        "{k} {past} {}",
        crate::source::query::inline_literal(st, &past_at, of)
    ));
    preds.push(format!(
        "{k} {upto} {}",
        crate::source::query::inline_literal(st, &upto_at, of)
    ));
    preds.push(format!("{k} IS NOT NULL"));
    format!(
        "SELECT {k} FROM ({}) {} WHERE {} ORDER BY {k}",
        plan.base_query,
        crate::sql::derived(st, "_rivet_pk"),
        preds.join(" AND ")
    )
}

/// The key at row `offset` of the window past `start` up to `end`, by one index seek.
fn probe_at(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key: (&str, Axis),
    fence: &[String],
    window: (i128, i128),
    offset: i64,
) -> Result<Option<String>> {
    let st = plan.source.source_type;
    sampler_scalar(
        src,
        plan,
        &format!(
            "{} {}",
            window_select(plan, key, fence, window),
            nth_row_clause(st, offset)
        ),
    )
}

/// The rows of the window past `start` up to `end`, counted over at most [`PROBE_ROWS`] index entries.
fn window_rows(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key: (&str, Axis),
    fence: &[String],
    window: (i128, i128),
) -> Result<i64> {
    let st = plan.source.source_type;
    let sql = format!(
        "SELECT COUNT(*) FROM ({} {}) {}",
        window_select(plan, key, fence, window),
        first_rows_clause(st, PROBE_ROWS),
        crate::sql::derived(st, "_rivet_n")
    );
    Ok(sampler_scalar(src, plan, &sql)?
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0))
}

/// Rows per key just above `start` up to `end`, from the key one seek lands on after skipping [`PROBE_ROWS`] rows, or from the exact count of the fewer rows the window holds.
fn probe_density(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key: (&str, Axis),
    fence: &[String],
    start: i128,
    end: i128,
) -> Result<f64> {
    let landed = probe_at(src, plan, key, fence, (start, end), PROBE_ROWS)?;
    Ok(match landed.and_then(|v| key.1.ord(&v)) {
        Some(at) => PROBE_ROWS as f64 / (at - start).max(1) as f64,
        None => {
            window_rows(src, plan, key, fence, (start, end))? as f64 / (end - start).max(1) as f64
        }
    })
}

/// The integer key ranges (start, end] that the cuts make, the first starting just below `lo`, densities unset.
fn spans_from(lo: i128, hi: i128, cuts: &[i128]) -> Vec<Span> {
    std::iter::once(lo - 1)
        .chain(cuts.iter().copied())
        .zip(cuts.iter().copied().chain(std::iter::once(hi)))
        .map(|(start, end)| Span {
            start,
            end,
            density: 0.0,
        })
        .collect()
}

/// The integer cuts that split the spans' estimated rows into `parts` equal shares, interpolated inside each span.
fn equal_row_cuts(spans: &[Span], parts: usize) -> Vec<i128> {
    let share = spans.iter().map(Span::rows).sum::<f64>() / parts as f64;
    let mut cuts: Vec<i128> = Vec::with_capacity(parts.saturating_sub(1));
    let mut next = share;
    for _ in 1..parts {
        let mut seen = 0.0;
        for s in spans {
            if next <= seen + s.rows() {
                let at = s.start + ((next - seen) / s.density).round() as i128;
                if at > s.start && at < s.end && cuts.last().is_none_or(|c| *c < at) {
                    cuts.push(at);
                }
                break;
            }
            seen += s.rows();
        }
        next += share;
    }
    cuts
}

/// The first existing key above `start` at or past `mid`, by one index seek.
fn snap_key(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    (k, axis): (&str, Axis),
    fence: &[String],
    start: i128,
    mid: i128,
) -> Result<Option<i128>> {
    let st = plan.source.source_type;
    let of = crate::source::query::KeyOf {
        base: &plan.base_query,
        expr: k,
    };
    let (past, past_at) = axis.after(start);
    let mut preds = fence.to_vec();
    preds.push(format!(
        "{k} {past} {}",
        crate::source::query::inline_literal(st, &past_at, of)
    ));
    preds.push(format!(
        "{k} >= {}",
        crate::source::query::inline_literal(st, &axis.show(mid), of)
    ));
    Ok(seek_key(src, plan, k, &preds, false)?.and_then(|v| axis.ord(&v)))
}

/// The last existing key above `start` and below `mid`, by one index seek.
fn prior_key(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    (k, axis): (&str, Axis),
    fence: &[String],
    start: i128,
    mid: i128,
) -> Result<Option<i128>> {
    let st = plan.source.source_type;
    let of = crate::source::query::KeyOf {
        base: &plan.base_query,
        expr: k,
    };
    let (past, past_at) = axis.after(start);
    let (below, below_at) = axis.below(mid);
    let mut preds = fence.to_vec();
    preds.push(format!(
        "{k} {past} {}",
        crate::source::query::inline_literal(st, &past_at, of)
    ));
    preds.push(format!(
        "{k} {below} {}",
        crate::source::query::inline_literal(st, &below_at, of)
    ));
    Ok(seek_key(src, plan, k, &preds, true)?.and_then(|v| axis.ord(&v)))
}

/// Each cut snapped to the existing key nearest it, forward or back across an empty stretch, past the previous one and skipping a cut the previous one already passed, stopping at the first that has none below `hi`; with the cuts that moved back to the edge of a dense stretch.
fn snap_cuts(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key: (&str, Axis),
    fence: &[String],
    first: i128,
    hi: i128,
    cuts: &[i128],
) -> Result<(Vec<i128>, Vec<i128>)> {
    let mut out = Vec::with_capacity(cuts.len());
    let mut edges = Vec::new();
    let mut prev = first;
    for &c in cuts {
        if seeded!("keyset-snap-stacks-passed-cuts", false, c <= prev) {
            continue;
        }
        let Some(mut b) = snap_key(src, plan, key, fence, prev, c)? else {
            break;
        };
        let mut edge = false;
        if b > c
            && let Some(p) = prior_key(src, plan, key, fence, prev, c)?
            && c - p < b - c
        {
            (b, edge) = (p, true);
        }
        if b > prev && b < hi {
            out.push(b);
            if edge {
                edges.push(b);
            }
            prev = b;
        } else {
            break;
        }
    }
    Ok((out, edges))
}

/// Recut the integer key-space cuts into equal-row ranges when the sampled densities are skewed; the cuts stand otherwise.
fn balance_by_density(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key: (&str, Axis),
    fence: &[String],
    (lo, hi): (i128, i128),
    bounds: Vec<String>,
) -> Result<(Vec<String>, Recut)> {
    let mut cuts: Vec<i128> = bounds.iter().filter_map(|b| key.1.ord(b)).collect();
    if cuts.len() != bounds.len() || cuts.is_empty() {
        return Ok((bounds, Recut::Even));
    }
    let m = cuts.len() + 1;
    let render = |cuts: &[i128]| -> Vec<String> { cuts.iter().map(|c| key.1.show(*c)).collect() };
    let mut spans = spans_from(lo, hi, &cuts);
    let points = m.min(PROBE_POINTS);
    let mut sampled = Vec::with_capacity(points);
    for j in 0..points {
        let s = spans[j * m / points];
        sampled.push(probe_density(src, plan, key, fence, s.start, s.end)?);
    }
    let (min, max) = sampled
        .iter()
        .fold((f64::MAX, 0f64), |(a, b), d| (a.min(*d), b.max(*d)));
    if max == 0.0 || max / min <= DENSITY_RATIO {
        return Ok((bounds, Recut::Even));
    }
    let mut known = (points == m).then_some(sampled);
    let mut rounds = 0;
    let mut best: Option<(f64, Vec<i128>, usize)> = None;
    let mut worse = 0;
    loop {
        match known.take() {
            Some(densities) => spans
                .iter_mut()
                .zip(densities)
                .for_each(|(s, d)| s.density = d),
            None => {
                spans = spans_from(lo, hi, &cuts);
                for s in spans.iter_mut() {
                    s.density = probe_density(src, plan, key, fence, s.start, s.end)?;
                }
            }
        }
        let heavy = spans.iter().map(Span::rows).sum::<f64>() / m as f64 * BALANCE_FACTOR;
        let mut refined = Vec::with_capacity(spans.len());
        for s in spans.drain(..) {
            if s.rows() > heavy {
                refine_span(src, plan, key, fence, s, &mut refined)?;
            } else {
                refined.push(s);
            }
        }
        spans = refined;
        let mean = spans.iter().map(Span::rows).sum::<f64>() / m as f64;
        let ratio = spans.iter().map(Span::rows).fold(0f64, f64::max) / mean;
        if ratio <= BALANCE_FACTOR {
            let recut = if rounds == 0 {
                Recut::Held { ratio }
            } else {
                Recut::Rounds(rounds)
            };
            return Ok((render(&cuts), recut));
        }
        if best.as_ref().is_none_or(|(r, ..)| ratio < *r) {
            best = Some((ratio, cuts.clone(), rounds));
            worse = 0;
        } else {
            worse += 1;
        }
        if worse == seeded!("keyset-recut-quits-early", 2, 3) || rounds == RECUT_ROUNDS {
            let (ratio, cuts, rounds) = best.take().unwrap();
            return Ok((render(&cuts), Recut::Unbalanced { rounds, ratio }));
        }
        let (next, _) = snap_cuts(
            src,
            plan,
            key,
            fence,
            lo - 1,
            hi,
            &equal_row_cuts(&spans, m),
        )?;
        if next == cuts {
            return Ok((render(&cuts), Recut::Unbalanced { rounds, ratio }));
        }
        cuts = next;
        rounds += 1;
    }
}

/// Push onto `bounds` the key-space split points of the fenced MIN..MAX, each snapped to the next existing key, and return the fenced MIN and MAX; an error leaves the bounds found so far.
fn seek_bounds(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    k: &str,
    fence: &[String],
    (parts, int_parts): (usize, usize),
    bounds: &mut Vec<String>,
) -> Result<SeekOutcome> {
    let st = plan.source.source_type;
    let of = crate::source::query::KeyOf {
        base: &plan.base_query,
        expr: k,
    };
    let (Some(lo), Some(hi)) = (
        seek_key(src, plan, k, fence, false)?,
        seek_key(src, plan, k, fence, true)?,
    ) else {
        return Ok(SeekOutcome::Ends(None));
    };
    let on_axis = Axis::of(&lo, st).is_some() && Axis::of(&hi, st).is_some();
    if !on_axis && catalog_rows(src, plan).is_some_and(|n| n < ROW_PERCENTILE_ROWS) {
        return Ok(SeekOutcome::Percentile);
    }
    let parts = if on_axis { int_parts } else { parts };
    for cut in super::key_space::split(&lo, &hi, parts) {
        let after = bounds.last().unwrap_or(&lo);
        let mut preds = fence.to_vec();
        preds.push(format!(
            "{k} >= {}",
            crate::source::query::inline_literal(st, &cut, of)
        ));
        preds.push(format!(
            "{k} > {}",
            crate::source::query::inline_literal(st, after, of)
        ));
        match seek_key(src, plan, k, &preds, false)? {
            Some(b) if b != hi => bounds.push(b),
            _ => break,
        }
    }
    Ok(SeekOutcome::Ends(Some((lo, hi))))
}

/// What the endpoint seeks found: the lowest and highest key (none on an empty table), or a key with no axis small enough for row percentiles.
enum SeekOutcome {
    Ends(Option<(String, String)>),
    Percentile,
}

/// The first non-NULL key of the base query under `preds` in key order (the last with `desc`), by one index seek.
fn seek_key(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    k: &str,
    preds: &[String],
    desc: bool,
) -> Result<Option<String>> {
    let st = plan.source.source_type;
    let preds = [preds, &[format!("{k} IS NOT NULL")]].concat();
    sampler_scalar(
        src,
        plan,
        &format!(
            "SELECT {k} FROM ({}) {} WHERE {} ORDER BY {k}{} {}",
            plan.base_query,
            crate::sql::derived(st, "_rivet_pk"),
            preds.join(" AND "),
            if desc { " DESC" } else { "" },
            first_row_clause(st)
        ),
    )
}

/// Sample the N key ranges for a FRESH parallel keyset run.
#[allow(clippy::type_complexity)]
fn sample_parallel_ranges(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key: &str,
    parallel: usize,
    floor: Option<&str>,
    ceil: Option<&str>,
) -> Result<Vec<(usize, Option<String>, Option<String>, bool)>> {
    let ranges = (parallel * RANGES_PER_WORKER).clamp(parallel, MAX_RANGES.max(parallel));
    let bounds = sample_key_boundaries(src, plan, key, parallel, ranges, floor, ceil)?;
    Ok(partition_ranges(&bounds, floor, ceil))
}

/// Pure partitioning half of [`sample_parallel_ranges`] (#161).
#[allow(clippy::type_complexity)]
fn partition_ranges(
    bounds: &[String],
    floor: Option<&str>,
    ceil: Option<&str>,
) -> Vec<(usize, Option<String>, Option<String>, bool)> {
    let mut ranges = Vec::with_capacity(bounds.len() + 1);
    let mut prev: Option<String> = floor.map(str::to_string);
    for (i, b) in bounds.iter().enumerate() {
        ranges.push((i, prev.clone(), Some(b.clone()), false));
        prev = Some(b.clone());
    }
    let last = ranges.len();
    ranges.push((last, prev, ceil.map(str::to_string), false));
    ranges
}

/// Sampled ranges as the worker seeks them: a composite key's lead-column bounds become one-value key tuples.
#[allow(clippy::type_complexity)]
fn lead_bounds(
    ranges: Vec<(usize, Option<String>, Option<String>, bool)>,
    key_rest: &[String],
) -> Vec<(usize, Option<String>, Option<String>, bool)> {
    if key_rest.is_empty() {
        return ranges;
    }
    let tuple = |v: Option<String>| v.map(|v| serde_json::json!([v]).to_string());
    ranges
        .into_iter()
        .map(|(i, lo, hi, done)| (i, tuple(lo), tuple(hi), done))
        .collect()
}

/// What a resumed full pass reads past a last, open-ended range the crashed run committed.
#[derive(Debug, PartialEq, Eq)]
enum UnreadTail {
    /// The keys above the highest key that range delivered.
    Past(String),
    /// The range again: it recorded no highest key (it was empty, or a rivet that did not record one committed it).
    Again,
}

/// The tail a resumed run still owes: `None` unless its last range is open-ended and committed.
fn unread_tail(ranges: &[crate::state::KeysetRangeRow]) -> Option<UnreadTail> {
    let last = ranges.last().filter(|r| {
        seeded!(
            "resumed-parallel-keyset-stops-at-its-last-range",
            !r.done,
            r.done
        ) && r.hi.is_none()
    })?;
    Some(match &last.max_key {
        Some(key) => UnreadTail::Past(key.clone()),
        None => UnreadTail::Again,
    })
}

/// The range a parallel-keyset part belongs to, read from the `_pk_w<range>_<page>` its name ends with.
fn range_of_part(file_name: &str) -> Option<i64> {
    let stem = file_name.rsplit_once('.').map_or(file_name, |(s, _)| s);
    let stem = match stem.rsplit_once("_p") {
        Some((head, n)) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => head,
        _ => stem,
    };
    let (head, page) = stem.rsplit_once('_')?;
    let (_, range) = head.rsplit_once("_pk_w")?;
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    (digits(page) && digits(range))
        .then(|| range.parse().ok())
        .flatten()
}

/// Plan what a resumed full pass still owes past its committed last range, so rows that arrived above it since the crash are read.
fn plan_unread_tail(
    st: &StateStore,
    export_name: &str,
    scope: &str,
    run_id: &str,
    key: &str,
    ranges: &mut Vec<crate::state::KeysetRangeRow>,
) -> Result<()> {
    let Some(tail) = unread_tail(ranges) else {
        return Ok(());
    };
    let last = ranges.len() - 1;
    match tail {
        UnreadTail::Past(top) => {
            let range_index = ranges.len() as i64;
            st.append_keyset_range(export_name, scope, run_id, key, range_index, &top)?;
            ranges.push(crate::state::KeysetRangeRow {
                range_index,
                lo: Some(top),
                hi: None,
                done: false,
                max_key: None,
            });
        }
        UnreadTail::Again => {
            let index = ranges[last].range_index;
            let parts: Vec<String> = st
                .list_files_for_run(run_id)?
                .into_iter()
                .map(|f| f.file_name)
                .filter(|name| range_of_part(name) == Some(index))
                .collect();
            st.forget_parts(run_id, &parts)?;
            st.reopen_keyset_range(export_name, run_id, index)?;
            ranges[last].done = false;
        }
    }
    log::info!(
        "export '{export_name}': the last key range of interrupted run {run_id} was committed \
         open-ended — reading the keys the source holds past it now"
    );
    Ok(())
}

/// Parallel keyset (feat/parallel-keyset).
fn seek_tag(seek: Option<&str>) -> String {
    match seek {
        None => "start".to_string(),
        Some(v) => {
            let mut h: u64 = 0xcbf29ce484222325;
            for b in v.as_bytes() {
                h ^= u64::from(*b);
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
            format!("{h:016x}")
        }
    }
}

fn run_keyset_parallel(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    key_plan: IncrementalCursorPlan,
    parallel: usize,
    state: Option<&StateStore>,
    progress: &crate::state::ProgressClaim<'_>,
) -> Result<()> {
    use std::sync::Mutex;

    let kp = keyset_plan(plan);
    let scope = plan.source.state_key();
    let key = kp.cursor_identity();
    let page_size = kp.chunk_size;
    let checkpoint = kp.checkpoint;
    let sample = |src: &mut dyn Source, floor, ceil| {
        sample_parallel_ranges(src, plan, &kp.key_column, parallel, floor, ceil)
            .map(|ranges| lead_bounds(ranges, &kp.key_rest))
    };

    let resume_run_id: Option<String> = if checkpoint {
        progress.resume_run_id().ok().flatten()
    } else {
        None
    };
    let resume_run_id = resume_anchor(
        state,
        &plan.export_name,
        &scope,
        resume_run_id,
        true,
        &key,
        kp.incremental,
    )?;

    let incremental = kp.incremental;
    let (floor, ceil): (Option<String>, Option<String>) = if incremental && resume_run_id.is_none()
    {
        let anchor = progress.cursor()?.last_cursor_value;
        let st = plan.source.source_type;
        let key_q = crate::sql::quote_ident(st, &kp.key_column);
        let of = crate::source::query::KeyOf {
            base: &plan.base_query,
            expr: &key_q,
        };
        let past_anchor: Vec<String> = anchor
            .as_deref()
            .map(|a| {
                format!(
                    "{key_q} > {}",
                    crate::source::query::inline_literal(st, a, of)
                )
            })
            .into_iter()
            .collect();
        let cur_max = seek_key(src, plan, &key_q, &past_anchor, true)?;
        if cur_max.is_none() {
            log::info!(
                "export '{}': parallel keyset incremental — no new rows past the anchor, nothing to export",
                plan.export_name
            );
            return Ok(());
        }
        (anchor, cur_max)
    } else {
        (None, None)
    };
    let (floor_r, ceil_r) = (floor.as_deref(), ceil.as_deref());

    let mut committed_max: Vec<(usize, Option<String>)> = Vec::new();
    let ranges: Vec<(usize, Option<String>, Option<String>, bool)> = match (&resume_run_id, state) {
        (Some(rid), Some(st)) => {
            summary.run_id = rid.clone();
            summary.resumed = true;
            let mut stored = st.load_keyset_ranges(&plan.export_name, rid, &key)?;
            plan_unread_tail(st, &plan.export_name, &scope, rid, &key, &mut stored)?;
            stored
                .into_iter()
                .map(|r| {
                    committed_max.push((r.range_index as usize, r.max_key));
                    (r.range_index as usize, r.lo, r.hi, r.done)
                })
                .collect()
        }
        (None, Some(st)) if checkpoint => {
            let fresh = sample(src, floor_r, ceil_r)?;
            st.persist_keyset_ranges(
                &plan.export_name,
                &scope,
                &summary.run_id,
                &key,
                &lo_hi_pairs(&fresh),
            )?;
            st.set_resume_run_id(progress.key(), &summary.run_id)?;
            fresh
        }
        _ => sample(src, floor_r, ceil_r)?,
    };

    let anchor_ceiling: Option<String> = ranges.last().and_then(|(_, _, hi, _)| hi.clone());
    let anchor_floor: Option<String> = ranges.first().and_then(|(_, lo, _, _)| lo.clone());

    let total_ranges = ranges.len();
    let pending: Vec<(usize, Option<String>, Option<String>)> = ranges
        .into_iter()
        .filter(|(_, _, _, done)| !done)
        .map(|(idx, lo, hi, _)| (idx, lo, hi))
        .collect();

    if fan_out_collapsed(parallel, total_ranges) {
        log::warn!(
            "export '{}': parallel keyset requested {} workers but sampled 0 boundaries — \
             running as a SINGLE worker. The key may hold one value, be skewed, or be a type the \
             boundary probe cannot read; data is complete but the parallel speed-up is absent.",
            plan.export_name,
            parallel
        );
    }

    log::info!(
        "export '{}': parallel keyset — {} range(s), {} to run{}, page size {}",
        plan.export_name,
        total_ranges,
        pending.len(),
        if resume_run_id.is_some() {
            " (resume)"
        } else {
            ""
        },
        page_size
    );

    let (dest, ext) = super::frame::RunnerFrame::open_shared(plan)?;
    let run_tag = super::summary::run_scoped_tag(&summary.run_id, &plan.export_name);
    let run_id = summary.run_id.clone();
    let state_ref = if checkpoint {
        state.map(|s| s.state_ref().clone())
    } else {
        None
    };
    let fmt_label = plan.format.label();
    let cmp_label = plan.compression.label();

    let fan = super::fan_in::FanIn::default();
    let range_max: Mutex<Vec<Option<String>>> =
        Mutex::new(seeded_range_max(total_ranges, committed_max));
    let range_first: Mutex<Option<String>> = Mutex::new(None);

    let semaphore = crate::resource::Semaphore::new(parallel.max(1));
    let slots = crate::resource::Semaphore::new(parallel.max(1));
    let governor = crate::pipeline::governor::GovernorHarness::arm(plan, parallel);

    std::thread::scope(|scope| {
        governor.spawn_into(
            scope,
            &semaphore,
            fan.finished(),
            pending.len(),
            &plan.export_name,
        );

        for (ridx, lo, hi) in pending.iter().cloned() {
            let dest = std::sync::Arc::clone(&dest);
            let (plan_r, key_plan_r, ext_r, tag_r, key_r) =
                (plan, &key_plan, &ext, run_tag.as_str(), key.as_str());
            let (fan_r, rfirst_r, rmax_r) = (&fan, &range_first, &range_max);
            let (sref_r, rid_r, fmt_r, cmp_r) = (&state_ref, run_id.as_str(), fmt_label, cmp_label);
            let (sem_r, slots_r) = (&semaphore, &slots);
            let unit = super::commit::UnitId::Range(ridx as i64);
            fan.spawn(scope, format!("range {ridx}"), move || {
                let _slot = crate::pipeline::governor::TaskPermit::acquire(slots_r);
                let mut wsrc = source::create_source(&plan_r.source)
                    .map_err(|e| super::single::attach_connect_hint(e, &plan_r.source))?;
                let mut cursor = lo;
                let mut pages = 0usize;
                let mut rmax: Option<String> = None;
                let mut range_parts: Vec<crate::state::KeysetRangePart> = Vec::new();
                let mut local_checks: Vec<super::commit::UnitChecksums> = Vec::new();
                loop {
                    let _permit = crate::pipeline::governor::TaskPermit::acquire(sem_r);
                    crate::test_hook::maybe_error_at_index("keyset_parallel_worker", ridx as i64)?;
                    if pages > 0 {
                        crate::test_hook::maybe_error_at_index(
                            "keyset_parallel_worker_midrange",
                            ridx as i64,
                        )?;
                    }
                    let base = format!(
                        "{}_{}_pk_w{}_{}.{}",
                        plan_r.export_name, tag_r, ridx, pages, ext_r
                    );
                    let (parts, page) = read_keyset_page_bounded(
                        &mut *wsrc,
                        plan_r,
                        key_plan_r,
                        page_size,
                        cursor.as_deref(),
                        hi.as_deref(),
                        &**dest,
                        &base,
                    );
                    for p in parts {
                        range_parts.push(crate::state::KeysetRangePart {
                            file_name: p.file_name.clone(),
                            rows: p.rows,
                            bytes: p.bytes as i64,
                        });
                        fan_r.part(unit, p);
                    }
                    let page = page.map_err(|e| {
                        crate::error::recoded(
                            &e,
                            crate::error::codes::RUN_UNITS_FAILED,
                            format!("page {pages}: {e:#}"),
                        )
                    })?;
                    let Some(page) = page else { break };
                    fan_r.observe(page.observed);
                    rmax = page.next_cursor.clone().or(rmax);
                    if ridx == 0 {
                        let mut first = rfirst_r.lock().unwrap_or_else(|e| e.into_inner());
                        if first.is_none() {
                            *first = page.first_cursor.clone();
                        }
                    }
                    local_checks.push(page.checksums);
                    let next = next_seek(
                        cursor.as_deref(),
                        page.rows,
                        page_size,
                        page.next_cursor,
                        key_r,
                        pages,
                        &format!("export '{}'", plan_r.export_name),
                    )?;
                    pages += 1;
                    match next {
                        Some(n) => cursor = Some(n),
                        None => break,
                    }
                }
                if let Some(sref) = sref_r {
                    let st = crate::state::StateStore::open_at_ref(sref).map_err(|e| {
                        crate::error::recoded(
                            &e,
                            crate::error::codes::STATE_NOT_WRITABLE,
                            format!("checkpoint commit: {e:#}"),
                        )
                    })?;
                    st.commit_keyset_range(
                        rid_r,
                        &plan_r.export_name,
                        (ridx as i64, rmax.as_deref()),
                        &range_parts,
                        fmt_r,
                        Some(cmp_r),
                    )
                    .map_err(|e| {
                        crate::error::recoded(
                            &e,
                            crate::error::codes::STATE_NOT_WRITABLE,
                            format!("checkpoint commit: {e:#}"),
                        )
                    })?;
                    if let Err(e) = st.project_running_aggregate(
                        rid_r,
                        &plan_r.export_name,
                        plan_r.strategy.mode_label(),
                        plan_r.format.label(),
                    ) {
                        log::warn!(
                            "export '{}': running-aggregate projection failed for range {ridx} \
                             (checkpoint is durable; metrics row will catch up at finalize): {e:#}",
                            plan_r.export_name
                        );
                    }
                }
                crate::test_hook::maybe_exit_at_index(
                    "keyset_parallel_range_committed",
                    ridx as i64,
                );
                rmax_r.lock().unwrap_or_else(|e| e.into_inner())[ridx] = rmax;
                for c in local_checks {
                    fan_r.contribute(unit, c);
                }
                Ok(())
            });
        }
    });

    let file_log_state = if checkpoint { None } else { state };
    fan.finish(
        plan,
        summary,
        file_log_state,
        Some(governor),
        |idx, _| super::commit::PartKind::Page {
            page_index: idx as i64,
            cursor_high: None,
        },
        |errs| {
            crate::error::coded(
                crate::error::codes::RUN_UNITS_FAILED,
                format!(
                    "export '{}': parallel keyset failed on {} range(s): {}",
                    plan.export_name,
                    errs.len(),
                    errs.join("; ")
                ),
            )
        },
    )?;

    summary.cursor_high =
        highest_range_max(range_max.into_inner().unwrap_or_else(|e| e.into_inner()));
    summary.cursor_low = range_first.into_inner().unwrap_or_else(|e| e.into_inner());

    if let Some(st) = state
        && checkpoint
    {
        rehydrate_keyset_pages_probed(st, &run_id, plan, summary)?;
    }

    log::info!(
        "export '{}': parallel keyset complete — {} range(s), {} parts, {} rows",
        plan.export_name,
        total_ranges,
        summary.manifest_parts.len(),
        summary.total_rows
    );

    if incremental && let Some(hi) = &anchor_ceiling {
        summary.cursor_high = Some(hi.clone());
        summary.cursor_low = anchor_floor.clone();
    }
    Ok(())
}

/// Did the sampler collapse a requested parallel fan-out to a single range?
fn fan_out_collapsed(parallel: usize, total_ranges: usize) -> bool {
    parallel > 1 && total_ranges == 1
}

/// The seek bound after a page: `None` on a short (last) page, else its max key.
fn committed_pages_gone(export: &str, names: &[&str]) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::DEST_COMMITTED_PART_GONE,
        format!(
            "keyset resume: {} committed page part(s) are GONE from the destination \
             ({}) — a gc/cleanup pass deleted them between attempts. Refusing to \
             finalize a Success manifest naming deleted files (their rows would be \
             silently absent). Reset this export's keyset state (`rivet state \
             reset -c <config> --export {export}`) so the whole range is re-read, \
             or point the export at a fresh prefix.",
            names.len(),
            names.join(", "),
        ),
    )
}

pub(crate) fn next_seek(
    prev: Option<&str>,
    rows: usize,
    page_size: usize,
    next: Option<String>,
    key: &str,
    page: usize,
    who: &str,
) -> Result<Option<String>> {
    if rows < page_size {
        return Ok(None);
    }
    let Some(next) = next else {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_PROBE_UNREADABLE,
            "{who}: keyset could not read the '{key}' value from the last row of page {page} \
             (NULL or unsupported type) — cannot advance safely (last readable key: {}). \
             The key must be NOT NULL and one of: integer, float, string, timestamp, date, uuid.",
            prev.unwrap_or("<none>"),
        );
    };
    if prev == Some(next.as_str()) {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_KEYSET_KEY_COARSE,
            "keyset page {page} ended on the same '{key}' value it started after ({next}): the key's \
             rendering is coarser than its values (e.g. a TIMESTAMP(7..9) read at microseconds), so the \
             seek cannot advance. Page on a unique key rivet reads exactly."
        );
    }
    Ok(Some(next))
}

/// Whether a crash anchor was written by this runner shape: only a parallel run persists ranges for it.
fn anchor_is_own(parallel_runner: bool, anchor_has_ranges: bool) -> bool {
    parallel_runner == anchor_has_ranges
}

/// The crash anchor to resume, or `None` (anchor cleared) when another runner shape wrote it.
fn resume_anchor(
    state: Option<&StateStore>,
    export_name: &str,
    scope: &str,
    rid: Option<String>,
    parallel_runner: bool,
    key: &str,
    incremental: bool,
) -> Result<Option<String>> {
    let (Some(rid), Some(st)) = (rid.clone(), state) else {
        return Ok(rid);
    };
    let on_key = parallel_runner.then_some(key);
    let anchor_has_ranges = st.has_keyset_ranges(export_name, &rid, on_key)?;
    if anchor_is_own(parallel_runner, anchor_has_ranges) {
        return Ok(Some(rid));
    }
    if parallel_runner && incremental && !st.has_keyset_ranges(export_name, &rid, None)? {
        crate::rivet_bail!(
            crate::error::codes::STATE_KEYSET_SEQUENTIAL_ANCHOR_UNFINISHED,
            "export '{export_name}': interrupted run {rid} was a sequential `keyset_incremental` \
             run whose cursor already moved past pages no manifest names — a parallel run would \
             seek past them and lose them. Re-run once with `parallel: 1` to finish run {rid}, \
             then raise `parallel:`."
        );
    }
    let why = if parallel_runner {
        "left no parallel ranges on this key (a sequential run, or one keyed differently)"
    } else {
        "was a parallel keyset run, which a sequential one cannot continue"
    };
    log::warn!(
        "export '{export_name}': interrupted run {rid} {why} — starting a fresh pass; its parts \
         stay unmanifested and are not read"
    );
    st.clear_resume_run_id(export_name, scope)?;
    Ok(None)
}

/// Whether a keyset export with this `parallel:` runs the range-worker runner instead of the single seek loop.
fn fans_out(parallel: usize) -> bool {
    parallel > 1
}

/// Does a sequential keyset run seek from the persisted cursor (crash recovery, or `keyset_incremental`) rather than from the start of the key space?
fn seeks_from_persisted_cursor(
    checkpoint: bool,
    recovering_crash: bool,
    incremental: bool,
) -> bool {
    checkpoint && (recovering_crash || incremental)
}

/// Is the resume anchor released as soon as the data is complete?
fn releases_anchor_at_data_complete(checkpoint: bool, incremental: bool) -> bool {
    checkpoint && !incremental
}

/// Does each committed page move the stored cursor?
fn advances_cursor_per_page(checkpoint: bool, incremental: bool) -> bool {
    checkpoint && !incremental
}

/// The `(lo, hi)` pairs of a sampled range list, for `persist_keyset_ranges`.
fn lo_hi_pairs(
    ranges: &[(usize, Option<String>, Option<String>, bool)],
) -> Vec<(Option<String>, Option<String>)> {
    ranges
        .iter()
        .map(|(_, lo, hi, _)| (lo.clone(), hi.clone()))
        .collect()
}

pub(crate) fn run_keyset(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    state: Option<&StateStore>,
    progress: &crate::state::ProgressClaim<'_>,
) -> Result<()> {
    let kp = keyset_plan(plan);
    let scope = plan.source.state_key();
    let key_plan = IncrementalCursorPlan {
        primary_column: kp.key_column.clone(),
        fallback_column: None,
        mode: IncrementalCursorMode::SingleColumn,
        settle: None,
    };

    if fans_out(kp.parallel) {
        return run_keyset_parallel(src, plan, summary, key_plan, kp.parallel, state, progress);
    }

    log::info!(
        "export '{}': keyset (seek) pagination on '{}', page size {}",
        plan.export_name,
        kp.key_column,
        kp.chunk_size
    );

    let resume_run_id: Option<String> = if kp.checkpoint {
        match state {
            Some(st) => resume_anchor(
                Some(st),
                &plan.export_name,
                &scope,
                progress.resume_run_id()?,
                false,
                &kp.key_column,
                kp.incremental,
            )?,
            None => None,
        }
    } else {
        None
    };
    let recovering_crash = resume_run_id.is_some();
    summary.resumed = recovering_crash;

    let mut last: Option<String> =
        if seeks_from_persisted_cursor(kp.checkpoint, recovering_crash, kp.incremental) {
            progress.cursor()?.last_cursor_value
        } else {
            None
        };
    summary.cursor_low = last.clone();

    if kp.checkpoint
        && let Some(st) = state
    {
        match &resume_run_id {
            Some(rid) => {
                summary.run_id = rid.clone();
                let forgotten = st.forget_unfinished_page_parts(rid)?;
                log::debug!("keyset resume: {forgotten} part(s) of an unfinished page re-read");
                rehydrate_keyset_pages_probed(st, rid, plan, summary)?;
                if let Some(hw) = st.last_committed_cursor_high(rid)? {
                    last = Some(hw);
                    summary.cursor_low = last.clone();
                }
            }
            None => {
                if !kp.incremental {
                    st.clear_cursor_value(&plan.export_name, &scope)?;
                }
                st.set_resume_run_id(progress.key(), &summary.run_id)?;
            }
        }
    }

    crate::test_hook::maybe_panic_at("keyset_after_open_before_first_page");

    let mut pages: usize = 0;

    let frame = super::frame::RunnerFrame::open(plan)?;
    let (dest, ext) = (frame.dest, frame.ext);
    let run_tag = super::summary::run_scoped_tag(&summary.run_id, &plan.export_name);

    loop {
        let base = format!(
            "{}_{}_keyset_{}.{}",
            plan.export_name,
            run_tag,
            seek_tag(last.as_deref()),
            ext
        );
        let (parts, page) = read_keyset_page(
            src,
            plan,
            &key_plan,
            kp.chunk_size,
            last.as_deref(),
            dest.as_ref(),
            &base,
        );
        let unit = super::commit::UnitId::Page(pages as i64);
        let page = match page {
            Ok(page) => page,
            Err(e) => {
                return super::fan_in::commit_unit(
                    plan,
                    summary,
                    state,
                    unit,
                    parts,
                    |_| super::commit::PartKind::Page {
                        page_index: pages as i64,
                        cursor_high: None,
                    },
                    Default::default(),
                    Err(e),
                    |_| Ok(()),
                );
            }
        };
        let Some(mut page) = page else {
            summary.cursor_high = last.clone();
            break;
        };

        if summary.cursor_low.is_none() {
            summary.cursor_low = page.first_cursor.clone();
        }

        let n_parts = parts.len();
        let high_water = page.next_cursor.clone();
        super::fan_in::commit_unit(
            plan,
            summary,
            state,
            unit,
            parts,
            |pi| super::commit::PartKind::Page {
                page_index: pages as i64,
                cursor_high: page_part_cursor(pi, n_parts, &high_water),
            },
            std::mem::take(&mut page.observed),
            Ok(std::mem::take(&mut page.checksums)),
            |_| {
                if advances_cursor_per_page(kp.checkpoint, kp.incremental)
                    && let (Some(st), Some(v)) = (state, page.next_cursor.as_ref())
                {
                    st.update_with_column(progress.key(), v)?;
                }
                Ok(())
            },
        )?;
        crate::test_hook::maybe_panic_at(&format!("after_keyset_page:{pages}"));
        log::info!(
            "export '{}': keyset page {} — {} rows",
            plan.export_name,
            pages,
            page.rows
        );

        let high = page.next_cursor.clone().or_else(|| last.clone());
        let unreadable = page.next_cursor.is_none().then(|| last.clone());
        let next = next_seek(
            last.as_deref(),
            page.rows,
            kp.chunk_size,
            page.next_cursor,
            &kp.key_column,
            pages,
            &format!("export '{}'", plan.export_name),
        )
        .inspect_err(|_| {
            if let Some(l) = unreadable {
                summary.offending_value = l.clone();
                summary.cursor_high = l;
            }
        })?;
        pages += 1;
        match next {
            Some(v) => last = Some(v),
            None => {
                summary.cursor_high = high;
                break;
            }
        }
    }

    if releases_anchor_at_data_complete(kp.checkpoint, kp.incremental)
        && let Some(st) = state
    {
        st.clear_resume_run_id(&plan.export_name, &plan.source.state_key())?;
    }

    crate::test_hook::maybe_panic_at("keyset_after_data_complete");

    log::info!(
        "export '{}': keyset complete — {} page(s), {} rows",
        plan.export_name,
        pages,
        summary.total_rows
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    /// A keyset page the destination holds at another size than the state logged is refused on resume, never declared.
    #[test]
    fn a_keyset_page_of_another_size_is_refused_on_resume() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = tempfile::tempdir().unwrap();
        let st =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        let (run_id, name) = ("r_pages", "orders_page_00001.parquet");
        st.record_file(crate::state::FilePart {
            run_id,
            export_name: "orders",
            file_name: name,
            rows: 3,
            bytes: 5,
            format: "parquet",
            compression: None,
            cursor_high: Some("3"),
        })
        .unwrap();
        let plan = crate::pipeline::chunked::m8_plan(dir.path());
        let summary = || {
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"))
        };
        std::fs::write(dir.path().join(name), b"12345").unwrap();
        let mut whole = summary();
        rehydrate_keyset_pages_probed(&st, run_id, &plan, &mut whole).unwrap();
        assert_eq!(
            whole.manifest_parts.len(),
            1,
            "the page at its logged size is declared"
        );
        std::fs::write(dir.path().join(name), b"1234567").unwrap();
        let err = rehydrate_keyset_pages_probed(&st, run_id, &plan, &mut summary()).unwrap_err();
        assert!(format!("{err:#}").contains(name), "{err:#}");
    }

    fn anchor_key() -> crate::state::ProgressKey {
        crate::state::ProgressKey {
            export_name: "e".into(),
            source: "p".into(),
            stream: String::new(),
            schema: String::new(),
            population: String::new(),
            column: Some("id".into()),
            landing: Default::default(),
            mode: "keyset",
            continues_high_water: false,
            resumable: true,
            shape: Default::default(),
        }
    }

    #[test]
    fn only_parallel_two_or_more_fans_out() {
        assert_eq!([0, 1, 2, 8].map(fans_out), [false, false, true, true]);
    }

    #[test]
    fn a_parallel_incremental_run_refuses_a_sequential_anchor_whose_cursor_ran_ahead() {
        let st = StateStore::open_in_memory().unwrap();
        st.set_resume_run_id(&anchor_key(), "seq").unwrap();
        let err =
            resume_anchor(Some(&st), "e", "p", Some("seq".into()), true, "id", true).unwrap_err();
        assert!(
            err.to_string()
                .contains("Re-run once with `parallel: 1` to finish run seq"),
            "{err}"
        );
        assert_eq!(
            st.get_resume_run_id("e", "p").unwrap(),
            Some("seq".into()),
            "the anchor keeps the crashed run's pages reachable"
        );
    }

    #[test]
    fn resume_anchor_keeps_its_own_shape_and_clears_the_other() {
        let st = StateStore::open_in_memory().unwrap();
        let ranges = [(None, Some("5".to_string())), (Some("5".to_string()), None)];
        let anchored = |rid: &str, ranged_on: Option<&str>| {
            st.set_resume_run_id(&anchor_key(), rid).unwrap();
            if let Some(key) = ranged_on {
                st.persist_keyset_ranges("e", "p", rid, key, &ranges)
                    .unwrap();
            }
        };
        anchored("par", Some("id"));
        assert_eq!(
            resume_anchor(Some(&st), "e", "p", Some("par".into()), true, "id", false).unwrap(),
            Some("par".into())
        );
        assert_eq!(
            st.get_resume_run_id("e", "p").unwrap(),
            Some("par".into()),
            "kept"
        );
        assert_eq!(
            resume_anchor(Some(&st), "e", "p", Some("par".into()), false, "id", false).unwrap(),
            None
        );
        assert_eq!(
            st.get_resume_run_id("e", "p").unwrap(),
            None,
            "a sequential run clears a parallel anchor"
        );

        anchored("seq", None);
        assert_eq!(
            resume_anchor(Some(&st), "e", "p", Some("seq".into()), false, "id", false).unwrap(),
            Some("seq".into())
        );
        assert_eq!(
            resume_anchor(Some(&st), "e", "p", Some("seq".into()), true, "id", false).unwrap(),
            None
        );
        assert_eq!(
            st.get_resume_run_id("e", "p").unwrap(),
            None,
            "a parallel run clears a sequential anchor"
        );

        anchored("other", Some("ts"));
        assert_eq!(
            resume_anchor(Some(&st), "e", "p", Some("other".into()), true, "id", false).unwrap(),
            None,
            "ranges on another key do not make a parallel anchor on this one"
        );
        assert_eq!(
            resume_anchor(None, "e", "p", Some("x".into()), true, "id", false).unwrap(),
            Some("x".into())
        );
    }

    #[test]
    fn an_anchor_belongs_to_the_runner_shape_that_wrote_it() {
        assert!(anchor_is_own(true, true), "parallel resumes its own ranges");
        assert!(
            anchor_is_own(false, false),
            "sequential resumes its own pages"
        );
        assert!(
            !anchor_is_own(true, false),
            "parallel never adopts a sequential anchor"
        );
        assert!(
            !anchor_is_own(false, true),
            "sequential never continues a parallel anchor"
        );
    }

    use super::*;
    use crate::config::SourceType;

    /// `next_seek` over a full page with the given bounds.
    fn seek(prev: Option<&str>, next: Option<&str>) -> Result<Option<String>> {
        next_seek(prev, 3, 3, next.map(str::to_string), "ID", 1, "export 'x'")
    }

    #[test]
    fn a_page_that_ends_on_its_start_bound_is_refused() {
        let ts = "2024-01-01T00:00:00.123456";
        assert!(next_seek(Some(ts), 3, 3, Some(ts.into()), "T9", 3, "export 'x'").is_err());
        assert_eq!(seek(Some("7"), Some("14")).unwrap().as_deref(), Some("14"));
        assert_eq!(seek(None, Some("7")).unwrap().as_deref(), Some("7"));
    }

    #[test]
    fn a_short_page_is_the_last_even_without_a_key() {
        assert_eq!(
            next_seek(Some("7"), 2, 3, None, "ID", 1, "export 'x'").unwrap(),
            None
        );
        assert_eq!(
            next_seek(Some("7"), 2, 3, Some("7".into()), "ID", 1, "x").unwrap(),
            None
        );
    }

    #[test]
    fn an_unreadable_key_error_is_classified_and_names_the_last_readable_key() {
        let msg = seek(Some("7"), None).unwrap_err().to_string();
        assert_eq!(
            crate::pipeline::job::classify_error_message(&msg),
            Some("keyset_unreadable_key"),
            "{msg}"
        );
        assert!(msg.contains("last readable key: 7"), "{msg}");
        assert!(msg.starts_with("export 'x': "), "{msg}");
    }

    const FNV_ID_000300: &str = "c4c7be0f3cc9638a";

    #[test]
    fn seek_tag_is_deterministic_and_distinguishes_seeks() {
        assert_eq!(seek_tag(None), "start");
        assert_eq!(seek_tag(Some("id-000300")), seek_tag(Some("id-000300")));
        assert_ne!(seek_tag(Some("id-000300")), seek_tag(Some("id-000600")));
        assert_ne!(seek_tag(Some("id-000300")), seek_tag(None));
        assert_eq!(seek_tag(Some("id-000300")), FNV_ID_000300);
        let t = seek_tag(Some("anything"));
        assert_eq!(t.len(), 16);
        assert!(
            t.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    /// Only the last part of a page carries the page's high-water key; earlier parts and an unkeyed page carry none.
    #[test]
    fn only_a_pages_last_part_carries_its_high_water() {
        let hw = Some("42".to_string());
        assert_eq!(page_part_cursor(0, 3, &hw), None);
        assert_eq!(page_part_cursor(1, 3, &hw), None);
        assert_eq!(page_part_cursor(2, 3, &hw), hw);
        assert_eq!(page_part_cursor(0, 1, &hw), hw);
        assert_eq!(page_part_cursor(0, 1, &None), None);
    }

    /// P-22: a resumed run's cursor is the highest key of the whole run, the ranges committed before the crash included.
    #[test]
    fn a_resumed_run_counts_the_keys_its_committed_ranges_recorded() {
        let s = |x: &str| Some(x.to_string());
        let seeded = seeded_range_max(4, vec![(0, None), (1, None), (3, s("400")), (9, s("x"))]);
        assert_eq!(seeded, vec![None, None, None, s("400")]);
        let mut after_the_rerun = seeded;
        after_the_rerun[1] = s("225");
        assert_eq!(highest_range_max(after_the_rerun), s("400"));
        assert_eq!(seeded_range_max(2, Vec::new()), vec![None, None]);
    }

    /// A stored range `(lo, hi]` of a parallel keyset run.
    fn stored_range(
        index: i64,
        lo: Option<&str>,
        hi: Option<&str>,
        done: bool,
        max_key: Option<&str>,
    ) -> crate::state::KeysetRangeRow {
        crate::state::KeysetRangeRow {
            range_index: index,
            lo: lo.map(Into::into),
            hi: hi.map(Into::into),
            done,
            max_key: max_key.map(Into::into),
        }
    }

    #[test]
    fn only_a_committed_open_ended_last_range_leaves_a_tail_to_read() {
        use super::UnreadTail::{Again, Past};
        let first = stored_range(0, None, Some("100"), true, Some("100"));
        let tail = |last| super::unread_tail(&[first.clone(), last]);
        assert_eq!(
            tail(stored_range(1, Some("100"), None, true, Some("200"))),
            Some(Past("200".into()))
        );
        assert_eq!(
            tail(stored_range(1, Some("100"), None, true, None)),
            Some(Again)
        );
        assert_eq!(tail(stored_range(1, Some("100"), None, false, None)), None);
        assert_eq!(
            tail(stored_range(1, Some("100"), Some("200"), true, Some("200"))),
            None,
            "an incremental run stops at the ceiling it pinned"
        );
        assert_eq!(super::unread_tail(&[]), None);
        let open_first = stored_range(0, None, None, true, Some("9"));
        let bounded_last = stored_range(1, Some("9"), Some("20"), false, None);
        assert_eq!(super::unread_tail(&[open_first, bounded_last]), None);
    }

    #[test]
    fn a_parallel_keyset_part_names_its_range() {
        let part = |range: usize, page: usize| {
            format!(
                "orders_pk_w1_{}_pk_w{range}_{page}.parquet",
                crate::pipeline::summary::run_scoped_tag("r-1", "orders")
            )
        };
        assert_eq!(super::range_of_part(&part(0, 3)), Some(0));
        assert_eq!(super::range_of_part(&part(12, 0)), Some(12));
        let rotated = crate::pipeline::commit::part_indexed_name(&part(7, 2), 1, 3);
        assert_eq!(super::range_of_part(&rotated), Some(7));
        assert_eq!(super::range_of_part("orders_pk_w3_0"), Some(3));
        for not_one in [
            "orders_20260101T000000_chunk3_0123456789abcdef.parquet",
            "orders_r-1_pk_wx_0.parquet",
            "orders_r-1_pk_w3_x.parquet",
            "orders_r-1_pk_w_0.parquet",
            "orders.parquet",
        ] {
            assert_eq!(super::range_of_part(not_one), None, "{not_one}");
        }
    }

    /// A resumed full pass reads past its committed last range.
    #[test]
    fn a_resumed_full_pass_plans_what_its_committed_last_range_left_unread() {
        use crate::state::KeysetRangePart;
        let pairs = [
            (None, Some("100".to_string())),
            (Some("100".to_string()), None),
        ];
        let part = |name: &str| KeysetRangePart {
            file_name: name.into(),
            rows: 5,
            bytes: 50,
        };
        let opened = |top: Option<&str>| {
            let st = StateStore::open_in_memory().unwrap();
            st.persist_keyset_ranges("e", "p", "run", "id", &pairs)
                .unwrap();
            let low = [part("e_run_pk_w0_0.parquet")];
            st.commit_keyset_range("run", "e", (0, Some("100")), &low, "parquet", None)
                .unwrap();
            let high = [part("e_run_pk_w1_0.parquet"), part("e_run_pk_w1_1.parquet")];
            st.commit_keyset_range("run", "e", (1, top), &high, "parquet", None)
                .unwrap();
            st
        };
        let plan = |st: &StateStore| {
            let mut ranges = st.load_keyset_ranges("e", "run", "id").unwrap();
            super::plan_unread_tail(st, "e", "p", "run", "id", &mut ranges).unwrap();
            let stored = st.load_keyset_ranges("e", "run", "id").unwrap();
            let shape = |r: &crate::state::KeysetRangeRow| (r.range_index, r.lo.clone(), r.done);
            assert_eq!(
                ranges.iter().map(shape).collect::<Vec<_>>(),
                stored.iter().map(shape).collect::<Vec<_>>(),
                "the run reads what the state holds"
            );
            let logged = st.list_files_for_run("run").unwrap();
            let logged: Vec<String> = logged.into_iter().map(|f| f.file_name).collect();
            (stored.iter().map(shape).collect::<Vec<_>>(), logged)
        };
        let own = |v: &str| Some(v.to_string());
        let all = [
            "e_run_pk_w0_0.parquet",
            "e_run_pk_w1_0.parquet",
            "e_run_pk_w1_1.parquet",
        ];

        let recorded = opened(Some("180"));
        let (ranges, logged) = plan(&recorded);
        assert_eq!(
            ranges,
            [
                (0, None, true),
                (1, own("100"), true),
                (2, own("180"), false)
            ]
        );
        assert_eq!(logged, all);

        let unrecorded = opened(None);
        let (ranges, logged) = plan(&unrecorded);
        assert_eq!(ranges, [(0, None, true), (1, own("100"), false)]);
        assert_eq!(
            logged,
            all[..1],
            "the range read again declares no part twice"
        );
        let (again, _) = plan(&unrecorded);
        assert_eq!(again, ranges, "a range left to read is no tail");
    }

    #[test]
    fn highest_range_max_takes_the_top_populated_range() {
        let s = |x: &str| Some(x.to_string());
        assert_eq!(highest_range_max(vec![s("k1"), s("k2"), s("k3")]), s("k3"));
        assert_eq!(highest_range_max(vec![s("k1"), s("k2"), None]), s("k2"));
        assert_eq!(highest_range_max(vec![s("lo"), None, s("hi")]), s("hi"));
        assert_eq!(highest_range_max(vec![None, None]), None);
        assert_eq!(highest_range_max(vec![]), None);
    }

    #[test]
    fn first_row_clause_is_per_dialect() {
        assert_eq!(first_row_clause(SourceType::Postgres), "LIMIT 1");
        assert_eq!(first_row_clause(SourceType::Mysql), "LIMIT 1");
        assert_eq!(
            first_row_clause(SourceType::Mssql),
            "OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY"
        );
        assert_eq!(
            first_row_clause(SourceType::Oracle),
            "OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY"
        );
    }

    #[test]
    fn fan_out_collapses_only_when_parallel_was_asked_for() {
        assert!(fan_out_collapsed(4, 1));
        assert!(!fan_out_collapsed(1, 1), "sequential was asked for");
        assert!(!fan_out_collapsed(4, 2));
    }

    #[test]
    fn keyset_seeks_from_the_cursor_only_for_recovery_or_incremental() {
        assert!(
            seeks_from_persisted_cursor(true, true, false),
            "crash recovery"
        );
        assert!(
            seeks_from_persisted_cursor(true, false, true),
            "keyset_incremental"
        );
        assert!(
            !seeks_from_persisted_cursor(true, false, false),
            "clean re-run: full pass"
        );
        assert!(
            !seeks_from_persisted_cursor(false, true, true),
            "no checkpoint, no cursor"
        );
    }

    #[test]
    fn only_a_crash_recovery_run_releases_its_anchor_at_data_complete() {
        assert!(releases_anchor_at_data_complete(true, false));
        assert!(
            !releases_anchor_at_data_complete(true, true),
            "incremental keeps it to finalize"
        );
        assert!(
            !releases_anchor_at_data_complete(false, false),
            "no checkpoint, no anchor"
        );
    }

    #[test]
    fn only_a_crash_recovery_run_moves_the_stored_cursor_page_by_page() {
        assert!(advances_cursor_per_page(true, false));
        assert!(
            !advances_cursor_per_page(true, true),
            "an incremental cursor moves once, after the manifest"
        );
        assert!(
            !advances_cursor_per_page(false, false),
            "no checkpoint, no cursor"
        );
    }

    #[test]
    fn lo_hi_pairs_projects_the_bounds() {
        let ranges = vec![
            (0usize, None, Some("k0500".to_string()), false),
            (
                1,
                Some("k0500".to_string()),
                Some("k1000".to_string()),
                false,
            ),
            (2, Some("k1000".to_string()), None, false),
        ];
        assert_eq!(
            lo_hi_pairs(&ranges),
            vec![
                (None, Some("k0500".to_string())),
                (Some("k0500".to_string()), Some("k1000".to_string())),
                (Some("k1000".to_string()), None),
            ]
        );
    }

    /// #161: the gap-free / overlap-free coverage property of the pure partitioning fold, asserted rather than trusted.
    #[test]
    fn partition_ranges_cover_the_key_space_without_gaps_or_overlap() {
        use proptest::prelude::*;
        proptest!(|(
            mut bounds in proptest::collection::vec("[0-9a-f]{1,8}", 0..12),
            floor in proptest::option::of("[0-9a-f]{1,8}"),
            ceil in proptest::option::of("[0-9a-f]{1,8}"),
        )| {
            bounds.sort();
            bounds.dedup();
            let ranges = partition_ranges(&bounds, floor.as_deref(), ceil.as_deref());
            prop_assert_eq!(ranges.len(), bounds.len() + 1);
            for (i, r) in ranges.iter().enumerate() {
                prop_assert_eq!(r.0, i, "dense range indices");
                prop_assert!(!r.3, "ranges start not-done");
            }
            prop_assert_eq!(ranges[0].1.as_deref(), floor.as_deref());
            for w in ranges.windows(2) {
                prop_assert_eq!(w[1].1.as_deref(), w[0].2.as_deref(), "no gap, no overlap");
            }
            prop_assert_eq!(ranges[ranges.len() - 1].2.as_deref(), ceil.as_deref());
        });
    }

    /// A source whose page is three rows over three distinct days of `d`.
    struct ThreeDays;
    impl Source for ThreeDays {
        fn export(
            &mut self,
            _request: &source::ExportRequest<'_>,
            sink: &mut dyn source::BatchSink,
        ) -> Result<()> {
            use arrow::datatypes::{DataType, Field, Schema};
            let schema =
                std::sync::Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
            sink.on_schema(schema.clone())?;
            let days = arrow::array::Date32Array::from(vec![0, 1, 2]);
            sink.on_batch(&arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![std::sync::Arc::new(days)],
            )?)
        }
        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>> {
            Ok(None)
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    struct CompositePage(Vec<String>, bool);
    impl Source for CompositePage {
        fn export(
            &mut self,
            request: &source::ExportRequest<'_>,
            sink: &mut dyn source::BatchSink,
        ) -> Result<()> {
            use arrow::datatypes::{DataType, Field, Schema};
            self.0 = request.key_rest.to_vec();
            self.1 = request.key_range;
            let schema = std::sync::Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("kind", DataType::Utf8, false),
            ]));
            sink.on_schema(schema.clone())?;
            sink.on_batch(&arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![
                    std::sync::Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
                    std::sync::Arc::new(arrow::array::StringArray::from(vec!["a", "z"])),
                ],
            )?)
        }
        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>> {
            Ok(None)
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    /// A composite-key page asks the source for the rest of the key and stores the last row's key tuple.
    #[test]
    fn a_composite_key_page_reads_by_the_whole_key_and_stores_its_tuple() {
        let mut plan = crate::plan::test_support::test_plan();
        plan.strategy = ExtractionStrategy::Keyset(KeysetPlan {
            key_column: "id".into(),
            chunk_size: 10,
            checkpoint: true,
            incremental: false,
            parallel: 1,
            key_rest: vec!["kind".into()],
        });
        let key = IncrementalCursorPlan {
            primary_column: "id".into(),
            fallback_column: None,
            mode: IncrementalCursorMode::SingleColumn,
            settle: None,
        };
        let mut src = CompositePage(Vec::new(), true);
        let sink = fill_keyset_page(&mut src, &plan, &key, 10, None, None)
            .unwrap()
            .expect("a page with rows");
        assert_eq!(src.0, vec!["kind".to_string()]);
        assert!(!src.1, "a sequential page is not a range worker's");
        assert_eq!(sink.effective_cursor().as_deref(), Some(r#"["2","z"]"#));
        if let ExtractionStrategy::Keyset(k) = &mut plan.strategy {
            k.parallel = 4;
        }
        fill_keyset_page(
            &mut src,
            &plan,
            &key,
            10,
            Some(r#"["0"]"#),
            Some(r#"["9"]"#),
        )
        .unwrap();
        assert!(src.1, "a parallel worker's page reads a lead-bounded range");
    }

    /// An indexed key column that answers only first-row seeks, refusing a count or an offset skip.
    struct Indexed {
        keys: Vec<String>,
        numeric: bool,
        st: SourceType,
        sql: Vec<String>,
        refuse: Option<&'static str>,
    }
    impl Indexed {
        /// A PostgreSQL table holding `keys` (sorted here), compared as numbers or as text.
        fn new(mut keys: Vec<String>, numeric: bool) -> Self {
            let order = |a: &String, b: &String| Indexed::order(numeric, a, b);
            keys.sort_by(order);
            Indexed {
                keys,
                numeric,
                st: SourceType::Postgres,
                sql: Vec::new(),
                refuse: None,
            }
        }
        /// The key order the fake index keeps.
        fn order(numeric: bool, a: &str, b: &str) -> std::cmp::Ordering {
            if numeric {
                a.parse::<i128>().unwrap().cmp(&b.parse::<i128>().unwrap())
            } else {
                a.cmp(b)
            }
        }
        /// One quoted literal at the head of `w` in this dialect, and the text after it.
        fn literal<'w>(&self, w: &'w str) -> (String, &'w str) {
            let pg = self.st == SourceType::Postgres;
            let backslash = pg || self.st == SourceType::Mysql;
            let body = w.strip_prefix(if pg { "E'" } else { "'" }).unwrap();
            let mut v = String::new();
            let mut chars = body.char_indices().peekable();
            while let Some((i, c)) = chars.next() {
                match c {
                    '\\' if backslash => v.push(chars.next().unwrap().1),
                    '\'' if !backslash && chars.peek().is_some_and(|(_, n)| *n == '\'') => {
                        chars.next();
                        v.push('\'');
                    }
                    '\'' => return (v, &body[i + 1..]),
                    c => v.push(c),
                }
            }
            panic!("unterminated literal: {w}")
        }
        /// The keys, ascending, that the `SELECT k … ORDER BY k [DESC]` statement `head` selects.
        fn hits(&self, head: &str) -> Vec<&String> {
            assert!(
                !head.contains("COUNT") && !head.contains("OFFSET"),
                "a scan: {head}"
            );
            let order = format!(" ORDER BY {}", crate::sql::quote_ident(self.st, "k"));
            let head = head.strip_suffix(" DESC").unwrap_or(head);
            let head = head.strip_suffix(&order).unwrap();
            let preds = head
                .split_once(" WHERE ")
                .map_or(vec![], |(_, w)| self.predicates(w));
            self.keys
                .iter()
                .filter(|k| {
                    preds.iter().all(|(op, v)| {
                        let o = Indexed::order(self.numeric, k, v);
                        match op.as_str() {
                            ">" => o.is_gt(),
                            ">=" => o.is_ge(),
                            "<" => o.is_lt(),
                            "<=" => o.is_le(),
                            other => panic!("unexpected operator {other}"),
                        }
                    })
                })
                .collect()
        }
        /// The `(operator, value)` predicates of a WHERE clause made of `k <op> '<v>'` and `k IS NOT NULL` joined by AND.
        fn predicates(&self, mut w: &str) -> Vec<(String, String)> {
            let k = format!("{} ", crate::sql::quote_ident(self.st, "k"));
            let mut out = Vec::new();
            while !w.is_empty() {
                w = w
                    .strip_prefix(" AND ")
                    .unwrap_or(w)
                    .strip_prefix(&k)
                    .unwrap();
                if let Some(rest) = w.strip_prefix("IS NOT NULL") {
                    w = rest;
                    continue;
                }
                let (op, rest) = w.split_once(' ').unwrap();
                let (v, rest) = self.literal(rest);
                out.push((op.to_string(), v));
                w = rest;
            }
            out
        }
    }
    impl Source for Indexed {
        fn export(
            &mut self,
            _request: &source::ExportRequest<'_>,
            _sink: &mut dyn source::BatchSink,
        ) -> Result<()> {
            unreachable!("sampling reads scalars only")
        }
        fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
            self.sql.push(sql.to_string());
            if self.refuse.is_some_and(|r| sql.contains(r)) {
                anyhow::bail!("invalid input value for the key's type");
            }
            if let Some(inner) = sql.strip_prefix("SELECT COUNT(*) FROM (") {
                let inner = inner.rsplit_once(") ").unwrap().0;
                let head = inner
                    .strip_suffix(&format!(" {}", first_rows_clause(self.st, PROBE_ROWS)))
                    .unwrap_or_else(|| panic!("not a bounded count: {sql}"));
                let n = self.hits(head).len().min(PROBE_ROWS as usize);
                return Ok(Some(n.to_string()));
            }
            let (head, skip) = match sql.rsplit_once(" OFFSET ") {
                Some((h, rest)) => {
                    let n = rest
                        .split(' ')
                        .next()
                        .and_then(|d| d.parse::<usize>().ok())
                        .unwrap_or_else(|| panic!("{sql}"));
                    (h.strip_suffix(" LIMIT 1").unwrap_or(h), n)
                }
                None => (
                    sql.strip_suffix(&format!(" {}", first_row_clause(self.st)))
                        .unwrap_or_else(|| panic!("not a first-row seek: {sql}")),
                    0,
                ),
            };
            assert!(skip <= PROBE_ROWS as usize, "a deep probe: {sql}");
            let found = if head.ends_with(" DESC") {
                self.hits(head).into_iter().rev().nth(skip)
            } else {
                self.hits(head).into_iter().nth(skip)
            };
            Ok(found.cloned())
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    #[test]
    fn percentile_offset_partitions_evenly() {
        assert_eq!(percentile_offset(1000, 1, 4), 250);
        assert_eq!(percentile_offset(1000, 2, 4), 500);
        assert_eq!(percentile_offset(1000, 3, 4), 750);
        assert_eq!(percentile_offset(999, 1, 3), 333);
        assert_eq!(percentile_offset(999, 2, 3), 666);
    }

    #[test]
    fn nth_row_clause_is_per_dialect() {
        assert_eq!(
            nth_row_clause(SourceType::Postgres, 250),
            "LIMIT 1 OFFSET 250"
        );
        assert_eq!(nth_row_clause(SourceType::Mysql, 250), "LIMIT 1 OFFSET 250");
        assert_eq!(
            nth_row_clause(SourceType::Mssql, 250),
            "OFFSET 250 ROWS FETCH NEXT 1 ROWS ONLY"
        );
    }

    /// A table of 1000 rows whose catalog estimate is `estimate`: counts and offset reads answered by row number, MIN and MAX of a text key `a`..`z`.
    struct Cataloged {
        estimate: &'static str,
        sql: Vec<String>,
    }
    impl Source for Cataloged {
        fn export(
            &mut self,
            _request: &source::ExportRequest<'_>,
            _sink: &mut dyn source::BatchSink,
        ) -> Result<()> {
            unreachable!("sampling reads scalars only")
        }
        fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
            self.sql.push(sql.to_string());
            if sql.contains("reltuples") {
                return Ok(Some(self.estimate.to_string()));
            }
            if sql.contains("COUNT(*)") {
                return Ok(Some("1000".into()));
            }
            if !sql.contains("OFFSET ") && sql.contains("LIMIT 1") {
                return Ok(Some(if sql.contains("DESC") { "z" } else { "a" }.into()));
            }
            Ok(sql
                .split_once("OFFSET ")
                .map(|(_, off)| (off.trim().parse::<i64>().unwrap() + 1).to_string()))
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    /// A key with no axis on a table the catalog puts under the row-percentile limit keeps the exact percentile sampler; a larger, unanalyzed or query export never counts.
    #[test]
    fn the_catalog_estimate_picks_row_percentiles_or_seeks() {
        let mut plan = crate::plan::test_support::test_plan();
        plan.base_query = "SELECT * FROM public.t".into();
        let mut small = Cataloged {
            estimate: "9999999",
            sql: vec![],
        };
        let got = sample_key_boundaries(&mut small, &plan, "k", 4, 64, None, None).unwrap();
        assert_eq!(got, keys([251, 501, 751]));
        assert_eq!(
            small.sql.len(),
            7,
            "MIN, MAX, estimate, COUNT, three offsets: {:?}",
            small.sql
        );
        assert!(small.sql[3].contains("COUNT(*)"), "{:?}", small.sql);
        for estimate in ["10000000", "0"] {
            let mut large = Cataloged {
                estimate,
                sql: vec![],
            };
            sample_key_boundaries(&mut large, &plan, "k", 4, 64, None, None).unwrap();
            assert!(
                large.sql.iter().any(|q| q.contains("reltuples")),
                "{:?}",
                large.sql
            );
            assert!(
                large
                    .sql
                    .iter()
                    .all(|q| !q.contains("COUNT") && !q.contains("OFFSET ")),
                "{:?}",
                large.sql
            );
        }
        plan.base_query = "SELECT * FROM public.t WHERE k > 0".into();
        let mut query = Cataloged {
            estimate: "5",
            sql: vec![],
        };
        sample_key_boundaries(&mut query, &plan, "k", 4, 64, None, None).unwrap();
        assert!(
            query
                .sql
                .iter()
                .all(|q| !q.contains("reltuples") && !q.contains("COUNT")),
            "a query has no catalog estimate and takes the seek path: {:?}",
            query.sql
        );
    }

    /// An integer key is cut into `int_parts` key-space ranges; any other key into `parts`.
    #[test]
    fn an_integer_key_is_cut_into_more_ranges_than_workers() {
        let plan = crate::plan::test_support::test_plan();
        let mut ints = Indexed::new(keys(1..=1000), true);
        let got = sample_key_boundaries(&mut ints, &plan, "k", 4, 16, None, None).unwrap();
        assert_eq!(got.len(), 15, "{got:?}");
        assert_eq!(
            ints.sql.len(),
            49,
            "MIN, MAX, one seek per cut, one probe and one bounded count per range"
        );
        let mut text = Indexed::new(keys(["a", "c", "e", "g", "i", "k", "m", "o"]), false);
        let got = sample_key_boundaries(&mut text, &plan, "k", 4, 16, None, None).unwrap();
        assert_eq!(got.len(), 3, "{got:?}");
    }

    /// A source that records the timeout of every scalar read and cancels the `expire_at`-th timed read.
    struct Timed<S> {
        inner: S,
        seen: Vec<Option<u64>>,
        expire_at: Option<usize>,
    }
    impl<S> Timed<S> {
        fn new(inner: S, expire_at: Option<usize>) -> Self {
            Timed {
                inner,
                seen: Vec::new(),
                expire_at,
            }
        }
    }
    impl<S: Source> Source for Timed<S> {
        fn export(
            &mut self,
            _request: &source::ExportRequest<'_>,
            _sink: &mut dyn source::BatchSink,
        ) -> Result<()> {
            unreachable!("sampling reads scalars only")
        }
        fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
            self.seen.push(None);
            self.inner.query_scalar(sql)
        }
        fn query_scalar_within(&mut self, sql: &str, seconds: u64) -> Result<Option<String>> {
            let nth = self.seen.len();
            self.seen.push(Some(seconds));
            if self.expire_at == Some(nth) {
                return Err(source::StatementDurationTimeout::mssql(seconds).into());
            }
            self.inner.query_scalar(sql)
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    /// The percentile, the seek and the recut paths all read through the run's statement timeout, and a timeout of 0 leaves the boundaries as they were.
    #[test]
    fn every_sampler_read_runs_under_the_statement_timeout() {
        let mut plan = crate::plan::test_support::test_plan();
        let mut table = plan.clone();
        table.base_query = "SELECT * FROM public.t".into();
        let skewed = || keys((1..=9_000i64).chain((1..=1_000i64).map(|i| 9_000 + i * 191)));
        for seconds in [0u64, 10] {
            plan.tuning.statement_timeout_s = seconds;
            table.tuning.statement_timeout_s = seconds;
            for (name, estimate, int_parts) in [("percentile", "9999999", 64), ("seek", "0", 4)] {
                let mut src = Timed::new(
                    Cataloged {
                        estimate,
                        sql: vec![],
                    },
                    None,
                );
                sample_key_boundaries(&mut src, &table, "k", 4, int_parts, None, None).unwrap();
                assert!(src.seen.len() >= 3, "{name}: {:?}", src.seen);
                assert!(
                    src.seen.iter().all(|s| *s == Some(seconds)),
                    "{name} at {seconds}s: {:?}",
                    src.seen
                );
            }
            let mut src = Timed::new(Indexed::new(skewed(), true), None);
            let got = sample_key_boundaries(&mut src, &plan, "k", 4, 4, None, None).unwrap();
            assert_eq!(got.len(), 3, "{got:?}");
            assert!(
                src.seen.len() > 9,
                "the recut probes and snaps: {:?}",
                src.seen
            );
            assert!(
                src.seen.iter().all(|s| *s == Some(seconds)),
                "recut at {seconds}s: {:?}",
                src.seen
            );
        }
        plan.tuning.statement_timeout_s = 0;
        let mut zero = Indexed::new(skewed(), true);
        let want = sample_key_boundaries(&mut zero, &plan, "k", 4, 4, None, None).unwrap();
        plan.tuning.statement_timeout_s = 10;
        let mut ten = Timed::new(Indexed::new(skewed(), true), None);
        let got = sample_key_boundaries(&mut ten, &plan, "k", 4, 4, None, None).unwrap();
        assert_eq!(got, want);
    }

    /// A seek or a density probe that times out keeps the cuts found so far; the data stays complete, with fewer ranges.
    #[test]
    fn a_seek_that_times_out_degrades_to_the_cuts_found_so_far() {
        let mut plan = crate::plan::test_support::test_plan();
        plan.tuning.statement_timeout_s = 10;
        let table = || Indexed::new(keys(1..=1000), true);
        let whole = sample_key_boundaries(&mut table(), &plan, "k", 4, 4, None, None).unwrap();
        assert_eq!(whole, keys([250, 500, 750]));
        for (expire_at, want) in [
            (0, Vec::<String>::new()),
            (1, Vec::<String>::new()),
            (3, keys([250])),
            (4, keys([250, 500])),
            (5, whole.clone()),
        ] {
            let mut src = Timed::new(table(), Some(expire_at));
            let got = sample_key_boundaries(&mut src, &plan, "k", 4, 4, None, None).unwrap();
            assert_eq!(got, want, "read {expire_at} timed out: {:?}", src.seen);
        }
    }

    /// A row-percentile count or offset read that times out fails the run with the coded statement-timeout error.
    #[test]
    fn a_percentile_read_that_times_out_fails_with_the_statement_timeout_code() {
        let mut plan = crate::plan::test_support::test_plan();
        plan.base_query = "SELECT * FROM public.t".into();
        plan.tuning.statement_timeout_s = 10;
        for expire_at in [3, 4, 6] {
            let mut src = Timed::new(
                Cataloged {
                    estimate: "9999999",
                    sql: vec![],
                },
                Some(expire_at),
            );
            let err = sample_key_boundaries(&mut src, &plan, "k", 4, 64, None, None).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_STATEMENT_TIMEOUT", 1);
            assert!(
                format!("{err:#}").contains("tuning.statement_timeout_s"),
                "{err:#}"
            );
        }
    }

    /// Owned key strings.
    fn keys(xs: impl IntoIterator<Item = impl ToString>) -> Vec<String> {
        xs.into_iter().map(|x| x.to_string()).collect()
    }

    /// Sampling reads the fenced MIN and MAX and one seek per cut, never a count or an offset skip, and nothing past an empty or one-key fence.
    #[test]
    fn sampled_boundaries_are_seeks_to_even_cuts_inside_the_fence() {
        let plan = crate::plan::test_support::test_plan();
        let mut src = Indexed::new(keys(1..=1000), true);
        let got =
            sample_key_boundaries(&mut src, &plan, "k", 4, 4, Some("100"), Some("900")).unwrap();
        assert_eq!(got, keys([300, 500, 700]));
        assert_eq!(
            src.sql.len(),
            13,
            "MIN, MAX, one seek per cut, one probe and one bounded count per range"
        );
        for sql in &src.sql {
            assert!(sql.contains("\"k\" > E'100' AND \"k\" <= E'900'"), "{sql}");
        }
        let mut sparse = Indexed::new(keys([1, 2, 3, 1_000_000]), true);
        let got = sample_key_boundaries(&mut sparse, &plan, "k", 4, 4, None, None).unwrap();
        assert!(got.is_empty(), "every cut snaps to the MAX: {got:?}");
        assert_eq!(
            sparse.sql.len(),
            3,
            "a cut that reaches the MAX ends the sampling"
        );
        let mut one = Indexed::new(keys([7, 7, 7]), true);
        assert!(
            sample_key_boundaries(&mut one, &plan, "k", 4, 4, None, None)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            one.sql.len(),
            2,
            "a single key value is read twice, never sampled"
        );
        let mut none = Indexed::new(vec![], true);
        assert!(
            sample_key_boundaries(&mut none, &plan, "k", 4, 4, None, None)
                .unwrap()
                .is_empty()
        );
        assert!(!none.sql[0].contains("> E"), "{}", none.sql[0]);
    }

    /// A seek the server refuses (a split point it cannot cast, an end value it cannot read) ends the sampling with the bounds found so far, not the run.
    #[test]
    fn a_seek_the_server_refuses_costs_ranges_not_the_run() {
        let plan = crate::plan::test_support::test_plan();
        for (refuse, want, reads) in [
            ("E'750'", keys([250, 500]), 5),
            (">=", vec![], 3),
            (" DESC", vec![], 2),
        ] {
            let mut src = Indexed::new(keys(1..=1000), true);
            src.refuse = Some(refuse);
            let got = sample_key_boundaries(&mut src, &plan, "k", 4, 4, None, None).unwrap();
            assert_eq!(got, want, "refusing {refuse}");
            assert_eq!(src.sql.len(), reads, "refusing {refuse}: {:?}", src.sql);
        }
    }

    /// Whatever the keys and the fence, the sampled ranges hold every fenced row exactly once, each key value in one range, in at most N+1 seeks.
    #[test]
    fn sampled_ranges_hold_every_fenced_row_once_for_any_keys() {
        use proptest::prelude::*;
        let plan = crate::plan::test_support::test_plan();
        let text = proptest::collection::vec("\\PC{0,6}", 0..60);
        let ints = proptest::collection::vec(-1_000_000i64..1_000_000, 0..60);
        proptest!(|(ts in text, is in ints, numeric: bool, parts in 2usize..9, fence in proptest::option::of((0usize..60, 0usize..60)))| {
            let mut src = if numeric { Indexed::new(keys(is), true) } else { Indexed::new(ts, false) };
            let all = src.keys.clone();
            let (floor, ceil) = fence
                .filter(|_| !all.is_empty())
                .map(|(a, b)| (all.get(a % all.len()).cloned(), all.get(b % all.len()).cloned()))
                .unwrap_or((None, None));
            let bounds = sample_key_boundaries(&mut src, &plan, "k", parts, parts, floor.as_deref(), ceil.as_deref()).unwrap();
            prop_assert!(src.sql.len() <= 2 + parts * (2 * RECUT_ROUNDS + 4), "{} statements for {parts} parts", src.sql.len());
            let ranges = partition_ranges(&bounds, floor.as_deref(), ceil.as_deref());
            prop_assert!(ranges.len() <= parts);
            let numeric = src.numeric;
            let inside = |k: &str, lo: &Option<String>, hi: &Option<String>| {
                lo.as_deref().is_none_or(|l| Indexed::order(numeric, k, l).is_gt())
                    && hi.as_deref().is_none_or(|h| Indexed::order(numeric, k, h).is_le())
            };
            for k in &all {
                let held = ranges.iter().filter(|(_, lo, hi, _)| inside(k, lo, hi)).count();
                let fenced = inside(k, &floor, &ceil);
                prop_assert_eq!(held, usize::from(fenced), "key {:?} in {:?}", k, ranges);
            }
        });
    }

    /// A key with 90% of its rows in 5% of its span keeps every row once, in ranges recut to near-equal row counts.
    #[test]
    fn a_skewed_key_keeps_every_row_once_in_balanced_ranges() {
        let plan = crate::plan::test_support::test_plan();
        let dense = 1..=9_000i64;
        let sparse = (1..=1_000i64).map(|i| 9_000 + i * 191);
        let mut src = Indexed::new(keys(dense.chain(sparse)), true);
        let all = src.keys.clone();
        let bounds = sample_key_boundaries(&mut src, &plan, "k", 4, 4, None, None).unwrap();
        let ranges = partition_ranges(&bounds, None, None);
        let held = |lo: &Option<String>, hi: &Option<String>| {
            all.iter()
                .filter(|k| {
                    lo.as_deref()
                        .is_none_or(|l| Indexed::order(true, k, l).is_gt())
                        && hi
                            .as_deref()
                            .is_none_or(|h| Indexed::order(true, k, h).is_le())
                })
                .count()
        };
        let spread: Vec<usize> = ranges.iter().map(|(_, lo, hi, _)| held(lo, hi)).collect();
        assert_eq!(spread.iter().sum::<usize>(), all.len(), "{spread:?}");
        assert_eq!(spread, vec![3_375, 3_374, 2_257, 994], "{bounds:?}");
    }

    /// A uuid key with 90% of its rows in 5% of its span keeps every row once, in ranges recut to near-equal row counts.
    #[test]
    fn a_skewed_uuid_key_keeps_every_row_once_in_balanced_ranges() {
        let plan = crate::plan::test_support::test_plan();
        let uuid = |units: i64| {
            uuid::Uuid::from_u128((units as u128) << 96)
                .hyphenated()
                .to_string()
        };
        let dense = 1..=9_000i64;
        let sparse = (1..=1_000i64).map(|i| 9_000 + i * 191);
        let mut src = Indexed::new(dense.chain(sparse).map(uuid).collect(), false);
        let all = src.keys.clone();
        let bounds = sample_key_boundaries(&mut src, &plan, "k", 4, 4, None, None).unwrap();
        let ranges = partition_ranges(&bounds, None, None);
        let held = |lo: &Option<String>, hi: &Option<String>| {
            all.iter()
                .filter(|k| {
                    lo.as_deref().is_none_or(|l| k.as_str() > l)
                        && hi.as_deref().is_none_or(|h| k.as_str() <= h)
                })
                .count()
        };
        let spread: Vec<usize> = ranges.iter().map(|(_, lo, hi, _)| held(lo, hi)).collect();
        assert_eq!(spread.iter().sum::<usize>(), all.len(), "{spread:?}");
        let mean = all.len() as f64 / spread.len() as f64;
        let heaviest = *spread.iter().max().unwrap() as f64;
        assert!(heaviest <= 2.0 * mean, "{spread:?}");
    }

    /// Uuid keys with a dense block, a wide empty gap and a sparse tail keep every row once in ranges no heavier than twice the mean, whatever the workers and the low bit of the keys.
    #[test]
    fn a_uuid_key_with_a_gap_after_its_dense_block_is_recut_into_balanced_ranges() {
        let plan = crate::plan::test_support::test_plan();
        for low in [0u128, 1] {
            let uuid = |units: i64| {
                uuid::Uuid::from_u128(((units as u128) << 96) | low)
                    .hyphenated()
                    .to_string()
            };
            let dense = 1..=6_300i64;
            let sparse = (1..=700i64).map(|i| 1_000_000 + i * 5_368);
            for (parts, int_parts) in [(2, 32), (4, 64)] {
                let mut src = Indexed::new(
                    dense.clone().chain(sparse.clone()).map(uuid).collect(),
                    false,
                );
                let all = src.keys.clone();
                let bounds =
                    sample_key_boundaries(&mut src, &plan, "k", parts, int_parts, None, None)
                        .unwrap();
                let ranges = partition_ranges(&bounds, None, None);
                let spread: Vec<usize> = ranges
                    .iter()
                    .map(|(_, lo, hi, _)| {
                        all.iter()
                            .filter(|k| {
                                lo.as_deref().is_none_or(|l| k.as_str() > l)
                                    && hi.as_deref().is_none_or(|h| k.as_str() <= h)
                            })
                            .count()
                    })
                    .collect();
                assert_eq!(spread.iter().sum::<usize>(), all.len(), "{spread:?}");
                assert!(
                    spread.len() >= parts,
                    "{parts} workers, low bit {low}: {spread:?}"
                );
                let share = all.len() as f64 / parts as f64;
                let thin = spread.iter().filter(|&&n| n < 3).count();
                assert!(thin <= 1, "{parts} workers, low bit {low}: {spread:?}");
                let heaviest = *spread.iter().max().unwrap() as f64;
                assert!(
                    heaviest <= share,
                    "{parts} workers, low bit {low}: {spread:?}"
                );
            }
        }
    }

    /// UUIDv7 keys written in narrow bursts far apart in time keep every row once in ranges no heavier than twice the mean.
    #[test]
    fn uuid_v7_bursts_are_recut_into_balanced_ranges() {
        let plan = crate::plan::test_support::test_plan();
        let uuid = |ms: u128, seq: u128| {
            let n = (ms << 80) | (0x7u128 << 76) | (seq << 64) | 0x8000_0000_0000_0000u128;
            uuid::Uuid::from_u128(n).hyphenated().to_string()
        };
        let bursts = [1_000u128, 90_000, 91_000, 7_000_000, 7_000_500];
        let all: Vec<String> = bursts
            .iter()
            .flat_map(|&ms| (0..1_200u128).map(move |i| (ms + i / 40, i % 40)))
            .map(|(ms, seq)| uuid(ms, seq))
            .collect();
        for (parts, int_parts) in [(2, 32), (4, 64)] {
            let mut src = Indexed::new(all.clone(), false);
            let bounds =
                sample_key_boundaries(&mut src, &plan, "k", parts, int_parts, None, None).unwrap();
            let spread: Vec<usize> = partition_ranges(&bounds, None, None)
                .iter()
                .map(|(_, lo, hi, _)| {
                    all.iter()
                        .filter(|k| {
                            lo.as_deref().is_none_or(|l| k.as_str() > l)
                                && hi.as_deref().is_none_or(|h| k.as_str() <= h)
                        })
                        .count()
                })
                .collect();
            assert_eq!(spread.iter().sum::<usize>(), all.len(), "{spread:?}");
            let mean = all.len() as f64 / spread.len() as f64;
            let heaviest = *spread.iter().max().unwrap() as f64;
            assert!(
                spread.len() >= parts && heaviest <= 2.0 * mean,
                "{parts} workers: {spread:?}"
            );
        }
    }

    /// A uuid key on SQL Server, which sorts uuids in its own byte order, is never probed or recut.
    #[test]
    fn a_sql_server_uuid_key_is_not_recut() {
        let mut plan = crate::plan::test_support::test_plan();
        plan.source.source_type = SourceType::Mssql;
        let uuid = |units: i64| {
            uuid::Uuid::from_u128((units as u128) << 96)
                .hyphenated()
                .to_string()
        };
        let keys: Vec<String> = (1..=9_000i64)
            .chain((1..=1_000).map(|i| 9_000 + i * 191))
            .map(uuid)
            .collect();
        let mut src = Indexed::new(keys, false);
        src.st = SourceType::Mssql;
        sample_key_boundaries(&mut src, &plan, "k", 4, 64, None, None).unwrap();
        assert!(
            src.sql.iter().all(|q| !q.contains("OFFSET 200 ROWS")),
            "{:?}",
            src.sql
        );
    }

    /// A seeded sparse integer key, 95% dense and the rest spread wide, keeps every row once in ranges no heavier than twice the mean.
    #[test]
    fn a_seeded_sparse_key_is_recut_into_balanced_ranges() {
        let plan = crate::plan::test_support::test_plan();
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut keys: Vec<i64> = (1..=9_500).collect();
        let mut at = 9_500i64;
        for _ in 0..500 {
            at += 1 + (next() % 10_000) as i64;
            keys.push(at);
        }
        let mut src = Indexed::new(keys.iter().map(|k| k.to_string()).collect(), true);
        let all = src.keys.clone();
        let bounds = sample_key_boundaries(&mut src, &plan, "k", 4, 16, None, None).unwrap();
        let ranges = partition_ranges(&bounds, None, None);
        let held = |lo: &Option<String>, hi: &Option<String>| {
            all.iter()
                .filter(|k| {
                    lo.as_deref()
                        .is_none_or(|l| Indexed::order(true, k, l).is_gt())
                        && hi
                            .as_deref()
                            .is_none_or(|h| Indexed::order(true, k, h).is_le())
                })
                .count()
        };
        let spread: Vec<usize> = ranges.iter().map(|(_, lo, hi, _)| held(lo, hi)).collect();
        assert_eq!(spread.iter().sum::<usize>(), all.len(), "{spread:?}");
        let mean = all.len() as f64 / spread.len() as f64;
        let heaviest = *spread.iter().max().unwrap() as f64;
        assert!(heaviest <= 2.0 * mean, "{spread:?}");
    }

    /// The density probe reads rows per key from the key one seek lands on after skipping the probe rows, or from the exact count of the fewer rows the span holds.
    #[test]
    fn a_density_probe_reads_rows_per_key_from_the_landing_key() {
        let plan = crate::plan::test_support::test_plan();
        let k = crate::sql::quote_ident(plan.source.source_type, "k");
        let probe = |all: Vec<String>, start: i128, end: i128| {
            let mut src = Indexed::new(all, true);
            probe_density(&mut src, &plan, (&k, Axis::Int), &[], start, end).unwrap()
        };
        assert_eq!(probe(keys(1..=1000), 0, 1000), 200.0 / 201.0);
        assert_eq!(
            probe(keys((1..=1000).map(|i| i * 10)), 0, 10_000),
            200.0 / 2_010.0
        );
        assert_eq!(probe(keys(1..=50), 0, 1000), 50.0 / 1000.0);
        assert_eq!(probe(keys(1..=1000), 500, 1000), 200.0 / 201.0);
    }

    /// Spans without an estimated row give no cut and never loop.
    #[test]
    fn spans_without_estimated_rows_give_no_equal_row_cuts() {
        let empty = |start, end| Span {
            start,
            end,
            density: 0.0,
        };
        assert!(equal_row_cuts(&[empty(0, 1000), empty(1000, 2000)], 4).is_empty());
        assert!(equal_row_cuts(&[], 4).is_empty());
    }

    /// Equal-row cuts split a dense span and a sparse span into shares of their estimated rows.
    #[test]
    fn equal_row_cuts_split_estimated_rows_evenly() {
        let spans = [
            Span {
                start: 0,
                end: 1000,
                density: 1.0,
            },
            Span {
                start: 1000,
                end: 2000,
                density: 0.01,
            },
        ];
        assert_eq!(equal_row_cuts(&spans, 2), vec![505]);
        let later = [
            Span {
                start: 0,
                end: 100,
                density: 1.0,
            },
            Span {
                start: 100,
                end: 1100,
                density: 0.1,
            },
        ];
        assert_eq!(equal_row_cuts(&later, 4), vec![50, 600]);
        let narrow = [Span {
            start: 0,
            end: 3,
            density: 1.0,
        }];
        assert_eq!(equal_row_cuts(&narrow, 8), vec![1, 2]);
    }

    /// Every SQL dialect samples by first-row seeks alone and lands on the same boundaries, for number and text keys.
    #[test]
    fn every_dialect_samples_by_seeks_to_the_same_boundaries() {
        let mut plan = crate::plan::test_support::test_plan();
        let text = || keys(["a'0", "a'5", "b\\1", "c", "d", "e'x", "f", "g", "h"]);
        for st in [
            SourceType::Postgres,
            SourceType::Mysql,
            SourceType::Mssql,
            SourceType::Oracle,
        ] {
            plan.source.source_type = st;
            for (keys, numeric, want) in [
                (keys(1..=1000), true, keys([250, 500, 750])),
                (text(), false, keys(["c", "e'x", "g"])),
            ] {
                let mut src = Indexed::new(keys, numeric);
                src.st = st;
                let got = sample_key_boundaries(&mut src, &plan, "k", 4, 4, None, None).unwrap();
                assert_eq!(got, want, "{st:?}: {:?}", src.sql);
                assert_eq!(src.sql.len(), if numeric { 9 } else { 5 }, "{st:?}");
            }
        }
    }

    /// Evenly spread keys split into even ranges, whatever the table size.
    #[test]
    fn evenly_spread_keys_split_into_even_ranges() {
        let plan = crate::plan::test_support::test_plan();
        for n in [1_000i64, 100_000] {
            let mut src = Indexed::new(keys((0..n).map(|i| i * 7)), true);
            let got = sample_key_boundaries(&mut src, &plan, "k", 4, 4, None, None).unwrap();
            let rows: Vec<i64> = got.iter().map(|b| b.parse::<i64>().unwrap() / 7).collect();
            assert_eq!(rows, vec![n / 4, n / 2, 3 * n / 4], "{got:?}");
            assert_eq!(src.sql.len(), 9);
        }
    }

    /// A composite key's sampled lead bounds become one-value tuples; a single key's stay bare values.
    #[test]
    fn a_composite_keys_ranges_are_bounded_by_lead_tuples() {
        let sampled = || {
            vec![
                (0, None, Some("b".to_string()), false),
                (1, Some("b".to_string()), None, false),
            ]
        };
        assert_eq!(
            lead_bounds(sampled(), &["kind".into()]),
            vec![
                (0, None, Some(r#"["b"]"#.to_string()), false),
                (1, Some(r#"["b"]"#.to_string()), None, false),
            ]
        );
        assert_eq!(lead_bounds(sampled(), &[]), sampled());
    }

    /// A page the sink rotated hands back the part that landed before a later part failed.
    #[test]
    fn a_page_that_fails_part_way_still_hands_back_its_durable_part() {
        let mut plan = crate::plan::test_support::test_plan();
        plan.partition_rollover = Some(crate::plan::rollover::PartitionRollover {
            column: "d".into(),
            granularity: crate::config::load::Granularity::Day,
            cap: 1,
        });
        let key = IncrementalCursorPlan {
            primary_column: "d".into(),
            fallback_column: None,
            mode: IncrementalCursorMode::SingleColumn,
            settle: None,
        };
        let dest = crate::pipeline::commit::tests::NthWriteFails::after(1);
        let (parts, page) = read_keyset_page_bounded(
            &mut ThreeDays,
            &plan,
            &key,
            10,
            None,
            None,
            &dest,
            "k.parquet",
        );
        assert!(
            page.is_err(),
            "the refused second part is the page's outcome"
        );
        assert_eq!(
            parts
                .iter()
                .map(|p| p.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["k_p0.parquet"],
            "the durable first part must survive the failure"
        );
    }

    /// `next_seek` and the resume's gone-parts refusal carry their codes.
    #[test]
    fn a_coarse_key_an_unreadable_key_and_gone_pages_are_refused_by_code() {
        let ts = "2024-01-01 00:00:00.000001";
        let err = next_seek(
            Some(ts),
            10,
            10,
            Some(ts.to_string()),
            "ts",
            3,
            "export 'o'",
        )
        .unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_KEYSET_KEY_COARSE", 5);
        let err = next_seek(None, 10, 10, None, "ts", 3, "export 'o'").unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_PROBE_UNREADABLE", 5);
        let err = committed_pages_gone("orders", &["orders_pk_w0_1.parquet"]);
        crate::error::assert_coded(&err, "RIVET_DEST_COMMITTED_PART_GONE", 5);
        assert!(format!("{err}").contains("--export orders`"), "{err}");
    }
}
