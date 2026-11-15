//! The partitions a load touches, estimated from the Parquet footers it is about to load.

use crate::destination::gcs::GcsStore;
use crate::load;
use crate::load::plan::{Granularity, PartitionKey, TablePartition};
use anyhow::{Context, Result};
use chrono::{DateTime, Datelike};
use parquet::basic::{LogicalType, TimeUnit, TimestampType, Type as PhysicalType};
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{FooterTail, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::statistics::Statistics;

pub(crate) use crate::plan::rollover::MAX_PARTITIONS_PER_JOB;

/// The unit a partition column's values are stored in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    Days,
    Millis,
    Micros,
    Nanos,
    Plain,
}

/// A column's `[lo, hi]` over the files, in its stored unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Span {
    lo: i64,
    hi: i64,
    unit: Unit,
}

/// One file to pack: its span, its rows, and whether any value is NULL.
struct Part {
    uri: String,
    span: Span,
    rows: i64,
    has_nulls: bool,
}

/// The partitions a file or batch writes: its span's buckets, one more for the NULL partition, and never more than its rows.
fn budgeted(key: &PartitionKey, span: Span, rows: i64, has_nulls: bool) -> i64 {
    (partitions_touched(key, span) + i64::from(has_nulls)).min(rows)
}

/// Refuse a load one of whose files alone spans more partitions than one BigQuery job may write.
pub(crate) fn check_partition_budget(
    store: &GcsStore,
    uris: &[String],
    partition: &TablePartition,
) -> Result<()> {
    plan_load_batches(store, uris, partition).map(|_| ())
}

/// The load jobs this Parquet needs: files packed, in order of their lowest value, into batches whose combined span stays within BigQuery's per-job partition cap.
pub(crate) fn plan_load_batches(
    store: &GcsStore,
    uris: &[String],
    partition: &TablePartition,
) -> Result<Vec<Vec<String>>> {
    let Some(column) = partition.key.column() else {
        return Ok(vec![uris.to_vec()]);
    };
    let mut spanned: Vec<Part> = Vec::new();
    let mut blind: Vec<String> = Vec::new();
    for uri in uris {
        let (_, key) = crate::load::split_object_uri(uri)?;
        let meta = read_footer(store, key)
            .with_context(|| format!("reading the Parquet footer of {uri}"))?;
        let rows = meta.file_metadata().num_rows();
        let rows = footer_buckets(&meta, &partition.key).map_or(rows, |b| rows.min(b));
        match column_span(&meta, column) {
            ColumnStats::Span(span, has_nulls) => spanned.push(Part {
                uri: uri.clone(),
                span,
                rows,
                has_nulls,
            }),
            ColumnStats::NoValues | ColumnStats::Unknown => blind.push(uri.clone()),
        }
    }
    let mut batches = pack_batches(&partition.key, spanned)?;
    if !blind.is_empty() {
        match batches.first_mut() {
            Some(first) => first.extend(blind),
            None => batches.push(blind),
        }
    }
    Ok(batches)
}

/// Pure packing: sort by the low end, then extend the current batch while the partitions it would write stay within the cap.
fn pack_batches(key: &PartitionKey, mut files: Vec<Part>) -> Result<Vec<Vec<String>>> {
    files.sort_by_key(|p| p.span.lo);
    let mut batches: Vec<Vec<String>> = Vec::new();
    let mut current: Option<(Vec<String>, Span, i64, bool)> = None;
    for Part {
        uri,
        span,
        rows,
        has_nulls,
    } in files
    {
        let alone = budgeted(key, span, rows, has_nulls);
        if alone > MAX_PARTITIONS_PER_JOB {
            crate::rivet_bail!(
                crate::error::codes::LOAD_PARTITION_BUDGET_EXCEEDED,
                "{uri} alone {} — no batching splits one file",
                over_budget_message(key, span, alone)
            );
        }
        current = Some(match current {
            None => (vec![uri], span, rows, has_nulls),
            Some((mut uris, cur, cur_rows, cur_nulls)) => {
                let merged = merge(cur, span);
                let merged_rows = cur_rows.saturating_add(rows);
                let merged_nulls = cur_nulls || has_nulls;
                if budgeted(key, merged, merged_rows, merged_nulls) > MAX_PARTITIONS_PER_JOB {
                    batches.push(uris);
                    (vec![uri], span, rows, has_nulls)
                } else {
                    uris.push(uri);
                    (uris, merged, merged_rows, merged_nulls)
                }
            }
        });
    }
    if let Some((uris, ..)) = current {
        batches.push(uris);
    }
    Ok(batches)
}

/// The distinct partitions the writer recorded for this file, when it budgeted the same column at the same granularity the load partitions by.
fn footer_buckets(meta: &ParquetMetaData, key: &PartitionKey) -> Option<i64> {
    use crate::plan::rollover::{PARTITION_BUCKETS_KEY, parse_partition_buckets_note};
    let PartitionKey::Time {
        column: Some(column),
        granularity,
    } = key
    else {
        return None;
    };
    let note = meta
        .file_metadata()
        .key_value_metadata()?
        .iter()
        .find(|kv| kv.key == PARTITION_BUCKETS_KEY)?
        .value
        .as_deref()?;
    let (c, g, n) = parse_partition_buckets_note(note)?;
    (c == column && g == granularity.as_str()).then_some(n)
}

/// Bytes of an object's end read in one request; a footer that fits needs no second read.
const FOOTER_GUESS: u64 = 65_536;

/// The footer of the Parquet object at the bucket-relative `key`.
pub(crate) fn read_footer(store: &GcsStore, key: &str) -> Result<ParquetMetaData> {
    read_footer_guessing(store, key, FOOTER_GUESS)
}

/// [`read_footer`] with the size of its first read given.
fn read_footer_guessing(store: &GcsStore, key: &str, guess: u64) -> Result<ParquetMetaData> {
    let size = store.stat_size(key)?;
    let tail_len = FOOTER_SIZE as u64;
    if size < tail_len {
        crate::rivet_bail!(
            crate::error::codes::LOAD_STAGING_OBJECT_UNSAFE,
            "{key} is {size} bytes, too short to be a Parquet file"
        );
    }
    let guess = size.min(guess.max(tail_len));
    let end = store.read_range(key, size - guess, guess)?;
    let tail: [u8; FOOTER_SIZE] = end
        .get(end.len().saturating_sub(FOOTER_SIZE)..)
        .unwrap_or_default()
        .try_into()
        .context("the Parquet footer tail is not 8 bytes")?;
    let len = FooterTail::try_new(&tail)?.metadata_length() as u64;
    if size < tail_len + len {
        crate::rivet_bail!(
            crate::error::codes::LOAD_STAGING_OBJECT_UNSAFE,
            "{key} declares a {len}-byte footer but is {size} bytes long"
        );
    }
    if let Some(metadata) = metadata_in(&end, len) {
        return Ok(ParquetMetaDataReader::decode_metadata(metadata)?);
    }
    let bytes = store.read_range(key, size - tail_len - len, len)?;
    Ok(ParquetMetaDataReader::decode_metadata(&bytes)?)
}

/// The `len` metadata bytes that sit before the 8-byte tail of `end`, an object's last bytes.
fn metadata_in(end: &[u8], len: u64) -> Option<&[u8]> {
    let stop = end.len().checked_sub(FOOTER_SIZE)?;
    let start = stop.checked_sub(usize::try_from(len).ok()?)?;
    end.get(start..stop)
}

/// What one file's footer says about a column's non-NULL values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnStats {
    /// Their `[lo, hi]`, and whether any row is known to be NULL.
    Span(Span, bool),
    /// Every row group is known to hold only NULLs (or no rows).
    NoValues,
    /// Absent, of a type no partition takes, or a row group the footer does not bound.
    Unknown,
}

/// `column`'s span over every row group of one file; an all-NULL row group adds no values.
fn column_span(meta: &ParquetMetaData, column: &str) -> ColumnStats {
    let schema = meta.file_metadata().schema_descr();
    match schema.columns().iter().position(|c| c.name() == column) {
        Some(idx) => column_span_at(meta, idx),
        None => ColumnStats::Unknown,
    }
}

/// The span of the leaf column at `idx` over every row group of one file.
fn column_span_at(meta: &ParquetMetaData, idx: usize) -> ColumnStats {
    let schema = meta.file_metadata().schema_descr();
    let descr = schema.column(idx);
    let Some(unit) = stored_unit(descr.logical_type_ref(), descr.physical_type()) else {
        return ColumnStats::Unknown;
    };
    let mut span: Option<Span> = None;
    let mut nulls = false;
    for rg in meta.row_groups() {
        let Some(stats) = rg.column(idx).statistics() else {
            return ColumnStats::Unknown;
        };
        let null_count = stats.null_count_opt();
        nulls |= null_count.is_some_and(|n| n > 0);
        let bounds = match stats {
            Statistics::Int32(s) => s
                .min_opt()
                .zip(s.max_opt())
                .map(|(lo, hi)| (i64::from(*lo), i64::from(*hi))),
            Statistics::Int64(s) => s.min_opt().zip(s.max_opt()).map(|(lo, hi)| (*lo, *hi)),
            _ => return ColumnStats::Unknown,
        };
        let Some((lo, hi)) = bounds else {
            if null_count.is_some_and(|n| i64::try_from(n) == Ok(rg.num_rows())) {
                continue;
            }
            return ColumnStats::Unknown;
        };
        let file = Span { lo, hi, unit };
        span = Some(match span {
            None => file,
            Some(s) => merge(s, file),
        });
    }
    span.map_or(ColumnStats::NoValues, |s| ColumnStats::Span(s, nulls))
}

/// The first timestamp column the footer does not show inside `[lo_secs, hi_secs]` (Unix seconds).
pub(crate) fn timestamp_outside(
    meta: &ParquetMetaData,
    lo_secs: i64,
    hi_secs: i64,
) -> Option<(String, Option<String>)> {
    let schema = meta.file_metadata().schema_descr();
    schema.columns().iter().enumerate().find_map(|(idx, c)| {
        if !matches!(c.logical_type_ref(), Some(LogicalType::Timestamp(_))) {
            return None;
        }
        let name = c.path().string();
        let span = match column_span_at(meta, idx) {
            ColumnStats::Span(span, _) => span,
            ColumnStats::NoValues => return None,
            ColumnStats::Unknown => return Some((name, None)),
        };
        let end = if to_seconds(span.lo, span.unit) < lo_secs {
            span.lo
        } else if to_seconds(span.hi, span.unit) > hi_secs {
            span.hi
        } else {
            return None;
        };
        Some((name, Some(render(end, span.unit))))
    })
}

/// The unit a partitionable column's statistics are in, by its Parquet type.
fn stored_unit(logical: Option<&LogicalType>, physical: PhysicalType) -> Option<Unit> {
    match (logical, physical) {
        (Some(LogicalType::Date), _) => Some(Unit::Days),
        (Some(LogicalType::Timestamp(TimestampType { unit, .. })), _) => Some(match unit {
            TimeUnit::MILLIS => Unit::Millis,
            TimeUnit::MICROS => Unit::Micros,
            TimeUnit::NANOS => Unit::Nanos,
        }),
        (None, PhysicalType::INT64 | PhysicalType::INT32)
        | (Some(LogicalType::Integer { .. }), PhysicalType::INT64 | PhysicalType::INT32) => {
            Some(Unit::Plain)
        }
        _ => None,
    }
}

/// The span covering both.
fn merge(a: Span, b: Span) -> Span {
    Span {
        lo: a.lo.min(b.lo),
        hi: a.hi.max(b.hi),
        unit: a.unit,
    }
}

/// A stored value as Unix seconds.
fn to_seconds(v: i64, unit: Unit) -> i64 {
    match unit {
        Unit::Days => v * 86_400,
        Unit::Millis => v.div_euclid(1_000),
        Unit::Micros => v.div_euclid(1_000_000),
        Unit::Nanos => v.div_euclid(1_000_000_000),
        Unit::Plain => v,
    }
}

/// How many partitions of `key` the values in `span` fall into.
fn partitions_touched(key: &PartitionKey, span: Span) -> i64 {
    match key {
        PartitionKey::Time { granularity, .. } => buckets_between(
            *granularity,
            to_seconds(span.lo, span.unit),
            to_seconds(span.hi, span.unit),
        ),
        PartitionKey::Range {
            start,
            end,
            interval,
            ..
        } => {
            let bucket = |v: i64| (v - start).div_euclid(*interval);
            let inside = |v: i64| v >= *start && v < *end;
            let (lo, hi) = (span.lo.max(*start), span.hi.min(end - 1));
            let within = if lo <= hi {
                bucket(hi) - bucket(lo) + 1
            } else {
                0
            };
            let outside = i64::from(!inside(span.lo) || !inside(span.hi));
            within + outside
        }
    }
}

/// The time partitions between two Unix seconds, inclusive.
fn buckets_between(granularity: Granularity, lo: i64, hi: i64) -> i64 {
    let (lo, hi) = (lo.min(hi), lo.max(hi));
    match granularity {
        Granularity::Hour => hi.div_euclid(3_600) - lo.div_euclid(3_600) + 1,
        Granularity::Day => hi.div_euclid(86_400) - lo.div_euclid(86_400) + 1,
        Granularity::Month => months_since_epoch(hi) - months_since_epoch(lo) + 1,
        Granularity::Year => i64::from(year_of(hi)) - i64::from(year_of(lo)) + 1,
    }
}

fn year_of(secs: i64) -> i32 {
    DateTime::from_timestamp(secs, 0).map_or(1970, |t| t.year())
}

fn months_since_epoch(secs: i64) -> i64 {
    DateTime::from_timestamp(secs, 0)
        .map_or(0, |t| i64::from(t.year()) * 12 + i64::from(t.month0()))
}

/// `2026-09-01 10:00` for a time value, the plain number for a range one.
fn render(v: i64, unit: Unit) -> String {
    match unit {
        Unit::Plain => v.to_string(),
        unit => DateTime::from_timestamp(to_seconds(v, unit), 0)
            .map_or_else(|| v.to_string(), |t| t.format("%Y-%m-%d %H:%M").to_string()),
    }
}

/// Why the load is refused, naming the coarser granularity that would fit.
fn over_budget_message(key: &PartitionKey, span: Span, touched: i64) -> String {
    let (lo, hi) = (render(span.lo, span.unit), render(span.hi, span.unit));
    match key {
        PartitionKey::Time {
            column,
            granularity,
        } => {
            let column = column.as_deref().unwrap_or("load time");
            let coarser = granularity.coarser().map_or_else(
                || "load a narrower range".to_string(),
                |g| {
                    let n = buckets_between(
                        g,
                        to_seconds(span.lo, span.unit),
                        to_seconds(span.hi, span.unit),
                    );
                    format!(
                        "use `granularity: {}` (about {n}) or load a narrower range",
                        g.as_str()
                    )
                },
            );
            format!(
                "spans about {touched} {} partitions of `{column}` ({lo} to {hi}); \
                 BigQuery writes at most {MAX_PARTITIONS_PER_JOB} partitions per job — {coarser}",
                granularity.as_str()
            )
        }
        PartitionKey::Range {
            column, interval, ..
        } => format!(
            "spans about {touched} ranges of `{column}` ({lo} to {hi}, {interval} wide); \
             BigQuery writes at most {MAX_PARTITIONS_PER_JOB} partitions per job — widen \
             `range.interval` or load a narrower range"
        ),
    }
}

/// The URIs the partition budget applies to: the files that land in the PARTITIONED target.
pub(crate) fn budgeted_uris(
    layout: load::plan::CdcLayout,
    runs: &[(String, crate::manifest::RunManifest)],
    uris: &[String],
) -> Vec<String> {
    if !layout.log_is_disposable() {
        return uris.to_vec();
    }
    let baseline: Vec<(String, crate::manifest::RunManifest)> = runs
        .iter()
        .filter(|(_, m)| load::orchestrate::is_baseline_leg(m))
        .cloned()
        .collect();
    if baseline.is_empty() {
        return Vec::new();
    }
    let keys: Vec<String> = uris
        .iter()
        .filter_map(|u| load::split_object_uri(u).ok().map(|(_, k)| k.to_string()))
        .collect();
    let want: std::collections::HashSet<String> =
        load::reconcile::select_load_keys(&baseline, &keys)
            .into_iter()
            .collect();
    uris.iter()
        .filter(|u| load::split_object_uri(u).is_ok_and(|(_, k)| want.contains(k)))
        .cloned()
        .collect()
}

/// The pre-load partition budget of a BigQuery plan (ADR-0034 D4).
pub(crate) fn partition_budget_ok(
    store: &crate::destination::gcs::GcsStore,
    plan: &load::plan::LoadPlan,
    uris: &[String],
) -> Result<()> {
    match &plan.partition {
        Some(partition) if plan.load.target.budgets_partitions() => {
            load::partition_budget::check_partition_budget(store, uris, partition)
                .with_context(|| format!("export `{}`", plan.export_name))
        }
        _ => Ok(()),
    }
}

/// The partition a load declares, for the progress line.
pub(crate) fn partition_label(plan: &load::plan::LoadPlan) -> String {
    plan.partition
        .as_ref()
        .map_or_else(|| "none".to_string(), |p| p.key.describe())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Date32Array, Int16Array, Int32Array, Int64Array, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit as ArrowUnit};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use std::sync::Arc;

    const HOUR: i64 = 3_600;
    const DAY: i64 = 86_400;

    fn micros(secs: i64) -> i64 {
        secs * 1_000_000
    }

    /// Unix seconds of a UTC date.
    fn at(y: i32, m: u32, d: u32, h: u32) -> i64 {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp()
    }

    fn write(dir: &std::path::Path, name: &str, field: Field, column: ArrayRef, stats: bool) {
        write_noted(dir, name, field, column, stats, None);
    }

    /// The metadata is cut from the bytes in hand only when all of it is there.
    #[test]
    fn the_metadata_is_taken_from_the_first_read_only_when_it_holds_all_of_it() {
        let end = b"..METADATAtailtail";
        assert_eq!(metadata_in(end, 8), Some(&b"METADATA"[..]));
        assert_eq!(
            metadata_in(end, 10),
            Some(&b"..METADATA"[..]),
            "exactly the whole read"
        );
        assert_eq!(
            metadata_in(end, 11),
            None,
            "one byte more than the read holds"
        );
        assert_eq!(metadata_in(end, 0), Some(&b""[..]));
        assert_eq!(
            metadata_in(b"tail", 0),
            None,
            "shorter than the tail itself"
        );
    }

    /// The footer reads the same whether the first read holds all of it, exactly it, or only its tail.
    #[test]
    fn a_footer_longer_than_the_first_read_takes_a_second_one() {
        let dir = tempfile::tempdir().unwrap();
        let (field, column) = ts_column(&[at(2024, 1, 1, 0), at(2024, 1, 2, 0)]);
        write(dir.path(), "p.parquet", field, column, true);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let size = store.stat_size("p.parquet").unwrap();
        let tail = store.read_range("p.parquet", size - 8, 8).unwrap();
        let len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as u64;
        let whole = read_footer(&store, "p.parquet").unwrap();
        assert_eq!(whole.file_metadata().num_rows(), 2);
        for guess in [0, 8, len + 7, len + 8, len + 9, size, size + 1] {
            let got = read_footer_guessing(&store, "p.parquet", guess).unwrap();
            assert_eq!(
                (got.file_metadata().num_rows(), got.num_row_groups()),
                (2, whole.num_row_groups()),
                "first read of {guess} bytes"
            );
            assert_eq!(
                timestamp_outside(&got, at(2024, 1, 1, 0), at(2024, 1, 1, 12)),
                timestamp_outside(&whole, at(2024, 1, 1, 0), at(2024, 1, 1, 12)),
                "first read of {guess} bytes"
            );
        }
    }

    #[test]
    fn a_timestamp_past_either_end_of_a_range_is_named_with_its_value() {
        let dir = tempfile::tempdir().unwrap();
        let (lo, hi) = (at(1900, 1, 1, 0), at(2299, 12, 31, 23));
        for (name, secs, stats) in [
            ("inside.parquet", vec![lo, hi], true),
            ("late.parquet", vec![lo, at(9999, 12, 31, 0)], true),
            ("early.parquet", vec![at(1850, 1, 1, 0), hi], true),
            ("unknown.parquet", vec![at(9999, 12, 31, 0)], false),
        ] {
            let (field, column) = ts_column(&secs);
            write(dir.path(), name, field, column, stats);
        }
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let check =
            |name: &str| timestamp_outside(&read_footer(&store, name).unwrap(), lo, hi + HOUR - 1);
        assert_eq!(check("inside.parquet"), None);
        assert_eq!(
            timestamp_outside(&read_footer(&store, "inside.parquet").unwrap(), lo, hi),
            None,
            "a value exactly at either end is inside"
        );
        assert_eq!(
            check("late.parquet"),
            Some(("ts".to_string(), Some("9999-12-31 00:00".to_string())))
        );
        assert_eq!(
            check("early.parquet"),
            Some(("ts".to_string(), Some("1850-01-01 00:00".to_string())))
        );
        assert_eq!(
            check("unknown.parquet"),
            Some(("ts".to_string(), None)),
            "no statistics is named, never passed"
        );
    }

    /// Footer metadata of list columns named `names`, each holding one row of `lists[i]` timestamps (texts when `None`).
    fn list_columns_meta(columns: &[(&str, Option<Vec<i64>>)]) -> ParquetMetaData {
        use arrow::array::{Array, ListBuilder, StringBuilder, TimestampMicrosecondBuilder};
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for (name, secs) in columns {
            match secs {
                None => {
                    let mut b = ListBuilder::new(StringBuilder::new());
                    b.values().append_value("x");
                    b.append(true);
                    let a = b.finish();
                    fields.push(Field::new(*name, a.data_type().clone(), true));
                    arrays.push(Arc::new(a));
                }
                Some(secs) => {
                    let mut b =
                        ListBuilder::new(TimestampMicrosecondBuilder::new().with_timezone("UTC"));
                    for s in secs {
                        b.values().append_value(micros(*s));
                    }
                    b.append(true);
                    let a = b.finish();
                    fields.push(Field::new(*name, a.data_type().clone(), true));
                    arrays.push(Arc::new(a));
                }
            }
        }
        let schema = Arc::new(Schema::new(fields));
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
        w.write(&RecordBatch::try_new(schema, arrays).unwrap())
            .unwrap();
        w.close().unwrap();
        ParquetMetaDataReader::new()
            .parse_and_finish(&bytes::Bytes::from(buf))
            .unwrap()
    }

    /// A timestamp list column is checked by its own footer leaf, never by the first leaf that shares its element name.
    #[test]
    fn a_timestamp_list_is_checked_by_its_own_leaf_not_by_the_name_item() {
        let (lo, hi) = (at(1900, 1, 1, 0), at(2299, 12, 31, 23));
        let late = at(9999, 12, 31, 0);
        let inside = at(2024, 1, 1, 0);
        assert_eq!(
            timestamp_outside(
                &list_columns_meta(&[("tags", None), ("seen", Some(vec![inside]))]),
                lo,
                hi
            ),
            None,
            "a text list before it must not stand in for its statistics"
        );
        assert_eq!(
            timestamp_outside(
                &list_columns_meta(&[("a", Some(vec![inside])), ("b", Some(vec![inside, late]))]),
                lo,
                hi
            ),
            Some((
                "b.list.item".to_string(),
                Some("9999-12-31 00:00".to_string())
            )),
            "the second list is checked, not the first one twice"
        );
    }

    /// Footer metadata of one `ts` column written as one row group per entry of `groups`.
    fn row_groups_meta(groups: &[Vec<Option<i64>>]) -> ParquetMetaData {
        let field = Field::new(
            "ts",
            DataType::Timestamp(ArrowUnit::Microsecond, Some("UTC".into())),
            true,
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
        for g in groups {
            let values: Vec<Option<i64>> = g.iter().map(|s| s.map(micros)).collect();
            let col: ArrayRef =
                Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"));
            w.write(&RecordBatch::try_new(schema.clone(), vec![col]).unwrap())
                .unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
        let meta = ParquetMetaDataReader::new()
            .parse_and_finish(&bytes::Bytes::from(buf))
            .unwrap();
        assert_eq!(meta.num_row_groups(), groups.len());
        meta
    }

    /// An all-NULL row group has no min/max; it must not hide an out-of-range value in another row group of the same part.
    #[test]
    fn an_all_null_row_group_does_not_hide_an_out_of_range_timestamp() {
        let (lo, hi) = (at(1900, 1, 1, 0), at(2299, 12, 31, 23));
        let late = Some(at(9999, 12, 31, 0));
        for groups in [
            vec![vec![None, None], vec![late]],
            vec![vec![late], vec![None]],
        ] {
            assert_eq!(
                timestamp_outside(&row_groups_meta(&groups), lo, hi),
                Some(("ts".to_string(), Some("9999-12-31 00:00".to_string()))),
                "{groups:?}"
            );
        }
    }

    /// The budget spans the bounded row groups and counts an all-NULL one as the NULL partition.
    #[test]
    fn an_all_null_row_group_is_the_null_partition_not_an_unknown_file() {
        let (a, b) = (at(2020, 1, 1, 0), at(2020, 1, 3, 0));
        let meta = row_groups_meta(&[vec![None], vec![Some(a), Some(b)]]);
        let ColumnStats::Span(span, nulls) = column_span(&meta, "ts") else {
            panic!("{:?}", column_span(&meta, "ts"));
        };
        assert_eq!((span.lo, span.hi, nulls), (micros(a), micros(b), true));
        assert_eq!(
            budgeted(&time("ts", Granularity::Day).key, span, 10, nulls),
            4,
            "three days plus the NULL partition"
        );
        assert_eq!(
            column_span(&row_groups_meta(&[vec![None], vec![None, None]]), "ts"),
            ColumnStats::NoValues
        );
    }

    #[test]
    fn the_budget_applies_only_to_a_partitioned_bigquery_plan_and_names_the_export() {
        use crate::load::plan::{LoadMode, test_plan};
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        let days: Vec<i64> = (0..4100).map(|i| start + i * DAY).collect();
        let (field, column) = ts_column(&days);
        write_noted(dir.path(), "wide.parquet", field, column, true, None);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let uris = vec!["gs://b/wide.parquet".to_string()];

        let mut plan = test_plan(LoadMode::Full, "gs://b/");
        assert_eq!(super::partition_label(&plan), "none");
        assert!(
            super::partition_budget_ok(&store, &plan, &uris).is_ok(),
            "no partition, no cap"
        );

        plan.partition = Some(time("ts", Granularity::Day));
        assert_eq!(
            super::partition_label(&plan),
            plan.partition.as_ref().unwrap().key.describe()
        );
        let err = super::partition_budget_ok(&store, &plan, &uris).unwrap_err();
        assert!(format!("{err:#}").contains("export `orders`"), "{err:#}");

        plan.load.target = crate::load::plan::LoadTarget::Clickhouse {
            url: "http://ch:8123".into(),
            database: "d".into(),
            user: "u".into(),
            password_env: "P".into(),
            named_collection: None,
            ca_file: None,
            allow_plaintext: false,
        };
        assert!(
            super::partition_budget_ok(&store, &plan, &uris).is_ok(),
            "only BigQuery caps the partitions one job writes"
        );
    }

    fn write_noted(
        dir: &std::path::Path,
        name: &str,
        field: Field,
        column: ArrayRef,
        stats: bool,
        note: Option<&str>,
    ) {
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
        let props = WriterProperties::builder()
            .set_statistics_enabled(if stats {
                EnabledStatistics::Chunk
            } else {
                EnabledStatistics::None
            })
            .set_key_value_metadata(note.map(|n| {
                vec![parquet::file::metadata::KeyValue::new(
                    crate::plan::rollover::PARTITION_BUCKETS_KEY.to_string(),
                    n.to_string(),
                )]
            }))
            .build();
        let path = dir.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    fn ts_column(secs: &[i64]) -> (Field, ArrayRef) {
        let field = Field::new(
            "ts",
            DataType::Timestamp(ArrowUnit::Microsecond, Some("UTC".into())),
            false,
        );
        let values: Vec<i64> = secs.iter().map(|s| micros(*s)).collect();
        let column: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"));
        (field, column)
    }

    fn ts_file(dir: &std::path::Path, name: &str, secs: &[i64], stats: bool) {
        let (field, column) = ts_column(secs);
        write(dir, name, field, column, stats);
    }

    /// The writer rotates parts at 4,000 DISTINCT partitions, and a part cut there over a gappy history spans far more calendar days than it occupies.
    #[test]
    fn a_part_the_writer_budgeted_is_bounded_by_the_count_it_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        let mut days: Vec<i64> = (0..4000).map(|i| start + i * 2 * DAY).collect();
        days.extend((0..100).map(|i| start + i * 2 * DAY));
        let key = time("ts", Granularity::Day);
        let uri = |n: &str| vec![format!("gs://b/{n}")];

        let (field, column) = ts_column(&days);
        write_noted(dir.path(), "bare.parquet", field, column, true, None);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let err = plan_load_batches(&store, &uri("bare.parquet"), &key)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("alone spans about 4100 day partitions"),
            "without the note the rows (4,100) are the tighter bound, and still over: {err}"
        );

        let (field, column) = ts_column(&days);
        write_noted(
            dir.path(),
            "noted.parquet",
            field,
            column,
            true,
            Some("ts|day|4000"),
        );
        assert_eq!(
            plan_load_batches(&store, &uri("noted.parquet"), &key).unwrap(),
            vec![uri("noted.parquet")],
            "4,000 recorded partitions fit one job, whatever the span"
        );

        for foreign in ["other|day|4000", "ts|month|4000"] {
            let (field, column) = ts_column(&days);
            write_noted(
                dir.path(),
                "foreign.parquet",
                field,
                column,
                true,
                Some(foreign),
            );
            assert!(
                plan_load_batches(&store, &uri("foreign.parquet"), &key).is_err(),
                "a note for {foreign} must not bound a DAY load of `ts`"
            );
        }
    }

    fn time(column: &str, granularity: Granularity) -> TablePartition {
        TablePartition {
            key: PartitionKey::Time {
                column: Some(column.into()),
                granularity,
            },
            expr: String::new(),
            expiration_days: None,
            require_filter: false,
        }
    }

    fn check(dir: &std::path::Path, names: &[&str], partition: &TablePartition) -> Result<()> {
        let store = GcsStore::open_fs(dir.to_str().unwrap()).unwrap();
        let uris: Vec<String> = names.iter().map(|n| format!("gs://b/{n}")).collect();
        check_partition_budget(&store, &uris, partition)
    }

    #[test]
    fn a_span_within_the_job_cap_passes_and_a_hourly_one_over_it_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2026, 1, 1, 0);
        ts_file(dir.path(), "a.parquet", &[start, start + 3 * DAY], true);
        let hourly: Vec<i64> = (0..=4680).map(|h| start + 5 * DAY + h * HOUR).collect();
        ts_file(dir.path(), "b.parquet", &hourly, true);
        let files = ["a.parquet", "b.parquet"];
        assert!(check(dir.path(), &files, &time("ts", Granularity::Day)).is_ok());
        let err = check(dir.path(), &files, &time("ts", Granularity::Hour))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("gs://b/b.parquet alone spans about 4681 hour partitions of `ts`"),
            "{err}"
        );
        assert!(
            err.contains("(2026-01-06 00:00 to 2026-07-20 00:00)"),
            "{err}"
        );
        assert!(err.contains("use `granularity: day` (about 196)"), "{err}");
    }

    /// The cap is inclusive: a file (or a batch) touching EXACTLY 4,000 partitions is one job.
    #[test]
    fn exactly_the_cap_is_one_job_and_one_more_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        let daily = |n: i64| -> Vec<i64> { (0..n).map(|d| start + d * DAY).collect() };
        ts_file(dir.path(), "cap.parquet", &daily(4000), true);
        ts_file(dir.path(), "over.parquet", &daily(4001), true);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let key = time("ts", Granularity::Day);
        assert_eq!(
            plan_load_batches(&store, &["gs://b/cap.parquet".to_string()], &key).unwrap(),
            vec![vec!["gs://b/cap.parquet".to_string()]],
            "4,000 day partitions fit one job"
        );
        let err = plan_load_batches(&store, &["gs://b/over.parquet".to_string()], &key)
            .unwrap_err()
            .to_string();
        assert!(err.contains("alone spans about 4001"), "{err}");
    }

    fn ts_nullable(dir: &std::path::Path, name: &str, secs: &[Option<i64>]) {
        let field = Field::new(
            "ts",
            DataType::Timestamp(ArrowUnit::Microsecond, Some("UTC".into())),
            true,
        );
        let values: Vec<Option<i64>> = secs.iter().map(|s| s.map(micros)).collect();
        let column: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"));
        write(dir, name, field, column, true);
    }

    /// NULL values land in a partition of their own, which `min`/`max` cannot see.
    #[test]
    fn a_null_value_occupies_a_partition_of_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        let daily = |from: i64, to: i64| -> Vec<Option<i64>> {
            (from..to).map(|d| Some(start + d * DAY)).collect()
        };
        let key = time("ts", Granularity::Day);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let uris = |names: &[&str]| -> Vec<String> {
            names.iter().map(|n| format!("gs://b/{n}")).collect()
        };

        let mut with_nulls = daily(0, 4000);
        with_nulls.extend([None; 5]);
        ts_nullable(dir.path(), "nulls.parquet", &with_nulls);
        let err = plan_load_batches(&store, &uris(&["nulls.parquet"]), &key)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("alone spans about 4001 day partitions"),
            "4,000 days plus the NULL partition: {err}"
        );

        let mut clean = daily(0, 4000);
        clean.push(Some(start));
        ts_nullable(dir.path(), "clean.parquet", &clean);
        assert_eq!(
            plan_load_batches(&store, &uris(&["clean.parquet"]), &key).unwrap(),
            vec![uris(&["clean.parquet"])],
            "a nullable column holding no NULL is not charged a partition"
        );

        let mut a = daily(0, 2000);
        a.extend([None; 3]);
        ts_nullable(dir.path(), "a.parquet", &a);
        ts_nullable(dir.path(), "b.parquet", &daily(2000, 4000));
        assert_eq!(
            plan_load_batches(&store, &uris(&["a.parquet", "b.parquet"]), &key).unwrap(),
            vec![uris(&["a.parquet"]), uris(&["b.parquet"])],
            "4,000 days plus the NULL partition is 4,001: two jobs, not one"
        );
    }

    /// A file whose values are SCATTERED over a huge range occupies one partition per distinct value, not one per day of its range.
    #[test]
    fn a_scattered_file_is_bounded_by_its_rows_not_its_span() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        let scattered: Vec<i64> = (0..1001).map(|i| start + i * 9 * DAY).collect();
        ts_file(dir.path(), "scattered.parquet", &scattered, true);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let key = time("ts", Granularity::Day);
        assert_eq!(
            plan_load_batches(&store, &["gs://b/scattered.parquet".to_string()], &key).unwrap(),
            vec![vec!["gs://b/scattered.parquet".to_string()]],
            "1,001 rows can touch at most 1,001 partitions, whatever they span"
        );
    }

    /// Files that are each narrow but together wide load in BATCHES.
    #[test]
    fn narrow_files_over_a_wide_history_are_packed_into_batches_under_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        let days =
            |from: i64, to: i64| -> Vec<i64> { (from..=to).map(|d| start + d * DAY).collect() };
        ts_file(dir.path(), "c.parquet", &days(4200, 6000), true);
        ts_file(dir.path(), "a.parquet", &days(0, 2000), true);
        ts_file(dir.path(), "b.parquet", &days(2001, 3999), true);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let uris: Vec<String> = ["c.parquet", "a.parquet", "b.parquet"]
            .iter()
            .map(|n| format!("gs://b/{n}"))
            .collect();
        let batches = plan_load_batches(&store, &uris, &time("ts", Granularity::Day)).unwrap();
        assert_eq!(
            batches,
            vec![
                vec![
                    "gs://b/a.parquet".to_string(),
                    "gs://b/b.parquet".to_string()
                ],
                vec!["gs://b/c.parquet".to_string()],
            ],
            "a+b span exactly 4000 days and fit one job; c starts the next"
        );
        let one = plan_load_batches(&store, &uris, &time("ts", Granularity::Month)).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].len(), 3);
    }

    /// A file with no statistics cannot be placed.
    #[test]
    fn a_stats_less_file_rides_in_the_first_batch() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        let days =
            |from: i64, to: i64| -> Vec<i64> { (from..=to).map(|d| start + d * DAY).collect() };
        ts_file(dir.path(), "a.parquet", &days(0, 2099), true);
        ts_file(dir.path(), "blind.parquet", &days(0, 9000), false);
        ts_file(dir.path(), "z.parquet", &days(5000, 7099), true);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let uris: Vec<String> = ["a.parquet", "blind.parquet", "z.parquet"]
            .iter()
            .map(|n| format!("gs://b/{n}"))
            .collect();
        let batches = plan_load_batches(&store, &uris, &time("ts", Granularity::Day)).unwrap();
        assert_eq!(batches.len(), 2);
        assert!(
            batches[0].contains(&"gs://b/blind.parquet".to_string()),
            "{batches:?}"
        );
        assert_eq!(
            batches.concat().len(),
            3,
            "every file is loaded exactly once"
        );
    }

    #[test]
    fn a_file_without_statistics_leaves_the_check_to_bigquery() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2026, 1, 1, 0);
        ts_file(dir.path(), "a.parquet", &[start, start + 400 * DAY], false);
        assert!(check(dir.path(), &["a.parquet"], &time("ts", Granularity::Hour)).is_ok());
        assert!(
            check(
                dir.path(),
                &["a.parquet"],
                &time("other", Granularity::Hour)
            )
            .is_ok(),
            "a column the file lacks"
        );
    }

    #[test]
    fn a_load_time_partition_reads_no_footer() {
        let dir = tempfile::tempdir().unwrap();
        let ingestion = TablePartition {
            key: PartitionKey::Time {
                column: None,
                granularity: Granularity::Hour,
            },
            ..time("ts", Granularity::Hour)
        };
        assert!(check(dir.path(), &["missing.parquet"], &ingestion).is_ok());
    }

    #[test]
    fn a_date_column_counts_days_from_its_day_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let field = Field::new("d", DataType::Date32, false);
        let days: Vec<i32> = (20_000..=20_000 + 4_500).collect();
        let column: ArrayRef = Arc::new(Date32Array::from(days));
        write(dir.path(), "d.parquet", field, column, true);
        let err = check(dir.path(), &["d.parquet"], &time("d", Granularity::Day))
            .unwrap_err()
            .to_string();
        assert!(err.contains("about 4501 day partitions of `d`"), "{err}");
        assert!(
            err.contains("use `granularity: month` (about 148)"),
            "{err}"
        );
        assert!(check(dir.path(), &["d.parquet"], &time("d", Granularity::Month)).is_ok());
    }

    #[test]
    fn a_range_counts_its_buckets_plus_one_for_values_outside() {
        let dir = tempfile::tempdir().unwrap();
        let field = Field::new("n", DataType::Int64, false);
        let mut values: Vec<i64> = vec![-5_i64, 6_000];
        values.extend(0..5_000);
        let column: ArrayRef = Arc::new(Int64Array::from(values));
        write(dir.path(), "n.parquet", field, column, true);
        let range = |interval: i64| TablePartition {
            key: PartitionKey::Range {
                column: "n".into(),
                start: 0,
                end: 5_000,
                interval,
            },
            ..time("n", Granularity::Day)
        };
        let err = check(dir.path(), &["n.parquet"], &range(1))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("about 5001 ranges of `n` (-5 to 6000, 1 wide)"),
            "{err}"
        );
        assert!(err.contains("widen `range.interval`"), "{err}");
        assert!(check(dir.path(), &["n.parquet"], &range(2)).is_ok());
    }

    /// A narrower integer column (PG `integer`, MySQL `int`) is stored as INT32 with the same statistics.
    #[test]
    fn a_range_on_an_int32_column_is_budgeted_like_int64() {
        let dir = tempfile::tempdir().unwrap();
        let mut i32_values: Vec<i32> = vec![-5_i32, 6_000];
        i32_values.extend(0..5_000);
        let i32_col: ArrayRef = Arc::new(Int32Array::from(i32_values));
        write(
            dir.path(),
            "i32.parquet",
            Field::new("n", DataType::Int32, false),
            i32_col,
            true,
        );
        let mut i16_values: Vec<i16> = vec![-5_i16, 6_000];
        i16_values.extend(0..5_000);
        let i16_col: ArrayRef = Arc::new(Int16Array::from(i16_values));
        write(
            dir.path(),
            "i16.parquet",
            Field::new("n", DataType::Int16, false),
            i16_col,
            true,
        );
        let range = |interval: i64| TablePartition {
            key: PartitionKey::Range {
                column: "n".into(),
                start: 0,
                end: 5_000,
                interval,
            },
            ..time("n", Granularity::Day)
        };
        for file in ["i32.parquet", "i16.parquet"] {
            let err = check(dir.path(), &[file], &range(1))
                .unwrap_err()
                .to_string();
            assert!(err.contains("about 5001 ranges of `n`"), "{file}: {err}");
            assert!(check(dir.path(), &[file], &range(2)).is_ok(), "{file}");
        }
    }

    /// One part without statistics leaves the check to BigQuery for ITS rows only.
    #[test]
    fn a_stats_less_file_does_not_hide_the_other_files_span() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2026, 1, 1, 0);
        let hourly: Vec<i64> = (0..=4800).map(|h| start + h * HOUR).collect();
        ts_file(dir.path(), "a.parquet", &hourly, true);
        ts_file(dir.path(), "b.parquet", &[start], false);
        for files in [["a.parquet", "b.parquet"], ["b.parquet", "a.parquet"]] {
            let err = check(dir.path(), &files, &time("ts", Granularity::Hour))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("about 4801 hour partitions"),
                "{files:?}: {err}"
            );
        }
    }

    #[test]
    fn buckets_count_calendar_boundaries_not_elapsed_time() {
        let (lo, hi) = (at(2025, 12, 31, 23), at(2026, 1, 1, 0));
        assert_eq!(buckets_between(Granularity::Hour, lo, hi), 2);
        assert_eq!(buckets_between(Granularity::Day, lo, hi), 2);
        assert_eq!(buckets_between(Granularity::Month, lo, hi), 2);
        assert_eq!(buckets_between(Granularity::Year, lo, hi), 2);
        let (lo, hi) = (at(2026, 1, 31, 5), at(2026, 3, 1, 0));
        assert_eq!(buckets_between(Granularity::Month, lo, hi), 3);
        assert_eq!(buckets_between(Granularity::Year, lo, hi), 1);
        assert_eq!(buckets_between(Granularity::Day, hi, lo), 30, "order-free");
        assert_eq!(buckets_between(Granularity::Hour, 0, HOUR - 1), 1);
    }

    #[test]
    fn stored_units_follow_the_parquet_type() {
        assert_eq!(
            stored_unit(Some(&LogicalType::Date), PhysicalType::INT32),
            Some(Unit::Days)
        );
        let ts = |unit| {
            LogicalType::Timestamp(TimestampType {
                is_adjusted_to_u_t_c: true,
                unit,
            })
        };
        assert_eq!(
            stored_unit(Some(&ts(TimeUnit::MILLIS)), PhysicalType::INT64),
            Some(Unit::Millis)
        );
        assert_eq!(
            stored_unit(Some(&ts(TimeUnit::NANOS)), PhysicalType::INT64),
            Some(Unit::Nanos)
        );
        assert_eq!(stored_unit(None, PhysicalType::INT64), Some(Unit::Plain));
        assert_eq!(stored_unit(None, PhysicalType::BYTE_ARRAY), None);
        assert_eq!(
            stored_unit(Some(&LogicalType::String), PhysicalType::BYTE_ARRAY),
            None
        );
        assert_eq!(to_seconds(2, Unit::Days), 2 * DAY);
        assert_eq!(
            to_seconds(-1, Unit::Millis),
            -1,
            "floors toward minus infinity"
        );
        assert_eq!(to_seconds(1_500_000_000, Unit::Nanos), 1);
    }

    /// A file over one job's partition budget and an object that is no Parquet file name their codes.
    #[test]
    fn an_over_budget_file_and_a_broken_part_carry_their_codes() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        let days: Vec<i64> = (0..4100).map(|i| start + i * DAY).collect();
        let (field, column) = ts_column(&days);
        write_noted(dir.path(), "wide.parquet", field, column, true, None);
        std::fs::write(dir.path().join("short.parquet"), b"PAR").unwrap();
        let mut lying = vec![0u8; 4];
        lying.extend_from_slice(&1000u32.to_le_bytes());
        lying.extend_from_slice(b"PAR1");
        std::fs::write(dir.path().join("lying.parquet"), lying).unwrap();
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let code = |e: anyhow::Error| crate::error::error_code(&e);

        let wide = plan_load_batches(
            &store,
            &["gs://b/wide.parquet".to_string()],
            &time("ts", Granularity::Day),
        );
        assert_eq!(
            code(wide.unwrap_err()),
            Some("RIVET_LOAD_PARTITION_BUDGET_EXCEEDED")
        );
        for broken in ["short.parquet", "lying.parquet"] {
            assert_eq!(
                code(read_footer(&store, broken).unwrap_err()),
                Some("RIVET_LOAD_STAGING_OBJECT_UNSAFE"),
                "{broken}"
            );
        }
    }
}
