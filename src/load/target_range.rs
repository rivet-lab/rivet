//! The date and timestamp range a warehouse holds, checked against staged Parquet footers before any write.

use anyhow::{Context, Result};
use parquet::file::metadata::ParquetMetaData;

use crate::config::load::LoadTarget;
use crate::destination::gcs::GcsStore;
use crate::types::target::{BIGQUERY_TEMPORAL_RANGES, TemporalKind, ValueRange};

pub(crate) const FOOTER_READERS: usize = 8;

pub(crate) type Ranges = [(TemporalKind, ValueRange)];

/// The ranges `execute_load` checks for `target`; a ClickHouse load checks the footers it reads itself.
pub(crate) fn orchestrated_ranges(target: &LoadTarget) -> &'static Ranges {
    match target {
        LoadTarget::Bigquery { .. } => &BIGQUERY_TEMPORAL_RANGES,
        LoadTarget::Snowflake { .. } | LoadTarget::Clickhouse { .. } => &[],
    }
}

/// Refuse a part holding a date or timestamp outside `ranges`, or one its footer cannot bound where the warehouse would store it silently.
pub(crate) fn refuse_outside_ranges(
    meta: &ParquetMetaData,
    uri: &str,
    ranges: &Ranges,
) -> Result<()> {
    let Some(out) = super::partition_budget::temporal_outside(meta, ranges) else {
        return Ok(());
    };
    let (column, range) = (&out.column, out.range);
    let (wh, name, span) = (range.warehouse, range.type_name, range.span());
    let nothing = range.nothing_done;
    match out.value {
        None => crate::rivet_bail!(
            crate::error::codes::LOAD_VALUE_OUT_OF_TARGET_RANGE,
            "{uri}: column `{column}` has no min/max statistics in the Parquet footer, so rivet \
             cannot check it against {wh} {name}'s range ({span}), \
             which {wh} enforces by {}; {nothing}. \
             Re-export the part with statistics enabled (rivet's writer keeps them), or cast \
             the column to text in the export's `query:`.",
            range.enforced
        ),
        Some(value) => crate::rivet_bail!(
            crate::error::codes::LOAD_VALUE_OUT_OF_TARGET_RANGE,
            "{uri}: column `{column}` holds {value}, outside {wh} {name}'s range \
             ({span}). {wh} would {}; {nothing}. Cast the column to text \
             in the export's `query:` to keep the value, or correct it at the source.",
            range.beyond
        ),
    }
}

/// `one` over the footer of every part in `uris`, in order, `FOOTER_READERS` parts at a time.
pub(crate) fn map_footers<T: Send>(
    store: &GcsStore,
    uris: &[String],
    one: impl Fn(&str, &str, &ParquetMetaData) -> Result<T> + Sync,
) -> Result<Vec<T>> {
    let read = |uri: &String| -> Result<T> {
        let (_, key) = super::split_object_uri(uri)?;
        let meta = super::partition_budget::read_footer(store, key)
            .with_context(|| format!("reading the footer of {uri}"))?;
        one(key, uri, &meta)
    };
    let mut out = Vec::with_capacity(uris.len());
    for wave in uris.chunks(FOOTER_READERS) {
        let done: Vec<Result<T>> = std::thread::scope(|scope| {
            let readers: Vec<_> = wave.iter().map(|uri| scope.spawn(|| read(uri))).collect();
            readers
                .into_iter()
                .map(|r| r.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
                .collect()
        });
        for r in done {
            out.push(r?);
        }
    }
    Ok(out)
}

/// Refuse a load whose plan holds a date or timestamp column when a part of `uris` holds a value the warehouse cannot.
pub(crate) fn refuse_parts_outside_ranges(
    store: &GcsStore,
    plan: &super::plan::LoadPlan,
    uris: &[String],
) -> Result<()> {
    let ranges = orchestrated_ranges(&plan.load.target);
    if ranges.is_empty()
        || !plan.specs.iter().any(holds_temporal)
        || seeded!("bq-load-skips-range-guard", true, false)
    {
        return Ok(());
    }
    map_footers(store, uris, |_, uri, meta| {
        refuse_outside_ranges(meta, uri, ranges)
    })
    .map(|_| ())
    .with_context(|| format!("export `{}`", plan.export_name))
}

/// A column the warehouse declares as a date or a timestamp, or a list of them.
fn holds_temporal(spec: &crate::types::target::TargetColumnSpec) -> bool {
    use crate::types::target::TargetType;
    match &spec.target_type {
        TargetType::BigQuery(t) => t.is_temporal(),
        TargetType::Snowflake(_)
        | TargetType::ClickHouse(_)
        | TargetType::DuckDb(_)
        | TargetType::Unmapped => false,
    }
}

/// The JSON number in `json` that Snowflake stores as a double and renders, at 16 significant digits, past the largest finite double.
pub(crate) fn number_past_snowflake_double(json: &[u8]) -> Option<&str> {
    let (mut i, len) = (0, json.len());
    while i < len {
        match json[i] {
            b'"' => {
                i += 1;
                while i < len && json[i] != b'"' {
                    i += if json[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                while i < len && matches!(json[i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9') {
                    i += 1;
                }
                let token = std::str::from_utf8(json.get(start..i)?).ok()?;
                if renders_as_infinity(token) {
                    return Some(token);
                }
            }
            _ => i += 1,
        }
    }
    None
}

/// Whether `token` is a finite double whose 16-significant-digit text reads back as an infinity.
fn renders_as_infinity(token: &str) -> bool {
    let Ok(v) = token.parse::<f64>() else {
        return false;
    };
    v.is_finite()
        && v.abs() >= 1e307
        && format!("{v:.15e}")
            .parse::<f64>()
            .is_ok_and(f64::is_infinite)
}

/// Refuse a Snowflake load whose VARIANT columns hold a JSON number the warehouse renders as an infinity.
pub(crate) fn refuse_variant_numbers_past_double(
    store: &GcsStore,
    plan: &super::plan::LoadPlan,
    uris: &[String],
) -> Result<()> {
    let columns: Vec<&str> = plan
        .specs
        .iter()
        .filter(|s| super::snowflake::needs_parse_json(s))
        .map(|s| s.column_name.as_str())
        .collect();
    if columns.is_empty() {
        return Ok(());
    }
    for uri in uris {
        let (_, key) = super::split_object_uri(uri)?;
        let part = bytes::Bytes::from(store.read(key)?);
        if let Some((column, number)) = variant_number_in_part(part, &columns)
            .with_context(|| format!("reading {uri}"))
            .with_context(|| format!("export `{}`", plan.export_name))?
        {
            let said = format!("{:.15e}", number.parse::<f64>().unwrap_or_default());
            crate::rivet_bail!(
                crate::error::codes::LOAD_VALUE_OUT_OF_TARGET_RANGE,
                "{uri}: column `{column}` holds the JSON number {number}, which Snowflake stores as a double and renders as {said}, larger than any double: a reader of the text gets an infinity. Nothing was loaded. Keep the value exact by delivering the column as text (cast it in the export's `query:`; MongoDB: set `source.mongo.json: canonical`) and re-export, or correct it at the source."
            );
        }
    }
    Ok(())
}

/// The first (column, number) of `columns` in the Parquet `part` that `number_past_snowflake_double` finds.
fn variant_number_in_part(
    part: bytes::Bytes,
    columns: &[&str],
) -> Result<Option<(String, String)>> {
    use arrow::array::AsArray;
    use arrow::datatypes::DataType;
    use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
    let builder = ParquetRecordBatchReaderBuilder::try_new(part)?;
    let mask = ProjectionMask::columns(builder.parquet_schema(), columns.iter().copied());
    for batch in builder.with_projection(mask).with_batch_size(64).build()? {
        let batch = batch?;
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            if !matches!(
                column.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                continue;
            }
            let text = arrow::compute::cast(column, &DataType::LargeUtf8)?;
            for doc in text.as_string::<i64>().iter().flatten() {
                if let Some(number) = number_past_snowflake_double(doc.as_bytes()) {
                    return Ok(Some((field.name().clone(), number.to_string())));
                }
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::target::CLICKHOUSE_TEMPORAL_RANGES;
    use arrow::array::{ArrayRef, Date32Array, TimestampMicrosecondArray};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::ParquetMetaDataReader;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::schema::types::ColumnPath;
    use std::sync::Arc;

    /// The scanner flags a double that renders past the largest finite one and nothing next to it.
    #[test]
    fn number_past_snowflake_double_flags_only_the_overflowing_doubles() {
        fn flagged(j: &str) -> Option<&str> {
            number_past_snowflake_double(j.as_bytes())
        }
        assert_eq!(
            flagged(r#"{"f":1.7976931348623157e308}"#),
            Some("1.7976931348623157e308")
        );
        assert_eq!(
            flagged(r#"[1,-1.7976931348623157e+308]"#),
            Some("-1.7976931348623157e+308")
        );
        assert_eq!(flagged(r#"{"a":{"b":[0,1.797693134862316e308]}}"#), None);
        for same in [
            r#"{"f":1.7976931348623e308}"#,
            r#"{"f":1.5}"#,
            r#"{"f":1e300}"#,
            r#"{"f":-5e-324}"#,
            r#"{"f":2.225073858507201e-308}"#,
            r#"{"f":9223372036854775807}"#,
            r#"{"f":18446744073709551616}"#,
            r#"{"f":"1.7976931348623157e308"}"#,
            r#"{"k\"1.7976931348623157e308":1}"#,
            r#"{"$numberDouble":"1.7976931348623157e308"}"#,
            r#"{"f":1e999}"#,
            "",
            "null",
        ] {
            assert_eq!(flagged(same), None, "{same}");
        }
    }

    const BQ_MIN_DAY: i32 = -719_162;
    const BQ_MAX_DAY: i32 = 2_932_896;
    const BQ_MIN_SECS: i64 = -62_135_596_800;
    const BQ_MAX_SECS: i64 = 253_402_300_799;

    fn footer(batch: RecordBatch, props: WriterProperties) -> ParquetMetaData {
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        ParquetMetaDataReader::new()
            .parse_and_finish(&bytes::Bytes::from(buf))
            .unwrap()
    }

    fn date_part(days: i32) -> ParquetMetaData {
        let col = Arc::new(Date32Array::from(vec![days])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("d", col)]).unwrap();
        footer(batch, WriterProperties::builder().build())
    }

    fn timestamp_part(micros: i64, stats: bool) -> ParquetMetaData {
        let col = Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC"))
            as ArrayRef;
        let batch = RecordBatch::try_from_iter([("ts", col)]).unwrap();
        let level = if stats {
            EnabledStatistics::Chunk
        } else {
            EnabledStatistics::None
        };
        footer(
            batch,
            WriterProperties::builder()
                .set_statistics_enabled(level)
                .build(),
        )
    }

    fn refusal(meta: &ParquetMetaData, ranges: &Ranges) -> Option<String> {
        let err = refuse_outside_ranges(meta, "gs://b/p", ranges).err()?;
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE")
        );
        Some(format!("{err:#}"))
    }

    /// A date past either end of BigQuery's range is refused by code, named by its day, saying what BigQuery would do.
    #[test]
    fn a_date_past_either_end_of_bigquery_date_is_refused_with_its_day() {
        let bq = &BIGQUERY_TEMPORAL_RANGES;
        for ok in [BQ_MIN_DAY, 0, BQ_MAX_DAY] {
            assert_eq!(refusal(&date_part(ok), bq), None, "{ok}");
        }
        for (bad, shown) in [
            (BQ_MIN_DAY - 1, "0000-12-31"),
            (BQ_MAX_DAY + 1, "+10000-01-01"),
        ] {
            assert_eq!(
                refusal(&date_part(bad), bq).as_deref(),
                Some(&*format!(
                    "gs://b/p: column `d` holds {shown}, outside BigQuery DATE's range \
                     (0001-01-01 to 9999-12-31). BigQuery would fail the whole load job; \
                     nothing was loaded. Cast the column to text in the export's `query:` to \
                     keep the value, or correct it at the source."
                )),
                "{bad}"
            );
        }
    }

    /// A timestamp past either end of BigQuery's range is refused, the ends themselves are not.
    #[test]
    fn a_timestamp_past_either_end_of_bigquery_timestamp_is_refused() {
        let bq = &BIGQUERY_TEMPORAL_RANGES;
        for ok in [
            BQ_MIN_SECS * 1_000_000,
            0,
            BQ_MAX_SECS * 1_000_000 + 999_999,
        ] {
            assert_eq!(refusal(&timestamp_part(ok, true), bq), None, "{ok}");
        }
        for bad in [BQ_MIN_SECS * 1_000_000 - 1, (BQ_MAX_SECS + 1) * 1_000_000] {
            let said = refusal(&timestamp_part(bad, true), bq).expect("refused");
            assert!(
                said.contains(
                    "outside BigQuery TIMESTAMP/DATETIME's range (0001-01-01 to 9999-12-31)"
                ),
                "{bad}: {said}"
            );
        }
    }

    /// A column whose footer holds no statistics is let through for BigQuery, which fails such a load itself, and refused for ClickHouse, which would clamp it silently.
    #[test]
    fn a_footer_without_statistics_is_refused_only_where_the_warehouse_clamps() {
        let blind = timestamp_part(BQ_MAX_SECS * 1_000_000, false);
        assert_eq!(refusal(&blind, &BIGQUERY_TEMPORAL_RANGES), None);
        let said = refusal(&blind, &CLICKHOUSE_TEMPORAL_RANGES).expect("refused");
        assert_eq!(
            said,
            "gs://b/p: column `ts` has no min/max statistics in the Parquet footer, so rivet \
             cannot check it against ClickHouse DateTime64's range (1900-01-01 to 2299-12-31), \
             which ClickHouse enforces by storing the nearest end silently; nothing was inserted. \
             Re-export the part with statistics enabled (rivet's writer keeps them), or cast \
             the column to text in the export's `query:`."
        );
    }

    /// A column without statistics that BigQuery lets through does not hide a later column outside the range.
    #[test]
    fn a_blind_column_does_not_hide_a_later_one_outside_bigquery_range() {
        let d = Arc::new(Date32Array::from(vec![BQ_MAX_DAY + 1])) as ArrayRef;
        let ts =
            Arc::new(TimestampMicrosecondArray::from(vec![0]).with_timezone("UTC")) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("ts", ts), ("d", d)]).unwrap();
        let props = WriterProperties::builder()
            .set_column_statistics_enabled(ColumnPath::from("ts"), EnabledStatistics::None)
            .build();
        let said = refusal(&footer(batch, props), &BIGQUERY_TEMPORAL_RANGES).expect("refused");
        assert!(said.contains("column `d` holds +10000-01-01"), "{said}");
    }

    /// The ranges the orchestrator checks are BigQuery's alone; ClickHouse checks its own reads and Snowflake holds every date.
    #[test]
    fn only_a_bigquery_load_is_checked_by_the_orchestrator() {
        use crate::config::load::LoadTarget;
        let sf = LoadTarget::Snowflake {
            account: String::new(),
            user: String::new(),
            private_key_path: String::new(),
            role: None,
            warehouse: String::new(),
            database: String::new(),
            schema: String::new(),
            storage_integration: String::new(),
        };
        let ch = LoadTarget::Clickhouse {
            url: crate::secret::Secret::new(String::new()),
            database: String::new(),
            user: String::new(),
            password_env: String::new(),
            named_collection: None,
            ca_file: None,
            allow_plaintext: false,
        };
        let bq = LoadTarget::Bigquery {
            project: String::new(),
            dataset: String::new(),
        };
        assert_eq!(orchestrated_ranges(&bq).len(), 2);
        assert!(orchestrated_ranges(&sf).is_empty());
        assert!(orchestrated_ranges(&ch).is_empty());
    }

    /// A column is temporal when BigQuery declares it a date or timestamp, or a list of them, and for no other warehouse.
    #[test]
    fn a_column_is_temporal_by_its_bigquery_type_alone() {
        use crate::types::target::{BqType, ChType, TargetColumnSpec, TargetStatus, TargetType};
        let spec = |t: TargetType| TargetColumnSpec {
            column_name: "c".into(),
            target_type: t.clone(),
            autoload_type: t,
            status: TargetStatus::Ok,
            note: None,
            cast_sql: None,
        };
        for (t, want) in [
            (BqType::Date, true),
            (BqType::Timestamp, true),
            (BqType::DateTime, true),
            (BqType::Array(Box::new(BqType::Date)), true),
            (BqType::Time, false),
            (BqType::String, false),
            (BqType::Array(Box::new(BqType::Int64)), false),
        ] {
            assert_eq!(holds_temporal(&spec(t.clone().into())), want, "{t}");
        }
        assert!(!holds_temporal(&spec(ChType::Date32.into())));
        assert!(!holds_temporal(&spec(TargetType::Unmapped)));
    }
}
