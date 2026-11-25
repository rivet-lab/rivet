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

/// The remedy a range refusal names: the operator declares the column text, and the export delivers the value as text.
pub(crate) fn text_remedy(column: &str) -> String {
    format!(
        "To keep the value, declare `{column}: text` under `columns:` in the export, or cast it \
         to text in the export's `query:`, and run it again: the warehouse then holds the value \
         as text. Or correct it at the source."
    )
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
             Re-export the part with statistics enabled (rivet's writer keeps them). {}",
            range.enforced,
            text_remedy(column)
        ),
        Some(value) => crate::rivet_bail!(
            crate::error::codes::LOAD_VALUE_OUT_OF_TARGET_RANGE,
            "{uri}: column `{column}` holds {value}, outside {wh} {name}'s range \
             ({span}). {wh} would {}; {nothing}. {}",
            range.beyond,
            text_remedy(column)
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
                     nothing was loaded. {}",
                    text_remedy("d")
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
             Re-export the part with statistics enabled (rivet's writer keeps them). \
             To keep the value, declare `ts: text` under `columns:` in the export, or cast it \
             to text in the export's `query:`, and run it again: the warehouse then holds the \
             value as text. Or correct it at the source."
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
