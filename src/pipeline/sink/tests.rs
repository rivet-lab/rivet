//! Unit tests for `ExportSink` — extracted from `mod.rs` to keep the implementation file focused.

use super::*;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[test]
fn strip_internal_column_from_schema() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_rivet_internal_col", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let result = ExportSink::schema_without_internal(&schema, "_rivet_internal_col").unwrap();
    assert_eq!(result.fields().len(), 2);
    assert_eq!(result.field(0).name(), "id");
    assert_eq!(result.field(1).name(), "name");
}

#[test]
fn strip_internal_column_missing_errors() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    assert!(ExportSink::schema_without_internal(&schema, "nonexistent").is_err());
}

#[test]
fn strip_internal_column_from_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_rivet_internal_col", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    let stripped =
        ExportSink::record_batch_without_internal(&batch, "_rivet_internal_col").unwrap();
    assert_eq!(stripped.num_columns(), 1);
    assert_eq!(stripped.schema().field(0).name(), "id");
    let ids = stripped
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
}

/// I1 (ADR-0001) — The temp-file writer must be finalized (`w.finish()`) before the file is transferred to the destination.
#[test]
fn i1_writer_finish_produces_complete_file_before_destination_write() {
    use crate::source::BatchSink;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let mut sink = minimal_sink();

    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["alice", "bob", "carol"]))],
    )
    .unwrap();

    assert!(sink.writer.is_none(), "writer must start as None");

    sink.on_schema(schema).unwrap();
    assert!(
        sink.writer.is_some(),
        "writer must be present after on_schema"
    );

    sink.on_batch(&batch).unwrap();

    if let Some(w) = sink.writer.take() {
        w.finish()
            .expect("I1: writer.finish() must succeed before destination write");
    }
    assert!(
        sink.writer.is_none(),
        "writer must be consumed after finish()"
    );

    let file_len = std::fs::metadata(sink.tmp.path())
        .expect("temp file must be accessible after finish()")
        .len();
    assert!(
        file_len > 0,
        "I1: temp file must be non-empty after writer.finish() — \
         the destination must never receive a truncated file; got {} bytes",
        file_len
    );

    assert_eq!(sink.total_rows, 3, "total_rows must count written rows");
    assert!(!sink.is_empty(), "a sink that streamed rows is not empty");
    assert!(minimal_sink().is_empty(), "a fresh sink is empty");
}

#[test]
fn track_quality_counts_nulls() {
    use crate::source::BatchSink;
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            None,
            Some("d"),
        ]))],
    )
    .unwrap();

    let mut sink = minimal_sink_with_quality(vec!["name".into()], vec![]);
    sink.on_schema(schema).unwrap();
    sink.track_quality(&batch).unwrap();
    assert_eq!(sink.quality.null_counts.get("name"), Some(&2));
}

#[test]
fn track_quality_counts_uniques() {
    use crate::source::BatchSink;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 1, 3]))],
    )
    .unwrap();

    let mut sink = minimal_sink_with_quality(vec![], vec!["id".into()]);
    sink.on_schema(schema).unwrap();
    sink.track_quality(&batch).unwrap();
    assert_eq!(sink.quality.unique_sets.get("id").unwrap().len(), 3);
}

#[test]
fn run_quality_checks_no_config_returns_empty() {
    let sink = ExportSink {
        quality: QualityTracker::default(),
        total_rows: 100,
        ..minimal_sink()
    };
    assert!(sink.run_quality_checks().is_empty());
}

#[test]
fn run_quality_checks_detects_excess_nulls() {
    let mut sink = minimal_sink_with_quality(vec!["col".into()], vec![]);
    sink.quality
        .columns
        .as_mut()
        .unwrap()
        .null_ratio_max
        .insert("col".into(), 0.1);
    sink.total_rows = 100;
    sink.quality.null_counts.insert("col".into(), 50);
    let issues = sink.run_quality_checks();
    assert_eq!(issues.len(), 1);
    assert!(issues[0].message.contains("null ratio"));
}

/// The null-ratio rule is a strict `>` on `nulls / rows`.
#[test]
fn null_ratio_at_exactly_the_cap_passes_and_one_null_over_it_fails() {
    let mut sink = minimal_sink_with_quality(vec!["col".into()], vec![]);
    sink.quality
        .columns
        .as_mut()
        .unwrap()
        .null_ratio_max
        .insert("col".into(), 0.1);
    sink.total_rows = 100;
    sink.quality.null_counts.insert("col".into(), 10);
    assert!(
        sink.run_quality_checks().is_empty(),
        "10 of 100 is the cap, not over it"
    );
    sink.quality.null_counts.insert("col".into(), 11);
    assert_eq!(sink.run_quality_checks().len(), 1, "11 of 100 is over");
    sink.quality.null_counts.insert("col".into(), 1);
    assert!(
        sink.run_quality_checks().is_empty(),
        "1 of 100 is a ratio of 0.01, whatever a product or a remainder would say"
    );
}

/// The byte cap closes a part only once it is reached and the part holds a row.
#[test]
fn the_byte_cap_splits_at_the_cap_and_never_an_empty_part() {
    assert!(!should_split(999, Some(1000), 5));
    assert!(should_split(1000, Some(1000), 5));
    assert!(should_split(5000, Some(1000), 1));
    assert!(
        !should_split(5000, Some(1000), 0),
        "an empty part is never shipped"
    );
    assert!(!should_split(5000, None, 5), "no cap declared");
}

/// Every date/timestamp Arrow type has a unit.
#[test]
fn every_date_and_timestamp_arrow_type_has_a_partition_unit() {
    use arrow::datatypes::TimeUnit;
    let cases = [
        (DataType::Date32, PartitionUnit::Days),
        (DataType::Date64, PartitionUnit::Millis),
        (
            DataType::Timestamp(TimeUnit::Second, None),
            PartitionUnit::Seconds,
        ),
        (
            DataType::Timestamp(TimeUnit::Millisecond, None),
            PartitionUnit::Millis,
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            PartitionUnit::Micros,
        ),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            PartitionUnit::Nanos,
        ),
    ];
    for (ty, unit) in cases {
        assert_eq!(partition_unit_of(&ty), Some(unit), "{ty:?}");
    }
    for ty in [DataType::Int64, DataType::Binary, DataType::Utf8] {
        assert_eq!(
            partition_unit_of(&ty),
            None,
            "{ty:?} is no time partition on this path"
        );
    }
}

#[test]
fn run_quality_checks_detects_duplicates() {
    let mut sink = minimal_sink_with_quality(vec![], vec!["id".into()]);
    sink.total_rows = 5;
    let mut set = std::collections::HashSet::new();
    set.insert(0u64);
    set.insert(1u64);
    set.insert(2u64);
    sink.quality.unique_sets.insert("id".into(), set);
    sink.quality.unique_non_null_counts.insert("id".into(), 5);
    let issues = sink.run_quality_checks();
    assert_eq!(issues.len(), 1);
    assert!(issues[0].message.contains("duplicate"));
}

fn sink_with_unique_cap(unique_cols: Vec<String>, cap: usize) -> ExportSink {
    ExportSink {
        quality: QualityTracker::new(Some(crate::config::QualityConfig {
            row_count_min: None,
            row_count_max: None,
            null_ratio_max: std::collections::HashMap::new(),
            unique_columns: unique_cols,
            unique_max_entries: Some(cap),
        })),
        ..minimal_sink()
    }
}

#[test]
fn unique_cap_stops_inserting_at_limit() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .unwrap();
    let mut sink = sink_with_unique_cap(vec!["id".into()], 3);
    sink.quality.unique_indices = vec![(0, "id".into())];
    sink.track_quality(&batch).unwrap();
    let set_len = sink
        .quality
        .unique_sets
        .get("id")
        .map(|s| s.len())
        .unwrap_or(0);
    assert!(set_len <= 3, "set must not grow past cap; got {set_len}");
    assert!(
        sink.quality.unique_capped.contains("id"),
        "id must be flagged as capped"
    );
}

#[test]
fn unique_cap_emits_warn_issue_not_fail() {
    let mut sink = sink_with_unique_cap(vec!["id".into()], 2);
    sink.total_rows = 5;
    sink.quality.unique_capped.insert("id".into());
    let mut set = std::collections::HashSet::new();
    set.insert(0u64);
    set.insert(1u64);
    sink.quality.unique_sets.insert("id".into(), set);
    let issues = sink.run_quality_checks();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].severity, crate::quality::Severity::Warn);
    assert!(
        issues[0].message.contains("capped"),
        "message must say 'capped'; got: {}",
        issues[0].message
    );
}

#[test]
fn duplicates_found_before_the_unique_cap_still_fail_the_run() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 1, 2, 3, 4]))],
    )
    .unwrap();
    let mut sink = sink_with_unique_cap(vec!["id".into()], 3);
    sink.quality.unique_indices = vec![(0, "id".into())];
    sink.track_quality(&batch).unwrap();
    sink.total_rows = 5;
    let issues = sink.run_quality_checks();
    let sev = |s| issues.iter().filter(|i| i.severity == s).count();
    assert_eq!(sev(crate::quality::Severity::Warn), 1, "{issues:?}");
    assert_eq!(sev(crate::quality::Severity::Fail), 1, "{issues:?}");
    assert!(
        issues
            .iter()
            .any(|i| i.message == "column 'id': at least 1 duplicate values out of 5 rows"),
        "{issues:?}"
    );
}

#[test]
fn unique_no_cap_grows_unbounded() {
    use crate::source::BatchSink;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..10i64))],
    )
    .unwrap();
    let mut sink = minimal_sink_with_quality(vec![], vec!["id".into()]);
    sink.on_schema(schema).unwrap();
    sink.track_quality(&batch).unwrap();
    assert_eq!(
        sink.quality.unique_sets.get("id").unwrap().len(),
        10,
        "without cap all distinct values must be tracked"
    );
    assert!(
        sink.quality.unique_capped.is_empty(),
        "no cap → no column should be flagged"
    );
}

#[test]
fn unique_cap_column_skipped_in_subsequent_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![4, 5, 6]))],
    )
    .unwrap();
    let mut sink = sink_with_unique_cap(vec!["id".into()], 2);
    sink.quality.unique_indices = vec![(0, "id".into())];
    sink.track_quality(&batch1).unwrap();
    let len_after_first = sink
        .quality
        .unique_sets
        .get("id")
        .map(|s| s.len())
        .unwrap_or(0);
    sink.track_quality(&batch2).unwrap();
    let len_after_second = sink
        .quality
        .unique_sets
        .get("id")
        .map(|s| s.len())
        .unwrap_or(0);
    assert_eq!(
        len_after_first, len_after_second,
        "set must not grow after cap is hit; was {len_after_first}, then {len_after_second}"
    );
}

fn utf8_batch(values: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
}

#[test]
fn value_ceiling_rejects_oversized_cell() {
    let mut sink = minimal_sink();
    sink.max_value_bytes = Some(1024);
    let big = "x".repeat(2048);
    let batch = utf8_batch(vec!["small", &big]);
    let err = sink.check_value_ceiling(&batch).unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("RIVET_VALUE_TOO_LARGE"), "got: {msg}");
    assert!(msg.contains("body"), "should name the column: {msg}");
    crate::error::assert_coded(&err, "RIVET_SOURCE_VALUE_TOO_LARGE", 1);
}

#[test]
fn value_ceiling_passes_under_limit() {
    let mut sink = minimal_sink();
    sink.max_value_bytes = Some(1024);
    let batch = utf8_batch(vec!["a", "bb", "ccc"]);
    assert!(sink.check_value_ceiling(&batch).is_ok());
}

#[test]
fn value_ceiling_disabled_when_none() {
    let mut sink = minimal_sink();
    sink.max_value_bytes = None;
    let big = "x".repeat(10 * 1024 * 1024);
    let batch = utf8_batch(vec![&big]);
    assert!(
        sink.check_value_ceiling(&batch).is_ok(),
        "None must disable the guard entirely"
    );
}

#[test]
fn value_ceiling_ignores_null_cells() {
    let mut sink = minimal_sink();
    sink.max_value_bytes = Some(1024);
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![Some("ok"), None]))],
    )
    .unwrap();
    assert!(sink.check_value_ceiling(&batch).is_ok());
}

/// One column of `ty` holding `values` (strings, or their bytes for a binary type).
fn sized_batch(ty: DataType, values: &[&str]) -> RecordBatch {
    let col: ArrayRef = match ty {
        DataType::Utf8 => Arc::new(StringArray::from(values.to_vec())),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(values.to_vec())),
        DataType::Binary => Arc::new(BinaryArray::from_iter_values(
            values.iter().map(|v| v.as_bytes()),
        )),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from_iter_values(
            values.iter().map(|v| v.as_bytes()),
        )),
        _ => unreachable!(),
    };
    let schema = Arc::new(Schema::new(vec![Field::new("body", ty, true)]));
    RecordBatch::try_new(schema, vec![col]).unwrap()
}

const SIZED_TYPES: [DataType; 4] = [
    DataType::Utf8,
    DataType::LargeUtf8,
    DataType::Binary,
    DataType::LargeBinary,
];

/// Every string and binary type: a value of exactly the ceiling passes, one byte over is refused.
#[test]
fn value_ceiling_holds_at_the_limit_for_every_sized_type() {
    let mut sink = minimal_sink();
    sink.max_value_bytes = Some(8);
    for ty in SIZED_TYPES {
        let at = sized_batch(ty.clone(), &["", "12345678"]);
        assert!(sink.check_value_ceiling(&at).is_ok(), "{ty:?} at the limit");
        let over = sized_batch(ty.clone(), &["1", "123456789"]);
        let err = format!("{:#}", sink.check_value_ceiling(&over).unwrap_err());
        assert!(
            err.contains("RIVET_VALUE_TOO_LARGE") && err.contains("'body'"),
            "{ty:?}: {err}"
        );
    }
}

/// Every string and binary type records its longest value, and a shorter later batch keeps it.
#[test]
fn shape_tracks_the_longest_value_of_every_sized_type() {
    for ty in SIZED_TYPES {
        let mut sink = minimal_sink();
        sink.track_shape(&sized_batch(ty.clone(), &["ab", "abcde", "abc"]));
        sink.track_shape(&sized_batch(ty.clone(), &["a"]));
        assert_eq!(sink.column_max_bytes.get("body"), Some(&5), "{ty:?}");
        sink.track_shape(&sized_batch(ty.clone(), &["abcdefg"]));
        assert_eq!(sink.column_max_bytes.get("body"), Some(&7), "{ty:?}");
    }
}

/// Under `fail`, a batch of exactly the memory limit is written; past it the refusal names the sizes.
#[test]
fn batch_memory_refusal_holds_at_the_limit_and_names_the_sizes() {
    let big = "x".repeat(3 * 1024 * 1024 + 12_345);
    let batch = sized_batch(DataType::Utf8, &[&big, "y"]);
    let bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
    let mut sink = minimal_sink();
    sink.batch_memory_policy = crate::tuning::BatchMemoryPolicy::Fail;
    sink.max_batch_memory_bytes = Some(bytes);
    assert!(
        sink.process_dest_batch(&batch).is_ok(),
        "a batch of exactly the limit is written"
    );

    let mut sink = minimal_sink();
    sink.batch_memory_policy = crate::tuning::BatchMemoryPolicy::Fail;
    let limit = 1024 * 1024 + 777;
    sink.max_batch_memory_bytes = Some(limit);
    let err = sink.process_dest_batch(&batch).unwrap_err();
    crate::error::assert_coded(&err, "RIVET_CONFIG_BATCH_MEMORY_EXCEEDED", 1);
    let err = format!("{err:#}");
    let want = format!(
        "batch memory {} MB exceeds max_batch_memory_mb={} MB (2 rows). Lower batch_size to ~{} ",
        bytes / (1024 * 1024),
        limit / (1024 * 1024),
        (2 * limit / bytes).max(1)
    );
    assert!(err.contains(&want), "{err}\nwant: {want}");
}

#[test]
fn a_settle_column_that_is_not_temporal_is_refused_by_code() {
    let mut sink = minimal_sink();
    sink.settle_columns = vec!["updated".to_string()];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "updated",
        DataType::Utf8,
        true,
    )]));
    let err = crate::source::BatchSink::on_schema(&mut sink, schema).unwrap_err();
    crate::error::assert_coded(&err, "RIVET_CONFIG_SETTLE_COLUMN_NOT_TEMPORAL", 1);
}

#[test]
fn note_nulled_accumulates_per_column_in_the_plan_shared_counter() {
    let mut sink = minimal_sink();
    let shared = sink.nulled_cells.clone();
    sink.note_nulled("b", 2);
    sink.note_nulled("a", 1);
    sink.note_nulled("b", 3);
    assert_eq!(
        shared.snapshot(),
        vec![("a".to_string(), 1), ("b".to_string(), 5)]
    );
}

#[test]
fn a_pipelined_sink_reports_nulled_cells_into_the_plan_shared_counter() {
    let inner = minimal_sink();
    let shared = inner.nulled_cells.clone();
    let mut p = PipelinedSink::spawn_with_sink(inner);
    p.note_nulled("b", 2);
    p.note_nulled("a", 1);
    p.note_nulled("b", 3);
    assert_eq!(
        shared.snapshot(),
        vec![("a".to_string(), 1), ("b".to_string(), 5)]
    );
}

fn minimal_sink() -> ExportSink {
    ExportSink {
        bytes_read: Default::default(),
        parts_landed: Default::default(),
        nulled_cells: Default::default(),
        writer: None,
        format_type: crate::config::FormatType::Csv,
        compression: crate::config::CompressionType::None,
        compression_level: None,
        tmp: tempfile::NamedTempFile::new().unwrap(),
        total_rows: 0,
        part_rows: 0,
        cursor_column: None,
        cursor_rest: Vec::new(),
        override_columns: Vec::new(),
        settle_columns: Vec::new(),
        last_cursor_value: None,
        first_cursor_value: None,
        source_cursor: None,
        schema: None,
        dest_schema: None,
        meta: crate::config::MetaColumns::default(),
        enriched_schema: None,
        exported_at_us: 0,
        quality: QualityTracker::default(),
        max_file_size: None,
        completed_parts: Vec::new(),
        strip_internal_column: None,
        column_max_bytes: std::collections::HashMap::new(),
        max_batch_memory_bytes: None,
        max_value_bytes: None,
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::Warn,
        oversized_batch_count: 0,
        parquet_config: None,
        parquet_row_group_rows: None,
        column_checksums: std::collections::BTreeMap::new(),
        checksum_key_col: None,
        row_progress: None,
        partition: PartBudget::default(),
    }
}

fn minimal_sink_with_quality(null_cols: Vec<String>, unique_cols: Vec<String>) -> ExportSink {
    let mut null_ratio_max = std::collections::HashMap::new();
    for col in &null_cols {
        null_ratio_max.insert(col.clone(), 0.5);
    }
    ExportSink {
        quality: QualityTracker::new(Some(crate::config::QualityConfig {
            row_count_min: None,
            row_count_max: None,
            null_ratio_max,
            unique_columns: unique_cols,
            unique_max_entries: None,
        })),
        ..minimal_sink()
    }
}

/// A sink budgeted to `cap` partitions of a `Date32` column named `d`.
fn sink_with_partition_budget(cap: usize) -> (ExportSink, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
    let sink = ExportSink {
        partition: PartBudget::new(Some(crate::plan::rollover::PartitionRollover {
            column: "d".into(),
            granularity: crate::config::load::Granularity::Day,
            cap,
        })),
        ..minimal_sink()
    };
    (sink, schema)
}

fn day_batch(schema: &Arc<Schema>, days: Vec<Option<i32>>) -> RecordBatch {
    RecordBatch::try_new(schema.clone(), vec![Arc::new(Date32Array::from(days))]).unwrap()
}

/// Nothing splits one Parquet file at load time, so a part that outgrows the load job's partition budget cannot be loaded at all.
#[test]
fn a_batch_past_the_partition_budget_closes_the_part_mid_batch() {
    let (mut sink, schema) = sink_with_partition_budget(3);
    sink.on_schema(schema.clone()).unwrap();
    assert!(
        sink.partition.col.is_some(),
        "the fixture must resolve the partition column, or it grades nothing"
    );

    sink.on_batch_inner(&day_batch(
        &schema,
        vec![Some(0), Some(1), Some(2), Some(3), Some(4)],
    ))
    .unwrap();

    assert_eq!(
        sink.completed_parts.len(),
        1,
        "the part must be closed before the fourth partition is written"
    );
    assert_eq!(
        sink.partition.footer_note().as_deref(),
        Some("d|day|2"),
        "the note names the column, the granularity and the distinct partitions held"
    );
    assert_eq!(
        sink.completed_parts[0].rows, 3,
        "the closed part holds exactly the rows that fit its budget"
    );
    assert_eq!(
        sink.partition.buckets.len(),
        2,
        "the open part carries the remaining two days"
    );
    assert_eq!(sink.total_rows, 5, "no row is dropped by the rotation");
}

/// The LAST part of an export never rotates.
#[test]
fn the_final_part_records_its_partitions_in_the_footer_like_a_rotated_one() {
    use parquet::file::reader::FileReader as _;
    let (sink, schema) = sink_with_partition_budget(10);
    let mut sink = ExportSink {
        format_type: crate::config::FormatType::Parquet,
        ..sink
    };
    sink.on_schema(schema.clone()).unwrap();
    sink.on_batch_inner(&day_batch(&schema, vec![Some(0), Some(1), Some(1)]))
        .unwrap();
    assert!(
        sink.completed_parts.is_empty(),
        "within budget: no rotation"
    );
    sink.finish_writer().unwrap();

    let reader = parquet::file::reader::SerializedFileReader::new(
        std::fs::File::open(sink.tmp.path()).unwrap(),
    )
    .unwrap();
    let note = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|kv| {
            kv.iter()
                .find(|kv| kv.key == crate::plan::rollover::PARTITION_BUCKETS_KEY)
                .and_then(|kv| kv.value.clone())
        });
    assert_eq!(
        note.as_deref(),
        Some("d|day|2"),
        "two distinct days in the final part"
    );
}

/// The budget counts DISTINCT partitions, not rows.
#[test]
fn a_wide_batch_inside_one_partition_never_rotates() {
    let (mut sink, schema) = sink_with_partition_budget(1);
    sink.on_schema(schema.clone()).unwrap();

    sink.on_batch_inner(&day_batch(&schema, vec![Some(7); 5_000]))
        .unwrap();

    assert!(
        sink.completed_parts.is_empty(),
        "one partition, whatever the row count — nothing to rotate"
    );
    assert_eq!(sink.total_rows, 5_000);
}

/// Rows with a NULL key share the warehouse's one NULL partition, so they cost one bucket between them.
#[test]
fn null_keys_cost_a_single_partition() {
    let (mut sink, schema) = sink_with_partition_budget(1);
    sink.on_schema(schema.clone()).unwrap();

    sink.on_batch_inner(&day_batch(&schema, vec![None, None, None]))
        .unwrap();

    assert!(sink.completed_parts.is_empty(), "one NULL partition in all");
    assert_eq!(sink.total_rows, 3);
}

/// An export the load does not partition is sized by `max_file_size` alone.
#[test]
fn an_unpartitioned_export_is_never_rotated_by_the_budget() {
    let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
    let mut sink = minimal_sink();
    sink.on_schema(schema.clone()).unwrap();
    assert!(sink.partition.col.is_none());

    sink.on_batch_inner(&day_batch(&schema, (0..500).map(Some).collect()))
        .unwrap();

    assert!(sink.completed_parts.is_empty());
    assert_eq!(sink.total_rows, 500);
}

fn make_int_batch(n_rows: usize, n_cols: usize) -> RecordBatch {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    let fields: Vec<Field> = (0..n_cols)
        .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let cols: Vec<Arc<dyn arrow::array::Array>> = (0..n_cols)
        .map(|_| {
            Arc::new(Int64Array::from_iter_values(0..n_rows as i64)) as Arc<dyn arrow::array::Array>
        })
        .collect();
    RecordBatch::try_new(schema, cols).unwrap()
}

#[test]
fn warn_policy_does_not_fail_on_oversized_batch() {
    let batch = make_int_batch(1_000, 10);
    let batch_bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
    assert!(batch_bytes > 0);

    let mut sink = ExportSink {
        max_batch_memory_bytes: Some(1),
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::Warn,
        ..minimal_sink()
    };
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(
        (0..10)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    sink.enriched_schema = Some(schema);

    assert!(sink.on_batch_inner(&batch).is_ok());
    assert_eq!(sink.oversized_batch_count, 0);
}

#[test]
fn fail_policy_returns_error_on_oversized_batch() {
    let schema_fields: Vec<arrow::datatypes::Field> = (0..5)
        .map(|i| {
            arrow::datatypes::Field::new(format!("c{i}"), arrow::datatypes::DataType::Int64, false)
        })
        .collect();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(schema_fields.clone()));
    let batch = RecordBatch::try_new(
        schema.clone(),
        schema_fields
            .iter()
            .map(|_| {
                std::sync::Arc::new(arrow::array::Int64Array::from_iter_values(0..100i64))
                    as std::sync::Arc<dyn arrow::array::Array>
            })
            .collect(),
    )
    .unwrap();

    let mut sink = ExportSink {
        max_batch_memory_bytes: Some(1),
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::Fail,
        enriched_schema: Some(schema),
        ..minimal_sink()
    };

    let result = sink.on_batch(&batch);
    assert!(result.is_err(), "Fail policy should return Err");
    assert!(
        result.unwrap_err().to_string().contains("exceeds"),
        "error should mention 'exceeds'"
    );
    assert_eq!(sink.oversized_batch_count, 1);
}

#[test]
fn batch_memory_bytes_returns_positive_for_non_empty_batch() {
    let batch = make_int_batch(1_000, 4);
    let bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
    assert!(bytes >= 32_000, "got {}", bytes);
}

fn sink_with_auto_shrink(limit_bytes: usize, n_cols: usize) -> ExportSink {
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(
        (0..n_cols)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    ExportSink {
        max_batch_memory_bytes: Some(limit_bytes),
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::AutoShrink,
        enriched_schema: Some(schema),
        ..minimal_sink()
    }
}

#[test]
fn auto_shrink_preserves_row_count_exact() {
    let batch = make_int_batch(100, 4);
    let mut sink = sink_with_auto_shrink(1, 4);

    assert!(sink.on_batch(&batch).is_ok());
    assert_eq!(
        sink.total_rows, 100,
        "auto_shrink must not lose or duplicate rows; got {}",
        sink.total_rows
    );
}

#[test]
fn auto_shrink_recursive_split_large_batch() {
    let batch = make_int_batch(1_000, 8);
    let mut sink = sink_with_auto_shrink(1, 8);

    assert!(sink.on_batch(&batch).is_ok());
    assert_eq!(
        sink.total_rows, 1_000,
        "recursive auto_shrink must preserve all rows; got {}",
        sink.total_rows
    );
}

#[test]
fn auto_shrink_odd_row_count_no_row_lost() {
    let batch = make_int_batch(101, 4);
    let mut sink = sink_with_auto_shrink(1, 4);

    assert!(sink.on_batch(&batch).is_ok());
    assert_eq!(
        sink.total_rows, 101,
        "odd-row batch must not lose the extra row; got {}",
        sink.total_rows
    );
}

#[test]
fn auto_shrink_single_row_over_limit_writes_and_does_not_panic() {
    let batch = make_int_batch(1, 4);
    let mut sink = sink_with_auto_shrink(1, 4);

    let result = sink.on_batch(&batch);
    assert!(
        result.is_ok(),
        "single-row over-limit must not fail: {:?}",
        result
    );
    assert_eq!(
        sink.total_rows, 1,
        "single row must be written; got {}",
        sink.total_rows
    );
}

#[test]
fn auto_shrink_oversized_count_tracks_original_batches_only() {
    let batch = make_int_batch(100, 4);
    let mut sink = sink_with_auto_shrink(1, 4);

    sink.on_batch(&batch).unwrap();
    assert_eq!(
        sink.oversized_batch_count, 1,
        "one original batch triggered the cap; got {}",
        sink.oversized_batch_count
    );
}

#[test]
fn auto_shrink_batch_within_limit_writes_directly() {
    let batch = make_int_batch(10, 2);
    let actual_bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
    let mut sink = sink_with_auto_shrink(actual_bytes * 2, 2);

    sink.on_batch(&batch).unwrap();
    assert_eq!(sink.total_rows, 10, "in-limit batch: all rows written");
    assert_eq!(
        sink.oversized_batch_count, 0,
        "in-limit batch must not increment oversized counter"
    );
}

#[test]
fn row_count_min_fires_when_total_rows_is_zero() {
    let mut sink = minimal_sink();
    sink.quality.columns = Some(crate::config::QualityConfig {
        row_count_min: Some(100),
        row_count_max: None,
        null_ratio_max: std::collections::HashMap::new(),
        unique_columns: vec![],
        unique_max_entries: None,
    });
    let issues = sink.run_quality_checks();
    assert_eq!(issues.len(), 1, "must emit exactly one issue");
    assert_eq!(issues[0].severity, crate::quality::Severity::Fail);
    assert!(
        issues[0].message.contains("row_count") && issues[0].message.contains("minimum"),
        "message must identify row_count minimum; got: {}",
        issues[0].message
    );
}

#[test]
fn row_count_min_passes_when_row_count_meets_threshold() {
    let mut sink = minimal_sink();
    sink.quality.columns = Some(crate::config::QualityConfig {
        row_count_min: Some(5),
        row_count_max: None,
        null_ratio_max: std::collections::HashMap::new(),
        unique_columns: vec![],
        unique_max_entries: None,
    });
    sink.total_rows = 5;
    assert!(
        sink.run_quality_checks().is_empty(),
        "exact minimum must pass"
    );
}

/// A writer that succeeds on the first N `write_batch` calls, then fails.
struct FailingWriter {
    calls: usize,
    fail_after: usize,
}
impl crate::format::FormatWriter for FailingWriter {
    fn write_batch(&mut self, _batch: &RecordBatch) -> crate::error::Result<()> {
        self.calls += 1;
        if self.calls > self.fail_after {
            anyhow::bail!("gremlin: injected write failure at call {}", self.calls);
        }
        Ok(())
    }
    fn finish(self: Box<Self>) -> crate::error::Result<()> {
        Ok(())
    }
    fn bytes_written(&self) -> u64 {
        0
    }
}

/// A writer that always reports enormous bytes_written, forcing `maybe_split` to trigger after every batch, while never actually failing.
struct SplitTriggerWriter {
    write_count: usize,
}
impl crate::format::FormatWriter for SplitTriggerWriter {
    fn write_batch(&mut self, _batch: &RecordBatch) -> crate::error::Result<()> {
        self.write_count += 1;
        Ok(())
    }
    fn finish(self: Box<Self>) -> crate::error::Result<()> {
        Ok(())
    }
    fn bytes_written(&self) -> u64 {
        999_999_999
    }
}

#[test]
fn gremlin_writer_fail_on_second_subbatch_propagates_error() {
    let batch = make_int_batch(4, 2);
    let mut sink = ExportSink {
        max_batch_memory_bytes: Some(1),
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::AutoShrink,
        enriched_schema: Some(Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Int64, false),
            Field::new("c1", DataType::Int64, false),
        ]))),
        writer: Some(Box::new(FailingWriter {
            calls: 0,
            fail_after: 1,
        })),
        ..minimal_sink()
    };

    let result = sink.on_batch(&batch);
    assert!(
        result.is_err(),
        "writer failure inside auto_shrink must propagate as Err"
    );
}

#[test]
fn gremlin_writer_fail_before_any_write_propagates_error() {
    let batch = make_int_batch(2, 2);
    let mut sink = ExportSink {
        enriched_schema: Some(Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Int64, false),
            Field::new("c1", DataType::Int64, false),
        ]))),
        writer: Some(Box::new(FailingWriter {
            calls: 0,
            fail_after: 0,
        })),
        ..minimal_sink()
    };
    let result = sink.on_batch(&batch);
    assert!(
        result.is_err(),
        "immediate writer failure must propagate as Err"
    );
}

#[test]
fn gremlin_auto_shrink_plus_file_split_total_rows_consistent() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let mut sink = ExportSink {
        max_batch_memory_bytes: Some(1),
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::AutoShrink,
        max_file_size: Some(1),
        enriched_schema: Some(schema.clone()),
        writer: Some(Box::new(SplitTriggerWriter { write_count: 0 })),
        ..minimal_sink()
    };
    sink.schema = Some(schema.clone());

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(0..20i64))],
    )
    .unwrap();

    for _ in 0..3 {
        sink.on_batch(&batch).unwrap();
    }

    let parts_rows: usize = sink.completed_parts.iter().map(|p| p.rows).sum();
    assert_eq!(
        parts_rows + sink.part_rows,
        sink.total_rows,
        "completed_parts + part_rows must equal total_rows; \
         parts_rows={parts_rows}, part_rows={}, total_rows={}",
        sink.part_rows,
        sink.total_rows
    );
    assert_eq!(sink.total_rows, 60, "all 60 rows must be counted");
}

#[test]
fn gremlin_unique_cap_exact_boundary_no_false_capped_flag() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let exact_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .unwrap();
    let mut sink = sink_with_unique_cap(vec!["id".into()], 5);
    sink.quality.unique_indices = vec![(0, "id".into())];
    sink.track_quality(&exact_batch).unwrap();

    assert!(
        !sink.quality.unique_capped.contains("id"),
        "exactly cap distinct values must NOT set the capped flag"
    );
    assert_eq!(
        sink.quality.unique_sets.get("id").unwrap().len(),
        5,
        "all 5 distinct values must be tracked"
    );

    let overflow_batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![6]))]).unwrap();
    sink.track_quality(&overflow_batch).unwrap();

    assert!(
        sink.quality.unique_capped.contains("id"),
        "cap+1 distinct values must set the capped flag"
    );
}

#[test]
fn gremlin_zero_row_batch_does_not_panic_or_increment_rows() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let empty =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(Vec::<i64>::new()))]).unwrap();
    let mut sink = minimal_sink();
    sink.on_batch(&empty).unwrap();
    assert_eq!(
        sink.total_rows, 0,
        "zero-row batch must not increment total_rows"
    );
}

#[test]
fn pipelined_sink_output_is_byte_identical_to_synchronous() {
    use crate::pipeline::manifest_writer::compute_part_checksums;
    use crate::source::BatchSink;

    let batches: Vec<RecordBatch> = (0..3).map(|_| make_int_batch(100, 4)).collect();
    let schema_ref = batches[0].schema();

    let mut sync_sink = minimal_sink();
    sync_sink.on_schema(schema_ref.clone()).unwrap();
    for b in &batches {
        sync_sink.on_batch(b).unwrap();
    }
    let sync_writer = sync_sink.writer.take().unwrap();
    sync_writer.finish().unwrap();
    let sync_fp = compute_part_checksums(sync_sink.tmp.path()).unwrap().0;
    let sync_rows = sync_sink.total_rows;

    let mut p = PipelinedSink::spawn_with_sink(minimal_sink());
    p.on_schema(schema_ref.clone()).unwrap();
    for b in &batches {
        p.on_batch(b).unwrap();
    }
    let mut rec = p.finish().unwrap();
    let p_writer = rec.writer.take().unwrap();
    p_writer.finish().unwrap();
    let p_fp = compute_part_checksums(rec.tmp.path()).unwrap().0;

    assert_eq!(sync_rows, 300);
    assert_eq!(rec.total_rows, sync_rows, "row count must match");
    assert_eq!(
        sync_fp, p_fp,
        "pipelined output must be byte-identical to synchronous"
    );
}

#[test]
fn roast_unique_check_treats_nulls_as_non_duplicates() {
    use crate::source::BatchSink;
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            None,
            Some("b"),
        ]))],
    )
    .unwrap();

    let mut sink = minimal_sink_with_quality(vec![], vec!["name".into()]);
    sink.on_schema(schema).unwrap();
    sink.on_batch(&batch).unwrap();

    let issues = sink.run_quality_checks();
    let dupe_issues: Vec<String> = issues
        .iter()
        .filter(|i| i.message.contains("duplicate"))
        .map(|i| format!("[{:?}] {}", i.severity, i.message))
        .collect();
    assert!(
        dupe_issues.is_empty(),
        "column 'name' holds distinct non-NULL values ('a', 'b') plus 2 NULLs — \
         per SQL UNIQUE semantics NULLs are not duplicates, so the quality gate \
         must pass with zero duplicates; got false duplicate report(s): {:?}",
        dupe_issues
    );
}

#[test]
fn unique_check_all_null_column_reports_zero_issues() {
    use crate::source::BatchSink;
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![None::<&str>, None, None]))],
    )
    .unwrap();

    let mut sink = minimal_sink_with_quality(vec![], vec!["name".into()]);
    sink.on_schema(schema).unwrap();
    sink.on_batch(&batch).unwrap();

    let issues = sink.run_quality_checks();
    assert!(
        issues.is_empty(),
        "an all-NULL unique column has zero duplicates and must pass; got: {:?}",
        issues
            .iter()
            .map(|i| i.message.as_str())
            .collect::<Vec<_>>()
    );
}

#[test]
fn unique_check_counts_only_real_duplicates_when_nulls_present() {
    use crate::source::BatchSink;
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("a"),
            Some("a"),
            None,
            Some("b"),
            None,
            Some("b"),
            Some("c"),
        ]))],
    )
    .unwrap();

    let mut sink = minimal_sink_with_quality(vec![], vec!["name".into()]);
    sink.on_schema(schema).unwrap();
    sink.on_batch(&batch).unwrap();

    let issues = sink.run_quality_checks();
    assert_eq!(
        issues.len(),
        1,
        "real duplicates must still be reported; got: {:?}",
        issues
            .iter()
            .map(|i| i.message.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(issues[0].severity, crate::quality::Severity::Fail);
    assert!(
        issues[0].message.contains("2 duplicate"),
        "NULLs must not inflate the count — exactly 2 duplicates; got: {}",
        issues[0].message
    );
}

#[test]
fn unique_check_empty_string_distinct_from_null() {
    use crate::source::BatchSink;
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![Some(""), None, Some("x")]))],
    )
    .unwrap();

    let mut sink = minimal_sink_with_quality(vec![], vec!["name".into()]);
    sink.on_schema(schema).unwrap();
    sink.on_batch(&batch).unwrap();

    let issues = sink.run_quality_checks();
    assert!(
        issues.is_empty(),
        "one empty string plus one NULL are distinct, not duplicates; got: {:?}",
        issues
            .iter()
            .map(|i| i.message.as_str())
            .collect::<Vec<_>>()
    );
}

#[test]
fn unique_check_duplicate_empty_strings_counted_nulls_excluded() {
    use crate::source::BatchSink;
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some(""),
            Some(""),
            None,
            Some("x"),
        ]))],
    )
    .unwrap();

    let mut sink = minimal_sink_with_quality(vec![], vec!["name".into()]);
    sink.on_schema(schema).unwrap();
    sink.on_batch(&batch).unwrap();

    let issues = sink.run_quality_checks();
    assert_eq!(
        issues.len(),
        1,
        "the repeated empty string is one real duplicate; got: {:?}",
        issues
            .iter()
            .map(|i| i.message.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        issues[0].message.contains("1 duplicate"),
        "exactly 1 duplicate (the second \"\"), NULL excluded; got: {}",
        issues[0].message
    );
}

#[test]
fn unique_check_non_null_counter_accumulates_across_batches() {
    use crate::source::BatchSink;
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
    )
    .unwrap();

    let mut sink = minimal_sink_with_quality(vec![], vec!["name".into()]);
    sink.on_schema(schema).unwrap();
    sink.on_batch(&batch).unwrap();
    sink.on_batch(&batch).unwrap();

    let issues = sink.run_quality_checks();
    assert_eq!(
        issues.len(),
        1,
        "the repeated 'a' across batches is one real duplicate; got: {:?}",
        issues
            .iter()
            .map(|i| i.message.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        issues[0].message.contains("1 duplicate"),
        "exactly 1 duplicate ('a'), NULLs excluded; got: {}",
        issues[0].message
    );
}

#[test]
fn unique_cap_with_nulls_emits_warn_only_no_false_duplicate_fail() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
        ]))],
    )
    .unwrap();

    let mut sink = sink_with_unique_cap(vec!["id".into()], 2);
    sink.quality.unique_indices = vec![(0, "id".into())];
    sink.track_quality(&batch).unwrap();
    sink.total_rows = 5;

    assert!(
        sink.quality.unique_capped.contains("id"),
        "third distinct value past cap=2 must flag the column as capped"
    );
    let issues = sink.run_quality_checks();
    assert_eq!(
        issues.len(),
        1,
        "capped column must emit exactly the Warn issue; got: {:?}",
        issues
            .iter()
            .map(|i| i.message.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(issues[0].severity, crate::quality::Severity::Warn);
    assert!(
        issues[0].message.contains("capped"),
        "message must say 'capped'; got: {}",
        issues[0].message
    );
}

#[test]
fn unique_cap_exact_boundary_trailing_nulls_do_not_trip_cap() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            None,
        ]))],
    )
    .unwrap();

    let mut sink = sink_with_unique_cap(vec!["id".into()], 3);
    sink.quality.unique_indices = vec![(0, "id".into())];
    sink.track_quality(&batch).unwrap();
    sink.total_rows = 5;

    assert!(
        !sink.quality.unique_capped.contains("id"),
        "NULL rows after exactly cap distinct values must not trip the cap"
    );
    let issues = sink.run_quality_checks();
    assert!(
        issues.is_empty(),
        "3 distinct + 2 NULLs at cap=3: no duplicates, no cap warning; got: {:?}",
        issues
            .iter()
            .map(|i| i.message.as_str())
            .collect::<Vec<_>>()
    );
}

/// #175: bytes_read accumulates on the RUN-wide shared counter.
#[test]
fn bytes_read_accumulates_across_sinks_sharing_the_plan_counter() {
    use crate::source::BatchSink;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let counter = Arc::new(AtomicU64::new(0));
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["alice", "bob"]))],
    )
    .unwrap();
    let expect_one = batch.get_array_memory_size() as u64;
    assert!(expect_one > 0, "fixture batch must have a non-zero size");

    for _ in 0..2 {
        let mut sink = minimal_sink();
        sink.bytes_read = Arc::clone(&counter);
        sink.on_schema(schema.clone()).unwrap();
        sink.on_batch(&batch).unwrap();
        if let Some(w) = sink.writer.take() {
            w.finish().unwrap();
        }
    }
    assert_eq!(
        counter.load(Ordering::Relaxed),
        expect_one * 2,
        "both sinks must sum into the ONE run-wide counter"
    );
}

/// Two Int64 columns and two rows: one column cannot express a `key ‖ value` boundary, and one row cannot distinguish `+` from `^` in the fold.
fn checksum_batch(vs: [i64; 2]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2])),
            Arc::new(Int64Array::from(vs.to_vec())),
        ],
    )
    .unwrap()
}

fn parquet_sink(checksum_key_col: Option<usize>) -> ExportSink {
    ExportSink {
        format_type: crate::config::FormatType::Parquet,
        checksum_key_col,
        ..minimal_sink()
    }
}

/// A part spanning more than one read batch must ACCUMULATE, not annihilate.
#[test]
fn track_checksum_sums_across_batches_within_one_part() {
    let batch = checksum_batch([10, 20]);
    let mut sink = parquet_sink(None);

    sink.track_checksum(&batch);
    let after_one = sink.column_checksums.clone();
    assert_eq!(after_one.len(), 2, "both columns must be recorded by name");
    sink.track_checksum(&batch);

    for (name, one) in &after_one {
        assert_ne!(*one, 0, "column '{name}' must hash to something non-zero");
        assert_eq!(
            sink.column_checksums[name],
            one.wrapping_add(*one),
            "column '{name}': the second batch must be SUMMED into the accumulator. \
             `^=` yields 0 here and `=`/`|=` yield the single-batch value — the bug \
             that shipped, invisible below PROBE_BATCH_SIZE because one batch per \
             part makes `0 ^ s == 0 + s`"
        );
    }
}

/// With a cursor column resolved the sink must hash `key ‖ value`.
#[test]
fn track_checksum_routes_through_the_keyed_path_when_a_key_column_is_resolved() {
    let recorded = |vs: [i64; 2], key: Option<usize>| {
        let mut sink = parquet_sink(key);
        sink.track_checksum(&checksum_batch(vs));
        sink.column_checksums["v"]
    };

    assert_eq!(
        recorded([10, 20], None),
        recorded([20, 10], None),
        "the un-keyed fold is order-independent — a swap between rows cannot move it"
    );
    assert_ne!(
        recorded([10, 20], Some(0)),
        recorded([20, 10], Some(0)),
        "with `checksum_key_col` resolved each cell is hashed against its own key, so \
         the swap must diverge — RED when the routing collapses to the un-keyed arm"
    );
}

/// Only Parquet records Form B: a CSV→Arrow re-read is not byte-faithful, so a recorded checksum would make `validate --depth full` report phantom corruption.
#[test]
fn track_checksum_records_nothing_for_a_non_parquet_export() {
    let mut sink = minimal_sink();
    assert_eq!(sink.format_type, crate::config::FormatType::Csv);
    sink.track_checksum(&checksum_batch([10, 20]));
    assert!(
        sink.column_checksums.is_empty(),
        "a CSV export must record no Form-B checksum"
    );
}

/// #173: PipelinedSink must FORWARD `set_source_cursor`.
#[test]
fn pipelined_forwards_the_source_cursor_to_the_inner_sink() {
    use crate::source::BatchSink;
    let mut p = PipelinedSink::spawn_with_sink(minimal_sink());
    p.set_source_cursor("bson:int64:42".to_string());
    let sink = p.finish().expect("worker joins clean");
    assert_eq!(
        sink.source_cursor.as_deref(),
        Some("bson:int64:42"),
        "the token must reach the inner ExportSink through the decorator"
    );
}

#[test]
fn batch_partition_buckets_counts_days_and_puts_nulls_in_their_own_bucket() {
    use arrow::array::Date32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Date32Array::from(vec![
            Some(10),
            Some(10),
            None,
            Some(11),
        ]))],
    )
    .unwrap();
    let r = crate::plan::rollover::PartitionRollover {
        column: "d".into(),
        granularity: crate::config::load::Granularity::Day,
        cap: 4000,
    };
    let b = super::batch_partition_buckets(&batch, &r).expect("a date column buckets");
    assert_eq!(b[0], b[1]);
    assert_ne!(b[0], b[3]);
    assert_eq!(b[2], crate::plan::rollover::NULL_BUCKET);
    let other = crate::plan::rollover::PartitionRollover {
        column: "missing".into(),
        ..r
    };
    assert!(super::batch_partition_buckets(&batch, &other).is_none());
}

#[test]
fn a_byte_cap_sizes_row_groups_so_it_can_rotate_between_them() {
    use super::{cap_piece_rows, piece_ranges, row_group_rows_for_cap};
    assert_eq!(piece_ranges(10, 4), vec![(0, 4), (4, 4), (8, 2)]);
    assert_eq!(piece_ranges(8, 4), vec![(0, 4), (4, 4)]);
    assert_eq!(piece_ranges(3, 10), vec![(0, 3)]);
    assert_eq!(
        piece_ranges(0, 4),
        vec![(0, 0)],
        "an empty batch still reaches the writer"
    );
    assert_eq!(
        piece_ranges(2, 0),
        vec![(0, 1), (1, 1)],
        "a zero step never loops"
    );
    assert_eq!(
        row_group_rows_for_cap(Some(1_000_000), None, 100),
        Some(1_000_000)
    );
    assert_eq!(row_group_rows_for_cap(None, None, 100), None);
    assert_eq!(
        row_group_rows_for_cap(Some(1_000_000), Some(400 * 1024), 100),
        Some(1024)
    );
    assert_eq!(
        row_group_rows_for_cap(None, Some(400 * 1024), 100),
        Some(1024)
    );
    assert_eq!(
        row_group_rows_for_cap(Some(10), Some(400 * 1024), 100),
        Some(10)
    );
    assert_eq!(row_group_rows_for_cap(None, Some(8), 1_000), Some(1));
    assert_eq!(cap_piece_rows(Some(1), Some(300), 10_000), 300);
    assert_eq!(cap_piece_rows(None, Some(300), 10_000), 10_000);
    assert_eq!(cap_piece_rows(Some(1), None, 10_000), 10_000);
}

#[test]
fn row_group_rows_outside_fixed_rows_is_said_to_be_ignored() {
    use crate::config::{ParquetConfig, RowGroupStrategy};
    let pc = |s: Option<RowGroupStrategy>, rows: Option<usize>| ParquetConfig {
        row_group_strategy: s,
        row_group_rows: rows,
        target_row_group_mb: None,
        max_row_group_mb: None,
    };
    let w = super::ignored_row_group_rows_warning(&pc(None, Some(500))).expect("auto ignores it");
    assert!(
        w.starts_with(
            "parquet.row_group_rows is ignored under row_group_strategy: auto (the default)"
        ),
        "{w}"
    );
    assert!(
        super::ignored_row_group_rows_warning(&pc(Some(RowGroupStrategy::FixedMemory), Some(5)))
            .is_some_and(|w| w.contains("row_group_strategy: fixed_memory —"))
    );
    assert!(
        super::ignored_row_group_rows_warning(&pc(Some(RowGroupStrategy::FixedRows), Some(5)))
            .is_none()
    );
    assert!(super::ignored_row_group_rows_warning(&pc(None, None)).is_none());
}

/// Only a Parquet sink under a byte cap gets row groups small enough to rotate; CSV has none.
#[test]
fn a_byte_capped_parquet_sink_sizes_its_row_groups_and_a_csv_sink_does_not() {
    use arrow::datatypes::{DataType, Field};
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let capped = |format_type| {
        let mut sink = ExportSink {
            format_type,
            max_file_size: Some(1024),
            ..minimal_sink()
        };
        crate::source::BatchSink::on_schema(&mut sink, schema.clone()).unwrap();
        sink.parquet_row_group_rows
    };
    assert!(
        capped(crate::config::FormatType::Parquet).is_some_and(|r| r < 1024),
        "parquet: {:?}",
        capped(crate::config::FormatType::Parquet)
    );
    assert_eq!(capped(crate::config::FormatType::Csv), None);
}

/// A sink holding two rotated parts and an open tail of one row each.
fn sink_with_three_parts() -> ExportSink {
    let (mut sink, schema) = sink_with_partition_budget(1);
    sink.on_schema(schema.clone()).unwrap();
    sink.on_batch_inner(&day_batch(&schema, vec![Some(0), Some(1), Some(2)]))
        .unwrap();
    sink
}

/// Sealing hands over every rotated part plus the open tail, and leaves the sink empty.
#[test]
fn seal_parts_hands_over_every_part_and_the_tail_once() {
    let mut sink = sink_with_three_parts();
    let parts = sink.seal_parts().unwrap();
    assert_eq!(
        parts.iter().map(|p| p.rows).collect::<Vec<_>>(),
        vec![1, 1, 1],
        "two rotated parts and the sealed tail"
    );
    assert_eq!(sink.part_rows, 0, "the tail is no longer open");
    assert!(
        sink.seal_parts().unwrap().is_empty(),
        "a second seal hands over nothing"
    );
}

/// A failed part write still hands back every part that reached the destination first.
#[test]
fn write_sink_parts_returns_the_durable_parts_beside_a_later_failure() {
    let mut sink = sink_with_three_parts();
    let dest = crate::pipeline::commit::tests::NthWriteFails::after(1);
    let (parts, wrote) =
        crate::pipeline::commit::write_sink_parts(&dest, &mut sink, None, |i, n| {
            crate::pipeline::commit::part_indexed_name("t.csv", i, n)
        });
    assert!(wrote.is_err(), "the second part's refusal is the outcome");
    assert_eq!(
        parts
            .iter()
            .map(|p| p.file_name.as_str())
            .collect::<Vec<_>>(),
        vec!["t_p0.csv"],
        "exactly the part that landed before the failure"
    );
}

#[test]
fn a_cursor_column_that_matches_only_ignoring_case_is_refused() {
    use crate::source::BatchSink;
    let mut sink = ExportSink {
        cursor_column: Some("updatedat".into()),
        ..minimal_sink()
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        "updatedAt",
        DataType::Int64,
        false,
    )]));
    let err = sink.on_schema(schema).unwrap_err();
    assert!(
        err.to_string().contains("Set `cursor_column: updatedAt`"),
        "{err}"
    );

    let mut absent = ExportSink {
        cursor_column: Some("updatedat".into()),
        ..minimal_sink()
    };
    absent
        .on_schema(Arc::new(Schema::empty()))
        .expect("a zero-row empty schema still opens");
}

/// A `columns:` key that matches a result column only ignoring case is refused, not silently dropped.
#[test]
fn a_column_override_key_spelled_in_another_case_is_refused_at_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int64, false),
        Field::new("CREATED_AT", DataType::Utf8, true),
    ]));
    let mut plan = crate::plan::test_support::test_plan();
    plan.column_overrides
        .insert("created_at".into(), crate::types::RivetType::String);
    let err = ExportSink::new(&plan)
        .unwrap()
        .on_schema(Arc::clone(&schema))
        .expect_err("a case-only match must be refused");
    assert_eq!(
        crate::error::error_code(&err),
        Some("RIVET_CONFIG_COLUMN_OVERRIDE_CASE"),
        "{err:#}"
    );
    assert!(
        format!("{err:#}").contains("Spell the key `CREATED_AT`"),
        "{err:#}"
    );

    let mut exact = crate::plan::test_support::test_plan();
    for key in ["CREATED_AT", "not_there"] {
        exact
            .column_overrides
            .insert(key.into(), crate::types::RivetType::String);
    }
    ExportSink::new(&exact)
        .unwrap()
        .on_schema(schema)
        .expect("an exact key, or one naming no column in any case, passes");
}
