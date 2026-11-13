//! **Layer: Execution**

mod cursor;
mod pipelined;
pub(crate) use cursor::{extract_first_cursor_value, extract_key_tuple, extract_last_cursor_value};
pub(crate) use pipelined::PipelinedSink;

use std::io::BufWriter;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::config::IncrementalCursorMode;
use crate::enrich;
use crate::error::Result;
use crate::format::{self, FormatWriter};
use crate::plan::rollover::PartitionUnit;
use crate::plan::{
    CompressionType, ExtractionStrategy, FormatType, IncrementalCursorPlan, MetaColumns,
    ResolvedRunPlan,
};
use crate::quality::QualityTracker;
use crate::source::BatchSink;
use crate::types::refuse_override_case_miss;

pub(crate) struct CompletedPart {
    pub(in crate::pipeline) tmp: tempfile::NamedTempFile,
    pub(in crate::pipeline) rows: usize,
}

pub(crate) struct ExportSink {
    pub(in crate::pipeline) writer: Option<Box<dyn FormatWriter + Send>>,
    pub(in crate::pipeline) format_type: FormatType,
    pub(in crate::pipeline) compression: CompressionType,
    pub(in crate::pipeline) compression_level: Option<u32>,
    tmp: tempfile::NamedTempFile,
    pub(in crate::pipeline) total_rows: usize,
    /// The RUN-wide bytes-read counter, shared from `plan.bytes_read` (every sink this run creates.
    pub(in crate::pipeline) bytes_read: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// The RUN-wide count of parts at the destination, shared from `plan.parts_landed`.
    pub(in crate::pipeline) parts_landed: std::sync::Arc<std::sync::atomic::AtomicU64>,
    nulled_cells: std::sync::Arc<crate::plan::NulledCells>,
    part_rows: usize,
    /// Cursor column name (with internal columns), set from plan at construction.
    pub(in crate::pipeline) cursor_column: Option<String>,
    /// The composite key's columns after `cursor_column`; when non-empty the cursor is a JSON tuple.
    pub(in crate::pipeline) cursor_rest: Vec<String>,
    /// The `columns:` override keys, checked against the result's spelling at `on_schema`.
    override_columns: Vec<String>,
    /// Columns the settle window compares; must be date/timestamp.
    pub(in crate::pipeline) settle_columns: Vec<String>,
    /// Last extracted cursor value — set by `on_batch`, consumed by `run_single_export`.
    pub(in crate::pipeline) last_cursor_value: Option<String>,
    /// First observed cursor value of the RUN (first non-null of the first batch carrying one).
    pub(in crate::pipeline) first_cursor_value: Option<String>,
    /// A lossless keyset token the SOURCE reported via `set_source_cursor` (MongoDB's BSON `_id`), overriding the type-ambiguous string extracted from the output column.
    pub(in crate::pipeline) source_cursor: Option<String>,
    /// Schema WITH internal columns — used in `on_batch` for inline cursor extraction.
    pub(in crate::pipeline) schema: Option<SchemaRef>,
    /// Destination-facing schema (stripped of internal columns).
    pub(in crate::pipeline) dest_schema: Option<SchemaRef>,
    pub(in crate::pipeline) meta: MetaColumns,
    pub(in crate::pipeline) enriched_schema: Option<SchemaRef>,
    pub(in crate::pipeline) exported_at_us: i64,
    /// The declared quality rules and everything accumulated against them.
    pub(in crate::pipeline) quality: QualityTracker,
    pub(in crate::pipeline) max_file_size: Option<u64>,
    completed_parts: Vec<CompletedPart>,
    /// When set, this column is removed from Arrow batches before enrichment and write (the coalesce cursor).
    pub(in crate::pipeline) strip_internal_column: Option<String>,
    /// Running per-column max byte length for string/binary columns (Epic 8).
    pub(in crate::pipeline) column_max_bytes: std::collections::HashMap<String, u64>,
    /// Hard cap on a single Arrow batch in bytes (`max_batch_memory_mb * 1024²`).
    pub(in crate::pipeline) max_batch_memory_bytes: Option<usize>,
    /// Hard ceiling on a single cell/value in bytes (`max_value_mb * 1024²`).
    pub(in crate::pipeline) max_value_bytes: Option<usize>,
    pub(in crate::pipeline) batch_memory_policy: crate::tuning::BatchMemoryPolicy,
    /// Count of batches that exceeded `max_batch_memory_bytes` (for run summary / logging).
    pub(in crate::pipeline) oversized_batch_count: u64,
    /// Parquet row group config from plan.
    pub(in crate::pipeline) parquet_config: Option<crate::config::ParquetConfig>,
    /// Resolved rows-per-row-group, computed from schema in `on_schema`.
    pub(in crate::pipeline) parquet_row_group_rows: Option<usize>,
    /// Form B value-checksum accumulator: per-column xxh3 (keyed to the cursor column when present.
    pub(in crate::pipeline) column_checksums: std::collections::BTreeMap<String, u64>,
    /// Index of the cursor/key column in the dest batch (the pk for the keyed checksum), resolved in `on_schema`.
    pub(in crate::pipeline) checksum_key_col: Option<usize>,
    /// Per-batch row-progress feed (chunked exports).
    pub(in crate::pipeline) row_progress: Option<RowProgress>,
    /// The warehouse partition budget the CURRENT part is kept inside, and what it has spent — see [`PartBudget`].
    pub(in crate::pipeline) partition: PartBudget,
}

/// Whether the byte cap closes the current part.
fn should_split(written: u64, max_file_size: Option<u64>, part_rows: usize) -> bool {
    max_file_size.is_some_and(|max| written >= max) && part_rows > 0
}

/// Each row's partition bucket in `batch` under `rollover`.
pub(crate) fn batch_partition_buckets(
    batch: &RecordBatch,
    rollover: &crate::plan::rollover::PartitionRollover,
) -> Option<Vec<i64>> {
    let idx = batch.schema().index_of(&rollover.column).ok()?;
    let unit = partition_unit_of(batch.schema().field(idx).data_type())?;
    column_buckets(batch.column(idx).as_ref(), unit, rollover.granularity)
}

/// The partition bucket of every value of `col`, NULL rows in the NULL bucket.
fn column_buckets(
    col: &dyn arrow::array::Array,
    unit: PartitionUnit,
    granularity: crate::config::load::Granularity,
) -> Option<Vec<i64>> {
    use crate::plan::rollover::{NULL_BUCKET, bucket_of, to_epoch_seconds};
    let raw = partition_values(col)?;
    Some(
        (0..col.len())
            .map(|row| {
                if col.is_null(row) {
                    NULL_BUCKET
                } else {
                    bucket_of(to_epoch_seconds(raw[row], unit), granularity)
                }
            })
            .collect(),
    )
}

/// The row-group size under which `max_file_size` can rotate a part.
fn row_group_rows_for_cap(
    rows: Option<usize>,
    max_file_size: Option<u64>,
    row_bytes: usize,
) -> Option<usize> {
    let Some(max) = max_file_size else {
        return rows;
    };
    let fit = (usize::try_from(max / 4).unwrap_or(usize::MAX) / row_bytes.max(1)).max(1);
    Some(rows.map_or(fit, |r| r.min(fit)))
}

/// Rows per piece a batch is written in: one row group when a byte cap is set, else the whole batch.
fn cap_piece_rows(
    max_file_size: Option<u64>,
    group_rows: Option<usize>,
    batch_rows: usize,
) -> usize {
    match (max_file_size, group_rows) {
        (Some(_), Some(g)) => g.max(1),
        _ => batch_rows.max(1),
    }
}

/// The `(offset, len)` pieces of an `n`-row batch cut every `step` rows; an empty batch is one empty piece.
fn piece_ranges(n: usize, step: usize) -> Vec<(usize, usize)> {
    let step = step.max(1);
    let mut out: Vec<(usize, usize)> = (0..n).step_by(step).map(|o| (o, step.min(n - o))).collect();
    if out.is_empty() {
        out.push((0, 0));
    }
    out
}

/// The warning for `row_group_rows` set under a strategy that does not read it.
fn ignored_row_group_rows_warning(pc: &crate::config::ParquetConfig) -> Option<String> {
    let strategy = pc.row_group_strategy.unwrap_or_default();
    (pc.row_group_rows.is_some() && strategy != crate::config::RowGroupStrategy::FixedRows).then(
        || {
            format!(
                "parquet.row_group_rows is ignored under row_group_strategy: {}{} — row groups \
                 are sized from target_row_group_mb. Set `row_group_strategy: fixed_rows` to \
                 use a fixed row count.",
                match strategy {
                    crate::config::RowGroupStrategy::Auto => "auto",
                    crate::config::RowGroupStrategy::FixedMemory => "fixed_memory",
                    crate::config::RowGroupStrategy::FixedRows => "fixed_rows",
                },
                if pc.row_group_strategy.is_none() {
                    " (the default)"
                } else {
                    ""
                }
            )
        },
    )
}

/// The unit an Arrow date/timestamp type stores, or `None` for a type no warehouse partitions by.
fn partition_unit_of(data_type: &arrow::datatypes::DataType) -> Option<PartitionUnit> {
    use arrow::datatypes::{DataType, TimeUnit};
    match data_type {
        DataType::Date32 => Some(PartitionUnit::Days),
        DataType::Date64 => Some(PartitionUnit::Millis),
        DataType::Timestamp(TimeUnit::Second, _) => Some(PartitionUnit::Seconds),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(PartitionUnit::Millis),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(PartitionUnit::Micros),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(PartitionUnit::Nanos),
        _ => None,
    }
}

/// The partition column's raw stored values, widened to `i64`.
fn partition_values(col: &dyn arrow::array::Array) -> Option<Vec<i64>> {
    use arrow::array::{
        Date32Array, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    let any = col.as_any();
    if let Some(a) = any.downcast_ref::<Date32Array>() {
        return Some(a.values().iter().map(|v| i64::from(*v)).collect());
    }
    any.downcast_ref::<Date64Array>()
        .map(|a| a.values().to_vec())
        .or_else(|| {
            any.downcast_ref::<TimestampSecondArray>()
                .map(|a| a.values().to_vec())
        })
        .or_else(|| {
            any.downcast_ref::<TimestampMillisecondArray>()
                .map(|a| a.values().to_vec())
        })
        .or_else(|| {
            any.downcast_ref::<TimestampMicrosecondArray>()
                .map(|a| a.values().to_vec())
        })
        .or_else(|| {
            any.downcast_ref::<TimestampNanosecondArray>()
                .map(|a| a.values().to_vec())
        })
}

/// The warehouse partition budget the CURRENT part is kept inside, and what it has spent.
#[derive(Default)]
pub(in crate::pipeline) struct PartBudget {
    /// The declared budget; `None` leaves part sizing to `max_file_size` alone.
    pub(in crate::pipeline) rollover: Option<crate::plan::rollover::PartitionRollover>,
    /// The partition column's index in the DEST batch and the unit its Arrow type stores, resolved by [`Self::resolve`].
    pub(in crate::pipeline) col: Option<(usize, PartitionUnit)>,
    /// Distinct partitions the current part already holds — the budget spent so far.
    pub(in crate::pipeline) buckets: std::collections::HashSet<i64>,
}

impl PartBudget {
    pub(in crate::pipeline) fn new(
        rollover: Option<crate::plan::rollover::PartitionRollover>,
    ) -> Self {
        Self {
            rollover,
            ..Self::default()
        }
    }

    /// Resolve the partition column against the DEST schema.
    fn resolve(&mut self, dest_schema: &arrow::datatypes::Schema) {
        self.col = None;
        if dest_schema.fields().is_empty() {
            return;
        }
        if let Some(r) = self.rollover.clone() {
            match dest_schema.field_with_name(&r.column) {
                Ok(field) => match partition_unit_of(field.data_type()) {
                    Some(unit) => {
                        self.col = dest_schema.index_of(&r.column).ok().map(|i| (i, unit));
                    }
                    None => log::warn!(
                        "the load partitions by `{}`, which this export writes as {} — not a \
                         date or timestamp, so parts cannot be kept within the {}-partition \
                         load budget and a wide history will be refused at load time",
                        r.column,
                        field.data_type(),
                        r.cap
                    ),
                },
                Err(_) => log::warn!(
                    "the load partitions by `{}`, which this export does not produce — parts \
                     cannot be kept within the {}-partition load budget",
                    r.column,
                    r.cap
                ),
            }
        }
    }

    /// The partition bucket of every row of `batch`, with the budget they are counted against.
    fn buckets_for(&self, batch: &RecordBatch) -> Option<(Vec<i64>, usize)> {
        let (idx, unit) = self.col?;
        let r = self.rollover.as_ref()?;
        let buckets = column_buckets(batch.column(idx).as_ref(), unit, r.granularity)?;
        Some((buckets, r.cap))
    }

    /// What the closing part records in its footer.
    fn footer_note(&self) -> Option<String> {
        self.col?;
        let r = self.rollover.as_ref()?;
        Some(crate::plan::rollover::partition_buckets_note(
            &r.column,
            r.granularity,
            self.buckets.len(),
        ))
    }

    /// The budget the current part has already spent.
    fn held(&self) -> &std::collections::HashSet<i64> {
        &self.buckets
    }

    /// Count `buckets` against the current part.
    fn spend(&mut self, buckets: &[i64]) {
        self.buckets.extend(buckets.iter().copied());
    }

    /// A new part starts with its whole budget.
    fn reset(&mut self) {
        self.buckets.clear();
    }
}

/// Per-batch progress feed for chunked exports.
pub(in crate::pipeline) struct RowProgress {
    pub(in crate::pipeline) handle: crate::pipeline::progress::ChunkProgressHandle,
    pub(in crate::pipeline) streamed: Arc<std::sync::atomic::AtomicI64>,
    pub(in crate::pipeline) last_tick: std::time::Instant,
}

impl ExportSink {
    /// ADR-0029 — what this sink SAW: its dest schema and per-column max bytes.
    pub(in crate::pipeline) fn take_observations(
        &mut self,
    ) -> crate::pipeline::commit::Observations {
        crate::pipeline::commit::Observations {
            drift_schema: self.dest_schema.as_deref().cloned(),
            column_max_bytes: std::mem::take(&mut self.column_max_bytes),
            quality: std::mem::take(&mut self.quality),
        }
    }

    /// [`Self::take_observations`] without the schema.
    pub(in crate::pipeline) fn take_shape(&mut self) -> crate::pipeline::commit::Observations {
        crate::pipeline::commit::Observations {
            drift_schema: None,
            column_max_bytes: std::mem::take(&mut self.column_max_bytes),
            quality: std::mem::take(&mut self.quality),
        }
    }

    /// ADR-0029 — this sink's Form-B checksums and their key column.
    pub(in crate::pipeline) fn take_checksums(&mut self) -> crate::pipeline::commit::UnitChecksums {
        crate::pipeline::commit::UnitChecksums {
            key: self.checksum_key(),
            sums: std::mem::take(&mut self.column_checksums),
        }
    }

    /// Feed [`Self::take_observations`] into `ledger`.
    pub(in crate::pipeline) fn drain_observations_into(
        &mut self,
        ledger: &mut crate::pipeline::commit::CommitLedger,
    ) {
        ledger.observe(self.take_observations());
    }

    /// The column this sink's Form-B checksums are keyed to, or `None` when they are un-keyed.
    pub(in crate::pipeline) fn checksum_key(&self) -> Option<String> {
        self.checksum_key_col.and(self.cursor_column.clone())
    }

    pub fn new(plan: &ResolvedRunPlan) -> Result<Self> {
        let tmp = tempfile::NamedTempFile::new()?;
        let exported_at_us = chrono::Utc::now().timestamp_micros();
        let strip_internal_column = match &plan.strategy {
            ExtractionStrategy::Incremental(p) if p.mode == IncrementalCursorMode::Coalesce => {
                Some(IncrementalCursorPlan::RIVET_COALESCE_CURSOR_COL.to_string())
            }
            _ => None,
        };
        Ok(Self {
            writer: None,
            format_type: plan.format,
            compression: plan.compression,
            compression_level: plan.compression_level,
            tmp,
            total_rows: 0,
            bytes_read: std::sync::Arc::clone(&plan.bytes_read),
            parts_landed: std::sync::Arc::clone(&plan.parts_landed),
            nulled_cells: std::sync::Arc::clone(&plan.nulled_cells),
            part_rows: 0,
            cursor_column: plan.strategy.cursor_extract_column().map(str::to_string),
            cursor_rest: match &plan.strategy {
                ExtractionStrategy::Keyset(k) => k.key_rest.clone(),
                _ => Vec::new(),
            },
            override_columns: plan.column_overrides.keys().cloned().collect(),
            settle_columns: plan
                .strategy
                .incremental_plan()
                .map(|p| p.settle_columns())
                .unwrap_or_default(),
            last_cursor_value: None,
            first_cursor_value: None,
            source_cursor: None,
            schema: None,
            dest_schema: None,
            meta: plan.meta_columns.clone(),
            enriched_schema: None,
            exported_at_us,
            quality: QualityTracker::new(plan.quality.clone()),
            max_file_size: plan.max_file_size_bytes,
            completed_parts: Vec::new(),
            strip_internal_column,
            column_max_bytes: std::collections::HashMap::new(),
            max_batch_memory_bytes: plan.tuning.max_batch_memory_mb.map(|mb| mb * 1024 * 1024),
            max_value_bytes: plan.tuning.max_value_bytes(),
            batch_memory_policy: plan.tuning.on_batch_memory_exceeded,
            oversized_batch_count: 0,
            parquet_config: plan.parquet.clone(),
            parquet_row_group_rows: None,
            column_checksums: std::collections::BTreeMap::new(),
            checksum_key_col: None,
            row_progress: None,
            partition: PartBudget::new(plan.partition_rollover.clone()),
        })
    }

    /// Attach a per-batch row-progress feed (see [`RowProgress`]).
    pub(in crate::pipeline) fn with_row_progress(
        mut self,
        handle: crate::pipeline::progress::ChunkProgressHandle,
        streamed: Arc<std::sync::atomic::AtomicI64>,
    ) -> Self {
        self.row_progress = Some(RowProgress {
            handle,
            streamed,
            last_tick: std::time::Instant::now(),
        });
        self
    }

    fn schema_without_internal(schema: &Schema, name: &str) -> Result<SchemaRef> {
        let idx = schema.index_of(name)?;
        let fields: Vec<_> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, f)| f.as_ref().clone())
            .collect();
        Ok(Arc::new(Schema::new(fields)))
    }

    fn record_batch_without_internal(batch: &RecordBatch, name: &str) -> Result<RecordBatch> {
        let schema = batch.schema();
        let idx = schema.index_of(name)?;
        let indices: Vec<usize> = (0..schema.fields().len()).filter(|&i| i != idx).collect();
        batch.project(&indices).map_err(|e| {
            crate::error::coded(
                crate::error::codes::INTERNAL_RUN_INVARIANT,
                format!("project batch without {}: {}", name, e),
            )
        })
    }

    pub fn maybe_split(&mut self) -> Result<()> {
        let written = self.writer.as_ref().map(|w| w.bytes_written()).unwrap_or(0);
        if should_split(written, self.max_file_size, self.part_rows) {
            self.split_now()?;
        }
        Ok(())
    }

    /// Close the current part and open the next one, whatever asked for it — the byte cap or the partition budget.
    pub(in crate::pipeline) fn finish_writer(&mut self) -> Result<()> {
        if let Some(mut w) = self.writer.take() {
            if let Some(note) = self.partition.footer_note() {
                w.note(crate::plan::rollover::PARTITION_BUCKETS_KEY, &note);
            }
            w.finish()?;
        }
        Ok(())
    }

    /// Close the writer, seal the partial tail as the last part, and hand over every part.
    pub(in crate::pipeline) fn seal_parts(&mut self) -> Result<Vec<CompletedPart>> {
        self.finish_writer()?;
        if self.part_rows > 0 {
            self.completed_parts.push(CompletedPart {
                tmp: std::mem::replace(&mut self.tmp, tempfile::NamedTempFile::new()?),
                rows: self.part_rows,
            });
            self.part_rows = 0;
        }
        Ok(std::mem::take(&mut self.completed_parts))
    }

    pub(in crate::pipeline) fn split_now(&mut self) -> Result<()> {
        self.finish_writer()?;

        let old_tmp = std::mem::replace(&mut self.tmp, tempfile::NamedTempFile::new()?);
        self.completed_parts.push(CompletedPart {
            tmp: old_tmp,
            rows: self.part_rows,
        });
        self.part_rows = 0;
        self.partition.reset();

        if let Some(schema) = &self.enriched_schema {
            let fmt = format::create_format(
                self.format_type,
                self.compression,
                self.compression_level,
                self.parquet_row_group_rows,
            );
            let file = self.tmp.as_file().try_clone()?;
            let buf_writer = BufWriter::with_capacity(256 * 1024, file);
            self.writer = Some(fmt.create_writer(schema, Box::new(buf_writer))?);
        }

        log::info!(
            "file split: started part {}",
            self.completed_parts.len() + 1
        );
        Ok(())
    }

    pub fn track_quality(&mut self, batch: &RecordBatch) -> Result<()> {
        self.quality.track(batch)
    }

    /// Hard per-value guard (OPT-1): abort with `RIVET_VALUE_TOO_LARGE` when a single variable-length cell exceeds `max_value_bytes`.
    fn check_value_ceiling(&self, batch: &RecordBatch) -> Result<()> {
        use arrow::array::{BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
        use arrow::datatypes::DataType;

        let Some(limit) = self.max_value_bytes else {
            return Ok(());
        };
        let schema = batch.schema();
        for (idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(idx);
            let over: Option<usize> = match field.data_type() {
                DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .and_then(|a| a.iter().flatten().map(|s| s.len()).find(|&n| n > limit)),
                DataType::LargeUtf8 => col
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .and_then(|a| a.iter().flatten().map(|s| s.len()).find(|&n| n > limit)),
                DataType::Binary => col
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .and_then(|a| a.iter().flatten().map(|b| b.len()).find(|&n| n > limit)),
                DataType::LargeBinary => col
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .and_then(|a| a.iter().flatten().map(|b| b.len()).find(|&n| n > limit)),
                _ => None,
            };
            if let Some(value_bytes) = over {
                crate::rivet_bail!(
                    crate::error::codes::SOURCE_VALUE_TOO_LARGE,
                    "RIVET_VALUE_TOO_LARGE: column '{}' has a single value of {:.1} MB, exceeding the \
                     per-value ceiling of {} MB. One oversized cell can OOM the process regardless of \
                     batch size. Raise `tuning.max_value_mb` (or set it to 0 to disable the guard) if \
                     this value is expected.",
                    field.name(),
                    value_bytes as f64 / (1024.0 * 1024.0),
                    limit / (1024 * 1024),
                );
            }
        }
        Ok(())
    }

    /// Update the running per-column max byte length for string/binary columns.
    pub fn track_checksum(&mut self, batch: &RecordBatch) {
        use crate::source::value_checksum::{arrow_batch_checksums, arrow_batch_checksums_keyed};
        if self.format_type != FormatType::Parquet {
            return;
        }
        let sums = match self.checksum_key_col {
            Some(k) => arrow_batch_checksums_keyed(batch, k),
            None => arrow_batch_checksums(batch),
        };
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let e = self
                .column_checksums
                .entry(field.name().clone())
                .or_insert(0);
            *e = e.wrapping_add(sums[i]);
        }
    }

    pub fn track_shape(&mut self, batch: &RecordBatch) {
        use arrow::array::{BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
        use arrow::datatypes::DataType;

        let schema = batch.schema();
        for (idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(idx);
            let batch_max: Option<u64> = match field.data_type() {
                DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .and_then(|a| a.iter().flatten().map(|s| s.len() as u64).max()),
                DataType::LargeUtf8 => col
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .and_then(|a| a.iter().flatten().map(|s| s.len() as u64).max()),
                DataType::Binary => col
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .and_then(|a| a.iter().flatten().map(|b| b.len() as u64).max()),
                DataType::LargeBinary => col
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .and_then(|a| a.iter().flatten().map(|b| b.len() as u64).max()),
                _ => None,
            };
            if let Some(m) = batch_max {
                let entry = self
                    .column_max_bytes
                    .entry(field.name().clone())
                    .or_insert(0);
                if m > *entry {
                    *entry = m;
                }
            }
        }
    }

    /// This sink's own verdict; the product grades the run-wide merge at the finalize seam.
    #[cfg(test)]
    pub fn run_quality_checks(&self) -> Vec<crate::quality::QualityIssue> {
        self.quality.issues(self.total_rows)
    }

    /// Core batch processing: keep the part inside the load's partition budget, then track quality/shape, enrich and write.
    fn on_batch_inner(&mut self, dest_batch: &RecordBatch) -> Result<()> {
        let n = dest_batch.num_rows();
        let step = cap_piece_rows(self.max_file_size, self.parquet_row_group_rows, n);
        for (offset, len) in piece_ranges(n, step) {
            self.on_piece(&dest_batch.slice(offset, len))?;
        }
        Ok(())
    }

    /// Write one piece of a batch within the part's partition budget.
    fn on_piece(&mut self, dest_batch: &RecordBatch) -> Result<()> {
        let Some((buckets, cap)) = self.partition.buckets_for(dest_batch) else {
            return self.write_batch_part(dest_batch);
        };
        if buckets.is_empty() {
            return self.write_batch_part(dest_batch);
        }
        let mut offset = 0;
        while offset < buckets.len() {
            let fit = crate::plan::rollover::rows_that_fit(
                self.partition.held(),
                &buckets[offset..],
                cap,
            );
            if fit == 0 {
                self.split_now()?;
                continue;
            }
            self.partition.spend(&buckets[offset..offset + fit]);
            self.write_batch_part(&dest_batch.slice(offset, fit))?;
            offset += fit;
        }
        Ok(())
    }

    /// Write one slice that is known to fit the current part's partition budget.
    fn write_batch_part(&mut self, dest_batch: &RecordBatch) -> Result<()> {
        self.total_rows += dest_batch.num_rows();
        if let Some(rp) = self.row_progress.as_mut() {
            let n = dest_batch.num_rows() as i64;
            let total = rp
                .streamed
                .fetch_add(n, std::sync::atomic::Ordering::Relaxed)
                + n;
            if rp.last_tick.elapsed() >= std::time::Duration::from_millis(120) {
                rp.handle.set_rows(total);
                rp.last_tick = std::time::Instant::now();
            }
        }
        self.part_rows += dest_batch.num_rows();
        self.track_quality(dest_batch)?;
        self.track_shape(dest_batch);
        self.track_checksum(dest_batch);

        let output = if let Some(es) = &self.enriched_schema {
            enrich::enrich_batch(dest_batch, &self.meta, es, self.exported_at_us)?
        } else {
            dest_batch.clone()
        };

        if let Some(w) = self.writer.as_mut() {
            w.write_batch(&output)?;
        }
        self.maybe_split()?;
        Ok(())
    }
}

impl ExportSink {
    /// Apply memory-cap policy and write `dest_batch` (already stripped of internal columns).
    fn process_dest_batch(&mut self, dest_batch: &RecordBatch) -> Result<()> {
        if let Some(limit) = self.max_batch_memory_bytes {
            let batch_bytes = crate::tuning::SourceTuning::batch_memory_bytes(dest_batch);
            if batch_bytes > limit {
                let batch_mb = batch_bytes / (1024 * 1024);
                let limit_mb = limit / (1024 * 1024);
                let suggested = dest_batch
                    .num_rows()
                    .saturating_mul(limit)
                    .checked_div(batch_bytes)
                    .unwrap_or(1)
                    .max(1);
                match self.batch_memory_policy {
                    crate::tuning::BatchMemoryPolicy::Warn => {
                        log::warn!(
                            "batch memory {} MB exceeds max_batch_memory_mb={} MB \
                             ({} rows). Consider lowering batch_size to ~{}.",
                            batch_mb,
                            limit_mb,
                            dest_batch.num_rows(),
                            suggested
                        );
                    }
                    crate::tuning::BatchMemoryPolicy::Fail => {
                        crate::rivet_bail!(
                            crate::error::codes::CONFIG_BATCH_MEMORY_EXCEEDED,
                            "batch memory {} MB exceeds max_batch_memory_mb={} MB \
                             ({} rows). Lower batch_size to ~{} or set \
                             on_batch_memory_exceeded: auto_shrink.",
                            batch_mb,
                            limit_mb,
                            dest_batch.num_rows(),
                            suggested
                        );
                    }
                    crate::tuning::BatchMemoryPolicy::AutoShrink => {
                        let mid = dest_batch.num_rows() / 2;
                        if mid == 0 {
                            log::warn!(
                                "single-row batch is {} MB — cannot shrink further, writing as-is.",
                                batch_mb
                            );
                        } else {
                            let lo = dest_batch.slice(0, mid);
                            let hi = dest_batch.slice(mid, dest_batch.num_rows() - mid);
                            self.process_dest_batch(&lo)?;
                            self.process_dest_batch(&hi)?;
                            return Ok(());
                        }
                    }
                }
            }
        }
        self.on_batch_inner(dest_batch)
    }
}

impl BatchSink for ExportSink {
    fn note_nulled(&mut self, column: &str, n: u64) {
        self.nulled_cells.add(column, n);
    }

    fn on_schema(&mut self, schema: SchemaRef) -> Result<()> {
        let dest_schema = match &self.strip_internal_column {
            Some(strip) if schema.index_of(strip).is_ok() => {
                Self::schema_without_internal(schema.as_ref(), strip)?
            }
            _ => schema.clone(),
        };
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        for c in self.cursor_column.iter().chain(&self.cursor_rest) {
            crate::types::refuse_cursor_case_miss(c, &names)?;
        }
        refuse_override_case_miss(&self.override_columns, &names)?;
        for name in &self.settle_columns {
            if let Ok(field) = schema.field_with_name(name)
                && !matches!(
                    field.data_type(),
                    arrow::datatypes::DataType::Date32
                        | arrow::datatypes::DataType::Date64
                        | arrow::datatypes::DataType::Timestamp(_, _)
                )
            {
                crate::rivet_bail!(
                    crate::error::codes::CONFIG_SETTLE_COLUMN_NOT_TEMPORAL,
                    "settle column `{name}` is {}, not a date/timestamp — the settle window \
                     compares it against the source clock. Point `settle.column` at the row's \
                     insert/update time (the cursor is used when `settle.column` is omitted).",
                    field.data_type()
                );
            }
        }
        let enriched = enrich::enrich_schema(&dest_schema, &self.meta)?;
        if let Some(pc) = &self.parquet_config {
            self.parquet_row_group_rows = pc.effective_row_group_rows(&dest_schema);
            if let Some(rows) = self.parquet_row_group_rows {
                log::debug!(
                    "parquet row_group_rows={} (strategy={:?})",
                    rows,
                    pc.row_group_strategy.unwrap_or_default()
                );
            }
        }
        if self.format_type == FormatType::Parquet {
            self.parquet_row_group_rows = row_group_rows_for_cap(
                self.parquet_row_group_rows,
                self.max_file_size,
                crate::tuning::estimate_row_bytes(&dest_schema),
            );
            if let Some(w) = self
                .parquet_config
                .as_ref()
                .and_then(ignored_row_group_rows_warning)
            {
                static WARN_ONCE: std::sync::Once = std::sync::Once::new();
                WARN_ONCE.call_once(|| log::warn!("{w}"));
            }
        }
        let fmt = format::create_format(
            self.format_type,
            self.compression,
            self.compression_level,
            self.parquet_row_group_rows,
        );
        let file = self.tmp.as_file().try_clone()?;
        let buf_writer = BufWriter::new(file);
        self.writer = Some(fmt.create_writer(&enriched, Box::new(buf_writer))?);
        self.quality.resolve_columns(&dest_schema)?;
        self.checksum_key_col = self
            .cursor_column
            .as_ref()
            .and_then(|c| dest_schema.index_of(c).ok());
        self.partition.resolve(&dest_schema);
        let skips = crate::source::value_checksum::coverage_skips(&dest_schema);
        if !skips.is_empty() {
            log::warn!(
                "value checksum covers {}/{} columns; NOT checked: {}",
                dest_schema.fields().len() - skips.len(),
                dest_schema.fields().len(),
                skips
                    .iter()
                    .map(|(n, r)| format!("{n} ({r})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        self.schema = Some(schema);
        self.dest_schema = Some(dest_schema);
        self.enriched_schema = Some(enriched);
        Ok(())
    }

    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.bytes_read.fetch_add(
            batch.get_array_memory_size() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        let stripped: Option<RecordBatch> = match &self.strip_internal_column {
            Some(strip) if batch.schema().index_of(strip).is_ok() => {
                Some(Self::record_batch_without_internal(batch, strip)?)
            }
            _ => None,
        };
        let dest_batch: &RecordBatch = stripped.as_ref().unwrap_or(batch);

        self.check_value_ceiling(dest_batch)?;
        crate::source::refuse_decimal_overflow(dest_batch)?;

        if let Some(limit) = self.max_batch_memory_bytes
            && crate::tuning::SourceTuning::batch_memory_bytes(dest_batch) > limit
        {
            self.oversized_batch_count += 1;
        }

        self.process_dest_batch(dest_batch)?;

        if let (Some(col), Some(schema)) = (&self.cursor_column, &self.schema)
            && !self.cursor_rest.is_empty()
        {
            let columns: Vec<&str> = std::iter::once(col.as_str())
                .chain(self.cursor_rest.iter().map(String::as_str))
                .collect();
            if self.first_cursor_value.is_none() {
                self.first_cursor_value = extract_key_tuple(batch, &columns, schema, false);
            }
            if let Some(v) = extract_key_tuple(batch, &columns, schema, true) {
                self.last_cursor_value = Some(v);
            }
        } else if let (Some(col), Some(schema)) = (&self.cursor_column, &self.schema) {
            if self.first_cursor_value.is_none() {
                self.first_cursor_value = extract_first_cursor_value(batch, col, schema);
            }
            if let Some(v) = extract_last_cursor_value(batch, col, schema) {
                self.last_cursor_value = Some(v);
            }
        }
        Ok(())
    }

    fn set_source_cursor(&mut self, token: String) {
        self.source_cursor = Some(token);
    }
}

impl ExportSink {
    /// Whether the export streamed no rows into this sink.
    pub(in crate::pipeline) fn is_empty(&self) -> bool {
        self.total_rows == 0
    }

    /// The keyset high-water mark to advance/checkpoint from.
    pub(in crate::pipeline) fn effective_cursor(&self) -> Option<String> {
        self.source_cursor
            .clone()
            .or_else(|| self.last_cursor_value.clone())
    }
}

#[cfg(test)]
mod tests;
