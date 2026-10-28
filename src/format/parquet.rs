use std::io::Write;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::{
    DEFAULT_MAX_ROW_GROUP_ROW_COUNT, DEFAULT_WRITE_BATCH_SIZE, WriterProperties,
};

use crate::config::CompressionType;
use crate::error::Result;

pub struct ParquetFormat {
    compression: CompressionType,
    compression_level: Option<u32>,
    /// Rows per Parquet row group. `None` = use library default (1,048,576).
    row_group_rows: Option<usize>,
}

impl ParquetFormat {
    pub fn new(
        compression: CompressionType,
        compression_level: Option<u32>,
        row_group_rows: Option<usize>,
    ) -> Self {
        Self {
            compression,
            compression_level,
            row_group_rows,
        }
    }

    fn writer_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_compression(self.build_compression())
            .set_created_by("rivet".to_string());
        if self.row_group_rows.is_some() {
            builder = builder.set_max_row_group_row_count(self.row_group_rows);
        }
        builder.build()
    }

    fn build_compression(&self) -> Compression {
        match self.compression {
            CompressionType::Zstd => {
                let level = self.compression_level.unwrap_or(3) as i32;
                Compression::ZSTD(ZstdLevel::try_new(level).unwrap_or_default())
            }
            CompressionType::Snappy => Compression::SNAPPY,
            CompressionType::Gzip => {
                let level = self.compression_level.unwrap_or(6);
                Compression::GZIP(GzipLevel::try_new(level).unwrap_or_default())
            }
            // LZ4_RAW, the standard ecosystem-supported codec — NOT the deprecated
            // Hadoop-framed `Compression::LZ4`, which DuckDB/Spark/pyarrow readers
            // may reject or mis-frame. rivet is pre-1.0, so the on-disk-bytes change
            // is acceptable; `lz4` means the interoperable codec.
            CompressionType::Lz4 => Compression::LZ4_RAW,
            CompressionType::None => Compression::UNCOMPRESSED,
        }
    }
}

pub struct ParquetFormatWriter {
    inner: ArrowWriter<Box<dyn Write + Send>>,
    group_rows: usize,
}

/// The two calls `write_in_spans` needs of an `ArrowWriter`.
trait SpanSink {
    fn in_progress_rows(&self) -> usize;
    fn write(&mut self, batch: &RecordBatch) -> Result<()>;
}

impl SpanSink for ArrowWriter<Box<dyn Write + Send>> {
    fn in_progress_rows(&self) -> usize {
        ArrowWriter::in_progress_rows(self)
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        ArrowWriter::write(self, batch)?;
        Ok(())
    }
}

/// Writes `batch` in calls of at most one column-writer mini-batch, none past a row group end.
///
/// Parquet 59.1+ pins a call's whole decimal/fixed-width arena for as long as a chunk's min/max
/// statistic lives, so a call stays small; the cuts match the library's own, the bytes stay equal.
fn write_in_spans(sink: &mut impl SpanSink, batch: &RecordBatch, group_rows: usize) -> Result<()> {
    let mut at = 0;
    while at < batch.num_rows() {
        let room = group_rows.saturating_sub(sink.in_progress_rows()).max(1);
        let len = (batch.num_rows() - at)
            .min(room)
            .min(DEFAULT_WRITE_BATCH_SIZE);
        sink.write(&batch.slice(at, len))?;
        at += len;
    }
    Ok(())
}

impl super::Format for ParquetFormat {
    fn create_writer(
        &self,
        schema: &SchemaRef,
        writer: Box<dyn Write + Send>,
    ) -> Result<Box<dyn super::FormatWriter + Send>> {
        // Parquet's decimal logical type requires scale >= 0. Arrow (and PostgreSQL
        // `numeric(p,-s)`) allow a NEGATIVE scale, so a negative-scale column resolves
        // to a valid Arrow type and `rivet check` passes — but the parquet writer then
        // crashes mid-export on the first batch. Refuse it LOUDLY here (at writer
        // creation, before any row is written) with an actionable message, so check
        // and run agree. CSV renders it fine, so this is parquet-specific.
        if let Some(field) = schema.fields().iter().find(|f| {
            matches!(
                f.data_type(),
                arrow::datatypes::DataType::Decimal128(_, s)
                | arrow::datatypes::DataType::Decimal256(_, s) if *s < 0
            )
        }) {
            crate::rivet_bail!(
                crate::error::codes::CONFIG_FORMAT_COLUMN_UNSUPPORTED,
                "Parquet cannot write column '{}' ({:?}): a NEGATIVE decimal scale is valid in \
                 the source and in Arrow but not in the Parquet decimal type (scale must be >= 0). \
                 Cast the column to a non-negative scale in the query (e.g. \
                 `round(col)::numeric(p,0)`), or use `format: csv`.",
                field.name(),
                field.data_type()
            );
        }
        // OPT-5: pin a version-independent `created_by`. By default parquet-rs
        // stamps each file with its own version (e.g. "parquet-rs version
        // 58.0.0"); that string changes on a lib bump, so identical rows would
        // produce different bytes — breaking the manifest `content_fingerprint`
        // as a *cross-release* dedup key. A constant keeps identical rows
        // byte-identical across rivet/parquet-rs versions. Writer provenance
        // lives in the run manifest/journal, not the file footer.
        let inner = ArrowWriter::try_new(writer, schema.clone(), Some(self.writer_properties()))?;
        let group_rows = self
            .row_group_rows
            .unwrap_or(DEFAULT_MAX_ROW_GROUP_ROW_COUNT);
        Ok(Box::new(ParquetFormatWriter { inner, group_rows }))
    }

    fn file_extension(&self) -> &str {
        "parquet"
    }
}

impl super::FormatWriter for ParquetFormatWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        write_in_spans(&mut self.inner, batch, self.group_rows)
    }

    fn finish(self: Box<Self>) -> Result<()> {
        self.inner.close()?;
        Ok(())
    }

    fn note(&mut self, key: &str, value: &str) {
        self.inner
            .append_key_value_metadata(parquet::file::metadata::KeyValue::new(
                key.to_string(),
                value.to_string(),
            ));
    }

    fn bytes_written(&self) -> u64 {
        self.inner.bytes_written() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::Format;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn int64_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn one_batch(schema: &Arc<Schema>) -> arrow::record_batch::RecordBatch {
        arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap()
    }

    fn make_writer(
        compression: CompressionType,
        level: Option<u32>,
    ) -> Box<dyn crate::format::FormatWriter> {
        let schema = int64_schema();
        ParquetFormat::new(compression, level, None)
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .expect("create_writer should succeed")
    }

    // ── file_extension ───────────────────────────────────────────────────────

    #[test]
    fn file_extension_is_parquet() {
        assert_eq!(
            ParquetFormat::new(CompressionType::None, None, None).file_extension(),
            "parquet"
        );
    }

    // ── create_writer succeeds for every compression codec ───────────────────

    #[test]
    fn create_writer_zstd_default_level_succeeds() {
        let _ = make_writer(CompressionType::Zstd, None);
    }

    #[test]
    fn create_writer_zstd_explicit_level_succeeds() {
        let _ = make_writer(CompressionType::Zstd, Some(9));
    }

    #[test]
    fn create_writer_snappy_succeeds() {
        let _ = make_writer(CompressionType::Snappy, None);
    }

    #[test]
    fn create_writer_gzip_succeeds() {
        let _ = make_writer(CompressionType::Gzip, None);
    }

    #[test]
    fn create_writer_lz4_succeeds() {
        let _ = make_writer(CompressionType::Lz4, None);
    }

    #[test]
    fn create_writer_uncompressed_succeeds() {
        let _ = make_writer(CompressionType::None, None);
    }

    // ── write_batch + finish ─────────────────────────────────────────────────

    #[test]
    fn write_batch_and_finish_returns_ok() {
        let schema = int64_schema();
        let fmt = ParquetFormat::new(CompressionType::Zstd, None, None);
        // Pass Vec by value — avoids &mut T 'static lifetime requirement.
        let mut writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.write_batch(&one_batch(&schema)).unwrap();
        writer.finish().unwrap(); // finalizes the parquet file footer
    }

    /// A note lands in the file footer's key/value metadata, where a loader reads it
    /// back — the sink records the partitions a part holds this way.
    #[test]
    fn a_note_is_written_into_the_footer() {
        use parquet::file::reader::FileReader as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("noted.parquet");
        let schema = int64_schema();
        let fmt = ParquetFormat::new(CompressionType::None, None, None);
        let mut writer = fmt
            .create_writer(&schema, Box::new(std::fs::File::create(&path).unwrap()))
            .unwrap();
        writer.write_batch(&one_batch(&schema)).unwrap();
        writer.note("rivet.partition_buckets", "d|day|3");
        writer.finish().unwrap();

        let reader =
            parquet::file::reader::SerializedFileReader::new(std::fs::File::open(&path).unwrap())
                .unwrap();
        let kv = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap();
        let note = kv
            .iter()
            .find(|kv| kv.key == "rivet.partition_buckets")
            .and_then(|kv| kv.value.clone());
        assert_eq!(note.as_deref(), Some("d|day|3"));
    }

    #[test]
    fn finish_without_write_produces_valid_empty_parquet() {
        let schema = int64_schema();
        let fmt = ParquetFormat::new(CompressionType::None, None, None);
        // finish() on a writer with no batches should not panic or error
        let writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.finish().unwrap();
    }

    // ── row group size ───────────────────────────────────────────────────────

    #[test]
    fn row_group_rows_none_uses_library_default() {
        let schema = int64_schema();
        let fmt = ParquetFormat::new(CompressionType::None, None, None);
        let mut writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.write_batch(&one_batch(&schema)).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn row_group_rows_some_succeeds() {
        let schema = int64_schema();
        let fmt = ParquetFormat::new(CompressionType::None, None, Some(100));
        let mut writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.write_batch(&one_batch(&schema)).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn lz4_maps_to_the_standard_raw_codec_not_hadoop_framed() {
        // RED before the fix: `lz4` mapped to the DEPRECATED Hadoop-framed
        // Compression::LZ4, which DuckDB/Spark/pyarrow readers may reject. It must
        // be the standard, interoperable LZ4_RAW.
        assert_eq!(
            ParquetFormat::new(CompressionType::Lz4, None, None).build_compression(),
            Compression::LZ4_RAW
        );
    }

    #[test]
    fn negative_scale_decimal_is_refused_loudly_at_writer_creation() {
        // RED before the guard: a negative-scale decimal is a valid Arrow type (so
        // `check` passes) but Parquet's decimal type requires scale >= 0, so the
        // writer crashed mid-export. It must bail LOUDLY at creation, before any row.
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, -2),
            true,
        )]));
        let result = ParquetFormat::new(CompressionType::None, None, None)
            .create_writer(&schema, Box::new(Vec::<u8>::new()));
        assert!(
            result.is_err(),
            "a negative-scale decimal must be refused, not crash mid-export"
        );
        crate::error::assert_coded(
            result.as_ref().err().unwrap(),
            "RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED",
            1,
        );
        let msg = result.err().unwrap().to_string();
        assert!(
            msg.contains("NEGATIVE decimal scale") && msg.contains("amount"),
            "error must name the column + the negative-scale cause: {msg}"
        );
    }

    // ── OPT-5: byte-determinism for the manifest content_fingerprint ──────────

    fn write_batch_to_bytes(compression: CompressionType) -> Vec<u8> {
        let schema = int64_schema();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::create(tmp.path()).unwrap();
        let mut w = ParquetFormat::new(compression, None, None)
            .create_writer(&schema, Box::new(file))
            .unwrap();
        w.write_batch(&one_batch(&schema)).unwrap();
        w.finish().unwrap();
        std::fs::read(tmp.path()).unwrap()
    }

    #[test]
    fn output_is_byte_deterministic_for_identical_rows() {
        // Identical rows must produce byte-identical Parquet so the manifest
        // `content_fingerprint` (xxh3 of the file bytes) is a stable dedup key.
        let a = write_batch_to_bytes(CompressionType::Zstd);
        let b = write_batch_to_bytes(CompressionType::Zstd);
        assert_eq!(a, b, "identical rows must yield byte-identical parquet");
    }

    #[test]
    fn created_by_is_pinned_and_version_free() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let bytes = write_batch_to_bytes(CompressionType::None);
        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        let created_by = reader.metadata().file_metadata().created_by();
        assert_eq!(
            created_by,
            Some("rivet"),
            "created_by must be the pinned constant"
        );
        // Must not leak the parquet-rs version — that's the cross-release drift
        // that would break the fingerprint as a dedup key.
        let cb = created_by.unwrap();
        assert!(
            !cb.contains("version") && !cb.contains("parquet"),
            "created_by must not embed the library version: {cb:?}"
        );
    }

    // ── write spans: bounded arena pins, unchanged bytes ─────────────────────

    struct Recorder {
        group_rows: usize,
        in_group: usize,
        calls: Vec<usize>,
    }

    impl SpanSink for Recorder {
        fn in_progress_rows(&self) -> usize {
            self.in_group
        }

        fn write(&mut self, batch: &RecordBatch) -> Result<()> {
            assert!(
                self.in_group + batch.num_rows() <= self.group_rows,
                "a call crossed a group end"
            );
            self.in_group = (self.in_group + batch.num_rows()) % self.group_rows;
            self.calls.push(batch.num_rows());
            Ok(())
        }
    }

    #[test]
    fn a_batch_is_written_in_mini_batch_calls_that_end_at_every_group_end() {
        for (rows, group_rows) in [
            (150_000, 275_000),
            (150_000, 7),
            (10, 275_000),
            (5_000, 1_024),
            (3_000, 1_000),
        ] {
            let mut sink = Recorder {
                group_rows,
                in_group: 0,
                calls: vec![],
            };
            write_in_spans(&mut sink, &mixed_batch(1, rows), group_rows).unwrap();
            assert_eq!(sink.calls.iter().sum::<usize>(), rows);
            assert!(
                sink.calls
                    .iter()
                    .all(|&n| (1..=DEFAULT_WRITE_BATCH_SIZE).contains(&n))
            );
            assert_eq!(sink.in_group, rows % group_rows);
        }
    }

    fn mixed_batch(lo: i64, n: usize) -> arrow::record_batch::RecordBatch {
        use arrow::array::{
            Decimal128Array, FixedSizeBinaryArray, StringArray, TimestampMicrosecondArray,
        };
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        let ids = || (lo..lo + n as i64).map(i128::from);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
            Field::new("d19", DataType::Decimal128(19, 0), false),
            Field::new("d38", DataType::Decimal128(38, 2), true),
            Field::new("u", DataType::FixedSizeBinary(16), false),
            Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(lo..lo + n as i64)),
                Arc::new(StringArray::from_iter(
                    (lo..lo + n as i64).map(|i| (i % 7 != 0).then(|| format!("v{}", i % 40))),
                )),
                Arc::new(
                    Decimal128Array::from_iter_values(ids())
                        .with_precision_and_scale(19, 0)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from_iter(ids().map(|i| (i % 5 != 0).then_some(i * 7)))
                        .with_precision_and_scale(38, 2)
                        .unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(
                        ids().map(|i| (i.wrapping_mul(0x9E37_79B9) as u128).to_le_bytes()),
                    )
                    .unwrap(),
                ),
                Arc::new(TimestampMicrosecondArray::from_iter_values(
                    (lo..lo + n as i64).map(|i| i * 1_000_003),
                )),
            ],
        )
        .unwrap()
    }

    /// The same batches handed to a bare ArrowWriter whole, as before the span split.
    fn whole_batch_bytes(
        fmt: &ParquetFormat,
        batches: &[arrow::record_batch::RecordBatch],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        let mut w =
            ArrowWriter::try_new(&mut out, batches[0].schema(), Some(fmt.writer_properties()))
                .unwrap();
        for b in batches {
            w.write(b).unwrap();
        }
        w.close().unwrap();
        out
    }

    #[test]
    fn spans_leave_the_file_byte_identical_to_whole_batch_writes() {
        // Batches straddle row group ends, and a group holds several 20,000-row pages.
        let batches = [
            mixed_batch(1, 30_000),
            mixed_batch(30_001, 50_000),
            mixed_batch(80_001, 30_000),
            mixed_batch(110_001, 9_000),
        ];
        for (groups, codec) in [
            (Some(45_000), CompressionType::Zstd),
            (Some(26_000), CompressionType::None),
            (Some(777), CompressionType::Snappy),
            (None, CompressionType::Zstd),
        ] {
            let fmt = ParquetFormat::new(codec, None, groups);
            let tmp = tempfile::NamedTempFile::new().unwrap();
            let mut w = fmt
                .create_writer(
                    &batches[0].schema(),
                    Box::new(std::fs::File::create(tmp.path()).unwrap()),
                )
                .unwrap();
            for b in &batches {
                w.write_batch(b).unwrap();
            }
            w.finish().unwrap();
            assert!(
                std::fs::read(tmp.path()).unwrap() == whole_batch_bytes(&fmt, &batches),
                "row groups {groups:?}: spans changed the bytes"
            );
        }
    }
}
