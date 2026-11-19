use std::io::Write;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::config::CompressionType;
use crate::error::Result;

pub struct ParquetFormat {
    compression: CompressionType,
    compression_level: Option<u32>,
    /// Rows per Parquet row group.
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
            CompressionType::Lz4 => Compression::LZ4_RAW,
            CompressionType::None => Compression::UNCOMPRESSED,
        }
    }
}

/// The coded refusal for a column Parquet cannot write: a negative decimal scale.
pub(crate) fn refuse_column(name: &str, dt: &arrow::datatypes::DataType) -> Result<()> {
    use arrow::datatypes::DataType::{Decimal128, Decimal256};
    if !matches!(dt, Decimal128(_, s) | Decimal256(_, s) if *s < 0) {
        return Ok(());
    }
    crate::rivet_bail!(
        crate::error::codes::CONFIG_FORMAT_COLUMN_UNSUPPORTED,
        "Parquet cannot write column '{name}' ({dt:?}): a NEGATIVE decimal scale is valid in \
         the source and in Arrow but not in the Parquet decimal type (scale must be >= 0). \
         Cast the column to a non-negative scale in the query (e.g. \
         `round(col)::numeric(p,0)`), or use `format: csv`."
    )
}

pub struct ParquetFormatWriter {
    inner: ArrowWriter<Box<dyn Write + Send>>,
}

impl super::Format for ParquetFormat {
    fn create_writer(
        &self,
        schema: &SchemaRef,
        writer: Box<dyn Write + Send>,
    ) -> Result<Box<dyn super::FormatWriter + Send>> {
        for field in schema.fields() {
            refuse_column(field.name(), field.data_type())?;
        }
        let builder = WriterProperties::builder()
            .set_compression(self.build_compression())
            .set_created_by("rivet".to_string());
        let mut builder = seeded!(
            "parquet-writer-bytes-move-from-release",
            builder.set_data_page_row_count_limit(10_000),
            builder
        );
        if self.row_group_rows.is_some() {
            builder = builder.set_max_row_group_row_count(self.row_group_rows);
        }
        let props = builder.build();

        let inner = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;
        Ok(Box::new(ParquetFormatWriter { inner }))
    }

    fn file_extension(&self) -> &str {
        "parquet"
    }
}

impl super::FormatWriter for ParquetFormatWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write(batch)?;
        Ok(())
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
        seeded!(
            "parquet-writer-reports-no-bytes",
            0,
            self.inner.bytes_written() as u64
        )
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

    #[test]
    fn file_extension_is_parquet() {
        assert_eq!(
            ParquetFormat::new(CompressionType::None, None, None).file_extension(),
            "parquet"
        );
    }

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

    #[test]
    fn write_batch_and_finish_returns_ok() {
        let schema = int64_schema();
        let fmt = ParquetFormat::new(CompressionType::Zstd, None, None);
        let mut writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.write_batch(&one_batch(&schema)).unwrap();
        writer.finish().unwrap();
    }

    /// A note lands in the file footer's key/value metadata, where a loader reads it back.
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
        let writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.finish().unwrap();
    }

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
        assert_eq!(
            ParquetFormat::new(CompressionType::Lz4, None, None).build_compression(),
            Compression::LZ4_RAW
        );
    }

    #[test]
    fn negative_scale_decimal_is_refused_loudly_at_writer_creation() {
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
        let cb = created_by.unwrap();
        assert!(
            !cb.contains("version") && !cb.contains("parquet"),
            "created_by must not embed the library version: {cb:?}"
        );
    }
}
