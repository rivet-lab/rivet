pub mod csv;
pub mod parquet;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::{CompressionType, FormatType};
use crate::error::Result;

/// Streaming writer: receives one RecordBatch at a time.
pub trait FormatWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<()>;
    /// Record a key/value in the file's footer, where the format has one.
    fn note(&mut self, _key: &str, _value: &str) {}
    /// Approximate bytes written so far (for file-size splitting).
    fn bytes_written(&self) -> u64;
}

pub trait Format {
    fn create_writer(
        &self,
        schema: &SchemaRef,
        writer: Box<dyn std::io::Write + Send>,
    ) -> Result<Box<dyn FormatWriter + Send>>;

    fn file_extension(&self) -> &str;
}

/// The coded refusal for the first of `columns` that `format` cannot write.
pub(crate) fn refuse_unwritable_columns<'a>(
    format: FormatType,
    columns: impl IntoIterator<Item = (&'a str, &'a arrow::datatypes::DataType)>,
) -> Result<()> {
    for (name, dt) in columns {
        match format {
            FormatType::Csv => csv::refuse_column(name, dt)?,
            FormatType::Parquet => parquet::refuse_column(name, dt)?,
        }
    }
    Ok(())
}

pub fn create_format(
    format_type: FormatType,
    compression: CompressionType,
    compression_level: Option<u32>,
    row_group_rows: Option<usize>,
) -> Box<dyn Format> {
    match format_type {
        FormatType::Csv => Box::new(csv::CsvFormat),
        FormatType::Parquet => Box::new(parquet::ParquetFormat::new(
            compression,
            compression_level,
            row_group_rows,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(schema: &Arc<Schema>) -> arrow::record_batch::RecordBatch {
        arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
        )
        .unwrap()
    }

    fn refusal_code(r: Result<()>) -> Option<&'static str> {
        crate::error::error_code(&r.unwrap_err())
    }

    /// The shared check refuses what each writer refuses, by the same code and message, and nothing else.
    #[test]
    fn unwritable_columns_are_refused_by_format_with_the_writers_own_message() {
        let dec256 = DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )));
        let neg = DataType::Decimal128(10, -2);
        let cols = |dt: &DataType| vec![("c", dt.clone())];
        let check = |f: FormatType, dt: &DataType| {
            let cols = cols(dt);
            refuse_unwritable_columns(f, cols.iter().map(|(n, d)| (*n, d)))
        };
        assert_eq!(
            refusal_code(check(FormatType::Csv, &dec256)),
            Some("RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED")
        );
        assert_eq!(
            refusal_code(check(FormatType::Parquet, &neg)),
            Some("RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED")
        );
        for (f, dt) in [
            (FormatType::Parquet, dec256.clone()),
            (FormatType::Csv, DataType::Int64),
            (FormatType::Csv, DataType::Decimal128(10, 2)),
            (FormatType::Parquet, DataType::Decimal128(10, 2)),
            (FormatType::Parquet, DataType::Int64),
        ] {
            assert!(check(f, &dt).is_ok(), "{f:?} {dt:?} is written today");
        }
        let schema = Arc::new(Schema::new(vec![Field::new("c", dec256.clone(), true)]));
        let writer_err = create_format(FormatType::Csv, CompressionType::None, None, None)
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .err()
            .expect("csv writer refuses");
        let seam_err = check(FormatType::Csv, &dec256).unwrap_err();
        assert_eq!(format!("{writer_err:#}"), format!("{seam_err:#}"));
        let schema = Arc::new(Schema::new(vec![Field::new("c", neg.clone(), true)]));
        let writer_err = create_format(FormatType::Parquet, CompressionType::None, None, None)
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .err()
            .expect("parquet writer refuses");
        let seam_err = check(FormatType::Parquet, &neg).unwrap_err();
        assert_eq!(format!("{writer_err:#}"), format!("{seam_err:#}"));
    }

    #[test]
    fn create_format_csv_extension_and_roundtrip() {
        let schema = schema();
        let fmt = create_format(FormatType::Csv, CompressionType::None, None, None);
        assert_eq!(fmt.file_extension(), "csv");
        let mut w = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        w.write_batch(&batch(&schema)).unwrap();
        w.finish().unwrap();
    }

    #[test]
    fn create_format_parquet_extension_and_roundtrip() {
        let schema = schema();
        let fmt = create_format(FormatType::Parquet, CompressionType::Zstd, None, None);
        assert_eq!(fmt.file_extension(), "parquet");
        let mut w = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        w.write_batch(&batch(&schema)).unwrap();
        w.finish().unwrap();
    }

    #[test]
    fn create_format_parquet_uncompressed_finish_ok() {
        let schema = schema();
        let fmt = create_format(FormatType::Parquet, CompressionType::None, None, None);
        let w = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        w.finish().unwrap();
    }
}
