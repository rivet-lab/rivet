//! The first `__pos` values of a staged Parquet part, read from one column chunk by ranged GETs.

use crate::destination::gcs::GcsStore;
use anyhow::{Context, Result};
use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use bytes::Bytes;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, Length};
use std::io::Cursor;
use std::sync::Arc;

/// Rows of `__pos` read from a part: a part is written by one process, so its head speaks for it.
const HEAD_ROWS: usize = 1024;

/// One column chunk held in memory at its file offset, served as the file.
struct Chunk {
    offset: u64,
    bytes: Bytes,
}

impl Length for Chunk {
    fn len(&self) -> u64 {
        self.offset + self.bytes.len() as u64
    }
}

impl Chunk {
    fn slice(&self, start: u64, len: Option<usize>) -> parquet::errors::Result<Bytes> {
        let outside = || ParquetError::General(format!("byte {start} is outside the chunk read"));
        let from = usize::try_from(start.checked_sub(self.offset).ok_or_else(outside)?)
            .map_err(|_| outside())?;
        let to = match len {
            Some(n) => from.checked_add(n).ok_or_else(outside)?,
            None => self.bytes.len(),
        };
        if from > to || to > self.bytes.len() {
            return Err(outside());
        }
        Ok(self.bytes.slice(from..to))
    }
}

impl ChunkReader for Chunk {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(Cursor::new(self.slice(start, None)?))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.slice(start, Some(length))
    }
}

/// The non-null `__pos` values at the head of the part at `key`, whose footer is `meta`; empty when the part has no such column or no rows.
pub(crate) fn head_positions(
    store: &GcsStore,
    key: &str,
    meta: &ParquetMetaData,
) -> Result<Vec<String>> {
    let schema = meta.file_metadata().schema_descr();
    let Some(leaf) = (0..schema.num_columns()).find(|&i| schema.column(i).name() == "__pos") else {
        return Ok(Vec::new());
    };
    if meta.num_row_groups() == 0 {
        return Ok(Vec::new());
    }
    let (start, len) = meta.row_group(0).column(leaf).byte_range();
    let chunk = Chunk {
        offset: start,
        bytes: Bytes::from(store.read_range(key, start, len)?),
    };
    let reader_meta =
        ArrowReaderMetadata::try_new(Arc::new(meta.clone()), ArrowReaderOptions::new())?;
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(chunk, reader_meta)
        .with_row_groups(vec![0])
        .with_projection(ProjectionMask::leaves(schema, [leaf]))
        .with_batch_size(HEAD_ROWS)
        .with_limit(HEAD_ROWS)
        .build()?;
    let Some(batch) = reader.into_iter().next() else {
        return Ok(Vec::new());
    };
    let column = arrow::compute::cast(batch?.column(0), &DataType::Utf8)
        .context("casting `__pos` to text")?;
    let text = column
        .as_any()
        .downcast_ref::<StringArray>()
        .context("`__pos` is not text")?;
    Ok(text.iter().flatten().map(str::to_string).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::RecordBatch;
    use arrow::datatypes::{Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    fn part(rows: &[Option<&str>], with_pos: bool) -> Vec<u8> {
        let ids = arrow::array::Int64Array::from_iter_values(0..rows.len() as i64);
        let mut fields = vec![Field::new("id", DataType::Int64, false)];
        let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(ids)];
        if with_pos {
            fields.push(Field::new("__pos", DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(rows.to_vec())));
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut out = Vec::new();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut w = ArrowWriter::try_new(&mut out, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        out
    }

    fn heads(file: &[u8], name: &str) -> Vec<String> {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(name), file).unwrap();
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let meta = super::super::partition_budget::read_footer(&store, name).unwrap();
        head_positions(&store, name, &meta).unwrap()
    }

    #[test]
    fn the_head_of_the_position_column_is_read_through_ranged_gets() {
        let file = part(&[Some("a"), None, Some("c"), Some("d"), Some("e")], true);
        assert_eq!(heads(&file, "p.parquet"), vec!["a"]);
        let file = part(&[Some("a"), Some("b"), Some("c")], true);
        assert_eq!(heads(&file, "q.parquet"), vec!["a", "b"]);
    }

    #[test]
    fn a_part_without_the_column_or_rows_has_no_head() {
        assert!(heads(&part(&[Some("a")], false), "n.parquet").is_empty());
        assert!(heads(&part(&[], true), "e.parquet").is_empty());
    }
}
