//! The NULL lists of a staged Parquet part, read from the list columns' chunks by ranged GETs.

use super::part_pos::{Chunk, Chunks};
use crate::destination::gcs::GcsStore;
use crate::source::cdc::ChangeOp;
use anyhow::{Context, Result};
use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use bytes::Bytes;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::basic::ConvertedType;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::Type;
use std::sync::Arc;

const BATCH_ROWS: usize = 8192;

/// The first of `columns` holding a NULL in a row of `batch` that is not a delete.
fn first_null<'c>(batch: &RecordBatch, columns: &'c [String]) -> Result<Option<&'c str>> {
    let ops = match batch.column_by_name("__op") {
        Some(op) => {
            Some(arrow::compute::cast(op, &DataType::Utf8).context("casting `__op` to text")?)
        }
        None => None,
    };
    let ops = ops
        .as_ref()
        .and_then(|o| o.as_any().downcast_ref::<StringArray>());
    let deletes = |row: usize| {
        ops.is_some_and(|o| o.is_valid(row) && o.value(row) == ChangeOp::Delete.as_str())
    };
    Ok(columns.iter().map(String::as_str).find(|name| {
        batch
            .column_by_name(name)
            .is_some_and(|c| (0..c.len()).any(|r| c.is_null(r) && !deletes(r)))
    }))
}

/// Whether the schema node is a list group.
fn is_list(t: &Type) -> bool {
    t.is_group() && t.get_basic_info().converted_type() == ConvertedType::LIST
}

/// The top-level columns of the part that are lists.
pub(crate) fn list_columns(meta: &ParquetMetaData) -> Vec<String> {
    let root = meta.file_metadata().schema_descr().root_schema();
    root.get_fields()
        .iter()
        .filter(|f| is_list(f))
        .map(|f| f.name().to_string())
        .collect()
}

/// The first of `columns` that the part at `key` (footer `meta`) holds a NULL in; for a change log, outside the rows of a delete.
pub(crate) fn first_null_list<'c>(
    store: &GcsStore,
    key: &str,
    meta: &ParquetMetaData,
    columns: &'c [String],
    change_log: bool,
) -> Result<Option<&'c str>> {
    let schema = meta.file_metadata().schema_descr();
    let root_of = |leaf: usize| schema.get_column_root(leaf).name();
    let listed = |leaf: usize| columns.iter().any(|c| c == root_of(leaf));
    let reader_meta =
        ArrowReaderMetadata::try_new(Arc::new(meta.clone()), ArrowReaderOptions::new())?;
    for group in 0..meta.num_row_groups() {
        let row_group = meta.row_group(group);
        let may_hold_null = |leaf: usize| {
            row_group
                .column(leaf)
                .statistics()
                .and_then(|s| s.null_count_opt())
                != Some(0)
        };
        let mut leaves: Vec<usize> = (0..schema.num_columns())
            .filter(|&leaf| listed(leaf) && may_hold_null(leaf))
            .collect();
        if leaves.is_empty() {
            continue;
        }
        leaves.extend((0..schema.num_columns()).filter(|&l| change_log && root_of(l) == "__op"));
        let mut chunks = Vec::with_capacity(leaves.len());
        for &leaf in &leaves {
            let (start, len) = row_group.column(leaf).byte_range();
            chunks.push(Chunk {
                offset: start,
                bytes: Bytes::from(store.read_range(key, start, len)?),
            });
        }
        let reader =
            ParquetRecordBatchReaderBuilder::new_with_metadata(Chunks(chunks), reader_meta.clone())
                .with_row_groups(vec![group])
                .with_projection(ProjectionMask::leaves(schema, leaves.iter().copied()))
                .with_batch_size(BATCH_ROWS)
                .build()?;
        for batch in reader {
            if let Some(name) = first_null(&batch?, columns)? {
                return Ok(Some(name));
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, ListArray};
    use arrow::datatypes::{Field, Int32Type, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{LogicalType, Repetition};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::reader::Length;

    fn lists(rows: &[Option<Vec<Option<i32>>>]) -> ListArray {
        ListArray::from_iter_primitive::<Int32Type, _, _>(rows.iter().cloned())
    }

    fn part(
        batch: RecordBatch,
        group_rows: usize,
    ) -> (tempfile::TempDir, GcsStore, ParquetMetaData) {
        part_with(batch, group_rows, EnabledStatistics::Page)
    }

    fn part_with(
        batch: RecordBatch,
        group_rows: usize,
        stats: EnabledStatistics,
    ) -> (tempfile::TempDir, GcsStore, ParquetMetaData) {
        let mut out = Vec::new();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(group_rows))
            .set_statistics_enabled(stats)
            .build();
        let mut w = ArrowWriter::try_new(&mut out, batch.schema(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("p.parquet"), out).unwrap();
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let meta = super::super::partition_budget::read_footer(&store, "p.parquet").unwrap();
        (dir, store, meta)
    }

    fn batch(tags: ListArray, ops: Option<Vec<Option<&str>>>) -> RecordBatch {
        let ids = Int32Array::from_iter_values(0..tags.len() as i32);
        let mut fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("tags", tags.data_type().clone(), true),
        ];
        let mut cols: Vec<ArrayRef> = vec![Arc::new(ids), Arc::new(tags)];
        if let Some(ops) = ops {
            fields.push(Field::new("__op", DataType::Utf8, true));
            cols.push(Arc::new(StringArray::from(ops)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    fn found_in(b: RecordBatch, group_rows: usize, change_log: bool) -> Option<String> {
        let (_dir, store, meta) = part(b, group_rows);
        let columns = list_columns(&meta);
        first_null_list(&store, "p.parquet", &meta, &columns, change_log)
            .unwrap()
            .map(str::to_string)
    }

    fn found(b: RecordBatch, group_rows: usize) -> Option<String> {
        found_in(b, group_rows, true)
    }

    #[test]
    fn a_null_list_is_found_and_an_empty_list_or_a_null_element_is_not() {
        let held = lists(&[Some(vec![Some(1)]), Some(vec![]), Some(vec![None, Some(2)])]);
        assert_eq!(found(batch(held, None), 2), None);
        let null = lists(&[Some(vec![Some(1)]), Some(vec![]), None]);
        assert_eq!(found(batch(null, None), 2).as_deref(), Some("tags"));
    }

    #[test]
    fn a_null_list_in_a_later_row_group_is_found() {
        let rows: Vec<_> = (0..9).map(|i| (i != 7).then(|| vec![Some(i)])).collect();
        assert_eq!(found(batch(lists(&rows), None), 4).as_deref(), Some("tags"));
    }

    #[test]
    fn a_null_list_of_a_delete_row_is_not_a_null_array_but_that_of_any_other_row_is() {
        let tags = lists(&[Some(vec![Some(1)]), None]);
        let ops = |op| Some(vec![Some("insert"), Some(op)]);
        assert_eq!(found(batch(tags.clone(), ops("delete")), 8), None);
        assert_eq!(
            found(batch(tags.clone(), ops("update")), 8).as_deref(),
            Some("tags")
        );
        let no_op = Some(vec![Some("insert"), None]);
        assert_eq!(
            found(batch(tags.clone(), no_op), 8).as_deref(),
            Some("tags")
        );
        assert_eq!(
            found_in(batch(tags, ops("delete")), 8, false).as_deref(),
            Some("tags"),
            "a part that is not a change log has no delete rows"
        );
    }

    #[test]
    fn a_null_list_is_found_without_column_statistics_and_a_clean_part_is_not_read() {
        let nulls = batch(lists(&[Some(vec![Some(1)]), None]), None);
        let (_dir, store, meta) = part_with(nulls, 8, EnabledStatistics::None);
        let columns = list_columns(&meta);
        let found = first_null_list(&store, "p.parquet", &meta, &columns, false).unwrap();
        assert_eq!(found, Some("tags"));
        let clean = batch(lists(&[Some(vec![Some(1)]), Some(vec![Some(2)])]), None);
        let (dir, store, meta) = part(clean, 8);
        std::fs::write(dir.path().join("p.parquet"), b"not a parquet body").unwrap();
        let columns = list_columns(&meta);
        assert_eq!(
            first_null_list(&store, "p.parquet", &meta, &columns, false).unwrap(),
            None,
            "a row group whose list leaf counts no nulls is not read"
        );
    }

    #[test]
    fn a_list_group_is_told_from_a_struct_group_and_a_leaf() {
        let leaf = || {
            Arc::new(
                Type::primitive_type_builder("e", parquet::basic::Type::INT32)
                    .build()
                    .unwrap(),
            )
        };
        let group = |name: &'static str| Type::group_type_builder(name).with_fields(vec![leaf()]);
        let logical = group("a")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .build()
            .unwrap();
        let converted = group("b")
            .with_repetition(Repetition::OPTIONAL)
            .with_converted_type(ConvertedType::LIST)
            .build()
            .unwrap();
        let plain = group("c").build().unwrap();
        assert!(is_list(&logical) && is_list(&converted));
        assert!(!is_list(&plain) && !is_list(&leaf()));
    }

    #[test]
    fn the_chunks_read_are_as_long_as_the_furthest_byte() {
        let chunk = |offset, n| Chunk {
            offset,
            bytes: Bytes::from(vec![0u8; n]),
        };
        assert_eq!(Chunks(vec![chunk(10, 5), chunk(0, 4)]).len(), 15);
        assert_eq!(Chunks(Vec::new()).len(), 0);
    }

    #[test]
    fn only_the_list_columns_are_read() {
        let (_dir, store, meta) = part(batch(lists(&[None]), None), 8);
        assert_eq!(list_columns(&meta), vec!["tags".to_string()]);
        let other = vec!["nope".to_string()];
        assert_eq!(
            first_null_list(&store, "p.parquet", &meta, &other, false).unwrap(),
            None
        );
        assert_eq!(
            first_null_list(&store, "p.parquet", &meta, &[], false).unwrap(),
            None
        );
    }
}
