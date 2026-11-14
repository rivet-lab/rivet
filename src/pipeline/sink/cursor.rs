//! Cursor value extraction from Arrow batches.

use arrow::array::Array;
use arrow::array::{
    Date32Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampMicrosecondArray, TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

/// Return the last row's value of `cursor_column` as a normalized string.
pub(crate) fn extract_last_cursor_value(
    batch: &RecordBatch,
    cursor_column: &str,
    schema: &SchemaRef,
) -> Option<String> {
    extract_cursor_value_at(batch, cursor_column, schema, true)
}

/// First NON-NULL cursor value of the batch.
pub(crate) fn extract_first_cursor_value(
    batch: &RecordBatch,
    cursor_column: &str,
    schema: &SchemaRef,
) -> Option<String> {
    extract_cursor_value_at(batch, cursor_column, schema, false)
}

fn extract_cursor_value_at(
    batch: &RecordBatch,
    cursor_column: &str,
    schema: &SchemaRef,
    from_end: bool,
) -> Option<String> {
    let col_idx = schema.index_of(cursor_column).ok()?;
    let array = batch.column(col_idx);

    let last_row = if from_end {
        (0..batch.num_rows()).rev().find(|&i| !array.is_null(i))?
    } else {
        (0..batch.num_rows()).find(|&i| !array.is_null(i))?
    };
    render_cursor_value(array, last_row)
}

/// The last (or first) row's composite key as a JSON array of its column values; `None` when any is unreadable.
pub(crate) fn extract_key_tuple(
    batch: &RecordBatch,
    columns: &[&str],
    schema: &SchemaRef,
    from_end: bool,
) -> Option<String> {
    let row = if from_end {
        batch.num_rows().checked_sub(1)?
    } else {
        (batch.num_rows() > 0).then_some(0)?
    };
    let values = columns
        .iter()
        .map(|c| {
            let array = batch.column(schema.index_of(c).ok()?);
            (!array.is_null(row)).then(|| render_cursor_value(array, row))?
        })
        .collect::<Option<Vec<String>>>()?;
    serde_json::to_string(&values).ok()
}

/// The value at `last_row` of a cursor-typed array as a normalized string; `None` for an unsupported type.
fn render_cursor_value(array: &arrow::array::ArrayRef, last_row: usize) -> Option<String> {
    match array.data_type() {
        DataType::Int16 => Some(
            array
                .as_any()
                .downcast_ref::<Int16Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Int32 => Some(
            array
                .as_any()
                .downcast_ref::<Int32Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Int64 => Some(
            array
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::UInt64 => Some(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Float32 => Some(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(|a| f64::from(a.value(last_row)))?
                .to_string(),
        ),
        DataType::Float64 => Some(
            array
                .as_any()
                .downcast_ref::<Float64Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Decimal128(_, _) => Some(
            array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()?
                .value_as_string(last_row),
        ),
        DataType::Utf8 => Some(
            array
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()?
                .value(last_row);
            let secs = micros.div_euclid(1_000_000);
            let nsecs = (micros.rem_euclid(1_000_000) * 1_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)?;
            Some(dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()?
                .value(last_row);
            let secs = nanos.div_euclid(1_000_000_000);
            let nsecs = nanos.rem_euclid(1_000_000_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)?;
            Some(dt.format("%Y-%m-%dT%H:%M:%S%.9f").to_string())
        }
        DataType::Date32 => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()?
                .value(last_row);
            let date =
                chrono::NaiveDate::from_ymd_opt(1970, 1, 1)? + chrono::Duration::days(days as i64);
            Some(date.to_string())
        }
        DataType::FixedSizeBinary(16) => {
            let bytes = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()?
                .value(last_row);
            if bytes.len() != 16 {
                log::warn!(
                    "cursor extract: FixedSizeBinary slice was {} bytes, expected 16",
                    bytes.len()
                );
                return None;
            }
            Some(format!(
                "{:02x}{:02x}{:02x}{:02x}-\
                 {:02x}{:02x}-\
                 {:02x}{:02x}-\
                 {:02x}{:02x}-\
                 {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                bytes[0],
                bytes[1],
                bytes[2],
                bytes[3],
                bytes[4],
                bytes[5],
                bytes[6],
                bytes[7],
                bytes[8],
                bytes[9],
                bytes[10],
                bytes[11],
                bytes[12],
                bytes[13],
                bytes[14],
                bytes[15],
            ))
        }
        _ => {
            log::warn!(
                "incremental cursor: column type {:?} cannot be rendered as a cursor value, so the cursor was not advanced",
                array.data_type()
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    use crate::plan::cursor_admission::cursor_renders;
    use crate::source::keyset_key::samples;

    /// `cursor_renders` holds for a delivered type exactly when the renderer reads a value of it.
    #[test]
    fn cursor_renders_is_the_set_the_renderer_reads() {
        for rivet_type in samples::rivet_type_samples() {
            let Some(data_type) = samples::mapping_of(rivet_type.clone()).arrow_type else {
                continue;
            };
            let array = new_null_array(&data_type, 1);
            assert_eq!(
                cursor_renders(&data_type),
                render_cursor_value(&array, 0).is_some(),
                "{rivet_type:?} lands as {data_type:?}"
            );
        }
    }

    /// A composite key cursor is the JSON tuple of the last (or first) row, and absent on a null.
    #[test]
    fn key_tuple_is_the_json_tuple_of_one_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("kind", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a\"b"), Some("z")])),
            ],
        )
        .unwrap();
        assert_eq!(
            extract_key_tuple(&batch, &["id", "kind"], &schema, true),
            Some(r#"["2","z"]"#.into())
        );
        assert_eq!(
            extract_key_tuple(&batch, &["id", "kind"], &schema, false),
            Some(r#"["1","a\"b"]"#.into())
        );
        let nulls = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();
        assert_eq!(
            extract_key_tuple(&nulls, &["id", "kind"], &schema, true),
            None
        );
        assert_eq!(
            extract_key_tuple(&batch.slice(0, 0), &["id", "kind"], &schema, true),
            None
        );
        assert_eq!(
            extract_key_tuple(&batch.slice(0, 0), &["id", "kind"], &schema, false),
            None
        );
    }

    #[test]
    fn cursor_int64() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("30".into())
        );
    }

    #[test]
    fn cursor_decimal_past_i64_is_exact_text() {
        let dt = DataType::Decimal128(38, 0);
        let schema = Arc::new(Schema::new(vec![Field::new("id", dt.clone(), false)]));
        let big: i128 = 12_345_678_901_234_567_890_123;
        let arr = Decimal128Array::from(vec![1, big]).with_data_type(dt);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("12345678901234567890123".into())
        );
    }

    #[test]
    fn cursor_float32_renders_the_exact_widened_value() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Float32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float32Array::from(vec![0.05f32, 0.1f32]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "k", &schema),
            Some("0.10000000149011612".into()),
            "MySQL compares FLOAT with a string as DOUBLE; the bound must equal the widened row"
        );
    }

    #[test]
    fn cursor_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![100, 200]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("200".into())
        );
    }

    #[test]
    fn cursor_int16() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int16, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int16Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("3".into())
        );
    }

    #[test]
    fn cursor_unsigned_integers_field_bug() {
        let big = u64::MAX - 5;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![1, big]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some(big.to_string())
        );
    }

    #[test]
    fn cursor_float64() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![1.5, 2.7]))],
        )
        .unwrap();
        let val = extract_last_cursor_value(&batch, "score", &schema).unwrap();
        assert!(val.starts_with("2.7"), "got: {val}");
    }

    #[test]
    fn cursor_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["aaa", "zzz"]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "key", &schema),
            Some("zzz".into())
        );
    }

    #[test]
    fn cursor_timestamp_microsecond() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        let micros = 1_700_000_000_000_000i64;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![micros]))],
        )
        .unwrap();
        let val = extract_last_cursor_value(&batch, "ts", &schema).unwrap();
        assert!(
            val.starts_with("2023-11-14T22:13:20"),
            "unexpected ts: {val}"
        );
    }

    #[test]
    fn cursor_timestamp_nanosecond_keeps_full_precision() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let nanos = 1_700_000_000_123_456_700i64;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampNanosecondArray::from(vec![nanos]))],
        )
        .unwrap();
        let val = extract_last_cursor_value(&batch, "ts", &schema).unwrap();
        assert_eq!(
            val, "2023-11-14T22:13:20.123456700",
            "ns precision lost: {val}"
        );
    }

    #[test]
    fn cursor_date32() {
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)]));
        let days = 19723i32;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Date32Array::from(vec![days]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "d", &schema),
            Some("2024-01-01".into())
        );
    }

    #[test]
    fn cursor_fixed_size_binary_16_decodes_to_canonical_uuid_string() {
        use arrow::array::FixedSizeBinaryArray;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let bytes: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x0a];
        let arr = FixedSizeBinaryArray::try_from_iter(std::iter::once(bytes.to_vec())).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("00000000-0000-0000-0000-00000000000a".into())
        );
    }

    #[test]
    fn cursor_fixed_size_binary_wrong_length_returns_none() {
        use arrow::array::FixedSizeBinaryArray;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::FixedSizeBinary(8),
            false,
        )]));
        let arr =
            FixedSizeBinaryArray::try_from_iter(std::iter::once(vec![1u8, 2, 3, 4, 5, 6, 7, 8]))
                .unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "id", &schema), None);
    }

    /// A NULL in the last row must not hide the value above it.
    #[test]
    fn cursor_null_last_row_uses_the_last_non_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("1".to_string())
        );
    }

    /// Several trailing NULLs are the realistic shape.
    #[test]
    fn cursor_many_trailing_nulls_still_find_the_maximum() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(7),
                None,
                None,
                None,
            ]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("7".to_string())
        );
    }

    /// The control: a batch with NOTHING readable genuinely says nothing, and must still report None so the caller keeps the mark it already has rather than inventing one.
    #[test]
    fn cursor_all_null_batch_reports_nothing() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![None, None]))],
        )
        .unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "id", &schema), None);
    }

    #[test]
    fn cursor_missing_column_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "nonexistent", &schema),
            None
        );
    }

    #[test]
    fn cursor_empty_batch_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "id", &schema), None);
    }

    #[test]
    fn cursor_unsupported_type_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "bin",
            DataType::Binary,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BinaryArray::from(vec![b"hello".as_slice()]))],
        )
        .unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "bin", &schema), None);
    }

    use crate::source::keyset_key::samples::{mapping_of, rivet_type_samples};

    /// Every type the keyset seam admits has a cursor arm; the renderable types it still refuses are named.
    #[test]
    fn a_key_the_seam_admits_is_a_key_the_cursor_renders() {
        use crate::config::SourceType;
        use crate::source::keyset_key::{KeyShape, keyset_key_refusal};
        use crate::types::RivetType;
        let renderable_but_refused = |source: SourceType, t: &RivetType| match t {
            RivetType::Json => source != SourceType::Postgres,
            RivetType::Interval => true,
            RivetType::Decimal { scale, .. } => source != SourceType::Oracle || *scale != 0,
            RivetType::Float32 | RivetType::Float64 => source == SourceType::Oracle,
            RivetType::Timestamp {
                timezone: Some(_), ..
            } => source == SourceType::Oracle,
            RivetType::Enum => source == SourceType::Mysql,
            _ => false,
        };
        for source in [
            SourceType::Postgres,
            SourceType::Mysql,
            SourceType::Mssql,
            SourceType::Oracle,
        ] {
            for t in rivet_type_samples() {
                let m = mapping_of(t.clone());
                let rendered = m.arrow_type.as_ref().and_then(|dt| {
                    let array = arrow::array::new_null_array(dt, 1);
                    render_cursor_value(&array, 0)
                });
                match keyset_key_refusal(source, &m, None, KeyShape::Single) {
                    None => assert!(
                        rendered.is_some(),
                        "{source:?} admits {t:?} as a keyset key but the cursor cannot render it"
                    ),
                    Some(cause) => assert!(
                        rendered.is_none() || renderable_but_refused(source, &t),
                        "{source:?} refuses {t:?} ({cause}) though the cursor renders it and no reason is recorded"
                    ),
                }
            }
        }
    }
}
