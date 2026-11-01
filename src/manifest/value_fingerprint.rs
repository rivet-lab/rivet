//! Per-part value fingerprint: a digest of the rows' values in row order, independent of the Parquet encoding.

use crate::enrich::COL_EXPORTED_AT;
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use xxhash_rust::xxh3::Xxh3;

/// Prefix of every value fingerprint string; the digest is xxh3-64 in lowercase hex.
pub const VALUE_FINGERPRINT_PREFIX: &str = "xxh3v1:";

const MAGIC: &[u8] = b"rivet-value-fingerprint-v1";

const _: () = assert!(
    cfg!(target_endian = "little"),
    "value fingerprint assumes little-endian"
);

/// Streaming accumulator: one xxh3 state per column, fed batch by batch in row order.
pub struct ValueFingerprint {
    schema: Schema,
    header: Vec<u8>,
    kept: Vec<usize>,
    columns: Vec<Xxh3>,
    rows: u64,
}

impl ValueFingerprint {
    /// An accumulator for `schema` without `_rivet_exported_at`, or `None` outside the canonical set.
    pub fn new(schema: &Schema) -> Option<Self> {
        let kept: Vec<usize> = (0..schema.fields().len())
            .filter(|&i| schema.field(i).name() != COL_EXPORTED_AT)
            .collect();
        let mut header = MAGIC.to_vec();
        put_u32(&mut header, kept.len() as u32);
        for &i in &kept {
            let field = schema.field(i);
            put_str(&mut header, field.name());
            canon_type(field.data_type(), &mut header)?;
        }
        Some(Self {
            schema: schema.clone(),
            header,
            columns: kept.iter().map(|_| Xxh3::new()).collect(),
            kept,
            rows: 0,
        })
    }

    /// Feed the batch's rows after the rows already fed; `None` if its schema is not this accumulator's.
    pub fn update(&mut self, batch: &RecordBatch) -> Option<()> {
        if batch.schema_ref().as_ref() != &self.schema {
            return None;
        }
        for (state, &i) in self.columns.iter_mut().zip(&self.kept) {
            feed(state, batch.column(i).as_ref())?;
        }
        self.rows += batch.num_rows() as u64;
        Some(())
    }

    /// The `xxh3v1:` string over the schema, each kept column's value stream, and the row count.
    pub fn finish(self) -> String {
        let mut h = Xxh3::new();
        h.update(&self.header);
        h.update(&self.rows.to_le_bytes());
        for column in &self.columns {
            h.update(&column.digest().to_le_bytes());
        }
        format!("{VALUE_FINGERPRINT_PREFIX}{:016x}", h.digest())
    }
}

/// Feed every row of `arr`, in order, into `h`: a presence tag, then the value's canonical bytes.
fn feed(h: &mut Xxh3, arr: &dyn Array) -> Option<()> {
    let len = arr.len();
    match arr.data_type() {
        DataType::Null => {
            for _ in 0..len {
                h.update(&[0]);
            }
        }
        DataType::Boolean => {
            let a = arr.as_boolean();
            for i in 0..len {
                if a.is_valid(i) {
                    h.update(&[1, a.value(i) as u8]);
                } else {
                    h.update(&[0]);
                }
            }
        }
        DataType::Utf8 => var(
            h,
            arr.as_string::<i32>().iter().map(|v| v.map(str::as_bytes)),
        ),
        DataType::LargeUtf8 => var(
            h,
            arr.as_string::<i64>().iter().map(|v| v.map(str::as_bytes)),
        ),
        DataType::Binary => var(h, arr.as_binary::<i32>().iter()),
        DataType::LargeBinary => var(h, arr.as_binary::<i64>().iter()),
        DataType::List(_) => {
            let a = arr.as_list::<i32>();
            for i in 0..len {
                if a.is_valid(i) {
                    let elems = a.value(i);
                    h.update(&[1]);
                    h.update(&(elems.len() as u64).to_le_bytes());
                    feed(h, elems.as_ref())?;
                } else {
                    h.update(&[0]);
                }
            }
        }
        DataType::LargeList(_) => {
            let a = arr.as_list::<i64>();
            for i in 0..len {
                if a.is_valid(i) {
                    let elems = a.value(i);
                    h.update(&[1]);
                    h.update(&(elems.len() as u64).to_le_bytes());
                    feed(h, elems.as_ref())?;
                } else {
                    h.update(&[0]);
                }
            }
        }
        DataType::FixedSizeList(_, _) => {
            let a = arr.as_fixed_size_list();
            for i in 0..len {
                if a.is_valid(i) {
                    h.update(&[1]);
                    feed(h, a.value(i).as_ref())?;
                } else {
                    h.update(&[0]);
                }
            }
        }
        dt => {
            let width = fixed_width(dt)?;
            fixed(h, arr, width);
        }
    }
    Some(())
}

/// Feed variable-length cells: a tag, then the u64 length and the bytes.
fn var<'a>(h: &mut Xxh3, items: impl Iterator<Item = Option<&'a [u8]>>) {
    for item in items {
        match item {
            Some(bytes) => {
                h.update(&[1]);
                h.update(&(bytes.len() as u64).to_le_bytes());
                h.update(bytes);
            }
            None => h.update(&[0]),
        }
    }
}

/// Feed fixed-width cells from the raw values buffer, skipping the bytes under nulls.
fn fixed(h: &mut Xxh3, arr: &dyn Array, width: usize) {
    let data = arr.to_data();
    let values = data.buffers()[0].as_slice();
    let start = data.offset() * width;
    let values = &values[start..start + arr.len() * width];
    for i in 0..arr.len() {
        if arr.is_valid(i) {
            h.update(&[1]);
            h.update(&values[i * width..(i + 1) * width]);
        } else {
            h.update(&[0]);
        }
    }
}

/// Byte width of a fixed-width value type, `None` for every other type.
fn fixed_width(dt: &DataType) -> Option<usize> {
    Some(match dt {
        DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => 2,
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Decimal32(_, _) => 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Decimal64(_, _) => 8,
        DataType::Decimal128(_, _) => 16,
        DataType::Decimal256(_, _) => 32,
        DataType::FixedSizeBinary(n) => usize::try_from(*n).ok()?,
        _ => return None,
    })
}

/// Canonical type bytes: a fixed code per type plus its parameters.
fn canon_type(dt: &DataType, out: &mut Vec<u8>) -> Option<()> {
    match dt {
        DataType::Null => out.push(0),
        DataType::Boolean => out.push(1),
        DataType::Int8 => out.push(2),
        DataType::Int16 => out.push(3),
        DataType::Int32 => out.push(4),
        DataType::Int64 => out.push(5),
        DataType::UInt8 => out.push(6),
        DataType::UInt16 => out.push(7),
        DataType::UInt32 => out.push(8),
        DataType::UInt64 => out.push(9),
        DataType::Float16 => out.push(10),
        DataType::Float32 => out.push(11),
        DataType::Float64 => out.push(12),
        DataType::Decimal32(p, s) => out.extend([13, *p, *s as u8]),
        DataType::Decimal64(p, s) => out.extend([14, *p, *s as u8]),
        DataType::Decimal128(p, s) => out.extend([15, *p, *s as u8]),
        DataType::Decimal256(p, s) => out.extend([16, *p, *s as u8]),
        DataType::Date32 => out.push(17),
        DataType::Date64 => out.push(18),
        DataType::Time32(u) => {
            out.push(19);
            canon_unit(*u, out);
        }
        DataType::Time64(u) => {
            out.push(20);
            canon_unit(*u, out);
        }
        DataType::Timestamp(u, tz) => {
            out.push(21);
            canon_unit(*u, out);
            match tz {
                None => out.push(0),
                Some(zone) => {
                    out.push(1);
                    put_str(out, zone);
                }
            }
        }
        DataType::Duration(u) => {
            out.push(22);
            canon_unit(*u, out);
        }
        DataType::Binary => out.push(23),
        DataType::LargeBinary => out.push(24),
        DataType::FixedSizeBinary(n) => {
            out.push(25);
            put_u32(out, u32::try_from(*n).ok()?);
        }
        DataType::Utf8 => out.push(26),
        DataType::LargeUtf8 => out.push(27),
        DataType::List(item) => {
            out.push(28);
            canon_type(item.data_type(), out)?;
        }
        DataType::LargeList(item) => {
            out.push(29);
            canon_type(item.data_type(), out)?;
        }
        DataType::FixedSizeList(item, n) => {
            out.push(30);
            put_u32(out, u32::try_from(*n).ok()?);
            canon_type(item.data_type(), out)?;
        }
        _ => return None,
    }
    Some(())
}

fn canon_unit(unit: TimeUnit, out: &mut Vec<u8>) {
    out.push(match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    });
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend(v.to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u32(out, s.len() as u32);
    out.extend(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float64Array, Int32Array, Int64Array, ListArray, StringArray, Time64MicrosecondArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{Field, Fields};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    const GOLDEN_FIXTURE: &str = "xxh3v1:f2de1ee7ec2d52ab";

    fn fp(batches: &[RecordBatch]) -> String {
        let mut acc = ValueFingerprint::new(&batches[0].schema()).expect("canonical schema");
        for b in batches {
            acc.update(b).expect("same schema");
        }
        acc.finish()
    }

    fn one_col(name: &str, dt: DataType, arr: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, dt, true)]));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    fn fixture(offset: i64, n: usize) -> RecordBatch {
        let ids: Vec<i64> = (0..n as i64).map(|i| i + offset).collect();
        let names: Vec<Option<String>> = ids
            .iter()
            .map(|i| (i % 4 != 0).then(|| format!("row {i}")))
            .collect();
        let amounts: Vec<Option<i128>> = ids.iter().map(|i| Some(*i as i128 * 125)).collect();
        let flags: Vec<Option<bool>> = ids.iter().map(|i| Some(i % 2 == 0)).collect();
        let floats: Vec<Option<f64>> = ids.iter().map(|i| Some(*i as f64 / 3.0)).collect();
        let bins: Vec<Option<&[u8]>> = ids.iter().map(|_| Some(&[1u8, 2, 3][..])).collect();
        let fixed: Vec<Option<Vec<u8>>> = ids
            .iter()
            .map(|i| Some(vec![(*i % 256) as u8, 7]))
            .collect();
        let days: Vec<Option<i32>> = ids.iter().map(|i| Some(*i as i32)).collect();
        let micros: Vec<Option<i64>> = ids.iter().map(|i| Some(i * 1_000_000)).collect();
        let times: Vec<Option<i64>> = ids.iter().map(|i| Some(i * 7)).collect();
        let tags: Vec<Option<Vec<Option<&str>>>> = ids
            .iter()
            .map(|i| Some(vec![Some("a"), (i % 3 != 0).then_some("b")]))
            .collect();
        let nums: Vec<Option<Vec<Option<i32>>>> = ids
            .iter()
            .map(|i| Some(vec![Some(*i as i32), None]))
            .collect();
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let num_item = Arc::new(Field::new("item", DataType::Int32, true));
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(12, 2), true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("f", DataType::Float64, true),
            Field::new("b", DataType::Binary, true),
            Field::new("fixed", DataType::FixedSizeBinary(2), true),
            Field::new("day", DataType::Date32, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("t", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("tags", DataType::List(item.clone()), true),
            Field::new("nums", DataType::List(num_item.clone()), true),
            Field::new("nothing", DataType::Null, true),
        ];
        let schema = Arc::new(Schema::new(fields));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(names)),
            Arc::new(
                Decimal128Array::from(amounts)
                    .with_precision_and_scale(12, 2)
                    .unwrap(),
            ),
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Float64Array::from(floats)),
            Arc::new(BinaryArray::from(bins)),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(fixed.into_iter(), 2).unwrap(),
            ),
            Arc::new(Date32Array::from(days)),
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone("UTC")),
            Arc::new(Time64MicrosecondArray::from(times)),
            Arc::new(list_utf8(tags, item)),
            Arc::new(list_i32(nums, num_item)),
            Arc::new(arrow::array::NullArray::new(n)),
        ];
        RecordBatch::try_new(schema, cols).unwrap()
    }

    fn list_utf8(rows: Vec<Option<Vec<Option<&str>>>>, item: Arc<Field>) -> ListArray {
        let flat: Vec<Option<&str>> = rows.iter().flatten().flatten().copied().collect();
        let values = StringArray::from(flat);
        let mut offsets = vec![0i32];
        for r in &rows {
            offsets.push(offsets.last().unwrap() + r.as_ref().map_or(0, |v| v.len()) as i32);
        }
        let nulls =
            arrow::buffer::NullBuffer::from(rows.iter().map(Option::is_some).collect::<Vec<_>>());
        ListArray::new(
            item,
            arrow::buffer::OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            Some(nulls),
        )
    }

    fn list_i32(rows: Vec<Option<Vec<Option<i32>>>>, item: Arc<Field>) -> ListArray {
        let flat: Vec<Option<i32>> = rows.iter().flatten().flatten().copied().collect();
        let values = Int32Array::from(flat);
        let mut offsets = vec![0i32];
        for r in &rows {
            offsets.push(offsets.last().unwrap() + r.as_ref().map_or(0, |v| v.len()) as i32);
        }
        let nulls =
            arrow::buffer::NullBuffer::from(rows.iter().map(Option::is_some).collect::<Vec<_>>());
        ListArray::new(
            item,
            arrow::buffer::OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            Some(nulls),
        )
    }

    #[test]
    fn golden_fixture_is_pinned() {
        assert_eq!(fp(&[fixture(0, 9)]), GOLDEN_FIXTURE);
    }

    #[test]
    fn canonical_set_accepts_written_types_and_refuses_the_rest() {
        let ok = [
            DataType::Int8,
            DataType::UInt16,
            DataType::Float16,
            DataType::Float32,
            DataType::Decimal32(9, 2),
            DataType::Decimal256(50, 4),
            DataType::Date64,
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::LargeUtf8,
            DataType::LargeBinary,
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), 2),
        ];
        for dt in ok {
            assert!(
                ValueFingerprint::new(&Schema::new(vec![Field::new("c", dt.clone(), true)]))
                    .is_some(),
                "{dt:?}"
            );
        }
        let refused = [
            DataType::Utf8View,
            DataType::Interval(arrow::datatypes::IntervalUnit::DayTime),
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int64, true)])),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ];
        for dt in refused {
            assert!(
                ValueFingerprint::new(&Schema::new(vec![Field::new("c", dt.clone(), true)]))
                    .is_none(),
                "{dt:?}"
            );
        }
    }

    #[test]
    fn null_is_distinct_from_a_value() {
        let a = one_col(
            "c",
            DataType::Int64,
            Arc::new(Int64Array::from(vec![Some(1), None])),
        );
        let b = one_col(
            "c",
            DataType::Int64,
            Arc::new(Int64Array::from(vec![Some(1), Some(0)])),
        );
        assert_ne!(fp(&[a]), fp(&[b]));
    }

    #[test]
    fn row_order_matters() {
        let a = one_col("c", DataType::Int64, Arc::new(Int64Array::from(vec![1, 2])));
        let b = one_col("c", DataType::Int64, Arc::new(Int64Array::from(vec![2, 1])));
        assert_ne!(fp(&[a]), fp(&[b]));
    }

    #[test]
    fn floats_compare_by_bit_pattern() {
        let pos = one_col(
            "c",
            DataType::Float64,
            Arc::new(Float64Array::from(vec![0.0])),
        );
        let neg = one_col(
            "c",
            DataType::Float64,
            Arc::new(Float64Array::from(vec![-0.0])),
        );
        assert_ne!(fp(&[pos]), fp(&[neg]));
    }

    #[test]
    fn row_count_is_part_of_the_value() {
        let two = one_col(
            "c",
            DataType::Int64,
            Arc::new(Int64Array::from(vec![None::<i64>, None])),
        );
        let three = one_col(
            "c",
            DataType::Int64,
            Arc::new(Int64Array::from(vec![None::<i64>, None, None])),
        );
        assert_ne!(fp(&[two]), fp(&[three]));
    }

    #[test]
    fn batch_split_does_not_change_the_value() {
        let whole = fixture(0, 9);
        let halves = [fixture(0, 4), fixture(4, 5)];
        assert_eq!(fp(&[whole]), fp(&halves));
    }

    #[test]
    fn exported_at_column_is_excluded() {
        let base = one_col("c", DataType::Int64, Arc::new(Int64Array::from(vec![1, 2])));
        let stamped = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("c", DataType::Int64, true),
                Field::new(COL_EXPORTED_AT, DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![111, 222])),
            ],
        )
        .unwrap();
        let other_stamp = RecordBatch::try_new(
            stamped.schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![333, 444])),
            ],
        )
        .unwrap();
        assert_eq!(fp(&[base]), fp(std::slice::from_ref(&stamped)));
        assert_eq!(fp(&[stamped]), fp(&[other_stamp]));
    }

    #[test]
    fn a_renamed_column_changes_the_value() {
        let a = one_col("c", DataType::Int64, Arc::new(Int64Array::from(vec![1])));
        let b = one_col("d", DataType::Int64, Arc::new(Int64Array::from(vec![1])));
        assert_ne!(fp(&[a]), fp(&[b]));
    }

    #[test]
    fn update_refuses_a_batch_of_another_schema() {
        let mut acc = ValueFingerprint::new(&fixture(0, 1).schema()).unwrap();
        let other = one_col("c", DataType::Int64, Arc::new(Int64Array::from(vec![1])));
        assert!(acc.update(&other).is_none());
    }

    #[test]
    fn two_encodings_share_the_value_and_differ_in_bytes() {
        let batches = [fixture(0, 5000)];
        let value = fp(&batches);
        let mut digests = Vec::new();
        for (tag, props) in [
            (
                "plain",
                WriterProperties::builder()
                    .set_compression(Compression::UNCOMPRESSED)
                    .set_max_row_group_row_count(Some(1000))
                    .build(),
            ),
            (
                "snappy",
                WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .set_dictionary_enabled(false)
                    .build(),
            ),
        ] {
            let path =
                std::env::temp_dir().join(format!("vfp-{}-{tag}.parquet", std::process::id()));
            let file = std::fs::File::create(&path).unwrap();
            let mut w = ArrowWriter::try_new(file, batches[0].schema(), Some(props)).unwrap();
            for b in &batches {
                w.write(b).unwrap();
            }
            w.close().unwrap();
            let bytes = std::fs::read(&path).unwrap();
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap())
                    .unwrap()
                    .build()
                    .unwrap();
            let read_back: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
            assert_eq!(
                fp(&read_back),
                value,
                "{tag}: value fingerprint must survive re-encoding"
            );
            digests.push(xxhash_rust::xxh3::xxh3_64(&bytes));
            std::fs::remove_file(&path).ok();
        }
        assert_ne!(
            digests[0], digests[1],
            "the two encodings must differ in bytes"
        );
    }
}
