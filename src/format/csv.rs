use std::io::{BufRead, Write};
use std::path::Path;

use arrow::array::types::{Decimal128Type, Time64MicrosecondType, Time64NanosecondType};
use arrow::array::*;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::types::decimal::scaled_i128_to_decimal_str;

pub struct CsvFormat;

pub struct CsvFormatWriter {
    writer: Box<dyn Write + Send>,
    bytes_written: u64,
}

impl super::Format for CsvFormat {
    fn create_writer(
        &self,
        schema: &SchemaRef,
        mut writer: Box<dyn Write + Send>,
    ) -> Result<Box<dyn super::FormatWriter + Send>> {
        for field in schema.fields() {
            refuse_column(field.name(), field.data_type())?;
        }
        let header = schema
            .fields()
            .iter()
            .map(|f| {
                let n = f.name();
                if n.bytes().any(|b| matches!(b, b',' | b'"' | b'\n' | b'\r')) {
                    format!("\"{}\"", n.replace('"', "\"\""))
                } else {
                    n.clone()
                }
            })
            .collect::<Vec<String>>()
            .join(",");
        let header_bytes = header.len() as u64 + 1;
        writeln!(writer, "{}", header)?;
        Ok(Box::new(CsvFormatWriter {
            writer,
            bytes_written: header_bytes,
        }))
    }

    fn file_extension(&self) -> &str {
        "csv"
    }
}

impl super::FormatWriter for CsvFormatWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut buf = Vec::with_capacity(batch.num_rows() * batch.num_columns() * 8);
        for row_idx in 0..batch.num_rows() {
            for col_idx in 0..batch.num_columns() {
                if col_idx > 0 {
                    buf.push(b',');
                }
                write_csv_value(&mut buf, batch.column(col_idx), row_idx)?;
            }
            buf.push(b'\n');
        }
        self.bytes_written += buf.len() as u64;
        self.writer.write_all(&buf)?;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

/// The coded refusal for a column CSV cannot serialize.
pub(crate) fn refuse_column(name: &str, dt: &DataType) -> Result<()> {
    if csv_serializable(dt) {
        return Ok(());
    }
    crate::rivet_bail!(
        crate::error::codes::CONFIG_FORMAT_COLUMN_UNSUPPORTED,
        "CSV cannot serialize column '{name}' (Arrow type {dt:?}); use `format: parquet` \
         or drop the column from the query"
    )
}

/// Arrow types `write_csv_value` can serialize.
pub(crate) fn csv_serializable(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Decimal128(_, _)
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Binary
            | DataType::FixedSizeBinary(16)
            | DataType::Date32
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Time64(TimeUnit::Nanosecond)
            | DataType::Timestamp(TimeUnit::Microsecond, _)
    )
}

/// Lowercase hex-encode `bytes` into `writer`.
fn write_lower_hex(writer: &mut dyn Write, bytes: &[u8]) -> Result<()> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut chunk = [0u8; 1024];
    for slab in bytes.chunks(chunk.len() / 2) {
        let mut n = 0;
        for &b in slab {
            chunk[n] = HEX[(b >> 4) as usize];
            chunk[n + 1] = HEX[(b & 0x0f) as usize];
            n += 2;
        }
        writer.write_all(&chunk[..n])?;
    }
    Ok(())
}

/// A `Time64` cell as `[-]hh:mm:ss.f`, six fraction digits for microseconds and nine for nanoseconds.
fn write_time64(
    writer: &mut dyn Write,
    array: &dyn Array,
    idx: usize,
    unit: &TimeUnit,
) -> Result<()> {
    let (value, per_second, digits) = match unit {
        TimeUnit::Nanosecond => (
            array.as_primitive::<Time64NanosecondType>().value(idx),
            1_000_000_000_u64,
            9,
        ),
        _ => (
            array.as_primitive::<Time64MicrosecondType>().value(idx),
            1_000_000_u64,
            6,
        ),
    };
    let abs = value.unsigned_abs();
    let secs = abs / per_second;
    write!(
        writer,
        "{}{:02}:{:02}:{:02}.{:0digits$}",
        if value < 0 { "-" } else { "" },
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60,
        abs % per_second
    )?;
    Ok(())
}

/// The bytes of an empty string or an empty binary cell: a quoted empty field, never the bare field that spells NULL.
fn empty_value() -> &'static [u8] {
    seeded!("csv-empty-value-written-bare", &b""[..], &b"\"\""[..])
}

fn write_csv_value(writer: &mut dyn Write, array: &dyn Array, idx: usize) -> Result<()> {
    if array.is_null(idx) {
        return Ok(());
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Decimal128(_, scale) => {
            let arr = array.as_primitive::<Decimal128Type>();
            let text = scaled_i128_to_decimal_str(arr.value(idx), *scale);
            writer.write_all(text.as_bytes())?;
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("DataType/Array mismatch");
            let val = arr.value(idx);
            if val.is_empty() {
                writer.write_all(empty_value())?;
                return Ok(());
            }
            if val
                .bytes()
                .any(|b| matches!(b, b',' | b'"' | b'\n' | b'\r'))
            {
                writer.write_all(b"\"")?;
                let mut rest = val;
                while let Some(pos) = rest.find('"') {
                    writer.write_all(&rest.as_bytes()[..pos])?;
                    writer.write_all(b"\"\"")?;
                    rest = &rest[pos + 1..];
                }
                writer.write_all(rest.as_bytes())?;
                writer.write_all(b"\"")?;
            } else {
                writer.write_all(val.as_bytes())?;
            }
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("DataType/Array mismatch");
            let val = arr.value(idx);
            if val.is_empty() {
                writer.write_all(empty_value())?;
                return Ok(());
            }
            write_lower_hex(writer, val)?;
        }
        DataType::FixedSizeBinary(16) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("DataType/Array mismatch");
            let val = arr.value(idx);
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(val);
            write!(writer, "{}", uuid::Uuid::from_bytes(bytes).hyphenated())?;
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("DataType/Array mismatch");
            let days = arr.value(idx);
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
            let date =
                chrono::Duration::try_days(days as i64).and_then(|d| epoch.checked_add_signed(d));
            if let Some(date) = date {
                write!(writer, "{}", date)?;
            }
        }
        DataType::Time64(unit) => write_time64(writer, array, idx, unit)?,
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("DataType/Array mismatch");
            let is_instant = tz.is_some();
            let micros = arr.value(idx);
            let secs = micros.div_euclid(1_000_000);
            let nsecs = (micros.rem_euclid(1_000_000) * 1_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                use chrono::{Datelike as _, Timelike as _};
                let y = dt.year();
                if (0..=9999).contains(&y) {
                    write!(
                        writer,
                        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                        y,
                        dt.month(),
                        dt.day(),
                        dt.hour(),
                        dt.minute(),
                        dt.second(),
                        dt.nanosecond() / 1_000
                    )?;
                } else {
                    write!(writer, "{}", dt.format("%Y-%m-%dT%H:%M:%S%.6f"))?;
                }
                if is_instant {
                    writer.write_all(b"Z")?;
                }
            }
        }
        other => {
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_DEST_INVARIANT,
                "CSV: no serializer for Arrow type {other:?} (column should have been rejected at writer creation)"
            );
        }
    }

    Ok(())
}

/// Counts RFC-4180 records by streaming the file.
pub(crate) fn count_csv_records(path: &Path) -> Result<usize> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut records = 0usize;
    let mut in_quotes = false;
    let mut pending = false;
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            break;
        }
        let len = buf.len();
        for &byte in buf {
            match byte {
                b'"' => {
                    in_quotes = !in_quotes;
                    pending = true;
                }
                b'\n' if !in_quotes => {
                    records += 1;
                    pending = false;
                }
                _ => pending = true,
            }
        }
        reader.consume(len);
    }
    if pending {
        records += 1;
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn cell<A: Array + 'static>(array: A, idx: usize) -> String {
        let mut buf = Vec::new();
        write_csv_value(&mut buf, &array, idx).unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn null_cell(dt: DataType) -> String {
        use arrow::array::new_null_array;
        let arr = new_null_array(&dt, 1);
        let mut buf = Vec::new();
        write_csv_value(&mut buf, arr.as_ref(), 0).unwrap();
        String::from_utf8(buf).unwrap()
    }

    /// A nanosecond time of day (SQL Server `time(7)`) keeps its 100ns tick in CSV.
    #[test]
    fn time64_nanoseconds_render_nine_fraction_digits() {
        use arrow::array::Time64NanosecondArray;
        assert!(csv_serializable(&DataType::Time64(TimeUnit::Nanosecond)));
        assert_eq!(
            cell(Time64NanosecondArray::from(vec![86_399_999_999_900_i64]), 0),
            "23:59:59.999999900"
        );
        assert_eq!(
            cell(Time64NanosecondArray::from(vec![100_i64]), 0),
            "00:00:00.000000100"
        );
    }

    #[test]
    fn time64_renders_exact_wall_clock() {
        use arrow::array::Time64MicrosecondArray;
        assert_eq!(
            cell(Time64MicrosecondArray::from(vec![86_399_999_999_i64]), 0),
            "23:59:59.999999"
        );
        assert_eq!(
            cell(Time64MicrosecondArray::from(vec![3_661_000_001_i64]), 0),
            "01:01:01.000001"
        );
        assert_eq!(
            cell(Time64MicrosecondArray::from(vec![0_i64]), 0),
            "00:00:00.000000"
        );
    }

    #[test]
    fn negative_time_renders_one_leading_sign_not_a_minus_per_field() {
        use arrow::array::Time64MicrosecondArray;
        for (micros, want) in [
            (-5_400_000_000_i64, "-01:30:00.000000"),
            (-1_i64, "-00:00:00.000001"),
            (-1_500_000_i64, "-00:00:01.500000"),
            (-3_020_399_000_000_i64, "-838:59:59.000000"),
            (5_400_000_000_i64, "01:30:00.000000"),
        ] {
            assert_eq!(
                cell(Time64MicrosecondArray::from(vec![micros]), 0),
                want,
                "micros={micros}"
            );
        }
    }

    #[test]
    fn write_batch_layout_and_byte_accounting_are_exact() {
        use arrow::array::Int64Array;
        use std::sync::{Arc as SArc, Mutex};

        #[derive(Clone)]
        struct SharedBuf(SArc<Mutex<Vec<u8>>>);
        impl std::io::Write for SharedBuf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let sink = SharedBuf(SArc::new(Mutex::new(Vec::new())));
        use crate::format::Format as _;
        let mut w = CsvFormat
            .create_writer(&schema, Box::new(sink.clone()))
            .unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        w.write_batch(&batch).unwrap();

        let text = String::from_utf8(sink.0.lock().unwrap().clone()).unwrap();
        assert_eq!(
            text, "a,b\n1,10\n2,20\n",
            "exact CSV layout (no leading commas)"
        );
        assert_eq!(
            w.bytes_written(),
            text.len() as u64,
            "byte accounting must equal the physical output"
        );
    }

    #[test]
    fn null_value_writes_empty_string() {
        assert_eq!(null_cell(DataType::Int64), "");
        assert_eq!(null_cell(DataType::Utf8), "");
        assert_eq!(null_cell(DataType::Boolean), "");
    }

    /// A present empty string is `""` while NULL stays bare.
    #[test]
    fn an_empty_string_is_quoted_and_distinct_from_null() {
        assert_eq!(cell(StringArray::from(vec![Some("")]), 0), "\"\"");
        assert_eq!(null_cell(DataType::Utf8), "");
        assert_ne!(
            cell(StringArray::from(vec![Some("")]), 0),
            null_cell(DataType::Utf8),
            "NULL and empty string must not collide byte-identically"
        );
    }

    #[test]
    fn bool_true_writes_true() {
        assert_eq!(cell(BooleanArray::from(vec![true]), 0), "true");
    }

    #[test]
    fn bool_false_writes_false() {
        assert_eq!(cell(BooleanArray::from(vec![false]), 0), "false");
    }

    #[test]
    fn int16_value() {
        assert_eq!(cell(Int16Array::from(vec![42i16]), 0), "42");
    }

    #[test]
    fn int32_negative() {
        assert_eq!(cell(Int32Array::from(vec![-7i32]), 0), "-7");
    }

    #[test]
    fn decimal128_writes_exact_text() {
        let arr = Decimal128Array::from(vec![10i128])
            .with_precision_and_scale(18, 2)
            .unwrap();
        assert_eq!(cell(arr, 0), "0.10");
        let scaled =
            crate::types::decimal::decimal_str_to_scaled_i128("999999999999.99", 2).unwrap();
        let arr = Decimal128Array::from(vec![scaled])
            .with_precision_and_scale(18, 2)
            .unwrap();
        assert_eq!(cell(arr, 0), "999999999999.99");
    }

    #[test]
    fn int64_large() {
        assert_eq!(
            cell(Int64Array::from(vec![9_999_999_999i64]), 0),
            "9999999999"
        );
    }

    #[test]
    fn float32_value() {
        let result = cell(Float32Array::from(vec![1.5f32]), 0);
        assert!(result.starts_with("1.5"), "got: {result}");
    }

    #[test]
    fn float64_value() {
        let result = cell(Float64Array::from(vec![std::f64::consts::PI]), 0);
        assert!(result.starts_with("3.14"), "got: {result}");
    }

    #[test]
    fn float_special_values_emit_literals_not_empty() {
        assert_eq!(cell(Float64Array::from(vec![f64::NAN]), 0), "NaN");
        assert_eq!(cell(Float64Array::from(vec![f64::INFINITY]), 0), "inf");
        assert_eq!(cell(Float64Array::from(vec![f64::NEG_INFINITY]), 0), "-inf");
        assert_eq!(cell(Float32Array::from(vec![f32::NAN]), 0), "NaN");
        assert_eq!(cell(Float32Array::from(vec![f32::INFINITY]), 0), "inf");
        assert_eq!(cell(Float64Array::from(vec![-0.0f64]), 0), "-0");
    }

    #[test]
    fn plain_string_no_quoting() {
        assert_eq!(cell(StringArray::from(vec!["hello"]), 0), "hello");
    }

    #[test]
    fn string_with_comma_is_quoted() {
        assert_eq!(cell(StringArray::from(vec!["a,b"]), 0), "\"a,b\"");
    }

    #[test]
    fn string_with_double_quote_is_escaped() {
        let result = cell(StringArray::from(vec![r#"say "hi""#]), 0);
        assert_eq!(result, r#""say ""hi""""#);
    }

    #[test]
    fn string_with_newline_is_quoted() {
        let result = cell(StringArray::from(vec!["line1\nline2"]), 0);
        assert!(
            result.starts_with('"') && result.ends_with('"'),
            "got: {result}"
        );
        assert!(result.contains("line1\nline2"), "got: {result}");
    }

    #[test]
    fn roast_string_with_carriage_return_is_quoted() {
        let result = cell(StringArray::from(vec!["a\rb"]), 0);
        assert_eq!(
            result, "\"a\rb\"",
            "lone CR must force quoting per RFC 4180, but got unquoted cell {result:?}"
        );
    }

    #[test]
    fn binary_is_written_as_hex() {
        let arr = BinaryArray::from_vec(vec![&[0xDE, 0xAD, 0xBE, 0xEF][..]]);
        assert_eq!(cell(arr, 0), "deadbeef");
    }

    #[test]
    fn an_empty_binary_is_quoted_and_distinct_from_null() {
        assert_eq!(cell(BinaryArray::from_vec(vec![&[][..]]), 0), "\"\"");
        assert_eq!(null_cell(DataType::Binary), "");
    }

    #[test]
    fn date32_epoch_is_1970_01_01() {
        assert_eq!(cell(Date32Array::from(vec![0i32]), 0), "1970-01-01");
    }

    #[test]
    fn date32_positive_offset() {
        assert_eq!(cell(Date32Array::from(vec![365i32]), 0), "1971-01-01");
    }

    #[test]
    fn timestamp_micros_formats_as_iso() {
        let micros: i64 = 1_672_531_200 * 1_000_000;
        let _schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let arr = TimestampMicrosecondArray::from(vec![micros]);
        let result = cell(arr, 0);
        assert!(result.starts_with("2023-01-01T"), "got: {result}");
        assert!(result.contains("00:00:00"), "got: {result}");
    }

    #[test]
    fn timestamp_marks_instant_utc_but_leaves_naive_bare() {
        let micros: i64 = 1_718_420_400 * 1_000_000;
        let naive = TimestampMicrosecondArray::from(vec![micros]);
        assert_eq!(
            cell(naive, 0),
            "2024-06-15T03:00:00.000000",
            "a naive TIMESTAMP renders bare — no tz marker"
        );
        let instant = TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC");
        assert_eq!(
            cell(instant, 0),
            "2024-06-15T03:00:00.000000Z",
            "an instant TIMESTAMPTZ renders an explicit UTC `Z`"
        );
    }

    #[test]
    fn csv_format_write_batch_tracks_bytes_and_succeeds() {
        use crate::format::Format;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), None])),
            ],
        )
        .unwrap();

        let fmt = CsvFormat;
        let mut writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.write_batch(&batch).unwrap();
        assert!(
            writer.bytes_written() > 10,
            "expected >10 bytes, got {}",
            writer.bytes_written()
        );
        writer.finish().unwrap();
    }

    #[test]
    fn csv_header_quotes_a_column_name_with_a_comma_or_quote() {
        use crate::format::Format;
        let schema = Arc::new(Schema::new(vec![
            Field::new("Amount, USD", DataType::Int64, false),
            Field::new("a\"b", DataType::Utf8, true),
            Field::new("plain", DataType::Utf8, true),
        ]));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        let w = CsvFormat
            .create_writer(&schema, Box::new(std::fs::File::create(&path).unwrap()))
            .unwrap();
        w.finish().unwrap();
        let out = std::fs::read_to_string(&path).unwrap();
        let header = out.lines().next().unwrap();
        assert_eq!(header, r#""Amount, USD","a""b",plain"#);
    }

    #[test]
    fn csv_rejects_array_columns_loudly() {
        use crate::format::Format;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));
        let Err(err) = CsvFormat.create_writer(&schema, Box::new(Vec::<u8>::new())) else {
            panic!("CSV must reject array columns, not silently drop them");
        };
        let msg = format!("{err:#}");
        assert!(msg.contains("tags"), "error must name the column: {msg}");
        assert!(msg.to_lowercase().contains("csv"), "{msg}");
    }

    /// Consistency guard: every type `csv_serializable` admits must actually be handled by `write_csv_value` (not hit its `other => bail` fallthrough).
    #[test]
    fn every_serializable_type_is_actually_written() {
        use crate::format::Format;
        let cols: Vec<(&str, ArrayRef)> = vec![
            ("b", Arc::new(BooleanArray::from(vec![true]))),
            ("i16", Arc::new(Int16Array::from(vec![1i16]))),
            ("i32", Arc::new(Int32Array::from(vec![1i32]))),
            ("i64", Arc::new(Int64Array::from(vec![1i64]))),
            ("u64", Arc::new(UInt64Array::from(vec![1u64]))),
            (
                "dec",
                Arc::new(
                    Decimal128Array::from(vec![100i128])
                        .with_precision_and_scale(18, 2)
                        .unwrap(),
                ),
            ),
            ("f32", Arc::new(Float32Array::from(vec![1.0f32]))),
            ("f64", Arc::new(Float64Array::from(vec![1.0f64]))),
            ("s", Arc::new(StringArray::from(vec!["x"]))),
            ("bin", Arc::new(BinaryArray::from_vec(vec![&[1u8][..]]))),
            (
                "uuid",
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(std::iter::once(vec![0u8; 16])).unwrap(),
                ),
            ),
            ("d", Arc::new(Date32Array::from(vec![0i32]))),
            ("t", Arc::new(Time64MicrosecondArray::from(vec![0i64]))),
            ("ts", Arc::new(TimestampMicrosecondArray::from(vec![0i64]))),
        ];
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect();
        for f in &fields {
            assert!(
                csv_serializable(f.data_type()),
                "test type {:?} not in csv_serializable",
                f.data_type()
            );
        }
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<ArrayRef> = cols.into_iter().map(|(_, a)| a).collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let mut w = CsvFormat
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        w.write_batch(&batch)
            .expect("every serializable type must write without hitting the fallthrough");
    }

    #[test]
    fn binary_hex_matches_per_byte_format_for_all_byte_values() {
        let all: Vec<u8> = (0..=255u8).collect();
        for case in [&all[..], &[0x00, 0xff, 0x10, 0x0a]] {
            let expected: String = case.iter().map(|b| format!("{b:02x}")).collect();
            let got = cell(BinaryArray::from_vec(vec![case]), 0);
            assert_eq!(got, expected, "hex mismatch for {case:?}");
        }
    }

    #[test]
    fn binary_hex_spans_chunk_boundary() {
        let big: Vec<u8> = (0..2000u32).map(|i| (i % 256) as u8).collect();
        let expected: String = big.iter().map(|b| format!("{b:02x}")).collect();
        let got = cell(BinaryArray::from_vec(vec![&big[..]]), 0);
        assert_eq!(got, expected);
    }

    #[test]
    fn timestamp_fast_path_matches_chrono_format() {
        let cases: [i64; 6] = [
            0,
            1_700_000_000_123_456,
            1_000_000 * 86_399 + 999_999,
            -62_135_596_800_000_000,
            253_402_300_799_000_000,
            300_000_000_000_000_000,
        ];
        for micros in cases {
            let got = cell(TimestampMicrosecondArray::from(vec![micros]), 0);
            let secs = micros.div_euclid(1_000_000);
            let nsecs = (micros.rem_euclid(1_000_000) * 1_000) as u32;
            let expected = match chrono::DateTime::from_timestamp(secs, nsecs) {
                Some(dt) => format!("{}", dt.format("%Y-%m-%dT%H:%M:%S%.6f")),
                None => String::new(),
            };
            assert_eq!(got, expected, "timestamp mismatch for micros={micros}");
        }
    }

    #[test]
    fn pre_1970_subsecond_timestamp_is_not_dropped_to_an_empty_cell() {
        let expect: [(i64, &str); 4] = [
            (-1, "1969-12-31T23:59:59.999999"),
            (-500_000, "1969-12-31T23:59:59.500000"),
            (-86_400_000_000, "1969-12-31T00:00:00.000000"),
            (-125_125_000, "1969-12-31T23:57:54.875000"),
        ];
        for (micros, want) in expect {
            let got = cell(TimestampMicrosecondArray::from(vec![micros]), 0);
            assert_eq!(
                got, want,
                "pre-1970 micros={micros} must render {want:?}, not an empty/other cell"
            );
        }
    }
}

#[cfg(test)]
mod error_code_tests {
    #[test]
    fn a_column_csv_cannot_serialize_is_refused_by_code() {
        use crate::format::Format as _;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(std::sync::Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            ))),
            true,
        )]));
        let err = super::CsvFormat
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .err()
            .expect("a list column has no CSV cell");
        crate::error::assert_coded(&err, "RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED", 1);
    }
}
