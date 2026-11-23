use std::io::{BufRead, Write};
use std::path::Path;

use arrow::array::types::{
    Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampNanosecondType, UInt64Type,
};
use arrow::array::*;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::types::decimal::{scaled_i128_to_decimal_str, scaled_i256_to_decimal_str};

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

/// Arrow types `write_csv_value` can serialize: a scalar, or a flat list of scalars written as JSON array text.
pub(crate) fn csv_serializable(dt: &DataType) -> bool {
    if seeded!("csv-text-types-refused", true, false)
        && matches!(
            dt,
            DataType::List(_)
                | DataType::Decimal256(_, _)
                | DataType::Timestamp(TimeUnit::Nanosecond, _)
        )
    {
        return false;
    }
    match dt {
        DataType::List(item) => scalar_serializable(item.data_type()),
        other => scalar_serializable(other),
    }
}

/// Arrow scalar types with a lossless CSV text.
fn scalar_serializable(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Binary
            | DataType::FixedSizeBinary(16)
            | DataType::Date32
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Time64(TimeUnit::Nanosecond)
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
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

/// One cell: empty for NULL, a quoted field for text and list cells, the scalar text otherwise.
fn write_csv_value(writer: &mut dyn Write, array: &dyn Array, idx: usize) -> Result<()> {
    if array.is_null(idx) {
        return Ok(());
    }
    match array.data_type() {
        DataType::List(_) => write_list_cell(writer, array.as_list::<i32>(), idx),
        DataType::Utf8 => write_text_field(writer, array.as_string::<i32>().value(idx).as_bytes()),
        DataType::Binary if array.as_binary::<i32>().value(idx).is_empty() => {
            Ok(writer.write_all(empty_value())?)
        }
        _ => write_scalar_text(writer, array, idx),
    }
}

/// A text field: an empty value as the quoted empty field, a value with a delimiter, quote or line break quoted with doubled quotes.
fn write_text_field(writer: &mut dyn Write, val: &[u8]) -> Result<()> {
    if val.is_empty() {
        return Ok(writer.write_all(empty_value())?);
    }
    if !val.iter().any(|b| matches!(b, b',' | b'"' | b'\n' | b'\r')) {
        return Ok(writer.write_all(val)?);
    }
    writer.write_all(b"\"")?;
    for (n, part) in val.split(|b| *b == b'"').enumerate() {
        if n > 0 {
            writer.write_all(b"\"\"")?;
        }
        writer.write_all(part)?;
    }
    Ok(writer.write_all(b"\"")?)
}

/// A list cell: the elements as one JSON array text in a single field.
fn write_list_cell(writer: &mut dyn Write, list: &ListArray, idx: usize) -> Result<()> {
    let items = list.value(idx);
    let mut json = Vec::with_capacity(items.len() * 8 + 2);
    json.push(b'[');
    for i in 0..items.len() {
        if i > 0 {
            json.push(b',');
        }
        write_json_element(&mut json, items.as_ref(), i)?;
    }
    json.push(b']');
    write_text_field(writer, &json)
}

/// One list element as JSON: null, a bare number or boolean, else a JSON string of its scalar text.
fn write_json_element(out: &mut Vec<u8>, array: &dyn Array, idx: usize) -> Result<()> {
    if array.is_null(idx) {
        out.extend_from_slice(b"null");
        return Ok(());
    }
    let mut text = Vec::new();
    write_scalar_text(&mut text, array, idx)?;
    let bare = match array.data_type() {
        DataType::Boolean | DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
        DataType::UInt64 => true,
        DataType::Float32 | DataType::Float64 => text
            .iter()
            .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'.')),
        _ => false,
    };
    if bare {
        out.extend_from_slice(&text);
        return Ok(());
    }
    serde_json::to_writer(&mut *out, String::from_utf8_lossy(&text).as_ref())
        .map_err(std::io::Error::from)?;
    Ok(())
}

/// The text of one non-null scalar, with no CSV quoting.
fn write_scalar_text(writer: &mut dyn Write, array: &dyn Array, idx: usize) -> Result<()> {
    match array.data_type() {
        DataType::Boolean => write!(writer, "{}", array.as_boolean().value(idx))?,
        DataType::Int16 => write_primitive::<Int16Type>(writer, array, idx)?,
        DataType::Int32 => write_primitive::<Int32Type>(writer, array, idx)?,
        DataType::Int64 => write_primitive::<Int64Type>(writer, array, idx)?,
        DataType::UInt64 => write_primitive::<UInt64Type>(writer, array, idx)?,
        DataType::Float32 => write_primitive::<Float32Type>(writer, array, idx)?,
        DataType::Float64 => write_primitive::<Float64Type>(writer, array, idx)?,
        DataType::Decimal128(_, scale) => {
            let value = array.as_primitive::<Decimal128Type>().value(idx);
            writer.write_all(scaled_i128_to_decimal_str(value, *scale).as_bytes())?;
        }
        DataType::Decimal256(_, scale) => {
            let value = array.as_primitive::<Decimal256Type>().value(idx);
            writer.write_all(scaled_i256_to_decimal_str(value, *scale).as_bytes())?;
        }
        DataType::Utf8 => writer.write_all(array.as_string::<i32>().value(idx).as_bytes())?,
        DataType::Binary => write_lower_hex(writer, array.as_binary::<i32>().value(idx))?,
        DataType::FixedSizeBinary(16) => {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(array.as_fixed_size_binary().value(idx));
            write!(writer, "{}", uuid::Uuid::from_bytes(bytes).hyphenated())?;
        }
        DataType::Date32 => write_date32(writer, array.as_primitive::<Date32Type>().value(idx))?,
        DataType::Time64(unit) => write_time64(writer, array, idx, unit)?,
        DataType::Timestamp(TimeUnit::Microsecond, tz) => write_timestamp(
            writer,
            array.as_primitive::<TimestampMicrosecondType>().value(idx),
            1_000,
            tz.is_some(),
        )?,
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => write_timestamp(
            writer,
            array.as_primitive::<TimestampNanosecondType>().value(idx),
            1,
            tz.is_some(),
        )?,
        other => {
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_DEST_INVARIANT,
                "CSV: no serializer for Arrow type {other:?} (column should have been rejected at writer creation)"
            );
        }
    }
    Ok(())
}

/// A primitive cell in its `Display` form.
fn write_primitive<T: ArrowPrimitiveType>(
    writer: &mut dyn Write,
    array: &dyn Array,
    idx: usize,
) -> Result<()>
where
    T::Native: std::fmt::Display,
{
    write!(writer, "{}", array.as_primitive::<T>().value(idx))?;
    Ok(())
}

/// A `Date32` cell as `YYYY-MM-DD`; nothing for a day count chrono cannot hold.
fn write_date32(writer: &mut dyn Write, days: i32) -> Result<()> {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
    let date = chrono::Duration::try_days(days as i64).and_then(|d| epoch.checked_add_signed(d));
    if let Some(date) = date {
        write!(writer, "{}", date)?;
    }
    Ok(())
}

/// A timestamp cell: six fraction digits for microseconds, nine for nanoseconds, a trailing `Z` for an instant.
fn write_timestamp(
    writer: &mut dyn Write,
    value: i64,
    nanos_per_unit: i64,
    is_instant: bool,
) -> Result<()> {
    let per_second = 1_000_000_000 / nanos_per_unit;
    let secs = value.div_euclid(per_second);
    let nsecs = (value.rem_euclid(per_second) * nanos_per_unit) as u32;
    let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) else {
        return Ok(());
    };
    if nanos_per_unit == 1 {
        let text = crate::types::iso_timestamp_nanos(dt.naive_utc(), is_instant);
        writer.write_all(text.as_bytes())?;
        return Ok(());
    }
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
    fn csv_rejects_nested_array_columns_loudly() {
        use crate::format::Format;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ))),
                true,
            ),
        ]));
        let Err(err) = CsvFormat.create_writer(&schema, Box::new(Vec::<u8>::new())) else {
            panic!("CSV must reject nested array columns, not silently drop them");
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
            ("tsn", Arc::new(TimestampNanosecondArray::from(vec![0i64]))),
            (
                "dec256",
                Arc::new(
                    Decimal256Array::from(vec![arrow::datatypes::i256::from_i128(1)])
                        .with_precision_and_scale(60, 10)
                        .unwrap(),
                ),
            ),
            (
                "list",
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1)]),
                ])),
            ),
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
mod text_cell_tests {
    use super::*;
    use arrow::array::types::{Int32Type, TimestampNanosecondType};
    use arrow::datatypes::i256;
    use std::sync::Arc;

    fn cell(array: &dyn Array, idx: usize) -> String {
        let mut buf = Vec::new();
        write_csv_value(&mut buf, array, idx).unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn unquote(field: &str) -> String {
        match field.strip_prefix('"').and_then(|f| f.strip_suffix('"')) {
            Some(inner) => inner.replace("\"\"", "\""),
            None => field.to_string(),
        }
    }

    fn json_of(array: &dyn Array, idx: usize) -> serde_json::Value {
        serde_json::from_str(&unquote(&cell(array, idx))).expect("a list cell is JSON array text")
    }

    fn dec256(v: &str, precision: u8, scale: i8) -> Decimal256Array {
        Decimal256Array::from(vec![i256::from_string(v).unwrap()])
            .with_precision_and_scale(precision, scale)
            .unwrap()
    }

    fn strings(items: Vec<Option<&str>>) -> ListArray {
        let mut b = ListBuilder::new(StringBuilder::new());
        for i in items {
            b.values().append_option(i);
        }
        b.append(true);
        b.finish()
    }

    #[test]
    fn decimal256_writes_exact_plain_text_beyond_i128() {
        let big = "123456789012345678901234567890123456789012345678901234567890";
        assert_eq!(
            cell(&dec256(big, 60, 10), 0),
            "12345678901234567890123456789012345678901234567890.1234567890"
        );
        assert_eq!(cell(&dec256("-5", 60, 10), 0), "-0.0000000005");
        assert_eq!(cell(&dec256("42", 50, 0), 0), "42");
        assert_eq!(cell(&dec256("42", 50, -2), 0), "4200");
    }

    #[test]
    fn decimal256_text_equals_decimal128_text_where_both_fit() {
        let d128 = Decimal128Array::from(vec![-123_456_789_i128])
            .with_precision_and_scale(38, 4)
            .unwrap();
        assert_eq!(
            cell(&dec256("-123456789", 60, 4), 0),
            cell(&d128, 0),
            "one decimal text across widths"
        );
    }

    #[test]
    fn timestamp_nanos_write_nine_digits_and_z_only_for_an_instant() {
        let naive = TimestampNanosecondArray::from(vec![1_719_136_800_123_456_789_i64]);
        assert_eq!(cell(&naive, 0), "2024-06-23T10:00:00.123456789");
        let zoned = naive.clone().with_timezone("UTC");
        assert_eq!(cell(&zoned, 0), "2024-06-23T10:00:00.123456789Z");
        let before_epoch = TimestampNanosecondArray::from(vec![-1_i64]);
        assert_eq!(cell(&before_epoch, 0), "1969-12-31T23:59:59.999999999");
    }

    #[test]
    fn timestamp_micros_keep_six_digits_next_to_nanos() {
        let us = TimestampMicrosecondArray::from(vec![1_719_136_800_123_456_i64]);
        assert_eq!(cell(&us, 0), "2024-06-23T10:00:00.123456");
        assert_eq!(
            cell(&us.with_timezone("UTC"), 0),
            "2024-06-23T10:00:00.123456Z"
        );
    }

    #[test]
    fn a_list_of_integers_is_a_json_array_with_null_elements() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            None,
            Some(3),
        ])]);
        assert_eq!(cell(&list, 0), "\"[1,null,3]\"");
        let empty = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![])]);
        assert_eq!(cell(&empty, 0), "[]");
    }

    #[test]
    fn a_null_list_is_the_bare_null_field_and_an_empty_list_is_not() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            None,
            Some(Vec::<Option<i32>>::new()),
        ]);
        assert_eq!(cell(&list, 0), "");
        assert_eq!(cell(&list, 1), "[]");
    }

    #[test]
    fn a_list_of_text_keeps_commas_quotes_nulls_and_empty_strings_apart() {
        let items = vec![
            Some("with,comma"),
            Some("he said \"hi\""),
            None,
            Some(""),
            Some("line\nbreak"),
            Some("null"),
        ];
        let list = strings(items);
        assert_eq!(
            json_of(&list, 0),
            serde_json::json!([
                "with,comma",
                "he said \"hi\"",
                null,
                "",
                "line\nbreak",
                "null"
            ])
        );
    }

    #[test]
    fn a_list_of_floats_names_non_finite_values_like_postgres_json() {
        let list =
            ListArray::from_iter_primitive::<arrow::array::types::Float64Type, _, _>(vec![Some(
                vec![Some(2.5), Some(f64::NAN), Some(f64::INFINITY), Some(-0.5)],
            )]);
        assert_eq!(
            json_of(&list, 0),
            serde_json::json!([2.5, "NaN", "inf", -0.5])
        );
    }

    #[test]
    fn a_list_of_typed_scalars_renders_each_as_its_scalar_text() {
        let mut dates = ListBuilder::new(Date32Builder::new());
        dates.values().append_value(19_723);
        dates.values().append_null();
        dates.append(true);
        assert_eq!(
            json_of(&dates.finish(), 0),
            serde_json::json!(["2024-01-01", null])
        );

        let mut ts = ListBuilder::new(TimestampMicrosecondBuilder::new().with_timezone("UTC"));
        ts.values().append_value(1_719_136_800_000_001);
        ts.append(true);
        assert_eq!(
            json_of(&ts.finish(), 0),
            serde_json::json!(["2024-06-23T10:00:00.000001Z"])
        );

        let mut bin = ListBuilder::new(BinaryBuilder::new());
        bin.values().append_value([0xde, 0xad, 0xbe, 0xef]);
        bin.values().append_value([]);
        bin.append(true);
        assert_eq!(
            json_of(&bin.finish(), 0),
            serde_json::json!(["deadbeef", ""])
        );

        let mut uuid = ListBuilder::new(FixedSizeBinaryBuilder::new(16));
        uuid.values().append_value([0x12; 16]).unwrap();
        uuid.append(true);
        assert_eq!(
            json_of(&uuid.finish(), 0),
            serde_json::json!(["12121212-1212-1212-1212-121212121212"])
        );

        let mut tm = ListBuilder::new(Time64MicrosecondBuilder::new());
        tm.values().append_value(36_000_000_000);
        tm.append(true);
        assert_eq!(
            json_of(&tm.finish(), 0),
            serde_json::json!(["10:00:00.000000"])
        );
    }

    #[test]
    fn a_list_of_nanosecond_timestamps_and_wide_decimals_is_text_per_element() {
        let mut ts = ListBuilder::new(PrimitiveBuilder::<TimestampNanosecondType>::new());
        ts.values().append_value(1_719_136_800_123_456_789);
        ts.append(true);
        assert_eq!(
            json_of(&ts.finish(), 0),
            serde_json::json!(["2024-06-23T10:00:00.123456789"])
        );
    }

    #[test]
    fn csv_admits_wide_decimals_nanosecond_timestamps_and_flat_lists() {
        for dt in [
            DataType::Decimal256(60, 10),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Utf8,
                true,
            ))),
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ))),
        ] {
            assert!(csv_serializable(&dt), "{dt:?} must have a CSV cell");
        }
    }

    #[test]
    fn csv_still_refuses_what_has_no_lossless_cell() {
        let inner = Arc::new(arrow::datatypes::Field::new("item", DataType::Utf8, true));
        let nested = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::List(inner),
            true,
        )));
        for dt in [
            nested,
            DataType::Int8,
            DataType::Float16,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Second, None),
        ] {
            assert!(!csv_serializable(&dt), "{dt:?} stays refused");
            let err = refuse_column("c", &dt).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED", 1);
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
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
                true,
            ))),
            true,
        )]));
        let err = super::CsvFormat
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .err()
            .expect("a nested list column has no CSV cell");
        crate::error::assert_coded(&err, "RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED", 1);
    }
}
