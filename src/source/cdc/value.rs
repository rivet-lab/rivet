//! `RivetValue` — a typed, owned cell value for the CDC path, replacing the lossy `serde_json::Value` intermediate.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
    Time64MicrosecondBuilder, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
    TimestampNanosecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use serde_json::Value as Json;

use crate::source::value_checksum::ListElem;

/// Days from the Unix epoch (1970-01-01) for `Date32`.
fn epoch_days(d: NaiveDate) -> i32 {
    (d - NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch")).num_days() as i32
}

/// A typed, owned CDC cell value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RivetValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    /// Naive date-time, extracted structurally (the source session is UTC in rivet's setups.
    DateTime(NaiveDateTime),
    /// Microseconds since midnight (`TIME`).
    TimeMicros(i64),
    /// Nanoseconds since midnight (a SQL Server `time` of scale 7, whose 100ns tick a microsecond would drop).
    TimeNanos(i64),
    /// Raw source bytes — `Utf8` text, `Binary`, or a `Decimal` string, decided by the target Arrow type at build time.
    Bytes(Vec<u8>),
    /// A one-dimensional array (PG `text[]`/`integer[]`/…).
    Array(Vec<RivetValue>),
}

/// The marker a MySQL zero or partial date decodes to, so the cell fix can count it before writing NULL.
pub(crate) const ZERO_DATE: &[u8] = b"0000-00-00";

impl RivetValue {
    /// Map a MySQL driver value to `RivetValue` — structurally, no string reparse.
    pub(crate) fn from_mysql(v: &mysql::Value) -> Self {
        use mysql::Value;
        match v {
            Value::NULL => RivetValue::Null,
            Value::Int(i) => RivetValue::Int(*i),
            Value::UInt(u) => RivetValue::UInt(*u),
            Value::Float(f) => RivetValue::Float(*f as f64),
            Value::Double(d) => RivetValue::Float(*d),
            Value::Date(y, mo, d, h, mi, s, us) => {
                NaiveDate::from_ymd_opt(*y as i32, *mo as u32, *d as u32)
                    .and_then(|date| date.and_hms_micro_opt(*h as u32, *mi as u32, *s as u32, *us))
                    .map_or_else(
                        || RivetValue::Bytes(ZERO_DATE.to_vec()),
                        RivetValue::DateTime,
                    )
            }
            Value::Time(neg, days, h, mi, s, us) => {
                let micros =
                    ((*days as i64 * 86_400 + *h as i64 * 3_600 + *mi as i64 * 60 + *s as i64)
                        * 1_000_000)
                        + *us as i64;
                RivetValue::TimeMicros(if *neg { -micros } else { micros })
            }
            Value::Bytes(b) => RivetValue::Bytes(b.clone()),
        }
    }

    /// Rough in-memory footprint of this cell — drives the sink's memory-budget rollover.
    pub(crate) fn payload_bytes(&self) -> usize {
        match self {
            RivetValue::Null | RivetValue::Bool(_) => 1,
            RivetValue::Int(_)
            | RivetValue::UInt(_)
            | RivetValue::Float(_)
            | RivetValue::TimeMicros(_)
            | RivetValue::TimeNanos(_) => 8,
            RivetValue::DateTime(_) => 12,
            RivetValue::Bytes(b) => b.len(),
            RivetValue::Array(v) => v.iter().map(RivetValue::payload_bytes).sum::<usize>(),
        }
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        match self {
            RivetValue::Null | RivetValue::Bool(_) => 1,
            RivetValue::Int(_)
            | RivetValue::UInt(_)
            | RivetValue::Float(_)
            | RivetValue::TimeMicros(_)
            | RivetValue::TimeNanos(_) => 8,
            RivetValue::DateTime(_) => 12,
            RivetValue::Bytes(b) => b.capacity(),
            RivetValue::Array(v) => {
                v.capacity() * std::mem::size_of::<RivetValue>()
                    + v.iter().map(RivetValue::estimated_bytes).sum::<usize>()
            }
        }
    }

    /// Render for NDJSON output.
    pub(crate) fn to_json(&self) -> Json {
        match self {
            RivetValue::Null => Json::Null,
            RivetValue::Bool(b) => (*b).into(),
            RivetValue::Int(i) => (*i).into(),
            RivetValue::UInt(u) => (*u).into(),
            RivetValue::Float(f) => Json::from(*f),
            RivetValue::DateTime(dt) => Json::String(dt.to_string()),
            RivetValue::TimeMicros(us) => Json::from(*us),
            RivetValue::TimeNanos(ns) => Json::from(*ns),
            RivetValue::Bytes(b) => Json::String(bytes_to_recoverable_string(b)),
            RivetValue::Array(v) => Json::Array(v.iter().map(RivetValue::to_json).collect()),
        }
    }
}

/// True when [`build_column`] can produce an array of *exactly* this Arrow type from a `RivetValue`.
pub(crate) fn is_buildable(dt: &DataType) -> bool {
    if let DataType::List(inner) = dt {
        return matches!(
            inner.data_type(),
            DataType::Boolean
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::Date32
                | DataType::Time64(TimeUnit::Microsecond)
                | DataType::Timestamp(TimeUnit::Microsecond, _)
                | DataType::FixedSizeBinary(16)
                | DataType::Binary
        );
    }
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Time64(TimeUnit::Nanosecond)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
    )
}

/// Canonical bytes for a `FixedSizeBinary(n)` cell, or `None` when the value genuinely cannot fill the width.
fn fixed_binary_bytes(by: &[u8], n: usize) -> Option<Vec<u8>> {
    if by.len() == n {
        return Some(by.to_vec());
    }
    if n == 16
        && let Ok(s) = std::str::from_utf8(by)
        && let Ok(u) = uuid::Uuid::parse_str(s.trim())
    {
        return Some(u.as_bytes().to_vec());
    }
    None
}

/// A non-NULL cell its planned column type cannot hold: the row, the value, and why.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CellRefusal {
    pub row: usize,
    pub value: RivetValue,
    pub reason: String,
    /// The engine's own code when the engine worded the refusal (its reason then carries the remedy).
    pub code: Option<crate::error::Code>,
}

/// Why an engine cell fix cannot read a wire value, optionally with the engine's own code.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FixRefusal {
    pub reason: String,
    pub code: Option<crate::error::Code>,
}

impl From<&str> for FixRefusal {
    /// A refusal worded by the fix, coded by the sink.
    fn from(reason: &str) -> Self {
        Self {
            reason: reason.into(),
            code: None,
        }
    }
}

impl From<String> for FixRefusal {
    /// A refusal worded by the fix, coded by the sink.
    fn from(reason: String) -> Self {
        Self { reason, code: None }
    }
}

/// Why a cell is refused when its variant has no reading as the column type at all.
const NO_READING: &str = "cannot be read as that type";

impl CellRefusal {
    /// A refusal of `value` at `row` for `reason`.
    fn new(row: usize, value: &RivetValue, reason: impl Into<String>) -> Self {
        Self::refused(row, value, FixRefusal::from(reason.into()))
    }

    /// A refusal of `value` at `row` as worded, and coded, by the fix that met it.
    fn refused(row: usize, value: &RivetValue, r: FixRefusal) -> Self {
        Self {
            row,
            value: value.clone(),
            reason: r.reason,
            code: r.code,
        }
    }

    /// The coded run error naming `column` of type `dt`; `overridden` picks the override code and remedy.
    pub(crate) fn into_error(self, column: &str, dt: &DataType, overridden: bool) -> anyhow::Error {
        let full = render_str(&self.value);
        let shown: String = full.chars().take(64).collect();
        let more = if shown.len() < full.len() { "…" } else { "" };
        let (code, remedy) = if let Some(code) = self.code {
            (code, String::new())
        } else if overridden {
            (
                crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
                format!(
                    "correct or drop the `columns:` override for '{column}', then re-snapshot \
                     the table"
                ),
            )
        } else {
            (
                crate::error::codes::SOURCE_CDC_CELL_UNSUPPORTED,
                format!(
                    "leave the column out of the capture, then: {}",
                    crate::source::cdc::checkpoint_identity::RECOVER
                ),
            )
        };
        anyhow::Error::new(crate::error::CodedError::new(
            code,
            format!(
                "cdc: column '{column}' is {dt} but the captured value {shown:?}{more} (row {}) \
                 {}; rivet refuses rather than writing NULL. {remedy}",
                self.row, self.reason
            )
            .trim_end()
            .to_string(),
        ))
    }
}

/// The flag a 0 or 1 holds as a boolean, or the refusal by code for any other integer.
fn flag_value(v: i128) -> Result<bool, FixRefusal> {
    match crate::source::mysql::flag_bool(v) {
        _ if seeded!("cdc-bool-cell-any-nonzero-true", v != 0, false) => Ok(true),
        Some(f) => Ok(f),
        None => Err(FixRefusal {
            reason: format!(
                "is not a flag: a boolean column carries 0 or 1 only. Declare the column an \
                 integer type in `columns:` (for example `int64`) to carry the number, or leave \
                 the column out of the capture, then: {}",
                crate::source::cdc::checkpoint_identity::RECOVER
            ),
            code: Some(crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE),
        }),
    }
}

/// The value a Boolean column stores for `v`: a boolean, or an integer that is a flag.
fn bool_value(v: &RivetValue) -> Result<bool, FixRefusal> {
    match v {
        RivetValue::Bool(b) => Ok(*b),
        RivetValue::Int(i) => flag_value(i128::from(*i)),
        RivetValue::UInt(u) => flag_value(i128::from(*u)),
        _ => Err(NO_READING.into()),
    }
}

/// The value an integer column of width `T` stores for `v`: exact integer text parses, overflow refuses.
fn int_value<T: TryFrom<i128>>(v: &RivetValue) -> Result<T, String> {
    let wide: i128 = match v {
        RivetValue::Bool(b) => i128::from(*b),
        RivetValue::Int(i) => i128::from(*i),
        RivetValue::UInt(u) => i128::from(*u),
        RivetValue::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(crate::types::decimal::decimal_text_to_int)
            .ok_or("is not an exact integer")?,
        _ => return Err(NO_READING.into()),
    };
    T::try_from(wide).map_err(|_| {
        "overflows the column's integer width (a BIT(64) with bit 63 set, or a BIGINT \
         UNSIGNED past i64::MAX); map the column to decimal(20,0) or a wider type"
            .into()
    })
}

/// The value a Float64 column stores for `v`; decimal text is read by the shared parser and refused when a float8 would round it.
fn f64_value(v: &RivetValue) -> Result<f64, FixRefusal> {
    use crate::types::decimal::{DecimalAsF64, decimal_text_to_f64_exact};
    match v {
        RivetValue::Float(f) => Ok(*f),
        RivetValue::Int(i) => Ok(*i as f64),
        RivetValue::UInt(u) => Ok(*u as f64),
        RivetValue::Bytes(b) => match std::str::from_utf8(b).map(decimal_text_to_f64_exact) {
            Ok(DecimalAsF64::Exact(f)) => Ok(f),
            Ok(DecimalAsF64::Rounded) => Err(FixRefusal {
                reason: format!(
                    "is a decimal that a float8 cannot hold exactly (it keeps {} significant \
                     digits and the integers up to 2^53). Declare the column `decimal(p,s)` in \
                     `columns:` to keep the digits, or leave the column out of the capture, \
                     then: {}",
                    crate::types::decimal::F64_DECIMAL_DIGITS,
                    crate::source::cdc::checkpoint_identity::RECOVER
                ),
                code: Some(crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE),
            }),
            _ => Err("is not a number".into()),
        },
        _ => Err(NO_READING.into()),
    }
}

/// The value a Float32 column stores for `v`, cast directly (never through f64) so rounding matches batch.
fn f32_value(v: &RivetValue) -> Result<f32, String> {
    match v {
        RivetValue::Float(f) => Ok(*f as f32),
        RivetValue::Int(i) => Ok(*i as f32),
        RivetValue::UInt(u) => Ok(*u as f32),
        RivetValue::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(crate::types::decimal::decimal_text_to_float)
            .ok_or_else(|| "is not a number".into()),
        _ => Err(NO_READING.into()),
    }
}

/// The Date32 a DateTime at midnight stores; a time of day is refused, never dropped.
fn date_value(v: &RivetValue) -> Result<i32, String> {
    match v {
        RivetValue::DateTime(d) if d.time() == chrono::NaiveTime::MIN => Ok(epoch_days(d.date())),
        RivetValue::DateTime(_) => Err("has a time of day, which a `date` override would drop; \
                                        declare it `timestamp`"
            .into()),
        _ => Err(NO_READING.into()),
    }
}

/// The UTC instant a timestamp cell stores, as microseconds since the epoch.
fn timestamp_micros_value(v: &RivetValue) -> Result<i64, String> {
    match v {
        RivetValue::DateTime(d) => Ok(d.and_utc().timestamp_micros()),
        _ => Err(NO_READING.into()),
    }
}

/// The UTC instant a timestamp cell stores, as nanoseconds since the epoch; a value outside 1677-09-21..2262-04-11 is refused.
fn timestamp_nanos_value(v: &RivetValue) -> Result<i64, String> {
    match v {
        RivetValue::DateTime(d) => d.and_utc().timestamp_nanos_opt().ok_or_else(|| {
            "is outside the nanosecond timestamp range (1677-09-21..2262-04-11); declare the column `timestamp` (microseconds, full range) in `columns:`".to_string()
        }),
        _ => Err(NO_READING.into()),
    }
}

/// The Time64 microseconds a time of day stores; a duration outside one day is refused.
fn time_value(v: &RivetValue) -> Result<i64, String> {
    match v {
        RivetValue::TimeMicros(us) if crate::types::is_time_of_day(*us) => Ok(*us),
        RivetValue::TimeMicros(_) => {
            Err("is outside 00:00..24:00, which a Parquet TIME cannot hold".into())
        }
        RivetValue::TimeNanos(ns) if ns % 1_000 == 0 => {
            time_value(&RivetValue::TimeMicros(ns / 1_000))
        }
        RivetValue::TimeNanos(_) => Err(
            "has a 100ns digit, which a microsecond TIME column would drop; the column is read \
             at nanoseconds when the catalog shows its scale above 6"
                .into(),
        ),
        _ => Err(NO_READING.into()),
    }
}

/// Nanoseconds since midnight of a `TIME` cell, for a `Time64(ns)` column.
fn time_nanos_value(v: &RivetValue) -> Result<i64, String> {
    match v {
        RivetValue::TimeNanos(ns) if (0..crate::types::MICROS_PER_DAY * 1_000).contains(ns) => {
            Ok(*ns)
        }
        RivetValue::TimeNanos(_) => {
            Err("is outside 00:00..24:00, which a Parquet TIME cannot hold".into())
        }
        RivetValue::TimeMicros(_) => time_value(v).map(|us| us * 1_000),
        _ => Err(NO_READING.into()),
    }
}

/// `HH:MM:SS.fffffffff` for nanoseconds since midnight.
fn time_of_day_nanos(ns: i64) -> String {
    let abs = ns.unsigned_abs();
    format!(
        "{}{:02}:{:02}:{:02}.{:09}",
        if ns < 0 { "-" } else { "" },
        abs / 3_600_000_000_000,
        abs / 60_000_000_000 % 60,
        abs / 1_000_000_000 % 60,
        abs % 1_000_000_000
    )
}

/// Why a decimal cell is refused.
const NOT_A_DECIMAL: &str = "is not representable in this decimal column (NaN/Infinity, more \
                             fraction digits than the column's scale, or a float past the \
                             f64-exact 2^53 range, which the source already rounded)";

/// Build one Arrow column of exactly `dt` from typed cells (one per row; `None` ⇒ null).
#[allow(clippy::redundant_closure_call)]
pub(crate) fn build_column(
    dt: &DataType,
    cells: &[Option<&RivetValue>],
) -> Result<ArrayRef, CellRefusal> {
    use RivetValue as V;

    let normalized: Vec<Option<&RivetValue>> = cells
        .iter()
        .map(|c| match c {
            Some(V::Null) => None,
            other => *other,
        })
        .collect();
    let cells: &[Option<&RivetValue>] = &normalized;

    macro_rules! typed_col {
        ($builder:expr, $value:expr) => {{
            let mut b = $builder;
            for (row, c) in cells.iter().enumerate() {
                match c {
                    None => b.append_null(),
                    Some(v) => match $value(*v) {
                        Ok(x) => b.append_value(x),
                        Err(reason) => return Err(CellRefusal::refused(row, v, reason.into())),
                    },
                }
            }
            b.finish()
        }};
    }

    let n = cells.len();
    Ok(match dt {
        DataType::Boolean => Arc::new(typed_col!(BooleanBuilder::with_capacity(n), bool_value)),
        DataType::Int8 => Arc::new(typed_col!(Int8Builder::with_capacity(n), int_value::<i8>)),
        DataType::Int16 => Arc::new(typed_col!(Int16Builder::with_capacity(n), int_value::<i16>)),
        DataType::Int32 => Arc::new(typed_col!(Int32Builder::with_capacity(n), int_value::<i32>)),
        DataType::Int64 => Arc::new(typed_col!(Int64Builder::with_capacity(n), int_value::<i64>)),
        DataType::UInt8 => Arc::new(typed_col!(UInt8Builder::with_capacity(n), int_value::<u8>)),
        DataType::UInt16 => Arc::new(typed_col!(
            UInt16Builder::with_capacity(n),
            int_value::<u16>
        )),
        DataType::UInt32 => Arc::new(typed_col!(
            UInt32Builder::with_capacity(n),
            int_value::<u32>
        )),
        DataType::UInt64 => Arc::new(typed_col!(
            UInt64Builder::with_capacity(n),
            int_value::<u64>
        )),
        DataType::Float32 => Arc::new(typed_col!(Float32Builder::with_capacity(n), f32_value)),
        DataType::Float64 => Arc::new(typed_col!(Float64Builder::with_capacity(n), f64_value)),
        DataType::Date32 => Arc::new(typed_col!(Date32Builder::with_capacity(n), date_value)),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let arr = typed_col!(
                TimestampMicrosecondBuilder::with_capacity(n),
                timestamp_micros_value
            );
            match tz {
                Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                None => Arc::new(arr),
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let arr = typed_col!(
                TimestampNanosecondBuilder::with_capacity(n),
                timestamp_nanos_value
            );
            match tz {
                Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                None => Arc::new(arr),
            }
        }
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(typed_col!(
            Time64MicrosecondBuilder::with_capacity(n),
            time_value
        )),
        DataType::Time64(TimeUnit::Nanosecond) => Arc::new(typed_col!(
            Time64NanosecondBuilder::with_capacity(n),
            time_nanos_value
        )),
        DataType::Decimal128(p, s) => Arc::new(typed_col!(
            Decimal128Builder::with_capacity(n).with_data_type(DataType::Decimal128(*p, *s)),
            |v: &RivetValue| decimal_to_i128(v, *s).ok_or_else(|| NOT_A_DECIMAL.to_string())
        )),
        DataType::Decimal256(p, s) => Arc::new(typed_col!(
            arrow::array::Decimal256Builder::with_capacity(n)
                .with_data_type(DataType::Decimal256(*p, *s)),
            |v: &RivetValue| decimal_to_i256(v, *s).ok_or_else(|| NOT_A_DECIMAL.to_string())
        )),
        DataType::List(field) => build_list_column(field, cells)?,
        DataType::Binary => Arc::new(typed_col!(
            BinaryBuilder::with_capacity(n, 0),
            |v: &RivetValue| match v {
                V::Bytes(by) => Ok(by.clone()),
                _ => Err(NO_READING.to_string()),
            }
        )),
        DataType::LargeBinary => Arc::new(typed_col!(
            LargeBinaryBuilder::with_capacity(n, 0),
            |v: &RivetValue| match v {
                V::Bytes(by) => Ok(by.clone()),
                _ => Err(NO_READING.to_string()),
            }
        )),
        DataType::FixedSizeBinary(w) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(n, *w);
            for (row, c) in cells.iter().enumerate() {
                let bytes = match c {
                    None => {
                        b.append_null();
                        continue;
                    }
                    Some(v @ V::Bytes(by)) => fixed_binary_bytes(by, *w as usize)
                        .ok_or_else(|| CellRefusal::new(row, v, NO_READING))?,
                    Some(v) => return Err(CellRefusal::new(row, v, NO_READING)),
                };
                b.append_value(&bytes)
                    .expect("fixed_binary_bytes returns exactly the column width");
            }
            Arc::new(b.finish())
        }
        DataType::Utf8 => Arc::new(typed_col!(StringBuilder::with_capacity(n, 0), text_value)),
        DataType::LargeUtf8 => Arc::new(typed_col!(
            LargeStringBuilder::with_capacity(n, 0),
            text_value
        )),
        other => unreachable!("build_column({other}): the sink asserts is_buildable first"),
    })
}

/// The text a Utf8 column stores for `v`; an array has no text reading here.
fn text_value(v: &RivetValue) -> Result<String, String> {
    match v {
        RivetValue::Array(_) => Err("is an array, which a text column cannot hold".into()),
        other => Ok(render_str(other)),
    }
}

/// The CDC side-A fold: an independent per-column checksum of the typed cells.
pub(crate) fn cells_checksum(dt: &DataType, cells: &[Option<&RivetValue>]) -> u64 {
    use RivetValue as V;
    use xxhash_rust::xxh3::xxh3_64;

    use crate::source::value_checksum::{encode_list_cell, is_covered};

    if !is_covered(dt) {
        return 0;
    }
    let mut acc: u64 = 0;
    for c in cells {
        let c = match c {
            Some(V::Null) | None => continue,
            Some(v) => *v,
        };
        macro_rules! le {
            ($opt:expr) => {
                if let Ok(v) = $opt {
                    acc = acc.wrapping_add(xxh3_64(&v.to_le_bytes()));
                }
            };
        }
        match dt {
            DataType::Boolean => {
                if let Ok(b) = bool_value(c) {
                    acc = acc.wrapping_add(xxh3_64(&[b as u8]));
                }
            }
            DataType::Int16 => le!(int_value::<i16>(c)),
            DataType::Int32 => le!(int_value::<i32>(c)),
            DataType::Int64 => le!(int_value::<i64>(c)),
            DataType::UInt64 => le!(int_value::<u64>(c)),
            DataType::Float32 => le!(f32_value(c)),
            DataType::Float64 => le!(f64_value(c)),
            DataType::Date32 => le!(date_value(c)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                if let V::DateTime(dt) = c {
                    acc = acc.wrapping_add(xxh3_64(&dt.and_utc().timestamp_micros().to_le_bytes()));
                }
            }
            DataType::Time64(TimeUnit::Microsecond) => le!(time_value(c)),
            DataType::Decimal128(_, s) => le!(decimal_to_i128(c, *s).ok_or(())),
            DataType::Decimal256(_, s) => le!(decimal_to_i256(c, *s).ok_or(())),
            DataType::Utf8 => {
                if let Ok(s) = text_value(c) {
                    acc = acc.wrapping_add(xxh3_64(s.as_bytes()));
                }
            }
            DataType::Binary => {
                if let V::Bytes(by) = c {
                    acc = acc.wrapping_add(xxh3_64(by));
                }
            }
            DataType::FixedSizeBinary(n) => {
                if let V::Bytes(by) = c
                    && let Some(bytes) = fixed_binary_bytes(by, *n as usize)
                {
                    acc = acc.wrapping_add(xxh3_64(&bytes));
                }
            }
            DataType::List(f) => {
                if let V::Array(elems) = c {
                    let encoded: Option<Vec<ListElem>> = elems
                        .iter()
                        .map(|e| list_elem(f.data_type(), e).ok())
                        .collect();
                    if let Some(encoded) = encoded {
                        acc = acc.wrapping_add(xxh3_64(&encode_list_cell(&encoded)));
                    }
                }
            }
            _ => {}
        }
    }
    acc
}

/// One list element as the checksum canon, read exactly as the list builder reads it.
fn list_elem(elem: &DataType, e: &RivetValue) -> Result<ListElem, String> {
    use RivetValue as V;
    Ok(match (elem, e) {
        (_, V::Null) => ListElem::Null,
        (DataType::Boolean, V::Bool(b)) => ListElem::Bool(*b),
        (DataType::Int16, V::Int(_)) => ListElem::I16(int_value(e)?),
        (DataType::Int32, V::Int(_)) => ListElem::I32(int_value(e)?),
        (DataType::Int64, V::Int(i)) => ListElem::I64(*i),
        (DataType::Float32, V::Float(x)) => ListElem::F32(*x as f32),
        (DataType::Float64, V::Float(x)) => ListElem::F64(*x),
        (DataType::Float64, V::Int(i)) => ListElem::F64(*i as f64),
        (DataType::Utf8, other) => ListElem::Str(text_value(other)?.into_bytes()),
        _ => return Err(NO_READING.into()),
    })
}

/// Why a non-array cell cannot fill a one-dimensional list column.
const NOT_ONE_DIMENSIONAL: &str = "is a multi-dimensional or non-representable array, which a \
                                   one-dimensional list column cannot hold; cast the column to \
                                   text in the source (e.g. col::text). The batch export fails \
                                   identically";

/// Build a `List<element>` column from [`RivetValue::Array`] cells, keeping the list field (batch parity).
fn build_list_column(
    field: &arrow::datatypes::FieldRef,
    cells: &[Option<&RivetValue>],
) -> Result<ArrayRef, CellRefusal> {
    use RivetValue as V;
    use arrow::array::ListBuilder;

    macro_rules! list_col {
        ($child:expr, $pat:pat => $val:expr) => {{
            let mut lb = ListBuilder::new($child).with_field(field.clone());
            for (row, c) in cells.iter().enumerate() {
                match c {
                    Some(v @ V::Array(elems)) => {
                        for e in elems {
                            match list_elem(field.data_type(), e) {
                                Ok(ListElem::Null) => lb.values().append_null(),
                                Ok($pat) => lb.values().append_value($val),
                                Ok(_) => {
                                    unreachable!("list_elem returns the element type's variant")
                                }
                                Err(reason) => {
                                    return Err(CellRefusal::new(
                                        row,
                                        v,
                                        format!("has an element {:?} that {reason}", render_str(e)),
                                    ));
                                }
                            }
                        }
                        lb.append(true);
                    }
                    None => lb.append(false),
                    Some(other) => return Err(CellRefusal::new(row, other, NOT_ONE_DIMENSIONAL)),
                }
            }
            Arc::new(lb.finish()) as ArrayRef
        }};
    }

    macro_rules! list_mapped {
        ($child:expr, $conv:expr, $b:ident, $x:ident => $push:expr) => {{
            let mut lb = ListBuilder::new($child).with_field(field.clone());
            for (row, c) in cells.iter().enumerate() {
                match c {
                    Some(v @ V::Array(elems)) => {
                        for e in elems {
                            if matches!(e, V::Null) {
                                lb.values().append_null();
                                continue;
                            }
                            match ($conv)(e) {
                                Ok($x) => {
                                    let $b = lb.values();
                                    $push
                                }
                                Err(reason) => {
                                    return Err(CellRefusal::new(
                                        row,
                                        v,
                                        format!("has an element {:?} that {reason}", render_str(e)),
                                    ));
                                }
                            }
                        }
                        lb.append(true);
                    }
                    None => lb.append(false),
                    Some(other) => return Err(CellRefusal::new(row, other, NOT_ONE_DIMENSIONAL)),
                }
            }
            Arc::new(lb.finish()) as ArrayRef
        }};
    }

    Ok(match field.data_type() {
        DataType::Date32 => {
            list_mapped!(Date32Builder::new(), date_value, b, x => b.append_value(x))
        }
        DataType::Time64(TimeUnit::Microsecond) => list_mapped!(
            Time64MicrosecondBuilder::new(),
            time_value,
            b,
            x => b.append_value(x)
        ),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => list_mapped!(
            TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone()),
            timestamp_micros_value,
            b,
            x => b.append_value(x)
        ),
        DataType::FixedSizeBinary(16) => list_mapped!(
            FixedSizeBinaryBuilder::new(16),
            |e: &RivetValue| match e {
                V::Bytes(by) => fixed_binary_bytes(by, 16).ok_or_else(|| NO_READING.to_string()),
                _ => Err(NO_READING.to_string()),
            },
            b,
            x => b.append_value(&x).expect("fixed_binary_bytes returns exactly 16 bytes")
        ),
        DataType::Binary => list_mapped!(
            BinaryBuilder::new(),
            |e: &RivetValue| match e {
                V::Bytes(by) => Ok(by.clone()),
                _ => Err(NO_READING.to_string()),
            },
            b,
            x => b.append_value(x)
        ),
        DataType::Boolean => list_col!(BooleanBuilder::new(), ListElem::Bool(x) => x),
        DataType::Int16 => list_col!(Int16Builder::new(), ListElem::I16(x) => x),
        DataType::Int32 => list_col!(Int32Builder::new(), ListElem::I32(x) => x),
        DataType::Int64 => list_col!(Int64Builder::new(), ListElem::I64(x) => x),
        DataType::Float32 => list_col!(Float32Builder::new(), ListElem::F32(x) => x),
        DataType::Float64 => list_col!(Float64Builder::new(), ListElem::F64(x) => x),
        DataType::Utf8 => list_col!(
            StringBuilder::new(),
            ListElem::Str(x) => String::from_utf8(x).expect("text_value is UTF-8")
        ),
        other => unreachable!("list of {other}: the sink asserts is_buildable first"),
    })
}

/// The scaled-integer magnitude past which an f64 can no longer hold a fixed-point value exactly (2^53).
const F64_EXACT_SCALED_LIMIT: f64 = 9_007_199_254_740_992.0;

/// A finite fixed-point f64 rendered at `scale` decimal places, or `None` when its scaled magnitude is past the f64-exact range (already rounded → refuse, so `build_column` fails loud instead of storing a falsely-exact decimal).
fn f64_fixed_point_str(f: f64, scale: i8) -> Option<String> {
    let prec = scale.max(0) as usize;
    if f.abs() * 10f64.powi(prec as i32) >= F64_EXACT_SCALED_LIMIT {
        return None;
    }
    Some(format!("{f:.prec$}"))
}

/// As [`decimal_to_i128`] but into `i256` for `Decimal256` columns.
fn decimal_to_i256(v: &RivetValue, scale: i8) -> Option<arrow::datatypes::i256> {
    let s = match v {
        RivetValue::Bytes(b) => std::str::from_utf8(b).ok()?.trim().to_string(),
        RivetValue::Int(i) => i.to_string(),
        RivetValue::UInt(u) => u.to_string(),
        RivetValue::Float(f) if f.is_finite() => f64_fixed_point_str(*f, scale)?,
        _ => return None,
    };
    crate::types::decimal::decimal_str_to_scaled_i256(&s, scale)
}

/// Parse a decimal carried as source bytes (e.g.
fn decimal_to_i128(v: &RivetValue, scale: i8) -> Option<i128> {
    let s = match v {
        RivetValue::Bytes(b) => std::str::from_utf8(b).ok()?.trim().to_string(),
        RivetValue::Int(i) => i.to_string(),
        RivetValue::UInt(u) => u.to_string(),
        RivetValue::Float(f) if f.is_finite() => f64_fixed_point_str(*f, scale)?,
        _ => return None,
    };
    crate::types::decimal::decimal_str_to_scaled_i128(&s, scale)
}

/// MySQL binlog cell quirks, keyed by the column's NATIVE type (the binlog row image is type-blind at decode time).
#[derive(Debug)]
pub(crate) enum MysqlCellFix {
    /// TIMESTAMP arrives as `"epoch[.micros]"` TEXT — parse to the UTC instant.
    TimestampEpoch,
    /// BIT(1) arrives as one raw byte — any set bit ⇒ true.
    BitBool,
    /// BIT(n>1) arrives as big-endian raw bytes — widen to u64.
    BitUint,
    /// YEAR arrives as its text rendering ("2024").
    YearText,
    /// Signed MEDIUMINT arrives WITHOUT 24-bit sign extension (the binlog stores 3 bytes.
    MediumIntSign,
    /// ENUM arrives as its 1-based index; mapped to the label the declaration holds, 0 is the empty string.
    EnumLabels(Labels),
    /// SET arrives as its little-endian bitmask; rendered as the members comma-joined in declaration order.
    SetLabels(Labels),
    /// BINARY(n): the driver right-trims trailing NULs.
    BinaryPad(usize),
    /// DATE and DATETIME arrive as the zero-date marker when MySQL holds a zero or partial date; written as NULL.
    ZeroDate,
    /// TIME is a duration up to 838:59:59; one outside a day is refused with the batch's MySQL wording.
    TimeOfDay,
}

/// The fix (if any) for a column, from the engine + native type.
pub(crate) fn mysql_cell_fix(
    engine: crate::source::cdc::CdcEngine,
    native: &str,
) -> Option<MysqlCellFix> {
    if engine != crate::source::cdc::CdcEngine::Mysql {
        return None;
    }
    let n = native.to_ascii_lowercase();
    if n.starts_with("timestamp") {
        return Some(MysqlCellFix::TimestampEpoch);
    }
    if n == "date" || n.starts_with("datetime") {
        return Some(MysqlCellFix::ZeroDate);
    }
    if n == "bit(1)" {
        return Some(MysqlCellFix::BitBool);
    }
    if n.starts_with("bit(") || n == "bit" {
        return Some(MysqlCellFix::BitUint);
    }
    if n == "year" || n.starts_with("year(") {
        return Some(MysqlCellFix::YearText);
    }
    if (n == "mediumint" || n.starts_with("mediumint(")) && !n.contains("unsigned") {
        return Some(MysqlCellFix::MediumIntSign);
    }
    if n == "time" || n.starts_with("time(") {
        return Some(MysqlCellFix::TimeOfDay);
    }
    if n.starts_with("enum(") {
        return Some(MysqlCellFix::EnumLabels(parse_enum_labels(native)));
    }
    if n.starts_with("set(") {
        return Some(MysqlCellFix::SetLabels(parse_enum_labels(native)));
    }
    if let Some(width) = n
        .strip_prefix("binary(")
        .and_then(|r| r.strip_suffix(')'))
        .and_then(|w| w.parse::<usize>().ok())
    {
        return Some(MysqlCellFix::BinaryPad(width));
    }
    None
}

/// The labels of an ENUM or SET declaration, or why one is unreadable.
type Labels = Result<Vec<String>, String>;

/// Labels from `enum('a','b','it''s')` as `append_unescaped` writes them, in declaration order.
fn parse_enum_labels(native: &str) -> Labels {
    let Some(start) = native.find('(') else {
        return Ok(Vec::new());
    };
    let end = native.rfind(')').unwrap_or(native.len());
    let Some(inner) = native.get(start..end) else {
        return Ok(Vec::new());
    };
    let mut labels = Vec::new();
    let mut chars = inner.chars().peekable();
    let mut prev = ',';
    while let Some(c) = chars.next() {
        if c != '\'' {
            prev = c;
            continue;
        }
        let hex = prev == 'x';
        let mut label = String::new();
        loop {
            match chars.next() {
                Some('\'') if chars.peek() == Some(&'\'') => {
                    chars.next();
                    label.push('\'');
                }
                Some('\'') | None => break,
                Some('\\') if seeded!("mysql-enum-label-escapes-unread", false, true) => {
                    match chars.peek() {
                        Some('\\') => {
                            chars.next();
                            label.push('\\');
                        }
                        Some('n') => {
                            chars.next();
                            label.push('\n');
                        }
                        Some('r') => {
                            chars.next();
                            label.push('\r');
                        }
                        Some('0') => {
                            chars.next();
                            label.push('\0');
                        }
                        _ => label.push('\\'),
                    }
                }
                Some(other) => label.push(other),
            }
        }
        if hex {
            return Err(hex_refusal(labels.len() + 1, &label));
        }
        labels.push(label);
    }
    Ok(labels)
}

/// Why label `n`, written by MySQL as the hex literal `x'digits'`, has no reading.
fn hex_refusal(n: usize, digits: &str) -> String {
    format!(
        "cannot be read: the column declares label {n} as the hex literal x'{digits}', which MySQL writes without zero padding, so the label's bytes cannot be recovered from the catalog"
    )
}

impl MysqlCellFix {
    /// The value the typed column needs for wire value `v`, or why the wire value has no reading.
    pub(crate) fn applies_to(&self, dt: &DataType) -> bool {
        !matches!(self, MysqlCellFix::TimeOfDay)
            || matches!(dt, DataType::Time64(TimeUnit::Microsecond))
    }

    /// Whether the fix wrote NULL over a held `input` date: the zero or partial date a MySQL column cannot hand on.
    pub(crate) fn nulled_a_date(&self, input: &RivetValue, out: &RivetValue) -> bool {
        seeded!(
            "mysql-zero-date-count-misses-a-class",
            matches!(self, MysqlCellFix::TimestampEpoch),
            matches!(self, MysqlCellFix::ZeroDate | MysqlCellFix::TimestampEpoch)
        ) && matches!(out, RivetValue::Null)
            && !matches!(input, RivetValue::Null)
    }

    /// ENUM and SET cells against `labels`, or the refusal for a declaration that has none; every other cell passes through.
    fn apply_labels(
        &self,
        labels: std::result::Result<&[String], &str>,
        v: &RivetValue,
    ) -> Result<RivetValue, FixRefusal> {
        use RivetValue as V;
        Ok(match (self, v) {
            (MysqlCellFix::EnumLabels(_), V::Int(i)) => {
                enum_label(labels.map_err(label_refusal)?, *i)?
            }
            (MysqlCellFix::EnumLabels(_), V::UInt(u)) => enum_label(
                labels.map_err(label_refusal)?,
                i64::try_from(*u).unwrap_or(i64::MAX),
            )?,
            (MysqlCellFix::SetLabels(_), V::Bytes(b)) if b.len() <= 8 => {
                let mut word = [0u8; 8];
                word.iter_mut().zip(b).for_each(|(d, s)| *d = *s);
                set_labels(labels.map_err(label_refusal)?, u64::from_le_bytes(word))
            }
            (MysqlCellFix::SetLabels(_), V::Int(i)) => {
                set_labels(labels.map_err(label_refusal)?, *i as u64)
            }
            (MysqlCellFix::SetLabels(_), V::UInt(u)) => {
                set_labels(labels.map_err(label_refusal)?, *u)
            }
            (_, other) => other.clone(),
        })
    }

    /// [`Self::apply`] with the labels `wire` carries from the event's binlog table map: listed labels replace the catalog's, an unlisted column refuses a label the catalog spelled with `?`.
    pub(crate) fn apply_with(
        &self,
        v: &RivetValue,
        wire: Option<&WireLabels>,
        table: &str,
    ) -> Result<RivetValue, FixRefusal> {
        let (Some(wire), MysqlCellFix::EnumLabels(_) | MysqlCellFix::SetLabels(_)) = (wire, self)
        else {
            return self.apply(v);
        };
        if seeded!("mysql-enum-4byte-label-wire-unread", true, false) {
            return self.apply(v);
        }
        let Some(listed) = &wire.labels else {
            let out = self.apply(v)?;
            return match &out {
                RivetValue::Bytes(b)
                    if b.contains(&b'?')
                        && seeded!("mysql-enum-unlisted-label-delivered", false, true) =>
                {
                    Err(unlisted_refusal(table, &wire.column))
                }
                _ => Ok(out),
            };
        };
        self.apply_labels(Ok(listed), v)
    }

    pub(crate) fn apply(&self, v: &RivetValue) -> Result<RivetValue, FixRefusal> {
        use RivetValue as V;
        Ok(match (self, v) {
            (_, V::Null) => V::Null,
            (MysqlCellFix::TimestampEpoch, V::Bytes(b)) => match epoch_text(b) {
                Some((0, 0)) => V::Null,
                Some((secs, micros)) => chrono::DateTime::from_timestamp(secs, micros * 1_000)
                    .map(|dt| V::DateTime(dt.naive_utc()))
                    .ok_or("is past the range of a timestamp")?,
                None => return Err("is not a binlog TIMESTAMP (epoch seconds text)".into()),
            },
            (MysqlCellFix::ZeroDate, V::Bytes(b)) if b == ZERO_DATE => V::Null,
            (MysqlCellFix::BitBool, V::Bytes(b)) => V::Bool(b.iter().any(|x| *x != 0)),
            (MysqlCellFix::BitBool, V::Int(i)) => V::Bool(*i != 0),
            (MysqlCellFix::BitBool, V::UInt(u)) => V::Bool(*u != 0),
            (MysqlCellFix::BitUint, V::Bytes(b)) if b.len() <= 8 => {
                V::UInt(b.iter().fold(0u64, |acc, x| (acc << 8) | *x as u64))
            }
            (MysqlCellFix::YearText, V::Bytes(b)) => {
                let y = std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .ok_or("is not a YEAR")?;
                V::Int(if y == 1900 { 0 } else { y })
            }
            (MysqlCellFix::MediumIntSign, V::Int(i)) if *i >= (1 << 23) => V::Int(*i - (1 << 24)),
            (MysqlCellFix::MediumIntSign, V::UInt(u)) => {
                let i = *u as i64;
                V::Int(if i >= (1 << 23) { i - (1 << 24) } else { i })
            }
            (MysqlCellFix::EnumLabels(labels) | MysqlCellFix::SetLabels(labels), _) => {
                self.apply_labels(labels.as_deref().map_err(String::as_str), v)?
            }
            (MysqlCellFix::TimeOfDay, V::TimeMicros(us)) if !crate::types::is_time_of_day(*us) => {
                return Err(FixRefusal {
                    reason: seeded!(
                        "mysql-cdc-time-remedy-names-the-batch-cast",
                        format!(
                            "{} Cast it in a query, e.g. TIME_TO_SEC(col) or CAST(col AS CHAR)",
                            crate::source::mysql::time_outside_day(*us)
                        ),
                        format!(
                            "{} A stream has no query to cast it in: declare the column `string` in \
                             `columns:` to carry it as text, or leave the column out of the capture, \
                             then: {}",
                            crate::source::mysql::time_outside_day(*us),
                            crate::source::cdc::checkpoint_identity::RECOVER
                        )
                    ),
                    code: Some(crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE),
                });
            }
            (MysqlCellFix::BinaryPad(w), V::Bytes(b)) => {
                let mut p = b.clone();
                p.resize(p.len().max(*w), 0);
                V::Bytes(p)
            }
            (_, other) => other.clone(),
        })
    }
}

/// `"secs[.frac]"` binlog TIMESTAMP text as (seconds, microseconds).
fn epoch_text(b: &[u8]) -> Option<(i64, u32)> {
    let s = std::str::from_utf8(b).ok()?;
    let (sec, frac) = s.split_once('.').unwrap_or((s, ""));
    let micros = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<6}").get(..6)?.parse().ok()?
    };
    Some((sec.parse().ok()?, micros))
}

/// The cell refusal of an ENUM or SET label that has no reading, coded and with the remedy no `columns:` override can replace.
fn label_refusal(why: impl Into<String>) -> FixRefusal {
    FixRefusal {
        reason: format!(
            "{}. A `columns:` override does not carry the label either: leave the column out of the capture, then: {}",
            why.into(),
            crate::source::cdc::checkpoint_identity::RECOVER
        ),
        code: Some(crate::error::codes::SOURCE_CDC_CELL_UNSUPPORTED),
    }
}

/// The labels a binlog TABLE_MAP carries for one ENUM or SET column whose catalog rendering cannot be trusted; `None` when the table map carries no labels.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WireLabels {
    pub(crate) column: String,
    pub(crate) labels: Option<Vec<String>>,
}

/// The refusal of a label the catalog spells with `?` when the table map carries no labels to read it from.
fn unlisted_refusal(table: &str, column: &str) -> FixRefusal {
    label_refusal(format!(
        "cannot be read: the catalog prints a character outside the Basic Multilingual Plane as '?', so a label of `{table}`.`{column}` holding '?' cannot be told from the stored label, and this binlog's table map carries no ENUM or SET labels to read it from. The table map carries them only under `binlog_row_metadata = FULL` (`SET PERSIST binlog_row_metadata = FULL;`, MySQL 8.0.1 or later)"
    ))
}

fn set_labels(labels: &[String], mask: u64) -> RivetValue {
    let joined = labels
        .iter()
        .enumerate()
        .filter(|(i, _)| mask & (1 << i) != 0)
        .map(|(_, l)| l.as_str())
        .collect::<Vec<_>>()
        .join(",");
    RivetValue::Bytes(joined.into_bytes())
}

/// The label of 1-based ENUM index `idx`; 0 is MySQL's invalid-value sentinel `''`, past the list is refused.
fn enum_label(labels: &[String], idx: i64) -> Result<RivetValue, FixRefusal> {
    if idx <= 0 {
        return Ok(RivetValue::Bytes(Vec::new()));
    }
    labels
        .get(idx as usize - 1)
        .map(|l| RivetValue::Bytes(l.clone().into_bytes()))
        .ok_or_else(|| label_refusal(format!("is past the column's {} ENUM labels", labels.len())))
}

/// Render `Bytes` to a string LOSSLESSLY: verbatim when the bytes are valid UTF-8 (the common case.
fn bytes_to_recoverable_string(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) => s.to_string(),
        Err(_) => {
            use std::fmt::Write as _;
            let mut hex = String::with_capacity(2 + b.len() * 2);
            hex.push_str("\\x");
            for byte in b {
                let _ = write!(hex, "{byte:02x}");
            }
            hex
        }
    }
}

fn render_str(v: &RivetValue) -> String {
    match v {
        RivetValue::Null => String::new(),
        RivetValue::Bool(b) => b.to_string(),
        RivetValue::Int(i) => i.to_string(),
        RivetValue::UInt(u) => u.to_string(),
        RivetValue::Float(f) => f.to_string(),
        RivetValue::DateTime(dt) => dt.to_string(),
        RivetValue::TimeMicros(us) => crate::types::time_beyond_day(*us),
        RivetValue::TimeNanos(ns) => time_of_day_nanos(*ns),
        RivetValue::Bytes(b) => bytes_to_recoverable_string(b),
        RivetValue::Array(v) => {
            let inner: Vec<String> = v.iter().map(render_str).collect();
            format!("[{}]", inner.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A SQL Server time(7) arrives as nanoseconds: a Time64(ns) column keeps the 100ns tick, a microsecond column takes a value with no such digit and refuses one that has it, never truncating.
    #[test]
    fn a_nanosecond_time_keeps_its_tick_and_never_truncates_into_microseconds() {
        use arrow::array::{Array, Time64MicrosecondArray, Time64NanosecondArray};
        let tick = RivetValue::TimeNanos(86_399_999_999_900);
        let ns = build_column(
            &DataType::Time64(TimeUnit::Nanosecond),
            &[Some(&tick), None],
        )
        .unwrap();
        let ns = ns.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
        assert_eq!((ns.value(0), ns.is_null(1)), (86_399_999_999_900, true));
        let whole = RivetValue::TimeNanos(3_723_456_789_000);
        let us = build_column(&DataType::Time64(TimeUnit::Microsecond), &[Some(&whole)]).unwrap();
        let us = us
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(us.value(0), 3_723_456_789);
        assert!(build_column(&DataType::Time64(TimeUnit::Microsecond), &[Some(&tick)]).is_err());
        assert!(is_buildable(&DataType::Time64(TimeUnit::Nanosecond)));
        assert_eq!(render_str(&tick), "23:59:59.999999900");
    }

    /// The two time readers take each time value at its own unit and refuse a duration outside one day, a 100ns digit into microseconds, and a value that is no time.
    #[test]
    fn the_time_readers_take_each_unit_and_refuse_what_does_not_fit() {
        let day_us = crate::types::MICROS_PER_DAY;
        assert_eq!(time_value(&RivetValue::TimeMicros(5)), Ok(5));
        assert_eq!(time_value(&RivetValue::TimeNanos(5_000)), Ok(5));
        assert!(
            time_value(&RivetValue::TimeNanos(5_100))
                .unwrap_err()
                .contains("100ns digit")
        );
        assert!(
            time_value(&RivetValue::TimeNanos(day_us * 1_000))
                .unwrap_err()
                .contains("outside 00:00..24:00")
        );
        assert!(
            time_value(&RivetValue::TimeMicros(-1))
                .unwrap_err()
                .contains("outside 00:00..24:00")
        );
        assert_eq!(time_value(&RivetValue::Int(1)), Err(NO_READING.into()));

        assert_eq!(time_nanos_value(&RivetValue::TimeNanos(0)), Ok(0));
        let last = day_us * 1_000 - 1;
        assert_eq!(time_nanos_value(&RivetValue::TimeNanos(last)), Ok(last));
        for bad in [-1, day_us * 1_000] {
            assert!(
                time_nanos_value(&RivetValue::TimeNanos(bad))
                    .unwrap_err()
                    .contains("outside 00:00..24:00"),
                "{bad}"
            );
        }
        assert_eq!(
            time_nanos_value(&RivetValue::TimeMicros(1_000)),
            Ok(1_000_000)
        );
        assert!(
            time_nanos_value(&RivetValue::TimeMicros(day_us))
                .unwrap_err()
                .contains("outside 00:00..24:00")
        );
        assert_eq!(
            time_nanos_value(&RivetValue::Int(1)),
            Err(NO_READING.into())
        );
    }

    /// `build_column` with the sink's refusal wording for a non-overridden column `col`.
    fn build(col: &str, dt: &DataType, cells: &[Option<&RivetValue>]) -> anyhow::Result<ArrayRef> {
        build_column(dt, cells).map_err(|r| r.into_error(col, dt, false))
    }

    #[test]
    fn to_json_bytes_are_lossless_utf8_verbatim_binary_hex() {
        assert_eq!(
            RivetValue::Bytes(b"hello".to_vec()).to_json(),
            Json::String("hello".to_string())
        );
        let raw = vec![0xDEu8, 0xAD, 0xBE, 0xEF, 0x00, 0xFF];
        let j = RivetValue::Bytes(raw).to_json();
        assert_eq!(j, Json::String("\\xdeadbeef00ff".to_string()));
        if let Json::String(s) = &j {
            assert!(
                !s.contains('\u{FFFD}'),
                "binary must not be lossy-replaced: {s}"
            );
        }
    }

    #[test]
    fn render_str_bytes_are_lossless_like_to_json() {
        assert_eq!(
            render_str(&RivetValue::Bytes(b"caf\xc3\xa9".to_vec())),
            "café"
        );
        let latin1 = vec![b'c', b'a', b'f', 0xE9];
        let s = render_str(&RivetValue::Bytes(latin1));
        assert_eq!(s, "\\x636166e9", "non-UTF-8 must render as recoverable hex");
        assert!(!s.contains('\u{FFFD}'), "must not be lossy-replaced: {s}");
    }

    #[test]
    fn enum_labels_unicode_and_escapes_parse_exactly() {
        assert_eq!(
            parse_enum_labels(
                "enum('\u{43f}\u{440}\u{438}\u{432}\u{435}\u{442}','\u{43c}\u{438}\u{440}')"
            )
            .unwrap(),
            vec![
                "\u{43f}\u{440}\u{438}\u{432}\u{435}\u{442}".to_string(),
                "\u{43c}\u{438}\u{440}".to_string()
            ]
        );
        assert_eq!(
            parse_enum_labels("enum('it''s','a')").unwrap(),
            vec!["it's".to_string(), "a".to_string()]
        );
        assert_eq!(parse_enum_labels("enum(").unwrap(), Vec::<String>::new());
        assert_eq!(
            parse_enum_labels("enum('ok'").unwrap(),
            vec!["ok".to_string()]
        );
        assert_eq!(parse_enum_labels("garbage").unwrap(), Vec::<String>::new());
    }

    /// The text `native`'s ENUM label `idx` (1-based) is written as, or the refusal.
    fn enum_text(native: &str, idx: i64) -> Result<String, FixRefusal> {
        let fix = mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native);
        match fix.apply(&RivetValue::Int(idx))? {
            RivetValue::Bytes(b) => Ok(String::from_utf8(b).unwrap()),
            other => panic!("{other:?}"),
        }
    }

    /// The text `native`'s SET members in `mask` are written as.
    fn set_text(native: &str, mask: u64) -> String {
        let fix = mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native);
        match fix.apply(&RivetValue::UInt(mask)).unwrap() {
            RivetValue::Bytes(b) => String::from_utf8(b).unwrap(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn every_append_unescaped_escape_is_reversed_in_an_enum_label() {
        let native = r"enum('C:\\temp','a\nb','c\rd','n\0z','it''s','lit\\n','e\\','\\\\','\\''x')";
        let want = [
            "C:\\temp", "a\nb", "c\rd", "n\0z", "it's", "lit\\n", "e\\", "\\\\", "\\'x",
        ];
        for (i, w) in want.iter().enumerate() {
            assert_eq!(
                enum_text(native, i as i64 + 1).unwrap(),
                *w,
                "label {}",
                i + 1
            );
        }
    }

    #[test]
    fn every_append_unescaped_escape_is_reversed_in_a_set_member() {
        let native = r"set('p\\q','r\ns','t\ru','v\0w','y''z')";
        assert_eq!(set_text(native, 0b00001), "p\\q");
        assert_eq!(set_text(native, 0b00110), "r\ns,t\ru");
        assert_eq!(set_text(native, 0b11000), "v\0w,y'z");
        assert_eq!(set_text(native, 0b11111), "p\\q,r\ns,t\ru,v\0w,y'z");
    }

    #[test]
    fn labels_without_an_escape_and_malformed_lists_read_as_before() {
        let native =
            "enum('a','b,c','(d)','','\u{43f}\u{440}\u{438}\u{432}\u{435}\u{442}','it''s')";
        let want = [
            "a",
            "b,c",
            "(d)",
            "",
            "\u{43f}\u{440}\u{438}\u{432}\u{435}\u{442}",
            "it's",
        ];
        for (i, w) in want.iter().enumerate() {
            assert_eq!(
                enum_text(native, i as i64 + 1).unwrap(),
                *w,
                "label {}",
                i + 1
            );
        }
        assert_eq!(enum_text("enum('ok'", 1).unwrap(), "ok");
        assert!(enum_text("enum(", 1).is_err());
        assert_eq!(enum_text(r"enum('a\qb','c\')", 1).unwrap(), r"a\qb");
        assert_eq!(enum_text(r"enum('a\qb','c\')", 2).unwrap(), "c\\");
        assert_eq!(enum_text("enum('a','b')", 0).unwrap(), "");
        assert!(mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, "garbage").is_none());
    }

    #[test]
    fn a_set_of_more_than_eight_members_reads_its_second_byte() {
        let native = "set('m1','m2','m3','m4','m5','m6','m7','m8','m9','m10')";
        let fix = mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native);
        match fix.apply(&RivetValue::Bytes(vec![0x01, 0x02])).unwrap() {
            RivetValue::Bytes(b) => assert_eq!(String::from_utf8(b).unwrap(), "m1,m10"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_set_value_of_more_than_eight_bytes_is_not_truncated_into_a_mask() {
        let native = "set('m1','m2','m3','m4','m5','m6','m7','m8','m9','m10')";
        let fix = mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native);
        let nine = vec![0x01, 0, 0, 0, 0, 0, 0, 0, 0xff];
        match fix.apply(&RivetValue::Bytes(nine.clone())).unwrap() {
            RivetValue::Bytes(b) => assert_eq!(b, nine),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn every_hex_label_is_refused_because_its_digits_are_unpadded() {
        for (native, idx, n, digits) in [
            ("enum(x'12f09f9a80','a')", 1, 1, "12f09f9a80"),
            ("enum(x'f09f9a80a','a')", 1, 1, "f09f9a80a"),
            ("enum('a',x'5ff')", 1, 2, "5ff"),
            ("enum('a',x'e9')", 2, 2, "e9"),
            ("enum('a','b',x'ff')", 1, 3, "ff"),
            ("set(x'1f0','b')", 1, 1, "1f0"),
        ] {
            let fix = mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native);
            let err = fix.apply(&RivetValue::Int(idx)).unwrap_err();
            let want = format!(
                "cannot be read: the column declares label {n} as the hex literal x'{digits}', which MySQL writes without zero padding, so the label's bytes cannot be recovered from the catalog"
            );
            assert!(
                err.reason
                    .starts_with(&format!("{want}. A `columns:` override")),
                "{native}: {}",
                err.reason
            );
            assert_eq!(
                err.code.map(|c| c.id),
                Some("RIVET_SOURCE_CDC_CELL_UNSUPPORTED"),
                "{native}"
            );
        }
    }

    #[test]
    fn an_enum_label_refusal_is_the_cell_refusal_with_or_without_a_columns_override() {
        for (native, idx, cause) in [
            ("enum(x'12f09f9a80','a')", 2, "without zero padding"),
            ("enum('a','b')", 3, "past the column's 2 ENUM labels"),
        ] {
            let err = enum_text(native, idx).unwrap_err();
            for overridden in [false, true] {
                let e = CellRefusal {
                    row: 3,
                    value: RivetValue::Int(idx),
                    reason: err.reason.clone(),
                    code: err.code,
                }
                .into_error("status", &DataType::Utf8, overridden);
                let msg = format!("{e:#}");
                assert_eq!(
                    crate::error::code_of(&e).map(|c| c.id),
                    Some("RIVET_SOURCE_CDC_CELL_UNSUPPORTED"),
                    "{native} overridden={overridden}: {msg}"
                );
                assert!(
                    msg.contains("column 'status'")
                        && msg.contains(cause)
                        && msg.contains("leave the column out of the capture, then: Re-baseline")
                        && !msg.contains("correct or drop"),
                    "{native} overridden={overridden}: {msg}"
                );
            }
        }
    }

    fn wire(column: &str, labels: Option<&[&str]>) -> WireLabels {
        WireLabels {
            column: column.into(),
            labels: labels.map(|l| l.iter().map(|s| s.to_string()).collect()),
        }
    }

    fn fix_of(native: &str) -> MysqlCellFix {
        mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native)
    }

    fn text(v: RivetValue) -> String {
        match v {
            RivetValue::Bytes(b) => String::from_utf8(b).unwrap(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn listed_wire_labels_replace_the_catalog_labels_of_an_enum_and_a_set() {
        let grin = "\u{1F600}";
        let e = fix_of("enum('a','?','b')");
        let w = wire("status", Some(&["a", grin, "b"]));
        for (idx, want) in [(0, ""), (1, "a"), (2, grin), (3, "b")] {
            let got = e.apply_with(&RivetValue::Int(idx), Some(&w), "orders");
            assert_eq!(text(got.unwrap()), want, "enum index {idx}");
        }
        assert_eq!(
            text(
                e.apply_with(&RivetValue::UInt(2), Some(&w), "orders")
                    .unwrap()
            ),
            grin
        );
        let s = fix_of("set('x','?','z')");
        let w = wire("tags", Some(&["x", grin, "z"]));
        for (mask, want) in [(0, ""), (2, grin), (3, "x,\u{1F600}"), (7, "x,\u{1F600},z")] {
            let got = s.apply_with(&RivetValue::Bytes(vec![mask]), Some(&w), "orders");
            assert_eq!(text(got.unwrap()), want, "set mask {mask}");
            let got = s.apply_with(&RivetValue::UInt(u64::from(mask)), Some(&w), "orders");
            assert_eq!(text(got.unwrap()), want, "set uint mask {mask}");
        }
    }

    #[test]
    fn listed_wire_labels_are_the_labels_even_when_the_catalog_declares_another_count() {
        let e = fix_of("enum('a','?')");
        let w = wire("status", Some(&["a", "\u{1F600}", "c"]));
        let got = e.apply_with(&RivetValue::Int(3), Some(&w), "orders");
        assert_eq!(text(got.unwrap()), "c");
        let past = e.apply_with(&RivetValue::Int(4), Some(&w), "orders");
        assert!(
            past.unwrap_err()
                .reason
                .contains("is past the column's 3 ENUM labels")
        );
    }

    #[test]
    fn an_unlisted_column_refuses_a_label_the_catalog_spelled_with_a_question_mark() {
        let e = fix_of("enum('a','?','b')");
        let w = wire("status", None);
        for idx in [0, 1, 3] {
            let got = e.apply_with(&RivetValue::Int(idx), Some(&w), "orders");
            assert_eq!(got, e.apply(&RivetValue::Int(idx)), "enum index {idx}");
        }
        let err = e
            .apply_with(&RivetValue::Int(2), Some(&w), "orders")
            .unwrap_err();
        assert_eq!(
            err.code.map(|c| c.id),
            Some("RIVET_SOURCE_CDC_CELL_UNSUPPORTED")
        );
        let want = "cannot be read: the catalog prints a character outside the Basic Multilingual Plane as '?', so a label of `orders`.`status` holding '?' cannot be told from the stored label, and this binlog's table map carries no ENUM or SET labels to read it from. The table map carries them only under `binlog_row_metadata = FULL` (`SET PERSIST binlog_row_metadata = FULL;`, MySQL 8.0.1 or later). A `columns:` override does not carry the label either: leave the column out of the capture, then: ";
        assert!(err.reason.starts_with(want), "{}", err.reason);
        let s = fix_of("set('a','?','c')");
        let w = wire("tags", None);
        for mask in [0u64, 1, 4, 5] {
            let got = s.apply_with(&RivetValue::UInt(mask), Some(&w), "orders");
            assert_eq!(got, s.apply(&RivetValue::UInt(mask)), "set mask {mask}");
        }
        for mask in [2u64, 3, 7] {
            let err = s
                .apply_with(&RivetValue::UInt(mask), Some(&w), "orders")
                .unwrap_err();
            assert!(err.reason.contains("`orders`.`tags`"), "mask {mask}");
        }
    }

    #[test]
    fn the_unlisted_refusal_is_the_coded_cell_refusal_naming_table_and_column() {
        let e = fix_of("enum('?')");
        let err = e
            .apply_with(&RivetValue::Int(1), Some(&wire("status", None)), "orders")
            .unwrap_err();
        let msg = format!(
            "{:#}",
            CellRefusal {
                row: 0,
                value: RivetValue::Int(1),
                reason: err.reason,
                code: err.code,
            }
            .into_error("status", &DataType::Utf8, false)
        );
        assert!(
            msg.contains("column 'status'")
                && msg.contains("`orders`.`status`")
                && msg.contains("binlog_row_metadata = FULL"),
            "{msg}"
        );
    }

    #[test]
    fn a_column_without_wire_labels_reads_as_main_did() {
        let cases = [
            ("enum('a','?','b')", RivetValue::Int(2)),
            ("enum('a','b')", RivetValue::Int(9)),
            ("enum(x'12f09f9a80','a')", RivetValue::Int(1)),
            ("set('a','?','c')", RivetValue::UInt(3)),
            ("set('a','b')", RivetValue::Bytes(vec![1])),
            ("timestamp", RivetValue::Bytes(b"1".to_vec())),
            ("bit(1)", RivetValue::Bytes(vec![1])),
        ];
        for (native, v) in cases {
            let f = fix_of(native);
            assert_eq!(f.apply_with(&v, None, "orders"), f.apply(&v), "{native}");
        }
        let w = wire("c", None);
        for (native, v) in [
            ("timestamp", RivetValue::Bytes(b"1".to_vec())),
            ("bit(1)", RivetValue::Bytes(vec![1])),
        ] {
            let f = fix_of(native);
            assert_eq!(
                f.apply_with(&v, Some(&w), "orders"),
                f.apply(&v),
                "{native}"
            );
        }
        let e = fix_of("enum('a','?')");
        assert_eq!(
            e.apply_with(&RivetValue::Null, Some(&wire("c", None)), "orders"),
            Ok(RivetValue::Null)
        );
    }

    #[test]
    fn labels_that_start_or_end_with_x_are_not_hex_literals() {
        let native = "enum('x','x''','xx','ax')";
        for (i, want) in ["x", "x'", "xx", "ax"].iter().enumerate() {
            assert_eq!(enum_text(native, i as i64 + 1).unwrap(), *want, "{i}");
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 256, ..Default::default()
        })]

        #[test]
        fn cell_fixes_are_total_over_arbitrary_wire_values(
            native in "[a-z0-9_() ',]{0,40}",
            bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
            ival in proptest::prelude::any::<i64>(),
            uval in proptest::prelude::any::<u64>(),
        ) {
            use crate::source::cdc::CdcEngine;
            if let Some(fix) = mysql_cell_fix(CdcEngine::Mysql, &native) {
                for v in [
                    RivetValue::Bytes(bytes.clone()),
                    RivetValue::Int(ival),
                    RivetValue::UInt(uval),
                    RivetValue::Float(f64::NAN),
                    RivetValue::Null,
                ] {
                    let _ = fix.apply(&v);
                }
            }
            let _ = parse_enum_labels(&native);
        }

        #[test]
        fn build_column_is_total_over_arbitrary_cells(
            bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..48),
            ival in proptest::prelude::any::<i64>(),
        ) {
            use arrow::datatypes::{DataType, TimeUnit};
            let owned = [
                Some(RivetValue::Bytes(bytes.clone())),
                Some(RivetValue::Int(ival)),
                Some(RivetValue::Float(f64::INFINITY)),
                None,
            ];
            let cells: Vec<Option<&RivetValue>> = owned.iter().map(|c| c.as_ref()).collect();
            for dt in [
                DataType::Int32,
                DataType::UInt64,
                DataType::Float64,
                DataType::Utf8,
                DataType::Binary,
                DataType::FixedSizeBinary(16),
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Time64(TimeUnit::Microsecond),
                DataType::Boolean,
            ] {
                let _ = build("c", &dt, &cells);
            }
        }
    }

    #[test]
    fn cells_checksum_matches_built_array_for_every_covered_type() {
        use arrow::datatypes::Field;

        use crate::source::value_checksum::array_checksum;
        use RivetValue as V;
        let list_utf8 = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let list_i32 = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let cases: Vec<(DataType, Vec<Option<RivetValue>>)> = vec![
            (
                DataType::Boolean,
                vec![Some(V::Bool(true)), Some(V::Int(0)), None, Some(V::Null)],
            ),
            (
                DataType::Int16,
                vec![
                    Some(V::Int(-5)),
                    Some(V::Int(20_000)),
                    Some(V::UInt(7)),
                    None,
                ],
            ),
            (DataType::Int32, vec![Some(V::Int(-8_388_608)), None]),
            (
                DataType::Int64,
                vec![
                    Some(V::Int(i64::MIN)),
                    Some(V::UInt(9_000_000_000)),
                    Some(V::Bytes(b"-42.00".to_vec())),
                    None,
                ],
            ),
            (
                DataType::UInt64,
                vec![Some(V::UInt(u64::MAX)), Some(V::UInt(1)), None],
            ),
            (
                DataType::Float32,
                vec![
                    Some(V::Float(1.5)),
                    Some(V::Int(i64::MAX)),
                    Some(V::Bytes(b"0.1".to_vec())),
                    None,
                ],
            ),
            (
                DataType::Float64,
                vec![
                    Some(V::Float(f64::NAN)),
                    Some(V::UInt(3)),
                    Some(V::Bytes(b"12345.67".to_vec())),
                    None,
                ],
            ),
            (
                DataType::Date32,
                vec![
                    Some(V::DateTime(
                        chrono::NaiveDate::from_ymd_opt(2024, 3, 15)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                    )),
                    None,
                ],
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                vec![
                    Some(V::DateTime(
                        chrono::NaiveDate::from_ymd_opt(2035, 8, 7)
                            .unwrap()
                            .and_hms_micro_opt(9, 8, 7, 987_654)
                            .unwrap(),
                    )),
                    None,
                ],
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                vec![Some(V::TimeMicros(86_399_999_999)), None],
            ),
            (
                DataType::Decimal128(18, 2),
                vec![
                    Some(V::Bytes(b"999999999999.99".to_vec())),
                    Some(V::Int(-42)),
                    None,
                ],
            ),
            (
                DataType::Decimal256(50, 10),
                vec![
                    Some(V::Bytes(
                        b"1234567890123456789012345678901234567890.0123456789".to_vec(),
                    )),
                    None,
                ],
            ),
            (
                DataType::Utf8,
                vec![Some(V::Bytes("üñíçødé".as_bytes().to_vec())), None],
            ),
            (
                DataType::Binary,
                vec![Some(V::Bytes(vec![0x00, 0xff, 0x01])), None],
            ),
            (
                DataType::FixedSizeBinary(16),
                vec![Some(V::Bytes(vec![7u8; 16])), None],
            ),
            (
                list_utf8,
                vec![
                    Some(V::Array(vec![
                        V::Bytes(b"with,comma".to_vec()),
                        V::Null,
                        V::Bytes(b"".to_vec()),
                    ])),
                    Some(V::Array(vec![])),
                    None,
                ],
            ),
            (
                list_i32,
                vec![Some(V::Array(vec![V::Int(1), V::Null, V::Int(-3)])), None],
            ),
        ];
        for (dt, owned) in cases {
            let cells: Vec<Option<&RivetValue>> = owned.iter().map(|c| c.as_ref()).collect();
            let arr = build("c", &dt, &cells).unwrap();
            assert_eq!(
                cells_checksum(&dt, &cells),
                array_checksum(arr.as_ref()),
                "fold ≠ built array for {dt:?}"
            );
        }
    }

    /// A `uuid` override on a MySQL `CHAR(36)`/`VARCHAR(36)` column resolves to `FixedSizeBinary(16)`.
    #[test]
    fn a_text_uuid_survives_cdc_under_a_fixed_size_binary_override() {
        use crate::source::value_checksum::array_checksum;
        use RivetValue as V;
        use arrow::array::{Array, FixedSizeBinaryArray};

        let dt = DataType::FixedSizeBinary(16);
        let text = V::Bytes(b"550e8400-e29b-41d4-a716-446655440000".to_vec());
        let expected: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let raw = V::Bytes(expected.to_vec());
        let cells = [Some(&text), Some(&raw), None];
        let refs: Vec<Option<&RivetValue>> = cells.to_vec();

        let arr = build("c", &dt, &refs).unwrap();
        let fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("FixedSizeBinary(16) array");

        assert!(
            !fsb.is_null(0),
            "the 36-char text form must decode, not degrade to null — this is the \
             whole defect: every row of the column became NULL while counts passed"
        );
        assert_eq!(
            fsb.value(0),
            expected,
            "text form must decode to its raw bytes"
        );
        assert_eq!(fsb.value(1), expected, "raw 16-byte form must pass through");
        assert!(fsb.is_null(2), "a missing cell stays null");

        assert_eq!(
            cells_checksum(&dt, &refs),
            array_checksum(arr.as_ref()),
            "both ends must canonicalise identically — teaching only the builder \
             turns a CORRECT export into a checksum-mismatch failure at sink.rs"
        );
    }

    #[test]
    fn cells_checksum_detects_a_changed_cell() {
        use RivetValue as V;
        let a = [Some(V::Int(1)), Some(V::Int(2))];
        let b = [Some(V::Int(1)), Some(V::Int(3))];
        let ra: Vec<Option<&RivetValue>> = a.iter().map(|c| c.as_ref()).collect();
        let rb: Vec<Option<&RivetValue>> = b.iter().map(|c| c.as_ref()).collect();
        assert_ne!(
            cells_checksum(&DataType::Int64, &ra),
            cells_checksum(&DataType::Int64, &rb)
        );
    }

    #[test]
    fn explicit_null_value_builds_as_null_not_empty_string() {
        use arrow::array::Array;
        let cells: Vec<Option<&RivetValue>> = vec![Some(&RivetValue::Null), None];
        for dt in [DataType::Utf8, DataType::LargeUtf8] {
            let arr = build("c", &dt, &cells).unwrap();
            assert!(
                arr.is_null(0),
                "{dt:?}: Some(Null) must append a NULL, not an empty string"
            );
            assert!(arr.is_null(1));
        }
    }

    #[test]
    fn mysql_cell_fixes_convert_the_wire_shapes_the_binlog_delivers() {
        use RivetValue as V;
        let fix = |native: &str| {
            mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native)
        };

        let ts = fix("timestamp(6)")
            .apply(&V::Bytes(b"1893553445.678901".to_vec()))
            .unwrap();
        assert_eq!(
            ts,
            V::DateTime(
                chrono::DateTime::from_timestamp(1_893_553_445, 678_901_000)
                    .unwrap()
                    .naive_utc()
            )
        );

        assert_eq!(
            fix("timestamp").apply(&V::Bytes(b"0".to_vec())).unwrap(),
            V::Null
        );
        assert_eq!(
            fix("timestamp(6)")
                .apply(&V::Bytes(b"0.000000".to_vec()))
                .unwrap(),
            V::Null
        );
        let counted = |native: &str, wire: V| {
            let f = fix(native);
            f.nulled_a_date(&wire, &f.apply(&wire).unwrap())
        };
        assert!(counted("timestamp", V::Bytes(b"0".to_vec())));
        assert!(counted("timestamp(6)", V::Bytes(b"0.000000".to_vec())));
        assert!(counted("date", V::Bytes(ZERO_DATE.to_vec())));
        assert!(counted("datetime(6)", V::Bytes(ZERO_DATE.to_vec())));
        assert!(!counted("timestamp", V::Bytes(b"1".to_vec())));
        assert!(!counted("timestamp", V::Null));
        assert!(!counted("date", V::Null));
        assert!(!counted("year", V::Bytes(b"2026".to_vec())));
        for native in ["date", "datetime(6)"] {
            let text = V::Bytes(b"2026-01-02".to_vec());
            assert_eq!(fix(native).apply(&text).unwrap(), text, "{native}");
            assert!(!counted(native, text), "{native}");
        }
        assert_eq!(
            fix("timestamp").apply(&V::Bytes(b"1".to_vec())).unwrap(),
            V::DateTime(chrono::DateTime::from_timestamp(1, 0).unwrap().naive_utc()),
            "the first real TIMESTAMP second stays a value"
        );

        assert_eq!(
            fix("year").apply(&V::Bytes(b"1900".to_vec())).unwrap(),
            V::Int(0)
        );
        assert_eq!(
            fix("year").apply(&V::Bytes(b"1901".to_vec())).unwrap(),
            V::Int(1901)
        );
        assert_eq!(
            fix("year").apply(&V::Bytes(b"2024".to_vec())).unwrap(),
            V::Int(2024)
        );

        assert_eq!(
            fix("bit(1)").apply(&V::Bytes(vec![1])).unwrap(),
            V::Bool(true)
        );
        assert_eq!(
            fix("bit(8)").apply(&V::Bytes(vec![0xAA])).unwrap(),
            V::UInt(170)
        );

        assert_eq!(
            fix("year").apply(&V::Bytes(b"2030".to_vec())).unwrap(),
            V::Int(2030)
        );

        let e = fix("enum('a','b','c')");
        assert_eq!(e.apply(&V::Int(2)).unwrap(), V::Bytes(b"b".to_vec()));
        assert_eq!(e.apply(&V::Int(0)).unwrap(), V::Bytes(Vec::new()));

        assert_eq!(
            fix("binary(4)").apply(&V::Bytes(Vec::new())).unwrap(),
            V::Bytes(vec![0, 0, 0, 0])
        );
        let pad = fix("binary(4)");
        assert_eq!(
            pad.apply(&V::Bytes(vec![7, 8])).unwrap(),
            V::Bytes(vec![7, 8, 0, 0])
        );
        assert_eq!(
            pad.apply(&V::Bytes(vec![1, 2, 3, 4])).unwrap(),
            V::Bytes(vec![1, 2, 3, 4])
        );
        assert_eq!(
            pad.apply(&V::Bytes(vec![1, 2, 3, 4, 5])).unwrap(),
            V::Bytes(vec![1, 2, 3, 4, 5])
        );

        let mi = fix("mediumint");
        assert_eq!(mi.apply(&V::Int(8_388_608)).unwrap(), V::Int(-8_388_608));
        assert_eq!(mi.apply(&V::Int(8_388_607)).unwrap(), V::Int(8_388_607));
        assert!(
            mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, "mediumint unsigned").is_none()
        );

        let st = fix("set('x','y','z')");
        assert_eq!(
            st.apply(&V::Bytes(vec![0x05])).unwrap(),
            V::Bytes(b"x,z".to_vec())
        );
        assert_eq!(st.apply(&V::UInt(0)).unwrap(), V::Bytes(Vec::new()));

        assert_eq!(fix("year").apply(&V::Null).unwrap(), V::Null);
        assert!(mysql_cell_fix(crate::source::cdc::CdcEngine::Postgres, "bit(1)").is_none());
        assert!(mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, "varbinary(4)").is_none());
    }

    #[test]
    fn decimal_parse_is_lossless() {
        let v = RivetValue::Bytes(b"150.05".to_vec());
        assert_eq!(decimal_to_i128(&v, 2), Some(15005));
        assert_eq!(
            decimal_to_i128(&RivetValue::Bytes(b"-7.5".to_vec()), 3),
            Some(-7500)
        );
        assert_eq!(
            decimal_to_i128(&RivetValue::Bytes(b"42".to_vec()), 0),
            Some(42)
        );
    }

    #[test]
    fn a_float_past_the_f64_exact_range_is_refused_not_a_falsely_exact_decimal() {
        use arrow::datatypes::DataType;
        assert_eq!(
            decimal_to_i128(&RivetValue::Float(9.0e11), 4),
            Some(900_000_000_000_i128 * 10_000)
        );
        assert_eq!(decimal_to_i128(&RivetValue::Float(1e12), 4), None);
        assert!(decimal_to_i256(&RivetValue::Float(1e12), 4).is_none());
        let small = RivetValue::Float(12.34);
        let ok = build("c", &DataType::Decimal128(19, 4), &[Some(&small)])
            .expect("a small float builds");
        assert_eq!(ok.len(), 1);
        let huge = RivetValue::Float(1e12);
        let err = build("c", &DataType::Decimal128(19, 4), &[Some(&huge)])
            .expect_err("a float past the f64-exact range must fail loud, not round silently");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("2^53") && msg.contains("float"),
            "message must name the 2^53 range and the float: {msg}"
        );
    }

    /// The `Value::Time` arm's arithmetic, which nothing exercised.
    #[test]
    fn mysql_time_arm_pins_every_arithmetic_step() {
        let v = RivetValue::from_mysql(&mysql::Value::Time(false, 2, 3, 4, 5, 678_901));
        assert_eq!(
            v,
            RivetValue::TimeMicros(183_845_678_901),
            "days, hours, minutes, seconds and microseconds must each carry their \
             own weight"
        );
        let neg = RivetValue::from_mysql(&mysql::Value::Time(true, 2, 3, 4, 5, 678_901));
        assert_eq!(neg, RivetValue::TimeMicros(-183_845_678_901));
        assert_eq!(
            RivetValue::from_mysql(&mysql::Value::Time(false, 0, 0, 0, 0, 1)),
            RivetValue::TimeMicros(1)
        );
    }

    #[test]
    fn temporal_is_structural_not_string() {
        let v = RivetValue::from_mysql(&mysql::Value::Date(2026, 6, 23, 11, 58, 1, 500_000));
        let expected = NaiveDate::from_ymd_opt(2026, 6, 23)
            .unwrap()
            .and_hms_micro_opt(11, 58, 1, 500_000)
            .unwrap();
        assert_eq!(v, RivetValue::DateTime(expected));
        for zero in [
            mysql::Value::Date(0, 0, 0, 0, 0, 0, 0),
            mysql::Value::Date(2026, 0, 0, 0, 0, 0, 0),
        ] {
            let marker = RivetValue::from_mysql(&zero);
            assert_eq!(marker, RivetValue::Bytes(ZERO_DATE.to_vec()));
            for native in ["date", "datetime", "datetime(6)"] {
                let fix =
                    mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native);
                assert_eq!(fix.apply(&marker).unwrap(), RivetValue::Null, "{native}");
                assert_eq!(fix.apply(&RivetValue::Null).unwrap(), RivetValue::Null);
            }
        }
    }

    #[test]
    fn a_bit64_value_with_the_top_bit_builds_in_an_unsigned_column() {
        use arrow::array::UInt64Array;
        let fix = mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, "bit(64)").expect("bit(64)");
        let top = fix
            .apply(&RivetValue::Bytes(vec![0x80, 0, 0, 0, 0, 0, 0, 0]))
            .unwrap();
        let max = fix.apply(&RivetValue::Bytes(vec![0xff; 8])).unwrap();
        let arr = build("c", &DataType::UInt64, &[Some(&top), Some(&max)]).unwrap();
        let a = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!((a.value(0), a.value(1)), (1u64 << 63, u64::MAX));
    }

    #[test]
    fn build_column_narrows_int_and_fails_loud_on_overflow() {
        use arrow::array::{Array, Int32Array};
        let (v7, vnull_src) = (RivetValue::Int(7), RivetValue::Int(-5));
        let arr = build("c", &DataType::Int32, &[Some(&v7), None, Some(&vnull_src)]).unwrap();
        let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.value(0), 7);
        assert!(a.is_null(1));
        assert_eq!(a.value(2), -5);
        let vmax = RivetValue::Int(i64::MAX);
        let err = build("c", &DataType::Int32, &[Some(&vmax)])
            .expect_err("an integer overflowing the declared width must fail loud");
        assert!(
            err.to_string().to_lowercase().contains("overflow"),
            "message names the overflow: {err}"
        );
        let bit64 = RivetValue::UInt(u64::MAX);
        assert!(
            build("c", &DataType::Int64, &[Some(&bit64)]).is_err(),
            "a BIT(64) value past i64::MAX must fail loud, never a silent CDC null"
        );
    }

    #[test]
    fn list_column_fails_loud_on_a_non_array_cell_not_a_silent_null() {
        use arrow::datatypes::Field;
        let list_i32 = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let none: Option<&RivetValue> = None;
        build("c", &list_i32, &[none]).expect("a null cell builds a null list");
        let raw = RivetValue::Bytes(b"{{1,2},{3,4}}".to_vec());
        let err = build("c", &list_i32, &[Some(&raw)])
            .expect_err("a non-array cell in a list column must fail loud");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("multi-dimensional") && msg.contains("::text"),
            "message must name the multi-dim cause and the ::text remediation: {msg}"
        );
    }

    /// The invariant that keeps schema and data in lockstep.
    #[test]
    fn is_buildable_iff_build_column_produces_that_type() {
        use arrow::array::Array;
        let buildable = [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Decimal128(10, 2),
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(16),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ];
        let list = |t| DataType::List(Arc::new(arrow::datatypes::Field::new("item", t, true)));
        let mut buildable = buildable.to_vec();
        buildable.extend([
            list(DataType::Date32),
            list(DataType::Time64(TimeUnit::Microsecond)),
            list(DataType::Timestamp(TimeUnit::Microsecond, None)),
            list(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),
            list(DataType::FixedSizeBinary(16)),
            list(DataType::Binary),
        ]);
        for dt in &buildable {
            assert!(is_buildable(dt), "is_buildable must accept {dt:?}");
            let arr = build("c", dt, &[None]).unwrap();
            assert_eq!(
                arr.data_type(),
                dt,
                "build({dt:?}) produced a mismatched type"
            );
        }
        for dt in [
            DataType::Date64,
            list(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            list(DataType::FixedSizeBinary(8)),
            list(DataType::Time64(TimeUnit::Nanosecond)),
            list(DataType::Decimal128(10, 2)),
        ] {
            assert!(!is_buildable(&dt), "is_buildable must reject {dt:?}");
        }
    }

    /// A nanosecond timestamp cell keeps its 100 ns digit and refuses an instant outside Arrow's nanosecond range.
    #[test]
    fn a_nanosecond_timestamp_cell_keeps_its_digit_and_refuses_out_of_range() {
        use arrow::array::{Array, TimestampNanosecondArray};
        let at = |s: &str| RivetValue::DateTime(s.parse::<NaiveDateTime>().unwrap());
        let dt = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        let cells = [at("2026-01-02T03:04:05.1234567"), at("1677-09-21T00:12:44")];
        let arr = build("c", &dt, &[Some(&cells[0]), None]).unwrap();
        let ns = arr
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ns.value(0) % 1_000_000_000, 123_456_700);
        assert!(ns.is_null(1));
        let early = at("1000-01-01T00:00:00");
        let err = build("c", &dt, &[Some(&early)]).unwrap_err();
        assert!(err.to_string().contains("1677-09-21..2262-04-11"), "{err}");
        assert!(build("c", &dt, &[Some(&cells[1])]).is_ok());
    }

    /// Date, time, timestamp, uuid and bytea list elements build to the stored values, with NULL elements, empty and NULL lists kept.
    #[test]
    fn temporal_uuid_and_bytea_lists_build_to_the_stored_values() {
        use RivetValue as V;
        use arrow::array::{Array, ListArray};
        use arrow::datatypes::Field;
        let day = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let uuid_bytes = vec![7u8; 16];
        let cases = [
            (
                DataType::Date32,
                V::DateTime(day.and_hms_opt(0, 0, 0).unwrap()),
                "2024-01-02",
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                V::TimeMicros(3_723_250_000),
                "01:02:03.250",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                V::DateTime(day.and_hms_micro_opt(3, 4, 5, 6).unwrap()),
                "2024-01-02T03:04:05.000006",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                V::DateTime(day.and_hms_micro_opt(3, 4, 5, 6).unwrap()),
                "2024-01-02T03:04:05.000006Z",
            ),
            (
                DataType::FixedSizeBinary(16),
                V::Bytes(uuid_bytes.clone()),
                "07070707070707070707070707070707",
            ),
            (DataType::Binary, V::Bytes(vec![0, 255]), "00ff"),
        ];
        for (elem, v, shown) in cases {
            let dt = DataType::List(Arc::new(Field::new("item", elem.clone(), true)));
            let cell = V::Array(vec![v.clone(), V::Null]);
            let empty = V::Array(vec![]);
            let arr = build_column(&dt, &[Some(&cell), Some(&empty), None])
                .unwrap_or_else(|r| panic!("{elem}: {v:?} refused: {}", r.reason));
            assert_eq!(arr.data_type(), &dt, "{elem}");
            let lists = arr.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(lists.value(0).len(), 2, "{elem}");
            assert!(lists.value(0).is_null(1), "{elem}");
            assert_eq!(lists.value(1).len(), 0, "{elem}");
            assert!(lists.is_null(2), "{elem}");
            let got = arrow::util::display::array_value_to_string(&lists.value(0), 0).unwrap();
            assert_eq!(got, shown, "{elem}");
        }
    }

    /// A list element its type cannot hold refuses the whole cell and names the element.
    #[test]
    fn an_unrepresentable_temporal_or_binary_list_element_is_refused() {
        use RivetValue as V;
        use arrow::datatypes::Field;
        let day = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let list = |t| DataType::List(Arc::new(Field::new("item", t, true)));
        let cases = [
            (
                list(DataType::Date32),
                V::DateTime(day.and_hms_opt(1, 0, 0).unwrap()),
            ),
            (
                list(DataType::Time64(TimeUnit::Microsecond)),
                V::TimeMicros(86_400_000_000),
            ),
            (
                list(DataType::Timestamp(TimeUnit::Microsecond, None)),
                V::Int(1),
            ),
            (list(DataType::FixedSizeBinary(16)), V::Bytes(vec![1; 15])),
            (list(DataType::FixedSizeBinary(16)), V::Int(1)),
            (list(DataType::Binary), V::Int(1)),
        ];
        for (dt, elem) in cases {
            let cell = V::Array(vec![elem.clone()]);
            let r = build_column(&dt, &[Some(&cell)])
                .expect_err(&format!("{dt}: {elem:?} must be refused"));
            assert_eq!((r.row, &r.value), (0, &cell), "{dt}");
            assert!(r.reason.starts_with("has an element"), "{}", r.reason);
        }
        let raw = V::Bytes(b"{2024-01-01,infinity}".to_vec());
        let r = build_column(&list(DataType::Date32), &[Some(&raw)]).unwrap_err();
        assert!(r.reason.contains("multi-dimensional"), "{}", r.reason);
    }

    /// Every typed arm refuses a mismatched non-NULL cell by name, and still nulls a genuine NULL.
    #[test]
    fn a_mismatched_cell_is_refused_by_column_name_and_a_null_stays_null() {
        use RivetValue as V;
        let dec = V::Bytes(b"1.5".to_vec());
        let cases: Vec<(DataType, V)> = vec![
            (DataType::Int8, dec.clone()),
            (DataType::Int16, dec.clone()),
            (DataType::Int32, dec.clone()),
            (DataType::Int64, dec.clone()),
            (DataType::UInt8, dec.clone()),
            (DataType::UInt16, dec.clone()),
            (DataType::UInt32, dec.clone()),
            (DataType::UInt64, dec.clone()),
            (DataType::Int32, V::Float(1.5)),
            (DataType::Boolean, V::Bytes(b"t".to_vec())),
            (DataType::Float32, V::Bytes(b"abc".to_vec())),
            (DataType::Float64, V::Bytes(b"1.5x".to_vec())),
            (DataType::Float64, V::Bool(true)),
            (
                DataType::Date32,
                V::DateTime(
                    NaiveDate::from_ymd_opt(2026, 1, 1)
                        .unwrap()
                        .and_hms_opt(13, 14, 0)
                        .unwrap(),
                ),
            ),
            (DataType::Utf8, V::Array(vec![V::Int(1)])),
            (DataType::Date32, V::Bytes(b"2026-01-01".to_vec())),
            (DataType::Date32, V::Int(3)),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                V::Bytes(b"2026-01-01 00:00:00".to_vec()),
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                V::Int(3),
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                V::Bytes(b"12:00:00".to_vec()),
            ),
            (DataType::Binary, V::Int(7)),
            (DataType::LargeBinary, V::Int(7)),
            (
                DataType::FixedSizeBinary(16),
                V::Bytes(b"not-a-uuid".to_vec()),
            ),
            (DataType::FixedSizeBinary(4), V::Int(7)),
        ];
        for (dt, v) in &cases {
            let err = build("amount", dt, &[Some(v)])
                .expect_err(&format!("{dt:?} must refuse {v:?}, not write NULL"));
            let msg = format!("{err:#}");
            assert!(
                msg.contains("column 'amount'") && msg.contains(&dt.to_string()),
                "the refusal must name the column and the type: {msg}"
            );
            assert!(
                msg.contains(&format!("{:?}", render_str(v))),
                "the refusal must show the value: {msg}"
            );
            let arr = build("amount", dt, &[Some(&V::Null), None]).unwrap();
            assert_eq!(arr.null_count(), 2, "{dt:?}: a genuine NULL stays NULL");
        }
    }

    /// A long offending value is shown truncated, not dumped whole into the message.
    #[test]
    fn a_mismatch_message_truncates_the_value() {
        let long = RivetValue::Bytes(vec![b'9'; 500]);
        let msg = format!(
            "{:#}",
            build("c", &DataType::Int64, &[Some(&long)]).unwrap_err()
        );
        assert!(msg.contains(&format!("\"{}\"…", "9".repeat(64))), "{msg}");
        assert!(!msg.contains(&"9".repeat(65)), "{msg}");
        let short = format!(
            "{:#}",
            build(
                "c",
                &DataType::Int64,
                &[Some(&RivetValue::Bytes(b"1.5".to_vec()))]
            )
            .unwrap_err()
        );
        assert!(
            short.contains("\"1.5\" (row 0)"),
            "no ellipsis on a short value: {short}"
        );
    }

    /// An unsigned cell in a Boolean column is true exactly when it is non-zero.
    #[test]
    fn an_unsigned_cell_in_a_boolean_column_is_true_when_non_zero() {
        let arr = build(
            "b",
            &DataType::Boolean,
            &[Some(&RivetValue::UInt(1)), Some(&RivetValue::UInt(0))],
        )
        .unwrap();
        let b = arr
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(b.value(0) && !b.value(1));
    }

    /// A MySQL TIME outside one day is refused naming its row and value; in-range times build.
    #[test]
    fn a_time_outside_one_day_is_refused_by_row_and_value() {
        let dt = DataType::Time64(TimeUnit::Microsecond);
        let ok = RivetValue::TimeMicros(0);
        for us in [
            (838 * 3600 + 59 * 60 + 59) * 1_000_000i64,
            -3_600_000_000,
            86_400_000_000,
        ] {
            let bad = RivetValue::TimeMicros(us);
            let r = build_column(&dt, &[Some(&ok), Some(&bad)]).unwrap_err();
            assert_eq!((r.row, &r.value), (1, &bad));
            assert!(r.reason.contains("outside 00:00..24:00"), "{}", r.reason);
        }
        let ok = [0i64, 86_399_999_999].map(RivetValue::TimeMicros);
        let arr = build_column(&dt, &[Some(&ok[0]), Some(&ok[1])]).unwrap();
        assert_eq!(arr.null_count(), 0);
    }

    /// A DATE override refuses a DateTime with a time of day (batch wording) and keeps a midnight one.
    #[test]
    fn a_date_column_refuses_a_time_of_day_instead_of_dropping_it() {
        use arrow::array::Date32Array;
        let day = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let midnight = RivetValue::DateTime(day.and_hms_opt(0, 0, 0).unwrap());
        let afternoon = RivetValue::DateTime(day.and_hms_opt(13, 14, 0).unwrap());
        let r = build_column(&DataType::Date32, &[Some(&midnight), Some(&afternoon)]).unwrap_err();
        assert_eq!((r.row, &r.value), (1, &afternoon));
        assert!(
            r.reason
                .contains("has a time of day, which a `date` override would drop")
                && r.reason.contains("declare it `timestamp`"),
            "{}",
            r.reason
        );
        let arr = build_column(&DataType::Date32, &[Some(&midnight)]).unwrap();
        let d = arr.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(d.value(0), 19_797, "2024-03-15 is day 19797 of the epoch");
    }

    /// Decimal text fills a float column by the shared parser; non-numeric text is refused.
    #[test]
    fn float_columns_parse_decimal_text_and_refuse_words() {
        use arrow::array::{Float32Array, Float64Array};
        let cells = [
            RivetValue::Bytes(b"1.5".to_vec()),
            RivetValue::Bytes(b"-12345.67".to_vec()),
            RivetValue::Bytes(b"0.10".to_vec()),
        ];
        let refs: Vec<Option<&RivetValue>> = cells.iter().map(Some).collect();
        let a64 = build_column(&DataType::Float64, &refs).unwrap();
        let a64 = a64.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(a64.values().to_vec(), vec![1.5, -12345.67, 0.1]);
        let a32 = build_column(&DataType::Float32, &refs).unwrap();
        let a32 = a32.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(a32.values().to_vec(), vec![1.5f32, -12345.67f32, 0.1f32]);
        for dt in [DataType::Float32, DataType::Float64] {
            let bad = RivetValue::Bytes(b"n/a".to_vec());
            let r = build_column(&dt, &[Some(&bad)]).unwrap_err();
            assert_eq!(
                (r.row, &r.value, r.reason.as_str()),
                (0, &bad, "is not a number")
            );
        }
    }

    /// Decimal text a float8 prints back is delivered; text it would round is refused by code, on the change stream as in batch.
    #[test]
    fn decimal_text_a_float8_would_round_is_refused_by_code() {
        use arrow::array::Float64Array;
        let kept = [
            RivetValue::Bytes(b"12345678901.2345".to_vec()),
            RivetValue::Bytes(b"9007199254740992".to_vec()),
            RivetValue::Bytes(b"-0.10".to_vec()),
        ];
        let refs: Vec<Option<&RivetValue>> = kept.iter().map(Some).collect();
        let a = build_column(&DataType::Float64, &refs).unwrap();
        let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(
            a.values().to_vec(),
            vec![12345678901.2345, 9_007_199_254_740_992.0, -0.1]
        );
        for text in [
            &b"922337203685477.5807"[..],
            b"9007199254740993",
            b"0.1234567890123456789",
        ] {
            let bad = RivetValue::Bytes(text.to_vec());
            let r = build_column(&DataType::Float64, &[Some(&bad)]).unwrap_err();
            assert_eq!(
                r.code.map(|c| c.id),
                Some(crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE.id),
                "{r:?}"
            );
        }
    }

    /// Exact integer text fills an integer column; a fraction or overflow is refused.
    #[test]
    fn integer_columns_take_exact_integer_text_only() {
        use arrow::array::Int32Array;
        let cells = [
            RivetValue::Bytes(b"42".to_vec()),
            RivetValue::Bytes(b"-7.00".to_vec()),
        ];
        let refs: Vec<Option<&RivetValue>> = cells.iter().map(Some).collect();
        let arr = build_column(&DataType::Int32, &refs).unwrap();
        let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.values().to_vec(), vec![42, -7]);
        let frac = RivetValue::Bytes(b"1.5".to_vec());
        let r = build_column(&DataType::Int32, &[Some(&frac)]).unwrap_err();
        assert_eq!(r.reason, "is not an exact integer");
        let big = RivetValue::Bytes(b"3000000000".to_vec());
        let r = build_column(&DataType::Int32, &[Some(&big)]).unwrap_err();
        assert!(r.reason.starts_with("overflows"), "{}", r.reason);
    }

    /// A list element that overflows or has the wrong variant refuses the cell, never a null element.
    #[test]
    fn a_list_element_that_does_not_fit_is_refused_not_nulled() {
        use arrow::datatypes::Field;
        let list = |t| DataType::List(Arc::new(Field::new("item", t, true)));
        let cases = [
            (list(DataType::Int16), RivetValue::Int(70_000)),
            (list(DataType::Int32), RivetValue::Int(i64::MAX)),
            (list(DataType::Int64), RivetValue::Bytes(b"x".to_vec())),
            (list(DataType::Boolean), RivetValue::Int(1)),
            (list(DataType::Float32), RivetValue::Bytes(b"x".to_vec())),
            (list(DataType::Float64), RivetValue::Bool(true)),
            (list(DataType::Utf8), RivetValue::Array(vec![])),
        ];
        for (dt, elem) in cases {
            let cell = RivetValue::Array(vec![RivetValue::Null, elem.clone()]);
            let r = build_column(&dt, &[None, Some(&cell)])
                .expect_err(&format!("{dt}: {elem:?} must be refused"));
            assert_eq!((r.row, &r.value), (1, &cell), "{dt}");
            assert!(r.reason.starts_with("has an element"), "{}", r.reason);
        }
        let ok = RivetValue::Array(vec![RivetValue::Int(1), RivetValue::Null]);
        let arr = build_column(&list(DataType::Int16), &[Some(&ok)]).unwrap();
        assert_eq!(arr.len(), 1);
    }

    /// A Boolean column carries 0 and 1 of either signedness; any other integer is refused by code with the remedy, on every engine.
    #[test]
    fn a_boolean_is_a_flag_or_refused_by_code_for_signed_and_unsigned_wire_ints() {
        use RivetValue as V;
        for (v, want) in [
            (V::Bool(true), true),
            (V::Bool(false), false),
            (V::Int(0), false),
            (V::Int(1), true),
            (V::UInt(0), false),
            (V::UInt(1), true),
        ] {
            assert_eq!(bool_value(&v), Ok(want), "{v:?}");
        }
        for bad in [
            V::Int(2),
            V::Int(-1),
            V::Int(i64::MIN),
            V::Int(i64::MAX),
            V::UInt(2),
            V::UInt(u64::MAX),
        ] {
            let err = bool_value(&bad).unwrap_err();
            assert_eq!(
                err.code.map(|c| c.id),
                Some("RIVET_SOURCE_VALUE_UNREPRESENTABLE"),
                "{bad:?}"
            );
            assert!(
                err.reason.starts_with(
                    "is not a flag: a boolean column carries 0 or 1 only. Declare the column \
                     an integer type in `columns:` (for example `int64`) to carry the number, \
                     or leave the column out of the capture, then: Re-baseline"
                ),
                "{err:?}"
            );
        }
        assert_eq!(
            bool_value(&V::Float(1.0)).unwrap_err().reason,
            NO_READING,
            "a variant with no reading keeps its own refusal"
        );
    }

    /// The flag rule is one function: 0 and 1 are flags at either edge of every integer width, the neighbours are not.
    #[test]
    fn a_flag_is_zero_or_one_at_every_edge_of_the_wire_widths() {
        for (v, want) in [(0i128, Some(false)), (1, Some(true))] {
            assert_eq!(flag_value(v).ok(), want, "{v}");
        }
        for v in [-1i128, 2, i128::from(i64::MIN), i128::from(u64::MAX)] {
            assert!(flag_value(v).is_err(), "{v}");
        }
    }

    /// A MySQL BIT(1) keeps its own reading (any set bit), and a built Boolean column refuses the cell that is no flag with the row.
    #[test]
    fn a_bit_one_keeps_its_reading_and_a_built_boolean_names_the_row_of_a_cell_that_is_no_flag() {
        use crate::source::cdc::CdcEngine::Mysql;
        use RivetValue as V;
        let bit1 = mysql_cell_fix(Mysql, "bit(1)").expect("BitBool");
        assert_eq!(bit1.apply(&V::Bytes(vec![1])).unwrap(), V::Bool(true));
        assert_eq!(bit1.apply(&V::Bytes(vec![0])).unwrap(), V::Bool(false));
        let ok = V::Int(1);
        let bad = V::Int(5);
        let r = build_column(&DataType::Boolean, &[Some(&ok), Some(&bad)]).unwrap_err();
        assert_eq!((r.row, r.value.clone()), (1, V::Int(5)));
        assert_eq!(
            r.code.map(|c| c.id),
            Some("RIVET_SOURCE_VALUE_UNREPRESENTABLE")
        );
    }

    /// Each numeric reader takes every wire variant a driver delivers for it.
    #[test]
    fn numeric_readers_take_every_wire_variant() {
        use RivetValue as V;
        assert_eq!(int_value::<i16>(&V::Bool(true)), Ok(1));
        assert_eq!(int_value::<i16>(&V::Bool(false)), Ok(0));
        assert_eq!(f64_value(&V::Int(-3)), Ok(-3.0));
        assert_eq!(f32_value(&V::UInt(7)), Ok(7.0));
    }

    /// Every list element type builds from its driver variant, and the cell fold equals the built list's fold.
    #[test]
    fn every_list_element_type_builds_from_its_driver_variant() {
        use arrow::array::{Array, ListArray};
        use arrow::datatypes::Field;

        use crate::source::value_checksum::array_checksum;
        use RivetValue as V;
        let cases = [
            (DataType::Boolean, V::Bool(true), "true"),
            (DataType::Int64, V::Int(i64::MIN), "-9223372036854775808"),
            (DataType::Float32, V::Float(1.5), "1.5"),
            (DataType::Float64, V::Float(2.25), "2.25"),
            (DataType::Float64, V::Int(3), "3.0"),
        ];
        for (elem, v, shown) in cases {
            let dt = DataType::List(Arc::new(Field::new("item", elem.clone(), true)));
            let cell = V::Array(vec![v.clone(), V::Null]);
            let arr = build_column(&dt, &[Some(&cell)])
                .unwrap_or_else(|r| panic!("{elem}: {v:?} refused: {}", r.reason));
            let list = arr.as_any().downcast_ref::<ListArray>().unwrap().value(0);
            assert_eq!(list.len(), 2, "{elem}");
            assert!(list.is_null(1), "{elem}");
            let got = arrow::util::display::array_value_to_string(&list, 0).unwrap();
            assert_eq!(got, shown, "{elem}");
            assert_eq!(
                cells_checksum(&dt, &[Some(&cell)]),
                array_checksum(arr.as_ref()),
                "{elem}: cell fold drifted from the built list"
            );
        }
    }

    /// Every MySQL cell fix refuses a wire value it cannot read instead of returning NULL.
    #[test]
    fn mysql_cell_fixes_refuse_an_unreadable_wire_value() {
        use RivetValue as V;
        let fix = |native: &str| {
            mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native)
        };
        assert!(fix("timestamp").apply(&V::Bytes(b"soon".to_vec())).is_err());
        assert!(
            fix("timestamp")
                .apply(&V::Bytes(b"99999999999999999".to_vec()))
                .is_err()
        );
        assert!(fix("year").apply(&V::Bytes(b"MMXXIV".to_vec())).is_err());
        let e = fix("enum('a','b')");
        let err = e.apply(&V::Int(3)).unwrap_err();
        assert!(
            err.reason.contains("past the column's 2 ENUM labels"),
            "{err:?}"
        );
        assert!(e.apply(&V::UInt(u64::MAX)).is_err());
        let t = fix("time(6)");
        let err = t
            .apply(&V::TimeMicros((838 * 3600 + 59 * 60 + 59) * 1_000_000))
            .unwrap_err();
        assert_eq!(
            err.code.map(|c| c.id),
            Some("RIVET_SOURCE_VALUE_UNREPRESENTABLE")
        );
        assert!(
            err.reason
                .starts_with("mysql: TIME 838:59:59.000000 is outside 00:00..24:00")
                && err
                    .reason
                    .contains("declare the column `string` in `columns:`")
                && err
                    .reason
                    .contains("leave the column out of the capture, then: Re-baseline")
                && !err.reason.contains("Cast it in a query"),
            "a stream names the remedies it has, not the batch cast: {err:?}"
        );
        assert!(
            t.applies_to(&DataType::Time64(TimeUnit::Microsecond))
                && !t.applies_to(&DataType::Utf8),
            "the TIME check guards a Time64 column only"
        );
        assert_eq!(
            t.apply(&V::TimeMicros(1)).unwrap(),
            V::TimeMicros(1),
            "a time of day passes through"
        );
        assert_eq!(e.apply(&V::UInt(1)).unwrap(), V::Bytes(b"a".to_vec()));
    }

    /// A TIME rendered as text is `[-]hh:mm:ss.ffffff`, never raw microseconds.
    #[test]
    fn a_time_renders_as_time_beyond_day_text() {
        let us = -(838 * 3600 + 59 * 60 + 59) * 1_000_000i64;
        assert_eq!(render_str(&RivetValue::TimeMicros(us)), "-838:59:59.000000");
        let arr = build_column(&DataType::Utf8, &[Some(&RivetValue::TimeMicros(1))]).unwrap();
        let a = arr
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(a.value(0), "00:00:00.000001");
    }
}

#[cfg(test)]
mod renderer_twins {
    use super::{RivetValue, bytes_to_recoverable_string, render_str};
    use crate::types::{hex_bytes, iso_timestamp_nanos};
    use chrono::NaiveDate;

    #[test]
    /// Strict known divergence: ADR-0038 divergence.
    #[should_panic(expected = "assertion")]
    fn non_utf8_bytes_render_like_hex_bytes() {
        for b in [vec![0xE9], vec![0xFF, 0x00, 0x80]] {
            assert_eq!(bytes_to_recoverable_string(&b), hex_bytes(&b));
        }
    }

    #[test]
    /// Strict known divergence: ADR-0038 divergence.
    #[should_panic(expected = "assertion")]
    fn a_datetime_renders_like_iso_timestamp_nanos() {
        for (h, n) in [(13, 123_000_000), (0, 0), (23, 1)] {
            let dt = NaiveDate::from_ymd_opt(2024, 2, 29)
                .unwrap()
                .and_hms_nano_opt(h, 4, 5, n)
                .unwrap();
            let want = iso_timestamp_nanos(dt, false);
            assert_eq!(render_str(&RivetValue::DateTime(dt)), want);
            assert_eq!(
                RivetValue::DateTime(dt).to_json(),
                serde_json::Value::String(want)
            );
        }
    }
}
