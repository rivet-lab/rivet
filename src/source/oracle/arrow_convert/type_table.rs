//! Per-type table of the Oracle mapper: its Rivet type, delivery, fidelity and the override pairs read.

use super::super::declared_type;
use super::super::kind::OraKind;
use super::{
    autodetect, deliver_number_text, is_bare_number, sub_microsecond, with_oracle_warning,
};
use crate::types::characterization::{Case, case, table_message, wrong_rows};
use crate::types::override_table::Engine;
use crate::types::{SourceColumn, TypeMapping};

/// The mapping the Oracle mapper gives an un-overridden column of `kind`, driver label `base`, `precision` and `scale`.
fn of(kind: OraKind, base: &str, precision: u8, scale: i8) -> TypeMapping {
    let native = declared_type(kind, base.to_string(), precision, scale);
    let rivet = autodetect(kind, &native, precision, scale, base);
    let mapping = TypeMapping::from_source(&SourceColumn::simple("c", native.clone(), true), rivet);
    let mapping = deliver_number_text(mapping, kind, precision, scale);
    with_oracle_warning(
        mapping,
        is_bare_number(kind, precision, scale),
        sub_microsecond(&native, scale),
    )
}

/// One row per Oracle driver type the mapper decides on, with the precision and scale that change its type.
fn table() -> Vec<Case> {
    vec![
        case(
            "number(1,0)",
            of(OraKind::Number, "number", 1, 0),
            "int32 | Int32 | native | exact | reads int16,int32,int64,float64,decimal(18,2),string,json",
        ),
        case(
            "number(9,0)",
            of(OraKind::Number, "number", 9, 0),
            "int32 | Int32 | native | exact | reads int16,int32,int64,float64,decimal(18,2),string,json",
        ),
        case(
            "number(10,0)",
            of(OraKind::Number, "number", 10, 0),
            "int64 | Int64 | native | exact | reads int16,int32,int64,float64,decimal(18,2),string,json",
        ),
        case(
            "number(18,0)",
            of(OraKind::Number, "number", 18, 0),
            "int64 | Int64 | native | exact | reads int16,int32,int64,float64,decimal(18,2),string,json",
        ),
        case(
            "number(19,0)",
            of(OraKind::Number, "number", 19, 0),
            "decimal(19,0) | Decimal128(19, 0) | native | exact | reads int16,int32,int64,float64,decimal(18,2),string,json",
        ),
        case(
            "number(38,0)",
            of(OraKind::Number, "number", 38, 0),
            "decimal(38,0) | Decimal128(38, 0) | native | exact | reads int16,int32,int64,float64,decimal(18,2),string,json",
        ),
        case(
            "number(12,2)",
            of(OraKind::Number, "number", 12, 2),
            "decimal(12,2) | Decimal128(12, 2) | native | exact | reads float64,decimal(18,2),string,json",
        ),
        case(
            "number(38,10)",
            of(OraKind::Number, "number", 38, 10),
            "decimal(38,10) | Decimal128(38, 10) | native | exact | reads float64,decimal(18,2),string,json",
        ),
        case(
            "number(5,-2)",
            of(OraKind::Number, "number", 5, -2),
            "decimal(7,0) | Decimal128(7, 0) | native | exact | reads int16,int32,int64,float64,decimal(18,2),string,json",
        ),
        case(
            "number(38,-5) (beyond 38 digits)",
            of(OraKind::Number, "number", 38, -5),
            "string | Utf8 | text:decimal_plain | logical_string | reads int16,int32,int64,float64,decimal(18,2),string,json",
        ),
        case(
            "number(38,39) (scale beyond 38)",
            of(OraKind::Number, "number", 38, 39),
            "string | Utf8 | text:decimal_plain | logical_string | reads int16,int32,int64,float64,decimal(18,2),string,json",
        ),
        case(
            "number (bare)",
            of(OraKind::Number, "number", 0, -127),
            "string | Utf8 | text:decimal_plain | logical_string | reads int16,int32,int64,float64,decimal(18,2),string,json",
        ),
        case(
            "number(0,0)",
            of(OraKind::Number, "number", 0, 0),
            "string | Utf8 | text:decimal_plain | logical_string | reads int16,int32,int64,float64,decimal(18,2),string,json",
        ),
        case(
            "float(10)",
            of(OraKind::Number, "number", 10, -127),
            "string | Utf8 | text:decimal_plain | logical_string | reads int16,int32,int64,float64,decimal(18,2),string,json # red:ORACLE_NUMBER_INF_FINITE",
        ),
        case(
            "float(126)",
            of(OraKind::Number, "number", 126, -127),
            "string | Utf8 | text:decimal_plain | logical_string | reads int16,int32,int64,float64,decimal(18,2),string,json # red:ORACLE_NUMBER_INF_FINITE",
        ),
        case(
            "binary_float",
            of(OraKind::BinaryFloat, "binary_float", 0, 0),
            "float32 | Float32 | native | exact | reads float32,float64,string,json",
        ),
        case(
            "binary_double",
            of(OraKind::BinaryDouble, "binary_double", 0, 0),
            "float64 | Float64 | native | exact | reads float64,string,json",
        ),
        case(
            "boolean",
            of(OraKind::Boolean, "boolean", 0, 0),
            "bool | Boolean | native | exact | reads bool,string,json",
        ),
        case(
            "date",
            of(OraKind::Date, "date", 0, 0),
            "timestamp(microsecond) | Timestamp(µs) | native | exact | reads date,timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,json",
        ),
        case(
            "timestamp(0)",
            of(OraKind::Timestamp, "timestamp", 0, 0),
            "timestamp(microsecond) | Timestamp(µs) | native | exact | reads date,timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,json",
        ),
        case(
            "timestamp(6)",
            of(OraKind::Timestamp, "timestamp", 0, 6),
            "timestamp(microsecond) | Timestamp(µs) | native | exact | reads date,timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,json",
        ),
        case(
            "timestamp(9)",
            of(OraKind::Timestamp, "timestamp", 0, 9),
            "timestamp(microsecond) | Timestamp(µs) | native | lossy | reads date,timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,json",
        ),
        case(
            "timestamp_tz(6)",
            of(OraKind::TimestampTz, "timestamp_tz", 0, 6),
            "timestamp_tz(microsecond,UTC) | Timestamp(µs, \"UTC\") | native | exact | reads date,timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,json",
        ),
        case(
            "timestamp_tz(9)",
            of(OraKind::TimestampTz, "timestamp_tz", 0, 9),
            "timestamp_tz(microsecond,UTC) | Timestamp(µs, \"UTC\") | native | lossy | reads date,timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,json",
        ),
        case(
            "timestamp_ltz(6)",
            of(OraKind::TimestampLtz, "timestamp_ltz", 0, 6),
            "timestamp_tz(microsecond,UTC) | Timestamp(µs, \"UTC\") | native | exact | reads date,timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,json",
        ),
        case(
            "timestamp_ltz(9)",
            of(OraKind::TimestampLtz, "timestamp_ltz", 0, 9),
            "timestamp_tz(microsecond,UTC) | Timestamp(µs, \"UTC\") | native | lossy | reads date,timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,json",
        ),
        case(
            "interval day to second(6)",
            of(OraKind::IntervalDs, "interval_ds", 0, 6),
            "interval | Utf8 | text:iso8601_duration | logical_string | reads string,json,interval",
        ),
        case(
            "interval day to second(9)",
            of(OraKind::IntervalDs, "interval_ds", 0, 9),
            "interval | Utf8 | text:iso8601_duration | lossy | reads string,json,interval # red:ORACLE_INTERVAL_DS_NS",
        ),
        case(
            "interval year to month",
            of(OraKind::IntervalYm, "interval_ym", 0, 0),
            "interval | Utf8 | text:iso8601_duration | logical_string | reads string,json,interval",
        ),
        case(
            "varchar2",
            of(OraKind::Text, "varchar", 0, 0),
            "string | Utf8 | native | exact | reads string,json",
        ),
        case(
            "nvarchar2",
            of(OraKind::Text, "nvarchar", 0, 0),
            "string | Utf8 | native | exact | reads string,json",
        ),
        case(
            "char",
            of(OraKind::Text, "char", 0, 0),
            "string | Utf8 | native | exact | reads string,json",
        ),
        case(
            "nchar",
            of(OraKind::Text, "nchar", 0, 0),
            "string | Utf8 | native | exact | reads string,json",
        ),
        case(
            "long",
            of(OraKind::Text, "long", 0, 0),
            "string | Utf8 | native | exact | reads string,json",
        ),
        case(
            "clob",
            of(OraKind::Clob, "clob", 0, 0),
            "string | Utf8 | native | exact | reads string,json",
        ),
        case(
            "nclob",
            of(OraKind::Clob, "nclob", 0, 0),
            "string | Utf8 | native | exact | reads string,json",
        ),
        case(
            "json (as clob)",
            of(OraKind::Clob, "json", 0, 0),
            "json | Utf8 | native | logical_string | reads json",
        ),
        case(
            "blob",
            of(OraKind::Blob, "blob", 0, 0),
            "binary | Binary | native | exact | reads string,binary,json,uuid",
        ),
        case(
            "raw",
            of(OraKind::Raw, "raw", 0, 0),
            "binary | Binary | native | exact | reads string,binary,json,uuid",
        ),
        case(
            "long raw",
            of(OraKind::Raw, "long_raw", 0, 0),
            "binary | Binary | native | exact | reads string,binary,json,uuid",
        ),
        case(
            "bfile",
            of(OraKind::Other, "bfile", 0, 0),
            "unsupported(bfile) | - | unsupported | unsupported | reads bool,int16,int32,int64,uint64,float32,float64,decimal(18,2),decimal(50,2),date,time(microsecond),timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,binary,json,uuid,enum,interval,list<int32>,unsupported(x)",
        ),
    ]
}

/// Every Oracle native type keeps the Rivet type, delivery, fidelity and override pairs pinned here.
#[test]
fn the_oracle_type_table_is_pinned() {
    let cases = table();
    let wrong = wrong_rows(Engine::Oracle, &cases);
    assert_eq!(
        wrong,
        Vec::<String>::new(),
        "{}",
        table_message(Engine::Oracle, cases.len(), &wrong)
    );
}
