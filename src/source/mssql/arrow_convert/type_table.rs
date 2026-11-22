//! Per-type table of the SQL Server mapper: its Rivet type, delivery, fidelity and the override pairs read.

use std::collections::{HashMap, HashSet};

use tiberius::{Column, ColumnType};

use super::{CatalogHints, mssql_type_mappings};
use crate::types::characterization::{Case, case, table_message, wrong_rows};
use crate::types::override_table::Engine;
use crate::types::{ColumnOverrides, TypeMapping};

/// The mapping the SQL Server mapper gives a column of wire type `ty` under `hints`.
fn of(ty: ColumnType, hints: Option<&CatalogHints>) -> TypeMapping {
    let column = Column::new("c".to_string(), ty);
    mssql_type_mappings(&[column], &ColumnOverrides::new(), hints)
        .expect("a column without an override maps")
        .remove(0)
}

/// The catalog hints of a decimal column declared `(precision, scale)`.
fn decimal(precision: u8, scale: i8) -> CatalogHints {
    CatalogHints {
        decimals: HashMap::from([("c".to_string(), (precision, scale))]),
        fine_timestamps: HashSet::new(),
        fine_times: HashSet::new(),
    }
}

/// The catalog hints of a `datetime2` / `datetimeoffset` column finer than a microsecond.
fn fine() -> CatalogHints {
    CatalogHints {
        decimals: HashMap::new(),
        fine_timestamps: HashSet::from(["c".to_string()]),
        fine_times: HashSet::new(),
    }
}

/// The catalog hints of a `time` column finer than a microsecond.
fn fine_time() -> CatalogHints {
    CatalogHints {
        decimals: HashMap::new(),
        fine_timestamps: HashSet::new(),
        fine_times: HashSet::from(["c".to_string()]),
    }
}

/// One row per SQL Server wire type the mapper decides on, plus the catalog-hint resolutions.
fn table() -> Vec<Case> {
    vec![
        case(
            "null",
            of(ColumnType::Null, None),
            "unsupported(null) | - | unsupported | unsupported | reads bool,int16,int32,int64,uint64,float32,float64,decimal(18,2),decimal(50,2),date,time(microsecond),timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,binary,json,uuid,enum,interval,list<int32>,unsupported(x)",
        ),
        case(
            "bit",
            of(ColumnType::Bit, None),
            "bool | Boolean | native | exact | reads bool",
        ),
        case(
            "bitn (bit)",
            of(ColumnType::Bitn, None),
            "bool | Boolean | native | exact | reads bool",
        ),
        case(
            "int1 (tinyint)",
            of(ColumnType::Int1, None),
            "int16 | Int16 | native | exact | reads int16,decimal(18,2)",
        ),
        case(
            "int2 (smallint)",
            of(ColumnType::Int2, None),
            "int16 | Int16 | native | exact | reads int16,decimal(18,2)",
        ),
        case(
            "int4 (int)",
            of(ColumnType::Int4, None),
            "int32 | Int32 | native | exact | reads int32,decimal(18,2)",
        ),
        case(
            "int8 (bigint)",
            of(ColumnType::Int8, None),
            "int64 | Int64 | native | exact | reads int64,decimal(18,2)",
        ),
        case(
            "intn",
            of(ColumnType::Intn, None),
            "int64 | Int64 | native | exact | reads int64,decimal(18,2)",
        ),
        case(
            "float4 (real)",
            of(ColumnType::Float4, None),
            "float32 | Float32 | native | exact | reads float32",
        ),
        case(
            "float8 (float)",
            of(ColumnType::Float8, None),
            "float64 | Float64 | native | exact | reads float64",
        ),
        case(
            "floatn",
            of(ColumnType::Floatn, None),
            "float64 | Float64 | native | exact | reads float64",
        ),
        case(
            "money",
            of(ColumnType::Money, None),
            "decimal(19,4) | Decimal128(19, 4) | native | exact | reads float64,decimal(18,2)",
        ),
        case(
            "money4 (smallmoney)",
            of(ColumnType::Money4, None),
            "decimal(10,4) | Decimal128(10, 4) | native | exact | reads float64,decimal(18,2)",
        ),
        case(
            "guid (uniqueidentifier)",
            of(ColumnType::Guid, None),
            "uuid | FixedSizeBinary(16) | native | exact | reads uuid",
        ),
        case(
            "decimaln (no catalog hint)",
            of(ColumnType::Decimaln, None),
            "decimal(38,0) | Decimal128(38, 0) | native | exact | reads decimal(18,2)",
        ),
        case(
            "numericn (no catalog hint)",
            of(ColumnType::Numericn, None),
            "decimal(38,0) | Decimal128(38, 0) | native | exact | reads decimal(18,2)",
        ),
        case(
            "daten (date)",
            of(ColumnType::Daten, None),
            "date | Date32 | native | exact | reads date",
        ),
        case(
            "timen (time)",
            of(ColumnType::Timen, None),
            "time(microsecond) | Time64(µs) | native | exact | reads time(microsecond)",
        ),
        case(
            "datetime",
            of(ColumnType::Datetime, None),
            "timestamp(microsecond) | Timestamp(µs) | native | exact | reads timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC)",
        ),
        case(
            "datetime4 (smalldatetime)",
            of(ColumnType::Datetime4, None),
            "timestamp(microsecond) | Timestamp(µs) | native | exact | reads timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC)",
        ),
        case(
            "datetimen",
            of(ColumnType::Datetimen, None),
            "timestamp(microsecond) | Timestamp(µs) | native | exact | reads timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC)",
        ),
        case(
            "datetime2 (no catalog hint)",
            of(ColumnType::Datetime2, None),
            "timestamp(microsecond) | Timestamp(µs) | native | exact | reads timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC)",
        ),
        case(
            "datetimeoffsetn (datetimeoffset)",
            of(ColumnType::DatetimeOffsetn, None),
            "timestamp_tz(microsecond,UTC) | Timestamp(µs, \"UTC\") | native | exact | reads timestamp(microsecond),timestamp_tz(microsecond,UTC)",
        ),
        case(
            "bigvarbin (varbinary)",
            of(ColumnType::BigVarBin, None),
            "binary | Binary | native | exact | reads binary",
        ),
        case(
            "bigbinary (binary)",
            of(ColumnType::BigBinary, None),
            "binary | Binary | native | exact | reads binary",
        ),
        case(
            "image",
            of(ColumnType::Image, None),
            "binary | Binary | native | exact | reads binary",
        ),
        case(
            "bigvarchar (varchar)",
            of(ColumnType::BigVarChar, None),
            "string | Utf8 | native | exact | reads string,json,uuid",
        ),
        case(
            "bigchar (char)",
            of(ColumnType::BigChar, None),
            "string | Utf8 | native | exact | reads string,json,uuid",
        ),
        case(
            "nvarchar",
            of(ColumnType::NVarchar, None),
            "string | Utf8 | native | exact | reads string,json,uuid",
        ),
        case(
            "nchar",
            of(ColumnType::NChar, None),
            "string | Utf8 | native | exact | reads string,json,uuid",
        ),
        case(
            "text",
            of(ColumnType::Text, None),
            "string | Utf8 | native | exact | reads string,json,uuid",
        ),
        case(
            "ntext",
            of(ColumnType::NText, None),
            "string | Utf8 | native | exact | reads string,json,uuid",
        ),
        case(
            "ssvariant (sql_variant)",
            of(ColumnType::SSVariant, None),
            "string | Utf8 | native | exact | reads string,json,uuid",
        ),
        case(
            "udt",
            of(ColumnType::Udt, None),
            "string | Utf8 | native | exact | reads string,json,uuid",
        ),
        case(
            "xml",
            of(ColumnType::Xml, None),
            "unsupported(xml) | - | unsupported | unsupported | reads bool,int16,int32,int64,uint64,float32,float64,decimal(18,2),decimal(50,2),date,time(microsecond),timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,binary,json,uuid,enum,interval,list<int32>,unsupported(x)",
        ),
        case(
            "decimaln(18,2) (catalog hint)",
            of(ColumnType::Decimaln, Some(&decimal(18, 2))),
            "decimal(18,2) | Decimal128(18, 2) | native | exact | reads decimal(18,2)",
        ),
        case(
            "decimaln(38,10) (catalog hint)",
            of(ColumnType::Decimaln, Some(&decimal(38, 10))),
            "decimal(38,10) | Decimal128(38, 10) | native | exact | reads decimal(18,2)",
        ),
        case(
            "numericn(10,0) (catalog hint)",
            of(ColumnType::Numericn, Some(&decimal(10, 0))),
            "decimal(10,0) | Decimal128(10, 0) | native | exact | reads decimal(18,2)",
        ),
        case(
            "datetime2(7) (catalog hint)",
            of(ColumnType::Datetime2, Some(&fine())),
            "timestamp(nanosecond) | Timestamp(ns) | native | exact | reads timestamp(nanosecond)",
        ),
        case(
            "datetimeoffset(7) (catalog hint)",
            of(ColumnType::DatetimeOffsetn, Some(&fine())),
            "timestamp_tz(nanosecond,UTC) | Timestamp(ns, \"UTC\") | native | exact | reads timestamp_tz(nanosecond,UTC)",
        ),
        case(
            "time(7) (catalog hint)",
            of(ColumnType::Timen, Some(&fine_time())),
            "time(nanosecond) | Time64(ns) | native | exact | reads time(microsecond)",
        ),
    ]
}

/// Every SQL Server native type keeps the Rivet type, delivery, fidelity and override pairs pinned here.
#[test]
fn the_mssql_type_table_is_pinned() {
    let cases = table();
    let wrong = wrong_rows(Engine::Mssql, &cases);
    assert_eq!(
        wrong,
        Vec::<String>::new(),
        "{}",
        table_message(Engine::Mssql, cases.len(), &wrong)
    );
}
