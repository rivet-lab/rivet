//! Per-type table of the MySQL mapper: its Rivet type, delivery, fidelity and the override pairs read.

use mysql::consts::ColumnFlags;
use mysql::consts::ColumnType::{self, *};

use super::{mysql_mapping, mysql_native_type_name, mysql_type_to_rivet};
use crate::types::characterization::{Case, case, table_message, wrong_rows};
use crate::types::override_table::Engine;
use crate::types::{SourceColumn, TypeMapping};

const UNSIGNED: ColumnFlags = ColumnFlags::UNSIGNED_FLAG;
const ENUM_F: ColumnFlags = ColumnFlags::ENUM_FLAG;
const SET_F: ColumnFlags = ColumnFlags::SET_FLAG;
const BIN: u16 = 63;
const UTF8: u16 = 33;

/// The mapping the MySQL mapper gives a column of wire type `ty` with the given charset, length, flags and decimals.
fn of(ty: ColumnType, charset: u16, length: u32, flags: ColumnFlags, decimals: u8) -> TypeMapping {
    let col = mysql::Column::new(ty)
        .with_name(b"c")
        .with_character_set(charset)
        .with_column_length(length)
        .with_flags(flags)
        .with_decimals(decimals);
    let source = SourceColumn::simple("c", mysql_native_type_name(&col), true);
    mysql_mapping(&source, mysql_type_to_rivet(&col))
}

/// One row per wire type the MySQL mapper decides on, plus the decimal and bit width boundaries.
fn table() -> Vec<Case> {
    let none = ColumnFlags::empty();
    vec![
        case(
            "tinyint(1)",
            of(MYSQL_TYPE_TINY, UTF8, 1, none, 0),
            "bool | Boolean | native | exact | reads bool,int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "tinyint",
            of(MYSQL_TYPE_TINY, UTF8, 4, none, 0),
            "int16 | Int16 | native | exact | reads bool,int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "tinyint unsigned",
            of(MYSQL_TYPE_TINY, UTF8, 3, UNSIGNED, 0),
            "int16 | Int16 | native | exact | reads bool,int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "smallint",
            of(MYSQL_TYPE_SHORT, UTF8, 6, none, 0),
            "int16 | Int16 | native | exact | reads bool,int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "smallint unsigned",
            of(MYSQL_TYPE_SHORT, UTF8, 5, UNSIGNED, 0),
            "int32 | Int32 | native | exact | reads bool,int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "mediumint",
            of(MYSQL_TYPE_INT24, UTF8, 9, none, 0),
            "int32 | Int32 | native | exact | reads bool,int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "mediumint unsigned",
            of(MYSQL_TYPE_INT24, UTF8, 8, UNSIGNED, 0),
            "int64 | Int64 | native | exact | reads bool,int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "int",
            of(MYSQL_TYPE_LONG, UTF8, 11, none, 0),
            "int32 | Int32 | native | exact | reads bool,int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "int unsigned",
            of(MYSQL_TYPE_LONG, UTF8, 10, UNSIGNED, 0),
            "int64 | Int64 | native | exact | reads bool,int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "bigint",
            of(MYSQL_TYPE_LONGLONG, UTF8, 20, none, 0),
            "int64 | Int64 | native | exact | reads bool,int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "bigint unsigned",
            of(MYSQL_TYPE_LONGLONG, UTF8, 20, UNSIGNED, 0),
            "uint64 | UInt64 | native | exact | reads bool,int16,int32,int64,uint64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "year",
            of(MYSQL_TYPE_YEAR, UTF8, 4, none, 0),
            "int16 | Int16 | native | exact | reads bool,int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "float",
            of(MYSQL_TYPE_FLOAT, UTF8, 12, none, 31),
            "float32 | Float32 | native | exact | reads float32,float64,string,json",
        ),
        case(
            "double",
            of(MYSQL_TYPE_DOUBLE, UTF8, 22, none, 31),
            "float64 | Float64 | native | exact | reads float64,string,json",
        ),
        case(
            "decimal(10,2)",
            of(MYSQL_TYPE_NEWDECIMAL, UTF8, 12, none, 2),
            "decimal(10,2) | Decimal128(10, 2) | native | exact | reads float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "decimal(10,2) unsigned",
            of(MYSQL_TYPE_NEWDECIMAL, UTF8, 11, UNSIGNED, 2),
            "decimal(10,2) | Decimal128(10, 2) | native | exact | reads float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "decimal(10,0)",
            of(MYSQL_TYPE_NEWDECIMAL, UTF8, 11, none, 0),
            "decimal(10,0) | Decimal128(10, 0) | native | exact | reads int16,int32,int64,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "decimal old wire (10,0)",
            of(MYSQL_TYPE_DECIMAL, UTF8, 11, none, 0),
            "decimal(10,0) | Decimal128(10, 0) | native | exact | reads int16,int32,int64,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "decimal(38,10)",
            of(MYSQL_TYPE_NEWDECIMAL, UTF8, 40, none, 10),
            "decimal(38,10) | Decimal128(38, 10) | native | exact | reads float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "decimal(39,0)",
            of(MYSQL_TYPE_NEWDECIMAL, UTF8, 40, none, 0),
            "decimal(39,0) | Decimal256(39, 0) | native | exact | reads int16,int32,int64,float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "decimal(65,30)",
            of(MYSQL_TYPE_NEWDECIMAL, UTF8, 67, none, 30),
            "decimal(65,30) | Decimal256(65, 30) | native | exact | reads float64,decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "decimal(66,0) (no derivable precision)",
            of(MYSQL_TYPE_NEWDECIMAL, UTF8, 67, none, 0),
            "unsupported(decimal) | - | unsupported | unsupported | reads bool,int16,int32,int64,uint64,float32,float64,decimal(18,2),decimal(50,2),date,time(microsecond),timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,binary,json,uuid,enum,interval,list<int32>,unsupported(x)",
        ),
        case(
            "decimal(0) (no derivable precision)",
            of(MYSQL_TYPE_NEWDECIMAL, UTF8, 1, none, 0),
            "unsupported(decimal) | - | unsupported | unsupported | reads bool,int16,int32,int64,uint64,float32,float64,decimal(18,2),decimal(50,2),date,time(microsecond),timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,binary,json,uuid,enum,interval,list<int32>,unsupported(x)",
        ),
        case(
            "varchar",
            of(MYSQL_TYPE_VAR_STRING, UTF8, 255, none, 0),
            "string | Utf8 | native | exact | reads int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),date,timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json,uuid",
        ),
        case(
            "varchar (VARCHAR wire)",
            of(MYSQL_TYPE_VARCHAR, UTF8, 255, none, 0),
            "string | Utf8 | native | exact | reads int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),date,timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json,uuid",
        ),
        case(
            "varbinary",
            of(MYSQL_TYPE_VAR_STRING, BIN, 255, none, 0),
            "binary | Binary | native | exact | reads bool,string,binary,json,uuid",
        ),
        case(
            "char",
            of(MYSQL_TYPE_STRING, UTF8, 10, none, 0),
            "string | Utf8 | native | exact | reads int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),date,timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json,uuid",
        ),
        case(
            "binary",
            of(MYSQL_TYPE_STRING, BIN, 10, none, 0),
            "binary | Binary | native | exact | reads bool,string,binary,json,uuid",
        ),
        case(
            "enum",
            of(MYSQL_TYPE_STRING, UTF8, 10, ENUM_F, 0),
            "enum | Utf8 | native | compatible | reads string,binary,json,enum",
        ),
        case(
            "set",
            of(MYSQL_TYPE_STRING, UTF8, 10, SET_F, 0),
            "enum | Utf8 | native | compatible | reads string,binary,json,enum",
        ),
        case(
            "enum (ENUM wire)",
            of(MYSQL_TYPE_ENUM, UTF8, 10, none, 0),
            "enum | Utf8 | native | compatible | reads string,binary,json,enum",
        ),
        case(
            "set (SET wire)",
            of(MYSQL_TYPE_SET, UTF8, 10, none, 0),
            "enum | Utf8 | native | compatible | reads string,binary,json,enum",
        ),
        case(
            "blob",
            of(MYSQL_TYPE_BLOB, BIN, 65535, none, 0),
            "binary | Binary | native | exact | reads bool,string,binary,json,uuid",
        ),
        case(
            "text",
            of(MYSQL_TYPE_BLOB, UTF8, 65535, none, 0),
            "text | Utf8 | native | exact | reads int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),date,timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json,uuid",
        ),
        case(
            "tinyblob",
            of(MYSQL_TYPE_TINY_BLOB, BIN, 255, none, 0),
            "binary | Binary | native | exact | reads bool,string,binary,json,uuid",
        ),
        case(
            "mediumblob",
            of(MYSQL_TYPE_MEDIUM_BLOB, BIN, 16_777_215, none, 0),
            "binary | Binary | native | exact | reads bool,string,binary,json,uuid",
        ),
        case(
            "longblob",
            of(MYSQL_TYPE_LONG_BLOB, BIN, u32::MAX, none, 0),
            "binary | Binary | native | exact | reads bool,string,binary,json,uuid",
        ),
        case(
            "longtext",
            of(MYSQL_TYPE_LONG_BLOB, UTF8, u32::MAX, none, 0),
            "text | Utf8 | native | exact | reads int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),date,timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json,uuid",
        ),
        case(
            "json",
            of(MYSQL_TYPE_JSON, UTF8, u32::MAX, none, 0),
            "json | Utf8 | native | logical_string | reads string,binary,json",
        ),
        case(
            "date",
            of(MYSQL_TYPE_DATE, BIN, 10, none, 0),
            "date | Date32 | native | compatible | reads date,timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json",
        ),
        case(
            "time",
            of(MYSQL_TYPE_TIME, BIN, 10, none, 0),
            "string | Utf8 | text:time_beyond_day | logical_string | reads int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),date,timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json,uuid",
        ),
        case(
            "time (TIME2 wire)",
            of(MYSQL_TYPE_TIME2, BIN, 10, none, 0),
            "string | Utf8 | text:time_beyond_day | logical_string | reads int16,int32,int64,float32,float64,decimal(18,2),decimal(50,2),date,timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json,uuid",
        ),
        case(
            "datetime",
            of(MYSQL_TYPE_DATETIME, BIN, 19, none, 0),
            "timestamp(microsecond) | Timestamp(µs) | native | compatible | reads timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json",
        ),
        case(
            "datetime (DATETIME2 wire)",
            of(MYSQL_TYPE_DATETIME2, BIN, 19, none, 0),
            "timestamp(microsecond) | Timestamp(µs) | native | compatible | reads timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json",
        ),
        case(
            "timestamp",
            of(MYSQL_TYPE_TIMESTAMP, BIN, 19, none, 0),
            "timestamp_tz(microsecond,UTC) | Timestamp(µs, \"UTC\") | native | compatible | reads timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json",
        ),
        case(
            "timestamp (TIMESTAMP2 wire)",
            of(MYSQL_TYPE_TIMESTAMP2, BIN, 19, none, 0),
            "timestamp_tz(microsecond,UTC) | Timestamp(µs, \"UTC\") | native | compatible | reads timestamp(microsecond),timestamp_tz(microsecond,UTC),string,json",
        ),
        case(
            "bit(1)",
            of(MYSQL_TYPE_BIT, BIN, 1, UNSIGNED, 0),
            "bool | Boolean | native | exact | reads bool,int64,string,binary,json # under-change",
        ),
        case(
            "bit(16)",
            of(MYSQL_TYPE_BIT, BIN, 16, UNSIGNED, 0),
            "int64 | Int64 | native | exact | reads int64,string,binary,json # under-change",
        ),
        case(
            "bit(64)",
            of(MYSQL_TYPE_BIT, BIN, 64, UNSIGNED, 0),
            "uint64 | UInt64 | native | exact | reads int64,uint64,string,binary,json",
        ),
        case(
            "geometry",
            of(MYSQL_TYPE_GEOMETRY, BIN, 0, none, 0),
            "unsupported(geometry) | Utf8 | text:hex_wkb | logical_string | reads bool,int16,int32,int64,uint64,float32,float64,decimal(18,2),decimal(50,2),date,time(microsecond),timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,binary,json,uuid,enum,interval,list<int32>,unsupported(x)",
        ),
        case(
            "null",
            of(MYSQL_TYPE_NULL, BIN, 0, none, 0),
            "unsupported(unknown) | - | unsupported | unsupported | reads bool,int16,int32,int64,uint64,float32,float64,decimal(18,2),decimal(50,2),date,time(microsecond),timestamp(microsecond),timestamp_tz(microsecond,UTC),timestamp(nanosecond),timestamp_tz(nanosecond,UTC),string,binary,json,uuid,enum,interval,list<int32>,unsupported(x)",
        ),
    ]
}

/// Every MySQL wire type keeps the Rivet type, delivery, fidelity and override pairs pinned here.
#[test]
fn the_mysql_type_table_is_pinned() {
    let cases = table();
    let wrong = wrong_rows(Engine::Mysql, &cases);
    assert_eq!(
        wrong,
        Vec::<String>::new(),
        "{}",
        table_message(Engine::Mysql, cases.len(), &wrong)
    );
}
