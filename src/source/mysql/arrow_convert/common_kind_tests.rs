//! The MySQL mapper reads its common types through `CommonKind` and keeps its engine-only types in one table.

use mysql::consts::ColumnFlags;
use mysql::consts::ColumnType::{self, *};

use super::{
    BIT_INT64_MAX_WIDTH, is_blob_wire, mysql_common_kind, mysql_decimal, mysql_engine_type,
    mysql_type_to_rivet,
};
use crate::types::kind::CommonKind;
use crate::types::{ColumnOverrides, RivetType, TimeUnit as RivetTimeUnit};

const UNSIGNED: ColumnFlags = ColumnFlags::UNSIGNED_FLAG;
const ENUM_F: ColumnFlags = ColumnFlags::ENUM_FLAG;
const SET_F: ColumnFlags = ColumnFlags::SET_FLAG;
const BIN: u16 = 63;
const UTF8: u16 = 33;

/// One column descriptor: wire type, charset, length, flags and decimals.
type Spec = (ColumnType, u16, u32, ColumnFlags, u8);

fn column(spec: Spec) -> mysql::Column {
    let (ty, charset, length, flags, decimals) = spec;
    mysql::Column::new(ty)
        .with_name(b"c")
        .with_character_set(charset)
        .with_column_length(length)
        .with_flags(flags)
        .with_decimals(decimals)
}

fn ts(timezone: Option<&str>) -> RivetType {
    RivetType::Timestamp {
        unit: RivetTimeUnit::Microsecond,
        timezone: timezone.map(str::to_string),
    }
}

fn dec(precision: u8, scale: i8) -> RivetType {
    RivetType::Decimal { precision, scale }
}

fn time() -> RivetType {
    RivetType::Time {
        unit: RivetTimeUnit::Microsecond,
    }
}

fn unsupported_decimal() -> RivetType {
    RivetType::Unsupported {
        native_type: "decimal".into(),
        reason: "could not derive precision/scale from the MySQL column metadata; \
                 add a column override (columns: amount: decimal(18,2))"
            .into(),
    }
}

fn unsupported(native: &str) -> RivetType {
    RivetType::Unsupported {
        native_type: native.into(),
        reason: "no Rivet mapping for this MySQL type".into(),
    }
}

/// Every column shape the mapper decides on: its common kind and the type the mapper gives it.
fn table() -> Vec<(&'static str, Spec, Option<CommonKind>, RivetType)> {
    use CommonKind::*;
    let none = ColumnFlags::empty();
    let text = RivetType::Text;
    let string = RivetType::String;
    vec![
        (
            "tiny len 0",
            (MYSQL_TYPE_TINY, UTF8, 0, none, 0),
            Some(I16),
            RivetType::Int16,
        ),
        (
            "tiny len 1",
            (MYSQL_TYPE_TINY, UTF8, 1, none, 0),
            Some(Bool),
            RivetType::Bool,
        ),
        (
            "tiny len 1 unsigned",
            (MYSQL_TYPE_TINY, UTF8, 1, UNSIGNED, 0),
            Some(Bool),
            RivetType::Bool,
        ),
        (
            "tiny len 2",
            (MYSQL_TYPE_TINY, UTF8, 2, none, 0),
            Some(I16),
            RivetType::Int16,
        ),
        (
            "tiny unsigned",
            (MYSQL_TYPE_TINY, UTF8, 3, UNSIGNED, 0),
            Some(I16),
            RivetType::Int16,
        ),
        (
            "short",
            (MYSQL_TYPE_SHORT, UTF8, 6, none, 0),
            Some(I16),
            RivetType::Int16,
        ),
        (
            "short unsigned",
            (MYSQL_TYPE_SHORT, UTF8, 5, UNSIGNED, 0),
            Some(I32),
            RivetType::Int32,
        ),
        (
            "int24",
            (MYSQL_TYPE_INT24, UTF8, 9, none, 0),
            Some(I32),
            RivetType::Int32,
        ),
        (
            "int24 unsigned",
            (MYSQL_TYPE_INT24, UTF8, 8, UNSIGNED, 0),
            Some(I64),
            RivetType::Int64,
        ),
        (
            "long",
            (MYSQL_TYPE_LONG, UTF8, 11, none, 0),
            Some(I32),
            RivetType::Int32,
        ),
        (
            "long unsigned",
            (MYSQL_TYPE_LONG, UTF8, 10, UNSIGNED, 0),
            Some(I64),
            RivetType::Int64,
        ),
        (
            "longlong",
            (MYSQL_TYPE_LONGLONG, UTF8, 20, none, 0),
            Some(I64),
            RivetType::Int64,
        ),
        (
            "longlong unsigned",
            (MYSQL_TYPE_LONGLONG, UTF8, 20, UNSIGNED, 0),
            None,
            RivetType::UInt64,
        ),
        (
            "float",
            (MYSQL_TYPE_FLOAT, UTF8, 12, none, 31),
            Some(F32),
            RivetType::Float32,
        ),
        (
            "double",
            (MYSQL_TYPE_DOUBLE, UTF8, 22, none, 31),
            Some(F64),
            RivetType::Float64,
        ),
        (
            "newdecimal signed",
            (MYSQL_TYPE_NEWDECIMAL, UTF8, 12, none, 2),
            Some(Dec),
            dec(10, 2),
        ),
        (
            "newdecimal unsigned",
            (MYSQL_TYPE_NEWDECIMAL, UTF8, 11, UNSIGNED, 2),
            Some(Dec),
            dec(10, 2),
        ),
        (
            "newdecimal scale 0",
            (MYSQL_TYPE_NEWDECIMAL, UTF8, 11, none, 0),
            Some(Dec),
            dec(10, 0),
        ),
        (
            "newdecimal wide",
            (MYSQL_TYPE_NEWDECIMAL, UTF8, 67, none, 30),
            Some(Dec),
            dec(65, 30),
        ),
        (
            "newdecimal too wide",
            (MYSQL_TYPE_NEWDECIMAL, UTF8, 67, none, 0),
            Some(Dec),
            unsupported_decimal(),
        ),
        (
            "newdecimal empty",
            (MYSQL_TYPE_NEWDECIMAL, UTF8, 1, none, 0),
            Some(Dec),
            unsupported_decimal(),
        ),
        (
            "decimal old wire",
            (MYSQL_TYPE_DECIMAL, UTF8, 11, none, 0),
            Some(Dec),
            dec(10, 0),
        ),
        (
            "decimal old wire unsupported",
            (MYSQL_TYPE_DECIMAL, UTF8, 1, none, 0),
            Some(Dec),
            unsupported_decimal(),
        ),
        (
            "var_string utf8",
            (MYSQL_TYPE_VAR_STRING, UTF8, 255, none, 0),
            Some(Text),
            string.clone(),
        ),
        (
            "var_string charset 62",
            (MYSQL_TYPE_VAR_STRING, 62, 255, none, 0),
            Some(Text),
            string.clone(),
        ),
        (
            "var_string charset 64",
            (MYSQL_TYPE_VAR_STRING, 64, 255, none, 0),
            Some(Text),
            string.clone(),
        ),
        (
            "var_string binary",
            (MYSQL_TYPE_VAR_STRING, BIN, 255, none, 0),
            Some(Binary),
            RivetType::Binary,
        ),
        (
            "varchar utf8",
            (MYSQL_TYPE_VARCHAR, UTF8, 255, none, 0),
            Some(Text),
            string.clone(),
        ),
        (
            "varchar binary",
            (MYSQL_TYPE_VARCHAR, BIN, 255, none, 0),
            Some(Binary),
            RivetType::Binary,
        ),
        (
            "string utf8",
            (MYSQL_TYPE_STRING, UTF8, 10, none, 0),
            Some(Text),
            string.clone(),
        ),
        (
            "string binary",
            (MYSQL_TYPE_STRING, BIN, 10, none, 0),
            Some(Binary),
            RivetType::Binary,
        ),
        (
            "string enum flag",
            (MYSQL_TYPE_STRING, UTF8, 10, ENUM_F, 0),
            None,
            RivetType::Enum,
        ),
        (
            "string set flag",
            (MYSQL_TYPE_STRING, UTF8, 10, SET_F, 0),
            None,
            RivetType::Enum,
        ),
        (
            "string enum and set flags",
            (MYSQL_TYPE_STRING, UTF8, 10, ENUM_F | SET_F, 0),
            None,
            RivetType::Enum,
        ),
        (
            "string enum flag binary charset",
            (MYSQL_TYPE_STRING, BIN, 10, ENUM_F, 0),
            None,
            RivetType::Enum,
        ),
        (
            "var_string enum flag",
            (MYSQL_TYPE_VAR_STRING, UTF8, 10, ENUM_F, 0),
            None,
            RivetType::Enum,
        ),
        (
            "var_string set flag",
            (MYSQL_TYPE_VAR_STRING, UTF8, 10, SET_F, 0),
            None,
            RivetType::Enum,
        ),
        (
            "varchar enum flag",
            (MYSQL_TYPE_VARCHAR, UTF8, 10, ENUM_F, 0),
            None,
            RivetType::Enum,
        ),
        (
            "varchar set flag",
            (MYSQL_TYPE_VARCHAR, UTF8, 10, SET_F, 0),
            None,
            RivetType::Enum,
        ),
        (
            "enum wire",
            (MYSQL_TYPE_ENUM, UTF8, 10, none, 0),
            None,
            RivetType::Enum,
        ),
        (
            "set wire",
            (MYSQL_TYPE_SET, UTF8, 10, none, 0),
            None,
            RivetType::Enum,
        ),
        (
            "blob binary",
            (MYSQL_TYPE_BLOB, BIN, 65535, none, 0),
            Some(Binary),
            RivetType::Binary,
        ),
        (
            "blob text",
            (MYSQL_TYPE_BLOB, UTF8, 65535, none, 0),
            Some(Text),
            text.clone(),
        ),
        (
            "blob text charset 62",
            (MYSQL_TYPE_BLOB, 62, 65535, none, 0),
            Some(Text),
            text.clone(),
        ),
        (
            "blob text charset 64",
            (MYSQL_TYPE_BLOB, 64, 65535, none, 0),
            Some(Text),
            text.clone(),
        ),
        (
            "blob text with enum flag",
            (MYSQL_TYPE_BLOB, UTF8, 65535, ENUM_F, 0),
            Some(Text),
            text.clone(),
        ),
        (
            "blob text with set flag",
            (MYSQL_TYPE_BLOB, UTF8, 65535, SET_F, 0),
            Some(Text),
            text.clone(),
        ),
        (
            "tiny_blob binary",
            (MYSQL_TYPE_TINY_BLOB, BIN, 255, none, 0),
            Some(Binary),
            RivetType::Binary,
        ),
        (
            "tiny_blob text",
            (MYSQL_TYPE_TINY_BLOB, UTF8, 255, none, 0),
            Some(Text),
            text.clone(),
        ),
        (
            "medium_blob binary",
            (MYSQL_TYPE_MEDIUM_BLOB, BIN, 16_777_215, none, 0),
            Some(Binary),
            RivetType::Binary,
        ),
        (
            "medium_blob text",
            (MYSQL_TYPE_MEDIUM_BLOB, UTF8, 16_777_215, none, 0),
            Some(Text),
            text.clone(),
        ),
        (
            "long_blob binary",
            (MYSQL_TYPE_LONG_BLOB, BIN, u32::MAX, none, 0),
            Some(Binary),
            RivetType::Binary,
        ),
        (
            "long_blob text",
            (MYSQL_TYPE_LONG_BLOB, UTF8, u32::MAX, none, 0),
            Some(Text),
            text.clone(),
        ),
        (
            "json",
            (MYSQL_TYPE_JSON, UTF8, u32::MAX, none, 0),
            Some(Json),
            RivetType::Json,
        ),
        (
            "date",
            (MYSQL_TYPE_DATE, BIN, 10, none, 0),
            Some(Date),
            RivetType::Date,
        ),
        (
            "newdate",
            (MYSQL_TYPE_NEWDATE, BIN, 10, none, 0),
            Some(Date),
            RivetType::Date,
        ),
        (
            "datetime",
            (MYSQL_TYPE_DATETIME, BIN, 19, none, 0),
            Some(Naive),
            ts(None),
        ),
        (
            "datetime fsp 6",
            (MYSQL_TYPE_DATETIME, BIN, 26, none, 6),
            Some(Naive),
            ts(None),
        ),
        (
            "datetime2",
            (MYSQL_TYPE_DATETIME2, BIN, 19, none, 0),
            Some(Naive),
            ts(None),
        ),
        (
            "datetime2 fsp 3",
            (MYSQL_TYPE_DATETIME2, BIN, 23, none, 3),
            Some(Naive),
            ts(None),
        ),
        (
            "timestamp",
            (MYSQL_TYPE_TIMESTAMP, BIN, 19, none, 0),
            Some(Utc),
            ts(Some("UTC")),
        ),
        (
            "timestamp fsp 6",
            (MYSQL_TYPE_TIMESTAMP, BIN, 26, none, 6),
            Some(Utc),
            ts(Some("UTC")),
        ),
        (
            "timestamp2",
            (MYSQL_TYPE_TIMESTAMP2, BIN, 19, none, 0),
            Some(Utc),
            ts(Some("UTC")),
        ),
        (
            "timestamp2 fsp 3",
            (MYSQL_TYPE_TIMESTAMP2, BIN, 23, none, 3),
            Some(Utc),
            ts(Some("UTC")),
        ),
        ("time", (MYSQL_TYPE_TIME, BIN, 10, none, 0), None, time()),
        (
            "time fsp 6",
            (MYSQL_TYPE_TIME, BIN, 17, none, 6),
            None,
            time(),
        ),
        ("time2", (MYSQL_TYPE_TIME2, BIN, 10, none, 0), None, time()),
        (
            "bit len 0",
            (MYSQL_TYPE_BIT, BIN, 0, UNSIGNED, 0),
            None,
            RivetType::Int64,
        ),
        (
            "bit len 1",
            (MYSQL_TYPE_BIT, BIN, 1, UNSIGNED, 0),
            None,
            RivetType::Bool,
        ),
        (
            "bit len 2",
            (MYSQL_TYPE_BIT, BIN, 2, UNSIGNED, 0),
            None,
            RivetType::Int64,
        ),
        (
            "bit len 63",
            (MYSQL_TYPE_BIT, BIN, BIT_INT64_MAX_WIDTH, UNSIGNED, 0),
            None,
            RivetType::Int64,
        ),
        (
            "bit len 64",
            (MYSQL_TYPE_BIT, BIN, BIT_INT64_MAX_WIDTH + 1, UNSIGNED, 0),
            None,
            RivetType::UInt64,
        ),
        (
            "bit len 65",
            (MYSQL_TYPE_BIT, BIN, BIT_INT64_MAX_WIDTH + 2, UNSIGNED, 0),
            None,
            RivetType::UInt64,
        ),
        (
            "year",
            (MYSQL_TYPE_YEAR, UTF8, 4, none, 0),
            None,
            RivetType::Int16,
        ),
        (
            "geometry",
            (MYSQL_TYPE_GEOMETRY, BIN, 0, none, 0),
            None,
            unsupported("unknown"),
        ),
        (
            "null",
            (MYSQL_TYPE_NULL, BIN, 0, none, 0),
            None,
            unsupported("unknown"),
        ),
        (
            "typed array",
            (MYSQL_TYPE_TYPED_ARRAY, BIN, 0, none, 0),
            None,
            unsupported("unknown"),
        ),
    ]
}

/// Every column shape reads as its common kind, or as none for a type this engine alone carries.
#[test]
fn the_common_columns_read_as_their_common_kind_and_the_rest_as_none() {
    let table = table();
    assert_eq!(table.len(), 77);
    for (label, spec, kind, _) in &table {
        assert_eq!(&mysql_common_kind(&column(*spec)), kind, "{label}");
    }
}

/// Every column shape is typed as pinned, through its kind when it has one and the engine table when it has none.
#[test]
fn every_column_is_typed_through_its_kind_or_the_engine_table() {
    for (label, spec, kind, want) in table() {
        let col = column(spec);
        let got = mysql_type_to_rivet(&col);
        assert_eq!(got, want, "{label}");
        match kind {
            Some(CommonKind::Dec) => assert_eq!(got, mysql_decimal(&col), "{label}"),
            Some(CommonKind::Text) if is_blob_wire(&col) => {
                assert_eq!(got, RivetType::Text, "{label}");
            }
            Some(k) => assert_eq!(Some(got), k.rivet_type(), "{label} is the kind's own type"),
            None => assert_eq!(got, mysql_engine_type(&col), "{label}"),
        }
    }
}

/// Every column shape: one no common kind reads is delivered as a common type only where the engine declares it.
#[test]
fn no_column_shape_outside_the_common_kinds_is_delivered_as_a_common_type() {
    let rows = table()
        .into_iter()
        .map(|(label, _, kind, t)| (label.to_string(), kind, t));
    crate::types::kind::assert_engine_only_rows_are_declared(
        crate::types::override_table::Engine::Mysql,
        rows,
    );
}

/// Only the four `*BLOB` wire types are blobs; a text column of any other wire type is `String`.
#[test]
fn only_the_four_blob_wire_types_are_blobs() {
    let none = ColumnFlags::empty();
    for ty in [
        MYSQL_TYPE_TINY_BLOB,
        MYSQL_TYPE_MEDIUM_BLOB,
        MYSQL_TYPE_LONG_BLOB,
        MYSQL_TYPE_BLOB,
    ] {
        assert!(is_blob_wire(&column((ty, UTF8, 10, none, 0))), "{ty:?}");
    }
    for ty in [
        MYSQL_TYPE_VARCHAR,
        MYSQL_TYPE_VAR_STRING,
        MYSQL_TYPE_STRING,
        MYSQL_TYPE_JSON,
        MYSQL_TYPE_ENUM,
        MYSQL_TYPE_SET,
        MYSQL_TYPE_GEOMETRY,
    ] {
        assert!(!is_blob_wire(&column((ty, UTF8, 10, none, 0))), "{ty:?}");
    }
}

/// A text blob keeps its own `Text` label while its kind is the shared text kind, and both are `Utf8` on the wire.
#[test]
fn a_text_blob_keeps_its_label_and_shares_the_text_kind_and_arrow_type() {
    let blob = column((MYSQL_TYPE_BLOB, UTF8, 65535, ColumnFlags::empty(), 0));
    let varchar = column((MYSQL_TYPE_VARCHAR, UTF8, 255, ColumnFlags::empty(), 0));
    assert_eq!(mysql_common_kind(&blob), Some(CommonKind::Text));
    assert_eq!(mysql_common_kind(&varchar), Some(CommonKind::Text));
    assert_eq!(mysql_type_to_rivet(&blob), RivetType::Text);
    assert_eq!(mysql_type_to_rivet(&varchar), RivetType::String);
    assert_eq!(
        crate::types::rivet_type_to_arrow(&RivetType::Text),
        crate::types::rivet_type_to_arrow(&RivetType::String)
    );
}

/// An override names the type a column is read as, whatever its wire type reads as by default.
#[test]
fn an_override_wins_over_the_kind() {
    let col = column((MYSQL_TYPE_LONG, UTF8, 11, ColumnFlags::empty(), 0));
    let target = dec(18, 2);
    let overrides = ColumnOverrides::from([("c".to_string(), target.clone())]);
    let (schema, _) = super::mysql_schema_and_arrow_types(std::slice::from_ref(&col), &overrides)
        .expect("int to decimal is read");
    assert_eq!(
        schema.field(0).data_type(),
        &arrow::datatypes::DataType::Decimal128(18, 2)
    );
}
