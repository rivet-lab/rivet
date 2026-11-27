//! Per-type table of the PostgreSQL mapper: its Rivet type, delivery, fidelity and the override pairs read.

use postgres::types::{Kind, Type};

use super::{pg_delivery, pg_type_to_rivet};
use crate::types::characterization::{Case, case, table_message, wrong_rows};
use crate::types::override_table::Engine;
use crate::types::{RivetType, SourceColumn, TypeMapping};

/// The mapping the PostgreSQL mapper gives a column of wire type `t`.
fn of(t: &Type) -> TypeMapping {
    pg_delivery(
        TypeMapping::from_source(
            &SourceColumn::simple("c", t.name(), true),
            pg_type_to_rivet(t),
        ),
        false,
    )
}

/// The mapping of a `numeric` column whose precision and scale the catalog hint supplies.
fn hinted(precision: u8, scale: i8) -> TypeMapping {
    TypeMapping::from_source(
        &SourceColumn::simple("c", "numeric", true),
        RivetType::Decimal { precision, scale },
    )
}

/// A user-defined enum type and the array of it.
fn enum_types() -> (Type, Type) {
    let labels = Kind::Enum(vec!["a".into(), "b".into()]);
    let e = Type::new("rivet_status".into(), 90_001, labels, "public".into());
    let arr = Type::new(
        "_rivet_status".into(),
        90_002,
        Kind::Array(e.clone()),
        "public".into(),
    );
    (e, arr)
}

/// One row per native type the PostgreSQL mapper decides on, with the override pairs the table reads.
fn table() -> Vec<Case> {
    let (e, earr) = enum_types();
    let composite = Type::new(
        "rivet_pair".into(),
        90_003,
        Kind::Composite(Vec::new()),
        "public".into(),
    );
    vec![
        case(
            "bool",
            of(&Type::BOOL),
            "bool | Boolean | native | exact | reads bool",
        ),
        case(
            "int2",
            of(&Type::INT2),
            "int16 | Int16 | native | exact | reads int16,int32,int64,decimal(18,2),decimal(50,2)",
        ),
        case(
            "int4",
            of(&Type::INT4),
            "int32 | Int32 | native | exact | reads int16,int32,int64,decimal(18,2),decimal(50,2)",
        ),
        case(
            "int8",
            of(&Type::INT8),
            "int64 | Int64 | native | exact | reads int16,int32,int64,decimal(18,2),decimal(50,2)",
        ),
        case(
            "oid",
            of(&Type::OID),
            "int64 | Int64 | native | exact | reads int16,int32,int64,decimal(18,2),decimal(50,2)",
        ),
        case(
            "float4",
            of(&Type::FLOAT4),
            "float32 | Float32 | native | exact | reads float32",
        ),
        case(
            "float8",
            of(&Type::FLOAT8),
            "float64 | Float64 | native | exact | reads float64",
        ),
        case(
            "numeric (no catalog hint)",
            of(&Type::NUMERIC),
            "unsupported(numeric) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "date",
            of(&Type::DATE),
            "date | Date32 | native | exact | reads date,string",
        ),
        case(
            "time",
            of(&Type::TIME),
            "time(microsecond) | Time64(µs) | native | exact | reads time(microsecond),string",
        ),
        case(
            "timestamp",
            of(&Type::TIMESTAMP),
            "timestamp(microsecond) | Timestamp(µs) | native | exact | reads timestamp(microsecond),timestamp_tz(microsecond,UTC),string",
        ),
        case(
            "timestamptz",
            of(&Type::TIMESTAMPTZ),
            "timestamp_tz(microsecond,UTC) | Timestamp(µs, \"UTC\") | native | exact | reads timestamp(microsecond),timestamp_tz(microsecond,UTC),string",
        ),
        case(
            "text",
            of(&Type::TEXT),
            "string | Utf8 | native | exact | reads string,json",
        ),
        case(
            "varchar",
            of(&Type::VARCHAR),
            "string | Utf8 | native | exact | reads string,json",
        ),
        case(
            "bpchar",
            of(&Type::BPCHAR),
            "string | Utf8 | native | exact | reads string,json",
        ),
        case(
            "name",
            of(&Type::NAME),
            "string | Utf8 | native | exact | reads string,json",
        ),
        case(
            "bytea",
            of(&Type::BYTEA),
            "binary | Binary | native | exact | reads binary",
        ),
        case(
            "json",
            of(&Type::JSON),
            "json | Utf8 | native | logical_string | reads string,json",
        ),
        case(
            "jsonb",
            of(&Type::JSONB),
            "json | Utf8 | native | logical_string | reads string,json",
        ),
        case(
            "uuid",
            of(&Type::UUID),
            "uuid | FixedSizeBinary(16) | native | exact | reads string,json,uuid",
        ),
        case(
            "interval",
            of(&Type::INTERVAL),
            "interval | Utf8 | text:iso8601_duration | logical_string | reads string,json,interval",
        ),
        case(
            "numeric(18,2) (catalog hint)",
            hinted(18, 2),
            "decimal(18,2) | Decimal128(18, 2) | native | exact | reads decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "numeric(38,0) (catalog hint)",
            hinted(38, 0),
            "decimal(38,0) | Decimal128(38, 0) | native | exact | reads decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "numeric(40,10) (catalog hint)",
            hinted(40, 10),
            "decimal(40,10) | Decimal256(40, 10) | native | exact | reads decimal(18,2),decimal(50,2),string,json",
        ),
        case(
            "enum rivet_status",
            of(&e),
            "enum | Utf8 | native | compatible | reads string,json,enum",
        ),
        case(
            "char (unmapped)",
            of(&Type::CHAR),
            "unsupported(char) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "money (unmapped)",
            of(&Type::MONEY),
            "unsupported(money) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "inet (unmapped)",
            of(&Type::INET),
            "unsupported(inet) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "cidr (unmapped)",
            of(&Type::CIDR),
            "unsupported(cidr) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "timetz (unmapped)",
            of(&Type::TIMETZ),
            "unsupported(timetz) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "macaddr (unmapped)",
            of(&Type::MACADDR),
            "unsupported(macaddr) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "macaddr8 (unmapped)",
            of(&Type::MACADDR8),
            "unsupported(macaddr8) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "bit (unmapped)",
            of(&Type::BIT),
            "unsupported(bit) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "varbit (unmapped)",
            of(&Type::VARBIT),
            "unsupported(varbit) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "xml (unmapped)",
            of(&Type::XML),
            "unsupported(xml) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "ts vector (unmapped)",
            of(&Type::TS_VECTOR),
            "unsupported(tsvector) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "tsquery (unmapped)",
            of(&Type::TSQUERY),
            "unsupported(tsquery) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "point (unmapped)",
            of(&Type::POINT),
            "unsupported(point) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "line (unmapped)",
            of(&Type::LINE),
            "unsupported(line) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "lseg (unmapped)",
            of(&Type::LSEG),
            "unsupported(lseg) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "box (unmapped)",
            of(&Type::BOX),
            "unsupported(box) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "path (unmapped)",
            of(&Type::PATH),
            "unsupported(path) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "polygon (unmapped)",
            of(&Type::POLYGON),
            "unsupported(polygon) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "circle (unmapped)",
            of(&Type::CIRCLE),
            "unsupported(circle) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "int4 range (unmapped)",
            of(&Type::INT4_RANGE),
            "unsupported(int4range) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "int8 range (unmapped)",
            of(&Type::INT8_RANGE),
            "unsupported(int8range) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "num range (unmapped)",
            of(&Type::NUM_RANGE),
            "unsupported(numrange) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "date range (unmapped)",
            of(&Type::DATE_RANGE),
            "unsupported(daterange) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "ts range (unmapped)",
            of(&Type::TS_RANGE),
            "unsupported(tsrange) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "tstz range (unmapped)",
            of(&Type::TSTZ_RANGE),
            "unsupported(tstzrange) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "int4multi range (unmapped)",
            of(&Type::INT4MULTI_RANGE),
            "unsupported(int4multirange) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "regclass (unmapped)",
            of(&Type::REGCLASS),
            "unsupported(regclass) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "regtype (unmapped)",
            of(&Type::REGTYPE),
            "unsupported(regtype) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "xid (unmapped)",
            of(&Type::XID),
            "unsupported(xid) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "cid (unmapped)",
            of(&Type::CID),
            "unsupported(cid) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "xid8 (unmapped)",
            of(&Type::XID8),
            "unsupported(xid8) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "pg lsn (unmapped)",
            of(&Type::PG_LSN),
            "unsupported(pg_lsn) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "tid (unmapped)",
            of(&Type::TID),
            "unsupported(tid) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "unknown (unmapped)",
            of(&Type::UNKNOWN),
            "unsupported(unknown) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "composite rivet_pair",
            of(&composite),
            "unsupported(rivet_pair) | Utf8 | text:server_text | logical_string | reads string,json,unsupported(x)",
        ),
        case(
            "bool[]",
            of(&Type::BOOL_ARRAY),
            "list<bool> | List(Boolean) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "int2[]",
            of(&Type::INT2_ARRAY),
            "list<int16> | List(Int16) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "int4[]",
            of(&Type::INT4_ARRAY),
            "list<int32> | List(Int32) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "int8[]",
            of(&Type::INT8_ARRAY),
            "list<int64> | List(Int64) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "float4[]",
            of(&Type::FLOAT4_ARRAY),
            "list<float32> | List(Float32) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "float8[]",
            of(&Type::FLOAT8_ARRAY),
            "list<float64> | List(Float64) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "text[]",
            of(&Type::TEXT_ARRAY),
            "list<string> | List(Utf8) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "varchar[]",
            of(&Type::VARCHAR_ARRAY),
            "list<string> | List(Utf8) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "bpchar[]",
            of(&Type::BPCHAR_ARRAY),
            "list<string> | List(Utf8) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "date[]",
            of(&Type::DATE_ARRAY),
            "list<date> | List(Date32) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "time[]",
            of(&Type::TIME_ARRAY),
            "list<time(microsecond)> | List(Time64(µs)) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "timestamp[]",
            of(&Type::TIMESTAMP_ARRAY),
            "list<timestamp(microsecond)> | List(Timestamp(µs)) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "timestamptz[]",
            of(&Type::TIMESTAMPTZ_ARRAY),
            "list<timestamp_tz(microsecond,UTC)> | List(Timestamp(µs, \"UTC\")) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "uuid[]",
            of(&Type::UUID_ARRAY),
            "list<uuid> | List(FixedSizeBinary(16), metadata: {\"ARROW:extension:name\": \"arrow.uuid\"}) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "bytea[]",
            of(&Type::BYTEA_ARRAY),
            "list<binary> | List(Binary) | native | compatible | reads string,list<int32> # red:PG_NULL_LIST_EMPTY_BQ",
        ),
        case(
            "numeric[]",
            of(&Type::NUMERIC_ARRAY),
            "list<unsupported(numeric)> | Utf8 | text:server_text | logical_string | reads string,list<int32>",
        ),
        case(
            "json[]",
            of(&Type::JSON_ARRAY),
            "list<json> | List(Utf8) | native | compatible | reads string,list<int32>",
        ),
        case(
            "jsonb[]",
            of(&Type::JSONB_ARRAY),
            "list<json> | List(Utf8) | native | compatible | reads string,list<int32>",
        ),
        case(
            "interval[]",
            of(&Type::INTERVAL_ARRAY),
            "list<interval> | List(Utf8) | native | compatible | reads string,list<int32>",
        ),
        case(
            "oid[]",
            of(&Type::OID_ARRAY),
            "list<int64> | List(Int64) | native | compatible | reads string,list<int32>",
        ),
        case(
            "inet[]",
            of(&Type::INET_ARRAY),
            "list<unsupported(inet)> | Utf8 | text:server_text | logical_string | reads string,list<int32>",
        ),
        case(
            "money[]",
            of(&Type::MONEY_ARRAY),
            "list<unsupported(money)> | Utf8 | text:server_text | logical_string | reads string,list<int32>",
        ),
        case(
            "int4_range[]",
            of(&Type::INT4_RANGE_ARRAY),
            "list<unsupported(int4range)> | Utf8 | text:server_text | logical_string | reads string,list<int32>",
        ),
        case(
            "point[]",
            of(&Type::POINT_ARRAY),
            "list<unsupported(point)> | Utf8 | text:server_text | logical_string | reads string,list<int32>",
        ),
        case(
            "rivet_status[]",
            of(&earr),
            "list<enum> | List(Utf8) | native | compatible | reads string,list<int32>",
        ),
    ]
}

/// Every PostgreSQL native type keeps the Rivet type, delivery, fidelity and override pairs pinned here.
#[test]
fn the_postgres_type_table_is_pinned() {
    let cases = table();
    let wrong = wrong_rows(Engine::Postgres, &cases);
    assert_eq!(
        wrong,
        Vec::<String>::new(),
        "{}",
        table_message(Engine::Postgres, cases.len(), &wrong)
    );
}
