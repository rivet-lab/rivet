//! Per-type table of the PostgreSQL mapper: its Rivet type, delivery, fidelity and the override pairs read.

use postgres::types::{Kind, Type};

use super::pg_type_to_rivet;
use crate::types::characterization::{Case, case, table_message, wrong_rows};
use crate::types::override_table::Engine;
use crate::types::{RivetType, SourceColumn, TypeMapping};

/// The mapping the PostgreSQL mapper gives a column of wire type `t`.
fn of(t: &Type) -> TypeMapping {
    TypeMapping::from_source(
        &SourceColumn::simple("c", t.name(), true),
        pg_type_to_rivet(t),
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
            "unsupported(numeric) | - | unsupported | unsupported | reads string,json,unsupported(x)",
        ),
        case(
            "date",
            of(&Type::DATE),
            "date | Date32 | native | exact | reads date",
        ),
        case(
            "time",
            of(&Type::TIME),
            "time(microsecond) | Time64(µs) | native | exact | reads time(microsecond)",
        ),
        case(
            "timestamp",
            of(&Type::TIMESTAMP),
            "timestamp(microsecond) | Timestamp(µs) | native | exact | reads timestamp(microsecond),timestamp_tz(microsecond,UTC)",
        ),
        case(
            "timestamptz",
            of(&Type::TIMESTAMPTZ),
            "timestamp_tz(microsecond,UTC) | Timestamp(µs, \"UTC\") | native | exact | reads timestamp(microsecond),timestamp_tz(microsecond,UTC)",
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
            "unsupported(char) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "money (unmapped)",
            of(&Type::MONEY),
            "unsupported(money) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "inet (unmapped)",
            of(&Type::INET),
            "unsupported(inet) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "cidr (unmapped)",
            of(&Type::CIDR),
            "unsupported(cidr) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "timetz (unmapped)",
            of(&Type::TIMETZ),
            "unsupported(timetz) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "macaddr (unmapped)",
            of(&Type::MACADDR),
            "unsupported(macaddr) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "macaddr8 (unmapped)",
            of(&Type::MACADDR8),
            "unsupported(macaddr8) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "bit (unmapped)",
            of(&Type::BIT),
            "unsupported(bit) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "varbit (unmapped)",
            of(&Type::VARBIT),
            "unsupported(varbit) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "xml (unmapped)",
            of(&Type::XML),
            "unsupported(xml) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "ts vector (unmapped)",
            of(&Type::TS_VECTOR),
            "unsupported(tsvector) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "tsquery (unmapped)",
            of(&Type::TSQUERY),
            "unsupported(tsquery) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "point (unmapped)",
            of(&Type::POINT),
            "unsupported(point) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "line (unmapped)",
            of(&Type::LINE),
            "unsupported(line) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "lseg (unmapped)",
            of(&Type::LSEG),
            "unsupported(lseg) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "box (unmapped)",
            of(&Type::BOX),
            "unsupported(box) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "path (unmapped)",
            of(&Type::PATH),
            "unsupported(path) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "polygon (unmapped)",
            of(&Type::POLYGON),
            "unsupported(polygon) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "circle (unmapped)",
            of(&Type::CIRCLE),
            "unsupported(circle) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "int4 range (unmapped)",
            of(&Type::INT4_RANGE),
            "unsupported(int4range) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "int8 range (unmapped)",
            of(&Type::INT8_RANGE),
            "unsupported(int8range) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "num range (unmapped)",
            of(&Type::NUM_RANGE),
            "unsupported(numrange) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "date range (unmapped)",
            of(&Type::DATE_RANGE),
            "unsupported(daterange) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "ts range (unmapped)",
            of(&Type::TS_RANGE),
            "unsupported(tsrange) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "tstz range (unmapped)",
            of(&Type::TSTZ_RANGE),
            "unsupported(tstzrange) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "int4multi range (unmapped)",
            of(&Type::INT4MULTI_RANGE),
            "unsupported(int4multirange) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "regclass (unmapped)",
            of(&Type::REGCLASS),
            "unsupported(regclass) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "regtype (unmapped)",
            of(&Type::REGTYPE),
            "unsupported(regtype) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "xid (unmapped)",
            of(&Type::XID),
            "unsupported(xid) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "cid (unmapped)",
            of(&Type::CID),
            "unsupported(cid) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "xid8 (unmapped)",
            of(&Type::XID8),
            "unsupported(xid8) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "pg lsn (unmapped)",
            of(&Type::PG_LSN),
            "unsupported(pg_lsn) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "tid (unmapped)",
            of(&Type::TID),
            "unsupported(tid) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "unknown (unmapped)",
            of(&Type::UNKNOWN),
            "unsupported(unknown) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "composite rivet_pair",
            of(&composite),
            "unsupported(rivet_pair) | - | unsupported | unsupported | reads string,json,unsupported(x) # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "bool[]",
            of(&Type::BOOL_ARRAY),
            "list<bool> | List(Boolean) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "int2[]",
            of(&Type::INT2_ARRAY),
            "list<int16> | List(Int16) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "int4[]",
            of(&Type::INT4_ARRAY),
            "list<int32> | List(Int32) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "int8[]",
            of(&Type::INT8_ARRAY),
            "list<int64> | List(Int64) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "float4[]",
            of(&Type::FLOAT4_ARRAY),
            "list<float32> | List(Float32) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "float8[]",
            of(&Type::FLOAT8_ARRAY),
            "list<float64> | List(Float64) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "text[]",
            of(&Type::TEXT_ARRAY),
            "list<string> | List(Utf8) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "varchar[]",
            of(&Type::VARCHAR_ARRAY),
            "list<string> | List(Utf8) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "bpchar[]",
            of(&Type::BPCHAR_ARRAY),
            "list<string> | List(Utf8) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "date[]",
            of(&Type::DATE_ARRAY),
            "list<date> | List(Date32) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "time[]",
            of(&Type::TIME_ARRAY),
            "list<time(microsecond)> | List(Time64(µs)) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "timestamp[]",
            of(&Type::TIMESTAMP_ARRAY),
            "list<timestamp(microsecond)> | List(Timestamp(µs)) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "timestamptz[]",
            of(&Type::TIMESTAMPTZ_ARRAY),
            "list<timestamp_tz(microsecond,UTC)> | List(Timestamp(µs, \"UTC\")) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "uuid[]",
            of(&Type::UUID_ARRAY),
            "list<uuid> | List(FixedSizeBinary(16), metadata: {\"ARROW:extension:name\": \"arrow.uuid\"}) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "bytea[]",
            of(&Type::BYTEA_ARRAY),
            "list<binary> | List(Binary) | native | compatible | reads list<int32> # red:PG_NULL_LIST_EMPTY_BQ,PG_NULL_LIST_EMPTY_CH",
        ),
        case(
            "numeric[]",
            of(&Type::NUMERIC_ARRAY),
            "list<unsupported(numeric)> | - | unsupported | unsupported | reads list<int32>",
        ),
        case(
            "json[]",
            of(&Type::JSON_ARRAY),
            "list<json> | List(Utf8) | native | compatible | reads list<int32>",
        ),
        case(
            "jsonb[]",
            of(&Type::JSONB_ARRAY),
            "list<json> | List(Utf8) | native | compatible | reads list<int32>",
        ),
        case(
            "interval[]",
            of(&Type::INTERVAL_ARRAY),
            "list<interval> | List(Utf8) | native | compatible | reads list<int32>",
        ),
        case(
            "oid[]",
            of(&Type::OID_ARRAY),
            "list<int64> | List(Int64) | native | compatible | reads list<int32>",
        ),
        case(
            "inet[]",
            of(&Type::INET_ARRAY),
            "list<unsupported(inet)> | - | unsupported | unsupported | reads list<int32> # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "money[]",
            of(&Type::MONEY_ARRAY),
            "list<unsupported(money)> | - | unsupported | unsupported | reads list<int32> # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "int4_range[]",
            of(&Type::INT4_RANGE_ARRAY),
            "list<unsupported(int4range)> | - | unsupported | unsupported | reads list<int32> # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "point[]",
            of(&Type::POINT_ARRAY),
            "list<unsupported(point)> | - | unsupported | unsupported | reads list<int32> # red:PG_UNMAPPED_TYPE",
        ),
        case(
            "rivet_status[]",
            of(&earr),
            "list<enum> | List(Utf8) | native | compatible | reads list<int32>",
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
