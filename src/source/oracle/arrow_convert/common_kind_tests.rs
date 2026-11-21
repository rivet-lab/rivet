//! The Oracle mapper reads its common types through `CommonKind` and keeps its engine-only types in one table.

use super::super::kind::OraKind;
use super::{
    autodetect, deliver_number_text, number_decimal, number_kind, oracle_common_kind,
    oracle_engine_type,
};
use crate::types::kind::CommonKind;
use crate::types::{
    ColumnOverrides, Delivery, RivetType, SourceColumn, TextForm, TimeUnit, TypeMapping,
};

fn ts(timezone: Option<&str>) -> RivetType {
    RivetType::Timestamp {
        unit: TimeUnit::Microsecond,
        timezone: timezone.map(str::to_string),
    }
}

fn dec(precision: u8, scale: i8) -> RivetType {
    RivetType::Decimal { precision, scale }
}

fn unsupported(label: &str) -> RivetType {
    RivetType::Unsupported {
        native_type: label.into(),
        reason: format!(
            "Oracle column type {label} has no Rivet mapping; select a convertible \
             expression of it in a `query:`, or drop it"
        ),
    }
}

/// One column shape: kind, declared native type, precision, scale, driver label.
type Spec = (OraKind, &'static str, u8, i8, &'static str);

/// Every column shape the mapper decides on: its common kind and the type the mapper gives it.
fn table() -> Vec<(&'static str, Spec, Option<CommonKind>, RivetType)> {
    use CommonKind::*;
    use OraKind as K;
    vec![
        (
            "number(1,0)",
            (K::Number, "number(1,0)", 1, 0, "number"),
            Some(I32),
            RivetType::Int32,
        ),
        (
            "number(9,0)",
            (K::Number, "number(9,0)", 9, 0, "number"),
            Some(I32),
            RivetType::Int32,
        ),
        (
            "number(10,0)",
            (K::Number, "number(10,0)", 10, 0, "number"),
            Some(I64),
            RivetType::Int64,
        ),
        (
            "number(18,0)",
            (K::Number, "number(18,0)", 18, 0, "number"),
            Some(I64),
            RivetType::Int64,
        ),
        (
            "number(19,0)",
            (K::Number, "number(19,0)", 19, 0, "number"),
            Some(Dec),
            dec(19, 0),
        ),
        (
            "number(38,0)",
            (K::Number, "number(38,0)", 38, 0, "number"),
            Some(Dec),
            dec(38, 0),
        ),
        (
            "number(12,2)",
            (K::Number, "number(12,2)", 12, 2, "number"),
            Some(Dec),
            dec(12, 2),
        ),
        (
            "number(3,5)",
            (K::Number, "number(3,5)", 3, 5, "number"),
            Some(Dec),
            dec(5, 5),
        ),
        (
            "number(10,38)",
            (K::Number, "number(10,38)", 10, 38, "number"),
            Some(Dec),
            dec(38, 38),
        ),
        (
            "number(10,39)",
            (K::Number, "number(10,39)", 10, 39, "number"),
            Some(Text),
            RivetType::String,
        ),
        (
            "number(5,-2)",
            (K::Number, "number(5,-2)", 5, -2, "number"),
            Some(Dec),
            dec(7, 0),
        ),
        (
            "number(36,-2)",
            (K::Number, "number(36,-2)", 36, -2, "number"),
            Some(Dec),
            dec(38, 0),
        ),
        (
            "number(37,-2)",
            (K::Number, "number(37,-2)", 37, -2, "number"),
            Some(Text),
            RivetType::String,
        ),
        (
            "number(255,-126)",
            (K::Number, "number(255,-126)", 255, -126, "number"),
            Some(Text),
            RivetType::String,
        ),
        (
            "number bare",
            (K::Number, "number", 0, -127, "number"),
            Some(Text),
            RivetType::String,
        ),
        (
            "number(0,0)",
            (K::Number, "number(0,0)", 0, 0, "number"),
            Some(Text),
            RivetType::String,
        ),
        (
            "float(126)",
            (K::Number, "number", 126, -127, "number"),
            Some(Text),
            RivetType::String,
        ),
        (
            "binary_float",
            (K::BinaryFloat, "binary_float", 0, 0, "binary_float"),
            Some(F32),
            RivetType::Float32,
        ),
        (
            "binary_double",
            (K::BinaryDouble, "binary_double", 0, 0, "binary_double"),
            Some(F64),
            RivetType::Float64,
        ),
        (
            "boolean",
            (K::Boolean, "boolean", 0, 0, "boolean"),
            Some(Bool),
            RivetType::Bool,
        ),
        (
            "date",
            (K::Date, "date", 0, 0, "date"),
            Some(Naive),
            ts(None),
        ),
        (
            "timestamp(0)",
            (K::Timestamp, "timestamp(0)", 0, 0, "timestamp"),
            Some(Naive),
            ts(None),
        ),
        (
            "timestamp(9)",
            (K::Timestamp, "timestamp(9)", 0, 9, "timestamp"),
            Some(Naive),
            ts(None),
        ),
        (
            "timestamp_tz(6) re-read",
            (K::Timestamp, "timestamp_tz(6)", 0, 6, "timestamp"),
            Some(Utc),
            ts(Some("UTC")),
        ),
        (
            "timestamp_ltz(6) re-read",
            (K::Timestamp, "timestamp_ltz(6)", 0, 6, "timestamp"),
            Some(Utc),
            ts(Some("UTC")),
        ),
        (
            "timestamp_tz(6) described",
            (K::TimestampTz, "timestamp_tz(6)", 0, 6, "timestamp_tz"),
            Some(Utc),
            ts(Some("UTC")),
        ),
        (
            "timestamp_ltz(6) described",
            (K::TimestampLtz, "timestamp_ltz(6)", 0, 6, "timestamp_ltz"),
            Some(Utc),
            ts(Some("UTC")),
        ),
        (
            "interval ds",
            (K::Text, "interval_ds", 0, 6, "interval_ds"),
            None,
            RivetType::Interval,
        ),
        (
            "interval ym",
            (K::Text, "interval_ym", 0, 0, "interval_ym"),
            None,
            RivetType::Interval,
        ),
        (
            "interval described",
            (K::IntervalDs, "interval_ds", 0, 6, "interval_ds"),
            None,
            RivetType::Interval,
        ),
        (
            "varchar2",
            (K::Text, "varchar", 0, 0, "varchar"),
            Some(Text),
            RivetType::String,
        ),
        (
            "nchar",
            (K::Text, "nchar", 0, 0, "nchar"),
            Some(Text),
            RivetType::String,
        ),
        (
            "long",
            (K::Text, "long", 0, 0, "long"),
            Some(Text),
            RivetType::String,
        ),
        (
            "clob",
            (K::Clob, "clob", 0, 0, "clob"),
            Some(Text),
            RivetType::String,
        ),
        (
            "nclob",
            (K::Clob, "nclob", 0, 0, "nclob"),
            Some(Text),
            RivetType::String,
        ),
        (
            "rowid re-read",
            (K::Text, "rowid", 0, 0, "rowid"),
            Some(Text),
            RivetType::String,
        ),
        (
            "xmltype re-read",
            (K::Clob, "object", 0, 0, "object"),
            Some(Text),
            RivetType::String,
        ),
        (
            "vector re-read",
            (K::Clob, "vector", 0, 0, "vector"),
            Some(Text),
            RivetType::String,
        ),
        (
            "json re-read",
            (K::Clob, "json", 0, 0, "json"),
            Some(Json),
            RivetType::Json,
        ),
        (
            "json described",
            (K::Json, "json", 0, 0, "json"),
            Some(Json),
            RivetType::Json,
        ),
        (
            "raw",
            (K::Raw, "raw", 0, 0, "raw"),
            Some(Binary),
            RivetType::Binary,
        ),
        (
            "long raw",
            (K::Raw, "long_raw", 0, 0, "long_raw"),
            Some(Binary),
            RivetType::Binary,
        ),
        (
            "blob",
            (K::Blob, "blob", 0, 0, "blob"),
            Some(Binary),
            RivetType::Binary,
        ),
        (
            "bfile",
            (K::Other, "bfile", 0, 0, "bfile"),
            None,
            unsupported("bfile"),
        ),
        (
            "rowid described",
            (K::Rowid, "rowid", 0, 0, "rowid"),
            None,
            unsupported("rowid"),
        ),
        (
            "object described",
            (K::Object, "object", 0, 0, "object"),
            None,
            unsupported("object"),
        ),
        (
            "vector described",
            (K::Vector, "vector", 0, 0, "vector"),
            None,
            unsupported("vector"),
        ),
    ]
}

/// Every column shape reads as its common kind, or as none for a type this engine alone carries.
#[test]
fn the_common_columns_read_as_their_common_kind_and_the_rest_as_none() {
    let table = table();
    assert_eq!(table.len(), 47);
    for (label, (kind, native, p, s, _), want, _) in &table {
        assert_eq!(&oracle_common_kind(*kind, native, *p, *s), want, "{label}");
    }
}

/// Every column shape is typed as pinned, through its kind when it has one and the engine table when it has none.
#[test]
fn every_column_is_typed_through_its_kind_or_the_engine_table() {
    for (label, (kind, native, p, s, lbl), common, want) in table() {
        let got = autodetect(kind, native, p, s, lbl);
        assert_eq!(got, want, "{label}");
        match common {
            Some(CommonKind::Dec) => assert_eq!(got, number_decimal(p, s), "{label}"),
            Some(k) => assert_eq!(Some(got), k.rivet_type(), "{label} is the kind's own type"),
            None => assert_eq!(got, oracle_engine_type(native, lbl), "{label}"),
        }
    }
}

/// The engine table keeps the interval and names every other unmapped type by its label.
#[test]
fn the_engine_table_holds_the_interval_and_the_unmapped_types() {
    assert_eq!(oracle_engine_type("interval_ds", "x"), RivetType::Interval);
    assert_eq!(oracle_engine_type("interval_ym", "x"), RivetType::Interval);
    assert_eq!(oracle_engine_type("bfile", "bfile"), unsupported("bfile"));
    assert_eq!(oracle_engine_type("bfile", "lbl"), unsupported("lbl"));
}

/// NUMBER(p,s) picks its kind at each edge: 9 and 10 digits, 18 and 19, scale 38 and 39, a negative scale that fits 38 digits and one that does not.
#[test]
fn a_number_reads_as_the_narrowest_kind_at_every_edge() {
    use CommonKind::*;
    for (p, s, want) in [
        (0, 0, Text),
        (0, 5, Text),
        (1, 0, I32),
        (9, 0, I32),
        (10, 0, I64),
        (18, 0, I64),
        (19, 0, Dec),
        (38, 0, Dec),
        (255, 0, Dec),
        (1, 1, Dec),
        (38, 38, Dec),
        (10, 38, Dec),
        (10, 39, Text),
        (10, 127, Text),
        (1, -1, Dec),
        (37, -1, Dec),
        (38, -1, Text),
        (1, -37, Dec),
        (1, -38, Text),
        (255, -1, Text),
        (255, -126, Text),
        (0, -1, Text),
        (1, -127, Text),
        (126, -127, Text),
        (10, -128, Text),
    ] {
        assert_eq!(number_kind(p, s), want, "number({p},{s})");
    }
}

/// A `Dec` number is delivered as the decimal its precision and scale name; a negative scale widens it into an integer decimal.
#[test]
fn a_decimal_number_is_delivered_at_its_own_precision_and_scale() {
    assert_eq!(number_decimal(12, 2), dec(12, 2));
    assert_eq!(number_decimal(3, 5), dec(5, 5));
    assert_eq!(number_decimal(19, 0), dec(19, 0));
    assert_eq!(number_decimal(5, -2), dec(7, 0));
    assert_eq!(number_decimal(37, -1), dec(38, 0));
    assert_eq!(CommonKind::Dec.rivet_type(), None);
}

/// A NUMBER whose kind is text is labelled plain decimal text; any other kind of NUMBER is not.
#[test]
fn only_a_number_of_the_text_kind_is_labelled_plain_decimal() {
    for (p, s, text) in [
        (0, -127, true),
        (0, 0, true),
        (38, 39, true),
        (38, -1, true),
        (10, 0, false),
        (19, 0, false),
        (12, 2, false),
        (5, -2, false),
    ] {
        let rivet = autodetect(OraKind::Number, "number", p, s, "number");
        let mapping = TypeMapping::from_source(&SourceColumn::simple("c", "number", true), rivet);
        let delivered = deliver_number_text(mapping, OraKind::Number, p, s).delivery;
        let want = if text {
            Delivery::Text(TextForm::DecimalPlain)
        } else {
            Delivery::Native
        };
        assert_eq!(delivered, want, "number({p},{s})");
    }
}

/// A declared override names the type a column is read as, whatever its kind reads as by default.
#[test]
fn an_override_wins_over_the_kind() {
    let overrides = ColumnOverrides::from([("c".to_string(), dec(18, 2))]);
    let got = crate::types::resolve_column(
        crate::types::override_table::Engine::Oracle,
        &overrides,
        "c",
        "number(10,0)",
        || autodetect(OraKind::Number, "number(10,0)", 10, 0, "number"),
    )
    .expect("int to decimal is read");
    assert_eq!(got, dec(18, 2));
}
