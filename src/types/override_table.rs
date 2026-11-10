//! The override compatibility table: for one engine, may a `columns:` override retype a column it did not autodetect.

use super::{ColumnOverrides, RivetType, TimeUnit};
use crate::config::SourceType;
use crate::error::{Result, codes};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Bool,
    I16,
    I32,
    I64,
    U64,
    F32,
    F64,
    Dec,
    Date,
    Time,
    Naive,
    Utc,
    NaiveNs,
    UtcNs,
    Text,
    Json,
    Enum,
    Interval,
    Uuid,
    Binary,
    List,
    Unsupported,
}

use Kind::*;

fn kind_of(t: &RivetType) -> Kind {
    match t {
        RivetType::Bool => Bool,
        RivetType::Int16 => I16,
        RivetType::Int32 => I32,
        RivetType::Int64 => I64,
        RivetType::UInt64 => U64,
        RivetType::Float32 => F32,
        RivetType::Float64 => F64,
        RivetType::Decimal { .. } => Dec,
        RivetType::Date => Date,
        RivetType::Time { .. } => Time,
        RivetType::Timestamp { unit, timezone } => match (unit, timezone) {
            (TimeUnit::Nanosecond, None) => NaiveNs,
            (TimeUnit::Nanosecond, Some(_)) => UtcNs,
            (_, None) => Naive,
            (_, Some(_)) => Utc,
        },
        RivetType::String | RivetType::Text => Text,
        RivetType::Json => Json,
        RivetType::Enum => Enum,
        RivetType::Interval => Interval,
        RivetType::Uuid => Uuid,
        RivetType::Binary => Binary,
        RivetType::List { .. } => List,
        RivetType::Unsupported { .. } => Unsupported,
    }
}

const TEXTS: &[Kind] = &[Text, Json];
const STAMPS: &[Kind] = &[Naive, Utc];
const STAMPS_NS: &[Kind] = &[Naive, Utc, NaiveNs, UtcNs];
const STAMPS_NS_TEXT: &[Kind] = &[Naive, Utc, NaiveNs, UtcNs, Text, Json];

fn is_bit(native: &str) -> bool {
    native.to_ascii_lowercase().starts_with("bit")
}

fn is_money(native: &str) -> bool {
    matches!(
        native.to_ascii_lowercase().as_str(),
        "money" | "money4" | "smallmoney"
    )
}

fn mysql_reads(native: &str, src: Kind, tgt: Kind) -> bool {
    let allowed: &[Kind] = match src {
        Bool if is_bit(native) => &[Bool, I64, Binary, Text, Json],
        I16 | I32 | I64 | U64 if is_bit(native) => &[I64, Binary, Text, Json],
        Bool => &[Bool, I16, I32, I64, F32, F64, Dec, Text, Json],
        I16 | I32 | I64 | U64 => &[Bool, I16, I32, I64, F32, F64, Dec, Text, Json],
        F32 => &[F32, F64, Text, Json],
        F64 => &[F64, Text, Json],
        Dec => &[I16, I32, I64, F64, Dec, Text, Json],
        Text => &[
            I16, I32, I64, F32, F64, Dec, Date, Naive, Utc, Uuid, Text, Json,
        ],
        Enum | Json => &[Binary, Text, Json],
        Binary => &[Bool, Uuid, Binary, Text, Json],
        Date => &[Date, Naive, Utc, Text, Json],
        Naive | Utc => &[Naive, Utc, Text, Json],
        _ => &[],
    };
    allowed.contains(&tgt)
}

fn postgres_reads(src: Kind, tgt: Kind) -> bool {
    let allowed: &[Kind] = match src {
        I16 | I32 | I64 => &[I16, I32, I64, Dec],
        Dec => &[Dec, Text, Json],
        Text | Json | Enum | Interval => TEXTS,
        Uuid => &[Uuid, Text, Json],
        Naive | Utc => STAMPS,
        Unsupported => TEXTS,
        _ => &[],
    };
    allowed.contains(&tgt)
}

fn mssql_reads(native: &str, src: Kind, tgt: Kind) -> bool {
    let allowed: &[Kind] = match src {
        Dec if is_money(native) => &[Dec, F64],
        Dec => &[Dec],
        I64 => &[I64, Dec],
        Text => TEXTS,
        Naive => STAMPS_NS,
        Utc => STAMPS,
        _ => &[],
    };
    allowed.contains(&tgt)
}

fn oracle_reads(native: &str, src: Kind, tgt: Kind) -> bool {
    let allowed: &[Kind] = match src {
        _ if native.to_ascii_lowercase().starts_with("number") => {
            &[I16, I32, I64, F64, Dec, Text, Json]
        }
        F32 => &[F32, F64, Text, Json],
        F64 => &[F64, Text, Json],
        Text => TEXTS,
        Naive | Utc => STAMPS_NS_TEXT,
        Binary => &[Binary, Uuid, Text, Json],
        _ => &[],
    };
    allowed.contains(&tgt)
}

fn decimal_fits(engine: SourceType, tgt: &RivetType) -> bool {
    match (engine, tgt) {
        (_, RivetType::Decimal { scale, .. }) if *scale < 0 => false,
        (SourceType::Mssql | SourceType::Oracle, RivetType::Decimal { precision, .. }) => {
            *precision <= 38
        }
        _ => true,
    }
}

/// Whether the pair narrows by type alone: a fraction into an integer, or a 64-bit or decimal number into a 32-bit float.
fn narrows(src: &RivetType, t: Kind) -> bool {
    match src {
        RivetType::Decimal { scale, .. } => *scale > 0 && matches!(t, I16 | I32 | I64) || t == F32,
        RivetType::Float64 => t == F32,
        _ => false,
    }
}

/// Whether `engine` converts a column it autodetects as `src` (native type `native`) to the overridden `tgt`.
pub fn override_is_read(
    engine: SourceType,
    native: &str,
    src: &RivetType,
    tgt: &RivetType,
) -> bool {
    let (s, t) = (kind_of(src), kind_of(tgt));
    if src == tgt
        || s == Unsupported && engine != SourceType::Postgres
        || s == t && !matches!(s, Dec)
    {
        return true;
    }
    if !decimal_fits(engine, tgt) || narrows(src, t) {
        return false;
    }
    match engine {
        SourceType::Mysql => mysql_reads(native, s, t),
        SourceType::Postgres => postgres_reads(s, t),
        SourceType::Mssql => mssql_reads(native, s, t),
        SourceType::Oracle => oracle_reads(native, s, t),
        SourceType::Mongo => s == Json && t == Text,
    }
}

/// The type of `column`: its override when the table reads that pair, a coded refusal when it does not, else the autodetected type.
pub fn resolve_column(
    engine: SourceType,
    overrides: &ColumnOverrides,
    column: &str,
    native: &str,
    autodetect: impl FnOnce() -> RivetType,
) -> Result<RivetType> {
    let detected = autodetect();
    match overrides.get(column) {
        None => Ok(detected),
        Some(target) if override_is_read(engine, native, &detected, target) => Ok(target.clone()),
        Some(target) => Err(crate::error::coded(
            codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
            refusal_message(engine, column, native, &detected, target),
        )),
    }
}

/// The refusal text for a pair the table does not read: names the column, both types and the two remedies.
pub fn refusal_message(
    engine: SourceType,
    column: &str,
    native: &str,
    detected: &RivetType,
    target: &RivetType,
) -> String {
    format!(
        "column `{column}` is {native} (read as {}) and `columns:` declares it {} — {engine:?} does \
         not convert that pair, and writing it as NULL would lose every value. Remove the override, \
         or CAST the column to the declared type in the export's `query:`.",
        detected.label(),
        target.label(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENGINES: [SourceType; 5] = [
        SourceType::Postgres,
        SourceType::Mysql,
        SourceType::Mssql,
        SourceType::Oracle,
        SourceType::Mongo,
    ];

    fn one(column: &str, t: RivetType) -> ColumnOverrides {
        ColumnOverrides::from([(column.to_string(), t)])
    }

    #[test]
    fn a_column_without_an_override_keeps_its_detected_type_on_every_engine() {
        for e in ENGINES {
            let got = resolve_column(e, &one("other", RivetType::Int64), "c", "x", || {
                RivetType::Date
            });
            assert_eq!(got.unwrap(), RivetType::Date, "{e:?}");
        }
    }

    #[test]
    fn an_override_to_the_detected_type_is_read_on_every_engine() {
        for e in ENGINES {
            let got = resolve_column(e, &one("c", RivetType::Json), "c", "document", || {
                RivetType::Json
            });
            assert_eq!(got.unwrap(), RivetType::Json, "{e:?}");
        }
    }

    #[test]
    fn an_unread_pair_is_refused_with_the_wire_code_and_names_column_and_both_types() {
        let err = resolve_column(
            SourceType::Mongo,
            &one("_id", RivetType::Int64),
            "_id",
            "objectid",
            || RivetType::String,
        )
        .unwrap_err();
        assert_eq!(
            crate::error::code_of(&err).map(|c| c.id),
            Some(codes::SOURCE_OVERRIDE_WIRE_MISMATCH.id)
        );
        let text = format!("{err:#}");
        assert!(
            text.contains("`_id`") && text.contains("objectid") && text.contains("int64"),
            "{text}"
        );
    }

    #[test]
    fn mongo_reads_its_document_as_plain_text_and_refuses_any_other_retype() {
        let doc =
            |t: RivetType| override_is_read(SourceType::Mongo, "document", &RivetType::Json, &t);
        assert!(doc(RivetType::String));
        assert!(!doc(RivetType::Int64));
        assert!(!override_is_read(
            SourceType::Mongo,
            "objectid",
            &RivetType::String,
            &RivetType::Json
        ));
    }

    #[test]
    fn a_decimal_wider_than_38_digits_is_refused_where_the_wire_cannot_carry_it() {
        let wide = RivetType::Decimal {
            precision: 60,
            scale: 2,
        };
        let src = RivetType::Decimal {
            precision: 18,
            scale: 2,
        };
        assert!(!override_is_read(SourceType::Mssql, "decimal", &src, &wide));
        assert!(!override_is_read(SourceType::Oracle, "number", &src, &wide));
        assert!(override_is_read(
            SourceType::Postgres,
            "numeric",
            &src,
            &wide
        ));
    }

    fn dec(precision: u8, scale: i8) -> RivetType {
        RivetType::Decimal { precision, scale }
    }

    #[test]
    fn a_pair_that_narrows_by_type_is_refused_on_every_engine() {
        for e in ENGINES {
            assert!(
                !override_is_read(e, "decimal", &dec(18, 2), &RivetType::Int64),
                "{e:?}"
            );
            assert!(
                !override_is_read(e, "double", &RivetType::Float64, &RivetType::Float32),
                "{e:?}"
            );
            assert!(
                !override_is_read(e, "decimal", &dec(18, 2), &RivetType::Float32),
                "{e:?}"
            );
            assert!(
                !override_is_read(e, "decimal", &dec(18, 2), &dec(18, -2)),
                "{e:?}"
            );
        }
    }

    #[test]
    fn the_pairs_main_delivered_whole_stay_read() {
        let read = [
            (
                SourceType::Mysql,
                "binary",
                RivetType::Binary,
                RivetType::Bool,
            ),
            (SourceType::Mysql, "decimal", dec(18, 0), RivetType::Int64),
            (SourceType::Mssql, "decimal", dec(18, 4), dec(38, 10)),
            (
                SourceType::Oracle,
                "NUMBER(9)",
                RivetType::Int32,
                RivetType::Int64,
            ),
            (
                SourceType::Oracle,
                "NUMBER(18)",
                RivetType::Int64,
                dec(38, 10),
            ),
            (
                SourceType::Oracle,
                "NUMBER(38,10)",
                dec(38, 10),
                RivetType::String,
            ),
            (
                SourceType::Postgres,
                "cidr",
                RivetType::Unsupported {
                    native_type: "cidr".into(),
                    reason: String::new(),
                },
                RivetType::String,
            ),
        ];
        for (e, native, src, tgt) in read {
            assert!(
                override_is_read(e, native, &src, &tgt),
                "{e:?} {native} -> {}",
                tgt.label()
            );
        }
    }

    #[test]
    fn a_wider_bit_or_an_unmapped_postgres_type_is_not_read_as_what_it_is_not() {
        assert!(!override_is_read(
            SourceType::Mysql,
            "bit",
            &RivetType::Int64,
            &RivetType::Bool
        ));
        assert!(override_is_read(
            SourceType::Mysql,
            "bit",
            &RivetType::Bool,
            &RivetType::Bool
        ));
        let cidr = RivetType::Unsupported {
            native_type: "cidr".into(),
            reason: String::new(),
        };
        assert!(!override_is_read(
            SourceType::Postgres,
            "cidr",
            &cidr,
            &RivetType::Uuid
        ));
        assert!(!override_is_read(
            SourceType::Mysql,
            "datetime",
            &RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None
            },
            &RivetType::Date
        ));
    }
}
