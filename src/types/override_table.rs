//! The override compatibility table: for one engine, may a `columns:` override retype a column it did not autodetect.

use super::kind::CommonKind;
use super::{ColumnOverrides, RivetType, TimeUnit};
use crate::error::{Result, codes};

/// The source engine whose override list is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Engine {
    Postgres,
    Mysql,
    Mssql,
    #[cfg_attr(not(feature = "oracle"), allow(dead_code))]
    Oracle,
    Mongo,
}

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

impl From<CommonKind> for Kind {
    fn from(c: CommonKind) -> Self {
        match c {
            CommonKind::Bool => Bool,
            CommonKind::I16 => I16,
            CommonKind::I32 => I32,
            CommonKind::I64 => I64,
            CommonKind::F32 => F32,
            CommonKind::F64 => F64,
            CommonKind::Dec => Dec,
            CommonKind::Date => Date,
            CommonKind::Naive => Naive,
            CommonKind::Utc => Utc,
            CommonKind::Text => Text,
            CommonKind::Binary => Binary,
            CommonKind::Json => Json,
            CommonKind::Time => Time,
            CommonKind::Uuid => Uuid,
            CommonKind::Interval => Interval,
        }
    }
}

fn kind_of(t: &RivetType) -> Kind {
    match t {
        RivetType::UInt64 => U64,
        RivetType::Timestamp {
            unit: TimeUnit::Nanosecond,
            timezone: None,
        } => NaiveNs,
        RivetType::Timestamp {
            unit: TimeUnit::Nanosecond,
            timezone: Some(_),
        } => UtcNs,
        RivetType::Enum => Enum,
        RivetType::List { .. } => List,
        RivetType::Unsupported { .. } => Unsupported,
        RivetType::Bool
        | RivetType::Int16
        | RivetType::Int32
        | RivetType::Int64
        | RivetType::Float32
        | RivetType::Float64
        | RivetType::Decimal { .. }
        | RivetType::Date
        | RivetType::Timestamp { .. }
        | RivetType::String
        | RivetType::Text
        | RivetType::Json
        | RivetType::Binary
        | RivetType::Time { .. }
        | RivetType::Uuid
        | RivetType::Interval => CommonKind::of(t).map_or(Unsupported, Kind::from),
    }
}

const TEXTS: &[Kind] = &[Text, Json];
const STAMPS: &[Kind] = &[Naive, Utc];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Native {
    Bit,
    Money,
    Number,
    Plain,
}

fn native_class(native: &str) -> Native {
    let n = native.to_ascii_lowercase();
    if n.starts_with("bit") {
        Native::Bit
    } else if matches!(n.as_str(), "money" | "money4" | "smallmoney") {
        Native::Money
    } else if n.starts_with("number") {
        Native::Number
    } else {
        Native::Plain
    }
}

fn mysql_reads(native: Native, src: Kind, tgt: Kind) -> bool {
    let allowed: &[Kind] = match src {
        Bool if native == Native::Bit => &[Bool, I64, Binary, Text, Json],
        I16 | I32 | I64 | U64 if native == Native::Bit => &[I64, Binary, Text, Json],
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
        Naive | Utc => {
            if seeded!("override-without-builder-internal-stop", true, false) {
                &[Naive, Utc, NaiveNs, UtcNs, Text, Json]
            } else {
                &[Naive, Utc, Text, Json]
            }
        }
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

fn mssql_reads(native: Native, src: Kind, tgt: Kind) -> bool {
    let allowed: &[Kind] = match src {
        Dec if native == Native::Money => &[Dec, F64],
        Dec => &[Dec],
        I16 | I32 => &[Dec],
        I64 => &[I64, Dec],
        Text => &[Text, Json, Uuid],
        Naive if seeded!("mssql-text-over-datetime-refused", true, false) => {
            &[Naive, Utc, NaiveNs, UtcNs]
        }
        Utc if seeded!("mssql-text-over-datetime-refused", true, false) => &[Naive, Utc],
        Naive => &[Naive, Utc, NaiveNs, UtcNs, Text],
        Utc => &[Naive, Utc, Text],
        _ => &[],
    };
    allowed.contains(&tgt)
}

fn oracle_reads(native: Native, src: Kind, tgt: Kind) -> bool {
    let allowed: &[Kind] = match src {
        _ if native == Native::Number => &[I16, I32, I64, F64, Dec, Text, Json],
        F32 => &[F32, F64, Text, Json],
        F64 => &[F64, Text, Json],
        Text | Bool | Interval => TEXTS,
        Naive | Utc => &[Naive, Utc, NaiveNs, UtcNs, Date, Text, Json],
        Binary => &[Binary, Uuid, Text, Json],
        _ => &[],
    };
    allowed.contains(&tgt)
}

fn decimal_fits(engine: Engine, tgt: &RivetType) -> bool {
    match (engine, tgt) {
        (_, RivetType::Decimal { scale, .. }) if *scale < 0 => false,
        (Engine::Mssql | Engine::Oracle, RivetType::Decimal { precision, .. }) => *precision <= 38,
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
pub(crate) fn override_is_read(
    engine: Engine,
    native: &str,
    src: &RivetType,
    tgt: &RivetType,
) -> bool {
    let (s, t) = (kind_of(src), kind_of(tgt));
    if src == tgt || s == Unsupported && engine != Engine::Postgres || s == t && !matches!(s, Dec) {
        return true;
    }
    if !decimal_fits(engine, tgt) || narrows(src, t) {
        return false;
    }
    let native = native_class(native);
    match engine {
        Engine::Mysql => mysql_reads(native, s, t),
        Engine::Postgres => postgres_reads(s, t),
        Engine::Mssql => mssql_reads(native, s, t),
        Engine::Oracle => oracle_reads(native, s, t),
        Engine::Mongo => s == Json && t == Text,
    }
}

/// The type of `column`: its override when the table reads that pair, a coded refusal when it does not, else the autodetected type.
pub(crate) fn resolve_column(
    engine: Engine,
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

/// The first of `detected` that `overrides` declares text over a SQL Server date-time, which the batch reader delivers as the server's own text and a CDC change stream renders in its own form.
pub(crate) fn mssql_datetime_text_declared<'a>(
    detected: &'a [crate::types::TypeMapping],
    overrides: &ColumnOverrides,
) -> Option<&'a crate::types::TypeMapping> {
    detected.iter().find(|m| {
        matches!(kind_of(&m.rivet_type), Naive | Utc | NaiveNs | UtcNs)
            && overrides
                .get(&m.column_name)
                .is_some_and(|target| kind_of(target) == Text)
    })
}

/// The refusal text for a pair the table does not read: names the column, both types and the two remedies.
pub fn refusal_message(
    engine: Engine,
    column: &str,
    native: &str,
    detected: &RivetType,
    target: &RivetType,
) -> String {
    let remedy = match target {
        RivetType::Timestamp {
            unit: TimeUnit::Nanosecond,
            timezone,
        } => {
            let us = if timezone.is_some() {
                "timestamp_tz"
            } else {
                "timestamp"
            };
            format!("Instead: declare `{us}` (microseconds), or remove the override.")
        }
        RivetType::Decimal { precision, .. } if *precision > 38 => {
            "Instead: its precision is above 38; declare the column's own `decimal(p,s)` with p <= 38 (see \
             the source column definition), or remove the override."
                .to_string()
        }
        _ => "Remove the override, or CAST the column to the declared type in the export's `query:` \
              (a `mode: cdc` export has no `query:`: remove the override)."
            .to_string(),
    };
    format!(
        "column `{column}` is {native} (read as {}) and `columns:` declares it {} — {engine:?} does \
         not convert that pair, and writing it as NULL would lose every value. {remedy}",
        detected.label(),
        target.label(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENGINES: [Engine; 5] = [
        Engine::Postgres,
        Engine::Mysql,
        Engine::Mssql,
        Engine::Oracle,
        Engine::Mongo,
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
    fn sql_server_reads_a_date_time_as_text_and_nothing_wider() {
        let ts = |tz| RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: tz,
        };
        for (native, src) in [
            ("datetime2", ts(None)),
            ("datetime", ts(None)),
            ("smalldatetime", ts(None)),
            ("datetimeoffset", ts(Some("UTC".into()))),
        ] {
            assert!(
                override_is_read(Engine::Mssql, native, &src, &RivetType::String),
                "{native}"
            );
            assert!(
                !override_is_read(Engine::Mssql, native, &src, &RivetType::Json),
                "{native} json"
            );
            assert!(
                !override_is_read(Engine::Mssql, native, &src, &RivetType::Int64),
                "{native} int"
            );
        }
    }

    #[test]
    fn a_declared_text_over_a_sql_server_date_time_is_found_and_nothing_else() {
        let col = |name: &str, ty: RivetType| {
            crate::types::TypeMapping::from_source(
                &crate::types::SourceColumn::simple(name, "datetime2", true),
                ty,
            )
        };
        let ts = RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        };
        let detected = [
            col("id", RivetType::Int64),
            col("s", RivetType::String),
            col("v", ts.clone()),
        ];
        let declared = |c: &str, t: RivetType| ColumnOverrides::from([(c.to_string(), t)]);
        for text in [RivetType::String, RivetType::Text] {
            let found = mssql_datetime_text_declared(&detected, &declared("v", text));
            assert_eq!(found.map(|m| m.column_name.as_str()), Some("v"));
        }
        assert!(mssql_datetime_text_declared(&detected, &declared("s", RivetType::Text)).is_none());
        assert!(
            mssql_datetime_text_declared(&detected, &declared("id", RivetType::Text)).is_none()
        );
        let nanos = RivetType::Timestamp {
            unit: TimeUnit::Nanosecond,
            timezone: None,
        };
        assert!(mssql_datetime_text_declared(&detected, &declared("v", nanos)).is_none());
        assert!(mssql_datetime_text_declared(&detected, &ColumnOverrides::new()).is_none());
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
            Engine::Mongo,
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
        let doc = |t: RivetType| override_is_read(Engine::Mongo, "document", &RivetType::Json, &t);
        assert!(doc(RivetType::String));
        assert!(!doc(RivetType::Int64));
        assert!(!override_is_read(
            Engine::Mongo,
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
        assert!(!override_is_read(Engine::Mssql, "decimal", &src, &wide));
        assert!(!override_is_read(Engine::Oracle, "number", &src, &wide));
        assert!(override_is_read(Engine::Postgres, "numeric", &src, &wide));
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
            (Engine::Mysql, "binary", RivetType::Binary, RivetType::Bool),
            (Engine::Mysql, "decimal", dec(18, 0), RivetType::Int64),
            (Engine::Mssql, "decimal", dec(18, 4), dec(38, 10)),
            (
                Engine::Oracle,
                "NUMBER(9)",
                RivetType::Int32,
                RivetType::Int64,
            ),
            (Engine::Oracle, "NUMBER(18)", RivetType::Int64, dec(38, 10)),
            (
                Engine::Mssql,
                "nvarchar",
                RivetType::String,
                RivetType::Uuid,
            ),
            (Engine::Mssql, "int", RivetType::Int32, dec(10, 2)),
            (Engine::Mssql, "bigint", RivetType::Int64, dec(19, 0)),
            (
                Engine::Oracle,
                "BINARY_DOUBLE",
                RivetType::Float64,
                RivetType::Text,
            ),
            (Engine::Mssql, "smallint", RivetType::Int16, dec(10, 2)),
            (
                Engine::Oracle,
                "DATE",
                RivetType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: None,
                },
                RivetType::Date,
            ),
            (Engine::Oracle, "BOOLEAN", RivetType::Bool, RivetType::Text),
            (
                Engine::Oracle,
                "INTERVAL DAY TO SECOND",
                RivetType::Interval,
                RivetType::String,
            ),
            (
                Engine::Oracle,
                "NUMBER(38,10)",
                dec(38, 10),
                RivetType::String,
            ),
            (
                Engine::Postgres,
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
    fn a_pair_an_engine_does_not_convert_is_refused() {
        assert!(!override_is_read(
            Engine::Mssql,
            "int",
            &RivetType::Int32,
            &RivetType::Text
        ));
        assert!(!override_is_read(
            Engine::Oracle,
            "RAW",
            &RivetType::Binary,
            &RivetType::Bool
        ));
    }

    #[test]
    fn an_integer_is_read_as_a_flag_only_where_the_engine_checks_each_value() {
        for (native, ty) in [
            ("tinyint", RivetType::Int16),
            ("int", RivetType::Int32),
            ("bigint", RivetType::Int64),
            ("bigint unsigned", RivetType::Int64),
        ] {
            assert!(
                override_is_read(Engine::Mysql, native, &ty, &RivetType::Bool),
                "mysql {native}"
            );
            for engine in [Engine::Postgres, Engine::Mssql, Engine::Oracle] {
                assert!(
                    !override_is_read(engine, native, &ty, &RivetType::Bool),
                    "{engine:?} {native}"
                );
            }
        }
    }

    #[test]
    fn a_wider_bit_or_an_unmapped_postgres_type_is_not_read_as_what_it_is_not() {
        assert!(!override_is_read(
            Engine::Mysql,
            "bit",
            &RivetType::Int64,
            &RivetType::Bool
        ));
        assert!(override_is_read(
            Engine::Mysql,
            "bit",
            &RivetType::Bool,
            &RivetType::Bool
        ));
        let cidr = RivetType::Unsupported {
            native_type: "cidr".into(),
            reason: String::new(),
        };
        assert!(!override_is_read(
            Engine::Postgres,
            "cidr",
            &cidr,
            &RivetType::Uuid
        ));
        assert!(!override_is_read(
            Engine::Mysql,
            "datetime",
            &RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None
            },
            &RivetType::Date
        ));
    }

    fn ts(unit: TimeUnit, utc: bool) -> RivetType {
        RivetType::Timestamp {
            unit,
            timezone: utc.then(|| "UTC".into()),
        }
    }

    #[test]
    fn mysql_reads_a_date_and_a_datetime_as_the_stamps_and_text_it_can_build() {
        let us = |utc| ts(TimeUnit::Microsecond, utc);
        let read = [
            ("date", RivetType::Date, RivetType::Date),
            ("date", RivetType::Date, us(false)),
            ("date", RivetType::Date, us(true)),
            ("date", RivetType::Date, RivetType::Text),
            ("date", RivetType::Date, RivetType::Json),
            ("datetime", us(false), us(true)),
            ("datetime", us(true), us(false)),
            ("datetime", us(false), RivetType::Text),
            ("datetime", us(true), RivetType::Json),
        ];
        for (native, src, tgt) in read {
            assert!(
                override_is_read(Engine::Mysql, native, &src, &tgt),
                "mysql {native} -> {}",
                tgt.label()
            );
        }
    }

    #[test]
    fn mysql_refuses_a_stamp_or_date_override_it_cannot_build() {
        let us = |utc| ts(TimeUnit::Microsecond, utc);
        let ns = |utc| ts(TimeUnit::Nanosecond, utc);
        let refused = [
            ("date", RivetType::Date, RivetType::Int64),
            ("date", RivetType::Date, ns(false)),
            ("datetime", us(false), RivetType::Date),
            ("datetime", us(false), ns(false)),
            ("datetime", us(false), ns(true)),
            ("datetime", us(true), ns(true)),
            ("datetime", us(false), RivetType::Int64),
        ];
        for (native, src, tgt) in refused {
            assert!(
                !override_is_read(Engine::Mysql, native, &src, &tgt),
                "mysql {native} -> {}",
                tgt.label()
            );
        }
    }

    #[test]
    fn a_native_name_is_classified_once_whatever_its_case() {
        for native in ["money", "MONEY", "Money4", "smallmoney"] {
            assert!(
                override_is_read(Engine::Mssql, native, &dec(19, 4), &RivetType::Float64),
                "{native}"
            );
        }
        for native in ["decimal", "moneyx", "xmoney", "money "] {
            assert!(
                !override_is_read(Engine::Mssql, native, &dec(19, 4), &RivetType::Float64),
                "{native}"
            );
        }
        for native in ["NUMBER(9)", "number(9)", "Number", "numbers"] {
            assert!(
                override_is_read(Engine::Oracle, native, &RivetType::Int32, &RivetType::Int64),
                "{native}"
            );
        }
        for native in ["INTEGER", "numb", "xnumber"] {
            assert!(
                !override_is_read(Engine::Oracle, native, &RivetType::Int32, &RivetType::Int64),
                "{native}"
            );
        }
        for native in ["bit", "BIT", "Bit(1)", "bit(64)"] {
            assert!(
                !override_is_read(Engine::Mysql, native, &RivetType::Int64, &RivetType::Bool),
                "{native}"
            );
            assert!(
                !override_is_read(Engine::Mysql, native, &RivetType::Bool, &RivetType::Int16),
                "{native}"
            );
        }
        for native in ["bi", "tinyint", "xbit"] {
            assert!(
                override_is_read(Engine::Mysql, native, &RivetType::Int64, &RivetType::Bool),
                "{native}"
            );
            assert!(
                override_is_read(Engine::Mysql, native, &RivetType::Bool, &RivetType::Int16),
                "{native}"
            );
        }
    }

    #[test]
    fn a_nanosecond_stamp_is_never_read_as_the_other_zone_on_any_engine() {
        for e in ENGINES {
            let ns = |utc| ts(TimeUnit::Nanosecond, utc);
            assert!(
                !override_is_read(e, "datetime2", &ns(false), &ns(true)),
                "{e:?}"
            );
            assert!(
                !override_is_read(e, "datetime2", &ns(true), &ns(false)),
                "{e:?}"
            );
            assert!(
                override_is_read(e, "datetime2", &ns(false), &ns(false)),
                "{e:?}"
            );
            assert!(
                override_is_read(e, "datetime2", &ns(true), &ns(true)),
                "{e:?}"
            );
        }
    }
}
