//! The common kinds: types that mean the same on every engine, classified once from a `RivetType`.

use super::{RivetType, TimeUnit};

/// A type that means the same on every engine that carries it; a type with engine-only meaning stays in that engine's own table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommonKind {
    Bool,
    I16,
    I32,
    I64,
    F32,
    F64,
    Dec,
    Date,
    Naive,
    Utc,
    Text,
    Binary,
    Json,
    Time,
    Uuid,
    Interval,
}

impl CommonKind {
    /// The common kind of `t`, or `None` for a type only some engines carry (unsigned, nanosecond stamp, enum, list, unsupported).
    pub(crate) fn of(t: &RivetType) -> Option<Self> {
        Some(match t {
            RivetType::Bool => Self::Bool,
            RivetType::Int16 => Self::I16,
            RivetType::Int32 => Self::I32,
            RivetType::Int64 => Self::I64,
            RivetType::Float32 => Self::F32,
            RivetType::Float64 => Self::F64,
            RivetType::Decimal { .. } => Self::Dec,
            RivetType::Date => Self::Date,
            RivetType::Timestamp { unit, timezone } => match (unit, timezone) {
                (TimeUnit::Nanosecond, _) => return None,
                (_, None) => Self::Naive,
                (_, Some(_)) => Self::Utc,
            },
            RivetType::String | RivetType::Text => Self::Text,
            RivetType::Json => Self::Json,
            RivetType::Binary => Self::Binary,
            RivetType::Time { .. } => Self::Time,
            RivetType::Uuid => Self::Uuid,
            RivetType::Interval => Self::Interval,
            RivetType::UInt64
            | RivetType::Enum
            | RivetType::List { .. }
            | RivetType::Unsupported { .. } => return None,
        })
    }

    /// The one type a kind is delivered as; `None` for `Dec`, whose precision and scale the source must supply.
    pub(crate) fn rivet_type(self) -> Option<RivetType> {
        Some(match self {
            Self::Bool => RivetType::Bool,
            Self::I16 => RivetType::Int16,
            Self::I32 => RivetType::Int32,
            Self::I64 => RivetType::Int64,
            Self::F32 => RivetType::Float32,
            Self::F64 => RivetType::Float64,
            Self::Dec => return None,
            Self::Date => RivetType::Date,
            Self::Naive => RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            },
            Self::Utc => RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            },
            Self::Text => RivetType::String,
            Self::Binary => RivetType::Binary,
            Self::Json => RivetType::Json,
            Self::Time => RivetType::Time {
                unit: TimeUnit::Microsecond,
            },
            Self::Uuid => RivetType::Uuid,
            Self::Interval => RivetType::Interval,
        })
    }
}

/// Native types an engine reads outside every `CommonKind` yet delivers as a type a common kind also delivers: engine, row label, why.
#[cfg(test)]
const ENGINE_ONLY_IN_COMMON_SHAPE: &[(super::override_table::Engine, &str, &str)] = {
    use super::override_table::Engine::{Mssql, Mysql, Postgres};
    &[
        (
            Postgres,
            "oid",
            "2026-11-21: unsigned 32-bit object id, read as int64",
        ),
        (
            Mysql,
            "bit len 0",
            "2026-11-21: BIT(n) bit field, int64 up to 63 bits",
        ),
        (Mysql, "bit len 1", "2026-11-21: BIT(1) is bool"),
        (
            Mysql,
            "bit len 2",
            "2026-11-21: BIT(n) bit field, int64 up to 63 bits",
        ),
        (
            Mysql,
            "bit len 63",
            "2026-11-21: BIT(n) bit field, int64 up to 63 bits",
        ),
        (Mysql, "year", "2026-11-21: calendar year, read as int16"),
        (
            Mssql,
            "money",
            "2026-11-21: fixed-scale decimal(19,4), exact over its range",
        ),
        (
            Mssql,
            "money4",
            "2026-11-21: smallmoney, fixed-scale decimal(10,4)",
        ),
        (
            Mssql,
            "ssvariant",
            "2026-11-21: sql_variant, the base value as text",
        ),
        (
            Mssql,
            "udt",
            "2026-11-21: CLR type, serialized bytes as text",
        ),
    ]
};

/// The kinds an engine table may never answer outside `CommonKind`, not even under a declared label.
#[cfg(test)]
const NEVER_ENGINE_ONLY: [CommonKind; 3] =
    [CommonKind::Time, CommonKind::Uuid, CommonKind::Interval];

/// Fails unless the rows no common kind reads but whose type is common-shaped are exactly the declared engine-only ones, none is a time, uuid or interval, and the doc's by-kind column for the engine matches the kinds read.
#[cfg(test)]
pub(crate) fn assert_engine_only_rows_are_declared(
    engine: super::override_table::Engine,
    rows: impl IntoIterator<Item = (String, Option<CommonKind>, RivetType)>,
) {
    let rows: Vec<_> = rows.into_iter().collect();
    let seen: Vec<CommonKind> = rows.iter().filter_map(|(_, kind, _)| *kind).collect();
    super::kind_doc::assert_doc_column_matches(engine, &seen);
    assert_rows_are_declared(engine, rows);
}

/// Fails unless the rows no common kind reads but whose type is common-shaped are exactly the declared engine-only ones, and none is a time, uuid or interval.
#[cfg(test)]
fn assert_rows_are_declared(
    engine: super::override_table::Engine,
    rows: Vec<(String, Option<CommonKind>, RivetType)>,
) {
    let loose: Vec<(String, CommonKind)> = rows
        .into_iter()
        .filter(|(_, kind, _)| kind.is_none())
        .filter_map(|(label, _, t)| CommonKind::of(&t).map(|k| (label, k)))
        .collect();
    let leaked: Vec<&str> = loose
        .iter()
        .filter(|(_, k)| NEVER_ENGINE_ONLY.contains(k))
        .map(|(label, _)| label.as_str())
        .collect();
    assert!(
        leaked.is_empty(),
        "{engine:?}: {leaked:?} answer a time, uuid or interval outside `CommonKind`; route them through it, \
         a declared label never excuses these three"
    );
    let mut got: Vec<String> = loose.into_iter().map(|(label, _)| label).collect();
    got.sort();
    got.dedup();
    let mut want: Vec<String> = ENGINE_ONLY_IN_COMMON_SHAPE
        .iter()
        .filter(|(e, _, _)| *e == engine)
        .map(|(_, label, _)| label.to_string())
        .collect();
    want.sort();
    assert_eq!(
        got, want,
        "{engine:?}: a native type no common kind reads is delivered as a common type; route it through its \
         `CommonKind`, or declare it in ENGINE_ONLY_IN_COMMON_SHAPE with the reason it is engine-only"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(unit: TimeUnit, utc: bool) -> RivetType {
        RivetType::Timestamp {
            unit,
            timezone: utc.then(|| "UTC".into()),
        }
    }

    #[test]
    fn every_type_is_classified_to_its_common_kind_or_to_none() {
        use CommonKind::*;
        let dec = |precision, scale| RivetType::Decimal { precision, scale };
        let list = RivetType::List {
            inner: Box::new(RivetType::Int64),
        };
        let unsupported = RivetType::Unsupported {
            native_type: "cidr".into(),
            reason: String::new(),
        };
        let cases = [
            (RivetType::Bool, Some(Bool)),
            (RivetType::Int16, Some(I16)),
            (RivetType::Int32, Some(I32)),
            (RivetType::Int64, Some(I64)),
            (RivetType::UInt64, None),
            (RivetType::Float32, Some(F32)),
            (RivetType::Float64, Some(F64)),
            (dec(18, 2), Some(Dec)),
            (dec(76, -3), Some(Dec)),
            (RivetType::Date, Some(Date)),
            (
                RivetType::Time {
                    unit: TimeUnit::Microsecond,
                },
                Some(Time),
            ),
            (
                RivetType::Time {
                    unit: TimeUnit::Nanosecond,
                },
                Some(Time),
            ),
            (ts(TimeUnit::Second, false), Some(Naive)),
            (ts(TimeUnit::Millisecond, false), Some(Naive)),
            (ts(TimeUnit::Microsecond, false), Some(Naive)),
            (ts(TimeUnit::Nanosecond, false), None),
            (ts(TimeUnit::Second, true), Some(Utc)),
            (ts(TimeUnit::Millisecond, true), Some(Utc)),
            (ts(TimeUnit::Microsecond, true), Some(Utc)),
            (ts(TimeUnit::Nanosecond, true), None),
            (RivetType::String, Some(Text)),
            (RivetType::Text, Some(Text)),
            (RivetType::Json, Some(Json)),
            (RivetType::Enum, None),
            (RivetType::Interval, Some(Interval)),
            (RivetType::Uuid, Some(Uuid)),
            (RivetType::Binary, Some(Binary)),
            (list, None),
            (unsupported, None),
        ];
        for (t, want) in cases {
            assert_eq!(CommonKind::of(&t), want, "{t:?}");
        }
    }

    #[test]
    fn every_kind_is_delivered_as_one_type_that_classifies_back_to_it() {
        use CommonKind::*;
        let cases = [
            (Bool, Some(RivetType::Bool)),
            (I16, Some(RivetType::Int16)),
            (I32, Some(RivetType::Int32)),
            (I64, Some(RivetType::Int64)),
            (F32, Some(RivetType::Float32)),
            (F64, Some(RivetType::Float64)),
            (Dec, None),
            (Date, Some(RivetType::Date)),
            (Naive, Some(ts(TimeUnit::Microsecond, false))),
            (Utc, Some(ts(TimeUnit::Microsecond, true))),
            (Text, Some(RivetType::String)),
            (Binary, Some(RivetType::Binary)),
            (Json, Some(RivetType::Json)),
            (
                Time,
                Some(RivetType::Time {
                    unit: TimeUnit::Microsecond,
                }),
            ),
            (Uuid, Some(RivetType::Uuid)),
            (Interval, Some(RivetType::Interval)),
        ];
        for (kind, want) in cases {
            let got = kind.rivet_type();
            assert_eq!(got, want, "{kind:?}");
            if let Some(t) = got {
                assert_eq!(CommonKind::of(&t), Some(kind), "{kind:?}");
            }
        }
    }

    fn row(label: &str, t: RivetType) -> (String, Option<CommonKind>, RivetType) {
        (label.to_string(), None, t)
    }

    #[test]
    #[should_panic(expected = "outside `CommonKind`")]
    fn a_time_read_by_no_kind_is_refused_even_under_a_declared_label() {
        use super::super::override_table::Engine::Mysql;
        let time = RivetType::Time {
            unit: TimeUnit::Microsecond,
        };
        assert_rows_are_declared(Mysql, vec![row("year", time)]);
    }

    #[test]
    #[should_panic(expected = "outside `CommonKind`")]
    fn a_uuid_read_by_no_kind_is_refused_even_under_a_declared_label() {
        use super::super::override_table::Engine::Mssql;
        assert_rows_are_declared(Mssql, vec![row("money", RivetType::Uuid)]);
    }

    #[test]
    #[should_panic(expected = "outside `CommonKind`")]
    fn an_interval_read_by_no_kind_is_refused_even_under_a_declared_label() {
        use super::super::override_table::Engine::Postgres;
        assert_rows_are_declared(Postgres, vec![row("oid", RivetType::Interval)]);
    }

    #[test]
    fn a_time_uuid_or_interval_read_by_its_kind_is_no_engine_only_row() {
        use super::super::override_table::Engine::Postgres;
        let read = |label: &str, kind: CommonKind| {
            (label.to_string(), Some(kind), kind.rivet_type().unwrap())
        };
        let rows = vec![
            read("time", CommonKind::Time),
            read("uuid", CommonKind::Uuid),
            read("interval", CommonKind::Interval),
            row("oid", RivetType::Int64),
        ];
        assert_rows_are_declared(Postgres, rows);
    }
}
