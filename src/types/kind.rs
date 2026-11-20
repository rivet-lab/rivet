//! The common kinds: types that mean the same on every engine, classified once from a `RivetType`.

use super::{RivetType, TimeUnit};

/// A type every engine reads the same way; kinds only some engines have stay in each engine's own table.
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
}

impl CommonKind {
    /// The common kind of `t`, or `None` for a type only some engines carry (unsigned, time of day, nanosecond stamp, enum, interval, uuid, list, unsupported).
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
            RivetType::UInt64
            | RivetType::Time { .. }
            | RivetType::Enum
            | RivetType::Interval
            | RivetType::Uuid
            | RivetType::List { .. }
            | RivetType::Unsupported { .. } => return None,
        })
    }
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
                None,
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
            (RivetType::Interval, None),
            (RivetType::Uuid, None),
            (RivetType::Binary, Some(Binary)),
            (list, None),
            (unsupported, None),
        ];
        for (t, want) in cases {
            assert_eq!(CommonKind::of(&t), want, "{t:?}");
        }
    }
}
