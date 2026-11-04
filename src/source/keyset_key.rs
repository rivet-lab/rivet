//! The keyset key admission seam: which delivered column types the keyset cursor can advance.

use crate::config::SourceType;
use crate::types::{Delivery, RivetType, TimeUnit, TypeMapping};

/// The fractional-second digits a microsecond timestamp keeps.
pub(crate) const MICROSECOND_DIGITS: i32 = 6;

/// The cause a column cannot key a keyset seek, or `None` when the cursor reads it and re-binds it exactly.
pub(crate) fn keyset_key_refusal(
    source: SourceType,
    mapping: &TypeMapping,
    finer_than_microsecond: bool,
) -> Option<&'static str> {
    if !matches!(mapping.delivery, Delivery::Native) {
        return Some("is delivered as text, which does not sort as the stored value");
    }
    match &mapping.rivet_type {
        RivetType::Float32 | RivetType::Float64 if source == SourceType::Oracle => {
            Some("is a binary float, which is not a keyset key on Oracle")
        }
        RivetType::Int16
        | RivetType::Int32
        | RivetType::Int64
        | RivetType::UInt64
        | RivetType::Float32
        | RivetType::Float64
        | RivetType::Date
        | RivetType::String
        | RivetType::Text
        | RivetType::Uuid => None,
        RivetType::Decimal { scale: 0, .. } if source == SourceType::Oracle => None,
        RivetType::Decimal { .. } => {
            Some("is a decimal, which the cursor cannot advance past exactly")
        }
        RivetType::Timestamp {
            timezone: Some(_), ..
        } if source == SourceType::Oracle => {
            Some("is a timestamp with a time zone, which is not a keyset key on Oracle")
        }
        RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            ..
        } if finer_than_microsecond => Some(
            "carries more than 6 fractional-second digits, which the microsecond cursor truncates",
        ),
        RivetType::Timestamp {
            unit: TimeUnit::Microsecond | TimeUnit::Nanosecond,
            ..
        } => None,
        RivetType::Timestamp { .. } => Some("is a timestamp unit the cursor cannot render"),
        RivetType::Enum if source == SourceType::Mysql => Some(
            "is an ENUM or SET, which sorts by member position, not by the label the cursor binds",
        ),
        RivetType::Enum => None,
        RivetType::Bool => Some("is a boolean, which the cursor cannot render"),
        RivetType::Time { .. } => Some("is a time of day, which the cursor cannot render"),
        RivetType::Binary => Some("is binary, which the cursor cannot render"),
        RivetType::Json => Some("is JSON, which does not sort as text"),
        RivetType::Interval => Some("is an interval, which does not sort as text"),
        RivetType::List { .. } => Some("is an array, which the cursor cannot render"),
        RivetType::Unsupported { .. } => Some("has a type rivet does not read"),
    }
}

/// The cause `column`, as the engine delivers it in `mappings`, cannot key a keyset seek; `None` when it can.
pub(crate) fn column_refusal(
    source: SourceType,
    mappings: &[TypeMapping],
    finer_than_microsecond: &[String],
    column: &str,
) -> Option<&'static str> {
    let Some(mapping) = mappings
        .iter()
        .find(|m| m.column_name == column)
        .or_else(|| {
            mappings
                .iter()
                .find(|m| m.column_name.eq_ignore_ascii_case(column))
        })
    else {
        return Some("is not among the columns the export delivers");
    };
    let finer = finer_than_microsecond
        .iter()
        .any(|c| c.eq_ignore_ascii_case(column));
    keyset_key_refusal(source, mapping, finer)
}

/// Samples the offline tests that sweep every Rivet type share.
#[cfg(test)]
pub(crate) mod samples {
    use crate::types::{RivetType, SourceColumn, TimeUnit as U, TypeMapping};

    /// The mapping of a column named `k` delivered as `rivet_type`.
    pub(crate) fn mapping_of(rivet_type: RivetType) -> TypeMapping {
        TypeMapping::from_source(&SourceColumn::simple("k", "native", false), rivet_type)
    }

    /// A sample of every Rivet type, with the precision, scale and zone variants the seam tells apart.
    pub(crate) fn rivet_type_samples() -> Vec<RivetType> {
        let ts = |unit, tz: Option<&str>| RivetType::Timestamp {
            unit,
            timezone: tz.map(Into::into),
        };
        vec![
            RivetType::Bool,
            RivetType::Int16,
            RivetType::Int32,
            RivetType::Int64,
            RivetType::UInt64,
            RivetType::Float32,
            RivetType::Float64,
            RivetType::Decimal {
                precision: 18,
                scale: 0,
            },
            RivetType::Decimal {
                precision: 30,
                scale: 4,
            },
            RivetType::Date,
            RivetType::Time {
                unit: U::Microsecond,
            },
            RivetType::Time {
                unit: U::Nanosecond,
            },
            ts(U::Second, None),
            ts(U::Millisecond, None),
            ts(U::Microsecond, None),
            ts(U::Microsecond, Some("UTC")),
            ts(U::Nanosecond, None),
            ts(U::Nanosecond, Some("UTC")),
            RivetType::String,
            RivetType::Text,
            RivetType::Binary,
            RivetType::Json,
            RivetType::Uuid,
            RivetType::Enum,
            RivetType::Interval,
            RivetType::List {
                inner: Box::new(RivetType::Int32),
            },
            RivetType::Unsupported {
                native_type: "geometry".into(),
                reason: "no mapping".into(),
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RivetType, SourceColumn, TimeUnit};

    fn named(name: &str, rivet_type: RivetType) -> TypeMapping {
        TypeMapping::from_source(&SourceColumn::simple(name, "native", false), rivet_type)
    }

    /// A zone-carrying timestamp keys a seek everywhere except Oracle.
    #[test]
    fn a_zoned_timestamp_is_refused_on_oracle_alone() {
        let zoned = samples::mapping_of(RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        });
        for source in [SourceType::Postgres, SourceType::Mysql, SourceType::Mssql] {
            assert_eq!(
                keyset_key_refusal(source, &zoned, false),
                None,
                "{source:?}"
            );
        }
        assert_eq!(
            keyset_key_refusal(SourceType::Oracle, &zoned, false),
            Some("is a timestamp with a time zone, which is not a keyset key on Oracle")
        );
    }

    /// A whole-number decimal keys a seek on Oracle alone; a scaled one never does.
    #[test]
    fn a_whole_number_decimal_keys_a_seek_on_oracle_alone() {
        let whole = samples::mapping_of(RivetType::Decimal {
            precision: 18,
            scale: 0,
        });
        let scaled = samples::mapping_of(RivetType::Decimal {
            precision: 18,
            scale: 2,
        });
        for source in [SourceType::Postgres, SourceType::Mysql, SourceType::Mssql] {
            assert!(
                keyset_key_refusal(source, &whole, false).is_some(),
                "{source:?}"
            );
        }
        for source in [
            SourceType::Postgres,
            SourceType::Mysql,
            SourceType::Mssql,
            SourceType::Oracle,
        ] {
            assert!(
                keyset_key_refusal(source, &scaled, false).is_some(),
                "{source:?}"
            );
        }
        assert_eq!(keyset_key_refusal(SourceType::Oracle, &whole, false), None);
    }

    /// The column is found by its own name, then by its name in any case, and is absent otherwise.
    #[test]
    fn a_column_is_looked_up_by_name_then_by_case_then_refused_when_absent() {
        let mappings = [
            named("flag", RivetType::Bool),
            named("id", RivetType::Int64),
        ];
        assert_eq!(
            column_refusal(SourceType::Postgres, &mappings, &[], "id"),
            None
        );
        assert_eq!(
            column_refusal(SourceType::Postgres, &mappings, &[], "ID"),
            None
        );
        assert_eq!(
            column_refusal(SourceType::Postgres, &mappings, &[], "flag"),
            Some("is a boolean, which the cursor cannot render")
        );
        assert_eq!(
            column_refusal(SourceType::Postgres, &mappings, &[], "missing"),
            Some("is not among the columns the export delivers")
        );
    }
}
