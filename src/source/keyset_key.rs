//! The keyset key admission seam: which delivered column types the keyset cursor can advance.

use crate::config::SourceType;
use crate::types::{Delivery, RivetType, TextForm, TimeUnit, TypeMapping};

/// The fractional-second digits a microsecond timestamp keeps.
pub(crate) const MICROSECOND_DIGITS: i32 = 6;

/// What the catalog declares a column holds beyond the type the driver delivers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wider {
    FinerTimestamp,
    InexactNumber,
}

/// Whether the keyset cursor pages one column or a tuple of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyShape {
    Single,
    Composite,
}

/// The cause an Oracle TIMESTAMP finer than a microsecond cannot key a seek, whatever type `columns:` delivers it as.
const ORACLE_FINER_TIMESTAMP: &str = "is a TIMESTAMP with more than 6 fractional-second digits, which the cursor cannot advance past exactly whatever type `columns:` delivers it as";

/// The cause an Oracle NUMBER with a scale or a FLOAT cannot key a seek, whatever type `columns:` delivers it as.
const ORACLE_INEXACT_NUMBER: &str = "is a NUMBER with a scale or a FLOAT, which the cursor cannot advance past exactly whatever type `columns:` delivers it as";

/// The cause a column cannot key a keyset seek, or `None` when the cursor reads it and re-binds it exactly; `wider` is the catalog saying the column holds more than the delivered type shows.
pub(crate) fn keyset_key_refusal(
    source: SourceType,
    mapping: &TypeMapping,
    wider: Option<Wider>,
    shape: KeyShape,
) -> Option<&'static str> {
    if source == SourceType::Oracle {
        match (wider, shape) {
            (Some(Wider::FinerTimestamp), _) => return Some(ORACLE_FINER_TIMESTAMP),
            (Some(Wider::InexactNumber), KeyShape::Single) => return Some(ORACLE_INEXACT_NUMBER),
            _ => {}
        }
    }
    let exact_number_text = source == SourceType::Oracle
        && matches!(mapping.delivery, Delivery::Text(TextForm::DecimalPlain));
    if !exact_number_text && !matches!(mapping.delivery, Delivery::Native) {
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
        } if wider == Some(Wider::FinerTimestamp) => Some(
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
        RivetType::Json if source == SourceType::Postgres => None,
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
    declared_wider: &[(String, Wider)],
    shape: KeyShape,
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
    let wider = declared_wider
        .iter()
        .find(|(c, _)| c.eq_ignore_ascii_case(column))
        .map(|(_, kind)| *kind);
    keyset_key_refusal(source, mapping, wider, shape)
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

    const ENGINES: [SourceType; 4] = [
        SourceType::Postgres,
        SourceType::Mysql,
        SourceType::Mssql,
        SourceType::Oracle,
    ];

    fn named(name: &str, rivet_type: RivetType) -> TypeMapping {
        TypeMapping::from_source(&SourceColumn::simple(name, "native", false), rivet_type)
    }

    fn single(source: SourceType, mapping: &TypeMapping) -> Option<&'static str> {
        keyset_key_refusal(source, mapping, None, KeyShape::Single)
    }

    /// A zone-carrying timestamp keys a seek everywhere except Oracle.
    #[test]
    fn a_zoned_timestamp_is_refused_on_oracle_alone() {
        let zoned = samples::mapping_of(RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        });
        for source in [SourceType::Postgres, SourceType::Mysql, SourceType::Mssql] {
            assert_eq!(single(source, &zoned), None, "{source:?}");
        }
        assert_eq!(
            single(SourceType::Oracle, &zoned),
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
            assert!(single(source, &whole).is_some(), "{source:?}");
        }
        for source in ENGINES {
            assert!(single(source, &scaled).is_some(), "{source:?}");
        }
        assert_eq!(single(SourceType::Oracle, &whole), None);
    }

    /// A NUMBER held as exact decimal text keys a seek on Oracle alone; other text forms never do.
    #[test]
    fn a_number_held_as_exact_text_keys_a_seek_on_oracle_alone() {
        let exact = samples::mapping_of(RivetType::String).with_text(TextForm::DecimalPlain);
        let instant = samples::mapping_of(RivetType::String).with_text(TextForm::IsoTimestampNanos);
        let duration = samples::mapping_of(RivetType::String).with_text(TextForm::Iso8601Duration);
        assert_eq!(single(SourceType::Oracle, &exact), None);
        for source in [SourceType::Postgres, SourceType::Mysql, SourceType::Mssql] {
            assert_eq!(
                single(source, &exact),
                Some("is delivered as text, which does not sort as the stored value"),
                "{source:?}"
            );
        }
        for text in [&instant, &duration] {
            assert_eq!(
                single(SourceType::Oracle, text),
                Some("is delivered as text, which does not sort as the stored value")
            );
        }
    }

    /// An Oracle column the catalog declares wider is refused whatever type `columns:` delivers it as.
    #[test]
    fn an_oracle_column_the_catalog_declares_wider_is_refused_whatever_it_is_delivered_as() {
        let deliveries = [
            samples::mapping_of(RivetType::String).with_text(TextForm::DecimalPlain),
            samples::mapping_of(RivetType::Int64),
            samples::mapping_of(RivetType::String),
            samples::mapping_of(RivetType::Float64),
            samples::mapping_of(RivetType::Decimal {
                precision: 18,
                scale: 0,
            }),
            samples::mapping_of(RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            }),
            samples::mapping_of(RivetType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: None,
            }),
        ];
        for mapping in &deliveries {
            let finer = keyset_key_refusal(
                SourceType::Oracle,
                mapping,
                Some(Wider::FinerTimestamp),
                KeyShape::Single,
            );
            assert_eq!(finer, Some(ORACLE_FINER_TIMESTAMP), "{mapping:?}");
            let composite = keyset_key_refusal(
                SourceType::Oracle,
                mapping,
                Some(Wider::FinerTimestamp),
                KeyShape::Composite,
            );
            assert_eq!(composite, Some(ORACLE_FINER_TIMESTAMP), "{mapping:?}");
            let inexact = keyset_key_refusal(
                SourceType::Oracle,
                mapping,
                Some(Wider::InexactNumber),
                KeyShape::Single,
            );
            assert_eq!(inexact, Some(ORACLE_INEXACT_NUMBER), "{mapping:?}");
        }
    }

    /// A composite Oracle key ignores the inexact-number fact, as main did; a finer timestamp is refused in either shape.
    #[test]
    fn an_inexact_oracle_number_is_refused_as_a_single_key_alone() {
        let exact = samples::mapping_of(RivetType::String).with_text(TextForm::DecimalPlain);
        assert_eq!(
            keyset_key_refusal(
                SourceType::Oracle,
                &exact,
                Some(Wider::InexactNumber),
                KeyShape::Composite
            ),
            None
        );
    }

    /// Outside Oracle the finer-timestamp fact refuses a microsecond delivery and admits a nanosecond one.
    #[test]
    fn a_finer_timestamp_is_admitted_in_nanoseconds_outside_oracle() {
        let ts = |unit| {
            samples::mapping_of(RivetType::Timestamp {
                unit,
                timezone: None,
            })
        };
        for source in [SourceType::Postgres, SourceType::Mysql, SourceType::Mssql] {
            let wider = Some(Wider::FinerTimestamp);
            assert_eq!(
                keyset_key_refusal(source, &ts(TimeUnit::Microsecond), wider, KeyShape::Single),
                Some(
                    "carries more than 6 fractional-second digits, which the microsecond cursor truncates"
                ),
                "{source:?}"
            );
            assert_eq!(
                keyset_key_refusal(source, &ts(TimeUnit::Nanosecond), wider, KeyShape::Single),
                None,
                "{source:?}"
            );
        }
    }

    /// JSON keys a seek on PostgreSQL alone, where jsonb has its own btree order the server applies to both sides.
    #[test]
    fn json_keys_a_seek_on_postgres_alone() {
        let json = samples::mapping_of(RivetType::Json);
        assert_eq!(single(SourceType::Postgres, &json), None);
        for source in [SourceType::Mysql, SourceType::Mssql, SourceType::Oracle] {
            assert_eq!(
                single(source, &json),
                Some("is JSON, which does not sort as text"),
                "{source:?}"
            );
        }
    }

    /// The column is found by its own name, then by its name in any case, and is absent otherwise.
    #[test]
    fn a_column_is_looked_up_by_name_then_by_case_then_refused_when_absent() {
        let mappings = [
            named("flag", RivetType::Bool),
            named("id", RivetType::Int64),
        ];
        let find = |column| {
            column_refusal(
                SourceType::Postgres,
                &mappings,
                &[],
                KeyShape::Single,
                column,
            )
        };
        assert_eq!(find("id"), None);
        assert_eq!(find("ID"), None);
        assert_eq!(
            find("flag"),
            Some("is a boolean, which the cursor cannot render")
        );
        assert_eq!(
            find("missing"),
            Some("is not among the columns the export delivers")
        );
    }

    /// The catalog fact is matched to the column by name in any case.
    #[test]
    fn the_catalog_fact_is_matched_to_the_column_in_any_case() {
        let mappings = [named("K", RivetType::Int64)];
        let wider = [("k".to_string(), Wider::InexactNumber)];
        assert_eq!(
            column_refusal(SourceType::Oracle, &mappings, &wider, KeyShape::Single, "K"),
            Some(ORACLE_INEXACT_NUMBER)
        );
    }
}
