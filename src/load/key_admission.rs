//! Admission of a keyed load's key and order columns by the type they land as.

use crate::error::Result;
use crate::plan::load_settings::LoadMode;
use crate::preflight::type_report::TypeReportRow;
use crate::types::{RivetType, TimeUnit, TypeMapping};

/// How a column lands in the file, against what a key or an order needs of it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum KeyLanding {
    #[default]
    Exact,
    NumberAsText,
    FinerThanMicrosecond,
}

impl KeyLanding {
    /// The landing of one resolved mapping; `finer` is the catalog fact that the source declares the column finer than a microsecond.
    pub fn of(m: &TypeMapping, finer: bool) -> Self {
        match &m.rivet_type {
            RivetType::String | RivetType::Text if orders_as_number(&m.source_native_type) => {
                Self::NumberAsText
            }
            RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                ..
            } if finer => Self::FinerThanMicrosecond,
            _ => Self::Exact,
        }
    }
}

/// Whether the source type orders by value (numbers, intervals), which its text form does not.
fn orders_as_number(native: &str) -> bool {
    let lower = native.trim().to_ascii_lowercase();
    let head = lower
        .split(|c: char| c == '(' || c.is_whitespace())
        .next()
        .unwrap_or("");
    matches!(
        head,
        "number"
            | "numeric"
            | "decimal"
            | "dec"
            | "float"
            | "double"
            | "real"
            | "int"
            | "integer"
            | "bigint"
            | "smallint"
            | "tinyint"
            | "mediumint"
            | "int2"
            | "int4"
            | "int8"
            | "float4"
            | "float8"
            | "money"
            | "smallmoney"
            | "serial"
            | "bigserial"
            | "smallserial"
            | "binary_float"
            | "binary_double"
            | "interval"
    )
}

/// One column's landing with the source type it came from.
#[derive(Debug, Clone)]
pub struct ColumnLanding {
    pub column: String,
    pub source_type: String,
    pub landing: KeyLanding,
}

/// The landing of every row of a type report.
pub fn landings(rows: &[TypeReportRow]) -> Vec<ColumnLanding> {
    rows.iter()
        .map(|r| ColumnLanding {
            column: r.column.clone(),
            source_type: r.source_type.clone(),
            landing: r.key_landing,
        })
        .collect()
}

/// The landing of every mapping, `finer` naming the columns the source declares finer than a microsecond.
pub fn landings_of_mappings(mappings: &[TypeMapping], finer: &[String]) -> Vec<ColumnLanding> {
    mappings
        .iter()
        .map(|m| ColumnLanding {
            column: m.column_name.clone(),
            source_type: m.source_native_type.clone(),
            landing: KeyLanding::of(m, finer.contains(&m.column_name)),
        })
        .collect()
}

/// Refuses a keyed load whose order column lands as text, or whose pk column lands finer-collapsed.
pub fn admit_load_keys(
    export: &str,
    mode: LoadMode,
    cursor: Option<&str>,
    pk: &[String],
    columns: &[ColumnLanding],
) -> Result<()> {
    let find = |name: &str| {
        columns.iter().find(|c| {
            let folded = super::latin_fold(&c.column).unwrap_or_else(|| c.column.clone());
            folded.eq_ignore_ascii_case(name)
        })
    };
    if mode == LoadMode::Incremental
        && let Some(col) = cursor.and_then(find)
        && col.landing == KeyLanding::NumberAsText
    {
        crate::rivet_bail!(
            crate::error::codes::LOAD_KEY_COLUMN_LANDS_INEXACT,
            "export `{export}`: the incremental load orders its current-state view by cursor \
             column `{}`, a `{}` in the source that lands as text; text orders `99` after \
             `100`, so a row updated across a digit boundary would keep its stale version in \
             the view. Declare `{}` under the export's `columns:` with a numeric type \
             (`int64` or `decimal(p,s)`), or cast it in the export's `query:`, then run the \
             export again and reload. If the table was already loaded, first drop its change \
             log and its view and run `rivet state reset --export {export}`: a loaded column \
             keeps its type",
            col.column,
            col.source_type,
            col.column
        );
    }
    if matches!(mode, LoadMode::Incremental | LoadMode::Cdc)
        && let Some(col) = pk
            .iter()
            .filter_map(|p| find(p))
            .find(|c| c.landing == KeyLanding::FinerThanMicrosecond)
    {
        crate::rivet_bail!(
            crate::error::codes::LOAD_KEY_COLUMN_LANDS_INEXACT,
            "export `{export}`: primary-key column `{}` is a `{}` that the source declares \
             finer than a microsecond and lands at microsecond precision, so rows that differ \
             only below it would collapse into one in the current-state view. Declare `{}` \
             under the export's `columns:` as `timestamp_ns` (`timestamp_tz_ns` for a zoned \
             column) to keep every digit, or name a `pk:` that does not depend on it, then run \
             `rivet state reset --export {export}` and the export again, and reload: parts \
             already extracted hold microseconds. If the table was already loaded, first drop \
             its change log and its view: a loaded column keeps its type",
            col.column,
            col.source_type,
            col.column
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SourceColumn, TextForm, TypeFidelity};

    fn mapping(native: &str, rivet: RivetType) -> TypeMapping {
        TypeMapping::from_source(&SourceColumn::simple("K", native, false), rivet)
    }

    fn micros() -> RivetType {
        RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        }
    }

    fn column(name: &str, native: &str, landing: KeyLanding) -> ColumnLanding {
        ColumnLanding {
            column: name.into(),
            source_type: native.into(),
            landing,
        }
    }

    fn said(r: Result<()>) -> (Option<&'static str>, String) {
        let err = r.unwrap_err();
        (crate::error::error_code(&err), format!("{err:#}"))
    }

    const CODE: &str = "RIVET_LOAD_KEY_COLUMN_LANDS_INEXACT";

    /// A numeric source type that lands as text is `NumberAsText`; a native text type, an exact number and every other type stay `Exact`.
    #[test]
    fn a_numeric_source_type_landing_as_text_is_told_from_native_text() {
        for native in [
            "number",
            "NUMBER",
            "number(38,0)",
            "numeric",
            "numeric(50,2)",
            "decimal(40,10)",
            "float",
            "float(126)",
            "double precision",
            "binary_double",
            "bigint",
            "money",
            "interval",
            "interval day to second",
        ] {
            for ty in [RivetType::String, RivetType::Text] {
                assert_eq!(
                    KeyLanding::of(&mapping(native, ty.clone()), false),
                    KeyLanding::NumberAsText,
                    "{native} landing as {ty:?}"
                );
            }
        }
        for native in [
            "varchar",
            "varchar2",
            "nvarchar(64)",
            "text",
            "char(10)",
            "uuid",
            "objectid",
            "long",
            "clob",
            "enum('a','b')",
            "citext",
            "",
        ] {
            assert_eq!(
                KeyLanding::of(&mapping(native, RivetType::String), false),
                KeyLanding::Exact,
                "{native} is text in the source too"
            );
        }
        for (native, ty) in [
            ("number", RivetType::Int64),
            ("number(10,0)", RivetType::Int32),
            (
                "number(12,2)",
                RivetType::Decimal {
                    precision: 12,
                    scale: 2,
                },
            ),
            ("float", RivetType::Float64),
            ("date", micros()),
        ] {
            assert_eq!(
                KeyLanding::of(&mapping(native, ty.clone()), false),
                KeyLanding::Exact,
                "{native} lands as {ty:?}, a type that orders by value"
            );
        }
    }

    /// A microsecond landing is `FinerThanMicrosecond` only when the source declares the column finer; a nanosecond landing, another unit and an unflagged column stay `Exact`.
    #[test]
    fn a_microsecond_landing_is_finer_only_where_the_source_says_so() {
        let ts = mapping("datetime2", micros());
        assert_eq!(KeyLanding::of(&ts, true), KeyLanding::FinerThanMicrosecond);
        assert_eq!(KeyLanding::of(&ts, false), KeyLanding::Exact);
        let ns = mapping(
            "datetime2",
            RivetType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: None,
            },
        );
        assert_eq!(KeyLanding::of(&ns, true), KeyLanding::Exact);
        assert_eq!(
            KeyLanding::of(&mapping("int", RivetType::Int32), true),
            KeyLanding::Exact
        );
    }

    /// The text landing of a bare Oracle NUMBER, and the text a number is delivered as, are the same fact.
    #[test]
    fn a_text_number_with_its_plain_decimal_form_is_a_text_landing() {
        let m = mapping("number", RivetType::String).with_text(TextForm::DecimalPlain);
        assert_eq!(KeyLanding::of(&m, false), KeyLanding::NumberAsText);
        let m = TypeMapping {
            fidelity: TypeFidelity::Exact,
            ..m
        };
        assert_eq!(KeyLanding::of(&m, false), KeyLanding::NumberAsText);
    }

    /// The incremental load refuses a cursor that lands as text, by code, naming the column, its source type and the remedy; every other mode, cursor and landing is admitted.
    #[test]
    fn an_incremental_cursor_landing_as_text_is_refused_with_its_remedy() {
        let cols = [
            column("ID", "number(10)", KeyLanding::Exact),
            column("VER", "number", KeyLanding::NumberAsText),
        ];
        let pk = ["ID".to_string()];
        let (code, text) = said(admit_load_keys(
            "e",
            LoadMode::Incremental,
            Some("VER"),
            &pk,
            &cols,
        ));
        assert_eq!(code, Some(CODE));
        assert_eq!(
            text,
            "export `e`: the incremental load orders its current-state view by cursor column \
             `VER`, a `number` in the source that lands as text; text orders `99` after `100`, \
             so a row updated across a digit boundary would keep its stale version in the view. \
             Declare `VER` under the export's `columns:` with a numeric type (`int64` or \
             `decimal(p,s)`), or cast it in the export's `query:`, then run the export again \
             and reload. If the table was already loaded, first drop its change log and its \
             view and run `rivet state reset --export e`: a loaded column keeps its type"
        );
        for mode in [LoadMode::Full, LoadMode::Cdc] {
            admit_load_keys("e", mode, Some("VER"), &pk, &cols)
                .unwrap_or_else(|e| panic!("{mode:?} orders by no cursor: {e:#}"));
        }
        admit_load_keys("e", LoadMode::Incremental, None, &pk, &cols).unwrap();
        admit_load_keys("e", LoadMode::Incremental, Some("ID"), &pk, &cols).unwrap();
        admit_load_keys("e", LoadMode::Incremental, Some("ver"), &pk, &cols)
            .expect_err("the cursor is matched the way the load matches a column: by folded name");
        admit_load_keys("e", LoadMode::Incremental, Some("missing"), &pk, &cols).unwrap();
        let native_text = [column("VER", "varchar2", KeyLanding::Exact)];
        admit_load_keys("e", LoadMode::Incremental, Some("VER"), &pk, &native_text).unwrap();
    }

    /// A primary-key column landed finer-collapsed is refused for the incremental and CDC loads, naming the column and the `timestamp_ns` remedy; `full`, another key and an exact column are admitted.
    #[test]
    fn a_primary_key_column_collapsing_below_a_microsecond_is_refused_with_its_remedy() {
        let cols = [
            column("dev", "int", KeyLanding::Exact),
            column("at", "datetime2", KeyLanding::FinerThanMicrosecond),
            column("seq", "int", KeyLanding::Exact),
        ];
        let pk = ["dev".to_string(), "at".to_string()];
        for mode in [LoadMode::Incremental, LoadMode::Cdc] {
            let (code, text) = said(admit_load_keys("e", mode, Some("seq"), &pk, &cols));
            assert_eq!(code, Some(CODE), "{mode:?}");
            assert_eq!(
                text,
                "export `e`: primary-key column `at` is a `datetime2` that the source declares \
                 finer than a microsecond and lands at microsecond precision, so rows that \
                 differ only below it would collapse into one in the current-state view. \
                 Declare `at` under the export's `columns:` as `timestamp_ns` \
                 (`timestamp_tz_ns` for a zoned column) to keep every digit, or name a `pk:` \
                 that does not depend on it, then run `rivet state reset --export e` and the \
                 export again, and reload: parts already extracted hold microseconds. If the \
                 table was already loaded, first drop its change log and its view: a loaded \
                 column keeps its type",
                "{mode:?}"
            );
        }
        admit_load_keys("e", LoadMode::Full, Some("seq"), &pk, &cols).unwrap();
        admit_load_keys(
            "e",
            LoadMode::Incremental,
            Some("seq"),
            &["dev".to_string()],
            &cols,
        )
        .unwrap();
        let exact = [column("at", "datetime2", KeyLanding::Exact)];
        admit_load_keys(
            "e",
            LoadMode::Incremental,
            Some("at"),
            &["at".to_string()],
            &exact,
        )
        .unwrap();
    }

    /// A cursor that is itself finer than a microsecond, and a text primary key, are not what this admission refuses.
    #[test]
    fn a_text_primary_key_and_a_finer_cursor_are_admitted() {
        let cols = [
            column("code", "number", KeyLanding::NumberAsText),
            column("at", "datetime2", KeyLanding::FinerThanMicrosecond),
        ];
        admit_load_keys(
            "e",
            LoadMode::Incremental,
            Some("at"),
            &["code".to_string()],
            &cols,
        )
        .unwrap();
    }
}
