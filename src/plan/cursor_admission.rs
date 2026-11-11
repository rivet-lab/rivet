//! The incremental cursor admission seam: which `cursor_column` the cursor can advance past without losing a row.

use arrow::datatypes::{DataType, TimeUnit};

use crate::config::{Config, ExportConfig, IncrementalCursorMode, SourceType};
use crate::error::Result;
use crate::source::Source;
use crate::types::{TypeMapping, parse_column_overrides};

/// Whether an array of `data_type` is a type the cursor renderer reads: the one set the plan admits a cursor column by.
pub(crate) fn cursor_renders(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::Date32
            | DataType::FixedSizeBinary(16)
            | DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
    )
}

/// Why a cursor column cannot carry an incremental export.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CursorCause {
    Nullable,
    Type(String),
}

/// The cause `mapping` read as nullable `nullable` cannot be the primary cursor of `mode`, or `None` when it can.
pub(crate) fn cursor_cause(
    mapping: &TypeMapping,
    nullable: Option<bool>,
    mode: IncrementalCursorMode,
) -> Option<CursorCause> {
    match mapping.arrow_type.as_ref() {
        Some(t) if cursor_renders(t) => {}
        Some(t) => {
            return Some(CursorCause::Type(format!(
                "lands as {t}, a type the cursor cannot render"
            )));
        }
        None => {
            return Some(CursorCause::Type(
                "has no type the cursor can render".to_string(),
            ));
        }
    }
    (nullable == Some(true) && mode == IncrementalCursorMode::SingleColumn)
        .then_some(CursorCause::Nullable)
}

/// Refuse the export when its primary cursor column can hold NULL or has a type the cursor cannot render; a source that cannot be asked is warned about, not refused.
pub(crate) fn admit_incremental_cursor(
    probe: Option<&mut (dyn Source + '_)>,
    config: &Config,
    export: &ExportConfig,
    query: &str,
    column: &str,
) -> Result<()> {
    let source = config.source.source_type;
    if source == SourceType::Mongo {
        return Ok(());
    }
    let mut owned: Box<dyn Source>;
    let src: &mut dyn Source = match probe {
        Some(src) => src,
        None => match super::build::connect_source(&config.source) {
            Ok(conn) => {
                owned = conn;
                owned.as_mut()
            }
            Err(e) => return unverified(export, column, &e),
        },
    };
    let overrides = crate::types::overrides_for_unit(
        &parse_column_overrides(&export.columns, &export.name)?,
        export.table.as_deref(),
    );
    let mappings = match src.type_mappings(query, &overrides) {
        Ok(m) => m,
        Err(e) => return unverified(export, column, &e),
    };
    let Some(mapping) = mappings.iter().find(|m| m.column_name == column) else {
        return Ok(());
    };
    let nullable = match src.column_nullable(query, column) {
        Ok(n) => n,
        Err(e) => return unverified(export, column, &e),
    };
    match cursor_cause(mapping, nullable, export.incremental_cursor_mode) {
        None => Ok(()),
        Some(CursorCause::Nullable) => crate::rivet_bail!(
            crate::error::codes::PLAN_CURSOR_NULLABLE,
            "export '{}': cursor_column '{column}' can hold NULL; the first run delivers the rows \
             whose cursor is NULL and no later run does, so a NULL-cursor row inserted afterwards is never \
             delivered; nothing was read or written. Make the cursor column NOT NULL, or set \
             `incremental_cursor_mode: coalesce` with a NOT NULL `cursor_fallback_column:` and run \
             `rivet state reset -c <config> --export {}` once",
            export.name,
            export.name
        ),
        Some(CursorCause::Type(cause)) => crate::rivet_bail!(
            crate::error::codes::PLAN_CURSOR_TYPE_UNSUPPORTED,
            "export '{}': cursor_column '{column}' {cause}, so the cursor could never advance past it \
             and every run would re-read the whole table; nothing was read or written. Use an integer, \
             decimal, text, date, timestamp or uuid `cursor_column:`, or use `mode: chunked` or `mode: full`",
            export.name
        ),
    }
}

/// The warning for a cursor column the source could not be asked about.
fn unverified(export: &ExportConfig, column: &str, cause: &anyhow::Error) -> Result<()> {
    log::warn!(
        "export '{}': cursor_column '{column}' was not checked for NULLs or a renderable type: {cause}",
        export.name
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::keyset_key::samples::{mapping_of, rivet_type_samples};
    use crate::types::RivetType;

    const MODES: [IncrementalCursorMode; 2] = [
        IncrementalCursorMode::SingleColumn,
        IncrementalCursorMode::Coalesce,
    ];

    /// A NOT NULL integer cursor is admitted in both modes, and an undescribed nullability is not claimed.
    #[test]
    fn a_not_null_or_undescribed_cursor_is_admitted() {
        for mode in MODES {
            for nullable in [Some(false), None] {
                let got = cursor_cause(&mapping_of(RivetType::Int64), nullable, mode);
                assert_eq!(got, None, "{mode:?} {nullable:?}");
            }
        }
    }

    /// A nullable cursor is refused in single-column mode and admitted under coalesce.
    #[test]
    fn a_nullable_cursor_is_refused_unless_coalesced() {
        let m = mapping_of(RivetType::Int64);
        let single = IncrementalCursorMode::SingleColumn;
        assert_eq!(
            cursor_cause(&m, Some(true), single),
            Some(CursorCause::Nullable)
        );
        let coalesce = IncrementalCursorMode::Coalesce;
        assert_eq!(cursor_cause(&m, Some(true), coalesce), None);
    }

    /// A type the cursor cannot render is refused with its cause in both modes, nullable or not.
    #[test]
    fn an_unrenderable_cursor_is_refused_with_its_cause() {
        for rivet_type in rivet_type_samples() {
            let mapping = mapping_of(rivet_type.clone());
            let renders = mapping.arrow_type.as_ref().is_some_and(cursor_renders);
            for mode in MODES {
                let got = cursor_cause(&mapping, Some(false), mode);
                assert_eq!(
                    matches!(got, Some(CursorCause::Type(_))),
                    !renders,
                    "{rivet_type:?} {mode:?}"
                );
            }
        }
    }

    /// The exact cause named for a boolean.
    #[test]
    fn the_cause_names_the_landed_type() {
        let single = IncrementalCursorMode::SingleColumn;
        let got = cursor_cause(&mapping_of(RivetType::Bool), Some(false), single);
        assert_eq!(
            got,
            Some(CursorCause::Type(
                "lands as Boolean, a type the cursor cannot render".to_string()
            ))
        );
    }

    /// A decimal cursor, which the keyset seam refuses, stays admitted.
    #[test]
    fn a_decimal_cursor_is_admitted() {
        let decimal = RivetType::Decimal {
            precision: 18,
            scale: 2,
        };
        let got = cursor_cause(
            &mapping_of(decimal),
            Some(false),
            IncrementalCursorMode::SingleColumn,
        );
        assert_eq!(got, None);
    }
}
