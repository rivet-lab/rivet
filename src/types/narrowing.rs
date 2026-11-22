//! The grading of a `columns:` override that keeps less than its source holds: the one place `check`, the type report and the CDC run decide `lossy`.

use crate::error::Result;

use super::{ColumnOverrides, RivetType, TypeFidelity, TypeMapping};

/// #32 (column-applicability): downgrade every overridden column whose override *narrows* what the source holds to [`TypeFidelity::Lossy`]: a decimal that loses digits, a time type that keeps fewer fraction digits than a result column finer than a microsecond.
pub(crate) fn downgrade_narrowing_overrides(
    mappings: &mut [TypeMapping],
    column_overrides: &ColumnOverrides,
    reprobe: impl FnOnce() -> Result<Vec<TypeMapping>>,
    finer_columns: impl FnOnce() -> Result<Vec<String>>,
) -> Result<()> {
    if column_overrides.is_empty() {
        return Ok(());
    }
    let source_mappings = reprobe()?;
    let source_by_name: std::collections::HashMap<&str, &RivetType> = source_mappings
        .iter()
        .map(|m| (m.column_name.as_str(), &m.rivet_type))
        .collect();
    let truncates = |m: &TypeMapping| {
        column_overrides.contains_key(&m.column_name)
            && kept_fraction_digits(&m.rivet_type).is_some_and(|kept| kept < 9)
    };
    let finer = mappings.iter().any(truncates).then(finer_columns);
    for m in mappings {
        if !column_overrides.contains_key(&m.column_name) {
            continue;
        }
        let finer_here = match &finer {
            Some(Ok(columns)) => columns.contains(&m.column_name),
            Some(Err(e)) if truncates(m) => {
                m.warnings.push(format!(
                    "the source could not say whether it holds more fraction digits of a second \
                     than this type keeps ({e:#}); a column finer than a microsecond is truncated"
                ));
                false
            }
            _ => false,
        };
        if let Some(&src_type) = source_by_name.get(m.column_name.as_str())
            && let Some(reason) = override_narrows(src_type, &m.rivet_type, finer_here)
        {
            m.fidelity = TypeFidelity::Lossy;
            m.warnings.push(reason);
        }
    }
    Ok(())
}

/// The fraction digits of a second a time type keeps; `None` for any other type.
fn kept_fraction_digits(t: &RivetType) -> Option<u8> {
    use crate::types::{RivetType, TimeUnit};
    let unit = match t {
        RivetType::Timestamp { unit, .. } | RivetType::Time { unit } => unit,
        RivetType::Date => return Some(0),
        _ => return None,
    };
    Some(match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    })
}

/// Detect whether a `columns:` override *narrows* the autodetected source type in a value-losing way; `finer_than_microsecond` says the result column holds more than a microsecond.
pub(crate) fn override_narrows(
    source: &RivetType,
    overridden: &RivetType,
    finer_than_microsecond: bool,
) -> Option<String> {
    use RivetType::Decimal;
    if finer_than_microsecond
        && let Some(kept) = kept_fraction_digits(overridden)
        && kept < 9
        && seeded!("narrowing-override-reported-exact", false, true)
    {
        return Some(format!(
            "the source column holds more than 6 fraction digits of a second and this override \
             keeps {kept} — the finer digits are truncated at run; this is lossy, not exact. To \
             keep them, drop the override on SQL Server (`datetime2(7)` and `datetimeoffset(7)` \
             keep nanoseconds by default) or declare `timestamp_ns` / `timestamp_tz_ns` on Oracle"
        ));
    }
    if let (
        Decimal {
            precision: sp,
            scale: ss,
        },
        Decimal {
            precision: op,
            scale: os,
        },
    ) = (source, overridden)
    {
        if os < ss {
            return Some(format!(
                "override decimal({op},{os}) reduces scale from source numeric({sp},{ss}) — \
                 {} fractional digit(s) are truncated at run; this is lossy, not exact",
                (*ss as i16) - (*os as i16)
            ));
        }
        let src_int_digits = *sp as i16 - *ss as i16;
        let ov_int_digits = *op as i16 - *os as i16;
        if ov_int_digits < src_int_digits {
            return Some(format!(
                "override decimal({op},{os}) reduces integer-digit capacity from source \
                 numeric({sp},{ss}) — large values overflow at run; this is lossy, not exact"
            ));
        }
    }
    if let (Decimal { precision, scale }, RivetType::Float64) = (source, overridden)
        && seeded!(
            "decimal-float8-override-reported-exact",
            false,
            *precision > crate::types::decimal::F64_DECIMAL_DIGITS
        )
    {
        return Some(format!(
            "override float8 keeps {} significant digits exactly and the source numeric({precision},{scale}) \
             holds up to {precision} — a value the float cannot hold exactly is refused at run \
             (RIVET_SOURCE_VALUE_UNREPRESENTABLE); this is lossy, not exact",
            crate::types::decimal::F64_DECIMAL_DIGITS
        ));
    }
    None
}
