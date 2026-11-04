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
        RivetType::Decimal { .. } if source == SourceType::Oracle => None,
        RivetType::Decimal { .. } => {
            Some("is a decimal, which the cursor cannot advance past exactly")
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
