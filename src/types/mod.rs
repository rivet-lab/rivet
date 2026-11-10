//! Rivet's internal type system.
//!
//! See `rivet_roadmap.md` §Epic 14 (Warehouse Load Layer). North Star —
//! *"No silent type degradation"* — is enforced architecturally by
//! routing every source-column type through the pipeline:
//!
//! ```text
//! Source Native Type
//!     ↓
//! SourceColumn  ← what the driver knows about the column
//!     ↓
//! RivetType     ← canonical, vendor-independent type
//!     ↓
//! TypePolicy    ← strict / lossy / unsupported decisions  (Chunk 4)
//!     ↓
//! Arrow DataType + Field metadata  ← physical export type
//! ```
//!
//! This module owns the first three boxes. The fourth (Arrow) is built by
//! [`mapping::build_arrow_field`]; the fifth (TypePolicy) lands in Chunk 4
//! of the type-safety milestones (see roadmap §18).
//!
//! ## Layer
//!
//! Layer-classification (ADR-0003): this module is **planning-layer** — it
//! only describes / classifies types. It must not perform I/O, log
//! metrics, or hold any pipeline state. Vendor mappers live in
//! `crate::source::*` and call into this module.

mod cursor;
pub mod decimal;
mod delivery;
mod fidelity;
pub(crate) mod ident;
mod mapping;
pub(crate) mod override_table;
mod override_type;
pub mod policy;
mod rivet_type;
mod source_column;
pub mod target;

pub use cursor::CursorState;
#[allow(unused_imports)]
pub use delivery::{
    Delivery, TextForm, bit_string, decimal_plain, hex_bytes, iso_timestamp_nanos,
    iso8601_duration, time_beyond_day, time_of_day_offset, uuid36,
};
pub use fidelity::TypeFidelity;
// Public surface for contract/integration tests; not referenced from the binary.
#[allow(unused_imports)]
pub use mapping::{TypeMapping, build_arrow_field, derive_fidelity, rivet_type_to_arrow};
pub use override_table::{override_is_read, resolve_column};
pub use override_type::{parse_column_overrides, parse_type_str};
pub(crate) use override_type::{refuse_cursor_case_miss, refuse_override_case_miss};
pub use policy::plan_columns;
pub use rivet_type::{RivetType, TimeUnit};
pub use source_column::SourceColumn;
// ColumnOverride is the planned public API for column type overrides (Chunk 6).
#[allow(unused_imports)]
pub use source_column::ColumnOverride;

/// Per-export column type overrides: column name → declared [`RivetType`].
///
/// Built at plan time from the `columns:` map in `rivet.yaml` (roadmap §8).
/// Passed to [`crate::source::Source::export`] so drivers can use the
/// declared precision/scale instead of autodetected (often unavailable) metadata.
pub type ColumnOverrides = std::collections::HashMap<String, RivetType>;

/// Microseconds in one day: an Arrow/Parquet `Time64` holds `[0, MICROS_PER_DAY)`.
pub const MICROS_PER_DAY: i64 = 86_400_000_000;

/// True when `us` is a time of day an Arrow/Parquet `Time64` can hold.
pub fn is_time_of_day(us: i64) -> bool {
    (0..MICROS_PER_DAY).contains(&us)
}

/// Nanoseconds since the Unix epoch of `dt` read as UTC, or the coded refusal naming `column` when `dt` is outside Arrow's 1677-09-21..2262-04-11.
pub fn timestamp_nanos_or_refuse(
    dt: chrono::NaiveDateTime,
    source: &str,
    column: &str,
) -> anyhow::Result<i64> {
    dt.and_utc().timestamp_nanos_opt().ok_or_else(|| {
        crate::error::coded(
            crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
            format!(
                "{source}: column `{column}` holds {dt}, outside the nanosecond timestamp range \
                 (1677-09-21..2262-04-11) its `timestamp_ns` / `timestamp_tz_ns` override declares, \
                 and writing it as NULL would lose the value. Declare the column `timestamp` \
                 (microseconds, full range) or `timestamp_tz` instead, or CAST it in the export's `query:`."
            ),
        )
    })
}

/// Narrow a parsed override map to ONE table: bare keys (`amount`) apply to
/// every table; qualified keys (`orders.amount`) apply only to their table and
/// WIN over a bare key for the same column. Foreign-qualified keys are
/// excluded entirely. `table` is the bare table name (no schema part).
/// This is what makes `columns:` safe on a schema-wide multi-table export —
/// one table's override can never bleed into a same-named column elsewhere.
pub fn overrides_for_table<V: Clone>(
    all: &std::collections::HashMap<String, V>,
    table: &str,
) -> std::collections::HashMap<String, V> {
    let mut out = std::collections::HashMap::new();
    // Bare keys first…
    for (k, v) in all {
        if !k.contains('.') {
            out.insert(k.clone(), v.clone());
        }
    }
    // …then this table's qualified keys, overwriting.
    for (k, v) in all {
        if let Some((t, col)) = k.split_once('.')
            && t == table
        {
            out.insert(col.to_string(), v.clone());
        }
    }
    out
}

/// The overrides that apply to ONE resolution unit — the rule every consumer of
/// `columns:` must key on, in ONE place.
///
/// A unit is the thing a `columns:` key can name: one `tables:` entry of a
/// multiplex CDC export, or a batch/single-table export's own `table:`. Either
/// spelling may carry a schema (`public.orders`), and [`overrides_for_table`]'s
/// contract is the BARE name — so the schema is stripped here rather than at
/// each caller, which is exactly what drifted: the capture
/// (`plan::build::build_plan`, `pipeline::cdc_job`) rsplit the schema off while
/// the type resolver's single-table arm passed the whole map through UNNARROWED,
/// so a qualified key (`orders.amount`) on a `table: public.orders` export never
/// matched a column (the lookup is by BARE column name,
/// `types::resolve_or(overrides, col.name(), …)`). The capture APPLIED the
/// override and the resolver DROPPED it; since `rivet load` reads that report
/// directly to build the warehouse DDL, the created column took the raw catalog
/// type while the Parquet already held the overridden one.
///
/// `None` means the unit has no table at all (a `query:` export) — config-load
/// already refuses a qualified key there, so the map passes through whole.
pub fn overrides_for_unit<V: Clone>(
    all: &std::collections::HashMap<String, V>,
    unit_table: Option<&str>,
) -> std::collections::HashMap<String, V> {
    match unit_table {
        Some(t) => overrides_for_table(all, t.rsplit('.').next().unwrap_or(t)),
        None => all.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overrides_for_table_bare_applies_qualified_wins_foreign_excluded() {
        let mut all = ColumnOverrides::new();
        all.insert("v".into(), RivetType::Text);
        all.insert("orders.v".into(), RivetType::Int64);
        all.insert("ghost.v".into(), RivetType::Bool);

        let orders = overrides_for_table(&all, "orders");
        assert_eq!(orders.get("v"), Some(&RivetType::Int64), "qualified wins");
        let users = overrides_for_table(&all, "users");
        assert_eq!(users.get("v"), Some(&RivetType::Text), "bare fallback");
        assert_eq!(users.len(), 1, "foreign-qualified keys never leak");
    }

    #[test]
    fn is_time_of_day_is_the_half_open_day() {
        assert!(is_time_of_day(0));
        assert!(is_time_of_day(86_399_999_999));
        assert!(!is_time_of_day(86_400_000_000));
        assert!(!is_time_of_day(-1));
    }

    use crate::types::mapping::{META_FIDELITY, META_LOGICAL_TYPE, META_NATIVE_TYPE};
    use arrow::datatypes::DataType;

    /// Top-level smoke test: feeding a typical PostgreSQL `payments` table
    /// through `SourceColumn → RivetType → Arrow Field` produces the schema
    /// shape demanded by the roadmap's §20 "Definition of Done":
    ///
    /// ```text
    /// id          bigint          int64            Int64                  exact
    /// amount      numeric(18,2)   decimal(18,2)    Decimal128(18,2)       exact
    /// created_at  timestamptz     timestamp_tz     Timestamp(us, UTC)     exact
    /// payload     jsonb           json             Utf8 + metadata        logical_string
    /// ```
    #[test]
    fn end_to_end_payments_schema_matches_definition_of_done() {
        let cols: Vec<(SourceColumn, RivetType)> = vec![
            (
                SourceColumn::simple("id", "bigint", false),
                RivetType::Int64,
            ),
            (
                SourceColumn::decimal("amount", "numeric", false, 18, 2),
                RivetType::Decimal {
                    precision: 18,
                    scale: 2,
                },
            ),
            (
                SourceColumn::simple("created_at", "timestamptz", false),
                RivetType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: Some("UTC".into()),
                },
            ),
            (
                SourceColumn::simple("payload", "jsonb", true),
                RivetType::Json,
            ),
        ];

        let mappings: Vec<TypeMapping> = cols
            .into_iter()
            .map(|(s, t)| TypeMapping::from_source(&s, t))
            .collect();

        // Fidelity matrix mirrors the table in the Definition of Done.
        assert_eq!(mappings[0].fidelity, TypeFidelity::Exact);
        assert_eq!(mappings[1].fidelity, TypeFidelity::Exact);
        assert_eq!(mappings[2].fidelity, TypeFidelity::Exact);
        assert_eq!(mappings[3].fidelity, TypeFidelity::LogicalString);

        // Arrow types are exactly what the roadmap demands — no Utf8 fallback for decimal.
        assert_eq!(mappings[0].arrow_type, Some(DataType::Int64));
        assert_eq!(mappings[1].arrow_type, Some(DataType::Decimal128(18, 2)));
        assert!(matches!(
            mappings[2].arrow_type,
            Some(DataType::Timestamp(_, Some(_)))
        ));
        assert_eq!(mappings[3].arrow_type, Some(DataType::Utf8));

        // Field-level metadata is preserved end-to-end.
        let amount_field = build_arrow_field(&mappings[1]).expect("amount");
        assert_eq!(
            amount_field
                .metadata()
                .get(META_NATIVE_TYPE)
                .map(String::as_str),
            Some("numeric")
        );
        assert_eq!(
            amount_field
                .metadata()
                .get(META_FIDELITY)
                .map(String::as_str),
            Some("exact")
        );

        let payload_field = build_arrow_field(&mappings[3]).expect("payload");
        assert_eq!(
            payload_field
                .metadata()
                .get(META_LOGICAL_TYPE)
                .map(String::as_str),
            Some("json")
        );
    }

    /// Keep `rivet_type_to_arrow` / `derive_fidelity` re-exports live for
    /// `tests/type_roundtrip` contract tests and downstream tooling.
    #[test]
    fn mapping_helpers_reexported_for_contract_tests() {
        let dec = RivetType::Decimal {
            precision: 18,
            scale: 2,
        };
        assert!(matches!(
            rivet_type_to_arrow(&dec),
            Some(DataType::Decimal128(18, 2))
        ));
        assert_eq!(derive_fidelity(&dec), TypeFidelity::Exact);
    }

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32, sec: u32, ns: u32) -> chrono::NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(y, mo, d)
            .unwrap()
            .and_hms_nano_opt(h, mi, sec, ns)
            .unwrap()
    }

    #[test]
    fn nanoseconds_hold_both_representable_ends_and_refuse_one_past_each() {
        let (min, max) = (
            at(1677, 9, 21, 0, 12, 43, 145_224_192),
            at(2262, 4, 11, 23, 47, 16, 854_775_807),
        );
        assert_eq!(
            timestamp_nanos_or_refuse(min, "mssql", "c").unwrap(),
            i64::MIN
        );
        assert_eq!(
            timestamp_nanos_or_refuse(max, "mssql", "c").unwrap(),
            i64::MAX
        );
        assert_eq!(
            timestamp_nanos_or_refuse(at(1970, 1, 1, 0, 0, 1, 123_456_700), "mssql", "c").unwrap(),
            1_123_456_700
        );
        for past in [
            min - chrono::TimeDelta::nanoseconds(1),
            max + chrono::TimeDelta::nanoseconds(1),
            at(1600, 1, 1, 0, 0, 0, 0),
            at(2300, 1, 1, 0, 0, 0, 0),
            at(9999, 12, 31, 23, 59, 59, 999_999_900),
            at(1, 1, 1, 0, 0, 0, 0),
        ] {
            let err = timestamp_nanos_or_refuse(past, "mssql", "event_ts").unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5);
            let text = format!("{err:#}");
            assert!(
                text.contains("column `event_ts` holds ")
                    && text.contains("Declare the column `timestamp`"),
                "{text}"
            );
        }
    }
}
