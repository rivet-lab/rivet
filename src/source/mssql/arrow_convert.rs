//! SQL Server `Row` → Arrow `RecordBatch` pipeline.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, Timelike};
use tiberius::{Column, ColumnData, ColumnType, Row};

/// Whether a cell is SQL NULL, whatever its wire type.
pub(super) fn is_null_cell(c: &ColumnData<'_>) -> bool {
    matches!(
        c,
        ColumnData::U8(None)
            | ColumnData::I16(None)
            | ColumnData::I32(None)
            | ColumnData::I64(None)
            | ColumnData::F32(None)
            | ColumnData::F64(None)
            | ColumnData::Bit(None)
            | ColumnData::String(None)
            | ColumnData::Guid(None)
            | ColumnData::Binary(None)
            | ColumnData::Numeric(None)
            | ColumnData::Xml(None)
            | ColumnData::DateTime(None)
            | ColumnData::SmallDateTime(None)
            | ColumnData::Time(None)
            | ColumnData::Date(None)
            | ColumnData::DateTime2(None)
            | ColumnData::DateTimeOffset(None)
    )
}

/// A column whose values have no lossless Arrow type and ride as text: `sql_variant` and CLR UDTs.
pub(crate) fn renders_as_text(ct: ColumnType) -> bool {
    matches!(ct, ColumnType::SSVariant | ColumnType::Udt)
}

/// A cell rendered as text (ISO temporals, exact decimals, lowercase hex bytes); `None` for SQL NULL.
pub(crate) fn cell_text(c: &ColumnData<'_>) -> Option<String> {
    const DAYS_TO_1900: i64 = 693_595;
    const NANOS_PER_DAY: i128 = 86_400_000_000_000;
    let frac = |nanos: u64, digits: u8| {
        if digits == 0 {
            return String::new();
        }
        format!(".{}", &format!("{nanos:09}")[..usize::from(digits.min(9))])
    };
    let date = |days_from_ce: i64| {
        NaiveDate::from_num_days_from_ce_opt(i32::try_from(days_from_ce + 1).ok()?)
            .map(|d| d.format("%Y-%m-%d").to_string())
    };
    let clock = |nanos_of_day: u64, digits: u8| {
        let s = nanos_of_day / 1_000_000_000;
        format!(
            "{:02}:{:02}:{:02}{}",
            s / 3600,
            s / 60 % 60,
            s % 60,
            frac(nanos_of_day % 1_000_000_000, digits)
        )
    };
    let nanos = |t: tiberius::time::Time| t.increments() * 10u64.pow(9 - u32::from(t.scale()));
    Some(match c {
        ColumnData::String(Some(s)) => s.to_string(),
        ColumnData::Bit(Some(b)) => u8::from(*b).to_string(),
        ColumnData::U8(Some(v)) => v.to_string(),
        ColumnData::I16(Some(v)) => v.to_string(),
        ColumnData::I32(Some(v)) => v.to_string(),
        ColumnData::I64(Some(v)) => v.to_string(),
        ColumnData::F32(Some(v)) => v.to_string(),
        ColumnData::F64(Some(v)) => v.to_string(),
        ColumnData::Numeric(Some(n)) => numeric_to_decimal_string(n.value(), n.scale()),
        ColumnData::Guid(Some(g)) => g.to_string().to_uppercase(),
        ColumnData::Binary(Some(b)) => b.iter().map(|x| format!("{x:02x}")).collect(),
        ColumnData::Xml(Some(x)) => x.to_string(),
        ColumnData::Date(Some(d)) => date(i64::from(d.days()))?,
        ColumnData::Time(Some(t)) => clock(nanos(*t), t.scale()),
        ColumnData::DateTime2(Some(dt)) => format!(
            "{} {}",
            date(i64::from(dt.date().days()))?,
            clock(nanos(dt.time()), dt.time().scale())
        ),
        ColumnData::DateTimeOffset(Some(dto)) => {
            let dt = dto.datetime2();
            let off = i64::from(dto.offset());
            let local = i128::from(dt.date().days()) * NANOS_PER_DAY
                + i128::from(nanos(dt.time()))
                + i128::from(off) * 60_000_000_000;
            format!(
                "{} {} {}{:02}:{:02}",
                date(i64::try_from(local.div_euclid(NANOS_PER_DAY)).ok()?)?,
                clock(local.rem_euclid(NANOS_PER_DAY) as u64, dt.time().scale()),
                if off < 0 { '-' } else { '+' },
                off.abs() / 60,
                off.abs() % 60
            )
        }
        ColumnData::DateTime(Some(dt)) => format!(
            "{} {}",
            date(DAYS_TO_1900 + i64::from(dt.days()))?,
            clock(
                (u64::from(dt.seconds_fragments()) * 10 + 1) / 3 * 1_000_000,
                3
            )
        ),
        ColumnData::SmallDateTime(Some(dt)) => format!(
            "{} {}",
            date(DAYS_TO_1900 + i64::from(dt.days()))?,
            clock(u64::from(dt.seconds_fragments()) * 60_000_000_000, 0)
        ),
        _ => return None,
    })
}

/// Render a tiberius `Numeric` (unscaled `value` + `scale`) to exact decimal text.
pub(crate) fn numeric_to_decimal_string(value: i128, scale: u8) -> String {
    let scale = scale as usize;
    if scale == 0 {
        return value.to_string();
    }
    let neg = value < 0;
    let digits = value.unsigned_abs().to_string();
    let digits = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let (int_part, frac) = digits.split_at(digits.len() - scale);
    format!("{}{}.{}", if neg { "-" } else { "" }, int_part, frac)
}

/// The wire type of a cell, for an error message.
fn cell_type_name(c: &ColumnData<'_>) -> String {
    format!("{c:?}")
        .split('(')
        .next()
        .unwrap_or("?")
        .to_string()
}

use crate::error::Result;
use crate::types::kind::CommonKind;
use crate::types::{
    ColumnOverrides, RivetType, SourceColumn, TimeUnit as RivetTimeUnit, TypeMapping,
    build_arrow_field, resolve_column,
};

/// Days from the Arrow/Unix epoch (1970-01-01) used to anchor `Date32`.
const UNIX_EPOCH_DAY: i32 = 0;

use crate::source::value_within_ceiling;

/// The common kind a SQL Server wire type is read as, or `None` for a type this engine alone carries.
fn mssql_common_kind(ct: ColumnType) -> Option<CommonKind> {
    Some(match ct {
        ColumnType::Bit | ColumnType::Bitn => CommonKind::Bool,
        ColumnType::Int1 | ColumnType::Int2 => CommonKind::I16,
        ColumnType::Int4 => CommonKind::I32,
        ColumnType::Int8 | ColumnType::Intn => CommonKind::I64,
        ColumnType::Float4 => CommonKind::F32,
        ColumnType::Float8 | ColumnType::Floatn => CommonKind::F64,
        ColumnType::Decimaln | ColumnType::Numericn => CommonKind::Dec,
        ColumnType::Daten => CommonKind::Date,
        ColumnType::Datetime
        | ColumnType::Datetime4
        | ColumnType::Datetimen
        | ColumnType::Datetime2 => CommonKind::Naive,
        ColumnType::DatetimeOffsetn => CommonKind::Utc,
        ColumnType::Timen => CommonKind::Time,
        ColumnType::Guid => CommonKind::Uuid,
        ColumnType::NVarchar
        | ColumnType::NChar
        | ColumnType::BigVarChar
        | ColumnType::BigChar
        | ColumnType::Text
        | ColumnType::NText => CommonKind::Text,
        ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => CommonKind::Binary,
        _ => return None,
    })
}

/// The type of a `decimal` / `numeric` column before the catalog supplies its precision and scale.
fn bare_decimal() -> RivetType {
    RivetType::Decimal {
        precision: 38,
        scale: 0,
    }
}

/// The type of a wire type this engine alone carries, or `Unsupported` for one it does not map.
fn mssql_engine_type(ct: ColumnType) -> RivetType {
    match ct {
        ColumnType::Money => RivetType::Decimal {
            precision: 19,
            scale: 4,
        },
        ColumnType::Money4 => RivetType::Decimal {
            precision: 10,
            scale: 4,
        },
        ColumnType::SSVariant | ColumnType::Udt => RivetType::String,
        other => RivetType::Unsupported {
            native_type: format!("{other:?}").to_lowercase(),
            reason: format!("SQL Server column type {other:?} has no Rivet mapping yet"),
        },
    }
}

/// Map a SQL Server column type to its `RivetType`.
pub(super) fn mssql_type_to_rivet(col: &Column, overrides: &ColumnOverrides) -> Result<RivetType> {
    resolve_column(
        crate::types::override_table::Engine::Mssql,
        overrides,
        col.name(),
        &format!("{:?}", col.column_type()).to_lowercase(),
        || match mssql_common_kind(col.column_type()) {
            Some(kind) => kind.rivet_type().unwrap_or_else(bare_decimal),
            None => mssql_engine_type(col.column_type()),
        },
    )
}

/// Build the Arrow schema for a result set's columns + the per-column `(name, ColumnType)` the row decoder dispatches on.
pub(super) fn mssql_columns_to_schema(
    columns: &[Column],
    overrides: &ColumnOverrides,
    rows: &[Row],
    hints: Option<&CatalogHints>,
) -> Result<(Schema, Vec<(String, ColumnType)>)> {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut decoders: Vec<(String, ColumnType)> = Vec::with_capacity(columns.len());
    let mut errors: Vec<String> = Vec::new();

    for (idx, col) in columns.iter().enumerate() {
        let rivet = resolve_catalog_type(
            mssql_type_to_rivet(col, overrides)?,
            col.name(),
            overrides,
            hints,
            Some((idx, rows)),
        );
        let native = format!("{:?}", col.column_type()).to_lowercase();
        let source = SourceColumn::simple(col.name(), native, true);
        let mapping = TypeMapping::from_source(&source, rivet);
        match build_arrow_field(&mapping) {
            Some(field) => {
                if let Some(dt) = mapping
                    .arrow_type
                    .as_ref()
                    .filter(|_| overrides.contains_key(col.name()))
                {
                    let built =
                        build_array(dt, idx, &[], col.name(), None, Some(&mapping.rivet_type));
                    if seeded!("override-without-builder-internal-stop", false, true) {
                        crate::source::refuse_unbuildable_override(
                            "mssql",
                            col.name(),
                            &mapping.rivet_type,
                            built,
                        )?;
                    }
                }
                fields.push(field);
                decoders.push((col.name().to_string(), col.column_type()));
            }
            None => {
                let reason = match &mapping.rivet_type {
                    RivetType::Unsupported { reason, .. } => reason.as_str(),
                    _ => "no Rivet mapping for this SQL Server type",
                };
                errors.push(format!(
                    "  • {} ({:?}): {reason}",
                    col.name(),
                    col.column_type()
                ));
            }
        }
    }

    if !errors.is_empty() {
        return Err(crate::source::unsafe_mapping_refusal(format!(
            "SQL Server export: {} column(s) have no safe type mapping:\n{}",
            errors.len(),
            errors.join("\n")
        )));
    }
    Ok((Schema::new(fields), decoders))
}

/// The warning on a `time` column whose scale could not be read: delivered at microseconds.
const TIME_SCALE_UNREAD_WARNING: &str = "time → Time64(microsecond): the column's declared scale could not be read from the catalog (a query that is not a single-table SELECT), so a scale-7 column (the bare `time` default) is read from its first value's scale; time(0..6) is exact.";

/// A `time` column still at microseconds with no override keeps the scale-unread warning.
fn warn_time_scale_unread(mapping: TypeMapping, overridden: bool) -> TypeMapping {
    if overridden
        || matches!(
            mapping.rivet_type,
            RivetType::Time {
                unit: RivetTimeUnit::Nanosecond
            }
        )
    {
        return mapping;
    }
    mapping.with_warning(TIME_SCALE_UNREAD_WARNING)
}

/// `TypeMapping` for every column — drives `rivet check --type-report`.
pub(super) fn mssql_type_mappings(
    columns: &[Column],
    overrides: &ColumnOverrides,
    hints: Option<&CatalogHints>,
) -> Result<Vec<TypeMapping>> {
    columns
        .iter()
        .map(|col| {
            let rivet = resolve_catalog_type(
                mssql_type_to_rivet(col, overrides)?,
                col.name(),
                overrides,
                hints,
                None,
            );
            let native = format!("{:?}", col.column_type()).to_lowercase();
            let source = SourceColumn::simple(col.name(), native.clone(), true);
            let mapping = TypeMapping::from_source(&source, rivet);
            if overrides.contains_key(col.name())
                && hints.is_some_and(|h| h.fine_timestamps.contains(col.name()))
            {
                return Ok(mapping.narrowed_fraction(7));
            }
            let mapping = if native == "timen" {
                warn_time_scale_unread(mapping, overrides.contains_key(col.name()))
            } else {
                mapping
            };
            Ok(
                if matches!(
                    mapping.rivet_type,
                    RivetType::Timestamp {
                        unit: RivetTimeUnit::Nanosecond,
                        ..
                    }
                ) && !overrides.contains_key(col.name())
                {
                    mapping.with_warning(FINE_TIMESTAMP_WARNING)
                } else if native == "datetime2" && !overrides.contains_key(col.name()) {
                    mapping.with_warning(
                    "datetime2 → Timestamp(microsecond): the column's declared scale could not be \
                     read from the catalog (a query that is not a single-table SELECT), so a \
                     scale-7 column would lose its 100ns tick. Declare it `timestamp_ns` in \
                     `columns:` to keep it; datetime2(0..6) is exact.",
                )
                } else {
                    mapping
                },
            )
        })
        .collect()
}

/// The declared facts a result set's columns need from the catalog: decimal precision and scale, and the `datetime2` / `datetimeoffset` columns finer than a microsecond.
#[derive(Default)]
pub(super) struct CatalogHints {
    pub decimals: HashMap<String, (u8, i8)>,
    pub fine_timestamps: HashSet<String>,
    /// The `time` columns declared finer than a microsecond (the bare `time` is scale 7).
    pub fine_times: HashSet<String>,
}

/// The warning on a sub-microsecond timestamp column delivered as `Timestamp(ns)`.
const FINE_TIMESTAMP_WARNING: &str = "datetime2 / datetimeoffset with a scale above 6 → Timestamp(nanosecond): the 100ns tick is kept; the column holds 1677-09-21..2262-04-11, and a value outside it stops the export. Declare the column `timestamp` (or `timestamp_tz`) in `columns:` for microseconds over the full 0001..9999 range.";

/// A decimal's declared precision and scale, and a `datetime2` / `datetimeoffset` finer than a microsecond as nanoseconds (the first value's own scale decides over the catalog, so a query that re-types the column is read as written); a `columns:` override is never touched.
fn resolve_catalog_type(
    rivet: RivetType,
    col_name: &str,
    overrides: &ColumnOverrides,
    hints: Option<&CatalogHints>,
    rows: Option<(usize, &[Row])>,
) -> RivetType {
    let rivet = resolve_decimal(
        rivet,
        col_name,
        overrides,
        hints.map(|h| &h.decimals),
        rows.and_then(|(idx, rows)| decimal_scale_from_rows(idx, rows)),
    );
    let finer = seeded!(
        "fine-timestamp-lands-at-microseconds",
        false,
        rows.and_then(|(idx, rows)| first_timestamp_scale(idx, rows))
            .map_or_else(
                || hints.is_some_and(|h| h.fine_timestamps.contains(col_name)),
                |scale| scale > 6
            )
    );
    let finer_time = seeded!(
        "mssql-time7-lands-at-microseconds",
        false,
        rows.and_then(|(idx, rows)| first_time_scale(idx, rows))
            .map_or_else(
                || hints.is_some_and(|h| h.fine_times.contains(col_name)),
                |scale| scale > 6
            )
    );
    match rivet {
        RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone,
        } if finer && !overrides.contains_key(col_name) => RivetType::Timestamp {
            unit: RivetTimeUnit::Nanosecond,
            timezone,
        },
        RivetType::Time {
            unit: RivetTimeUnit::Microsecond,
        } if finer_time && !overrides.contains_key(col_name) => RivetType::Time {
            unit: RivetTimeUnit::Nanosecond,
        },
        other => other,
    }
}

/// The scale of the first non-null `time` value of column `idx` (every value of a column shares it).
fn first_time_scale(idx: usize, rows: &[Row]) -> Option<u8> {
    rows.iter().find_map(|row| {
        if let ColumnData::Time(Some(t)) = cell(row, idx)? {
            return Some(t.scale());
        }
        None
    })
}

/// The scale of the first non-null `datetime2` / `datetimeoffset` value of column `idx` (every value of a column shares it).
fn first_timestamp_scale(idx: usize, rows: &[Row]) -> Option<u8> {
    rows.iter().find_map(|row| {
        let value = cell(row, idx)?;
        if let ColumnData::DateTime2(Some(dt)) = value {
            return Some(dt.time().scale());
        }
        if let ColumnData::DateTimeOffset(Some(dto)) = value {
            return Some(dto.datetime2().time().scale());
        }
        None
    })
}

/// The UTC instant of a `datetime2` / `datetimeoffset` cell, or a coded mismatch when the cell is neither.
fn utc_instant(
    row: &Row,
    idx: usize,
    declared: Option<&RivetType>,
    column: &str,
    target: &DataType,
) -> Result<Option<NaiveDateTime>> {
    match row.try_get::<NaiveDateTime, _>(idx) {
        Ok(v) => Ok(v),
        Err(_) => match row.try_get::<chrono::DateTime<chrono::FixedOffset>, _>(idx) {
            Ok(v) => Ok(v.map(|dt| dt.naive_utc())),
            Err(e) => Err(cell_mismatch(
                declared,
                column,
                target,
                cell(row, idx),
                &e.to_string(),
            )),
        },
    }
}

/// Recover an autodetected decimal's declared precision/scale.
fn resolve_decimal(
    rivet: RivetType,
    col_name: &str,
    overrides: &ColumnOverrides,
    decimal_hints: Option<&HashMap<String, (u8, i8)>>,
    data_scale: Option<u8>,
) -> RivetType {
    if overrides.contains_key(col_name) {
        return rivet;
    }
    let precision = match &rivet {
        RivetType::Decimal { precision, .. } => *precision,
        _ => return rivet,
    };
    if let Some(&(p, s)) = decimal_hints.and_then(|h| h.get(col_name)) {
        RivetType::Decimal {
            precision: p,
            scale: s,
        }
    } else if let Some(s) = data_scale {
        RivetType::Decimal {
            precision: precision.max(s),
            scale: s as i8,
        }
    } else {
        rivet
    }
}

/// Fetch the `ColumnData` of column `idx` from a row without consuming it.
fn naive_micros(dt: NaiveDateTime, legacy_datetime: bool) -> i64 {
    let dt = if legacy_datetime {
        nearest_micro(dt)
    } else {
        dt
    };
    dt.and_utc().timestamp_micros()
}

/// A legacy DATETIME value moved to its nearest microsecond — the one reading batch and CDC both give it.
pub(crate) fn nearest_micro(dt: NaiveDateTime) -> NaiveDateTime {
    let sub = i64::from(dt.nanosecond() % 1_000);
    let shift = if sub >= 500 { 1_000 - sub } else { -sub };
    dt + chrono::TimeDelta::nanoseconds(shift)
}

fn cell(row: &Row, idx: usize) -> Option<&ColumnData<'static>> {
    row.cells().nth(idx).map(|(_, d)| d)
}

/// The decimal scale of column `idx`, read from the first non-null `Numeric` value (all values in a column share it).
fn decimal_scale_from_rows(idx: usize, rows: &[Row]) -> Option<u8> {
    rows.iter().find_map(|row| match cell(row, idx) {
        Some(ColumnData::Numeric(Some(n))) => Some(n.scale()),
        _ => None,
    })
}

/// A `Float64` column: an f64 cell as is, a MONEY cell as its double when the double prints back the same digits, any other cell refused.
fn float64_array(
    idx: usize,
    rows: &[Row],
    column: &str,
    declared: Option<&RivetType>,
) -> Result<ArrayRef> {
    let money = rows
        .first()
        .is_some_and(|r| is_money(r.columns()[idx].column_type()));
    let mut b = Float64Builder::with_capacity(rows.len());
    for row in rows {
        match cell(row, idx) {
            Some(ColumnData::F64(Some(v))) => b.append_value(*v),
            Some(ColumnData::Numeric(Some(n))) if money => {
                let text = numeric_to_decimal_string(n.value(), n.scale());
                b.append_value(
                    crate::types::decimal::exact_f64_of_text(column, &text)?
                        .unwrap_or_else(|| numeric_to_f64(n)),
                )
            }
            Some(
                ColumnData::U8(None)
                | ColumnData::I16(None)
                | ColumnData::I32(None)
                | ColumnData::I64(None)
                | ColumnData::F32(None)
                | ColumnData::F64(None)
                | ColumnData::Bit(None)
                | ColumnData::String(None)
                | ColumnData::Binary(None)
                | ColumnData::Guid(None)
                | ColumnData::Numeric(None),
            )
            | None => b.append_null(),
            Some(other) => {
                return Err(cell_mismatch(
                    declared,
                    column,
                    &DataType::Float64,
                    Some(other),
                    "",
                ));
            }
        }
    }
    Ok(Arc::new(b.finish()))
}

/// Build one Arrow array for column `idx`, dispatching on the Arrow target type (so the array always matches the schema field by construction).
fn build_array(
    target: &DataType,
    idx: usize,
    rows: &[Row],
    column: &str,
    max_value_bytes: Option<usize>,
    declared: Option<&RivetType>,
) -> Result<ArrayRef> {
    macro_rules! simple {
        ($builder:ty, $pat:pat => $val:expr) => {{
            let mut b = <$builder>::with_capacity(rows.len());
            for row in rows {
                match cell(row, idx) {
                    $pat => b.append_value($val),
                    Some(ColumnData::U8(None))
                    | Some(ColumnData::I16(None))
                    | Some(ColumnData::I32(None))
                    | Some(ColumnData::I64(None))
                    | Some(ColumnData::F32(None))
                    | Some(ColumnData::F64(None))
                    | Some(ColumnData::Bit(None))
                    | Some(ColumnData::String(None))
                    | Some(ColumnData::Binary(None))
                    | Some(ColumnData::Guid(None))
                    | Some(ColumnData::Numeric(None))
                    | None => b.append_null(),
                    Some(other) => {
                        return Err(cell_mismatch(declared, column, target, Some(other), ""));
                    }
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }

    let arr: ArrayRef = match target {
        DataType::Boolean => simple!(BooleanBuilder, Some(ColumnData::Bit(Some(v))) => *v),
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                match cell(row, idx) {
                    Some(ColumnData::I16(Some(v))) => b.append_value(*v),
                    Some(ColumnData::U8(Some(v))) => b.append_value(*v as i16),
                    Some(ColumnData::U8(None))
                    | Some(ColumnData::I16(None))
                    | Some(ColumnData::I32(None))
                    | Some(ColumnData::I64(None))
                    | Some(ColumnData::F32(None))
                    | Some(ColumnData::F64(None))
                    | Some(ColumnData::Bit(None))
                    | Some(ColumnData::String(None))
                    | Some(ColumnData::Binary(None))
                    | Some(ColumnData::Guid(None))
                    | Some(ColumnData::Numeric(None))
                    | None => b.append_null(),
                    Some(other) => {
                        return Err(cell_mismatch(declared, column, target, Some(other), ""));
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int32 => simple!(Int32Builder, Some(ColumnData::I32(Some(v))) => *v),
        DataType::Int64 => simple!(Int64Builder, Some(ColumnData::I64(Some(v))) => *v),
        DataType::Float32 => simple!(Float32Builder, Some(ColumnData::F32(Some(v))) => *v),
        DataType::Float64 => float64_array(idx, rows, column, declared)?,
        DataType::Utf8
            if rows
                .first()
                .is_some_and(|r| renders_as_text(r.columns()[idx].column_type())) =>
        {
            let mut b = StringBuilder::new();
            for row in rows {
                let c = cell(row, idx);
                match c.and_then(cell_text) {
                    Some(s) => {
                        value_within_ceiling(column, s.len(), max_value_bytes)?;
                        b.append_value(s);
                    }
                    None if c.is_none_or(is_null_cell) => b.append_null(),
                    None => crate::rivet_bail!(
                        crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                        "mssql: column `{column}` holds a {c:?} value with no text rendering"
                    ),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for row in rows {
                match cell(row, idx) {
                    Some(ColumnData::String(Some(s))) => {
                        value_within_ceiling(column, s.len(), max_value_bytes)?;
                        b.append_value(s.as_ref());
                    }
                    Some(other) if !is_null_cell(other) => crate::rivet_bail!(
                        crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
                        "column `{column}` is declared text (`columns: {column}: string`) but SQL \
                         Server sends it as {} — rivet does not convert it, and writing it as NULL \
                         would lose every value. Remove the `string` override: rivet reads this \
                         type natively. (A server-side CAST to nvarchar rounds float, money and \
                         datetime.)",
                        cell_type_name(other)
                    ),
                    Some(_) | None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::new();
            for row in rows {
                match binary_cell(cell(row, idx), column)? {
                    Some(bytes) => {
                        value_within_ceiling(column, bytes.len(), max_value_bytes)?;
                        b.append_value(bytes);
                    }
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::FixedSizeBinary(16) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
            for row in rows {
                match uuid_cell(cell(row, idx), column)? {
                    Some(bytes) => b.append_value(bytes)?,
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Decimal128(_p, s) => {
            let scale = (*s).max(0) as u8;
            let mut b = Decimal128Builder::with_capacity(rows.len()).with_data_type(target.clone());
            for (r, row) in rows.iter().enumerate() {
                match decimal_cell(cell(row, idx), scale, column)
                    .with_context(|| format!("mssql decimal column {idx} row {r}"))?
                {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            for row in rows {
                match row.try_get::<NaiveDate, _>(idx) {
                    Ok(Some(d)) => b.append_value(
                        (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
                            + UNIX_EPOCH_DAY,
                    ),
                    Ok(None) => b.append_null(),
                    Err(e) => {
                        return Err(cell_mismatch(
                            declared,
                            column,
                            target,
                            cell(row, idx),
                            &e.to_string(),
                        ));
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                let legacy = matches!(cell(row, idx), Some(ColumnData::DateTime(_)));
                let micros = match row.try_get::<NaiveDateTime, _>(idx) {
                    Ok(v) => v.map(|dt| naive_micros(dt, legacy)),
                    Err(_) => match row.try_get::<chrono::DateTime<chrono::FixedOffset>, _>(idx) {
                        Ok(v) => v.map(|dt| dt.timestamp_micros()),
                        Err(e) => {
                            return Err(cell_mismatch(
                                declared,
                                column,
                                target,
                                cell(row, idx),
                                &e.to_string(),
                            ));
                        }
                    },
                };
                match micros {
                    Some(m) => b.append_value(m),
                    None => b.append_null(),
                }
            }
            let arr = b.finish();
            return Ok(match tz {
                Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                None => Arc::new(arr),
            });
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let mut b = TimestampNanosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match utc_instant(row, idx, declared, column, target)? {
                    Some(dt) => match seeded!(
                        "mssql-ns-override-out-of-range-null",
                        dt.and_utc().timestamp_nanos_opt(),
                        Some(crate::types::timestamp_nanos_or_refuse(
                            dt, "mssql", column
                        )?)
                    ) {
                        Some(ns) => b.append_value(ns),
                        None => b.append_null(),
                    },
                    None => b.append_null(),
                }
            }
            let arr = b.finish();
            return Ok(match tz {
                Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                None => Arc::new(arr),
            });
        }
        DataType::Time64(unit) => build_time64(unit, target, idx, rows, column, declared)?,
        other => crate::rivet_bail!(
            crate::error::codes::INTERNAL_TYPE_BUILDER,
            "mssql: no array builder for Arrow type {other:?} (column {idx})"
        ),
    };
    Ok(arr)
}

/// A `time` column as `Time64` at `unit` (nanoseconds keep the 100 ns tick, microseconds drop below it).
fn build_time64(
    unit: &TimeUnit,
    target: &DataType,
    idx: usize,
    rows: &[Row],
    column: &str,
    declared: Option<&RivetType>,
) -> Result<ArrayRef> {
    let per_second: i64 = if matches!(unit, TimeUnit::Nanosecond) {
        1_000_000_000
    } else {
        1_000_000
    };
    let divisor = 1_000_000_000 / per_second;
    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        values.push(match row.try_get::<chrono::NaiveTime, _>(idx) {
            Ok(t) => t.map(|t| {
                t.num_seconds_from_midnight() as i64 * per_second + t.nanosecond() as i64 / divisor
            }),
            Err(e) => {
                return Err(cell_mismatch(
                    declared,
                    column,
                    target,
                    cell(row, idx),
                    &e.to_string(),
                ));
            }
        });
    }
    Ok(match unit {
        TimeUnit::Nanosecond => Arc::new(arrow::array::Time64NanosecondArray::from(values)),
        _ => Arc::new(arrow::array::Time64MicrosecondArray::from(values)),
    })
}

/// The stop for a cell the `target` builder cannot read.
fn cell_mismatch(
    declared: Option<&RivetType>,
    column: &str,
    target: &DataType,
    cell: Option<&ColumnData<'_>>,
    detail: &str,
) -> anyhow::Error {
    match declared {
        Some(d) if seeded!("override-without-builder-internal-stop", false, true) => {
            let sent = cell.map_or_else(|| "no value".to_string(), cell_type_name);
            declared_mismatch(column, &d.label(), &sent)
        }
        _ => crate::error::coded(
            crate::error::codes::INTERNAL_TYPE_BUILDER,
            format!("mssql column `{column}`: no {target} from {cell:?} {detail}"),
        ),
    }
}

/// A valued cell whose wire type a `columns:` override declared as something rivet does not convert.
fn override_mismatch(column: &str, declared: &str, other: &ColumnData<'_>) -> anyhow::Error {
    declared_mismatch(column, declared, &cell_type_name(other))
}

/// The coded refusal for column `column` declared `declared` that SQL Server sends as `sent`.
fn declared_mismatch(column: &str, declared: &str, sent: &str) -> anyhow::Error {
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
        format!(
            "column `{column}` is declared {declared} by a `columns:` override but SQL Server \
             sends it as {sent} — rivet does not convert it, and writing it as NULL would lose \
             every value. Remove the override, or CAST the column to that type in the \
             export's `query:`."
        ),
    ))
}

/// Whether a column is MONEY or SMALLMONEY.
fn is_money(t: ColumnType) -> bool {
    matches!(t, ColumnType::Money | ColumnType::Money4)
}

/// A money value as the nearest f64 of its exact decimal text.
fn numeric_to_f64(n: &tiberius::numeric::Numeric) -> f64 {
    numeric_to_decimal_string(n.value(), n.scale())
        .parse()
        .unwrap_or(f64::NAN)
}

/// A `Decimal128(_, scale)` cell: numeric or money rescaled, an integer scaled; any other value refused.
fn decimal_cell(c: Option<&ColumnData<'_>>, scale: u8, column: &str) -> Result<Option<i128>> {
    let int = |v: i128| rescale_i128(v, 0, scale).map(Some);
    match c {
        Some(ColumnData::Numeric(Some(n))) => {
            let unscaled = seeded!(
                "mssql-money-decoded-as-f64",
                crate::seeded::money_through_f64(n.value(), n.scale())?,
                n.value()
            );
            rescale_i128(unscaled, n.scale(), scale).map(Some)
        }
        Some(ColumnData::U8(Some(v))) => int(i128::from(*v)),
        Some(ColumnData::I16(Some(v))) => int(i128::from(*v)),
        Some(ColumnData::I32(Some(v))) => int(i128::from(*v)),
        Some(ColumnData::I64(Some(v))) => int(i128::from(*v)),
        Some(other) if !is_null_cell(other) => Err(override_mismatch(column, "decimal", other)),
        _ => Ok(None),
    }
}

/// A `uuid` cell: a GUID, or uuid text from a char/nvarchar column; any other value refused.
fn uuid_cell(c: Option<&ColumnData<'_>>, column: &str) -> Result<Option<[u8; 16]>> {
    match c {
        Some(ColumnData::Guid(Some(g))) => Ok(Some(*g.as_bytes())),
        Some(ColumnData::String(Some(s))) => match uuid::Uuid::parse_str(s.trim()) {
            Ok(u) => Ok(Some(*u.as_bytes())),
            Err(e) => Err(anyhow::Error::new(crate::error::CodedError::new(
                crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
                format!(
                    "column `{column}` is declared uuid by a `columns:` override but holds \
                     {s:?}, which is not a uuid ({e}) — rivet refuses rather than writing NULL. \
                     Fix the value, or remove the override."
                ),
            ))),
        },
        Some(other) if !is_null_cell(other) => Err(override_mismatch(column, "uuid", other)),
        _ => Ok(None),
    }
}

/// A `bytes` cell: binary as-is; any other value refused.
fn binary_cell<'a>(c: Option<&'a ColumnData<'_>>, column: &str) -> Result<Option<&'a [u8]>> {
    match c {
        Some(ColumnData::Binary(Some(bytes))) => Ok(Some(bytes.as_ref())),
        Some(other) if !is_null_cell(other) => Err(override_mismatch(column, "bytes", other)),
        _ => Ok(None),
    }
}

/// Rescale an unscaled decimal from `from_scale` to `to_scale` (Arrow's fixed column scale).
fn rescale_i128(value: i128, from_scale: u8, to_scale: u8) -> Result<i128> {
    use std::cmp::Ordering;
    match to_scale.cmp(&from_scale) {
        Ordering::Equal => Ok(value),
        Ordering::Greater => {
            let factor = 10i128.pow((to_scale - from_scale) as u32);
            value.checked_mul(factor).ok_or_else(|| {
                crate::error::coded(
                    crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                    format!(
                        "mssql decimal overflow: unscaled value {value} at scale {from_scale} \
                     does not fit i128 when rescaled to {to_scale}"
                    ),
                )
            })
        }
        Ordering::Less => {
            let factor = 10i128.pow((from_scale - to_scale) as u32);
            if value % factor != 0 {
                crate::rivet_bail!(
                    crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
                    "mssql decimal rescale is lossy: unscaled value {value} at scale \
                     {from_scale} does not fit column scale {to_scale} (non-zero digits \
                     would be dropped). The column's declared scale was likely lost — \
                     tiberius drops decimal precision/scale, and a first batch whose \
                     decimal values are all NULL falls back to scale 0. Declare the \
                     scale with a column override, e.g. \
                     columns: <name>: decimal(38,{from_scale})"
                );
            }
            Ok(value / factor)
        }
    }
}

/// Build a full `RecordBatch` from collected rows.
pub(super) fn mssql_rows_to_record_batch(
    schema: &SchemaRef,
    rows: &[Row],
    max_value_bytes: Option<usize>,
    overrides: &ColumnOverrides,
) -> Result<arrow::record_batch::RecordBatch> {
    if let Some(first) = rows.first() {
        let expected: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let wire: Vec<&str> = first.columns().iter().map(|c| c.name()).collect();
        crate::source::verify_wire_columns(&expected, &wire)?;
    }
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        arrays.push(
            build_array(
                field.data_type(),
                idx,
                rows,
                field.name(),
                max_value_bytes,
                overrides.get(field.name()),
            )
            .with_context(|| format!("mssql column '{}'", field.name()))?,
        );
    }
    let batch = arrow::record_batch::RecordBatch::try_new(schema.clone(), arrays)?;
    let a = crate::source::value_checksum::source_checksums(schema, &MssqlCellSource { rows });
    let b = crate::source::value_checksum::arrow_batch_checksums(&batch);
    crate::source::value_checksum::verify(&a, &b, schema)?;
    Ok(batch)
}

/// Side A of the Form A value-checksum for SQL Server.
struct MssqlCellSource<'a> {
    rows: &'a [Row],
}

impl crate::source::value_checksum::CellSource for MssqlCellSource<'_> {
    fn num_rows(&self) -> usize {
        self.rows.len()
    }
    fn boolean(&self, col: usize, row: usize) -> Option<bool> {
        match cell(&self.rows[row], col) {
            Some(ColumnData::Bit(Some(v))) => Some(*v),
            _ => None,
        }
    }
    fn int16(&self, col: usize, row: usize) -> Option<i16> {
        match cell(&self.rows[row], col) {
            Some(ColumnData::I16(Some(v))) => Some(*v),
            Some(ColumnData::U8(Some(v))) => Some(*v as i16),
            _ => None,
        }
    }
    fn int32(&self, col: usize, row: usize) -> Option<i32> {
        match cell(&self.rows[row], col) {
            Some(ColumnData::I32(Some(v))) => Some(*v),
            _ => None,
        }
    }
    fn int64(&self, col: usize, row: usize) -> Option<i64> {
        match cell(&self.rows[row], col) {
            Some(ColumnData::I64(Some(v))) => Some(*v),
            _ => None,
        }
    }
    fn uint64(&self, _col: usize, _row: usize) -> Option<u64> {
        None
    }
    fn float32(&self, col: usize, row: usize) -> Option<f32> {
        match cell(&self.rows[row], col) {
            Some(ColumnData::F32(Some(v))) => Some(*v),
            _ => None,
        }
    }
    fn float64(&self, col: usize, row: usize) -> Option<f64> {
        match cell(&self.rows[row], col) {
            Some(ColumnData::F64(Some(v))) => Some(*v),
            Some(ColumnData::Numeric(Some(n))) => Some(numeric_to_f64(n)),
            _ => None,
        }
    }
    fn decimal128(&self, col: usize, row: usize, scale: i8) -> Option<i128> {
        use bigdecimal::{BigDecimal, RoundingMode, num_bigint::BigInt, num_traits::ToPrimitive};
        let target_scale = scale.max(0) as i64;
        let int = |v: i128| v.checked_mul(10i128.pow(target_scale as u32));
        match cell(&self.rows[row], col) {
            Some(ColumnData::Numeric(Some(n))) => {
                BigDecimal::new(BigInt::from(n.value()), n.scale() as i64)
                    .with_scale_round(target_scale, RoundingMode::Down)
                    .into_bigint_and_exponent()
                    .0
                    .to_i128()
            }
            Some(ColumnData::U8(Some(v))) => int(i128::from(*v)),
            Some(ColumnData::I16(Some(v))) => int(i128::from(*v)),
            Some(ColumnData::I32(Some(v))) => int(i128::from(*v)),
            Some(ColumnData::I64(Some(v))) => int(i128::from(*v)),
            _ => None,
        }
    }
    fn date32(&self, col: usize, row: usize) -> Option<i32> {
        let d = self.rows[row].try_get::<NaiveDate, _>(col).ok().flatten()?;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        Some((d - epoch).num_days() as i32 + UNIX_EPOCH_DAY)
    }
    fn ts_micros(&self, col: usize, row: usize) -> Option<i64> {
        let legacy = matches!(cell(&self.rows[row], col), Some(ColumnData::DateTime(_)));
        match self.rows[row].try_get::<NaiveDateTime, _>(col) {
            Ok(v) => v.map(|dt| naive_micros(dt, legacy)),
            Err(_) => match self.rows[row].try_get::<chrono::DateTime<chrono::FixedOffset>, _>(col)
            {
                Ok(v) => v.map(|dt| dt.timestamp_micros()),
                Err(_) => None,
            },
        }
    }
    fn binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        match cell(&self.rows[row], col) {
            Some(ColumnData::Binary(Some(v))) => Some(Cow::Borrowed(v.as_ref())),
            _ => None,
        }
    }
    fn utf8(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        let r = &self.rows[row];
        if renders_as_text(r.columns()[col].column_type()) {
            return cell(r, col)
                .and_then(cell_text)
                .map(|s| Cow::Owned(s.into_bytes()));
        }
        match cell(r, col) {
            Some(ColumnData::String(Some(v))) => Some(Cow::Borrowed(v.as_bytes())),
            _ => None,
        }
    }
    fn time64_micros(&self, col: usize, row: usize) -> Option<i64> {
        self.rows[row]
            .try_get::<chrono::NaiveTime, _>(col)
            .ok()
            .flatten()
            .map(|t| {
                t.num_seconds_from_midnight() as i64 * 1_000_000 + t.nanosecond() as i64 / 1000
            })
    }
    fn fixed_binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        match cell(&self.rows[row], col) {
            Some(ColumnData::Guid(Some(g))) => Some(Cow::Owned(g.as_bytes().to_vec())),
            Some(ColumnData::String(Some(s))) => uuid::Uuid::parse_str(s.trim())
                .ok()
                .map(|u| Cow::Owned(u.as_bytes().to_vec())),
            _ => None,
        }
    }
    fn decimal256(&self, _col: usize, _row: usize, _scale: i8) -> Option<arrow::datatypes::i256> {
        None
    }
    fn list(
        &self,
        _col: usize,
        _row: usize,
        _elem: &DataType,
    ) -> Option<Vec<crate::source::value_checksum::ListElem>> {
        None
    }
}

#[cfg(test)]
mod type_table;

#[cfg(test)]
mod type_map_tests {
    use super::{bare_decimal, mssql_common_kind, mssql_engine_type, mssql_type_to_rivet};
    use crate::types::kind::CommonKind;
    use crate::types::{ColumnOverrides, RivetType, TimeUnit as RivetTimeUnit};
    use tiberius::{Column, ColumnType};

    fn ts(timezone: Option<&str>) -> RivetType {
        RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: timezone.map(str::to_string),
        }
    }

    fn unsupported(native: &str, ct: ColumnType) -> RivetType {
        RivetType::Unsupported {
            native_type: native.into(),
            reason: format!("SQL Server column type {ct:?} has no Rivet mapping yet"),
        }
    }

    /// Every wire type: the common kind it reads as, and the type the mapper gives it with no override or hint.
    fn wire_table() -> Vec<(ColumnType, Option<CommonKind>, RivetType)> {
        use CommonKind::*;
        let dec = |precision, scale| RivetType::Decimal { precision, scale };
        vec![
            (
                ColumnType::Null,
                None,
                unsupported("null", ColumnType::Null),
            ),
            (ColumnType::Bit, Some(Bool), RivetType::Bool),
            (ColumnType::Bitn, Some(Bool), RivetType::Bool),
            (ColumnType::Int1, Some(I16), RivetType::Int16),
            (ColumnType::Int2, Some(I16), RivetType::Int16),
            (ColumnType::Int4, Some(I32), RivetType::Int32),
            (ColumnType::Int8, Some(I64), RivetType::Int64),
            (ColumnType::Intn, Some(I64), RivetType::Int64),
            (ColumnType::Float4, Some(F32), RivetType::Float32),
            (ColumnType::Float8, Some(F64), RivetType::Float64),
            (ColumnType::Floatn, Some(F64), RivetType::Float64),
            (ColumnType::Decimaln, Some(Dec), dec(38, 0)),
            (ColumnType::Numericn, Some(Dec), dec(38, 0)),
            (ColumnType::Money, None, dec(19, 4)),
            (ColumnType::Money4, None, dec(10, 4)),
            (ColumnType::Daten, Some(Date), RivetType::Date),
            (ColumnType::Datetime, Some(Naive), ts(None)),
            (ColumnType::Datetime4, Some(Naive), ts(None)),
            (ColumnType::Datetimen, Some(Naive), ts(None)),
            (ColumnType::Datetime2, Some(Naive), ts(None)),
            (ColumnType::DatetimeOffsetn, Some(Utc), ts(Some("UTC"))),
            (
                ColumnType::Timen,
                Some(Time),
                RivetType::Time {
                    unit: RivetTimeUnit::Microsecond,
                },
            ),
            (ColumnType::NVarchar, Some(Text), RivetType::String),
            (ColumnType::NChar, Some(Text), RivetType::String),
            (ColumnType::BigVarChar, Some(Text), RivetType::String),
            (ColumnType::BigChar, Some(Text), RivetType::String),
            (ColumnType::Text, Some(Text), RivetType::String),
            (ColumnType::NText, Some(Text), RivetType::String),
            (ColumnType::BigVarBin, Some(Binary), RivetType::Binary),
            (ColumnType::BigBinary, Some(Binary), RivetType::Binary),
            (ColumnType::Image, Some(Binary), RivetType::Binary),
            (ColumnType::Guid, Some(Uuid), RivetType::Uuid),
            (ColumnType::SSVariant, None, RivetType::String),
            (ColumnType::Udt, None, RivetType::String),
            (ColumnType::Xml, None, unsupported("xml", ColumnType::Xml)),
        ]
    }

    /// Every wire type reads as its common kind, or as none for a type this engine alone carries.
    #[test]
    fn the_common_wire_types_read_as_their_common_kind_and_the_rest_as_none() {
        let table = wire_table();
        assert_eq!(table.len(), 35);
        for (ct, kind, _) in &table {
            assert_eq!(&mssql_common_kind(*ct), kind, "{ct:?}");
        }
    }

    /// Every wire type is typed as pinned, through its kind when it has one and the engine table when it has none.
    #[test]
    fn every_wire_type_is_typed_through_its_kind_or_the_engine_table() {
        for (ct, kind, want) in wire_table() {
            let column = Column::new("c".to_string(), ct);
            let got = mssql_type_to_rivet(&column, &ColumnOverrides::new()).expect("maps");
            assert_eq!(got, want, "{ct:?}");
            match kind.and_then(CommonKind::rivet_type) {
                Some(by_kind) => assert_eq!(got, by_kind, "{ct:?} is the kind's own type"),
                None if kind.is_some() => assert_eq!(got, bare_decimal(), "{ct:?}"),
                None => assert_eq!(got, mssql_engine_type(ct), "{ct:?}"),
            }
        }
    }

    /// Every wire type: one no common kind reads is delivered as a common type only where the engine declares it.
    #[test]
    fn no_wire_type_outside_the_common_kinds_is_delivered_as_a_common_type() {
        let rows = wire_table()
            .into_iter()
            .map(|(ct, kind, t)| (format!("{ct:?}").to_lowercase(), kind, t));
        crate::types::kind::assert_engine_only_rows_are_declared(
            crate::types::override_table::Engine::Mssql,
            rows,
        );
    }

    /// A decimal with no catalog hint is `decimal(38,0)`, and the one kind without a type is the one that takes it.
    #[test]
    fn a_decimal_without_a_hint_is_the_widest_scale_zero_decimal() {
        assert_eq!(
            bare_decimal(),
            RivetType::Decimal {
                precision: 38,
                scale: 0
            }
        );
        assert_eq!(CommonKind::Dec.rivet_type(), None);
    }

    /// An override names the type a column is read as, whatever its wire type reads as by default.
    #[test]
    fn an_override_wins_over_the_kind() {
        let column = Column::new("c".to_string(), ColumnType::Int4);
        let decimal = RivetType::Decimal {
            precision: 18,
            scale: 2,
        };
        let overrides = ColumnOverrides::from([("c".to_string(), decimal.clone())]);
        assert_eq!(
            mssql_type_to_rivet(&column, &overrides).expect("int to decimal is read"),
            decimal
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two text columns: an nvarchar, and a sql_variant holding an int then NULL.
    fn text_rows() -> Vec<Row> {
        let row = |s: &str, v: ColumnData<'static>| {
            Row::builder()
                .column(
                    "s",
                    ColumnType::NVarchar,
                    ColumnData::String(Some(s.to_string().into())),
                )
                .column("v", ColumnType::SSVariant, v)
                .build()
        };
        vec![
            row("h\u{e9}llo", ColumnData::I32(Some(42))),
            row("", ColumnData::I32(None)),
        ]
    }

    fn text_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("v", DataType::Utf8, true),
        ]))
    }

    /// nvarchar and sql_variant cells build as text, the variant's NULL stays NULL.
    #[test]
    fn text_and_variant_columns_build_and_pass_the_value_checksum() {
        use arrow::array::{Array, AsArray};
        let batch =
            mssql_rows_to_record_batch(&text_schema(), &text_rows(), None, &ColumnOverrides::new())
                .unwrap();
        let s = batch.column(0).as_string::<i32>();
        assert_eq!((s.value(0), s.value(1)), ("h\u{e9}llo", ""));
        let v = batch.column(1).as_string::<i32>();
        assert_eq!(v.value(0), "42");
        assert!(v.is_null(1));
    }

    /// Side A of a text column MISMATCHES an Arrow column whose cell was corrupted.
    #[test]
    fn a_corrupted_text_cell_fails_the_value_checksum() {
        use arrow::array::StringArray;

        use crate::source::value_checksum::{arrow_batch_checksums, source_checksums, verify};
        let schema = text_schema();
        let rows = text_rows();
        let a = source_checksums(&schema, &MssqlCellSource { rows: &rows });
        let batch = |s: [&str; 2], v: [Option<&str>; 2]| {
            arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(s.to_vec())),
                    Arc::new(StringArray::from(v.to_vec())),
                ],
            )
            .unwrap()
        };
        let good = batch(["h\u{e9}llo", ""], [Some("42"), None]);
        verify(&a, &arrow_batch_checksums(&good), &schema).expect("a faithful build matches");
        for (bad, col) in [
            (batch(["hello", ""], [Some("42"), None]), "s"),
            (batch(["h\u{e9}llo", ""], [Some("43"), None]), "v"),
        ] {
            let err = verify(&a, &arrow_batch_checksums(&bad), &schema).unwrap_err();
            assert!(
                err.to_string().contains(&format!("column '{col}'")),
                "{err}"
            );
        }
    }

    /// A `string` override on an int column fails naming the column, never a stringified value.
    #[test]
    fn a_string_override_on_an_int_column_is_refused() {
        let rows = [Row::builder()
            .column("n", ColumnType::Int4, ColumnData::I32(Some(7)))
            .build()];
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Utf8, true)]));
        let err =
            mssql_rows_to_record_batch(&schema, &rows, None, &ColumnOverrides::new()).unwrap_err();
        assert!(
            format!("{err:#}").contains("column `n` is declared text"),
            "{err:#}"
        );
    }

    /// Only sql_variant and CLR UDT columns ride as text; a native text or int column does not.
    #[test]
    fn only_variant_and_udt_columns_render_as_text() {
        use tiberius::ColumnType as T;
        assert!(renders_as_text(T::SSVariant));
        assert!(renders_as_text(T::Udt));
        assert!(!renders_as_text(T::NVarchar));
        assert!(!renders_as_text(T::Int4));
    }

    /// Every base type a `sql_variant` or UDT can carry renders as SQL Server's own ISO text.
    #[test]
    fn a_variant_cell_renders_each_base_type_as_text() {
        use tiberius::numeric::Numeric;
        use tiberius::time::{Date, DateTime, DateTime2, DateTimeOffset, SmallDateTime, Time};
        let dt2 = |days, inc, scale| DateTime2::new(Date::new(days), Time::new(inc, scale));
        let cases: Vec<(ColumnData<'static>, &str)> = vec![
            (ColumnData::I32(Some(-7)), "-7"),
            (ColumnData::I16(Some(-300)), "-300"),
            (ColumnData::F32(Some(0.5)), "0.5"),
            (
                ColumnData::Xml(Some(std::borrow::Cow::Owned(tiberius::xml::XmlData::new(
                    "<a/>",
                )))),
                "<a/>",
            ),
            (
                ColumnData::DateTimeOffset(Some(DateTimeOffset::new(dt2(693_595, 36_000, 0), 60))),
                "1900-01-01 11:00:00 +01:00",
            ),
            (ColumnData::U8(Some(255)), "255"),
            (ColumnData::I64(Some(i64::MIN)), "-9223372036854775808"),
            (ColumnData::Bit(Some(true)), "1"),
            (ColumnData::F64(Some(0.1)), "0.1"),
            (ColumnData::String(Some("héllo".into())), "héllo"),
            (
                ColumnData::Numeric(Some(Numeric::new_with_scale(1250, 2))),
                "12.50",
            ),
            (
                ColumnData::Numeric(Some(Numeric::new_with_scale(-5, 3))),
                "-0.005",
            ),
            (
                ColumnData::Binary(Some(vec![0xDE, 0xAD, 0x0F].into())),
                "dead0f",
            ),
            (
                ColumnData::Guid(Some(tiberius::Uuid::from_bytes([0xab; 16]))),
                "ABABABAB-ABAB-ABAB-ABAB-ABABABABABAB",
            ),
            (ColumnData::Date(Some(Date::new(0))), "0001-01-01"),
            (ColumnData::Date(Some(Date::new(3_652_058))), "9999-12-31"),
            (ColumnData::Time(Some(Time::new(86_399, 0))), "23:59:59"),
            (ColumnData::Time(Some(Time::new(1_500, 3))), "00:00:01.500"),
            (
                ColumnData::DateTime2(Some(dt2(693_595, 452_961_234_567, 7))),
                "1900-01-01 12:34:56.1234567",
            ),
            (
                ColumnData::DateTimeOffset(Some(DateTimeOffset::new(dt2(693_595, 0, 0), 330))),
                "1900-01-01 05:30:00 +05:30",
            ),
            (
                ColumnData::DateTimeOffset(Some(DateTimeOffset::new(dt2(693_595, 0, 0), -60))),
                "1899-12-31 23:00:00 -01:00",
            ),
            (
                ColumnData::DateTime(Some(DateTime::new(0, 1))),
                "1900-01-01 00:00:00.003",
            ),
            (
                ColumnData::DateTime(Some(DateTime::new(0, 2))),
                "1900-01-01 00:00:00.007",
            ),
            (
                ColumnData::DateTime(Some(DateTime::new(-1, 25_919_999))),
                "1899-12-31 23:59:59.997",
            ),
            (
                ColumnData::SmallDateTime(Some(SmallDateTime::new(1, 61))),
                "1900-01-02 01:01:00",
            ),
        ];
        for (cell, want) in cases {
            assert_eq!(cell_text(&cell).as_deref(), Some(want), "{cell:?}");
        }
        assert_eq!(cell_text(&ColumnData::String(None)), None);
        assert_eq!(cell_text(&ColumnData::I32(None)), None);
    }

    /// A `decimal` override on an integer column scales the integer exactly.
    #[test]
    fn an_integer_column_under_a_decimal_override_is_scaled_not_nulled() {
        let d = |c: ColumnData<'static>, s: u8| decimal_cell(Some(&c), s, "v").unwrap();
        assert_eq!(d(ColumnData::I64(Some(1)), 0), Some(1));
        assert_eq!(d(ColumnData::I32(Some(7)), 2), Some(700));
        assert_eq!(d(ColumnData::I16(Some(-3)), 1), Some(-30));
        assert_eq!(d(ColumnData::U8(Some(255)), 0), Some(255));
        assert_eq!(d(ColumnData::I64(None), 2), None);
        assert_eq!(decimal_cell(None, 2, "v").unwrap(), None);
    }

    /// A valued cell the decimal, uuid or bytes arm cannot convert is refused with the coded error.
    #[test]
    fn a_valued_cell_of_an_unconvertible_type_is_refused_not_nulled() {
        let bit = ColumnData::Bit(Some(true));
        for err in [
            decimal_cell(Some(&bit), 0, "v").unwrap_err(),
            uuid_cell(Some(&bit), "v").map(|_| ()).unwrap_err(),
            binary_cell(Some(&ColumnData::I32(Some(1))), "v")
                .map(|_| ())
                .unwrap_err(),
        ] {
            let coded = err
                .downcast_ref::<crate::error::CodedError>()
                .expect("coded");
            assert_eq!(coded.code(), "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH");
            assert!(err.to_string().contains("column `v` is declared"), "{err}");
        }
        assert!(
            uuid_cell(Some(&ColumnData::String(None)), "v")
                .unwrap()
                .is_none()
        );
        assert!(
            binary_cell(Some(&ColumnData::Binary(None)), "v")
                .unwrap()
                .is_none()
        );
    }

    /// A `uuid` override on an nvarchar(36) column parses the text; a non-uuid is refused.
    #[test]
    fn a_uuid_override_on_text_parses_it_and_refuses_garbage() {
        let text = ColumnData::String(Some("6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b".into()));
        assert_eq!(
            uuid_cell(Some(&text), "u").unwrap(),
            Some([
                0x6f, 0x1c, 0x2a, 0x3b, 0x4d, 0x5e, 0x4f, 0x60, 0x8a, 0x7b, 0x9c, 0x0d, 0x1e, 0x2f,
                0x3a, 0x4b
            ])
        );
        let bad = ColumnData::String(Some("not-a-uuid".into()));
        let err = uuid_cell(Some(&bad), "u").unwrap_err();
        assert!(
            err.to_string()
                .contains("\"not-a-uuid\", which is not a uuid"),
            "{err}"
        );
    }

    #[test]
    fn only_a_null_cell_is_null_whatever_its_wire_type() {
        assert!(is_null_cell(&ColumnData::F64(None)));
        assert!(is_null_cell(&ColumnData::DateTime2(None)));
        assert!(!is_null_cell(&ColumnData::F64(Some(12.5))));
        assert!(!is_null_cell(&ColumnData::I64(Some(0))));
        assert_eq!(cell_type_name(&ColumnData::F64(Some(1.0))), "F64");
    }

    #[test]
    fn roast_rescale_i128_lossy_downscale_must_err() {
        let got = rescale_i128(12345, 2, 0);
        assert!(
            got.is_err(),
            "lossy down-scale of 12345 (scale 2 -> 0) must return Err, \
             got Ok({:?}) — the .45 cents were silently truncated",
            got.ok()
        );
    }

    #[test]
    fn roast_rescale_i128_lossless_paths_stay_ok() {
        let down = rescale_i128(12300, 2, 0)
            .expect("lossless down-scale of 12300 (scale 2 -> 0) must stay Ok");
        assert_eq!(
            down, 123,
            "lossless down-scale of 12300 (scale 2 -> 0): expected 123, got {down}"
        );
        let up = rescale_i128(123, 0, 2).expect("up-scale of 123 (scale 0 -> 2) must stay Ok");
        assert_eq!(
            up, 12300,
            "up-scale of 123 (scale 0 -> 2): expected 12300, got {up}"
        );
    }

    #[test]
    fn rescale_i128_equal_scale_is_identity() {
        assert_eq!(rescale_i128(12345, 2, 2).unwrap(), 12345);
        assert_eq!(rescale_i128(0, 0, 0).unwrap(), 0);
    }

    #[test]
    fn rescale_i128_upscale_overflow_must_err() {
        let got = rescale_i128(i128::MAX, 0, 2);
        assert!(
            got.is_err(),
            "up-scaling i128::MAX (scale 0 -> 2) must overflow loudly, got Ok({:?})",
            got.ok()
        );
        let msg = format!("{:#}", got.unwrap_err());
        assert!(
            msg.contains("overflow"),
            "overflow Err must say so, got: {msg}"
        );
    }

    #[test]
    fn a_legacy_datetime_rounds_to_the_nearest_microsecond_and_datetime2_truncates() {
        let at = |nanos| {
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_nano_opt(10, 0, 0, nanos)
                .unwrap()
        };
        let base = at(0).and_utc().timestamp_micros();
        assert_eq!(
            naive_micros(at(456_666_666), true) - base,
            456_667,
            "DATETIME .457 is .45666… s"
        );
        assert_eq!(
            naive_micros(at(456_666_666), false) - base,
            456_666,
            "DATETIME2 never rounds up"
        );
        assert_eq!(naive_micros(at(123_333_333), true) - base, 123_333);
    }

    /// A column the catalog marks finer than a microsecond is delivered as nanoseconds; every other column and every `columns:` override keeps its type.
    #[test]
    fn a_fine_timestamp_column_is_nanoseconds_and_its_neighbours_are_untouched() {
        let ts = |unit, tz: Option<&str>| RivetType::Timestamp {
            unit,
            timezone: tz.map(str::to_string),
        };
        let hints = CatalogHints {
            decimals: HashMap::new(),
            fine_timestamps: [
                "fine".to_string(),
                "fine_tz".to_string(),
                "forced".to_string(),
            ]
            .into_iter()
            .collect(),
            fine_times: HashSet::new(),
        };
        let none = ColumnOverrides::new();
        let resolve = |rivet, name, overrides: &ColumnOverrides| {
            resolve_catalog_type(rivet, name, overrides, Some(&hints), None)
        };
        let us = || ts(RivetTimeUnit::Microsecond, None);
        let ns = || ts(RivetTimeUnit::Nanosecond, None);
        assert_eq!(resolve(us(), "fine", &none), ns());
        assert_eq!(
            resolve(
                ts(RivetTimeUnit::Microsecond, Some("UTC")),
                "fine_tz",
                &none
            ),
            ts(RivetTimeUnit::Nanosecond, Some("UTC"))
        );
        assert_eq!(
            resolve(us(), "scale6", &none),
            us(),
            "datetime2(0..6) is exact"
        );
        let forced: ColumnOverrides = [("forced".to_string(), us())].into_iter().collect();
        assert_eq!(
            resolve(us(), "forced", &forced),
            us(),
            "an override is the operator's word"
        );
        assert_eq!(resolve(RivetType::Int32, "fine", &none), RivetType::Int32);
        assert_eq!(
            resolve_catalog_type(us(), "fine", &none, None, None),
            us(),
            "no catalog read, no first-batch evidence: the mapping is unchanged"
        );
    }

    /// A `time(7)` (the bare `time` default) is read at nanoseconds and keeps its 100ns tick; `time(0..6)` stays microseconds; the first value's own scale decides over the catalog, and an all-NULL column follows the catalog.
    #[test]
    fn a_scale_seven_time_is_nanoseconds_and_keeps_its_100ns_tick() {
        use tiberius::time::Time;
        let one = |scale: u8, increments: u64| {
            vec![
                Row::builder()
                    .column(
                        "t",
                        ColumnType::Timen,
                        ColumnData::Time(Some(Time::new(increments, scale))),
                    )
                    .build(),
            ]
        };
        let us = RivetType::Time {
            unit: RivetTimeUnit::Microsecond,
        };
        let ns = RivetType::Time {
            unit: RivetTimeUnit::Nanosecond,
        };
        let none = ColumnOverrides::new();
        let fine = CatalogHints {
            fine_times: ["t".to_string()].into_iter().collect(),
            ..CatalogHints::default()
        };
        let resolve = |hints: Option<&CatalogHints>, rows: &[Row]| {
            resolve_catalog_type(us.clone(), "t", &none, hints, Some((0, rows)))
        };
        assert_eq!(resolve(None, &one(7, 1)), ns, "a value's scale 7");
        assert_eq!(
            resolve(None, &one(6, 1)),
            us,
            "time(6) is exact at microseconds"
        );
        assert_eq!(
            resolve(Some(&fine), &one(3, 1)),
            us,
            "the value's scale over the catalog"
        );
        let nulls = vec![
            Row::builder()
                .column("t", ColumnType::Timen, ColumnData::Time(None))
                .build(),
        ];
        assert_eq!(resolve(Some(&fine), &nulls), ns, "all null: the catalog");
        assert_eq!(resolve(None, &nulls), us);
        let forced: ColumnOverrides = [("t".to_string(), us.clone())].into_iter().collect();
        assert_eq!(
            resolve_catalog_type(us.clone(), "t", &forced, Some(&fine), Some((0, &one(7, 1)))),
            us,
            "an override is the operator's word"
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Time64(TimeUnit::Nanosecond),
            true,
        )]));
        for (increments, want) in [(1u64, 100i64), (863_999_999_999, 86_399_999_999_900)] {
            let rows = one(7, increments);
            let batch =
                mssql_rows_to_record_batch(&schema, &rows, None, &ColumnOverrides::new()).unwrap();
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Time64NanosecondArray>()
                .unwrap();
            assert_eq!(col.value(0), want, "{increments}");
        }
    }

    /// A datetime2 or time column whose scale the catalog did not give is warned, a fine one by the catalog is not, and an override is the operator's word.
    #[test]
    fn a_catalog_blind_time_column_is_warned_unless_it_is_fine_or_overridden() {
        let row = Row::builder()
            .column("ts", ColumnType::Datetime2, ColumnData::DateTime2(None))
            .column("t", ColumnType::Timen, ColumnData::Time(None))
            .build();
        let warnings = |overrides: &ColumnOverrides, hints: Option<&CatalogHints>| {
            mssql_type_mappings(row.columns(), overrides, hints)
                .unwrap()
                .into_iter()
                .map(|m| m.warnings)
                .collect::<Vec<_>>()
        };
        let none = ColumnOverrides::new();
        let blind = warnings(&none, None);
        assert!(blind[0][0].starts_with("datetime2 → Timestamp(microsecond)"));
        assert_eq!(blind[1], vec![TIME_SCALE_UNREAD_WARNING.to_string()]);
        let empty = CatalogHints::default();
        assert_eq!(warnings(&none, Some(&empty)), blind);
        let fine = CatalogHints {
            fine_timestamps: ["ts".to_string()].into_iter().collect(),
            fine_times: ["t".to_string()].into_iter().collect(),
            ..CatalogHints::default()
        };
        assert_eq!(
            warnings(&none, Some(&fine)),
            vec![vec![FINE_TIMESTAMP_WARNING.to_string()], Vec::new()]
        );
        let forced: ColumnOverrides = [
            (
                "ts".to_string(),
                RivetType::Timestamp {
                    unit: RivetTimeUnit::Microsecond,
                    timezone: None,
                },
            ),
            (
                "t".to_string(),
                RivetType::Time {
                    unit: RivetTimeUnit::Microsecond,
                },
            ),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            warnings(&forced, None),
            vec![Vec::<String>::new(), Vec::new()]
        );
        let narrowed = warnings(&forced, Some(&fine));
        assert!(
            narrowed[0].len() == 1 && narrowed[0][0].contains("keeps 6"),
            "a microsecond override over a scale-7 datetime2 says it truncates: {narrowed:?}"
        );
        assert!(narrowed[1].is_empty(), "{narrowed:?}");
    }

    /// A `time(7)` built at microseconds drops the 100ns tick and keeps whole microseconds.
    #[test]
    fn a_scale_seven_time_built_at_microseconds_keeps_whole_microseconds() {
        use tiberius::time::Time;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Time64(TimeUnit::Microsecond),
            true,
        )]));
        for (increments, want) in [(1u64, 0i64), (10, 1), (863_999_999_999, 86_399_999_999)] {
            let rows = vec![
                Row::builder()
                    .column(
                        "t",
                        ColumnType::Timen,
                        ColumnData::Time(Some(Time::new(increments, 7))),
                    )
                    .build(),
            ];
            let batch =
                mssql_rows_to_record_batch(&schema, &rows, None, &ColumnOverrides::new()).unwrap();
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Time64MicrosecondArray>()
                .unwrap();
            assert_eq!(col.value(0), want, "{increments}");
        }
    }

    /// A `timestamp` override over a datetime2(7) column truncates the 100ns tick: the type report says lossy, not exact; a nanosecond override and a column the catalog shows at scale 0..6 stay exact.
    #[test]
    fn a_microsecond_override_over_a_scale_seven_column_is_reported_lossy() {
        use tiberius::time::{Date, DateTime2, Time};
        let dt2 = DateTime2::new(Date::new(693_595), Time::new(1, 7));
        let rows = [Row::builder()
            .column("t", ColumnType::Datetime2, ColumnData::DateTime2(Some(dt2)))
            .build()];
        let fine = CatalogHints {
            decimals: HashMap::new(),
            fine_timestamps: ["t".to_string()].into_iter().collect(),
            fine_times: HashSet::new(),
        };
        let coarse = CatalogHints::default();
        let fidelity = |ov: &str, hints: &CatalogHints| {
            let overrides: ColumnOverrides =
                [("t".to_string(), crate::types::parse_type_str(ov).unwrap())]
                    .into_iter()
                    .collect();
            mssql_type_mappings(rows[0].columns(), &overrides, Some(hints)).unwrap()[0].fidelity
        };
        use crate::types::TypeFidelity::{Exact, Lossy};
        assert_eq!(fidelity("timestamp", &fine), Lossy);
        assert_eq!(fidelity("timestamp_tz", &fine), Lossy);
        assert_eq!(fidelity("timestamp_ns", &fine), Exact);
        assert_eq!(
            fidelity("timestamp", &coarse),
            Exact,
            "datetime2(0..6) is exact"
        );
    }

    /// The first non-null datetime2 / datetimeoffset value's own scale decides the type over the catalog; with no such value the catalog decides.
    #[test]
    fn a_fine_timestamp_is_read_from_the_first_values_own_scale() {
        use tiberius::time::{Date, DateTime2, DateTimeOffset, Time};
        let dt2 = |scale: u8| DateTime2::new(Date::new(693_595), Time::new(1, scale));
        let one =
            |name: &'static str, ty, data| vec![Row::builder().column(name, ty, data).build()];
        let datetime2 = |scale: u8| {
            one(
                "t",
                ColumnType::Datetime2,
                ColumnData::DateTime2(Some(dt2(scale))),
            )
        };
        let offset = |scale: u8| {
            one(
                "t",
                ColumnType::DatetimeOffsetn,
                ColumnData::DateTimeOffset(Some(DateTimeOffset::new(dt2(scale), 60))),
            )
        };
        let us = RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: None,
        };
        let ns = RivetType::Timestamp {
            unit: RivetTimeUnit::Nanosecond,
            timezone: None,
        };
        let none = ColumnOverrides::new();
        let catalog_fine = CatalogHints {
            decimals: HashMap::new(),
            fine_timestamps: ["t".to_string()].into_iter().collect(),
            fine_times: HashSet::new(),
        };
        let catalog_empty = CatalogHints {
            decimals: HashMap::new(),
            fine_timestamps: HashSet::new(),
            fine_times: HashSet::new(),
        };
        let resolve = |hints: Option<&CatalogHints>, rows: &[Row]| {
            resolve_catalog_type(us.clone(), "t", &none, hints, Some((0, rows)))
        };
        for scale in [7, 0] {
            assert_eq!(first_timestamp_scale(0, &datetime2(scale)), Some(scale));
            assert_eq!(first_timestamp_scale(0, &offset(scale)), Some(scale));
        }
        assert_eq!(resolve(None, &datetime2(7)), ns, "a query's scale-7 column");
        assert_eq!(resolve(None, &offset(7)), ns, "a query's scale-7 offset");
        assert_eq!(resolve(None, &datetime2(6)), us, "scale 6 is exact");
        assert_eq!(resolve(None, &datetime2(0)), us);
        assert_eq!(
            resolve(Some(&catalog_fine), &datetime2(3)),
            us,
            "the value's scale over the catalog"
        );
        assert_eq!(resolve(Some(&catalog_empty), &datetime2(7)), ns);
        let nulls = one("t", ColumnType::Datetime2, ColumnData::DateTime2(None));
        assert_eq!(first_timestamp_scale(0, &nulls), None);
        assert_eq!(
            resolve(Some(&catalog_fine), &nulls),
            ns,
            "all null: the catalog"
        );
        assert_eq!(resolve(Some(&catalog_empty), &nulls), us);
        let text = one("t", ColumnType::NVarchar, ColumnData::String(None));
        assert_eq!(first_timestamp_scale(0, &text), None);
    }

    #[test]
    fn rescale_i128_negative_values() {
        assert!(
            rescale_i128(-12345, 2, 0).is_err(),
            "-123.45 (scale 2 -> 0) drops -.45 and must Err"
        );
        assert_eq!(rescale_i128(-12300, 2, 0).unwrap(), -123);
    }

    #[test]
    fn rescale_i128_zero_downscales_losslessly() {
        assert_eq!(rescale_i128(0, 38, 0).unwrap(), 0);
    }

    #[test]
    fn rescale_i128_scale_arithmetic_is_a_difference_not_a_sum() {
        assert_eq!(
            rescale_i128(123, 1, 3).unwrap(),
            12_300,
            "up-scale must multiply by 10^(to-from), not 10^(to+from)"
        );
        assert_eq!(
            rescale_i128(12_300, 3, 1).unwrap(),
            123,
            "down-scale must divide by 10^(from-to), not 10^(from+to)"
        );
    }

    #[test]
    fn rescale_i128_max_scale_boundary_no_panic() {
        assert!(
            rescale_i128(1, 38, 0).is_err(),
            "1e-38 (scale 38 -> 0) is lossy and must Err, not panic"
        );
        assert_eq!(rescale_i128(1, 0, 38).unwrap(), 10i128.pow(38));
    }

    #[test]
    fn rescale_i128_lossy_err_is_actionable() {
        let msg = format!("{:#}", rescale_i128(12345, 2, 0).unwrap_err());
        for needle in ["12345", "scale", "2", "0", "columns:", "override"] {
            assert!(
                msg.contains(needle),
                "lossy-rescale Err must mention {needle:?}, got: {msg}"
            );
        }
    }
}

#[cfg(test)]
mod renderer_twins {
    use super::{cell_text, numeric_to_decimal_string};
    use crate::types::{decimal_plain, hex_bytes, uuid36};
    use tiberius::ColumnData;

    #[test]
    fn numeric_text_matches_decimal_plain() {
        for (u, s) in [
            (12345, 2),
            (-12345, 2),
            (5, 3),
            (-5, 3),
            (150, 2),
            (0, 0),
            (0, 3),
            (42, 0),
            (-42, 0),
            (i128::MIN, 0),
            (i128::MAX, 38),
            (99_999_999_999_999_999_999_999_999_999_999_999_999, 38),
        ] {
            assert_eq!(
                numeric_to_decimal_string(u, s),
                decimal_plain(u, s as i8),
                "({u}, {s})"
            );
        }
    }

    #[test]
    fn binary_cell_text_matches_hex_bytes() {
        let all: Vec<u8> = (0..=255).collect();
        for b in [vec![], vec![0x00], vec![0x0A, 0xFF], all] {
            let cell = ColumnData::Binary(Some(b.clone().into()));
            assert_eq!(cell_text(&cell), Some(hex_bytes(&b)));
        }
    }

    #[test]
    fn guid_cell_text_matches_uuid36_but_for_case() {
        let g = tiberius::Uuid::from_bytes([0xAB; 16]);
        let cell = ColumnData::Guid(Some(g));
        assert_eq!(
            cell_text(&cell).map(|t| t.to_lowercase()),
            Some(uuid36(g.as_bytes()))
        );
    }

    #[test]
    /// Strict known divergence: ADR-0038 divergence.
    #[should_panic(expected = "assertion")]
    fn guid_cell_text_matches_uuid36() {
        let g = tiberius::Uuid::from_bytes([0xAB; 16]);
        assert_eq!(
            cell_text(&ColumnData::Guid(Some(g))),
            Some(uuid36(g.as_bytes()))
        );
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    /// A lossy decimal rescale and an overflowing one carry their codes.
    #[test]
    fn money_and_decimal_refusals_carry_their_codes() {
        let err = rescale_i128(123, 2, 0).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
        let err = rescale_i128(i128::MAX, 0, 2).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5);
    }

    /// Every `columns:` override type over every wire value variant ends Ok or as a coded usage refusal, never an internal stop.
    #[test]
    fn no_override_over_any_wire_value_ends_as_an_internal_stop() {
        use tiberius::numeric::Numeric;
        use tiberius::time::{Date, DateTime, DateTime2, DateTimeOffset, SmallDateTime, Time};
        let dt2 = DateTime2::new(Date::new(693_595), Time::new(0, 0));
        let cells: Vec<(ColumnType, ColumnData<'static>)> = vec![
            (ColumnType::Bit, ColumnData::Bit(Some(true))),
            (ColumnType::Int1, ColumnData::U8(Some(1))),
            (ColumnType::Int2, ColumnData::I16(Some(1))),
            (ColumnType::Int4, ColumnData::I32(Some(1))),
            (ColumnType::Int8, ColumnData::I64(Some(1))),
            (ColumnType::Float4, ColumnData::F32(Some(1.5))),
            (ColumnType::Float8, ColumnData::F64(Some(1.5))),
            (
                ColumnType::Numericn,
                ColumnData::Numeric(Some(Numeric::new_with_scale(125, 2))),
            ),
            (ColumnType::NVarchar, ColumnData::String(Some("x".into()))),
            (
                ColumnType::BigVarBin,
                ColumnData::Binary(Some(vec![1u8, 2].into())),
            ),
            (
                ColumnType::Guid,
                ColumnData::Guid(Some(tiberius::Uuid::from_bytes([1; 16]))),
            ),
            (ColumnType::Daten, ColumnData::Date(Some(Date::new(1)))),
            (ColumnType::Timen, ColumnData::Time(Some(Time::new(1, 0)))),
            (ColumnType::Datetime2, ColumnData::DateTime2(Some(dt2))),
            (
                ColumnType::DatetimeOffsetn,
                ColumnData::DateTimeOffset(Some(DateTimeOffset::new(dt2, 0))),
            ),
            (
                ColumnType::Datetime,
                ColumnData::DateTime(Some(DateTime::new(0, 1))),
            ),
            (
                ColumnType::Datetime4,
                ColumnData::SmallDateTime(Some(SmallDateTime::new(1, 1))),
            ),
            (ColumnType::Int4, ColumnData::I32(None)),
        ];
        let overrides = [
            "bool",
            "int2",
            "int4",
            "int8",
            "float4",
            "float8",
            "decimal(10,2)",
            "decimal(50,2)",
            "text",
            "binary",
            "date",
            "json",
            "uuid",
            "timestamp",
            "timestamp_ns",
            "timestamp_tz",
            "timestamp_tz_ns",
        ];
        for ov in overrides {
            let rivet = crate::types::parse_type_str(ov).unwrap();
            let mapping = TypeMapping::from_source(&SourceColumn::simple("c", "x", true), rivet);
            let field = build_arrow_field(&mapping).expect("override has an Arrow field");
            let probe = build_array(
                field.data_type(),
                0,
                &[],
                "c",
                None,
                Some(&mapping.rivet_type),
            );
            if let Err(e) =
                crate::source::refuse_unbuildable_override("mssql", "c", &mapping.rivet_type, probe)
            {
                crate::error::assert_coded(&e, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
                continue;
            }
            let schema = Arc::new(Schema::new(vec![field]));
            for (ty, data) in &cells {
                let rows = [Row::builder().column("c", *ty, data.clone()).build()];
                let declared =
                    ColumnOverrides::from([("c".to_string(), mapping.rivet_type.clone())]);
                if let Err(e) = mssql_rows_to_record_batch(&schema, &rows, None, &declared) {
                    let code = crate::error::error_code(&e);
                    assert_ne!(
                        code,
                        Some("RIVET_INTERNAL_TYPE_BUILDER"),
                        "override {ov} over {data:?}: {e:#}"
                    );
                }
            }
        }
    }

    /// A native column (no `columns:` override) whose cell the builder cannot read is an internal stop, never a blame on the operator.
    #[test]
    fn a_foreign_cell_in_a_native_column_is_an_internal_stop() {
        let foreign = [
            (DataType::Int32, ColumnData::I64(Some(1))),
            (DataType::Int16, ColumnData::I32(Some(1))),
            (DataType::Date32, ColumnData::I32(Some(1))),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                ColumnData::I32(Some(1)),
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                ColumnData::I32(Some(1)),
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                ColumnData::I32(Some(1)),
            ),
        ];
        for (ty, data) in foreign {
            let schema = Arc::new(Schema::new(vec![Field::new("c", ty.clone(), true)]));
            let rows = [Row::builder().column("c", ColumnType::Int4, data).build()];
            let err = mssql_rows_to_record_batch(&schema, &rows, None, &ColumnOverrides::new())
                .unwrap_err();
            crate::error::assert_coded(&err, "RIVET_INTERNAL_TYPE_BUILDER", 6);
            let declared = ColumnOverrides::from([("c".to_string(), RivetType::Int32)]);
            let err = mssql_rows_to_record_batch(&schema, &rows, None, &declared).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
            assert!(
                format!("{err:#}").contains("is declared int32 by a `columns:` override"),
                "{err:#}"
            );
        }
    }

    /// An operator-chosen decimal(p,s) with p <= 38 over the column the wide override was refused for delivers the cents exactly and inside its precision.
    #[test]
    fn a_valid_decimal_declaration_delivers_the_cents_on_sql_server() {
        use arrow::util::display::array_value_to_string;
        use tiberius::numeric::Numeric;
        for (declared, unscaled, source_scale, delivered) in [
            ("decimal(10,2)", 1_250i128, 2u8, "12.50"),
            ("decimal(38,2)", 123_456, 2, "1234.56"),
            ("decimal(38,10)", 125, 1, "12.5000000000"),
        ] {
            let rivet = crate::types::parse_type_str(declared).unwrap();
            let mapping = TypeMapping::from_source(&SourceColumn::simple("c", "x", true), rivet);
            let field = build_arrow_field(&mapping).expect("declaration has an Arrow field");
            let overrides = ColumnOverrides::from([("c".to_string(), mapping.rivet_type.clone())]);
            let schema = Arc::new(Schema::new(vec![field]));
            let rows = [Row::builder()
                .column(
                    "c",
                    ColumnType::Numericn,
                    ColumnData::Numeric(Some(Numeric::new_with_scale(unscaled, source_scale))),
                )
                .build()];
            let batch = mssql_rows_to_record_batch(&schema, &rows, None, &overrides).unwrap();
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .expect("decimal column");
            values
                .validate_decimal_precision(values.precision())
                .unwrap_or_else(|e| panic!("{declared}: {e}"));
            assert_eq!(
                array_value_to_string(batch.column(0), 0).unwrap(),
                delivered,
                "{declared}"
            );
        }
    }

    /// The schema stage refuses a wide decimal override by code, builds a narrow one, and leaves the same column without an override alone.
    #[test]
    fn the_mssql_schema_stage_refuses_a_wide_decimal_override() {
        use tiberius::numeric::Numeric;
        let rows = [Row::builder()
            .column(
                "d",
                ColumnType::Numericn,
                ColumnData::Numeric(Some(Numeric::new_with_scale(125, 2))),
            )
            .build()];
        let declared = |ov: &str| {
            ColumnOverrides::from([("d".to_string(), crate::types::parse_type_str(ov).unwrap())])
        };
        let err =
            mssql_columns_to_schema(rows[0].columns(), &declared("decimal(50,2)"), &rows, None)
                .unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
        assert!(
            err.to_string().contains(
                "its precision is above 38; declare the column's own `decimal(p,s)` with p <= 38"
            ),
            "{err}"
        );
        for ok in [declared("decimal(38,2)"), ColumnOverrides::new()] {
            let (schema, _) = mssql_columns_to_schema(rows[0].columns(), &ok, &rows, None).unwrap();
            assert_eq!(schema.fields().len(), 1);
        }
    }
}

#[cfg(test)]
mod money_tests {
    use super::*;
    use crate::source::value_checksum::CellSource;
    use arrow::array::{Array, Decimal128Array, Float64Array};
    use tiberius::numeric::Numeric;

    fn money(v: i64) -> ColumnData<'static> {
        ColumnData::Numeric(Some(Numeric::new_with_scale(i128::from(v), 4)))
    }

    fn rows_of(ty: ColumnType, cells: Vec<ColumnData<'static>>) -> Vec<Row> {
        cells
            .into_iter()
            .map(|c| Row::builder().column("c", ty, c).build())
            .collect()
    }

    fn batch(
        dt: DataType,
        rows: &[Row],
        declared: &ColumnOverrides,
    ) -> Result<arrow::record_batch::RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("c", dt, true)]));
        mssql_rows_to_record_batch(&schema, rows, None, declared)
    }

    /// MONEY at the type's extremes and a NULL arrive as the exact unscaled integers.
    #[test]
    fn money_extremes_are_delivered_exact() {
        let rows = rows_of(
            ColumnType::Money,
            vec![
                money(i64::MAX),
                money(i64::MIN),
                money(9_007_199_254_740_993),
                ColumnData::Numeric(None),
            ],
        );
        let b = batch(DataType::Decimal128(19, 4), &rows, &ColumnOverrides::new()).unwrap();
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(a.value(0), i128::from(i64::MAX));
        assert_eq!(a.value(1), i128::from(i64::MIN));
        assert_eq!(a.value(2), 9_007_199_254_740_993);
        assert!(a.is_null(3));
    }

    /// SMALLMONEY at the type's extremes is exact.
    #[test]
    fn smallmoney_extremes_are_delivered_exact() {
        let rows = rows_of(
            ColumnType::Money4,
            vec![money(i64::from(i32::MAX)), money(i64::from(i32::MIN))],
        );
        let b = batch(DataType::Decimal128(10, 4), &rows, &ColumnOverrides::new()).unwrap();
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(a.value(0), i128::from(i32::MAX));
        assert_eq!(a.value(1), i128::from(i32::MIN));
    }

    /// The value-checksum side reads every exact integer cell and a money cell without the f64 detour.
    #[test]
    fn the_checksum_source_reads_integers_and_money_exact() {
        let cases = [
            (ColumnType::Int1, ColumnData::U8(Some(7)), 70_000_i128),
            (ColumnType::Int2, ColumnData::I16(Some(-7)), -70_000),
            (ColumnType::Int4, ColumnData::I32(Some(7)), 70_000),
            (ColumnType::Int8, ColumnData::I64(Some(-7)), -70_000),
            (ColumnType::Money, money(i64::MAX), i128::from(i64::MAX)),
        ];
        for (ty, cell, want) in cases {
            let rows = rows_of(ty, vec![cell]);
            let src = MssqlCellSource { rows: &rows };
            assert_eq!(src.decimal128(0, 0, 4), Some(want), "{ty:?}");
        }
        let rows = rows_of(ColumnType::Money, vec![money(1_234_500)]);
        assert_eq!(MssqlCellSource { rows: &rows }.float64(0, 0), Some(123.45));
    }

    /// A `float64` override over MONEY stores the same f64 the driver used to deliver, NULL kept.
    #[test]
    fn a_float64_override_over_money_keeps_the_old_f64() {
        for (scaled, want) in [(123_456_i64, 12.3456_f64), (-5, -0.0005), (0, 0.0)] {
            let rows = rows_of(
                ColumnType::Money,
                vec![money(scaled), ColumnData::Numeric(None)],
            );
            let b = batch(DataType::Float64, &rows, &ColumnOverrides::new()).unwrap();
            let a = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(a.value(0), want);
            assert_eq!(a.value(0), scaled as f64 / 10000.0);
            assert!(a.is_null(1));
        }
    }

    /// A `float64` override over MONEY delivers a value the float prints back as written and refuses, by code, one it would round.
    #[test]
    fn a_float64_override_over_money_refuses_a_value_the_float_would_round() {
        for (scaled, want) in [
            (123_456_789_012_345_i64, 12345678901.2345_f64),
            (9_223_372_036_854_775, 922337203685.4775),
            (-9_007_199_254_740_992, -900719925474.0992),
        ] {
            let rows = rows_of(ColumnType::Money, vec![money(scaled)]);
            let b = batch(DataType::Float64, &rows, &ColumnOverrides::new()).unwrap();
            let a = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(a.value(0), want, "{scaled}");
        }
        for scaled in [
            i64::MAX,
            i64::MIN,
            9_007_199_254_740_993,
            9_223_372_036_854_775_806,
        ] {
            let rows = rows_of(ColumnType::Money, vec![money(scaled)]);
            let err = batch(DataType::Float64, &rows, &ColumnOverrides::new()).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5);
            let said = format!("{err:#}");
            assert!(said.contains("column `c` is declared float8"), "{said}");
            assert!(said.contains("Declare `decimal(p,s)`"), "{said}");
        }
    }

    /// A NUMERIC column is still refused by a Float64 target; only MONEY widens.
    #[test]
    fn a_numeric_column_into_float64_is_still_refused() {
        let rows = rows_of(
            ColumnType::Numericn,
            vec![ColumnData::Numeric(Some(Numeric::new_with_scale(125, 2)))],
        );
        assert!(batch(DataType::Float64, &rows, &ColumnOverrides::new()).is_err());
    }

    /// A `decimal` override at a narrower scale keeps a value that fits and refuses one that would lose digits.
    #[test]
    fn a_narrower_decimal_override_over_money_refuses_lost_digits() {
        let fits = rows_of(ColumnType::Money, vec![money(123_400)]);
        let b = batch(DataType::Decimal128(19, 2), &fits, &ColumnOverrides::new()).unwrap();
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(a.value(0), 1_234);
        let lossy = rows_of(ColumnType::Money, vec![money(123_456)]);
        let err = batch(DataType::Decimal128(19, 2), &lossy, &ColumnOverrides::new()).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
    }

    /// Only the two money wire types are money.
    #[test]
    fn only_money_and_smallmoney_are_money() {
        assert!(is_money(ColumnType::Money));
        assert!(is_money(ColumnType::Money4));
        assert!(!is_money(ColumnType::Float8));
        assert!(!is_money(ColumnType::Numericn));
        assert!(!is_money(ColumnType::Decimaln));
    }
}
