//! Postgres → Arrow conversion machinery.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, ListBuilder, StringBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::Timelike as _;
use postgres::Row;
use postgres::types::{FromSql as PgFromSql, Kind, Type};

use crate::error::Result;
use crate::source::pg_numeric_wire::{
    PgNumericWire, numeric_wire_normalized_plain, numeric_wire_special_text,
};
use crate::types::kind::CommonKind;
use crate::types::{
    ColumnOverrides, RivetType, SourceColumn, TimeUnit as RivetTimeUnit, TypeMapping,
    build_arrow_field,
};

use crate::source::value_within_ceiling;

/// PostgreSQL `uuid` rows materialised as their canonical 16-byte form.
#[derive(Clone)]
struct PgUuidBytes([u8; 16]);

impl<'a> PgFromSql<'a> for PgUuidBytes {
    fn accepts(ty: &Type) -> bool {
        ty == &Type::UUID
    }

    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() == 16 {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(raw);
            return Ok(Self(bytes));
        }
        let text = simdutf8::basic::from_utf8(raw)?.trim();
        Ok(Self(*uuid::Uuid::parse_str(text)?.as_bytes()))
    }
}

/// PostgreSQL `time` as raw microseconds since midnight, refusing `24:00:00`.
struct PgTimeMicros(i64);

impl<'a> PgFromSql<'a> for PgTimeMicros {
    fn accepts(ty: &Type) -> bool {
        ty == &Type::TIME
    }

    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let us = i64::from_be_bytes(raw.try_into()?);
        if !crate::types::is_time_of_day(us) {
            return Err(format!("time of {us} microseconds is outside 00:00..24:00").into());
        }
        Ok(Self(us))
    }
}

/// Any PostgreSQL integer cell (`int2`/`int4`/`int8`/`oid`) decoded at its wire width, widened to i64.
struct PgInt(i64);

impl<'a> PgFromSql<'a> for PgInt {
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INT2 | Type::INT4 | Type::INT8 | Type::OID)
    }

    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(match *ty {
            Type::INT2 => i16::from_be_bytes(raw.try_into()?).into(),
            Type::INT4 => i32::from_be_bytes(raw.try_into()?).into(),
            Type::OID => u32::from_be_bytes(raw.try_into()?).into(),
            _ => i64::from_be_bytes(raw.try_into()?),
        }))
    }
}

/// A `columns:` override the wire value cannot be read as, named instead of panicking in `Row::get`.
fn pg_override_mismatch(
    col: &str,
    wire: &Type,
    declared: &str,
    cause: impl std::fmt::Display,
) -> anyhow::Error {
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
        format!(
            "postgres: column `{col}` is declared {declared} by a `columns:` override but \
             PostgreSQL sends it as {wire} ({cause}) — rivet does not convert it. Remove the \
             override, or CAST the column to that type in the export's `query:`.",
        ),
    ))
}

/// A wire payload that does not decode (invalid UTF-8 from a SQL_ASCII server, a malformed value), refused by name.
fn pg_undecodable(
    col: &str,
    wire: &Type,
    cause: &(dyn std::error::Error + 'static),
) -> anyhow::Error {
    let msg = if cause.is::<std::str::Utf8Error>() || cause.is::<simdutf8::basic::Utf8Error>() {
        format!(
            "postgres: column `{col}` ({wire}) holds a value that is not valid UTF-8 ({cause}) — \
             the server stored the bytes unchecked (a SQL_ASCII database does). rivet refuses \
             rather than aborting or writing NULL. CAST the column to bytea in the export's \
             `query:` (e.g. `{col}::bytea`), or convert the database to a UTF-8 server encoding."
        )
    } else {
        format!(
            "postgres: column `{col}` ({wire}) sent a malformed wire payload rivet cannot decode \
             ({cause}). rivet refuses rather than writing NULL, because a NULL here is \
             indistinguishable from a genuinely absent value. CAST the column to text in the \
             export's `query:`, or exclude the column."
        )
    };
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
        msg,
    ))
}

/// Name a failed cell read: a wire type the declared type rejects, or a payload that does not decode.
fn pg_cell_error(
    col: &str,
    wire: &Type,
    declared: &str,
    cause: &(dyn std::error::Error + 'static),
) -> anyhow::Error {
    if cause.is::<postgres::types::WrongType>() {
        pg_override_mismatch(col, wire, declared, cause)
    } else {
        pg_undecodable(col, wire, cause)
    }
}

/// Read one cell as `T`, turning a wire/override mismatch or an undecodable payload into a named error.
fn pg_cell<'a, T: PgFromSql<'a>>(
    row: &'a Row,
    col_idx: usize,
    declared: &str,
) -> Result<Option<T>> {
    row.try_get::<_, Option<T>>(col_idx).map_err(|e| {
        let c = &row.columns()[col_idx];
        let cause = std::error::Error::source(&e).unwrap_or(&e);
        pg_cell_error(c.name(), c.type_(), declared, cause)
    })
}

/// Side A re-reads a cell `build_array` already decoded; an error there surfaces as a checksum mismatch.
fn side_a<T>(cell: Result<Option<T>>) -> Option<T> {
    cell.ok().flatten()
}

/// Narrow a widened integer to the declared width, refusing a value that does not fit.
fn narrow_int<T: TryFrom<i64>>(v: i64, col: &str, wire: &str, declared: &str) -> Result<T> {
    T::try_from(v).map_err(|_| {
        anyhow::Error::new(crate::error::CodedError::new(
            crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
            format!(
                "postgres: column `{col}` ({wire}) holds {v}, which does not fit the {declared} \
                 declared by its `columns:` override — rivet refuses rather than wrapping \
                 or writing NULL. Declare a wider integer type, or remove the override.",
            ),
        ))
    })
}

/// An integer cell narrowed to the declared width, refusing a value that does not fit.
fn pg_int_cell<T: TryFrom<i64>>(row: &Row, col_idx: usize, declared: &str) -> Result<Option<T>> {
    match pg_cell::<PgInt>(row, col_idx, declared)? {
        None => Ok(None),
        Some(PgInt(v)) => {
            let c = &row.columns()[col_idx];
            narrow_int(v, c.name(), &c.type_().to_string(), declared).map(Some)
        }
    }
}

/// PostgreSQL `json` / `jsonb` cells borrowed as their raw source text.
pub(super) struct PgJsonRawText<'a>(pub(super) &'a str);

impl<'a> PgFromSql<'a> for PgJsonRawText<'a> {
    fn accepts(ty: &Type) -> bool {
        ty == &Type::JSON || ty == &Type::JSONB
    }

    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let payload = if *ty == Type::JSONB {
            match raw.split_first() {
                Some((&1, rest)) => rest,
                _ => return Err("unsupported JSONB wire format version (expected 1)".into()),
            }
        } else {
            raw
        };
        Ok(Self(simdutf8::basic::from_utf8(payload)?))
    }
}

/// A PostgreSQL date/time/timestamp the Arrow type cannot hold.
fn unrepresentable_temporal(col: &str, kind: &str, cause: impl std::fmt::Display) -> anyhow::Error {
    let msg = format!(
        "postgres: column `{col}` holds a {kind} value Arrow cannot represent ({cause}) — \
         almost certainly PostgreSQL's `infinity` or `-infinity` sentinel, which has \
         no instant to map to, or a time of `24:00:00`, which Arrow's TIME range \
         [00:00, 24:00) excludes. rivet refuses rather than writing NULL, because a NULL \
         here is indistinguishable from a genuinely absent value and every count and \
         checksum would agree about the loss. Project the column through a `query:` \
         that maps the sentinels to a real bound (e.g. \
         `CASE WHEN {col} = 'infinity' THEN '9999-12-31' ELSE {col} END`), or exclude \
         the column."
    );
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
        msg,
    ))
}

fn pg_numeric_optional_utf8_string(row: &Row, col_idx: usize) -> Result<Option<String>> {
    match row.try_get::<_, Option<PgNumericWire<'_>>>(col_idx)? {
        None => Ok(None),
        Some(w) => Ok(numeric_raw_to_optional_decimal_text(w.0)),
    }
}

fn numeric_raw_to_optional_decimal_text(raw: &[u8]) -> Option<String> {
    numeric_wire_normalized_plain(raw)
        .or_else(|| numeric_wire_special_text(raw).map(str::to_owned))
        .or_else(|| {
            let text = simdutf8::basic::from_utf8(raw).ok()?.trim();
            (!text.is_empty()).then(|| text.to_owned())
        })
}

/// The common kind a PostgreSQL wire type is read as, or `None` for a type this engine alone carries.
fn pg_common_kind(t: &Type) -> Option<CommonKind> {
    Some(match *t {
        Type::BOOL => CommonKind::Bool,
        Type::INT2 => CommonKind::I16,
        Type::INT4 => CommonKind::I32,
        Type::INT8 => CommonKind::I64,
        Type::FLOAT4 => CommonKind::F32,
        Type::FLOAT8 => CommonKind::F64,
        Type::NUMERIC => CommonKind::Dec,
        Type::DATE => CommonKind::Date,
        Type::TIMESTAMP => CommonKind::Naive,
        Type::TIMESTAMPTZ => CommonKind::Utc,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => CommonKind::Text,
        Type::BYTEA => CommonKind::Binary,
        Type::JSON | Type::JSONB => CommonKind::Json,
        _ => return None,
    })
}

/// The type of a `numeric` column whose precision and scale the wire does not carry.
fn bare_numeric() -> RivetType {
    RivetType::Unsupported {
        native_type: "numeric".into(),
        reason: "precision/scale unavailable from query metadata and catalog lookup; \
                 use a column override (e.g. columns: amount: decimal(18,2)), \
                 or a single-table SELECT ... FROM schema.table \
                 when the DDL declares numeric precision."
            .into(),
    }
}

/// Map a PostgreSQL wire-protocol type to Rivet's canonical type.
fn pg_type_to_rivet(t: &Type) -> RivetType {
    if let Some(kind) = pg_common_kind(t) {
        return kind.rivet_type().unwrap_or_else(bare_numeric);
    }
    match *t {
        Type::OID => RivetType::Int64,
        Type::TIME => RivetType::Time {
            unit: RivetTimeUnit::Microsecond,
        },
        Type::UUID => RivetType::Uuid,
        Type::INTERVAL => RivetType::Interval,

        _ => match t.kind() {
            Kind::Enum(_) => RivetType::Enum,
            Kind::Array(elem_type) => RivetType::List {
                inner: Box::new(pg_type_to_rivet(elem_type)),
            },
            _ => RivetType::Unsupported {
                native_type: t.name().to_string(),
                reason: "no Rivet mapping for this PostgreSQL type".into(),
            },
        },
    }
}

/// The type of column `name` read as wire type `t`; a `numeric` takes its catalog hint when it has one.
fn pg_column_rivet_type(
    t: &Type,
    name: &str,
    numeric_hints: Option<&HashMap<String, (u8, i8)>>,
) -> RivetType {
    if pg_common_kind(t) == Some(CommonKind::Dec)
        && let Some(&(precision, scale)) = numeric_hints.and_then(|h| h.get(name))
    {
        return RivetType::Decimal { precision, scale };
    }
    pg_type_to_rivet(t)
}

/// Apply per-column overrides + numeric catalog hints on top of the wire-type derived RivetType.
pub(super) fn rivet_type_for_pg_column(
    col: &postgres::Column,
    column_overrides: &ColumnOverrides,
    numeric_hints: Option<&HashMap<String, (u8, i8)>>,
) -> crate::error::Result<RivetType> {
    crate::types::resolve_column(
        crate::types::override_table::Engine::Postgres,
        column_overrides,
        col.name(),
        col.type_().name(),
        || pg_column_rivet_type(col.type_(), col.name(), numeric_hints),
    )
}

/// Build an Arrow `Schema` from PostgreSQL `Column` descriptors by routing each column through the `SourceColumn → RivetType → TypeMapping → Field` pipeline.
pub(super) fn pg_columns_to_schema(
    columns: &[postgres::Column],
    column_overrides: &ColumnOverrides,
    numeric_catalog_hints: Option<&HashMap<String, (u8, i8)>>,
) -> crate::error::Result<Schema> {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut errors: Vec<String> = Vec::new();
    for col in columns {
        let rivet = rivet_type_for_pg_column(col, column_overrides, numeric_catalog_hints)?;
        let source = SourceColumn::simple(col.name(), col.type_().name(), true);
        let mapping = TypeMapping::from_source(&source, rivet);
        match build_arrow_field(&mapping) {
            Some(field) => fields.push(field),
            None => {
                let reason = match &mapping.rivet_type {
                    RivetType::Unsupported { reason, .. } => reason.as_str(),
                    _ => "no Rivet mapping for this PostgreSQL type",
                };
                errors.push(format!(
                    "  • {} (PG type '{}'): {reason}",
                    col.name(),
                    col.type_().name()
                ));
            }
        }
    }
    if !errors.is_empty() {
        return Err(crate::source::unsafe_mapping_refusal(format!(
            "{} column(s) have no safe Rivet mapping — add column overrides in rivet.yaml:\n\
             columns:\n{}",
            errors.len(),
            errors.join("\n")
        )));
    }
    Ok(Schema::new(fields))
}

/// Reads a PostgreSQL `INTERVAL` value from its 16-byte binary wire format.
pub(super) struct PgInterval {
    pub(super) microseconds: i64,
    pub(super) days: i32,
    pub(super) months: i32,
}

impl<'a> postgres_types::FromSql<'a> for PgInterval {
    fn from_sql(
        _ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err(format!("expected 16-byte interval, got {}", raw.len()).into());
        }
        let microseconds = i64::from_be_bytes(raw[0..8].try_into()?);
        let days = i32::from_be_bytes(raw[8..12].try_into()?);
        let months = i32::from_be_bytes(raw[12..16].try_into()?);
        Ok(Self {
            microseconds,
            days,
            months,
        })
    }
    fn accepts(ty: &postgres_types::Type) -> bool {
        *ty == postgres_types::Type::INTERVAL
    }
}

/// Serialise a PostgreSQL INTERVAL to an ISO 8601 duration string.
pub(crate) fn pg_interval_to_iso8601(months: i32, days: i32, microseconds: i64) -> String {
    use std::fmt::Write as _;
    let years = months / 12;
    let m = months % 12;
    let mut s = String::from("P");
    if years != 0 {
        write!(s, "{years}Y").ok();
    }
    if m != 0 {
        write!(s, "{m}M").ok();
    }
    if days != 0 {
        write!(s, "{days}D").ok();
    }
    if microseconds != 0 {
        let neg = microseconds < 0;
        let abs = microseconds.unsigned_abs();
        let h = abs / 3_600_000_000;
        let r = abs % 3_600_000_000;
        let mi = r / 60_000_000;
        let r2 = r % 60_000_000;
        let sec = r2 / 1_000_000;
        let us = r2 % 1_000_000;
        let sign = if neg { "-" } else { "" };
        s.push('T');
        if h != 0 {
            write!(s, "{sign}{h}H").ok();
        }
        if mi != 0 {
            write!(s, "{sign}{mi}M").ok();
        }
        if us != 0 {
            write!(s, "{sign}{sec}.{us:06}S").ok();
        } else if sec != 0 || (h == 0 && mi == 0) {
            write!(s, "{sign}{sec}S").ok();
        }
    }
    if s == "P" {
        s.push_str("T0S");
    }
    s
}

/// Generic wrapper that reads any Postgres binary value as a UTF-8 string.
pub(super) struct AnyAsString(String);

impl<'a> postgres_types::FromSql<'a> for AnyAsString {
    fn from_sql(
        _ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(AnyAsString(simdutf8::basic::from_utf8(raw)?.to_string()))
    }
    fn accepts(_ty: &postgres_types::Type) -> bool {
        true
    }
}

pub(super) fn rows_to_record_batch_typed(
    schema: &SchemaRef,
    columns: &[(String, Type)],
    rows: &[Row],
    max_value_bytes: Option<usize>,
) -> Result<RecordBatch> {
    if let Some(first) = rows.first() {
        let expected: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
        let wire: Vec<&str> = first.columns().iter().map(|c| c.name()).collect();
        crate::source::verify_wire_columns(&expected, &wire)?;
    }
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(columns.len());
    for (col_idx, (name, pg_type)) in columns.iter().enumerate() {
        let target_type = schema.field(col_idx).data_type();
        let arr = build_array(pg_type, target_type, col_idx, rows, name, max_value_bytes)?;
        if arr.data_type() != target_type {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
                "column '{name}' (PG wire type {pg_type:?}): the value converter produced \
                 {:?} but the resolved column type is {target_type:?} — a column override \
                 retyped it to something the converter cannot build; remove the override \
                 or choose a compatible target type",
                arr.data_type(),
            );
        }
        arrays.push(arr);
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let a =
        crate::source::value_checksum::source_checksums(schema, &PgCellSource { columns, rows });
    let b = crate::source::value_checksum::arrow_batch_checksums(&batch);
    crate::source::value_checksum::verify(&a, &b, schema)?;
    Ok(batch)
}

/// Side A of the Form A value-checksum for Postgres.
struct PgCellSource<'a> {
    columns: &'a [(String, Type)],
    rows: &'a [Row],
}

impl crate::source::value_checksum::CellSource for PgCellSource<'_> {
    fn num_rows(&self) -> usize {
        self.rows.len()
    }
    fn int16(&self, col: usize, row: usize) -> Option<i16> {
        side_a(pg_int_cell(&self.rows[row], col, "int2"))
    }
    fn int32(&self, col: usize, row: usize) -> Option<i32> {
        side_a(pg_int_cell(&self.rows[row], col, "int4"))
    }
    fn int64(&self, col: usize, row: usize) -> Option<i64> {
        side_a(pg_int_cell(&self.rows[row], col, "int8"))
    }
    fn uint64(&self, _col: usize, _row: usize) -> Option<u64> {
        None
    }
    fn float32(&self, col: usize, row: usize) -> Option<f32> {
        side_a(pg_cell(&self.rows[row], col, "float4"))
    }
    fn float64(&self, col: usize, row: usize) -> Option<f64> {
        side_a(pg_cell(&self.rows[row], col, "float8"))
    }
    fn decimal128(&self, col: usize, row: usize, scale: i8) -> Option<i128> {
        let Ok(wire) = self.rows[row].try_get::<_, Option<PgNumericWire<'_>>>(col) else {
            let PgDecimalFallback(t) = self.rows[row]
                .try_get::<_, Option<PgDecimalFallback>>(col)
                .ok()
                .flatten()?;
            return crate::types::decimal::decimal_str_to_scaled_i128(&t, scale);
        };
        let wire = wire?;
        let bd = crate::source::pg_numeric_wire::wire_to_big_decimal(wire.0)?;
        let scaled = bd.with_scale_round(scale as i64, bigdecimal::RoundingMode::Down);
        bigdecimal::num_traits::ToPrimitive::to_i128(&scaled.into_bigint_and_exponent().0)
    }
    fn date32(&self, col: usize, row: usize) -> Option<i32> {
        let d = self.rows[row]
            .try_get::<_, Option<chrono::NaiveDate>>(col)
            .ok()
            .flatten()?;
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch valid");
        Some((d - epoch).num_days() as i32)
    }
    fn ts_micros(&self, col: usize, row: usize) -> Option<i64> {
        if self.columns[col].1 == Type::TIMESTAMPTZ {
            self.rows[row]
                .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(col)
                .ok()
                .flatten()
                .map(|ts| ts.timestamp_micros())
        } else {
            self.rows[row]
                .try_get::<_, Option<chrono::NaiveDateTime>>(col)
                .ok()
                .flatten()
                .map(|ts| ts.and_utc().timestamp_micros())
        }
    }
    fn boolean(&self, col: usize, row: usize) -> Option<bool> {
        side_a(pg_cell(&self.rows[row], col, "bool"))
    }
    fn binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        side_a(pg_cell::<Vec<u8>>(&self.rows[row], col, "bytes")).map(Cow::Owned)
    }
    fn utf8(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        let r = &self.rows[row];
        match self.columns[col].1 {
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
                side_a(pg_cell::<&str>(r, col, "string")).map(|t| Cow::Borrowed(t.as_bytes()))
            }
            Type::JSON | Type::JSONB => match r.try_get::<_, Option<PgJsonRawText<'_>>>(col) {
                Ok(Some(PgJsonRawText(t))) => Some(Cow::Borrowed(t.as_bytes())),
                _ => None,
            },
            Type::NUMERIC => match pg_numeric_optional_utf8_string(r, col) {
                Ok(Some(t)) => Some(Cow::Owned(t.into_bytes())),
                _ => None,
            },
            Type::UUID => match r.try_get::<_, Option<PgUuidBytes>>(col) {
                Ok(Some(PgUuidBytes(b))) => Some(Cow::Owned(
                    uuid::Uuid::from_bytes(b).to_string().into_bytes(),
                )),
                _ => None,
            },
            Type::INTERVAL => side_a(pg_cell::<PgInterval>(r, col, "string")).map(|iv| {
                Cow::Owned(pg_interval_to_iso8601(iv.months, iv.days, iv.microseconds).into_bytes())
            }),
            ref t if matches!(t.kind(), Kind::Enum(_)) => {
                side_a(pg_cell::<AnyAsString>(r, col, "string"))
                    .map(|s| Cow::Owned(s.0.into_bytes()))
            }
            _ => None,
        }
    }
    fn time64_micros(&self, col: usize, row: usize) -> Option<i64> {
        self.rows[row]
            .try_get::<_, Option<chrono::NaiveTime>>(col)
            .ok()
            .flatten()
            .map(naive_time_to_micros)
    }
    fn fixed_binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        match self.rows[row].try_get::<_, Option<PgUuidBytes>>(col) {
            Ok(Some(PgUuidBytes(b))) => Some(Cow::Owned(b.to_vec())),
            _ => None,
        }
    }
    fn decimal256(&self, col: usize, row: usize, scale: i8) -> Option<arrow::datatypes::i256> {
        use crate::types::decimal::decimal_str_to_scaled_i256;
        match pg_numeric_optional_plain(&self.rows[row], col) {
            Ok(Some(t)) => decimal_str_to_scaled_i256(&t, scale),
            _ => None,
        }
    }
    fn list(
        &self,
        col: usize,
        row: usize,
        elem: &DataType,
    ) -> Option<Vec<crate::source::value_checksum::ListElem>> {
        use crate::source::value_checksum::ListElem as E;
        let r = &self.rows[row];
        macro_rules! l {
            ($T:ty, $mk:expr) => {
                r.try_get::<_, Option<Vec<Option<$T>>>>(col)
                    .ok()
                    .flatten()
                    .map(|v| {
                        v.into_iter()
                            .map(|o| o.map($mk).unwrap_or(E::Null))
                            .collect()
                    })
            };
        }
        match elem {
            DataType::Boolean => l!(bool, E::Bool),
            DataType::Int16 => l!(i16, E::I16),
            DataType::Int32 => l!(i32, E::I32),
            DataType::Int64 if is_oid_array(&self.columns[col].1) => {
                l!(PgInt, |v: PgInt| E::I64(v.0))
            }
            DataType::Int64 => l!(i64, E::I64),
            DataType::Float32 => l!(f32, E::F32),
            DataType::Float64 => l!(f64, E::F64),
            DataType::Utf8 => read_utf8_list(&self.columns[col].1, r, col)
                .ok()
                .flatten()
                .map(|v| {
                    v.into_iter()
                        .map(|o| o.map(|s| E::Str(s.into_bytes())).unwrap_or(E::Null))
                        .collect()
                }),
            _ => None,
        }
    }
}

/// Microseconds since midnight, truncating sub-microsecond nanos.
fn naive_time_to_micros(t: chrono::NaiveTime) -> i64 {
    t.num_seconds_from_midnight() as i64 * 1_000_000 + t.nanosecond() as i64 / 1_000
}

fn build_array(
    pg_type: &Type,
    target_type: &DataType,
    col_idx: usize,
    rows: &[Row],
    column: &str,
    max_value_bytes: Option<usize>,
) -> Result<Arc<dyn Array>> {
    match target_type {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                b.append_option(pg_cell::<bool>(row, col_idx, "bool")?);
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(pg_int_cell::<i16>(row, col_idx, "int2")?);
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(pg_int_cell::<i32>(row, col_idx, "int4")?);
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(pg_int_cell::<i64>(row, col_idx, "int8")?);
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(pg_cell::<f32>(row, col_idx, "float4")?);
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(pg_cell::<f64>(row, col_idx, "float8")?);
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Decimal128(p, s) => pg_numeric_to_decimal128(*p, *s, col_idx, rows),
        DataType::Decimal256(p, s) => pg_numeric_to_decimal256(*p, *s, col_idx, rows),
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
            for row in rows {
                match pg_cell::<Vec<u8>>(row, col_idx, "bytes")? {
                    Some(v) => {
                        value_within_ceiling(column, v.len(), max_value_bytes)?;
                        b.append_value(&v);
                    }
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            for row in rows {
                match row
                    .try_get::<_, Option<chrono::NaiveDate>>(col_idx)
                    .map_err(|e| unrepresentable_temporal(column, "DATE", e))?
                {
                    Some(d) => {
                        let epoch =
                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
                        b.append_value((d - epoch).num_days() as i32);
                    }
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Time64(_) => {
            let mut b = Time64MicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row
                    .try_get::<_, Option<PgTimeMicros>>(col_idx)
                    .map_err(|e| unrepresentable_temporal(column, "TIME", e))?
                {
                    Some(PgTimeMicros(us)) => b.append_value(us),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(_, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            if *pg_type == Type::TIMESTAMPTZ {
                for row in rows {
                    match row
                        .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(col_idx)
                        .map_err(|e| unrepresentable_temporal(column, "TIMESTAMPTZ", e))?
                    {
                        Some(ts) => b.append_value(ts.timestamp_micros()),
                        None => b.append_null(),
                    }
                }
            } else {
                for row in rows {
                    match row
                        .try_get::<_, Option<chrono::NaiveDateTime>>(col_idx)
                        .map_err(|e| unrepresentable_temporal(column, "TIMESTAMP", e))?
                    {
                        Some(ts) => b.append_value(ts.and_utc().timestamp_micros()),
                        None => b.append_null(),
                    }
                }
            }
            let arr = b.finish();
            Ok(match tz {
                Some(tz) => Arc::new(arr.with_timezone(tz.as_ref())),
                None => Arc::new(arr),
            })
        }
        DataType::FixedSizeBinary(16) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
            for row in rows {
                match row.try_get::<_, Option<PgUuidBytes>>(col_idx)? {
                    None => b.append_null(),
                    Some(PgUuidBytes(bytes)) => b
                        .append_value(bytes)
                        .expect("16 bytes always matches FixedSizeBinary(16)"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => build_pg_text_array(pg_type, col_idx, rows, column, max_value_bytes),
        DataType::List(_) => build_pg_list_array(pg_type, target_type, col_idx, rows),
        other => crate::rivet_bail!(
            crate::error::codes::INTERNAL_TYPE_BUILDER,
            "no PostgreSQL value converter for target Arrow type {other:?} \
             (wire type {pg_type:?}, column index {col_idx})"
        ),
    }
}

/// Build a `Utf8` array from whichever PostgreSQL wire type the schema resolved to text.
fn build_pg_text_array(
    pg_type: &Type,
    col_idx: usize,
    rows: &[Row],
    column: &str,
    max_value_bytes: Option<usize>,
) -> Result<Arc<dyn Array>> {
    let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
    match *pg_type {
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            for row in rows {
                match pg_cell::<&str>(row, col_idx, "string")? {
                    Some(s) => {
                        value_within_ceiling(column, s.len(), max_value_bytes)?;
                        b.append_value(s);
                    }
                    None => b.append_null(),
                }
            }
        }
        Type::JSON | Type::JSONB => {
            for row in rows {
                match row.try_get::<_, Option<PgJsonRawText<'_>>>(col_idx)? {
                    None => b.append_null(),
                    Some(PgJsonRawText(text)) => {
                        value_within_ceiling(column, text.len(), max_value_bytes)?;
                        b.append_value(text);
                    }
                }
            }
        }
        Type::NUMERIC => {
            for row in rows {
                let val = pg_numeric_optional_utf8_string(row, col_idx)?;
                b.append_option(val.as_deref());
            }
        }
        Type::UUID => {
            for row in rows {
                match row.try_get::<_, Option<PgUuidBytes>>(col_idx)? {
                    None => b.append_null(),
                    Some(PgUuidBytes(bytes)) => {
                        b.append_value(uuid::Uuid::from_bytes(bytes).to_string())
                    }
                }
            }
        }
        Type::INTERVAL => {
            for row in rows {
                match pg_cell::<PgInterval>(row, col_idx, "string")? {
                    Some(iv) => {
                        b.append_value(pg_interval_to_iso8601(iv.months, iv.days, iv.microseconds))
                    }
                    None => b.append_null(),
                }
            }
        }
        _ if matches!(pg_type.kind(), Kind::Enum(_)) => {
            for row in rows {
                match pg_cell::<AnyAsString>(row, col_idx, "string")? {
                    Some(s) => b.append_value(&s.0),
                    None => b.append_null(),
                }
            }
        }
        _ => crate::rivet_bail!(
            crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
            "no text rendering for PostgreSQL wire type {pg_type:?} (column index \
             {col_idx}); a `string` override is supported only for \
             text/json/enum/interval/numeric/uuid columns"
        ),
    }
    Ok(Arc::new(b.finish()))
}

/// A PostgreSQL array cell failed to decode into a 1-D `Vec<Option<T>>`.
fn pg_array_decode_error(rows: &[Row], col_idx: usize, e: &postgres::Error) -> anyhow::Error {
    let col = rows
        .first()
        .and_then(|r| r.columns().get(col_idx))
        .map(|c| c.name())
        .unwrap_or("?");
    crate::error::coded(
        crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
        array_decode_message(col, e),
    )
}

/// The refusal text for an array cell that does not decode.
fn array_decode_message(col: &str, e: &dyn std::fmt::Display) -> String {
    format!(
        "column '{col}': a PostgreSQL array value could not be decoded as a one-dimensional \
         Arrow List ({e}). The usual cause is a MULTI-dimensional (nested) array value — an \
         `integer[]` column may legally hold 2-D+ matrices, but Arrow's List is one-dimensional \
         and has no flat mapping. Cast the column to text in the export query (e.g. `{col}::text`) \
         to export the array literal, or flatten it to a 1-D array. In a date, time or timestamp \
         array an element of `infinity`, `-infinity` or `24:00:00` fails the same way: map it \
         to a real bound in the query."
    )
}

/// The element type of a PostgreSQL array wire type.
fn pg_array_element(pg_type: &Type) -> Option<&Type> {
    match pg_type.kind() {
        Kind::Array(elem) => Some(elem),
        _ => None,
    }
}

/// Whether the wire type is an array of `oid`, read at its unsigned width.
fn is_oid_array(pg_type: &Type) -> bool {
    pg_array_element(pg_type) == Some(&Type::OID)
        && seeded!("pg-array-element-wire-type-refused", false, true)
}

/// A `json` / `jsonb` element read as its raw source text.
struct PgJsonText(String);

impl<'a> PgFromSql<'a> for PgJsonText {
    fn accepts(ty: &Type) -> bool {
        PgJsonRawText::accepts(ty)
    }

    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(PgJsonRawText::from_sql(ty, raw)?.0.to_owned()))
    }
}

/// The elements of an array cell, each turned into text by `read`.
fn text_elements<T>(
    row: &Row,
    col: usize,
    read: impl Fn(T) -> String,
) -> std::result::Result<Option<Vec<Option<String>>>, postgres::Error>
where
    T: for<'a> PgFromSql<'a>,
{
    Ok(row
        .try_get::<_, Option<Vec<Option<T>>>>(col)?
        .map(|v| v.into_iter().map(|e| e.map(&read)).collect()))
}

/// The text of every element of a `text`, `json`, `jsonb`, `interval` or enum array cell.
fn read_utf8_list(
    pg_type: &Type,
    row: &Row,
    col: usize,
) -> std::result::Result<Option<Vec<Option<String>>>, postgres::Error> {
    let element = pg_array_element(pg_type);
    let typed = seeded!("pg-array-element-wire-type-refused", false, true);
    match element {
        Some(e) if typed && (*e == Type::JSON || *e == Type::JSONB) => {
            text_elements::<PgJsonText>(row, col, |v| v.0)
        }
        Some(e) if typed && *e == Type::INTERVAL => text_elements::<PgInterval>(row, col, |v| {
            pg_interval_to_iso8601(v.months, v.days, v.microseconds)
        }),
        Some(e) if typed && matches!(e.kind(), Kind::Enum(_)) => {
            text_elements::<AnyAsString>(row, col, |v| v.0)
        }
        _ => text_elements::<String>(row, col, |v| v),
    }
}

/// Build a `List<Utf8>` from an array of text, json, interval or enum elements.
fn build_pg_utf8_list(pg_type: &Type, col_idx: usize, rows: &[Row]) -> Result<Arc<dyn Array>> {
    let mut lb = ListBuilder::new(StringBuilder::new());
    for row in rows {
        match read_utf8_list(pg_type, row, col_idx) {
            Ok(Some(v)) => {
                for elem in v {
                    lb.values().append_option(elem);
                }
                lb.append(true);
            }
            Ok(None) => lb.append(false),
            Err(e) => return Err(pg_array_decode_error(rows, col_idx, &e)),
        }
    }
    Ok(Arc::new(lb.finish()))
}

/// Build an Arrow `ListArray` from a PostgreSQL array column.
fn build_pg_list_array(
    pg_type: &Type,
    target_type: &DataType,
    col_idx: usize,
    rows: &[Row],
) -> Result<Arc<dyn Array>> {
    let (field_ref, inner_dt) = if let DataType::List(field_ref) = target_type {
        (field_ref, field_ref.data_type())
    } else {
        crate::rivet_bail!(
            crate::error::codes::INTERNAL_TYPE_BUILDER,
            "build_pg_list_array called with non-List target type"
        );
    };

    macro_rules! list_of {
        ($T:ty, $Builder:ty) => {{
            let mut lb = ListBuilder::new(<$Builder>::new());
            for row in rows {
                match row.try_get::<_, Option<Vec<Option<$T>>>>(col_idx) {
                    Ok(Some(v)) => {
                        for x in &v {
                            match x {
                                Some(val) => lb.values().append_value(*val),
                                None => lb.values().append_null(),
                            }
                        }
                        lb.append(true);
                    }
                    Ok(None) => lb.append(false),
                    Err(e) => return Err(pg_array_decode_error(rows, col_idx, &e)),
                }
            }
            Ok(Arc::new(lb.finish()))
        }};
    }

    macro_rules! list_mapped {
        ($T:ty, $builder:expr, $b:ident, $x:ident => $push:expr) => {{
            let mut lb = ListBuilder::new($builder).with_field(field_ref.clone());
            for row in rows {
                match row.try_get::<_, Option<Vec<Option<$T>>>>(col_idx) {
                    Ok(Some(v)) => {
                        for elem in v {
                            match elem {
                                Some($x) => {
                                    let $b = lb.values();
                                    $push;
                                }
                                None => lb.values().append_null(),
                            }
                        }
                        lb.append(true);
                    }
                    Ok(None) => lb.append(false),
                    Err(e) => return Err(pg_array_decode_error(rows, col_idx, &e)),
                }
            }
            Ok(Arc::new(lb.finish()))
        }};
    }

    match inner_dt {
        DataType::Boolean => list_of!(bool, BooleanBuilder),
        DataType::Int16 => list_of!(i16, Int16Builder),
        DataType::Int32 => list_of!(i32, Int32Builder),
        DataType::Int64 if is_oid_array(pg_type) => {
            list_mapped!(PgInt, Int64Builder::new(), b, v => b.append_value(v.0))
        }
        DataType::Int64 => list_of!(i64, Int64Builder),
        DataType::Float32 => list_of!(f32, Float32Builder),
        DataType::Float64 => list_of!(f64, Float64Builder),
        DataType::Utf8 => build_pg_utf8_list(pg_type, col_idx, rows),
        DataType::Date32 if seeded!("pg-temporal-uuid-bytea-array-refused", false, true) => {
            list_mapped!(
                chrono::NaiveDate,
                Date32Builder::new(),
                b,
                d => {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
                    b.append_value((d - epoch).num_days() as i32)
                }
            )
        }
        DataType::Time64(_) if seeded!("pg-temporal-uuid-bytea-array-refused", false, true) => {
            list_mapped!(PgTimeMicros, Time64MicrosecondBuilder::new(), b, t => b.append_value(t.0))
        }
        DataType::Timestamp(_, Some(tz))
            if seeded!("pg-temporal-uuid-bytea-array-refused", false, true) =>
        {
            list_mapped!(
                chrono::DateTime<chrono::Utc>,
                TimestampMicrosecondBuilder::new().with_timezone(tz.as_ref()),
                b,
                ts => b.append_value(ts.timestamp_micros())
            )
        }
        DataType::Timestamp(_, None)
            if seeded!("pg-temporal-uuid-bytea-array-refused", false, true) =>
        {
            list_mapped!(
                chrono::NaiveDateTime,
                TimestampMicrosecondBuilder::new(),
                b,
                ts => b.append_value(ts.and_utc().timestamp_micros())
            )
        }
        DataType::FixedSizeBinary(16)
            if seeded!("pg-temporal-uuid-bytea-array-refused", false, true) =>
        {
            list_mapped!(
                PgUuidBytes,
                FixedSizeBinaryBuilder::new(16),
                b,
                u => b.append_value(u.0).expect("a uuid is 16 bytes")
            )
        }
        DataType::Binary if seeded!("pg-temporal-uuid-bytea-array-refused", false, true) => {
            list_mapped!(Vec<u8>, BinaryBuilder::new(), b, v => b.append_value(v))
        }
        other => {
            let col = rows
                .first()
                .and_then(|r| r.columns().get(col_idx))
                .map_or("?", |c| c.name());
            crate::rivet_bail!(
                crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                "column '{col}': PG array column has element type {other:?} which the list \
                 builder cannot decode — it would export as 100% NULL. Cast the column to \
                 text[] in the source query, or exclude it."
            )
        }
    }
}

/// Decode a single `NUMERIC` cell to a scaled `i128` for Arrow `Decimal128`.
fn pg_numeric_optional_plain(row: &Row, col_idx: usize) -> Result<Option<String>> {
    match row.try_get::<_, Option<PgNumericWire<'_>>>(col_idx) {
        Ok(Some(wire)) => match numeric_wire_normalized_plain(wire.0) {
            Some(plain) => {
                let t = plain.trim();
                Ok((!t.is_empty()).then(|| t.to_string()))
            }
            None => Err(crate::error::coded(
                crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                format!(
                    "PostgreSQL NUMERIC: unsupported NaN/infinity payload (column idx {col_idx})",
                ),
            )),
        },
        Ok(None) => Ok(None),
        Err(_) => match row.try_get::<_, Option<PgDecimalFallback>>(col_idx) {
            Ok(v) => Ok(v.and_then(|PgDecimalFallback(t)| (!t.is_empty()).then_some(t))),
            Err(_) => {
                let c = &row.columns()[col_idx];
                crate::rivet_bail!(
                    crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
                    "postgres: column `{}` is declared decimal by a `columns:` override but \
                     PostgreSQL sends it as {} — rivet does not convert it, and writing it as \
                     NULL would lose every value. Remove the override, or CAST the column to \
                     numeric in the export's `query:`.",
                    c.name(),
                    c.type_()
                )
            }
        },
    }
}

/// A non-`numeric` cell read under a `decimal` override: an integer or text, as plain text.
struct PgDecimalFallback(String);

impl<'a> PgFromSql<'a> for PgDecimalFallback {
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INT2 | Type::INT4 | Type::INT8) || <&str as PgFromSql>::accepts(ty)
    }

    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(match *ty {
            Type::INT2 => i16::from_sql(ty, raw)?.to_string(),
            Type::INT4 => i32::from_sql(ty, raw)?.to_string(),
            Type::INT8 => i64::from_sql(ty, raw)?.to_string(),
            _ => <&str as PgFromSql>::from_sql(ty, raw)?.trim().to_string(),
        }))
    }
}

fn pg_numeric_optional_scaled_i128(row: &Row, col_idx: usize, scale: i8) -> Result<Option<i128>> {
    match pg_numeric_optional_plain(row, col_idx)? {
        Some(t) => crate::types::decimal::decimal_str_to_scaled_i128(&t, scale)
            .map(Some)
            .ok_or_else(|| {
                crate::error::coded(
                    crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                    format!("cannot parse DECIMAL {t:?} as decimal(scale={scale})"),
                )
            }),
        None => Ok(None),
    }
}

/// Build a `Decimal128Array` from a PostgreSQL `NUMERIC` column.
fn pg_numeric_to_decimal128(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[Row],
) -> Result<Arc<dyn Array>> {
    let mut b = Decimal128Builder::with_capacity(rows.len());
    for row in rows {
        match pg_numeric_optional_scaled_i128(row, col_idx, scale)? {
            Some(v) => b.append_value(v),
            None => b.append_null(),
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}

/// Build a `Decimal256Array` for precision > 38 (roadmap §12).
fn pg_numeric_to_decimal256(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[Row],
) -> Result<Arc<dyn Array>> {
    use crate::types::decimal::decimal_str_to_scaled_i256;
    let mut b = Decimal256Builder::with_capacity(rows.len());
    for row in rows {
        match pg_numeric_optional_plain(row, col_idx)? {
            Some(t) => {
                let v = decimal_str_to_scaled_i256(&t, scale).ok_or_else(|| {
                    crate::error::coded(
                        crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                        format!("cannot parse DECIMAL {t:?} as decimal({precision},{scale})"),
                    )
                })?;
                b.append_value(v);
            }
            None => b.append_null(),
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}

#[cfg(test)]
mod interval_render_tests {
    use super::pg_interval_to_iso8601;

    /// `pg_interval_to_iso8601` is a pure renderer with seven arithmetic steps and, until now, no direct test.
    #[test]
    fn interval_render_pins_every_arithmetic_step() {
        assert_eq!(
            pg_interval_to_iso8601(25, 5, 3_723_456_789),
            "P2Y1M5DT1H2M3.456789S"
        );
        assert_eq!(
            pg_interval_to_iso8601(25, 5, -3_723_456_789),
            "P2Y1M5DT-1H-2M-3.456789S"
        );
        assert_eq!(pg_interval_to_iso8601(0, 0, 456_789), "PT0.456789S");
        assert_eq!(pg_interval_to_iso8601(0, 0, 3_000_000), "PT3S");
        assert_eq!(pg_interval_to_iso8601(0, 0, 3_600_000_000), "PT1H");
        assert_eq!(pg_interval_to_iso8601(0, 0, 0), "PT0S");
        assert_eq!(pg_interval_to_iso8601(24, 0, 0), "P2Y");
    }
}

#[cfg(test)]
mod decimal_override_tests {
    use super::PgDecimalFallback;
    use postgres::types::{FromSql, Type};

    /// A `decimal` override on a PG integer column reads the integer exactly.
    #[test]
    fn an_integer_column_under_a_decimal_override_reads_its_value() {
        for ty in [
            Type::INT2,
            Type::INT4,
            Type::INT8,
            Type::TEXT,
            Type::VARCHAR,
        ] {
            assert!(PgDecimalFallback::accepts(&ty), "{ty}");
        }
        let v = |ty: &Type, raw: &[u8]| PgDecimalFallback::from_sql(ty, raw).unwrap().0;
        assert_eq!(v(&Type::INT2, &(-7i16).to_be_bytes()), "-7");
        assert_eq!(v(&Type::INT4, &123_456i32.to_be_bytes()), "123456");
        assert_eq!(
            v(&Type::INT8, &i64::MAX.to_be_bytes()),
            "9223372036854775807"
        );
        assert_eq!(v(&Type::TEXT, b" 12.50 "), "12.50");
    }

    /// A wire type rivet does not convert (float, bool) is refused, never nulled.
    #[test]
    fn a_float_column_under_a_decimal_override_is_not_accepted() {
        for ty in [Type::FLOAT4, Type::FLOAT8, Type::BOOL, Type::DATE] {
            assert!(!PgDecimalFallback::accepts(&ty), "{ty}");
        }
    }
}

#[cfg(test)]
mod cell_refusal_tests {
    use super::{narrow_int, pg_cell_error};
    use crate::error::CodedError;
    use postgres::types::{Type, WrongType};

    fn code(e: &anyhow::Error) -> &'static str {
        e.downcast_ref::<CodedError>().expect("coded").code()
    }

    /// Exact fits pass through; one past either bound, and an OID above i32::MAX, are refused.
    #[test]
    fn narrow_int_keeps_exact_fits_and_refuses_one_past_the_bound() {
        assert_eq!(
            narrow_int::<i16>(-32_768, "c", "int4", "int2").unwrap(),
            i16::MIN
        );
        assert_eq!(
            narrow_int::<i16>(32_767, "c", "int4", "int2").unwrap(),
            i16::MAX
        );
        assert_eq!(
            narrow_int::<i32>(2_147_483_647, "c", "oid", "int4").unwrap(),
            i32::MAX
        );
        assert_eq!(
            narrow_int::<i64>(4_000_000_000, "c", "oid", "int8").unwrap(),
            4_000_000_000
        );
        for (v, wire) in [(-32_769i64, "int4"), (32_768, "int4")] {
            let e = narrow_int::<i16>(v, "qty", wire, "int2").unwrap_err();
            assert_eq!(code(&e), "RIVET_SOURCE_VALUE_UNREPRESENTABLE");
            assert!(
                e.to_string().contains(&format!("`qty` ({wire}) holds {v}")),
                "{e}"
            );
        }
        let e = narrow_int::<i32>(4_000_000_000, "o", "oid", "int4").unwrap_err();
        assert!(
            e.to_string()
                .contains("holds 4000000000, which does not fit the int4"),
            "{e}"
        );
    }

    /// Invalid UTF-8 from either decoder is a named value refusal with the bytea remedy.
    #[test]
    fn invalid_utf8_is_refused_by_name_not_as_an_override() {
        let bytes: &[u8] = b"caf\xe9";
        let std_err = std::str::from_utf8(std::hint::black_box(bytes)).unwrap_err();
        let simd_err = simdutf8::basic::from_utf8(bytes).unwrap_err();
        let causes: [&(dyn std::error::Error + 'static); 2] = [&std_err, &simd_err];
        for cause in causes {
            let e = pg_cell_error("name", &Type::TEXT, "string", cause);
            assert_eq!(code(&e), "RIVET_SOURCE_VALUE_UNREPRESENTABLE");
            let m = e.to_string();
            assert!(
                m.contains("column `name` (text) holds a value that is not valid UTF-8"),
                "{m}"
            );
            assert!(
                m.contains("`name::bytea`") && !m.contains("override"),
                "{m}"
            );
        }
    }

    /// A malformed payload is a value refusal; only a rejected wire type blames the override.
    #[test]
    fn a_malformed_payload_is_not_reported_as_an_override_mismatch() {
        let bad: Box<dyn std::error::Error + Sync + Send> =
            "expected 16-byte interval, got 3".into();
        let e = pg_cell_error("iv", &Type::INTERVAL, "string", &*bad);
        assert_eq!(code(&e), "RIVET_SOURCE_VALUE_UNREPRESENTABLE");
        assert!(
            e.to_string()
                .contains("`iv` (interval) sent a malformed wire payload"),
            "{e}"
        );
        assert!(!e.to_string().contains("override"), "{e}");

        let wrong = WrongType::new::<i32>(Type::TEXT);
        let e = pg_cell_error("n", &Type::TEXT, "int4", &wrong);
        assert_eq!(code(&e), "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH");
        assert!(
            e.to_string()
                .contains("`n` is declared int4 by a `columns:` override"),
            "{e}"
        );
    }
}

#[cfg(test)]
mod temporal_refusal_tests {
    use super::{PgInt, PgTimeMicros};
    use crate::types::MICROS_PER_DAY;
    use postgres::types::{FromSql, Type};

    /// PostgreSQL's legal `time '24:00:00'` is refused, not wrapped to midnight.
    #[test]
    fn a_time_of_24_00_is_refused_not_wrapped_to_midnight() {
        let raw = 86_400_000_000i64.to_be_bytes();
        let wrapped = <chrono::NaiveTime as FromSql>::from_sql(&Type::TIME, &raw).unwrap();
        assert_eq!(
            wrapped,
            chrono::NaiveTime::MIN,
            "driver wraps 24:00 to midnight"
        );
        assert!(PgTimeMicros::from_sql(&Type::TIME, &raw).is_err());
        assert!(PgTimeMicros::from_sql(&Type::TIME, &(-1i64).to_be_bytes()).is_err());
        let last = MICROS_PER_DAY - 1;
        let got = PgTimeMicros::from_sql(&Type::TIME, &last.to_be_bytes()).unwrap();
        assert_eq!(got.0, 86_399_999_999);
        let got = PgTimeMicros::from_sql(&Type::TIME, &3_723_456_789i64.to_be_bytes()).unwrap();
        assert_eq!(got.0, 3_723_456_789);
    }

    /// No temporal column is read with the PANICKING `Row::get`.
    #[test]
    fn no_temporal_column_is_read_with_panicking_get() {
        let src = include_str!("arrow_convert.rs");
        let product = &src[..src.find("#[cfg(test)]").unwrap()];
        for needle in [
            [".get::<_, Option<", "chrono::"].concat(),
            [".get::<_, Option<", "PgTimeMicros"].concat(),
        ] {
            assert!(
                !product.contains(&needle),
                "a temporal read goes through Row::get, which panics on `infinity`: {needle}"
            );
        }
    }

    /// Every integer wire width decodes at its own width and widens exactly to i64.
    #[test]
    fn an_integer_cell_is_decoded_at_its_wire_width_and_widened() {
        let dec = |ty: &Type, raw: &[u8]| PgInt::from_sql(ty, raw).unwrap().0;
        assert_eq!(dec(&Type::INT2, &(-12_345i16).to_be_bytes()), -12_345);
        assert_eq!(
            dec(&Type::INT4, &(-2_000_000_001i32).to_be_bytes()),
            -2_000_000_001
        );
        assert_eq!(
            dec(&Type::INT8, &(-9_000_000_000_123i64).to_be_bytes()),
            -9_000_000_000_123
        );
        assert_eq!(
            dec(&Type::OID, &4_000_000_000u32.to_be_bytes()),
            4_000_000_000
        );
        assert!(PgInt::from_sql(&Type::INT8, &7i32.to_be_bytes()).is_err());
        assert!(!<PgInt as FromSql>::accepts(&Type::BOOL));
        for ty in [Type::INT2, Type::INT4, Type::INT8, Type::OID] {
            assert!(<PgInt as FromSql>::accepts(&ty), "{ty}");
        }
    }

    /// Side A keeps a decoded value and turns only a decode error into a missing cell.
    #[test]
    fn side_a_keeps_the_value_and_drops_only_an_error() {
        use super::side_a;
        assert_eq!(side_a(Ok(Some(7i16))), Some(7));
        assert_eq!(side_a::<i16>(Ok(None)), None);
        assert_eq!(side_a::<i16>(Err(anyhow::anyhow!("x"))), None);
    }

    /// No cell is read with the panicking `Row::get`; the only `.get(` calls are on non-Row receivers.
    #[test]
    fn no_scalar_column_is_read_with_panicking_get() {
        let src = include_str!("arrow_convert.rs");
        let product = &src[..src.find("#[cfg(test)]").unwrap()];
        let panicking: Vec<&str> = product
            .lines()
            .filter(|l| l.contains(".get(") || l.contains(".get::<"))
            .filter(|l| !l.contains("columns().get(") && !l.contains("h.get(name)"))
            .collect();
        assert!(
            panicking.is_empty(),
            "Row::get panics on an override: {panicking:?}"
        );
    }
}

#[cfg(test)]
mod time_arithmetic_tests {
    use super::naive_time_to_micros;
    use chrono::NaiveTime;

    /// Pins every arithmetic step of the `time` conversion.
    #[test]
    fn naive_time_to_micros_pins_every_arithmetic_step() {
        let t = NaiveTime::from_hms_nano_opt(1, 2, 3, 456_789_000).unwrap();
        assert_eq!(naive_time_to_micros(t), 3723 * 1_000_000 + 456_789);

        let t = NaiveTime::from_hms_nano_opt(0, 0, 1, 999).unwrap();
        assert_eq!(naive_time_to_micros(t), 1_000_000);

        let t = NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_000).unwrap();
        assert_eq!(naive_time_to_micros(t), 86_399 * 1_000_000 + 999_999);
    }
}

#[cfg(test)]
mod type_table;

#[cfg(test)]
mod type_map_tests {
    use super::{bare_numeric, pg_column_rivet_type, pg_common_kind, pg_type_to_rivet};
    use crate::types::kind::CommonKind;
    use crate::types::{RivetType, TimeUnit as RivetTimeUnit};
    use postgres::types::Type;
    use std::collections::HashMap;

    /// Every arm of `pg_type_to_rivet`, in one table.
    #[test]
    fn every_pg_type_maps_to_its_declared_rivet_type() {
        let cases: &[(Type, RivetType)] = &[
            (Type::BOOL, RivetType::Bool),
            (Type::INT2, RivetType::Int16),
            (Type::INT4, RivetType::Int32),
            (Type::INT8, RivetType::Int64),
            (Type::OID, RivetType::Int64),
            (Type::FLOAT4, RivetType::Float32),
            (Type::FLOAT8, RivetType::Float64),
            (Type::DATE, RivetType::Date),
            (
                Type::TIME,
                RivetType::Time {
                    unit: RivetTimeUnit::Microsecond,
                },
            ),
            (Type::TEXT, RivetType::String),
            (Type::VARCHAR, RivetType::String),
            (Type::BPCHAR, RivetType::String),
            (Type::NAME, RivetType::String),
            (Type::BYTEA, RivetType::Binary),
            (Type::JSON, RivetType::Json),
            (Type::JSONB, RivetType::Json),
            (Type::UUID, RivetType::Uuid),
            (Type::INTERVAL, RivetType::Interval),
        ];
        for (pg, want) in cases {
            let got = pg_type_to_rivet(pg);
            assert_eq!(
                &got, want,
                "postgres type {pg:?} must map to {want:?}, got {got:?} — a dropped \
                 arm falls through to Unsupported and silently changes the schema"
            );
        }
    }

    /// Every wire type the table reads as a common kind, and the engine-only ones it leaves out.
    #[test]
    fn the_common_wire_types_read_as_their_common_kind_and_the_rest_as_none() {
        use crate::types::kind::CommonKind::*;
        let cases: &[(Type, Option<CommonKind>)] = &[
            (Type::BOOL, Some(Bool)),
            (Type::INT2, Some(I16)),
            (Type::INT4, Some(I32)),
            (Type::INT8, Some(I64)),
            (Type::FLOAT4, Some(F32)),
            (Type::FLOAT8, Some(F64)),
            (Type::NUMERIC, Some(Dec)),
            (Type::DATE, Some(Date)),
            (Type::TIMESTAMP, Some(Naive)),
            (Type::TIMESTAMPTZ, Some(Utc)),
            (Type::TEXT, Some(Text)),
            (Type::VARCHAR, Some(Text)),
            (Type::BPCHAR, Some(Text)),
            (Type::NAME, Some(Text)),
            (Type::BYTEA, Some(Binary)),
            (Type::JSON, Some(Json)),
            (Type::JSONB, Some(Json)),
            (Type::OID, None),
            (Type::TIME, None),
            (Type::UUID, None),
            (Type::INTERVAL, None),
            (Type::INT4_ARRAY, None),
            (Type::MONEY, None),
        ];
        for (pg, want) in cases {
            assert_eq!(&pg_common_kind(pg), want, "{pg:?}");
        }
    }

    /// A catalog hint types a `numeric` column of that name and nothing else.
    #[test]
    fn a_numeric_hint_types_only_the_numeric_column_it_names() {
        let hints = HashMap::from([("amount".to_string(), (18u8, 2i8))]);
        let decimal = RivetType::Decimal {
            precision: 18,
            scale: 2,
        };
        assert_eq!(
            pg_column_rivet_type(&Type::NUMERIC, "amount", Some(&hints)),
            decimal
        );
        for (t, name, hints) in [
            (&Type::NUMERIC, "other", Some(&hints)),
            (&Type::NUMERIC, "amount", None),
        ] {
            assert_eq!(pg_column_rivet_type(t, name, hints), bare_numeric());
        }
        assert_eq!(
            pg_column_rivet_type(&Type::INT8, "amount", Some(&hints)),
            RivetType::Int64
        );
        assert_eq!(
            pg_column_rivet_type(&Type::FLOAT8, "amount", Some(&hints)),
            RivetType::Float64
        );
    }

    /// The two timestamp arms differ ONLY in the timezone field, which is the whole point (roadmap §13.
    #[test]
    fn timestamptz_carries_utc_and_timestamp_does_not() {
        match pg_type_to_rivet(&Type::TIMESTAMP) {
            RivetType::Timestamp { timezone, .. } => {
                assert_eq!(timezone, None, "naive TIMESTAMP must carry NO timezone")
            }
            other => panic!("TIMESTAMP mapped to {other:?}"),
        }
        match pg_type_to_rivet(&Type::TIMESTAMPTZ) {
            RivetType::Timestamp { timezone, .. } => assert_eq!(
                timezone.as_deref(),
                Some("UTC"),
                "TIMESTAMPTZ must carry UTC, or the instant loses its meaning downstream"
            ),
            other => panic!("TIMESTAMPTZ mapped to {other:?}"),
        }
    }

    /// NUMERIC without declared precision is Unsupported ON PURPOSE, and the reason must stay actionable.
    #[test]
    fn bare_numeric_is_unsupported_with_an_actionable_reason() {
        match pg_type_to_rivet(&Type::NUMERIC) {
            RivetType::Unsupported { reason, .. } => {
                for needle in ["override", "decimal("] {
                    assert!(
                        reason.contains(needle),
                        "the NUMERIC reason must mention {needle:?}, got: {reason}"
                    );
                }
            }
            other => panic!("bare NUMERIC mapped to {other:?} — it has no precision to use"),
        }
    }
}

#[cfg(test)]
mod numeric_string_path_tests {
    use super::numeric_raw_to_optional_decimal_text;

    /// PostgreSQL `numeric` binary header, built from the wire spec by hand.
    fn wire(sign: u16) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&0u16.to_be_bytes());
        v.extend_from_slice(&0i16.to_be_bytes());
        v.extend_from_slice(&sign.to_be_bytes());
        v.extend_from_slice(&0u16.to_be_bytes());
        v
    }

    /// A `columns: { c: string }` override on a PG `numeric` must carry the three non-finite values as text, not degrade them to NULL.
    #[test]
    fn a_string_override_carries_nan_and_infinity_instead_of_nulling_them() {
        for (sign, expected) in [
            (0xC000u16, "NaN"),
            (0xD000, "Infinity"),
            (0xF000, "-Infinity"),
        ] {
            assert_eq!(
                numeric_raw_to_optional_decimal_text(&wire(sign)).as_deref(),
                Some(expected),
                "sign field {sign:#06x} must render as {expected}, not degrade to NULL — \
                 a string column holds it losslessly"
            );
        }
    }

    /// The finite path is untouched, and a genuinely undecodable payload still yields None rather than a bogus string.
    #[test]
    fn finite_values_are_unaffected_and_garbage_still_yields_none() {
        let mut finite = Vec::new();
        finite.extend_from_slice(&1u16.to_be_bytes());
        finite.extend_from_slice(&0i16.to_be_bytes());
        finite.extend_from_slice(&0u16.to_be_bytes());
        finite.extend_from_slice(&0u16.to_be_bytes());
        finite.extend_from_slice(&1u16.to_be_bytes());
        assert_eq!(
            numeric_raw_to_optional_decimal_text(&finite).as_deref(),
            Some("1"),
            "finite decoding must not regress"
        );

        assert_eq!(
            numeric_raw_to_optional_decimal_text(&[]).as_deref(),
            None,
            "an empty payload has no text form"
        );
    }
}

#[cfg(test)]
mod renderer_twins {
    use super::pg_interval_to_iso8601;
    use crate::types::{iso8601_duration, uuid36};

    /// Inputs on which the PG interval renderer and the canonical one agree.
    const AGREE: &[(i32, i32, i64)] = &[
        (0, 0, 0),
        (14, 3, 14_706_000_001),
        (-14, 0, 0),
        (12, 0, 0),
        (-1, 5, 0),
        (0, -1, 3_600_000_000),
        (0, 0, -14_706_000_000),
        (0, 0, 1),
        (0, 0, 456_789),
        (0, 0, 60_000_000),
        (0, 0, 90_000_000_000),
        (i32::MAX, i32::MAX, i64::MAX),
        (i32::MIN, i32::MIN, i64::MIN),
    ];

    #[test]
    fn pg_interval_matches_the_canonical_duration() {
        for &(m, d, us) in AGREE {
            assert_eq!(
                pg_interval_to_iso8601(m, d, us),
                iso8601_duration(m, d, us),
                "({m}, {d}, {us})"
            );
        }
    }

    #[test]
    /// Strict known divergence: ADR-0038 divergence.
    #[should_panic(expected = "assertion")]
    fn pg_interval_fraction_matches_the_canonical_duration() {
        for &(m, d, us) in &[(0, 0, 500_000), (0, 0, -1_500_000), (14, 3, 14_706_789_000)] {
            assert_eq!(
                pg_interval_to_iso8601(m, d, us),
                iso8601_duration(m, d, us),
                "({m}, {d}, {us})"
            );
        }
    }

    #[test]
    fn pg_uuid_text_matches_uuid36() {
        let mixed = [
            0x12, 0x3E, 0x45, 0x67, 0xE8, 0x9B, 0x12, 0xD3, 0xA4, 0x56, 0x42, 0x66, 0x14, 0x17,
            0x40, 0x00,
        ];
        for b in [[0u8; 16], [0xFF; 16], mixed] {
            assert_eq!(uuid::Uuid::from_bytes(b).to_string(), uuid36(&b));
        }
    }
}

#[cfg(test)]
mod array_decode_message_text {
    use super::array_decode_message;

    /// The refusal names the column, the nested-array cause, the cast remedy and the temporal bounds remedy.
    #[test]
    fn the_refusal_text_names_cause_and_remedy() {
        let m = array_decode_message("tags", &"wire error");
        assert!(m.starts_with("column 'tags': a PostgreSQL array value could not be decoded"));
        assert!(m.contains("(wire error)"));
        assert!(m.contains("MULTI-dimensional"));
        assert!(m.contains("`tags::text`"));
        assert!(m.contains(
            "In a date, time or timestamp array an element of `infinity`, `-infinity` or `24:00:00` fails the same way: map it to a real bound in the query."
        ));
    }
}

#[cfg(test)]
mod array_element_tests {
    use super::*;

    #[test]
    fn an_array_type_names_its_element_and_a_scalar_names_none() {
        assert_eq!(pg_array_element(&Type::JSON_ARRAY), Some(&Type::JSON));
        assert_eq!(pg_array_element(&Type::JSONB_ARRAY), Some(&Type::JSONB));
        assert_eq!(
            pg_array_element(&Type::INTERVAL_ARRAY),
            Some(&Type::INTERVAL)
        );
        assert_eq!(pg_array_element(&Type::OID_ARRAY), Some(&Type::OID));
        assert_eq!(pg_array_element(&Type::INT4_ARRAY), Some(&Type::INT4));
        assert_eq!(pg_array_element(&Type::TEXT_ARRAY), Some(&Type::TEXT));
        assert_eq!(pg_array_element(&Type::JSONB), None);
        assert_eq!(pg_array_element(&Type::OID), None);
    }

    #[test]
    fn a_json_element_reads_its_own_text() {
        let json = PgJsonText::from_sql(&Type::JSON, br#"{"a": 1}"#).unwrap();
        assert_eq!(json.0, r#"{"a": 1}"#);
        let mut jsonb = vec![1u8];
        jsonb.extend_from_slice(br#"{"a": 1}"#);
        let jsonb = PgJsonText::from_sql(&Type::JSONB, &jsonb).unwrap();
        assert_eq!(jsonb.0, r#"{"a": 1}"#);
    }

    #[test]
    fn a_jsonb_element_of_another_wire_version_or_none_is_refused() {
        assert!(PgJsonText::from_sql(&Type::JSONB, &[2, b'1']).is_err());
        assert!(PgJsonText::from_sql(&Type::JSONB, &[0, b'1']).is_err());
        assert!(PgJsonText::from_sql(&Type::JSONB, &[]).is_err());
    }

    #[test]
    fn only_json_and_jsonb_elements_are_read_as_json_text() {
        assert!(PgJsonText::accepts(&Type::JSON));
        assert!(PgJsonText::accepts(&Type::JSONB));
        assert!(!PgJsonText::accepts(&Type::TEXT));
        assert!(!PgJsonText::accepts(&Type::JSON_ARRAY));
    }

    #[test]
    fn only_an_oid_array_is_read_at_its_unsigned_width() {
        assert!(is_oid_array(&Type::OID_ARRAY));
        assert!(!is_oid_array(&Type::INT8_ARRAY));
        assert!(!is_oid_array(&Type::INT4_ARRAY));
        assert!(!is_oid_array(&Type::OID));
    }

    #[test]
    fn an_oid_element_keeps_its_unsigned_range() {
        let top = PgInt::from_sql(&Type::OID, &u32::MAX.to_be_bytes()).unwrap();
        assert_eq!(top.0, 4_294_967_295);
        let zero = PgInt::from_sql(&Type::OID, &0u32.to_be_bytes()).unwrap();
        assert_eq!(zero.0, 0);
        assert!(PgInt::from_sql(&Type::OID, &[0, 0, 1]).is_err());
    }
}
