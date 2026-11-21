//! MySQL → Arrow conversion machinery — twin of `postgres/arrow_convert.rs`.

use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use mysql::Value;
use mysql::consts::{ColumnFlags, ColumnType};

use crate::error::Result;
use crate::types::kind::CommonKind;
use crate::types::{
    ColumnOverrides, RivetType, SourceColumn, TypeFidelity, TypeMapping, build_arrow_field,
};

use crate::source::value_within_ceiling;

pub(super) fn mysql_native_type_name(col: &mysql::Column) -> String {
    use mysql::consts::ColumnType::*;
    let unsigned = col.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    let int_name = |base: &str| -> String {
        if unsigned {
            format!("{base} unsigned")
        } else {
            base.into()
        }
    };
    match col.column_type() {
        MYSQL_TYPE_TINY if col.column_length() == 1 => "tinyint(1)".into(),
        MYSQL_TYPE_TINY => int_name("tinyint"),
        MYSQL_TYPE_SHORT => int_name("smallint"),
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => int_name("int"),
        MYSQL_TYPE_LONGLONG => int_name("bigint"),
        MYSQL_TYPE_FLOAT => "float".into(),
        MYSQL_TYPE_DOUBLE => "double".into(),
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => "decimal".into(),
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING
            if col.flags().contains(ColumnFlags::ENUM_FLAG) =>
        {
            "enum".into()
        }
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING
            if col.flags().contains(ColumnFlags::SET_FLAG) =>
        {
            "set".into()
        }
        MYSQL_TYPE_STRING if col.character_set() == 63 => "binary".into(),
        MYSQL_TYPE_VAR_STRING if col.character_set() == 63 => "varbinary".into(),
        MYSQL_TYPE_STRING => "char".into(),
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => "varchar".into(),
        MYSQL_TYPE_ENUM => "enum".into(),
        MYSQL_TYPE_SET => "set".into(),
        MYSQL_TYPE_JSON => "json".into(),
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB => {
            "blob".into()
        }
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => "date".into(),
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => "time".into(),
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => "datetime".into(),
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => "timestamp".into(),
        MYSQL_TYPE_BIT if col.column_length() == 1 => "bit(1)".into(),
        MYSQL_TYPE_BIT => "bit".into(),
        MYSQL_TYPE_YEAR => "year".into(),
        _ => "unknown".into(),
    }
}

/// Derive DECIMAL `(precision, scale)` from a MySQL wire column definition.
fn derive_decimal_ps(column_length: u32, scale: u8, signed: bool) -> Option<(u8, i8)> {
    let point = u32::from(scale > 0);
    let sign = u32::from(signed);
    let precision = column_length.checked_sub(point + sign)?;
    if !(1..=65).contains(&precision) {
        return None;
    }
    Some((u8::try_from(precision).ok()?, i8::try_from(scale).ok()?))
}

/// The widest `BIT(n)` whose every value fits a signed 64-bit integer; `BIT(64)` is delivered unsigned.
const BIT_INT64_MAX_WIDTH: u32 = 63;

/// The common kind a MySQL column is read as, or `None` for a type this engine alone carries.
fn mysql_common_kind(col: &mysql::Column) -> Option<CommonKind> {
    use mysql::consts::ColumnType::*;
    let unsigned = col.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    Some(match col.column_type() {
        MYSQL_TYPE_TINY if col.column_length() == 1 => CommonKind::Bool,
        MYSQL_TYPE_TINY => CommonKind::I16,
        MYSQL_TYPE_SHORT if unsigned => CommonKind::I32,
        MYSQL_TYPE_SHORT => CommonKind::I16,
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG if unsigned => CommonKind::I64,
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => CommonKind::I32,
        MYSQL_TYPE_LONGLONG if !unsigned => CommonKind::I64,
        MYSQL_TYPE_FLOAT => CommonKind::F32,
        MYSQL_TYPE_DOUBLE => CommonKind::F64,
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => CommonKind::Dec,
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING
            if col.flags().contains(ColumnFlags::ENUM_FLAG)
                || col.flags().contains(ColumnFlags::SET_FLAG) =>
        {
            return None;
        }
        MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_STRING
        | MYSQL_TYPE_TINY_BLOB
        | MYSQL_TYPE_MEDIUM_BLOB
        | MYSQL_TYPE_LONG_BLOB
        | MYSQL_TYPE_BLOB => {
            if col.character_set() == 63 {
                CommonKind::Binary
            } else {
                CommonKind::Text
            }
        }
        MYSQL_TYPE_JSON => CommonKind::Json,
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => CommonKind::Date,
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => CommonKind::Naive,
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => CommonKind::Utc,
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => CommonKind::Time,
        _ => return None,
    })
}

/// Whether the wire type is one of the four `*BLOB` types, the ones a text column of this engine is delivered as `Text` on.
fn is_blob_wire(col: &mysql::Column) -> bool {
    matches!(
        col.column_type(),
        ColumnType::MYSQL_TYPE_TINY_BLOB
            | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
            | ColumnType::MYSQL_TYPE_LONG_BLOB
            | ColumnType::MYSQL_TYPE_BLOB
    )
}

/// The type of a `decimal` column from its wire precision and scale, or `Unsupported` when they cannot be derived.
fn mysql_decimal(col: &mysql::Column) -> RivetType {
    let signed = !col.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    match derive_decimal_ps(col.column_length(), col.decimals(), signed) {
        Some((precision, scale)) => RivetType::Decimal { precision, scale },
        None => RivetType::Unsupported {
            native_type: "decimal".into(),
            reason: "could not derive precision/scale from the MySQL column metadata; \
                     add a column override (columns: amount: decimal(18,2))"
                .into(),
        },
    }
}

/// The type of a column this engine alone carries, or `Unsupported` for a wire type it does not map.
fn mysql_engine_type(col: &mysql::Column) -> RivetType {
    use mysql::consts::ColumnType::*;
    match col.column_type() {
        MYSQL_TYPE_LONGLONG => RivetType::UInt64,
        MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_STRING
        | MYSQL_TYPE_ENUM
        | MYSQL_TYPE_SET => RivetType::Enum,
        MYSQL_TYPE_BIT if col.column_length() == 1 => RivetType::Bool,
        MYSQL_TYPE_BIT
            if col.column_length()
                > seeded!("mysql-bit64-signed-delivery", 64, BIT_INT64_MAX_WIDTH) =>
        {
            RivetType::UInt64
        }
        MYSQL_TYPE_BIT => RivetType::Int64,
        MYSQL_TYPE_YEAR => RivetType::Int16,
        _ => RivetType::Unsupported {
            native_type: mysql_native_type_name(col),
            reason: "no Rivet mapping for this MySQL type".into(),
        },
    }
}

/// Map a MySQL column descriptor to Rivet's canonical type.
pub(super) fn mysql_type_to_rivet(col: &mysql::Column) -> RivetType {
    match mysql_common_kind(col) {
        Some(CommonKind::Text) if is_blob_wire(col) => RivetType::Text,
        Some(kind) => kind.rivet_type().unwrap_or_else(|| mysql_decimal(col)),
        None => mysql_engine_type(col),
    }
}

/// The mapping of a MySQL column; a date or time stamp column that can hold a zero or partial date is `compatible`, never `exact`.
pub(crate) fn mysql_mapping(source: &SourceColumn, rivet: RivetType) -> TypeMapping {
    let temporal = matches!(rivet, RivetType::Date | RivetType::Timestamp { .. })
        && matches!(
            source.native_type.as_str(),
            "date" | "datetime" | "timestamp"
        );
    let duration = matches!(rivet, RivetType::Time { .. })
        && seeded!("mysql-time-delivered-as-time-of-day", false, true);
    let rivet = if duration { RivetType::String } else { rivet };
    let mut mapping = TypeMapping::from_source(source, rivet);
    if temporal && seeded!("mysql-zero-date-column-claims-exact", false, true) {
        mapping.fidelity = TypeFidelity::Compatible;
    }
    if duration {
        mapping = mapping.with_text(crate::types::TextForm::TimeBeyondDay);
    }
    mapping
}

/// Build an Arrow `Schema` and a parallel `Vec<DataType>` from MySQL column descriptors.
pub(super) fn mysql_schema_and_arrow_types(
    columns: &[mysql::Column],
    column_overrides: &ColumnOverrides,
) -> crate::error::Result<(Schema, Vec<DataType>)> {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut arrow_types: Vec<DataType> = Vec::with_capacity(columns.len());
    let mut errors: Vec<String> = Vec::new();

    for col in columns {
        let native = mysql_native_type_name(col);
        let rivet = crate::types::resolve_column(
            crate::types::override_table::Engine::Mysql,
            column_overrides,
            col.name_str().as_ref(),
            &native,
            || mysql_type_to_rivet(col),
        )?;
        let source = SourceColumn::simple(col.name_str().to_string(), native.clone(), true);
        let mapping = mysql_mapping(&source, rivet);

        match (build_arrow_field(&mapping), mapping.arrow_type) {
            (Some(field), Some(dt)) => {
                if column_overrides.contains_key(col.name_str().as_ref()) {
                    let built = build_array(&dt, 0, &[], false, col.name_str().as_ref(), None);
                    if seeded!("override-without-builder-internal-stop", false, true) {
                        crate::source::refuse_unbuildable_override(
                            "mysql",
                            col.name_str().as_ref(),
                            &mapping.rivet_type,
                            built,
                        )?;
                    }
                }
                fields.push(field);
                arrow_types.push(dt);
            }
            _ => {
                let reason = match &mapping.rivet_type {
                    RivetType::Unsupported { reason, .. } => reason.as_str(),
                    _ => "no Rivet mapping for this MySQL type",
                };
                errors.push(format!(
                    "  • {} (MySQL type '{native}'): {reason}",
                    col.name_str()
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
    Ok((Schema::new(fields), arrow_types))
}

/// The calendar date a MySQL date value names, `None` for a zero or partial date.
fn date_of(y: u16, m: u8, d: u8) -> Option<chrono::NaiveDate> {
    chrono::NaiveDate::from_ymd_opt(i32::from(y), u32::from(m), u32::from(d))
}

/// The instant a MySQL datetime value names, `None` for a zero or partial date.
fn datetime_of(
    y: u16,
    mo: u8,
    d: u8,
    h: u8,
    mi: u8,
    s: u8,
    us: u32,
) -> Option<chrono::NaiveDateTime> {
    date_of(y, mo, d)
        .and_then(|d| d.and_hms_micro_opt(u32::from(h), u32::from(mi), u32::from(s), us))
}

/// The per-column counts of zero or partial date cells written as NULL, as `col=n` joined by commas.
pub(crate) fn zero_date_counts(cells: &[(String, u64)]) -> String {
    cells
        .iter()
        .map(|(c, n)| format!("{c}={n}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The warning for `n` zero or partial date cells of `column` written as NULL.
pub(crate) fn zero_date_warning(column: &str, n: u64) -> String {
    format!("column `{column}`: {n} MySQL zero or partial date value(s) written as NULL")
}

/// Per date or timestamp column, the cells of `rows` that hold a zero or partial date and so become NULL.
pub(super) fn nulled_dates(
    schema: &SchemaRef,
    arrow_types: &[DataType],
    rows: &[mysql::Row],
) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for (col_idx, arrow_type) in arrow_types.iter().enumerate() {
        if !matches!(arrow_type, DataType::Date32 | DataType::Timestamp(..)) {
            continue;
        }
        let n = rows
            .iter()
            .filter(|r| match r.as_ref(col_idx) {
                Some(Value::Date(y, mo, d, h, mi, s, us)) => {
                    seeded!(
                        "mysql-zero-date-count-misses-a-class",
                        *y == 0 && *mo == 0 && *d == 0,
                        datetime_of(*y, *mo, *d, *h, *mi, *s, *us).is_none()
                    )
                }
                _ => false,
            })
            .count() as u64;
        if n > 0 {
            out.push((schema.field(col_idx).name().clone(), n));
        }
    }
    out
}

pub(super) fn rows_to_record_batch_typed(
    schema: &SchemaRef,
    arrow_types: &[DataType],
    rows: &[mysql::Row],
    max_value_bytes: Option<usize>,
) -> Result<RecordBatch> {
    let wire_columns = rows.first().map(|r| r.columns_ref());
    if let Some(cols) = wire_columns {
        let expected: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let names: Vec<String> = cols.iter().map(|c| c.name_str().into_owned()).collect();
        let wire: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        crate::source::verify_wire_columns(&expected, &wire)?;
    }
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(arrow_types.len());
    for (col_idx, arrow_type) in arrow_types.iter().enumerate() {
        let is_bit = wire_columns.is_some_and(|cols| {
            cols.get(col_idx)
                .is_some_and(|c| matches!(c.column_type(), ColumnType::MYSQL_TYPE_BIT))
        });
        let column = schema.field(col_idx).name();
        arrays.push(if is_time_text(schema.field(col_idx)) {
            build_time_text(col_idx, rows)?
        } else {
            build_array(arrow_type, col_idx, rows, is_bit, column, max_value_bytes)?
        });
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let a = crate::source::value_checksum::source_checksums(schema, &MysqlCellSource { rows });
    let b = crate::source::value_checksum::arrow_batch_checksums(&batch);
    crate::source::value_checksum::verify(&a, &b, schema)?;
    Ok(batch)
}

/// Whether the field is declared as `time_beyond_day` text.
fn is_time_text(field: &Field) -> bool {
    field
        .metadata()
        .get(crate::types::META_TEXT_FORM)
        .map(String::as_str)
        == Some(crate::types::TextForm::TimeBeyondDay.label())
}

/// Whether wire column `col` is a MySQL `TIME`.
fn is_time_wire(cols: Option<&[mysql::Column]>, col: usize) -> bool {
    cols.and_then(|c| c.get(col)).is_some_and(|c| {
        matches!(
            c.column_type(),
            ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2
        )
    })
}

/// A `TIME` column as `[-]hh:mm:ss.ffffff` text: the whole signed duration, no range refusal.
fn build_time_text(col_idx: usize, rows: &[mysql::Row]) -> Result<Arc<dyn Array>> {
    let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 17);
    for row in rows {
        match mysql_time_micros(row.as_ref(col_idx))? {
            Some(us) => b.append_value(crate::types::time_beyond_day(us)),
            None => b.append_null(),
        }
    }
    Ok(Arc::new(b.finish()))
}

/// Side A of the Form A value-checksum for MySQL.
struct MysqlCellSource<'a> {
    rows: &'a [mysql::Row],
}

impl MysqlCellSource<'_> {
    /// MySQL surfaces a `BIT` column's bytes big-endian.
    fn is_bit(&self, col: usize) -> bool {
        self.rows.first().is_some_and(|r| {
            r.columns_ref()
                .get(col)
                .is_some_and(|c| matches!(c.column_type(), ColumnType::MYSQL_TYPE_BIT))
        })
    }
}

impl crate::source::value_checksum::CellSource for MysqlCellSource<'_> {
    fn num_rows(&self) -> usize {
        self.rows.len()
    }
    fn boolean(&self, col: usize, row: usize) -> Option<bool> {
        match self.rows[row].as_ref(col) {
            Some(v) => flag_int(v, self.is_bit(col)).and_then(|n| {
                seeded!(
                    "mysql-checksum-flag-any-nonzero",
                    Some(n != 0),
                    flag_bool(n)
                )
            }),
            None => None,
        }
    }
    fn int16(&self, col: usize, row: usize) -> Option<i16> {
        match self.rows[row].as_ref(col) {
            Some(Value::Int(v)) => Some(*v as i16),
            Some(Value::UInt(v)) => Some(*v as i16),
            Some(Value::Bytes(bv)) => atoi::atoi::<i128>(bv).map(|v| v as i16),
            _ => None,
        }
    }
    fn int32(&self, col: usize, row: usize) -> Option<i32> {
        match self.rows[row].as_ref(col) {
            Some(Value::Int(v)) => Some(*v as i32),
            Some(Value::UInt(v)) => Some(*v as i32),
            Some(Value::Bytes(bv)) => atoi::atoi::<i128>(bv).map(|v| v as i32),
            _ => None,
        }
    }
    fn int64(&self, col: usize, row: usize) -> Option<i64> {
        match self.rows[row].as_ref(col) {
            Some(Value::Int(v)) => Some(*v),
            Some(Value::UInt(v)) => Some(*v as i64),
            Some(Value::Bytes(bv)) => int64_from_bytes(bv, self.is_bit(col)).ok().flatten(),
            _ => None,
        }
    }
    fn uint64(&self, col: usize, row: usize) -> Option<u64> {
        match self.rows[row].as_ref(col) {
            Some(Value::UInt(v)) => Some(*v),
            Some(Value::Int(v)) if *v >= 0 => Some(*v as u64),
            Some(Value::Bytes(bv)) if self.is_bit(col) => Some(bit_bytes_to_u64(bv)),
            Some(Value::Bytes(bv)) => atoi::atoi::<u64>(bv),
            _ => None,
        }
    }
    fn float32(&self, col: usize, row: usize) -> Option<f32> {
        match self.rows[row].as_ref(col) {
            Some(Value::Float(v)) => Some(*v),
            Some(Value::Double(v)) => Some(*v as f32),
            Some(Value::Int(v)) if seeded!("mysql-override-unread-value-nulled", false, true) => {
                Some(*v as f32)
            }
            Some(Value::UInt(v)) if seeded!("mysql-override-unread-value-nulled", false, true) => {
                Some(*v as f32)
            }
            Some(Value::Bytes(bv)) => {
                bytes_to_str(bv).and_then(crate::types::decimal::decimal_text_to_float)
            }
            _ => None,
        }
    }
    fn float64(&self, col: usize, row: usize) -> Option<f64> {
        match self.rows[row].as_ref(col) {
            Some(Value::Float(v)) => Some(*v as f64),
            Some(Value::Double(v)) => Some(*v),
            Some(Value::Int(v)) if seeded!("mysql-override-unread-value-nulled", false, true) => {
                Some(*v as f64)
            }
            Some(Value::UInt(v)) if seeded!("mysql-override-unread-value-nulled", false, true) => {
                Some(*v as f64)
            }
            Some(Value::Bytes(bv)) => {
                bytes_to_str(bv).and_then(crate::types::decimal::decimal_text_to_float)
            }
            _ => None,
        }
    }
    fn decimal128(&self, col: usize, row: usize, scale: i8) -> Option<i128> {
        use bigdecimal::{BigDecimal, RoundingMode, num_traits::ToPrimitive};
        let bd: BigDecimal = match self.rows[row].as_ref(col) {
            Some(Value::Bytes(bv)) => bytes_to_str(bv)?.parse().ok()?,
            Some(Value::Int(v)) => BigDecimal::from(*v),
            Some(Value::UInt(v)) => BigDecimal::from(*v),
            _ => return None,
        };
        bd.with_scale_round(scale as i64, RoundingMode::Down)
            .into_bigint_and_exponent()
            .0
            .to_i128()
    }
    fn date32(&self, col: usize, row: usize) -> Option<i32> {
        let d = match self.rows[row].as_ref(col) {
            Some(Value::Date(y, m, d, _, _, _, _)) => date_of(*y, *m, *d),
            Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(|s| {
                chrono::NaiveDate::parse_from_str(s.split(' ').next().unwrap_or(s), "%Y-%m-%d").ok()
            }),
            _ => None,
        }?;
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        Some((d - epoch).num_days() as i32)
    }
    fn ts_micros(&self, col: usize, row: usize) -> Option<i64> {
        let dt = match self.rows[row].as_ref(col) {
            Some(Value::Date(y, mo, d, h, mi, sx, us)) => {
                datetime_of(*y, *mo, *d, *h, *mi, *sx, *us)
            }
            Some(Value::Bytes(bv)) => bytes_to_str(bv)
                .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()),
            _ => None,
        }?;
        Some(dt.and_utc().timestamp_micros())
    }
    fn binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        match self.rows[row].as_ref(col) {
            Some(Value::Bytes(bv)) => Some(Cow::Borrowed(bv.as_slice())),
            _ => None,
        }
    }
    fn utf8(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        if is_time_wire(self.rows.first().map(|r| r.columns_ref()), col) {
            let us = mysql_time_micros(self.rows[row].as_ref(col))
                .ok()
                .flatten()?;
            return Some(Cow::Owned(crate::types::time_beyond_day(us).into_bytes()));
        }
        match self.rows[row].as_ref(col) {
            Some(Value::Bytes(bv)) => Some(match bytes_to_str(bv) {
                Some(s) => Cow::Borrowed(s.as_bytes()),
                None => Cow::Owned(String::from_utf8_lossy(bv).into_owned().into_bytes()),
            }),
            Some(Value::Int(v)) => Some(Cow::Owned(v.to_string().into_bytes())),
            Some(Value::UInt(v)) => Some(Cow::Owned(v.to_string().into_bytes())),
            Some(Value::Float(v)) => Some(Cow::Owned(v.to_string().into_bytes())),
            Some(Value::Double(v)) => Some(Cow::Owned(v.to_string().into_bytes())),
            Some(Value::Date(y, m, d, hh, mi, sx, us)) => Some(Cow::Owned(
                format!("{y:04}-{m:02}-{d:02} {hh:02}:{mi:02}:{sx:02}.{us:06}").into_bytes(),
            )),
            _ => None,
        }
    }
    fn time64_micros(&self, col: usize, row: usize) -> Option<i64> {
        mysql_time_of_day(self.rows[row].as_ref(col)).ok().flatten()
    }
    fn fixed_binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        match self.rows[row].as_ref(col) {
            Some(Value::Bytes(bv)) if bv.len() == 16 => Some(Cow::Borrowed(bv.as_slice())),
            Some(Value::Bytes(bv)) => bytes_to_str(bv)
                .and_then(|s| uuid::Uuid::parse_str(s.trim()).ok())
                .map(|u| Cow::Owned(u.as_bytes().to_vec())),
            _ => None,
        }
    }
    fn decimal256(&self, col: usize, row: usize, scale: i8) -> Option<arrow::datatypes::i256> {
        use crate::types::decimal::decimal_str_to_scaled_i256;
        match self.rows[row].as_ref(col) {
            Some(Value::Bytes(bv)) => {
                bytes_to_str(bv).and_then(|s| decimal_str_to_scaled_i256(s.trim(), scale))
            }
            Some(Value::Int(v)) => decimal_str_to_scaled_i256(&v.to_string(), scale),
            Some(Value::UInt(v)) => decimal_str_to_scaled_i256(&v.to_string(), scale),
            _ => None,
        }
    }
    fn list(
        &self,
        _col: usize,
        _row: usize,
        _elem: &arrow::datatypes::DataType,
    ) -> Option<Vec<crate::source::value_checksum::ListElem>> {
        None
    }
}

fn bytes_to_str(b: &[u8]) -> Option<&str> {
    simdutf8::basic::from_utf8(b).ok()
}

/// Narrow a value already widened to `i128` into a smaller signed integer, erroring on overflow instead of silently wrapping the way `as iN` does.
fn narrow<T>(v: i128, column_type: &str) -> Result<T>
where
    T: TryFrom<i128>,
{
    T::try_from(v).map_err(|_| {
        crate::error::coded(
            crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
            format!("value {v} overflows {column_type} column"),
        )
    })
}

/// Interpret raw big-endian bytes from a MySQL BIT column as an unsigned integer.
pub(super) fn bit_bytes_to_u64(b: &[u8]) -> u64 {
    b.iter().fold(0u64, |acc, &byte| acc << 8 | u64::from(byte))
}

/// Decode a UInt64 cell that arrived as `Value::Bytes`.
fn uint64_from_bytes(column: &str, bv: &[u8], is_bit: bool) -> Result<u64> {
    if is_bit {
        return Ok(bit_bytes_to_u64(bv));
    }
    match atoi::atoi::<u64>(bv) {
        Some(v) => Ok(v),
        None => unparsed(column, "unsigned bigint", bv),
    }
}

/// Decode an Int64 cell that arrived as `Value::Bytes`.
fn int64_from_bytes(bv: &[u8], is_bit: bool) -> Result<Option<i64>> {
    if is_bit {
        return Ok(Some(narrow::<i64>(bit_bytes_to_u64(bv) as i128, "bit")?));
    }
    match atoi::atoi::<i128>(bv) {
        Some(v) => Ok(Some(narrow::<i64>(v, "bigint")?)),
        None => Ok(None),
    }
}

/// Parse MySQL text-protocol TIME string ("HH:MM:SS", "-HHH:MM:SS", "HH:MM:SS.uuuuuu") into microseconds since midnight.
fn mysql_time_micros(v: Option<&Value>) -> Result<Option<i64>> {
    let us = match v {
        None | Some(Value::NULL) => return Ok(None),
        Some(Value::Time(neg, days, h, m, s, us)) => {
            let total = (*days as i64 * 86_400 + *h as i64 * 3_600 + *m as i64 * 60 + *s as i64)
                * 1_000_000
                + *us as i64;
            if *neg { -total } else { total }
        }
        Some(Value::Bytes(bv)) => match bytes_to_str(bv).and_then(parse_time_str_to_micros) {
            Some(us) => us,
            None => crate::rivet_bail!(
                crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                "mysql: a TIME value {:?} could not be read; rivet refuses rather than writing NULL",
                String::from_utf8_lossy(bv)
            ),
        },
        Some(other) => crate::rivet_bail!(
            crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
            "mysql: a TIME column delivered {other:?}; rivet refuses rather than writing NULL"
        ),
    };
    Ok(Some(us))
}

/// [`mysql_time_micros`] held to one day: a duration outside 00:00..24:00 is refused.
fn mysql_time_of_day(v: Option<&Value>) -> Result<Option<i64>> {
    mysql_time_micros(v)?.map(time_of_day_in_range).transpose()
}

/// MySQL TIME microseconds unchanged when inside 00:00..24:00; a refusal with the MySQL remedy otherwise.
fn time_of_day_in_range(us: i64) -> Result<i64> {
    if !crate::types::is_time_of_day(us) {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
            "{}",
            time_outside_day_refusal(us)
        );
    }
    Ok(us)
}

/// The MySQL wording (batch and CDC) for a TIME outside one day, without a remedy.
pub(crate) fn time_outside_day(us: i64) -> String {
    format!(
        "mysql: TIME {} is outside 00:00..24:00, which a Parquet TIME cannot hold (MySQL TIME is a \
         duration up to 838:59:59).",
        fmt_micros_as_time(us)
    )
}

/// [`time_outside_day`] with the batch remedy: a cast in the export's query.
pub(crate) fn time_outside_day_refusal(us: i64) -> String {
    format!(
        "{} Cast it in a query, e.g. TIME_TO_SEC(col) or CAST(col AS CHAR)",
        time_outside_day(us)
    )
}

/// `[-]H:MM:SS.ffffff` for a message.
fn fmt_micros_as_time(us: i64) -> String {
    let (sign, a) = if us.is_negative() {
        ("-", -us)
    } else {
        ("", us)
    };
    let s = a / 1_000_000;
    format!(
        "{sign}{}:{:02}:{:02}.{:06}",
        s / 3600,
        s / 60 % 60,
        s % 60,
        a % 1_000_000
    )
}

fn parse_time_str_to_micros(s: &str) -> Option<i64> {
    let (neg, rest) = if let Some(r) = s.strip_prefix('-') {
        (true, r)
    } else {
        (false, s)
    };
    let (hms, us_part) = if let Some(pos) = rest.find('.') {
        let us_str = &rest[pos + 1..];
        let us_digits = us_str.len().min(6);
        let us = us_str[..us_digits].parse::<i64>().ok()?;
        let scale = 10i64.pow((6 - us_digits) as u32);
        (&rest[..pos], us * scale)
    } else {
        (rest, 0i64)
    };
    let mut parts = hms.splitn(3, ':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    let total = (h * 3_600 + m * 60 + s) * 1_000_000 + us_part;
    Some(if neg { -total } else { total })
}

/// Largest integer magnitude a `float8` holds exactly (2^53).
const F64_EXACT_INT_MAX: i128 = 1 << 53;
/// Largest integer magnitude a `float4` holds exactly (2^24).
const F32_EXACT_INT_MAX: i128 = 1 << 24;

/// A MySQL integer under a `float4`/`float8` override as a float, or a coded refusal when the float cannot hold it exactly.
fn exact_float(v: i128, max: i128, kind: &str, wider: &str, column: &str) -> Result<f64> {
    if v.abs() > max {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
            "column `{column}` is declared {kind} by a `columns:` override but holds {v}, which a \
             {kind} cannot hold exactly (exact up to {max}) — rivet refuses rather than rounding \
             it. {wider}"
        );
    }
    Ok(v as f64)
}

/// [`exact_float`] for `float8`.
fn exact_f64(v: i128, column: &str) -> Result<f64> {
    exact_float(
        v,
        F64_EXACT_INT_MAX,
        "float8",
        "Declare `decimal(20,0)` to keep the integer, or remove the override.",
        column,
    )
}

/// [`exact_float`] for `float4`.
fn exact_f32(v: i128, column: &str) -> Result<f32> {
    exact_float(
        v,
        F32_EXACT_INT_MAX,
        "float4",
        "Declare `float8` (exact up to 9007199254740992) or `decimal(20,0)`, or remove the override.",
        column,
    )
    .map(|f| f as f32)
}

/// A text cell that no parse of `target` accepts: refused by code, never written as NULL.
fn unparsed<T>(column: &str, target: &str, bv: &[u8]) -> Result<T> {
    crate::rivet_bail!(
        crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
        "column `{column}` holds {:?}, which is not a valid {target} — rivet refuses rather than \
         writing NULL. Fix the value, or remove the `columns:` override.",
        String::from_utf8_lossy(bv)
    )
}

/// A float8 read from text: the same number when the float prints it back, refused by code when it would round or is not a number.
fn float_of_bytes(column: &str, bv: &[u8]) -> Result<f64> {
    match bytes_to_str(bv)
        .map(|s| crate::types::decimal::exact_f64_of_text(column, s))
        .transpose()?
        .flatten()
    {
        Some(v) => Ok(v),
        None => unparsed(column, "float", bv),
    }
}

/// True for the zero date MySQL stores in place of a missing one.
fn is_zero_date(bv: &[u8]) -> bool {
    bv.starts_with(b"0000-00-00")
}

/// A flag read as a boolean: 0 and 1 are the only values, any other is refused by code whatever wire type holds it.
fn build_flags(
    col_idx: usize,
    rows: &[mysql::Row],
    is_bit: bool,
    column: &str,
) -> Result<Arc<dyn Array>> {
    let mut b = BooleanBuilder::with_capacity(rows.len());
    for row in rows {
        let cell = row.as_ref(col_idx);
        match cell.and_then(|v| flag_int(v, is_bit)) {
            Some(n) => b.append_value(flag(column, n)?),
            None => match cell {
                Some(Value::Bytes(bv)) => return unparsed(column, "bool (0 or 1)", bv),
                other => {
                    null_or_refuse(column, &DataType::Boolean, other)?;
                    b.append_null();
                }
            },
        }
    }
    Ok(Arc::new(b.finish()))
}

/// The number a flag cell holds on any wire type: an integer, BIT bytes, or the text `0` or `1`.
fn flag_int(v: &Value, is_bit: bool) -> Option<i128> {
    match v {
        Value::Int(n) => Some(i128::from(*n)),
        Value::UInt(n) => Some(i128::from(*n)),
        Value::Bytes(bv) if is_bit => Some(i128::from(bit_bytes_to_u64(bv))),
        Value::Bytes(bv) => match bv.as_slice() {
            [d @ (b'0' | b'1')] => Some(i128::from(d - b'0')),
            [..] => None,
        },
        Value::NULL | Value::Float(_) | Value::Double(_) | Value::Date(..) | Value::Time(..) => {
            None
        }
    }
}

/// The one flag rule: 0 is false, 1 is true, any other number is no flag.
pub(crate) fn flag_bool(v: i128) -> Option<bool> {
    (0..=1).contains(&v).then_some(v == 1)
}

/// The flag a 0 or 1 holds; any other value is refused by code.
fn flag(column: &str, v: i128) -> Result<bool> {
    match flag_bool(v) {
        _ if seeded!("mysql-flag-outside-zero-one-delivered-true", v != 0, false) => Ok(true),
        Some(f) => Ok(f),
        None => crate::rivet_bail!(
            crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
            "column `{column}` holds {v}, which is not a flag: a boolean column carries 0 or 1 \
             only, and rivet refuses rather than writing it as true. Fix the value, or declare \
             the column an integer type in `columns:` (for example `int64`) to carry the number."
        ),
    }
}

/// The kind of value MySQL sent, for a refusal.
fn wire_kind(v: &Value) -> &'static str {
    match v {
        Value::NULL => "NULL",
        Value::Bytes(_) => "text or bytes",
        Value::Int(_) => "a signed integer",
        Value::UInt(_) => "an unsigned integer",
        Value::Float(_) => "a FLOAT",
        Value::Double(_) => "a DOUBLE",
        Value::Date(..) => "a date or datetime",
        Value::Time(..) => "a TIME",
    }
}

/// A cell no arm of the `target` builder converts.
fn null_or_refuse(column: &str, target: &DataType, cell: Option<&Value>) -> Result<()> {
    match cell {
        None | Some(Value::NULL) => Ok(()),
        Some(_) if seeded!("mysql-override-unread-value-nulled", true, false) => Ok(()),
        Some(other) => crate::rivet_bail!(
            crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
            "column `{column}` is read as {target} but MySQL sends {} — rivet does not convert \
             it, and writing it as NULL would lose every value. Remove the `columns:` override, \
             or CAST the column to that type in the export's `query:`.",
            wire_kind(other)
        ),
    }
}

fn build_array(
    arrow_type: &DataType,
    col_idx: usize,
    rows: &[mysql::Row],
    is_bit: bool,
    column: &str,
    max_value_bytes: Option<usize>,
) -> Result<Arc<dyn Array>> {
    match arrow_type {
        DataType::Boolean => build_flags(col_idx, rows, is_bit, column),
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => b.append_value(narrow::<i16>(*v as i128, "smallint")?),
                    Some(Value::UInt(v)) => b.append_value(narrow::<i16>(*v as i128, "smallint")?),
                    Some(Value::Bytes(bv)) => match atoi::atoi::<i128>(bv) {
                        Some(v) => b.append_value(narrow::<i16>(v, "smallint")?),
                        None => b.append_value(unparsed(column, "smallint", bv)?),
                    },
                    other => {
                        null_or_refuse(column, arrow_type, other)?;
                        b.append_null();
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => b.append_value(narrow::<i32>(*v as i128, "int")?),
                    Some(Value::UInt(v)) => b.append_value(narrow::<i32>(*v as i128, "int")?),
                    Some(Value::Bytes(bv)) => match atoi::atoi::<i128>(bv) {
                        Some(v) => b.append_value(narrow::<i32>(v, "int")?),
                        None => b.append_value(unparsed(column, "int", bv)?),
                    },
                    other => {
                        null_or_refuse(column, arrow_type, other)?;
                        b.append_null();
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::UInt64 => {
            let mut b = UInt64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::UInt(v)) => b.append_value(*v),
                    Some(Value::Int(v)) if *v >= 0 => b.append_value(*v as u64),
                    Some(Value::Bytes(bv)) => {
                        b.append_value(uint64_from_bytes(column, bv, is_bit)?)
                    }
                    other => {
                        null_or_refuse(column, arrow_type, other)?;
                        b.append_null();
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => b.append_value(*v),
                    Some(Value::UInt(v)) => b.append_value(narrow::<i64>(*v as i128, "bigint")?),
                    Some(Value::Bytes(bv)) => match int64_from_bytes(bv, is_bit)? {
                        Some(v) => b.append_value(v),
                        None => b.append_value(unparsed(column, "bigint", bv)?),
                    },
                    other => {
                        null_or_refuse(column, arrow_type, other)?;
                        b.append_null();
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Float(v)) => b.append_value(*v),
                    Some(Value::Double(v)) => b.append_value(*v as f32),
                    Some(Value::Int(v))
                        if seeded!("mysql-override-unread-value-nulled", false, true) =>
                    {
                        b.append_value(exact_f32(*v as i128, column)?)
                    }
                    Some(Value::UInt(v))
                        if seeded!("mysql-override-unread-value-nulled", false, true) =>
                    {
                        b.append_value(exact_f32(*v as i128, column)?)
                    }
                    Some(Value::Bytes(bv)) => match bytes_to_str(bv)
                        .and_then(crate::types::decimal::decimal_text_to_float)
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_value(unparsed(column, "float", bv)?),
                    },
                    other => {
                        null_or_refuse(column, arrow_type, other)?;
                        b.append_null();
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Float(v)) => b.append_value(*v as f64),
                    Some(Value::Double(v)) => b.append_value(*v),
                    Some(Value::Int(v))
                        if seeded!("mysql-override-unread-value-nulled", false, true) =>
                    {
                        b.append_value(exact_f64(*v as i128, column)?)
                    }
                    Some(Value::UInt(v))
                        if seeded!("mysql-override-unread-value-nulled", false, true) =>
                    {
                        b.append_value(exact_f64(*v as i128, column)?)
                    }
                    Some(Value::Bytes(bv)) => b.append_value(float_of_bytes(column, bv)?),
                    other => {
                        null_or_refuse(column, arrow_type, other)?;
                        b.append_null();
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Bytes(bv)) => {
                        value_within_ceiling(column, bv.len(), max_value_bytes)?;
                        match bytes_to_str(bv) {
                            Some(s) => b.append_value(s),
                            None => b.append_value(String::from_utf8_lossy(bv).as_ref()),
                        }
                    }
                    Some(Value::Int(v)) => b.append_value(v.to_string()),
                    Some(Value::UInt(v)) => b.append_value(v.to_string()),
                    Some(Value::Float(v)) => b.append_value(v.to_string()),
                    Some(Value::Double(v)) => b.append_value(v.to_string()),
                    Some(Value::Date(y, m, d, h, mi, s, us)) => {
                        b.append_value(format!(
                            "{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"
                        ));
                    }
                    other => {
                        null_or_refuse(column, arrow_type, other)?;
                        b.append_null();
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Bytes(bv)) => {
                        value_within_ceiling(column, bv.len(), max_value_bytes)?;
                        b.append_value(bv);
                    }
                    other => {
                        null_or_refuse(column, arrow_type, other)?;
                        b.append_null();
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::FixedSizeBinary(16) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
            for row in rows {
                let bytes = match row.as_ref(col_idx) {
                    Some(Value::Bytes(bv)) if bv.len() == 16 => {
                        let mut a = [0u8; 16];
                        a.copy_from_slice(bv);
                        Some(a)
                    }
                    Some(Value::Bytes(bv)) => {
                        match bytes_to_str(bv).and_then(|s| uuid::Uuid::parse_str(s.trim()).ok()) {
                            Some(u) => Some(*u.as_bytes()),
                            None => crate::rivet_bail!(
                                crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                                "column `{column}` is declared uuid by a `columns:` override but \
                                 holds {:?}, which is not a uuid — rivet refuses rather than \
                                 writing NULL. Fix the value, or remove the override.",
                                String::from_utf8_lossy(bv)
                            ),
                        }
                    }
                    None | Some(Value::NULL) => None,
                    Some(other) => crate::rivet_bail!(
                        crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
                        "column `{column}` is declared uuid by a `columns:` override but MySQL \
                         sends {other:?} — rivet does not convert it, and writing it as NULL \
                         would lose every value. Remove the override."
                    ),
                };
                match bytes {
                    Some(a) => b
                        .append_value(a)
                        .expect("16 bytes always fits FixedSizeBinary(16)"),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let mut b = Time64MicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                b.append_option(mysql_time_of_day(row.as_ref(col_idx))?);
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            for row in rows {
                let d = match row.as_ref(col_idx) {
                    Some(Value::Date(y, m, d, _, _, _, _)) => date_of(*y, *m, *d),
                    Some(Value::Bytes(bv)) => {
                        let parsed = bytes_to_str(bv).and_then(|s| {
                            chrono::NaiveDate::parse_from_str(
                                s.split(' ').next().unwrap_or(s),
                                "%Y-%m-%d",
                            )
                            .ok()
                        });
                        if parsed.is_none() && !is_zero_date(bv) {
                            return unparsed(column, "date", bv);
                        }
                        parsed
                    }
                    other => {
                        null_or_refuse(column, arrow_type, other)?;
                        None
                    }
                };
                match d {
                    Some(date) => {
                        let epoch =
                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
                        b.append_value((date - epoch).num_days() as i32);
                    }
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let tz_tag = tz.clone();
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                let dt = match row.as_ref(col_idx) {
                    Some(Value::Date(y, mo, d, h, mi, s, us)) => {
                        datetime_of(*y, *mo, *d, *h, *mi, *s, *us)
                    }
                    Some(Value::Bytes(bv)) => {
                        let parsed = bytes_to_str(bv).and_then(|s| {
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()
                        });
                        if parsed.is_none() && !is_zero_date(bv) {
                            return unparsed(column, "timestamp", bv);
                        }
                        parsed
                    }
                    other => {
                        null_or_refuse(column, arrow_type, other)?;
                        None
                    }
                };
                match dt {
                    Some(dt) => b.append_value(dt.and_utc().timestamp_micros()),
                    None => b.append_null(),
                }
            }
            let arr = b.finish();
            match tz_tag {
                Some(tz_str) => Ok(Arc::new(arr.with_timezone(tz_str.as_ref()))),
                None => Ok(Arc::new(arr)),
            }
        }
        DataType::Decimal128(p, s) => mysql_decimal_to_decimal128(*p, *s, col_idx, rows, column),
        DataType::Decimal256(p, s) => mysql_decimal_to_decimal256(*p, *s, col_idx, rows, column),
        _ => crate::rivet_bail!(
            crate::error::codes::INTERNAL_TYPE_BUILDER,
            "no value converter for MySQL column → Arrow {:?} (column index {col_idx}); \
             this Arrow type has no builder — report it as a type-support gap",
            arrow_type,
        ),
    }
}

/// Build a `Decimal128Array` from MySQL DECIMAL column bytes (text protocol), or from a MySQL integer column when the operator declared a Decimal override.
fn mysql_decimal_to_decimal128(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[mysql::Row],
    column: &str,
) -> Result<Arc<dyn Array>> {
    use crate::types::decimal::decimal_str_to_scaled_i128;
    let target = DataType::Decimal128(precision, scale);
    let mut b = Decimal128Builder::with_capacity(rows.len());
    for row in rows {
        match row.as_ref(col_idx) {
            Some(Value::Bytes(bv)) => {
                let s = bytes_to_str(bv).unwrap_or("");
                match decimal_str_to_scaled_i128(s, scale) {
                    Some(v) => b.append_value(v),
                    None => {
                        return Err(crate::error::coded(
                            crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                            format!("cannot parse '{}' as decimal({},{})", s, precision, scale),
                        ));
                    }
                }
            }
            Some(Value::Int(v)) => match scale_int_to_i128(*v as i128, scale) {
                Some(scaled) => b.append_value(scaled),
                None => {
                    return Err(crate::error::coded(
                        crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                        format!(
                            "decimal({},{}) overflow scaling integer {}",
                            precision, scale, v
                        ),
                    ));
                }
            },
            Some(Value::UInt(v)) => match scale_int_to_i128(*v as i128, scale) {
                Some(scaled) => b.append_value(scaled),
                None => {
                    return Err(crate::error::coded(
                        crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                        format!(
                            "decimal({},{}) overflow scaling unsigned integer {}",
                            precision, scale, v
                        ),
                    ));
                }
            },
            other => {
                null_or_refuse(column, &target, other)?;
                b.append_null();
            }
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}

/// Scale an integer value (already widened to `i128`) by `10^scale` for storage as a fixed-point Decimal.
fn scale_int_to_i128(v: i128, scale: i8) -> Option<i128> {
    if scale < 0 {
        return None;
    }
    10i128
        .checked_pow(scale as u32)
        .and_then(|mult| v.checked_mul(mult))
}

/// Build a `Decimal256Array` for precision > 38.
fn mysql_decimal_to_decimal256(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[mysql::Row],
    column: &str,
) -> Result<Arc<dyn Array>> {
    use crate::types::decimal::{decimal_str_to_scaled_i256, scale_int_to_i256};
    let target = DataType::Decimal256(precision, scale);
    let mut b = Decimal256Builder::with_capacity(rows.len());
    for row in rows {
        match row.as_ref(col_idx) {
            Some(Value::Bytes(bv)) => {
                let s = bytes_to_str(bv).unwrap_or("");
                match decimal_str_to_scaled_i256(s, scale) {
                    Some(v) => b.append_value(v),
                    None => {
                        return Err(crate::error::coded(
                            crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                            format!("cannot parse '{}' as decimal({},{})", s, precision, scale),
                        ));
                    }
                }
            }
            Some(Value::Int(v)) => match scale_int_to_i256(*v as i128, scale) {
                Some(scaled) => b.append_value(scaled),
                None => {
                    return Err(crate::error::coded(
                        crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                        format!(
                            "decimal({},{}) overflow scaling integer {}",
                            precision, scale, v
                        ),
                    ));
                }
            },
            Some(Value::UInt(v)) => match scale_int_to_i256(*v as i128, scale) {
                Some(scaled) => b.append_value(scaled),
                None => {
                    return Err(crate::error::coded(
                        crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
                        format!(
                            "decimal({},{}) overflow scaling unsigned integer {}",
                            precision, scale, v
                        ),
                    ));
                }
            },
            other => {
                null_or_refuse(column, &target, other)?;
                b.append_null();
            }
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}

#[cfg(test)]
mod common_kind_tests;
#[cfg(test)]
mod type_table;

#[cfg(test)]
mod time_parse_tests {
    use super::parse_time_str_to_micros;

    /// `parse_time_str_to_micros` had NO direct test.
    #[test]
    fn parse_time_str_to_micros_pins_every_arithmetic_step() {
        assert_eq!(
            parse_time_str_to_micros("01:02:03.456789"),
            Some(3_723_456_789),
            "hours, minutes, seconds and a full 6-digit fraction"
        );
        assert_eq!(
            parse_time_str_to_micros("00:00:01.5"),
            Some(1_500_000),
            "a 1-digit fraction scales by 10^(6-1), not 10^(6+1)"
        );
        assert_eq!(parse_time_str_to_micros("02:00:00"), Some(7_200_000_000));
        assert_eq!(
            parse_time_str_to_micros("-01:02:03.456789"),
            Some(-3_723_456_789)
        );
        assert_eq!(parse_time_str_to_micros("00:00:00.1234567"), Some(123_456));
        assert_eq!(parse_time_str_to_micros("not a time"), None);
        assert_eq!(parse_time_str_to_micros("01:02"), None);
    }
}

#[cfg(test)]
mod scale_int_overflow_tests {
    use super::{derive_decimal_ps, narrow, scale_int_to_i128};

    #[test]
    fn negative_scale_is_rejected() {
        assert_eq!(scale_int_to_i128(123, -1), None);
    }

    #[test]
    fn scale_zero_is_identity_and_u64_max_rides_losslessly() {
        assert_eq!(scale_int_to_i128(123, 0), Some(123));
        assert_eq!(
            scale_int_to_i128(u64::MAX as i128, 0),
            Some(u64::MAX as i128)
        );
    }

    #[test]
    fn normal_scaling() {
        assert_eq!(scale_int_to_i128(5, 2), Some(500));
        assert_eq!(scale_int_to_i128(-7, 3), Some(-7000));
    }

    #[test]
    fn overflow_returns_none_not_wrap() {
        assert_eq!(scale_int_to_i128(u64::MAX as i128, 20), None);
        assert_eq!(scale_int_to_i128(1, 39), None);
        assert_eq!(scale_int_to_i128(i128::MAX, 1), None);
    }

    #[test]
    fn narrow_fits_in_range() {
        assert_eq!(narrow::<i16>(100, "smallint").unwrap(), 100i16);
        assert_eq!(narrow::<i32>(-5, "int").unwrap(), -5i32);
        assert_eq!(
            narrow::<i64>(u32::MAX as i128, "bigint").unwrap(),
            u32::MAX as i64
        );
    }

    #[test]
    fn narrow_overflow_errors_not_wraps() {
        assert!(narrow::<i16>(40_000, "smallint").is_err());
        assert!(narrow::<i16>(-40_000, "smallint").is_err());
        assert!(narrow::<i32>(5_000_000_000, "int").is_err());
        assert!(narrow::<i64>(u64::MAX as i128, "bigint").is_err());
    }

    #[test]
    fn derive_decimal_ps_signed_and_unsigned() {
        assert_eq!(derive_decimal_ps(12, 2, true), Some((10, 2)));
        assert_eq!(derive_decimal_ps(11, 0, true), Some((10, 0)));
        assert_eq!(derive_decimal_ps(11, 2, false), Some((10, 2)));
        assert_eq!(derive_decimal_ps(20, 2, true), Some((18, 2)));
        assert_eq!(derive_decimal_ps(22, 6, true), Some((20, 6)));
    }

    #[test]
    fn derive_decimal_ps_rejects_insane_widths() {
        assert_eq!(derive_decimal_ps(1, 2, true), None);
        assert_eq!(derive_decimal_ps(0, 0, false), None);
        assert_eq!(derive_decimal_ps(100, 0, false), None);
    }
}

#[cfg(test)]
mod utf8_fast_path_tests {
    use super::bytes_to_str;

    /// The Utf8 text-column append swaps `String::from_utf8_lossy` for a simdutf8 fast path (`bytes_to_str`) with a lossy fallback on the rare invalid value.
    fn append_as_done_in_decode(bv: &[u8]) -> String {
        match bytes_to_str(bv) {
            Some(s) => s.to_owned(),
            None => String::from_utf8_lossy(bv).into_owned(),
        }
    }

    #[test]
    fn fast_path_is_byte_identical_to_lossy() {
        let cases: &[&[u8]] = &[
            b"",
            b"plain ascii",
            "héllo wörld 日本語".as_bytes(),
            &[0xff, 0xfe, 0x00, 0x41],
            &[0x41, 0xc0, 0x42],
            &[0xe4, 0xb8],
        ];
        for &bv in cases {
            assert_eq!(
                append_as_done_in_decode(bv),
                String::from_utf8_lossy(bv),
                "fast path diverged from lossy for {bv:?}"
            );
        }
    }
}

#[cfg(test)]
mod int64_bytes_dispatch_tests {
    use super::int64_from_bytes;

    #[test]
    fn bit_bytes_decode_big_endian_even_when_ascii_digits() {
        assert_eq!(int64_from_bytes(&[0x39], true).unwrap(), Some(57));
        assert_eq!(int64_from_bytes(&[0x31, 0x32], true).unwrap(), Some(0x3132));
        assert_eq!(int64_from_bytes(&[0x31, 0xFF], true).unwrap(), Some(12799));
        assert_eq!(int64_from_bytes(&[0xAB, 0xCD], true).unwrap(), Some(0xABCD));
    }

    #[test]
    fn bit64_top_bit_set_errors_not_wraps() {
        assert!(int64_from_bytes(&[0xFF; 8], true).is_err());
        assert_eq!(
            int64_from_bytes(&[0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], true).unwrap(),
            Some(i64::MAX)
        );
    }

    #[test]
    fn text_bytes_follow_the_int_arm_mismatch_policy() {
        assert_eq!(int64_from_bytes(b"12", false).unwrap(), Some(12));
        assert_eq!(int64_from_bytes(b"-42", false).unwrap(), Some(-42));
        assert_eq!(int64_from_bytes(b"hello", false).unwrap(), None);
        assert_eq!(int64_from_bytes(b"", false).unwrap(), None);
        assert!(int64_from_bytes(b"9223372036854775808", false).is_err());
        assert_eq!(
            int64_from_bytes(b"9223372036854775807", false).unwrap(),
            Some(i64::MAX)
        );
    }
}

#[cfg(test)]
mod roast_mysql_bit_decode_tests {
    use super::RivetType;
    use super::{
        MysqlCellSource, TimeUnit, build_array, mysql_native_type_name, mysql_type_to_rivet,
    };
    use crate::types::TimeUnit as RivetTimeUnit;
    use std::io::{Read, Write};

    /// A MySQL date or time stamp column is `compatible` and every other mapping keeps the fidelity `from_source` derives.
    #[test]
    fn a_mysql_temporal_column_is_never_exact_and_the_rest_are_unchanged() {
        use crate::types::{SourceColumn, TypeFidelity, TypeMapping};
        let utc = || RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        };
        for (native, t) in [
            ("date", RivetType::Date),
            ("datetime", utc()),
            ("timestamp", utc()),
        ] {
            let m = super::mysql_mapping(&SourceColumn::simple("c", native, true), t);
            assert_eq!(m.fidelity, TypeFidelity::Compatible, "{native}");
        }
        for (native, t) in [
            ("int", RivetType::Int32),
            ("varchar", RivetType::String),
            ("date", RivetType::String),
            ("datetime", RivetType::String),
        ] {
            let same =
                TypeMapping::from_source(&SourceColumn::simple("c", native, true), t.clone());
            let m = super::mysql_mapping(&SourceColumn::simple("c", native, true), t);
            assert_eq!(m.fidelity, same.fidelity, "{native}");
        }
    }

    /// A MySQL `TIME` is a signed duration to 838:59:59, so it is delivered as `time_beyond_day` text and every value is kept.
    #[test]
    fn a_time_column_is_delivered_as_text_so_a_duration_beyond_a_day_is_kept() {
        use arrow::array::StringArray;
        use mysql::consts::ColumnType::MYSQL_TYPE_TIME;

        let def = column_definition("c", CS_BIN, 17, MYSQL_TYPE_TIME as u8, 0, 6);
        let cols = vec![def.clone()];
        let rows = fetch_binary_rows(
            cols,
            vec![
                vec![Some(v_time(false, 0, 14, 30, 0, 123_456).1)],
                vec![Some(v_time(false, 1, 1, 30, 15, 0).1)],
                vec![Some(v_time(false, 34, 22, 59, 59, 0).1)],
                vec![Some(v_time(true, 0, 0, 0, 1, 0).1)],
                vec![Some(v_time(true, 34, 22, 59, 59, 0).1)],
                vec![None],
            ],
        );
        let column = rows[0].columns_ref()[0].clone();
        let source = crate::types::SourceColumn::simple("c", mysql_native_type_name(&column), true);
        let mapping = super::mysql_mapping(&source, mysql_type_to_rivet(&column));
        assert_eq!(
            mapping.delivery,
            crate::types::Delivery::Text(crate::types::TextForm::TimeBeyondDay)
        );
        let dt = mapping.arrow_type.clone().expect("a delivery type");
        assert_eq!(mapping.rivet_type, crate::types::RivetType::String);
        assert_eq!(dt, DataType::Utf8);
        let field = crate::types::build_arrow_field(&mapping).expect("a text field");
        let schema = Arc::new(Schema::new(vec![field]));
        let batch =
            rows_to_record_batch_typed(&schema, &[dt], &rows, None).expect("TIME rows build");
        let got = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8");
        let shown: Vec<Option<&str>> = got.iter().collect();
        assert_eq!(
            shown,
            vec![
                Some("14:30:00.123456"),
                Some("25:30:15.000000"),
                Some("838:59:59.000000"),
                Some("-00:00:01.000000"),
                Some("-838:59:59.000000"),
                None,
            ]
        );
    }

    /// A `text` override on a MySQL `TIME` reads nothing from it: the value is refused, as before the text delivery.
    #[test]
    fn a_text_override_on_a_time_column_is_still_refused() {
        let (def, bytes) = v_time(false, 1, 1, 30, 15, 0);
        let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
        let err = build_array(&DataType::Utf8, 0, &rows, false, "c", None)
            .expect_err("a TIME under a text override");
        crate::error::assert_coded(&err, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
    }

    /// How long any socket in the fake harness waits.
    const FAKE_IO_TIMEOUT: Duration = Duration::from_secs(1);
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use mysql::consts::ColumnType;
    use mysql::prelude::Queryable;
    use mysql::{Conn, OptsBuilder, Value};

    use super::{
        mysql_schema_and_arrow_types, nulled_dates, rows_to_record_batch_typed, zero_date_warning,
    };
    use crate::types::ColumnOverrides;

    /// Frame a payload as a MySQL protocol packet: 3-byte LE length + seq id.
    fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
        let len = payload.len();
        let header = [len as u8, (len >> 8) as u8, (len >> 16) as u8, seq];
        stream
            .write_all(&header)
            .expect("fake MySQL server: write packet header");
        stream
            .write_all(payload)
            .expect("fake MySQL server: write packet payload");
    }

    /// Read one client packet (header + payload); contents are ignored.
    fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
        let mut header = [0u8; 4];
        stream
            .read_exact(&mut header)
            .expect("fake MySQL server: read packet header");
        let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let mut payload = vec![0u8; len];
        stream
            .read_exact(&mut payload)
            .expect("fake MySQL server: read packet payload");
        payload
    }

    /// Length-encoded string (all our strings are < 251 bytes).
    fn lenc_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
        debug_assert!(bytes.len() < 251);
        out.push(bytes.len() as u8);
        out.extend_from_slice(bytes);
    }

    /// HandshakeV10 greeting advertising PROTOCOL_41 | SECURE_CONNECTION | PLUGIN_AUTH with `mysql_native_password`.
    fn handshake_greeting() -> Vec<u8> {
        let mut p = Vec::new();
        p.push(0x0a);
        p.extend_from_slice(b"8.0.99\0");
        p.extend_from_slice(&1u32.to_le_bytes());
        p.extend_from_slice(b"abcdefgh");
        p.push(0x00);
        p.extend_from_slice(&0x8201u16.to_le_bytes());
        p.push(0x21);
        p.extend_from_slice(&0x0002u16.to_le_bytes());
        p.extend_from_slice(&0x0008u16.to_le_bytes());
        p.push(21);
        p.extend_from_slice(&[0u8; 10]);
        p.extend_from_slice(b"ijklmnopqrst\0");
        p.extend_from_slice(b"mysql_native_password\0");
        p
    }

    /// ColumnDefinition41 for a `BIT(16)` column named `b`.
    fn column_definition(
        name: &str,
        character_set: u16,
        column_length: u32,
        column_type: u8,
        flags: u16,
        decimals: u8,
    ) -> Vec<u8> {
        let mut p = Vec::new();
        lenc_bytes(&mut p, b"def");
        lenc_bytes(&mut p, b"");
        lenc_bytes(&mut p, b"t");
        lenc_bytes(&mut p, b"t");
        lenc_bytes(&mut p, name.as_bytes());
        lenc_bytes(&mut p, name.as_bytes());
        p.push(0x0c);
        p.extend_from_slice(&character_set.to_le_bytes());
        p.extend_from_slice(&column_length.to_le_bytes());
        p.push(column_type);
        p.extend_from_slice(&flags.to_le_bytes());
        p.push(decimals);
        p.extend_from_slice(&[0, 0]);
        p
    }

    fn bit16_column_definition() -> Vec<u8> {
        column_definition("b", 63, 16, 0x10, 0x0020, 0)
    }

    /// Serve one connection whose first COM_QUERY returns one `BIT(width)` column holding `values` as raw big-endian bytes.
    fn serve_bit_rows(listener: TcpListener, width: u32, values: Vec<Vec<u8>>) {
        let (mut s, _) = listener.accept().expect("fake MySQL server: accept");
        s.set_read_timeout(Some(FAKE_IO_TIMEOUT)).unwrap();
        s.set_write_timeout(Some(FAKE_IO_TIMEOUT)).unwrap();

        write_packet(&mut s, 0, &handshake_greeting());
        let _handshake_response = read_packet(&mut s);
        write_packet(&mut s, 2, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);

        let _com_query = read_packet(&mut s);
        write_packet(&mut s, 1, &[0x01]);
        write_packet(
            &mut s,
            2,
            &column_definition("b", 63, width, 0x10, 0x0020, 0),
        );
        write_packet(&mut s, 3, &[0xfe, 0x00, 0x00, 0x02, 0x00]);
        let mut seq = 4u8;
        for v in &values {
            let mut row = Vec::new();
            lenc_bytes(&mut row, v);
            write_packet(&mut s, seq, &row);
            seq += 1;
        }
        write_packet(&mut s, seq, &[0xfe, 0x00, 0x00, 0x02, 0x00]);
        let mut buf = [0u8; 64];
        let _ = s.read(&mut buf);
    }

    /// Read `values` back through the product's batch path from a fake server holding a `BIT(width)` column.
    fn read_bit_column(width: u32, values: Vec<Vec<u8>>) -> (DataType, arrow::array::ArrayRef) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake MySQL server");
        let port = listener.local_addr().expect("local_addr").port();
        let server = std::thread::spawn(move || serve_bit_rows(listener, width, values));
        let opts = OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(port)
            .user(Some("root"))
            .pass(Some(""))
            .prefer_socket(false)
            .max_allowed_packet(Some(16 * 1024 * 1024))
            .tcp_connect_timeout(Some(FAKE_IO_TIMEOUT))
            .read_timeout(Some(FAKE_IO_TIMEOUT))
            .write_timeout(Some(FAKE_IO_TIMEOUT));
        let mut conn = Conn::new(opts).expect("connect to fake MySQL server");
        let rows: Vec<mysql::Row> = {
            let mut result = conn.query_iter("SELECT b FROM t").expect("COM_QUERY");
            let row_set = result.iter().expect("one result set");
            row_set.map(|r| r.expect("row decodes")).collect()
        };
        drop(conn);
        server.join().expect("fake MySQL server thread");
        let columns = rows[0].columns();
        let (schema, arrow_types) = mysql_schema_and_arrow_types(&columns, &ColumnOverrides::new())
            .expect("schema for a BIT column");
        let batch = rows_to_record_batch_typed(&Arc::new(schema), &arrow_types, &rows, None)
            .expect("a BIT value is delivered, never refused");
        (arrow_types[0].clone(), batch.column(0).clone())
    }

    #[test]
    fn a_bit64_is_delivered_unsigned_so_every_value_is_kept() {
        let top = 1u64 << 63;
        let values = [top, u64::MAX, u64::MAX - 1, top - 1, 1, 0];
        let wire = values.iter().map(|v| v.to_be_bytes().to_vec()).collect();
        let (ty, col) = read_bit_column(64, wire);
        assert_eq!(
            ty,
            DataType::UInt64,
            "BIT(64) holds 2^64 values, Int64 holds half"
        );
        let got = col
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("UInt64 column");
        assert_eq!(got.values().to_vec(), values.to_vec());
    }

    #[test]
    fn a_bit_up_to_63_bits_is_still_delivered_as_int64() {
        for (width, value) in [(2u32, 3u64), (16, 0x3132), (63, (1u64 << 63) - 1)] {
            let nbytes = width.div_ceil(8) as usize;
            let wire = vec![value.to_be_bytes()[8 - nbytes..].to_vec()];
            let (ty, col) = read_bit_column(width, wire);
            assert_eq!(ty, DataType::Int64, "BIT({width})");
            let got = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column");
            assert_eq!(got.value(0), value as i64, "BIT({width})");
        }
    }

    /// Serve exactly one connection: handshake, auth OK, then answer the first COM_QUERY with one BIT(16) row whose raw wire bytes are [0x31, 0x32].
    fn serve_one_bit16_query(listener: TcpListener) {
        let (mut s, _) = listener.accept().expect("fake MySQL server: accept");
        s.set_read_timeout(Some(FAKE_IO_TIMEOUT)).unwrap();
        s.set_write_timeout(Some(FAKE_IO_TIMEOUT)).unwrap();

        write_packet(&mut s, 0, &handshake_greeting());
        let _handshake_response = read_packet(&mut s);
        write_packet(&mut s, 2, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);

        let _com_query = read_packet(&mut s);
        write_packet(&mut s, 1, &[0x01]);
        write_packet(&mut s, 2, &bit16_column_definition());
        write_packet(&mut s, 3, &[0xfe, 0x00, 0x00, 0x02, 0x00]);
        write_packet(&mut s, 4, &[0x02, 0x31, 0x32]);
        write_packet(&mut s, 5, &[0xfe, 0x00, 0x00, 0x02, 0x00]);
        let mut buf = [0u8; 64];
        let _ = s.read(&mut buf);
    }

    /// Serve one connection whose single result set has `defs.len()` columns and exactly one all-NULL row.
    fn serve_one_metadata_query(listener: TcpListener, defs: Vec<Vec<u8>>) {
        let (mut s, _) = listener.accept().expect("fake MySQL server: accept");
        s.set_read_timeout(Some(FAKE_IO_TIMEOUT)).unwrap();
        s.set_write_timeout(Some(FAKE_IO_TIMEOUT)).unwrap();

        write_packet(&mut s, 0, &handshake_greeting());
        let _handshake_response = read_packet(&mut s);
        write_packet(&mut s, 2, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);

        let _com_query = read_packet(&mut s);
        let n = u8::try_from(defs.len()).expect("fixture stays under the 251-column lenc boundary");
        let mut seq = 1u8;
        write_packet(&mut s, seq, &[n]);
        for def in &defs {
            seq += 1;
            write_packet(&mut s, seq, def);
        }
        seq += 1;
        write_packet(&mut s, seq, &[0xfe, 0x00, 0x00, 0x02, 0x00]);
        seq += 1;
        write_packet(&mut s, seq, &vec![0xfb; defs.len()]);
        seq += 1;
        write_packet(&mut s, seq, &[0xfe, 0x00, 0x00, 0x02, 0x00]);

        let mut buf = [0u8; 64];
        let _ = s.read(&mut buf);
    }

    /// One BINARY-protocol row packet: header, null bitmap, then the non-NULL values already encoded in their per-type binary form.
    fn binary_row_packet(cells: &[Option<Vec<u8>>]) -> Vec<u8> {
        let n = cells.len();
        let mut p = vec![0x00u8];
        let mut bitmap = vec![0u8; (n + 9) / 8];
        for (i, c) in cells.iter().enumerate() {
            if c.is_none() {
                bitmap[(i + 2) / 8] |= 1 << ((i + 2) % 8);
            }
        }
        p.extend_from_slice(&bitmap);
        for c in cells.iter().flatten() {
            p.extend_from_slice(c);
        }
        p
    }

    /// Serve one connection speaking the BINARY (prepared-statement) protocol.
    fn serve_one_binary_query(
        listener: TcpListener,
        defs: Vec<Vec<u8>>,
        rows: Vec<Vec<Option<Vec<u8>>>>,
    ) {
        let (mut s, _) = listener.accept().expect("fake MySQL server: accept");
        s.set_read_timeout(Some(FAKE_IO_TIMEOUT)).unwrap();
        s.set_write_timeout(Some(FAKE_IO_TIMEOUT)).unwrap();

        write_packet(&mut s, 0, &handshake_greeting());
        let _handshake_response = read_packet(&mut s);
        write_packet(&mut s, 2, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);

        let ncol = u8::try_from(defs.len()).expect("fixture stays under 251 columns");
        let eof = [0xfe, 0x00, 0x00, 0x02, 0x00];

        loop {
            let cmd = read_packet(&mut s);
            match cmd.first() {
                Some(0x16) => {
                    let mut ok = vec![0x00];
                    ok.extend_from_slice(&1u32.to_le_bytes());
                    ok.extend_from_slice(&u16::from(ncol).to_le_bytes());
                    ok.extend_from_slice(&0u16.to_le_bytes());
                    ok.push(0x00);
                    ok.extend_from_slice(&0u16.to_le_bytes());
                    let mut seq = 1u8;
                    write_packet(&mut s, seq, &ok);
                    for def in &defs {
                        seq += 1;
                        write_packet(&mut s, seq, def);
                    }
                    if !defs.is_empty() {
                        seq += 1;
                        write_packet(&mut s, seq, &eof);
                    }
                }
                Some(0x17) => {
                    let mut seq = 1u8;
                    write_packet(&mut s, seq, &[ncol]);
                    for def in &defs {
                        seq += 1;
                        write_packet(&mut s, seq, def);
                    }
                    seq += 1;
                    write_packet(&mut s, seq, &eof);
                    for r in &rows {
                        assert_eq!(r.len(), defs.len(), "every fixture row must be full width");
                        seq += 1;
                        write_packet(&mut s, seq, &binary_row_packet(r));
                    }
                    seq += 1;
                    write_packet(&mut s, seq, &eof);
                }
                Some(0x01) | None => break,
                Some(0x19) => {}
                Some(_) => write_packet(&mut s, 1, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]),
            }
        }
    }

    /// Fetch real `mysql::Row`s over the BINARY protocol.
    fn fetch_binary_rows(defs: Vec<Vec<u8>>, cells: Vec<Vec<Option<Vec<u8>>>>) -> Vec<mysql::Row> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake MySQL server");
        let port = listener.local_addr().expect("local_addr").port();
        let server = std::thread::spawn(move || serve_one_binary_query(listener, defs, cells));

        let opts = OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(port)
            .user(Some("root"))
            .pass(Some(""))
            .prefer_socket(false)
            .max_allowed_packet(Some(16 * 1024 * 1024))
            .tcp_connect_timeout(Some(FAKE_IO_TIMEOUT))
            .read_timeout(Some(FAKE_IO_TIMEOUT))
            .write_timeout(Some(FAKE_IO_TIMEOUT));
        let mut conn = Conn::new(opts).expect("connect to fake MySQL server");
        let rows: Vec<mysql::Row> = conn
            .exec("SELECT * FROM t", ())
            .expect("binary-protocol resultset decodes");
        drop(conn);
        server.join().expect("fake MySQL server thread");
        rows
    }

    /// Drive a set of column definitions through a real `mysql::Conn` and return the `(native_type_name, rivet_type)` pair rivet derives for each.
    fn map_columns(defs: Vec<Vec<u8>>) -> Vec<(String, RivetType)> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake MySQL server");
        let port = listener.local_addr().expect("local_addr").port();
        let server = std::thread::spawn(move || serve_one_metadata_query(listener, defs));

        let opts = OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(port)
            .user(Some("root"))
            .pass(Some(""))
            .prefer_socket(false)
            .max_allowed_packet(Some(16 * 1024 * 1024))
            .tcp_connect_timeout(Some(FAKE_IO_TIMEOUT))
            .read_timeout(Some(FAKE_IO_TIMEOUT))
            .write_timeout(Some(FAKE_IO_TIMEOUT));
        let mut conn = Conn::new(opts).expect("connect to fake MySQL server");

        let mapped = {
            let mut result = conn.query_iter("SELECT * FROM t").expect("COM_QUERY");
            let row_set = result.iter().expect("one result set");
            let rows: Vec<mysql::Row> = row_set.map(|r| r.expect("row decodes")).collect();
            assert_eq!(rows.len(), 1, "fake server sent exactly one row");
            rows[0]
                .columns()
                .iter()
                .map(|c| (mysql_native_type_name(c), mysql_type_to_rivet(c)))
                .collect()
        };
        drop(conn);
        server.join().expect("fake MySQL server thread");
        mapped
    }

    /// Smoke test for the binary-protocol server.
    #[test]
    fn the_binary_protocol_server_yields_typed_values_not_text() {
        use mysql::consts::ColumnType::*;
        let defs = vec![column_definition("i", 33, 11, MYSQL_TYPE_LONG as u8, 0, 0)];
        let rows = fetch_binary_rows(defs, vec![vec![Some(42i32.to_le_bytes().to_vec())]]);
        assert_eq!(rows.len(), 1, "one row");
        assert_eq!(
            rows[0].as_ref(0),
            Some(&Value::Int(42)),
            "the binary protocol must yield a TYPED Int — text would give Bytes(b\"42\")"
        );
    }

    const F_UNSIGNED: u16 = 32;
    const CS_BIN: u16 = 63;
    const CS_UTF8: u16 = 33;

    fn lenc_of(v: &[u8]) -> Vec<u8> {
        let mut o = Vec::new();
        lenc_bytes(&mut o, v);
        o
    }

    /// `Value::Int` — a signed BIGINT.
    fn v_int(v: i64) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_UTF8, 20, MYSQL_TYPE_LONGLONG as u8, 0, 0),
            v.to_le_bytes().to_vec(),
        )
    }

    /// `Value::UInt` — and the value MUST exceed `i64::MAX` to actually get one.
    fn v_uint(v: u64) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_UTF8, 20, MYSQL_TYPE_LONGLONG as u8, F_UNSIGNED, 0),
            v.to_le_bytes().to_vec(),
        )
    }

    /// `Value::Bytes` — the text-ish fallback every arm also accepts.
    fn v_bytes(v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_UTF8, 255, MYSQL_TYPE_VAR_STRING as u8, 0, 0),
            lenc_of(v),
        )
    }

    /// `Value::Bytes` from a BINARY column (charset 63) — same variant, but the bytes are not text.
    fn v_blob(v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_BIN, 65535, MYSQL_TYPE_BLOB as u8, 0, 0),
            lenc_of(v),
        )
    }

    fn v_float(v: f32) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_UTF8, 12, MYSQL_TYPE_FLOAT as u8, 0, 31),
            v.to_le_bytes().to_vec(),
        )
    }

    fn v_double(v: f64) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_UTF8, 22, MYSQL_TYPE_DOUBLE as u8, 0, 31),
            v.to_le_bytes().to_vec(),
        )
    }

    /// `Value::Date` — the 11-byte DATETIME form (length, year u16, month, day, hour, minute, second, micros u32).
    fn v_date(y: u16, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        let mut b = vec![11u8];
        b.extend_from_slice(&y.to_le_bytes());
        b.extend_from_slice(&[mo, d, h, mi, s]);
        b.extend_from_slice(&us.to_le_bytes());
        (
            column_definition("c", CS_BIN, 26, MYSQL_TYPE_DATETIME as u8, 0, 6),
            b,
        )
    }

    /// `Value::Time` — the 12-byte TIME form (length, sign, days u32, h, m, s, micros u32).
    fn v_time(neg: bool, days: u32, h: u8, m: u8, s: u8, us: u32) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        let mut b = vec![12u8, u8::from(neg)];
        b.extend_from_slice(&days.to_le_bytes());
        b.extend_from_slice(&[h, m, s]);
        b.extend_from_slice(&us.to_le_bytes());
        (
            column_definition("c", CS_BIN, 17, MYSQL_TYPE_TIME as u8, 0, 6),
            b,
        )
    }

    /// Build every case's array from ONE fetch.
    type Cell = (Vec<u8>, Vec<u8>);

    fn arrays_for(cases: &[(&'static str, Cell, DataType, &'static str)]) -> Vec<Arc<dyn Array>> {
        let defs: Vec<Vec<u8>> = cases.iter().map(|(_, (d, _), _, _)| d.clone()).collect();
        let row: Vec<Option<Vec<u8>>> = cases
            .iter()
            .map(|(_, (_, b), _, _)| Some(b.clone()))
            .collect();
        let rows = fetch_binary_rows(defs, vec![row]);
        assert_eq!(rows.len(), 1, "one row");
        cases
            .iter()
            .enumerate()
            .map(|(i, (label, _, dt, _))| {
                build_array(dt, i, &rows, false, "c", None)
                    .unwrap_or_else(|e| panic!("{label}: build_array errored: {e}"))
            })
            .collect()
    }

    /// One fetch for a single case — kept for the assertions that need a lone column (the temporal grid grades raw primitives one type at a time).
    fn one_array(cell: Cell, dt: &DataType, label: &str) -> Arc<dyn Array> {
        let (def, bytes) = cell;
        let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
        assert_eq!(rows.len(), 1, "{label}: one row");
        build_array(dt, 0, &rows, false, "c", None)
            .unwrap_or_else(|e| panic!("{label}: build_array errored: {e}"))
    }

    /// A `uuid` override refuses text that is not a uuid and a non-text value; NULL stays NULL.
    #[test]
    fn a_uuid_override_refuses_a_value_it_cannot_parse_instead_of_nulling_it() {
        let fsb = DataType::FixedSizeBinary(16);
        for (label, code, (def, bytes)) in [
            (
                "text",
                "RIVET_SOURCE_VALUE_UNREPRESENTABLE",
                v_bytes(b"not-a-uuid"),
            ),
            ("int", "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", v_int(7)),
        ] {
            let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
            let err = build_array(&fsb, 0, &rows, false, "u", None).unwrap_err();
            let coded = err
                .downcast_ref::<crate::error::CodedError>()
                .expect("coded");
            assert_eq!(coded.code(), code, "{label}");
            assert!(
                err.to_string().contains("column `u` is declared uuid"),
                "{label}: {err}"
            );
        }
        let rows = fetch_binary_rows(vec![v_bytes(b"").0], vec![vec![None]]);
        let a = build_array(&fsb, 0, &rows, false, "u", None).unwrap();
        assert_eq!((a.len(), a.null_count()), (1, 1));
    }

    /// Every `CellSource` accessor, over the BINARY protocol.
    #[test]
    fn every_cell_source_accessor_reads_the_binary_value_it_should() {
        use crate::source::value_checksum::CellSource;

        let cases: Vec<(&str, Cell)> = vec![
            ("bool_true", v_int(1)),
            ("bool_false", v_int(0)),
            ("i16", v_int(-12345)),
            ("i32", v_int(-2_000_000_000)),
            ("i64", v_int(-9_000_000_000_000_000_000)),
            ("u64", v_uint(18_000_000_000_000_000_000)),
            ("u64_text", v_bytes(b"18000000000000000000")),
            ("f32", v_float(0.5)),
            ("f64", v_double(-1.25)),
            ("f64_text", v_bytes(b"-2.5")),
            ("dec", v_bytes(b"150.05")),
            ("utf8", v_bytes("h\u{e9}llo".as_bytes())),
            ("bin", v_blob(&[0xde, 0xad])),
            ("date_ts", v_date(2026, 6, 23, 10, 0, 0, 123_456)),
            ("time", v_time(false, 0, 13, 45, 30, 123_456)),
        ];
        let col = |name: &str| cases.iter().position(|(n, _)| *n == name).expect("column");
        let defs: Vec<Vec<u8>> = cases.iter().map(|(_, (d, _))| d.clone()).collect();
        let row0: Vec<Option<Vec<u8>>> = cases.iter().map(|(_, (_, b))| Some(b.clone())).collect();
        let row1: Vec<Option<Vec<u8>>> = cases.iter().map(|_| None).collect();
        let rows = fetch_binary_rows(defs, vec![row0, row1]);
        let s = MysqlCellSource { rows: &rows };

        assert_eq!(s.num_rows(), 2);
        assert_eq!(s.boolean(col("bool_true"), 0), Some(true));
        assert_eq!(s.boolean(col("bool_false"), 0), Some(false));
        assert_eq!(s.int16(col("i16"), 0), Some(-12345));
        assert_eq!(s.int32(col("i32"), 0), Some(-2_000_000_000));
        assert_eq!(s.int64(col("i64"), 0), Some(-9_000_000_000_000_000_000));
        assert_eq!(s.uint64(col("u64"), 0), Some(18_000_000_000_000_000_000));
        assert_eq!(
            s.uint64(col("u64_text"), 0),
            Some(18_000_000_000_000_000_000),
            "a text-wire unsigned integer is parsed, never read as BIT bytes"
        );
        assert_eq!(s.float32(col("f32"), 0), Some(0.5));
        assert_eq!(s.float64(col("f64"), 0), Some(-1.25));
        assert_eq!(
            s.float64(col("f64_text"), 0),
            Some(-2.5),
            "a text-wire double"
        );
        assert_eq!(
            s.float32(col("f64"), 0),
            Some(-1.25),
            "a double read as float"
        );
        assert_eq!(
            s.float32(col("f64_text"), 0),
            Some(-2.5),
            "a text-wire float"
        );
        assert_eq!(s.float32(col("i16"), 0), Some(-12345.0), "Int as float32");
        assert_eq!(s.float64(col("i16"), 0), Some(-12345.0), "Int as float64");
        assert_eq!(
            s.float32(col("u64"), 0),
            Some(18_000_000_000_000_000_000_u64 as f32),
            "UInt as float32"
        );
        assert_eq!(
            s.float64(col("u64"), 0),
            Some(18_000_000_000_000_000_000_u64 as f64),
            "UInt as float64"
        );
        assert_eq!(s.decimal128(col("dec"), 0, 2), Some(15005));
        assert_eq!(
            s.utf8(col("utf8"), 0).as_deref(),
            Some("h\u{e9}llo".as_bytes())
        );
        assert_eq!(s.binary(col("bin"), 0).as_deref(), Some(&[0xde, 0xad][..]));
        assert_eq!(s.date32(col("date_ts"), 0), Some(20627));
        assert_eq!(s.ts_micros(col("date_ts"), 0), Some(1_782_208_800_123_456));
        assert_eq!(s.time64_micros(col("time"), 0), Some(49_530_123_456));

        assert_eq!(s.int64(col("i64"), 1), None, "NULL is absent, never 0");
        assert_eq!(s.boolean(col("bool_true"), 1), None);
        assert_eq!(s.float64(col("f64"), 1), None);
        assert_eq!(s.utf8(col("utf8"), 1), None);
        assert_eq!(s.date32(col("date_ts"), 1), None);
        assert_eq!(s.time64_micros(col("time"), 1), None);
    }

    /// The FULL `DataType` x `Value` dispatch grid of `build_array`, over the BINARY protocol.
    #[test]
    fn build_array_covers_every_value_variant_of_every_arrow_arm() {
        use arrow::util::display::array_value_to_string;

        let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
        /// (label, the (column-definition, binary-cell) pair, target type, expected render)
        type GridCase = (&'static str, Cell, DataType, &'static str);
        let cases: Vec<GridCase> = vec![
            ("bool/int", v_int(1), DataType::Boolean, "true"),
            ("bool/int0", v_int(0), DataType::Boolean, "false"),
            ("bool/bytes", v_bytes(b"1"), DataType::Boolean, "true"),
            ("i16/int", v_int(-12345), DataType::Int16, "-12345"),
            ("i16/bytes", v_bytes(b"-42"), DataType::Int16, "-42"),
            (
                "i32/int",
                v_int(-2000000000),
                DataType::Int32,
                "-2000000000",
            ),
            (
                "i32/uint",
                v_uint(2000000000),
                DataType::Int32,
                "2000000000",
            ),
            ("i32/bytes", v_bytes(b"-42"), DataType::Int32, "-42"),
            (
                "i64/int",
                v_int(-9000000000000000000),
                DataType::Int64,
                "-9000000000000000000",
            ),
            ("i64/bytes", v_bytes(b"-42"), DataType::Int64, "-42"),
            (
                "u64/uint",
                v_uint(18000000000000000000),
                DataType::UInt64,
                "18000000000000000000",
            ),
            ("u64/int", v_int(42), DataType::UInt64, "42"),
            (
                "utf8/uint_big",
                v_uint(u64::MAX),
                DataType::Utf8,
                "18446744073709551615",
            ),
            ("u64/bytes", v_bytes(b"123"), DataType::UInt64, "123"),
            ("f32/float", v_float(0.5), DataType::Float32, "0.5"),
            ("f32/double", v_double(1.5), DataType::Float32, "1.5"),
            ("f32/bytes", v_bytes(b"2.5"), DataType::Float32, "2.5"),
            ("f64/double", v_double(-1.25), DataType::Float64, "-1.25"),
            ("f64/float", v_float(0.5), DataType::Float64, "0.5"),
            ("f64/bytes", v_bytes(b"2.5"), DataType::Float64, "2.5"),
            (
                "utf8/bytes",
                v_bytes("h\u{e9}llo".as_bytes()),
                DataType::Utf8,
                "h\u{e9}llo",
            ),
            ("utf8/int", v_int(42), DataType::Utf8, "42"),
            ("utf8/float", v_float(0.5), DataType::Utf8, "0.5"),
            ("utf8/double", v_double(-1.25), DataType::Utf8, "-1.25"),
            (
                "utf8/date",
                v_date(2026, 6, 23, 10, 0, 0, 123_456),
                DataType::Utf8,
                "2026-06-23 10:00:00.123456",
            ),
            (
                "binary/bytes",
                v_blob(&[0xde, 0xad]),
                DataType::Binary,
                "dead",
            ),
            (
                "fsb/bytes16",
                v_blob(&[
                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                    0x0e, 0x0f, 0x10,
                ]),
                DataType::FixedSizeBinary(16),
                "0102030405060708090a0b0c0d0e0f10",
            ),
            (
                "fsb/uuid_text",
                v_bytes(b"01020304-0506-0708-090a-0b0c0d0e0f10"),
                DataType::FixedSizeBinary(16),
                "0102030405060708090a0b0c0d0e0f10",
            ),
            (
                "dec128/bytes",
                v_bytes(b"150.05"),
                DataType::Decimal128(10, 2),
                "150.05",
            ),
            (
                "dec256/bytes",
                v_bytes(b"150.05"),
                DataType::Decimal256(50, 2),
                "150.05",
            ),
            (
                "date32/bytes",
                v_bytes(b"2026-06-23"),
                DataType::Date32,
                "2026-06-23",
            ),
            (
                "ts/bytes",
                v_bytes(b"2026-06-23 10:00:00"),
                ts.clone(),
                "2026-06-23T10:00:00",
            ),
        ];

        let arrays = arrays_for(&cases);
        let mut wrong = Vec::new();
        for ((label, _, _, want), arr) in cases.iter().zip(&arrays) {
            let got = array_value_to_string(arr.as_ref(), 0).expect("renderable");
            if got != *want {
                wrong.push(format!("{label}: want {want:?}, got {got:?}"));
            }
        }
        assert!(
            wrong.is_empty(),
            "build_array diverged from its documented conversion on the BINARY \
             protocol (the one `exec_iter` uses):\n  {}",
            wrong.join("\n  ")
        );
    }

    /// The grid cells whose correct answer is an ERROR, not a value.
    #[test]
    fn build_array_fails_loud_instead_of_wrapping_an_out_of_range_unsigned() {
        for (label, dt) in [
            ("i16", DataType::Int16),
            ("i32", DataType::Int32),
            ("i64", DataType::Int64),
        ] {
            let (def, bytes) = v_uint(u64::MAX);
            let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
            let got = build_array(&dt, 0, &rows, false, "c", None);
            assert!(
                got.is_err(),
                "{label}: u64::MAX must ERROR on a signed builder, not wrap — got {:?}",
                got.map(|a| a.len())
            );
        }

        let (def, bytes) = v_int(-1);
        let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
        let err = build_array(&DataType::UInt64, 0, &rows, false, "c", None)
            .expect_err("a negative into UInt64 is a refusal, not a NULL");
        crate::error::assert_coded(&err, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
    }

    /// DECIMAL text under a `float8` override is the same number when the float prints it back, and refused by code when it would round.
    #[test]
    fn decimal_text_is_a_float8_only_when_the_float_prints_it_back() {
        use arrow::array::AsArray;
        use arrow::datatypes::Float64Type;
        let build = |text: &[u8]| {
            let (def, bytes) = v_bytes(text);
            let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
            build_array(&DataType::Float64, 0, &rows, false, "c", None)
        };
        for (text, want) in [
            (&b"2.50"[..], 2.5),
            (b"-7.00", -7.0),
            (b"12345678901.2345", 12345678901.2345),
            (b"9007199254740992", 9_007_199_254_740_992.0),
            (b"0.1", 0.1),
        ] {
            let a = build(text).unwrap();
            assert_eq!(a.as_primitive::<Float64Type>().value(0), want);
        }
        for text in [
            &b"922337203685477.5807"[..],
            b"9007199254740993",
            b"-9007199254740993",
            b"12345678901234567890123456789012345678",
            b"0.1234567890123456789",
        ] {
            let err = build(text).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5);
            let said = err.to_string();
            assert!(said.contains("column `c` is declared float8"), "{said}");
            assert!(said.contains("Declare `decimal(p,s)`"), "{said}");
        }
    }

    /// An integer under `float4`/`float8` is the same number up to 2^24 / 2^53 and refused by code past it.
    #[test]
    fn an_integer_is_a_float_only_when_the_float_holds_it_exactly() {
        use arrow::array::AsArray;
        use arrow::datatypes::{Float32Type, Float64Type};
        let build = |cell: Cell, dt: &DataType| {
            let (def, bytes) = cell;
            let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
            build_array(dt, 0, &rows, false, "c", None)
        };
        let (m53, m24) = (1i64 << 53, 1i64 << 24);
        for (cell, want) in [
            (v_int(7), 7.0),
            (v_int(-5), -5.0),
            (v_int(0), 0.0),
            (v_int(m53), m53 as f64),
            (v_int(-m53), -m53 as f64),
            (v_uint(m53 as u64), m53 as f64),
        ] {
            let a = build(cell, &DataType::Float64).unwrap();
            assert_eq!(a.as_primitive::<Float64Type>().value(0), want);
        }
        for (cell, want) in [
            (v_int(7), 7.0),
            (v_int(m24), m24 as f32),
            (v_int(-m24), -m24 as f32),
            (v_uint(m24 as u64), m24 as f32),
        ] {
            let a = build(cell, &DataType::Float32).unwrap();
            assert_eq!(a.as_primitive::<Float32Type>().value(0), want);
        }
        let past: Vec<(Cell, DataType)> = vec![
            (v_int(m53 + 1), DataType::Float64),
            (v_int(-m53 - 1), DataType::Float64),
            (v_uint(u64::MAX), DataType::Float64),
            (v_int(m24 + 1), DataType::Float32),
            (v_int(-m24 - 1), DataType::Float32),
            (v_uint(m24 as u64 + 1), DataType::Float32),
        ];
        for (cell, dt) in past {
            let err = build(cell, &dt).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5);
            let said = err.to_string();
            assert!(said.contains("column `c` is declared float"), "{said}");
            assert!(said.contains("Declare `"), "{said}");
        }
    }

    /// A value of a kind a target's arms do not read is refused by code, never appended as NULL.
    #[test]
    fn a_value_no_arm_reads_is_refused_and_a_source_null_stays_null() {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
        let refused: Vec<(&str, Cell, DataType)> = vec![
            ("double>bool", v_double(1.5), DataType::Boolean),
            ("double>i16", v_double(1.5), DataType::Int16),
            ("double>i32", v_double(1.5), DataType::Int32),
            ("double>i64", v_double(1.5), DataType::Int64),
            ("float>i64", v_float(0.5), DataType::Int64),
            ("double>u64", v_double(1.5), DataType::UInt64),
            ("int>u64 negative", v_int(-1), DataType::UInt64),
            ("time>utf8", v_time(false, 0, 10, 20, 30, 0), DataType::Utf8),
            ("time>i64", v_time(false, 0, 10, 20, 30, 0), DataType::Int64),
            (
                "time>f64",
                v_time(false, 0, 10, 20, 30, 0),
                DataType::Float64,
            ),
            ("date>i64", v_date(2026, 6, 23, 0, 0, 0, 0), DataType::Int64),
            (
                "date>f64",
                v_date(2026, 6, 23, 0, 0, 0, 0),
                DataType::Float64,
            ),
            (
                "date>bool",
                v_date(2026, 6, 23, 0, 0, 0, 0),
                DataType::Boolean,
            ),
            ("int>binary", v_int(7), DataType::Binary),
            ("double>binary", v_double(1.5), DataType::Binary),
            ("int>date32", v_int(7), DataType::Date32),
            ("double>date32", v_double(1.5), DataType::Date32),
            ("int>timestamp", v_int(7), ts.clone()),
            ("double>timestamp", v_double(1.5), ts.clone()),
            ("double>dec128", v_double(1.5), DataType::Decimal128(10, 2)),
            ("double>dec256", v_double(1.5), DataType::Decimal256(50, 2)),
        ];
        for (label, (def, bytes), dt) in refused {
            let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
            let err = build_array(&dt, 0, &rows, false, "c", None)
                .err()
                .unwrap_or_else(|| panic!("{label}: must be refused, was built"));
            crate::error::assert_coded(&err, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
            let said = err.to_string();
            assert!(said.contains("column `c` is read as "), "{label}: {said}");
            assert!(said.contains("writing it as NULL"), "{label}: {said}");
        }
        for dt in [
            DataType::Boolean,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Date32,
            ts.clone(),
            DataType::Decimal128(10, 2),
            DataType::Decimal256(50, 2),
        ] {
            let rows = fetch_binary_rows(vec![v_int(0).0], vec![vec![None]]);
            let a = build_array(&dt, 0, &rows, false, "c", None).unwrap();
            assert_eq!((a.len(), a.null_count()), (1, 1), "{dt}");
        }
    }

    /// Text that does not parse is refused by code; a zero date is still NULL.
    #[test]
    fn unparsable_text_is_refused_by_code_and_a_zero_date_stays_null() {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
        let refused: Vec<(&str, Cell, DataType)> = vec![
            ("text>i16", v_bytes(b"abc"), DataType::Int16),
            ("text>i32", v_bytes(b"abc"), DataType::Int32),
            ("text>i64", v_bytes(b"abc"), DataType::Int64),
            ("text>f32", v_bytes(b"abc"), DataType::Float32),
            ("text>f64", v_bytes(b"abc"), DataType::Float64),
            ("text>date32", v_bytes(b"garbage"), DataType::Date32),
            ("text>timestamp", v_bytes(b"garbage"), ts.clone()),
        ];
        for (label, (def, bytes), dt) in refused {
            let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
            let err = build_array(&dt, 0, &rows, false, "c", None).expect_err(label);
            crate::error::assert_coded(&err, "RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5);
        }
        let zero: Vec<(&str, Cell, DataType)> = vec![
            (
                "zero date>date32",
                v_date(0, 0, 0, 0, 0, 0, 0),
                DataType::Date32,
            ),
            ("zero date>timestamp", v_date(0, 0, 0, 0, 0, 0, 0), ts),
        ];
        for (label, (def, bytes), dt) in zero {
            let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
            let a = build_array(&dt, 0, &rows, false, "c", None)
                .unwrap_or_else(|e| panic!("{label}: {e}"));
            assert_eq!((a.len(), a.null_count()), (1, 1), "{label}");
            let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("c", dt.clone(), true)]));
            let counted = nulled_dates(&schema, std::slice::from_ref(&dt), &rows);
            let expect: Vec<(String, u64)> = if label.starts_with("zero date") {
                vec![("c".to_string(), 1)]
            } else {
                Vec::new()
            };
            assert_eq!(counted, expect, "{label}");
        }
    }

    /// A zero or partial date is counted per column, and a valid date, text and NULL are not.
    #[test]
    fn nulled_dates_counts_zero_and_partial_dates_per_column() {
        let z = v_date(0, 0, 0, 0, 0, 0, 0);
        let defs = vec![z.0.clone(), z.0.clone(), v_bytes(b"").0];
        let row = |a: Cell, b: Cell, c: Option<Cell>| vec![Some(a.1), Some(b.1), c.map(|c| c.1)];
        let rows = fetch_binary_rows(
            defs,
            vec![
                row(z.clone(), z.clone(), None),
                row(
                    v_date(2026, 0, 0, 0, 0, 0, 0),
                    v_date(2026, 11, 10, 1, 2, 3, 4),
                    Some(v_bytes(b"x")),
                ),
                row(
                    v_date(2026, 11, 10, 0, 0, 0, 0),
                    v_date(2026, 0, 5, 0, 0, 0, 0),
                    None,
                ),
            ],
        );
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("dt", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        assert_eq!(
            nulled_dates(&schema, &types, &rows),
            vec![("d".to_string(), 2), ("dt".to_string(), 2)]
        );
        assert_eq!(
            zero_date_warning("d", 2),
            "column `d`: 2 MySQL zero or partial date value(s) written as NULL"
        );
    }

    /// The temporal half of the grid, graded on the RAW primitive.
    #[test]
    fn build_array_temporal_grid_converts_to_independently_computed_values() {
        use arrow::array::{Date32Array, Time64MicrosecondArray, TimestampMicrosecondArray};

        let a = one_array(
            v_date(2026, 6, 23, 10, 0, 0, 123_456),
            &DataType::Date32,
            "date32/date",
        );
        let d = a.as_any().downcast_ref::<Date32Array>().expect("Date32");
        assert_eq!(d.value(0), 20627, "Date32 must be days since the epoch");

        let a = one_array(
            v_date(2026, 6, 23, 10, 0, 0, 123_456),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            "ts/date",
        );
        let t = a
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Timestamp");
        assert_eq!(t.value(0), 1_782_208_800_123_456, "micros since the epoch");

        let a = one_array(
            v_time(false, 0, 13, 45, 30, 123_456),
            &DataType::Time64(TimeUnit::Microsecond),
            "time64/time",
        );
        let t = a
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("Time64");
        assert_eq!(t.value(0), 49_530_123_456, "micros since midnight");

        for (neg, days, h, label) in [
            (true, 2, 1, "negative"),
            (false, 34, 22, "838h"),
            (false, 1, 0, "24h"),
        ] {
            let (def, bytes) = v_time(neg, days, h, 0, 0, 1);
            let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
            let err = build_array(
                &DataType::Time64(TimeUnit::Microsecond),
                0,
                &rows,
                false,
                "c",
                None,
            )
            .expect_err(label);
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_SOURCE_VALUE_UNREPRESENTABLE"),
                "{label}: {err:#}"
            );
            let shown = match label {
                "negative" => "TIME -49:00:00.000001 ",
                "838h" => "TIME 838:00:00.000001 ",
                _ => "TIME 24:00:00.000001 ",
            };
            assert!(format!("{err:#}").contains(shown), "{label}: {err:#}");
        }

        let a = one_array(
            v_bytes(b"13:45:30.123456"),
            &DataType::Time64(TimeUnit::Microsecond),
            "time64/bytes",
        );
        let t = a
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("Time64");
        assert_eq!(t.value(0), 49_530_123_456);
    }

    /// The MySQL wire type map, asserted end-to-end against a REAL driver parse.
    #[test]
    fn the_mysql_wire_type_map_is_exhaustive_and_stable() {
        const UNSIGNED: u16 = 32;
        const ENUM_F: u16 = 256;
        const SET_F: u16 = 2048;
        const BIN: u16 = 63;
        const UTF8: u16 = 33;
        let us = RivetTimeUnit::Microsecond;

        // makes a missing or duplicated wire type visible at a glance. rustfmt
        use mysql::consts::ColumnType::*;
        type WireCase = (&'static str, u16, u32, u8, u16, u8, &'static str, RivetType);
        #[rustfmt::skip]
        let cases: Vec<WireCase> = vec![
            ("tiny1",   UTF8, 1,  MYSQL_TYPE_TINY as u8, 0, 0, "tinyint(1)", RivetType::Bool),
            ("tiny",    UTF8, 4,  MYSQL_TYPE_TINY as u8, 0, 0, "tinyint", RivetType::Int16),
            ("tinyu",   UTF8, 3,  MYSQL_TYPE_TINY as u8, UNSIGNED, 0, "tinyint unsigned", RivetType::Int16),
            ("short",   UTF8, 6,  MYSQL_TYPE_SHORT as u8, 0, 0, "smallint", RivetType::Int16),
            ("shortu",  UTF8, 5,  MYSQL_TYPE_SHORT as u8, UNSIGNED, 0, "smallint unsigned", RivetType::Int32),
            ("int24",   UTF8, 9,  MYSQL_TYPE_INT24 as u8, 0, 0, "int", RivetType::Int32),
            ("int24u",  UTF8, 8,  MYSQL_TYPE_INT24 as u8, UNSIGNED, 0, "int unsigned", RivetType::Int64),
            ("long",    UTF8, 11, MYSQL_TYPE_LONG as u8, 0, 0, "int", RivetType::Int32),
            ("longu",   UTF8, 10, MYSQL_TYPE_LONG as u8, UNSIGNED, 0, "int unsigned", RivetType::Int64),
            ("big",     UTF8, 20, MYSQL_TYPE_LONGLONG as u8, 0, 0, "bigint", RivetType::Int64),
            ("bigu",    UTF8, 20, MYSQL_TYPE_LONGLONG as u8, UNSIGNED, 0, "bigint unsigned", RivetType::UInt64),
            ("year",    UTF8, 4,  MYSQL_TYPE_YEAR as u8, 0, 0, "year", RivetType::Int16),
            ("f32",     UTF8, 12, MYSQL_TYPE_FLOAT as u8, 0, 31, "float", RivetType::Float32),
            ("f64",     UTF8, 22, MYSQL_TYPE_DOUBLE as u8, 0, 31, "double", RivetType::Float64),
            ("dec_s",   UTF8, 12, MYSQL_TYPE_NEWDECIMAL as u8, 0, 2, "decimal",
             RivetType::Decimal { precision: 10, scale: 2 }),
            ("dec_u",   UTF8, 11, MYSQL_TYPE_NEWDECIMAL as u8, UNSIGNED, 2, "decimal",
             RivetType::Decimal { precision: 10, scale: 2 }),
            ("dec_i",   UTF8, 11, MYSQL_TYPE_NEWDECIMAL as u8, 0, 0, "decimal",
             RivetType::Decimal { precision: 10, scale: 0 }),
            ("dec_old", UTF8, 11, MYSQL_TYPE_DECIMAL as u8, 0, 0, "decimal",
             RivetType::Decimal { precision: 10, scale: 0 }),
            ("varchar",   UTF8, 255, MYSQL_TYPE_VAR_STRING as u8, 0, 0, "varchar", RivetType::String),
            ("varbinary", BIN,  255, MYSQL_TYPE_VAR_STRING as u8, 0, 0, "varbinary", RivetType::Binary),
            ("char",      UTF8, 10,  MYSQL_TYPE_STRING as u8, 0, 0, "char", RivetType::String),
            ("binary",    BIN,  10,  MYSQL_TYPE_STRING as u8, 0, 0, "binary", RivetType::Binary),
            ("varchar_oid", UTF8, 255, MYSQL_TYPE_VARCHAR as u8, 0, 0, "varchar", RivetType::String),
            ("enum",    UTF8, 10, MYSQL_TYPE_STRING as u8, ENUM_F, 0, "enum", RivetType::Enum),
            ("set",     UTF8, 10, MYSQL_TYPE_STRING as u8, SET_F, 0, "set", RivetType::Enum),
            ("enum_oid", UTF8, 10, MYSQL_TYPE_ENUM as u8, 0, 0, "enum", RivetType::Enum),
            ("set_oid",  UTF8, 10, MYSQL_TYPE_SET as u8, 0, 0, "set", RivetType::Enum),
            ("blob",     BIN,  65535, MYSQL_TYPE_BLOB as u8, 0, 0, "blob", RivetType::Binary),
            ("textblob", UTF8, 65535, MYSQL_TYPE_BLOB as u8, 0, 0, "blob", RivetType::Text),
            ("tinyblob", BIN,  255,   MYSQL_TYPE_TINY_BLOB as u8, 0, 0, "blob", RivetType::Binary),
            ("medblob",  BIN,  16777215, MYSQL_TYPE_MEDIUM_BLOB as u8, 0, 0, "blob", RivetType::Binary),
            ("longblob", UTF8, 4294967295, MYSQL_TYPE_LONG_BLOB as u8, 0, 0, "blob", RivetType::Text),
            ("json",     UTF8, 4294967295, MYSQL_TYPE_JSON as u8, 0, 0, "json", RivetType::Json),
            ("date",      BIN, 10, MYSQL_TYPE_DATE as u8, 0, 0, "date", RivetType::Date),
            ("time",      BIN, 10, MYSQL_TYPE_TIME as u8, 0, 0, "time", RivetType::Time { unit: us }),
            ("time2",     BIN, 10, MYSQL_TYPE_TIME2 as u8, 0, 0, "time", RivetType::Time { unit: us }),
            ("datetime",  BIN, 19, MYSQL_TYPE_DATETIME as u8, 0, 0, "datetime",
             RivetType::Timestamp { unit: us, timezone: None }),
            ("datetime2", BIN, 19, MYSQL_TYPE_DATETIME2 as u8, 0, 0, "datetime",
             RivetType::Timestamp { unit: us, timezone: None }),
            ("ts",        BIN, 19, MYSQL_TYPE_TIMESTAMP as u8, 0, 0, "timestamp",
             RivetType::Timestamp { unit: us, timezone: Some("UTC".into()) }),
            ("ts2",       BIN, 19, MYSQL_TYPE_TIMESTAMP2 as u8, 0, 0, "timestamp",
             RivetType::Timestamp { unit: us, timezone: Some("UTC".into()) }),
            ("bit1",  BIN, 1,  MYSQL_TYPE_BIT as u8, UNSIGNED, 0, "bit(1)", RivetType::Bool),
            ("bit16", BIN, 16, MYSQL_TYPE_BIT as u8, UNSIGNED, 0, "bit", RivetType::Int64),
            ("bit63", BIN, 63, MYSQL_TYPE_BIT as u8, UNSIGNED, 0, "bit", RivetType::Int64),
            ("bit64", BIN, 64, MYSQL_TYPE_BIT as u8, UNSIGNED, 0, "bit", RivetType::UInt64),
        ];

        let defs = cases
            .iter()
            .map(|(name, cs, len, ty, flags, dec, _, _)| {
                column_definition(name, *cs, *len, *ty, *flags, *dec)
            })
            .collect();
        let got = map_columns(defs);
        assert_eq!(got.len(), cases.len(), "one mapping per column definition");

        let mut wrong = Vec::new();
        for ((label, .., want_name, want_type), (got_name, got_type)) in cases.iter().zip(&got) {
            if got_name != want_name || got_type != want_type {
                wrong.push(format!(
                    "{label}: want ({want_name}, {want_type:?}), got ({got_name}, {got_type:?})"
                ));
            }
        }
        assert!(
            wrong.is_empty(),
            "the MySQL wire type map diverged from its spec. Each line is a column \
             the driver parsed from a protocol-shaped packet, mapped by rivet, and \
             compared to a hand-written expectation:\n  {}",
            wrong.join("\n  ")
        );
    }

    #[test]
    fn roast_mysql_bit16_decodes_as_big_endian_bits_not_ascii_digits() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake MySQL server");
        let port = listener.local_addr().expect("local_addr").port();
        let server = std::thread::spawn(move || serve_one_bit16_query(listener));

        let opts = OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(port)
            .user(Some("root"))
            .pass(Some(""))
            .prefer_socket(false)
            .max_allowed_packet(Some(16 * 1024 * 1024))
            .tcp_connect_timeout(Some(FAKE_IO_TIMEOUT))
            .read_timeout(Some(FAKE_IO_TIMEOUT))
            .write_timeout(Some(FAKE_IO_TIMEOUT));
        let mut conn = Conn::new(opts).expect("connect to fake MySQL server");

        let rows: Vec<mysql::Row> = {
            let mut result = conn.query_iter("SELECT b FROM t").expect("COM_QUERY");
            let row_set = result.iter().expect("one result set");
            row_set.map(|r| r.expect("row decodes")).collect()
        };
        drop(conn);
        server.join().expect("fake MySQL server thread");

        assert_eq!(rows.len(), 1, "fake server sent exactly one row");
        let columns = rows[0].columns();
        assert!(
            matches!(columns[0].column_type(), ColumnType::MYSQL_TYPE_BIT),
            "column metadata is MYSQL_TYPE_BIT — the BIT-ness the decoder must consult"
        );
        assert_eq!(
            rows[0].as_ref(0),
            Some(&Value::Bytes(vec![0x31, 0x32])),
            "BIT(16) travels as raw big-endian bytes on the wire"
        );

        let (schema, arrow_types) = mysql_schema_and_arrow_types(&columns, &ColumnOverrides::new())
            .expect("schema for BIT(16) column");
        assert_eq!(
            arrow_types,
            vec![DataType::Int64],
            "BIT(n>1) maps to Int64 — the buggy builder arm"
        );

        let batch = rows_to_record_batch_typed(&Arc::new(schema), &arrow_types, &rows, None)
            .expect("record batch from BIT(16) row");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 column");
        assert!(!arr.is_null(0), "BIT value must not be nulled");
        assert_eq!(
            arr.value(0),
            0x3132,
            "BIT(16) value 0x3132 must decode as big-endian bits (12594); the \
             atoi-first Int64 path reads bytes [0x31, 0x32] as ASCII \"12\" and \
             yields the wrong value 12"
        );
    }

    type FlagCase = (&'static str, Vec<u8>, Option<Vec<u8>>);

    /// Wire cells for a flag read as a boolean: TINYINT(1), a wider TINYINT or an integer under a `bool` override, and BIT.
    fn flag_cases() -> Vec<FlagCase> {
        use mysql::consts::ColumnType::*;
        let tiny1 = column_definition("c", CS_UTF8, 1, MYSQL_TYPE_TINY as u8, 0, 0);
        let tiny1u = column_definition("c", CS_UTF8, 1, MYSQL_TYPE_TINY as u8, F_UNSIGNED, 0);
        let tiny4 = column_definition("c", CS_UTF8, 4, MYSQL_TYPE_TINY as u8, 0, 0);
        let big = column_definition("c", CS_UTF8, 20, MYSQL_TYPE_LONGLONG as u8, 0, 0);
        let bigu = column_definition("c", CS_UTF8, 20, MYSQL_TYPE_LONGLONG as u8, F_UNSIGNED, 0);
        let bit8 = column_definition("c", CS_BIN, 8, MYSQL_TYPE_BIT as u8, F_UNSIGNED, 0);
        let bit1 = column_definition("c", CS_BIN, 1, MYSQL_TYPE_BIT as u8, F_UNSIGNED, 0);
        vec![
            ("tinyint(1) 0", tiny1.clone(), Some(vec![0])),
            ("tinyint(1) 1", tiny1.clone(), Some(vec![1])),
            ("tinyint(1) 2", tiny1.clone(), Some(vec![2])),
            ("tinyint(1) 5", tiny1.clone(), Some(vec![5])),
            ("tinyint(1) -1", tiny1.clone(), Some(vec![0xFF])),
            ("tinyint(1) 127", tiny1.clone(), Some(vec![127])),
            ("tinyint(1) -128", tiny1.clone(), Some(vec![0x80])),
            ("tinyint(1) NULL", tiny1, None),
            ("tinyint(1) unsigned 1", tiny1u.clone(), Some(vec![1])),
            ("tinyint(1) unsigned 255", tiny1u, Some(vec![255])),
            ("tinyint(4) bool 1", tiny4.clone(), Some(vec![1])),
            ("tinyint(4) bool 5", tiny4, Some(vec![5])),
            (
                "bigint bool 1",
                big.clone(),
                Some(1i64.to_le_bytes().to_vec()),
            ),
            ("bigint bool 2", big, Some(2i64.to_le_bytes().to_vec())),
            (
                "bigint unsigned bool max",
                bigu,
                Some(u64::MAX.to_le_bytes().to_vec()),
            ),
            ("bit(1) 0", bit1.clone(), Some(lenc_of(&[0]))),
            ("bit(1) 1", bit1, Some(lenc_of(&[1]))),
            ("bit(8) bool 0", bit8.clone(), Some(lenc_of(&[0]))),
            ("bit(8) bool 1", bit8.clone(), Some(lenc_of(&[1]))),
            ("bit(8) bool 5", bit8.clone(), Some(lenc_of(&[5]))),
            ("bit(8) bool 255", bit8, Some(lenc_of(&[0xFF]))),
        ]
    }

    /// A flag cell built as a boolean column: the delivered value, or the refusal.
    fn build_flag(def: Vec<u8>, cell: Option<Vec<u8>>) -> anyhow::Result<Option<bool>> {
        use arrow::array::BooleanArray;
        let rows = fetch_binary_rows(vec![def], vec![vec![cell]]);
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Boolean, true)]));
        let batch = rows_to_record_batch_typed(&schema, &[DataType::Boolean], &rows, None)?;
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("Boolean");
        Ok((!a.is_null(0)).then(|| a.value(0)))
    }

    /// A text cell on a flag column reads as 0 or 1 only; any other text is refused by code, never delivered as true.
    #[test]
    fn a_text_cell_on_a_flag_column_is_zero_one_or_refused_never_true() {
        use arrow::array::BooleanArray;
        let columns: Arc<[mysql::Column]> =
            Arc::from(vec![mysql::Column::new(ColumnType::MYSQL_TYPE_VAR_STRING)]);
        let flags = |text: &str| {
            let row = mysql_common::row::new_row(vec![Value::Bytes(text.into())], columns.clone());
            super::build_flags(0, &[row], false, "c")
        };
        for (text, want) in [("0", false), ("1", true)] {
            let arr = flags(text).expect(text);
            let arr = arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Boolean");
            assert_eq!(arr.value(0), want, "text {text:?}");
        }
        for text in ["2", "5", "01", "x", "true", ""] {
            let err = flags(text)
                .err()
                .unwrap_or_else(|| panic!("text {text:?}: delivered, not refused"));
            crate::error::assert_coded(&err, "RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5);
            assert!(
                err.to_string().contains("is not a valid bool (0 or 1)"),
                "text {text:?}: {err}"
            );
        }
    }

    /// A boolean flag carries 0 or 1: any other value is refused by code on every wire type that holds one, never delivered as true.
    #[test]
    fn a_flag_outside_zero_and_one_is_refused_by_code_never_delivered_as_true() {
        let refused = [
            "tinyint(1) 2",
            "tinyint(1) 5",
            "tinyint(1) -1",
            "tinyint(1) 127",
            "tinyint(1) -128",
            "tinyint(1) unsigned 255",
            "tinyint(4) bool 5",
            "bigint bool 2",
            "bigint unsigned bool max",
            "bit(8) bool 5",
            "bit(8) bool 255",
        ];
        for (label, def, cell) in flag_cases() {
            let got = build_flag(def, cell);
            if refused.contains(&label) {
                let err = got
                    .err()
                    .unwrap_or_else(|| panic!("{label}: delivered, not refused"));
                crate::error::assert_coded(&err, "RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5);
                let said = err.to_string();
                assert!(said.contains("column `c` holds "), "{label}: {said}");
                assert!(
                    said.contains("not a flag: a boolean column carries 0 or 1 only"),
                    "{label}: {said}"
                );
                assert!(
                    said.contains("declare the column an integer type"),
                    "{label}: {said}"
                );
            } else {
                let want = match label.rsplit(' ').next().unwrap_or_default() {
                    "NULL" => None,
                    "0" => Some(false),
                    "1" => Some(true),
                    other => panic!("{label}: {other}"),
                };
                assert_eq!(got.expect(label), want, "{label}");
            }
        }
    }

    /// The checksum side and the delivery side read a flag cell by one rule: a cell delivery refuses is no flag to the checksum, and a delivered flag is the checksummed one.
    #[test]
    fn a_flag_cell_is_one_value_to_the_checksum_and_to_the_delivery() {
        use crate::source::value_checksum::CellSource;
        use arrow::array::BooleanArray;
        let text = |t: &str| Value::Bytes(t.into());
        let cells: Vec<(&str, Value, ColumnType, Option<bool>)> = vec![
            (
                "text 0",
                text("0"),
                ColumnType::MYSQL_TYPE_VAR_STRING,
                Some(false),
            ),
            (
                "text 1",
                text("1"),
                ColumnType::MYSQL_TYPE_VAR_STRING,
                Some(true),
            ),
            ("text 2", text("2"), ColumnType::MYSQL_TYPE_VAR_STRING, None),
            ("text 5", text("5"), ColumnType::MYSQL_TYPE_VAR_STRING, None),
            (
                "text 01",
                text("01"),
                ColumnType::MYSQL_TYPE_VAR_STRING,
                None,
            ),
            ("text x", text("x"), ColumnType::MYSQL_TYPE_VAR_STRING, None),
            (
                "text empty",
                text(""),
                ColumnType::MYSQL_TYPE_VAR_STRING,
                None,
            ),
            (
                "int 0",
                Value::Int(0),
                ColumnType::MYSQL_TYPE_TINY,
                Some(false),
            ),
            (
                "int 1",
                Value::Int(1),
                ColumnType::MYSQL_TYPE_TINY,
                Some(true),
            ),
            ("int 2", Value::Int(2), ColumnType::MYSQL_TYPE_TINY, None),
            ("int -1", Value::Int(-1), ColumnType::MYSQL_TYPE_TINY, None),
            (
                "uint 255",
                Value::UInt(255),
                ColumnType::MYSQL_TYPE_TINY,
                None,
            ),
            (
                "bit 0",
                Value::Bytes(vec![0]),
                ColumnType::MYSQL_TYPE_BIT,
                Some(false),
            ),
            (
                "bit 1",
                Value::Bytes(vec![1]),
                ColumnType::MYSQL_TYPE_BIT,
                Some(true),
            ),
            (
                "bit 5",
                Value::Bytes(vec![5]),
                ColumnType::MYSQL_TYPE_BIT,
                None,
            ),
        ];
        for (label, value, column_type, delivered) in cells {
            let columns: Arc<[mysql::Column]> = Arc::from(vec![mysql::Column::new(column_type)]);
            let rows = [mysql_common::row::new_row(vec![value], columns)];
            let is_bit = matches!(column_type, ColumnType::MYSQL_TYPE_BIT);
            let checksum = MysqlCellSource { rows: &rows }.boolean(0, 0);
            match super::build_flags(0, &rows, is_bit, "c") {
                Ok(arr) => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .expect("Boolean");
                    assert_eq!(Some(arr.value(0)), delivered, "{label}: delivered");
                    assert_eq!(checksum, delivered, "{label}: checksum");
                }
                Err(_) => {
                    assert_eq!(
                        delivered, None,
                        "{label}: delivery refused an accepted flag"
                    );
                    assert_eq!(
                        checksum, None,
                        "{label}: checksum read a flag delivery refuses"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod error_code_tests {
    /// A value past the declared column width is refused as unrepresentable (exit 5).
    #[test]
    fn a_value_overflowing_its_declared_width_is_refused_by_code() {
        let err = super::narrow::<i32>(1 << 40, "INT").unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5);
    }
    /// A refused cell names the kind of value MySQL sent; NULL is not refused.
    #[test]
    fn a_wire_mismatch_names_the_kind_of_value_sent() {
        use arrow::datatypes::DataType;
        use mysql::Value;
        for (v, kind) in [
            (Value::Int(1), "MySQL sends a signed integer"),
            (Value::UInt(1), "MySQL sends an unsigned integer"),
            (Value::Bytes(b"x".to_vec()), "MySQL sends text or bytes"),
            (Value::Double(1.0), "MySQL sends a DOUBLE"),
        ] {
            let err = super::null_or_refuse("c", &DataType::Int32, Some(&v)).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
            assert!(err.to_string().contains(kind), "{kind}: {err}");
        }
        assert!(super::null_or_refuse("c", &DataType::Int32, Some(&Value::NULL)).is_ok());
    }
}

#[cfg(test)]
mod override_builder_tests {
    use super::*;

    /// Every override type either builds on zero rows or is refused by code at the schema stage, never an internal stop.
    #[test]
    fn no_override_type_ends_as_an_internal_stop_on_zero_rows() {
        for ov in [
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
        ] {
            let rivet = crate::types::parse_type_str(ov).unwrap();
            let mapping = TypeMapping::from_source(&SourceColumn::simple("c", "x", true), rivet);
            let dt = mapping
                .arrow_type
                .clone()
                .expect("override has an Arrow type");
            let probe = build_array(&dt, 0, &[], false, "c", None);
            let verdict = crate::source::refuse_unbuildable_override(
                "mysql",
                "c",
                &mapping.rivet_type,
                probe,
            );
            let refused = ov.ends_with("_ns");
            assert_eq!(verdict.is_err(), refused, "{ov}");
            if let Err(e) = verdict {
                crate::error::assert_coded(&e, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
            }
        }
    }

    /// The schema stage refuses a nanosecond override on a MySQL column by code, and builds the microsecond one and the undeclared column.
    #[test]
    fn the_mysql_schema_stage_refuses_a_nanosecond_override() {
        use mysql::consts::ColumnType;
        let columns = [mysql::Column::new(ColumnType::MYSQL_TYPE_DATETIME).with_name(b"ts")];
        let declared = |ov: &str| {
            ColumnOverrides::from([("ts".to_string(), crate::types::parse_type_str(ov).unwrap())])
        };
        let err = mysql_schema_and_arrow_types(&columns, &declared("timestamp_ns")).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
        assert!(err.to_string().contains("declare `timestamp`"), "{err}");
        for ok in [declared("timestamp"), ColumnOverrides::new()] {
            let (schema, types) = mysql_schema_and_arrow_types(&columns, &ok).unwrap();
            assert_eq!((schema.fields().len(), types.len()), (1, 1));
        }
    }
}
