//! Always-on value checksum — a cheap, order-independent per-column `xxh3`, cross-checked at two stages.

use std::borrow::Cow;

use arrow::array::types::{
    Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Time64MicrosecondType, TimestampMicrosecondType, UInt64Type,
};
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use xxhash_rust::xxh3::{Xxh3, xxh3_64};

use crate::error::Result;

/// Side B — order-independent per-column checksum over a built batch.
pub fn arrow_batch_checksums(batch: &RecordBatch) -> Vec<u64> {
    batch.columns().iter().map(|c| column_checksum(c)).collect()
}

fn column_checksum(arr: &dyn Array) -> u64 {
    column_xxh3(arr, None)
}

/// Side B for ONE column — the CDC sink verifies per column as it builds (the batch path verifies whole batches via [`arrow_batch_checksums`]).
pub fn array_checksum(arr: &dyn Array) -> u64 {
    column_xxh3(arr, None)
}

/// What the value checksum does with a column of a given Arrow type.
enum CheckRule {
    /// Covered — [`feed_cell`] knows how to hash this type's value bytes.
    Covered,
    /// Not yet matched between the source pass and the Arrow pass.
    Skipped(&'static str),
}

fn check_rule(dt: &DataType) -> CheckRule {
    match dt {
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(..)
        | DataType::Decimal256(..)
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Microsecond, _)
        | DataType::Time64(TimeUnit::Microsecond)
        | DataType::FixedSizeBinary(_)
        | DataType::Boolean
        | DataType::Utf8
        | DataType::Binary => CheckRule::Covered,
        DataType::List(f) if list_elem_covered(f.data_type()) => CheckRule::Covered,
        DataType::List(_) | DataType::LargeList(_) => {
            CheckRule::Skipped("List element type not matched")
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            CheckRule::Skipped("nanosecond timestamp not yet matched")
        }
        _ => CheckRule::Skipped("type not covered by the value checksum"),
    }
}

fn list_elem_covered(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
    )
}

/// One element of a list cell, as its source pass hands it over.
pub enum ListElem {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(Vec<u8>),
}

/// Canonical byte encoding of ONE list cell.
pub fn encode_list_cell(elems: &[ListElem]) -> Vec<u8> {
    let mut out = Vec::with_capacity(elems.len() * 8);
    let mut put = |tag: u8, bytes: &[u8]| {
        out.push(tag);
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    };
    for e in elems {
        match e {
            ListElem::Null => put(0, &[]),
            ListElem::Bool(v) => put(1, &[*v as u8]),
            ListElem::I16(v) => put(2, &v.to_le_bytes()),
            ListElem::I32(v) => put(3, &v.to_le_bytes()),
            ListElem::I64(v) => put(4, &v.to_le_bytes()),
            ListElem::F32(v) => put(5, &v.to_le_bytes()),
            ListElem::F64(v) => put(6, &v.to_le_bytes()),
            ListElem::Str(b) => put(7, b),
        }
    }
    out
}

/// Cell `r` of a `List` array as [`ListElem`]s.
fn list_cell_elems(arr: &dyn Array, r: usize) -> Vec<ListElem> {
    use arrow::array::ListArray;
    let list = arr.as_any().downcast_ref::<ListArray>().expect("List");
    let v = list.value(r);
    let inner = v.as_ref();
    (0..inner.len())
        .map(|i| {
            if inner.is_null(i) {
                return ListElem::Null;
            }
            match inner.data_type() {
                DataType::Boolean => ListElem::Bool(inner.as_boolean().value(i)),
                DataType::Int16 => ListElem::I16(inner.as_primitive::<Int16Type>().value(i)),
                DataType::Int32 => ListElem::I32(inner.as_primitive::<Int32Type>().value(i)),
                DataType::Int64 => ListElem::I64(inner.as_primitive::<Int64Type>().value(i)),
                DataType::Float32 => ListElem::F32(inner.as_primitive::<Float32Type>().value(i)),
                DataType::Float64 => ListElem::F64(inner.as_primitive::<Float64Type>().value(i)),
                DataType::Utf8 => {
                    ListElem::Str(inner.as_string::<i32>().value(i).as_bytes().to_vec())
                }
                _ => ListElem::Null,
            }
        })
        .collect()
}

/// How per-cell hashes are combined into one column checksum.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Fold {
    /// `v1`, XOR.
    Xor,
    /// `v2`, wrapping addition.
    Sum,
}

/// The rendering identity of a manifest's `column_checksums`.
pub const CHECKSUM_RENDER_ID: &str = "xxh3-64-sum-v2";

impl Fold {
    /// The fold that produced a manifest's checksums.
    pub(crate) fn from_render_id(id: Option<&str>) -> Self {
        match id {
            Some(CHECKSUM_RENDER_ID) => Fold::Sum,
            _ => Fold::Xor,
        }
    }

    fn combine(self, acc: u64, h: u64) -> u64 {
        match self {
            Fold::Xor => acc ^ h,
            Fold::Sum => acc.wrapping_add(h),
        }
    }
}

/// The single per-column hasher behind both [`arrow_batch_checksums`] (un-keyed, `key = None`) and [`arrow_batch_checksums_keyed`] (swap-resistant, `key = Some`).
fn column_xxh3(arr: &dyn Array, key: Option<&dyn Array>) -> u64 {
    column_xxh3_with(arr, key, Fold::Sum)
}

pub(crate) fn column_xxh3_with(arr: &dyn Array, key: Option<&dyn Array>, fold: Fold) -> u64 {
    if matches!(check_rule(arr.data_type()), CheckRule::Skipped(_)) {
        return 0;
    }
    let mut acc: u64 = 0;
    for r in 0..arr.len() {
        if arr.is_null(r) {
            continue;
        }
        match key {
            None => acc = fold.combine(acc, with_cell_bytes(arr, r, xxh3_64)),
            Some(k) => {
                let keyed = |kb: &[u8]| {
                    with_cell_bytes(arr, r, |vb| {
                        let mut h = Xxh3::new();
                        h.update(kb);
                        h.update(vb);
                        h.digest()
                    })
                };
                let d = match null_key_width(k, r) {
                    Some(w) => keyed(&vec![0u8; w]),
                    None => with_cell_bytes(k, r, keyed),
                };
                acc = fold.combine(acc, d);
            }
        }
    }
    acc
}

/// Byte width of a NULL fixed-width key cell, hashed as zeros (the writer's `append_null` slot), since a Parquet re-read leaves stale values there.
fn null_key_width(k: &dyn Array, r: usize) -> Option<usize> {
    if !k.is_null(r) {
        return None;
    }
    match k.data_type() {
        DataType::Boolean => Some(1),
        DataType::FixedSizeBinary(n) => usize::try_from(*n).ok(),
        dt => dt.primitive_width(),
    }
}

/// Whether this Arrow type participates in the value checksum (both sides hash it).
pub fn is_covered(dt: &DataType) -> bool {
    matches!(check_rule(dt), CheckRule::Covered)
}

/// The columns the value checksum does NOT cover, each with the reason.
pub fn coverage_skips(schema: &SchemaRef) -> Vec<(String, &'static str)> {
    schema
        .fields()
        .iter()
        .filter_map(|f| match check_rule(f.data_type()) {
            CheckRule::Skipped(reason) => Some((f.name().clone(), reason)),
            CheckRule::Covered => None,
        })
        .collect()
}

/// Keyed side B — the **swap-resistant** variant.
pub fn arrow_batch_checksums_keyed(batch: &RecordBatch, key_col: usize) -> Vec<u64> {
    let key = batch.column(key_col).as_ref();
    batch
        .columns()
        .iter()
        .map(|c| column_xxh3(c.as_ref(), Some(key)))
        .collect()
}

/// Hand cell `r` of `arr` to `f` as its canonical value bytes.
fn with_cell_bytes<R>(arr: &dyn Array, r: usize, f: impl FnOnce(&[u8]) -> R) -> R {
    macro_rules! fp {
        ($t:ty) => {
            f(&arr.as_primitive::<$t>().value(r).to_le_bytes())
        };
    }
    match arr.data_type() {
        DataType::Int16 => fp!(Int16Type),
        DataType::Int32 => fp!(Int32Type),
        DataType::Int64 => fp!(Int64Type),
        DataType::UInt64 => fp!(UInt64Type),
        DataType::Float32 => fp!(Float32Type),
        DataType::Float64 => fp!(Float64Type),
        DataType::Decimal128(..) => fp!(Decimal128Type),
        DataType::Decimal256(..) => f(&arr.as_primitive::<Decimal256Type>().value(r).to_le_bytes()),
        DataType::Date32 => fp!(Date32Type),
        DataType::Timestamp(TimeUnit::Microsecond, _) => fp!(TimestampMicrosecondType),
        DataType::Time64(TimeUnit::Microsecond) => fp!(Time64MicrosecondType),
        DataType::FixedSizeBinary(_) => f(arr.as_fixed_size_binary().value(r)),
        DataType::Boolean => f(&[arr.as_boolean().value(r) as u8]),
        DataType::Utf8 => f(arr.as_string::<i32>().value(r).as_bytes()),
        DataType::Binary => f(arr.as_binary::<i32>().value(r)),
        DataType::List(_) => f(&encode_list_cell(&list_cell_elems(arr, r))),
        _ => f(&[]),
    }
}

/// A per-engine source of typed cell values.
pub trait CellSource {
    fn num_rows(&self) -> usize;
    fn int16(&self, col: usize, row: usize) -> Option<i16>;
    fn int32(&self, col: usize, row: usize) -> Option<i32>;
    fn int64(&self, col: usize, row: usize) -> Option<i64>;
    fn uint64(&self, col: usize, row: usize) -> Option<u64>;
    fn float32(&self, col: usize, row: usize) -> Option<f32>;
    fn float64(&self, col: usize, row: usize) -> Option<f64>;
    fn decimal128(&self, col: usize, row: usize, scale: i8) -> Option<i128>;
    fn date32(&self, col: usize, row: usize) -> Option<i32>;
    fn ts_micros(&self, col: usize, row: usize) -> Option<i64>;
    fn boolean(&self, col: usize, row: usize) -> Option<bool>;
    fn utf8(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>>;
    fn binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>>;
    /// Microseconds since midnight (`TIME` → `Time64(µs)`).
    fn time64_micros(&self, col: usize, row: usize) -> Option<i64>;
    /// The exact bytes of a `FixedSizeBinary` cell (UUID's 16, `BINARY(n)`'s n).
    fn fixed_binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>>;
    /// Unscaled `i256` of a `Decimal256(_, scale)` cell (precision > 38).
    fn decimal256(&self, col: usize, row: usize, scale: i8) -> Option<arrow::datatypes::i256>;
    /// A list cell's elements — encoded via [`encode_list_cell`] by the dispatcher, so engines only extract, never encode.
    fn list(&self, col: usize, row: usize, elem: &DataType) -> Option<Vec<ListElem>>;
}

/// Side A — the per-column checksum computed from a [`CellSource`] (the raw driver values), the single dispatch behind every engine's source pass.
pub fn source_checksums<S: CellSource>(schema: &SchemaRef, src: &S) -> Vec<u64> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col, field)| {
            let dt = field.data_type();
            if matches!(check_rule(dt), CheckRule::Skipped(_)) {
                return 0;
            }
            let mut acc: u64 = 0;
            for row in 0..src.num_rows() {
                macro_rules! le {
                    ($opt:expr) => {
                        if let Some(v) = $opt {
                            acc = acc.wrapping_add(xxh3_64(&v.to_le_bytes()));
                        }
                    };
                }
                match dt {
                    DataType::Int16 => le!(src.int16(col, row)),
                    DataType::Int32 => le!(src.int32(col, row)),
                    DataType::Int64 => le!(src.int64(col, row)),
                    DataType::UInt64 => le!(src.uint64(col, row)),
                    DataType::Float32 => le!(src.float32(col, row)),
                    DataType::Float64 => le!(src.float64(col, row)),
                    DataType::Decimal128(_, scale) => le!(src.decimal128(col, row, *scale)),
                    DataType::Date32 => le!(src.date32(col, row)),
                    DataType::Timestamp(TimeUnit::Microsecond, _) => le!(src.ts_micros(col, row)),
                    DataType::Boolean => {
                        if let Some(b) = src.boolean(col, row) {
                            acc = acc.wrapping_add(xxh3_64(&[b as u8]));
                        }
                    }
                    DataType::Utf8 => {
                        if let Some(b) = src.utf8(col, row) {
                            acc = acc.wrapping_add(xxh3_64(b.as_ref()));
                        }
                    }
                    DataType::Binary => {
                        if let Some(b) = src.binary(col, row) {
                            acc = acc.wrapping_add(xxh3_64(b.as_ref()));
                        }
                    }
                    DataType::Time64(TimeUnit::Microsecond) => {
                        le!(src.time64_micros(col, row))
                    }
                    DataType::FixedSizeBinary(_) => {
                        if let Some(b) = src.fixed_binary(col, row) {
                            acc = acc.wrapping_add(xxh3_64(b.as_ref()));
                        }
                    }
                    DataType::Decimal256(_, scale) => {
                        if let Some(v) = src.decimal256(col, row, *scale) {
                            acc = acc.wrapping_add(xxh3_64(&v.to_le_bytes()));
                        }
                    }
                    DataType::List(f) => {
                        if let Some(elems) = src.list(col, row, f.data_type()) {
                            acc = acc.wrapping_add(xxh3_64(&encode_list_cell(&elems)));
                        }
                    }
                    _ => {}
                }
            }
            acc
        })
        .collect()
}

/// Compare the source-side ("A") and Arrow-side ("B") checksums column-by-column.
pub fn verify(source: &[u64], arrow: &[u64], schema: &SchemaRef) -> Result<()> {
    for (i, (a, b)) in source.iter().zip(arrow.iter()).enumerate() {
        if a != b {
            let col = schema.field(i).name();
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_VALUE_CONVERTER,
                "value checksum mismatch in column '{col}': source={a} arrow={b} \
                 — the value converter changed a value between read and Arrow build"
            );
        }
    }
    Ok(())
}

/// Form B: re-read the exported Parquet parts and recompute the per-column checksum ("side C") BY NAME.
pub fn validate_recorded_checksums(
    recorded: &[crate::manifest::ColumnChecksum],
    part_paths: &[std::path::PathBuf],
    key_col_name: Option<&str>,
    fold: Fold,
) -> Result<Option<String>> {
    let actual = match reread_column_checksums(part_paths, key_col_name, fold)? {
        Ok(actual) => actual,
        Err(detail) => return Ok(Some(detail)),
    };

    for rec in recorded {
        let want: u64 = rec.checksum.parse().map_err(|_| {
            crate::error::coded(
                crate::error::codes::DEST_CONTROL_ARTIFACT_INVALID,
                format!(
                    "value checksum: manifest column '{}' hash '{}' is not a u64",
                    rec.name, rec.checksum
                ),
            )
        })?;
        match actual.get(&rec.name) {
            Some(&got) if got == want => {}
            Some(&got) => {
                return Ok(Some(format!(
                    "value checksum mismatch in column '{}': manifest={want} re-read={got} \
                     — the Parquet differs from what was checksummed at write (Arrow→Parquet \
                     encode fault or post-write corruption)",
                    rec.name
                )));
            }
            None => {
                return Ok(Some(format!(
                    "value checksum: manifest column '{}' was not found when re-reading the exported parts",
                    rec.name
                )));
            }
        }
    }
    Ok(None)
}

/// Re-read Parquet parts and fold per-column checksums by name; `Ok(Err(detail))` when a part will not decode.
pub(crate) fn reread_column_checksums(
    part_paths: &[std::path::PathBuf],
    key_col_name: Option<&str>,
    fold: Fold,
) -> Result<std::result::Result<std::collections::BTreeMap<String, u64>, String>> {
    use std::collections::BTreeMap;

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut actual: BTreeMap<String, u64> = BTreeMap::new();
    for path in part_paths {
        let file = std::fs::File::open(path).map_err(|e| {
            crate::error::coded(
                crate::error::codes::VALIDATE_UNVERIFIED,
                format!("value checksum: open {}: {e}", path.display()),
            )
        })?;
        let reader = match ParquetRecordBatchReaderBuilder::try_new(file).and_then(|b| b.build()) {
            Ok(r) => r,
            Err(e) => {
                return Ok(Err(format!(
                    "value checksum: part {} is not readable as Parquet ({e}) — post-write corruption",
                    path.display()
                )));
            }
        };
        for batch in reader {
            let batch = match batch {
                Ok(b) => b,
                Err(e) => {
                    return Ok(Err(format!(
                        "value checksum: part {} failed to decode ({e}) — post-write corruption",
                        path.display()
                    )));
                }
            };
            let key_col = key_col_name.and_then(|n| batch.schema().index_of(n).ok());
            let key_arr = key_col.map(|k| batch.column(k).clone());
            let sums: Vec<u64> = batch
                .columns()
                .iter()
                .map(|c| column_xxh3_with(c.as_ref(), key_arr.as_deref(), fold))
                .collect();
            for (i, f) in batch.schema().fields().iter().enumerate() {
                let e = actual.entry(f.name().clone()).or_insert(0);
                *e = fold.combine(*e, sums[i]);
            }
        }
    }
    Ok(Ok(actual))
}

/// What a destination-side re-read found wrong, typed so the caller classifies it without sniffing the message.
#[derive(Debug)]
pub enum ReadbackFault {
    /// A per-column value digest disagreed: the bytes are not what was written.
    ValueChecksum(String),
    /// A part's declared row count disagreed: the bytes may be fine and the manifest is wrong.
    PartRowCount(String),
}

/// Each part's DECLARED row count against the rows the file actually holds.
fn part_row_count_mismatch(
    manifest: &crate::manifest::RunManifest,
    paths: &[std::path::PathBuf],
) -> Result<Option<String>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let committed = manifest
        .parts
        .iter()
        .filter(|p| p.status == crate::manifest::PartStatus::Committed);
    for (part, path) in committed.zip(paths) {
        if manifest.format == crate::config::FormatType::Csv.label() {
            let records = crate::format::csv::count_csv_records(path).map_err(|e| {
                crate::error::coded(
                    crate::error::codes::VALIDATE_UNVERIFIED,
                    format!("part row count: read {}: {e}", part.path),
                )
            })?;
            let actual = records.saturating_sub(1) as i64;
            if actual != part.rows {
                return Ok(Some(format!(
                    "part '{}' declares {} rows but holds {actual} CSV records after the header",
                    part.path, part.rows
                )));
            }
            continue;
        }
        let file = std::fs::File::open(path).map_err(|e| {
            crate::error::coded(
                crate::error::codes::VALIDATE_UNVERIFIED,
                format!("part row count: open {}: {e}", part.path),
            )
        })?;
        let actual = match SerializedFileReader::new(file) {
            Ok(r) => r.metadata().file_metadata().num_rows(),
            Err(e) => {
                return Ok(Some(format!(
                    "part '{}' is present at its declared size but does not decode \
                     as the Parquet the manifest committed ({e}) — the bytes are \
                     corrupt, not missing",
                    part.path
                )));
            }
        };
        let declared = part.rows;
        if actual != declared {
            return Ok(Some(format!(
                "part '{}' declares {declared} rows but holds {actual} — the manifest \
                 disagrees with the data it describes (every consumer that SUMS these \
                 numbers, the loader's expected_delta and reconcile's exported side, \
                 inherits the error)",
                part.path
            )));
        }
    }
    Ok(None)
}

/// Form B at the destination: read the manifest + every part via `dest`, materialise each part to a temp file (a part may be remote), and verify the re-read per-column checksums against the manifest's `column_checksums`.
pub fn validate_manifest_checksums(
    dest: &dyn crate::destination::Destination,
    prefix: &str,
) -> Result<Option<ReadbackFault>> {
    use crate::manifest::{MANIFEST_FILENAME, RunManifest, join_key};

    let manifest_key = join_key(prefix, MANIFEST_FILENAME);
    let manifest: RunManifest = serde_json::from_slice(&dest.read(&manifest_key)?)?;

    for unit in split_unit_manifests(dest, prefix, manifest)? {
        if let Some(fault) = validate_one_manifest_checksums(dest, prefix, &unit)? {
            return Ok(Some(fault));
        }
    }
    Ok(None)
}

/// The unit manifests whose value-integrity [`validate_manifest_checksums`] must check.
fn split_unit_manifests(
    dest: &dyn crate::destination::Destination,
    prefix: &str,
    canonical: crate::manifest::RunManifest,
) -> Result<Vec<crate::manifest::RunManifest>> {
    use crate::manifest::RunManifest;
    use crate::manifest::census::{ManifestCensus, is_copy_key};
    if canonical.export_family.is_empty() {
        return Ok(vec![canonical]);
    }
    let mut keyed: Vec<(String, RunManifest)> = Vec::new();
    for meta in dest.list_prefix(prefix)? {
        if !is_copy_key(&meta.key) {
            continue;
        }
        if let Ok(bytes) = dest.read(&meta.key)
            && let Ok(m) = serde_json::from_slice::<RunManifest>(&bytes)
        {
            keyed.push((meta.key, m));
        }
    }
    let mut seen = std::collections::BTreeSet::from([canonical.run_id.clone()]);
    let family = canonical.export_family.clone();
    let mut out = vec![canonical];
    for unit in ManifestCensus::new(&keyed).completed_split_units(&family) {
        if seen.insert(unit.manifest.run_id.clone()) {
            out.push(unit.manifest.clone());
        }
    }
    Ok(out)
}

/// Re-read ONE manifest's parts and check per-part row counts + per-column checksums against what THAT manifest recorded.
fn validate_one_manifest_checksums(
    dest: &dyn crate::destination::Destination,
    prefix: &str,
    manifest: &crate::manifest::RunManifest,
) -> Result<Option<ReadbackFault>> {
    use std::io::Write;

    use crate::manifest::join_key;

    let mut tmps: Vec<tempfile::NamedTempFile> = Vec::with_capacity(manifest.parts.len());
    for part in &manifest.parts {
        if part.status != crate::manifest::PartStatus::Committed {
            continue;
        }
        let body = dest.read(&join_key(prefix, &part.path))?;
        let mut tmp = tempfile::NamedTempFile::new()?;
        tmp.write_all(&body)?;
        tmp.flush()?;
        tmps.push(tmp);
    }
    let paths: Vec<std::path::PathBuf> = tmps.iter().map(|t| t.path().to_path_buf()).collect();

    if let Some(detail) = part_row_count_mismatch(manifest, &paths)? {
        return Ok(Some(ReadbackFault::PartRowCount(detail)));
    }

    let Some(recorded) = manifest.column_checksums.as_deref() else {
        return Ok(None);
    };
    if manifest.format == crate::config::FormatType::Csv.label() {
        return Ok(None);
    }

    let fold = Fold::from_render_id(manifest.checksum_render.as_deref());
    Ok(validate_recorded_checksums(
        recorded,
        &paths,
        manifest.checksum_key_column.as_deref(),
        fold,
    )?
    .map(ReadbackFault::ValueChecksum))
}

#[cfg(test)]
mod tests {
    /// The legacy fold is XOR and the current one a wrapping sum.
    #[test]
    fn the_two_folds_combine_as_xor_and_as_a_wrapping_sum() {
        use super::Fold;
        assert_eq!(Fold::Xor.combine(0b1100, 0b1010), 0b0110);
        assert_eq!(Fold::Sum.combine(0b1100, 0b1010), 0b1_0110);
        assert_eq!(Fold::Sum.combine(u64::MAX, 2), 1);
    }

    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};

    use super::*;

    fn schema3() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]))
    }

    fn csv_manifest(parts: Vec<crate::manifest::ManifestPart>) -> crate::manifest::RunManifest {
        use crate::manifest::*;
        RunManifest {
            split_window: None,
            checksum_render: None,
            row_hash: None,
            mode: "batch".into(),
            manifest_version: MANIFEST_VERSION,
            run_id: "r".into(),
            export_name: "e".into(),
            export_family: String::new(),
            started_at: String::new(),
            finished_at: String::new(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: None,
                table: None,
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: String::new(),
            },
            format: "csv".into(),
            compression: "none".into(),
            schema_fingerprint: String::new(),
            row_count: 0,
            part_count: 0,
            parts,
            column_checksums: None,
            checksum_key_column: None,
        }
    }

    fn csv_part(
        path: &str,
        rows: i64,
        status: crate::manifest::PartStatus,
    ) -> crate::manifest::ManifestPart {
        crate::manifest::ManifestPart {
            part_id: 0,
            path: path.into(),
            rows,
            size_bytes: 0,
            content_fingerprint: String::new(),
            content_md5: String::new(),
            status,
        }
    }

    #[test]
    fn csv_part_rows_are_recounted_after_the_header_and_skip_quarantined_parts() {
        use crate::manifest::PartStatus::{Committed, Quarantined};
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.csv");
        std::fs::write(&file, "id,s\n1,\"x\ny\"\n2,z\n").unwrap();
        let paths = vec![file];

        let ok = csv_manifest(vec![
            csv_part("q.csv", 7, Quarantined),
            csv_part("a.csv", 2, Committed),
        ]);
        assert_eq!(part_row_count_mismatch(&ok, &paths).unwrap(), None);

        let lost = csv_manifest(vec![
            csv_part("q.csv", 7, Quarantined),
            csv_part("a.csv", 3, Committed),
        ]);
        assert_eq!(
            part_row_count_mismatch(&lost, &paths).unwrap().as_deref(),
            Some("part 'a.csv' declares 3 rows but holds 2 CSV records after the header")
        );
    }

    /// Fake source cells mirroring `covered_arrays()` row-for-row: 3 rows per column, row 2 null.
    struct FakeCells;

    impl CellSource for FakeCells {
        fn num_rows(&self) -> usize {
            3
        }
        fn int16(&self, _c: usize, r: usize) -> Option<i16> {
            [Some(1), Some(-2), None][r]
        }
        fn int32(&self, _c: usize, r: usize) -> Option<i32> {
            [Some(3), Some(-4), None][r]
        }
        fn int64(&self, _c: usize, r: usize) -> Option<i64> {
            [Some(5), Some(-6), None][r]
        }
        fn uint64(&self, _c: usize, r: usize) -> Option<u64> {
            [Some(u64::MAX), Some(7), None][r]
        }
        fn float32(&self, _c: usize, r: usize) -> Option<f32> {
            [Some(1.5), Some(-2.25), None][r]
        }
        fn float64(&self, _c: usize, r: usize) -> Option<f64> {
            [Some(3.5), Some(-4.75), None][r]
        }
        fn decimal128(&self, _c: usize, r: usize, scale: i8) -> Option<i128> {
            assert_eq!(scale, 2, "fixture declares Decimal128(10,2)");
            [Some(12345), Some(-678), None][r]
        }
        fn date32(&self, _c: usize, r: usize) -> Option<i32> {
            [Some(19000), Some(1), None][r]
        }
        fn ts_micros(&self, _c: usize, r: usize) -> Option<i64> {
            [Some(1_700_000_000_000_000), Some(1), None][r]
        }
        fn boolean(&self, _c: usize, r: usize) -> Option<bool> {
            [Some(true), Some(false), None][r]
        }
        fn utf8(&self, _c: usize, r: usize) -> Option<std::borrow::Cow<'_, [u8]>> {
            ["h\u{e9}llo".as_bytes(), b"", b""]
                .get(r)
                .filter(|_| r < 2)
                .map(|b| std::borrow::Cow::Borrowed(*b))
        }
        fn binary(&self, _c: usize, r: usize) -> Option<std::borrow::Cow<'_, [u8]>> {
            [&[0xde_u8, 0xad][..], &[][..], &[][..]]
                .get(r)
                .filter(|_| r < 2)
                .map(|b| std::borrow::Cow::Borrowed(*b))
        }
        fn time64_micros(&self, _c: usize, r: usize) -> Option<i64> {
            [Some(86_399_999_999), Some(0), None][r]
        }
        fn fixed_binary(&self, _c: usize, r: usize) -> Option<std::borrow::Cow<'_, [u8]>> {
            [&[1_u8, 2, 3, 4][..], &[5, 6, 7, 8][..], &[][..]]
                .get(r)
                .filter(|_| r < 2)
                .map(|b| std::borrow::Cow::Borrowed(*b))
        }
        fn decimal256(&self, _c: usize, r: usize, scale: i8) -> Option<arrow::datatypes::i256> {
            assert_eq!(scale, 10, "fixture declares Decimal256(50,10)");
            [
                Some(arrow::datatypes::i256::from_i128(123)),
                Some(arrow::datatypes::i256::from_i128(-9)),
                None,
            ][r]
        }
        fn list(&self, _c: usize, r: usize, elem: &DataType) -> Option<Vec<ListElem>> {
            assert_eq!(elem, &DataType::Int64, "fixture declares List<Int64>");
            match r {
                0 => Some(vec![ListElem::I64(1), ListElem::I64(2)]),
                1 => Some(vec![]),
                _ => None,
            }
        }
    }

    /// The arrow-side twins of [`FakeCells`], one array per covered type.
    fn covered_arrays() -> (SchemaRef, Vec<arrow::array::ArrayRef>) {
        use arrow::array::{
            BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
            FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Builder,
            ListBuilder, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
            UInt64Array,
        };
        use arrow::datatypes::i256;

        let mut lb = ListBuilder::new(Int64Builder::new());
        lb.values().append_value(1);
        lb.values().append_value(2);
        lb.append(true);
        lb.append(true);
        lb.append(false);
        let list = lb.finish();
        let list_dt = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));

        let arrays: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(Int16Array::from(vec![Some(1), Some(-2), None])),
            Arc::new(Int32Array::from(vec![Some(3), Some(-4), None])),
            Arc::new(Int64Array::from(vec![Some(5), Some(-6), None])),
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(7), None])),
            Arc::new(Float32Array::from(vec![Some(1.5), Some(-2.25), None])),
            Arc::new(Float64Array::from(vec![Some(3.5), Some(-4.75), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(12345), Some(-678), None])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![Some(19000), Some(1), None])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_700_000_000_000_000),
                Some(1),
                None,
            ])),
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(StringArray::from(vec![Some("h\u{e9}llo"), Some(""), None])),
            Arc::new(BinaryArray::from(vec![
                Some(&[0xde_u8, 0xad][..]),
                Some(&[][..]),
                None,
            ])),
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(86_399_999_999),
                Some(0),
                None,
            ])),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![Some([1_u8, 2, 3, 4]), Some([5, 6, 7, 8]), None].into_iter(),
                    4,
                )
                .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::from_i128(123)),
                    Some(i256::from_i128(-9)),
                    None,
                ])
                .with_precision_and_scale(50, 10)
                .unwrap(),
            ),
            Arc::new(list),
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("c_i16", DataType::Int16, true),
            Field::new("c_i32", DataType::Int32, true),
            Field::new("c_i64", DataType::Int64, true),
            Field::new("c_u64", DataType::UInt64, true),
            Field::new("c_f32", DataType::Float32, true),
            Field::new("c_f64", DataType::Float64, true),
            Field::new("c_dec", DataType::Decimal128(10, 2), true),
            Field::new("c_date", DataType::Date32, true),
            Field::new(
                "c_ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("c_bool", DataType::Boolean, true),
            Field::new("c_str", DataType::Utf8, true),
            Field::new("c_bin", DataType::Binary, true),
            Field::new("c_time", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("c_fsb", DataType::FixedSizeBinary(4), true),
            Field::new("c_dec256", DataType::Decimal256(50, 10), true),
            Field::new("c_list", list_dt, true),
        ]));
        (schema, arrays)
    }

    #[test]
    fn source_checksums_match_arrow_side_for_every_covered_type() {
        let (schema, arrays) = covered_arrays();
        let a = source_checksums(&schema, &FakeCells);
        assert_eq!(a.len(), arrays.len(), "one checksum per column");
        for (i, arr) in arrays.iter().enumerate() {
            let b = column_xxh3(arr.as_ref(), None);
            assert_eq!(
                a[i],
                b,
                "column {} ({:?}): source(A) != arrow(B)",
                schema.field(i).name(),
                schema.field(i).data_type()
            );
            assert_ne!(
                a[i],
                0,
                "column {} must actually hash — a deleted match arm folds nothing and yields 0",
                schema.field(i).name()
            );
        }
    }

    #[test]
    fn column_fold_sums_per_cell_hashes_and_never_cancels() {
        use xxhash_rust::xxh3::xxh3_64;
        let expect = xxh3_64(&10_i64.to_le_bytes())
            .wrapping_add(xxh3_64(&20_i64.to_le_bytes()))
            .wrapping_add(xxh3_64(&30_i64.to_le_bytes()));
        let arr = Int64Array::from(vec![10_i64, 20, 30]);
        assert_eq!(column_xxh3(&arr, None), expect, "fold must be wrapping sum");
        let shuffled = Int64Array::from(vec![30_i64, 10, 20]);
        assert_eq!(
            column_xxh3(&shuffled, None),
            expect,
            "the fold must stay order-independent — parts combine in no fixed order"
        );

        let dup = Int64Array::from(vec![7_i64, 7]);
        let other = Int64Array::from(vec![9_i64, 9]);
        let empty = Int64Array::from(Vec::<i64>::new());
        assert_ne!(
            column_xxh3(&dup, None),
            column_xxh3(&empty, None),
            "a column of duplicates must not collapse to the empty column's checksum"
        );
        assert_ne!(
            column_xxh3(&dup, None),
            column_xxh3(&other, None),
            "two columns of duplicated but DIFFERENT values must not share a checksum \
             — under XOR both were 0, which is exactly how a corrupted value hid"
        );
    }

    #[test]
    fn validate_folds_multiple_parts_by_summing_them() {
        let b1 = batch3(2);
        let b2 = batch3(7);
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("part1.parquet");
        let p2 = dir.path().join("part2.parquet");
        write_parquet(&b1, &p1);
        write_parquet(&b2, &p2);
        let s1 = arrow_batch_checksums(&b1);
        let s2 = arrow_batch_checksums(&b2);
        let recorded: Vec<crate::manifest::ColumnChecksum> = s1
            .iter()
            .zip(s2.iter())
            .zip(b1.schema().fields())
            .map(|((a, b), f)| crate::manifest::ColumnChecksum {
                name: f.name().clone(),
                checksum: a.wrapping_add(*b).to_string(),
            })
            .collect();
        let verdict = validate_recorded_checksums(&recorded, &[p1, p2], None, Fold::Sum)
            .expect("two matching parts must not raise an OPERATIONAL error");
        assert_eq!(
            verdict, None,
            "two matching parts must validate clean (Ok(None)) — the cross-part fold \
             is the same wrapping sum the writer used"
        );
    }

    #[test]
    fn list_parity_holds_for_every_covered_element_type() {
        use arrow::array::{
            BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
            ListBuilder, StringBuilder,
        };

        struct ListCells;
        impl CellSource for ListCells {
            fn num_rows(&self) -> usize {
                1
            }
            fn int16(&self, _c: usize, _r: usize) -> Option<i16> {
                None
            }
            fn int32(&self, _c: usize, _r: usize) -> Option<i32> {
                None
            }
            fn int64(&self, _c: usize, _r: usize) -> Option<i64> {
                None
            }
            fn uint64(&self, _c: usize, _r: usize) -> Option<u64> {
                None
            }
            fn float32(&self, _c: usize, _r: usize) -> Option<f32> {
                None
            }
            fn float64(&self, _c: usize, _r: usize) -> Option<f64> {
                None
            }
            fn decimal128(&self, _c: usize, _r: usize, _s: i8) -> Option<i128> {
                None
            }
            fn date32(&self, _c: usize, _r: usize) -> Option<i32> {
                None
            }
            fn ts_micros(&self, _c: usize, _r: usize) -> Option<i64> {
                None
            }
            fn boolean(&self, _c: usize, _r: usize) -> Option<bool> {
                None
            }
            fn utf8(&self, _c: usize, _r: usize) -> Option<std::borrow::Cow<'_, [u8]>> {
                None
            }
            fn binary(&self, _c: usize, _r: usize) -> Option<std::borrow::Cow<'_, [u8]>> {
                None
            }
            fn time64_micros(&self, _c: usize, _r: usize) -> Option<i64> {
                None
            }
            fn fixed_binary(&self, _c: usize, _r: usize) -> Option<std::borrow::Cow<'_, [u8]>> {
                None
            }
            fn decimal256(&self, _c: usize, _r: usize, _s: i8) -> Option<arrow::datatypes::i256> {
                None
            }
            fn list(&self, c: usize, _r: usize, _elem: &DataType) -> Option<Vec<ListElem>> {
                Some(match c {
                    0 => vec![ListElem::Bool(true), ListElem::Bool(false), ListElem::Null],
                    1 => vec![ListElem::I16(7), ListElem::I16(-8)],
                    2 => vec![ListElem::I32(9), ListElem::I32(-10)],
                    3 => vec![ListElem::F32(1.5), ListElem::F32(-2.5)],
                    4 => vec![ListElem::F64(3.5), ListElem::F64(-4.5)],
                    _ => vec![
                        ListElem::Str("h\u{e9}y".as_bytes().to_vec()),
                        ListElem::Null,
                    ],
                })
            }
        }

        let mut b_bool = ListBuilder::new(BooleanBuilder::new());
        b_bool.values().append_value(true);
        b_bool.values().append_value(false);
        b_bool.values().append_null();
        b_bool.append(true);
        let mut b_i16 = ListBuilder::new(Int16Builder::new());
        b_i16.values().append_value(7);
        b_i16.values().append_value(-8);
        b_i16.append(true);
        let mut b_i32 = ListBuilder::new(Int32Builder::new());
        b_i32.values().append_value(9);
        b_i32.values().append_value(-10);
        b_i32.append(true);
        let mut b_f32 = ListBuilder::new(Float32Builder::new());
        b_f32.values().append_value(1.5);
        b_f32.values().append_value(-2.5);
        b_f32.append(true);
        let mut b_f64 = ListBuilder::new(Float64Builder::new());
        b_f64.values().append_value(3.5);
        b_f64.values().append_value(-4.5);
        b_f64.append(true);
        let mut b_str = ListBuilder::new(StringBuilder::new());
        b_str.values().append_value("h\u{e9}y");
        b_str.values().append_null();
        b_str.append(true);

        let arrays: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(b_bool.finish()),
            Arc::new(b_i16.finish()),
            Arc::new(b_i32.finish()),
            Arc::new(b_f32.finish()),
            Arc::new(b_f64.finish()),
            Arc::new(b_str.finish()),
        ];
        let elem_types = [
            DataType::Boolean,
            DataType::Int16,
            DataType::Int32,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
        ];
        let fields: Vec<Field> = elem_types
            .iter()
            .enumerate()
            .map(|(i, dt)| {
                Field::new(
                    format!("l{i}"),
                    DataType::List(Arc::new(Field::new("item", dt.clone(), true))),
                    true,
                )
            })
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        let a = source_checksums(&schema, &ListCells);
        for (i, arr) in arrays.iter().enumerate() {
            let b = column_xxh3(arr.as_ref(), None);
            assert_eq!(
                a[i], b,
                "List<{:?}> column {i}: source(A) != arrow(B)",
                elem_types[i]
            );
            assert_ne!(a[i], 0, "List<{:?}> must actually hash", elem_types[i]);
        }
    }

    #[test]
    fn is_covered_rejects_skipped_types() {
        assert!(is_covered(&DataType::Int64));
        assert!(is_covered(&DataType::Utf8));
        assert!(!is_covered(&DataType::Timestamp(
            TimeUnit::Nanosecond,
            None
        )));
        assert!(!is_covered(&DataType::LargeList(Arc::new(Field::new(
            "item",
            DataType::Int64,
            true
        )))));
    }

    #[test]
    fn check_rule_gates_list_element_types() {
        let covered = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        assert!(
            matches!(check_rule(&covered), CheckRule::Covered),
            "List<Int64> is covered"
        );
        let uncovered = DataType::List(Arc::new(Field::new("item", DataType::Binary, true)));
        match check_rule(&uncovered) {
            CheckRule::Skipped(reason) => assert_eq!(reason, "List element type not matched"),
            CheckRule::Covered => panic!("List<Binary> must be Skipped, got Covered"),
        }
        let large = DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true)));
        match check_rule(&large) {
            CheckRule::Skipped(reason) => assert_eq!(reason, "List element type not matched"),
            CheckRule::Covered => panic!("LargeList must be Skipped, got Covered"),
        }
    }

    #[test]
    fn coverage_skips_names_uncovered_columns_and_they_contribute_zero() {
        use arrow::array::TimestampNanosecondArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let skips = coverage_skips(&schema);
        assert_eq!(
            skips.len(),
            1,
            "exactly the ns-timestamp column is uncovered: {skips:?}"
        );
        assert_eq!(skips[0].0, "t");
        assert!(
            skips[0].1.contains("nanosecond"),
            "reason names the type: {}",
            skips[0].1
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(TimestampNanosecondArray::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let sums = arrow_batch_checksums(&batch);
        assert_ne!(sums[0], 0, "covered Int64 contributes");
        assert_eq!(sums[1], 0, "uncovered ns-timestamp contributes 0");
    }

    #[test]
    fn newly_covered_types_contribute_and_detect_corruption() {
        use arrow::array::{Decimal256Array, FixedSizeBinaryArray, Time64MicrosecondArray};
        use arrow::datatypes::i256;
        let schema = Arc::new(Schema::new(vec![
            Field::new("t", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("u", DataType::FixedSizeBinary(16), true),
            Field::new("d", DataType::Decimal256(50, 10), true),
            Field::new(
                "l",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));
        assert!(
            coverage_skips(&schema).is_empty(),
            "all four newly-covered types must be Covered"
        );
        let mk = |tval: i64, uuid_last: u8, dval: i128, s: &str| {
            use arrow::array::StringBuilder;
            let mut lb = arrow::array::ListBuilder::new(StringBuilder::new())
                .with_field(Arc::new(Field::new("item", DataType::Utf8, true)));
            lb.values().append_value(s);
            lb.values().append_null();
            lb.append(true);
            let mut ub = [7u8; 16];
            ub[15] = uuid_last;
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Time64MicrosecondArray::from(vec![tval])),
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter(vec![ub.to_vec()].into_iter()).unwrap(),
                    ),
                    Arc::new(
                        Decimal256Array::from(vec![i256::from_i128(dval)])
                            .with_precision_and_scale(50, 10)
                            .unwrap(),
                    ),
                    Arc::new(lb.finish()) as Arc<dyn Array>,
                ],
            )
            .unwrap()
        };
        let good = arrow_batch_checksums(&mk(1000, 1, 42, "alpha"));
        for (i, bad) in [
            arrow_batch_checksums(&mk(1001, 1, 42, "alpha")),
            arrow_batch_checksums(&mk(1000, 2, 42, "alpha")),
            arrow_batch_checksums(&mk(1000, 1, 43, "alpha")),
            arrow_batch_checksums(&mk(1000, 1, 42, "alphb")),
        ]
        .into_iter()
        .enumerate()
        {
            assert_ne!(good[i], bad[i], "column {i}: corruption must move the sum");
            assert_ne!(good[i], 0, "column {i}: covered type must contribute");
        }
    }

    #[test]
    fn verify_passes_on_equal() {
        assert!(verify(&[1, 2, 3], &[1, 2, 3], &schema3()).is_ok());
    }

    #[test]
    fn verify_fails_on_mismatch_naming_the_column() {
        let err = verify(&[1, 2, 3], &[1, 9, 3], &schema3()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("column 'b'"),
            "must name the diverged column: {msg}"
        );
        assert!(msg.contains("source=2") && msg.contains("arrow=9"), "{msg}");
    }

    #[test]
    fn checksum_distinguishes_a_changed_value_so_verify_fires() {
        let s = schema3();
        let good = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        let bad = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![1, 999, 3])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        let cg = arrow_batch_checksums(&good);
        let cb = arrow_batch_checksums(&bad);
        assert_eq!(cg[0], cb[0], "the unchanged column must still match");
        assert_ne!(cg[1], cb[1], "the changed column's checksum must diverge");
        assert!(
            verify(&cg, &cb, &s).is_err(),
            "verify must fire when a column diverges"
        );
    }

    #[test]
    fn keyed_checksum_catches_a_value_swap_that_unkeyed_misses() {
        use arrow::array::StringArray;
        let s: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let id = Int64Array::from(vec![1_i64, 2]);
        let original = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(id.clone()),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
            ],
        )
        .unwrap();
        let swapped = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(id),
                Arc::new(StringArray::from(vec!["beta", "alpha"])),
            ],
        )
        .unwrap();

        let uk = arrow_batch_checksums(&original);
        let uk_swapped = arrow_batch_checksums(&swapped);
        assert_eq!(
            uk[1], uk_swapped[1],
            "un-keyed XOR cannot distinguish a same-multiset row swap"
        );

        let k = arrow_batch_checksums_keyed(&original, 0);
        let k_swapped = arrow_batch_checksums_keyed(&swapped, 0);
        assert_ne!(
            k[1], k_swapped[1],
            "keying to the PK must catch a value swap the un-keyed check missed"
        );
        assert_eq!(
            k[0], k_swapped[0],
            "the id column is identical in both batches"
        );
    }

    #[test]
    fn decimal128_checksum_distinguishes_a_scaling_bug() {
        use arrow::array::Decimal128Array;
        let s: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(18, 2),
            true,
        )]));
        let mk = |mid: i128| {
            RecordBatch::try_new(
                s.clone(),
                vec![Arc::new(
                    Decimal128Array::from(vec![1234_i128, mid, -99])
                        .with_precision_and_scale(18, 2)
                        .unwrap(),
                )],
            )
            .unwrap()
        };
        let good = mk(5000);
        let bad = mk(500);
        let cg = arrow_batch_checksums(&good);
        let cb = arrow_batch_checksums(&bad);
        assert_ne!(cg[0], cb[0], "a decimal scaling bug must move the checksum");
        assert!(
            verify(&cg, &cb, &s).is_err(),
            "verify must fire on the decimal divergence"
        );
    }

    fn write_parquet(batch: &RecordBatch, path: &std::path::Path) {
        let file = std::fs::File::create(path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        w.write(batch).unwrap();
        w.close().unwrap();
    }

    fn batch3(b_mid: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema3(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![1, b_mid, 3])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap()
    }

    fn recorded_of(batch: &RecordBatch) -> Vec<crate::manifest::ColumnChecksum> {
        arrow_batch_checksums(batch)
            .iter()
            .zip(batch.schema().fields())
            .map(|(v, f)| crate::manifest::ColumnChecksum {
                name: f.name().clone(),
                checksum: v.to_string(),
            })
            .collect()
    }

    #[test]
    fn keyed_form_b_validates_leading_null_keys_after_a_parquet_reread() {
        use arrow::array::TimestampMicrosecondArray;
        let s: SchemaRef = Arc::new(Schema::new(vec![
            Field::new(
                "key",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            s,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![
                    None,
                    None,
                    Some(5),
                    Some(9),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();
        let recorded: Vec<crate::manifest::ColumnChecksum> = arrow_batch_checksums_keyed(&batch, 0)
            .iter()
            .zip(batch.schema().fields())
            .map(|(v, f)| crate::manifest::ColumnChecksum {
                name: f.name().clone(),
                checksum: v.to_string(),
            })
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.parquet");
        write_parquet(&batch, &p);
        let verdict = validate_recorded_checksums(&recorded, &[p], Some("key"), Fold::Sum).unwrap();
        assert_eq!(
            verdict, None,
            "healthy data with NULL keys first must validate clean"
        );
    }

    #[test]
    fn form_b_validate_passes_on_matching_parts() {
        let batch = batch3(2);
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.parquet");
        write_parquet(&batch, &p);
        let verdict = validate_recorded_checksums(&recorded_of(&batch), &[p], None, Fold::Sum)
            .expect("matching parts must not raise an operational error");
        assert_eq!(
            verdict, None,
            "matching parts must validate clean (Ok(None))"
        );
    }

    #[test]
    fn form_b_validate_fires_on_corrupted_part() {
        let recorded = recorded_of(&batch3(2));
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.parquet");
        write_parquet(&batch3(999), &p);
        let verdict = validate_recorded_checksums(&recorded, &[p], None, Fold::Sum)
            .expect("a corrupted re-read is verified-wrong, not an operational error");
        let detail = verdict.expect("a corrupted re-read must fire a mismatch");
        assert!(
            detail.contains("column 'b'"),
            "must name the diverged column: {detail}"
        );
    }

    #[test]
    fn form_b_operational_error_is_not_a_mismatch() {
        let batch = batch3(2);
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.parquet");
        write_parquet(&batch, &p);
        let mut recorded = recorded_of(&batch);
        recorded[0].checksum = "not-a-u64".into();
        let res = validate_recorded_checksums(&recorded, &[p], None, Fold::Sum);
        assert!(
            res.is_err(),
            "a non-u64 recorded hash is operational (Err/exit 1), not a mismatch: {res:?}"
        );
    }

    fn split_unit(run: &str, name: &str, family: &str) -> crate::manifest::RunManifest {
        use crate::manifest::{
            MANIFEST_VERSION, ManifestDestination, ManifestSource, ManifestStatus, RunManifest,
            SplitWindow,
        };
        RunManifest {
            manifest_version: MANIFEST_VERSION,
            run_id: run.into(),
            export_name: name.into(),
            export_family: family.into(),
            mode: "batch".into(),
            started_at: "t".into(),
            finished_at: "t".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "pg".into(),
                schema: None,
                table: None,
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///x".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0".into(),
            row_count: 0,
            part_count: 0,
            parts: vec![],
            column_checksums: None,
            checksum_render: None,
            checksum_key_column: None,
            row_hash: None,
            split_window: Some(SplitWindow {
                key_column: "id".into(),
                lo: None,
                hi: None,
            }),
        }
    }

    #[test]
    fn split_unit_manifests_folds_every_same_family_split_sibling() {
        use crate::destination::local::LocalDestination;
        use crate::manifest::run_unique_manifest_name;
        let dir = tempfile::tempdir().unwrap();
        for (run, name, fam) in [
            ("r0", "orders#0", "orders"),
            ("r1", "orders#1", "orders"),
            ("rx", "other#0", "other"),
        ] {
            let m = split_unit(run, name, fam);
            std::fs::write(
                dir.path().join(run_unique_manifest_name(run)),
                serde_json::to_vec(&m).unwrap(),
            )
            .unwrap();
        }
        let dest = LocalDestination::new(&crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap();

        let canonical = split_unit("r1", "orders#1", "orders");
        let units = split_unit_manifests(&dest, "", canonical).unwrap();
        let mut runs: Vec<&str> = units.iter().map(|m| m.run_id.as_str()).collect();
        runs.sort_unstable();
        assert_eq!(
            runs,
            vec!["r0", "r1"],
            "value-checksum fold must cover every same-family split unit (canonical + siblings), \
             deduped, and never a foreign family"
        );
    }

    fn local_prefix(dir: &std::path::Path) -> crate::destination::local::LocalDestination {
        crate::destination::local::LocalDestination::new(&crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap()
    }

    fn write_copy(dir: &std::path::Path, m: &crate::manifest::RunManifest) {
        std::fs::write(
            dir.join(crate::manifest::run_unique_manifest_name(&m.run_id)),
            serde_json::to_vec(m).unwrap(),
        )
        .unwrap();
    }

    /// A failed or interrupted attempt of a sibling unit declares parts gc may have collected; the fold skips it.
    #[test]
    fn split_unit_manifests_skips_a_failed_sibling_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let mut failed = split_unit("r0-failed", "orders#0", "orders");
        failed.status = crate::manifest::ManifestStatus::Failed;
        let mut interrupted = split_unit("r0-cut", "orders#0", "orders");
        interrupted.status = crate::manifest::ManifestStatus::Interrupted;
        for m in [
            &failed,
            &interrupted,
            &split_unit("r0", "orders#0", "orders"),
        ] {
            write_copy(dir.path(), m);
        }
        let canonical = split_unit("r1", "orders#1", "orders");
        write_copy(dir.path(), &canonical);
        let units = split_unit_manifests(&local_prefix(dir.path()), "", canonical).unwrap();
        let mut runs: Vec<&str> = units.iter().map(|m| m.run_id.as_str()).collect();
        runs.sort_unstable();
        assert_eq!(runs, vec!["r0", "r1"]);
    }

    /// A plain export's historical same-family copy is not a split unit and is not folded.
    #[test]
    fn split_unit_manifests_leaves_a_plain_exports_history_alone() {
        let dir = tempfile::tempdir().unwrap();
        let mut plain = split_unit("old", "orders", "orders");
        plain.split_window = None;
        write_copy(dir.path(), &plain);
        let canonical = split_unit("new", "orders", "orders");
        write_copy(dir.path(), &canonical);
        let units = split_unit_manifests(&local_prefix(dir.path()), "", canonical).unwrap();
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].run_id, "new");
    }

    /// A family-less canonical is checked alone, whatever else the prefix holds.
    #[test]
    fn split_unit_manifests_of_a_familyless_canonical_is_the_canonical() {
        let dir = tempfile::tempdir().unwrap();
        write_copy(dir.path(), &split_unit("s0", "orders#0", "orders"));
        let canonical = split_unit("legacy", "orders", "");
        let units = split_unit_manifests(&local_prefix(dir.path()), "", canonical).unwrap();
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].run_id, "legacy");
    }
}

#[cfg(test)]
mod error_code_tests {
    use std::sync::Arc;

    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    /// A converter mismatch is internal (exit 6).
    #[test]
    fn a_checksum_mismatch_and_a_malformed_manifest_hash_carry_their_codes() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let err = verify(&[1], &[2], &schema).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_INTERNAL_VALUE_CONVERTER", 6);
        let recorded = [crate::manifest::ColumnChecksum {
            name: "c".into(),
            checksum: "not-a-u64".into(),
        }];
        let err = validate_recorded_checksums(&recorded, &[], None, Fold::Sum).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_DEST_CONTROL_ARTIFACT_INVALID", 5);
    }
}
