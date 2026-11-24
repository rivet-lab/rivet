//! A MySQL spatial value is delivered as the hex of its standard WKB, or refused by code.

use super::{BATCH_SRID_REMEDY, is_spatial_native, wkb_hex_of_internal};

const POINT_1_2: &str = "0101000000000000000000f03f0000000000000040";

/// The internal value MySQL stores: a 4-byte little-endian SRID, then the WKB.
fn internal(srid: u32, wkb_hex: &str) -> Vec<u8> {
    let mut v = srid.to_le_bytes().to_vec();
    v.extend(
        wkb_hex
            .as_bytes()
            .chunks(2)
            .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap()),
    );
    v
}

/// SRID 0 drops its prefix and keeps every WKB byte.
#[test]
fn srid_zero_delivers_the_wkb_after_the_prefix() {
    assert_eq!(
        wkb_hex_of_internal(&internal(0, POINT_1_2), BATCH_SRID_REMEDY).unwrap(),
        POINT_1_2
    );
    let empty = "0107000000".to_string() + "00000000";
    assert_eq!(
        wkb_hex_of_internal(&internal(0, &empty), BATCH_SRID_REMEDY).unwrap(),
        empty
    );
}

/// A value with an SRID other than 0 is refused with the remedy; nothing is delivered without its SRS.
#[test]
fn a_nonzero_srid_is_refused_with_its_remedy() {
    for srid in [1, 3857, 4326] {
        let why = wkb_hex_of_internal(&internal(srid, POINT_1_2), BATCH_SRID_REMEDY).unwrap_err();
        assert!(why.contains(&format!("has SRID {srid}")), "{why}");
        assert!(
            why.contains("Declare the column `binary` in `columns:`"),
            "{why}"
        );
        assert!(why.contains("ST_AsBinary(col) and ST_SRID(col)"), "{why}");
    }
}

/// A value that is not SRID plus little-endian WKB is refused, never delivered.
#[test]
fn a_malformed_value_is_refused() {
    for bad in [
        vec![],
        vec![0, 0, 0],
        internal(0, ""),
        internal(0, "0001000000"),
        internal(0, "0100000000"),
        internal(0, "0108000000"),
        internal(0, "01010000"),
    ] {
        assert!(
            wkb_hex_of_internal(&bad, BATCH_SRID_REMEDY).is_err(),
            "{bad:?}"
        );
    }
}

fn column(ty: mysql::consts::ColumnType) -> mysql::Column {
    mysql::Column::new(ty)
        .with_name(b"c")
        .with_character_set(63)
}

/// An undeclared spatial column is read as `hex_wkb` text, one declared in `columns:` as what it declares.
#[test]
fn a_spatial_column_is_text_unless_declared() {
    use crate::types::{ColumnOverrides, META_TEXT_FORM, RivetType};
    let col = column(mysql::consts::ColumnType::MYSQL_TYPE_GEOMETRY);
    let (schema, types) =
        super::mysql_schema_and_arrow_types(std::slice::from_ref(&col), &ColumnOverrides::new())
            .expect("spatial is delivered");
    assert_eq!(types, [arrow::datatypes::DataType::Utf8]);
    assert_eq!(
        schema
            .field(0)
            .metadata()
            .get(META_TEXT_FORM)
            .map(String::as_str),
        Some("hex_wkb")
    );
    let declared = ColumnOverrides::from([("c".to_string(), RivetType::Binary)]);
    let (schema, types) =
        super::mysql_schema_and_arrow_types(std::slice::from_ref(&col), &declared)
            .expect("binary is read");
    assert_eq!(types, [arrow::datatypes::DataType::Binary]);
    assert!(schema.field(0).metadata().get(META_TEXT_FORM).is_none());
}

/// A wire type this engine does not map is still refused by code, so spatial is the only type that left the refusal.
#[test]
fn an_unmapped_wire_type_is_still_refused_by_code() {
    use crate::types::ColumnOverrides;
    let col = column(mysql::consts::ColumnType::MYSQL_TYPE_NULL);
    let err =
        super::mysql_schema_and_arrow_types(std::slice::from_ref(&col), &ColumnOverrides::new())
            .expect_err("a NULL-typed column has no mapping");
    assert_eq!(
        err.downcast_ref::<crate::error::CodedError>()
            .map(|c| c.code()),
        Some("RIVET_TYPE_UNSAFE_MAPPING")
    );
}

/// Every spatial name MySQL's catalog prints is classified, and no other type is.
#[test]
fn the_catalog_names_of_the_spatial_types_are_classified() {
    for n in [
        "geometry",
        "POINT",
        "linestring",
        "Polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geomcollection",
        "geometrycollection",
    ] {
        assert!(is_spatial_native(n), "{n}");
    }
    for n in [
        "blob",
        "varbinary(16)",
        "json",
        "vector(3)",
        "geometry2",
        "",
    ] {
        assert!(!is_spatial_native(n), "{n}");
    }
}
