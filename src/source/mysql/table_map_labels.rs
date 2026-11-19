//! ENUM and SET labels carried by a binlog TABLE_MAP, for the columns the catalog cannot spell.

use std::sync::Arc;

use mysql::binlog::events::{OptionalMetaExtractor, OptionalMetadataField, TableMapEvent};

use super::folded_name::Spelling;
use crate::source::cdc::value::WireLabels;

/// One ENUM or SET column as its table map describes it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EnumSetColumn {
    pub(crate) name: String,
    pub(crate) charset: Option<u16>,
    pub(crate) labels: Option<Vec<Vec<u8>>>,
}

/// The columns whose catalog label can differ from the stored one: a label outside the Basic Multilingual Plane, or no labels in the table map.
pub(crate) fn lossy_columns(columns: Vec<EnumSetColumn>) -> Vec<WireLabels> {
    columns
        .into_iter()
        .filter_map(|c| {
            let utf8 = c.charset.is_none_or(|id| {
                matches!(
                    Spelling::of_collation(Some(id)),
                    Spelling::Utf8 | Spelling::Unknown
                )
            });
            if !utf8 {
                return None;
            }
            let Some(raw) = c.labels else {
                return Some(WireLabels {
                    column: c.name,
                    labels: None,
                });
            };
            let labels = raw
                .into_iter()
                .map(|b| String::from_utf8(b).ok())
                .collect::<Option<Vec<String>>>()?;
            labels
                .iter()
                .any(|l| l.chars().any(|ch| u32::from(ch) > 0xFFFF))
                .then_some(WireLabels {
                    column: c.name,
                    labels: Some(labels),
                })
        })
        .collect()
}

/// The labels of `tme`'s ENUM and SET columns the catalog cannot be trusted to spell, named by `names`.
pub(crate) fn of(tme: &TableMapEvent<'_>, names: &[String]) -> Option<Arc<[WireLabels]>> {
    let kinds: Vec<(usize, bool)> = (0..names.len())
        .filter_map(|i| match tme.get_column_type(i) {
            Ok(Some(t)) if t.is_enum_or_set_type() => Some((i, t.is_set_type())),
            Ok(_) | Err(_) => None,
        })
        .collect();
    if kinds.is_empty() {
        return None;
    }
    let (mut enums, mut sets) = (None, None);
    for field in tme.iter_optional_meta() {
        match field {
            Ok(OptionalMetadataField::EnumStrValue(v)) => {
                enums = v
                    .iter_values()
                    .map(|e| e.map(|e| e.values().iter().map(|l| l.value_raw().to_vec()).collect()))
                    .collect::<std::io::Result<Vec<Vec<Vec<u8>>>>>()
                    .ok();
            }
            Ok(OptionalMetadataField::SetStrValue(v)) => {
                sets = v
                    .iter_values()
                    .map(|e| e.map(|e| e.values().iter().map(|l| l.value_raw().to_vec()).collect()))
                    .collect::<std::io::Result<Vec<Vec<Vec<u8>>>>>()
                    .ok();
            }
            _ => {}
        }
    }
    let charsets: Vec<Option<u16>> = OptionalMetaExtractor::new(tme.iter_optional_meta())
        .map(|x| {
            x.iter_enum_and_set_charset()
                .take(kinds.len())
                .map(|c| c.ok())
                .collect()
        })
        .unwrap_or_default();
    let (mut next_enum, mut next_set) = (0, 0);
    let columns = kinds
        .iter()
        .enumerate()
        .map(|(k, &(i, set))| {
            let (list, n) = if set {
                (&sets, &mut next_set)
            } else {
                (&enums, &mut next_enum)
            };
            let labels = list.as_ref().and_then(|l| l.get(*n)).cloned();
            *n += 1;
            EnumSetColumn {
                name: names[i].clone(),
                charset: charsets.get(k).copied().flatten(),
                labels,
            }
        })
        .collect();
    let lossy = lossy_columns(columns);
    (!lossy.is_empty()).then(|| lossy.into())
}

/// The column names the table map's optional metadata carries, when the server sent them.
pub(crate) fn column_names(tme: &TableMapEvent<'_>) -> Option<Arc<[String]>> {
    tme.iter_optional_meta().find_map(|f| match f {
        Ok(OptionalMetadataField::ColumnName(names)) => Some(
            names
                .iter_names()
                .filter_map(|n| n.ok())
                .map(|n| String::from_utf8_lossy(n.name_raw()).into_owned())
                .collect(),
        ),
        Ok(_) | Err(_) => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_common::binlog::BinlogCtx;
    use mysql_common::binlog::consts::{BinlogVersion, EventType};
    use mysql_common::binlog::events::FormatDescriptionEvent;
    use mysql_common::io::ParseBuf;
    use mysql_common::proto::MyDeserialize;

    const GRIN: &str = "\u{1F600}";

    fn col(name: &str, charset: Option<u16>, labels: Option<&[&str]>) -> EnumSetColumn {
        EnumSetColumn {
            name: name.into(),
            charset,
            labels: labels.map(|l| l.iter().map(|s| s.as_bytes().to_vec()).collect()),
        }
    }

    fn wire(column: &str, labels: Option<&[&str]>) -> WireLabels {
        WireLabels {
            column: column.into(),
            labels: labels.map(|l| l.iter().map(|s| s.to_string()).collect()),
        }
    }

    #[test]
    fn only_a_column_the_catalog_cannot_spell_is_listed() {
        let got = lossy_columns(vec![
            col("bmp", Some(255), Some(&["a", "\u{43f}", "\u{20ac}"])),
            col("grin", Some(255), Some(&["a", GRIN])),
            col("bare", None, Some(&["x", GRIN])),
            col("unlisted", Some(45), None),
            col("latin1", Some(8), Some(&[GRIN])),
            col("latin1_unlisted", Some(8), None),
            col("gbk", Some(28), Some(&[GRIN])),
        ]);
        assert_eq!(
            got,
            vec![
                wire("grin", Some(&["a", GRIN])),
                wire("bare", Some(&["x", GRIN])),
                wire("unlisted", None),
            ]
        );
        let bad = lossy_columns(vec![EnumSetColumn {
            name: "bad".into(),
            charset: Some(255),
            labels: Some(vec![vec![0xE9], GRIN.as_bytes().to_vec()]),
        }]);
        assert_eq!(bad, Vec::<WireLabels>::new());
    }

    #[test]
    fn a_label_on_the_edge_of_the_basic_multilingual_plane_is_not_listed() {
        let edge = "\u{FFFF}";
        let past = "\u{10000}";
        let got = lossy_columns(vec![
            col("edge", Some(255), Some(&[edge])),
            col("past", Some(255), Some(&[past])),
        ]);
        assert_eq!(got, vec![wire("past", Some(&[past]))]);
    }

    fn lenenc(n: usize) -> Vec<u8> {
        assert!(n < 251);
        vec![n as u8]
    }

    fn tlv(ty: u8, value: Vec<u8>) -> Vec<u8> {
        [vec![ty], lenenc(value.len()), value].concat()
    }

    fn strings(per_column: &[&[&str]]) -> Vec<u8> {
        per_column
            .iter()
            .flat_map(|labels| {
                let mut out = lenenc(labels.len());
                for l in *labels {
                    out.extend(lenenc(l.len()));
                    out.extend(l.as_bytes());
                }
                out
            })
            .collect()
    }

    /// A table map of `kinds` (`'e'` ENUM, `'s'` SET, `'i'` INT) with the optional metadata `optional`.
    fn table_map(kinds: &str, optional: Vec<u8>) -> TableMapEvent<'static> {
        let mut types = Vec::new();
        let mut meta = Vec::new();
        for k in kinds.chars() {
            match k {
                'i' => types.push(3),
                'e' => {
                    types.push(254);
                    meta.extend([247, 1]);
                }
                's' => {
                    types.push(254);
                    meta.extend([248, 1]);
                }
                other => panic!("{other}"),
            }
        }
        let mut raw = vec![7, 0, 0, 0, 0, 0, 0, 0];
        raw.extend([2, b'd', b'b', 0, 1, b't', 0]);
        raw.extend(lenenc(types.len()));
        raw.extend(&types);
        raw.extend(lenenc(meta.len()));
        raw.extend(&meta);
        raw.extend(vec![0u8; types.len().div_ceil(8)]);
        raw.extend(optional);
        let fde = FormatDescriptionEvent::new(BinlogVersion::Version4);
        let ctx = BinlogCtx::new(raw.len(), &fde, EventType::TABLE_MAP_EVENT as u8);
        TableMapEvent::deserialize(ctx, &mut ParseBuf(&raw))
            .expect("a table map")
            .into_owned()
    }

    fn names(n: &[&str]) -> Vec<String> {
        n.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_table_map_lists_the_enum_and_set_columns_the_catalog_cannot_spell() {
        let optional = [
            tlv(6, strings(&[&["a", "b"], &["c", GRIN], &["m"]])),
            tlv(5, strings(&[&["x", GRIN], &["p", "q"]])),
            tlv(10, vec![255]),
        ]
        .concat();
        let tme = table_map("iesese", optional);
        let got = of(&tme, &names(&["id", "e1", "s1", "e2", "s2", "e3"]));
        assert_eq!(
            got.as_deref(),
            Some(
                &[
                    wire("s1", Some(&["x", GRIN])),
                    wire("e2", Some(&["c", GRIN])),
                ][..]
            )
        );
    }

    #[test]
    fn a_table_map_without_labels_lists_each_utf8_column_as_unlisted() {
        let tme = table_map("ies", tlv(10, vec![255]));
        let got = of(&tme, &names(&["id", "e", "s"]));
        assert_eq!(
            got.as_deref(),
            Some(&[wire("e", None), wire("s", None)][..])
        );
        let latin1 = table_map("ie", tlv(10, vec![8]));
        assert_eq!(of(&latin1, &names(&["id", "e"])), None);
    }

    #[test]
    fn a_column_charset_other_than_utf8_is_left_to_the_catalog() {
        let optional = [
            tlv(6, strings(&[&[GRIN], &[GRIN]])),
            tlv(10, vec![255, 1, 8]),
        ]
        .concat();
        let tme = table_map("ee", optional);
        let got = of(&tme, &names(&["a", "b"]));
        assert_eq!(got.as_deref(), Some(&[wire("a", Some(&[GRIN]))][..]));
    }

    #[test]
    fn a_table_map_with_no_enum_or_set_column_or_only_bmp_labels_lists_nothing() {
        let plain = table_map("ii", Vec::new());
        assert_eq!(of(&plain, &names(&["a", "b"])), None);
        let bmp = table_map(
            "es",
            [
                tlv(6, strings(&[&["a", "\u{43f}"]])),
                tlv(5, strings(&[&["x", "y"]])),
                tlv(10, vec![255]),
            ]
            .concat(),
        );
        assert_eq!(of(&bmp, &names(&["e", "s"])), None);
    }

    #[test]
    fn the_column_names_of_a_table_map_are_read_in_order_and_absent_without_them() {
        let named = table_map("ie", tlv(4, strings(&[&["id", "mood"]])[1..].to_vec()));
        assert_eq!(
            column_names(&named).as_deref(),
            Some(&["id".to_string(), "mood".to_string()][..])
        );
        let unnamed = table_map("ie", tlv(10, vec![255]));
        assert_eq!(column_names(&unnamed), None);
        assert_eq!(column_names(&table_map("i", Vec::new())), None);
    }
}
