//! Post-hoc `__pos` continuity check for CDC outputs.

use crate::destination::Destination;
use crate::error::Result;
use crate::manifest::{MANIFEST_FILENAME, RunManifest, join_key};
use crate::source::cdc::order::{self, Key};

/// Outcome of a `__pos` continuity check over one CDC run's output.
#[derive(Default)]
pub(crate) struct PositionCheck {
    pub parts: usize,
    pub rows: usize,
    pub first: Option<String>,
    pub last: Option<String>,
    /// Out-of-order / unparseable `__pos` findings — empty ⇒ clean.
    pub violations: Vec<String>,
}

impl PositionCheck {
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Parse one `__pos` JSON string into its comparable key; the order itself is owned by [`order`].
fn parse_pos(s: &str) -> Option<Key> {
    order::key(&serde_json::from_str(s).ok()?)
}

/// Read the `__pos` column (Utf8) out of a Parquet part body.
fn read_pos_column(body: Vec<u8>) -> Result<Vec<String>> {
    use std::io::Write;

    use arrow::array::{Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut tmp = tempfile::NamedTempFile::new()?;
    tmp.write_all(&body)?;
    tmp.flush()?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(tmp.reopen()?)?.build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let idx = batch.schema().index_of("__pos").map_err(|_| {
            crate::error::coded(
                crate::error::codes::VALIDATE_UNVERIFIED,
                "part has no __pos column — not a CDC output?".to_string(),
            )
        })?;
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                crate::error::coded(
                    crate::error::codes::VALIDATE_UNVERIFIED,
                    "__pos column is not Utf8".to_string(),
                )
            })?;
        for i in 0..col.len() {
            if col.is_valid(i) {
                out.push(col.value(i).to_string());
            }
        }
    }
    Ok(out)
}

/// Verify `__pos` is monotonically non-decreasing across a CDC run's Parquet parts at `dest`/`prefix`.
pub(crate) fn check_positions(dest: &dyn Destination, prefix: &str) -> Result<PositionCheck> {
    let manifest_key = join_key(prefix, MANIFEST_FILENAME);
    let mut copies: Vec<(String, RunManifest)> = Vec::new();
    for m in dest.list_prefix(prefix)? {
        let rel = m
            .key
            .strip_prefix(prefix)
            .unwrap_or(&m.key)
            .trim_start_matches('/');
        if rel.contains('/') {
            continue;
        }
        if crate::manifest::census::is_copy_key(rel) {
            let parsed: RunManifest = serde_json::from_slice(&dest.read(&m.key)?)?;
            copies.push((m.key, parsed));
        }
    }
    let census = crate::manifest::census::ManifestCensus::new(&copies);
    let mut manifests: Vec<RunManifest> = census
        .succeeded()
        .into_iter()
        .map(|run| run.manifest.clone())
        .collect();
    if manifests.is_empty() && dest.head(&manifest_key)?.is_some() {
        manifests.push(serde_json::from_slice(&dest.read(&manifest_key)?)?);
    }
    if manifests.is_empty() {
        return Ok(PositionCheck::default());
    }

    let (mut parts, mut rows) = (0usize, 0usize);
    let (mut first, mut last): (Option<String>, Option<String>) = (None, None);
    let mut violations = Vec::new();
    for manifest in &manifests {
        let mut items: Vec<(u32, String)> = Vec::new();
        for part in &manifest.parts {
            if part.status != crate::manifest::PartStatus::Committed {
                continue;
            }
            let body = match dest.read(&join_key(prefix, &part.path)) {
                Ok(b) => b,
                Err(e)
                    if format!("{e:#}").contains("NotFound")
                        || format!("{e:#}").contains("No such file") =>
                {
                    violations.push(format!(
                        "declared part '{}' (run {}) is MISSING at the destination: {e:#}",
                        part.path, manifest.run_id
                    ));
                    continue;
                }
                Err(e) => return Err(e),
            };
            match read_pos_column(body) {
                Ok(ps) => items.extend(ps.into_iter().map(|p| (part.part_id, p))),
                Err(e) => violations.push(format!(
                    "declared part '{}' (run {}) does not decode as the Parquet the \
                     manifest committed: {e:#}",
                    part.path, manifest.run_id
                )),
            }
        }
        let (f, l, v) = check_order(&items);
        parts += manifest.parts.len();
        rows += items.len();
        if first.is_none() {
            first = f;
        }
        if l.is_some() {
            last = l;
        }
        violations.extend(v);
    }
    Ok(PositionCheck {
        parts,
        rows,
        first,
        last,
        violations,
    })
}

/// Pure half: the monotonicity invariant over `(part_id, __pos)` in part→row order, lifted out of the IO so it is testable without a destination.
fn check_order(items: &[(u32, String)]) -> (Option<String>, Option<String>, Vec<String>) {
    let mut prev: Option<Key> = None;
    let (mut first, mut last, mut violations) = (None, None, Vec::new());
    for (part_id, raw) in items {
        let Some(key) = parse_pos(raw) else {
            violations.push(format!("part {part_id}: unparseable __pos {raw:?}"));
            continue;
        };
        first.get_or_insert_with(|| raw.clone());
        if prev.as_ref().is_some_and(|p| key < *p) {
            violations.push(format!(
                "part {part_id}: __pos went backwards at {raw:?} (out of log order)"
            ));
        }
        prev = Some(key);
        last = Some(raw.clone());
    }
    (first, last, violations)
}

#[cfg(test)]
mod v016_checkpoint_compat {
    //! A checkpoint written by rivet 0.16 must still LOAD and still yield the same resume coordinates.

    use super::parse_pos;
    use crate::source::cdc::Position;
    use crate::source::cdc::order::{Key, Val};

    const DIR: &str = "tests/fixtures/compat/v0.16";

    fn load(name: &str) -> Position {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(DIR)
            .join(name);
        Position::load(&path)
            .unwrap_or_else(|e| panic!("{name}: a v0.16 checkpoint must still load: {e:#}"))
            .unwrap_or_else(|| panic!("{name}: loader reported the fixture as ABSENT"))
    }

    /// The five decisions a mutation run over this PURE file found ungraded.
    #[test]
    fn anchor_shaped_positions_order_against_event_positions() {
        let anchor = r#"{"file":"binlog.000007","pos":500,"server_uuid":"u","gtid_executed":""}"#;
        let earlier = r#"{"file":"binlog.000007","pos":100}"#;
        let later = r#"{"file":"binlog.000008","pos":4}"#;
        assert!(
            parse_pos(anchor).is_some(),
            "extras must not break the parse"
        );
        assert!(parse_pos(earlier) < parse_pos(anchor));
        assert!(parse_pos(anchor) < parse_pos(later));
        let pos = |lsn: &str| format!(r#"{{"lsn":"{lsn}"}}"#);
        assert!(parse_pos(&pos("0/16B2D48")) < parse_pos(&pos("0/16B2D50")));
        assert!(
            parse_pos(r#"{"lsn":"0000002a000001f80003","pinned":true}"#)
                < parse_pos(r#"{"lsn":"0000002a000002000001"}"#)
        );
        assert!(
            parse_pos(r#"{"_data":"8264AA00000000000129"}"#)
                < parse_pos(r#"{"_data":"8264AB00000000000129"}"#)
        );
    }

    #[test]
    fn the_position_verdict_the_order_check_and_the_lsn_key_all_answer_for_themselves() {
        use super::{PositionCheck, check_order, parse_pos};

        assert!(
            PositionCheck::default().is_ok(),
            "a check with no violations is OK — `-> false` fails every correct export"
        );
        assert!(
            !PositionCheck {
                violations: vec!["part 0: __pos went backwards".into()],
                ..Default::default()
            }
            .is_ok(),
            "a violation means NOT ok — `-> true` reports every backwards jump as a \
             clean run, which is the only thing this check produces"
        );

        let pos = |lsn: &str| format!(r#"{{"lsn":"{lsn}"}}"#);
        let (_, _, v) = check_order(&[(0, pos("0/10")), (0, pos("0/10")), (0, pos("0/20"))]);
        assert!(
            v.is_empty(),
            "two rows at the SAME position are one transaction, not a backwards \
             jump: {v:?}"
        );
        let (first, last, v) = check_order(&[(0, pos("0/20")), (0, pos("0/10"))]);
        assert_eq!(v.len(), 1, "a strictly backwards position IS a violation");
        assert!(v[0].contains("went backwards"));
        assert_eq!(first.as_deref(), Some(pos("0/20").as_str()));
        assert_eq!(last.as_deref(), Some(pos("0/10").as_str()));

        let (_, _, v) = check_order(&[(7, "not json".into())]);
        assert_eq!(v.len(), 1);
        assert!(v[0].contains("part 7") && v[0].contains("unparseable"));

        assert!(parse_pos(&pos("0/FFFFFFFF")) < parse_pos(&pos("1/0")));
        assert!(parse_pos(&pos("0/2")) > parse_pos(&pos("0/1")));
        assert_eq!(
            parse_pos(&pos("1/1FFFFFFFFF")),
            None,
            "a low half wider than 32 bits is not a PostgreSQL LSN; folding it in \
             silently produces an ordering key that means nothing"
        );
    }

    /// The nested-leg skip must actually skip — measured ungraded by BOTH suites.
    #[test]
    fn a_nested_snapshot_legs_manifest_is_not_read_as_this_prefixs_own() {
        use std::sync::Arc;

        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().expect("dest root");
        let root = dir.path().join("cdc");
        std::fs::create_dir_all(root.join("snapshot")).expect("nested leg");

        let write_part = |at: &std::path::Path, vals: &[&str]| {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "__pos",
                DataType::Utf8,
                false,
            )]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vals.to_vec()))],
            )
            .expect("batch");
            let f = std::fs::File::create(at).expect("part file");
            let mut w = ArrowWriter::try_new(f, schema, None).expect("writer");
            w.write(&batch).expect("write");
            w.close().expect("close");
        };
        write_part(
            &root.join("cdc-000000.parquet"),
            &[r#"{"lsn":"0/10"}"#, r#"{"lsn":"0/20"}"#],
        );

        let manifest = |part: &str| {
            serde_json::to_string(&crate::manifest::RunManifest::for_test("r1", &[(part, 2)]))
                .expect("serialize the manifest")
        };

        std::fs::write(
            root.join("manifest-r1.json"),
            manifest("cdc-000000.parquet"),
        )
        .expect("this leg's own copy");
        std::fs::write(
            root.join("snapshot").join("manifest-r9.json"),
            manifest("snap-000000.parquet"),
        )
        .expect("nested leg's copy");

        std::fs::write(
            root.join(crate::manifest::MANIFEST_FILENAME),
            manifest("cdc-000000.parquet"),
        )
        .expect("canonical manifest");

        let dest =
            crate::destination::local::LocalDestination::new(&crate::config::DestinationConfig {
                destination_type: crate::config::DestinationType::Local,
                path: Some(dir.path().to_string_lossy().into_owned()),
                ..Default::default()
            })
            .expect("local destination");
        let got = super::check_positions(&dest, "cdc")
            .expect("a correct export must not fail validation because a nested leg exists");
        assert_eq!(
            got.parts, 1,
            "only THIS prefix's manifest may be adopted; counting the nested leg's \
             makes validate read parts that were never written here"
        );
        assert_eq!(got.rows, 2);
        assert!(
            got.violations.is_empty(),
            "a monotonic single run has no backwards jump: {:?}",
            got.violations
        );
    }

    /// Writes one monotonic `__pos` part and returns a local destination over its root.
    fn dest_with_part(
        part: &str,
    ) -> (
        tempfile::TempDir,
        crate::destination::local::LocalDestination,
    ) {
        use std::sync::Arc;

        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().expect("dest root");
        let root = dir.path().join("cdc");
        std::fs::create_dir_all(&root).expect("prefix");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "__pos",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                r#"{"lsn":"0/10"}"#,
                r#"{"lsn":"0/20"}"#,
            ]))],
        )
        .expect("batch");
        let f = std::fs::File::create(root.join(part)).expect("part file");
        let mut w = ArrowWriter::try_new(f, schema, None).expect("writer");
        w.write(&batch).expect("write");
        w.close().expect("close");
        let dest =
            crate::destination::local::LocalDestination::new(&crate::config::DestinationConfig {
                destination_type: crate::config::DestinationType::Local,
                path: Some(dir.path().to_string_lossy().into_owned()),
                ..Default::default()
            })
            .expect("local destination");
        (dir, dest)
    }

    fn write_manifest(dir: &tempfile::TempDir, name: &str, run: &str, part: &str, ok: bool) {
        let mut m = crate::manifest::RunManifest::for_test(run, &[(part, 2)]);
        if !ok {
            m.status = crate::manifest::ManifestStatus::Failed;
        }
        std::fs::write(
            dir.path().join("cdc").join(name),
            serde_json::to_string(&m).expect("serialize"),
        )
        .expect("manifest");
    }

    /// A failed copy declares a part gc collected; only the successful copy is read.
    #[test]
    fn a_failed_copy_is_not_read_and_a_successful_one_is() {
        let (dir, dest) = dest_with_part("cdc-000000.parquet");
        write_manifest(&dir, "manifest-r1.json", "r1", "cdc-000000.parquet", true);
        write_manifest(&dir, "manifest-r2.json", "r2", "collected.parquet", false);
        let got = super::check_positions(&dest, "cdc").expect("positions");
        assert_eq!((got.parts, got.rows), (1, 2));
        assert!(got.violations.is_empty(), "{:?}", got.violations);
    }

    /// With no successful copy the canonical manifest answers, as before.
    #[test]
    fn with_only_a_failed_copy_the_canonical_manifest_answers() {
        let (dir, dest) = dest_with_part("cdc-000000.parquet");
        write_manifest(&dir, "manifest-r2.json", "r2", "collected.parquet", false);
        write_manifest(&dir, "manifest.json", "r1", "cdc-000000.parquet", true);
        let got = super::check_positions(&dest, "cdc").expect("positions");
        assert_eq!((got.parts, got.rows), (1, 2));
        assert!(got.violations.is_empty(), "{:?}", got.violations);
    }

    #[test]
    fn v016_mysql_checkpoint_still_yields_its_binlog_coordinates() {
        let p = load("mysql_cdc.ckpt");
        let raw = serde_json::to_string(&p.0).expect("serialize");
        assert_eq!(
            parse_pos(&raw),
            Some(Key(vec![Val::Num(4), Val::Num(150_838_422)])),
            "the 0.16 MySQL checkpoint must still resume at the SAME file+pos;              a renamed or newly-required field re-anchors the stream instead"
        );
    }

    #[test]
    fn v016_pg_and_mssql_checkpoints_still_yield_their_lsns() {
        let pg = load("pg_cdc.ckpt");
        let raw = serde_json::to_string(&pg.0).expect("serialize");
        assert_eq!(
            parse_pos(&raw),
            Some(Key(vec![Val::Num(0x1A2B_3C4D)])),
            "the 0.16 PostgreSQL checkpoint must still resume at the same LSN"
        );

        let ms = load("mssql_cdc.ckpt");
        let raw = serde_json::to_string(&ms.0).expect("serialize");
        assert_eq!(
            parse_pos(&raw),
            Some(Key(vec![Val::Text("0000003400001A2B0003".into())])),
            "the 0.16 SQL Server checkpoint must still resume at the same LSN"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pos_ordering_per_engine() {
        assert!(
            parse_pos(r#"{"file":"binlog.000046","pos":100}"#).unwrap()
                < parse_pos(r#"{"file":"binlog.000046","pos":200}"#).unwrap()
        );
        assert!(
            parse_pos(r#"{"file":"binlog.000046","pos":999}"#).unwrap()
                < parse_pos(r#"{"file":"binlog.000047","pos":4}"#).unwrap()
        );
        assert!(
            parse_pos(r#"{"file":"binlog.999999","pos":10}"#).unwrap()
                < parse_pos(r#"{"file":"binlog.1000000","pos":1}"#).unwrap(),
            "ordinal order: .1000000 is newer than .999999 despite lexical order"
        );
        assert_ne!(
            parse_pos(r#"{"file":"binlog","pos":1}"#).unwrap(),
            parse_pos(r#"{"file":"binlog.000001","pos":1}"#).unwrap()
        );
        assert!(parse_pos(r#"{"lsn":"3C/9"}"#).unwrap() < parse_pos(r#"{"lsn":"3C/1A"}"#).unwrap());
        assert!(
            parse_pos(r#"{"lsn":"3C/FFFFFFFF"}"#).unwrap()
                < parse_pos(r#"{"lsn":"3D/0"}"#).unwrap()
        );
    }

    #[test]
    fn roast_mongo_pos_parses_and_orders_by_data_keystring() {
        let a = r#"{"_data":"826A4E0001","rt":"0eFFFFFF"}"#;
        let b = r#"{"_data":"826A4E0002","rt":"0e00"}"#;
        assert!(parse_pos(a).is_some(), "mongo __pos must parse");
        assert!(parse_pos(a).unwrap() < parse_pos(b).unwrap());
    }

    #[test]
    fn malformed_pos_is_none() {
        assert!(parse_pos("not json").is_none());
        assert!(parse_pos(r#"{"nope":1}"#).is_none());
    }

    #[test]
    fn check_order_flags_a_backwards_jump() {
        let items = vec![
            (1, r#"{"lsn":"3C/10"}"#.into()),
            (2, r#"{"lsn":"3C/05"}"#.into()),
        ];
        let (.., violations) = check_order(&items);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("backwards"));
    }

    #[test]
    fn check_order_clean_when_monotonic() {
        let items = vec![
            (1, r#"{"file":"b.000001","pos":100}"#.into()),
            (2, r#"{"file":"b.000002","pos":4}"#.into()),
        ];
        let (first, last, violations) = check_order(&items);
        assert!(violations.is_empty());
        assert!(first.unwrap().contains("100") && last.unwrap().contains("000002"));
    }
}
