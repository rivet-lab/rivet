//! Live CDC coverage for the MongoDB change-stream engine.

use crate::common::*;

/// A `mode: cdc` Rig (until_current + checkpoint) over `table` in a fresh db.
fn cdc(db: &str, table: &str) -> Rig {
    Rig::mongo_cdc(table)
        .source_url(&MongoTest::url(mongo_rs_port(), db))
        .duckdb_oracle()
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_capture_resume_and_until_current_drain() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_cap");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok();
    assert!(
        rig.checkpoint().exists(),
        "a fresh checkpointed open must pin its anchor"
    );

    m.upsert_set("t", 1, "v", "a");
    m.upsert_set("t", 2, "v", "b");
    m.upsert_set("t", 3, "v", "c");

    rig.run_ok();
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    assert!(
        ["1", "2", "3"].iter().all(|i| ids.contains(*i)),
        "until_current dropped part of the backlog: got {ids:?}"
    );

    let rig2 = cdc(&db, "t").checkpoint_path(rig.checkpoint());
    rig2.run_ok();
    assert_eq!(
        duckdb_declared_rows(rig2.oracle_dir()),
        0,
        "resume must capture only new changes"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_crash_after_flush_before_ack_re_reads_on_resume() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_crash");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("t");

    let mut rig = cdc(&db, "t");
    rig.run_ok();
    for i in 1..=5 {
        m.upsert_set("t", i, "v", &format!("x{i}"));
    }

    let crashed = rig.run_with_env("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack");
    assert!(
        !crashed.status.success(),
        "the fault hook must crash the run"
    );

    rig.resume_into_fresh_dest();
    rig.run_ok();
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    for i in 1..=5 {
        assert!(ids.contains(&i.to_string()), "id {i} lost across the crash");
    }
    assert!(
        duckdb_declared_rows(rig.oracle_dir()) >= 5,
        "crash + resume must be at-least-once (superset), not lose rows"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_soak_dedup_matches_source_current_state() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_soak");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("s");

    let rig = cdc(&db, "s");
    rig.run_ok();
    for r in 0..200_i64 {
        let id = r % 40;
        m.upsert_set("s", id, "v", &format!("r{r}"));
        if r % 5 == 0 {
            m.txn_updates("s", &[(id, "v", "txn_a"), (id, "v", &format!("t{r}b"))]);
        }
    }

    for _ in 0..3 {
        rig.run_ok();
    }

    let deduped = mongo_deduped_field(read_mongo_cdc_changes(&rig.out_dir()), "v");
    let source = m.current_state_i64("s", "v");
    assert_eq!(
        deduped, source,
        "deduped CDC change log must reproduce the source current state exactly \
         (intra-transaction ordering + no loss)"
    );
    assert_eq!(source.len(), 40, "all 40 _ids present");
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_idle_first_run_then_change_is_captured() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_idle");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok();
    assert!(
        rig.checkpoint().exists(),
        "an idle first run must still pin the anchor"
    );

    m.upsert_set("t", 42, "v", "after_quiet_enable");
    rig.run_ok();
    assert!(
        duckdb_declared_distinct_set(rig.oracle_dir(), "_id").contains("42"),
        "the change made during a quiet first run must be captured on resume"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_update_and_delete_carry_document() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_ud");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok();

    m.upsert_set("t", 7, "v", "created");
    m.upsert_set("t", 7, "v", "updated");
    rig.run_ok();
    m.delete_one("t", 7);
    rig.run_ok();

    let changes = read_mongo_cdc_changes(&rig.out_dir());
    let ops: Vec<&str> = changes.iter().map(|c| c.op.as_str()).collect();
    assert!(ops.contains(&"insert"), "insert op captured: {ops:?}");
    assert!(ops.contains(&"update"), "update op captured: {ops:?}");
    assert!(ops.contains(&"delete"), "delete op captured: {ops:?}");
    let upd = changes.iter().find(|c| c.op == "update").unwrap();
    assert!(
        upd.document.contains("updated"),
        "update must carry the post-image document, got: {}",
        upd.document
    );
    let del = changes.iter().find(|c| c.op == "delete").unwrap();
    assert_eq!(del.id, "7", "delete must carry the _id");
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_initial_snapshot_covers_preexisting_rows() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_snap");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.seed_int_id("t", 500);

    let rig = cdc(&db, "t").cdc("initial: snapshot").cdc("max_events: 1");
    m.upsert_set("t", 1, "v", "touched");
    rig.run_ok();

    let snap_ids = walkdir_parquet_ids(&rig.out_dir(), "snapshot");
    assert_eq!(
        snap_ids.len(),
        500,
        "initial snapshot must cover all pre-existing rows"
    );
}

/// The printed re-baseline remedy for a lost changes-only checkpoint re-reads the missed document.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs + the rivet-duckdb oracle"]
fn mongo_missing_checkpoint_refusal_on_a_changes_only_stream_remedy_recovers_the_document_written_while_it_was_gone()
 {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let mut s = CdcScenario::mongo_with("cdc_ckgap", |r, _| r);
    s.rig.run_ok();
    s.insert(1);
    s.rig.run_ok();
    std::fs::remove_file(s.rig.checkpoint()).expect("the checkpoint the run wrote");
    s.insert(2);
    let want = Refused::by_code("RIVET_SOURCE_CDC_CHECKPOINT_INVALID", 5);
    let out = s.rig.run_args(&[]);
    assert_refused(&out, want);
    let said = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        said.contains("is missing, but the state DB records that export")
            && said.contains("rivet state reset -c")
            && said.contains(REBASELINE_REMEDY),
        "the refusal names the state record and both remedies:\n{said}"
    );
    assert_refused(&s.rig.run_args(&[]), want);
    follow_rebaseline_remedy(&mut s.rig, false);
    assert_eq!(
        walkdir_parquet_ids(&s.rig.out_dir(), "snapshot"),
        ["1".to_string(), "2".to_string()].into(),
        "the remedy's baseline holds the document written while the checkpoint was gone"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs + the rivet-duckdb oracle"]
fn mongo_lost_checkpoint_is_refused_not_reanchored() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let mut s = CdcScenario::mongo_with("cdc_ckno", |r, _| r);
    assert_a_lost_checkpoint_is_refused_not_reanchored(&mut s, false);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs + the rivet-duckdb oracle"]
fn mongo_state_reset_lifts_a_lost_checkpoint_refusal() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let mut s = CdcScenario::mongo_with("cdc_ckrst", |r, _| r);
    assert_state_reset_lifts_a_lost_checkpoint_refusal(&mut s);
}

/// Distinct `_id` values across `.parquet` files under any subdir of `root` whose path contains `marker` (e.g. the `snapshot/` handoff dir).
fn walkdir_parquet_ids(root: &std::path::Path, marker: &str) -> std::collections::BTreeSet<String> {
    let mut ids = std::collections::BTreeSet::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "parquet")
                && p.to_string_lossy().contains(marker)
            {
                for id in duckdb_dir_parquet_distinct_strings(p.parent().unwrap(), "_id") {
                    ids.insert(id);
                }
            }
        }
    }
    ids
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_mixed_transaction_ending_on_uncaptured_table() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_mix");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("orders");
    m.drop_collection("audit");

    let rig = cdc(&db, "orders");
    rig.run_ok();

    m.txn_two_collections("orders", 1, "audit", 99);

    rig.run_ok();
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    assert!(
        ids.contains("1"),
        "the captured-collection change must appear"
    );
    assert!(
        !ids.contains("99"),
        "the uncaptured collection must NOT leak into the orders stream"
    );
}

/// A standalone server (no change streams).
#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_cdc_initial_snapshot_on_a_standalone_is_refused_before_any_write() {
    require_alive(LiveService::Mongo);
    let db = unique_name("cdc_standalone");
    let m = MongoTest::connect(27017, &db);
    let _g = MongoDbGuard {
        port: 27017,
        db: db.clone(),
    };
    m.upsert_set("t", 1, "v", "a");
    let d = tempfile::tempdir().unwrap();
    let (ckpt, out) = (d.path().join("cdc.ckpt"), d.path().join("out"));
    Rig::mongo_cdc("t")
        .source_url(&MongoTest::url(27017, &db))
        .cdc("initial: snapshot")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone())
        .run_expect_fail();
    assert_refused_before_any_write(&out, &ckpt);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_corrupt_checkpoint_fails_loudly_not_silent_reanchor() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_corrupt");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok();
    assert!(rig.checkpoint().exists(), "run pins a checkpoint");

    std::fs::write(rig.checkpoint(), b"{ not valid json at all").unwrap();
    m.upsert_set("t", 1, "v", "a");

    let stderr = rig.run_expect_fail();
    assert!(
        stderr.contains("Error: [RIVET_SOURCE_CDC_CHECKPOINT_INVALID]")
            && stderr.contains(REBASELINE_REMEDY),
        "refused by code, ending with the re-baseline remedy:\n{stderr}"
    );

    for (file, why) in [
        (&b"{ not valid json at all"[..], "is corrupt or truncated"),
        (&b"{}"[..], "the run refuses this file"),
    ] {
        std::fs::write(rig.checkpoint(), file).unwrap();
        let doc = rig.cli(&["doctor", "--json"]);
        let report: serde_json::Value =
            serde_json::from_slice(&doc.stdout).expect("doctor --json output");
        let check = report["checks"]
            .as_array()
            .expect("checks array")
            .iter()
            .find(|c| {
                c["name"]
                    .as_str()
                    .is_some_and(|n| n.starts_with("CDC checkpoint (export '"))
            })
            .unwrap_or_else(|| panic!("doctor ran no CDC checkpoint check: {report}"));
        assert_eq!(check["ok"], false, "doctor must fail the file: {check}");
        assert!(
            check["detail"].as_str().is_some_and(|d| d.contains(why)),
            "doctor says why ({why}): {check}"
        );
        assert_eq!(
            check["hint"],
            format!("restore the file, or: {REBASELINE_REMEDY}"),
            "doctor's remedy is the run's own"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_pos_column_leads_with_data_for_downstream_sort() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_posdata");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok();
    for i in 1..=4 {
        m.upsert_set("t", i, "v", "x");
    }
    rig.run_ok();

    let changes = read_mongo_cdc_changes(&rig.out_dir());
    assert!(!changes.is_empty(), "changes captured");
    for c in &changes {
        assert!(
            c.pos.starts_with("{\"_data\""),
            "__pos must be _data-first for a correct downstream sort, got: {}",
            c.pos
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_until_current_terminates_under_sustained_writes_and_keeps_backlog() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_hbound");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok();
    for i in 0..30 {
        m.upsert_set("t", i, "v", "backlog");
    }

    let bg_db = db.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
        let w = MongoTest::connect(mongo_rs_port(), &bg_db);
        let mut i = 10_000;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            w.upsert_set("t", i, "v", "live");
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
    });

    let elapsed = run_rivet_bounded(&rig.config_path(), std::time::Duration::from_secs(30));
    bg.stop();
    let elapsed = elapsed.unwrap_or_else(|| {
        panic!("until_current drain HUNG past the 30s watchdog under sustained writes")
    });

    assert!(
        elapsed < std::time::Duration::from_secs(6),
        "until_current must terminate under sustained writes, took {elapsed:?}"
    );
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    for i in 0..30 {
        assert!(
            ids.contains(&i.to_string()),
            "backlog _id {i} must be captured, got {} ids",
            ids.len()
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_mongo_cdc_until_current_open_bound_two_runs_lose_nothing() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_mongoob");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok();

    for i in 0..30 {
        m.upsert_set("t", i, "v", "backlog");
    }

    let bg_db = db.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
        let w = MongoTest::connect(mongo_rs_port(), &bg_db);
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            w.upsert_set("t", i, "v", "live");
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
    });

    let cfg = rig.config_path();
    let elapsed = run_rivet_bounded(&cfg, std::time::Duration::from_secs(30));
    bg.stop();
    assert!(
        elapsed.is_some(),
        "run 1 must terminate at the open-time cluster-time bound under sustained writes (killed at 30s)"
    );

    let elapsed2 = run_rivet_bounded(&cfg, std::time::Duration::from_secs(60));
    assert!(
        elapsed2.is_some(),
        "run 2 (no writers) must drain the deferred tail and exit"
    );

    let got = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    let want: std::collections::BTreeSet<String> = m
        .current_state_i64("t", "v")
        .into_keys()
        .map(|k| k.to_string())
        .collect();
    assert_eq!(
        got, want,
        "run1 ∪ run2 must hold exactly the source's committed _ids — the open-time \
         bound defers the tail to run 2, never drops it"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_uncaptured_collection_drop_does_not_wedge_capture() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_ddl");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("orders");
    m.drop_collection("scratch");
    m.upsert_set("scratch", 1, "v", "x");

    let rig = cdc(&db, "orders");
    rig.run_ok();

    m.upsert_set("orders", 1, "v", "a");
    m.drop_collection("scratch");
    m.upsert_set("orders", 2, "v", "b");

    rig.run_ok();
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    assert!(
        ids.contains("1") && ids.contains("2"),
        "captured collection must keep flowing across an uncaptured drop, got: {ids:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_checkpoint_advances_on_uncaptured_only_traffic() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_kstall");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("orders");
    m.drop_collection("audit");

    let rig = cdc(&db, "orders");
    rig.run_ok();
    let ckpt_after_pin = std::fs::read(rig.checkpoint()).unwrap();

    for i in 0..20 {
        m.upsert_set("audit", i, "v", "log");
    }

    rig.run_ok();
    let ckpt_after_run = std::fs::read(rig.checkpoint()).unwrap();

    assert_ne!(
        ckpt_after_pin, ckpt_after_run,
        "the checkpoint must advance past uncaptured-only traffic, not stall"
    );
}

/// Audit blind cell (Mongo CDC per-type value fidelity).
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_change_stream_renders_tricky_bson_verbatim_like_batch() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    use mongodb::bson::{Bson, doc};
    let db = unique_name("cdc_types");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok();

    m.insert_many(
        "t",
        vec![doc! {
            "_id": 1_i64,
            "i64_big": 9_007_199_254_740_993_i64,
            "dec": Bson::Decimal128("123456789.987654321012345".parse().unwrap()),
            "nested": doc! { "k": "v-\u{00e9}\u{4e2d}", "arr": [1_i32, 2_i32, 3_i32] },
        }],
    );
    m.upsert_set("t", 1, "note", "changed");
    rig.run_ok();

    let changes = read_mongo_cdc_changes(&rig.out_dir());
    let with_doc: Vec<&MongoCdcChange> = changes
        .iter()
        .filter(|c| c.document.contains("i64_big"))
        .collect();
    assert!(
        !with_doc.is_empty(),
        "the change stream must capture at least one post-image carrying the tricky \
         document; captured {} change(s)",
        changes.len()
    );
    for ch in with_doc {
        assert!(
            ch.document.contains("9007199254740993") && !ch.document.contains("$numberLong"),
            "large Int64 must render VERBATIM as the default relaxed bare number in the CDC \
             document (op '{}') — a canonical/relaxed DRIFT on the op path (or f64 rounding) \
             corrupts it; got: {}",
            ch.op,
            ch.document
        );
        assert!(
            ch.document.contains("123456789.987654321012345")
                && ch.document.contains("$numberDecimal"),
            "Decimal128 must be VERBATIM + type-tagged (`$numberDecimal`) in the CDC document \
             (op '{}'); got: {}",
            ch.op,
            ch.document
        );
        assert!(
            ch.document.contains("v-\u{00e9}\u{4e2d}") && ch.document.contains("arr"),
            "nested unicode + array must be VERBATIM in the CDC document (op '{}'); got: {}",
            ch.op,
            ch.document
        );
    }
}

/// Per-FIELD presence profile against MONGODB ITSELF.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_per_field_presence_matches_the_source() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    use mongodb::bson::doc;
    let db = unique_name("cdc_fields");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("f");

    let rig = cdc(&db, "f");
    rig.run_ok();

    const DOCS: i64 = 24;
    let docs: Vec<_> = (1..=DOCS)
        .map(|i| {
            let mut d = doc! { "_id": i, "always": format!("a{i}"), "nested": doc!{ "k": i } };
            if i % 2 == 0 {
                d.insert("opt", format!("o{i}"));
            }
            d
        })
        .collect();
    m.insert_many("f", docs);
    for _ in 0..3 {
        rig.run_ok();
    }

    let captured = read_mongo_cdc_changes(&rig.out_dir()).len() as i64;
    assert!(
        captured >= DOCS,
        "fixture inert: only {captured} change(s) captured for {DOCS} inserted documents"
    );

    let missing_doc = duckdb_dir_scalar(
        &rig.out_dir(),
        "count(*) - count(\"document\")",
        Some("__op <> 'delete'"),
    );
    assert_eq!(
        missing_doc, 0,
        "{missing_doc} captured post-image(s) carry no document at all — a blob that \
         degrades to NULL is this model's whole-row silent loss"
    );

    for (field, expected) in [("always", DOCS), ("opt", DOCS / 2), ("nested", DOCS)] {
        let src = m.count_field_present("f", field) as i64;
        assert_eq!(
            src, expected,
            "fixture drifted: MongoDB reports {src} document(s) with '{field}', the fixture \
             builds {expected} — the comparison below would then grade the wrong population"
        );
        let dst = duckdb_dir_scalar(
            &rig.out_dir(),
            &format!("count(json_extract(\"document\", '$.{field}'))"),
            Some("__op <> 'delete'"),
        );
        assert_eq!(
            src, dst,
            "field '{field}': presence parity against MongoDB itself — source {src}, \
             captured {dst}. A renderer that drops a field moves this and nothing else; \
             the change count, the `_id` set and a single-field dedup all still balance."
        );
    }
}

/// A DELETE carries the PRE-IMAGE when the collection has one.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs (6.0+ for pre-images)"]
fn mongo_cdc_delete_carries_the_pre_image_when_the_collection_has_one() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_preimg");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("t");
    m.upsert_set("t", 9, "v", "seed");
    if !m.enable_pre_images("t") {
        skip_live(&format!(
            "server major {} has no changeStreamPreAndPostImages",
            m.server_major()
        ));
        return;
    }

    let rig = cdc(&db, "t");
    rig.run_ok();

    m.upsert_set("t", 9, "v", "doomed");
    m.delete_one("t", 9);
    rig.run_ok();

    let changes = read_mongo_cdc_changes(&rig.out_dir());
    let del = changes
        .iter()
        .find(|c| c.op == "delete")
        .unwrap_or_else(|| {
            panic!(
                "no delete captured — the fixture is inert ({} changes)",
                changes.len()
            )
        });
    assert_eq!(del.id, "9", "the delete must carry its _id");

    assert!(
        del.document.contains("\"v\":\"doomed\""),
        "the delete must carry the PRE-IMAGE — the value the document held when it \
         was deleted. Got: {}",
        del.document
    );
    assert!(
        !del.document.contains("seed"),
        "...and the pre-image is the state at DELETE time, not an earlier one. \
         Got: {}",
        del.document
    );
}

/// The OTHER side of the same window: a crash after the checkpoint is persisted.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_crash_after_the_checkpoint_still_delivers_every_change() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_ckcrash");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("t");

    let mut rig = cdc(&db, "t").cdc("rollover: 2");
    rig.run_ok();
    for i in 1..=6 {
        m.upsert_set("t", i, "v", &format!("x{i}"));
    }

    let crashed = rig.run_with_env("RIVET_TEST_PANIC_AT", "cdc_after_checkpoint_before_ack");
    assert!(
        !crashed.status.success(),
        "the fault hook must crash the run — a fault that did not fire leaves this \
         asserting an ordinary two-run export"
    );
    let leg1 = duckdb_declared_rows(rig.oracle_dir());
    assert!(
        leg1 < 6,
        "run 1 delivered {leg1} of 6 before the crash — nothing was left for the \
         resume, so the union below would pass over a resume that captured zero"
    );

    let leg1_ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");

    rig.resume_into_fresh_dest();
    rig.run_ok();
    let resumed = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    assert!(
        !resumed.is_empty(),
        "the resume leg delivered NOTHING — with four changes left unread after the \
         crash, an empty leg means the checkpoint jumped past them"
    );
    let mut union = leg1_ids.clone();
    union.extend(resumed.iter().cloned());
    for i in 1..=6 {
        assert!(
            union.contains(&i.to_string()),
            "id {i} was never delivered by EITHER leg — the checkpoint advanced past a \
             change whose part was not durable. leg1={leg1_ids:?} resume={resumed:?}"
        );
    }
}

/// A configured collection that does not exist must SAY so — today it is silence.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_warns_when_a_configured_collection_does_not_exist() {
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_ghost");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.upsert_set("real", 1, "v", "here");

    let rig = cdc(&db, "no_such_collection");
    let said = rig.run_ok_capture();
    assert!(
        said.contains("no_such_collection"),
        "the run must name the collection it could not find. Got:\n{said}"
    );
    assert!(
        said.contains("real"),
        "...and name what the database DOES hold, because the cause is almost always \
         a typo and the fix is the neighbouring name. Got:\n{said}"
    );

    let quiet = cdc(&db, "real");
    let out = quiet.run_ok_capture();
    assert!(
        !out.contains("could not find"),
        "an existing collection is the ordinary case and must not warn. Got:\n{out}"
    );
    m.upsert_set("real", 2, "v", "after-anchor");
    quiet.run_ok();
    let delivered = read_mongo_cdc_changes(&quiet.out_dir()).len();
    assert_eq!(
        delivered, 1,
        "the warning path must not wedge capture — a change written after the anchor \
         has to arrive, or `warning, not refusal` is an empty claim"
    );
}

/// `rivet cdc --output --format csv` on Mongo.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_cli_writes_a_faithful_csv_of_the_document_column() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    use mongodb::bson::doc;
    let db = unique_name("cdc_cli_csv");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("docs");
    m.drop_collection("other");

    let d = tempfile::tempdir().expect("tempdir");
    let ckpt = d.path().join("cli.ckpt");
    let url = MongoTest::url(mongo_rs_port(), &db);
    let (host, container) = live_shared_workdir(&unique_name("mongo_cli_csv"));

    let anchor = run_rivet_args_bounded_env(
        &[
            "cdc",
            "--source",
            &url,
            "--table",
            "docs",
            "--checkpoint",
            ckpt.to_str().unwrap(),
        ],
        &[],
        std::time::Duration::from_secs(60),
    );
    assert!(anchor.is_some(), "the anchoring run did not terminate");
    assert!(ckpt.is_file(), "run 1 wrote no checkpoint to resume from");

    m.insert_many(
        "docs",
        vec![
            doc! { "_id": 11_i64, "note": "has,comma" },
            doc! { "_id": 12_i64, "note": "has\"quote" },
            doc! { "_id": 13_i64, "note": "line\nbreak" },
            doc! { "_id": 14_i64, "nested": doc! { "a": 1_i64, "b": [1_i64, 2_i64] } },
        ],
    );
    m.insert_many("other", vec![doc! { "_id": 99_i64, "note": "not mine" }]);

    let out = run_rivet_args_bounded_env(
        &[
            "cdc",
            "--source",
            &url,
            "--table",
            "docs",
            "--checkpoint",
            ckpt.to_str().unwrap(),
            "--output",
            host.to_str().unwrap(),
            "--format",
            "csv",
        ],
        &[],
        std::time::Duration::from_secs(90),
    );
    assert!(out.is_some(), "the capturing run did not terminate");
    assert!(
        !files_with_extension(&host, "csv").is_empty(),
        "no .csv here — an oracle that shrugged at an empty set would grade nothing. \
         This one assert catches TWO of the graded flags, MEASURED: `--format \
         parquet` leaves the directory holding parquet, and dropping `--checkpoint` \
         re-anchors run 2 at now, so it captures zero events and the sink writes no \
         file at all"
    );

    let v = duckdb_run_sql_json(&format!(
        "SELECT _id, document FROM read_csv_auto('{container}/**/*.csv', header=true) ORDER BY _id"
    ));
    let rows = v["rows"].as_array().cloned().unwrap_or_default();
    let got: std::collections::BTreeMap<i64, serde_json::Value> = rows
        .iter()
        .filter_map(|r| {
            let id = r.get(0)?.as_str()?.parse::<i64>().ok()?;
            let doc: serde_json::Value = serde_json::from_str(r.get(1)?.as_str()?).ok()?;
            Some((id, doc))
        })
        .collect();

    assert_eq!(
        got.keys().copied().collect::<Vec<_>>(),
        vec![11, 12, 13, 14],
        "expected exactly the four documents written to the CONFIGURED collection. \
         Id 99 is the neighbouring collection and means the routing filter let it \
         through (`--table` cannot simply be dropped here — the subcommand refuses \
         `--output` without exactly one, MEASURED, so the mutant that grades this \
         line is pointing it at `other`, which yields [99]); zero means the \
         checkpoint never resumed; a row lost or gained anywhere else means the CSV \
         split. Got: {got:#?}"
    );

    assert_eq!(got[&11]["note"], serde_json::json!("has,comma"));
    assert_eq!(got[&12]["note"], serde_json::json!("has\"quote"));
    assert_eq!(got[&13]["note"], serde_json::json!("line\nbreak"));
    assert_eq!(got[&14]["nested"], serde_json::json!({"a": 1, "b": [1, 2]}));

    m.drop_collection("docs");
    m.drop_collection("other");
}

/// A dotted collection name is one collection, not a schema qualifier.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_captures_a_dotted_collection_without_swallowing_its_sibling() {
    require_alive(LiveService::MongoRs);
    use mongodb::bson::doc;
    let db = unique_name("dotted").to_lowercase();
    let dotted = format!("{db}.orders");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.create_empty_collection(&dotted);
    m.drop_collection("orders");

    let rig = cdc(&db, &dotted).cdc_line("initial: snapshot");
    rig.run_ok();
    m.insert_many(&dotted, vec![doc! { "_id": 1_i64, "who": "dotted" }]);
    m.insert_many("orders", vec![doc! { "_id": 2_i64, "who": "sibling" }]);
    rig.run_ok();

    let who: std::collections::BTreeSet<String> = read_mongo_cdc_changes(&rig.out_dir())
        .iter()
        .filter_map(|c| {
            serde_json::from_str::<serde_json::Value>(&c.document)
                .ok()?
                .get("who")?
                .as_str()
                .map(|s| s.to_string())
        })
        .collect();
    assert_eq!(
        who,
        ["dotted"]
            .iter()
            .map(|s| s.to_string())
            .collect::<std::collections::BTreeSet<_>>(),
        "the dotted collection must be captured (a config that no longer even \
         LOADS would give an empty set) and the sibling `orders` must not be — \
         `sibling` appearing here is the interleave, which counts cannot see"
    );

    {
        let p = rig.out_dir().join("snapshot").join("manifest.json");
        assert!(
            p.is_file(),
            "the snapshot leg must have produced a manifest — without one this \
             assertion is vacuous, which is exactly how the defect survived"
        );
        let doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&p).expect("read manifest"))
                .expect("parse manifest");
        assert_eq!(
            doc["source"]["schema"],
            serde_json::Value::Null,
            "a MongoDB manifest must carry NO schema — the database is not one, and \
             recording the first dot-segment as a schema names a different \
             collection. Got: {}",
            doc["source"]
        );
        assert_eq!(
            doc["source"]["table"].as_str(),
            Some(dotted.as_str()),
            "and the table must be the collection's WHOLE name: {}",
            doc["source"]
        );
    }

    m.drop_collection(&dotted);
    m.drop_collection("orders");
}

#[test]
#[ignore = "live: requires docker compose --profile replica up -d mongo-rs2-a mongo-rs2-b"]
fn mongo_cdc_streams_changes_from_a_secondary() {
    for port in [27022u16, 27023] {
        assert!(
            std::net::TcpStream::connect_timeout(
                &format!("127.0.0.1:{port}").parse().unwrap(),
                std::time::Duration::from_millis(500),
            )
            .is_ok(),
            "fixture: the rs2 replica set is not up on :{port}"
        );
    }
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_secondary");
    let primary = MongoTest::connect(27022, &db);
    let secondary = MongoTest::connect(27023, &db);
    primary.drop_collection("t");
    let url = format!("{}&readPreference=secondary", MongoTest::url(27023, &db));
    let rig = Rig::mongo_cdc("t").source_url(&url).duckdb_oracle();
    rig.run_ok();

    primary.upsert_set("t", 1, "v", "a");
    primary.upsert_set("t", 2, "v", "b");
    for _ in 0..60 {
        if secondary.count("t") == 2 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    assert_eq!(
        secondary.count("t"),
        2,
        "fixture: both documents reached the secondary"
    );
    rig.run_ok();
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    assert!(
        ["1", "2"].iter().all(|i| ids.contains(*i)),
        "both changes written on the primary must be captured from the secondary: got {ids:?}"
    );
}

/// `(op, _id)` of every row in `batches`.
fn op_doc_ids(batches: &[arrow::record_batch::RecordBatch]) -> Vec<(String, String)> {
    use arrow::array::{Array, StringArray};
    let mut out = Vec::new();
    for b in batches {
        let col = |n: &str| b.column(b.schema().index_of(n).unwrap()).clone();
        let (op, id) = (col("__op"), col("_id"));
        let op = op.as_any().downcast_ref::<StringArray>().unwrap();
        let id = id.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            out.push((op.value(i).to_string(), id.value(i).to_string()));
        }
    }
    out
}

/// A capture read from a secondary.
#[test]
#[ignore = "live: requires docker compose --profile replica up -d mongo-rs2-a mongo-rs2-b"]
fn mongo_cdc_follows_a_failover_to_another_replica_set_member() {
    for port in [27022u16, 27023] {
        assert!(
            std::net::TcpStream::connect_timeout(
                &format!("127.0.0.1:{port}").parse().unwrap(),
                std::time::Duration::from_millis(500),
            )
            .is_ok(),
            "fixture: the rs2 replica set is not up on :{port}"
        );
    }
    let db = unique_name("cdc_failover");
    let primary = MongoTest::connect(27022, &db);
    let secondary = MongoTest::connect(27023, &db);
    primary.drop_collection("t");
    let on_secondary = format!("{}&readPreference=secondary", MongoTest::url(27023, &db));
    let on_primary = MongoTest::url(27022, &db);
    let rig = Rig::mongo_cdc("t").source_url_env("RIVET_FAILOVER_URL");
    let run_on = |url: &str| {
        let out = rig.run_args_env(&[], &[("RIVET_FAILOVER_URL", url)]);
        assert!(
            out.status.success(),
            "the run against {url} failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    let reached = |n: u64| {
        for _ in 0..60 {
            if secondary.count("t") == n {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        panic!("fixture: the secondary never reached {n} document(s)");
    };
    run_on(&on_secondary);
    primary.upsert_set("t", 1, "v", "a");
    primary.upsert_set("t", 2, "v", "b");
    reached(2);
    run_on(&on_secondary);
    primary.upsert_set("t", 3, "v", "c");
    primary.upsert_set("t", 4, "v", "d");
    reached(4);
    run_on(&on_primary);
    let ids: Vec<String> = op_doc_ids(&read_all_parts(&rig.out_dir()))
        .into_iter()
        .map(|(_, id)| id)
        .collect();
    assert_eq!(
        ids,
        ["1", "2", "3", "4"],
        "across the failover every change is captured exactly once, none skipped"
    );
    primary.drop_collection("t");
}

/// Documents `{_id: null}` and `{_id: "null"}`, with pre-images when asked.
fn null_and_string_null_ids(pre_images: bool) -> Option<(MongoTest, MongoDbGuard, Rig)> {
    use mongodb::bson::{Bson, doc};
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_nullid");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    let g = MongoDbGuard {
        port: mongo_rs_port(),
        db: db.clone(),
    };
    m.drop_collection("t");
    m.insert_many(
        "t",
        vec![
            doc! { "_id": Bson::Null, "v": 1 },
            doc! { "_id": "null", "v": 2 },
        ],
    );
    assert_eq!(
        m.count("t"),
        2,
        "fixture: two documents, distinct by BSON type"
    );
    if pre_images && !m.enable_pre_images("t") {
        return None;
    }
    let rig = cdc(&db, "t");
    rig.run_ok();
    Some((m, g, rig))
}

/// A delete of `_id: null` and a delete of `_id: "null"` must be distinguishable in the change log.
#[test]
/// Strict known defect (Mongo engine step).
#[ignore = "live: requires docker compose up -d mongo-rs"]
#[should_panic(expected = "cannot tell which document was deleted")]
fn roast_mongo_cdc_deletes_of_null_and_string_null_id_are_distinguishable() {
    use mongodb::bson::Bson;
    let (m, _g, rig) = null_and_string_null_ids(false).unwrap();
    m.delete_bson_id("t", Bson::Null);
    m.delete_bson_id("t", Bson::String("null".into()));
    rig.run_ok();
    let dir = rig.out_dir();
    let deletes = duckdb_declared_dir_scalar(&dir, "count(*) FILTER (WHERE __op = 'delete')");
    let distinct = duckdb_declared_dir_scalar(
        &dir,
        "count(DISTINCT (_id, coalesce(document::VARCHAR, '<null>'))) FILTER (WHERE __op = 'delete')",
    );
    assert_eq!(deletes, 2, "fixture: both deletes captured");
    assert_eq!(
        distinct, 2,
        "a downstream MERGE cannot tell which document was deleted when the two delete \
         rows carry identical identity (_id, document)"
    );
}

/// With collection pre-images.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs (6.0+ for pre-images)"]
fn mongo_cdc_pre_images_tell_deletes_of_null_and_string_null_id_apart() {
    use mongodb::bson::Bson;
    let Some((m, _g, rig)) = null_and_string_null_ids(true) else {
        skip_live("server has no changeStreamPreAndPostImages");
        return;
    };
    m.delete_bson_id("t", Bson::Null);
    m.delete_bson_id("t", Bson::String("null".into()));
    rig.run_ok();
    let dir = rig.out_dir();
    let deletes = duckdb_declared_dir_scalar(&dir, "count(*) FILTER (WHERE __op = 'delete')");
    let distinct = duckdb_declared_dir_scalar(
        &dir,
        "count(DISTINCT (_id, coalesce(document::VARCHAR, '<null>'))) FILTER (WHERE __op = 'delete')",
    );
    assert_eq!(deletes, 2, "fixture: both deletes captured");
    assert_eq!(
        distinct, 2,
        "with pre-images each delete's document holds its typed _id"
    );
}

/// A `rivet cdc --max-events` run on MongoDB stops at its cap and owes the rest to the next run.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_cli_a_capped_run_leaves_its_remainder_to_the_next_run() {
    require_alive(LiveService::MongoRs);
    use mongodb::bson::doc;
    let db = unique_name("cdc_cli_defer");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    m.drop_collection("docs");
    let d = tempfile::tempdir().expect("tempdir");
    let ckpt = d.path().join("cli.ckpt").to_str().unwrap().to_string();
    let url = MongoTest::url(mongo_rs_port(), &db);
    let run = |cap: &[&str]| {
        let mut args = vec![
            "cdc",
            "--source",
            &url,
            "--table",
            "docs",
            "--checkpoint",
            &ckpt,
        ];
        args.extend(cap);
        run_rivet_args_bounded_env(&args, &[], std::time::Duration::from_secs(60))
            .expect("a bounded `rivet cdc` run terminates")
    };
    run(&[]);
    m.insert_many(
        "docs",
        (1..=4_i64).map(|i| doc! { "_id": i, "v": i }).collect(),
    );
    let capped = ndjson_after_ids(&run(&["--max-events", "2"]), "docs");
    assert_eq!(
        capped,
        [1, 2].into(),
        "the cap stops after two single-document commits"
    );
    let rest = ndjson_after_ids(&run(&[]), "docs");
    assert_eq!(rest, [3, 4].into(), "the next run delivers the remainder");
}

/// Apply one engine-neutral write to a collection.
fn mongo_churn(m: &MongoTest, coll: &str, op: Churn) {
    match op {
        Churn::Insert(id) => m.insert_many(coll, vec![mongodb::bson::doc! { "_id": id, "v": id }]),
        Churn::Update(id) => m.upsert_set(coll, id, "v", "99"),
        Churn::Delete(id) => m.delete_one(coll, id),
    }
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_collection_removed_from_tables_and_put_back_is_refused_until_rebaselined() {
    let db = unique_name("cdc_rejoin");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    let _g = MongoDbGuard {
        port: mongo_rs_port(),
        db: db.clone(),
    };
    a_table_put_back_is_refused_until_rebaselined(
        Rig::mongo_cdc("a").source_url(&MongoTest::url(mongo_rs_port(), &db)),
        ["a", "b", "c"],
        &mut |t, op| mongo_churn(&m, t, op),
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_collection_switched_to_another_over_its_baseline_is_refused_and_switched_back_continues()
 {
    let db = unique_name("cdc_switch");
    let m = MongoTest::connect(mongo_rs_port(), &db);
    let _g = MongoDbGuard {
        port: mongo_rs_port(),
        db: db.clone(),
    };
    a_table_switched_over_a_baseline_is_refused_and_switched_back_continues(
        Rig::mongo_cdc("a").source_url(&MongoTest::url(mongo_rs_port(), &db)),
        ["a", "b"],
        &|r, t| r.repoint(t),
        &mut |t, op| mongo_churn(&m, t, op),
    );
}
