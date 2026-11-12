//! `rivet validate` of a prefix with no manifest and no `_SUCCESS`: never delivered exits 0, a recorded delivery exits 3.

use std::path::{Path, PathBuf};
use std::process::Output;

use rivet::state::StateStore;

const RIVET_BIN: &str = env!("CARGO_BIN_EXE_rivet");
const CODE: &str = "RIVET_VERIFY_DELIVERY_ABSENT";

/// A config of one local export `orders` writing to `prefix`; returns the config path.
fn write_cfg(dir: &Path, prefix: &Path) -> PathBuf {
    let cfg = dir.join("rivet.yaml");
    let yaml = format!(
        "source:\n  type: postgres\n  url: postgresql://nobody@localhost/nope\nexports:\n  - name: orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: \"{}\"\n",
        prefix.to_string_lossy()
    );
    std::fs::write(&cfg, yaml).unwrap();
    cfg
}

/// Record a finished run of `export` at `prefix` in the state beside `cfg`, as `rivet run` does.
fn record_run(cfg: &Path, export: &str, run_id: &str, prefix: &str, status: &str) {
    let state = StateStore::open(cfg.to_str().unwrap()).unwrap();
    state
        .begin_run(run_id, export, prefix, "2026-11-01T00:00:00Z")
        .unwrap();
    state
        .finish_run(run_id, status, "2026-11-01T00:01:00Z")
        .unwrap();
}

fn validate(cfg: &Path, extra: &[&str]) -> Output {
    std::process::Command::new(RIVET_BIN)
        .args(["validate", "--config", cfg.to_str().unwrap()])
        .args(extra)
        .output()
        .expect("spawn rivet validate")
}

fn text(o: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

fn uri(prefix: &Path) -> String {
    format!("file://{}", prefix.to_string_lossy())
}

/// A real dataset at `prefix`: manifest, `_SUCCESS` and one part, written by the product's own writer.
fn stage_dataset(prefix: &Path) {
    use rivet::manifest::{
        MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, ManifestStatus,
        PartStatus, RunManifest,
    };
    use std::sync::Arc;
    std::fs::create_dir_all(prefix).unwrap();
    let part = prefix.join("part-000001.parquet");
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut w =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(&part).unwrap(), schema, None)
            .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let m = RunManifest {
        split_window: None,
        checksum_render: None,
        row_hash: None,
        mode: "batch".to_string(),
        manifest_version: MANIFEST_VERSION,
        run_id: "r-rec".into(),
        export_name: "orders".into(),
        export_family: String::new(),
        started_at: "2026-06-09T12:00:00Z".into(),
        finished_at: "2026-06-09T12:01:00Z".into(),
        status: ManifestStatus::Success,
        source: ManifestSource {
            engine: "postgres".into(),
            schema: Some("public".into()),
            table: Some("orders".into()),
            extraction: None,
        },
        destination: ManifestDestination {
            kind: "local".into(),
            uri: uri(prefix),
        },
        format: "parquet".into(),
        compression: "zstd".into(),
        schema_fingerprint: "xxh3:0123456789abcdef".into(),
        row_count: 1,
        part_count: 1,
        parts: vec![ManifestPart {
            part_id: 1,
            path: "part-000001.parquet".into(),
            rows: 1,
            size_bytes: std::fs::metadata(&part).unwrap().len(),
            content_fingerprint: "xxh3:1111111111111111".into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }],
        column_checksums: None,
        checksum_key_column: None,
    };
    let dest =
        rivet::destination_for_tests::create_destination(&rivet::config::DestinationConfig {
            destination_type: rivet::config::DestinationType::Local,
            path: Some(prefix.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap();
    rivet::pipeline::for_tests::write_manifest(&*dest, &m).unwrap();
}

#[test]
fn a_prefix_of_a_recorded_delivery_that_holds_nothing_exits_3_by_code() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = dir.path().join("out");
    let cfg = write_cfg(dir.path(), &prefix);
    record_run(&cfg, "orders", "r1", &uri(&prefix), "success");
    let o = validate(&cfg, &[]);
    assert_eq!(o.status.code(), Some(3), "{}", text(&o));
    assert!(text(&o).contains(CODE), "{}", text(&o));
    assert!(
        text(&o).contains("rivet run --export orders"),
        "the refusal names the remedy: {}",
        text(&o)
    );
}

#[test]
fn a_recorded_delivery_is_found_under_a_trailing_slash() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = dir.path().join("out");
    let cfg = write_cfg(dir.path(), &prefix);
    record_run(
        &cfg,
        "orders",
        "r1",
        &format!("{}/", uri(&prefix)),
        "success",
    );
    assert_eq!(validate(&cfg, &[]).status.code(), Some(3));
}

#[test]
fn a_delivery_recorded_at_a_parent_prefix_is_not_this_prefixs_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("out").join("orders");
    let cfg = write_cfg(dir.path(), &nested);
    record_run(
        &cfg,
        "orders",
        "r1",
        &uri(&dir.path().join("out")),
        "success",
    );
    assert_eq!(validate(&cfg, &[]).status.code(), Some(0));
}

#[test]
fn a_prefix_never_delivered_is_still_legacy_run_exit_0_and_creates_no_state() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_cfg(dir.path(), &dir.path().join("out"));
    let o = validate(&cfg, &[]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o));
    assert!(text(&o).contains("legacy_run"), "{}", text(&o));
    assert!(
        !dir.path().join(".rivet_state.db").exists(),
        "validate must not create a state file"
    );
}

#[test]
fn a_state_with_no_success_for_the_prefix_leaves_it_never_delivered() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = dir.path().join("out");
    let cfg = write_cfg(dir.path(), &prefix);
    record_run(&cfg, "orders", "failed", &uri(&prefix), "failed");
    record_run(&cfg, "orders", "stopped", &uri(&prefix), "interrupted");
    record_run(&cfg, "orders", "running", &uri(&prefix), "running");
    record_run(
        &cfg,
        "other",
        "success-of-another",
        &uri(&prefix),
        "success",
    );
    record_run(
        &cfg,
        "orders",
        "elsewhere",
        &uri(&dir.path().join("somewhere-else")),
        "success",
    );
    record_run(
        &cfg,
        "orders",
        "sibling",
        &uri(&dir.path().join("out-2")),
        "success",
    );
    let o = validate(&cfg, &[]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o));
    assert!(text(&o).contains("legacy_run"), "{}", text(&o));
}

#[test]
fn an_intact_dataset_with_a_recorded_delivery_still_passes() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = dir.path().join("out");
    stage_dataset(&prefix);
    let cfg = write_cfg(dir.path(), &prefix);
    record_run(&cfg, "orders", "r1", &uri(&prefix), "success");
    let o = validate(&cfg, &[]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o));
    assert!(text(&o).contains("PASSED"), "{}", text(&o));
}

#[test]
fn a_recorded_delivery_at_another_date_leaves_today_never_delivered() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("rivet.yaml");
    let base = dir.path().join("out");
    std::fs::write(
        &cfg,
        format!(
            "source:\n  type: postgres\n  url: postgresql://nobody@localhost/nope\nexports:\n  - name: orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: \"{}/{{date}}\"\n",
            base.to_string_lossy()
        ),
    )
    .unwrap();
    record_run(
        &cfg,
        "orders",
        "r1",
        &uri(&base.join("2026-01-01")),
        "success",
    );
    let o = validate(&cfg, &[]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o));
    let o = validate(&cfg, &["--date", "2026-01-01"]);
    assert_eq!(o.status.code(), Some(3), "{}", text(&o));
}

#[test]
fn an_unreadable_state_cannot_prove_never_delivered_and_is_refused_by_code() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_cfg(dir.path(), &dir.path().join("out"));
    std::fs::write(
        dir.path().join(".rivet_state.db"),
        "not a sqlite database ".repeat(60),
    )
    .unwrap();
    let o = validate(&cfg, &[]);
    assert_eq!(o.status.code(), Some(1), "{}", text(&o));
    assert!(
        text(&o).contains("RIVET_STATE_FILE_CORRUPT"),
        "{}",
        text(&o)
    );
}

#[test]
fn an_unreadable_state_is_not_read_when_the_manifest_already_decides() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = dir.path().join("out");
    stage_dataset(&prefix);
    let cfg = write_cfg(dir.path(), &prefix);
    std::fs::write(
        dir.path().join(".rivet_state.db"),
        "not a sqlite database ".repeat(60),
    )
    .unwrap();
    let o = validate(&cfg, &[]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o));
    assert!(text(&o).contains("PASSED"), "{}", text(&o));
}

#[test]
fn a_success_marker_without_a_manifest_still_fails_with_or_without_a_record() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = dir.path().join("out");
    std::fs::create_dir_all(&prefix).unwrap();
    std::fs::write(prefix.join("_SUCCESS"), "xxh3:0000000000000000").unwrap();
    let cfg = write_cfg(dir.path(), &prefix);
    let before = validate(&cfg, &[]);
    assert_eq!(before.status.code(), Some(3), "{}", text(&before));
    record_run(&cfg, "orders", "r1", &uri(&prefix), "success");
    let after = validate(&cfg, &[]);
    assert_eq!(after.status.code(), Some(3), "{}", text(&after));
    assert!(text(&after).contains("RIVET_VERIFY_MANIFEST_MISSING"));
}

#[test]
fn a_pinned_prefix_with_no_manifest_keeps_its_own_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = dir.path().join("out");
    let cfg = write_cfg(dir.path(), &prefix);
    record_run(&cfg, "orders", "r1", &uri(&prefix), "success");
    let o = validate(
        &cfg,
        &["--export", "orders", "--prefix", prefix.to_str().unwrap()],
    );
    assert_eq!(o.status.code(), Some(3), "{}", text(&o));
    assert!(
        text(&o).contains("RIVET_VERIFY_MANIFEST_REQUIRED"),
        "{}",
        text(&o)
    );
}
