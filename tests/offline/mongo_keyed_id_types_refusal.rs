//! A keyed MongoDB CDC export whose checkpoint already holds two `_id` types is refused by code before its snapshot writes (offline, dead source URL).

use std::path::Path;
use std::process::Command;

const RIVET_BIN: &str = env!("CARGO_BIN_EXE_rivet");
const MIXED: &str = "RIVET_SOURCE_ID_TYPES_MIXED";

/// `rivet run` in `dir` with every inherited `RIVET_*` stripped; the exit code and stderr.
fn rivet_run(dir: &Path) -> (i32, String) {
    let mut cmd = Command::new(RIVET_BIN);
    cmd.args(["run", "-c", "a.yaml"]).current_dir(dir);
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("RIVET_") {
            cmd.env_remove(&k);
        }
    }
    let out = cmd.output().expect("spawn rivet");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// A `mode: cdc` Mongo config over a dead URL with `initial: snapshot`, keyed by `pk`, and a checkpoint holding `idt`.
fn write(dir: &Path, pk: &str, idt: &str) {
    let yaml = format!(
        "source:\n  type: mongo\n  url: \"mongodb://u@127.0.0.1:1/app?serverSelectionTimeoutMS=300\"\n  tls: {{ mode: disable }}\n\
         load: {{ target: clickhouse, url: \"http://127.0.0.1:1\", database: d, user: u, password_env: P{pk} }}\n\
         exports:\n  - name: o\n    table: orders\n    mode: cdc\n    format: parquet\n    cdc:\n      initial: snapshot\n      checkpoint: ./o.ckpt\n    \
         destination: {{ type: local, path: ./out }}\n"
    );
    std::fs::write(dir.join("a.yaml"), yaml).unwrap();
    std::fs::write(
        dir.join("o.ckpt"),
        format!("{{\"_data\":\"82\",\"idt\":{{\"orders\":{idt}}}}}"),
    )
    .unwrap();
}

/// The keyed export is refused by code at its snapshot; a uniform checkpoint is not refused for `_id` types.
#[test]
fn a_keyed_mongo_snapshot_over_a_mixed_id_checkpoint_is_refused_by_code() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), ", pk: [_id]", "[0,2]");
    let (rc, err) = rivet_run(dir.path());
    assert_eq!(rc, 5, "{err}");
    assert!(err.contains(&format!("[{MIXED}]")), "{err}");
    write(dir.path(), ", pk: [_id]", "[0]");
    let (_, err) = rivet_run(dir.path());
    assert!(
        !err.contains(MIXED),
        "a uniform checkpoint is not refused:\n{err}"
    );
}
