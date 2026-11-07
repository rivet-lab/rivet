//! `rivet state reset` of a partitioned export on an SQLite state: the family it takes is bounded by nothing, so the files it holds, and the leases it waits for, are checked on the binary.

use rivet::state::StateStore;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const RIVET_BIN: &str = env!("CARGO_BIN_EXE_rivet");

/// A config declaring the partitioned export `ev` and its state beside it.
fn config(dir: &Path) -> PathBuf {
    let path = dir.join("rivet.yaml");
    std::fs::write(
        &path,
        "source:\n  type: postgres\n  url: postgresql://localhost/testdb\nexports:\n  - name: ev\n    query: \"SELECT 1\"\n    mode: incremental\n    cursor_column: id\n    partition_by: d\n    format: parquet\n    destination:\n      type: local\n      path: ./out/{partition}\n",
    )
    .unwrap();
    path
}

fn source_of(cfg: &Path) -> String {
    rivet::config::Config::load(&cfg.display().to_string())
        .unwrap()
        .source
        .state_key()
}

fn child(i: usize) -> String {
    format!("ev__{i:05}")
}

/// The state beside `cfg` holding `ev` and `children` partitions under `prefix`, each with a cursor and a boundary.
fn seeded(cfg: &Path, prefix: &str, children: usize) -> rusqlite::Connection {
    drop(StateStore::open(&cfg.display().to_string()).unwrap());
    let db = rusqlite::Connection::open(cfg.with_file_name(".rivet_state.db")).unwrap();
    let names = std::iter::once("ev".to_string()).chain((0..children).map(child));
    for name in names {
        db.execute(
            "INSERT INTO export_state (export_name, prefix, last_cursor_value) VALUES (?1, ?2, '7')",
            rusqlite::params![name, prefix],
        )
        .unwrap();
        db.execute(
            "INSERT INTO export_progression (export_name, last_committed_strategy, last_committed_cursor) VALUES (?1, 'incremental', '7')",
            rusqlite::params![name],
        )
        .unwrap();
    }
    db
}

fn held(db: &rusqlite::Connection, table: &str) -> i64 {
    db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

/// `rivet <args>` over `cfg`, capped at `open_files` open files when given.
fn rivet(cfg: &Path, args: &[&str], open_files: Option<u64>) -> Output {
    use std::os::unix::process::CommandExt as _;
    let mut cmd = Command::new(RIVET_BIN);
    cmd.args(args).arg("-c").arg(cfg);
    if let Some(most) = open_files {
        let limit = libc::rlimit {
            rlim_cur: most,
            rlim_max: most,
        };
        // SAFETY: `setrlimit` is async-signal-safe and touches only the child between fork and exec.
        unsafe {
            cmd.pre_exec(move || match libc::setrlimit(libc::RLIMIT_NOFILE, &limit) {
                0 => Ok(()),
                _ => Err(std::io::Error::last_os_error()),
            });
        }
    }
    cmd.output().expect("spawn rivet")
}

fn said(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// The files a reset may hold open at once, below the family it takes.
const FEW_FILES: u64 = 256;

#[test]
fn state_reset_family_a_reset_larger_than_the_open_file_limit_clears_every_member() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let children = 300;
    let db = seeded(&cfg, &source_of(&cfg), children);
    let out = rivet(&cfg, &["state", "reset", "--export", "ev"], Some(FEW_FILES));
    let said = said(&out);
    assert!(out.status.success(), "{said}");
    assert!(
        said.contains(&format!("and its {children} partition(s)")),
        "{said}"
    );
    assert_eq!(held(&db, "export_state"), 0, "{said}");
    assert_eq!(held(&db, "export_progression"), 0, "{said}");
}

#[test]
fn state_reset_family_a_small_family_is_cleared_under_the_same_cap() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let db = seeded(&cfg, &source_of(&cfg), 20);
    let out = rivet(&cfg, &["state", "reset", "--export", "ev"], Some(FEW_FILES));
    let said = said(&out);
    assert!(out.status.success(), "{said}");
    assert!(said.contains("and its 20 partition(s)"), "{said}");
    assert_eq!(held(&db, "export_state"), 0, "{said}");
    assert_eq!(held(&db, "export_progression"), 0, "{said}");
}

/// A live run's lease on the partition `ev__00007`, held by this process through its own state handle.
fn child_lease(cfg: &Path) -> (StateStore, String) {
    let state = StateStore::open(&cfg.display().to_string()).unwrap();
    let key = rivet::pipeline::run_lease_key(&child(7), &source_of(cfg));
    (state, key)
}

#[test]
fn state_reset_family_a_live_run_of_a_child_refuses_the_reset_and_touches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let db = seeded(&cfg, &source_of(&cfg), 12);
    let (state, key) = child_lease(&cfg);
    let _held = state.try_load_lease(&key).unwrap().expect("a live run");
    let out = rivet(
        &cfg,
        &["--lock-wait", "0", "state", "reset", "--export", "ev"],
        None,
    );
    let said = said(&out);
    assert_eq!(out.status.code(), Some(2), "{said}");
    assert!(said.contains("RIVET_STATE_RUN_IN_PROGRESS"), "{said}");
    assert!(said.contains("export 'ev__00007'"), "{said}");
    assert_eq!(held(&db, "export_state"), 13, "nothing was reset: {said}");
    assert_eq!(held(&db, "export_progression"), 13, "{said}");
}

#[test]
fn state_reset_family_a_wait_that_runs_out_on_a_child_is_refused_by_code_and_touches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let db = seeded(&cfg, &source_of(&cfg), 12);
    let (state, key) = child_lease(&cfg);
    let _held = state.try_load_lease(&key).unwrap().expect("a live run");
    let out = rivet(
        &cfg,
        &["--lock-wait", "2", "state", "reset", "--export", "ev"],
        None,
    );
    let said = said(&out);
    assert_eq!(out.status.code(), Some(2), "{said}");
    assert!(said.contains("RIVET_STATE_LOCK_TIMEOUT"), "{said}");
    assert!(
        said.contains("the run lease of export 'ev__00007'"),
        "{said}"
    );
    assert!(said.contains("nothing was done"), "{said}");
    assert_eq!(held(&db, "export_state"), 13, "{said}");
    assert_eq!(held(&db, "export_progression"), 13, "{said}");
}

#[test]
fn state_reset_family_a_wait_on_a_child_ends_with_the_run_and_the_reset_clears_the_family() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let db = seeded(&cfg, &source_of(&cfg), 12);
    let (taken, took) = std::sync::mpsc::channel();
    let at = cfg.clone();
    let ends = std::thread::spawn(move || {
        let (state, key) = child_lease(&at);
        let run = state.try_load_lease(&key).unwrap().expect("a live run");
        taken.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(2));
        drop(run);
    });
    took.recv().unwrap();
    let out = rivet(
        &cfg,
        &["--lock-wait", "30", "state", "reset", "--export", "ev"],
        None,
    );
    ends.join().unwrap();
    let said = said(&out);
    assert!(out.status.success(), "{said}");
    assert_eq!(held(&db, "export_state"), 0, "{said}");
    assert_eq!(held(&db, "export_progression"), 0, "{said}");
}
