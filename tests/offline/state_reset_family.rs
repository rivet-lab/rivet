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
    let day = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap() + chrono::Days::new(i as u64);
    format!("ev__{}", day.format("%Y-%m-%d"))
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
            "INSERT INTO export_progression (export_name, last_committed_strategy, last_committed_cursor, last_committed_run_id, last_committed_at) VALUES (?1, 'incremental', '7', 'r', '2026-01-01T00:00:00Z')",
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

/// A live run's lease on the partition `child(7)`, held by this process through its own state handle.
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
    assert!(said.contains(&format!("export '{}'", child(7))), "{said}");
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
        said.contains(&format!("the run lease of export '{}'", child(7))),
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

#[test]
fn state_reset_family_a_reset_takes_the_partitions_of_its_labels_and_never_another_exports_name() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let db = seeded(&cfg, &source_of(&cfg), 3);
    for name in [
        "ev__archive",
        "ev__2026-02-30",
        "ev__x__2026-01-01",
        "ev__2026-01",
        "ev__2026",
    ] {
        db.execute(
            "INSERT INTO export_state (export_name, prefix, last_cursor_value) VALUES (?1, ?2, '7')",
            rusqlite::params![name, source_of(&cfg)],
        )
        .unwrap();
    }
    let out = rivet(&cfg, &["state", "reset", "--export", "ev"], None);
    let said = said(&out);
    assert!(out.status.success(), "{said}");
    assert!(said.contains("and its 5 partition(s)"), "{said}");
    assert!(said.contains("ev__2026-01"), "{said}");
    let left: Vec<String> = db
        .prepare("SELECT export_name FROM export_state ORDER BY 1")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        left,
        ["ev__2026-02-30", "ev__archive", "ev__x__2026-01-01"],
        "{said}"
    );
}

#[test]
fn state_reset_family_an_inspect_command_refuses_a_name_no_partition_could_carry_and_shows_a_held_one()
 {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let _db = seeded(&cfg, &source_of(&cfg), 3);
    for cmd in [
        &["state", "files"][..],
        &["state", "chunks"],
        &["state", "progression"],
        &["journal"],
        &["metrics"],
    ] {
        for name in ["ev__typo", "ev__1999-01-01"] {
            let mut args = cmd.to_vec();
            args.extend(["--export", name]);
            let out = rivet(&cfg, &args, None);
            let text = said(&out);
            assert_eq!(out.status.code(), Some(1), "{cmd:?} {name}: {text}");
            assert!(
                text.contains("RIVET_CLI_EXPORT_UNKNOWN"),
                "{cmd:?} {name}: {text}"
            );
            if name == "ev__typo" {
                assert!(
                    text.contains("is not defined in") && text.contains("Known exports: ev"),
                    "{cmd:?} {name}: {text}"
                );
                assert!(!text.contains("Partitions held"), "{cmd:?} {name}: {text}");
            } else {
                assert!(
                    text.contains("is a partition of 'ev' but the state holds nothing for it.")
                        && text.contains("`rivet state show -c ")
                        && !text.contains("Partitions held"),
                    "{cmd:?} {name}: {text}"
                );
            }
        }
    }
    let held = child(1);
    let out = rivet(&cfg, &["state", "progression", "--export", &held], None);
    let said = said(&out);
    assert!(out.status.success(), "{said}");
    assert!(said.contains(&held), "{said}");
    assert!(!said.contains("No progression boundaries"), "{said}");
}

#[test]
fn state_reset_family_reset_refuses_a_typo_naming_the_exports_and_an_unheld_label_naming_the_partitions()
 {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let _db = seeded(&cfg, &source_of(&cfg), 2);
    let out = rivet(&cfg, &["state", "reset", "--export", "ev__typo"], None);
    let text = said(&out);
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(
        text.contains("RIVET_CLI_EXPORT_UNKNOWN")
            && text.contains("export 'ev__typo' not found in config")
            && text.contains("Known exports: ev")
            && !text.contains("Partitions held"),
        "{text}"
    );
    let out = rivet(
        &cfg,
        &["state", "reset", "--export", "ev__1999-01-01"],
        None,
    );
    let text = said(&out);
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(
        text.contains("RIVET_CLI_EXPORT_UNKNOWN")
            && text.contains("export 'ev__1999-01-01' is a partition of 'ev' but holds no progress on this source.")
            && text.contains(&format!("Partitions held: {}, {}", child(0), child(1))),
        "{text}"
    );
}

#[test]
fn state_reset_family_a_partition_stored_under_another_granularity_is_shown_and_reset_with_the_export()
 {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let db = seeded(&cfg, &source_of(&cfg), 2);
    for name in ["ev__2026-01", "ev__2026-02", "ev__2026"] {
        db.execute(
            "INSERT INTO export_state (export_name, prefix, last_cursor_value) VALUES (?1, ?2, '7')",
            rusqlite::params![name, source_of(&cfg)],
        )
        .unwrap();
        db.execute(
            "INSERT INTO export_progression (export_name, last_committed_strategy, last_committed_cursor, last_committed_run_id, last_committed_at) VALUES (?1, 'incremental', '7', 'r', '2026-01-01T00:00:00Z')",
            rusqlite::params![name],
        )
        .unwrap();
    }
    let show = rivet(&cfg, &["state", "show"], None);
    let text = said(&show);
    assert!(show.status.success(), "{text}");
    for name in ["ev__2026-01", "ev__2026-02", "ev__2026 "] {
        assert!(text.contains(name), "{name}: {text}");
    }
    let out = rivet(
        &cfg,
        &["state", "progression", "--export", "ev__2026-01"],
        None,
    );
    let text = said(&out);
    assert!(
        out.status.success() && text.contains("ev__2026-01"),
        "{text}"
    );
    assert!(!text.contains("No progression boundaries"), "{text}");
    let out = rivet(&cfg, &["state", "reset", "--export", "ev__2026-02"], None);
    assert!(out.status.success(), "{}", said(&out));
    assert_eq!(held(&db, "export_state"), 5);
    let out = rivet(&cfg, &["state", "reset", "--export", "ev"], None);
    let text = said(&out);
    assert!(out.status.success(), "{text}");
    assert!(text.contains("and its 4 partition(s)"), "{text}");
    assert_eq!(held(&db, "export_state"), 0, "{text}");
    assert_eq!(held(&db, "export_progression"), 0, "{text}");
}
