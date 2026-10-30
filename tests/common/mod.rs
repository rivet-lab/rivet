//! Shared test helpers for live-infrastructure integration tests.

#![allow(dead_code, unused_imports)]

use std::sync::atomic::{AtomicU64, Ordering};

mod bigquery;
mod canon;
mod clickhouse;
mod duckdb;
mod env;
mod mongo;
mod mssql;
mod mysql;
#[cfg(feature = "oracle")]
mod oracle;
mod parquet;
mod pg;
mod refusal;
mod registry;
mod rig;
mod runner;
mod sf_reader;
mod snowflake;
mod sql_engine;
mod state;
mod storage;
mod toxi;
mod verify;
mod warehouse;

pub use bigquery::*;
pub use canon::*;
pub use clickhouse::*;
pub use duckdb::*;
pub use env::*;
pub use mongo::*;
pub use mssql::*;
pub use mysql::*;
#[cfg(feature = "oracle")]
pub use oracle::*;
pub use parquet::*;
pub use pg::*;
pub use refusal::{FAILED_RUN_LEAVES_ENV, Leftover};
pub use registry::*;
pub use rig::*;
pub use runner::*;
pub use snowflake::*;
pub use sql_engine::*;
pub use state::*;
pub use storage::*;
pub use toxi::*;
pub use warehouse::*;

static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Build a globally-unique identifier safe to use as a SQL table name or an export name.
pub fn skip_live(why: &str) {
    let who = std::thread::current()
        .name()
        .unwrap_or("<unnamed test>")
        .to_string();
    let line = format!("RIVET-SKIP {who} — {why}");
    eprintln!("{line}");
    let path = std::env::var("RIVET_SKIP_LOG").unwrap_or_else(|_| {
        format!(
            "{}/rivet-skips.log",
            std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into())
        )
    });
    if let Some(dir) = std::path::Path::new(&path).parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    use std::io::Write as _;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let _ = f.write_all(format!("{line}\n").as_bytes());
    }
}

/// The Postgres state URL `RIVET_TEST_STATE_URL` names, or `None` after recording the skip.
pub fn pg_state_url() -> Option<String> {
    match std::env::var("RIVET_TEST_STATE_URL") {
        Ok(url) if url.starts_with("postgres") => Some(url),
        Ok(_) => {
            skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
            None
        }
        Err(_) => {
            skip_live("RIVET_TEST_STATE_URL unset");
            None
        }
    }
}

/// The re-baseline remedy every CDC data-loss message prints.
pub const REBASELINE_REMEDY: &str = "Re-baseline the stream in one run: delete the checkpoint \
     file if there is one; move every file out of the export's destination (for a `tables:` \
     export, every table's directory under it): the parts there still hold rows the source may \
     no longer have, and each table's snapshot/_SUCCESS marker goes with them; delete the \
     export's `cdc_snapshot` rows (one per table) from the state DB; give the export \
     `cdc.initial: snapshot` if it has none; and if a warehouse load consumes this stream, \
     truncate its `<table>__changes` table before the next load. That run anchors FIRST and \
     re-reads every table after, so nothing falls between the two. A separate `mode: full` \
     export does not re-baseline the stream.";

/// Edit the JSON checkpoint at `ckpt` in place.
pub fn rewrite_checkpoint(ckpt: &std::path::Path, edit: impl FnOnce(&mut serde_json::Value)) {
    let mut v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(ckpt).expect("a checkpoint to edit"))
            .expect("a JSON checkpoint");
    edit(&mut v);
    std::fs::write(ckpt, v.to_string()).expect("write the edited checkpoint");
}

/// Follow [`REBASELINE_REMEDY`] on a rig with a local destination, step by step as printed.
pub fn follow_rebaseline_remedy(rig: &mut Rig, has_baseline: bool) {
    apply_rebaseline_remedy(rig, has_baseline);
    rig.run_ok();
}

/// The steps of [`REBASELINE_REMEDY`] as printed, up to the run.
pub fn apply_rebaseline_remedy(rig: &mut Rig, has_baseline: bool) {
    let _ = std::fs::remove_file(rig.checkpoint());
    let out = rig.out_dir();
    std::fs::rename(&out, out.with_extension("pre-rebaseline"))
        .expect("move the destination's files aside");
    std::fs::create_dir_all(&out).expect("recreate the destination");
    let cleared = clear_cdc_snapshot(
        &rig.config_path(),
        rig.export_name(),
        &out.to_string_lossy(),
    );
    assert_eq!(
        cleared > 0,
        has_baseline,
        "fixture: {cleared} `cdc_snapshot` row(s) for an export whose baseline is {has_baseline}"
    );
    if !has_baseline {
        rig.amend_cdc_line("initial: snapshot");
    }
}

/// Assert the lost-checkpoint refusal by code with the re-baseline remedy and `says`.
fn assert_lost_checkpoint_refusal(rig: &Rig, says: &[&str]) {
    let out = rig.run_args(&[]);
    assert_refused(
        &out,
        Refused::by_code("RIVET_SOURCE_CDC_CHECKPOINT_INVALID", 5),
    );
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        says.iter().all(|p| said.contains(p)) && said.contains(REBASELINE_REMEDY),
        "the refusal says {says:?} and names the re-baseline remedy:\n{said}"
    );
}

/// Phrases of the refusal proven by the state DB's anchor record.
fn lost_anchor_phrases(rig: &Rig) -> Vec<String> {
    vec![
        "is missing, but the state DB records that export".to_string(),
        format!("`rivet state reset -c {}", rig.config_path().display()),
    ]
}

/// A checkpoint lost after a delivered run is refused on every later run with nothing written.
pub fn assert_a_lost_checkpoint_is_refused_not_reanchored(s: &mut CdcScenario, baseline: bool) {
    let phrases = match baseline {
        true => vec!["is missing but prior-run evidence exists".to_string()],
        false => lost_anchor_phrases(&s.rig),
    };
    let says: Vec<&str> = phrases.iter().map(String::as_str).collect();
    s.rig.run_ok();
    s.insert(1);
    s.settle();
    s.rig.run_ok();
    let delivered = files_under(&s.rig.out_dir());
    std::fs::remove_file(s.rig.checkpoint()).expect("the checkpoint the run wrote");
    s.insert(2);
    s.settle();
    assert_lost_checkpoint_refusal(&s.rig, &says);
    s.insert(3);
    s.settle();
    assert_lost_checkpoint_refusal(&s.rig, &says);
    assert!(
        !s.rig.checkpoint().exists(),
        "a refused run leaves no checkpoint behind"
    );
    assert_eq!(
        files_under(&s.rig.out_dir()),
        delivered,
        "a refused run writes nothing"
    );
}

/// `rivet state reset` accepts the loss and the stream delivers from the current position.
pub fn assert_state_reset_lifts_a_lost_checkpoint_refusal(s: &mut CdcScenario) {
    let phrases = lost_anchor_phrases(&s.rig);
    let says: Vec<&str> = phrases.iter().map(String::as_str).collect();
    s.rig.run_ok();
    s.insert(1);
    s.settle();
    s.rig.run_ok();
    std::fs::remove_file(s.rig.checkpoint()).expect("the checkpoint the run wrote");
    assert_lost_checkpoint_refusal(&s.rig, &says);
    let reset = s
        .rig
        .cli(&["state", "reset", "--export", s.rig.export_name()]);
    assert!(
        reset.status.success(),
        "`rivet state reset` runs from the refused state:\n{}",
        String::from_utf8_lossy(&reset.stderr)
    );
    s.rig.run_ok();
    s.insert(2);
    s.settle();
    s.rig.run_ok();
}

/// One source write on an `(id, v)` table, engine-neutral.
#[derive(Clone, Copy)]
pub enum Churn {
    Insert(i64),
    Update(i64),
    Delete(i64),
}

impl Churn {
    /// The statement for a SQL engine.
    pub fn sql(self, table: &str) -> String {
        match self {
            Churn::Insert(id) => format!("INSERT INTO {table} (id, v) VALUES ({id}, {id}0)"),
            Churn::Update(id) => format!("UPDATE {table} SET v = 99 WHERE id = {id}"),
            Churn::Delete(id) => format!("DELETE FROM {table} WHERE id = {id}"),
        }
    }
}

/// The text of a run's `RIVET_STATE_CDC_TABLE_REJOINED` refusal, from its code to the end.
pub fn rejoin_refusal(said: String) -> String {
    let at = said
        .find("[RIVET_STATE_CDC_TABLE_REJOINED]")
        .unwrap_or_else(|| panic!("the run names its refusal:\n{said}"));
    said[at..].to_string()
}

/// Every file under `dir`, recursively.
pub fn files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    for e in std::fs::read_dir(dir)
        .expect("read the directory")
        .flatten()
    {
        if e.path().is_dir() {
            found.extend(files_under(&e.path()));
        } else {
            found.push(e.path());
        }
    }
    found.sort();
    found
}

/// `tb` leaves `tables:`, changes, and comes back.
pub fn a_table_put_back_is_refused_until_rebaselined(
    mut rig: Rig,
    [ta, tb, tc]: [&str; 3],
    exec: &mut dyn FnMut(&str, Churn),
) {
    for t in [ta, tb, tc] {
        exec(t, Churn::Insert(1));
        exec(t, Churn::Insert(3));
    }
    rig = rig.tables(&[ta, tb, tc]).cdc_line("initial: snapshot");
    rig.run_ok();
    rig = rig.tables(&[ta, tc]);
    exec(tb, Churn::Insert(2));
    exec(tb, Churn::Update(1));
    exec(tb, Churn::Delete(3));
    exec(ta, Churn::Insert(2));
    rig.run_ok();
    rig = rig.tables(&[ta, tb, tc]);
    let dir = rig.out_dir().join(tb);
    let aside = dir.with_extension("before-rejoin");
    let files_out = {
        let (dir, aside) = (dir.clone(), aside.clone());
        move || std::fs::rename(&dir, &aside).expect("move the table's files out")
    };
    let first = rig.refuses_twice_and_walks_out(
        &["run"],
        &[],
        Refused::by_code("RIVET_STATE_CDC_TABLE_REJOINED", 5),
        vec![
            Remedy::wrong(
                "moves the table's files out and keeps its `cdc_snapshot` row",
                Then::Refuses(Refused::by_code("RIVET_STATE_CDC_TABLE_REJOINED", 5)),
                {
                    let files_out = files_out.clone();
                    move |_| files_out()
                },
            ),
            Remedy::new(
                &format!("move every file out of `file://{}`", dir.display()),
                Then::DeliversTheSource,
                {
                    let dir = dir.clone();
                    move |r| {
                        files_out();
                        let cleared = clear_cdc_snapshot(
                            &r.config_path(),
                            r.export_name(),
                            &dir.to_string_lossy(),
                        );
                        assert_eq!(
                            cleared, 1,
                            "fixture: the one `cdc_snapshot` row of the table put back"
                        );
                    }
                },
            ),
        ],
    );
    for said in [
        format!(
            "table `{tb}` is not among the tables this stream captured on its last run ({ta}, {tc})"
        ),
        format!(
            "delete the `cdc_snapshot` row of export '{}', table `{tb}`",
            rig.export_name()
        ),
    ] {
        assert!(first.contains(&said), "the refusal says `{said}`:\n{first}");
    }
    assert!(
        dir.join("snapshot").join("_SUCCESS").exists(),
        "the remedy's run took the table's baseline again"
    );
    exec(tb, Churn::Insert(4));
    rig.run_ok();
}

/// `table:` pointed at `tb` over `ta`'s baseline is refused twice.
pub fn a_table_switched_over_a_baseline_is_refused_and_switched_back_continues(
    mut rig: Rig,
    [ta, tb]: [&str; 2],
    repoint: &dyn Fn(Rig, &str) -> Rig,
    exec: &mut dyn FnMut(&str, Churn),
) {
    for t in [ta, tb] {
        exec(t, Churn::Insert(1));
    }
    rig = repoint(rig, ta).cdc_line("initial: snapshot");
    rig.run_ok();
    exec(ta, Churn::Insert(2));
    rig = repoint(rig, tb);
    let before = files_under(&rig.out_dir());
    let first = rejoin_refusal(rig.run_expect_fail());
    assert!(
        first.contains(&format!(
            "table `{tb}` is not among the tables this stream captured on its last run ({ta})"
        )),
        "the refusal names the table and the last capture:\n{first}"
    );
    assert_eq!(
        rejoin_refusal(rig.run_expect_fail()),
        first,
        "the second run refuses for the same reason"
    );
    assert_eq!(
        files_under(&rig.out_dir()),
        before,
        "a refused run writes nothing"
    );
    rig = repoint(rig, ta);
    rig.run_ok();
    assert!(
        files_under(&rig.out_dir()).len() > before.len(),
        "pointed back: the row written before the switch arrives as a change part"
    );
}

pub fn unique_name(prefix: &str) -> String {
    let c = NAME_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    format!("{prefix}_{pid}_{c}")
}

/// RAII cross-process lock for the suite's QUIET WINDOW.
pub struct QuietWindowGuard {
    _file: std::fs::File,
}

/// Cross-PROCESS serialization keyed by `name`.
pub fn cross_process_serial(name: &str) -> QuietWindowGuard {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join(format!("rivet_qa_serial_{name}.lock"));
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .unwrap_or_else(|e| panic!("open serial lock {}: {e}", path.display()));
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if rc != 0 {
        panic!(
            "flock(LOCK_EX) on {} failed: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }
    QuietWindowGuard { _file: file }
}

/// The stand's pgBouncer (one server connection) for one cell alone.
pub fn pgbouncer_alone() -> QuietWindowGuard {
    require_alive(LiveService::PgBouncer);
    cross_process_serial("pgbouncer")
}

/// Make the pooler's one server connection forget every prepared statement.
pub fn forget_pooled_statements() {
    postgres::Client::connect(PGBOUNCER_URL, postgres::NoTls)
        .expect("pgbouncer")
        .batch_execute("DEALLOCATE ALL")
        .expect("DEALLOCATE ALL through pgbouncer");
}

/// The backend behind the transaction pooler and every setting of its session with where it was set.
pub fn pooled_session() -> (i32, Vec<(String, String, String)>) {
    let mut c = postgres::Client::connect(PGBOUNCER_URL, postgres::NoTls).expect("pgbouncer");
    let pid = c.query_one("SELECT pg_backend_pid()", &[]).unwrap().get(0);
    let settings = c
        .query(
            "SELECT name, setting, source FROM pg_settings ORDER BY name",
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    (pid, settings)
}

/// The pooled session after the pooler's one server connection gave back the text formats an earlier client set on it.
pub fn pooled_session_without_set_formats() -> (i32, Vec<(String, String, String)>) {
    postgres::Client::connect(PGBOUNCER_URL, postgres::NoTls)
        .expect("pgbouncer")
        .batch_execute(
            "RESET TimeZone; RESET DateStyle; RESET IntervalStyle; RESET bytea_output; \
             RESET extra_float_digits",
        )
        .expect("RESET through pgbouncer");
    let session = pooled_session();
    let formats = [
        "TimeZone",
        "DateStyle",
        "IntervalStyle",
        "bytea_output",
        "extra_float_digits",
    ];
    let set: Vec<_> = (session.1.iter())
        .filter(|(name, _, source)| formats.contains(&name.as_str()) && source == "session")
        .collect();
    assert!(
        set.is_empty(),
        "fixture: the pooled connection still carries a session SET: {set:?}"
    );
    session
}

/// RAII background-writer.
pub struct BgWriter {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl BgWriter {
    pub fn spawn<F>(body: F) -> Self
    where
        F: FnOnce(&std::sync::atomic::AtomicBool) + Send + 'static,
    {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s2 = stop.clone();
        let handle = std::thread::spawn(move || body(&s2));
        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// Stop + join now (idempotent).
    pub fn stop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for BgWriter {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

pub fn quiet_window_guard() -> QuietWindowGuard {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("rivet_qa_quiet_window.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .unwrap_or_else(|e| panic!("open quiet-window lock {}: {e}", path.display()));
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if rc != 0 {
        panic!(
            "flock(LOCK_EX) on {} failed: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }
    QuietWindowGuard { _file: file }
}

/// Peak resident memory of any child process this test binary has reaped, in bytes.
#[cfg(unix)]
pub fn peak_child_rss_bytes() -> u64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_CHILDREN, &mut ru) } != 0 {
        return 0;
    }
    let raw = ru.ru_maxrss.max(0) as u64;
    if cfg!(target_os = "macos") {
        raw
    } else {
        raw * 1024
    }
}

#[cfg(not(unix))]
pub fn peak_child_rss_bytes() -> u64 {
    0
}
