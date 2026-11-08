//! P5 live matrix: a run stopped at every hook of the product, then run again, leaves no debris and delivers what an uninterrupted run delivers.
//! The axes come from the code (`p5_axes`); the grade is `dev/release_oracle/no_debris.py` (native listings, raw state SQL, a whole-directory DuckDB glob). One line per cell.

#[allow(dead_code)]
#[path = "../p5/axes.rs"]
mod p5;
use crate::common::*;
use crate::live_sabotage::seeded;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;

const PARK_MS: &str = "600000";
const PARK_CEILING: std::time::Duration = std::time::Duration::from_secs(120);
const SETTLE: std::time::Duration = std::time::Duration::from_millis(400);
const QUIET: std::time::Duration = std::time::Duration::from_secs(2);
const RUNNING_ROWS: &str =
    "SELECT COUNT(*) FROM run_status WHERE status = 'running' AND export_name = '{export}'";

/// The ids of `table` read by the engine's own client.
fn source_ids(engine: SqlEngine, table: &str) -> Vec<i64> {
    let text: Vec<String> = match engine {
        SqlEngine::Pg => pg_connect()
            .query(
                &format!("SELECT CAST(id AS TEXT) FROM {table} ORDER BY id"),
                &[],
            )
            .expect("pg read")
            .iter()
            .map(|r| r.get(0))
            .collect(),
        SqlEngine::Mysql => {
            use mysql::prelude::Queryable as _;
            mysql_connect()
                .query(format!("SELECT CAST(id AS CHAR) FROM {table} ORDER BY id"))
                .expect("mysql read")
        }
        SqlEngine::Mssql => mssql_query_strings(&format!(
            "SELECT CAST(id AS VARCHAR(30)) FROM {table} ORDER BY id"
        )),
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => ora_text_rows(&format!("SELECT TO_CHAR(id) FROM {table} ORDER BY id"))
            .into_iter()
            .map(|r| r[0].clone().expect("id"))
            .collect(),
    };
    text.iter()
        .map(|t| t.trim().parse().expect("an integer id"))
        .collect()
}

/// The oracle's answer to `verb` over `spec`, or why it did not answer.
fn oracle(verb: &str, spec: &Value) -> Result<String, String> {
    use std::io::Write as _;
    let mut child = std::process::Command::new(oracle_python())
        .args(["-m", "dev.release_oracle.no_debris", verb])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn the oracle: {e}"))?;
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(spec.to_string().as_bytes())
        .map_err(|e| format!("write the spec: {e}"))?;
    let out = child.wait_with_output().map_err(|e| format!("wait: {e}"))?;
    let err = String::from_utf8_lossy(&out.stderr);
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(format!(
            "exit {:?}: {}",
            out.status.code(),
            err.lines().last().unwrap_or("")
        ))
    }
}

/// The spec of one rig over one table: where it delivers, where its state lives, what the source holds.
fn spec(rig: &Rig, table: &str, source: &[i64]) -> Value {
    let state = match rig.state_url(&[]) {
        Some(url) => json!({"kind": "postgres", "url": url}),
        None => {
            json!({"kind": "sqlite", "path": rig.config_path().with_file_name(".rivet_state.db")})
        }
    };
    json!({
        "out_dir": rig.out_dir(),
        "state": state,
        "export": table,
        "lease_key": rig.lease_key(),
        "expected_ids": source,
        "id_column": "id",
    })
}

/// The listing and state rows of one moment, by the oracle.
fn snapshot(spec: &Value) -> Value {
    let said = oracle("snapshot", spec).unwrap_or_else(|e| panic!("snapshot: {e}"));
    serde_json::from_str(said.trim().lines().last().unwrap_or("")).expect("a snapshot is JSON")
}

/// What a snapshot holds of the things a stop may leave.
fn left_of(snap: &Value) -> p5::Left {
    p5::Left {
        staged: snap["files"]
            .as_object()
            .map_or(0, |f| f.keys().filter(|k| k.ends_with(".tmp")).count()),
        running: snap["running"].as_i64().unwrap_or(-1),
        leases: snap["leases"].as_i64().unwrap_or(-1),
    }
}

/// A rig of `shape` over `table` delivering to a local directory.
fn rig_for(engine: SqlEngine, table: &str, shape: p5::Shape) -> Rig {
    let (mode, lines) = shape.export().expect("a runnable shape");
    engine.staged(engine.rig(table), mode, lines)
}

/// The ids an uninterrupted run of `shape` delivers, read from its whole output directory.
fn clean_ids(engine: SqlEngine, shape: p5::Shape) -> Vec<i64> {
    let (table, _guard) = seeded(engine, "pm_p5");
    let source = source_ids(engine, &table);
    let rig = rig_for(engine, &table, shape);
    rig.run_ok();
    let said = oracle("rows", &spec(&rig, &table, &source)).unwrap_or_else(|e| panic!("rows: {e}"));
    let v: Value =
        serde_json::from_str(said.trim().lines().last().unwrap_or("")).expect("rows JSON");
    v["ids"]
        .as_array()
        .expect("ids")
        .iter()
        .map(|i| i.as_i64().expect("an id"))
        .collect()
}

/// The `running` run_status rows of the rig's export; zero while the state is not created yet.
fn running_rows(rig: &Rig) -> i64 {
    if rig.state_url(&[]).is_some() {
        return rig.state_count(RUNNING_ROWS);
    }
    let path = rig.config_path().with_file_name(".rivet_state.db");
    rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .and_then(|con| {
            con.query_row(
                &RUNNING_ROWS.replace("{export}", rig.export_name()),
                [],
                |r| r.get(0),
            )
        })
        .unwrap_or(0)
}

/// Whether the run has exited.
fn gone(run: &mut Spawned<'_>) -> bool {
    run.try_wait().expect("try_wait").is_some()
}

/// Start the run armed to park at `hook`, wait until it waits there, send `how`, reap it.
fn parked_and_signalled(rig: &Rig, hook: &p5::Hook, how: p5::Stop) -> Result<(), String> {
    let marker_dir = tempfile::tempdir().expect("a marker dir outside the destination");
    let marker = marker_dir.path().join("window");
    let marker_text = marker.display().to_string();
    let armed = hook.armed(PARK_MS);
    let mut envs: Vec<(&str, &str)> = vec![(hook.kind.env(), &armed)];
    match hook.kind {
        p5::Kind::PauseAt => envs.push(("RIVET_TEST_PAUSE_MARKER", &marker_text)),
        _ => envs.push(("RIVET_TEST_BLOCK_MS", PARK_MS)),
    }
    let mut run = rig.spawn_args_env(&[], &envs);
    let t0 = std::time::Instant::now();
    let mut quiet = (String::new(), std::time::Instant::now());
    loop {
        if gone(&mut run) {
            return Err(format!("the run ended before it parked at {}", hook.name));
        }
        let ready = match hook.kind {
            p5::Kind::PauseAt => marker.exists(),
            _ => {
                let seen = format!(
                    "{:?}/{}/{}",
                    files_under(&rig.out_dir()),
                    run.stderr_so_far().len(),
                    running_rows(rig)
                );
                if seen != quiet.0 {
                    quiet = (seen, std::time::Instant::now());
                }
                quiet.0.ends_with("/1") && quiet.1.elapsed() >= QUIET
            }
        };
        if ready {
            break;
        }
        if t0.elapsed() > PARK_CEILING {
            let _ = run.kill();
            let _ = run.wait();
            return Err(format!(
                "the run never parked at {} within {PARK_CEILING:?}",
                hook.name
            ));
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    std::thread::sleep(SETTLE);
    if gone(&mut run) {
        return Err(format!("the run ended while parked at {}", hook.name));
    }
    let sig = match how {
        p5::Stop::Kill => libc::SIGKILL,
        p5::Stop::Term => libc::SIGTERM,
        p5::Stop::Fault => unreachable!("a fault is not a signal"),
    };
    let rc = unsafe { libc::kill(run.id() as i32, sig) };
    if rc != 0 {
        return Err("the signal was not delivered".into());
    }
    run.wait().map(|_| ()).map_err(|e| format!("reap: {e}"))
}

/// One cell: stop, snapshot, run again, grade. Returns the verdict, its evidence and what the stop left.
fn grade_cell(
    engine: SqlEngine,
    cell: &p5::Cell,
    clean: &mut BTreeMap<p5::Shape, Vec<i64>>,
) -> (p5::Verdict, String, Option<p5::Left>) {
    let clean_set = clean
        .entry(cell.shape)
        .or_insert_with(|| clean_ids(engine, cell.shape))
        .clone();
    let (table, _guard) = seeded(engine, "pm_p5");
    let source = source_ids(engine, &table);
    let rig = rig_for(engine, &table, cell.shape);
    let base = spec(&rig, &table, &source);
    let before = snapshot(&base);

    let mut note = String::new();
    let mut rerun = true;
    match cell.stop {
        p5::Stop::Fault => {
            let armed = cell.hook.armed("0");
            let out = rig.run_with_envs(&[(cell.hook.kind.env(), &armed)]);
            let said = String::from_utf8_lossy(&out.stderr);
            let reached =
                out.status.code() == Some(70) || said.contains(&format!("'{}", cell.hook.name));
            if !reached {
                return (
                    p5::Verdict::NotGraded,
                    format!(
                        "hook not reached on this shape, run exited {:?}",
                        out.status.code()
                    ),
                    None,
                );
            }
            if out.status.success() {
                rerun = false;
                note = "fault retried through, no re-run; ".into();
            }
        }
        p5::Stop::Term | p5::Stop::Kill => {
            if let Err(why) = parked_and_signalled(&rig, &cell.hook, cell.stop) {
                return (p5::Verdict::NotGraded, why, None);
            }
        }
    }
    let left = left_of(&snapshot(&base));
    let held = format!(
        "{note}stop left staged={} running={} leases={}",
        left.staged, left.running, left.leases
    );

    let mut again = String::new();
    let mut tail = String::new();
    if rerun {
        let resume: &[&str] = if matches!(cell.shape, p5::Shape::Range | p5::Shape::Keyset) {
            &["--resume"]
        } else {
            &[]
        };
        match catch_unwind(AssertUnwindSafe(|| rig.run_args(resume))) {
            Ok(out) if out.status.success() => {}
            Ok(out)
                if out.status.code() == Some(5)
                    && String::from_utf8_lossy(&out.stderr)
                        .contains("RIVET_DEST_ALREADY_COMPLETE") =>
            {
                tail = "re-run refused, the destination was already whole; ".into();
            }
            Ok(out) => {
                let said = String::from_utf8_lossy(&out.stderr);
                again = format!(
                    "extra stop: the re-run exited {:?}: {}",
                    out.status.code(),
                    said.lines().last().unwrap_or("")
                );
            }
            Err(p) => {
                again = format!(
                    "extra stop: the rig oracle refused the re-run: {}",
                    panic_text(&p)
                );
            }
        }
    }
    let mut grade = base;
    grade["before"] = before;
    grade["clean_ids"] = json!(clean_set);
    let (verdict, evidence) =
        p5::oracle_verdict(oracle("grade", &grade).as_deref().map_err(String::clone));
    match (verdict, again.is_empty()) {
        (p5::Verdict::NotGraded, _) => (verdict, evidence, Some(left)),
        (_, false) => (
            p5::Verdict::Red,
            format!("{held}; {again}; {evidence}"),
            Some(left),
        ),
        _ => (verdict, format!("{held}; {tail}{evidence}"), Some(left)),
    }
}

/// The text of a caught panic.
fn panic_text(p: &Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<String>()
        .cloned()
        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "a panic".into())
        .lines()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Every cell of `engine`: one line each, the SIGTERM-versus-SIGKILL comparison, a summary; any RED fails the test.
fn matrix(engine: SqlEngine, name: &str) {
    engine.alive();
    let only = std::env::var("RIVET_P5_CELLS").ok();
    let all = p5::cells(Path::new(env!("CARGO_MANIFEST_DIR")), &[name.to_string()]);
    let mut verdicts = vec![];
    let mut reds = vec![];
    let mut clean = BTreeMap::new();
    let mut pairs: BTreeMap<String, (Option<p5::Left>, Option<p5::Left>)> = BTreeMap::new();
    for cell in all
        .iter()
        .filter(|c| only.as_deref().is_none_or(|o| c.key().contains(o)))
    {
        let (verdict, evidence, left) = match cell.not_graded() {
            Some(why) => (p5::Verdict::NotGraded, why, None),
            None => catch_unwind(AssertUnwindSafe(|| grade_cell(engine, cell, &mut clean)))
                .unwrap_or_else(|p| {
                    (
                        p5::Verdict::NotGraded,
                        format!("the cell could not run: {}", panic_text(&p)),
                        None,
                    )
                }),
        };
        let line = p5::line(&cell.key(), verdict, &evidence);
        println!("{line}");
        if verdict == p5::Verdict::Red {
            reds.push(line);
        }
        verdicts.push(verdict);
        let group = format!("{}/{}/{}", cell.engine, cell.shape.label(), cell.hook.name);
        match cell.stop {
            p5::Stop::Term => pairs.entry(group).or_default().0 = left,
            p5::Stop::Kill => pairs.entry(group).or_default().1 = left,
            p5::Stop::Fault => {}
        }
    }
    for (group, pair) in pairs {
        let (Some(term), Some(kill)) = (&pair.0, &pair.1) else {
            continue;
        };
        let key = format!("{group}/sigterm_vs_sigkill");
        let line = match p5::term_worse_than_kill(term, kill) {
            Some(why) => {
                let line = p5::line(&key, p5::Verdict::Red, &why);
                reds.push(line.clone());
                verdicts.push(p5::Verdict::Red);
                line
            }
            None => {
                verdicts.push(p5::Verdict::Pass);
                p5::line(&key, p5::Verdict::Pass, "SIGTERM left no more than SIGKILL")
            }
        };
        println!("{line}");
    }
    println!("{}", p5::summary(&verdicts));
    assert!(
        reds.is_empty(),
        "{} RED cell(s):\n{}",
        reds.len(),
        reds.join("\n")
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn no_debris_and_an_equal_rerun_after_every_stop_postgres() {
    matrix(SqlEngine::Pg, "pg");
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn no_debris_and_an_equal_rerun_after_every_stop_mysql() {
    matrix(SqlEngine::Mysql, "mysql");
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn no_debris_and_an_equal_rerun_after_every_stop_mssql() {
    matrix(SqlEngine::Mssql, "mssql");
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn no_debris_and_an_equal_rerun_after_every_stop_oracle() {
    matrix(SqlEngine::Oracle, "oracle");
}
