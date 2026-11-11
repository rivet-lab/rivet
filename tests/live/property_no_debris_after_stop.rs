//! Live matrix: a run stopped at every hook, then run again, leaves no debris and delivers what an uninterrupted run does.

use crate::common::*;
use crate::live_sabotage::{N, mongo_rig, seeded};
use crate::property_no_debris_after_stop_axes as axes;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;

const PARK_MS: &str = "600000";
const PARK_CEILING: std::time::Duration = std::time::Duration::from_secs(120);
const SETTLE: std::time::Duration = std::time::Duration::from_millis(400);
const QUIET: std::time::Duration = std::time::Duration::from_secs(2);
const LOAD_BUCKET: &str = "rivet-qa-nodebris-load";
const CH_PASSWORD_ENV: &str = "RIVET_TEST_CH_PASSWORD";
const STORE_BUCKET: &str = "rivet-qa-nodebris";
const SOURCE_URL_ENV: &str = "RIVET_NODEBRIS_SOURCE_URL";
const MUTATION_WAIT: std::time::Duration = std::time::Duration::from_secs(15);
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

/// A source of the matrix: a SQL engine, or MongoDB.
#[derive(Clone, Copy, Debug)]
enum Src {
    Sql(SqlEngine),
    Mongo,
}

impl Src {
    /// Fail fast when the source's service is down.
    fn alive(self) {
        match self {
            Src::Sql(engine) => engine.alive(),
            Src::Mongo => require_alive(LiveService::Mongo),
        }
    }

    /// The column that carries the source's ids.
    fn id_column(self) -> &'static str {
        match self {
            Src::Sql(_) => "id",
            Src::Mongo => "_id",
        }
    }

    /// A freshly seeded table, its base rig, its ids and its drop guard.
    fn seed(self) -> (Rig, String, Vec<i64>, Box<dyn std::any::Any>) {
        match self {
            Src::Sql(engine) => {
                let (table, guard) = seeded(engine, "pm_nodebris");
                let ids = source_ids(engine, &table);
                (engine.rig(&table), table, ids, guard)
            }
            Src::Mongo => {
                let (rig, guard) = mongo_rig("pm_nodebris");
                (rig, "t".into(), (1..=N).collect(), Box::new(guard))
            }
        }
    }

    /// `rig` restaged to `shape`, plus `extra` export lines.
    fn staged(self, rig: Rig, shape: axes::Shape, extra: &[&str]) -> Rig {
        let (mode, lines) = shape.export().expect("a runnable shape");
        let lines: Vec<&str> = lines.iter().chain(extra).copied().collect();
        match self {
            Src::Sql(engine) => engine.staged(rig, mode, &lines),
            Src::Mongo => rig.restage(mode, &lines),
        }
    }
}

/// The oracle's answer to `verb` over `spec`, or why it did not answer.
fn oracle(verb: &str, spec: &Value) -> Result<String, String> {
    use std::io::Write as _;
    let mut child = std::process::Command::new(oracle_python())
        .args([
            "-m",
            "dev.release_oracle.property_no_debris_after_stop",
            verb,
        ])
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
fn spec(rig: &Rig, src: Src, source: &[i64]) -> Value {
    let state = match rig.state_url(&[]) {
        Some(url) => json!({"kind": "postgres", "url": url}),
        None => {
            json!({"kind": "sqlite", "path": rig.config_path().with_file_name(".rivet_state.db")})
        }
    };
    json!({
        "out_dir": rig.out_dir(),
        "state": state,
        "export": rig.export_name(),
        "lease_key": rig.lease_key(),
        "expected_ids": source,
        "id_column": src.id_column(),
    })
}

/// The listing and state rows of one moment, by the oracle.
fn snapshot(spec: &Value) -> Value {
    let said = oracle("snapshot", spec).unwrap_or_else(|e| panic!("snapshot: {e}"));
    serde_json::from_str(said.trim().lines().last().unwrap_or("")).expect("a snapshot is JSON")
}

/// What a snapshot holds of the things a stop may leave.
fn left_of(snap: &Value) -> axes::Left {
    axes::Left {
        staged: snap["files"]
            .as_object()
            .map_or(0, |f| f.keys().filter(|k| k.ends_with(".tmp")).count()),
        running: snap["running"].as_i64().unwrap_or(-1),
        leases: snap["leases"].as_i64().unwrap_or(-1),
    }
}

/// The ids an uninterrupted run of `shape` delivers, read from its whole output directory.
fn clean_ids(src: Src, shape: axes::Shape) -> Vec<i64> {
    let (base, _table, source, _guard) = src.seed();
    let rig = src.staged(base, shape, &[]);
    rig.run_ok();
    let said = oracle("rows", &spec(&rig, src, &source)).unwrap_or_else(|e| panic!("rows: {e}"));
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
    if let Some(url) = rig.state_url(&[]) {
        return postgres::Client::connect(&url, postgres::NoTls)
            .and_then(|mut con| {
                con.query_one(
                    RUNNING_ROWS.replace("{export}", rig.export_name()).as_str(),
                    &[],
                )
            })
            .map(|r| r.get(0))
            .unwrap_or(0);
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
fn parked_and_signalled(
    rig: &Rig,
    hook: &axes::Hook,
    how: axes::Stop,
    base_envs: &[(&str, &str)],
    while_parked: &mut dyn FnMut() -> String,
) -> Result<String, String> {
    let marker_dir = tempfile::tempdir().expect("a marker dir outside the destination");
    let marker = marker_dir.path().join("window");
    let marker_text = marker.display().to_string();
    let armed = hook.armed(PARK_MS);
    let mut envs: Vec<(&str, &str)> = base_envs.to_vec();
    envs.push((hook.kind.env(), &armed));
    match hook.kind {
        axes::Kind::PauseAt => envs.push(("RIVET_TEST_PAUSE_MARKER", &marker_text)),
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
            axes::Kind::PauseAt => marker.exists(),
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
    let note = while_parked();
    if gone(&mut run) {
        return Err(format!(
            "the run ended while parked at {} before the signal; {note}",
            hook.name
        ));
    }
    let sig = match how {
        axes::Stop::Kill => libc::SIGKILL,
        axes::Stop::Term => libc::SIGTERM,
        axes::Stop::Fault | axes::Stop::HostCrash | axes::Stop::SourceEvent => {
            unreachable!("only a parked run takes a signal")
        }
    };
    let rc = unsafe { libc::kill(run.id() as i32, sig) };
    if rc != 0 {
        return Err("the signal was not delivered".into());
    }
    run.wait().map(|_| note).map_err(|e| format!("reap: {e}"))
}

/// One ClickHouse statement over HTTP, the body or why it failed.
fn ch(sql: &str) -> Result<String, String> {
    let resp = reqwest::blocking::Client::new()
        .post(CLICKHOUSE_HTTP_URL)
        .basic_auth(CLICKHOUSE_USER, Some(CLICKHOUSE_PASSWORD))
        .body(sql.to_string())
        .send()
        .map_err(|e| e.to_string())?;
    let ok = resp.status().is_success();
    let body = resp.text().unwrap_or_default();
    if ok { Ok(body) } else { Err(body) }
}

/// A scratch ClickHouse database, dropped on drop.
struct ChDb(String);

impl ChDb {
    /// A new database named from the matrix prefix.
    fn new() -> Self {
        let name = unique_name("pm_nodebris_ch");
        ch(&format!("CREATE DATABASE {name}")).expect("create the ClickHouse database");
        ChDb(name)
    }
}

impl Drop for ChDb {
    fn drop(&mut self) {
        let _ = ch(&format!("DROP DATABASE IF EXISTS {}", self.0));
    }
}

/// A load cell: export to the fake-gcs stage, load into ClickHouse armed at the hook, load again, grade what ClickHouse holds.
fn grade_load(src: Src, cell: &axes::Cell) -> (axes::Verdict, String, Option<axes::Left>) {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(LOAD_BUCKET);
    let (base, _table, source, _guard) = src.seed();
    let db = ChDb::new();
    let rig = src
        .staged(base, cell.shape, &[])
        .dest_gcs(
            LOAD_BUCKET,
            &unique_name("pm_nodebris_ld"),
            FAKE_GCS_ENDPOINT,
        )
        .top_line(&format!(
            "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {}, \
             user: {CLICKHOUSE_USER}, password_env: {CH_PASSWORD_ENV}, pk: [id] }}",
            db.0
        ));
    rig.run_ok();
    let armed = cell.hook.armed("0");
    let out = rig.load_args_env(
        &[],
        &[
            (CH_PASSWORD_ENV, CLICKHOUSE_PASSWORD),
            (cell.hook.kind.env(), &armed),
        ],
    );
    let said = String::from_utf8_lossy(&out.stderr);
    if out.status.code() != Some(70) && !said.contains(&format!("'{}", cell.hook.name)) {
        return (
            axes::Verdict::NotGraded,
            format!(
                "hook not reached by this load, it exited {:?}",
                out.status.code()
            ),
            None,
        );
    }
    let mut again = String::new();
    if !out.status.success() {
        let re = rig.load_args_env(&[], &[(CH_PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
        if !re.status.success() {
            again = format!(
                "extra stop: the re-load exited {:?}: {}",
                re.status.code(),
                String::from_utf8_lossy(&re.stderr)
                    .lines()
                    .last()
                    .unwrap_or("")
            );
        }
    }
    let spec = json!({
        "clickhouse": {"url": CLICKHOUSE_HTTP_URL, "user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD, "database": db.0},
        "table": rig.export_name(),
        "id_column": src.id_column(),
        "expected_ids": source,
    });
    let (verdict, evidence) =
        axes::oracle_verdict(oracle("warehouse", &spec).as_deref().map_err(String::clone));
    match (verdict, again.is_empty()) {
        (axes::Verdict::NotGraded, _) | (_, true) => (verdict, evidence, None),
        (_, false) => (axes::Verdict::Red, format!("{again}; {evidence}"), None),
    }
}

/// A cloud destination of a variant and the prefix its export lands under.
enum Remote {
    S3(String),
    Gcs(String),
    Azure(String),
}

impl Remote {
    /// Pull everything under the prefix into `into`, emptied first.
    fn pull(&self, into: &Path) {
        let _ = std::fs::remove_dir_all(into);
        std::fs::create_dir_all(into).expect("a mirror directory");
        match self {
            Remote::S3(prefix) => {
                minio_pull_prefix(STORE_BUCKET, prefix, into);
            }
            Remote::Gcs(prefix) => {
                gcs_pull_prefix(FAKE_GCS_ENDPOINT, STORE_BUCKET, prefix, None, into);
            }
            Remote::Azure(prefix) => {
                azure_pull_prefix(AZURITE_ENDPOINT, STORE_BUCKET, prefix, into);
            }
        }
    }
}

/// A webhook receiver on a free port: its URL and the bodies it got.
fn webhook_receiver() -> (String, std::sync::mpsc::Receiver<String>) {
    use std::io::{Read as _, Write as _};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a webhook port");
    let url = format!(
        "http://{}/hook",
        listener.local_addr().expect("its address")
    );
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut s) = conn else { return };
            let _ = s.set_read_timeout(Some(std::time::Duration::from_secs(2)));
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            while let Ok(n) = s.read(&mut chunk) {
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf);
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let want = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if body.len() >= want {
                        break;
                    }
                }
            }
            let _ = s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok");
            let text = String::from_utf8_lossy(&buf).into_owned();
            let body = text
                .split_once("\r\n\r\n")
                .map_or(text.clone(), |(_, b)| b.to_string());
            if tx.send(body).is_err() {
                return;
            }
        }
    });
    (url, rx)
}

/// Apply the mutation `value` to `table`; a drop-and-recreate rebuilds it with the same ids.
fn mutate(engine: SqlEngine, table: &str, value: &str) {
    match value {
        "insert" => engine.insert(table, N + 1..=N + 5, 180, Some(10)),
        "update" => engine.exec(&format!("UPDATE {table} SET time_spent = 99 WHERE id <= 5")),
        "delete" => engine.exec(&format!("DELETE FROM {table} WHERE id <= 5")),
        "add_column" => engine.exec(&format!("ALTER TABLE {table} ADD extra_col INT")),
        "truncate" => engine.exec(&format!("TRUNCATE TABLE {table}")),
        "drop_recreate" => {
            engine.exec(&format!("DROP TABLE {table}"));
            engine.range_table_again(table);
            engine.insert(table, 1..=N, 180, Some(10));
        }
        other => panic!("no statement for the mutation {other}"),
    }
}

/// The run id of the export's `running` run_status row, by `rivet state runs --running --json`.
fn running_run_id(rig: &Rig, envs: &[(&str, &str)]) -> Option<String> {
    let out = rig.cli_env(&["state", "runs", "--running", "--json"], envs);
    let v: Value = serde_json::from_slice(&out.stdout).ok()?;
    v.as_array()?
        .iter()
        .find(|r| {
            r["export_name"]
                .as_str()
                .is_none_or(|e| e == rig.export_name())
        })
        .and_then(|r| r["run_id"].as_str().map(String::from))
}

/// One variant cell: the base cell with one axis turned to `value`.
fn grade_variant(
    src: Src,
    v: &axes::Variant,
    clean: &mut BTreeMap<axes::Shape, Vec<i64>>,
) -> (axes::Verdict, String) {
    match v.axis {
        axes::Axis::Warehouse => (
            axes::Verdict::NotGraded,
            "only the ClickHouse leg of a load is on the stand".into(),
        ),
        _ => {
            let (verdict, evidence, _) = grade(src, &v.base, Some((v.axis, &v.value)), clean);
            (verdict, evidence)
        }
    }
}

/// One base cell: stop, snapshot, run again, grade. Returns the verdict, its evidence and what the stop left.
fn grade_cell(
    src: Src,
    cell: &axes::Cell,
    clean: &mut BTreeMap<axes::Shape, Vec<i64>>,
) -> (axes::Verdict, String, Option<axes::Left>) {
    if cell.shape == axes::Shape::Load {
        return grade_load(src, cell);
    }
    grade(src, cell, None, clean)
}

/// The exit of an invocation: its code, or `None` when a signal ended it.
fn exit_of(out: &std::process::Output) -> Option<i32> {
    out.status.code()
}

/// `cell` stopped and re-run with one axis turned to a value (or none for the base cell), then graded by the oracle.
fn grade(
    src: Src,
    cell: &axes::Cell,
    twist: Option<(axes::Axis, &str)>,
    clean: &mut BTreeMap<axes::Shape, Vec<i64>>,
) -> (axes::Verdict, String, Option<axes::Left>) {
    use axes::Axis;
    let value = twist.map_or("", |t| t.1);
    let axis = twist.map(|t| t.0);
    let clean_set = clean
        .entry(cell.shape)
        .or_insert_with(|| clean_ids(src, cell.shape))
        .clone();
    let (seeded_rig, table, source, _guard) = src.seed();
    let mut keep: Vec<Box<dyn std::any::Any>> = vec![];
    let mut owned_envs: Vec<(&'static str, String)> = vec![];
    let mut remote = None;
    let extra: &[&str] = match axis {
        Some(Axis::Parallel) => &["parallel: 4"],
        _ => &[],
    };
    let mut rig = src.staged(seeded_rig, cell.shape, extra);
    match axis {
        Some(Axis::Destination) => {
            let prefix = unique_name("pm_nodebris_st");
            let export = rig.export_name().to_string();
            match value {
                "s3" => {
                    require_alive(LiveService::Minio);
                    ensure_minio_bucket(STORE_BUCKET);
                    rig = rig.dest_s3(STORE_BUCKET, &prefix, MINIO_ENDPOINT);
                    owned_envs.push(("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY.into()));
                    owned_envs.push(("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY.into()));
                    remote = Some(Remote::S3(format!("{prefix}/{export}/")));
                }
                "gcs" => {
                    require_alive(LiveService::FakeGcs);
                    ensure_gcs_bucket(STORE_BUCKET);
                    rig = rig.dest_gcs(STORE_BUCKET, &prefix, FAKE_GCS_ENDPOINT);
                    remote = Some(Remote::Gcs(format!("{prefix}/{export}/")));
                }
                "azure" => {
                    require_alive(LiveService::Azurite);
                    ensure_azure_container(STORE_BUCKET);
                    rig = rig.dest_azure(STORE_BUCKET, &prefix);
                    owned_envs.push(("RIVET_TEST_AZURITE_KEY", AZURITE_KEY.into()));
                    remote = Some(Remote::Azure(format!("{prefix}/{export}/")));
                }
                other => panic!("no destination leg for {other}"),
            }
        }
        Some(Axis::State) if value == "postgres" => {
            let db = ScratchStateDb::new("pm_nodebris_state");
            rig = rig.on_postgres_state(db.url());
            keep.push(Box::new(db));
        }
        Some(Axis::Entry) if value == "apply" => {
            let Src::Sql(engine) = src else {
                unreachable!("apply is built for SQL rigs")
            };
            rig = rig.source_url_env(SOURCE_URL_ENV);
            owned_envs.push((SOURCE_URL_ENV, engine.url().to_string()));
        }
        Some(Axis::RunEnd) => {
            let (url, got) = webhook_receiver();
            rig = rig.top_line(&format!(
                "notifications:\n  slack:\n    webhook_url: \"{url}\"\n    on: [failure]"
            ));
            keep.push(Box::new(got));
        }
        _ => {}
    }
    let envs: Vec<(&str, &str)> = owned_envs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let mirror = tempfile::tempdir().expect("a mirror of the destination");
    let mut base = spec(&rig, src, &source);
    if remote.is_some() {
        base["out_dir"] = json!(mirror.path());
    }
    let look = |spec: &Value| {
        if let Some(r) = &remote {
            r.pull(mirror.path());
        }
        snapshot(spec)
    };
    let before = look(&base);
    let plan_dir = tempfile::tempdir().expect("a plan directory");
    let plan = plan_dir.path().join("plan.json");
    if axis == Some(Axis::Entry) && value == "apply" {
        let out = rig.plan_json_env(&plan, &[], &envs);
        if !out.status.success() {
            return (
                axes::Verdict::NotGraded,
                format!(
                    "plan exited {:?}: {}",
                    out.status.code(),
                    String::from_utf8_lossy(&out.stderr)
                        .lines()
                        .last()
                        .unwrap_or("")
                ),
                None,
            );
        }
    }

    let mut exits: Vec<Option<i32>> = vec![];
    let mut note: String;
    let mut rerun = true;
    match cell.stop {
        axes::Stop::Fault => {
            let armed = cell.hook.armed("0");
            let mut armed_envs = envs.clone();
            armed_envs.push((cell.hook.kind.env(), &armed));
            let out = if axis == Some(Axis::Entry) && value == "apply" {
                rig.apply_env(&plan, &[], &armed_envs)
            } else {
                rig.run_args_env(&[], &armed_envs)
            };
            exits.push(exit_of(&out));
            let said = String::from_utf8_lossy(&out.stderr);
            let reached =
                out.status.code() == Some(70) || said.contains(&format!("'{}", cell.hook.name));
            if !reached {
                return (
                    axes::Verdict::NotGraded,
                    format!(
                        "hook not reached on this shape, run exited {:?}",
                        out.status.code()
                    ),
                    None,
                );
            }
            note = format!("fault exited {:?}; ", out.status.code());
            if out.status.success() {
                rerun = false;
                note = format!("{note}fault retried through, no re-run; ");
            }
        }
        axes::Stop::Term | axes::Stop::Kill => {
            let mut while_parked = || -> String {
                match (axis, src) {
                    (Some(Axis::Mutation), Src::Sql(engine)) => {
                        let (t, v) = (table.clone(), value.to_string());
                        let job = std::thread::spawn(move || {
                            catch_unwind(AssertUnwindSafe(|| mutate(engine, &t, &v)))
                                .map_err(|p| panic_text(&p))
                        });
                        let t0 = std::time::Instant::now();
                        while !job.is_finished() && t0.elapsed() < MUTATION_WAIT {
                            std::thread::sleep(std::time::Duration::from_millis(200));
                        }
                        if job.is_finished() {
                            match job.join().expect("the mutation thread") {
                                Ok(()) => format!("{value} applied while the run was parked"),
                                Err(e) => format!("{value} failed while parked: {e}"),
                            }
                        } else {
                            keep.push(Box::new(job));
                            format!(
                                "{value} waited on the parked run's lock and lands after the stop"
                            )
                        }
                    }
                    (Some(Axis::Refusal), _) => {
                        let seen = look(&base);
                        let second =
                            catch_unwind(AssertUnwindSafe(|| rig.run_args_env(&[], &envs)));
                        let after = look(&base);
                        match second {
                            Ok(out) => {
                                let said = String::from_utf8_lossy(&out.stderr);
                                let coded = said.contains(value);
                                let same = seen["files"] == after["files"];
                                if out.status.code() == Some(2) && coded && same {
                                    format!(
                                        "a second run beside it exited 2 with {value} and wrote nothing"
                                    )
                                } else {
                                    format!(
                                        "REFUSAL_MISS a second run beside it exited {:?}, code named: {coded}, destination unchanged: {same}: {}",
                                        out.status.code(),
                                        said.lines().last().unwrap_or("")
                                    )
                                }
                            }
                            Err(p) => format!(
                                "REFUSAL_MISS the second run panicked the rig: {}",
                                panic_text(&p)
                            ),
                        }
                    }
                    _ => String::new(),
                }
            };
            match parked_and_signalled(&rig, &cell.hook, cell.stop, &envs, &mut while_parked) {
                Ok(said) => note = format!("{said}; "),
                Err(why) => return (axes::Verdict::NotGraded, why, None),
            }
            exits.push(None);
        }
        axes::Stop::HostCrash | axes::Stop::SourceEvent => {
            unreachable!("not a runnable stop")
        }
    }
    let left = left_of(&look(&base));
    let held = format!(
        "{note}stop left staged={} running={} leases={}",
        left.staged, left.running, left.leases
    );
    if let Some(Axis::Mutation) = axis {
        while let Some(job) = keep.pop() {
            if let Ok(job) = job.downcast::<std::thread::JoinHandle<Result<(), String>>>() {
                let _ = job.join();
            }
        }
    }
    let mut finish = String::new();
    if axis == Some(Axis::Entry) && value == "state" {
        match running_run_id(&rig, &envs) {
            Some(id) => {
                let out = rig.cli_env(&["state", "finish-run", "--run-id", &id], &envs);
                finish = format!("state finish-run exited {:?}; ", out.status.code());
                if !out.status.success() {
                    finish = format!(
                        "extra stop: state finish-run exited {:?}: {}; ",
                        out.status.code(),
                        String::from_utf8_lossy(&out.stderr)
                            .lines()
                            .last()
                            .unwrap_or("")
                    );
                }
            }
            None => finish = "no running row left to finish; ".into(),
        }
    }
    let mut rerun_rig = None;
    if axis == Some(Axis::State) && value == "second_state_db" {
        let Src::Sql(engine) = src else {
            unreachable!("a second rig over the same source is built for SQL")
        };
        let dir = tempfile::tempdir().expect("a second config directory");
        let again = src
            .staged(engine.rig(&table), cell.shape, extra)
            .export_named(rig.export_name())
            .dest_path(rig.out_dir())
            .config_dir(dir.path().to_path_buf());
        keep.push(Box::new(dir));
        rerun_rig = Some(again);
    }
    let runner = rerun_rig.as_ref().unwrap_or(&rig);

    let mut again = String::new();
    let mut tail = String::new();
    if rerun {
        let resume: &[&str] = if rerun_rig.is_none()
            && matches!(cell.shape, axes::Shape::Range | axes::Shape::Keyset)
        {
            &["--resume"]
        } else {
            &[]
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            if axis == Some(Axis::Entry) && value == "apply" {
                runner.apply_env(&plan, resume, &envs)
            } else {
                runner.run_args_env(resume, &envs)
            }
        }));
        match result {
            Ok(out) if out.status.success() => exits.push(Some(0)),
            Ok(out)
                if out.status.code() == Some(5)
                    && String::from_utf8_lossy(&out.stderr)
                        .contains("RIVET_DEST_ALREADY_COMPLETE") =>
            {
                exits.push(exit_of(&out));
                tail = "re-run refused, the destination was already whole; ".into();
            }
            Ok(out) => {
                exits.push(exit_of(&out));
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
    if finish.starts_with("extra stop") {
        again = format!("{finish}{again}");
    }
    let mut grade = base;
    if let Some(r) = &rerun_rig {
        let fresh = spec(r, src, &source);
        grade["state"] = fresh["state"].clone();
        grade["lease_key"] = fresh["lease_key"].clone();
        exits = exits.split_off(exits.len().saturating_sub(1));
    }
    grade["before"] = before;
    grade["clean_ids"] = json!(clean_set);
    grade["exits"] = json!(exits);
    if let (Some(Axis::Mutation), Src::Sql(engine)) = (axis, src) {
        let now = source_ids(engine, &table);
        let required: Vec<i64> = source.iter().filter(|i| now.contains(i)).copied().collect();
        let mut allowed = source.clone();
        allowed.extend(now.iter().filter(|i| !source.contains(i)));
        grade["expected_ids"] = json!(required);
        grade["allowed_ids"] = json!(allowed);
        grade["clean_ids"] = Value::Null;
    }
    if let Some(r) = &remote {
        r.pull(mirror.path());
    }
    let (verdict, evidence) =
        axes::oracle_verdict(oracle("grade", &grade).as_deref().map_err(String::clone));
    let mut extra_red = String::new();
    if note.contains("REFUSAL_MISS") {
        extra_red = "a second run beside the live one was not refused cleanly; ".into();
    }
    if axis == Some(Axis::RunEnd) {
        let got = keep
            .iter()
            .find_map(|k| k.downcast_ref::<std::sync::mpsc::Receiver<String>>());
        match got.map(|rx| rx.recv_timeout(std::time::Duration::from_secs(10))) {
            Some(Ok(body)) if body.contains(rig.export_name()) => {
                tail = format!("{tail}the failure webhook named the export; ");
            }
            Some(Ok(body)) => {
                extra_red = format!(
                    "{extra_red}the failure webhook did not name the export: {}; ",
                    body.chars().take(160).collect::<String>()
                );
            }
            _ => extra_red = format!("{extra_red}no failure webhook within 10s of the fault; "),
        }
    }
    let mut out_left = Some(left);
    if axis.is_some() {
        out_left = None;
    }
    match (verdict, again.is_empty() && extra_red.is_empty()) {
        (axes::Verdict::NotGraded, _) => (verdict, evidence, out_left),
        (_, false) => (
            axes::Verdict::Red,
            format!("{held}; {extra_red}{again}; {evidence}"),
            out_left,
        ),
        _ => (
            verdict,
            format!("{held}; {finish}{tail}{evidence}"),
            out_left,
        ),
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
fn matrix(src: Src, name: &str) {
    src.alive();
    let only = std::env::var("RIVET_NO_DEBRIS_AFTER_STOP_CELLS").ok();
    let all = axes::cells(Path::new(env!("CARGO_MANIFEST_DIR")), &[name.to_string()]);
    let mut verdicts = vec![];
    let mut reds: Vec<(String, String)> = vec![];
    let mut clean = BTreeMap::new();
    let mut pairs: BTreeMap<String, (Option<axes::Left>, Option<axes::Left>)> = BTreeMap::new();
    let mut exit_classes: BTreeMap<String, BTreeMap<&str, String>> = BTreeMap::new();
    for cell in all
        .iter()
        .filter(|c| only.as_deref().is_none_or(|o| c.key().contains(o)))
    {
        let (verdict, evidence, left) = match cell.not_graded() {
            Some(why) => (axes::Verdict::NotGraded, why, None),
            None => catch_unwind(AssertUnwindSafe(|| grade_cell(src, cell, &mut clean)))
                .unwrap_or_else(|p| {
                    (
                        axes::Verdict::NotGraded,
                        format!("the cell could not run: {}", panic_text(&p)),
                        None,
                    )
                }),
        };
        let line = axes::line(&cell.key(), verdict, &evidence);
        println!("{line}");
        if verdict == axes::Verdict::Red {
            reds.push((cell.key(), evidence.clone()));
        }
        verdicts.push(verdict);
        let group = format!("{}/{}/{}", cell.engine, cell.shape.label(), cell.hook.name);
        match cell.stop {
            axes::Stop::Term => pairs.entry(group).or_default().0 = left,
            axes::Stop::Kill => pairs.entry(group).or_default().1 = left,
            axes::Stop::Fault => {
                if let Some(exit) = evidence
                    .split("fault exited ")
                    .nth(1)
                    .and_then(|r| r.split(';').next())
                {
                    exit_classes
                        .entry(format!("{}/{}", cell.engine, cell.hook.name))
                        .or_default()
                        .insert(cell.shape.label(), exit.to_string());
                }
            }
            axes::Stop::HostCrash | axes::Stop::SourceEvent => {}
        }
    }
    for (group, by_shape) in exit_classes.iter().filter(|(_, m)| m.len() > 1) {
        let (hook_engine, hook) = group.split_once('/').expect("engine/hook");
        let key = format!("{hook_engine}/*/{hook}/fault_exit_class");
        let classes: std::collections::BTreeSet<&String> = by_shape.values().collect();
        let line = if classes.len() == 1 {
            verdicts.push(axes::Verdict::Pass);
            axes::line(
                &key,
                axes::Verdict::Pass,
                &format!("one exit across shapes: {by_shape:?}"),
            )
        } else {
            let why = format!("the same fault exits differently by shape: {by_shape:?}");
            reds.push((key.clone(), why.clone()));
            verdicts.push(axes::Verdict::Red);
            axes::line(&key, axes::Verdict::Red, &why)
        };
        println!("{line}");
    }
    for (group, pair) in pairs {
        let (Some(term), Some(kill)) = (&pair.0, &pair.1) else {
            continue;
        };
        let key = format!("{group}/sigterm_vs_sigkill");
        let line = match axes::term_worse_than_kill(term, kill) {
            Some(why) => {
                let line = axes::line(&key, axes::Verdict::Red, &why);
                reds.push((key.clone(), why.clone()));
                verdicts.push(axes::Verdict::Red);
                line
            }
            None => {
                verdicts.push(axes::Verdict::Pass);
                axes::line(
                    &key,
                    axes::Verdict::Pass,
                    "SIGTERM left no more than SIGKILL",
                )
            }
        };
        println!("{line}");
    }
    for v in axes::variants(Path::new(env!("CARGO_MANIFEST_DIR")), &all)
        .iter()
        .filter(|v| only.as_deref().is_none_or(|o| v.key().contains(o)))
    {
        let (verdict, evidence) = match v.not_graded() {
            Some(why) => (axes::Verdict::NotGraded, why),
            None => catch_unwind(AssertUnwindSafe(|| grade_variant(src, v, &mut clean)))
                .unwrap_or_else(|p| {
                    (
                        axes::Verdict::NotGraded,
                        format!("the cell could not run: {}", panic_text(&p)),
                    )
                }),
        };
        let line = axes::line(&v.key(), verdict, &evidence);
        println!("{line}");
        if verdict == axes::Verdict::Red {
            reds.push((v.key(), evidence.clone()));
        }
        verdicts.push(verdict);
    }
    println!("{}", axes::summary(&verdicts));
    let unknown: Vec<String> = reds
        .iter()
        .filter(|(key, evidence)| axes::known_red(key, evidence).is_none())
        .map(|(key, evidence)| format!("{key} {evidence}"))
        .collect();
    let stale: Vec<&str> = if only.is_none() {
        axes::KNOWN_RED
            .iter()
            .filter(|k| k.keys.starts_with(&format!("{name}/")))
            .filter(|k| {
                !reds
                    .iter()
                    .any(|(key, evidence)| axes::glob(k.keys, key) && evidence.contains(k.finding))
            })
            .map(|k| k.keys)
            .collect()
    } else {
        vec![]
    };
    assert!(
        unknown.is_empty() && stale.is_empty(),
        "{} RED cell(s) no known-red entry covers:\n{}\nknown-red entries no RED met (remove them): {stale:?}",
        unknown.len(),
        unknown.join("\n")
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn no_debris_and_an_equal_rerun_after_every_stop_postgres() {
    matrix(Src::Sql(SqlEngine::Pg), "pg");
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn no_debris_and_an_equal_rerun_after_every_stop_mysql() {
    matrix(Src::Sql(SqlEngine::Mysql), "mysql");
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn no_debris_and_an_equal_rerun_after_every_stop_mssql() {
    matrix(Src::Sql(SqlEngine::Mssql), "mssql");
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn no_debris_and_an_equal_rerun_after_every_stop_oracle() {
    matrix(Src::Sql(SqlEngine::Oracle), "oracle");
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn no_debris_and_an_equal_rerun_after_every_stop_mongo() {
    matrix(Src::Mongo, "mongo");
}
