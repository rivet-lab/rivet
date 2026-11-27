//! `rivet check` asks the warehouse a `load:` block names (BigQuery, ClickHouse, Snowflake) whether the place it loads into exists.
//!
//!     docker compose up -d postgres fake-gcs
//!     cargo test --test live_suite check_warehouse_target -- --ignored

use crate::common::*;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

const GCS_BUCKET: &str = "rivet-qa-check-warehouse-target";
const LOAD: &str = "load: { target: bigquery, project: p, dataset: d, pk: [id] }";

struct Warehouse {
    port: u16,
    paths: Arc<Mutex<Vec<String>>>,
}

impl Warehouse {
    /// A loopback BigQuery API that answers every `datasets.get` with `status`.
    fn start(status: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the warehouse");
        let port = listener
            .local_addr()
            .expect("the warehouse's address")
            .port();
        let paths = Arc::new(Mutex::new(Vec::new()));
        let log = paths.clone();
        std::thread::spawn(move || {
            for mut conn in listener.incoming().flatten() {
                let mut reader = BufReader::new(conn.try_clone().expect("clone the connection"));
                let mut line = String::new();
                let _ = reader.read_line(&mut line);
                log.lock()
                    .unwrap()
                    .push(line.split_whitespace().nth(1).unwrap_or("").to_string());
                let body = r#"{"location":"US","error":{"message":"stand"}}"#;
                let _ = write!(
                    conn,
                    "HTTP/1.1 {status} stand\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        Warehouse { port, paths }
    }

    fn asked(&self) -> Vec<String> {
        self.paths.lock().unwrap().clone()
    }

    /// `rivet check` of a PostgreSQL export to fake-gcs, then the run it vouches for with the BigQuery API at this stand.
    fn check(&self, load: Option<&str>) -> std::process::Output {
        require_alive(LiveService::Postgres);
        ensure_gcs_bucket(GCS_BUCKET);
        let table = seed_pg_numeric_table(5);
        let mut rig = Rig::pg_batch(table.name()).dest_gcs(
            GCS_BUCKET,
            &unique_name("check_wt"),
            FAKE_GCS_ENDPOINT,
        );
        if let Some(line) = load {
            rig = rig.top_line(line);
        }
        let base = format!("http://127.0.0.1:{}", self.port);
        let envs = [
            ("RIVET_BQ_API_ENDPOINT", base.as_str()),
            ("RIVET_BQ_ACCESS_TOKEN", "tok"),
        ];
        rig.run_after_env(&["check"], &[], &envs, Pairing::GreenOnly)
            .preflight
    }
}

fn stderr_of(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// A dataset the project does not hold stops `check` with the code `load` gives.
#[test]
#[ignore = "live: requires postgres + fake-gcs"]
fn check_refuses_a_bigquery_dataset_the_project_does_not_hold() {
    let wh = Warehouse::start(404);
    let out = wh.check(Some(LOAD));
    let err = stderr_of(&out);
    assert_eq!(out.status.code(), Some(1), "{err}");
    assert!(
        err.contains("RIVET_LOAD_WAREHOUSE_REJECTED") && err.contains("`p:d` does not exist"),
        "{err}"
    );
    assert_eq!(wh.asked(), vec!["/bigquery/v2/projects/p/datasets/d"]);
}

/// A dataset that exists passes `check` after one `datasets.get`.
#[test]
#[ignore = "live: requires postgres + fake-gcs"]
fn check_passes_a_bigquery_dataset_the_project_holds() {
    let wh = Warehouse::start(200);
    let out = wh.check(Some(LOAD));
    assert!(out.status.success(), "{}", stderr_of(&out));
    assert_eq!(wh.asked(), vec!["/bigquery/v2/projects/p/datasets/d"]);
}

/// An answer that cannot say (403) is a warning: `check` exits 0, as it did before the probe.
#[test]
#[ignore = "live: requires postgres + fake-gcs"]
fn check_warns_when_the_warehouse_will_not_say_whether_the_dataset_exists() {
    let wh = Warehouse::start(403);
    let out = wh.check(Some(LOAD));
    let err = stderr_of(&out);
    assert!(out.status.success(), "{err}");
    assert!(
        err.contains("could not confirm BigQuery dataset `p:d`"),
        "{err}"
    );
}

/// A config with no `load:` block never contacts the warehouse.
#[test]
#[ignore = "live: requires postgres + fake-gcs"]
fn check_without_a_load_block_never_asks_the_warehouse() {
    let wh = Warehouse::start(404);
    let out = wh.check(None);
    assert!(out.status.success(), "{}", stderr_of(&out));
    assert_eq!(wh.asked(), Vec::<String>::new());
}

const CH_PASSWORD_ENV: &str = "RIVET_TEST_CH_PASSWORD";

fn clickhouse_load(database: &str) -> String {
    format!(
        "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {database}, \
         user: {CLICKHOUSE_USER}, password_env: {CH_PASSWORD_ENV}, pk: [id] }}"
    )
}

/// One ClickHouse statement on the stand; a panic naming the SQL when it fails.
fn ch(sql: &str) {
    let resp = reqwest::blocking::Client::new()
        .post(CLICKHOUSE_HTTP_URL)
        .basic_auth(CLICKHOUSE_USER, Some(CLICKHOUSE_PASSWORD))
        .body(sql.to_string())
        .send()
        .expect("post to clickhouse");
    let status = resp.status();
    assert!(status.is_success(), "clickhouse {status}: {sql}");
}

/// A scratch ClickHouse database that is dropped when the cell ends.
struct ChDb(String);

impl ChDb {
    fn created(prefix: &str) -> Self {
        let name = unique_name(prefix);
        ch(&format!("CREATE DATABASE {name}"));
        ChDb(name)
    }
}

impl Drop for ChDb {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(|| ch(&format!("DROP DATABASE IF EXISTS {}", self.0)));
    }
}

/// A PostgreSQL export to fake-gcs loading as `load` says, and the table it reads, dropped with the guard.
fn staged_rig(load: &str) -> (Rig, PgTable) {
    require_alive(LiveService::Postgres);
    ensure_gcs_bucket(GCS_BUCKET);
    let table = seed_pg_numeric_table(5);
    let rig = Rig::pg_batch(table.name())
        .dest_gcs(GCS_BUCKET, &unique_name("check_wt"), FAKE_GCS_ENDPOINT)
        .top_line(load);
    (rig, table)
}

/// The code in the `Error: [RIVET_...]` line a refusal printed.
fn code_of(stderr: &str) -> String {
    let at = stderr.find("Error: [").expect("a coded refusal") + "Error: [".len();
    stderr[at..]
        .chars()
        .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
        .collect()
}

/// A ClickHouse database the server does not show stops `check` with the code a load statement answered `UNKNOWN_DATABASE` gets.
#[test]
#[ignore = "live: requires postgres + fake-gcs + clickhouse"]
fn check_refuses_a_clickhouse_database_the_server_does_not_hold() {
    require_alive(LiveService::ClickHouse);
    let missing = unique_name("rivet_chcheck_missing");
    let (rig, _table) = staged_rig(&clickhouse_load(&missing));
    let envs = [(CH_PASSWORD_ENV, CLICKHOUSE_PASSWORD)];
    let check = rig.cli_env(&["check"], &envs);
    let check_err = stderr_of(&check);
    assert_eq!(check.status.code(), Some(1), "{check_err}");
    assert!(
        check_err.contains(&format!(
            "ClickHouse database `{missing}` is not visible to user `{CLICKHOUSE_USER}`"
        )),
        "{check_err}"
    );
    assert_eq!(
        code_of(&check_err),
        "RIVET_LOAD_WAREHOUSE_OBJECT_NOT_FOUND",
        "{check_err}"
    );
    rig.run_ok();
}

/// A ClickHouse database the server shows passes `check`.
#[test]
#[ignore = "live: requires postgres + fake-gcs + clickhouse"]
fn check_passes_a_clickhouse_database_the_server_holds() {
    require_alive(LiveService::ClickHouse);
    let db = ChDb::created("rivet_chcheck_held");
    let (rig, _table) = staged_rig(&clickhouse_load(&db.0));
    let out = rig.cli_env(&["check"], &[(CH_PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
    let err = stderr_of(&out);
    assert_eq!(out.status.code(), Some(0), "{err}");
    assert!(!err.contains("could not confirm"), "{err}");
    rig.run_ok();
}

/// A ClickHouse that cannot be reached is a warning, as it was before the probe.
#[test]
#[ignore = "live: requires postgres + fake-gcs"]
fn check_warns_when_clickhouse_cannot_be_reached() {
    let load = format!(
        "load: {{ target: clickhouse, url: \"http://127.0.0.1:1\", database: d, \
         user: u, password_env: {CH_PASSWORD_ENV}, pk: [id] }}"
    );
    let (rig, _table) = staged_rig(&load);
    let out = rig.cli_env(&["check"], &[(CH_PASSWORD_ENV, "x")]);
    let err = stderr_of(&out);
    assert!(out.status.success(), "{err}");
    assert!(
        err.contains("could not confirm ClickHouse database `d` exists"),
        "{err}"
    );
    rig.run_ok();
}

/// A Snowflake database or schema the login cannot see stops `check` with the code a load statement error gets; the stand's own passes.
#[test]
#[ignore = "live: requires postgres + the Snowflake stand"]
fn check_refuses_a_snowflake_place_the_login_cannot_see_and_passes_the_stand() {
    let Some(sf) = SfLive::from_env("sf_check_place") else {
        return;
    };
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5);
    let rig_for = |line: &str| {
        Rig::pg_batch(table.name())
            .dest_gcs_live(&sf.bucket, &sf.prefix)
            .top_line(line)
    };
    let (database, schema) = sf.place();
    let held = rig_for(&sf.load_line(""));
    let ok = held.cli(&["check"]);
    assert!(ok.status.success(), "{}", stderr_of(&ok));
    held.run_ok();
    let no_database = unique_name("RIVET_NO_DB").to_uppercase();
    let no_schema = unique_name("RIVET_NO_SCHEMA").to_uppercase();
    for (db, sc, named) in [
        (
            no_database.as_str(),
            schema,
            format!("database `{no_database}`"),
        ),
        (
            database,
            no_schema.as_str(),
            format!("schema `{database}.{no_schema}`"),
        ),
    ] {
        let rig = rig_for(&sf.load_line_in(db, sc, ""));
        let check = rig.cli(&["check"]);
        let check_err = stderr_of(&check);
        assert_eq!(check.status.code(), Some(1), "{check_err}");
        assert!(
            check_err.contains(&format!("Snowflake {named} is not visible to this login")),
            "{check_err}"
        );
        assert_eq!(
            code_of(&check_err),
            "RIVET_LOAD_WAREHOUSE_REJECTED",
            "{check_err}"
        );
        rig.run_ok();
    }
}
