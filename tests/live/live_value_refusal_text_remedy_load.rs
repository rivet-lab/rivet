//! The remedy of a value refusal, loaded: a PostgreSQL date, time, timestamp, timestamptz or array column declared `text` lands in each warehouse as the server's own text, read back through that warehouse's own client.

use crate::common::*;
use crate::live_int_to_float_override::readback_only;

const BUCKET: &str = "rivet-qa-clickhouse-load";
const PASSWORD_ENV: &str = "RIVET_TEST_CH_PASSWORD";
const COLUMNS: &[(&str, &str)] = &[
    ("d", "DATE"),
    ("t", "TIME"),
    ("ts", "TIMESTAMP"),
    ("tz", "TIMESTAMPTZ"),
    ("ia", "INTEGER[]"),
    ("ta", "TEXT[]"),
    ("da", "DATE[]"),
    ("tsa", "TIMESTAMP[]"),
];
const ROWS: &[&str] = &[
    "(1, 'infinity', '24:00:00', 'infinity', 'infinity', '{{1,2},{3,4}}', '{{a,b},{c,d}}', '{2024-01-01,infinity}', '{2024-01-01 00:00:00,-infinity}')",
    "(2, '-infinity', '00:00:00', '-infinity', '-infinity', '{1,2,3}', '{a,b}', '{-infinity}', '{2024-01-01 00:00:00}')",
    "(3, '5874897-12-31', '14:30:00.123456', '2024-03-15 10:11:12.123456', '2024-03-15 10:11:12.123456+00', '{{{1},{2}},{{3},{4}}}', '{x}', '{2024-03-15}', '{2024-03-15 10:11:12.123456}')",
    "(4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
];

pub(crate) type Grid = Vec<Vec<Option<String>>>;

fn declared() -> String {
    let cols: Vec<String> = COLUMNS.iter().map(|(c, _)| format!("{c}: text")).collect();
    format!("columns: {{{}}}", cols.join(", "))
}

fn select_list() -> String {
    COLUMNS
        .iter()
        .map(|(c, _)| *c)
        .collect::<Vec<_>>()
        .join(", ")
}

fn held_table() -> (String, PgTable) {
    let table = unique_name("vrl_x");
    let defs: Vec<String> = COLUMNS.iter().map(|(c, t)| format!("{c} {t}")).collect();
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, {})",
        defs.join(", ")
    ))
    .unwrap();
    let guard = PgTable::adopt(table.clone());
    c.batch_execute(&format!(
        "INSERT INTO {table} (id, {}) VALUES {}",
        select_list(),
        ROWS.join(", ")
    ))
    .unwrap();
    (table, guard)
}

/// The server's own text of every declared column, by id, read through a second client.
fn server_rows(table: &str) -> Grid {
    let casts: Vec<String> = COLUMNS.iter().map(|(c, _)| format!("{c}::text")).collect();
    let mut oracle = pg_connect();
    oracle.batch_execute("SET TimeZone = 'UTC'").unwrap();
    oracle
        .query(
            &format!("SELECT {} FROM {table} ORDER BY id", casts.join(", ")),
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| {
            (0..COLUMNS.len())
                .map(|i| r.get::<_, Option<String>>(i))
                .collect()
        })
        .collect()
}

pub(crate) fn json_text(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

fn check(receiver: &str, got: Grid, want: Grid) {
    assert_eq!(
        got, want,
        "{receiver}: the warehouse returned values other than the server's text"
    );
}

pub(crate) fn clickhouse_rows_json(sql: &str) -> Grid {
    let body = {
        let resp = reqwest::blocking::Client::new()
            .post(CLICKHOUSE_HTTP_URL)
            .basic_auth(CLICKHOUSE_USER, Some(CLICKHOUSE_PASSWORD))
            .body(sql.to_string())
            .send()
            .expect("post to clickhouse");
        let status = resp.status();
        let text = resp.text().unwrap_or_default();
        assert!(status.is_success(), "clickhouse HTTP {status}: {text}");
        text
    };
    let doc: serde_json::Value = serde_json::from_str(&body).expect("clickhouse JSONCompact");
    doc["data"]
        .as_array()
        .expect("data")
        .iter()
        .map(|row| row.as_array().expect("row").iter().map(json_text).collect())
        .collect()
}

pub(crate) struct ChDb(pub(crate) String);

impl ChDb {
    pub(crate) fn new() -> Self {
        let name = unique_name("vrl_chdb");
        let resp = reqwest::blocking::Client::new()
            .post(CLICKHOUSE_HTTP_URL)
            .basic_auth(CLICKHOUSE_USER, Some(CLICKHOUSE_PASSWORD))
            .body(format!("CREATE DATABASE {name}"))
            .send()
            .expect("post to clickhouse");
        assert!(resp.status().is_success(), "create database {name}");
        ChDb(name)
    }
}

impl Drop for ChDb {
    fn drop(&mut self) {
        let _ = reqwest::blocking::Client::new()
            .post(CLICKHOUSE_HTTP_URL)
            .basic_auth(CLICKHOUSE_USER, Some(CLICKHOUSE_PASSWORD))
            .body(format!("DROP DATABASE IF EXISTS {}", self.0))
            .send();
    }
}

#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn a_text_declared_column_loads_into_clickhouse_as_the_server_text() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (table, _guard) = held_table();
    let db = ChDb::new();
    let rig = readback_only(Rig::pg_batch(&table))
        .export_line(&declared())
        .dest_gcs(BUCKET, &unique_name("vrlch"), FAKE_GCS_ENDPOINT)
        .top_line(&format!(
            "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {}, \
             user: {CLICKHOUSE_USER}, password_env: {PASSWORD_ENV}, pk: [id] }}",
            db.0
        ));
    rig.run_ok();
    rig.load_ok(&[], &[(PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
    let got = clickhouse_rows_json(&format!(
        "SELECT {} FROM {}.{table} ORDER BY id FORMAT JSONCompact",
        select_list(),
        db.0
    ));
    check("clickhouse", got, server_rows(&table));
}

#[test]
#[ignore = "live: requires BigQuery creds + postgres"]
fn a_text_declared_column_loads_into_bigquery_as_the_server_text() {
    let Some(bq) = BqLive::from_env("vrl_bq") else {
        return;
    };
    require_alive(LiveService::Postgres);
    let (table, _guard) = held_table();
    let _cleanup = bq.cleanup(&[&table]);
    let rig = readback_only(Rig::pg_batch(&table))
        .export_line(&declared())
        .dest_gcs_live(&bq.bucket, &format!("{}/{table}", bq.prefix))
        .top_line(&bq.load_line(""));
    rig.run_ok();
    rig.load_ok(&[], &[]);
    let rows = bq.read_bq_rows(&format!(
        "SELECT {} FROM `{}.{}.{table}` ORDER BY id",
        select_list(),
        bq.project,
        bq.dataset
    ));
    let got: Grid = rows
        .iter()
        .map(|r| COLUMNS.iter().map(|(c, _)| json_text(&r[*c])).collect())
        .collect();
    check("bigquery", got, server_rows(&table));
}

#[test]
#[ignore = "live: requires Snowflake creds + postgres"]
fn a_text_declared_column_loads_into_snowflake_as_the_server_text() {
    let Some(sf) = SfLive::from_env("vrl_sf") else {
        return;
    };
    require_alive(LiveService::Postgres);
    let (table, _guard) = held_table();
    let _cleanup = sf.cleanup(&[&table]);
    let rig = readback_only(Rig::pg_batch(&table))
        .export_line(&declared())
        .dest_gcs_live(&sf.bucket, &sf.prefix)
        .top_line(&sf.load_line(""));
    rig.run_ok();
    rig.load_ok(&[], &[]);
    let rows = sf.read(&format!(
        "SELECT {} FROM {} ORDER BY id",
        select_list(),
        sf.fq(&table)
    ));
    let got: Grid = rows
        .iter()
        .map(|r| r.iter().map(json_text).collect())
        .collect();
    check("snowflake", got, server_rows(&table));
}
