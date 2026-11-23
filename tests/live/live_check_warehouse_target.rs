//! `rivet check` asks the BigQuery warehouse a `load:` block names whether its dataset exists.
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
