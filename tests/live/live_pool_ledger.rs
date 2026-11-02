//! The state LEDGER under a pooled `rivet load` — slow, cut, and single-writer.
//!
//! Toxiproxy is the instrument for the first two tests only; the SQLite one
//! needs no proxy at all, which is why this file is no longer named after it.
//!
//! The pool's only new shared resource is the state ledger. A load never touches
//! the source — it reads Parquet from the bucket and issues `LOAD DATA` to the
//! warehouse — and every worker opens its OWN connection to the ledger
//! (`open_at_ref`, one per worker). So the question a pooled load actually
//! raises is "what happens when the STATE DB throttles under N workers?", and
//! this file puts the ledger, not the source, behind toxiproxy. The existing
//! `live_pool_toxiproxy.rs` proxies the source for `apply --pool`, which is the
//! other subsystem entirely.
//!
//! The toxic is added AFTER the extract, so the fault is isolated to the load
//! while both legs still share one ledger database (the proxy's upstream).
//!
//! Oracle: BigQuery's own per-table counts, never rivet's summary — a pool that
//! dropped a table would still exit 0 and print a tidy run.
//!
//! Scope honesty: this does NOT exercise `reconnect_failure_is_fatal`. Workers
//! connect once at pool start, so the window between the parent's open and the
//! workers' is milliseconds wide and cannot be hit by flipping a proxy; that
//! predicate is graded offline (`only_a_worker_that_lost_a_ledger_the_run_had_is_fatal`).
//! What this grades is the throttled-but-alive ledger, which is the shape an
//! overloaded state DB actually takes.
//!
//! Needs postgres + BigQuery creds throughout, plus postgres-state + toxiproxy
//! for the two proxied tests; each SKIPS without what it needs.

use crate::common::*;

/// Six tables at pool 6: enough that every worker slot is occupied and the
/// ledger sees six concurrent connections rather than a sequence of one.
const TABLES: usize = 6;
const ROWS: i64 = 200;
/// Per-response delay on the ledger link. Large enough that a ledger round-trip
/// is unmistakably the slow part, small enough that the run stays in seconds.
const LEDGER_LATENCY_MS: u64 = 150;
/// The pool ceiling. The SQLite measurement needs this many TABLES as well as
/// this many workers: `effective_pool` clamps the pool to the work available, so
/// six tables could never engage sixteen slots.
const SQLITE_TABLES: usize = 16;

/// The SOURCE's own row count, re-queried from Postgres.
///
/// The independent side of every completeness claim in this file. `ROWS` is this
/// file's own constant, so comparing the warehouse to it grades the fixture
/// against itself; the database that was actually read is the honest oracle, and
/// it is a different implementation from both rivet and BigQuery.
fn source_rows(table: &str) -> i64 {
    let mut c = pg_connect();
    c.query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
        .expect("the source's own row count")
        .get(0)
}

#[test]
#[ignore = "live: requires postgres + postgres-state + toxiproxy + BigQuery creds"]
fn a_pooled_load_through_a_throttled_ledger_loses_no_table() {
    let Some(bq) = BqLive::from_env("pool_ledger") else {
        return;
    };
    let _lock = toxiproxy_guard();
    ensure_toxi_proxy("postgres_state", 15433, "postgres-state:5432");
    toxi_reset_toxics("postgres_state");
    // A neighbour that panics between its toxi_disable and toxi_enable leaves the proxy off.
    toxi_enable("postgres_state");
    require_alive(LiveService::PostgresStateToxi);

    let pg = SqlEngine::Pg;
    pg.alive();
    let (t, _guard) = pg.create("pool_led", "id BIGINT PRIMARY KEY, v TEXT");
    pg.exec(&format!(
        "INSERT INTO {t} (id, v) SELECT g, md5(g::text) FROM generate_series(1, {ROWS}) g"
    ));

    // The primary export is the table itself; the secondaries are query exports
    // named after it, so every warehouse table name is unique to this run.
    // `render.rs` gives each export its own `<prefix>/<export>/` destination, so
    // the six never share a folder — a shared prefix is a different bug class.
    let secondaries: Vec<String> = (1..TABLES).map(|i| format!("{t}_s{i}")).collect();
    let mut rig = pg
        .rig(&t)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(""));
    for name in &secondaries {
        rig = rig.also_export(name, &format!("SELECT id, v FROM {t}"));
    }

    let mut tables: Vec<&str> = vec![&t];
    tables.extend(secondaries.iter().map(|s| s.as_str()));
    let _cleanup = bq.cleanup(&tables);

    // Extract at full speed: the toxic belongs to the load leg only.
    let toxi_url = postgres_state_toxi_url();
    let env = [("RIVET_STATE_URL", toxi_url.as_str())];
    let out = rig.run_args_env(&[], &env);
    assert!(
        out.status.success(),
        "the extract must succeed before the load is measured:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Now throttle the ledger and load every table at once.
    let _lat = toxi_add_latency("postgres_state", LEDGER_LATENCY_MS);
    rig.load_ok(&["--pool", &TABLES.to_string()], &env);

    // BigQuery is the oracle: a worker that lost its table would not say so.
    let expected = source_rows(&t);
    for table in &tables {
        let n: i64 = bq.read_bq_count(table).parse().expect("a count");
        assert_eq!(
            n, expected,
            "`{table}` must hold every row the SOURCE still has, after a pooled \
             load over a throttled ledger"
        );
    }
}

/// Which of `names` currently exist as tables — a probe that does NOT panic on
/// an absent table, unlike a per-table `COUNT(*)`.
fn existing_tables(bq: &BqLive, names: &[&str]) -> Vec<String> {
    let quoted = names
        .iter()
        .map(|n| format!("'{n}'"))
        .collect::<Vec<_>>()
        .join(", ");
    bq.read_bq_rows(&format!(
        "SELECT table_name FROM `{}.{}.INFORMATION_SCHEMA.TABLES` WHERE table_name IN ({quoted})",
        bq.project, bq.dataset
    ))
    .iter()
    .map(|r| r["table_name"].as_str().expect("name").to_string())
    .collect()
}

/// A pooled load whose ledger is cut mid-run, after every table's first successful load, fails loudly; the next run leaves every table holding the source's current rows.
#[test]
#[ignore = "live: requires postgres + postgres-state + toxiproxy + BigQuery creds"]
fn a_ledger_cut_mid_load_fails_loudly_and_the_next_run_finishes_the_job() {
    let Some(bq) = BqLive::from_env("pool_topup") else {
        return;
    };
    let _lock = toxiproxy_guard();
    ensure_toxi_proxy("postgres_state", 15433, "postgres-state:5432");
    toxi_reset_toxics("postgres_state");
    toxi_enable("postgres_state");
    require_alive(LiveService::PostgresStateToxi);

    let pg = SqlEngine::Pg;
    pg.alive();
    let (t, _guard) = pg.create("pool_top", "id BIGINT PRIMARY KEY, v TEXT");
    pg.exec(&format!(
        "INSERT INTO {t} (id, v) SELECT g, md5(g::text) FROM generate_series(1, {ROWS}) g"
    ));

    let secondaries: Vec<String> = (1..TABLES).map(|i| format!("{t}_s{i}")).collect();
    let mut rig = pg
        .rig(&t)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(""));
    for name in &secondaries {
        rig = rig.also_export(name, &format!("SELECT id, v FROM {t}"));
    }
    let mut tables: Vec<&str> = vec![&t];
    tables.extend(secondaries.iter().map(|s| s.as_str()));
    let _cleanup = bq.cleanup(&tables);

    let toxi_url = postgres_state_toxi_url();
    let env = [("RIVET_STATE_URL", toxi_url.as_str())];
    let out = rig.run_args_env(&[], &env);
    assert!(
        out.status.success(),
        "the extract must succeed before the crash is staged:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    rig.load_ok(&["--pool", &TABLES.to_string()], &env);
    pg.exec(&format!(
        "INSERT INTO {t} (id, v) SELECT g, md5(g::text) FROM generate_series({}, {}) g",
        ROWS + 1,
        ROWS + 10
    ));
    let out = rig.run_args_env(&[], &env);
    assert!(
        out.status.success(),
        "the second extract must succeed before the crash is staged:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let pool = TABLES.to_string();
    let watch = format!("%{t}%");
    let direct = postgres_state_toxi_url().replace(":15433/", ":5433/");
    let killer = std::thread::spawn(move || {
        let mut c =
            postgres::Client::connect(&direct, postgres::NoTls).expect("the ledger, directly");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while std::time::Instant::now() < deadline {
            let n: i64 = c
                .query_one(
                    "SELECT count(*) FROM load_run WHERE status = 'writing' AND target_table LIKE $1",
                    &[&watch],
                )
                .map(|r| r.get(0))
                .unwrap_or(0);
            if n > 0 {
                toxi_disable("postgres_state");
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        false
    });
    let crashed = rig.load_args_env(&["--pool", &pool], &env);
    assert!(
        killer.join().expect("the killer thread"),
        "fixture inert: no `writing` marker appeared, so the ledger was never cut mid-load"
    );
    assert!(
        !crashed.status.success(),
        "a load whose ledger died mid-run must FAIL, not report a tidy success:\n{}",
        String::from_utf8_lossy(&crashed.stdout)
    );
    eprintln!(
        "--- crashed load said ---\n{}{}",
        String::from_utf8_lossy(&crashed.stdout),
        String::from_utf8_lossy(&crashed.stderr)
    );

    toxi_enable("postgres_state");
    toxi_reset_toxics("postgres_state");
    let after_crash = existing_tables(&bq, &tables);
    eprintln!(
        "--- after the crash: {} of {} tables exist: {after_crash:?}",
        after_crash.len(),
        tables.len()
    );
    eprintln!("--- BigQuery job history for the same tables ---");
    for r in bq.read_bq_rows(&format!(
        "SELECT l.value AS tbl, state, IFNULL(error_result.reason, '-') AS err \
         FROM `{}.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, UNNEST(labels) l \
         WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE) \
           AND statement_type = 'LOAD_DATA' AND l.key = 'rivet_table' \
           AND l.value LIKE '{}%' ORDER BY tbl",
        bq.project, t
    )) {
        eprintln!(
            "    {} state={} err={}",
            r["tbl"].as_str().unwrap_or("?"),
            r["state"].as_str().unwrap_or("?"),
            r["err"].as_str().unwrap_or("?")
        );
    }

    let topup = rig.load_args_env(&[], &env);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&topup.stdout),
        String::from_utf8_lossy(&topup.stderr)
    );
    eprintln!("--- top-up load said ---\n{said}");
    assert!(
        topup.status.success(),
        "the top-up load must finish what the crashed one left:\n{said}"
    );

    for table in &tables {
        let n: i64 = bq.read_bq_count(table).parse().expect("a count");
        assert_eq!(
            n,
            source_rows(&t),
            "`{table}` must hold exactly what the SOURCE has after crash + top-up \
             — a re-consumed run must OVERWRITE under `full`, never double"
        );
    }
}

/// Sixteen workers on a SQLITE ledger — the backend's single writer, measured.
///
/// SQLite in WAL gives many readers and exactly ONE writer, which is what
/// `--pool`'s ceiling warning tells operators about. Whether that is a real
/// ceiling for THIS workload is a measurement, not a deduction: a load reads
/// each export's spec at the front and writes one record at the end, and the
/// per-table lease on SQLite is a `flock` sidecar — not a SQLite write at all.
/// The answer decides how high a DEFAULT pool may go on the default backend.
///
/// No `RIVET_STATE_URL`, so the ledger is the SQLite file beside the rig's
/// config: exactly what an operator who configured nothing gets. Row counts are
/// asserted; the contention evidence is REPORTED rather than asserted, because
/// the point is to find out whether any appears.
///
/// Read the contention count HONESTLY. rivet sets `PRAGMA busy_timeout = 10000`
/// (`SQLITE_BUSY_TIMEOUT_MS`), so a writer that has to wait simply waits and then
/// succeeds — queuing is absorbed SILENTLY. The filter can fire (rusqlite's text
/// is "database is locked", which it matches), so zero lines does mean no lock
/// error ESCAPED; it does not mean no worker ever queued. Measured 2026-09-21:
/// 16 workers, 16 tables, 0 lines, every table exact — enough to default the
/// pool on this backend, not enough to claim there was no waiting.
#[test]
#[ignore = "live: requires postgres + BigQuery creds (no proxy, no postgres-state)"]
fn sixteen_workers_on_a_sqlite_ledger_do_not_lose_a_table() {
    let Some(bq) = BqLive::from_env("pool_sqlite") else {
        return;
    };
    let pg = SqlEngine::Pg;
    pg.alive();
    let (t, _guard) = pg.create("pool_sqlt", "id BIGINT PRIMARY KEY, v TEXT");
    pg.exec(&format!(
        "INSERT INTO {t} (id, v) SELECT g, md5(g::text) FROM generate_series(1, {ROWS}) g"
    ));

    let secondaries: Vec<String> = (1..SQLITE_TABLES).map(|i| format!("{t}_s{i}")).collect();
    let mut rig = pg
        .rig(&t)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(""));
    for name in &secondaries {
        rig = rig.also_export(name, &format!("SELECT id, v FROM {t}"));
    }
    let mut tables: Vec<&str> = vec![&t];
    tables.extend(secondaries.iter().map(|s| s.as_str()));
    let _cleanup = bq.cleanup(&tables);

    let out = rig.run_args(&[]);
    assert!(
        out.status.success(),
        "the extract must succeed before the pool is measured:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let load = rig.load_args(&["--pool", &SQLITE_TABLES.to_string()]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&load.stdout),
        String::from_utf8_lossy(&load.stderr)
    );
    let contended: Vec<&str> = said
        .lines()
        .filter(|l| l.contains("SQLITE_BUSY") || l.contains("database is locked"))
        .collect();
    eprintln!(
        "--- SQLite ledger at pool {SQLITE_TABLES} over {} tables: {} contention line(s) ---",
        tables.len(),
        contended.len()
    );
    for l in &contended {
        eprintln!("    {l}");
    }
    assert!(
        load.status.success(),
        "a pooled load on the DEFAULT (SQLite) ledger must succeed:\n{said}"
    );
    for table in &tables {
        let n: i64 = bq.read_bq_count(table).parse().expect("a count");
        assert_eq!(
            n,
            source_rows(&t),
            "`{table}` must hold every row the SOURCE still has"
        );
    }
}
