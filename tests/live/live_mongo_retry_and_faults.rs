//! Runtime retry + mid-scan fault injection for the MongoDB batch read path, on
//! the canonical [`Rig`] — the Mongo twin of `live_retry_and_faults.rs` (PG) and
//! `live_mysql_retry_and_faults.rs`. Requires `mongo` + Toxiproxy
//! (`docker compose up -d mongo toxiproxy`).
//!
//! These prove the Mongo batch read path RECOVERS from a mid-scan transport
//! outage via rivet's chunk retry loop — the operational parity the SQL engines
//! have. A dropped socket surfaces as `mongodb::error::ErrorKind::Io`; rivet
//! classifies it TRANSIENT and retries on a fresh connection rather than aborting.
//!
//! Scope note: an `Io` drop is *also* matched by the retry string fallback
//! ("connection reset" / "unexpected eof"), so these tests do not, on their own,
//! isolate the typed `classify_mongo_error` arm — its non-redundant value is the
//! retryable-read COMMAND CODES (replica-set failover: stepdown / election),
//! which Toxiproxy (network faults, not server command errors) cannot induce.
//!
//! Reads cross Toxiproxy (`:27019` → `mongo:27017`); the collection is seeded
//! over the DIRECT `:27017` driver so the fault only ever hits rivet's reads.

use crate::common::*;
use std::time::Duration;

const DIRECT_PORT: u16 = 27017; // seed here; rivet reads through :27019

/// Register the `mongo` toxi proxy (listen :27019 → `mongo:27017`) and wipe any
/// leftover toxics — idempotent, mirrors `reset_mysql_proxy`.
fn reset_mongo_proxy() {
    ensure_toxi_proxy("mongo", 27019, "mongo:27017");
    toxi_reset_toxics("mongo");
    // A prior `..._fails_cleanly..` test disables the proxy and does not restore
    // it; re-enable so each test starts from a healthy link regardless of order.
    toxi_enable("mongo");
}

/// A batch Rig whose reads cross the toxi proxy, with a bounded retry budget.
fn proxied(db: &str) -> Rig {
    Rig::mongo_batch("t")
        .source_url(&mongo_toxi_url(db))
        .mongo("page_size: 2000")
        .export_line("tuning: { max_retries: 4, retry_backoff_ms: 200 }")
}

#[test]
#[ignore = "live: requires docker compose up -d mongo toxiproxy"]
fn mongo_export_survives_transient_latency_added_via_toxiproxy() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Toxiproxy);
    reset_mongo_proxy();
    let db = unique_name("rt_lat");
    MongoTest::connect(DIRECT_PORT, &db).seed_int_id("t", 8000);

    // A per-read delay slows the scan but must not fail it.
    toxi_add_latency("mongo", 25);

    let rig = proxied(&db);
    let t0 = std::time::Instant::now();
    rig.run_ok();
    let took = t0.elapsed();
    assert!(
        took >= Duration::from_millis(3 * 25),
        "the 25 ms toxic must have slowed at least three sequential round trips; took {took:?}"
    );
    let ran =
        recorded_run_outcome(&rig.config_path(), rig.export_name()).expect("the run's metrics row");
    assert_eq!(ran.retries, 0, "latency must not cost a retry: {ran:?}");
    assert_eq!(
        duckdb_dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        8000,
        "latency must not lose rows"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo toxiproxy"]
fn mongo_export_recovers_after_mid_stream_connection_cut_with_retries() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Toxiproxy);
    reset_mongo_proxy();
    let db = unique_name("rt_mid");
    MongoTest::connect(DIRECT_PORT, &db).seed_int_id("t", 100_000);

    let rig = Rig::mongo_batch("t")
        .source_url(&mongo_toxi_url(&db))
        .mongo("page_size: 2000")
        .export_line("tuning: { max_retries: 4, retry_backoff_ms: 1500 }");
    let cut = toxi_add_limit_data("mongo", 30_000, "downstream");
    let mut run = rig.spawn_args_env(&[], &[]);
    let t0 = std::time::Instant::now();
    while !run.stderr_so_far().contains("retry 1/")
        && !matches!(run.try_wait(), Ok(Some(_)))
        && t0.elapsed() < Duration::from_secs(90)
    {
        std::thread::sleep(Duration::from_millis(25));
    }
    toxi_remove_toxic("mongo", &cut);
    let out = run.wait_with_output().expect("reap the run");

    assert!(
        out.status.success(),
        "a cut before any part is durable must be retried on a fresh connection; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let ran =
        recorded_run_outcome(&rig.config_path(), rig.export_name()).expect("the run's metrics row");
    assert!(
        ran.status == "success" && ran.retries >= 1,
        "the run must have retried the cut; recorded: {ran:?}"
    );
    let rows: usize = read_all_parts(&rig.out_dir())
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        rows, 100_000,
        "a retried cut must deliver every document exactly once"
    );
    assert_eq!(
        duckdb_dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        100_000,
        "no document may be lost or doubled across the retry"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo toxiproxy"]
fn mongo_export_fails_cleanly_when_proxy_is_disabled_before_run() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Toxiproxy);
    reset_mongo_proxy();
    let db = unique_name("rt_dead");
    MongoTest::connect(DIRECT_PORT, &db).seed_int_id("t", 100);

    toxi_disable("mongo"); // upstream unreachable for the whole run

    let rig = proxied(&db);
    let _stderr = rig.run_expect_fail();
    toxi_enable("mongo");
    assert_recorded_failure(
        &rig.config_path(),
        rig.export_name(),
        &["no answer from", "server selection timeout"],
    );
}
