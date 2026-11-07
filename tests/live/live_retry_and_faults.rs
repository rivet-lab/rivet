//! Runtime retry behaviour and mid-stream fault-injection via Toxiproxy.
//!
//! QA backlog:
//!   * Task 4.1 — retry contract at runtime (not just classifier-level)
//!   * Task 4.2 — mid-stream transient failures, then recovery
//!
//! Every test that manipulates Toxiproxy registers the `postgres` proxy
//! lazily via `ensure_toxi_proxy("postgres", 15432, "postgres:5432")` and
//! clears leftover toxics at the start so the suite is resilient to
//! interleaved reruns.

use std::time::Duration;

use crate::common::*;

/// Idempotent setup: register the proxy and wipe any previous toxics.
fn reset_postgres_proxy() {
    ensure_toxi_proxy("postgres", 15432, "postgres:5432");
    toxi_reset_toxics("postgres");
}

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn clean_postgres_via_toxi_works_as_a_baseline_and_exports_successfully() {
    // Negative control: without any toxic, routing through Toxiproxy must
    // behave identically to the direct postgres URL.  If this fails, the
    // subsequent fault-injection tests cannot be trusted.
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Toxiproxy);
    reset_postgres_proxy();

    // Seed on primary, export through toxi URL.
    let table = seed_pg_numeric_table(5);
    let export_name = unique_name("qa41_baseline");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export_name)
        .source_url(POSTGRES_TOXI_URL)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .dest_path(out.path().to_path_buf());
    let out_run = rig.run_args(&["--export", &export_name]);
    assert!(
        out_run.status.success(),
        "baseline run through toxiproxy failed; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn export_survives_transient_latency_added_via_toxiproxy() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Toxiproxy);
    reset_postgres_proxy();

    let toxic = toxi_add_latency("postgres", 50);

    let table = seed_pg_numeric_table(5);
    let export_name = unique_name("qa41_latency");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export_name)
        .source_url(POSTGRES_TOXI_URL)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 2")
        .dest_path(out.path().to_path_buf());
    let t0 = std::time::Instant::now();
    let out_run = rig.run_args(&["--export", &export_name]);
    let took = t0.elapsed();
    let _ = toxic;
    toxi_reset_toxics("postgres");

    assert!(
        out_run.status.success(),
        "export must complete under 50ms latency; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
    assert!(
        took >= Duration::from_millis(3 * 50),
        "the 50 ms toxic must have slowed at least three sequential round trips; took {took:?}"
    );
    let ran =
        recorded_run_outcome(&rig.config_path(), &export_name).expect("the run's metrics row");
    assert_eq!(ran.retries, 0, "latency must not cost a retry: {ran:?}");
}

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn export_fails_cleanly_when_toxiproxy_is_disabled_before_run() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Toxiproxy);
    reset_postgres_proxy();

    toxi_disable("postgres");

    let export_name = unique_name("qa42_disabled");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export_name)
        .source_url(POSTGRES_TOXI_URL)
        .query("SELECT 1::int")
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 0")
        .dest_path(out.path().to_path_buf());
    let out_run = rig.run_args(&["--export", &export_name]);

    // Re-enable before assertions so subsequent tests are not affected.
    toxi_enable("postgres");

    assert!(
        !out_run.status.success(),
        "export through a disabled proxy must exit non-zero"
    );
    let failed = assert_recorded_failure(&rig.config_path(), &export_name, &["connectivity error"]);
    assert_eq!(
        failed.retries, 0,
        "max_retries 0 is not retried: {failed:?}"
    );
    assert_eq!(
        failed.error_class.as_deref(),
        Some("connection"),
        "{failed:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn export_recovers_after_mid_stream_connection_reset_with_retries() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::DuckDb);
    require_alive(LiveService::Toxiproxy);
    reset_postgres_proxy();

    let table = seed_pg_numeric_table(50);
    let export_name = unique_name("qa42_midstream");
    let (out_host, out_container) = duckdb_shared_workdir("qa42_midstream_oracle");
    let rig = Rig::pg_batch(&export_name)
        .source_url(POSTGRES_TOXI_URL)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 3")
        .export_line("  retry_backoff_ms: 1500")
        .dest_path(out_host.clone());

    let marker = std::env::temp_dir().join(format!("rivet-{export_name}.paused"));
    let _ = std::fs::remove_file(&marker);
    let marker_env = marker.to_string_lossy().into_owned();
    let mut run = rig.spawn_args_env(
        &["--export", &export_name],
        &[
            ("RIVET_TEST_PAUSE_AT", "pg_after_snapshot_open:2000"),
            ("RIVET_TEST_PAUSE_MARKER", &marker_env),
        ],
    );
    let reset = reset_peer_until("postgres", &marker, || {
        run.stderr_so_far().contains("retry 2/") || matches!(run.try_wait(), Ok(Some(_)))
    });
    let out_run = run.wait_with_output().expect("reap the run");
    let _ = std::fs::remove_file(&marker);

    assert!(
        reset,
        "the run never paused after its snapshot: nothing was cut"
    );
    assert!(
        out_run.status.success(),
        "export must recover after a mid-stream reset and a refused reconnect; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
    let ran =
        recorded_run_outcome(&rig.config_path(), &export_name).expect("the run's metrics row");
    assert!(
        ran.status == "success" && ran.retries >= 2,
        "the run must have retried the cut and the refused reconnect; recorded: {ran:?}"
    );
    let files = files_with_extension(&out_host, "parquet");
    assert_eq!(files.len(), 1, "one part expected; got {files:?}");
    duckdb_assert_complete(
        &out_container,
        "id",
        50,
        "a retry across a mid-stream reset",
    );
}

// ─── Task 4.1: retry counter & classification at runtime ───────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn permanent_sql_error_fails_fast_without_exhausting_retries() {
    // An invalid SQL query (syntax error) is classified as permanent.
    // Even with max_retries=5 the runtime must NOT retry — it should bail
    // after the first attempt with an actionable error.  Observable: the
    // process exit time is short (< 2s) even with a high retry_backoff_ms.
    require_alive(LiveService::Postgres);

    let export_name = unique_name("qa43_permerr");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export_name)
        // intentional typo → a permanent SQL syntax error
        .query("SELCT 1")
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 5")
        .export_line("  retry_backoff_ms: 5000")
        .dest_path(out.path().to_path_buf());

    let t0 = std::time::Instant::now();
    let out_run = rig.run_args(&["--export", &export_name]);
    let elapsed = t0.elapsed();

    assert!(
        !out_run.status.success(),
        "invalid query must exit non-zero"
    );

    // If retries had been executed, wall time would approach
    //   sum(retry_backoff_ms * 2^k for k in 0..5) ≈ 5_000 * 31 = 155 s.
    // Permanent-error classification caps this at the first attempt +
    // connection overhead — easily under 15 seconds.
    assert!(
        elapsed < Duration::from_secs(15),
        "permanent error must short-circuit retry loop; elapsed={elapsed:?}"
    );
    let failed = assert_recorded_failure(
        &rig.config_path(),
        &export_name,
        &["syntax error at or near"],
    );
    assert_eq!(
        failed.retries, 0,
        "a permanent error is not retried: {failed:?}"
    );
}

const CUT_ROWS: i64 = 100;
const CUT_PAD_BYTES: usize = 60_000;
const CUT_AFTER_BYTES: u64 = 5 * CUT_PAD_BYTES as u64 + (CUT_PAD_BYTES / 2) as u64;

/// A table of `rows` rows, each holding `pad_bytes` of text, so one result row is one large message.
fn seed_pg_padded_table(rows: i64, pad_bytes: usize) -> PgTable {
    let name = unique_name("rivet_qa_cut");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, pad TEXT NOT NULL); \
         INSERT INTO {name} SELECT g, repeat('x', {pad_bytes}) FROM generate_series(1, {rows}) g;"
    ))
    .expect("seed padded postgres table");
    PgTable::adopt(name)
}

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy + duckdb"]
fn postgres_export_retries_a_connection_cut_in_the_middle_of_a_row_and_delivers_exactly_once() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::DuckDb);
    require_alive(LiveService::Toxiproxy);
    reset_postgres_proxy();

    let table = seed_pg_padded_table(CUT_ROWS, CUT_PAD_BYTES);
    let export_name = unique_name("pg_cut_midrow");
    let (out_host, out_container) = duckdb_shared_workdir("pg_cut_midrow_oracle");
    let rig = Rig::pg_batch(&export_name)
        .source_url(POSTGRES_TOXI_URL)
        .query(&format!("SELECT id, pad FROM {}", table.name()))
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 3")
        .export_line("  retry_backoff_ms: 3000")
        .dest_path(out_host.clone());
    assert_midrow_cut_retried_and_delivered_exactly_once(
        &rig,
        &export_name,
        "postgres",
        CUT_AFTER_BYTES,
        CUT_ROWS,
        CUT_PAD_BYTES,
        &out_container,
    );
}
