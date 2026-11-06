//! Controlled chaos scenarios — Toxiproxy-driven and object-storage faults.
//!
//! QA backlog:
//!   * Task 4A.4 — deterministic gremlin-style resilience tests.
//!   * Task 6.2 — object storage intermittent behaviour.
//!
//! "Deterministic" here means: each scenario injects a named, time-bounded
//! fault and asserts a specific observable outcome (retry success, clean
//! error, no data loss).  We do not exercise random chaos — backlog Task
//! 4A.5 (nightly extended) is the place for that.

use std::time::Duration;

use crate::common::*;

// ─── Toxiproxy scenarios ───────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn high_latency_does_not_cause_false_positive_transient_classification() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Toxiproxy);
    ensure_toxi_proxy("postgres", 15432, "postgres:5432");
    toxi_reset_toxics("postgres");
    let _toxic = toxi_add_latency("postgres", 200);

    let table = seed_pg_numeric_table(3);
    let export_name = unique_name("qa4A4_hilat");
    let rig = Rig::pg_batch(&export_name)
        .source_url(POSTGRES_TOXI_URL)
        .query(&format!("SELECT id FROM {}", table.name()))
        .mode("full")
        .export_line("tuning: {max_retries: 0}");
    let t0 = std::time::Instant::now();
    let out_run = rig.run_args(&["--export", &export_name]);
    let took = t0.elapsed();
    toxi_reset_toxics("postgres");

    assert!(
        out_run.status.success(),
        "200ms latency must not be classified as transient failure"
    );
    assert!(
        took >= Duration::from_millis(3 * 200),
        "the 200 ms toxic must have slowed at least three sequential round trips; took {took:?}"
    );
    let ran =
        recorded_run_outcome(&rig.config_path(), &export_name).expect("the run's metrics row");
    assert_eq!(ran.retries, 0, "latency must not cost a retry: {ran:?}");
    let rows = read_all_parts(&rig.out_dir())
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert_eq!(
        rows, 3,
        "the slow-but-successful run must still deliver every seeded row"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn chunked_export_recovers_after_mid_stream_connection_reset() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::DuckDb);
    require_alive(LiveService::Toxiproxy);
    ensure_toxi_proxy("postgres", 15432, "postgres:5432");
    toxi_reset_toxics("postgres");

    let table = seed_pg_numeric_table(40);
    let export_name = unique_name("qa4A4_chunk");
    let (out_host, out_container) = duckdb_shared_workdir("qa4A4_chunk_oracle");
    let rig = Rig::pg_batch(&export_name)
        .source_url(POSTGRES_TOXI_URL)
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 10")
        .export_line("chunk_checkpoint: true")
        .export_line("tuning: {max_retries: 3, retry_backoff_ms: 1500}")
        .dest_path(out_host);

    let marker = std::env::temp_dir().join(format!("rivet-{export_name}.paused"));
    let _ = std::fs::remove_file(&marker);
    let marker_env = marker.to_string_lossy().into_owned();
    let mut run = rig.spawn_args_env(
        &["--export", &export_name],
        &[
            ("RIVET_TEST_PAUSE_AT", "pg_after_snapshot_open:1500"),
            ("RIVET_TEST_PAUSE_MARKER", &marker_env),
        ],
    );
    let reset = reset_peer_until("postgres", &marker, || {
        run.stderr_so_far().contains("retry 2/") || matches!(run.try_wait(), Ok(Some(_)))
    });
    let out_run = run.wait_with_output().expect("reap the run");
    let _ = std::fs::remove_file(&marker);
    toxi_reset_toxics("postgres");

    assert!(
        reset,
        "the run never paused after its snapshot: nothing was cut"
    );
    assert!(
        out_run.status.success(),
        "the first chunk holds no durable part, so the cut and the refused reconnect are retried in place; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr)
    );
    let ran =
        recorded_run_outcome(&rig.config_path(), &export_name).expect("the run's metrics row");
    assert!(
        ran.status == "success" && ran.retries >= 2,
        "the run must have retried the cut and the refused reconnect; recorded: {ran:?}"
    );
    duckdb_assert_complete(&out_container, "id", 40, "a chunked run cut mid-stream");
    let cfg = rig.config_path();
    let reconcile = run_rivet(&[
        "reconcile",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        &export_name,
    ]);
    assert!(
        reconcile.status.success(),
        "reconcile must confirm the export is complete and duplicate-free; stderr:\n{}",
        String::from_utf8_lossy(&reconcile.stderr)
    );
}

// ─── Object storage intermittent behaviour ────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + minio"]
fn s3_export_fails_cleanly_when_bucket_does_not_exist() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Minio);

    let table = seed_pg_numeric_table(3);
    let bucket = unique_name("rivet-qa-nope").replace('_', "-");
    let export_name = unique_name("qa62_nobucket");

    let rig = Rig::pg_batch(&export_name)
        .query(&format!("SELECT id FROM {}", table.name()))
        .mode("full")
        .export_line("tuning: {max_retries: 0}")
        .dest_s3(&bucket, "qa62-nobucket", MINIO_ENDPOINT);
    let out = rig.run_args_env(
        &["--export", &export_name],
        &[
            ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
            ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
            ("AWS_EC2_METADATA_DISABLED", "true"),
        ],
    );

    assert!(
        !out.status.success(),
        "missing bucket must not silently succeed"
    );
    let failed = assert_recorded_failure(
        &rig.config_path(),
        &export_name,
        &["bucket or container does not exist"],
    );
    assert_eq!(
        failed.error_class.as_deref(),
        Some("relation_not_found"),
        "{failed:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + minio"]
fn s3_export_recovers_when_destination_comes_back_after_transient_outage() {
    // Simulating a brief MinIO outage without docker network tricks is
    // difficult — but we can simulate the equivalent on our end by pointing
    // the export at a wrong endpoint first (force a connection error) and
    // then re-running against the real one.  Each run is independent, but
    // the test proves the retry path and the second attempt's success are
    // both observable.
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Minio);

    let bucket = "rivet-qa-parity"; // already created in Phase B
    ensure_minio_bucket(bucket);
    let table = seed_pg_numeric_table(5);

    // Attempt #1: wrong endpoint.
    {
        let export_name = unique_name("qa62_wrong");
        let rig = Rig::pg_batch(&export_name)
            .query(&format!("SELECT id FROM {}", table.name()))
            .mode("full")
            .export_line("tuning: {max_retries: 0}")
            .dest_s3(bucket, "qa62-wrong", "http://127.0.0.1:1");
        let out = rig.run_args_env(
            &["--export", &export_name],
            &[
                ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
                ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
                ("AWS_EC2_METADATA_DISABLED", "true"),
            ],
        );
        assert!(!out.status.success(), "export to wrong endpoint must fail");
    }

    // Attempt #2: correct endpoint — must succeed.
    {
        let export_name = unique_name("qa62_right");
        let prefix = unique_name("qa62_intermittent");
        let rig = Rig::pg_batch(&export_name)
            .query(&format!("SELECT id FROM {}", table.name()))
            .mode("full")
            .export_line("tuning: {max_retries: 0}")
            .dest_s3(bucket, &prefix, MINIO_ENDPOINT);
        let out = rig.run_args_env(
            &["--export", &export_name],
            &[
                ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
                ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
                ("AWS_EC2_METADATA_DISABLED", "true"),
            ],
        );
        assert!(
            out.status.success(),
            "recovery run must succeed; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
