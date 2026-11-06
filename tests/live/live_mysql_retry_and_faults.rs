//! Runtime retry behaviour and mid-stream fault-injection via Toxiproxy —
//! MySQL twin of `tests/live_retry_and_faults.rs`.
//!
//! `RetryClass` and the retry backoff loop live in `pipeline::retry` and
//! `pipeline::single` respectively; both are driver-agnostic. This file
//! mirrors the PG retry matrix against MySQL so a regression of the
//! transient/permanent classifier on the MySQL error surface is caught
//! independently.
//!
//! ## Test symmetry with `live_retry_and_faults.rs`
//!
//! | PG test                                                                  | MySQL twin (this file)                                                             |
//! |--------------------------------------------------------------------------|------------------------------------------------------------------------------------|
//! | `clean_postgres_via_toxi_works_as_a_baseline_and_exports_successfully`   | `mysql_clean_via_toxi_works_as_a_baseline_and_exports_successfully`                |
//! | `export_survives_transient_latency_added_via_toxiproxy`                  | `mysql_export_survives_transient_latency_added_via_toxiproxy`                      |
//! | `export_fails_cleanly_when_toxiproxy_is_disabled_before_run`             | `mysql_export_fails_cleanly_when_toxiproxy_is_disabled_before_run`                 |
//! | `export_recovers_after_mid_stream_proxy_disable_then_enable_with_retries`| `mysql_export_recovers_after_mid_stream_proxy_disable_then_enable_with_retries`    |
//! | `permanent_sql_error_fails_fast_without_exhausting_retries`              | `mysql_permanent_sql_error_fails_fast_without_exhausting_retries`                  |

use std::time::Duration;

use crate::common::*;

/// Idempotent setup: register the `mysql` toxi proxy (listen on 13306 →
/// `mysql:3306`) and wipe any leftover toxics.
fn reset_mysql_proxy() {
    ensure_toxi_proxy("mysql", 13306, "mysql:3306");
    toxi_reset_toxics("mysql");
}

#[test]
#[ignore = "live: requires docker compose mysql + toxiproxy"]
fn mysql_clean_via_toxi_works_as_a_baseline_and_exports_successfully() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Mysql);
    require_alive(LiveService::Toxiproxy);
    reset_mysql_proxy();

    let table = seed_mysql_numeric_table(5);
    let export_name = unique_name("qa41my_baseline");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_batch(&export_name)
        .source_url(MYSQL_TOXI_URL)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .dest_path(out.path().to_path_buf());
    let out_run = rig.run_args(&["--export", &export_name]);
    assert!(
        out_run.status.success(),
        "baseline mysql run through toxiproxy failed; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + toxiproxy"]
fn mysql_export_survives_transient_latency_added_via_toxiproxy() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Mysql);
    require_alive(LiveService::Toxiproxy);
    reset_mysql_proxy();

    let toxic = toxi_add_latency("mysql", 50);

    let table = seed_mysql_numeric_table(5);
    let export_name = unique_name("qa41my_latency");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_batch(&export_name)
        .source_url(MYSQL_TOXI_URL)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 2")
        .dest_path(out.path().to_path_buf());
    let out_run = rig.run_args(&["--export", &export_name]);

    let _ = toxic;
    toxi_reset_toxics("mysql");

    assert!(
        out_run.status.success(),
        "mysql export must complete under 50ms latency; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + toxiproxy"]
fn mysql_export_fails_cleanly_when_toxiproxy_is_disabled_before_run() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Mysql);
    require_alive(LiveService::Toxiproxy);
    reset_mysql_proxy();

    toxi_disable("mysql");

    let export_name = unique_name("qa42my_disabled");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_batch(&export_name)
        .source_url(MYSQL_TOXI_URL)
        .query("SELECT 1 AS n")
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 0")
        .dest_path(out.path().to_path_buf());
    let out_run = rig.run_args(&["--export", &export_name]);

    toxi_enable("mysql");

    assert!(
        !out_run.status.success(),
        "mysql export through a disabled proxy must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out_run.stderr);
    let low = stderr.to_lowercase();
    assert!(
        low.contains("connection")
            || low.contains("refused")
            || low.contains("reset")
            || low.contains("timeout")
            || low.contains("failed")
            || low.contains("broken"),
        "stderr must include a network-level diagnostic; got:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + toxiproxy"]
fn mysql_export_recovers_after_mid_stream_proxy_disable_then_enable_with_retries() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Mysql);
    require_alive(LiveService::DuckDb);
    require_alive(LiveService::Toxiproxy);
    reset_mysql_proxy();

    let table = seed_mysql_numeric_table(50);
    let export_name = unique_name("qa42my_midstream");
    // Output lands in the shared bind mount so the DuckDB container can read
    // it — the oracle below must not share rivet's own parquet codec.
    let (out_host, out_container) = duckdb_shared_workdir("qa42my_midstream_oracle");
    let rig = Rig::mysql_batch(&export_name)
        .source_url(MYSQL_TOXI_URL)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 3")
        .export_line("  retry_backoff_ms: 200")
        .dest_path(out_host.clone());

    let bg = std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(50));
        toxi_disable("mysql");
        std::thread::sleep(Duration::from_millis(500));
        toxi_enable("mysql");
    });

    let out_run = rig.run_args(&["--export", &export_name]);
    let _ = bg.join();

    assert!(
        out_run.status.success(),
        "mysql export must recover after transient proxy outage; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
    // File COUNT was the only assertion here, so rows lost at the reconnect
    // boundary passed unnoticed. MySQL parity means parity of the row set, not of
    // the file count.
    // FILE count stays on read_dir: counting directory entries never goes
    // through rivet's codec, so a container round-trip would buy nothing.
    let files = files_with_extension(&out_host, "parquet");
    assert_eq!(files.len(), 1, "one part expected; got {files:?}");

    // ROWS go through DuckDB. `total_parquet_rows` decodes with the SAME parquet
    // crate rivet encodes with, so a fault in that shared path cancels out and
    // the assertion passes over corrupt output. DuckDB does not share it.
    // Distinct is asserted alongside the total because a retry can satisfy one
    // and break the other: fewer rows is loss, fewer distinct is duplication.
    duckdb_assert_complete(
        &out_container,
        "id",
        50,
        "a retry across a mid-stream connection death",
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_permanent_sql_error_fails_fast_without_exhausting_retries() {
    require_alive(LiveService::Mysql);

    let export_name = unique_name("qa43my_permerr");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_batch(&export_name)
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
        "invalid mysql query must exit non-zero"
    );

    // With 5 retries × 5s base backoff, full retry chain would approach
    // 5_000 * 31 ≈ 155 s. Permanent-error classification must short-circuit.
    assert!(
        elapsed < Duration::from_secs(15),
        "permanent error must short-circuit retry loop; elapsed={elapsed:?}"
    );
    let stderr = String::from_utf8_lossy(&out_run.stderr);
    let low = stderr.to_lowercase();
    assert!(
        low.contains("syntax") || low.contains("error") || low.contains("sql"),
        "stderr must include the driver's error text; got:\n{stderr}"
    );
}

const CUT_ROWS: i64 = 100;
const CUT_PAD_BYTES: usize = 60_000;
const CUT_AFTER_BYTES: u64 = 5 * CUT_PAD_BYTES as u64 + (CUT_PAD_BYTES / 2) as u64;

/// A table of `rows` rows, each holding `pad_bytes` of text, so one result row is one large packet.
fn seed_mysql_padded_table(rows: i64, pad_bytes: usize) -> MysqlTable {
    use mysql::prelude::Queryable;

    let name = unique_name("rivet_qa_cut");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, pad LONGTEXT NOT NULL) ENGINE=InnoDB"
    ))
    .expect("create padded mysql table");
    let table = MysqlTable::adopt(name.clone());
    c.query_drop(format!(
        "INSERT INTO {name} (id, pad) \
         WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < {rows}) \
         SELECT n, REPEAT('x', {pad_bytes}) FROM s"
    ))
    .expect("seed padded mysql rows");
    table
}

#[test]
#[ignore = "live: requires docker compose mysql + toxiproxy + duckdb"]
fn mysql_export_retries_a_connection_cut_in_the_middle_of_a_row_and_delivers_exactly_once() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Mysql);
    require_alive(LiveService::DuckDb);
    require_alive(LiveService::Toxiproxy);
    reset_mysql_proxy();

    let table = seed_mysql_padded_table(CUT_ROWS, CUT_PAD_BYTES);
    let export_name = unique_name("my_cut_midrow");
    let (out_host, out_container) = duckdb_shared_workdir("my_cut_midrow_oracle");
    let rig = Rig::mysql_batch(&export_name)
        .source_url(MYSQL_TOXI_URL)
        .query(&format!("SELECT id, pad FROM {}", table.name()))
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 3")
        .export_line("  retry_backoff_ms: 3000")
        .dest_path(out_host.clone());

    toxi_add_limit_data("mysql", CUT_AFTER_BYTES, "downstream");
    let mut run = rig.spawn_args_env(&["--export", &export_name], &[]);
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    while std::time::Instant::now() < deadline {
        if run.stderr_so_far().contains("retry 1/") {
            toxi_reset_toxics("mysql");
            break;
        }
        if run.try_wait().expect("poll the run").is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let out_run = run.wait_with_output().expect("reap the run");
    toxi_reset_toxics("mysql");

    let stderr = String::from_utf8_lossy(&out_run.stderr);
    assert!(
        out_run.status.success(),
        "a connection cut mid-row must be retried and the run must exit 0; stderr:\n{stderr}"
    );
    let state = StateDb::next_to_config(&rig.config_path());
    let metrics = state.metrics_row(&state.latest_run_id(&export_name));
    assert!(
        metrics.retries.unwrap_or(0) >= 1,
        "export_metrics.retries must record the retry; got {:?}; stderr:\n{stderr}",
        metrics.retries
    );
    duckdb_declared_assert_complete(
        &out_container,
        "id",
        CUT_ROWS,
        "a retry across a connection cut in the middle of a row",
    );
    assert_eq!(
        duckdb_declared_distinct_set(&out_container, "id"),
        (1..=CUT_ROWS).map(|i| i.to_string()).collect(),
        "the delivered id set must equal the source id set"
    );
    let intact = duckdb_run_sql_json(&format!(
        "SELECT count(*) FROM read_parquet('{out_container}/**/*.parquet') \
         WHERE length(pad) = {CUT_PAD_BYTES}"
    ));
    assert_eq!(
        intact["rows"][0][0].as_str(),
        Some(CUT_ROWS.to_string().as_str()),
        "every delivered pad must be whole"
    );
}
