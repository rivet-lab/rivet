//! Parallel-keyset coverage through the canonical Rig, on EVERY SQL keyset engine
//! (feat/parallel-keyset). One assertion helper × {PostgreSQL, MySQL, SQL Server}
//! × the parity/fan-out scenario. Replaces the single-engine, hand-rolled
//! `write_config`/`run_rivet_export` parallel tests with the rig seam so the
//! chunking / resilience matrices can cite a real per-engine cell.
//!
//! Mongo's `parallel: N` is the SEPARATE `_id`-range path (`mongo_parallel`), not
//! SQL keyset, so it is out of this file by construction.

use std::collections::BTreeSet;

use arrow::array::{Array, Int64Array};

use crate::common::*;

/// Total rows across every parquet part — schema-agnostic (works for any key type:
/// varchar / decimal / unsigned bigint where the typed `id` reader below can't apply).
fn count_rows(dir: &std::path::Path) -> usize {
    read_all_parts(dir).iter().map(|b| b.num_rows()).sum()
}

/// Distinct `pk_w{range}` workers the parallel runner fanned out to (a collapse to
/// one worker == a boundary probe that could not read the key). Format-agnostic —
/// the `_pk_w{ridx}_` token is in every part name regardless of parquet/csv.
fn worker_fanout(dir: &std::path::Path) -> usize {
    ["parquet", "csv"]
        .iter()
        .flat_map(|ext| files_with_extension(dir, ext))
        .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(String::from))
        .filter_map(|n| {
            n.split("_pk_w")
                .nth(1)
                .and_then(|s| s.split('_').next())
                .map(String::from)
        })
        .collect::<BTreeSet<String>>()
        .len()
}

/// Read the BIGINT `id` column across every parquet part + count the DISTINCT
/// `pk_w{range}` workers the parallel runner fanned out to. A collapse to one
/// worker (a boundary probe that could not read the key) shows as workers == 1.
fn id_set_and_fanout(dir: &std::path::Path) -> (usize, BTreeSet<i64>, usize) {
    let mut count = 0usize;
    let mut keys = BTreeSet::new();
    for b in read_all_parts(dir) {
        let id = b
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        for i in 0..id.len() {
            count += 1;
            keys.insert(id.value(i));
        }
    }
    let workers: BTreeSet<String> = files_with_extension(dir, "parquet")
        .iter()
        .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(String::from))
        .filter_map(|n| {
            n.split("_pk_w")
                .nth(1)
                .and_then(|s| s.split('_').next())
                .map(String::from)
        })
        .collect();
    (count, keys, workers.len())
}

/// Parquet parts per range worker, keyed `w{ridx}` (the `_pk_w{ridx}_{page}` token
/// in every part name). Lets a test assert a SPECIFIC range left N durable pages —
/// needed to prove a mid-range failure's pre-failure pages are on disk (#200-1).
fn worker_part_counts(dir: &std::path::Path) -> std::collections::BTreeMap<String, usize> {
    let mut out: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for p in files_with_extension(dir, "parquet") {
        if let Some(ridx) = p
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.split("_pk_w").nth(1))
            .and_then(|s| s.split('_').next())
        {
            *out.entry(format!("w{ridx}")).or_default() += 1;
        }
    }
    out
}

/// The shared scenario: run parallel keyset on an already-seeded `1..=n` integer
/// key and assert (1) every row round-trips exactly once (structural parity across
/// the N ranges) and (2) the run FANNED OUT to ≥2 workers (did not collapse).
fn assert_parallel_parity(rig: Rig, n: usize) {
    rig.run_ok();
    let (count, keys, workers) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, n,
        "row count must round-trip exactly across all workers (no boundary drop/dupe)"
    );
    let expected: BTreeSet<i64> = (1..=n as i64).collect();
    assert_eq!(
        keys, expected,
        "the union of all workers' keys must equal 1..=n"
    );
    assert!(
        workers >= 2,
        "parallel keyset must fan out to ≥2 workers, got {workers} — the boundary probe collapsed"
    );
}

/// `chunk_by_key: id` + `parallel: 4` + a small `chunk_size` so each ~n/4 range
/// still pages several times (both the inter-worker boundary and the intra-worker
/// `key > last` boundary are exercised).
fn parallel_keyset(rig: Rig) -> Rig {
    rig.mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 500")
}

const N: usize = 3000;

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_parity_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_par_pg");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, {N}) g;"
    ))
    .unwrap();
    assert_parallel_parity(
        parallel_keyset(Rig::pg_batch(&format!("public.{table}"))),
        N,
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_parity_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_par_my");
    let mut c = mysql_connect();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    c.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {N}) \
         SELECT n, n FROM seq"
    ))
    .unwrap();
    assert_parallel_parity(parallel_keyset(Rig::mysql_batch(&table)), N);
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mssql"]
fn parallel_keyset_parity_mssql() {
    require_alive(LiveService::Mssql);
    let table = unique_name("pk_par_ms");
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ));
    // A recursive-CTE tally (MAXRECURSION 0 lifts the 100 cap) seeds 1..=N.
    mssql_exec(&format!(
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < {N}) \
         INSERT INTO {table} (id, payload) SELECT n, n FROM nums OPTION (MAXRECURSION 0)"
    ));
    assert_parallel_parity(parallel_keyset(Rig::mssql_batch(&table)), N);
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
}

// ── Run-unique part naming: two full runs into ONE prefix must not clobber ───

/// Two consecutive FULL parallel-keyset runs into the SAME destination prefix.
/// Each run re-exports all `n` rows under a FRESH run_id, so run 2's parts must
/// sit ALONGSIDE run 1's (the part base is `{export}_{run_tag}_pk_w{ridx}_{page}`,
/// run_tag = sanitized run_id) — never overwrite them. Re-running without --resume
/// is a WARN that appends, not a clobber (finalize::rerun_warning_message).
///
/// Discriminator = the TOTAL row count across ALL parquet on disk: `2*n` if both
/// runs' parts survive, `n` if run 2's `pk_w{ridx}_{page}` names collided with
/// run 1's and idempotent_overwrite silently replaced them. This is the N-worker ×
/// 2-run collision the runner-coverage matrix warned keyset_PARALLEL could lack —
/// it goes RED if the run_tag is dropped from the part base.
fn assert_two_full_runs_no_clobber(rig: Rig, n: usize) {
    rig.run_ok();
    assert_eq!(
        id_set_and_fanout(&rig.out_dir()).0,
        n,
        "run 1 writes exactly n rows"
    );
    rig.run_ok();
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count,
        2 * n,
        "two full runs into one prefix must ACCUMULATE 2*n parquet rows; \
         count == n means run 2 clobbered run 1's parts (run_tag not in the part name)"
    );
    assert_eq!(
        keys,
        (1..=n as i64).collect::<BTreeSet<i64>>(),
        "the distinct key set is still 1..=n (each id present once per run)"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_two_full_runs_into_same_prefix_no_clobber_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_clob_pg");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, {N}) g;"
    ))
    .unwrap();
    assert_two_full_runs_no_clobber(
        parallel_keyset(Rig::pg_batch(&format!("public.{table}"))),
        N,
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

// ── Per-range crash-recovery (iteration 2) across every engine ───────────────

/// Crash after ONE range commits (durable in file_log + keyset_range `done=1`),
/// resume, and assert the DESTINATION manifest declares ALL `n` rows — the done
/// range's parts are REHYDRATED, not orphaned. Manifest-driven (not a parquet
/// re-read): the committed parquet survives regardless, so a manifest-orphaned
/// part is invisible to a data re-read. `parallel: 4` + `chunk_checkpoint: true`.
fn assert_parallel_crash_recovery(rig: Rig, n: usize) {
    // Run 1: HARD-EXIT after range 0's atomic commit (a worker panic would defer to
    // the scope join, by which point every worker has finished).
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:0");
    assert!(
        !crash.status.success(),
        "the injected hard-exit must make run 1 fail"
    );
    // Run 2: resume — skips the done range (rehydrates it), re-runs the rest.
    rig.run_ok();
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(rig.out_dir().join("manifest.json")).unwrap())
            .expect("destination manifest.json");
    assert_eq!(
        manifest["row_count"].as_i64(),
        Some(n as i64),
        "resumed manifest must declare every row (done range not orphaned); got {}",
        manifest["row_count"]
    );
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, n,
        "the parquet union must hold every row once (no dup)"
    );
    assert_eq!(keys, (1..=n as i64).collect::<BTreeSet<i64>>());
}

fn crash_rig(rig: Rig) -> Rig {
    rig.mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 200")
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_crash_recovery_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_crash_pg");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    assert_parallel_crash_recovery(crash_rig(Rig::pg_batch(&format!("public.{table}"))), 2000);
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_crash_recovery_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_crash_my");
    let mut c = mysql_connect();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    c.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 2000) \
         SELECT n, n FROM seq"
    ))
    .unwrap();
    assert_parallel_crash_recovery(crash_rig(Rig::mysql_batch(&table)), 2000);
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mssql"]
fn parallel_keyset_crash_recovery_mssql() {
    require_alive(LiveService::Mssql);
    let table = unique_name("pk_crash_ms");
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ));
    mssql_exec(&format!(
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 2000) \
         INSERT INTO {table} (id, payload) SELECT n, n FROM nums OPTION (MAXRECURSION 0)"
    ));
    assert_parallel_crash_recovery(crash_rig(Rig::mssql_batch(&table)), 2000);
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
}

// ── Incremental-by-key (iteration 3) across every engine ─────────────────────

/// Parallel incremental: a CLEAN re-run pulls only keys past the persisted anchor,
/// across N workers. Discriminator = the TOTAL row count on disk (a set dedups):
/// 1000 → 1000 (unchanged) → 1500 (after +500 insert), never a full re-read (2000).
/// `parallel: 4` + `chunk_checkpoint: true` + `keyset_incremental: true`. The
/// `seed_more` closure inserts the 500 higher keys between run 2 and run 3.
fn assert_parallel_incremental(rig: Rig, seed_more: impl FnOnce()) {
    rig.run_ok();
    assert_eq!(
        id_set_and_fanout(&rig.out_dir()).0,
        1000,
        "run 1 exports all 1000"
    );
    rig.run_ok();
    assert_eq!(
        id_set_and_fanout(&rig.out_dir()).0,
        1000,
        "unchanged re-run adds zero (a full re-read would double it to 2000)"
    );
    seed_more();
    rig.run_ok();
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 1500,
        "incremental adds ONLY the 500 new keys (2500 = full re-read)"
    );
    assert_eq!(keys, (1..=1500i64).collect::<BTreeSet<i64>>());
}

fn incremental_rig(rig: Rig) -> Rig {
    rig.mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 200")
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_incremental_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_inc_pg");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 1000) g;"
    ))
    .unwrap();
    let t2 = table.clone();
    assert_parallel_incremental(
        incremental_rig(Rig::pg_batch(&format!("public.{table}"))),
        || {
            let mut c2 = pg_connect();
            c2.execute(
                &format!("INSERT INTO {t2} SELECT g, g FROM generate_series(1001, 1500) g"),
                &[],
            )
            .unwrap();
        },
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_incremental_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_inc_my");
    let mut c = mysql_connect();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    c.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 1000) \
         SELECT n, n FROM seq"
    ))
    .unwrap();
    let t2 = table.clone();
    assert_parallel_incremental(incremental_rig(Rig::mysql_batch(&table)), || {
        let mut c2 = mysql_connect();
        c2.query_drop("SET SESSION cte_max_recursion_depth = 20000")
            .unwrap();
        c2.query_drop(format!(
            "INSERT INTO {t2} (id, payload) \
             WITH RECURSIVE seq AS (SELECT 1001 n UNION ALL SELECT n+1 FROM seq WHERE n < 1500) \
             SELECT n, n FROM seq"
        ))
        .unwrap();
    });
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mssql"]
fn parallel_keyset_incremental_mssql() {
    require_alive(LiveService::Mssql);
    let table = unique_name("pk_inc_ms");
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ));
    mssql_exec(&format!(
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 1000) \
         INSERT INTO {table} (id, payload) SELECT n, n FROM nums OPTION (MAXRECURSION 0)"
    ));
    let t2 = table.clone();
    assert_parallel_incremental(incremental_rig(Rig::mssql_batch(&table)), || {
        mssql_exec(&format!(
            "WITH nums AS (SELECT 1001 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 1500) \
             INSERT INTO {t2} (id, payload) SELECT n, n FROM nums OPTION (MAXRECURSION 0)"
        ));
    });
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
}

// ── Edge-case scenarios (regression guards for the under-tested surface) ──────

/// `parallel: N` with N GREATER than the row count: the OFFSET percentile sampler
/// yields fewer distinct boundaries than requested, so the runner must produce fewer,
/// larger ranges and still export EVERY row exactly once — never crash or drop.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_more_workers_than_rows_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_fewrows");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 5) g;"
    ))
    .unwrap();
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 8")
        .export_line("chunk_size: 2");
    rig.run_ok();
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 5,
        "every row exported once even with more workers than rows"
    );
    assert_eq!(keys, (1..=5i64).collect::<BTreeSet<i64>>());
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Parallel keyset on a VARCHAR key: the boundary literals are text, injected + escaped
/// per dialect. Fan-out must not collapse and every key must round-trip (a text-boundary
/// mishandling would drop a range).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_varchar_key_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_vc");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (k VARCHAR(20) PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT 'k' || lpad(g::text, 6, '0'), g FROM generate_series(1, 3000) g;"
    ))
    .unwrap();
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("parallel: 4")
        .export_line("chunk_size: 500");
    rig.run_ok();
    assert_eq!(
        count_rows(&rig.out_dir()),
        3000,
        "varchar-key parallel keyset exports every row"
    );
    assert!(
        worker_fanout(&rig.out_dir()) >= 2,
        "varchar key must fan out, not collapse"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Parallel keyset on a DECIMAL key must FAIL LOUD at plan time — the keyset cursor
/// cannot advance past a decimal/numeric, so accepting it would fail MID-RUN after a
/// partial write (silent data risk). The planner rejects it up front with an actionable
/// message; this guards that the rejection stays loud + pre-run (never a mid-run crash).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_decimal_key_rejected_loudly_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_dec");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (dkey DECIMAL(20,0) PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g::decimal, g FROM generate_series(1, 3000) g;"
    ))
    .unwrap();
    let err = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: dkey")
        .export_line("parallel: 4")
        .export_line("chunk_size: 500")
        .run_expect_fail();
    assert!(
        err.contains("not a usable keyset key") && err.to_lowercase().contains("decimal"),
        "a decimal keyset key must be rejected loudly at plan time, got: {err}"
    );
    // Nothing was written — the rejection is pre-run, not a mid-run partial.
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Parallel keyset on a BIGINT UNSIGNED key ENTIRELY above i64::MAX (the field
/// unsigned-keyset regime, fast small-N variant of the 10M golden): the boundary
/// literals + paging cursor must round-trip as u64, or rows past i64::MAX are dropped.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_unsigned_above_i64_max_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_u64");
    let mut c = mysql_connect();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT UNSIGNED PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    // 2000 keys all > i64::MAX (9223372036854775807): id = 10^19 + n*10^6.
    c.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 2000) \
         SELECT CAST(10000000000000000000 AS UNSIGNED) + CAST(n AS UNSIGNED) * 1000000, n FROM seq"
    ))
    .unwrap();
    let rig = Rig::mysql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 400");
    rig.run_ok();
    assert_eq!(
        count_rows(&rig.out_dir()),
        2000,
        "every u64 key above i64::MAX exported once (no signed-overflow drop)"
    );
    assert!(
        worker_fanout(&rig.out_dir()) >= 2,
        "unsigned key must fan out"
    );
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

/// Crash after a LATER range commits (range 2 of 4), not just range 0 — resume must
/// still recover every row. Guards the assumption that recovery works regardless of
/// WHICH range's commit the crash follows.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_crash_after_later_range_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_crash2");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let rig = crash_rig(Rig::pg_batch(&format!("public.{table}")));
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:2");
    assert!(
        !crash.status.success(),
        "crash after range 2 must fail run 1"
    );
    rig.run_ok();
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 2000,
        "resume after a later-range crash recovers every row"
    );
    assert_eq!(keys, (1..=2000i64).collect::<BTreeSet<i64>>());
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Bug #2 regression: incremental parallel keyset on a TEXT key CONTAINING backslashes,
/// under a server whose global sql_mode includes NO_BACKSLASH_ESCAPES. The boundary
/// sampler (query_scalar) injects the escaped anchor literal; if its connection does not
/// pin sql_mode like the workers' do, `'p\\...'` mis-parses, a sampled boundary lands
/// below the true anchor, and a worker range re-reads already-exported rows -> cross-run
/// DUP. With the sampler pinned (reuse of MysqlSessionGuard), the union is exact.
/// Sets + restores @@global.sql_mode; the restore runs BEFORE the asserts so a failure
/// never leaves the server dirty.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_incremental_survives_no_backslash_escapes_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_nbs");
    // :3306 GLOBAL flip — mutually exclude with every other shared-batch-server
    // GLOBAL mutator (binlog-compression, governor tmp-storage) and the timing
    // canaries under the SAME lock (r5 bughunt: a per-name lock excluded none).
    let _serial = quiet_window_guard();
    let mut c = mysql_connect();
    let orig: String = c.query_first("SELECT @@global.sql_mode").unwrap().unwrap();
    // The hostile sql_mode must be the SERVER default so rivet's OWN connections inherit
    // it — that needs SUPER / SYSTEM_VARIABLES_ADMIN. Skip where the test user lacks it
    // (the dev stack); this is a real guard on a server the tester controls.
    if c.query_drop("SET GLOBAL sql_mode = CONCAT(@@global.sql_mode, ',NO_BACKSLASH_ESCAPES')")
        .is_err()
    {
        skip_live("setting @@global.sql_mode needs SUPER (unavailable for the test user)");
        return;
    }
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (k VARCHAR(40) PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    c.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    // 1000 text keys each containing a backslash (CHAR(92)): 'p\000001'..'p\001000'.
    c.query_drop(format!(
        "INSERT INTO {table} (k, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 1000) \
         SELECT CONCAT('p', CHAR(92), LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();

    let rig = Rig::mysql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 200");
    rig.run_ok();
    let r1 = count_rows(&rig.out_dir());
    // add 500 more keys ABOVE the anchor, then an incremental run past the backslash anchor.
    c.query_drop(format!(
        "INSERT INTO {table} (k, payload) \
         WITH RECURSIVE seq AS (SELECT 1001 n UNION ALL SELECT n+1 FROM seq WHERE n < 1500) \
         SELECT CONCAT('p', CHAR(92), LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();
    rig.run_ok();
    let r2 = count_rows(&rig.out_dir());

    // Restore the global sql_mode + drop BEFORE asserting.
    let _ = c.query_drop(format!(
        "SET GLOBAL sql_mode = '{}'",
        orig.replace('\'', "''")
    ));
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));

    assert_eq!(r1, 1000, "run 1 exports all 1000 backslash-keyed rows");
    assert_eq!(
        r2, 1500,
        "incremental run past a backslash anchor under NO_BACKSLASH_ESCAPES must add EXACTLY \
         the 500 new keys — not dup (>1500) via a mis-parsed sampler boundary, nor lose (<1500)"
    );
}

/// Least-privilege: a batch parallel-keyset export must succeed with a source user that
/// has ONLY SELECT (+ USAGE on the schema) — NEVER SUPER / CREATE / write. rivet's export
/// does only session-scoped SETs (SET LOCAL / SET SESSION, no privilege) + SELECT, so a
/// read-only extraction role is all a user must grant. A failure here means the export
/// reaches for a privilege it should not.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_works_with_a_select_only_source_user_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_ro");
    let role = format!("ro_{}", &table[table.len().saturating_sub(12)..]).replace('-', "_");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 3000) g;"
    ))
    .unwrap();
    // A least-privilege role: LOGIN + USAGE on the schema + SELECT on the table. Nothing
    // else — the only grants a read-only extractor may assume.
    c.batch_execute(&format!(
        "DROP ROLE IF EXISTS {role};
         CREATE ROLE {role} LOGIN PASSWORD 'ro';
         GRANT USAGE ON SCHEMA public TO {role};
         GRANT SELECT ON public.{table} TO {role};"
    ))
    .unwrap();

    // Run parallel keyset AS the SELECT-only role.
    let ro_url = POSTGRES_URL.replacen("rivet:rivet", &format!("{role}:ro"), 1);
    let rig = parallel_keyset(Rig::pg_batch(&format!("public.{table}")).source_url(&ro_url));
    rig.run_ok();
    let (count, keys, workers) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 3000,
        "a SELECT-only user must fully export via parallel keyset"
    );
    assert_eq!(keys, (1..=3000i64).collect::<BTreeSet<i64>>());
    assert!(
        workers >= 2,
        "fan-out must still happen under a SELECT-only user"
    );

    let _ = c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table}; DROP OWNED BY {role}; DROP ROLE IF EXISTS {role};"
    ));
}

/// THE SEAM the retry guard reads. `decide_export_retry` bails with
/// `BailDuplicateGuard` only when `summary.files_committed > 0`
/// (`src/pipeline/single.rs`), and `record_part` is the sole production site
/// that raises it. Parallel keyset used to `bail!` on a worker error ABOVE that
/// drain, so the guard read ZERO while ranges were already durable — a TRANSIENT
/// worker failure then retried the whole export over parts on disk.
///
/// Why the existing tests could not see it, which is the point of adding this
/// one: `decide_export_retry`'s unit matrix feeds `files_committed` as a
/// parameter (0/2/5) and is correct on every input, while
/// `parallel_keyset_worker_error_aborts_cleanly_postgres` deliberately DELETES
/// the orphan parts before its clean re-run (a vacuous-oracle guard its own
/// comment explains). Neither observes the VALUE one layer hands the other.
/// This test observes exactly that value, at the boundary.
///
/// Measured before the fix: 4 parts on disk, `retry 1/2` and `retry 2/2`, and
/// with a range whose output SHRANK between attempts (a mid-backoff DELETE) the
/// earlier attempt's extra part survived unoverwritten — 751 rows / 750
/// distinct, id 501 duplicated.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_worker_error_still_counts_the_durable_parts_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_wcnt");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let rig = parallel_keyset(Rig::pg_batch(&format!("public.{table}")));
    let out = rig.run_with_env("RIVET_TEST_ERROR_AT", "keyset_parallel_worker:2");
    assert!(
        !out.status.success(),
        "a worker mid-range error must fail the run"
    );

    // The successful ranges DID write parquet — the premise of the whole test.
    let on_disk = files_with_extension(&rig.out_dir(), "parquet").len();
    assert!(
        on_disk > 0,
        "fixture is inert: the surviving workers wrote no parts, so there is \
         nothing for the retry guard to protect"
    );

    // …and the run must SAY so. This is the assertion that goes RED against the
    // pre-fix code: files_committed was 0 with parts already on disk.
    let db = StateDb::next_to_config(&rig.config_path());
    let run_id = db.latest_run_id(rig.export_name());
    let m = db.metrics_row(&run_id);
    assert_eq!(
        m.files_committed,
        Some(on_disk as i64),
        "a failed parallel-keyset run must count the parts it left durable \
         ({on_disk} on disk) — the retry guard reads this and nothing else"
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}

/// #200-1: the SAME durable-parts blind spot, one level deeper — page granularity
/// WITHIN a range. The worker-level template above fails a range at its FIRST page
/// (`keyset_parallel_worker:2`), so the failing range writes nothing and every
/// on-disk part belongs to a COMMITTED range — `files_committed == on_disk` holds
/// even with the bug, because the bug only drops the parts of a range that both
/// (a) wrote pages AND (b) never committed.
///
/// This fixture makes a range do exactly that: `keyset_parallel_worker_midrange:2`
/// fires only after range 2 has already made page 0 durable, so range 2 leaves a
/// parquet on disk yet never reaches its checkpoint commit. Pre-fix those parts
/// lived in the worker's `local_parts`, published to the shared count ONLY at
/// range completion — the mid-range `return` dropped them, so `files_committed`
/// under-counted the debris the retry guard reads. RED before the per-page publish:
/// `files_committed` = the committed ranges only, `on_disk` = those + range 2's
/// page 0.
///
/// Needs ≥2 pages per range so page 0 lands before the mid-range fire: 4000 rows /
/// `parallel: 4` = 1000-row ranges over `chunk_size: 500` = 2 pages each.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_midrange_error_counts_pre_failure_page_parts_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_midr");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 4000) g;
         ANALYZE {table};"
    ))
    .unwrap();
    let rig = parallel_keyset(Rig::pg_batch(&format!("public.{table}")));
    let out = rig.run_with_env("RIVET_TEST_ERROR_AT", "keyset_parallel_worker_midrange:2");
    assert!(
        !out.status.success(),
        "a worker mid-range error must fail the run"
    );

    // The premise: range 2 wrote page 0 to disk BEFORE the mid-range failure, and
    // the committed ranges wrote their pages too. If range 2 wrote nothing this
    // test degenerates into the worker-level one and proves nothing new.
    let per_worker = worker_part_counts(&rig.out_dir());
    let on_disk: usize = per_worker.values().sum();
    assert!(
        per_worker.get("w2").copied().unwrap_or(0) >= 1,
        "fixture is inert for #200-1: range 2 must leave ≥1 durable page BEFORE it \
         fails mid-range, got per-worker parts {per_worker:?}"
    );
    assert!(
        on_disk > per_worker.get("w2").copied().unwrap_or(0),
        "fixture needs committed ranges too, got {per_worker:?}"
    );

    // The seam: files_committed must count EVERY durable part on disk, including
    // range 2's pre-failure page — the retry guard reads this and nothing else.
    // RED pre-fix: files_committed omitted range 2's uncommitted-but-durable page.
    let db = StateDb::next_to_config(&rig.config_path());
    let run_id = db.latest_run_id(rig.export_name());
    let m = db.metrics_row(&run_id);
    assert_eq!(
        m.files_committed,
        Some(on_disk as i64),
        "a mid-range failure must count the pages the failing range already made \
         durable ({on_disk} on disk, per-worker {per_worker:?})"
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}

/// Worker mid-range ERROR propagation (the non-crash path): when one worker's source read
/// errors (connection drop / statement timeout), the run must FAIL CLEANLY — collect the
/// error, bail, and finalize NOTHING (no _SUCCESS, no manifest), never a partial success.
/// A clean re-run then recovers every row.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_worker_error_aborts_cleanly_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_werr");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let rig = parallel_keyset(Rig::pg_batch(&format!("public.{table}")));
    // range 2's worker errors mid-range (Err, not a crash).
    let out = rig.run_with_env("RIVET_TEST_ERROR_AT", "keyset_parallel_worker:2");
    assert!(
        !out.status.success(),
        "a worker mid-range error must FAIL the run"
    );
    // No FALSE success: no _SUCCESS marker, and the manifest (written as a FAILED record
    // so gc_orphans can classify + delete its partial parts) is NOT a Success.
    assert!(
        !rig.out_dir().join("_SUCCESS").exists(),
        "a failed run must not write _SUCCESS"
    );
    let mf = rig.out_dir().join("manifest.json");
    if mf.exists() {
        let m: serde_json::Value = serde_json::from_slice(&std::fs::read(&mf).unwrap()).unwrap();
        let status = m["status"].as_str().unwrap_or("").to_lowercase();
        assert_ne!(
            status, "success",
            "a worker-error abort must leave a Failed/Interrupted manifest, not a Success"
        );
    }
    // ISOLATE the clean re-run: the failed run left ranges 0/1/3 as orphan parquet
    // (~75% of the key space). Reading orphans ∪ rerun would let the union hit 1..=2000
    // even if the clean run DROPPED a range — a vacuous oracle. Delete the orphans first,
    // so the completeness check sees ONLY run 2's output.
    for p in files_with_extension(&rig.out_dir(), "parquet") {
        std::fs::remove_file(&p).unwrap();
    }
    // A clean re-run recovers everything: exact count (no dupe/no orphan inflation) AND
    // the full key set — RED if the clean run drops ANY range (0/1/3 included), not just
    // range 2.
    rig.run_ok();
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 2000,
        "the clean re-run writes exactly 2000 rows (no dupe/orphan)"
    );
    assert_eq!(
        keys,
        (1..=2000i64).collect::<BTreeSet<i64>>(),
        "a clean re-run after a worker-error abort recovers every key"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// CSV row count across every `.csv` part (lines minus the per-file header).
fn csv_row_count(dir: &std::path::Path) -> usize {
    files_with_extension(dir, "csv")
        .iter()
        .map(|p| {
            std::fs::read_to_string(p)
                .map(|s| s.lines().count().saturating_sub(1))
                .unwrap_or(0)
        })
        .sum()
}

/// Parallel keyset writing CSV (not parquet): the text-writer path must fan out and
/// export every row (the runner is format-agnostic, but only parquet was exercised).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_csv_format_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_csv");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 3000) g;"
    ))
    .unwrap();
    let rig = parallel_keyset(Rig::pg_batch(&format!("public.{table}"))).with_format("csv");
    rig.run_ok();
    assert_eq!(
        csv_row_count(&rig.out_dir()),
        3000,
        "parallel keyset CSV exports every row"
    );
    assert!(
        worker_fanout(&rig.out_dir()) >= 2,
        "CSV parallel keyset must fan out (workers keyed in the part name like parquet)"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// partition_by + keyset is REJECTED LOUDLY at plan time: partitioning rewrites the
/// query into a subquery, which defeats the `table:` index verification keyset needs.
/// Guards that the incompatibility stays a pre-run error (not a silent wrong-plan).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_partition_by_rejected_loudly_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_part");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, grp INT NOT NULL);
         INSERT INTO {table} SELECT g, g % 4 FROM generate_series(1, 3000) g;"
    ))
    .unwrap();
    let err = parallel_keyset(Rig::pg_batch(&format!("public.{table}")))
        .export_line("partition_by: grp")
        .run_expect_fail();
    assert!(
        err.contains("partition_by is not compatible with chunk_by_key"),
        "partition_by + keyset must be rejected loudly at plan time, got: {err}"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Concurrency stress: MORE workers (parallel: 8) + a small chunk_size so each range
/// pages many times, exercising the shared Mutex/Atomic merge (parts, checksums,
/// range_max, rows) under heavier contention than the 4-worker parity tests.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_eight_workers_concurrency_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_conc");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 8000) g;"
    ))
    .unwrap();
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 8")
        .export_line("chunk_size: 250");
    rig.run_ok();
    let (count, keys, workers) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 8000,
        "8-worker keyset merges every row once (no race drop/dupe)"
    );
    assert_eq!(keys, (1..=8000i64).collect::<BTreeSet<i64>>());
    assert!(workers >= 4, "8 workers must fan out widely, got {workers}");
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Resume with a CHANGED parallel N: crash a `parallel: 4` checkpoint run, then resume
/// with `parallel: 8` (same export, same state db + out dir). The resume must RELOAD the
/// crashed run's persisted keyset_range rows (the original 4 ranges) rather than re-sample
/// with the new N, and recover every row. Built without the Rig — two configs sharing one
/// tempdir (hence one `.rivet_state.db` + out dir) is the only way to vary N across a
/// crash/resume pair.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_resume_with_changed_worker_count_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_resn");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    // Through the rig: the old hand-built config here justified itself with
    // "the only way to vary N across a crash/resume pair" — false since
    // replace_export_line exists for exactly that shape (and the r2 bughunt
    // flagged this file as a live ratchet bypass: format!-built yaml +
    // fs::write scored zero bespoke sites).
    let mut rig = Rig::pg_batch(&format!("public.{table}"))
        .export_named("pk_resn")
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 200")
        .dest_path(out.clone());
    // Run 1: parallel:4, crash after range 1 commits (durable in keyset_range).
    let crash = rig.run_args_env(
        &[],
        &[("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:1")],
    );
    assert!(!crash.status.success(), "the crash run must fail");
    // Run 2: parallel:8 — resume must reuse the crashed run's 4 persisted ranges.
    rig.replace_export_line("parallel:", "parallel: 8");
    let resume = rig.run_args(&[]);
    assert!(
        resume.status.success(),
        "resume with a changed parallel N must succeed: {}",
        String::from_utf8_lossy(&resume.stderr)
    );
    let (count, keys, _) = id_set_and_fanout(&out);
    assert_eq!(
        count, 2000,
        "resume with a changed worker count recovers every row"
    );
    assert_eq!(keys, (1..=2000i64).collect::<BTreeSet<i64>>());
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// ADR-0029, the ORIGINALLY-REPORTED defect: a parallel-keyset run that FAILS
/// must record in its Failed manifest the schema fingerprint it OBSERVED, not
/// the stale baseline the state DB held before the run.
///
/// This runner records durable parts ABOVE its error bail and used to feed the
/// whole `CommitLedger` BELOW it, so `finalize_export_records` — the failure half
/// of the ADR-0028 seam, whose entire job is "a Failed manifest must describe its
/// debris honestly" — received an EMPTY ledger. Parallel keyset is the worst case
/// because it has no direct `summary.schema_fingerprint` assignment at all (its
/// three siblings set one from their own `shared_fingerprint`), so the ledger is
/// its ONLY path. ADR-0029's split fixes it: the schema is an OBSERVATION — it
/// describes what the runner READ and carries no coverage obligation — so it is
/// fed above the bail while the Form-B checksums stay commit-gated below, the
/// asymmetry `keyset.rs` states at its part-publishing site, now expressed in the
/// ledger's shape instead of left to feed order.
///
/// THE FIXTURE IS THE WHOLE TEST, and two earlier drafts of it proved nothing —
/// both measured GREEN with the fix reverted:
///
/// * "the Failed manifest carries a real fingerprint, not the `unavailable`
///   placeholder" is fix-INVARIANT: the fallback `finalize_manifest` uses when the
///   ledger is empty is `get_stored_schema`, and `capture_open_forensics` stores
///   the schema (`store_schema_if_absent`) at run OPEN — so the fallback answers
///   with the same schema the run observed.
/// * making the source DRIFT between two runs is not enough either, if the second
///   run gets a FRESH `Rig`: the rig owns its tempdir, so it owns its state DB,
///   and a new rig has no baseline to be stale. The failing run stored the drifted
///   schema itself at open and the fallback answered with it.
///
/// So the subject must be ONE rig (one state DB) run TWICE across a schema change:
///
///   1. same rig, clean run over `(id, payload)` — manifest gives fingerprint A,
///      and A becomes this export's stored baseline;
///   2. `ALTER TABLE ADD COLUMN` — the source is now schema B;
///   3. a separate rig, clean run over the ALTERED table: an independent oracle
///      for fingerprint B;
///   4. the SAME rig again, failed by a worker error. Its stored baseline is still
///      A (`store_schema_if_absent` will not overwrite, and the drift gate that
///      would refresh it does not run on a failure), so a manifest carrying A is
///      the fallback and a manifest carrying B is the observation.
///
/// RED against feeding `note_schema` below the bail: the Failed manifest carries
/// A while its durable parquet is schema B — actively wrong forensics about data
/// sitting on the prefix.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_failed_parallel_keyset_run_records_the_observed_schema_fingerprint_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_fpfail");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();

    // (1) The SUBJECT rig — reused for run 4, so its state DB carries the baseline.
    let subject = parallel_keyset(Rig::pg_batch(&format!("public.{table}")));
    let r1 = subject.run();
    assert!(
        r1.status.success(),
        "baseline run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r1.stderr)
    );
    let fp_a = manifest_schema_fingerprint(&subject.out_dir());

    // (2) The source moves to schema B.
    c.batch_execute(&format!("ALTER TABLE {table} ADD COLUMN note TEXT;"))
        .unwrap();

    // (3) Independent oracle for B: its own rig, its own state DB, clean run.
    let oracle = parallel_keyset(Rig::pg_batch(&format!("public.{table}")));
    let r2 = oracle.run();
    assert!(
        r2.status.success(),
        "oracle run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );
    let fp_b = manifest_schema_fingerprint(&oracle.out_dir());
    assert_ne!(
        fp_a, fp_b,
        "fixture is inert: adding a column did not change the fingerprint, so the stale \
         baseline and the observation are indistinguishable and this test grades nothing"
    );

    // (4) Same rig, same state DB (baseline still A), source now B, failed by a
    // worker error after other ranges have already committed parts.
    let out = subject.run_with_env("RIVET_TEST_ERROR_AT", "keyset_parallel_worker:2");
    assert!(
        !out.status.success(),
        "a worker error must fail the run — otherwise this is not the failure path"
    );
    assert!(
        !files_with_extension(&subject.out_dir(), "parquet").is_empty(),
        "fixture is inert: the surviving ranges wrote no parts, so the Failed manifest \
         has no debris to describe"
    );

    let recorded = manifest_schema_fingerprint(&subject.out_dir());
    assert_eq!(
        recorded, fp_b,
        "a FAILED parallel-keyset run must pin the fingerprint it OBSERVED ({fp_b}); it \
         recorded {recorded}, and the stale pre-ALTER baseline is {fp_a} — a Failed manifest \
         describing durable parquet with the wrong schema is actively wrong forensics"
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}

/// `manifest.json`'s recorded schema fingerprint, for either a Success or a
/// Failed manifest (ADR-0012 M7 writes the manifest either way; only `_SUCCESS`
/// is success-only).
fn manifest_schema_fingerprint(dir: &std::path::Path) -> String {
    let text = std::fs::read_to_string(dir.join("manifest.json"))
        .unwrap_or_else(|e| panic!("read {}/manifest.json: {e}", dir.display()));
    let v: serde_json::Value = serde_json::from_str(&text).expect("parse manifest.json");
    v["schema_fingerprint"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest.json has no schema_fingerprint: {text}"))
        .to_string()
}

/// Seeded PG table of `n` ids for the runner-shape resume tests.
fn pg_shape_table(prefix: &str, n: usize) -> String {
    let table = unique_name(prefix);
    pg_connect()
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
             INSERT INTO {table} SELECT g, g FROM generate_series(1, {n}) g;"
        ))
        .unwrap();
    table
}

/// Every row once in the manifest and in the parts it declares (orphans of the failed run excluded).
fn assert_every_row_once(rig: &Rig, n: usize) {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(rig.out_dir().join("manifest.json")).unwrap())
            .expect("destination manifest.json");
    let declared = |sql: &str| duckdb_declared_dir_scalar(&rig.out_dir(), sql);
    assert_eq!(
        (
            manifest["row_count"].as_i64(),
            declared("count(*)"),
            declared("count(DISTINCT id)"),
            declared("min(id)"),
            declared("max(id)"),
        ),
        (Some(n as i64), n as i64, n as i64, 1, n as i64),
        "manifest rows, declared rows, distinct ids, id range"
    );
}

/// A parallel keyset run that fails part-way, resumed after `parallel:` was lowered to 1:
/// the sequential runner must not continue a checkpoint the parallel runner wrote.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_parallel_keyset_failure_resumed_sequentially_exports_every_row_once() {
    require_alive(LiveService::Postgres);
    let table = pg_shape_table("pk_shape_ps", 2000);
    let rig = crash_rig(Rig::pg_batch(&format!("public.{table}")));
    rig.run_ok();
    let _ = std::fs::remove_dir_all(rig.out_dir());
    let failed = rig.run_with_env("RIVET_TEST_ERROR_AT", "keyset_parallel_worker:3");
    assert!(!failed.status.success(), "the worker error fails run 2");
    let rig = rig.restage(
        "chunked",
        &[
            "chunk_by_key: id",
            "parallel: 1",
            "chunk_checkpoint: true",
            "chunk_size: 200",
        ],
    );
    rig.run_ok();
    assert_every_row_once(&rig, 2000);
    let _ = pg_connect().execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// A sequential keyset run that crashes part-way, resumed after `parallel:` was raised:
/// the parallel runner must not adopt the sequential run's pages beside its own full pass.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_sequential_keyset_crash_resumed_in_parallel_exports_every_row_once() {
    require_alive(LiveService::Postgres);
    let table = pg_shape_table("pk_shape_sp", 2000);
    let rig = Rig::pg_batch(&format!("public.{table}")).restage(
        "chunked",
        &[
            "chunk_by_key: id",
            "parallel: 1",
            "chunk_checkpoint: true",
            "chunk_size: 200",
        ],
    );
    let crashed = rig.run_with_env("RIVET_TEST_PANIC_AT", "after_keyset_page:2");
    assert!(!crashed.status.success(), "the injected panic fails run 1");
    let rig = rig.restage(
        "chunked",
        &[
            "chunk_by_key: id",
            "parallel: 4",
            "chunk_checkpoint: true",
            "chunk_size: 200",
        ],
    );
    rig.run_ok();
    assert_every_row_once(&rig, 2000);
    let _ = pg_connect().execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Tuples PostgreSQL read from `table` and its indexes, once the shared counts settle.
fn pg_tuples_read(c: &mut postgres::Client, table: &str) -> i64 {
    let read = |c: &mut postgres::Client| -> i64 {
        c.query_one(
            "SELECT (COALESCE(t.seq_tup_read, 0) + COALESCE((SELECT sum(i.idx_tup_read) \
             FROM pg_stat_user_indexes i WHERE i.relid = t.relid), 0))::bigint \
             FROM pg_stat_user_tables t WHERE t.relname = $1",
            &[&table],
        )
        .map(|r| r.get(0))
        .unwrap()
    };
    let mut last = read(c);
    for _ in 0..20 {
        std::thread::sleep(std::time::Duration::from_millis(500));
        let now = read(c);
        if now == last {
            return now;
        }
        last = now;
    }
    last
}

/// Parallel keyset over a composite uuid + text key reads each row about once: the range sampler seeks, it never counts or skips through the table.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_sampler_reads_a_bounded_number_of_rows_postgres() {
    require_alive(LiveService::Postgres);
    const ROWS: i64 = 100_000;
    let table = unique_name("pk_bounded");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id UUID NOT NULL, kind VARCHAR(16) NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (id, kind));
         INSERT INTO {table} SELECT md5(g::text)::uuid, 'k' || (g % 3), repeat('x', 40) FROM generate_series(1, {ROWS}) g;"
    ))
    .unwrap();
    pg_catalog_says_rows(&mut c, &table, 20_000_000);
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: [id, kind]")
        .export_line("parallel: 4")
        .export_line("chunk_size: 10000")
        .no_oracle("the test counts reads of the source table; the oracle's own read-back would be counted");
    let before = pg_tuples_read(&mut c, &table);
    rig.run_ok();
    let read = pg_tuples_read(&mut c, &table) - before;
    let out = rig.out_dir();
    let keys: std::collections::HashSet<_> = dir_parquet_display(&out, "id")
        .into_iter()
        .zip(dir_parquet_display(&out, "kind"))
        .collect();
    assert_eq!(
        (
            rows_per_range(&out).values().sum::<usize>(),
            keys.len(),
            worker_fanout(&out),
        ),
        (ROWS as usize, ROWS as usize, 4),
        "declared rows, distinct keys, workers"
    );
    assert!(
        read <= ROWS + ROWS / 20,
        "the run read {read} tuples for {ROWS} rows: the range sampler scans the table"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Rows each `pk_w{range}` range delivered, by range index, read from the parts themselves.
fn rows_per_range(dir: &std::path::Path) -> std::collections::BTreeMap<usize, usize> {
    let mut spread = std::collections::BTreeMap::new();
    for part in declared_parquet_parts(dir) {
        let name = part
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let range = name
            .split("_pk_w")
            .nth(1)
            .and_then(|s| s.split('_').next())
            .and_then(|r| r.parse().ok())
            .unwrap_or_else(|| panic!("not a keyset range part: {name}"));
        *spread.entry(range).or_insert(0) += parquet_rows(&part);
    }
    spread
}

/// A key with 90% of its rows in 5% of its span: every row once, in non-empty ranges that follow the key's value, not its row count.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_skewed_parallel_keyset_key_reads_every_row_once_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_skew");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 18000) g;
         INSERT INTO {table} SELECT 18000 + g * 171, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    pg_catalog_says_rows(&mut c, &table, 20_000_000);
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000");
    rig.run_ok();
    let ids = dir_parquet_ids(&rig.out_dir());
    let expected: Vec<i64> = (1..=18_000)
        .chain((1..=2_000).map(|g| 18_000 + g * 171))
        .collect();
    assert_eq!(
        (ids.len(), dir_parquet_id_set(&rig.out_dir())),
        (expected.len(), expected.into_iter().collect()),
        "every source id delivered once"
    );
    let spread = rows_per_range(&rig.out_dir());
    eprintln!("SPREAD {table}: {spread:?}");
    let spread: Vec<usize> = spread.into_values().collect();
    let heaviest = spread.iter().max().copied().unwrap_or_default();
    assert_eq!(
        spread.len(),
        64,
        "16 key-space ranges per worker: {spread:?}"
    );
    assert!(
        heaviest * spread.len() <= 2 * 20_000,
        "the recut keeps the heaviest range within twice the mean ({heaviest} rows over {} ranges): {spread:?}",
        spread.len()
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// A uuid key, which PostgreSQL has no MAX for, takes the incremental ceiling by a seek and re-reads only the keys past the anchor.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_incremental_uuid_key_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_inc_uuid");
    let mut c = pg_connect();
    let seed = |lo: i64, hi: i64| {
        format!(
            "INSERT INTO {table} SELECT lpad(to_hex(g), 32, '0')::uuid, g \
             FROM generate_series({lo}, {hi}) g"
        )
    };
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (k UUID PRIMARY KEY, id BIGINT NOT NULL);
         {};",
        seed(1, 1000)
    ))
    .unwrap();
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 200");
    let more = seed(1001, 1500);
    assert_parallel_incremental(rig, || {
        pg_connect().batch_execute(&more).unwrap();
    });
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// A datetimeoffset key keeps the run: every row once, whatever text the sampler reads its split points as.
#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn a_parallel_keyset_datetimeoffset_key_keeps_the_run_mssql() {
    require_alive(LiveService::Mssql);
    let table = unique_name("pk_dto_ms");
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (k DATETIMEOFFSET(6) NOT NULL PRIMARY KEY, id BIGINT NOT NULL)"
    ));
    mssql_exec(&format!(
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 1000) \
         INSERT INTO {table} (k, id) \
         SELECT DATEADD(minute, n, CAST('2024-01-01 00:00:00 +00:00' AS DATETIMEOFFSET)), n FROM nums \
         OPTION (MAXRECURSION 0)"
    ));
    let rig = Rig::mssql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("parallel: 4")
        .export_line("chunk_size: 200");
    let out = rig.run_args(&[]);
    assert!(
        out.status.success(),
        "a datetimeoffset keyset key stops the run:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        (count, keys),
        (1000, (1..=1000i64).collect::<BTreeSet<i64>>()),
        "every source id delivered once"
    );
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
}

/// The sparse-key fixture of dev/parallel_keyset/results.md Finding 1: 95% of the rows dense at the bottom, 5% spread out to 500M.
const SPARSE_DENSE: i64 = 19_000;

/// The ids of the sparse-key fixture.
fn sparse_key_ids() -> BTreeSet<i64> {
    (1..=SPARSE_DENSE)
        .chain((1..=1_000).map(|g| SPARSE_DENSE + g * 500_000))
        .collect()
}

/// Rows per range of a parallel keyset run over the sparse-key fixture, after checking every id arrived once.
fn sparse_key_spread(rig: Rig) -> Vec<usize> {
    let rig = rig
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 2000");
    rig.run_ok();
    let (count, ids, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        (count, ids),
        (20_000, sparse_key_ids()),
        "every source id once"
    );
    let spread = rows_per_range(&rig.out_dir());
    eprintln!("SPREAD {}: {spread:?}", rig.export_name());
    spread.into_values().collect()
}

/// A sparse integer key takes key-space ranges whatever the catalog estimate says: several per worker, balanced by the density recut.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_sparse_parallel_keyset_key_takes_key_space_ranges_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_sparse");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, {SPARSE_DENSE}) g;
         INSERT INTO {table} SELECT {SPARSE_DENSE} + g * 500000, g FROM generate_series(1, 1000) g;
         ANALYZE {table};"
    ))
    .unwrap();
    let qualified = format!("public.{table}");
    let above = sparse_key_spread(Rig::pg_batch(&qualified));
    assert!(
        (5..=64).contains(&above.len()),
        "key-space ranges, not four row percentiles, at most 16 per worker: {above:?}"
    );
    let (total, heaviest) = (
        above.iter().sum::<usize>(),
        above.iter().max().copied().unwrap_or(0),
    );
    assert!(
        heaviest * above.len() <= 2 * total,
        "the density recut spreads the dense stretch: {above:?}"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// The sparse-key fixture as a MySQL table; `options` ends its `CREATE TABLE`.
fn mysql_sparse_table(c: &mut mysql::PooledConn, table: &str, options: &str) {
    use mysql::prelude::Queryable;
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, payload INT NOT NULL) {options}"
    ))
    .unwrap();
    c.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {SPARSE_DENSE}) \
         SELECT n, n FROM seq"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 1000) \
         SELECT {SPARSE_DENSE} + n * 500000, n FROM seq"
    ))
    .unwrap();
}

/// The MySQL twin: an analyzed table and the same table the catalog puts at 20M rows both take key-space ranges.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn a_sparse_parallel_keyset_key_takes_key_space_ranges_mysql() {
    use mysql::prelude::Queryable;
    require_alive(LiveService::Mysql);
    let (table, large) = (unique_name("pk_sparse_my"), unique_name("pk_sparse_my_l"));
    let mut c = mysql_connect();
    mysql_sparse_table(&mut c, &table, "");
    c.query_drop(format!("ANALYZE TABLE {table}")).unwrap();
    mysql_sparse_table(&mut c, &large, "STATS_PERSISTENT=1 STATS_AUTO_RECALC=0");
    mysql_catalog_says_rows(&large, 20_000_000);
    let small = sparse_key_spread(Rig::mysql_batch(&table));
    assert!(
        (5..=64).contains(&small.len()),
        "key-space ranges under the limit too: {small:?}"
    );
    let above = sparse_key_spread(Rig::mysql_batch(&large));
    assert!(
        (5..=64).contains(&above.len()),
        "key-space ranges, not four row percentiles, at most 16 per worker: {above:?}"
    );
    let (total, heaviest) = (
        above.iter().sum::<usize>(),
        above.iter().max().copied().unwrap_or(0),
    );
    assert!(
        heaviest * above.len() <= 2 * total,
        "the density recut spreads the dense stretch: {above:?}"
    );
    for t in [table, large] {
        let _ = c.query_drop(format!("DROP TABLE IF EXISTS {t}"));
    }
}

/// Rows of the statement-timeout fixtures: a read that evaluates every row costs `TIMEOUT_ROWS` x 2 ms, well over the 1 s budget.
const TIMEOUT_ROWS: i64 = 2_000;

/// A `chunk_by_key: id` run of four workers under a `seconds` statement budget (0 lifts it).
fn timeout_rig(rig: Rig, seconds: u64) -> Rig {
    timeout_rig_by(rig, seconds, "id")
}

/// `timeout_rig` over the key column `key`.
fn timeout_rig_by(rig: Rig, seconds: u64, key: &str) -> Rig {
    rig.mode("chunked")
        .export_line(&format!("chunk_by_key: {key}"))
        .export_line("parallel: 4")
        .export_line("chunk_size: 100")
        .export_line(&format!(
            "tuning: {{statement_timeout_s: {seconds}, max_retries: 0}}"
        ))
}

/// Every generated id once, over at least four workers, from the delivered parts.
fn assert_timeout_fixture_delivered(rig: &Rig, what: &str) {
    let (count, ids, workers) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        (count, ids == (1..=TIMEOUT_ROWS).collect::<BTreeSet<i64>>()),
        (TIMEOUT_ROWS as usize, true),
        "{what}: rows, and ids equal the generated 1..=N"
    );
    assert!(workers >= 4, "{what}: {workers} workers");
}

/// A table `(id BIGINT PRIMARY KEY, payload INT)` of `TIMEOUT_ROWS` rows whose rows cost a reader that is not its owner `sleep_s` each to see, and that reader's URL.
fn pg_slow_to_read_table(c: &mut postgres::Client, table: &str, visible: &str) -> Reader {
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, {TIMEOUT_ROWS}) g;
         ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;
         CREATE POLICY slow_to_read ON {table} FOR SELECT USING ({visible});"
    ))
    .unwrap();
    SqlEngine::Pg.reader(table)
}

/// The sampler's COUNT of a text key (no axis) on a table the catalog puts under 10M rows is cancelled at the budget and fails the run by code; with no budget the same run delivers every row.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_percentile_sampler_read_obeys_the_statement_timeout_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_sto");
    let mut c = pg_connect();
    let reader = pg_slow_to_read_table(&mut c, &table, "pg_sleep(0.002) IS NOT NULL");
    c.batch_execute(&format!(
        "ALTER TABLE {table} ADD COLUMN k TEXT GENERATED ALWAYS AS ('k' || lpad(id::text, 6, '0')) STORED NOT NULL;
         CREATE UNIQUE INDEX {table}_k ON {table} (k);"
    ))
    .unwrap();
    pg_catalog_says_rows(&mut c, &table, TIMEOUT_ROWS);
    let out = timeout_rig_by(Rig::pg_batch(&table).source_url(reader.url()), 1, "k").run();
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_STATEMENT_TIMEOUT", 1));
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(
        said.contains("tuning.statement_timeout_s"),
        "the refusal names the setting: {said}"
    );
    let unbounded = timeout_rig_by(Rig::pg_batch(&table).source_url(reader.url()), 0, "k");
    unbounded.run_ok();
    assert_timeout_fixture_delivered(&unbounded, "no budget");
    drop(reader);
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// A seek that scans past rows the reader cannot see is cancelled at the budget: the run says it could not seek, naming the setting.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_sampler_seek_that_scans_is_cancelled_by_the_statement_timeout_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_sto_gap");
    let mut c = pg_connect();
    let half = TIMEOUT_ROWS / 2;
    let reader = pg_slow_to_read_table(
        &mut c,
        &table,
        &format!("id <= {half} OR pg_sleep(0.004) IS NULL"),
    );
    pg_catalog_says_rows(&mut c, &table, 20_000_000);
    let said = timeout_rig(Rig::pg_batch(&table).source_url(reader.url()), 1)
        .a_failed_run_may_leave(
            &[
                Leftover::OrphanPart,
                Leftover::FileLog,
                Leftover::ObservedSchema,
                Leftover::ChunkCheckpoint,
            ],
            "the export of the rows past the cancelled seek fails after the ranges before it were written and checkpointed",
        )
        .run_expect_fail();
    assert!(
        said.contains("cannot seek the key to a split point")
            && said.contains("tuning.statement_timeout_s"),
        "the cancelled seek is named with its setting: {said}"
    );
    drop(reader);
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// A key the seeks reach in one row keeps its run under the same budget: every id once over four workers, no lost split.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_seek_sampler_under_the_statement_timeout_keeps_its_run_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_sto_ix");
    let mut c = pg_connect();
    let reader = pg_slow_to_read_table(&mut c, &table, "pg_sleep(0.002) IS NOT NULL");
    pg_catalog_says_rows(&mut c, &table, 20_000_000);
    let bounded = timeout_rig(Rig::pg_batch(&table).source_url(reader.url()), 1);
    let said = bounded.run_ok_capture();
    assert!(
        !said.contains("cannot seek the key to a split point"),
        "no seek was lost: {said}"
    );
    assert_timeout_fixture_delivered(&bounded, "1 s budget");
    let unbounded = timeout_rig(Rig::pg_batch(&table).source_url(reader.url()), 0);
    unbounded.run_ok();
    assert_timeout_fixture_delivered(&unbounded, "no budget");
    drop(reader);
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// MySQL: the sampler under a 1 s `max_execution_time` budget delivers every id once over four workers.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn a_sampler_under_the_statement_timeout_keeps_its_run_mysql() {
    use mysql::prelude::Queryable;
    require_alive(LiveService::Mysql);
    let table = unique_name("pk_sto_my");
    let mut c = mysql_connect();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    c.query_drop("SET SESSION cte_max_recursion_depth = 5000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {TIMEOUT_ROWS}) \
         SELECT n, n FROM seq"
    ))
    .unwrap();
    c.query_drop(format!("ANALYZE TABLE {table}")).unwrap();
    let bounded = timeout_rig(Rig::mysql_batch(&table), 1);
    let said = bounded.run_ok_capture();
    assert!(
        !said.contains("cannot seek the key to a split point"),
        "no seek was lost: {said}"
    );
    assert_timeout_fixture_delivered(&bounded, "1 s budget");
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

/// `Source::query_scalar_within` cancels a read that outlasts its budget with the coded statement-timeout error, and with no budget it returns the read's value.
fn assert_scalar_within_obeys_its_budget(source: &mut dyn rivet::source::Source, slow_count: &str) {
    let started = std::time::Instant::now();
    let err = source
        .query_scalar_within(slow_count, 1)
        .expect_err("a read that takes seconds is cancelled at 1 s");
    assert_eq!(
        rivet::error::error_code(&err),
        Some("RIVET_SOURCE_STATEMENT_TIMEOUT"),
        "{err:#}"
    );
    assert!(
        format!("{err:#}").contains("tuning.statement_timeout_s"),
        "names the setting: {err:#}"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(3),
        "cancelled at its budget, not at the end of the read: {:?}",
        started.elapsed()
    );
    assert_eq!(
        source.query_scalar_within("SELECT 41 + 1", 1).unwrap(),
        Some("42".to_string()),
        "the connection serves the next read after a cancelled one"
    );
    assert_eq!(
        source.query_scalar_within(slow_count, 0).unwrap(),
        Some(TIMEOUT_ROWS.to_string()),
        "no budget: the read finishes"
    );
}

/// PostgreSQL: the bounded scalar read is cancelled by `SET LOCAL statement_timeout` and leaves the connection usable.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_bounded_scalar_read_is_cancelled_at_its_budget_postgres() {
    require_alive(LiveService::Postgres);
    let mut source = rivet::source::postgres::PostgresSource::connect(POSTGRES_URL).unwrap();
    assert_scalar_within_obeys_its_budget(
        &mut source,
        &format!(
            "SELECT COUNT(*) FROM generate_series(1, {TIMEOUT_ROWS}) g WHERE pg_sleep(0.002) IS NOT NULL"
        ),
    );
}

/// MySQL: the bounded scalar read is cancelled by `max_execution_time` and leaves the session as it was.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn a_bounded_scalar_read_is_cancelled_at_its_budget_mysql() {
    use mysql::prelude::Queryable;
    require_alive(LiveService::Mysql);
    let table = unique_name("pk_sto_sc");
    let mut c = mysql_connect();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY)"
    ))
    .unwrap();
    c.query_drop("SET SESSION cte_max_recursion_depth = 5000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {TIMEOUT_ROWS}) \
         SELECT n FROM seq"
    ))
    .unwrap();
    let mut source = rivet::source::mysql::MysqlSource::connect(MYSQL_URL).unwrap();
    assert_scalar_within_obeys_its_budget(
        &mut source,
        &format!("SELECT COUNT(*) FROM {table} WHERE SLEEP(0.002) = 0"),
    );
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

/// Dense keys of a skew fixture: a block at the bottom of the key axis, the nil uuid among them.
const SKEW_DENSE: i64 = 180_000;
/// Sparse keys of a skew fixture, spread over the whole key axis above the dense block.
const SKEW_SPARSE: i64 = 20_000;
/// Rows of a skew fixture.
const SKEW_ROWS: usize = (SKEW_DENSE + SKEW_SPARSE) as usize;

/// The integer keys of the skew fixture.
fn skew_int_keys() -> BTreeSet<i64> {
    (1..=SKEW_DENSE)
        .chain((1..=SKEW_SPARSE).map(|g| SKEW_DENSE + g * 500_000))
        .collect()
}

/// The uuid keys of the skew fixture as canonical lowercase text.
fn skew_uuid_keys() -> BTreeSet<String> {
    (0..SKEW_DENSE as u128)
        .chain((1..=SKEW_SPARSE as u128).map(|g| g << 96))
        .map(|v| uuid::Uuid::from_u128(v).hyphenated().to_string())
        .collect()
}

/// Runs a skew export at info level; returns what it logged and the rows per range.
fn skew_run(rig: &Rig) -> (String, std::collections::BTreeMap<usize, usize>) {
    let out = rig.run_args_env(&[], &[("RUST_LOG", "info")]);
    let said = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "skewed keyset run must exit 0:\n{said}"
    );
    (said, rows_per_range(&rig.out_dir()))
}

/// The seek path with a density recut that balanced, not the percentile path nor an unbalanced recut.
fn assert_seek_recut(said: &str) {
    assert!(
        said.contains("parallel keyset path: seek+recut (") && !said.contains("not balanced"),
        "a skewed key takes the seek+recut path:\n{said}"
    );
}

/// The row-percentile path, which is row-balanced by construction.
fn assert_percentile(said: &str) {
    assert!(
        said.contains("parallel keyset path: percentile ("),
        "a skewed key under the row limit takes the percentile path:\n{said}"
    );
}

/// The heaviest range holds at most twice the mean range.
fn assert_within_twice_the_mean(spread: &std::collections::BTreeMap<usize, usize>) {
    let total: usize = spread.values().sum();
    let heaviest = spread.values().max().copied().unwrap_or_default();
    assert!(
        spread.len() >= 4 && heaviest * spread.len() <= 2 * total,
        "the heaviest range ({heaviest} rows) is over twice the mean over {} ranges: {spread:?}",
        spread.len()
    );
}

/// Every integer-keyed skew row lands once.
fn assert_skew_int_delivered(dir: &std::path::Path) {
    assert_eq!(
        (count_rows(dir), dir_parquet_id_set(dir)),
        (SKEW_ROWS, skew_int_keys()),
        "every source id once"
    );
}

/// Every uuid-keyed skew row lands once, the nil uuid among them.
fn assert_skew_uuid_delivered(dir: &std::path::Path) {
    let ids: Vec<String> = dir_parquet_display(dir, "id")
        .into_iter()
        .flatten()
        .map(|id| id.replace('-', ""))
        .collect();
    let distinct: BTreeSet<String> = ids.iter().cloned().collect();
    let want: BTreeSet<String> = skew_uuid_keys()
        .iter()
        .map(|k| k.replace('-', ""))
        .collect();
    assert_eq!(
        (ids.len(), distinct),
        (SKEW_ROWS, want),
        "every source uuid once"
    );
}

/// SQL Server's uuids come from NEWID for the sparse part, so only counts and the nil uuid are fixed.
fn assert_skew_mssql_uuid_delivered(dir: &std::path::Path, col: &str) {
    let ids: Vec<String> = dir_parquet_display(dir, col)
        .into_iter()
        .flatten()
        .map(|id| id.replace('-', ""))
        .collect();
    let distinct: BTreeSet<String> = ids.iter().cloned().collect();
    assert_eq!(
        (
            ids.len(),
            distinct.len(),
            distinct.contains(&"0".repeat(32))
        ),
        (SKEW_ROWS, SKEW_ROWS, true),
        "every source uuid once, the nil uuid among them"
    );
}

/// The integer skew fixture on MySQL: `options` ends its `CREATE TABLE`.
fn mysql_skew_int(c: &mut mysql::PooledConn, table: &str, options: &str) {
    use mysql::prelude::Queryable;
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, payload INT NOT NULL) {options}"
    ))
    .unwrap();
    c.query_drop(format!("SET SESSION cte_max_recursion_depth = {SKEW_ROWS}"))
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n + 1 FROM seq WHERE n < {SKEW_DENSE}) \
         SELECT n, n FROM seq"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n + 1 FROM seq WHERE n < {SKEW_SPARSE}) \
         SELECT {SKEW_DENSE} + n * 500000, n FROM seq"
    ))
    .unwrap();
}

/// The uuid skew fixture on MySQL as CHAR(36): `options` ends its `CREATE TABLE`.
fn mysql_skew_uuid(c: &mut mysql::PooledConn, table: &str, options: &str) {
    use mysql::prelude::Queryable;
    let last = SKEW_DENSE - 1;
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id CHAR(36) NOT NULL PRIMARY KEY, payload INT NOT NULL) {options}"
    ))
    .unwrap();
    c.query_drop(format!("SET SESSION cte_max_recursion_depth = {SKEW_ROWS}"))
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 0 n UNION ALL SELECT n + 1 FROM seq WHERE n < {last}), \
         hx AS (SELECT LPAD(HEX(n), 32, '0') h, n FROM seq) \
         SELECT LOWER(CONCAT_WS('-', SUBSTR(h,1,8), SUBSTR(h,9,4), SUBSTR(h,13,4), SUBSTR(h,17,4), SUBSTR(h,21,12))), n FROM hx"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n + 1 FROM seq WHERE n < {SKEW_SPARSE}), \
         hx AS (SELECT CONCAT(LPAD(HEX(n), 8, '0'), REPEAT('0', 24)) h, n FROM seq) \
         SELECT LOWER(CONCAT_WS('-', SUBSTR(h,1,8), SUBSTR(h,9,4), SUBSTR(h,13,4), SUBSTR(h,17,4), SUBSTR(h,21,12))), n FROM hx"
    ))
    .unwrap();
}

/// A skewed integer key (a dense block and a sparse tail) on PostgreSQL is recut by density: every row once, balanced, and no COUNT(*) read.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_skewed_integer_keyset_is_recut_by_density_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_skew_int");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, {SKEW_DENSE}) g;
         INSERT INTO {table} SELECT {SKEW_DENSE} + g::bigint * 500000, g FROM generate_series(1, {SKEW_SPARSE}) g;"
    ))
    .unwrap();
    pg_catalog_says_rows(&mut c, &table, 20_000_000);
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000")
        .no_oracle("the test counts reads of the source table; the oracle's own read-back would be counted");
    let before = pg_tuples_read(&mut c, &table);
    let (said, spread) = skew_run(&rig);
    let read = pg_tuples_read(&mut c, &table) - before;
    assert_seek_recut(&said);
    assert_skew_int_delivered(&rig.out_dir());
    assert_within_twice_the_mean(&spread);
    assert!(
        read <= SKEW_ROWS as i64 * 6 / 5,
        "the run read {read} tuples for {SKEW_ROWS} rows: the seek path counts or scans the table"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// A skewed uuid key (the nil uuid in the dense block) on PostgreSQL is recut by density: every row once, balanced.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_skewed_uuid_keyset_is_recut_by_density_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_skew_uuid");
    let last = SKEW_DENSE - 1;
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id UUID PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT lpad(to_hex(g), 32, '0')::uuid, g FROM generate_series(0, {last}) g;
         INSERT INTO {table} SELECT (lpad(to_hex(g), 8, '0') || repeat('0', 24))::uuid, g FROM generate_series(1, {SKEW_SPARSE}) g;"
    ))
    .unwrap();
    pg_catalog_says_rows(&mut c, &table, 20_000_000);
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000")
        .no_oracle("the test grades the delivered uuids itself against the fixture's keys");
    let (said, spread) = skew_run(&rig);
    assert_seek_recut(&said);
    assert_skew_uuid_delivered(&rig.out_dir());
    assert_within_twice_the_mean(&spread);
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// A skewed integer key on MySQL is recut by density: every row once, balanced ranges.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn a_skewed_integer_keyset_is_recut_by_density_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_skew_my");
    let mut c = mysql_connect();
    mysql_skew_int(&mut c, &table, "STATS_PERSISTENT=1 STATS_AUTO_RECALC=0");
    mysql_catalog_says_rows(&table, 20_000_000);
    let rig = Rig::mysql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000");
    let (said, spread) = skew_run(&rig);
    assert_seek_recut(&said);
    assert_skew_int_delivered(&rig.out_dir());
    assert_within_twice_the_mean(&spread);
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

/// A skewed uuid key stored as CHAR(36) on MySQL is recut by density: every row once, the nil uuid among them.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn a_skewed_uuid_keyset_is_recut_by_density_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_skew_uuid_my");
    let mut c = mysql_connect();
    mysql_skew_uuid(&mut c, &table, "STATS_PERSISTENT=1 STATS_AUTO_RECALC=0");
    mysql_catalog_says_rows(&table, 20_000_000);
    let rig = Rig::mysql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000");
    let (said, spread) = skew_run(&rig);
    assert_seek_recut(&said);
    assert_skew_uuid_delivered(&rig.out_dir());
    assert_within_twice_the_mean(&spread);
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

/// Bursts of a UUIDv7-shaped key: rows per burst and bursts in the fixture.
const V7_BURST: u32 = 1200;
const V7_BURSTS: u32 = 5;

/// The UUIDv7-shaped keys: a timestamp prefix per burst, far apart, and a counter inside it.
fn v7_keys() -> BTreeSet<String> {
    (0..V7_BURSTS)
        .flat_map(|b| (1..=V7_BURST).map(move |g| (u128::from(b + 1) << 112) | u128::from(g)))
        .map(|v| uuid::Uuid::from_u128(v).simple().to_string())
        .collect()
}

/// Every burst row lands once.
fn assert_v7_delivered(dir: &std::path::Path) {
    let ids: Vec<String> = dir_parquet_display(dir, "id")
        .into_iter()
        .flatten()
        .map(|id| id.replace('-', ""))
        .collect();
    let distinct: BTreeSet<String> = ids.iter().cloned().collect();
    assert_eq!(
        (ids.len(), distinct),
        ((V7_BURST * V7_BURSTS) as usize, v7_keys()),
        "every source uuid once"
    );
}

/// UUIDv7 bursts far apart in time on PostgreSQL are recut into balanced ranges: every row once.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn uuid_v7_bursts_are_recut_into_balanced_ranges_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_v7");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id UUID PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT (lpad(to_hex(b), 4, '0') || repeat('0', 12) || lpad(to_hex(g), 16, '0'))::uuid, g
           FROM generate_series(1, {V7_BURSTS}) b, generate_series(1, {V7_BURST}) g;"
    ))
    .unwrap();
    pg_catalog_says_rows(&mut c, &table, 20_000_000);
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000")
        .no_oracle("the test grades the delivered uuids itself against the fixture's keys");
    let (said, spread) = skew_run(&rig);
    assert_seek_recut(&said);
    assert_v7_delivered(&rig.out_dir());
    assert_within_twice_the_mean(&spread);
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// UUIDv7 bursts far apart in time on MySQL (CHAR(36)) are recut into balanced ranges: every row once.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn uuid_v7_bursts_are_recut_into_balanced_ranges_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_v7_my");
    let mut c = mysql_connect();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id CHAR(36) NOT NULL PRIMARY KEY, payload INT NOT NULL) STATS_PERSISTENT=1 STATS_AUTO_RECALC=0"
    ))
    .unwrap();
    c.query_drop(format!("SET SESSION cte_max_recursion_depth = {V7_BURST}"))
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE g AS (SELECT 1 n UNION ALL SELECT n + 1 FROM g WHERE n < {V7_BURST}), \
         b AS (SELECT 1 k UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5), \
         hx AS (SELECT CONCAT(LPAD(HEX(k), 4, '0'), REPEAT('0', 12), LPAD(HEX(n), 16, '0')) h, n FROM g CROSS JOIN b) \
         SELECT LOWER(CONCAT_WS('-', SUBSTR(h,1,8), SUBSTR(h,9,4), SUBSTR(h,13,4), SUBSTR(h,17,4), SUBSTR(h,21,12))), n FROM hx"
    ))
    .unwrap();
    mysql_catalog_says_rows(&table, 20_000_000);
    let rig = Rig::mysql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000");
    let (said, spread) = skew_run(&rig);
    assert_seek_recut(&said);
    assert_v7_delivered(&rig.out_dir());
    assert_within_twice_the_mean(&spread);
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

/// A composite key whose first column holds one hot value for 90% of the rows exports every row once, in more than one range.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_composite_key_with_a_hot_first_value_exports_every_row_once_postgres() {
    require_alive(LiveService::Postgres);
    const HOT: i64 = 18_000;
    const TAIL: i64 = 2_000;
    let table = unique_name("pk_hot_first");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (grp INT NOT NULL, id BIGINT NOT NULL, payload INT NOT NULL, PRIMARY KEY (grp, id));
         INSERT INTO {table} SELECT 1, g, g FROM generate_series(1, {HOT}) g;
         INSERT INTO {table} SELECT 1 + g, g, g FROM generate_series(1, {TAIL}) g;"
    ))
    .unwrap();
    pg_catalog_says_rows(&mut c, &table, 20_000_000);
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: [grp, id]")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000")
        .no_oracle("the test grades the delivered key pairs itself against the fixture");
    rig.run_ok();
    let out = rig.out_dir();
    let want: BTreeSet<(String, String)> = (1..=HOT)
        .map(|g| (1, g))
        .chain((1..=TAIL).map(|g| (1 + g, g)))
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
    let got: Vec<(String, String)> = dir_parquet_display(&out, "grp")
        .into_iter()
        .flatten()
        .zip(dir_parquet_display(&out, "id").into_iter().flatten())
        .collect();
    let distinct: BTreeSet<(String, String)> = got.iter().cloned().collect();
    assert_eq!(
        (got.len(), distinct, worker_fanout(&out) >= 2),
        ((HOT + TAIL) as usize, want, true),
        "rows, distinct key pairs, more than one worker"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// A skewed integer key on Oracle, whose catalog gives no row estimate, is recut by density: every row once, NUMBER(19) key.
#[test]
#[ignore = "live: requires docker compose up -d oracle"]
fn a_skewed_integer_keyset_is_recut_by_density_oracle() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_skew_int",
        "id NUMBER(19) PRIMARY KEY, payload NUMBER(10) NOT NULL",
    );
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, LEVEL FROM dual CONNECT BY LEVEL <= {SKEW_DENSE}",
        t.name()
    ));
    ora_exec(&format!(
        "INSERT INTO {} SELECT {SKEW_DENSE} + LEVEL * 500000, LEVEL FROM dual CONNECT BY LEVEL <= {SKEW_SPARSE}",
        t.name()
    ));
    let rig = Rig::oracle_batch(t.name())
        .mode("chunked")
        .export_line("chunk_by_key: ID")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000");
    let (said, spread) = skew_run(&rig);
    assert_seek_recut(&said);
    assert_skew_int_delivered(&rig.out_dir());
    assert_within_twice_the_mean(&spread);
}

/// A skewed uuid key stored as VARCHAR2(36) on Oracle is recut by density: every row once, the nil uuid among them.
#[test]
#[ignore = "live: requires docker compose up -d oracle"]
fn a_skewed_uuid_keyset_is_recut_by_density_oracle() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_skew_uuid",
        "id VARCHAR2(36) PRIMARY KEY, payload NUMBER(10) NOT NULL",
    );
    let canon = |h: &str| {
        format!(
            "LOWER(SUBSTR({h},1,8)||'-'||SUBSTR({h},9,4)||'-'||SUBSTR({h},13,4)||'-'||SUBSTR({h},17,4)||'-'||SUBSTR({h},21,12))"
        )
    };
    ora_exec(&format!(
        "INSERT INTO {} SELECT {}, g FROM (SELECT LEVEL - 1 g, LPAD(TO_CHAR(LEVEL - 1, 'FMXXXXXXXX'), 32, '0') h FROM dual CONNECT BY LEVEL <= {SKEW_DENSE})",
        t.name(),
        canon("h")
    ));
    ora_exec(&format!(
        "INSERT INTO {} SELECT {}, g FROM (SELECT LEVEL g, LPAD(TO_CHAR(LEVEL, 'FMXXXXXXXX'), 8, '0') || RPAD('0', 24, '0') h FROM dual CONNECT BY LEVEL <= {SKEW_SPARSE})",
        t.name(),
        canon("h")
    ));
    let rig = Rig::oracle_batch(t.name())
        .mode("chunked")
        .export_line("chunk_by_key: ID")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000");
    let (said, spread) = skew_run(&rig);
    assert_seek_recut(&said);
    assert_skew_uuid_delivered(&rig.out_dir());
    assert_within_twice_the_mean(&spread);
}

/// A skewed integer key on SQL Server is recut by density although sys.partitions counts 200k, under the limit: every row once, balanced.
#[test]
#[ignore = "live+gate-only: requires docker compose up -d mssql"]
fn a_skewed_integer_keyset_is_recut_by_density_mssql() {
    require_alive(LiveService::Mssql);
    let table = unique_name("pk_skew_ms");
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ));
    mssql_exec(&format!(
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < {SKEW_DENSE}) \
         INSERT INTO {table} (id, payload) SELECT n, n FROM nums OPTION (MAXRECURSION 0)"
    ));
    mssql_exec(&format!(
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < {SKEW_SPARSE}) \
         INSERT INTO {table} (id, payload) SELECT {SKEW_DENSE} + CAST(n AS BIGINT) * 500000, n FROM nums OPTION (MAXRECURSION 0)"
    ));
    let rig = Rig::mssql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000");
    let (said, spread) = skew_run(&rig);
    assert_seek_recut(&said);
    assert_skew_int_delivered(&rig.out_dir());
    assert_within_twice_the_mean(&spread);
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
}

/// A skewed uniqueidentifier key on SQL Server stays on the percentile path, never the recut: every row once, the nil uuid among them.
#[test]
#[ignore = "live+gate-only: requires docker compose up -d mssql"]
fn a_skewed_uniqueidentifier_keyset_keeps_row_percentiles_mssql() {
    require_alive(LiveService::Mssql);
    let table = unique_name("pk_skew_uid_ms");
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (k UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ));
    mssql_exec(&format!(
        "WITH nums AS (SELECT 0 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < {}) \
         INSERT INTO {table} (k, payload) SELECT CONVERT(UNIQUEIDENTIFIER, CONVERT(BINARY(16), n)), n FROM nums OPTION (MAXRECURSION 0)",
        SKEW_DENSE - 1
    ));
    mssql_exec(&format!(
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < {SKEW_SPARSE}) \
         INSERT INTO {table} (k, payload) SELECT NEWID(), n FROM nums OPTION (MAXRECURSION 0)"
    ));
    let rig = Rig::mssql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000");
    let (said, spread) = skew_run(&rig);
    assert_percentile(&said);
    assert_skew_mssql_uuid_delivered(&rig.out_dir(), "k");
    assert_within_twice_the_mean(&spread);
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
}
