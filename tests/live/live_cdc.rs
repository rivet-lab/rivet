//! Live CDC regression — locks the invariants the CDC build kept getting wrong.

use crate::common::MysqlCdcTable as Table;
use crate::common::*;
use mysql::prelude::Queryable;

fn conn() -> mysql::PooledConn {
    mysql::Pool::new(MYSQL_CDC_URL)
        .expect("mysql pool")
        .get_conn()
        .expect("mysql conn")
}

/// The CDC rig for this file — ONE builder behind both accessors below.
fn cdc_rig(tbl: &str, ckpt: &std::path::Path, out: &std::path::Path) -> Rig {
    Rig::mysql_cdc(tbl)
        .checkpoint_path(ckpt.to_path_buf())
        .dest_path(out.to_path_buf())
}

/// Config PATH in a CALLER-owned dir, via [`Rig::config_in`].
fn cdc_config(
    d: &tempfile::TempDir,
    tbl: &str,
    ckpt: &std::path::Path,
    out: &std::path::Path,
) -> std::path::PathBuf {
    cdc_rig(tbl, ckpt, out).config_in(d.path())
}

/// Template-equivalence golden.
#[test]
fn rig_renders_the_exact_legacy_cdc_template() {
    let yaml = Rig::mysql_cdc("t1")
        .export_named("t1")
        .checkpoint_path("/tmp/ck".into())
        .dest_path("/tmp/o".into())
        .yaml();
    let legacy = format!(
        "source: {{ type: mysql, url: \"{MYSQL_CDC_URL}\" }}\nexports:\n  - name: t1\n    table: t1\n    mode: cdc\n    format: parquet\n    cdc: {{ until_current: true, checkpoint: \"/tmp/ck\", server_id: {} }}\n    destination: {{ type: local, path: \"/tmp/o\" }}\n",
        server_id_for("t1"),
    );
    assert_eq!(
        yaml, legacy,
        "the rig must not drift from the proven template"
    );
}

/// The single `.parquet` part written under `dir` (CDC + batch each write one for these small fixtures).
fn find_parquet_part(dir: &std::path::Path) -> std::path::PathBuf {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|x| x == "parquet"))
        .expect("a .parquet part")
}

/// `(column, Arrow type)` for the part.
fn parquet_fields(dir: &std::path::Path) -> Vec<(String, arrow::datatypes::DataType)> {
    let f = std::fs::File::open(find_parquet_part(dir)).unwrap();
    let b = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    b.schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect()
}

/// The single string value under `col` in the part.
fn parquet_one_string(dir: &std::path::Path, col: &str) -> String {
    use arrow::array::{Array, StringArray};
    let f = std::fs::File::open(find_parquet_part(dir)).unwrap();
    let mut r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap();
    let batch = r.next().expect("a row").unwrap();
    let idx = batch.schema().index_of(col).expect("column present");
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    arr.value(0).to_string()
}

fn full_config(d: &tempfile::TempDir, tbl: &str, out: &std::path::Path) -> std::path::PathBuf {
    full_rig(tbl, out, "parquet").config_in(d.path())
}

/// The `<tbl>_batch` table-form full export against a Postgres `url`.
fn pg_full_rig(tbl: &str, url: &str, out: &std::path::Path) -> Rig {
    Rig::pg_batch(tbl)
        .export_named(&format!("{tbl}_batch"))
        .source_url(url)
        .dest_path(out.to_path_buf())
}

/// The `<tbl>_batch` SELECT-* full export against the CDC MySQL.
fn full_rig(tbl: &str, out: &std::path::Path, format: &'static str) -> Rig {
    Rig::mysql_batch(&format!("{tbl}_batch"))
        .query(&format!("SELECT * FROM {tbl}"))
        .source_url(MYSQL_CDC_URL)
        .with_format(format)
        .dest_path(out.to_path_buf())
}

#[test]
#[ignore = "live: requires docker compose mysql (binlog ROW + REPLICATION grant)"]
fn cdc_column_types_match_a_batch_full_export() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_types");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, amount DECIMAL(10,2), n BIGINT, meta JSON)"
    ))
    .unwrap();
    c.query_drop(format!(
        r#"INSERT INTO {tbl} VALUES (1, 12.34, 9000000000, '{{"k":1}}')"#
    ))
    .unwrap();

    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();
    c.query_drop(format!(
        r#"INSERT INTO {tbl} VALUES (2, 56.78, 9000000001, '{{"k":2}}')"#
    ))
    .unwrap();

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &cdc_out));
    run_rivet_ok(&full_config(&d, &tbl, &batch_out));

    let cdc: std::collections::HashMap<_, _> = parquet_fields(&cdc_out).into_iter().collect();
    for (name, batch_ty) in parquet_fields(&batch_out) {
        let cdc_ty = cdc
            .get(&name)
            .unwrap_or_else(|| panic!("cdc output is missing source column {name}"));
        assert_eq!(
            cdc_ty, &batch_ty,
            "column {name}: cdc type {cdc_ty:?} must match batch type {batch_ty:?}"
        );
    }
    assert!(cdc.contains_key("__op") && cdc.contains_key("__pos"));
}

#[test]
#[ignore = "live: requires docker compose mysql (binlog ROW + REPLICATION grant)"]
fn cdc_captures_json_as_valid_json() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_json");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, meta JSON)"
    ))
    .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();
    c.query_drop(format!(
        r#"INSERT INTO {tbl} VALUES (1, '{{"a":1,"b":[2,3]}}')"#
    ))
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));

    let json = parquet_one_string(&out, "meta");
    let parsed: serde_json::Value = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("meta is not valid JSON ({e}): {json}"));
    assert_eq!(parsed["a"], 1);
    assert_eq!(parsed["b"][1], 3);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_picks_up_a_column_added_between_runs() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_drift");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out1));
    let f1: std::collections::HashMap<_, _> = parquet_fields(&out1).into_iter().collect();
    assert!(!f1.contains_key("w"), "run 1 predates the added column");

    c.query_drop(format!("ALTER TABLE {tbl} ADD COLUMN w VARCHAR(20)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (2, 20, 'hello')"))
        .unwrap();

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out2));
    let f2: std::collections::HashMap<_, _> = parquet_fields(&out2).into_iter().collect();
    assert!(
        f2.contains_key("w"),
        "run 2 must re-resolve and pick up the column added between runs"
    );
    assert_eq!(parquet_one_string(&out2, "w"), "hello");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_throughput_drains_a_large_backlog() {
    const N: i64 = 5_000;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_bench");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();

    let mut id = 0;
    while id < N {
        let end = (id + 1000).min(N);
        let vals: Vec<String> = (id..end).map(|i| format!("({i},{i})")).collect();
        c.query_drop(format!("INSERT INTO {tbl} VALUES {}", vals.join(",")))
            .unwrap();
        id = end;
    }

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let t = std::time::Instant::now();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));
    let secs = t.elapsed().as_secs_f64();

    assert_eq!(manifest_rows(&out), N, "all {N} changes must be captured");
    assert_eq!(
        duckdb_dir_scalar(&out, "count(DISTINCT id)", None),
        N,
        "the destination must HOLD all {N} ids, not merely count them"
    );

    eprintln!(
        "CDC throughput: {N} changes drained in {secs:.2}s = {:.0} rows/s",
        N as f64 / secs
    );
    assert!(
        secs < 60.0,
        "draining {N} changes took {secs:.1}s (>60s — perf regression?)"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_intra_transaction_updates_get_distinct_seq() {
    const N: i64 = 200;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_seq");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, counter BIGINT)"
    ))
    .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 0)"))
        .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();

    c.query_drop("START TRANSACTION").unwrap();
    for i in 1..=N {
        c.query_drop(format!("UPDATE {tbl} SET counter = {i} WHERE id = 1"))
            .unwrap();
    }
    c.query_drop("COMMIT").unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));

    assert_intra_transaction_seq(&out, N);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_sum_reconciles_across_intra_txn_updates() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_sum");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"
    ))
    .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();

    for txn in cdc_sum_workload(&tbl) {
        c.query_drop("START TRANSACTION").unwrap();
        for stmt in txn {
            c.query_drop(stmt).unwrap();
        }
        c.query_drop("COMMIT").unwrap();
    }
    let source_sum: i64 = c
        .query_first(format!("SELECT COALESCE(SUM(v), 0) FROM {tbl}"))
        .unwrap()
        .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));

    let changes = read_cdc_changes(&out);
    assert!(
        intra_txn_multi_change_count(&changes) > 0,
        "workload must exercise intra-transaction multi-updates or the sum passes vacuously"
    );
    let target_sum = deduped_current_sum(changes, CdcEngine::MySql);
    assert_eq!(
        source_sum, target_sum,
        "deduped-by-(__pos,__seq) SUM(v) must equal the source's SUM(v)"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_idle_first_run_then_change_is_captured_not_skipped() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_idle_bin");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    let ckpt = d.path().join("cdc.ckpt");
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out1));
    assert_eq!(manifest_rows(&out1), 0, "idle run 1 captures nothing");
    assert!(
        ckpt.exists(),
        "an idle first run must still pin the open position to the checkpoint"
    );

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 100)"))
        .unwrap();

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        1,
        "the change between an idle run and the next run must be captured, not skipped"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(1, "insert".to_string())],
        "the captured parquet must hold exactly THE change (a count of 1 could be a wrong row)"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql (binlog ROW + REPLICATION grant)"]
fn cdc_crash_after_flush_before_ack_re_reads_on_resume() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_crash");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();

    let crash_out = d.path().join("crash");
    std::fs::create_dir_all(&crash_out).unwrap();
    let crashed = cdc_rig(&tbl, &ckpt, &crash_out).run_args_env(
        &[],
        &[("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")],
    );
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume after a crash before the checkpoint re-reads both changes (no loss)"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(1, "insert".to_string()), (2, "insert".to_string())],
        "the re-read parquet must hold exactly the un-acked changes"
    );
}

fn pg_cdc_config(
    d: &tempfile::TempDir,
    tbl: &str,
    slot: &str,
    out: &std::path::Path,
) -> std::path::PathBuf {
    Rig::pg_cdc(tbl, slot)
        .dest_path(out.to_path_buf())
        .config_in(d.path())
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_resume_captures_only_new_changes() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pg");
    let slot = unique_name("rivet_regr_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"), &[])
        .unwrap();
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out1));
    assert_eq!(manifest_rows(&out1), 2, "run 1 drains the 2 changes");

    c.execute(&format!("INSERT INTO {tbl} VALUES (3,30),(4,40)"), &[])
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume drains only the 2 new changes (slot advanced, no re-read)"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(3, "insert".to_string()), (4, "insert".to_string())],
        "the resumed parquet must hold exactly the NEW changes (count 2 cannot tell new-2 from wrong-2)"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_intra_transaction_updates_get_distinct_seq() {
    use postgres::NoTls;
    const N: i64 = 200;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pg_seq");
    let slot = unique_name("rivet_seq_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, counter BIGINT); \
         ALTER TABLE {tbl} REPLICA IDENTITY FULL; INSERT INTO {tbl} VALUES (1, 0)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    c.batch_execute(&format!(
        "DO $$ BEGIN FOR i IN 1..{N} LOOP UPDATE {tbl} SET counter = i WHERE id = 1; END LOOP; END $$"
    ))
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out));

    assert_intra_transaction_seq(&out, N);
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_sum_reconciles_across_intra_txn_updates() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pg_sum");
    let slot = unique_name("rivet_sum_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL); \
         ALTER TABLE {tbl} REPLICA IDENTITY FULL"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    for txn in cdc_sum_workload(&tbl) {
        c.batch_execute(&format!("BEGIN; {}; COMMIT", txn.join("; ")))
            .unwrap();
    }
    let source_sum: i64 = c
        .query_one(
            &format!("SELECT COALESCE(SUM(v), 0)::bigint FROM {tbl}"),
            &[],
        )
        .unwrap()
        .get::<_, i64>(0);

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out));

    let changes = read_cdc_changes(&out);
    assert!(
        intra_txn_multi_change_count(&changes) > 0,
        "workload must exercise intra-transaction multi-updates or the sum passes vacuously"
    );
    let target_sum = deduped_current_sum(changes, CdcEngine::Postgres);
    assert_eq!(
        source_sum, target_sum,
        "deduped-by-(__pos,__seq) SUM(v) must equal the source's SUM(v)"
    );
}

/// Assert every source column of the batch export is byte-for-byte identical.
fn assert_cdc_matches_batch(cdc_out: &std::path::Path, batch_out: &std::path::Path) {
    let batch = read_one_batch(batch_out);
    let cdc = read_one_batch(cdc_out);
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let name = field.name();
        let cidx = cdc
            .schema()
            .index_of(name)
            .unwrap_or_else(|_| panic!("cdc output is missing source column {name}"));
        assert_eq!(
            batch.column(i).to_data(),
            cdc.column(cidx).to_data(),
            "column {name}: CDC differs from the batch export (type or value drift)"
        );
    }
    assert!(cdc.schema().index_of("__op").is_ok() && cdc.schema().index_of("__pos").is_ok());
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_csv_rendering_matches_batch_csv() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_csv_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, amount DECIMAL(18,4), dt DATETIME(6), \
         d DATE, t TIME(6), note VARCHAR(40))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    let cdc_rig = Rig::mysql_cdc(&tbl)
        .with_format("csv")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = cdc_rig.config_path();
    run_rivet_ok(&cfg);
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES \
         (1, 999999999999.9999, '2035-08-07 09:08:07.987654', '2024-03-15', \
          '23:59:59.999999', 'plain text'), \
         (2, NULL, NULL, NULL, NULL, NULL)"
    ))
    .unwrap();
    run_rivet_ok(&cfg);
    let batch_rig = full_rig(&tbl, &batch_out, "csv");
    run_rivet_ok(&batch_rig.config_path());

    let read_csv = |dir: &std::path::Path| -> Vec<String> {
        let p = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|p| p.extension().is_some_and(|x| x == "csv"))
            .expect("a .csv part");
        std::fs::read_to_string(p)
            .unwrap()
            .lines()
            .map(|l| l.to_string())
            .collect()
    };
    let cdc_lines = read_csv(&out);
    let batch_lines = read_csv(&batch_out);
    assert_eq!(
        cdc_lines.len(),
        batch_lines.len(),
        "same row count + header"
    );
    const DATA_COLS: usize = 6;
    for (cl, bl) in cdc_lines.iter().zip(batch_lines.iter()) {
        let cdc_tail: Vec<&str> = cl.rsplitn(DATA_COLS + 1, ',').collect();
        let cdc_data: Vec<&str> = cdc_tail[..DATA_COLS].iter().rev().cloned().collect();
        let batch_data: Vec<&str> = bl.split(',').collect();
        assert_eq!(
            cdc_data, batch_data,
            "CSV rendering differs between CDC and batch"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_non_utc_server_timezone_matches_batch_and_utc_instant() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_tz_my");
    let root_url = MYSQL_CDC_URL.replace("rivet:rivet@", "root:rivet@");
    let mut admin = mysql::Conn::new(mysql::Opts::from_url(&root_url).unwrap()).unwrap();
    use mysql::prelude::Queryable as _;
    let old_tz: String = admin
        .query_first("SELECT @@global.time_zone")
        .unwrap()
        .unwrap();
    admin
        .query_drop("SET GLOBAL time_zone = '+09:00'")
        .expect("set global tz");
    struct TzGuard(String, String);
    impl Drop for TzGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(&self.1).unwrap()) {
                use mysql::prelude::Queryable as _;
                let _ = c.query_drop(format!("SET GLOBAL time_zone = '{}'", self.0));
            }
        }
    }
    let _tz = TzGuard(old_tz, root_url);

    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, ts TIMESTAMP(6), dt DATETIME(6))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, '2024-06-15 12:00:00', '2024-06-15 12:00:00')"
    ))
    .unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));
    run_rivet_ok(&full_config(&d, &tbl, &batch_out));
    assert_cdc_matches_batch(&out, &batch_out);

    use arrow::array::TimestampMicrosecondArray;
    let b = read_one_batch(&out);
    let val = |col: &str| -> i64 {
        b.column(b.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0)
    };
    assert_eq!(
        val("ts"),
        1_718_420_400_000_000,
        "TIMESTAMP must be the UTC instant 2024-06-15T03:00:00Z"
    );
    assert_eq!(
        val("dt"),
        1_718_452_800_000_000,
        "DATETIME must stay the literal wall-clock 12:00:00"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_non_utc_database_timezone_matches_batch() {
    let d = tempfile::tempdir().unwrap();
    let cdc_db = CdcDb::new("cdc_tz");
    let tbl = unique_name("cdc_tz_pg");
    let slot = unique_name("rivet_tz_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "ALTER DATABASE {} SET timezone TO 'Asia/Tokyo'",
        cdc_db.name()
    ))
    .expect("set db tz");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, tstz TIMESTAMPTZ, ts TIMESTAMP)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, '2024-06-15T03:00:00Z', '2024-06-15 12:00:00')"
    ))
    .unwrap();

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .source_url(cdc_db.url())
        .dest_path(out.clone());
    run_rivet_ok(&rig.config_path());
    let batch_rig = pg_full_rig(&tbl, cdc_db.url(), &batch_out);
    run_rivet_ok(&batch_rig.config_path());
    assert_cdc_matches_batch(&out, &batch_out);

    use arrow::array::TimestampMicrosecondArray;
    let b = read_one_batch(&out);
    let tstz = b
        .column(b.schema().index_of("tstz").unwrap())
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        tstz.value(0),
        1_718_420_400_000_000,
        "TIMESTAMPTZ must be the UTC instant regardless of the rendered zone"
    );
    let ts = b
        .column(b.schema().index_of("ts").unwrap())
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        ts.value(0),
        1_718_452_800_000_000,
        "naive TIMESTAMP must stay the literal wall-clock"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_unchanged_toast_recovers_from_replica_identity_full() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_toast_full");
    let slot = unique_name("rivet_toast_full_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id INT PRIMARY KEY, small TEXT, big TEXT); \
         ALTER TABLE {tbl} ALTER COLUMN big SET STORAGE EXTERNAL; \
         ALTER TABLE {tbl} REPLICA IDENTITY FULL;"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} (id, small, big) VALUES \
           (1, 'a', (SELECT string_agg(md5(g::text || random()::text), '') \
                     FROM generate_series(1, 200) g)); \
         UPDATE {tbl} SET small = 'b' WHERE id = 1;"
    ))
    .unwrap();
    let real: String = c
        .query_one(&format!("SELECT big FROM {tbl} WHERE id = 1"), &[])
        .unwrap()
        .get(0);

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let batches = Rig::pg_cdc(&tbl, &slot).dest_path(out).run_and_read();

    use arrow::array::{Array, StringArray};
    let mut bigs = Vec::new();
    for b in &batches {
        let idx = b.schema().index_of("big").expect("big column present");
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("big is a string column");
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                bigs.push(arr.value(i).to_string());
            }
        }
    }
    assert!(
        !bigs.iter().any(|v| v == "unchanged-toast-datum"),
        "the literal TOAST marker must NEVER be written as data; got {bigs:?}"
    );
    assert!(
        bigs.iter().filter(|v| **v == real).count() >= 2,
        "both the INSERT and the recovered UPDATE row must carry the real value"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_unchanged_toast_without_full_identity_fails_loud() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_toast_default");
    let slot = unique_name("rivet_toast_default_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id INT PRIMARY KEY, small TEXT, big TEXT); \
         ALTER TABLE {tbl} ALTER COLUMN big SET STORAGE EXTERNAL;"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} (id, small, big) VALUES \
           (1, 'a', (SELECT string_agg(md5(g::text || random()::text), '') \
                     FROM generate_series(1, 200) g)); \
         UPDATE {tbl} SET small = 'b' WHERE id = 1;"
    ))
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let err = Rig::pg_cdc(&tbl, &slot).dest_path(out).run_expect_fail();
    assert!(
        err.contains("unchanged-TOAST"),
        "must fail loud on an unrecoverable TOAST datum; got: {err}"
    );
    assert!(
        err.contains("REPLICA IDENTITY FULL"),
        "must name the upstream fix; got: {err}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_non_iso_datestyle_and_escape_bytea_match_batch() {
    let d = tempfile::tempdir().unwrap();
    let cdc_db = CdcDb::new("cdc_fmt");
    let tbl = unique_name("cdc_fmt_pg");
    let slot = unique_name("rivet_fmt_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "ALTER DATABASE {db} SET datestyle TO 'German, DMY'; \
         ALTER DATABASE {db} SET bytea_output TO 'escape'",
        db = cdc_db.name()
    ))
    .expect("set db formats");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, d DATE, ts TIMESTAMP, blob BYTEA)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, '2024-03-05', '2024-03-05 12:00:00', '\\xdeadbeef')"
    ))
    .unwrap();

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .source_url(cdc_db.url())
        .dest_path(out.clone());
    run_rivet_ok(&rig.config_path());
    let batch_rig = pg_full_rig(&tbl, cdc_db.url(), &batch_out);
    run_rivet_ok(&batch_rig.config_path());
    assert_cdc_matches_batch(&out, &batch_out);
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_lmt_timestamptz_under_a_named_zone_matches_batch() {
    let d = tempfile::tempdir().unwrap();
    let cdc_db = CdcDb::new("cdc_lmt");
    let tbl = unique_name("cdc_lmt_pg");
    let slot = unique_name("rivet_lmt_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "ALTER DATABASE {db} SET timezone TO 'Europe/Berlin'",
        db = cdc_db.name()
    ))
    .expect("set db timezone");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, ts TIMESTAMPTZ)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, '1850-01-01 00:00:00+00'), (2, '2024-03-05 12:00:00+00')"
    ))
    .unwrap();

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .source_url(cdc_db.url())
        .dest_path(out.clone());
    run_rivet_ok(&rig.config_path());
    let batch_rig = pg_full_rig(&tbl, cdc_db.url(), &batch_out);
    run_rivet_ok(&batch_rig.config_path());
    assert_cdc_matches_batch(&out, &batch_out);
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_domain_columns_decode_as_their_base_type_and_match_batch() {
    let d = tempfile::tempdir().unwrap();
    let cdc_db = CdcDb::new("cdc_domain");
    let tbl = unique_name("cdc_domain_pg");
    let slot = unique_name("rivet_domain_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE DOMAIN posint AS integer CHECK (VALUE > 0); \
         CREATE DOMAIN day_d AS date; \
         CREATE DOMAIN workday AS day_d; \
         CREATE DOMAIN flag AS boolean; \
         CREATE TABLE {tbl} (id INT PRIMARY KEY, qty posint, d workday, ok flag)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, 5, '2024-02-29', true), (2, 7, '1999-12-31', false)"
    ))
    .unwrap();

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .source_url(cdc_db.url())
        .dest_path(out.clone());
    run_rivet_ok(&rig.config_path());
    let batch_rig = pg_full_rig(&tbl, cdc_db.url(), &batch_out);
    run_rivet_ok(&batch_rig.config_path());
    assert_cdc_matches_batch(&out, &batch_out);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_update_and_delete_carry_full_types() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_updel_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, amount DECIMAL(18,4), dt DATETIME(6), \
         tm TIME(6), en ENUM('a','b','c'), st SET('x','y','z'), vb VARBINARY(8), \
         big BIGINT UNSIGNED, note TEXT)"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, 1.5000, '2024-01-01 00:00:00', '01:02:03', \
         'a', 'x', 0xAA, 1, 'v1')"
    ))
    .unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));

    c.query_drop(format!(
        "UPDATE {tbl} SET amount=999999999999.9999, dt='2035-08-07 09:08:07.987654', \
         tm='23:59:59.999999', en='c', st='x,y,z', vb=0xDEADBEEF, \
         big=18446744073709551615, note='üñíçødé v2' WHERE id=1"
    ))
    .unwrap();
    let upd_out = d.path().join("upd");
    std::fs::create_dir_all(&upd_out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &upd_out));
    run_rivet_ok(&full_config(&d, &tbl, &batch_out));
    let upd = read_one_batch(&upd_out);
    assert_eq!(upd.num_rows(), 1, "exactly the update event");
    assert_eq!(parquet_one_string(&upd_out, "__op"), "update");
    let batch = read_one_batch(&batch_out);
    for field in batch.schema().fields() {
        let bi = batch.schema().index_of(field.name()).unwrap();
        let ci = upd.schema().index_of(field.name()).unwrap();
        assert_eq!(
            batch.column(bi).to_data(),
            upd.column(ci).to_data(),
            "update after-image column {}: differs from post-update batch",
            field.name()
        );
    }

    c.query_drop(format!("DELETE FROM {tbl} WHERE id=1"))
        .unwrap();
    let del_out = d.path().join("del");
    std::fs::create_dir_all(&del_out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &del_out));
    let del = read_one_batch(&del_out);
    assert_eq!(del.num_rows(), 1);
    assert_eq!(parquet_one_string(&del_out, "__op"), "delete");
    use arrow::array::Int32Array;
    let id = del
        .column(del.schema().index_of("id").unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("typed PK in the delete image");
    assert_eq!(id.value(0), 1);
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_update_and_delete_carry_full_types() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_updel_pg");
    let slot = unique_name("rivet_updel_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (
           id BIGINT PRIMARY KEY, amount NUMERIC(18,2), ts TIMESTAMPTZ, u UUID,
           tags TEXT[], nums INTEGER[], iv INTERVAL, note TEXT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, 1.50, '2024-01-01T00:00:00Z',
           'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011', ARRAY['a'], ARRAY[1],
           INTERVAL '1 day', 'v1')"
    ))
    .unwrap();
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out));

    c.batch_execute(&format!(
        "UPDATE {tbl} SET amount=999999999999.99, ts='2035-08-07T09:08:07.987654Z',
           u='ffffffff-ffff-ffff-ffff-ffffffffffff',
           tags=ARRAY['with,comma', NULL], nums=ARRAY[7, NULL, 9],
           iv=INTERVAL '1 year 2 mons 3 days', note='üñíçødé v2' WHERE id=1"
    ))
    .unwrap();
    let upd_out = d.path().join("upd");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&upd_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &upd_out));
    let batch_rig = pg_full_rig(&tbl, POSTGRES_CDC_URL, &batch_out);
    run_rivet_ok(&batch_rig.config_path());
    let upd = read_one_batch(&upd_out);
    assert_eq!(upd.num_rows(), 1, "exactly the update event");
    assert_eq!(parquet_one_string(&upd_out, "__op"), "update");
    let batch = read_one_batch(&batch_out);
    for field in batch.schema().fields() {
        let bi = batch.schema().index_of(field.name()).unwrap();
        let ci = upd.schema().index_of(field.name()).unwrap();
        assert_eq!(
            batch.column(bi).to_data(),
            upd.column(ci).to_data(),
            "update after-image column {}: differs from post-update batch",
            field.name()
        );
    }

    c.execute(&format!("DELETE FROM {tbl} WHERE id=1"), &[])
        .unwrap();
    let del_out = d.path().join("del");
    std::fs::create_dir_all(&del_out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &del_out));
    let del = read_one_batch(&del_out);
    assert_eq!(del.num_rows(), 1);
    assert_eq!(parquet_one_string(&del_out, "__op"), "delete");
    use arrow::array::Int64Array;
    let id = del
        .column(del.schema().index_of("id").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("typed PK in the delete image");
    assert_eq!(id.value(0), 1);
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_hostile_floats_match_batch_and_nan_numeric_fails_loudly() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_hostile_pg");
    let slot = unique_name("rivet_hostile_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (
           id INT PRIMARY KEY, f8 FLOAT8, f4 REAL, n NUMERIC(18,2))"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES
           (1, 'Infinity', '-Infinity', 1.50),
           (2, '-Infinity', 'NaN', NULL),
           (3, 'NaN', 'Infinity', 0.01),
           (4, NULL, NULL, NULL)"
    ))
    .unwrap();
    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &cdc_out));
    let batch_rig = pg_full_rig(&tbl, POSTGRES_CDC_URL, &batch_out);
    run_rivet_ok(&batch_rig.config_path());
    assert_cdc_matches_batch(&cdc_out, &batch_out);

    c.execute(
        &format!("INSERT INTO {tbl} VALUES (5, 1.0, 1.0, 'NaN')"),
        &[],
    )
    .unwrap();
    let out = run_rivet_env(
        &[
            "run",
            "--config",
            pg_cdc_config(&d, &tbl, &slot, &cdc_out).to_str().unwrap(),
        ],
        &[],
    );
    assert!(
        !out.status.success(),
        "CDC must fail loudly on NaN::numeric, like batch — not NULL it silently"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("RIVET_SOURCE_CDC_CELL_UNSUPPORTED")
            && stderr.contains("column 'n' is Decimal128(18, 2)")
            && stderr.contains("\"NaN\"")
            && stderr.contains("NaN/Infinity"),
        "the failure must name the column, the payload and why: {stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_hostile_zero_date_and_nul_string_match_batch() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_hostile_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, dt DATETIME, s VARCHAR(20), \
         ts TIMESTAMP NULL, ts6 TIMESTAMP(6) NULL)"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));
    c.query_drop("SET SESSION sql_mode=''").unwrap();
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, '0000-00-00 00:00:00', CONCAT('a', CHAR(0), 'b'), \
         '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000'), \
         (2, '2024-03-15 12:00:00', 'plain', '2024-03-15 12:00:00', '2024-03-15 12:00:00.500000')"
    ))
    .unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));
    run_rivet_ok(&full_config(&d, &tbl, &batch_out));
    assert_cdc_matches_batch(&out, &batch_out);

    use arrow::array::{Array, TimestampMicrosecondArray};
    let b = read_one_batch(&out);
    let dt_idx = b.schema().index_of("dt").unwrap();
    let dt = b
        .column(dt_idx)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert!(dt.is_null(0), "zero-date degrades to NULL (documented)");
    assert!(!dt.is_null(1), "a real datetime stays");
    for col in ["ts", "ts6"] {
        let arr = b.column(b.schema().index_of(col).unwrap());
        assert!(
            arr.is_null(0),
            "{col}: a zero TIMESTAMP is NULL, not 1970-01-01"
        );
        assert!(!arr.is_null(1), "{col}: a real TIMESTAMP stays");
    }
    let s = parquet_one_string(&out, "s");
    assert_eq!(s.as_bytes(), b"a\0b", "embedded NUL survives byte-for-byte");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_qualified_overrides_target_one_table_bare_applies_to_the_rest() {
    use arrow::datatypes::DataType;
    let d = tempfile::tempdir().unwrap();
    let ta = unique_name("cdc_qo_a");
    let tb = unique_name("cdc_qo_b");
    let mut c = conn();
    for t in [&ta, &tb] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"))
            .unwrap();
    }
    let (_g1, _g2) = (Table(ta.clone()), Table(tb.clone()));

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mysql_cdc(&ta)
        .tables(&[&ta, &tb])
        .export_named("app_cdc")
        .export_line(&format!(
            "columns: {{ v: text, \"{tb}.v\": \"decimal(20,4)\" }}"
        ))
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();

    run_rivet_ok(&cfg);
    c.query_drop(format!("INSERT INTO {ta} VALUES (1, -42)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tb} VALUES (1, 7)"))
        .unwrap();
    run_rivet_ok(&cfg);

    let ty_of = |t: &str| {
        parquet_fields(&out.join(t))
            .into_iter()
            .find(|(n, _)| n == "v")
            .map(|(_, ty)| ty)
            .unwrap()
    };
    assert_eq!(ty_of(&ta), DataType::Utf8, "bare `v: text` applies to a");
    assert_eq!(
        ty_of(&tb),
        DataType::Decimal128(20, 4),
        "qualified key wins over the bare one for b"
    );
    assert_eq!(parquet_one_string(&out.join(&ta), "v"), "-42");
    use arrow::array::Decimal128Array;
    let b = read_one_batch(&out.join(&tb));
    let bv = b
        .column(b.schema().index_of("v").unwrap())
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(bv.value(0), 70_000, "7 at scale 4");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_multi_table_same_column_name_different_types_resolve_per_table() {
    use arrow::datatypes::DataType;
    let d = tempfile::tempdir().unwrap();
    let ta = unique_name("cdc_nm_int");
    let tb = unique_name("cdc_nm_dec");
    let tc = unique_name("cdc_nm_enum");
    let mut c = conn();
    for (t, ty) in [
        (&ta, "INT"),
        (&tb, "DECIMAL(10,2)"),
        (&tc, "ENUM('on','off')"),
    ] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v {ty})"))
            .unwrap();
    }
    let (_g1, _g2, _g3) = (Table(ta.clone()), Table(tb.clone()), Table(tc.clone()));

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mysql_cdc(&ta)
        .tables(&[&ta, &tb, &tc])
        .export_named("app_cdc")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();

    run_rivet_ok(&cfg);
    c.query_drop(format!("INSERT INTO {ta} VALUES (1, -42)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tb} VALUES (1, 13.37)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tc} VALUES (1, 'off')"))
        .unwrap();
    run_rivet_ok(&cfg);

    let ty_of = |t: &str| {
        parquet_fields(&out.join(t))
            .into_iter()
            .find(|(n, _)| n == "v")
            .map(|(_, ty)| ty)
            .unwrap()
    };
    assert_eq!(ty_of(&ta), DataType::Int32, "a.v stays INT");
    assert_eq!(ty_of(&tb), DataType::Decimal128(10, 2), "b.v stays DECIMAL");
    assert_eq!(ty_of(&tc), DataType::Utf8, "c.v stays ENUM→Utf8");
    assert_eq!(parquet_one_string(&out.join(&tc), "v"), "off");
    use arrow::array::{Decimal128Array, Int32Array};
    let a = read_one_batch(&out.join(&ta));
    let av = a
        .column(a.schema().index_of("v").unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(av.value(0), -42);
    let b = read_one_batch(&out.join(&tb));
    let bv = b
        .column(b.schema().index_of("v").unwrap())
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(bv.value(0), 1337, "13.37 at scale 2");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_initial_snapshot_covers_preexisting_rows_then_streams() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_init_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mysql_cdc(&tbl)
        .cdc_line("initial: snapshot")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();

    run_rivet_ok(&cfg);
    let snap = out.join("snapshot");
    assert_eq!(
        manifest_rows(&snap),
        2,
        "pre-existing rows land in snapshot/"
    );
    assert_eq!(manifest_rows(&out), 0, "nothing to drain yet");
    assert_eq!(
        duckdb_dir_parquet_id_set(&snap)
            .into_iter()
            .collect::<Vec<i64>>(),
        vec![1, 2],
        "snapshot parquet must hold exactly the pre-existing ids (independent re-read)"
    );
    let snap_parts = || {
        std::fs::read_dir(&snap)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "parquet")
            })
            .count()
    };
    assert_eq!(snap_parts(), 1);

    c.query_drop(format!("INSERT INTO {tbl} VALUES (3,30)"))
        .unwrap();
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out), 1, "the post-snapshot change streams");
    assert_eq!(
        cdc_id_ops(&out),
        vec![(3, "insert".to_string())],
        "streamed parquet must hold exactly the post-snapshot change (not just a count of 1)"
    );
    assert_eq!(snap_parts(), 1, "run 2 must NOT re-snapshot");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_initial_snapshot_leg_drops_batch_meta_columns_for_load_parity() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_init_meta");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mysql_cdc(&tbl)
        .cdc_line("initial: snapshot")
        .export_line("meta_columns: { exported_at: true, row_hash: true }")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();
    run_rivet_ok(&cfg);

    let snap = out.join("snapshot");
    let snap_cols: Vec<String> = parquet_fields(&snap).into_iter().map(|(n, _)| n).collect();
    assert!(
        !snap_cols.iter().any(|c| c == "_rivet_exported_at"),
        "snapshot leg must NOT carry `_rivet_exported_at` — only the batch leg can \
         produce it, so it would break column parity with the CDC leg's __changes \
         append; got {snap_cols:?}"
    );
    assert!(
        snap_cols.iter().any(|c| c == "_rivet_row_hash"),
        "snapshot leg MUST carry `_rivet_row_hash` — both legs produce it, and a \
         column only one leg wrote leaves half the __changes table NULL; got \
         {snap_cols:?}"
    );
    assert!(
        snap_cols.iter().any(|c| c == "id") && snap_cols.iter().any(|c| c == "v"),
        "snapshot leg still carries the source columns; got {snap_cols:?}"
    );

    c.query_drop(format!("INSERT INTO {tbl} VALUES (3,30)"))
        .unwrap();
    run_rivet_ok(&cfg);
    let cdc_cols: Vec<String> = parquet_fields(&out).into_iter().map(|(n, _)| n).collect();
    let cdc_data: Vec<&String> = cdc_cols.iter().filter(|c| !c.starts_with("__")).collect();
    let snap_data: Vec<&String> = snap_cols.iter().filter(|c| !c.starts_with("__")).collect();
    assert_eq!(
        cdc_data, snap_data,
        "the CDC stream's data columns must equal the snapshot's — else the shared \
         __changes append mismatches"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_initial_snapshot_vanished_slot_fails_loudly_not_recreates() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_init_vslot");
    let slot = unique_name("rivet_initv_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT); \
         INSERT INTO {tbl} VALUES (1,10)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .cdc_line("initial: snapshot")
        .dest_path(out.clone());
    let cfg = rig.config_path();

    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out.join("snapshot")), 1);

    c.execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (2,20)"), &[])
        .unwrap();

    let res = run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], &[]);
    assert!(
        !res.status.success(),
        "a vanished slot with a completed snapshot behind it must FAIL, not silently re-anchor"
    );
    let stderr = String::from_utf8_lossy(&res.stderr);
    let expected = format!(
        "pg cdc: slot '{slot}' is missing but a prior run completed this export's snapshot (the \
         state DB's `cdc_snapshot` row or the destination's snapshot/_SUCCESS marker) — the slot \
         was dropped or invalidated, and the changes since then are no longer in the log. \
         {REBASELINE_REMEDY}"
    );
    assert!(
        stderr.contains(&expected),
        "the refusal must name the snapshot it found, and only that:\n{stderr}"
    );
}

/// The slot-created warning's remedy, followed as printed.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical) + the rivet-duckdb oracle"]
fn pg_slot_created_warning_remedy_recovers_the_row_written_while_the_slot_was_gone() {
    use postgres::NoTls;
    let tbl = unique_name("cdc_slotgap");
    let slot = unique_name("rivet_slotgap");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let mut rig = Rig::pg_cdc(&tbl, &slot);
    rig.run_ok();
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    rig.run_ok();
    c.execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (2, 20)"))
        .unwrap();
    let said = rig.run_ok_capture_known_defect(
        "undelivered rows",
        "known defect: a PostgreSQL slot dropped under a stream with no checkpoint or baseline \
         is re-created with only a warning, so the row written in the gap is lost until the \
         warning's re-baseline runs",
    );
    assert!(
        said.contains(&format!("pg cdc: creating replication slot '{slot}'"))
            && said.contains(REBASELINE_REMEDY),
        "the re-created slot warns with the re-baseline remedy:\n{said}"
    );
    follow_rebaseline_remedy(&mut rig, false);
    assert_eq!(
        dir_parquet_id_set(&rig.out_dir().join("snapshot")),
        [1, 2].into(),
        "the remedy's baseline holds the row written while the slot was gone"
    );
}

/// A slot lost under a completed baseline.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical) + the rivet-duckdb oracle"]
fn pg_missing_slot_refusal_remedy_recovers_the_row_written_while_the_slot_was_gone() {
    use postgres::NoTls;
    let tbl = unique_name("cdc_slotref");
    let slot = unique_name("rivet_slotref");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT); \
         INSERT INTO {tbl} VALUES (1, 10)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let mut rig = Rig::pg_cdc(&tbl, &slot).cdc_line("initial: snapshot");
    rig.run_ok();
    c.execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (2, 20)"))
        .unwrap();
    let said = rig.run_expect_fail();
    assert!(
        said.contains("is missing but a prior run completed this export's snapshot")
            && said.contains(REBASELINE_REMEDY),
        "the refusal names the re-baseline remedy:\n{said}"
    );
    follow_rebaseline_remedy(&mut rig, true);
    assert_eq!(
        dir_parquet_id_set(&rig.out_dir().join("snapshot")),
        [1, 2].into(),
        "the re-read baseline holds the row written while the slot was gone"
    );
}

/// A `tables:` stream whose slot was lost under a completed baseline.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical) + the rivet-duckdb oracle"]
fn pg_multi_table_missing_slot_refusal_remedy_re_reads_every_table() {
    use postgres::NoTls;
    let (ta, tb) = (unique_name("cdc_mrb_a"), unique_name("cdc_mrb_b"));
    let slot = unique_name("rivet_mrb_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    for t in [&ta, &tb] {
        c.batch_execute(&format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id INT PRIMARY KEY, v INT); \
             INSERT INTO {t} VALUES (1, 10)"
        ))
        .unwrap();
    }
    let _guards = (
        PgTable::adopt_on(POSTGRES_CDC_URL, ta.clone()),
        PgTable::adopt_on(POSTGRES_CDC_URL, tb.clone()),
    );
    let mut rig = Rig::pg_cdc(&ta, &slot)
        .tables(&[&ta, &tb])
        .cdc_line("initial: snapshot");
    rig.run_ok();
    c.execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    for t in [&ta, &tb] {
        c.execute(&format!("INSERT INTO {t} VALUES (2, 20)"), &[])
            .unwrap();
    }
    let said = rig.run_expect_fail();
    assert!(
        said.contains("is missing but a prior run completed this export's snapshot")
            && said.contains(REBASELINE_REMEDY),
        "the refusal names the re-baseline remedy:\n{said}"
    );
    follow_rebaseline_remedy(&mut rig, true);
    for t in [&ta, &tb] {
        assert_eq!(
            dir_parquet_id_set(&rig.out_dir().join(t).join("snapshot")),
            [1, 2].into(),
            "table {t}: the re-read baseline holds the row written while the slot was gone"
        );
    }
}

/// A truncate after its rows went out as change parts.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical) + the rivet-duckdb oracle"]
fn pg_truncate_refusal_remedy_leaves_no_removed_row_live_in_the_prefix() {
    use postgres::NoTls;
    let tbl = unique_name("cdc_trrb");
    let slot = unique_name("rivet_trrb");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let mut rig = Rig::pg_cdc(&tbl, &slot);
    rig.run_ok();
    c.execute(&format!("INSERT INTO {tbl} VALUES (1, 10)"), &[])
        .unwrap();
    rig.run_ok();
    c.execute(&format!("TRUNCATE {tbl}"), &[]).unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (2, 20)"), &[])
        .unwrap();
    let said = rig.run_expect_fail();
    assert!(
        said.contains("was TRUNCATEd") && said.contains(REBASELINE_REMEDY),
        "the refusal names the slot advance and the re-baseline remedy:\n{said}"
    );
    let past: String = c
        .query_one(
            &format!(
                "WITH p AS (SELECT lsn, data FROM pg_logical_slot_peek_changes('{slot}', NULL, NULL)) \
                 SELECT min(lsn)::text FROM p WHERE data LIKE 'COMMIT%' \
                 AND lsn > (SELECT max(lsn) FROM p WHERE data LIKE '%{tbl}%: TRUNCATE:%')"
            ),
            &[],
        )
        .unwrap()
        .get(0);
    c.execute(
        &format!("SELECT pg_replication_slot_advance('{slot}', '{past}')"),
        &[],
    )
    .unwrap();
    follow_rebaseline_remedy(&mut rig, false);
    assert_eq!(
        dir_parquet_id_set(&rig.out_dir().join("snapshot")),
        [2].into(),
        "the remedy's baseline holds the row written after the truncate, and only it"
    );
}

/// A `tables:` stream whose truncate transaction also wrote another table.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical) + the rivet-duckdb oracle"]
fn pg_multi_table_truncate_remedy_recovers_the_other_tables_row_the_advance_skipped() {
    use postgres::NoTls;
    let (ta, tb) = (unique_name("cdc_mtr_a"), unique_name("cdc_mtr_b"));
    let slot = unique_name("rivet_mtr_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    for t in [&ta, &tb] {
        c.batch_execute(&format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id INT PRIMARY KEY, v INT); \
             INSERT INTO {t} VALUES (1, 10)"
        ))
        .unwrap();
    }
    let _guards = (
        PgTable::adopt_on(POSTGRES_CDC_URL, ta.clone()),
        PgTable::adopt_on(POSTGRES_CDC_URL, tb.clone()),
    );
    let mut rig = Rig::pg_cdc(&ta, &slot)
        .tables(&[&ta, &tb])
        .cdc_line("initial: snapshot");
    rig.run_ok();
    c.batch_execute(&format!(
        "BEGIN; TRUNCATE {ta}; INSERT INTO {tb} VALUES (2, 20); COMMIT"
    ))
    .unwrap();
    c.execute(&format!("INSERT INTO {ta} VALUES (2, 20)"), &[])
        .unwrap();
    let said = rig.run_expect_fail();
    assert!(
        said.contains("was TRUNCATEd") && said.contains(REBASELINE_REMEDY),
        "the refusal names the slot advance and the re-baseline remedy:\n{said}"
    );
    let past: String = c
        .query_one(
            &format!(
                "WITH p AS (SELECT lsn, data FROM pg_logical_slot_peek_changes('{slot}', NULL, NULL)) \
                 SELECT min(lsn)::text FROM p WHERE data LIKE 'COMMIT%' \
                 AND lsn > (SELECT max(lsn) FROM p WHERE data LIKE '%{ta}%: TRUNCATE:%')"
            ),
            &[],
        )
        .unwrap()
        .get(0);
    c.execute(
        &format!("SELECT pg_replication_slot_advance('{slot}', '{past}')"),
        &[],
    )
    .unwrap();
    follow_rebaseline_remedy(&mut rig, true);
    assert_eq!(
        dir_parquet_id_set(&rig.out_dir().join(&ta).join("snapshot")),
        [2].into(),
        "the truncated table's baseline holds only the row written after the truncate"
    );
    assert_eq!(
        dir_parquet_id_set(&rig.out_dir().join(&tb).join("snapshot")),
        [1, 2].into(),
        "the other table's baseline holds the row its truncate transaction wrote"
    );
}

/// Three empty `(id, v)` tables on the PostgreSQL CDC stand, their guards, and a connection.
fn pg_trio(label: &str) -> ([String; 3], Vec<PgTable>, postgres::Client) {
    let names = ["a", "b", "c"].map(|x| unique_name(&format!("{label}_{x}")));
    let mut c =
        postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("connect postgres");
    for t in &names {
        c.batch_execute(&format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"
        ))
        .unwrap();
    }
    let guards = names
        .iter()
        .map(|t| PgTable::adopt_on(POSTGRES_CDC_URL, t.clone()))
        .collect();
    (names, guards, c)
}

/// Three empty `(id, v)` tables on the MySQL CDC stand, their guards, and a connection.
fn mysql_trio(label: &str) -> ([String; 3], [Table; 3], mysql::PooledConn) {
    let names = ["a", "b", "c"].map(|x| unique_name(&format!("{label}_{x}")));
    let mut c = conn();
    for t in &names {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"))
            .unwrap();
    }
    (names.clone(), names.map(Table), c)
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical) + the rivet-duckdb oracle"]
fn pg_table_removed_from_tables_and_put_back_is_refused_until_rebaselined() {
    let (t, _guards, mut c) = pg_trio("cdc_rejoin");
    let slot = unique_name("rivet_rejoin_slot");
    let _slot = Slot::new(slot.clone());
    a_table_put_back_is_refused_until_rebaselined(
        Rig::pg_cdc(&t[0], &slot),
        [&t[0], &t[1], &t[2]],
        &mut |t, op| c.batch_execute(&op.sql(t)).unwrap(),
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + the rivet-duckdb oracle"]
fn mysql_table_removed_from_tables_and_put_back_is_refused_until_rebaselined() {
    let (t, _guards, mut c) = mysql_trio("cdc_rejoin");
    a_table_put_back_is_refused_until_rebaselined(
        Rig::mysql_cdc(&t[0]),
        [&t[0], &t[1], &t[2]],
        &mut |t, op| c.query_drop(op.sql(t)).unwrap(),
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical) + the rivet-duckdb oracle"]
fn pg_table_switched_to_another_over_its_baseline_is_refused_and_switched_back_continues() {
    let (t, _guards, mut c) = pg_trio("cdc_switch");
    let slot = unique_name("rivet_switch_slot");
    let _slot = Slot::new(slot.clone());
    a_table_switched_over_a_baseline_is_refused_and_switched_back_continues(
        Rig::pg_cdc(&t[0], &slot),
        [&t[0], &t[1]],
        &|r, t| r.repoint(t),
        &mut |t, op| c.batch_execute(&op.sql(t)).unwrap(),
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + the rivet-duckdb oracle"]
fn mysql_table_switched_to_another_over_its_baseline_is_refused_and_switched_back_continues() {
    let (t, _guards, mut c) = mysql_trio("cdc_switch");
    a_table_switched_over_a_baseline_is_refused_and_switched_back_continues(
        Rig::mysql_cdc(&t[0]),
        [&t[0], &t[1]],
        &|r, t| r.repoint(t),
        &mut |t, op| c.query_drop(op.sql(t)).unwrap(),
    );
}

/// Unchanged: a table added to `tables:` that the stream never captured gets its baseline.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical) + the rivet-duckdb oracle"]
fn pg_table_added_to_tables_mid_stream_is_baselined() {
    let ([ta, tb, tc], _guards, mut c) = pg_trio("cdc_join");
    let slot = unique_name("rivet_join_slot");
    let _slot = Slot::new(slot.clone());
    for t in [&ta, &tb, &tc] {
        c.batch_execute(&format!("INSERT INTO {t} VALUES (1, 10), (2, 20)"))
            .unwrap();
    }
    let mut rig = Rig::pg_cdc(&ta, &slot)
        .tables(&[&ta, &tc])
        .cdc_line("initial: snapshot");
    rig.run_ok();
    rig = rig.tables(&[&ta, &tb, &tc]);
    rig.run_ok();
    assert_eq!(
        dir_parquet_id_set(&rig.out_dir().join(&tb).join("snapshot")),
        [1, 2].into(),
        "the joining table's baseline holds its rows"
    );
}

/// `rollover: 1` in config mode.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical) + the rivet-duckdb oracle"]
fn pg_cdc_rollover_1_delivers_the_commits_behind_an_idle_runs_barrier() {
    let mut s = CdcScenario::pg_with("cdc_roll1", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
        r.cdc("rollover: 1")
    });
    s.rig.run_ok();
    s.rig.run_ok();
    for id in 1..=3 {
        s.insert(id);
    }
    s.rig.run_ok();
    assert_eq!(
        dir_parquet_id_set(&s.rig.out_dir()),
        [1, 2, 3].into(),
        "three single-row commits behind two idle runs' barriers"
    );
}

/// The printed re-baseline remedy for a lost changes-only checkpoint re-reads the missed row.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + the rivet-duckdb oracle"]
fn mysql_missing_checkpoint_refusal_on_a_changes_only_stream_remedy_recovers_the_row_written_while_it_was_gone()
 {
    let mut s = CdcScenario::mysql_with("cdc_ckgap", "id INT PRIMARY KEY, v BIGINT", |r, _| r);
    s.rig.run_ok();
    s.insert(1);
    s.rig.run_ok();
    std::fs::remove_file(s.rig.checkpoint()).expect("the checkpoint the run wrote");
    s.insert(2);
    let out = s.rig.run_args(&[]);
    assert_refused(
        &out,
        Refused::by_code("RIVET_SOURCE_CDC_CHECKPOINT_INVALID", 5),
    );
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        said.contains("is missing, but the state DB records that export")
            && said.contains("rivet state reset -c")
            && said.contains(REBASELINE_REMEDY),
        "the refusal names the state record and both remedies:\n{said}"
    );
    follow_rebaseline_remedy(&mut s.rig, false);
    assert_eq!(
        dir_parquet_id_set(&s.rig.out_dir().join("snapshot")),
        [1, 2].into(),
        "the remedy's baseline holds the row written while the checkpoint was gone"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + the rivet-duckdb oracle"]
fn mysql_lost_checkpoint_on_a_changes_only_stream_is_refused_not_reanchored() {
    let mut s = CdcScenario::mysql_with("cdc_ckno", "id INT PRIMARY KEY, v BIGINT", |r, _| r);
    assert_a_lost_checkpoint_is_refused_not_reanchored(&mut s, false);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + the rivet-duckdb oracle"]
fn mysql_lost_checkpoint_under_a_baseline_stream_is_refused_not_reanchored() {
    let mut s = CdcScenario::mysql_with("cdc_cksn", "id INT PRIMARY KEY, v BIGINT", |r, _| {
        r.cdc_line("initial: snapshot")
    });
    assert_a_lost_checkpoint_is_refused_not_reanchored(&mut s, true);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + the rivet-duckdb oracle"]
fn mysql_state_reset_lifts_a_lost_checkpoint_refusal() {
    let mut s = CdcScenario::mysql_with("cdc_ckrst", "id INT PRIMARY KEY, v BIGINT", |r, _| r);
    assert_state_reset_lifts_a_lost_checkpoint_refusal(&mut s);
}

/// A checkpoint lost under a completed baseline.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + the rivet-duckdb oracle"]
fn mysql_missing_checkpoint_refusal_remedy_recovers_the_row_written_while_it_was_gone() {
    let mut s = CdcScenario::mysql_with("cdc_ckref", "id INT PRIMARY KEY, v BIGINT", |r, _| {
        r.cdc_line("initial: snapshot")
    });
    s.insert(1);
    s.rig.run_ok();
    std::fs::remove_file(s.rig.checkpoint()).expect("the checkpoint the run wrote");
    s.insert(2);
    let said = s.rig.run_expect_fail();
    assert!(
        said.contains("is missing but prior-run evidence exists")
            && said.contains(REBASELINE_REMEDY),
        "the refusal names the re-baseline remedy:\n{said}"
    );
    follow_rebaseline_remedy(&mut s.rig, true);
    assert_eq!(
        dir_parquet_id_set(&s.rig.out_dir().join("snapshot")),
        [1, 2].into(),
        "the re-read baseline holds the row written while the checkpoint was gone"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_mixed_transaction_ending_on_uncaptured_table_advances_checkpoint() {
    let d = tempfile::tempdir().unwrap();
    let orders = unique_name("cdc_mix_orders");
    let audit = unique_name("cdc_mix_audit");
    let mut c = conn();
    for t in [&orders, &audit] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"))
            .unwrap();
    }
    let (_g1, _g2) = (Table(orders.clone()), Table(audit.clone()));

    let out1 = d.path().join("out1");
    let out2 = d.path().join("out2");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out1).unwrap();
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&cdc_config(&d, &orders, &ckpt, &out1));

    c.query_drop("START TRANSACTION").unwrap();
    c.query_drop(format!("INSERT INTO {orders} VALUES (1, 10)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {audit} VALUES (1, 99)"))
        .unwrap();
    c.query_drop("COMMIT").unwrap();

    run_rivet_ok(&cdc_config(&d, &orders, &ckpt, &out1));
    assert_eq!(
        cdc_id_ops(&out1),
        vec![(1, "insert".to_string())],
        "exactly the ORDERS row — the audit row must not be routed here"
    );

    run_rivet_ok(&cdc_config(&d, &orders, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        0,
        "checkpoint must have advanced past the mixed transaction"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_mixed_transaction_ending_on_uncaptured_table_advances_checkpoint() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let orders = unique_name("cdc_mixp_o");
    let audit = unique_name("cdc_mixp_a");
    let slot = unique_name("rivet_mixp_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {orders}; DROP TABLE IF EXISTS {audit}; \
         CREATE TABLE {orders} (id INT PRIMARY KEY, v INT); \
         CREATE TABLE {audit} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let (_t1, _t2) = (
        PgTable::adopt_on(POSTGRES_CDC_URL, orders.clone()),
        PgTable::adopt_on(POSTGRES_CDC_URL, audit.clone()),
    );
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "BEGIN; INSERT INTO {orders} VALUES (1,10); INSERT INTO {audit} VALUES (1,99); COMMIT;"
    ))
    .unwrap();

    let out1 = d.path().join("out1");
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out1).unwrap();
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &orders, &slot, &out1));
    assert_eq!(
        cdc_id_ops(&out1),
        vec![(1, "insert".to_string())],
        "exactly the ORDERS row — the audit row must not be routed here"
    );
    run_rivet_ok(&pg_cdc_config(&d, &orders, &slot, &out2));
    assert_eq!(
        manifest_rows(&out2),
        0,
        "slot advanced past the mixed transaction"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_schema_qualified_table_config_captures_events() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_qual_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let qualified = format!("rivet.{tbl}");
    run_rivet_ok(&cdc_config(&d, &qualified, &ckpt, &out));
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    run_rivet_ok(&cdc_config(&d, &qualified, &ckpt, &out));
    assert_eq!(
        cdc_id_ops(&out),
        vec![(1, "insert".to_string())],
        "a db-qualified table: the ROW must land, not merely a count of 1"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_schema_qualified_table_config_captures_events() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_qual_pg");
    let slot = unique_name("rivet_qual_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (1, 10)"), &[])
        .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let qualified = format!("public.{tbl}");
    run_rivet_ok(&pg_cdc_config(&d, &qualified, &slot, &out));
    assert_eq!(
        cdc_id_ops(&out),
        vec![(1, "insert".to_string())],
        "a schema-qualified table: the ROW must land, not merely a count of 1"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_initial_snapshot_of_an_empty_table_converges_despite_skip_empty() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_init_empty");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mysql_cdc(&tbl)
        .cdc_line("initial: snapshot")
        .export_line("skip_empty: true")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();

    run_rivet_ok(&cfg);
    let marker = out.join("snapshot").join("_SUCCESS");
    assert!(
        marker.exists(),
        "an EMPTY snapshot must still write _SUCCESS or the handoff never converges"
    );
    let stamp = std::fs::metadata(&marker).unwrap().modified().unwrap();

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    run_rivet_ok(&cfg);
    assert_eq!(
        std::fs::metadata(&marker).unwrap().modified().unwrap(),
        stamp,
        "run 2 must not re-snapshot"
    );
    assert_eq!(manifest_rows(&out), 1, "the change streams normally");
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_initial_snapshot_covers_preexisting_rows_then_streams() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_init_pg");
    let slot = unique_name("rivet_init_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT); \
         INSERT INTO {tbl} VALUES (1,10),(2,20)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .cdc_line("initial: snapshot")
        .dest_path(out.clone());
    let cfg = rig.config_path();

    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out.join("snapshot")), 2);
    assert_eq!(
        duckdb_dir_parquet_id_set(&out.join("snapshot"))
            .into_iter()
            .collect::<Vec<i64>>(),
        vec![1, 2],
        "snapshot parquet must hold exactly the pre-existing ids (independent re-read)"
    );
    assert_eq!(manifest_rows(&out), 0);

    c.execute(&format!("INSERT INTO {tbl} VALUES (3,30)"), &[])
        .unwrap();
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out), 1, "the post-snapshot change streams");
    assert_eq!(
        cdc_id_ops(&out),
        vec![(3, "insert".to_string())],
        "streamed parquet must hold exactly the post-snapshot change (not just a count of 1)"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_column_overrides_apply_like_batch() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_ovr");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, bigu BIGINT UNSIGNED)"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, 18446744073709551615)"
    ))
    .unwrap();

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    let cdc_rig = Rig::mysql_cdc(&tbl)
        .export_line("columns: { bigu: \"decimal(20,0)\" }")
        .checkpoint_path(ckpt.clone())
        .dest_path(cdc_out.clone());
    let batch_rig = Rig::mysql_batch(&tbl)
        .export_named(&format!("{tbl}_batch"))
        .source_url(MYSQL_CDC_URL)
        .export_line("columns: { bigu: \"decimal(20,0)\" }")
        .dest_path(batch_out.clone());
    run_rivet_ok(&cdc_rig.config_path());
    run_rivet_ok(&batch_rig.config_path());

    let fields: std::collections::HashMap<_, _> = parquet_fields(&cdc_out).into_iter().collect();
    assert_eq!(
        fields.get("bigu"),
        Some(&arrow::datatypes::DataType::Decimal128(20, 0)),
        "the override must reach the CDC schema"
    );
    assert_cdc_matches_batch(&cdc_out, &batch_out);
}

/// A `float64` override on a MySQL DECIMAL reads the binlog's decimal text as the server's own DOUBLE, in CDC and batch alike.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn mysql_cdc_float_override_on_a_decimal_matches_the_servers_double_and_batch() {
    use arrow::array::{Array, Float64Array, Int32Array};
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_f64dec");
    let mut c = conn();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, amount DECIMAL(10,2))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());
    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, 12345.67), (2, -0.10), (3, 0.05), (4, NULL)"
    ))
    .unwrap();
    let want: Vec<(i32, Option<f64>)> = c
        .query(format!(
            "SELECT id, CAST(amount AS DOUBLE) FROM {tbl} ORDER BY id"
        ))
        .unwrap();

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    Rig::mysql_cdc(&tbl)
        .export_line("columns: { amount: float64 }")
        .checkpoint_path(ckpt)
        .dest_path(cdc_out.clone())
        .run_ok();
    Rig::mysql_batch(&tbl)
        .export_named(&format!("{tbl}_batch"))
        .source_url(MYSQL_CDC_URL)
        .export_line("columns: { amount: float64 }")
        .dest_path(batch_out.clone())
        .run_ok();

    let b = read_one_batch(&cdc_out);
    let ids = b
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let amounts = b
        .column_by_name("amount")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("the override makes the CDC column Float64");
    let mut got: Vec<(i32, Option<f64>)> = (0..b.num_rows())
        .map(|i| {
            (
                ids.value(i),
                (!amounts.is_null(i)).then(|| amounts.value(i)),
            )
        })
        .collect();
    got.sort_by_key(|r| r.0);
    assert_eq!(
        got, want,
        "CDC values must equal MySQL's own CAST(amount AS DOUBLE)"
    );
    assert_cdc_matches_batch(&cdc_out, &batch_out);
}

/// A bare PostgreSQL `numeric` arrives on CDC as the server's exact text, labelled server_text.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_bare_numeric_is_delivered_as_labelled_server_text() {
    use arrow::array::{Array, Int64Array, StringArray};
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_bare_num");
    let slot = unique_name("rivet_bare_num_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, n NUMERIC)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, 1.50), (2, -0.000001234),
           (3, 123456789012345678901234567890.123456789012), (4, 'NaN'), (5, NULL)"
    ))
    .unwrap();
    let want: Vec<(i64, Option<String>)> = c
        .query(&format!("SELECT id, n::text FROM {tbl} ORDER BY id"), &[])
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();

    let out = d.path().join("cdc");
    std::fs::create_dir_all(&out).unwrap();
    let said = Rig::pg_cdc(&tbl, &slot)
        .dest_path(out.clone())
        .run_ok_capture();
    assert!(
        !said.contains("fidelity="),
        "a server-text column is not a policy violation: {said}"
    );

    let b = read_one_batch(&out);
    let field = b.schema().field_with_name("n").unwrap().clone();
    assert_eq!(field.data_type(), &arrow::datatypes::DataType::Utf8);
    assert_eq!(
        field.metadata().get("rivet.text_form").map(String::as_str),
        Some("server_text")
    );
    assert_eq!(
        field.metadata().get("rivet.fidelity").map(String::as_str),
        Some("logical_string")
    );
    let ids = b
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ns = b
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut got: Vec<(i64, Option<String>)> = (0..b.num_rows())
        .map(|i| {
            (
                ids.value(i),
                (!ns.is_null(i)).then(|| ns.value(i).to_string()),
            )
        })
        .collect();
    got.sort_by_key(|r| r.0);
    assert_eq!(got, want, "each value is the server's own n::text");
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_multi_table_stream_uses_one_slot_and_resumes() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let t1 = unique_name("rivet_cdc_ma");
    let t2 = unique_name("rivet_cdc_mb");
    let slot = unique_name("rivet_multi_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    for t in [&t1, &t2] {
        c.batch_execute(&format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"
        ))
        .unwrap();
    }
    let (_g1, _g2) = (
        PgTable::adopt_on(POSTGRES_CDC_URL, t1.clone()),
        PgTable::adopt_on(POSTGRES_CDC_URL, t2.clone()),
    );

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&t1, &slot)
        .tables(&[&t1, &t2])
        .export_named("app_cdc")
        .dest_path(out.clone());
    let cfg = rig.config_path();

    run_rivet_ok(&cfg);
    let n: i64 = c
        .query_one(
            "SELECT count(*)::bigint FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert_eq!(n, 1, "two tables ride ONE slot");

    c.execute(&format!("INSERT INTO {t1} VALUES (1,10),(2,20)"), &[])
        .unwrap();
    c.execute(&format!("INSERT INTO {t2} VALUES (7,70)"), &[])
        .unwrap();
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out.join(&t1)), 2, "table 1 sub-prefix");
    assert_eq!(manifest_rows(&out.join(&t2)), 1, "table 2 sub-prefix");
    assert_eq!(
        cdc_id_ops(&out.join(&t1)),
        vec![(1, "insert".to_string()), (2, "insert".to_string())],
        "table 1 parquet holds exactly its own changes (routing, not just counts)"
    );
    assert_eq!(
        cdc_id_ops(&out.join(&t2)),
        vec![(7, "insert".to_string())],
        "table 2 parquet holds exactly its own changes (routing, not just counts)"
    );

    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out.join(&t1)), 0, "no re-read for table 1");
    assert_eq!(manifest_rows(&out.join(&t2)), 0, "no re-read for table 2");
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_multi_table_check_target_emits_one_resolver_document_per_table() {
    use postgres::NoTls;
    let t1 = unique_name("rivet_cdc_tr_a");
    let t2 = unique_name("rivet_cdc_tr_b");
    let slot = unique_name("rivet_tr_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    for t in [&t1, &t2] {
        c.batch_execute(&format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"
        ))
        .unwrap();
    }
    let (_g1, _g2) = (
        PgTable::adopt_on(POSTGRES_CDC_URL, t1.clone()),
        PgTable::adopt_on(POSTGRES_CDC_URL, t2.clone()),
    );

    let rig = Rig::pg_cdc(&t1, &slot)
        .tables(&[&t1, &t2])
        .export_named("app_cdc")
        .export_line(&format!("columns: {{ \"{t2}.v\": text }}"));
    let out = rig.cli(&["check", "--target", "bigquery", "--json"]);
    assert!(
        out.status.success(),
        "check --target must succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);

    let docs: Vec<serde_json::Value> = stdout
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.get("columns").is_some())
        .collect();
    assert_eq!(
        docs.len(),
        2,
        "one resolver document per captured table — got {} from:\n{stdout}",
        docs.len()
    );
    let by_table = |t: &str| {
        docs.iter()
            .find(|d| d["table"] == serde_json::json!(t))
            .unwrap_or_else(|| panic!("no resolver document for table {t} in:\n{stdout}"))
    };
    for t in [&t1, &t2] {
        let d = by_table(t);
        assert_eq!(
            d["export"],
            serde_json::json!("app_cdc"),
            "each document still names the EXPORT the operator wrote"
        );
        let cols: Vec<&str> = d["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["column"].as_str().unwrap())
            .collect();
        assert_eq!(cols, vec!["id", "v"], "table {t} columns");
    }
    let target_of = |t: &str| {
        by_table(t)["columns"].as_array().unwrap()[1]["target_type"]
            .as_str()
            .unwrap()
            .to_string()
    };
    assert_eq!(target_of(&t1), "INT64", "bare table keeps its source type");
    assert_eq!(
        target_of(&t2),
        "STRING",
        "the `<table>.<column>` override applies to its own table only — the \
         same precedence the capture uses"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_multi_table_stream_one_binlog_connection_and_resumes() {
    let d = tempfile::tempdir().unwrap();
    let ta = unique_name("cdc_multi_a");
    let tb = unique_name("cdc_multi_b");
    let mut c = conn();
    for t in [&ta, &tb] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"))
            .unwrap();
    }
    let (_g1, _g2) = (Table(ta.clone()), Table(tb.clone()));

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mysql_cdc(&ta)
        .tables(&[&ta, &tb])
        .export_named("app_cdc")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();

    run_rivet_ok(&cfg);
    assert!(ckpt.exists(), "idle first run pins the shared checkpoint");

    c.query_drop(format!("INSERT INTO {ta} VALUES (1,10),(2,20)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tb} VALUES (7,70)"))
        .unwrap();
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out.join(&ta)), 2);
    assert_eq!(manifest_rows(&out.join(&tb)), 1);
    assert_eq!(
        cdc_id_ops(&out.join(&ta)),
        vec![(1, "insert".to_string()), (2, "insert".to_string())],
        "table a parquet holds exactly its own changes (routing, not just counts)"
    );
    assert_eq!(
        cdc_id_ops(&out.join(&tb)),
        vec![(7, "insert".to_string())],
        "table b parquet holds exactly its own changes (routing, not just counts)"
    );

    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out.join(&ta)), 0, "resume: no re-read");
    assert_eq!(manifest_rows(&out.join(&tb)), 0, "resume: no re-read");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + fake-gcs"]
fn cdc_multi_table_to_gcs_lands_per_table_prefixes() {
    let d = tempfile::tempdir().unwrap();
    let ta = unique_name("cdc_gcs_a");
    let tb = unique_name("cdc_gcs_b");
    let mut c = conn();
    for t in [&ta, &tb] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"))
            .unwrap();
    }
    let (_g1, _g2) = (Table(ta.clone()), Table(tb.clone()));

    let bucket = "rivet-qa-cdc-gcs";
    ensure_gcs_bucket(bucket);
    let prefix = unique_name("cdcgcs");
    let ckpt = d.path().join("cdc.ckpt");
    let rig = Rig::mysql_cdc(&ta)
        .tables(&[&ta, &tb])
        .export_named("app_cdc")
        .checkpoint_path(ckpt.clone())
        .dest_gcs(bucket, &prefix, FAKE_GCS_ENDPOINT);
    let cfg = rig.config_path();
    let root = format!("{prefix}/app_cdc");

    run_rivet_ok(&cfg);
    c.query_drop(format!("INSERT INTO {ta} VALUES (1,10),(2,20)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tb} VALUES (7,70)"))
        .unwrap();
    run_rivet_ok(&cfg);

    let body = reqwest::blocking::get(format!(
        "{FAKE_GCS_ENDPOINT}/storage/v1/b/{bucket}/o?prefix={prefix}"
    ))
    .expect("gcs list request")
    .text()
    .expect("gcs list body");
    let json: serde_json::Value = serde_json::from_str(&body).expect("gcs list json");
    let keys: Vec<&str> = json["items"]
        .as_array()
        .map(|items| items.iter().filter_map(|o| o["name"].as_str()).collect())
        .unwrap_or_default();

    for t in [&ta, &tb] {
        assert!(
            keys.iter()
                .any(|k| *k == format!("{root}/{t}/manifest.json")),
            "per-table manifest key missing for {t}; keys: {keys:?}"
        );
        assert!(
            keys.iter().any(|k| *k == format!("{root}/{t}/_SUCCESS")),
            "per-table _SUCCESS key missing for {t}; keys: {keys:?}"
        );
        assert!(
            keys.iter()
                .any(|k| k.starts_with(&format!("{root}/{t}/cdc-")) && k.ends_with(".parquet")),
            "per-table part key missing for {t}; keys: {keys:?}"
        );
        assert!(
            !keys.iter().any(|k| k.contains(&format!("{t}cdc-"))
                || k.contains(&format!("{t}manifest"))
                || k.contains(&format!("{t}_SUCCESS"))),
            "mangled flat key (missing '/') detected for {t}; keys: {keys:?}"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_resume_from_missing_binlog_fails_loudly_not_silently() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_1236");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    let ckpt = d.path().join("cdc.ckpt");
    std::fs::write(&ckpt, r#"{"file":"binlog.999999","pos":4}"#).unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();

    let out_dir = d.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let cfg = cdc_config(&d, &tbl, &ckpt, &out_dir);
    let out = run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], &[]);
    assert!(
        !out.status.success(),
        "resuming from a purged/missing binlog must FAIL, not silently re-anchor"
    );
    assert!(
        !out_dir.join("_SUCCESS").exists(),
        "no _SUCCESS may be written for the failed run"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_corrupt_checkpoint_fails_loud_not_silently_absent() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_corrupt_bin");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    let ckpt = d.path().join("cdc.ckpt");
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out1));
    assert!(ckpt.exists(), "run 1 pins a checkpoint");

    std::fs::write(&ckpt, b"{ not valid json at all").unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 100)"))
        .unwrap();

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let res = run_rivet_env(
        &[
            "run",
            "--config",
            cdc_config(&d, &tbl, &ckpt, &out2).to_str().unwrap(),
        ],
        &[],
    );
    assert!(
        !res.status.success(),
        "a corrupt checkpoint must fail the run, not silently re-anchor and skip changes"
    );
    let stderr = String::from_utf8_lossy(&res.stderr);
    assert!(
        stderr.contains("corrupt or truncated"),
        "the failure must name the corrupt checkpoint, got:\n{stderr}"
    );
    assert_refused(
        &res,
        Refused::by_code("RIVET_SOURCE_CDC_CHECKPOINT_INVALID", 5),
    );
    assert!(
        stderr.contains(REBASELINE_REMEDY),
        "the refusal ends with the re-baseline remedy:\n{stderr}"
    );
    assert!(
        !out2.join("_SUCCESS").exists(),
        "no _SUCCESS may be written for the failed run"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_vanished_slot_with_checkpoint_fails_loudly_not_recreates() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_gone");
    let slot = unique_name("rivet_gone_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let rig_for = |out: &std::path::Path| {
        Rig::pg_cdc(&tbl, &slot)
            .checkpoint_path(ckpt.clone())
            .dest_path(out.to_path_buf())
    };
    let rig1 = rig_for(&out1);
    run_rivet_ok(&rig1.config_path());
    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10)"), &[])
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let rig2 = rig_for(&out2);
    run_rivet_ok(&rig2.config_path());
    assert_eq!(manifest_rows(&out2), 1, "run 2 captured the change");
    assert!(ckpt.exists(), "checkpoint persisted");

    c.execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (2,20)"), &[])
        .unwrap();

    let out3 = d.path().join("out3");
    std::fs::create_dir_all(&out3).unwrap();
    let rig3 = rig_for(&out3);
    let out = rig3.run_args(&[]);
    assert!(
        !out.status.success(),
        "a vanished slot with an existing checkpoint must fail the run, not silently re-create"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    let expected = format!(
        "pg cdc: slot '{slot}' is missing but the checkpoint file holds a position from a prior \
         run — the slot was dropped or invalidated, and the changes since then are no longer in \
         the log. {REBASELINE_REMEDY}"
    );
    assert!(
        stderr.contains(&expected),
        "the refusal must name the checkpoint it found, and only that:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_corrupt_checkpoint_fails_loud_not_silently_absent() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgcorrupt");
    let slot = unique_name("rivet_corrupt_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    let ckpt = d.path().join("cdc.ckpt");
    let rig_for = |out: &std::path::Path| {
        Rig::pg_cdc(&tbl, &slot)
            .checkpoint_path(ckpt.clone())
            .dest_path(out.to_path_buf())
    };

    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    let rig1 = rig_for(&out1);
    run_rivet_ok(&rig1.config_path());

    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10)"), &[])
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let rig2 = rig_for(&out2);
    run_rivet_ok(&rig2.config_path());
    assert_eq!(manifest_rows(&out2), 1, "run 2 captures the change");
    assert!(ckpt.exists(), "run 2 pins a checkpoint");

    std::fs::write(&ckpt, b"{ truncated").unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (2,20)"), &[])
        .unwrap();

    let out3 = d.path().join("out3");
    std::fs::create_dir_all(&out3).unwrap();
    let rig3 = rig_for(&out3);
    let res = rig3.run_args(&[]);
    assert!(
        !res.status.success(),
        "a corrupt checkpoint must fail the run, not be read as absent and re-anchor"
    );
    let stderr = String::from_utf8_lossy(&res.stderr);
    assert!(
        stderr.contains("corrupt or truncated"),
        "the failure must name the corrupt checkpoint, got:\n{stderr}"
    );
    assert_refused(
        &res,
        Refused::by_code("RIVET_SOURCE_CDC_CHECKPOINT_INVALID", 5),
    );
    assert!(
        stderr.contains(REBASELINE_REMEDY),
        "the refusal ends with the re-baseline remedy:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn doctor_reports_cdc_slot_health_and_flags_foreign_inactive_slots() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_doc");
    let own_slot = unique_name("rivet_doc_slot");
    let _own_slot = Slot::new(own_slot.clone());
    let foreign_slot = unique_name("abandoned_tool_slot");
    let _foreign = Slot::new(foreign_slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&foreign_slot],
    )
    .unwrap();

    let out_dir = d.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let cfg = pg_cdc_config(&d, &tbl, &own_slot, &out_dir);
    let out = run_rivet(&["doctor", "--config", cfg.to_str().unwrap(), "--json"]);
    let report: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("doctor --json output");
    let checks = report["checks"].as_array().expect("checks array");

    let own = checks
        .iter()
        .find(|c| c["name"].as_str().unwrap_or("").contains(&own_slot))
        .expect("own-slot check present");
    assert_eq!(own["ok"], true, "absent slot is healthy: {own}");

    let foreign = checks
        .iter()
        .find(|c| {
            c["name"]
                .as_str()
                .unwrap_or("")
                .contains("other inactive slots")
        })
        .expect("foreign-slots check present");
    assert!(
        foreign["detail"]
            .as_str()
            .unwrap_or("")
            .contains(&foreign_slot),
        "the abandoned slot must be named: {foreign}"
    );
    assert_eq!(
        report["all_ok"], true,
        "small foreign slot must not fail doctor"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_idle_first_run_then_change_is_captured_not_skipped() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgidle");
    let slot = unique_name("rivet_idle_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out1));
    assert_eq!(manifest_rows(&out1), 0, "idle run 1 drains nothing");

    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10)"), &[])
        .unwrap();

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out2));
    assert_eq!(
        manifest_rows(&out2),
        1,
        "the change between an idle run and the next run must be captured, not skipped"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(1, "insert".to_string())],
        "the captured parquet must hold exactly THE change (a count of 1 could be a wrong row)"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_crash_after_flush_before_ack_does_not_advance_the_slot() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgcrash");
    let slot = unique_name("rivet_crash_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"), &[])
        .unwrap();

    let crash_out = d.path().join("crash");
    std::fs::create_dir_all(&crash_out).unwrap();
    let crashed = run_rivet_env(
        &[
            "run",
            "--config",
            pg_cdc_config(&d, &tbl, &slot, &crash_out).to_str().unwrap(),
        ],
        &[("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")],
    );
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "the slot stayed put across the crash → resume re-reads both (no loss)"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(1, "insert".to_string()), (2, "insert".to_string())],
        "the re-read parquet must hold exactly the un-acked changes"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_crash_in_a_re_drain_pass_stays_at_least_once() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let a = unique_name("rivet_cdc_rdcap");
    let b = unique_name("rivet_cdc_rdforgn");
    let slot = unique_name("rivet_rd_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {a}; DROP TABLE IF EXISTS {b}; \
         CREATE TABLE {a} (id BIGINT PRIMARY KEY, v INT); \
         CREATE TABLE {b} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _ta = PgTable::adopt_on(POSTGRES_CDC_URL, a.clone());
    let _tb = PgTable::adopt_on(POSTGRES_CDC_URL, b.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(
        &format!("INSERT INTO {b} SELECT g, g FROM generate_series(1, 100) g"),
        &[],
    )
    .unwrap();
    c.execute(
        &format!("INSERT INTO {a} SELECT g, g FROM generate_series(0, 11) g"),
        &[],
    )
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&a, &slot)
        .cdc("rollover: 5")
        .dest_path(out.clone());
    let crashed = rig.run_args_env(&[], &[("RIVET_TEST_PANIC_AT", "cdc_after_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    let rig2 = Rig::pg_cdc(&a, &slot).dest_path(out.clone());
    run_rivet_ok(&rig2.config_path());

    let ids = duckdb_dir_parquet_i64(&out, "id");
    let distinct: std::collections::BTreeSet<i64> = ids.iter().copied().collect();
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        distinct, want,
        "A's 12-row transaction must survive the mid-re-drain crash whole — got {:?} \
         (a pass-1 ack that overshot into A's un-read transaction lost part of it)",
        distinct
    );
    assert_eq!(
        ids.len(),
        12,
        "no duplication across the crash + resume — got {} rows for 12 distinct ids",
        ids.len()
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_large_transaction_is_atomic_across_a_mid_flush_crash() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgatomic");
    let slot = unique_name("rivet_atomic_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(
        &format!("INSERT INTO {tbl} SELECT g, g FROM generate_series(0, 11) g"),
        &[],
    )
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .cdc("rollover: 5")
        .dest_path(out.clone());
    let crashed = rig.run_args_env(&[], &[("RIVET_TEST_PANIC_AT", "cdc_after_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    let rig2 = Rig::pg_cdc(&tbl, &slot).dest_path(out.clone());
    run_rivet_ok(&rig2.config_path());

    let got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        got,
        want,
        "the 12-row transaction must survive the mid-flush crash whole — got {} ids \
         (a mid-transaction ack advanced the slot past the commit and lost the tail)",
        got.len()
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn roast_mysql_cdc_large_transaction_is_atomic_across_a_mid_flush_crash() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_myatomic");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    let _drop = Table(tbl.clone());

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mysql_cdc(&tbl)
        .cdc("rollover: 5")
        .dest_path(out.clone());
    rig.run_ok();
    let vals = (0..12)
        .map(|i| format!("({i},{i})"))
        .collect::<Vec<_>>()
        .join(",");
    c.query_drop(format!("INSERT INTO {tbl} VALUES {vals}"))
        .unwrap();

    let crashed = rig.run_args_env(&[], &[("RIVET_TEST_PANIC_AT", "cdc_after_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    run_rivet_ok(&rig.config_path());

    let got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        got,
        want,
        "the 12-row transaction must survive the mid-flush crash whole — got {} ids \
         (a mid-transaction ack advanced the binlog checkpoint past the commit and lost the tail)",
        got.len()
    );
}

/// Rows of the spilled transaction in the two crash-mid-tail tests.
const SPILL_CRASH_ROWS: i64 = 12;

/// Run 1 spills the transaction's tail and crashes at the first ack that follows a roll with parts; returns its stderr.
fn crash_with_a_spilled_tail(rig: &Rig) -> String {
    let crashed = rig.run_with_envs(&[
        ("RIVET_CDC_MAX_TX_ROWS", "4"),
        ("RIVET_CDC_SPILL_DIR", "1"),
        ("RIVET_TEST_PANIC_AT", "cdc_after_ack_of_parts"),
    ]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );
    let log = String::from_utf8_lossy(&crashed.stderr).to_string();
    assert!(
        log.contains("passed the in-memory cap") && log.contains(" from disk ("),
        "the fixture is inert unless run 1 SPILLED and replayed part of the tail \
         before the crash. stderr: {log}"
    );
    log
}

/// The union of both runs' parts (`ids`, read by DuckDB) must hold the transaction exactly once.
fn assert_spilled_transaction_whole_once(ids: &[i64]) {
    let got: std::collections::BTreeSet<i64> = ids.iter().copied().collect();
    let want: std::collections::BTreeSet<i64> = (0..SPILL_CRASH_ROWS).collect();
    assert_eq!(
        got, want,
        "every row of the spilled transaction must survive the crash — a missing \
         row means the sink checkpointed and acked past the commit while the rest \
         of the tail was still in the dead process's spill file"
    );
    assert_eq!(
        ids.len(),
        SPILL_CRASH_ROWS as usize,
        "no row may arrive twice across the crash + resume"
    );
}

/// A crash while a spilled PostgreSQL transaction is replayed must lose none of it on resume.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_a_crash_mid_spilled_tail_loses_no_transaction_on_resume() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_spillcrash_pg");
    let slot = unique_name("rivet_spillcrash_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(
        &format!(
            "INSERT INTO {tbl} SELECT g, g FROM generate_series(0, {}) g",
            SPILL_CRASH_ROWS - 1
        ),
        &[],
    )
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .cdc("rollover: 5")
        .dest_path(out.clone());
    crash_with_a_spilled_tail(&rig);
    let resumed =
        rig.run_with_envs(&[("RIVET_CDC_MAX_TX_ROWS", "4"), ("RIVET_CDC_SPILL_DIR", "1")]);
    assert!(
        resumed.status.success(),
        "the resume run must succeed: {}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert_spilled_transaction_whole_once(&duckdb_dir_parquet_i64(&out, "id"));
}

/// A crash while a spilled MySQL transaction is replayed must lose none of it on resume.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn roast_mysql_cdc_a_crash_mid_spilled_tail_loses_no_transaction_on_resume() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_spillcrash_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    let _drop = Table(tbl.clone());

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mysql_cdc(&tbl)
        .cdc("rollover: 5")
        .dest_path(out.clone());
    rig.run_ok();
    let vals = (0..SPILL_CRASH_ROWS)
        .map(|i| format!("({i},{i})"))
        .collect::<Vec<_>>()
        .join(",");
    c.query_drop(format!("INSERT INTO {tbl} VALUES {vals}"))
        .unwrap();

    crash_with_a_spilled_tail(&rig);
    let resumed =
        rig.run_with_envs(&[("RIVET_CDC_MAX_TX_ROWS", "4"), ("RIVET_CDC_SPILL_DIR", "1")]);
    assert!(
        resumed.status.success(),
        "the resume run must succeed: {}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert_spilled_transaction_whole_once(&duckdb_dir_parquet_i64(&out, "id"));
}

fn pg_full_config(d: &tempfile::TempDir, tbl: &str, out: &std::path::Path) -> std::path::PathBuf {
    Rig::pg_batch(&format!("{tbl}_batch"))
        .query(&format!("SELECT * FROM {tbl}"))
        .source_url(POSTGRES_CDC_URL)
        .dest_path(out.to_path_buf())
        .config_in(d.path())
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_column_types_match_batch_export() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgtypes");
    let slot = unique_name("rivet_types_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id int, amount numeric(10,2), \
         meta jsonb, label text, ts timestamp, tstz timestamptz, u uuid)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(
        &format!(
            "INSERT INTO {tbl} VALUES (1, 12.34, '{{\"k\":1}}', 'hi', \
             '2026-06-23 10:00:00', '2026-06-23 10:00:00+00', gen_random_uuid())"
        ),
        &[],
    )
    .unwrap();

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &cdc_out));
    run_rivet_ok(&pg_full_config(&d, &tbl, &batch_out));

    let cdc: std::collections::HashMap<_, _> = parquet_fields(&cdc_out).into_iter().collect();
    let batch: std::collections::HashMap<_, _> = parquet_fields(&batch_out).into_iter().collect();
    for col in ["id", "amount", "meta", "label", "ts", "tstz", "u"] {
        assert_eq!(
            cdc.get(col),
            batch.get(col),
            "column {col}: CDC type must match the batch export (full parity)"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose mysql (binlog ROW + REPLICATION grant)"]
fn cdc_resume_captures_only_new_changes() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_regr");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();

    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out1));
    assert_eq!(
        manifest_rows(&out1),
        2,
        "run 1 should capture the 2 new changes"
    );

    c.query_drop(format!("INSERT INTO {tbl} VALUES (3,30),(4,40)"))
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume must capture exactly the 2 changes since the checkpoint (no gap, no dup)"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(3, "insert".to_string()), (4, "insert".to_string())],
        "the resumed parquet must hold exactly the NEW changes (count 2 cannot tell new-2 from wrong-2)"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql (binlog ROW + REPLICATION grant)"]
fn cdc_run_is_recorded_in_state_db() {
    let tbl = unique_name("rivet_cdc_regr");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20),(3,30)"))
        .unwrap();
    let rig = Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .census_oracle();
    rig.run_ok();
    let out = rig.out_dir();
    let db = out.parent().expect("shared dir").join(".rivet_state.db");

    let sql = rusqlite::Connection::open(&db).expect("state db");

    let (rows, mode): (i64, String) = sql
        .query_row(
            "SELECT total_rows, mode FROM export_metrics WHERE export_name = ?1 ORDER BY rowid DESC LIMIT 1",
            [&tbl],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("export_metrics row for the cdc run");
    assert_eq!(mode, "cdc");
    assert_eq!(rows, 3, "metric total_rows = captured changes");

    let census = rig.row_census();
    assert_eq!(
        census.delivered, 3,
        "the parquet must hold the three changes an independent reader can count, \
         not the three rivet says it wrote: {census:?}"
    );
    assert_eq!(
        census.metrics, census.file_log,
        "and rivet's two ledgers must agree with each other and with the artifacts \
         — a metric that outruns the file log is a run claiming rows no part \
         declares: {census:?}"
    );

    let journal: String = sql
        .query_row(
            "SELECT journal_json FROM run_journal WHERE export_name = ?1 ORDER BY rowid DESC LIMIT 1",
            [&tbl],
            |r| r.get(0),
        )
        .expect("run_journal row for the cdc run");
    assert!(
        journal.contains("RunCompleted") && journal.contains("FileWritten"),
        "cdc journal must carry FileWritten + RunCompleted, got: {journal}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_column_added_mid_stream_is_captured() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgdrift");
    let slot = unique_name("rivet_drift_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    c.execute(&format!("INSERT INTO {tbl} VALUES (1, 10)"), &[])
        .unwrap();
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out1));
    assert!(
        !duckdb_dir_parquet_has_column(&out1, "w"),
        "run 1 predates the added column"
    );

    c.batch_execute(&format!(
        "ALTER TABLE {tbl} ADD COLUMN w TEXT; INSERT INTO {tbl} VALUES (2, 20, 'hello')"
    ))
    .unwrap();

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out2));
    assert!(
        duckdb_dir_parquet_has_column(&out2, "w"),
        "run 2 must re-resolve and pick up the column added between runs"
    );
    assert_eq!(parquet_one_string(&out2, "w"), "hello");
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_until_current_terminates_under_sustained_writes() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pghb");
    let slot = unique_name("rivet_hb_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    for i in 0..30i64 {
        c.execute(&format!("INSERT INTO {tbl} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    let tbl_bg = tbl.clone();
    let _bg = BgWriter::spawn(move |stop| {
        let mut w = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("bg connect");
        let mut i = 10_000i64;
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = w.execute(&format!("INSERT INTO {tbl_bg} VALUES ({i},{i})"), &[]);
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
    });

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let elapsed = run_rivet_bounded(
        &pg_cdc_config(&d, &tbl, &slot, &out),
        std::time::Duration::from_secs(30),
    );

    assert!(
        elapsed.is_some(),
        "until_current must terminate under sustained writes (killed at the 30s ceiling)"
    );
    let ids: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
    for i in 0..30 {
        assert!(
            ids.contains(&i),
            "backlog id {i} must be captured, got {} ids",
            ids.len()
        );
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_until_current_terminates_under_sustained_writes() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_myhb");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();

    let vals: Vec<String> = (0..30).map(|i| format!("({i},{i})")).collect();
    c.query_drop(format!("INSERT INTO {tbl} VALUES {}", vals.join(",")))
        .unwrap();

    let tbl_bg = tbl.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
        let mut w = conn();
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = w.query_drop(format!("INSERT INTO {tbl_bg} VALUES ({i},{i})"));
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
    });

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let elapsed = run_rivet_bounded(
        &cdc_config(&d, &tbl, &ckpt, &out),
        std::time::Duration::from_secs(30),
    );
    bg.stop();

    assert!(
        elapsed.is_some(),
        "until_current must terminate under sustained writes (killed at the 30s ceiling)"
    );
    let ids: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
    for i in 0..30 {
        assert!(
            ids.contains(&i),
            "backlog id {i} must be captured, got {} ids",
            ids.len()
        );
    }
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_until_current_open_bound_two_runs_lose_nothing() {
    use postgres::NoTls;
    let tbl = unique_name("rivet_cdc_pgob");
    let slot = unique_name("rivet_ob_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    for i in 0..30i64 {
        c.execute(&format!("INSERT INTO {tbl} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    let tbl_bg = tbl.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
        let mut w = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("bg connect");
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            let vals: Vec<String> = (i..i + 10).map(|k| format!("({k},{k})")).collect();
            let _ = w.batch_execute(&format!("INSERT INTO {tbl_bg} VALUES {}", vals.join(",")));
            i += 10;
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });

    let rig = Rig::pg_cdc(&tbl, &slot).cdc("rollover: 5");
    let cfg = rig.config_path();
    let elapsed = run_rivet_bounded(&cfg, std::time::Duration::from_secs(30));
    bg.stop();
    assert!(
        elapsed.is_some(),
        "run 1 must terminate at the open-time WAL bound under sustained writes \
         (killed at the 30s ceiling ⇒ the drain chased the moving head)"
    );

    let rig2 = Rig::pg_cdc(&tbl, &slot).dest_path(rig.out_dir());
    let elapsed2 = run_rivet_bounded(&rig2.config_path(), std::time::Duration::from_secs(60));
    assert!(
        elapsed2.is_some(),
        "run 2 (no writers) must drain the tail and exit"
    );

    let got: std::collections::BTreeSet<i64> = duckdb_dir_parquet_i64(&rig.out_dir(), "id")
        .into_iter()
        .collect();
    let want: std::collections::BTreeSet<i64> = c
        .query(&format!("SELECT id FROM {tbl}"), &[])
        .unwrap()
        .iter()
        .map(|r| r.get::<_, i64>(0))
        .collect();
    assert_eq!(
        got, want,
        "run1 ∪ run2 must hold exactly the source's committed ids — the bound \
         defers the tail to run 2, never drops it"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn roast_mysql_until_current_open_bound_two_runs_lose_nothing() {
    let tbl = unique_name("rivet_cdc_myob");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    let rig = Rig::mysql_cdc(&tbl);
    rig.pin_binlog_here();

    let vals: Vec<String> = (0..30).map(|i| format!("({i},{i})")).collect();
    c.query_drop(format!("INSERT INTO {tbl} VALUES {}", vals.join(",")))
        .unwrap();

    let tbl_bg = tbl.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
        let mut w = conn();
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = w.query_drop(format!("INSERT INTO {tbl_bg} VALUES ({i},{i})"));
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    });

    let cfg = rig.config_path();
    let elapsed = run_rivet_bounded(&cfg, std::time::Duration::from_secs(30));
    bg.stop();
    assert!(
        elapsed.is_some(),
        "run 1 must terminate under sustained writes (NON_BLOCK EOF; killed at 30s)"
    );

    let elapsed2 = run_rivet_bounded(&cfg, std::time::Duration::from_secs(60));
    assert!(
        elapsed2.is_some(),
        "run 2 (no writers) must drain the tail and exit"
    );

    let got: std::collections::BTreeSet<i64> = duckdb_dir_parquet_i64(&rig.out_dir(), "id")
        .into_iter()
        .collect();
    let want: std::collections::BTreeSet<i64> = c
        .query_map(format!("SELECT id FROM {tbl}"), |id: i64| id)
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(
        got, want,
        "run1 ∪ run2 must hold exactly the source's committed ids — the bound \
         defers the tail to run 2, never drops it"
    );
}

/// A throwaway PostgreSQL database on the CDC server.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_refuses_a_partitioned_parent_before_acking_the_slot() {
    let cdc_db = CdcDb::new("cdc_part");
    let slot = unique_name("rivet_part_slot");
    let _slot = Slot::new(slot.clone());
    let parent = unique_name("rivet_cdc_par").to_lowercase();
    let part = format!("{parent}_2026_01");
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {parent} (id BIGINT NOT NULL, ts DATE NOT NULL, v INT) \
         PARTITION BY RANGE (ts); \
         CREATE TABLE {part} PARTITION OF {parent} \
         FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(
        &format!("INSERT INTO {parent} VALUES (1,'2026-01-05',10),(2,'2026-01-06',20)"),
        &[],
    )
    .unwrap();
    let source_rows: i64 = c
        .query_one(&format!("SELECT count(*) FROM {parent}"), &[])
        .unwrap()
        .get(0);
    assert_eq!(source_rows, 2, "fixture seeded 2 rows through the parent");
    let before: String = c
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0);

    let wrong = Rig::pg_cdc(&format!("public.{parent}"), &slot).source_url(cdc_db.url());
    let out = run_rivet(&["run", "--config", wrong.config_path().to_str().unwrap()]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !out.status.success(),
        "capturing a partitioned parent must fail at open, not ship a 0-row \
         success — output:\n{said}"
    );
    assert!(
        said.contains(&part),
        "the refusal must name the PARTITION `{part}` — listing the partitions is \
         the remediation, and a user who could guess that would not have hit this. \
         Output:\n{said}"
    );

    let moved: bool = c
        .query_one(
            &format!(
                "SELECT confirmed_flush_lsn <> '{before}'::pg_lsn \
                 FROM pg_replication_slots WHERE slot_name = $1"
            ),
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert!(
        !moved,
        "the refused run acked the slot anyway — the 2 changes are unreachable \
         now and the guard bought nothing"
    );

    let right = Rig::pg_cdc(&format!("public.{part}"), &slot).source_url(cdc_db.url());
    run_rivet_ok(&right.config_path());
    let captured: usize = right
        .read_declared_parts()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        captured, source_rows as usize,
        "the corrected run must recover all {source_rows} changes from the \
         un-acked slot — that is what makes the refusal a delay, not a loss"
    );

    let snapshot = Rig::pg_batch(&format!("public.{parent}"))
        .source_url(cdc_db.url())
        .mode("full");
    let snapshot_rows: usize = snapshot.run_and_read().iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        snapshot_rows, source_rows as usize,
        "`mode: full` on the partitioned parent must read every row — it is the \
         second remediation the refusal names, and a message may only promise \
         what a test has run"
    );
}

/// The manifest-scoped read-back must disagree with a glob.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn the_cdc_reader_reads_the_manifest_declared_parts_not_the_directory() {
    let cdc_db = CdcDb::new("cdc_declared");
    let tbl = unique_name("rivet_cdc_decl").to_lowercase();
    let slot = unique_name("rivet_decl_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    for i in 1..=2i64 {
        c.execute(&format!("INSERT INTO {tbl} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    let rig = Rig::pg_cdc(&format!("public.{tbl}"), &slot)
        .source_url(cdc_db.url())
        .cdc("rollover: 1");
    run_rivet_ok(&rig.config_path());
    let out = rig.out_dir();

    let before = cdc_id_ops(&out);
    assert_eq!(
        before.len(),
        2,
        "the fixture must produce TWO parts' worth of rows, or the manifest edit \
         below cannot express under-declaration: {before:?}"
    );
    let files_on_disk = std::fs::read_dir(&out)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .count();
    assert_eq!(files_on_disk, 2, "two committed parts on disk");

    for entry in std::fs::read_dir(&out).unwrap().flatten() {
        let path = entry.path();
        let is_manifest = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("manifest") && n.ends_with(".json"));
        if !is_manifest {
            continue;
        }
        let mut doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        if let Some(parts) = doc.get_mut("parts").and_then(|p| p.as_array_mut())
            && parts.len() > 1
        {
            parts.pop();
        }
        std::fs::write(&path, doc.to_string()).unwrap();
    }

    let after = cdc_id_ops(&out);
    assert!(
        after.len() < before.len(),
        "the reader must follow the MANIFEST: both parquet files are still on disk, \
         so a directory scan reads {} rows either way and reports delivery no \
         consumer would ever see. Got {after:?} after under-declaring, {before:?} \
         before",
        before.len()
    );
    assert_eq!(after.len(), 1, "exactly the still-declared part: {after:?}");
}

/// A configured MySQL VIEW must be refused at open — the binlog can never name it.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn roast_mysql_cdc_refuses_a_view_whose_binlog_identity_is_the_base_table() {
    let d = tempfile::tempdir().unwrap();
    let base = unique_name("cdc_vbase");
    let view = format!("{base}_v");
    let mut c = conn();
    c.query_drop(format!("DROP VIEW IF EXISTS {view}")).unwrap();
    c.query_drop(format!("DROP TABLE IF EXISTS {base}"))
        .unwrap();
    c.query_drop(format!("CREATE TABLE {base} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    c.query_drop(format!("CREATE VIEW {view} AS SELECT * FROM {base}"))
        .unwrap();
    let _g = Table(base.clone());
    struct ViewGuard(String);
    impl Drop for ViewGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(MYSQL_CDC_URL).unwrap()) {
                use mysql::prelude::Queryable as _;
                let _ = c.query_drop(format!("DROP VIEW IF EXISTS {}", self.0));
            }
        }
    }
    let _vg = ViewGuard(view.clone());

    let msg = Rig::mysql_cdc(&view)
        .checkpoint_path(d.path().join("v.ckpt"))
        .dest_path(d.path().join("out"))
        .run_expect_fail();
    assert!(
        msg.contains("VIEW") && msg.contains("base table"),
        "the refusal must name what the relation IS and what to capture instead — \
         shipping a 0-row success past an advanced checkpoint is the alternative. \
         Got: {msg}"
    );
    assert!(
        msg.contains("NOT lost"),
        "MySQL binlog retention is reader-independent, so this is recoverable by \
         deleting the checkpoint; an operator told to re-snapshot would do work they \
         do not need. Got: {msg}"
    );
    assert!(
        !msg.contains("REPLICATION SLAVE") && !msg.contains("binlog_format"),
        "a routing/config refusal must not be prefixed with a permissions hint — an \
         operator would go read the grants docs for a problem that is not there. \
         Got: {msg}"
    );

    let base_rig = || {
        Rig::mysql_cdc(&base)
            .checkpoint_path(d.path().join("b.ckpt"))
            .dest_path(d.path().join("out_base"))
    };
    base_rig().run_ok();
    c.query_drop(format!("INSERT INTO {base} VALUES (1,10),(2,20),(3,30)"))
        .unwrap();
    let rows: usize = base_rig().run_and_read().iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 3,
        "the guard must refuse only what the binlog cannot name — a base table has \
         to keep working, or the fix is worse than the defect"
    );
}

/// The three routing hazards an adversarial pass found in the FIRST version of this guard.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_refuses_a_config_whose_resolved_identity_routing_cannot_match() {
    let cdc_db = CdcDb::new("cdc_ident");
    let slot = unique_name("rivet_ident_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(
        "CREATE TABLE \"MixedCase\" (id int, v text); \
         CREATE TABLE mixedcase (id int, other_col text, extra text); \
         CREATE UNLOGGED TABLE unlg (id int primary key, v text)",
    )
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    for (cfg, must_say) in [
        ("MixedCase", "public.mixedcase"),
        ("rivet.public.mixedcase", "public.mixedcase"),
        ("public.unlg", "SET LOGGED"),
    ] {
        let rig = Rig::pg_cdc(cfg, &slot).source_url(cdc_db.url());
        let out = run_rivet(&["run", "--config", rig.config_path().to_str().unwrap()]);
        let said = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            !out.status.success(),
            "`{cfg}` must be refused — it either captures nothing or writes rows \
             under another table's schema, silently. Output:\n{said}"
        );
        assert!(
            said.contains(must_say),
            "the refusal for `{cfg}` must name `{must_say}` — that is the whole \
             remediation. Output:\n{said}"
        );
        assert!(
            !said.contains("wal_level=logical and a role"),
            "a routing refusal must not be prefixed with the permissions hint. \
             Output:\n{said}"
        );
    }

    c.batch_execute("CREATE TABLE plain_ok (id int primary key, v text)")
        .unwrap();
    c.execute("INSERT INTO plain_ok VALUES (1,'a'),(2,'b')", &[])
        .unwrap();
    let ok = Rig::pg_cdc("public.plain_ok", &slot).source_url(cdc_db.url());
    let rows: usize = ok.run_and_read().iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 2,
        "the guard must refuse only what routing cannot match — an ordinary table \
         has to keep working, or the fix is worse than the defect"
    );
}

/// A dotted `table:` is read as `schema.table` everywhere.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_a_relation_named_with_a_dot_is_refused_by_a_remedy_that_can_be_followed() {
    let cdc_db = CdcDb::new("cdc_dotted");
    let slot = unique_name("rivet_dotted_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(
        "CREATE TABLE public.\"d.a\" (id int primary key, v text); \
         CREATE SCHEMA d_s; \
         CREATE TABLE d_s.t (id bigint primary key, v text); \
         CREATE TABLE public.\"d_s.t\" (id bigint primary key, v text)",
    )
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    for (cfg, code, needle) in [
        ("d.a", "RIVET_SOURCE_CDC_PREREQUISITE", "does not exist"),
        (
            "public.\"d.a\"",
            "RIVET_SOURCE_CDC_TABLE_UNRESOLVED",
            "Rename the relation",
        ),
        (
            "public.d.a",
            "RIVET_SOURCE_CDC_TABLE_UNRESOLVED",
            "rename it",
        ),
    ] {
        let rig = Rig::pg_cdc(cfg, &slot).source_url(cdc_db.url());
        let out = rig.run();
        assert_refused(&out, Refused::by_code(code, 1));
        let said = String::from_utf8_lossy(&out.stderr).to_string()
            + &String::from_utf8_lossy(&out.stdout);
        assert!(
            said.contains(needle),
            "`{cfg}`: the refusal must say `{needle}`: {said}"
        );
    }
    c.batch_execute(
        "INSERT INTO d_s.t VALUES (1,'in'),(2,'in'); INSERT INTO public.\"d_s.t\" VALUES (9,'foreign')",
    )
    .unwrap();
    let rig = Rig::pg_cdc("d_s.t", &slot).source_url(cdc_db.url());
    let batches = rig.run_and_read();
    let mut ids = ids_of(&batches);
    ids.sort();
    assert_eq!(
        ids,
        vec![1, 2],
        "only the relation `d_s.t` is captured: the dotted-name twin cannot route into the export"
    );
}

/// A TRUNCATE on a captured table must FAIL the run, not vanish.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_refuses_a_truncate_instead_of_silently_diverging() {
    let cdc_db = CdcDb::new("cdc_trunc");
    let tbl = unique_name("rivet_cdc_tr").to_lowercase();
    let other = unique_name("rivet_cdc_trother").to_lowercase();
    let slot = unique_name("rivet_trunc_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT); \
         CREATE TABLE {other} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"), &[])
        .unwrap();
    c.execute(&format!("TRUNCATE {tbl}"), &[]).unwrap();
    let source_rows: i64 = c
        .query_one(&format!("SELECT count(*) FROM {tbl}"), &[])
        .unwrap()
        .get(0);
    assert_eq!(
        source_rows, 0,
        "the fixture's TRUNCATE must have emptied it"
    );

    let rig = Rig::pg_cdc(&format!("public.{tbl}"), &slot)
        .a_failed_run_may_leave(
            &[Leftover::OrphanPart, Leftover::FileLog, Leftover::CdcFlush],
            "the stream delivers the rows it read before the TRUNCATE it refuses",
        )
        .source_url(cdc_db.url());
    let said = rig.run_expect_fail();
    assert!(
        said.contains("TRUNCATE"),
        "a TRUNCATE on a captured table must fail the run — shipping the 2 inserts \
         as a success leaves them in the destination with nothing to retract them. \
         Output:\n{said}"
    );
    assert!(
        said.contains("TRUNCATE") && said.contains(REBASELINE_REMEDY),
        "the refusal must name what happened AND the re-snapshot that recovers \
         from it — a bail with no way forward just moves the operator's problem. \
         Output:\n{said}"
    );

    let other_slot = unique_name("rivet_trunc_slot_b").to_lowercase();
    let _other_slot = Slot::new(other_slot.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&other_slot],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {other} VALUES (7,70)"), &[])
        .unwrap();
    c.execute(&format!("TRUNCATE {tbl}"), &[]).unwrap();
    let bystander = Rig::pg_cdc(&format!("public.{other}"), &other_slot).source_url(cdc_db.url());
    let rows: usize = bystander.run_and_read().iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 1,
        "an export that does not capture the truncated table must complete and \
         capture its own change"
    );

    let third_slot = unique_name("rivet_trunc_slot_c").to_lowercase();
    let _third_slot = Slot::new(third_slot.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&third_slot],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (9,90)"), &[])
        .unwrap();
    c.execute(&format!("TRUNCATE {other}, {tbl}"), &[]).unwrap();
    let multi = Rig::pg_cdc(&format!("public.{tbl}"), &third_slot)
        .a_failed_run_may_leave(
            &[Leftover::OrphanPart, Leftover::FileLog, Leftover::CdcFlush],
            "the stream delivers the rows it read before the TRUNCATE it refuses",
        )
        .source_url(cdc_db.url());
    let said2 = multi.run_expect_fail();
    assert!(
        said2.contains(&tbl),
        "a TRUNCATE naming our table SECOND in a list must still refuse, and name \
         OUR table rather than whichever came first. Output:\n{said2}"
    );
    c.execute(&format!("INSERT INTO {tbl} VALUES (77,770)"), &[])
        .unwrap();
    let _still_wedged = rig.run_expect_fail();
    assert!(
        said.contains("pg_replication_slot_advance"),
        "the refusal must name the way OUT of the wedge, not only the re-snapshot: \
         re-snapshotting alone leaves the slot where it is. Got:\n{said}"
    );
    let past: String = c
        .query_one(
            &format!(
                "WITH p AS (SELECT lsn, data FROM pg_logical_slot_peek_changes('{slot}', NULL, NULL)) \
                 SELECT min(lsn)::text FROM p WHERE data LIKE 'COMMIT%' \
                 AND lsn > (SELECT max(lsn) FROM p WHERE data LIKE '%{tbl}%: TRUNCATE:%')"
            ),
            &[],
        )
        .unwrap()
        .get(0);
    c.execute(
        &format!("SELECT pg_replication_slot_advance('{slot}', '{past}')"),
        &[],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (88,880)"), &[])
        .unwrap();
    let resumed = Rig::pg_cdc(&format!("public.{tbl}"), &slot)
        .source_url(cdc_db.url())
        .no_oracle(
            "the stream delivered (1,10),(2,20) before the TRUNCATE it refused; the refusal \
             prescribes a re-snapshot this test does not take, so the destination keeps them",
        );
    resumed.run_ok();
    let mut ids: Vec<i64> = read_cdc_changes(&resumed.out_dir())
        .iter()
        .map(|c| c.id)
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![77, 88],
        "after the advance the stream must deliver every clean change behind the truncate"
    );
}

/// MySQL peer of `roast_pg_cdc_refuses_a_truncate_instead_of_silently_diverging`.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn roast_mysql_cdc_refuses_a_truncate_instead_of_silently_diverging() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_mtr");
    let other = unique_name("cdc_mtrother");
    let mut c = conn();
    for t in [&tbl, &other] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"))
            .unwrap();
    }
    let _g1 = Table(tbl.clone());
    let _g2 = Table(other.clone());

    let ck = d.path().join("tr.ckpt");
    let rig = || {
        Rig::mysql_cdc(&tbl)
            .checkpoint_path(ck.to_path_buf())
            .dest_path(d.path().join("out"))
    };
    rig().run_ok();

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();
    c.query_drop(format!("TRUNCATE {tbl}")).unwrap();
    let left: Option<i64> = c
        .query_first(format!("SELECT count(*) FROM {tbl}"))
        .unwrap();
    assert_eq!(left, Some(0), "the fixture's TRUNCATE must have emptied it");

    let msg = rig().run_expect_fail();
    assert!(
        msg.contains("TRUNCATE") && msg.contains(REBASELINE_REMEDY),
        "the run must refuse and name both what happened and the re-snapshot that \
         recovers from it — shipping the 2 inserts as a success leaves them in the \
         destination with nothing to retract them. Got: {msg}"
    );

    let other_ck = d.path().join("tr_other.ckpt");
    let other_rig = || {
        Rig::mysql_cdc(&other)
            .checkpoint_path(other_ck.to_path_buf())
            .dest_path(d.path().join("out_other"))
    };
    other_rig().run_ok();
    c.query_drop(format!("INSERT INTO {other} VALUES (7,70)"))
        .unwrap();
    c.query_drop(format!("TRUNCATE {tbl}")).unwrap();
    let rows: usize = other_rig()
        .run_and_read()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        rows, 1,
        "an export that does not capture the truncated table must complete and \
         capture its own change"
    );
}

/// An unqualified `table:` means the relation the search_path resolves.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_a_bare_table_name_beside_a_same_named_relation_gets_no_foreign_row() {
    let cdc_db = CdcDb::new("cdc_bare");
    let tbl = unique_name("rivet_cdc_bare").to_lowercase();
    let slot = unique_name("rivet_bare_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id int PRIMARY KEY, v text); \
         CREATE SCHEMA other; \
         CREATE TABLE other.{tbl} (zzz int PRIMARY KEY, qqq text)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1,'mine'); \
         INSERT INTO other.{tbl} VALUES (2,'FROM_OTHER_SCHEMA')"
    ))
    .unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot).source_url(cdc_db.url());
    rig.run_ok();
    assert_eq!(
        ids_read_all(&rig.out_dir()),
        vec![1],
        "only the search_path table's row; never the other schema's"
    );

    c.batch_execute(&format!("TRUNCATE other.{tbl}")).unwrap();
    rig.run_ok();
    assert_eq!(ids_read_all(&rig.out_dir()), vec![1]);
    c.batch_execute("DROP SCHEMA other CASCADE").unwrap();
    c.batch_execute(&format!(
        "CREATE SCHEMA mv; CREATE TABLE mv.src(id int, v text); \
         INSERT INTO mv.src VALUES (9,'from_a_matview'); \
         CREATE MATERIALIZED VIEW mv.{tbl} AS SELECT id, v FROM mv.src; \
         INSERT INTO {tbl} VALUES (3,'only')"
    ))
    .unwrap();
    rig.run_ok();
    assert_eq!(
        ids_read_all(&rig.out_dir()),
        vec![1, 3],
        "the matview's row never reaches this export"
    );
}

struct CdcDb {
    name: String,
    url: String,
}
impl CdcDb {
    fn new(label: &str) -> Self {
        let name = unique_name(label).to_lowercase();
        let mut admin = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls)
            .expect("connect cdc admin");
        admin
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .expect("create dedicated cdc db");
        let base = POSTGRES_CDC_URL
            .rsplit_once('/')
            .expect("cdc url has a /db path")
            .0;
        Self {
            url: format!("{base}/{name}"),
            name,
        }
    }
    fn url(&self) -> &str {
        &self.url
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn connect(&self) -> postgres::Client {
        postgres::Client::connect(&self.url, postgres::NoTls).expect("connect dedicated cdc db")
    }
}
impl Drop for CdcDb {
    fn drop(&mut self) {
        if let Ok(mut admin) = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls) {
            let _ = admin.batch_execute(&format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE datname = '{}' AND pid <> pg_backend_pid()",
                self.name
            ));
            let _ = admin.batch_execute(&format!("DROP DATABASE IF EXISTS {}", self.name));
        }
    }
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_reaches_open_bound_past_a_large_empty_ddl_span() {
    let cdc_db = CdcDb::new("cdc_ddlspan");
    let a = unique_name("rivet_cdc_ddlspan");
    let slot = unique_name("rivet_ddl_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(&format!("CREATE TABLE {a} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    for i in 0..40 {
        c.batch_execute(&format!(
            "CREATE TABLE {a}_ddl_{i} (x int); DROP TABLE {a}_ddl_{i}"
        ))
        .unwrap();
    }
    for i in 0..12i64 {
        c.execute(&format!("INSERT INTO {a} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&a, &slot)
        .source_url(cdc_db.url())
        .cdc("rollover: 5")
        .dest_path(out.clone());
    run_rivet_ok(&rig.config_path());

    let got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        got,
        want,
        "one bounded run must capture all of A's in-bound rows past the large \
         empty DDL span — got {} ids (the run stopped after one window of the \
         empty span and wrote _SUCCESS with in-bound data unread)",
        got.len()
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_empty_transaction_churn_must_not_pin_the_slot() {
    let cdc_db = CdcDb::new("cdc_empty");
    let tbl = unique_name("rivet_cdc_pgempty");
    let slot = unique_name("rivet_empty_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    let rig = Rig::pg_cdc(&tbl, &slot).source_url(cdc_db.url());
    let cfg = rig.config_path();
    run_rivet_ok(&cfg);
    let before: String = c
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0);

    for i in 0..20 {
        c.batch_execute(&format!(
            "CREATE TABLE {tbl}_junk_{i} (id INT); DROP TABLE {tbl}_junk_{i}"
        ))
        .unwrap();
    }

    run_rivet_ok(&cfg);
    let advanced: bool = c
        .query_one(
            &format!(
                "SELECT confirmed_flush_lsn > '{before}'::pg_lsn \
                 FROM pg_replication_slots WHERE slot_name = $1"
            ),
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert!(
        advanced,
        "a zero-yield run must advance the slot past the empty-transaction span \
         (confirmed_flush_lsn stuck at {before} — WAL pinned behind DDL noise)"
    );
}

/// `rivet cdc --max-events` must not be able to WEDGE a checkpointed CLI run.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn roast_mysql_cdc_cli_max_events_below_a_transaction_still_advances_the_checkpoint() {
    let mut c = conn();
    let tbl = unique_name("rivet_cdc_cap");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ck");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();

    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1,1),(2,2),(3,3),(4,4),(5,5)"
    ))
    .expect("seed one transaction");

    let ckpt_s = ckpt.to_str().unwrap().to_string();
    let tbl_q = tbl.clone();
    let sid = server_id_for(&tbl).to_string();
    let cdc_run = move || {
        run_rivet_args_bounded(
            &[
                "cdc",
                "--source",
                MYSQL_CDC_URL,
                "--server-id",
                &sid,
                "--table",
                &tbl_q,
                "--checkpoint",
                &ckpt_s,
                "--max-events",
                "2",
            ],
            std::time::Duration::from_secs(60),
        )
    };

    let first = cdc_run().expect("run 1 must terminate");
    let ids1 = ndjson_after_ids(&first, &tbl);
    assert_eq!(
        ids1,
        (1..=5).collect::<std::collections::BTreeSet<i64>>(),
        "a cap of 2 inside a 5-row transaction must reach the boundary, not cut it; got {ids1:?}\n{first}"
    );

    let second = cdc_run().expect("run 2 must terminate");
    let ids2 = ndjson_after_ids(&second, &tbl);
    assert!(
        ids2.is_empty(),
        "run 2 must emit nothing — the checkpoint advanced past the transaction. Re-emitting \
         {ids2:?} is the wedge: pre-fix no boundary was ever saved, so every run re-printed the \
         same prefix and the export never progressed"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_ndjson_until_current_terminates_and_emits_backlog() {
    use postgres::NoTls;
    let tbl = unique_name("rivet_cdc_pgnd");
    let slot = unique_name("rivet_nd_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    for i in 0..30i64 {
        c.execute(&format!("INSERT INTO {tbl} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    let tbl_bg = tbl.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
        let mut w = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("bg connect");
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            let vals: Vec<String> = (i..i + 10).map(|k| format!("({k},{k})")).collect();
            let _ = w.batch_execute(&format!("INSERT INTO {tbl_bg} VALUES {}", vals.join(",")));
            i += 10;
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });

    let out = run_rivet_args_bounded(
        &[
            "cdc",
            "--source",
            POSTGRES_CDC_URL,
            "--slot",
            &slot,
            "--table",
            &tbl,
        ],
        std::time::Duration::from_secs(30),
    );
    bg.stop();
    let stdout = out.expect("bounded NDJSON run must terminate under sustained writes");

    let ids = ndjson_after_ids(&stdout, &tbl);
    for i in 0..30 {
        assert!(
            ids.contains(&i),
            "backlog id {i} must be emitted to stdout, got {} ids",
            ids.len()
        );
    }
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_reaches_open_bound_past_a_large_uncaptured_transaction() {
    let cdc_db = CdcDb::new("cdc_dens");
    let a = unique_name("rivet_cdc_capa");
    let b = unique_name("rivet_cdc_forgnb");
    let slot = unique_name("rivet_dens_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {a} (id BIGINT PRIMARY KEY, v INT); \
         CREATE TABLE {b} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    c.execute(
        &format!("INSERT INTO {b} SELECT g, g FROM generate_series(1, 200) g"),
        &[],
    )
    .unwrap();
    for i in 0..30i64 {
        c.execute(&format!("INSERT INTO {a} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    let rig = Rig::pg_cdc(&a, &slot)
        .source_url(cdc_db.url())
        .cdc("rollover: 5");
    run_rivet_ok(&rig.config_path());

    let got: std::collections::BTreeSet<i64> = duckdb_dir_parquet_i64(&rig.out_dir(), "id")
        .into_iter()
        .collect();
    let want: std::collections::BTreeSet<i64> = (0..30).collect();
    assert_eq!(
        got,
        want,
        "a single bounded run must capture ALL of A's in-bound rows past the \
         large uncaptured B transaction — got {} ids (the slot starved on B and \
         exhausted before reaching A)",
        got.len()
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_drain_releases_pinned_wal_and_advances_xmin() {
    let d = tempfile::tempdir().unwrap();
    let cdc_db = CdcDb::new("cdc_walret");
    let tbl = unique_name("rivet_cdc_walret");
    let slot = unique_name("rivet_walret_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id bigint primary key, v int, pad text)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    let confirmed = |c: &mut postgres::Client| -> String {
        c.query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0)
    };
    let cf_before = confirmed(&mut c);
    c.execute(
        &format!(
            "INSERT INTO {tbl} SELECT g, g%1000, repeat('x',80) FROM generate_series(1,20000) g"
        ),
        &[],
    )
    .unwrap();
    let pinned_before: i64 = c
        .query_one(
            &format!("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '{cf_before}'::pg_lsn)::bigint"),
            &[],
        )
        .unwrap()
        .get(0);
    assert!(
        pinned_before > 1_000_000,
        "the slot must pin a real amount of WAL before the drain (got {pinned_before} bytes)"
    );

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .source_url(cdc_db.url())
        .dest_path(out.clone());
    run_rivet_ok(&rig.config_path());

    assert_eq!(
        manifest_rows(&out),
        20000,
        "the drain must capture the whole backlog"
    );
    let cf_after = confirmed(&mut c);
    let advanced: i64 = c
        .query_one(
            &format!("SELECT pg_wal_lsn_diff('{cf_after}'::pg_lsn, '{cf_before}'::pg_lsn)::bigint"),
            &[],
        )
        .unwrap()
        .get(0);
    assert!(
        advanced > 1_000_000,
        "the drain must RELEASE the pinned WAL by advancing confirmed_flush past the multi-MB \
         backlog — it advanced only {advanced} bytes (an un-acked drain leaves confirmed_flush \
         pinned: the disk-fill harm; catalog_xmin/vacuum follows the same advance)"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_captures_a_silent_update_a_watermark_sync_would_miss() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_silent");
    let slot = unique_name("rivet_silent_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id bigint primary key, v bigint, updated_at timestamptz)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    c.execute(
        &format!("INSERT INTO {tbl} VALUES (1, 0, '2020-01-01T00:00:00Z')"),
        &[],
    )
    .unwrap();
    let wm_before: chrono::DateTime<chrono::Utc> = c
        .query_one(&format!("SELECT updated_at FROM {tbl} WHERE id=1"), &[])
        .unwrap()
        .get(0);

    c.execute(&format!("UPDATE {tbl} SET v = 42 WHERE id = 1"), &[])
        .unwrap();
    let wm_after: chrono::DateTime<chrono::Utc> = c
        .query_one(&format!("SELECT updated_at FROM {tbl} WHERE id=1"), &[])
        .unwrap()
        .get(0);
    assert_eq!(
        wm_before, wm_after,
        "the silent update must NOT bump updated_at — a watermark sync would miss it"
    );

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out));

    let ops = cdc_id_ops(&out);
    assert_eq!(
        ops,
        vec![(1, "insert".to_string()), (1, "update".to_string())],
        "CDC must capture the insert AND the silent update the watermark missed — got {ops:?}"
    );
    let vcs: Vec<i64> = duckdb_dir_parquet_i64(&out, "v");
    assert_eq!(
        vcs,
        vec![0, 42],
        "the captured after-images must carry the silent update's NEW value (0 then 42) — got {vcs:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_oversized_transaction_bails_loud_not_oom() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_bigtx");
    let slot = unique_name("rivet_bigtx_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id bigint primary key, v bigint)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(
        &format!("INSERT INTO {tbl} SELECT g, g FROM generate_series(1, 20) g"),
        &[],
    )
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot).dest_path(out);
    let output = rig.run_with_env("RIVET_CDC_MAX_TX_ROWS", "10");
    assert!(
        !output.status.success(),
        "an over-cap transaction must FAIL the run, not OOM or silently truncate"
    );
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        err.contains("more than 10 rows") && err.contains("buffered whole"),
        "the failure must name the cap and the never-split-a-transaction reason — got:\n{err}"
    );
}

#[test]
#[ignore = "live+gate-only: requires the cdc-standby profile — python3 -m dev.pytools.cdc_stand standby (pg-cdc-standby on :5436)"]
fn roast_pg_cdc_bounded_on_a_standby_fails_loud() {
    let standby_url = PG_STANDBY_URL;
    if std::net::TcpStream::connect_timeout(
        &"127.0.0.1:5436".parse().unwrap(),
        std::time::Duration::from_millis(500),
    )
    .is_err()
    {
        skip_live(
            "roast_pg_cdc_bounded_on_a_standby_fails_loud: cdc-standby not up on :5436 \
             (bring it up with `python3 -m dev.pytools.cdc_stand standby`)",
        );
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("t_standby");
    let slot = unique_name("standby_slot");
    let _slot = Slot::on(standby_url, slot.clone());
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .source_url(standby_url)
        .dest_path(out);
    let err = rig.run_expect_fail();
    assert!(
        err.contains("standby") && err.contains("recovery") && err.contains("until_current: false"),
        "the failure must name the standby and the escape (stream continuously / point at the \
         primary) — got:\n{err}"
    );
    let mut sc = postgres::Client::connect(standby_url, postgres::NoTls).expect("connect standby");
    let slot_created: bool = sc
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert!(
        !slot_created,
        "the proactive recovery check must refuse a standby BEFORE creating a slot — a slot \
         named '{slot}' was left on the standby, meaning the run blocked on slot creation and \
         leaked a WAL-pinning slot instead of failing fast"
    );
}

#[test]
#[ignore = "live+gate-only: requires the cdc-standby profile — python3 -m dev.pytools.cdc_stand standby (pg-cdc-primary :5437 → pg-cdc-standby :5436)"]
fn pg_cdc_streams_changes_from_a_standby_in_continuous_mode() {
    let (primary_url, standby_url) = (PG_STANDBY_PRIMARY_URL, PG_STANDBY_URL);
    for port in ["5436", "5437"] {
        if std::net::TcpStream::connect_timeout(
            &format!("127.0.0.1:{port}").parse().unwrap(),
            std::time::Duration::from_millis(500),
        )
        .is_err()
        {
            panic!("fixture: the cdc-standby pair is not up on :{port}");
        }
    }
    let mut p = postgres::Client::connect(primary_url, postgres::NoTls).expect("connect primary");
    let mut sb = postgres::Client::connect(standby_url, postgres::NoTls).expect("connect standby");
    let tbl = unique_name("t_standby_stream");
    let slot = unique_name("standby_stream_slot");
    let _slot = Slot::on(standby_url, slot.clone());
    p.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(primary_url, tbl.clone());
    let replicated = |sb: &mut postgres::Client, sql: &str, want: i64| {
        for _ in 0..60 {
            if sb.query_one(sql, &[]).ok().map(|r| r.get::<_, i64>(0)) == Some(want) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        panic!("fixture: the standby never reached {want} for `{sql}`");
    };
    replicated(
        &mut sb,
        &format!("SELECT COUNT(*) FROM pg_class WHERE relname = '{tbl}'"),
        1,
    );

    let rig = Rig::pg_cdc_standby(&tbl, &slot).continuous();
    let first = rig.run_nudged(&[]);
    if !first.status.success() {
        panic!(
            "a continuous run must stream from a PostgreSQL 16 standby:\n{}",
            String::from_utf8_lossy(&first.stderr)
        );
    }

    p.batch_execute(&format!("INSERT INTO {tbl} VALUES (1, 10), (2, 20)"))
        .unwrap();
    replicated(&mut sb, &format!("SELECT COUNT(*) FROM {tbl}"), 2);
    let second = rig.run();
    assert!(
        second.status.success(),
        "the second standby run:\n{}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(
        duckdb_declared_dir_id_set(&rig.out_dir()),
        [1, 2].into_iter().collect(),
        "both changes made on the primary must be captured from the standby"
    );
}

const PG_FAILOVER_PRIMARY: &str = PG_STANDBY_PRIMARY_URL;
const PG_FAILOVER_STANDBY: &str = PG_STANDBY_URL;
const PG_FAILOVER_URL: &str = "RIVET_FAILOVER_URL";

/// A capture on the primary (ids 1-2).
struct PgFailover {
    rig: Rig,
    _guards: (PgTable, Slot, Slot),
}

/// `(op, id)` of every row in `batches`, the id read as i64 whatever its integer width.
fn op_ids(batches: &[arrow::record_batch::RecordBatch]) -> Vec<(String, i64)> {
    use arrow::array::{Array, Int64Array, StringArray};
    let mut out = Vec::new();
    for b in batches {
        let op = b.column(b.schema().index_of("__op").unwrap()).clone();
        let id = arrow::compute::cast(
            b.column(b.schema().index_of("id").unwrap()),
            &arrow::datatypes::DataType::Int64,
        )
        .unwrap();
        let op = op.as_any().downcast_ref::<StringArray>().unwrap();
        let id = id.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((op.value(i).to_string(), id.value(i)));
        }
    }
    out
}

/// Run `rig` against `url` while the primary keeps logging standby snapshots (a slot created on a standby waits for one).
fn run_on(rig: &Rig, url: &str) -> std::process::Output {
    rig.run_nudged(&[(PG_FAILOVER_URL, url)])
}

/// The capture before the switch, and the rows the standby holds after it.
fn pg_failover(tag: &str, with_checkpoint: bool, mark: impl FnOnce(Rig) -> Rig) -> PgFailover {
    for port in ["5436", "5437"] {
        assert!(
            std::net::TcpStream::connect_timeout(
                &format!("127.0.0.1:{port}").parse().unwrap(),
                std::time::Duration::from_millis(500),
            )
            .is_ok(),
            "fixture: the cdc-standby pair is not up on :{port}"
        );
    }
    let mut p = postgres::Client::connect(PG_FAILOVER_PRIMARY, postgres::NoTls).unwrap();
    let mut sb = postgres::Client::connect(PG_FAILOVER_STANDBY, postgres::NoTls).unwrap();
    let tbl = unique_name(tag);
    let slot = unique_name(&format!("{tag}_slot"));
    p.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let guards = (
        PgTable::adopt_on(PG_FAILOVER_PRIMARY, tbl.clone()),
        Slot::on(PG_FAILOVER_PRIMARY, slot.clone()),
        Slot::on(PG_FAILOVER_STANDBY, slot.clone()),
    );
    let mut rig = Rig::pg_cdc(&tbl, &slot)
        .source_url_env(PG_FAILOVER_URL)
        .continuous();
    if with_checkpoint {
        let ckpt = rig.checkpoint();
        rig = rig.checkpoint_path(ckpt);
    }
    let rig = mark(rig);
    let on_primary = |rig: &Rig| {
        let out = run_on(rig, PG_FAILOVER_PRIMARY);
        assert!(
            out.status.success(),
            "fixture: a run on the primary failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    on_primary(&rig);
    p.batch_execute(&format!("INSERT INTO {tbl} VALUES (1, 10), (2, 20)"))
        .unwrap();
    on_primary(&rig);
    assert_eq!(
        op_ids(&read_all_parts(&rig.out_dir())),
        vec![("insert".into(), 1), ("insert".into(), 2)],
        "fixture: the primary capture holds ids 1-2"
    );
    p.batch_execute(&format!("INSERT INTO {tbl} VALUES (3, 30), (4, 40)"))
        .unwrap();
    let rows = format!("SELECT COUNT(*) FROM {tbl}");
    for _ in 0..60 {
        if sb.query_one(&rows, &[]).ok().map(|r| r.get::<_, i64>(0)) == Some(4) {
            return PgFailover {
                rig,
                _guards: guards,
            };
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    panic!("fixture: the standby never received ids 3-4");
}

/// With a checkpoint, a failover to a server without the slot is refused.
#[test]
#[ignore = "live+gate-only: requires the cdc-standby profile — python3 -m dev.pytools.cdc_stand standby (pg-cdc-primary :5437 → pg-cdc-standby :5436)"]
fn a_pg_failover_to_the_standby_with_a_checkpoint_is_refused_not_resumed_on_a_new_slot() {
    let f = pg_failover("pg_fo_ckpt", true, |r| r);
    let parts_before = read_all_parts(&f.rig.out_dir()).len();
    let out = run_on(&f.rig, PG_FAILOVER_STANDBY);
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success()
            && said.contains("is missing but the checkpoint file holds a position"),
        "the switch to a server without the slot is refused by name:\n{said}"
    );
    assert_eq!(
        read_all_parts(&f.rig.out_dir()).len(),
        parts_before,
        "no part was written"
    );
}

/// Strict known defect (graded by the rig oracle).
#[test]
#[ignore = "live+gate-only: requires the cdc-standby profile — python3 -m dev.pytools.cdc_stand standby (pg-cdc-primary :5437 → pg-cdc-standby :5436)"]
fn a_pg_failover_to_the_standby_without_a_checkpoint_loses_the_rows_written_during_the_switch() {
    let f = pg_failover("pg_fo_nockpt", false, |r| {
        r.oracle_known_defect(
            "undelivered rows",
            "known defect: a PostgreSQL CDC failover without `cdc.checkpoint` creates a new slot on the \
             new server with only a warning, so the rows written during the switch are lost \
             (PostgreSQL CDC failover step)",
        )
    });
    let out = run_on(&f.rig, PG_FAILOVER_STANDBY);
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && said.contains("creating replication slot"),
        "the run on the standby creates a new slot and warns:\n{said}"
    );
    assert_eq!(
        op_ids(&read_all_parts(&f.rig.out_dir())),
        vec![("insert".into(), 1), ("insert".into(), 2)],
        "ids 3-4, written during the switch, are not captured"
    );
}

/// Strict known defect (PostgreSQL CDC failover step).
#[test]
#[ignore = "live+gate-only: requires the cdc-standby profile — python3 -m dev.pytools.cdc_stand standby (pg-cdc-primary :5437 → pg-cdc-standby :5436)"]
#[should_panic(expected = "rivet did not follow the failover")]
fn pg_cdc_follows_a_failover_to_the_standby() {
    let f = pg_failover("pg_fo_follow", true, |r| r);
    let out = run_on(&f.rig, PG_FAILOVER_STANDBY);
    assert!(
        out.status.success(),
        "rivet did not follow the failover to the standby:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        op_ids(&read_all_parts(&f.rig.out_dir())),
        (1..=4)
            .map(|i| ("insert".to_string(), i))
            .collect::<Vec<_>>(),
        "across the failover every change is captured exactly once, none skipped"
    );
}

/// Finding #3: MySQL CDC enriches ENUM/SET labels from information_schema.COLUMNS.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_cross_database_enum_enriches_from_the_tables_own_schema() {
    let d = tempfile::tempdir().unwrap();
    let dbname = unique_name("rivet_xdb").to_lowercase();
    let qualified = format!("{dbname}.orders");

    let root_url = MYSQL_CDC_URL.replace("rivet:rivet@", "root:rivet@");
    let mut admin = mysql::Conn::new(mysql::Opts::from_url(&root_url).unwrap()).unwrap();
    admin
        .query_drop(format!("CREATE DATABASE {dbname}"))
        .expect("create cross db");
    struct DbGuard(String, String);
    impl Drop for DbGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(&self.1).unwrap()) {
                let _ = c.query_drop(format!("DROP DATABASE IF EXISTS {}", self.0));
            }
        }
    }
    let _guard = DbGuard(dbname.clone(), root_url.clone());
    admin
        .query_drop(format!(
            "CREATE TABLE {dbname}.orders \
             (id INT PRIMARY KEY, status ENUM('active','shipped','off'))"
        ))
        .expect("create cross-db table");
    admin
        .query_drop(format!("GRANT SELECT ON {dbname}.* TO 'rivet'@'%'"))
        .expect("grant select");
    admin.query_drop("FLUSH PRIVILEGES").unwrap();

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let cfg = cdc_config(&d, &qualified, &ckpt, &out);

    run_rivet_ok(&cfg);
    admin
        .query_drop(format!("INSERT INTO {dbname}.orders VALUES (1, 'off')"))
        .expect("insert enum row");
    run_rivet_ok(&cfg);

    assert_eq!(
        parquet_one_string(&out, "status"),
        "off",
        "a cross-database ENUM must enrich to its LABEL from the table's own schema, \
         not the connection's default DATABASE()"
    );
}

/// Closes the CDC/incremental → warehouse LOAD quadrant.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + duckdb"]
fn cdc_changes_parquet_loads_into_a_warehouse_and_dedups_to_current_state() {
    let d = tempfile::tempdir().unwrap();
    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("cdc_load"));
    let tbl = unique_name("cdc_load");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();

    let ckpt = d.path().join("cdc.ckpt");
    let rig = Rig::mysql_cdc(&tbl)
        .cdc_line("initial: snapshot")
        .export_line("meta_columns: { exported_at: true, row_hash: true }")
        .checkpoint_path(ckpt.clone())
        .dest_path(host_dir.clone());
    let cfg = rig.config_path();
    run_rivet_ok(&cfg);

    c.query_drop(format!("UPDATE {tbl} SET v = 100 WHERE id = 1"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (3,30)"))
        .unwrap();
    run_rivet_ok(&cfg);

    let changes = format!(
        "read_parquet(['{c}/snapshot/*.parquet','{c}/cdc-*.parquet'], union_by_name=true)",
        c = container_dir,
    );

    let desc = duckdb_run_sql_json(&format!("DESCRIBE SELECT * FROM {changes}"));
    let cols = duckdb_parse_describe(&desc);
    let mut got_cols: Vec<String> = cols.keys().cloned().collect();
    got_cols.sort();
    assert_eq!(
        got_cols,
        vec![
            "__op".to_string(),
            "__pos".to_string(),
            "__seq".to_string(),
            "_rivet_row_hash".to_string(),
            "id".to_string(),
            "v".to_string(),
        ],
        "the unioned __changes must carry EXACTLY the 3 CDC meta columns + the \
         both-legs `_rivet_row_hash` + the source data columns; any other column is \
         a leaked snapshot-leg meta_column (fda1653 — breaks the warehouse load, \
         silent under count/sum checks)"
    );

    let sql = format!(
        "SELECT id, v FROM (
           SELECT id, v, ROW_NUMBER() OVER (
             PARTITION BY id ORDER BY (__pos IS NOT NULL) DESC, __seq DESC
           ) AS rn FROM {changes}
         ) WHERE rn = 1 ORDER BY id"
    );
    let res = duckdb_run_sql_json(&sql);
    let got: Vec<(String, String)> = res["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            let a = r.as_array().unwrap();
            (
                a[0].as_str().unwrap().to_string(),
                a[1].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("1".into(), "100".into()),
            ("2".into(), "20".into()),
            ("3".into(), "30".into()),
        ],
        "CDC __changes must LOAD (both legs into one table) and DEDUP to the source's \
         current state — id=1 updated to 100, id=2 unchanged, id=3 insert-only"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn both_legs_of_an_initial_snapshot_export_claim_one_source_identity() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_ident");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let rig = Rig::mysql_cdc(&tbl)
        .source_url_env("MYSQL_CDC_URL")
        .cdc_line("initial: snapshot")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let out_run = rig.run_args_env(&[], &[("MYSQL_CDC_URL", MYSQL_CDC_URL)]);
    assert!(
        out_run.status.success(),
        "the snapshot+cdc run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr)
    );

    let source_of = |p: &std::path::Path| -> (String, Option<String>) {
        let body =
            std::fs::read_to_string(p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()));
        let m: serde_json::Value = serde_json::from_str(&body).unwrap();
        (
            m["source"]["engine"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            m["source"]["table"].as_str().map(str::to_string),
        )
    };
    let drain = source_of(&out.join("manifest.json"));
    let leg = source_of(&out.join("snapshot").join("manifest.json"));

    assert!(
        !drain.0.is_empty() && !leg.0.is_empty(),
        "fixture is inert — one leg wrote no manifest: drain={drain:?} leg={leg:?}"
    );
    assert_eq!(
        drain, leg,
        "the two legs of ONE export disagree about their source: a load over this prefix sees \
         two identities and refuses to load either"
    );
    assert_eq!(
        leg.1.as_deref(),
        Some(tbl.as_str()),
        "the snapshot leg must record the DECLARED table, not whatever its synthesized export \
         name happens to parse to"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_destination_placeholders_resolve_like_the_batch_path() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_placeholder");
    let slot = unique_name("rivet_placeholder_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id INT PRIMARY KEY, v TEXT);"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    struct SlotGuard(String);
    impl Drop for SlotGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls) {
                let _ = c.execute(
                    &format!("SELECT pg_drop_replication_slot('{}')", self.0),
                    &[],
                );
            }
        }
    }
    let _slot_guard = SlotGuard(slot.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c');"
    ))
    .unwrap();

    let base = d.path().join("out");
    std::fs::create_dir_all(&base).unwrap();
    Rig::pg_cdc(&tbl, &slot)
        .dest_path(base.join("{date}"))
        .run_ok();

    let literal = base.join("{date}");
    let resolved = base.join(chrono::Utc::now().format("%Y-%m-%d").to_string());

    let parquet_in = |p: &std::path::Path| -> usize {
        std::fs::read_dir(p)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
                    .count()
            })
            .unwrap_or(0)
    };
    let at_resolved = parquet_in(&resolved);
    assert!(
        at_resolved > 0,
        "no parquet at the RESOLVED prefix the reader computes ({}) — the \
         template reached `create_destination` unexpanded, so validate, load and \
         gc all look somewhere the drain never wrote",
        resolved.display()
    );
    assert_eq!(
        parquet_in(&literal),
        0,
        "parquet landed under the LITERAL `{{date}}` directory ({}) — the write \
         address and the read address disagree",
        literal.display()
    );
}

/// The binlog-compression guard, live.
#[test]
#[ignore = "live+exclusive: flips a :3306 GLOBAL every new session inherits — set RIVET_TEST_EXCLUSIVE=1"]
fn mysql_cdc_refuses_a_compressed_binlog_instead_of_capturing_nothing() {
    if binlog_compression_needs_the_stand() {
        return;
    }
    let _serial = quiet_window_guard();
    let root_url = MYSQL_URL.replace("rivet:rivet@", "root:rivet@");
    let _meta = RowMetadata::set(&root_url, "FULL");
    let mut admin = match mysql::Conn::new(mysql::Opts::from_url(&root_url).unwrap()) {
        Ok(c) => c,
        Err(e) => panic!("cdc-profile MySQL admin connection: {e}"),
    };
    let supported: Option<String> = admin
        .query_first("SELECT @@global.binlog_transaction_compression")
        .ok()
        .flatten();
    if supported.is_none() {
        skip_live("server has no binlog_transaction_compression (pre-8.0.20)");
        return;
    }

    struct CompressionGuard(String);
    impl Drop for CompressionGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(&self.0).unwrap()) {
                let _ = c.query_drop("SET GLOBAL binlog_transaction_compression = OFF");
            }
        }
    }
    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = ON")
        .expect("set global binlog_transaction_compression");
    let _restore = CompressionGuard(root_url.clone());

    let tbl = unique_name("cdc_compressed");
    let batch_conn =
        || mysql::Conn::new(mysql::Opts::from_url(MYSQL_URL).unwrap()).expect("batch mysql");
    let mut c = batch_conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    struct BatchTable(String);
    impl Drop for BatchTable {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(MYSQL_URL).unwrap()) {
                let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
            }
        }
    }
    let _guard = BatchTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();

    let cfg_into = |dest: &std::path::Path| {
        Rig::mysql_cdc(&tbl)
            .source_url(&root_url)
            .checkpoint_path(ckpt.clone())
            .dest_path(dest.to_path_buf())
            .config_in(d.path())
    };
    let cfg = cfg_into(&out);
    let out_text = {
        let o = run_rivet(&["run", "--config", cfg.to_str().unwrap()]);
        assert!(
            !o.status.success(),
            "a compressed binlog must FAIL the run, not capture nothing:\n{}",
            String::from_utf8_lossy(&o.stdout)
        );
        format!(
            "{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        )
    };
    assert!(
        out_text.contains("binlog_transaction_compression"),
        "the refusal must name the setting so it is actionable:\n{out_text}"
    );

    drop(_restore);
    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = OFF")
        .unwrap();
    let out1 = d.path().join("out1");
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out1).unwrap();
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&cfg_into(&out1));
    let mut fresh = batch_conn();
    fresh
        .query_drop(format!("INSERT INTO {tbl} VALUES (1, 10), (2, 20)"))
        .unwrap();
    run_rivet_ok(&cfg_into(&out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "with compression off the same table must capture both rows"
    );
}

/// True (and a skip is recorded) unless the stand is ours alone.
fn binlog_compression_needs_the_stand() -> bool {
    let shared = std::env::var("RIVET_TEST_EXCLUSIVE").is_err();
    if shared {
        skip_live(
            "flips MySQL's global binlog_transaction_compression, which every session opened \
             meanwhile inherits — run with RIVET_TEST_EXCLUSIVE=1 and --test-threads=1",
        );
    }
    shared
}

/// #200-2: the STREAM-time sibling of the open-time guard above.
#[test]
#[ignore = "live+exclusive: flips a :3306 GLOBAL every new session inherits — set RIVET_TEST_EXCLUSIVE=1"]
fn mysql_cdc_compressed_payload_in_stream_refuses_not_skips() {
    if binlog_compression_needs_the_stand() {
        return;
    }
    let _serial = quiet_window_guard();
    let root_url = MYSQL_URL.replace("rivet:rivet@", "root:rivet@");
    let _meta = RowMetadata::set(&root_url, "FULL");
    let mut admin = match mysql::Conn::new(mysql::Opts::from_url(&root_url).unwrap()) {
        Ok(c) => c,
        Err(e) => panic!("cdc-profile MySQL admin connection: {e}"),
    };
    let supported: Option<String> = admin
        .query_first("SELECT @@global.binlog_transaction_compression")
        .ok()
        .flatten();
    if supported.is_none() {
        skip_live("server has no binlog_transaction_compression (pre-8.0.20)");
        return;
    }

    struct CompressionGuard(String);
    impl Drop for CompressionGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(&self.0).unwrap()) {
                let _ = c.query_drop("SET GLOBAL binlog_transaction_compression = OFF");
            }
        }
    }
    let _restore = CompressionGuard(root_url.clone());
    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = OFF")
        .unwrap();

    let tbl = unique_name("cdc_cmp_stream");
    let mut c = mysql::Conn::new(mysql::Opts::from_url(MYSQL_URL).unwrap()).expect("batch mysql");
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v TEXT)"))
        .unwrap();
    struct BatchTable(String);
    impl Drop for BatchTable {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(MYSQL_URL).unwrap()) {
                let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
            }
        }
    }
    let _guard = BatchTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let cfg_into = |dest: &std::path::Path| {
        Rig::mysql_cdc(&tbl)
            .source_url(&root_url)
            .checkpoint_path(ckpt.clone())
            .dest_path(dest.to_path_buf())
            .config_in(d.path())
    };

    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_rivet_ok(&cfg_into(&out1));

    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = ON")
        .unwrap();
    let mut writer = mysql::Conn::new(mysql::Opts::from_url(MYSQL_URL).unwrap()).expect("writer");
    writer.query_drop("BEGIN").unwrap();
    for i in 1..=50 {
        writer
            .query_drop(format!(
                "INSERT INTO {tbl} VALUES ({i}, REPEAT('rivet-compressible-', 200))"
            ))
            .unwrap();
    }
    writer.query_drop("COMMIT").unwrap();

    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = OFF")
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let o = run_rivet(&["run", "--config", cfg_into(&out2).to_str().unwrap()]);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    assert!(
        !o.status.success(),
        "a compressed transaction in the stream must FAIL the resume, not silently \
         skip it and report success:\n{text}"
    );
    assert!(
        text.contains("Transaction_payload_event")
            && text.contains("binlog_transaction_compression"),
        "the stream-time refusal must name the event + the setting so it is actionable:\n{text}"
    );
    assert!(
        !out2.join("_SUCCESS").exists(),
        "a refused resume must not write _SUCCESS"
    );
}

/// `rivet cdc --output --rollover N`.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn roast_mysql_cdc_cli_rollover_keeps_transactions_whole_and_two_runs_do_not_clobber() {
    require_alive(LiveService::DuckDb);
    let mut c = conn();
    let tbl = unique_name("rivet_cdc_roll");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ck");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();

    let ckpt_s = ckpt.to_str().unwrap().to_string();
    let out_s = out.to_str().unwrap().to_string();
    let tbl_q = tbl.clone();
    let sid = server_id_for(&tbl).to_string();
    let cdc_run = move || {
        run_rivet_env(
            &[
                "cdc",
                "--source",
                MYSQL_CDC_URL,
                "--server-id",
                &sid,
                "--table",
                &tbl_q,
                "--checkpoint",
                &ckpt_s,
                "--output",
                &out_s,
                "--rollover",
                "2",
            ],
            &[],
        )
    };

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,1),(2,2),(3,3)"))
        .expect("tx1");
    let r1 = cdc_run();
    assert!(
        r1.status.success(),
        "cdc run 1 failed:\n{}",
        String::from_utf8_lossy(&r1.stderr)
    );
    let parts_after_1 = files_with_extension(&out, "parquet").len();
    assert_eq!(
        duckdb_total_parquet_rows(&out),
        3,
        "run 1 must capture the whole transaction"
    );
    assert_eq!(
        parts_after_1, 1,
        "a 3-row transaction at --rollover 2 must land as ONE part: the sink rolls at a \
         COMMIT boundary, so splitting it into 2 parts means a crash between them leaves \
         half a transaction durable"
    );

    c.query_drop(format!("INSERT INTO {tbl} VALUES (4,4),(5,5),(6,6)"))
        .expect("tx2");
    let r2 = cdc_run();
    assert!(
        r2.status.success(),
        "cdc run 2 failed:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );

    let ids = duckdb_dir_parquet_id_set(&out);
    let _ = parts_after_1;
    assert!(
        (4..=6).all(|i| ids.contains(&i)),
        "run 2 captured nothing of its own (ids {ids:?}) — the fixture is inert, so the \
         union below would pass without testing anything"
    );
    assert_eq!(
        ids,
        (1..=6).collect::<std::collections::BTreeSet<i64>>(),
        "both runs' rows must survive in one --output directory; a missing 1..=3 is the \
         clobber class (run 2's first part overwriting run 1's) AFTER the checkpoint \
         advanced past them — gone from the binlog and the destination both"
    );
}

/// `rivet cdc --output --format csv` — the WIRING, not the fidelity.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn mysql_cdc_cli_csv_output_is_wired_and_readable() {
    require_alive(LiveService::DuckDb);
    let mut c = conn();
    let tbl = unique_name("rivet_cdc_csv");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ck");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,1),(2,2),(3,3)"))
        .expect("seed");
    let sid = server_id_for(&tbl).to_string();
    let r = run_rivet_env(
        &[
            "cdc",
            "--source",
            MYSQL_CDC_URL,
            "--server-id",
            &sid,
            "--table",
            &tbl,
            "--checkpoint",
            ckpt.to_str().unwrap(),
            "--output",
            out.to_str().unwrap(),
            "--format",
            "csv",
        ],
        &[],
    );
    assert!(
        r.status.success(),
        "cdc --format csv failed — the CDC schema carries __op/__pos/__seq beside the \
         source columns, and a type CSV cannot serialize fails the whole run:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    assert!(
        !files_with_extension(&out, "csv").is_empty(),
        "no .csv part written; directory holds {} parquet file(s)",
        files_with_extension(&out, "parquet").len()
    );
    assert_eq!(
        duckdb_dir_csv_id_set(&out),
        (1..=3).collect::<std::collections::BTreeSet<i64>>(),
        "DuckDB must read back every captured id from the CSV parts"
    );
}

/// `rivet cdc --stream` and `--server-id`.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn mysql_cdc_cli_stream_with_a_cap_terminates_and_accepts_a_server_id() {
    let mut c = conn();
    let tbl = unique_name("rivet_cdc_stream");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ck");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,1),(2,2),(3,3)"))
        .expect("tx1");
    c.query_drop(format!("INSERT INTO {tbl} VALUES (4,4),(5,5),(6,6)"))
        .expect("tx2");

    let out = run_rivet_args_bounded(
        &[
            "cdc",
            "--source",
            MYSQL_CDC_URL,
            "--table",
            &tbl,
            "--checkpoint",
            ckpt.to_str().unwrap(),
            "--stream",
            "--max-events",
            "2",
            "--server-id",
            "919191",
        ],
        std::time::Duration::from_secs(60),
    );
    let stdout = out.expect(
        "`--stream --max-events` must TERMINATE on its own — a continuous drain that only \
         stops when the watchdog kills it wedges every scheduler slot it runs in",
    );

    let ids = ndjson_after_ids(&stdout, &tbl);
    assert_eq!(
        ids,
        (1..=3).collect::<std::collections::BTreeSet<i64>>(),
        "a cap of 2 must stop at tx1's boundary: {{1,2}} means it cut the transaction, \
         {{1..=6}} means the cap stopped nothing"
    );
    run_rivet_args_bounded(
        &[
            "cdc",
            "--source",
            MYSQL_CDC_URL,
            "--table",
            &tbl,
            "--checkpoint",
            ckpt.to_str().unwrap(),
            "--server-id",
            "919191",
        ],
        std::time::Duration::from_secs(60),
    )
    .expect("the follow-up bounded run terminates");

    let ckpt2 = d.path().join("ck2");
    Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt2.clone())
        .pin_binlog_here();
    let never = run_rivet_args_bounded(
        &[
            "cdc",
            "--source",
            MYSQL_CDC_URL,
            "--table",
            &tbl,
            "--checkpoint",
            ckpt2.to_str().unwrap(),
            "--stream",
            "--server-id",
            "919192",
        ],
        std::time::Duration::from_secs(15),
    );
    assert!(
        never.is_none(),
        "`--stream` with no cap must keep running until it is stopped — it terminated on \
         its own, which is the BOUNDED behaviour and means the flag never reached \
         `until_current` (dispatch.rs: `until_current: !stream`)"
    );
}

/// The PostgreSQL `--output` leg of the capped-run cell below.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_cli_a_capped_run_leaves_its_remainder_to_the_next_run() {
    use postgres::NoTls;
    let tbl = unique_name("rivet_cdc_pgdefer");
    let slot = unique_name("rivet_defer_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let out = tempfile::tempdir().unwrap();
    let dir = out.path().to_str().unwrap().to_string();
    let run = |cap: &[&str]| {
        let mut args = vec!["cdc", "--source", POSTGRES_CDC_URL, "--slot", &slot];
        args.extend(["--table", &tbl, "--output", &dir]);
        args.extend(cap);
        run_rivet_args_bounded(&args, std::time::Duration::from_secs(60))
            .expect("a bounded `rivet cdc` run terminates");
    };
    run(&[]);
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (1,1),(2,2),(3,3)"))
        .unwrap();
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (4,4),(5,5),(6,6)"))
        .unwrap();
    run(&["--max-events", "2"]);
    let ids: std::collections::BTreeSet<i64> =
        read_cdc_changes(out.path()).iter().map(|c| c.id).collect();
    assert_eq!(ids, (1..=3).collect(), "the cap stops at tx1's commit");
    run(&[]);
}

/// A `--max-events` run that stops at its cap owes the rest to the stream's next run.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn mysql_cdc_cli_a_capped_run_leaves_its_remainder_to_the_next_run() {
    let mut c = conn();
    let tbl = unique_name("rivet_cdc_defer");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());
    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ck").to_str().unwrap().to_string();
    let run = |cap: &[&str]| {
        let mut args = vec!["cdc", "--source", MYSQL_CDC_URL, "--table", &tbl];
        args.extend(["--checkpoint", &ckpt]);
        args.extend(cap);
        run_rivet_args_bounded(&args, std::time::Duration::from_secs(60))
            .expect("a bounded `rivet cdc` run terminates")
    };
    run(&[]);
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,1),(2,2),(3,3)"))
        .expect("tx1");
    c.query_drop(format!("INSERT INTO {tbl} VALUES (4,4),(5,5),(6,6)"))
        .expect("tx2");
    let capped = ndjson_after_ids(&run(&["--max-events", "2"]), &tbl);
    assert_eq!(capped, (1..=3).collect(), "the cap stops at tx1's commit");
    let rest = ndjson_after_ids(&run(&[]), &tbl);
    assert_eq!(rest, (4..=6).collect(), "the next run delivers tx2");
}

/// `rivet cdc --source-env` / `--source-file`, and the ArgGroup that keeps them mutually exclusive.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn mysql_cdc_cli_resolves_the_source_from_env_and_file_alike() {
    let mut c = conn();
    let tbl = unique_name("rivet_cdc_src");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let url_file = d.path().join("url.txt");
    std::fs::write(&url_file, MYSQL_CDC_URL).expect("write url file");

    let capture = |form: &[&str], envs: &[(&str, &str)]| -> std::collections::BTreeSet<i64> {
        let ckpt = d
            .path()
            .join(format!("ck_{}", form.join("_").replace('/', "_")));
        Rig::mysql_cdc(&tbl)
            .checkpoint_path(ckpt.clone())
            .pin_binlog_here();
        conn()
            .query_drop(format!("INSERT INTO {tbl} VALUES (7,7)"))
            .expect("seed");
        conn()
            .query_drop(format!("DELETE FROM {tbl} WHERE id = 7"))
            .expect("cleanup seed");
        let mut args: Vec<&str> = vec!["cdc"];
        args.extend_from_slice(form);
        let ck = ckpt.to_str().unwrap().to_string();
        args.extend_from_slice(&["--table", &tbl, "--checkpoint", &ck]);
        let out = run_rivet_args_bounded_env(&args, envs, std::time::Duration::from_secs(60))
            .unwrap_or_else(|| panic!("`rivet cdc {}` did not terminate", form.join(" ")));
        ndjson_after_ids(&out, &tbl)
    };

    let inline = capture(&["--source", MYSQL_CDC_URL], &[]);
    assert!(
        inline.contains(&7),
        "the inline form must capture the seeded change — the fixture is inert otherwise: {inline:?}"
    );
    let from_env = capture(
        &["--source-env", "RIVET_TEST_CDC_URL"],
        &[("RIVET_TEST_CDC_URL", MYSQL_CDC_URL)],
    );
    assert_eq!(
        from_env, inline,
        "`--source-env` must resolve to the same source as `--source`; exit 0 alone would \
         pass on a resolver that connected somewhere else"
    );
    let from_file = capture(&["--source-file", url_file.to_str().unwrap()], &[]);
    assert_eq!(
        from_file, inline,
        "`--source-file` must resolve to the same source as `--source`"
    );

    let both = run_rivet_env(
        &[
            "cdc",
            "--source",
            MYSQL_CDC_URL,
            "--source-env",
            "RIVET_TEST_CDC_URL",
            "--table",
            &tbl,
        ],
        &[("RIVET_TEST_CDC_URL", MYSQL_CDC_URL)],
    );
    assert!(
        !both.status.success(),
        "passing two source forms must be REFUSED, not silently resolved to one of them"
    );
}

/// A PARTIAL_JSON binlog must be REFUSED, not silently skipped.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn mysql_cdc_refuses_a_partial_json_binlog_instead_of_dropping_the_update() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_pj");
    let root_url = MYSQL_CDC_URL.replace("rivet:rivet@", "root:rivet@");
    let mut writer = mysql::Conn::new(mysql::Opts::from_url(&root_url).unwrap()).unwrap();
    use mysql::prelude::Queryable as _;
    writer
        .query_drop("SET SESSION binlog_row_value_options = 'PARTIAL_JSON'")
        .expect("set partial json on this session only");

    writer
        .query_drop(format!("DROP TABLE IF EXISTS {tbl}"))
        .unwrap();
    writer
        .query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, doc JSON)"))
        .unwrap();
    let _guard = Table(tbl.clone());
    writer
        .query_drop(format!(
            "INSERT INTO {tbl} VALUES (1, JSON_OBJECT('a', 1, 'pad', REPEAT('x', 400)))"
        ))
        .unwrap();

    let ck = d.path().join("pj.ckpt");
    let rig = || {
        Rig::mysql_cdc(&tbl)
            .checkpoint_path(ck.to_path_buf())
            .dest_path(d.path().join("out"))
    };
    rig().run_ok();

    writer
        .query_drop(format!(
            "UPDATE {tbl} SET doc = JSON_SET(doc, '$.a', 2) WHERE id = 1"
        ))
        .unwrap();

    let msg = rig().run_expect_fail();
    assert!(
        msg.contains("partial-JSON"),
        "the run must REFUSE and name the shape it met. Silently skipping the diff \
         loses the update while the checkpoint advances past it — a subset loss no \
         count check can see. Got: {msg}"
    );
    assert!(
        msg.contains("binlog_row_value_options"),
        "the message must name the SETTING to change — it is the only remediation an \
         operator gets: {msg}"
    );
    assert!(
        msg.contains("re-read"),
        "the message must say the un-acked span is re-read; an operator who believes \
         the data is already gone has no reason to fix the source and re-run: {msg}"
    );

    let other = unique_name("cdc_pj_other");
    writer
        .query_drop(format!("DROP TABLE IF EXISTS {other}"))
        .unwrap();
    writer
        .query_drop(format!("CREATE TABLE {other} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _other_guard = Table(other.clone());
    let other_ck = d.path().join("other.ckpt");
    let other_rig = || {
        Rig::mysql_cdc(&other)
            .checkpoint_path(other_ck.to_path_buf())
            .dest_path(d.path().join("out_other"))
    };
    other_rig().run_ok();

    writer
        .query_drop(format!(
            "UPDATE {tbl} SET doc = JSON_SET(doc, '$.a', 3) WHERE id = 1"
        ))
        .unwrap();
    writer
        .query_drop(format!("INSERT INTO {other} VALUES (7, 70)"))
        .unwrap();

    let rows: usize = other_rig()
        .run_and_read()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        rows, 1,
        "an export that does NOT capture the partial-JSON table must complete and \
         capture its own change — the undecodable event belongs to another table, \
         and the routing filter would have dropped it anyway"
    );
}

/// Sets `binlog_row_metadata` on the server at `root_url` and restores the prior value on drop.
pub(crate) struct RowMetadata(String, String);

impl RowMetadata {
    pub(crate) fn set(root_url: &str, to: &str) -> Self {
        let mut c = mysql::Conn::new(mysql::Opts::from_url(root_url).unwrap()).expect("root conn");
        let was: String = c
            .query_first("SELECT @@GLOBAL.binlog_row_metadata")
            .expect("read binlog_row_metadata")
            .expect("a value");
        c.query_drop(format!("SET GLOBAL binlog_row_metadata={to}"))
            .expect("SET GLOBAL binlog_row_metadata");
        Self(root_url.to_string(), was)
    }
}

impl Drop for RowMetadata {
    fn drop(&mut self) {
        if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(&self.0).unwrap()) {
            let _ = c.query_drop(format!("SET GLOBAL binlog_row_metadata={}", self.1));
        }
    }
}

/// A table on the isolated :3306 server, dropped on scope exit.
pub(crate) struct BatchServerTable(pub(crate) String, pub(crate) String);

impl Drop for BatchServerTable {
    fn drop(&mut self) {
        if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(&self.0).unwrap()) {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.1));
        }
    }
}

/// A `(id, a, b)` table on :3306 as root, for the row-metadata refusals.
pub(crate) fn row_metadata_fixture(prefix: &str) -> (String, mysql::PooledConn, BatchServerTable) {
    let root_url = MYSQL_URL.replace("rivet:rivet@", "root:rivet@");
    let table = unique_name(prefix).to_lowercase();
    let mut c = mysql::Pool::new(root_url.as_str())
        .and_then(|p| p.get_conn())
        .expect("root conn on :3306");
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table}(id INT PRIMARY KEY, a VARCHAR(9), b VARCHAR(9))"
    ))
    .unwrap();
    let guard = BatchServerTable(root_url.clone(), table.clone());
    (root_url, c, guard)
}

/// No run may leave a part, a delivered row, or a successful ledger row behind.
fn assert_nothing_delivered(rig: &Rig) {
    assert!(
        files_with_extension(&rig.out_dir(), "parquet").is_empty(),
        "a refused run must write no part"
    );
    let runs = StateDb::next_to_config(&rig.config_path()).export_runs();
    assert!(
        !runs.is_empty()
            && runs
                .iter()
                .all(|(_, status, _, rows)| status == "failed" && *rows == 0),
        "the ledger must record the run as failed with no rows: {runs:?}"
    );
}

/// docs/reference/compatibility.md: a `mode: cdc` run sends no session-level `SET` either.
#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_cdc_run_refused_through_a_transaction_pooler_leaves_its_session_as_it_was_postgres() {
    let _alone = pgbouncer_alone();
    let engine = SqlEngine::Pg;
    engine.alive();
    let (table, _guard) = engine.range_table("guar_pool_cdc");
    let slot = unique_name("guar_pool_slot");
    let _slot = Slot::on(POSTGRES_URL, slot.clone());
    let before = pooled_session_without_set_formats();
    let rig = Rig::pg_cdc(&table, &slot).source_url(PGBOUNCER_URL);
    for cycle in 1..=2 {
        let out = rig.run();
        assert!(
            !out.status.success(),
            "cycle {cycle}: the server behind the pooler runs wal_level = replica"
        );
        assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_PREREQUISITE", 1));
        assert_eq!(
            pooled_session(),
            before,
            "cycle {cycle}: the refused CDC run changed the session of the pooled connection"
        );
    }
    let slots: i64 = pg_connect()
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert_eq!(slots, 0, "a refused capture creates no slot");
}

/// `binlog_row_metadata = MINIMAL` at open is refused before the first run pins an anchor.
#[test]
#[ignore = "live: requires docker compose up -d mysql (:3306, log_bin=ON)"]
fn mysql_cdc_refuses_minimal_row_metadata_at_open() {
    let _serial = quiet_window_guard();
    let (root_url, mut c, t) = row_metadata_fixture("rivet_cdc_min");
    let _meta = RowMetadata::set(&root_url, "MINIMAL");
    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let rig = Rig::mysql_cdc(&t.1)
        .source_url(&root_url)
        .checkpoint_path(ckpt.clone());
    c.query_drop(format!("INSERT INTO {} VALUES (1,'AAA','BBB')", t.1))
        .unwrap();

    let out = rig.run();
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "MINIMAL must fail the run:\n{said}");
    assert!(said.contains("[RIVET_SOURCE_CDC_PREREQUISITE]"), "{said}");
    assert!(
        said.contains("the server has binlog_row_metadata = MINIMAL"),
        "the refusal names the setting and its value: {said}"
    );
    assert!(
        said.contains("`SET PERSIST binlog_row_metadata = FULL;`"),
        "the refusal names the SQL fix: {said}"
    );
    assert!(
        said.contains(REBASELINE_REMEDY),
        "the recovery is anchor first, then snapshot: {said}"
    );
    assert!(
        !ckpt.exists(),
        "a refused first run must not pin an anchor inside a MINIMAL span"
    );
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()),
        0,
        "no row delivered"
    );
    assert_nothing_delivered(&rig);

    let doc = run_rivet(&[
        "doctor",
        "--config",
        rig.config_path().to_str().unwrap(),
        "--json",
    ]);
    let report: serde_json::Value =
        serde_json::from_slice(&doc.stdout).expect("doctor --json output");
    let check = report["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .find(|c| c["name"] == "CDC binlog server config")
        .unwrap_or_else(|| panic!("doctor ran no CDC binlog server config check: {report}"))
        .clone();
    assert_eq!(check["ok"], false, "doctor must fail MINIMAL: {check}");
    assert!(
        check.to_string().contains("binlog_row_metadata"),
        "doctor names the setting: {check}"
    );
}

/// Under `initial: snapshot` the MINIMAL refusal comes before the anchor and the snapshot leg write anything.
#[test]
#[ignore = "live: requires docker compose up -d mysql (:3306, log_bin=ON)"]
fn mysql_cdc_initial_snapshot_is_refused_before_any_write() {
    let _serial = quiet_window_guard();
    let (root_url, mut c, t) = row_metadata_fixture("rivet_cdc_minsnap");
    let _meta = RowMetadata::set(&root_url, "MINIMAL");
    c.query_drop(format!("INSERT INTO {} VALUES (1,'AAA','BBB')", t.1))
        .unwrap();
    let d = tempfile::tempdir().unwrap();
    let (ckpt, out) = (d.path().join("cdc.ckpt"), d.path().join("out"));
    let err = Rig::mysql_cdc(&t.1)
        .source_url(&root_url)
        .cdc("initial: snapshot")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone())
        .run_expect_fail();
    assert!(err.contains("[RIVET_SOURCE_CDC_PREREQUISITE]"), "{err}");
    assert_refused_before_any_write(&out, &ckpt);
}

/// A PostgreSQL routing refusal.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_initial_snapshot_is_refused_before_any_write() {
    let cdc_db = CdcDb::new("cdc_refsnap");
    let slot = unique_name("rivet_refsnap_slot");
    let _slot = Slot::new(slot.clone());
    let name = unique_name("rivet_cdc_refsnap").to_lowercase();
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name}_base (id BIGINT PRIMARY KEY, v INT); \
         INSERT INTO {name}_base VALUES (1,10),(2,20); \
         CREATE VIEW {name} AS SELECT id, v FROM {name}_base"
    ))
    .unwrap();
    let d = tempfile::tempdir().unwrap();
    let (ckpt, out) = (d.path().join("cdc.ckpt"), d.path().join("out"));
    let err = Rig::pg_cdc(&name, &slot)
        .source_url(cdc_db.url())
        .cdc("initial: snapshot")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone())
        .run_expect_fail();
    assert!(err.contains("is a VIEW"), "{err}");
    assert_refused_before_any_write(&out, &ckpt);
    let slot_exists: bool = c
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert!(!slot_exists, "a refused run must create no slot");
}

/// ...and under the FULL the stack pins, a reorder across the resume boundary maps BY NAME.
#[test]
#[ignore = "live: requires docker compose mysql-cdc"]
fn mysql_cdc_full_row_metadata_maps_a_reorder_by_name_and_stays_quiet() {
    let table = unique_name("rivet_cdc_full");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table}(id INT PRIMARY KEY, a VARCHAR(9), b VARCHAR(9))"
    ))
    .unwrap();
    let _t = Table(table.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let rig = Rig::mysql_cdc(&table).checkpoint_path(ckpt.clone());
    rig.pin_binlog_here();

    c.query_drop(format!("INSERT INTO {table} VALUES (1,'AAA','BBB')"))
        .unwrap();
    c.query_drop(format!(
        "ALTER TABLE {table} MODIFY COLUMN b VARCHAR(9) AFTER id"
    ))
    .unwrap();

    let out = rig.out_dir();
    let said = rig.run_ok_capture();
    assert_eq!(
        duckdb_dir_parquet_distinct_strings(&out, "a"),
        ["AAA".to_string()].into_iter().collect(),
    );
    assert_eq!(
        duckdb_dir_parquet_distinct_strings(&out, "b"),
        ["BBB".to_string()].into_iter().collect()
    );
    assert!(!said.contains("binlog_row_metadata"), "{said}");
}

/// PostgreSQL crash between the checkpoint write and the slot ack.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn roast_pg_cdc_crash_between_checkpoint_and_ack_re_reads_and_releases_the_slot() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgckpt");
    let slot = unique_name("rivet_ckpt_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    for g in 0..12 {
        c.execute(&format!("INSERT INTO {tbl} VALUES ({g}, {g})"), &[])
            .unwrap();
    }

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .cdc("rollover: 3")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let crashed = rig.run_args_env(
        &[],
        &[("RIVET_TEST_PANIC_AT", "cdc_after_checkpoint_before_ack")],
    );
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1 — a fault that did not fire leaves this \
         test asserting an ordinary two-run export"
    );

    let confirmed = |c: &mut postgres::Client| -> String {
        c.query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .map(|r| r.get(0))
        .expect("the slot must exist after the crashed run")
    };
    let after_crash = confirmed(&mut c);

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let rig2 = Rig::pg_cdc(&tbl, &slot)
        .checkpoint_path(ckpt.clone())
        .dest_path(out2.clone());
    run_rivet_ok(&rig2.config_path());

    let mut got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
    got.extend(duckdb_dir_parquet_i64(&out2, "id"));
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        got, want,
        "every row must survive a crash between the checkpoint write and the slot \
         ack. A missing id means resume trusted the checkpoint FILE over the slot \
         and started past changes the slot had never released."
    );
    let after_resume = confirmed(&mut c);
    let advanced: i64 = c
        .query_one(
            &format!(
                "SELECT pg_wal_lsn_diff('{after_resume}'::pg_lsn, '{after_crash}'::pg_lsn)::bigint"
            ),
            &[],
        )
        .map(|r| r.get(0))
        .expect("compare the two anchor positions");

    assert!(
        advanced > 0,
        "the resume must ADVANCE the slot past what it re-read: confirmed_flush_lsn \
         went {after_crash} -> {after_resume} ({advanced} B). Standing still means \
         the WAL the crash pinned is pinned for good, and an operator who \
         crash-loops fills the disk"
    );
    assert!(
        !duckdb_dir_parquet_i64(&out2, "id").is_empty(),
        "the resume leg delivered NOTHING: the un-acked span was not re-read, so this \
         cell proves nothing about the boundary it exists for"
    );
}

/// A slot nobody is draining pins WAL forever, and `run` used to say nothing.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn roast_pg_cdc_run_warns_about_an_abandoned_slot_pinning_wal() {
    use postgres::NoTls;
    let tbl = unique_name("rivet_cdc_slotwarn");
    let slot = unique_name("rivet_warn_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let orphan = unique_name("rivet_orphan_slot").to_lowercase();
    let _orphan_guard = Slot::new(orphan.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    for s in [&slot, &orphan] {
        c.execute(
            "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
            &[s],
        )
        .unwrap();
    }
    c.execute(&format!("INSERT INTO {tbl} VALUES (1)"), &[])
        .unwrap();

    let rig = Rig::pg_cdc(&tbl, &slot);
    let quiet = rig.run_ok_capture();
    assert_eq!(
        duckdb_dir_parquet_i64(&rig.out_dir(), "id"),
        vec![1],
        "the run must still CAPTURE while it warns — a warning path that broke the \
         export would be worse than the silence it replaced"
    );
    assert!(
        !quiet.to_lowercase().contains("pinning"),
        "a healthy instance must stay silent, or the message is noise on every \
         scheduler cycle and stops being read. Got:\n{quiet}"
    );

    let loud = Rig::pg_cdc(&tbl, &slot).run_with_env("RIVET_TEST_SLOT_WAL_BAR", "1");
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&loud.stdout),
        String::from_utf8_lossy(&loud.stderr)
    );
    assert!(
        loud.status.success(),
        "a retention WARNING must never fail the run — the export is the thing that \
         drains the slot, so refusing here would make the problem permanent:\n{said}"
    );
    assert!(
        said.contains(&orphan),
        "the run must name the ABANDONED slot — it is the one nothing is draining, \
         and an operator cannot drop what they were not told about. Got:\n{said}"
    );
    assert!(
        said.contains("pg_drop_replication_slot"),
        "and hand over the command, not just the diagnosis. Got:\n{said}"
    );
    assert!(
        said.contains(&slot) && said.contains("This run will drain it"),
        "our OWN slot gets the other message — saying it will be drained, so nobody \
         drops a slot that was about to recover. Got:\n{said}"
    );
}

/// The TRUNCATE refusal must not throw away what it has already read.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn roast_pg_cdc_truncate_refusal_delivers_the_rows_it_already_read() {
    use postgres::NoTls;
    let ta = unique_name("rivet_cdc_trka").to_lowercase();
    let tb = unique_name("rivet_cdc_trkb").to_lowercase();
    let slot = unique_name("rivet_trk_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {ta}; DROP TABLE IF EXISTS {tb}; \
         CREATE TABLE {ta}(id int PRIMARY KEY, v text); \
         CREATE TABLE {tb}(id int PRIMARY KEY, v text)"
    ))
    .unwrap();
    let _ta = PgTable::adopt_on(POSTGRES_CDC_URL, ta.clone());
    let _tb = PgTable::adopt_on(POSTGRES_CDC_URL, tb.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    c.execute(
        &format!("INSERT INTO {tb} VALUES (1,'B_MUST_SURVIVE')"),
        &[],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {ta} VALUES (1,'a1')"), &[])
        .unwrap();
    c.execute(&format!("TRUNCATE {ta}"), &[]).unwrap();

    let rig = Rig::pg_cdc(&format!("public.{ta}"), &slot)
        .a_failed_run_may_leave(
            &[Leftover::OrphanPart, Leftover::FileLog, Leftover::CdcFlush],
            "the stream delivers the rows it read before the TRUNCATE it refuses",
        )
        .tables(&[&format!("public.{ta}"), &format!("public.{tb}")]);
    let said = rig.run_expect_fail();
    assert!(
        said.to_lowercase().contains("truncate"),
        "the run must still REFUSE — deferring the bail must not turn it into a \
         silent success. Got:\n{said}"
    );

    let got: std::collections::BTreeSet<String> =
        duckdb_dir_parquet_distinct_strings(&rig.out_dir().join(format!("public.{tb}")), "v");
    assert!(
        got.contains("B_MUST_SURVIVE"),
        "the bystander's insert committed BEFORE the truncate and must be delivered \
         — the remedy advances the slot past this point, so anything not delivered \
         here is lost for good. Got: {got:?}"
    );

    let remaining: Vec<String> = c
        .query(
            "SELECT data FROM pg_logical_slot_peek_changes($1, NULL, NULL)",
            &[&slot],
        )
        .expect("peek the slot")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .filter(|d| !d.starts_with("BEGIN") && !d.starts_with("COMMIT"))
        .filter(|d| d.contains(&ta) || d.contains(&tb))
        .collect();
    assert_eq!(
        remaining.len(),
        1,
        "the slot must hold ONLY the truncate — anything else of OURS still in it is \
         what `pg_replication_slot_advance` would destroy. Got: {remaining:?}"
    );
    assert!(
        remaining[0].contains("TRUNCATE"),
        "and that one thing is the truncate itself: {remaining:?}"
    );
}

/// A backlog written under MINIMAL is refused at its rows event once the server is FULL again.
#[test]
#[ignore = "live: requires docker compose up -d mysql (:3306, log_bin=ON)"]
fn mysql_cdc_refuses_a_minimal_backlog_after_the_server_is_switched_to_full() {
    let _serial = quiet_window_guard();
    let (root_url, mut c, t) = row_metadata_fixture("rivet_cdc_wire");
    let table = t.1.clone();
    let _meta = RowMetadata::set(&root_url, "FULL");
    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let rig = Rig::mysql_cdc(&table)
        .source_url(&root_url)
        .checkpoint_path(ckpt.clone());
    rig.pin_binlog_here();
    let anchored = std::fs::read_to_string(&ckpt).unwrap();
    let file = serde_json::from_str::<serde_json::Value>(&anchored).unwrap()["file"]
        .as_str()
        .unwrap()
        .to_string();

    c.query_drop("SET GLOBAL binlog_row_metadata=MINIMAL")
        .unwrap();
    c.query_drop(format!("INSERT INTO {table} VALUES (1,'AAA','BBB')"))
        .unwrap();
    c.query_drop("SET GLOBAL binlog_row_metadata=FULL").unwrap();
    c.query_drop(format!(
        "ALTER TABLE {table} MODIFY COLUMN b VARCHAR(9) AFTER id"
    ))
    .unwrap();
    c.query_drop(format!("INSERT INTO {table} VALUES (2,'CCC','DDD')"))
        .unwrap();
    let now: String = c
        .query_first("SELECT @@GLOBAL.binlog_row_metadata")
        .unwrap()
        .unwrap();
    assert_eq!(now, "FULL", "the server must LOOK healthy at open");

    let out = rig.run();
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !out.status.success(),
        "a nameless backlog must fail the run:\n{said}"
    );
    assert!(said.contains("[RIVET_SOURCE_CDC_UNDECODABLE]"), "{said}");
    assert!(
        said.contains(&format!(
            "`rivet`.`{table}`: the rows event ending at binlog position {file}:"
        )),
        "the refusal names the table and the binlog position: {said}"
    );
    assert!(said.contains("binlog_row_metadata = MINIMAL"), "{said}");
    assert!(
        said.contains("`SET PERSIST binlog_row_metadata = FULL;`"),
        "{said}"
    );
    assert_eq!(
        std::fs::read_to_string(&ckpt).unwrap(),
        anchored,
        "the checkpoint must not advance past a refused event"
    );
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()),
        0,
        "no row delivered"
    );
    assert_nothing_delivered(&rig);
}

/// A folded-twin refusal must name BOTH relations — resolution-first.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_folded_twin_refusal_names_both_relations() {
    let cdc_db = CdcDb::new("cdc_twin");
    let slot = unique_name("rivet_twin_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let mut c = cdc_db.connect();
    c.batch_execute(
        "CREATE TABLE \"MixedCase\" (id int, v text); \
         CREATE TABLE mixedcase (id int, other_col text, extra text)",
    )
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    let rig = Rig::pg_cdc("MixedCase", &slot).source_url(cdc_db.url());
    let said = rig.run_expect_fail();
    assert!(
        said.contains("public.mixedcase") && said.contains("public.MixedCase"),
        "the refusal must name BOTH relations — the folded one the probe resolves AND \
         the byte-exact one the router would capture. An operator shown only one \
         cannot know the collision exists. Got:\n{said}"
    );
    assert!(
        !said.contains("wal_level=logical and a role"),
        "a config problem must not wear the permissions hint. Got:\n{said}"
    );
}

/// A second database `db` on the CDC server, readable by `rivet`.
struct OtherDb(String);

impl OtherDb {
    fn with(db: &str, table: &str, ids: &[i64]) -> Self {
        let root = mysql::Pool::new("mysql://root:rivet@127.0.0.1:3307/rivet").expect("root pool");
        let mut rc = root.get_conn().expect("root conn");
        rc.query_drop(format!("DROP DATABASE IF EXISTS {db}"))
            .unwrap();
        rc.query_drop(format!("CREATE DATABASE {db}")).unwrap();
        rc.query_drop(format!(
            "CREATE TABLE {db}.{table}(id INT PRIMARY KEY, v VARCHAR(20))"
        ))
        .unwrap();
        for id in ids {
            rc.query_drop(format!("INSERT INTO {db}.{table} VALUES ({id}, 'foreign')"))
                .unwrap();
        }
        rc.query_drop(format!("GRANT SELECT ON {db}.* TO 'rivet'@'%'"))
            .unwrap();
        rc.query_drop("FLUSH PRIVILEGES").unwrap();
        OtherDb(db.to_string())
    }

    fn insert(&self, table: &str, id: i64) {
        let root = mysql::Pool::new("mysql://root:rivet@127.0.0.1:3307/rivet").expect("root pool");
        root.get_conn()
            .unwrap()
            .query_drop(format!(
                "INSERT INTO {}.{table} VALUES ({id}, 'foreign')",
                self.0
            ))
            .unwrap();
    }
}

impl Drop for OtherDb {
    fn drop(&mut self) {
        if let Ok(p) = mysql::Pool::new("mysql://root:rivet@127.0.0.1:3307/rivet")
            && let Ok(mut rc) = p.get_conn()
        {
            let _ = rc.query_drop(format!("DROP DATABASE IF EXISTS {}", self.0));
        }
    }
}

/// Every `id` in every Parquet part under `dir`, recursively, sorted.
fn ids_read_all(dir: &std::path::Path) -> Vec<i64> {
    use arrow::array::{Array, Int64Array};
    let mut ids = Vec::new();
    for f in crate::common::files_under(dir) {
        if f.extension().is_none_or(|x| x != "parquet") {
            continue;
        }
        let file = std::fs::File::open(&f).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        for b in reader {
            let b = b.unwrap();
            let col = b.column_by_name("id").expect("an id column");
            let col = arrow::compute::cast(col, &arrow::datatypes::DataType::Int64).unwrap();
            let col = col.as_any().downcast_ref::<Int64Array>().unwrap();
            ids.extend((0..col.len()).map(|i| col.value(i)));
        }
    }
    ids.sort_unstable();
    ids
}

/// #675: a bare name means the connection's own database when that database holds it.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn mysql_cdc_a_bare_name_captures_its_own_database_beside_a_same_named_table() {
    let _serial = cross_process_serial("mysql_cdc_ambiguity");
    let table = unique_name("rivet_cdc_amb").to_lowercase();
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table}(id INT PRIMARY KEY, v VARCHAR(20))"
    ))
    .unwrap();
    let _t = Table(table.clone());
    for id in [1, 2] {
        c.query_drop(format!("INSERT INTO {table} VALUES ({id}, 'own')"))
            .unwrap();
    }
    let other = OtherDb::with(&format!("amb_{}", std::process::id()), &table, &[101, 102]);

    let d = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_cdc(&table)
        .checkpoint_path(d.path().join("cdc.ckpt"))
        .cdc_line("initial: snapshot");
    rig.pin_binlog_here();
    rig.run_ok();
    c.query_drop(format!("INSERT INTO {table} VALUES (3, 'own')"))
        .unwrap();
    other.insert(&table, 103);
    rig.run_ok();
    assert_eq!(
        ids_read_all(&rig.out_dir()),
        vec![1, 2, 3],
        "the baseline and the changes are the connection database's rows; the other database's never"
    );
}

/// #675, as `rivet init --mode cdc` writes it.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn mysql_cdc_an_init_config_runs_as_written_beside_a_same_named_table() {
    let _serial = cross_process_serial("mysql_cdc_ambiguity");
    let table = unique_name("rivet_cdc_init_amb").to_lowercase();
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table}(id INT PRIMARY KEY, v VARCHAR(20))"
    ))
    .unwrap();
    let _t = Table(table.clone());
    for id in [1, 2] {
        c.query_drop(format!("INSERT INTO {table} VALUES ({id}, 'own')"))
            .unwrap();
    }
    let other = OtherDb::with(&format!("amb_init_{}", std::process::id()), &table, &[101]);
    let d = tempfile::tempdir().unwrap();
    let cfg = d.path().join("cdc.yaml");
    let env = [("RIVET_AMB_URL", MYSQL_CDC_URL)];
    let init = run_rivet_env(
        &[
            "init",
            "--source-env",
            "RIVET_AMB_URL",
            "--table",
            &table,
            "--mode",
            "cdc",
            "-o",
            cfg.to_str().unwrap(),
        ],
        &env,
    );
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    let yaml = std::fs::read_to_string(&cfg).unwrap();
    assert!(
        yaml.contains(&format!("table: {table}")),
        "init writes the bare name, as before:\n{yaml}"
    );
    let run = |what: &str| {
        let out = run_rivet_env(&["run", "-c", cfg.to_str().unwrap()], &env);
        assert!(
            out.status.success(),
            "{what}: the generated config must run as written:\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run("first run");
    c.query_drop(format!("INSERT INTO {table} VALUES (3, 'own')"))
        .unwrap();
    other.insert(&table, 102);
    run("second run");
    assert_eq!(ids_read_all(&d.path().join("output")), vec![1, 2, 3]);
}

/// #675 across the upgrade.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + RIVET_PREV_RELEASE_BIN"]
fn mysql_cdc_a_previous_release_stream_continues_beside_a_same_named_table() {
    let _serial = cross_process_serial("mysql_cdc_ambiguity");
    let table = unique_name("rivet_cdc_amb_up").to_lowercase();
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table}(id INT PRIMARY KEY, v VARCHAR(20))"
    ))
    .unwrap();
    let _t = Table(table.clone());
    for id in [1, 2] {
        c.query_drop(format!("INSERT INTO {table} VALUES ({id}, 'own')"))
            .unwrap();
    }
    let d = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_cdc(&table)
        .checkpoint_path(d.path().join("cdc.ckpt"))
        .cdc_line("initial: snapshot");
    let Some(old) = rig.as_previous_release() else {
        return;
    };
    rig.pin_binlog_here();
    old.run_ok();
    c.query_drop(format!("INSERT INTO {table} VALUES (3, 'own')"))
        .unwrap();
    old.run_ok();
    let other = OtherDb::with(&format!("amb_up_{}", std::process::id()), &table, &[101]);
    c.query_drop(format!("INSERT INTO {table} VALUES (4, 'own')"))
        .unwrap();
    other.insert(&table, 102);
    rig.run_ok();
    assert_eq!(
        ids_read_all(&rig.out_dir()),
        vec![1, 2, 3, 4],
        "one baseline, every change once, nothing of the other database"
    );
}

/// #675, kept: a bare name the connection's database does not hold.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn mysql_cdc_a_bare_name_only_another_database_holds_is_still_captured() {
    let _serial = cross_process_serial("mysql_cdc_ambiguity");
    let table = unique_name("rivet_cdc_elsewhere").to_lowercase();
    let other = OtherDb::with(&format!("amb_else_{}", std::process::id()), &table, &[]);
    let d = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_cdc(&table)
        .checkpoint_path(d.path().join("cdc.ckpt"))
        .no_oracle("the oracle reads the bare name in the connection's database, which does not hold it; the parts are read below");
    rig.pin_binlog_here();
    rig.run_ok();
    other.insert(&table, 7);
    other.insert(&table, 8);
    rig.run_ok();
    assert_eq!(ids_read_all(&rig.out_dir()), vec![7, 8]);
}

/// #675: a bare name and its qualified twin in ONE stream are refused by code at config load.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn mysql_cdc_a_bare_name_beside_its_qualified_twin_is_refused() {
    let _serial = cross_process_serial("mysql_cdc_ambiguity");
    let table = unique_name("rivet_cdc_twin").to_lowercase();
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table}(id INT PRIMARY KEY, v VARCHAR(20))"
    ))
    .unwrap();
    let _t = Table(table.clone());
    let db = format!("amb_twin_{}", std::process::id());
    let _other = OtherDb::with(&db, &table, &[]);
    let d = tempfile::tempdir().unwrap();
    let twin = format!("{db}.{table}");
    let rig = Rig::mysql_cdc(&table)
        .checkpoint_path(d.path().join("cdc.ckpt"))
        .tables(&[&table, &twin]);
    let said = rig.run_expect_fail();
    assert!(
        said.contains("[RIVET_CONFIG_CDC_SHAPE_INVALID]") && said.contains(&twin),
        "{said}"
    );
}

/// A TRUNCATE in the SAME transaction as a row change must still refuse.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn roast_pg_cdc_truncate_inside_a_transaction_with_rows_still_refuses() {
    use postgres::NoTls;
    let tbl = unique_name("rivet_cdc_wedge").to_lowercase();
    let slot = unique_name("rivet_wedge_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl}(id int PRIMARY KEY, v text)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    c.batch_execute(&format!(
        "BEGIN; INSERT INTO {tbl} VALUES (1,'a'); TRUNCATE {tbl}; COMMIT"
    ))
    .unwrap();

    let rig = Rig::pg_cdc(&format!("public.{tbl}"), &slot);
    let said = rig.run_expect_fail();
    assert!(
        said.to_lowercase().contains("truncate"),
        "the refusal must fire even though the truncate shares its transaction with \
         a row change — deferring it here strands it forever, because the COMMIT \
         that would flush the buffer is never reached. Got:\n{said}"
    );

    c.execute(&format!("INSERT INTO {tbl} VALUES (2,'after')"), &[])
        .unwrap();
    let again = Rig::pg_cdc(&format!("public.{tbl}"), &slot);
    let said2 = again.run_expect_fail();
    assert!(
        said2.to_lowercase().contains("truncate"),
        "the truncate still heads the window, so the refusal repeats — what must NOT \
         happen is a green run reporting zero rows over it. Got:\n{said2}"
    );
}

/// Classification must ask about the relation RESOLUTION elected.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_classification_asks_about_the_resolved_relation_not_the_search_path_one() {
    let cdc_db = CdcDb::new("cdc_resolved");
    let slot = unique_name("rivet_resolved_slot");
    let _slot = Slot::new(slot.clone());
    let name = unique_name("rivet_cdc_rv").to_lowercase();
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE SCHEMA IF NOT EXISTS deep; \
         CREATE TABLE deep.{name} (id BIGINT PRIMARY KEY, v INT); \
         CREATE VIEW public.{name} AS SELECT id, v FROM deep.{name}"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(
        &format!("INSERT INTO deep.{name} VALUES (1,10),(2,20)"),
        &[],
    )
    .unwrap();

    let rig = Rig::pg_cdc(&name, &slot).source_url(cdc_db.url());
    let out = run_rivet(&["run", "--config", rig.config_path().to_str().unwrap()]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !said.contains("is a VIEW"),
        "the view is the relation resolution DISCARDED — refusing the config because \
         of it contradicts the warning printed moments earlier in the same run, and \
         sends the operator to 'capture the BASE table' which is what rivet already \
         resolved to. Output:\n{said}"
    );
    assert!(
        out.status.success(),
        "a config the router would capture must not be refused:\n{said}"
    );
    let rows = read_all_parts(&rig.out_dir())
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert_eq!(
        rows, 2,
        "and it must actually capture the base table's two rows — a green run over \
         zero rows would pass the assertions above while proving nothing"
    );
}

/// A MATERIALIZED VIEW is not a guaranteed zero — refuse it and you refuse a capture that works.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_matview_is_a_loud_partial_not_a_refusal() {
    let cdc_db = CdcDb::new("cdc_matview");
    let slot = unique_name("rivet_mv_slot");
    let _slot = Slot::new(slot.clone());
    let base = unique_name("rivet_cdc_mvb").to_lowercase();
    let mv = format!("{base}_mv");
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {base} (id BIGINT PRIMARY KEY, v INT); \
         INSERT INTO {base} VALUES (1,10); \
         CREATE MATERIALIZED VIEW {mv} AS SELECT id, v FROM {base} WITH DATA; \
         CREATE UNIQUE INDEX ON {mv} (id)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "INSERT INTO {base} VALUES (2,20); REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}"
    ))
    .unwrap();

    let rig = Rig::pg_cdc(&format!("public.{mv}"), &slot).source_url(cdc_db.url());
    let out = run_rivet(&["run", "--config", rig.config_path().to_str().unwrap()]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "a matview refreshed CONCURRENTLY emits rows under its own name (MEASURED), \
         so refusing it refuses a working capture:\n{said}"
    );
    assert!(
        said.to_lowercase().contains("concurrently"),
        "and the run must WARN what it is actually delivering — refresh deltas, only \
         from CONCURRENTLY refreshes, never the base table's own changes. Silence \
         here is how someone ships a matview capture that sits at 0 rows forever \
         because their refresh is the plain form:\n{said}"
    );
    let rows = read_all_parts(&rig.out_dir())
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert!(
        rows > 0,
        "the CONCURRENTLY refresh's delta must reach the destination — a green run \
         over zero rows makes the assertions above vacuous:\n{said}"
    );
}

/// The routing WARNING must print once per run, like the note beside it.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_says_the_inheritance_gap_once_per_run_not_twice() {
    let cdc_db = CdcDb::new("cdc_partial_once");
    let slot = unique_name("rivet_partial_slot");
    let _slot = Slot::new(slot.clone());
    let parent = unique_name("rivet_cdc_inh").to_lowercase();
    let child = format!("{parent}_child");
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {parent} (id BIGINT PRIMARY KEY, v INT); \
         CREATE TABLE {child} () INHERITS ({parent})"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {parent} VALUES (1,10)"), &[])
        .unwrap();

    let rig = Rig::pg_cdc(&format!("public.{parent}"), &slot).source_url(cdc_db.url());
    let out = run_rivet(&["run", "--config", rig.config_path().to_str().unwrap()]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "an inheritance parent routes its OWN rows, so this run must succeed:\n{said}"
    );
    let times = said.matches(&child).filter(|_| true).count();
    assert!(
        times >= 1,
        "the fixture is not inert — the inheritance gap must be reported at all, \
         naming the child whose direct writes would be dropped:\n{said}"
    );
    let delivered: usize = read_all_parts(&rig.out_dir())
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        delivered, 1,
        "the parent's own row must still be captured — the inheritance gap is about \
         rows written DIRECTLY to a child, not about the parent's"
    );
    assert_eq!(
        said.matches("inheritance child table(s)").count(),
        1,
        "and exactly ONCE per run. The precheck runs twice (preflight, then open) \
         and the note beside this warning is already gated on that; this one was \
         not, so a `Partial` config repeats it on every scheduler cycle \
         forever:\n{said}"
    );
}

/// A relative `cdc.checkpoint:` must resolve against the CONFIG's directory.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn a_relative_cdc_checkpoint_follows_the_config_not_the_working_directory() {
    let mut c = conn();
    let tbl = unique_name("rivet_ckcwd").to_lowercase();
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::mysql_cdc(&tbl)
        .relative_checkpoint("./relative.ckpt")
        .dest_path(out.path().to_path_buf());
    let cwd_a = tempfile::tempdir().expect("cwd A");
    let cwd_b = tempfile::tempdir().expect("cwd B");
    let run_from = |dir: &std::path::Path| {
        let o = rig.run_in_dir(dir);
        assert!(
            o.status.success(),
            "run from {} failed:\n{}",
            dir.display(),
            String::from_utf8_lossy(&o.stderr)
        );
    };

    run_from(cwd_a.path());
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .expect("seed between runs");
    run_from(cwd_b.path());
    c.query_drop(format!("INSERT INTO {tbl} VALUES (3,30)"))
        .expect("seed after");
    run_from(cwd_b.path());

    let got: std::collections::BTreeSet<i64> = read_cdc_changes(out.path())
        .iter()
        .map(|ch| ch.id)
        .collect();
    assert_eq!(
        got,
        [1, 2, 3]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        "every row written across the three runs must be delivered. Before the fix \
         this returned [3]: run 2 resolved `./relative.ckpt` against ITS OWN working \
         directory, found nothing, and re-anchored at the current binlog position — \
         `status: success, rows: 0`, with ids 1 and 2 gone from both the stream and \
         the destination"
    );
}

/// The re-anchor warning must name the place rivet actually looked.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn the_reanchor_warning_names_the_config_relative_path_it_looked_at() {
    let mut c = conn();
    let tbl = unique_name("rivet_anchorwarn").to_lowercase();
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::mysql_cdc(&tbl)
        .relative_checkpoint("./anchorwarn.ckpt")
        .dest_path(out.path().to_path_buf());

    let o = rig.run_args(&[]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    assert!(o.status.success(), "the anchor run must succeed:\n{said}");
    assert!(
        said.contains("no checkpoint at"),
        "a first run re-anchors at the current binlog position and MUST say so at \
         `warn` — silent here is what round 9 measured as three green runs \
         delivering [3] of [1,2,3]. Got:\n{said}"
    );
    let expected = rig
        .config_path()
        .parent()
        .expect("config dir")
        .join("anchorwarn.ckpt");
    assert!(
        said.contains(&expected.display().to_string()),
        "the warning must name the path rivet LOOKED AT ({}), not a bare relative \
         spelling — the operator reading it is deciding whether to re-snapshot. \
         Got:\n{said}",
        expected.display()
    );
    c.query_drop(format!("INSERT INTO {tbl} VALUES (5,50)"))
        .expect("seed after the anchor");
    rig.run_ok();
    let after: std::collections::BTreeSet<i64> = read_cdc_changes(out.path())
        .iter()
        .map(|ch| ch.id)
        .collect();
    assert_eq!(
        after,
        [5].into_iter().collect::<std::collections::BTreeSet<i64>>(),
        "the run that emitted the warning must really have pinned an anchor — \
         everything after it is captured, and nothing before"
    );
    assert!(
        said.contains("CONFIG FILE's directory") && !said.contains("process working directory"),
        "the warning must explain the resolution that is in force. It said `resolved \
         against the process working directory` until round 15 — true before round 9 \
         and false since, which sends the operator to a directory the file was never \
         written to. Got:\n{said}"
    );
}

/// A bounded run EMITS its barrier, and each cycle's is distinct.
#[test]
#[ignore = "live+exclusive: holds an observer slot — set RIVET_TEST_EXCLUSIVE=1"]
fn a_bounded_run_emits_a_distinct_barrier_per_cycle() {
    use postgres::NoTls;

    if std::env::var("RIVET_TEST_EXCLUSIVE").is_err() {
        skip_live(
            "holds a passive observer slot, which two neighbours correctly report as \
             an abandoned foreign slot — run with RIVET_TEST_EXCLUSIVE=1 and \
             --test-threads=1",
        );
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_barrier").to_lowercase();
    let slot = unique_name("rivet_barrier_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .expect("rivet's slot");

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::pg_cdc(&tbl, &slot)
        .relative_checkpoint("./barrier.ckpt")
        .dest_path(out.path().to_path_buf());
    let _ = &d;

    let mut nonces: Vec<String> = Vec::new();
    for cycle in 0..2 {
        let watcher = unique_name("rivet_barrier_watch_slot").to_lowercase();
        let _w = Slot::new(watcher.clone());
        c.execute(
            "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
            &[&watcher],
        )
        .expect("watcher slot");
        c.batch_execute(&format!("INSERT INTO {tbl} VALUES ({cycle}, {cycle})"))
            .expect("seed");
        rig.run_ok();

        let rows = c
            .query(
                "SELECT data FROM pg_logical_slot_get_changes($1, NULL, NULL)",
                &[&watcher],
            )
            .expect("read the watcher");
        let found: Vec<String> = rows
            .iter()
            .map(|r| r.get::<_, String>(0))
            .filter(|l| l.starts_with("message:") && l.contains(&format!("content:{slot}-")))
            .collect();
        assert_eq!(
            found.len(),
            1,
            "cycle {cycle}: a bounded run must emit exactly ONE barrier. Zero means \
             the emit failed and the run fell back to the approximate ceiling \
             (which still terminates — that is the fail-OPEN design — but the \
             exactness is gone). More than one means a cycle emitted twice and a \
             later drain can stop at the wrong marker. Saw: {found:?}"
        );
        let nonce = found[0]
            .split(" content:")
            .nth(1)
            .expect("the barrier carries content")
            .trim()
            .to_string();
        assert!(
            nonce.contains(&slot),
            "the nonce must carry the SLOT, so two exports draining the same \
             database cannot end each other's runs: {nonce}"
        );
        nonces.push(nonce);
    }

    assert_ne!(
        nonces[0], nonces[1],
        "each cycle needs its OWN nonce. With a constant one, cycle N+1 stops at \
         cycle N's leftover barrier — which sits in the WAL ahead of everything \
         written since — and reports a clean drain over an unread backlog"
    );

    let got: std::collections::BTreeSet<i64> = read_cdc_changes(out.path())
        .iter()
        .map(|ch| ch.id)
        .collect();
    assert_eq!(
        got,
        [0, 1]
            .into_iter()
            .collect::<std::collections::BTreeSet<i64>>(),
        "both cycles' rows must be delivered: the barrier bounds a drain, it does \
         not shorten it, and a boundary that also drops data is not a boundary"
    );
}

/// A MySQL checkpoint from ANOTHER server must be refused, not resumed from.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn a_mysql_checkpoint_from_another_server_is_refused() {
    let mut c = conn();
    let tbl = unique_name("rivet_srvid").to_lowercase();
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::mysql_cdc(&tbl)
        .relative_checkpoint("./srvid.ckpt")
        .dest_path(out.path().to_path_buf());
    rig.run_ok();

    let ckpt = rig
        .config_path()
        .parent()
        .expect("config dir")
        .join("srvid.ckpt");
    let text = std::fs::read_to_string(&ckpt).expect("the anchor wrote a checkpoint");
    let mut doc: serde_json::Value = serde_json::from_str(&text).expect("checkpoint json");
    let recorded = doc
        .get("server_uuid")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    assert!(
        !recorded.is_empty(),
        "the fixture is inert: rivet recorded no server identity, so the check below \
         would pass over an unverifiable checkpoint and prove nothing"
    );

    doc["server_uuid"] = serde_json::json!("00000000-1111-2222-3333-444444444444");
    std::fs::write(&ckpt, doc.to_string()).expect("forge the identity");

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10)"))
        .expect("a change to resume into");
    let said = rig.run_expect_fail();
    assert!(
        said.contains("00000000-1111-2222-3333-444444444444") && said.contains(&recorded),
        "the refusal must name BOTH servers — an operator whose failover just \
         happened needs to know which is which. Got:\n{said}"
    );
    assert!(
        said.contains(REBASELINE_REMEDY),
        "and name the recovery: coordinates cannot be carried to a new server at \
         all, so a re-snapshot is the only path. Got:\n{said}"
    );

    assert!(
        read_cdc_changes(out.path()).is_empty(),
        "a refused resume must deliver nothing — reading from a foreign server's \
         coordinates is exactly what the refusal exists to prevent"
    );
}

/// The identity the anchor records must SURVIVE the first captured transaction.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn a_mysql_checkpoint_keeps_the_server_identity_after_a_captured_transaction() {
    let mut c = conn();
    let tbl = unique_name("rivet_srvid_keep").to_lowercase();
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::mysql_cdc(&tbl)
        .relative_checkpoint("./keep.ckpt")
        .dest_path(out.path().to_path_buf());
    rig.run_ok();
    let ckpt = rig
        .config_path()
        .parent()
        .expect("config dir")
        .join("keep.ckpt");
    let read = |label: &str| -> serde_json::Value {
        serde_json::from_str(&std::fs::read_to_string(&ckpt).expect(label)).expect("json")
    };
    let anchored = read("the anchor wrote a checkpoint");
    let uuid = anchored["server_uuid"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        !uuid.is_empty(),
        "inert fixture: the anchor recorded no identity"
    );

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10)"))
        .expect("a change to capture");
    rig.run_ok();
    assert!(
        !read_cdc_changes(out.path()).is_empty(),
        "inert fixture: nothing was captured, so no commit rewrote the checkpoint"
    );

    let committed = read("the commit rewrote the checkpoint");
    assert_ne!(
        committed["pos"], anchored["pos"],
        "inert fixture: the position did not advance past the anchor"
    );
    assert_eq!(
        committed["server_uuid"].as_str(),
        Some(uuid.as_str()),
        "the commit save must carry the server identity the anchor recorded — without \
         it the next resume cannot tell this server from a foreign one: {committed}"
    );
    assert!(
        committed.get("gtid_executed").is_some(),
        "the GTID tier must survive too: {committed}"
    );
}

/// REGENERATE `tests/fixtures/pgoutput/*.hex` from the rig's own scenarios.
#[test]
#[ignore = "live+writes: regenerates tests/fixtures/pgoutput from the pg-cdc stand"]
fn regenerate_the_pgoutput_fixture_from_the_rig_scenarios() {
    use postgres::NoTls;

    if std::env::var("RIVET_REGENERATE_FIXTURES").is_err() {
        skip_live(
            "regenerate_the_pgoutput_fixture: set RIVET_REGENERATE_FIXTURES=1 to \
             rewrite tests/fixtures/pgoutput — it is a generator, not a check",
        );
        return;
    }

    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    let tbl = "rivet_pgout_fx";
    let slot = unique_name("rivet_pgout_fx_slot");
    let _slot = Slot::new(slot.clone());
    let pubn = "rivet_pgout_fx_pub";

    let _ = c.batch_execute(&format!("DROP PUBLICATION IF EXISTS {pubn}"));
    let _ = c.batch_execute(&format!("DROP TABLE IF EXISTS {tbl}"));

    c.batch_execute(&pg_hard_types_ddl(tbl))
        .expect("create the hard-types table");
    let _t = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.to_string());
    c.batch_execute(&format!("CREATE PUBLICATION {pubn} FOR TABLE {tbl}"))
        .expect("publication");
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
        &[&slot],
    )
    .expect("pgoutput slot");

    c.batch_execute(&in_one_transaction(&[
        pg_hard_types_row(tbl, 42),
        pg_all_null_row(tbl, 43),
        pg_hard_types_row(tbl, 44),
    ]))
    .expect("a multi-row transaction");
    c.batch_execute(&format!(
        "BEGIN; UPDATE {tbl} SET txt='u1' WHERE id=42; DELETE FROM {tbl} WHERE id=43; COMMIT;"
    ))
    .expect("update + delete under DEFAULT identity");
    c.batch_execute(&format!("ALTER TABLE {tbl} REPLICA IDENTITY FULL"))
        .expect("full identity");
    c.batch_execute(&format!(
        "BEGIN; UPDATE {tbl} SET txt='u-full' WHERE id=44; COMMIT;"
    ))
    .expect("update under FULL identity");
    c.batch_execute(&format!("TRUNCATE {tbl}"))
        .expect("truncate");

    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pgoutput");
    std::fs::create_dir_all(&dir).expect("fixture dir");
    for (file, binary) in [("messages.hex", false), ("binary_values.hex", true)] {
        if binary {
            c.batch_execute(&in_one_transaction(&[
                pg_hard_types_row(tbl, 52),
                pg_all_null_row(tbl, 53),
                pg_hard_types_row(tbl, 54),
            ]))
            .expect("re-seed for the binary capture");
            c.batch_execute(&format!("ALTER TABLE {tbl} REPLICA IDENTITY DEFAULT"))
                .expect("back to default identity");
            c.batch_execute(&format!(
                "BEGIN; UPDATE {tbl} SET txt='u2' WHERE id=52; DELETE FROM {tbl} WHERE id=53; COMMIT;"
            ))
            .expect("re-seed update + delete");
            c.batch_execute(&format!("ALTER TABLE {tbl} REPLICA IDENTITY FULL"))
                .expect("full identity");
            c.batch_execute(&format!(
                "BEGIN; UPDATE {tbl} SET txt='u2-full' WHERE id=54; COMMIT;"
            ))
            .expect("re-seed full-identity update");
            c.batch_execute(&format!("TRUNCATE {tbl}"))
                .expect("re-seed truncate");
        }
        c.batch_execute("SELECT pg_logical_emit_message(false, 'rivet', 'fixture-barrier-nonce')")
            .expect("emit a barrier");
        let sql = format!(
            "SELECT encode(data,'hex') FROM pg_logical_slot_get_binary_changes($1, NULL, NULL, \
             'proto_version','1','publication_names','{pubn}','messages','true'{}) ",
            if binary { ",'binary','true'" } else { "" }
        );
        let rows = c.query(sql.as_str(), &[&slot]).expect("read the slot");
        let hex: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        assert!(
            hex.len() >= 8,
            "{file}: captured only {} messages — the scenario did not produce a \
             fixture worth grading against",
            hex.len()
        );
        std::fs::write(dir.join(file), hex.join("\n") + "\n").expect("write the fixture");
        eprintln!(
            "wrote {} ({} messages)",
            dir.join(file).display(),
            hex.len()
        );
    }

    let _ = c.batch_execute(&format!("DROP PUBLICATION IF EXISTS {pubn}"));
}

/// The SYMPTOM half of the two-phase probe.
#[test]
#[ignore = "live+exclusive: RESTARTS postgres-cdc — set RIVET_TEST_EXCLUSIVE=1"]
fn a_rolled_back_prepared_transaction_is_never_published_postgres() {
    use postgres::NoTls;

    if std::env::var("RIVET_TEST_EXCLUSIVE").is_err() {
        skip_live(
            "restarts postgres-cdc, which breaks concurrent tests — run with \
             RIVET_TEST_EXCLUSIVE=1 and --test-threads=1",
        );
        return;
    }

    let _setting =
        PgSetting::set_with_restart("max_prepared_transactions", "10", "rivet-postgres-cdc-1");
    assert_ne!(
        PgSetting::current("max_prepared_transactions"),
        "0",
        "the flip did not land — every assertion below would then pass over a \
         server that cannot PREPARE at all, which is not evidence of anything"
    );

    let tbl = unique_name("rivet_2pcsym").to_lowercase();
    let slot = unique_name("rivet_2pcsym_slot").to_lowercase();
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let _slot = Slot::new(slot.clone());

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::pg_cdc(&tbl, &slot).dest_path(out.path().to_path_buf());
    rig.run_ok();

    c.batch_execute(&format!(
        "BEGIN; INSERT INTO {tbl} VALUES (1, 100); PREPARE TRANSACTION 'rivet_p1'"
    ))
    .expect("prepare");
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (2, 200)"))
        .expect("an unrelated commit while the branch is prepared");
    let outstanding: i64 = c
        .query_one("SELECT count(*) FROM pg_prepared_xacts", &[])
        .expect("count prepared")
        .get(0);
    assert!(
        outstanding >= 1,
        "the fixture is inert: no prepared transaction was outstanding, so there is \
         nothing that could have been published early"
    );
    c.batch_execute("ROLLBACK PREPARED 'rivet_p1'")
        .expect("roll the branch back");

    let live: Vec<i64> = c
        .query(&format!("SELECT id FROM {tbl} ORDER BY id"), &[])
        .expect("read the source")
        .iter()
        .map(|r| r.get::<_, i64>(0))
        .collect();
    assert_eq!(live, vec![2], "the source must hold only the committed row");

    rig.run_ok();
    let got: std::collections::BTreeSet<i64> = read_cdc_changes(out.path())
        .iter()
        .map(|ch| ch.id)
        .collect();
    assert_eq!(
        got,
        [2].into_iter().collect::<std::collections::BTreeSet<i64>>(),
        "id=1 was ROLLED BACK and must never reach the destination. PostgreSQL is \
         immune by construction — a non-two-phase slot decodes only at COMMIT \
         PREPARED and a rollback emits no records — and this asserts the property \
         rather than the mechanism, so it also fails if the decoding model changes."
    );
}

/// PostgreSQL's slot must stay NON-two-phase.
#[test]
#[ignore = "live: requires docker compose postgres-cdc (wal_level=logical)"]
fn the_pg_slot_is_not_two_phase_so_a_prepared_transaction_cannot_be_published_early() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_2pc").to_lowercase();
    let slot = unique_name("rivet_2pc_slot").to_lowercase();
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let _slot = Slot::new(slot.clone());

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::pg_cdc(&tbl, &slot).dest_path(out.path().to_path_buf());
    rig.run_ok();

    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .expect("seed after the anchor");
    rig.run_ok();
    let got: std::collections::BTreeSet<i64> = read_cdc_changes(out.path())
        .iter()
        .map(|ch| ch.id)
        .collect();
    assert_eq!(
        got,
        [1].into_iter().collect::<std::collections::BTreeSet<i64>>(),
        "the slot rivet created must actually capture — a two-phase assertion over \
         an inert slot grades nothing"
    );

    let row = c
        .query_one(
            "SELECT two_phase, plugin FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .expect("rivet must have created its slot during the run");
    let two_phase: bool = row.get(0);
    let plugin: String = row.get(1);
    assert_eq!(
        plugin, "test_decoding",
        "the reader this immunity reasons about"
    );
    assert!(
        !two_phase,
        "the slot became TWO-PHASE. PostgreSQL then decodes a transaction at \\
         `PREPARE TRANSACTION`, before it is committed — and rivet has no framing \\
         for a prepared transaction anywhere in the sink, so a `ROLLBACK PREPARED` \\
         would leave those rows published under the next transaction's position. \\
         That is exactly what MySQL's XA path did (measured: source held one row, \\
         rivet delivered two). Enabling two-phase decoding needs the per-xid \\
         framing FIRST."
    );
    let _ = d;
}

/// A PREPARED XA branch must never be published by another transaction's commit.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn a_prepared_xa_branch_is_never_published_by_another_transactions_commit() {
    let mut c = conn();
    let tbl = unique_name("rivet_xa").to_lowercase();
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::mysql_cdc(&tbl).dest_path(out.path().to_path_buf());
    rig.run_ok();

    let xid = format!("x{}", std::process::id());
    c.query_drop(format!(
        "XA START '{xid}'; INSERT INTO {tbl} VALUES (1,100); XA END '{xid}'; \
         XA PREPARE '{xid}'"
    ))
    .expect("prepare an XA branch");
    c.query_drop(format!("INSERT INTO {tbl} VALUES (2,200)"))
        .expect("an unrelated commit");
    c.query_drop(format!("XA ROLLBACK '{xid}'"))
        .expect("roll the branch back");

    let live: Vec<i64> = c
        .query(format!("SELECT id FROM {tbl} ORDER BY id"))
        .expect("read the source");
    assert_eq!(
        live,
        vec![2],
        "the fixture is inert: the source must hold ONLY the committed row, or there \
         is no fabrication to detect"
    );

    let o = rig.run_args(&[]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    let got: std::collections::BTreeSet<i64> = read_cdc_changes(out.path())
        .iter()
        .map(|ch| ch.id)
        .collect();
    assert!(
        !got.contains(&1),
        "id=1 was ROLLED BACK and must never reach the destination; delivered {got:?}. \
         Before the fix this held {{1, 2}} at `status: success` — a row that never \
         existed, which no later change event can retract."
    );
    assert!(
        !o.status.success() && said.contains("XA PREPARE") && said.contains(&tbl),
        "the run must refuse, and NAME the branch and the table — publishing nothing \
         while exiting 0 would advance the checkpoint past the branch and turn this \
         into silent loss. Got:\n{said}"
    );
}

/// `rivet doctor` must grade the checkpoint the RUN will open.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn doctor_grades_the_checkpoint_the_run_opens_not_one_relative_to_the_shell() {
    let mut c = conn();
    let tbl = unique_name("rivet_ckdoctor").to_lowercase();
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::mysql_cdc(&tbl)
        .relative_checkpoint("./doctor.ckpt")
        .dest_path(out.path().to_path_buf());

    rig.run_ok();
    let ckpt = rig
        .config_path()
        .parent()
        .expect("config dir")
        .join("doctor.ckpt");
    assert!(
        ckpt.is_file(),
        "the fixture is inert: run 1 wrote no checkpoint at {}, so both doctor \
         invocations below would agree on `not yet written` and prove nothing",
        ckpt.display()
    );

    let say = |o: &std::process::Output| {
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        );
        text.lines()
            .find(|l| l.contains("CDC checkpoint"))
            .unwrap_or("<no CDC checkpoint check ran>")
            .to_string()
    };

    c.query_drop(format!("INSERT INTO {tbl} VALUES (7,70)"))
        .expect("seed after the anchor");
    rig.run_ok();
    let resumed: std::collections::BTreeSet<i64> = read_cdc_changes(out.path())
        .iter()
        .map(|ch| ch.id)
        .collect();
    assert_eq!(
        resumed,
        [7].into_iter().collect::<std::collections::BTreeSet<i64>>(),
        "the checkpoint under test must be a live resume position, not an inert file"
    );

    let elsewhere = tempfile::tempdir().expect("a different working directory");
    let from_config = say(&rig.cli_in_dir(&["doctor"], rig.config_path().parent().unwrap()));
    let from_elsewhere = say(&rig.cli_in_dir(&["doctor"], elsewhere.path()));

    assert!(
        from_config.contains("binlog."),
        "the control half is broken — doctor run from the config's own directory \
         must read the position the run just wrote, got:\n{from_config}"
    );
    let identity =
        |line: &str| -> String { line.split(", backlog").next().unwrap_or(line).to_string() };
    assert_eq!(
        identity(&from_config),
        identity(&from_elsewhere),
        "doctor described two different checkpoints for one config. The verdict \
         from another working directory is the one deployments actually see, and \
         `no checkpoint yet` is GREEN — so a position below binlog retention grades \
         [OK] and the run then dies with ERROR 1236.\n  from config:    {from_config}\n  from elsewhere: {from_elsewhere}"
    );
}

/// An existing CWD-relative checkpoint must keep working.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn an_existing_working_directory_checkpoint_is_not_stranded_by_the_new_resolution() {
    let mut c = conn();
    let tbl = unique_name("rivet_ckcompat").to_lowercase();
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::mysql_cdc(&tbl)
        .relative_checkpoint("./legacy.ckpt")
        .dest_path(out.path().to_path_buf());
    let cfg = rig.config_path();
    let cwd = tempfile::tempdir().expect("cwd");

    let run = || {
        let o = rig.run_in_dir(cwd.path());
        assert!(
            o.status.success(),
            "run failed:\n{}",
            String::from_utf8_lossy(&o.stderr)
        );
        format!(
            "{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        )
    };

    run();
    let by_config = cfg.parent().expect("config dir").join("legacy.ckpt");
    assert!(by_config.is_file(), "run 1 must have written a checkpoint");
    rig.move_checkpoint(&by_config, &cwd.path().join("legacy.ckpt"), cwd.path());

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .expect("seed between runs");
    let said = run();
    assert!(
        said.contains("relative to the working directory"),
        "the run must SAY it fell back, and where the file should move to — a silent \
         fallback leaves the deployment permanently dependent on where rivet is \
         invoked from. Got:\n{said}"
    );
    c.query_drop(format!("INSERT INTO {tbl} VALUES (3,30)"))
        .expect("seed after");
    run();

    let got: std::collections::BTreeSet<i64> = read_cdc_changes(out.path())
        .iter()
        .map(|ch| ch.id)
        .collect();
    assert_eq!(
        got,
        [1, 2, 3]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        "and nothing may be skipped across the upgrade: an existing checkpoint that \
         the new resolution cannot see would re-anchor at the current position, \
         which is the loss this whole change removes"
    );
}

/// One opaque leaf must not discard the whole JSON document.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn a_json_opaque_leaf_is_refused_loudly_and_only_for_the_captured_table() {
    let mut c = conn();
    let tbl = unique_name("rivet_jsop").to_lowercase();
    let bystander = format!("{tbl}_other");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, doc JSON); \
         DROP TABLE IF EXISTS {bystander}; \
         CREATE TABLE {bystander} (id BIGINT PRIMARY KEY, doc JSON)"
    ))
    .expect("create tables");
    let _t = MysqlCdcTable(tbl.clone());
    let _b = MysqlCdcTable(bystander.clone());

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::mysql_cdc(&tbl).dest_path(out.path().to_path_buf());
    rig.run_ok();

    c.query_drop(format!(
        "INSERT INTO {bystander} VALUES (1, JSON_OBJECT('amt', CAST(1.50 AS DECIMAL(10,2)))); \
         INSERT INTO {tbl} VALUES (1, '{{\"sku\": \"A1\"}}')"
    ))
    .expect("seed");
    rig.run_ok();
    let delivered: usize = read_all_parts(out.path())
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        delivered, 1,
        "an opaque leaf in an UNCAPTURED table must not affect this run — the binlog \
         dump is server-wide, so a refusal there would wedge capture of every table \
         that merely shares the stream"
    );

    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (2, JSON_OBJECT('amt', CAST(9.99 AS DECIMAL(10,2))))"
    ))
    .expect("seed opaque");
    let said = rig.run_expect_fail();
    assert!(
        said.contains("doc"),
        "the refusal must name the COLUMN — an operator cannot act on 'some JSON \
         somewhere'. Got:\n{said}"
    );
    assert!(
        said.contains(REBASELINE_REMEDY),
        "and hand over what actually works: the server renders this document \
         correctly for a snapshot read, so the stream's re-baseline is the way out. \
         Got:\n{said}"
    );
}

/// An idle cycle must not blind `validate --depth full`.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn an_idle_cycle_does_not_blind_validate_to_a_missing_part() {
    let mut c = conn();
    let tbl = unique_name("rivet_vblind").to_lowercase();
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let out = tempfile::tempdir().expect("out dir");
    let rig = Rig::mysql_cdc(&tbl).dest_path(out.path().to_path_buf());
    rig.run_ok();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20),(3,30)"))
        .expect("seed");
    rig.run_ok();
    let parts = files_with_extension(out.path(), "parquet");
    assert_eq!(parts.len(), 1, "fixture: one part must have been written");

    rig.run_ok();
    let canonical: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out.path().join("manifest.json")).expect("read canonical"),
    )
    .expect("parse canonical");
    assert_eq!(
        canonical["parts"].as_array().map(Vec::len),
        Some(0),
        "fixture: the idle cycle must really have emptied the canonical pointer — \
         without that this test grades nothing"
    );

    std::fs::remove_file(&parts[0]).expect("delete the part");
    let v = rig.cli(&["validate", "--depth", "full"]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&v.stdout),
        String::from_utf8_lossy(&v.stderr)
    );
    assert!(
        !v.status.success(),
        "a deleted part must fail validation. Before the fix this exited 0 with \
         `PASSED, 0 parts verified` — the canonical declared nothing, so validate \
         verified nothing and said so in words that read as a clean bill of \
         health:\n{said}"
    );
    assert!(
        said.to_uppercase().contains("MISSING"),
        "and it must name the missing part, not merely fail:\n{said}"
    );
}

/// A bounded run must reach its OPEN-TIME bound across an empty-transaction span.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn a_bounded_run_reaches_its_bound_across_an_empty_transaction_span() {
    let cdc_db = CdcDb::new("cdc_empty_span");
    let slot = unique_name("rivet_espan_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let tbl = unique_name("rivet_cdc_espan").to_lowercase();
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10)"), &[])
        .unwrap();
    for i in 0..6 {
        c.batch_execute(&format!(
            "CREATE TEMP TABLE espan_tmp{i}(x int); DROP TABLE espan_tmp{i}"
        ))
        .unwrap();
    }
    c.execute(
        &format!("INSERT INTO {tbl} VALUES (2,20),(3,30),(4,40)"),
        &[],
    )
    .unwrap();

    let rig = Rig::pg_cdc(&format!("public.{tbl}"), &slot)
        .source_url(cdc_db.url())
        .cdc_line("rollover: 5");
    rig.run_ok();

    let got: std::collections::BTreeSet<i64> = read_cdc_changes(&rig.out_dir())
        .iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(
        got,
        [1, 2, 3, 4]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        "ONE bounded run must deliver every row committed before it opened. Before \
         the fix this returned {{1}} — the run met the empty DDL span, exhausted, and \
         wrote `_SUCCESS` over a prefix missing three quarters of its data, with the \
         slot still pinned behind it"
    );
}

/// After a mid-window roll, a failed final part must leave the slot at the last WRITTEN commit.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn pg_cdc_a_failed_final_part_after_a_mid_window_roll_leaves_the_slot_behind_it() {
    use std::collections::BTreeSet;
    let cdc_db = CdcDb::new("cdc_ack_write");
    let slot = unique_name("rivet_ackw_slot").to_lowercase();
    let _slot = Slot::new(slot.clone());
    let ta = unique_name("rivet_cdc_ackw_a").to_lowercase();
    let tb = unique_name("rivet_cdc_ackw_b").to_lowercase();
    let mut c = cdc_db.connect();
    for t in [&ta, &tb] {
        c.batch_execute(&format!(
            "CREATE TABLE {t} (id BIGINT PRIMARY KEY, v INT, pad TEXT)"
        ))
        .unwrap();
    }
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(
        &format!(
            "INSERT INTO {ta} VALUES (1, 10, repeat('x', 700000)), (2, 20, repeat('y', 700000))"
        ),
        &[],
    )
    .unwrap();
    let after_t1: String = c
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);
    c.execute(&format!("INSERT INTO {tb} VALUES (21, 210, 't2')"), &[])
        .unwrap();
    c.execute(&format!("INSERT INTO {tb} VALUES (31, 310, 't3')"), &[])
        .unwrap();

    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join(&tb), b"in the way").unwrap();
    let rig = Rig::pg_cdc(&ta, &slot)
        .source_url(cdc_db.url())
        .tables(&[&ta, &tb])
        .export_named("ackw_cdc")
        .cdc_line("rollover: 1000")
        .cdc_line("rollover_memory_mb: 1")
        .a_failed_run_may_leave(
            &[Leftover::OrphanPart, Leftover::FileLog, Leftover::CdcFlush],
            "the mid-window roll commits table A's part before the final part fails",
        )
        .dest_path(out.clone());
    let said = rig.run_expect_fail();

    let ids = |dir: &std::path::Path| -> BTreeSet<i64> {
        read_cdc_changes(dir).iter().map(|c| c.id).collect()
    };
    assert_eq!(
        ids(&out.join(&ta)),
        BTreeSet::from([1, 2]),
        "fixture: run 1 must have rolled and declared T1's part BEFORE it failed — \
         otherwise there was no mid-window ack and this grades an ordinary failed run:\n{said}"
    );
    let held: bool = c
        .query_one(
            &format!(
                "SELECT confirmed_flush_lsn <= '{after_t1}'::pg_lsn \
                 FROM pg_replication_slots WHERE slot_name = $1"
            ),
            &[&slot],
        )
        .unwrap()
        .get(0);
    let confirmed: String = c
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0);

    assert!(
        held,
        "the failed run acknowledged the slot past what it wrote: confirmed_flush_lsn \
         {confirmed} is beyond {after_t1}, the WAL position right after T1 — the only \
         transaction whose part was written"
    );

    std::fs::remove_file(out.join(&tb)).unwrap();
    rig.run_ok();
    assert_eq!(
        ids(&out.join(&tb)),
        BTreeSet::from([21, 31]),
        "T2 and T3 were committed before run 1 opened and never written; the re-run \
         must deliver them. confirmed_flush_lsn after the failed run was {confirmed} \
         (T1 ended at or before {after_t1}):\n{said}"
    );
}

/// A transaction past the memory cap SPILLS to disk and delivers exactly the rows a transaction under the cap delivers.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_a_transaction_past_the_memory_cap_spills_rather_than_failing() {
    use postgres::NoTls;
    const ROWS: usize = 400;
    const CAP: usize = 50;
    /// Rows in the SECOND transaction, which follows the spilled one in the same peek window.
    const TAIL_TX: usize = 3;

    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_spill_pg");
    let slot_spill = unique_name("rivet_spill_slot");
    let _slot_spill = Slot::new(slot_spill.clone());
    let slot_plain = unique_name("rivet_plain_slot");
    let _slot_plain = Slot::new(slot_plain.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} {ONE_TRANSACTION_DDL}"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    for slot in [&slot_spill, &slot_plain] {
        c.execute(
            "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
            &[slot],
        )
        .unwrap();
    }
    c.batch_execute(&one_transaction_of(&tbl, ROWS)).unwrap();
    c.batch_execute(&transaction_over(&tbl, ROWS + 1..=ROWS + TAIL_TX))
        .unwrap();

    let out_plain = d.path().join("out_plain");
    std::fs::create_dir_all(&out_plain).unwrap();

    let spill_rig = Rig::pg_cdc(&tbl, &slot_spill).census_oracle();
    let out_spill = spill_rig.out_dir();
    let a = spill_rig.run_with_envs(&[
        ("RIVET_CDC_MAX_TX_ROWS", &CAP.to_string()),
        ("RIVET_CDC_SPILL_DIR", "1"),
    ]);
    assert!(
        a.status.success(),
        "a transaction past the cap must SPILL, not fail the run: {}",
        String::from_utf8_lossy(&a.stderr)
    );
    let log_a = String::from_utf8_lossy(&a.stderr).to_string();
    assert!(
        log_a.contains("passed the in-memory cap"),
        "the run must SAY it spilled — a silent spill and a cap that was never \
         reached are indistinguishable, and this test would then prove nothing \
         about spilling at all. stderr: {log_a}"
    );
    assert_spill_evidence(&log_a, CAP, OwnSpill::Transaction(ROWS));

    let plain_rig = Rig::pg_cdc(&tbl, &slot_plain).dest_path(out_plain.clone());
    plain_rig.run_ok();

    let census = spill_rig.row_census();
    assert_eq!(
        census.source,
        (ROWS + TAIL_TX) as i64,
        "the fixture itself must hold what the test thinks it does, or every \
         comparison below is against the wrong number: {census:?}"
    );
    assert!(
        census.agrees(),
        "source, delivered parquet, export_metrics and file_log must all agree — \
         a spilled tail that never reached the destination, or reached it without \
         being counted, shows up here and nowhere else: {census:?}"
    );

    let spilled = cdc_id_ops(&out_spill);
    let buffered = cdc_id_ops(&out_plain);
    assert_eq!(
        buffered.len(),
        ROWS + TAIL_TX,
        "the unspilled leg is the ORACLE — if it is short the comparison below \
         proves nothing"
    );
    assert_eq!(
        spilled, buffered,
        "a spilled transaction must deliver exactly what a buffered one does — \
         same rows, same order, same ops"
    );

    let mut ids: Vec<i64> = spilled.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(
        ids.len(),
        ROWS + TAIL_TX,
        "every id exactly once across the memory head, the disk tail, and the \
         transaction that follows them"
    );
    assert_eq!(
        spilled.last().map(|(id, _)| *id),
        Some((ROWS + TAIL_TX) as i64),
        "the transaction AFTER the spilled one must still arrive, and arrive last \
         — ending the read window to drain a tail defers it, and a run that treats \
         that early end as `drained` silently leaves it for the next cycle"
    );

    fn spill_leaks(dir: &std::path::Path, found: &mut Vec<String>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                spill_leaks(&p, found);
            } else if p
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("rivet-spill-"))
            {
                found.push(p.display().to_string());
            }
        }
    }
    let mut leaked = Vec::new();
    spill_leaks(d.path(), &mut leaked);
    spill_leaks(std::path::Path::new(".rivet/spill"), &mut leaked);
    assert!(
        leaked.is_empty(),
        "a spill must not outlive the transaction it held — a CDC run leaking one \
         per oversized transaction fills a disk on a scheduler: {leaked:?}"
    );
}

/// A MySQL transaction past the memory cap spills through the GENERAL frame and delivers all of it.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW + REPLICATION grant)"]
fn mysql_cdc_a_transaction_past_the_memory_cap_spills_rather_than_failing() {
    const ROWS: usize = 400;
    const CAP: usize = 50;
    /// The transaction that FOLLOWS the spilled one.
    const TAIL_TX: usize = 3;

    let tbl = unique_name("cdc_spill_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} {ONE_TRANSACTION_DDL}"))
        .unwrap();
    let _t = Table(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl).census_oracle();
    rig.pin_binlog_here();
    mysql_seed_one_transaction(&mut c, &tbl, 1..=ROWS);
    mysql_seed_one_transaction(&mut c, &tbl, ROWS + 1..=ROWS + TAIL_TX);

    let out = rig.run_with_envs(&[
        ("RIVET_CDC_MAX_TX_ROWS", &CAP.to_string()),
        ("RIVET_CDC_SPILL_DIR", "1"),
    ]);
    assert!(
        out.status.success(),
        "a transaction past the cap must SPILL, not fail the run: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let log = String::from_utf8_lossy(&out.stderr).to_string();

    assert_spill_evidence(&log, CAP, OwnSpill::Transaction(ROWS));

    let census = rig.row_census();
    assert_eq!(
        census.source,
        (ROWS + TAIL_TX) as i64,
        "the fixture itself must hold what the test thinks it does, or every \
         comparison below is against the wrong number: {census:?}"
    );
    assert!(
        census.agrees(),
        "source, delivered parquet, export_metrics and file_log must all agree — \
         a spilled tail that never reached the destination, or reached it without \
         being counted, shows up here and nowhere else: {census:?}"
    );

    let changes = cdc_id_ops(&rig.out_dir());
    let mut ids: Vec<i64> = changes.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(
        ids.len(),
        ROWS + TAIL_TX,
        "every id exactly once across the memory head and the disk tail"
    );
    assert_eq!(
        changes.last().map(|(id, _)| *id),
        Some((ROWS + TAIL_TX) as i64),
        "the transaction AFTER the spilled one must still arrive, and arrive last"
    );
}

/// An anchor whose identity query FAILS must refuse to write the checkpoint.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW + REPLICATION grant)"]
fn mysql_anchor_refuses_to_write_an_unverifiable_checkpoint() {
    let tbl = unique_name("cdc_anchor_id");
    let mut c = conn();
    use mysql::prelude::Queryable;
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _t = MysqlCdcTable(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl);
    let out = rig.run_with_envs(&[("RIVET_TEST_ERROR_AT", "mysql_identity_query")]);
    assert!(
        !out.status.success(),
        "an anchor that cannot record the server's identity must FAIL, not write \
         a checkpoint the resume can never verify"
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("UNVERIFIABLE anchor"),
        "the refusal must say WHY the anchor was not written: {err}"
    );
    assert!(
        !rig.checkpoint().exists(),
        "no checkpoint may exist — a half-anchor with an empty uuid is exactly \
         the artifact this refusal exists to prevent"
    );
}

/// A rolled-back MyISAM statement is framed as its OWN transaction.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW + REPLICATION grant)"]
fn roast_mysql_cdc_a_rolled_back_myisam_statement_is_framed_as_its_own_transaction() {
    use mysql::prelude::Queryable;
    const KEPT_BY_MYISAM: usize = 6;
    const COMMITTED: usize = 3;

    let tbl = unique_name("cdc_rollback_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} {ONE_TRANSACTION_DDL} ENGINE=MyISAM"
    ))
    .unwrap();
    let _t = MysqlCdcTable(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl).census_oracle();
    rig.pin_binlog_here();

    mysql_seed_rolled_back_transaction(&mut c, &tbl, 1..=KEPT_BY_MYISAM);
    mysql_seed_one_transaction(&mut c, &tbl, 100..=99 + COMMITTED);

    rig.run_ok();

    let changes = read_cdc_changes(&rig.out_dir());
    let mut ids: Vec<i64> = changes.iter().map(|c| c.id).collect();
    ids.sort_unstable();
    let mut expected: Vec<i64> = (1..=KEPT_BY_MYISAM as i64).collect();
    expected.extend(100..=99 + COMMITTED as i64);
    assert_eq!(
        ids, expected,
        "a MyISAM rollback KEEPS its rows — they are in the binlog and in the \
         source, so rivet must deliver them. Dropping them would be the loss."
    );

    let pos_of = |want: i64| {
        changes
            .iter()
            .find(|c| c.id == want)
            .map(|c| c.pos.clone())
            .unwrap_or_else(|| panic!("id {want} missing from the delivered changes"))
    };
    assert_ne!(
        pos_of(1),
        pos_of(100),
        "the rolled-back statement and the committed transaction are TWO \
         transactions and must carry two commit positions — one shared position \
         means the buffer survived its marker and was published under the next \
         transaction's commit"
    );
}

/// Round-5 completeness critic.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + uv"]
fn cdc_drain_row_hash_matches_the_independent_implementation() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_rowhash_val");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, a VARCHAR(64) NULL, b VARCHAR(64) NOT NULL)"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mysql_cdc(&tbl)
        .export_line("meta_columns: { row_hash: true }")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();
    run_rivet_ok(&cfg);

    c.exec_drop(
        format!("INSERT INTO {tbl} (id, a, b) VALUES (?,?,?),(?,?,?),(?,?,?),(?,?,?)"),
        (
            1,
            Option::<String>::None,
            "plain",
            2,
            Some(String::new()),
            "x",
            3,
            Some("a\u{1f}b".to_string()),
            "sep",
            4,
            Some("a, b".to_string()),
            "[c]",
        ),
    )
    .unwrap();
    run_rivet_ok(&cfg);

    let q = format!(
        "SELECT id, a, b, _rivet_row_hash AS h FROM read_parquet('{}/cdc-*.parquet') ORDER BY id",
        out.display()
    );
    let duck = uv_python()
        .args(["dev/pytools/duckcli.py", "-json", "-c", &q])
        .output()
        .expect("spawn `uv run` for duckcli — install uv (the oracle is pinned by uv.lock)");
    assert!(
        duck.status.success(),
        "duckdb read failed: {}",
        String::from_utf8_lossy(&duck.stderr)
    );
    let rows: serde_json::Value =
        serde_json::from_slice(&duck.stdout).expect("duckdb -json output");
    let rows = rows.as_array().expect("array");
    assert_eq!(
        rows.len(),
        4,
        "fixture: all four corpus rows must be captured"
    );

    let py_script = d.path().join("check.py");
    std::fs::write(
        &py_script,
        format!(
            "import sys, json\nsys.path.insert(0, '{repo}/dev/release_oracle')\nfrom rowhash import row_hash_of\nrows = json.load(sys.stdin)\nfor r in rows:\n    cells = [str(r['id']), r['a'], r['b']]\n    exp = row_hash_of(cells)\n    got = int(r['h'])\n    assert exp == got, f\"row {{r['id']}}: independent={{exp}} rivet={{got}}\"\nprint('OK', len(rows))\n",
            repo = env!("CARGO_MANIFEST_DIR")
        ),
    )
    .unwrap();
    let mut child = uv_python()
        .arg(&py_script)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn `uv run` for the independent hasher (xxhash, pinned by uv.lock)");
    use std::io::Write as _;
    child.stdin.take().unwrap().write_all(&duck.stdout).unwrap();
    let py_out = child.wait_with_output().unwrap();
    assert!(
        py_out.status.success(),
        "the independent implementation DISAGREES with the drain leg's \
         _rivet_row_hash:\n{}\n{}",
        String::from_utf8_lossy(&py_out.stdout),
        String::from_utf8_lossy(&py_out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&py_out.stdout).contains("OK 4"),
        "positive control: the checker must have graded all four rows"
    );
}

/// `uv run python` in the repo root: the interpreter with the uv-pinned duckdb and xxhash.
fn uv_python() -> std::process::Command {
    let mut c = std::process::Command::new("uv");
    c.args(["run", "--frozen", "--quiet", "python"])
        .current_dir(env!("CARGO_MANIFEST_DIR"));
    c
}

/// A MySQL TIME outside one day fails the CDC run with the batch export's code and sentence.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW + REPLICATION grant)"]
fn mysql_cdc_time_outside_one_day_is_refused_like_batch_and_never_checkpointed() {
    let tbl = unique_name("cdc_time_range");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, t TIME(6))"
    ))
    .unwrap();
    let _t = Table(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl);
    let ckpt = rig.checkpoint();
    rig.pin_binlog_here();
    let anchored = std::fs::read(&ckpt).unwrap();
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, '838:59:59'), (2, '-01:00:00')"
    ))
    .unwrap();

    let batch = Rig::mysql_batch(&tbl)
        .source_url(MYSQL_CDC_URL)
        .run_expect_fail();
    let refusal = batch
        .find("is outside 00:00..24:00")
        .and_then(|i| {
            batch[i..]
                .find(" Cast it in a query")
                .map(|j| &batch[i..i + j])
        })
        .unwrap_or_else(|| panic!("the batch export must refuse the fixture: {batch}"));
    assert!(
        batch.contains("RIVET_SOURCE_VALUE_UNREPRESENTABLE"),
        "{batch}"
    );

    let err = rig.run_expect_fail();
    assert!(
        err.contains("RIVET_SOURCE_VALUE_UNREPRESENTABLE")
            && err.contains("column 't'")
            && err.contains("mysql: TIME 838:59:59.000000 ")
            && err.contains(refusal)
            && err.contains("declare the column `string` in `columns:`")
            && !err.contains("Cast it in a query"),
        "CDC must refuse with the batch's code and sentence, naming the column and the stream's remedies, not the batch cast: {err}\nbatch: {batch}"
    );
    assert_eq!(
        std::fs::read(&ckpt).unwrap(),
        anchored,
        "a refused flush must not advance the checkpoint past the rows"
    );
}
