//! Live SQL Server CDC regression — at-least-once resume.

use std::time::Duration;

use crate::common::*;

/// One CDC rig per (table, capture instance, checkpoint, destination).
fn mssql_cdc_rig(table: &str, ci: &str, ckpt: &std::path::Path, out: &std::path::Path) -> Rig {
    Rig::mssql_cdc(table, ci)
        .checkpoint_path(ckpt.to_path_buf())
        .dest_path(out.to_path_buf())
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_resume_captures_only_new_changes() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10),(2,20)"));
    wait_for_capture(&ci, 2);
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out1).run_ok();
    assert_eq!(manifest_rows(&out1), 2, "run 1 captures the 2 changes");

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (3,30),(4,40)"));
    wait_for_capture(&ci, 4);
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out2).run_ok();
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume must capture only the 2 new changes (LSN resume), not re-read all 4"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(3, "insert".to_string()), (4, "insert".to_string())],
        "the resumed parquet must hold exactly the NEW changes (count 2 cannot tell new-2 from wrong-2)"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_intra_transaction_updates_get_distinct_seq() {
    let _serial = cross_process_serial("mssql_cdc");
    const N: i64 = 200;
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_ms_seq");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, counter BIGINT)"
    ));
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1, 0)"));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "BEGIN TRAN; DECLARE @i INT = 1; WHILE @i <= {N} BEGIN \
         UPDATE dbo.{table} SET counter = @i WHERE id = 1; SET @i = @i + 1; END; COMMIT;"
    ));
    wait_for_capture(&ci, 2 * N);

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();

    assert_intra_transaction_seq(&out, N);
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_sum_reconciles_across_intra_txn_updates() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_ms_sum");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    for txn in cdc_sum_workload(&table) {
        mssql_cdc_exec(&format!("BEGIN TRAN; {}; COMMIT;", txn.join("; ")));
    }
    const SENTINEL: i64 = 9_000_000;
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES ({SENTINEL}, 0)"));
    let mut drained = false;
    for _ in 0..120 {
        if mssql_cdc_query_i64(&format!(
            "SELECT COUNT(*) FROM cdc.{ci}_CT WHERE id = {SENTINEL}"
        )) >= 1
        {
            drained = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(drained, "capture job did not reach the sentinel in 60s");

    let source_sum = mssql_cdc_query_i64(&format!(
        "SELECT CAST(COALESCE(SUM(v), 0) AS INT) FROM dbo.{table}"
    ));

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();

    let changes = read_cdc_changes(&out);
    assert!(
        intra_txn_multi_change_count(&changes) > 0,
        "workload must exercise intra-transaction multi-updates or the sum passes vacuously"
    );
    let target_sum = deduped_current_sum(changes, CdcEngine::SqlServer);
    assert_eq!(
        source_sum, target_sum,
        "deduped-by-(__pos,__seq) SUM(v) must equal the source's SUM(v)"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_idle_first_run_then_change_is_captured_not_skipped() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_msidle");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out1).run_ok();
    assert_eq!(manifest_rows(&out1), 0, "idle run 1 captures nothing");

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci, 1);

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out2).run_ok();
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
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_mixed_transaction_and_qualified_table_conformance() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let orders = unique_name("rivet_cdc_mixq");
    let audit = unique_name("rivet_cdc_mixa");
    let (ci_o, ci_a) = (format!("dbo_{orders}"), format!("dbo_{audit}"));
    for (t, _ci) in [(&orders, &ci_o), (&audit, &ci_a)] {
        mssql_cdc_drop_table(&format!("dbo.{t}"));
        mssql_cdc_exec(&format!("CREATE TABLE dbo.{t}(id INT PRIMARY KEY, v INT)"));
    }
    enable_cdc(&orders, &ci_o);
    enable_cdc(&audit, &ci_a);
    let _g1 = MssqlCdcTable {
        table: orders.clone(),
        ci: ci_o.clone(),
    };
    let _g2 = MssqlCdcTable {
        table: audit.clone(),
        ci: ci_a.clone(),
    };

    mssql_cdc_exec(&format!(
        "BEGIN TRANSACTION; INSERT INTO dbo.{orders} VALUES (1,10); \
         INSERT INTO dbo.{audit} VALUES (1,99); COMMIT;"
    ));
    wait_for_capture(&ci_o, 1);

    let ckpt = d.path().join("cdc.ckpt");
    let out1 = d.path().join("out1");
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out1).unwrap();
    std::fs::create_dir_all(&out2).unwrap();
    let qualified = format!("dbo.{orders}");
    mssql_cdc_rig(&qualified, &ci_o, &ckpt, &out1).run_ok();
    assert_eq!(
        cdc_id_ops(&out1),
        vec![(1, "insert".to_string())],
        "exactly the ORDERS row"
    );

    mssql_cdc_rig(&qualified, &ci_o, &ckpt, &out2).run_ok();
    assert_eq!(
        manifest_rows(&out2),
        0,
        "no re-read of the mixed transaction"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn gremlin_mssql_capture_job_stall_loses_nothing() {
    let _serial = cross_process_serial("mssql_cdc");
    resume_capture_job();
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_stall");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci, 1);
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out1).run_ok();
    assert_eq!(manifest_rows(&out1), 1);

    let stalled = stall_capture_job();

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (2,20),(3,30)"));

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out2).run_ok();
    assert_eq!(manifest_rows(&out2), 0, "stalled job ⇒ nothing new visible");

    drop(stalled);
    wait_for_capture(&ci, 3);
    let out3 = d.path().join("out3");
    std::fs::create_dir_all(&out3).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out3).run_ok();
    assert_eq!(
        cdc_id_ops(&out3),
        vec![(2, "insert".to_string()), (3, "insert".to_string())],
        "exactly the two stall-window rows, by id — not any two rows"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_update_and_delete_carry_full_types() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_updel");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, amount DECIMAL(18,4), \
         dt2 DATETIME2, u UNIQUEIDENTIFIER, vb VARBINARY(8), m MONEY, note NVARCHAR(50))"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, 1.5, '2024-01-01', \
         '12345678-1234-1234-1234-123456789012', 0xAA, 1.00, N'v1')"
    ));
    wait_for_capture(&ci, 1);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();

    mssql_cdc_exec(&format!(
        "UPDATE dbo.{table} SET amount=99999999999999.9999, \
         dt2='2035-08-07T09:08:07.987654', u='FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF', \
         vb=0xDEADBEEF, m=123.4567, note=N'üñíçødé v2' WHERE id=1"
    ));
    wait_for_capture(&ci, 3);
    let upd_out = d.path().join("upd");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&upd_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &upd_out).run_ok();
    mssql_full_rig(&table, &batch_out).run_ok();
    let upd = read_one_batch(&upd_out);
    assert_eq!(upd.num_rows(), 1, "exactly the update after-image");
    let batch = read_one_batch(&batch_out);
    for field in batch.schema().fields() {
        let bi = batch.schema().index_of(field.name()).unwrap();
        let ui = upd.schema().index_of(field.name()).unwrap();
        assert_eq!(
            batch.column(bi).to_data(),
            upd.column(ui).to_data(),
            "update after-image column {}: differs from post-update batch",
            field.name()
        );
    }

    mssql_cdc_exec(&format!("DELETE FROM dbo.{table} WHERE id=1"));
    wait_for_capture(&ci, 4);
    let del_out = d.path().join("del");
    std::fs::create_dir_all(&del_out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &del_out).run_ok();
    let del = read_one_batch(&del_out);
    assert_eq!(del.num_rows(), 1);
    use arrow::array::Int32Array;
    let id = del
        .column(del.schema().index_of("id").unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("typed PK in the delete image");
    assert_eq!(id.value(0), 1);
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_initial_snapshot_covers_preexisting_rows_then_streams() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("rivet_cdc_init");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10),(2,20)"));
    wait_for_capture(&ci, 2);

    let rig = Rig::mssql_cdc(&table, &ci)
        .cdc("initial: snapshot")
        .cdc("until_current: true");
    let out = rig.out_dir();
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
    assert_eq!(
        manifest_rows(&out),
        0,
        "anchor at max LSN ⇒ nothing to drain"
    );

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (3,30)"));
    wait_for_capture(&ci, 3);
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out), 1, "the post-snapshot change streams");
    assert_eq!(
        cdc_id_ops(&out),
        vec![(3, "insert".to_string())],
        "streamed parquet must hold exactly the post-snapshot change (not just a count of 1)"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_money_values_survive_batch_and_cdc() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_money");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, m MONEY, sm SMALLMONEY)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, 123.4567, 12.34), (2, NULL, NULL)"
    ));
    wait_for_capture(&ci, 2);

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &cdc_out).run_ok();
    mssql_full_rig(&table, &batch_out).run_ok();

    use arrow::array::{Array, Decimal128Array};
    let batch = read_one_batch(&batch_out);
    let m_idx = batch.schema().index_of("m").unwrap();
    let m = batch
        .column(m_idx)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("money must be Decimal128");
    assert!(!m.is_null(0), "money value must survive the batch export");
    assert_eq!(m.value(0), 1_234_567, "123.4567 at scale 4");
    let sm_idx = batch.schema().index_of("sm").unwrap();
    let sm = batch
        .column(sm_idx)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("smallmoney must be Decimal128");
    assert_eq!(sm.value(0), 123_400, "12.34 at scale 4");
    assert!(m.is_null(1) && sm.is_null(1), "real NULLs stay NULL");

    let cdc = read_one_batch(&cdc_out);
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let cidx = cdc.schema().index_of(field.name()).unwrap();
        assert_eq!(
            batch.column(i).to_data(),
            cdc.column(cidx).to_data(),
            "column {}: CDC differs from batch",
            field.name()
        );
    }
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_capture_instance_name_must_not_decide_the_table() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_und");
    let ci = table.clone();
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10),(2,20)"));
    wait_for_capture(&ci, 2);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();
    assert_eq!(
        cdc_id_ops(&out),
        vec![(1, "insert".to_string()), (2, "insert".to_string())],
        "both rows ROUTED (the historic 6-of-8 loss was count-invisible)"
    );
}

/// The routing bug's actual SHAPE needs two tables — the test above cannot express it.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_two_underscored_tables_do_not_cross_route() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();

    let t1 = unique_name("rivet_cdc_ord");
    let t2 = unique_name("rivet_cdc_inv");
    let (ci1, ci2) = (t1.clone(), t2.clone());
    for (t, ci) in [(&t1, &ci1), (&t2, &ci2)] {
        mssql_cdc_drop_table(&format!("dbo.{t}"));
        mssql_cdc_exec(&format!("CREATE TABLE dbo.{t}(id INT PRIMARY KEY, v INT)"));
        enable_cdc(t, ci);
    }
    let _g1 = MssqlCdcTable {
        table: t1.clone(),
        ci: ci1.clone(),
    };
    let _g2 = MssqlCdcTable {
        table: t2.clone(),
        ci: ci2.clone(),
    };

    mssql_cdc_exec(&format!("INSERT INTO dbo.{t1} VALUES (10,1),(11,1)"));
    mssql_cdc_exec(&format!("INSERT INTO dbo.{t2} VALUES (20,2),(21,2)"));
    wait_for_capture(&ci1, 2);
    wait_for_capture(&ci2, 2);

    let rig = Rig::mssql_cdc(&t1, &ci1)
        .checkpoint_path(d.path().join("cdc1.ckpt"))
        .also_cdc_export(&t2, &t2, &[&format!("capture_instance: {ci2}")]);
    rig.run_ok();

    assert_eq!(
        cdc_id_ops(&rig.out_dir()),
        vec![(10, "insert".to_string()), (11, "insert".to_string())],
        "export '{t1}' must hold ONLY its own rows — a cross-route keeps the count \
         at 2 and swaps the ids, which is the shape the field failure had"
    );
    assert_eq!(
        cdc_id_ops(&rig.out_dir_for(&t2)),
        vec![(20, "insert".to_string()), (21, "insert".to_string())],
        "export '{t2}' must hold ONLY its own rows"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_crash_before_checkpoint_re_reads_on_resume() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_mscrash");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10),(2,20)"));
    wait_for_capture(&ci, 2);
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out1).run_ok();
    assert_eq!(manifest_rows(&out1), 2);

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (3,30),(4,40)"));
    wait_for_capture(&ci, 4);
    let crash_out = d.path().join("crash");
    std::fs::create_dir_all(&crash_out).unwrap();
    let crashed = mssql_cdc_rig(&table, &ci, &ckpt, &crash_out)
        .run_with_envs(&[("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail the run"
    );

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out2).run_ok();
    assert_eq!(
        manifest_rows(&out2),
        2,
        "crash before the checkpoint → resume re-reads exactly the 2 un-checkpointed changes"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(3, "insert".to_string()), (4, "insert".to_string())],
        "the re-read parquet must hold exactly the un-acked changes"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_datetimeoffset_value_is_preserved() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_dto");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, dto DATETIMEOFFSET)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, '2026-06-23 10:00:00 +05:30')"
    ));
    wait_for_capture(&ci, 1);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();

    let dto = parquet_one_timestamp(&out, "dto");
    assert!(
        dto.starts_with("2026-06-23 04:30:00"),
        "datetimeoffset must be captured as the 04:30 UTC instant — got {dto:?}"
    );
}

/// The first row's `col` (a Timestamp(µs)) as its UTC `NaiveDateTime` string.
fn parquet_one_timestamp(dir: &std::path::Path, col: &str) -> String {
    use arrow::array::{AsArray, types::TimestampMicrosecondType};
    let part = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|x| x == "parquet"))
        .expect("a .parquet part");
    let f = std::fs::File::open(part).unwrap();
    let mut r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap();
    let batch = r.next().expect("a row").unwrap();
    let idx = batch.schema().index_of(col).expect("column present");
    batch
        .column(idx)
        .as_primitive::<TimestampMicrosecondType>()
        .value_as_datetime(0)
        .expect("a non-null instant")
        .to_string()
}

/// Whether the first row's `col` is non-null in the one `.parquet` part.
fn parquet_col0_present(dir: &std::path::Path, col: &str) -> bool {
    use arrow::array::Array;
    let part = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|x| x == "parquet"))
        .expect("a .parquet part");
    let f = std::fs::File::open(part).unwrap();
    let mut r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap();
    let batch = r.next().expect("a row").unwrap();
    let idx = batch.schema().index_of(col).expect("column present");
    !batch.column(idx).is_null(0)
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_uniqueidentifier_value_is_preserved() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_uuid");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, u UNIQUEIDENTIFIER)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, '12345678-1234-1234-1234-123456789012')"
    ));
    wait_for_capture(&ci, 1);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();
    assert!(
        parquet_col0_present(&out, "u"),
        "uniqueidentifier must be captured (16 canonical bytes), not dropped to NULL"
    );
    let rendered = duckdb_dir_parquet_distinct_strings(&out, "u");
    assert_eq!(
        rendered,
        ["12345678-1234-1234-1234-123456789012".to_string()]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        "the CANONICAL uuid text must round-trip — byte order included"
    );
}

fn mssql_full_rig(table: &str, out: &std::path::Path) -> Rig {
    Rig::mssql_batch(&format!("{table}_batch"))
        .source_url(MSSQL_CDC_URL)
        .query(&format!("SELECT * FROM dbo.{table}"))
        .dest_path(out.to_path_buf())
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_resume_past_retention_errors_not_a_silent_gap() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_stale");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci, 1);

    let ckpt = d.path().join("cdc.ckpt");
    std::fs::write(&ckpt, r#"{"lsn":"00000000000000000001"}"#).unwrap();
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let res = mssql_cdc_rig(&table, &ci, &ckpt, &out).run();
    assert!(
        !res.status.success(),
        "a resume past retention must fail, not silently skip the gap"
    );
    let stderr = String::from_utf8_lossy(&res.stderr);
    assert!(
        stderr.contains("older than") && stderr.contains(REBASELINE_REMEDY),
        "the error must name the retention gap + the re-snapshot remedy, got:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC + the rivet-duckdb oracle"]
fn mssql_lost_checkpoint_is_refused_not_reanchored() {
    let _serial = cross_process_serial("mssql_cdc");
    let mut s = CdcScenario::mssql_with("cdc_ckno", "id INT PRIMARY KEY, v INT", |r, _| r);
    assert_a_lost_checkpoint_is_refused_not_reanchored(&mut s, false);
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC + the rivet-duckdb oracle"]
fn mssql_state_reset_lifts_a_lost_checkpoint_refusal() {
    let _serial = cross_process_serial("mssql_cdc");
    let mut s = CdcScenario::mssql_with("cdc_ckrst", "id INT PRIMARY KEY, v INT", |r, _| r);
    assert_state_reset_lifts_a_lost_checkpoint_refusal(&mut s);
}

/// A resume past change-table retention.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC + the rivet-duckdb oracle"]
fn mssql_retention_gap_refusal_remedy_recovers_the_rows_the_cleanup_removed() {
    let _serial = cross_process_serial("mssql_cdc");
    let mut s = CdcScenario::mssql("cdc_retgap", "id INT PRIMARY KEY, v INT");
    s.insert(1);
    s.settle();
    s.rig.run_ok();
    s.insert(2);
    s.insert(3);
    s.settle();
    mssql_cdc_exec(&format!(
        "DECLARE @lw binary(10) = sys.fn_cdc_get_max_lsn(); \
         EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'dbo_{}', \
         @low_water_mark = @lw, @threshold = 5000;",
        s.table
    ));
    let said = s.rig.run_expect_fail();
    assert!(
        said.contains("older than the SQL Server CDC change-table retention")
            && said.contains(REBASELINE_REMEDY),
        "the retention refusal names the re-baseline remedy:\n{said}"
    );
    follow_rebaseline_remedy(&mut s.rig, false);
    assert_eq!(
        dir_parquet_id_set(&s.rig.out_dir().join("snapshot")),
        [1, 2, 3].into(),
        "the remedy's baseline holds the rows the cleanup removed from the change table"
    );
}

/// A quiet table's checkpoint must move to the drained open-time bound.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_quiet_table_checkpoint_advances_so_cleanup_is_not_a_false_gap() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_quiet");
    let other = unique_name("rivet_cdc_busy");
    let (ci, other_ci) = (format!("dbo_{table}"), format!("dbo_{other}"));
    for t in [&table, &other] {
        mssql_cdc_drop_table(&format!("dbo.{t}"));
        mssql_cdc_exec(&format!("CREATE TABLE dbo.{t}(id INT PRIMARY KEY, v INT)"));
    }
    enable_cdc(&table, &ci);
    enable_cdc(&other, &other_ci);
    let _guards = [
        MssqlCdcTable {
            table: table.clone(),
            ci: ci.clone(),
        },
        MssqlCdcTable {
            table: other.clone(),
            ci: other_ci.clone(),
        },
    ];
    let ckpt = d.path().join("cdc.ckpt");
    let lsn = || -> String {
        let j: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&ckpt).unwrap()).unwrap();
        j["lsn"].as_str().unwrap().to_ascii_lowercase()
    };
    let run = |n: &str| {
        let out = d.path().join(n);
        std::fs::create_dir_all(&out).unwrap();
        mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();
        out
    };

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci, 1);
    assert_eq!(
        manifest_rows(&run("out1")),
        1,
        "run 1 captures the one change"
    );
    let captured_at = lsn();

    mssql_cdc_exec(&format!("INSERT INTO dbo.{other} VALUES (1,10)"));
    wait_for_capture(&other_ci, 1);
    assert_eq!(
        manifest_rows(&run("out2")),
        0,
        "run 2 is idle for this table"
    );
    let frontier = lsn();
    assert!(
        frontier > captured_at,
        "an idle drain must move the checkpoint past {captured_at}, got {frontier}"
    );

    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'{ci}', \
         @low_water_mark = 0x{frontier}, @threshold = 5000;"
    ));
    assert_eq!(
        manifest_rows(&run("out3")),
        0,
        "no change was lost, so the run must not be refused as a retention gap"
    );

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (2,20)"));
    wait_for_capture(&ci, 1);
    let out4 = run("out4");
    assert_eq!(duckdb_dir_scalar(&out4, "count(*)", None), 1);
    assert_eq!(
        duckdb_dir_scalar(&out4, "count(*)", Some("id = 2 AND __op = 'insert'")),
        1,
        "run 4 must deliver exactly the new insert"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_corrupt_checkpoint_fails_loud_not_silently_absent() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_corrupt");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci, 1);
    let ckpt = d.path().join("cdc.ckpt");
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out1).run_ok();
    assert!(ckpt.exists(), "run 1 pins a checkpoint");

    std::fs::write(&ckpt, b"{ not valid json").unwrap();
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (2,20)"));
    wait_for_capture(&ci, 2);

    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let res = mssql_cdc_rig(&table, &ci, &ckpt, &out2).run();
    assert!(
        !res.status.success(),
        "a corrupt checkpoint must fail the run, not be read as absent"
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
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_column_added_via_new_capture_instance_is_captured() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_msdrift");
    let ci1 = format!("dbo_{table}_v1");
    let ci2 = format!("dbo_{table}_v2");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id BIGINT PRIMARY KEY, v INT)"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci1}';"
    ));
    let _g1 = MssqlCdcTable {
        table: table.clone(),
        ci: ci1.clone(),
    };

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci1, 1);
    let ckpt1 = d.path().join("cdc1.ckpt");
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci1, &ckpt1, &out1).run_ok();
    assert!(
        !duckdb_dir_parquet_has_column(&out1, "w"),
        "ci1 predates the added column"
    );

    mssql_cdc_exec(&format!("ALTER TABLE dbo.{table} ADD w VARCHAR(20)"));
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci2}';"
    ));
    let _g2 = MssqlCdcTable {
        table: table.clone(),
        ci: ci2.clone(),
    };
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (2,20,'hello')"));
    wait_for_capture(&ci2, 1);

    let ckpt2 = d.path().join("cdc2.ckpt");
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    mssql_cdc_rig(&table, &ci2, &ckpt2, &out2).run_ok();
    assert!(
        duckdb_dir_parquet_has_column(&out2, "w"),
        "the new capture instance must expose the column added after ci1"
    );
    assert!(
        duckdb_dir_parquet_distinct_strings(&out2, "w").contains("hello"),
        "the added column's value must be captured, not nulled"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_until_current_terminates_under_sustained_writes() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_mshb");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id BIGINT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let vals: Vec<String> = (0..30).map(|i| format!("({i},{i})")).collect();
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES {}",
        vals.join(",")
    ));
    wait_for_capture(&ci, 30);

    let table_bg = table.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            mssql_cdc_try_exec(&format!("INSERT INTO dbo.{table_bg} VALUES ({i},{i})"));
            i += 1;
            std::thread::sleep(Duration::from_millis(20));
        }
    });

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let bounded_rig = mssql_cdc_rig(&table, &ci, &ckpt, &out);
    let elapsed = run_rivet_bounded(&bounded_rig.config_path(), Duration::from_secs(30));
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
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn roast_mssql_until_current_open_bound_two_runs_lose_nothing() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("rivet_cdc_msob");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id BIGINT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let vals: Vec<String> = (0..30).map(|i| format!("({i},{i})")).collect();
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES {}",
        vals.join(",")
    ));
    wait_for_capture(&ci, 30);

    let table_bg = table.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            mssql_cdc_try_exec(&format!("INSERT INTO dbo.{table_bg} VALUES ({i},{i})"));
            i += 1;
            std::thread::sleep(Duration::from_millis(5));
        }
    });

    let rig = Rig::mssql_cdc(&table, &ci).cdc("until_current: true");
    let cfg = rig.config_path();
    let elapsed = run_rivet_bounded(&cfg, Duration::from_secs(30));
    bg.stop();
    assert!(
        elapsed.is_some(),
        "run 1 must terminate at the open-time max-LSN bound under sustained writes"
    );

    let total = mssql_cdc_query_i64(&format!("SELECT COUNT(*) FROM dbo.{table}"));
    wait_for_capture(&ci, total);
    let elapsed2 = run_rivet_bounded(&cfg, Duration::from_secs(60));
    assert!(
        elapsed2.is_some(),
        "run 2 (no writers) must drain the tail and exit"
    );

    let got: std::collections::BTreeSet<i64> = duckdb_dir_parquet_i64(&rig.out_dir(), "id")
        .into_iter()
        .collect();
    let sum: i64 = got.iter().sum();
    assert_eq!(
        got.len() as i64,
        total,
        "distinct dest ids must match the source count"
    );
    assert_eq!(
        sum,
        mssql_cdc_query_i64(&format!("SELECT ISNULL(SUM(id),0) FROM dbo.{table}")),
        "dest id sum must match the source"
    );
    assert_eq!(
        got.first().copied(),
        Some(mssql_cdc_query_i64(&format!(
            "SELECT MIN(id) FROM dbo.{table}"
        ))),
        "dest min id must match the source"
    );
    assert_eq!(
        got.last().copied(),
        Some(mssql_cdc_query_i64(&format!(
            "SELECT MAX(id) FROM dbo.{table}"
        ))),
        "dest max id must match the source"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn roast_mssql_cdc_large_transaction_is_atomic_across_a_mid_flush_crash() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_msatomic");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id BIGINT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let vals: Vec<String> = (0..12).map(|i| format!("({i},{i})")).collect();
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES {}",
        vals.join(",")
    ));
    wait_for_capture(&ci, 12);

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mssql_cdc(&table, &ci)
        .cdc("rollover: 5")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let crashed = rig.run_with_envs(&[("RIVET_TEST_PANIC_AT", "cdc_after_checkpoint_before_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    let rig2 = Rig::mssql_cdc(&table, &ci)
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    run_rivet_ok(&rig2.config_path());

    let got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        got,
        want,
        "the 12-row transaction must survive the mid-flush crash whole — got {} ids \
         (a mid-transaction checkpoint at the shared start LSN skipped the tail on resume)",
        got.len()
    );
}

/// A PARTIAL capture instance must be refused even when a sibling instance of the same table is complete.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_a_partial_capture_instance_is_refused_even_beside_a_complete_sibling() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_twoci");
    let ci_full = format!("{table}_full");
    let ci_part = format!("{table}_part");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, keep INT, dropped INT)"
    ));

    enable_cdc(&table, &ci_full);
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci_part}', \
         @captured_column_list=N'id,keep';"
    ));
    let _g1 = MssqlCdcTable {
        table: table.clone(),
        ci: ci_full.clone(),
    };
    let _g2 = MssqlCdcTable {
        table: table.clone(),
        ci: ci_part.clone(),
    };

    let n_ci = mssql_cdc_query_i64(&format!(
        "SELECT COUNT(*) FROM cdc.change_tables ct JOIN sys.tables t \
         ON t.object_id = ct.source_object_id WHERE t.name = '{table}'"
    ));
    assert_eq!(
        n_ci, 2,
        "fixture is inert — the two-instance case is the whole point"
    );
    let n_part = mssql_cdc_query_i64(&format!(
        "SELECT COUNT(*) FROM cdc.captured_columns cc JOIN cdc.change_tables ct \
         ON ct.object_id = cc.object_id WHERE ct.capture_instance = '{ci_part}'"
    ));
    assert_eq!(
        n_part, 2,
        "the partial instance must capture 2 of the table's 3 columns"
    );

    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1,10,100),(2,20,200)"
    ));
    wait_for_capture(&ci_part, 2);

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let partial_rig = mssql_cdc_rig(&table, &ci_part, &ckpt, &out);
    let cfg = partial_rig.config_path();
    let res = run_rivet(&["run", "--config", cfg.to_str().unwrap()]);
    let stderr = String::from_utf8_lossy(&res.stderr).into_owned();

    assert!(
        !res.status.success(),
        "capturing through an instance that omits a column must be REFUSED, not reported \
         successful with that column NULL in every event; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("2 of 3 columns"),
        "the refusal must name the instance's REAL arity (2 of 3), not the sum across \
         instances; stderr:\n{stderr}"
    );
}

/// A `table:` that differs from the catalog only in CASE must not silently capture nothing while the checkpoint advances past the changes.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_case_only_table_mismatch_must_not_silently_drop_events() {
    let _serial = cross_process_serial("mssql_cdc");

    let table = format!("CaseIdent{}", std::process::id() % 100_000);
    let table = table.as_str();
    let ci = &format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} (id int PRIMARY KEY, v nvarchar(50))"
    ));
    enable_cdc(table, ci);
    let _guard = MssqlCdcTable {
        table: table.to_string(),
        ci: ci.clone(),
    };

    let d = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let anchor_rig = mssql_cdc_rig(&table.to_lowercase(), ci, &ckpt, out.path());
    let cfg = anchor_rig.config_path();

    let anchor = run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], &[]);
    if !anchor.status.success() {
        let why = String::from_utf8_lossy(&anchor.stderr);
        assert!(
            why.contains("no configured table matches"),
            "the anchor run failed for an unrelated reason:\n{why}"
        );
        return;
    }
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1,'a'),(2,'b'),(3,'c'); \
         UPDATE dbo.{table} SET v='B' WHERE id=2; \
         DELETE FROM dbo.{table} WHERE id=3;"
    ));
    wait_for_capture(ci, 6);

    let o = run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], &[]);
    if !o.status.success() {
        return;
    }

    let captured = read_cdc_changes(out.path()).len();
    if captured > 0 {
        return;
    }

    let still_in_ct = mssql_cdc_query_i64(&format!("SELECT COUNT(*) FROM cdc.{ci}_CT"));
    let fixed_rig = mssql_cdc_rig(&format!("dbo.{table}"), ci, &ckpt, out.path());
    let fixed = fixed_rig.config_path();
    let _ = run_rivet_env(&["run", "--config", fixed.to_str().unwrap()], &[]);
    let after_fix = read_cdc_changes(out.path()).len();

    panic!(
        "a case-only `table:` mismatch captured 0 of the {still_in_ct} change row(s) and exited 0. \
         Re-running with the case FIXED against the same checkpoint recovered {after_fix} — the \
         change rows never left the change table, so the checkpoint advanced past events that \
         were never captured. `MssqlChangeStream::open` reads identity from the catalog; the sink \
         routes with the raw config string through the byte-exact `table_matches` \
         (src/source/cdc/sink.rs:220), and nothing compares the two. SQL Server's default \
         collation is case-insensitive, so every other layer accepted the name."
    );
}

/// #200-3: the catalog-identity guard must also fire on the `rivet cdc` CLI path.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_cli_path_case_only_table_mismatch_must_not_silently_drop_events() {
    let _serial = cross_process_serial("mssql_cdc");

    let table = format!("CliIdent{}", std::process::id() % 100_000);
    let table = table.as_str();
    let ci = &format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} (id int PRIMARY KEY, v nvarchar(50))"
    ));
    enable_cdc(table, ci);
    let _guard = MssqlCdcTable {
        table: table.to_string(),
        ci: ci.clone(),
    };

    let d = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cli.ckpt");

    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1,'a'),(2,'b'),(3,'c');"
    ));
    wait_for_capture(ci, 3);
    let cli = |dir: &std::path::Path, tbl: &str| {
        run_rivet_env(
            &[
                "cdc",
                "--source",
                MSSQL_CDC_URL,
                "--capture-instance",
                ci,
                "--table",
                tbl,
                "--checkpoint",
                ckpt.to_str().unwrap(),
                "--output",
                dir.to_str().unwrap(),
            ],
            &[],
        )
    };

    let out = cli(dest.path(), &table.to_lowercase());
    assert!(
        !out.status.success(),
        "a case-only --table mismatch must refuse, not run: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let why = String::from_utf8_lossy(&out.stderr);
    assert!(
        why.contains("no configured table matches"),
        "the refusal must name the unmatched table, not fail for an unrelated reason:\n{why}"
    );
    assert_refused_before_any_write(dest.path(), &ckpt);
}

/// A `rivet cdc --max-events` run on SQL Server stops at a commit and owes the rest to the next run, graded by the rig oracle as a pair.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_cli_a_capped_run_leaves_its_remainder_to_the_next_run() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = format!("CliDefer{}", std::process::id() % 100_000);
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} (id int PRIMARY KEY, v int)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let d = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ck").to_str().unwrap().to_string();
    let dir = out.path().to_str().unwrap().to_string();
    let qualified = format!("dbo.{table}");
    let run = |cap: &[&str]| {
        let mut args = vec!["cdc", "--source", MSSQL_CDC_URL, "--capture-instance", &ci];
        args.extend([
            "--table",
            &qualified,
            "--checkpoint",
            &ckpt,
            "--output",
            &dir,
        ]);
        args.extend(cap);
        run_rivet_args_bounded(&args, Duration::from_secs(90))
            .expect("a bounded `rivet cdc` run terminates");
    };
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (0,0);"));
    wait_for_capture(&ci, 1);
    run(&[]);
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1,1),(2,2),(3,3);"
    ));
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (4,4),(5,5),(6,6);"
    ));
    wait_for_capture(&ci, 7);
    run(&["--max-events", "2"]);
    let ids: std::collections::BTreeSet<i64> =
        read_cdc_changes(out.path()).iter().map(|c| c.id).collect();
    assert_eq!(
        ids,
        (0..=3).collect(),
        "the cap stops at the first statement's commit"
    );
    run(&[]);
}

/// A checkpoint that PARSES but has no `lsn` silently re-reads the whole change table.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_checkpoint_without_an_lsn_must_refuse_not_silently_reread_everything() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_nolsn");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1,10),(2,20),(3,30)"
    ));
    wait_for_capture(&ci, 3);

    let r1 = mssql_cdc_rig(&table, &ci, &ckpt, &d.path().join("out1"));
    r1.run_ok();
    assert_eq!(
        duckdb_dir_parquet_id_set(&r1.out_dir())
            .into_iter()
            .collect::<Vec<i64>>(),
        vec![1, 2, 3],
        "leg 1 must really capture — a leg that captured nothing would make every \
         assertion below vacuous"
    );

    let before: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&ckpt).unwrap()).unwrap();
    assert!(
        before.get("lsn").and_then(|v| v.as_str()).is_some(),
        "the fixture is inert: leg 1 wrote no `lsn` to key off. Got {before}"
    );
    std::fs::write(
        &ckpt,
        serde_json::to_string(&serde_json::json!({
            "lsn_renamed_by_a_future_version": before.get("lsn").unwrap(),
            "pinned": false,
        }))
        .unwrap(),
    )
    .unwrap();

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (4,40)"));
    wait_for_capture(&ci, 4);

    let r2 = mssql_cdc_rig(&table, &ci, &ckpt, &d.path().join("out2"));
    let said = r2.run_expect_fail();
    assert!(
        said.contains("lsn") && said.to_lowercase().contains("checkpoint"),
        "the refusal must name the key and the file, or an operator cannot act on \
         it. Got:\n{said}"
    );

    assert!(
        !r2.out_dir().join("manifest.json").is_file(),
        "a refused run must deliver nothing"
    );
}

/// SQL Server needs no ambiguity resolution.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_capture_instance_resolves_identity_so_a_same_named_table_cannot_interfere() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("rivet_cdc_amb");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "IF SCHEMA_ID('other') IS NULL EXEC('CREATE SCHEMA other'); \
         CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    mssql_cdc_exec(&format!(
        "IF OBJECT_ID('other.{table}') IS NOT NULL DROP TABLE other.{table}; \
         CREATE TABLE other.{table}(zzz INT PRIMARY KEY, qqq INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    mssql_cdc_exec(&format!("INSERT INTO other.{table} VALUES (2,20)"));
    wait_for_capture(&ci, 1);

    let d = tempfile::tempdir().unwrap();
    let rig = mssql_cdc_rig(&table, &ci, &d.path().join("c.ckpt"), &d.path().join("out"));
    rig.run_ok();
    assert_eq!(
        duckdb_dir_parquet_i64(&rig.out_dir(), "id"),
        vec![1],
        "only the capture instance's own relation is streamed — a same-named table \
         in another schema is not in this change table at all, which is why this \
         engine has no ambiguity to resolve"
    );

    let wrong = Rig::mssql_cdc(&format!("other.{table}"), &ci)
        .checkpoint_path(d.path().join("w.ckpt"))
        .dest_path(d.path().join("w"));
    let said = wrong.run_expect_fail();
    assert!(
        said.contains(&format!("dbo.{table}")),
        "the refusal must name the relation the INSTANCE emits — that is the whole \
         remediation, and it is the catalog's spelling rather than the config's. \
         Got:\n{said}"
    );
    mssql_cdc_exec(&format!("DROP TABLE IF EXISTS other.{table}"));
}

/// The schema probe must read the relation the CAPTURE INSTANCE names.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_schema_probe_follows_the_capture_instance_not_the_default_schema() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_sch");
    let ci = format!("rvsales_{table}");
    mssql_cdc_exec("IF SCHEMA_ID('rvsales') IS NULL EXEC('CREATE SCHEMA rvsales');");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, amount INT, dbo_only INT); \
         INSERT INTO dbo.{table} VALUES (900, 42, 7);"
    ));
    mssql_cdc_exec(&format!(
        "CREATE TABLE rvsales.{table}(id INT PRIMARY KEY, note NVARCHAR(50))"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'rvsales', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
    let _guard = MssqlCdcTable::in_schema("rvsales", &table, &ci);
    let _decoy = DboDecoy(table.clone());

    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO rvsales.{table} VALUES (1, N'real-note-one'), (2, N'real-note-two')"
    ));
    wait_for_capture(&ci, 2);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();

    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();

    let batches = read_all_parts(&out);
    let cols: std::collections::BTreeSet<String> = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect()
        })
        .unwrap_or_default();
    assert!(
        cols.contains("note"),
        "the captured table's own column must be in the output. Its absence is the \
         defect in its purest form: the probe read dbo's columns, so the only data \
         column of the relation actually being captured never reached the \
         destination. Got: {cols:?}"
    );
    assert!(
        !cols.contains("dbo_only"),
        "and the DECOY's columns must not be — a column that exists in neither the \
         captured table nor its events can only arrive all-NULL, which reads as \
         'the source had no data' rather than as a mis-resolved config: {cols:?}"
    );

    let checked = run_rivet(&[
        "check",
        "--config",
        mssql_cdc_rig(&table, &ci, &ckpt, &out)
            .config_path()
            .to_str()
            .unwrap(),
        "--json",
    ]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&checked.stdout),
        String::from_utf8_lossy(&checked.stderr)
    );
    assert!(
        said.contains("note"),
        "preflight must probe the relation the capture instance names — it reported \
         the decoy's columns while the run wrote the captured table's, and the two \
         disagreeing is worse than both being wrong together. Got:\n{said}"
    );
    assert!(
        !said.contains("dbo_only"),
        "and it must not report the decoy's columns at all:\n{said}"
    );

    let notes: std::collections::BTreeSet<String> =
        duckdb_dir_parquet_distinct_strings(&out, "note");
    assert_eq!(
        notes,
        ["real-note-one", "real-note-two"]
            .iter()
            .map(|s| s.to_string())
            .collect::<std::collections::BTreeSet<_>>(),
        "the captured rows' real values must reach the destination"
    );
}

/// Drops the `dbo` decoy the schema-probe test plants.
struct DboDecoy(String);
impl Drop for DboDecoy {
    fn drop(&mut self) {
        let t = self.0.clone();
        let _ = std::panic::catch_unwind(move || {
            mssql_cdc_drop_table(&format!("dbo.{t}"));
        });
    }
}

/// The SNAPSHOT leg must read the same relation the drain captures.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_snapshot_leg_reads_the_captured_relation_not_the_default_schema() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_snp");
    let ci = format!("rvsales_{table}");
    mssql_cdc_exec("IF SCHEMA_ID('rvsales') IS NULL EXEC('CREATE SCHEMA rvsales');");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, amount INT, dbo_only INT); \
         INSERT INTO dbo.{table} VALUES (900, 42, 7);"
    ));
    mssql_cdc_exec(&format!(
        "CREATE TABLE rvsales.{table}(id INT PRIMARY KEY, note NVARCHAR(50)); \
         INSERT INTO rvsales.{table} VALUES (1, N'base-one'), (2, N'base-two');"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'rvsales', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
    let _guard = MssqlCdcTable::in_schema("rvsales", &table, &ci);
    let _decoy = DboDecoy(table.clone());

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out)
        .cdc_line("initial: snapshot")
        .run_ok();

    let snap = out.join("snapshot");
    assert!(
        snap.is_dir(),
        "the snapshot leg must have run at all — without it this test grades nothing"
    );
    let notes: std::collections::BTreeSet<String> =
        duckdb_dir_parquet_distinct_strings(&snap, "note");
    assert_eq!(
        notes,
        ["base-one", "base-two"]
            .iter()
            .map(|s| s.to_string())
            .collect::<std::collections::BTreeSet<_>>(),
        "the baseline must be the CAPTURED table's rows. Before the fix this \
         directory held the decoy's row (id 900) — data that never existed in the \
         relation being captured, written into its prefix under a green run"
    );

    let doc: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(snap.join("manifest.json")).expect("read the leg's manifest"),
    )
    .expect("parse the leg's manifest");
    assert_eq!(
        doc["source"]["table"].as_str(),
        Some(table.as_str()),
        "the leg must record the CONFIGURED name, which is what the drain records \
         (`schema: None, table: <configured>`). Got: {}",
        doc["source"]
    );
    assert_eq!(
        doc["source"]["schema"],
        serde_json::Value::Null,
        "and no schema, for the same reason — the drain writes none, and an identity \
         the two legs spell differently is a refused load: {}",
        doc["source"]
    );
}

/// A capture instance the catalog does not know must be refused BEFORE the snapshot leg makes a fabricated baseline permanent.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_refuses_an_unknown_capture_instance_before_the_snapshot_is_durable() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_ghost");
    let ci = format!("rvsales_{table}");
    mssql_cdc_exec("IF SCHEMA_ID('rvsales') IS NULL EXEC('CREATE SCHEMA rvsales');");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, amount INT); \
         INSERT INTO dbo.{table} VALUES (900, 42);"
    ));
    mssql_cdc_exec(&format!(
        "CREATE TABLE rvsales.{table}(id INT PRIMARY KEY, note NVARCHAR(50)); \
         INSERT INTO rvsales.{table} VALUES (1, N'real-one'), (2, N'real-two');"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'rvsales', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
    let _guard = MssqlCdcTable::in_schema("rvsales", &table, &ci);
    let _decoy = DboDecoy(table.clone());

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();

    let typo = format!("{ci}_typo");
    let rig = mssql_cdc_rig(&table, &typo, &ckpt, &out).cdc_line("initial: snapshot");
    let said = rig.run_expect_fail();
    assert!(
        said.contains(&typo),
        "the refusal must name the instance it could not find — an operator cannot \
         fix a name they were not shown. Got:\n{said}"
    );
    assert!(
        !out.join("snapshot").exists()
            || files_with_extension(&out.join("snapshot"), "parquet").is_empty(),
        "and it must refuse BEFORE the snapshot writes. A baseline on disk here is \
         the whole defect: it is also marked done, so correcting the typo never \
         backfills the rows it skipped"
    );

    let ok_rig = mssql_cdc_rig(&table, &ci, &ckpt, &out).cdc_line("initial: snapshot");
    ok_rig.run_ok();
    let notes: std::collections::BTreeSet<String> =
        duckdb_dir_parquet_distinct_strings(&out.join("snapshot"), "note");
    assert_eq!(
        notes,
        ["real-one", "real-two"]
            .iter()
            .map(|s| s.to_string())
            .collect::<std::collections::BTreeSet<_>>(),
        "the corrected run must produce the CAPTURED table's baseline — this is the \
         assertion that failed before the fix, because the typo'd run had already \
         marked the snapshot done"
    );
}

/// The snapshot-done row must be keyed by the LABEL both times.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_a_completed_snapshot_is_not_redone_after_the_destination_is_wiped() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_once");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT); \
         INSERT INTO dbo.{table} VALUES (1,10),(2,20);"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = mssql_cdc_rig(&table, &ci, &ckpt, &out).cdc_line("initial: snapshot");
    rig.run_ok();
    let snap = out.join("snapshot");
    assert!(
        !files_with_extension(&snap, "parquet").is_empty(),
        "run 1 must actually produce a baseline — without one this test grades nothing"
    );

    std::fs::remove_dir_all(&snap).expect("wipe the snapshot prefix");
    rig.run_ok();
    assert_eq!(
        read_all_parts(&out)
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        0,
        "the drain captured no changes here, so the export prefix holds none — the \
         baseline lives under snapshot/ and is asserted below"
    );
    assert!(
        files_with_extension(&snap, "parquet").is_empty(),
        "run 2 must NOT re-snapshot: the state DB already records this table as \
         backfilled, and that record is the ONLY thing standing between a wiped \
         destination and a full re-read plus a duplicated baseline on every \
         scheduled cycle"
    );
}

/// `rivet check --type-report` must still type the export when the cdc catalog is unreadable.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_check_still_types_the_export_when_the_cdc_catalog_is_unreadable() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_chk_nocdc").to_lowercase();
    let db = format!("nocdc_{table}");
    mssql_cdc_exec(&format!(
        "IF DB_ID(N'{db}') IS NOT NULL DROP DATABASE [{db}]; CREATE DATABASE [{db}];"
    ));
    let _db_guard = NoCdcDb(db.clone());
    mssql_cdc_exec(&format!(
        "USE [{db}]; CREATE TABLE dbo.{table}(id INT PRIMARY KEY, amount INT); \
         INSERT INTO dbo.{table} VALUES (1, 10);"
    ));

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = mssql_cdc_rig(&table, &format!("dbo_{table}"), &ckpt, &out)
        .source_url(&MSSQL_CDC_URL.replace("/rivet", &format!("/{db}")));
    let checked = run_rivet(&[
        "check",
        "--config",
        rig.config_path().to_str().unwrap(),
        "--type-report",
    ]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&checked.stdout),
        String::from_utf8_lossy(&checked.stderr)
    );
    assert!(
        said.contains("amount"),
        "the column report must still be produced — an unreadable cdc catalog is \
         the NORMAL state before `sp_cdc_enable_table` runs, and a check that \
         silently types nothing while printing a verdict is worse than one that \
         refuses. Got:\n{said}"
    );
}

/// Drops the CDC-less database the check test stands up.
struct NoCdcDb(String);
impl Drop for NoCdcDb {
    fn drop(&mut self) {
        let db = self.0.clone();
        let _ = std::panic::catch_unwind(move || {
            mssql_cdc_exec(&format!(
                "IF DB_ID(N'{db}') IS NOT NULL BEGIN ALTER DATABASE [{db}] SET SINGLE_USER \
                 WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db}]; END"
            ));
        });
    }
}

/// A SQL Server poll batch past the memory cap spills.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_a_batch_past_the_memory_cap_spills_rather_than_failing() {
    const ROWS: usize = 400;
    const CAP: usize = 50;
    const TAIL_TX: usize = 3;

    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("cdc_spill_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!("CREATE TABLE dbo.{table} {ONE_TRANSACTION_DDL}"));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let rig = Rig::mssql_cdc(&table, &ci).census_oracle();

    mssql_seed_one_transaction(&table, 1..=ROWS);
    mssql_seed_one_transaction(&table, ROWS + 1..=ROWS + TAIL_TX);
    wait_for_capture(&ci, (ROWS + TAIL_TX) as i64);

    let out = rig.run_with_envs(&[
        ("RIVET_CDC_MAX_TX_ROWS", &CAP.to_string()),
        ("RIVET_CDC_SPILL_DIR", "1"),
    ]);
    assert!(
        out.status.success(),
        "a batch past the cap must SPILL, not fail the run: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let log = String::from_utf8_lossy(&out.stderr).to_string();

    assert_spill_evidence(&log, CAP, OwnSpill::Batch);

    let census = rig.row_census();
    assert_eq!(
        census.source,
        (ROWS + TAIL_TX) as i64,
        "the fixture itself must hold what the test thinks it does, or every \\
         comparison below is against the wrong number: {census:?}"
    );
    assert!(
        census.agrees(),
        "source, delivered parquet, export_metrics and file_log must all agree — a \\
         spilled tail that never reached the destination, or reached it without \\
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
        "the transaction after the spilled one must still arrive, and arrive last"
    );
}

/// A crash while a SPILLED tail is being handed out must lose nothing on resume.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn roast_mssql_cdc_a_crash_mid_spilled_tail_loses_no_group_on_resume() {
    const TXS: usize = 3;
    /// SEVEN on purpose — not five, and the difference is the whole test.
    const PER_TX: usize = 7;

    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("cdc_spillcrash_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!("CREATE TABLE dbo.{table} {ONE_TRANSACTION_DDL}"));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    for t in 0..TXS {
        mssql_seed_one_transaction(&table, t * PER_TX + 1..=(t + 1) * PER_TX);
    }
    wait_for_capture(&ci, (TXS * PER_TX) as i64);

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mssql_cdc(&table, &ci)
        .cdc("rollover: 5")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());

    let crashed = rig.run_with_envs(&[
        ("RIVET_CDC_MAX_TX_ROWS", "4"),
        ("RIVET_CDC_SPILL_DIR", "1"),
        ("RIVET_TEST_PANIC_AT", "cdc_after_checkpoint_before_ack"),
    ]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );
    let log1 = String::from_utf8_lossy(&crashed.stderr).to_string();
    assert!(
        log1.contains("passed the in-memory cap"),
        "the fixture is inert unless run 1 actually SPILLED before crashing — \
         without this line the crash fired on the unspilled path and the test \
         proves nothing about it. stderr: {log1}"
    );

    let out2 = rig.run_with_envs(&[("RIVET_CDC_MAX_TX_ROWS", "4"), ("RIVET_CDC_SPILL_DIR", "1")]);
    assert!(
        out2.status.success(),
        "the resume run must succeed: {}",
        String::from_utf8_lossy(&out2.stderr)
    );

    let got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = (1..=(TXS * PER_TX) as i64).collect();
    assert_eq!(
        got, want,
        "every row of every spilled group must survive the mid-tail crash — a \
         missing group means the checkpoint advanced past a commit whose rows died \
         with the crashed process's spill file"
    );

    let leaked: Vec<String> = std::fs::read_dir(ckpt.parent().unwrap().join(".rivet-spill"))
        .map(|rd| {
            rd.flatten()
                .map(|e| e.path().display().to_string())
                .filter(|p| p.contains("rivet-spill-"))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        leaked.is_empty(),
        "the crashed run's spill must be collected by the resume's sweep: {leaked:?}"
    );
}

/// The OTHER half of the snapshot-done OR.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_a_marker_only_snapshot_survives_a_fresh_state_db() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_marker");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT); \
         INSERT INTO dbo.{table} VALUES (1,10),(2,20);"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = mssql_cdc_rig(&table, &ci, &ckpt, &out).cdc_line("initial: snapshot");
    rig.run_ok();
    let snap = out.join("snapshot");
    let baseline: std::collections::BTreeSet<String> = files_with_extension(&snap, "parquet")
        .into_iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert!(
        !baseline.is_empty(),
        "run 1 must produce a baseline — without one this test grades nothing"
    );
    assert!(
        snap.join("_SUCCESS").is_file(),
        "fixture: the snapshot leg must leave its completion marker"
    );

    let state_db = rig.config_path().parent().unwrap().join(".rivet_state.db");
    assert!(
        state_db.is_file(),
        "fixture: the rig's state DB must exist where this test deletes it"
    );
    std::fs::remove_file(&state_db).unwrap();

    rig.run_ok();
    let after: std::collections::BTreeSet<String> = files_with_extension(&snap, "parquet")
        .into_iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        after, baseline,
        "a fresh state DB with the marker intact must NOT re-snapshot — new part \
         names here mean a duplicated baseline appended under status: success"
    );
    assert_eq!(
        duckdb_dir_parquet_id_set(&snap),
        [1, 2]
            .into_iter()
            .collect::<std::collections::BTreeSet<i64>>(),
        "run 1's baseline ids must be the ones still standing"
    );
    assert_eq!(
        duckdb_dir_scalar(&snap, "sum(v)", None),
        30,
        "and their VALUES, not merely their count"
    );
}

#[test]
#[ignore = "live+gate-only: requires docker compose --profile replica up -d mssql-ag-primary mssql-ag-secondary, then dev/mssql-ag/setup.sh"]
fn mssql_cdc_reads_changes_from_a_readable_secondary() {
    const PRIMARY: u16 = 1440;
    const SECONDARY: u16 = 1441;
    for port in [PRIMARY, SECONDARY] {
        assert!(
            std::net::TcpStream::connect_timeout(
                &format!("127.0.0.1:{port}").parse().unwrap(),
                std::time::Duration::from_millis(500),
            )
            .is_ok(),
            "fixture: the availability group is not up on :{port} (dev/mssql-ag/setup.sh)"
        );
    }
    let wait = |sql: &str, want: i64| {
        for _ in 0..120 {
            if mssql_query_i64_on(SECONDARY, sql) == want {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        panic!("fixture: the secondary never reached {want} for `{sql}`");
    };
    let table = unique_name("cdc_ag");
    let ci = format!("dbo_{table}");
    mssql_exec_on(
        PRIMARY,
        &format!(
            "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT);
             EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}',
               @role_name=NULL, @capture_instance=N'{ci}';"
        ),
    );
    wait(
        &format!("SELECT COUNT(*) FROM cdc.change_tables WHERE capture_instance = N'{ci}'"),
        1,
    );

    let rig = Rig::mssql_cdc(&table, &ci).source_url(&format!(
        "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:{SECONDARY}/rivet"
    ));
    rig.run_ok();

    mssql_exec_on(
        PRIMARY,
        &format!("INSERT INTO dbo.{table} VALUES (1, 10), (2, 20)"),
    );
    wait(&format!("SELECT COUNT(*) FROM cdc.{ci}_CT"), 2);
    rig.run_ok();
    let delivered = duckdb_declared_dir_id_set(&rig.out_dir());
    mssql_exec_on(
        PRIMARY,
        &format!(
            "EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N'{table}',
               @capture_instance=N'{ci}';
             DROP TABLE dbo.{table};"
        ),
    );
    assert_eq!(
        delivered,
        [1, 2].into_iter().collect(),
        "both changes captured on the primary must be read from the readable secondary"
    );
}

/// Wait until `sql` on the server at `port` returns at least `want`.
fn wait_on(port: u16, sql: &str, want: i64) {
    for _ in 0..120 {
        if mssql_query_i64_on(port, sql) >= want {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    panic!("fixture: `{sql}` on :{port} never reached {want}");
}

/// Assert a SQL Server is listening on `port`, naming the stand service when it is not.
fn require_port(port: u16, what: &str) {
    assert!(
        std::net::TcpStream::connect_timeout(
            &format!("127.0.0.1:{port}").parse().unwrap(),
            std::time::Duration::from_millis(500),
        )
        .is_ok(),
        "fixture: {what} is not up on :{port}"
    );
}

#[test]
#[ignore = "live+gate-only: requires docker compose mssql-cdc :1434 and the replica-profile AG primary :1440"]
fn mssql_checkpoint_from_another_database_is_refused() {
    const OTHER: u16 = 1440;
    require_port(
        OTHER,
        "the availability group primary (dev/mssql-ag/setup.sh)",
    );
    let table = unique_name("cdc_foreign");
    let ci = format!("dbo_{table}");
    let _serial = cross_process_serial("mssql_cdc");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let t = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let a = Rig::mssql_cdc(&table, &ci);
    a.run_ok();
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1, 10), (2, 20)"));
    wait_for_capture(&ci, 2);
    a.run_ok();

    mssql_exec_on(
        OTHER,
        &format!(
            "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT);
             EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}',
               @role_name=NULL, @capture_instance=N'{ci}';"
        ),
    );
    mssql_exec_on(OTHER, &format!("INSERT INTO dbo.{table} VALUES (3, 30)"));
    wait_on(OTHER, &format!("SELECT COUNT(*) FROM cdc.{ci}_CT"), 1);
    let before = std::fs::read(a.checkpoint()).unwrap();
    let b = Rig::mssql_cdc(&table, &ci)
        .source_url(&format!(
            "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:{OTHER}/rivet"
        ))
        .checkpoint_path(a.checkpoint());
    let said = b.run_expect_fail();
    mssql_exec_on(
        OTHER,
        &format!(
            "EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N'{table}',
               @capture_instance=N'{ci}';
             DROP TABLE dbo.{table};"
        ),
    );
    drop(t);
    assert!(
        said.contains("mssql cdc: this checkpoint was written against a different database"),
        "a checkpoint from another server's database must be refused:\n{said}"
    );
    assert_eq!(
        std::fs::read(a.checkpoint()).unwrap(),
        before,
        "the refused run must leave the checkpoint untouched"
    );
}

/// A throwaway CDC-enabled database on `mssql-cdc`, dropped with its capture jobs.
struct ForkDb(String);

impl Drop for ForkDb {
    fn drop(&mut self) {
        mssql_cdc_try_exec(&format!(
            "USE [{0}]; EXEC sys.sp_cdc_disable_db; USE master;
             ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{0}];",
            self.0
        ));
    }
}

#[test]
#[ignore = "live: requires docker compose mssql-cdc :1434 (SQL Server Agent)"]
fn mssql_checkpoint_on_a_database_restored_from_backup_is_refused() {
    let _serial = cross_process_serial("mssql_cdc");
    let db = unique_name("rivet_fork");
    let table = "t";
    let ci = "dbo_t";
    mssql_cdc_exec(&format!("CREATE DATABASE [{db}]"));
    let _db = ForkDb(db.clone());
    mssql_cdc_exec(&format!(
        "USE [{db}]; ALTER DATABASE [{db}] SET RECOVERY FULL; EXEC sys.sp_cdc_enable_db;
         CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT);
         EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}',
           @role_name=NULL, @capture_instance=N'{ci}';"
    ));
    let ct = |want| {
        wait_on(
            1434,
            &format!("SELECT COUNT(*) FROM [{db}].cdc.{ci}_CT"),
            want,
        )
    };
    let rig = Rig::mssql_cdc(table, ci).source_url(&format!(
        "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1434/{db}"
    ));
    rig.run_ok();
    let bak = format!("/var/opt/mssql/data/{db}.bak");
    mssql_cdc_exec(&format!(
        "BACKUP DATABASE [{db}] TO DISK = N'{bak}' WITH INIT"
    ));
    mssql_cdc_exec(&format!(
        "INSERT INTO [{db}].dbo.{table} VALUES (1, 10), (2, 20)"
    ));
    ct(2);
    rig.run_ok();

    mssql_cdc_exec(&format!(
        "USE master; ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
         RESTORE DATABASE [{db}] FROM DISK = N'{bak}' WITH REPLACE, KEEP_CDC, RECOVERY;
         ALTER DATABASE [{db}] SET MULTI_USER;"
    ));
    mssql_cdc_exec(&format!("INSERT INTO [{db}].dbo.{table} VALUES (3, 30)"));
    ct(1);
    let said = rig.run_expect_fail();
    assert!(
        said.contains("mssql cdc: this database was restored from a backup"),
        "a checkpoint that predates a restore must be refused, not resumed past the rewound log:\n{said}"
    );
}

#[test]
#[ignore = "live+gate-only: requires the replica-profile availability group :1440/:1441 (dev/mssql-ag/setup.sh)"]
fn mssql_checkpoint_follows_a_failover_to_the_secondary() {
    const PRIMARY: u16 = 1440;
    const SECONDARY: u16 = 1441;
    require_port(PRIMARY, "the availability group primary");
    require_port(SECONDARY, "the availability group secondary");
    let table = unique_name("cdc_failover");
    let ci = format!("dbo_{table}");
    mssql_exec_on(
        PRIMARY,
        &format!(
            "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT);
             EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}',
               @role_name=NULL, @capture_instance=N'{ci}';"
        ),
    );
    let url = |port: u16| format!("sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:{port}/rivet");
    let on_primary = Rig::mssql_cdc(&table, &ci).source_url(&url(PRIMARY));
    on_primary.run_ok();
    mssql_exec_on(
        PRIMARY,
        &format!("INSERT INTO dbo.{table} VALUES (1, 10), (2, 20)"),
    );
    wait_on(PRIMARY, &format!("SELECT COUNT(*) FROM cdc.{ci}_CT"), 2);
    on_primary.run_ok();

    mssql_exec_on(PRIMARY, &format!("INSERT INTO dbo.{table} VALUES (3, 30)"));
    wait_on(SECONDARY, &format!("SELECT COUNT(*) FROM cdc.{ci}_CT"), 3);
    let on_secondary = Rig::mssql_cdc(&table, &ci)
        .source_url(&url(SECONDARY))
        .checkpoint_path(on_primary.checkpoint());
    let outcome = on_secondary.run_with_envs(&[]);
    let delivered = outcome
        .status
        .success()
        .then(|| duckdb_declared_dir_id_set(&on_secondary.out_dir()));
    mssql_exec_on(
        PRIMARY,
        &format!(
            "EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N'{table}',
               @capture_instance=N'{ci}';
             DROP TABLE dbo.{table};"
        ),
    );
    assert!(
        outcome.status.success(),
        "a replica shares the primary's log, so its checkpoint must resume there:\n{}",
        String::from_utf8_lossy(&outcome.stderr)
    );
    assert_eq!(
        delivered,
        Some([3].into_iter().collect()),
        "the secondary resumes after the primary's checkpoint: only the new change"
    );
}

/// A captured `xml` value has no CDC decoder: the run refuses instead of writing it as NULL.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_refuses_a_captured_value_it_cannot_decode() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_xml");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, doc XML)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1, '<a/>')"));
    wait_for_capture(&ci, 1);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let said = mssql_cdc_rig(&table, &ci, &d.path().join("cdc.ckpt"), &out).run_expect_fail();
    assert!(
        said.contains("no decoder for a captured Xml"),
        "a valued xml cell must refuse the run, not land as NULL: {said}"
    );
}

/// A snapshot-baselined SQL Server scenario with ids 1..=3 delivered and two updates waiting unread.
fn baselined_with_two_unread_updates(label: &str, shape: fn(Rig) -> Rig) -> CdcScenario {
    let mut s = CdcScenario::mssql_with(label, "id INT PRIMARY KEY, v INT", |r, _| {
        shape(r.cdc("initial: snapshot").cdc("until_current: true"))
    });
    for id in 1..=3 {
        s.insert(id);
    }
    s.settle();
    s.rig.run_ok();
    s.update(1);
    s.update(2);
    s.settle();
    s
}

/// Run once and require the coded log-gap refusal.
fn assert_log_gap_refusal(s: &CdcScenario) {
    let out = s.rig.run();
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        out.status.code(),
        Some(5),
        "the refusal's exit class:\n{said}"
    );
    assert!(
        said.contains("RIVET_SOURCE_CDC_LOG_GAP")
            && said.contains("older than the SQL Server CDC change-table retention")
            && said.contains(REBASELINE_REMEDY),
        "the refusal carries the code, the cause and the re-baseline remedy:\n{said}"
    );
}

/// Disable the capture instance and enable it again under the same name.
fn recreate_capture_instance(table: &str) {
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @capture_instance=N'dbo_{table}'; \
         EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'dbo_{table}';"
    ));
}

/// Retention cleanup past changes no run has read yet, before the first changes run.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC + the rivet-duckdb oracle"]
fn mssql_cleanup_past_unread_changes_before_the_first_changes_run_is_refused() {
    let _serial = cross_process_serial("mssql_cdc");
    let mut s = baselined_with_two_unread_updates("cdc_pingap", |r| r);
    mssql_cdc_exec(&format!(
        "DECLARE @lw binary(10) = (SELECT MAX(__$start_lsn) FROM cdc.dbo_{t}_CT); \
         EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'dbo_{t}', \
         @low_water_mark = @lw, @threshold = 5000;",
        t = s.table
    ));
    assert_log_gap_refusal(&s);
    assert_log_gap_refusal(&s);
    follow_rebaseline_remedy(&mut s.rig, true);
    assert_eq!(
        dir_parquet_id_set(&s.rig.out_dir().join("snapshot")),
        [1, 2, 3].into(),
        "the remedy's run is graded against the source by the rig oracle; its baseline holds every key"
    );
}

/// The capture instance disabled and re-enabled before the first changes run.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC + the rivet-duckdb oracle"]
fn mssql_capture_instance_recreated_before_the_first_changes_run_is_refused() {
    let _serial = cross_process_serial("mssql_cdc");
    let mut s = baselined_with_two_unread_updates("cdc_pinrecreate", |r| r);
    recreate_capture_instance(&s.table);
    assert_log_gap_refusal(&s);
    assert_log_gap_refusal(&s);
    follow_rebaseline_remedy(&mut s.rig, true);
    assert_eq!(
        dir_parquet_id_set(&s.rig.out_dir().join("snapshot")),
        [1, 2, 3].into(),
        "the remedy's run is graded against the source by the rig oracle; its baseline holds every key"
    );
}

/// The ADD COLUMN refusal's remedy.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC + the rivet-duckdb oracle"]
fn mssql_add_column_remedy_before_the_first_changes_run_ends_in_the_log_gap_refusal() {
    let _serial = cross_process_serial("mssql_cdc");
    let mut s = baselined_with_two_unread_updates("cdc_pinaddcol", |r| {
        r.a_failed_run_may_leave(
            &[Leftover::ResumePoint],
            "the refused run after the re-enable has already stored the widened schema (id, v, w) in export_schema",
        )
    });
    mssql_cdc_exec(&format!("ALTER TABLE dbo.{} ADD w INT NULL", s.table));
    let said = s.rig.run_expect_fail();
    assert!(
        said.contains("Re-enable the table with the full column"),
        "fixture: the ADD COLUMN refusal names the re-enable remedy:\n{said}"
    );
    recreate_capture_instance(&s.table);
    assert_log_gap_refusal(&s);
    follow_rebaseline_remedy(&mut s.rig, true);
    assert_eq!(
        dir_parquet_id_set(&s.rig.out_dir().join("snapshot")),
        [1, 2, 3].into(),
        "the remedy's run is graded against the source by the rig oracle; its baseline holds every key"
    );
}

/// A scenario enabled and anchored while the capture job is stalled.
fn anchored_before_the_capture_job_reached_the_instance() -> CdcScenario {
    let _stalled = stall_capture_job();
    let s = CdcScenario::mssql_with("cdc_pinfresh", "id INT PRIMARY KEY, v INT", |r, _| {
        r.cdc("initial: snapshot").cdc("until_current: true")
    });
    s.rig.run_ok();
    let ckpt: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(s.rig.checkpoint()).unwrap()).unwrap();
    assert_eq!(
        mssql_cdc_query_i64(&format!(
            "SELECT COUNT(*) FROM cdc.change_tables WHERE capture_instance = N'dbo_{}' \
             AND sys.fn_cdc_get_max_lsn() < start_lsn AND 0x{} < start_lsn",
            s.table,
            ckpt["lsn"].as_str().unwrap()
        )),
        1,
        "fixture: with the capture job stalled, the max LSN and the anchor are below the instance's start"
    );
    s
}

/// An anchor below a just-enabled instance's start loses nothing and refuses nothing.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC + the rivet-duckdb oracle"]
fn mssql_an_anchor_below_a_just_enabled_instance_still_delivers_every_later_change() {
    let _serial = cross_process_serial("mssql_cdc");
    let mut s = anchored_before_the_capture_job_reached_the_instance();
    s.insert(1);
    s.insert(2);
    s.settle();
    s.rig.run_ok();
    assert_eq!(
        cdc_id_ops(&s.rig.out_dir()),
        vec![(1, "insert".to_string()), (2, "insert".to_string())],
        "every row inserted after the anchor is delivered"
    );
    s.rig.run_ok();
}

/// A table with no change since its baseline.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC + the rivet-duckdb oracle"]
fn mssql_a_never_changed_table_is_not_refused_after_cleanup_passes_its_anchor() {
    let _serial = cross_process_serial("mssql_cdc");
    let mut quiet = CdcScenario::mssql_with("cdc_pinquiet", "id INT PRIMARY KEY, v INT", |r, _| {
        r.cdc("initial: snapshot").cdc("until_current: true")
    });
    let mut busy = CdcScenario::mssql_with("cdc_pinbusy", "id INT PRIMARY KEY, v INT", |r, _| r);
    quiet.insert(1);
    quiet.settle();
    quiet.rig.run_ok();
    busy.insert(1);
    busy.settle();
    quiet.rig.run_ok();
    mssql_cdc_exec(&format!(
        "DECLARE @lw binary(10) = sys.fn_cdc_get_max_lsn(); \
         EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'dbo_{}', \
         @low_water_mark = @lw, @threshold = 5000;",
        quiet.table
    ));
    quiet.rig.run_ok();
    mssql_cdc_exec(&format!(
        "UPDATE dbo.{} SET v = 99 WHERE id = 1",
        quiet.table
    ));
    wait_for_capture(&format!("dbo_{}", quiet.table), 2);
    quiet.rig.run_ok();
    assert!(
        cdc_id_ops(&quiet.rig.out_dir()).contains(&(1, "update".to_string())),
        "the change made after the cleanup is delivered"
    );
}

/// An anchor written by rivet 0.31 or older.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC + the rivet-duckdb oracle"]
fn mssql_a_legacy_pinned_anchor_below_the_instance_start_is_refused_until_rebaselined() {
    let _serial = cross_process_serial("mssql_cdc");
    let mut s = CdcScenario::mssql_with("cdc_pinlegacy", "id INT PRIMARY KEY, v INT", |r, _| {
        r.cdc("initial: snapshot").cdc("until_current: true")
    });
    s.rig.run_ok();
    let ckpt = s.rig.checkpoint();
    let mut j: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&ckpt).unwrap()).unwrap();
    j["lsn"] = "00000000000000000001".into();
    j["pinned"] = true.into();
    std::fs::write(&ckpt, j.to_string()).unwrap();
    s.insert(1);
    s.settle();
    assert_log_gap_refusal(&s);
    assert_log_gap_refusal(&s);
    follow_rebaseline_remedy(&mut s.rig, true);
    assert_eq!(
        dir_parquet_id_set(&s.rig.out_dir().join("snapshot")),
        [1].into(),
        "the remedy's baseline holds the row inserted after the legacy anchor"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_table_switched_to_another_over_its_baseline_is_refused_and_switched_back_continues() {
    let _serial = cross_process_serial("mssql_cdc");
    let mut pair = ["a", "b"].map(|x| {
        CdcScenario::mssql_with(
            &format!("cdc_switch_{x}"),
            "id INT PRIMARY KEY, v INT",
            |r, _| r,
        )
    });
    let (ta, tb) = (pair[0].table.clone(), pair[1].table.clone());
    a_table_switched_over_a_baseline_is_refused_and_switched_back_continues(
        Rig::mssql_cdc(&ta, &format!("dbo_{ta}")),
        [&ta, &tb],
        &|r, t| r.repoint(t).capture_instance(&format!("dbo_{t}")),
        &mut |t, op| {
            let s = pair
                .iter_mut()
                .find(|s| s.table == t)
                .expect("a table of the pair");
            match op {
                Churn::Insert(id) => s.insert(id),
                Churn::Update(id) => s.update(id),
                Churn::Delete(id) => s.delete(id),
            }
            s.settle();
        },
    );
}
