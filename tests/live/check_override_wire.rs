//! `rivet check` refuses a `columns:` override the run's converter refuses, with the run's code and exit class.

use crate::common::*;

const CODE: &str = "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH";

/// The run and both `check` spellings refuse `rig`'s export by `CODE`, exit 1.
fn check_and_run_refuse_alike(rig: Rig, name: &str) {
    let want = Refused::by_code(CODE, 1);
    assert_refused(&rig.run_args(&["--export", name]), want);
    assert_refused(&rig.cli(&["check", "--export", name]), want);
    assert_refused(
        &rig.cli(&["check", "--export", name, "--type-report"]),
        want,
    );
}

#[test]
#[ignore = "live: postgres"]
fn check_refuses_an_int4_column_declared_string_postgres() {
    require_alive(LiveService::Postgres);
    let name = unique_name("ovw_wire_pg");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&name)
        .query("SELECT id, quantity, price, status FROM orders")
        .mode("full")
        .export_line("columns:")
        .export_line("  quantity: \"string\"")
        .dest_path(out.path().to_path_buf());
    check_and_run_refuse_alike(rig, &name);
}

#[test]
#[ignore = "live: mysql"]
fn check_refuses_a_text_value_declared_uuid_mysql() {
    require_alive(LiveService::Mysql);
    let name = unique_name("ovw_wire_my");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_batch(&name)
        .query("SELECT 'not-a-uuid' AS u")
        .mode("full")
        .export_line("columns:")
        .export_line("  u: \"uuid\"")
        .dest_path(out.path().to_path_buf());
    check_and_run_refuse_alike(rig, &name);
}

#[test]
#[ignore = "live: mssql"]
fn check_refuses_an_int_column_declared_string_mssql() {
    require_alive(LiveService::Mssql);
    let name = unique_name("ovw_wire_ms");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mssql_batch(&name)
        .query("SELECT CAST(7 AS INT) AS n")
        .mode("full")
        .export_line("columns:")
        .export_line("  n: \"string\"")
        .dest_path(out.path().to_path_buf());
    check_and_run_refuse_alike(rig, &name);
}

#[test]
#[ignore = "live: oracle"]
fn check_refuses_a_timestamp_with_a_time_declared_date_oracle() {
    require_alive(LiveService::Oracle);
    let name = unique_name("ovw_wire_or");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_batch(&name)
        .query("SELECT TIMESTAMP '2024-01-01 10:30:00' AS d FROM dual")
        .mode("full")
        .export_line("columns:")
        .export_line("  D: \"date\"")
        .dest_path(out.path().to_path_buf());
    check_and_run_refuse_alike(rig, &name);
}

#[test]
#[ignore = "live: postgres"]
fn check_still_accepts_overrides_the_run_accepts_postgres() {
    require_alive(LiveService::Postgres);
    let name = unique_name("ovw_ok_pg");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&name)
        .query("SELECT id, quantity, price, status FROM orders")
        .mode("full")
        .export_line("columns:")
        .export_line("  quantity: \"int64\"")
        .export_line("  status: \"string\"")
        .dest_path(out.path().to_path_buf());
    for args in [
        &["check", "--export", name.as_str()][..],
        &["check", "--export", name.as_str(), "--type-report"][..],
    ] {
        let o = rig.cli(args);
        assert_eq!(
            o.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&o.stderr)
        );
    }
    let o = rig.run_args(&["--export", &name]);
    assert_eq!(
        o.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
}
