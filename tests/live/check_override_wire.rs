//! `rivet check` refuses a `columns:` override the run's converter refuses, with the run's code and exit class.

use crate::common::*;

const CODE: &str = "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH";

/// The run and both `check` spellings refuse `rig`'s export by `CODE`, exit 1; returns the run's stderr.
fn check_and_run_refuse_alike(rig: Rig, name: &str) -> String {
    let want = Refused::by_code(CODE, 1);
    let run = rig.run_args(&["--export", name]);
    assert_refused(&run, want);
    assert_refused(&rig.cli(&["check", "--export", name]), want);
    assert_refused(
        &rig.cli(&["check", "--export", name, "--type-report"]),
        want,
    );
    String::from_utf8_lossy(&run.stderr).into_owned()
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
#[ignore = "live: postgres"]
fn a_nanosecond_timestamp_override_is_refused_by_the_wire_check_postgres() {
    require_alive(LiveService::Postgres);
    let name = unique_name("ovw_ns_pg");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&name)
        .query("SELECT TIMESTAMP '2300-01-01 00:00:00' AS ts")
        .mode("full")
        .export_line("columns:")
        .export_line("  ts: \"timestamp_ns\"")
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

#[test]
#[ignore = "live: mysql"]
fn a_nanosecond_timestamp_override_is_refused_by_the_wire_check_mysql() {
    require_alive(LiveService::Mysql);
    for (declared, remedy) in [
        ("timestamp_ns", "declare `timestamp` (microseconds)"),
        ("timestamp_tz_ns", "declare `timestamp_tz` (microseconds)"),
    ] {
        let name = unique_name("ovw_ns_my");
        let out = tempfile::tempdir().unwrap();
        let rig = Rig::mysql_batch(&name)
            .query("SELECT CAST('2024-01-01 00:00:01.123456' AS DATETIME(6)) AS ts")
            .mode("full")
            .export_line("columns:")
            .export_line(&format!("  ts: \"{declared}\""))
            .dest_path(out.path().to_path_buf());
        let stderr = check_and_run_refuse_alike(rig, &name);
        assert!(stderr.contains(remedy), "{declared}: {stderr}");
        assert!(
            stderr.contains("mysql: column `ts` is declared timestamp"),
            "{declared}: {stderr}"
        );
    }
}

#[test]
#[ignore = "live: mysql"]
fn a_nanosecond_timestamp_override_is_refused_on_an_empty_source_mysql() {
    require_alive(LiveService::Mysql);
    let name = unique_name("ovw_ns_my_empty");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_batch(&name)
        .query(
            "SELECT CAST('2024-01-01 00:00:01' AS DATETIME(6)) AS ts FROM (SELECT 1) t WHERE 1 = 0",
        )
        .mode("full")
        .export_line("columns:")
        .export_line("  ts: \"timestamp_ns\"")
        .dest_path(out.path().to_path_buf());
    check_and_run_refuse_alike(rig, &name);
}

#[test]
#[ignore = "live: mssql"]
fn a_value_the_declared_type_cannot_read_is_refused_not_an_internal_stop_mssql() {
    require_alive(LiveService::Mssql);
    for (expr, declared, label) in [
        ("CAST(1 AS TINYINT)", "bool", "bool"),
        ("CAST(1 AS INT)", "float8", "float64"),
        (
            "CAST('2024-01-01' AS DATE)",
            "timestamp",
            "timestamp(microsecond)",
        ),
        ("CAST(1.5 AS DECIMAL(10,2))", "int8", "int64"),
    ] {
        let name = unique_name("ovw_val_ms");
        let out = tempfile::tempdir().unwrap();
        let rig = Rig::mssql_batch(&name)
            .query(&format!("SELECT {expr} AS c"))
            .mode("full")
            .export_line("columns:")
            .export_line(&format!("  c: \"{declared}\""))
            .dest_path(out.path().to_path_buf());
        let stderr = check_and_run_refuse_alike(rig, &name);
        assert!(
            stderr.contains(&format!(
                "column `c` is declared {label} by a `columns:` override"
            )),
            "{expr} as {declared}: {stderr}"
        );
    }
}

/// The declaration `stderr`'s refusal prints between backticks after "declare ".
fn printed_declaration(stderr: &str) -> String {
    stderr
        .split("declare `")
        .nth(1)
        .and_then(|r| r.split('`').next())
        .unwrap_or_else(|| panic!("no declaration in {stderr}"))
        .to_string()
}

/// A wide-decimal refusal prints `decimal(38,2)`, and that declaration applied as the config passes `check` and delivers the row.
fn wide_decimal_refusal_remedy_delivers(
    rig: impl Fn(&str, &str) -> Rig,
    column: &str,
    prefix: &str,
) {
    let name = unique_name(prefix);
    let out = tempfile::tempdir().unwrap();
    let refused = rig(&name, "decimal(50,2)").dest_path(out.path().to_path_buf());
    let stderr = check_and_run_refuse_alike(refused, &name);
    assert!(
        stderr.contains("declare `decimal(38,2)`, or remove the override"),
        "{stderr}"
    );
    assert!(!stderr.contains("`text`"), "{stderr}");
    let remedy = printed_declaration(&stderr);
    let name = unique_name(prefix);
    let out = tempfile::tempdir().unwrap();
    let applied = rig(&name, &remedy).dest_path(out.path().to_path_buf());
    let checked = applied.cli(&["check", "--export", &name]);
    assert_eq!(
        checked.status.code(),
        Some(0),
        "{column}: {}",
        String::from_utf8_lossy(&checked.stderr)
    );
    let run = applied.run_args(&["--export", &name]);
    assert_eq!(
        run.status.code(),
        Some(0),
        "{column}: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(total_parquet_rows(out.path()), 1);
}

#[test]
#[ignore = "live: mssql"]
fn a_decimal_override_past_precision_38_is_refused_by_the_wire_check_mssql() {
    require_alive(LiveService::Mssql);
    wide_decimal_refusal_remedy_delivers(
        |name, declared| {
            Rig::mssql_batch(name)
                .query("SELECT CAST(1.5 AS DECIMAL(10,2)) AS d")
                .mode("full")
                .export_line("columns:")
                .export_line(&format!("  d: \"{declared}\""))
        },
        "d",
        "ovw_dec_ms",
    );
}

#[test]
#[ignore = "live: oracle"]
fn a_decimal_override_past_precision_38_is_refused_by_the_wire_check_oracle() {
    require_alive(LiveService::Oracle);
    wide_decimal_refusal_remedy_delivers(
        |name, declared| {
            Rig::oracle_batch(name)
                .query("SELECT CAST(1.5 AS NUMBER(10,2)) AS d FROM dual")
                .mode("full")
                .export_line("columns:")
                .export_line(&format!("  D: \"{declared}\""))
        },
        "D",
        "ovw_dec_or",
    );
}
