//! `rivet check` refuses a `columns:` override the run's converter refuses, with the run's code and exit class.

use crate::common::*;

const CODE: &str = "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH";

/// The run and both `check` spellings refuse `rig`'s export by `CODE`, exit 1; returns the run's stderr.
fn check_and_run_refuse_alike(rig: Rig, name: &str) -> String {
    check_and_run_refuse_alike_by(rig, name, Refused::by_code(CODE, 1))
}

/// The run and both `check` spellings refuse `rig`'s export by `want`; returns the run's stderr.
pub(crate) fn check_and_run_refuse_alike_by(rig: Rig, name: &str, want: Refused<'_>) -> String {
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
    let said = check_and_run_refuse_alike_by(
        rig,
        &name,
        Refused::by_code("RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5),
    );
    assert!(said.contains("column `u` is declared uuid"), "{said}");
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
    check_and_run_refuse_alike_by(
        rig,
        &name,
        Refused::by_code("RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5),
    );
}

/// `check` exits 0 and prints the values-unread note on stderr only for an export that declares `columns:`.
fn check_says_it_read_no_values_only_under_columns(declared: Rig, plain: Rig, name: &str) {
    let said = |rig: &Rig| {
        let o = rig.cli(&["check", "--export", name]);
        let (err, out) = (
            String::from_utf8_lossy(&o.stderr).into_owned(),
            String::from_utf8_lossy(&o.stdout).into_owned(),
        );
        assert_eq!(o.status.code(), Some(0), "{err}");
        assert!(!out.contains("note[values-unread]"), "stdout: {out}");
        err
    };
    let with = said(&declared);
    assert!(with.contains("note[values-unread]"), "{with}");
    for code in [
        "RIVET_SOURCE_VALUE_UNREPRESENTABLE",
        "RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE",
    ] {
        assert!(with.contains(code), "{code}: {with}");
    }
    let without = said(&plain);
    assert!(!without.contains("note[values-unread]"), "{without}");
}

#[test]
#[ignore = "live: postgres"]
fn check_says_it_read_no_values_only_under_columns_postgres() {
    require_alive(LiveService::Postgres);
    let name = unique_name("ovw_note_pg");
    let out = tempfile::tempdir().unwrap();
    let rig = || {
        Rig::pg_batch(&name)
            .query("SELECT id, quantity FROM orders")
            .mode("full")
            .dest_path(out.path().to_path_buf())
    };
    let declared = rig()
        .export_line("columns:")
        .export_line("  quantity: \"int64\"");
    check_says_it_read_no_values_only_under_columns(declared, rig(), &name);
}

#[test]
#[ignore = "live: mysql"]
fn check_says_it_read_no_values_only_under_columns_mysql() {
    require_alive(LiveService::Mysql);
    let name = unique_name("ovw_note_my");
    let out = tempfile::tempdir().unwrap();
    let rig = || {
        Rig::mysql_batch(&name)
            .query("SELECT CAST(7 AS SIGNED) AS n")
            .mode("full")
            .dest_path(out.path().to_path_buf())
    };
    let declared = rig().export_line("columns:").export_line("  n: \"int64\"");
    check_says_it_read_no_values_only_under_columns(declared, rig(), &name);
}

#[test]
#[ignore = "live: mssql"]
fn check_says_it_read_no_values_only_under_columns_mssql() {
    require_alive(LiveService::Mssql);
    let name = unique_name("ovw_note_ms");
    let out = tempfile::tempdir().unwrap();
    let rig = || {
        Rig::mssql_batch(&name)
            .query("SELECT CAST(7 AS INT) AS n")
            .mode("full")
            .dest_path(out.path().to_path_buf())
    };
    let declared = rig().export_line("columns:").export_line("  n: \"int64\"");
    check_says_it_read_no_values_only_under_columns(declared, rig(), &name);
}

#[test]
#[ignore = "live: oracle"]
fn check_says_it_read_no_values_only_under_columns_oracle() {
    require_alive(LiveService::Oracle);
    let name = unique_name("ovw_note_or");
    let out = tempfile::tempdir().unwrap();
    let rig = || {
        Rig::oracle_batch(&name)
            .query("SELECT CAST(7 AS NUMBER(10,0)) AS n FROM dual")
            .mode("full")
            .dest_path(out.path().to_path_buf())
    };
    let declared = rig().export_line("columns:").export_line("  N: \"int64\"");
    check_says_it_read_no_values_only_under_columns(declared, rig(), &name);
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

/// The run and both `check` spellings refuse a `decimal(20,0)` override over a value with cents, exit 5.
fn lossy_scale_override_is_refused_like_the_run(rig: Rig, name: &str) {
    let said = check_and_run_refuse_alike_by(
        rig,
        name,
        Refused::by_code("RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5),
    );
    assert!(said.contains("1.5"), "{said}");
}

#[test]
#[ignore = "live: postgres"]
fn a_lossy_scale_override_is_refused_by_check_like_the_run_postgres() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(2);
    let name = unique_name("ovw_scale_pg");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&name)
        .query(&format!(
            "SELECT id, amount FROM {} WHERE id = 1",
            table.name()
        ))
        .mode("full")
        .export_line("columns:")
        .export_line("  amount: \"decimal(20,0)\"")
        .dest_path(out.path().to_path_buf());
    lossy_scale_override_is_refused_like_the_run(rig, &name);
}

#[test]
#[ignore = "live: mysql"]
fn a_lossy_scale_override_is_refused_by_check_like_the_run_mysql() {
    require_alive(LiveService::Mysql);
    let table = seed_mysql_numeric_table(2);
    let name = unique_name("ovw_scale_my");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_batch(&name)
        .query(&format!(
            "SELECT id, amount FROM {} WHERE id = 1",
            table.name()
        ))
        .mode("full")
        .export_line("columns:")
        .export_line("  amount: \"decimal(20,0)\"")
        .dest_path(out.path().to_path_buf());
    lossy_scale_override_is_refused_like_the_run(rig, &name);
}

#[test]
#[ignore = "live: mysql"]
fn a_nanosecond_timestamp_override_is_refused_by_the_wire_check_mysql() {
    require_alive(LiveService::Mysql);
    for (declared, remedy, label) in [
        (
            "timestamp_ns",
            "declare `timestamp` (microseconds)",
            "timestamp(nanosecond)",
        ),
        (
            "timestamp_tz_ns",
            "declare `timestamp_tz` (microseconds)",
            "timestamp_tz(nanosecond,UTC)",
        ),
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
            stderr.contains(&format!(
                "column `ts` is datetime (read as timestamp(microsecond)) and `columns:` declares it {label}"
            )),
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
            stderr.contains("column `c` is ")
                && stderr.contains(&format!(
                    "and `columns:` declares it {label} — Mssql does not convert that pair"
                )),
            "{expr} as {declared}: {stderr}"
        );
    }
}

/// The message every wide-decimal refusal carries after the declared type.
const WIDE_DECIMAL_REFUSAL: &str = "its precision is above 38; declare the column's own `decimal(p,s)` with p <= 38 (see the source column definition), or remove the override";

/// A wide-decimal refusal names the cause and the rule, and the operator's own `valid` declaration then passes `check` and delivers `delivered` for the one source row.
fn wide_decimal_refusal_then_valid_declaration_delivers(
    rig: impl Fn(&str, &str) -> Rig,
    column: &str,
    prefix: &str,
    (wide, valid, delivered): (&str, &str, &str),
) {
    let name = unique_name(prefix);
    let out = tempfile::tempdir().unwrap();
    let refused = rig(&name, wide).dest_path(out.path().to_path_buf());
    let stderr = check_and_run_refuse_alike(refused, &name);
    assert!(stderr.contains(WIDE_DECIMAL_REFUSAL), "{wide}: {stderr}");
    assert!(!stderr.contains("`text`"), "{stderr}");
    let name = unique_name(prefix);
    let out = tempfile::tempdir().unwrap();
    let applied = rig(&name, valid).dest_path(out.path().to_path_buf());
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
    assert_eq!(
        dir_parquet_decimal_strings(out.path(), column),
        vec![delivered.to_string()],
        "{wide} -> {valid}"
    );
}

/// The wide declarations, the operator's own valid declaration for a source holding 12.50, and the text it must deliver.
const WIDE_DECIMAL_CASES: [(&str, &str, &str); 2] = [
    ("decimal(50,2)", "decimal(10,2)", "12.50"),
    ("decimal(50,40)", "decimal(38,2)", "12.50"),
];

#[test]
#[ignore = "live: mssql"]
fn a_decimal_override_past_precision_38_is_refused_by_the_wire_check_mssql() {
    require_alive(LiveService::Mssql);
    for case in WIDE_DECIMAL_CASES {
        wide_decimal_refusal_then_valid_declaration_delivers(
            |name, declared| {
                Rig::mssql_batch(name)
                    .query("SELECT CAST(12.5 AS DECIMAL(10,2)) AS d")
                    .mode("full")
                    .export_line("columns:")
                    .export_line(&format!("  d: \"{declared}\""))
            },
            "d",
            "ovw_dec_ms",
            case,
        );
    }
}

#[test]
#[ignore = "live: oracle"]
fn a_decimal_override_past_precision_38_is_refused_by_the_wire_check_oracle() {
    require_alive(LiveService::Oracle);
    for case in WIDE_DECIMAL_CASES {
        wide_decimal_refusal_then_valid_declaration_delivers(
            |name, declared| {
                Rig::oracle_batch(name)
                    .query("SELECT CAST(12.5 AS NUMBER(10,2)) AS d FROM dual")
                    .mode("full")
                    .export_line("columns:")
                    .export_line(&format!("  D: \"{declared}\""))
            },
            "D",
            "ovw_dec_or",
            case,
        );
    }
}

const FORMAT_REFUSAL: Refused<'static> =
    Refused::by_code("RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED", 1);
const UNMAPPED_REFUSAL: Refused<'static> = Refused::by_code("RIVET_TYPE_UNSAFE_MAPPING", 1);

/// A `format: csv` export of a column declared `declared` is refused by the run and both `check` spellings, naming the column and the format's remedy.
fn csv_column_is_refused_like_the_run(rig: Rig, name: &str, column: &str, declared: &str) {
    let said = check_and_run_refuse_alike_by(
        rig.with_format("csv")
            .export_line("columns:")
            .export_line(&format!("  {column}: \"{declared}\"")),
        name,
        FORMAT_REFUSAL,
    );
    assert!(
        said.contains(&format!("CSV cannot serialize column '{column}'")),
        "{said}"
    );
    assert!(said.contains("use `format: parquet`"), "{said}");
}

/// An export of a column no Arrow type carries is refused by the run and both `check` spellings with the one `columns:` declaration refusal.
fn unmapped_column_is_refused_like_the_run(rig: Rig, name: &str, column: &str) {
    let said = check_and_run_refuse_alike_by(rig, name, UNMAPPED_REFUSAL);
    assert!(
        said.contains("1 column(s) need a `columns:` declaration"),
        "{said}"
    );
    assert!(said.contains(&format!("column '{column}'")), "{said}");
}

#[test]
#[ignore = "live: postgres"]
fn a_csv_export_of_a_decimal256_column_is_refused_by_check_like_the_run_postgres() {
    require_alive(LiveService::Postgres);
    let name = unique_name("fmt_dec_pg");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&name)
        .query("SELECT price FROM orders")
        .mode("full")
        .dest_path(out.path().to_path_buf());
    csv_column_is_refused_like_the_run(rig, &name, "price", "decimal(60,10)");
}

#[test]
#[ignore = "live: mysql"]
fn a_csv_export_of_a_decimal256_column_is_refused_by_check_like_the_run_mysql() {
    require_alive(LiveService::Mysql);
    let name = unique_name("fmt_dec_my");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_batch(&name)
        .query("SELECT CAST(12.5 AS DECIMAL(10,2)) AS d")
        .mode("full")
        .dest_path(out.path().to_path_buf());
    csv_column_is_refused_like_the_run(rig, &name, "d", "decimal(60,10)");
}

/// A column no Arrow type carries, declared as an integer, is refused by the run and both `check` spellings with the one wire-mismatch code, naming the column.
#[test]
#[ignore = "live: postgres"]
fn a_declared_type_on_a_column_without_an_arrow_type_is_refused_by_check_like_the_run_postgres() {
    require_alive(LiveService::Postgres);
    let name = unique_name("unmapped_ovr_pg");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&name)
        .query("SELECT '10.0.0.1'::inet AS c")
        .mode("full")
        .export_line("columns:")
        .export_line("  c: \"int64\"")
        .dest_path(out.path().to_path_buf());
    let said = check_and_run_refuse_alike(rig, &name);
    assert!(
        said.contains("column `c` is inet") && said.contains("Postgres does not convert that pair"),
        "{said}"
    );
}

#[test]
#[ignore = "live: mysql"]
fn a_column_without_an_arrow_type_is_refused_by_check_like_the_run_mysql() {
    require_alive(LiveService::Mysql);
    let name = unique_name("unmapped_my");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_batch(&name)
        .query("SELECT ST_GeomFromText('POINT(1 1)') AS g")
        .mode("full")
        .dest_path(out.path().to_path_buf());
    unmapped_column_is_refused_like_the_run(rig, &name, "g");
}

#[test]
#[ignore = "live: postgres"]
fn check_still_accepts_a_csv_decimal128_export_and_a_declared_cidr_postgres() {
    require_alive(LiveService::Postgres);
    let name = unique_name("fmt_ok_pg");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&name)
        .query("SELECT price, '10.0.0.0/8'::cidr::text AS c FROM orders")
        .mode("full")
        .export_line("columns:")
        .export_line("  price: \"decimal(38,10)\"")
        .with_format("csv")
        .dest_path(out.path().to_path_buf());
    for args in [
        &["check", "--export", name.as_str()][..],
        &["check", "--export", name.as_str(), "--type-report"][..],
        &["plan", "--export", name.as_str()][..],
    ] {
        let o = rig.cli(args);
        assert_eq!(
            o.status.code(),
            Some(0),
            "{args:?}: {}",
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
#[ignore = "live: mssql"]
fn a_csv_export_of_a_nanosecond_timestamp_column_is_refused_by_check_like_the_run_mssql() {
    require_alive(LiveService::Mssql);
    let name = unique_name("fmt_ns_ms");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mssql_batch(&name)
        .query("SELECT CAST('2024-01-01 10:30:00' AS DATETIME2) AS d")
        .mode("full")
        .dest_path(out.path().to_path_buf());
    csv_column_is_refused_like_the_run(rig, &name, "d", "timestamp_ns");
}

#[test]
#[ignore = "live: oracle"]
fn a_csv_export_of_a_nanosecond_timestamp_column_is_refused_by_check_like_the_run_oracle() {
    require_alive(LiveService::Oracle);
    let name = unique_name("fmt_ns_or");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_batch(&name)
        .query("SELECT TIMESTAMP '2024-01-01 10:30:00' AS D FROM dual")
        .mode("full")
        .dest_path(out.path().to_path_buf());
    csv_column_is_refused_like_the_run(rig, &name, "D", "timestamp_ns");
}

#[test]
#[ignore = "live: mssql"]
fn a_column_without_an_arrow_type_is_refused_by_check_like_the_run_mssql() {
    require_alive(LiveService::Mssql);
    let name = unique_name("unmapped_ms");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::mssql_batch(&name)
        .query("SELECT CAST('<a/>' AS XML) AS x")
        .mode("full")
        .dest_path(out.path().to_path_buf());
    unmapped_column_is_refused_like_the_run(rig, &name, "x");
}
