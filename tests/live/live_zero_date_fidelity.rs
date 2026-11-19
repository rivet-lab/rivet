//! A MySQL temporal column that can hold a zero or partial date never claims `exact`, and `validate` and `--reconcile` count the cells written as NULL.

use crate::common::*;

const DDL: &str =
    "id INT PRIMARY KEY, d DATE NULL, dt DATETIME NULL, ts TIMESTAMP NULL, t TIME NULL, n INT NULL";

const ROWS: &str = "(1, '0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00', '10:00:00', 1), \
    (2, '2026-00-00', '2026-00-00 00:00:00', '2024-03-15 12:00:00', '11:00:00', 2), \
    (3, '2024-03-15', '2024-03-15 12:00:00', '2024-03-15 12:00:00', '12:00:00', 3), \
    (4, NULL, NULL, NULL, NULL, NULL)";

const COUNTED: &str = "d=2, dt=2, ts=1";

fn seeded_table() -> (String, MysqlTable) {
    use mysql::prelude::Queryable;
    let table = unique_name("zdf_my");
    let mut c = mysql_connect();
    c.query_drop(format!("CREATE TABLE {table} ({DDL})"))
        .unwrap();
    let guard = MysqlTable::adopt(table.clone());
    c.query_drop("SET SESSION sql_mode=''").unwrap();
    c.query_drop(format!("INSERT INTO {table} VALUES {ROWS}"))
        .unwrap();
    (table, guard)
}

fn assert_fidelity(dir: &std::path::Path) {
    for col in ["d", "dt", "ts"] {
        for (native, fidelity) in parquet_field_labels(dir, col) {
            assert_ne!(
                fidelity, "exact",
                "column {col} ({native}) holds zero dates written as NULL and must not claim exact"
            );
        }
    }
    for col in ["t", "n", "id"] {
        for (native, fidelity) in parquet_field_labels(dir, col) {
            assert_eq!(fidelity, "exact", "column {col} ({native}) is unchanged");
        }
    }
}

fn counted_after(said: &str, marker: &str) -> String {
    said.split(marker)
        .nth(1)
        .unwrap_or_else(|| panic!("`{marker}` missing:\n{said}"))
        .split(['\n', ';', '"'])
        .next()
        .unwrap_or_default()
        .trim()
        .to_string()
}

const RECONCILE_MARKER: &str = "zero or partial date cells written as NULL: ";
const VALIDATE_MARKER: &str = "zero or partial date cells were written as NULL: ";

fn text(o: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

/// A batch run over zero dates labels the date columns below exact and keeps the other columns exact.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn zero_dates_are_labelled_compatible_batch_mysql() {
    require_alive(LiveService::Mysql);
    let (table, _guard) = seeded_table();
    let rig = Rig::mysql_batch(&table);
    rig.run_ok();
    assert_fidelity(&rig.out_dir());
    let delivered = dir_parquet_display(&rig.out_dir(), "d");
    assert_eq!(
        delivered.iter().filter(|v| v.is_none()).count(),
        3,
        "{COUNTED}: 2 nulled + 1 source NULL"
    );
}

/// A batch run over zero dates is counted by the reconcile row and by validate.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn zero_dates_are_counted_by_validate_and_reconcile_batch_mysql() {
    require_alive(LiveService::Mysql);
    let (table, _guard) = seeded_table();
    let rig = Rig::mysql_batch(&table);
    let run = rig.run_args(&["--reconcile"]);
    assert!(run.status.success(), "{}", text(&run));
    let reconcile_row = text(&run)
        .lines()
        .find(|l| l.contains("reconcile"))
        .unwrap_or_default()
        .to_string();
    assert_eq!(counted_after(&reconcile_row, RECONCILE_MARKER), COUNTED);
    let validate = rig.cli(&["validate"]);
    assert!(validate.status.success(), "{}", text(&validate));
    assert_eq!(counted_after(&text(&validate), VALIDATE_MARKER), COUNTED);
}

/// A CDC run over zero dates labels the date columns below exact.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn zero_dates_are_labelled_compatible_cdc_mysql() {
    let sc = cdc_zero_dates();
    assert_fidelity(&sc.rig.out_dir());
}

/// A CDC run over zero dates is counted by validate.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn zero_dates_are_counted_by_validate_cdc_mysql() {
    let sc = cdc_zero_dates();
    let validate = sc.rig.cli(&["validate"]);
    assert!(validate.status.success(), "{}", text(&validate));
    assert_eq!(counted_after(&text(&validate), VALIDATE_MARKER), COUNTED);
}

fn cdc_zero_dates() -> CdcScenario {
    let mut sc = CdcScenario::mysql_with("zdf_cdc", DDL, |r, _| r);
    sc.rig.run_ok();
    let t = sc.table.clone();
    sc.sql("SET SESSION sql_mode=''");
    sc.sql(&format!("INSERT INTO {t} VALUES {ROWS}"));
    sc.rig.run_ok();
    sc
}

/// A run with no zero or partial date writes no count row and keeps its values.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_clean_temporal_column_is_delivered_and_validated_without_a_count_mysql() {
    use mysql::prelude::Queryable;
    require_alive(LiveService::Mysql);
    let table = unique_name("zdf_clean");
    let mut c = mysql_connect();
    c.query_drop(format!("CREATE TABLE {table} ({DDL})"))
        .unwrap();
    let _guard = MysqlTable::adopt(table.clone());
    c.query_drop(format!(
        "INSERT INTO {table} VALUES (1, '2024-03-15', '2024-03-15 12:00:00', '2024-03-15 12:00:00', '10:00:00', 1)"
    ))
    .unwrap();
    let rig = Rig::mysql_batch(&table);
    let run = rig.run_args(&["--reconcile"]);
    assert!(run.status.success(), "{}", text(&run));
    assert!(
        !text(&run).contains("zero or partial date"),
        "{}",
        text(&run)
    );
    let validate = rig.cli(&["validate"]);
    assert!(validate.status.success(), "{}", text(&validate));
    assert!(
        !text(&validate).contains("zero or partial date"),
        "{}",
        text(&validate)
    );
    assert_eq!(
        dir_parquet_display(&rig.out_dir(), "d"),
        vec![Some("2024-03-15".to_string())]
    );
}
