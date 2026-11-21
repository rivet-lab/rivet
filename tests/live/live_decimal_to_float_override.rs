//! A source decimal under a `float8` override is delivered when the double prints back the same digits and refused by code, nothing written, when it would round.

use crate::common::*;
use crate::live_int_to_float_override::readback_only;

const NOT_EXACT: &str = "RIVET_SOURCE_VALUE_UNREPRESENTABLE";

const HOLDS: [&str; 4] = ["2.5", "-7", "12345678901.2345", "NULL"];
const HOLDS_FLOATS: [Option<f64>; 4] = [Some(2.5), Some(-7.0), Some(12345678901.2345), None];
const ROUNDED: [&str; 2] = ["1.5", "922337203685477.5807"];

fn insert_rows(table: &str, values: &[&str]) -> String {
    let rows = values
        .iter()
        .enumerate()
        .map(|(k, v)| format!("({}, {v})", k + 1))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {table} (id, v) VALUES {rows}")
}

fn assert_refused_with_remedy(rig: &Rig, rows_written: usize) {
    let out = rig.run();
    assert_refused(&out, Refused::by_code(NOT_EXACT, 5));
    let said = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(said.contains("Declare `decimal(p,s)`"), "{said}");
    assert_eq!(
        files_with_extension(&rig.out_dir(), "parquet").len(),
        rows_written,
        "nothing is written before the refusal"
    );
}

/// MySQL DECIMAL(19,4) under float8 delivers a value the double prints back and refuses one it would round.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_decimal_under_float8_is_delivered_or_refused_never_rounded_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let make = |values: &[&str]| {
        let table = unique_name("dec_f8_my");
        let mut c = mysql_connect();
        c.query_drop(format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY, v DECIMAL(19,4) NULL)"
        ))
        .unwrap();
        let guard = MysqlTable::adopt(table.clone());
        c.query_drop(insert_rows(&table, values)).unwrap();
        let rig = readback_only(Rig::mysql_batch(&table)).export_line("columns: {v: float8}");
        (rig, guard)
    };
    let (rig, _g) = make(&HOLDS);
    rig.run_ok();
    assert_eq!(dir_parquet_floats(&rig.out_dir(), "v"), HOLDS_FLOATS);
    let (rig, _g) = make(&ROUNDED);
    assert_refused_with_remedy(&rig, 0);
}

/// SQL Server money under float8 delivers a value the double prints back and refuses one it would round.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_money_under_float8_is_delivered_or_refused_never_rounded_mssql() {
    require_alive(LiveService::Mssql);
    let make = |values: &[&str]| {
        let table = unique_name("dec_f8_ms");
        mssql_exec(&format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY, v MONEY NULL)"
        ));
        let guard = MssqlTable::adopt(table.clone());
        mssql_exec(&insert_rows(&table, values));
        let rig = readback_only(Rig::mssql_batch(&table)).export_line("columns: {v: float8}");
        (rig, guard)
    };
    let (rig, _g) = make(&HOLDS);
    rig.run_ok();
    assert_eq!(dir_parquet_floats(&rig.out_dir(), "v"), HOLDS_FLOATS);
    let (rig, _g) = make(&ROUNDED);
    assert_refused_with_remedy(&rig, 0);
}

/// Oracle NUMBER(38,4) under float8 delivers a value the double prints back and refuses one it would round.
#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_number_under_float8_is_delivered_or_refused_never_rounded_oracle() {
    require_alive(LiveService::Oracle);
    let make = |values: &[&str]| {
        let t = OracleTable::create("dec_f8_or", "id INTEGER PRIMARY KEY, v NUMBER(38,4)");
        ora_exec(&insert_rows(t.name(), values));
        let rig = readback_only(Rig::oracle_batch(t.name())).export_line("columns: {V: float8}");
        (rig, t)
    };
    let (rig, _t) = make(&HOLDS);
    rig.run_ok();
    assert_eq!(dir_parquet_floats(&rig.out_dir(), "v"), HOLDS_FLOATS);
    let (rig, _t) = make(&["1.5", "9007199254740993"]);
    assert_refused_with_remedy(&rig, 0);
}
