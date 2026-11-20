//! SQL Server MONEY and SMALLMONEY are delivered exact over their whole range.

use crate::common::*;

const MONEY: &[&str] = &[
    "922337203685477.5807",
    "-922337203685477.5808",
    "-922337203685477.5807",
    "922337203685476.5807",
    "1000000000000.0001",
    "900719925474.0991",
    "0",
    "0.0001",
    "-0.0001",
    "123.4567",
];

const SMALLMONEY: &[&str] = &[
    "214748.3647",
    "-214748.3648",
    "0",
    "0.0001",
    "-0.0001",
    "12.34",
];

const NO_ORACLE: &str =
    "the subject is MONEY exactness, graded below by the server's own CONVERT text";

/// A table `id, v` holding `values` then a NULL row; returns the guard.
fn table_of(ty: &str, values: &[&str]) -> (String, MssqlTable) {
    let table = unique_name("mssql_money");
    mssql_exec(&format!(
        "CREATE TABLE dbo.{table} (id INT PRIMARY KEY, v {ty} NULL)"
    ));
    let guard = MssqlTable::adopt(table.clone());
    let rows = values
        .iter()
        .enumerate()
        .map(|(i, v)| format!("({}, CAST('{v}' AS {ty}))", i + 1))
        .chain([format!("({}, NULL)", values.len() + 1)])
        .collect::<Vec<_>>()
        .join(", ");
    mssql_exec(&format!("INSERT INTO dbo.{table} (id, v) VALUES {rows}"));
    (table, guard)
}

/// What the server itself prints for each row, four decimals, NULL as the empty string.
fn server_text(table: &str) -> Vec<String> {
    sqlcmd_lines(
        "rivet-mssql-1",
        &format!("SELECT ISNULL(CONVERT(varchar(40), v, 2), '') FROM dbo.{table} ORDER BY id"),
    )
}

/// A money value of 9e11 or more is delivered exact, as are the type's extremes and the NULL row.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn money_and_smallmoney_are_delivered_exact_over_their_whole_range() {
    require_alive(LiveService::Mssql);
    for (ty, values) in [("MONEY", MONEY), ("SMALLMONEY", SMALLMONEY)] {
        let (table, _guard) = table_of(ty, values);
        let rig = Rig::mssql_batch(&table).no_oracle(NO_ORACLE);
        rig.run_ok();
        let got = dir_parquet_decimal_strings(&rig.out_dir(), "v");
        assert_eq!(got, server_text(&table), "{ty}");
        assert_eq!(got.len(), values.len() + 1, "{ty}: every row and the NULL");
    }
}

const NEAR: &[&str] = &[
    "900719925474.0991",
    "-900719925474.0991",
    "123.4567",
    "0.0001",
    "-0.0001",
    "0",
    "12.3",
];

/// A MONEY value inside the old f64-exact range under the default mapping is delivered as it was before.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn money_inside_the_old_exact_range_is_delivered_as_before() {
    require_alive(LiveService::Mssql);
    let (table, _guard) = table_of("MONEY", NEAR);
    let rig = Rig::mssql_batch(&table).no_oracle(NO_ORACLE);
    rig.run_ok();
    assert_eq!(
        dir_parquet_decimal_strings(&rig.out_dir(), "v"),
        server_text(&table)
    );
}

/// A `float64` override over MONEY delivers the nearest f64 of the server's value, NULL kept.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_float64_override_over_money_delivers_the_nearest_f64() {
    require_alive(LiveService::Mssql);
    let (table, _guard) = table_of("MONEY", NEAR);
    let rig = Rig::mssql_batch(&table)
        .no_oracle(NO_ORACLE)
        .export_line("columns: {v: float64}");
    rig.run_ok();
    let want: Vec<Option<f64>> = server_text(&table)
        .iter()
        .map(|t| (!t.is_empty()).then(|| t.parse().unwrap()))
        .collect();
    assert_eq!(dir_parquet_floats(&rig.out_dir(), "v"), want);
}

/// A `decimal(19,2)` override over MONEY delivers the values that fit two decimals as the server prints them.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_narrower_decimal_override_over_money_delivers_values_that_fit() {
    require_alive(LiveService::Mssql);
    let (table, _guard) = table_of("MONEY", &["900719925474.09", "-5.5", "0", "12.34"]);
    let rig = Rig::mssql_batch(&table)
        .no_oracle(NO_ORACLE)
        .export_line("columns: {v: \"decimal(19,2)\"}");
    rig.run_ok();
    let want = sqlcmd_lines(
        "rivet-mssql-1",
        &format!(
            "SELECT ISNULL(CONVERT(varchar(40), CAST(v AS decimal(19,2))), '') FROM dbo.{table} ORDER BY id"
        ),
    );
    assert_eq!(dir_parquet_decimal_strings(&rig.out_dir(), "v"), want);
}

/// A `decimal(19,2)` override over a MONEY value holding four decimals is refused by code, never written rounded.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_narrower_decimal_override_over_money_refuses_a_value_it_would_round() {
    require_alive(LiveService::Mssql);
    let (table, _guard) = table_of("MONEY", &["12.34", "123.4567"]);
    let rig = Rig::mssql_batch(&table).export_line("columns: {v: \"decimal(19,2)\"}");
    let out = rig.run();
    assert_refused(
        &out,
        Refused::by_code("RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1),
    );
}
