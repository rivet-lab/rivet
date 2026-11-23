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

/// The delivered `v` column as decimal text, NULL as the empty string.
fn delivered(rig: &Rig) -> Vec<String> {
    dir_parquet_decimal_strings(&rig.out_dir(), "v")
}

/// What the server itself prints for each row, four decimals, NULL as the empty string.
fn server_rendering(table: &str) -> Vec<String> {
    sqlcmd_lines(
        "rivet-mssql-1",
        &format!("SELECT ISNULL(CONVERT(varchar(40), v, 2), '') FROM dbo.{table} ORDER BY id"),
    )
}

/// A money value of 9e11 or more is delivered exact, as are the type's extremes and the NULL row.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn money_and_smallmoney_keep_every_digit_over_their_whole_range() {
    require_alive(LiveService::Mssql);
    for (ty, values) in [("MONEY", MONEY), ("SMALLMONEY", SMALLMONEY)] {
        let (table, _guard) = table_of(ty, values);
        let rig = Rig::mssql_batch(&table);
        rig.run_ok();
        let got = delivered(&rig);
        assert_eq!(got, server_rendering(&table), "{ty}");
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
fn money_inside_the_old_range_keeps_its_digits() {
    require_alive(LiveService::Mssql);
    let (table, _guard) = table_of("MONEY", NEAR);
    let rig = Rig::mssql_batch(&table);
    rig.run_ok();
    assert_eq!(delivered(&rig), server_rendering(&table));
}

/// A `float64` override over MONEY delivers the nearest f64 of the server's value, NULL kept.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_float64_override_over_money_delivers_the_nearest_f64() {
    require_alive(LiveService::Mssql);
    let (table, _guard) = table_of("MONEY", NEAR);
    let rig = Rig::mssql_batch(&table).export_line("columns: {v: float64}");
    rig.run_ok();
    let want: Vec<Option<f64>> = server_rendering(&table)
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
    let rig = Rig::mssql_batch(&table).export_line("columns: {v: \"decimal(19,2)\"}");
    rig.run_ok();
    let want = sqlcmd_lines(
        "rivet-mssql-1",
        &format!(
            "SELECT ISNULL(CONVERT(varchar(40), CAST(v AS decimal(19,2))), '') FROM dbo.{table} ORDER BY id"
        ),
    );
    assert_eq!(delivered(&rig), want);
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
        Refused::by_code("RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5),
    );
}

/// A MONEY cursor column advances the incremental run by its value: the second run delivers only the row past the stored cursor.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_money_cursor_column_advances_an_incremental_run() {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_money_cursor");
    mssql_exec(&format!(
        "CREATE TABLE dbo.{table} (id INT PRIMARY KEY, v MONEY NOT NULL);\n\
         INSERT INTO dbo.{table} VALUES (1, 1.5), (2, 3.5), (3, 2.25)"
    ));
    let _guard = MssqlTable::adopt(table.clone());
    let rig = Rig::mssql_batch(&table)
        .mode("incremental")
        .export_line("cursor_column: v");
    rig.run_ok();
    let first = delivered(&rig);
    assert_eq!(first.len(), 3);
    let cursor = StateDb::next_to_config(&rig.config_path()).cursor_value(rig.export_name());
    println!("money cursor after run 1: {cursor:?}");
    assert_eq!(
        cursor.as_deref().and_then(|c| c.parse::<f64>().ok()),
        Some(3.5),
        "the stored cursor is the largest value"
    );
    mssql_exec(&format!(
        "INSERT INTO dbo.{table} (id, v) VALUES (10, CAST('4.75' AS MONEY)), (11, CAST('5.5' AS MONEY))"
    ));
    rig.run_ok();
    let all = delivered(&rig);
    let mut seen: Vec<&str> = all
        .iter()
        .map(String::as_str)
        .filter(|t| !t.is_empty())
        .collect();
    seen.sort_unstable();
    println!("money rows delivered over both runs: {seen:?}");
    assert_eq!(seen, ["1.5000", "2.2500", "3.5000", "4.7500", "5.5000"]);
}

/// A sql_variant holding MONEY is delivered as the text the server renders it, whatever the money decode.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_sql_variant_holding_money_is_delivered_as_the_servers_text() {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_money_variant");
    mssql_exec(&format!(
        "CREATE TABLE dbo.{table} (id INT PRIMARY KEY, v SQL_VARIANT NULL);\n\
         INSERT INTO dbo.{table} VALUES (1, CAST('123.4567' AS MONEY)), (2, CAST('922337203685477.5807' AS MONEY)), (3, CAST('-0.0001' AS SMALLMONEY)), (4, NULL)"
    ));
    let _guard = MssqlTable::adopt(table.clone());
    let rig = Rig::mssql_batch(&table).no_oracle(
        "a sql_variant is graded here by the server's own CONVERT text, which the rig oracle does not render alike",
    );
    rig.run_ok();
    let got: std::collections::BTreeSet<String> = dir_parquet_distinct_strings(&rig.out_dir(), "v")
        .into_iter()
        .filter(|t| !t.is_empty())
        .collect();
    println!("variant money text: {got:?}");
    let want: std::collections::BTreeSet<String> = sqlcmd_lines(
        "rivet-mssql-1",
        &format!("SELECT ISNULL(CONVERT(varchar(40), CONVERT(money, v), 2), '') FROM dbo.{table} ORDER BY id"),
    )
    .into_iter()
    .filter(|t| !t.is_empty())
    .collect();
    assert_eq!(got, want);
}
