//! An integer column under a `bool` override is delivered as the same 0/1 flags, or refused by code; any other value is never turned into `true`.

use crate::common::*;
use crate::live_int_to_float_override::readback_only;

const NOT_EXACT: &str = "RIVET_SOURCE_VALUE_UNREPRESENTABLE";

fn mysql_table(ty: &str, values: &[&str]) -> (String, MysqlTable) {
    use mysql::prelude::Queryable;
    let table = unique_name("ovb_my");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, i {ty} NULL)"
    ))
    .unwrap();
    let guard = MysqlTable::adopt(table.clone());
    let rows = values
        .iter()
        .enumerate()
        .map(|(k, v)| format!("({}, {v})", k + 1))
        .collect::<Vec<_>>()
        .join(", ");
    c.query_drop(format!("INSERT INTO {table} (id, i) VALUES {rows}"))
        .unwrap();
    (table, guard)
}

fn mysql_rig(table: &str) -> Rig {
    readback_only(Rig::mysql_batch(table)).export_line("columns: {i: bool}")
}

const INT_TYPES: [&str; 6] = [
    "TINYINT",
    "SMALLINT",
    "MEDIUMINT",
    "INT",
    "BIGINT",
    "BIGINT UNSIGNED",
];

/// 0, 1 and NULL under a `bool` override are delivered as false, true and NULL on every integer type.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn zero_and_one_under_a_bool_override_are_delivered_as_flags_mysql() {
    require_alive(LiveService::Mysql);
    for ty in INT_TYPES {
        let (table, _guard) = mysql_table(ty, &["0", "1", "NULL"]);
        let rig = mysql_rig(&table);
        rig.run_ok();
        assert_eq!(
            dir_parquet_display(&rig.out_dir(), "i"),
            vec![Some("false".into()), Some("true".into()), None],
            "{ty} as bool"
        );
    }
}

/// A value other than 0 or 1 under a `bool` override is refused by code and nothing is written, never `true`.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_integer_that_is_not_a_flag_is_refused_never_turned_into_true_mysql() {
    require_alive(LiveService::Mysql);
    let cases: [(&str, &str); 7] = [
        ("TINYINT", "2"),
        ("TINYINT", "-1"),
        ("SMALLINT", "300"),
        ("INT", "2147483647"),
        ("BIGINT", "-9223372036854775808"),
        ("BIGINT UNSIGNED", "18446744073709551615"),
        ("BIGINT UNSIGNED", "2"),
    ];
    for (ty, value) in cases {
        let (table, _guard) = mysql_table(ty, &["0", "1", value]);
        let rig = mysql_rig(&table);
        let out = rig.run();
        assert_refused(&out, Refused::by_code(NOT_EXACT, 5));
        assert_eq!(
            files_with_extension(&rig.out_dir(), "parquet").len(),
            0,
            "{ty} holding {value}: nothing written"
        );
    }
}

fn mysql_flag_rig(table: &str) -> Rig {
    readback_only(Rig::mysql_batch(table))
}

/// A `TINYINT(1)` and a `BIT(1)` holding 0, 1 and NULL are delivered as false, true and NULL with no override.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_flag_column_holding_zero_one_and_null_is_delivered_as_a_flag_mysql() {
    require_alive(LiveService::Mysql);
    for (ty, values) in [
        ("TINYINT(1)", ["0", "1", "NULL"]),
        ("TINYINT(1) UNSIGNED", ["0", "1", "NULL"]),
        ("BIT(1)", ["b'0'", "b'1'", "NULL"]),
    ] {
        let (table, _guard) = mysql_table(ty, &values);
        let rig = mysql_flag_rig(&table);
        rig.run_ok();
        assert_eq!(
            dir_parquet_display(&rig.out_dir(), "i"),
            vec![Some("false".into()), Some("true".into()), None],
            "{ty}"
        );
    }
}

/// A `TINYINT(1)`, with or without the `bool` override, holding a value outside 0 and 1 is refused by code and nothing is written, never `true`.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_flag_column_outside_zero_and_one_is_refused_never_turned_into_true_mysql() {
    require_alive(LiveService::Mysql);
    let cases: [(&str, &str, bool); 7] = [
        ("TINYINT(1)", "2", false),
        ("TINYINT(1)", "5", false),
        ("TINYINT(1)", "-1", false),
        ("TINYINT(1)", "127", false),
        ("TINYINT(1)", "-128", false),
        ("TINYINT(1)", "5", true),
        ("TINYINT(1) UNSIGNED", "255", false),
    ];
    for (ty, value, overridden) in cases {
        let (table, _guard) = mysql_table(ty, &["0", "1", value]);
        let rig = if overridden {
            mysql_rig(&table)
        } else {
            mysql_flag_rig(&table)
        };
        let out = rig.run();
        assert_refused(&out, Refused::by_code(NOT_EXACT, 5));
        assert_eq!(
            files_with_extension(&rig.out_dir(), "parquet").len(),
            0,
            "{ty} holding {value}: nothing written"
        );
    }
}

/// The remedy the refusal names works: the same `TINYINT(1)` declared `int64` delivers the number.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_refused_flag_column_is_delivered_as_the_number_when_declared_int64_mysql() {
    require_alive(LiveService::Mysql);
    let (table, _guard) = mysql_table("TINYINT(1)", &["0", "5", "-1"]);
    let rig = readback_only(Rig::mysql_batch(&table)).export_line("columns: {i: int64}");
    rig.run_ok();
    let mut got = dir_parquet_display(&rig.out_dir(), "i");
    got.sort();
    assert_eq!(
        got,
        vec![Some("-1".into()), Some("0".into()), Some("5".into())]
    );
}

fn cdc_scenario(ty: &str) -> CdcScenario {
    let sc = CdcScenario::mysql_with(
        "ovb_cdc",
        &format!("id INT PRIMARY KEY, i {ty} NULL"),
        |r, _| readback_only(r).export_line("columns: {i: bool}"),
    );
    sc.rig.run_ok();
    sc
}

/// 0, 1 and NULL under a `bool` override are delivered as false, true and NULL in CDC on every integer type.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn zero_and_one_under_a_bool_override_are_delivered_as_flags_in_cdc_mysql() {
    for ty in INT_TYPES {
        let mut sc = cdc_scenario(ty);
        let t = sc.table.clone();
        sc.sql(&format!(
            "INSERT INTO {t} (id, i) VALUES (1, 0), (2, 1), (3, NULL)"
        ));
        sc.rig.run_ok();
        let mut got = dir_parquet_display(&sc.rig.out_dir(), "i");
        got.sort();
        assert_eq!(
            got,
            vec![None, Some("false".into()), Some("true".into())],
            "{ty} as bool in CDC"
        );
    }
}

/// A value other than 0 or 1 under a `bool` override is refused by code in CDC, never delivered as `true`.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn an_integer_that_is_not_a_flag_is_refused_in_cdc_never_turned_into_true_mysql() {
    let cases: [(&str, &str); 5] = [
        ("TINYINT", "2"),
        ("SMALLINT", "300"),
        ("INT", "2147483647"),
        ("BIGINT", "-9223372036854775808"),
        ("BIGINT UNSIGNED", "18446744073709551615"),
    ];
    for (ty, value) in cases {
        let mut sc = cdc_scenario(ty);
        let t = sc.table.clone();
        sc.sql(&format!(
            "INSERT INTO {t} (id, i) VALUES (1, 0), (2, 1), (3, {value})"
        ));
        let out = sc.rig.run();
        assert_refused(&out, Refused::by_code(NOT_EXACT, 5));
        assert_eq!(
            files_with_extension(&sc.rig.out_dir(), "parquet").len(),
            0,
            "{ty} holding {value}: nothing written"
        );
    }
}

/// A `TINYINT(1)` and a `BIT(1)` in CDC holding 0, 1 and NULL are delivered as false, true and NULL with no override.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_flag_column_holding_zero_one_and_null_is_delivered_in_cdc_mysql() {
    for (ty, values) in [
        ("TINYINT(1)", "(1, 0), (2, 1), (3, NULL)"),
        ("BIT(1)", "(1, b'0'), (2, b'1'), (3, NULL)"),
    ] {
        let mut sc = CdcScenario::mysql_with(
            "ovb_cdc1",
            &format!("id INT PRIMARY KEY, i {ty} NULL"),
            |r, _| readback_only(r),
        );
        sc.rig.run_ok();
        let t = sc.table.clone();
        sc.sql(&format!("INSERT INTO {t} (id, i) VALUES {values}"));
        sc.rig.run_ok();
        let mut got = dir_parquet_display(&sc.rig.out_dir(), "i");
        got.sort();
        assert_eq!(
            got,
            vec![None, Some("false".into()), Some("true".into())],
            "{ty} in CDC"
        );
    }
}

/// A `TINYINT(1)` in CDC, with or without the `bool` override, holding a value outside 0 and 1 is refused by code, never delivered as `true`.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_flag_column_outside_zero_and_one_is_refused_in_cdc_never_turned_into_true_mysql() {
    let cases: [(&str, &str, bool); 4] = [
        ("TINYINT(1)", "2", false),
        ("TINYINT(1)", "-1", false),
        ("TINYINT(1)", "5", true),
        ("TINYINT(1) UNSIGNED", "255", false),
    ];
    for (ty, value, overridden) in cases {
        let mut sc = CdcScenario::mysql_with(
            "ovb_cdc2",
            &format!("id INT PRIMARY KEY, i {ty} NULL"),
            |r, _| {
                let r = readback_only(r);
                if overridden {
                    r.export_line("columns: {i: bool}")
                } else {
                    r
                }
            },
        );
        sc.rig.run_ok();
        let t = sc.table.clone();
        sc.sql(&format!(
            "INSERT INTO {t} (id, i) VALUES (1, 0), (2, 1), (3, {value})"
        ));
        let out = sc.rig.run();
        assert_refused(&out, Refused::by_code(NOT_EXACT, 5));
        assert_eq!(
            files_with_extension(&sc.rig.out_dir(), "parquet").len(),
            0,
            "{ty} holding {value}: nothing written"
        );
    }
}

/// The remedy the CDC refusal names works: the same `TINYINT(1)` declared `int64` delivers the number in a stream.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_refused_flag_column_is_delivered_as_the_number_when_declared_int64_in_cdc_mysql() {
    let mut sc = CdcScenario::mysql_with(
        "ovb_cdc3",
        "id INT PRIMARY KEY, i TINYINT(1) NULL",
        |r, _| readback_only(r).export_line("columns: {i: int64}"),
    );
    sc.rig.run_ok();
    let t = sc.table.clone();
    sc.sql(&format!(
        "INSERT INTO {t} (id, i) VALUES (1, 0), (2, 5), (3, -1)"
    ));
    sc.rig.run_ok();
    let mut got = dir_parquet_display(&sc.rig.out_dir(), "i");
    got.sort();
    assert_eq!(
        got,
        vec![Some("-1".into()), Some("0".into()), Some("5".into())]
    );
}
