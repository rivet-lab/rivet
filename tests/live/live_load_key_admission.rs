//! A keyed load's key and order columns are admitted by the type they land as (docs/operator-contract-matrix.yaml, load_key_columns_admitted_by_landed_type): an incremental cursor that lands as text, and a primary-key column that lands finer-collapsed, are refused by code before the warehouse is touched, and the named remedy delivers the source.

use crate::common::*;

const INEXACT: Refused = Refused::by_code("RIVET_LOAD_KEY_COLUMN_LANDS_INEXACT", 5);

/// The incremental export of `table` into `w`, keyed on `pk`, cursor `cursor`, graded by the second client.
fn incremental(e: SqlEngine, w: &Warehouse, table: &str, cursor: &str, pk: &str) -> Rig {
    let line = format!("cursor_column: {cursor}");
    w.shape(e.staged(e.rig(table), "incremental", &[line.as_str()]), pk)
}

/// `rivet state reset` of the rig's export, which must succeed.
fn reset_state(rig: &Rig) {
    let out = rig.cli(&["state", "reset", "--export", rig.export_name()]);
    assert!(out.status.success(), "state reset: {out:?}");
}

/// The rig's destination emptied and its state reset, so the oracle grades the parts of the remedy alone.
fn rebaseline(rig: &Rig) {
    assert!(rig.empty_fake_gcs_destination() > 0);
    reset_state(rig);
}

/// A bare Oracle `NUMBER` cursor lands as text: the load is refused twice and by `check`, the warehouse is unchanged, and `columns: {VER: int64}` then delivers the source in numeric order across a digit boundary.
#[cfg(feature = "oracle")]
fn oracle_number_cursor_is_refused(kind: WarehouseKind) {
    let e = SqlEngine::Oracle;
    e.alive();
    let Some(mut w) = Warehouse::of(kind, "kadm") else {
        return;
    };
    let (table, _guard) = e.create(
        "kadm",
        "id NUMBER(10) PRIMARY KEY, ver NUMBER NOT NULL, v VARCHAR2(10)",
    );
    w.adopt(&table);
    e.exec(&format!(
        "INSERT INTO {table} (id, ver, v) VALUES (1, 9, 'old1')"
    ));
    e.exec(&format!(
        "INSERT INTO {table} (id, ver, v) VALUES (2, 99, 'old2')"
    ));
    let mut rig = incremental(e, &w, &table, "ver", "ID");
    rig.run_ok();
    let before = w.fingerprint(&rig, &table);

    assert_refused(&rig.cli(&["check"]), INEXACT);
    assert_refused(&rig.cli_env(&["load"], w.load_envs()), INEXACT);
    assert_eq!(
        w.fingerprint(&rig, &table),
        before,
        "a refused load touches no warehouse object"
    );
    let text = rig.refuses_twice_and_walks_out(
        &["load"],
        w.load_envs(),
        INEXACT,
        vec![
            Remedy::new(
                "Declare `VER` under the export's `columns:` with a numeric type",
                Then::DeliversTheSource,
                |rig| {
                    rebaseline(rig);
                    rig.amend_export_lines(&["columns: { VER: int64 }"]);
                    SqlEngine::Oracle.exec(&format!(
                        "UPDATE {table} SET ver = 100, v = 'new2' WHERE id = 2"
                    ));
                    rig.run_ok();
                },
            ),
            Remedy::wrong(
                "declared the column and reloaded without running the export again",
                Then::Refuses(INEXACT),
                |rig| {
                    rig.amend_export_lines(&["columns: { VER: int64 }"]);
                },
            ),
        ],
    );
    assert!(
        text.contains("orders `99` after `100`")
            && text.contains("a `number` in the source that lands as text"),
        "the refusal names the order column and why:\n{text}"
    );
    assert_eq!(
        w.pairs(&rig, &table, "ID", "VER", false, false),
        vec![(1, 9), (2, 100)],
        "the view orders VER as a number: 100 replaces 99"
    );
}

/// An Oracle cursor that lands as a number, or is text in the source too, loads as it did before the admission.
#[cfg(feature = "oracle")]
fn oracle_neighbour_cursor_is_admitted(
    kind: WarehouseKind,
    ddl: &str,
    first: &str,
    next: &str,
    read: &str,
    want: i64,
) {
    let e = SqlEngine::Oracle;
    e.alive();
    let Some(mut w) = Warehouse::of(kind, "kadmn") else {
        return;
    };
    let (table, _guard) = e.create(
        "kadmn",
        &format!("id NUMBER(10) PRIMARY KEY, ver {ddl} NOT NULL, v VARCHAR2(10)"),
    );
    w.adopt(&table);
    e.exec(&format!(
        "INSERT INTO {table} (id, ver, v) VALUES (1, {first}, 'old1')"
    ));
    let rig = incremental(e, &w, &table, "ver", "ID");
    rig.run_ok();
    w.load(&rig);
    e.exec(&format!(
        "UPDATE {table} SET ver = {next}, v = 'new1' WHERE id = 1"
    ));
    rig.run_ok();
    w.load(&rig);
    assert_eq!(
        w.pairs(&rig, &table, "ID", read, false, false),
        vec![(1, want)],
        "{ddl}: the cursor column is admitted and the load delivers the update"
    );
}

/// An MSSQL `datetime2(7)` primary-key column with no declaration lands at nanoseconds, so the load is admitted and keeps both rows that differ below a microsecond; declared as microsecond `timestamp` the load is refused twice and by `check`, the warehouse is unchanged, and `timestamp_ns` then keeps both rows that differ below a microsecond, read back by the second client (the rig oracle reads the source at microseconds).
fn mssql_sub_microsecond_key(kind: WarehouseKind, declared: bool, via_query: bool) {
    let e = SqlEngine::Mssql;
    e.alive();
    let Some(mut w) = Warehouse::of(kind, "kadmk") else {
        return;
    };
    let (table, _guard) = e.create(
        "kadmk",
        "dev INT NOT NULL, ts DATETIME2(7) NOT NULL, seq INT NOT NULL",
    );
    w.adopt(&table);
    e.exec(&format!(
        "INSERT INTO {table} (dev, ts, seq) VALUES (1, '2024-01-01 00:00:00.1234561', 1)"
    ));
    let source = if via_query {
        e.rig(&table)
            .query(&format!("SELECT dev, ts, seq FROM {table}"))
    } else {
        e.rig(&table)
    };
    let lines: &[&str] = if declared {
        &["cursor_column: seq", "columns: { ts: timestamp }"]
    } else {
        &["cursor_column: seq"]
    };
    let mut rig = w
        .shape(e.staged(source, "incremental", lines), "dev, ts")
        .no_oracle(
            "the datetime2(7) rows differ below a microsecond, which the oracle reads the source at; the view is read back by the second client",
        );
    rig.run_ok();
    e.exec(&format!(
        "INSERT INTO {table} (dev, ts, seq) VALUES (1, '2024-01-01 00:00:00.1234569', 2)"
    ));
    rig.run_ok();
    if !declared {
        assert!(rig.cli(&["check"]).status.success(), "check admits the key");
        let load = rig.cli_env(&["load"], w.load_envs());
        assert!(load.status.success(), "the load: {load:?}");
        assert_eq!(
            w.pairs(&rig, &table, "seq", "dev", false, false),
            vec![(1, 1), (2, 1)],
            "both rows that differ below a microsecond are in the view"
        );
        return;
    }
    let before = w.fingerprint(&rig, &table);

    assert_refused(&rig.cli(&["check"]), INEXACT);
    let refused = rig.cli_env(&["load"], w.load_envs());
    assert_refused(&refused, INEXACT);
    assert_refused(&rig.cli_env(&["load"], w.load_envs()), INEXACT);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&refused.stdout),
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(
        text.contains("primary-key column `ts` is a `datetime2`")
            && text.contains("as `timestamp_ns`"),
        "the refusal names the key column, its source type and the remedy:\n{text}"
    );
    assert_eq!(
        w.fingerprint(&rig, &table),
        before,
        "a refused load touches no warehouse object"
    );

    rebaseline(&rig);
    rig.replace_export_line("columns:", "columns: { ts: timestamp_ns }");
    let run = rig.cli(&["run"]);
    assert!(run.status.success(), "the remedy's run: {run:?}");
    let load = rig.cli_env(&["load"], w.load_envs());
    assert!(load.status.success(), "the remedy's load: {load:?}");
    assert_eq!(
        w.pairs(&rig, &table, "seq", "dev", false, false),
        vec![(1, 1), (2, 1)],
        "both rows that differ below a microsecond are in the view"
    );
}

/// An MSSQL CDC export keyed on a `datetime2(7)` column lands at nanoseconds and is admitted by `check` and `load`; declared as microsecond `timestamp`, `check` refuses it by the code `load` refuses it by, and the load refuses twice.
fn mssql_cdc_sub_microsecond_key_check_agrees_with_load(kind: WarehouseKind, declared: bool) {
    let Some(mut w) = Warehouse::of(kind, "kadmc") else {
        return;
    };
    let mut scn = CdcScenario::mssql_with(
        "kadmc",
        "dev INT NOT NULL, ts DATETIME2(7) NOT NULL, seq INT NOT NULL, PRIMARY KEY (dev, ts)",
        |rig, _| {
            let rig = w.shape(rig.cdc("initial: snapshot"), "dev, ts");
            if declared {
                rig.export_line("columns: { ts: timestamp }").no_oracle(
                    "the declared microsecond landing differs from the datetime2(7) source below a microsecond, which the oracle reads the source at",
                )
            } else {
                rig
            }
        },
    );
    let table = scn.table.clone();
    w.adopt(&table);
    scn.sql(&format!(
        "INSERT INTO dbo.{table} (dev, ts, seq) VALUES (1, '2024-01-01 00:00:00.1234561', 1)"
    ));
    scn.settle();
    scn.rig.run_ok();
    if declared {
        assert_refused(&scn.rig.cli_env(&["load"], w.load_envs()), INEXACT);
        assert_refused(&scn.rig.cli(&["check"]), INEXACT);
        return;
    }
    assert!(
        scn.rig.cli(&["check"]).status.success(),
        "check admits the key"
    );
    let load = scn.rig.cli_env(&["load"], w.load_envs());
    assert!(load.status.success(), "the load: {load:?}");
}

/// An Oracle `TIMESTAMP(9)` primary-key column declared as microsecond `timestamp`: the load is refused twice and by `check`, the warehouse is unchanged, and `timestamp_ns` then keeps both rows that differ below a microsecond.
#[cfg(feature = "oracle")]
fn oracle_sub_microsecond_key_is_refused(kind: WarehouseKind, via_query: bool) {
    let e = SqlEngine::Oracle;
    e.alive();
    let Some(mut w) = Warehouse::of(kind, "kadmo") else {
        return;
    };
    let (table, _guard) = e.create(
        "kadmo",
        "dev NUMBER(10) NOT NULL, ts TIMESTAMP(9) NOT NULL, seq NUMBER(10) NOT NULL",
    );
    w.adopt(&table);
    e.exec(&format!(
        "INSERT INTO {table} (dev, ts, seq) VALUES (1, TIMESTAMP '2024-01-01 00:00:00.123456100', 1)"
    ));
    let source = if via_query {
        e.rig(&table)
            .query(&format!("SELECT dev, ts, seq FROM {table}"))
    } else {
        e.rig(&table)
    };
    let mut rig = w
        .shape(
            e.staged(
                source,
                "incremental",
                &["cursor_column: seq", "columns: { TS: timestamp }"],
            ),
            "DEV, TS",
        )
        .no_oracle(
            "the TIMESTAMP(9) rows differ below a microsecond, which the oracle reads the delivered column at; the view is read back by the second client",
        );
    rig.run_ok();
    e.exec(&format!(
        "INSERT INTO {table} (dev, ts, seq) VALUES (1, TIMESTAMP '2024-01-01 00:00:00.123456900', 2)"
    ));
    rig.run_ok();
    let before = w.fingerprint(&rig, &table);

    assert_refused(&rig.cli(&["check"]), INEXACT);
    let refused = rig.cli_env(&["load"], w.load_envs());
    assert_refused(&refused, INEXACT);
    assert_refused(&rig.cli_env(&["load"], w.load_envs()), INEXACT);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&refused.stdout),
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(
        text.contains("primary-key column `TS` is a `timestamp(9)`")
            && text.contains("as `timestamp_ns`"),
        "the refusal names the key column, its source type and the remedy:\n{text}"
    );
    assert_eq!(
        w.fingerprint(&rig, &table),
        before,
        "a refused load touches no warehouse object"
    );

    rebaseline(&rig);
    rig.replace_export_line("columns:", "columns: { TS: timestamp_ns }");
    let run = rig.cli(&["run"]);
    assert!(run.status.success(), "the remedy's run: {run:?}");
    let load = rig.cli_env(&["load"], w.load_envs());
    assert!(load.status.success(), "the remedy's load: {load:?}");
    assert_eq!(
        w.pairs(&rig, &table, "SEQ", "DEV", false, false),
        vec![(1, 1), (2, 1)],
        "both rows that differ below a microsecond are in the view"
    );
}

/// An MSSQL primary-key column at or under a microsecond loads as it did before the admission, one view row per source row.
fn mssql_exact_key_is_admitted(kind: WarehouseKind) {
    let e = SqlEngine::Mssql;
    e.alive();
    let Some(mut w) = Warehouse::of(kind, "kadmx") else {
        return;
    };
    let (table, _guard) = e.create(
        "kadmx",
        "dev INT NOT NULL, ts DATETIME2(6) NOT NULL, seq INT NOT NULL",
    );
    w.adopt(&table);
    e.exec(&format!(
        "INSERT INTO {table} (dev, ts, seq) VALUES (1, '2024-01-01 00:00:00.123456', 1)"
    ));
    let rig = incremental(e, &w, &table, "seq", "dev, ts");
    rig.run_ok();
    w.load(&rig);
    e.exec(&format!(
        "INSERT INTO {table} (dev, ts, seq) VALUES (1, '2024-01-01 00:00:00.123457', 2)"
    ));
    rig.run_ok();
    w.load(&rig);
    assert_eq!(
        w.pairs(&rig, &table, "seq", "dev", false, false),
        vec![(1, 1), (2, 1)],
        "a microsecond key is admitted and both rows are delivered"
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle + clickhouse + fake-gcs"]
fn an_oracle_number_cursor_is_refused_into_clickhouse() {
    oracle_number_cursor_is_refused(WarehouseKind::ClickHouse);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle + clickhouse + fake-gcs"]
fn an_oracle_integer_cursor_is_admitted_into_clickhouse() {
    oracle_neighbour_cursor_is_admitted(
        WarehouseKind::ClickHouse,
        "NUMBER(10,0)",
        "9",
        "100",
        "VER",
        100,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle + clickhouse + fake-gcs"]
fn an_oracle_text_cursor_is_admitted_into_clickhouse() {
    oracle_neighbour_cursor_is_admitted(
        WarehouseKind::ClickHouse,
        "VARCHAR2(10)",
        "'a1'",
        "'b2'",
        "ID",
        1,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle + clickhouse + fake-gcs"]
fn an_oracle_sub_microsecond_key_is_refused_into_clickhouse() {
    oracle_sub_microsecond_key_is_refused(WarehouseKind::ClickHouse, false);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle + clickhouse + fake-gcs"]
fn an_oracle_sub_microsecond_query_key_is_refused_into_clickhouse() {
    oracle_sub_microsecond_key_is_refused(WarehouseKind::ClickHouse, true);
}

#[test]
#[ignore = "live: requires docker compose mssql + clickhouse + fake-gcs"]
fn an_mssql_sub_microsecond_key_declared_timestamp_is_refused_into_clickhouse() {
    mssql_sub_microsecond_key(WarehouseKind::ClickHouse, true, false);
}

#[test]
#[ignore = "live: requires docker compose mssql + clickhouse + fake-gcs"]
fn an_mssql_sub_microsecond_query_key_declared_timestamp_is_refused_into_clickhouse() {
    mssql_sub_microsecond_key(WarehouseKind::ClickHouse, true, true);
}

#[test]
#[ignore = "live: requires docker compose mssql + clickhouse + fake-gcs"]
fn an_mssql_sub_microsecond_key_lands_at_its_precision_into_clickhouse() {
    mssql_sub_microsecond_key(WarehouseKind::ClickHouse, false, false);
}

#[test]
#[ignore = "live: requires docker compose mssql + clickhouse + fake-gcs"]
fn an_mssql_sub_microsecond_query_key_lands_at_its_precision_into_clickhouse() {
    mssql_sub_microsecond_key(WarehouseKind::ClickHouse, false, true);
}

#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mssql-cdc with SQL Server Agent"]
fn an_mssql_cdc_sub_microsecond_key_declared_timestamp_check_agrees_with_load_into_clickhouse() {
    mssql_cdc_sub_microsecond_key_check_agrees_with_load(WarehouseKind::ClickHouse, true);
}

#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mssql-cdc with SQL Server Agent"]
fn an_mssql_cdc_sub_microsecond_key_is_admitted_at_its_precision_into_clickhouse() {
    mssql_cdc_sub_microsecond_key_check_agrees_with_load(WarehouseKind::ClickHouse, false);
}

#[test]
#[ignore = "live: requires docker compose mssql + clickhouse + fake-gcs"]
fn an_mssql_microsecond_key_is_admitted_into_clickhouse() {
    mssql_exact_key_is_admitted(WarehouseKind::ClickHouse);
}
