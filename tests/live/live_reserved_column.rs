//! A source column named like one rivet owns in a delta load's tables (docs/operator-contract-matrix.yaml): the load of that export is refused by code on every attempt, before the warehouse is touched, and `mode: full` loads the same source.

use crate::common::*;

const RESERVED: Refused = Refused::by_code("RIVET_LOAD_RESERVED_COLUMN", 1);
const UNGRADED: &str = "the rig oracle reads a warehouse table that has `__is_deleted` as a change log and drops that column, so a source column of the name reads as absent; the cell reads the rows through the second client";

/// An incremental export of `e` whose source has a column called `column`: its run succeeds, its load is refused twice with the warehouse untouched, and `mode: full` then delivers it.
fn reserved_column_is_refused(e: SqlEngine, kind: WarehouseKind, column: &str) {
    e.alive();
    let Some(mut w) = Warehouse::of(kind, "rsv") else {
        return;
    };
    let (ty, flags) = match e {
        SqlEngine::Pg => ("BOOLEAN", ["false", "true"]),
        SqlEngine::Mysql => ("BOOLEAN", ["0", "1"]),
        _ => ("BIT", ["0", "1"]),
    };
    let (table, _guard) = e.create(
        "rsv",
        &format!("id BIGINT PRIMARY KEY, {column} {ty}, v INT"),
    );
    w.adopt(&table);
    e.exec(&format!(
        "INSERT INTO {table} (id, {column}, v) VALUES (1, {}, 10), (2, {}, 20)",
        flags[0], flags[1]
    ));
    let rig = reserved_rig(&w, e, &table, false);
    rig.run_ok();
    let before = w.fingerprint(&rig, &table);

    for attempt in 1..=2 {
        let out = rig.cli_env(&["load"], w.load_envs());
        assert_refused(&out, RESERVED);
        let said = String::from_utf8_lossy(&out.stderr);
        assert!(
            said.contains(&format!("`{column}`"))
                && said.contains("rivet OWNS")
                && said.contains("load this table with `mode: full`"),
            "attempt {attempt}: the refusal names the column and the remedy:\n{said}"
        );
    }
    assert_eq!(
        w.fingerprint(&rig, &table),
        before,
        "a refused load touches no warehouse object"
    );

    let rig = rig.restage("full", &[]);
    rig.run_ok();
    w.load(&rig);
    assert_eq!(
        w.pairs(&rig, &table, "id", "v", false, false),
        vec![(1, 10), (2, 20)],
        "`mode: full` delivers the rows the incremental load refused"
    );
}

/// The incremental rig of `table` into `w`, keyed on `id`, with the oracle off (`UNGRADED`); a base-and-buffer layout when `base` is set.
fn reserved_rig(w: &Warehouse, e: SqlEngine, table: &str, base: bool) -> Rig {
    let rig = e.staged(e.rig(table), "incremental", &["cursor_column: id"]);
    let rig = if base {
        w.shape_base_buffer(rig, "id")
    } else {
        w.shape(rig, "id")
    };
    rig.no_oracle(UNGRADED)
}

/// A source gains a column called `column` after its first good load: the next `rivet compact` is refused by code and the warehouse is unchanged.
fn reserved_column_is_refused_by_compact(e: SqlEngine, kind: WarehouseKind, column: &str) {
    e.alive();
    let Some(mut w) = Warehouse::of(kind, "rsvc") else {
        return;
    };
    let (table, _guard) = e.create("rsvc", "id BIGINT PRIMARY KEY, v INT");
    w.adopt(&table);
    e.exec(&format!(
        "INSERT INTO {table} (id, v) VALUES (1, 10), (2, 20)"
    ));
    let rig = reserved_rig(&w, e, &table, true);
    rig.run_ok();
    w.load(&rig);
    let (add, ty, flag) = match e {
        SqlEngine::Pg => ("ADD COLUMN", "BOOLEAN", "true"),
        SqlEngine::Mysql => ("ADD COLUMN", "BOOLEAN", "1"),
        _ => ("ADD", "BIT", "1"),
    };
    e.exec(&format!("ALTER TABLE {table} {add} {column} {ty}"));
    e.exec(&format!(
        "INSERT INTO {table} (id, v, {column}) VALUES (3, 30, {flag})"
    ));
    rig.run_ok();
    let before = w.fingerprint(&rig, &table);

    let out = rig.cli_env(&["compact"], w.load_envs());
    assert_refused(&out, RESERVED);
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(
        said.contains(&format!("`{column}`")) && said.contains("rivet OWNS"),
        "the refusal names the column:\n{said}"
    );
    assert_eq!(
        w.fingerprint(&rig, &table),
        before,
        "a refused compact touches no warehouse object"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + clickhouse + fake-gcs"]
fn reserved_column_is_refused_into_clickhouse_postgres() {
    reserved_column_is_refused(SqlEngine::Pg, WarehouseKind::ClickHouse, "__is_deleted");
}

#[test]
#[ignore = "live: requires docker compose mysql + clickhouse + fake-gcs"]
fn reserved_column_is_refused_into_clickhouse_mysql() {
    reserved_column_is_refused(SqlEngine::Mysql, WarehouseKind::ClickHouse, "__is_deleted");
}

#[test]
#[ignore = "live: requires docker compose mssql + clickhouse + fake-gcs"]
fn reserved_column_is_refused_into_clickhouse_mssql() {
    reserved_column_is_refused(SqlEngine::Mssql, WarehouseKind::ClickHouse, "__is_deleted");
}

#[test]
#[ignore = "live: requires docker compose postgres + BigQuery creds"]
fn reserved_column_is_refused_into_bigquery_postgres() {
    reserved_column_is_refused(SqlEngine::Pg, WarehouseKind::BigQuery, "__is_deleted");
}

#[test]
#[ignore = "live: requires docker compose mysql + BigQuery creds"]
fn reserved_column_is_refused_into_bigquery_mysql() {
    reserved_column_is_refused(SqlEngine::Mysql, WarehouseKind::BigQuery, "__is_deleted");
}

#[test]
#[ignore = "live: requires docker compose mssql + BigQuery creds"]
fn reserved_column_is_refused_into_bigquery_mssql() {
    reserved_column_is_refused(SqlEngine::Mssql, WarehouseKind::BigQuery, "__is_deleted");
}

#[test]
#[ignore = "live: requires docker compose postgres + Snowflake creds"]
fn reserved_column_is_refused_into_snowflake_postgres() {
    reserved_column_is_refused(SqlEngine::Pg, WarehouseKind::Snowflake, "__is_deleted");
}

#[test]
#[ignore = "live: requires docker compose mysql + Snowflake creds"]
fn reserved_column_is_refused_into_snowflake_mysql() {
    reserved_column_is_refused(SqlEngine::Mysql, WarehouseKind::Snowflake, "__is_deleted");
}

#[test]
#[ignore = "live: requires docker compose mssql + Snowflake creds"]
fn reserved_column_is_refused_into_snowflake_mssql() {
    reserved_column_is_refused(SqlEngine::Mssql, WarehouseKind::Snowflake, "__is_deleted");
}

#[test]
#[ignore = "live: requires docker compose postgres + BigQuery creds"]
fn reserved_column_is_refused_by_compact_into_bigquery_postgres() {
    reserved_column_is_refused_by_compact(SqlEngine::Pg, WarehouseKind::BigQuery, "__is_deleted");
}

#[test]
#[ignore = "live: requires docker compose mysql + BigQuery creds"]
fn reserved_column_is_refused_by_compact_into_bigquery_mysql() {
    reserved_column_is_refused_by_compact(
        SqlEngine::Mysql,
        WarehouseKind::BigQuery,
        "__is_deleted",
    );
}

#[test]
#[ignore = "live: requires docker compose mssql + BigQuery creds"]
fn reserved_column_is_refused_by_compact_into_bigquery_mssql() {
    reserved_column_is_refused_by_compact(
        SqlEngine::Mssql,
        WarehouseKind::BigQuery,
        "__is_deleted",
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + Snowflake creds"]
fn reserved_column_is_refused_by_compact_into_snowflake_postgres() {
    reserved_column_is_refused_by_compact(SqlEngine::Pg, WarehouseKind::Snowflake, "__is_deleted");
}

#[test]
#[ignore = "live: requires docker compose mysql + Snowflake creds"]
fn reserved_column_is_refused_by_compact_into_snowflake_mysql() {
    reserved_column_is_refused_by_compact(
        SqlEngine::Mysql,
        WarehouseKind::Snowflake,
        "__is_deleted",
    );
}

#[test]
#[ignore = "live: requires docker compose mssql + Snowflake creds"]
fn reserved_column_is_refused_by_compact_into_snowflake_mssql() {
    reserved_column_is_refused_by_compact(
        SqlEngine::Mssql,
        WarehouseKind::Snowflake,
        "__is_deleted",
    );
}
