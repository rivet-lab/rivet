//! A temporal value a warehouse cannot hold is refused by `RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE`, and the remedy that refusal names, `columns: {c: text}`, delivers the source's value as text: graded by the values a second client reads back from the warehouse against what the source server renders itself.

use crate::common::*;

const OUT_OF_RANGE: Refused = Refused::by_code("RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE", 5);
const UNGRADED: &str = "the subject is the text a declared override delivers, graded against the source server's own text";

const CH: &[WarehouseKind] = &[WarehouseKind::ClickHouse];
const CH_BQ: &[WarehouseKind] = &[WarehouseKind::ClickHouse, WarehouseKind::BigQuery];

/// One temporal family: its column type, the values a warehouse cannot hold (with the warehouses that cannot), one ordinary value, and how the source renders the column as text.
struct Family {
    ty: &'static str,
    held: &'static [(&'static str, &'static [WarehouseKind])],
    ordinary: &'static str,
    text_of: fn(&str) -> String,
    sep: char,
}

fn cast_char(c: &str) -> String {
    format!("CAST({c} AS CHAR)")
}

#[cfg_attr(not(feature = "oracle"), allow(dead_code))]
fn oracle_date(c: &str) -> String {
    format!("TO_CHAR({c}, 'YYYY-MM-DD HH24:MI:SS')")
}

#[cfg_attr(not(feature = "oracle"), allow(dead_code))]
fn oracle_ts6(c: &str) -> String {
    format!("TO_CHAR({c}, 'YYYY-MM-DD HH24:MI:SS.FF6')")
}

const MYSQL_DATE: Family = Family {
    ty: "DATE",
    held: &[
        ("'1000-01-01'", CH),
        ("'1899-12-31'", CH),
        ("'2300-01-01'", CH),
        ("'9999-12-31'", CH),
        ("'0000-01-01'", CH_BQ),
    ],
    ordinary: "'2024-02-29'",
    text_of: cast_char,
    sep: ' ',
};

const MYSQL_DATETIME6: Family = Family {
    ty: "DATETIME(6)",
    held: &[
        ("'1000-01-01 00:00:00.000001'", CH),
        ("'1677-09-21 00:12:43.145224'", CH),
        ("'9999-12-31 23:59:59.999999'", CH),
    ],
    ordinary: "'2024-02-29 12:34:56.789012'",
    text_of: cast_char,
    sep: ' ',
};

const MYSQL_DATETIME3: Family = Family {
    ty: "DATETIME(3)",
    held: &[
        ("'1000-01-01 00:00:00.001'", CH),
        ("'9999-12-31 23:59:59.999'", CH),
    ],
    ordinary: "'2024-02-29 12:34:56.789'",
    text_of: cast_char,
    sep: ' ',
};

#[cfg_attr(not(feature = "oracle"), allow(dead_code))]
const ORACLE_DATE: Family = Family {
    ty: "DATE",
    held: &[
        ("DATE '0001-01-01'", CH),
        ("DATE '9999-12-31'", CH),
        ("TO_DATE('1677-09-21 00:12:43','YYYY-MM-DD HH24:MI:SS')", CH),
    ],
    ordinary: "DATE '2024-02-29'",
    text_of: oracle_date,
    sep: 'T',
};

#[cfg_attr(not(feature = "oracle"), allow(dead_code))]
const ORACLE_TIMESTAMP6: Family = Family {
    ty: "TIMESTAMP(6)",
    held: &[
        ("TIMESTAMP '0001-01-01 00:00:00.000000'", CH),
        ("TIMESTAMP '9999-12-31 23:59:59.999999'", CH),
    ],
    ordinary: "TIMESTAMP '2024-02-29 12:34:56.789012'",
    text_of: oracle_ts6,
    sep: 'T',
};

/// The server's text in the form rivet delivers a declared-text temporal: the separator `sep`, a time of day and six fraction digits.
fn delivered_form(server: &str, sep: char) -> String {
    let (date, time) = server
        .split_once([' ', 'T'])
        .unwrap_or((server, "00:00:00"));
    let (clock, frac) = time.split_once('.').unwrap_or((time, ""));
    format!("{date}{sep}{clock}.{frac:0<6}")
}

/// A table `(id, v)` of `family` holding every value `kind` cannot hold, an ordinary one and NULL: the load is refused by code with the `columns:` remedy named, and the export that declares `v: text` lands the source's own text, which the source server and the warehouse's second client agree on.
fn text_remedy_lands(e: SqlEngine, kind: WarehouseKind, family: &Family) {
    e.alive();
    let held: Vec<&str> = family
        .held
        .iter()
        .filter(|(_, whs)| whs.contains(&kind))
        .map(|(lit, _)| *lit)
        .collect();
    assert!(
        !held.is_empty(),
        "{kind:?} has no held value in {}",
        family.ty
    );
    let Some(mut w) = Warehouse::of(kind, "txr") else {
        return;
    };
    let (id, v) = (e.col("id"), e.col("v"));
    let (table, _guard) = e.create(
        "txr",
        &format!("{id} {} PRIMARY KEY, {v} {}", e.int64(), family.ty),
    );
    w.adopt(&table);
    let mut rows: Vec<String> = held.iter().map(|lit| lit.to_string()).collect();
    rows.push(family.ordinary.to_string());
    rows.push("NULL".to_string());
    let values: Vec<String> = rows
        .iter()
        .enumerate()
        .map(|(i, lit)| format!("({}, {lit})", i + 1))
        .collect();
    e.exec(&format!(
        "INSERT INTO {table} ({id}, {v}) VALUES {}",
        values.join(", ")
    ));
    let server = e.id_texts(&table, &(family.text_of)(&v));
    let delivered: Vec<(i64, Option<String>)> = server
        .iter()
        .map(|(k, t)| (*k, t.as_deref().map(|t| delivered_form(t, family.sep))))
        .collect();
    assert_eq!(server.len(), rows.len(), "the source holds every row");

    let plain = w.shape(e.staged(e.rig(&table), "full", &[]), "id");
    let plain = plain.no_oracle(UNGRADED);
    plain.run_ok();
    let out = plain.cli_env(&["load"], w.load_envs());
    assert_refused(&out, OUT_OF_RANGE);
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(
        said.contains("declare `v: text` under `columns:`"),
        "the refusal names the remedy:\n{said}"
    );

    let declared = w
        .shape(
            e.staged(e.rig(&table), "full", &["columns: {v: text}"]),
            "id",
        )
        .no_oracle(UNGRADED);
    declared.run_ok();
    w.load(&declared);
    let (landed, landed_type) = w.texts(&table, "id", "v");
    assert!(
        landed_type.to_lowercase().contains("string")
            || landed_type.to_lowercase().contains("text")
            || landed_type.to_lowercase().contains("varchar"),
        "the declared column lands as text, not {landed_type}"
    );
    assert_eq!(
        landed, delivered,
        "the warehouse holds the source's value as text for every row, in {landed_type}"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + clickhouse + fake-gcs"]
fn text_remedy_lands_in_clickhouse_mysql_date() {
    text_remedy_lands(SqlEngine::Mysql, WarehouseKind::ClickHouse, &MYSQL_DATE);
}

#[test]
#[ignore = "live: requires docker compose mysql + clickhouse + fake-gcs"]
fn text_remedy_lands_in_clickhouse_mysql_datetime6() {
    text_remedy_lands(
        SqlEngine::Mysql,
        WarehouseKind::ClickHouse,
        &MYSQL_DATETIME6,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + clickhouse + fake-gcs"]
fn text_remedy_lands_in_clickhouse_mysql_datetime3() {
    text_remedy_lands(
        SqlEngine::Mysql,
        WarehouseKind::ClickHouse,
        &MYSQL_DATETIME3,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + BigQuery creds"]
fn text_remedy_lands_in_bigquery_mysql_date() {
    text_remedy_lands(SqlEngine::Mysql, WarehouseKind::BigQuery, &MYSQL_DATE);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle + clickhouse + fake-gcs"]
fn text_remedy_lands_in_clickhouse_oracle_date() {
    text_remedy_lands(SqlEngine::Oracle, WarehouseKind::ClickHouse, &ORACLE_DATE);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle + clickhouse + fake-gcs"]
fn text_remedy_lands_in_clickhouse_oracle_timestamp6() {
    text_remedy_lands(
        SqlEngine::Oracle,
        WarehouseKind::ClickHouse,
        &ORACLE_TIMESTAMP6,
    );
}
