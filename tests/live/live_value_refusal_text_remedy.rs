//! A value the native column cannot hold is refused by code, and the refusal's remedy delivers: the column declared `text` is the server's own text, read back through a second client.

use crate::common::*;
use crate::live_int_to_float_override::readback_only;

const UNREPRESENTABLE: &str = "RIVET_SOURCE_VALUE_UNREPRESENTABLE";
const PG_REMEDY: &str = "declare `v: text` under `columns:`";

struct Case {
    label: &'static str,
    ty: &'static str,
    held: &'static str,
    ordinary: &'static str,
    code: &'static str,
    exit: i32,
    remedy: &'static str,
}

const POSTGRES: &[Case] = &[
    Case {
        label: "timestamp infinity",
        ty: "TIMESTAMP",
        held: "'infinity'",
        ordinary: "'2024-03-15 10:11:12.123456'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "timestamp -infinity",
        ty: "TIMESTAMP",
        held: "'-infinity'",
        ordinary: "'2024-03-15 10:11:12.123456'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "timestamptz infinity",
        ty: "TIMESTAMPTZ",
        held: "'infinity'",
        ordinary: "'2024-03-15 10:11:12.123456+00'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "timestamptz -infinity",
        ty: "TIMESTAMPTZ",
        held: "'-infinity'",
        ordinary: "'2024-03-15 10:11:12.123456+00'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "date infinity",
        ty: "DATE",
        held: "'infinity'",
        ordinary: "'2024-03-15'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "date -infinity",
        ty: "DATE",
        held: "'-infinity'",
        ordinary: "'2024-03-15'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "date 5874897-12-31",
        ty: "DATE",
        held: "'5874897-12-31'",
        ordinary: "'2024-03-15'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "time 24:00:00",
        ty: "TIME",
        held: "'24:00:00'",
        ordinary: "'14:30:00.123456'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "integer[] 2-D",
        ty: "INTEGER[]",
        held: "'{{1,2},{3,4}}'",
        ordinary: "'{1,2,3}'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "integer[] 3-D",
        ty: "INTEGER[]",
        held: "'{{{1},{2}},{{3},{4}}}'",
        ordinary: "'{1,2,3}'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "text[] 2-D",
        ty: "TEXT[]",
        held: "'{{a,b},{c,d}}'",
        ordinary: "'{a,b}'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "date[] infinity element",
        ty: "DATE[]",
        held: "'{2024-01-01,infinity}'",
        ordinary: "'{2024-01-01}'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "timestamp[] infinity element",
        ty: "TIMESTAMP[]",
        held: "'{2024-01-01 00:00:00,-infinity}'",
        ordinary: "'{2024-01-01 00:00:00}'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
    Case {
        label: "time[] 24:00:00 element",
        ty: "TIME[]",
        held: "'{12:00:00,24:00:00}'",
        ordinary: "'{12:00:00}'",
        code: UNREPRESENTABLE,
        exit: 5,
        remedy: PG_REMEDY,
    },
];

/// A table holding the case's three rows (held, ordinary, NULL), dropped with its guard.
fn case_table(case: &Case) -> (String, PgTable) {
    let rows = [(1, case.held), (2, case.ordinary), (3, "NULL")];
    let table = unique_name("vrt_x");
    let ddl = format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, v {} NULL)",
        case.ty
    );
    let values = rows
        .iter()
        .map(|(id, lit)| format!("({id}, {lit})"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut c = pg_connect();
    c.batch_execute(&ddl).unwrap();
    let guard = PgTable::adopt(table.clone());
    c.batch_execute(&format!("INSERT INTO {table} (id, v) VALUES {values}"))
        .unwrap();
    (table, guard)
}

/// The server's own text of every row, by id, read through a second client.
fn held_as_text(table: &str) -> Vec<Option<String>> {
    let mut oracle = pg_connect();
    oracle.batch_execute("SET TimeZone = 'UTC'").unwrap();
    oracle
        .query(&format!("SELECT v::text FROM {table} ORDER BY id"), &[])
        .unwrap()
        .iter()
        .map(|r| r.get::<_, Option<String>>(0))
        .collect()
}

fn sorted_by_id(rig: &Rig) -> Vec<Option<String>> {
    let ids = dir_parquet_display(&rig.out_dir(), "id");
    let vals = dir_parquet_display(&rig.out_dir(), "v");
    let mut pairs: Vec<(i64, Option<String>)> = ids
        .into_iter()
        .map(|i| i.unwrap().parse().unwrap())
        .zip(vals)
        .collect();
    pairs.sort_by_key(|(i, _)| *i);
    pairs.into_iter().map(|(_, v)| v).collect()
}

fn stop_text(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// The value is refused by code with the override named as the remedy, and that override delivers the server's text for every row.
fn remedy_delivers() {
    require_alive(LiveService::Postgres);
    let mut bad = Vec::new();
    for case in POSTGRES {
        let (table, _guard) = case_table(case);
        let refused = readback_only(Rig::pg_batch(&table)).run();
        let said = stop_text(&refused);
        if refused.status.code() != Some(case.exit) || !said.contains(case.code) {
            bad.push(format!(
                "{}: no override must stop {} exit {}, got {:?}: {said}",
                case.label,
                case.code,
                case.exit,
                refused.status.code()
            ));
            continue;
        }
        if !said.to_lowercase().contains(case.remedy) {
            bad.push(format!(
                "{}: the refusal does not name `{}`: {said}",
                case.label, case.remedy
            ));
            continue;
        }
        let rig = readback_only(Rig::pg_batch(&table)).export_line("columns: {v: text}");
        let out = rig.run();
        if !out.status.success() {
            bad.push(format!(
                "{}: the named remedy is refused (exit {:?}): {}",
                case.label,
                out.status.code(),
                stop_text(&out)
            ));
            continue;
        }
        let got = sorted_by_id(&rig);
        let want = held_as_text(&table);
        if got != want {
            bad.push(format!(
                "{}: delivered {got:?}, server text {want:?}",
                case.label
            ));
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn the_remedy_of_a_value_refusal_delivers_the_server_text_postgres() {
    remedy_delivers();
}

const NEIGHBOUR_TABLE: &str = "id INT PRIMARY KEY, d DATE, t TIME, ts TIMESTAMP, tz TIMESTAMPTZ, ia INTEGER[], ta TEXT[], iv INTERVAL, u UUID";
const NEIGHBOUR_ROW: &str = "(1, '2024-03-15', '14:30:00.123456', '2024-03-15 10:11:12.123456', '2024-03-15 10:11:12.123456+00', '{1,2,3}', '{a,b}', '1 day 02:03:04', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')";
const NATIVE: &[(&str, &str)] = &[
    ("d", "2024-03-15"),
    ("t", "14:30:00.123456"),
    ("ts", "2024-03-15T10:11:12.123456"),
    ("tz", "2024-03-15T10:11:12.123456Z"),
    ("ia", "[1, 2, 3]"),
    ("ta", "[a, b]"),
    ("iv", "P1DT2H3M4S"),
    ("u", "a0eebc999c0b4ef8bb6d6bb9bd380a11"),
];
const READABLE_TEXT_UUID: &str = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
const STILL_REFUSED: &[(&str, &str)] = &[
    ("d", "int64"),
    ("ts", "bool"),
    ("ia", "int64"),
    ("t", "float64"),
    ("tz", "date"),
];

fn neighbour_table() -> (String, PgTable) {
    let table = unique_name("vrt_n");
    let mut c = pg_connect();
    c.batch_execute(&format!("CREATE TABLE {table} ({NEIGHBOUR_TABLE})"))
        .unwrap();
    let guard = PgTable::adopt(table.clone());
    c.batch_execute(&format!("INSERT INTO {table} VALUES {NEIGHBOUR_ROW}"))
        .unwrap();
    (table, guard)
}

fn first_delivered(rig: &Rig, col: &str) -> String {
    dir_parquet_display(&rig.out_dir(), col)
        .remove(0)
        .unwrap_or_else(|| "<null>".into())
}

fn delivered_row(rig: &Rig) -> Vec<(String, String)> {
    NATIVE
        .iter()
        .map(|(c, _)| (c.to_string(), first_delivered(rig, c)))
        .collect()
}

fn pinned(row: &[(&str, &str)]) -> Vec<(String, String)> {
    row.iter()
        .map(|(c, v)| (c.to_string(), v.to_string()))
        .collect()
}

/// What main delivered and refused next to the remedy stays byte-identical.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn the_remedy_leaves_the_neighbouring_postgres_reads_unchanged() {
    require_alive(LiveService::Postgres);
    let (table, _guard) = neighbour_table();
    let rig = readback_only(Rig::pg_batch(&table));
    assert_eq!(rig.run().status.code(), Some(0));
    assert_eq!(delivered_row(&rig), pinned(NATIVE), "no override");
    let rig = readback_only(Rig::pg_batch(&table)).export_line("columns: {iv: text, u: text}");
    assert_eq!(rig.run().status.code(), Some(0));
    let mut want = pinned(NATIVE);
    want[7].1 = READABLE_TEXT_UUID.to_string();
    assert_eq!(delivered_row(&rig), want, "text over interval and uuid");
    for (col, ty) in STILL_REFUSED {
        let rig =
            readback_only(Rig::pg_batch(&table)).export_line(&format!("columns: {{{col}: {ty}}}"));
        let out = rig.run();
        assert_eq!(out.status.code(), Some(1), "{col}: {ty}");
        assert!(
            stop_text(&out).contains("RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH"),
            "{col}: {ty}: {}",
            stop_text(&out)
        );
    }
}
