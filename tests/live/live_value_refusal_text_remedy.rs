//! A value the native column cannot hold is refused by code, and the refusal's remedy delivers: the column declared `text` is the server's own text, read back through a second client.

use crate::common::*;
use crate::live_int_to_float_override::readback_only;

const UNREPRESENTABLE: &str = "RIVET_SOURCE_VALUE_UNREPRESENTABLE";
const PG_REMEDY: &str = "declare `v: text` under `columns:`";

#[derive(Clone, Copy, Debug)]
enum Eng {
    Postgres,
    Mssql,
}

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

const MSSQL: &[Case] = &[Case {
    label: "xml",
    ty: "XML",
    held: "N'<a>x</a>'",
    ordinary: "N'<b/>'",
    code: "RIVET_TYPE_UNSAFE_MAPPING",
    exit: 2,
    remedy: "`columns:`",
}];

impl Eng {
    fn cases(self) -> &'static [Case] {
        match self {
            Eng::Postgres => POSTGRES,
            Eng::Mssql => MSSQL,
        }
    }

    fn live(self) -> LiveService {
        match self {
            Eng::Postgres => LiveService::Postgres,
            Eng::Mssql => LiveService::Mssql,
        }
    }

    fn rig(self, table: &str) -> Rig {
        match self {
            Eng::Postgres => Rig::pg_batch(table),
            Eng::Mssql => Rig::mssql_batch(table),
        }
    }

    fn table(self, case: &Case) -> (String, Box<dyn std::any::Any>) {
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
        let dml = format!("INSERT INTO {table} (id, v) VALUES {values}");
        let guard: Box<dyn std::any::Any> = match self {
            Eng::Postgres => {
                let mut c = pg_connect();
                c.batch_execute(&ddl).unwrap();
                let g = PgTable::adopt(table.clone());
                c.batch_execute(&dml).unwrap();
                Box::new(g)
            }
            _ => {
                mssql_exec(&ddl);
                let g = MssqlTable::adopt(table.clone());
                mssql_exec(&dml);
                Box::new(g)
            }
        };
        (table, guard)
    }

    /// The server's own text of every row, by id, read through the engine's own client.
    fn server_text(self, table: &str) -> Vec<Option<String>> {
        match self {
            Eng::Postgres => pg_connect()
                .query(&format!("SELECT v::text FROM {table} ORDER BY id"), &[])
                .unwrap()
                .iter()
                .map(|r| r.get::<_, Option<String>>(0))
                .collect(),
            Eng::Mssql => mssql_query_strings(&format!(
                "SELECT ISNULL(CAST(v AS NVARCHAR(MAX)), N'<null>') FROM {table} ORDER BY id"
            ))
            .into_iter()
            .map(|t| (t != "<null>").then_some(t))
            .collect(),
        }
    }
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
fn remedy_delivers(eng: Eng) {
    require_alive(eng.live());
    let mut bad = Vec::new();
    for case in eng.cases() {
        let (table, _guard) = eng.table(case);
        let refused = readback_only(eng.rig(&table)).run();
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
        let rig = readback_only(eng.rig(&table)).export_line("columns: {v: text}");
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
        let want = eng.server_text(&table);
        if got != want {
            bad.push(format!(
                "{}: delivered {got:?}, server text {want:?}",
                case.label
            ));
        }
    }
    assert!(bad.is_empty(), "{eng:?}:\n{}", bad.join("\n"));
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn the_remedy_of_a_value_refusal_delivers_the_server_text_postgres() {
    remedy_delivers(Eng::Postgres);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn the_remedy_of_a_value_refusal_delivers_the_server_text_mssql() {
    remedy_delivers(Eng::Mssql);
}
