//! A column declared `json` in `columns:` is delivered as the text it holds when that text is JSON, and refused by code (exit 5, nothing written) when it is not.

use crate::common::*;
use crate::live_int_to_float_override::readback_only;

#[derive(Clone, Copy, Debug)]
#[cfg_attr(not(feature = "oracle"), allow(dead_code))]
enum Eng {
    Mysql,
    Postgres,
    Mssql,
    Oracle,
}

#[derive(Clone, Copy)]
enum Src {
    Varchar,
    Blob,
    Geometry,
}

impl Eng {
    fn ty(self, s: Src) -> Option<&'static str> {
        match (self, s) {
            (Eng::Mysql, Src::Varchar) => Some("VARCHAR(64)"),
            (Eng::Mysql, Src::Blob) => Some("BLOB"),
            (Eng::Mysql, Src::Geometry) => Some("GEOMETRY"),
            (Eng::Postgres, Src::Varchar) => Some("VARCHAR(64)"),
            (Eng::Mssql, Src::Varchar) => Some("VARCHAR(64)"),
            (Eng::Oracle, Src::Varchar) => Some("VARCHAR2(64)"),
            _ => None,
        }
    }

    fn literal(self, s: Src, text: &str) -> String {
        match (self, s) {
            (Eng::Mysql, Src::Geometry) => {
                "ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0))')".into()
            }
            (Eng::Mysql, Src::Blob) => format!("X'{}'", hex_of(text)),
            _ => format!("'{}'", text.replace('\'', "''")),
        }
    }

    fn col(self) -> &'static str {
        match self {
            Eng::Oracle => "V",
            _ => "v",
        }
    }

    fn live(self) -> LiveService {
        match self {
            Eng::Mysql => LiveService::Mysql,
            Eng::Postgres => LiveService::Postgres,
            Eng::Mssql => LiveService::Mssql,
            Eng::Oracle => LiveService::Oracle,
        }
    }

    fn rig(self, table: &str) -> Rig {
        match self {
            Eng::Mysql => Rig::mysql_batch(table),
            Eng::Postgres => Rig::pg_batch(table),
            Eng::Mssql => Rig::mssql_batch(table),
            Eng::Oracle => Rig::oracle_batch(table),
        }
    }

    fn table(self, ty: &str, value: &str) -> (String, Box<dyn std::any::Any>) {
        #[cfg(feature = "oracle")]
        if let Eng::Oracle = self {
            let t = OracleTable::create("jot_or", &format!("id INTEGER PRIMARY KEY, v {ty}"));
            ora_exec(&format!(
                "INSERT INTO {} (id, v) VALUES (1, {value})",
                t.name()
            ));
            ora_exec(&format!(
                "INSERT INTO {} (id, v) VALUES (2, NULL)",
                t.name()
            ));
            return (t.name().to_string(), Box::new(t));
        }
        let table = unique_name("jot_x");
        let ddl = format!("CREATE TABLE {table} (id INT PRIMARY KEY, v {ty} NULL)");
        let dml = format!("INSERT INTO {table} (id, v) VALUES (1, {value}), (2, NULL)");
        let guard: Box<dyn std::any::Any> = match self {
            Eng::Mysql => {
                use mysql::prelude::Queryable;
                let mut c = mysql_connect();
                c.query_drop(ddl).unwrap();
                let g = MysqlTable::adopt(table.clone());
                c.query_drop(dml).unwrap();
                Box::new(g)
            }
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
}

fn hex_of(text: &str) -> String {
    text.bytes().map(|b| format!("{b:02X}")).collect()
}

struct Case {
    label: &'static str,
    src: Src,
    text: &'static str,
}

const CASES: &[Case] = &[
    Case {
        label: "varchar word",
        src: Src::Varchar,
        text: "abc",
    },
    Case {
        label: "varchar object",
        src: Src::Varchar,
        text: r#"{"a":1}"#,
    },
    Case {
        label: "varchar string scalar",
        src: Src::Varchar,
        text: r#""x""#,
    },
    Case {
        label: "varchar number",
        src: Src::Varchar,
        text: "12.5",
    },
    Case {
        label: "varchar padded array",
        src: Src::Varchar,
        text: " [1, 2] ",
    },
    Case {
        label: "varchar trailing text",
        src: Src::Varchar,
        text: r#"{"a":1}x"#,
    },
    Case {
        label: "varchar single quoted",
        src: Src::Varchar,
        text: "{'a':1}",
    },
    Case {
        label: "blob control bytes",
        src: Src::Blob,
        text: "\u{1}\u{2}",
    },
    Case {
        label: "blob json bytes",
        src: Src::Blob,
        text: r#"{"a":1}"#,
    },
    Case {
        label: "geometrycollection",
        src: Src::Geometry,
        text: "",
    },
];

const D: &str = "delivered the text";
const U: &str = "refused RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5)";
const N: &str = "DELIVERED non-JSON text, exit 0";
const NA: &str = "-";

/// Pinned outcome per case in `CASES` order: MySQL, PostgreSQL, SQL Server, Oracle.
const EXPECTED: [[&str; 4]; 10] = [
    [U, U, U, U],
    [D, D, D, D],
    [D, D, D, D],
    [D, D, D, D],
    [D, D, D, D],
    [U, U, U, U],
    [U, U, U, U],
    [U, NA, NA, NA],
    [D, NA, NA, NA],
    [U, NA, NA, NA],
];

/// What a run of `case` did, graded against the text inserted: delivered as it was, refused by code with no part written, or a delivery that is not the source.
fn outcome(eng: Eng, case: &Case) -> Option<String> {
    let ty = eng.ty(case.src)?;
    let (table, _guard) = eng.table(ty, &eng.literal(case.src, case.text));
    let rig =
        readback_only(eng.rig(&table)).export_line(&format!("columns: {{{}: json}}", eng.col()));
    let out = rig.run();
    let parts = files_with_extension(&rig.out_dir(), "parquet").len();
    if !out.status.success() {
        let said = String::from_utf8_lossy(&out.stderr);
        let code = said
            .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .find(|w| w.starts_with("RIVET_"))
            .unwrap_or("uncoded");
        let left = if parts > 0 { " parts left" } else { "" };
        return Some(format!(
            "refused {code} (exit {}){left}",
            out.status.code().unwrap_or(-1)
        ));
    }
    let got = dir_parquet_display(&rig.out_dir(), eng.col());
    let held: Vec<&String> = got.iter().flatten().collect();
    if got.len() != 2 || held.len() != 1 {
        return Some(format!("WRONG {got:?}"));
    }
    Some(
        if serde_json::from_str::<serde_json::Value>(held[0]).is_err() {
            N.into()
        } else if held[0].as_str() == case.text {
            D.into()
        } else {
            format!("WRONG {:?}", held[0])
        },
    )
}

fn matrix(eng: Eng) {
    require_alive(eng.live());
    let col = match eng {
        Eng::Mysql => 0,
        Eng::Postgres => 1,
        Eng::Mssql => 2,
        Eng::Oracle => 3,
    };
    assert_eq!(CASES.len(), EXPECTED.len(), "every case has a pin");
    let mut bad = Vec::new();
    for (case, pins) in CASES.iter().zip(EXPECTED) {
        let Some(got) = outcome(eng, case) else {
            continue;
        };
        eprintln!("MATRIX {eng:?} | {} | {got}", case.label);
        if got != pins[col] {
            bad.push(format!("{}: got {got}, pinned {}", case.label, pins[col]));
        }
    }
    assert!(
        bad.is_empty(),
        "{eng:?}: a column declared json must deliver JSON text or refuse by code, as pinned:\n{}",
        bad.join("\n")
    );
}

/// MySQL VARCHAR, BLOB and GEOMETRYCOLLECTION under `json`: JSON text is delivered as written, any other valid UTF-8 text is refused by code and nothing is written.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_json_override_delivers_json_text_or_refuses_by_code_mysql() {
    matrix(Eng::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_json_override_delivers_json_text_or_refuses_by_code_postgres() {
    matrix(Eng::Postgres);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_json_override_delivers_json_text_or_refuses_by_code_mssql() {
    matrix(Eng::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_json_override_delivers_json_text_or_refuses_by_code_oracle() {
    matrix(Eng::Oracle);
}

/// The refusal names the column, the declared type and the text remedy, and that remedy delivers the same value.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn the_remedy_of_the_json_refusal_delivers_the_text_mysql() {
    require_alive(LiveService::Mysql);
    let eng = Eng::Mysql;
    let (table, _guard) = eng.table("VARCHAR(64)", "'abc'");
    let said = {
        let rig = readback_only(eng.rig(&table)).export_line("columns: {v: json}");
        String::from_utf8_lossy(&rig.run().stderr).into_owned()
    };
    for want in [
        "`v`",
        "declared `json`",
        "is not JSON",
        "Declare `v: text` under `columns:`",
    ] {
        assert!(said.contains(want), "the refusal says {want:?}: {said}");
    }
    let rig = readback_only(eng.rig(&table)).export_line("columns: {v: text}");
    rig.run_ok();
    assert_eq!(
        dir_parquet_display(&rig.out_dir(), "v"),
        vec![Some("abc".to_string()), None]
    );
}

const NESTED: usize = 90;

fn nested(depth: usize) -> String {
    format!("{}1{}", "[".repeat(depth), "]".repeat(depth))
}

/// The text the server itself holds in `v` of `table`, by row id.
fn server_json_text(eng: Eng, table: &str) -> Vec<Option<String>> {
    match eng {
        Eng::Postgres => pg_connect()
            .query(&format!("SELECT v::text FROM {table} ORDER BY id"), &[])
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect(),
        _ => {
            use mysql::prelude::Queryable;
            let rows: Vec<mysql::Row> = mysql_connect()
                .query(format!("SELECT CAST(v AS CHAR) FROM {table} ORDER BY id"))
                .unwrap();
            rows.into_iter()
                .map(|mut r| r.take::<Option<String>, _>(0).unwrap())
                .collect()
        }
    }
}

/// A native JSON column, undeclared and declared `json`, is delivered as the server's own text of it, whatever its depth or numbers.
fn native_json(eng: Eng, ty: &str, values: &[String]) {
    require_alive(eng.live());
    for declared in [false, true] {
        let table = unique_name("jot_n");
        let ddl = format!("CREATE TABLE {table} (id INT PRIMARY KEY, v {ty} NULL)");
        let rows = values
            .iter()
            .enumerate()
            .map(|(k, v)| format!("({}, '{}')", k + 1, v.replace('\'', "''")))
            .chain([format!("({}, NULL)", values.len() + 1)])
            .collect::<Vec<_>>()
            .join(", ");
        let _guard: Box<dyn std::any::Any> = match eng {
            Eng::Postgres => {
                let mut c = pg_connect();
                c.batch_execute(&ddl).unwrap();
                let g = PgTable::adopt(table.clone());
                c.batch_execute(&format!("INSERT INTO {table} (id, v) VALUES {rows}"))
                    .unwrap();
                Box::new(g)
            }
            _ => {
                use mysql::prelude::Queryable;
                let mut c = mysql_connect();
                c.query_drop(&ddl).unwrap();
                let g = MysqlTable::adopt(table.clone());
                c.query_drop(format!("INSERT INTO {table} (id, v) VALUES {rows}"))
                    .unwrap();
                Box::new(g)
            }
        };
        let mut rig = readback_only(eng.rig(&table));
        if declared {
            rig = rig.export_line("columns: {v: json}");
        }
        let out = rig.run();
        assert!(
            out.status.success(),
            "{eng:?} {ty} declared={declared}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let mut got = dir_parquet_display(&rig.out_dir(), "v");
        let want = server_json_text(eng, &table);
        let normal = |t: &Option<String>| {
            t.as_ref()
                .and_then(|t| serde_json::from_str::<serde_json::Value>(t).ok())
        };
        got.sort();
        let mut sorted = want.clone();
        sorted.sort();
        assert_eq!(
            got.iter().map(normal).collect::<Vec<_>>(),
            sorted.iter().map(normal).collect::<Vec<_>>(),
            "{eng:?} {ty} declared={declared}: delivered vs the server's text"
        );
        eprintln!(
            "NEIGHBOUR {eng:?} {ty} declared={declared} ok {} rows",
            got.len()
        );
    }
}

/// PostgreSQL json and jsonb columns, undeclared and declared `json`, are delivered as the server holds them, a 200-deep document and a number past f64 included.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_native_json_column_is_delivered_whole_declared_or_not_postgres() {
    let values = [
        r#"{"a": [1, 2.50, null], "b": "xé"}"#.to_string(),
        nested(200),
        "5".to_string(),
    ];
    native_json(Eng::Postgres, "jsonb", &values);
    native_json(Eng::Postgres, "json", &values);
}

/// MySQL JSON columns, undeclared and declared `json`, are delivered as the server holds them.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_native_json_column_is_delivered_whole_declared_or_not_mysql() {
    let values = [
        r#"{"a": [1, 2.50, null], "b": "xé"}"#.to_string(),
        nested(NESTED),
        "5".to_string(),
    ];
    native_json(Eng::Mysql, "JSON", &values);
}

/// A MongoDB export holds no text column a `json` declaration could retype: `_id: json` is refused by its pair, and the `document` column, declared `json`, holds JSON on every row.
#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_json_override_on_a_mongo_export_refuses_a_pair_it_does_not_read_and_delivers_json_mongo() {
    require_alive(LiveService::Mongo);
    let db = unique_name("jot_m");
    let m = MongoTest::connect(27017, &db);
    m.seed_int_id("bench", 3);
    let rig = |line: &str| {
        readback_only(Rig::mongo_batch("bench").source_url(&MongoTest::url(27017, &db)))
            .export_line(line)
    };
    let refused = rig("columns: {_id: json}").run();
    assert!(!refused.status.success(), "{:?}", refused.status);
    let said = String::from_utf8_lossy(&refused.stderr);
    assert!(
        said.contains("RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH"),
        "{said}"
    );
    let ok = rig("columns: {document: json}");
    ok.run_ok();
    let docs = dir_parquet_display(&ok.out_dir(), "document");
    assert_eq!(docs.len(), 3);
    for d in docs.iter().flatten() {
        serde_json::from_str::<serde_json::Value>(d).unwrap_or_else(|e| panic!("{d}: {e}"));
    }
    m.drop_database();
}
