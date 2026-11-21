//! A binary column under a `string` override is delivered as the bytes it holds or refused by code, never written with U+FFFD in place of a byte.

use crate::common::*;
use crate::live_int_to_float_override::readback_only;

const NON_UTF8: &str = "F83FC0AAFF00";
const UTF8: &str = "68C3A96C6C6F20E697A5E69CAC";

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
    Varbinary,
    Blob,
    Geometry,
}

impl Eng {
    fn ty(self, s: Src) -> Option<&'static str> {
        match (self, s) {
            (Eng::Mysql, Src::Varbinary) => Some("VARBINARY(64)"),
            (Eng::Mysql, Src::Blob) => Some("BLOB"),
            (Eng::Mysql, Src::Geometry) => Some("GEOMETRY"),
            (Eng::Postgres, Src::Varbinary) => Some("BYTEA"),
            (Eng::Mssql, Src::Varbinary) => Some("VARBINARY(64)"),
            (Eng::Oracle, Src::Varbinary) => Some("RAW(64)"),
            (Eng::Oracle, Src::Blob) => Some("BLOB"),
            _ => None,
        }
    }

    fn literal(self, s: Src, hex: &str) -> String {
        match (self, s) {
            (Eng::Mysql, Src::Geometry) => "ST_GeomFromText('POINT(1.5 2.5)')".into(),
            (Eng::Mysql, _) => format!("X'{hex}'"),
            (Eng::Postgres, _) => format!("decode('{hex}', 'hex')"),
            (Eng::Mssql, _) => format!("0x{hex}"),
            (Eng::Oracle, Src::Blob) => format!("TO_BLOB(HEXTORAW('{hex}'))"),
            (Eng::Oracle, _) => format!("HEXTORAW('{hex}')"),
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
            let t = OracleTable::create("bso_or", &format!("id INTEGER PRIMARY KEY, v {ty}"));
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
        let table = unique_name("bso_x");
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

struct Case {
    label: &'static str,
    src: Src,
    hex: &'static str,
}

const CASES: &[Case] = &[
    Case {
        label: "varbinary non-utf8",
        src: Src::Varbinary,
        hex: NON_UTF8,
    },
    Case {
        label: "varbinary utf8",
        src: Src::Varbinary,
        hex: UTF8,
    },
    Case {
        label: "blob non-utf8",
        src: Src::Blob,
        hex: NON_UTF8,
    },
    Case {
        label: "blob utf8",
        src: Src::Blob,
        hex: UTF8,
    },
    Case {
        label: "geometry point",
        src: Src::Geometry,
        hex: "",
    },
];

fn hex_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len() / 2)
        .map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap())
        .collect()
}

/// What a run of `case` did, graded against the bytes inserted: the bytes as text, their hex, a refusal by code, or a delivery that is not the source.
fn outcome(eng: Eng, case: &Case) -> Option<String> {
    let ty = eng.ty(case.src)?;
    let (table, _guard) = eng.table(ty, &eng.literal(case.src, case.hex));
    let rig =
        readback_only(eng.rig(&table)).export_line(&format!("columns: {{{}: string}}", eng.col()));
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
    if got.len() != 2 || got[1].is_some() {
        return Some(format!("WRONG {got:?}"));
    }
    let s = got[0].clone().unwrap_or_default();
    let bytes = hex_bytes(case.hex);
    Some(if s.contains('\u{fffd}') {
        format!("LOSSY {s:?}")
    } else if !case.hex.is_empty() && s.as_bytes() == bytes.as_slice() {
        "delivered the bytes".into()
    } else if s.eq_ignore_ascii_case(case.hex) {
        "delivered hex".into()
    } else {
        format!("WRONG {s:?}")
    })
}

const BYTES: &str = "delivered the bytes";
const HEX: &str = "delivered hex";
const U: &str = "refused RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5)";
const NA: &str = "-";

/// Pinned outcome per case in `CASES` order: MySQL, PostgreSQL, SQL Server, Oracle.
const EXPECTED: [[&str; 4]; 5] = [
    [U, NA, NA, NA],
    [BYTES, NA, NA, HEX],
    [U, NA, NA, NA],
    [BYTES, NA, NA, NA],
    [U, NA, NA, NA],
];

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
        "{eng:?}: a binary column under `string` must deliver its bytes or refuse by code, as pinned:\n{}",
        bad.join("\n")
    );
}

/// MySQL VARBINARY, BLOB and GEOMETRY under `string`: valid UTF-8 is delivered, any other byte is refused by code and nothing is written.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_binary_column_declared_string_never_loses_a_byte_mysql() {
    matrix(Eng::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_binary_column_declared_string_never_loses_a_byte_postgres() {
    matrix(Eng::Postgres);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_binary_column_declared_string_never_loses_a_byte_mssql() {
    matrix(Eng::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_binary_column_declared_string_never_loses_a_byte_oracle() {
    matrix(Eng::Oracle);
}
