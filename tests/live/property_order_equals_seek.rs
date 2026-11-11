//! Live matrix: page order and seek predicate agree, for every engine and text key type, in every seeking mode.

use crate::common::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

const HOSTILE_KEYS: [&str; 10] = ["a", "é'z", "é-a", "é-z", "é a", "éa", "Éb", "éc", "z", "Z1"];
const CURSOR_KEY: &str = "é'z";

/// A way a run seeks: the cell's fourth axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Sequential,
    Parallel,
    Incremental,
    KeysetIncremental,
}

pub(crate) const MODES: [Mode; 4] = [
    Mode::Sequential,
    Mode::Parallel,
    Mode::Incremental,
    Mode::KeysetIncremental,
];

/// The text key column types of `engine` the matrix grades, as `(name, column type)`.
pub(crate) fn key_types(engine: SqlEngine) -> &'static [(&'static str, &'static str)] {
    match engine {
        SqlEngine::Pg => &[
            ("text", "TEXT"),
            ("varchar", "VARCHAR(20)"),
            ("text_c", "TEXT COLLATE \"C\""),
        ],
        SqlEngine::Mysql => &[
            ("utf8mb4", "VARCHAR(20) CHARACTER SET utf8mb4"),
            ("latin1", "VARCHAR(20) CHARACTER SET latin1"),
            (
                "utf8mb4_bin",
                "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
            ),
        ],
        SqlEngine::Mssql => &[
            (
                "varchar_sql_latin1",
                "VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS",
            ),
            (
                "varchar_windows_latin1",
                "VARCHAR(20) COLLATE Latin1_General_100_CI_AS",
            ),
            ("varchar_bin2", "VARCHAR(20) COLLATE Latin1_General_BIN2"),
            ("nvarchar", "NVARCHAR(20)"),
        ],
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => &[
            ("varchar2", "VARCHAR2(20 CHAR)"),
            ("nvarchar2", "NVARCHAR2(20)"),
        ],
    }
}

/// The `INSERT` of `keys` (ids from `first_id`) into `table`.
fn insert(engine: SqlEngine, table: &str, keys: &[&str], first_id: usize) {
    let rows = keys
        .iter()
        .enumerate()
        .map(|(i, k)| format!("({}, {})", engine.text_literal(Some(*k)), first_id + i))
        .collect::<Vec<_>>()
        .join(", ");
    engine.exec(&format!("INSERT INTO {table} (k, id) VALUES {rows}"));
}

/// The keys of `table` in the order the server sorts them, the order a page is cut by.
fn server_order(engine: SqlEngine, table: &str) -> Vec<String> {
    let sql = format!("SELECT k FROM {table} ORDER BY k");
    match engine {
        SqlEngine::Pg => pg_connect()
            .query(&sql, &[])
            .expect("pg read")
            .iter()
            .map(|r| r.get(0))
            .collect(),
        SqlEngine::Mysql => {
            ::mysql::prelude::Queryable::query(&mut mysql_connect(), sql).expect("mysql read")
        }
        SqlEngine::Mssql => mssql_query_strings(&sql),
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => ora_text_rows(&sql)
            .into_iter()
            .map(|r| r[0].clone().expect("key"))
            .collect(),
    }
}

/// The keys the server sorts at or below [`CURSOR_KEY`], and those above it.
fn split_at_cursor(engine: SqlEngine, table: &str) -> (Vec<String>, Vec<String>) {
    let ordered = server_order(engine, table);
    let at = ordered
        .iter()
        .position(|k| k == CURSOR_KEY)
        .expect("the cursor key is seeded");
    assert!(at + 1 < ordered.len(), "no key sorts above the cursor key");
    (ordered[..=at].to_vec(), ordered[at + 1..].to_vec())
}

/// One cell: seed the hostile keys, run `mode`, and let the rig oracle grade the delivered keys against the source.
fn cell(engine: SqlEngine, column_type: &str, mode: Mode) {
    let (table, _guard) = engine.create(
        "oes",
        &format!(
            "k {column_type} PRIMARY KEY, id {} NOT NULL",
            engine.int64()
        ),
    );
    let export = if let SqlEngine::Pg = engine {
        format!("public.{table}")
    } else {
        table.clone()
    };
    let seek = match mode {
        Mode::Sequential => vec!["chunk_by_key: k", "chunk_size: 2"],
        Mode::Parallel => vec!["chunk_by_key: k", "chunk_size: 2", "parallel: 2"],
        Mode::Incremental => vec!["cursor_column: k"],
        Mode::KeysetIncremental => vec![
            "chunk_by_key: k",
            "chunk_size: 2",
            "chunk_checkpoint: true",
            "keyset_incremental: true",
        ],
    };
    let kind = if mode == Mode::Incremental {
        "incremental"
    } else {
        "chunked"
    };
    let rig = engine.staged(engine.rig(&export), kind, &seek);
    match mode {
        Mode::Sequential | Mode::Parallel => {
            insert(engine, &table, &HOSTILE_KEYS, 0);
            rig.run_ok();
        }
        Mode::Incremental | Mode::KeysetIncremental => {
            insert(engine, &table, &HOSTILE_KEYS, 0);
            let (first, later) = split_at_cursor(engine, &table);
            let later_sql = later
                .iter()
                .map(|k| engine.text_literal(Some(k)))
                .collect::<Vec<_>>()
                .join(", ");
            engine.exec(&format!("DELETE FROM {table} WHERE k IN ({later_sql})"));
            rig.run_ok();
            let later: Vec<&str> = later.iter().map(String::as_str).collect();
            insert(engine, &table, &later, first.len());
            rig.run_ok();
        }
    }
}

/// Every key type of `engine` in every mode; one verdict line per cell, then fail on any red.
fn grade(engine: SqlEngine) {
    engine.alive();
    let mut red = Vec::new();
    for (name, column_type) in key_types(engine) {
        for mode in MODES {
            let verdict = catch_unwind(AssertUnwindSafe(|| cell(engine, column_type, mode)));
            let word = if verdict.is_ok() { "PASS" } else { "FAIL" };
            println!("ORDER-EQUALS-SEEK {word} {engine:?}/{name}/{mode:?}");
            if verdict.is_err() {
                red.push(format!("{engine:?}/{name}/{mode:?}"));
            }
        }
    }
    assert!(red.is_empty(), "page order and seek disagree in: {red:?}");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn order_equals_seek_postgres() {
    grade(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn order_equals_seek_mysql() {
    grade(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn order_equals_seek_mssql() {
    grade(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn order_equals_seek_oracle() {
    grade(SqlEngine::Oracle);
}
