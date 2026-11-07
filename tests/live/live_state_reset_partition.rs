//! `state reset` on a `partition_by` export (docs/mode-transition-matrix.yaml, `partition_reset_*`):
//! the export's progress is kept per partition child, so a reset of the declared name starts every
//! child over and a reset of one child starts only that child over.
//!
//! Oracles: the ids the next run adds to an empty destination, against the rows inserted in the source,
//! and the `export_state` / `export_progression` rows the state database holds, read with SQLite directly.

use crate::common::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::{Path, PathBuf};

const DAY_MINUTES: i64 = 24 * 60;

/// Six rows over three days: ids 1-2 three days ago, ids 3-4 two days ago, ids 5-6 yesterday.
fn three_days(engine: SqlEngine) -> (String, Box<dyn std::any::Any>) {
    let (table, guard) = engine.range_table("part_reset");
    for (ids, days) in [(1..=2, 3), (3..=4, 2), (5..=6, 1)] {
        engine.insert(&table, ids, days * DAY_MINUTES + 30, Some(10));
    }
    (table, guard)
}

fn partition_column(engine: SqlEngine) -> &'static str {
    match engine {
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => "SERVER_TIME",
        _ => "server_time",
    }
}

fn partitioned(engine: SqlEngine, table: &str, mode: &str, dest: &Path) -> Rig {
    let column = partition_column(engine);
    let cursor = format!("cursor_column: {}", engine.col("id"));
    let by = format!("partition_by: {column}");
    let lines: Vec<&str> = if mode == "incremental" {
        vec![&cursor, &by]
    } else {
        vec![&by]
    };
    engine
        .staged(engine.rig(table), mode, &lines)
        .dest_path(dest.join("{partition}"))
}

fn delivered(dest: &Path) -> (Vec<i64>, usize) {
    let parts: Vec<PathBuf> = files_under(dest)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect();
    let batches: Vec<_> = parts
        .iter()
        .flat_map(|p| {
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(p).unwrap())
                .unwrap()
                .build()
                .unwrap()
                .map(|b| b.unwrap())
        })
        .collect();
    let (ids, rows) = ids_and_rows(&batches);
    (ids.into_iter().collect(), rows)
}

fn emptied(dest: &Path) {
    for entry in std::fs::read_dir(dest).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path).unwrap();
        } else {
            std::fs::remove_file(&path).unwrap();
        }
    }
    assert!(files_under(dest).is_empty(), "the destination is empty");
}

fn partition_emptied(dest: &Path, value: &str) {
    let mut gone = 0;
    for entry in std::fs::read_dir(dest).unwrap() {
        let path = entry.unwrap().path();
        if path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().ends_with(value))
        {
            std::fs::remove_dir_all(&path).unwrap();
            gone += 1;
        }
    }
    assert_eq!(gone, 1, "one partition directory ends with {value}");
}

fn state_names(rig: &Rig, table: &str) -> Vec<String> {
    let db = rusqlite::Connection::open(rig.config_path().with_file_name(".rivet_state.db"))
        .expect("the state database");
    let mut names = Vec::new();
    for sql in [
        "SELECT export_name FROM export_state",
        "SELECT export_name FROM export_progression",
    ] {
        let mut q = db.prepare(sql).unwrap();
        names.extend(
            q.query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .map(|n| n.unwrap()),
        );
    }
    names.retain(|n| n.starts_with(rig.export_name()) || n.starts_with(table));
    names.sort();
    names.dedup();
    names
}

fn reset(rig: &Rig, export: &str) -> String {
    let out = rig.cli(&["state", "reset", "--export", export]);
    assert!(
        out.status.success(),
        "state reset {export} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn child_of(rig: &Rig, names: &[String], day: usize) -> String {
    let mut children: Vec<&String> = names
        .iter()
        .filter(|n| n.starts_with(&format!("{}__", rig.export_name())))
        .collect();
    children.sort();
    children.dedup();
    children[day].clone()
}

/// A reset of the declared name leaves no state of any child, and the next run into an empty destination delivers every source row.
fn reset_of_the_parent_starts_every_child_over(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let first = tempfile::tempdir().unwrap();
    let rig = partitioned(engine, &table, "incremental", first.path());
    rig.run_ok();
    assert_eq!(delivered(first.path()), (vec![1, 2, 3, 4, 5, 6], 6));
    let held = state_names(&rig, &table);
    assert!(
        held.iter()
            .filter(|n| n.starts_with(&format!("{}__", rig.export_name())))
            .count()
            >= 3,
        "the run must leave the children's progress in the state: {held:?}"
    );

    let said = reset(&rig, rig.export_name());
    assert!(
        said.contains(&format!("State reset for export '{}'", rig.export_name())),
        "{said}"
    );
    let left = state_names(&rig, &table);

    emptied(first.path());
    rig.run_ok();
    assert_eq!(
        delivered(first.path()),
        (vec![1, 2, 3, 4, 5, 6], 6),
        "the run after a reset delivers every source row to the empty destination"
    );
    assert_eq!(
        left,
        Vec::<String>::new(),
        "a reset leaves no state of the export or its children"
    );
}

/// A reset of one child starts that partition over: with its directory emptied the destination holds every row once, so the others' cursors stayed.
fn reset_of_one_child_starts_only_that_child_over(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let rig = partitioned(engine, &table, "incremental", dest.path());
    rig.run_ok();
    let names = state_names(&rig, &table);
    let child = child_of(&rig, &names, 1);

    let said = reset(&rig, &child);
    assert!(
        said.contains(&format!("State reset for export '{child}'")),
        "{said}"
    );
    let left = state_names(&rig, &table);
    let expected: Vec<String> = names.iter().filter(|n| **n != child).cloned().collect();
    assert_eq!(left, expected, "only that child's rows go");

    let value = child.rsplit_once("__").unwrap().1;
    partition_emptied(dest.path(), value);
    rig.run_ok();
    assert_eq!(delivered(dest.path()), (vec![1, 2, 3, 4, 5, 6], 6));
}

/// A reset of a name that is neither declared nor a stored child of a declared export is refused by code.
fn reset_of_an_unknown_child_is_refused(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let rig = partitioned(engine, &table, "incremental", dest.path());
    rig.run_ok();
    let before = state_names(&rig, &table);
    let ghost = format!("{}__1999-01-01", rig.export_name());
    let out = rig.cli(&["state", "reset", "--export", &ghost]);
    assert!(!out.status.success());
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(said.contains("RIVET_CLI_EXPORT_UNKNOWN"), "{said}");
    assert_eq!(state_names(&rig, &table), before, "nothing was reset");
}

/// Mode full keeps no cursor: a reset prints its line, changes nothing and the next run delivers every row again.
fn reset_of_a_full_partitioned_export_changes_nothing(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let rig = partitioned(engine, &table, "full", dest.path());
    rig.run_ok();
    let before = state_names(&rig, &table);
    let said = reset(&rig, rig.export_name());
    assert!(
        said.contains(&format!("State reset for export '{}'", rig.export_name())),
        "{said}"
    );
    assert_eq!(state_names(&rig, &table), before);
    emptied(dest.path());
    rig.run_ok();
    assert_eq!(delivered(dest.path()), (vec![1, 2, 3, 4, 5, 6], 6));
}

/// A reset of a plain incremental export clears its cursor and the next run delivers every row.
fn reset_of_a_plain_incremental_export_starts_it_over(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let cursor = format!("cursor_column: {}", engine.col("id"));
    let rig = engine
        .staged(engine.rig(&table), "incremental", &[&cursor])
        .dest_path(dest.path().to_path_buf());
    rig.run_ok();
    assert_eq!(delivered(dest.path()), (vec![1, 2, 3, 4, 5, 6], 6));
    assert_eq!(
        state_names(&rig, &table),
        vec![rig.export_name().to_string()]
    );
    reset(&rig, rig.export_name());
    assert_eq!(state_names(&rig, &table), Vec::<String>::new());
    emptied(dest.path());
    rig.run_ok();
    assert_eq!(delivered(dest.path()), (vec![1, 2, 3, 4, 5, 6], 6));
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_parent_postgres() {
    reset_of_the_parent_starts_every_child_over(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_parent_mysql() {
    reset_of_the_parent_starts_every_child_over(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_parent_mssql() {
    reset_of_the_parent_starts_every_child_over(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_parent_oracle() {
    reset_of_the_parent_starts_every_child_over(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_child_postgres() {
    reset_of_one_child_starts_only_that_child_over(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_child_mysql() {
    reset_of_one_child_starts_only_that_child_over(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_child_mssql() {
    reset_of_one_child_starts_only_that_child_over(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_child_oracle() {
    reset_of_one_child_starts_only_that_child_over(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_unknown_child_postgres() {
    reset_of_an_unknown_child_is_refused(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_unknown_child_mysql() {
    reset_of_an_unknown_child_is_refused(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_unknown_child_mssql() {
    reset_of_an_unknown_child_is_refused(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_unknown_child_oracle() {
    reset_of_an_unknown_child_is_refused(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_full_postgres() {
    reset_of_a_full_partitioned_export_changes_nothing(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_full_mysql() {
    reset_of_a_full_partitioned_export_changes_nothing(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_full_mssql() {
    reset_of_a_full_partitioned_export_changes_nothing(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_full_oracle() {
    reset_of_a_full_partitioned_export_changes_nothing(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_plain_postgres() {
    reset_of_a_plain_incremental_export_starts_it_over(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_plain_mysql() {
    reset_of_a_plain_incremental_export_starts_it_over(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_plain_mssql() {
    reset_of_a_plain_incremental_export_starts_it_over(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_plain_oracle() {
    reset_of_a_plain_incremental_export_starts_it_over(SqlEngine::Oracle);
}
