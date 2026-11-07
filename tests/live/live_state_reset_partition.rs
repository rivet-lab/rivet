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
    partitioned_with(engine, table, mode, &[], dest)
}

fn partitioned_with(engine: SqlEngine, table: &str, mode: &str, more: &[&str], dest: &Path) -> Rig {
    let column = partition_column(engine);
    let cursor = format!("cursor_column: {}", engine.col("id"));
    let by = format!("partition_by: {column}");
    let mut lines: Vec<&str> = if mode == "incremental" {
        vec![&cursor, &by]
    } else {
        vec![&by]
    };
    lines.extend_from_slice(more);
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
    if rig.state_count("SELECT COUNT(*) FROM export_state WHERE export_name = '{export}'") == 0 {
        let ours = source_key(&rig);
        rig.edit_state(
            &format!(
                "INSERT INTO export_state (export_name, prefix, last_cursor_value) \
                 VALUES ('{{export}}', '{ours}', '7')"
            ),
            1,
        );
    }
    let before = state_names(&rig, &table);
    let ghost = format!("{}__1999-01-01", rig.export_name());
    let out = rig.cli(&["state", "reset", "--export", &ghost]);
    assert!(!out.status.success());
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(said.contains("RIVET_CLI_EXPORT_UNKNOWN"), "{said}");
    let held_line = said
        .lines()
        .find_map(|l| l.trim().strip_prefix("Partitions held: "))
        .unwrap_or_else(|| panic!("the refusal lists the partitions held: {said}"));
    assert!(
        held_line.split(", ").all(|p| p != rig.export_name()),
        "the parent is no partition of itself: {held_line}"
    );
    assert!(
        held_line.contains(&format!("{}__", rig.export_name())),
        "{held_line}"
    );
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

const FEW_FILES: u64 = 256;
const CLONES: usize = 300;

/// The rig on the scratch Postgres state `tag` names, or on the SQLite state beside its config.
fn on_state(rig: Rig, tag: Option<&str>) -> (Rig, Option<ScratchStateDb>) {
    match tag {
        Some(tag) => {
            let db = ScratchStateDb::new(tag);
            (rig.on_postgres_state(db.url()), Some(db))
        }
        None => (rig, None),
    }
}

/// The state key of the source the rig's config names.
fn source_key(rig: &Rig) -> String {
    rivet::config::Config::load(&rig.config_path().display().to_string())
        .expect("fixture: the rig's config loads")
        .source
        .state_key()
}

/// The rows of `table` held for the export and its partitions.
fn family_rows(rig: &Rig, table: &str) -> i64 {
    rig.state_count(&format!(
        "SELECT COUNT(*) FROM {table} WHERE export_name LIKE '{{export}}%'"
    ))
}

/// The rows of `table` held for the export's partitions alone.
fn child_rows(rig: &Rig, table: &str) -> i64 {
    rig.state_count(&format!(
        "SELECT COUNT(*) FROM {table} WHERE export_name <> '{{export}}' \
         AND export_name LIKE '{{export}}%'"
    ))
}

const CLONE_LABEL: &str = "CAST(1000 + i / 57 AS TEXT) || '-' || CAST(10 + (i / 19) % 3 AS TEXT) || '-' || CAST(10 + i % 19 AS TEXT)";

/// The day label of the `i`-th extra partition, as `CLONE_LABEL` spells it.
fn clone_label(i: i64) -> String {
    format!("{}-{}-{}", 1000 + i / 57, 10 + (i / 19) % 3, 10 + i % 19)
}

/// `count` more partitions of the export, each with a cursor on its source and a boundary.
fn stored_partitions(rig: &Rig, count: usize) {
    let numbers = format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {count})"
    );
    rig.edit_state(
        &format!(
            "{numbers} INSERT INTO export_state (export_name, prefix, last_cursor_value) \
             SELECT '{{export}}__' || {CLONE_LABEL}, (SELECT prefix FROM export_state \
             WHERE export_name <> '{{export}}' AND export_name LIKE '{{export}}%' LIMIT 1), '7' \
             FROM n"
        ),
        count as u64,
    );
    rig.edit_state(
        &format!(
            "{numbers} INSERT INTO export_progression \
             (export_name, last_committed_strategy, last_committed_cursor, \
             last_committed_run_id, last_committed_at) \
             SELECT '{{export}}__' || {CLONE_LABEL}, 'incremental', '7', 'r', \
             '2026-01-01T00:00:00Z' FROM n"
        ),
        count as u64,
    );
}

/// `key` spelled without its engine's default port.
fn without_default_port(key: &str) -> String {
    let port = match key.split_once("://").expect("a source key").0 {
        "postgres" => 5432,
        "mysql" => 3306,
        "mssql" => 1433,
        "oracle" => 1521,
        other => panic!("fixture: no default port is known for {other}"),
    };
    let respelled = key.replacen(&format!(":{port}"), "", 1);
    assert_ne!(
        respelled, key,
        "fixture: the stand listens on the default port {port}"
    );
    respelled
}

/// The state the rig's runs use, opened by this process.
fn state_of(rig: &Rig) -> rivet::state::StateStore {
    match rig.state_url(&[]) {
        Some(url) => rivet::state::StateStore::open_at_ref(&rivet::state::StateRef::Postgres(url))
            .expect("fixture: open the Postgres state"),
        None => rivet::state::StateStore::open(&rig.config_path().display().to_string())
            .expect("fixture: open the SQLite state"),
    }
}

fn text_of(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// A reset of a family larger than the files one process may hold open clears every member, and the next run delivers every row.
fn reset_of_a_family_larger_than_the_open_file_limit(engine: SqlEngine, state: Option<&str>) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let (rig, _state) = on_state(
        partitioned(engine, &table, "incremental", dest.path()),
        state,
    );
    rig.run_ok();
    stored_partitions(&rig, CLONES);
    assert!(child_rows(&rig, "export_state") > CLONES as i64, "fixture");
    let rig = rig.open_files(FEW_FILES);
    let said = reset(&rig, rig.export_name());
    assert!(said.contains("partition(s)"), "{said}");
    assert_eq!(family_rows(&rig, "export_state"), 0, "{said}");
    assert_eq!(family_rows(&rig, "export_progression"), 0, "{said}");
    emptied(dest.path());
    rig.run_ok();
    assert_eq!(delivered(dest.path()), (vec![1, 2, 3, 4, 5, 6], 6));
}

/// Partitions stored under a spelling of the source the config no longer writes, with no boundary, go with a reset of the export.
fn reset_takes_partitions_stored_under_a_respelled_source(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let rig = partitioned(engine, &table, "incremental", dest.path());
    rig.run_ok();
    let respelled = without_default_port(&source_key(&rig));
    let children = child_rows(&rig, "export_state");
    assert!(children >= 3, "fixture: the run stored its partitions");
    rig.edit_state(
        &format!(
            "UPDATE export_state SET prefix = '{respelled}' WHERE export_name <> '{{export}}' \
             AND export_name LIKE '{{export}}%'"
        ),
        children as u64,
    );
    rig.edit_state(
        "DELETE FROM export_progression WHERE export_name <> '{export}' \
         AND export_name LIKE '{export}%'",
        child_rows(&rig, "export_progression") as u64,
    );
    let said = reset(&rig, rig.export_name());
    assert_eq!(child_rows(&rig, "export_state"), 0, "{said}");
    emptied(dest.path());
    rig.run_ok();
    assert_eq!(delivered(dest.path()), (vec![1, 2, 3, 4, 5, 6], 6));
}

/// A reset takes the partitions of this source and leaves the same-named export's partition of another source as it was.
fn reset_leaves_the_partitions_of_another_source(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let rig = partitioned(engine, &table, "incremental", dest.path());
    rig.run_ok();
    let other = "postgres://other.example:5432/elsewhere";
    rig.edit_state(
        &format!(
            "INSERT INTO export_state (export_name, prefix, last_cursor_value) \
             VALUES ('{{export}}__elsewhere', '{other}', '7')"
        ),
        1,
    );
    rig.edit_state(
        "INSERT INTO export_progression (export_name, last_committed_strategy, \
         last_committed_cursor) VALUES ('{export}__elsewhere', 'incremental', '7')",
        1,
    );
    let said = reset(&rig, rig.export_name());
    assert_eq!(
        rig.state_count(&format!(
            "SELECT COUNT(*) FROM export_state WHERE export_name LIKE '{{export}}%' \
             AND prefix <> '{other}'"
        )),
        0,
        "{said}"
    );
    assert_eq!(
        rig.state_count(&format!(
            "SELECT COUNT(*) FROM export_state WHERE export_name = '{{export}}__elsewhere' \
             AND prefix = '{other}'"
        )),
        1,
        "the other source keeps its cursor: {said}"
    );
    assert_eq!(
        rig.state_count(
            "SELECT COUNT(*) FROM export_progression WHERE export_name = '{export}__elsewhere'"
        ),
        1,
        "the other source keeps its boundary: {said}"
    );
}

/// A run holding the lease of one partition stops a reset of the export: at once by code, after `--lock-wait` by its timeout code, and never half done; a wait that outlasts the run clears every partition.
fn reset_beside_a_run_of_one_partition(engine: SqlEngine, state: Option<&str>) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let (rig, _state) = on_state(
        partitioned(engine, &table, "incremental", dest.path()),
        state,
    );
    rig.run_ok();
    stored_partitions(&rig, 5);
    let export = rig.export_name().to_string();
    let child = format!("{export}__{}", clone_label(3));
    let key = rivet::pipeline::run_lease_key(&child, &source_key(&rig));
    let (states, boundaries) = (
        family_rows(&rig, "export_state"),
        family_rows(&rig, "export_progression"),
    );
    let untouched = |why: &str| {
        assert_eq!(family_rows(&rig, "export_state"), states, "{why}");
        assert_eq!(family_rows(&rig, "export_progression"), boundaries, "{why}");
    };
    {
        let store = state_of(&rig);
        let _run = store.try_load_lease(&key).unwrap().expect("a live run");
        let at_once = rig.cli(&["--lock-wait", "0", "state", "reset", "--export", &export]);
        let said = text_of(&at_once);
        assert_eq!(at_once.status.code(), Some(2), "{said}");
        assert!(said.contains("RIVET_STATE_RUN_IN_PROGRESS"), "{said}");
        assert!(said.contains(&format!("export '{child}'")), "{said}");
        untouched(&said);
        let waited = rig.cli(&["--lock-wait", "2", "state", "reset", "--export", &export]);
        let said = text_of(&waited);
        assert_eq!(waited.status.code(), Some(2), "{said}");
        assert!(said.contains("RIVET_STATE_LOCK_TIMEOUT"), "{said}");
        assert!(
            said.contains(&format!("the run lease of export '{child}'")),
            "{said}"
        );
        assert!(said.contains("nothing was done"), "{said}");
        untouched(&said);
    }
    let (taken, took) = std::sync::mpsc::channel();
    let ends = {
        let (rig_url, config) = (rig.state_url(&[]), rig.config_path());
        std::thread::spawn(move || {
            let store = match rig_url {
                Some(url) => {
                    rivet::state::StateStore::open_at_ref(&rivet::state::StateRef::Postgres(url))
                        .unwrap()
                }
                None => rivet::state::StateStore::open(&config.display().to_string()).unwrap(),
            };
            let run = store.try_load_lease(&key).unwrap().expect("a live run");
            taken.send(()).unwrap();
            std::thread::sleep(std::time::Duration::from_secs(2));
            drop(run);
        })
    };
    took.recv().unwrap();
    let out = rig.cli(&["--lock-wait", "30", "state", "reset", "--export", &export]);
    ends.join().unwrap();
    let said = text_of(&out);
    assert!(out.status.success(), "{said}");
    assert_eq!(family_rows(&rig, "export_state"), 0, "{said}");
    assert_eq!(family_rows(&rig, "export_progression"), 0, "{said}");
}

fn at_granularity(engine: SqlEngine, rig: Rig, granularity: &str) -> Rig {
    let cursor = format!("cursor_column: {}", engine.col("id"));
    let by = format!("partition_by: {}", partition_column(engine));
    let width = format!("partition_granularity: {granularity}");
    engine.staged(rig, "incremental", &[&cursor, &by, &width])
}

/// After `partition_granularity` changed day to month and a run, a reset of the export clears the partitions of both granularities, the inspect commands show the old ones, and the next run into an empty destination delivers every row.
fn reset_after_a_granularity_change_takes_every_partition(engine: SqlEngine, state: Option<&str>) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let (rig, _state) = on_state(
        partitioned(engine, &table, "incremental", dest.path()),
        state,
    );
    rig.run_ok();
    assert_eq!(delivered(dest.path()), (vec![1, 2, 3, 4, 5, 6], 6));
    let export = rig.export_name().to_string();
    let day_children = child_rows(&rig, "export_state");
    assert!(day_children > 0);
    let rig = at_granularity(engine, rig, "month");
    rig.run_ok();
    let shown = rig.cli(&["state", "progression", "--export", &export]);
    let said = text_of(&shown);
    assert!(shown.status.success(), "{said}");
    let day_lines = said
        .lines()
        .filter(|l| {
            l.split_whitespace().next().is_some_and(|n| {
                n.starts_with(&format!("{export}__")) && n.len() == export.len() + 2 + 10
            })
        })
        .count() as i64;
    assert_eq!(day_lines, day_children, "{said}");
    let said = reset(&rig, &export);
    assert!(said.contains("partition(s)"), "{said}");
    assert_eq!(family_rows(&rig, "export_state"), 0, "{said}");
    assert_eq!(family_rows(&rig, "export_progression"), 0, "{said}");
    emptied(dest.path());
    let rig = at_granularity(engine, rig, "day");
    rig.run_ok();
    assert_eq!(delivered(dest.path()), (vec![1, 2, 3, 4, 5, 6], 6));
}

/// A reset of the export takes the partitions whose suffix is a label of its granularity and never an undeclared name that merely starts with `<export>__`.
fn reset_takes_labels_and_never_another_exports_name(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let rig = partitioned(engine, &table, "incremental", dest.path());
    rig.run_ok();
    let source = source_key(&rig);
    let kept = ["archive", "2026-02-30", "x__2026-01-01"];
    let taken = ["2026-01", "2026"];
    for name in kept.into_iter().chain(taken) {
        rig.edit_state(
            &format!(
                "INSERT INTO export_state (export_name, prefix, last_cursor_value) \
                 VALUES ('{{export}}__{name}', '{source}', '7')"
            ),
            1,
        );
    }
    let said = reset(&rig, rig.export_name());
    assert!(said.contains("partition(s)"), "{said}");
    assert_eq!(child_rows(&rig, "export_progression"), 0, "{said}");
    assert_eq!(
        rig.state_count("SELECT COUNT(*) FROM export_state WHERE export_name = '{export}'"),
        0,
        "{said}"
    );
    for name in taken {
        assert_eq!(
            rig.state_count(&format!(
                "SELECT COUNT(*) FROM export_state WHERE export_name = '{{export}}__{name}'"
            )),
            0,
            "'{}__{name}' is a partition of another granularity and goes with the reset: {said}",
            rig.export_name()
        );
    }
    for name in kept {
        assert_eq!(
            rig.state_count(&format!(
                "SELECT COUNT(*) FROM export_state WHERE export_name = '{{export}}__{name}'"
            )),
            1,
            "the cursor of '{}__{name}' is not a partition of the export: {said}",
            rig.export_name()
        );
    }
    emptied(dest.path());
    rig.run_ok();
    assert_eq!(delivered(dest.path()), (vec![1, 2, 3, 4, 5, 6], 6));
}

/// An inspect command refuses a name no partition could carry with the unknown-export code and shows a partition the state holds.
fn inspect_refuses_a_typo_and_shows_a_held_partition(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let rig = partitioned(engine, &table, "incremental", dest.path());
    rig.run_ok();
    let before = state_names(&rig, &table);
    let held = child_of(&rig, &before, 1);
    for cmd in [
        &["state", "files"][..],
        &["state", "chunks"],
        &["state", "progression"],
        &["journal"],
        &["metrics"],
    ] {
        for name in [
            format!("{}__typo", rig.export_name()),
            format!("{}__1999-01-01", rig.export_name()),
        ] {
            let mut args = cmd.to_vec();
            args.extend(["--export", &name]);
            let out = rig.cli(&args);
            let said = text_of(&out);
            assert_eq!(out.status.code(), Some(1), "{cmd:?} {name}: {said}");
            assert!(
                said.contains("RIVET_CLI_EXPORT_UNKNOWN"),
                "{cmd:?} {name}: {said}"
            );
        }
    }
    let out = rig.cli(&["state", "progression", "--export", &held]);
    let said = text_of(&out);
    assert!(out.status.success(), "{said}");
    assert!(said.contains(&held), "{said}");
    assert!(!said.contains("No progression boundaries"), "{said}");
    assert_eq!(state_names(&rig, &table), before, "{said}");
}

/// A chunked export's partition boundaries go with a reset of the export when its chunk runs are of this source, and a partition another source's chunk runs wrote stays.
fn reset_of_a_chunked_partitioned_export_leaves_another_sources_partition(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = three_days(engine);
    let dest = tempfile::tempdir().unwrap();
    let rig = partitioned_with(
        engine,
        &table,
        "chunked",
        &["chunk_column: id", "chunk_size: 2"],
        dest.path(),
    );
    rig.run_ok();
    assert_eq!(delivered(dest.path()), (vec![1, 2, 3, 4, 5, 6], 6));
    let ours = source_key(&rig);
    let other = "postgres://other.example:5432/elsewhere";
    for (label, source) in [("1999-01-01", other), ("1999-01-02", ours.as_str())] {
        rig.edit_state(
            &format!(
                "INSERT INTO export_progression (export_name, last_committed_strategy, \
                 last_committed_chunk_index, last_committed_run_id, last_committed_at) \
                 VALUES ('{{export}}__{label}', 'chunked', 3, 'r', '2026-01-01T00:00:00Z')"
            ),
            1,
        );
        rig.edit_state(
            &format!(
                "INSERT INTO chunk_run (run_id, export_name, source, plan_hash, status, \
                 max_chunk_attempts, created_at, updated_at) VALUES ('run{label}', \
                 '{{export}}__{label}', '{source}', 'h', 'completed', 3, \
                 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
            ),
            1,
        );
    }
    let said = reset(&rig, rig.export_name());
    assert_eq!(child_rows(&rig, "export_progression"), 1, "{said}");
    assert_eq!(
        rig.state_count(
            "SELECT COUNT(*) FROM export_progression WHERE export_name = '{export}__1999-01-01'"
        ),
        1,
        "the other source keeps its boundary: {said}"
    );
    emptied(dest.path());
    rig.run_ok();
    assert_eq!(delivered(dest.path()), (vec![1, 2, 3, 4, 5, 6], 6));
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_family_postgres() {
    reset_of_a_family_larger_than_the_open_file_limit(SqlEngine::Pg, None);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_family_mysql() {
    reset_of_a_family_larger_than_the_open_file_limit(SqlEngine::Mysql, None);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_family_mssql() {
    reset_of_a_family_larger_than_the_open_file_limit(SqlEngine::Mssql, None);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_family_oracle() {
    reset_of_a_family_larger_than_the_open_file_limit(SqlEngine::Oracle, None);
}

#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn partition_reset_family_pg_state_postgres() {
    reset_of_a_family_larger_than_the_open_file_limit(SqlEngine::Pg, Some("family_pg"));
}

#[test]
#[ignore = "live: requires mysql + postgres-state"]
fn partition_reset_family_pg_state_mysql() {
    reset_of_a_family_larger_than_the_open_file_limit(SqlEngine::Mysql, Some("family_my"));
}

#[test]
#[ignore = "live: requires mssql + postgres-state"]
fn partition_reset_family_pg_state_mssql() {
    reset_of_a_family_larger_than_the_open_file_limit(SqlEngine::Mssql, Some("family_ms"));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires oracle + postgres-state"]
fn partition_reset_family_pg_state_oracle() {
    reset_of_a_family_larger_than_the_open_file_limit(SqlEngine::Oracle, Some("family_or"));
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_respelled_postgres() {
    reset_takes_partitions_stored_under_a_respelled_source(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_respelled_mysql() {
    reset_takes_partitions_stored_under_a_respelled_source(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_respelled_mssql() {
    reset_takes_partitions_stored_under_a_respelled_source(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_respelled_oracle() {
    reset_takes_partitions_stored_under_a_respelled_source(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_cross_source_postgres() {
    reset_leaves_the_partitions_of_another_source(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_cross_source_mysql() {
    reset_leaves_the_partitions_of_another_source(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_cross_source_mssql() {
    reset_leaves_the_partitions_of_another_source(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_cross_source_oracle() {
    reset_leaves_the_partitions_of_another_source(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_child_lease_postgres() {
    reset_beside_a_run_of_one_partition(SqlEngine::Pg, None);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_child_lease_mysql() {
    reset_beside_a_run_of_one_partition(SqlEngine::Mysql, None);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_child_lease_mssql() {
    reset_beside_a_run_of_one_partition(SqlEngine::Mssql, None);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_child_lease_oracle() {
    reset_beside_a_run_of_one_partition(SqlEngine::Oracle, None);
}

#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn partition_reset_child_lease_pg_state_postgres() {
    reset_beside_a_run_of_one_partition(SqlEngine::Pg, Some("lease_pg"));
}

#[test]
#[ignore = "live: requires mysql + postgres-state"]
fn partition_reset_child_lease_pg_state_mysql() {
    reset_beside_a_run_of_one_partition(SqlEngine::Mysql, Some("lease_my"));
}

#[test]
#[ignore = "live: requires mssql + postgres-state"]
fn partition_reset_child_lease_pg_state_mssql() {
    reset_beside_a_run_of_one_partition(SqlEngine::Mssql, Some("lease_ms"));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires oracle + postgres-state"]
fn partition_reset_child_lease_pg_state_oracle() {
    reset_beside_a_run_of_one_partition(SqlEngine::Oracle, Some("lease_or"));
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_labels_postgres() {
    reset_takes_labels_and_never_another_exports_name(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_labels_mysql() {
    reset_takes_labels_and_never_another_exports_name(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_labels_mssql() {
    reset_takes_labels_and_never_another_exports_name(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_labels_oracle() {
    reset_takes_labels_and_never_another_exports_name(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_inspect_postgres() {
    inspect_refuses_a_typo_and_shows_a_held_partition(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_inspect_mysql() {
    inspect_refuses_a_typo_and_shows_a_held_partition(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_inspect_mssql() {
    inspect_refuses_a_typo_and_shows_a_held_partition(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_inspect_oracle() {
    inspect_refuses_a_typo_and_shows_a_held_partition(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_chunked_postgres() {
    reset_of_a_chunked_partitioned_export_leaves_another_sources_partition(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_chunked_mysql() {
    reset_of_a_chunked_partitioned_export_leaves_another_sources_partition(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_chunked_mssql() {
    reset_of_a_chunked_partitioned_export_leaves_another_sources_partition(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_chunked_oracle() {
    reset_of_a_chunked_partitioned_export_leaves_another_sources_partition(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_reset_granularity_postgres() {
    reset_after_a_granularity_change_takes_every_partition(SqlEngine::Pg, None);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn partition_reset_granularity_mysql() {
    reset_after_a_granularity_change_takes_every_partition(SqlEngine::Mysql, None);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn partition_reset_granularity_mssql() {
    reset_after_a_granularity_change_takes_every_partition(SqlEngine::Mssql, None);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_reset_granularity_oracle() {
    reset_after_a_granularity_change_takes_every_partition(SqlEngine::Oracle, None);
}

#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn partition_reset_granularity_pg_state_postgres() {
    reset_after_a_granularity_change_takes_every_partition(SqlEngine::Pg, Some("gran_pg"));
}

#[test]
#[ignore = "live: requires mysql + postgres-state"]
fn partition_reset_granularity_pg_state_mysql() {
    reset_after_a_granularity_change_takes_every_partition(SqlEngine::Mysql, Some("gran_my"));
}

#[test]
#[ignore = "live: requires mssql + postgres-state"]
fn partition_reset_granularity_pg_state_mssql() {
    reset_after_a_granularity_change_takes_every_partition(SqlEngine::Mssql, Some("gran_ms"));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires oracle + postgres-state"]
fn partition_reset_granularity_pg_state_oracle() {
    reset_after_a_granularity_change_takes_every_partition(SqlEngine::Oracle, Some("gran_or"));
}
