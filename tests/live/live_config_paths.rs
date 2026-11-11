//! A path a config names resolves against the config's directory, whatever directory rivet starts from.

use crate::common::*;
use std::path::Path;

/// Sorted ids of every part under `out`, in the column case the engine folds to.
fn delivered_ids(engine: SqlEngine, out: &Path) -> Vec<i64> {
    use arrow::array::{Array, Int64Array};
    let col = if engine.folds_upper() { "ID" } else { "id" };
    let mut ids = Vec::new();
    for b in read_all_parts(out) {
        let cast = arrow::compute::cast(
            b.column_by_name(col).expect("the id column"),
            &arrow::datatypes::DataType::Int64,
        )
        .expect("an integer id");
        let cast = cast.as_any().downcast_ref::<Int64Array>().unwrap();
        ids.extend((0..cast.len()).map(|i| cast.value(i)));
    }
    ids.sort();
    ids
}

/// The config's `url_file` names the stand; the working directory holds a same-named file naming a second database with 30 rows.
fn url_file_follows_the_config(engine: SqlEngine) {
    engine.alive();
    let other = engine.second_database("url_file_other");
    let (table, _guard) = engine.range_table("url_file_t");
    engine.insert(&table, 1..=10, 180, Some(10));
    other.table_with(&table, 1..=30);
    let cwd = tempfile::tempdir().unwrap();
    let rig = engine
        .rig(&table)
        .source_url_file("./db.url")
        .in_dir(cwd.path());
    let beside = rig.config_path().parent().unwrap().join("db.url");
    std::fs::write(&beside, engine.url()).unwrap();
    std::fs::write(cwd.path().join("db.url"), other.url()).unwrap();
    let run = rig.run();
    let got = delivered_ids(engine, &rig.out_dir());
    assert_eq!(
        got,
        (1..=10).collect::<Vec<_>>(),
        "the config's own database is delivered, exit {:?}:\n{}",
        run.status.code(),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(run.status.success());
}

/// Only the working directory holds the `url_file`: the run is refused naming both places, and moving the file delivers; with `old`, the release it resolves has delivered rows from there first and the cursor continues beside the config.
fn url_file_only_the_working_directory_holds(
    engine: SqlEngine,
    old: Option<fn(&Rig) -> Option<Rig>>,
) {
    engine.alive();
    let (table, _guard) = engine.range_table("url_file_moved");
    engine.insert(&table, 1..=10, 180, Some(10));
    let cwd = tempfile::tempdir().unwrap();
    let cursor = format!(
        "cursor_column: {}",
        if engine.folds_upper() { "ID" } else { "id" }
    );
    let rig = engine.rig(&table);
    let mut rig = if old.is_some() {
        rig.restage("incremental", &[cursor.as_str()])
    } else {
        rig
    }
    .source_url_file("./db.url")
    .in_dir(cwd.path());
    let beside = rig.config_path().parent().unwrap().join("db.url");
    let under_cwd = cwd.path().join("db.url");
    std::fs::write(&under_cwd, engine.url()).unwrap();
    let out = rig.out_dir();
    let state = state_url_under_test().map(|_| ScratchStateDb::new("url_file_moved"));
    let url = state.as_ref().map(ScratchStateDb::url);
    let envs: Vec<(&str, &str)> = url
        .iter()
        .map(|u| ("RIVET_STATE_URL", u.as_str()))
        .collect();
    let mut want = 10;
    if let Some(as_old) = old {
        let Some(previous) = as_old(&rig) else {
            return;
        };
        let run = previous.run_with_envs(&envs);
        assert!(
            run.status.success(),
            "the previous release: {}",
            String::from_utf8_lossy(&run.stderr)
        );
        assert_eq!(delivered_ids(engine, &out), (1..=10).collect::<Vec<_>>());
        engine.insert(&table, 11..=13, 170, Some(10));
        want = 13;
    }
    let refused = Refused::by_code("RIVET_CONFIG_SECRET_UNSET", 1);
    let text = rig.refuses_twice_and_walks_out(
        &["run"],
        &envs,
        refused,
        vec![
            Remedy::wrong(
                "runs `rivet validate`, which meets the same refusal",
                Then::Refuses(refused),
                |_| {},
            )
            .rerun_as(&["validate"])
            .in_place(),
            Remedy::new(
                &format!("move the file to {}", beside.display()),
                Then::DeliversTheSource,
                |_| {
                    assert!(!beside.exists(), "nothing beside the config yet");
                    std::fs::rename(&under_cwd, &beside).unwrap();
                },
            )
            .in_place(),
        ],
    );
    assert!(
        text.contains(under_cwd.to_str().unwrap()) && text.contains(beside.to_str().unwrap()),
        "the refusal names both places:\n{text}"
    );
    assert_eq!(delivered_ids(engine, &out), (1..=want).collect::<Vec<_>>());
}

/// A relative `url_file` run from the config's own directory, and an absolute one run from a directory holding a decoy `db.url`, read the file they name.
fn url_file_resolved_as_before(engine: SqlEngine) {
    engine.alive();
    let other = engine.second_database("url_file_decoy");
    let (table, _guard) = engine.range_table("url_file_same");
    engine.insert(&table, 1..=10, 180, Some(10));
    other.table_with(&table, 1..=30);
    let ids = |url_file: &dyn Fn(&Path) -> String, from_config_dir: bool| {
        let cwd = tempfile::tempdir().unwrap();
        let rig = engine.rig(&table);
        let dir = rig.config_path().parent().unwrap().to_path_buf();
        std::fs::write(dir.join("db.url"), engine.url()).unwrap();
        std::fs::write(cwd.path().join("db.url"), other.url()).unwrap();
        let rig = rig
            .source_url_file(&url_file(&dir))
            .in_dir(if from_config_dir {
                dir.as_path()
            } else {
                cwd.path()
            });
        let run = rig.run();
        assert!(
            run.status.success(),
            "{}",
            String::from_utf8_lossy(&run.stderr)
        );
        delivered_ids(engine, &rig.out_dir())
    };
    let first = (1..=10).collect::<Vec<_>>();
    assert_eq!(
        ids(&|_| "db.url".to_string(), true),
        first,
        "relative, from the config directory"
    );
    assert_eq!(
        ids(&|dir| dir.join("db.url").display().to_string(), false),
        first,
        "absolute, from another directory"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn url_file_resolved_as_before_postgres() {
    url_file_resolved_as_before(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn url_file_follows_the_config_postgres() {
    url_file_follows_the_config(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn url_file_resolved_as_before_mysql() {
    url_file_resolved_as_before(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn url_file_follows_the_config_mysql() {
    url_file_follows_the_config(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn url_file_resolved_as_before_mssql() {
    url_file_resolved_as_before(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn url_file_follows_the_config_mssql() {
    url_file_follows_the_config(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn url_file_resolved_as_before_oracle() {
    url_file_resolved_as_before(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn url_file_follows_the_config_oracle() {
    url_file_follows_the_config(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn url_file_only_the_working_directory_holds_postgres() {
    url_file_only_the_working_directory_holds(SqlEngine::Pg, None);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn url_file_only_the_working_directory_holds_mysql() {
    url_file_only_the_working_directory_holds(SqlEngine::Mysql, None);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn url_file_only_the_working_directory_holds_mssql() {
    url_file_only_the_working_directory_holds(SqlEngine::Mssql, None);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn url_file_only_the_working_directory_holds_oracle() {
    url_file_only_the_working_directory_holds(SqlEngine::Oracle, None);
}

#[test]
#[ignore = "live: requires docker compose postgres and RIVET_MIGRATION_CARRIER_BIN"]
fn a_url_file_moved_by_upgrading_the_pinned_release_is_refused_until_the_file_follows() {
    url_file_only_the_working_directory_holds(SqlEngine::Pg, Some(Rig::as_migration_carrier));
}
