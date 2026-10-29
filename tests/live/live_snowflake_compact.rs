//! `rivet compact` on Snowflake: the buffer folds into the base inside one transaction, graded by the rig's DuckDB reader and the source, never by rivet's counters.

use crate::common::*;

fn said_of(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn load_ok(rig: &Rig) -> String {
    let out = rig.cli(&["load"]);
    let said = said_of(&out);
    assert!(out.status.success(), "rivet load failed:\n{said}");
    said
}

fn compact_ok(rig: &Rig) -> String {
    let out = rig.cli(&["compact"]);
    let said = said_of(&out);
    assert!(out.status.success(), "rivet compact failed:\n{said}");
    said
}

fn num(v: &serde_json::Value) -> i64 {
    match v {
        serde_json::Value::String(s) => s.parse().expect("a number"),
        other => other.as_i64().expect("a number"),
    }
}

/// `(rows, live rows, flagged rows, NULL flags, SUM(key) over live rows)` of the base.
fn base_profile(sf: &SfLive, table: &str, key: &str) -> (i64, i64, i64, i64, i64) {
    let r = &sf.read(&format!(
        "SELECT COUNT(*), COUNT_IF(NOT __is_deleted), COUNT_IF(__is_deleted), \
         COUNT_IF(__is_deleted IS NULL), COALESCE(SUM(IFF(__is_deleted, 0, {key})), 0) FROM {}",
        sf.fq(table)
    ))[0];
    (num(&r[0]), num(&r[1]), num(&r[2]), num(&r[3]), num(&r[4]))
}

/// Every `(key, value, flag)` of the base, ordered by key: the second client's whole read.
fn base_rows(sf: &SfLive, table: &str, key: &str, value: &str) -> Vec<(i64, Option<i64>, bool)> {
    sf.read(&format!(
        "SELECT {key}, {value}, __is_deleted FROM {} ORDER BY 1",
        sf.fq(table)
    ))
    .iter()
    .map(|r| {
        (
            num(&r[0]),
            (!r[1].is_null()).then(|| num(&r[1])),
            r[2].as_bool()
                .unwrap_or_else(|| r[2].as_str() == Some("true")),
        )
    })
    .collect()
}

fn is_transient(sf: &SfLive, table: &str) -> Option<bool> {
    sf.read(&format!(
        "SELECT is_transient FROM {}.INFORMATION_SCHEMA.TABLES WHERE table_schema = '{}' AND table_name = '{}'",
        sf.database(),
        sf.schema().to_uppercase(),
        table.to_uppercase()
    ))
    .first()
    .map(|r| r[0].as_str() == Some("YES"))
}

fn shape(rig: Rig, table: &str, pk: &str, sf: &SfLive) -> Rig {
    rig.cdc("backfill: auto")
        .also_batch_export("baseline", table, "full")
        .dest_gcs_live(&sf.bucket, &sf.prefix)
        .top_line(&sf.load_line(&format!(", pk: [{pk}]")))
}

/// Baseline, a delta of inserts, an update and a delete, a key repeated in the buffer, an insert deleted in the same buffer, then a replay of an already merged buffer from `__merging`: the base ends as the source, once.
fn snowflake_compact_cycle(mut scn: CdcScenario, key: &str, value: &str, sf: &SfLive) {
    let table = scn.table.clone();
    let changes = format!("{table}__changes");
    let merging = format!("{table}__changes__merging");
    let saved = format!("{table}__saved");
    let _cleanup = sf.cleanup(&[&table, &saved]);
    for id in 1..=5 {
        scn.insert(id);
    }
    scn.settle();

    scn.rig.run_ok();
    let said = load_ok(&scn.rig);
    assert!(said.contains("layout=base+buffer"), "{said}");
    assert_eq!(sf.kind(&table).as_deref(), Some("BASE TABLE"));
    assert_eq!(
        base_profile(sf, &table, key),
        (5, 5, 0, 0, 15),
        "five live rows, no NULL flag"
    );
    compact_ok(&scn.rig);
    assert_eq!(
        base_profile(sf, &table, key),
        (5, 5, 0, 0, 15),
        "a baseline the stream re-captured merges into the same five keys"
    );
    assert_eq!(sf.kind(&changes), None, "no buffer survives a compact");

    for id in 6..=8 {
        scn.insert(id);
    }
    scn.update(1);
    scn.delete(2);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(sf.counts(&changes, "__seq").0, 5, "the five changes wait");
    assert_eq!(
        is_transient(sf, &changes),
        Some(true),
        "the buffer is disposable"
    );
    assert_eq!(
        base_profile(sf, &table, key).0,
        5,
        "the base waits for compact"
    );
    sf.exec(&format!(
        "CREATE TABLE {} AS SELECT * FROM {}",
        sf.fq(&saved),
        sf.fq(&changes)
    ));

    let said = compact_ok(&scn.rig);
    assert!(
        said.contains("COMPACT OK") && said.contains("5 change row(s)"),
        "{said}"
    );
    let (n, live, gone, nulls, sum) = base_profile(sf, &table, key);
    assert_eq!(
        (n, gone, nulls),
        (8, 1, 0),
        "nothing deleted, the delete flagged"
    );
    assert_eq!(live, scn.count(), "live rows == source");
    assert_eq!(sum, 1 + 3 + 4 + 5 + 6 + 7 + 8);
    let rows = base_rows(sf, &table, key, value);
    assert!(
        rows.contains(&(1, Some(99), false)),
        "the update landed: {rows:?}"
    );
    assert!(
        rows.contains(&(2, Some(2), true)),
        "the tombstone keeps its values: {rows:?}"
    );
    assert_eq!(sf.kind(&changes), None, "the buffer is dropped");
    assert_eq!(sf.kind(&merging), None, "nothing is left over");

    sf.exec(&format!(
        "CREATE TABLE {} AS SELECT * FROM {}",
        sf.fq(&merging),
        sf.fq(&saved)
    ));
    let said = compact_ok(&scn.rig);
    assert!(
        said.contains("COMPACT OK"),
        "the leftover is merged: {said}"
    );
    assert_eq!(
        base_rows(sf, &table, key, value),
        rows,
        "merging a buffer again changes nothing"
    );
    assert_eq!(sf.kind(&merging), None, "the leftover is dropped");

    scn.insert(10);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    scn.update(10);
    scn.insert(11);
    scn.delete(11);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    let said = compact_ok(&scn.rig);
    assert!(said.contains("COMPACT OK"), "{said}");
    let rows = base_rows(sf, &table, key, value);
    assert!(
        rows.contains(&(10, Some(99), false)),
        "the later change won: {rows:?}"
    );
    assert!(
        rows.iter().all(|r| r.0 != 11),
        "an insert deleted inside one buffer never reaches the base: {rows:?}"
    );
    let (n, live, gone, nulls, _) = base_profile(sf, &table, key);
    assert_eq!((n, gone, nulls), (9, 1, 0));
    assert_eq!(live, scn.count(), "live rows == source");
    assert!(
        compact_ok(&scn.rig).contains("COMPACT SKIP"),
        "a second compact is a no-op"
    );
}

/// A baseline staged under `layout: log_view` carries no `__is_deleted`: loaded into a base under the default layout, every row lands live.
fn a_baseline_staged_without_the_flag_lands_live(
    mut scn: CdcScenario,
    pk: &str,
    key: &str,
    sf: &SfLive,
) {
    let table = scn.table.clone();
    let _cleanup = sf.cleanup(&[&table]);
    for id in 1..=5 {
        scn.insert(id);
    }
    scn.settle();
    scn.rig
        .set_top_line(&sf.load_line(&format!(", pk: [{pk}], layout: log_view")));
    scn.rig.run_ok();
    scn.rig
        .set_top_line(&sf.load_line(&format!(", pk: [{pk}]")));
    let said = load_ok(&scn.rig);
    assert!(said.contains("layout=base+buffer"), "{said}");
    assert_eq!(
        base_profile(sf, &table, key),
        (5, 5, 0, 0, 15),
        "five live rows, no NULL flag"
    );
}

#[test]
#[ignore = "live: requires postgres-cdc + Snowflake creds"]
fn snowflake_baseline_without_the_flag_lands_live_postgres() {
    let Some(sf) = SfLive::from_env("sfb_pg") else {
        return;
    };
    let scn = CdcScenario::pg_with("sfb_pg", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        shape(r, t, "id", &sf)
    });
    a_baseline_staged_without_the_flag_lands_live(scn, "id", "id", &sf);
}

#[test]
#[ignore = "live: requires mysql-cdc + Snowflake creds"]
fn snowflake_baseline_without_the_flag_lands_live_mysql() {
    let Some(sf) = SfLive::from_env("sfb_my") else {
        return;
    };
    let scn = CdcScenario::mysql_with("sfb_my", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        shape(r, t, "id", &sf)
    });
    a_baseline_staged_without_the_flag_lands_live(scn, "id", "id", &sf);
}

#[test]
#[ignore = "live: requires mssql-cdc + Snowflake creds"]
fn snowflake_baseline_without_the_flag_lands_live_mssql() {
    let Some(sf) = SfLive::from_env("sfb_ms") else {
        return;
    };
    let scn = CdcScenario::mssql_with("sfb_ms", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        shape(r, t, "id", &sf)
    });
    a_baseline_staged_without_the_flag_lands_live(scn, "id", "id", &sf);
}

#[test]
#[ignore = "live: requires mongo-rs + Snowflake creds"]
fn snowflake_baseline_without_the_flag_lands_live_mongo() {
    let Some(sf) = SfLive::from_env("sfb_mg") else {
        return;
    };
    require_alive(LiveService::MongoRs);
    let scn = CdcScenario::mongo_with("sfb_mg", |r, t| shape(r, t, "_id", &sf));
    a_baseline_staged_without_the_flag_lands_live(scn, "_id", "CAST(_id AS INT)", &sf);
}

#[test]
#[ignore = "live: requires postgres-cdc + Snowflake creds"]
fn snowflake_compact_cycle_postgres() {
    let Some(sf) = SfLive::from_env("sfc_pg") else {
        return;
    };
    let scn = CdcScenario::pg_with("sfc_pg", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        shape(r, t, "id", &sf)
    });
    snowflake_compact_cycle(scn, "id", "v", &sf);
}

#[test]
#[ignore = "live: requires mysql-cdc + Snowflake creds"]
fn snowflake_compact_cycle_mysql() {
    let Some(sf) = SfLive::from_env("sfc_my") else {
        return;
    };
    let scn = CdcScenario::mysql_with("sfc_my", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        shape(r, t, "id", &sf)
    });
    snowflake_compact_cycle(scn, "id", "v", &sf);
}

#[test]
#[ignore = "live: requires mssql-cdc + Snowflake creds"]
fn snowflake_compact_cycle_mssql() {
    let Some(sf) = SfLive::from_env("sfc_ms") else {
        return;
    };
    let scn = CdcScenario::mssql_with("sfc_ms", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        shape(r, t, "id", &sf)
    });
    snowflake_compact_cycle(scn, "id", "v", &sf);
}

#[test]
#[ignore = "live: requires mongo-rs + Snowflake creds"]
fn snowflake_compact_cycle_mongo() {
    let Some(sf) = SfLive::from_env("sfc_mg") else {
        return;
    };
    require_alive(LiveService::MongoRs);
    let scn = CdcScenario::mongo_with("sfc_mg", |r, t| shape(r, t, "_id", &sf));
    snowflake_compact_cycle(scn, "CAST(_id AS INT)", "CAST(document:v AS INT)", &sf);
}

/// An incremental export under the default layout: the first cycle lands the base, later cycles fill the buffer, and `rivet compact` folds it in.
fn an_incremental_buffer_compacts_into_its_base(e: SqlEngine) {
    let Some(sf) = SfLive::from_env("sfc_inc") else {
        return;
    };
    e.alive();
    let (table, _guard) = e.table("sfc_inc");
    let changes = format!("{table}__changes");
    let _cleanup = sf.cleanup(&[&table]);
    e.insert(&table, 1..=10, 10, Some(1));
    let rig = e
        .rig(&table)
        .restage("incremental", &["cursor_column: id"])
        .dest_gcs_live(&sf.bucket, &sf.prefix)
        .top_line(&sf.load_line(", pk: [id]"));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(sf.kind(&table).as_deref(), Some("BASE TABLE"));
    assert_eq!(sf.kind(&changes), None, "no buffer yet");

    e.insert(&table, 11..=13, 5, Some(2));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(
        sf.kind(&table).as_deref(),
        Some("BASE TABLE"),
        "no view over a buffer"
    );
    assert_eq!(sf.counts(&changes, "id"), (3, 3));
    assert_eq!(
        sf.counts(&table, "id"),
        (10, 10),
        "the base waits for compact"
    );

    let said = compact_ok(&rig);
    assert!(
        said.contains("COMPACT OK") && said.contains("3 change row(s)"),
        "{said}"
    );
    assert_eq!(sf.counts(&table, "id"), (13, 13));
    assert_eq!(sf.kind(&changes), None);
    assert!(compact_ok(&rig).contains("COMPACT SKIP"));
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + Snowflake creds"]
fn snowflake_incremental_buffer_compacts_into_its_base_postgres() {
    an_incremental_buffer_compacts_into_its_base(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql + Snowflake creds"]
fn snowflake_incremental_buffer_compacts_into_its_base_mysql() {
    an_incremental_buffer_compacts_into_its_base(SqlEngine::Mysql);
}

/// A log+view table's view under the base name stops `rivet load` and `rivet compact` once the default layout is back: the coded conflict names `load.layout: log_view`, and the view and the log are as they were.
fn a_view_under_the_base_name_stops_load_and_compact(e: SqlEngine) {
    let Some(sf) = SfLive::from_env("sfc_view") else {
        return;
    };
    e.alive();
    let (table, _guard) = e.table("sfc_view");
    let changes = format!("{table}__changes");
    let _cleanup = sf.cleanup(&[&table]);
    e.insert(&table, 1..=10, 10, Some(1));
    let rig = e
        .rig(&table)
        .restage("incremental", &["cursor_column: id"])
        .dest_gcs_live(&sf.bucket, &sf.prefix)
        .top_line(&sf.load_line(", pk: [id], layout: log_view"));
    rig.run_ok();
    load_ok(&rig);
    e.insert(&table, 11..=13, 5, Some(2));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(sf.kind(&table).as_deref(), Some("VIEW"));
    assert_eq!(sf.counts(&changes, "id"), (13, 13));

    let rig = rig.clear_top_lines().top_line(&sf.load_line(", pk: [id]"));
    e.insert(&table, 14..=15, 5, Some(3));
    rig.run_ok();
    for cmd in ["load", "compact"] {
        let out = rig.cli(&[cmd]);
        assert_refused(
            &out,
            Refused::by_code("RIVET_LOAD_TARGET_OBJECT_CONFLICT", 5),
        );
        assert!(
            said_of(&out).contains("`load.layout: log_view`"),
            "{cmd}: the remedy is named:\n{}",
            said_of(&out)
        );
        assert_eq!(
            sf.kind(&table).as_deref(),
            Some("VIEW"),
            "{cmd} left the view alone"
        );
        assert_eq!(
            sf.counts(&changes, "id"),
            (13, 13),
            "{cmd} wrote nothing to the log"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + Snowflake creds"]
fn snowflake_view_under_the_base_name_stops_load_and_compact_postgres() {
    a_view_under_the_base_name_stops_load_and_compact(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql + Snowflake creds"]
fn snowflake_view_under_the_base_name_stops_load_and_compact_mysql() {
    a_view_under_the_base_name_stops_load_and_compact(SqlEngine::Mysql);
}
