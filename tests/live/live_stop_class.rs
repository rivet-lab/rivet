//! The exit class of a stop (docs/operator-contract-matrix.yaml, seam stop-class): a coded stop exits by its
//! code and by the typed verdict of its cause, never by a word in a table or column name the operator chose.
//! A cell asserts the correct behaviour.
//!
//! Oracles: the exit and `Error:` line of the real binary; the relation names are the operator's.

use crate::common::*;
use crate::live_mssql_incremental_isolation::{server_wait, wait_visible};

const MONGO_PORT: u16 = 27017;
const NAMED_FOR_A_FAILURE: [&str; 3] = ["login_timeouts", "dns_zones", "deadlock_log"];
const NAMED_PLAINLY: [&str; 2] = ["users", "orders_archive"];
const COLUMNS_NAMED_FOR_A_FAILURE: [&str; 3] = ["login_timeout", "dns_cache", "deadlock_flag"];
const COLUMNS_NAMED_PLAINLY: [&str; 2] = ["amount", "region_code"];

/// Run `check`; a panic in it is recorded in `gaps` under `label`, its first line only.
fn probe(gaps: &mut Vec<String>, label: &str, check: impl FnOnce()) {
    if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(check)) {
        let said = panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        gaps.push(format!("{label}: {}", said.lines().next().unwrap_or("")));
    }
}

/// Panic with the count of failed probes on the first line and each failure under it.
fn no_gaps(gaps: Vec<String>, probes: usize, what: &str) {
    if !gaps.is_empty() {
        panic!(
            "{} of {probes} {what} exit by the words of the name\n{}",
            gaps.len(),
            gaps.join("\n")
        );
    }
}

/// A relation name that holds no table: the word, a counter and nothing else.
fn absent_name(word: &str) -> String {
    unique_name(word)
}

/// Every name of `names` that no table holds is refused by `code` with exit 1, whatever words it holds.
fn missing_relation_exits_by_its_code(engine: SqlEngine, names: &[&str]) {
    engine.alive();
    let mut gaps = Vec::new();
    for word in names {
        let name = absent_name(word);
        probe(&mut gaps, word, || {
            let out = engine.rig(&name).run();
            assert_refused(&out, Refused::by_code("RIVET_SOURCE_TABLE_NOT_FOUND", 1));
        });
    }
    no_gaps(gaps, names.len(), "absent relation names");
}

/// Every column of `words` that the table does not hold fails the read with exit 1 whatever words it holds; SQL Server and Oracle also carry `RIVET_SOURCE_QUERY_FAILED`.
fn missing_column_exits_by_its_code(engine: SqlEngine, words: &[&str]) {
    engine.alive();
    let (table, _guard) = engine.table("sc_col");
    let mut gaps = Vec::new();
    for word in words {
        probe(&mut gaps, word, || {
            let column = engine.col(word);
            let out = engine
                .rig("sc_col")
                .query(&format!("SELECT {column} FROM {table}"))
                .run();
            if matches!(engine, SqlEngine::Pg | SqlEngine::Mysql) {
                assert_eq!(out.status.code(), Some(1), "{column}");
            } else {
                assert_refused(&out, Refused::by_code("RIVET_SOURCE_QUERY_FAILED", 1));
            }
        });
    }
    no_gaps(gaps, words.len(), "absent column names");
}

/// [`missing_relation_exits_by_its_code`] over a MongoDB collection.
fn missing_collection_exits_by_its_code(names: &[&str]) {
    require_alive(LiveService::Mongo);
    let db = unique_name("sc_absent");
    let _guard = MongoDbGuard {
        port: MONGO_PORT,
        db: db.clone(),
    };
    let m = MongoTest::connect(MONGO_PORT, &db);
    m.seed_int_id("t", 1);
    let mut gaps = Vec::new();
    for word in names {
        probe(&mut gaps, word, || {
            let out = Rig::mongo_batch(word)
                .source_url(&MongoTest::url(MONGO_PORT, &db))
                .run();
            assert_refused(
                &out,
                Refused::by_code("RIVET_SOURCE_COLLECTION_NOT_FOUND", 1),
            );
        });
    }
    no_gaps(gaps, names.len(), "absent collection names");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_missing_relation_exits_by_its_code_whatever_its_name_holds_postgres() {
    missing_relation_exits_by_its_code(SqlEngine::Pg, &NAMED_FOR_A_FAILURE);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_missing_relation_exits_by_its_code_whatever_its_name_holds_mysql() {
    missing_relation_exits_by_its_code(SqlEngine::Mysql, &NAMED_FOR_A_FAILURE);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_missing_relation_exits_by_its_code_whatever_its_name_holds_mssql() {
    missing_relation_exits_by_its_code(SqlEngine::Mssql, &NAMED_FOR_A_FAILURE);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_missing_relation_exits_by_its_code_whatever_its_name_holds_oracle() {
    missing_relation_exits_by_its_code(SqlEngine::Oracle, &NAMED_FOR_A_FAILURE);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_missing_relation_exits_by_its_code_whatever_its_name_holds_mongo() {
    missing_collection_exits_by_its_code(&NAMED_FOR_A_FAILURE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_missing_relation_named_plainly_exits_by_its_code_postgres() {
    missing_relation_exits_by_its_code(SqlEngine::Pg, &NAMED_PLAINLY);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_missing_relation_named_plainly_exits_by_its_code_mysql() {
    missing_relation_exits_by_its_code(SqlEngine::Mysql, &NAMED_PLAINLY);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_missing_relation_named_plainly_exits_by_its_code_mssql() {
    missing_relation_exits_by_its_code(SqlEngine::Mssql, &NAMED_PLAINLY);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_missing_relation_named_plainly_exits_by_its_code_oracle() {
    missing_relation_exits_by_its_code(SqlEngine::Oracle, &NAMED_PLAINLY);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_missing_collection_named_plainly_exits_by_its_code_mongo() {
    missing_collection_exits_by_its_code(&NAMED_PLAINLY);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_missing_column_exits_by_its_code_whatever_its_name_holds_postgres() {
    missing_column_exits_by_its_code(SqlEngine::Pg, &COLUMNS_NAMED_FOR_A_FAILURE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_missing_column_named_plainly_exits_by_its_code_postgres() {
    missing_column_exits_by_its_code(SqlEngine::Pg, &COLUMNS_NAMED_PLAINLY);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_missing_column_exits_by_its_code_whatever_its_name_holds_mysql() {
    missing_column_exits_by_its_code(SqlEngine::Mysql, &COLUMNS_NAMED_FOR_A_FAILURE);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_missing_column_named_plainly_exits_by_its_code_mysql() {
    missing_column_exits_by_its_code(SqlEngine::Mysql, &COLUMNS_NAMED_PLAINLY);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_missing_column_exits_by_its_code_whatever_its_name_holds_mssql() {
    missing_column_exits_by_its_code(SqlEngine::Mssql, &COLUMNS_NAMED_FOR_A_FAILURE);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_missing_column_named_plainly_exits_by_its_code_mssql() {
    missing_column_exits_by_its_code(SqlEngine::Mssql, &COLUMNS_NAMED_PLAINLY);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_missing_column_exits_by_its_code_whatever_its_name_holds_oracle() {
    missing_column_exits_by_its_code(SqlEngine::Oracle, &COLUMNS_NAMED_FOR_A_FAILURE);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_missing_column_named_plainly_exits_by_its_code_oracle() {
    missing_column_exits_by_its_code(SqlEngine::Oracle, &COLUMNS_NAMED_PLAINLY);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn an_mssql_read_blocked_past_its_lock_timeout_exits_retryable() {
    let table = format!("rivet.dbo.{}", unique_name("sc_lock"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL);\nINSERT INTO {table} VALUES (1,1);"
    ));
    let _guard = MssqlTable::adopt(table.clone());
    let release = format!("rivet.dbo.{}", unique_name("sc_lock_go"));
    mssql_exec(&format!("CREATE TABLE {release} (id BIGINT);"));
    let _release_guard = MssqlTable::adopt(release.clone());
    let rig = Rig::mssql_batch("sc_lock")
        .query(&format!("SELECT id, v FROM {table}"))
        .source_line("tuning:")
        .source_line("  lock_timeout_s: 1");
    let blocked = "EXISTS (SELECT 1 FROM sys.dm_exec_requests WHERE blocking_session_id = @@SPID)";
    let tx = format!(
        "SET XACT_ABORT ON; DECLARE @deadline DATETIME2;\n\
         BEGIN TRAN; UPDATE {table} SET v = 2 WHERE id = 1;\n{}{}COMMIT;",
        server_wait(
            blocked,
            60,
            "fixture: rivet's read never blocked on the held row lock"
        ),
        server_wait(
            &format!("EXISTS (SELECT 1 FROM {release} WITH (NOLOCK))"),
            120,
            "fixture: the run never ended while the lock was held"
        ),
    );
    let writer = std::thread::spawn(move || mssql_exec_once(&tx));
    wait_visible(
        &format!("SELECT COUNT(*) FROM {table} WITH (NOLOCK) WHERE v = 2"),
        "the writer's row lock",
    );
    let out = rig.run();
    mssql_exec(&format!("INSERT INTO {release} VALUES (1);"));
    let said = String::from_utf8_lossy(&out.stderr).to_string();
    let held = writer.join().expect("writer thread");
    held.unwrap_or_else(|e| panic!("{e}"));
    assert!(
        said.contains("Lock request time out period exceeded"),
        "{said}"
    );
    assert_eq!(
        out.status.code(),
        Some(2),
        "a lock wait that timed out clears when the lock does: retryable\n{said}"
    );
}
