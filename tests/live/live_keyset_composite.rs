//! Keyset paging over a composite primary key `(id, kind)` on every SQL engine: one id spans several pages, parallel workers split the key by its lead column, and a crash resumes to an exact destination.

use crate::common::*;

const PAGE: usize = 20;
const IDS: usize = 30;
const WIDE_ID: usize = 7;
const WIDE_KINDS: usize = 50;

/// The `(id, kind)` rows of the seed: uuid-v7-shaped ids, one id with more kinds than a page, a kind with a quote.
fn seed_rows() -> Vec<(String, String)> {
    let mut rows = Vec::new();
    for i in 0..IDS {
        let id = format!("0190{i:04x}-1c2d-7e3f-8a4b-{:012x}", i * 7919);
        let kinds = if i == WIDE_ID { WIDE_KINDS } else { i % 3 + 1 };
        for k in 0..kinds {
            let kind = if k == 1 {
                "it's".to_string()
            } else {
                format!("k{k:02}")
            };
            rows.push((id.clone(), kind));
        }
    }
    rows
}

/// A seeded composite-key table of `e` and its drop guard.
fn composite_table(e: SqlEngine, prefix: &str) -> (String, Box<dyn std::any::Any>, usize) {
    let (id, kind) = match e {
        SqlEngine::Pg => ("uuid", "varchar(32)"),
        SqlEngine::Mysql => ("char(36)", "varchar(32)"),
        SqlEngine::Mssql => ("char(36)", "nvarchar(32)"),
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => ("varchar2(36)", "varchar2(32)"),
    };
    let (table, guard) = e.create(
        prefix,
        &format!("id {id} NOT NULL, kind {kind} NOT NULL, v INT NOT NULL, PRIMARY KEY (id, kind)"),
    );
    let rows = seed_rows();
    let values: Vec<String> = rows
        .iter()
        .enumerate()
        .map(|(n, (id, kind))| format!("('{id}', '{}', {n})", kind.replace('\'', "''")))
        .collect();
    e.exec(&format!(
        "INSERT INTO {table} (id, kind, v) VALUES {}",
        values.join(", ")
    ));
    (table, guard, rows.len())
}

/// DuckDB over the declared parts: the row count when every `(id, kind)` is distinct, else -1.
const ROWS_ONCE: &str = "CASE WHEN count(*) = count(DISTINCT (id, kind)) THEN count(*) ELSE -1 END";

/// A chunked export keyed by `[id, kind]` pages the whole table, the wide id across pages, every row once.
fn composite_key_pages_every_row_once(e: SqlEngine) {
    e.alive();
    let (table, _guard, rows) = composite_table(e, "ck_pages");
    let rig = e.staged(
        e.rig(&table),
        "chunked",
        &["chunk_by_key: [id, kind]", &format!("chunk_size: {PAGE}")],
    );
    rig.run_ok();
    assert!(
        files_with_extension(&rig.out_dir(), "parquet").len() > rows / PAGE,
        "a page per {PAGE} rows: the export did not page by the composite key"
    );
    assert_eq!(
        duckdb_declared_dir_scalar(&rig.out_dir(), ROWS_ONCE),
        rows as i64,
        "every seeded row once in the destination"
    );
}

/// A checkpointed composite-key export that crashes after its second page resumes to every row once.
fn composite_key_resume_after_a_crash_is_exact(e: SqlEngine) {
    e.alive();
    let (table, _guard, rows) = composite_table(e, "ck_resume");
    let rig = e.staged(
        e.rig(&table),
        "chunked",
        &[
            "chunk_by_key: [id, kind]",
            "chunk_checkpoint: true",
            &format!("chunk_size: {PAGE}"),
        ],
    );
    let crash = rig.run_args_env(&[], &[("RIVET_TEST_PANIC_AT", "after_keyset_page:1")]);
    assert!(
        !crash.status.success(),
        "the crash run must fail; stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );
    rig.run_ok();
    assert_eq!(
        duckdb_declared_dir_scalar(&rig.out_dir(), ROWS_ONCE),
        rows as i64,
        "every seeded row once in the destination after the resume"
    );
}

/// Distinct parallel range workers that wrote the parts in `dir`.
fn worker_ranges(dir: &std::path::Path) -> usize {
    files_with_extension(dir, "parquet")
        .iter()
        .filter_map(|p| {
            p.file_name()?
                .to_str()?
                .split("_pk_w")
                .nth(1)?
                .split('_')
                .next()
                .map(String::from)
        })
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

/// Four workers over `[id, kind]` split the table by `id`, the wide id whole in one range, every row once.
fn composite_key_parallel_reads_every_row_once(e: SqlEngine) {
    e.alive();
    let (table, _guard, rows) = composite_table(e, "ck_par");
    let rig = e.staged(
        e.rig(&table),
        "chunked",
        &[
            "chunk_by_key: [id, kind]",
            "parallel: 4",
            &format!("chunk_size: {PAGE}"),
        ],
    );
    rig.run_ok();
    assert!(
        worker_ranges(&rig.out_dir()) > 1,
        "parallel: 4 over the composite key ran as one worker"
    );
    assert_eq!(
        duckdb_declared_dir_scalar(&rig.out_dir(), ROWS_ONCE),
        rows as i64,
        "every seeded row once across the parallel ranges"
    );
}

/// A checkpointed parallel composite-key export that crashes after its first range commits resumes to every row once.
fn composite_key_parallel_resume_after_a_crash_is_exact(e: SqlEngine) {
    e.alive();
    let (table, _guard, rows) = composite_table(e, "ck_parres");
    let rig = e.staged(
        e.rig(&table),
        "chunked",
        &[
            "chunk_by_key: [id, kind]",
            "parallel: 4",
            "chunk_checkpoint: true",
            &format!("chunk_size: {PAGE}"),
        ],
    );
    let crash = rig.run_args_env(
        &[],
        &[("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:0")],
    );
    assert!(
        !crash.status.success(),
        "the crash run must fail; stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );
    rig.run_ok();
    assert_eq!(
        duckdb_declared_dir_scalar(&rig.out_dir(), ROWS_ONCE),
        rows as i64,
        "every seeded row once in the destination after the parallel resume"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn composite_key_pages_every_row_once_postgres() {
    composite_key_pages_every_row_once(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn composite_key_pages_every_row_once_mysql() {
    composite_key_pages_every_row_once(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn composite_key_pages_every_row_once_mssql() {
    composite_key_pages_every_row_once(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn composite_key_pages_every_row_once_oracle() {
    composite_key_pages_every_row_once(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn composite_key_resume_after_a_crash_is_exact_postgres() {
    composite_key_resume_after_a_crash_is_exact(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn composite_key_resume_after_a_crash_is_exact_mysql() {
    composite_key_resume_after_a_crash_is_exact(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn composite_key_resume_after_a_crash_is_exact_mssql() {
    composite_key_resume_after_a_crash_is_exact(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn composite_key_resume_after_a_crash_is_exact_oracle() {
    composite_key_resume_after_a_crash_is_exact(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn composite_key_parallel_reads_every_row_once_postgres() {
    composite_key_parallel_reads_every_row_once(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn composite_key_parallel_reads_every_row_once_mysql() {
    composite_key_parallel_reads_every_row_once(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn composite_key_parallel_reads_every_row_once_mssql() {
    composite_key_parallel_reads_every_row_once(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn composite_key_parallel_reads_every_row_once_oracle() {
    composite_key_parallel_reads_every_row_once(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn composite_key_parallel_resume_after_a_crash_is_exact_postgres() {
    composite_key_parallel_resume_after_a_crash_is_exact(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn composite_key_parallel_resume_after_a_crash_is_exact_mysql() {
    composite_key_parallel_resume_after_a_crash_is_exact(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn composite_key_parallel_resume_after_a_crash_is_exact_mssql() {
    composite_key_parallel_resume_after_a_crash_is_exact(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn composite_key_parallel_resume_after_a_crash_is_exact_oracle() {
    composite_key_parallel_resume_after_a_crash_is_exact(SqlEngine::Oracle);
}
