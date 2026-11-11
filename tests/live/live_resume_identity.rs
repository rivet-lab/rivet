//! A crashed checkpointed run is resumed only under the shape it was opened with: every byte-shaping field edited between the crash and the resume is refused, on every runner.

use crate::common::*;
use std::path::Path;

const SHAPE_CODE: &str = "RIVET_STATE_RUN_SHAPE_CHANGED";

/// One checkpointed runner: its stage, where its first run crashes and the state subcommand that abandons it.
struct Runner {
    mode: &'static str,
    lines: &'static [&'static str],
    crash_at: &'static str,
    abandon: &'static str,
}

const RANGE_CHUNKED: Runner = Runner {
    mode: "chunked",
    lines: &[
        "chunk_column: id",
        "chunk_size: 4",
        "chunk_checkpoint: true",
    ],
    crash_at: "after_chunk_complete:0",
    abandon: "reset-chunks",
};
const KEYSET: Runner = Runner {
    mode: "chunked",
    lines: &[
        "chunk_by_key: id",
        "chunk_size: 4",
        "chunk_checkpoint: true",
    ],
    crash_at: "after_keyset_page:0",
    abandon: "reset",
};
const PARALLEL_KEYSET: Runner = Runner {
    mode: "chunked",
    lines: &[
        "chunk_by_key: id",
        "chunk_size: 4",
        "parallel: 2",
        "chunk_checkpoint: true",
    ],
    crash_at: "keyset_parallel_range_committed:0",
    abandon: "reset",
};

/// One edit of a byte-shaping field: the config key the refusal names and the export lines that make the edit.
struct Edit {
    key: &'static str,
    lines: &'static [&'static str],
}

const COMPRESSION: Edit = Edit {
    key: "compression",
    lines: &["compression: snappy"],
};
const COLUMNS: Edit = Edit {
    key: "columns",
    lines: &["columns:", "  time_spent: int32"],
};
const META_COLUMNS: Edit = Edit {
    key: "meta_columns",
    lines: &["meta_columns:", "  exported_at: true"],
};

enum Remedy {
    Revert,
    Reset,
}

/// Part files under `out`, by name.
fn part_names(out: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(out)
        .map(|d| {
            d.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|n| n.ends_with(".parquet"))
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn edited(engine: SqlEngine, rig: Rig, runner: &Runner, edit: &Edit, out: &Path) -> Rig {
    let mut rig = engine
        .staged(rig, runner.mode, runner.lines)
        .dest_path(out.to_path_buf());
    for line in edit.lines {
        let line = match engine.folds_upper() {
            true => line.replace("time_spent", "TIME_SPENT"),
            false => line.to_string(),
        };
        rig = rig.export_line(&line);
    }
    rig
}

/// A checkpointed run crashes with a part delivered, one byte-shaping field is edited, and the resume is refused twice with nothing written; after `remedy` the run completes.
fn crashed_then_field_edited(engine: SqlEngine, runner: Runner, edit: Edit, remedy: Remedy) {
    engine.alive();
    let (table, _guard) = engine.range_table("resume_identity");
    engine.insert(&table, 1..=10, 180, Some(10));
    engine.refresh_row_estimate(&table);
    let (crashed, fresh) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let run = engine
        .staged(engine.rig(&table), runner.mode, runner.lines)
        .dest_path(crashed.path().to_path_buf());
    let crash = run.run_with_env("RIVET_TEST_PANIC_AT", runner.crash_at);
    assert!(!crash.status.success(), "the first run must crash");
    let parts = part_names(crashed.path());
    assert!(
        !parts.is_empty(),
        "the crashed run delivered a part first:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );

    let changed = edited(engine, run, &runner, &edit, crashed.path());
    for cycle in 1..=2 {
        let o = changed.run_args(&["--resume"]);
        let said = String::from_utf8_lossy(&o.stderr).to_string();
        assert_eq!(
            o.status.code(),
            Some(5),
            "cycle {cycle}: the resume under an edited `{}` must be refused:\n{said}",
            edit.key
        );
        let command = format!(
            "rivet state {} -c <config> --export {table}",
            runner.abandon
        );
        for want in [SHAPE_CODE, edit.key, command.as_str()] {
            assert!(
                said.contains(want),
                "cycle {cycle}: must name {want}:\n{said}"
            );
        }
        assert_eq!(
            part_names(crashed.path()),
            parts,
            "cycle {cycle}: the refused resume wrote nothing"
        );
        assert!(!crashed.path().join("_SUCCESS").exists(), "cycle {cycle}");
    }

    let source: Vec<i64> = (1..=10).collect();
    match remedy {
        Remedy::Revert => {
            let back = engine
                .staged(changed, runner.mode, runner.lines)
                .dest_path(crashed.path().to_path_buf());
            let o = back.run_args(&["--resume"]);
            assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
            assert_eq!(delivered_ids(engine, crashed.path()), source);
            assert!(crashed.path().join("_SUCCESS").exists());
        }
        Remedy::Reset => {
            let reset = changed.cli(&["state", runner.abandon, "--export", &table]);
            assert!(
                reset.status.success(),
                "{}",
                String::from_utf8_lossy(&reset.stderr)
            );
            let again = edited(engine, changed, &runner, &edit, fresh.path());
            again.run_ok();
            assert_eq!(delivered_ids(engine, fresh.path()), source);
        }
    }
}

/// A checkpointed run crashes with a part delivered; `lines` are added to its export and the `--resume` finishes it with every source row once.
fn crashed_then_resumed_under(engine: SqlEngine, runner: Runner, lines: &[&str]) {
    engine.alive();
    let (table, _guard) = engine.range_table("resume_identity_kept");
    engine.insert(&table, 1..=10, 180, Some(10));
    engine.refresh_row_estimate(&table);
    let out = tempfile::tempdir().unwrap();
    let run = engine
        .staged(engine.rig(&table), runner.mode, runner.lines)
        .dest_path(out.path().to_path_buf());
    let crash = run.run_with_env("RIVET_TEST_PANIC_AT", runner.crash_at);
    assert!(!crash.status.success(), "the first run must crash");
    assert!(!part_names(out.path()).is_empty(), "a part was delivered");
    let mut resumed = run;
    for line in lines {
        resumed = resumed.export_line(line);
    }
    let o = resumed.run_args(&["--resume"]);
    assert!(
        o.status.success(),
        "a setting that shapes no byte does not stop the resume:\n{}",
        String::from_utf8_lossy(&o.stderr)
    );
    assert_eq!(
        delivered_ids(engine, out.path()),
        (1..=10).collect::<Vec<i64>>()
    );
    assert!(out.path().join("_SUCCESS").exists());
}

/// A crashed checkpointed run with a part delivered is resumed under `format: csv`: refused by the owner check, which reads the format of the parts.
fn crashed_then_format_edited(engine: SqlEngine, runner: Runner) {
    engine.alive();
    let (table, _guard) = engine.range_table("resume_identity_format");
    engine.insert(&table, 1..=10, 180, Some(10));
    engine.refresh_row_estimate(&table);
    let out = tempfile::tempdir().unwrap();
    let run = engine
        .staged(engine.rig(&table), runner.mode, runner.lines)
        .dest_path(out.path().to_path_buf());
    let crash = run.run_with_env("RIVET_TEST_PANIC_AT", runner.crash_at);
    assert!(!crash.status.success(), "the first run must crash");
    let o = run.with_format("csv").run_args(&["--resume"]);
    let said = String::from_utf8_lossy(&o.stderr).to_string();
    assert_eq!(o.status.code(), Some(5), "{said}");
    assert!(
        said.contains("RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH"),
        "{said}"
    );
    assert!(!said.contains(SHAPE_CODE), "{said}");
}

/// Sorted ids re-read from every part under `out`, whatever case and integer type the engine gives the column.
fn delivered_ids(engine: SqlEngine, out: &Path) -> Vec<i64> {
    use arrow::array::{Array, Int64Array};
    let col = if engine.folds_upper() { "ID" } else { "id" };
    let mut v = Vec::new();
    for b in read_all_parts(out) {
        let ids = arrow::compute::cast(
            b.column_by_name(col).expect("the id column"),
            &arrow::datatypes::DataType::Int64,
        )
        .expect("an integer id");
        let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
        v.extend((0..ids.len()).map(|i| ids.value(i)));
    }
    v.sort();
    v
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_range_chunked_then_compression_edited_postgres() {
    crashed_then_field_edited(SqlEngine::Pg, RANGE_CHUNKED, COMPRESSION, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_range_chunked_then_columns_edited_postgres() {
    crashed_then_field_edited(SqlEngine::Pg, RANGE_CHUNKED, COLUMNS, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_range_chunked_then_meta_columns_edited_postgres() {
    crashed_then_field_edited(SqlEngine::Pg, RANGE_CHUNKED, META_COLUMNS, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_keyset_then_compression_edited_postgres() {
    crashed_then_field_edited(SqlEngine::Pg, KEYSET, COMPRESSION, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_keyset_then_columns_edited_postgres() {
    crashed_then_field_edited(SqlEngine::Pg, KEYSET, COLUMNS, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_keyset_then_meta_columns_edited_postgres() {
    crashed_then_field_edited(SqlEngine::Pg, KEYSET, META_COLUMNS, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_parallel_keyset_then_compression_edited_postgres() {
    crashed_then_field_edited(SqlEngine::Pg, PARALLEL_KEYSET, COMPRESSION, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_parallel_keyset_then_columns_edited_postgres() {
    crashed_then_field_edited(SqlEngine::Pg, PARALLEL_KEYSET, COLUMNS, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_parallel_keyset_then_meta_columns_edited_postgres() {
    crashed_then_field_edited(SqlEngine::Pg, PARALLEL_KEYSET, META_COLUMNS, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_range_chunked_then_an_unrelated_setting_edited_resumes_postgres() {
    crashed_then_resumed_under(SqlEngine::Pg, RANGE_CHUNKED, &["tuning: {batch_size: 100}"]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_range_chunked_then_nothing_edited_resumes_postgres() {
    crashed_then_resumed_under(SqlEngine::Pg, RANGE_CHUNKED, &[]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_keyset_then_an_unrelated_setting_edited_resumes_postgres() {
    crashed_then_resumed_under(SqlEngine::Pg, KEYSET, &["tuning: {batch_size: 100}"]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_keyset_then_nothing_edited_resumes_postgres() {
    crashed_then_resumed_under(SqlEngine::Pg, KEYSET, &[]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_parallel_keyset_then_an_unrelated_setting_edited_resumes_postgres() {
    crashed_then_resumed_under(
        SqlEngine::Pg,
        PARALLEL_KEYSET,
        &["tuning: {batch_size: 100}"],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_parallel_keyset_then_nothing_edited_resumes_postgres() {
    crashed_then_resumed_under(SqlEngine::Pg, PARALLEL_KEYSET, &[]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_range_chunked_then_format_edited_is_refused_by_the_owner_check_postgres() {
    crashed_then_format_edited(SqlEngine::Pg, RANGE_CHUNKED);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_range_chunked_then_compression_edited_mysql() {
    crashed_then_field_edited(SqlEngine::Mysql, RANGE_CHUNKED, COMPRESSION, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_range_chunked_then_columns_edited_mysql() {
    crashed_then_field_edited(SqlEngine::Mysql, RANGE_CHUNKED, COLUMNS, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_range_chunked_then_meta_columns_edited_mysql() {
    crashed_then_field_edited(
        SqlEngine::Mysql,
        RANGE_CHUNKED,
        META_COLUMNS,
        Remedy::Revert,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_keyset_then_compression_edited_mysql() {
    crashed_then_field_edited(SqlEngine::Mysql, KEYSET, COMPRESSION, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_keyset_then_columns_edited_mysql() {
    crashed_then_field_edited(SqlEngine::Mysql, KEYSET, COLUMNS, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_keyset_then_meta_columns_edited_mysql() {
    crashed_then_field_edited(SqlEngine::Mysql, KEYSET, META_COLUMNS, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_parallel_keyset_then_compression_edited_mysql() {
    crashed_then_field_edited(
        SqlEngine::Mysql,
        PARALLEL_KEYSET,
        COMPRESSION,
        Remedy::Revert,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_parallel_keyset_then_columns_edited_mysql() {
    crashed_then_field_edited(SqlEngine::Mysql, PARALLEL_KEYSET, COLUMNS, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_parallel_keyset_then_meta_columns_edited_mysql() {
    crashed_then_field_edited(
        SqlEngine::Mysql,
        PARALLEL_KEYSET,
        META_COLUMNS,
        Remedy::Revert,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_range_chunked_then_an_unrelated_setting_edited_resumes_mysql() {
    crashed_then_resumed_under(
        SqlEngine::Mysql,
        RANGE_CHUNKED,
        &["tuning: {batch_size: 100}"],
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_keyset_then_an_unrelated_setting_edited_resumes_mysql() {
    crashed_then_resumed_under(SqlEngine::Mysql, KEYSET, &["tuning: {batch_size: 100}"]);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_parallel_keyset_then_an_unrelated_setting_edited_resumes_mysql() {
    crashed_then_resumed_under(
        SqlEngine::Mysql,
        PARALLEL_KEYSET,
        &["tuning: {batch_size: 100}"],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_range_chunked_then_compression_edited_mssql() {
    crashed_then_field_edited(SqlEngine::Mssql, RANGE_CHUNKED, COMPRESSION, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_range_chunked_then_columns_edited_mssql() {
    crashed_then_field_edited(SqlEngine::Mssql, RANGE_CHUNKED, COLUMNS, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_range_chunked_then_meta_columns_edited_mssql() {
    crashed_then_field_edited(
        SqlEngine::Mssql,
        RANGE_CHUNKED,
        META_COLUMNS,
        Remedy::Revert,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_keyset_then_compression_edited_mssql() {
    crashed_then_field_edited(SqlEngine::Mssql, KEYSET, COMPRESSION, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_keyset_then_columns_edited_mssql() {
    crashed_then_field_edited(SqlEngine::Mssql, KEYSET, COLUMNS, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_keyset_then_meta_columns_edited_mssql() {
    crashed_then_field_edited(SqlEngine::Mssql, KEYSET, META_COLUMNS, Remedy::Revert);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_parallel_keyset_then_compression_edited_mssql() {
    crashed_then_field_edited(
        SqlEngine::Mssql,
        PARALLEL_KEYSET,
        COMPRESSION,
        Remedy::Revert,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_parallel_keyset_then_columns_edited_mssql() {
    crashed_then_field_edited(SqlEngine::Mssql, PARALLEL_KEYSET, COLUMNS, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_parallel_keyset_then_meta_columns_edited_mssql() {
    crashed_then_field_edited(
        SqlEngine::Mssql,
        PARALLEL_KEYSET,
        META_COLUMNS,
        Remedy::Revert,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_range_chunked_then_an_unrelated_setting_edited_resumes_mssql() {
    crashed_then_resumed_under(
        SqlEngine::Mssql,
        RANGE_CHUNKED,
        &["tuning: {batch_size: 100}"],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_keyset_then_an_unrelated_setting_edited_resumes_mssql() {
    crashed_then_resumed_under(SqlEngine::Mssql, KEYSET, &["tuning: {batch_size: 100}"]);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_parallel_keyset_then_an_unrelated_setting_edited_resumes_mssql() {
    crashed_then_resumed_under(
        SqlEngine::Mssql,
        PARALLEL_KEYSET,
        &["tuning: {batch_size: 100}"],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_range_chunked_then_compression_edited_oracle() {
    crashed_then_field_edited(
        SqlEngine::Oracle,
        RANGE_CHUNKED,
        COMPRESSION,
        Remedy::Revert,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_range_chunked_then_columns_edited_oracle() {
    crashed_then_field_edited(SqlEngine::Oracle, RANGE_CHUNKED, COLUMNS, Remedy::Reset);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_range_chunked_then_meta_columns_edited_oracle() {
    crashed_then_field_edited(
        SqlEngine::Oracle,
        RANGE_CHUNKED,
        META_COLUMNS,
        Remedy::Revert,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_keyset_then_compression_edited_oracle() {
    crashed_then_field_edited(SqlEngine::Oracle, KEYSET, COMPRESSION, Remedy::Revert);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_keyset_then_columns_edited_oracle() {
    crashed_then_field_edited(SqlEngine::Oracle, KEYSET, COLUMNS, Remedy::Reset);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_keyset_then_meta_columns_edited_oracle() {
    crashed_then_field_edited(SqlEngine::Oracle, KEYSET, META_COLUMNS, Remedy::Revert);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_parallel_keyset_then_compression_edited_oracle() {
    crashed_then_field_edited(
        SqlEngine::Oracle,
        PARALLEL_KEYSET,
        COMPRESSION,
        Remedy::Revert,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_parallel_keyset_then_columns_edited_oracle() {
    crashed_then_field_edited(SqlEngine::Oracle, PARALLEL_KEYSET, COLUMNS, Remedy::Reset);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_parallel_keyset_then_meta_columns_edited_oracle() {
    crashed_then_field_edited(
        SqlEngine::Oracle,
        PARALLEL_KEYSET,
        META_COLUMNS,
        Remedy::Revert,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_range_chunked_then_an_unrelated_setting_edited_resumes_oracle() {
    crashed_then_resumed_under(
        SqlEngine::Oracle,
        RANGE_CHUNKED,
        &["tuning: {batch_size: 100}"],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_keyset_then_an_unrelated_setting_edited_resumes_oracle() {
    crashed_then_resumed_under(SqlEngine::Oracle, KEYSET, &["tuning: {batch_size: 100}"]);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_parallel_keyset_then_an_unrelated_setting_edited_resumes_oracle() {
    crashed_then_resumed_under(
        SqlEngine::Oracle,
        PARALLEL_KEYSET,
        &["tuning: {batch_size: 100}"],
    );
}
