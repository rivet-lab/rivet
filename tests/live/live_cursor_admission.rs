//! Incremental cursor admission (docs/modes/incremental.md): a `cursor_column` that can hold NULL, or
//! whose type the cursor cannot render, is refused by code before any read, on every command.
//!
//! Oracles: the exit and `Error:` line of the real binary, the parts under the destination, and the
//! ids the destination holds against the ids the source holds.

use crate::common::*;
use arrow::array::AsArray;
use arrow::datatypes::{DataType, Decimal128Type, Int32Type, Int64Type};

const NULLABLE: Refused = Refused::by_code("RIVET_PLAN_CURSOR_NULLABLE", 5);
const UNSUPPORTED: Refused = Refused::by_code("RIVET_PLAN_CURSOR_TYPE_UNSUPPORTED", 5);
const COMMANDS: [&str; 4] = ["check", "doctor", "plan", "run"];

/// The cursor column and the fallback column, spelt as the engine folds them.
fn names(engine: SqlEngine) -> (&'static str, &'static str) {
    match engine.folds_upper() {
        true => ("C", "D"),
        false => ("c", "d"),
    }
}

/// The microsecond timestamp column type of the engine.
fn timestamp(engine: SqlEngine) -> &'static str {
    match engine {
        SqlEngine::Pg => "TIMESTAMP",
        SqlEngine::Mysql => "DATETIME(6)",
        SqlEngine::Mssql => "DATETIME2(6)",
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => "TIMESTAMP(6)",
    }
}

/// A column type the cursor cannot render: a time of day, or RAW on Oracle (which has no time of day).
fn unrenderable(engine: SqlEngine) -> &'static str {
    match engine {
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => "RAW(8)",
        _ => "TIME",
    }
}

/// The literal of timestamp row `i`, or NULL for `None`.
fn ts_literal(engine: SqlEngine, i: Option<i64>) -> String {
    let Some(i) = i else { return "NULL".into() };
    match engine {
        SqlEngine::Pg => format!("TIMESTAMP '2026-01-01 00:00:0{i}'"),
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => format!("TIMESTAMP '2026-01-01 00:00:0{i}'"),
        _ => format!("'2026-01-01 00:00:0{i}'"),
    }
}

/// The literal of an unrenderable-type row `i`.
fn unrenderable_literal(engine: SqlEngine, i: i64) -> String {
    match engine {
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => format!("HEXTORAW('{i:016X}')"),
        _ => format!("'10:00:0{i}'"),
    }
}

/// A fresh `(id, c, d)` table: `c` of `c_type` with `c_null`, `d` a NOT NULL timestamp; rows 1..=3, row 3 NULL in `c` when `null_row`.
fn seeded(
    engine: SqlEngine,
    c_type: &str,
    c_null: &str,
    null_row: bool,
) -> (String, Box<dyn std::any::Any>) {
    engine.alive();
    let (c, d) = names(engine);
    let id = match engine.folds_upper() {
        true => "ID",
        false => "id",
    };
    let id_ddl = match engine.folds_upper() {
        true => "NUMBER(18)",
        false => engine.int64(),
    };
    let ts = timestamp(engine);
    let (table, guard) = engine.create(
        "cur_adm",
        &format!("{id} {id_ddl} NOT NULL PRIMARY KEY, {c} {c_type} {c_null}, {d} {ts} NOT NULL"),
    );
    for i in 1..=3_i64 {
        let value = match (c_type == ts, null_row && i == 3) {
            (_, true) => "NULL".to_string(),
            (true, false) => ts_literal(engine, Some(i)),
            (false, false) => unrenderable_literal(engine, i),
        };
        engine.exec(&format!(
            "INSERT INTO {table} ({id}, {c}, {d}) VALUES ({i}, {value}, {})",
            ts_literal(engine, Some(i))
        ));
    }
    engine.refresh_row_estimate(&table);
    (table, guard)
}

/// The sorted ids the parts under `dir` hold, the column named in either case.
fn delivered_ids(dir: &std::path::Path) -> Vec<i64> {
    let mut ids = Vec::new();
    for batch in read_all_parts(dir) {
        let schema = batch.schema();
        let at = schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case("id"))
            .expect("an id column");
        let column = batch.column(at);
        match column.data_type() {
            DataType::Int32 => ids.extend(
                column
                    .as_primitive::<Int32Type>()
                    .iter()
                    .flatten()
                    .map(i64::from),
            ),
            DataType::Int64 => ids.extend(column.as_primitive::<Int64Type>().iter().flatten()),
            DataType::Decimal128(_, 0) => ids.extend(
                column
                    .as_primitive::<Decimal128Type>()
                    .iter()
                    .flatten()
                    .map(|v| v as i64),
            ),
            other => panic!("the id column is {other:?}"),
        }
    }
    ids.sort();
    ids.dedup();
    ids
}

/// An incremental rig on `table` with the export lines `lines`.
fn incremental(engine: SqlEngine, table: &str, lines: &[&str]) -> Rig {
    engine.rig(table).restage("incremental", lines)
}

/// `doctor` exits 0 on a config whose cursor the admission lets through.
fn assert_doctor_passes(rig: &Rig) {
    let out = rig.cli(&["doctor"]);
    assert!(
        out.status.success(),
        "doctor refused an admitted cursor: exit {:?}\n{}{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Every command refuses by `want`, and the run writes no part.
fn refused_on_every_command(rig: &Rig, want: Refused, what: &str) {
    let mut gaps = Vec::new();
    for command in COMMANDS {
        let out = match command {
            "run" => rig.run(),
            other => rig.cli(&[other]),
        };
        let said =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| assert_refused(&out, want)));
        if said.is_err() {
            gaps.push(format!(
                "{command}: exit {:?}, not refused as wanted",
                out.status.code()
            ));
        }
    }
    let parts = files_with_extension(&rig.out_dir(), "parquet").len();
    if parts > 0 {
        gaps.push(format!(
            "run: {parts} part(s) written, ids {:?}",
            delivered_ids(&rig.out_dir())
        ));
    }
    assert!(
        gaps.is_empty(),
        "{what}: not refused on every command\n{}",
        gaps.join("\n")
    );
}

/// The refusal repeats, an unchanged re-run stays refused, and the sentence of its remedy leads out.
fn refusal_walks_out(mut rig: Rig, want: Refused, sentence: &str, fixed: Vec<String>) {
    rig.refuses_twice_and_walks_out(
        &["run"],
        &[],
        want,
        vec![
            Remedy::wrong("re-ran the export unchanged", Then::Refuses(want), |_| {}),
            Remedy::new(sentence, Then::DeliversTheSource, move |rig| {
                let lines: Vec<&str> = fixed.iter().map(String::as_str).collect();
                rig.set_export_lines(&lines);
            }),
        ],
    );
}

/// A nullable cursor column is refused by code on every command, and the coalesce remedy leads out.
fn nullable_cursor_is_refused(engine: SqlEngine) {
    let (table, _guard) = seeded(engine, timestamp(engine), "NULL", true);
    let (c, d) = names(engine);
    let rig = incremental(engine, &table, &[&format!("cursor_column: {c}")]);
    refused_on_every_command(&rig, NULLABLE, &format!("nullable cursor {c} on {table}"));
    refusal_walks_out(
        rig,
        NULLABLE,
        "set `incremental_cursor_mode: coalesce` with a NOT NULL `cursor_fallback_column:`",
        vec![
            format!("cursor_column: {c}"),
            format!("cursor_fallback_column: {d}"),
            "incremental_cursor_mode: coalesce".to_string(),
        ],
    );
}

/// A NOT NULL cursor column delivers every row once and nothing the second time.
fn not_null_cursor_delivers_once(engine: SqlEngine) {
    let (table, _guard) = seeded(engine, timestamp(engine), "NOT NULL", false);
    let (c, _) = names(engine);
    let rig = incremental(engine, &table, &[&format!("cursor_column: {c}")]);
    assert_doctor_passes(&rig);
    rig.run_ok();
    assert_eq!(delivered_ids(&rig.out_dir()), vec![1, 2, 3]);
    let parts = files_with_extension(&rig.out_dir(), "parquet").len();
    rig.run_ok();
    assert_eq!(
        files_with_extension(&rig.out_dir(), "parquet").len(),
        parts,
        "second run wrote a part"
    );
}

/// A nullable primary under `coalesce` with a NOT NULL fallback still delivers every row, NULL one included.
fn coalesce_over_a_nullable_cursor_delivers(engine: SqlEngine) {
    let (table, _guard) = seeded(engine, timestamp(engine), "NULL", true);
    let (c, d) = names(engine);
    let rig = incremental(
        engine,
        &table,
        &[
            &format!("cursor_column: {c}"),
            &format!("cursor_fallback_column: {d}"),
            "incremental_cursor_mode: coalesce",
        ],
    );
    assert_doctor_passes(&rig);
    rig.run_ok();
    assert_eq!(delivered_ids(&rig.out_dir()), vec![1, 2, 3]);
}

/// A cursor column of a type the cursor cannot render is refused by code on every command.
fn unrenderable_cursor_is_refused(engine: SqlEngine) {
    let (table, _guard) = seeded(engine, unrenderable(engine), "NOT NULL", false);
    let (c, d) = names(engine);
    let rig = incremental(engine, &table, &[&format!("cursor_column: {c}")]);
    refused_on_every_command(
        &rig,
        UNSUPPORTED,
        &format!("unrenderable cursor {c} on {table}"),
    );
    refusal_walks_out(
        rig,
        UNSUPPORTED,
        "Use an integer, decimal, text, date, timestamp or uuid `cursor_column:`",
        vec![format!("cursor_column: {d}")],
    );
}

/// Drops the view it names when the cell ends.
struct DropView(SqlEngine, String);

impl Drop for DropView {
    /// Drop the view, ignoring a failure so a cell that already failed reports its own cause.
    fn drop(&mut self) {
        let sql = format!("DROP VIEW {}", self.1);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.0.exec(&sql)));
    }
}

/// A view over a table whose cursor column is NOT NULL is admitted and delivers every row once.
fn a_view_over_a_not_null_cursor_delivers_once(engine: SqlEngine) {
    let (table, _guard) = seeded(engine, timestamp(engine), "NOT NULL", false);
    let view = format!("{table}_v");
    engine.exec(&format!("CREATE VIEW {view} AS SELECT * FROM {table}"));
    let _view = DropView(engine, view.clone());
    let (c, _) = names(engine);
    let rig = incremental(engine, &view, &[&format!("cursor_column: {c}")]);
    rig.run_ok();
    assert_eq!(delivered_ids(&rig.out_dir()), vec![1, 2, 3]);
}

/// The integer cast of a NOT NULL id, the cursor column of a query export, is admitted and delivers every row once.
fn an_expression_over_a_not_null_cursor_delivers_once(engine: SqlEngine) {
    let (table, _guard) = seeded(engine, timestamp(engine), "NOT NULL", false);
    let (id, cast) = match engine {
        SqlEngine::Pg | SqlEngine::Mssql => ("id", "BIGINT"),
        SqlEngine::Mysql => ("id", "SIGNED"),
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => ("ID", "NUMBER(18)"),
    };
    let rig = engine
        .rig(&table)
        .query(&format!(
            "SELECT CAST({id} AS {cast}) AS {id}, {id} AS other FROM {table}"
        ))
        .mode("incremental")
        .export_line(&format!("cursor_column: {id}"));
    rig.run_ok();
    assert_eq!(delivered_ids(&rig.out_dir()), vec![1, 2, 3]);
    let parts = files_with_extension(&rig.out_dir(), "parquet").len();
    rig.run_ok();
    assert_eq!(
        files_with_extension(&rig.out_dir(), "parquet").len(),
        parts,
        "second run wrote a part"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn nullable_cursor_is_refused_postgres() {
    nullable_cursor_is_refused(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn nullable_cursor_is_refused_mysql() {
    nullable_cursor_is_refused(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn nullable_cursor_is_refused_mssql() {
    nullable_cursor_is_refused(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn nullable_cursor_is_refused_oracle() {
    nullable_cursor_is_refused(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn unrenderable_cursor_is_refused_postgres() {
    unrenderable_cursor_is_refused(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn unrenderable_cursor_is_refused_mysql() {
    unrenderable_cursor_is_refused(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn unrenderable_cursor_is_refused_mssql() {
    unrenderable_cursor_is_refused(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn unrenderable_cursor_is_refused_oracle() {
    unrenderable_cursor_is_refused(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn not_null_cursor_delivers_once_postgres() {
    not_null_cursor_delivers_once(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn not_null_cursor_delivers_once_mysql() {
    not_null_cursor_delivers_once(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn not_null_cursor_delivers_once_mssql() {
    not_null_cursor_delivers_once(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn not_null_cursor_delivers_once_oracle() {
    not_null_cursor_delivers_once(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn coalesce_over_a_nullable_cursor_delivers_postgres() {
    coalesce_over_a_nullable_cursor_delivers(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn coalesce_over_a_nullable_cursor_delivers_mysql() {
    coalesce_over_a_nullable_cursor_delivers(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn coalesce_over_a_nullable_cursor_delivers_mssql() {
    coalesce_over_a_nullable_cursor_delivers(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn coalesce_over_a_nullable_cursor_delivers_oracle() {
    coalesce_over_a_nullable_cursor_delivers(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_view_over_a_not_null_cursor_delivers_once_postgres() {
    a_view_over_a_not_null_cursor_delivers_once(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_view_over_a_not_null_cursor_delivers_once_mysql() {
    a_view_over_a_not_null_cursor_delivers_once(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_view_over_a_not_null_cursor_delivers_once_mssql() {
    a_view_over_a_not_null_cursor_delivers_once(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_view_over_a_not_null_cursor_delivers_once_oracle() {
    a_view_over_a_not_null_cursor_delivers_once(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_expression_over_a_not_null_cursor_delivers_once_postgres() {
    an_expression_over_a_not_null_cursor_delivers_once(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_expression_over_a_not_null_cursor_delivers_once_mysql() {
    an_expression_over_a_not_null_cursor_delivers_once(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn an_expression_over_a_not_null_cursor_delivers_once_mssql() {
    an_expression_over_a_not_null_cursor_delivers_once(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_expression_over_a_not_null_cursor_delivers_once_oracle() {
    an_expression_over_a_not_null_cursor_delivers_once(SqlEngine::Oracle);
}
