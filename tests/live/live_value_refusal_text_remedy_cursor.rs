//! A cursor column declared `text` over a date or timestamp is refused on every command, and a cursor that is text already, or declared nothing, advances as before.
//!
//! ```text
//! cargo nextest run --test live_suite --run-ignored only -E 'test(/^live_value_refusal_text_remedy_cursor::/)'
//! ```

use crate::common::*;

const UNSUPPORTED: Refused = Refused::by_code("RIVET_PLAN_CURSOR_TYPE_UNSUPPORTED", 5);

const TEMPORAL: &[(&str, &str, &str)] = &[
    ("date", "DATE", "DATE '2026-01-0"),
    ("timestamp", "TIMESTAMP", "TIMESTAMP '2026-01-01 00:00:0"),
    (
        "timestamptz",
        "TIMESTAMPTZ",
        "TIMESTAMPTZ '2026-01-01 00:00:0",
    ),
];

fn cursor_table(ddl: &str, literal: impl Fn(i64) -> String) -> (String, Box<dyn std::any::Any>) {
    let engine = SqlEngine::Pg;
    engine.alive();
    let (table, guard) = engine.create(
        "vrt_cur",
        &format!("id BIGINT NOT NULL PRIMARY KEY, c {ddl} NOT NULL"),
    );
    for i in 1..=3_i64 {
        engine.exec(&format!(
            "INSERT INTO {table} (id, c) VALUES ({i}, {})",
            literal(i)
        ));
    }
    (table, guard)
}

fn declared_text_cursor(table: &str) -> Rig {
    SqlEngine::Pg.rig(table).restage(
        "incremental",
        &["cursor_column: c", "columns:", "  c: \"text\""],
    )
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_temporal_cursor_declared_text_is_refused_on_every_command_postgres() {
    for (label, ddl, prefix) in TEMPORAL {
        let (table, _guard) = cursor_table(ddl, |i| format!("{prefix}{i}'"));
        let rig = declared_text_cursor(&table);
        for command in ["check", "doctor", "plan", "run"] {
            let out = match command {
                "run" => rig.run(),
                other => rig.cli(&[other]),
            };
            assert_refused(&out, UNSUPPORTED);
            let said = String::from_utf8_lossy(&out.stderr);
            assert!(
                said.contains("cursor_column 'c' is declared text over a `")
                    && said.contains(&format!("`{label}` column"))
                    && said.contains("nothing was read or written. Remove `c` from `columns:`"),
                "{label} {command}: {said}"
            );
        }
        assert_eq!(
            files_with_extension(&rig.out_dir(), "parquet").len(),
            0,
            "{label}: a part was written"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_text_cursor_declared_text_and_a_temporal_cursor_declared_nothing_advance_once_postgres() {
    for (label, ddl, literal) in [
        (
            "varchar",
            "VARCHAR(20)",
            Box::new(|i| format!("'v{i}'")) as Box<dyn Fn(i64) -> String>,
        ),
        (
            "timestamp",
            "TIMESTAMP",
            Box::new(|i| format!("TIMESTAMP '2026-01-01 00:00:0{i}'")),
        ),
    ] {
        let (table, _guard) = cursor_table(ddl, literal);
        let lines: &[&str] = match label {
            "varchar" => &["cursor_column: c", "columns:", "  c: \"text\""],
            _ => &["cursor_column: c"],
        };
        let rig = SqlEngine::Pg.rig(&table).restage("incremental", lines);
        rig.run_ok();
        let parts = files_with_extension(&rig.out_dir(), "parquet").len();
        assert!(parts > 0, "{label}: nothing delivered");
        rig.run_ok();
        assert_eq!(
            files_with_extension(&rig.out_dir(), "parquet").len(),
            parts,
            "{label}: the second run wrote a part"
        );
    }
}
