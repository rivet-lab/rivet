//! A baseline leg and the change stream of one table deliver one spelling: a server-text column declared `text` is refused on the pending baseline, and what the stream already carried stays as it was.
//!
//! ```text
//! cargo nextest run --test live_suite --run-ignored only -E 'test(/^live_value_refusal_text_remedy_cdc::/)'
//! ```

use crate::check_override_wire_cdc::a_refused_baseline_may_leave_its_schema_and_checkpoint;
use crate::common::*;
use std::collections::BTreeSet;

const WIRE: Refused<'static> = Refused::by_code("RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);

const SERVER_TEXT: &[(&str, &str, &str)] = &[
    ("date", "DATE", "'2024-03-15'"),
    ("time", "TIME", "'10:11:12.5'"),
    ("timestamp", "TIMESTAMP", "'2024-03-15 10:11:12.5'"),
    ("timestamptz", "TIMESTAMPTZ", "'2024-03-15 10:11:12.5+00'"),
    ("int4_array", "INTEGER[]", "'{1,2}'"),
];

fn declared_text(rig: Rig) -> Rig {
    rig.export_line("columns:").export_line("  v: \"text\"")
}

fn scenario(label: &str, ddl: &str, shape: impl FnOnce(Rig) -> Rig) -> CdcScenario {
    CdcScenario::pg_with(label, &format!("id BIGINT PRIMARY KEY, v {ddl}"), |r, _| {
        shape(r.cdc_line("initial: snapshot"))
    })
}

fn put(scn: &mut CdcScenario, literal: &str) {
    let t = scn.table.clone();
    scn.sql(&format!("INSERT INTO {t} (id, v) VALUES (1, {literal})"));
    scn.settle();
}

fn delivered(rig: &Rig, column: &str) -> BTreeSet<String> {
    duckdb_dir_parquet_distinct_strings(&rig.out_dir(), column)
}

fn server_text(table: &str) -> BTreeSet<String> {
    let mut oracle = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).unwrap();
    oracle.batch_execute("SET TimeZone = 'UTC'").unwrap();
    oracle
        .query(&format!("SELECT v::text FROM {table}"), &[])
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect()
}

fn ids(of: &[&str]) -> BTreeSet<String> {
    of.iter().map(|i| i.to_string()).collect()
}

fn both_check_spellings_accept(rig: &Rig, name: &str) {
    for args in [
        &["check", "--export", name][..],
        &["check", "--export", name, "--type-report"][..],
    ] {
        let o = rig.cli(args);
        assert_eq!(
            o.status.code(),
            Some(0),
            "{args:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
    }
}

/// The run and both `check` spellings refuse `rig`'s export by `WIRE`; returns the run's stderr.
fn refused_alike(rig: &Rig, name: &str) -> String {
    assert_eq!(
        files_with_extension(&rig.out_dir(), "parquet").len(),
        0,
        "a refused baseline starts without a part"
    );
    let run = rig.run_args(&["--export", name]);
    assert_refused(&run, WIRE);
    assert_refused(&rig.cli(&["check", "--export", name]), WIRE);
    assert_refused(
        &rig.cli(&["check", "--export", name, "--type-report"]),
        WIRE,
    );
    assert_eq!(
        files_with_extension(&rig.out_dir(), "parquet").len(),
        0,
        "a refused baseline delivered a part"
    );
    String::from_utf8_lossy(&run.stderr).into_owned()
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_pending_baseline_declared_text_over_a_server_text_column_is_refused_and_its_remedy_delivers_postgres()
 {
    for (label, ddl, literal) in SERVER_TEXT {
        let mut scn = scenario(&format!("vrt_cdc_{label}"), ddl, declared_text);
        put(&mut scn, literal);
        let name = scn.rig.export_name().to_string();
        scn.rig = a_refused_baseline_may_leave_its_schema_and_checkpoint(scn.rig);
        let said = refused_alike(&scn.rig, &name);
        let native = match *label {
            "int4_array" => "_int4",
            other => other,
        };
        assert!(
            said.contains(&format!(
                "column 'v' is declared text over a `{native}` column, which the baseline leg would deliver as the server's own text while the change stream renders the same cell in its own form"
            )),
            "{label}: {said}"
        );
        assert!(
            said.contains("no row was delivered. Remove `v` from `columns:`"),
            "{label}: {said}"
        );
        scn.rig.replace_export_line("columns:", "");
        scn.rig.replace_export_line("  v:", "");
        scn.rig.run_ok();
        assert_eq!(
            delivered(&scn.rig, "id"),
            ids(&["1"]),
            "{label}: the remedy delivers the row"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_stream_only_export_declared_text_is_admitted_as_it_was_postgres() {
    for (label, ddl, literal) in SERVER_TEXT {
        let mut scn = CdcScenario::pg_with(
            &format!("vrt_cdc_only_{label}"),
            &format!("id BIGINT PRIMARY KEY, v {ddl}"),
            |r, _| declared_text(r),
        );
        put(&mut scn, literal);
        let name = scn.rig.export_name().to_string();
        both_check_spellings_accept(&scn.rig, &name);
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_done_baseline_is_not_admitted_again_over_a_text_declaration_postgres() {
    let mut scn = scenario("vrt_cdc_done", "DATE", |r| r);
    put(&mut scn, "'2024-03-15'");
    scn.rig.run_ok();
    assert_eq!(delivered(&scn.rig, "id"), ids(&["1"]));
    let name = scn.rig.export_name().to_string();
    scn.rig.amend_export_lines(&["columns:", "  v: \"text\""]);
    both_check_spellings_accept(&scn.rig, &name);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_pending_baseline_declared_text_over_columns_the_server_does_not_render_is_admitted_postgres() {
    for (label, ddl, literal) in [
        ("numeric", "NUMERIC(10,2)", "12.50"),
        ("varchar", "VARCHAR(20)", "'abc'"),
    ] {
        let mut scn = scenario(&format!("vrt_cdc_n_{label}"), ddl, declared_text);
        put(&mut scn, literal);
        let name = scn.rig.export_name().to_string();
        both_check_spellings_accept(&scn.rig, &name);
        scn.rig.run_ok();
        assert_eq!(
            delivered(&scn.rig, "v"),
            server_text(&scn.table),
            "{label}: the declared text is the server's text"
        );
    }
}
