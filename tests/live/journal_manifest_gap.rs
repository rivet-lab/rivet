//! A run whose terminal `manifest.json` never landed is a failed run in every record of it: the journal read back
//! through `rivet journal`, and the card the run prints, on `rivet run` and `rivet apply`, on each engine.

use crate::audit_plan_apply::{block_manifest, declare_the_manifest_gap};
use crate::common::*;
use crate::live_sabotage::{mongo_rig, seeded};
use std::any::Any;
use std::path::Path;

const MANIFEST_GAP_TEXT: &str = "the manifest write FAILED";
const URL_ENV: &str = "RIVET_GAP_SOURCE_URL";

#[derive(Clone, Copy)]
enum Target {
    Sql(SqlEngine),
    Mongo,
}

type Envs = Vec<(&'static str, String)>;

struct Fixture {
    rig: Rig,
    table: String,
    envs: Envs,
    _guard: Box<dyn Any>,
}

/// A full export of a seeded table (or collection) of `target` writing into `out`, its manifest write declared failing.
fn fixture(target: Target, tag: &str, out: &Path) -> Fixture {
    let (rig, table, envs, guard): (Rig, String, Envs, Box<dyn Any>) = match target {
        Target::Sql(engine) => {
            let (table, guard) = seeded(engine, tag);
            let rig = engine.rig(&table).source_url_env(URL_ENV);
            (rig, table, vec![(URL_ENV, engine.url().to_string())], guard)
        }
        Target::Mongo => {
            let (rig, guard) = mongo_rig(tag);
            (rig, "t".to_string(), Vec::new(), Box::new(guard))
        }
    };
    Fixture {
        rig: declare_the_manifest_gap(rig).dest_path(out.to_path_buf()),
        table,
        envs,
        _guard: guard,
    }
}

impl Fixture {
    fn envs(&self) -> Vec<(&str, &str)> {
        self.envs.iter().map(|(k, v)| (*k, v.as_str())).collect()
    }
}

/// `rivet journal` for `export`, read back through the product: its header line and its full text.
fn journal_of(rig: &Rig, envs: &[(&str, &str)], export: &str) -> (String, String) {
    let out = rig.cli_env(&["journal", "--export", export, "--last", "1"], envs);
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let header = text
        .lines()
        .find(|l| l.contains(export) && l.starts_with(['✓', '✗', '•']))
        .unwrap_or_else(|| panic!("no journal header for {export} in:\n{text}"))
        .to_string();
    (header, text)
}

fn assert_journal_says_failed_at_the_manifest(
    rig: &Rig,
    envs: &[(&str, &str)],
    export: &str,
    how: &str,
) {
    let (header, text) = journal_of(rig, envs, export);
    assert!(
        header.starts_with('✗') && header.contains("failed"),
        "{how}: the journal records a run whose manifest never landed as {header:?}; full journal:\n{text}"
    );
    assert!(
        text.lines()
            .any(|l| l.trim_start().starts_with("error:") && l.contains(MANIFEST_GAP_TEXT)),
        "{how}: the journal's error line must name the manifest gap ({MANIFEST_GAP_TEXT:?}); full journal:\n{text}"
    );
}

/// A run's cards on stderr: the lines that begin with a card glyph.
fn card_lines(stderr: &str) -> Vec<&str> {
    stderr
        .lines()
        .filter(|l| l.starts_with(['✓', '✗', '•', '▸']))
        .collect()
}

fn run_journals_the_gap(target: Target) {
    let out_dir = tempfile::tempdir().unwrap();
    let f = fixture(target, "gap_run", out_dir.path());
    f.rig.config_path();
    block_manifest(out_dir.path());

    let out = f.rig.run_args_env(&[], &f.envs());
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        !files_with_extension(out_dir.path(), "parquet").is_empty(),
        "fixture is inert: no part landed; stderr:\n{stderr}"
    );
    assert!(
        !out.status.success(),
        "control: the run exits non-zero; stderr:\n{stderr}"
    );
    assert_journal_says_failed_at_the_manifest(&f.rig, &f.envs(), f.rig.export_name(), "rivet run");
}

fn apply_journals_the_gap(target: Target) {
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let f = fixture(target, "gap_apply", out_dir.path());
    f.rig.config_path();
    let plan_path = cfg_dir.path().join("plan.json");
    let plan_out = f.rig.plan_json_env(&plan_path, &[], &f.envs());
    assert!(
        plan_out.status.success(),
        "plan must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );
    block_manifest(out_dir.path());

    let out = f.rig.apply_env(&plan_path, &[], &f.envs());
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        !files_with_extension(out_dir.path(), "parquet").is_empty(),
        "fixture is inert: no part landed; stderr:\n{stderr}"
    );
    assert!(
        !out.status.success(),
        "control: apply exits non-zero; stderr:\n{stderr}"
    );
    assert_journal_says_failed_at_the_manifest(
        &f.rig,
        &f.envs(),
        f.rig.export_name(),
        "rivet apply",
    );
}

/// Two exports in one run, the first with a blocked manifest: its card and its journal say failed, its neighbour's say success.
fn cards_and_journals_of_a_pair(target: Target, flags: &[&str], how: &str) {
    let out_dir = tempfile::tempdir().unwrap();
    let f = fixture(target, "gap_card", out_dir.path());
    let envs = f.envs.clone();
    let envs: Vec<(&str, &str)> = envs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let blocked = f.rig.export_name().to_string();
    let good = format!("{blocked}_ok");
    let rig = f.rig.also_batch_export(&good, &f.table, "full");
    rig.config_path();
    block_manifest(out_dir.path());

    let out = rig.run_args_env(flags, &envs);
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        !out.status.success(),
        "{how}: control: the run exits non-zero; stderr:\n{stderr}"
    );
    let cards = card_lines(&stderr);
    let card_of = |name: &str| {
        cards
            .iter()
            .copied()
            .find(|l| l.split_whitespace().nth(1) == Some(name))
    };
    let blocked_card = card_of(&blocked)
        .unwrap_or_else(|| panic!("{how}: no card for {blocked}; stderr:\n{stderr}"));
    assert!(
        blocked_card.starts_with('✗'),
        "{how}: the card of an export whose manifest never landed is {blocked_card:?}; cards:\n{}",
        cards.join("\n")
    );
    let good_card =
        card_of(&good).unwrap_or_else(|| panic!("{how}: no card for {good}; stderr:\n{stderr}"));
    assert!(
        good_card.starts_with('✓'),
        "{how}: the neighbour's card changed: {good_card:?}"
    );
    assert_journal_says_failed_at_the_manifest(&rig, &envs, &blocked, how);
    let (good_header, _) = journal_of(&rig, &envs, &good);
    assert!(
        good_header.starts_with('✓') && good_header.contains("success"),
        "{how}: the neighbour's journal changed: {good_header:?}"
    );
}

fn sequential_cards_the_gap(target: Target) {
    cards_and_journals_of_a_pair(target, &[], "sequential run");
}

fn parallel_cards_the_gap(target: Target) {
    cards_and_journals_of_a_pair(target, &["--parallel-exports"], "--parallel-exports run");
}

#[test]
#[ignore = "live: postgres"]
fn run_journals_a_run_whose_manifest_never_landed_as_failed_postgres() {
    run_journals_the_gap(Target::Sql(SqlEngine::Pg));
}

#[test]
#[ignore = "live: mysql"]
fn run_journals_a_run_whose_manifest_never_landed_as_failed_mysql() {
    run_journals_the_gap(Target::Sql(SqlEngine::Mysql));
}

#[test]
#[ignore = "live: mssql"]
fn run_journals_a_run_whose_manifest_never_landed_as_failed_mssql() {
    run_journals_the_gap(Target::Sql(SqlEngine::Mssql));
}

#[test]
#[cfg(feature = "oracle")]
#[ignore = "live: oracle"]
fn run_journals_a_run_whose_manifest_never_landed_as_failed_oracle() {
    run_journals_the_gap(Target::Sql(SqlEngine::Oracle));
}

#[test]
#[ignore = "live: mongo"]
fn run_journals_a_run_whose_manifest_never_landed_as_failed_mongo() {
    run_journals_the_gap(Target::Mongo);
}

#[test]
#[ignore = "live: postgres"]
fn apply_journals_a_run_whose_manifest_never_landed_as_failed_postgres() {
    apply_journals_the_gap(Target::Sql(SqlEngine::Pg));
}

#[test]
#[ignore = "live: mysql"]
fn apply_journals_a_run_whose_manifest_never_landed_as_failed_mysql() {
    apply_journals_the_gap(Target::Sql(SqlEngine::Mysql));
}

#[test]
#[ignore = "live: mssql"]
fn apply_journals_a_run_whose_manifest_never_landed_as_failed_mssql() {
    apply_journals_the_gap(Target::Sql(SqlEngine::Mssql));
}

#[test]
#[cfg(feature = "oracle")]
#[ignore = "live: oracle"]
fn apply_journals_a_run_whose_manifest_never_landed_as_failed_oracle() {
    apply_journals_the_gap(Target::Sql(SqlEngine::Oracle));
}

#[test]
#[ignore = "live: mongo"]
fn apply_journals_a_run_whose_manifest_never_landed_as_failed_mongo() {
    apply_journals_the_gap(Target::Mongo);
}

#[test]
#[ignore = "live: postgres"]
fn a_sequential_run_cards_a_manifest_gap_as_failed_beside_a_good_export_postgres() {
    sequential_cards_the_gap(Target::Sql(SqlEngine::Pg));
}

#[test]
#[ignore = "live: mysql"]
fn a_sequential_run_cards_a_manifest_gap_as_failed_beside_a_good_export_mysql() {
    sequential_cards_the_gap(Target::Sql(SqlEngine::Mysql));
}

#[test]
#[ignore = "live: mssql"]
fn a_sequential_run_cards_a_manifest_gap_as_failed_beside_a_good_export_mssql() {
    sequential_cards_the_gap(Target::Sql(SqlEngine::Mssql));
}

#[test]
#[cfg(feature = "oracle")]
#[ignore = "live: oracle"]
fn a_sequential_run_cards_a_manifest_gap_as_failed_beside_a_good_export_oracle() {
    sequential_cards_the_gap(Target::Sql(SqlEngine::Oracle));
}

#[test]
#[ignore = "live: mongo"]
fn a_sequential_run_cards_a_manifest_gap_as_failed_beside_a_good_export_mongo() {
    sequential_cards_the_gap(Target::Mongo);
}

#[test]
#[ignore = "live: postgres"]
fn a_parallel_run_cards_a_manifest_gap_as_failed_beside_a_good_export_postgres() {
    parallel_cards_the_gap(Target::Sql(SqlEngine::Pg));
}

#[test]
#[ignore = "live: mysql"]
fn a_parallel_run_cards_a_manifest_gap_as_failed_beside_a_good_export_mysql() {
    parallel_cards_the_gap(Target::Sql(SqlEngine::Mysql));
}

#[test]
#[ignore = "live: mssql"]
fn a_parallel_run_cards_a_manifest_gap_as_failed_beside_a_good_export_mssql() {
    parallel_cards_the_gap(Target::Sql(SqlEngine::Mssql));
}

#[test]
#[cfg(feature = "oracle")]
#[ignore = "live: oracle"]
fn a_parallel_run_cards_a_manifest_gap_as_failed_beside_a_good_export_oracle() {
    parallel_cards_the_gap(Target::Sql(SqlEngine::Oracle));
}

#[test]
#[ignore = "live: mongo"]
fn a_parallel_run_cards_a_manifest_gap_as_failed_beside_a_good_export_mongo() {
    parallel_cards_the_gap(Target::Mongo);
}
