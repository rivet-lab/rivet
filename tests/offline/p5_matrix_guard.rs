//! P5 (no debris after a stop, a re-run equals an uninterrupted run): the axes are read from the code and the oracle contract is strict.

#[path = "../common/p5_axes.rs"]
mod p5_axes;

use p5_axes::*;
use std::collections::BTreeSet;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn engines() -> Vec<String> {
    ENGINES.iter().map(|e| e.to_string()).collect()
}

/// The CamelCase variants of `enum_name` in `rel`, one per line of the enum body.
fn enum_variants(rel: &str, enum_name: &str) -> BTreeSet<String> {
    let text = std::fs::read_to_string(root().join(rel)).expect("read the enum's file");
    let needle = format!("enum {enum_name} {{");
    let body = &text[text.find(&needle).expect("enum not found") + needle.len()..];
    let body = &body[..body.find("\n}").expect("enum end")];
    body.lines()
        .map(str::trim)
        .filter(|l| !l.starts_with("//") && !l.starts_with('#') && !l.is_empty())
        .filter_map(|l| {
            let id: String = l
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            id.chars()
                .next()
                .filter(char::is_ascii_uppercase)
                .map(|_| id)
        })
        .collect()
}

fn snake(camel: &str) -> String {
    let mut out = String::new();
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

#[test]
fn the_hook_primitives_are_the_ones_the_scanner_knows() {
    let text = std::fs::read_to_string(root().join("src/test_hook.rs")).unwrap();
    let in_code: BTreeSet<String> = text
        .lines()
        .filter_map(|l| l.strip_prefix("pub(crate) fn maybe_"))
        .map(|l| format!("maybe_{}", l.split('(').next().unwrap()))
        .collect();
    let known: BTreeSet<String> = Kind::ALL.iter().map(|k| k.function().to_string()).collect();
    let extra: Vec<_> = in_code.difference(&known).collect();
    assert!(
        extra.is_empty(),
        "src/test_hook.rs has primitive(s) {extra:?} that P5 has no Kind for: a new way to stop a run is a new axis value"
    );
    let gone: Vec<_> = known.difference(&in_code).collect();
    assert!(
        gone.is_empty(),
        "Kind names primitive(s) {gone:?} that test_hook.rs no longer has"
    );
    for k in Kind::ALL {
        assert!(
            text.contains(k.env()),
            "{} is not read by test_hook.rs",
            k.env()
        );
    }
}

#[test]
fn every_hook_is_found_in_the_code_and_belongs_to_a_family() {
    let found = hooks(&root());
    assert!(
        found.len() >= 30,
        "found only {} hooks: the scan found nothing rather than the hooks going away",
        found.len()
    );
    let orphans: Vec<_> = found
        .iter()
        .filter(|h| h.family.is_none())
        .map(|h| (&h.name, &h.file))
        .collect();
    assert!(
        orphans.is_empty(),
        "hook(s) in a file no Family owns, add it to family_of: {orphans:?}"
    );
    let names: BTreeSet<_> = found.iter().map(|h| h.name.clone()).collect();
    let missing: Vec<_> = documented(&root()).difference(&names).cloned().collect();
    assert!(
        missing.is_empty(),
        "documented hook(s) with no literal in src/: {missing:?}"
    );
    for known in [
        "after_source_read",
        "before_commit_rename",
        "cdc_after_open",
        "pg_after_snapshot_open",
        "clickhouse_full_after_exchange",
    ] {
        assert!(names.contains(known), "the scan lost {known}");
    }
}

#[test]
fn the_engine_axis_is_the_sql_engine_enum() {
    let scanned: BTreeSet<String> = enum_variants("tests/common/sql_engine.rs", "SqlEngine")
        .iter()
        .map(|v| v.to_lowercase())
        .collect();
    let declared: BTreeSet<String> = ENGINES.iter().map(|e| e.to_string()).collect();
    assert_eq!(scanned, declared, "ENGINES must be the SqlEngine variants");
}

#[test]
fn every_export_mode_has_a_shape() {
    let modes: BTreeSet<String> = enum_variants("src/config/export.rs", "ExportMode")
        .iter()
        .map(|v| snake(v))
        .collect();
    let shaped: BTreeSet<String> = Shape::ALL.iter().map(|s| s.mode().to_string()).collect();
    let none: Vec<_> = modes.difference(&shaped).collect();
    assert_eq!(
        none,
        Vec::<&String>::new(),
        "export mode(s) have no Shape in p5_axes"
    );
}

#[test]
fn the_cells_are_unique_and_each_is_runnable_or_says_why_not() {
    let all = cells(&root(), &engines());
    assert!(all.len() > 300, "{} cells", all.len());
    let keys: BTreeSet<String> = all.iter().map(Cell::key).collect();
    assert_eq!(keys.len(), all.len(), "duplicate cell keys");
    for c in &all {
        if c.not_graded().is_none() {
            assert!(
                c.shape.export().is_some(),
                "{} claims to run without a built shape",
                c.key()
            );
        }
    }
    let runnable = all.iter().filter(|c| c.not_graded().is_none()).count();
    assert!(runnable > 50, "only {runnable} runnable cells");
    for needle in [
        "pg/range/after_chunk_file/fault",
        "mysql/full/after_source_read/sigterm",
        "pg/keyset/pg_after_snapshot_open/sigkill",
    ] {
        assert!(keys.contains(needle), "no cell {needle}");
    }
    assert!(
        !keys.contains("mysql/full/pg_after_snapshot_open/sigkill"),
        "a Postgres-source hook reached another engine"
    );
}

#[test]
fn only_parking_hooks_take_a_signal_and_only_faulting_ones_a_fault() {
    for k in Kind::ALL {
        assert_ne!(k.faults(), k.parks());
        assert_eq!(Stop::Fault.impossible_at(k).is_none(), k.faults());
        assert_eq!(Stop::Kill.impossible_at(k).is_none(), k.parks());
        assert_eq!(Stop::Term.impossible_at(k).is_none(), k.parks());
    }
}

#[test]
fn a_hook_is_armed_the_way_its_primitive_reads_it() {
    let hook = |name: &str, kind, indexed| Hook {
        name: name.into(),
        kind,
        file: String::new(),
        family: None,
        indexed,
    };
    assert_eq!(hook("a", Kind::PanicAt, false).armed("9"), "a");
    assert_eq!(hook("a", Kind::PanicAt, true).armed("9"), "a:0");
    assert_eq!(hook("a", Kind::PauseAt, false).armed("600000"), "a:600000");
    assert_eq!(hook("a", Kind::BlockAt, false).armed("9"), "a");
    assert_eq!(hook("a", Kind::ErrorAtIndex, true).armed("9"), "a:0");
}

#[test]
fn the_scanner_reads_literals_formats_and_calls_across_lines() {
    let dir = tempfile::tempdir().unwrap();
    let w = |rel: &str, text: &str| {
        let p: PathBuf = dir.path().join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, text).unwrap();
    };
    w(
        "src/test_hook.rs",
        "//! | `named_by_variable` | x | y |\n//! | `after_page:{N}` | x | y |\n",
    );
    w(
        "src/pipeline/keyset.rs",
        "fn f(){ crate::test_hook::maybe_panic_at(&format!(\"after_page:{pages}\"));\n  crate::test_hook::maybe_panic_at_chunk(\n  \"after_x\",\n  i);\n  my_maybe_panic_at(\"not_a_call\"); }",
    );
    w(
        "src/load/clickhouse.rs",
        "fn g(){ maybe_panic_at(point); let _ = Some(\"named_by_variable\"); maybe_block_at(\"b\"); }",
    );
    let found = hooks(dir.path());
    let got: BTreeSet<(String, bool)> = found.iter().map(|h| (h.name.clone(), h.indexed)).collect();
    let want: BTreeSet<(String, bool)> = [
        ("after_page", true),
        ("after_x", true),
        ("named_by_variable", false),
        ("b", false),
    ]
    .iter()
    .map(|(n, i)| (n.to_string(), *i))
    .collect();
    assert_eq!(got, want);
    assert!(found.iter().all(|h| h.family.is_some()));
}

#[test]
fn an_oracle_that_proves_nothing_never_reads_pass() {
    let canary = |verdict: &str, flagged: bool| {
        format!("{{\"verdict\":\"{verdict}\",\"canary_flagged\":{flagged},\"evidence\":\"e\"}}")
    };
    assert_eq!(oracle_verdict(Ok(&canary("PASS", true))).0, Verdict::Pass);
    assert_eq!(oracle_verdict(Ok(&canary("RED", true))).0, Verdict::Red);
    assert_eq!(
        oracle_verdict(Ok(&canary("PASS", false))).0,
        Verdict::NotGraded,
        "an always-OK oracle"
    );
    assert_eq!(
        oracle_verdict(Ok("{\"verdict\":\"PASS\"}")).0,
        Verdict::NotGraded,
        "no canary field"
    );
    assert_eq!(
        oracle_verdict(Ok(&canary("MAYBE", true))).0,
        Verdict::NotGraded,
        "an unknown verdict"
    );
    assert_eq!(oracle_verdict(Ok("not json")).0, Verdict::NotGraded);
    assert_eq!(oracle_verdict(Ok("")).0, Verdict::NotGraded);
    assert_eq!(
        oracle_verdict(Err("exit 1".into())).0,
        Verdict::NotGraded,
        "a crashed oracle"
    );
}

#[test]
fn a_sigterm_is_held_to_the_sigkill_of_the_same_point() {
    let kill = Left {
        staged: 1,
        running: 1,
        leases: 0,
    };
    assert_eq!(term_worse_than_kill(&kill.clone(), &kill), None);
    assert_eq!(
        term_worse_than_kill(
            &Left {
                staged: 0,
                running: 0,
                leases: 0
            },
            &kill
        ),
        None
    );
    for (more, why) in [
        (
            Left {
                staged: 2,
                ..kill.clone()
            },
            "SIGTERM left 2 staged .tmp file(s), SIGKILL left 1",
        ),
        (
            Left {
                running: 2,
                ..kill.clone()
            },
            "SIGTERM left 2 running run_status row(s), SIGKILL left 1",
        ),
        (
            Left {
                leases: 1,
                ..kill.clone()
            },
            "SIGTERM left 1 run lease row(s), SIGKILL left 0",
        ),
    ] {
        assert_eq!(term_worse_than_kill(&more, &kill).as_deref(), Some(why));
    }
}

#[test]
fn a_verdict_line_and_its_summary_carry_the_counts() {
    assert_eq!(
        line("pg/full/a/fault", Verdict::Red, "x\ny"),
        "P5 pg/full/a/fault RED x y"
    );
    assert_eq!(
        summary(&[
            Verdict::Pass,
            Verdict::Red,
            Verdict::Red,
            Verdict::NotGraded
        ]),
        "P5 SUMMARY pass=1 red=2 not_graded=1"
    );
}

#[test]
fn the_python_oracle_passes_its_own_self_test() {
    let out = std::process::Command::new("uv")
        .args([
            "run",
            "--frozen",
            "--quiet",
            "python",
            "-m",
            "dev.release_oracle.no_debris",
            "--self-test",
        ])
        .current_dir(root())
        .output()
        .expect("spawn `uv run`: install uv, the oracle is pinned by uv.lock");
    assert!(
        out.status.success() && String::from_utf8_lossy(&out.stdout).contains("self-test: ok"),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
