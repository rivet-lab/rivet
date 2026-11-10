//! no_debris_after_stop (no debris after a stop, a re-run equals an uninterrupted run): the axes are read from the code and the oracle contract is strict.

#[path = "../live/property_no_debris_after_stop_axes.rs"]
mod property_no_debris_after_stop_axes;

use property_no_debris_after_stop_axes::*;
use std::collections::BTreeSet;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn engines() -> Vec<String> {
    ENGINES.iter().map(|e| e.to_string()).collect()
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
        "src/test_hook.rs has primitive(s) {extra:?} that no_debris_after_stop has no Kind for: a new way to stop a run is a new axis value"
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
fn the_engine_axis_is_the_source_type_enum() {
    let scanned: BTreeSet<String> = enum_variants(&root(), "src/config/source.rs", "SourceType")
        .iter()
        .map(|v| match v.as_str() {
            "Postgres" => "pg".to_string(),
            other => other.to_lowercase(),
        })
        .collect();
    let declared: BTreeSet<String> = ENGINES.iter().map(|e| e.to_string()).collect();
    assert_eq!(scanned, declared, "ENGINES must be the SourceType variants");
    let sql: BTreeSet<String> = enum_variants(&root(), "tests/common/sql_engine.rs", "SqlEngine")
        .iter()
        .map(|v| v.to_lowercase())
        .collect();
    let without_mongo: BTreeSet<String> = declared.into_iter().filter(|e| e != "mongo").collect();
    assert_eq!(
        sql, without_mongo,
        "every SQL engine of SourceType has a SqlEngine to seed it"
    );
}

#[test]
fn a_shape_reaches_only_the_engines_that_can_run_it() {
    let all = cells(&root(), &engines());
    for c in &all {
        assert!(
            c.shape.fits(&c.engine),
            "{} on an engine that cannot run it",
            c.key()
        );
    }
    let keys: BTreeSet<String> = all.iter().map(Cell::key).collect();
    assert!(keys.contains("mongo/mongo_full/before_commit_rename/sigkill"));
    assert!(!keys.contains("mongo/full/before_commit_rename/sigkill"));
    assert!(!keys.contains("pg/mongo_full/before_commit_rename/sigkill"));
    assert!(
        all.iter()
            .any(|c| c.engine == "mongo" && c.not_graded().is_none())
    );
}

#[test]
fn every_export_mode_has_a_shape() {
    let modes: BTreeSet<String> = enum_variants(&root(), "src/config/export.rs", "ExportMode")
        .iter()
        .map(|v| snake(v))
        .collect();
    let shaped: BTreeSet<String> = Shape::ALL.iter().map(|s| s.mode().to_string()).collect();
    let none: Vec<_> = modes.difference(&shaped).collect();
    assert_eq!(
        none,
        Vec::<&String>::new(),
        "export mode(s) have no Shape in property_no_debris_after_stop_axes"
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
        "no_debris_after_stop pg/full/a/fault RED x y"
    );
    assert_eq!(
        summary(&[
            Verdict::Pass,
            Verdict::Red,
            Verdict::Red,
            Verdict::NotGraded
        ]),
        "no_debris_after_stop SUMMARY pass=1 red=2 not_graded=1"
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
            "dev.release_oracle.property_no_debris_after_stop",
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

#[test]
fn the_warehouse_axis_is_the_load_target_enum() {
    let scanned: BTreeSet<String> = enum_variants(&root(), "src/config/load.rs", "LoadTarget")
        .iter()
        .map(|v| snake(v))
        .collect();
    let declared: BTreeSet<String> = Warehouse::ALL
        .iter()
        .map(|w| w.label().to_string())
        .collect();
    assert_eq!(
        scanned, declared,
        "Warehouse must be the LoadTarget variants"
    );
    assert_eq!(
        Warehouse::serving("src/load/clickhouse.rs"),
        vec![Warehouse::Clickhouse]
    );
    assert_eq!(
        Warehouse::serving("src/load/mod.rs"),
        Warehouse::ALL.to_vec()
    );
}

#[test]
fn a_load_cell_runs_on_clickhouse_and_names_each_cloud_warehouse_it_cannot_reach() {
    let all = cells(&root(), &engines());
    let load: Vec<&Cell> = all.iter().filter(|c| c.shape == Shape::Load).collect();
    assert!(
        load.iter().any(|c| c.not_graded().is_none()),
        "no runnable load cell"
    );
    for c in &load {
        let on_stand = Warehouse::serving(&c.hook.file).contains(&Warehouse::Clickhouse);
        if !on_stand {
            assert!(
                c.not_graded().is_some(),
                "{} serves no stand warehouse",
                c.key()
            );
        }
    }
    let keys: BTreeSet<String> = variants(&root(), &all).iter().map(Variant::key).collect();
    assert!(keys.contains("pg/load/load_after_adopt/fault@warehouse=bigquery"));
    assert!(!keys.iter().any(|k| k.ends_with("@warehouse=clickhouse")));
    assert!(
        variants(&root(), &all)
            .iter()
            .filter(|v| v.axis == Axis::Warehouse)
            .all(|v| v.not_graded().is_some())
    );
}

#[test]
fn every_axis_reads_its_values_from_the_code() {
    let r = root();
    let mutation: BTreeSet<String> = Axis::Mutation.values(&r).into_iter().collect();
    for op in ["insert", "update", "delete"] {
        assert!(mutation.contains(op), "ChangeOp lost {op}: {mutation:?}");
    }
    assert!(SCHEMA_MUTATIONS.iter().all(|m| mutation.contains(*m)));
    let refusal = Axis::Refusal.values(&r);
    assert!(
        refusal.contains(&"RIVET_STATE_RUN_IN_PROGRESS".to_string()),
        "{refusal:?}"
    );
    assert!(refusal.len() >= 4, "errors.md busy codes: {refusal:?}");
    let entry: BTreeSet<String> = Axis::Entry.values(&r).into_iter().collect();
    for cmd in ["apply", "state", "load", "cdc", "compact"] {
        assert!(entry.contains(cmd), "Commands lost {cmd}: {entry:?}");
    }
    assert!(!entry.contains("run"), "run is the base entry");
    let dest: BTreeSet<String> = Axis::Destination.values(&r).into_iter().collect();
    let want: BTreeSet<String> = ["s3", "gcs", "azure", "stdout"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(dest, want, "DestinationType beside local");
    let state = Axis::State.values(&r);
    assert_eq!(state[0], "postgres", "StateRef beside sqlite: {state:?}");
    assert_eq!(Axis::Parallel.values(&r), vec!["4".to_string()]);
}

#[test]
fn each_point_is_a_hook_the_scan_finds_and_every_axis_grades_somewhere() {
    let names: BTreeSet<String> = hooks(&root()).into_iter().map(|h| h.name).collect();
    for (point, _) in POINTS {
        assert!(
            names.contains(point),
            "the point {point} is no hook any more"
        );
    }
    let all = cells(&root(), &engines());
    let vs = variants(&root(), &all);
    let keys: BTreeSet<String> = vs.iter().map(Variant::key).collect();
    assert_eq!(keys.len(), vs.len(), "variant keys repeat");
    for axis in Axis::AT_POINTS {
        assert!(
            vs.iter()
                .any(|v| v.axis == axis && v.not_graded().is_none()),
            "the {} axis grades no variant",
            axis.label()
        );
    }
    for v in &vs {
        if let Some(why) = v.not_graded() {
            assert!(!why.trim().is_empty(), "{} has an empty reason", v.key());
        }
    }
    assert!(keys.contains("pg/full/before_commit_rename/sigkill@mutation=truncate"));
    assert!(keys.contains("pg/full/after_file_write/fault@entry=apply"));
}

#[test]
fn a_host_crash_and_a_source_event_are_named_and_not_graded() {
    let all = cells(&root(), &engines());
    for stop in [Stop::HostCrash, Stop::SourceEvent] {
        let of: Vec<&Cell> = all.iter().filter(|c| c.stop == stop).collect();
        assert!(!of.is_empty(), "no {} cell", stop.label());
        assert!(of.iter().all(|c| c.not_graded().is_some()));
    }
}

#[test]
fn a_known_red_entry_is_dated_matches_a_cell_and_names_a_finding() {
    let all = cells(&root(), &engines());
    let mut keys: Vec<String> = all.iter().map(Cell::key).collect();
    keys.extend(variants(&root(), &all).iter().map(Variant::key));
    keys.extend(all.iter().map(|c| {
        format!(
            "{}/{}/{}/sigterm_vs_sigkill",
            c.engine,
            c.shape.label(),
            c.hook.name
        )
    }));
    keys.extend(
        all.iter()
            .map(|c| format!("{}/*/{}/fault_exit_class", c.engine, c.hook.name)),
    );
    for k in KNOWN_RED {
        let date = k.reason.get(..10).unwrap_or("");
        assert!(
            date.len() == 10
                && date.chars().filter(|c| *c == '-').count() == 2
                && k.reason[10..].starts_with(':'),
            "{} has no dated reason: {:?}",
            k.keys,
            k.reason
        );
        assert!(!k.finding.is_empty(), "{} names no finding", k.keys);
        assert!(
            keys.iter().any(|key| glob(k.keys, key)),
            "{} matches no cell",
            k.keys
        );
        assert!(
            ENGINES.iter().any(|e| k.keys.starts_with(&format!("{e}/"))),
            "{} names no engine",
            k.keys
        );
    }
    assert!(glob(
        "pg/*/before_commit_rename/sigkill",
        "pg/full/before_commit_rename/sigkill"
    ));
    assert!(!glob("pg/*/x", "pg/full/y"));
    assert!(known_red("pg/no_such_cell", "").is_none());
    assert!(glob(
        "pg/range/*",
        "pg/range/after_chunk_file/fault@parallel=4"
    ));
}
