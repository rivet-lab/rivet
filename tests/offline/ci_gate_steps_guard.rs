//! The semantic gates are STEPS of the `Tests` job of ci.yml; the service-backed suites are steps of the E2E `pre` leg
//! and the censuses steps of the `E2E matrix` verdict job of nightly-live.yml. A step is easy to drop or to point at the
//! wrong filter in an edit that reads as tidying; this pins each one by its command.

use std::path::Path;

const ALWAYS: &str = "${{ !cancelled() }}";
/// The E2E job is a matrix; these run on its `pre` leg only, so once per check.
const PRE_LEG: &str = "${{ !cancelled() && matrix.leg == 'pre' }}";

/// (workflow, job id, step name, one line its `run:` must hold EXACTLY — a substring would accept `recovery_typo`, its `if:`)
const GATES: &[(&str, &str, &str, &str, &str)] = &[
    (
        "ci.yml",
        "test",
        "Invariant tests (semantic gate)",
        "cargo test --test invariants --test journal_invariants",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test",
        "Recovery tests (semantic gate)",
        "cargo test --test recovery",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test",
        "Compatibility matrix tests (semantic gate)",
        "cargo test --lib -- plan::validate::tests",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test",
        "Type mapping contracts (semantic gate)",
        "cargo test --test type_roundtrip contract_",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test",
        "Stability tests — format & row-group goldens (semantic gate)",
        "cargo test --test format_golden",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test",
        "Stability tests — sink unit tests (semantic gate)",
        "cargo test --lib -- pipeline::sink::tests",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test",
        "Generated docs are in sync (docs-as-code)",
        "python3 -m dev.pytools.docgen --check",
        ALWAYS,
    ),
    (
        "nightly-live.yml",
        "e2e",
        "Type-golden tests (semantic gate)",
        "cargo test --test live_type_golden -- --ignored",
        PRE_LEG,
    ),
    (
        "nightly-live.yml",
        "e2e",
        "Type round-trip validators — DuckDB · ClickHouse · pyarrow (semantic gate)",
        "cargo test --test type_roundtrip -- --include-ignored --skip bigquery",
        PRE_LEG,
    ),
    (
        "nightly-live.yml",
        "e2e",
        "Differential correctness at scale (DuckDB vs source)",
        "cargo test --test live_differential -- --ignored",
        PRE_LEG,
    ),
    (
        "nightly-live.yml",
        "e2e",
        "PR regression matrices (cli + cfg + path)",
        "python3 -m dev.pytools.matrices --tier=pr --skip-compose | tee dev/matrices/run.log",
        PRE_LEG,
    ),
    (
        "nightly-live.yml",
        "e2e-matrix",
        "Self-skip census",
        "python3 -m dev.release_oracle.skip_census target/rivet-skips.log --lacking \"$LACKING\"",
        ALWAYS,
    ),
    (
        "nightly-live.yml",
        "e2e-matrix",
        "Rig oracle verdict census",
        "python3 -m dev.release_oracle.skip_census --verdicts target/rivet-oracle.log --lane ci",
        ALWAYS,
    ),
];

fn workflow(file: &str) -> serde_yaml_ng::Value {
    let text = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(".github/workflows")
            .join(file),
    )
    .unwrap_or_else(|e| panic!("read {file}: {e}"));
    serde_yaml_ng::from_str(&text).unwrap_or_else(|e| panic!("parse {file}: {e}"))
}

fn steps_of(wf: &serde_yaml_ng::Value, job: &str) -> Vec<serde_yaml_ng::Value> {
    wf["jobs"][job]["steps"]
        .as_sequence()
        .unwrap_or_else(|| panic!("no job `{job}` with steps"))
        .clone()
}

/// Every former gate job still runs, under its own name, with its own filter.
#[test]
fn every_semantic_gate_is_a_step_with_its_command() {
    for (file, job, name, cmd, cond) in GATES {
        let steps = steps_of(&workflow(file), job);
        let step = steps
            .iter()
            .find(|s| s["name"].as_str() == Some(*name))
            .unwrap_or_else(|| panic!("job `{job}` lost its `{name}` step"));
        let run = step["run"].as_str().unwrap_or_default();
        assert!(
            run.lines().any(|l| l.trim() == *cmd),
            "`{name}` in job `{job}` no longer runs `{cmd}` — got `{run}`"
        );
        assert_eq!(
            step["if"].as_str(),
            Some(*cond),
            "`{name}` must run even after an earlier gate fails, or a red run names only the first"
        );
    }
}

/// `Some(n)` when `legs` is `pre` once plus every partition `i/n` of ONE n, each once.
fn partitions_of(legs: &[&str]) -> Option<usize> {
    let parts: Vec<&str> = legs.iter().copied().filter(|l| *l != "pre").collect();
    let n = parts.len();
    let mut want: Vec<String> = (1..=n).map(|i| format!("{i}/{n}")).collect();
    let mut got: Vec<String> = parts.iter().map(|s| s.to_string()).collect();
    want.sort();
    got.sort();
    (n > 0 && legs.len() == n + 1 && got == want).then_some(n)
}

/// A leg list short of one partition drops a third of the live suite and stays green.
#[test]
fn the_e2e_legs_are_pre_plus_every_partition_and_the_verdict_job_needs_them() {
    assert_eq!(partitions_of(&["pre", "1/3", "2/3", "3/3"]), Some(3));
    assert_eq!(partitions_of(&["pre", "1/1"]), Some(1));
    for bad in [
        &["pre", "1/3", "2/3"][..],
        &["pre", "1/3", "2/3", "3/4"],
        &["pre", "1/3", "1/3", "3/3"],
        &["1/2", "2/2"],
        &["pre", "pre", "1/1"],
        &["pre"],
    ] {
        assert_eq!(partitions_of(bad), None, "{bad:?}");
    }
    let ci = workflow("nightly-live.yml");
    let legs: Vec<String> = ci["jobs"]["e2e"]["strategy"]["matrix"]["leg"]
        .as_sequence()
        .expect("the e2e job has a `leg` matrix")
        .iter()
        .map(|v| v.as_str().expect("a leg is a string").to_string())
        .collect();
    let legs: Vec<&str> = legs.iter().map(String::as_str).collect();
    assert!(
        partitions_of(&legs).is_some(),
        "the e2e legs {legs:?} are not `pre` plus every `i/N` of one N: a partition nobody runs is a silent cut"
    );
    let sweep = steps_of(&ci, "e2e")
        .into_iter()
        .find(|s| s["name"].as_str() == Some("Run live cargo integration tests"))
        .expect("the e2e job runs the live sweep");
    assert_eq!(
        sweep["if"].as_str(),
        Some("${{ !cancelled() && matrix.leg != 'pre' }}"),
        "every partition leg runs the sweep"
    );
    let run = sweep["run"].as_str().unwrap_or_default();
    for flag in [
        "--partition \"count:${{ matrix.leg }}\"",
        "--run-ignored only ",
        "--profile pr ",
    ] {
        assert!(run.contains(flag), "the live sweep lost `{flag}`: {run}");
    }
    let verdict = &ci["jobs"]["e2e-matrix"];
    let needs: Vec<&str> = verdict["needs"]
        .as_sequence()
        .map(|n| n.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    assert_eq!(needs, ["start", "e2e"]);
    assert_eq!(verdict["if"].as_str(), Some(STARTED));
    let first = &steps_of(&ci, "e2e-matrix")[0];
    assert!(
        first["if"].is_null()
            && first["run"]
                .as_str()
                .unwrap_or_default()
                .lines()
                .any(|l| l.trim() == r#"[ "${{ needs.e2e.result }}" = success ]"#),
        "the verdict job must fail unless every leg succeeded"
    );
}

/// A nightly job the verdict job does not wait for can go red and leave the run green.
#[test]
fn the_nightly_verdict_waits_for_every_other_nightly_job() {
    let nightly = workflow("nightly-live.yml");
    let jobs = nightly["jobs"].as_mapping().expect("nightly jobs");
    let mut want: Vec<&str> = jobs
        .keys()
        .filter_map(|k| k.as_str())
        .filter(|k| *k != "nightly-verdict")
        .collect();
    let mut got: Vec<&str> = nightly["jobs"]["nightly-verdict"]["needs"]
        .as_sequence()
        .expect("nightly-verdict needs the other jobs")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    want.sort();
    got.sort();
    assert_eq!(
        got, want,
        "nightly-verdict must need exactly every other nightly job"
    );
    assert_eq!(
        nightly["jobs"]["nightly-verdict"]["if"].as_str(),
        Some(STARTED)
    );
}

const STARTED: &str = "${{ always() && needs.start.result == 'success' }}";

const FREE_DISK: &str = "./.github/actions/free-disk";

/// A job that brings a compose stack up frees the runner's disk first, through the one shared action.
#[test]
fn every_job_that_starts_a_compose_stack_frees_the_disk_first() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let action = std::fs::read_to_string(root.join(".github/actions/free-disk/action.yml"))
        .expect("read the free-disk action");
    assert!(
        action.contains("jlumbroso/free-disk-space@") && action.contains("df -h /"),
        "the shared free-disk action must run the cleaner and print the disk"
    );
    let mut seen = 0;
    for entry in std::fs::read_dir(root.join(".github/workflows")).expect("list workflows") {
        let path = entry.expect("workflow entry").path();
        let text = std::fs::read_to_string(&path).expect("read workflow");
        let wf: serde_yaml_ng::Value = serde_yaml_ng::from_str(&text).expect("parse workflow");
        let Some(jobs) = wf["jobs"].as_mapping() else {
            continue;
        };
        for (job, body) in jobs {
            let steps = body["steps"].as_sequence().cloned().unwrap_or_default();
            let starts_stack = |s: &serde_yaml_ng::Value| {
                s["run"].as_str().unwrap_or_default().lines().any(|l| {
                    l.contains("docker compose") && l.split_whitespace().any(|w| w == "up")
                })
            };
            let Some(first_up) = steps.iter().position(starts_stack) else {
                continue;
            };
            seen += 1;
            assert!(
                steps[..first_up]
                    .iter()
                    .any(|s| s["uses"].as_str() == Some(FREE_DISK)),
                "{}: job `{}` starts a compose stack without `uses: {FREE_DISK}` before it",
                path.display(),
                job.as_str().unwrap_or("?")
            );
        }
    }
    assert!(
        seen >= 2,
        "found {seen} compose jobs: the scan lost its target"
    );
}
