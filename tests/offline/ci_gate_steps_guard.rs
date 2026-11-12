//! The semantic gates are STEPS of the `Tests (semantic gates)` job of ci.yml; the service-backed suites are steps of the E2E `pre` leg
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
        "test-gates",
        "Invariant tests (semantic gate)",
        "cargo test --test invariants --test journal_invariants",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test-gates",
        "Recovery tests (semantic gate)",
        "cargo test --test recovery",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test-gates",
        "Compatibility matrix tests (semantic gate)",
        "cargo test --lib -- plan::validate::tests",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test-gates",
        "Type mapping contracts (semantic gate)",
        "cargo test --test type_roundtrip contract_",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test-gates",
        "Stability tests — format & row-group goldens (semantic gate)",
        "cargo test --test format_golden",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test-gates",
        "Stability tests — sink unit tests (semantic gate)",
        "cargo test --lib -- pipeline::sink::tests",
        ALWAYS,
    ),
    (
        "ci.yml",
        "test-gates",
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

/// `Some(n)` when `parts` is every partition `i/n` of ONE n, each once.
fn every_partition(parts: &[&str]) -> Option<usize> {
    let n = parts.len();
    let mut want: Vec<String> = (1..=n).map(|i| format!("{i}/{n}")).collect();
    let mut got: Vec<String> = parts.iter().map(|s| s.to_string()).collect();
    want.sort();
    got.sort();
    (n > 0 && got == want).then_some(n)
}

/// `Some(n)` when `legs` is `pre` once plus every partition `i/n` of ONE n, each once.
fn partitions_of(legs: &[&str]) -> Option<usize> {
    let parts: Vec<&str> = legs.iter().copied().filter(|l| *l != "pre").collect();
    (legs.len() == parts.len() + 1)
        .then(|| every_partition(&parts))
        .flatten()
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

/// A shard nobody runs, or a `Tests` check that does not wait for every `Tests (...)` job, is a silent cut.
#[test]
fn the_tests_check_fails_unless_every_shard_and_the_gates_succeeded() {
    assert_eq!(every_partition(&["1/3", "2/3", "3/3"]), Some(3));
    for bad in [
        &["1/3", "2/3"][..],
        &["1/3", "1/3", "3/3"],
        &["1/3", "2/4"],
        &[],
    ] {
        assert_eq!(every_partition(bad), None, "{bad:?}");
    }
    let ci = workflow("ci.yml");
    let parts: Vec<String> = ci["jobs"]["test"]["strategy"]["matrix"]["part"]
        .as_sequence()
        .expect("the `test` job has a `part` matrix")
        .iter()
        .map(|v| v.as_str().expect("a part is a string").to_string())
        .collect();
    let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
    assert!(
        every_partition(&parts).is_some(),
        "the test shards {parts:?} are not every `i/N` of one N: a partition nobody runs is a silent cut"
    );
    let run = steps_of(&ci, "test")
        .into_iter()
        .filter_map(|s| s["run"].as_str().map(str::to_string))
        .collect::<Vec<_>>()
        .join("\n");
    for flag in [
        "cargo nextest run ",
        "--all-targets ",
        "--profile pr ",
        "--partition hash:${{ matrix.part }}",
    ] {
        assert!(run.contains(flag), "the test shards lost `{flag}`: {run}");
    }
    let jobs = ci["jobs"].as_mapping().expect("ci jobs");
    let verdict = jobs
        .iter()
        .filter(|(_, j)| j["name"].as_str() == Some("Tests"))
        .map(|(k, _)| k.as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    assert_eq!(
        verdict.len(),
        1,
        "exactly one job is named `Tests`: {verdict:?}"
    );
    let verdict = &ci["jobs"][verdict[0]];
    let mut want: Vec<&str> = jobs
        .iter()
        .filter(|(_, j)| j["name"].as_str().is_some_and(|n| n.starts_with("Tests (")))
        .filter_map(|(k, _)| k.as_str())
        .collect();
    let mut got: Vec<&str> = verdict["needs"]
        .as_sequence()
        .expect("`Tests` needs the test jobs")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    want.sort();
    got.sort();
    assert_eq!(
        got, want,
        "`Tests` must need exactly every `Tests (...)` job"
    );
    assert_eq!(
        verdict["if"].as_str(),
        Some("${{ always() }}"),
        "a job skipped by a failed `needs` counts as passing for a required check"
    );
    let steps = verdict["steps"].as_sequence().expect("`Tests` has steps");
    assert_eq!(steps.len(), 1, "`Tests` is one verdict step");
    let run = steps[0]["run"].as_str().unwrap_or_default();
    let env = steps[0]["env"]
        .as_mapping()
        .expect("`Tests` reads each result from env");
    for job in want {
        let result = format!("${{{{ needs.{job}.result }}}}");
        let var = env
            .iter()
            .find(|(_, v)| v.as_str() == Some(result.as_str()))
            .and_then(|(k, _)| k.as_str())
            .unwrap_or_else(|| panic!("`Tests` never reads `{result}`"));
        assert!(
            run.contains(&format!("\"${var}\" = success")),
            "`Tests` does not require `{job}` to be `success`: {run}"
        );
    }
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
