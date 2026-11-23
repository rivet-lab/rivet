//! The nightly's `e2e` job and `live-full` job each hand-maintain a `docker compose … up -d` service
//! list, and hand-maintained twins drift: `mssql-governor` was added to the E2E stack on 2026-08-17 and
//! missed in the nightly full run, so both governor canaries failed the 08-19 nightly with "requires
//! MssqlGovernor reachable on :1435" — the runner-bypass class, in workflow YAML.
//!
//! The invariant is directional, not equality: `live-full` runs the full live suite (a superset of
//! the E2E matrix), so every service the `e2e` job starts must be started by `live-full` too.

use std::collections::BTreeSet;

const NIGHTLY: &str = ".github/workflows/nightly-live.yml";

/// Every service named across all `docker compose … up -d …` lines of one job — flags (`--wait`,
/// `--profile x`) stripped, the union taken because the list may span several steps.
fn compose_services(path: &str, job: &str) -> BTreeSet<String> {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {path}: {e} (run from the repo root)"));
    let doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&text).unwrap_or_else(|e| panic!("parse {path}: {e}"));
    let steps = doc["jobs"][job]["steps"]
        .as_sequence()
        .unwrap_or_else(|| panic!("{path}: no job `{job}` with steps"));
    let mut out = BTreeSet::new();
    for line in steps
        .iter()
        .filter_map(|s| s["run"].as_str())
        .flat_map(|r| r.lines())
    {
        let Some(idx) = line.find("up -d") else {
            continue;
        };
        if !line.contains("docker compose") {
            continue;
        }
        let mut words = line[idx + "up -d".len()..].split_whitespace().peekable();
        while let Some(w) = words.next() {
            // A redirect or control operator ends the service list (`… oracle > log &`).
            if w.starts_with('>') || w.starts_with(['&', '|', ')', ';']) {
                break;
            }
            if w == "--wait" {
                continue;
            }
            if w == "--profile" {
                words.next(); // the profile name is not a service
                continue;
            }
            if w.starts_with("--") || w.starts_with('#') {
                continue;
            }
            out.insert(w.trim_end_matches(&['"', '\''][..]).to_string());
        }
    }
    assert!(
        !out.is_empty(),
        "{path} `{job}`: no `docker compose … up -d` service lists found — if the startup moved to \
         a script, point this guard at it rather than letting it pass on an empty set"
    );
    out
}

#[test]
fn the_full_live_job_covers_every_service_the_e2e_job_starts() {
    let e2e = compose_services(NIGHTLY, "e2e");
    let full = compose_services(NIGHTLY, "live-full");
    let missing: Vec<&String> = e2e.difference(&full).collect();
    assert!(
        missing.is_empty(),
        "nightly-live.yml `live-full` does not start {missing:?}, which its `e2e` job does. \
         `live-full` runs the FULL live suite — a superset of the E2E matrix — so a service the \
         E2E tests need is a service it needs too. Add it to the `docker compose … up -d` line \
         (and its wait/seed steps); this exact drift is how both governor canaries failed the \
         2026-08-19 nightly."
    );
}

#[test]
fn the_guard_reads_real_lists_not_an_empty_parse() {
    let e2e = compose_services(NIGHTLY, "e2e");
    for known in ["postgres", "mysql", "mssql", "mssql-governor", "minio"] {
        assert!(
            e2e.contains(known),
            "parser lost `{known}` from the e2e job — the subset check above is grading a \
             truncated list; parsed: {e2e:?}"
        );
    }
}

const STACK_FILES: [&str; 2] = [".github/actions/live-stack/action.yml", NIGHTLY];

/// What is wrong with how `text` waits for services and applies seeds: a shell polling loop (waiting is
/// `python3 -m dev.pytools.stand_wait`, which fails when a service never answered), a psql file run that goes
/// on after an error, a sqlcmd that exits 0 after one. The loop that waits for the background `docker compose up`
/// to leave its exit code (`compose.rc`) checks that code itself and is not one of them.
fn stack_problems(text: &str) -> Vec<String> {
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.starts_with('#'))
        .collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if line.contains("for i in $(seq") && !line.contains("compose.rc") {
            out.push(format!(
                "a shell polling loop (use `python3 -m dev.pytools.stand_wait`): {line}"
            ));
        }
        if line.contains("psql ") && line.contains(" -f ") && !line.contains("ON_ERROR_STOP=1") {
            out.push(format!(
                "psql applies a file without -v ON_ERROR_STOP=1: {line}"
            ));
        }
        let statement = if line.ends_with('\\') {
            format!("{line} {}", lines.get(i + 1).unwrap_or(&""))
        } else {
            (*line).to_string()
        };
        if statement.contains("sqlcmd")
            && (statement.contains(" -i ") || statement.contains(" -Q "))
            && !statement.contains(" -b ")
        {
            out.push(format!("sqlcmd without -b exits 0 after an error: {line}"));
        }
    }
    out
}

#[test]
fn a_service_that_never_came_up_or_a_seed_that_failed_fails_the_step() {
    for path in STACK_FILES {
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let problems = stack_problems(&text);
        assert!(
            problems.is_empty(),
            "{path}: {} problem(s), the first: {}",
            problems.len(),
            problems[0]
        );
    }
}

#[test]
fn the_stack_waits_through_the_python_tool() {
    for path in STACK_FILES {
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        assert!(
            text.contains("python3 -m dev.pytools.stand_wait"),
            "{path} no longer waits for its services through dev.pytools.stand_wait"
        );
    }
}

#[test]
fn the_stack_check_goes_red_on_each_spelling_it_guards() {
    assert_eq!(
        stack_problems("for i in $(seq 1 30); do pg_isready && break || sleep 2; done").len(),
        1,
        "a polling loop passed"
    );
    assert!(
        stack_problems(
            "for i in $(seq 1 600); do [ -f \"$RUNNER_TEMP/compose.rc\" ] && break; sleep 2; done"
        )
        .is_empty()
    );
    assert_eq!(
        stack_problems("psql -h h -d d -f seed.sql").len(),
        1,
        "psql -f without ON_ERROR_STOP passed"
    );
    assert!(stack_problems("psql -h h -d d -v ON_ERROR_STOP=1 -f seed.sql").is_empty());
    assert_eq!(
        stack_problems("sqlcmd -S s -C -i init.sql").len(),
        1,
        "sqlcmd -i without -b passed"
    );
    assert_eq!(
        stack_problems("sqlcmd -S s -C -Q 'CREATE DATABASE d'").len(),
        1,
        "sqlcmd -Q without -b passed"
    );
    assert!(stack_problems("sqlcmd -S s -C -b -i init.sql").is_empty());
    assert_eq!(
        stack_problems("docker compose exec -T mssql sqlcmd \\\n  -S s -C -i init.sql").len(),
        1,
        "a continued sqlcmd line passed"
    );
    assert!(
        stack_problems("# for i in $(seq 1 3); do x; done").is_empty(),
        "a comment was graded"
    );
}

const COMPOSE_UP_TOOL: &str = "python3 -m dev.pytools.compose_up docker compose";

/// Lines of `text` that run `docker compose … up` without the pull-retry tool in front of them.
fn bare_compose_ups(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.starts_with('#') && !l.starts_with("echo"))
        .filter(|l| l.contains("docker compose") && l.split_whitespace().any(|w| w == "up"))
        .filter(|l| !l.contains(COMPOSE_UP_TOOL))
        .map(str::to_string)
        .collect()
}

fn workflow_and_action_files() -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(".github/workflows").expect("list workflows") {
        out.push(entry.expect("workflow entry").path());
    }
    for entry in std::fs::read_dir(".github/actions").expect("list actions") {
        let path = entry.expect("action entry").path().join("action.yml");
        if path.exists() {
            out.push(path);
        }
    }
    out
}

#[test]
fn every_compose_up_in_ci_pulls_through_the_retry_tool() {
    let mut seen = 0;
    for path in workflow_and_action_files() {
        let text = std::fs::read_to_string(&path).expect("read workflow or action");
        seen += text.matches(COMPOSE_UP_TOOL).count();
        let bare = bare_compose_ups(&text);
        assert!(
            bare.is_empty(),
            "{}: `docker compose … up` without `{COMPOSE_UP_TOOL}` (a registry error on the pull fails the \
             whole step, as in gate run 45232816173): {}",
            path.display(),
            bare[0]
        );
    }
    assert!(
        seen >= 2,
        "found {seen} wrapped compose ups: the scan lost its target"
    );
}

#[test]
fn the_compose_up_check_goes_red_on_a_bare_up() {
    assert_eq!(
        bare_compose_ups("run: docker compose up -d postgres").len(),
        1
    );
    assert_eq!(
        bare_compose_ups("( docker compose --profile cdc up -d x > log 2>&1 ) &").len(),
        1
    );
    assert!(bare_compose_ups(&format!("run: {COMPOSE_UP_TOOL} up -d postgres")).is_empty());
    assert!(bare_compose_ups("docker compose exec -T postgres psql").is_empty());
    assert!(bare_compose_ups("echo \"::error::docker compose up did not finish\"").is_empty());
}
