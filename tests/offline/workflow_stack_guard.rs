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
