//! Each flock-backed nextest test-group names exactly the live tests that take its flock.

use std::collections::BTreeSet;

/// (test-group, the call that takes its cross-process flock)
const GROUPS: &[(&str, &str)] = &[
    ("toxiproxy", "toxiproxy_guard()"),
    ("mssql_cdc", "cross_process_serial(\"mssql_cdc\")"),
    ("oracle_cdc", "cross_process_serial(\"oracle_cdc\")"),
    ("mysql_replica", "cross_process_serial(\"mysql_replica\")"),
    ("pgbouncer", "pgbouncer_alone()"),
];

/// A top-level fn of a test source file.
struct Item {
    module: String,
    name: String,
    is_test: bool,
    calls: String,
}

/// Every top-level fn under `dir`, with its comment-free body below the signature line.
fn items_in(root: &std::path::Path, dir: &str) -> Vec<Item> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(root.join(dir))
        .unwrap_or_else(|e| panic!("read {dir}: {e}"))
        .flatten()
    {
        let p = e.path();
        let (Some(stem), Some("rs")) = (
            p.file_stem().and_then(|s| s.to_str()),
            p.extension().and_then(|s| s.to_str()),
        ) else {
            continue;
        };
        let text = std::fs::read_to_string(&p).expect("read a test source");
        let mut lines = text.lines();
        let mut test_next = false;
        while let Some(line) = lines.next() {
            if line.trim() == "#[test]" {
                test_next = true;
                continue;
            }
            let sig = ["fn ", "pub fn ", "pub(crate) fn ", "async fn "]
                .iter()
                .find_map(|k| line.strip_prefix(k));
            let Some(sig) = sig else {
                if !line.trim_start().starts_with("#[") {
                    test_next = false;
                }
                continue;
            };
            let name = sig.split(['(', '<']).next().unwrap().trim().to_string();
            let mut calls = String::new();
            if !line.ends_with('}') {
                for l in lines.by_ref() {
                    if l == "}" {
                        break;
                    }
                    if !l.trim_start().starts_with("//") {
                        calls.push_str(l);
                        calls.push('\n');
                    }
                }
            }
            out.push(Item {
                module: stem.to_string(),
                name,
                is_test: std::mem::take(&mut test_next),
                calls,
            });
        }
    }
    out
}

/// `(module::test, body)` for every `#[test]` under tests/live.
fn live_tests(root: &std::path::Path) -> Vec<(String, String)> {
    items_in(root, "tests/live")
        .into_iter()
        .filter(|i| i.is_test)
        .map(|i| (format!("{}::{}", i.module, i.name), i.calls))
        .collect()
}

/// Every `pub fn` of tests/common/toxi.rs that changes the admin API's state: all but the guard.
fn toxi_mutators(root: &std::path::Path) -> BTreeSet<String> {
    let text = std::fs::read_to_string(root.join("tests/common/toxi.rs")).expect("toxi.rs");
    let out: BTreeSet<String> = text
        .lines()
        .filter_map(|l| l.strip_prefix("pub fn "))
        .filter_map(|l| l.split_once('(').map(|(n, _)| n.to_string()))
        .filter(|n| n != "toxiproxy_guard")
        .collect();
    assert!(out.len() >= 6, "found {out:?} — the toxi.rs scan is broken");
    out
}

/// True when `body` calls `name(` as a whole identifier.
fn calls_fn(body: &str, name: &str) -> bool {
    let pat = format!("{name}(");
    body.match_indices(&pat).any(|(i, _)| {
        !body[..i]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
    })
}

/// `module::test` for every live test whose body reaches a toxi mutator, directly or through a helper fn.
fn mutator_users(root: &std::path::Path) -> BTreeSet<String> {
    let mut mutating = toxi_mutators(root);
    let mut items = items_in(root, "tests/live");
    items.extend(items_in(root, "tests/common"));
    loop {
        let before = mutating.len();
        for i in items.iter().filter(|i| !i.is_test) {
            if mutating.iter().any(|m| calls_fn(&i.calls, m)) {
                mutating.insert(i.name.clone());
            }
        }
        if mutating.len() == before {
            break;
        }
    }
    items
        .iter()
        .filter(|i| i.is_test && mutating.iter().any(|m| calls_fn(&i.calls, m)))
        .map(|i| format!("{}::{}", i.module, i.name))
        .collect()
}

/// `module::test` for every `#[test]` under tests/live whose body holds `call`.
fn guard_users(root: &std::path::Path, call: &str) -> BTreeSet<String> {
    live_tests(root)
        .into_iter()
        .filter(|(_, body)| body.contains(call))
        .map(|(name, _)| name)
        .collect()
}

/// `module::test` for every `test(=…)` in the override that assigns `test-group = '<group>'`.
fn grouped(toml: &str, group: &str) -> BTreeSet<String> {
    let assign = format!("test-group = '{group}'");
    let block = toml
        .split("[[profile.default.overrides]]")
        .find(|b| b.contains(&assign))
        .unwrap_or_else(|| panic!("no override assigns the {group} test-group"));
    block
        .split("test(=")
        .skip(1)
        .map(|s| s.split(')').next().unwrap().to_string())
        .collect()
}

#[test]
fn every_flock_test_group_names_every_flock_user_and_nothing_else() {
    let root = super::nonvacuity::repo_root();
    let toml = std::fs::read_to_string(root.join(".config/nextest.toml")).expect("nextest.toml");
    for (group, call) in GROUPS {
        let mut users = guard_users(&root, call);
        if *group == "toxiproxy" {
            users.extend(mutator_users(&root));
        }
        assert!(
            users.len() >= 2,
            "found {} `{call}` users — the scan is broken",
            users.len()
        );
        assert_eq!(
            grouped(&toml, group),
            users,
            "a test that takes `{call}` must sit in the `{group}` test-group (.config/nextest.toml), \
             or it waits on the flock inside its own timeout, holding a runner slot"
        );
        assert!(
            toml.contains(&format!("{group} = {{ max-threads = 1 }}")),
            "the `{group}` test-group must run one test at a time: its users share one flock"
        );
    }
}

/// A live test that changes the Toxiproxy admin state holds the suite-wide guard.
#[test]
fn every_toxi_mutating_test_holds_the_guard() {
    let root = super::nonvacuity::repo_root();
    let holders = guard_users(&root, "toxiproxy_guard()");
    let bare: Vec<String> = mutator_users(&root).difference(&holders).cloned().collect();
    assert!(
        bare.is_empty(),
        "these tests call a toxi mutator without `toxiproxy_guard()` and can delete a toxic another test armed: {bare:?}"
    );
}

/// Every cell nextest runs alone names a live test that exists.
#[test]
fn every_exclusive_cell_names_a_live_test() {
    let root = super::nonvacuity::repo_root();
    let toml = std::fs::read_to_string(root.join(".config/nextest.toml")).expect("nextest.toml");
    let live: BTreeSet<String> = live_tests(&root).into_iter().map(|(n, _)| n).collect();
    let named: BTreeSet<String> = toml
        .split("[[profile.default.overrides]]")
        .filter(|b| b.contains("threads-required = 'num-test-threads'"))
        .flat_map(|b| b.split("test(=").skip(1))
        .map(|s| s.split(')').next().unwrap().to_string())
        .collect();
    assert_ne!(
        named,
        BTreeSet::new(),
        "no exclusive cell found — the scan is broken"
    );
    assert!(
        named.is_subset(&live),
        "an exclusive nextest cell names no live test (renamed?): {:?}",
        named.difference(&live).collect::<Vec<_>>()
    );
}

/// Every `retries` setting of the file that is not the literal `retries = 0`.
fn retrying_lines(toml: &str) -> Vec<String> {
    toml.lines()
        .map(str::trim)
        .filter(|l| l.starts_with("retries") && l.replace(' ', "") != "retries=0")
        .map(str::to_string)
        .collect()
}

#[test]
fn no_nextest_profile_retries_a_failed_test() {
    let root = super::nonvacuity::repo_root();
    let toml = std::fs::read_to_string(root.join(".config/nextest.toml")).expect("nextest.toml");
    assert_eq!(
        retrying_lines(&toml),
        Vec::<String>::new(),
        "a retried test reads green after failing once"
    );
    assert!(
        toml.matches("retries = 0").count() >= 2,
        "the default and pr profiles pin retries = 0"
    );
    assert_eq!(
        retrying_lines(
            "[profile.ci]\nretries = 1\n[profile.x]\nretries = { backoff = \"fixed\", count = 2 }\n"
        )
        .len(),
        2,
        "a numeric or table retries is named"
    );
}
