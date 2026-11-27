//! The known-red ledger (dev/release_oracle/known_red.py) is held by three guards: every entry
//! carries a dated reason (its count is the `known-red-entries` pin that pr_ready.py grades
//! against the base), its horizon never moves unseen, and every open-defect entry names a live
//! cell that exists. A red gate is red ONLY for recorded reasons; this keeps the
//! record from quietly becoming a place where reds go to be forgotten.

use std::path::PathBuf;

const LEDGER: &str = "dev/release_oracle/known_red.py";
const EXCUSES: &str = "dev/release_oracle/skip_allow.py";

// ratchet-pin: known-red-entries (review 2027-02-01) count dev/release_oracle/known_red.py ^    (_open|KnownRed)\(

/// The horizon every known-red entry and every excused skip expires at; moves only here and in both ledgers together.
const EXPIRES: &str = "2026-11-29";

/// The one date past the horizon the ledger may hold, as `_BASELINE_EXPIRES`: the ab-diff rows of the keyset_parallel part count, which close when 0.34.0 is the previous release.
const BASELINE_EXPIRES: &str = "2026-12-15";

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn ledger() -> String {
    std::fs::read_to_string(root().join(LEDGER)).expect("the known-red ledger")
}

/// The body of `KNOWN_RED: tuple[KnownRed, ...] = ( ... )`.
fn entries_block(text: &str) -> &str {
    let start = text
        .find("KNOWN_RED: tuple[KnownRed, ...] = (")
        .expect("the ledger declares KNOWN_RED");
    let rest = &text[start..];
    let end = rest
        .find("\n)\n")
        .expect("the tuple closes on its own line");
    &rest[..end]
}

/// `(open cells, gate rows, nightly matrices)`: the `module::fn` of every `_open(` entry, the match text of every bare `KnownRed(` entry and the matrix of every `KnownRed(nightly_key(` entry.
fn entries(text: &str) -> (Vec<String>, Vec<String>, Vec<String>) {
    let block = entries_block(text);
    let mut open = Vec::new();
    let mut rows = Vec::new();
    let mut nightly = Vec::new();
    for line in block.lines() {
        let t = line.trim_start();
        if let Some(rest) = t.strip_prefix("_open(\"") {
            open.push(rest.split('"').next().unwrap().to_string());
        } else if let Some(rest) = t.strip_prefix("KnownRed(nightly_key(\"") {
            nightly.push(rest.split('"').next().unwrap().to_string());
        } else if let Some(rest) = t
            .strip_prefix("KnownRed('")
            .or_else(|| t.strip_prefix("KnownRed(\""))
        {
            rows.push(rest.split(['\'', '"']).next().unwrap().to_string());
        }
    }
    (open, rows, nightly)
}

/// Run a Python module of the gate from the repo root.
fn python(args: &[&str]) -> std::process::Output {
    std::process::Command::new("python3")
        .args(args)
        .current_dir(root())
        .output()
        .expect("python3 is a prerequisite of every gate script in this repo")
}

/// The entry count the ledger's own check prints once every reason is dated.
fn ledger_count() -> usize {
    let out = python(&["-m", "dev.release_oracle.known_red"]);
    let said = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "a known-red entry carries no dated reason (YYYY-MM-DD saying why it is accepted and what closes it):\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    said.trim()
        .strip_prefix("known_red: ")
        .and_then(|r| r.split(' ').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("unreadable ledger check output: {said}"))
}

#[test]
fn every_known_red_entry_carries_a_dated_reason() {
    let (open, rows, nightly) = entries(&ledger());
    super::nonvacuity::require_enumerated(
        open.len() + rows.len() + nightly.len(),
        2,
        "known-red entries",
        "the ledger moved or changed shape",
    );
    assert_eq!(
        open.len() + rows.len() + nightly.len(),
        ledger_count(),
        "the entry parser of this guard and the ledger disagree on how many entries there are"
    );
}

#[test]
fn the_ledger_check_grades_its_own_fixture() {
    let out = python(&["-m", "dev.release_oracle.known_red", "--self-test"]);
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "known_red self-test: ok",
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn the_entry_pin_counts_every_ledger_entry() {
    let out = python(&["dev/pytools/pr_ready.py", "pins"]);
    let said = String::from_utf8_lossy(&out.stdout);
    let pinned: usize = said
        .lines()
        .find_map(|l| l.strip_prefix("tests/offline/known_red_guard.rs:known-red-entries "))
        .and_then(|n| n.trim().parse().ok())
        .unwrap_or_else(|| {
            panic!(
                "pr_ready.py pins lists no known-red-entries pin:\n{said}{}",
                String::from_utf8_lossy(&out.stderr)
            )
        });
    assert_eq!(
        pinned,
        ledger_count(),
        "the known-red-entries census (the `ratchet-pin:` line in this file) misses an entry shape the ledger holds"
    );
}

#[test]
fn the_known_red_horizon_moves_only_with_this_guard() {
    let is_date = |s: &str| {
        s.len() == 10
            && s.bytes().enumerate().all(|(i, b)| {
                if i == 4 || i == 7 {
                    b == b'-'
                } else {
                    b.is_ascii_digit()
                }
            })
    };
    for path in [LEDGER, EXCUSES] {
        let text = std::fs::read_to_string(root().join(path)).expect("a dated ledger");
        assert!(
            text.contains(&format!("\n_EXPIRES = \"{EXPIRES}\"\n")),
            "{path}: the horizon is {EXPIRES} and moves only together with this guard: an extension is a reviewed diff, not an edit"
        );
        let later: Vec<&str> = text
            .match_indices('"')
            .filter_map(|(i, _)| {
                text.get(i + 1..i + 11)
                    .filter(|s| is_date(s) && text[i + 11..].starts_with('"'))
            })
            .filter(|s| *s > EXPIRES)
            .collect();
        let allowed: Vec<&str> = if path == LEDGER
            && text.contains(&format!("\n_BASELINE_EXPIRES = \"{BASELINE_EXPIRES}\"\n"))
        {
            vec![BASELINE_EXPIRES]
        } else {
            Vec::new()
        };
        assert_eq!(
            later, allowed,
            "{path}: dates past the horizon {EXPIRES}. An entry that must outlive the horizon moves the horizon here, in the open."
        );
    }
}

#[test]
fn every_open_entry_names_a_live_cell_that_exists() {
    let (open, rows, nightly) = entries(&ledger());
    let tests = super::chunking_matrix_guard::all_test_fn_names();
    let missing: Vec<&String> = open
        .iter()
        .filter(|cell| {
            let (module, fn_name) = cell.rsplit_once("::").unwrap_or(("", cell));
            let file = root().join("tests/live").join(format!("{module}.rs"));
            !fn_name.starts_with("open_defect_")
                || !tests.contains(fn_name)
                || !std::fs::read_to_string(&file)
                    .is_ok_and(|t| t.contains(&format!("fn {fn_name}(")))
        })
        .collect();
    assert_eq!(
        missing,
        Vec::<&String>::new(),
        "known-red entries that name no `open_defect_*` test under tests/live/<module>.rs. \
         A fixed defect deletes its entry; a renamed cell renames its entry."
    );
    // A gate row names a cell family the gate composes (`upgrade[...]`, `sentinels[...]`).
    let gate: String = std::fs::read_dir(root().join("dev/release_oracle"))
        .expect("the gate's sources")
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "py"))
        .map(|e| std::fs::read_to_string(e.path()).unwrap_or_default())
        .collect();
    let orphan_rows: Vec<&String> = rows
        .iter()
        .filter(|m| {
            let family = m.split(['[', ':']).next().unwrap_or("");
            family.is_empty() || !gate.contains(&format!("\"{family}["))
        })
        .collect();
    assert_eq!(
        orphan_rows,
        Vec::<&String>::new(),
        "known-red gate rows whose cell family the gate no longer composes"
    );
    let dead_nightly: Vec<&String> = nightly
        .iter()
        .filter(|m| {
            !root()
                .join(format!("tests/offline/property_{m}.rs"))
                .is_file()
        })
        .collect();
    assert_eq!(
        dead_nightly,
        Vec::<&String>::new(),
        "nightly known-red entries whose matrix has no tests/offline/property_<matrix>.rs"
    );
}

#[test]
fn the_entry_parser_reads_every_shape() {
    let text = "x\nKNOWN_RED: tuple[KnownRed, ...] = (\n    _open(\"live_a::open_defect_one\",\n          \"s\", \"r\"),\n    KnownRed('upgrade[oracle/x]: boom',\n             \"why\",\n             \"2026-10-31\"),\n    KnownRed(\"sentinels[y]: z\", \"why\", \"2026-10-31\"),\n    KnownRed(nightly_key(\"m_x\", \"k\", \"c\"),\n             \"why\", \"2026-10-31\"),\n)\n\ndef match():\n";
    let (open, rows, nightly) = entries(text);
    assert_eq!(open, ["live_a::open_defect_one"]);
    assert_eq!(rows, ["upgrade[oracle/x]: boom", "sentinels[y]: z"]);
    assert_eq!(nightly, ["m_x"]);
}
