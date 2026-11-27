//! Shrink-only ceilings for the rig's default oracle.

use std::path::{Path, PathBuf};

// ratchet-pin: no-oracle-opt-outs (review 2027-02-01) count tests/live/ \.no_oracle\(|run_rivet_ok_no_oracle\(|"RIVET_TEST_NO_ORACLE"|NO_ORACLE_ENV

// Typed declarations in tests/live of what a run that does not exit 0 may leave. History frozen at 2026-11-04.
// 0 -> 19 (2026-10-07): the refusal grade's first pass; every site is a gate that fails a run after its parts are written (quality, schema drift, a manifest that did not land).
// 19 -> 34 (2026-10-07): the full-stand sweep: 9 CDC streams that commit what they read before the refusal, 3 runs cut mid-way that keep the parts they wrote, 2 ClickHouse loads whose answer is lost after the write, 1 --reconcile verdict exit.
// 34 -> 35 (2026-10-07): the refused-run-keeps-refusing cells (one shared rig): parts, their file_log rows, the observed schema and the kept anchor.
// 35 -> 36 (2026-10-07): a parallel keyset resume that refuses over a deleted page while another worker finishes its range (seen on CI only).
// 36 -> 39 (2026-10-08): `run --validate` over a failed verification now exits 3 and a failed cursor write exits 1, both after the export completed: the two operator-contract generators and roast_metric_validated_matches_final_summary_verdict declare the delivered run.
// 39 -> 40 (2026-10-08): the SQL Server ADD COLUMN remedy cell: the log-gap refusal after the re-enable has stored the widened schema.
// 40 -> 41 (2026-10-08): the sabotage cells that take a resource away from a live run (one shared declaration, `stopped_mid_run`): the parts, file_log rows and checkpoint rows written before the stop stay for the next run.
// 41 -> 42 (2026-10-09): a load whose lease is lost after the warehouse write exits 3 and keeps its `failed` ledger row, which is what makes the table rivet's own.
// 42 -> 43 (2026-10-14): the Mongo mid-stream outage cell's refuse-then-rerun path (nightly, Mongo 4.4: parts commit before the proxy drops, the retry is refused with exit 3): the committed parts, their file_log rows and the observed schema stay for the clean re-run that completes the export.
// 43 -> 44 (2026-10-15): a refusal met by one parallel worker near the end of the key range (a_refusal_in_one_worker_exits_as_with_one_worker_*): the other workers' pages and chunks were written first, and their parts, file_log rows and observed schema stay.
// 44 -> 45 (2026-10-16): the same refusal met by one chunk of a CHECKPOINTED export (a_refusal_in_a_checkpointed_chunk_exits_by_code_and_so_does_its_resume_*): the chunks read before it were written and checkpointed, so their parts, file_log rows, chunk tasks and observed schema stay for the --resume the cell then runs.
// 45 -> 46 (2026-10-16): a Snowflake full load whose COPY fails on a corrupted part (snowflake_full_load_that_fails_keeps_the_last_table_*): the load reached the warehouse write (the staging table), so its load_run row is `failed`, which the ledger counts as ownership by design.
// 2026-10-21: +1 - the SQL Server hot ADD COLUMN stop (#644) comes at the end of a drain whose parts were
// committed before the column was added; they and the checkpoint stay, as on every mid-drain CDC stop.
// 47 -> 51 (2026-10-29, #719): four column-grants cells whose failed run leaves its parts, rows and checkpoint for the re-run by design.
// 51 -> 52 (2026-10-29, #719 follow-up): the same helper; a refused run leaves its parts, rows and checkpoint for the re-run by design.
// 52 -> 53 (2026-11-02): a BigQuery first load whose lease is lost mid-write (a_first_load_whose_lease_is_taken_during_the_write_*): the unowned table and its failed load_run row stay for the refusal the cell then asserts.
// 53 -> 52 (2026-11-04): the datetime2(7) keyset cell now asserts the refusal before any read (fix/seam-keyset-key-type); one leftover declaration left.
// 52 -> 56 (2026-11-06): the first-CDC-run-refused-after-its-anchor cells on PostgreSQL, MySQL, SQL Server and Oracle (a_first_cdc_run_refused_after_its_anchor_releases_it_*): the observed schema of the refused baseline leg stays; the slot, the checkpoint and the captured-tables record are asserted gone.
// 56 -> 58 (2026-11-12): the two Mongo resume cells that meet the heterogeneous `_id` refusal (roast_resume_must_not_bypass_heterogeneous_id_guard, removing_page_size_runs_after_the_heterogeneous_id_refusal_mongo): the refused fresh run records its shape with its anchor before the guard refuses, and the next run continues it.
// 58 -> 59 (2026-11-18): a_sampler_seek_that_scans_is_cancelled_by_the_statement_timeout_postgres: the export of the rows past the cancelled seek fails after the ranges before it were written; their parts, file_log rows, observed schema and range checkpoints stay for the re-run.
// 58 -> 59 (2026-11-19, #713): the SQL Server revoke-between-two-batches cell (a_revoke_between_two_batches_of_a_run_...): the refused run leaves the batch it committed before the revoke, its rows and its checkpoint for the re-run by design.
// 59 -> 60 (2026-11-19, #918): the pending-baseline cells of check_override_wire_cdc.rs: the run refused by the baseline leg leaves its observed schema and a chunk checkpoint, asserted through one shared helper.
// 60 -> 61 (2026-11-22, #1163): the added-column cells of live_pg_server_text.rs run on_schema_drift: fail, whose gate fails the run after its parts are written; one shared declaration in the helper, closed when the drift gate stops failing after the write.
// 61 -> 62 (2026-11-22, #1148): the Oracle chunked cell refused on its fifth chunk (a_chunked_run_refused_on_a_later_chunk_*): the four finished chunks keep their parts, file_log rows and observed schema by design, and the cell asserts them.
// 62 -> 63 (2026-11-23): the refused-run baseline cells of live_schema_drift.rs (a_refused_*_run_leaves_the_stored_baseline_as_it_found_it_*): the quality gate judges the parts after they are written, one shared declaration in the helper; closed when the gate refuses before the write.
// ratchet-pin: failed-run-leftover-declarations (review 2027-02-01) count tests/live/ \.a_failed_run_may_leave\(|FAILED_RUN_LEAVES_ENV

/// Entries of `KNOWN_PRODUCT_DEFECTS` (tests/common/refusal.rs).
// 2 -> 1 (2026-10-08): a run that fails before its first write leaves the prefix alone, so a failed manifest before a write is a failure, no longer an excuse.
const KNOWN_PRODUCT_DEFECT_CEILING: usize = 1; // ratchet-pin: failed-run-known-product-defects (review 2027-02-01)

/// Rust DuckDB-helper call sites across tests/ (see [`duckdb_helper_names`]).
// 626 -> 630 (2026-10-01): #378 merged first and added 4 calls in its Mongo null-_id tests.
// 630 -> 632 (2026-10-03): the PG/MySQL crash-mid-spilled-tail cells; on PG the rig oracle grades only the keys a stream's first run touched and passed 5 of 12 rows under the loss mutant.
// 632 -> 628 (2026-10-30): four helper-call mentions left the prose comments of the reduced files; no call site moved.
// 626 -> 624 (2026-11-01): live_batch_census rewrites parts through one Rig::rewrite_part; two helper calls left.
// 628 -> 626 (2026-10-31): the unused duckdb_s3_parquet_rows helper left tests/common/duckdb.rs; no call site moved.
// 624 -> 626 (2026-11-03): #792 composite keyset cells read distinct (id, kind) tuples; row_census keys one column. Closes when row_census takes a key list.
// 626 -> 628 (2026-11-03): #792 parallel composite keyset cells, same tuple read. Closes with the line above.
// 628 -> 623 (2026-11-04): the datetime2(7) keyset cell now asserts the refusal before any read (fix/seam-keyset-key-type); five helper calls left.
// 623 -> 626 (2026-11-06): mid-row cut cells read count + id set back through DuckDB; closed when they move to the rig oracle.
// 626 -> 621 (2026-11-11): the mssql varchar keyset cell grades through the rig oracle; five helper calls left.
// 621 -> 626 (2026-11-20): the PG json/interval/oid/enum array cell and the Oracle number/interval cells read the delivered list back through DuckDB; closed when they move to the rig oracle.
// 626 -> 625 (2026-11-22): the Oracle TIMESTAMP(9) declared-override cell reads the delivered value back through Arrow, not DuckDB; one helper call left.
const DUCKDB_HELPER_CEILING: usize = 625; // ratchet-pin: duckdb-helper-sites (review 2027-02-01)

/// Owned by a concurrent branch and migrated after it lands; not counted.
const EXCLUDED: &[&str] = &["tests/live/live_cdc_type_parity.rs"];

/// Every `.rs` file under `dir`, recursively.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read tests/").flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// (repo-relative path, text) of every counted test source.
fn sources() -> Vec<(String, String)> {
    let root = super::nonvacuity::repo_root();
    let mut files = Vec::new();
    rust_files(&root.join("tests"), &mut files);
    files
        .into_iter()
        .map(|p| {
            let rel = p.strip_prefix(&root).unwrap().display().to_string();
            let text = std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {rel}: {e}"));
            (rel, text)
        })
        .filter(|(rel, _)| !EXCLUDED.contains(&rel.as_str()) && !rel.starts_with("tests/offline/"))
        .collect()
}

/// The Rust DuckDB helpers, derived from the source.
fn duckdb_helper_names(srcs: &[(String, String)]) -> Vec<String> {
    let mut names: Vec<String> = [
        "census_oracle",
        "row_census",
        "duckdb_oracle",
        "assert_complete",
        "oracle_dir",
        "oracle_container_out",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for (rel, text) in srcs {
        for line in text.lines() {
            let l = line.trim_start();
            let decl = l
                .strip_prefix("pub fn ")
                .filter(|_| rel == "tests/common/duckdb.rs")
                .or_else(|| l.strip_prefix("pub fn duckdb_").map(|_| &l[7..]))
                .or_else(|| l.strip_prefix("fn duckdb_").map(|_| &l[3..]));
            if let Some(d) = decl {
                let name: String = d
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !name.is_empty() && !names.contains(&name) {
                    names.push(name);
                }
            }
        }
    }
    names
}

/// Occurrences of `name(` that are not the `fn name(` declaration.
fn call_sites(text: &str, name: &str) -> usize {
    let needle = format!("{name}(");
    text.match_indices(&needle)
        .filter(|(i, _)| {
            let before = &text[..*i];
            let prev = before.chars().next_back();
            !prev.is_some_and(|c| c.is_alphanumeric() || c == '_') && !before.ends_with("fn ")
        })
        .count()
}

/// Refusals the live cells accept with no `RIVET_*` code.
// 5 -> 4 (2026-10-08): `--resume` over a complete prefix is refused as RIVET_DEST_ALREADY_COMPLETE.
// 5 -> 4 (2026-10-08): the corrupt CDC checkpoint is refused by code (#497).
// 4 -> 3 (2026-10-08): the bounded-drain refusal on a PostgreSQL standby carries RIVET_SOURCE_CDC_PREREQUISITE.
// 3 -> 2 (2026-10-15): a run beside a held run lease is refused as RIVET_STATE_RUN_IN_PROGRESS (#538).
// 2 -> 1 (2026-10-15): the mixed-type MongoDB `_id` refusal of a resumed keyset run is asserted as RIVET_SOURCE_KEYSET_KEY_UNORDERED.
const UNCODED_REFUSAL_CEILING: usize = 1; // ratchet-pin: uncoded-refusals (review 2027-02-01)

#[test]
fn uncoded_refusals_the_cells_accept_never_grow() {
    let n: usize = sources()
        .iter()
        .filter(|(rel, _)| rel.starts_with("tests/live/"))
        .map(|(_, t)| t.matches("Refused::uncoded_known_defect(").count())
        .sum();
    assert_eq!(
        n, UNCODED_REFUSAL_CEILING,
        "refusals accepted with no RIVET_* code (`Refused::uncoded_known_defect(`): {n}, ceiling {UNCODED_REFUSAL_CEILING}. \
         A refusal is expected by code; a coded one lowers the ceiling here."
    );
}

/// `(file, test fn, reason)` of every live cell that accepts any non-zero exit as the refusal.
const BARE_REFUSALS: &[(&str, &str, &str)] = &[(
    "tests/live/chunking_stand.rs",
    "run_partial_unique_index_is_not_a_keyset_key",
    "2026-11-11: the product has no refusal for a partial unique keyset key (AUDIT-RED keyset-partial-unique); closes when the refusal is coded and the cell asserts it",
)];

/// The enclosing fn of each bare `if !x.status.success() { return; }` in `text`.
fn bare_nonzero_accepts(text: &str) -> Vec<String> {
    let bare = regex::Regex::new(
        r"if\s+!\s*[\w.]+\.status\.success\(\)\s*\{\s*(?://[^\n]*\s*)*return\s*;?\s*\}",
    )
    .unwrap();
    let decl = regex::Regex::new(r"fn\s+(\w+)").unwrap();
    bare.find_iter(text)
        .map(|m| {
            decl.captures_iter(&text[..m.start()])
                .last()
                .map(|c| c[1].to_string())
                .unwrap_or_default()
        })
        .collect()
}

#[test]
fn a_live_refusal_is_asserted_by_code_never_by_a_bare_nonzero_exit() {
    let live: Vec<(String, String)> = sources()
        .into_iter()
        .filter(|(rel, _)| rel.starts_with("tests/live/"))
        .collect();
    super::nonvacuity::require_enumerated(
        live.len(),
        30,
        "live test sources scanned",
        "tests/live moved",
    );
    let sample = "fn a_cell() {\n    if !run.status.success() {\n        // refusing is fine\n        return;\n    }\n}";
    assert_eq!(
        bare_nonzero_accepts(sample),
        vec!["a_cell".to_string()],
        "the bare-exit pattern no longer matches its own sample"
    );
    assert!(
        bare_nonzero_accepts(
            "fn a_cell() {\n    if !run.status.success() {\n        assert_refused(&run, r);\n        return;\n    }\n}"
        )
        .is_empty(),
        "a refusal asserted by code is not a bare exit"
    );
    let mut found: Vec<(String, String)> = live
        .iter()
        .flat_map(|(rel, t)| {
            bare_nonzero_accepts(t)
                .into_iter()
                .map(move |f| (rel.clone(), f))
        })
        .collect();
    found.sort();
    let mut ledger: Vec<(String, String)> = BARE_REFUSALS
        .iter()
        .map(|(rel, f, _)| (rel.to_string(), f.to_string()))
        .collect();
    ledger.sort();
    let unlisted: Vec<_> = found.iter().filter(|s| !ledger.contains(s)).collect();
    let stale: Vec<_> = ledger.iter().filter(|s| !found.contains(s)).collect();
    assert!(
        unlisted.is_empty() && stale.is_empty(),
        "live cells that accept any non-zero exit as the refusal: unlisted {unlisted:?}, stale ledger entries {stale:?}. \
         Assert it with `assert_refused(&out, Refused::by_code(\"RIVET_...\", exit))`; a fixed site leaves BARE_REFUSALS."
    );
}

#[test]
fn known_product_defects_of_a_failed_run_never_grow() {
    let srcs = sources();
    let (_, text) = srcs
        .iter()
        .find(|(rel, _)| rel == "tests/common/refusal.rs")
        .expect("tests/common/refusal.rs moved");
    let list = text
        .split_once("const KNOWN_PRODUCT_DEFECTS")
        .and_then(|(_, rest)| rest.split_once("];"))
        .expect("KNOWN_PRODUCT_DEFECTS moved")
        .0;
    let n = list.matches("Leftover::").count();
    assert_eq!(
        n, KNOWN_PRODUCT_DEFECT_CEILING,
        "KNOWN_PRODUCT_DEFECTS entries: {n}, ceiling {KNOWN_PRODUCT_DEFECT_CEILING}. A product defect every failed run may show is \
         added in a reviewed diff; a fixed one lowers the ceiling here."
    );
}

#[test]
fn rust_duckdb_helper_call_sites_never_grow() {
    let srcs = sources();
    let names = duckdb_helper_names(&srcs);
    super::nonvacuity::require_enumerated(
        names.len(),
        20,
        "Rust DuckDB helper names",
        "the helpers moved",
    );
    let n: usize = srcs
        .iter()
        .map(|(_, t)| names.iter().map(|name| call_sites(t, name)).sum::<usize>())
        .sum();
    assert_eq!(
        n, DUCKDB_HELPER_CEILING,
        "Rust DuckDB helper call sites: {n}, ceiling {DUCKDB_HELPER_CEILING}. New reads go through \
         dev/release_oracle (the one DuckDB session); a migrated call lowers the ceiling here."
    );
}

/// `(test fn, failure class, reason prefix)` of every `.oracle_known_defect(` site.
const KNOWN_DEFECTS: &[(&str, &str, &str)] = &[
    // ratchet-pin: oracle-known-defects (review 2027-02-01) strings
    (
        "a_pg_failover_to_the_standby_without_a_checkpoint_loses_the_rows_written_during_the_switch",
        "undelivered rows",
        "known defect: a PostgreSQL CDC failover without `cdc.checkpoint` creates a new slot",
    ),
    (
        "pg_slot_created_warning_remedy_recovers_the_row_written_while_the_slot_was_gone",
        "undelivered rows",
        "known defect: a PostgreSQL slot dropped under a stream with no checkpoint or baseline",
    ),
    (
        "pg_duplicate_run",
        "a failed run left: resume-point",
        "known defect: a checkpointed run its quality gate fails is recorded in export_progression",
    ),
]; // ratchet-pin: end

/// `(enclosing fn, class, reason)` of every known-defect call site in `text`.
fn known_defect_sites(text: &str) -> Vec<(String, String, String)> {
    text.match_indices(".oracle_known_defect(")
        .chain(text.match_indices(".run_ok_capture_known_defect("))
        .map(|(i, _)| {
            let before = &text[..i];
            let f = before
                .rfind("\nfn ")
                .map(|j| {
                    before[j + 4..]
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect::<String>()
                })
                .unwrap_or_default();
            let mut strings = text[i..].split('"').skip(1).step_by(2);
            let class = strings.next().unwrap_or_default().to_string();
            let reason = strings.next().unwrap_or_default().to_string();
            (f, class, reason)
        })
        .collect()
}

#[test]
fn oracle_known_defects_are_a_named_set_that_only_shrinks() {
    let sites: Vec<(String, String, String)> = sources()
        .iter()
        .filter(|(rel, _)| rel != "tests/common/rig/verify.rs")
        .flat_map(|(_, t)| known_defect_sites(t))
        .collect();
    super::nonvacuity::require_enumerated(
        sites.len(),
        1,
        "oracle_known_defect sites",
        "the marker moved",
    );
    let unlisted: Vec<&(String, String, String)> = sites
        .iter()
        .filter(|(f, c, r)| {
            !KNOWN_DEFECTS
                .iter()
                .any(|(kf, kc, kp)| kf == f && kc == c && r.starts_with(kp))
        })
        .collect();
    assert!(
        unlisted.is_empty(),
        "new `.oracle_known_defect` site(s) {unlisted:?}: a known product defect is added to \
         KNOWN_DEFECTS in a reviewed diff, never silently"
    );
}

/// Hand-built spawns of the rivet binary in tests/live.
const RAW_RIVET_CEILING: usize = 11; // ratchet-pin: raw-rivet-invocations (review 2027-02-01)

/// `Command::new` of the rivet binary in `text`, in each spelling the suite uses.
fn raw_rivet_sites(text: &str) -> usize {
    [
        "Command::new(RIVET_BIN)",
        "Command::new(rivet_bin())",
        "Command::new(env!(\"CARGO_BIN_EXE_rivet\"))",
    ]
    .iter()
    .map(|n| text.matches(n).count())
    .sum()
}

#[test]
fn raw_rivet_invocations_in_live_tests_never_grow() {
    let live: Vec<(String, String)> = sources()
        .into_iter()
        .filter(|(rel, _)| rel.starts_with("tests/live/"))
        .collect();
    super::nonvacuity::require_enumerated(
        live.len(),
        30,
        "live test sources scanned",
        "tests/live moved",
    );
    let n: usize = live.iter().map(|(_, t)| raw_rivet_sites(t)).sum();
    assert_eq!(
        n, RAW_RIVET_CEILING,
        "raw rivet invocations in tests/live: {n}, ceiling {RAW_RIVET_CEILING}. These runs bypass \
         the default oracle; drive rivet through `Rig` or a `run_rivet*` helper, and a \
         migrated call lowers the ceiling here."
    );
}
