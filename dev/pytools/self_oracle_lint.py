"""No live test may grade rivet by rivet's own report alone.

Second rule, the same ratchet: a live test that asserts on the verdict of a preflight verb
(`check`, `doctor`, `validate`, `init`) must also run the consequence the verdict vouches
for or refuses (`Rig::run_after`, `InitConfig::run_after`, or a run of its own), because the
default oracle grades only run/load/compact/apply/cdc. BLIND_PREFLIGHT lists the tests that
break it today, each with a dated reason; the list only shrinks.

A test that reads what rivet SAYS it did — `manifest_rows()`, a metrics row's
`total_rows` / `files_committed`, the `export_harm` it recorded — and asserts on it,
with nothing independent in the same test, holds both sides of its comparison: it
cannot fail when rivet miscounts, only when rivet stops counting. Every such test
must also call an independent reader (DuckDB over the declared parts, the source's
own count) in the same function.

The rule is a ratchet: SELF_ONLY lists the tests that break it today, each with the
date it was listed; a new offender fails, and a listed one that has since gained an
independent reader must be removed (the list only shrinks).

    python3 dev/pytools/self_oracle_lint.py            # grade the tree
    python3 dev/pytools/self_oracle_lint.py --self-test
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

SELF_REPORT = re.compile(r"manifest_rows\(|\.total_rows\b|\.files_committed\b|export_harm|rows_written")
INDEPENDENT = re.compile(
    r"duckdb_|pg_count\(|mysql_count|mssql_count|mongo_count|source_count|dir_manifest_copy_id_set|"
    r"read_parquet|count_rows\(|query_i64\(|query_bigints|query_strings|\.query_one\(|\.query\(|"
    r"SELECT count|parquet_ids|parquet_rows|parquet_i64|parquet_id_set|parquet_distinct|"
    r"read_cdc_changes|cdc_id_ops|read_mongo_cdc_changes|read_ids|read_all_parts|read_declared_parts|"
    r"read_bq_|row_census|census_oracle|count_field_present|ora_text_rows|\.count_documents|"
    # the test's OWN counters: a sink it wrote, the files it lists on disk
    r"CountingSink|files_with_extension"
)

#: `file::test` → why it is allowed today. Shrink-only.
SELF_ONLY: dict[str, str] = {  # ratchet-pin: self-only-tests strings
    "audit_repair.rs::audit_repair_then_reconcile_converges": "listed 2026-09-27",
    "live_cdc.rs::cdc_initial_snapshot_of_an_empty_table_converges_despite_skip_empty": "listed 2026-09-27",
    "live_cdc.rs::mysql_cdc_refuses_a_compressed_binlog_instead_of_capturing_nothing": "listed 2026-09-27",
    "live_cdc.rs::pg_cdc_vanished_slot_with_checkpoint_fails_loudly_not_recreates": "listed 2026-09-27",
    "live_cdc.rs::pg_initial_snapshot_vanished_slot_fails_loudly_not_recreates": "listed 2026-09-27",
    "live_cdc_golden.rs::cdc_golden_fixture_tables_calculated_metrics": "listed 2026-09-27",
    "live_keyset_parallel.rs::parallel_keyset_midrange_error_counts_pre_failure_page_parts_postgres": "listed 2026-09-27",
    "live_pg_state.rs::pg_metrics_record_and_query": "listed 2026-09-27",
}  # ratchet-pin: end

PREFLIGHT = re.compile(r'\[\s*"(check|doctor|validate|init)"\s*[,\]]')
CONSEQUENCE = re.compile(
    r"run_after|run_export_after_check|\.run_ok|\.run_args|\.run_with|\.run_in_dir|\.run_expect|"
    r"\.run_nudged|\.run\(|run_rivet_ok|spawn_args|cli_cdc|\[\s*\"(run|apply|load|compact|cdc)\"|"
    r"run_and_read|drain_and_read|runs_at_once|run_rivet_export|continued_after|delivers_or_|\.damage\("
)

R_MESSAGE = "listed 2026-11-03: the cell is about the report's own words or order; no run twin exists"
R_TARGET = "listed 2026-11-03: `--target` exists only on `check`, so the run has no counterpart"
R_FLAG = "listed 2026-11-03: grades a scaffold flag spelling or its refusal by the file written, not a run"
R_VALIDATE = "listed 2026-11-03: validate follows a run the default oracle already graded; it reads the delivered tree"
R_CDC = "listed 2026-11-03: a CDC scenario; the verdict describes the stream and the run is graded by the scenario itself"
R_MATRIX = "listed 2026-11-03: the cell is the check-vs-planner agreement matrix; plan is the runner's own gate"
R_ENV = "listed 2026-11-03: the refusal is reached before any connect, so a run twin would dial the same sink"
R_PERM = "listed 2026-11-03: the note depends on the login's grants; the run twin needs a second stand login"
R_INIT = "listed 2026-11-03: graded by the scaffold content (a chosen mode, key or scope) that a run does not restate; closes with a per-field scaffold oracle"
R_DISAGREES = "listed 2026-11-03: check disagrees with the run today (reported as a product defect); closes with the product fix, then route through run_after"
R_STATE = "listed 2026-11-03: a refusal on an unreachable state store; closes when the run twin is paired with SameRefusal"

#: `file::test` → why a preflight verdict stands without its consequence today. Shrink-only.
BLIND_PREFLIGHT: dict[str, str] = {  # ratchet-pin: blind-preflight-tests strings
    "audit_column_validation.rs::audit_check_rejects_inapplicable_type_override": R_DISAGREES,
    "live_audit_suspects.rs::mssql_check_reports_a_row_estimate_for_a_read_only_login": R_DISAGREES,
    "audit_doctor_fastfail.rs::audit_doctor_fastfails_against_unreachable_cloud_endpoint": R_MESSAGE,
    "audit_doctor_probe.rs::audit_doctor_removes_destination_probe_after_local_write_check": R_MESSAGE,
    "audit_maxfile.rs::audit_validate_absent_prefix_can_fail": R_VALIDATE,
    "audit_preflight_table.rs::check_must_not_accept_a_config_the_planner_refuses": R_MATRIX,
    "audit_target_typo.rs::audit_check_unknown_cli_target_exits_nonzero": R_TARGET,
    "audit_target_typo.rs::audit_check_unknown_cli_target_names_the_typo": R_TARGET,
    "audit_target_typo.rs::audit_check_unknown_config_target_errors_loudly_contrast": R_TARGET,
    "live_cdc.rs::doctor_reports_cdc_slot_health_and_flags_foreign_inactive_slots": R_CDC,
    "live_cdc.rs::pg_cdc_multi_table_check_target_emits_one_resolver_document_per_table": R_CDC,
    "live_cdc_mbt.rs::doctor_sees_the_fault_states_the_gremlins_create": R_CDC,
    "live_cdc_mssql.rs::mssql_check_still_types_the_export_when_the_cdc_catalog_is_unreadable": R_CDC,
    "live_density_probe.rs::init_does_not_panic_on_a_bigint_unsigned_key_past_i64": R_INIT,
    "live_density_probe.rs::init_probe_overrules_a_stale_table_rows_on_a_versioned_table": R_INIT,
    "live_density_probe.rs::pg_density_probe_corrects_a_never_analyzed_table": R_INIT,
    "live_density_probe.rs::pg_density_probe_counts_a_never_analyzed_non_integer_pk_table": R_INIT,
    "live_entry_points_run_set.rs::check_and_doctor_describe_the_declared_exports_mongo": R_CDC,
    "live_entry_points_run_set.rs::check_and_doctor_describe_the_declared_exports_mssql": R_CDC,
    "live_entry_points_run_set.rs::check_and_doctor_describe_the_declared_exports_mysql": R_CDC,
    "live_entry_points_run_set.rs::check_and_doctor_describe_the_declared_exports_oracle": R_CDC,
    "live_entry_points_run_set.rs::check_and_doctor_describe_the_declared_exports_postgres": R_CDC,
    "live_entry_points_run_set.rs::check_target_note_counts_a_refused_column_once_per_loaded_table_postgres": R_CDC,
    "live_init.rs::init_mongo_refuses_a_forced_mode_mongo_cannot_run": R_INIT,
    "live_init.rs::init_mongo_schema_wide_lists_collections_in_rivets_order": R_INIT,
    "live_init.rs::init_pg_out_flag_writes_file_and_nothing_to_stdout": R_INIT,
    "live_init.rs::init_unreachable_url_exits_nonzero_with_actionable_message": R_INIT,
    "live_init_delta.rs::an_export_init_cannot_give_a_cursor_does_not_cost_the_others_their_key": R_INIT,
    "live_init_delta.rs::two_cdc_configs_of_one_cluster_name_their_own_stream_objects": R_INIT,
    "live_init_delta.rs::two_existing_configs_on_one_checkpoint_the_first_claims_it_mssql": R_INIT,
    "live_init_delta.rs::two_existing_configs_on_one_checkpoint_the_first_claims_it_mysql": R_INIT,
    "live_init_delta.rs::two_existing_configs_on_one_checkpoint_the_first_claims_it_oracle": R_INIT,
    "live_init_delta.rs::two_existing_configs_on_one_checkpoint_the_first_claims_it_postgres": R_INIT,
    "live_init_extended.rs::init_bigquery_flags_scaffold_the_load_block_and_require_each_other": R_FLAG,
    "live_init_extended.rs::init_clickhouse_flags_scaffold_the_load_block_and_require_each_other": R_FLAG,
    "live_init_extended.rs::init_cloud_destination_flags_scaffold_and_exclude_each_other": R_FLAG,
    "live_init_extended.rs::init_discover_flag_emits_json_artifact": R_FLAG,
    "live_init_extended.rs::init_source_env_reads_url_from_env_var": R_FLAG,
    "live_init_extended.rs::init_source_env_unset_exits_nonzero": R_FLAG,
    "live_init_extended.rs::init_source_file_reads_url_from_file": R_FLAG,
    "live_mssql_harm_permission.rs::doctor_notes_missing_view_server_state_for_restricted_login": R_PERM,
    "live_mssql_harm_permission.rs::doctor_silent_on_view_server_state_for_sysadmin": R_PERM,
    "live_operator_contract.rs::validate_fails_on_a_missing_bucket_azure": R_VALIDATE,
    "live_operator_contract.rs::validate_fails_on_a_missing_bucket_gcs": R_VALIDATE,
    "live_operator_contract.rs::validate_fails_on_a_missing_bucket_s3": R_VALIDATE,
    "live_operator_contract.rs::validate_reads_an_empty_prefix_of_an_existing_bucket_as_legacy_azure": R_VALIDATE,
    "live_operator_contract.rs::validate_reads_an_empty_prefix_of_an_existing_bucket_as_legacy_gcs": R_VALIDATE,
    "live_operator_contract.rs::validate_reads_an_empty_prefix_of_an_existing_bucket_as_legacy_s3": R_VALIDATE,
    "live_oracle.rs::init_discover_names_the_oracle_table_scope": R_INIT,
    "live_oracle.rs::init_oracle_schema_wide_discovers_seeded_table": R_INIT,
    "live_pg_state.rs::check_doctor_and_init_refuse_a_state_that_does_not_answer": R_STATE,
    "preflight_target_fail_note.rs::check_target_fail_with_strict_gates_exit_nonzero": R_TARGET,
    "preflight_target_fail_note.rs::check_target_fail_without_strict_notes_ungated_exit": R_TARGET,
    "sec_tls_defaults.rs::sec_remote_plaintext_pg_is_loud": R_ENV,
}  # ratchet-pin: end


def tests_in(text: str, comments: bool = False) -> list[tuple[str, str]]:
    """(name, body) of every `#[test]` function in a Rust file; `comments` lets line comments sit between the attributes and `fn`."""
    gap = r"(?:\s*(?:#\[[^\n]*|//[^\n]*)\n)*" if comments else r"(?:\s*#\[[^\n]*\n)*"
    out = []
    for m in re.finditer(r"#\[test\][^\n]*\n" + gap + r"\s*(?:pub\s+)?fn\s+(\w+)\s*\(", text):
        start = text.index("{", m.end())
        depth, i = 0, start
        while i < len(text):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        out.append((m.group(1), text[start:i + 1]))
    return out


def offenders(root: Path) -> set[str]:
    """`file::test` for every live test that reads rivet's report and nothing independent."""
    found = set()
    for f in sorted((root / "tests" / "live").glob("*.rs")):
        for name, body in tests_in(f.read_text()):
            if SELF_REPORT.search(body) and "assert" in body and not INDEPENDENT.search(body):
                found.add(f"{f.name}::{name}")
    return found


def preflight_blind(root: Path) -> set[str]:
    """`file::test` for every live test that asserts on a preflight verb and never runs its consequence."""
    found = set()
    for f in sorted((root / "tests" / "live").glob("*.rs")):
        text = f.read_text()
        helpers = fns_in(text)
        for name, body in tests_in(text, comments=True):
            full = reach(body, helpers)
            if PREFLIGHT.search(full) and "assert" in full and not CONSEQUENCE.search(full):
                found.add(f"{f.name}::{name}")
    return found


def fns_in(text: str) -> dict[str, str]:
    """name → body of every function in a Rust file."""
    out = {}
    for m in re.finditer(r"(?:^|\n)\s*(?:pub(?:\([a-z]+\))?\s+)?(?:async\s+)?fn\s+(\w+)\s*[<(]", text):
        start = text.find("{", m.end())
        if start < 0:
            continue
        depth, i = 0, start
        while i < len(text):
            depth += (text[i] == "{") - (text[i] == "}")
            if depth == 0:
                break
            i += 1
        out[m.group(1)] = text[start:i + 1]
    return out


def reach(body: str, helpers: dict[str, str]) -> str:
    """The test body plus the bodies of the same-file helpers it calls, two levels deep."""
    seen, texts, frontier = set(), [body], [body]
    for _ in range(2):
        nxt = []
        for b in frontier:
            for h in re.findall(r"\b(\w+)\s*\(", b):
                if h in helpers and h not in seen:
                    seen.add(h)
                    texts.append(helpers[h])
                    nxt.append(helpers[h])
        frontier = nxt
    return "\n".join(texts)


def self_test() -> int:
    """The scanner finds a self-only test, spares one with an independent reader."""
    bad = '#[test]\nfn a() {\n    let n = manifest_rows(&o);\n    assert_eq!(n, 5);\n}\n'
    good = '#[test]\nfn b() {\n    assert_eq!(manifest_rows(&o), duckdb_declared_dir_scalar(&o, "count(*)"));\n}\n'
    assert [n for n, _ in tests_in(bad)] == ["a"]
    assert SELF_REPORT.search(tests_in(bad)[0][1]) and not INDEPENDENT.search(tests_in(bad)[0][1])
    assert INDEPENDENT.search(tests_in(good)[0][1])
    blind = '#[test]\nfn c() {\n    let o = rig.cli(&["check"]);\n    assert!(o.status.success());\n}\n'
    paired = '#[test]\nfn d() {\n    let c = rig.run_after(&["check"], &[], Pairing::SameRefusal);\n    assert!(c.green);\n}\n'
    viahelper = 'fn go(r: &Rig) -> Output { r.cli(&["doctor"]) }\n#[test]\nfn e() {\n    let o = go(&rig);\n    assert!(o.status.success());\n}\n'
    for src, want in ((blind, True), (paired, False), (viahelper, True)):
        full = reach(tests_in(src)[0][1], fns_in(src))
        assert bool(PREFLIGHT.search(full) and not CONSEQUENCE.search(full)) is want, src
    commented = '#[test]\n#[ignore = "live"]\n// AUDIT-RED note\nfn f() {\n    let o = rig.cli(&["check"]);\n    assert!(o.status.success());\n}\n'
    assert not tests_in(commented) and [n for n, _ in tests_in(commented, comments=True)] == ["f"]
    print("self_oracle_lint self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()
    found = offenders(ROOT)
    new = sorted(found - set(SELF_ONLY))
    stale = sorted(set(SELF_ONLY) - found)
    for t in new:
        print(f"SELF-ORACLE: {t} asserts on rivet's own report with no independent reader in the test")
    for t in stale:
        print(f"STALE: {t} no longer offends — remove it from SELF_ONLY (the list only shrinks)")
    blind = preflight_blind(ROOT)
    blind_new = sorted(blind - set(BLIND_PREFLIGHT))
    blind_stale = sorted(set(BLIND_PREFLIGHT) - blind)
    for t in blind_new:
        print(f"PREFLIGHT-BLIND: {t} asserts on a preflight verdict and never runs what it vouches for (use run_after)")
    for t in blind_stale:
        print(f"STALE: {t} no longer offends — remove it from BLIND_PREFLIGHT (the list only shrinks)")
    bad = bool(new or stale or blind_new or blind_stale)
    if not bad:
        print(f"self_oracle_lint: ok ({len(found)} self-only, {len(blind)} preflight-blind, none new)")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
