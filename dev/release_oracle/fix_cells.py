"""A fix cell must fail on the previous release.

A live cell that a `fix` commit added or edited, and that passes on the release
WITHOUT the fix, does not see the defect it was written for: it would have been
green before the fix and it will be green after a regression.

The set is derived, never listed: every `fix…` commit since the baseline's tag
(the tag is the downloaded binary's own `--version`) that touches `src/`, and in
it every `#[test]` of `tests/live/` whose text is new or changed. Each is run
through the downloaded previous-release binary (`RIVET_BIN_OVERRIDE`) and must
FAIL there. One that passes is a VACUOUS FIX CELL and fails the stage, unless
GREEN_ON_PREV names it with a reason. `open_defect_*` cells are known_red's.

The cells run on the SQLite state only: the previous binary must neither migrate
nor be refused by the gate's Postgres state. A cell that needs one self-skips
there and is recorded SKIP with its reason.

A `fix` commit touching `src/` that credits no live cell (its test is a lib test, or the cell changed only
through a helper) proves nothing on the previous release: it is NOT PROVEN and fails the stage unless NO_LIVE_CELL
names the test that sees its defect, with a dated reason; that row is a SKIP, never a PASS.
A selection on the command line that matches no fix cell fails, naming what it did not match.

A fix commit that rewrote many cells mechanically (a port-env rename) credits
every body it changed; FIX_SCOPE names, per commit, the one pattern of cells that
commit is the fix for. A cell that fails on the previous release only because its
stand was down (the `live test requires ... reachable on` panic, or the stand watchdog's
verdict over its run) is NOT PROVEN: INFRA, so the gate is NOT GRADED; bring the stand up and run it again.

When the baseline moves to the next release, the tag moves with it: the set
starts again from the fixes made after that release, and every GREEN_ON_PREV or
FIX_SCOPE entry it no longer contains is reported STALE until it is removed.

    RIVET_PREV_RELEASE_BIN=<old rivet> python3 -m dev.release_oracle.fix_cells [cell ...]
    python3 -m dev.release_oracle.fix_cells --self-test
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

from . import stand_health
from .core import ROOT, Ledger, SkipCause, run

__all__ = ["verify_fix_cells", "GREEN_ON_PREV", "FIX_SCOPE", "NO_LIVE_CELL"]

SCEN = "fix_cells"
GIT = "/usr/bin/git" if Path("/usr/bin/git").exists() else "git"
FIX = re.compile(r"fix[(:!]")
TEST = re.compile(r"^#\[test\]\n(?:#\[[^\n]*\n)*(?:pub )?fn (\w+)\(\) \{\n(.*?)^\}", re.M | re.S)
#: Fix cells allowed to pass on the previous release, each with its reason. Shrink-only.
GREEN_ON_PREV: dict[str, str] = {  # ratchet-pin: fix-cells-green-on-previous-release (review 2027-02-01) strings
    "a_mongo_re_snapshot_over_a_log_holding_the_stream_is_refused_until_truncated_bigquery":
        "2026-11-01: #737 second commit pins a refusal the BigQuery loader already made on 0.32.2; no product change in it, a regression guard for the new Mongo order",
    "a_table_swap_without_the_policy_is_met_as_before_mysql":
        "2026-11-01: #727 control: with columns_from off the table swap is met as before, so 0.32.2 delivers both rows too",
    "a_truncate_drop_or_statement_write_is_met_as_before_without_double_quoted_names_mysql":
        "2026-11-01: #727 control: statements the reader met before the sql_mode fix are met the same way, so 0.32.2 passes",
    "a_load_that_dies_after_appending_is_re_run_without_duplicating_the_view":
        "2026-11-02: #743 moved this cell's crash behind a first successful load; re-appending after a crash held on 0.32.2 too, and the cell that fails there is a_first_load_that_dies_after_appending_is_refused_until_the_leftovers_are_dropped; closes when the baseline moves past 0.32.2",
    "a_sharded_mongo_stream_with_chunks_moving_and_runs_killed_loads_as_the_source_bigquery":
        "2026-11-02: #738 rewrote the shard env of the cells; the BigQuery warehouse variant converges on 0.32.2 too (3 of 3 runs green there), and the a_sharded_ mongo cells and the Snowflake and ClickHouse variants of this body are the ones that fail there; closes when the baseline moves past 0.32.2",
    "a_drop_spelled_in_another_case_stops_the_stream_on_a_folding_server_mysql":
        "2026-11-02: #757 folding-server control: 0.32.2 lower-cases the statement before it matches, so a drop spelled in another case was refused there as well; the cells that fail there are the folding-server cells of the table spelled in another case; closes when the baseline moves past 0.32.2",
    "the_same_statements_on_tables_the_export_does_not_capture_leave_the_stream_running_mysql":
        "2026-11-02: #757 no-refusal control: statements on tables the export does not capture were met as before on 0.32.2, so it passes there; it guards the name compare from refusing what it must not; closes when the baseline moves past 0.32.2",
    "apply_forms_partition_like_run_mssql":
        "2026-11-02: #755 only moved the fixture into a helper; the apply forms partition like run on 0.32.2 too, and the cell that fails there is validate_reads_every_partition_mssql; closes when the baseline moves past 0.32.2",
    "apply_forms_partition_like_run_mysql":
        "2026-11-02: #755 only moved the fixture into a helper; the apply forms partition like run on 0.32.2 too, and the cell that fails there is validate_reads_every_partition_mysql; closes when the baseline moves past 0.32.2",
    "apply_forms_partition_like_run_oracle":
        "2026-11-02: #755 only moved the fixture into a helper; the apply forms partition like run on 0.32.2 too, and the cell that fails there is validate_reads_every_partition_oracle; closes when the baseline moves past 0.32.2",
    "apply_must_not_advance_the_cursor_when_the_manifest_did_not_land":
        "2026-11-01: #731 reorders the journal and card only; the cursor guard was already right on 0.32.2 and this cell grades the cursor",
    "mysql_cdc_a_statement_logged_insert_is_captured_or_refused_never_dropped":
        "2026-11-01: #729 re-cut the COMMIT arm this cell crosses; 0.32.2 already captured or refused a statement-logged insert, so it is a regression guard",
    "mysql_lost_checkpoint_under_a_baseline_stream_is_refused_not_reanchored":
        "2026-11-01: #730 sibling: a finished initial snapshot was already refused by ensure_anchor on 0.32.2; the changes-only cell is the one that fails there",
    "validate_prefix_of_a_partitioned_export_verifies_that_directory_postgres":
        "2026-11-02: #755 control: a pinned --prefix verified that one directory on 0.32.2 as well; it guards the run-set validate from widening it; closes when the baseline moves past 0.32.2",
    "check_still_accepts_overrides_the_run_accepts_postgres":
        "2026-11-04: #805 control: an override the run accepts passes check on 0.32.2 as well; it guards the new wire-mismatch refusal from widening onto it; the check_refuses_ cells are the ones that fail there; closes when the baseline moves past 0.32.2",
}  # ratchet-pin: end
#: Fix commit (short sha) → pattern of the only cells it is the fix for, with its reason. Shrink-only.
FIX_SCOPE: dict[str, tuple[str, str]] = {
    "ff0a4f2c": (
        r"a_sharded_",
        "2026-11-01: #738 rewrote the port env of 47 mongo cells; only the four a_sharded_ cells need the shard stand and fail on 0.32.2",
    ),
    "fcc9f1d8": (
        r"^(?!partition_reset_(full|plain|unknown_child)_)",
        "2026-11-07: #881 new file; the full, plain and unknown_child cells are controls 0.32.2 passes (full keeps no cursor, a plain export has no children, a ghost child was refused there too); the parent, child, family, respelled, cross_source and child_lease cells fail there",
    ),
    "32df35d8": (
        r"^(?!a_missing_column_named_plainly_|a_missing_column_exits_by_its_code_whatever_its_name_holds_postgres$)",
        "2026-11-07: #827 plain-name cells are controls and PostgreSQL already exits 1 for a missing column whatever its name holds on 0.32.2; the mysql, mssql and oracle whatever_its_name_holds cells fail there",
    ),
    "34ab117d": (
        r"^(?!a_binary_double_a_float_|a_domain_over_text_keyset_key_|an_alias_type_over_datetime2_6_|init_scaffolds_no_keyset_key_for_a_(binary16|bytea)_|keyset_key_types_the_cursor_reads_deliver_the_seeded_ids_oracle$|parallel_keyset_incremental_on_an_mssql_datetime_key_)",
        "2026-11-07: #814 admitted-key and stays-refused cells are controls 0.32.2 passes (domain over text, datetime2(6) alias and datetime, oracle integer and text keys, init without a keyset key for binary16 and bytea, the refusals Oracle made before); the refused-key, bare-NUMBER and datetime2(7) cells fail there",
    ),
    "2c52067f": (
        r"^(?!an_incremental_cursor_on_a_nanosecond_timestamp_is_refused_before_it_re_exports$|an_init_generated_config_runs_on_a_key_the_oracle_planner_cannot_seek$)",
        "2026-11-07: #854 these two now declare the TIMESTAMP(9) column and 0.32.2 delivers or refuses them the same; the oracle_batch_timestamp9 and oracle_cdc_timestamp9 undeclared-refusal cells fail there",
    ),
    "a0cc571f": (
        r"^(?!in_range_values_under_the_override_keep_every_digit_and_null_stays_null$)",
        "2026-11-07: #875 in-range values and NULL were delivered right on 0.32.2; the past-range cells fail there",
    ),
    "c20c6403": (
        r"^(?!cdc_to_files_still_refuses_a_partial_after_image_mysql$)",
        "2026-11-08: #882 the partial after image was refused by the file sink on 0.32.2 as well (the control of the new before-image check); the before-image and stdout cells fail there",
    ),
}
#: Fix commit (short sha) → (a test that sees its defect, reason) for a fix that credits no live cell. Shrink-only.
NO_LIVE_CELL: dict[str, tuple[str, str]] = {
    "0130d0db": ("parquet_bytes_match_the_v0_32_2_digests", "2026-11-10: superseded by d7867ec8 (#754), which dropped the write-call splitting; the byte golden grades the shipped writer; closes when the baseline moves past 0.32.2"),
    "6993e9fb": ("a_job_failed_with_a_transient_reason_is_unavailable_and_retryable", "2026-11-10: lib test, seed bigquery-transient-job-error-final; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "c62b2f09": ("an_existing_change_log_that_this_load_would_corrupt_is_refused", "2026-11-10: lib test, seed clickhouse-paren-sorting-key; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "d7867ec8": ("parquet_bytes_match_the_v0_32_2_digests", "2026-11-10: the byte golden holds the 0.32.2 digests itself, seed parquet-writer-bytes-move-from-release; closes when the baseline moves past 0.32.2"),
    "3f610778": ("stand_pool_split_resume_grows_postgres", "2026-11-10: the live cell changed through the run_pool_split_rerun_with helper, so its body is unedited; RED on main at #756, not run on 0.32.2; closes when the baseline moves past 0.32.2"),
    "2377caa1": ("sqlite_state_decides_liveness_by_literal_path_containment", "2026-11-10: lib test, seed state-prefix-match-reads-text-as-pattern; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "123acb80": ("a_named_credentials_file_that_is_no_key_stops_the_load", "2026-11-10: lib test, seed named-credential-falls-back; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "9c962422": ("ambient_access_keys_never_replace_a_named_profile", "2026-11-10: offline test, seed named-profile-falls-back; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "ed13affb": ("a_number_held_as_text_is_labelled_plain_decimal_and_every_other_column_is_not", "2026-11-10: lib test, seed number-text-without-its-label; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "b1d452a2": ("a_baseline_loaded_alone_is_cleaned_under_cleanup_source", "2026-11-10: lib test only, no seed; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "3154de1c": ("the_probe_reads_its_globals_without_a_full_variables_scan", "2026-11-10: lib test only, no seed; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "e523454a": ("a_query_pair_whose_key_names_a_password_is_masked_in_any_case", "2026-11-10: lib test, seed query-password-key-by-suffix-family; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "1aeea54f": ("postgres_system_store_is_not_a_second_anchor", "2026-11-10: offline test, seed ca-file-second-trust-anchor; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "208823de": ("mysql_system_store_is_not_a_second_anchor", "2026-11-10: offline test, seed ca-file-second-trust-anchor; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "3bd8620c": ("a_transient_reply_from_the_token_endpoint_is_retried_and_the_next_answer_mints", "2026-11-10: lib test, seed token-endpoint-transient-final; not run on the previous binary; closes when the baseline moves past 0.32.2"),
    "8679e73f": ("a_chunk_claim_beside_a_writer_that_never_lets_go_gets_its_turn", "2026-11-10: lib test, seed sqlite-chunk-claim-takes-no-turn; not run on the previous binary; closes when the baseline moves past 0.32.2"),
}


def git(*args: str) -> str:
    """git's stdout; a failing git is an error, never an empty answer."""
    return subprocess.run([GIT, *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def prev_tag(prev: Path) -> str:
    """The release tag of the baseline binary, from its own `--version`."""
    said = run([str(prev), "--version"]).stdout
    m = re.search(r"\b(\d+\.\d+\.\d+)\b", said)
    if not m:
        raise ValueError(f"{prev} --version printed no x.y.z: {said!r}")
    return f"v{m.group(1)}"


def tests_in(text: str) -> dict[str, str]:
    """Bare name → body of every top-level `#[test] fn name()` in a Rust file."""
    return dict(TEST.findall(text))


def touched(before: str, after: str) -> set[str]:
    """The tests of `after` that `before` does not have, or has with another body."""
    was = tests_in(before)
    return {name for name, body in tests_in(after).items() if was.get(name) != body}


def fixes(tag: str) -> list[tuple[str, str, list[str]]]:
    """(full sha, subject, files) of every `fix` commit since `tag` that touches `src/`."""
    out = []
    for line in git("log", "--no-merges", "--reverse", "--format=%H%x02%s", f"{tag}..HEAD").splitlines():
        sha, subject = line.split("\x02", 1)
        files = git("diff-tree", "--no-commit-id", "--name-only", "-r", sha).split()
        if FIX.match(subject) and any(f.startswith("src/") for f in files):
            out.append((sha, subject, files))
    return out


def credited(fixed: list[tuple[str, str, list[str]]]) -> list[tuple[str, str, str]]:
    """(full sha, subject, cell) for every live test the `fixed` commits added or edited."""
    out = []
    for sha, subject, files in fixed:
        for f in files:
            if f.startswith("tests/live/") and f.endswith(".rs"):
                before = subprocess.run([GIT, "show", f"{sha}^:{f}"], cwd=ROOT, capture_output=True, text=True).stdout
                out += [(sha, subject, name) for name in sorted(touched(before, git("show", f"{sha}:{f}")))]
    return out


def scoped(sha: str, name: str, scope: dict[str, tuple[str, str]]) -> bool:
    """False when `scope` names `sha` and `name` is not among the cells that commit is the fix for."""
    return all(re.search(pat, name) for known, (pat, _) in scope.items() if sha.startswith(known))


def live_cells(seen: list[tuple[str, str, str]], scope: dict[str, tuple[str, str]] | None = None) -> list[tuple[str, str, str]]:
    """The `seen` credits that are in scope, still in tests/live, and not an open defect's."""
    scope = FIX_SCOPE if scope is None else scope
    now = {n for f in sorted((ROOT / "tests" / "live").glob("*.rs")) for n in tests_in(f.read_text())}
    return [(sha, subject, name) for sha, subject, name in seen
            if scoped(sha, name, scope) and name in now and not name.startswith("open_defect_")]


def fix_cells(seen: list[tuple[str, str, str]], scope: dict[str, tuple[str, str]] | None = None) -> dict[str, str]:
    """Bare cell name → the `fix` commit that added or edited it (the last one wins)."""
    return {name: f"{sha[:8]} {subject}" for sha, subject, name in live_cells(seen, scope)}


def uncelled(fixed: list[tuple[str, str, list[str]]], kept: list[tuple[str, str, str]]) -> list[tuple[str, str]]:
    """(sha, subject) of every `fixed` commit that has no cell among `kept`."""
    has = {sha for sha, _, _ in kept}
    return [(sha, subject) for sha, subject, _ in fixed if sha not in has]


def test_fns() -> set[str]:
    """Every `fn name(` under src/ and tests/."""
    return {n for d in ("src", "tests") for f in (ROOT / d).rglob("*.rs") for n in re.findall(r"\bfn (\w+)\(", f.read_text())}


def grade_uncelled(missing: list[tuple[str, str]], ledger: dict[str, tuple[str, str]], fns: set[str], tag: str) -> list[tuple[str, str, str]]:
    """(status, id, line) per fix commit with no live cell and per ledger entry that no such commit uses."""
    rows = []
    for sha, subject in missing:
        known = next((k for k in ledger if sha.startswith(k)), None)
        if known is None:
            rows.append(("FAIL", sha[:8], f"NOT PROVEN: {subject} touches src/ and credits no live cell, so nothing of it runs on {tag}. "
                         "Edit the live #[test] it is the fix for, or name the test that sees its defect in fix_cells.NO_LIVE_CELL"))
        elif ledger[known][0] not in fns:
            rows.append(("FAIL", sha[:8], f"NOT PROVEN: NO_LIVE_CELL names {ledger[known][0]}, which no file under src/ or tests/ defines"))
        else:
            rows.append(("SKIP", sha[:8], f"NOT RUN on {tag}: {subject} credits no live cell; its test is {ledger[known][0]} ({ledger[known][1]})"))
    used = {k for k in ledger for sha, _ in missing if sha.startswith(k)}
    rows += [("FAIL", k, f"STALE: no fix commit since {tag} lacks a live cell under it; remove it from NO_LIVE_CELL") for k in sorted(set(ledger) - used)]
    return rows


def select(cells: dict[str, str], allowed: dict[str, str], only: list[str] | None) -> tuple[dict[str, str], dict[str, str], list[str]]:
    """`cells` and `allowed` cut to the names in `only`, and the names in `only` that are in neither."""
    if not only:
        return cells, allowed, []
    return ({c: w for c, w in cells.items() if c in only}, {c: w for c, w in allowed.items() if c in only},
            sorted(n for n in set(only) if n not in cells and n not in allowed))


def stale_scope(seen: list[tuple[str, str, str]], scope: dict[str, tuple[str, str]]) -> list[str]:
    """FIX_SCOPE entries whose commit is not in `seen`, or whose pattern keeps none of its cells or drops none."""
    rows = []
    for known, (pat, _) in sorted(scope.items()):
        names = [n for sha, _, n in seen if sha.startswith(known)]
        if not any(re.search(pat, n) for n in names) or all(re.search(pat, n) for n in names):
            rows.append(known)
    return rows


def grade(cells: dict[str, str], verdicts: dict[str, str], skipped: dict[str, str], allowed: dict[str, str],
          tag: str, panics: dict[str, str] | None = None, stand: stand_health.StandVerdict = stand_health.ALIVE) -> list[tuple[str, str, str]]:
    """(status, cell, line) per fix cell and per ledger entry; `verdicts` is bare name → nextest status on `tag`, `panics` its first panic line, `stand` the watchdog's verdict over the run."""
    panics = panics or {}
    rows = []
    for cell, why in sorted(cells.items()):
        verdict = verdicts.get(cell)
        if cell in skipped:
            rows.append(("SKIP", cell, f"NOT GRADED on {tag}: self-skipped there ({skipped[cell]})"))
        elif verdict not in ("PASS", "LEAK") and not (seen := stand_health.classify([stand_health.from_text(panics.get(cell, "")), stand])).alive:
            rows.append(("INFRA", cell, f"NOT PROVEN: red on {tag} only because its stand was {seen.kind.value} ({seen.reason}); "
                         "bring the stand up and run it again"))
        elif verdict is None:
            rows.append(("FAIL", cell, f"NOT RUN on {tag}: nextest gave no verdict ({why})"))
        elif verdict in ("PASS", "LEAK"):
            if cell in allowed:
                rows.append(("PASS", cell, f"green on {tag}, allowed: {allowed[cell]}"))
            else:
                rows.append(("FAIL", cell, f"VACUOUS FIX CELL: passes on {tag}, the release without the fix ({why}). "
                             "Make it fail there, or give its reason in fix_cells.GREEN_ON_PREV"))
        elif cell in allowed:
            rows.append(("FAIL", cell, f"STALE: red on {tag} and listed in GREEN_ON_PREV; remove the entry"))
        else:
            rows.append(("PASS", cell, f"red on {tag}, as its fix requires ({why})"))
    for cell in sorted(set(allowed) - set(cells)):
        rows.append(("FAIL", cell, f"STALE: no fix commit since {tag} touches it; remove it from GREEN_ON_PREV"))
    return rows


def verify_fix_cells(led: Ledger, only: list[str] | None = None) -> None:
    """Run every fix cell since the baseline's tag through the baseline binary; one ledger row per cell."""
    from .regression import _require_prev_binary
    from .scenarios import nextest_live, work_dir
    led.phase("fix cells · each must fail on the previous release")
    prev = _require_prev_binary(led, "all", "-", SCEN, "-", "fix cells")
    if prev is None:
        return
    try:
        tag = prev_tag(prev)
        fixed = fixes(tag)
        seen = credited(fixed)
        cells = fix_cells(seen)
    except (ValueError, subprocess.CalledProcessError) as e:
        led.failed("all", "-", SCEN, "-", f"fix cells: the set could not be derived: {e}", getattr(e, "stderr", "") or "")
        return
    cells, allowed, unmatched = select(cells, GREEN_ON_PREV, only)
    if unmatched:
        led.failed("all", "-", SCEN, "-", f"fix cells: the selection names no fix cell since {tag}: {', '.join(unmatched)}")
    if not cells:
        n = len(git("log", "--no-merges", "--format=%h", f"{tag}..HEAD").split())
        if not unmatched:
            led.passed("all", "-", SCEN, "-", f"fix cells: no fix commit among the {n} since {tag} adds or edits a live cell")
        verdicts, skipped, panics = {}, {}, {}
    else:
        since = stand_health.BOOK.mark()
        verdicts, _, skipped, panics = nextest_live(
            work_dir() / "fix_cells_prev.log", "test(/::(" + "|".join(sorted(cells)) + ")$/)",
            env={"RIVET_BIN": str(prev), "RIVET_BIN_OVERRIDE": str(prev),
                 "RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": "", "RIVET_TEST_STATE_URL": ""})
    def bare(d: dict[str, str]) -> dict[str, str]:
        """nextest's `module::cell` keys as the bare cell name."""
        return {name.rsplit("::", 1)[-1]: v for name, v in d.items()}

    record = {"PASS": led.passed, "FAIL": led.failed, "INFRA": led.ungraded}
    stand = stand_health.judge("", since) if cells else stand_health.ALIVE
    for status, cell, line in grade(cells, bare(verdicts), bare(skipped), allowed, tag, bare(panics), stand):
        if status == "SKIP":
            led.skipped("all", "-", SCEN, cell, f"fix cell · {cell} — {line}", cause=SkipCause.ENV)
        else:
            record[status]("all", "-", SCEN, "-", f"fix cell · {cell} — {line}")
    for known in [] if only else stale_scope(seen, FIX_SCOPE):
        led.failed("all", "-", SCEN, "-", f"fix scope · {known} — STALE: its commit is not among the fixes since {tag}, "
                   "or its pattern no longer separates the cells it credits; remove it from FIX_SCOPE")
    for status, sha, line in [] if only else grade_uncelled(uncelled(fixed, live_cells(seen)), NO_LIVE_CELL, test_fns(), tag):
        if status == "SKIP":
            led.skipped("all", "-", SCEN, sha, f"fix commit · {sha} — {line}", cause=SkipCause.DESIGN)
        else:
            led.failed("all", "-", SCEN, "-", f"fix commit · {sha} — {line}")


def _self_test() -> None:
    """The derivation and the grade on fixtures: a vacuous fix cell is the one row that fails."""
    before = '#[test]\n#[ignore = "live"]\nfn kept() {\n    a();\n}\n\n#[test]\nfn edited() {\n    assert!(x);\n}\n\nfn helper() {\n}\n'
    after = before.replace("assert!(x);", "assert_eq!(x, 1);") + '\n#[test]\n#[ignore = "live"]\npub fn added() {\n    if a {\n        b();\n    }\n}\n'
    assert set(tests_in(after)) == {"kept", "edited", "added"}, tests_in(after)
    assert touched(before, after) == {"edited", "added"} and touched("", before) == {"kept", "edited"}
    assert FIX.match("fix(cdc): x") and FIX.match("fix: x") and not FIX.match("fixture: x") and not FIX.match("test(fix): x")
    cells = {"red": "a1 fix(x): one", "green": "a1 fix(x): one", "control": "b2 fix(y): two", "skipped": "b2 fix(y): two", "gone": "b2 fix(y): two"}
    on_prev = {"red": "FAIL", "green": "PASS", "control": "LEAK", "skipped": "PASS"}
    rows = {c: (s, line) for s, c, line in grade(cells, on_prev, {"skipped": "no RIVET_TEST_STATE_URL"}, {"control": "a control"}, "v1.2.3")}
    assert rows["red"][0] == "PASS" and "red on v1.2.3" in rows["red"][1], rows["red"]
    assert rows["green"][0] == "FAIL" and rows["green"][1].startswith("VACUOUS FIX CELL: passes on v1.2.3"), rows["green"]
    assert rows["control"] == ("PASS", "green on v1.2.3, allowed: a control"), rows["control"]
    assert rows["skipped"] == ("SKIP", "NOT GRADED on v1.2.3: self-skipped there (no RIVET_TEST_STATE_URL)"), rows["skipped"]
    assert rows["gone"][0] == "FAIL" and "no verdict" in rows["gone"][1], rows["gone"]
    stale = grade({"red": "a1 fix"}, {"red": "FAIL"}, {}, {"red": "r", "old": "r"}, "v1.2.3")
    assert [(s, c, line.split(":")[0]) for s, c, line in stale] == [("FAIL", "red", "STALE"), ("FAIL", "old", "STALE")], stale
    down = {"red": "live test requires MongoShard reachable on 127.0.0.1:27991."}
    proof = {c: (s, line) for s, c, line in grade({"red": "a1 fix"}, {"red": "FAIL"}, {}, {}, "v1.2.3", down)}
    assert proof["red"][0] == "INFRA" and proof["red"][1].startswith("NOT PROVEN: red on v1.2.3 only because its stand was DOWN"), proof
    watched = stand_health.down("the state DB stopped answering")
    gone = {c: (s, line) for s, c, line in grade({"red": "a1 fix", "ok": "a1 fix"}, {"red": "FAIL", "ok": "PASS"}, {}, {}, "v1.2.3", None, watched)}
    assert gone["red"][0] == "INFRA" and "the state DB stopped answering" in gone["red"][1], "a red that ran while the watchdog said DOWN is the stand's"
    assert gone["ok"][0] == "FAIL", "a pass is a pass, but a fix cell that passes on the previous release is still vacuous"
    unread = grade({"red": "a1 fix"}, {"red": "FAIL"}, {}, {}, "v1.2.3", None, stand_health.unreadable("docker events exited 1"))
    assert unread[0][0] == "INFRA" and "UNREADABLE" in unread[0][2], unread
    assert grade({"red": "a1 fix"}, {}, {}, {}, "v1.2.3", None, watched)[0][0] == "INFRA", "no verdict while the stand is down is the stand's"
    real = {"red": "assertion `left == right` failed: delivered 6 of 10"}
    assert grade({"red": "a1 fix"}, {"red": "FAIL"}, {}, {}, "v1.2.3", real)[0][0] == "PASS"
    scope = {"ab12": (r"^sharded_", "r")}
    assert scoped("ab12cd34", "sharded_a", scope) and not scoped("ab12cd34", "port_env", scope) and scoped("ff00", "port_env", scope)
    seen = [("ab12cd34", "fix: x", "sharded_a"), ("ab12cd34", "fix: x", "port_env")]
    assert stale_scope(seen, scope) == [] and stale_scope(seen[:1], scope) == ["ab12"] and stale_scope(seen[1:], scope) == ["ab12"]
    assert stale_scope([("ff00", "fix: y", "port_env")], scope) == ["ab12"]
    assert all(pat and why.strip() for pat, why in FIX_SCOPE.values()), "a FIX_SCOPE entry has no pattern or reason"
    assert all(re.compile(pat) for pat, _ in FIX_SCOPE.values()), "a FIX_SCOPE pattern does not compile"
    here = {n for f in (ROOT / "tests" / "live").glob("*.rs") for n in tests_in(f.read_text())}
    assert len(here) > 1000, f"the test pattern reads {len(here)} live cells: it stopped matching"
    assert all(why.strip() for why in GREEN_ON_PREV.values()), "a GREEN_ON_PREV entry has no reason"
    fixed = [("aa11aa11", "fix: a", ["src/a.rs"]), ("bb22bb22", "fix: b", ["src/b.rs"]), ("cc33cc33", "fix: c", ["src/c.rs"])]
    assert uncelled(fixed, [("aa11aa11", "fix: a", "cell_a")]) == [("bb22bb22", "fix: b"), ("cc33cc33", "fix: c")], "a fix with a cell is celled"
    assert uncelled(fixed, []) == [(sha, subject) for sha, subject, _ in fixed], "no cell at all leaves every fix uncelled"
    ledger = {"bb22": ("lib_b", "r"), "cc33": ("gone_fn", "r"), "ee55": ("lib_b", "r")}
    missing = [("bb22bb22", "fix: b"), ("cc33cc33", "fix: c"), ("dd44dd44", "fix: d")]
    credit = {i: (s, line) for s, i, line in grade_uncelled(missing, ledger, {"lib_b"}, "v1.2.3")}
    assert credit["bb22bb22"][0] == "SKIP" and credit["bb22bb22"][1].startswith("NOT RUN on v1.2.3"), credit["bb22bb22"]
    assert credit["cc33cc33"][0] == "FAIL" and "gone_fn" in credit["cc33cc33"][1], "a ledger entry naming no test is not a proof"
    assert credit["dd44dd44"][0] == "FAIL" and credit["dd44dd44"][1].startswith("NOT PROVEN"), "a fix that credits nothing and is not listed fails"
    assert credit["ee55"][0] == "FAIL" and credit["ee55"][1].startswith("STALE"), credit["ee55"]
    assert grade_uncelled([], {}, set(), "v1.2.3") == [], "no uncelled fix and no entry grades nothing"
    kept, free, unmatched = select({"x": "w", "y": "w"}, {"z": "r"}, ["x", "typo", "z"])
    assert (kept, free, unmatched) == ({"x": "w"}, {"z": "r"}, ["typo"]), "a name that is no fix cell is reported"
    assert select({"x": "w"}, {"z": "r"}, None) == ({"x": "w"}, {"z": "r"}, []) and select({"x": "w"}, {}, ["a", "b"])[2] == ["a", "b"]
    fns = test_fns()
    assert len(fns) > 1000, f"the fn pattern reads {len(fns)} functions: it stopped matching"
    assert all(len(k) == 8 and fn in fns for k, (fn, _) in NO_LIVE_CELL.items()), "a NO_LIVE_CELL entry names no test or a short sha"
    assert all(re.match(r"\d{4}-\d{2}-\d{2}: \S", why) for _, why in NO_LIVE_CELL.values()), "a NO_LIVE_CELL entry has no dated reason"
    print("self-test ok: a fix cell that passes on the previous release fails the stage; a listed control does not")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        _self_test()
        raise SystemExit(0)
    _led = Ledger()
    verify_fix_cells(_led, sys.argv[1:] or None)
    raise SystemExit(_led.report())
