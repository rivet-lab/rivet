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

Each ledger entry is keyed by its fix commit. When the baseline moves to the next release the tag moves with it:
the set starts again from the fixes made after that release, and an entry whose commit is in the new tag's history
is inert (its fix shipped), never red. An entry whose commit is after the tag but touches nothing is STALE.

    RIVET_PREV_RELEASE_BIN=<old rivet> python3 -m dev.release_oracle.fix_cells [cell ...]
    python3 -m dev.release_oracle.fix_cells --self-test
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

from . import stand_health
from .core import ROOT, Ledger, SkipCause, run

__all__ = ["verify_fix_cells", "GREEN_ON_PREV", "FIX_SCOPE", "NO_LIVE_CELL"]

SCEN = "fix_cells"
V = TypeVar("V")
GIT = "/usr/bin/git" if Path("/usr/bin/git").exists() else "git"
FIX = re.compile(r"fix[(:!]")
TEST = re.compile(r"^#\[test\]\n(?:#\[[^\n]*\n)*(?:pub )?fn (\w+)\(\) \{\n(.*?)^\}", re.M | re.S)
#: Fix cell allowed to pass on the previous release → (its fix commit, reason). Shrink-only.
GREEN_ON_PREV: dict[str, tuple[str, str]] = {  # ratchet-pin: fix-cells-green-on-previous-release (review 2027-02-01) strings
    "a_bounded_scalar_read_is_cancelled_at_its_budget_mysql": (
        "a94c9b19",
        "2026-11-25: in-process lib cell, never runs the baseline binary (MysqlSource::connect); closes when the baseline moves past 0.33.0",
    ),
    "a_bounded_scalar_read_is_cancelled_at_its_budget_postgres": (
        "a94c9b19",
        "2026-11-25: in-process lib cell, never runs the baseline binary (PostgresSource::connect); the binary-run postgres cells of #1073 are red on 0.33.0; closes when the baseline moves past 0.33.0",
    ),
    "a_sampler_under_the_statement_timeout_keeps_its_run_mysql": (
        "a94c9b19",
        "2026-11-25: control: every id once under the budget, green before and after the fix by design; closes when the baseline moves past 0.33.0",
    ),
    "a_seek_sampler_under_the_statement_timeout_keeps_its_run_postgres": (
        "a94c9b19",
        "2026-11-25: control: every id once under the budget, green before and after the fix by design; the cancel cells of #1073 are red on 0.33.0; closes when the baseline moves past 0.33.0",
    ),
    "a_clean_temporal_column_is_delivered_and_validated_without_a_count_mysql": (
        "229cfbad",
        "2026-11-25: control: a column with no zero date writes no count row, green before and after the fix by design; the zero-date cells of #1084 are red on 0.33.0; closes when the baseline moves past 0.33.0",
    ),
    "an_oracle_sub_microsecond_key_is_refused_into_clickhouse": (
        "f6c8d093",
        "2026-11-25: the fix edits only the helper's argument; the table export already refused on 0.33.0 and the query export cell of #1081 is red there; closes when the baseline moves past 0.33.0",
    ),
    "an_integer_column_declared_float_is_delivered_with_its_values_oracle": (
        "bd36fb3d",
        "2026-11-25: expectation edit, float4 to float8 in the declaration; float8 was read correctly on 0.33.0; closes when the baseline moves past 0.33.0",
    ),
    "every_override_decoder_round_trips_against_oracles_rendering": (
        "bd36fb3d",
        "2026-11-25: expectation edit, N_F4 graded as float64; the cell passes on 0.33.0; closes when the baseline moves past 0.33.0",
    ),
    "text_and_decimal_under_a_float_or_int_override_read_as_before_mysql": (
        "bd36fb3d",
        "2026-11-25: expectation edit: the fixture drops the unparsable text and float4 becomes float8; the cell passes on 0.33.0; closes when the baseline moves past 0.33.0",
    ),
    "zero_and_one_under_a_bool_override_are_delivered_as_flags_mysql": (
        "64312e43",
        "2026-11-25: control: the batch read of 0 and 1 as flags is the behaviour the stream fix (#995) matches; the cdc cells are red on 0.33.0; closes when the baseline moves past 0.33.0",
    ),
    "parallel_keyset_midrange_error_counts_pre_failure_page_parts_postgres": (
        "b9a515e6",
        "2026-11-25: the fix adds ANALYZE to the fixture only; the sampler cells of #1062 are red on 0.33.0; closes when the baseline moves past 0.33.0",
    ),
}  # ratchet-pin: end
#: Fix commit (short sha) → pattern of the only cells it is the fix for, with its reason. Shrink-only.
FIX_SCOPE: dict[str, tuple[str, str]] = {
    "5ae3d4a5": (r"(_in_parallel|_keeps_the_run)_mssql$", "2026-11-25: #1078 fixes the text of a parallel split point read by the mssql samplers (RIVET_SOURCE_QUERY_FAILED on every parallel range, per the commit body); the cursor cells of all four engines (mssql and postgres also gained datetimeoffset(6) and timestamptz(6) keys) read no split point, and the mysql, oracle and postgres parallel cells run no mssql code; read from the diff, not measured on 0.33.0; closes when the mssql stand grades the cursor cell on 0.33.0 or the baseline moves past 0.33.0"),
}
#: Fix commit (short sha) → (a test that sees its defect, reason) for a fix that credits no live cell. Shrink-only.
NO_LIVE_CELL: dict[str, tuple[str, str]] = {
    "29e487eb": (
        "allow_anonymous_signs_nothing_even_with_an_ambient_identity",
        "2026-11-16: #1034 offline test through the in-process S3 signer (tests/s3_named_profile.rs: 4 of 16 red before the fix, per the PR body), no rig cell; closes when a live MinIO cell runs allow_anonymous beside static keys or the baseline moves past 0.33.0",
    ),
    "25b76fd9": (
        "a_store_error_in_a_failure_detail_never_carries_a_registered_credential",
        "2026-11-25: #1065 lib test in src/pipeline/validate_manifest.rs, no live cell; closes when a live store cell masks a credential in a failure detail or the baseline moves past 0.33.0",
    ),
    "696d5b20": (
        "a_columns_override_on_the_blob_columns_is_refused_or_read_never_ignored",
        "2026-11-25: #1044 lib test in src/source/mongo/mod.rs pins the override wiring, no live cell; closes when a live Mongo cell runs a blob-column override or the baseline moves past 0.33.0",
    ),
    "b61f7dc8": (
        "a_corrupt_source_is_refused_as_corrupt",
        "2026-11-25: #1087 lib test in src/state/copy.rs, no live cell; closes when a live cell copies a corrupt SQLite state or the baseline moves past 0.33.0",
    ),
}


def git(*args: str) -> str:
    """git's stdout; a failing git is an error, never an empty answer."""
    return subprocess.run([GIT, *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def is_ancestor(sha: str, tag: str) -> bool:
    """True when commit `sha` is in the history of `tag`; an unknown commit is not."""
    return subprocess.run([GIT, "merge-base", "--is-ancestor", sha, tag], cwd=ROOT, capture_output=True).returncode == 0


def unshipped(ledger: dict[str, V], sha_of: Callable[[str, V], str], tag: str, ancestor: Callable[[str, str], bool] = is_ancestor) -> dict[str, V]:
    """`ledger` without the entries whose fix commit is in `tag`: that fix shipped, so the entry has done its job."""
    return {k: v for k, v in ledger.items() if not ancestor(sha_of(k, v), tag)}


def active_ledgers(tag: str, ancestor: Callable[[str, str], bool] = is_ancestor, ledgers: tuple[dict, dict, dict] | None = None) -> tuple[dict[str, str], dict[str, tuple[str, str]], dict[str, tuple[str, str]]]:
    """GREEN_ON_PREV (cell → reason), FIX_SCOPE and NO_LIVE_CELL cut to the fixes made after `tag`."""
    green, scope, nolive = ledgers or (GREEN_ON_PREV, FIX_SCOPE, NO_LIVE_CELL)
    kept = unshipped(green, lambda _, v: v[0], tag, ancestor)
    return {cell: why for cell, (_, why) in kept.items()}, unshipped(scope, lambda k, _: k, tag, ancestor), unshipped(nolive, lambda k, _: k, tag, ancestor)


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
        green, scope, nolive = active_ledgers(tag)
        cells = fix_cells(seen, scope)
    except (ValueError, subprocess.CalledProcessError) as e:
        led.failed("all", "-", SCEN, "-", f"fix cells: the set could not be derived: {e}", getattr(e, "stderr", "") or "")
        return
    cells, allowed, unmatched = select(cells, green, only)
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
    for known in [] if only else stale_scope(seen, scope):
        led.failed("all", "-", SCEN, "-", f"fix scope · {known} — STALE: its commit is not among the fixes since {tag}, "
                   "or its pattern no longer separates the cells it credits; remove it from FIX_SCOPE")
    for status, sha, line in [] if only else grade_uncelled(uncelled(fixed, live_cells(seen, scope)), nolive, test_fns(), tag):
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
    shipped = {"aa11", "bb22"}
    ledger = {"cell_a": ("aa11", "r"), "cell_b": ("cc33", "r")}
    assert unshipped(ledger, lambda _, v: v[0], "v1", lambda sha, tag: sha in shipped) == {"cell_b": ("cc33", "r")}, "a shipped fix leaves the ledger"
    mixed = ({"c1": ("aa11aa11", "r1"), "c2": ("cc33cc33", "r2")}, {"aa11aa11": ("p", "r"), "cc33cc33": ("q", "r")}, {"aa11aa11": ("f", "r"), "cc33cc33": ("g", "r")})
    kept = active_ledgers("v1", lambda sha, tag: sha.startswith("aa11"), mixed)
    assert kept == ({"c2": "r2"}, {"cc33cc33": ("q", "r")}, {"cc33cc33": ("g", "r")}), "every ledger drops the entries of a shipped fix and no other"
    assert is_ancestor("HEAD", "HEAD") and not is_ancestor("0" * 40, "HEAD"), "an unknown commit is never shipped"
    assert all(len(sha) == 8 and why.strip() for sha, why in GREEN_ON_PREV.values()), "a GREEN_ON_PREV entry has no commit or reason"
    assert all(pat and why.strip() for pat, why in FIX_SCOPE.values()), "a FIX_SCOPE entry has no pattern or reason"
    assert all(re.compile(pat) for pat, _ in FIX_SCOPE.values()), "a FIX_SCOPE pattern does not compile"
    here = {n for f in (ROOT / "tests" / "live").glob("*.rs") for n in tests_in(f.read_text())}
    assert len(here) > 1000, f"the test pattern reads {len(here)} live cells: it stopped matching"
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
