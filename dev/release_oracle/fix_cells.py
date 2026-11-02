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

A fix commit that rewrote many cells mechanically (a port-env rename) credits
every body it changed; FIX_SCOPE names, per commit, the one pattern of cells that
commit is the fix for. A cell that fails on the previous release only because its
stand was down (the `live test requires ... reachable on` panic) is NOT PROVEN,
which fails the stage: bring the stand up and run it again.

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

from .core import ROOT, Ledger, run

__all__ = ["verify_fix_cells", "GREEN_ON_PREV", "FIX_SCOPE"]

SCEN = "fix_cells"
GIT = "/usr/bin/git" if Path("/usr/bin/git").exists() else "git"
FIX = re.compile(r"fix[(:!]")
STAND_DOWN = re.compile(r"live test requires \w+ reachable on ")
TEST = re.compile(r"^#\[test\]\n(?:#\[[^\n]*\n)*(?:pub )?fn (\w+)\(\) \{\n(.*?)^\}", re.M | re.S)
#: Fix cells allowed to pass on the previous release, each with its reason. Shrink-only.
GREEN_ON_PREV: dict[str, str] = {  # ratchet-pin: fix-cells-green-on-previous-release strings
    "a_mongo_re_snapshot_over_a_log_holding_the_stream_is_refused_until_truncated_bigquery":
        "2026-11-01: #737 second commit pins a refusal the BigQuery loader already made on 0.32.2; no product change in it, a regression guard for the new Mongo order",
    "a_table_swap_without_the_policy_is_met_as_before_mysql":
        "2026-11-01: #727 control: with columns_from off the table swap is met as before, so 0.32.2 delivers both rows too",
    "a_truncate_drop_or_statement_write_is_met_as_before_without_double_quoted_names_mysql":
        "2026-11-01: #727 control: statements the reader met before the sql_mode fix are met the same way, so 0.32.2 passes",
    "apply_must_not_advance_the_cursor_when_the_manifest_did_not_land":
        "2026-11-01: #731 reorders the journal and card only; the cursor guard was already right on 0.32.2 and this cell grades the cursor",
    "mysql_cdc_a_statement_logged_insert_is_captured_or_refused_never_dropped":
        "2026-11-01: #729 re-cut the COMMIT arm this cell crosses; 0.32.2 already captured or refused a statement-logged insert, so it is a regression guard",
    "mysql_lost_checkpoint_under_a_baseline_stream_is_refused_not_reanchored":
        "2026-11-01: #730 sibling: a finished initial snapshot was already refused by ensure_anchor on 0.32.2; the changes-only cell is the one that fails there",
}  # ratchet-pin: end
#: Fix commit (short sha) → pattern of the only cells it is the fix for, with its reason. Shrink-only.
FIX_SCOPE: dict[str, tuple[str, str]] = {
    "ff0a4f2c": (
        r"a_sharded_",
        "2026-11-01: #738 rewrote the port env of 47 mongo cells; only the four a_sharded_ cells need the shard stand and fail on 0.32.2",
    ),
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


def credited(tag: str) -> list[tuple[str, str, str]]:
    """(full sha, subject, cell) for every live test a `fix` commit since `tag` that touches `src/` added or edited."""
    out = []
    for line in git("log", "--no-merges", "--reverse", "--format=%H%x02%s", f"{tag}..HEAD").splitlines():
        sha, subject = line.split("\x02", 1)
        files = git("diff-tree", "--no-commit-id", "--name-only", "-r", sha).split()
        if not FIX.match(subject) or not any(f.startswith("src/") for f in files):
            continue
        for f in files:
            if f.startswith("tests/live/") and f.endswith(".rs"):
                before = subprocess.run([GIT, "show", f"{sha}^:{f}"], cwd=ROOT, capture_output=True, text=True).stdout
                out += [(sha, subject, name) for name in sorted(touched(before, git("show", f"{sha}:{f}")))]
    return out


def scoped(sha: str, name: str, scope: dict[str, tuple[str, str]]) -> bool:
    """False when `scope` names `sha` and `name` is not among the cells that commit is the fix for."""
    return all(re.search(pat, name) for known, (pat, _) in scope.items() if sha.startswith(known))


def fix_cells(tag: str, scope: dict[str, tuple[str, str]] | None = None) -> dict[str, str]:
    """Bare cell name → the `fix` commit since `tag` that added or edited it (the last one wins)."""
    scope = FIX_SCOPE if scope is None else scope
    now = {n for f in sorted((ROOT / "tests" / "live").glob("*.rs")) for n in tests_in(f.read_text())}
    cells = {name: f"{sha[:8]} {subject}" for sha, subject, name in credited(tag) if scoped(sha, name, scope)}
    return {n: why for n, why in cells.items() if n in now and not n.startswith("open_defect_")}


def stale_scope(seen: list[tuple[str, str, str]], scope: dict[str, tuple[str, str]]) -> list[str]:
    """FIX_SCOPE entries whose commit is not in `seen`, or whose pattern keeps none of its cells or drops none."""
    rows = []
    for known, (pat, _) in sorted(scope.items()):
        names = [n for sha, _, n in seen if sha.startswith(known)]
        if not any(re.search(pat, n) for n in names) or all(re.search(pat, n) for n in names):
            rows.append(known)
    return rows


def grade(cells: dict[str, str], verdicts: dict[str, str], skipped: dict[str, str], allowed: dict[str, str],
          tag: str, panics: dict[str, str] | None = None) -> list[tuple[str, str, str]]:
    """(status, cell, line) per fix cell and per ledger entry; `verdicts` is bare name → nextest status on `tag`, `panics` its first panic line."""
    panics = panics or {}
    rows = []
    for cell, why in sorted(cells.items()):
        verdict = verdicts.get(cell)
        if cell in skipped:
            rows.append(("SKIP", cell, f"NOT GRADED on {tag}: self-skipped there ({skipped[cell]})"))
        elif verdict is None:
            rows.append(("FAIL", cell, f"NOT RUN on {tag}: nextest gave no verdict ({why})"))
        elif verdict in ("PASS", "LEAK"):
            if cell in allowed:
                rows.append(("PASS", cell, f"green on {tag}, allowed: {allowed[cell]}"))
            else:
                rows.append(("FAIL", cell, f"VACUOUS FIX CELL: passes on {tag}, the release without the fix ({why}). "
                             "Make it fail there, or give its reason in fix_cells.GREEN_ON_PREV"))
        elif STAND_DOWN.search(panics.get(cell, "")):
            rows.append(("FAIL", cell, f"NOT PROVEN: red on {tag} only because its stand was down ({panics[cell]}); "
                         "bring the stand up and run it again"))
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
        cells = fix_cells(tag)
    except (ValueError, subprocess.CalledProcessError) as e:
        led.failed("all", "-", SCEN, "-", f"fix cells: the set could not be derived: {e}", getattr(e, "stderr", "") or "")
        return
    allowed = GREEN_ON_PREV
    if only:
        cells = {c: w for c, w in cells.items() if c in only}
        allowed = {c: w for c, w in allowed.items() if c in only}
    if not cells:
        n = len(git("log", "--no-merges", "--format=%h", f"{tag}..HEAD").split())
        led.passed("all", "-", SCEN, "-", f"fix cells: no fix commit among the {n} since {tag} adds or edits a live cell")
        verdicts, skipped, panics = {}, {}, {}
    else:
        verdicts, _, skipped, panics = nextest_live(
            work_dir() / "fix_cells_prev.log", "test(/::(" + "|".join(sorted(cells)) + ")$/)",
            env={"RIVET_BIN": str(prev), "RIVET_BIN_OVERRIDE": str(prev),
                 "RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": "", "RIVET_TEST_STATE_URL": ""})
    def bare(d: dict[str, str]) -> dict[str, str]:
        """nextest's `module::cell` keys as the bare cell name."""
        return {name.rsplit("::", 1)[-1]: v for name, v in d.items()}

    record = {"PASS": led.passed, "FAIL": led.failed, "SKIP": led.skipped}
    for status, cell, line in grade(cells, bare(verdicts), bare(skipped), allowed, tag, bare(panics)):
        record[status]("all", "-", SCEN, "-", f"fix cell · {cell} — {line}")
    for known in [] if only else stale_scope(credited(tag), FIX_SCOPE):
        led.failed("all", "-", SCEN, "-", f"fix scope · {known} — STALE: its commit is not among the fixes since {tag}, "
                   "or its pattern no longer separates the cells it credits; remove it from FIX_SCOPE")


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
    assert proof["red"][0] == "FAIL" and proof["red"][1].startswith("NOT PROVEN: red on v1.2.3 only because its stand was down"), proof
    real = {"red": "assertion `left == right` failed: delivered 6 of 10"}
    assert grade({"red": "a1 fix"}, {"red": "FAIL"}, {}, {}, "v1.2.3", real)[0][0] == "PASS"
    scope = {"ab12": (r"^sharded_", "r")}
    assert scoped("ab12cd34", "sharded_a", scope) and not scoped("ab12cd34", "port_env", scope) and scoped("ff00", "port_env", scope)
    seen = [("ab12cd34", "fix: x", "sharded_a"), ("ab12cd34", "fix: x", "port_env")]
    assert stale_scope(seen, scope) == [] and stale_scope(seen[:1], scope) == ["ab12"] and stale_scope(seen[1:], scope) == ["ab12"]
    assert stale_scope([("ff00", "fix: y", "port_env")], scope) == ["ab12"]
    assert all(pat and why.strip() for pat, why in FIX_SCOPE.values()), "a FIX_SCOPE entry has no pattern or reason"
    here = {n for f in (ROOT / "tests" / "live").glob("*.rs") for n in tests_in(f.read_text())}
    assert len(here) > 1000, f"the test pattern reads {len(here)} live cells: it stopped matching"
    assert all(why.strip() for why in GREEN_ON_PREV.values()), "a GREEN_ON_PREV entry has no reason"
    print("self-test ok: a fix cell that passes on the previous release fails the stage; a listed control does not")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        _self_test()
        raise SystemExit(0)
    _led = Ledger()
    verify_fix_cells(_led, sys.argv[1:] or None)
    raise SystemExit(_led.report())
