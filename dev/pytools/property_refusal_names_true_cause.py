#!/usr/bin/env python3
"""Property P4 verdict lines: the offline refusal matrix plus the sabotage refusal rows run through live_slot.

    property_refusal_names_true_cause.py offline            run the offline matrix, print its verdict lines
    property_refusal_names_true_cause.py live               offline lines plus one line per sabotage refusal cell
    property_refusal_names_true_cause.py --self-test
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
MATRIX = "refusal_names_true_cause"
VERDICT = re.compile(rf"^{MATRIX} (\S+) (PASS|RED|NOT_GRADED) (.*)$")


def verdict_lines(text: str) -> list[str]:
    """The verdict lines in `text`, one per cell, in order."""
    return [ln for ln in text.splitlines() if VERDICT.match(ln)]


def summary(lines: list[str]) -> str:
    """The counts line for a list of verdict lines."""
    n = {"PASS": 0, "RED": 0, "NOT_GRADED": 0}
    for ln in lines:
        m = VERDICT.match(ln)
        if m and m.group(1) != "SUMMARY":
            n[m.group(2)] += 1
    return f"{MATRIX} TOTAL PASS={n['PASS']} RED={n['RED']} NOT_GRADED={n['NOT_GRADED']}"


def run_offline(stand: bool) -> list[str]:
    """The matrix's verdict lines from the test binary's stdout; `stand` adds the stand axes under live_slot."""
    cmd = ["cargo", "test", "--test", "offline_suite", "property_refusal_names_true_cause::every_refusal", "--", "--nocapture"]
    env = dict(os.environ)
    if stand:
        cmd = [sys.executable, "dev/pytools/live_slot.py", "--", *cmd]
        env.update(RIVET_PM_P4_STAND="1", RIVET_ALLOW_WORKTREE_LIVE="1")
    out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, env=env)
    return verdict_lines(out.stdout)


def sabotage_cells(doc: dict) -> list[tuple[str, str, dict]]:
    """Every (row id, engine, cell) of the refusal rows, in file order."""
    cells = []
    for row in doc["scenarios"]:
        if not str(row["id"]).startswith("refusal_"):
            continue
        for engine in doc["engines"]:
            cells.append((row["id"], engine, row[engine]))
    return cells


def grade_live(passed: bool, ran: bool) -> str:
    """PASS when the sabotage test ran and passed, RED when it ran and failed, else NOT_GRADED."""
    if not ran:
        return "NOT_GRADED"
    return "PASS" if passed else "RED"


def test_paths(listing: str) -> dict[str, list[str]]:
    """Each bare test name of a `--list` output, with every module path that defines it."""
    paths: dict[str, list[str]] = {}
    for ln in listing.splitlines():
        if ln.endswith(": test"):
            path = ln.removesuffix(": test")
            paths.setdefault(path.rsplit("::", 1)[-1], []).append(path)
    return paths


def live_tests() -> dict[str, list[str]]:
    """The ignored tests of live_suite, by bare name."""
    cmd = ["cargo", "test", "--test", "live_suite", "--", "--list", "--ignored"]
    return test_paths(subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True).stdout)


def run_live_cell(test: str) -> tuple[str, str]:
    """Runs one sabotage test by its full path through live_slot; returns its verdict and evidence."""
    cmd = [sys.executable, "dev/pytools/live_slot.py", "--", "cargo", "test", "--test", "live_suite", test, "--", "--ignored", "--exact"]
    env = {**os.environ, "RIVET_ALLOW_WORKTREE_LIVE": "1"}
    out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, env=env)
    text = out.stdout + out.stderr
    m = re.search(r"test result: \w+\. (\d+) passed; (\d+) failed", text)
    ran = bool(m) and (int(m.group(1)) + int(m.group(2))) > 0
    verdict = grade_live(out.returncode == 0, ran)
    tail = next((ln for ln in reversed(text.splitlines()) if ln.startswith("test result")), "no test result line")
    return verdict, f"{test}: {tail}"


def cause_key(row: str, engine: str) -> str:
    """The offline cause key of a refusal row, so the live verdict replaces the stand NOT_GRADED line."""
    return f"cause/RIVET_{row.removeprefix('refusal_').upper()}/{engine}"


def run_live() -> list[str]:
    """One line per sabotage refusal cell: its test's verdict, or the reason it is not graded."""
    doc = yaml.safe_load((ROOT / "docs/sabotage-matrix.yaml").read_text())
    tests = live_tests()
    lines = []
    for row, engine, cell in sabotage_cells(doc):
        key = cause_key(row, engine)
        if "test" in cell:
            paths = tests.get(cell["test"], [])
            if len(paths) == 1:
                verdict, ev = run_live_cell(paths[0])
            else:
                verdict, ev = "NOT_GRADED", f"{cell['test']}: {len(paths)} tests in live_suite carry this name"
        else:
            kind = "na" if "na" in cell else "gap"
            verdict, ev = "NOT_GRADED", f"{kind}: {cell.get(kind)}"
        lines.append(f"{MATRIX} {key} {verdict} {ev}")
    return lines


def merge_live(offline: list[str], live: list[str]) -> list[str]:
    """The offline lines without the `cause/X/-` stand line of a code the live rows grade per engine, then the live rows."""
    graded = {VERDICT.match(ln).group(1).rsplit("/", 1)[0] + "/-" for ln in live}
    return [ln for ln in offline if VERDICT.match(ln).group(1) not in graded] + live


def self_test() -> int:
    """Checks the line grammar, the counts and the live grading on synthetic input."""
    ok = f"{MATRIX} cause/RIVET_X/postgres PASS exit=1 \"Error: [RIVET_X] m\""
    red = f"{MATRIX} cause/RIVET_Y/mysql RED uncoded"
    ng = f"{MATRIX} cause/RIVET_Z/- NOT_GRADED needs the live stand"
    noise = "test result: ok. 1 passed"
    lines = verdict_lines("\n".join([ok, noise, red, ng, f"{MATRIX} SUMMARY PASS=1 RED=1 NOT_GRADED=1"]))
    assert lines[:3] == [ok, red, ng], lines
    assert summary(lines) == f"{MATRIX} TOTAL PASS=1 RED=1 NOT_GRADED=1", summary(lines)
    assert grade_live(True, True) == "PASS"
    assert grade_live(False, True) == "RED"
    assert grade_live(True, False) == "NOT_GRADED", "a filter that ran nothing is not green"
    engines = ["postgres", "oracle"]
    doc = {"engines": engines, "scenarios": [{"id": "refusal_a", **{e: {"na": "x"} for e in engines}}, {"id": "other_b", **{e: {} for e in engines}}]}
    assert [c[:2] for c in sabotage_cells(doc)] == [("refusal_a", e) for e in engines]
    live = yaml.safe_load((ROOT / "docs/sabotage-matrix.yaml").read_text())
    assert sabotage_cells(live), "the sabotage ledger has refusal rows"
    listing = "a::b::t_one: test\nc::t_one: test\nd::t_two: test\n2 tests, 0 benchmarks\n"
    assert test_paths(listing) == {"t_one": ["a::b::t_one", "c::t_one"], "t_two": ["d::t_two"]}, test_paths(listing)
    assert cause_key("refusal_source_cdc_grants_changed", "mysql") == "cause/RIVET_SOURCE_CDC_GRANTS_CHANGED/mysql"
    stand = f"{MATRIX} cause/RIVET_A/- NOT_GRADED needs the live stand; sabotage row refusal_a injects it"
    other = f"{MATRIX} cause/RIVET_B/- NOT_GRADED needs the live stand; no sabotage row refusal_b"
    row = f"{MATRIX} cause/RIVET_A/postgres PASS t: test result: ok. 1 passed"
    assert merge_live([ok, stand, other], [row]) == [ok, other, row]
    print("refusal_names_true_cause self-test ok")
    return 0


def main(argv: list[str]) -> int:
    """Entry point."""
    if argv[1:] == ["--self-test"]:
        return self_test()
    if argv[1:] not in (["offline"], ["live"]):
        print(__doc__)
        return 2
    lines = run_offline(argv[1] == "live")
    if argv[1] == "live":
        lines = merge_live(lines, run_live())
    print("\n".join(lines))
    print(summary(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
