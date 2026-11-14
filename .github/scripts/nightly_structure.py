#!/usr/bin/env python3
"""Structure checks of the nightly workflows, read from the workflows and the Makefile and never from a list kept here.

    nightly_structure.py check          exit 1 when a nightly-gate part re-runs live_suite cells that live-full already grades
    nightly_structure.py --self-test

A nightly-gate part whose make recipe selects live_suite tests by `test(/^MODULE::/)` is graded a second time:
nightly-live.yml `live-full` runs every live_suite test that its `-E 'not (...)'` list does not exclude.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
GATE = ".github/workflows/nightly-gate.yml"
LIVE = ".github/workflows/nightly-live.yml"
LIVE_FULL_JOB, LIVE_FULL_STEP = "live-full", "Run FULL live suite"
MODULE = re.compile(r"test\(/\^(\w+)::/\)")
EXCLUDED = re.compile(r"test\((\w+)\)")


def recipe(makefile: str, target: str) -> str:
    """The recipe lines of `target:` up to the next line that is not a tab line."""
    out, inside = [], False
    for line in makefile.splitlines():
        if inside:
            if line.startswith("\t"):
                out.append(line)
            elif line.strip():
                break
        elif re.match(rf"{re.escape(target)}:(\s|$)", line):
            inside = True
    return "\n".join(out)


def part_targets(gate_text: str) -> dict[str, str]:
    """`{part name: make target}` from the gate workflow's own matrix."""
    return {p["name"]: p["target"] for p in yaml.safe_load(gate_text)["jobs"]["part"]["strategy"]["matrix"]["part"]}


def live_full_excludes(live_text: str) -> list[str]:
    """The `test(X)` terms the live-full step excludes."""
    steps = yaml.safe_load(live_text)["jobs"][LIVE_FULL_JOB]["steps"]
    run = next(s["run"] for s in steps if s.get("name") == LIVE_FULL_STEP)
    return EXCLUDED.findall(run.split("not (", 1)[1])


def doubled(gate_text: str, makefile: str, live_text: str) -> list[str]:
    """`part (module)` for every part whose live_suite module live-full also runs."""
    excludes = live_full_excludes(live_text)
    found = []
    for name, target in part_targets(gate_text).items():
        for line in recipe(makefile, target).splitlines():
            if "live_suite" not in line:
                continue
            found += [f"{name} ({m})" for m in MODULE.findall(line) if not any(x in m for x in excludes)]
    return list(dict.fromkeys(found))


def check() -> int:
    """Exit 1 when a part re-runs what live-full grades."""
    rows = doubled((ROOT / GATE).read_text(), (ROOT / "Makefile").read_text(), (ROOT / LIVE).read_text())
    for r in rows:
        print(f"graded twice: nightly-gate part {r} is also run by {LIVE_FULL_JOB}", file=sys.stderr)
    return 1 if rows else 0


def self_test() -> int:
    """The live tree has no doubled part; planted trees do."""
    gate_text, make_text, live_text = (ROOT / GATE).read_text(), (ROOT / "Makefile").read_text(), (ROOT / LIVE).read_text()
    assert doubled(gate_text, make_text, live_text) == [], doubled(gate_text, make_text, live_text)
    assert len(live_full_excludes(live_text)) >= 10, "the live-full exclusion list is read"
    planted_gate = gate_text.replace(
        "          - { name: open-defects,",
        "          - { name: no-debris-after-stop, target: property-no-debris-after-stop }\n          - { name: open-defects,",
        1,
    )
    assert planted_gate != gate_text, "the planted part lands in the matrix"
    assert "no-debris-after-stop (property_no_debris_after_stop)" in doubled(planted_gate, make_text, live_text), "a part that re-runs a live-full module is doubled"
    planted_live = live_text.replace("or test(open_defect_))'", "or test(open_defect_) or test(property_no_debris_after_stop))'", 1)
    assert planted_live != live_text
    assert doubled(planted_gate, make_text, planted_live) == [], "a module live-full excludes is the part's alone"
    assert recipe("a:\n\tx\n\ty\nb:\n\tz\n", "a") == "\tx\n\ty" and recipe("a:\n\tx\n", "c") == ""
    print("self-test ok: no nightly-gate part re-runs a live-full module; a planted part is doubled, an excluded module is not")
    return 0


def main(argv: list[str]) -> int:
    """Dispatch `check` or `--self-test`."""
    if argv == ["--self-test"]:
        return self_test()
    if argv == ["check"]:
        return check()
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
