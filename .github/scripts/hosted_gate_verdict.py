#!/usr/bin/env python3
"""One table for the hosted half of the release gate: every slice's verdict line and its non-green stages.

    hosted_gate_verdict.py --artifacts DIR --sha SHA --slices N   print the table, exit 1 unless every slice is green
    hosted_gate_verdict.py --self-test

Each slice artifact `DIR/<slice>/` holds `record.json` (the slice's last timings line) and `gate.log` (its output).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from collections import Counter
from pathlib import Path

GREEN = "RELEASE-READY"
SHARD_KIND = "release-oracle-full-hosted-shard"
ANSI = re.compile(r"\x1b\[[0-9;]*m")
CELL = re.compile(r"^  (\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(PASS|FAIL|SKIP|KNOWN|INFRA|N/A)\b")
NOT_GREEN = ("FAIL", "INFRA", "SKIP", "KNOWN")


def load_slice(root: Path, index: int) -> tuple[dict | None, str]:
    """The record and the log of slice `index` under `root`; the record is None when the gate never wrote one."""
    d = root / f"slice-{index}"
    rec_path, log_path = d / "record.json", d / "gate.log"
    log = ANSI.sub("", log_path.read_text(errors="replace")) if log_path.exists() else ""
    if not rec_path.exists() or not rec_path.read_text().strip():
        return None, log
    return json.loads(rec_path.read_text()), log


def scenario_counts(log: str) -> dict[str, Counter]:
    """Per scenario column, the count of each cell status in the gate log's result table."""
    out: dict[str, Counter] = {}
    for line in log.splitlines():
        m = CELL.match(line)
        if m:
            out.setdefault(m.group(3), Counter())[m.group(5)] += 1
    return out


def problems_of(rec: dict | None, sha: str) -> list[str]:
    """Why a slice record does not count as green for `sha`; empty when it does."""
    if rec is None:
        return ["no verdict line: the gate died before it reported"]
    found = []
    if rec.get("gate") != SHARD_KIND:
        found.append(f"gate kind {rec.get('gate')!r}, expected {SHARD_KIND!r}")
    if rec.get("sha") != sha:
        found.append(f"graded {rec.get('sha')}, not {sha}")
    if rec.get("dirty"):
        found.append("graded a dirty tree")
    if rec.get("verdict") != GREEN:
        found.append(f"verdict {rec.get('verdict')}")
    return found


def render(root: Path, sha: str, slices: int) -> tuple[str, bool]:
    """The markdown table for every slice 0..`slices` and whether the whole hosted half is green."""
    rows, ok, binaries = [], True, {}
    for i in range(slices + 1):
        rec, log = load_slice(root, i)
        bad = problems_of(rec, sha)
        if rec is not None and rec.get("shard") != f"{i}/{slices}":
            bad.append(f"shard {rec.get('shard')!r}, expected '{i}/{slices}'")
        ok &= not bad
        if rec is not None:
            binaries[i] = rec.get("binary_sha256")
        result = "GREEN" if not bad else "RED: " + "; ".join(bad)
        counts = Counter()
        for c in scenario_counts(log).values():
            counts.update(c)
        stats = (f"pass {rec.get('passed')} fail {rec.get('failed')} skip {rec.get('skipped')} "
                 f"{rec.get('total_min')} min") if rec else "-"
        rows.append(f"| slice {i}/{slices} | {result} | {stats} |")
        for scenario, c in sorted(scenario_counts(log).items()):
            if any(c[s] for s in NOT_GREEN):
                rows.append(f"| slice {i}: {scenario} | " + " ".join(f"{s} {c[s]}" for s in NOT_GREEN if c[s]) + " | |")
    distinct = {b for b in binaries.values() if b}
    if len(distinct) > 1:
        ok = False
        rows.append(f"| binaries | RED: slices graded {len(distinct)} different binaries | "
                    + ", ".join(f"{i}={(b or '?')[:12]}" for i, b in sorted(binaries.items())) + " |")
    head = [f"### Hosted gate half for {sha[:12]}: {'GREEN' if ok else 'RED'}", "", "| stage / slice | result | detail |", "|---|---|---|"]
    return "\n".join(head + rows) + "\n", ok


def _fixture(root: Path, index: int, **over) -> None:
    """Write a slice artifact under `root`: a green record and a log, with `over` replacing record fields."""
    d = root / f"slice-{index}"
    d.mkdir(parents=True)
    rec = {"gate": SHARD_KIND, "shard": f"{index}/3", "sha": "a" * 40, "dirty": False, "verdict": GREEN,
           "passed": 10, "failed": 0, "skipped": 0, "total_min": 1.0, "binary_sha256": "b" * 64}
    rec.update(over)
    if rec.get("drop"):
        return
    (d / "record.json").write_text(json.dumps(rec))
    (d / "gate.log").write_text("  postgres   16     live             s3       PASS ok\n  mysql      8      live             s3       FAIL boom\n")


def self_test() -> None:
    """A green set passes and every way a slice can be missing, red, stale or on another binary fails."""
    sha = "a" * 40
    cases = {
        "green": ({}, True),
        "slice never reported": ({2: {"drop": True}}, False),
        "red verdict": ({1: {"verdict": "NOT RELEASABLE"}}, False),
        "other sha": ({3: {"sha": "c" * 40}}, False),
        "dirty tree": ({0: {"dirty": True}}, False),
        "half's own kind": ({1: {"gate": "release-oracle-full-hosted"}}, False),
        "wrong shard label": ({2: {"shard": "1/3"}}, False),
        "other binary": ({1: {"binary_sha256": "d" * 64}}, False),
    }
    for name, (over, want) in cases.items():
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for i in range(4):
                _fixture(root, i, **over.get(i, {}))
            text, ok = render(root, sha, 3)
            assert ok is want, f"{name}: expected {want}, got {ok}\n{text}"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for i in range(4):
            _fixture(root, i, **({"verdict": "NOT RELEASABLE"} if i == 1 else {}))
        text, _ = render(root, sha, 3)
        assert "slice 1: live | FAIL 1" in text and "RED: verdict NOT RELEASABLE" in text, text
    print("hosted_gate_verdict self-test ok")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--artifacts", type=Path)
    ap.add_argument("--sha")
    ap.add_argument("--slices", type=int)
    ap.add_argument("--self-test", action="store_true")
    ns = ap.parse_args(argv)
    if ns.self_test:
        self_test()
        return 0
    if ns.artifacts is None or not ns.sha or not ns.slices:
        ap.error("--artifacts, --sha and --slices are required")
    text, ok = render(ns.artifacts, ns.sha, ns.slices)
    sys.stdout.write(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
