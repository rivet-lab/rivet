#!/usr/bin/env python3
"""One table for the hosted half of the release gate: every slice's verdict line and its non-green stages.

    hosted_gate_verdict.py --artifacts DIR --sha SHA --slices N [--json FILE]   print the table, exit 1 unless every slice is green; FILE gets the verdict record the tag reads
    hosted_gate_verdict.py --self-test

Each slice artifact `DIR/<slice>/` holds `record.json` (the slice's last timings line), `gate.log` (its output) and `exit-code.txt`.
"""

from __future__ import annotations

import argparse
import json
import os
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
VERDICT_KIND = "release-oracle-hosted-verdict"
KEPT = ("at", "sha", "binary", "binary_sha256", "gate", "shard", "config", "dirty", "verdict", "passed", "failed", "skipped", "skipped_env", "stages", "host", "total_min")
RUN_ENV = {"run_id": "GITHUB_RUN_ID", "run_attempt": "GITHUB_RUN_ATTEMPT", "repository": "GITHUB_REPOSITORY", "ref": "GITHUB_REF",
           "event": "GITHUB_EVENT_NAME", "workflow_ref": "GITHUB_WORKFLOW_REF"}


def load_slice(root: Path, index: int) -> tuple[dict | None, str]:
    """The record and the log of slice `index` under `root`; the record is None when the gate never wrote one."""
    d = root / f"slice-{index}"
    rec_path, log_path = d / "record.json", d / "gate.log"
    log = ANSI.sub("", log_path.read_text(errors="replace")) if log_path.exists() else ""
    if not rec_path.exists() or not rec_path.read_text().strip():
        return None, log
    return json.loads(rec_path.read_text()), log


def load_side(root: Path, index: int, name: str) -> object:
    """The JSON or integer file `name` of slice `index`, or None when it is absent or unreadable."""
    path = root / f"slice-{index}" / name
    try:
        text = path.read_text().strip()
        return int(text) if name.endswith(".txt") else json.loads(text)
    except (OSError, ValueError):
        return None


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
    if rec.get("dirty") is not False:
        found.append("graded a tree with uncommitted changes")
    stamp = rec.get("binary")
    if not (isinstance(stamp, str) and stamp and sha.startswith(stamp)):
        found.append(f"graded a binary stamped {stamp!r}, not this commit")
    if not (isinstance(rec.get("stages"), list) and rec["stages"] and all(isinstance(n, str) for n in rec["stages"])):
        found.append("recorded no stage list")
    if rec.get("verdict") != GREEN:
        found.append(f"verdict {rec.get('verdict')}")
    return found


def render(root: Path, sha: str, slices: int) -> tuple[str, bool]:
    """The markdown table for every slice 0..`slices` and whether the whole hosted half is green."""
    rows, ok = [], True
    for i in range(slices + 1):
        rec, log = load_slice(root, i)
        bad = problems_of(rec, sha)
        if rec is not None and rec.get("shard") != f"{i}/{slices}":
            bad.append(f"shard {rec.get('shard')!r}, expected '{i}/{slices}'")
        if (code := load_side(root, i, "exit-code.txt")) != 0:
            bad.append(f"gate exit code {code}")
        ok &= not bad
        result = "GREEN" if not bad else "RED: " + "; ".join(bad)
        stats = (f"pass {rec.get('passed')} fail {rec.get('failed')} skip {rec.get('skipped')} "
                 f"{rec.get('total_min')} min") if rec else "-"
        rows.append(f"| slice {i}/{slices} | {result} | {stats} |")
        for scenario, c in sorted(scenario_counts(log).items()):
            if any(c[s] for s in NOT_GREEN):
                rows.append(f"| slice {i}: {scenario} | " + " ".join(f"{s} {c[s]}" for s in NOT_GREEN if c[s]) + " | |")
    head = [f"### Hosted gate half for {sha[:12]}: {'GREEN' if ok else 'RED'}", "", "| stage / slice | result | detail |", "|---|---|---|"]
    return "\n".join(head + rows) + "\n", ok


def verdict_record(root: Path, sha: str, slices: int, env: dict[str, str]) -> dict:
    """The record the tag reads: this run's identity from `env`, and per slice its exit code and the kept fields of its record."""
    out = []
    for i in range(slices + 1):
        rec, _ = load_slice(root, i)
        out.append({"slice": i, "exit_code": load_side(root, i, "exit-code.txt"), "record": None if rec is None else {k: rec.get(k) for k in KEPT}})
    return {"kind": VERDICT_KIND, "schema": 1, "sha": sha, "slices": slices,
            "github": {k: env.get(v, "") for k, v in RUN_ENV.items()}, "records": out}


def _fixture(root: Path, index: int, **over) -> None:
    """Write a slice artifact under `root`: a green record and a log, with `over` replacing record fields."""
    d = root / f"slice-{index}"
    d.mkdir(parents=True)
    rec = {"gate": SHARD_KIND, "shard": f"{index}/3", "sha": "a" * 40, "binary": "a" * 9, "dirty": False, "verdict": GREEN,
           "passed": 10, "failed": 0, "skipped": 0, "total_min": 1.0, "binary_sha256": f"{index:x}" * 64,
           "stages": ["live modules"], "skipped_env": [], "host": {"system": "Linux", "machine": "x86_64"}}
    code = over.pop("exit_code", 0)
    rec.update(over)
    if rec.get("drop"):
        return
    (d / "exit-code.txt").write_text(f"{code}\n")
    (d / "record.json").write_text(json.dumps(rec))
    (d / "gate.log").write_text("  postgres   16     live             s3       PASS ok\n  mysql      8      live             s3       FAIL boom\n")


def self_test() -> None:
    """A green set passes, slices that each built their own binary pass, and every way a slice can be missing, red, stale, dirty or of another commit's binary fails."""
    sha = "a" * 40
    cases = {
        "green": ({}, True),
        "slice never reported": ({2: {"drop": True}}, False),
        "red verdict": ({1: {"verdict": "NOT RELEASABLE"}}, False),
        "other sha": ({3: {"sha": "c" * 40}}, False),
        "dirty tree": ({0: {"dirty": True}}, False),
        "half's own kind": ({1: {"gate": "release-oracle-full-hosted"}}, False),
        "wrong shard label": ({2: {"shard": "1/3"}}, False),
        "each slice builds its own binary": ({1: {"binary_sha256": "d" * 64}}, True),
        "binary of another commit": ({1: {"binary": "c" * 9}}, False),
        "no binary stamp": ({1: {"binary": ""}}, False),
        "no stage list": ({2: {"stages": []}}, False),
        "dirty unknown": ({0: {"dirty": None}}, False),
        "gate exited non-zero": ({3: {"exit_code": 2}}, False),
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
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for i in range(4):
            _fixture(root, i)
        got = verdict_record(root, sha, 3, {"GITHUB_RUN_ID": "7", "GITHUB_REF": "refs/heads/main"})
        assert got["kind"] == VERDICT_KIND and got["sha"] == sha and got["slices"] == 3 and len(got["records"]) == 4, got
        assert got["github"]["run_id"] == "7" and got["github"]["ref"] == "refs/heads/main", got["github"]
        first = got["records"][0]
        assert first["exit_code"] == 0, first
        assert set(first["record"]) == set(KEPT) and first["record"]["stages"] == ["live modules"], first
        assert first["record"]["host"] == {"system": "Linux", "machine": "x86_64"}, first
    print("hosted_gate_verdict self-test ok")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--artifacts", type=Path)
    ap.add_argument("--sha")
    ap.add_argument("--slices", type=int)
    ap.add_argument("--json", type=Path, help="write the verdict record the tag reads to this file")
    ap.add_argument("--self-test", action="store_true")
    ns = ap.parse_args(argv)
    if ns.self_test:
        self_test()
        return 0
    if ns.artifacts is None or not ns.sha or not ns.slices:
        ap.error("--artifacts, --sha and --slices are required")
    text, ok = render(ns.artifacts, ns.sha, ns.slices)
    if ns.json:
        ns.json.write_text(json.dumps(verdict_record(ns.artifacts, ns.sha, ns.slices, dict(os.environ)), indent=1) + "\n")
    sys.stdout.write(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
