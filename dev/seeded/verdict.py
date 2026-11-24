"""The hosted recall's one verdict per sha: the shards' records read back, every expected seed run exactly once and green, one run-record line written."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from collections.abc import Collection, Sequence
from pathlib import Path

from dev.seeded import run_record
from dev.seeded.recall import gate_ceiling
from dev.seeded.shards import coverage_problems

WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "nightly-recall.yml"
SHARD_FILES = ("exit-code.txt", "record.jsonl", "table.txt")


def shard_line(shard: Path, sha: str) -> tuple[dict | None, str]:
    """The last record line of `sha` a shard wrote, or why it has none."""
    code = shard / SHARD_FILES[0]
    if not shard.is_dir():
        return None, "no artifact (the shard never ran or never uploaded)"
    if not code.is_file() or code.read_text().strip() != "0":
        return None, f"exit code {code.read_text().strip() if code.is_file() else 'unknown'}"
    record = shard / SHARD_FILES[1]
    last = None
    for line in record.read_text().splitlines() if record.is_file() else []:
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict) and rec.get("sha") == sha:
            last = rec
    if last is None:
        return None, f"no record line of {sha[:12]}"
    if last.get("green") is not True or last.get("dirty") is not False:
        return None, "its recall is red or ran on a dirty tree"
    if not isinstance(last.get("seeds"), list):
        return None, "its record names no seeds"
    return last, ""


def judge(root: Path, sha: str, shards: int, expected: Sequence[str]) -> tuple[list[str], list[str]]:
    """(problems, the seeds the shards ran): every shard green and clean on `sha`, every expected seed in exactly one."""
    problems: list[str] = []
    ran: list[dict] = []
    for i in range(shards):
        rec, why = shard_line(root / f"recall-shard-{i}", sha)
        if rec is None:
            problems.append(f"shard {i}: {why}")
        else:
            ran.append({"index": i, "seeds": ",".join(rec["seeds"])})
    gaps = coverage_problems(ran, expected)
    if len(ran) < shards:
        unread = [g for g in gaps if g.endswith(" is in no shard")]
        gaps = [g for g in gaps if g not in unread]
        if unread:
            problems.append(f"{len(unread)} expected seed(s) sit in the {shards - len(ran)} shard(s) above that gave no record")
    problems += gaps
    return problems, sorted(s for sh in ran for s in sh["seeds"].split(",") if s)


def not_graded_here(root: Path, sha: str, shards: int) -> int:
    """The NOT-GRADED-HERE rows the shards' records add up to."""
    total = 0
    for i in range(shards):
        rec, _ = shard_line(root / f"recall-shard-{i}", sha)
        total += int(rec.get("not_graded_here", 0)) if rec else 0
    return total


def verdict(root: Path, sha: str, shards: int, expected: Collection[str], ceiling: int, record: Path) -> list[str]:
    """Judge the shards under `root`, append the verdict line to `record`, return the problems."""
    problems, ran = judge(root, sha, shards, sorted(expected))
    here = not_graded_here(root, sha, shards)
    if here > ceiling:
        problems.append(f"{here} row(s) NOT-GRADED-HERE across the shards, over the gate matrix's ceiling of {ceiling}")
    run_record.append_run(record, sha=sha, complete=False, green=not problems, dirty=False,
                          seeds=ran if not problems else sorted(expected), not_graded_here=here)
    return problems


def self_test() -> int:
    """Each failure of the verdict is shown against its own mutant: a missing shard, a red or dirty one, a lost or doubled seed, a wrong sha, a ceiling overrun."""
    sha, expected = "a" * 40, ["s1", "s2", "s3", "s4"]

    def world(root: Path, **kw) -> None:
        """Two shards, each green and clean on `sha`, then `kw` mutates one."""
        for i, seeds in enumerate((["s1", "s2"], ["s3", "s4"])):
            d = root / f"recall-shard-{i}"
            d.mkdir(parents=True)
            line = {"sha": sha, "complete": False, "green": True, "dirty": False, "seeds": seeds, "not_graded_here": 0}
            line.update(kw.get(f"line{i}", {}))
            (d / SHARD_FILES[1]).write_text(json.dumps(line) + "\n")
            (d / SHARD_FILES[0]).write_text(kw.get(f"exit{i}", "0"))
        if kw.get("drop"):
            for f in (root / "recall-shard-1").iterdir():
                f.unlink()
            (root / "recall-shard-1").rmdir()

    cases = [
        ({}, None),
        ({"drop": True}, "shard 1: no artifact"),
        ({"exit1": "1"}, "shard 1: exit code 1"),
        ({"line1": {"green": False}}, "red or ran on a dirty tree"),
        ({"line1": {"dirty": True}}, "red or ran on a dirty tree"),
        ({"line1": {"sha": "b" * 40}}, "no record line"),
        ({"line1": {"seeds": ["s3"]}}, "seed s4 is in no shard"),
        ({"line1": {"green": False}}, "2 expected seed(s) sit in the 1 shard(s) above"),
        ({"line1": {"seeds": ["s3", "s4", "s1"]}}, "seed s1 is in 2 shards"),
        ({"line1": {"seeds": ["s3", "s4", "s9"]}}, "seed s9 is in a shard but the ledger does not place it"),
        ({"line0": {"not_graded_here": 4}, "line1": {"not_graded_here": 4}}, "NOT-GRADED-HERE across the shards"),
    ]
    for i, (kw, want) in enumerate(cases):
        with tempfile.TemporaryDirectory() as td:
            root, record = Path(td) / "shards", Path(td) / "rec.jsonl"
            world(root, **kw)
            got = verdict(root, sha, 2, expected, 7, record)
            assert (not got) if want is None else any(want in p for p in got), (i, kw, got)
            assert not (any(p.startswith("shard ") for p in got) and any("is in no shard" in p for p in got)), (i, "a not-green shard's seeds are one line, not one per seed", got)
            line = json.loads(record.read_text().splitlines()[-1])
            assert line["green"] is (want is None) and line["sha"] == sha and line["seeds"] == expected, (i, line)
            ok = run_record.refusal(sha, record, expected)
            assert (ok is None) if want is None else (ok is not None), (i, "the tag rule must read the verdict", ok)
    flow = WORKFLOW.read_text()
    for name in SHARD_FILES:
        assert f"out/verdict/{name}" in flow, f"the workflow does not write {name} where the verdict reads it"
    assert "name: recall-shard-${{ matrix.shard.index }}\n          path: out/verdict/\n" in flow, "the shard artifact must hold exactly the verdict folder"
    print(f"self-test ok: recall verdict — {len(cases)} cases, a missing, red, dirty, lost, doubled or foreign shard never reads as green")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """CLI: judge `--dir` (one `recall-shard-N` folder per shard) for `--sha`, print the problems, append the line to `--record`."""
    ap = argparse.ArgumentParser(prog="verdict")
    ap.add_argument("--dir", default="shards")
    ap.add_argument("--sha", default="")
    ap.add_argument("--shards", type=int, default=0)
    ap.add_argument("--expected", default="")
    ap.add_argument("--record", default="out/recall-runs.jsonl")
    ap.add_argument("--self-test", action="store_true")
    ns = ap.parse_args(argv)
    if ns.self_test:
        return self_test()
    expected = [s for s in ns.expected.split(",") if s]
    if not (ns.sha and ns.shards and expected):
        print("verdict: --sha, --shards and --expected are all required", file=sys.stderr)
        return 2
    problems = verdict(Path(ns.dir), ns.sha, ns.shards, expected, gate_ceiling("not_graded_here_ceiling"), Path(ns.record))
    print(f"recall verdict for {ns.sha[:12]}: {len(expected)} seed(s) over {ns.shards} shard(s): " + ("GREEN" if not problems else "RED"))
    for p in problems:
        print(f"  {p}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
