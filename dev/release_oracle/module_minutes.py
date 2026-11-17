"""The measured cost of every live module, read from the nextest logs of one hosted run, and what a set of modules costs a slice.

    python3 -m dev.release_oracle.module_minutes refresh RUN_ID    download the slices of RUN_ID and rewrite module_minutes.json
    python3 -m dev.release_oracle.module_minutes --self-test
"""

from __future__ import annotations

import functools
import json
import math
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .core import ROOT

__all__ = ["Table", "load", "measured", "module_cost", "slice_minutes", "build_table", "TABLE_PATH"]

TABLE_PATH = ROOT / "dev" / "release_oracle" / "module_minutes.json"
GROWTH_SLACK = 1.25
GROWTH_TESTS = 3
TEST_LINE = re.compile(r"^\s+(?:TRY \d+ )?(?:PASS|FAIL)\s+\[\s*([\d.]+)s\].*?live_suite (\w+)::(\S+)", re.M)
SUMMARY_LINE = re.compile(r"^\s+Summary\s+\[\s*([\d.]+)s\]", re.M)


@dataclass(frozen=True)
class Table:
    """Per-module (tests, test-minutes) measured on one run, plus the slice model fitted to the same run."""

    modules: dict[str, tuple[int, float]]
    fixed_min: float
    wall_per_cost: float
    fallback_sec_per_test: float


def parse_nextest(text: str) -> tuple[dict[tuple[str, str], float], float]:
    """(seconds per (module, test), summary wall seconds) of one nextest log; a FAIL repeated in the final summary counts once."""
    return {(m, t): float(s) for s, m, t in TEST_LINE.findall(text)}, sum(float(s) for s in SUMMARY_LINE.findall(text))


def build_table(slices: dict[int, tuple[str, list[str]]], run_id: str, sha: str) -> dict:
    """The table JSON of a run from `slices` = {slice: (record.json text, [nextest log texts])}: per-module minutes, the slowest wall per test-minute and the fixed minutes that cover every slice."""
    per_module: dict[str, list] = {}
    fits = []
    for _, (record, logs) in sorted(slices.items()):
        secs, wall = {}, 0.0
        for log in logs:
            s, w = parse_nextest(log)
            secs.update(s)
            wall += w
        for (m, _), d in secs.items():
            cell = per_module.setdefault(m, [0, 0.0])
            cell[0] += 1
            cell[1] += d / 60
        fits.append((json.loads(record)["total_min"], wall / 60, sum(secs.values()) / 60, len(secs)))
    rate = round(max(w / c for _, w, c, _ in fits), 3)
    return {
        "run_id": run_id,
        "sha": sha,
        "fixed_min": math.ceil(max(t - rate * c for t, _, c, _ in fits) * 10) / 10,
        "wall_per_cost": rate,
        "fallback_sec_per_test": round(max(c * 60 / n for _, _, c, n in fits), 1),
        "modules": {m: {"tests": n, "minutes": round(x, 2)} for m, (n, x) in sorted(per_module.items())},
    }


@functools.cache
def load(path: Path = TABLE_PATH) -> Table:
    """The committed table."""
    raw = json.loads(path.read_text())
    return Table({m: (v["tests"], v["minutes"]) for m, v in raw["modules"].items()}, raw["fixed_min"], raw["wall_per_cost"], raw["fallback_sec_per_test"])


def measured(table: Table, module: str) -> bool:
    """Whether `module` has a measured cost."""
    return module in table.modules


def module_cost(table: Table, module: str, tests: int) -> float:
    """The test-minutes of `module` with `tests` tests: its measured minutes (scaled by its growth only past the slack), else the slowest measured rate times `tests`."""
    if module not in table.modules:
        return tests * table.fallback_sec_per_test / 60
    seen, minutes = table.modules[module]
    return minutes * tests / seen if tests > seen * GROWTH_SLACK + GROWTH_TESTS else minutes


def slice_minutes(table: Table, cost: float) -> float:
    """The minutes a hosted slice holding `cost` test-minutes takes: the fixed build, then the slowest measured wall per test-minute."""
    return table.fixed_min + cost * table.wall_per_cost


def render(table: dict) -> str:
    """The table JSON, one module per line."""
    head = ",\n".join(f" {json.dumps(k)}: {json.dumps(v)}" for k, v in table.items() if k != "modules")
    mods = ",\n".join(f"  {json.dumps(m)}: {json.dumps(v)}" for m, v in table["modules"].items())
    return "{\n" + head + ',\n "modules": {\n' + mods + "\n }\n}\n"


def refresh(run_id: str) -> None:
    """Rewrite module_minutes.json from the slice artifacts of hosted run `run_id`."""
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(["gh", "run", "download", run_id, "-p", "slice-*", "-D", tmp], check=True)
        sha = subprocess.run(["gh", "run", "view", run_id, "--json", "headSha", "--jq", ".headSha"], check=True, capture_output=True, text=True).stdout.strip()
        slices = {}
        for d in sorted(Path(tmp).glob("slice-*")):
            logs = [f.read_text(errors="replace") for f in sorted((d / "nextest").glob("[0-9]*.log"))]
            if logs and (d / "record.json").is_file():
                slices[int(d.name.split("-")[1])] = ((d / "record.json").read_text(), logs)
    TABLE_PATH.write_text(render(build_table(slices, run_id, sha)))
    load.cache_clear()


def self_test() -> None:
    """The nextest parser, the table build, the cost rule and the slice model on fixtures."""
    log_a = ("        PASS [   6.000s] (  1/2) rivet-cli::live_suite m_a::one\n        FAIL [  30.000s] (  2/2) rivet-cli::live_suite m_a::two\n"
             "     Summary [  60.000s] 2 tests run\n        FAIL [  30.000s] (  2/2) rivet-cli::live_suite m_a::two\n")
    log_b = ("        PASS [  12.000s] (  1/2) rivet-cli::live_suite m_b::three\n        TRY 2 PASS [  18.000s] (  2/2) rivet-cli::live_suite m_b::four\n"
             "     Summary [  30.000s] 2 tests run\n")
    secs, wall = parse_nextest(log_a)
    assert secs == {("m_a", "one"): 6.0, ("m_a", "two"): 30.0} and wall == 60.0, (secs, wall)
    built = build_table({1: (json.dumps({"total_min": 20.0}), [log_a]), 2: (json.dumps({"total_min": 50.0}), [log_b])}, "7", "abc")
    assert built["modules"] == {"m_a": {"tests": 2, "minutes": 0.6}, "m_b": {"tests": 2, "minutes": 0.5}}, built["modules"]
    assert built["wall_per_cost"] == 1.667 and built["fixed_min"] == 49.2 and built["fallback_sec_per_test"] == 18.0, built
    assert json.loads(render(built)) == built, "the rendered table does not read back"
    t = Table({"m": (10, 5.0)}, 30.0, 0.3, 12.0)
    assert module_cost(t, "m", 10) == module_cost(t, "m", 11) == module_cost(t, "m", 15) == 5.0, "growth inside the slack moved a module's cost"
    assert module_cost(t, "m", 20) == 10.0, "a module that doubled is still priced at its old minutes"
    assert module_cost(t, "unlisted", 5) == 1.0 and not measured(t, "unlisted") and measured(t, "m"), "an unmeasured module is not priced at the slowest rate"
    assert slice_minutes(t, 100.0) == 60.0, slice_minutes(t, 100.0)
    real = load()
    assert real.modules and real.fixed_min > 0 and 0 < real.wall_per_cost < 1 and real.fallback_sec_per_test > 0, "the committed table is empty or unreadable"
    print("self-test ok: nextest lines parse once per test, a module is priced by its measured minutes, an unmeasured one by the slowest rate")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "refresh":
        refresh(sys.argv[2])
    elif sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        raise SystemExit("usage: python3 -m dev.release_oracle.module_minutes refresh RUN_ID | --self-test")
