"""The seeded recall's run record: one line per recall run or shard verdict, read by release.py to decide whether a sha may be tagged."""

from __future__ import annotations

import json
import os
from collections.abc import Collection
from pathlib import Path

RUNS_FILE = "recall-runs.jsonl"


def seeded_dir() -> Path:
    """Where the scratch tree and the run record live (RIVET_SEEDED_DIR, else target/seeded); never the repo tree."""
    return Path(os.environ.get("RIVET_SEEDED_DIR", Path(__file__).resolve().parents[2] / "target" / "seeded"))


def append_run(path: Path, *, sha: str, complete: bool, green: bool, dirty: bool,
               seeds: Collection[str] = (), not_graded_here: int = 0) -> None:
    """Append one recall run's record: the seeds it covered and how many of its rows were NOT-GRADED-HERE."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps({"sha": sha, "complete": complete, "green": green, "dirty": dirty,
                            "seeds": sorted(seeds), "not_graded_here": not_graded_here}, sort_keys=True) + "\n")


def ledger_seeds(manifest: Path | None = None) -> list[str]:
    """Every seed name of the ledger: what a tag needs a green run of."""
    import yaml
    path = manifest or Path(__file__).resolve().parent / "seeds.yaml"
    return sorted(s["name"] for s in yaml.safe_load(path.read_text())["seeds"])


def covers(rec: dict, required: Collection[str]) -> set[str]:
    """The required seeds one line covers: its `seeds`, or every required seed for a line of the older shape that says `complete`."""
    if isinstance(rec.get("seeds"), list):
        return set(rec["seeds"]) & set(required)
    return set(required) if rec.get("complete") is True else set()


def refusal(sha: str, record: Path, required: Collection[str]) -> str | None:
    """Why `sha` may not be tagged on recall's account: some required seed has no run of exactly this sha in `record`, or the latest run covering it is red or from a dirty tree."""
    if not record.is_file():
        return f"no seeded-recall record at {record}: run `make seeded-recall` on {sha[:12]}"
    runs = []
    for line in record.read_text().splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict) and rec.get("sha") == sha:
            runs.append(rec)
    latest: dict[str, dict] = {}
    for rec in runs:
        for seed in covers(rec, required):
            latest[seed] = rec
    unrun = sorted(set(required) - set(latest))
    bad = sorted(s for s, rec in latest.items() if not (rec.get("green") is True and rec.get("dirty") is False))
    if not unrun and not bad:
        return None
    parts = ([f"{len(unrun)} seed(s) no run of it covers (first: {unrun[0]})"] if unrun else []) + (
        [f"{len(bad)} seed(s) whose latest run is red or dirty (first: {bad[0]})"] if bad else [])
    also = f" ({len(runs)} run(s) of it, and an earlier green cannot support a tag)" if runs else ""
    return (f"no complete green seeded recall of {sha[:12]} in {record}: " + "; ".join(parts) + also
            + ": run `make seeded-recall` (devbox seeds) and the Nightly Recall workflow (hosted seeds) on this sha")
