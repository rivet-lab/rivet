"""The seeded recall's run record: one line per recall run, read by release.py to decide whether a sha may be tagged."""

from __future__ import annotations

import json
import os
from pathlib import Path

RUNS_FILE = "recall-runs.jsonl"


def seeded_dir() -> Path:
    """Where the scratch tree and the run record live (RIVET_SEEDED_DIR, else target/seeded); never the repo tree."""
    return Path(os.environ.get("RIVET_SEEDED_DIR", Path(__file__).resolve().parents[2] / "target" / "seeded"))


def append_run(path: Path, *, sha: str, complete: bool, green: bool, dirty: bool) -> None:
    """Append one recall run's record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps({"sha": sha, "complete": complete, "green": green, "dirty": dirty}, sort_keys=True) + "\n")


def refusal(sha: str, record: Path) -> str | None:
    """Why `sha` may not be tagged on recall's account: the latest recall of exactly this sha in `record` is not complete, green and from a clean tree."""
    if not record.is_file():
        return f"no seeded-recall record at {record}: run `make seeded-recall` on {sha[:12]}"
    seen, last = 0, None
    for line in record.read_text().splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict) and rec.get("sha") == sha:
            seen, last = seen + 1, rec
    if last is not None and last.get("complete") is True and last.get("green") is True and last.get("dirty") is False:
        return None
    also = f" (the latest of {seen} run(s) of it is incomplete, red or dirty, and an earlier green cannot support a tag)" if seen else ""
    return f"no complete green seeded recall of {sha[:12]} in {record}{also}: run `make seeded-recall` with no --seeds or --engines"
