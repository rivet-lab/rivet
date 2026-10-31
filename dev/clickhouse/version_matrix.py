"""Run the release gate's ClickHouse load cells against each ClickHouse server version on the local stand."""

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dev.release_oracle.clickhouse_load import CELLS, LIB_CELLS  # noqa: E402

TAGS = ("24.8", "25.8", "26.3", "26.8", "26.10")
RESULT = re.compile(r"^test (\S+) \.\.\. (ok|FAILED|ignored)", re.M)


def recreate(tag: str) -> str:
    """Recreate the stand's ClickHouse on `tag` and return the version it reports once healthy."""
    env = {**os.environ, "COMPOSE_PROJECT_NAME": "rivet", "RIVET_ALLOW_WORKTREE_LIVE": "1", "RIVET_CLICKHOUSE_TAG": tag}
    subprocess.run(["docker", "compose", "up", "-d", "--force-recreate", "--renew-anon-volumes", "clickhouse"], cwd=ROOT, env=env, check=True)
    for _ in range(120):
        state = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Health.Status}}", "rivet-clickhouse"], capture_output=True, text=True
        ).stdout.strip()
        if state == "healthy":
            return subprocess.run(
                ["curl", "-s", "-u", "rivet:rivet", "http://127.0.0.1:8123/?query=SELECT%20version()"],
                capture_output=True, text=True, check=True,
            ).stdout.strip()
        time.sleep(2)
    raise SystemExit(f"rivet-clickhouse on {tag} never became healthy")


def run_cells(target: list[str], names: list[str], log: Path) -> dict[str, str]:
    """Run the named ignored cells of one cargo test target and return each cell's outcome."""
    out = subprocess.run(
        ["cargo", "test", *target, "--", "--ignored", "--test-threads=4", *names],
        cwd=ROOT, capture_output=True, text=True,
    )
    log.write_text(out.stdout + out.stderr)
    seen = {m.group(1).rsplit("::", 1)[-1]: m.group(2) for m in RESULT.finditer(out.stdout)}
    return {n: seen.get(n, "missing") for n in names}


def main() -> None:
    """Run every gate cell on every tag and print a cell-by-version table."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tags", nargs="+", default=list(TAGS))
    ap.add_argument("--logs", type=Path, default=ROOT / "target" / "clickhouse-versions")
    args = ap.parse_args()
    args.logs.mkdir(parents=True, exist_ok=True)
    table: dict[str, dict[str, str]] = {}
    versions: dict[str, str] = {}
    for tag in args.tags:
        versions[tag] = recreate(tag)
        print(f"== {tag}: server {versions[tag]}", flush=True)
        res = run_cells(["--test", "live_suite"], list(CELLS), args.logs / f"{tag}-live.log")
        res |= run_cells(["--lib"], list(LIB_CELLS), args.logs / f"{tag}-lib.log")
        table[tag] = res
        bad = [n for n, r in res.items() if r != "ok"]
        print(f"   {len(res) - len(bad)}/{len(res)} ok" + "".join(f"\n   {res[n]}: {n}" for n in bad), flush=True)
    names = list(CELLS) + list(LIB_CELLS)
    print("\ncell | " + " | ".join(f"{t} ({versions[t]})" for t in args.tags))
    for n in names:
        label = CELLS.get(n) or LIB_CELLS[n]
        print(f"{label} | " + " | ".join(table[t][n] for t in args.tags))
    if any(r != "ok" for t in table.values() for r in t.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
