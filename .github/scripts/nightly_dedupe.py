#!/usr/bin/env python3
"""Keeps the cron fallback of the nightly workflow from repeating a dispatched run, and names the last real green run.

    nightly_dedupe.py check    write `duplicate=true|false` to $GITHUB_OUTPUT (a schedule event on a sha a recent dispatch covers)
    nightly_dedupe.py green    print the head sha of the newest run whose `Nightly verdict` job succeeded, or nothing
    nightly_dedupe.py --self-test

Reads GITHUB_EVENT_NAME, GITHUB_SHA, GITHUB_REPOSITORY, GITHUB_OUTPUT and GH_TOKEN.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

WORKFLOW = "nightly-live.yml"
VERDICT_JOB = "Nightly verdict"
WINDOW_H = 20
GATE_JOBS = {"dedupe", "start"}
ROOT = Path(__file__).resolve().parents[2]


def covers(runs: list[dict]) -> bool:
    """True when any dispatched run succeeded or has not finished."""
    return any(r["status"] != "completed" or r["conclusion"] == "success" for r in runs)


def decide(event: str, runs: list[dict]) -> bool:
    """A duplicate is only ever a schedule event with a covering dispatch run."""
    return event == "schedule" and covers(runs)


def first_green(runs: list[dict], verdict: dict[int, str]) -> str:
    """The head sha of the first run (newest first) whose verdict job concluded success."""
    return next((r["headSha"] for r in runs if verdict.get(r["databaseId"]) == "success"), "")


def gh(*args: str) -> str:
    """Stdout of `gh args`; a failure raises with gh's own message."""
    p = subprocess.run(["gh", *args], capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)}: {p.stderr.strip()}")
    return p.stdout


def check() -> int:
    """Write the duplicate verdict; a failed query fails open so the run proceeds with a warning."""
    event, sha = os.environ["GITHUB_EVENT_NAME"], os.environ["GITHUB_SHA"]
    since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=WINDOW_H)).strftime("%Y-%m-%dT%H:%M:%SZ")
    runs: list[dict] = []
    if event == "schedule":
        try:
            runs = json.loads(gh("run", "list", "--workflow", WORKFLOW, "--event", "workflow_dispatch", "--commit", sha,
                                 "--created", f">={since}", "--json", "status,conclusion", "--limit", "50"))
        except RuntimeError as e:
            print(f"::warning title=Nightly dedupe::query failed, running anyway: {e}")
    dup = decide(event, runs)
    print(f"event={event} sha={sha[:9]} dispatched_runs={len(runs)} duplicate={str(dup).lower()}")
    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        f.write(f"duplicate={str(dup).lower()}\n")
    return 0


def green() -> int:
    """Print the sha of the newest successful run on this branch whose verdict job really ran green."""
    runs = json.loads(gh("run", "list", "--workflow", WORKFLOW, "--branch", os.environ["GITHUB_REF_NAME"],
                         "--status", "success", "--limit", "10", "--json", "databaseId,headSha"))
    verdict = {}
    for r in runs:
        jobs = json.loads(gh("run", "view", str(r["databaseId"]), "--json", "jobs"))["jobs"]
        verdict[r["databaseId"]] = next((j["conclusion"] for j in jobs if j["name"] == VERDICT_JOB), "")
    print(first_green(runs, verdict))
    return 0


def unguarded(workflow: dict) -> list[str]:
    """The jobs that do not transitively wait on `start`."""
    jobs = workflow["jobs"]

    def waits(name: str, seen: frozenset[str] = frozenset()) -> bool:
        needs = jobs[name].get("needs", [])
        needs = [needs] if isinstance(needs, str) else needs
        return "start" in needs or any(n not in seen and waits(n, seen | {name}) for n in needs)

    return sorted(j for j in jobs if j not in GATE_JOBS and not waits(j))


def self_test() -> int:
    """The decision, the green pick and the workflow wiring each fail on the mutant they exist to catch."""
    done = {"status": "completed", "conclusion": "success"}
    live = {"status": "in_progress", "conclusion": ""}
    red = {"status": "completed", "conclusion": "failure"}
    cancelled = {"status": "completed", "conclusion": "cancelled"}
    assert decide("schedule", [done]) and decide("schedule", [live]) and decide("schedule", [red, live])
    assert not decide("schedule", []) and not decide("schedule", [red, cancelled])
    assert not decide("workflow_dispatch", [done]) and not decide("push", [live])
    runs = [{"databaseId": 3, "headSha": "skipped"}, {"databaseId": 2, "headSha": "real"}, {"databaseId": 1, "headSha": "old"}]
    assert first_green(runs, {3: "skipped", 2: "success", 1: "success"}) == "real"
    assert first_green(runs, {3: "", 2: "failure"}) == "" and first_green([], {}) == ""
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / WORKFLOW).read_text())
    assert unguarded(wf) == [], f"jobs that run on a duplicate schedule: {unguarded(wf)}"
    assert wf["jobs"]["start"]["needs"] == "dedupe" and "duplicate" in wf["jobs"]["start"]["if"]
    for j in ("nightly-verdict", "e2e-matrix"):
        assert "needs.start.result" in wf["jobs"][j]["if"], f"{j} would grade a skipped run"
    chain = {"jobs": {"a": {}, "b": {"needs": "a"}, "c": {"needs": ["b"]}, "start": {}, "dedupe": {}}}
    assert unguarded(chain) == ["a", "b", "c"]
    chain["jobs"]["a"]["needs"] = "start"
    assert unguarded(chain) == []
    print("self-test ok: duplicate only on a schedule event with a covering dispatch; every job waits on start")
    return 0


def main(argv: list[str]) -> int:
    """Dispatch the subcommand."""
    cmds = {"check": check, "green": green, "--self-test": self_test}
    if len(argv) != 1 or argv[0] not in cmds:
        print(__doc__, file=sys.stderr)
        return 2
    return cmds[argv[0]]()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
