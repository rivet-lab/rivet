#!/usr/bin/env python3
"""One line per graded sha on the `gate-history` branch: the facts of the hosted gate half, and what changed against the line before.

    gate_history.py seen   --sha SHA --event EVENT [--remote URL]                 write `seen=true|false` to $GITHUB_OUTPUT (true only for a schedule event on a sha already on the branch)
    gate_history.py record --artifacts DIR --sha SHA --slices N [--remote URL]    append the sha's line to history.jsonl and push the branch (never any other branch)
    gate_history.py --self-test

A line holds only what the slices wrote: verdict, test counts, self-skips, minutes per slice, the red keys and the delta against the previous line.
Reads GITHUB_REPOSITORY, GITHUB_REF, GITHUB_RUN_ID, GITHUB_OUTPUT and GH_TOKEN; the token reaches git through GIT_CONFIG_* and never through argv.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hosted_gate_verdict as hosted  # noqa: E402

BRANCH = "gate-history"
FILE = "history.jsonl"
MAIN_REF = "refs/heads/main"
WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "gate-hosted.yml"
RETRIES = 4
BOT = ("github-actions[bot]", "41898282+github-actions[bot]@users.noreply.github.com")
RED_STATUS = ("FAIL", "INFRA")


def red_keys(log: str) -> list[str]:
    """`engine/scenario STATUS` for every red cell of a slice's result table."""
    keys = set()
    for line in log.splitlines():
        m = hosted.CELL.match(line)
        if m and m.group(5) in RED_STATUS:
            keys.add(f"{m.group(1)}/{m.group(3)} {m.group(5)}")
    return sorted(keys)


def history_line(root: Path, sha: str, slices: int, run_id: str, previous: dict | None) -> dict:
    """The line of `sha`: per slice its facts, the sums, the red keys, and the delta against `previous`."""
    per, reds, skips = [], set(), set()
    for i in range(slices + 1):
        rec, log = hosted.load_slice(root, i)
        code = hosted.load_side(root, i, "exit-code.txt")
        if rec is None:
            per.append({"slice": i, "exit": code, "lost": True})
            reds.add(f"slice-{i} lost")
            continue
        per.append({"slice": i, "exit": code, "verdict": rec.get("verdict"), "passed": rec.get("passed"), "failed": rec.get("failed"),
                    "skipped": rec.get("skipped"), "minutes": rec.get("total_min"), "binary": str(rec.get("binary_sha256"))[:12], "at": rec.get("at")})
        reds.update(red_keys(log))
        if code != 0:
            reds.add(f"slice-{i} exit {code}")
        skips.update("/".join(map(str, s)) for s in rec.get("skipped_env") or [])
    ok = hosted.render(root, sha, slices)[1]
    before = set((previous or {}).get("red_keys", []))
    count = lambda k: sum(p.get(k) or 0 for p in per)  # noqa: E731
    return {"schema": 1, "sha": sha, "run_id": run_id, "verdict": "GREEN" if ok else "RED", "slices": per,
            "tests": {"passed": count("passed"), "failed": count("failed"), "skipped": count("skipped")},
            "self_skips": sorted(skips), "red_keys": sorted(reds),
            "delta": {"against": (previous or {}).get("sha"), "new": sorted(reds - before), "fixed": sorted(before - reds)}}


def git(cwd: Path, *args: str, token: str = "", check: bool = True) -> subprocess.CompletedProcess:
    """`git ARGS` in `cwd`; a token reaches the transport through GIT_CONFIG_*, never through the command line."""
    env = dict(os.environ)
    if token:
        auth = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        env.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.https://github.com/.extraheader", GIT_CONFIG_VALUE_0=f"AUTHORIZATION: basic {auth}")
    return subprocess.run(["git", *args], cwd=cwd, env=env, capture_output=True, text=True, check=check)


def remote_url() -> str:
    """The origin the workflow pushes to."""
    return f"https://github.com/{os.environ['GITHUB_REPOSITORY']}.git"


def clone_branch(work: Path, remote: str, token: str) -> list[dict]:
    """Make `work` a checkout of the branch (or of an empty branch when the remote has none) and return its lines."""
    git(work, "init", "-q", "-b", BRANCH)
    git(work, "remote", "add", "origin", remote)
    if git(work, "ls-remote", "--exit-code", "--heads", "origin", f"refs/heads/{BRANCH}", token=token, check=False).returncode != 0:
        return []
    git(work, "fetch", "-q", "--depth", "1", "origin", f"refs/heads/{BRANCH}", token=token)
    git(work, "reset", "-q", "--hard", "FETCH_HEAD")
    path = work / FILE
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()] if path.exists() else []


def seen(sha: str, event: str, remote: str, token: str) -> bool:
    """True when a schedule event finds `sha` already graded on the branch; any other event always grades."""
    if event != "schedule":
        return False
    with tempfile.TemporaryDirectory() as tmp:
        return any(x.get("sha") == sha for x in clone_branch(Path(tmp), remote, token))


def record(root: Path, sha: str, slices: int, remote: str, token: str, run_id: str) -> str:
    """Append the line of `sha` and push the branch; `recorded`, `already recorded`, or an error after the retries."""
    for _ in range(RETRIES):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            lines = clone_branch(work, remote, token)
            if any(x.get("sha") == sha for x in lines):
                return "already recorded"
            line = history_line(root, sha, slices, run_id, lines[-1] if lines else None)
            with (work / FILE).open("a") as f:
                f.write(json.dumps(line, sort_keys=True) + "\n")
            git(work, "add", FILE)
            git(work, "-c", f"user.name={BOT[0]}", "-c", f"user.email={BOT[1]}", "commit", "-q", "-m", f"gate-history: {sha[:12]} {line['verdict']}")
            if git(work, "push", "-q", "origin", f"HEAD:refs/heads/{BRANCH}", token=token, check=False).returncode == 0:
                return "recorded"
    raise SystemExit(f"gate-history: the push lost the race {RETRIES} times")


def on_main(sha: str, env: dict[str, str]) -> bool:
    """True when the run started on main and `sha` is on main's first-parent history of the checkout."""
    if env.get("GITHUB_REF") != MAIN_REF:
        return False
    return subprocess.run(["git", "merge-base", "--is-ancestor", sha, "HEAD"], capture_output=True).returncode == 0


def writers(doc: dict) -> tuple[list[str], list[str]]:
    """(jobs that run `gate_history.py record`, jobs that hold `contents: write`) of a workflow document."""
    jobs = doc["jobs"]
    runs = [n for n, j in jobs.items() if any("gate_history.py record" in str(s.get("run", "")) for s in j.get("steps", []))]
    writes = [n for n, j in jobs.items() if (j.get("permissions") or {}).get("contents") == "write"]
    return runs, writes


def self_test() -> int:
    """The branch gets one line per sha, a race retries, main is never touched, and an unreadable slice is a red key."""
    sha1, sha2 = "a" * 40, "b" * 40
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        bare, seed = base / "remote.git", base / "seed"
        subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(bare)], check=True)
        seed.mkdir()
        git(seed, "init", "-q", "-b", "main")
        (seed / "x").write_text("x")
        git(seed, "add", "x")
        git(seed, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "main")
        git(seed, "push", "-q", str(bare), "main")
        main_before = git(bare, "rev-parse", "main").stdout.strip()

        def slices_dir(name: str, over: dict | None = None) -> Path:
            root = base / name
            for i in range(4):
                hosted._fixture(root, i, **(over or {}).get(i, {}))
            return root

        green, red = slices_dir("green"), slices_dir("red", {1: {"verdict": "NOT RELEASABLE", "exit_code": 2}, 3: {"drop": True}})
        url = str(bare)
        assert not seen(sha1, "schedule", url, ""), "an empty remote has seen nothing"
        assert record(green, sha1, 3, url, "", "7") == "recorded"
        assert record(green, sha1, 3, url, "", "8") == "already recorded", "one line per sha"
        assert seen(sha1, "schedule", url, "") and not seen(sha1, "workflow_dispatch", url, ""), "only a schedule event skips a graded sha"
        assert record(red, sha2, 3, url, "", "9") == "recorded"
        with tempfile.TemporaryDirectory() as t2:
            lines = clone_branch(Path(t2), url, "")
        assert [x["sha"] for x in lines] == [sha1, sha2] and lines[0]["verdict"] == "GREEN" and lines[1]["verdict"] == "RED", lines
        assert lines[0]["tests"] == {"passed": 40, "failed": 0, "skipped": 0} and lines[0]["slices"][0]["minutes"] == 1.0, lines[0]
        assert "slice-3 lost" in lines[1]["red_keys"] and "slice-1 exit 2" in lines[1]["red_keys"] and "mysql/live FAIL" in lines[1]["red_keys"], lines[1]["red_keys"]
        assert lines[1]["delta"] == {"against": sha1, "new": ["slice-1 exit 2", "slice-3 lost"], "fixed": []}, lines[1]["delta"]
        assert git(bare, "rev-parse", "main").stdout.strip() == main_before, "main is never written"
        assert sorted(git(bare, "for-each-ref", "--format=%(refname)").stdout.split()) == ["refs/heads/gate-history", "refs/heads/main"]
        racer, mine = base / "racer", base / "mine"
        for d in (racer, mine):
            d.mkdir()
        orig = sys.modules[__name__].clone_branch
        state = {"n": 0}

        def racing_clone(work: Path, remote: str, token: str) -> list[dict]:
            """Clone, then let another writer push a line before this one pushes."""
            got = orig(work, remote, token)
            if state["n"] == 0:
                state["n"] = 1
                record(green, "c" * 40, 3, remote, token, "10")
            return got

        sys.modules[__name__].clone_branch = racing_clone
        try:
            assert record(green, "d" * 40, 3, url, "", "11") == "recorded"
        finally:
            sys.modules[__name__].clone_branch = orig
        with tempfile.TemporaryDirectory() as t3:
            assert [x["sha"][0] for x in clone_branch(Path(t3), url, "")] == ["a", "b", "c", "d"], "the lost race is retried on the new tip"
    doc = yaml.safe_load(WORKFLOW.read_text())
    runs, writes = writers(doc)
    assert len(runs) == 1 and writes == runs and doc["permissions"] == {"contents": "read"}, (runs, writes, doc["permissions"])
    wide = yaml.safe_load(WORKFLOW.read_text())
    wide["jobs"]["plan"]["permissions"] = {"contents": "write"}
    assert writers(wide)[1] != writers(wide)[0], "contents: write on a job that does not record is seen"
    assert red_keys("  oracle     -      no_silent_null   -        FAIL x\n  mysql 8 live s3 KNOWN y\n") == ["oracle/no_silent_null FAIL"]
    assert not on_main(sha1, {"GITHUB_REF": "refs/heads/feature"}), "a run off main never records"
    print("gate_history self-test ok")
    return 0


def main(argv: list[str]) -> int:
    """`seen`, `record` or `--self-test`."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", nargs="?", choices=["seen", "record"])
    ap.add_argument("--artifacts", type=Path)
    ap.add_argument("--sha")
    ap.add_argument("--slices", type=int)
    ap.add_argument("--event", default="")
    ap.add_argument("--remote")
    ap.add_argument("--self-test", action="store_true")
    ns = ap.parse_args(argv)
    if ns.self_test:
        return self_test()
    if not ns.command or not ns.sha:
        ap.error("a command and --sha are required")
    token, env = os.environ.get("GH_TOKEN", ""), dict(os.environ)
    remote = ns.remote or remote_url()
    if ns.command == "seen":
        out = f"seen={'true' if seen(ns.sha, ns.event, remote, token) else 'false'}"
        print(out)
        if env.get("GITHUB_OUTPUT"):
            with open(env["GITHUB_OUTPUT"], "a") as f:
                f.write(out + "\n")
        return 0
    if ns.artifacts is None or not ns.slices:
        ap.error("record needs --artifacts and --slices")
    if not on_main(ns.sha, env):
        print(f"gate-history: {ns.sha[:12]} is not a main commit of a main run; nothing recorded")
        return 0
    print(f"gate-history: {record(ns.artifacts, ns.sha, ns.slices, remote, token, env.get('GITHUB_RUN_ID', ''))}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
