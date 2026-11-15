#!/usr/bin/env python3
"""One line per graded run on the `gate-history` branch: the facts of the hosted gate half, the ref it was graded under, and what changed against the line before of that ref.

    gate_history.py seen   --sha SHA --event EVENT [--remote URL]                 write `seen=true|false` to $GITHUB_OUTPUT (true only for a schedule event on a sha already graded under this ref)
    gate_history.py record --artifacts DIR --sha SHA --slices N [--remote URL]    append the run's line to history.jsonl and push the branch (never any other branch)
    gate_history.py show   (--ref REF | --sha SHA) [--remote URL]                 print the lines of one ref (`refs/heads/main` or `main`) or of one sha (full or prefix)
    gate_history.py --self-test

A line holds the run's identity (ref, ref_name, ref_type, event, run_id, run_url, and workflow_sha when it is not the graded sha) and only what the slices wrote:
verdict, test counts, self-skips, minutes per slice, the red keys and the delta against the previous line of the same ref. The key of a line is sha + ref + run_id.
Reads GITHUB_* (through hosted_gate_verdict.run_identity) and GH_TOKEN; the token reaches git through GIT_CONFIG_* and never through argv.
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
REF_TYPES = {"refs/heads/": "branch", "refs/tags/": "tag"}
EVENTS = ("schedule", "workflow_dispatch", "push")


def identity(env: dict[str, str]) -> dict[str, str]:
    """The run identity of `env`, refused (SystemExit) when its ref, ref type, ref name, event, run id or run url is missing or inconsistent."""
    gh = hosted.run_identity(env)
    prefix = next((p for p in REF_TYPES if gh["ref"].startswith(p)), None)
    bad = []
    if prefix is None:
        bad.append(f"ref {gh['ref']!r} is neither refs/heads/* nor refs/tags/*")
    else:
        if gh["ref_type"] != REF_TYPES[prefix]:
            bad.append(f"ref_type {gh['ref_type']!r} does not match ref {gh['ref']!r}")
        if gh["ref_name"] != gh["ref"][len(prefix):]:
            bad.append(f"ref_name {gh['ref_name']!r} does not match ref {gh['ref']!r}")
    if gh["event"] not in EVENTS:
        bad.append(f"event {gh['event']!r} is not one of {EVENTS}")
    if not gh["run_id"].isdigit():
        bad.append(f"run_id {gh['run_id']!r} is not a run number")
    if not gh["run_url"]:
        bad.append("no run url (GITHUB_SERVER_URL, GITHUB_REPOSITORY)")
    if bad:
        raise SystemExit("gate-history: the run identity is not usable: " + "; ".join(bad))
    return gh


def line_key(line: dict) -> tuple:
    """What makes a line the same line: the sha, the ref it was graded under and the run."""
    return line.get("sha"), line.get("ref"), line.get("run_id")


def red_keys(log: str) -> list[str]:
    """`engine/scenario STATUS` for every red cell of a slice's result table."""
    keys = set()
    for line in log.splitlines():
        m = hosted.CELL.match(line)
        if m and m.group(5) in RED_STATUS:
            keys.add(f"{m.group(1)}/{m.group(3)} {m.group(5)}")
    return sorted(keys)


def history_line(root: Path, sha: str, slices: int, ident: dict[str, str], previous: dict | None) -> dict:
    """The line of `sha` graded by the run `ident`: per slice its facts, the sums, the red keys, and the delta against `previous`."""
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
    who = {k: ident[k] for k in ("ref", "ref_name", "ref_type", "event", "run_id", "run_attempt", "run_url")}
    if ident["workflow_sha"] != sha:
        who["workflow_sha"] = ident["workflow_sha"]
    return {"schema": 1, "sha": sha, **who, "verdict": "GREEN" if ok else "RED", "slices": per,
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
    """The origin the workflow pushes to, or the `origin` of the current checkout outside a run."""
    if os.environ.get("GITHUB_REPOSITORY"):
        return f"https://github.com/{os.environ['GITHUB_REPOSITORY']}.git"
    return subprocess.run(["git", "remote", "get-url", "origin"], capture_output=True, text=True, check=True).stdout.strip()


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


def seen(sha: str, ref: str, event: str, remote: str, token: str) -> bool:
    """True when a schedule event finds `sha` already graded under `ref` on the branch; any other event always grades."""
    if event != "schedule":
        return False
    with tempfile.TemporaryDirectory() as tmp:
        return any(x.get("sha") == sha and x.get("ref") == ref for x in clone_branch(Path(tmp), remote, token))


def record(root: Path, sha: str, slices: int, remote: str, token: str, ident: dict[str, str]) -> str:
    """Append the line of the run `ident` and push the branch; `recorded`, `already recorded` (same sha, ref and run), or an error after the retries."""
    for _ in range(RETRIES):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            lines = clone_branch(work, remote, token)
            if any(line_key(x) == (sha, ident["ref"], ident["run_id"]) for x in lines):
                return "already recorded"
            line = history_line(root, sha, slices, ident, next((x for x in reversed(lines) if x.get("ref") == ident["ref"]), None))
            with (work / FILE).open("a") as f:
                f.write(json.dumps(line, sort_keys=True) + "\n")
            git(work, "add", FILE)
            git(work, "-c", f"user.name={BOT[0]}", "-c", f"user.email={BOT[1]}", "commit", "-q", "-m", f"gate-history: {sha[:12]} {line['verdict']} {ident['ref']}")
            if git(work, "push", "-q", "origin", f"HEAD:refs/heads/{BRANCH}", token=token, check=False).returncode == 0:
                return "recorded"
    raise SystemExit(f"gate-history: the push lost the race {RETRIES} times")


def show(lines: list[dict], ref: str | None, sha: str | None) -> list[dict]:
    """The lines of `ref` (full ref or ref name) or of `sha` (full or prefix), in branch order."""
    return [x for x in lines if (ref is None or ref in (x.get("ref"), x.get("ref_name"))) and (sha is None or str(x.get("sha")).startswith(sha))]


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
    """The branch gets one line per sha+ref+run, a race retries, main is never touched, an unreadable slice is a red key, and every line names its ref."""
    sha1, sha2 = "a" * 40, "b" * 40

    def env(ref: str = "refs/heads/main", run: str = "7", event: str = "schedule", sha: str = sha1) -> dict[str, str]:
        """The GITHUB_* of a run on `ref`."""
        kind = "tag" if ref.startswith("refs/tags/") else "branch"
        return {"GITHUB_REF": ref, "GITHUB_REF_NAME": ref.split("/", 2)[2], "GITHUB_REF_TYPE": kind, "GITHUB_EVENT_NAME": event, "GITHUB_RUN_ID": run,
                "GITHUB_RUN_ATTEMPT": "1", "GITHUB_REPOSITORY": "o/r", "GITHUB_SERVER_URL": "https://github.com", "GITHUB_SHA": sha}

    def ident(**kw: str) -> dict[str, str]:
        """The identity of such a run."""
        return identity(env(**kw))

    def refused(**over: str) -> bool:
        """True when `identity` refuses the run env with `over` replacing its variables."""
        try:
            identity({**env(), **over})
        except SystemExit:
            return True
        return False

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
        tag = "refs/tags/v0.33.0"
        assert not seen(sha1, "refs/heads/main", "schedule", url, ""), "an empty remote has seen nothing"
        assert record(green, sha1, 3, url, "", ident()) == "recorded"
        assert record(green, sha1, 3, url, "", ident()) == "already recorded", "the same sha, ref and run is one line"
        assert record(green, sha1, 3, url, "", ident(run="8", event="workflow_dispatch")) == "recorded", "a second run of the sha under the same ref is not dropped"
        assert record(green, sha1, 3, url, "", ident(ref=tag, run="9", event="workflow_dispatch")) == "recorded", "the same sha under a second ref is not dropped"
        assert seen(sha1, "refs/heads/main", "schedule", url, "") and not seen(sha1, "refs/heads/main", "workflow_dispatch", url, ""), "only a schedule event skips a graded sha"
        assert not seen(sha1, "refs/heads/other", "schedule", url, ""), "a sha graded under other refs is not graded under this one"
        assert record(red, sha2, 3, url, "", ident(run="10", sha=sha1)) == "recorded"
        with tempfile.TemporaryDirectory() as t2:
            lines = clone_branch(Path(t2), url, "")
        assert [(x["sha"][0], x["ref"], x["run_id"]) for x in lines] == [("a", "refs/heads/main", "7"), ("a", "refs/heads/main", "8"), ("a", tag, "9"), ("b", "refs/heads/main", "10")], lines
        assert lines[0]["verdict"] == "GREEN" and lines[3]["verdict"] == "RED", lines
        assert (lines[0]["ref_name"], lines[0]["ref_type"], lines[0]["event"], lines[0]["run_url"]) == ("main", "branch", "schedule", "https://github.com/o/r/actions/runs/7"), lines[0]
        assert (lines[2]["ref_name"], lines[2]["ref_type"], lines[2]["event"]) == ("v0.33.0", "tag", "workflow_dispatch"), lines[2]
        assert "workflow_sha" not in lines[0] and lines[3]["workflow_sha"] == sha1, "the workflow sha is kept only when it is not the graded sha"
        assert lines[0]["tests"] == {"passed": 40, "failed": 0, "skipped": 0} and lines[0]["slices"][0]["minutes"] == 1.0, lines[0]
        assert "slice-3 lost" in lines[3]["red_keys"] and "slice-1 exit 2" in lines[3]["red_keys"] and "mysql/live FAIL" in lines[3]["red_keys"], lines[3]["red_keys"]
        assert lines[3]["delta"] == {"against": sha1, "new": ["slice-1 exit 2", "slice-3 lost"], "fixed": []}, lines[3]["delta"]
        assert lines[2]["delta"]["against"] is None, "the first line of a ref has no line to differ from"
        assert [x["run_id"] for x in show(lines, "refs/heads/main", None)] == ["7", "8", "10"] and [x["run_id"] for x in show(lines, "v0.33.0", None)] == ["9"]
        assert [x["run_id"] for x in show(lines, None, "bbbb")] == ["10"] and [x["run_id"] for x in show(lines, "main", sha1)] == ["7", "8"], "show reads by ref or by sha"
        assert git(bare, "rev-parse", "main").stdout.strip() == main_before, "main is never written"
        assert sorted(git(bare, "for-each-ref", "--format=%(refname)").stdout.split()) == ["refs/heads/gate-history", "refs/heads/main"]
        orig = sys.modules[__name__].clone_branch
        state = {"n": 0}

        def racing_clone(work: Path, remote: str, token: str) -> list[dict]:
            """Clone, then let another writer push a line before this one pushes."""
            got = orig(work, remote, token)
            if state["n"] == 0:
                state["n"] = 1
                record(green, "c" * 40, 3, remote, token, ident(run="11", sha="c" * 40))
            return got

        sys.modules[__name__].clone_branch = racing_clone
        try:
            assert record(green, "d" * 40, 3, url, "", ident(run="12", sha="d" * 40)) == "recorded"
        finally:
            sys.modules[__name__].clone_branch = orig
        with tempfile.TemporaryDirectory() as t3:
            assert [x["sha"][0] for x in clone_branch(Path(t3), url, "")][-2:] == ["c", "d"], "the lost race is retried on the new tip"
    for what, over in (("no ref", {"GITHUB_REF": ""}), ("a pull ref", {"GITHUB_REF": "refs/pull/7/merge"}), ("ref type of the other kind", {"GITHUB_REF_TYPE": "tag"}),
                       ("ref name of another ref", {"GITHUB_REF_NAME": "other"}), ("no event", {"GITHUB_EVENT_NAME": ""}), ("an event the workflow does not run on", {"GITHUB_EVENT_NAME": "pull_request"}),
                       ("no run id", {"GITHUB_RUN_ID": ""}), ("no server url", {"GITHUB_SERVER_URL": ""})):
        assert refused(**over), f"{what}: a run identity that names no ref is never recorded"
    assert not refused(), "the control run identity is usable"
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
    """`seen`, `record`, `show` or `--self-test`."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", nargs="?", choices=["seen", "record", "show"])
    ap.add_argument("--artifacts", type=Path)
    ap.add_argument("--sha")
    ap.add_argument("--ref")
    ap.add_argument("--slices", type=int)
    ap.add_argument("--event", default="")
    ap.add_argument("--remote")
    ap.add_argument("--self-test", action="store_true")
    ns = ap.parse_args(argv)
    if ns.self_test:
        return self_test()
    token, env = os.environ.get("GH_TOKEN", ""), dict(os.environ)
    remote = ns.remote or remote_url()
    if ns.command == "show":
        if not (ns.ref or ns.sha):
            ap.error("show needs --ref or --sha")
        with tempfile.TemporaryDirectory() as tmp:
            found = show(clone_branch(Path(tmp), remote, token), ns.ref, ns.sha)
        for line in found:
            print(json.dumps(line, sort_keys=True))
        return 0 if found else 1
    if not ns.command or not ns.sha:
        ap.error("a command and --sha are required")
    if ns.command == "seen":
        out = f"seen={'true' if seen(ns.sha, env.get('GITHUB_REF', ''), ns.event, remote, token) else 'false'}"
        print(out)
        if env.get("GITHUB_OUTPUT"):
            with open(env["GITHUB_OUTPUT"], "a") as f:
                f.write(out + "\n")
        return 0
    if ns.artifacts is None or not ns.slices:
        ap.error("record needs --artifacts and --slices")
    ident = identity(env)
    if not on_main(ns.sha, env):
        print(f"gate-history: {ns.sha[:12]} is not a main commit of a main run; nothing recorded")
        return 0
    print(f"gate-history: {record(ns.artifacts, ns.sha, ns.slices, remote, token, ident)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
