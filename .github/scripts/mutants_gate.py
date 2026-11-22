#!/usr/bin/env python3
"""Decide the required `Mutants (changed lines)` check: a head with Rust to mutate merges only on a recorded verdict.

    mutants_gate.py check          read the job's env, print the verdict, record it on the PR head, exit 1 on a refusal
    mutants_gate.py --self-test

Self-test mutants, each RED: ignore `held` in `decide_pr`; treat an empty state as a pass; accept an unlabeled
over-budget head; skip the key comparison in `decide_queue`; drop the `missed` refusal; hash the hunk headers in `surface_diff`.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable

CONTEXT = "mutants/graded"
WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "ci.yml"
SURFACE = re.compile(r"(\.rs$|^tests/|^Cargo\.(toml|lock)$|^\.cargo/)")
HEADER = re.compile(r"^diff --git a/(.*) b/(.*)$")
RECORD = re.compile(r"^(graded|ungraded|nothing-to-mutate) ([0-9a-f]{64})$")
QUEUE_PR = re.compile(r"/pr-(\d+)-")
ENV_KEYS = ("EVENT", "BASE_SHA", "REPO", "PR_HEAD_SHA", "QUEUE_REF", "READY", "HELD", "STATE", "RESULT")
Run = Callable[[list[str]], str]


def surface_diff(diff: str) -> list[str]:
    """The added and removed lines of the files that can change a mutation verdict, hunk positions dropped."""
    kept: list[str] = []
    live = False
    for line in diff.splitlines():
        head = HEADER.match(line)
        if line.startswith("diff --git "):
            live = not head or bool(SURFACE.search(head.group(2)))
            if live:
                kept.append(line)
        elif live and line[:1] in "+-" and not line.startswith(("+++", "---")):
            kept.append(line)
    return kept


def diff_key(lines: list[str]) -> str:
    """The sha256 that names this exact change to the mutable surface."""
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def decide_pr(surface: bool, ready: bool, held: bool, state: str, result: str) -> tuple[bool, str, str]:
    """`(pass, kind, reason)` for a pull_request head; `kind` names the record a pass leaves."""
    if not surface:
        return True, "", "no Rust, tests, Cargo or mutants.toml change: nothing to mutate"
    if held:
        return False, "", "no graded mutants verdict for this head: add the `ready` label to run the mutation shards"
    if not state:
        return False, "", "the mutation plan reached no state for this head: no verdict exists"
    if state == "docs-only":
        return True, "nothing-to-mutate", "the plan found no mutable changed line"
    if state == "over-budget-ungraded":
        if ready:
            return True, "ungraded", "over budget, accepted as UNGRADED by the `ready` label: mutation testing says nothing about this diff"
        return False, "", "over budget and ungraded: add the `ready` label to accept it as ungraded"
    if state != "in-budget":
        return False, "", f"the mutation plan ended '{state}', which is not a verdict"
    if result == "clean":
        return True, "graded", "graded clean"
    if result == "missed":
        return False, "", "missed mutants in code the offline suite executes"
    if not result:
        return False, "", "the grade recorded no verdict (killed mid-flight or timed out)"
    return False, "", f"the grade ended '{result}', which is not a verdict"


def decide_queue(surface: bool, key: str, recorded: str) -> tuple[bool, str]:
    """`(pass, reason)` for a merge_group head: the diff must carry a verdict recorded on its PR."""
    if not surface:
        return True, "no Rust, tests, Cargo or mutants.toml change: nothing to mutate"
    found = RECORD.match(recorded)
    if not found:
        return False, "no mutants verdict is recorded on the PR head: label the PR `ready` and let the Mutants jobs finish, then enqueue again"
    if found.group(2) != key:
        return False, "the verdict recorded on the PR head is for a different diff than this queue entry: label the final head `ready`"
    return True, f"the PR head carries a recorded '{found.group(1)}' verdict for this exact diff"


def gh_json(run: Run, path: str) -> dict:
    """One read of the GitHub API as JSON."""
    return json.loads(run(["gh", "api", path]))


def recorded_verdict(run: Run, repo: str, queue_ref: str) -> str:
    """The description of the `mutants/graded` status on the head of the PR this queue entry carries, or ''."""
    found = QUEUE_PR.search(queue_ref)
    if not found:
        raise RuntimeError(f"queue ref '{queue_ref}' names no PR")
    sha = gh_json(run, f"repos/{repo}/pulls/{found.group(1)}")["head"]["sha"]
    for status in gh_json(run, f"repos/{repo}/commits/{sha}/status")["statuses"]:
        if status["context"] == CONTEXT and status["state"] == "success":
            return status["description"] or ""
    return ""


def record(run: Run, repo: str, sha: str, kind: str, key: str) -> None:
    """Leave the verdict on the PR head, where a merge_group run of the same diff reads it."""
    run(["gh", "api", "-X", "POST", f"repos/{repo}/statuses/{sha}", "-f", "state=success", "-f", f"context={CONTEXT}", "-f", f"description={kind} {key}"])


def surface_of(run: Run, base: str) -> tuple[bool, str]:
    """`(has a mutable-surface change, its key)` for the diff against `base`."""
    lines = surface_diff(run(["git", "diff", "-U0", "--no-color", "--no-ext-diff", f"{base}...HEAD"]))
    return bool(lines), diff_key(lines)


def check(env: dict[str, str], run: Run, out=print) -> int:
    """Grade one head from the job's env: 0 pass, 1 refusal."""
    get = lambda k: env.get(k, "")  # noqa: E731
    event = get("EVENT")
    if event not in ("pull_request", "merge_group"):
        out(f"::error::mutants_gate: event '{event}' is neither pull_request nor merge_group, so nothing can be said about it")
        return 1
    try:
        surface, key = surface_of(run, get("BASE_SHA"))
    except subprocess.CalledProcessError as e:
        out(f"::error::mutants_gate: could not diff against '{get('BASE_SHA')}' ({e}), so the scope of this head is unknown: a refusal, not a pass")
        return 1
    if event == "merge_group":
        try:
            ok, reason = decide_queue(surface, key, recorded_verdict(run, get("REPO"), get("QUEUE_REF")) if surface else "")
        except (RuntimeError, KeyError, ValueError, subprocess.CalledProcessError) as e:
            out(f"::error::mutants_gate: could not read the recorded verdict ({e}), which is a refusal, not a pass")
            return 1
        out(f"{'ok' if ok else '::error::'} {reason}")
        return 0 if ok else 1
    ok, kind, reason = decide_pr(surface, get("READY") == "true", get("HELD") == "true", get("STATE"), get("RESULT"))
    if not ok:
        out(f"::error::{reason}")
        return 1
    out(f"ok: {reason}")
    if surface and kind:
        record(run, get("REPO"), get("PR_HEAD_SHA"), kind, key)
        out(f"recorded '{kind} {key}' as the {CONTEXT} status of {get('PR_HEAD_SHA')[:8]}")
    return 0


def shell(argv: list[str]) -> str:
    """Run one command and return its stdout; a failure raises."""
    return subprocess.run(argv, check=True, capture_output=True, text=True).stdout


def job_block(text: str, job: str) -> str:
    """The text of one top-level job of a workflow."""
    found = re.search(rf"^  {re.escape(job)}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)", text, re.M | re.S)
    return found.group(1) if found else ""


def workflow_failures(text: str) -> list[str]:
    """The properties of ci.yml that make the required check impossible to skip into a pass."""
    bad: list[str] = []
    req = job_block(text, "mutants-required")
    cond = re.search(r"^    if: (.*)$", req, re.M)
    if "name: Mutants (changed lines)" not in req:
        bad.append("ci.yml: mutants-required is not named 'Mutants (changed lines)', the required context")
    if not cond or cond.group(1).strip() != "always() && contains(fromJSON('[\"pull_request\", \"merge_group\"]'), github.event_name)":
        bad.append("ci.yml: mutants-required must run on every pull_request and merge_group event, whatever its needs did")
    if "mutants_gate.py check" not in req:
        bad.append("ci.yml: mutants-required never runs mutants_gate.py check")
    for job in ("mutants-shard", "mutants-in-diff"):
        if "held != 'true'" not in job_block(text, job):
            bad.append(f"ci.yml: {job} does not stop on a held head, so an unlabeled push would run the expensive jobs")
    return bad


def diff_of(files: dict[str, list[str]]) -> str:
    """A `git diff -U0` text over `files`, each changing the given lines."""
    parts = []
    for path, lines in files.items():
        parts.append(f"diff --git a/{path} b/{path}\nindex 111..222 100644\n--- a/{path}\n+++ b/{path}\n@@ -1,1 +1,1 @@\n" + "\n".join(lines))
    return "\n".join(parts)


def self_test() -> int:
    """Grade the decision, the key and the whole `check` flow against fixtures."""
    failures: list[str] = []

    def expect(name: str, got, want) -> None:
        if got != want:
            failures.append(f"{name}: got {got!r}, want {want!r}")

    src = diff_of({"src/lib.rs": ["-old", "+new"]})
    docs = diff_of({"docs/a.md": ["+x"], ".github/workflows/ci.yml": ["+y"]})
    expect("src is surface", bool(surface_diff(src)), True)
    expect("docs and workflow are not", surface_diff(docs), [])
    expect("tests fixture is surface", bool(surface_diff(diff_of({"tests/data/x.json": ["+1"]}))), True)
    expect("Cargo.lock is surface", bool(surface_diff(diff_of({"Cargo.lock": ["+1"]}))), True)
    expect("quoted path fails closed", bool(surface_diff('diff --git "a/s\\303.rs" "b/s\\303.rs"\n+x')), True)
    moved = src.replace("@@ -1,1 +1,1 @@", "@@ -40,1 +90,1 @@")
    expect("hunk position is not in the key", diff_key(surface_diff(moved)), diff_key(surface_diff(src)))
    expect("a changed line is", diff_key(surface_diff(src.replace("+new", "+newer"))) != diff_key(surface_diff(src)), True)

    pr = lambda **k: decide_pr(**{"surface": True, "ready": False, "held": False, "state": "in-budget", "result": "clean", **k})[0]  # noqa: E731
    expect("no surface passes unlabeled", pr(surface=False, held=True, state="", result=""), True)
    expect("unlabeled held head is refused", pr(held=True, result=""), False)
    expect("labeled clean passes", pr(ready=True), True)
    expect("unlabeled clean (cache replay) passes", pr(), True)
    expect("missed is refused", pr(ready=True, result="missed"), False)
    expect("timeout (no result) is refused", pr(ready=True, result=""), False)
    expect("tool-error is refused", pr(ready=True, result="tool-error"), False)
    expect("empty state is refused", pr(ready=True, state="", result=""), False)
    expect("unknown state is refused", pr(ready=True, state="scope-error", result="tool-error"), False)
    expect("over-budget unlabeled is refused", pr(state="over-budget-ungraded", result="n/a"), False)
    expect("over-budget labeled is accepted as ungraded", decide_pr(True, True, False, "over-budget-ungraded", "n/a")[:2], (True, "ungraded"))
    expect("plan docs-only passes", decide_pr(True, False, False, "docs-only", "n/a")[:2], (True, "nothing-to-mutate"))
    expect("a pass records only on a verdict", decide_pr(True, True, False, "in-budget", "clean")[1], "graded")

    key = "a" * 64
    expect("queue: no surface passes", decide_queue(False, key, "")[0], True)
    expect("queue: nothing recorded is refused", decide_queue(True, key, "")[0], False)
    expect("queue: other diff is refused", decide_queue(True, key, f"graded {'b' * 64}")[0], False)
    expect("queue: matching record passes", decide_queue(True, key, f"graded {key}")[0], True)
    expect("queue: a malformed record is refused", decide_queue(True, key, f"graded {key[:10]}")[0], False)

    calls: list[list[str]] = []
    statuses: dict[str, list[dict]] = {}

    def fake(diff: str) -> Run:
        def run(argv: list[str]) -> str:
            calls.append(argv)
            if argv[0] == "git":
                return diff
            if argv[:3] == ["gh", "api", "-X"]:
                sha = argv[4].rsplit("/", 1)[1]
                statuses.setdefault(sha, []).append({"context": CONTEXT, "state": "success", "description": argv[-1].split("=", 1)[1]})
                return "{}"
            if "/pulls/" in argv[2]:
                return json.dumps({"head": {"sha": "headsha"}})
            return json.dumps({"statuses": statuses.get(argv[2].split("/")[4], [])})
        return run

    base = {"BASE_SHA": "b", "REPO": "o/r", "PR_NUMBER": "7", "PR_HEAD_SHA": "headsha"}
    quiet = lambda *_: None  # noqa: E731
    pr_env = {**base, "EVENT": "pull_request", "STATE": "in-budget", "RESULT": "clean", "READY": "false", "HELD": "true"}
    expect("flow: unlabeled src PR is refused", check(pr_env, fake(src), quiet), 1)
    expect("flow: nothing recorded for it", statuses, {})
    expect("flow: workflow-only PR passes", check(pr_env, fake(docs), quiet), 0)
    queue_env = {**base, "EVENT": "merge_group", "QUEUE_REF": "refs/heads/gh-readonly-queue/main/pr-7-abc"}
    expect("flow: queue entry with no record is refused", check(queue_env, fake(src), quiet), 1)
    expect("flow: labeled clean PR passes", check({**pr_env, "READY": "true", "HELD": "false"}, fake(src), quiet), 0)
    expect("flow: and records the verdict", [s["description"].split()[0] for s in statuses["headsha"]], ["graded"])
    expect("flow: the queue then passes it", check(queue_env, fake(src), quiet), 0)
    expect("flow: a different diff in the queue is refused", check(queue_env, fake(diff_of({"src/lib.rs": ["+other"]})), quiet), 1)
    expect("flow: unknown event is refused", check({**base, "EVENT": "push"}, fake(src), quiet), 1)
    expect("flow: an unreadable diff is a refusal", check(pr_env, lambda argv: shell(["false"]), quiet), 1)

    def broken(argv: list[str]) -> str:
        if argv[0] == "git":
            return src
        raise subprocess.CalledProcessError(1, argv)

    expect("flow: an unreadable record is a refusal", check(queue_env, broken, quiet), 1)
    expect("flow: a queue ref without a PR is a refusal", check({**queue_env, "QUEUE_REF": "refs/heads/x"}, fake(src), quiet), 1)
    if WORKFLOW.exists():
        live = WORKFLOW.read_text()
        expect("workflow: a skippable required job is caught", bool(workflow_failures(live.replace("if: always() && contains(fromJSON", "if: github.event_name == 'pull_request' && contains(fromJSON"))), True)
        expect("workflow: an ungated shard is caught", bool(workflow_failures(live.replace(" && needs.mutants-plan.outputs.held != 'true'", ""))), True)
    failures.extend(workflow_failures(WORKFLOW.read_text() if WORKFLOW.exists() else ""))
    for line in failures:
        print(f"FAIL {line}", file=sys.stderr)
    print(f"mutants_gate self-test: {'FAILED' if failures else 'ok'}")
    return 1 if failures else 0


def main(argv: list[str]) -> int:
    """Entry point."""
    if argv == ["--self-test"]:
        return self_test()
    if argv == ["check"]:
        return check({k: os.environ.get(k, "") for k in ENV_KEYS}, shell)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
