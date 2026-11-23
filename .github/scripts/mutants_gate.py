#!/usr/bin/env python3
"""Decide the required `Mutants (changed lines)` check: a head with Rust to mutate merges only on a recorded verdict.

    mutants_gate.py check          read the job's env, print the verdict, record it on the PR head, exit 1 on a refusal
    mutants_gate.py --self-test

The self-test is graded against mutants of this file and of ci.yml (see the PR that introduced it).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable

CONTEXT = "mutants/graded"
WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "ci.yml"
SURFACE = re.compile(r"(\.rs$|^tests/|^Cargo\.(toml|lock)$|^\.cargo/)")
HEADER = re.compile(r"^diff --git a/(.*) b/(.*)$")
POSITION = re.compile(r"^(@@ |index )")
RECORD = re.compile(r"^(graded|ungraded|nothing-to-mutate) ([0-9a-f]{64})$")
QUEUE_PR = re.compile(r"/pr-(\d+)-")
ENV_KEYS = ("EVENT", "BASE_SHA", "REPO", "PR_HEAD_SHA", "QUEUE_REF", "READY", "HELD", "STATE", "RESULT", "OUTCOME")
RECORDER = "github-actions[bot]"
Run = Callable[[list[str]], str]


def surface_diff(diff: str) -> list[str]:
    """Every line of the files that can change a mutation verdict, but the hunk positions and blob ids."""
    kept: list[str] = []
    live = False
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            head = HEADER.match(line)
            live = not head or bool(SURFACE.search(head.group(2)))
        if live and not POSITION.match(line):
            kept.append(line)
    return kept


def diff_key(lines: list[str]) -> str:
    """The sha256 that names this exact change to the mutable surface."""
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def decide_pr(surface: bool, ready: bool, held: bool, state: str, result: str, outcome: str) -> tuple[bool, str, str]:
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
        if outcome != "success":
            return False, "", f"the grade published 'clean' but its step ended '{outcome or 'unset'}': it did not finish"
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


def gh_json(run: Run, path: str):
    """One read of the GitHub API as JSON."""
    return json.loads(run(["gh", "api", path]))


def recorded_verdict(run: Run, repo: str, queue_ref: str) -> str:
    """The newest `mutants/graded` success the workflow's own token left on the head of the PR this queue entry carries, or ''."""
    found = QUEUE_PR.search(queue_ref)
    if not found:
        raise RuntimeError(f"queue ref '{queue_ref}' names no PR")
    sha = gh_json(run, f"repos/{repo}/pulls/{found.group(1)}")["head"]["sha"]
    for status in gh_json(run, f"repos/{repo}/commits/{sha}/statuses?per_page=100"):
        if status["context"] == CONTEXT and status["state"] == "success" and (status.get("creator") or {}).get("login") == RECORDER:
            return status["description"] or ""
    return ""


def record(run: Run, repo: str, sha: str, kind: str, key: str) -> None:
    """Leave the verdict on the PR head, where a merge_group run of the same diff reads it."""
    run(["gh", "api", "-X", "POST", f"repos/{repo}/statuses/{sha}", "-f", "state=success", "-f", f"context={CONTEXT}", "-f", f"description={kind} {key}"])


def surface_of(run: Run, base: str) -> tuple[bool, str]:
    """`(has a mutable-surface change, its key)` for the diff against `base`; a base that is not a commit is an error."""
    if not base:
        raise ValueError("no base commit was given, so an empty diff would read as nothing to mutate")
    run(["git", "rev-parse", "--verify", "--quiet", f"{base}^{{commit}}"])
    lines = surface_diff(run(["git", "diff", "--binary", "--no-renames", "-U0", "--no-color", "--no-ext-diff", f"{base}...HEAD"]))
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
    except (ValueError, subprocess.CalledProcessError) as e:
        out(f"::error::mutants_gate: could not diff against '{get('BASE_SHA')}' ({e}), so the scope of this head is unknown: a refusal, not a pass")
        return 1
    if event == "merge_group":
        try:
            ok, reason = decide_queue(surface, key, recorded_verdict(run, get("REPO"), get("QUEUE_REF")) if surface else "")
        except (RuntimeError, KeyError, ValueError, subprocess.CalledProcessError) as e:
            out(f"::error::mutants_gate: could not read the recorded verdict ({e}), which is a refusal, not a pass")
            return 1
        out(f"ok: {reason}" if ok else f"::error::{reason}")
        return 0 if ok else 1
    ok, kind, reason = decide_pr(surface, get("READY") == "true", get("HELD") == "true", get("STATE"), get("RESULT"), get("OUTCOME"))
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


REQUIRED_IF = "always() && contains(fromJSON('[\"pull_request\", \"merge_group\"]'), github.event_name)"
CHECK_LINE = "python3 .github/scripts/mutants_gate.py check"
BASE_EXPR = "${{ github.event.pull_request.base.sha || github.event.merge_group.base_sha }}"
CHECK_ENV = {
    "GH_TOKEN": "${{ github.token }}",
    "EVENT": "${{ github.event_name }}",
    "BASE_SHA": BASE_EXPR,
    "REPO": "${{ github.repository }}",
    "PR_HEAD_SHA": "${{ github.event.pull_request.head.sha }}",
    "QUEUE_REF": "${{ github.event.merge_group.head_ref }}",
    "READY": "${{ contains(github.event.pull_request.labels.*.name, env.MUTANTS_READY_LABEL) }}",
    "HELD": "${{ needs.mutants-plan.outputs.held }}",
    "STATE": "${{ needs.mutants-in-diff.outputs.state }}",
    "RESULT": "${{ needs.mutants-in-diff.outputs.result }}",
    "OUTCOME": "${{ needs.mutants-in-diff.outputs.outcome }}",
}
GRADE_OUTPUTS = {
    "state": "${{ steps.grade.outputs.state || steps.report.outputs.state }}",
    "result": "${{ steps.grade.outputs.result || steps.report.outputs.result }}",
    "outcome": "${{ steps.report.outputs.outcome }}",
}
PUBLISH_LINE = 'if [ -n "$VERDICT" ]; then say result "$VERDICT"; fi'


def required_job_failures(jobs: dict) -> list[str]:
    """What keeps `mutants-required` from being skipped, masked or fed a value the gate did not compute."""
    bad: list[str] = []
    req = jobs.get("mutants-required") or {}
    steps = req.get("steps") or []
    checks = [s for s in steps if CHECK_LINE in str(s.get("run", ""))]
    if req.get("name") != "Mutants (changed lines)":
        bad.append("ci.yml: mutants-required is not named 'Mutants (changed lines)', the required context")
    if str(req.get("if", "")).strip() != REQUIRED_IF:
        bad.append("ci.yml: mutants-required must run on every pull_request and merge_group event, whatever its needs did")
    if "continue-on-error" in req:
        bad.append("ci.yml: mutants-required must not be continue-on-error")
    if len(checks) != 1:
        return bad + ["ci.yml: mutants-required must run mutants_gate.py check in exactly one step"]
    step = checks[0]
    lines = [x.strip() for x in str(step["run"]).splitlines() if x.strip()]
    if CHECK_LINE not in lines or any(x.startswith(("exit", "set +e")) for x in lines):
        bad.append("ci.yml: the mutants_gate.py check step must run the bare command, with nothing that can turn its exit into a pass")
    if "if" in step or "continue-on-error" in step or "shell" in step:
        bad.append("ci.yml: the mutants_gate.py check step must have no `if`, `continue-on-error` or `shell`")
    if steps[-1] is not step:
        bad.append("ci.yml: the mutants_gate.py check must be the last step of mutants-required")
    env = step.get("env") or {}
    bad += [f"ci.yml: the check step's {k} must be `{want}`, it is `{env.get(k)}`" for k, want in CHECK_ENV.items() if env.get(k) != want]
    bad += [f"ci.yml: the check step passes {k}, which the gate does not read" for k in env if k not in CHECK_ENV]
    return bad


def grade_failures(jobs: dict) -> list[str]:
    """The grade job publishes its verdict last and holds the shards on a held head."""
    bad: list[str] = []
    for job in ("mutants-shard", "mutants-in-diff"):
        if "held != 'true'" not in str((jobs.get(job) or {}).get("if", "")):
            bad.append(f"ci.yml: {job} does not stop on a held head, so an unlabeled push would run the expensive jobs")
    grade = jobs.get("mutants-in-diff") or {}
    outputs = grade.get("outputs") or {}
    bad += [f"ci.yml: mutants-in-diff output {k} must be `{want}`, it is `{outputs.get(k)}`" for k, want in GRADE_OUTPUTS.items() if outputs.get(k) != want]
    runs = [s for s in grade.get("steps") or [] if s.get("id") == "grade"]
    lines = [x.strip() for x in str(runs[0].get("run", "")).splitlines() if x.strip()] if runs else []
    says = [n for n, x in enumerate(lines) if "say result" in x and "tool-error" not in x]
    if len(says) != 1 or lines[says[0]] != PUBLISH_LINE or lines[-1] != 'exit "$rc"' or any(x.startswith("say ") for x in lines[says[0] + 1:]):
        bad.append("ci.yml: the grade step must publish `result` once, as its last `say`, right before `exit \"$rc\"`, so a crash earlier leaves no verdict")
    return bad


def workflow_failures(text: str) -> list[str]:
    """The properties of ci.yml that make the required check impossible to skip into a pass."""
    import yaml

    jobs = (yaml.safe_load(text) or {}).get("jobs") or {}
    return required_job_failures(jobs) + grade_failures(jobs)


def diff_of(files: dict[str, list[str]]) -> str:
    """A `git diff -U0` text over `files`, each changing the given lines."""
    parts = []
    for path, lines in files.items():
        parts.append(f"diff --git a/{path} b/{path}\nindex 111..222 100644\n--- a/{path}\n+++ b/{path}\n@@ -1,1 +1,1 @@\n" + "\n".join(lines))
    return "\n".join(parts)


class Grader:
    """Collects the failures of one self-test run."""

    def __init__(self) -> None:
        self.failures: list[str] = []

    def expect(self, name: str, got, want) -> None:
        """Record a failure when `got` is not `want`."""
        if got != want:
            self.failures.append(f"{name}: got {got!r}, want {want!r}")


def grade_key(g: Grader) -> None:
    """The key names the mutable surface, text and binary, and nothing else."""
    src = diff_of({"src/lib.rs": ["-old", "+new"]})
    g.expect("src is surface", bool(surface_diff(src)), True)
    g.expect("docs and workflow are not", surface_diff(diff_of({"docs/a.md": ["+x"], ".github/workflows/ci.yml": ["+y"]})), [])
    g.expect("tests fixture is surface", bool(surface_diff(diff_of({"tests/data/x.json": ["+1"]}))), True)
    g.expect("Cargo.lock is surface", bool(surface_diff(diff_of({"Cargo.lock": ["+1"]}))), True)
    g.expect("quoted path fails closed", bool(surface_diff('diff --git "a/s\\303.rs" "b/s\\303.rs"\n+x')), True)
    moved = src.replace("@@ -1,1 +1,1 @@", "@@ -40,1 +90,1 @@").replace("index 111..222", "index 333..444")
    g.expect("hunk position and blob ids are not in the key", diff_key(surface_diff(moved)), diff_key(surface_diff(src)))
    g.expect("a changed line is", diff_key(surface_diff(src.replace("+new", "+newer"))) != diff_key(surface_diff(src)), True)
    g.expect("a removed `-- x` line is in the key", diff_key(surface_diff(diff_of({"src/a.rs": ["--- x"]}))) != diff_key(surface_diff(diff_of({"src/a.rs": []}))), True)
    g.expect("an added `++ x` line is in the key", diff_key(surface_diff(diff_of({"src/a.rs": ["+++ x"]}))) != diff_key(surface_diff(diff_of({"src/a.rs": []}))), True)
    grade_git(g)


def grade_git(g: Grader) -> None:
    """Binary files and dash-led lines reach the key through a real `git diff`."""
    with tempfile.TemporaryDirectory() as tmp:
        git = lambda *a: shell(["git", "-C", tmp, "-c", "user.name=t", "-c", "user.email=t@t", *a])  # noqa: E731
        git("init", "-q", "-b", "main")
        for name, body in {"tests/a.bin": b"\x00\x01\x02", "src/a.rs": b"keep\n-- old\n"}.items():
            (Path(tmp) / name).parent.mkdir(exist_ok=True)
            (Path(tmp) / name).write_bytes(body)
        git("add", "-A")
        git("commit", "-q", "-m", "base")
        keys = []
        for binary, text in ((b"\x00\x01\x09", b"keep\n-- old\n"), (b"\x00\x01\x0a", b"keep\n-- old\n"), (b"\x00\x01\x02", b"keep\n")):
            git("checkout", "-q", "-B", "work", "main")
            (Path(tmp) / "tests/a.bin").write_bytes(binary)
            (Path(tmp) / "src/a.rs").write_bytes(text)
            git("commit", "-q", "-a", "-m", "work")
            keys.append(surface_of(lambda argv: shell(["git", "-C", tmp, *argv[1:]]), "main"))
    g.expect("git: a binary change is a surface change", keys[0][0], True)
    g.expect("git: two binary contents have two keys", keys[0][1] != keys[1][1], True)
    g.expect("git: a deleted `-- old` line is a surface change", keys[2][0], True)


def grade_decide(g: Grader) -> None:
    """The pull_request and merge_group decisions."""
    pr = lambda **k: decide_pr(**{"surface": True, "ready": False, "held": False, "state": "in-budget", "result": "clean", "outcome": "success", **k})[0]  # noqa: E731
    g.expect("no surface passes unlabeled", pr(surface=False, held=True, state="", result=""), True)
    g.expect("unlabeled held head is refused", pr(held=True, result=""), False)
    g.expect("labeled clean passes", pr(ready=True), True)
    g.expect("unlabeled clean (cache replay) passes", pr(), True)
    g.expect("a clean the grade step did not finish is refused", pr(outcome="failure"), False)
    g.expect("a clean with no step outcome is refused", pr(outcome=""), False)
    g.expect("missed is refused", pr(ready=True, result="missed", outcome="failure"), False)
    g.expect("timeout (no result) is refused", pr(ready=True, result=""), False)
    g.expect("tool-error is refused", pr(ready=True, result="tool-error"), False)
    g.expect("empty state is refused", pr(ready=True, state="", result=""), False)
    g.expect("unknown state is refused", pr(ready=True, state="scope-error", result="tool-error"), False)
    g.expect("over-budget unlabeled is refused", pr(state="over-budget-ungraded", result="n/a"), False)
    g.expect("over-budget labeled is accepted as ungraded", decide_pr(True, True, False, "over-budget-ungraded", "n/a", "")[:2], (True, "ungraded"))
    g.expect("plan docs-only passes", decide_pr(True, False, False, "docs-only", "n/a", "")[:2], (True, "nothing-to-mutate"))
    g.expect("a pass records only on a verdict", decide_pr(True, True, False, "in-budget", "clean", "success")[1], "graded")
    key = "a" * 64
    g.expect("queue: no surface passes", decide_queue(False, key, "")[0], True)
    g.expect("queue: nothing recorded is refused", decide_queue(True, key, "")[0], False)
    g.expect("queue: other diff is refused", decide_queue(True, key, f"graded {'b' * 64}")[0], False)
    g.expect("queue: matching record passes", decide_queue(True, key, f"graded {key}")[0], True)
    g.expect("queue: a malformed record is refused", decide_queue(True, key, f"graded {key[:10]}")[0], False)


def grade_flow(g: Grader) -> None:
    """The whole `check` against a fake git and a fake GitHub."""
    src = diff_of({"src/lib.rs": ["-old", "+new"]})
    docs = diff_of({"docs/a.md": ["+x"], ".github/workflows/ci.yml": ["+y"]})
    statuses: dict[str, list[dict]] = {}

    def post(sha: str, description: str, creator: str = RECORDER, context: str = CONTEXT, state: str = "success") -> None:
        statuses.setdefault(sha, []).insert(0, {"context": context, "state": state, "description": description, "creator": {"login": creator}})

    def fake(diff: str) -> Run:
        def run(argv: list[str]) -> str:
            if argv[:2] == ["git", "rev-parse"] and argv[-1].startswith("missing"):
                raise subprocess.CalledProcessError(128, argv)
            if argv[0] == "git":
                return diff
            if argv[:3] == ["gh", "api", "-X"]:
                post(argv[4].rsplit("/", 1)[1], argv[-1].split("=", 1)[1])
                return "{}"
            if "/pulls/" in argv[2]:
                return json.dumps({"head": {"sha": "headsha"}})
            return json.dumps(statuses.get(argv[2].split("/")[4].split("?")[0], []))
        return run

    quiet = lambda *_: None  # noqa: E731
    base = {"BASE_SHA": "b", "REPO": "o/r", "PR_HEAD_SHA": "headsha"}
    pr_env = {**base, "EVENT": "pull_request", "STATE": "in-budget", "RESULT": "clean", "OUTCOME": "success", "READY": "false", "HELD": "true"}
    queue_env = {**base, "EVENT": "merge_group", "QUEUE_REF": "refs/heads/gh-readonly-queue/main/pr-7-abc"}
    g.expect("flow: unlabeled src PR is refused", check(pr_env, fake(src), quiet), 1)
    for event_env in (pr_env, queue_env):
        for what, base_sha in (("an empty base", ""), ("a base that is no commit", "missing1")):
            g.expect(f"flow: {event_env['EVENT']} with {what} is refused, not read as nothing to mutate", check({**event_env, "BASE_SHA": base_sha}, fake(docs), quiet), 1)
    g.expect("flow: workflow-only PR passes", check(pr_env, fake(docs), quiet), 0)
    g.expect("flow: queue entry with no record is refused", check(queue_env, fake(src), quiet), 1)
    over = {**pr_env, "STATE": "over-budget-ungraded", "RESULT": "n/a", "HELD": "false"}
    g.expect("flow: over budget with READY=false is refused", check({**over, "READY": "false"}, fake(src), quiet), 1)
    g.expect("flow: over budget with READY=true is accepted", check({**over, "READY": "true"}, fake(src), quiet), 0)
    g.expect("flow: and recorded as ungraded", [s["description"].split()[0] for s in statuses["headsha"]], ["ungraded"])
    statuses.clear()
    g.expect("flow: labeled clean PR passes", check({**pr_env, "READY": "true", "HELD": "false"}, fake(src), quiet), 0)
    g.expect("flow: and records the verdict", [s["description"].split()[0] for s in statuses["headsha"]], ["graded"])
    g.expect("flow: the queue then passes it", check(queue_env, fake(src), quiet), 0)
    g.expect("flow: a different diff in the queue is refused", check(queue_env, fake(diff_of({"src/lib.rs": ["+other"]})), quiet), 1)
    good = statuses["headsha"][0]["description"]
    for name, forged in {
        "a status by another login": dict(creator="mallory"),
        "a status with another context": dict(context="other/context"),
        "a pending status": dict(state="pending"),
        "a failure status": dict(state="failure"),
    }.items():
        statuses.clear()
        post("headsha", good, **forged)
        g.expect(f"flow: {name} is not a record", check(queue_env, fake(src), quiet), 1)
    statuses.clear()
    post("headsha", good)
    post("headsha", good, creator="mallory")
    g.expect("flow: a forged status over a real one does not hide it", check(queue_env, fake(src), quiet), 0)
    g.expect("flow: unknown event is refused", check({**base, "EVENT": "push"}, fake(src), quiet), 1)
    g.expect("flow: an unreadable diff is a refusal", check(pr_env, lambda argv: shell(["false"]), quiet), 1)

    def broken(argv: list[str]) -> str:
        if argv[0] == "git":
            return src
        raise subprocess.CalledProcessError(1, argv)

    g.expect("flow: an unreadable record is a refusal", check(queue_env, broken, quiet), 1)
    g.expect("flow: a queue ref without a PR is a refusal", check({**queue_env, "QUEUE_REF": "refs/heads/x"}, fake(src), quiet), 1)


def grade_workflow(g: Grader) -> None:
    """ci.yml keeps the required check unskippable, and the mutants of it are caught."""
    live = WORKFLOW.read_text() if WORKFLOW.exists() else ""
    g.failures.extend(workflow_failures(live))
    if not live:
        return
    run_line = "        run: python3 .github/scripts/mutants_gate.py check"
    base_old = "github.event.pull_request.base.sha || github.event.merge_group.base_sha"
    mutants = {
        "a typo in the base expression": (base_old, base_old.replace("base.sha", "base.shaa")),
        "READY forced true": ("          READY: ${{ contains(github.event.pull_request.labels.*.name, env.MUTANTS_READY_LABEL) }}", "          READY: 'true'"),
        "OUTCOME forced success": ("          OUTCOME: ${{ needs.mutants-in-diff.outputs.outcome }}", "          OUTCOME: success"),
        "the grade output outcome forced success": ("      outcome: ${{ steps.report.outputs.outcome }}", "      outcome: success"),
        "the queue ref from head_sha": ("QUEUE_REF: ${{ github.event.merge_group.head_ref }}", "QUEUE_REF: ${{ github.event.merge_group.head_sha }}"),
        "result published before the audit": ("            0) VERDICT=clean ;;", "            0) say result clean ;;"),
        "an extra say after the result": ('          exit "$rc"\n      - name: Save mutation outcomes', '          say p2_audit late\n          exit "$rc"\n      - name: Save mutation outcomes'),
        "a skippable required job": ("if: always() && contains(fromJSON", "if: github.event_name == 'pull_request' && contains(fromJSON"),
        "an ungated shard": (" && needs.mutants-plan.outputs.held != 'true'", ""),
        "continue-on-error on the job": ("    name: Mutants (changed lines)\n", "    name: Mutants (changed lines)\n    continue-on-error: true\n"),
        "`exit 0` after the check": (run_line, "        run: |\n          python3 .github/scripts/mutants_gate.py check\n          exit 0"),
        "`|| true` on the check": (run_line, run_line + " || true"),
        "`if: false` on the check step": (run_line, run_line + "\n        if: false"),
        "continue-on-error on the check step": (run_line, run_line + "\n        continue-on-error: true"),
        "a renamed required context": ("    name: Mutants (changed lines)\n    runs-on: ubuntu-latest\n    needs: [mutants-plan, mutants-in-diff]", "    name: Mutants (changed)\n    runs-on: ubuntu-latest\n    needs: [mutants-plan, mutants-in-diff]"),
    }
    for name, (old, new) in mutants.items():
        g.expect(f"workflow: {name} is caught", old in live and bool(workflow_failures(new.join(live.rsplit(old, 1)))), True)


def self_test() -> int:
    """Grade the key, the decisions, the whole `check` flow and ci.yml."""
    g = Grader()
    for grade in (grade_key, grade_decide, grade_flow, grade_workflow):
        grade(g)
    for line in g.failures:
        print(f"FAIL {line}", file=sys.stderr)
    print(f"mutants_gate self-test: {'FAILED' if g.failures else 'ok'}")
    return 1 if g.failures else 0


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
