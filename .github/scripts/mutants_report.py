#!/usr/bin/env python3
"""Publish what `Mutants (changed lines)` concluded and fail when the plan could not be made.

    mutants_report.py             read the job's env, append outputs to $GITHUB_OUTPUT, exit 1 on a plan error
    mutants_report.py --self-test
"""

from __future__ import annotations

import contextlib
import io
import os
import re
import sys
import tempfile
from pathlib import Path

CODE = "RIVET-CI-MUTANTS-PLAN-ERROR"
ENV_KEYS = ("GRADE_STATE", "GRADE_RESULT", "GRADE_OUTCOME", "PLAN_SCOPE", "PLAN_CODE", "PLAN_STATE", "PLAN_RESULT", "PLAN_COUNT")
EMPTY = "::notice::empty plan: the diff has no mutable changed lines, nothing was mutated."
REPORT_STEP = "Publish this run's verdict"


def judge(env: dict[str, str]) -> tuple[dict[str, str], list[str], bool]:
    """`(outputs, annotations, failed)` for one job's env; a plan that was not made is `failed`."""
    get = lambda k: env.get(k, "")  # noqa: E731
    outputs: dict[str, str] = {}
    notes: list[str] = []
    failed = False
    if get("GRADE_STATE"):
        failed = get("GRADE_RESULT") == "tool-error"
        if failed:
            notes.append(f"::error::{CODE}: the grade step ended in a tool error, which is not a verdict.")
        elif get("PLAN_COUNT") == "0":
            notes.append(EMPTY)
    elif get("PLAN_SCOPE") != "success":
        outputs.update(state="scope-error", result="tool-error")
        failed = True
        notes.append(f"::error::{CODE}: the plan job's scope step ended '{get('PLAN_SCOPE') or 'unset'}', so no diff was planned and nothing was graded.")
    elif get("PLAN_CODE") != "true":
        outputs.update(state="docs-only", result="n/a")
        notes.append("::notice::docs-only diff: no mutable changed lines, nothing was mutated.")
    elif get("PLAN_STATE"):
        outputs.update(state=get("PLAN_STATE"), result=get("PLAN_RESULT"))
        if get("PLAN_RESULT") == "tool-error":
            failed = True
            notes.append(f"::error::{CODE}: the plan ended '{get('PLAN_STATE')}' in a tool error (cargo-mutants or the diff broke), so nothing was graded.")
        elif get("PLAN_STATE") == "in-budget" and not re.fullmatch(r"\d+", get("PLAN_COUNT")):
            failed = True
            notes.append(f"::error::{CODE}: the plan is in-budget but published no mutant count ('{get('PLAN_COUNT')}'), so it is not a plan.")
        elif get("PLAN_STATE") == "in-budget" and get("PLAN_COUNT") == "0":
            notes.append(EMPTY)
    else:
        failed = True
        notes.append(f"::error::{CODE}: the plan job published no state for a code diff (the step died before planning: bad diff, cargo-mutants install or list failure), so nothing was graded.")
    outputs["outcome"] = get("GRADE_OUTCOME")
    return outputs, notes, failed


def main() -> int:
    """Judge the environment, write the outputs, print the annotations."""
    outputs, notes, failed = judge({k: os.environ.get(k, "") for k in ENV_KEYS})
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as fh:
        fh.writelines(f"{k}={v}\n" for k, v in outputs.items())
    print("\n".join(notes))
    return 1 if failed else 0


def self_test() -> int:
    """Grade every shape of the job's env, and that ci.yml's report step runs this script."""
    failures: list[str] = []
    base = dict(PLAN_SCOPE="success", PLAN_CODE="true")
    cases = [
        ("plan tool-error", dict(base, PLAN_STATE="tool-error", PLAN_RESULT="tool-error"), True, "tool error"),
        ("plan died before publishing", dict(base), True, "published no state"),
        ("scope step failed", dict(PLAN_SCOPE="failure"), True, "scope step"),
        ("scope output unset", dict(), True, "scope step"),
        ("in-budget without a count", dict(base, PLAN_STATE="in-budget", PLAN_COUNT=""), True, "no mutant count"),
        ("grade tool-error", dict(base, GRADE_STATE="in-budget", GRADE_RESULT="tool-error"), True, "grade step"),
        ("honest empty plan", dict(base, PLAN_STATE="in-budget", PLAN_COUNT="0", GRADE_STATE="in-budget", GRADE_RESULT="clean"), False, "empty plan"),
        ("honest empty plan, grade skipped", dict(base, PLAN_STATE="in-budget", PLAN_COUNT="0", PLAN_RESULT=""), False, "empty plan"),
        ("docs-only", dict(PLAN_SCOPE="success", PLAN_CODE="false"), False, "docs-only"),
        ("over budget", dict(base, PLAN_STATE="over-budget-ungraded", PLAN_RESULT="n/a", PLAN_COUNT="900"), False, None),
        ("graded clean", dict(base, PLAN_STATE="in-budget", PLAN_COUNT="12", GRADE_STATE="in-budget", GRADE_RESULT="missed"), False, None),
    ]
    for name, env, want_fail, want_text in cases:
        _, notes, failed = judge(env)
        text = "\n".join(notes)
        if failed != want_fail:
            failures.append(f"{name}: failed={failed}, want {want_fail}")
        if want_fail and CODE not in text:
            failures.append(f"{name}: a failure must carry {CODE}")
        if want_text and want_text not in text:
            failures.append(f"{name}: message lacks '{want_text}': {text!r}")
    outs, _, _ = judge(dict(PLAN_SCOPE="failure"))
    if outs.get("state") != "scope-error" or outs.get("result") != "tool-error":
        failures.append(f"scope failure must still publish scope-error/tool-error, got {outs}")
    with tempfile.NamedTemporaryFile("w+", suffix=".out") as out:
        os.environ["GITHUB_OUTPUT"] = out.name
        for k in ENV_KEYS:
            os.environ.pop(k, None)
        with contextlib.redirect_stdout(io.StringIO()):
            code = main()
        if code != 1 or "state=scope-error" not in Path(out.name).read_text():
            failures.append("main() on an empty env must exit 1 and publish scope-error")
    ci = (Path(__file__).resolve().parents[1] / "workflows" / "ci.yml").read_text()
    step = re.search(rf"- name: {re.escape(REPORT_STEP)}\n(.*?)(?=\n      - |\n  \S)", ci, re.S)
    if not step or "python3 .github/scripts/mutants_report.py" not in step.group(1):
        failures.append(f"ci.yml step '{REPORT_STEP}' must run mutants_report.py")
    elif re.search(r"\bexit 0\b", step.group(1)):
        failures.append(f"ci.yml step '{REPORT_STEP}' must not exit 0 by hand")
    if failures:
        print("\n".join(f"FAIL {f}" for f in failures), file=sys.stderr)
        return 1
    print(f"mutants_report.py self-test: ok ({len(cases)} cases)")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if sys.argv[1:] == ["--self-test"] else main())
