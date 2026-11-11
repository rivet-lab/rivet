"""Deterministic FIX round: carry accepted bug-hunt classes, each to a verified PR.

    python -m dev.pytools.fix --hunt OUT_DIR --classes C1,C3 [--parallel 2] [--budget-usd N] [--go] [--resume]
                              [--only STEP] [--force-class ID] [--record-closed]
    python -m dev.pytools.fix --self-test

Without --go the plan is printed and nothing runs. Per class, in order: choose (deepener or bug-fixer), plan (the
fixer stops at PLAN.md), plan-check (rivet-verifier), implement (a fresh fixer, up to two checkpoint continuations),
runner (rivet-runner on the fixer's list), seed (rivet-seed-keeper when the fixer reported no seed rows), review
(rivet-verifier on the PR; one rework round by a fresh fixer, then needs-human), close (prints the PR and the hunt
close command; never merges). Every step writes a file under target/fix/<hunt>/<class>/ and is skipped on --resume.
--budget-usd is per class.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import datetime as dt
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GIT = "/usr/bin/git"
CARGO = str(Path.home() / ".cargo" / "bin" / "cargo")
GATE_WAIT = 'until ! /bin/ps -axo command | /usr/bin/grep -E "[d]ev\\.release_oracle( |$)" | /usr/bin/grep -qv -- "--self-test"; do sleep 60; done'
RULES_PATH = Path.home() / ".claude" / "rivet-process-rules.md"
SCRATCH_ROOT = Path("/private/tmp/claude-501/-Users-andriipanchenko-rivet")
STEPS = ("choose", "plan", "plan-check", "implement", "runner", "seed", "review", "close")
RUN_STEPS = ("plan", "plan-check", "implement", "runner", "seed", "review", "rework")
SHARES = {"fixer": 0.45, "plan-check": 0.05, "runner": 0.15, "seed": 0.05, "review": 0.20, "rework": 0.10}
POOL = {"plan": "fixer", "implement": "fixer", "plan-check": "plan-check", "runner": "runner", "seed": "seed", "review": "review", "rework": "rework"}
FIRST_SHARE = {"plan": 0.3, "review": 0.5}
MIN_RUN_USD = 0.05
MAX_CONTINUATIONS = 2
TIMEOUT_S = {"plan": 5400, "plan-check": 3600, "implement": 14400, "runner": 14400, "seed": 7200, "review": 7200, "rework": 14400}
USAGE_KEYS = ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
PERMISSION_MODE = "dontAsk"
VERDICTS = ("mergeable", "mergeable-after-fixes", "not-mergeable")
PR_URL = re.compile(r"^https://github\.com/panchenkoai/rivet/pull/(\d+)$")
HEX = re.compile(r"^[0-9a-f]{7,40}$")
CYRILLIC = re.compile("[\u0400-\u04ff]")
GH_READ = ("pull_request_read", "list_pull_requests", "search_pull_requests", "issue_read", "list_issues", "search_issues",
           "actions_list", "actions_get", "get_job_logs", "get_commit", "list_commits", "list_branches", "get_me")
GH_WRITE = ("create_pull_request", "update_pull_request", "update_pull_request_branch", "add_issue_comment", "issue_write")
GH_DENY = ("merge_pull_request", "push_files", "create_or_update_file", "delete_file", "delete_repository", "create_repository", "fork_repository")
IMPL_SCHEMA = ('{"pr_url": "https://github.com/panchenkoai/rivet/pull/N", "branch": "BRANCH", "head_sha": "40-hex", "red_line": "...", '
               '"green_line": "...", "engines_table": [{"engine": "postgres", "status": "same defect, fixed"}], '
               '"for_runner": [{"cmd": "exact command", "expect": "expected verdict line"}], "ratchets_raised": [], '
               '"not_verified": [], "second_defects": [], "seed_rows": ["seed x engine: CAUGHT"]}')
PLAN_SCHEMA = ('{"plan_path": "PATH", "reproduced": true, "red_test": "test name and file", "red_line": "the RED line on origin/main", '
               '"seam": "file:line of the shared seam", "fix_intent": "one paragraph", "engines": ["postgres", "mysql"]}')
CHECK_SCHEMA = '{"holds": true, "reasons": ["what you opened or ran, quoted"]}'
RUNNER_SCHEMA = '{"results": [{"cmd": "the command as given", "verdict_line": "the line you read", "pass": true}]}'
SEED_SCHEMA = '{"recall_lines": ["the recall table lines exactly as printed"], "missed": [], "product_defects": []}'
REVIEW_SCHEMA = '{"verdict": "mergeable|mergeable-after-fixes|not-mergeable", "head_sha": "40-hex", "findings": [{"sev": "high", "input": "...", "output": "..."}]}'


class FixError(Exception):
    """A fix round that cannot start or go on."""


class Stop(Exception):
    """A class ends here with a status and its reason."""

    def __init__(self, status: str, reason: str):
        """Keep the status and the reason."""
        super().__init__(f"{status}: {reason}")
        self.status, self.reason = status, reason


def slug(text: str) -> str:
    """A short file-name and branch form of a text."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:40].strip("-") or "class"


def norm(text: str) -> str:
    """Text compared without case, spacing or quote style."""
    return re.sub(r"\s+", " ", text.replace("\u2019", "'").replace("`", "")).strip().lower()


def english(text: str, ref: str) -> str:
    """The text, or a pointer to it when it is not English."""
    return f"(not in English; read it at {ref} and work in English)" if CYRILLIC.search(text or "") else text


def json_block(text: str) -> tuple[object, str | None]:
    """The fenced ```json block that ends the text, parsed, or None and why."""
    blocks = list(re.finditer(r"```json\s*\n(.*?)\n\s*```", text, re.S))
    if not blocks:
        return None, "no fenced ```json block"
    last = blocks[-1]
    if text[last.end():].strip():
        return None, "text follows the last ```json block; the reply must end with it"
    try:
        return json.loads(last.group(1)), None
    except json.JSONDecodeError as e:
        return None, f"the ```json block is not JSON: {e}"


def parse_rules(text: str) -> dict[str, tuple[str, str]]:
    """Normalized title -> (title, body) for every `## ` section of the process-rules file."""
    parts = re.split(r"^## (.+)$", text, flags=re.M)
    return {norm(t): (t.strip(), b.strip()) for t, b in zip(parts[1::2], parts[2::2])}


def candidates(text: str) -> list[dict]:
    """The `## ` sections of candidates.md that end in a KEY CLAIM: lines, claim, Strong grade, deepening shape."""
    lines = text.splitlines()
    heads = [i for i, x in enumerate(lines) if x.startswith("## ")] + [len(lines)]
    out = []
    for a, b in zip(heads, heads[1:]):
        sec = "\n".join(lines[a:b])
        m = re.search(r"KEY CLAIM\W*?[:\-\u2013\u2014]\s*(.+)", sec)
        if not m:
            continue
        out.append({"start": a + 1, "end": b, "text": sec.strip(), "claim": m.group(1).strip().strip("*_ ").strip(),
                    "strong": bool(re.search(r"^\*\*[^*\n]{1,40}\*\*:?\s*Strong\b", sec, re.M)),
                    "deepening": bool(re.search(r"\bmodule\b", sec, re.I) and re.search(r"deletable|deletion test|deepen|locality|leverage", sec, re.I))})
    return out


def validate_plan(obj: object, plan_path: Path) -> list[str]:
    """What is wrong with a plan-stage block."""
    if not isinstance(obj, dict):
        return ["the block is not an object"]
    errs = [f"`{k}` is missing or not a string" for k in ("plan_path", "red_test", "red_line", "seam", "fix_intent") if not isinstance(obj.get(k), str)]
    if not isinstance(obj.get("reproduced"), bool):
        errs.append("`reproduced` is not true or false")
    if not isinstance(obj.get("engines"), list) or not all(isinstance(e, str) for e in obj.get("engines") or [None]):
        errs.append("`engines` is not a non-empty list of strings")
    if obj.get("plan_path") != str(plan_path) or not plan_path.is_file():
        errs.append(f"`plan_path` must be {plan_path} and the file must exist")
    return errs


def validate_check(obj: object) -> list[str]:
    """What is wrong with a plan-check block."""
    if not isinstance(obj, dict) or not isinstance(obj.get("holds"), bool):
        return ["`holds` is missing or not true or false"]
    return [] if isinstance(obj.get("reasons"), list) and obj["reasons"] and all(isinstance(r, str) for r in obj["reasons"]) else ["`reasons` is not a non-empty list of strings"]


def validate_impl(obj: object, branch: str, pr_url: str | None = None) -> list[str]:
    """What is wrong with an implement or rework block."""
    if not isinstance(obj, dict):
        return ["the block is not an object"]
    errs = []
    if not isinstance(obj.get("pr_url"), str) or not PR_URL.match(obj["pr_url"]):
        errs.append("`pr_url` is not https://github.com/panchenkoai/rivet/pull/N")
    elif pr_url and obj["pr_url"] != pr_url:
        errs.append(f"`pr_url` must stay {pr_url}")
    if obj.get("branch") != branch:
        errs.append(f"`branch` must be {branch}")
    if not isinstance(obj.get("head_sha"), str) or not HEX.match(obj["head_sha"]):
        errs.append("`head_sha` is not a hex commit sha")
    errs += [f"`{k}` is empty" for k in ("red_line", "green_line") if not isinstance(obj.get(k), str) or not obj[k].strip()]
    rows = obj.get("engines_table")
    if not isinstance(rows, list) or not rows or not all(isinstance(r, dict) and isinstance(r.get("engine"), str) and isinstance(r.get("status"), str) for r in rows):
        errs.append("`engines_table` is not a non-empty list of {engine, status}")
    runs = obj.get("for_runner")
    if not isinstance(runs, list) or not all(isinstance(r, dict) and isinstance(r.get("cmd"), str) and r["cmd"].strip() and isinstance(r.get("expect"), str) for r in runs):
        errs.append("`for_runner` is not a list of {cmd, expect}")
    errs += [f"`{k}` is not a list" for k in ("ratchets_raised", "not_verified", "second_defects") if not isinstance(obj.get(k), list)]
    if "seed_rows" in obj and not isinstance(obj["seed_rows"], list):
        errs.append("`seed_rows` is not a list")
    return errs


def validate_runner(obj: object, cmds: list[str]) -> list[str]:
    """What is wrong with a runner block: one result per command, in order."""
    rows = obj.get("results") if isinstance(obj, dict) else None
    if not isinstance(rows, list):
        return ["`results` is missing or not a list"]
    if [r.get("cmd") if isinstance(r, dict) else None for r in rows] != cmds:
        return [f"`results` must hold one row per command, in the given order ({len(cmds)} rows)"]
    return [f"results[{i}] needs a verdict_line string and a boolean pass" for i, r in enumerate(rows) if not isinstance(r.get("verdict_line"), str) or not isinstance(r.get("pass"), bool)]


def validate_seed(obj: object) -> list[str]:
    """What is wrong with a seed-keeper block."""
    lines = obj.get("recall_lines") if isinstance(obj, dict) else None
    return [] if isinstance(lines, list) and lines and all(isinstance(x, str) for x in lines) else ["`recall_lines` is not a non-empty list of strings"]


def same_sha(a: str, b: str) -> bool:
    """Two shas name the same commit (one may be abbreviated)."""
    return bool(a and b and HEX.match(a) and HEX.match(b) and (a.startswith(b) or b.startswith(a)))


def validate_review(obj: object, head: str) -> list[str]:
    """What is wrong with a review block; it must name the head it reviewed."""
    if not isinstance(obj, dict):
        return ["the block is not an object"]
    errs = [] if obj.get("verdict") in VERDICTS else [f"`verdict` is not one of {'|'.join(VERDICTS)}"]
    if not same_sha(str(obj.get("head_sha") or ""), head):
        errs.append(f"`head_sha` must be the PR head {head}")
    rows = obj.get("findings")
    if not isinstance(rows, list) or not all(isinstance(r, dict) and all(isinstance(r.get(k), str) for k in ("sev", "input", "output")) for r in rows):
        errs.append("`findings` is not a list of {sev, input, output}")
    return errs


def budgets(per_class: float) -> dict[str, float]:
    """The per-class pools of --budget-usd."""
    return {k: round(per_class * v, 2) for k, v in SHARES.items()}


def lanes(order: list[str], files: dict[str, set[str]]) -> list[list[str]]:
    """Classes that share a file go into one sequential lane, in the given order; lanes run in parallel."""
    parent = {c: c for c in order}

    def root(c: str) -> str:
        """The lane representative of a class."""
        while parent[c] != c:
            c = parent[c]
        return c

    for i, a in enumerate(order):
        for b in order[i + 1:]:
            if files[a] & files[b]:
                parent[root(b)] = root(a)
    out: dict[str, list[str]] = {}
    for c in order:
        out.setdefault(root(c), []).append(c)
    return list(out.values())


def usage_of(result: dict | None) -> dict:
    """Token counts and cost of one `claude -p --output-format json` result."""
    u = (result or {}).get("usage") or {}
    return {**{k: int(u.get(k) or 0) for k in USAGE_KEYS}, "cost_usd": float((result or {}).get("total_cost_usd") or 0.0)}


def tokens(records: list[dict]) -> dict:
    """Per-step and total tokens, cost and wall time over the recorded runs."""
    zero = lambda: {"runs": 0, **{k: 0 for k in USAGE_KEYS}, "cost_usd": 0.0, "wall_s": 0.0}
    steps = {s: zero() for s in RUN_STEPS}
    for r in records:
        st = steps[r["step"]]
        st["runs"] += 1
        st["wall_s"] += float(r.get("wall_s") or 0.0)
        for k, v in usage_of(r.get("result")).items():
            st[k] += v
    total = zero()
    for st in steps.values():
        for k in total:
            total[k] += st[k]
    for d in (*steps.values(), total):
        d["cost_usd"], d["wall_s"] = round(d["cost_usd"], 4), round(d["wall_s"], 1)
    runs = [{k: r.get(k) for k in ("name", "step", "agent", "rc", "wall_s", "budget_usd", "session_id")} | {"cost_usd": usage_of(r.get("result"))["cost_usd"]} for r in records]
    return {"steps": steps, "total": total, "runs": runs}


def settings(kind: str, wt: Path, out: Path, scratch: Path, hunt: Path) -> dict:
    """The permission settings of one class: `write` agents edit only their worktree, handoff and scratch; `read` agents edit nothing."""
    a = lambda p: f"/{p}"
    allow = [f"Read({a(p)}/**)" for p in (wt, out, scratch, hunt)] + ["Read(~/.claude/rivet-*.md)", "Read(~/.claude/agents/rivet-*.md)"]
    allow += [f"{t}({a(p)}/**)" for t in ("Edit", "Write") for p in ((wt, out, scratch) if kind == "write" else (out, scratch))]
    allow += [f"Bash({g} {s}*)" for g in (GIT, "git") for s in ("", "-C ")]
    allow += [f"Bash({c} *)" for c in (CARGO, "cargo", "/usr/bin/make", "make", "uv", "pytest", "python3 -m dev.", "python -m dev.")]
    allow += [f"Bash(CARGO_TARGET_DIR={a(wt)[1:]}/target {c} *)" for c in (CARGO, "cargo", "/usr/bin/make", "make", "uv")]
    allow += ["Bash(COMPOSE_PROJECT_NAME=rivet docker compose *)", "Bash(docker container inspect *)", "Bash(docker inspect *)", "Bash(docker ps *)"]
    allow += [f"Bash({c} *)" for c in ("/usr/bin/grep", "grep", "rg", "sed -n", "head", "tail", "wc", "ls", "cat", "cmp", "diff", "cp", "touch", "mkdir", "sleep", "/bin/ps", "date")]
    allow += ["Bash(until ! /bin/ps -axo command *)", "Bash(until /usr/bin/grep -q *)", "Bash(until grep -q *)"]
    allow += [f"Bash({c} {a(p)[1:]}/*)" for c in ("rm", "rm -f", "rm -r", "rm -rf", "bash", "/bin/sh", "nohup bash", "nohup /bin/sh") for p in (wt, scratch)]
    allow += [f"Bash(export {v}=*)" for v in ("CARGO_TARGET_DIR", "BIGQUERY_TEST_PROJECT", "RIVET_TEST_GCS_BUCKET", "BIGQUERY_TEST_LOCATION", "COMPOSE_PROJECT_NAME", "RIVET_TEST_STATE_URL", "RIVET_GATE_STATE_URL")]
    allow += [f"mcp__github__{t}" for t in GH_READ + (GH_WRITE if kind == "write" else ())]
    deny = ["Agent", "WebFetch", "WebSearch", "NotebookEdit", "Bash(gh *)", "Bash(*/gh *)", "Bash(curl *)", "Bash(*--dangerously*)", "Bash(sudo *)"]
    deny += [f"Bash({g} push{s})" for g in (GIT, "git") for s in (" --force*", " -f*", " *--force*", " * -f *", " *+*", " origin main*", " * main", " *:main*", " *HEAD:main*", " --mirror*", " --delete*", " * --delete *")]
    deny += [f"Bash({g} rebase*)" for g in (GIT, "git")] + ["Bash(rm -rf /)", "Bash(rm -rf ~*)", "Bash(rm -rf $HOME*)", "Bash(rm -rf ..*)", "Bash(rm -rf .)", "Bash(rm -rf *)" if kind == "read" else "Bash(rm -rf /Users/*/rivet)"]
    deny += ["Bash(*docker compose up*)", "Bash(*docker compose down*)", "Bash(*docker compose * up*)", "Bash(*docker compose * down*)", "Bash(pkill *)", "Bash(killall *)"]
    deny += [f"mcp__github__{t}" for t in GH_DENY]
    if kind == "read":
        deny += [f"Bash({g} push*)" for g in (GIT, "git")] + [f"Bash({g} commit*)" for g in (GIT, "git")] + [f"mcp__github__{t}" for t in GH_WRITE]
    return {"permissions": {"allow": allow, "deny": deny, "defaultMode": PERMISSION_MODE}}


class Fix:
    """One fix round over the accepted classes of one hunt; every output stays on disk under OUT."""

    def __init__(self, a: argparse.Namespace, root: Path = ROOT, gate_cmd: str = GATE_WAIT, rules_path: Path = RULES_PATH,
                 scratch_root: Path = SCRATCH_ROOT, hunt_cli: list[str] | None = None):
        """Load the hunt, select and check the classes, and resolve every path."""
        self.a, self.root, self.gate_cmd, self.rules_path, self.scratch_root = a, root, gate_cmd, rules_path, scratch_root
        self.hunt_cli = hunt_cli or [sys.executable, "-m", "dev.pytools.hunt"]
        self.hunt = Path(a.hunt).resolve()
        if not (self.hunt / "classes.json").is_file():
            raise FixError(f"{self.hunt} holds no classes.json; is it a hunt OUT dir?")
        self.out = (a.out or root / "target" / "fix" / self.hunt.name).resolve()
        common = subprocess.run([GIT, "rev-parse", "--path-format=absolute", "--git-common-dir"], cwd=root, capture_output=True, text=True)
        self.main = Path(common.stdout.strip()).parent if common.returncode == 0 and common.stdout.strip() else root
        self.classes = {c["id"]: c for c in json.loads((self.hunt / "classes.json").read_text())["classes"]}
        self.verdicts = self.load_verdicts()
        self.cands = candidates((self.hunt / "candidates.md").read_text()) if (self.hunt / "candidates.md").is_file() else []
        self.ids = [c.strip() for c in a.classes.split(",") if c.strip()]
        self.force = {c.strip() for f in a.force_class for c in f.split(",") if c.strip()}
        errs = [e for cid in self.ids for e in [self.refusal(cid)] if e]
        if errs:
            raise FixError("; ".join(errs))
        if len(set(self.ids)) != len(self.ids) or not self.ids:
            raise FixError("--classes needs distinct class ids")
        self.gate_done = False
        self.gate_at = ""

    def load_verdicts(self) -> dict[str, dict]:
        """Class id -> the hunt verifier's verdict row."""
        p = self.hunt / "verdicts.md"
        obj, err = json_block(p.read_text()) if p.is_file() else (None, "absent")
        rows = obj.get("verdicts", []) if isinstance(obj, dict) and not err else []
        return {r["class"]: r for r in rows if isinstance(r, dict) and isinstance(r.get("class"), str)}

    def closed(self) -> dict[str, object]:
        """Class id -> closing PR, from the hunt state file when it records this hunt's closed classes."""
        p = self.root / "target" / "hunt" / "state.json"
        try:
            data = json.loads(p.read_text()) if p.is_file() else {}
        except json.JSONDecodeError:
            return {}
        rows = data.get("closed", {}) if isinstance(data, dict) else {}
        if isinstance(rows, dict):
            rows = [{"class": k, **(v if isinstance(v, dict) else {"pr": v})} for k, v in rows.items()]
        out = {}
        for r in rows if isinstance(rows, list) else []:
            cid = isinstance(r, dict) and (r.get("class") or r.get("class_id") or r.get("id"))
            hunt = isinstance(r, dict) and str(r.get("hunt") or r.get("out") or "")
            if cid and (not hunt or Path(hunt).name == self.hunt.name):
                out[str(cid).split("/")[-1]] = r.get("pr")
        return out

    def refusal(self, cid: str) -> str | None:
        """Why a class may not be fixed, or None when the hunt accepted it."""
        c = self.classes.get(cid)
        if not c:
            return f"{cid}: not in {self.hunt / 'classes.json'} (known: {', '.join(self.classes)})"
        if cid in self.closed():
            return f"{cid}: already closed by PR {self.closed()[cid]} (target/hunt/state.json)"
        if str(c["status"]).startswith("closed"):
            return f"{cid}: {c['status']}"
        if c["status"] == "clean":
            return f"{cid}: clean, no violated site"
        if c.get("one_off"):
            return None if cid in self.force else f"{cid}: {c['status']}; pass --force-class {cid} to fix a ONE-OFF anyway"
        v = self.verdicts.get(cid)
        if v is None:
            return f"{cid}: not verified by the hunt (below --top); not accepted"
        if v.get("holds") is not True:
            return f"{cid}: refuted by the hunt verifier: {english(str(v.get('evidence', ''))[:200], str(self.hunt / 'verdicts.md'))}"
        return None

    def files(self, cid: str) -> set[str]:
        """The files a class touches, from its sites."""
        return {s["file"] for s in self.classes[cid]["sites"]}

    def candidate(self, cid: str) -> dict | None:
        """The architect candidate whose KEY CLAIM is one of the class predicates."""
        preds = {norm(p) for p in self.classes[cid]["predicates"]}
        return next((k for k in self.cands if norm(k["claim"]) in preds), None)

    def role(self, cid: str) -> tuple[str, str]:
        """rivet-deepener for a Strong deepening candidate, else rivet-bug-fixer, with the reason."""
        k = self.candidate(cid)
        if k and k["strong"] and k["deepening"]:
            return "rivet-deepener", f"candidates.md:{k['start']}-{k['end']} is Strong and a deepening (one module, copies deletable)"
        why = "no architect candidate" if not k else "candidate not graded Strong" if not k["strong"] else "candidate is not a deepening"
        return "rivet-bug-fixer", why

    def rules(self, cid: str) -> list[tuple[str, str]]:
        """The process rules the class names: its rule predicates, rule titles in its candidate, the rules of its seam."""
        if not self.rules_path.is_file():
            return []
        rules = parse_rules(self.rules_path.read_text())
        c, k = self.classes[cid], self.candidate(cid)
        hay = norm(" ".join(c["predicates"]) + " " + (k["text"] if k else ""))
        titles = {t for t in rules if t in hay}
        with contextlib.suppress(Exception):
            from dev.pytools import seams
            titles |= {norm(r) for s in seams.SEAMS if s.name == c["seam"] for r in s.rules if norm(r) in rules}
        return [rules[t] for t in sorted(titles)]

    def cslug(self, cid: str) -> str:
        """The class slug: branch fix/<slug>, worktree .claude/worktrees/fix-<slug>."""
        return slug(self.classes[cid]["seam"])

    def paths(self, cid: str) -> dict[str, Path | str]:
        """Branch, worktree, handoff dir and scratch of one class."""
        s = self.cslug(cid)
        return {"branch": f"fix/{s}", "wt": self.main / ".claude" / "worktrees" / f"fix-{s}", "out": self.out / cid,
                "scratch": self.scratch_root / f"fix-{self.hunt.name}-{cid}" / "scratchpad"}

    def per_class(self) -> float:
        """The budget of one class."""
        return self.a.budget_usd

    def plan(self) -> list[str]:
        """The planned round, one line each."""
        order = lanes(self.ids, {c: self.files(c) for c in self.ids})
        pools = budgets(self.per_class())
        lines = [f"plan: out {self.out}", f"plan: hunt {self.hunt}", f"plan: budget ${self.per_class():.2f} per class, ${self.per_class() * len(self.ids):.2f} total; pools " + ", ".join(f"{k} ${v:.2f}" for k, v in pools.items()),
                 f"plan: {len(order)} lane(s), at most {max(1, self.a.parallel)} at once; wait for the release gate once before the first claude run"]
        for lane in order:
            if len(lane) > 1:
                shared = sorted(set.intersection(*[self.files(c) for c in lane]) or set.union(*[self.files(a) & self.files(b) for a in lane for b in lane if a != b]))
                lines.append(f"plan: SEQUENTIAL {' -> '.join(lane)} (they share {', '.join(shared[:4])}{' ...' if len(shared) > 4 else ''})")
        for cid in self.ids:
            c, p, (role, why) = self.classes[cid], self.paths(cid), self.role(cid)
            rules = [t for t, _ in self.rules(cid)]
            lines.append(f"plan: {cid} {c['seam']!r} status={c['status']} violated={c['violated']} files={len(self.files(cid))} role={role} ({why})")
            lines.append(f"plan: {cid} branch={p['branch']} worktree={p['wt']} (created by the agent) handoff={p['out']}")
            lines.append(f"plan: {cid} rules={rules or ['-']}")
            lines.append(f"plan: {cid} steps plan({role}) -> plan-check(rivet-verifier) -> implement({role}, <= {MAX_CONTINUATIONS} continuations) -> runner(rivet-runner) -> seed(rivet-seed-keeper, if no seed rows) -> review(rivet-verifier, <= 1 rework by a fresh {role}) -> close (no merge)")
        if self.a.record_closed:
            lines.append("plan: on mergeable, record the class closed with dev.pytools.hunt --close when its --help lists --close")
        return lines

    def path(self, cid: str, *parts: str) -> Path:
        """A path under the class handoff dir."""
        return self.out.joinpath(cid, *parts)

    def wait_gate(self) -> None:
        """Wait once for a running release gate."""
        if self.gate_done:
            return
        print("fix: waiting for the release gate (if one runs)", flush=True)
        subprocess.run(["/bin/sh", "-c", self.gate_cmd], check=True)
        self.gate_done, self.gate_at = True, f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ}"

    def settings_file(self, cid: str, kind: str) -> Path:
        """Write the class's scoped settings file for `write` or `read` agents."""
        p = self.paths(cid)
        f = self.path(cid, f"claude-settings-{kind}.json")
        f.write_text(json.dumps(settings(kind, p["wt"], p["out"], p["scratch"], self.hunt), indent=1))
        return f

    def records(self, cid: str) -> list[dict]:
        """Every run record of a class."""
        d = self.path(cid, "runs")
        return [json.loads(f.read_text()) for f in sorted(d.glob("*.json"))] if d.is_dir() else []

    def cap(self, cid: str, step: str) -> float:
        """The --max-budget-usd of the next run of `step`: what is left of its pool, the first plan/review run a share of it."""
        pool = POOL[step]
        total = budgets(self.per_class())[pool]
        spent = sum(usage_of(r.get("result"))["cost_usd"] for r in self.records(cid) if POOL.get(r["step"]) == pool)
        left = total - spent
        ran = any(r["step"] == step for r in self.records(cid))
        cap = min(left, total * FIRST_SHARE[step]) if step in FIRST_SHARE and not ran else left
        if cap < MIN_RUN_USD:
            raise Stop("budget-exhausted", f"the {pool} pool (${total:.2f}) has ${left:.2f} left for {step}")
        return round(cap, 2)

    def claude(self, cid: str, step: str, name: str, agent: str, kind: str, prompt: str, resume: str | None = None) -> dict:
        """One `claude -p` run from the main checkout; its record lands in OUT/<class>/runs/NAME.json."""
        if not self.a.go:
            raise FixError("refusing to call claude without --go")
        self.wait_gate()
        exe = shutil.which("claude")
        if not exe:
            raise FixError("no `claude` executable on PATH")
        p = self.paths(cid)
        Path(p["scratch"]).mkdir(parents=True, exist_ok=True)
        budget = self.cap(cid, step)
        sid = resume or str(uuid.uuid4())
        cmd = [exe, "-p", "--agent", agent, "--output-format", "json", "--max-budget-usd", f"{budget:.2f}",
               "--permission-mode", PERMISSION_MODE, "--permission-prompts", "none", "--settings", str(self.settings_file(cid, kind)),
               "--add-dir", str(p["out"]), "--add-dir", str(p["scratch"]), "--add-dir", str(self.hunt)] + (["--resume", sid] if resume else ["--session-id", sid])
        t0 = time.monotonic()
        try:
            r = subprocess.run(cmd, cwd=self.main, input=prompt, capture_output=True, text=True, timeout=TIMEOUT_S[step])
            rc, stdout, stderr = r.returncode, r.stdout, r.stderr
        except subprocess.TimeoutExpired as e:
            rc, stdout, stderr = 124, e.stdout or "", f"timed out after {TIMEOUT_S[step]}s"
        try:
            result = json.loads(stdout)
        except (json.JSONDecodeError, TypeError):
            result = None
        rec = {"name": name, "step": step, "agent": agent, "rc": rc, "wall_s": round(time.monotonic() - t0, 1), "budget_usd": budget,
               "session_id": (result or {}).get("session_id") or sid, "cmd": cmd, "result": result, "stderr_tail": (stderr or "")[-500:]}
        self.path(cid, "runs").mkdir(parents=True, exist_ok=True)
        self.path(cid, "runs", f"{name}.json").write_text(json.dumps(rec, indent=1))
        print(f"fix: {cid} {name} agent={agent} rc={rc} ${usage_of(result)['cost_usd']:.2f} {rec['wall_s']:.0f}s", flush=True)
        return rec

    @staticmethod
    def text_of(rec: dict) -> str:
        """The agent's final text, or empty when the run failed."""
        r = rec.get("result") or {}
        return r.get("result") or "" if rec["rc"] == 0 and not r.get("is_error") else ""

    def ask(self, cid: str, step: str, name: str, agent: str, kind: str, prompt: str, check, allow_checkpoint: bool = False) -> tuple[str, object]:
        """Run, grade the closing JSON block, re-ask the same session once; a checkpoint block is returned as is."""
        rec = self.claude(cid, step, name, agent, kind, prompt)
        text = self.text_of(rec)
        if not text:
            raise Stop("run-failed", f"{name} (rc {rec['rc']}); see {self.path(cid, 'runs', name + '.json')}")
        for attempt in (0, 1):
            obj, err = json_block(text)
            if allow_checkpoint and isinstance(obj, dict) and isinstance(obj.get("checkpoint"), str) and Path(obj["checkpoint"]).is_file():
                return text, obj
            errs = [err] if err else check(obj)
            if not errs:
                return text, obj
            if attempt:
                self.path(cid, f"{name}.invalid.md").write_text(text)
                raise Stop("invalid-reply", f"{name}: {'; '.join(errs)[:300]} (after one re-ask)")
            again = (f"Your reply could not be graded: {'; '.join(errs)[:800]}. Reply again with the whole result and end with ONE fenced "
                     "```json block of the shape you were given, with nothing after it. English only.")
            text = self.text_of(self.claude(cid, step, f"{name}-reask", agent, kind, again, resume=rec["session_id"])) or text
        raise AssertionError("unreachable")

    def header(self, cid: str, step: str, extra: str = "") -> str:
        """The fixed head of every brief: class, step, paths and the rules of a headless fix run."""
        p = self.paths(cid)
        return (f"FIX-CLASS: {cid}\nFIX-STEP: {step}\nFIX-HANDOFF: {p['out']}\nFIX-BRANCH: {p['branch']}\n{extra}\n"
                f"You were started headless by dev/pytools/fix.py (FIX round of hunt {self.hunt.name}). Read ~/.claude/rivet-agents-base.md and your role file first; they bind you.\n"
                "- English only in every file, commit, PR, comment and reply; no Cyrillic (run the base file's Cyrillic grep before every commit and PR).\n"
                "- Never write fix/fixes/fixed/close(s/d)/resolve(s/d) directly before #N; write `see #N`.\n"
                "- No subagents (the Agent tool is denied). Never merge a PR. Never push to main, never force-push, never rebase a pushed branch. GitHub only through the github MCP tools, never `gh`.\n"
                f"- Worktree: {p['wt']} on branch {p['branch']}; CARGO_TARGET_DIR={p['wt']}/target. Your shell starts in the main checkout {self.main}: cd into the worktree before any other command.\n"
                f"- Scratchpad: {p['scratch']}. Handoff dir (your notes, PLAN.md, checkpoints): {p['out']}.\n"
                f"- fix.py waited for the release gate once at {self.gate_at or 'start'}; still run the base file's gate wait before your first cargo/make/docker command. Every live run goes through the live slot (`/usr/bin/make live CMD=...`).\n"
                "- Permissions are a fixed allow list (dontAsk): a denied command is denied for good; report it, do not look for a way around it.\n")

    def ctx(self, cid: str) -> str:
        """The class brief: table, verifier evidence, candidate, key claim, process rules, the per-engine instruction."""
        c, ref = self.classes[cid], str(self.hunt / "classes.json")
        out = [f"## Class {cid}: {c['seam']} ({c['status']}, {c['violated']} violated, {c['proven_violated']} proven, engines {', '.join(c['engines']) or '-'})", ""]
        out += [f"- predicate: {english(p, ref)}" for p in c["predicates"]]
        v = self.verdicts.get(cid)
        if v:
            out += ["", f"Hunt verifier: holds={v.get('holds')}: {english(str(v.get('evidence', '')), str(self.hunt / 'verdicts.md'))}"]
        out += ["", "| site | engine | verdict | proven | evidence |", "|---|---|---|---|---|"]
        out += [f"| `{s['file']}:{s['line']}` | {s['engine']} | {s['verdict']} | {'yes' if s['proven'] else 'no'} | {english(s['evidence'], ref).replace('|', '/')} |" for s in c["sites"]]
        k = self.candidate(cid)
        if k:
            kref = f"{self.hunt / 'candidates.md'}:{k['start']}-{k['end']}"
            out += ["", f"## Architect candidate ({kref})", "", english(k["text"], kref), "", f"KEY CLAIM: {english(k['claim'], kref)}"]
        for title, body in self.rules(cid):
            out += ["", f"## Process rule: {title}", "", english(body, str(self.rules_path))]
        out += ["", "Fixed instruction: build a per-engine table before calling anything a one-off (base rule 2026-11-08)."]
        return "\n".join(out) + "\n"

    def done(self, cid: str, name: str) -> tuple[str, object] | None:
        """The saved reply of a finished step and its block, when the step ran before."""
        f = self.path(cid, name)
        if not f.is_file():
            return None
        return f.read_text(), json_block(f.read_text())[0]

    def save(self, cid: str, name: str, text: str, later: tuple[str, ...]) -> None:
        """Keep a step's reply and drop the outputs of the steps after it."""
        for f in later:
            self.path(cid, f).unlink(missing_ok=True)
        self.path(cid, name).write_text(text)

    def gated(self, cid: str, step: str) -> bool:
        """Whether `step` may run now: always, unless --only names another step."""
        if self.a.only and self.a.only != step:
            raise Stop("incomplete", f"--only {self.a.only}: step {step} has no output yet; run it first")
        return True

    def step_choose(self, cid: str) -> dict:
        """00-choose.json: role, reason, branch, worktree; brief.md."""
        f = self.path(cid, "00-choose.json")
        if f.is_file():
            return json.loads(f.read_text())
        role, why = self.role(cid)
        p = self.paths(cid)
        got = {"class": cid, "role": role, "reason": why, "branch": p["branch"], "worktree": str(p["wt"]), "handoff": str(p["out"])}
        self.path(cid, "brief.md").write_text(self.ctx(cid))
        f.write_text(json.dumps(got, indent=1))
        return got

    def implement_like(self, cid: str, step: str, role: str, first: str, check) -> tuple[str, dict]:
        """A writer run that may end in a checkpoint: a fresh agent of the same role continues, at most twice."""
        k = 0
        while self.path(cid, f"{step}-cp{k + 1}.md").is_file():
            k += 1
        prompt = first
        if k:
            note = json_block(self.path(cid, f"{step}-cp{k}.md").read_text())[0]["checkpoint"]
            prompt = self.continuation(cid, step, note, first)
        while True:
            if k > MAX_CONTINUATIONS:
                raise Stop("checkpoint-exhausted", f"{step}: {MAX_CONTINUATIONS} continuations ended in checkpoints; last note {self.path(cid, f'{step}-cp{k}.md')}")
            text, obj = self.ask(cid, step, step if not k else f"{step}-c{k}", role, "write", prompt, check, allow_checkpoint=True)
            if "checkpoint" in obj and check(obj):
                k += 1
                self.path(cid, f"{step}-cp{k}.md").write_text(text)
                prompt = self.continuation(cid, step, obj["checkpoint"], first)
                continue
            return text, obj

    def continuation(self, cid: str, step: str, note: str, first: str) -> str:
        """The brief of a fresh agent that continues from a checkpoint note."""
        return first.replace(f"FIX-STEP: {step}\n", f"FIX-STEP: {step}\nFIX-CONTINUE: {note}\n", 1) + (
            f"\nA previous agent of your role stopped at a checkpoint. Its note is {note}: read it first, re-run the claims it lists as done "
            "before you repeat them, and continue from its exact next command. The worktree and branch already exist.\n")

    def checkpoint_rule(self, cid: str, step: str) -> str:
        """How a writer ends at the 150-tool-call checkpoint."""
        return (f"If you pass ~150 tool calls or your context grows large, write the checkpoint note (base file) to {self.path(cid, step + '-checkpoint-<n>.md')} "
                'with the next free <n>, and end your reply with ```json\n{"checkpoint": "<that path>"}\n``` instead; a fresh agent continues from it.\n')

    def step_plan(self, cid: str, role: str) -> dict:
        """01-plan.md: the fixer reproduces RED and writes PLAN.md, then stops before code."""
        got = self.done(cid, "01-plan.md")
        plan_path = self.path(cid, "PLAN.md")
        if not got and self.gated(cid, "plan"):
            p = self.paths(cid)
            prompt = (self.header(cid, "plan", f"FIX-PLAN: {plan_path}") + "\n" + self.ctx(cid) + "\n## Your task: the PLAN stage only\n"
                      f"Create your worktree per the base file: `cd {self.main} && /usr/bin/git fetch -q origin && /usr/bin/git worktree add -q -b {p['branch']} "
                      f"{p['wt']} origin/main && cd {p['wt']} && uv sync --frozen -q` (if it already exists from an earlier stage, reuse it).\n"
                      "Do the first steps of your role's order of work: verify the claim, write the test that shows the defect and run it on origin/main "
                      "(capture the RED line), find the class and its shared seam, and draft the per-engine table plan.\n"
                      f"Then write {plan_path} with: the defect, the seam (file:line), the RED test and its RED line, the fix intent, the unchanged neighbours "
                      "you will pin, existing users, and the engines where the path exists. Leave the test uncommitted in the worktree.\n"
                      "STOP there: write no fix, no commit, no push, no PR. A verifier attacks the plan before any code.\n"
                      "If the defect does not reproduce (deepener: the candidate's claim is wrong), still write PLAN.md with the evidence and set reproduced false.\n"
                      f"End your reply with ONE fenced ```json block, nothing after it:\n{PLAN_SCHEMA.replace('PATH', str(plan_path))}\n")
            text, _ = self.ask(cid, "plan", "plan", role, "write", prompt, lambda o: validate_plan(o, plan_path))
            self.save(cid, "01-plan.md", text, ("02-plan-check.md", "03-implement.md", "04-runner.md", "05-seed.md", "06-review.md", "06-rework.md", "06-review-2.md", "07-close.md"))
            got = self.done(cid, "01-plan.md")
        obj = got[1]
        if obj.get("reproduced") is not True:
            raise Stop("not-reproduced", f"the plan stage did not reproduce the defect; see {plan_path}")
        return obj

    def step_plan_check(self, cid: str) -> dict:
        """02-plan-check.md: rivet-verifier attacks PLAN.md; refuted stops the class before code."""
        got = self.done(cid, "02-plan-check.md")
        if not got and self.gated(cid, "plan-check"):
            p = self.paths(cid)
            prompt = (self.header(cid, "plan-check") + "\n" + self.ctx(cid) + "\n## Your task: verify the plan (your role's \"Verify the plan when asked\")\n"
                      f"The fixer's plan is {self.path(cid, 'PLAN.md')}; its RED test is uncommitted in {p['wt']}. Stay read-only. Attack the plan: the neighbours it "
                      "would change, existing users, the engines it forgets, a seam that is not the shared one, a RED line that does not show the defect.\n"
                      f"End your reply with ONE fenced ```json block, nothing after it (holds false means the plan is refuted):\n{CHECK_SCHEMA}\n")
            text, _ = self.ask(cid, "plan-check", "plan-check", "rivet-verifier", "read", prompt, validate_check)
            self.save(cid, "02-plan-check.md", text, ("03-implement.md", "04-runner.md", "05-seed.md", "06-review.md", "06-rework.md", "06-review-2.md", "07-close.md"))
            got = self.done(cid, "02-plan-check.md")
        obj = got[1]
        if obj["holds"] is not True:
            raise Stop("plan-refuted", " | ".join(obj["reasons"])[:600])
        return obj

    def step_implement(self, cid: str, role: str) -> dict:
        """03-implement.md: a fresh fixer continues from PLAN.md to a PR."""
        got = self.done(cid, "03-implement.md")
        if not got and self.gated(cid, "implement"):
            p = self.paths(cid)
            prompt = (self.header(cid, "implement") + "\n" + self.ctx(cid) + "\n## Your task: implement the accepted plan\n"
                      f"The plan is {self.path(cid, 'PLAN.md')}; the verifier's plan check is {self.path(cid, '02-plan-check.md')} (take its reasons into account). "
                      f"The worktree {p['wt']} exists with the RED test uncommitted. Do your role's whole order of work and open ONE PR "
                      f"(mcp__github__create_pull_request, owner panchenkoai, repo rivet, head {p['branch']}, base main) after `/usr/bin/make pr-ready`.\n"
                      + self.checkpoint_rule(cid, "implement") +
                      f"Otherwise end your reply with ONE fenced ```json block, nothing after it (seed_rows: the seeded-recall rows you added and ran, empty if none):\n{IMPL_SCHEMA.replace('BRANCH', p['branch'])}\n")
            text, _ = self.implement_like(cid, "implement", role, prompt, lambda o: validate_impl(o, p["branch"]))
            self.save(cid, "03-implement.md", text, ("04-runner.md", "05-seed.md", "06-review.md", "06-rework.md", "06-review-2.md", "07-close.md"))
            got = self.done(cid, "03-implement.md")
        return got[1]

    def step_runner(self, cid: str, impl: dict) -> None:
        """04-runner.md: rivet-runner executes the fixer's list; any red stops the class."""
        cmds = [r["cmd"] for r in impl["for_runner"]]
        got = self.done(cid, "04-runner.md")
        if not got and self.gated(cid, "runner"):
            if not cmds:
                self.save(cid, "04-runner.md", 'The fixer listed nothing for rivet-runner.\n\n```json\n{"results": []}\n```\n', ())
                return
            p = self.paths(cid)
            listing = "\n".join(f"{i}. `{r['cmd']}` -> expect `{r['expect']}`" for i, r in enumerate(impl["for_runner"], 1))
            prompt = (self.header(cid, "runner") + f"\n## Your task: run this list on branch {p['branch']} in the existing worktree {p['wt']} (PR {impl['pr_url']}, head {impl['head_sha']})\n\n"
                      f"{listing}\n\nOne script, one wait (your role file). pass is true only when the verdict line matches the expectation with a non-zero count.\n"
                      f"End your reply with ONE fenced ```json block, nothing after it, one row per command in this order:\n{RUNNER_SCHEMA}\n")
            text, _ = self.ask(cid, "runner", "runner", "rivet-runner", "write", prompt, lambda o: validate_runner(o, cmds))
            self.save(cid, "04-runner.md", text, ("05-seed.md", "06-review.md", "06-rework.md", "06-review-2.md", "07-close.md"))
            got = self.done(cid, "04-runner.md")
        red = [r for r in got[1]["results"] if not r["pass"]]
        if red:
            raise Stop("runner-red", " | ".join(f"{r['cmd']} -> {r['verdict_line']}" for r in red)[:600])

    def step_seed(self, cid: str, impl: dict) -> None:
        """05-seed.md: rivet-seed-keeper when the fixer reported no seed rows."""
        if self.done(cid, "05-seed.md"):
            return
        self.gated(cid, "seed")
        if impl.get("seed_rows"):
            self.save(cid, "05-seed.md", "The fixer reported seed rows:\n" + "\n".join(f"- {r}" for r in impl["seed_rows"]) +
                      '\n\n```json\n' + json.dumps({"recall_lines": impl["seed_rows"], "missed": [], "product_defects": [], "skipped": True}) + "\n```\n", ())
            return
        p = self.paths(cid)
        prompt = (self.header(cid, "seed") + "\n" + self.ctx(cid) + f"\n## Your task: the seed of this class, on the fix branch\nThe fix is PR {impl['pr_url']} on branch {p['branch']} "
                  f"(head {impl['head_sha']}) in the existing worktree {p['wt']}; the fixer reported no seeded-recall rows. Add the seed for this class per your role, commit "
                  "on this branch and push it with a plain push (no new PR), then run seeded-recall for it.\n"
                  f"End your reply with ONE fenced ```json block, nothing after it:\n{SEED_SCHEMA}\n")
        text, _ = self.ask(cid, "seed", "seed", "rivet-seed-keeper", "write", prompt, validate_seed)
        self.save(cid, "05-seed.md", text, ("06-review.md", "06-rework.md", "06-review-2.md", "07-close.md"))

    def review(self, cid: str, name: str, impl: dict) -> dict:
        """One review of the PR at its current head."""
        got = self.done(cid, name)
        if got:
            return got[1]
        self.gated(cid, "review")
        p = self.paths(cid)
        prompt = (self.header(cid, "review", f"FIX-HEAD: {impl['head_sha']}") + "\n" + self.ctx(cid) + f"\n## Your task: review PR {impl['pr_url']} (your role's \"When the subject is a PR\")\n"
                  f"Branch {p['branch']}, head {impl['head_sha']}; the worktree {p['wt']} holds it (read-only for you; fetch first). Read the diff, not the description. "
                  f"The runner's verdicts are {self.path(cid, '04-runner.md')}, the seed {self.path(cid, '05-seed.md')}. If the head moved, review the new head and name it.\n"
                  f"End your reply with ONE fenced ```json block, nothing after it:\n{REVIEW_SCHEMA}\n")
        head = impl["head_sha"]
        text, _ = self.ask(cid, "review", name.removesuffix(".md"), "rivet-verifier", "read", prompt, lambda o: validate_review(o, head))
        self.save(cid, name, text, ("07-close.md",))
        return self.done(cid, name)[1]

    def step_review(self, cid: str, role: str, impl: dict) -> tuple[dict, dict]:
        """06-review*.md: review; one rework by a fresh agent on mergeable-after-fixes; then needs-human."""
        r = self.review(cid, "06-review.md", impl)
        if r["verdict"] == "not-mergeable":
            raise Stop("not-mergeable", self.findings(r))
        if r["verdict"] == "mergeable":
            return impl, r
        got = self.done(cid, "06-rework.md")
        if not got and self.gated(cid, "review"):
            p = self.paths(cid)
            prompt = (self.header(cid, "rework") + "\n" + self.ctx(cid) + f"\n## Your task: the rework round on PR {impl['pr_url']}\n"
                      f"A review found fixes to make: {self.path(cid, '06-review.md')}. You are a fresh agent of the author's role; the worktree {p['wt']} and branch "
                      f"{p['branch']} exist (head {impl['head_sha']}). Fix every listed finding on this branch, re-run pr-ready, push with a plain push, and update "
                      "the PR body (Evidence table) with mcp__github__update_pull_request.\n" + self.checkpoint_rule(cid, "rework") +
                      f"Otherwise end your reply with ONE fenced ```json block, nothing after it, pr_url unchanged and head_sha the new head:\n{IMPL_SCHEMA.replace('BRANCH', p['branch'])}\n")
            text, _ = self.implement_like(cid, "rework", role, prompt, lambda o: validate_impl(o, p["branch"], impl["pr_url"]))
            self.save(cid, "06-rework.md", text, ("06-review-2.md", "07-close.md"))
            got = self.done(cid, "06-rework.md")
        impl2 = got[1]
        r2 = self.review(cid, "06-review-2.md", impl2)
        if r2["verdict"] != "mergeable":
            raise Stop("needs-human", f"after one rework the review says {r2['verdict']}: {self.findings(r2)}")
        return impl2, r2

    @staticmethod
    def findings(r: dict) -> str:
        """The review findings, one line."""
        return " | ".join(f"[{f['sev']}] {f['input']} -> {f['output']}" for f in r["findings"])[:600] or "no findings listed"

    def hunt_has_close(self) -> bool:
        """Whether `dev.pytools.hunt --help` lists --close."""
        r = subprocess.run([*self.hunt_cli, "--help"], cwd=self.root, capture_output=True, text=True)
        return r.returncode == 0 and "--close" in r.stdout

    def step_close(self, cid: str, impl: dict, review: dict) -> str:
        """07-close.md: the PR, head, evidence, ratchets, second defects and the hunt close command; never merges."""
        n = PR_URL.match(impl["pr_url"]).group(1)
        cmd = f"python -m dev.pytools.hunt --close {cid} --pr {n} --out {self.hunt}"
        f = self.path(cid, "07-close.md")
        if not f.is_file():
            lines = [f"# {cid} mergeable", "", f"- PR: {impl['pr_url']}", f"- head: {review['head_sha']}", f"- RED: {impl['red_line']}", f"- GREEN: {impl['green_line']}", "",
                     "| engine | status |", "|---|---|", *[f"| {r['engine']} | {r['status']} |" for r in impl["engines_table"]], "",
                     "Ratchets raised: " + ("; ".join(map(str, impl["ratchets_raised"])) or "none"),
                     "Second defects: " + ("; ".join(map(str, impl["second_defects"])) or "none"),
                     "Not verified: " + ("; ".join(map(str, impl["not_verified"])) or "none"), "",
                     "fix.py does not merge: the merge (rivet-pr-shepherd) is the owner's call.", f"Record the class closed: `{cmd}`"]
            if self.a.record_closed and self.hunt_has_close():
                r = subprocess.run([*self.hunt_cli, "--close", cid, "--pr", n, "--out", str(self.hunt)], cwd=self.root, capture_output=True, text=True)
                lines.append(f"Recorded: rc {r.returncode} {(r.stdout + r.stderr).strip()[-200:]}")
            elif self.a.record_closed:
                lines.append("Not recorded: `dev.pytools.hunt --help` does not list --close.")
            f.write_text("\n".join(lines) + "\n")
        return f.read_text()

    def run_class(self, cid: str) -> dict:
        """Every step of one class in order; the class's status lands in status.json."""
        self.path(cid).mkdir(parents=True, exist_ok=True)
        status, reason, pr = "mergeable", "", ""
        try:
            role = self.step_choose(cid)["role"]
            if self.a.only == "choose":
                raise Stop("incomplete", "--only choose")
            self.step_plan(cid, role)
            self.step_plan_check(cid)
            impl = self.step_implement(cid, role)
            pr = impl["pr_url"]
            self.step_runner(cid, impl)
            self.step_seed(cid, impl)
            impl, review = self.step_review(cid, role, impl)
            if self.a.only and self.a.only != "close":
                raise Stop("incomplete", f"--only {self.a.only} done")
            reason = self.step_close(cid, impl, review)
        except Stop as s:
            status, reason = s.status, s.reason
        cost = sum(usage_of(r.get("result"))["cost_usd"] for r in self.records(cid))
        got = {"class": cid, "status": status, "reason": reason[:2000], "pr_url": pr, "cost_usd": round(cost, 4)}
        self.path(cid, "status.json").write_text(json.dumps(got, indent=1))
        self.path(cid, "tokens.json").write_text(json.dumps(tokens(self.records(cid)), indent=1))
        print(f"fix: {cid} {status} ${cost:.2f}{' ' + pr if pr else ''}: {reason.splitlines()[0][:160] if reason else ''}", flush=True)
        return got

    def run(self) -> int:
        """Run the lanes, at most --parallel at once; write summary.md and tokens.json."""
        if self.out.is_dir() and any(self.out.iterdir()) and not self.a.resume:
            print(f"fix: {self.out} already holds a fix round; pass --resume", file=sys.stderr)
            return 2
        self.out.mkdir(parents=True, exist_ok=True)
        order = lanes(self.ids, {c: self.files(c) for c in self.ids})
        try:
            self.wait_gate()
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(self.a.parallel, len(order)))) as pool:
                results = [r for lane in pool.map(lambda lane: [self.run_class(c) for c in lane], order) for r in lane]
        except FixError as e:
            print(f"fix: FAILED: {e}", file=sys.stderr)
            return 1
        by = {r["class"]: r for r in results}
        records = [r for cid in self.ids for r in self.records(cid)]
        tok = tokens(records)
        self.out.joinpath("tokens.json").write_text(json.dumps(tok, indent=1))
        rows = ["| class | status | cost | PR | reason |", "|---|---|---|---|---|"]
        rows += [f"| {c} | {by[c]['status']} | ${by[c]['cost_usd']:.2f} | {by[c]['pr_url'] or '-'} | {by[c]['reason'].splitlines()[0][:200].replace('|', '/') if by[c]['reason'] else ''} |" for c in self.ids]
        summary = [f"# Fix round: {self.hunt.name}", "", f"Cost ${tok['total']['cost_usd']:.2f}, {tok['total']['runs']} claude runs (tokens.json).", "", *rows, ""]
        summary += [x for c in self.ids if by[c]["status"] == "mergeable" for x in ["", self.path(c, "07-close.md").read_text()]]
        self.out.joinpath("summary.md").write_text("\n".join(summary) + "\n")
        for c in self.ids:
            if by[c]["status"] == "mergeable":
                print(self.path(c, "07-close.md").read_text(), flush=True)
        print(f"fix: done ${tok['total']['cost_usd']:.2f} -> {self.out / 'summary.md'}", flush=True)
        return 0 if all(by[c]["status"] == "mergeable" for c in self.ids) else 3


def parser() -> argparse.ArgumentParser:
    """The command line."""
    ap = argparse.ArgumentParser(prog="python -m dev.pytools.fix", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hunt", type=Path, help="the hunt OUT dir (classes.json, verdicts.md, candidates.md)")
    ap.add_argument("--classes", default="", help="class ids, comma-separated, in the order to run")
    ap.add_argument("--force-class", action="append", default=[], help="a ONE-OFF class id to fix anyway (repeatable)")
    ap.add_argument("--out", type=Path, help="default target/fix/<hunt dir name>")
    ap.add_argument("--parallel", type=int, default=2, help="lanes at once (default 2, the live-slot limit)")
    ap.add_argument("--budget-usd", type=float, default=25.0, help="per class, split into the step pools (default 25)")
    ap.add_argument("--go", action="store_true", help="run; without it only the plan is printed")
    ap.add_argument("--resume", action="store_true", help="continue in an existing OUT; finished steps are skipped by their files")
    ap.add_argument("--only", choices=STEPS)
    ap.add_argument("--record-closed", action="store_true", help="on mergeable, run dev.pytools.hunt --close when it exists")
    ap.add_argument("--self-test", action="store_true")
    return ap


FAKE_CLAUDE = r'''#!{python}
import json, os, re, sys
argv = sys.argv[1:]
prompt = sys.stdin.read()
state = os.environ["FIX_FAKE_DIR"]
scen = json.load(open(os.environ["FIX_FAKE_SCENARIO"]))
agent = argv[argv.index("--agent") + 1]
resume = argv[argv.index("--resume") + 1] if "--resume" in argv else None
sid = resume or argv[argv.index("--session-id") + 1]
sess = os.path.join(state, sid + ".json")
if resume:
    h = json.load(open(sess))
else:
    h = dict(re.findall(r"^FIX-([A-Z]+): (.*)$", prompt, re.M))
    json.dump(h, open(sess, "w"))
cid, step = h["CLASS"], h["STEP"]
cnt = os.path.join(state, cid + "-" + step + ".n")
n = int(open(cnt).read()) if os.path.exists(cnt) else 0
open(cnt, "w").write(str(n + 1))
replies = scen.get(cid, {}).get(step, ["ok"])
kind = replies[min(n, len(replies) - 1)]
with open(os.path.join(state, "log.jsonl"), "a") as f:
    f.write(json.dumps({"cid": cid, "step": step, "agent": agent, "kind": kind, "resume": resume, "argv": argv, "cwd": os.getcwd(), "cont": h.get("CONTINUE")}) + "\n")
fence = lambda o: "result\n```json\n" + json.dumps(o) + "\n```\n"
head = "def5678" if step == "rework" else "abc1234"
impl = {"pr_url": "https://github.com/panchenkoai/rivet/pull/42", "branch": h["BRANCH"], "head_sha": head, "red_line": "test result: FAILED. 0 passed; 1 failed",
        "green_line": "test result: ok. 1 passed", "engines_table": [{"engine": "postgres", "status": "fixed"}], "for_runner": [{"cmd": "make a", "expect": "ok"}],
        "ratchets_raised": [], "not_verified": [], "second_defects": ["x"], "seed_rows": []}
if kind == "nojson":
    text = "I am done."
elif kind == "checkpoint":
    p = os.path.join(h["HANDOFF"], step + "-checkpoint-" + str(n + 1) + ".md")
    open(p, "w").write("done: a\nnext: b\n")
    text = fence({"checkpoint": p})
elif step == "plan":
    open(h["PLAN"], "w").write("# plan\n")
    text = fence({"plan_path": h["PLAN"], "reproduced": kind != "not-reproduced", "red_test": "t", "red_line": "FAILED", "seam": "src/a.rs:1", "fix_intent": "x", "engines": ["postgres"]})
elif step == "plan-check":
    text = fence({"holds": kind != "refuted", "reasons": ["opened src/a.rs:1"]})
elif step in ("implement", "rework"):
    text = fence(impl)
elif step == "runner":
    text = fence({"results": [{"cmd": "make a", "verdict_line": "test result: FAILED" if kind == "red" else "test result: ok. 1 passed", "pass": kind != "red"}]})
elif step == "seed":
    text = fence({"recall_lines": ["seed x postgres CAUGHT"], "missed": [], "product_defects": []})
else:
    text = fence({"verdict": kind if kind in ("mergeable", "mergeable-after-fixes", "not-mergeable") else "mergeable", "head_sha": h["HEAD"], "findings": [{"sev": "high", "input": "i", "output": "o"}]})
print(json.dumps({"type": "result", "is_error": False, "result": text, "session_id": sid, "total_cost_usd": 0.5,
                  "usage": {"input_tokens": 100, "output_tokens": 10, "cache_creation_input_tokens": 5, "cache_read_input_tokens": 1000}}))
'''

RULES_FIXTURE = "# rivet\n\n## A lock is keyed by exactly what it protects\n\nThe lease key names the export.\n\n## An empty source is a result, never a failure\n\nZero rows exit 0.\n"


def fixture_hunt(d: Path) -> None:
    """A hunt OUT dir: accepted, refuted, ONE-OFF, unverified and clean classes; C1 and C4 share a file."""
    s = lambda f, n, e, v: {"file": f, "line": n, "engine": e, "verdict": v, "evidence": f"{f}:{n} quoted", "proven": True}
    cls = lambda cid, seam, sites, status="class", **kw: {"id": cid, "seam": seam, "predicates": kw.get("preds", ["A lock is keyed by exactly what it protects"]), "summaries": [], "sites": sites,
                                                          "violated": 2, "proven_violated": 2, "engines": ["pg"], "per_engine_table": True, "severity": 3, "score": 9,
                                                          "one_off": status.startswith("ONE-OFF"), "status": status}
    claim = "the lock key in src/a.rs:1 omits the source."
    classes = [cls("C1", "lock keys", [s("src/a.rs", 1, "pg", "violated"), s("src/b.rs", 2, "mysql", "violated")], preds=[claim]),
               cls("C2", "panics", [s("src/p.rs", 1, "pg", "violated")]),
               cls("C3", "stop proxy", [s("src/s.rs", 1, "n/a", "violated")], "ONE-OFF (needs per-engine table)", preds=["\u0441\u0442\u043e\u043f src/s.rs:1"]),
               cls("C4", "journal context", [s("src/b.rs", 9, "pg", "violated")]),
               cls("C5", "retries", [s("src/r.rs", 1, "pg", "violated")]),
               cls("C6", "clean seam", [s("src/c.rs", 1, "pg", "holds")], "clean"),
               cls("C7", "env reads", [s("src/e.rs", 1, "pg", "violated")]),
               cls("C8", "spill", [s("src/f.rs", 1, "pg", "violated")]),
               cls("C9", "unverified", [s("src/u.rs", 1, "pg", "violated")])]
    d.mkdir(parents=True)
    (d / "classes.json").write_text(json.dumps({"classes": classes, "invalid_sweeps": []}))
    v = [{"class": c, "holds": c != "C2", "evidence": "opened"} for c in ("C1", "C2", "C4", "C5", "C7", "C8")]
    (d / "verdicts.md").write_text("rows\n```json\n" + json.dumps({"verdicts": v}) + "\n```\n")
    (d / "candidates.md").write_text("# Lens\n\n## 1. Lock key\n\nOne module owns the key; the copies become deletable (locality).\n\n**Strength:** Strong\n\n"
                                     f"KEY CLAIM: {claim}\n\n## 2. Other\n\n**Strength:** Worth exploring\n\nKEY CLAIM: other src/z.rs:1.\n")


def self_test() -> int:
    """Fixture-only: plan, refusals, lanes, JSON grading and re-ask, checkpoints, stops, review loop, resume, budgets, settings."""
    assert abs(sum(SHARES.values()) - 1.0) < 1e-9 and set(SHARES) == set(POOL.values()), SHARES
    b = budgets(20)
    assert b == {"fixer": 9.0, "plan-check": 1.0, "runner": 3.0, "seed": 1.0, "review": 4.0, "rework": 2.0} and abs(sum(b.values()) - 20) < 0.01, b
    assert lanes(["C1", "C4", "C5"], {"C1": {"a", "b"}, "C4": {"b"}, "C5": {"c"}}) == [["C1", "C4"], ["C5"]]
    assert lanes(["A", "B", "C"], {"A": {"x"}, "B": {"y"}, "C": {"x", "y"}}) == [["A", "B", "C"]]
    assert english("plain", "r") == "plain" and "read it at r" in english("\u0441\u0442\u043e\u043f", "r")
    good = {"pr_url": "https://github.com/panchenkoai/rivet/pull/7", "branch": "fix/x", "head_sha": "abc1234", "red_line": "r", "green_line": "g", "engines_table": [{"engine": "pg", "status": "ok"}],
            "for_runner": [], "ratchets_raised": [], "not_verified": [], "second_defects": []}
    assert validate_impl(good, "fix/x") == [] and len(validate_impl({**good, "head_sha": "zz", "branch": "fix/y", "engines_table": []}, "fix/x")) == 3
    assert validate_impl(good, "fix/x", "https://github.com/panchenkoai/rivet/pull/8") and validate_impl([], "fix/x")
    assert validate_review({"verdict": "mergeable", "head_sha": "abc1234ff", "findings": []}, "abc1234") == []
    assert validate_review({"verdict": "ok", "head_sha": "abc9999", "findings": [{}]}, "abc1234") == ["`verdict` is not one of mergeable|mergeable-after-fixes|not-mergeable", "`head_sha` must be the PR head abc1234", "`findings` is not a list of {sev, input, output}"]
    assert validate_runner({"results": [{"cmd": "a", "verdict_line": "x", "pass": True}]}, ["a"]) == [] and validate_runner({"results": []}, ["a"])
    assert validate_check({"holds": False, "reasons": ["x"]}) == [] and validate_check({"holds": "no"}) and validate_seed({"recall_lines": []})
    for kind in ("write", "read"):
        st = settings(kind, Path("/w/wt"), Path("/w/out"), Path("/s"), Path("/h"))
        flat = json.dumps(st)
        assert "bypassPermissions" not in flat and "dangerously-skip" not in flat and st["permissions"]["defaultMode"] == "dontAsk", kind
        allow, deny = st["permissions"]["allow"], st["permissions"]["deny"]
        for t in GH_DENY:
            assert f"mcp__github__{t}" not in allow and f"mcp__github__{t}" in deny, t
        assert {"Agent", "WebFetch", "WebSearch", "Bash(gh *)"} <= set(deny) and "Edit(//w/wt/**)" in allow if kind == "write" else "Edit(//w/wt/**)" not in allow
        assert not any(x.startswith(("Bash(rm", "Bash(bash", "Edit(", "Write(")) and "/w/wt" not in x and "/s" not in x and "/w/out" not in x for x in allow), allow
        assert any(x.startswith(f"Bash({GIT} push --force") for x in deny) and any(" main" in x and "push" in x for x in deny)
    assert "mcp__github__create_pull_request" not in settings("read", Path("/w"), Path("/o"), Path("/s"), Path("/h"))["permissions"]["allow"]

    with tempfile.TemporaryDirectory() as tmp:
        t = Path(tmp)
        hunt = t / "hunt" / "2026-11-11-locks"
        fixture_hunt(hunt)
        repo = t / "repo"
        repo.mkdir()
        subprocess.run([GIT, "init", "-q", "-b", "main"], cwd=repo, check=True)
        fake_dir = t / "fake"
        fake_dir.mkdir()
        fake = t / "bin" / "claude"
        fake.parent.mkdir()
        fake.write_text(FAKE_CLAUDE.replace("{python}", sys.executable))
        fake.chmod(0o755)
        rules_path = t / "rules.md"
        rules_path.write_text(RULES_FIXTURE)
        scen = t / "scenario.json"
        scen.write_text(json.dumps({"C1": {"implement": ["nojson", "ok"]},
                                    "C4": {"implement": ["checkpoint", "checkpoint", "ok"], "review": ["mergeable-after-fixes", "mergeable-after-fixes"]},
                                    "C5": {"plan-check": ["refuted"]}, "C7": {"runner": ["red"]}, "C8": {"implement": ["checkpoint"]}}))
        env_keep = {k: os.environ.get(k) for k in ("PATH", "FIX_FAKE_DIR", "FIX_FAKE_SCENARIO")}
        os.environ.update({"PATH": f"{fake.parent}{os.pathsep}{os.environ.get('PATH', '')}", "FIX_FAKE_DIR": str(fake_dir), "FIX_FAKE_SCENARIO": str(scen)})
        log = fake_dir / "log.jsonl"
        calls = lambda: [json.loads(x) for x in log.read_text().splitlines()] if log.is_file() else []
        hunt_cli = [sys.executable, "-c", "import sys; print('usage: --close CLASS --pr N') if '--help' in sys.argv else print('closed', sys.argv[1:])"]

        def fix(out: str, classes: str, *extra: str) -> Fix:
            """A fix round over the fixture hunt with a no-op gate wait."""
            a = parser().parse_args(["--hunt", str(hunt), "--classes", classes, "--out", str(t / out), "--budget-usd", "20", *extra])
            return Fix(a, root=repo, gate_cmd="true", rules_path=rules_path, scratch_root=t / "scratch", hunt_cli=hunt_cli)

        noise = io.StringIO()
        quiet = contextlib.ExitStack()
        quiet.enter_context(contextlib.redirect_stdout(noise))
        quiet.enter_context(contextlib.redirect_stderr(noise))
        try:
            for ids, want in (("C2", "refuted by the hunt verifier"), ("C3", "--force-class C3"), ("C6", "clean"), ("C9", "not verified"), ("C42", "not in")):
                try:
                    fix("x", ids)
                except FixError as e:
                    assert want in str(e), (ids, e)
                else:
                    raise AssertionError(f"{ids} was accepted")
            assert fix("x", "C3", "--force-class", "C3").ids == ["C3"]
            f = fix("dry", "C1,C4,C5")
            plan = f.plan()
            assert any(x == "plan: SEQUENTIAL C1 -> C4 (they share src/b.rs)" for x in plan), plan
            assert any("plan: C1 " in x and "role=rivet-deepener" in x for x in plan) and any("plan: C5 " in x and "role=rivet-bug-fixer" in x for x in plan), plan
            assert any("plan: C1 branch=fix/lock-keys" in x for x in plan) and any("rules=['A lock is keyed by exactly what it protects']" in x for x in plan), plan
            assert any("$20.00 per class, $60.00 total" in x for x in plan), plan
            assert "per-engine table before calling anything a one-off" in f.ctx("C1") and "read it at" in fix("x", "C3", "--force-class", "C3").ctx("C3")
            assert main(["--hunt", str(hunt), "--classes", "C1", "--out", str(t / "dry")], root=repo, gate_cmd="true", rules_path=rules_path, scratch_root=t / "scratch") == 0
            assert not calls() and not (t / "dry").exists(), "a run without --go called claude or wrote OUT"
            try:
                f.claude("C1", "plan", "plan", "rivet-deepener", "write", "x")
            except FixError:
                pass
            else:
                raise AssertionError("claude ran without --go")

            f = fix("full", "C1,C4,C5,C7,C8", "--go", "--record-closed")
            assert f.run() == 3
            c = calls()
            by = lambda cid: [x for x in c if x["cid"] == cid]
            st = lambda cid: json.loads((t / "full" / cid / "status.json").read_text())
            assert st("C1")["status"] == "mergeable" and st("C1")["pr_url"].endswith("/pull/42"), st("C1")
            assert [(x["step"], x["agent"], bool(x["resume"])) for x in by("C1")] == [("plan", "rivet-deepener", False), ("plan-check", "rivet-verifier", False), ("implement", "rivet-deepener", False),
                                                                                        ("implement", "rivet-deepener", True), ("runner", "rivet-runner", False), ("seed", "rivet-seed-keeper", False), ("review", "rivet-verifier", False)], by("C1")
            close = (t / "full" / "C1" / "07-close.md").read_text()
            assert f"python -m dev.pytools.hunt --close C1 --pr 42 --out {f.hunt}" in close and f"Recorded: rc 0 closed ['--close', 'C1', '--pr', '42', '--out', '{f.hunt}']" in close and "| postgres | fixed |" in close, close
            assert st("C4")["status"] == "needs-human", st("C4")
            c4 = [(x["step"], x["kind"], bool(x["cont"])) for x in by("C4")]
            assert c4[2:5] == [("implement", "checkpoint", False), ("implement", "checkpoint", True), ("implement", "ok", True)], c4
            assert [s for s, _, _ in c4[5:]] == ["runner", "seed", "review", "rework", "review"] and not any(x["resume"] for x in by("C4")), c4
            assert max(i for i, x in enumerate(c) if x["cid"] == "C1") < min(i for i, x in enumerate(c) if x["cid"] == "C4"), "C4 overlapped C1"
            assert st("C5")["status"] == "plan-refuted" and [x["step"] for x in by("C5")] == ["plan", "plan-check"], by("C5")
            assert st("C7")["status"] == "runner-red" and [x["step"] for x in by("C7")][-1] == "runner" and "make a -> test result: FAILED" in st("C7")["reason"], st("C7")
            assert st("C8")["status"] == "checkpoint-exhausted" and [x["step"] for x in by("C8")].count("implement") == 3, by("C8")
            assert all(x["cwd"] == str(repo.resolve()) or x["cwd"] == str(repo) for x in c), {x["cwd"] for x in c}
            for x in c:
                a = x["argv"]
                assert a[a.index("--permission-mode") + 1] == "dontAsk" and "--settings" in a and "--max-budget-usd" in a and "--dangerously-skip-permissions" not in a, a
            caps = [float(x["argv"][x["argv"].index("--max-budget-usd") + 1]) for x in by("C1")]
            assert caps[0] == 2.7 and caps[1] == 1.0 and caps[6] == 2.0, caps
            tok = json.loads((t / "full" / "tokens.json").read_text())
            assert tok["total"]["runs"] == len(c) and abs(tok["total"]["cost_usd"] - 0.5 * len(c)) < 1e-6, tok["total"]
            assert "| C1 | mergeable | $3.50 |" in (t / "full" / "summary.md").read_text()
            for p in (t / "full").rglob("*.md"):
                assert not CYRILLIC.search(p.read_text()), p

            n = len(calls())
            assert fix("full", "C1,C4,C5,C7,C8", "--go").run() == 2
            assert fix("full", "C1,C4,C5,C7,C8", "--go", "--resume").run() == 3 and len(calls()) == n, "a resume of a finished round ran something"
            (t / "full" / "C1" / "06-review.md").unlink()
            assert fix("full", "C1", "--go", "--resume").run() == 0
            again = calls()[n:]
            assert [(x["cid"], x["step"]) for x in again] == [("C1", "review")], again
            assert fix("only", "C5", "--go", "--only", "plan").run() == 3 and [x["step"] for x in calls()[n + 1:]] == ["plan"], calls()[n + 1:]
            assert json.loads((t / "only" / "C5" / "status.json").read_text())["status"] == "incomplete"
        except BaseException:
            quiet.close()
            sys.stderr.write(noise.getvalue()[-3000:])
            raise
        finally:
            quiet.close()
            for k, v in env_keep.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
    print("fix self-test: ok")
    return 0


def main(argv: list[str], **paths) -> int:
    """Plan, run, or self-test a fix round."""
    a = parser().parse_args(argv)
    if a.self_test:
        return self_test()
    if not a.hunt or not a.classes:
        parser().error("--hunt and --classes are required")
    try:
        f = Fix(a, **paths)
        if not a.go:
            print("\n".join(f.plan()))
            print("plan: dry run; pass --go to start the agents (they write code, push branches and open PRs)")
            return 0
        return f.run()
    except FixError as e:
        print(f"fix: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
