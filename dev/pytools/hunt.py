"""Deterministic bug hunt: architect lenses, one sweeper per predicate, a pure merge, one verifier pass.

    python -m dev.pytools.hunt --lens NAME|TEXT [--lenses a,b,c] [--rules TITLE ...] [--claims-from FILE] [--out DIR]
                               [--parallel 3] [--budget-usd N] [--verify-top K] [--dry-run] [--resume] [--only STEP]
    python -m dev.pytools.hunt --auto [--auto-lenses 2] [--dry-run] [--resume]
    python -m dev.pytools.hunt --close CLASS_ID --pr N [--out DIR]
    python -m dev.pytools.hunt --list-lenses | --self-test

Steps: setup (gate wait, read-only worktree of origin/main), map, lens, sweep, merge, verify, report.
A step is skipped when its output file exists; a step that runs drops the outputs of every later step.
A preset lens (--list-lenses) brings its default rules; free text brings none. --lenses runs several lenses over one
map, one merge, one verify. --auto takes the next lenses in rotation plus the rules whose seams changed since the last
auto round (target/hunt/state.json), minus closed classes. --close records a class as fixed by a PR.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import datetime as dt
import hashlib
import inspect
import io
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

from dev.pytools import seams

LENSES: dict[str, tuple[str, tuple[str, ...]]] = {
    "hot-spots": ("hot spots: the files that change most and are coupled most (git log churn x fan-in), and the decisions they hold",
                  ("refusal sites", "operator/source text reads", "type and size boundaries", "typed error classification")),
    "cdc": ("CDC: snapshot, stream position, resume and the hand-off between them; what a stop, a restart or an empty batch leaves in state",
            ("state/ledger/progress writes", "state store reads", "stop/progress proxy", "run liveness", "empty-source branches", "journal/ledger context")),
    "load-and-types": ("load and types: warehouse loads, type and size mapping, overwrite/replace, and what a second client reads back",
                       ("warehouse overwrite/replace", "type and size boundaries", "state-vs-destination checks", "destination reads (cost per kind)", "result layout reads", "seed load and read-back")),
    "runners": ("runners: workers, fan-in, retries, panics and how a failure reaches the exit code",
                ("worker failure collection", "failure stringified into a list", "retries/backoff", "typed error classification", "error text matching", "panic prevention and catch sites", "value-range planning")),
    "state-config-cli": ("state, config and CLI: locks, leases, progress records, generated config, catalog reads and the CLI entry points",
                         ("lock acquire/release", "lock keys", "state/ledger/progress writes", "journal/ledger context", "run liveness", "generated config", "write-location upgrade",
                          "stored-artifact guard refusals", "catalog reads", "order/choice near a catalog read", "credential mask sites", "text cut sites")),
    "harness": ("harness: release oracle, ledgers, ratchets, live tests, seeds and the guards that can be fooled",
                ("guard reads its own history", "test-hook points", "child-process polling (race harness)", "silent return in live tests without skip_live", "Command::new outside tests/common",
                 "ratchet pins", "mutants exclusions", "accepted-red ledgers", "seeded defects corpus", "env reads (tests/dev)", "seed files", "seed load and read-back")),
    "duplicates": ("duplicates: the same decision derived in N places (engine copies, repeated classification, parallel match arms) that can drift apart",
                   ("source engine copies (match arms)", "warehouse targets (match arms)", "object-store destinations (match arms)", "state backends (match arms)", "typed error classification",
                    "error text matching", "catalog order seam", "credential mask sites")),
    "engine-drift": ("engine drift: rows of the seam map where a concept present on some engines is missing on another engine",
                     ("source engine copies (files)", "source engine copies (match arms)", "warehouse targets (files)", "warehouse targets (match arms)", "object-store destinations (files)",
                      "object-store destinations (match arms)", "state backends (functions)", "state backends (match arms)")),
}
LENS_ORDER = tuple(LENSES)

ROOT = Path(__file__).resolve().parents[2]
GIT = "/usr/bin/git"
GATE_WAIT = 'until ! /bin/ps -axo command | /usr/bin/grep -E "[d]ev\\.release_oracle( |$)" | /usr/bin/grep -qv -- "--self-test"; do sleep 60; done'
STEPS = ("setup", "map", "lens", "sweep", "merge", "verify", "report")
VERDICTS = ("holds", "violated", "n/a")
NO_ENGINE = {"", "n/a", "-", "any", "none", "all"}
ENGINE_WORDS = ("engine copies", "targets", "destinations", "state backends")
REFUTED_PREMISE = "REFUTED-PREMISE"
SITE_CAP = 80
DRIFT_CAP = 40
RUN_TIMEOUT_S = 3600
USAGE_TIMEOUT_S = 90
LENS_SHARE = VERIFY_SHARE = 0.15
LENS_CAP_SHARE = 0.4
REASK_SHARE = 0.25
SWEEP_FLOOR_USD = 1.0
CLAIMS_PER_LENS = 5
USAGE_KEYS = ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
PERMISSION_MODE = "dontAsk"
ALLOW = [
    "Read", "Grep", "Glob", "Skill", "Read(~/.claude/rivet-*.md)", "Read(~/.claude/skills/**)", "mcp__code-review-graph",
    "Bash(/usr/bin/grep *)", "Bash(grep *)", "Bash(rg *)", "Bash(sed -n *)", "Bash(head *)", "Bash(tail *)", "Bash(wc *)",
    "Bash(ls *)", "Bash(cat *)", "Bash(/usr/bin/git grep *)", "Bash(/usr/bin/git log *)", "Bash(/usr/bin/git show *)",
    "Bash(/usr/bin/git blame *)", "Bash(git grep *)", "Bash(git log *)", "Bash(git show *)", "Bash(git blame *)",
    "Bash(python3 -m dev.pytools.seams *)", "Bash(python -m dev.pytools.seams *)",
]
DENY = ["Edit", "Write", "NotebookEdit", "Agent", "WebFetch", "WebSearch"]
SWEEP_SCHEMA = (
    '{"predicate": "...", "seam": "...", "premise": {"holds": true, "evidence": "one command + output line, or file:line quoted"}, '
    '"sites": [{"file": "src/x.rs", "line": 12, "engine": "mysql", "verdict": "holds|violated|n/a", "evidence": "command + output line, or file:line quoted", '
    '"proven": true}], "summary": "..."}'
)
WORKTREE_NOTE = (
    "Your cwd {wt} is a detached read-only worktree of origin/main that dev/pytools/hunt.py created for this run; "
    "it is your worktree under the base file's workspace rule. Do not create another worktree, edit no file, start no "
    "subagent, run no cargo, make, docker or test. Your final message is saved by hunt.py to {out}; write no file yourself."
)
ENGLISH = "Write your whole reply in English, whatever language other files or messages use; quoted code and identifiers stay as they are."
USAGE_HINT = "hunt: usage could not be read non-interactively; run /usage in Claude Code before and after this round to calibrate"


class HuntError(Exception):
    """A hunt step that cannot go on."""


@dataclass
class Lens:
    """One architect lens: its name, its text and its default rule titles."""

    name: str
    text: str
    titles: list[str]
    seams: tuple[str, ...] = ()


@dataclass
class Predicate:
    """One sweep: the predicate, the rule text it comes with, its seams, its site list and the lenses that asked for it."""

    n: int
    text: str
    origin: str
    rule_title: str
    rule_body: str
    seams: list[str]
    sites: list[str]
    mapped: int = -1
    lenses: list[str] = field(default_factory=list)
    cited: list[str] = field(default_factory=list)


def slug(text: str, cap: int = 40) -> str:
    """A short file-name form of a lens."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:cap].strip("-") or "hunt"


def lens_titles(names: tuple[str, ...]) -> list[str]:
    """The rule titles of the named seams in dev.pytools.seams.SEAMS, in SEAMS order, once each."""
    out: dict[str, str] = {}
    for s in seams.SEAMS:
        if s.name in names:
            for r in s.rules:
                out.setdefault(seams.norm(r), r)
    return list(out.values())


def resolve_lens(spec: str) -> Lens:
    """A preset by name, else free text with no default rules."""
    spec = spec.strip()
    if spec in LENSES:
        text, names = LENSES[spec]
        return Lens(spec, text, lens_titles(names), names)
    return Lens(slug(spec), spec, [])


def lens_table() -> list[str]:
    """--list-lenses: name, text and default rules of every preset."""
    out = []
    for name in LENS_ORDER:
        lens = resolve_lens(name)
        out += [f"{name}: {lens.text}", f"    seams: {', '.join(lens.seams)}", f"    rules ({len(lens.titles)}): {'; '.join(lens.titles) or '-'}"]
    return out


def parse_rules(text: str) -> dict[str, tuple[str, str]]:
    """Normalized title -> (title, body) for every `## ` section of the process-rules file."""
    parts = re.split(r"^## (.+)$", text, flags=re.M)
    return {seams.norm(t): (t.strip(), b.strip()) for t, b in zip(parts[1::2], parts[2::2])}


def extract_claims(text: str) -> list[str]:
    """The KEY CLAIM sentences of an architect report, in order, without markdown emphasis."""
    out = []
    for m in re.finditer(r"KEY CLAIM\W*?[:\-–—]\s*(.+)", text):
        claim = m.group(1).strip().strip("*_ ").strip()
        if claim and claim not in out:
            out.append(claim)
    return out


def claim_files(claim: str) -> list[str]:
    """Repo paths a claim cites."""
    return list(dict.fromkeys(m.group(1).lstrip("./") for m in re.finditer(r"([\w./-]+\.(?:rs|py|sql|ya?ml|toml|sh))(?::\d+)?", claim)))


def engine_bound(entry: dict) -> bool:
    """Whether a seam is engine-bound: its `engines` field when the map has one, else its name."""
    if "engines" in entry:
        return bool(entry["engines"])
    return any(w in entry.get("name", "") for w in ENGINE_WORDS)


def load_map(path: Path) -> list[dict]:
    """The seam entries of seams.json, whether it holds a list or a {"seams": [...]} object, each with `engine_bound`."""
    data = json.loads(path.read_text())
    entries = data["seams"] if isinstance(data, dict) else data
    return [{**e, "engine_bound": engine_bound(e)} for e in entries]


def site_path(site: dict) -> str:
    """A seam site's file."""
    return site.get("file") or site.get("path") or ""


def seams_for_rule(title: str) -> list[str]:
    """The seam names whose row in dev.pytools.seams.SEAMS serves this rule."""
    key = seams.norm(title)
    return [s.name for s in seams.SEAMS if key in {seams.norm(r) for r in s.rules}]


def seams_for_claim(claim: str, entries: list[dict]) -> list[str]:
    """The seams with sites in the files the claim cites, most sites first, at most three."""
    files = set(claim_files(claim))
    hits = [(sum(site_path(s) in files for s in e.get("sites", [])), e["name"]) for e in entries]
    return [name for count, name in sorted(hits, key=lambda h: -h[0]) if count][:3]


def site_lines(entries: list[dict], names: list[str], cap: int = SITE_CAP) -> list[str]:
    """`- path:line [kind] text` for the named seams, product sites first, `cap` per seam."""
    order = {"prod": 0, "test": 1, "dev": 2}
    out = []
    for e in entries:
        if e["name"] not in names:
            continue
        rows = sorted(e.get("sites", []), key=lambda s: (order.get(s.get("kind"), 3), site_path(s), s.get("line", 0)))
        out.append(f"### {e['name']}: {len(rows)} sites{f' ({cap} shown)' if len(rows) > cap else ''}")
        out += [f"- {site_path(s)}:{s.get('line')} [{s.get('kind', '?')}] {str(s.get('text', ''))[:100]}" for s in rows[:cap]]
    return out


def mapped_sites(entries: list[dict] | None, names: list[str]) -> int:
    """Map sites over the named seams; -1 before the map exists."""
    if entries is None:
        return -1
    return sum(len(e.get("sites", [])) for e in entries if e["name"] in names)


def build_predicates(titles: list[str], rules: dict[str, tuple[str, str]], claims: list[tuple[str, str]], entries: list[dict] | None,
                     title_lenses: dict[str, list[str]] | None = None) -> list[Predicate]:
    """One predicate per rule title, then one per distinct claim (with every lens that made it), numbered from 1."""
    out: list[Predicate] = []
    for title in titles:
        if seams.norm(title) not in rules:
            raise HuntError(f"no process rule titled {title!r} in the rules file")
        name, body = rules[seams.norm(title)]
        names = seams_for_rule(name)
        lenses = (title_lenses or {}).get(seams.norm(title), [])
        out.append(Predicate(len(out) + 1, name, "rule", name, body, names, site_lines(entries, names) if entries else [], mapped_sites(entries, names), list(lenses)))
    by_text: dict[str, Predicate] = {}
    for lens, claim in claims:
        if claim in by_text:
            if lens not in by_text[claim].lenses:
                by_text[claim].lenses.append(lens)
            continue
        names = seams_for_claim(claim, entries) if entries else []
        sites = site_lines(entries, names) if entries else []
        sites += [f"- {f} [cited by the claim]" for f in claim_files(claim)]
        p = Predicate(len(out) + 1, claim, "claim", "", "", names, sites, mapped_sites(entries, names), [lens], claim_files(claim))
        by_text[claim] = p
        out.append(p)
    return out


def primary_seam(p: Predicate, entries: list[dict] | None) -> str:
    """The seam a sweep reports under: its first seam that holds sites, else its first, else claim-<n>."""
    counts = {e["name"]: e.get("count", len(e.get("sites", []))) for e in entries or []}
    return next((n for n in p.seams if counts.get(n)), p.seams[0] if p.seams else f"claim-{p.n}")


def changed_rules(entries: list[dict], closed: list[dict], cap: int) -> tuple[list[str], list[str], int]:
    """Rule titles whose seams have changed sites outside closed (seam, file) pairs, most changed first: kept, dropped by the cap, sites skipped as closed."""
    shut = {(c["seam"], f) for c in closed for f in c.get("files", [])}
    score: dict[str, int] = {}
    skipped = 0
    for e in entries:
        live = [s for s in e.get("sites", []) if s.get("changed", True)]
        n = sum((e["name"], site_path(s)) not in shut for s in live)
        skipped += len(live) - n
        if n:
            for r in e.get("rules", []):
                score[r] = score.get(r, 0) + n
    ranked = sorted(score, key=lambda r: (-score[r], r))
    return ranked[:cap], ranked[cap:], skipped


def drift_rows(entries: list[dict], names: tuple[str, ...], cap: int = DRIFT_CAP) -> list[str]:
    """Concept rows of the named seams with a missing engine."""
    out = []
    for e in entries:
        if e["name"] in names:
            for c in e.get("concepts") or []:
                if c.get("missing"):
                    out.append(f"- {e['name']}: {c['concept']} present on {', '.join(c['present'])}; missing on {', '.join(c['missing'])}")
    return out[:cap]


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


def validate_sweep(obj: object) -> list[str]:
    """What is wrong with a sweep block; empty when it is gradable."""
    if not isinstance(obj, dict):
        return ["the block is not an object"]
    errs = [f"`{k}` is missing or not a string" for k in ("predicate", "seam", "summary") if not isinstance(obj.get(k), str)]
    prem = obj.get("premise")
    if not isinstance(prem, dict) or not isinstance(prem.get("holds"), bool):
        errs.append("`premise.holds` is missing or not true or false")
    elif not isinstance(prem.get("evidence"), str) or not prem["evidence"].strip():
        errs.append("`premise.evidence` is empty")
    sites = obj.get("sites")
    if not isinstance(sites, list):
        return errs + ["`sites` is missing or not a list"]
    for i, s in enumerate(sites):
        if not isinstance(s, dict):
            errs.append(f"sites[{i}] is not an object")
            continue
        if not isinstance(s.get("file"), str) or not s["file"]:
            errs.append(f"sites[{i}].file is missing")
        if not isinstance(s.get("line"), int) or isinstance(s.get("line"), bool) or s["line"] < 1:
            errs.append(f"sites[{i}].line is not a positive integer")
        if not isinstance(s.get("engine"), str):
            errs.append(f"sites[{i}].engine is not a string")
        if s.get("verdict") not in VERDICTS:
            errs.append(f"sites[{i}].verdict is not one of {'|'.join(VERDICTS)}")
        if not isinstance(s.get("evidence"), str) or not s["evidence"].strip():
            errs.append(f"sites[{i}].evidence is empty")
        if not isinstance(s.get("proven"), bool):
            errs.append(f"sites[{i}].proven is not true or false")
    return errs


def validate_verdicts(obj: object, ids: list[str]) -> list[str]:
    """What is wrong with a verifier block; empty when every class id has a boolean `holds`."""
    rows = obj.get("verdicts") if isinstance(obj, dict) else None
    if not isinstance(rows, list):
        return ["`verdicts` is missing or not a list"]
    seen = {r.get("class"): r for r in rows if isinstance(r, dict)}
    errs = [f"no verdict for class {i}" for i in ids if i not in seen]
    errs += [f"class {i}: `holds` is not true or false" for i in ids if i in seen and not isinstance(seen[i].get("holds"), bool)]
    return errs


def severity(path: str) -> int:
    """Severity hint of a violated site: product 3, harness 2, anything else 1."""
    return 3 if path.startswith("src/") else 2 if path.startswith(("tests/", "dev/")) else 1


def site_rank(s: dict) -> tuple:
    """Order of strength for one site report: violated, then proven, then holds."""
    return (s["verdict"] == "violated", s["proven"], s["verdict"] == "holds")


def merge(blocks: list[dict], bound: set[str] | frozenset = frozenset(), closed: list[dict] = ()) -> list[dict]:
    """Classes by seam: sites deduped by file:line (strongest wins), premises, lenses that confirm, ONE-OFFs, closed classes, ranked."""
    by: dict[str, dict] = {}
    cited: list[tuple[str, set[str]]] = []
    for b in blocks:
        c = by.setdefault(b["seam"], {"seam": b["seam"], "predicates": [], "summaries": [], "sites": {}, "lenses": [], "claim_lenses": [], "held": 0, "refuted_premise": []})
        if b["predicate"] not in c["predicates"]:
            c["predicates"].append(b["predicate"])
        lenses = list(b.get("lenses") or [])
        c["lenses"] += [x for x in lenses if x not in c["lenses"]]
        prem = b.get("premise") or {}
        if prem.get("holds") is False:
            c["refuted_premise"].append({"predicate": b["predicate"], "evidence": prem.get("evidence", "")})
            continue
        c["held"] += 1
        if b.get("origin") == "claim":
            c["claim_lenses"] += [x for x in lenses if x not in c["claim_lenses"]]
            cited += [(x, set(b.get("cited") or [])) for x in lenses]
        c["summaries"].append(b["summary"])
        for s in b["sites"]:
            key = f"{s['file']}:{s['line']}"
            if key not in c["sites"] or site_rank(s) > site_rank(c["sites"][key]):
                c["sites"][key] = dict(s)
    out = []
    for c in by.values():
        sites = sorted(c["sites"].values(), key=lambda s: (s["file"], s["line"]))
        bad = [s for s in sites if s["verdict"] == "violated"]
        bad_files = {s["file"] for s in bad}
        proven = sum(s["proven"] for s in bad)
        engines = sorted({s["engine"].strip().lower() for s in sites} - NO_ENGINE)
        is_bound = c["seam"] in bound
        table = len(engines) >= 2
        sev = max((severity(s["file"]) for s in bad), default=0)
        one_off = bool(bad) and ((len(bad) < 2 or not table) if is_bound else len(bad) == 1)
        confirmed = sorted(set(c["claim_lenses"]) | {x for x, files in cited if files & bad_files})
        confirmed = confirmed if len(confirmed) >= 2 else []
        shut = next((x for x in closed if x["seam"] == c["seam"] and bad_files and bad_files <= set(x.get("files", []))), None)
        if not c["held"]:
            status = REFUTED_PREMISE
        elif shut:
            status = f"closed (PR #{shut['pr']})"
        elif not bad:
            status = "clean"
        elif one_off:
            status = "ONE-OFF (needs per-engine table)" if is_bound and not table else "ONE-OFF"
        else:
            status = "class"
        live = c["held"] and not shut
        score = len(bad) * sev * (1 + proven) * max(1, len(confirmed)) if live else 0
        out.append({**{k: v for k, v in c.items() if k != "held"}, "sites": sites, "violated": len(bad), "proven_violated": proven, "engines": engines,
                    "engine_bound": is_bound, "per_engine_table": table, "severity": sev, "score": score, "one_off": one_off and live,
                    "confirmed_by": confirmed, "status": status, "files": sorted(bad_files)})
    out.sort(key=lambda c: (-c["score"], c["seam"]))
    for i, c in enumerate(out, 1):
        c["id"] = f"C{i}"
    return out


def verifiable(c: dict) -> bool:
    """A class the verifier may get: a proven violated site, premise held, not closed."""
    return c["proven_violated"] >= 1 and c["status"] != REFUTED_PREMISE and not c["status"].startswith("closed")


def verify_set(classes: list[dict], k: int) -> list[dict]:
    """The classes the verifier gets: every verifiable class, best score first, at most k."""
    return [c for c in classes if verifiable(c)][:k]


def unverified(classes: list[dict], k: int) -> list[tuple[dict, str]]:
    """Violated classes the verifier does not get, each with why."""
    chosen = {c["id"] for c in verify_set(classes, k)}
    out = []
    for c in classes:
        if c["violated"] and c["id"] not in chosen and c["status"] != REFUTED_PREMISE and not c["status"].startswith("closed"):
            out.append((c, f"below --verify-top {k}" if verifiable(c) else "no proven violated site"))
    return out


def strongest(c: dict) -> list[dict]:
    """The strongest violated site of a class and one other site."""
    ranked = sorted(c["sites"], key=lambda s: (not (s["verdict"] == "violated"), not s["proven"], -severity(s["file"]), s["file"], s["line"]))
    return ranked[:2]


def site_table(sites: list[dict]) -> list[str]:
    """A Markdown table of site reports."""
    rows = ["| site | engine | verdict | proven | evidence |", "|---|---|---|---|---|"]
    rows += [f"| `{s['file']}:{s['line']}` | {s['engine']} | {s['verdict']} | {'yes' if s['proven'] else 'no'} | {s['evidence'][:160].replace('|', '/')} |" for s in sites]
    return rows


def render_classes(classes: list[dict]) -> str:
    """classes.md: one section per class, ranked."""
    out = ["# Classes", "", "| id | seam | status | violated | proven | severity | engines | engine-bound | confirmed-by | score |", "|---|---|---|---|---|---|---|---|---|---|"]
    out += [f"| {c['id']} | {c['seam']} | {c['status']} | {c['violated']} | {c['proven_violated']} | {c['severity']} | {', '.join(c['engines']) or '-'} | "
            f"{'yes' if c['engine_bound'] else 'no'} | {', '.join(c['confirmed_by']) or '-'} | {c['score']} |" for c in classes]
    for c in classes:
        out += ["", f"## {c['id']} {c['seam']} ({c['status']})", "", *[f"- predicate: {p}" for p in c["predicates"]]]
        out += [f"- premise refuted for: {r['predicate']}: {r['evidence']}" for r in c["refuted_premise"]]
        out += ["", *site_table(c["sites"])]
    return "\n".join(out) + "\n"


def usage_of(result: dict | None) -> dict:
    """Token counts and cost of one `claude -p --output-format json` result."""
    u = (result or {}).get("usage") or {}
    return {**{k: int(u.get(k) or 0) for k in USAGE_KEYS}, "cost_usd": float((result or {}).get("total_cost_usd") or 0.0)}


def parse_usage(text: str) -> dict | None:
    """The `<label>: N% used` lines of the /usage text, or None when it has none."""
    pct = {m.group(1).strip(): float(m.group(2)) for m in re.finditer(r"^([^:\n]+): (\d+(?:\.\d+)?)% used", text, re.M)}
    return {"percent": pct, "text": text[:1500]} if pct else None


def tokens(records: list[dict], timing: dict[str, float], lens_of: dict[str, list[str]] | None = None) -> dict:
    """Per-step, per-lens and total tokens, cost and wall time over every recorded run."""
    zero = lambda: {"runs": 0, **{k: 0 for k in USAGE_KEYS}, "cost_usd": 0.0, "wall_s": 0.0}
    steps = {s: zero() for s in STEPS}
    lenses: dict[str, dict] = {}
    for r in records:
        st = steps[r["step"]]
        st["runs"] += 1
        u = usage_of(r.get("result"))
        for k, v in u.items():
            st[k] += v
        owners = (lens_of or {}).get(r["name"].removesuffix("-reask"), [])
        for x in owners:
            row = lenses.setdefault(x, {"runs": 0, "cost_usd": 0.0, "lens_usd": 0.0, "sweep_usd": 0.0})
            row["runs"] += 1
            row["cost_usd"] += u["cost_usd"] / len(owners)
            row[f"{r['step']}_usd"] = row.get(f"{r['step']}_usd", 0.0) + u["cost_usd"] / len(owners)
    for s, w in timing.items():
        steps[s]["wall_s"] = round(w, 1)
    total = zero()
    for st in steps.values():
        for k in total:
            total[k] += st[k]
    for d in (*steps.values(), total):
        d["cost_usd"] = round(d["cost_usd"], 4)
        d["wall_s"] = round(d["wall_s"], 1)
    for row in lenses.values():
        for k in row:
            if k.endswith("usd"):
                row[k] = round(row[k], 4)
    runs = [{k: r.get(k) for k in ("name", "step", "agent", "rc", "wall_s", "budget_usd", "session_id")} | {"cost_usd": usage_of(r.get("result"))["cost_usd"]} for r in records]
    return {"steps": steps, "total": total, "lenses": lenses, "runs": runs}


def usage_line(usage: dict | None) -> str:
    """One line for the /usage reading before and after the round."""
    show = lambda u: "; ".join(f"{k} {v:g}%" for k, v in u["percent"].items()) if u else "not read"
    if not usage or (not usage.get("before") and not usage.get("after")):
        return "Usage: not read non-interactively; run /usage before and after a round to calibrate."
    return f"Usage before: {show(usage.get('before'))}. After: {show(usage.get('after'))}."


def render_summary(title: str, classes: list[dict], verdicts: dict[str, dict], invalid: list[str], tok: dict, k: int, no_sites: list[dict]) -> str:
    """summary.md: costs, accepted, refuted, refuted premises, unverified, ONE-OFFs, closed, unmapped predicates, clean seams, invalid sweeps."""
    acc = [c for c in classes if verdicts.get(c["id"], {}).get("holds") is True]
    ref = [c for c in classes if c["id"] in verdicts and verdicts[c["id"]].get("holds") is not True]
    prem = [c for c in classes if c["refuted_premise"]]
    rest = unverified(classes, k)
    out = [f"# Hunt: {title}", "", f"Cost ${tok['total']['cost_usd']:.2f}, {tok['total']['runs']} claude runs, wall {tok['total']['wall_s']:.0f}s (tokens.json).", ""]
    shared = tok["total"]["cost_usd"] - sum(x["cost_usd"] for x in tok.get("lenses", {}).values())
    out += [f"- lens {n}: ${x['cost_usd']:.2f} (architect ${x.get('lens_usd', 0):.2f}, sweeps ${x.get('sweep_usd', 0):.2f}, {x['runs']} runs)" for n, x in tok.get("lenses", {}).items()]
    out += [f"- shared (verify, unattributed): ${max(shared, 0):.2f}", "", usage_line(tok.get("usage")), ""]
    out += [f"Models: {', '.join(f'{k}={v}' for k, v in tok['models'].items())}; other steps use the agent's own model.", ""] if tok.get("models") else []
    out += ["## Accepted (verifier: holds)", ""] + (["none"] if not acc else [])
    for c in acc:
        conf = f" (confirmed-by: {', '.join(c['confirmed_by'])})" if c["confirmed_by"] else ""
        out += [f"### {c['id']} {c['seam']}{conf}", "", f"- status: {c['status']}", *[f"- predicate: {p}" for p in c["predicates"]],
                f"- verifier: {verdicts[c['id']].get('evidence', '')}", "", *site_table(c["sites"]), ""]
    out += ["", "## Refuted (verifier)", ""] + [f"- {c['id']} {c['seam']} ({c['status']}): {verdicts[c['id']].get('evidence', 'no verdict')}" for c in ref] + (["none"] if not ref else [])
    out += ["", f"## {REFUTED_PREMISE} (sweeper: the rule's premise does not hold on this tree; no verifier run)", ""]
    out += [f"- {c['id']} {c['seam']} ({c['status']}): {r['predicate']}: {r['evidence']}" for c in prem for r in c["refuted_premise"]] + (["none"] if not prem else [])
    out += ["", "## Not verified", ""] + [f"- {c['id']} {c['seam']} ({c['status']}): {why}; {c['violated']} violated, {c['proven_violated']} proven, score {c['score']}" for c, why in rest]
    out += ["none"] if not rest else []
    one = [c for c in classes if c["one_off"]]
    out += ["", "## ONE-OFF", ""] + [f"- {c['id']} {c['seam']}: {c['status']}; {c['violated']} violated; engines {', '.join(c['engines']) or '-'}; "
                                      f"verifier {'holds' if verdicts.get(c['id'], {}).get('holds') is True else 'refuted' if c['id'] in verdicts else 'not run'}" for c in one]
    out += ["none"] if not one else []
    shut = [c for c in classes if c["status"].startswith("closed")]
    out += ["", "## Closed earlier (target/hunt/state.json)", ""] + [f"- {c['id']} {c['seam']}: {c['status']}; files {', '.join(c['files'])}" for c in shut] + (["none"] if not shut else [])
    out += ["", f"## No sites: claim not mapped to a seam ({len(no_sites)}, not swept)", ""] + [f"- P{p['n']} {p['origin']} ({', '.join(p['lenses']) or '-'}): {p['text']}" for p in no_sites]
    out += ["none"] if not no_sites else []
    clean = [c for c in classes if c["status"] == "clean"]
    out += ["", "## Clean (no violated site)", ""] + [f"- {c['seam']} ({len(c['sites'])} sites)" for c in clean] + (["none"] if not clean else [])
    out += ["", "## Invalid sweeps (no gradable JSON block after one re-ask)", ""] + [f"- {i}" for i in invalid] + (["none"] if not invalid else [])
    return "\n".join(out) + "\n"


def state_path(root: Path) -> Path:
    """The auto-round state file."""
    return root / "target" / "hunt" / "state.json"


def load_state(root: Path) -> dict:
    """The auto-round state, with defaults when the file is absent."""
    p = state_path(root)
    got = json.loads(p.read_text()) if p.is_file() else {}
    return {"last_rev": None, "last_lens": -1, "last_out": None, "closed": [], **got}


def save_state(root: Path, state: dict) -> None:
    """Write the auto-round state."""
    p = state_path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, indent=1) + "\n")


def close_class(root: Path, cid: str, pr: int, out: Path | None) -> int:
    """Record class CID of a hunt OUT (default: the last auto round) as closed by PR, so later rounds skip its seam and files."""
    state = load_state(root)
    out = out or (Path(state["last_out"]) if state["last_out"] else None)
    if not out or not (out / "classes.json").is_file():
        print(f"hunt: no classes.json to close {cid} in; pass --out DIR", file=sys.stderr)
        return 2
    c = next((x for x in json.loads((out / "classes.json").read_text())["classes"] if x["id"] == cid), None)
    if not c or not c.get("files"):
        print(f"hunt: {out} has no class {cid} with a violated file", file=sys.stderr)
        return 2
    row = {"class": cid, "seam": c["seam"], "files": c["files"], "pr": pr, "date": f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%d}", "out": str(out)}
    state["closed"] = [x for x in state["closed"] if (x["seam"], x["files"]) != (row["seam"], row["files"])] + [row]
    save_state(root, state)
    hit = close_in_history(out, cid, pr)
    print(f"hunt: closed {cid} {c['seam']} ({', '.join(c['files'])}) by PR #{pr} -> {state_path(root)}, {hit} history records -> {history_path()}")
    return 0


HISTORY_ENV = "RIVET_HUNT_HISTORY"


def history_path() -> Path:
    """The hunt history journal: $RIVET_HUNT_HISTORY, else ~/.claude/hunt-history/rounds.jsonl; never in the repo."""
    got = os.environ.get(HISTORY_ENV)
    return Path(got).expanduser() if got else Path.home() / ".claude" / "hunt-history" / "rounds.jsonl"


def fingerprint(seam: str, files: list[str]) -> str:
    """A stable hash of a seam and its sorted violated-site files."""
    return hashlib.sha256("\n".join([seam, *sorted(set(files))]).encode()).hexdigest()[:16]


def read_history(path: Path | None = None) -> list[dict]:
    """Every record of the history journal, in append order."""
    p = path or history_path()
    return [json.loads(x) for x in p.read_text().splitlines() if x.strip()] if p.is_file() else []


def history_key(r: dict) -> tuple:
    """What makes a history record unique: date, round (OUT name), class and fingerprint."""
    return (r["date"], r["round"], r["class"], r["fingerprint"])


def class_status(c: dict, verdicts: dict[str, dict]) -> str:
    """One word for a class in history: accepted, refuted, refuted-premise, closed, one-off, not-verified or clean."""
    if c["status"] == REFUTED_PREMISE:
        return "refuted-premise"
    if c["status"].startswith("closed"):
        return "closed"
    if c["id"] in verdicts:
        return "accepted" if verdicts[c["id"]].get("holds") is True else "refuted"
    if not c.get("violated"):
        return "clean"
    return "one-off" if c.get("one_off") or c["status"].startswith("ONE-OFF") else "not-verified"


def round_records(out: Path, lenses: list[str] | None = None, rev: str | None = None, date: str | None = None) -> list[dict]:
    """One history record per class of a finished hunt OUT, from classes.json, verdicts.md, tokens.json and the map."""
    if not (out / "classes.json").is_file():
        raise HuntError(f"{out} has no classes.json; the round did not reach its merge")
    classes = json.loads((out / "classes.json").read_text())["classes"]
    obj, err = json_block((out / "verdicts.md").read_text()) if (out / "verdicts.md").is_file() else (None, "none")
    rows = obj.get("verdicts", []) if isinstance(obj, dict) and not err else []
    verdicts = {r["class"]: r for r in rows if isinstance(r, dict) and isinstance(r.get("class"), str)}
    tok = json.loads((out / "tokens.json").read_text()) if (out / "tokens.json").is_file() else {}
    per_lens = {k: v.get("cost_usd", 0.0) for k, v in (tok.get("lenses") or {}).items()}
    if not lenses:
        title = (out / "summary.md").read_text().splitlines()[0].removeprefix("# Hunt:").strip() if (out / "summary.md").is_file() else out.name
        lenses = list(per_lens) or [title.removesuffix(" (auto)")]
    total = float((tok.get("total") or {}).get("cost_usd") or 0.0)
    if not per_lens:
        per_lens = {x: round(total / len(lenses), 4) for x in lenses}
    if not rev:
        plan = out / "plan.json"
        m = out / "map" / "seams.json"
        rev = (json.loads(plan.read_text()).get("rev") if plan.is_file() else None) or (json.loads(m.read_text()).get("rev") if m.is_file() else None)
    if not date:
        got = re.match(r"\d{4}-\d{2}-\d{2}", out.name)
        date = got.group(0) if got else f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%d}"
    out_rows = []
    for c in classes:
        files = c.get("files") or sorted({s["file"] for s in c["sites"] if s["verdict"] == "violated"})
        out_rows.append({"date": date, "rev": rev, "out": str(out), "round": out.name, "lenses": lenses, "class": c["id"], "seam": c["seam"],
                         "fingerprint": fingerprint(c["seam"], files), "files": files, "status": class_status(c, verdicts),
                         "class_lenses": c.get("lenses") or lenses, "confirmed_by": c.get("confirmed_by", []),
                         "sites": {"swept": len(c["sites"]), "violated": c.get("violated", 0), "proven": c.get("proven_violated", 0)},
                         "cost": {"round_usd": total, "lenses": per_lens}, "models": tok.get("models") or {}, "pr": None})
    return out_rows


def append_history(records: list[dict], path: Path | None = None) -> int:
    """Append the records not yet in the journal (by date, round, class and fingerprint); how many were added."""
    p = path or history_path()
    seen = {history_key(r) for r in read_history(p)}
    new = [r for r in records if history_key(r) not in seen]
    if new:
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a") as f:
            f.writelines(json.dumps(r) + "\n" for r in new)
    return len(new)


def backfill(out: Path) -> int:
    """Add a finished hunt OUT to the history journal, skipping records already there."""
    got = round_records(out.resolve())
    n = append_history(got)
    print(f"hunt: backfill {out}: {n} of {len(got)} class records added -> {history_path()}")
    return 0


def close_in_history(out: Path, cid: str, pr: int) -> int:
    """Set the closing PR on the history records of class CID of OUT; how many were updated."""
    p = history_path()
    rows = read_history(p)
    hit = 0
    for r in rows:
        if r["out"] == str(out) and r["class"] == cid:
            r["pr"] = pr
            hit += 1
    if hit:
        p.write_text("".join(json.dumps(r) + "\n" for r in rows))
    return hit


def parse_models(pairs: list[str]) -> dict[str, str]:
    """--model STEP=MODEL pairs as {step: model}; a step outside lens, sweep and verify is refused."""
    got = {}
    for pair in pairs:
        step, sep, model = pair.partition("=")
        if not sep or step not in ("lens", "sweep", "verify") or not model.strip():
            raise HuntError(f"--model takes STEP=MODEL with STEP one of lens, sweep, verify, got {pair!r}")
        got[step] = model.strip()
    return got


def hunter(name: str, row: dict) -> str:
    """A lens name tagged with the hunter model of its round, when the round set one."""
    m = (row.get("models") or {})
    tag = m.get("lens") or m.get("sweep")
    return f"{name} [{tag}]" if tag else name


def history_report(rows: list[dict]) -> list[str]:
    """--history: yield per lens, repeated holes, lenses that find the same classes, trend per round."""
    if not rows:
        return [f"hunt history: no records in {history_path()}"]
    rounds: dict[str, list[dict]] = {}
    for r in rows:
        rounds.setdefault(r["out"], []).append(r)
    order = sorted(rounds, key=lambda k: (rounds[k][0]["date"], list(rounds).index(k)))
    out = [f"hunt history: {len(rows)} class records, {len(rounds)} rounds ({history_path()})", "", "Yield per lens (accepted classes per $):"]
    lens: dict[str, dict] = {}
    for k in order:
        first = rounds[k][0]
        for x, usd in first["cost"]["lenses"].items():
            row = lens.setdefault(hunter(x, first), {"rounds": 0, "usd": 0.0, "accepted": 0})
            row["rounds"] += 1
            row["usd"] += usd
        for r in rounds[k]:
            if r["status"] == "accepted":
                for x in r["class_lenses"]:
                    lens.setdefault(hunter(x, r), {"rounds": 0, "usd": 0.0, "accepted": 0})["accepted"] += 1
    for x, v in sorted(lens.items(), key=lambda kv: -(kv[1]["accepted"] / kv[1]["usd"] if kv[1]["usd"] else 0)):
        rate = f"{v['accepted'] / v['usd']:.2f}" if v["usd"] else "-"
        out.append(f"- {x}: {v['accepted']} accepted / ${v['usd']:.2f} = {rate} per $ ({v['rounds']} rounds)")
    out += ["", "Repeated holes (same fingerprint in more than one round, or back after a closed PR):"]
    holes = [r for r in rows if r["sites"]["violated"]]
    by_fp: dict[str, list[dict]] = {}
    for r in holes:
        by_fp.setdefault(r["fingerprint"], []).append(r)
    rep = []
    for fp, rs in by_fp.items():
        seen = sorted({r["out"] for r in rs}, key=order.index)
        if len(seen) > 1:
            rep.append(f"- {rs[0]['seam']} [{fp}] in {len(seen)} rounds: {', '.join(Path(s).name for s in seen)}; statuses {', '.join(r['status'] for r in rs)}")
    for c in (r for r in rows if r.get("pr")):
        later = [r for r in holes if order.index(r["out"]) > order.index(c["out"]) and r["seam"] == c["seam"] and set(r["files"]) & set(c["files"])]
        rep += [f"- {r['seam']} back after PR #{c['pr']} ({c['round']} {c['class']} -> {r['round']} {r['class']}): {', '.join(sorted(set(r['files']) & set(c['files'])))}" for r in later]
    out += rep or ["none"]
    out += ["", "Lenses that find the same classes:"]
    pairs: dict[tuple[str, str], set[str]] = {}
    fp_lenses: dict[str, set[str]] = {}
    for r in holes:
        fp_lenses.setdefault(r["fingerprint"], set()).update(r["class_lenses"])
    for fp, ls in fp_lenses.items():
        ls = sorted(ls)
        for i, a in enumerate(ls):
            for b in ls[i + 1:]:
                pairs.setdefault((a, b), set()).add(fp)
    out += [f"- {a} + {b}: {len(fps)} shared classes" for (a, b), fps in sorted(pairs.items(), key=lambda kv: -len(kv[1]))] or ["none"]
    out += ["", "Trend per round:"]
    for k in order:
        rs = rounds[k]
        count = lambda s: sum(r["status"] == s for r in rs)
        usd = rs[0]["cost"]["round_usd"]
        acc = count("accepted")
        out.append(f"- {rs[0]['date']} {str(rs[0]['rev'])[:8]} {hunter(', '.join(rs[0]['lenses']), rs[0])}: ${usd:.2f}, {acc} accepted, {count('refuted')} refuted, "
                   f"{count('refuted-premise')} refuted-premise, {count('one-off')} one-off, {count('not-verified')} not-verified, {count('clean')} clean, "
                   f"{acc / usd if usd else 0:.2f} accepted per $")
    return out


class Hunt:
    """One hunt over OUT: plans the runs, executes the steps, and keeps every output on disk."""

    def __init__(self, a: argparse.Namespace, root: Path = ROOT, gate_cmd: str = GATE_WAIT, rules_path: Path = seams.RULES):
        """Resolve the lenses, paths and plan inputs from the parsed arguments and the auto state."""
        self.a, self.root, self.gate_cmd, self.rules_path = a, root, gate_cmd, rules_path
        self.models = parse_models(a.model)
        self.state = load_state(root)
        if a.auto:
            start = self.state["last_lens"] + 1
            self.lens_index = [(start + i) % len(LENS_ORDER) for i in range(max(1, a.auto_lenses))]
            specs = [LENS_ORDER[i] for i in self.lens_index]
        else:
            specs = [x for x in (a.lenses or "").split(",") if x.strip()] + ([a.lens] if a.lens else [])
        self.lenses = [resolve_lens(x) for x in dict.fromkeys(s.strip() for s in specs)]
        self.slug = "-".join(slug(x.name, 30) for x in self.lenses)[:80].strip("-") or "hunt"
        self.out = (a.out or root / "target" / "hunt" / f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%d}-{self.slug}").resolve()
        common = subprocess.run([GIT, "rev-parse", "--path-format=absolute", "--git-common-dir"], cwd=root, capture_output=True, text=True)
        main = Path(common.stdout.strip()).parent if common.returncode == 0 and common.stdout.strip() else root
        self.wt = main / ".claude" / "worktrees" / f"hunt-{self.slug}"
        self.explicit = [t.strip() for r in a.rules for t in r.split(";") if t.strip()]
        self.made_wt = False
        self.rev: str | None = None
        self.auto_plan: dict | None = None
        self.timing: dict[str, float] = {}

    def path(self, *parts: str) -> Path:
        """A path under OUT."""
        return self.out.joinpath(*parts)

    def title_lenses(self) -> dict[str, list[str]]:
        """Normalized rule title -> the lenses (or --rules / --auto) that asked for it."""
        out: dict[str, list[str]] = {}
        for t in self.explicit:
            out.setdefault(seams.norm(t), []).append("--rules")
        for x in [] if self.a.auto else self.lenses:
            for t in x.titles:
                out.setdefault(seams.norm(t), []).append(x.name)
        for t in (self.auto_plan or {}).get("rules", []):
            out.setdefault(seams.norm(t), []).append("--auto")
        return out

    @property
    def titles(self) -> list[str]:
        """Every rule title to sweep, once each: --rules, the preset lenses' defaults (not in --auto), the auto round's changed rules."""
        lens = [] if self.a.auto else [t for x in self.lenses for t in x.titles]
        got: dict[str, str] = {}
        for t in self.explicit + lens + list((self.auto_plan or {}).get("rules", [])):
            got.setdefault(seams.norm(t), t)
        return list(got.values())

    def rules(self) -> dict[str, tuple[str, str]]:
        """The process rules, or none when no rule title is asked for."""
        if not self.titles:
            return {}
        if not self.rules_path.is_file():
            raise HuntError(f"the rules file {self.rules_path} is absent")
        return parse_rules(self.rules_path.read_text())

    def candidates(self, lens: Lens) -> Path:
        """The architect report of one lens."""
        return self.path("candidates", f"{slug(lens.name, 60)}.md")

    def claims(self) -> list[tuple[str, str]] | None:
        """(lens, KEY CLAIM) from --claims-from or every lens's candidates file; None while a lens has not run."""
        if self.a.claims_from:
            text = self.a.claims_from.read_text()
            owner = self.lenses[0].name if self.lenses else "claims-from"
            got = [str(c) for c in json.loads(text)] if self.a.claims_from.suffix == ".json" else extract_claims(text)
            return [(owner, c) for c in got]
        out = []
        for x in self.lenses:
            if not self.candidates(x).is_file():
                return None
            out += [(x.name, c) for c in extract_claims(self.candidates(x).read_text())]
        return out

    def entries(self) -> list[dict] | None:
        """The seam map, once the map step has written it."""
        p = self.path("map", "seams.json")
        return load_map(p) if p.is_file() else None

    def predicates(self) -> list[Predicate]:
        """Every sweep predicate known now."""
        return build_predicates(self.titles, self.rules(), self.claims() or [], self.entries(), self.title_lenses())

    def lens_of(self) -> dict[str, list[str]]:
        """Run name -> the lenses its cost is charged to."""
        out = {f"lens-{slug(x.name, 60)}": [x.name] for x in self.lenses}
        out.update({f"sweep-{p.n:02d}": p.lenses for p in self.predicates()})
        return out

    def budgets(self, sweeps: int) -> dict[str, float]:
        """Per-run caps from the total --budget-usd: each lens a share (all lenses at most 40%), verify a share, the rest split over the sweeps."""
        b = self.a.budget_usd
        n = 0 if self.a.claims_from else len(self.lenses)
        lens_total = min(b * LENS_SHARE * n, b * LENS_CAP_SHARE)
        each = (b - lens_total - b * VERIFY_SHARE) / max(sweeps, 1)
        return {"lens": round(lens_total / max(n, 1), 2), "sweep": round(each, 2), "reask": round(each * REASK_SHARE, 2), "verify": round(b * VERIFY_SHARE, 2)}

    def rule_cap(self) -> int:
        """How many changed rules an auto round can sweep at SWEEP_FLOOR_USD each beside the lenses' expected claims."""
        b = self.a.budget_usd
        pool = b - min(b * LENS_SHARE * len(self.lenses), b * LENS_CAP_SHARE) - b * VERIFY_SHARE
        return max(0, int(pool / SWEEP_FLOOR_USD) - CLAIMS_PER_LENS * len(self.lenses))

    def select_auto(self, tree: Path, map_dir: Path) -> dict:
        """The auto round's rules: those whose seams have changed sites in TREE since last_rev, minus closed classes, capped by budget."""
        since = self.state["last_rev"]
        plan = {"rev": self.rev, "since": since, "lenses": [x.name for x in self.lenses], "lens_index": self.lens_index, "rules": [], "dropped": [], "closed_sites": 0}
        if since:
            entries = seams.write_map(tree, map_dir, since, False)
            plan["rules"], plan["dropped"], plan["closed_sites"] = changed_rules(entries, self.state["closed"], self.rule_cap())
        return plan

    def plan(self) -> list[str]:
        """The planned runs, one line each: step, agent, predicate, cwd, budget, output."""
        if self.a.auto and self.auto_plan is None:
            p = self.path("plan.json")
            if p.is_file():
                self.auto_plan = json.loads(p.read_text())
            else:
                with tempfile.TemporaryDirectory() as tmp:
                    self.auto_plan = self.select_auto(self.root, Path(tmp))
                self.auto_plan["estimate"] = True
        preds = self.predicates()
        pending = self.claims() is None
        cap = self.budgets(len(preds) + (3 * len(self.lenses) if pending else 0))
        lines = [f"plan: out {self.out}", f"plan: setup wait for the release gate once, then worktree {self.wt} (detached origin/main, read-only), removed at the end",
                 f"plan: map python -m dev.pytools.seams -> {self.path('map', 'seams.md')} (one map for every lens)"]
        if self.a.auto:
            ap = self.auto_plan
            where = "this checkout (dry-run estimate; the round recomputes on origin/main)" if ap.get("estimate") else f"origin/main {ap.get('rev')}"
            lines.append(f"plan: auto lenses {', '.join(ap['lenses'])} (rotation index {', '.join(map(str, ap['lens_index']))} of {len(LENS_ORDER)}); state {state_path(self.root)}")
            if not ap["since"]:
                lines.append("plan: auto no last_rev yet: no changed-seam rules this round")
            else:
                lines.append(f"plan: auto rules whose seams changed since {ap['since'][:12]} on {where}: {len(ap['rules'])} kept (cap {self.rule_cap()}), "
                             f"{len(ap['dropped'])} dropped by the budget, {ap['closed_sites']} changed sites skipped as closed")
                lines += [f"plan: auto dropped rule {t!r}" for t in ap["dropped"]]
        if not self.a.claims_from:
            for x in self.lenses:
                lines.append(f"plan: lens agent=rivet-architect cwd={self.wt} budget=${cap['lens']:.2f} out={self.candidates(x)} lens={x.name!r} rules={len(x.titles) if not self.a.auto else 0}")
        for p in preds:
            skip = " SKIPPED: no sites (claim not mapped to a seam)" if p.mapped == 0 else ""
            lines.append(f"plan: sweep agent=rivet-sweeper cwd={self.wt} budget=${cap['sweep']:.2f} (re-ask ${cap['reask']:.2f}) out={self.path('sweep', f'{p.n:02d}.md')} "
                         f"{p.origin}={p.text[:90]!r} seams={p.seams or ['-']} lenses={p.lenses}{skip}")
        if pending:
            lines.append(f"plan: sweep agent=rivet-sweeper one per KEY CLAIM of each lens (known after the lenses; the sweep budget is split again then, ~${cap['sweep']:.2f} each for 3 claims per lens)")
        lines += [f"plan: merge pure python -> {self.path('classes.md')}, {self.path('classes.json')}",
                  f"plan: verify agent=rivet-verifier cwd={self.wt} budget=${cap['verify']:.2f} verify-top={self.a.verify_top} (every class with a proven violated site) out={self.path('verdicts.md')}",
                  f"plan: report -> {self.path('summary.md')}, {self.path('tokens.json')} (with /usage before and after when readable)",
                  f"plan: total budget ${self.a.budget_usd:.2f} across {len(self.lenses)} lens(es)",
                  f"plan: models {', '.join(f'{k}={v}' for k, v in self.models.items()) or 'the agents own'}"]
        return lines

    def ensure_worktree(self) -> None:
        """Wait for the release gate once, then make the detached read-only worktree of origin/main."""
        if self.made_wt:
            return
        print("hunt: waiting for the release gate (if one runs)", flush=True)
        subprocess.run(["/bin/sh", "-c", self.gate_cmd], check=True)
        git = lambda *args: subprocess.run([GIT, *args], cwd=self.root, capture_output=True, text=True)
        f = git("fetch", "-q", "origin")
        if f.returncode != 0:
            raise HuntError(f"git fetch origin failed: {f.stderr.strip()[:300]}")
        if self.wt.exists():
            git("worktree", "remove", "--force", str(self.wt))
        git("worktree", "prune")
        self.made_wt = True
        r = git("worktree", "add", "-q", "--detach", str(self.wt), "origin/main")
        if r.returncode != 0:
            raise HuntError(f"git worktree add failed: {r.stderr.strip()[:300]}")
        self.rev = subprocess.run([GIT, "rev-parse", "HEAD"], cwd=self.wt, capture_output=True, text=True).stdout.strip() or None
        for d, dirs, files in os.walk(self.wt):
            dirs[:] = [x for x in dirs if x != ".git"]
            for name in files:
                p = Path(d) / name
                if not p.is_symlink() and name != ".git":
                    p.chmod(p.stat().st_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
        print(f"hunt: worktree {self.wt} at origin/main {str(self.rev)[:12]}", flush=True)

    def remove_worktree(self) -> None:
        """Remove the worktree this run made, whatever happened."""
        if not self.made_wt:
            return
        subprocess.run([GIT, "worktree", "remove", "--force", str(self.wt)], cwd=self.root, capture_output=True)
        if self.wt.exists():
            shutil.rmtree(self.wt, ignore_errors=True)
        subprocess.run([GIT, "worktree", "prune"], cwd=self.root, capture_output=True)
        self.made_wt = False
        print(f"hunt: worktree {self.wt} removed", flush=True)

    def settings(self) -> Path:
        """The permission settings every run gets: read-only tools allowed, editing and subagents denied."""
        p = self.path("claude-settings.json")
        p.write_text(json.dumps({"permissions": {"allow": ALLOW, "deny": DENY}}, indent=1))
        return p

    def read_usage(self) -> dict | None:
        """The /usage reading of `claude -p`, which costs no model call; None when it cannot be read."""
        exe = shutil.which("claude")
        if not exe:
            return None
        try:
            p = subprocess.run([exe, "-p", "--output-format", "json"], cwd=self.out, input="/usage", capture_output=True, text=True, timeout=USAGE_TIMEOUT_S)
            got = parse_usage(str(json.loads(p.stdout).get("result") or "")) if p.returncode == 0 else None
        except (subprocess.TimeoutExpired, json.JSONDecodeError, AttributeError, OSError):
            got = None
        if got:
            got["at"] = f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ}"
        return got

    def claude(self, step: str, name: str, agent: str, prompt: str, budget: float, resume: str | None = None) -> dict:
        """One `claude -p` run with cwd in the worktree; its record (rc, result JSON, wall time) lands in OUT/runs/NAME.json."""
        self.ensure_worktree()
        exe = shutil.which("claude")
        if not exe:
            raise HuntError("no `claude` executable on PATH")
        sid = resume or str(uuid.uuid4())
        cmd = [exe, "-p", "--agent", agent, "--output-format", "json", "--max-budget-usd", f"{max(budget, 0.01):.2f}",
               "--permission-mode", PERMISSION_MODE, "--permission-prompts", "none", "--settings", str(self.settings()),
               "--add-dir", str(self.out)] + (["--resume", sid] if resume else ["--session-id", sid]) + (["--model", self.models[step]] if step in self.models else [])
        t0 = time.monotonic()
        try:
            p = subprocess.run(cmd, cwd=self.wt, input=prompt, capture_output=True, text=True, timeout=RUN_TIMEOUT_S)
            rc, stdout, stderr = p.returncode, p.stdout, p.stderr
        except subprocess.TimeoutExpired as e:
            rc, stdout, stderr = 124, e.stdout or "", f"timed out after {RUN_TIMEOUT_S}s"
        try:
            result = json.loads(stdout)
        except (json.JSONDecodeError, TypeError):
            result = None
        rec = {"name": name, "step": step, "agent": agent, "rc": rc, "wall_s": round(time.monotonic() - t0, 1), "budget_usd": round(budget, 2),
               "session_id": (result or {}).get("session_id") or sid, "cmd": cmd, "result": result, "stderr_tail": (stderr or "")[-500:]}
        self.path("runs").mkdir(parents=True, exist_ok=True)
        self.path("runs", f"{name}.json").write_text(json.dumps(rec, indent=1))
        u = usage_of(result)
        print(f"hunt: {name} agent={agent} rc={rc} ${u['cost_usd']:.2f} {rec['wall_s']:.0f}s", flush=True)
        return rec

    @staticmethod
    def text_of(rec: dict) -> str:
        """The agent's final text, or empty when the run failed."""
        r = rec.get("result") or {}
        return r.get("result") or "" if rec["rc"] == 0 and not r.get("is_error") else ""

    def ask(self, step: str, name: str, agent: str, prompt: str, budget: float, reask_budget: float, check) -> tuple[str, object, list[str]]:
        """Run, grade the closing JSON block with `check`, and re-ask the same session once when it is missing or invalid."""
        rec = self.claude(step, name, agent, prompt, budget)
        text = self.text_of(rec)
        if not text:
            return "", None, [f"run failed (rc {rec['rc']}); see {self.path('runs', name + '.json')}"]
        obj, err = json_block(text)
        errs = [err] if err else check(obj)
        if not errs:
            return text, obj, []
        again = (f"Your reply could not be graded: {'; '.join(errs)[:800]}. Reply again in English with the whole result and end with ONE fenced "
                 "```json block of the shape you were given, with nothing after it.")
        rec2 = self.claude(step, f"{name}-reask", agent, again, reask_budget, resume=rec["session_id"])
        text2 = self.text_of(rec2) or text
        obj, err = json_block(text2)
        errs = [err] if err else check(obj)
        return text2, (obj if not errs else None), errs

    def step_map(self) -> bool:
        """Write the seam map of the worktree into OUT/map, once for every lens."""
        if self.path("map", "seams.json").is_file():
            return False
        self.ensure_worktree()
        got = seams.write_map(self.wt, self.path("map"), self.a.since, False)
        print(f"hunt: map {len(got)} seams -> {self.path('map', 'seams.md')}", flush=True)
        return True

    def lens_prompt(self, x: Lens, out: Path) -> str:
        """The brief of one architect run."""
        entries = self.entries() or []
        counts = {e["name"]: len(e.get("sites", [])) for e in entries}
        focus = [f"- {n}: {counts.get(n, 0)} sites" for n in x.seams]
        drift = drift_rows(entries, x.seams) if x.name == "engine-drift" else []
        return (f"Lens {x.name}: {x.text}\n\n" + WORKTREE_NOTE.format(wt=self.wt, out=out) + "\n\n" + ENGLISH + "\n\n"
                f"The seam map of this tree is {self.path('map', 'seams.md')} (every site in seams.json beside it)."
                + (("\nFocus seams of this lens:\n" + "\n".join(focus)) if focus else "")
                + (("\nRows with a missing engine:\n" + "\n".join(drift)) if drift else "")
                + "\n\nRun the review through this ONE lens and reply with the report itself, in Markdown. "
                "End every candidate with one line `KEY CLAIM: <one factual sentence in English with file:line>`.")

    def step_lens(self) -> bool:
        """One architect run per lens, at most --parallel at once; each report becomes candidates/<lens>.md."""
        if self.a.claims_from:
            return False
        todo = [x for x in self.lenses if not self.candidates(x).is_file()]
        if not todo:
            return False
        self.path("candidates").mkdir(parents=True, exist_ok=True)
        self.ensure_worktree()
        cap = self.budgets(1)["lens"]

        def one(x: Lens) -> str | None:
            """Run one lens; the error text when it produced nothing."""
            out = self.candidates(x)
            name = f"lens-{slug(x.name, 60)}"
            rec = self.claude("lens", name, "rivet-architect", self.lens_prompt(x, out), cap)
            text = self.text_of(rec)
            if not text:
                return f"lens {x.name} failed (rc {rec['rc']}); see {self.path('runs', name + '.json')}"
            out.write_text(text)
            print(f"hunt: lens {x.name} {len(extract_claims(text))} KEY CLAIMs -> {out}", flush=True)
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, self.a.parallel)) as pool:
            failed = [e for e in pool.map(one, todo) if e]
        if failed:
            raise HuntError(f"{len(failed)} lens run(s) failed; --resume retries them: {failed[0]}")
        return True

    def sweep_prompt(self, p: Predicate, out: Path) -> str:
        """The brief of one sweeper."""
        seam = primary_seam(p, self.entries())
        rule = f"Process rule: {p.rule_title}\n\n{p.rule_body}" if p.rule_body else "Process rule: none (an architecture KEY CLAIM; grade the claim itself)."
        sites = "\n".join(p.sites) or "- (no mapped seam: start from the files the predicate cites)"
        cmds = "\n".join(f'    python3 -m dev.pytools.seams --seam "{n}"' for n in p.seams) or "    (none)"
        return (f"Predicate {p.n}: {p.text}\n\n{rule}\n\nSeams: {', '.join(p.seams) or '-'}. Full site lists: {self.path('map', 'seams.json')}, or\n{cmds}\n\n"
                f"Sites:\n{sites}\n\n" + WORKTREE_NOTE.format(wt=self.wt, out=out) + "\n\n" + ENGLISH + "\n\n"
                "Before the table, check the premise of the rule or claim on this tree with one command or one file:line (a rule about "
                'panic = "abort" needs the Cargo.toml profile and the release workflow) and record it as "premise".\n\n'
                f'End your reply with ONE fenced ```json block, nothing after it, with "predicate": {json.dumps(p.text)} and "seam": {json.dumps(seam)}:\n{SWEEP_SCHEMA}\n')

    def step_sweep(self) -> bool:
        """One sweeper per mapped predicate, at most --parallel at once; a predicate with no sites is skipped; a failed run leaves no file."""
        preds = self.predicates()
        for p in preds:
            if p.mapped == 0:
                print(f"hunt: sweep {p.n:02d} skipped: no sites (claim not mapped to a seam): {p.text[:90]}", flush=True)
        live = [p for p in preds if p.mapped != 0]
        todo = [p for p in live if not self.path("sweep", f"{p.n:02d}.md").is_file()]
        if not todo:
            return False
        self.path("sweep").mkdir(parents=True, exist_ok=True)
        self.ensure_worktree()
        cap = self.budgets(len(live))

        def one(p: Predicate) -> str | None:
            """Run one sweep; the error text when it produced nothing."""
            out = self.path("sweep", f"{p.n:02d}.md")
            text, _, errs = self.ask("sweep", f"sweep-{p.n:02d}", "rivet-sweeper", self.sweep_prompt(p, out), cap["sweep"], cap["reask"], validate_sweep)
            if not text:
                return f"sweep {p.n:02d}: {'; '.join(errs)}"
            out.write_text(text)
            print(f"hunt: sweep {p.n:02d} {'valid' if not errs else 'INVALID: ' + '; '.join(errs)[:120]} -> {out}", flush=True)
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, self.a.parallel)) as pool:
            failed = [e for e in pool.map(one, todo) if e]
        if failed:
            raise HuntError(f"{len(failed)} sweep run(s) failed; --resume retries them: {failed[0]}")
        return True

    def sweeps(self) -> tuple[list[dict], list[str]]:
        """The valid sweep blocks, each with its predicate's origin, lenses and cited files, and the names of the invalid sweep files."""
        preds = {p.n: p for p in self.predicates()}
        blocks, invalid = [], []
        for f in sorted(self.path("sweep").glob("[0-9][0-9].md")) if self.path("sweep").is_dir() else []:
            obj, err = json_block(f.read_text())
            if err or validate_sweep(obj):
                invalid.append(f.name)
                continue
            p = preds.get(int(f.stem))
            blocks.append({**obj, "origin": p.origin if p else "claim", "lenses": p.lenses if p else [], "cited": p.cited if p else []})
        return blocks, invalid

    def step_merge(self) -> bool:
        """Group the sweeps of every lens into ranked classes: classes.json and classes.md."""
        if self.path("classes.json").is_file():
            return False
        blocks, invalid = self.sweeps()
        entries = self.entries() or []
        bound = {e["name"] for e in entries if e["engine_bound"]}
        classes = merge(blocks, bound, self.state["closed"])
        no_sites = [{"n": p.n, "origin": p.origin, "text": p.text, "lenses": p.lenses} for p in self.predicates() if p.mapped == 0]
        self.path("classes.json").write_text(json.dumps({"classes": classes, "invalid_sweeps": invalid, "no_sites": no_sites}, indent=1))
        self.path("classes.md").write_text(render_classes(classes))
        print(f"hunt: merge {len(blocks)} sweeps -> {len(classes)} classes, {sum(c['one_off'] for c in classes)} ONE-OFF, "
              f"{sum(c['status'] == REFUTED_PREMISE for c in classes)} {REFUTED_PREMISE}, {len(invalid)} invalid, {len(no_sites)} no sites", flush=True)
        return True

    def top(self) -> list[dict]:
        """The classes the verifier gets: every class with a proven violated site, best score first, at most --verify-top."""
        return verify_set(json.loads(self.path("classes.json").read_text())["classes"], self.a.verify_top)

    def step_verify(self) -> bool:
        """One verifier run over the chosen classes; verdicts.md ends with the parsed verdict block."""
        out = self.path("verdicts.md")
        if out.is_file():
            return False
        top = self.top()
        if not top:
            out.write_text('No class to verify.\n\n```json\n{"verdicts": []}\n```\n')
            return True
        ids = [c["id"] for c in top]
        parts = []
        for c in top:
            parts += [f"Class {c['id']} (seam {c['seam']}): the predicate is VIOLATED at these sites.", *[f"- predicate: {p}" for p in c["predicates"]], *site_table(strongest(c)), ""]
        prompt = ("Try to REFUTE each class below on its two sites (the strongest and one other): open each site, check the line against "
                  "the current code, and re-run any command its evidence names when it is read-only. Uncertain means refuted.\n\n" + "\n".join(parts) + "\n"
                  + WORKTREE_NOTE.format(wt=self.wt, out=out) + "\n\n" + ENGLISH + "\n\nEnd your reply with ONE fenced ```json block, nothing after it: "
                  '{"verdicts": [{"class": "C1", "holds": true, "evidence": "what you opened or ran, quoted"}]} with one row per class '
                  f"({', '.join(ids)}); holds true means the violation is confirmed.")
        cap = self.budgets(1)
        text, obj, errs = self.ask("verify", "verify", "rivet-verifier", prompt, cap["verify"], cap["verify"] * REASK_SHARE, lambda o: validate_verdicts(o, ids))
        if not text:
            raise HuntError(f"the verify run failed: {'; '.join(errs)}")
        out.write_text(text)
        print(f"hunt: verify {len(ids)} classes{'' if not errs else ' INVALID: ' + '; '.join(errs)[:120]} -> {out}", flush=True)
        return True

    def verdicts(self) -> dict[str, dict]:
        """Class id -> verdict row from verdicts.md; a class the block does not grade is refuted."""
        p = self.path("verdicts.md")
        if not p.is_file():
            return {}
        obj, err = json_block(p.read_text())
        rows = obj.get("verdicts", []) if isinstance(obj, dict) and not err else []
        got = {r["class"]: r for r in rows if isinstance(r, dict) and isinstance(r.get("class"), str)}
        top = self.top() if self.path("classes.json").is_file() else []
        return {c["id"]: got.get(c["id"], {"holds": False, "evidence": "no gradable verdict"}) for c in top}

    def step_report(self) -> bool:
        """summary.md and tokens.json, with /usage before and after the round."""
        if self.path("summary.md").is_file():
            return False
        records = [json.loads(f.read_text()) for f in sorted(self.path("runs").glob("*.json"))] if self.path("runs").is_dir() else []
        tok = tokens(records, self.load_timing(), self.lens_of())
        before = self.path("usage-before.json")
        tok["usage"] = {"before": json.loads(before.read_text()) if before.is_file() else None, "after": self.read_usage()}
        tok["models"] = self.models
        if tok["usage"]["before"] is None and tok["usage"]["after"] is None:
            tok["usage"] = None
        if not tok["usage"] or tok["usage"]["after"] is None:
            print(USAGE_HINT, flush=True)
        self.path("tokens.json").write_text(json.dumps(tok, indent=1))
        data = json.loads(self.path("classes.json").read_text())
        title = ", ".join(x.name for x in self.lenses) + (" (auto)" if self.a.auto else "")
        self.path("summary.md").write_text(render_summary(title, data["classes"], self.verdicts(), data["invalid_sweeps"], tok, self.a.verify_top, data.get("no_sites", [])))
        acc = sum(v.get("holds") is True for v in self.verdicts().values())
        print(f"hunt: report {acc} accepted, {len(data['classes'])} classes, ${tok['total']['cost_usd']:.2f} -> {self.path('summary.md')}", flush=True)
        return True

    def load_timing(self) -> dict[str, float]:
        """Wall time per step, as recorded by every invocation that ran it."""
        p = self.path("timing.json")
        return json.loads(p.read_text()) if p.is_file() else {}

    def invalidate(self, step: str) -> None:
        """Drop the outputs of every step after `step`."""
        later = STEPS[STEPS.index(step) + 1:]
        drop = {"merge": ["classes.json", "classes.md"], "verify": ["verdicts.md"], "report": ["summary.md", "tokens.json"]}
        if "lens" in later and not self.a.claims_from:
            shutil.rmtree(self.path("candidates"), ignore_errors=True)
        if "sweep" in later and step in ("map", "lens"):
            shutil.rmtree(self.path("sweep"), ignore_errors=True)
        for s in later:
            for f in drop.get(s, []):
                self.path(f).unlink(missing_ok=True)

    def start_auto(self) -> None:
        """Fix the auto round's plan on origin/main once (plan.json), and print it."""
        p = self.path("plan.json")
        if p.is_file():
            self.auto_plan = json.loads(p.read_text())
            return
        self.ensure_worktree()
        self.auto_plan = self.select_auto(self.wt, self.path("map-since"))
        p.write_text(json.dumps(self.auto_plan, indent=1))
        print("\n".join(x for x in self.plan() if x.startswith("plan: auto")), flush=True)

    def finish_auto(self) -> None:
        """Advance the rotation and last_rev after a finished auto round."""
        state = load_state(self.root)
        state.update({"last_rev": self.auto_plan.get("rev") or self.rev, "last_lens": self.auto_plan["lens_index"][-1], "last_out": str(self.out)})
        save_state(self.root, state)
        print(f"hunt: auto state -> {state_path(self.root)} (last_lens {state['last_lens']}, last_rev {str(state['last_rev'])[:12]})", flush=True)

    def run(self) -> int:
        """Execute the steps in order (or --only one), removing the worktree at the end, also on failure."""
        if self.out.is_dir() and any(self.out.iterdir()) and not self.a.resume:
            print(f"hunt: {self.out} already holds a hunt; pass --resume or another --out", file=sys.stderr)
            return 2
        self.path().mkdir(parents=True, exist_ok=True)
        if not self.path("usage-before.json").is_file():
            before = self.read_usage()
            self.path("usage-before.json").write_text(json.dumps(before))
            if before is None:
                print(USAGE_HINT, flush=True)
        steps = {"setup": lambda: False, "map": self.step_map, "lens": self.step_lens, "sweep": self.step_sweep,
                 "merge": self.step_merge, "verify": self.step_verify, "report": self.step_report}
        try:
            if self.a.auto:
                self.start_auto()
            if self.titles:
                self.predicates()
            for name in [self.a.only] if self.a.only else STEPS:
                if name == "setup":
                    if self.a.only:
                        self.ensure_worktree()
                    continue
                t0 = time.monotonic()
                ran = steps[name]()
                if ran:
                    timing = self.load_timing()
                    timing[name] = timing.get(name, 0.0) + time.monotonic() - t0
                    self.path("timing.json").write_text(json.dumps(timing, indent=1))
                    if name != "report":
                        self.invalidate(name)
                else:
                    print(f"hunt: {name} skipped (output exists)", flush=True)
            if self.path("summary.md").is_file():
                n = append_history(round_records(self.out, [x.name for x in self.lenses], (self.auto_plan or {}).get("rev") or self.rev))
                print(f"hunt: history {n} class records -> {history_path()}", flush=True)
            if self.a.auto and self.path("summary.md").is_file():
                self.finish_auto()
        except HuntError as e:
            print(f"hunt: FAILED: {e}", file=sys.stderr)
            return 1
        finally:
            self.remove_worktree()
        print(f"hunt: done -> {self.path('summary.md')}", flush=True)
        return 0


def parser() -> argparse.ArgumentParser:
    """The command line."""
    ap = argparse.ArgumentParser(prog="python -m dev.pytools.hunt", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lens", help="a preset lens name (--list-lenses) or the lens in words")
    ap.add_argument("--lenses", help="several lenses, comma-separated: preset names or free text, over one map, merge and verify")
    ap.add_argument("--list-lenses", action="store_true", help="print the preset lenses and their default rules")
    ap.add_argument("--auto", action="store_true", help="the next lenses in rotation plus the rules whose seams changed since the last auto round")
    ap.add_argument("--auto-lenses", type=int, default=2, help="lenses per auto round (default 2)")
    ap.add_argument("--close", metavar="CLASS_ID", help="record a class of --out (default: the last auto round) as closed by --pr")
    ap.add_argument("--pr", type=int, help="the PR that closes --close")
    ap.add_argument("--history", action="store_true", help="yield per lens, repeated holes, duplicate lenses and the trend, from the hunt history journal")
    ap.add_argument("--backfill", type=Path, metavar="OUT", help="add a finished hunt OUT to the history journal (idempotent)")
    ap.add_argument("--rules", action="append", default=[], help="a process-rule title to sweep (repeatable, or `;`-separated)")
    ap.add_argument("--claims-from", type=Path, help="an architect report (KEY CLAIM lines) or a JSON list of claims, instead of the lens runs")
    ap.add_argument("--out", type=Path, help="default target/hunt/<UTC date>-<lens names>")
    ap.add_argument("--parallel", type=int, default=3)
    ap.add_argument("--model", action="append", default=[], metavar="STEP=MODEL", help="model for a step (lens, sweep, verify) instead of the agent's own; repeatable")
    ap.add_argument("--budget-usd", type=float, default=30.0, help="the total across every lens and claude run (default 30)")
    ap.add_argument("--verify-top", "--top", dest="verify_top", type=int, default=8, help="most classes the verifier gets (default 8)")
    ap.add_argument("--since", help="map only files changed since REV")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--resume", action="store_true", help="continue in an existing OUT; done steps are skipped by their files")
    ap.add_argument("--only", choices=STEPS)
    ap.add_argument("--self-test", action="store_true")
    return ap


FAKE_CLAUDE = r'''#!{python}
import json, os, re, sys
argv = sys.argv[1:]
prompt = sys.stdin.read()
def out(text, cost):
    print(json.dumps({"type": "result", "is_error": False, "result": text, "session_id": sid, "total_cost_usd": cost,
                      "usage": {"input_tokens": 100, "output_tokens": 10, "cache_creation_input_tokens": 5, "cache_read_input_tokens": 1000}}))
if prompt.strip() == "/usage":
    sid = "usage"
    if os.environ.get("HUNT_FAKE_MODE") != "no-usage":
        out("Current week (all models): 52% used · resets\nCurrent week (Fable): 45% used\n", 0)
    sys.exit(0)
log = os.environ["HUNT_FAKE_LOG"]
agent = argv[argv.index("--agent") + 1]
resume = argv[argv.index("--resume") + 1] if "--resume" in argv else None
sid = resume or argv[argv.index("--session-id") + 1]
verify = re.findall(r"^Class (C\d+) \(seam (.*?)\)", prompt, re.M)
with open(log, "a") as f:
    f.write(json.dumps({"agent": agent, "cwd": os.getcwd(), "argv": argv, "resume": resume, "head": prompt[:70], "ro": not os.access("src/state/lock.rs", os.W_OK),
                        "verify": verify, "english": "in English" in prompt, "premise": '"premise"' in prompt}) + "\n")
def site(file, line, engine, verdict, proven):
    return {"file": file, "line": line, "engine": engine, "verdict": verdict, "evidence": file + ":" + str(line) + " quoted", "proven": proven}
if os.environ.get("HUNT_FAKE_MODE") == "fail-lens" and agent == "rivet-architect":
    print("boom", file=sys.stderr)
    sys.exit(1)
held = {"holds": True, "evidence": "Cargo.toml:1 quoted"}
if agent == "rivet-architect":
    lens = re.match(r"Lens (\S+):", prompt).group(1)
    text = ("# Lens\n\n## A\nKEY CLAIM: the lock key in src/state/lock.rs:1 omits the export name (" + lens + ").\n\n## B\n**KEY CLAIM:** BADJSON src/other.py:5 swallows a typed error.\n"
            "\n## C\nKEY CLAIM: nothing maps here, docs only.\n")
elif agent == "rivet-sweeper":
    if resume:
        pred, seam = "BADJSON src/other.py:5 swallows a typed error.", "error text matching"
    else:
        pred = json.loads(re.search(r'"predicate": (".*?"(?<!\\")) and', prompt).group(1))
        seam = json.loads(re.search(r'"seam": (".*?") *:', prompt).group(1))
    if pred.startswith("BADJSON") and not resume:
        text = "table without a block"
    elif pred.startswith("A lock"):
        block = {"predicate": pred, "seam": seam, "premise": held, "summary": "two engines", "sites": [site("src/state/lock.rs", 1, "sqlite", "violated", True), site("src/state/lock.rs", 2, "postgres", "violated", False), site("src/state/lock.rs", 3, "n/a", "holds", True)]}
        text = "| table |\n\n```json\n" + json.dumps(block) + "\n```\n"
    elif pred.startswith("An empty source"):
        block = {"predicate": pred, "seam": seam, "premise": {"holds": False, "evidence": "Cargo.toml:9 has no such profile"}, "summary": "premise false", "sites": [site("src/empty.rs", 1, "n/a", "violated", True), site("src/empty.rs", 2, "n/a", "violated", True)]}
        text = "```json\n" + json.dumps(block) + "\n```"
    elif pred.startswith("the lock key"):
        block = {"predicate": pred, "seam": seam, "premise": held, "summary": "dup", "sites": [site("src/state/lock.rs", 1, "sqlite", "holds", False), site("src/state/lock.rs", 2, "postgres", "violated", True)]}
        text = "```json\n" + json.dumps(block) + "\n```"
    else:
        block = {"predicate": pred, "seam": seam, "premise": held, "summary": "one", "sites": [site("src/other.py", 5, "mysql", "violated", True)]}
        text = "fixed\n```json\n" + json.dumps(block) + "\n```\n"
else:
    text = "rows\n```json\n" + json.dumps({"verdicts": [{"class": i, "holds": i == "C1", "evidence": "opened"} for i, _ in verify]}) + "\n```\n"
out(text, 0.25)
'''

RULES_FIXTURE = "# rivet\n\n## A lock is keyed by exactly what it protects\n\nThe lease key names the export.\n\n## An empty source is a result, never a failure\n\nZero rows exit 0.\n"


def self_test() -> int:
    """Fixture-only: lenses, plan, grading and re-ask, premise, zero-site skip, merge, verify set, auto rounds, close, tokens, worktree cleanup."""
    live = {s.name for s in seams.SEAMS}
    for name, (_, names) in LENSES.items():
        assert set(names) <= live, f"lens {name} names seams absent from dev.pytools.seams.SEAMS: {sorted(set(names) - live)}"
    assert all(resolve_lens(n).titles for n in LENS_ORDER if n != "engine-drift"), "a preset lens maps to no rule"
    assert resolve_lens("Lock keys") == Lens("lock-keys", "Lock keys", [])
    rules = parse_rules(RULES_FIXTURE)
    assert list(rules) == [seams.norm("A lock is keyed by exactly what it protects"), seams.norm("An empty source is a result, never a failure")]
    assert rules[seams.norm("a lock is keyed by exactly what it protects")][1] == "The lease key names the export."
    assert slug("Load & types: BigQuery!") == "load-types-bigquery"
    assert extract_claims("x\nKEY CLAIM: a src/a.rs:3 b.\n**KEY CLAIM:** c.\n- KEY CLAIM - d\n") == ["a src/a.rs:3 b.", "c.", "d"]
    assert claim_files("see src/a.rs:3 and ./dev/x.py and src/a.rs:9") == ["src/a.rs", "dev/x.py"]
    assert [engine_bound(e) for e in ({"name": "lock keys"}, {"name": "warehouse targets (files)"}, {"name": "x", "engines": "state"}, {"name": "state backends (x)", "engines": ""})] == [False, True, True, False]
    assert parse_usage("a\nCurrent week (all models): 52% used · x\n") == {"percent": {"Current week (all models)": 52.0}, "text": "a\nCurrent week (all models): 52% used · x\n"} and parse_usage("Unknown command") is None
    held = {"holds": True, "evidence": "Cargo.toml:3"}
    good = {"predicate": "p", "seam": "s", "summary": "x", "premise": held, "sites": [{"file": "src/a.rs", "line": 3, "engine": "pg", "verdict": "violated", "evidence": "src/a.rs:3", "proven": True}]}
    fence = lambda o: "t\n```json\n" + json.dumps(o) + "\n```\n"
    assert json_block(fence(good)) == (good, None) and validate_sweep(good) == []
    assert json_block("no block")[1] and json_block(fence(good) + "after")[1] and json_block("```json\n{bad\n```")[1]
    bad = {**good, "sites": [{**good["sites"][0], "verdict": "maybe", "line": "3", "proven": "yes", "evidence": " "}]}
    assert len(validate_sweep(bad)) == 4 and validate_sweep([]) and validate_sweep({**good, "sites": None})
    assert validate_sweep({**good, "premise": None}) == ["`premise.holds` is missing or not true or false"] and validate_sweep({**good, "premise": {"holds": False, "evidence": ""}}) == ["`premise.evidence` is empty"]
    assert validate_verdicts({"verdicts": [{"class": "C1", "holds": True}]}, ["C1"]) == [] and validate_verdicts({"verdicts": [{"class": "C1", "holds": "y"}]}, ["C1", "C2"]) == ["no verdict for class C2", "class C1: `holds` is not true or false"]
    s = lambda f, n, e, v, p: {"file": f, "line": n, "engine": e, "verdict": v, "evidence": f"{f}:{n}", "proven": p}
    blk = lambda pred, seam, sites, **kw: {"predicate": pred, "seam": seam, "summary": "", "premise": held, "sites": sites, **kw}
    blocks = [
        blk("p1", "lock keys", [s("src/a.rs", 1, "sqlite", "holds", False), s("src/a.rs", 2, "postgres", "violated", False)]),
        blk("p2", "lock keys", [s("src/a.rs", 1, "sqlite", "violated", True), s("src/a.rs", 2, "postgres", "violated", True)]),
        blk("p3", "env reads", [s("dev/x.py", 4, "mysql", "violated", True), s("dev/x.py", 9, "postgres", "holds", True)]),
        blk("p4", "retries", [s("src/r.rs", 4, "n/a", "violated", True), s("src/r.rs", 8, "", "violated", False)]),
        blk("p5", "clean", [s("src/c.rs", 1, "pg", "holds", True)]),
        blk("p6", "state backends (functions)", [s("src/st.rs", 1, "sqlite", "violated", True), s("src/st.rs", 2, "sqlite", "violated", True)]),
        blk("p7", "panic", [s("src/p.rs", 1, "n/a", "violated", True)], premise={"holds": False, "evidence": "Cargo.toml:240 no panic"}),
        blk("p8", "closed seam", [s("src/z.rs", 1, "n/a", "violated", True), s("src/z.rs", 2, "n/a", "violated", True)]),
    ]
    merged = merge(blocks, {"state backends (functions)"}, [{"seam": "closed seam", "files": ["src/z.rs"], "pr": 7}])
    got = {c["seam"]: c for c in merged}
    lk = got["lock keys"]
    assert lk["predicates"] == ["p1", "p2"] and [x["line"] for x in lk["sites"]] == [1, 2] and lk["violated"] == 2 and lk["proven_violated"] == 2, lk
    assert lk["status"] == "class" and not lk["one_off"] and lk["engines"] == ["postgres", "sqlite"] and lk["score"] == 2 * 3 * 3 and lk["id"] == "C1"
    assert got["env reads"]["status"] == "ONE-OFF" and got["env reads"]["one_off"], got["env reads"]
    assert got["retries"]["status"] == "class" and not got["retries"]["one_off"] and not got["retries"]["engine_bound"], got["retries"]
    assert got["state backends (functions)"]["status"] == "ONE-OFF (needs per-engine table)" and got["state backends (functions)"]["engine_bound"], got["state backends (functions)"]
    assert got["clean"]["status"] == "clean" and not got["clean"]["one_off"] and got["clean"]["score"] == 0
    assert got["panic"]["status"] == REFUTED_PREMISE and got["panic"]["score"] == 0 and got["panic"]["refuted_premise"][0]["evidence"] == "Cargo.toml:240 no panic"
    assert got["closed seam"]["status"] == "closed (PR #7)" and got["closed seam"]["score"] == 0
    vs = [c["seam"] for c in verify_set(merged, 8)]
    assert vs == ["lock keys", "state backends (functions)", "retries", "env reads"], vs
    assert [(c["seam"], why) for c, why in unverified(merged, 2)] == [("retries", "below --verify-top 2"), ("env reads", "below --verify-top 2")]
    two = merge([blk("c1", "lock keys", [s("src/a.rs", 1, "n/a", "violated", True)], origin="claim", lenses=["cdc"], cited=["src/a.rs"]),
                 blk("c2", "other", [s("src/b.rs", 1, "n/a", "violated", True)], origin="claim", lenses=["runners"], cited=["src/a.rs"]),
                 blk("c3", "lock keys", [s("src/a.rs", 2, "n/a", "violated", True)], origin="claim", lenses=["hot-spots"], cited=[])])
    assert two[0]["seam"] == "lock keys" and two[0]["confirmed_by"] == ["cdc", "hot-spots", "runners"] and two[0]["score"] == 2 * 3 * 3 * 3 and two[1]["confirmed_by"] == [], two
    assert [x["line"] for x in strongest(lk)] == [1, 2]
    assert changed_rules([{"name": "a", "rules": ["R1"], "sites": [{"path": "x.rs", "changed": True}, {"path": "y.rs", "changed": True}]},
                          {"name": "b", "rules": ["R2"], "sites": [{"path": "z.rs", "changed": True}]}], [{"seam": "a", "files": ["x.rs", "y.rs"]}], 5) == (["R2"], [], 2)
    recs = [{"step": "sweep", "name": "sweep-01", "result": {"total_cost_usd": 0.5, "usage": {"input_tokens": 10, "output_tokens": 2}}}, {"step": "lens", "name": "lens-a", "result": None, "rc": 1},
            {"step": "sweep", "name": "sweep-01-reask", "result": {"total_cost_usd": 0.25}}]
    tok = tokens(recs, {"sweep": 3.0, "merge": 0.5}, {"sweep-01": ["a", "b"], "lens-a": ["a"]})
    assert tok["total"]["runs"] == 3 and tok["total"]["cost_usd"] == 0.75 and tok["total"]["input_tokens"] == 10 and tok["total"]["wall_s"] == 3.5 and tok["steps"]["lens"]["runs"] == 1, tok
    assert tok["lenses"]["a"]["cost_usd"] == 0.375 and tok["lenses"]["b"]["sweep_usd"] == 0.375 and tok["lenses"]["a"]["runs"] == 3, tok["lenses"]
    assert fingerprint("s", ["b.rs", "a.rs", "a.rs"]) == fingerprint("s", ["a.rs", "b.rs"]) != fingerprint("t", ["a.rs", "b.rs"])
    assert [class_status({"id": "C1", "status": x, "violated": v, "one_off": o}, {"C1": {"holds": h}} if h is not None else {}) for x, v, o, h in
            (("class", 2, False, True), ("class", 2, False, False), (REFUTED_PREMISE, 2, False, None), ("closed (PR #1)", 1, False, None), ("ONE-OFF", 1, True, None),
             ("class", 3, False, None), ("clean", 0, False, None))] == ["accepted", "refuted", "refuted-premise", "closed", "one-off", "not-verified", "clean"]
    hrow = lambda out, date, cid, seam, files, status, ls, cost, pr=None: {"date": date, "rev": "r" + out, "out": out, "round": out, "lenses": ls, "class": cid, "seam": seam,
                                                                         "fingerprint": fingerprint(seam, files), "files": files, "status": status, "class_lenses": ls,
                                                                         "sites": {"swept": 2, "violated": len(files), "proven": len(files)}, "cost": {"round_usd": sum(cost.values()), "lenses": cost}, "pr": pr}
    hist = [hrow("r1", "2026-01-01", "C1", "lock keys", ["src/a.rs"], "accepted", ["cdc", "runners"], {"cdc": 2.0, "runners": 2.0}, 9),
            hrow("r1", "2026-01-01", "C2", "env", [], "clean", ["cdc"], {"cdc": 2.0, "runners": 2.0}),
            hrow("r2", "2026-01-02", "C1", "lock keys", ["src/a.rs", "src/b.rs"], "refuted", ["harness"], {"harness": 1.0}),
            hrow("r3", "2026-01-03", "C4", "retries", ["src/r.rs"], "accepted", ["harness"], {"harness": 1.0}),
            hrow("r4", "2026-01-04", "C1", "retries", ["src/r.rs"], "not-verified", ["cdc"], {"cdc": 3.0})]
    rep = "\n".join(history_report(hist))
    assert "- harness: 1 accepted / $2.00 = 0.50 per $ (2 rounds)" in rep and "- cdc: 1 accepted / $5.00 = 0.20 per $ (2 rounds)" in rep, rep
    assert "- retries [" in rep and "in 2 rounds: r3, r4" in rep and "- lock keys back after PR #9 (r1 C1 -> r2 C1): src/a.rs" in rep, rep
    assert "- cdc + harness: 1 shared classes" in rep and "- cdc + runners: 1 shared classes" in rep, rep
    assert "- 2026-01-01 rr1 cdc, runners: $4.00, 1 accepted, 0 refuted, 0 refuted-premise, 0 one-off, 0 not-verified, 1 clean, 0.25 accepted per $" in rep, rep

    with tempfile.TemporaryDirectory() as tmp:
        t = Path(tmp)
        repo = t / "repo"
        (repo / "src" / "state").mkdir(parents=True)
        (repo / "src" / "state" / "lock.rs").write_text("fn lock_key() -> String { lock_name() }\nfn other() { let lock_id = 1; }\nfn f() {}\n")
        (repo / "src" / "other.py").write_text("x = 1\n")
        (repo / ".gitignore").write_text("target/\n")
        git = lambda *a: subprocess.run([GIT, "-c", "user.email=t@t", "-c", "user.name=t", *a], cwd=repo, capture_output=True, text=True, check=True)
        git("init", "-q", "-b", "main")
        git("add", "-A")
        git("commit", "-qm", "base")
        git("remote", "add", "origin", str(repo))
        fake = t / "bin" / "claude"
        fake.parent.mkdir()
        fake.write_text(FAKE_CLAUDE.replace("{python}", sys.executable))
        fake.chmod(0o755)
        rules_path = t / "rules.md"
        rules_path.write_text(RULES_FIXTURE)
        log = t / "claude.log"
        env_keep = {k: os.environ.get(k) for k in ("PATH", "HUNT_FAKE_LOG", "HUNT_FAKE_MODE", HISTORY_ENV)}
        real_history = Path.home() / ".claude" / "hunt-history" / "rounds.jsonl"
        real_before = real_history.read_bytes() if real_history.is_file() else None
        seams_keep = (seams.SEAMS, seams.write_map)
        title, empty = "A lock is keyed by exactly what it protects", "An empty source is a result, never a failure"
        seams.SEAMS = (SimpleNamespace(name="state backends (functions)", rules=(title,)), SimpleNamespace(name="lock keys", rules=(title,)),
                       SimpleNamespace(name="empty-source branches", rules=(empty,)), SimpleNamespace(name="error text matching", rules=()))
        mapped: list[tuple[Path, str | None]] = []

        def fake_map(root: Path, out_dir: Path, since: str | None, all_kinds: bool) -> list[dict]:
            """The fixture seam map, restricted to the files changed since SINCE when given, written where hunt asks for it."""
            mapped.append((root, since))
            changed = set(subprocess.run([GIT, "diff", "--name-only", since, "--"], cwd=root, capture_output=True, text=True, check=True).stdout.split()) if since else None
            site = lambda p, n: {"path": p, "line": n, "kind": "prod", "text": "x", "changed": changed is not None and p in changed}
            got = [{"name": "state backends (functions)", "rules": [title], "engines": "state", "count": 0, "sites": []},
                   {"name": "lock keys", "rules": [title], "sites": [site("src/state/lock.rs", n) for n in (1, 2)]},
                   {"name": "empty-source branches", "rules": [empty], "sites": [site("src/empty.rs", 1)]},
                   {"name": "error text matching", "rules": [], "sites": [site("src/other.py", 5)]}]
            for e in got:
                e["sites"] = [x for x in e["sites"] if changed is None or x["changed"]]
                e["count"] = len(e["sites"])
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "seams.json").write_text(json.dumps({"rev": "x", "since": since, "seams": got}))
            (out_dir / "seams.md").write_text("## lock keys\n")
            return got

        assert list(inspect.signature(fake_map).parameters) == list(inspect.signature(seams_keep[1]).parameters), "the map fake drifted from seams.write_map"
        seams.write_map = fake_map
        os.environ.update({"PATH": f"{fake.parent}{os.pathsep}{os.environ.get('PATH', '')}", "HUNT_FAKE_LOG": str(log), HISTORY_ENV: str(t / "home" / "rounds.jsonl")})
        assert history_path() == t / "home" / "rounds.jsonl"
        os.environ.pop("HUNT_FAKE_MODE", None)
        calls = lambda: [json.loads(x) for x in log.read_text().splitlines()] if log.is_file() else []

        def hunt(*args: str) -> Hunt:
            """A hunt over the fixture repo with a no-op gate wait."""
            return Hunt(parser().parse_args(["--budget-usd", "10", *args]), root=repo, gate_cmd="true", rules_path=rules_path)

        single = lambda out, *extra: hunt("--lens", "Lock keys", "--rules", title, "--rules", empty, "--out", str(t / out), *extra)
        noise = io.StringIO()
        quiet = contextlib.ExitStack()
        quiet.enter_context(contextlib.redirect_stdout(noise))
        quiet.enter_context(contextlib.redirect_stderr(noise))
        try:
            assert seams_for_rule(title) == ["state backends (functions)", "lock keys"]
            assert resolve_lens("state-config-cli").titles == [title] and resolve_lens("runners").titles == []
            wt = single("dry").wt
            plan = single("dry").plan()
            assert any("lens agent=rivet-architect" in x and "budget=$1.50" in x for x in plan), plan
            assert any("sweep agent=rivet-sweeper" in x and "A lock is keyed" in x and "lock keys" in x for x in plan), plan
            assert any("one per KEY CLAIM" in x for x in plan) and any("verify agent=rivet-verifier" in x and "verify-top=8" in x for x in plan), plan
            assert "plan: models sweep=fable, lens=fable" in single("dry", "--model", "sweep=fable", "--model", "lens=fable").plan()
            assert parse_models(["verify=opus"]) == {"verify": "opus"} and hunter("cdc", {"models": {"lens": "fable"}}) == "cdc [fable]" and hunter("cdc", {}) == "cdc"
            for bad in (["merge=fable"], ["lens"], ["sweep= "]):
                try:
                    parse_models(bad)
                except HuntError:
                    pass
                else:
                    raise AssertionError(f"--model {bad} was accepted")
            assert not calls() and not wt.exists() and not (t / "dry").exists()
            try:
                hunt("--lens", "x", "--rules", "No such rule", "--out", str(t / "x")).predicates()
            except HuntError:
                pass
            else:
                raise AssertionError("an unknown rule title was planned")

            assert single("full").run() == 0
            c = calls()
            agents = [x["agent"] for x in c]
            assert agents.count("rivet-sweeper") == 5 and agents[0] == "rivet-architect" and agents[-1] == "rivet-verifier", agents
            assert {x["cwd"] for x in c} == {str(wt)} and all(x["ro"] and x["english"] for x in c), c
            assert all(x["premise"] for x in c if x["agent"] == "rivet-sweeper" and not x["resume"]), "a sweep brief asks for no premise"
            assert not any("nothing maps here" in x["head"] for x in c), "a predicate with no sites was swept"
            for flag in ("-p", "--output-format", "--max-budget-usd", "--permission-mode", "--settings", "--add-dir"):
                assert all(flag in x["argv"] for x in c), flag
            assert all(x["argv"][x["argv"].index("--permission-mode") + 1] == PERMISSION_MODE for x in c)
            reask = [x for x in c if x["resume"]]
            assert len(reask) == 1 and reask[0]["agent"] == "rivet-sweeper", reask
            first = next(x for x in c if x["head"].startswith("Predicate 4:"))
            assert reask[0]["resume"] == first["argv"][first["argv"].index("--session-id") + 1]
            assert not wt.exists(), "the worktree outlived the hunt"
            assert mapped == [(wt, None)], mapped
            out = t / "full"
            data = json.loads((out / "classes.json").read_text())
            by = {x["seam"]: x for x in data["classes"]}
            assert data["invalid_sweeps"] == [] and by["lock keys"]["status"] == "class" and len(by["lock keys"]["sites"]) == 3, by
            assert by["lock keys"]["predicates"] == [title, "the lock key in src/state/lock.rs:1 omits the export name (lock-keys)."]
            assert by["error text matching"]["status"] == "ONE-OFF" and not by["error text matching"]["engine_bound"], by["error text matching"]
            assert by["empty-source branches"]["status"] == REFUTED_PREMISE, by["empty-source branches"]
            assert [p["text"] for p in data["no_sites"]] == ["nothing maps here, docs only."], data["no_sites"]
            assert sorted(s for _, s in c[-1]["verify"]) == ["error text matching", "lock keys"], c[-1]["verify"]
            tok = json.loads((out / "tokens.json").read_text())
            assert tok["total"]["runs"] == len(c) == 7 and tok["total"]["cost_usd"] == 1.75 and tok["total"]["cache_read_input_tokens"] == 7000, tok["total"]
            assert tok["steps"]["sweep"]["runs"] == 5 and tok["steps"]["verify"]["runs"] == 1
            assert tok["usage"]["before"]["percent"]["Current week (all models)"] == 52.0 and tok["usage"]["after"]["percent"], tok["usage"]
            summary = (out / "summary.md").read_text()
            assert "### C1 lock keys" in summary and "error text matching: ONE-OFF" in summary and "$1.75" in summary, summary
            assert "Cargo.toml:9 has no such profile" in summary and "## No sites: claim not mapped to a seam (1, not swept)" in summary and "Usage before: Current week (all models) 52%" in summary, summary

            hist = read_history()
            assert [(r["class"], r["seam"], r["status"]) for r in hist] == [("C1", "lock keys", "accepted"), ("C2", "error text matching", "refuted"), ("C3", "empty-source branches", "refuted-premise")], hist
            assert hist[0]["out"] == str(out.resolve()) and hist[0]["lenses"] == ["lock-keys"] and hist[0]["rev"] and hist[0]["sites"] == {"swept": 3, "violated": 2, "proven": 2}, hist[0]
            assert hist[0]["cost"] == {"round_usd": 1.75, "lenses": {"lock-keys": 1.0, "--rules": 0.5}} and hist[0]["class_lenses"] == ["--rules", "lock-keys"] and hist[0]["fingerprint"] == fingerprint("lock keys", ["src/state/lock.rs"]) and hist[0]["pr"] is None, hist[0]
            n = len(calls())
            assert single("full").run() == 2
            assert single("full", "--resume").run() == 0 and len(calls()) == n and not wt.exists(), "a resume of a finished hunt ran something"
            (out / "sweep" / "03.md").unlink()
            assert single("full", "--resume").run() == 0
            again = calls()[n:]
            assert [x["agent"] for x in again] == ["rivet-sweeper", "rivet-verifier"] and again[0]["head"].startswith("Predicate 3:"), again

            os.environ["HUNT_FAKE_MODE"] = "no-usage"
            n, mapped[:] = len(calls()), []
            two = hunt("--lenses", "state-config-cli,runners", "--out", str(t / "two"), "--model", "lens=fable", "--model", "sweep=fable")
            assert two.out.name == "two" and hunt("--lenses", "state-config-cli,runners").out.name.endswith("-state-config-cli-runners")
            assert two.run() == 0 and len(mapped) == 1, mapped
            c2 = calls()[n:]
            model = lambda x: x["argv"][x["argv"].index("--model") + 1] if "--model" in x["argv"] else None
            assert {(x["agent"], model(x)) for x in c2 if x["agent"] != "rivet-sweeper" or "--resume" not in x["argv"]} <= {("rivet-architect", "fable"), ("rivet-sweeper", "fable"), ("rivet-verifier", None)}, c2
            assert json.loads((t / "two" / "tokens.json").read_text())["models"] == {"lens": "fable", "sweep": "fable"}
            assert sorted(x["head"][:22] for x in c2 if x["agent"] == "rivet-architect") == ["Lens runners: runners:", "Lens state-config-cli:"], c2
            data = json.loads((t / "two" / "classes.json").read_text())
            lk = next(x for x in data["classes"] if x["seam"] == "lock keys")
            assert lk["id"] == "C1" and lk["confirmed_by"] == ["runners", "state-config-cli"], lk
            tok = json.loads((t / "two" / "tokens.json").read_text())
            assert tok["usage"] is None and set(tok["lenses"]) == {"runners", "state-config-cli"}, tok
            summary = (t / "two" / "summary.md").read_text()
            assert "- lens runners: $" in summary and "- lens state-config-cli: $" in summary and "confirmed-by: runners, state-config-cli" in summary and "Usage: not read" in summary, summary
            assert USAGE_HINT in noise.getvalue()
            os.environ.pop("HUNT_FAKE_MODE")

            auto = lambda *extra: hunt("--auto", "--budget-usd", "30", *extra)
            assert auto().lenses[0].name == LENS_ORDER[0] and auto().out.name.endswith(f"-{LENS_ORDER[0]}-{LENS_ORDER[1]}")
            first = auto()
            assert first.run() == 0
            st = load_state(repo)
            head = git("rev-parse", "HEAD").stdout.strip()
            assert st["last_lens"] == 1 and st["last_rev"] == head and st["last_out"] == str(first.out), st
            assert json.loads(first.path("plan.json").read_text())["rules"] == []
            (repo / "src" / "state" / "lock.rs").write_text("fn lock_key() -> String { lock_name(export) }\n")
            git("commit", "-qam", "change lock")
            second = auto()
            assert [x.name for x in second.lenses] == [LENS_ORDER[2], LENS_ORDER[3]], "the rotation did not advance"
            plan = second.plan()
            assert second.titles == [title] and any("1 kept" in x for x in plan), plan
            assert second.run() == 0
            assert json.loads(second.path("plan.json").read_text())["rules"] == [title]
            assert any(x["head"].startswith(f"Predicate 1: {title}") for x in calls()), "the changed-seam rule was not swept"
            assert load_state(repo)["last_lens"] == 3 and load_state(repo)["last_rev"] == git("rev-parse", "HEAD").stdout.strip()
            (repo / "src" / "state" / "lock.rs").write_text("fn lock_key() -> String { lock_name(export, source) }\n")
            git("commit", "-qam", "fix lock")
            pre = auto()
            assert pre.plan() and pre.titles == [title], "a changed lock rule was not planned before the close"
            cid = next(x["id"] for x in json.loads(second.path("classes.json").read_text())["classes"] if x["seam"] == "lock keys")
            assert close_class(repo, cid, 812, None) == 0
            assert [r["pr"] for r in read_history() if r["out"] == str(second.out) and r["class"] == cid] == [812] and sum(r["pr"] == 812 for r in read_history()) == 1
            closed = load_state(repo)["closed"]
            assert closed == [{"class": cid, "seam": "lock keys", "files": ["src/state/lock.rs"], "pr": 812, "date": closed[0]["date"], "out": str(second.out)}], closed
            third = auto()
            plan = third.plan()
            assert [x.name for x in third.lenses] == [LENS_ORDER[4], LENS_ORDER[5]] and third.titles == [] and any("0 kept" in x and "2 changed sites skipped as closed" in x for x in plan), plan
            assert close_class(repo, "C99", 1, None) == 2
            size = len(read_history())
            assert backfill(t / "full") == 0 and len(read_history()) == size, "a backfill of a journaled round added records"
            old = t / "2026-01-01-old"
            shutil.copytree(t / "full", old)
            (old / "tokens.json").write_text(json.dumps({"total": {"cost_usd": 4.0}}))
            (old / "summary.md").write_text("# Hunt: state, locks and leases\n")
            assert backfill(old) == 0 and backfill(old) == 0 and len(read_history()) == size + 3
            back = [r for r in read_history() if r["out"] == str(old.resolve())]
            assert {r["date"] for r in back} == {"2026-01-01"} and back[0]["lenses"] == ["state, locks and leases"] and back[0]["cost"]["lenses"] == {"state, locks and leases": 4.0}, back[0]
            assert back[0]["rev"] == "x", back[0]
            rep = "\n".join(history_report(read_history()))
            assert "lock keys [" in rep and "back after PR #812" not in rep and "Trend per round:" in rep, rep

            os.environ["HUNT_FAKE_MODE"] = "fail-lens"
            mapped[:] = []
            h = single("fail")
            assert h.run() == 1 and not list((t / "fail").glob("candidates/*.md")), "a failed lens left a report"
            assert not wt.exists(), "a failed hunt left its worktree"
            assert str(wt) not in git("worktree", "list").stdout and mapped[-1] == (wt, None)
        except BaseException:
            quiet.close()
            sys.stderr.write(noise.getvalue()[-3000:])
            raise
        finally:
            quiet.close()
            seams.SEAMS, seams.write_map = seams_keep
            assert (real_history.read_bytes() if real_history.is_file() else None) == real_before, "the self-test touched the real hunt history"
            for k, v in env_keep.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
    print("hunt self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    """List lenses, close a class, plan, run, or self-test a hunt."""
    a = parser().parse_args(argv)
    if a.self_test:
        return self_test()
    if a.list_lenses:
        print("\n".join(lens_table()))
        return 0
    if a.history:
        print("\n".join(history_report(read_history())))
        return 0
    if a.backfill:
        try:
            return backfill(a.backfill)
        except HuntError as e:
            print(f"hunt: {e}", file=sys.stderr)
            return 2
    if a.close:
        if a.pr is None:
            parser().error("--close needs --pr N")
        return close_class(ROOT, a.close, a.pr, a.out)
    if not (a.lens or a.lenses or a.auto):
        parser().error("--lens, --lenses or --auto is required")
    try:
        h = Hunt(a)
        if a.dry_run:
            print("\n".join(h.plan()))
            return 0
        return h.run()
    except HuntError as e:
        print(f"hunt: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
