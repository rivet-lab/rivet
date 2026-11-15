"""One markdown report of a paired release gate on one sha: the hosted half (a gate-hosted run on main) beside the devbox half (its record line).

    python3 -m dev.pytools.paired_report --sha SHA [--devbox-record FILE | --ssh HOST] [--devbox-log FILE] [--run-id N] [--out FILE]
    python3 -m dev.pytools.paired_report --self-test

The verdicts are `release_halves`' own (its readers and HALVES_* codes, never a second contract). The hosted half is read with `gh`: the
`hosted-verdict-<ref>` artifact, run.json and jobs.json for the contract, the `slice-N` artifacts and the sampler check runs for the detail.
The devbox half is the latest `release-oracle-full-devbox` line of SHA in the gate record, read over `ssh HOST 'cat RECORD; echo RC=$?'` and nothing else.
Red cells are classified with `nightly_delta`: GREEN, KNOWN (ledger entry in date, with expiry and issue), NEW, LOST, INFRA. Every line names the ref and sha it came from.
Exit 0 when both halves are green and cover the gate, 1 when the report was written and is not green (a missing half included), 2 on usage.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / ".github" / "scripts"))
import gate_history  # noqa: E402
import nightly_delta as ndelta  # noqa: E402

from dev.pytools import release, release_halves as rh  # noqa: E402
from dev.release_oracle import known_red  # noqa: E402
from dev.release_oracle.core import DEVBOX, HOSTED, half_gate  # noqa: E402
from dev.release_oracle.halves import STAGE_HALF  # noqa: E402

hosted = gate_history.hosted
DEVBOX_RECORD = "~/wt/gate/dev/release-oracle/timings.jsonl"
SSH_TIMEOUT_S = 90
TOP = 15
CUT = 160
INFRA = "INFRA"
BANNER = re.compile(r"^▸ (.+)$")
KNOWN_LINE = re.compile(r"KNOWN RED \(until (\d{4}-\d\d-\d\d): (.*?)\) — (.*)$")
PROGRESS = re.compile(r"^(?:[\w.-]+: (?:round \d+/\d+, )?\d+ cells|(?:odd|even): \d+ of \d+ columns)")
MATRIX_OF = re.compile(r"^([a-z][a-z0-9_]*)\[")
ISSUE = re.compile(r"#(\d{1,5})\b")
RC_TAIL = re.compile(r"RC=(\d+)\s*$")
SAMPLER_ROW = re.compile(r"^\d\d:\d\d:\d\d\t")
NO_STAGE = "(no stage name matches)"
Gh = rh.Gh
Run = Callable[..., subprocess.CompletedProcess]


@dataclass
class Slice:
    """One live slice of the hosted half as its artifact, job and sampler hold it."""

    index: int
    record: dict | None = None
    exit_code: int | None = None
    log: str = ""
    job: dict | None = None
    sampler: dict | None = None


@dataclass
class Hosted:
    """The hosted half of one sha: the run, its slices and why anything is missing."""

    sha: str
    run_id: str | None = None
    run: dict = field(default_factory=dict)
    jobs: list = field(default_factory=list)
    slices: list[Slice] = field(default_factory=list)
    root: Path | None = None
    contract: Path | None = None
    fetch_error: str | None = None
    problem: str | None = None

    @property
    def ref(self) -> str:
        """The branch name of the run, `main` when there is no run."""
        return str(self.run.get("head_branch") or rh.BRANCH)

    @property
    def tag(self) -> str:
        """The prefix of every line from this half."""
        return f"[H {self.ref}@{self.sha[:8]}]"


@dataclass
class Devbox:
    """The devbox half of one sha: its latest record line, the line before it and an optional gate log."""

    sha: str
    source: str
    error: str | None = None
    path: Path | None = None
    rec: dict | None = None
    prev: dict | None = None
    log: str = ""

    @property
    def tag(self) -> str:
        """The prefix of every line from this half."""
        return f"[D devbox@{self.sha[:8]}]"


@dataclass
class Report:
    """Everything the markdown is made of."""

    sha: str
    hosted: Hosted
    devbox: Devbox
    history: list[dict] | None
    history_error: str | None
    head: str
    now: dt.datetime
    repo: str

    @property
    def tag(self) -> str:
        """The prefix of every line about both halves."""
        return f"[P {self.hosted.ref}@{self.sha[:8]}+devbox@{self.sha[:8]}]"


def ssh_record(host: str, run: Run = subprocess.run) -> tuple[str, str | None]:
    """(the gate record text, an error) read by `ssh HOST 'cat RECORD; echo RC=$?'`; the error unless the remote cat said RC=0."""
    try:
        p = run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", host, f"cat {DEVBOX_RECORD}; echo RC=$?"], capture_output=True, text=True, timeout=SSH_TIMEOUT_S)
    except (OSError, subprocess.SubprocessError) as e:
        return "", f"ssh {host} failed: {type(e).__name__}: {e}"
    m = RC_TAIL.search(p.stdout)
    if not m:
        return "", f"ssh {host} printed no RC= line (exit {p.returncode}): {p.stderr.strip()[:200] or 'no stderr'}"
    if m[1] != "0":
        return "", f"ssh {host}: cat {DEVBOX_RECORD} exited {m[1]}: {p.stderr.strip()[:200] or 'no stderr'}"
    return p.stdout[:m.start()], None


def load_devbox(sha: str, text: str, error: str | None, source: str, work: Path, log: str = "") -> Devbox:
    """The devbox half of `sha` from the gate record `text`, kept as a file in `work` for `release_halves`' readers."""
    d = Devbox(sha=sha, source=source, error=error, log=log)
    if error:
        return d
    d.path = work / "devbox-timings.jsonl"
    d.path.write_text(text)
    kind = half_gate(DEVBOX)
    runs, _ = release.gate_runs(sha, d.path, kind)
    d.rec = runs[-1] if runs else None
    lines = []
    for line in text.splitlines():
        try:
            lines.append(json.loads(line))
        except ValueError:
            continue
    at = max((i for i, r in enumerate(lines) if d.rec is not None and r == d.rec), default=len(lines))
    d.prev = next((r for r in reversed(lines[:at]) if isinstance(r, dict) and r.get("gate") == kind and r.get("sha") != sha), None)
    return d


def parse_sampler(text: str) -> dict | None:
    """Samples, peak memory used, minimum memory available, peak swap, OOM kills and minimum free disk of a sampler series; None when it holds no row."""
    rows = [line.split("\t") for line in text.splitlines() if SAMPLER_ROW.match(line)]
    if not rows:
        return None

    def col(i: int) -> list[int]:
        """The integers of column `i`; a `-` or a short row is skipped."""
        return [int(r[i]) for r in rows if len(r) > i and r[i].lstrip("-").isdigit()]
    used, avail, swap, disk, oom = col(2), col(3), col(4), col(6), col(7)
    return {"samples": len(rows), "peak_used_mb": max(used, default=None), "min_avail_mb": min(avail, default=None), "peak_swap_mb": max(swap, default=None),
            "oom_kills": max(oom, default=0), "min_disk_gb": min(disk, default=None), "elapsed_s": int(rows[-1][1]) if rows[-1][1].isdigit() else None}


def sampler_check_runs(repo: str, sha: str, run_id: str, attempt: str, gh: Gh) -> dict[int, dict]:
    """The sampler series of each slice of run `run_id`, read from the check runs of `sha` (they survive a lost runner)."""
    try:
        out = gh(["api", "--paginate", f"repos/{repo}/commits/{sha}/check-runs?per_page=100", "--jq", ".check_runs[] | [.name, .output.text] | @json"])
    except (subprocess.CalledProcessError, OSError):
        return {}
    found: dict[int, dict] = {}
    for line in out.splitlines():
        try:
            name, text = json.loads(line)
        except ValueError:
            continue
        m = re.fullmatch(rf"slice (\d+) run {re.escape(run_id)}\.{re.escape(attempt)} sampler", str(name))
        if m and (stats := parse_sampler(str(text or ""))):
            found[int(m[1])] = stats
    return found


def slice_count(verdict: object, root: Path, jobs: list) -> int:
    """The live slice count: the verdict's, else the highest slice any artifact or job names."""
    if n := rh.slices_of(verdict):
        return n
    found = [int(m[1]) for p in root.glob("slice-*") if (m := re.fullmatch(r"slice-(\d+)", p.name))]
    named = [int(m[1]) for j in jobs if isinstance(j, dict) and (m := re.match(r"slice (\d+)\b", str(j.get("name"))))]
    return max(found + named, default=0)


def job_of(jobs: list, index: int) -> dict | None:
    """The job of slice `index` in a job list."""
    return next((j for j in jobs if isinstance(j, dict) and re.match(rf"slice {index}\b", str(j.get("name")))), None)


def read_slice(root: Path, index: int, jobs: list) -> Slice:
    """The artifact of slice `index` under `root`; whatever is missing stays None."""
    try:
        rec, log = hosted.load_slice(root, index)
    except ValueError:
        rec, log = None, ""
    code = hosted.load_side(root, index, "exit-code.txt")
    tsv = root / f"slice-{index}" / "sampler.tsv"
    series = parse_sampler(tsv.read_text(errors="replace")) if tsv.is_file() else None
    return Slice(index, rec, code if isinstance(code, int) else None, log, job_of(jobs, index), series)


def newest_run_of(sha: str, repo: str, gh: Gh) -> str | None:
    """The newest completed hosted run on main whose head is `sha`."""
    try:
        runs = rh.hosted_runs(repo, gh)
    except (subprocess.CalledProcessError, OSError, ValueError):
        return None
    return next((str(r["databaseId"]) for r in runs if r.get("headSha") == sha and r.get("status") == "completed"), None)


def load_hosted(sha: str, run_id: str | None, repo: str, work: Path, gh: Gh = rh.run_gh) -> Hosted:
    """The hosted half of `sha`: the contract inputs `release_halves` reads, then the slice artifacts and sampler check runs of the same run."""
    h = Hosted(sha=sha, contract=work / "contract", root=work / "slices")
    h.contract.mkdir()
    h.fetch_error = rh.fetch_hosted(sha, h.contract, repo, gh, run_id=run_id)
    rid = run_id or (str((rh.load_json(h.contract / "run.json") or {}).get("id") or "") or None) or newest_run_of(sha, repo, gh)
    if rid is None:
        h.problem = f"no completed run of {rh.WORKFLOW} on {rh.BRANCH} for {sha[:12]} among the last {rh.RUN_LIMIT}"
        return h
    h.run_id = str(rid)
    try:
        run_text, jobs_text = ((h.contract / "run.json").read_text(), (h.contract / "jobs.json").read_text()) if h.fetch_error is None else rh.run_records(repo, rid, gh)
        h.run, jobs = json.loads(run_text), json.loads(jobs_text)
    except (subprocess.CalledProcessError, OSError, ValueError) as e:
        h.problem = f"run {rid}: GitHub's run record or job list could not be read ({type(e).__name__}: {e})"
        return h
    h.jobs = jobs.get("jobs", []) if isinstance(jobs, dict) else []
    try:
        gh(["run", "download", h.run_id, "--repo", repo, "--pattern", "slice-*", "--dir", str(h.root)])
    except (subprocess.CalledProcessError, OSError) as e:
        h.problem = f"run {rid}: no slice artifact could be downloaded ({type(e).__name__})"
    h.root.mkdir(exist_ok=True)
    n = slice_count(rh.load_json(h.contract / "verdict.json"), h.root, h.jobs)
    checks = sampler_check_runs(repo, sha, h.run_id, str(h.run.get("run_attempt", 1)), gh)
    h.slices = [read_slice(h.root, i, h.jobs) for i in range(n + 1)] if n else []
    for s in h.slices:
        pick = [x for x in (checks.get(s.index), s.sampler) if x]
        s.sampler = max(pick, key=lambda x: x["samples"], default=None)
    return h


def minutes_between(a: object, b: object) -> float | None:
    """Minutes from ISO time `a` to `b`; None when either is missing or unreadable."""
    try:
        t0, t1 = (dt.datetime.fromisoformat(str(x).replace("Z", "+00:00")) for x in (a, b))
    except ValueError:
        return None
    return round((t1 - t0).total_seconds() / 60.0, 1)


def step_minutes(job: dict | None, needle: str) -> float | None:
    """Minutes of the first step of `job` whose name holds `needle`."""
    step = next((s for s in (job or {}).get("steps") or [] if needle in str(s.get("name"))), None)
    return minutes_between(step.get("started_at"), step.get("completed_at")) if step else None


def issue_of(reason: str) -> str:
    """`#N` of the first issue number a ledger reason names, else `-`."""
    m = ISSUE.search(reason)
    return f"#{m[1]}" if m else "-"


def first_failing(text: str) -> str:
    """The first `✗` line of a gate log, else its last non-blank line."""
    lines = [x.strip() for x in hosted.ANSI.sub("", text).splitlines() if x.strip()]
    return next((x for x in lines if x.startswith("✗")), lines[-1] if lines else "-")[:CUT]


def red_items(text: str, baseline: dict, today: dt.date) -> list[dict]:
    """Every red class of a gate log, each with its stage banner, class, expiry, issue and first line; `nightly_delta` decides KNOWN or NEW."""
    items, seen, stage = [], set(), "(before the first stage)"
    for raw in hosted.ANSI.sub("", text).splitlines():
        if (m := BANNER.match(raw)) and not PROGRESS.match(m[1]):
            stage = m[1].split(" — ")[0][:80]
        if m := KNOWN_LINE.search(raw):
            entry, _ = known_red.match(m[3])
            cell = MATRIX_OF.match(m[3])
            items.append({"cls": ndelta.KNOWN, "stage": f"matrix {cell[1]}" if cell else stage, "key": m[3][:100], "expires": m[1], "issue": issue_of(entry.reason if entry else m[2]), "line": m[2][:CUT]})
        keys, crashes = ndelta.red_keys(raw)
        for key in sorted(keys - seen):
            seen.add(key)
            graded = ndelta.grade_part(1, raw, baseline, today)
            ok = key in graded["known"]
            items.append({"cls": ndelta.KNOWN if ok else ndelta.NEW, "stage": stage, "key": key, "expires": baseline[key].expires if ok else "-",
                          "issue": issue_of(baseline[key].reason) if ok else "-", "line": raw.strip()[:CUT]})
        for key in sorted(crashes - seen):
            seen.add(key)
            items.append({"cls": INFRA if "runner shutdown" in key else ndelta.NEW, "stage": stage, "key": key, "expires": "-", "issue": "-", "line": raw.strip()[:CUT]})
        if (c := hosted.CELL.match(raw)) and c[5] == INFRA and (key := f"gate|{c[1]}/{c[3]}") not in seen:
            seen.add(key)
            items.append({"cls": INFRA, "stage": stage, "key": key, "expires": "-", "issue": "-", "line": raw.strip()[:CUT]})
    return items


def grade_log(code: int | None, text: str, lost: bool, baseline: dict, today: dt.date) -> tuple[str, list[dict]]:
    """(slice class, its red items): INFRA with no artifact at all, else `nightly_delta.grade_part` plus an item for an exit nothing explains."""
    if lost:
        return INFRA, [{"cls": INFRA, "stage": "(slice)", "key": "runner lost: no record, no log, no exit code", "expires": "-", "issue": "-", "line": "-"}]
    graded = ndelta.grade_part(code, text, baseline, today)
    items = red_items(text, baseline, today)
    if graded["verdict"] == ndelta.LOST:
        items.append({"cls": ndelta.LOST, "stage": "(slice)", "key": graded["new"][0], "expires": "-", "issue": "-", "line": first_failing(text)})
    for why in graded["new"] if not any(x["cls"] == INFRA for x in items) else []:
        if why.startswith("exit ") and "explains" in why or why == "red cells under exit 0":
            items.append({"cls": ndelta.NEW, "stage": "(slice exit)", "key": why, "expires": "-", "issue": "-", "line": first_failing(text)})
    seen = {x["cls"] for x in items}
    return next((c for c in (ndelta.LOST, ndelta.NEW, INFRA, ndelta.KNOWN) if c in seen), graded["verdict"]), items


def slice_grade(s: Slice, baseline: dict, today: dt.date) -> tuple[str, list[dict]]:
    """The class and red items of one hosted slice."""
    lost = s.record is None and s.exit_code is None and not s.log.strip()
    return grade_log(s.exit_code, s.log, lost, baseline, today)


def norm(text: str) -> str:
    """`text` lower-cased with every run of other characters one space."""
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def stage_of(title: str, stages: Sequence[str]) -> str:
    """The stage whose normalised name starts the phase title (else is a whole-word part of it), the longest such; `NO_STAGE` when none."""
    t = norm(title)
    hits = [s for s in stages if t.startswith(norm(s))] or [s for s in stages if f" {norm(s)} " in f" {t} "]
    return max(hits, key=len, default=NO_STAGE)


def phases(rec: dict | None) -> dict[str, float]:
    """The phase minutes of one record in banner order, a per-engine progress banner added to the phase before it; `{}` when it holds none."""
    p = (rec or {}).get("phases_min")
    out: dict[str, float] = {}
    last = None
    for k, v in (p.items() if isinstance(p, dict) else []):
        if PROGRESS.match(str(k)) and last is not None:
            out[last] = round(out[last] + float(v), 2)
            continue
        last = str(k)
        out[last] = round(out.get(last, 0.0) + float(v), 2)
    return out


def stage_minutes(recs: Sequence[dict | None], stages: Sequence[str]) -> dict[str, float]:
    """Minutes per stage name summed over `recs`' phases; a phase no stage names lands under its own title behind `NO_STAGE`."""
    out: dict[str, float] = {}
    for rec in recs:
        for title, m in phases(rec).items():
            key = stage_of(title, stages)
            key = key if key != NO_STAGE else f"{NO_STAGE}: {title[:60]}"
            out[key] = round(out.get(key, 0.0) + m, 2)
    return out


def hosted_stages(h: Hosted) -> list[str]:
    """Every stage name any hosted slice record lists, in first-seen order."""
    seen: dict[str, None] = {}
    for s in h.slices:
        for n in (s.record or {}).get("stages") or []:
            seen[str(n)] = None
    return list(seen)


def overlap(h: Hosted, d: Devbox, whole: Sequence[str]) -> list[dict]:
    """One row per stage of the gate table or of either record: what `STAGE_HALF` places, what ran where, and whether it is in both or neither; `neither` is only a flag for a stage whose half has a record."""
    ran_h, ran_d = set(hosted_stages(h)), set((d.rec or {}).get("stages") or [])
    present = {HOSTED: bool(h.slices), DEVBOX: d.rec is not None}
    rows = []
    for name in dict.fromkeys([*whole, *sorted(ran_h | ran_d)]):
        placed = STAGE_HALF.get(name, "-")
        where = "both" if name in ran_h and name in ran_d else "hosted" if name in ran_h else "devbox" if name in ran_d else "neither"
        owner_there = present.get(placed, all(present.values()))
        bad = where == "both" or (where == "neither" and name in whole and owner_there) or name not in whole or (where in (HOSTED, DEVBOX) and placed not in ("-", where))
        rows.append({"stage": name, "placed": placed, "ran": where, "in_gate": name in whole, "flag": bad})
    return rows


def whole_stages(n: int) -> tuple[list[str], str | None]:
    """(the stages of the whole gate from this checkout's table with `n` live slices, an error text)."""
    try:
        return rh.stage_cuts(max(n, 1), rh.default_stage_names)[0], None
    except SystemExit as e:
        return [], f"the stage table could not be read: {e}"


def verdicts(r: Report) -> dict[str, str | None]:
    """The contract refusal of each half and of the pair, None when it supports a tag; both read with `release_halves`."""
    h, d = r.hosted, r.devbox
    devbox: str | None = rh.refuse("SHA", f"the devbox record could not be read: {d.error}") if d.error else None
    if devbox is None and d.path is not None:
        devbox = rh.devbox_refusal(r.sha, d.path, [])
    if devbox is None and d.path is None:
        devbox = rh.refuse("SHA", "no devbox record was given")
    host = h.fetch_error or (rh.hosted_refusal(r.sha, h.contract, rh.workflow_plan(), r.repo, {}) if h.contract else rh.refuse("HOSTED", h.problem or "no hosted run"))
    if d.path is not None:
        devbox = devbox.replace(str(d.path), d.source) if devbox else devbox
    host = host.replace(str(h.contract), "hosted-verdict") if host and h.contract else host
    pair = devbox or host or (rh.halves_refusal(r.sha, d.path, h.contract, repo=r.repo) if d.path and h.contract else rh.refuse("HOSTED", "a half is missing"))
    pair = pair.replace(str(d.path), d.source) if pair and d.path is not None else pair
    return {"hosted": host, "devbox": devbox, "pair": pair}


def md(cells: Sequence[object]) -> str:
    """One markdown table row."""
    return "| " + " | ".join(str(c).replace("|", "\\|").replace("\n", " ") for c in cells) + " |"


def table(head: Sequence[str], rows: Sequence[Sequence[object]], tag: str) -> list[str]:
    """A markdown table; with no rows, one row that says `no rows` under `tag`."""
    return [md(head), md(["---"] * len(head)), *(md(r) for r in rows or [[tag, "no rows", *["-"] * (len(head) - 2)]])]


def num(x: object) -> str:
    """A number for a table cell, `-` when missing."""
    return "-" if x is None else f"{x:.1f}" if isinstance(x, float) else str(x)


def header(r: Report) -> list[str]:
    """Section 1: sha, refs, host and binary stamp of each half, run url, devbox record time."""
    h, d = r.hosted, r.devbox
    recs = [s.record for s in h.slices if s.record]
    hosts = sorted({f"{(x.get('host') or {}).get('system')}/{(x.get('host') or {}).get('machine')}" for x in recs})
    stamps = sorted({str(x.get("binary")) for x in recs})
    out = [f"# Paired gate report for {r.sha}", "", "## 1. Header", "", f"- {r.tag} sha `{r.sha}`, report made {r.now.strftime('%Y-%m-%d %H:%M:%SZ')} from checkout `{r.head[:12]}`"
           + ("" if r.head == r.sha else " (not the graded sha: the stage table below is the checkout's)")]
    if h.run_id:
        out.append(f"- {h.tag} ref `refs/heads/{h.ref}`, event `{h.run.get('event')}`, run {h.run.get('html_url') or h.run_id} attempt {h.run.get('run_attempt')}, "
                   f"{h.run.get('created_at')} to {h.run.get('updated_at')}, host {', '.join(hosts) or '-'}, binary stamp {', '.join(stamps) or '-'} "
                   f"({len({x.get('binary_sha256') for x in recs})} distinct binaries over {len(recs)} slice records)")
    else:
        out.append(f"- {h.tag} no record for this sha: {h.problem or h.fetch_error}")
    rec = d.rec
    out.append(f"- {d.tag} ref none recorded (the record keys on the sha), source {d.source}, " + (
        f"record `{rec.get('at')}`, host {(rec.get('host') or {}).get('system')}/{(rec.get('host') or {}).get('machine')}, binary stamp {rec.get('binary')}, "
        f"sha256 {str(rec.get('binary_sha256'))[:12]}, config {json.dumps(rec.get('config'), sort_keys=True)}" if rec
        else f"record unreadable: {d.error}" if d.error else "no record for this sha"))
    return out


def verdict_section(r: Report) -> list[str]:
    """Section 2: contract verdict and observed facts per half and combined."""
    v, h, d = verdicts(r), r.hosted, r.devbox
    green = [s for s in h.slices if s.record and s.record.get("verdict") == hosted.GREEN and s.exit_code == 0]
    rows = [[h.tag, "GREEN" if v["hosted"] is None else f"REFUSED {v['hosted']}", f"{len(green)}/{len(h.slices)} slices RELEASE-READY with exit 0" if h.slices else "no slice record"],
            [d.tag, "GREEN" if v["devbox"] is None else f"REFUSED {v['devbox']}", f"record verdict {d.rec.get('verdict')}" if d.rec else "no record for this sha"],
            [r.tag, "GREEN" if v["pair"] is None else f"REFUSED {v['pair']}", "both halves support a tag on this sha" if v["pair"] is None else "the pair does not support a tag"]]
    return ["## 2. Verdicts (release_halves contract)", "", *table(["source", "contract", "observed"], rows, r.tag)]


def numbers_section(r: Report) -> list[str]:
    """Section 3: wall minutes, passed/failed/skipped and env skips per half."""
    h, d = r.hosted, r.devbox
    recs = [s.record for s in h.slices if s.record]
    gates = [float(x.get("total_min") or 0) for x in recs]
    wall = minutes_between(h.run.get("created_at"), h.run.get("updated_at"))
    total = lambda k, rs: sum(int(x.get(k) or 0) for x in rs)  # noqa: E731
    rows = []
    if h.run_id:
        rows.append([h.tag, f"{num(wall)} run wall, {num(max(gates, default=None))} slowest slice, {num(round(sum(gates), 1))} summed", total("passed", recs), total("failed", recs), total("skipped", recs),
                     sum(len(x.get("skipped_env") or []) for x in recs)])
    if d.rec:
        rows.append([d.tag, f"{num(d.rec.get('total_min'))} gate", total("passed", [d.rec]), total("failed", [d.rec]), total("skipped", [d.rec]), len(d.rec.get("skipped_env") or [])])
    out = ["## 3. Wall minutes and counts", "", *table(["source", "minutes", "passed", "failed", "skipped", "skipped_env"], rows, r.tag)]
    for tag, rs in ((h.tag, recs), (d.tag, [d.rec] if d.rec else [])):
        cells = sorted({"/".join(map(str, c)) for x in rs for c in x.get("skipped_env") or []})
        if cells:
            out += ["", f"{tag} skipped_env ({len(cells)}):", *(f"- {tag} {c}" for c in cells[:30]), *([f"- {tag} ... {len(cells) - 30} more"] if len(cells) > 30 else [])]
    return out


def red_section(r: Report, today: dt.date) -> list[str]:
    """Section 4: every red cell of both halves classified, grouped by stage with its first failing line."""
    base = known_red.nightly_classes()
    h, d = r.hosted, r.devbox
    items: list[tuple[str, str, dict]] = []
    per_slice = []
    for s in h.slices:
        cls, found = slice_grade(s, base, today)
        per_slice.append([h.tag, f"slice {s.index}", s.exit_code if s.exit_code is not None else "none", cls, len(found)])
        items += [(h.tag, f"slice {s.index}", x) for x in found]
    note = []
    if d.rec and d.log:
        code = 0 if d.rec.get("verdict") == hosted.GREEN else 1
        cls, found = grade_log(code, d.log, False, base, today)
        per_slice.append([d.tag, "gate log", code, cls, len(found)])
        items += [(d.tag, "gate", x) for x in found]
    elif d.rec:
        note.append(f"- {d.tag} the record holds counts only (failed {d.rec.get('failed')}, verdict {d.rec.get('verdict')}), so its red cells are not known: pass --devbox-log FILE with the gate's output")
    out = ["## 4. Red cells", "", f"{r.tag} classes: GREEN no red line; KNOWN a ledger entry in date (expiry, issue); NEW a class no entry acknowledges or an expired one; LOST exit 137/143; INFRA runner lost or an INFRA cell.", "",
           *table(["source", "part", "exit", "class", "red items"], per_slice, r.tag), *note]
    for stage in sorted({x["stage"] for _, _, x in items}):
        out += ["", f"### stage: {stage}", "", *table(["source", "part", "class", "key", "expires", "issue", "first line"],
                [[t, p, x["cls"], x["key"], x["expires"], x["issue"], x["line"]] for t, p, x in items if x["stage"] == stage], r.tag)]
    read = f"{len(h.slices)} hosted slice logs, devbox log {'read' if d.log else 'not read'}"
    return out + ([] if items else ["", f"- {r.tag} no red cell in the logs read ({read})"])


def ranked(rec: dict | None) -> list[tuple[str, float]]:
    """The `TOP` slowest phases of one record."""
    return sorted(phases(rec).items(), key=lambda p: -p[1])[:TOP]


def minutes_section(r: Report) -> list[str]:
    """Section 5: slowest phases per half and one row per hosted slice with its sampler figures."""
    h, d = r.hosted, r.devbox
    out = ["## 5. Stage minutes"]
    top = sorted(((f"slice {s.index}", t, m) for s in h.slices for t, m in phases(s.record).items()), key=lambda x: -x[2])[:TOP]
    out += ["", f"{h.tag} slowest {TOP} phases over all slices", "", *table(["source", "slice", "phase", "min"], [[h.tag, a, t[:70], m] for a, t, m in top], h.tag)]
    out += ["", f"{d.tag} slowest {TOP} phases", "", *table(["source", "phase", "min"], [[d.tag, t[:70], m] for t, m in ranked(d.rec)], d.tag)]
    rows = []
    for s in h.slices:
        sm, rec = s.sampler or {}, s.record or {}
        best = ranked(rec)[:1]
        rows.append([h.tag, s.index, num(minutes_between((s.job or {}).get("started_at"), (s.job or {}).get("completed_at"))), num(step_minutes(s.job, "live-stack")),
                     num(rec.get("total_min")), f"{rec.get('passed', '-')}/{rec.get('failed', '-')}/{rec.get('skipped', '-')}", num(sm.get("peak_used_mb")), num(sm.get("min_avail_mb")),
                     num(sm.get("peak_swap_mb")), sm.get("oom_kills", "-"), num(sm.get("min_disk_gb")), sm.get("samples", "no sampler"), f"{best[0][0][:40]} {best[0][1]}" if best else "-"])
    return out + ["", f"{h.tag} per slice (sampler: check run or artifact, whichever holds more samples)", "", *table(
        ["source", "slice", "job min", "setup min", "gate min", "pass/fail/skip", "peak used MB", "min avail MB", "peak swap MB", "OOM kills", "min disk GB", "samples", "slowest phase"], rows, h.tag)]


def overlap_section(r: Report) -> list[str]:
    """Section 6: stages run on both halves or neither, and one stage -> minutes table per half."""
    h, d = r.hosted, r.devbox
    whole, err = whole_stages(len(h.slices) - 1 if h.slices else 1)
    rows = overlap(h, d, whole)
    out = ["## 6. Overlap and stage minutes", "", f"{r.tag} stage table of checkout {r.head[:8]}" + (f": {err}" if err else f": {len(whole)} stages")]
    missing = [t for t, ok in ((h.tag, bool(h.slices)), (d.tag, bool(d.rec))) if not ok]
    out += [f"- {r.tag} partial: {' and '.join(missing)} has no record, so `neither` and `both` below are about the other half only"] if missing else []
    bad = [x for x in rows if x["flag"]]
    out += ["", *(f"- {r.tag} FLAG stage `{x['stage']}` placed {x['placed']}, ran {x['ran']}{'' if x['in_gate'] else ', not in the gate table'}" for x in bad)] if bad else ["", f"- {r.tag} no stage is flagged among the halves with a record" if missing else f"- {r.tag} every stage ran on exactly one half, where it is placed"]
    hm = stage_minutes([s.record for s in h.slices], [x["stage"] for x in rows])
    dm = stage_minutes([d.rec], [x["stage"] for x in rows])
    names = sorted(set(hm) | set(dm), key=lambda n: -max(hm.get(n, 0), dm.get(n, 0)))
    placed = {x["stage"]: x for x in rows}
    return out + ["", f"{r.tag} stage -> minutes (hosted summed over slices that run in parallel)", "", *table(["source", "stage", "placed", "ran", "hosted min", "devbox min"], [
        [r.tag, n[:80], placed.get(n, {}).get("placed", "-"), placed.get(n, {}).get("ran", "-"), num(hm.get(n)), num(dm.get(n))] for n in names[:40]], r.tag)]


def delta_section(r: Report) -> list[str]:
    """Section 7: this hosted run against the previous gate-history line of its ref, and the devbox record against the line before it."""
    h, d = r.hosted, r.devbox
    out = ["## 7. Delta against the previous record", ""]
    if r.history is None:
        out.append(f"- {h.tag} gate-history unreadable: {r.history_error}")
    elif not h.slices:
        out.append(f"- {h.tag} no hosted run, nothing to compare")
    else:
        ref = f"refs/heads/{h.ref}"
        lines = gate_history.show(r.history, ref, None)
        at = next((i for i, x in enumerate(lines) if str(x.get("run_id")) == h.run_id), len(lines))
        prev = lines[at - 1] if at else None
        ident = {"ref": ref, "ref_name": h.ref, "ref_type": "branch", "event": str(h.run.get("event")), "run_id": str(h.run_id), "run_attempt": str(h.run.get("run_attempt", 1)),
                 "run_url": str(h.run.get("html_url") or ""), "workflow_sha": str(h.run.get("head_sha") or r.sha)}
        cur = gate_history.history_line(h.root, r.sha, len(h.slices) - 1, ident, prev)
        out += delta_lines(h.tag, cur, prev) if prev else [f"- {h.tag} gate-history holds no earlier line of {ref}: no delta (this run {cur['verdict']}, {tests_of(cur)})"]
    rec, prev = d.rec, d.prev
    if rec and prev:
        diff = sorted(((phases(rec).get(n, 0.0) - phases(prev).get(n, 0.0), n) for n in set(phases(rec)) | set(phases(prev))), reverse=True)
        out += ["", f"- {d.tag} against devbox@{str(prev.get('sha'))[:8]} ({prev.get('at')}): total {prev.get('total_min')} -> {rec.get('total_min')} min, "
                f"passed {prev.get('passed')} -> {rec.get('passed')}, failed {prev.get('failed')} -> {rec.get('failed')}, skipped {prev.get('skipped')} -> {rec.get('skipped')}",
                *[f"- {d.tag} phase {n[:60]}: {x:+.1f} min" for x, n in dict.fromkeys([*diff[:5], *diff[-3:]]) if abs(x) >= 0.5]]
    elif rec:
        out.append(f"- {d.tag} no earlier {half_gate(DEVBOX)} line in the record: no delta")
    return out


def tests_of(line: dict) -> str:
    """`P passed, F failed, S skipped` of a gate-history line."""
    t = line.get("tests") or {}
    return f"{t.get('passed')} passed, {t.get('failed')} failed, {t.get('skipped')} skipped"


def delta_lines(tag: str, cur: dict, prev: dict) -> list[str]:
    """The lines that say how a gate-history line differs from the one before it."""
    ref = f"{prev.get('ref_name')}@{str(prev.get('sha'))[:8]}"
    out = [f"- {tag} against {ref} (run {prev.get('run_id')}): verdict {prev.get('verdict')} -> {cur['verdict']}, tests {tests_of(prev)} -> {tests_of(cur)}"]
    before = {p["slice"]: p.get("minutes") for p in prev.get("slices", [])}
    out += [f"- {tag} slice {p['slice']} minutes {before.get(p['slice'])} -> {p.get('minutes')}" for p in cur["slices"]]
    out += [f"- {tag} NEW red key {k}" for k in cur["delta"]["new"]] + [f"- {tag} FIXED red key {k}" for k in cur["delta"]["fixed"]]
    return out + ([] if cur["delta"]["new"] or cur["delta"]["fixed"] else [f"- {tag} the red keys are the same as {ref}"])


def render(r: Report, today: dt.date | None = None) -> str:
    """The whole report."""
    today = today or r.now.date()
    parts = [header(r), verdict_section(r), numbers_section(r), red_section(r, today), minutes_section(r), overlap_section(r), delta_section(r)]
    return "\n".join(line for part in parts for line in [*part, ""]).rstrip() + "\n"


def history_lines(repo: str) -> tuple[list[dict] | None, str | None]:
    """(the lines of the gate-history branch, None) or (None, why they could not be read)."""
    try:
        origin = subprocess.run(["git", "-C", str(ROOT), "remote", "get-url", "origin"], capture_output=True, text=True, check=True).stdout.strip()
        with tempfile.TemporaryDirectory() as tmp:
            return gate_history.clone_branch(Path(tmp), origin, ""), None
    except (subprocess.CalledProcessError, OSError, ValueError, SystemExit) as e:
        return None, f"{type(e).__name__}: {e}"


def head_sha() -> str:
    """The commit of this checkout."""
    return subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()


def gather(sha: str, work: Path, *, run_id: str | None, record: Path | None, host: str, devbox_log: Path | None, gh: Gh = rh.run_gh, run: Run = subprocess.run,
           history: Callable[[str], tuple[list[dict] | None, str | None]] = history_lines, now: dt.datetime | None = None) -> Report:
    """Read both halves of `sha` and the history into one `Report`."""
    repo = rh.repo_slug()
    text, err = (record.read_text(), None) if record and record.is_file() else ("", f"{record} is not a file") if record else ssh_record(host, run)
    log = devbox_log.read_text(errors="replace") if devbox_log and devbox_log.is_file() else ""
    devbox = load_devbox(sha, text, err, str(record) if record else f"ssh {host}:{DEVBOX_RECORD}", work, log)
    half = load_hosted(sha, run_id, repo, work, gh)
    lines, why = history(repo)
    return Report(sha, half, devbox, lines, why, head_sha(), now or dt.datetime.now(dt.timezone.utc), repo)


def main(argv: Sequence[str]) -> int:
    """Print or write the report; 0 when both halves are green, 1 when not, 2 on usage."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sha")
    ap.add_argument("--devbox-record", type=Path)
    ap.add_argument("--ssh", default="devbox")
    ap.add_argument("--devbox-log", type=Path)
    ap.add_argument("--run-id")
    ap.add_argument("--out", type=Path)
    ap.add_argument("--self-test", action="store_true")
    ns = ap.parse_args(argv)
    if ns.self_test:
        return self_test()
    if not (ns.sha and re.fullmatch(r"[0-9a-f]{40}", ns.sha)):
        ap.error("--sha is required and must be a full 40-hex commit sha")
    with tempfile.TemporaryDirectory() as tmp:
        r = gather(ns.sha, Path(tmp), run_id=ns.run_id, record=ns.devbox_record, host=ns.ssh, devbox_log=ns.devbox_log)
        text = render(r)
        green = verdicts(r)["pair"] is None
    if ns.out:
        ns.out.write_text(text)
        print(f"{r.tag} report written to {ns.out}: {'GREEN' if green else 'NOT GREEN'}")
    else:
        sys.stdout.write(text)
    return 0 if green else 1


SHA = rh.SHA
RUN = rh.RUN_ID
NOW = dt.datetime(2026, 11, 15, 20, 0, tzinfo=dt.timezone.utc)
TODAY = dt.date(2026, 11, 15)
CLEAN_LOG = "▸ Release build path\n  ✓ built\n  postgres 16 live s3 PASS ok\n"
SAMPLER = "time_utc\telapsed_s\tmem_used_mb\tmem_avail_mb\tswap_used_mb\tload1\tdisk_free_gb\toom_kills\n10:00:00\t0\t4000\t9000\t0\t1.0\t60\t0\n10:01:00\t60\t12000\t2000\t512\t3.0\t55\t0\n"


def fixture(root: Path, *, slices: int = 3, logs: dict[int, str] | None = None, codes: dict[int, int] | None = None, sha: str = SHA) -> tuple[dict, Gh]:
    """(a green pair of `sha` as data with its slice artifacts written under `root`, a fake `gh` that serves them)."""
    d = rh.green(slices, sha)
    d["run"] = {**d["run"], "html_url": f"https://example.invalid/runs/{RUN}", "created_at": "2026-11-15T08:00:00Z", "updated_at": "2026-11-15T09:30:00Z", "head_sha": sha}
    steps = [{"name": "Run ./.github/actions/live-stack", "started_at": "2026-11-15T08:02:00Z", "completed_at": "2026-11-15T08:12:00Z"}]
    d["jobs"] = {"jobs": [{**j, "started_at": "2026-11-15T08:01:00Z", "completed_at": "2026-11-15T09:01:00Z", "steps": steps} for j in d["jobs"]["jobs"]]}
    art = root / "art"
    for e in d["verdict"]["records"]:
        i = e["slice"]
        e["exit_code"] = (codes or {}).get(i, 0)
        e["record"].update(passed=10, failed=0, skipped=1, total_min=40.0 + i, phases_min={"Release build path (lock sync)": 9.0, "postgres: round 1/2, 5 cells": 1.0, "live modules · every other (3)": 30.0 + i})
        (art / f"slice-{i}").mkdir(parents=True)
        (art / f"slice-{i}" / "record.json").write_text(json.dumps(e["record"]))
        (art / f"slice-{i}" / "gate.log").write_text((logs or {}).get(i, CLEAN_LOG))
        (art / f"slice-{i}" / "exit-code.txt").write_text(f"{e['exit_code']}\n")
        (art / f"slice-{i}" / "sampler.tsv").write_text(SAMPLER)
    d["devbox"][0].update(passed=20, failed=0, skipped=2, total_min=55.5, phases_min={"cdc standby": 7.0, "replica read": 2.0})
    return d, fake_gh(d, art, sha)


def fake_gh(d: dict, art: Path, sha: str) -> Gh:
    """A `gh` serving the fixture `d` and the slice artifacts under `art`: one completed run of `sha`, its records and its check runs."""
    def gh(args: Sequence[str]) -> str:
        """The answer of `gh ARGS`; anything this fake does not serve raises like a failed call."""
        a = list(args)
        fail = subprocess.CalledProcessError(1, a)
        if a[:2] == ["run", "list"]:
            return json.dumps([{"databaseId": RUN, "status": "completed", "headSha": sha, "conclusion": "success"}] if d["verdict"] else [])
        if a[:2] == ["run", "download"]:
            out = Path(a[a.index("--dir") + 1])
            if a[2] != str(RUN):
                raise fail
            if "--name" in a:
                out.mkdir(parents=True)
                (out / "hosted-verdict.json").write_text(json.dumps(d["verdict"]))
            else:
                shutil.copytree(art, out)
            return ""
        if a[0] == "api" and "--paginate" in a:
            return json.dumps(["slice 0 run %s.1 sampler" % RUN, "```\n" + SAMPLER + "```"]) + "\n"
        if a[0] == "api" and a[1].endswith(f"/runs/{RUN}"):
            return json.dumps(d["run"])
        if a[0] == "api" and "/jobs" in a[1]:
            return json.dumps(d["jobs"])
        raise fail
    return gh


def ssh_with(text: str, code: int = 0, tail: str | None = None) -> Run:
    """A fake `subprocess.run` for ssh that prints `text` and then `RC=code` (or `tail`); it records the command it was given."""
    def run(cmd: Sequence[str], **kw: object) -> subprocess.CompletedProcess:
        """The finished fake ssh."""
        run.cmd = list(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout=text + (tail if tail is not None else f"RC={code}\n"), stderr="")
    return run


def pair(root: Path, d: dict, gh: Gh, *, devbox: str | None = None, history: list[dict] | None = None, run_id: str | None = None) -> Report:
    """The report of the fixture `d`: its own devbox line (or `devbox` text) over a fake ssh and `gh`, with `history` as the gate-history lines."""
    text = devbox if devbox is not None else "".join(json.dumps(x) + "\n" for x in d["devbox"])
    work = root / f"work{len(list(root.glob('work*')))}"
    work.mkdir()
    return gather(d["verdict"]["sha"] if d["verdict"] else SHA, work, run_id=run_id, record=None, host="devbox", devbox_log=None, gh=gh, run=ssh_with(text),
                  history=lambda repo: (history, None if history is not None else "no history"), now=NOW)


def untagged(text: str) -> list[str]:
    """The lines of a report that are neither a heading, a table header or separator, nor name the sha or ref they came from."""
    tagged = re.compile(r"@[0-9a-f]{8}|\b[0-9a-f]{40}\b")
    return [x for x in text.splitlines() if x.strip() and not x.startswith("#") and not x.startswith("| source |") and not x.startswith("| ---") and not tagged.search(x)]


def classes(code: int | None, text: str, lost: bool = False, today: dt.date = TODAY) -> tuple[str, list[dict]]:
    """`grade_log` of a log against a baseline holding one in-date postgres class and one expired mysql class."""
    base = {"nightly[m|postgres] WRONG_VALUE": known_red.KnownRed("x", "2026-11-12: wrong value, see #913", "2026-11-22"),
            "nightly[gate|postgres/s3] FAIL": known_red.KnownRed("y", "2026-11-01: old", "2026-11-10")}
    return grade_log(code, text, lost, base, today)


def check_classes() -> None:
    """Every class of a red cell is told apart: GREEN, KNOWN with expiry and issue, NEW, LOST and INFRA, an expired entry is NEW, and a lost slice never reads green."""
    assert classes(0, CLEAN_LOG) == (ndelta.GREEN, [])
    cls, items = classes(1, "▸ matrices\nm postgres/x RED WRONG_VALUE over 2 pairs\n")
    assert cls == ndelta.KNOWN and (items[0]["expires"], items[0]["issue"], items[0]["stage"]) == ("2026-11-22", "#913", "matrices"), (cls, items)
    cls, items = classes(1, "▸ matrices\nm postgres/x RED WRONG_VALUE over 2 pairs\n", today=dt.date(2026, 11, 23))
    assert cls == ndelta.NEW and "expired" not in items[0]["key"] and items[0]["expires"] == "-", "an entry past its expiry acknowledges nothing"
    assert classes(1, "▸ s\n  mysql 8 live s3 FAIL boom\n")[0] == ndelta.NEW
    assert classes(1, "▸ s\n  postgres 16 live s3 FAIL boom\n", today=TODAY)[0] == ndelta.NEW, "an expired ledger entry is not KNOWN"
    cls, items = classes(0, "▸ s\n  ⊘ KNOWN RED (until 2026-11-22: 2026-11-12: a reason, see #982) — no_silent_null[mysql] smoke: SILENT_NULL over 3 pairs #abc\n")
    assert (cls, items[0]["cls"], items[0]["expires"], items[0]["issue"], items[0]["stage"]) == (ndelta.KNOWN, ndelta.KNOWN, "2026-11-22", "#982", "matrix no_silent_null"), (cls, items)
    assert classes(137, "▸ s\n")[0] == ndelta.LOST and classes(143, "x")[0] == ndelta.LOST and classes(None, "x")[0] == ndelta.LOST
    assert classes(None, "", lost=True)[0] == INFRA and classes(None, "", lost=True)[1][0]["key"].startswith("runner lost")
    assert classes(1, "  postgres 16 live s3 INFRA no host\n")[0] == INFRA
    assert classes(1, "The runner has received a shutdown signal\n")[0] == INFRA
    cls, items = classes(1, "▸ s\n✗ it broke\n")
    assert cls == ndelta.NEW and items[0]["line"] == "✗ it broke", (cls, items)
    assert classes(0, "▸ s\n  mysql 8 live s3 FAIL boom\n")[0] == ndelta.NEW, "a red cell under exit 0 is not green"
    s = Slice(1)
    assert slice_grade(s, {}, TODAY)[0] == INFRA and slice_grade(Slice(1, exit_code=0), {}, TODAY)[0] == ndelta.GREEN, "no record, no log and no exit code is INFRA, never GREEN"


def check_ssh() -> None:
    """The devbox record is read by `cat` and a trailing RC=0; no RC, a nonzero RC and a failing ssh are an error, never an empty record."""
    run = ssh_with('{"a": 1}\n')
    assert ssh_record("box", run) == ('{"a": 1}\n', None)
    assert run.cmd == ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", "box", f"cat {DEVBOX_RECORD}; echo RC=$?"], run.cmd
    assert ssh_record("box", ssh_with("x\n", tail=""))[1] and "no RC=" in ssh_record("box", ssh_with("x\n", tail=""))[1]
    assert "exited 1" in (ssh_record("box", ssh_with("x\n", code=1))[1] or "")
    assert ssh_record("box", ssh_with("x", tail="RC=0"))[1] is None, "a record whose last line has no newline is still read"

    def boom(cmd: Sequence[str], **kw: object) -> subprocess.CompletedProcess:
        """A ssh that times out."""
        raise subprocess.TimeoutExpired(cmd, 1)
    assert "TimeoutExpired" in (ssh_record("box", boom)[1] or "")


def check_overlap() -> None:
    """A stage run by both halves, by neither, or by the half it is not placed on is flagged; with a half missing, only the present half's stages can be `neither`."""
    h = Hosted(SHA, slices=[Slice(0, record={"stages": ["release build path", "cdc standby"]})])
    d = Devbox(SHA, "x", rec={"stages": ["cdc standby"]})
    rows = {x["stage"]: x for x in overlap(h, d, ["release build path", "cdc standby", "replica read"])}
    assert rows["cdc standby"]["ran"] == "both" and rows["cdc standby"]["flag"], rows
    assert rows["replica read"]["ran"] == "neither" and rows["replica read"]["flag"], rows
    assert rows["release build path"]["ran"] == "hosted" and not rows["release build path"]["flag"], rows
    wrong = {x["stage"]: x for x in overlap(Hosted(SHA, slices=[Slice(0, record={"stages": ["replica read"]})]), d, ["replica read"])}
    assert wrong["replica read"]["flag"], "a devbox stage run by the hosted half is misplaced"
    gone = {x["stage"]: x for x in overlap(h, Devbox(SHA, "x"), ["release build path", "replica read"])}
    assert not gone["replica read"]["flag"] and gone["release build path"]["ran"] == "hosted", "a missing devbox half does not make its stages `neither`"


def check_pair(root: Path) -> None:
    """A green pair is GREEN in the contract and in the report; a missing or unreadable half is named, never crashes and never reads green; every line names its ref."""
    d, gh = fixture(root)
    r = pair(root, d, gh)
    v = verdicts(r)
    assert v == {"hosted": None, "devbox": None, "pair": None}, v
    text = render(r, TODAY)
    assert not untagged(text), untagged(text)
    for needle in (f"[H main@{SHA[:8]}]", f"[D devbox@{SHA[:8]}]", "slice 0", "no red cell in the logs read", "peak used MB", "12000", "2000", f"https://example.invalid/runs/{RUN}"):
        assert needle in text, needle
    assert "no earlier" in text or "holds no earlier" in text, "a first record has no delta"
    gone = pair(root, d, gh, devbox="")
    t = render(gone, TODAY)
    assert verdicts(gone)["pair"] and "no record for this sha" in t and "HALVES_SHA" in t and not untagged(t), t
    nothing = rh.green(3, "b" * 40)
    nothing["verdict"] = None
    r2 = pair(root, nothing, fake_gh(nothing, root / "none", "b" * 40), devbox="")
    t2 = render(r2, TODAY)
    assert verdicts(r2)["pair"] and "no record for this sha" in t2 and "no completed run" in t2 and not untagged(t2), t2
    bad = gather(SHA, root / "w9" if (root / "w9").mkdir() is None else root, run_id=None, record=None, host="devbox", devbox_log=None, gh=gh, run=ssh_with("", code=1), history=lambda x: (None, "n/a"), now=NOW)
    tb = render(bad, TODAY)
    assert "record unreadable" in tb and "no record for this sha" not in tb.split("## 2.")[0], "an unreadable record is not a missing one"
    d2, gh2 = fixture(root / "both")
    d2["devbox"][0]["stages"] = [*d2["devbox"][0]["stages"], "release build path"]
    both = pair(root, d2, gh2)
    assert verdicts(both)["hosted"] is None and verdicts(both)["devbox"] is None and "STAGE_IN_BOTH" in (verdicts(both)["pair"] or ""), "two green halves that both ran a stage are not a green pair"
    pinned = pair(root, d, gh, run_id="999")
    assert verdicts(pinned)["hosted"] and "999" in (pinned.hosted.fetch_error or ""), "a pinned run that does not exist is refused, not replaced by another"


def check_report(root: Path) -> None:
    """A red slice is classified in its stage and the pair is not green; a delta against the previous record is shown for both halves."""
    d, gh = fixture(root, logs={1: "▸ matrices\n  mysql 8 live s3 FAIL boom\n", 2: ""}, codes={1: 1, 2: 143})
    prev = gate_history.history_line(root / "art", SHA, 3, {"ref": "refs/heads/main", "ref_name": "main", "ref_type": "branch", "event": "schedule", "run_id": "1", "run_attempt": "1", "run_url": "u", "workflow_sha": SHA}, None)
    older = {**d["devbox"][0], "sha": "c" * 40, "at": "2026-11-13T10:00:00Z", "total_min": 50.0, "phases_min": {"cdc standby": 5.0}}
    text = "".join(json.dumps(x) + "\n" for x in [older, d["devbox"][0]])
    r = pair(root, d, gh, devbox=text, history=[prev])
    out = render(r, TODAY)
    assert "### stage: matrices" in out and "| NEW |" in out and "| LOST |" in out, out
    assert verdicts(r)["pair"] and not untagged(out), untagged(out)
    assert f"against devbox@{'c' * 8}" in out and "total 50.0 -> 55.5" in out and "phase cdc standby: +2.0 min" in out, out
    assert "against main@" in out and "(run 1)" in out, out


def self_test() -> int:
    """Both halves read through one set of readers: green pair, missing or unreadable half, every red class, overlap, ssh and ref tagging."""
    check_classes()
    check_ssh()
    check_overlap()
    assert parse_sampler(SAMPLER) == {"samples": 2, "peak_used_mb": 12000, "min_avail_mb": 2000, "peak_swap_mb": 512, "oom_kills": 0, "min_disk_gb": 55, "elapsed_s": 60}
    assert parse_sampler("no rows") is None
    assert phases({"phases_min": {"A": 2.0, "postgres: round 1/2, 5 cells": 1.0, "B": 4.0}}) == {"A": 3.0, "B": 4.0}, "a per-engine progress banner is time of the stage before it"
    assert stage_of("live modules · every other (3)", ["live modules", "live modules devbox"]) == "live modules" and stage_of("clean tree", ["live modules"]) == NO_STAGE
    with tempfile.TemporaryDirectory() as tmp:
        check_pair(Path(tmp))
    with tempfile.TemporaryDirectory() as tmp:
        check_report(Path(tmp))
    print("paired_report self-test ok: green pair, missing and unreadable halves, 5 red classes, overlap, ssh RC and ref tagging")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
