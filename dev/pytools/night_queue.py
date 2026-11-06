"""Night queue: run each queued task in a fresh headless Claude session (or a plain command) within a budget, and post a summary.

    python3 -I dev/pytools/night_queue.py --until 07:30 --post        run the queue
    python3 -I dev/pytools/night_queue.py --tag weekly --until 06:00  run the tasks tagged weekly (own lock)
    python3 -I dev/pytools/night_queue.py --dry-run | --self-test

Queue ~/night/queue.json, a list of tasks: {"id", "brief" | "brief_file" | "cmd": [argv], "agent", "after": [ids],
"budget_m", "timeout_min", "cwd", "tag": "night"}. A `cmd` task runs the command and must print `VERDICT: done` or
`VERDICT: blocked`. Weekly: "every": "week", "weekday" (Mon=0), optional "rotate" for {lens} in the brief; it runs once
per ISO week as <id>-<year>w<week>. A done task is never run twice. Before each new task the queue holds while load1
is above --max-load (resuming at three quarters of it), a release gate runs, the disk is under --min-free-gib or
~/night/PAUSE exists (dev/pytools/devbox_load.py); a hold past the cutoff skips the task. Running tasks are never touched.
"""

from __future__ import annotations

import argparse
import collections
import contextlib
import datetime as dt
import fcntl
import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import types
import urllib.request
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dev.pytools import devbox_load  # noqa: E402

HOME = Path.home()
NIGHT = devbox_load.NIGHT
QUEUE = NIGHT / "queue.json"
NIGHT_BUDGET_M = float(os.environ.get("NIGHT_BUDGET_M", "1500"))
TOKEN_KEYS = ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens", "output_tokens")
UNATTENDED = (
    "You run unattended at night from a task queue: nobody will answer a question, so never ask one. "
    "Do only what the brief asks; never merge a PR, never force-push, never kill a process group (kill by explicit PID). "
    "Write long output to files and read them with grep/tail. "
    "End with a final message that starts with `VERDICT: done` or `VERDICT: blocked`, then the evidence: "
    "what was measured or changed, file paths, PR links, and what the next step should be."
)


def claude_bin() -> str:
    for c in (HOME / ".local/bin/claude", Path("/opt/homebrew/bin/claude")):
        if c.exists():
            return str(c)
    return "claude"


def load_status(run_dir: Path, tid: str) -> dict:
    try:
        return json.loads((run_dir / f"{tid}.json").read_text())
    except (OSError, ValueError):
        return {}


def find_status(tid: str) -> dict:
    """The newest status of a task across nights: a done task is never run twice."""
    for d in sorted((NIGHT / "runs").glob("*"), reverse=True):
        s = load_status(d, tid)
        if s:
            return s
    return {}


def ensure_cwd(cwd: str) -> None:
    """Recreate a missing task worktree as a detached checkout of origin/main."""
    if os.path.isdir(cwd):
        return
    repo = str(HOME / "rivet")
    subprocess.run(["git", "-C", repo, "fetch", "-q", "origin"], check=False)
    subprocess.run(["git", "-C", repo, "worktree", "prune"], check=False)
    subprocess.run(["git", "-C", repo, "worktree", "add", "--detach", cwd, "origin/main"], check=True)


def task_cmd(task: dict) -> list[str]:
    """The command a task runs: its own `cmd` argv, or a headless Claude session on its brief."""
    if task.get("cmd"):
        return [os.path.expanduser(a) for a in task["cmd"]]
    brief = task["brief"] if "brief" in task else Path(os.path.expanduser(task["brief_file"])).read_text()
    cmd = [claude_bin(), "-p", brief + "\n\n" + UNATTENDED, "--permission-mode", "auto", "--output-format", "stream-json", "--verbose",
           "--append-system-prompt", UNATTENDED, "--add-dir", str(HOME)]
    if task.get("agent"):
        cmd += ["--agent", task["agent"]]
    if task.get("model"):
        cmd += ["--model", task["model"]]
    return cmd


def run_task(task: dict, run_dir: Path) -> dict:
    """One fresh session; stopped when it passes its token budget or its timeout."""
    tid = task["id"]
    budget = float(task.get("budget_m", 150)) * 1e6
    timeout = float(task.get("timeout_min", 120)) * 60
    cwd = os.path.expanduser(task.get("cwd", "~/rivet"))
    ensure_cwd(cwd)
    cmd = task_cmd(task)
    status = {"id": tid, "agent": task.get("agent"), "started": dt.datetime.now().isoformat(timespec="seconds"),
              "tokens": 0, "result": "", "state": "running"}
    (run_dir / f"{tid}.json").write_text(json.dumps(status, indent=2))
    seen: set = set()
    started = time.monotonic()
    log = open(run_dir / f"{tid}.jsonl", "w")
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            stdin=subprocess.DEVNULL, start_new_session=True)
    stop_reason: list[str] = []

    def watchdog() -> None:
        while proc.poll() is None:
            if time.monotonic() - started > timeout:
                stop_reason.append(f"timeout {timeout / 60:.0f} min")
                proc.send_signal(signal.SIGTERM)
                return
            time.sleep(5)

    threading.Thread(target=watchdog, daemon=True).start()
    plain, tail = bool(task.get("cmd")), collections.deque(maxlen=60)
    for line in proc.stdout:
        log.write(line)
        if plain:
            tail.append(line)
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        if ev.get("type") == "assistant":
            msg = ev.get("message") or {}
            key = msg.get("id")
            if key and key not in seen:
                seen.add(key)
                status["tokens"] += sum((msg.get("usage") or {}).get(k) or 0 for k in TOKEN_KEYS)
                if status["tokens"] > budget and not stop_reason:
                    stop_reason.append(f"budget {budget / 1e6:.1f}M passed")
                    proc.send_signal(signal.SIGTERM)
        elif ev.get("type") == "result":
            status["result"] = ev.get("result") or ""
            status["session_id"] = ev.get("session_id")
    proc.wait()
    log.close()
    if plain:
        status["result"] = "".join(tail)
    verdict = "done" if "VERDICT: done" in status["result"] else "blocked" if "VERDICT: blocked" in status["result"] else None
    if stop_reason:
        status["state"] = "stopped"
    elif proc.returncode != 0 or verdict == "blocked":
        status["state"] = "failed"
    else:
        status["state"] = "done" if verdict == "done" else "unverified"
    status["stop_reason"] = stop_reason[0] if stop_reason else None
    status["exit"] = proc.returncode
    status["minutes"] = round((time.monotonic() - started) / 60, 1)
    (run_dir / f"{tid}.md").write_text(status["result"] or f"(no final message; {status['stop_reason']})\n")
    (run_dir / f"{tid}.json").write_text(json.dumps(status, indent=2))
    return status


def webhook() -> str | None:
    if os.environ.get("DISCORD_WEBHOOK_URL"):
        return os.environ["DISCORD_WEBHOOK_URL"]
    try:
        return json.load(open(HOME / ".claude/settings.json")).get("env", {}).get("DISCORD_WEBHOOK_URL")
    except (OSError, ValueError):
        return None


def post(text: str) -> None:
    url = webhook()
    if not url:
        print("no DISCORD_WEBHOOK_URL; summary not posted", file=sys.stderr)
        return
    req = urllib.request.Request(url, data=json.dumps({"content": text[:1990]}).encode(),
                                 headers={"Content-Type": "application/json", "User-Agent": "night-queue"})
    with urllib.request.urlopen(req, timeout=15) as r:
        r.read()


def summary(results: list[dict], skipped: list[str], spent: float, run_dir: Path) -> str:
    icon = {"done": "✅", "failed": "❌", "stopped": "⛔", "unverified": "❔"}
    lines = [f"**Night queue — {run_dir.name}** ({spent / 1e6:.0f}M tokens of {NIGHT_BUDGET_M:.0f}M)"]
    for s in results:
        first = next((l for l in s["result"].splitlines() if l.strip()), s.get("stop_reason") or "")
        lines.append(f"{icon.get(s['state'], '?')} `{s['id']}` {s['minutes']} min, {s['tokens'] / 1e6:.1f}M — {first[:160]}")
    for tid in skipped:
        lines.append(f"⏭️ `{tid}` not run")
    lines.append(f"reports: `{run_dir}`")
    return "\n".join(lines)


def instance(task: dict, today: dt.date) -> dict | None:
    """The task as it runs today: a weekly task gets its week id and lens, or None off its weekday."""
    if task.get("every") != "week":
        return task
    if today.weekday() != task.get("weekday", 5):
        return None
    year, week, _ = today.isocalendar()
    rotate = task.get("rotate") or [""]
    lens = rotate[week % len(rotate)]
    brief = {"brief": task["brief"].replace("{lens}", lens)} if "brief" in task else {}
    return {**task, "id": f"{task['id']}-{year}w{week:02d}", **brief}


def hold_deadline(cutoff: dt.datetime | None, hold_max_min: float) -> float:
    """The monotonic time past which a held task is skipped: the queue's cutoff, or `hold_max_min` from now."""
    wait = (cutoff - dt.datetime.now()).total_seconds() if cutoff else hold_max_min * 60
    return time.monotonic() + max(wait, 0.0)


def main(argv: list[str] | None = None) -> int:
    """Run the queue's tasks for --tag in order; hold each new one while the machine is busy."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--until", help="start no task after this local time, HH:MM")
    ap.add_argument("--post", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--queue", default=str(QUEUE))
    ap.add_argument("--tag", default="night", help="run only tasks with this tag (default night)")
    ap.add_argument("--no-hold", action="store_true", help="start tasks without the load, gate and disk hold")
    ap.add_argument("--max-load", type=float, help="hold above this load1 (default: one per core)")
    ap.add_argument("--min-free-gib", type=float, default=devbox_load.WATCH_FLOOR_GIB)
    ap.add_argument("--hold-poll", type=float, default=60.0, help="seconds between hold checks")
    ap.add_argument("--hold-max-min", type=float, default=600.0, help="longest wait for a quiet machine when there is no --until")
    args = ap.parse_args(argv)
    if args.self_test:
        return self_test()
    if not Path(args.queue).exists():
        print(f"no queue at {args.queue}")
        return 0
    tasks = json.loads(Path(args.queue).read_text())
    today = dt.date.today()
    run_dir = NIGHT / "runs" / today.isoformat()
    run_dir.mkdir(parents=True, exist_ok=True)
    lock = open(NIGHT / (".lock" if args.tag == "night" else f".lock-{args.tag}"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(f"another night queue ({args.tag}) is running", file=sys.stderr)
        return 1
    cutoff = None
    if args.until:
        h, m = map(int, args.until.split(":"))
        cutoff = dt.datetime.now().replace(hour=h, minute=m, second=0)
        if cutoff < dt.datetime.now():
            cutoff += dt.timedelta(days=1)
    hold = devbox_load.default_hold(args.max_load, args.min_free_gib)
    deadline = hold_deadline(cutoff, args.hold_max_min)
    results, skipped, spent = [], [], 0.0
    try:
        for raw in tasks:
            if raw.get("tag", "night") != args.tag:
                continue
            task = instance(raw, today)
            if task is None:
                continue
            tid = task["id"]
            if find_status(tid).get("state") == "done":
                continue
            blocked = [a for a in task.get("after", []) if find_status(a).get("state") != "done"]
            if blocked:
                skipped.append(f"{tid} (waits for {', '.join(blocked)})")
                continue
            if spent / 1e6 >= NIGHT_BUDGET_M:
                skipped.append(f"{tid} (night budget spent)")
                continue
            if args.dry_run:
                why = "" if args.no_hold else hold.reason(devbox_load.snapshot())
                print(f"would {'hold (' + why + ') then ' if why else ''}run {tid} as {task.get('agent') or 'default'} in {task.get('cwd', '~/rivet')}")
                continue
            if not args.no_hold:
                why = devbox_load.wait_quiet(hold, deadline, args.hold_poll)
                if why:
                    skipped.append(f"{tid} (held: {why})")
                    continue
            if cutoff and dt.datetime.now() > cutoff:
                skipped.append(f"{tid} (past {args.until})")
                continue
            print(f"{dt.datetime.now():%H:%M} start {tid}", flush=True)
            s = run_task(task, run_dir)
            spent += s["tokens"]
            results.append(s)
            print(f"{dt.datetime.now():%H:%M} {s['state']} {tid} {s['tokens'] / 1e6:.1f}M {s['minutes']} min", flush=True)
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()
    text = summary(results, skipped, spent, run_dir)
    print(text)
    if args.post and (results or skipped) and not args.dry_run:
        post(text)
    return 0


def self_test() -> int:
    """The hold, the resume, the PAUSE file, the tag, a task past midnight and a plain command task, on a temp ~/night."""
    me = sys.modules[__name__]
    quiet = {"load1": 0.5, "free_gib": 900.0, "gates": [], "pause": False}
    sat = dt.date(2026, 11, 7)
    assert sat.weekday() == 5

    def run(tasks: list[dict], argv: list[str], snaps, today=sat) -> tuple[int, str, dict]:
        """main() over a temp night dir; `snaps` is a dict or a list consumed one per hold check."""
        with tempfile.TemporaryDirectory() as t:
            night = Path(t)
            (night / "q.json").write_text(json.dumps(tasks))
            seq = iter(snaps) if isinstance(snaps, list) else None
            clock = types.SimpleNamespace(**{**vars(dt), "date": type("D", (dt.date,), {"today": classmethod(lambda c: today())})}) if callable(today) else None
            out = io.StringIO()
            with mock.patch.object(me, "NIGHT", night), mock.patch.object(devbox_load, "NIGHT", night), \
                    mock.patch.object(devbox_load, "snapshot", lambda path=None: next(seq) if seq else snaps), \
                    mock.patch.object(me, "dt", clock or dt), contextlib.redirect_stdout(out):
                code = main(["--queue", str(night / "q.json"), "--hold-poll", "0", *argv])
            return code, out.getvalue(), {p.stem: json.loads(p.read_text()) for p in night.glob("runs/*/*.json")}

    def cmd_task(tid: str, marker: Path | None = None, **extra) -> dict:
        """A plain task that drops `marker` and says it is done."""
        body = (f"open({str(marker)!r}, 'w').write('x');" if marker else "") + "print('VERDICT: done')"
        return {"id": tid, "cmd": [sys.executable, "-c", body], "cwd": str(Path.cwd()), **extra}

    with tempfile.TemporaryDirectory() as m:
        mk = Path(m) / "ran"
        code, out, runs = run([cmd_task("a", mk)], [], quiet)
        assert code == 0 and mk.exists() and runs["a"]["state"] == "done" and runs["a"]["tokens"] == 0, (out, runs)
        mk.unlink()

        busy = {**quiet, "gates": [4242]}
        code, out, _ = run([cmd_task("a", mk)], ["--hold-max-min", "0"], busy)
        assert not mk.exists() and "held: release gate running (pid 4242)" in out, f"a running gate must hold a new job: {out}"

        code, out, _ = run([cmd_task("a", mk)], ["--hold-max-min", "0"], {**quiet, "pause": True})
        assert not mk.exists() and "PAUSE" in out, out

        code, out, _ = run([cmd_task("a", mk)], ["--hold-max-min", "0"], {**quiet, "load1": 99.0})
        assert not mk.exists() and "load1 99.0" in out, out

        code, out, _ = run([cmd_task("a", mk)], ["--hold-max-min", "1", "--max-load", "10"], [busy, {**quiet, "load1": 20.0}, {**quiet, "load1": 8.0}, quiet])
        assert "start a" in out and mk.exists(), f"the queue resumes once the machine is quiet: {out}"
        mk.unlink()

        code, out, _ = run([cmd_task("a", mk)], ["--dry-run", "--hold-max-min", "0"], busy)
        assert "would hold (release gate running" in out and not mk.exists(), out

        code, out, _ = run([cmd_task("a", mk)], ["--no-hold"], busy)
        assert mk.exists(), "--no-hold starts regardless"
        mk.unlink()

        code, out, _ = run([cmd_task("a", mk, tag="weekly")], [], quiet)
        assert not mk.exists(), "an untagged run leaves weekly-tagged tasks alone"
        code, out, _ = run([cmd_task("a", mk, tag="weekly"), cmd_task("b", tag="night")], ["--tag", "weekly"], quiet)
        assert mk.exists() and "start b" not in out, out
        mk.unlink()

        days = iter([sat, dt.date(2026, 11, 8), dt.date(2026, 11, 8), dt.date(2026, 11, 8)])
        weekly = lambda tid: cmd_task(tid, every="week", weekday=5)
        code, out, _ = run([weekly("a"), weekly("b")], [], quiet, today=lambda: next(days))
        assert "start a-2026w45" in out and "start b-2026w45" in out, f"a weekly task that follows one running past midnight still runs: {out}"
    print("night_queue self-test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
