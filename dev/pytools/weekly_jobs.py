"""Weekly devbox jobs, run by the night queue (`night_queue.py --tag weekly`, queue example in dev/launchd/).

    python3 -m dev.pytools.weekly_jobs gate [--dry-run] [--switch]    `make release-oracle-full` in the gate worktree, on a quiet machine
    python3 -m dev.pytools.weekly_jobs mutants [--plan] [--dry-run-issues] [--packages src/a,src/b.rs]
    python3 -m dev.pytools.weekly_jobs --self-test

Both end with `VERDICT: done|blocked`; `gate` logs to ~/wt/gate-logs/<sha>/weekly-full.log ending in `exit <rc> <utc>`.
"""

from __future__ import annotations

import argparse
import collections
import contextlib
import datetime as dt
import io
import json
import os
import plistlib
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dev.pytools import devbox_load  # noqa: E402

GATE_CMD = ["make", "release-oracle-full"]
MUTANTS_WORKTREE = Path.home() / "wt" / "weekly-mutants"
REPORT_ROOT = devbox_load.NIGHT / "reports" / "hot-mutants"
HOT_COUNT, HOT_DAYS = 5, 30
GATE_WAIT_MIN, BUDGET_MIN = 180.0, 600.0
GATE_TOOLS, MUTANTS_TOOLS = ("git", "make", "uv", "gh"), ("git", "cargo", "gh")
SUBCOMMANDS = ("gate", "mutants")
CLOSING = re.compile(r"(fix|fixe[sd]|close[sd]?|resolve[sd]?):? +#[0-9]+", re.I)
EXAMPLES = ROOT / "dev" / "launchd"
SENTINEL = re.compile(r"^exit (-?\d+) \S+$")


def git(wt: Path, *args: str, check: bool = True) -> str:
    """Stdout of `git -C wt args`; a failure raises with git's own message unless `check` is off."""
    p = subprocess.run(["git", "-C", str(wt), *args], capture_output=True, text=True)
    if check and p.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} in {wt}: {p.stderr.strip()}")
    return p.stdout.strip()


def missing_tools(names: tuple[str, ...]) -> str:
    """A refusal naming the tools not on PATH ("" = all found), so a bare launchd PATH is said out loud."""
    gone = [n for n in names if shutil.which(n) is None]
    return f"not on PATH: {', '.join(gone)} (PATH={os.environ.get('PATH', '')})" if gone else ""


def verdict(done: bool, why: str) -> int:
    """Print the VERDICT line the night queue reads and return the matching exit code."""
    print(f"{why}\nVERDICT: {'done' if done else 'blocked'}")
    return 0 if done else devbox_load.BUSY_EXIT


def sentinel_rc(log: Path) -> int | None:
    """The exit code a finished weekly log ends with, or None when the log is absent or never finished."""
    try:
        last = log.read_text(errors="replace").rstrip().splitlines()[-1]
    except (OSError, IndexError):
        return None
    m = SENTINEL.match(last)
    return int(m.group(1)) if m else None


def oracle_lines(log: Path, keep: int = 6) -> list[str]:
    """The last `keep` RIVET-ORACLE verdict lines of a gate log."""
    tail: collections.deque[str] = collections.deque(maxlen=keep)
    with log.open(errors="replace") as f:
        tail.extend(line.rstrip() for line in f if "RIVET-ORACLE-" in line)
    return list(tail)


def switch_refusal(wt: Path, head: str, sha: str, switch: bool) -> str:
    """Why a tree standing at `head` may not be moved to origin/main ("" = it may): no --switch, or a head that is not an ancestor of main."""
    if not switch:
        return f"{wt} stands at {head}, origin/main is {sha}; pass --switch to move it, or check origin/main out in it by hand"
    if subprocess.run(["git", "-C", str(wt), "merge-base", "--is-ancestor", "HEAD", "origin/main"], capture_output=True).returncode != 0:
        return f"{wt} stands at {head}, which is not an ancestor of origin/main {sha}; it is never moved, park the candidate in another worktree or check origin/main out by hand"
    return ""


def busy(why: str) -> int:
    """A refusal for a machine that is not quiet or runs another gate, with the way out."""
    return verdict(False, f"refused: {why}; wait for it to clear (raise --wait-min) or run the gate again by hand")


def run_gate(wt: Path, logs: Path, cmd: list[str], dry_run: bool, snap: Callable[[], dict], quiet_load: float,
             wait_min: float = 0.0, poll_s: float = 60.0, switch: bool = False) -> int:
    """Wait up to `wait_min` for a quiet machine, then run `cmd` into <logs>/<sha>/ at origin/main; --switch moves a clean tree that is an ancestor of it."""
    if wt.resolve() != devbox_load.GATE_WORKTREE.resolve() and cmd == GATE_CMD:
        return verdict(False, f"refused: {wt} is not the gate worktree {devbox_load.GATE_WORKTREE}; run it with --worktree {devbox_load.GATE_WORKTREE}")
    why = devbox_load.wait_gate_quiet(snap, quiet_load, time.monotonic() + (0 if dry_run else wait_min * 60), poll_s)
    if why:
        return busy(why)
    if git(wt, "status", "--porcelain"):
        return verdict(False, f"refused: {wt} has uncommitted changes; commit or discard them, the weekly gate never touches a dirty tree")
    if not dry_run:
        git(wt, "fetch", "-q", "origin", "main")
    sha = git(wt, "rev-parse", "--short=8", "origin/main")
    head = git(wt, "rev-parse", "--short=8", "HEAD")
    why = switch_refusal(wt, head, sha, switch) if head != sha else ""
    if why:
        return verdict(False, f"refused: {why}")
    logdir = logs / sha
    log = logdir / "weekly-full.log"
    if dry_run:
        return verdict(True, f"dry-run: would run `{' '.join(cmd)}` in {wt} at {sha} into {log}")
    if sentinel_rc(log) == 0:
        return verdict(True, f"already graded green at {sha}: {log}")
    why = devbox_load.wait_gate_quiet(snap, quiet_load, 0.0, poll_s)
    if why:
        return busy(why)
    git(wt, "checkout", "-q", "--detach", "origin/main")
    moved = f"moved {wt} from {head} to {sha}; recreate the rivet-duckdb container (uv.lock pins may have changed), as after a manual checkout\n" if head != sha else ""
    logdir.mkdir(parents=True, exist_ok=True)
    with log.open("w") as f:
        rc = subprocess.run(cmd, cwd=wt, env={**os.environ, "COMPOSE_PROJECT_NAME": "rivet"}, stdout=f, stderr=subprocess.STDOUT).returncode
        f.write(f"\nexit {rc} {dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ}\n")
    lines = "\n".join(oracle_lines(log)) or "no RIVET-ORACLE verdict line in the log"
    return verdict(rc == 0, f"{moved}{sha}: `{' '.join(cmd)}` exit {rc}, log {log}\n{lines}")


def gate_job(a: argparse.Namespace, snap: Callable[[], dict] = devbox_load.snapshot) -> int:
    """The `gate` subcommand: refuse on a bare PATH, else run_gate."""
    gone = missing_tools(GATE_TOOLS)
    if gone:
        return verdict(False, f"refused: {gone}")
    return run_gate(a.worktree, a.logs, GATE_CMD, a.dry_run, snap, a.quiet_load, a.wait_min, a.poll, a.switch)


def package_of(path: str) -> str | None:
    """`src/<dir>` for a file inside a module directory, the file itself for `src/x.rs`, None outside src/."""
    parts = path.split("/")
    if parts[0] != "src" or len(parts) < 2:
        return None
    return f"src/{parts[1]}" if len(parts) > 2 else path


def churn(numstat: str) -> collections.Counter:
    """Changed lines per package from `git log --numstat` text; binary rows (`-`) count nothing."""
    out: collections.Counter = collections.Counter()
    for row in numstat.splitlines():
        cols = row.split("\t")
        if len(cols) != 3 or not (cols[0].isdigit() and cols[1].isdigit()):
            continue
        pkg = package_of(cols[2])
        if pkg:
            out[pkg] += int(cols[0]) + int(cols[1])
    return out


def package_glob(pkg: str) -> str:
    """The cargo-mutants `--file` pattern covering the whole package."""
    return pkg if pkg.endswith(".rs") else f"{pkg}/**/*.rs"


def mutant_count(wt: Path, pkg: str) -> int:
    """Mutants cargo-mutants lists for the package; a failed listing raises, it is never read as zero."""
    p = subprocess.run(["cargo", "mutants", "--list", "--colors=never", "--file", package_glob(pkg)], cwd=wt, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"cargo mutants --list failed for {pkg} (exit {p.returncode}): {p.stderr.strip()[-300:]}")
    return len([ln for ln in p.stdout.splitlines() if ln.strip()])


def hot_packages(counts: collections.Counter, n: int, count_mutants: Callable[[str], int]) -> list[tuple[str, int, int]]:
    """The `n` most-changed packages that hold mutants, as (package, changed lines, mutants); excluded ones are skipped."""
    picked = []
    for pkg, lines in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        total = count_mutants(pkg)
        if total:
            picked.append((pkg, lines, total))
        if len(picked) == n:
            break
    return picked


def read_rows(path: Path) -> list[str]:
    """Sorted unique lines of a cargo-mutants outcome file, [] when absent."""
    return sorted(set(path.read_text().splitlines())) if path.is_file() else []


def run_package(wt: Path, pkg: str, out_root: Path, jobs: int, timeout_min: int) -> dict:
    """scripts/mutants.py over the whole package; COMPLETE only when its own summary line was printed."""
    out = out_root / pkg.replace("/", "_")
    p = subprocess.run([sys.executable, str(wt / "scripts" / "mutants.py"), package_glob(pkg), "-j", str(jobs), "--out", str(out),
                        "--timeout-min", str(timeout_min)], cwd=wt, capture_output=True, text=True)
    (out_root / f"{pkg.replace('/', '_')}.log").write_text(p.stdout + p.stderr)
    return {"package": pkg, "complete": p.returncode == 0, "missed": read_rows(out / "mutants.out" / "missed.txt"),
            "timeout": read_rows(out / "mutants.out" / "timeout.txt")}


def issue_title(pkg: str) -> str:
    """The one title a package's survivors are filed and found under."""
    return f"[hot-mutants] {pkg}: surviving mutants"


def issue_body(res: dict, sha: str, day: str) -> str:
    """The issue text: the survivors verbatim, labelled partial when the run did not finish."""
    state = "complete run" if res["complete"] else "PARTIAL run (cargo-mutants did not finish inside its timeout)"
    miss = "\n".join(res["missed"]) or "(none)"
    slow = "\n".join(res["timeout"]) or "(none)"
    return (f"Weekly hot-package mutants over `{res['package']}` at {sha[:8]}, {day}: {state}.\n\n"
            f"Missed ({len(res['missed'])}), read as \"no unit test executes this\" (`--lib`):\n\n```\n{miss}\n```\n\n"
            f"Timed out ({len(res['timeout'])}):\n\n```\n{slow}\n```\n")


def file_issue(res: dict, sha: str, day: str, dry_run: bool, wt: Path) -> str:
    """Create the package's issue, or comment on the open one; print it instead under `dry_run`."""
    title, body = issue_title(res["package"]), issue_body(res, sha, day)
    if dry_run:
        print(f"--- dry-run issue: {title}\n{body}")
        return "dry-run"
    try:
        found = subprocess.run(["gh", "issue", "list", "--state", "open", "--search", f'"{title}" in:title', "--json", "number,title"],
                               cwd=wt, capture_output=True, text=True)
    except OSError as e:
        return f"gh unavailable: {e}"
    if found.returncode != 0:
        return f"gh issue list failed: {found.stderr.strip()[-200:]}"
    open_nums = [i["number"] for i in json.loads(found.stdout or "[]") if i["title"] == title]
    cmd = ["gh", "issue", "comment", str(open_nums[0]), "--body-file", "-"] if open_nums else ["gh", "issue", "create", "--title", title, "--body-file", "-"]
    made = subprocess.run(cmd, cwd=wt, input=body, capture_output=True, text=True)
    if made.returncode != 0:
        return f"gh issue failed: {made.stderr.strip()[-200:]}"
    return made.stdout.strip().splitlines()[-1] if made.stdout.strip() else "filed"


def report_row(res: dict, churn_lines: int, total: int, issue: str) -> str:
    """One report.md table row; an unfinished run never reads as clean."""
    state = "COMPLETE" if res["complete"] else "INCOMPLETE"
    return f"| {res['package']} | {churn_lines} | {total} | {state} | {len(res['missed'])} | {len(res['timeout'])} | {issue} |"


def prepare_worktree(wt: Path, repo: Path) -> str:
    """The mutants worktree at origin/main (created when absent); never the gate worktree, never a dirty tree."""
    if wt.resolve() == devbox_load.GATE_WORKTREE.resolve():
        raise RuntimeError(f"{wt} is the gate worktree; the mutants job does not touch it")
    if not wt.is_dir():
        git(repo, "fetch", "-q", "origin", "main")
        git(repo, "worktree", "add", "-q", "--detach", str(wt), "origin/main")
    elif git(wt, "status", "--porcelain"):
        raise RuntimeError(f"{wt} has uncommitted changes")
    git(wt, "fetch", "-q", "origin", "main")
    git(wt, "checkout", "-q", "--detach", "origin/main")
    return git(wt, "rev-parse", "HEAD")


def package_timeout(budget_min: float, elapsed_min: float, per_package_min: int) -> int:
    """Minutes the next package may run: its own limit capped by what is left of the job budget (0 = none left)."""
    return max(0, min(per_package_min, int(budget_min - elapsed_min)))


def write_report(out_root: Path, day: str, sha: str, rows: list[str]) -> Path:
    """Rewrite report.md from the rows so far, so a job stopped midway still leaves its report."""
    head = f"# Hot-package mutants {day} at {sha[:8]}\n\n| package | changed lines | mutants | run | missed | timed out | issue |\n|---|---|---|---|---|---|---|\n"
    (out_root / "report.md").write_text(head + "\n".join(rows) + "\n")
    return out_root / "report.md"


def run_mutants(a: argparse.Namespace) -> int:
    """Pick the hot packages, mutate each whole within the budget, write the report as it grows and file the issues."""
    started = time.monotonic()
    gone = missing_tools(MUTANTS_TOOLS)
    if gone:
        return verdict(False, f"refused: {gone}")
    try:
        sha = prepare_worktree(a.worktree, a.repo)
        counts = churn(git(a.worktree, "log", f"--since={HOT_DAYS}.days", "--no-renames", "--numstat", "--pretty=format:", "--", "src"))
        if a.packages:
            picked = [(p, counts.get(p, 0), mutant_count(a.worktree, p)) for p in a.packages.split(",")]
        else:
            picked = hot_packages(counts, HOT_COUNT, lambda p: mutant_count(a.worktree, p))
    except (RuntimeError, OSError) as e:
        return verdict(False, f"refused: {e}")
    day = dt.date.today().isoformat()
    out_root = a.report_dir / day
    out_root.mkdir(parents=True, exist_ok=True)
    plan = "\n".join(f"{p}: {lines} changed lines, {total} mutants" for p, lines, total in picked)
    if a.plan:
        return verdict(True, f"plan at {sha[:8]}:\n{plan}")
    hold = devbox_load.default_hold(a.max_load)
    rows, survivors, unfinished = [], 0, []
    for pkg, lines, total in picked:
        left = package_timeout(a.budget_min, (time.monotonic() - started) / 60, a.timeout_min)
        why = "job budget spent" if not left else devbox_load.wait_quiet(hold, time.monotonic() + min(a.hold_max_min, left) * 60, a.hold_poll)
        if not why:
            left = package_timeout(a.budget_min, (time.monotonic() - started) / 60, a.timeout_min)
            why = "" if left else "job budget spent"
        if why:
            rows.append(f"| {pkg} | {lines} | {total} | NOT RUN | - | - | {why} |")
            unfinished.append(pkg)
            write_report(out_root, day, sha, rows)
            continue
        res = run_package(a.worktree, pkg, out_root, a.jobs, left)
        (out_root / f"survivors-{pkg.replace('/', '_')}.txt").write_text("\n".join(res["missed"]) + "\n")
        issue = "-"
        if res["missed"] or res["timeout"]:
            issue = file_issue(res, sha, day, a.dry_run_issues, a.worktree)
        survivors += len(res["missed"])
        if not res["complete"]:
            unfinished.append(pkg)
        rows.append(report_row(res, lines, total, issue))
        write_report(out_root, day, sha, rows)
    report = write_report(out_root, day, sha, rows)
    done = not unfinished
    return verdict(done, f"{len(picked)} packages, {survivors} survivors, report {report}"
                   + ("" if done else f"; NOT COMPLETE: {', '.join(unfinished)}"))


def self_test() -> int:
    """Package picking, the issue text and filing, a fake cargo-mutants through scripts/mutants.py, and the gate job on a temp repo."""
    assert package_of("src/source/pg/mod.rs") == "src/source" and package_of("src/redact.rs") == "src/redact.rs"
    assert package_of("tests/x.rs") is None and package_of("src") is None
    numstat = "10\t5\tsrc/source/a.rs\n3\t0\tsrc/source/b/c.rs\n-\t-\tsrc/state/blob.bin\n7\t1\tsrc/redact.rs\n99\t0\tdocs/x.md\nnot a row\n"
    counts = churn(numstat)
    assert counts == {"src/source": 18, "src/redact.rs": 8}, counts
    many = collections.Counter({"src/a": 50, "src/b": 40, "src/bin": 30, "src/c": 20, "src/d.rs": 20, "src/e": 10, "src/f": 5})
    got = hot_packages(many, 5, lambda p: 0 if p == "src/bin" else 7)
    assert [g[0] for g in got] == ["src/a", "src/b", "src/c", "src/d.rs", "src/e"], got
    assert hot_packages(many, 99, lambda p: 0) == []
    assert package_glob("src/source") == "src/source/**/*.rs" and package_glob("src/redact.rs") == "src/redact.rs"

    res = {"package": "src/source", "complete": False, "missed": ["src/source/a.rs:1:2: replace f with ()"], "timeout": []}
    body = issue_body(res, "abcdef0123", "2026-11-07")
    assert "PARTIAL" in body and "replace f with ()" in body and not CLOSING.search(body) and "src/source" in issue_title("src/source")
    assert "| INCOMPLETE |" in report_row(res, 18, 100, "-") and "| COMPLETE |" in report_row({**res, "complete": True}, 18, 100, "-")
    with contextlib.redirect_stdout(io.StringIO()) as buf:
        assert file_issue(res, "abcdef0123", "d", True, Path(".")) == "dry-run"
    assert "dry-run issue" in buf.getvalue()

    snap = {"load1": 0.5, "free_gib": 900.0, "gates": [], "pause": False}
    with tempfile.TemporaryDirectory() as t:
        t = Path(t)
        fake_cargo(t / "bin")
        env = {**os.environ, "PATH": f"{t / 'bin'}{os.pathsep}{os.environ['PATH']}"}
        foreign = Path(tempfile.gettempdir()) / f"cargo-mutants-foreign-{os.getpid()}.tmp"
        foreign.mkdir()
        try:
            (t / "scripts").mkdir()
            (t / "scripts" / "mutants.py").write_text((ROOT / "scripts" / "mutants.py").read_text())
            with mock_env(env):
                assert mutant_count(t, "src/source") == 3, "blank lines are not mutants"
                try:
                    mutant_count(t, "src/none")
                except RuntimeError as e:
                    assert "exit 1" in str(e)
                else:
                    raise AssertionError("a failed cargo mutants --list was read as zero mutants")
                issue_selftest(t, res)
                done = run_package(t, "src/source", t / "out", 1, 1)
                lonely = run_package(t, "src/none", t / "out", 1, 1)
            assert done["complete"] and done["missed"] == ["src/source/a.rs:3:4: replace g with 1"], done
            assert not lonely["complete"] and lonely["missed"] == [], "a run that printed no summary line is INCOMPLETE, never clean"
            assert foreign.is_dir(), "mutants.py removed a cargo-mutants temp tree it did not make"
            assert not (t / "out" / "src_source" / "tmp").exists(), "the run's own temp tree is removed"
        finally:
            foreign.rmdir()
        gate_selftest(t, snap)
        tools_selftest(t, snap)
    assert package_timeout(600, 590, 240) == 10 and package_timeout(600, 0, 240) == 240 and package_timeout(600, 700, 240) == 0
    check_examples(EXAMPLES)
    print("weekly_jobs self-test: ok")
    return 0


def check_examples(examples: Path) -> None:
    """The example queue names only real subcommands and known ids; the plist runs the same tag on the same weekday."""
    queue = json.loads((examples / "weekly-queue.json.example").read_text())
    plist = plistlib.loads((examples / "ai.rivet.weekly.plist.example").read_bytes())
    ids = [t["id"] for t in queue]
    assert len(set(ids)) == len(ids) and {"weekly-gate", "weekly-hot-mutants"} <= set(ids)
    for t in queue:
        assert t["tag"] == "weekly" and t["every"] == "week" and t["cmd"][:3] == ["python3", "-m", "dev.pytools.weekly_jobs"], t
        assert t["cmd"][3] in SUBCOMMANDS and "after" not in t, f"queue order carries the sequence; `after` waits on a renamed weekly id: {t}"
    by_id = {t["id"]: t for t in queue}
    assert "--switch" in by_id["weekly-gate"]["cmd"], "the weekly gate must move its tree to origin/main or it grades a stale sha"
    assert by_id["weekly-gate"]["timeout_min"] > GATE_WAIT_MIN and by_id["weekly-hot-mutants"]["timeout_min"] > BUDGET_MIN
    args = plist["ProgramArguments"]
    assert args[args.index("--tag") + 1] == "weekly" and "--queue" in args and "--until" in args, args
    path = plist["EnvironmentVariables"]["PATH"].split(":")
    assert any(p.endswith(".cargo/bin") for p in path) and any("/opt/homebrew/bin" == p for p in path), "launchd's default PATH has no cargo, gh or uv"
    days = {t["weekday"] for t in queue}
    assert len(days) == 1 and plist["StartCalendarInterval"]["Weekday"] == (days.pop() + 1) % 7, "launchd counts Sunday=0, the queue Monday=0"


@contextlib.contextmanager
def mock_env(env: dict):
    """Run the block with os.environ replaced by `env`."""
    saved = dict(os.environ)
    os.environ.clear()
    os.environ.update(env)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


def fake_cargo(bindir: Path) -> None:
    """A `cargo` that behaves like `cargo mutants` for the package named src/source and dies silently for any other."""
    bindir.mkdir(parents=True)
    exe = bindir / "cargo"
    exe.write_text(f"#!{sys.executable}\n" + '''import os, pathlib, sys
a = sys.argv
if "src/source" not in a[a.index("--file") + 1]:
    sys.exit(1)
if "--list" in a:
    print("src/source/a.rs:3:4: replace g with 1\\nsrc/source/a.rs:5:6: replace h with 2\\n\\nsrc/source/b.rs:1:1: replace k with 0")
    sys.exit(0)
out = pathlib.Path(a[a.index("--output") + 1])
assert pathlib.Path(os.environ["TMPDIR"]) == out / "tmp", os.environ["TMPDIR"]
(pathlib.Path(os.environ["TMPDIR"]) / "cargo-mutants-own.tmp").mkdir()
(out / "mutants.out").mkdir()
(out / "mutants.out" / "missed.txt").write_text("src/source/a.rs:3:4: replace g with 1\\nsrc/source/a.rs:3:4: replace g with 1\\n")
print("3 mutants tested in 1s: 1 missed, 2 caught")
''')
    exe.chmod(0o755)


def fake_gh(bindir: Path, log: Path) -> None:
    """A `gh` that lists the issues in $FAKE_GH_LIST and logs every create/comment call with its stdin."""
    exe = bindir / "gh"
    exe.write_text(f"#!{sys.executable}\n" + '''import os, sys
a = sys.argv[1:]
if a[:2] == ["issue", "list"]:
    print(os.environ["FAKE_GH_LIST"])
else:
    open(os.environ["FAKE_GH_LOG"], "a").write(" ".join(a) + " <<< " + sys.stdin.read().splitlines()[0] + "\\n")
    print("https://github.com/o/r/issues/77")
''')
    exe.chmod(0o755)


def issue_selftest(t: Path, res: dict) -> None:
    """file_issue comments on the open issue of the same title and creates one otherwise."""
    log = t / "gh.log"
    fake_gh(t / "bin", log)
    title = issue_title(res["package"])
    other = json.dumps([{"number": 5, "title": "something else"}])
    same = json.dumps([{"number": 5, "title": "something else"}, {"number": 9, "title": title}])
    for listing, want in ((other, "issue create --title"), (same, "issue comment 9 --body-file -")):
        log.unlink(missing_ok=True)
        with mock_env({**os.environ, "FAKE_GH_LIST": listing, "FAKE_GH_LOG": str(log)}):
            url = file_issue(res, "abcdef0123", "2026-11-07", False, t)
        assert url.endswith("/issues/77") and want in log.read_text() and len(log.read_text().splitlines()) == 1, (listing, log.read_text())


def gate_selftest(t: Path, snap: dict) -> None:
    """run_gate on a temp origin/clone: refusals, one run into <sha>/, the sentinel, and the second call skipping."""
    origin, wt, logs = t / "origin", t / "gate", t / "gate-logs"
    for cmd in (["git", "init", "-q", "-b", "main", str(origin)],
                ["git", "-C", str(origin), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "c1"],
                ["git", "clone", "-q", str(origin), str(wt)]):
        subprocess.run(cmd, check=True)
    ok = [sys.executable, "-c", "print('RIVET-ORACLE-GO: ready')"]
    red = [sys.executable, "-c", "import sys; print('RIVET-ORACLE-NO-GO'); sys.exit(1)"]
    gate_wt = lambda: mock_gate_worktree(wt)

    def go(cmd, snap=snap, dry=False, **kw) -> tuple[int, str]:
        """run_gate with stdout captured; `snap` is a dict or a callable giving one."""
        buf = io.StringIO()
        with gate_wt(), contextlib.redirect_stdout(buf):
            return run_gate(wt, logs, cmd, dry, snap if callable(snap) else (lambda: snap), 4.0, **kw), buf.getvalue()

    sha = git(wt, "rev-parse", "--short=8", "origin/main")
    for bad, word in (({"gates": [7]}, "another release gate"), ({"load1": 9.0}, "not quiet"), ({"free_gib": 10.0}, "start floor"), ({"pause": True}, "PAUSE")):
        rc, out = go(ok, {**snap, **bad})
        assert rc != 0 and word in out and "VERDICT: blocked" in out and not (logs / sha).exists(), (bad, out)
    rc, out = go(ok, dry=True)
    assert rc == 0 and "dry-run" in out and not (logs / sha).exists()
    rc, out = go(ok)
    assert rc == 0 and "RIVET-ORACLE-GO" in out and "VERDICT: done" in out, out
    assert sentinel_rc(logs / sha / "weekly-full.log") == 0
    (logs / sha / "weekly-full.log").write_text("x\n")
    (wt / "dirty").write_text("x")
    rc, out = go(ok)
    assert rc != 0 and "uncommitted" in out
    (wt / "dirty").unlink()
    (logs / sha / "weekly-full.log").unlink()
    rc, out = go(red)
    assert rc != 0 and "RIVET-ORACLE-NO-GO" in out and sentinel_rc(logs / sha / "weekly-full.log") == 1, out
    (logs / sha / "weekly-full.log").write_text("x\nexit 0 2026-11-07T20:00:00Z\n")
    rc, out = go(red)
    assert rc == 0 and "already graded green" in out, "a sha already graded green is not run again"
    other = Path(t) / "elsewhere"
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        assert run_gate(other, logs, GATE_CMD, False, lambda: snap, 4.0) != 0
    assert "not the gate worktree" in buf.getvalue()
    subprocess.run(["git", "-C", str(origin), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "c2"], check=True)
    rc, out = go(ok)
    assert rc != 0 and "--switch" in out and git(wt, "rev-parse", "--short=8", "HEAD") == sha, f"a tree standing elsewhere is not moved without --switch: {out}"
    (logs / sha / "sub").mkdir(parents=True, exist_ok=True)
    for name, text in (("weekly-full.log", "started, never finished\n"), ("gate.log", "gate exit 1 2026-11-07T20:00:00Z\n"), ("master.log", "all done full_rc=1\n"), ("x.out", "?\n"), ("sub/y.log", "?\n")):
        (logs / sha / name).write_text(text)
    rc, out = go(ok, switch=True)
    assert rc == 0 and git(wt, "rev-parse", "--short=8", "HEAD") != sha and "VERDICT: done" in out and "rivet-duckdb" in out, f"a log in any format does not block the switch: {out}"
    loads = iter([9.0, 9.0, 1.0])
    seen = []
    rc, out = go(ok, lambda: (seen.append(1), {**snap, "load1": next(loads)})[1], wait_min=1, poll_s=0)
    assert rc == 0 and len(seen) == 3, f"a loaded machine is waited on, not refused at once: {seen} {out}"
    rc, out = go(ok, {**snap, "load1": 9.0}, wait_min=0)
    assert rc != 0 and "not quiet" in out
    head = git(wt, "rev-parse", "--short=8", "HEAD")
    subprocess.run(["git", "-C", str(origin), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "c3"], check=True)
    calls = iter(range(99))
    rc, out = go(ok, lambda: {**snap, "gates": [7] if next(calls) else []}, switch=True)
    assert rc != 0 and "another release gate" in out and "wait for it" in out and git(wt, "rev-parse", "--short=8", "HEAD") == head, f"a gate that starts after the first check stops the move: {out}"
    rc, out = go(ok, switch=True)
    assert rc == 0 and git(wt, "rev-parse", "--short=8", "HEAD") != head, out
    git(wt, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "candidate")
    off = git(wt, "rev-parse", "--short=8", "HEAD")
    subprocess.run(["git", "-C", str(origin), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "c4"], check=True)
    rc, out = go(ok, switch=True)
    assert rc != 0 and "not an ancestor" in out and git(wt, "rev-parse", "--short=8", "HEAD") == off, f"a head off main is never moved: {out}"
    git(wt, "checkout", "-q", "--detach", "origin/main")
    try:
        prepare_worktree(devbox_load.GATE_WORKTREE, origin)
    except RuntimeError as e:
        assert "gate worktree" in str(e)
    else:
        raise AssertionError("the mutants job accepted the gate worktree")
    for pkg in SUBCOMMANDS:
        assert pkg in __doc__


def tools_selftest(t: Path, snap: dict) -> None:
    """A bare PATH is refused by both jobs with a VERDICT; the mutants job writes its report as it goes and stops at its budget."""
    bare = t / "bare"
    bare.mkdir()
    git_exe = shutil.which("git")
    assert git_exe
    (bare / "git").symlink_to(git_exe)
    gate_args = argparse.Namespace(worktree=t / "gate", logs=t / "gate-logs", dry_run=True, quiet_load=4.0, wait_min=0.0, poll=0.0, switch=False)
    mut = argparse.Namespace(worktree=t / "gate", repo=t / "gate", packages="src/source", plan=False, dry_run_issues=True, report_dir=t / "reports",
                             jobs=1, timeout_min=1, budget_min=BUDGET_MIN, max_load=None, hold_poll=0.0, hold_max_min=0.0)
    with mock_env({**os.environ, "PATH": str(bare)}):
        for job in (lambda: gate_job(gate_args, lambda: snap), lambda: run_mutants(mut)):
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = job()
            assert rc != 0 and "VERDICT: blocked" in buf.getvalue() and "not on PATH" in buf.getvalue(), buf.getvalue()
    origin = t / "origin"
    (origin / "scripts").mkdir(exist_ok=True)
    (origin / "scripts" / "mutants.py").write_text((ROOT / "scripts" / "mutants.py").read_text())
    subprocess.run(["git", "-C", str(origin), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(origin), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "scripts"], check=True)
    mut.worktree = t / "mutants-wt"
    mut.timeout_min = 240
    real_snapshot, devbox_load.snapshot = devbox_load.snapshot, lambda *_: snap
    try:
        _mutants_runs(t, snap, mut)
    finally:
        devbox_load.snapshot = real_snapshot


def budget(real, mut: argparse.Namespace, t: Path, lefts: list[int], expect: int | None, packages: int = 1) -> None:
    """run_mutants with package_timeout answering `lefts` (then stopping the job): the package gets exactly `expect` minutes, or is NOT RUN with no cargo started (None)."""
    seen, answers = [], iter(lefts)

    def spy(*args):
        """run_package that records the minutes it was given."""
        seen.append(args[4])
        return real(*args)

    def left(*_):
        """package_timeout that answers from `lefts` and raises StopIteration when they run out."""
        return next(answers)

    out = t / f"budget-{len(list(t.glob('budget-*')))}"
    g = globals()
    saved = g["run_package"], g["package_timeout"]
    g["run_package"], g["package_timeout"] = spy, left
    try:
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run_mutants(argparse.Namespace(**{**vars(mut), "report_dir": out, "packages": ",".join(["src/source"] * packages)}))
        except (KeyboardInterrupt, StopIteration):
            pass
    finally:
        g["run_package"], g["package_timeout"] = saved
    reports = list(out.glob("*/report.md"))
    assert reports, "a stopped job leaves the rows it had written"
    report = reports[0].read_text()
    if expect is None:
        assert not seen and "NOT RUN" in report and "job budget spent" in report, (seen, report)
    else:
        assert seen == [expect], f"a package gets what is left of the budget, not its own limit: {seen}"


def _mutants_runs(t: Path, snap: dict, mut: argparse.Namespace) -> None:
    """The budget, midway-report and complete runs of run_mutants against a quiet snapshot."""
    with mock_env({**os.environ, "PATH": f"{t / 'bin'}{os.pathsep}{os.environ['PATH']}"}):
        starved = argparse.Namespace(**{**vars(mut), "budget_min": 0.0, "report_dir": t / "starved"})
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            assert run_mutants(starved) != 0
        report = next((t / "starved").glob("*/report.md")).read_text()
        assert "NOT RUN" in report and "job budget spent" in report and "NOT COMPLETE: src/source" in buf.getvalue(), (report, buf.getvalue())
        real, calls = run_package, []

        def die_on_second(*args):
            """run_package that raises on its second call, as a SIGTERM would."""
            calls.append(1)
            if len(calls) > 1:
                raise KeyboardInterrupt
            return real(*args)

        globals()["run_package"] = die_on_second
        try:
            twice = argparse.Namespace(**{**vars(mut), "packages": "src/source,src/source", "report_dir": t / "midway"})
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    run_mutants(twice)
            except KeyboardInterrupt:
                pass
        finally:
            globals()["run_package"] = real
        assert "| COMPLETE |" in next((t / "midway").glob("*/report.md")).read_text(), "a job stopped between packages still leaves the finished rows"
        budget(real, mut, t, [100, 100], expect=100)
        budget(real, mut, t, [5, 0], expect=None)
        budget(real, mut, t, [0, 7], expect=None, packages=2)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = run_mutants(mut)
        assert rc == 0 and "| COMPLETE |" in next((t / "reports").glob("*/report.md")).read_text(), buf.getvalue()


@contextlib.contextmanager
def mock_gate_worktree(wt: Path):
    """Point devbox_load.GATE_WORKTREE at `wt` for the block."""
    saved = devbox_load.GATE_WORKTREE
    devbox_load.GATE_WORKTREE = wt
    try:
        yield
    finally:
        devbox_load.GATE_WORKTREE = saved


def main(argv: list[str]) -> int:
    """Dispatch `gate` or `mutants`."""
    if argv == ["--self-test"]:
        return self_test()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="job", required=True)
    g = sub.add_parser("gate")
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--worktree", type=Path, default=devbox_load.GATE_WORKTREE)
    g.add_argument("--logs", type=Path, default=devbox_load.GATE_LOGS)
    g.add_argument("--quiet-load", type=float, default=4.0)
    g.add_argument("--wait-min", type=float, default=GATE_WAIT_MIN, help="longest wait for a quiet machine before refusing")
    g.add_argument("--poll", type=float, default=60.0)
    g.add_argument("--switch", action="store_true", help="move the gate worktree to origin/main when it stands elsewhere")
    m = sub.add_parser("mutants")
    m.add_argument("--plan", action="store_true", help="print the picked packages and stop")
    m.add_argument("--dry-run-issues", action="store_true", help="print the issues instead of filing them")
    m.add_argument("--packages", help="comma-separated src packages instead of the 30-day churn pick")
    m.add_argument("--worktree", type=Path, default=MUTANTS_WORKTREE)
    m.add_argument("--repo", type=Path, default=Path.home() / "rivet")
    m.add_argument("--report-dir", type=Path, default=REPORT_ROOT)
    m.add_argument("-j", "--jobs", type=int, default=4)
    m.add_argument("--timeout-min", type=int, default=240, help="per package")
    m.add_argument("--budget-min", type=float, default=BUDGET_MIN, help="whole job; packages left over are reported NOT RUN")
    m.add_argument("--max-load", type=float)
    m.add_argument("--hold-poll", type=float, default=60.0)
    m.add_argument("--hold-max-min", type=float, default=120.0)
    a = ap.parse_args(argv)
    if a.job == "gate":
        return gate_job(a)
    return run_mutants(a)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
