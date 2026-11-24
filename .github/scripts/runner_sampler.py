#!/usr/bin/env python3
"""A hosted slice's memory, swap, disk and running-test record, kept off the runner: a file for the artifact and a check run the runner's loss cannot take with it.

    runner_sampler.py start NAME OUT   shield the runner agent from the OOM killer, then sample into OUT/sampler.tsv from a detached process
    runner_sampler.py stop OUT [DIR]   end the sampler, close its check run, copy the nextest logs of DIR into OUT/nextest
    runner_sampler.py --self-test

The check run (`GITHUB_TOKEN` with `checks: write`, `GITHUB_REPOSITORY`, `SAMPLER_SHA`) holds the whole series in its output text and is patched every REPORT_S seconds,
so a runner that vanishes leaves its last minutes on the commit; a sampler that cannot reach the API keeps sampling to the file.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

INTERVAL_S = 30
REPORT_S = 120
AGENTS = ("Runner.Listener", "Runner.Worker")
TEXT_LIMIT = 60000
HEADER = "time_utc\telapsed_s\tmem_used_mb\tmem_avail_mb\tswap_used_mb\tload1\tdisk_free_gb\toom_kills\tcontainers_mib\tprocesses_mb\toracle_limits\trunning_tests"
RUNNING_TEST = re.compile(r"--exact\s+(\S+)")


def meminfo(text: str) -> dict[str, int]:
    """The kB fields of /proc/meminfo `text`."""
    return {k: int(v) for k, v in re.findall(r"^(\w+):\s+(\d+)", text, re.M)}


def containers(out: str, top: int = 6) -> str:
    """`name:MiB` of the `top` biggest containers of `docker stats --format '{{.Name}} {{.MemUsage}}'` output."""
    units = {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "B": 1 / 1048576}
    rows = []
    for line in out.splitlines():
        m = re.match(r"(\S+)\s+([\d.]+)([KMG]?i?B)\b", line)
        if m and m[3] in units:
            rows.append((float(m[2]) * units[m[3]], m[1]))
    return ",".join(f"{n.removeprefix('rivet-')}:{mib:.0f}" for mib, n in sorted(rows, reverse=True)[:top])


ORACLE_LIMITS_ARGV = ["docker", "exec", "rivet-oracle-1", "bash", "-c",
                      "echo \"set pagesize 0 feedback off\nselect resource_name||' '||current_utilization||'/'||max_utilization||'/'||limit_value "
                      "from v\\$resource_limit where resource_name in ('processes','sessions');\" | sqlplus -s / as sysdba"]


def oracle_limits(out: str) -> str:
    """`processes:current/peak/limit,sessions:...` of the stand Oracle's `v$resource_limit` rows, empty when it cannot be read."""
    rows = re.findall(r"^(processes|sessions) (\d+/\d+/\w+)\s*$", out, re.M)
    return ",".join(f"{name}:{figures}" for name, figures in rows)


PS_ARGV = ["ps", "-eww", "-o", "pid=,ppid=,rss=,args="]


def ps_rows(ps: str) -> dict[int, tuple[int, int, str]]:
    """`{pid: (ppid, rss_kb, args)}` from `PS_ARGV` output; a line that does not start with three numbers is a continuation and is dropped."""
    rows: dict[int, tuple[int, int, str]] = {}
    for line in ps.splitlines():
        parts = line.split(None, 3)
        if len(parts) == 4 and all(p.isdigit() for p in parts[:3]):
            rows[int(parts[0])] = (int(parts[1]), int(parts[2]), parts[3])
    return rows


def processes(ps: str, top: int = 4) -> tuple[str, str]:
    """(`name@test:MB` of the `top` biggest processes, the names of the live tests) from `PS_ARGV` output; `name` is the program, or its `-m` module, and `test` the nearest ancestor's `--exact` test."""
    rows = ps_rows(ps)
    tests = {pid: t[1] for pid, (_, _, args) in rows.items() if (t := RUNNING_TEST.search(args))}

    def owner(pid: int) -> str:
        """The test of `pid` or of its nearest ancestor, `-` when none runs it."""
        for _ in range(12):
            if pid in tests:
                return tests[pid].rsplit("::", 1)[-1][:40]
            pid = rows[pid][0] if pid in rows else 0
        return "-"

    def name(args: str) -> str:
        """The program of `args`, its `-m` module for an interpreter."""
        words = args.split()
        mod = words[words.index("-m") + 1].rsplit(".", 1)[-1] if "-m" in words[:-1] else ""
        return mod or Path(words[0]).name

    big = ",".join(f"{name(args)}@{owner(pid)}:{kb // 1024}" for pid, (_, kb, args) in sorted(rows.items(), key=lambda r: -r[1][1])[:top])
    return big, ",".join(sorted(set(tests.values()))[:4])


def sh(cmd: list[str]) -> str:
    """The stdout of `cmd`, empty when it cannot run or fails."""
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError):
        return ""


def sample(started: float, now: float | None = None, read=lambda p: Path(p).read_text(), run=sh) -> str:
    """One TSV row of the machine now: memory, swap, load, disk, OOM kills, the biggest containers and processes, the tests running."""
    now = time.time() if now is None else now
    mem = meminfo(read("/proc/meminfo"))
    swap_used = (mem.get("SwapTotal", 0) - mem.get("SwapFree", 0)) // 1024
    used = (mem.get("MemTotal", 0) - mem.get("MemAvailable", 0)) // 1024
    kills = re.search(r"^oom_kill (\d+)", read("/proc/vmstat"), re.M)
    free_gb = shutil.disk_usage("/").free // (1 << 30)
    big, tests = processes(run(PS_ARGV))
    cols = [time.strftime("%H:%M:%S", time.gmtime(now)), f"{now - started:.0f}", str(used), str(mem.get("MemAvailable", 0) // 1024), str(swap_used),
            read("/proc/loadavg").split()[0], str(free_gb), kills[1] if kills else "-",
            containers(run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}"])), big, oracle_limits(run(ORACLE_LIMITS_ARGV)), tests]
    return "\t".join(cols)


def shield(procs: str = "/proc", write=None) -> list[str]:
    """Set oom_score_adj -1000 on the runner agent's processes so memory pressure kills a test, never the connection to the server; returns the pids shielded."""
    done = []
    for d in sorted(Path(procs).iterdir()):
        if not d.name.isdigit():
            continue
        try:
            comm = (d / "comm").read_text().strip()
        except OSError:
            continue
        if comm in AGENTS:
            try:
                (write or (lambda p: subprocess.run(["sudo", "tee", str(p)], input="-1000\n", text=True, capture_output=True, check=True)))(d / "oom_score_adj")
                done.append(d.name)
            except (OSError, subprocess.SubprocessError):
                pass
    return done


class Report:
    """The check run that holds the series: created on the first report, patched after, closed at the end; every API failure is swallowed."""

    def __init__(self, name: str, api: str, repo: str, sha: str, token: str) -> None:
        self.name, self.api, self.repo, self.sha, self.token = name, api, repo, sha, token
        self.id: int | None = None
        self.error = ""

    def call(self, method: str, path: str, body: dict) -> dict:
        """One JSON request to the API."""
        req = urllib.request.Request(f"{self.api}/repos/{self.repo}{path}", json.dumps(body).encode(), method=method, headers={
            "Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json", "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read() or b"{}")

    def push(self, rows: list[str], done: bool = False) -> None:
        """Publish the series so far; `done` closes the check run as neutral."""
        text = "\n".join(rows)[-TEXT_LIMIT:]
        body = {"output": {"title": f"{self.name}: {len(rows)} samples, last {rows[-1].split(chr(9))[0] if rows else '-'}", "summary": HEADER.replace("\t", " | "), "text": "```\n" + HEADER + "\n" + text + "\n```"},
                **({"status": "completed", "conclusion": "neutral"} if done else {"status": "in_progress"})}
        try:
            if self.id is None:
                self.id = self.call("POST", "/check-runs", {"name": self.name, "head_sha": self.sha, **body})["id"]
            else:
                self.call("PATCH", f"/check-runs/{self.id}", body)
        except Exception as e:  # noqa: BLE001 - evidence must never fail the slice
            self.error = f"{type(e).__name__}: {e}"


def loop(out: Path, interval: float, report_every: float, report: Report | None, stop=lambda: False, clock=time.time, sleep=time.sleep, take=sample) -> list[str]:
    """Sample every `interval` seconds into OUT/sampler.tsv and push the series every `report_every`, until `stop()`; returns the rows."""
    started, last, rows = clock(), 0.0, []
    path = out / "sampler.tsv"
    path.write_text(HEADER + "\n")
    while True:
        rows.append(take(started, clock()))
        with path.open("a") as f:
            f.write(rows[-1] + "\n")
        halt = stop()
        if report and (halt or clock() - last >= report_every):
            report.push(rows, done=halt)
            last = clock()
        if halt:
            return rows
        sleep(interval)


def report_from_env(name: str) -> Report | None:
    """The check-run reporter the environment configures, or None without a token."""
    env = os.environ
    if not (env.get("GITHUB_TOKEN") and env.get("GITHUB_REPOSITORY") and env.get("SAMPLER_SHA")):
        return None
    return Report(name, env.get("GITHUB_API_URL", "https://api.github.com"), env["GITHUB_REPOSITORY"], env["SAMPLER_SHA"], env["GITHUB_TOKEN"])


def start(name: str, out: Path) -> int:
    """Shield the agent, print the first sample and detach the sampler; its pid goes to OUT/sampler.pid."""
    out.mkdir(parents=True, exist_ok=True)
    print(f"runner_sampler: shielded pids {shield() or 'none (no sudo or no agent found)'}")
    print(HEADER + "\n" + sample(time.time()))
    if os.fork():
        return 0
    os.setsid()
    (out / "sampler.pid").write_text(str(os.getpid()))
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
    devnull = os.open(os.devnull, os.O_RDWR)
    for fd in (0, 1, 2):
        os.dup2(devnull, fd)
    report = report_from_env(f"{name} sampler")
    loop(out, INTERVAL_S, REPORT_S, report, stop=lambda: bool(stop))
    if report and report.error:
        with (out / "sampler.tsv").open("a") as f:
            f.write(f"# report error: {report.error}\n")
    return 0


def stop_sampler(out: Path, logs: Path | None) -> int:
    """End the detached sampler (it closes its check run), then copy the nextest logs of `logs` into OUT/nextest."""
    pid_file = out / "sampler.pid"
    if pid_file.is_file():
        pid = int(pid_file.read_text())
        try:
            os.kill(pid, signal.SIGTERM)
            for _ in range(60):
                os.kill(pid, 0)
                time.sleep(1)
        except OSError:
            pass
    if logs and logs.is_dir():
        (out / "nextest").mkdir(exist_ok=True)
        for f in logs.glob("*.log"):
            shutil.copy(f, out / "nextest" / f.name)
    tsv = out / "sampler.tsv"
    print(tsv.read_text()[-4000:] if tsv.is_file() else "runner_sampler: no sampler.tsv")
    return 0


def self_test() -> None:
    """With fake /proc files and a local API: the row reads every field, the shield names only the agent, the check run holds the series and an unreachable API never raises."""
    import http.server
    import threading

    meminfo_text = "MemTotal:       16000000 kB\nMemAvailable:    4000000 kB\nSwapTotal:       3000000 kB\nSwapFree:         100000 kB\n"
    files = {"/proc/meminfo": meminfo_text, "/proc/vmstat": "nr_free 1\noom_kill 2\n", "/proc/loadavg": "7.50 3 2 1/1 1"}
    ps = ("10 1 3000000 /usr/bin/mongod --x\n20 1 2048 /t/live_suite-abc --exact live_cdc::a_test --nocapture\n"
          "21 20 1800000 /usr/bin/python3 -m dev.release_oracle.rig_oracle verify\nimport duckdb, json\n22 21 5 bash\n")
    dstats = "rivet-oracle-1 2.4GiB / 15GiB\nrivet-mssql-1 800MiB / 3GiB\nnoise\n"
    limits = "processes 41/612/1000\nsessions 30/700/1522\nnoise 1/2/3\n"
    row = sample(100.0, 160.0, read=lambda p: files[p],
                 run=lambda c: ps if c[0] == "ps" else limits if c[:2] == ["docker", "exec"] else dstats).split("\t")
    assert row[1:8] == ["60", str(12000000 // 1024), str(4000000 // 1024), str(2900000 // 1024), "7.50", row[6], "2"], row
    assert row[8] == "oracle-1:2458,mssql-1:800", row[8]
    assert row[9] == "mongod@-:2929,rig_oracle@a_test:1757,live_suite-abc@a_test:2,bash@a_test:0" and row[11] == "live_cdc::a_test", row[9:]
    assert row[10] == "processes:41/612/1000,sessions:30/700/1522", row[10]
    assert oracle_limits("Error: No such container: rivet-oracle-1") == "", "an absent stand Oracle leaves the column empty"
    assert len(row) == len(HEADER.split("\t")), "a row and its header differ in width"
    me = ps_rows(sh(PS_ARGV)).get(os.getpid())
    assert me and me[0] == os.getppid() and me[1] > 0 and Path(sys.argv[0]).name in me[2], f"the ps argv and the parser disagree on this process: {me}"
    assert containers("a 1KiB / 3GiB") == "a:0" and containers("garbage") == "", "a unit or a line it cannot read is not skipped"
    with tempfile.TemporaryDirectory() as d:
        proc = Path(d, "proc")
        for pid, comm in (("11", "Runner.Listener"), ("12", "dockerd"), ("13", "Runner.Worker"), ("x", "Runner.Worker")):
            (proc / pid).mkdir(parents=True)
            (proc / pid / "comm").write_text(comm + "\n")
        wrote = []
        assert shield(str(proc), write=lambda p: wrote.append(p.parent.name)) == ["11", "13"] and wrote == ["11", "13"], wrote
        calls: list[tuple[str, str, dict]] = []

        class Api(http.server.BaseHTTPRequestHandler):
            """A GitHub check-runs endpoint that records what it is sent."""

            def do_POST(self) -> None:  # noqa: N802
                self.answer()

            def do_PATCH(self) -> None:  # noqa: N802
                self.answer()

            def answer(self) -> None:
                """Record the request and answer a check-run id."""
                calls.append((self.command, self.path, json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
                body = b'{"id": 77}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a) -> None:
                """Silence the server."""

        srv = http.server.HTTPServer(("127.0.0.1", 0), Api)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        out = Path(d, "out")
        out.mkdir()
        ticks = iter(range(1000, 1000000, 40))
        n = []
        rep = Report("slice 2 sampler", f"http://127.0.0.1:{srv.server_port}", "o/r", "abc", "tok")

        def fake(started: float, now: float) -> str:
            """A sample of the fake machine."""
            return sample(started, now, read=lambda p: files[p], run=lambda c: ps if c[0] == "ps" else dstats)

        def stopped() -> bool:
            """True from the seventh sample on."""
            n.append(1)
            return len(n) > 6

        rows = loop(out, 30, 100, rep, stop=stopped, clock=lambda: next(ticks), sleep=lambda s: None, take=fake)
        srv.shutdown()
        assert (out / "sampler.tsv").read_text().count("\n") == len(rows) + 1, "the file does not hold every sample"
        assert [c[0] for c in calls][:2] == ["POST", "PATCH"] and calls[0][1] == "/repos/o/r/check-runs" and calls[1][1] == "/repos/o/r/check-runs/77", calls[:2]
        assert calls[-1][2]["conclusion"] == "neutral" and rows[-1].split("\t")[0] in calls[-1][2]["output"]["text"], "the last sample is not in the closed check run"
        assert rows[0].split("\t")[0] in calls[-1][2]["output"]["text"], "the check run holds only the tail of the series"
        dead = Report("x", "http://127.0.0.1:1", "o/r", "abc", "tok")
        dead.push(["a\tb"])
        assert dead.error and dead.id is None, "an unreachable API raised or left no mark"
    assert report_from_env("n") is None or os.environ.get("GITHUB_TOKEN"), "a reporter without a token"
    print("runner_sampler self-test ok")


def main(argv: list[str]) -> int:
    """Dispatch the three commands."""
    if argv == ["--self-test"]:
        self_test()
        return 0
    if len(argv) == 3 and argv[0] == "start":
        return start(argv[1], Path(argv[2]))
    if len(argv) in (2, 3) and argv[0] == "stop":
        return stop_sampler(Path(argv[1]), Path(argv[2]) if len(argv) == 3 else None)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
