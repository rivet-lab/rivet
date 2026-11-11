#!/usr/bin/env python3
"""The satellite pre-flight of the release gate: separate light checks on the gate's sha, on machines that are not the gate.

    satellite.py linux  --sha SHA [--repo DIR] [--out DIR] [--new-bin FILE]   on the Linux x86_64 satellite itself
    satellite.py mac    --sha SHA [--repo DIR] [--out DIR] [--release-date YYYY-MM-DD]   on a machine with cloud credentials
    satellite.py remote --sha SHA [--host H] [--label L]     drive `linux` over ssh, fetch the result, publish it
    satellite.py publish --sha SHA --dir DIR [--label L]     copy a result directory beside the gate's logs
    satellite.py --self-test

Each profile writes `verdict.json` and `logs/` into its output directory. Every check is PASS, FAIL or NOT-RUN with
its first failing line; a NOT-RUN is never a PASS, and the exit code is 0 only when every check passed.

Release-blocking policy (verdict.json lists what blocks):
    * a check that is not PASS in the pilot scenario (`pilot-*`) or the release notes (`release-notes-*`) blocks the release;
    * a cloud smoke (`cloud-*`) that is FAIL blocks only when its re-run is FAIL too;
    * every other check is reported and decided by the coordinator.

The pilot scenario is the pilot client's shape: SQLite state, incremental exports, a Linux x86_64 binary. The previous
release runs first, the binary under test continues on the same state, the previous release runs again; the destination
is re-read with pyarrow and compared with the source, never with rivet's own counters. A second run of one export,
started while the first holds the lease, must be refused. The Linux builds share one cargo target dir, RIVET_SATELLITE_TARGET_DIR (default ~/.cache/rivet-satellite/target).
The host comes from RIVET_SATELLITE_HOST (an ssh alias), the
gate's log host from RIVET_GATE_LOGS_HOST and its directory from RIVET_GATE_LOGS_DIR; nothing about a machine is stored here.
Stdlib only; runs on Python 3.10.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import os
import re
import shlex
import shutil
import socket
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import traceback
import urllib.request
from collections import Counter
from contextlib import closing
from pathlib import Path
from unittest import mock

PASS, FAIL, NOT_RUN, FLAKY = "PASS", "FAIL", "NOT-RUN", "FLAKY"
OLD_RELEASE = "0.32.2"
RELEASES = "https://github.com/rivet-lab/rivet/releases/download"
ASSET = "rivet-v{v}-x86_64-unknown-linux-gnu-glibc2.28"
UV_VERSION = "0.6.3"
UV_ASSET = f"https://github.com/astral-sh/uv/releases/download/{UV_VERSION}/uv-x86_64-unknown-linux-gnu.tar.gz"
LABEL = "rivet.satellite=1"
PREFIX = "rivet-satellite"
ENGINES = {
    "postgres": {"port": 0, "inner": 5432, "image": "postgres:16"},
    "mysql": {"port": 0, "inner": 3306, "image": "mysql:8.0"},
}
SEED_ROWS, DELTA_A, DELTA_B = 500, (501, 700), (701, 800)
CLOUD_CELLS = {
    "cloud-bigquery": ("bigquery", "live_load_spec::bigquery_full_load_clusters_on_the_recorded_key_and_overwrites_its_own_table"),
    "cloud-snowflake": ("snowflake", "live_snowflake_load::snowflake_full_load_lands_and_the_next_cycle_follows_the_source_postgres"),
}
STAND_PG_PORT = 5432
HARD = ("pilot-", "release-notes")
SOFT = ("cloud-",)
READER = (
    "import glob, sys, pyarrow.parquet as pq\n"
    "for f in sorted(glob.glob(sys.argv[1] + '/*.parquet')):\n"
    "    t = pq.read_table(f).to_pydict()\n"
    "    for i, p in zip(t['id'], t['payload']):\n"
    "        print(f'{i},{p}')\n"
)
ROOT = Path(__file__).resolve().parents[2]


def say(text: str) -> None:
    """Print one progress line at once."""
    print(text, flush=True)


def scrub(text: str) -> str:
    """`text` without this machine's home directory, host name or user name."""
    for needle, repl in ((str(Path.home()), "~"), (socket.gethostname(), "<host>"), (os.environ.get("USER", ""), "<user>")):
        if len(needle) > 2:
            text = text.replace(needle, repl)
    return text


def first_failing_line(text: str) -> str:
    """The first line that reads as a failure, else the last non-empty line."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines:
        if re.search(r"(^|\W)(error|Error|ERROR|FAILED|FAIL|panicked|assertion|✗)(\W|$)", ln):
            return ln[:300]
    return lines[-1][:300] if lines else "no output"


@dataclasses.dataclass
class Check:
    """One check: its name, PASS | FAIL | FLAKY | NOT-RUN, the first failing line (or the proof line), wall seconds, every attempt's status."""

    name: str
    status: str
    line: str
    seconds: float
    attempts: list[str] = dataclasses.field(default_factory=list)


def blocks(check: Check) -> str | None:
    """Why `check` blocks the release under the policy, else None."""
    if check.status == PASS:
        return None
    if check.name.startswith(HARD):
        return f"{check.status}: {check.line}"
    if check.name.startswith(SOFT) and (check.status in (FAIL, FLAKY) or FAIL in check.attempts):
        return f"{check.status}: {check.line}"
    return None


def overall(checks: list[Check]) -> str:
    """FAIL if any check failed or was flaky, else NOT-RUN if any did not run (or none did), else PASS."""
    if any(c.status in (FAIL, FLAKY) for c in checks):
        return FAIL
    if not checks or any(c.status != PASS for c in checks):
        return NOT_RUN
    return PASS


class Run:
    """A profile run: the checks so far, the logs directory, and `verdict.json` rewritten after each check."""

    def __init__(self, profile: str, sha: str, out: Path) -> None:
        self.profile, self.sha, self.out = profile, sha, out
        self.checks: list[Check] = []
        self.started = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        (out / "logs").mkdir(parents=True, exist_ok=True)

    def log(self, name: str) -> Path:
        """The log file of one check."""
        return self.out / "logs" / (re.sub(r"[^A-Za-z0-9_.-]+", "_", name) + ".log")

    def add(self, name: str, status: str, line: str, seconds: float = 0.0, attempts: list[str] | None = None) -> Check:
        """Record a check and rewrite the verdict."""
        c = Check(name, status, scrub(line), round(seconds, 1), attempts or [])
        self.checks.append(c)
        say(f"satellite: {name}: {status} ({c.seconds}s) {c.line}")
        self.write()
        return c

    def step(self, name: str, fn) -> Check:
        """Run `fn() -> (status, line)`; an exception is a FAIL carrying its message, the traceback goes to the log."""
        t = time.monotonic()
        try:
            status, line = fn()
        except Exception as e:
            self.log(name).open("a").write(traceback.format_exc())
            status, line = FAIL, f"{type(e).__name__}: {e}"
        return self.add(name, status, line, time.monotonic() - t)

    def write(self) -> None:
        """Write `verdict.json` atomically."""
        body = {
            "profile": self.profile,
            "sha": self.sha,
            "started": self.started,
            "overall": overall(self.checks),
            "blocking": [f"{c.name}: {why}" for c in self.checks if (why := blocks(c))],
            "checks": [dataclasses.asdict(c) for c in self.checks],
        }
        tmp = self.out / "verdict.json.tmp"
        tmp.write_text(json.dumps(body, indent=2) + "\n")
        tmp.replace(self.out / "verdict.json")

    def exit_code(self) -> int:
        """0 when every check passed, 1 when one failed, 2 when only NOT-RUN checks are left."""
        return {PASS: 0, FAIL: 1, NOT_RUN: 2}[overall(self.checks)]


def sh(argv: list[str], log: Path, cwd: Path | None = None, env: dict | None = None, timeout: int | None = None, stdin: str | None = None) -> tuple[int, str]:
    """Run `argv` with its output appended to `log`: (exit code, that output); a timeout is exit 124."""
    with log.open("a") as fh:
        fh.write(f"$ {scrub(' '.join(shlex.quote(a) for a in argv))}\n")
        fh.flush()
        try:
            p = subprocess.run(argv, cwd=cwd, env=env, input=stdin, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
            rc, text = p.returncode, p.stdout
        except subprocess.TimeoutExpired as e:
            rc, text = 124, f"{e.stdout or ''}\ntimed out after {timeout}s"
        except FileNotFoundError as e:
            rc, text = 127, str(e)
        fh.write(text + f"\n[exit {rc}]\n")
    return rc, text


def git_env() -> dict:
    """The environment of a git call: anonymous https, no prompts."""
    return {**os.environ, "GIT_TERMINAL_PROMPT": "0"}


def cargo_bin() -> str:
    """The cargo executable."""
    return shutil.which("cargo") or str(Path.home() / ".cargo" / "bin" / "cargo")


def checkout(run: Run, repo: Path, sha: str, url: str) -> tuple[str, str]:
    """Clone or update `repo` and detach it at `sha`; PASS only when HEAD is that commit."""
    log = run.log("checkout")
    if (repo / ".git").exists() and head_is(repo, sha, log)[0] == PASS:
        return head_is(repo, sha, log)
    if not (repo / ".git").exists():
        repo.parent.mkdir(parents=True, exist_ok=True)
        rc, text = sh(["git", "-c", "credential.helper=", "clone", "--no-checkout", url, str(repo)], log, env=git_env())
        if rc:
            return FAIL, first_failing_line(text)
    sh(["git", "-c", "credential.helper=", "fetch", "origin", sha], log, cwd=repo, env=git_env())
    rc, text = sh(["git", "checkout", "--detach", "-f", sha], log, cwd=repo)
    if rc:
        return FAIL, f"cannot check out {sha}: {first_failing_line(text)}"
    return head_is(repo, sha, log)


def head_is(repo: Path, sha: str, log: Path) -> tuple[str, str]:
    """PASS when `repo`'s HEAD is `sha` (a full or abbreviated hash)."""
    rc, head = sh(["git", "rev-parse", "HEAD"], log, cwd=repo)
    head = head.splitlines()[0] if head.strip() else ""
    ok = rc == 0 and head.startswith(sha.lower()) and len(sha) >= 7
    return (PASS, f"HEAD {head[:12]}") if ok else (FAIL, f"HEAD is {head[:12] or 'unknown'}, not {sha}")


def binary_version(binary: Path, log: Path) -> str:
    """The first line of `binary --version`, or an empty string."""
    rc, text = sh([str(binary), "--version"], log, timeout=60)
    return text.splitlines()[0].strip() if rc == 0 and text.strip() else ""


def target_dir() -> Path:
    """The cargo target dir every Linux build and test run shares, so dependencies compile once."""
    return Path(os.environ.get("RIVET_SATELLITE_TARGET_DIR") or Path.home() / ".cache" / "rivet-satellite" / "target").expanduser().resolve()


def built_verdict(binary: Path, sha: str, rc: int, wall: float, text: str, log: Path) -> tuple[str, str]:
    """PASS only when the build succeeded and the binary names `sha`; a binary left in the shared target dir by another sha is FAIL."""
    if rc or not binary.exists():
        return FAIL, f"cargo build --release exit {rc} after {wall:.0f}s: {first_failing_line(text)}"
    version = binary_version(binary, log)
    if sha[:7] not in version:
        return FAIL, f"built in {wall:.0f}s but `{version}` does not name {sha[:7]}: a stale binary"
    return PASS, f"built in {wall:.0f}s: {version}"


def build_release(run: Run, repo: Path, sha: str) -> tuple[str, str]:
    """`cargo build --release` into the shared target dir; the line carries the wall time and the version, which must name the sha."""
    target = target_dir()
    target.mkdir(parents=True, exist_ok=True)
    t = time.monotonic()
    rc, text = sh([cargo_bin(), "build", "--release"], run.log("build-release"), cwd=repo, env={**os.environ, "CARGO_TARGET_DIR": str(target)})
    return built_verdict(target / "release" / "rivet", sha, rc, time.monotonic() - t, text, run.log("build-release"))


def offline_suite(run: Run, repo: Path, py: Path | None) -> tuple[str, str]:
    """The repo's own offline suite (pr_ready.offline_suite) with the locked python first on PATH."""
    code = "import sys; from pathlib import Path; from dev.pytools import pr_ready; v, l = pr_ready.offline_suite(Path(sys.argv[1])); print(v); print(l); sys.exit(0 if v == 'PASS' else 1)"
    (run.out / "offline").mkdir(exist_ok=True)
    env = {**os.environ, "CARGO_TARGET_DIR": str(target_dir())}
    if py:
        env["PATH"] = f"{py.parent}{os.pathsep}{os.environ['PATH']}"
    rc, text = sh([sys.executable, "-c", code, str(run.out / "offline")], run.log("offline-suite"), cwd=repo, env=env)
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.startswith("[exit")]
    tail = lines[-1] if lines else "no output"
    return (PASS, tail) if rc == 0 and len(lines) > 1 and lines[-2] == "PASS" else (FAIL, tail)


def download(url: str, dest: Path) -> None:
    """Fetch `url` into `dest` (a part file renamed on success)."""
    part = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(url, timeout=300) as r, part.open("wb") as fh:
        shutil.copyfileobj(r, fh)
    part.replace(dest)


def sha256_of(path: Path) -> str:
    """The SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def fetch_old_release(run: Run, cache: Path) -> tuple[str, str]:
    """Download the published OLD_RELEASE linux x86_64 asset, verify it against SHA256SUMS.txt, unpack `rivet`."""
    cache.mkdir(parents=True, exist_ok=True)
    name = ASSET.format(v=OLD_RELEASE)
    tarball, sums = cache / f"{name}.tar.gz", cache / f"SHA256SUMS-{OLD_RELEASE}.txt"
    binary = cache / f"rivet-{OLD_RELEASE}"
    base = f"{RELEASES}/v{OLD_RELEASE}"
    download(f"{base}/SHA256SUMS.txt", sums)
    want = next((ln.split()[0] for ln in sums.read_text().splitlines() if ln.endswith(f"{name}.tar.gz")), "")
    if not want:
        return FAIL, f"SHA256SUMS.txt of v{OLD_RELEASE} lists no {name}.tar.gz"
    if not tarball.exists() or sha256_of(tarball) != want:
        download(f"{base}/{name}.tar.gz", tarball)
    got = sha256_of(tarball)
    if got != want:
        return FAIL, f"{tarball.name} sha256 {got[:12]} is not the published {want[:12]}"
    with tarfile.open(tarball) as tf:
        member = tf.getmember(f"{name}/rivet")
        member.name = binary.name
        tf.extract(member, cache, filter="data") if sys.version_info >= (3, 12) else tf.extract(member, cache)
    binary.chmod(0o755)
    version = binary_version(binary, run.log("pilot-download"))
    if f"rivet {OLD_RELEASE}" not in version:
        return FAIL, f"the downloaded binary says `{version}`, not {OLD_RELEASE}"
    return PASS, f"{version} (sha256 {want[:12]} as published)"


def uv_binary(cache: Path) -> str:
    """The uv executable: the one on PATH or ~/.local/bin, else the pinned release fetched into `cache` and checked against its sha256."""
    uv = shutil.which("uv") or str(Path.home() / ".local" / "bin" / "uv")
    if Path(uv).exists():
        return uv
    tarball = cache / "uv.tar.gz"
    download(UV_ASSET, tarball)
    with urllib.request.urlopen(UV_ASSET + ".sha256", timeout=60) as r:
        want = r.read().decode().split()[0]
    if sha256_of(tarball) != want:
        raise RuntimeError("the uv tarball does not match its published sha256")
    with tarfile.open(tarball) as tf:
        member = tf.getmember("uv-x86_64-unknown-linux-gnu/uv")
        member.name = "uv"
        tf.extract(member, cache)
    (cache / "uv").chmod(0o755)
    return str(cache / "uv")


def repo_python(run: Run, repo: Path, cache: Path) -> Path:
    """The checkout's own locked python environment (`uv sync --frozen`, python 3.12): the destination reader and the offline suite's python3."""
    rc, text = sh([uv_binary(cache), "sync", "--frozen", "--python", "3.12", "-q"], run.log("pilot-reader"), cwd=repo, timeout=900)
    if rc:
        raise RuntimeError(f"uv sync failed: {first_failing_line(text)}")
    py = repo / ".venv" / "bin" / "python"
    rc, text = sh([str(py), "-c", "import pyarrow; print(pyarrow.__version__)"], run.log("pilot-reader"))
    if rc:
        raise RuntimeError(f"the locked environment has no pyarrow: {first_failing_line(text)}")
    return py


def container(engine: str) -> str:
    """The container name of one pilot source."""
    return f"{PREFIX}-{engine}"


def docker_run_argv(engine: str) -> list[str]:
    """`docker run` for one pilot source: published on 127.0.0.1 only, labelled, data in a labelled named volume."""
    e = ENGINES[engine]
    env = (["-e", "POSTGRES_USER=rivet", "-e", "POSTGRES_PASSWORD=rivet", "-e", "POSTGRES_DB=rivet"] if engine == "postgres"
           else ["-e", "MYSQL_ROOT_PASSWORD=root", "-e", "MYSQL_USER=rivet", "-e", "MYSQL_PASSWORD=rivet", "-e", "MYSQL_DATABASE=rivet"])
    data = "/var/lib/postgresql/data" if engine == "postgres" else "/var/lib/mysql"
    return ["docker", "run", "-d", "--name", container(engine), "--label", LABEL, "-p", f"127.0.0.1::{e['inner']}",
            "-v", f"{container(engine)}-data:{data}", *env, e["image"]]


def published_port(text: str) -> int | None:
    """The host port of a `docker port` answer bound on 127.0.0.1; None when no line is."""
    m = re.search(r"^127\.0\.0\.1:(\d+)$", text, re.M)
    return int(m.group(1)) if m else None


def source_url(engine: str) -> str:
    """The URL rivet reads a pilot source by."""
    scheme = "postgresql" if engine == "postgres" else "mysql"
    return f"{scheme}://rivet:rivet@127.0.0.1:{ENGINES[engine]['port']}/rivet"


def sql(engine: str, text: str, log: Path) -> tuple[int, str]:
    """Run native SQL `text` through the engine's own client inside its container."""
    if engine == "postgres":
        argv = ["docker", "exec", "-i", container(engine), "psql", "-h", "127.0.0.1", "-U", "rivet", "-d", "rivet", "-v", "ON_ERROR_STOP=1", "-q"]
    else:
        argv = ["docker", "exec", "-i", container(engine), "mysql", "--default-character-set=utf8mb4", "-h127.0.0.1", "-urivet", "-privet", "rivet"]
    return sh(argv, log, stdin=text, timeout=120)


def rows_of(engine: str, query: str, log: Path) -> list[str]:
    """The result lines of one query, run through the engine's client."""
    if engine == "postgres":
        argv = ["docker", "exec", container(engine), "psql", "-h", "127.0.0.1", "-U", "rivet", "-d", "rivet", "-v", "ON_ERROR_STOP=1", "-Atc", query]
    else:
        argv = ["docker", "exec", container(engine), "mysql", "--default-character-set=utf8mb4", "-h127.0.0.1", "-urivet", "-privet", "-N", "-B", "-e", query, "rivet"]
    p = subprocess.run(argv, capture_output=True, text=True, timeout=120)
    log.open("a").write(f"$ {query}\n{p.stdout[-400:]}{p.stderr[-400:]}[exit {p.returncode}]\n")
    if p.returncode:
        raise RuntimeError(f"{engine} query failed: {first_failing_line(p.stderr)}")
    return [ln for ln in p.stdout.splitlines() if ln]


def source_rows(engine: str, log: Path) -> list[str]:
    """Every `id,payload` line the source holds, read through its own client."""
    concat = "CONCAT(id, ',', payload)" if engine == "mysql" else "id || ',' || payload"
    return rows_of(engine, f"SELECT {concat} FROM pilot_items ORDER BY id", log)


def insert_sql(engine: str, lo: int, hi: int) -> str:
    """Native SQL inserting ids lo..hi with a deterministic payload."""
    if engine == "postgres":
        return f"INSERT INTO pilot_items SELECT g, g % 7, md5(g::text) FROM generate_series({lo}, {hi}) g;\n"
    return f"INSERT INTO pilot_items WITH RECURSIVE s(g) AS (SELECT {lo} UNION ALL SELECT g + 1 FROM s WHERE g < {hi}) SELECT g, g % 7, MD5(CAST(g AS CHAR)) FROM s;\n"


def create_sql(engine: str) -> str:
    """Native SQL creating the pilot table."""
    text = "VARCHAR(64)" if engine == "mysql" else "TEXT"
    return f"CREATE TABLE pilot_items (id BIGINT PRIMARY KEY, grp INT NOT NULL, payload {text} NOT NULL);\n"


def stand_down(log: Path) -> None:
    """Remove the pilot containers and volumes; absent ones are not an error."""
    for engine in ENGINES:
        sh(["docker", "rm", "-f", "-v", container(engine)], log)
        sh(["docker", "volume", "rm", "-f", f"{container(engine)}-data"], log)


def stand_up(run: Run) -> tuple[str, str]:
    """Start and seed PG and MySQL on this machine's 127.0.0.1 ports; PASS when both answer a login and hold the seed."""
    log = run.log("pilot-stand")
    stand_down(log)
    for engine in ENGINES:
        rc, text = sh(docker_run_argv(engine), log)
        if rc:
            return FAIL, f"{engine}: {first_failing_line(text)}"
        port = published_port(sh(["docker", "port", container(engine), f"{ENGINES[engine]['inner']}/tcp"], log)[1])
        if port is None:
            return FAIL, f"{engine}: docker published no 127.0.0.1 port"
        ENGINES[engine]["port"] = port
    for engine in ENGINES:
        deadline = time.monotonic() + 240
        while True:
            try:
                rows_of(engine, "SELECT 1", log)
                break
            except (RuntimeError, subprocess.TimeoutExpired):
                if time.monotonic() > deadline:
                    return FAIL, f"{engine} did not accept a TCP login in 240s"
                time.sleep(2)
        rc, text = sql(engine, create_sql(engine) + insert_sql(engine, 1, SEED_ROWS), log)
        if rc:
            return FAIL, f"{engine} seed: {first_failing_line(text)}"
        n = rows_of(engine, "SELECT COUNT(*) FROM pilot_items", log)
        if n != [str(SEED_ROWS)]:
            return FAIL, f"{engine} holds {n} rows after the seed, not {SEED_ROWS}"
    published = sh(["docker", "ps", "--filter", f"label={LABEL}", "--format", "{{.Ports}}"], log)[1]
    if "0.0.0.0" in published or "[::]" in published:
        return FAIL, f"a pilot container is published beyond 127.0.0.1: {published.strip()}"
    return PASS, f"postgres :{ENGINES['postgres']['port']} and mysql :{ENGINES['mysql']['port']} on 127.0.0.1, {SEED_ROWS} rows each"


def stand_clean(run: Run) -> tuple[str, str]:
    """Remove the pilot stand and confirm no labelled container or volume is left."""
    log = run.log("pilot-cleanup")
    stand_down(log)
    left = sh(["docker", "ps", "-a", "-q", "--filter", f"label={LABEL}"], log)[1].split() + sh(["docker", "volume", "ls", "-q", "--filter", f"name={PREFIX}-"], log)[1].split()
    return (PASS, "no pilot container or volume left") if not left else (FAIL, f"left behind: {' '.join(left)}")


def write_config(dirp: Path, engine: str, export: str, query: str, out: Path) -> None:
    """Write the incremental, parquet, local-destination config of one pilot export."""
    dirp.mkdir(parents=True, exist_ok=True)
    (dirp / "rivet.yaml").write_text(
        f"source:\n  type: {engine}\n  url_env: PILOT_SOURCE_URL\nexports:\n  - name: {export}\n    query: \"{query}\"\n"
        f"    mode: incremental\n    cursor_column: id\n    format: parquet\n    destination:\n      type: local\n      path: {out}\n"
    )


def rivet_env(engine: str) -> dict:
    """The environment of a rivet child: no inherited RIVET_*, the pilot source URL."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("RIVET_") and k != "DATABASE_URL"}
    env["PILOT_SOURCE_URL"] = source_url(engine)
    return env


def rivet_run(binary: Path, engine: str, cfg: Path, log: Path, extra: list[str] | None = None, timeout: int = 600) -> tuple[int, str]:
    """`rivet run -c rivet.yaml` in the config's directory."""
    return sh([str(binary), "run", "-c", "rivet.yaml", *(extra or [])], log, cwd=cfg, env=rivet_env(engine), timeout=timeout)


def read_destination(py: Path, out: Path) -> list[str]:
    """Every `id,payload` line in the parquet files under `out`, read by pyarrow."""
    p = subprocess.run([str(py), "-c", READER, str(out)], capture_output=True, text=True, timeout=300)
    if p.returncode:
        raise RuntimeError(f"reading the destination failed: {first_failing_line(p.stderr)}")
    return [ln for ln in p.stdout.splitlines() if ln]


def pilot_grade(source: list[str], dest: list[str], allowed_dup: set[str] | None = None) -> tuple[str, str]:
    """PASS when the destination holds every source row and nothing else, each once; `allowed_dup` ids may repeat."""
    allowed_dup = allowed_dup or set()
    want, got = Counter(source), Counter(dest)
    missing = sorted(want.keys() - got.keys(), key=lambda s: int(s.split(",")[0]))
    extra = sorted(got.keys() - want.keys(), key=lambda s: int(s.split(",")[0]))
    dups = sorted((r for r, n in got.items() if n > 1 and r.split(",")[0] not in allowed_dup), key=lambda s: int(s.split(",")[0]))
    if missing:
        return FAIL, f"gap: {len(missing)} source row(s) missing from the destination, first id {missing[0].split(',')[0]}"
    if extra:
        return FAIL, f"{len(extra)} destination row(s) are not in the source, first id {extra[0].split(',')[0]}"
    if dups:
        return FAIL, f"duplicates: {len(dups)} row(s) delivered more than once, first id {dups[0].split(',')[0]}"
    return PASS, f"{len(dest)} rows, {len(got)} distinct, equal to the source"


def newest_backup(cfg: Path) -> Path | None:
    """The newest pre-migration copy of the SQLite state beside `cfg`."""
    copies = sorted(cfg.glob(".rivet_state.db.v*.bak"))
    return copies[-1] if copies else None


def pilot_upgrade(engine: str, old: Path, new: Path, py: Path, work: Path, log: Path) -> tuple[str, str]:
    """OLD on a fresh state, NEW on the same state, OLD again: the delta lands once, and the way back loses nothing."""
    cfg, out = work / f"upgrade-{engine}", work / f"upgrade-{engine}" / "out"
    write_config(cfg, engine, "pilot_items", "SELECT id, grp, payload FROM pilot_items", out)

    def stage(label: str, binary: Path, allowed: set[str] | None = None, extra_ok: bool = False) -> tuple[str, str] | None:
        rc, text = rivet_run(binary, engine, cfg, log)
        if rc:
            return FAIL, f"[{label}] exit {rc}: {first_failing_line(text)}"
        verdict, line = pilot_grade(source_rows(engine, log), read_destination(py, out), allowed)
        return None if verdict == PASS else (FAIL, f"[{label}] {line}")

    if (bad := stage(f"{OLD_RELEASE} first run", old)):
        return bad
    sql(engine, insert_sql(engine, *DELTA_A), log)
    if (bad := stage("new binary continues the state", new)):
        return bad
    sql(engine, insert_sql(engine, *DELTA_B), log)
    rc, text = rivet_run(old, engine, cfg, log)
    way, allowed = "directly", set()
    if rc != 0 and "RIVET_STATE_SCHEMA_NEWER" in text:
        copy = newest_backup(cfg)
        if copy is None:
            return FAIL, f"[way back] {OLD_RELEASE} refuses the migrated state and no .bak copy exists"
        shutil.copy2(copy, cfg / ".rivet_state.db")
        for suffix in ("-wal", "-shm"):
            (cfg / f".rivet_state.db{suffix}").unlink(missing_ok=True)
        way, allowed = f"from {copy.name.split('.db.')[1]}", {str(i) for i in range(DELTA_A[0], DELTA_A[1] + 1)}
        rc, text = rivet_run(old, engine, cfg, log)
    if rc:
        return FAIL, f"[way back] {OLD_RELEASE} exit {rc}: {first_failing_line(text)}"
    verdict, line = pilot_grade(source_rows(engine, log), read_destination(py, out), allowed)
    if verdict != PASS:
        return FAIL, f"[way back] {line}"
    return PASS, f"{OLD_RELEASE} -> new -> {OLD_RELEASE} ({way}): {line}"


def state_running(cfg: Path) -> bool:
    """True while the SQLite state beside `cfg` holds a run_status row still running."""
    db = cfg / ".rivet_state.db"
    if not db.exists():
        return False
    try:
        with closing(sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=1)) as con:
            return con.execute("SELECT COUNT(*) FROM run_status WHERE status = 'running'").fetchone()[0] > 0
    except sqlite3.Error:
        return False


def pilot_lease(engine: str, new: Path, py: Path, work: Path, log: Path) -> tuple[str, str]:
    """A second run of an export started while the first holds the lease is refused (exit 2); the first still delivers once."""
    cfg, out = work / f"lease-{engine}", work / f"lease-{engine}" / "out"
    where = "pg_sleep(0.02) IS NOT NULL" if engine == "postgres" else "SLEEP(0.02) = 0"
    write_config(cfg, engine, "pilot_slow", f"SELECT id, grp, payload FROM pilot_items WHERE {where}", out)
    with (work / f"lease-{engine}-a.log").open("w") as fh:
        a = subprocess.Popen([str(new), "run", "-c", "rivet.yaml"], cwd=cfg, env=rivet_env(engine), stdout=fh, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 90
        while not state_running(cfg):
            if a.poll() is not None or time.monotonic() > deadline:
                return FAIL, f"the first run never reached the source (exit {a.poll()})"
            time.sleep(1)
        rc, text = rivet_run(new, engine, cfg, log, ["--lock-wait", "0"], timeout=120)
        if rc != 2 or "RIVET_STATE_RUN_IN_PROGRESS" not in text:
            return FAIL, f"the second run was not refused by the lease: exit {rc}: {first_failing_line(text)}"
        a.wait(timeout=300)
    finally:
        if a.poll() is None:
            a.terminate()
    if a.returncode != 0:
        return FAIL, f"the first run exited {a.returncode}: {first_failing_line((work / f'lease-{engine}-a.log').read_text())}"
    source = source_rows(engine, log)
    verdict, line = pilot_grade(source, read_destination(py, out))
    return (PASS, f"second run refused (exit 2, RIVET_STATE_RUN_IN_PROGRESS); {line}") if verdict == PASS else (FAIL, line)


def run_linux(args: argparse.Namespace) -> int:
    """The Linux profile: checkout, release build, pilot scenario, offline suite."""
    out = Path(args.out).expanduser().resolve()
    repo = Path(args.repo).expanduser().resolve()
    cache = out.parent / "cache" if not args.cache else Path(args.cache).expanduser().resolve()
    run = Run("linux", args.sha, out)
    if run.step("checkout", lambda: checkout(run, repo, args.sha, args.url)).status != PASS:
        run.add("build-release", NOT_RUN, "no checkout of the sha")
    else:
        run.step("build-release", lambda: build_release(run, repo, args.sha))
    built = run.checks[-1].status == PASS
    new = Path(args.new_bin).expanduser().resolve() if args.new_bin else target_dir() / "release" / "rivet"
    state: dict = {}

    def locked_python() -> tuple[str, str]:
        """Build the checkout's locked python environment."""
        state["py"] = repo_python(run, repo, cache)
        return PASS, f"{state['py'].parent.parent.name} with pyarrow, python 3.12"

    if run.checks[0].status == PASS:
        run.step("locked-python", locked_python)
    else:
        run.add("locked-python", NOT_RUN, "no checkout of the sha")

    if not built or "py" not in state:
        for name in ("pilot-download", "pilot-stand", "pilot-upgrade[postgres]", "pilot-upgrade[mysql]", "pilot-lease[postgres]", "pilot-lease[mysql]"):
            run.add(name, NOT_RUN, "build-release or locked-python did not pass: nothing to continue the state with")
    else:
        old = cache / f"rivet-{OLD_RELEASE}"
        if run.step("pilot-download", lambda: fetch_old_release(run, cache)).status != PASS:
            for name in ("pilot-stand", "pilot-upgrade[postgres]", "pilot-upgrade[mysql]", "pilot-lease[postgres]", "pilot-lease[mysql]"):
                run.add(name, NOT_RUN, "pilot-download did not pass")
        else:
            work = out / "work"
            shutil.rmtree(work, ignore_errors=True)
            try:
                if run.step("pilot-stand", lambda: stand_up(run)).status == PASS:
                    for engine in ENGINES:
                        run.step(f"pilot-upgrade[{engine}]", lambda e=engine: pilot_upgrade(e, old, new, state["py"], work, run.log(f"pilot-upgrade-{e}")))
                    for engine in ENGINES:
                        run.step(f"pilot-lease[{engine}]", lambda e=engine: pilot_lease(e, new, state["py"], work, run.log(f"pilot-lease-{e}")))
                else:
                    for name in ("pilot-upgrade[postgres]", "pilot-upgrade[mysql]", "pilot-lease[postgres]", "pilot-lease[mysql]"):
                        run.add(name, NOT_RUN, "pilot-stand did not pass")
            finally:
                run.step("pilot-cleanup", lambda: stand_clean(run))
    if run.checks[0].status == PASS and "py" in state:
        run.step("offline-suite", lambda: offline_suite(run, repo, state.get("py")))
    else:
        run.add("offline-suite", NOT_RUN, "no checkout of the sha or no locked python")
    return finish(run, args)


def finish(run: Run, args: argparse.Namespace) -> int:
    """Print the verdict, publish when asked, return the exit code."""
    say(json.dumps(json.loads((run.out / "verdict.json").read_text()), indent=2))
    if getattr(args, "publish", False):
        publish(run.out, run.sha, args.label or run.profile)
    return run.exit_code()


def tool_check(repo: Path, argv: list[str], log: Path, label: str) -> tuple[str, str]:
    """Run a repo script that exits 0 PASS, 1 FAIL, 2 NOT RUN, as pr-ready runs it."""
    rc, text = sh(argv, log, cwd=repo)
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.startswith("[exit")]
    tail = first_failing_line(text) if rc == 1 else (lines[-1][:300] if lines else "no output")
    return {0: (PASS, f"{label}: clean"), 1: (FAIL, tail), 2: (NOT_RUN, tail)}.get(rc, (FAIL, f"{label} exit {rc}: {tail}"))


def unreleased_entries(changelog: str) -> int:
    """The number of entries under `## Unreleased` of a CHANGELOG text."""
    m = re.search(r"^## Unreleased[ \t]*\n(.*?)(?=^## )", changelog, re.M | re.S)
    return len(re.findall(r"^- \*\*", m.group(1), re.M)) if m else 0


def notes_assemble(run: Run, repo: Path) -> tuple[str, str]:
    """`changelog.py assemble` on a clone: every fragment entry lands under `## Unreleased` and no fragment is left."""
    log = run.log("release-notes-assemble")
    fragments = [p for p in sorted((repo / "changelog.d").glob("*.md")) if p.name != "README.md"]
    if not fragments:
        return NOT_RUN, "changelog.d holds no fragment: nothing to assemble"
    want = sum(len(re.findall(r"^- \*\*", p.read_text(), re.M)) for p in fragments)
    with tempfile.TemporaryDirectory(prefix="satellite-notes-") as tmp:
        clone = Path(tmp) / "clone"
        rc, text = sh(["git", "clone", "-q", "--no-hardlinks", str(repo), str(clone)], log)
        if rc:
            return FAIL, f"cannot clone for the dry run: {first_failing_line(text)}"
        before = unreleased_entries((clone / "CHANGELOG.md").read_text())
        rc, text = sh([sys.executable, "dev/pytools/changelog.py", "assemble"], log, cwd=clone)
        if rc:
            return FAIL, f"assemble exit {rc}: {first_failing_line(text)}"
        got = unreleased_entries((clone / "CHANGELOG.md").read_text()) - before
        left = [p.name for p in (clone / "changelog.d").glob("*.md") if p.name != "README.md"]
    if left:
        return FAIL, f"{len(left)} fragment(s) left after assemble: {', '.join(left[:3])}"
    if got != want:
        return FAIL, f"{want} fragment entries but {got} landed under `## Unreleased`"
    return PASS, f"{len(fragments)} fragments, {want} entries assembled under `## Unreleased`"


def known_red_expiry(run: Run, repo: Path, release_date: str | None) -> tuple[str, str]:
    """Every known-red entry stays in date through the planned release day."""
    if not release_date:
        return NOT_RUN, "no --release-date given: the expiry has nothing to be compared with"
    code = "import json; from dev.release_oracle import known_red as k; print(json.dumps([[e.match[:80], e.expires] for e in k.KNOWN_RED]))"
    rc, text = sh([sys.executable, "-c", code], run.log("known-red-expiry"), cwd=repo)
    if rc:
        return FAIL, f"cannot read known_red.py: {first_failing_line(text)}"
    return expiry_verdict(json.loads([ln for ln in text.splitlines() if ln.strip()][-1]), release_date)


def expiry_verdict(entries: list[list[str]], release_date: str) -> tuple[str, str]:
    """FAIL when an entry's expiry is before `release_date` (an entry is in date through its expiry day)."""
    day = dt.date.fromisoformat(release_date)
    late = [(name, exp) for name, exp in entries if dt.date.fromisoformat(exp) < day]
    if late:
        return FAIL, f"{len(late)} known-red entr(ies) expire before {release_date}: {late[0][0]} ({late[0][1]})"
    return PASS, f"{len(entries)} entries, the earliest expiry {min((e for _, e in entries), default='-')} is on or after {release_date}"


def nextest_verdict(text: str, skips: str) -> tuple[str, str]:
    """PASS only for a Summary of one passed test and an empty skip log; a skip is NOT-RUN, never a pass."""
    if skips.strip():
        return NOT_RUN, skips.strip().splitlines()[0][:300]
    m = re.search(r"Summary \[.*?\] +(\d+) tests? run: (\d+) passed", text)
    if m and int(m.group(1)) > 0 and m.group(1) == m.group(2) and "failed" not in text.split("Summary", 1)[-1].split("\n", 1)[0]:
        return PASS, f"{m.group(0).split('] ', 1)[-1]}"
    return FAIL, first_failing_line(text)


def stand_pg_alive(host: str = "127.0.0.1", port: int = STAND_PG_PORT) -> bool:
    """True when a Postgres answers the SSL request on host:port, not merely accepts the connection."""
    try:
        with socket.create_connection((host, port), timeout=5) as c:
            c.sendall(b"\x00\x00\x00\x08\x04\xd2\x16\x2f")
            return c.recv(1) in (b"S", b"N")
    except OSError:
        return False


def retried(runs: list[tuple[str, str]]) -> tuple[str, str, list[str]]:
    """The verdict over every attempt: a FAIL in any attempt is kept (FAIL when all failed, else FLAKY) with the first failure's line."""
    attempts = [status for status, _ in runs]
    first_fail = next((line for status, line in runs if status == FAIL), None)
    if first_fail is None:
        return runs[-1][0], runs[-1][1], attempts
    return (FAIL if all(a == FAIL for a in attempts) else FLAKY), first_fail, attempts


def cloud_cell(run: Run, repo: Path, name: str) -> tuple[str, str, list[str]]:
    """Run one existing live cell with the warehouse env filled by warehouse_env; re-run once when it fails and keep the failure."""
    warehouse, cell = CLOUD_CELLS[name]
    if not stand_pg_alive():
        return NOT_RUN, f"the stand postgres on 127.0.0.1:{STAND_PG_PORT} does not answer, the cell reads its source from it", []
    runs: list[tuple[str, str]] = []
    for attempt in (1, 2):
        skips = run.out / "logs" / f"{name}.skips"
        skips.unlink(missing_ok=True)
        env = {**os.environ, "RIVET_WAREHOUSE_LIVE": warehouse, "RIVET_SKIP_LOG": str(skips),
               "CARGO_TARGET_DIR": str(repo / "target")}
        argv = ["uv", "run", "--frozen", "python", "-m", "dev.pytools.warehouse_env", "--", cargo_bin(), "nextest", "run", "--no-fail-fast",
                "--test", "live_suite", "--run-ignored", "only", "-E", f"test(={cell})"]
        rc, text = sh(argv, run.log(name), cwd=repo, env=env, timeout=1800)
        runs.append(nextest_verdict(text, skips.read_text() if skips.exists() else ""))
        if runs[-1][0] != FAIL:
            break
    return retried(runs)


def run_mac(args: argparse.Namespace) -> int:
    """The Mac profile: release-notes assembly, lychee, agnix, gitleaks, known-red expiry, a BigQuery and a Snowflake load."""
    out, repo = Path(args.out).expanduser().resolve(), Path(args.repo).expanduser().resolve()
    run = Run("mac", args.sha, out)
    if run.step("checkout", lambda: head_is(repo, args.sha, run.log("checkout"))).status != PASS:
        return finish(run, args)
    run.step("release-notes-assemble", lambda: notes_assemble(run, repo))
    run.step("lychee", lambda: tool_check(repo, [sys.executable, "dev/pytools/lint_tools.py", "run", "lychee"], run.log("lychee"), "lychee"))
    run.step("agnix", lambda: tool_check(repo, [sys.executable, "dev/pytools/lint_tools.py", "run", "agnix"], run.log("agnix"), "agnix"))
    run.step("gitleaks", lambda: tool_check(repo, [sys.executable, "dev/pytools/secrets_scan.py", "tree"], run.log("gitleaks"), "gitleaks"))
    run.step("known-red-expiry", lambda: known_red_expiry(run, repo, args.release_date))
    for name in CLOUD_CELLS:
        t = time.monotonic()
        try:
            status, line, attempts = cloud_cell(run, repo, name)
        except Exception as e:
            status, line, attempts = FAIL, f"{type(e).__name__}: {e}", [FAIL]
        run.add(name, status, line, time.monotonic() - t, attempts)
    return finish(run, args)


def ssh_argv(host: str, remote: str) -> list[str]:
    """`ssh HOST REMOTE` without prompts."""
    return ["ssh", "-o", "BatchMode=yes", host, remote]


def gate_target(sha: str, label: str) -> tuple[str, str]:
    """(ssh host, directory) where a result lands beside the gate's logs, from the environment."""
    host = os.environ.get("RIVET_GATE_LOGS_HOST", "")
    base = os.environ.get("RIVET_GATE_LOGS_DIR", "wt/gate-logs")
    if not host:
        raise SystemExit("RIVET_GATE_LOGS_HOST is not set: no place to publish the result")
    return host, f"{base}/{sha}/satellite-{label}"


def publish(directory: Path, sha: str, label: str) -> None:
    """Copy `directory` to the gate host under `<logs>/<sha>/satellite-<label>/`."""
    host, dest = gate_target(sha, label)
    subprocess.run(ssh_argv(host, f"mkdir -p {shlex.quote(dest)}"), check=True)
    subprocess.run(["scp", "-q", "-r", "-o", "BatchMode=yes", f"{directory}/.", f"{host}:{dest}/"], check=True)
    say(f"satellite: published to {dest} on the gate host")


def run_remote(args: argparse.Namespace) -> int:
    """Ship this file to the satellite host, run the Linux profile there, fetch the result, publish it."""
    host = args.host or os.environ.get("RIVET_SATELLITE_HOST", "")
    if not host:
        raise SystemExit("RIVET_SATELLITE_HOST is not set (an ssh alias of the Linux satellite)")
    sha = subprocess.run(["git", "rev-parse", "--verify", f"{args.sha}^{{commit}}"], capture_output=True, text=True).stdout.strip() or args.sha
    home = "satellite"
    subprocess.run(ssh_argv(host, f"mkdir -p {home}/out"), check=True)
    subprocess.run(["scp", "-q", "-o", "BatchMode=yes", str(Path(__file__).resolve()), f"{host}:{home}/satellite.py"], check=True)
    cmd = f"python3 {home}/satellite.py linux --sha {sha} --repo {home}/repo --out {home}/out/{sha} --cache {home}/cache"
    if args.new_bin:
        cmd += f" --new-bin {shlex.quote(args.new_bin)}"
    rc = subprocess.run(ssh_argv(host, cmd)).returncode
    local = Path(args.out).expanduser().resolve() if args.out else Path(tempfile.mkdtemp(prefix="satellite-"))
    local.mkdir(parents=True, exist_ok=True)
    subprocess.run(["scp", "-q", "-r", "-o", "BatchMode=yes", f"{host}:{home}/out/{sha}/verdict.json", f"{host}:{home}/out/{sha}/logs", str(local)], check=True)
    say(f"satellite: result copied to {local}")
    if args.publish:
        publish(local, sha, args.label or "linux")
    return rc


def fake_pg_port(reply: bytes | None) -> int:
    """Port of a one-shot listener that answers `reply` (closes at once when empty); a closed port when None."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    port = srv.getsockname()[1]
    if reply is None:
        srv.close()
        return port
    srv.listen(1)

    def serve() -> None:
        c, _ = srv.accept()
        c.recv(8)
        if reply:
            c.sendall(reply)
        c.close()
        srv.close()

    threading.Thread(target=serve, daemon=True).start()
    return port


def self_test() -> int:
    """Each rule goes RED on the input it exists for and stays GREEN on a sound one."""
    src = [f"{i},p{i}" for i in range(1, 11)]
    assert pilot_grade(src, list(src))[0] == PASS
    assert pilot_grade(src, src[:8])[0] == FAIL, "a dropped delta must fail"
    assert "gap" in pilot_grade(src, src[:8])[1]
    assert pilot_grade(src, src + src[7:])[0] == FAIL, "a duplicated delta must fail"
    assert "duplicates" in pilot_grade(src, src + src[7:])[1]
    assert pilot_grade(src, src + src[8:], {"9", "10"})[0] == PASS, "a documented re-delivery is allowed only for its ids"
    assert pilot_grade(src, src + src[8:], {"9"})[0] == FAIL, "a duplicate outside the allowed ids must fail"
    assert pilot_grade(src, src + ["99,x"])[0] == FAIL, "a row the source never held must fail"
    assert pilot_grade(src, [])[0] == FAIL, "an empty destination is not equal to a full source"
    assert overall([]) == NOT_RUN, "no check at all is not a pass"
    assert overall([Check("a", PASS, "", 0), Check("b", NOT_RUN, "", 0)]) == NOT_RUN, "a NOT-RUN never counts as a PASS"
    assert overall([Check("a", FAIL, "", 0), Check("b", NOT_RUN, "", 0)]) == FAIL
    assert overall([Check("a", PASS, "", 0)]) == PASS
    assert blocks(Check("pilot-upgrade[postgres]", FAIL, "x", 0)), "a red pilot blocks"
    assert blocks(Check("pilot-lease[mysql]", NOT_RUN, "x", 0)), "an unrun pilot blocks"
    assert blocks(Check("release-notes-assemble", FAIL, "x", 0)), "a false release note blocks"
    assert blocks(Check("cloud-bigquery", FAIL, "x", 0, [FAIL])), "one red cloud smoke blocks"
    assert blocks(Check("cloud-bigquery", FAIL, "x", 0, [FAIL, FAIL])), "a repeated red cloud smoke blocks"
    assert blocks(Check("cloud-bigquery", FLAKY, "x", 0, [FAIL, PASS])), "a cloud smoke that failed once and then passed blocks"
    assert blocks(Check("cloud-bigquery", NOT_RUN, "x", 0, [NOT_RUN])) is None, "a cloud smoke that did not run is reported"
    assert overall([Check("a", FLAKY, "x", 0, [FAIL, PASS])]) == FAIL, "a flaky check is not a pass"
    losing = [(1, "thread 't' panicked at x: BigQuery holds 9 rows, the source 10\n     Summary [ 1.0s] 1 test run: 0 passed, 1 failed"),
              (0, "     Summary [   1.2s] 1 test run: 1 passed, 5 skipped")]
    with tempfile.TemporaryDirectory() as tmp, mock.patch.object(sys.modules[__name__], "stand_pg_alive", lambda: True), \
            mock.patch.object(sys.modules[__name__], "sh", lambda *a, **k: losing.pop(0)):
        status, line, attempts = cloud_cell(Run("mac", "abc1234", Path(tmp)), Path(tmp), "cloud-bigquery")
    assert (status, attempts) == (FLAKY, [FAIL, PASS]) and "BigQuery holds 9 rows" in line, (status, line, attempts)
    assert not losing, "the cell is re-run once after a FAIL"
    again = [(1, "thread 't' panicked at x: first loss"), (1, "thread 't' panicked at x: second loss")]
    with tempfile.TemporaryDirectory() as tmp, mock.patch.object(sys.modules[__name__], "stand_pg_alive", lambda: True), \
            mock.patch.object(sys.modules[__name__], "sh", lambda *a, **k: again.pop(0)):
        status, line, attempts = cloud_cell(Run("mac", "abc1234", Path(tmp)), Path(tmp), "cloud-bigquery")
    assert (status, attempts) == (FAIL, [FAIL, FAIL]) and "first loss" in line, "the first failure's line is the evidence"
    assert blocks(Check("offline-suite", FAIL, "x", 0)) is None, "other checks are reported, not blocking"
    assert blocks(Check("pilot-stand", PASS, "ok", 0)) is None
    for engine in ENGINES:
        argv = docker_run_argv(engine)
        ports = [argv[i + 1] for i, a in enumerate(argv) if a == "-p"]
        assert ports and all(p.startswith("127.0.0.1:") for p in ports), "a pilot port must be published on 127.0.0.1 only"
        assert LABEL in argv
    assert published_port("127.0.0.1:49153\n[exit 0]") == 49153
    assert published_port("0.0.0.0:49153\n") is None and published_port("") is None, "only a 127.0.0.1 binding is a pilot port"
    assert expiry_verdict([["a", "2026-11-15"]], "2026-11-09")[0] == PASS
    assert expiry_verdict([["a", "2026-11-09"]], "2026-11-09")[0] == PASS, "an entry is in date through its expiry day"
    assert expiry_verdict([["a", "2026-11-08"]], "2026-11-09")[0] == FAIL, "an expired entry fails"
    one = "     Summary [   1.2s] 1 test run: 1 passed, 5 skipped"
    assert nextest_verdict(one, "")[0] == PASS
    with tempfile.TemporaryDirectory() as tmp:
        fake = Path(tmp) / "rivet"
        fake.write_text("#!/bin/sh\necho 'rivet 0.32.2 (aaaaaaa11)'\n")
        fake.chmod(0o755)
        assert built_verdict(fake, "aaaaaaa1" + "0" * 32, 0, 1.0, "", Path(tmp) / "v.log")[0] == PASS
        assert built_verdict(fake, "bbbbbbb2" + "0" * 32, 0, 1.0, "", Path(tmp) / "v.log")[0] == FAIL, "a binary of another sha is stale"
        assert built_verdict(Path(tmp) / "absent", "aaaaaaa1" + "0" * 32, 0, 1.0, "", Path(tmp) / "v.log")[0] == FAIL
    assert not stand_pg_alive("127.0.0.1", fake_pg_port(None)), "a closed port is not a live postgres"
    for reply, alive in ((b"N", True), (b"S", True), (b"", False)):
        assert stand_pg_alive("127.0.0.1", fake_pg_port(reply)) is alive, f"reply {reply!r}"
    assert nextest_verdict(one, "RIVET-SKIP t - no creds\n")[0] == NOT_RUN, "a skipped cell is not a pass"
    assert nextest_verdict("     Summary [   1.2s] 1 test run: 0 passed, 1 failed", "")[0] == FAIL
    assert nextest_verdict("     Summary [   0.0s] 0 tests run: 0 passed", "")[0] == FAIL, "zero tests run proves nothing"
    assert unreleased_entries("# c\n\n## Unreleased\n\n- **a** x\n\n- **b** y\n\n## 0.1.0 - d\n\n- **old**\n") == 2
    assert first_failing_line("fine\nError: boom\nlast") == "Error: boom"
    assert first_failing_line("a\nb") == "b"
    home = str(Path.home())
    assert home not in scrub(f"x {home}/y"), "the home directory must not reach a verdict"
    with tempfile.TemporaryDirectory() as tmp:
        run = Run("linux", "abc1234", Path(tmp))
        run.add("pilot-stand", FAIL, "boom", 1.0)
        body = json.loads((Path(tmp) / "verdict.json").read_text())
        assert body["overall"] == FAIL and body["blocking"] and body["checks"][0]["line"] == "boom"
        assert run.exit_code() == 1
        run.step("raises", lambda: 1 / 0)
        assert run.checks[-1].status == FAIL and "ZeroDivisionError" in run.checks[-1].line, "an exception is a FAIL, never a pass"
    print("satellite self-test: ok")
    return 0


def parser() -> argparse.ArgumentParser:
    """The command line."""
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd")
    for name in ("linux", "mac", "remote", "publish"):
        s = sub.add_parser(name)
        s.add_argument("--sha", required=True)
        s.add_argument("--label")
        s.add_argument("--publish", action="store_true")
        if name in ("linux", "mac"):
            s.add_argument("--repo", default=str(ROOT))
            s.add_argument("--out", default=None)
        if name == "linux":
            s.add_argument("--new-bin")
            s.add_argument("--cache")
            s.add_argument("--url", default="https://github.com/rivet-lab/rivet.git")
        if name == "mac":
            s.add_argument("--release-date")
        if name == "remote":
            s.add_argument("--host")
            s.add_argument("--out")
            s.add_argument("--new-bin")
        if name == "publish":
            s.add_argument("--dir", required=True)
    return p


def main(argv: list[str]) -> int:
    """Dispatch a subcommand."""
    if argv == ["--self-test"]:
        return self_test()
    args = parser().parse_args(argv)
    if args.cmd in ("linux", "mac") and not args.out:
        args.out = f"satellite-out/{args.cmd}-{args.sha[:12]}"
    if args.cmd == "linux":
        return run_linux(args)
    if args.cmd == "mac":
        return run_mac(args)
    if args.cmd == "remote":
        return run_remote(args)
    if args.cmd == "publish":
        publish(Path(args.dir).resolve(), args.sha, args.label or "linux")
        return 0
    parser().print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
