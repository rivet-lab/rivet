#!/usr/bin/env python3
"""Start a compose stack in CI: pull its images with a bounded retry on registry and network errors, then run the `up`.

    compose_up.py [--attempts N] [--pause S] [--parallel N] docker compose [--profile P ...] up [-d] [--wait] [SERVICE ...]
    compose_up.py --self-test

The images of the named services (and their dependencies) are pulled first, at most `--parallel` at a time.
A pull that fails with a registry or network error (`RETRYABLE`) is retried after a growing pause, up to
`--attempts` tries; any other failure, and the last retryable one, fails at once with the pull's exit code and
no `up`. The `up` itself then runs once, unchanged, and its exit code is this script's.
"""

from __future__ import annotations

import os
import re
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from pathlib import Path

ATTEMPTS = 5
PAUSE_S = 15
PARALLEL = 3
UP_FLAGS = ("-d", "--detach", "--wait")
RETRYABLE = re.compile(
    r"too many connections|error from registry|toomanyrequests|too many requests|"
    r"tls handshake timeout|i/o timeout|connection reset|connection refused|unexpected eof|"
    r"temporary failure in name resolution|no such host|context deadline exceeded|"
    r"received unexpected http status: 5\d\d|50[0234] (?:bad gateway|service unavailable|gateway time-?out)|"
    r"internal server error|net/http: request canceled",
    re.I,
)


def split_up(argv: Sequence[str]) -> tuple[list[str], list[str]]:
    """The compose arguments before `up` and the service names after it; an `up` flag this tool does not read is refused."""
    if list(argv[:2]) != ["docker", "compose"] or "up" not in argv:
        raise SystemExit(f"compose_up: expected `docker compose ... up ...`, got `{' '.join(argv)}`")
    at = list(argv).index("up")
    services = []
    for word in argv[at + 1 :]:
        if word.startswith("-") and word not in UP_FLAGS:
            raise SystemExit(f"compose_up: `up` flag {word} is not one this tool reads; services would be misread")
        if not word.startswith("-"):
            services.append(word)
    return list(argv[:at]), services


def run_streamed(argv: Sequence[str]) -> tuple[int, str]:
    """Run `argv`, echo its merged output as it arrives, return (exit code, the output)."""
    proc = subprocess.Popen(list(argv), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace")
    assert proc.stdout is not None
    lines = []
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        lines.append(line)
    return proc.wait(), "".join(lines)


def pull(
    pre: Sequence[str],
    services: Sequence[str],
    attempts: int,
    pause_s: float,
    parallel: int,
    runner: Callable[[Sequence[str]], tuple[int, str]] = run_streamed,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Pull the images, retrying only a registry or network error; the exit code of the last attempt."""
    cmd = [*pre[:2], "--parallel", str(parallel), *pre[2:], "pull", "--include-deps", *services]
    rc = 1
    for attempt in range(1, attempts + 1):
        rc, out = runner(cmd)
        if rc == 0:
            return 0
        if not RETRYABLE.search(out):
            print(f"compose_up: pull failed with a non-registry error (exit {rc}); not retried", file=sys.stderr)
            return rc
        if attempt < attempts:
            wait = pause_s * attempt
            print(f"compose_up: registry or network error on pull, attempt {attempt}/{attempts}; retrying in {wait:g}s", file=sys.stderr)
            sleep(wait)
    print(f"compose_up: pull still failing after {attempts} attempts (exit {rc})", file=sys.stderr)
    return rc


def main(argv: Sequence[str]) -> int:
    """Pull with retry, then run the `up` given."""
    opts = {"--attempts": ATTEMPTS, "--pause": PAUSE_S, "--parallel": PARALLEL}
    rest = list(argv)
    while rest and rest[0] in opts:
        if len(rest) < 2:
            raise SystemExit(f"compose_up: {rest[0]} needs a value")
        opts[rest[0]] = float(rest[1]) if rest[0] == "--pause" else int(rest[1])
        rest = rest[2:]
    pre, services = split_up(rest)
    rc = pull(pre, services, int(opts["--attempts"]), float(opts["--pause"]), int(opts["--parallel"]))
    if rc:
        return rc
    return subprocess.run(rest, check=False).returncode


FAKE_DOCKER = """#!/bin/sh
echo "$@" >> "$FAKE_LOG"
case "$*" in
  *" pull "*)
    n=$(wc -l < "$FAKE_LOG" | tr -d ' ')
    if [ "$n" -le "$FAKE_FAILS" ]; then echo "$FAKE_ERR" >&2; exit 1; fi
    exit 0 ;;
  *) exit "${FAKE_UP_RC:-0}" ;;
esac
"""


def drive(fails: int, err: str, extra: Sequence[str] = (), up_rc: int = 0) -> tuple[int, list[str]]:
    """Run this script on a fake `docker` that fails `fails` pulls with `err`; (exit code, the fake's call log)."""
    with tempfile.TemporaryDirectory() as tmp:
        bindir = Path(tmp)
        fake = bindir / "docker"
        fake.write_text(FAKE_DOCKER)
        fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
        log = bindir / "calls.log"
        log.write_text("")
        env = {**os.environ, "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}", "FAKE_LOG": str(log), "FAKE_FAILS": str(fails), "FAKE_ERR": err, "FAKE_UP_RC": str(up_rc)}
        cmd = [sys.executable, "-m", "dev.pytools.compose_up", "--pause", "0", *extra, "docker", "compose", "--profile", "cdc", "up", "-d", "--wait", "pg", "my"]
        done = subprocess.run(cmd, env=env, cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True, check=False)
        return done.returncode, log.read_text().splitlines()


def self_test() -> int:
    """Grade the split, the retry classification, the attempt bound, and the order of pull and up on a fake docker."""
    pre, services = split_up(["docker", "compose", "--profile", "cdc", "up", "-d", "--wait", "pg", "my"])
    assert pre == ["docker", "compose", "--profile", "cdc"] and services == ["pg", "my"], (pre, services)
    for bad in (["docker", "compose", "up", "--timeout", "5", "pg"], ["docker", "compose", "ps"], ["compose", "up"]):
        try:
            split_up(bad)
        except SystemExit:
            continue
        raise AssertionError(f"a command this tool cannot read was accepted: {bad}")
    assert RETRYABLE.search("error from registry: rpc error: code = Unknown desc = Error 1040: Too many connections")
    assert RETRYABLE.search("Get https://registry-1.docker.io/v2/: net/http: TLS handshake timeout")
    assert not RETRYABLE.search("pull access denied for rivet, repository does not exist or may require authorization")
    assert not RETRYABLE.search("no such service: nope")

    registry = "error from registry: Error 1040: Too many connections"
    rc, calls = drive(2, registry)
    assert rc == 0, f"two registry failures then success did not end green (exit {rc})"
    pulls = [c for c in calls if " pull " in f" {c} "]
    assert len(pulls) == 3 and calls[-1].startswith("compose") and " up " in f" {calls[-1]} ", f"pull x3 then one up expected: {calls}"
    assert all("--parallel 3" in c and "--include-deps pg my" in c for c in pulls), pulls
    assert calls[-1] == "compose --profile cdc up -d --wait pg my", f"the up was changed: {calls[-1]}"

    rc, calls = drive(99, registry)
    assert rc == 1 and len([c for c in calls if " pull " in f" {c} "]) == ATTEMPTS and not any(" up " in f" {c} " for c in calls), (rc, calls)
    rc, calls = drive(99, registry, ["--attempts", "2"])
    assert rc == 1 and len(calls) == 2, f"--attempts 2 made {len(calls)} calls"

    rc, calls = drive(99, "pull access denied for rivet, repository does not exist")
    assert rc == 1 and len(calls) == 1, f"a non-registry failure was retried or masked: {calls}"

    rc, calls = drive(0, "", up_rc=7)
    assert rc == 7 and len(calls) == 2, f"the up's own exit code was lost (exit {rc})"
    print("compose_up self-test ok: retry then green, attempts bound, non-registry error not retried, up code kept, unreadable up refused")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if sys.argv[1:] == ["--self-test"] else main(sys.argv[1:]))
