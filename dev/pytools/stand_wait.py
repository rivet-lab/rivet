#!/usr/bin/env python3
"""Wait until every started service of the docker stand answers, or fail naming the one that did not.

    stand_wait.py [SERVICE ...]   wait for the named compose services, or for every one compose has started
    stand_wait.py --self-test

Readiness is read from docker, not listed here: a service with a healthcheck is ready when docker calls it
healthy and its budget is that healthcheck's own (start period + retries x interval); one without is ready when
its first published TCP port accepts a connection, within `PORT_BUDGET_S`; one with neither is reported as not
probed. A container that exited fails at once. The failure prints `docker compose ps -a` and the log tail of
each service that did not answer, and exits 1.
"""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

PORT_BUDGET_S = 60
POLL_S = 2
MARGIN_S = 60
LOG_TAIL = 40


@dataclass(frozen=True)
class Service:
    """One started container as docker reports it: what to probe and for how long."""

    name: str
    state: str
    health: str | None
    budget_s: float
    port: int | None
    exit_code: int = 0


def run(argv: Sequence[str]) -> str:
    """The stdout of `argv`; a failing command raises with its stderr."""
    done = subprocess.run(list(argv), capture_output=True, text=True, check=False)
    if done.returncode:
        raise SystemExit(f"stand_wait: `{' '.join(argv)}` exited {done.returncode}: {done.stderr.strip()}")
    return done.stdout


def inspect_services(runner: Callable[[Sequence[str]], str], names: Sequence[str]) -> list[Service]:
    """The started services named in `names` (all when empty) with the readiness docker declares for each."""
    ids = runner(["docker", "compose", "ps", "-aq", *names]).split()
    if not ids:
        return []
    out = []
    for box in json.loads(runner(["docker", "inspect", *ids])):
        labels = box["Config"].get("Labels") or {}
        hc = box["Config"].get("Healthcheck") or {}
        health = (box["State"].get("Health") or {}).get("Status") if hc.get("Test") and hc["Test"] != ["NONE"] else None
        budget = (hc.get("StartPeriod", 0) + hc.get("Retries", 3) * max(hc.get("Interval", 30_000_000_000), hc.get("Timeout", 0))) / 1e9 + MARGIN_S if health else PORT_BUDGET_S
        published = sorted(int(b["HostPort"]) for v in (box["NetworkSettings"].get("Ports") or {}).values() for b in (v or []) if b.get("HostPort"))
        out.append(Service(labels.get("com.docker.compose.service") or box["Name"].lstrip("/"), box["State"]["Status"], health, budget,
                           None if health or not published else published[0], box["State"].get("ExitCode", 0)))
    return sorted(out, key=lambda s: s.name)


def tcp_open(port: int) -> bool:
    """Whether 127.0.0.1:`port` accepts a connection."""
    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def verdict(svc: Service, probe: Callable[[int], bool]) -> str:
    """`ready`, `waiting` or `failed` for one service from its container state and its readiness probe."""
    if svc.state in ("exited", "dead"):
        return "ready" if svc.state == "exited" and svc.exit_code == 0 else "failed"
    if svc.health is not None:
        return {"healthy": "ready", "unhealthy": "failed"}.get(svc.health, "waiting")
    if svc.state != "running":
        return "waiting"
    return "ready" if svc.port is None or probe(svc.port) else "waiting"


def wait(names: Sequence[str] = (), *, runner: Callable[[Sequence[str]], str] = run, probe: Callable[[int], bool] = tcp_open,
         clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep, say: Callable[[str], None] = print) -> list[str]:
    """The names of the services that did not answer within their budgets (empty when all did)."""
    start = clock()
    pending = inspect_services(runner, names)
    if not pending:
        say("stand_wait: no started service to wait for")
        return []
    budgets = {s.name: s.budget_s for s in pending}
    say(f"stand_wait: waiting for {len(pending)} service(s): {', '.join(s.name for s in pending)}")
    failed: list[str] = []
    while pending:
        still = []
        for svc in pending:
            v = verdict(svc, probe)
            if v == "ready":
                say(f"  ready   {svc.name} ({clock() - start:.0f}s)")
            elif v == "failed":
                say(f"  FAILED  {svc.name}: container {svc.state}, health {svc.health}")
                failed.append(svc.name)
            elif clock() - start > budgets[svc.name]:
                say(f"  FAILED  {svc.name}: not ready within {budgets[svc.name]:.0f}s")
                failed.append(svc.name)
            else:
                still.append(svc.name)
        if not still:
            break
        sleep(POLL_S)
        pending = inspect_services(runner, still)
        for gone in sorted(set(still) - {s.name for s in pending}):
            say(f"  FAILED  {gone}: the container is gone")
            failed.append(gone)
    return failed


def main(argv: Sequence[str]) -> int:
    """Wait; on failure show the stand and the log tail of every service that did not answer."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("services", nargs="*")
    ap.add_argument("--self-test", action="store_true")
    ns = ap.parse_args(argv)
    if ns.self_test:
        return self_test()
    failed = wait(ns.services)
    if failed:
        print(f"::error::stand_wait: {', '.join(failed)} did not become ready", flush=True)
        print(run(["docker", "compose", "ps", "-a"]))
        for name in failed:
            print(f"--- {name} ---\n{run(['docker', 'compose', 'logs', '--tail', str(LOG_TAIL), name])}")
        return 1
    return 0


def _box(name: str, state: str = "running", health: str | None = None, hc: bool = True, ports: dict | None = None, code: int = 0) -> dict:
    """A `docker inspect` record, as much of it as `inspect_services` reads."""
    cfg = {"Labels": {"com.docker.compose.service": name}}
    if hc:
        cfg["Healthcheck"] = {"Test": ["CMD", "true"], "Interval": 10_000_000_000, "Retries": 3, "StartPeriod": 5_000_000_000}
    st = {"Status": state, "ExitCode": code, **({"Health": {"Status": health}} if health else {})}
    return {"Name": "/" + name, "Config": cfg, "State": st, "NetworkSettings": {"Ports": ports or {}}}


def self_test() -> int:
    """A healthy stand passes; an unhealthy, an exited, a never-ready and a closed-port service each fail by name; one without any probe is not waited for."""
    now = [0.0]

    def play(boxes_by_poll: list[list[dict]], ports_open: Callable[[int], bool] = lambda p: True, names: Sequence[str] = ()) -> tuple[list[str], list[str]]:
        polls, lines, now[0] = [0], [], 0.0

        def runner(argv: Sequence[str]) -> str:
            if not boxes_by_poll:
                return ""
            if argv[:3] == ["docker", "compose", "ps"]:
                boxes = boxes_by_poll[min(polls[0], len(boxes_by_poll) - 1)]
                return " ".join(b["Name"].lstrip("/") for b in boxes)
            polls[0] += 1
            return json.dumps(boxes_by_poll[min(polls[0] - 1, len(boxes_by_poll) - 1)])

        def sleep(s: float) -> None:
            now[0] += s

        return wait(names, runner=runner, probe=ports_open, clock=lambda: now[0], sleep=sleep, say=lines.append), lines

    failed, _ = play([[_box("a", health="healthy"), _box("b", health="healthy")]])
    assert failed == [], failed
    failed, _ = play([[_box("a", health="starting")], [_box("a", health="healthy")]])
    assert failed == [], "a service that turns healthy on the second look failed"
    failed, _ = play([[_box("a", health="healthy"), _box("b", health="unhealthy")]])
    assert failed == ["b"], failed
    failed, _ = play([[_box("a", state="exited", hc=False, code=1)]])
    assert failed == ["a"], "a crashed container was waited for"
    failed, _ = play([[_box("job", state="exited", hc=False, code=0)]])
    assert failed == [], "a one-shot container that finished was refused"
    failed, lines = play([[_box("a", health="starting")]])
    assert failed == ["a"] and any("not ready within" in x for x in lines), "a service that never turned healthy did not fail"
    open_port = {"8080/tcp": [{"HostIp": "0.0.0.0", "HostPort": "8080"}]}
    failed, _ = play([[_box("p", hc=False, ports=open_port)]], ports_open=lambda port: port == 8080)
    assert failed == [], failed
    failed, _ = play([[_box("p", hc=False, ports=open_port)]], ports_open=lambda port: False)
    assert failed == ["p"], "a closed published port was ready"
    failed, _ = play([[_box("q", hc=False)]])
    assert failed == [], "a service with no healthcheck and no published port should not be waited for"
    failed, lines = play([])
    assert failed == [] and any("no started service" in x for x in lines), lines
    print("stand_wait self-test ok: healthy, late-healthy, unhealthy, crashed, finished one-shot, never-ready, open port, closed port, unprobed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
