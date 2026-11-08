"""The stands the release gate needs up (dev/stand/registry.yaml `gate_profiles`): started here, or the gate refuses to start.

A live cell whose stand is down panics `live test requires ... reachable on`; on the previous release that read as a fix
cell proven red. The gate brings every gate profile up before any cell and names the one that did not come up. A container
whose bind-mount source is gone (a deleted worktree) is recreated from this checkout, and named if it stays broken.

    python3 -m dev.release_oracle.stand_profiles --self-test
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlparse

from ..pytools import registry
from ..pytools.shell import tcp_open
from . import stand_health
from .core import ROOT, run

__all__ = ["bring_up", "preflight", "refusal", "unusable"]

UP_WAIT_SECS = 240
RETRIES = 5
RETRY_SECS = 3.0
STORES = ("s3", "gcs", "azure")
AG_ROLES = {"mssql-ag-primary": "PRIMARY HEALTHY", "mssql-ag-secondary": "SECONDARY HEALTHY"}
AG_QUERY = ("SET NOCOUNT ON; SELECT r.replica_server_name, s.role_desc + ' ' + s.synchronization_health_desc "
            "FROM sys.dm_hadr_availability_replica_states s JOIN sys.availability_replicas r ON r.replica_id = s.replica_id")
AG_START = "dev/mssql-ag/setup.sh"
SA_PASSWORD = "Rivet_Passw0rd!"
ENV_HINT = re.compile(r"--profile (\w+) up -d(?: --wait)? ([\w-]+)")
NOT_GATE_OWNED = {
    "pgbouncer": "2026-11-08: pool profile, CI-owned pooler suite; the gate runs no live cell against it",
    "proxysql": "2026-11-08: pool profile, CI-owned pooler suite; the gate runs no live cell against it",
}


def is_up(url: str) -> bool:
    """Does the URL's host port accept a connection within the readiness deadline (`stand_health.ready`)?"""
    u = urlparse(url)
    return stand_health.reach_tcp(u.hostname or "127.0.0.1", u.port or 0)


def profile_flag(name: str, spec: dict) -> list[str]:
    """The `--profile` argument of a gate profile (none for a service of the default set)."""
    profile = spec.get("profile", name)
    return ["--profile", profile] if profile else []


def missing_sources(mounts_json: str) -> list[str]:
    """The bind-mount sources in a `docker inspect` `.Mounts` document that do not exist on this host."""
    return [m["Source"] for m in (json.loads(mounts_json or "null") or []) if m.get("Type") == "bind" and not os.path.exists(m["Source"])]


def gone_mounts(name: str, spec: dict) -> list[str]:
    """`service: source` for every bind-mount source of the profile's containers that no longer exists."""
    gone: list[str] = []
    for svc in spec["services"]:
        ids = run(["docker", "compose", *profile_flag(name, spec), "ps", "-aq", svc], cwd=ROOT, timeout=60).out.split()
        for cid in ids:
            gone += [f"{svc}: {src}" for src in missing_sources(run(["docker", "inspect", "--format", "{{json .Mounts}}", cid], timeout=60).out)]
    return gone


def seed_args(spec: dict) -> list[str]:
    """The `compose exec` arguments that run the profile's idempotent `seed_sql` on its first service."""
    return ["exec", "-T", spec["services"][0], "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa", "-P", SA_PASSWORD,
            "-C", "-b", "-Q", spec["seed_sql"]]


def bring_up(profiles: dict[str, dict] | None = None, up: Callable[[str], bool] = is_up,
             compose: Callable[[list[str]], bool] | None = None,
             mounts: Callable[[str, dict], list[str]] = gone_mounts) -> dict[str, str]:
    """Start each profile whose stand is down or mounts a source that is gone, waiting for health; profile -> why it is still unusable."""
    profiles = registry.gate_profiles() if profiles is None else profiles
    compose = compose or (lambda args: run(["docker", "compose", *args], cwd=ROOT, timeout=UP_WAIT_SECS + 120).ok)
    for name, spec in profiles.items():
        gone = mounts(name, spec)
        if gone or not up(spec["url"]):
            compose([*profile_flag(name, spec), "up", "-d", "--wait", "--wait-timeout", str(UP_WAIT_SECS),
                     *(["--force-recreate", "--no-deps"] if gone else []), *spec["services"]])
    still: dict[str, str] = {}
    for name, spec in profiles.items():
        gone = mounts(name, spec)
        if gone:
            still[name] = "bind-mount source gone: " + ", ".join(gone)
        elif not up(spec["url"]):
            still[name] = "did not come up"
        elif spec.get("seed_sql") and not compose(seed_args(spec)):
            still[name] = "its seed statement failed"
    return still


def replica_ports() -> list[int]:
    """Every host port a replica topology row needs (derived from the rows, never listed twice)."""
    from . import scenarios

    return sorted({port for _, ports, _, _ in scenarios.REPLICA_CELLS for port in ports})


def ag_roles() -> dict[str, str]:
    """`replica -> "ROLE HEALTH"` as the availability-group primary reports it ({} when it cannot be asked)."""
    p = run(["docker", "exec", "rivet-mssql-ag-primary-1", "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa",
             "-P", SA_PASSWORD, "-C", "-h", "-1", "-W", "-s", "|", "-Q", AG_QUERY], timeout=60)
    rows = [line.split("|") for line in p.out.splitlines() if "|" in line] if p.ok else []
    return {name.strip(): state.strip() for name, state in rows}


def replica_problem(port_open: Callable[[int], bool], roles: Callable[[], dict[str, str]]) -> str:
    """Why a replica topology row could not run now ("" = every port answers and the availability group is PRIMARY and SECONDARY, both HEALTHY)."""
    from . import scenarios

    open_ports = {port: port_open(port) for port in replica_ports()}
    why = [f"{label}: port(s) " + ", ".join(f":{p}" for p in ports if not open_ports[p]) + f" closed ({hint})"
           for label, ports, _, hint in scenarios.REPLICA_CELLS if not all(open_ports[p] for p in ports)]
    if all(open_ports[p] for p in next(ports for label, ports, _, _ in scenarios.REPLICA_CELLS if label == "mssql-secondary")):
        seen = roles()
        if not seen:
            why.append(f"availability group: the primary gave no reply to the role query ({AG_START})")
        why += [f"{name}: {seen.get(name, 'absent')}, wanted {want} ({AG_START})" for name, want in AG_ROLES.items() if seen and seen.get(name) != want]
    return "; ".join(why)


def stores_problem(store: Callable[[str], bool]) -> str:
    """Why the object-store fakes cannot serve a cell now ("" = minio, fake-gcs and azurite answer their health probes)."""
    down = [name for name in STORES if not store(name)]
    return f"{', '.join(down)} not answering (docker compose up -d minio fake-gcs azurite)" if down else ""


def settled(check: Callable[[], str], sleep: Callable[[float], None] = time.sleep) -> str:
    """The last answer of `check` after at most RETRIES tries, stopping at the first "" (a stand that is still settling is not down)."""
    why = check()
    for _ in range(RETRIES - 1):
        if not why:
            break
        sleep(RETRY_SECS)
        why = check()
    return why


def port_up(port: int) -> bool:
    """Does 127.0.0.1:port accept a connection within two seconds (one probe; `settled` retries)?"""
    return tcp_open("127.0.0.1", port, timeout=2.0)


def unusable(replica: bool, port_open: Callable[[int], bool] = port_up, roles: Callable[[], dict[str, str]] = ag_roles,
             store: Callable[[str], bool] | None = None, sleep: Callable[[float], None] = time.sleep) -> dict[str, str]:
    """The checks the stand must pass before any cell (`replica` only when replica rows run): name -> why, empty when usable."""
    from . import scenarios

    store = store or scenarios.store_up
    found = {"replica": settled(lambda: replica_problem(port_open, roles), sleep) if replica else "",
             "stores": settled(lambda: stores_problem(store), sleep)}
    return {name: why for name, why in found.items() if why}


def refusal(down: dict[str, str], profiles: dict[str, dict] | None = None) -> str:
    """Why the gate must not start ("" = every profile is usable), naming each broken profile, why, and its command."""
    profiles = registry.gate_profiles() if profiles is None else profiles
    if not down:
        return ""
    cmds = "; ".join(
        f"`docker compose {' '.join(profile_flag(n, profiles[n]))} up -d --wait --force-recreate --no-deps {' '.join(profiles[n]['services'])}` ({profiles[n]['url']})"
        for n in down if n in profiles
    )
    why = "; ".join(f"{n}: {w}" for n, w in down.items())
    return f"STAND-PROFILE-DOWN: {why}; the live cells that need it would fail unread." + (f" Start: {cmds}" if cmds else "")


def check_self_test() -> None:
    """The replica, availability-group and object-store checks name what is down, retry a bounded number of times, and stay silent when everything answers."""
    roles = {"mssql-ag-primary": "PRIMARY HEALTHY", "mssql-ag-secondary": "SECONDARY HEALTHY"}
    naps: list[float] = []
    every = dict(port_open=lambda port: True, roles=lambda: roles, store=lambda name: True, sleep=naps.append)
    assert unusable(True, **every) == {} and naps == []
    gate = unusable(True, **{**every, "port_open": lambda port: port != 1441})
    assert set(gate) == {"replica"} and ":1441 closed" in gate["replica"] and "mssql-secondary" in gate["replica"], gate
    assert len(naps) == RETRIES - 1, naps
    sick = unusable(True, **{**every, "roles": lambda: {**roles, "mssql-ag-secondary": "SECONDARY NOT_HEALTHY"}})
    assert "mssql-ag-secondary: SECONDARY NOT_HEALTHY" in sick["replica"], sick
    swapped = unusable(True, **{**every, "roles": lambda: {"mssql-ag-primary": "SECONDARY HEALTHY", "mssql-ag-secondary": "PRIMARY HEALTHY"}})
    assert "mssql-ag-primary: SECONDARY HEALTHY" in swapped["replica"], swapped
    assert "no reply" in unusable(True, **{**every, "roles": lambda: {}})["replica"]
    stores = unusable(True, **{**every, "store": lambda name: name != "gcs"})
    assert set(stores) == {"stores"} and "gcs" in stores["stores"] and "s3" not in stores["stores"], stores
    assert "replica" not in unusable(False, **{**every, "port_open": lambda port: False, "roles": lambda: {}})
    seen: list[int] = []
    naps.clear()
    flaky = lambda port: (seen.append(port), len(seen) > len(replica_ports()))[1]  # noqa: E731
    assert unusable(True, **{**every, "port_open": flaky}) == {} and naps, "a stand that answers on the retry is usable"
    import socket

    with socket.socket() as held, socket.socket() as spare:
        held.bind(("127.0.0.1", 0))
        held.listen()
        spare.bind(("127.0.0.1", 0))
        gone = spare.getsockname()[1]
        spare.close()
        assert port_up(held.getsockname()[1]) and not port_up(gone), "the default probe tells a listening port from a closed one"
    assert isinstance(unusable(True, **{k: v for k, v in every.items() if k != "port_open"}), dict), "the default probe runs"
    text = refusal(gate)
    assert text.startswith("STAND-PROFILE-DOWN: replica: ") and "dev/mssql-ag/setup.sh" in text and "docker compose" in text, text


def preflight(replica: bool) -> str:
    """The refusal text for a stand that is not ready to start the gate ("" = start): profiles brought up, then the replica and store checks."""
    return refusal({**bring_up(), **unusable(replica)})


def self_test() -> None:
    """Bring-up starts a stand that is down or mounts a gone source (recreating it), and a stand that stays broken is named in the refusal."""
    import socket
    import threading
    import yaml

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    late_port = probe.getsockname()[1]
    probe.close()
    late = socket.socket()

    def listen_late() -> None:
        time.sleep(1.5)
        late.bind(("127.0.0.1", late_port))
        late.listen(1)

    threading.Thread(target=listen_late, daemon=True).start()
    assert is_up(f"tcp://127.0.0.1:{late_port}"), "a stand that starts listening a moment after the first probe is up, not skipped"
    late.close()

    spec = {"a": {"services": ["sa"], "url": "tcp://127.0.0.1:1"}, "b": {"services": ["sb1", "sb2"], "url": "tcp://127.0.0.1:2"},
            "c": {"services": ["sc"], "url": "tcp://127.0.0.1:3", "profile": ""}}
    state = {"a": True, "b": False, "c": True}
    calls: list[list[str]] = []

    def is_it_up(url: str) -> bool:
        return state[{"1": "a", "2": "b", "3": "c"}[url[-1]]]

    def compose(args: list[str]) -> bool:
        calls.append(args)
        state[{"sa": "a", "sb1": "b", "sc": "c"}[next(x for x in args if x in ("sa", "sb1", "sc"))]] = True
        return True

    healthy = lambda name, s: []  # noqa: E731
    assert bring_up(spec, up=is_it_up, compose=compose, mounts=healthy) == {}
    assert calls == [["--profile", "b", "up", "-d", "--wait", "--wait-timeout", str(UP_WAIT_SECS), "sb1", "sb2"]], calls
    state.update(a=True, b=False, c=True)
    assert bring_up(spec, up=is_it_up, compose=lambda args: False, mounts=healthy) == {"b": "did not come up"}
    seeded = {"c": {**spec["c"], "seed_sql": "SELECT 1"}}
    seen: list[list[str]] = []
    state.update(a=True, b=True, c=True)
    assert bring_up(seeded, up=is_it_up, compose=lambda args: seen.append(args) or True, mounts=healthy) == {}
    assert seen == [seed_args(seeded["c"])] and seen[0][:3] == ["exec", "-T", "sc"] and seen[0][-1] == "SELECT 1", seen
    assert bring_up(seeded, up=is_it_up, compose=lambda args: False, mounts=healthy) == {"c": "its seed statement failed"}
    assert registry.gate_profiles()["governor"]["seed_sql"] == "IF DB_ID('rivet') IS NULL CREATE DATABASE rivet"
    calls.clear()
    state.update(a=True, b=True, c=True)
    gone = {"c": ["sc: /deleted/worktree/dev/x.xml"]}
    broken = bring_up(spec, up=is_it_up, compose=compose, mounts=lambda name, s: gone.get(name, []))
    assert calls == [["up", "-d", "--wait", "--wait-timeout", str(UP_WAIT_SECS), "--force-recreate", "--no-deps", "sc"]], calls
    assert broken == {"c": "bind-mount source gone: sc: /deleted/worktree/dev/x.xml"}, broken
    text = refusal(broken, spec)
    assert "c: bind-mount source gone" in text and "up -d --wait --force-recreate --no-deps sc" in text, text
    assert refusal({}, spec) == "" and "--profile b up -d --wait --force-recreate --no-deps sb1 sb2" in refusal({"b": "did not come up"}, spec)
    with tempfile.TemporaryDirectory() as d:
        mounts = json.dumps([{"Type": "bind", "Source": d}, {"Type": "bind", "Source": d + "/deleted"}, {"Type": "volume", "Source": "/var/lib/docker/volumes/x"}])
        assert missing_sources(mounts) == [d + "/deleted"] and missing_sources("") == [] and missing_sources("null") == []
    assert set(registry.gate_profiles()) >= {"shard", "tz", "mysql-stmt", "clickhouse", "governor", "minio", "fake-gcs", "azurite"}, \
        "the gate must start shard, tz, mysql-stmt, clickhouse, governor and the three object stores"
    compose_file = yaml.safe_load((ROOT / "docker-compose.yaml").read_text())["services"]
    for name, p in registry.gate_profiles().items():
        for svc in p["services"]:
            assert p.get("profile", name) in (compose_file[svc].get("profiles") or [""]), f"gate profile {name}: compose service {svc} is not in it"
    owned = {(p.get("profile", n), s) for n, p in registry.gate_profiles().items() for s in p["services"]}
    hints = set(ENV_HINT.findall((ROOT / "tests/common/env.rs").read_text()))
    assert hints, "tests/common/env.rs names no `--profile P up -d SERVICE` hint to compare with the gate profiles"
    missing = sorted(h for h in hints if h not in owned and h[1] not in NOT_GATE_OWNED)
    assert not missing, f"a live cell hints a stand the gate never starts (add it to dev/stand/registry.yaml gate_profiles): {missing}"
    check_self_test()
    print("stand_profiles self-test ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
