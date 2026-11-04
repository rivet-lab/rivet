"""The stands the release gate needs up (dev/stand/registry.yaml `gate_profiles`): started here, or the gate refuses to start.

A live cell whose stand is down panics `live test requires ... reachable on`; on the previous release that read as a fix
cell proven red. The gate brings every gate profile up before any cell and names the one that did not come up. A container
whose bind-mount source is gone (a deleted worktree) is recreated from this checkout, and named if it stays broken.

    python3 -m dev.release_oracle.stand_profiles --self-test
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlparse

from ..pytools import registry
from . import stand_health
from .core import ROOT, run

__all__ = ["bring_up", "refusal"]

UP_WAIT_SECS = 240


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
                     *(["--force-recreate"] if gone else []), *spec["services"]])
    still: dict[str, str] = {}
    for name, spec in profiles.items():
        gone = mounts(name, spec)
        if gone:
            still[name] = "bind-mount source gone: " + ", ".join(gone)
        elif not up(spec["url"]):
            still[name] = "did not come up"
    return still


def refusal(down: dict[str, str], profiles: dict[str, dict] | None = None) -> str:
    """Why the gate must not start ("" = every profile is usable), naming each broken profile, why, and its command."""
    profiles = registry.gate_profiles() if profiles is None else profiles
    if not down:
        return ""
    cmds = "; ".join(
        f"`docker compose {' '.join(profile_flag(n, profiles[n]))} up -d --wait --force-recreate {' '.join(profiles[n]['services'])}` ({profiles[n]['url']})"
        for n in down
    )
    why = "; ".join(f"{n}: {w}" for n, w in down.items())
    return f"STAND-PROFILE-DOWN: {why}; the live cells that need it would fail unread. Start: {cmds}"


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
    calls.clear()
    state.update(a=True, b=True, c=True)
    gone = {"c": ["sc: /deleted/worktree/dev/x.xml"]}
    broken = bring_up(spec, up=is_it_up, compose=compose, mounts=lambda name, s: gone.get(name, []))
    assert calls == [["up", "-d", "--wait", "--wait-timeout", str(UP_WAIT_SECS), "--force-recreate", "sc"]], calls
    assert broken == {"c": "bind-mount source gone: sc: /deleted/worktree/dev/x.xml"}, broken
    text = refusal(broken, spec)
    assert "c: bind-mount source gone" in text and "up -d --wait --force-recreate sc" in text, text
    assert refusal({}, spec) == "" and "--profile b up -d --wait --force-recreate sb1 sb2" in refusal({"b": "did not come up"}, spec)
    with tempfile.TemporaryDirectory() as d:
        mounts = json.dumps([{"Type": "bind", "Source": d}, {"Type": "bind", "Source": d + "/deleted"}, {"Type": "volume", "Source": "/var/lib/docker/volumes/x"}])
        assert missing_sources(mounts) == [d + "/deleted"] and missing_sources("") == [] and missing_sources("null") == []
    assert set(registry.gate_profiles()) >= {"shard", "tz", "mysql-stmt", "clickhouse"}, "the gate must start shard, tz, mysql-stmt and clickhouse"
    compose_file = yaml.safe_load((ROOT / "docker-compose.yaml").read_text())["services"]
    for name, p in registry.gate_profiles().items():
        for svc in p["services"]:
            assert p.get("profile", name) in (compose_file[svc].get("profiles") or [""]), f"gate profile {name}: compose service {svc} is not in it"
    print("stand_profiles self-test ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
