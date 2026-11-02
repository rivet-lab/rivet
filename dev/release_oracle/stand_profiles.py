"""The compose profiles the gate needs up (dev/stand/registry.yaml `gate_profiles`): started here, or the gate refuses to start.

A live cell whose stand is down panics `live test requires ... reachable on`; on the previous release that read as a fix
cell proven red. The gate brings `shard` and `tz` up before any cell and names the one that did not come up.

    python3 -m dev.release_oracle.stand_profiles --self-test
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlparse

from ..pytools import registry
from ..pytools.shell import tcp_open
from .core import ROOT, run

__all__ = ["bring_up", "refusal"]

UP_WAIT_SECS = 240


def is_up(url: str) -> bool:
    """Does something listen on the URL's host port?"""
    u = urlparse(url)
    return tcp_open(u.hostname or "127.0.0.1", u.port or 0, timeout=2.0)


def bring_up(profiles: dict[str, dict] | None = None, up: Callable[[str], bool] = is_up,
             compose: Callable[[list[str]], bool] | None = None) -> list[str]:
    """Start each profile whose stand is down, waiting for health; the profiles still down afterwards."""
    profiles = registry.gate_profiles() if profiles is None else profiles
    compose = compose or (lambda args: run(["docker", "compose", *args], cwd=ROOT, timeout=UP_WAIT_SECS + 120).ok)
    for name, spec in profiles.items():
        if not up(spec["url"]):
            compose(["--profile", name, "up", "-d", "--wait", "--wait-timeout", str(UP_WAIT_SECS), *spec["services"]])
    return [name for name, spec in profiles.items() if not up(spec["url"])]


def refusal(down: list[str], profiles: dict[str, dict] | None = None) -> str:
    """Why the gate must not start ("" = every profile is up), naming each missing profile and its command."""
    profiles = registry.gate_profiles() if profiles is None else profiles
    if not down:
        return ""
    cmds = "; ".join(f"`docker compose --profile {n} up -d --wait {' '.join(profiles[n]['services'])}` ({profiles[n]['url']})" for n in down)
    return f"STAND-PROFILE-DOWN: {', '.join(down)} did not come up; the live cells that need it would fail unread. Start: {cmds}"


def self_test() -> None:
    """Bring-up starts only the stands that are down, and a stand that stays down is named in the refusal."""
    import yaml

    spec = {"a": {"services": ["sa"], "url": "tcp://127.0.0.1:1"}, "b": {"services": ["sb1", "sb2"], "url": "tcp://127.0.0.1:2"}}
    state = {"a": True, "b": False}
    calls: list[list[str]] = []

    def compose(args: list[str]) -> bool:
        calls.append(args)
        state[args[1]] = True
        return True

    still = bring_up(spec, up=lambda url: state["a" if url.endswith(":1") else "b"], compose=compose)
    assert still == [] and calls == [["--profile", "b", "up", "-d", "--wait", "--wait-timeout", str(UP_WAIT_SECS), "sb1", "sb2"]], calls
    state.update(a=True, b=False)
    assert bring_up(spec, up=lambda url: state["a" if url.endswith(":1") else "b"], compose=lambda args: False) == ["b"]
    assert refusal([], spec) == "" and "b did not come up" in refusal(["b"], spec) and "--profile b up -d --wait sb1 sb2" in refusal(["b"], spec)
    assert set(registry.gate_profiles()) >= {"shard", "tz"}, "the gate must start shard and tz"
    compose_file = yaml.safe_load((ROOT / "docker-compose.yaml").read_text())["services"]
    for name, p in registry.gate_profiles().items():
        for svc in p["services"]:
            assert name in compose_file[svc].get("profiles", []), f"gate profile {name}: compose service {svc} is not in it"
    print("stand_profiles self-test ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
