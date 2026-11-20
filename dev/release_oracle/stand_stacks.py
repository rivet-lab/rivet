"""The gate profiles a CI stack starts, read from dev/stand/registry.yaml, and the guard that a live test's service is started.

A gate profile with `stacks: [...]` is started in each of those stacks (the live-stack action's `stack` input and the Mongo job
of nightly-live) by `up STACK`, so a workflow names a stack and never a service. The self-test fails when a service a live test
requires (tests/common/env.rs) is in neither a stack's hand-kept compose list nor a profile of that stack.

    python3 -m dev.release_oracle.stand_stacks up STACK
    python3 -m dev.release_oracle.stand_stacks --self-test
"""

from __future__ import annotations

import re
import sys

import yaml

from ..pytools import registry
from . import stand_profiles
from .core import ROOT

ACTION = ROOT / ".github/actions/live-stack/action.yml"
NIGHTLY = ROOT / ".github/workflows/nightly-live.yml"
ENV_RS = ROOT / "tests/common/env.rs"
COMPOSE = ROOT / "docker-compose.yaml"
LIVE_STACKS = ("lean", "full")
CALL = "dev.release_oracle.stand_stacks up"
SERVICE_NAME = re.compile(r"service `([a-z0-9-]+)`|up -d (?:--wait )?([a-z0-9-]+)\"")
ACTION_LIST = re.compile(r'(core|lean|\*)\) services="([^"]*)"')
EXEMPT = {
    ("lean", "mongos"): "2026-11-19: the shard profile is not in the lean stack, a_sharded_mongo_run_* is STAND there; closes when lean starts it or those cells move to the Mongo job",
    ("full", "mongos"): "2026-11-19: the shard profile is not in the full stack, a_sharded_mongo_run_* is STAND there; closes when full starts it or those cells move to the Mongo job",
}


def profiles_of(stack: str, profiles: dict[str, dict] | None = None, stacks: list[str] | None = None) -> dict[str, dict]:
    """The gate profiles that name `stack`; a stack the registry does not list is an error, never an empty start."""
    profiles = registry.gate_profiles() if profiles is None else profiles
    stacks = registry.load()["stacks"] if stacks is None else stacks
    if stack not in stacks:
        raise ValueError(f"stack {stack!r} is not one of the registry's stacks {stacks}")
    return {n: p for n, p in profiles.items() if stack in p.get("stacks", [])}


def up(stack: str) -> int:
    """Start the stack's gate profiles; 0 when every one is up, 1 with the refusal text when one is not, 2 for an unknown stack."""
    try:
        profiles = profiles_of(stack)
    except ValueError as e:
        print(e, flush=True)
        return 2
    down = stand_profiles.bring_up(profiles)
    text = stand_profiles.refusal(down, profiles)
    print(text or f"stack {stack}: gate profiles up: {', '.join(profiles) or 'none'}", flush=True)
    return 1 if text else 0


def required_services(env_rs: str, compose_services: set[str]) -> set[str]:
    """The compose services the live tests' `LiveService` hints name."""
    return {n for a, b in SERVICE_NAME.findall(env_rs) for n in (a or b,) if n in compose_services}


def profile_services(stack: str, profiles: dict[str, dict] | None = None) -> set[str]:
    """The compose services of the gate profiles that name `stack`."""
    return {s for p in profiles_of(stack, profiles).values() for s in p["services"]}


def action_started(action: dict, stack: str, profiles: dict[str, dict] | None = None) -> set[str]:
    """What the live-stack action starts for `stack`: its compose list, plus the stack's gate profiles when the step calls `up`."""
    step = next(s for s in action["runs"]["steps"] if s.get("name") == "Start services")["run"]
    lists = {("full" if k == "*" else k): set(v.split()) for k, v in ACTION_LIST.findall(step)}
    return lists[stack] | (profile_services(stack, profiles) if CALL in step else set())


def mongo_job_started(workflow: dict, profiles: dict[str, dict] | None = None) -> set[str]:
    """What nightly-live's Mongo job starts: its `up -d --wait` list, plus the `mongo` stack's gate profiles when the step calls `up`."""
    step = next(s for s in workflow["jobs"]["mongo-versions"]["steps"] if s.get("name") == "Start Mongo stack")["run"]
    listed = set(re.search(r"up -d --wait ([^\n]+)", step).group(1).split())
    return listed | (profile_services("mongo", profiles) if f"{CALL} mongo" in step else set())


def gaps(required: set[str], started: dict[str, set[str]], exempt: dict[tuple[str, str], str] = EXEMPT) -> list[str]:
    """`stack: service` for each required service a stack does not start and no exemption names, and each exemption that no longer applies."""
    missing = [f"{stack}: {svc}" for stack, have in started.items() for svc in sorted(required - have) if (stack, svc) not in exempt]
    stale = [f"{stack}: {svc} is exempt but started or not required" for (stack, svc) in exempt if svc in started.get(stack, set()) or svc not in required]
    return missing + stale


def live_gaps() -> list[str]:
    """The gaps of this checkout: the lean and full stacks against every hinted service, the Mongo job against the hinted mongo services."""
    required = required_services(ENV_RS.read_text(), set(yaml.safe_load(COMPOSE.read_text())["services"]))
    action = yaml.safe_load(ACTION.read_text())
    found = gaps(required, {s: action_started(action, s) for s in LIVE_STACKS})
    mongo = {s for s in required if s.startswith("mongo")}
    return found + [f"mongo job: {s}" for s in sorted(mongo - mongo_job_started(yaml.safe_load(NIGHTLY.read_text())))]


def self_test() -> None:
    """A stack starts exactly the profiles that name it, an unknown stack is refused, and a required service no stack starts is a gap."""
    spec = {"a": {"services": ["sa"], "stacks": ["lean"]}, "b": {"services": ["sb1", "sb2"], "stacks": ["lean", "mongo"]}, "c": {"services": ["sc"]}}
    known = ["core", "lean", "mongo"]
    assert list(profiles_of("lean", spec, known)) == ["a", "b"] and list(profiles_of("mongo", spec, known)) == ["b"] and profiles_of("core", spec, known) == {}
    try:
        profiles_of("lan", spec, known)
    except ValueError:
        pass
    else:
        raise AssertionError("a stack the registry does not list must be refused")
    assert up("no-such-stack") == 2
    assert gaps({"x", "y"}, {"lean": {"x", "y", "z"}, "full": {"x"}}, {}) == ["full: y"]
    assert gaps({"x"}, {"lean": set()}, {("lean", "x"): "dated"}) == [] and gaps({"x"}, {"lean": {"x"}}, {("lean", "x"): "dated"}), "an exemption that no longer applies is a gap"
    assert gaps(set(), {"lean": set()}, {("lean", "x"): "dated"}), "an exemption for a service no live test requires is a gap"
    action = {"runs": {"steps": [{"name": "Start services", "run": 'case "$STACK" in\n core) services="p" ;;\n lean) services="p q" ;;\n *) services="p q r" ;;\n esac\n' + CALL + ' "$STACK"'}]}}
    assert action_started(action, "lean", spec) == {"p", "q", "sa", "sb1", "sb2"} and action_started(action, "full", spec) == {"p", "q", "r"}
    action["runs"]["steps"][0]["run"] = action["runs"]["steps"][0]["run"].replace(CALL, "true")
    assert action_started(action, "lean", spec) == {"p", "q"}, "a start step that does not call `up` starts no profile"
    job = {"jobs": {"mongo-versions": {"steps": [{"name": "Start Mongo stack", "run": f"docker compose up -d --wait m1 m2\n{CALL} mongo"}]}}}
    assert mongo_job_started(job, spec) == {"m1", "m2", "sb1", "sb2"}
    job["jobs"]["mongo-versions"]["steps"][0]["run"] = "docker compose up -d --wait m1 m2"
    assert mongo_job_started(job, spec) == {"m1", "m2"}
    assert required_services("service `mongo-auth-rs` x up -d --wait mongos\" service `nope`", {"mongo-auth-rs", "mongos"}) == {"mongo-auth-rs", "mongos"}
    assert set(registry.load()["stacks"]) >= {"core", *LIVE_STACKS, "mongo"}
    assert {s for p in registry.gate_profiles().values() for s in p.get("stacks", [])} <= set(registry.load()["stacks"]), "a profile names a stack the registry does not list"
    action_text = yaml.safe_load(ACTION.read_text())["runs"]["steps"]
    labels = {("full" if k == "*" else k) for k, _ in ACTION_LIST.findall(next(s for s in action_text if s.get("name") == "Start services")["run"])}
    assert labels <= set(registry.load()["stacks"]), f"the live-stack action starts a stack the registry does not list: {labels}"
    found = live_gaps()
    assert not found, f"a service a live test requires is started by no stack's list and no gate profile naming that stack: {found}"
    print("stand_stacks self-test ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    elif len(sys.argv) == 3 and sys.argv[1] == "up":
        sys.exit(up(sys.argv[2]))
    else:
        sys.exit("usage: python3 -m dev.release_oracle.stand_stacks (up STACK | --self-test)")
