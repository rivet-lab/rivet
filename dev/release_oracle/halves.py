"""The release gate in two halves: the one table that places every stage, and the derivation that places every seeded-recall seed, on a machine."""

from __future__ import annotations

import re
from collections.abc import Collection, Iterable, Mapping, Sequence

from .core import DEVBOX, HALVES, HOSTED

#: Every per-run gate stage by name, in table order, with the half that runs it; the measured stages are the island's and are not here.
STAGE_HALF: dict[str, str] = {
    "release build path": HOSTED,
    "release hygiene": HOSTED,
    "state migrations": HOSTED,
    "network faults": HOSTED,
    "tls required": HOSTED,
    "tls downgrade": HOSTED,
    "no_secret_in_output": HOSTED,
    "auth": HOSTED,
    "cdc standby": DEVBOX,
    "coverage matrices": HOSTED,
    "property matrices": HOSTED,
    "flag surface": HOSTED,
    "run strict": HOSTED,
    "gif currency": HOSTED,
    "replica read": DEVBOX,
    "pool e2e": HOSTED,
    "pool split": HOSTED,
    "failed-run tail": HOSTED,
    "transient retry": HOSTED,
    "batch resume": HOSTED,
    "partition footer": HOSTED,
    "audit suspects": HOSTED,
    "cdc harm": HOSTED,
    "session state": HOSTED,
    "no silent null": HOSTED,
    "no silent null grants": HOSTED,
    "no silent null cdc": HOSTED,
    "no silent null load": HOSTED,
    "cdc e2e": HOSTED,
    "cdc differential": HOSTED,
    "live-only coverage": DEVBOX,
    "upgrade continuity": DEVBOX,
    "upgrade from a field state": DEVBOX,
    "byte-identical parts": HOSTED,
    "state backend parity": HOSTED,
    "shared state": DEVBOX,
    "warehouse layout": DEVBOX,
    "init delta": DEVBOX,
    "partner shape": DEVBOX,
    "clickhouse load": HOSTED,
    "cdc schema drift": HOSTED,
    "concurrent writers": DEVBOX,
    "engine matrix": DEVBOX,
    "cloud legs": DEVBOX,
    "bigquery golden": DEVBOX,
    "fix cells": DEVBOX,
    "live modules devbox": DEVBOX,
    "live modules": HOSTED,
}

#: The half each `Need` variant of tests/common/mod.rs places a live test in: a test whose call graph names a need runs where the need is held; a variant missing here fails `needs_problems`.
NEED_HALF: dict[str, str] = {
    "Credential": DEVBOX,
    "MacOs": DEVBOX,
    "Stand": DEVBOX,
    "Timing": DEVBOX,
}

#: The topology a live test's `#[ignore = "..."]` reason names that no call in the test says (a gate-only stand, a replica or availability-group pair, a standby, a latin1 server).
DEVBOX_TOPOLOGY = re.compile(r"gate-only|--profile replica|replica-profile|replica profile|mongo-rs2|mssql-ag|cdc-standby|oracle-latin1", re.I)

#: The stage that runs the live tests the hosted stack can run, and the stage that runs the rest.
LIVE_HOSTED_STAGE, LIVE_DEVBOX_STAGE = "live modules", "live modules devbox"


def devbox_only(reason: str, needs: Iterable[str] = ()) -> bool:
    """Whether a live test runs in the devbox half: its `#[ignore]` reason names a topology only the devbox holds, or its call graph carries a need `NEED_HALF` places there."""
    return DEVBOX_TOPOLOGY.search(reason) is not None or any(NEED_HALF[n] == DEVBOX for n in needs)


def needs_problems(variants: Iterable[str]) -> list[str]:
    """The `Need` variants of the Rust side that `NEED_HALF` does not place, and the placements naming a variant that no longer exists."""
    variants = set(variants)
    return [f"Need::{v} is declared in tests/common/mod.rs and placed in no half" for v in sorted(variants - set(NEED_HALF))] + [
        f"NEED_HALF places Need::{v}, which tests/common/mod.rs does not declare" for v in sorted(set(NEED_HALF) - variants)
    ] + [f"NEED_HALF places Need::{v} in {h!r}, not a half" for v, h in NEED_HALF.items() if h not in HALVES]


def parse_live_shard(spec: str) -> tuple[int, int]:
    """`i/n` as (i, n) with 0 <= i <= n and n >= 1; anything else raises ValueError."""
    m = re.fullmatch(r"(\d+)/(\d+)", spec)
    if not m or not (0 <= int(m[1]) <= int(m[2])) or int(m[2]) < 1:
        raise ValueError(f"--live-shard wants i/n with 0 <= i <= n and n >= 1 (0 = the hosted stages but the live modules, 1..n = a slice of them), got {spec!r}")
    return int(m[1]), int(m[2])


def slice_modules(weights: Mapping[str, float], n: int, anchored: Collection[str] | None = None) -> list[list[str]]:
    """The modules dealt into `n` slices, heaviest first into the lightest slice, the `anchored` ones (all when None) before the rest; deterministic, each module in exactly one slice."""
    first = set(weights) if anchored is None else set(anchored)
    slices: list[list[str]] = [[] for _ in range(n)]
    load = [0.0] * n
    for m in sorted(weights, key=lambda k: (k not in first, -weights[k], k)):
        i = load.index(min(load))
        slices[i].append(m)
        load[i] += weights[m]
    return [sorted(s) for s in slices]


def stage_half(name: str) -> str:
    """The half that runs the stage `name`; a stage the table does not place is an error, never a default."""
    try:
        return STAGE_HALF[name]
    except KeyError:
        raise SystemExit(f"stage {name!r} is placed in neither half: add it to STAGE_HALF in dev/release_oracle/halves.py") from None


def seed_homes() -> dict[str, str]:
    """Seed -> half, derived from the ledger, the live-test table and the Need seam (`dev.seeded.shards.homes`); the hosted ones run in the Nightly Recall workflow, the rest in the devbox half."""
    from dev.seeded import shards

    rows, devbox_tests, _ = shards.load()
    return {seed: half for seed, (half, _) in shards.homes(rows, devbox_tests).items()}


def half_seeds(half: str, homes: Mapping[str, str] | None = None) -> list[str]:
    """The seeds placed in `half`, in sorted order."""
    return sorted(s for s, h in (seed_homes() if homes is None else homes).items() if h == half)


def problems(stage_names: Sequence[str], seed_names: Sequence[str], stage_table: Mapping[str, str] = STAGE_HALF,
             homes: Mapping[str, str] | None = None) -> list[str]:
    """What is wrong with the placement: a stage in no half, a table row naming no stage, a value that is no half, a seed that runs in no half or in a half that does not exist."""
    out = [f"stage {n!r} is placed in neither half" for n in stage_names if n not in stage_table]
    out += [f"STAGE_HALF names {n!r}, which no gate stage has" for n in stage_table if n not in stage_names]
    out += [f"stage {n!r} is placed in {h!r}, which is no half of {HALVES}" for n, h in stage_table.items() if h not in HALVES]
    homes = seed_homes() if homes is None else homes
    out += [f"seed {s!r} is placed in no half" for s in seed_names if s not in homes]
    out += [f"placement names seed {s!r}, which the ledger does not have" for s in homes if s not in seed_names]
    out += [f"seed {s!r} is placed in {h!r}, which is no half of {HALVES}" for s, h in homes.items() if h not in HALVES]
    return out
