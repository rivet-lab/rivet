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
    "no silent null load cloud": DEVBOX,
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


def parse_shard_spec(spec: str) -> tuple[int, int, int]:
    """`i/n` or `i/n:k` as (i, n, k): n live slices, k stage slices (default 1), 0 <= i <= n + k - 1; anything else raises ValueError."""
    m = re.fullmatch(r"(\d+)/(\d+)(?::(\d+))?", spec)
    i, n, k = (int(m[1]), int(m[2]), int(m[3] or 1)) if m else (-1, 0, 0)
    if not m or n < 1 or k < 1 or not 0 <= i <= n + k - 1:
        raise ValueError(f"--live-shard wants i/n or i/n:k with n, k >= 1 and 0 <= i <= n + k - 1 (1..n = a slice of the live modules, 0 and n+1..n+k-1 = a slice of the other hosted stages), got {spec!r}")
    return i, n, k


def parse_live_shard(spec: str) -> tuple[int, int]:
    """`i/n[:k]` as (i, n); anything else raises ValueError."""
    i, n, _ = parse_shard_spec(spec)
    return i, n


def stage_slice_index(i: int, n: int) -> int | None:
    """Which stage slice slice `i` of `n` live slices is (0 for slice 0, `i - n` past the live ones), or None when `i` is a live slice."""
    return 0 if i == 0 else None if i <= n else i - n


#: Minutes each hosted stage took on the hosted run 44261857361 (sha bd36fb3d, 4 cores); only the balance of the stage slices reads them, so a stale number costs minutes and never a verdict.
STAGE_MINUTES: dict[str, float] = {
    "no_secret_in_output": 20.8,
    "release build path": 9.8,
    "no silent null": 14.0,
    "no silent null cdc": 6.0,
    "no silent null grants": 4.0,
    "no silent null load": 3.8,
    "cdc differential": 3.5,
    "state migrations": 3.4,
    "byte-identical parts": 2.4,
    "clickhouse load": 1.9,
    "cdc e2e": 2.2,
    "batch resume": 1.4,
    "network faults": 1.0,
}
#: What a stage costs that `STAGE_MINUTES` does not name.
DEFAULT_STAGE_MINUTES = 0.5


def slice_stages(names: Sequence[str], k: int, weights: Mapping[str, float] = STAGE_MINUTES) -> list[list[str]]:
    """The hosted stages `names` dealt into `k` stage slices, heaviest first into the lightest, each slice in the order of `names`; deterministic, each stage in exactly one slice."""
    load, out = [0.0] * k, [[] for _ in range(k)]
    for name in sorted(names, key=lambda n: (-weights.get(n, DEFAULT_STAGE_MINUTES), names.index(n))):
        j = load.index(min(load))
        out[j].append(name)
        load[j] += weights.get(name, DEFAULT_STAGE_MINUTES)
    return [[n for n in names if n in set(sl)] for sl in out]


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
