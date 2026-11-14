"""The release gate in two halves: the one table that places every stage and every seeded-recall shard on a machine."""

from __future__ import annotations

import importlib.util
from collections.abc import Iterable, Mapping, Sequence

from .core import DEVBOX, HALVES, HOSTED, ROOT

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
    "cdc e2e": DEVBOX,
    "cdc differential": DEVBOX,
    "live-only coverage": DEVBOX,
    "upgrade continuity": DEVBOX,
    "upgrade from a field state": DEVBOX,
    "byte-identical parts": DEVBOX,
    "state backend parity": DEVBOX,
    "shared state": DEVBOX,
    "warehouse layout": DEVBOX,
    "init delta": DEVBOX,
    "partner shape": DEVBOX,
    "clickhouse load": DEVBOX,
    "cdc schema drift": DEVBOX,
    "concurrent writers": DEVBOX,
    "engine matrix": DEVBOX,
    "cloud legs": DEVBOX,
    "bigquery golden": DEVBOX,
    "fix cells": DEVBOX,
    "live modules": HOSTED,
}

#: The half of each seeded-recall shard, in shard order; its length is the shard count (the nightly workflow deals 4).
SEED_SHARD_HALF: tuple[str, ...] = (DEVBOX, DEVBOX, DEVBOX, DEVBOX)


def stage_half(name: str) -> str:
    """The half that runs the stage `name`; a stage the table does not place is an error, never a default."""
    try:
        return STAGE_HALF[name]
    except KeyError:
        raise SystemExit(f"stage {name!r} is placed in neither half: add it to STAGE_HALF in dev/release_oracle/halves.py") from None


def deal(names: Sequence[str], shards: int) -> list[dict]:
    """The nightly workflow's own dealer, loaded from its script: sorted names round-robin into shards."""
    path = ROOT / ".github" / "scripts" / "recall_shards.py"
    spec = importlib.util.spec_from_file_location("recall_shards", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.deal(sorted(names), shards)


def half_seeds(half: str, names: Iterable[str], shard_half: Sequence[str] = SEED_SHARD_HALF) -> list[str]:
    """The seeds whose recall shard belongs to `half`, in sorted order."""
    owned = [s for shard in deal(list(names), len(shard_half)) if shard_half[shard["index"]] == half for s in shard["seeds"].split(",")]
    return sorted(owned)


def problems(stage_names: Sequence[str], seed_names: Sequence[str], stage_table: Mapping[str, str] = STAGE_HALF,
             shard_half: Sequence[str] = SEED_SHARD_HALF) -> list[str]:
    """What is wrong with the placement: a stage in no half, a table row naming no stage, a value that is no half, a seed that runs in no half or in two."""
    out = [f"stage {n!r} is placed in neither half" for n in stage_names if n not in stage_table]
    out += [f"STAGE_HALF names {n!r}, which no gate stage has" for n in stage_table if n not in stage_names]
    out += [f"stage {n!r} is placed in {h!r}, which is no half of {HALVES}" for n, h in stage_table.items() if h not in HALVES]
    if not shard_half:
        return out + ["SEED_SHARD_HALF has no shard: no seed runs in any half"]
    out += [f"shard {i} is placed in {h!r}, which is no half of {HALVES}" for i, h in enumerate(shard_half) if h not in HALVES]
    if out:
        return out
    runs = [s for half in HALVES for s in half_seeds(half, seed_names, shard_half)]
    out += [f"seed {s!r} runs in no half" for s in seed_names if s not in runs]
    out += [f"seed {s!r} runs in {runs.count(s)} halves" for s in sorted(set(runs)) if runs.count(s) > 1]
    return out
