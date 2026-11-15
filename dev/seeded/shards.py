"""Where each seed's recall runs and how the hosted share is dealt into shards: all derived from the ledger, the live-test table and the measured costs."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Collection, Iterable, Mapping, Sequence

import yaml

from dev.release_oracle.core import DEVBOX, HOSTED
from dev.seeded.patch_seeds import PATCH_SEEDS
from dev.seeded.recall import LIB, MANIFEST, Row, parse_manifest, patch_form_problems, switch_problems

#: Measured on devbox (timings.jsonl a9d9209a): switch waves 13.18 min / 67 seeds, CONTROL 34 min / 260 cells.
SEED_MIN, CELL_MIN = 0.2, 0.085
#: Minutes a shard spends on its checkout, build download and docker stack before its first cell (hosted run 43615816196: 12-15).
STATE_ENGINE = "postgres-state"
SETUP_MIN = {"lean": 14.0, "none": 3.0}
#: Work minutes one shard is dealt (setup is on top).
SHARD_TARGET_MIN = 35.0


def homes(rows: Sequence[Row], devbox_tests: Collection[str]) -> dict[str, tuple[str, str]]:
    """Seed -> (half, why): a patch-form row, a macOS row or a cell only the devbox stand holds puts the seed on the devbox; every other seed is hosted."""
    out: dict[str, tuple[str, str]] = {}
    for seed in sorted({r.seed for r in rows}):
        live = [r for r in rows if r.seed == seed and not r.na]
        why = ""
        if any(r.patch for r in live):
            why = "a patch-form row (one build per patch)"
        elif any(r.platform == "macos" for r in live):
            why = "a macOS-only row"
        else:
            held = sorted({c for r in live for c in r.cells if c.removeprefix(LIB) in devbox_tests})
            if held:
                why = f"cell {held[0]} runs on the devbox stand only"
        out[seed] = (DEVBOX, why) if why else (HOSTED, "")
    return out


def cells_of(rows: Sequence[Row], seed: str) -> set[str]:
    """The distinct cells the seed's graded rows name."""
    return {c for r in rows if r.seed == seed and not r.na for c in r.cells}


def stack_of(rows: Sequence[Row], seed: str, live_names: Collection[str]) -> str:
    """The docker stack a seed's cells need: `lean` when one is a live test or a row grades the state store, else `none`."""
    graded = [r for r in rows if r.seed == seed and not r.na]
    live = any(c.removeprefix(LIB) in live_names for r in graded for c in r.cells)
    return "lean" if live or any(r.engine == STATE_ENGINE for r in graded) else "none"


def cost(rows: Sequence[Row], seed: str) -> float:
    """Estimated work minutes of one seed: its own control and switched run over its cells."""
    return SEED_MIN + CELL_MIN * len(cells_of(rows, seed))


def deal(weights: Mapping[str, int], n: int) -> list[list[str]]:
    """The seeds dealt into `n` shards, heaviest first into the lightest one; deterministic, each seed in exactly one shard."""
    from dev.release_oracle.halves import slice_modules
    return slice_modules(weights, n)


def plan(rows: Sequence[Row], devbox_tests: Collection[str], live_names: Collection[str], only: Sequence[str] = ()) -> list[dict]:
    """The hosted shards `[{index, seeds, stack, minutes}]`: lib-only seeds and live seeds are separate pools, each cut by `SHARD_TARGET_MIN` of estimated work; `only` narrows to named hosted seeds and a name that is not one is an error."""
    placed = homes(rows, devbox_tests)
    mine = sorted(s for s, (half, _) in placed.items() if half == HOSTED)
    if only:
        bad = [s for s in only if s not in placed or placed[s][0] != HOSTED]
        if bad:
            raise SystemExit("not hosted seeds: " + "; ".join(f"{s} ({placed[s][1] if s in placed else 'not in the ledger'})" for s in bad))
        mine = sorted(set(only))
    pools = {st: [s for s in mine if stack_of(rows, s, live_names) == st] for st in ("lean", "none")}
    out: list[dict] = []
    for stack, seeds in pools.items():
        if not seeds:
            continue
        weights = {s: round(cost(rows, s) * 100) for s in seeds}
        n = max(1, math.ceil(sum(weights.values()) / 100 / SHARD_TARGET_MIN))
        for names in deal(weights, n):
            if names:
                work = sum(weights[s] for s in names) / 100
                out.append({"index": len(out), "stack": stack, "seeds": ",".join(names), "minutes": round(work + SETUP_MIN[stack])})
    return out


def coverage_problems(shards: Sequence[dict], expected: Sequence[str]) -> list[str]:
    """The expected seeds no shard holds, the ones in two shards and the ones no ledger placement expects."""
    held = [s for sh in shards for s in sh["seeds"].split(",") if s]
    return ([f"seed {s} is in no shard" for s in sorted(set(expected) - set(held))]
            + [f"seed {s} is in {held.count(s)} shards" for s in sorted({s for s in held if held.count(s) > 1})]
            + [f"seed {s} is in a shard but the ledger does not place it here" for s in sorted(set(held) - set(expected))])


def load() -> tuple[list[Row], set[str], set[str]]:
    """The ledger's rows, the bare names of the devbox-only live tests and of every live test."""
    from dev.release_oracle import live_modules
    table = live_modules.live_test_table()
    return parse_manifest(yaml.safe_load(MANIFEST.read_text())), live_modules.part_tests(DEVBOX, table), {name for _, name, _ in table}


def expected(rows: Sequence[Row], devbox_tests: Collection[str], only: Sequence[str] = ()) -> list[str]:
    """The hosted seeds this run owes: every hosted-homed seed, or the named ones."""
    placed = homes(rows, devbox_tests)
    return sorted(only) if only else sorted(s for s, (half, _) in placed.items() if half == HOSTED)


def self_test() -> int:
    """The placement, the dealer and the coverage guard on a synthetic ledger and on the real one; each guard is shown to refuse its mutant."""
    rows = [
        Row("a", "postgres", switch="sa", cells=("t_live",)), Row("a", "mysql", na="x" * 40),
        Row("b", "postgres", switch="sb", cells=(f"{LIB}t_lib",)),
        Row("c", "postgres", patch="c/x.patch", cells=("t_live",)),
        Row("d", "postgres", switch="sd", cells=("t_snow",)),
        Row("e", "postgres", switch="se", cells=("t_live",), platform="macos"),
        Row("f", "postgres", switch="sf", cells=("t_live", f"{LIB}t_lib")),
        Row("g", "postgres", na="y" * 40),
        Row("h", "postgres-state", switch="sh", cells=(f"{LIB}t_lib",)),
    ]
    devbox_tests, live_names = {"t_snow"}, {"t_live", "t_snow"}
    placed = homes(rows, devbox_tests)
    assert {s: h for s, (h, _) in placed.items()} == {"a": HOSTED, "b": HOSTED, "c": DEVBOX, "d": DEVBOX, "e": DEVBOX, "f": HOSTED, "g": HOSTED, "h": HOSTED}, placed
    assert all(placed[s][1] for s in "cde"), "a devbox seed names why"
    shards = plan(rows, devbox_tests, live_names)
    assert not coverage_problems(shards, expected(rows, devbox_tests)), coverage_problems(shards, expected(rows, devbox_tests))
    assert {sh["stack"] for sh in shards} == {"lean", "none"} and [sh["index"] for sh in shards] == list(range(len(shards)))
    assert [sh["seeds"] for sh in shards if sh["stack"] == "none"] == ["b,g"], "lib-only and all-N/A seeds share the stackless shard"
    assert any("h" in sh["seeds"].split(",") for sh in shards if sh["stack"] == "lean"), "a seed that grades the state store needs the stack's Postgres"
    assert plan(rows, devbox_tests, live_names, ["a"])[0]["seeds"] == "a"
    for bad in ("c", "nope"):
        try:
            plan(rows, devbox_tests, live_names, [bad])
        except SystemExit:
            continue
        raise AssertionError(f"a non-hosted seed {bad!r} was planned")
    assert any("no shard" in p for p in coverage_problems(shards[:-1], expected(rows, devbox_tests))), "a lost shard goes unnoticed"
    assert any("2 shards" in p for p in coverage_problems([*shards, shards[0]], expected(rows, devbox_tests))), "a doubled shard goes unnoticed"
    assert any("does not place" in p for p in coverage_problems(shards, ["a"])), "a seed the ledger does not place here goes unnoticed"
    many = [Row(f"s{i:03}", "postgres", switch=f"w{i}", cells=tuple(f"t{j}" for j in range(30))) for i in range(40)]
    big = plan(many, set(), {f"t{j}" for j in range(30)})
    assert len(big) > 1 and not coverage_problems(big, expected(many, set())), "a heavy ledger is cut into several shards"
    assert plan(many, set(), {f"t{j}" for j in range(30)}) == big, "the deal is deterministic"
    real, devbox, live = load()
    placed = homes(real, devbox)
    hosted = [s for s, (h, _) in placed.items() if h == HOSTED]
    assert hosted and set(placed) == {r.seed for r in real}, "every ledger seed has a home"
    real_plan = plan(real, devbox, live)
    assert not coverage_problems(real_plan, expected(real, devbox)), "the real plan drops or doubles a seed"
    print(f"self-test ok: shards — {len(hosted)} hosted seed(s) in {len(real_plan)} shard(s), {len(placed) - len(hosted)} on the devbox")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """CLI: print the workflow's `shards=` matrix and `expected=` seed list, or run the self-test."""
    ap = argparse.ArgumentParser(prog="shards")
    ap.add_argument("--seeds", default="", help="comma-separated hosted seeds to run instead of all")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--table", action="store_true", help="print the placement of every seed")
    ns = ap.parse_args(argv)
    if ns.self_test:
        return self_test()
    only = [s for s in ns.seeds.split(",") if s]
    rows, devbox, live = load()
    if ns.table:
        for s, (half, why) in homes(rows, devbox).items():
            print(f"{s}\t{half}\t{why}")
        return 0
    problems = switch_problems(rows, complete=True) + patch_form_problems(rows, PATCH_SEEDS)
    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 1
    shards = plan(rows, devbox, live, only)
    print("shards=" + json.dumps(shards, separators=(",", ":")))
    print("expected=" + ",".join(expected(rows, devbox, only)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
