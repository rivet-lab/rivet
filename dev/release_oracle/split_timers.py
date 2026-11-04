"""Offline proof that every stage with an internal wall-clock records it as a split, and that the timings record keeps the splits whole."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

from . import core
from .core import Ledger, run_lanes


def _names(led: Ledger, prefix: str = "") -> list[str]:
    """The split names a ledger holds, in the order they were recorded."""
    return [n for n, _ in led._splits if n.startswith(prefix)]


def _record_self_test() -> None:
    """A split is kept uncut and summed per name, beside a `top_spans_min` the existing readers still parse."""
    spans = [(f"span {i}", float(i)) for i in range(60)]
    splits = [(f"split {i}", 60.0) for i in range(60)] + [("split 0", 60.0)]
    with tempfile.TemporaryDirectory() as tmp:
        hist = Path(tmp) / "timings.jsonl"
        core.record_timings(hist, [("A", 60.0)], spans, "RELEASE-READY", 1, 0, splits=splits)
        core.record_timings(hist, [("A", 60.0)], spans, "RELEASE-READY", 1, 0)
        first, second = (json.loads(x) for x in hist.read_text().splitlines())
    assert len(first["top_spans_min"]) == 40 and first["phases_min"] == {"A": 1.0}, first
    assert len(first["splits_min"]) == 60 and first["splits_min"]["split 0"] == 2.0, first["splits_min"]
    assert second["splits_min"] == {}, second
    parent, child = Ledger(colour=False), Ledger(colour=False)
    child._buf = []
    with child.split("engine bring-up"):
        pass
    child.flush_into(parent)
    assert _names(parent) == ["engine bring-up"], "a buffered child's splits travel to its parent"


def _lane_self_test() -> None:
    """Every lane of `run_lanes` is a split named by the stage and the lane key, a raising lane included."""
    led = Ledger(colour=False)
    led._buf = []
    led.phase("Upgrade from a field state — detail")

    def boom(sub: Ledger) -> None:
        raise RuntimeError("lane blew up")

    try:
        run_lanes(led, [("k1", lambda sub: None), ("k2", boom), ("k1", lambda sub: None), (None, lambda sub: None)])
    except RuntimeError:
        pass
    assert sorted(_names(led)) == sorted(f"lane Upgrade from a field state / {k}" for k in ("k1", "k2", "None")), led._splits


def _perf_and_rss_self_test() -> None:
    """One split per perf section and per flat-RSS engine, batch and CDC."""
    from . import guarantees, perf, upgrade

    sections = ("_aa", "_batch", "_off_happy_path", "_cdc", "_conns", "_batch_conns", "_load", "_ch_load", "_cdc20")
    led = Ledger(colour=False)
    led._buf = []
    with ExitStack() as st:
        st.enter_context(mock.patch.object(perf, "_require_prev_binary", lambda *a, **k: Path("prev")))
        for name in sections:
            st.enter_context(mock.patch.object(perf, name, lambda *a, **k: None))
        perf.verify_perf_regression(led)
    assert len(_names(led, "perf ")) == len(sections) and "perf aa" in _names(led), led._splits

    led = Ledger(colour=False)
    led._buf = []
    with ExitStack() as st:
        st.enter_context(mock.patch.object(guarantees, "ENGINES", ("postgres", "mongo")))
        st.enter_context(mock.patch.object(upgrade, "CDC_ENGINES", ("postgres",)))
        st.enter_context(mock.patch.object(guarantees, "_batch_rss", lambda *a, **k: None))
        st.enter_context(mock.patch.object(guarantees, "_cdc_rss", lambda *a, **k: None))
        guarantees.verify_flat_rss(led)
    assert _names(led, "flat rss ") == ["flat rss postgres batch", "flat rss mongo batch", "flat rss postgres cdc"], led._splits


def _upgrade_self_test() -> None:
    """One split per engine leg of the upgrade continuity (each state store, pinned or not), the load leg and each CDC leg."""
    from . import upgrade

    def leg(name: str):
        def run_leg(*a, **k) -> None:
            return None
        run_leg.__name__ = name
        return run_leg

    led = Ledger(colour=False)
    led._buf = []
    env = {"RIVET_ORACLE_POSTGRES_URL": "postgresql://x", "RIVET_GATE_STATE_URL": "postgresql://s",
           "RIVET_CDC_POSTGRES_URL": "postgresql://c"}
    with ExitStack() as st:
        st.enter_context(mock.patch.dict(os.environ, env))
        st.enter_context(mock.patch.object(upgrade, "_require_prev_binary", lambda *a, **k: Path("prev")))
        st.enter_context(mock.patch.object(upgrade, "_migration_carrier", lambda *a, **k: None))
        st.enter_context(mock.patch.object(upgrade, "ENGINES", ("postgres",)))
        st.enter_context(mock.patch.object(upgrade, "CDC_ENGINES", ("postgres",)))
        st.enter_context(mock.patch.object(upgrade, "CDC_URL_VARS", {}))
        st.enter_context(mock.patch.object(upgrade, "isolate_state_db", lambda *a, **k: "postgresql://fresh"))
        st.enter_context(mock.patch.object(upgrade, "warehouse_lane_cells", lambda *a, **k: []))
        st.enter_context(mock.patch.object(upgrade, "_load_leg", lambda *a, **k: None))
        st.enter_context(mock.patch.object(upgrade, "_cdc_leg", lambda *a, **k: None))
        legs = ("_cursor_leg", "_crash_leg", "_way_back_leg", "_no_way_back_leg")
        for name in legs:
            st.enter_context(mock.patch.object(upgrade, name, leg(name)))
        upgrade.verify_upgrade_continuity(led)
    want = {f"upgrade postgres {store} {name.strip('_')}" for store in ("sqlite", "pg") for name in legs}
    want |= {"upgrade postgres load", "upgrade cdc postgres"}
    assert set(_names(led, "upgrade ")) == want and len(_names(led, "upgrade ")) == len(want), led._splits


def _engine_self_test() -> None:
    """One engine x version records its bring-up, seed and golden as three splits."""
    from . import __main__ as gate, scenarios, sentinels

    led = Ledger(colour=False)
    led._buf = []
    saved = dict(core._BLESSING)
    core.set_blessing(local=True, cdc=False)
    try:
        with ExitStack() as st:
            st.enter_context(mock.patch.object(gate, "bring_up", lambda *a, **k: "postgresql://x"))
            st.enter_context(mock.patch.object(gate, "seed_engine", lambda *a, **k: ""))
            st.enter_context(mock.patch.object(scenarios, "sc_verdicts", lambda *a, **k: None))
            st.enter_context(mock.patch.object(scenarios, "sc_integrity_types", lambda *a, **k: None))
            st.enter_context(mock.patch.object(sentinels, "sc_sentinels", lambda *a, **k: None))
            gate._run_one_version(led, argparse.Namespace(keep=True), "postgres", "16 img 5432")
    finally:
        core._BLESSING.update(saved)
    assert _names(led, "postgres 16:") == ["postgres 16: bring-up", "postgres 16: seed", "postgres 16: golden"], led._splits


def _live_modules_self_test() -> None:
    """Every nextest leg of the live-modules stage is a split and a kept log; the stage asks for both on each invocation."""
    from . import live_modules, scenarios

    def fake_nextest(path, expr, env=None, threads=None):
        path.write_text("nextest output")
        if env and env.get("X"):
            return {"m::a": "PASS"}, 1, {}, {}
        return {"m::a": "PASS"}, 1, {"m::a": "set X"}, {}

    with tempfile.TemporaryDirectory() as tmp:
        kept = Path(tmp) / "gate-failures"
        for keep, files in ((True, 2), (False, 0)):
            led = Ledger(colour=False)
            led._buf = []
            with ExitStack() as st:
                st.enter_context(mock.patch.object(core, "GATE_FAILURES", kept))
                st.enter_context(mock.patch.object(scenarios, "_WORK", Path(tmp)))
                st.enter_context(mock.patch.object(scenarios, "have", lambda tool: True))
                st.enter_context(mock.patch.object(scenarios, "nextest_live", fake_nextest))
                scenarios._run_live_modules(led, "live", "live modules", "p", ["m"], again=("X", {"X": "1"}), keep=keep)
            assert _names(led) == ["nextest live modules", "nextest live modules again"], led._splits
            assert len(list(kept.glob("*"))) == files, (keep, list(kept.glob("*")))
            for f in kept.glob("*"):
                f.unlink()
    calls = []
    with ExitStack() as st:
        st.enter_context(mock.patch.object(live_modules, "_run_live_modules", lambda *a, **k: calls.append(k)))
        st.enter_context(mock.patch.object(live_modules, "exclusive_tests", lambda modules=None: ["a_test"]))
        live_modules.verify_live_modules(Ledger(colour=False))
    assert len(calls) == 2 and all(c.get("keep") for c in calls), calls


def _coverage_self_test() -> None:
    """The llvm-cov battery and the classification of its lcov are two splits of the live-only coverage stage."""
    from . import scenarios

    led = Ledger(colour=False)
    led._buf = []
    with tempfile.TemporaryDirectory() as tmp, ExitStack() as st:
        st.enter_context(mock.patch.object(scenarios, "_WORK", Path(tmp)))
        st.enter_context(mock.patch.object(scenarios, "have", lambda tool: True))
        st.enter_context(mock.patch.object(scenarios, "run", lambda argv, **k: core.Proc(list(argv), 0, "confirmed 1", "")))
        scenarios.verify_live_only_coverage(led)
    assert _names(led) == ["live-only coverage llvm-cov battery", "live-only coverage classify"], led._splits


def _self_test() -> None:
    """Splits are recorded by every timed stage site, and the timings line keeps them."""
    _record_self_test()
    _lane_self_test()
    _perf_and_rss_self_test()
    _upgrade_self_test()
    _engine_self_test()
    _live_modules_self_test()
    _coverage_self_test()
    print("self-test ok: perf sections, flat-RSS engines, lanes, upgrade legs, engine bring-up/seed/golden, nextest legs and the live-only coverage battery each record a split")
