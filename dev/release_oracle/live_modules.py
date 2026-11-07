"""Every live_suite module the gate has not already run, through the gate's release binary.

The gate used to name its live tests by hand, cell by cell, so a regression test written for
a defect found later (a keyset anchor resumed by the wrong runner shape, a split re-run, a
ClickHouse idle load) never entered it. This cell DERIVES the set: every `mod` in
tests/live_suite.rs, minus the modules and tests the dedicated cells already ran this gate
(they register what they run), minus the named exclusions below — each with its reason.
It runs last so the registry is complete.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from . import live_owners
from .core import RAN_LIVE_MODULES, RAN_LIVE_TESTS, ROOT, Ledger, SkipCause
from .scenarios import _run_live_modules, cfg

__all__ = ["verify_live_modules", "live_suite_modules", "exclusive_tests", "EXCLUDED", "select_modules", "engines_arg"]

#: Modules the derived run skips, each for a reason (a stale entry fails the offline guard).
EXCLUDED = {
    "common": "the shared test helpers — no tests of its own",
}


@dataclass(frozen=True)
class Selection:
    """Which live modules an `--engines` selection runs: engine modules, engine-less modules, and what each other engine left out."""

    engines: tuple[str, ...]
    run: tuple[str, ...]
    engineless: tuple[str, ...]
    left_out: dict[str, tuple[str, ...]]

    @property
    def narrowed(self) -> bool:
        """Whether any engine was named."""
        return bool(self.engines)

    def line(self) -> str:
        """The report line: the selection is not a gate, what is excluded, and the engine-less modules by name."""
        out = "; ".join(f"{e} {len(m)}" for e, m in self.left_out.items())
        return (f"live modules NARROWED to {', '.join(self.engines)}: not a gate verdict, release.py refuses it; the other engines are not graded here "
                f"(excluded modules per engine: {out}); {len(self.engineless)} engine-less module(s) RUN under any selection: "
                f"{', '.join(self.engineless) or 'none'}")


def select_modules(engines: list[str], modules: list[str], owners: dict[str, set[str]], known: list[str]) -> Selection:
    """The `modules` `engines` run: those naming a selected source engine plus those naming none; no engine runs all. An unknown engine or a module with no owner entry raises."""
    if unknown := [e for e in engines if e not in known]:
        raise ValueError(f"--engines names {unknown}, the gate's engines are {sorted(known)}")
    sources = set(live_owners.SOURCE_ENGINES)
    run, engineless = [], []
    left_out: dict[str, list[str]] = {e: [] for e in known if engines and e not in engines}
    for m in sorted(modules):
        if m not in owners:
            raise ValueError(f"live module {m} has no tests/live/{m}.rs to say which engine it exercises")
        mine = owners[m] & sources
        if not mine:
            engineless.append(m)
        elif not engines or mine & set(engines):
            run.append(m)
        else:
            for e in mine:
                left_out.setdefault(e, []).append(m)
    return Selection(tuple(engines), tuple(sorted(run + engineless)), tuple(engineless), {e: tuple(m) for e, m in left_out.items()})


def engines_arg(spec: str) -> list[str]:
    """The engines `--engines` names, each one a gate engine; an unknown name raises ValueError."""
    engines = [e for e in spec.split(",") if e]
    select_modules(engines, [], {}, cfg("engines").split())
    return engines


def live_suite_modules() -> list[str]:
    """Every module tests/live_suite.rs declares."""
    text = (ROOT / "tests" / "live_suite.rs").read_text()
    return re.findall(r"^mod ([a-z0-9_]+);", text, re.M)


#: What a `live+exclusive` test runs under, one at a time with nothing else on the stand.
EXCLUSIVE_ENV = {"RIVET_TEST_EXCLUSIVE": "1"}


def exclusive_tests(modules: set[str] | None = None) -> list[str]:
    """Bare names of live tests marked `live+exclusive` (each needs the stand to itself), of `modules` when given."""
    names = []
    for f in sorted((ROOT / "tests" / "live").glob("*.rs")):
        if modules is not None and f.stem not in modules:
            continue
        names += re.findall(r'#\[ignore = "live\+exclusive[^\n]*\n(?:\s*#\[[^\n]*\n)*\s*(?:pub )?fn ([a-z0-9_]+)',
                            f.read_text())
    return names


def derived_filter(modules: list[str], ran_modules: set[str], ran_tests: set[str],
                   owners: frozenset[str] = frozenset()) -> tuple[list[str], str]:
    """The modules left to run and the nextest filter over them, minus tests already run and, under `owners`, tests named for another engine."""
    left = [m for m in modules if m not in EXCLUDED and m not in ran_modules]
    expr = live_owners.narrow_foreign(f"test(/^({'|'.join(left)})::/)", left, set(owners))
    if ran_tests:
        names = "|".join(sorted(re.escape(t) for t in ran_tests))
        expr = f"({expr}) - test(/::({names})$/)"
    return left, expr


def live_modules_env(state: str) -> tuple[dict[str, str], tuple[str, dict[str, str]] | None]:
    """The env this stage gives every Rig cell under the gate's Postgres state `state` (or ""), and the leg a self-skip naming RIVET_GATE_STATE_URL runs again under; seeded recall runs its cells in the same."""
    # A warehouse project and bucket for the cloud tests, a Postgres state URL for the ones that share
    # one, and NO ambient RIVET_STATE_URL, which would move every SQLite-reading test onto Postgres
    # (the batch_resume lesson).
    env = {"RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": "", "RIVET_TEST_STATE_URL": state}
    # A test that grades the Postgres state alone says so by its self-skip; it runs again on the gate's.
    again = ("RIVET_GATE_STATE_URL", {"RIVET_STATE_URL": state, "RIVET_GATE_STATE_URL": state}) if state.startswith("postgres") else None
    return env, again


def verify_live_modules(led: Ledger, engines: list[str] | None = None) -> None:
    """Run every live_suite module and test no dedicated cell ran, those of `engines` only when named; one ledger row per test."""
    engines = engines or []
    modules = [m for m in live_suite_modules() if m not in EXCLUDED]
    sel = select_modules(engines, modules, live_owners.module_owners(), cfg("engines").split())
    owners = frozenset(sel.engines)
    if sel.narrowed:
        led.skip(sel.line())
        for e, mods in sel.left_out.items():
            led.skipped(e, "-", "live modules", "-", f"--engines {','.join(sel.engines)}: {len(mods)} {e} live module(s) not run",
                        cause=SkipCause.REDUCED)
    left, expr = derived_filter(list(sel.run), set(RAN_LIVE_MODULES), set(RAN_LIVE_TESTS), owners)
    excl = [n for n in exclusive_tests(set(sel.run) if sel.narrowed else None) if n not in RAN_LIVE_TESTS]
    env, again = live_modules_env(os.environ.get("RIVET_CDC_STATE_URL") or os.environ.get("RIVET_GATE_STATE_URL") or "")
    _run_live_modules(led, "live", "live modules",
                      f"every other live_suite module ({len(left)}), derived from tests/live_suite.rs",
                      left, env=env, again=again, keep=True,
                      expr=f"({expr}) - test(/::({'|'.join(excl)})$/)" if excl else expr)
    if excl:
        _run_live_modules(led, "live_modules", "live modules (exclusive)",
                          f"tests that need the stand to themselves ({len(excl)}), one at a time",
                          left, env={**env, **EXCLUSIVE_ENV}, threads=1, again=again, keep=True,
                          expr=live_owners.narrow_foreign("(" + " | ".join(f"test(/::{n}$/)" for n in excl) + ")", left, set(owners)))


def self_test() -> None:
    """With fake owners and no stand: `--engines mysql` runs the mysql and engine-less modules only, no selector runs all, a bad name or an ownerless module is an error."""
    import contextlib
    from unittest import mock

    owners = {"pg_a": {"postgres"}, "my_a": {"mysql"}, "my_b": {"mysql", "bigquery"}, "ms_a": {"mssql"}, "mix": {"postgres", "mysql"},
              "mo_a": {"mongo"}, "or_a": {"oracle"}, "cloud": {"snowflake"}, "plain": set()}
    known = ["postgres", "mysql", "mssql", "mongo", "oracle"]
    mods = sorted(owners)
    one = select_modules(["mysql"], mods, owners, known)
    assert one.run == ("cloud", "mix", "my_a", "my_b", "plain") and one.engineless == ("cloud", "plain"), one
    assert one.left_out == {"postgres": ("pg_a",), "mssql": ("ms_a",), "mongo": ("mo_a",), "oracle": ("or_a",)}, one.left_out
    both = select_modules(["mysql", "mssql"], mods, owners, known)
    assert "ms_a" in both.run and "pg_a" not in both.run and set(both.left_out) == {"postgres", "mongo", "oracle"}, both
    every = select_modules([], mods, owners, known)
    assert every.run == tuple(mods) and every.left_out == {} and not every.narrowed, every
    for bad, why in ((lambda: select_modules(["mysq"], mods, owners, known), "unknown engine"),
                     (lambda: select_modules([], [*mods, "ghost"], owners, known), "module without an owner entry")):
        try:
            bad()
        except ValueError:
            continue
        raise AssertionError(f"{why} was accepted")
    gate_engines = cfg("engines").split()
    assert set(gate_engines) <= set(live_owners.SOURCE_ENGINES), f"a gate engine no live module can be tied to: {gate_engines}"

    def stage(engines: list[str] | None) -> tuple[list[str], str, list]:
        """Run verify_live_modules over the fake owners; return the modules handed to nextest, the report line and the ledger rows."""
        seen: list[list[str]] = []
        led = Ledger(colour=False)
        led._buf = []
        with contextlib.ExitStack() as st:
            st.enter_context(mock.patch.object(live_owners, "module_owners", lambda: owners))
            st.enter_context(mock.patch.object(live_owners, "kept_by_body", lambda *a: []))
            st.enter_context(mock.patch(f"{__name__}.live_suite_modules", lambda: mods))
            st.enter_context(mock.patch(f"{__name__}.cfg", lambda *q: " ".join(known)))
            st.enter_context(mock.patch(f"{__name__}.exclusive_tests", lambda modules=None: []))
            st.enter_context(mock.patch(f"{__name__}._run_live_modules", lambda led, s, label, phase, left, **k: seen.append(list(left))))
            verify_live_modules(led, engines)
        return seen[0], "\n".join(led._buf), led.cells

    ran, said, cells = stage(["mysql"])
    assert ran == list(one.run), ran
    for e in ("postgres", "mssql", "mongo", "oracle"):
        assert e in said, f"the narrowed run does not print that {e} is excluded: {said}"
    assert "cloud, plain" in said and "RUN under any selection" in said and "not a gate verdict" in said, said
    assert sorted(c.engine for c in cells if c.cause == SkipCause.REDUCED) == ["mongo", "mssql", "oracle", "postgres"], cells
    assert select_modules(["mysql"], ["my_a"], owners, known).line().endswith("none"), "an empty engine-less set is said, not left blank"
    ran, said, cells = stage(None)
    assert ran == mods and "NARROWED" not in said and not cells, (ran, said)
    left, expr = derived_filter(["live_cdc", "live_mssql_variant"], set(), set(), frozenset({"mysql"}))
    assert left == ["live_cdc", "live_mssql_variant"] and "mssql" in expr and expr.startswith("(test(/^(live_cdc|"), expr
    assert derived_filter(["live_cdc"], set(), set())[1] == "test(/^(live_cdc)::/)", "no selector narrows no test"
    live_owners.self_test()
    print("self-test ok: --engines runs the named engines' live modules and the engine-less ones, prints the rest as excluded, and no selector runs all")
