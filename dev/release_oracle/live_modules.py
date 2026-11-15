"""Every live_suite module the gate has not already run, through the gate's release binary.

The gate used to name its live tests by hand, cell by cell, so a regression test written for
a defect found later (a keyset anchor resumed by the wrong runner shape, a split re-run, a
ClickHouse idle load) never entered it. This cell DERIVES the set: every `mod` in
tests/live_suite.rs, minus the modules and tests the dedicated cells already ran this gate
(they register what they run), minus the named exclusions below — each with its reason.
It runs last so the registry is complete.
"""

from __future__ import annotations

import ast
import functools
import os
import re
import sys
from dataclasses import dataclass

from . import live_needs, live_owners
from .core import DEVBOX, HOSTED, RAN_LIVE_MODULES, RAN_LIVE_TESTS, ROOT, Ledger, SkipCause
from .halves import NEED_HALF, STAGE_HALF, devbox_only, needs_problems, slice_modules
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


#: A live test's `#[ignore = "<reason>"]` followed by its fn: (reason, fn name).
IGNORED_TEST = re.compile(r'^\s*#\[ignore = "([^"\n]*)"\]\s*(?:(?:#\[|//)[^\n]*\n\s*)*(?:pub )?(?:async )?fn ([a-z0-9_]+)', re.M)
IGNORE_ATTR = re.compile(r'^\s*#\[ignore = "', re.M)

#: The calls through which a stage names the live tests or modules it runs itself.
REGISTRARS = frozenset({"_drive_live_tests", "run_rig_tests", "_run_live_modules"})


@functools.cache
def _need_map() -> dict[tuple[str, str], frozenset[str]]:
    """The needs each live test carries, scanned once per process."""
    return live_needs.test_needs()


def live_test_table() -> list[tuple[str, str, bool]]:
    """Every ignored live test as (module, fn name, runs in the devbox half), read from tests/live/*.rs: a test goes to the devbox when its reason names a topology only it holds or its call graph carries a need only it meets."""
    problems = needs_problems(live_needs.need_variants())
    if problems:
        raise ValueError("; ".join(problems))
    needs = _need_map()
    return [(f.stem, name, devbox_only(reason, needs.get((f.stem, name), ())))
            for f in sorted((ROOT / "tests" / "live").glob("*.rs")) for reason, name in IGNORED_TEST.findall(f.read_text())]


def part_tests(part: str, table: list[tuple[str, str, bool]] | None = None) -> set[str]:
    """Bare names of the live tests the `part` ("hosted" or "devbox") half runs."""
    return {name for _, name, devbox in (live_test_table() if table is None else table) if devbox is (part == DEVBOX)}


def _strings(node: ast.AST, consts: dict[str, set[str]]) -> set[str]:
    """Every string literal under `node`, with a bare name replaced by the strings of the constant it names."""
    out: set[str] = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            out.add(n.value)
        elif isinstance(n, ast.Name) and n.id in consts:
            out |= consts[n.id]
    return out


def dedicated_live(sources: dict[str, str] | None = None) -> tuple[set[str], set[str]]:
    """(modules, tests) the gate's dedicated stages run, read from the harness's own calls to the registrars: a string an argument carries that names a live module or test."""
    if sources is None:
        sources = {f.name: f.read_text() for f in sorted(ROOT.joinpath("dev", "release_oracle").glob("*.py")) if f.name != "live_modules.py"}
    trees = {name: ast.parse(text) for name, text in sources.items()}
    consts: dict[str, set[str]] = {}
    for tree in trees.values():
        for n in tree.body:
            if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name):
                consts.setdefault(n.targets[0].id, set()).update(_strings(n.value, {}))
    named: set[str] = set()
    for tree in trees.values():
        for n in ast.walk(tree):
            if not isinstance(n, ast.Call):
                continue
            f = n.func
            fname = f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else ""
            registry = isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id in ("RAN_LIVE_TESTS", "RAN_LIVE_MODULES")
            if fname in REGISTRARS or registry:
                named |= _strings(n, consts)
    modules = {m for m in live_suite_modules() if m in named}
    tests = {name for _, name, _ in live_test_table() if name in named}
    return modules, tests


def dedicated_expr(modules: list[str]) -> str:
    """The nextest filter of a dedicated stage over whole `modules`: their hosted tests only, the devbox ones being the devbox part's."""
    return part_filter(" | ".join(f"test(/^{m}::/)" for m in modules), HOSTED, live_test_table())


def dedicated_stage_problems(sources: dict[str, str] | None = None) -> list[str]:
    """A stage that runs whole live modules (a `_run_live_modules` call with no `expr`) and is not placed in the hosted half: it filters its modules to the hosted tests, so another half would leave them to no one."""
    if sources is None:
        sources = {f.name: f.read_text() for f in sorted(ROOT.joinpath("dev", "release_oracle").glob("*.py")) if f.name != "live_modules.py"}
    mods = set(live_suite_modules())
    out = []
    for name, text in sources.items():
        for n in ast.walk(ast.parse(text)):
            if not (isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "_run_live_modules") or any(k.arg == "expr" for k in n.keywords):
                continue
            label = n.args[2].value if len(n.args) > 2 and isinstance(n.args[2], ast.Constant) else None
            named = sorted(mods & _strings(n.args[4], {})) if len(n.args) > 4 else []
            if named and STAGE_HALF.get(label) != HOSTED:
                out.append(f"{name}: stage {label!r} runs {named} whole and is not placed in the hosted half")
    return out


def verify_registry_covered(led: Ledger) -> None:
    """A test or module a stage registered at run time that `dedicated_live` did not read from source is a FAIL: the static set is what the other half and the live slices subtract."""
    modules, tests = dedicated_live()
    missed = sorted((set(RAN_LIVE_TESTS) - tests) | (set(RAN_LIVE_MODULES) - modules))
    if missed:
        led.failed("-", "-", "live_registry", "-", f"live registry: {len(missed)} registration(s) the static dedicated set does not hold: {', '.join(missed[:8])}",
                   "static dedicated set incomplete")
        return
    led.passed("-", "-", "live_registry", "-", f"live registry: {len(RAN_LIVE_TESTS)} test(s) and {len(RAN_LIVE_MODULES)} module(s) registered, all held by the static dedicated set")


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


def part_filter(expr: str, part: str, table: list[tuple[str, str, bool]]) -> str:
    """`expr` narrowed to the tests `part` runs: the devbox part keeps the devbox-only tests, the hosted part drops them."""
    names = "|".join(re.escape(n) for n in sorted(part_tests(DEVBOX, table)))
    return f"({expr}) & test(/::({names})$/)" if part == DEVBOX else f"({expr}) - test(/::({names})$/)"


def part_plan(table: list[tuple[str, str, bool]], modules: list[str], part: str, gone: set[str], dedicated_modules: set[str],
              shard: tuple[int, int] | None = None) -> dict[str, list[str]]:
    """The tests each of `modules` runs in `part`, minus the tests in `gone` and, in the hosted part, the dedicated modules (their hosted tests run in their own stage; their devbox tests stay here); under `shard` (i, n) the i-th slice of those modules only."""
    own = part_tests(part, table)
    skipped_modules = dedicated_modules if part == HOSTED else set()
    plan = {m: sorted(name for mod, name, _ in table if mod == m and name in own and name not in gone) for m in modules if m not in skipped_modules}
    plan = {m: names for m, names in plan.items() if names}
    if shard is not None:
        keep = slice_modules({m: len(names) for m, names in plan.items()}, shard[1])[shard[0] - 1]
        plan = {m: plan[m] for m in keep}
    return plan


def coverage_problems(table: list[tuple[str, str, bool]], modules: list[str], dedicated_modules: set[str], dedicated_tests: set[str], slices: int) -> list[str]:
    """Live tests that run in no place or in two: a dedicated test or a hosted test of a dedicated module must run in no live part, every other one in exactly one of the devbox part and the hosted slices."""
    runs: dict[tuple[str, str], int] = {}
    plans = [part_plan(table, modules, DEVBOX, set(dedicated_tests), dedicated_modules)] + [
        part_plan(table, modules, HOSTED, set(dedicated_tests), dedicated_modules, (i, slices)) for i in range(1, slices + 1)]
    for plan in plans:
        for m, names in plan.items():
            for n in names:
                runs[(m, n)] = runs.get((m, n), 0) + 1
    out = []
    for mod, name, devbox in table:
        if mod not in modules:
            continue
        want = 0 if name in dedicated_tests or (mod in dedicated_modules and not devbox) else 1
        if runs.get((mod, name), 0) != want:
            out.append(f"{mod}::{name} runs {runs.get((mod, name), 0)} time(s) in the live parts, wants {want}")
    return out


#: 2026-11-14: a hosted slice over this many minutes lost its runner twice (run 43633356678, slice 2 of 3, 902 tests); closes when a slice is cut by measured minutes instead of test count.
SLICE_CEILING_MIN = 50
#: 2026-11-14: every slice builds the release binary from nothing; measured 16.4-16.8 min on slices 1 and 3 of run 43633356678.
SLICE_BUILD_MIN = 17.0
#: 2026-11-14: the slowest measured hosted rate, slice 3 of run 43633356678: 45.6 min over 902 tests; closes with a per-module cost read from the slices' nextest logs.
SLICE_SEC_PER_TEST = 3.1


def slice_minutes(tests: int) -> float:
    """The minutes a hosted slice of `tests` live tests takes: the build, then each test at the slowest measured rate."""
    return SLICE_BUILD_MIN + tests * SLICE_SEC_PER_TEST / 60


def hosted_slice_plans(slices: int) -> list[dict[str, list[str]]]:
    """The tests each live slice 1..`slices` runs, planned exactly as `verify_live_modules` plans a slice of an unnarrowed run."""
    modules = [m for m in live_suite_modules() if m not in EXCLUDED]
    run = select_modules([], modules, live_owners.module_owners(), []).run
    dedicated_modules, dedicated_tests = dedicated_live()
    return [part_plan(live_test_table(), list(run), HOSTED, set(dedicated_tests), set(dedicated_modules), (i, slices)) for i in range(1, slices + 1)]


def slice_budget_problems(plans: list[dict[str, list[str]]], ceiling: float = SLICE_CEILING_MIN) -> list[str]:
    """One line per slice whose estimated minutes exceed `ceiling`: the planner refuses to start a slice a runner is not known to survive."""
    out = []
    for i, plan in enumerate(plans, 1):
        tests = sum(len(v) for v in plan.values())
        if (m := slice_minutes(tests)) > ceiling:
            out.append(f"live slice {i}/{len(plans)}: {tests} tests in {len(plan)} modules estimate {m:.0f} min, over the {ceiling:.0f} min ceiling; raise LIVE_SLICES in gate-hosted.yml")
    return out


def plan_matrix(slices: int) -> str:
    """The gate-hosted matrix JSON for `slices` live slices plus slice 0, or a SystemExit naming every slice over the ceiling."""
    import json

    plans = hosted_slice_plans(slices)
    for i, plan in enumerate(plans, 1):
        tests = sum(len(v) for v in plan.values())
        print(f"live slice {i}/{slices}: {tests} tests, {len(plan)} modules, ~{slice_minutes(tests):.0f} min", file=sys.stderr)
    if bad := slice_budget_problems(plans):
        raise SystemExit("\n".join(bad))
    return json.dumps({"slice": list(range(slices + 1))})


def verify_live_modules(led: Ledger, engines: list[str] | None = None, part: str = HOSTED, shard: tuple[int, int] | None = None) -> None:
    """Run the live_suite modules and tests no dedicated cell ran and `part` (hosted or devbox) can run, those of `engines` only when named and of slice `shard` (i, n) only when given; one ledger row per test."""
    engines = engines or []
    modules = [m for m in live_suite_modules() if m not in EXCLUDED]
    sel = select_modules(engines, modules, live_owners.module_owners(), cfg("engines").split())
    owners = frozenset(sel.engines)
    table = live_test_table()
    dedicated_modules, dedicated_tests = dedicated_live()
    gone = set(RAN_LIVE_TESTS) | dedicated_tests
    ran_modules = dedicated_modules | set(RAN_LIVE_MODULES) if part == HOSTED else set()
    plan = part_plan(table, list(sel.run), part, gone, ran_modules, shard)
    run_modules = list(plan)
    if sel.narrowed:
        led.skip(sel.line())
        for e, mods in sel.left_out.items():
            led.skipped(e, "-", "live modules", "-", f"--engines {','.join(sel.engines)}: {len(mods)} {e} live module(s) not run",
                        cause=SkipCause.REDUCED)
    label = "live modules" if part == HOSTED else "live modules devbox"
    if not run_modules:
        led.skipped("-", "-", label, "-", f"{label}: no live test left for this part and slice", cause=SkipCause.DESIGN)
        return
    left, expr = derived_filter(run_modules, ran_modules, gone, owners)
    expr = part_filter(expr, part, table)
    excl = [n for n in exclusive_tests(set(run_modules)) if n in {t for names in plan.values() for t in names}]
    env, again = live_modules_env(os.environ.get("RIVET_CDC_STATE_URL") or os.environ.get("RIVET_GATE_STATE_URL") or "")
    _run_live_modules(led, "live" if part == HOSTED else "live_devbox", label,
                      f"every other live_suite module ({len(left)}), derived from tests/live_suite.rs",
                      left, env=env, again=again, keep=True, register=False,
                      expr=f"({expr}) - test(/::({'|'.join(excl)})$/)" if excl else expr)
    if excl:
        _run_live_modules(led, "live_modules" if part == HOSTED else "live_devbox_excl", f"{label} (exclusive)",
                          f"tests that need the stand to themselves ({len(excl)}), one at a time",
                          left, env={**env, **EXCLUSIVE_ENV}, threads=1, again=again, keep=True, register=False,
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

    table = [(m, f"{m}_t{i}", False) for m in mods for i in (1, 2)] + [("cloud", "cloud_bq", True)]

    def stage(engines: list[str] | None, part: str = HOSTED, shard: tuple[int, int] | None = None) -> tuple[list[str], str, list]:
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
            st.enter_context(mock.patch(f"{__name__}.live_test_table", lambda: table))
            st.enter_context(mock.patch(f"{__name__}.dedicated_live", lambda: (set(), set())))
            st.enter_context(mock.patch(f"{__name__}._run_live_modules", lambda led, s, label, phase, left, **k: seen.append(list(left))))
            verify_live_modules(led, engines, part, shard)
        return (seen[0] if seen else []), "\n".join(led._buf), led.cells

    ran, said, cells = stage(["mysql"])
    assert ran == list(one.run), ran
    for e in ("postgres", "mssql", "mongo", "oracle"):
        assert e in said, f"the narrowed run does not print that {e} is excluded: {said}"
    assert "cloud, plain" in said and "RUN under any selection" in said and "not a gate verdict" in said, said
    assert sorted(c.engine for c in cells if c.cause == SkipCause.REDUCED) == ["mongo", "mssql", "oracle", "postgres"], cells
    assert select_modules(["mysql"], ["my_a"], owners, known).line().endswith("none"), "an empty engine-less set is said, not left blank"
    ran, said, cells = stage(None)
    assert ran == mods and "NARROWED" not in said and not cells, (ran, said)
    dev_ran, _, _ = stage(None, DEVBOX)
    assert dev_ran == ["cloud"], f"the devbox part runs the module with a credentials test and nothing else: {dev_ran}"
    cut = [stage(None, HOSTED, (i, 3))[0] for i in (1, 2, 3)]
    assert sorted(m for c in cut for m in c) == mods and sum(map(len, cut)) == len(mods), f"the slices do not partition the modules: {cut}"
    assert cut == [stage(None, HOSTED, (i, 3))[0] for i in (1, 2, 3)], "the slices are not deterministic"
    left, expr = derived_filter(["live_cdc", "live_mssql_variant"], set(), set(), frozenset({"mysql"}))
    assert left == ["live_cdc", "live_mssql_variant"] and "mssql" in expr and expr.startswith("(test(/^(live_cdc|"), expr
    assert derived_filter(["live_cdc"], set(), set())[1] == "test(/^(live_cdc)::/)", "no selector narrows no test"
    live_owners.self_test()
    real = live_test_table()
    marked = sum(len(IGNORE_ATTR.findall(f.read_text())) for f in (ROOT / "tests" / "live").glob("*.rs"))
    assert len(real) == marked, f"{marked - len(real)} ignored live test(s) the reason scan does not read: the hosted/devbox split would lose them"
    assert part_tests(HOSTED, real) | part_tests(DEVBOX, real) == {n for _, n, _ in real} and not part_tests(HOSTED, real) & part_tests(DEVBOX, real), "a live test runs in neither part or in both"
    assert part_tests(DEVBOX, real), "no live test needs the devbox half"
    derived = _need_map()
    carried = set().union(*derived.values())
    strays = sorted(n for m, n, devbox in real if not devbox and any(NEED_HALF[v] == DEVBOX for v in derived.get((m, n), ())))
    assert not strays, f"{len(strays)} live test(s) carry a need only the devbox half meets and run hosted: {strays[:5]}"
    assert carried == set(NEED_HALF), f"a Need variant no live test carries, or one the scanner does not read: {sorted(carried ^ set(NEED_HALF))}"
    assert needs_problems(live_needs.need_variants()) == [] and len(needs_problems([*live_needs.need_variants(), "Ghost"])) == 1 and len(needs_problems([])) == len(NEED_HALF), "the Need placement guard does not name an unplaced or a stale variant"
    live_needs.self_test()
    assert dedicated_stage_problems() == [], dedicated_stage_problems()
    audit = 'x = 1\n_run_live_modules(led, "a", "{}", "p", ["live_audit_suspects"]%s)\n'
    devbox_stage = next(n for n, h in STAGE_HALF.items() if h == DEVBOX)
    assert dedicated_stage_problems({"a.py": audit.replace("{}", "audit suspects") % ""}) == [], "a hosted stage over whole modules is refused"
    assert len(dedicated_stage_problems({"a.py": audit.replace("{}", devbox_stage) % ""})) == 1, "a devbox stage over whole modules goes unnoticed"
    assert dedicated_stage_problems({"a.py": audit.replace("{}", devbox_stage) % ", expr=E"}) == [], "a stage with its own filter is not a whole-module run"
    audit_devbox = sorted(n for m, n, devbox in real if m == "live_audit_suspects" and devbox)
    assert audit_devbox, f"the audit stage's credential tests are not placed in the devbox half: {audit_devbox}"
    audit_expr = dedicated_expr(["live_audit_suspects"])
    assert all(n in audit_expr for n in audit_devbox) and audit_expr.count(" - test(") == 1, f"the audit stage's filter must drop its devbox tests: {audit_expr[:200]}"
    real_mods = [m for m in live_suite_modules() if m not in EXCLUDED]
    real_dedicated = dedicated_live()
    for n in (1, 3, 5):
        bad = coverage_problems(real, real_mods, *real_dedicated, n)
        assert not bad, f"{len(bad)} live test(s) run in no part or in two with {n} slice(s): {bad[:5]}"
    dedicated_m, dedicated_t = dedicated_live({"a.py": 'X = ("same_named_configs_share_a_postgres_state_cdc_cycle",)\nrun_rig_tests(led, "s", X)\nother("live_cdc")\n'})
    assert dedicated_t == {"same_named_configs_share_a_postgres_state_cdc_cycle"} and not dedicated_m, (dedicated_m, dedicated_t)
    assert dedicated_live({"a.py": 'RAN_LIVE_TESTS.add(T)\nT = "roast_pg_cdc_bounded_on_a_standby_fails_loud"\n'})[1] == {"roast_pg_cdc_bounded_on_a_standby_fails_loud"}, "a registration through a named constant is not read"
    real_dedicated_modules, real_dedicated_tests = dedicated_live()
    held, ghost = sorted(real_dedicated_tests)[0], "a_test_no_source_names_registered_at_run_time"
    saved_tests, saved_modules = set(RAN_LIVE_TESTS), set(RAN_LIVE_MODULES)
    try:
        for ran, red in (({held}, False), ({held, ghost}, True)):
            RAN_LIVE_MODULES.clear()
            RAN_LIVE_TESTS.clear()
            RAN_LIVE_TESTS.update(ran)
            led = Ledger(colour=False)
            verify_registry_covered(led)
            assert led.red is red, f"registered {sorted(ran)}: the static dedicated set reads as {'complete' if not red else 'incomplete'} wrongly"
            assert len(led.cells) == 1, f"registered {sorted(ran)}: the stage recorded {len(led.cells)} ledger row(s), want exactly one whichever way it grades"
    finally:
        RAN_LIVE_TESTS.clear()
        RAN_LIVE_TESTS.update(saved_tests)
        RAN_LIVE_MODULES.clear()
        RAN_LIVE_MODULES.update(saved_modules)
    even = [{"m": ["t"] * 100}] * 2
    assert slice_budget_problems(even) == [] and len(slice_budget_problems([{"m": ["t"] * 1000}, *even])) == 1, "a slice over the ceiling is not refused alone"
    assert slice_budget_problems(even, 1.0) and slice_minutes(0) == SLICE_BUILD_MIN, "the ceiling or the build floor is not read"
    declared = int(re.search(r'^  LIVE_SLICES: "(\d+)"', (ROOT / ".github" / "workflows" / "gate-hosted.yml").read_text(), re.M)[1])
    assert declared >= 1 and not slice_budget_problems(hosted_slice_plans(declared)), f"gate-hosted.yml runs {declared} live slices and the planner refuses them: {slice_budget_problems(hosted_slice_plans(declared))}"
    print("self-test ok: --engines runs the named engines' live modules and the engine-less ones, prints the rest as excluded, and no selector runs all")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "plan":
        print(plan_matrix(int(sys.argv[2])))
    else:
        raise SystemExit("usage: python3 -m dev.release_oracle.live_modules plan N")
