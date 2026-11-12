"""Seeded-defect recall: re-introduce known bug classes as source patches and grade whether the harness catches each.

    python -m dev.seeded                                  # every seed on every engine
    python -m dev.seeded --seeds committed-every-event --engines postgres,mysql
    python -m dev.seeded --self-test                      # manifest + grading logic, no build

See docs/seeded-recall.md for what a row means and how to add a seed.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from dev.pytools import evidence
from dev.release_oracle import stand_health
from dev.release_oracle.core import Ledger, SkipCause, isolate_state_db, nextest_filter, nextest_outcomes, nextest_panics, self_skipped
from dev.release_oracle.live_modules import EXCLUSIVE_ENV, exclusive_tests, live_modules_env
from dev.seeded import run_record

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
MANIFEST = HERE / "seeds.yaml"


def enum_variants(rel: str, name: str, root: Path = ROOT) -> tuple[str, ...]:
    """The lowercased variants of the Rust enum `name` in `rel`, in declaration order; a missing or empty enum is an error."""
    text = (root / rel).read_text()
    m = re.search(rf"\benum {re.escape(name)} \{{", text)
    if m is None:
        raise ValueError(f"`enum {name}` not found in {rel}")
    out, depth = [], 1
    for line in text[m.end():].splitlines():
        t = line.strip()
        ident = re.match(r"[A-Za-z0-9_]+", t)
        if depth == 1 and t and not t.startswith(("//", "#")) and ident and ident.group()[0].isupper():
            out.append(ident.group().lower())
        depth += t.count("{") - min(t.count("}"), depth)
        if depth == 0:
            break
    if not out:
        raise ValueError(f"`enum {name}` in {rel} has no variants")
    return tuple(out)


SCOPES = {
    "source": enum_variants("src/config/source.rs", "SourceType"),
    "target": enum_variants("src/config/load.rs", "LoadTargetKind"),
    "destination": enum_variants("src/config/destination.rs", "DestinationType"),
    "state": ("postgres-state", "sqlite-state"),
}
OPTIONAL = ("lib",)
MIN_NA_REASON = 30
CAUGHT, MISSED, STALE, BROKEN, NO_CONTROL, NA = "CAUGHT", "MISSED", "STALE", "BROKEN", "NO-CONTROL", "N/A"
ABORTED = "ABORTED"
#: A row whose cells exist only on another platform: owed to that platform's island, never a miss on this host.
OWED = "OWED"
PLATFORMS = ("linux", "macos")
#: Every declared cell skipped on this stand for a reason it named: reported, never a catch, held under a ceiling.
NOT_GRADED_HERE = "NOT-GRADED-HERE"
#: A row whose cells ran while the stand was down or unreadable: neither a catch nor a miss, and the run is NOT GRADED.
STAND_DOWN = "STAND-DOWN"
#: A cell whose red is the stand's own panic, not an assertion about the product.
STAND = "STAND"
#: The gate matrix row whose `not_graded_here_ceiling` bounds the NOT-GRADED-HERE rows.
GATE_MATRIX = ROOT / "docs" / "release-gate-matrix.yaml"
LIB = "lib:"
POOL = 3
CELL_VERDICTS = ("PASS", "FAIL", "SKIP", "ABSENT")


@dataclass(frozen=True)
class Row:
    """One seed on one engine, as the manifest declares it."""

    seed: str
    engine: str
    patch: str | None = None
    cells: tuple[str, ...] = ()
    na: str | None = None
    platform: str | None = None
    switch: str | None = None


@dataclass(frozen=True)
class Verdict:
    """A graded row: its status and the evidence for it."""

    row: Row
    status: str
    detail: str


def host_platform() -> str:
    """The platform this run's cells compile for: `linux`, `macos` or the raw `sys.platform`."""
    return "linux" if sys.platform.startswith("linux") else "macos" if sys.platform == "darwin" else sys.platform


def parse_manifest(doc: dict, patch_dir: Path = HERE) -> list[Row]:
    """The manifest's seed x engine rows; raises ValueError naming the first malformed entry."""
    rows: list[Row] = []
    names: set[str] = set()
    for s in (doc or {}).get("seeds") or []:
        name = s.get("name")
        if not name or name in names:
            raise ValueError(f"seed {name!r}: missing or duplicate name")
        names.add(name)
        scope = SCOPES.get(s.get("scope"))
        if scope is None:
            raise ValueError(f"seed {name}: scope must be one of {sorted(SCOPES)}")
        engines = s.get("engines") or {}
        missing = [e for e in scope if e not in engines]
        extra = [e for e in engines if e not in scope and e not in OPTIONAL]
        if missing or extra:
            raise ValueError(f"seed {name}: engines missing {missing}, outside the scope {extra} — "
                             "name every engine, with `na:` and a reason where the code path does not exist")
        for eng, e in engines.items():
            e = e or {}
            if "na" in e:
                if set(e) != {"na"} or len(str(e["na"]).strip()) < MIN_NA_REASON:
                    raise ValueError(f"seed {name}/{eng}: `na:` takes a reason of at least {MIN_NA_REASON} characters and nothing else")
                rows.append(Row(name, eng, na=str(e["na"]).strip()))
                continue
            patch, cells = e.get("patch", s.get("patch")), e.get("cells")
            platform = e.get("platform", s.get("platform"))
            switch = e.get("switch", s.get("switch"))
            if platform is not None and platform not in PLATFORMS:
                raise ValueError(f"seed {name}/{eng}: platform must be one of {list(PLATFORMS)}")
            if switch and patch:
                raise ValueError(f"seed {name}/{eng}: a `switch:` row has no `patch:` — one source for the defect")
            if not (patch or switch) or not isinstance(cells, list):
                raise ValueError(f"seed {name}/{eng}: needs a patch (or a `switch:`) and a `cells:` list, or `na:`")
            if patch and not (patch_dir / patch).is_file():
                raise ValueError(f"seed {name}/{eng}: patch {patch} does not exist under {patch_dir}")
            rows.append(Row(name, eng, patch, tuple(str(c) for c in cells), platform=platform, switch=switch))
    if not rows:
        raise ValueError("the manifest declares no seeds")
    return rows


def unknown_cells(rows: list[Row], root: Path = ROOT) -> list[tuple[Row, str]]:
    """Declared cells with no `fn <name>(` in the tree — a renamed test would otherwise grade as absent forever."""
    live, lib = (_fn_names(root / tree) for tree in ("tests", "src"))
    out = []
    for r in rows:
        for c in r.cells:
            names, name = (lib, c[len(LIB):]) if c.startswith(LIB) else (live, c)
            if name not in names:
                out.append((r, c))
    return out


SWITCH_SITE = re.compile(r'\bseeded!\(\s*"([^"]+)"')


def switch_sites(root: Path = ROOT) -> dict[str, int]:
    """Each `seeded!("<name>", ..)` switch in src/ and how many sites carry it."""
    found: dict[str, int] = {}
    for f in sorted((root / "src").rglob("*.rs")):
        for name in SWITCH_SITE.findall(f.read_text()):
            found[name] = found.get(name, 0) + 1
    return found


def switch_problems(rows: list[Row], root: Path = ROOT, complete: bool = True) -> list[str]:
    """A row naming a switch the source lacks (it would grade the unseeded build), or, on a complete run, a source switch no row names (dead defect code)."""
    found = switch_sites(root)
    named = {r.switch for r in rows if r.switch}
    return ([f"seed {r.seed}/{r.engine}: switch {r.switch!r} is in no `seeded!` site in src/" for r in rows if r.switch and r.switch not in found]
            + ([f"`seeded!({n!r})` in src/ is named by no manifest row" for n in sorted(set(found) - named)] if complete else []))


def _fn_names(tree: Path) -> set[str]:
    """Every name a `fn <name>(` declares under one tree, read in one pass."""
    return {m for p in tree.rglob("*.rs") for m in re.findall(r"\bfn (\w+)\(", p.read_text())}


def patch_files(patch_text: str) -> list[str]:
    """The repo-relative files a unified diff changes."""
    return re.findall(r"^\+\+\+ b/(\S+)", patch_text, re.M)


def stale_message(seed: str, patch_text: str, git_out: str) -> str:
    """The loud refusal for a patch that no longer applies, naming the file(s) git rejected."""
    files = patch_files(patch_text)
    named = [f for f in files if f in git_out] or files
    return f"seed {seed} no longer applies to {', '.join(named)}: update the patch"


def cell_outcomes(out: str, skipped: dict[str, str], cells: tuple[str, ...]) -> dict[str, str]:
    """Each cell's verdict in one nextest run: PASS, FAIL (a failed test), STAND (every failure is the stand's own panic), SKIP (self-skipped), ABSENT, or the raw status of a test nextest stopped (TIMEOUT, ABORT, ...)."""
    final = nextest_outcomes(out)
    panics = nextest_panics(out)
    skip_names = {k.split("::")[-1] for k in skipped}
    res: dict[str, str] = {}
    for c in cells:
        bare = c.removeprefix(LIB)
        named = [(n, s) for n, s in final.items() if n == bare or n.endswith("::" + bare)]
        seen = [s for _, s in named]
        failed = [n for n, s in named if s == "FAIL" or s.startswith("FAIL ")]
        if not seen:
            res[c] = "ABSENT"
        elif failed:
            res[c] = STAND if all(not stand_health.from_text(panics.get(n, "")).alive for n in failed) else "FAIL"
        elif any(s not in ("PASS", "LEAK") for s in seen):
            res[c] = next(s for s in seen if s not in ("PASS", "LEAK"))
        else:
            res[c] = "SKIP" if bare in skip_names else "PASS"
    return res


def grade(row: Row, control: dict[str, str], seeded: dict[str, str] | None,
          skipped: dict[str, str] | None = None, stand: stand_health.StandVerdict = stand_health.ALIVE) -> Verdict:
    """CAUGHT iff a cell green on the unpatched tree is red on the patched one; `seeded=None` = the patched tree did not build; `skipped` = each control cell's own skip reason; `stand` = how the stand read while the cells ran (down or unreadable grades nothing)."""
    if row.na:
        return Verdict(row, NA, row.na)
    if row.platform and row.platform != host_platform():
        return Verdict(row, OWED, f"its cells are {row.platform}-only and this host is {host_platform()}: owed to the {row.platform} island")
    if seeded is None:
        return Verdict(row, BROKEN, "the patched tree does not build — a red build is not a catch")
    if not row.cells:
        return Verdict(row, MISSED, "no cell catches this class on this engine")
    green = [c for c in row.cells if control.get(c) == "PASS"]
    if not stand.alive:
        return Verdict(row, STAND_DOWN, f"the stand was {stand.kind.value} while the cells ran, so no catch or miss is read ({stand.reason})")
    red = [c for c in green if seeded.get(c) == "FAIL"]
    hit = [c for c in row.cells if STAND in (control.get(c), seeded.get(c))]
    if hit and not red:
        return Verdict(row, STAND_DOWN, f"cell(s) {', '.join(hit)} failed on the stand's own panic, so no catch or miss is read")
    if not green and all(control.get(c) == "SKIP" for c in row.cells):
        why = "; ".join(f"{c}: {(skipped or {}).get(c, 'skipped')}" for c in row.cells)
        return Verdict(row, NOT_GRADED_HERE, f"every declared cell skipped on this stand ({why})")
    if not green:
        said = ", ".join(f"{c}={control.get(c, 'ABSENT')}" for c in row.cells)
        return Verdict(row, NO_CONTROL, f"no declared cell is green on the unpatched tree ({said})")
    if red:
        return Verdict(row, CAUGHT, ", ".join(red))
    stopped = [f"{c}={seeded[c]}" for c in green if seeded.get(c) not in (None, *CELL_VERDICTS)]
    if stopped:
        return Verdict(row, ABORTED, f"a control-green cell was stopped, not failed ({', '.join(stopped)}): only a failing "
                                     "assertion catches a seed, so a hang-class seed needs a watchdog test that fails")
    said = ", ".join(f"{c}={seeded.get(c, 'ABSENT')}" for c in green)
    return Verdict(row, MISSED, f"every control-green cell stayed green under the seed ({said})")


# ── the scratch tree ──────────────────────────────────────────────────────────


def sh(argv: list[str], *, cwd: Path = ROOT, env: dict[str, str] | None = None,
       timeout: float | None = None) -> subprocess.CompletedProcess:
    """Run argv (no shell) with exactly `env` (or the inherited one); stdout+stderr merged into `.stdout`."""
    return subprocess.run(argv, cwd=cwd, env=env, timeout=timeout, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def git(cwd: Path, *args: str) -> str:
    """A git command that must succeed; its trimmed output."""
    p = sh(["git", "-C", str(cwd), *args])
    if p.returncode != 0:
        raise SystemExit(f"git {' '.join(args)} in {cwd} failed:\n{p.stdout}")
    return p.stdout.strip()


def scratch_base() -> Path:
    """Where the scratch worktree, its own target dir and the run record live (kept between runs so builds stay incremental)."""
    return run_record.seeded_dir()


def prepare_tree(base: Path) -> tuple[Path, str]:
    """A clean scratch worktree at HEAD; refuses one a killed run left a seed in."""
    sha = git(ROOT, "rev-parse", "HEAD")
    tree = base / "tree"
    if (tree / ".git").exists():
        dirty = git(tree, "status", "--porcelain", "--untracked-files=no")
        if dirty:
            raise SystemExit(f"{tree} has local changes — a run killed mid-seed? `git -C {tree} diff` "
                             f"shows them; remove the tree to start over:\n{dirty}")
        git(tree, "switch", "--quiet", "--detach", sha)
    else:
        base.mkdir(parents=True, exist_ok=True)
        git(ROOT, "worktree", "prune")
        git(ROOT, "worktree", "add", "--quiet", "--detach", str(tree), sha)
    # The Rig's oracle runs `uv run` in the tree; parallel first uses race to build one .venv.
    # On this harness's own interpreter: left to discovery, uv picks whatever Python PATH offers.
    venv = sh(["uv", "sync", "--frozen", "-q", "--python", sys.executable], cwd=tree)
    if venv.returncode != 0:
        raise SystemExit(f"uv sync in {tree} failed:\n{venv.stdout}")
    return tree, sha


def apply_seed(tree: Path, patch: Path) -> dict[Path, bytes]:
    """Snapshot every file the patch touches, then apply it; the snapshot is the restore point."""
    snap = {tree / f: (tree / f).read_bytes() for f in patch_files(patch.read_text())}
    p = sh(["git", "-C", str(tree), "apply", str(patch)])
    if p.returncode != 0:
        restore(tree, snap)
        raise RuntimeError(p.stdout)
    return snap


def restore(tree: Path, snap: dict[Path, bytes]) -> None:
    """Write each snapshotted file back with a fresh mtime (so cargo rebuilds it), then demand a clean tree."""
    for f, data in snap.items():
        f.write_bytes(data)
        os.utime(f)
    dirty = git(tree, "status", "--porcelain", "--untracked-files=no")
    if dirty:
        raise SystemExit(f"{tree} is not clean after restoring the snapshot — stop and inspect:\n{dirty}")


#: The knobs stages read the gate's own Postgres state from: the scratch DB replaces each, so no seed run reaches the shared stand state.
SCRATCH_STATE_VARS = ("RIVET_CDC_STATE_URL", "RIVET_CONC_STATE_URL")


class Cargo:
    """Builds and runs nextest in the scratch tree with its own target dir and the tree's own rivet binary."""

    def __init__(self, tree: Path, base: Path, state_url: str | None, features: tuple[str, ...] = ()) -> None:
        self.tree, self.base = tree, base
        self.feature_args = ["--features", ",".join(features)] if features else []
        env = {k: v for k, v in os.environ.items()
               if k not in ("RIVET_BIN", "RIVET_BIN_OVERRIDE", "RIVET_STATE_URL", "RIVET_TEST_STATE_URL", "RIVET_SEED")}
        env["CARGO_TARGET_DIR"] = str(base / "target")
        env["CARGO_PROFILE_DEV_DEBUG"] = "line-tables-only"
        # The cells run in the scratch tree: a binary the gate names relative to this checkout (`.gate-baseline/...`) is named from here.
        for k, v in list(env.items()):
            if k.endswith("_BIN") and v and not os.path.isabs(v):
                env[k] = str(ROOT / v)
        # Each cell runs in the env the gate's live-modules stage runs it in, with the scratch DB as the gate's state:
        # a control is then the run the gate already grades.
        stage_env, self.again = live_modules_env(state_url or "")
        env.update(stage_env)
        if state_url:
            for var in SCRATCH_STATE_VARS:
                env[var] = state_url
            t = urllib.parse.urlsplit(state_url)
            env["RIVET_TEST_STATE_TOXI_URL"] = urllib.parse.urlunsplit(
                (t.scheme, f"{t.username}:{t.password}@127.0.0.1:15433", t.path, "", ""))
        self.env = env
        self.skipped: dict[str, str] = {}

    def binaries(self, cells) -> list[str]:
        """The nextest binary flags these cells need: `--lib` for `lib:` cells, else the top-level tests/<name>.rs defining the cell, else offline_suite for a cell under tests/offline, else live_suite."""
        if not cells:
            return ["--lib"]
        tops = sorted((self.tree / "tests").glob("*.rs"))
        offline = "\n".join(f.read_text() for f in sorted((self.tree / "tests" / "offline").glob("*.rs")))
        picked: list[tuple[str, ...]] = []
        for c in cells:
            if c.startswith(LIB):
                one: tuple[str, ...] = ("--lib",)
            else:
                owner = [f.stem for f in tops if re.search(rf"\bfn {re.escape(c)}\(", f.read_text())]
                if not owner and re.search(rf"\bfn {re.escape(c)}\(", offline):
                    owner = ["offline_suite"]
                one = ("--test", owner[0] if owner else "live_suite")
            if one not in picked:
                picked.append(one)
        return [flag for one in picked for flag in one]

    def build(self, cells, log: Path) -> subprocess.CompletedProcess:
        """Compile the test binaries (and the rivet binary they drive)."""
        p = sh(["cargo", "nextest", "run", "--no-run", "--manifest-path", str(self.tree / "Cargo.toml"),
                *self.feature_args, *self.binaries(cells)], cwd=self.tree, env=self.env)
        log.write_text(p.stdout)
        return p

    def run(self, cells, log: Path) -> dict[str, str]:
        """Run exactly these cells, each as the live-modules stage runs it (a `live+exclusive` one alone, under RIVET_TEST_EXCLUSIVE); each one's verdict."""
        alone = set(exclusive_tests())
        shared = [c for c in cells if c.removeprefix(LIB) not in alone]
        exclusive = [c for c in cells if c.removeprefix(LIB) in alone]
        out: dict[str, str] = {}
        why: dict[str, str] = {}
        for group, extra, suffix in ((shared, {}, ""), (exclusive, EXCLUSIVE_ENV, ".exclusive")):
            if group:
                o, w = self._run_group(group, log.with_name(log.stem + suffix + log.suffix), extra)
                out.update(o)
                why.update(w)
        self.skipped = why
        return out

    def _run_group(self, cells, log: Path, extra: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
        """One nextest run of `cells` under this env plus `extra`, and the leg a self-skip naming the gate's state var owes."""
        threads = ["--test-threads", "1"] if extra else []
        skip_log = log.with_suffix(".skips")
        skip_log.write_text("")
        p = sh(["cargo", "nextest", "run", "--manifest-path", str(self.tree / "Cargo.toml"),
                *self.feature_args, *self.binaries(cells), "--run-ignored", "all", "--no-fail-fast", "--retries", "0", *threads,
                "-E", nextest_filter([c.removeprefix(LIB) for c in cells])],
               cwd=self.tree, env={**self.env, **extra, "RIVET_SKIP_LOG": str(skip_log)})
        log.write_text(p.stdout)
        skipped = self_skipped(skip_log)
        out = cell_outcomes(p.stdout, skipped, tuple(cells))
        why = {k.split("::")[-1]: v for k, v in skipped.items()}
        owed = [c for c in cells if self.again and out.get(c) == "SKIP" and self.again[0] in why.get(c.removeprefix(LIB), "")]
        if owed:
            # As the stage does: a self-skip naming the gate's state var runs again under it, and that leg is its verdict.
            again_log = log.with_suffix(".again.skips")
            again_log.write_text("")
            q = sh(["cargo", "nextest", "run", "--manifest-path", str(self.tree / "Cargo.toml"),
                    *self.feature_args, *self.binaries(owed), "--run-ignored", "all", "--no-fail-fast", "--retries", "0", *threads,
                    "-E", nextest_filter([c.removeprefix(LIB) for c in owed])],
                   cwd=self.tree, env={**self.env, **extra, **self.again[1], "RIVET_SKIP_LOG": str(again_log)})
            log.with_suffix(".again.log").write_text(q.stdout)
            second = self_skipped(again_log)
            out.update(cell_outcomes(q.stdout, second, tuple(owed)))
            for c in owed:
                why.pop(c.removeprefix(LIB), None)
            why.update({k.split("::")[-1]: v for k, v in second.items()})
        return out, why


def recompiled(build_out: str) -> bool:
    """Did cargo compile the crate (not report it Fresh)? A Fresh build grades the unpatched binary."""
    return re.search(r"^\s*Compiling rivet-cli\b", build_out, re.M) is not None


# ── the run ───────────────────────────────────────────────────────────────────


def gate_ceiling(key: str, matrix: Path = GATE_MATRIX) -> int:
    """The gate matrix's integer bound `key` on the `seeded_recall` row."""
    rows = (yaml.safe_load(matrix.read_text()) or {}).get("infra") or []
    row = next((r for r in rows if r.get("id") == "seeded_recall"), None)
    if row is None or not isinstance(row.get(key), int):
        raise SystemExit(f"{matrix}: the seeded_recall row names no integer {key}")
    return row[key]


def record(led: Ledger, out: list[Verdict], ceiling: int | None = None, owed_fails: bool = False) -> bool:
    """One ledger row per verdict: CAUGHT passes, STAND-DOWN is an INFRA row (the run is NOT GRADED), N/A is a counted N/A row held by its own reason, NOT-GRADED-HERE is a SKIP row held under `ceiling` (more is a FAIL), OWED is a DESIGN SKIP row (a FAIL on the island that owes it, `owed_fails`), anything else (MISSED, STALE, BROKEN, NO-CONTROL, ABORTED) fails the gate; True when no row failed."""
    here, owed, nas, green = [], [], 0, True
    for v in out:
        msg = f"seeded {v.row.seed} × {v.row.engine}: {v.status} — {v.detail}"
        if v.status == CAUGHT:
            led.passed(v.row.engine, "seeded", v.row.seed, "-", msg)
        elif v.status == NOT_GRADED_HERE:
            led.skipped(v.row.engine, "seeded", v.row.seed, "-", msg, cause=SkipCause.ENV)
            here.append(f"{v.row.seed} × {v.row.engine}")
        elif v.status == NA:
            led.not_applicable(v.row.engine, "seeded", v.row.seed, "-", msg)
            nas += 1
        elif v.status == OWED and not owed_fails:
            led.skipped(v.row.engine, "seeded", v.row.seed, "-", msg, cause=SkipCause.DESIGN)
            owed.append(f"{v.row.seed} × {v.row.engine}")
        elif v.status == STAND_DOWN:
            led.ungraded(v.row.engine, "seeded", v.row.seed, "-", msg)
            green = False
        else:
            led.failed(v.row.engine, "seeded", v.row.seed, "-", msg)
            green = False
    limit = gate_ceiling("not_graded_here_ceiling") if ceiling is None else ceiling
    if len(here) > limit:
        led.failed("-", "seeded", "not_graded_here", "-",
                   f"seeded recall: {len(here)} row(s) NOT-GRADED-HERE, over the gate matrix's ceiling of {limit} "
                   f"(docs/release-gate-matrix.yaml seeded_recall): a row whose cells now skip on this stand is new: "
                   + ", ".join(here))
        green = False
    elif here:
        print(f"  {len(here)} row(s) NOT-GRADED-HERE (ceiling {limit}): " + ", ".join(here))
    if owed:
        print(f"  {len(owed)} row(s) OWED to another platform's island: " + ", ".join(owed))
    if nas:
        print(f"  {nas} N/A row(s), each held by its reason in the manifest")
    return green


@dataclass(eq=False)
class Job:
    """One patch's turn: the rows it grades, the cells it runs, and what its worker parked for the grader."""

    patch: str
    name: str
    cells: list[str]
    group: list[Row]
    cargo: Cargo | None = None
    snap: dict[Path, bytes] | None = None
    build: subprocess.CompletedProcess | None = None
    error: BaseException | None = None
    done: bool = False
    ready: threading.Event = field(default_factory=threading.Event)
    released: threading.Event = field(default_factory=threading.Event)


class BuildPool:
    """Applies and builds patches ahead of the grader, one patch per worker tree; a tree is held until its patch is released."""

    def __init__(self, cargos: list[Cargo], jobs: list[Job], logs: Path, apply=None, undo=None) -> None:
        assert cargos, "a pool needs at least one worker tree"
        self.logs, self.todo, self.taken = logs, list(jobs), []
        self.apply, self.undo = apply or apply_seed, undo or restore
        self.lock, self.stopped = threading.Lock(), False
        self.threads = [threading.Thread(target=self._work, args=(c,), daemon=True) for c in cargos]
        for t in self.threads:
            t.start()

    def _next(self) -> Job | None:
        """The next patch in order, or None once the pool is stopped or drained."""
        with self.lock:
            if self.stopped or not self.todo:
                return None
            self.taken.append(self.todo.pop(0))
            return self.taken[-1]

    def _work(self, cargo: Cargo) -> None:
        """Apply and build patch after patch in this worker's own tree, parking each result until it is released."""
        while (job := self._next()) is not None:
            job.cargo = cargo
            try:
                job.snap = self.apply(cargo.tree, HERE / job.patch)
                job.build = cargo.build(job.cells, self.logs / f"{job.name}.build.log")
            except BaseException as e:
                job.error = e
            job.ready.set()
            job.released.wait()

    def take(self, job: Job) -> Job:
        """Wait for this patch's build."""
        job.ready.wait()
        return job

    def release(self, job: Job) -> None:
        """Restore the job's tree and free its worker for the next patch."""
        job.done = True
        try:
            if job.snap is not None:
                self.undo(job.cargo.tree, job.snap)
        finally:
            job.released.set()

    def close(self) -> None:
        """Stop taking patches, restore every tree still holding a seed, and join the workers."""
        with self.lock:
            self.stopped = True
            held = list(self.taken)
        for job in held:
            job.ready.wait()
            if not job.done:
                self.release(job)
        for t in self.threads:
            t.join()


def grade_seeds(led: Ledger, live: list[Row], control: dict[str, str], control_skips: dict[str, str],
                cargos: list[Cargo], logs: Path, apply=None, undo=None, book: stand_health.Book | None = None,
                control_stand: stand_health.StandVerdict = stand_health.ALIVE) -> dict[Row, Verdict]:
    """Grade each patch in sorted order: its build comes from a worker tree ahead of time, its cells run here, one patch at a time; a patch whose cells ran while the stand was down (or the control did) grades nothing."""
    book = book or stand_health.BOOK
    jobs = []
    for patch in sorted({r.patch for r in live}):
        group = [r for r in live if r.patch == patch]
        cells = sorted({c for r in group for c in r.cells if control.get(c) == "PASS"})
        jobs.append(Job(patch, patch.replace("/", "__").removesuffix(".patch"), cells, group))
    verdicts: dict[Row, Verdict] = {}
    pool = BuildPool(cargos, jobs, logs, apply, undo)
    try:
        for job in jobs:
            led.phase(f"SEED {job.patch} — {', '.join(r.engine for r in job.group)} ({len(job.cells)} control-green cell(s))")
            pool.take(job)
            stand = control_stand
            try:
                if job.error is not None:
                    raise job.error
                b = job.build
                if b.returncode != 0 or not recompiled(b.stdout):
                    why = "build failed" if b.returncode != 0 else "cargo did not recompile rivet-cli"
                    led.bad(f"{why} (see {logs / f'{job.name}.build.log'})")
                    seeded = None
                else:
                    since = book.mark()
                    seeded = job.cargo.run(job.cells, logs / f"{job.name}.log") if job.cells else {}
                    stand = stand_health.classify([control_stand, book.judge("", since)])
            finally:
                pool.release(job)
            for r in job.group:
                verdicts[r] = grade(r, control, seeded, control_skips, stand)
    finally:
        pool.close()
    return verdicts


def grade_switches(led: Ledger, live: list[Row], control_skips: dict[str, str], cargo: Cargo, logs: Path,
                   book: stand_health.Book | None = None) -> dict[Row, Verdict]:
    """Grade each switched seed on ONE `seeded` build of the clean tree: its own control with RIVET_SEED unset, then its cells with RIVET_SEED=<switch>."""
    book = book or stand_health.BOOK
    cells = sorted({c for r in live for c in r.cells})
    led.phase(f"SWITCHED BUILD — one `seeded` build, {len({r.switch for r in live})} switch(es), {len(cells)} cell(s)")
    b = cargo.build(cells, logs / "switched.build.log")
    if b.returncode != 0:
        led.bad(f"the `seeded` build does not build (see {logs / 'switched.build.log'})")
        return {r: grade(r, {}, None) for r in live}
    since = book.mark()
    control = cargo.run(cells, logs / "switched.control.log")
    control_stand = stand_health.judge("", since)
    skips = {**control_skips, **cargo.skipped}
    for c, v in control.items():
        (led.ok if v == "PASS" else led.bad)(f"switched control {c}: {v}")
    out: dict[Row, Verdict] = {}
    for switch in sorted({r.switch for r in live}):
        group = [r for r in live if r.switch == switch]
        mine = sorted({c for r in group for c in r.cells if control.get(c) == "PASS"})
        led.phase(f"SWITCH {switch} — {', '.join(r.engine for r in group)} ({len(mine)} control-green cell(s))")
        since = book.mark()
        cargo.env["RIVET_SEED"] = switch
        try:
            seeded = cargo.run(mine, logs / f"switch__{switch}.log") if mine else {}
        finally:
            cargo.env.pop("RIVET_SEED", None)
        stand = stand_health.classify([control_stand, book.judge("", since)])
        for r in group:
            out[r] = grade(r, control, seeded, skips, stand)
    return out


def recall(led: Ledger, rows: list[Row], *, complete: bool = True, owed_fails: bool = False) -> list[Verdict]:
    """Grade every row (a row of another platform is OWED, a FAIL under `owed_fails`): STALE check, one CONTROL run on the unpatched tree, then one build+run per patch; appends the run to the record."""
    t0 = time.perf_counter()
    led.phase("Seeded-defect recall — does the harness catch known bug classes re-introduced?")
    verdicts: dict[Row, Verdict] = {}
    for r, c in unknown_cells(rows):
        verdicts[r] = Verdict(r, STALE, f"cell {c} names no test in the tree: update the manifest")
    base = scratch_base()
    tree, sha = prepare_tree(base)
    logs = base / "logs"
    logs.mkdir(exist_ok=True)
    led.ok(f"scratch tree {tree} at {sha[:12]} (HEAD — commit first; uncommitted work is not graded)")
    gate_state = os.environ.get("RIVET_GATE_STATE_URL", "")
    state_url = isolate_state_db(gate_state, f"seed_{os.getpid()}") if gate_state else None
    led.ok(f"scratch state DB: {state_url.rsplit('/', 1)[-1] if state_url else 'none (RIVET_GATE_STATE_URL unset)'}")
    cargo = Cargo(tree, base, state_url)

    for r in rows:
        if r.patch and r not in verdicts:
            text = (HERE / r.patch).read_text()
            p = sh(["git", "-C", str(tree), "apply", "--check", str(HERE / r.patch)])
            if p.returncode != 0:
                verdicts[r] = Verdict(r, STALE, stale_message(r.seed, text, p.stdout))
    live = [r for r in rows if r not in verdicts and not r.na and r.platform in (None, host_platform())]
    for problem in switch_problems(rows, tree, complete):
        led.bad(problem)
    sw = [r for r in live if r.switch]
    live = [r for r in live if not r.switch]
    if sw:
        verdicts.update(grade_switches(led, sw, {}, Cargo(tree, base / "sw", state_url, ("seeded",)), logs))
    workers = [cargo] + [Cargo(prepare_tree(base / f"w{k}")[0], base / f"w{k}", state_url)
                         for k in range(1, min(POOL, len({r.patch for r in live})))]
    control_cells = sorted({c for r in live for c in r.cells})
    control: dict[str, str] = {}
    control_skips: dict[str, str] = {}
    control_stand = stand_health.ALIVE
    if control_cells:
        led.phase(f"CONTROL — {len(control_cells)} cell(s) on the unpatched tree")
        b = cargo.build(control_cells, logs / "control.build.log")
        if b.returncode != 0:
            led.bad(f"the unpatched tree does not build (see {logs / 'control.build.log'})")
        else:
            since = stand_health.BOOK.mark()
            control = cargo.run(control_cells, logs / "control.log")
            control_stand = stand_health.judge("", since)
            control_skips = dict(cargo.skipped)
            for c, v in control.items():
                (led.ok if v == "PASS" else led.bad)(f"control {c}: {v}")

    verdicts.update(grade_seeds(led, live, control, control_skips, workers, logs, control_stand=control_stand))

    out = [verdicts.get(r) or grade(r, control, {}, control_skips, control_stand) for r in rows]
    green = record(led, out, owed_fails=owed_fails)
    dirty = evidence.tree_dirty(ROOT)
    run_record.append_run(base / run_record.RUNS_FILE, sha=sha, complete=complete, green=green, dirty=dirty)
    print(render(out))
    print(f"  seeded-defect recall took {(time.perf_counter() - t0) / 60:.1f} min; logs under {logs}")
    return out


def render(verdicts: list[Verdict]) -> str:
    """The seed × engine recall table, then one line per row that is not CAUGHT."""
    seeds = list(dict.fromkeys(v.row.seed for v in verdicts))
    engines = list(dict.fromkeys(v.row.engine for v in verdicts))
    at = {(v.row.seed, v.row.engine): v.status for v in verdicts}
    w = max(len(s) for s in seeds)
    lines = ["", "seed".ljust(w) + " | " + " | ".join(e.ljust(10) for e in engines)]
    lines.append("-" * len(lines[-1]))
    for s in seeds:
        lines.append(s.ljust(w) + " | " + " | ".join(at.get((s, e), "").ljust(10) for e in engines))
    graded = [v for v in verdicts if v.status not in (NA, OWED)]
    caught = sum(v.status == CAUGHT for v in graded)
    lines.append(f"\nrecall: {caught}/{len(graded)} graded rows CAUGHT (N/A and OWED rows excluded)")
    for v in verdicts:
        if v.status != CAUGHT:
            lines.append(f"  {v.status:<10} {v.row.seed} × {v.row.engine}: {v.detail}")
    return "\n".join(lines)


def platform_rows(rows: list[Row]) -> list[Row]:
    """The rows that declare a platform: what the platform's island owes beside the per-run recall."""
    return [r for r in rows if r.platform]


def select(rows: list[Row], seeds: str, engines: str) -> list[Row]:
    """Narrow to the named seeds / engines (comma lists); an unknown name is an error, never an empty run."""
    for want, have, what in ((seeds, {r.seed for r in rows}, "seed"), (engines, {r.engine for r in rows}, "engine")):
        unknown = [x for x in want.split(",") if x and x not in have]
        if unknown:
            raise SystemExit(f"unknown {what}(s): {unknown}; the manifest has {sorted(have)}")
    return [r for r in rows
            if (not seeds or r.seed in seeds.split(",")) and (not engines or r.engine in engines.split(","))]


def pool_self_test() -> None:
    """Pooled grading equals serial grading: same verdicts, same phase lines, every seed run in the tree it was applied to."""

    class World:
        def __init__(self) -> None:
            self.lock, self.applied, self.applied_in = threading.Lock(), {}, {}
            self.building = self.max_building = self.running = 0
            self.ran: list[tuple[str | None, Path]] = []
            self.on_run = None

    class Fake:
        def __init__(self, tree: Path, world: World) -> None:
            self.tree, self.world, self.skipped = tree, world, {}

        def build(self, cells, log):
            w = self.world
            with w.lock:
                w.building += 1
                w.max_building = max(w.max_building, w.building)
                patch = w.applied[self.tree]
            time.sleep(DELAY[patch])
            with w.lock:
                w.building -= 1
            return subprocess.CompletedProcess([], 1 if patch == "p2.patch" else 0, "Fresh" if patch == "p4.patch" else "Compiling rivet-cli")

        def run(self, cells, log):
            w = self.world
            with w.lock:
                assert w.running == 0, "two cell runs overlapped"
                w.running += 1
                patch = w.applied[self.tree]
                w.ran.append((patch, self.tree))
            time.sleep(0.01)
            if w.on_run:
                w.on_run()
            with w.lock:
                w.running -= 1
            return {c: "FAIL" if c in CATCH.get(patch, ()) else "PASS" for c in cells}

    class Spy(Ledger):
        def __init__(self) -> None:
            super().__init__(colour=False)
            self.lines: list[str] = []

        def phase(self, msg: str) -> None:
            self.lines.append(msg)

        def bad(self, msg: str) -> None:
            self.lines.append("bad " + msg.split(" (see")[0])

    names = [f"p{i}.patch" for i in range(8)]
    DELAY = {n: 0.02 * (8 - i) for i, n in enumerate(names)}
    CATCH = {"p0.patch": {"t0"}, "p1.patch": {"t1", "t9"}, "p3.patch": {"lib:u3"}, "p5.patch": {"t5"}, "p7.patch": {"t7"}}
    live = [Row(f"s{i}", e, n, cells) for i, n in enumerate(names)
            for e, cells in (("postgres", (f"t{i}", "t9")), ("mysql", (f"lib:u{i}",)))]
    control = {c: "PASS" for r in live for c in r.cells}

    def drive(k: int, fail: str | None = None, book: stand_health.Book | None = None, control_stand=stand_health.ALIVE):
        w = World()
        if book is not None:
            w.on_run = lambda: book.note(stand_health.WATCH + "x", stand_health.down("the stand died"))

        def apply(tree, patch):
            with w.lock:
                assert w.applied.get(tree) is None, f"{patch.name} applied over {w.applied[tree]} in {tree}"
                if patch.name == fail:
                    raise RuntimeError("does not apply")
                w.applied[tree], w.applied_in[patch.name] = patch.name, tree
            return {tree: patch.name}

        def undo(tree, snap):
            with w.lock:
                assert w.applied[tree] == snap[tree], "restored a tree that holds another seed"
                w.applied[tree] = None

        trees = [Path(f"/t{i}") for i in range(k)]
        for t in trees:
            w.applied[t] = None
        led = Spy()
        try:
            got = grade_seeds(led, list(live), control, {}, [Fake(t, w) for t in trees], Path("/logs"), apply, undo, book, control_stand)
        except RuntimeError:
            got = None
        assert all(v is None for v in w.applied.values()), ("a tree still holds a seed", w.applied)
        return w, led, got

    serial, pooled = drive(1), drive(POOL)
    assert serial[0].max_building == 1 and pooled[0].max_building > 1, (serial[0].max_building, pooled[0].max_building)
    assert pooled[1].lines == serial[1].lines and len(pooled[1].lines) > len(names), (pooled[1].lines, serial[1].lines)
    status = lambda got: [(r.seed, r.engine, v.status, v.detail) for r, v in got.items()]
    assert status(pooled[2]) == status(serial[2]), (status(pooled[2]), status(serial[2]))
    by = {(r.seed, r.engine): v.status for r, v in pooled[2].items()}
    assert by[("s3", "mysql")] == CAUGHT and by[("s2", "mysql")] == BROKEN and by[("s4", "mysql")] == BROKEN, by
    assert by[("s6", "mysql")] == MISSED and by[("s0", "postgres")] == CAUGHT, by
    for w, _, _ in (serial, pooled):
        assert [p for p, _ in w.ran] == [n for n in names if n not in ("p2.patch", "p4.patch")], w.ran
        assert all(w.applied_in[p] == t for p, t in w.ran), ("a seed ran in a tree it was not applied to", w.ran, w.applied_in)
    assert len({t for _, t in pooled[0].ran}) == POOL
    for k in (1, POOL):
        died = drive(k, book=stand_health.Book())[2]
        assert {v.status for r, v in died.items() if r.seed in ("s0", "s1", "s3")} == {STAND_DOWN}, "a patch whose cells ran while the stand was down is not a catch"
    assert {v.status for v in drive(1, control_stand=stand_health.down("control died"))[2].values() if v.status != BROKEN} == {STAND_DOWN}, "a control that ran on a dead stand grades no patch"
    for k in (1, POOL):
        assert drive(k, fail="p5.patch")[2] is None, "an apply failure must stop the run, with every tree restored"


def switch_self_test() -> None:
    """The switch seam: a row and a source site must name each other, a stray RIVET_SEED never reaches a clean build, no gate flag turns the feature on."""
    import tempfile
    rows = parse_manifest(yaml.safe_load(MANIFEST.read_text()))
    assert any(r.switch for r in rows), "no row declares a switch"
    assert switch_problems(rows) == [], switch_problems(rows)
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "src").mkdir()
        (Path(d) / "src" / "a.rs").write_text('fn f() { seeded!("only-in-source", 1, 2); }\n')
        ghost = Row("ghost", "lib", "p", ("c",), switch="only-in-manifest")
        found = switch_problems([ghost], Path(d))
        assert len(found) == 2 and "only-in-manifest" in found[0] and "only-in-source" in found[1], found
        assert switch_problems([ghost], Path(d), complete=False) == found[:1]
    os.environ["RIVET_SEED"] = "leak"
    try:
        clean = Cargo(Path("/t"), Path("/b"), None)
        switched = Cargo(Path("/t"), Path("/b"), None, ("seeded",))
    finally:
        os.environ.pop("RIVET_SEED")
    assert "RIVET_SEED" not in clean.env and "RIVET_SEED" not in switched.env
    assert clean.feature_args == [] and switched.feature_args == ["--features", "seeded"]
    flagged = [str(f) for f in [ROOT / "Makefile", *(ROOT / ".github").rglob("*.yml")] if "--all-features" in f.read_text()]
    assert not flagged, f"--all-features turns the `seeded` feature on: {flagged}"


def self_test() -> int:
    """Manifest parsing and the CAUGHT / MISSED / STALE / BROKEN / NO-CONTROL logic, on fakes."""
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        pd = Path(d)
        WHY = "the code path does not exist on this engine"
        (pd / "p.patch").write_text("--- a/src/x.rs\n+++ b/src/x.rs\n@@ -1 +1 @@\n-a\n+b\n")
        eng = {"postgres": {"patch": "p.patch", "cells": ["t1", "lib:u1"]}, "mysql": {"patch": "p.patch", "cells": []},
               "mssql": {"na": WHY}, "mongo": {"na": WHY}, "oracle": {"na": WHY}}
        rows = parse_manifest({"seeds": [{"name": "s", "scope": "source", "engines": eng}]}, pd)
        assert [(r.engine, r.cells, r.na) for r in rows][:3] == [
            ("postgres", ("t1", "lib:u1"), None), ("mysql", (), None), ("mssql", (), WHY)], rows
        bad = [
            ({"seeds": []}, "no seeds"),
            ({"seeds": [{"name": "s", "scope": "nope", "engines": eng}]}, "scope"),
            ({"seeds": [{"name": "s", "scope": "source", "engines": {**eng, "mysql": None}}]}, "needs a patch"),
            ({"seeds": [{"name": "s", "scope": "source", "engines": {k: v for k, v in eng.items() if k != "mongo"}}]},
             "missing ['mongo']"),
            ({"seeds": [{"name": "s", "scope": "source", "engines": {**eng, "mongo": {"na": ""}}}]}, "takes a reason"),
            ({"seeds": [{"name": "s", "scope": "source", "engines": {**eng, "mongo": {"na": "n/a"}}}]}, "takes a reason"),
            ({"seeds": [{"name": "s", "scope": "source", "engines": {**eng, "mssql": {"patch": "gone.patch",
                                                                                      "cells": []}}}]}, "does not exist"),
            ({"seeds": [{"name": "s", "scope": "source", "engines": eng}] * 2}, "duplicate"),
            ({"seeds": [{"name": "s", "scope": "state", "engines": eng}]}, "outside the scope"),
        ]
        for doc, want in bad:
            try:
                parse_manifest(doc, pd)
            except ValueError as e:
                assert want in str(e), (want, str(e))
            else:
                raise AssertionError(f"accepted a manifest that should fail with {want!r}")
        wh = {"seeds": [{"name": "w", "scope": "target", "patch": "p.patch", "engines": {
            "bigquery": {"na": WHY}, "snowflake": {"cells": ["t1"]}, "clickhouse": {"na": WHY}}},
            {"name": "d", "scope": "destination", "patch": "p.patch", "engines": {
                "local": {"na": WHY}, "s3": {"cells": ["t2"]}, "gcs": {"na": WHY}, "azure": {"na": WHY}, "stdout": {"na": WHY},
                "lib": {"cells": ["lib:u1"]}}}]}
        assert [r.engine for r in parse_manifest(wh, pd) if r.cells] == ["snowflake", "s3", "lib"]
        for scope, drop in (("target", "clickhouse"), ("destination", "stdout")):
            short = {"seeds": [{**wh["seeds"][0 if scope == "target" else 1],
                                "engines": {k: v for k, v in wh["seeds"][0 if scope == "target" else 1]["engines"].items()
                                            if k != drop}}]}
            try:
                parse_manifest(short, pd)
            except ValueError as e:
                assert f"missing ['{drop}']" in str(e), str(e)
            else:
                raise AssertionError(f"a {scope} seed that leaves out {drop} was accepted")
    for scope, rel, name in (("source", "src/config/source.rs", "SourceType"), ("target", "src/config/load.rs", "LoadTargetKind"),
                             ("destination", "src/config/destination.rs", "DestinationType")):
        assert SCOPES[scope] == enum_variants(rel, name), (scope, SCOPES[scope])
    assert {"bigquery", "snowflake", "clickhouse"} <= set(SCOPES["target"]) and {"s3", "gcs", "azure"} <= set(SCOPES["destination"])
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "e.rs").write_text("pub enum Kind {\n    /// doc\n    #[serde(rename = \"a\")]\n    Alpha,\n"
                                      "    Beta { x: u8 },\n    Gamma(u8),\n}\nenum Other {\n    Zed,\n}\n")
        assert enum_variants("e.rs", "Kind", Path(d)) == ("alpha", "beta", "gamma")
        for bad_enum in ("Nope", "Empty"):
            (Path(d) / "e.rs").write_text("enum Empty {\n}\n")
            try:
                enum_variants("e.rs", bad_enum, Path(d))
            except ValueError:
                pass
            else:
                raise AssertionError(f"enum {bad_enum} read as variants")
    pg = Row("s", "postgres", "p.patch", ("t1", "t2"))
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, {"t1": "PASS", "t2": "FAIL"}).status == CAUGHT
    for stopped in ("TIMEOUT", "ABORT", "SIGKILL", "EXECFAIL", "SIGSEGV"):
        v = grade(pg, {"t1": "PASS", "t2": "PASS"}, {"t1": "PASS", "t2": stopped})
        assert v.status == ABORTED and stopped in v.detail, (stopped, v)
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, {"t1": "TIMEOUT", "t2": "FAIL"}).status == CAUGHT, "a real failure still catches"
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, {"t1": "PASS", "t2": "PASS"}).status == MISSED
    # red already on the unpatched tree proves nothing: that cell is not graded
    assert grade(pg, {"t1": "FAIL", "t2": "PASS"}, {"t1": "FAIL", "t2": "PASS"}).status == MISSED
    assert grade(pg, {"t1": "FAIL", "t2": "SKIP"}, {"t1": "FAIL", "t2": "FAIL"}).status == NO_CONTROL
    # a self-skip under the seed graded nothing, and a cell that did not run is not red
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, {"t1": "SKIP", "t2": "ABSENT"}).status == MISSED
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, None).status == BROKEN
    assert grade(Row("s", "mysql", "p.patch", ()), {}, {}).status == MISSED
    assert grade(Row("s", "mysql", "p.patch", ()), {}, None).status == BROKEN, "a cell-less seed must still compile"
    assert grade(Row("s", "mongo", na="immutable"), {}, {}).status == NA
    sample = (
        "        PASS [   1.0s] (1/4) rivet-cli::live_suite live_a::t1\n"
        " FAIL + LEAK [   2.0s] (2/4) rivet-cli::live_suite live_a::t2\n"
        "        PASS [   0.1s] (3/4) rivet-cli state::tests::u1\n"
        "        PASS [   0.1s] (4/4) rivet-cli::live_suite live_b::t1_longer\n"
    )
    got = cell_outcomes(sample, {"state::tests::u1": "no url"}, ("t1", "t2", "lib:u1", "t3"))
    assert got == {"t1": "PASS", "t2": "FAIL", "lib:u1": "SKIP", "t3": "ABSENT"}, got
    for status, want in (("TIMEOUT", "TIMEOUT"), ("ABORT", "ABORT"), ("SIGKILL", "SIGKILL"), ("FAIL", "FAIL"), ("FAIL + LEAK", "FAIL"),
                         ("PASS", "PASS"), ("LEAK", "PASS")):
        line = f"{status:>11} [   9.0s] (1/1) rivet-cli::live_suite live_a::t1\n"
        assert cell_outcomes(line, {}, ("t1",)) == {"t1": want}, (status, cell_outcomes(line, {}, ("t1",)))
    with tempfile.TemporaryDirectory() as td:
        (Path(td) / "tests").mkdir()
        (Path(td) / "tests" / "top_level.rs").write_text("fn offline_cell() {}\n")
        probe = Cargo.__new__(Cargo)
        probe.tree = Path(td)
        assert probe.binaries(["offline_cell", "live_cell", "lib:u1"]) == ["--test", "top_level", "--test", "live_suite", "--lib"]
        assert probe.binaries(["live_cell", "other_live"]) == ["--test", "live_suite"]
        assert probe.binaries([]) == ["--lib"]
        (Path(td) / "src").mkdir()
        (Path(td) / "src" / "a.rs").write_text("fn lib_cell() {}\nfn generic_cell<T>() {}\n")
        named = Row("s", "postgres", "p", ("offline_cell", "renamed_cell", "lib:lib_cell", "lib:generic_cell", "lib:offline_cell"))
        assert [c for _, c in unknown_cells([named], Path(td))] == ["renamed_cell", "lib:generic_cell", "lib:offline_cell"]
    patch = "--- a/src/a.rs\n+++ b/src/a.rs\n@@\n--- a/src/b.rs\n+++ b/src/b.rs\n@@\n"
    assert stale_message("s", patch, "error: patch failed: src/b.rs:12\nerror: src/b.rs: patch does not apply") \
        == "seed s no longer applies to src/b.rs: update the patch"
    assert "src/a.rs, src/b.rs" in stale_message("s", patch, "fatal: corrupt patch")
    assert recompiled("   Compiling rivet-cli v0.30.0 (/x)\n") and not recompiled("       Fresh rivet-cli v0.30.0\n")
    table = render([Verdict(pg, CAUGHT, "t2"), Verdict(Row("s", "mysql", "p", ()), MISSED, "none"),
                    Verdict(Row("s", "mongo", na="x"), NA, "x")])
    assert "recall: 1/2" in table and "MISSED     s × mysql" in table, table
    skip = Row("s", "mysql", "p", ("t1", "t2"))
    v = grade(skip, {"t1": "SKIP", "t2": "SKIP"}, {}, {"t1": "no SNOWFLAKE_TEST_CONNECTION"})
    assert v.status == NOT_GRADED_HERE and "no SNOWFLAKE_TEST_CONNECTION" in v.detail, v
    assert grade(skip, {"t1": "SKIP", "t2": "FAIL"}, {}).status == NO_CONTROL, "a red control is not a stand skip"
    assert grade(skip, {"t1": "SKIP", "t2": "PASS"}, {"t2": "SKIP"}).status == MISSED, \
        "a green control the seed made skip is no catch"
    dead = stand_health.down("the state DB stopped answering")
    both = {"t1": "PASS", "t2": "PASS"}
    assert grade(pg, both, {"t1": "PASS", "t2": "FAIL"}, stand=dead).status == STAND_DOWN, "a red seen while the watchdog had the stand down is no catch"
    assert grade(pg, {"t1": STAND, "t2": "PASS"}, {"t2": "PASS"}).status == STAND_DOWN, "a control that died on the stand is no missing control"
    assert grade(pg, both, {"t1": "PASS", "t2": STAND}).status == STAND_DOWN, "a seeded cell that died on the stand is no miss"
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, {"t1": STAND, "t2": "FAIL"}).status == CAUGHT, "a real failure beside a stand panic still catches"
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, {"t1": "PASS", "t2": "PASS"}, stand=stand_health.unreadable("docker events exited 1")).status == STAND_DOWN, \
        "an unreadable stand grades neither a catch nor a miss"
    stood = ("        FAIL [   0.2s] (1/2) rivet-cli::live_suite live_a::t1\n"
             "        FAIL [   0.2s] (2/2) rivet-cli::live_suite live_a::t2\n"
             "  stderr ───\n\n    thread 'live_a::t1' panicked at tests/common/env.rs:1:1:\n"
             "    live test requires Postgres reachable on 127.0.0.1:5433.\n\n"
             "    thread 'live_a::t2' panicked at tests/x.rs:2:2:\n    assertion `left == right` failed\n")
    assert cell_outcomes(stood, {}, ("t1", "t2")) == {"t1": STAND, "t2": "FAIL"}, cell_outcomes(stood, {}, ("t1", "t2"))
    crashed = ("        FAIL [   0.2s] (1/5) rivet-cli::live_suite live_a::t1\n"
               "        FAIL [   0.2s] (2/5) rivet-cli::live_suite live_a::t2\n"
               "        FAIL [   0.2s] (3/5) rivet-cli::live_suite live_a::t3\n"
               "        FAIL [   0.2s] (4/5) rivet-cli::live_suite live_a::t4\n"
               "        FAIL [   0.2s] (5/5) rivet-cli::live_suite live_a::t5\n"
               "  stderr ───\n\n    thread 'live_a::t1' panicked at tests/common/verify.rs:1768:9:\n"
               "    oracle error: dev/release_oracle/rig_oracle.py grade exited exit status: 1 after 0.4s (an oracle bug, never a verdict)\n"
               "    stderr:\n    ModuleNotFoundError: No module named 'duckdb'\n\n"
               "    thread 'live_a::t2' panicked at tests/common/verify.rs:255:13:\n"
               "    the oracle's grade panicked; rivet's own verdict was: exit Some(0)\n"
               "    --- rivet stderr (last 40 lines) ---\n    done\n"
               "    --- oracle panic ---\n    rig oracle printed no JSON verdict (EOF while parsing a value):\n\n"
               "    thread 'live_a::t3' panicked at tests/common/verify.rs:255:13:\n"
               "    the oracle's grade panicked; rivet's own verdict was: exit Some(0)\n"
               "    --- oracle panic ---\n    rig oracle: export 'e' disagrees with its source / rivet's own ledger (rig_oracle.py grade):\n"
               "      - COUNT(*): source 10, delivered 6\n\n"
               "    thread 'live_a::t4' panicked at tests/x.rs:2:2:\n    Connection reset by peer (os error 104)\n\n"
               "    thread 'live_a::t5' panicked at tests/x.rs:3:3:\n    assertion `left == right` failed\n")
    cells = ("t1", "t2", "t3", "t4", "t5")
    assert cell_outcomes(crashed, {}, cells) == {"t1": STAND, "t2": STAND, "t3": "FAIL", "t4": "FAIL", "t5": "FAIL"}, \
        "a red whose panic is the oracle's own crash graded nothing, a disagreement or a product io error is a verdict"
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, {"t1": STAND, "t2": STAND}).status == STAND_DOWN, "an oracle crash under a seed is no catch"
    led = Ledger(colour=False)
    assert not record(led, [Verdict(pg, STAND_DOWN, "dead")], ceiling=0) and not led.red and [c.status.name for c in led.cells] == ["INFRA"], \
        "a stand-down row is INFRA: the run is NOT GRADED, not red and not green"
    limit = gate_ceiling("not_graded_here_ceiling")
    for n, red in ((limit, False), (limit + 1, True)):
        rows = [Verdict(Row(f"s{i}", "mysql", "p", ("t1",)), NOT_GRADED_HERE, "skipped") for i in range(n)]
        led = Ledger(colour=False)
        assert record(led, rows) is (not red)
        assert sum(c.status.name == "SKIP" for c in led.cells) == n and led.red is red, (n, limit, led.cells)
    for status in (MISSED, STALE, BROKEN, NO_CONTROL, ABORTED):
        led = Ledger(colour=False)
        green = record(led, [Verdict(pg, CAUGHT, "t2"), Verdict(Row("s", "mysql", "p", ()), status, "x"),
                             Verdict(Row("s", "mongo", na="x"), NA, "x")], ceiling=0)
        assert led.red and not green and [c.status.name for c in led.cells] == ["PASS", "FAIL", "NA"], (status, led.cells)
    real = parse_manifest(yaml.safe_load(MANIFEST.read_text()))
    assert not unknown_cells(real), unknown_cells(real)
    why = "the code path does not exist on this engine"
    grown = {"seeds": [*yaml.safe_load(MANIFEST.read_text())["seeds"],
                       {"name": "zz-new-na-seed", "scope": "source", "engines": {e: {"na": why} for e in SCOPES["source"]}}]}
    added = [r for r in parse_manifest(grown) if r.seed == "zz-new-na-seed"]
    assert len(added) == len(SCOPES["source"]) and all(r.na == why for r in added), "a new seed of N/A rows parses with no other file edited"
    led = Ledger(colour=False)
    every = [Verdict(r, NA, r.na) for r in parse_manifest(grown) if r.na is not None]
    assert record(led, every) and not led.red and sum(c.status.name == "NA" for c in led.cells) == len(every), \
        "a valid N/A row is held by its own reason, not by a hand-summed ceiling"
    with tempfile.TemporaryDirectory() as d:
        pd = Path(d)
        base = {e: {"na": why} for e in SCOPES["source"]}
        for label, engines, want in (
                ("an empty reason", {**base, "mongo": {"na": ""}}, "takes a reason"),
                ("a blank reason", {**base, "mongo": {"na": " " * 40}}, "takes a reason"),
                ("a missing reason", {**base, "mongo": {"na": None}}, "takes a reason"),
                ("a reason with a cell beside it", {**base, "mongo": {"na": why, "cells": ["t1"]}}, "takes a reason"),
                ("an engine the scope does not name", {**base, "oracel": {"na": why}}, "outside the scope ['oracel']")):
            try:
                parse_manifest({"seeds": [{"name": "s", "scope": "source", "engines": engines}]}, pd)
            except ValueError as e:
                assert want in str(e), (label, str(e))
            else:
                raise AssertionError(f"{label} was accepted")
    led = Ledger(colour=False)
    assert not record(led, [Verdict(pg, ABORTED, "stopped")], ceiling=0) and led.red, "ABORTED fails the gate"
    other = "macos" if host_platform() == "linux" else "linux"
    away, here_row = Row("s", "postgres", "p", ("t1",), platform=other), Row("s", "mysql", "p", ("t1",), platform=host_platform())
    assert grade(away, {}, {}).status == OWED, "a row of another platform is owed, not NO-CONTROL"
    assert grade(here_row, {"t1": "PASS"}, {"t1": "FAIL"}).status == CAUGHT, "a row of this platform is graded"
    owed_row = [Verdict(away, OWED, "x")]
    led = Ledger(colour=False)
    assert record(led, owed_row, ceiling=0) and not led.red and [(c.status.name, c.cause.name) for c in led.cells] == [("SKIP", "DESIGN")], \
        "an owed row is a DESIGN skip, not red"
    led = Ledger(colour=False)
    assert not record(led, owed_row, ceiling=0, owed_fails=True) and led.red, "the island that owes a row fails when it cannot run it"
    assert platform_rows([away, pg, here_row]) == [away, here_row]
    assert {r.platform for r in real if r.seed == "ca-file-second-trust-anchor" and r.cells} == {"linux"}, \
        "the ca_file cells exist under cfg(target_os = \"linux\") only, so the seed declares it"
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "p.patch").write_text("x")
        doc = {"seeds": [{"name": "s", "scope": "state", "patch": "p.patch", "platform": "plan9",
                          "engines": {"postgres-state": {"cells": ["t1"]}, "sqlite-state": {"cells": ["t1"]}}}]}
        try:
            parse_manifest(doc, Path(d))
        except ValueError as e:
            assert "platform must be one of" in str(e), str(e)
        else:
            raise AssertionError("an unknown platform was accepted")
        doc["seeds"][0]["platform"] = "linux"
        assert {r.platform for r in parse_manifest(doc, Path(d))} == {"linux"}
    saved = os.environ.get("RIVET_GATE_STATE_URL")
    os.environ["RIVET_GATE_STATE_URL"] = "postgresql://u:p@127.0.0.1:5433/rivet_state"
    os.environ["RIVET_SELFTEST_CARRIER_BIN"] = ".gate-baseline/field/rivet"
    try:
        env = Cargo(Path("/t"), Path("/b"), "postgresql://u:p@127.0.0.1:5433/scratch").env
    finally:
        os.environ.pop("RIVET_GATE_STATE_URL")
        os.environ.pop("RIVET_SELFTEST_CARRIER_BIN")
        if saved is not None:
            os.environ["RIVET_GATE_STATE_URL"] = saved
    assert all(env[v].endswith("/scratch") for v in SCRATCH_STATE_VARS), {v: env.get(v) for v in SCRATCH_STATE_VARS}
    cargo = Cargo(Path("/t"), Path("/b"), "postgresql://u:p@127.0.0.1:5433/scratch")
    stage, again = live_modules_env("postgresql://u:p@127.0.0.1:5433/scratch")
    assert {k: cargo.env.get(k) for k in stage} == stage and cargo.again == again, (
        "recall's child env differs from the live-modules stage's", {k: cargo.env.get(k) for k in stage}, stage)
    assert cargo.env["RIVET_GATE_STATE_URL"] == "", "a SQLite-reading cell runs on SQLite, as in the stage"
    assert cargo.env["CARGO_PROFILE_DEV_DEBUG"] == "line-tables-only", "seed builds keep line tables, not full debuginfo"
    alone = exclusive_tests()
    assert alone, "the live suite marks some test live+exclusive"
    calls: list[tuple[list[str], dict]] = []
    real_sh = globals()["sh"]
    globals()["sh"] = lambda argv, cwd=None, env=None, timeout=None: (
        calls.append((list(argv), env or {})), subprocess.CompletedProcess(argv, 0, ""))[1]
    try:
        with tempfile.TemporaryDirectory() as d:
            cargo.run([alone[0], "a_shared_cell"], Path(d) / "x.log")
    finally:
        globals()["sh"] = real_sh
    runs = {("RIVET_TEST_EXCLUSIVE" in env, alone[0] in " ".join(argv)) for argv, env in calls}
    assert runs == {(True, True), (False, False)}, (
        "a live+exclusive cell runs alone under RIVET_TEST_EXCLUSIVE, the others without it", calls)
    assert env["RIVET_SELFTEST_CARRIER_BIN"] == str(ROOT / ".gate-baseline/field/rivet"), env["RIVET_SELFTEST_CARRIER_BIN"]
    assert env["RIVET_TEST_STATE_TOXI_URL"] == "postgresql://u:p@127.0.0.1:15433/scratch", env["RIVET_TEST_STATE_TOXI_URL"]
    stale = []
    for patch in sorted({r.patch for r in real if r.patch}):
        assert patch_files((HERE / patch).read_text()), f"{patch} changes no file"
        p = sh(["git", "-C", str(ROOT), "apply", "--check", str(HERE / patch)])
        if p.returncode != 0:
            stale.append(stale_message(patch.split("/")[0], (HERE / patch).read_text(), p.stdout))
    assert not stale, "a seed that no longer applies is a STALE row in the gate; re-cut it on this tree: " + "; ".join(stale)
    with tempfile.TemporaryDirectory() as td:
        rec = Path(td) / "sub" / run_record.RUNS_FILE
        assert "no seeded-recall record" in run_record.refusal("a" * 40, rec)
        run_record.append_run(rec, sha="a" * 40, complete=True, green=True, dirty=False)
        assert run_record.refusal("a" * 40, rec) is None and run_record.refusal("b" * 40, rec) is not None, "a written run must be the one the tag rule reads"
    pool_self_test()
    switch_self_test()
    print(f"self-test ok: seeded recall — manifest ({len(real)} rows), grading, build pool, stale and build checks, run record")
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry: grade the selected seeds against the local stand."""
    ap = argparse.ArgumentParser(prog="seeded-recall")
    ap.add_argument("--seeds", default="", help="comma-separated seed names (default: all)")
    ap.add_argument("--engines", default="", help="comma-separated engines (default: all)")
    ap.add_argument("--platform-rows", action="store_true", help="only the rows that declare a platform, failing a row of another platform (the island's run)")
    ap.add_argument("--self-test", action="store_true", help="check the manifest and the grading logic; no build")
    ns = ap.parse_args(argv)
    if ns.self_test:
        return self_test()
    rows = select(parse_manifest(yaml.safe_load(MANIFEST.read_text())), ns.seeds, ns.engines)
    if ns.platform_rows:
        rows = platform_rows(rows)
    led = Ledger()
    recall(led, rows, complete=not (ns.seeds or ns.engines or ns.platform_rows), owed_fails=ns.platform_rows)
    return 1 if led.red else 0


if __name__ == "__main__":
    sys.exit(main())
