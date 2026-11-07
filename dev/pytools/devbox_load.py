"""Devbox load watcher: load, free disk and a running release gate, read without touching any of them.

    python3 -m dev.pytools.devbox_load [--max-load N] [--min-free-gib G] [--path P]    exit 0 quiet, 3 busy
    python3 -m dev.pytools.devbox_load --self-test

"""

from __future__ import annotations

import argparse
import fnmatch
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dev.release_oracle.stand_disk import START_FLOOR_GIB, WATCH_FLOOR_GIB, host_free_gib  # noqa: E402

NIGHT = Path.home() / "night"
GATE_WORKTREE = Path.home() / "wt" / "gate"
STAND_WORKTREE = Path.home() / "wt" / "stand"
GATE_LOGS = Path.home() / "wt" / "gate-logs"
GATE_MODULE = "dev.release_oracle"
GATE_TARGETS = ("release-oracle", "release-oracle-full", "release-oracle-island")
LAUNCHER_PATTERN = "gate-*-launch.py"
BUSY_EXIT = 3


def is_gate(argv: list[str]) -> bool:
    """Whether this command line is the release gate: its main module, the make target that starts it, or its launcher script; never a self-test."""
    if not argv or "--self-test" in argv:
        return False
    prog = Path(argv[0]).name.lower()
    if prog in ("make", "gmake"):
        return any(t in argv for t in GATE_TARGETS)
    if prog.startswith(("python", "uv")):
        return any(a == "-m" and b == GATE_MODULE for a, b in zip(argv, argv[1:])) or any(fnmatch.fnmatch(Path(a).name, LAUNCHER_PATTERN) for a in argv[1:])
    return False


def protection(path: str | Path, gate: Path | None = None, stand: Path | None = None) -> str:
    """Why a worktree path is never swept ("" = free): the gate worktree or the stand worktree, or anything under either."""
    real = os.path.realpath(path)
    for why, root in (("the gate worktree", gate or GATE_WORKTREE), ("the stand worktree", stand or STAND_WORKTREE)):
        top = os.path.realpath(root)
        if real == top or real.startswith(top + os.sep):
            return why
    return ""


def gate_pids(rows: Iterable[tuple[int, str]]) -> list[int]:
    """The pids among (pid, command line) rows that run the release gate."""
    return [pid for pid, cmd in rows if is_gate(cmd.split())]


def ps_rows() -> list[tuple[int, str]]:
    """(pid, command line) of every process on the machine."""
    out = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True, text=True).stdout
    rows = []
    for line in out.splitlines():
        pid, _, cmd = line.strip().partition(" ")
        if pid.isdigit():
            rows.append((int(pid), cmd.strip()))
    return rows


def snapshot(path: Path | None = None) -> dict:
    """The machine now: load1, free GiB under `path` (None when unreadable), the gate pids and the operator's PAUSE file."""
    return {"load1": os.getloadavg()[0], "free_gib": host_free_gib(path or Path.home()), "gates": gate_pids(ps_rows()),
            "pause": (NIGHT / "PAUSE").exists()}


@dataclass
class Hold:
    """Pause above `max_load`, resume only at or below `resume_load`; a gate or a short disk always holds."""

    max_load: float
    resume_load: float
    min_free_gib: float = WATCH_FLOOR_GIB
    paused: bool = False

    def reason(self, snap: dict) -> str:
        """Why a new job must wait ("" = it may start), with hysteresis on the load."""
        if snap["pause"]:
            return f"{NIGHT / 'PAUSE'} exists (remove it to resume)"
        if snap["gates"]:
            return f"release gate running (pid {', '.join(map(str, snap['gates']))})"
        free = snap["free_gib"]
        if free is None or free < self.min_free_gib:
            return f"disk {'unreadable' if free is None else f'{free:.0f} GiB free'}, floor {self.min_free_gib:.0f} GiB"
        load = snap["load1"]
        self.paused = load > self.max_load or (self.paused and load > self.resume_load)
        return f"load1 {load:.1f} (pause above {self.max_load:.1f}, resume at {self.resume_load:.1f})" if self.paused else ""


def default_hold(max_load: float | None = None, min_free_gib: float = WATCH_FLOOR_GIB) -> Hold:
    """A hold at `max_load` (default: one per core), resuming at three quarters of it."""
    top = max_load if max_load is not None else float(os.cpu_count() or 1)
    return Hold(top, top * 0.75, min_free_gib)


def wait_quiet(hold: Hold, deadline: float, poll_s: float = 60.0, snap: Callable[[], dict] | None = None,
               sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> str:
    """Block until the hold lets a job start; return "" then, or the last reason once `deadline` (monotonic) passes."""
    while True:
        why = hold.reason((snap or snapshot)())
        if not why or clock() >= deadline:
            return why
        sleep(poll_s)


def gate_start_refusal(snap: dict, quiet_load: float = 4.0) -> str:
    """Why a weekly gate must not start now ("" = it may): another gate, a disk under the gate floor, or a loaded machine."""
    if snap["pause"]:
        return f"{NIGHT / 'PAUSE'} exists (remove it to resume)"
    if snap["gates"]:
        return f"another release gate is running (pid {', '.join(map(str, snap['gates']))})"
    free = snap["free_gib"]
    if free is None or free < START_FLOOR_GIB:
        return f"disk {'unreadable' if free is None else f'{free:.0f} GiB free'}, gate start floor {START_FLOOR_GIB:.0f} GiB"
    if snap["load1"] >= quiet_load:
        return f"machine not quiet: load1 {snap['load1']:.1f} >= {quiet_load:.1f}"
    return ""


def wait_gate_quiet(snap: Callable[[], dict], quiet_load: float, deadline: float, poll_s: float = 60.0,
                    sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> str:
    """Block until `gate_start_refusal` clears; return "" then, or the last refusal once `deadline` (monotonic) passes."""
    while True:
        why = gate_start_refusal(snap(), quiet_load)
        if not why or clock() >= deadline:
            return why
        sleep(poll_s)


def self_test() -> int:
    """The classifier, the hysteresis, the wait loop and a real process seen through the real ps."""
    gate = "/x/.venv/bin/python -m dev.release_oracle --with-seeded-recall"
    assert is_gate(gate.split()) and is_gate("uv run python -m dev.release_oracle --island".split())
    assert is_gate("make release-oracle-full ARGS=x".split()) and is_gate("/usr/bin/make release-oracle-island".split())
    for not_gate in ("python -m dev.release_oracle --self-test", "python -m dev.release_oracle.state_lib", "zsh -lc python -m dev.release_oracle",
                     "grep dev.release_oracle", "python other.py dev.release_oracle", "ssh devbox python -m dev.release_oracle", "make sweep-test-db", "python -m dev.release_oracle_x", ""):
        assert not is_gate(not_gate.split()), not_gate
    framework = "/Library/Frameworks/Python.framework/Versions/3.12/Resources/Python.app/Contents/MacOS/Python -m dev.release_oracle"
    assert is_gate(framework.split()), "macOS framework python shows as Python.app/Contents/MacOS/Python"
    assert gate_pids([(1, gate), (2, "grep dev.release_oracle"), (3, "make release-oracle-full")]) == [1, 3]
    launcher = "python3 /h/wt/gate-logs/launchers/gate-ab12cd34-launch.py"
    assert is_gate(launcher.split()) and is_gate(("uv run " + launcher).split()) and not is_gate((launcher + " --self-test").split()), "the launcher process lives for the whole run"
    assert not is_gate("python3 /h/wt/gate-logs/launchers/gate-launch.py x".split()) and not is_gate("vim /h/gate-ab12cd34-launch.py".split()) and not is_gate("python3 gate-ab-launch.pyc".split())
    assert is_gate("make release-oracle".split()) and is_gate("gmake release-oracle-full".split()) and is_gate("/opt/homebrew/bin/gmake release-oracle-island".split())
    assert not is_gate("make release-oracle-x".split()) and not is_gate("gmake sweep-test-db sweep-test-cloud".split())
    phases = [(5, "uv sync --frozen"), (6, "make sweep-test-db sweep-test-cloud"), (7, launcher)]
    assert gate_pids(phases) == [7], "uv sync and the sweeps between phases are covered by the launcher process alone"
    assert protection("/h/wt/gate/target/seeded/w1/tree", Path("/h/wt/gate"), Path("/h/wt/stand")) == "the gate worktree"
    assert protection("/h/wt/stand", Path("/h/wt/gate"), Path("/h/wt/stand")) == "the stand worktree"
    assert protection("/h/wt/gate2", Path("/h/wt/gate"), Path("/h/wt/stand")) == "" and protection("/h/wt/other", Path("/h/wt/gate"), Path("/h/wt/stand")) == ""

    quiet = {"load1": 1.0, "free_gib": 500.0, "gates": [], "pause": False}
    default = default_hold(8.0)
    assert (default.max_load, default.resume_load, default.min_free_gib) == (8.0, 6.0, WATCH_FLOOR_GIB), "resume mark is three quarters of the pause mark"
    hold = Hold(10, 7)
    assert hold.reason(quiet) == ""
    assert "load1" in hold.reason(quiet | {"load1": 11.0})
    assert hold.reason(quiet | {"load1": 8.0}), "paused stays paused until the load is at or below the resume mark"
    assert hold.reason(quiet | {"load1": 7.0}) == "" and hold.reason(quiet | {"load1": 9.0}) == "", "resumed stays resumed up to the max"
    assert "release gate running" in hold.reason(quiet | {"gates": [42]})
    assert "PAUSE" in hold.reason(quiet | {"pause": True}) and "PAUSE" in gate_start_refusal(quiet | {"pause": True})
    assert "disk" in hold.reason(quiet | {"free_gib": 5.0}) and "unreadable" in hold.reason(quiet | {"free_gib": None})
    assert gate_start_refusal(quiet) == ""
    assert "another release gate" in gate_start_refusal(quiet | {"gates": [9]})
    assert "start floor" in gate_start_refusal(quiet | {"free_gib": START_FLOOR_GIB - 1}) and "unreadable" in gate_start_refusal(quiet | {"free_gib": None})
    assert "not quiet" in gate_start_refusal(quiet | {"load1": 4.0}) and gate_start_refusal(quiet | {"load1": 3.9}) == ""

    now = [0.0]
    seq = iter([{**quiet, "gates": [1]}, {**quiet, "gates": [1]}, quiet])
    slept = []
    assert wait_quiet(Hold(10, 7), 100, 5, lambda: next(seq), lambda s: (slept.append(s), now.__setitem__(0, now[0] + s)), lambda: now[0]) == ""
    assert slept == [5, 5], f"held twice, then resumed: {slept}"
    now[0] = 0.0
    why = wait_quiet(Hold(10, 7), 12, 5, lambda: {**quiet, "gates": [1]}, lambda s: now.__setitem__(0, now[0] + s), lambda: now[0])
    assert "release gate running" in why, "a hold past its deadline returns the reason, not an empty go-ahead"

    probe = f"--probe-{os.getpid()}-{time.monotonic_ns()}"
    me = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", probe])
    try:
        seen = [cmd for pid, cmd in ps_rows() if pid == me.pid]
        assert seen and probe in seen[0], "the real ps lists a spawned process with its arguments"
        assert is_gate((seen[0] + " -m " + GATE_MODULE).split()) and not is_gate(seen[0].split()), "the listed command line is classified by its arguments"
    finally:
        me.terminate()
        me.wait()
    assert me.pid not in [pid for pid, _ in ps_rows()]
    quiet_now = {"load1": 1.0, "free_gib": 500.0, "gates": [], "pause": False}
    tick = [0.0]
    loads = iter([9.0, 9.0, 1.0])
    assert wait_gate_quiet(lambda: quiet_now | {"load1": next(loads)}, 4.0, 100, 5, lambda s: tick.__setitem__(0, tick[0] + s), lambda: tick[0]) == "" and tick[0] == 10
    tick[0] = 0.0
    assert "not quiet" in wait_gate_quiet(lambda: quiet_now | {"load1": 9.0}, 4.0, 12, 5, lambda s: tick.__setitem__(0, tick[0] + s), lambda: tick[0])
    snap = snapshot()
    assert isinstance(snap["load1"], float) and isinstance(snap["gates"], list) and isinstance(snap["pause"], bool) and shutil.disk_usage(Path.home()).free > 0
    print("devbox_load self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    """Print the machine state and exit 3 when a job should wait."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--max-load", type=float)
    ap.add_argument("--min-free-gib", type=float, default=WATCH_FLOOR_GIB)
    ap.add_argument("--path", type=Path, default=Path.home())
    a = ap.parse_args(argv)
    if a.self_test:
        return self_test()
    snap = snapshot(a.path)
    why = default_hold(a.max_load, a.min_free_gib).reason(snap)
    print(f"load1 {snap['load1']:.1f}  free {snap['free_gib']} GiB  gates {snap['gates']}  -> {'BUSY: ' + why if why else 'quiet'}")
    return BUSY_EXIT if why else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
