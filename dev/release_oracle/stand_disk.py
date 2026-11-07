"""Free disk on the host and on the docker VM's data disk: a floor before the gate starts, and the reading the stand watchdog of `stand_health` takes mid-run.

On 2026-10-22 a full gate filled the host disk; the docker VM's data disk shares it, so InnoDB
stopped on "Disk is full", the Postgres state server crashed, and ~150 cells went red with no
product cause. Below the start floor the gate does not begin; under the watchdog floor mid-run the
run is STAND-INVALID, a verdict above any product red, because its reds no longer grade the product.

    python3 -m dev.release_oracle.stand_disk --self-test
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

try:
    from .core import ROOT
except ImportError:  # pragma: no cover - bare sys.path entry
    from core import ROOT  # type: ignore

__all__ = ["HELPER_IMAGE", "START_FLOOR_GIB", "ISLAND_START_FLOOR_GIB", "WATCH_FLOOR_GIB", "free_gib", "start_refusal", "low",
           "disk_reason", "vm_run"]

START_FLOOR_GIB = 150  # ratchet-pin: gate-start-free-disk-floor (review 2027-02-01)
ISLAND_START_FLOOR_GIB = 60  # ratchet-pin: island-start-free-disk-floor (review 2027-02-01)
WATCH_FLOOR_GIB = 20  # ratchet-pin: gate-watchdog-free-disk-floor (review 2027-02-01)
GIB = 1024 ** 3
HELPER_IMAGE = "alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a"


def host_free_gib(path: Path = ROOT) -> float | None:
    """Free GiB on the file system holding the checkout."""
    try:
        return shutil.disk_usage(path).free / GIB
    except OSError:
        return None


def vm_run(*cmd: str, timeout: float = 120) -> str | None:
    """Stdout of `cmd` inside a throwaway container of the pinned helper image (pulled on first use); None when it failed."""
    try:
        p = subprocess.run(["docker", "run", "--rm", HELPER_IMAGE, *cmd], capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return p.stdout if p.returncode == 0 else None


def vm_free_gib() -> float | None:
    """Free GiB on the docker daemon's data disk, read inside a throwaway container; None when unreadable."""
    out = vm_run("df", "-Pk", "/")
    return None if out is None else parse_df(out)


def parse_df(text: str) -> float | None:
    """Free GiB from `df -Pk` output (the Available column of its one data row)."""
    rows = [ln.split() for ln in text.splitlines()[1:] if ln.strip()]
    try:
        return int(rows[0][3]) * 1024 / GIB
    except (IndexError, ValueError):
        return None


def free_gib() -> dict[str, float | None]:
    """Both readings, named."""
    return {"host": host_free_gib(), "docker VM": vm_free_gib()}


def shown(free: dict[str, float | None]) -> str:
    """`host 201 GiB, docker VM 177 GiB` (`unreadable` for a reading that failed)."""
    return ", ".join(f"{k} {'unreadable' if v is None else f'{v:.0f} GiB'}" for k, v in free.items())


def start_refusal(free: dict[str, float | None], island: bool = False) -> str:
    """Why the run must not start ("" = it may): the floor is the full gate's, or the island's; an unreadable disk is refused like a full one."""
    floor, what = (ISLAND_START_FLOOR_GIB, "an island run writes ~25 GiB") if island else (START_FLOOR_GIB, "a full gate writes ~170 GiB")
    short = [k for k, v in free.items() if v is None or v < floor]
    if not short:
        return ""
    return (f"DISK-LOW: {', '.join(short)} below the {floor:.0f} GiB start floor ({shown(free)}); "
            f"{what} and a full disk crashes the stand's servers mid-run")


def low(free: dict[str, float | None], floor: float = WATCH_FLOOR_GIB) -> str:
    """Why the running gate's stand is no longer valid ("" = it still is); an unreadable reading is not a verdict."""
    short = [k for k, v in free.items() if v is not None and v < floor]
    return f"{', '.join(short)} under {floor:.0f} GiB free ({shown(free)})" if short else ""


def disk_reason() -> str:
    """Why the stand's disks no longer grade the product ("" = they do)."""
    return low(free_gib())


def self_test() -> None:
    """The floors decide on both disks and an unreadable VM disk refuses the start."""
    assert parse_df("Filesystem 1024-blocks Used Available Capacity Mounted on\n"
                    "/dev/vdb1 412614656 226799616 185815040 55% /\n") == 185815040 * 1024 / GIB
    assert parse_df("") is None and parse_df("h\nx y z notanumber\n") is None
    assert start_refusal({"host": 201.0, "docker VM": 177.0}) == ""
    said = start_refusal({"host": 201.0, "docker VM": 19.0})
    assert said.startswith("DISK-LOW: docker VM below") and "host 201 GiB, docker VM 19 GiB" in said, said
    assert "host below" in start_refusal({"host": 149.9, "docker VM": 177.0})
    assert "docker VM below" in start_refusal({"host": 201.0, "docker VM": None}), "an unreadable VM disk refuses"
    assert start_refusal({"host": 129.0, "docker VM": 129.0}, island=True) == "", "the island starts on 129 GiB"
    assert "150 GiB start floor" in start_refusal({"host": 129.0, "docker VM": 129.0}), "a full gate does not start on 129 GiB"
    assert "60 GiB start floor" in start_refusal({"host": 59.0, "docker VM": 129.0}, island=True)
    assert START_FLOOR_GIB > ISLAND_START_FLOOR_GIB > WATCH_FLOOR_GIB
    seen: list[list[str]] = []
    real = subprocess.run
    subprocess.run = lambda cmd, **_: (seen.append(cmd), subprocess.CompletedProcess(cmd, 0, "out", ""))[1]  # type: ignore[assignment]
    try:
        assert vm_run("true") == "out" and seen[0][:3] == ["docker", "run", "--rm"] and "--pull" not in seen[0], seen
    finally:
        subprocess.run = real
    assert low({"host": 21.0, "docker VM": 177.0}) == ""
    assert low({"host": 201.0, "docker VM": None}) == "", "an unreadable reading mid-run is not a verdict"
    assert low({"host": 19.0, "docker VM": 177.0}).startswith("host under 20 GiB")
    print("stand_disk self-test: ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        f = free_gib()
        print(start_refusal(f) or f"disk ok: {shown(f)}")
