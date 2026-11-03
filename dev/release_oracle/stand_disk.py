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

from .core import ROOT

__all__ = ["START_FLOOR_GIB", "WATCH_FLOOR_GIB", "free_gib", "start_refusal", "low", "disk_reason"]

START_FLOOR_GIB = 150  # ratchet-pin: gate-start-free-disk-floor
WATCH_FLOOR_GIB = 20  # ratchet-pin: gate-watchdog-free-disk-floor
GIB = 1024 ** 3


def host_free_gib(path: Path = ROOT) -> float | None:
    """Free GiB on the file system holding the checkout."""
    try:
        return shutil.disk_usage(path).free / GIB
    except OSError:
        return None


def vm_free_gib() -> float | None:
    """Free GiB on the docker daemon's data disk, read inside a throwaway container; None when unreadable."""
    try:
        p = subprocess.run(["docker", "run", "--rm", "--pull", "never", "alpine:3", "df", "-Pk", "/"],
                           capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return parse_df(p.stdout) if p.returncode == 0 else None


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


def start_refusal(free: dict[str, float | None], floor: float = START_FLOOR_GIB) -> str:
    """Why the gate must not start ("" = it may); an unreadable disk is refused like a full one."""
    short = [k for k, v in free.items() if v is None or v < floor]
    if not short:
        return ""
    return (f"DISK-LOW: {', '.join(short)} below the {floor:.0f} GiB start floor ({shown(free)}); "
            "a full gate writes ~170 GiB and a full disk crashes the stand's servers mid-run")


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
