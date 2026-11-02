"""Memory of the docker VM: a heavy version leg (Oracle, SQL Server) starts alone and only with headroom.

On 2026-11-01 the gate's `oracle:23-free` leg died while its container initialised (`ORA-12805: parallel query server
died unexpectedly`, sqlplus rc=137) because every other engine's version legs were starting beside it in the same VM.

    python3 -m dev.release_oracle.stand_memory --self-test
"""

from __future__ import annotations

import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

__all__ = ["HEAVY", "admitted"]

HEAVY = frozenset({"oracle", "mssql"})
FLOOR_GIB = 4.5
WAIT_SECS = 600.0
POLL_SECS = 15.0
KIB_PER_GIB = 1024 ** 2
_ONE_AT_A_TIME = threading.Lock()


def parse_meminfo(text: str) -> float | None:
    """GiB of `MemAvailable` in /proc/meminfo text; None when the line is missing or unreadable."""
    for line in text.splitlines():
        if line.startswith("MemAvailable:"):
            try:
                return int(line.split()[1]) / KIB_PER_GIB
            except (IndexError, ValueError):
                return None
    return None


def vm_available_gib() -> float | None:
    """Memory the docker VM can still hand out, read inside a throwaway container; None when unreadable."""
    try:
        p = subprocess.run(["docker", "run", "--rm", "--pull", "never", "alpine:3", "cat", "/proc/meminfo"],
                           capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return parse_meminfo(p.stdout) if p.returncode == 0 else None


def wait_for_headroom(read: Callable[[], float | None] = vm_available_gib, floor: float = FLOOR_GIB,
                      limit: float = WAIT_SECS, poll: float = POLL_SECS,
                      clock: Callable[[], float] = time.monotonic, nap: Callable[[float], None] = time.sleep) -> str:
    """Block until the VM has `floor` GiB available; the sentence to log ("" = there was room at once, nothing waited)."""
    start = clock()
    seen = read()
    if seen is None or seen >= floor:
        return ""
    while clock() - start < limit:
        nap(poll)
        seen = read()
        if seen is None or seen >= floor:
            return f"waited {clock() - start:.0f}s for the docker VM to free memory ({seen} GiB available, floor {floor:.0f})"
    return f"docker VM still under {floor:.0f} GiB available after {limit:.0f}s ({seen:.1f} GiB); starting anyway"


@contextmanager
def admitted(engine: str, say: Callable[[str], None],
             wait: Callable[[], str] = wait_for_headroom) -> Iterator[None]:
    """Hold the one heavy start slot (with headroom) for `engine`; other engines pass through."""
    if engine not in HEAVY:
        yield
        return
    with _ONE_AT_A_TIME:
        note = wait()
        if note:
            say(f"{engine}: {note}")
        yield


def self_test() -> None:
    """The reading parses, a start waits for headroom or gives up loudly, and only heavy engines queue."""
    assert parse_meminfo("MemTotal: 100 kB\nMemAvailable:    6291456 kB\n") == 6.0
    assert parse_meminfo("MemTotal: 1 kB\n") is None and parse_meminfo("MemAvailable: x kB\n") is None
    now = [0.0]

    def clock() -> float:
        return now[0]

    def nap(s: float) -> None:
        now[0] += s

    assert wait_for_headroom(lambda: 12.0, clock=clock, nap=nap) == "" and now[0] == 0.0
    assert wait_for_headroom(lambda: None, clock=clock, nap=nap) == "", "an unreadable VM does not block a start"
    reads = iter([2.0, 3.0, 7.5])
    said = wait_for_headroom(lambda: next(reads), clock=clock, nap=nap, poll=10.0)
    assert said.startswith("waited 20s") and now[0] == 20.0, said
    now[0] = 0.0
    gave_up = wait_for_headroom(lambda: 1.0, clock=clock, nap=nap, limit=30.0, poll=10.0)
    assert "starting anyway" in gave_up and now[0] == 30.0, gave_up
    held: list[str] = []
    with admitted("postgres", held.append, lambda: "never asked"):
        assert not _ONE_AT_A_TIME.locked(), "a light engine does not queue"
    with admitted("oracle", held.append, lambda: "waited 5s"):
        assert _ONE_AT_A_TIME.locked(), "a heavy engine holds the slot while it starts"
    assert not _ONE_AT_A_TIME.locked() and held == ["oracle: waited 5s"], held
    print("stand_memory self-test: ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
