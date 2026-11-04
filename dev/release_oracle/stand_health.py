"""The stand's servers staying up, said once: one verdict (ALIVE / DOWN / UNREADABLE) that every stage grading a live cell reads.

On 2026-11-02 the state DB went into recovery mode (a backend SIGQUIT, then a docker VM stall); 76 cells failed with
`the database system is in recovery mode` and the gate graded them as product reds. The watchdog below reads the disks, the
state DB and the stand containers; a DOWN from any probe makes the run STAND-INVALID. A server or store is proven ready by
`ready`, one probe with a deadline. A cell that ran while the stand was DOWN or UNREADABLE (`judge`) is INFRA, never a
SKIP and never a FAIL with the panic text. None of it reads rivet's own output, so a product bug that cannot connect while
the server is healthy stays a product red.

    python3 -m dev.release_oracle.stand_health --self-test
"""

from __future__ import annotations

import os
import re
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from enum import Enum

try:
    from .stand_disk import disk_reason
except ImportError:  # pragma: no cover - bare sys.path entry
    from stand_disk import disk_reason  # type: ignore

__all__ = ["Kind", "StandVerdict", "ALIVE", "classify", "judge", "ready", "Probe", "Watchdog", "watchdog", "BringUpError"]

STATE_EVERY = 15.0
OOM_EVERY = 30.0
DISK_EVERY = 60.0
PROTOCOL_3 = 196608
DOWN_CLASS = "57"
WATCH = "stand:"
TCP_DEADLINE = 6.0
HTTP_DEADLINE = 15.0
PANIC = re.compile(r"live test requires (?:\w+ reachable on |`[^`]+` healthy|the stand at )")


class Kind(str, Enum):
    """What the evidence says about the stand."""

    ALIVE = "ALIVE"
    DOWN = "DOWN"
    UNREADABLE = "UNREADABLE"


@dataclass(frozen=True)
class StandVerdict:
    """The stand's state as one value: ALIVE, DOWN for a reason, or UNREADABLE for a reason."""

    kind: Kind
    reason: str = ""

    @property
    def alive(self) -> bool:
        return self.kind is Kind.ALIVE


ALIVE = StandVerdict(Kind.ALIVE)
_RANK = {Kind.ALIVE: 0, Kind.UNREADABLE: 1, Kind.DOWN: 2}


def down(reason: str) -> StandVerdict:
    """The stand is DOWN for `reason`."""
    return StandVerdict(Kind.DOWN, reason)


def unreadable(reason: str) -> StandVerdict:
    """The stand could not be read, for `reason`."""
    return StandVerdict(Kind.UNREADABLE, reason)


def classify(evidence: Iterable[StandVerdict]) -> StandVerdict:
    """The one verdict of the stand: the worst of the evidence (DOWN over UNREADABLE over ALIVE), with the reasons of that kind."""
    items = list(evidence)
    worst = max((v.kind for v in items), key=_RANK.__getitem__, default=Kind.ALIVE)
    if worst is Kind.ALIVE:
        return ALIVE
    return StandVerdict(worst, "; ".join(dict.fromkeys(v.reason for v in items if v.kind is worst and v.reason)))


def from_text(text: str) -> StandVerdict:
    """DOWN when a live cell's panic says its stand was not there (`require_alive`), else ALIVE."""
    return down(text.strip()[:300]) if PANIC.search(text) else ALIVE


class BringUpError(RuntimeError):
    """An engine x version that did not come up: the one failure `bring_up` raises, with the last probe's reason."""


class Book:
    """The stand facts of one run: watchdog verdicts as episodes with a start and an end, and which targets ever answered."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock, self._lock = clock, threading.Lock()
        self._episodes: list[list] = []
        self._up: set[str] = set()

    def mark(self) -> float:
        """Now, on the book's clock: the start of a window a later `judge` reads."""
        return self._clock()

    def note(self, source: str, verdict: StandVerdict) -> None:
        """Record what `source` says now: ALIVE closes its open episode, another kind opens one."""
        with self._lock:
            now = self._clock()
            open_ = next((e for e in self._episodes if e[0] == source and e[3] is None), None)
            if verdict.alive:
                self._up.add(source)
            if open_ is not None and open_[1].kind is verdict.kind:
                open_[1] = verdict
                return
            if open_ is not None:
                open_[3] = now
            if not verdict.alive:
                self._episodes.append([source, verdict, now, None])

    def seen_up(self, source: str) -> bool:
        """Did `source` answer at any time in this run?"""
        with self._lock:
            return source in self._up

    def judge(self, text: str = "", since: float | None = None) -> StandVerdict:
        """The verdict for a cell that ran from `since` (default: now): its panic text and every watchdog episode open since."""
        t0 = self.mark() if since is None else since
        with self._lock:
            held = [v for s, v, _, end in self._episodes if s.startswith(WATCH) and (end is None or end >= t0)]
        return classify([from_text(text), *held])


BOOK = Book()


def judge(text: str = "", since: float | None = None) -> StandVerdict:
    """`BOOK.judge`: how a stage reads the stand for a live cell."""
    return BOOK.judge(text, since)


def ready(name: str, probe: Callable[[], str], deadline: float, *, every: float = 1.0, nap: Callable[[float], None] = time.sleep,
          clock: Callable[[], float] = time.monotonic, book: Book | None = None) -> str:
    """"" once `probe` (returns "" = up, else why not) answers within `deadline` seconds, else why it did not; a target that answered before and now does not is a DOWN incident."""
    book = book or BOOK
    end = clock() + deadline
    while True:
        try:
            last = probe()
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        if not last:
            book.note(name, ALIVE)
            return ""
        left = end - clock()
        if left <= 0:
            break
        nap(min(every, left))
    why = f"not ready after {deadline:g}s: {last}"
    if book.seen_up(name):
        book.note(name, down(why))
    return why


def tcp_target(host: str, port: int) -> str:
    """The name a TCP target is recorded under."""
    return f"tcp {host}:{port}"


def store_target(store: str) -> str:
    """The name an object store is recorded under."""
    return f"store {store}"


def tcp_probe(host: str, port: int, timeout: float = 2.0) -> Callable[[], str]:
    """A probe that is up when a TCP connect to host:port succeeds."""
    def probe() -> str:
        try:
            socket.create_connection((host, port), timeout=timeout).close()
            return ""
        except OSError as e:
            return f"{host}:{port} {type(e).__name__}: {e}"
    return probe


def http_probe(url: str, timeout: float = 3.0) -> Callable[[], str]:
    """A probe that is up when `url` answers with any HTTP status (a 4xx is a server that answered)."""
    def probe() -> str:
        try:
            with urllib.request.urlopen(url, timeout=timeout):
                return ""
        except urllib.error.HTTPError:
            return ""
        except (urllib.error.URLError, OSError) as e:
            return f"{url} {type(e).__name__}: {e}"
    return probe


def reach_tcp(host: str, port: int, deadline: float = TCP_DEADLINE) -> bool:
    """Does host:port accept a connection within `deadline`?"""
    return not ready(tcp_target(host, port), tcp_probe(host, port), deadline)


def reach_http(store: str, url: str, deadline: float = HTTP_DEADLINE) -> bool:
    """Does the object store's health `url` answer within `deadline`?"""
    return not ready(store_target(store), http_probe(url), deadline)


def startup_packet(user: str, database: str) -> bytes:
    """The Postgres v3 StartupMessage for `user` on `database`."""
    body = struct.pack("!i", PROTOCOL_3) + b"".join(
        f"{k}\0{v}\0".encode() for k, v in (("user", user), ("database", database))) + b"\0"
    return struct.pack("!i", len(body) + 4) + body


def parse_reply(data: bytes) -> str:
    """Why a server's first reply to a StartupMessage says it is down ("" = it answered: an auth request, or an error that is not class 57)."""
    if not data:
        return "closed the connection without a reply"
    if data[:1] != b"E":
        return ""
    fields = {f[:1]: f[1:].decode(errors="replace") for f in data[5:].split(b"\0") if f}
    code = fields.get(b"C", "")
    return f"{code} {fields.get(b'M', '')}".strip() if code.startswith(DOWN_CLASS) else ""


def ask(host: str, port: int, user: str, database: str, timeout: float = 10.0) -> str:
    """Why the Postgres server at host:port is down ("" = it answered), from one StartupMessage; no password is sent."""
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall(startup_packet(user, database))
            return parse_reply(s.recv(4096))
    except OSError as e:
        return f"{type(e).__name__}: {e}"


def state_reason(url: str, ask_server: Callable[..., str] = ask, nap: Callable[[float], None] = time.sleep) -> str:
    """Why the gate's Postgres state DB at `url` stopped answering ("" = it answers); two refusals two seconds apart, so a blip is not a verdict."""
    u = urllib.parse.urlsplit(url)
    if not u.hostname:
        return ""
    args = (u.hostname, u.port or 5432, urllib.parse.unquote(u.username or "postgres"), u.path.lstrip("/") or "postgres")
    why = ask_server(*args)
    if why:
        nap(2.0)
        why = ask_server(*args)
    return f"the state DB {u.hostname}:{u.port or 5432} stopped answering ({why})" if why else ""


class DockerUnreadable(RuntimeError):
    """docker could not be asked: a non-zero exit, a missing binary or a timeout."""


def container_events(since: int, until: int) -> list[tuple[str, str, str]]:
    """(action, container, exit code) of every oom and die event between two unix times; DockerUnreadable when docker cannot say."""
    try:
        p = subprocess.run(["docker", "events", "--since", str(since), "--until", str(until), "--filter", "type=container",
                            "--filter", "event=oom", "--filter", "event=die",
                            "--format", "{{.Action}} {{.Actor.Attributes.name}} {{.Actor.Attributes.exitCode}}"],
                           capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise DockerUnreadable(f"docker events: {type(e).__name__}: {e}") from e
    if p.returncode != 0:
        raise DockerUnreadable(f"docker events exited {p.returncode}: {p.stderr.strip()[-200:]}")
    rows = [line.split() for line in p.stdout.splitlines()]
    return [(r[0], r[1], r[2] if len(r) > 2 else "") for r in rows if len(r) >= 2]


def container_running(name: str) -> bool:
    """Is the container running now?"""
    try:
        p = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", name], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise DockerUnreadable(f"docker inspect {name}: {type(e).__name__}: {e}") from e
    return p.returncode == 0 and p.stdout.strip() == "true"


def container_verdict(since: int, project: str, read: Callable[[int, int], list[tuple[str, str, str]]] = container_events,
                      running: Callable[[str], bool] = container_running, clock: Callable[[], float] = time.time) -> StandVerdict:
    """DOWN when a container of compose `project` was OOM-killed since `since`, or a stand container died with a non-zero exit and is still not running; UNREADABLE when docker cannot say."""
    from .core import ENGINE_PREFIX

    try:
        mine = [(a, n, c) for a, n, c in read(since, int(clock())) if n.startswith(f"{project}-")]
        oom = sorted({n for a, n, _ in mine if a == "oom"})
        if oom:
            return down(f"stand container {', '.join(oom)} OOM-killed")
        died = sorted({(n, c) for a, n, c in mine if a == "die" and c not in ("", "0") and not n.startswith(ENGINE_PREFIX)})
        gone = [f"{n} (exit {c})" for n, c in died if not running(n)]
    except DockerUnreadable as e:
        return unreadable(str(e))
    return down(f"stand container {', '.join(gone)} died and is not running") if gone else ALIVE


def reason_verdict(reason: str) -> StandVerdict:
    """A probe's reason text ("" = fine) as a verdict."""
    return down(reason) if reason else ALIVE


@dataclass(frozen=True)
class Probe:
    """One stand check: `read()` returns the stand's verdict, run at most every `every` seconds."""

    name: str
    read: Callable[[], StandVerdict]
    every: float


class Watchdog:
    """Runs each probe on its own cadence and notes every verdict in the book; the first DOWN reason is kept as `reason`."""

    def __init__(self, probes: Sequence[Probe] | None = None, tick: float = 5.0,
                 clock: Callable[[], float] = time.monotonic, book: Book | None = None) -> None:
        self.reason = ""
        self._probes = list(probes) if probes is not None else [Probe(WATCH + "disk", lambda: reason_verdict(disk_reason()), DISK_EVERY)]
        self._due = [0.0] * len(self._probes)
        self._tick, self._clock, self._book = tick, clock, book or BOOK
        self._said: set[str] = set()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> Watchdog:
        self._thread.start()
        return self

    def poll(self, force: bool = False) -> None:
        """One pass: every probe that is due (all when `force`) is read, a probe that raises reads UNREADABLE, and the first DOWN is kept and said."""
        now = self._clock()
        for i, probe in enumerate(self._probes):
            if not force and now < self._due[i]:
                continue
            self._due[i] = now + probe.every
            try:
                verdict = probe.read()
            except Exception as e:
                verdict = unreadable(f"{probe.name} probe raised {type(e).__name__}: {e}")
            self._book.note(probe.name, verdict)
            if verdict.kind is Kind.DOWN and not self.reason:
                self.reason = verdict.reason
                print(f"  ✗ STAND-INVALID: {verdict.reason}; the reds from here on are the stand's, not the product's", flush=True)
            elif verdict.kind is Kind.UNREADABLE and verdict.reason not in self._said:
                self._said.add(verdict.reason)
                print(f"  ⚠ STAND-UNREADABLE: {verdict.reason}; the cells that run now are INFRA, not graded", flush=True)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.poll()
            self._stop.wait(self._tick)

    def stop(self) -> str:
        """Stop, after one last reading of every probe; the reason (empty when the stand held)."""
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=5)
        self.poll(force=True)
        return self.reason


def watchdog(state_url: str, project: str | None = None) -> Watchdog:
    """The stand watchdog of one gate run: disks, the state DB (when the run has one) and container OOM kills and deaths."""
    started = int(time.time())
    project = project or os.environ.get("COMPOSE_PROJECT_NAME") or "rivet"
    probes = [Probe(WATCH + "disk", lambda: reason_verdict(disk_reason()), DISK_EVERY),
              Probe(WATCH + "containers", lambda: container_verdict(started, project), OOM_EVERY)]
    if state_url.startswith(("postgres://", "postgresql://")):
        probes.append(Probe(WATCH + "state DB", lambda: reason_verdict(state_reason(state_url)), STATE_EVERY))
    return Watchdog(probes)


def self_test() -> None:
    """A recovery-mode reply and a dead socket are the state DB down, an auth request is not; a container OOM or death is DOWN and an unreadable docker UNREADABLE; a cell reads the stand through one verdict; a target is proven ready by one probe with a deadline."""
    packet = startup_packet("rivet", "rivet_state")
    assert struct.unpack("!i", packet[:4])[0] == len(packet) and packet.endswith(b"user\0rivet\0database\0rivet_state\0\0")

    def error(code: str, msg: str) -> bytes:
        body = b"SFATAL\0C" + code.encode() + b"\0M" + msg.encode() + b"\0\0"
        return b"E" + struct.pack("!i", len(body) + 4) + body

    auth = b"R" + struct.pack("!ii", 8, 10)
    assert parse_reply(auth) == "", "an authentication request means the server answers"
    assert parse_reply(error("57P03", "the database system is in recovery mode")) == "57P03 the database system is in recovery mode"
    assert parse_reply(error("57P01", "terminating connection due to administrator command")).startswith("57P01")
    assert parse_reply(error("28P01", "password authentication failed")) == "", "a refused login is a live server"
    assert parse_reply(error("53300", "sorry, too many clients already")) == "", "a full connection table is a product question, not a down server"
    assert parse_reply(b"") != "", "a server that closes without a word is down"

    calls: list[str] = []
    napped: list[float] = []

    def flaky(*a: object) -> str:
        calls.append("x")
        return "boom" if len(calls) == 1 else ""

    assert state_reason("postgresql://rivet:pw@127.0.0.1:5433/rivet_state_gate_1", flaky, napped.append) == ""
    assert len(calls) == 2 and napped == [2.0], "one refusal is asked again before it is a verdict"
    lost = state_reason("postgresql://rivet:pw@127.0.0.1:5433/rivet_state_gate_1", lambda *a: "57P03 recovery", lambda s: None)
    assert lost == "the state DB 127.0.0.1:5433 stopped answering (57P03 recovery)", lost
    assert state_reason("postgresql://rivet:pw@127.0.0.1:5433/x", lambda *a: "", lambda s: None) == ""
    assert state_reason("", lambda *a: "never asked", lambda s: None) == "", "a run without a Postgres state has nothing to watch"

    ev = lambda *rows: (lambda a, b: list(rows))
    never = lambda n: False
    saw = container_verdict(100, "rivet", ev(("oom", "rivet-mssql-governor-1", ""), ("oom", "other-db-1", ""), ("oom", "rivet-mssql-governor-1", "")), never, lambda: 200.0)
    assert saw == down("stand container rivet-mssql-governor-1 OOM-killed"), saw
    assert container_verdict(100, "rivet", ev(), never, lambda: 200.0).alive
    assert container_verdict(100, "rivet", ev(("oom", "otherrivet-x-1", "")), never, lambda: 200.0).alive, "only the run's compose project counts"
    died = container_verdict(100, "rivet", ev(("die", "rivet-postgres-1", "137")), never, lambda: 200.0)
    assert died == down("stand container rivet-postgres-1 (exit 137) died and is not running"), died
    assert container_verdict(100, "rivet", ev(("die", "rivet-postgres-1", "137")), lambda n: True, lambda: 200.0).alive, "a container that came back is not down"
    assert container_verdict(100, "rivet", ev(("die", "rivet-postgres-1", "0")), never, lambda: 200.0).alive, "a clean stop is not a death"
    assert container_verdict(100, "rivet", ev(("die", "rivet-oracle-eng-pg-16", "1")), never, lambda: 200.0).alive, "a gate engine container is removed on purpose"

    def broken(a: int, b: int) -> list:
        raise DockerUnreadable("docker events exited 1: cannot connect to the Docker daemon")

    unread = container_verdict(100, "rivet", broken, never, lambda: 200.0)
    assert unread.kind is Kind.UNREADABLE and "cannot connect" in unread.reason, "an unreadable docker is UNREADABLE, never a quiet 'nothing happened'"

    from unittest import mock

    def refuse(*a: object, **k: object) -> object:
        return subprocess.CompletedProcess(a, 1, "", "Cannot connect to the Docker daemon")

    def missing(*a: object, **k: object) -> object:
        raise FileNotFoundError("docker")

    def timed_out(*a: object, **k: object) -> object:
        raise subprocess.TimeoutExpired("docker", 30)

    for fake in (refuse, missing, timed_out):
        with mock.patch.object(subprocess, "run", fake):
            try:
                container_events(0, 1)
            except DockerUnreadable:
                pass
            else:
                raise AssertionError(f"docker events that failed ({fake.__name__}) read as no events")
            assert container_verdict(0, "rivet").kind is Kind.UNREADABLE, fake.__name__
    with mock.patch.object(subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, "die rivet-postgres-1 137\noom rivet-x-1 \n", "")):
        assert container_events(0, 1) == [("die", "rivet-postgres-1", "137"), ("oom", "rivet-x-1", "")]

    assert classify([]) == ALIVE and classify([ALIVE, ALIVE]) == ALIVE
    assert classify([ALIVE, unreadable("a")]).kind is Kind.UNREADABLE
    assert classify([unreadable("a"), down("b"), down("c")]) == down("b; c"), "DOWN outranks UNREADABLE"
    assert from_text("live test requires Postgres reachable on 127.0.0.1:5433.").kind is Kind.DOWN
    assert from_text("live test requires `rivet-mongo` healthy (got \"starting\").").kind is Kind.DOWN
    assert from_text("live test requires the stand at http://x (refused)").kind is Kind.DOWN
    assert from_text("assertion failed: rows == 3").alive, "a product panic is not the stand"

    ticks = [0.0]
    book = Book(lambda: ticks[0])
    book.note(WATCH + "state DB", down("57P03 recovery"))
    t0 = book.mark()
    assert book.judge("", t0).kind is Kind.DOWN, "a cell that runs while an episode is open is graded by it"
    ticks[0] = 10.0
    book.note(WATCH + "state DB", ALIVE)
    ticks[0] = 20.0
    assert book.judge("", 15.0).alive, "an episode that closed before the cell began does not touch it"
    assert book.judge("", 5.0).kind is Kind.DOWN, "an episode that closed while the cell ran does"
    book.note(WATCH + "containers", unreadable("docker events exited 1"))
    assert book.judge("", 20.0).kind is Kind.UNREADABLE
    book.note("tcp 127.0.0.1:1", down("not ready"))
    assert "not ready" not in book.judge("", 0.0).reason, "a store that no cell used is not the verdict of every cell"

    beat = [0.0]

    def sleep(sec: float) -> None:
        beat[0] += sec

    gone = Book()
    tries: list[int] = []

    def later() -> str:
        tries.append(1)
        return "" if len(tries) == 3 else "refused"

    assert ready("t", later, 10.0, nap=sleep, clock=lambda: beat[0], book=gone) == "" and len(tries) == 3, "a blip is retried inside the deadline"
    why = ready("t", lambda: "refused", 3.0, nap=sleep, clock=lambda: beat[0], book=Book())
    assert why == "not ready after 3s: refused", why
    assert ready("t", lambda: 1 / 0, 1.0, nap=sleep, clock=lambda: beat[0], book=Book()).startswith("not ready after 1s: ZeroDivisionError"), "a raising probe is a reason, not a crash"
    assert ready("t", lambda: "refused", 1.0, nap=sleep, clock=lambda: beat[0], book=gone) != ""
    assert gone.judge("", 0.0).alive, "a target is never the stand's verdict until a cell reads it"
    assert gone.seen_up("t"), "a target that answered once is remembered, so a later refusal is an incident"

    dogbook = Book()
    stuck = Watchdog([Probe(WATCH + "x", lambda: 1 / 0, 1.0), Probe(WATCH + "y", lambda: down("y down"), 1.0)], book=dogbook)
    dogbook_t0 = dogbook.mark()
    stuck.poll()
    assert stuck.reason == "y down", "a probe that raises does not stop the probes after it"
    assert dogbook.judge("", dogbook_t0).kind is Kind.DOWN
    only = Book()
    Watchdog([Probe(WATCH + "x", lambda: 1 / 0, 1.0)], book=only).poll()
    assert only.judge("", 0.0).kind is Kind.UNREADABLE and "ZeroDivisionError" in only.judge("", 0.0).reason, "a raising probe is UNREADABLE"
    now = [0.0]
    called: list[int] = []
    cadence = Watchdog([Probe(WATCH + "q", lambda: called.append(1) or ALIVE, 60.0)], clock=lambda: now[0], book=Book())
    cadence.poll()
    now[0] = 30.0
    cadence.poll()
    assert len(called) == 1, "a probe is not read before it is due"
    now[0] = 61.0
    cadence.poll()
    assert len(called) == 2
    assert Watchdog([Probe(WATCH + "q", lambda: ALIVE, 1.0)], book=Book()).start().stop() == ""

    assert len(watchdog("postgresql://u@h/db")._probes) == 3 and len(watchdog("")._probes) == 2, "a Postgres state adds the state probe"
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    def serve(reply: bytes) -> None:
        conn, _ = server.accept()
        conn.recv(4096)
        conn.sendall(reply)
        conn.close()

    for reply, down_expected in ((error("57P03", "the database system is in recovery mode"), True), (auth, False)):
        t = threading.Thread(target=serve, args=(reply,))
        t.start()
        got = ask("127.0.0.1", port, "rivet", "db", timeout=3.0)
        t.join()
        assert bool(got) is down_expected, (reply, got)
    server.close()
    assert ask("127.0.0.1", port, "rivet", "db", timeout=1.0) != "", "a closed port is down"
    print("stand_health self-test: ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        print(__doc__)
