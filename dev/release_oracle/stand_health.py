"""The stand's servers staying up: the Postgres state DB answers, and no stand container is OOM-killed, for the whole run.

On 2026-11-02 the state DB went into recovery mode (a backend SIGQUIT, then a docker VM stall); 76 cells failed with
`the database system is in recovery mode` and the gate graded them as product reds. Both probes run in the stand
watchdog of `stand_disk`; a reason from either makes the run STAND-INVALID. Neither reads rivet's own output, so a
product bug that cannot connect while the server is healthy stays a product red.

    python3 -m dev.release_oracle.stand_health --self-test
"""

from __future__ import annotations

import os
import socket
import struct
import subprocess
import sys
import time
import urllib.parse
from collections.abc import Callable

from .stand_disk import Probe, Watchdog, disk_reason

__all__ = ["state_reason", "oom_reason", "watchdog"]

STATE_EVERY = 15.0
OOM_EVERY = 30.0
DISK_EVERY = 60.0
PROTOCOL_3 = 196608
DOWN_CLASS = "57"


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


def oom_events(since: int, until: int) -> list[str]:
    """Names of the containers docker saw OOM-killed between two unix times; empty when docker is unreadable."""
    try:
        p = subprocess.run(["docker", "events", "--since", str(since), "--until", str(until), "--filter", "type=container",
                            "--filter", "event=oom", "--format", "{{.Actor.Attributes.name}}"],
                           capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return []
    return p.stdout.split() if p.returncode == 0 else []


def oom_reason(since: int, project: str, read: Callable[[int, int], list[str]] = oom_events,
               clock: Callable[[], float] = time.time) -> str:
    """Why a stand container of compose `project` died of memory since `since` ("" = none did)."""
    hit = sorted({n for n in read(since, int(clock())) if n.startswith(f"{project}-")})
    return f"stand container {', '.join(hit)} OOM-killed" if hit else ""


def watchdog(state_url: str, project: str | None = None) -> Watchdog:
    """The stand watchdog of one gate run: disks, the state DB (when the run has one) and container OOM kills."""
    started = int(time.time())
    project = project or os.environ.get("COMPOSE_PROJECT_NAME") or "rivet"
    probes = [Probe(disk_reason, DISK_EVERY), Probe(lambda: oom_reason(started, project), OOM_EVERY)]
    if state_url.startswith(("postgres://", "postgresql://")):
        probes.append(Probe(lambda: state_reason(state_url), STATE_EVERY))
    return Watchdog(probes)


def self_test() -> None:
    """A recovery-mode reply and a dead socket are the state DB down, an auth request is not; an OOM event names the project's container; a healthy stand is silent."""
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
    down = state_reason("postgresql://rivet:pw@127.0.0.1:5433/rivet_state_gate_1", lambda *a: "57P03 recovery", lambda s: None)
    assert down == "the state DB 127.0.0.1:5433 stopped answering (57P03 recovery)", down
    assert state_reason("postgresql://rivet:pw@127.0.0.1:5433/x", lambda *a: "", lambda s: None) == ""
    assert state_reason("", lambda *a: "never asked", lambda s: None) == "", "a run without a Postgres state has nothing to watch"

    saw = oom_reason(100, "rivet", lambda a, b: ["rivet-mssql-governor-1", "other-db-1", "rivet-mssql-governor-1"], lambda: 200.0)
    assert saw == "stand container rivet-mssql-governor-1 OOM-killed", saw
    assert oom_reason(100, "rivet", lambda a, b: [], lambda: 200.0) == ""
    assert oom_reason(100, "rivet", lambda a, b: ["otherrivet-x-1"], lambda: 200.0) == "", "only the run's compose project counts"

    assert len(watchdog("postgresql://u@h/db")._probes) == 3 and len(watchdog("")._probes) == 2, "a Postgres state adds the state probe"
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    import threading

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
