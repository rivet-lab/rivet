"""no_secret_in_output matrix: a credential canary written into any credential slot never reaches any output.

Every axis is read from the product: the command tree from `rivet --help` recursion, the engines, destination types, load
targets and export modes from schemas/rivet.schema.json, the secret-bearing environment variables from src/. A command or
enum value the recipes do not cover is a red guard row, never a skip. Each run owns unique canaries; every byte of every
sink (stdout, stderr, each file the run wrote) is searched for the plain, percent-encoded and base64 forms of each canary.
A slot the run never connected to is NOT GRADED (an ENV skip), a leak is a FAIL naming engine, command, spelling, sink and line.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures as cf
import dataclasses
import contextlib
import hashlib
import itertools
import json
import os
import re
import resource
import secrets
import select
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import urllib.parse
from pathlib import Path

try:
    from . import property_no_secret_in_output_known
    from .core import Ledger, SkipCause, Status, rivet_bin
except ImportError:  # pragma: no cover
    import property_no_secret_in_output_known  # type: ignore
    from core import Ledger, SkipCause, Status, rivet_bin  # type: ignore

__all__ = ["verify_credential_never_reaches_a_sink"]

REPO = Path(__file__).resolve().parents[2]
SPECIALS = "@:/?#&%é"
EXPORT = "canary_orders"
TIMEOUT_S = 90
CONNECT_VARIANTS = (("plain", {}, ()), ("trace-json", {"RUST_LOG": "trace"}, ("--json-errors",)))
SCHEME = {"postgres": "postgresql", "mysql": "mysql", "mssql": "sqlserver", "oracle": "oracle", "mongo": "mongodb"}
AMBIENT_ECOSYSTEM = ("PGPASSWORD", "MYSQL_PWD", "MSSQL_SA_PASSWORD", "SNOWSQL_PWD", "AWS_SECRET_ACCESS_KEY",
                     "AWS_SESSION_TOKEN", "AZURE_STORAGE_KEY", "AZURE_STORAGE_SAS_TOKEN", "DATABASE_URL")
SECRET_ENV_NAME = re.compile(r"PASS|SECRET|TOKEN|KEY|CRED|URL")
ENV_READ = re.compile(r'env::var(?:_os)?\(\s*"([A-Z][A-Z0-9_]+)"')
DIRECT_ENV = {"RIVET_STATE_URL", "GOOGLE_APPLICATION_CREDENTIALS"}
CONNECT_SHAPED = re.compile(r"refused|connect|reset|unreachable|timed? ?out|eof|broken pipe|handshake|closed|resolve", re.I)
PANIC = re.compile(r"panicked at|RUST_BACKTRACE", re.I)


@dataclasses.dataclass(frozen=True)
class Canary:
    """One run's unique secret: its plain text and two alnum tokens that survive a truncating redactor."""

    plain: str
    head: str
    tail: str

    @property
    def quoted(self) -> str:
        """The secret as a URL userinfo or query value carries it."""
        return urllib.parse.quote(self.plain, safe="")

    @property
    def pct_all(self) -> str:
        """Every byte of the secret written as `%XX`."""
        return "".join(f"%{b:02X}" for b in self.plain.encode())


def canary(tag: str) -> Canary:
    """A password no other run, cell or slot shares: tokens around URL-hostile characters and a non-ASCII letter."""
    h = hashlib.sha256(tag.encode()).hexdigest()
    head, tail = "Cq" + h[:14], "Zk" + h[14:28]
    return Canary(f"{head}{SPECIALS}{tail}", head, tail)


def token_canary(tag: str) -> Canary:
    """A secret that is one plain token (an access key, a webhook path segment, an API token)."""
    h = hashlib.sha256(tag.encode()).hexdigest()
    head, tail = "Qx" + h[:14], "Vm" + h[14:28]
    return Canary(head + tail, head, tail)


def _b64_runs(raw: bytes) -> dict[str, bytes]:
    """The base64 text `raw` leaves in a stream at each of the three byte alignments, standard and URL-safe alphabets."""
    out: dict[str, bytes] = {}
    for k, skip in ((0, 0), (1, 2), (2, 3)):
        padded = b"\0" * k + raw
        enc = base64.b64encode(padded).decode().rstrip("=")
        if len(padded) % 3:
            enc = enc[:-1]
        enc = enc[skip:]
        out[f"b64+{k}"] = enc.encode()
        out[f"b64url+{k}"] = enc.replace("+", "-").replace("/", "_").encode()
    return out


def forms(c: Canary) -> dict[str, bytes]:
    """Every spelling of `c` a sink could hold: plain, its tokens, percent-encoded, JSON-escaped, base64 at three alignments, hex."""
    out: dict[str, bytes] = {"plain": c.plain.encode(), "head": c.head.encode(), "tail": c.tail.encode(),
                             "pct": c.quoted.encode(), "pct-lower": c.quoted.lower().encode(),
                             "pct-all": c.pct_all.encode(), "pct-all-lower": c.pct_all.lower().encode(),
                             "hex-head": c.head.encode().hex().encode(), "hex-tail": c.tail.encode().hex().encode(),
                             "json": json.dumps(c.plain)[1:-1].encode(), "json-raw": json.dumps(c.plain, ensure_ascii=False)[1:-1].encode(),
                             "json-slash": json.dumps(c.plain)[1:-1].replace("/", "\\/").encode(),
                             "json-pct": json.dumps(c.quoted)[1:-1].encode()}
    for name, raw in (("plain", c.plain.encode()), ("head", c.head.encode()), ("tail", c.tail.encode())):
        out.update({f"{name}:{k}": v for k, v in _b64_runs(raw).items()})
    return out


@dataclasses.dataclass(frozen=True)
class Hit:
    """One canary form found in one sink."""

    role: str
    form: str
    sink: str
    line: str


def find_hits(roles: dict[str, Canary], sinks: dict[str, bytes]) -> list[Hit]:
    """Every (role, form, sink) where a canary form occurs, with the line it sits on."""
    hits: list[Hit] = []
    for role, c in roles.items():
        for form, needle in forms(c).items():
            for sink, data in sinks.items():
                i = data.find(needle)
                if i < 0:
                    continue
                a, b = data.rfind(b"\n", 0, i) + 1, data.find(b"\n", i)
                line = data[a: b if b >= 0 else len(data)][:300].decode("utf-8", "replace")
                hits.append(Hit(role, form, sink, line))
    return hits


def leaks(roles: dict[str, Canary], sinks: dict[str, bytes]) -> list[Hit]:
    """The hits that are leaks: a credential reaching the endpoint it authenticates against is the protocol, not a leak."""
    return [h for h in find_hits(roles, sinks) if not h.sink.endswith(f"/request:{OWN_STUB.get(h.role)}")]


def schema_enum(defn: str) -> list[str]:
    """The string values a `$defs` entry of the config schema allows, across `enum`, `const` and `oneOf`."""
    d = json.loads((REPO / "schemas" / "rivet.schema.json").read_text())["$defs"][defn]
    vals: list[str] = []

    def walk(n: dict) -> None:
        vals.extend(n.get("enum") or ([n["const"]] if "const" in n else []))
        for sub in n.get("oneOf", []) + n.get("anyOf", []):
            walk(sub)

    walk(d)
    return vals


SIGNER_PREFIX = {"aws": "AWS_", "azure": "AZ", "google": "GOOGLE_"}
DECOY_KIND = ((re.compile(r"_FILE$|CREDENTIALS$"), "file"), (re.compile(r"_ENDPOINT(_URL)?$|_HOST$"), "redirect"),
              (re.compile(r"SECRET|_KEY$|_KEY_ID$|_TOKEN$"), "secret"), (re.compile(r"_PROFILE$"), "profile"))
METADATA_URL = re.compile(r'"(http://(?:169\.254\.169\.254|metadata\.google\.internal)[^"{]*)')


def signer_module(dest: str) -> str | None:
    """The reqsign module that signs the requests of one destination type, None for a type that signs nothing."""
    match dest:
        case "s3":
            return "aws"
        case "azure":
            return "azure"
        case "gcs":
            return "google"
        case "local" | "stdout":
            return None
    raise KeyError(f"destination type `{dest}` has no signer module")


def signer_source(module: str) -> list[Path]:
    """The Rust sources of one module of the reqsign version Cargo.lock pins, empty when the registry holds none."""
    lock = (REPO / "Cargo.lock").read_text()
    m = re.search(r'name = "reqsign"\nversion = "([^"]+)"', lock)
    home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    roots = sorted(home.glob(f"registry/src/*/reqsign-{m.group(1)}/src/{module}")) if m else []
    return sorted(roots[0].rglob("*.rs")) if roots else []


def ambient_identity(module: str) -> tuple[dict[str, str], list[str], list[str]]:
    """The environment names one signer module reads with the decoy kind each takes, its home-relative well-known files, and its fixed metadata URLs."""
    names: dict[str, str] = {}
    files: set[str] = set()
    urls: set[str] = set()
    for p in signer_source(module):
        text = p.read_text(errors="replace")
        for n in re.findall(r'"([A-Z][A-Z0-9]*_[A-Z0-9_]+)"', text):
            if n.startswith(SIGNER_PREFIX[module]):
                names[n] = next((k for rx, k in DECOY_KIND if rx.search(n)), "selector")
        files.update(re.findall(r'"~/([^"]+)"', text) + re.findall(r'"\{config_dir\}/([^"]+)"', text))
        urls.update(METADATA_URL.findall(text))
    return names, sorted(files), sorted(urls)


REDIRECT_ENV_NAME = re.compile(r"ENDPOINT|_HOST$|_ADDR$")


def product_redirect_env() -> list[str]:
    """Environment variables `src/` reads by a literal name that move where an authenticated request goes, tests and test seams left out."""
    found: set[str] = set()
    for p in (REPO / "src").rglob("*.rs"):
        if "tests" in p.parts or p.name.startswith("tests") or p.name == "test_hook.rs":
            continue
        found.update(n for n in ENV_READ.findall(p.read_text(errors="replace")) if REDIRECT_ENV_NAME.search(n) and not n.startswith("RIVET_TEST_"))
    return sorted(found)


def redirect_slot(name: str) -> str:
    """The credential slot whose requests one redirecting environment variable moves."""
    match name:
        case "RIVET_BQ_API_ENDPOINT":
            return "bigquery"
    raise KeyError(f"redirecting environment variable `{name}` has no recipe")


def bigquery_spellings() -> list[str]:
    """The BigQuery leg's credential sources: the ambient key, each destination key field the leg signs with, and each redirecting variable."""
    fields = [f"destination-{f}" for f in schema_fields("DestinationConfig")
              if DEST_CREDENTIAL_FIELD.search(f) and f != "allow_anonymous" and "gcs" in credential_field_dests(f)]
    redirects = []
    for n in product_redirect_env():
        with contextlib.suppress(KeyError):
            redirects += [f"redirect-{n}"] if redirect_slot(n) == "bigquery" else []
    return ["service-account-key", *fields, *redirects]


def product_secret_env() -> list[str]:
    """Secret-looking environment variables `src/` reads by a literal name, tests and the test seams left out."""
    found: set[str] = set()
    for p in (REPO / "src").rglob("*.rs"):
        if "tests" in p.parts or p.name.startswith("tests") or p.name == "test_hook.rs":
            continue
        for name in ENV_READ.findall(p.read_text(errors="replace")):
            if SECRET_ENV_NAME.search(name) and not name.startswith("RIVET_TEST_") and name not in DIRECT_ENV:
                found.add(name)
    return sorted(found)


@dataclasses.dataclass(frozen=True)
class Arg:
    """A required argument of a command: its flag (None for a positional) and value name."""

    flag: str | None
    value: str | None


@dataclasses.dataclass(frozen=True)
class Cmd:
    """A leaf of the clap command tree: what its usage line requires and what else its help offers."""

    path: tuple[str, ...]
    required: tuple[tuple[Arg, ...], ...]
    options: frozenset[str] = frozenset()
    writers: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        """The command as typed after `rivet`."""
        return " ".join(self.path)


_GROUP = re.compile(r"<(--[\w-]+(?: <[^<>]+>)?(?:\|--[\w-]+(?: <[^<>]+>)?)+)>")
_OPT = re.compile(r"(--[\w-]+)(?: <([^<>]+)>)?")
_POS = re.compile(r"<([A-Za-z_]+)>(?:\.\.\.)?")
_HELP_OPT = re.compile(r"^\s+(?:-\w, )?(--[\w-]+)(?: <([^>]+)>)?", re.M)
WRITER_VALUES = {"OUTPUT", "PATH", "DIR"}
WRITER_FLAGS = ("--output", "--summary-output", "--checkpoint")


def parse_usage(usage: str) -> tuple[tuple[Arg, ...], ...]:
    """The required arguments in a clap usage tail: one tuple of alternatives per requirement."""
    rest = re.sub(r"\[[^\]]*\]", " ", usage)
    reqs: list[tuple[Arg, ...]] = []
    for g in _GROUP.findall(rest):
        reqs.append(tuple(Arg(m.group(1), m.group(2)) for m in _OPT.finditer(g)))
    rest = _GROUP.sub(" ", rest)
    for m in _OPT.finditer(rest):
        reqs.append((Arg(m.group(1), m.group(2)),))
    rest = _OPT.sub(" ", rest)
    reqs.extend((Arg(None, m.group(1)),) for m in _POS.finditer(rest))
    return tuple(reqs)


def _help(binary: Path, path: tuple[str, ...]) -> str:
    """`rivet <path> --help`."""
    return subprocess.run([str(binary), *path, "--help"], capture_output=True, text=True, timeout=60).stdout


def command_tree(binary: Path) -> list[Cmd]:
    """Every leaf subcommand of the binary, read from its own help output."""
    leaves: list[Cmd] = []

    def walk(path: tuple[str, ...]) -> None:
        text = _help(binary, path)
        m = re.search(r"^Commands:\n(.*?)(?:\n\n|\nOptions:|\Z)", text, re.S | re.M)
        kids = [k.group(1) for k in (re.match(r"^  ([a-z][a-z0-9-]*)\s", ln) for ln in (m.group(1).splitlines() if m else []))
                if k and k.group(1) != "help"]
        if kids:
            for k in kids:
                walk((*path, k))
            return
        u = re.search(r"^Usage: (.*)$", text, re.M)
        tail = u.group(1).split(" ", 1 + len(path))[-1] if u else ""
        opts = {m.group(1): m.group(2) or "" for m in _HELP_OPT.finditer(text)}
        writers = tuple(f for f in WRITER_FLAGS if opts.get(f) in WRITER_VALUES)
        leaves.append(Cmd(path, parse_usage(tail), frozenset(opts), writers))

    walk(())
    return leaves


WIRE_CAP = 1 << 22
OWN_STUB = {"source": "source", "state": "state", "load": "load", "bigquery key": "load", "snowflake key": "load",
            "gcp key": "token", "gcp access token": "token", "webhook": "notify", "gcs key": "dest", "s3 access key": "dest",
            "s3 secret key": "dest", "s3 session token": "dest", "azure account key": "dest", "azure sas token": "dest"}


class Stub:
    """A loopback listener that counts connections: it resets them (`rst`), refuses a PostgreSQL login (`pg`), answers HTTP 500 (`http`) or relays to a real server (`fwd`)."""

    def __init__(self, kind: str, upstream: int = 0, body: bytes = b"", host: str = "127.0.0.1") -> None:
        self.kind, self.count, self.password, self.wire, self.upstream, self.body = kind, 0, None, bytearray(), upstream, body
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, 0))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        self._stop = False
        threading.Thread(target=self._accept, daemon=True).start()

    def close(self) -> None:
        """Stop accepting."""
        self._stop = True
        try:
            self.sock.close()
        except OSError:
            pass

    def _accept(self) -> None:
        """Hand each connection to its own thread until closed."""
        while not self._stop:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            self.count += 1
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        """Run the stub's script on one connection."""
        conn.settimeout(3)
        try:
            getattr(self, f"_{self.kind}")(conn)
        except OSError:
            pass
        finally:
            conn.close()

    def _rst(self, conn: socket.socket) -> None:
        """Read what the client opens with, then reset."""
        try:
            self.wire += conn.recv(65536)
        except OSError:
            pass
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")

    def _fwd(self, conn: socket.socket) -> None:
        """Relay the connection to the real server so the cell counts its reaches."""
        up = socket.create_connection(("127.0.0.1", self.upstream), timeout=5)
        conn.settimeout(None)
        up.settimeout(None)
        try:
            while True:
                ready, _, _ = select.select([conn, up], [], [], 30)
                if not ready:
                    return
                for sock in ready:
                    data = sock.recv(65536)
                    if not data:
                        return
                    if sock is conn and len(self.wire) < WIRE_CAP:
                        self.wire += data
                    (up if sock is conn else conn).sendall(data)
        finally:
            up.close()

    def _http(self, conn: socket.socket) -> None:
        """Read one request, answer 500."""
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = conn.recv(65536)
            if not chunk:
                return
            buf += chunk
        self.wire += buf
        if self.body:
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" % len(self.body) + self.body)
            return
        conn.sendall(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")

    def _pg(self, conn: socket.socket) -> None:
        """Ask for a cleartext password, record it, refuse with 28P01."""
        while True:
            head = self._take(conn, 4)
            body = self._take(conn, int.from_bytes(head, "big") - 4)
            if int.from_bytes(body[:4], "big") != 80877103:
                break
            conn.sendall(b"N")
        conn.sendall(b"R\x00\x00\x00\x08\x00\x00\x00\x03")
        kind = self._take(conn, 1)
        size = int.from_bytes(self._take(conn, 4), "big")
        data = self._take(conn, size - 4)
        if kind == b"p":
            self.password = data.split(b"\0")[0].decode("utf-8", "replace")
        err = b"SFATAL\0C28P01\0Mpassword authentication failed for user \"rivet\"\0\0"
        conn.sendall(b"E" + (len(err) + 4).to_bytes(4, "big") + err)

    @staticmethod
    def _take(conn: socket.socket, n: int) -> bytes:
        """Exactly `n` bytes from the connection."""
        buf = b""
        while len(buf) < n:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                raise OSError("closed")
            buf += chunk
        return buf


@dataclasses.dataclass
class Scene:
    """One prepared run: the working directory, the arguments, the environment and what the run is graded for."""

    root: Path
    argv: list[str]
    env: dict[str, str]
    roles: dict[str, Canary]
    stubs: dict[str, Stub]
    note: str = ""
    forbidden: dict[str, Canary] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class Spec:
    """One cell of the matrix."""

    command: Cmd
    engine: str
    slot: str
    spelling: str
    path: str = "dead-host"
    mode: str = "full"

    @property
    def scenario(self) -> str:
        """The ledger scenario: command, slot and spelling."""
        return f"no_secret_in_output/{self.command.name}/{self.slot}:{self.spelling}"

    @property
    def store(self) -> str:
        """The ledger store: the path and mode the cell runs."""
        return f"{self.path}/{self.mode}"


SRC_SPELLINGS = ("userinfo", "userinfo-raw", "query-password", "query-PASSWORD", "query-PaSsWoRd", "query-pct-key",
                 "query-pct-value", "fields", "password-env", "keyword", "url-env", "url-file", "ambient-env")
URL_SPELLINGS = tuple(s for s in SRC_SPELLINGS if s not in ("fields", "password-env", "url-env", "url-file"))
QUERY_KEYS = {"query-password": "password", "query-PASSWORD": "PASSWORD", "query-PaSsWoRd": "PaSsWoRd", "query-pct-key": "pass%77ord"}
STATE_SPELLINGS = ("userinfo", "query-password", "query-PASSWORD", "query-PaSsWoRd", "query-pct-key", "query-pct-value",
                   "query-sslpassword", "query-password-tls")
LOAD_SPELLINGS = ("url-userinfo", "url-query-password", "password-env")
USER = {"mssql": "sa"}
DB = {"oracle": "rivet"}
MODE_FIELDS = {"full": {}, "incremental": {"cursor_column": "id"}, "chunked": {"chunk_column": "id", "chunk_size": 100},
               "time_window": {"time_column": "created_at", "days_window": 7}, "cdc": {"cdc": {}}}
FILL = {"CONFIG": "cfg", "EXPORT": EXPORT, "RUN_ID": "canary-run", "SHELL": "bash", "PLAN_FILE": "plan", "ENV_VAR": "env",
        "SOURCE": "url", "PATH": "file", "OUTPUT": "out", "DIR": "dir"}
SA_KEY: list[str] = []
SHOW = False


def host_url(engine: str, user: str, port: int, password: str | None = None, query: str = "", raw: bool = False) -> str:
    """A source URL for `engine` on loopback, with `password` already spelled for its place."""
    auth = user + (f":{password}" if password is not None else "")
    db = DB.get(engine, "rivet")
    if engine == "mongo":
        query += ("&" if "?" in query else "?") + "serverSelectionTimeoutMS=300&connectTimeoutMS=300"
    return f"{SCHEME[engine]}://{auth}@127.0.0.1:{port}/{db}{query}"


def source_scene(engine: str, spelling: str, c: Canary, port: int, root: Path, env: dict[str, str]) -> dict:
    """The `source:` block (and env/files) that puts `c` into one credential spelling for `engine`."""
    user = USER.get(engine, "rivet")
    db = DB.get(engine, "rivet")
    typ = {"type": engine}
    if spelling == "userinfo":
        return {**typ, "url": host_url(engine, user, port, c.quoted)}
    if spelling == "userinfo-raw":
        return {**typ, "url": host_url(engine, user, port, c.plain)}
    if spelling in QUERY_KEYS:
        return {**typ, "url": host_url(engine, user, port, None, f"?{QUERY_KEYS[spelling]}={c.quoted}")}
    if spelling == "query-pct-value":
        return {**typ, "url": host_url(engine, user, port, None, f"?password={c.pct_all}")}
    if spelling == "fields":
        return {**typ, "host": "127.0.0.1", "port": port, "user": user, "password": c.plain, "database": db}
    if spelling == "password-env":
        env["CANARY_SRC_PW"] = c.plain
        return {**typ, "host": "127.0.0.1", "port": port, "user": user, "password_env": "CANARY_SRC_PW", "database": db}
    if spelling == "keyword":
        kw = (f"sqlserver://127.0.0.1:{port};databaseName={db};user={user};password={c.plain};encrypt=false"
              if engine == "mssql" else f"host=127.0.0.1 port={port} user={user} password={c.plain} dbname={db}")
        return {**typ, "url": kw}
    if spelling == "url-env":
        env["CANARY_SRC_URL"] = host_url(engine, user, port, c.quoted)
        return {**typ, "url_env": "CANARY_SRC_URL"}
    if spelling == "url-file":
        f = root / "src.url"
        f.write_text(host_url(engine, user, port, c.quoted) + "\n")
        return {**typ, "url_file": "src.url"}
    if spelling == "ambient-env":
        env.update({k: c.plain for k in AMBIENT_ECOSYSTEM})
        return {**typ, "url": host_url(engine, user, port)}
    raise ValueError(f"no source recipe for spelling {spelling}")


def state_url(spelling: str, c: Canary, port: int, user: str = "rivet", db: str = "rivet_state") -> str:
    """The `RIVET_STATE_URL` that puts `c` into one spelling."""
    base = f"postgresql://{user}@127.0.0.1:{port}/{db}"
    if spelling == "userinfo":
        return f"postgresql://{user}:{c.quoted}@127.0.0.1:{port}/{db}"
    if spelling == "query-pct-value":
        return f"{base}?password={c.pct_all}"
    if spelling == "query-sslpassword":
        return f"{base}?sslpassword={c.quoted}"
    if spelling == "query-password-tls":
        return f"{base}?sslmode=require&password={c.quoted}"
    return f"{base}?{QUERY_KEYS[spelling]}={c.quoted}"


def service_account_key(tag: str, token_uri: str, email: str = "canary") -> tuple[str, Canary] | None:
    """A service-account JSON whose private key body is the secret under test, or None when no RSA key can be made."""
    if not SA_KEY:
        r = subprocess.run(["openssl", "genrsa", "2048"], capture_output=True, text=True)
        if r.returncode != 0 or "BEGIN" not in r.stdout:
            return None
        SA_KEY.append(r.stdout)
    pem = SA_KEY[0]
    body = [ln for ln in pem.splitlines() if ln and not ln.startswith("-----")]
    line = body[2]
    key = Canary(line, line[:16], line[-16:])
    doc = {"type": "service_account", "project_id": "canary-project", "private_key_id": "0123456789abcdef",
           "private_key": pem, "client_email": f"{email}@canary-project.iam.gserviceaccount.com",
           "client_id": "1", "token_uri": token_uri}
    return json.dumps(doc), key


AMBIENT_SLOTS = ("dest", "bigquery", "gcp-secret", "snowflake")


def decoy_file(path: Path, module: str, tag: str, token_uri: str, forbidden: dict[str, Canary]) -> None:
    """Write one ambient credential file whose every secret is forbidden: a service-account key, an AWS ini, or a token."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if module == "google":
        email = token_canary(tag + "|email")
        made = service_account_key(tag, token_uri, email.plain.lower())
        path.write_text(made[0] if made else "{}")
        forbidden[f"ambient {path.name} identity"] = Canary(email.plain.lower(), email.head.lower(), email.tail.lower())
        return
    a, s = token_canary(tag + "|ak"), token_canary(tag + "|sk")
    if module == "aws":
        head = "profile decoy" if path.name == "config" else "decoy"
        body = f"aws_access_key_id = {a.plain}\naws_secret_access_key = {s.plain}\n"
        with path.open("a") as fh:
            fh.write(f"[default]\n{body}[{head}]\n{body}")
        forbidden.update({f"ambient {path.name} access key": a, f"ambient {path.name} secret key": s})
        return
    path.write_text(a.plain + "\n")
    forbidden[f"ambient {path.name}"] = a


def ambient_decoys(tag: str, root: Path, env: dict[str, str], stubs: dict[str, Stub], forbidden: dict[str, Canary]) -> None:
    """Plant every ambient identity the signer modules read, each a forbidden canary that must never stand in for the configured credential."""
    served = token_canary(tag + "|ambient-token")
    forbidden["ambient served token"] = served
    stubs["ambient"] = Stub("http", body=json.dumps({"access_token": served.plain, "token_type": "Bearer", "expires_in": 3600}).encode())
    base = f"http://127.0.0.1:{stubs['ambient'].port}"
    for module in SIGNER_PREFIX:
        names, files, _ = ambient_identity(module)
        for rel in files:
            decoy_file(root / rel, module, f"{tag}|{rel}", base + "/token", forbidden)
        for n, kind in sorted(names.items()):
            if n in env:
                continue
            match kind:
                case "secret":
                    c = token_canary(f"{tag}|{n}")
                    env[n] = c.plain
                    forbidden[f"ambient {n}"] = c
                case "redirect":
                    env[n] = base
                case "profile":
                    env[n] = "decoy"
                case "file":
                    same = next((root / rel for rel in files if Path(rel).name in n.lower()), None)
                    target = same or root / f"ambient-{n.lower()}"
                    if not target.exists():
                        decoy_file(target, module, f"{tag}|{n}", base + "/token", forbidden)
                    env[n] = str(target)
                case "selector":
                    pass
                case other:
                    raise KeyError(f"decoy kind {other} of {n} has no recipe")


def seed_state(binary: Path, root: Path, env: dict[str, str]) -> None:
    """Give the scene a SQLite state holding one cursor row, so `state copy` has a state to copy."""
    seed = {"source": {"type": "postgres", "url": "postgresql://seed@127.0.0.1:1/seed"},
            "exports": [{"name": EXPORT, "query": "SELECT 1", "format": "parquet", "destination": {"type": "local", "path": "out"}}]}
    seedp = root / "seed.yaml"
    seedp.write_text(json.dumps(seed))
    env = {k: v for k, v in env.items() if k != "RIVET_STATE_URL"}
    subprocess.run([str(binary), "state", "show", "--config", str(seedp)], cwd=root, env=env, capture_output=True, timeout=120,
                   stdin=subprocess.DEVNULL)
    with contextlib.closing(sqlite3.connect(root / ".rivet_state.db")) as db, db:
        db.execute("INSERT OR REPLACE INTO export_state (export_name, last_cursor_value, last_run_at) VALUES (?, '1', '2026-01-01T00:00:00Z')", (EXPORT,))


def build_scene(spec: Spec, root: Path, variant: tuple[str, dict[str, str], tuple[str, ...]], binary: Path,
                target: dict | None = None) -> Scene:
    """Write the config, files and environment of one run of `spec`; `target` points the source at a live server."""
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(root), "RIVET_LOCK_WAIT": "0",
           "RUST_BACKTRACE": "1", "XDG_CONFIG_HOME": str(root), "TMPDIR": str(root)}
    env.update(variant[1])
    stubs: dict[str, Stub] = {}
    roles: dict[str, Canary] = {}
    forbidden: dict[str, Canary] = {}
    tag = f"{spec.scenario}|{spec.engine}|{spec.path}|{spec.mode}"
    src_kind = "pg" if spec.path == "wrong-password" and spec.engine == "postgres" else "rst"
    stubs["source"] = Stub(src_kind)
    target = target if spec.path in ("wrong-password", "live-state", "live-source") else None
    src_target = target if spec.path == "wrong-password" else None
    src_port = src_target["port"] if src_target else stubs["source"].port
    if src_target:
        stubs["source"].close()
        del stubs["source"]
    secret = canary(tag)
    cfg: dict = {"exports": [], "source": {}}
    dest: dict = {"type": "local", "path": "out"}
    argv_extra: list[str] = []
    pw_for_source = secret if spec.slot in ("source", "ambient") else canary(tag + "|source")
    cfg["source"] = source_scene(spec.engine, spec.spelling if spec.slot in ("source", "ambient") else "userinfo", pw_for_source,
                                 src_port, root, env)
    roles["source"] = pw_for_source
    if spec.path == "live-source":
        assert target is not None
        del roles["source"]
        cfg["source"] = {"type": "postgres", "url": f"postgresql://rivet:rivet@127.0.0.1:{target['port']}/rivet"}
    if spec.slot == "gcp-secret":
        token = token_canary(tag + "token")
        body = json.dumps({"access_token": token.plain, "token_type": "Bearer", "expires_in": 3600}).encode() if spec.path == "fetch-token" else b""
        stubs["token"] = Stub("http", body=body)
        made = service_account_key(tag, f"http://127.0.0.1:{stubs['token'].port}/token")
        if made:
            (root / "sa.json").write_text(made[0])
            env["GOOGLE_APPLICATION_CREDENTIALS"] = str(root / "sa.json")
            roles["gcp key"] = made[1]
        if spec.path == "fetch-token":
            roles["gcp access token"] = token
        name = "projects/canary-project/secrets/db-credential"
        if spec.spelling == "password_secret":
            cfg["source"] = {"type": "postgres", "host": "127.0.0.1", "port": 5432, "user": "rivet", "database": "rivet", "password_secret": name}
        elif spec.spelling == "url_secret":
            cfg["source"] = {"type": "postgres", "url_secret": name}
        else:
            env["CANARY_SECRET_NAME"] = name
            cfg["source"] = {"type": "postgres", "url_secret": "${CANARY_SECRET_NAME}"}
    if spec.slot == "state":
        if spec.path == "live-state":
            assert target is not None
            secret = target["secret"]
            stubs["state"] = Stub("fwd", target["port"])
            env["RIVET_STATE_URL"] = state_url(spec.spelling, secret, stubs["state"].port, target["user"], target["dbs"][spec.spelling])
        else:
            stubs["state"] = Stub("rst")
            env["RIVET_STATE_URL"] = state_url(spec.spelling, secret, stubs["state"].port)
        roles["state"] = secret
        if spec.command.name == "state copy":
            argv_extra += ["--to", env.pop("RIVET_STATE_URL")]
    if spec.slot == "load":
        stubs["load"] = Stub("http")
        base = f"http://127.0.0.1:{stubs['load'].port}"
        env["CANARY_CH_PW"] = canary("baseline").plain
        load = {"target": "clickhouse", "database": "canary", "user": "rivet", "url": base, "password_env": "CANARY_CH_PW"}
        spelling, _, shape = spec.spelling.partition("@")
        if spelling == "url-userinfo":
            load["url"] = f"http://rivet:{secret.quoted}@127.0.0.1:{stubs['load'].port}"
        elif spelling == "url-query-password":
            load["url"] = f"{base}/?password={secret.quoted}"
        else:
            env["CANARY_CH_PW"] = secret.plain
        if shape:
            stubs["remote"] = Stub("http", host=lan_ip() or "127.0.0.1")
            load["url"] = endpoint_url(shape, stubs["remote"].port, stubs["load"].port)
        cfg["load"] = load
        roles["load"] = secret
    if spec.slot == "bigquery":
        token = token_canary(tag + "bqtoken")
        redirect = spec.spelling.startswith("redirect-")
        body = json.dumps({"access_token": token.plain, "token_type": "Bearer", "expires_in": 3600}).encode() if redirect else b""
        stubs["load"] = Stub("http", body=body)
        made = service_account_key(tag, f"http://127.0.0.1:{stubs['load'].port}/token")
        if made:
            key_file = "bq.json" if spec.spelling.startswith("destination-") else "sa.json"
            (root / key_file).write_text(made[0])
            if key_file == "sa.json":
                env["GOOGLE_APPLICATION_CREDENTIALS"] = str(root / key_file)
            roles["bigquery key"] = made[1]
        if spec.spelling.startswith("destination-"):
            stubs["dest"] = Stub("http")
            dest = {"type": "gcs", "bucket": "canary", "prefix": "p", "endpoint": f"http://127.0.0.1:{stubs['dest'].port}",
                    spec.spelling.removeprefix("destination-"): "bq.json"}
        if redirect:
            stubs["redirect"] = Stub("http")
            env[spec.spelling.removeprefix("redirect-")] = f"http://127.0.0.1:{stubs['redirect'].port}"
            roles["gcp access token"] = token
        cfg["load"] = {"target": "bigquery", "project": "canary-project", "dataset": "canary"}
    if spec.slot == "snowflake":
        key = token_canary(tag)
        (root / "sf.pem").write_text(f"-----BEGIN PRIVATE KEY-----\n{key.plain}\n-----END PRIVATE KEY-----\n")
        cfg["load"] = {"target": "snowflake", "account": "canary-account", "user": "rivet", "database": "canary",
                       "schema": "public", "warehouse": "wh", "private_key_path": "sf.pem"}
        roles["snowflake key"] = key
    if spec.slot == "dest":
        dest = dest_scene(spec.spelling, tag, root, env, stubs, roles)
    if spec.slot == "notify":
        stubs["notify"] = Stub("http")
        hook = token_canary(tag)
        url = f"http://127.0.0.1:{stubs['notify'].port}/services/{hook.plain}"
        if spec.spelling == "webhook-url-env":
            env["CANARY_HOOK"] = url
            cfg["notifications"] = {"slack": {"webhook_url_env": "CANARY_HOOK", "on": ["failure"]}}
        else:
            cfg["notifications"] = {"slack": {"webhook_url": url, "on": ["failure"]}}
        roles["webhook"] = hook
    if spec.slot in AMBIENT_SLOTS:
        ambient_decoys(tag, root, env, stubs, forbidden)
    exp = {"name": EXPORT, "mode": spec.mode, "format": "parquet", "destination": dest, **MODE_FIELDS[spec.mode]}
    if spec.engine == "mongo" or spec.mode == "cdc":
        exp["table"] = "canary_orders" if spec.engine == "mongo" else "public.canary_orders"
    else:
        exp["query"] = "SELECT 1 AS id" + (" FROM dual" if spec.engine == "oracle" else "")
    cfg["exports"].append(exp)
    cfgp = root / "rivet.yaml"
    cfgp.write_text(json.dumps(cfg, indent=1))
    argv = list(spec.command.path)
    cmd = spec.command
    for group in cmd.required:
        arg = pick(group, spec)
        if arg.flag:
            argv.append(arg.flag)
        if arg.value:
            argv.append(fill(arg, spec, cfg, env, root))
    if "--config" in cmd.options and not any(a.flag == "--config" for g in cmd.required for a in g):
        argv += ["--config", str(cfgp)]
    for flag in cmd.writers:
        argv += [flag, str(root / "artifacts" / flag.strip("-"))]
    (root / "artifacts").mkdir(exist_ok=True)
    if "--format" in cmd.options:
        argv += ["--format", "json"]
    argv += variant[2] if all(v in cmd.options for v in variant[2]) else ()
    if spec.command.name == "state copy":
        seed_state(binary, root, env)
    return Scene(root, argv + argv_extra, env, roles, stubs, forbidden=forbidden)


DEST_CREDENTIAL_FIELD = re.compile(r"_env$|_file$|_profile$|^allow_anonymous$")
PATH_SHAPES = ("file", "dir", "empty", "bad-json")


ENDPOINT_FIELD = re.compile(r"^(url|endpoint)$")
ENDPOINT_SHAPES = ("remote-http", "backslash-at")
LAN: list[str | None] = []


def lan_ip() -> str | None:
    """This host's non-loopback address a remote-host stub binds to, None when the host has no route off loopback."""
    if not LAN:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            ip = s.getsockname()[0] if s.connect_ex(("192.0.2.1", 9)) == 0 else "127.0.0.1"
        LAN.append(None if ip.startswith("127.") else ip)
    return LAN[0]


def endpoint_url(shape: str, remote: int, local: int) -> str:
    """An endpoint in one spelling: a plain remote `http://` host, or one a hand parser reads as loopback and a WHATWG parser dials remote."""
    match shape:
        case "remote-http":
            return f"http://{lan_ip()}:{remote}"
        case "backslash-at":
            return f"http://{lan_ip()}:{remote}\\@127.0.0.1:{local}"
    raise KeyError(f"endpoint shape `{shape}` has no recipe")


def endpoint_spellings(dest_recipes: dict[str, list[str]]) -> tuple[list[tuple[str, str]], list[str]]:
    """Every (slot, spelling) that puts a URL-typed field of the load or destination schema on a non-loopback host."""
    out: list[tuple[str, str]] = []
    guards: list[str] = []
    for defn in ("LoadSection", "DestinationConfig"):
        for f in (f for f in schema_fields(defn) if ENDPOINT_FIELD.match(f)):
            match (defn, f):
                case ("LoadSection", "url"):
                    bases = [("load", "password-env")]
                case ("DestinationConfig", "endpoint"):
                    bases = [("dest", sps[0]) for sps in dest_recipes.values()]
                case other:
                    guards.append(f"URL field `{other[0]}.{other[1]}` has no endpoint recipe")
                    continue
            out += [(slot, f"{b}@{shape}") for slot, b in bases for shape in ENDPOINT_SHAPES]
    return out, guards


def schema_fields(defn: str) -> list[str]:
    """The property names of one `$defs` entry of the config schema, in schema order."""
    return list(json.loads((REPO / "schemas" / "rivet.schema.json").read_text())["$defs"][defn]["properties"])


def credential_field_dests(field: str) -> tuple[str, ...]:
    """The destination types one credential field of `DestinationConfig` configures."""
    match field:
        case "access_key_env" | "secret_key_env" | "session_token_env" | "aws_profile":
            return ("s3",)
        case "account_key_env" | "sas_token_env":
            return ("azure",)
        case "credentials_file":
            return ("gcs",)
        case "allow_anonymous":
            return ("s3", "azure", "gcs")
    raise KeyError(f"destination credential field `{field}` has no recipe")


def dest_spellings(dests: list[str]) -> tuple[dict[str, list[str]], list[str]]:
    """Every credential spelling per destination type: each credential field alone (a path field in every shape) and every pair of them."""
    per: dict[str, list[str]] = {d: [] for d in dests}
    guards: list[str] = []
    for f in (f for f in schema_fields("DestinationConfig") if DEST_CREDENTIAL_FIELD.search(f)):
        try:
            for d in credential_field_dests(f):
                per.setdefault(d, []).append(f)
        except KeyError as e:
            guards.append(str(e).strip("'\""))
    out: dict[str, list[str]] = {}
    for d, fields in per.items():
        singles = [f"{d}:{f}:{sh}" for f in fields for sh in (PATH_SHAPES if f.endswith("_file") else ("",))]
        out[d] = [sp.rstrip(":") for sp in singles] + [f"{d}:{a}+{b}" for a, b in itertools.combinations(fields, 2)]
    return {d: sps for d, sps in out.items() if sps}, guards


def dest_field(field: str, shape: str, tag: str, root: Path, base: str, env: dict[str, str], roles: dict[str, Canary]) -> dict:
    """The destination keys, environment and files one credential field takes in one shape."""
    match field:
        case "access_key_env" | "session_token_env":
            ak, sk = token_canary(tag + "ak"), token_canary(tag + "sk")
            env.update({"CANARY_AK": ak.plain, "CANARY_SK": sk.plain})
            roles.update({"s3 access key": ak, "s3 secret key": sk})
            keys = {"access_key_env": "CANARY_AK", "secret_key_env": "CANARY_SK"}
            if field == "session_token_env":
                st = token_canary(tag + "st")
                env["CANARY_ST"] = st.plain
                roles["s3 session token"] = st
                keys["session_token_env"] = "CANARY_ST"
            return keys
        case "secret_key_env":
            sk = token_canary(tag + "sk")
            env["CANARY_SK"] = sk.plain
            roles["s3 secret key"] = sk
            return {"secret_key_env": "CANARY_SK"}
        case "aws_profile":
            ak, sk = token_canary(tag + "pak"), token_canary(tag + "psk")
            (root / ".aws").mkdir(exist_ok=True)
            with (root / ".aws" / "credentials").open("a") as fh:
                fh.write(f"[canary]\naws_access_key_id = {ak.plain}\naws_secret_access_key = {sk.plain}\n")
            roles.update({"s3 access key": ak, "s3 secret key": sk})
            return {"aws_profile": "canary"}
        case "account_key_env":
            key = token_canary(tag + "key")
            env["CANARY_AZ_KEY"] = key.plain
            roles["azure account key"] = key
            return {"account_key_env": "CANARY_AZ_KEY"}
        case "sas_token_env":
            sas = token_canary(tag + "sas")
            env["CANARY_AZ_SAS"] = sas.plain
            roles["azure sas token"] = sas
            return {"sas_token_env": "CANARY_AZ_SAS"}
        case "credentials_file":
            made = service_account_key(tag, f"{base}/token")
            p = root / "gcs.json"
            match shape:
                case "file":
                    p.write_text(made[0] if made else "{}")
                case "dir":
                    p.mkdir()
                case "empty":
                    p.write_text("")
                case "bad-json":
                    p.write_text(made[0][: len(made[0]) // 2] if made else "{")
            if made and shape in ("file", "bad-json"):
                roles["gcs key"] = made[1]
            return {"credentials_file": "gcs.json"}
        case "allow_anonymous":
            return {"allow_anonymous": True}
    raise KeyError(f"destination credential field `{field}` has no recipe")


def dest_scene(spelling: str, tag: str, root: Path, env: dict[str, str], stubs: dict[str, Stub],
               roles: dict[str, Canary]) -> dict:
    """The `destination:` block that carries one credential spelling; beside `allow_anonymous` no credential may reach any request."""
    stubs["dest"] = Stub("http")
    base = f"http://127.0.0.1:{stubs['dest'].port}"
    spelling, _, endpoint_shape = spelling.partition("@")
    dest_type, rest = spelling.split(":", 1)
    field_part, _, shape = rest.partition(":")
    fields = field_part.split("+")
    dest: dict = {"type": dest_type, "bucket": "canary", "prefix": "p", "endpoint": base}
    dest.update({"s3": {"region": "us-east-1"}, "azure": {"account_name": "canaryacct"}}.get(dest_type, {}))
    own: dict[str, Canary] = {}
    for f in fields:
        dest.update(dest_field(f, shape or "file", tag, root, base, env, own))
    roles.update({f"anonymous {k}": v for k, v in own.items()} if "allow_anonymous" in fields else own)
    if endpoint_shape:
        stubs["remote"] = Stub("http", host=lan_ip() or "127.0.0.1")
        dest["endpoint"] = endpoint_url(endpoint_shape, stubs["remote"].port, stubs["dest"].port)
    return dest


def pick(group: tuple[Arg, ...], spec: Spec) -> Arg:
    """The alternative of a required group this cell exercises: the source spelling decides among `--source*` flags."""
    by_flag = {a.flag: a for a in group}
    if "--source" in by_flag:
        return by_flag["--source-env" if spec.spelling == "url-env" else "--source-file" if spec.spelling == "url-file" else "--source"]
    return group[0]


def fill(arg: Arg, spec: Spec, cfg: dict, env: dict[str, str], root: Path) -> str:
    """The value a required argument takes in this cell."""
    v = arg.value
    if v == "CONFIG":
        return str(root / "rivet.yaml")
    if v == "SOURCE":
        return cfg["source"].get("url") or host_url(spec.engine, USER.get(spec.engine, "rivet"), 1)
    if v == "ENV_VAR":
        return "CANARY_SRC_URL"
    if v == "PATH" and arg.flag == "--source-file":
        return str(root / "src.url")
    if v == "PLAN_FILE":
        p = root / "plan.json"
        p.write_text("{}")
        return str(p)
    if v in FILL:
        return FILL[v]
    raise KeyError(f"no fill for required argument {v} of `rivet {spec.command.name}`")


@dataclasses.dataclass
class Outcome:
    """What one cell's runs produced."""

    spec: Spec
    hits: list[Hit]
    reached: bool
    wire_ok: bool | None
    rcs: list[int]
    refused: bool
    timed_out: bool
    panic: bool
    errors: list[str]


AUTH_FAILED = re.compile(r"authentication failed|28P01|password authentication", re.I)
AUTH_SHAPED = re.compile(r"authentic|password|login|denied|unauthori|credential|scram", re.I)
CODE = re.compile(r"RIVET_[A-Z0-9_]+")
ERROR_LINE = re.compile(r"^Error:|\bERROR\b|\"error\"", re.M)


def _snapshot(root: Path) -> dict[Path, str]:
    """The content hash of every file under `root`."""
    return {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob("*") if p.is_file()}


def _db_dump(path: Path) -> bytes:
    """Every table row of a SQLite file as text, read from a copy so the run's own file stays untouched."""
    with tempfile.TemporaryDirectory(prefix="ss-dump-") as tmp:
        copy = Path(tmp) / path.name
        copy.write_bytes(path.read_bytes())
        try:
            with contextlib.closing(sqlite3.connect(copy)) as db:
                return "\n".join(db.iterdump()).encode()
        except sqlite3.DatabaseError:
            return b""


def run_variant(binary: Path, spec: Spec, variant: tuple[str, dict[str, str], tuple[str, ...]], target: dict | None,
                timeout: int) -> tuple[Scene, dict[str, bytes], int, bool, bool]:
    """Run `spec` once in a fresh directory; the scene, its sinks, the exit code, the timeout flag and the stub reach."""
    root = Path(tempfile.mkdtemp(prefix="ss-"))
    scene = build_scene(spec, root, variant, binary, target)
    before = _snapshot(root)
    timed_out = False
    p = subprocess.Popen([str(binary), *scene.argv], cwd=root, env=scene.env, stdin=subprocess.DEVNULL,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        out, err = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        p.terminate()
        try:
            out, err = p.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
            out, err = p.communicate()
    sinks = {f"{variant[0]}/stdout": out, f"{variant[0]}/stderr": err}
    after = _snapshot(root)
    for path, sig in after.items():
        if before.get(path) != sig:
            sinks[f"{variant[0]}/file:{path.relative_to(root)}"] = path.read_bytes()
            if path.suffix in (".db", ".sqlite", ".sqlite3"):
                sinks[f"{variant[0]}/state-dump:{path.relative_to(root)}"] = _db_dump(path)
    for name, stub in scene.stubs.items():
        if stub.wire:
            sinks[f"{variant[0]}/request:{name}"] = bytes(stub.wire)
    return scene, sinks, p.returncode if p.returncode is not None else -1, timed_out, False


def run_cell(binary: Path, spec: Spec, target: dict | None = None, timeout: int = TIMEOUT_S) -> Outcome:
    """Run every variant of `spec` and collect leaks, reach and shape of the result."""
    hits: list[Hit] = []
    rcs: list[int] = []
    reached = False
    wire_ok: bool | None = None
    coded = timed_out = panic = False
    errors: list[str] = []
    for variant in CONNECT_VARIANTS:
        scene = None
        try:
            scene, sinks, rc, to, _ = run_variant(binary, spec, variant, target, timeout)
        except Exception as e:  # noqa: BLE001
            errors.append(f"{variant[0]}: {type(e).__name__}: {e}")
            continue
        finally:
            if scene is not None:
                for s in scene.stubs.values():
                    s.close()
        rcs.append(rc)
        timed_out |= to
        text = b"\n".join(sinks.values()).decode("utf-8", "replace")
        if SHOW:
            print(f"--- {spec.scenario} {spec.engine} {spec.path} {spec.mode} [{variant[0]}] rc={rc} reach="
                  f"{ {k: v.count for k, v in scene.stubs.items()} }\n{text[:900]}", flush=True)
        coded |= bool(CODE.search(text) or ERROR_LINE.search(text))
        panic |= bool(PANIC.search(text))
        hits += leaks(scene.roles, sinks) + find_hits(scene.forbidden, sinks)
        slot_stub = scene.stubs.get({"source": "source", "ambient": "source", "state": "state", "load": "load",
                                     "bigquery": "load", "snowflake": "load", "dest": "dest", "notify": "notify",
                                     "gcp-secret": "token"}.get(spec.slot, "source"))
        if slot_stub is not None:
            reached |= slot_stub.count > 0 and not (slot_stub.kind == "fwd" and AUTH_FAILED.search(text))
            if slot_stub.kind == "pg" and slot_stub.password is not None and "source" in scene.roles:
                wire_ok = (wire_ok is not False) and slot_stub.password == scene.roles["source"].plain
        elif target:
            reached |= rc != 0 and bool(AUTH_SHAPED.search(text))
        shutil.rmtree(scene.root, ignore_errors=True)
    return Outcome(spec, hits, reached, wire_ok, rcs, coded, timed_out, panic, errors)


def cell_matrix(cmds: list[Cmd], engines: list[str], modes: list[str], dests: list[str], loads: list[str],
                live: set[str], live_state: bool = False, live_source: bool = False) -> tuple[list[Spec], list[str]]:
    """Every cell the product's own axes imply, and a guard line for each axis value or argument no recipe covers."""
    guards: list[str] = []
    guards += [f"source engine `{e}` has no URL scheme recipe" for e in engines if e not in SCHEME]
    guards += [f"export mode `{m}` has no config recipe" for m in modes if m not in MODE_FIELDS]
    dest_recipes, dest_guards = dest_spellings(dests)
    guards += dest_guards
    endpoint_cells, endpoint_guards = endpoint_spellings(dest_recipes)
    guards += endpoint_guards
    guards += [f"destination type `{d}` has no credential recipe" for d in dests if d not in dest_recipes and d not in NO_CREDENTIAL]
    guards += [f"load target `{t}` has no credential recipe" for t in loads if t not in LOAD_RECIPES]
    for d in dests:
        try:
            signer_module(d)
        except KeyError as e:
            guards.append(str(e).strip("'\""))
    bq_spellings = bigquery_spellings()
    for n in product_redirect_env():
        try:
            redirect_slot(n)
        except KeyError as e:
            guards.append(str(e).strip("'\""))
    known_values = set(FILL) | {"PLAN_FILE", "ENV_VAR"}
    for c in cmds:
        for g in c.required:
            guards += [f"`rivet {c.name}` requires `{a.value}` and the matrix cannot fill it" for a in g
                       if a.value and a.value not in known_values]
    engines = [e for e in engines if e in SCHEME]
    specs: list[Spec] = []
    for c in cmds:
        cfg = "--config" in c.options
        src_flag = any(a.flag == "--source" for g in c.required for a in g)
        if cfg:
            for e in engines:
                for sp in SRC_SPELLINGS:
                    specs.append(Spec(c, e, "source", sp))
                    if e == "postgres" or e in live:
                        specs.append(Spec(c, e, "source", sp, "wrong-password"))
                for m in modes:
                    if m in MODE_FIELDS and m != "full":
                        specs.append(Spec(c, e, "source", "userinfo", "dead-host", m))
            specs += [Spec(c, "postgres", "state", sp, "dead-state", m) for sp in STATE_SPELLINGS for m in modes if m in MODE_FIELDS]
            specs += [Spec(c, "postgres", "state", sp, "live-state") for sp in STATE_SPELLINGS if live_state]
            specs += [Spec(c, "postgres", "load", sp) for sp in LOAD_SPELLINGS if "clickhouse" in loads]
            specs += [Spec(c, "postgres", sl, sp) for sl, sp in endpoint_cells if lan_ip()]
            specs += [Spec(c, "postgres", "bigquery", sp) for sp in bq_spellings] if "bigquery" in loads else []
            specs += [Spec(c, "postgres", "snowflake", "private-key-file")] if "snowflake" in loads else []
            specs += [Spec(c, "postgres", "dest", sp) for d in dests for sp in dest_recipes.get(d, ())]
            specs += [Spec(c, "postgres", "notify", sp) for sp in ("webhook-url", "webhook-url-env")]
            specs += [Spec(c, "postgres", "gcp-secret", sp, pa) for sp in GCP_SECRET_SPELLINGS for pa in ("fetch-fail", "fetch-token")]
            if live_source:
                specs += [Spec(c, "postgres", "dest", dest_recipes[d][0], "live-source") for d in dests if d in dest_recipes]
                specs += [Spec(c, "postgres", "notify", sp, "live-source") for sp in ("webhook-url", "webhook-url-env")]
        elif src_flag:
            for e in engines:
                for sp in URL_SPELLINGS + ("url-env", "url-file"):
                    specs.append(Spec(c, e, "source", sp))
        else:
            specs.append(Spec(c, "postgres", "ambient", "ambient-env"))
    return specs, guards


GCP_SECRET_SPELLINGS = ("password_secret", "url_secret", "url_secret-env")
NO_CREDENTIAL = {"local", "stdout"}
LOAD_RECIPES = {"clickhouse", "bigquery", "snowflake"}


def base_key(s: Spec) -> tuple:
    """The cells that differ only by spelling share one reach baseline."""
    return (s.command.name, s.engine, s.slot, s.path, s.mode)


def grade(led: Ledger, outs: list[Outcome]) -> tuple[dict[str, int], set[int]]:
    """Record every outcome on the ledger; returns the counts by verdict and the known-leak entries that matched."""
    by_base: dict[tuple, bool] = {}
    for o in outs:
        s = o.spec
        if o.reached:
            by_base[base_key(s)] = True
    counts = {"PASS": 0, "FAIL": 0, "KNOWN": 0, "SKIP": 0, "N/A": 0}
    used_known: set[int] = set()
    na_seen: set[tuple] = set()
    for o in outs:
        s = o.spec
        label = f"{s.engine}/{s.command.name}/{s.slot}:{s.spelling}/{s.path}/{s.mode}"
        det = f"rc={o.rcs}"
        if o.errors:
            led.failed(s.engine, "-", s.scenario, s.store, f"{label}: cell could not run: {o.errors[0]}")
            counts["FAIL"] += 1
        elif o.hits:
            kinds = {h.sink.split("/", 1)[1].split(":")[0] for h in o.hits}
            cell = {"command": s.command.name, "engine": s.engine, "slot": s.slot, "spelling": s.spelling}
            known, in_date = property_no_secret_in_output_known.match(cell, kinds)
            first = o.hits[0]
            msg = (f"LEAK {label}: {first.role} [{first.form}] in {first.sink}: {first.line!r} "
                   f"({len(o.hits)} hit(s) across {sorted({h.sink for h in o.hits})})")
            if known and in_date:
                used_known.update(id(k) for k in known)
                led.add(s.engine, "-", s.scenario, s.store, Status.KNOWN, f"{msg} — {known[0].reason}")
                counts["KNOWN"] += 1
            else:
                led.failed(s.engine, "-", s.scenario, s.store, msg)
                counts["FAIL"] += 1
        elif o.reached:
            wire = {True: " wire-password==canary", False: " WIRE-PASSWORD-DIFFERS", None: ""}[o.wire_ok]
            led.add(s.engine, "-", s.scenario, s.store, Status.PASS, f"clean; slot reached;{wire} {det}")
            counts["PASS"] += 1
        elif not by_base.get(base_key(s)):
            counts["N/A"] += 1
            if base_key(s) in na_seen:
                continue
            na_seen.add(base_key(s))
            led.not_applicable(s.engine, "-", s.scenario, s.store,
                               f"`rivet {s.command.name}` never opened the {s.slot} connection in its baseline spelling "
                               f"(clean output graded, credential slot not exercised) {det}; one row per command, slot and path")
        elif o.refused and any(rc != 0 for rc in o.rcs):
            led.add(s.engine, "-", s.scenario, s.store, Status.PASS, f"clean; spelling refused before connecting {det}")
            counts["PASS"] += 1
        else:
            led.skipped(s.engine, "-", s.scenario, s.store,
                        f"{label}: NOT GRADED, the credential never reached its connection (no coded refusal either) {det}",
                        cause=SkipCause.ENV)
            counts["SKIP"] += 1
    return counts, used_known


def stale_known(used: set[int]) -> list[str]:
    """Known-leak entries no cell matched: they are fixed and must be removed."""
    return [k.reason for k in property_no_secret_in_output_known.KNOWN_LEAKS if id(k) not in used]


def stand_target(engine: str) -> dict | None:
    """The stand server of `engine` the live tier points the source at, or None when the stand has none."""
    try:
        from ..pytools import registry
    except ImportError:  # pragma: no cover
        import registry  # type: ignore
    name = {"postgres": "postgres", "mysql": "mysql", "mssql": "mssql", "mongo": "mongo"}.get(engine)
    if name is None:
        return None
    m = re.search(r":(\d+)(?:/|$|\?)", registry.source(name)["url"].split("@")[-1])
    port = int(m.group(1)) if m else None
    if port is None:
        return None
    try:
        socket.create_connection(("127.0.0.1", port), timeout=2).close()
    except OSError:
        return None
    return {"port": port}


def target_for(s: Spec, live: dict[str, dict]) -> dict | None:
    """The real server a cell points at: the stand state database, or a stand source for a wrong password."""
    if s.path == "live-state":
        return live.get("state")
    if s.path == "live-source":
        return live.get("postgres")
    if s.path == "wrong-password" and s.engine != "postgres":
        return live.get(s.engine)
    return None


def execute(binary: Path, specs: list[Spec], live: dict[str, dict], jobs: int) -> list[Outcome]:
    """Run every cell, `jobs` at a time; cells on the shared state databases run one at a time."""
    shared = [s for s in specs if s.path == "live-state"]
    rest = [s for s in specs if s.path != "live-state"]
    with cf.ThreadPoolExecutor(jobs) as ex:
        outs = [f.result() for f in [ex.submit(run_cell, binary, s, target_for(s, live)) for s in rest]]
    return outs + [run_cell(binary, s, target_for(s, live)) for s in shared]


@contextlib.contextmanager
def state_role(port: int):
    """A scratch login role whose password is a canary, with one database it owns per state spelling; dropped on exit."""
    try:
        from ..pytools import registry
    except ImportError:  # pragma: no cover
        import registry  # type: ignore
    container = registry.source("postgres")["container"]

    def sql(text: str) -> None:
        """Run one statement as the stand superuser inside the stand container."""
        subprocess.run(["docker", "exec", container, "psql", "-U", "rivet", "-d", "rivet", "-v", "ON_ERROR_STOP=1", "-q", "-c", text],
                       check=True, capture_output=True, timeout=60)

    role = "ss_" + secrets.token_hex(5)
    secret = canary(f"live-state|{role}")
    dbs = {sp: f"{role}_{i}" for i, sp in enumerate(STATE_SPELLINGS)}
    try:
        sql(f"CREATE ROLE {role} LOGIN PASSWORD '{secret.plain}'")
        for db in dbs.values():
            sql(f"CREATE DATABASE {db} OWNER {role}")
        yield {"port": port, "user": role, "dbs": dbs, "secret": secret}
    finally:
        for db in dbs.values():
            with contextlib.suppress(Exception):
                sql(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
        with contextlib.suppress(Exception):
            sql(f"DROP ROLE IF EXISTS {role}")


def verify_credential_never_reaches_a_sink(led: Ledger, binary: Path | None = None, jobs: int = 8, only: str = "", live_tier: bool = True) -> None:
    """No credential canary in any credential slot reaches any sink of any command, engine, mode or path."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard if hard != resource.RLIM_INFINITY else 8192, hard))
    binary = (binary or rivet_bin()).resolve()
    cmds = command_tree(binary)
    engines, modes = schema_enum("SourceType"), schema_enum("ExportMode")
    dests, loads = schema_enum("DestinationType"), schema_enum("LoadTargetKind")
    live = {e: t for e in SCHEME if live_tier and (t := stand_target(e)) is not None}
    state_cm: contextlib.AbstractContextManager = contextlib.nullcontext()
    state_why = "live tier off"
    if live_tier:
        state_why = "the stand postgres does not answer"
        if "postgres" in live:
            state_cm = state_role(live["postgres"]["port"])
    with state_cm as state_target_:
        _verify(led, binary, cmds, engines, modes, dests, loads, live, state_target_ if isinstance(state_target_, dict) else None,
                state_why, jobs, only, live_tier)


def _verify(led: Ledger, binary: Path, cmds: list[Cmd], engines: list[str], modes: list[str], dests: list[str], loads: list[str],
            live: dict[str, dict], state_target_: dict | None, state_why: str, jobs: int, only: str, live_tier: bool) -> None:
    """Build the matrix, run it and record the verdicts."""
    specs, guards = cell_matrix(cmds, engines, modes, dests, loads, set(live) - {"postgres"}, state_target_ is not None, "postgres" in live)
    if state_target_ is None:
        led.skipped("postgres", "-", "no_secret_in_output/state-success-live", "-",
                    f"state URL success path against a real postgres state database is NOT GRADED: {state_why}", cause=SkipCause.ENV)
    live = {**live, **({"state": state_target_} if state_target_ else {})}
    guards += [f"`rivet {n}` has no cell in the matrix" for n in sorted({c.name for c in cmds} - {s.command.name for s in specs})]
    if only:
        specs = [s for s in specs if only in s.scenario or only in s.engine or only in s.store]
    for g in guards:
        led.failed("-", "-", "no_secret_in_output/guard", "-", f"no_secret_in_output matrix has no recipe: {g}")
    for e in SCHEME:
        if e not in live:
            led.skipped(e, "-", "no_secret_in_output/wrong-password-live", "-",
                        f"wrong-password against a real {e} server is NOT GRADED: " + ("no stand answers for it" if live_tier else "live tier off"), cause=SkipCause.ENV)
    for scenario, why in (
            ("warehouse-load-success-path", "the warehouse load error path (clickhouse, BigQuery, Snowflake reached after staging) is NOT GRADED: `rivet load` refuses a local destination and no object-store emulator stages a bucket; the config-time refusals are graded"),
            ("gcp-secret-resolved", "the resolved Secret Manager value reaching a sink is NOT GRADED: the endpoint is a const https URL (src/config/gcp_secret.rs:14) with no mock seam and the stand holds no Google project; the fetch-fail and token paths are graded"),
            ("state-tls-success", "state over TLS success is NOT GRADED: the stand postgres runs ssl=off; the sslmode=require error path is graded"),
            ("oracle-cdc-events", "Oracle CDC with events is NOT GRADED: no Oracle stand answers")):
        led.skipped("-", "-", f"no_secret_in_output/{scenario}", "-", why, cause=SkipCause.ENV)
    if lan_ip() is None:
        led.skipped("-", "-", "no_secret_in_output/endpoint-remote", "-",
                    "a credential sent to a non-loopback `http://` endpoint is NOT GRADED: this host has no non-loopback address "
                    "for the remote stub to bind", cause=SkipCause.ENV)
    for module in SIGNER_PREFIX:
        names, _, urls = ambient_identity(module)
        if not names:
            led.skipped("-", "-", f"no_secret_in_output/ambient-identity/{module}", "-",
                        f"the reqsign {module} sources are not in the cargo registry: its ambient identities are not planted, NOT GRADED",
                        cause=SkipCause.ENV)
        for u in urls:
            led.skipped("-", "-", f"no_secret_in_output/ambient-metadata/{module}", "-",
                        f"a fall-back to the metadata server {u} is NOT GRADED: reqsign reads that fixed link-local URL with no "
                        f"environment override (GCE_METADATA_HOST is not read), and no stub can stand on it without root",
                        cause=SkipCause.ENV)
    led.skipped("-", "-", "no_secret_in_output/success-path", "-",
                "right-password success path of the source engines is NOT GRADED: no provisioned canary users on the stand", cause=SkipCause.ENV)
    outs = execute(binary, specs, live, jobs)
    counts, used = grade(led, outs)
    if not only:
        for reason in stale_known(used):
            led.failed("-", "-", "no_secret_in_output/known-leak", "-", f"known-leak entry matched no cell, it is fixed: remove it ({reason})")
    never = sorted({o.spec.command.name for o in outs if o.spec.slot == "source" and not any(
        p.reached for p in outs if p.spec.command.name == o.spec.command.name and p.spec.slot == "source")})
    for eng in engines:
        if eng in SCHEME and not any(o.reached for o in outs if o.spec.engine == eng and o.spec.slot == "source"):
            led.skipped(eng, "-", "no_secret_in_output/engine-reach", "-",
                        f"no command ever connected to the {eng} source stub: every {eng} cell is clean but ungraded", cause=SkipCause.ENV)
    reach = {sl: sorted({o.spec.command.name for o in outs if o.spec.slot == sl and o.reached}) for sl in sorted({o.spec.slot for o in outs})}
    for sl, reached_by in reach.items():
        if not reached_by:
            led.skipped("-", "-", f"no_secret_in_output/slot-reach/{sl}", "-",
                        f"no command ever connected to the {sl} credential slot: its cells are clean but ungraded", cause=SkipCause.ENV)
    led.ok(f"no_secret_in_output: slot reach (commands that connected): { {sl: len(c) for sl, c in reach.items()} }")
    led.ok(f"no_secret_in_output: {len(specs)} cells over {len(cmds)} commands, {len(engines)} engines, {len(modes)} modes: {counts}; "
           f"commands that never connect to a source: {never}")


def _self_test() -> None:
    """Prove the oracle catches a planted leak in every sink and form, and the stubs count and speak."""
    c = canary("self-test")
    assert find_hits({"r": c}, {"a": b"clean\nnothing here"}) == []
    for name, needle in forms(c).items():
        hits = find_hits({"r": c}, {"stdout": b"x\n" + b"pre " + needle + b" post\ny"})
        assert any(h.form == name for h in hits), f"form {name} not found"
    for k in range(3):
        blob = base64.b64encode(b"\0" * k + c.plain.encode())
        assert any(h.form.split(":")[1].startswith("b64+") for h in find_hits({"r": c}, {"s": blob})), f"b64 alignment {k}"
    assert find_hits({"r": c}, {"s": b"user:" + c.head.encode() + b"***" + c.tail.encode()})
    reqs = parse_usage("[OPTIONS] --config <CONFIG> <--source <SOURCE>|--source-env <ENV_VAR>> <SHELL>")
    assert [[a.flag or a.value for a in g] for g in reqs] == [["--source", "--source-env"], ["--config"], ["SHELL"]], reqs
    rst, http, pg = Stub("rst"), Stub("http"), Stub("pg")
    for st in (rst, http):
        socket.create_connection(("127.0.0.1", st.port), timeout=2).sendall(b"GET / HTTP/1.1\r\n\r\n")
    import time

    deadline = time.time() + 3
    while (rst.count < 1 or http.count < 1) and time.time() < deadline:
        time.sleep(0.05)
    assert rst.count == 1 and http.count == 1
    s = socket.create_connection(("127.0.0.1", pg.port), timeout=2)
    s.sendall((8).to_bytes(4, "big") + (80877103).to_bytes(4, "big"))
    assert s.recv(1) == b"N"
    startup = b"\x00\x03\x00\x00user\0rivet\0\0"
    s.sendall((len(startup) + 4).to_bytes(4, "big") + startup)
    assert s.recv(9)[:1] == b"R"
    pw = b"hunter2\0"
    s.sendall(b"p" + (len(pw) + 4).to_bytes(4, "big") + pw)
    assert s.recv(64)[:1] == b"E"
    assert pg.password == "hunter2", pg.password
    relay = Stub("fwd", http.port)
    r = socket.create_connection(("127.0.0.1", relay.port), timeout=2)
    r.sendall(b"GET / HTTP/1.1\r\n\r\n")
    assert r.recv(64).startswith(b"HTTP/1.1 500"), "the relay did not carry the upstream answer back"
    assert relay.count == 1
    for st in (rst, http, pg, relay):
        st.close()
    assert find_hits({"r": c}, {"s": json.dumps({"p": c.plain}).encode()}), "a JSON-escaped canary is not found"
    assert find_hits({"r": c}, {"s": json.dumps({"p": c.plain}, ensure_ascii=False).encode()}), "a raw-UTF-8 JSON canary is not found"
    with tempfile.TemporaryDirectory() as tmp:
        dbp = Path(tmp) / "s.db"
        with contextlib.closing(sqlite3.connect(dbp)) as db:
            db.execute("create table t(v text)")
            db.execute("insert into t values (?)", (c.plain,))
            db.commit()
        assert find_hits({"r": c}, {"d": _db_dump(dbp)}), "the state DB dump does not carry the stored value"
    other = canary("other")
    wire = {"plain/request:notify": c.plain.encode() + b" " + other.plain.encode()}
    got = leaks({"webhook": c, "source": other}, wire)
    assert got and {h.role for h in got} == {"source"}, "an own credential on its own endpoint is graded, or a foreign one is not"
    assert find_hits({"ambient AWS_ACCESS_KEY_ID": other}, {"plain/request:dest": b"Credential=" + other.plain.encode()}), \
        "a forbidden identity on the destination's own endpoint is not graded"
    names, files, _ = ambient_identity("aws")
    assert not signer_source("aws") or (names.get("AWS_ACCESS_KEY_ID") == "secret" and ".aws/credentials" in files), (names, files)
    assert "token" == OWN_STUB["gcp key"] and set(OWN_STUB.values()) <= {"source", "state", "load", "dest", "notify", "token"}
    print("no_secret_in_output self-test: ok")


def main() -> int:
    """Run the matrix against a binary and print every non-green cell."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--bin", type=Path, default=None)
    ap.add_argument("--jobs", type=int, default=8)
    ap.add_argument("--only", default="")
    ap.add_argument("--no-live", action="store_true")
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--list-commands", action="store_true")
    a = ap.parse_args()
    global SHOW
    SHOW = a.show
    if a.self_test:
        _self_test()
        return 0
    if a.list_commands:
        print("\n".join(c.name for c in command_tree((a.bin or rivet_bin()).resolve())))
        return 0
    led = Ledger()
    verify_credential_never_reaches_a_sink(led, a.bin, a.jobs, a.only, not a.no_live)
    verdict = {Status.PASS: "PASS", Status.FAIL: "RED", Status.KNOWN: "RED"}
    tally = {"PASS": 0, "RED": 0, "NOT_GRADED": 0}
    for c in led.cells:
        word = verdict.get(c.status, "NOT_GRADED")
        tally[word] += 1
        print(f"no_secret_in_output {c.engine}/{c.scenario}/{c.store} {word} {' '.join(c.detail.split())[:400]}")
    print(f"no_secret_in_output summary PASS={tally['PASS']} RED={tally['RED']} NOT_GRADED={tally['NOT_GRADED']} cells={len(led.cells)}")
    return 1 if led.red else 0


if __name__ == "__main__":
    sys.exit(main())
