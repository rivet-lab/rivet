"""A recording reverse proxy for a cloud REST API and a loopback stub that replays what it recorded.

The recorder sits between rivet and a real endpoint, forwards every request with its own credentials and stores
the exchange with every credential masked first. The player answers rivet from a recording and keeps a request
log, so a cell grades what rivet asked a server and what it did with the answer without any cloud.
"""

from __future__ import annotations

import base64
import http.client
import json
import re
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

KEPT_REQUEST_HEADERS = ("content-type", "content-range", "x-goog-upload-protocol")
KEPT_RESPONSE_HEADERS = ("content-type", "etag", "x-goog-hash", "location", "x-guploader-uploadid")
HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "proxy-authorization", "proxy-connection", "host", "content-length"}
SECRET_SHAPES = [
    (re.compile(r"ya29\.[A-Za-z0-9_\-\.]{20,}"), "<MASKED:access-token>"),
    (re.compile(r"1//[A-Za-z0-9_\-]{30,}"), "<MASKED:refresh-token>"),
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S), "<MASKED:private-key>"),
    (re.compile(r"AIza[0-9A-Za-z_\-]{35}"), "<MASKED:api-key>"),
    (re.compile(r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}"), "<MASKED:jwt>"),
    (re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"), "<MASKED:email>"),
    (re.compile(r"(?i)(sig|signature|x-goog-signature|x-amz-signature)=[A-Za-z0-9%+/=_\-]{16,}"), r"\1=<MASKED:signature>"),
]
PLACEHOLDER = re.compile(r"\{\{([A-Z_]+)\}\}")


def mask(text: str, secrets: tuple[str, ...] = ()) -> str:
    """The text with every known secret value and every credential shape replaced, before anything is cut from it."""
    for s in sorted((s for s in secrets if s and len(s) >= 6), key=len, reverse=True):
        text = text.replace(s, "<MASKED:secret>")
    for shape, repl in SECRET_SHAPES:
        text = shape.sub(repl, text)
    return text


def leaks(text: str) -> list[str]:
    """The names of the credential shapes still readable in `text`."""
    return [repl for shape, repl in SECRET_SHAPES if repl.startswith("<MASKED:") and "email" not in repl and shape.search(text)]


@dataclass
class Exchange:
    """One request and the answer it got."""

    method: str
    path: str
    body: str
    status: int
    reply: str
    request_headers: dict = field(default_factory=dict)
    reply_headers: dict = field(default_factory=dict)
    binary: bool = False

    def to_json(self) -> dict:
        return self.__dict__.copy()


def _text(raw: bytes) -> tuple[str, bool]:
    """A body as text, or as base64 when it is not UTF-8."""
    try:
        return raw.decode(), False
    except UnicodeDecodeError:
        return base64.b64encode(raw).decode(), True


def read_body(handler: BaseHTTPRequestHandler) -> bytes:
    """The request body, whether it came with a length or chunked."""
    if handler.headers.get("Transfer-Encoding", "").lower() == "chunked":
        out = b""
        while True:
            size = int(handler.rfile.readline().strip().split(b";")[0] or b"0", 16)
            if size == 0:
                handler.rfile.readline()
                return out
            out += handler.rfile.read(size)
            handler.rfile.readline()
    return handler.rfile.read(int(handler.headers.get("Content-Length") or 0))


class Recorder:
    """A reverse proxy on a loopback port that forwards to `upstream` and records every exchange, masked."""

    def __init__(self, upstream: str, secrets: tuple[str, ...] = ()):
        self.upstream, self.secrets, self.exchanges = upstream, secrets, []
        self.lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                return

            def handle_any(self):
                body = read_body(self)
                headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP}
                conn = http.client.HTTPSConnection(outer.upstream, timeout=300)
                conn.request(self.command, self.path, body=body or None, headers=headers)
                resp = conn.getresponse()
                raw = resp.read()
                conn.close()
                req_text, req_bin = _text(body)
                rep_text, rep_bin = _text(raw)
                ex = Exchange(self.command, mask(self.path, outer.secrets), req_text if req_bin else mask(req_text, outer.secrets), resp.status,
                              rep_text if rep_bin else mask(rep_text, outer.secrets),
                              {k: mask(v, outer.secrets) for k, v in ((k.lower(), v) for k, v in self.headers.items()) if k in KEPT_REQUEST_HEADERS},
                              {k: mask(v, outer.secrets) for k, v in ((k.lower(), v) for k, v in resp.getheaders()) if k in KEPT_RESPONSE_HEADERS},
                              req_bin or rep_bin)
                with outer.lock:
                    outer.exchanges.append(ex)
                self.send_response(resp.status)
                for k, v in resp.getheaders():
                    if k.lower() not in HOP_BY_HOP:
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = handle_any

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()

    def mark(self) -> int:
        """The index the next exchange will get: the start of a phase."""
        with self.lock:
            return len(self.exchanges)

    def save(self, path: Path, meta: dict, start: int = 0, end: int | None = None) -> None:
        """Write the exchanges from `start` to `end`; a credential shape left in them refuses the write."""
        text = json.dumps({"meta": meta, "exchanges": [e.to_json() for e in self.exchanges[start:end]]}, indent=1, sort_keys=True)
        left = leaks(text)
        if left:
            raise RuntimeError(f"refusing to write {path}: credential shapes left in the recording: {left}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n")


def load(path: Path) -> dict:
    """A recording written by `Recorder.save`."""
    return json.loads(path.read_text())


def fill(text: str, values: dict[str, str]) -> str:
    """The text with each `{{NAME}}` placeholder replaced by this run's value; an unknown name stays and is reported by the caller."""
    return PLACEHOLDER.sub(lambda m: values.get(m.group(1), m.group(0)), text)


def unfilled(text: str) -> list[str]:
    """The placeholder names still open in `text`."""
    return sorted(set(PLACEHOLDER.findall(text)))


def normalize(text: str, values: dict[str, str]) -> str:
    """The text with this run's values turned into `{{NAME}}` placeholders, longest value first."""
    for name, v in sorted(values.items(), key=lambda kv: len(kv[1]), reverse=True):
        if v:
            text = text.replace(v, "{{" + name + "}}")
    return text


class Player:
    """A loopback stub that answers from a recording and logs every request; a request with no recorded answer is a 599."""

    def __init__(self, exchanges: list[dict], values: dict[str, str], key=None, objects: dict | None = None):
        self.values, self.log, self.lock = values, [], threading.Lock()
        self.by_key: dict[str, list[dict]] = {}
        self.key = key or self.default_key
        self.objects = objects if objects is not None else {}
        for e in exchanges:
            self.by_key.setdefault(self.key(e["method"], fill(e["path"], values)), []).append(e)
        self.served: dict[str, int] = {}
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                return

            def handle_any(self):
                body = read_body(self)
                status, reply, headers = outer.answer(self.command, self.path, body, dict(self.headers.items()))
                raw = reply if isinstance(reply, bytes) else reply.encode()
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(raw)

            do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = handle_any

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @staticmethod
    def default_key(method: str, path: str) -> str:
        """The method and the path without its query."""
        return f"{method} {path.split('?')[0]}"

    def answer(self, method: str, path: str, body: bytes, headers: dict) -> tuple[int, bytes | str, dict]:
        text, binary = _text(body)
        entry = {"method": method, "path": path, "body": text, "binary": binary, "authorization": next((v for k, v in headers.items() if k.lower() == "authorization"), ""), "headers": {k.lower(): v for k, v in headers.items()}}
        with self.lock:
            self.log.append(entry)
            key = self.key(method, path)
            hits = self.by_key.get(key)
            if not hits:
                entry["unrecorded"] = True
                return 599, json.dumps({"error": {"code": 599, "message": f"no recorded answer for {key}"}}), {"Content-Type": "application/json"}
            n = self.served.get(key, 0)
            self.served[key] = n + 1
            ex = hits[min(n, len(hits) - 1)]
        reply = fill(ex["reply"], self.values).replace("{{AUTHORIZATION}}", entry["authorization"])
        if ex.get("binary"):
            return ex["status"], base64.b64decode(ex["reply"]), ex["reply_headers"]
        return ex["status"], reply, ex["reply_headers"]

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def unrecorded(self) -> list[dict]:
        """The requests the recording had no answer for."""
        return [e for e in self.log if e.get("unrecorded")]
