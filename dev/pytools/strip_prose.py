"""Strip prose comments from Rust sources and collapse doc blocks to one line."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

KEEP = re.compile(r"SAFETY|clippy::|rustfmt|mutants::|cfg_attr")
EXEMPT = {"tests/offline/rig_oracle_ratchet.rs", "tests/offline/sprawl_ratchets.rs"}


def _skip_string(src: str, i: int) -> int:
    """Return the index just past the string or char literal opening at i."""
    n = len(src)
    if src[i] == "r" or src.startswith("br", i):
        j = i + (2 if src[i] == "b" else 1)
        hashes = 0
        while j < n and src[j] == "#":
            hashes += 1
            j += 1
        if j < n and src[j] == '"':
            end = src.find('"' + "#" * hashes, j + 1)
            return n if end < 0 else end + 1 + hashes
        return i + 1
    if src[i] == "'":
        m = re.match(r"'(?:\\.[^']*|[^\\'])'", src[i:])
        return i + m.end() if m else i + 1
    j = i + (2 if src[i] == "b" else 1)
    while j < n:
        if src[j] == "\\":
            j += 2
        elif src[j] == '"':
            return j + 1
        else:
            j += 1
    return n


def _scan(src: str):
    """Yield (start, end, kind) of every comment; kind is line, doc, or block."""
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if src.startswith("//", i):
            end = src.find("\n", i)
            end = n if end < 0 else end
            kind = "doc" if src.startswith(("///", "//!"), i) and not src.startswith("////", i) else "line"
            yield i, end, kind
            i = end
        elif src.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if src.startswith("/*", j):
                    depth += 1
                    j += 2
                elif src.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            yield i, j, "block"
            i = j
        elif c == '"' or (c in "rb" and re.match(r'b?r#*"|b"', src[i : i + 8]) and not _ident_before(src, i)):
            i = _skip_string(src, i)
        elif c == "'":
            i = _skip_string(src, i)
        else:
            i += 1


def _ident_before(src: str, i: int) -> bool:
    return i > 0 and (src[i - 1].isalnum() or src[i - 1] == "_")


def _first_sentence(lines: list[str]) -> str:
    """Return the first sentence of a doc block as one line."""
    paras: list[str] = []
    for ln in lines:
        t = ln.strip()
        if not t:
            if paras:
                break
            continue
        paras.append(t)
    text = " ".join(paras)
    m = re.search(r"[.!?](\s|$)", text)
    sent = text[: m.start() + 1] if m else text
    if len(sent) > 110:
        cut = re.search(r"( — |; |: |, but | because | so that )", sent[40:])
        if cut:
            sent = sent[: 40 + cut.start()].rstrip(",;:") + "."
    return sent


def strip(src: str) -> str:
    """Return src without prose comments; doc blocks become one line."""
    out: list[str] = []
    pos = 0
    items = list(_scan(src))
    k = 0
    while k < len(items):
        s, e, kind = items[k]
        text = src[s:e]
        line_start = src.rfind("\n", 0, s) + 1
        indent = src[line_start:s]
        whole = indent.strip() == ""
        if kind == "line" and KEEP.search(text):
            k += 1
            continue
        if kind == "doc":
            j = k
            block = [text]
            while (
                j + 1 < len(items)
                and items[j + 1][2] == "doc"
                and src[items[j][1] : items[j + 1][0]].strip() == ""
                and src.startswith(text[:3], items[j + 1][0])
            ):
                j += 1
                block.append(src[items[j][0] : items[j][1]])
                e = items[j][1]
            body = [b[3:] for b in block]
            if any(b.strip().startswith("```") for b in body) or not whole:
                k = j + 1
                continue
            first = _first_sentence(body)
            repl = f"{text[:3]} {first}" if first else None
            out.append(src[pos:line_start])
            if repl and len(block) == 1 and body[0].strip() == first:
                out.append(indent + text.rstrip())
            elif repl:
                out.append(indent + repl)
            pos = e
            if not repl:
                pos = e + 1 if src[e : e + 1] == "\n" else e
            k = j + 1
            continue
        if whole:
            out.append(src[pos:line_start])
            pos = e + 1 if src[e : e + 1] == "\n" else e
        else:
            out.append(src[pos:s].rstrip(" \t"))
            pos = e
        k += 1
    out.append(src[pos:])
    return "".join(out)


def _git(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)


def recut_seeds(root: Path) -> int:
    """Re-cut every seed patch that no longer applies to the cleaned working tree."""
    stripped = set(_git(["diff", "--name-only", "--", "src"], root).stdout.split())
    patches = sorted((root / "dev/seeded").glob("*/*.patch"))
    stale = [p for p in patches if _git(["apply", "--check", str(p)], root).returncode]
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp) / "base"
        _git(["worktree", "add", "-q", "--detach", str(base), "HEAD"], root)
        try:
            for p in stale:
                files = re.findall(r"^\+\+\+ b/(.+)$", p.read_text(), re.M)
                if _git(["apply", str(p)], base).returncode:
                    print(f"cannot re-cut {p}", file=sys.stderr)
                    continue
                old, new = Path(tmp) / "old", Path(tmp) / "new"
                for d in (old, new):
                    shutil.rmtree(d, ignore_errors=True)
                for f in files:
                    for d in (old, new):
                        (d / f).parent.mkdir(parents=True, exist_ok=True)
                    (old / f).write_text((root / f).read_text())
                    body = (base / f).read_text()
                    (new / f).write_text(strip(body) if f in stripped else body)
                    if f in stripped:
                        subprocess.run(["rustfmt", "--edition", "2024", str(new / f)], capture_output=True)
                diff = _git(["diff", "--no-index", "--no-prefix", "old", "new"], Path(tmp)).stdout
                diff = re.sub(r"^(diff --git |--- |\+\+\+ )(old|new)/", lambda m: m.group(1) + ("a/" if m.group(2) == "old" else "b/"), diff, flags=re.M)
                diff = re.sub(r"^diff --git a/(\S+) new/(\S+)", r"diff --git a/\1 b/\2", diff, flags=re.M)
                _git(["checkout", "--", "."], base)
                if diff:
                    p.write_text(diff)
        finally:
            _git(["worktree", "remove", "--force", str(base)], root)
    return len(stale)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="*")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--recut-seeds", action="store_true", help="re-cut seed patches the strip made stale")
    a = ap.parse_args()
    changed = 0
    done: set[str] = set()
    for p in a.paths:
        if p in EXEMPT:
            continue
        path = Path(p)
        src = path.read_text()
        new = strip(src)
        if new != src:
            changed += 1
            done.add(p)
            if a.check:
                print(p)
            else:
                path.write_text(new)
    print(f"changed: {changed}", file=sys.stderr)
    if a.recut_seeds and not a.check:
        print(f"seed patches re-cut: {recut_seeds(Path.cwd())}", file=sys.stderr)
    return 1 if a.check and changed else 0


if __name__ == "__main__":
    raise SystemExit(main())
