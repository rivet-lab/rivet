"""What each live test needs that a hosted runner does not hold, read from the Rust call graph under it.

A function that names `Need::<Variant>` (tests/common/mod.rs) carries that need; a test needs what any function it calls, through
tests/common and its own module, carries. The gate places a test in the devbox half when one of its needs is one the hosted runner lacks.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from .core import ROOT

NEED_ENUM = re.compile(r"pub enum Need \{(.*?)\n\}", re.S)
NEED_USE = re.compile(r"\bNeed::([A-Z][A-Za-z]*)\b")
VARIANT_USE = re.compile(r"\b([A-Z][A-Za-z0-9_]*)::([A-Z][A-Za-z0-9_]*)\b")
FN_HEAD = re.compile(r"\bfn\s+([a-z_][a-z0-9_]*)")
IMPL_HEAD = re.compile(r"\bimpl\b(?:\s*<[^{>]*>)?\s+(?:[^{;]*?\bfor\s+)?&?([A-Z][A-Za-z0-9_]*)[^{;]*\{")
CALL = re.compile(r"(?P<pre>\.|::)?\s*\b(?P<name>[a-z_][a-z0-9_]*)\s*(?:::\s*<[^>()]*>)?\s*\(")
USE_LINE = re.compile(r"^use (?:crate|super)::([a-z0-9_]+)::(?:\{([^}]*)\}|([a-z0-9_]+));", re.M)
KEYWORDS = frozenset({"if", "while", "for", "match", "fn", "return", "loop", "in", "else", "let", "mut", "move", "unsafe", "as", "ref", "where", "impl", "dyn", "pub", "crate", "super", "self"})


def blank(text: str) -> str:
    """`text` with comments and the insides of string and char literals turned to spaces, newlines and length kept."""
    out, i, n = [], 0, len(text)
    raw = re.compile(r'b?r(#*)"')
    char = re.compile(r"'(?:\\.[^']*?|[^\\'])'")
    while i < n:
        c = text[i]
        m = None
        if text.startswith("//", i):
            j = text.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i))
            i = j
        elif text.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            out.append(re.sub(r"[^\n]", " ", text[i:j]))
            i = j
        elif c in "rb" and (m := raw.match(text, i)) and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            end = text.find('"' + m.group(1), m.end())
            j = n if end < 0 else end + 1 + len(m.group(1))
            out.append(re.sub(r"[^\n]", " ", text[i:j]))
            i = j
        elif c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            out.append('"' + re.sub(r"[^\n]", " ", text[i + 1:j]) + '"')
            i = j + 1
        elif c == "'" and (m := char.match(text, i)):
            out.append(" " * (m.end() - i))
            i = m.end()
        else:
            out.append(c)
            i += 1
    return "".join(out)


def _match_brace(code: str, i: int) -> int:
    """The index just past the `}` that closes the `{` at `i`."""
    depth, j = 0, i
    while j < len(code):
        depth += (code[j] == "{") - (code[j] == "}")
        j += 1
        if not depth:
            break
    return j


@dataclass(frozen=True)
class Fn:
    """One function of the live suite: its file, the type whose `impl` holds it, whether it takes `self`, and its blanked body."""

    file: str
    name: str
    owner: str | None
    has_self: bool
    body: str


def functions(file: str, text: str) -> list[Fn]:
    """Every `fn` of `text` that has a body, with the `impl` type around it; a declaration without a body is skipped."""
    code = blank(text)
    impls = [(m.end() - 1, _match_brace(code, m.end() - 1), m.group(1)) for m in IMPL_HEAD.finditer(code)]
    out = []
    for m in FN_HEAD.finditer(code):
        i = m.end()
        while i < len(code) and code[i] not in "{;":
            i += 1
        if i >= len(code) or code[i] == ";":
            continue
        owner = None
        for s, e, ty in impls:
            if s < m.start() < e:
                owner = ty
        out.append(Fn(file, m.group(1), owner, bool(re.search(r"\bself\b", code[m.end():i])), code[i:_match_brace(code, i)]))
    return out


def need_variants(root: Path = ROOT) -> list[str]:
    """The `Need` variants tests/common/mod.rs declares."""
    m = NEED_ENUM.search(blank((root / "tests" / "common" / "mod.rs").read_text()))
    return re.findall(r"\b([A-Z][A-Za-z]*)\b", m.group(1)) if m else []


def arms(body: str) -> list[tuple[int, int, frozenset[tuple[str, str]]]]:
    """Every match arm of `body` as (start, end, the `Enum::Variant` pairs its pattern names); a pattern naming none is a wildcard."""
    out = []
    for m in re.finditer(r"=>", body):
        k = m.start()
        while k > 0 and body[k - 1] not in ",{};":
            k -= 1
        alts = frozenset(VARIANT_USE.findall(body[k:m.start()]))
        i = m.end()
        while i < len(body) and body[i].isspace():
            i += 1
        if i < len(body) and body[i] == "{":
            end = _match_brace(body, i)
        else:
            depth, end = 0, i
            while end < len(body):
                ch = body[end]
                if ch in "([{":
                    depth += 1
                elif ch in ")]}":
                    if depth == 0:
                        break
                    depth -= 1
                elif ch == "," and depth == 0:
                    break
                end += 1
        out.append((m.end(), end, alts))
    return out


class Graph:
    """The functions of tests/common and tests/live, their call edges and the needs each carries."""

    def __init__(self, sources: dict[str, str]):
        self.fns: list[Fn] = []
        for f, text in sources.items():
            self.fns += functions(f, text)
        self.by_name: dict[str, list[Fn]] = {}
        for fn in self.fns:
            self.by_name.setdefault(fn.name, []).append(fn)
        self.imports: dict[str, dict[str, str]] = {}
        for f, text in sources.items():
            self.imports[f] = {}
            for mod, group, single in USE_LINE.findall(blank(text)):
                for name in re.findall(r"[a-z0-9_]+", group or single):
                    if f"live/{mod}" in sources:
                        self.imports[f][name] = f"live/{mod}"
        self._edges: dict[Fn, list[tuple[Fn, tuple]]] = {}

    def resolve(self, caller: Fn, name: str, method: bool, qual: str | None) -> list[Fn]:
        """The functions a call to `name` can mean from `caller`: `Type::name` means that type's `impl`, `.name` a method, a bare name a free function; the caller's file shadows an import, an import shadows tests/common."""
        cands = self.by_name.get(name, [])
        if qual is not None:
            typed = [c for c in cands if c.owner == (caller.owner if qual == "Self" else qual)]
            if typed:
                return typed
        for pool in (
            [c for c in cands if c.file == caller.file],
            [c for c in cands if c.file == self.imports[caller.file].get(name)],
            [c for c in cands if c.file.startswith("common/")],
        ):
            hit = [c for c in pool if c.has_self == method]
            if hit:
                return hit
        return []

    def edges(self, fn: Fn) -> list[tuple[Fn, tuple]]:
        """(callee, guards) for every call in `fn`, a guard being the `Enum::Variant` pairs of an enclosing match arm."""
        if fn in self._edges:
            return self._edges[fn]
        spans = arms(fn.body)
        out = []
        for m in CALL.finditer(fn.body):
            name = m["name"]
            if name in KEYWORDS:
                continue
            qual = None
            if m["pre"] == "::":
                q = re.search(r"([A-Za-z0-9_]+)\s*$", fn.body[:m.start()])
                qual = q.group(1) if q and q.group(1)[:1].isupper() else None
            guards = tuple(a for s, e, a in spans if s <= m.start() < e and a)
            out += [(c, guards) for c in self.resolve(fn, name, m["pre"] == ".", qual) if c is not fn]
        self._edges[fn] = out
        return out

    def needs(self, root: Fn) -> frozenset[str]:
        """The needs `root` carries over the calls its own naming of enum variants leaves reachable.

        A match arm whose pattern names `Enum::Variant` is skipped when `root`, or a function of its own file that it calls, names some other variant of that enum and not this one.
        """
        local, stack = {root}, [root]
        while stack:
            for callee, _ in self.edges(stack.pop()):
                if callee.file == root.file and callee not in local:
                    local.add(callee)
                    stack.append(callee)
        mentioned = frozenset(p for f in local for p in VARIANT_USE.findall(f.body))
        enums = {e for e, _ in mentioned}
        got: set[str] = set()
        seen, stack = {root}, [root]
        while stack:
            fn = stack.pop()
            got |= set(NEED_USE.findall(fn.body))
            for callee, guards in self.edges(fn):
                if callee not in seen and all(any(e not in enums or (e, v) in mentioned for e, v in alts) for alts in guards):
                    seen.add(callee)
                    stack.append(callee)
        return frozenset(got)


def sources_of(root: Path) -> dict[str, str]:
    """The suite's sources keyed `common/<path under tests/common>` and `live/<module>`."""
    out = {f"common/{p.relative_to(root / 'tests' / 'common')}": p.read_text() for p in sorted((root / "tests" / "common").rglob("*.rs"))}
    out |= {f"live/{p.stem}": p.read_text() for p in sorted((root / "tests" / "live").glob("*.rs"))}
    return out


def test_needs(root: Path = ROOT, sources: dict[str, str] | None = None) -> dict[tuple[str, str], frozenset[str]]:
    """(module, fn) -> the needs of every free fn of tests/live, closed over the calls it makes; `sources` replaces the files read."""
    graph = Graph(sources if sources is not None else sources_of(root))
    result: dict[tuple[str, str], frozenset[str]] = {}
    for fn in graph.fns:
        if fn.file.startswith("live/") and fn.owner is None:
            key = (fn.file.removeprefix("live/"), fn.name)
            result[key] = result.get(key, frozenset()) | graph.needs(fn)
    return result


def self_test() -> None:
    """The scanner reads a need through helpers, `impl` paths, imports and match arms, and ignores comments, strings, shadowed names and arms the test does not take."""
    common = """
pub enum Need { Credential, MacOs }
pub fn skip_needs(_: Need, _why: &str) {}
pub struct Vol;
impl Vol {
    pub fn mounted() -> Option<Self> { skip_needs(Need::MacOs, "hdiutil"); None }
}
pub fn token() { skip_needs(Need::Credential, "gcloud"); }
pub fn of(kind: Kind) {
    match kind {
        Kind::Cloud => token(),
        Kind::Plain => {}
    }
}
pub fn quiet() { /* Need::Credential */ let _s = "Need::MacOs"; }
pub fn run(x: u8) { if x > 0 { run(x - 1) } }
"""
    live = """
use crate::live_b::helper;
#[test] fn direct() { Vol::mounted(); }
#[test] fn via_import() { helper(); }
#[test] fn via_own_helper() { own(); }
fn own() { token(); }
#[test] fn plain_kind() { of(Kind::Plain); }
#[test] fn cloud_kind() { of(Kind::Cloud); }
#[test] fn unnamed_kind() { of(pick()); }
#[test] fn mentions_both() { let _ = Kind::Plain; of(Kind::Cloud); }
#[test] fn comment_and_string_only() { quiet(); run(3); }
#[test] fn shadowed_method() { Thing.of(1); }
struct Thing;
impl Thing { fn of(&self, _n: u8) {} }
"""
    other = "pub fn helper() { token(); }\n"
    got = test_needs(sources={"common/mod.rs": common, "live/live_a": live, "live/live_b": other})
    want = {
        "direct": {"MacOs"},
        "via_import": {"Credential"},
        "via_own_helper": {"Credential"},
        "plain_kind": set(),
        "cloud_kind": {"Credential"},
        "unnamed_kind": {"Credential"},
        "mentions_both": {"Credential"},
        "comment_and_string_only": set(),
        "shadowed_method": set(),
    }
    for name, needs in want.items():
        assert got[("live_a", name)] == needs, f"{name}: the scanner reads {sorted(got[('live_a', name)])}, expected {sorted(needs)}"
    assert need_variants() and "Credential" in need_variants(), "the Need enum of tests/common/mod.rs is not read"
