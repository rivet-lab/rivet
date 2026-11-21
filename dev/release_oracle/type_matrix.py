"""The edge-value type matrix: seed chunks and units cut from the ledger, and the grade of an observed verdict against the ledger's."""

from __future__ import annotations

import functools
import json
import re
from dataclasses import dataclass
from decimal import Decimal

from . import property_no_silent_null as nsn
from . import type_edges as te

MAX_COLS = 16
TABLE = "pm_tm{k}"
PRESENT = "present"
UNEXPECTED_REFUSAL, REFUSAL_GONE, UNEXPECTED_FORM, UNGRADED_VALUE, UNGRADED_SEED = (
    "UNEXPECTED_REFUSAL", "REFUSAL_GONE", "UNEXPECTED_FORM", "UNGRADED_VALUE", "UNGRADED_SEED")
TARGET_ORDER = ("duck", "clickhouse", "bigquery", "snowflake")
LIT_SHOWN = 40
ONE_PER_TABLE = ("LONG", "LONG RAW")


@dataclass(frozen=True)
class Column:
    """One ledger row seeded as a column: its literals sit at ids 1..n, the all-NULL row after them."""

    name: str
    native: str
    delivery: str
    family: str
    whole: te.Verdict | None
    lits: tuple[str, ...]
    edges: tuple[te.Edge, ...]
    null: te.Edge

    def edge_at(self, i: int) -> te.Edge:
        """The edge seeded at id `i` (1-based)."""
        return self.edges[i - 1]


@dataclass(frozen=True)
class Chunk:
    """A seed table: the columns it holds and the id of its all-NULL row."""

    table: str
    cols: tuple[Column, ...]

    @property
    def null_id(self) -> int:
        """The id of the all-NULL row."""
        return max(len(c.lits) for c in self.cols) + 1

    def seed_cols(self) -> list[nsn.Col]:
        """The columns as `nsn.seed_sql` takes them."""
        return [nsn.Col(c.name, c.native, c.delivery, c.family, c.lits) for c in self.cols]


@dataclass(frozen=True)
class Unit:
    """Ids of one column read together: a group of values that deliver, or one value that some stage refuses."""

    chunk: Chunk
    col: Column
    ids: tuple[int, ...]
    solo: bool

    def label(self) -> str:
        """A short name for logs."""
        return f"{self.chunk.table}.{self.col.name}{'@' + str(self.ids[0]) if self.solo else ''}"


def columns(engine: str) -> list[Column]:
    """Every ledger row of the engine as a column; its index in the ledger names it, so a row added later renames nothing."""
    out = []
    for i, r in enumerate(nsn.ledger_rows(engine)):
        lits = te.lits_of(r)
        if not lits and not r.get("refuses"):
            continue
        by_lit = {e.lit: e for e in te.edges_of(r)}
        if engine == "mongo" and any(e.solo for e in by_lit.values()):
            raise ValueError(f"mongo row {r['native_type']!r}: an export reads the whole collection, so no edge can be refused alone")
        null = next((e for e in by_lit.values() if e.lit.strip().upper() == "NULL"), te.Edge("NULL"))
        d = r.get("delivery", "")
        out.append(Column(f"c{i:03d}", r["native_type"], d, nsn.family(r["native_type"], d, engine), te.row_verdict(r), tuple(lits),
                          tuple(by_lit.get(x, te.Edge(x)) for x in lits), null))
    return out


def chunks(engine: str) -> list[Chunk]:
    """The columns cut into seed tables of at most MAX_COLS columns (a Mongo collection holds one), in ledger order; Oracle holds one LONG or LONG RAW column a table."""
    out: list[list[Column]] = [[]]
    width = 1 if engine == "mongo" else MAX_COLS
    for c in columns(engine):
        if len(out[-1]) == width or (c.native in ONE_PER_TABLE and any(x.native in ONE_PER_TABLE for x in out[-1])):
            out.append([])
        out[-1].append(c)
    return [Chunk(TABLE.format(k=k), tuple(cs)) for k, cs in enumerate(out) if cs]


def units(chunk: Chunk) -> list[Unit]:
    """A column's group (every value no stage refuses, and its NULL row) and one solo per value some stage refuses; a refusing row is one unit."""
    out = []
    for c in chunk.cols:
        n = len(c.lits)
        if c.whole:
            out.append(Unit(chunk, c, tuple(range(1, n + 1)), True))
            continue
        solos = [i for i in range(1, n + 1) if c.edge_at(i).solo]
        group = tuple(i for i in range(1, n + 1) if i not in solos) + (chunk.null_id,)
        out.append(Unit(chunk, c, group, False))
        out += [Unit(chunk, c, (i,), True) for i in solos]
    return out


def expected(col: Column, i: int, chunk: Chunk, target: str) -> te.Verdict:
    """What the ledger says the cell at id `i` delivers at `target` (`duck` is the extract read)."""
    if col.whole:
        return col.whole
    e = col.null if i == chunk.null_id else col.edge_at(i)
    return e.x if target == "duck" else e.verdict(target)


def lit_at(col: Column, i: int, chunk: Chunk) -> str:
    """The source literal at id `i`."""
    return "NULL" if i == chunk.null_id else col.lits[i - 1]


def observe(engine: str, col: Column, lit: str, got, want: te.Verdict) -> te.Verdict:
    """The verdict a delivered value amounts to: exact when it equals the source literal, null when it is NULL, else its canonical form."""
    if engine == "mongo":
        return observe_mongo(col, lit, got, want)
    exp = nsn.parse_lit(engine, lit)
    if exp is None:
        return te.Verdict(te.EXACT) if got is None else te.Verdict(te.FORM, text=te.canon(got))
    if got is None:
        return te.Verdict(te.NULL)
    got = nsn.element_got(exp, got, col.native)
    if want.kind == te.FORM and te.canon(text_elements(got, col.native)) == want.text:
        return te.Verdict(te.FORM, text=want.text)
    v = nsn.compare(engine, lit, col.family, "none", got, False, col.native)
    if v == "OK" and isinstance(got, bool) and isinstance(exp, Decimal) and exp not in (0, 1):
        return te.Verdict(te.FORM, text=te.canon(got))
    if v == "OK":
        return te.Verdict(te.EXACT)
    if v == "PRESENCE":
        return te.Verdict(PRESENT, text=te.canon(got))
    return te.Verdict(te.FORM, text=te.canon(got))


def text_elements(got, native: str):
    """A delivered list of a text type with its bytes elements decoded (a reader that returns strings as bytes); any other value as it is."""
    base = native[:-2].upper() if native.endswith("[]") else ""
    if not isinstance(got, (list, tuple)) or base in ("", "UUID", "BYTEA"):
        return got
    return [x.decode("utf-8", "surrogateescape") if isinstance(x, bytes) else x for x in got]


def observe_server_text(exp: str | None, got) -> te.Verdict:
    """The verdict a delivered value amounts to against the source's own text of it (a reader that returns strings as bytes read as text): exact when equal, null when it is NULL, else its canonical form."""
    got = got.decode("utf-8", "surrogateescape") if isinstance(got, bytes) else got
    v = nsn.server_text_verdict(exp, got, False)
    return te.Verdict(te.EXACT) if v == "OK" else te.Verdict(te.NULL) if v == nsn.NULLED else te.Verdict(te.FORM, text=te.canon(got))


def observe_mongo(col: Column, lit: str, doc, want: te.Verdict) -> te.Verdict:
    """The verdict of one Mongo field of a delivered `document`: exact when type and value hold, null when it is gone, else its relaxed text."""
    exp = te.mongo_value(lit)
    got = None if doc is None else te.mongo_doc(doc).get(col.name)
    if te.same_bson(exp, got):
        return te.Verdict(te.EXACT)
    if got is None:
        return te.Verdict(te.NULL)
    return te.Verdict(te.FORM, text=te.mongo_text(got))


GRID_IDS = ("int", "long", "string", "objectId")


@functools.cache
def mongo_grid() -> tuple[list[dict], dict[int, dict[str, tuple[Column, str, te.Edge]]]]:
    """The override grid's collection: document k holds the k-th literal of every Mongo column (long strings left to the matrix), its `_id` the i-th literal of the i-th GRID_IDS row (never equal numerically); plus what each field must deliver."""
    cols = columns("mongo")
    items = {c.name: [(c, lit, e) for lit, e in zip(c.lits, c.edges) if "REPEAT(" not in lit] for c in cols}
    kinds = {c.native: [x for x in c.lits if x.strip().upper() != "NULL"] for c in cols}
    ids = [te.mongo_value(kinds[n][min(i, len(kinds[n]) - 1)]) for i, n in enumerate(GRID_IDS)]
    n = max(len(v) for v in items.values())
    docs, want = [], {}
    for k in range(1, n + 1):
        here = {name: v[k - 1] for name, v in items.items() if k <= len(v)}
        docs.append({"_id": ids[k - 1] if k <= len(ids) else 1000 + k, "n": k, "nul": None, **{name: te.mongo_value(x[1]) for name, x in here.items()}})
        want[k] = {name: x for name, x in here.items()}
    docs.append({"_id": 2000, "n": n + 1, "nul": None})
    return docs, want


def mongo_grid_verdict(doc, n: int) -> list[tuple[str, str]]:
    """(class, detail) for every field of grid document n that the delivered JSON dropped or changed against the ledger's verdict."""
    bad = []
    for name, (col, lit, edge) in mongo_grid()[1].get(n, {}).items():
        k = klass(edge.x, observe_mongo(col, lit, doc, edge.x))
        if k and not (k.startswith("DEFECT_") and not k.startswith("DEFECT_GONE_")):
            bad.append((nsn.NULLED if k == nsn.NULLED else nsn.WRONG, f"{col.native}[{lit[:40]}]: {k}"))
    return bad


def same(want: te.Verdict, got: te.Verdict) -> bool:
    """Whether two verdicts say the same thing, the defect name aside."""
    if want.kind != got.kind:
        return False
    if want.kind == te.FORM:
        return want.text == got.text
    if want.kind == te.REFUSE:
        return (want.code, want.stage) == (got.code, got.stage)
    return True


def klass(want: te.Verdict, got: te.Verdict) -> str:
    """The failure class of an observed verdict against the ledger's, empty when the cell holds."""
    ok = same(want, got)
    if want.defect:
        return f"DEFECT_{want.defect}" if ok else f"DEFECT_GONE_{want.defect}"
    if ok:
        return ""
    if got.kind == PRESENT:
        return UNGRADED_VALUE
    if got.kind == te.REFUSE:
        return UNEXPECTED_REFUSAL
    if want.kind == te.REFUSE:
        return REFUSAL_GONE
    if want.kind == te.EXACT:
        return nsn.NULLED if got.kind == te.NULL else nsn.WRONG
    return UNEXPECTED_FORM


def spell(v: te.Verdict) -> str:
    """A verdict as the ledger writes it; an unreadable one as `present:` and its value."""
    return f"present:{v.text}" if v.kind == PRESENT else v.spelled()


def cell_name(engine: str, col: Column, lit: str, target: str) -> str:
    """`engine/native[literal]->target`, the name a pair carries into the ledger line."""
    shown = lit if len(lit) <= LIT_SHOWN else lit[:LIT_SHOWN] + "~" + str(len(lit))
    return f"{engine}/{col.native}[{shown}]->{target}"


def verdict_json(v: te.Verdict) -> str:
    """The verdict as JSON for `--observe`."""
    return json.dumps({"kind": v.kind, "text": v.text, "code": v.code, "stage": v.stage})


def self_test() -> None:
    """Grade the grader: units split what refuses, and every failure class fires on its exact harm."""
    from .property_no_silent_null import NULLED, WRONG

    exact, null = te.Verdict(te.EXACT), te.Verdict(te.NULL)
    form = lambda t, d="": te.Verdict(te.FORM, text=t, defect=d)
    refuse = lambda c, s=te.EXTRACT, d="": te.Verdict(te.REFUSE, code=c, stage=s, defect=d)
    boolcol = Column("c000", "BOOLEAN", "Int8", "bool", None, ("5",), (te.Edge("5"),), te.Edge("NULL"))
    assert observe("mysql", boolcol, "5", True, exact) == form("true") and observe("mysql", boolcol, "1", True, exact) == exact, "a number delivered as a bool is a changed value"
    intcol = Column("c001", "INT", "Int64", "int", None, ("5",), (te.Edge("5"),), te.Edge("NULL"))
    assert observe("mysql", intcol, "5", b"5", form("5")) == form("5"), "a value delivered as bytes is read as its text before its form is compared"
    ivcol = Column("c004", "INTERVAL[]", "List<Utf8>", "other", None, ("'{\"1 day\"}'",), (te.Edge("'{\"1 day\"}'"),), te.Edge("NULL"))
    assert observe("postgres", ivcol, "'{\"1 day\"}'", [b"P1D"], form('["P1D"]')) == form('["P1D"]'), "a list of text delivered as bytes is read as its text before its form is compared"
    assert text_elements([b"\x00"], "BYTEA[]") == [b"\x00"] and text_elements([b"a"], "TEXT") == [b"a"], "a binary list is never decoded"
    assert klass(exact, exact) == "" and klass(null, null) == "" and klass(form("[]"), form("[]")) == ""
    assert klass(exact, null) == NULLED and klass(exact, form("1")) == WRONG
    assert klass(exact, refuse("RIVET_X")) == UNEXPECTED_REFUSAL and klass(refuse("RIVET_X"), exact) == REFUSAL_GONE
    assert klass(refuse("RIVET_X"), refuse("RIVET_Y")) == UNEXPECTED_REFUSAL, "another code is a different refusal"
    assert klass(refuse("RIVET_X"), refuse("RIVET_X", te.LOAD)) == UNEXPECTED_REFUSAL, "another stage is a different refusal"
    assert klass(form("a"), form("b")) == UNEXPECTED_FORM and klass(null, exact) == UNEXPECTED_FORM
    assert klass(exact, te.Verdict(PRESENT, text="x")) == UNGRADED_VALUE
    assert klass(form("true", "BOOL"), form("true")) == "DEFECT_BOOL" and klass(form("true", "BOOL"), exact) == "DEFECT_GONE_BOOL"
    assert klass(refuse("RIVET_X", te.EXTRACT, "NAMED"), refuse("RIVET_X")) == "DEFECT_NAMED"
    col = Column("c000", "INT", "Int32", "int", None, ("1", "2", "3"), (te.Edge("1"), te.Edge("2", refuse("RIVET_X")), te.Edge("3")), te.Edge("NULL"))
    ch = Chunk("pm_tm0", (col,))
    us = units(ch)
    assert [(u.ids, u.solo) for u in us] == [((1, 3, 4), False), ((2,), True)], us
    assert expected(col, 2, ch, "duck") == refuse("RIVET_X") and expected(col, 4, ch, "bigquery") == exact
    whole = Column("c001", "MONEY", "", "decimal", refuse("RIVET_TYPE_UNSAFE_MAPPING"), ("1",), (te.Edge("1"),), te.Edge("NULL"))
    assert [(u.ids, u.solo) for u in units(Chunk("t", (whole,)))] == [((1,), True)]
    assert observe("mysql", col, "1", 1, exact) == exact and observe("mysql", col, "1", None, exact) == null
    assert observe("mysql", col, "1", 2, exact).kind == te.FORM and observe("mysql", col, "NULL", [], exact) == form("[]")
    assert observe("mysql", col, "NULL", None, exact) == exact
    assert observe("mysql", col, "ARRAY[1]", [1], exact).kind == PRESENT
    assert cell_name("mysql", col, "x" * 50, "duck") == "mysql/INT[" + "x" * 40 + "~50]->duck"
    cols = columns("mysql")
    assert cols and all(c.lits for c in cols if not c.whole), "a ledger row seeds at least one value"
    assert len({c.name for c in cols}) == len(cols)
    assert all(len(c.cols) <= MAX_COLS for c in chunks("mysql"))
    print(f"type_matrix self-test ok: {sum(len(chunks(e)) for e in ('postgres', 'mysql', 'mssql', 'oracle'))} chunks")


if __name__ == "__main__":
    self_test()
