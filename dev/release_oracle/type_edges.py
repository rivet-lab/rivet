"""The edge values of docs/type-capability-matrix.yaml: their verdict grammar and the columns a row seeds."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation

TARGETS = ("bigquery", "snowflake", "clickhouse")
EXTRACT, LOAD = "extract", "load"
EXACT, NULL, FORM, REFUSE = "exact", "null", "form", "refuse"
UNCODED = "UNCODED"
_VERDICT = re.compile(r"^(?:(?P<defect>[A-Z_]+)!)?(?:(?P<plain>exact|null)|form:(?P<form>.*)|refuse:(?P<code>[A-Z_]+)(?:@(?P<stage>extract|load))?)$", re.S)


@dataclass(frozen=True)
class Verdict:
    """What a cell delivers: the value as is, NULL by design, a canonical text, or a coded refusal; `defect` names a known defect."""

    kind: str
    text: str = ""
    code: str = ""
    stage: str = ""
    defect: str = ""

    def spelled(self) -> str:
        """The verdict as the ledger writes it."""
        body = {EXACT: "exact", NULL: "null", FORM: f"form:{self.text}", REFUSE: f"refuse:{self.code}@{self.stage}"}[self.kind]
        return f"{self.defect}!{body}" if self.defect else body


EXACT_V = Verdict(EXACT)


def parse_verdict(text: str, default_stage: str = EXTRACT) -> Verdict:
    """The verdict of a ledger string; a spelling the grammar does not know is an error, never exact."""
    m = _VERDICT.fullmatch(text.lstrip())
    if not m:
        raise ValueError(f"verdict {text!r} is not [NAME!]exact | null | form:TEXT | refuse:CODE[@extract|load]")
    defect = m["defect"] or ""
    if m["plain"]:
        return Verdict(m["plain"], defect=defect)
    if m["form"] is not None:
        return Verdict(FORM, text=m["form"], defect=defect)
    return Verdict(REFUSE, code=m["code"], stage=m["stage"] or default_stage, defect=defect)


@dataclass(frozen=True)
class Edge:
    """One SQL literal of a row and its verdict per stage or target."""

    lit: str
    x: Verdict = EXACT_V
    by_target: dict = field(default_factory=dict, hash=False, compare=False)
    cdc: Verdict | None = None

    def verdict(self, target: str) -> Verdict:
        """The verdict at `target`: its own key, else the extract verdict."""
        return self.by_target.get(target, self.x)

    def verdicts(self) -> list[Verdict]:
        """Every batch verdict the edge declares: the extract and every target; the CDC verdict is graded by its own matrix."""
        return [self.x, *self.by_target.values()]

    @property
    def solo(self) -> bool:
        """Whether some target or the extract refuses this value: it then needs a table of its own, or it refuses its neighbours."""
        return any(v.kind == REFUSE for v in self.verdicts())

    @property
    def defect(self) -> str:
        """The known-defect names any verdict of this edge carries."""
        return ",".join(sorted({v.defect for v in self.verdicts() if v.defect}))


def edge_of(item) -> Edge:
    """The edge a ledger `edges:` item spells: a literal string, or a mapping with `v:` and verdict keys."""
    if isinstance(item, str):
        return Edge(item)
    unknown = set(item) - {"v", "x", "cdc", *TARGETS}
    if unknown or not isinstance(item.get("v"), str):
        raise ValueError(f"edge {item!r}: keys are v, x, cdc and {TARGETS}")
    return Edge(item["v"], parse_verdict(item["x"], EXTRACT) if "x" in item else EXACT_V,
                {t: parse_verdict(item[t], LOAD) for t in TARGETS if t in item},
                parse_verdict(item["cdc"], EXTRACT) if "cdc" in item else None)


def edges_of(row: dict) -> list[Edge]:
    """The edges of a ledger row; the same literal twice is an error."""
    out = [edge_of(i) for i in row.get("edges") or []]
    lits = [e.lit for e in out]
    if len(set(lits)) != len(lits):
        raise ValueError(f"row {row.get('native_type')}: an edge is listed twice")
    return out


def row_cdc(row: dict) -> Verdict:
    """The CDC verdict of a delivering row: `exact` (as batch) or a coded refusal; a row without one, a known defect or a form is an error."""
    if "cdc" not in row:
        raise ValueError(f"row {row.get('native_type')}: no `cdc:` verdict")
    v = parse_verdict(row["cdc"], EXTRACT)
    if v.kind not in (EXACT, REFUSE) or v.defect:
        raise ValueError(f"row {row.get('native_type')}: `cdc: {row['cdc']}` is exact or refuse:CODE; a form belongs to an edge, a known defect to known_defect")
    return v


def cdc_of(row: dict, edge: Edge) -> Verdict:
    """What the change stream delivers for one edge: its own `cdc`, else the row's refusal, else what batch extracts."""
    if edge.cdc is not None:
        return edge.cdc
    whole = row_cdc(row)
    return whole if whole.kind == REFUSE else edge.x


def row_verdict(row: dict) -> Verdict | None:
    """The refusal a row declares for its whole column, or None when the column delivers."""
    return parse_verdict(row["refuses"], EXTRACT) if row.get("refuses") else None


def _split(s: str) -> list[str]:
    """`s` cut at its top-level `+` and `||`, outside quotes and parentheses."""
    out, depth, quote, cur, i = [], 0, False, "", 0
    while i < len(s):
        c = s[i]
        if c == "'":
            quote = not quote
        elif not quote and c == "(":
            depth += 1
        elif not quote and c == ")":
            depth -= 1
        if not quote and depth == 0 and (c == "+" or s.startswith("||", i)) and cur.strip():
            out.append(cur.strip())
            cur, i = "", i + (2 if c == "|" else 1)
            continue
        cur += c
        i += 1
    return [*out, cur.strip()]


def _args(s: str) -> list[str]:
    """The top-level comma-separated arguments of `s`."""
    out, depth, quote, cur = [], 0, False, ""
    for c in s:
        quote ^= c == "'"
        depth += (c == "(") - (c == ")") if not quote else 0
        if c == "," and not quote and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += c
    return [*out, cur.strip()]


_QUOTED = re.compile(r"N?'((?:[^']|'')*)'$", re.S)
_CALL = re.compile(r"([A-Za-z_.]+)\((.*)\)(?:::\w+)?$", re.S)
_UNISTR = re.compile(r"\\\+([0-9A-Fa-f]{6})|\\([0-9A-Fa-f]{4})")


def _term(t: str):
    """The str or bytes one term of a concatenation stands for; None when the harness cannot evaluate it."""
    if m := _QUOTED.match(t):
        return m.group(1).replace("''", "'")
    if re.fullmatch(r"0[xX][0-9A-Fa-f]*", t):
        return bytes.fromhex(t[2:])
    if not (m := _CALL.match(t)):
        return None
    fn, args = m.group(1).upper(), _args(m.group(2))
    num = lambda a: int(a, 16) if a.lower().startswith("0x") else int(a)
    try:
        if fn in ("CHAR", "CHR", "NCHAR"):
            return chr(num(args[0]))
        if fn == "UNISTR":
            return _UNISTR.sub(lambda e: chr(int(e.group(1) or e.group(2), 16)), _term(args[0]))
        if fn in ("REPEAT", "REPLICATE"):
            return _term(args[0]) * int(args[1])
        if fn == "RPAD":
            return (_term(args[0]) * int(args[1]))[:int(args[1])] if len(args) == 3 and args[0] == args[2] else None
        if fn == "UTL_RAW.COPIES":
            return _term(args[0]) * int(args[1])
        if fn in ("HEXTORAW", "UNHEX"):
            return bytes.fromhex(_term(args[0]))
        if fn == "CAST":
            return _cat(re.split(r"\s+AS\s+(?=[A-Za-z_]+(?:\([^()]*\))?$)", args[0], maxsplit=1, flags=re.I)[0])
        if fn in ("TO_BLOB", "TO_CLOB"):
            return _term(args[0])
        if fn in ("EMPTY_CLOB",):
            return ""
        if fn == "EMPTY_BLOB":
            return b""
    except (TypeError, ValueError, IndexError):
        return None
    return None


def _cat(s: str):
    """The str or bytes a concatenation of terms stands for; None when its terms differ in kind or one is unknown."""
    parts = [_term(t) for t in _split(s)]
    return None if None in parts or len({type(p) for p in parts}) != 1 else (b"" if isinstance(parts[0], bytes) else "").join(parts)


def reduce(engine: str, lit: str) -> str:
    """The plain SQL literal an expression literal (CHAR/CHR/NCHAR, UNISTR, `+` and `||`, REPEAT/REPLICATE/RPAD/COPIES, EMPTY_*) stands for; the literal itself when it is not one."""
    s = lit.strip()
    if _QUOTED.match(s) and "'" not in s[2 if s[:1] == "N" else 1:-1].replace("''", ""):
        return lit
    joined = _cat(s)
    if joined is None:
        return lit
    if isinstance(joined, bytes):
        return "0x" + joined.hex()
    text = joined.encode("utf-16-le", "surrogatepass").decode("utf-16-le", "surrogatepass")
    body = text.replace("\\", "\\\\").replace("'", "''") if engine == "mysql" else text.replace("'", "''")
    return "'" + body + "'"


_NUMERIC_CALL = re.compile(r"(?:TO_BINARY_FLOAT|TO_BINARY_DOUBLE|TO_NUMBER)\('([^']*)'\)|CAST\(\s*([+-]?[0-9.]+(?:[eE][+-]?\d+)?)\s+AS\s+(?:tiny|small|big)?int(?:eger)?\b[^)]*\)|CAST\(\s*([+-]?[0-9.]+(?:[eE][+-]?\d+)?)\s+AS\s+(?:decimal|numeric|money|smallmoney|float|real)\b(?:\([^)]*\))?\)|([+-]?[0-9.]+(?:[eE][+-]?\d+)?)[fFdD]", re.I)


def numeric_of(lit: str):
    """The plain number a typed-numeric literal (`1.5f`, `TO_BINARY_FLOAT('x')`, `CAST(n AS int|decimal|float...)`) spells, else None."""
    m = _NUMERIC_CALL.fullmatch(lit.strip())
    if not m:
        return None
    t = next(g for g in m.groups() if g is not None)
    try:
        return Decimal(t)
    except InvalidOperation:
        return None


_SPECIAL = re.compile(r"^(?:CAST\()?(?:TO_BINARY_(?:FLOAT|DOUBLE)\()?'?(BINARY_(?:FLOAT|DOUBLE)_(?:NAN|INFINITY)|[+-]?(?:NaN|nan|Infinity|inf)|-0(?:\.0)?(?![\d.]))'?\)?", re.I)


def special_float(lit: str):
    """NaN, an infinity or -0.0 when the literal spells one, else None."""
    m = _SPECIAL.match(lit.strip())
    if not m:
        return None
    t = m.group(1).lower().replace("binary_float_", "").replace("binary_double_", "").replace("infinity", "inf").lstrip("+")
    return {"nan": float("nan"), "inf": float("inf"), "-inf": float("-inf"), "-0": -0.0, "-0.0": -0.0}.get(t)


CANON_LIMIT = 120


def _plain(v):
    """A value nested in a delivered object as JSON keeps: a null stays null, a list and an object stay structured, the rest is its canonical text."""
    if v is None or isinstance(v, (bool, int, str)) or (isinstance(v, float) and v - v == 0):
        return v
    if isinstance(v, (list, tuple)):
        return [_plain(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _plain(x) for k, x in v.items()}
    return canon(v)


def canon(v) -> str:
    """A deterministic text of a delivered value: what a `form:` verdict spells, a long value as its length and digest."""
    if v is None:
        out = "NULL"
    elif isinstance(v, bool):
        out = "true" if v else "false"
    elif isinstance(v, float):
        out = "NaN" if v != v else "Infinity" if v == float("inf") else "-Infinity" if v == float("-inf") else repr(v)
    elif isinstance(v, Decimal):
        out = format(v, "f")
    elif isinstance(v, (bytes, bytearray, memoryview)):
        out = "0x" + bytes(v).hex()
    elif isinstance(v, datetime):
        out = v.isoformat(" ")
    elif isinstance(v, (date, time)):
        out = v.isoformat()
    elif isinstance(v, timedelta):
        out = str(v)
    elif isinstance(v, (list, tuple)):
        out = "[" + ",".join(json.dumps(x, ensure_ascii=False) if isinstance(x, str) else canon(x) for x in v) + "]"
    elif isinstance(v, str):
        out = v.replace("\\", "\\\\").encode("utf-8", "backslashreplace").decode()
    elif isinstance(v, dict):
        out = json.dumps({str(k): _plain(x) for k, x in v.items()}, sort_keys=True, ensure_ascii=False)
    else:
        out = str(v)
    if len(out) > CANON_LIMIT:
        return f"sha:{len(out)}:{hashlib.sha1(out.encode('utf-8', 'surrogatepass')).hexdigest()[:16]}"
    return out


def lits_of(row: dict) -> list[str]:
    """The literals a row seeds, in id order: its samples, then its edges, each once; NULL is the implicit last row."""
    return list(dict.fromkeys(x for x in [*row.get("sample", []), *(e.lit for e in edges_of(row))] if x.strip().upper() != "NULL"))


def _bson_options(relaxed: bool = False):
    """The extended-JSON options of every Mongo literal and delivered document: canonical (relaxed to spell a form), an out-of-range date kept as milliseconds."""
    from bson.datetime_ms import DatetimeConversion
    from bson.json_util import CANONICAL_JSON_OPTIONS, RELAXED_JSON_OPTIONS

    return (RELAXED_JSON_OPTIONS if relaxed else CANONICAL_JSON_OPTIONS).with_options(datetime_conversion=DatetimeConversion.DATETIME_MS)


def mongo_value(lit: str):
    """The BSON value of a Mongo edge literal: canonical extended JSON, `NULL`, or `REPEAT('x',N)` for a long string."""
    from bson import json_util

    if lit.strip().upper() == "NULL":
        return None
    r = reduce("postgres", lit)
    if r != lit and r.startswith("'") and r.endswith("'"):
        return r[1:-1].replace("''", "'")
    return json_util.loads(lit, json_options=_bson_options())


def mongo_doc(text) -> dict:
    """A delivered `document` (JSON text or an already parsed object) read as BSON."""
    from bson import json_util

    return json_util.loads(text if isinstance(text, (str, bytes)) else json.dumps(text), json_options=_bson_options())


def same_bson(a, b) -> bool:
    """Whether two BSON values have the same type and value: Int64 is not int, NaN equals NaN, -0.0 is not 0.0, a dict is ordered."""
    if type(a) is not type(b):
        return False
    if isinstance(a, float):
        return a != a and b != b or (a == b and str(a) == str(b))
    if isinstance(a, dict):
        return list(a) == list(b) and all(same_bson(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(same_bson(x, y) for x, y in zip(a, b))
    if hasattr(a, "to_decimal"):
        return str(a) == str(b)
    if hasattr(a, "pattern") and hasattr(a, "flags"):
        return (a.pattern, a.flags) == (b.pattern, b.flags)
    return a == b


def mongo_text(v) -> str:
    """The relaxed extended-JSON text of a BSON value: what a `form:` verdict spells for a document that lost a type tag."""
    from bson import json_util

    out = json_util.dumps(v, json_options=_bson_options(relaxed=True)) if not isinstance(v, str) else json.dumps(v, ensure_ascii=False)
    return out if len(out) <= CANON_LIMIT else f"sha:{len(out)}:{hashlib.sha1(out.encode('utf-8', 'surrogatepass')).hexdigest()[:16]}"


def self_test() -> None:
    """Grade the grammar: a verdict round-trips, an unknown spelling is an error, the reducer reads what the engines seed, canon never merges two values."""
    for text in ("exact", "null", "form:[]", "refuse:RIVET_X@load", "BOOL!form:true", "NAMED!refuse:RIVET_X@extract"):
        assert parse_verdict(text, EXTRACT).spelled() == text, text
    assert parse_verdict("form:<a/>\n").text == "<a/>\n", "a form text keeps its trailing newline"
    assert parse_verdict("refuse:RIVET_X", LOAD).stage == LOAD and parse_verdict("refuse:RIVET_X").stage == EXTRACT
    for bad in ("", "ok", "form", "refuse:", "refuse:x@y", "exact!", "Exact", "nul"):
        try:
            parse_verdict(bad)
        except ValueError:
            continue
        raise AssertionError(f"{bad!r} must not parse")
    e = edge_of({"v": "1", "x": "exact", "snowflake": "refuse:RIVET_X"})
    assert e.verdict("snowflake").kind == REFUSE and e.verdict("bigquery") == EXACT_V and e.solo and not edge_of("1").solo
    for bad_item in ({"v": "1", "oops": "exact"}, {"x": "exact"}):
        try:
            edge_of(bad_item)
        except ValueError:
            continue
        raise AssertionError(f"{bad_item!r} must not parse")
    try:
        edges_of({"native_type": "T", "edges": ["1", "1"]})
        raise AssertionError("a literal twice must not pass")
    except ValueError:
        pass
    assert reduce("mssql", "REPLICATE(CAST('x' AS varchar(max)),3)") == "'xxx'" and reduce("mssql", "NCHAR(0xD83D)+NCHAR(0xDE00)") == "'\U0001F600'"
    assert reduce("oracle", "UNISTR('\\D83D\\DE00')") == "'\U0001F600'" and reduce("oracle", "'a'||CHR(0)||'b'") == "'a\0b'"
    assert reduce("oracle", "RPAD('c',5,'c')") == "'ccccc'" and reduce("oracle", "RPAD('c',5,'d')") == "RPAD('c',5,'d')"
    assert reduce("oracle", "HEXTORAW('00FF')") == "0x00ff"
    assert reduce("oracle", "TO_BLOB(UTL_RAW.COPIES(HEXTORAW('AB'),3))") == "0xababab" and reduce("mysql", "'plain'") == "'plain'"
    assert reduce("mssql", "N'a'+0x01") == "N'a'+0x01", "terms of different kinds stay unreduced"
    assert numeric_of("1.5f") == Decimal("1.5") and numeric_of("TO_BINARY_FLOAT('2.5')") == Decimal("2.5") and numeric_of("CAST(7 AS int)") == Decimal(7)
    assert numeric_of("CAST(-1.25 AS decimal(5,2))") == Decimal("-1.25") and numeric_of("'x'") is None and numeric_of("1.5") is None
    nan, inf, ninf, nz = (special_float(x) for x in ("'NaN'", "'Infinity'", "TO_BINARY_FLOAT('-Inf')", "'-0'"))
    assert nan != nan and inf == float("inf") and ninf == float("-inf") and str(nz) == "-0.0" and special_float("1.5") is None and special_float("-0.5") is None and special_float("-0.10") is None
    assert special_float("BINARY_DOUBLE_NAN") != special_float("BINARY_DOUBLE_NAN") and special_float("BINARY_FLOAT_INFINITY") == float("inf")
    assert canon("a\ud800b") != canon("a\\ud800b") and canon(["a,b"]) != canon(["a", "b"]) and canon(["1"]) != canon([1])
    assert canon({"i": None}) != canon({"i": "NULL"}) and canon({"a": [1, {"b": None}]}) == '{"a": [1, {"b": null}]}'
    assert canon(b"\x00\xff") == "0x00ff" and canon(None) == "NULL" and canon(-0.0) == "-0.0" and canon(float("nan")) == "NaN" and canon(True) == "true"
    long_a, long_b = "x" * 500, "x" * 499 + "y"
    assert canon(long_a).startswith("sha:500:") and canon(long_a) != canon(long_b)
    assert edge_of({"v": "1", "cdc": "form:1.0"}).cdc == Verdict(FORM, text="1.0") and edge_of("1").cdc is None and not edge_of({"v": "1", "cdc": "refuse:RIVET_X"}).solo
    plain, shut = {"native_type": "T", "cdc": "exact"}, {"native_type": "T", "cdc": "refuse:RIVET_X"}
    assert row_cdc(plain) == EXACT_V and row_cdc(shut).code == "RIVET_X" and row_cdc(shut).stage == EXTRACT
    assert cdc_of(plain, edge_of("1")) == EXACT_V and cdc_of(plain, edge_of({"v": "1", "x": "form:a"})).kind == FORM
    assert cdc_of(shut, edge_of("1")).kind == REFUSE and cdc_of(shut, edge_of({"v": "1", "cdc": "null"})).kind == NULL
    for bad_row in ({"native_type": "T"}, {"native_type": "T", "cdc": "form:a"}, {"native_type": "T", "cdc": "D!exact"}, {"native_type": "T", "cdc": "null"}, {"native_type": "T", "cdc": "ok"}):
        try:
            row_cdc(bad_row)
        except ValueError:
            continue
        raise AssertionError(f"{bad_row!r} must not read as a CDC verdict")
    assert lits_of({"sample": ["1"], "edges": ["1", {"v": "2"}, "NULL"]}) == ["1", "2"]
    from bson import Int64

    assert mongo_value("NULL") is None and mongo_value('{"$numberLong":"2"}') == Int64(2) and type(mongo_value('{"$numberLong":"2"}')) is Int64
    assert mongo_value("REPEAT('ab',3)") == "ababab" and mongo_value('"a\'b"') == "a'b" and mongo_value('{"$numberDouble":"-0.0"}') == 0.0
    assert not same_bson(Int64(2), 2) and same_bson(float("nan"), float("nan")) and not same_bson(-0.0, 0.0) and same_bson([1, {"a": [2]}], [1, {"a": [2]}])
    assert not same_bson({"a": 1, "b": 2}, {"b": 2, "a": 1}) and not same_bson(1, True) and not same_bson(1, 1.0)
    assert not same_bson(mongo_value('{"$numberDecimal":"1.0"}'), mongo_value('{"$numberDecimal":"1.00"}'))
    assert mongo_text(Int64(2)) == "2" and mongo_text("a") == '"a"' and mongo_doc('{"c": {"$numberLong": "5"}}')["c"] == 5
    print("type_edges self-test ok")


if __name__ == "__main__":
    self_test()
