"""No silent NULL: every non-NULL source value is delivered equal or refused with a code (owner rule 2026-11-08).

The grid is read from the product: engines from `SourceType`, modes from `ExportMode`, formats from
`FormatType`, override targets from `parse_type_str`, source columns from docs/type-capability-matrix.yaml.
Each cell seeds non-NULL edge values and a NULL row, runs `rivet check` and `rivet run`, and a second
reader (DuckDB) grades the delivered parts, never rivet's own counters.
"""

from __future__ import annotations

import concurrent.futures as cf
import hashlib
import json
import math
import os
import re
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from uuid import UUID
from zoneinfo import ZoneInfo
from decimal import Context, Decimal, InvalidOperation, localcontext
from pathlib import Path

import yaml

from . import cdc, core, engines
from . import type_edges as te
from .core import Ledger, SkipCause
from .sentinels import warned_counts

ROOT = Path(__file__).resolve().parents[2]
LEDGER = ROOT / "docs" / "type-capability-matrix.yaml"
SCENARIO = "no_silent_null"
TABLE = "pm_no_silent_null_t"
STAND = {
    "postgres": "postgresql://rivet:rivet@127.0.0.1:5432/rivet",
    "mysql": "mysql://rivet:rivet@127.0.0.1:3306/rivet",
    "mssql": "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet",
    "oracle": "oracle://rivet:rivet@127.0.0.1:1521/FREEPDB1",
    "mongo": "mongodb://127.0.0.1:27017/rivet",
}
TS_NATIVE = {"postgres": "TIMESTAMP", "mysql": "DATETIME", "mssql": "DATETIME2", "oracle": "TIMESTAMP"}
TS_LITERAL = {"oracle": "TIMESTAMP '2026-11-01 00:00:00'"}
RUN_TIMEOUT = 120
MODE_SHAPES = {"full": {}, "incremental": {"cursor_column": "id"}, "chunked": {"chunk_column": "id", "chunk_size": 6},
               "time_window": {"time_column": "nsn_ts", "days_window": 3650}}
WORKERS = 6
CODE = re.compile(r"\[(RIVET_[A-Z0-9_]+)\]")
NULLED, WRONG, SPLIT, UNCODED, IGNORED, EMPTY, CRASH, UNGRADED = (
    "SILENT_NULL", "WRONG_VALUE", "CHECK_RUN_SPLIT", "UNCODED_STOP", "OVERRIDE_IGNORED", "NO_OUTPUT", "CRASH", "UNGRADED_CONFIG")
TYPE_REFUSALS = ("RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED",)
PROBE_UNREACHED = re.compile(r"type report for '[^']*' failed: .*(?:refused|timed out|timeout|unreachable)", re.I)
STAND_REFUSALS = ("RIVET_SOURCE_CONNECT_FAILED", "RIVET_SOURCE_RUNTIME_UNAVAILABLE", "RIVET_SOURCE_STATEMENT_TIMEOUT")
STAND_TRIES = 5
RECHECKS = 2
LEASE_REFUSAL = "RIVET_STATE_RUN_IN_PROGRESS"
CELL_STATE = {"RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": ""}
DELIVERED, REFUSED, DECLARED = "delivered", "refused", "declared_lossy"
HIDDEN, SHAPE = "HIDDEN_COLUMN_DELIVERED", "LEG_SHAPE_DIFFERS"
BLIND = "INTEGRITY_BLIND"
COLUMNS, PARTS = "COLUMN_SET_DIFFERS", "PART_SET_DIFFERS"
FAIL_CLASSES = (NULLED, WRONG, SPLIT, UNCODED, IGNORED, EMPTY, CRASH, UNGRADED, HIDDEN, SHAPE, COLUMNS, PARTS, BLIND)
NOT_GRADED = "NOT_GRADED"


def _enum_variants(path: str, name: str) -> list[str]:
    """The variant names of `pub enum <name>` or `pub(crate) enum <name>` in a product source file, snake_cased."""
    text = (ROOT / path).read_text()
    m = re.search(r"pub(?:\(crate\))? enum %s \{(.*?)\n\}" % name, text, re.S)
    if not m:
        raise AssertionError(f"{path}: enum {name} not found: the grid cannot be read from the product")
    body = re.sub(r"///[^\n]*|//[^\n]*", "", m.group(1))
    out = [re.sub(r"(?<!^)(?=[A-Z])", "_", v).lower() for v in re.findall(r"^\s+([A-Z]\w*)\s*[,({]?", body, re.M)]
    if not out:
        raise AssertionError(f"{path}: enum {name} has no variants")
    return out


def override_targets() -> list[str]:
    """One spelling per type `parse_type_str` accepts (the first of each match arm), plus decimal precisions."""
    text = (ROOT / "src/types/override_type.rs").read_text()
    fn = text.split("pub fn parse_type_str", 1)[1].split("fn parse_decimal_params", 1)[0]
    arms = re.findall(r'^\s+((?:"[a-z0-9_ ]+"\s*\|?\s*)+)=>', fn, re.M)
    names = [re.findall(r'"([a-z0-9_ ]+)"', a)[0] for a in arms]
    if len(names) < 15 or "float4" not in names or "decimal(" not in fn:
        raise AssertionError(f"override_type.rs: parsed {names}: the override grid cannot be read from the product")
    return [*names, "decimal(9,2)", "decimal(18,4)", "decimal(38,10)", "decimal(60,10)", *negative_scale_targets(text)]


def negative_scale_targets(text: str) -> list[str]:
    """`decimal(p,-s)` targets, one per Arrow decimal width, when `parse_decimal_params` parses the scale as a signed integer."""
    m = re.search(r"let scale: (\w+) =", text.split("fn parse_decimal_params", 1)[1])
    if not m:
        raise AssertionError("override_type.rs: parse_decimal_params has no `let scale: <type>`: the scale axis cannot be read")
    return ["decimal(18,-2)", "decimal(60,-2)"] if m.group(1).startswith("i") else []


def rivet_types() -> list[str]:
    """The `RivetType` variants of the product; each needs a value family in RIVET_TYPE_FAMILY or the grid refuses to build."""
    found = _enum_variants("src/types/rivet_type.rs", "RivetType")
    if set(found) != set(RIVET_TYPE_FAMILY):
        raise AssertionError(f"RivetType variants {sorted(set(found) ^ set(RIVET_TYPE_FAMILY))} have no sample family: add them to RIVET_TYPE_FAMILY")
    return found


def axes() -> dict:
    """Engines, modes, formats and override targets, as the product declares them."""
    return {
        "engines": [e for e in _enum_variants("src/config/source.rs", "SourceType")],
        "modes": _enum_variants("src/config/export.rs", "ExportMode"),
        "formats": _enum_variants("src/config/format.rs", "FormatType"),
        "targets": ["none", *override_targets()],
        "rivet_types": rivet_types(),
    }


def ledger_rows(engine: str) -> list[dict]:
    """The native-type rows of one engine in the type capability ledger."""
    return yaml.safe_load(LEDGER.read_text())["engines"][engine]["rows"]


@dataclass(frozen=True)
class Col:
    name: str
    native: str
    delivery: str
    family: str
    lits: tuple[str, ...]
    edges: tuple = ()
    refuses: object = None


FLOAT4_NATIVE = {"mysql": r"FLOAT(\(\d+,\d+\))?( UNSIGNED)?", "mssql": "REAL", "postgres": "REAL|FLOAT4", "oracle": "BINARY_FLOAT"}
FLOAT8_NATIVE = {"mysql": r"DOUBLE(\(\d+,\d+\))?( UNSIGNED)?", "mssql": r"FLOAT(\(\d+\))?", "postgres": "DOUBLE PRECISION|FLOAT8", "oracle": "BINARY_DOUBLE"}


def family(native: str, delivery: str, engine: str = "") -> str:
    """The value family a source column belongs to: what its values can be compared as."""
    n = native.upper()
    if re.fullmatch(r"(TINY|SMALL|MEDIUM|BIG)?INT(EGER)?( UNSIGNED)?|YEAR|OID|NUMBER\(\d+\)", n):
        return "int"
    if re.fullmatch(r"(N|VAR|NVAR)?CHAR2?\(.*\)|N?TEXT|(NVAR|VAR)CHAR2?(\(.*\))?", n):
        return "text"
    if re.fullmatch(r"(DECIMAL|NUMERIC|NUMBER)\(\d+,\s*\d+\)|MONEY|SMALLMONEY|NUMBER|NUMERIC|DECIMAL", n):
        return "decimal"
    if delivery == "Float32" or re.fullmatch(FLOAT4_NATIVE.get(engine, "(?!)"), n):
        return "float4"
    if delivery == "Float64" or re.fullmatch(FLOAT8_NATIVE.get(engine, "(?!)"), n):
        return "float8"
    if n in ("BOOLEAN", "BIT") or delivery == "Boolean":
        return "bool"
    return "other"


TEMPORAL = re.compile(r"(DATE|TIMESTAMPTZ|TIMESTAMP|DATETIME2|DATETIME|SMALLDATETIME|DATETIMEOFFSET)(\(\d\))?( WITH (LOCAL )?TIME ZONE)?( NULL)?")


ZERO_DATE_LIT = re.compile(r"'(0000-00-00|\d{4}-00-\d\d|\d{4}-\d\d-00)( [0-9:.]+)?'")
ALTER_TABLE = re.compile(r"\bALTER\s+TABLE\b", re.I)


def is_zero_date_null(engine: str, lit: str, verdict: str) -> bool:
    """Whether a NULLED value is a MySQL zero or partial date, which rivet writes as NULL by design under a counted WARN."""
    return verdict == NULLED and engine == "mysql" and bool(ZERO_DATE_LIT.fullmatch(lit.strip()))


def server_text_of(url: str, col: Col, n_rows: int) -> list[str | None] | None:
    """The source's own text of each seeded value (PostgreSQL `::text`), the expectation of a column delivered as server text; None when unreadable."""
    p = cdc._psql(url, "-Atc", f"SELECT id, {col.name}::text IS NULL, {col.name}::text FROM {TABLE} ORDER BY id")
    rows = [line.split("|", 2) for line in p.out.splitlines() if line.strip()] if p.ok else []
    if len(rows) != n_rows or any(len(r) != 3 for r in rows):
        return None
    return [None if r[1] == "t" else r[2] for r in rows]


def server_text_verdict(exp: str | None, got, csv: bool) -> str:
    """OK, WRONG or NULLED for one delivered cell against the source's own text of its value."""
    if exp is None:
        return "OK" if got is None or (csv and got == "") else WRONG
    if got is None:
        return NULLED
    return "OK" if got == exp else WRONG


def zero_date_unwarned(zeros: int, warned: int) -> str:
    """The failure text when the zero dates delivered as NULL are not the number the run's WARN counted, else empty."""
    return f"{zeros} zero or partial dates delivered as NULL, the run's WARN counted {warned}" if zeros and zeros != warned else ""
MYSQL_LAX = "SET SESSION sql_mode = ''; "
ORACLE_SESSION_ZONE = "Europe/Brussels"
ORACLE_ZONE = f"ALTER SESSION SET TIME_ZONE = '{ORACLE_SESSION_ZONE}'; "


RIVET_TYPE_FAMILY = {
    "bool": "bool", "int16": "int", "int32": "int", "int64": "int", "u_int64": "int", "float32": "float4", "float64": "float8",
    "decimal": "decimal", "date": "other", "time": "other", "timestamp": "other", "string": "text", "text": "text",
    "binary": "other", "json": "other", "uuid": "other", "enum": "other", "interval": "other", "list": "other", "unsupported": "other",
}


def row_cols(engine: str, i: int, r: dict) -> Col:
    """The seed column of ledger row `i`: its samples and the edges every stage delivers (a column that refuses holds every edge)."""
    fam, delivery, whole = family(r["native_type"], r.get("delivery", ""), engine), r.get("delivery", ""), te.row_verdict(r)
    edges = te.edges_of(r)
    lits = list(dict.fromkeys([*r["sample"], *(e.lit for e in edges if whole or (not e.solo and e.x.kind in (te.EXACT, te.NULL)))]))
    by_lit = {e.lit: e for e in edges}
    return Col(f"c{i:02d}", r["native_type"], delivery, fam, tuple(lits), tuple(by_lit.get(x, te.Edge(x)) for x in lits), whole)


def columns(engine: str) -> list[Col]:
    """The seed columns of the override grid: the column of every ledger row that names a delivery."""
    return [row_cols(engine, i, r) for i, r in enumerate(ledger_rows(engine)) if r.get("delivery")]


def row_inserts(engine: str, cols: list[Col], table: str = TABLE, offset: int = 0) -> tuple[list[str], int]:
    """INSERTs of one row per value index, then an all-NULL row, with ids shifted by `offset`."""
    rows = max(len(c.lits) for c in cols)
    ts = TS_LITERAL.get(engine, "'2026-11-01 00:00:00'")
    names = "id, " + ", ".join(c.name for c in cols) + ", nsn_ts"
    ins = []
    for i in range(rows):
        vals = [c.lits[i] if i < len(c.lits) else "NULL" for c in cols]
        ins.append(f"INSERT INTO {table} ({names}) VALUES ({offset + i + 1}, {', '.join(vals)}, {ts})")
    ins.append(f"INSERT INTO {table} ({names}) VALUES ({offset + rows + 1}, {', '.join('NULL' for _ in cols)}, {ts})")
    return ins, rows + 1


def seed_sql(engine: str, cols: list[Col], table: str = TABLE) -> tuple[list[str], int]:
    """Statements that (re)create the seed table: one row per value index, then an all-NULL row."""
    idt = "NUMBER(9)" if engine == "oracle" else "INT"
    ddl = f"CREATE TABLE {table} (id {idt} NOT NULL PRIMARY KEY, " + ", ".join(f"{c.name} {c.native}" for c in cols) + f", nsn_ts {TS_NATIVE[engine]})"
    ins, n = row_inserts(engine, cols, table)
    return [f"DROP TABLE IF EXISTS {table}", ddl, *ins], n


def mysql_text(statement: str) -> str:
    """`statement` led by an empty sql_mode when it names a zero date or alters a table, which the stand's strict mode refuses."""
    return (MYSQL_LAX if ZERO_DATE_LIT.search(statement) or ALTER_TABLE.search(statement) else "") + statement


def sql(engine: str, url: str, statement: str) -> core.Proc:
    """One source statement of this matrix, MySQL under the session mode mysql_text picks, Oracle in the fixed session zone."""
    text = mysql_text(statement) if engine == "mysql" else ORACLE_ZONE + statement if engine == "oracle" else statement
    return engines.sql(engine, url, text)


def seed(engine: str, url: str) -> tuple[list[Col], int]:
    """Create the seed table on the stand; raises when the server refuses a statement."""
    cols = columns(engine)
    stmts, n = seed_sql(engine, cols)
    for s in ledger_setup(engine):
        sql(engine, url, s)
    for st in stmts:
        p = sql(engine, url, st + ";")
        if not p.ok:
            raise RuntimeError(f"{engine} seed refused `{st[:160]}`: exit {p.returncode}: {p.out.strip()[:300]}")
    return cols, n


def ledger_setup(engine: str) -> list[str]:
    """Engine-level DDL the ledger rows need before the table (types they name)."""
    return list(yaml.safe_load(LEDGER.read_text())["engines"][engine].get("setup", []))


_NUM = re.compile(r"[+-]?\d+(\.\d+)?([eE][+-]?\d+)?")
_STR = re.compile(r"N?'((?:[^']|'')*)'")
_STR_MYSQL = re.compile(r"'((?:[^'\\]|\\.|'')*)'", re.S)
_MYSQL_ESC = {"0": "\0", "n": "\n", "r": "\r", "t": "\t", "b": "\b", "Z": "\x1a"}
_TS_FUNC = re.compile(r"(?:TO_DATE|TO_TIMESTAMP_TZ|TO_TIMESTAMP)\('([^']*)'\s*,\s*'[^']*'\)|(?:DATE|TIMESTAMP) '([^']*)'", re.I)
_HEX = re.compile(r"0x([0-9A-Fa-f]*)|(?:UNHEX|HEXTORAW)\('([0-9A-Fa-f]*)'\)|x'([0-9A-Fa-f]*)'", re.I)
_PG_HEX = re.compile(r"'\\x([0-9A-Fa-f]*)'(?:::bytea)?")
_BITS = re.compile(r"b'([01]+)'")
_CAST = re.compile(r"('(?:[^']|'')*')::[a-z_]+")


def parse_lit(engine: str, lit: str):
    """The Python value of a simple SQL literal: None for NULL, Decimal, bool, str; `...` when it cannot be read."""
    s = te.reduce(engine, lit).strip()
    if (n := te.numeric_of(s)) is not None:
        return n
    if s.upper() == "NULL" or (engine == "oracle" and s == "''"):
        return None
    if _NUM.fullmatch(s):
        return Decimal(s)
    if s.upper() in ("TRUE", "FALSE"):
        return s.upper() == "TRUE"
    if m := _BITS.fullmatch(s):
        return Decimal(int(m.group(1), 2))
    if m := _HEX.fullmatch(s):
        hexed = m.group(1) or m.group(2) or m.group(3) or ""
        return None if engine == "oracle" and not hexed else bytes.fromhex(hexed)
    if engine == "postgres" and (m := _PG_HEX.fullmatch(s)):
        return bytes.fromhex(m.group(1))
    if m := _TS_FUNC.fullmatch(s):
        return m.group(1) if m.group(1) is not None else m.group(2)
    if engine == "postgres" and (m := _CAST.fullmatch(s)):
        return parse_lit(engine, m.group(1))
    m = (_STR_MYSQL if engine == "mysql" else _STR).fullmatch(s)
    if not m:
        return ...
    body = m.group(1).replace("''", "'")
    if engine == "mysql":
        return re.sub(r"\\(.)", lambda e: _MYSQL_ESC.get(e.group(1), e.group(1)), body, flags=re.S)
    return body


def _num(v):
    """A Decimal of a delivered scalar, or None when it is no number."""
    if isinstance(v, bool):
        return Decimal(int(v))
    if isinstance(v, (int, Decimal)):
        return Decimal(v)
    if isinstance(v, float):
        return Decimal(repr(v)) if v == v and abs(v) != float("inf") else None
    if isinstance(v, str):
        try:
            return Decimal(v.strip())
        except InvalidOperation:
            return {"true": Decimal(1), "false": Decimal(0)}.get(v.strip().lower())
    return None


def tolerance(src_family: str, target: str, expected: Decimal) -> Decimal:
    """The relative slack a float on either side of the conversion allows: its own rounding, never more."""
    if "float4" in (src_family, _target_family(target)):
        return max(abs(expected) * Decimal(2) ** -23, Decimal(2) ** -149)
    if "float8" in (src_family, _target_family(target)):
        return max(abs(expected) * Decimal(2) ** -51, Decimal(2) ** -1074)
    return Decimal(0)


def _within_slack(src_family: str, target: str, e: Decimal, g: Decimal) -> bool:
    """Whether g is e within the float slack; a text that reads as a huge or non-finite number is a difference, never a crash."""
    with localcontext(Context(traps=[])):
        return abs(g - e) <= tolerance(src_family, target, e)


def _target_family(target: str) -> str:
    """The value family a override target delivers."""
    return {"float4": "float4", "float8": "float8", "bool": "bool", "text": "text"}.get(
        target, "int" if target.startswith("int") else "decimal" if target.startswith("decimal") else "other")


_ZONE = re.compile(r"\s+([A-Za-z]+/[A-Za-z_]+)$")
_ZONED = re.compile(r"\d:\d{2}(?::\d{2}(?:\.\d+)?)?\s*(?:[+-]\d{2}:?\d{2}|UTC|Z|[A-Za-z]+/[A-Za-z_]+)$")
_OFFSET = re.compile(r"\s*(?:(UTC|Z)|([+-]\d{2})(?::?(\d{2}))?)$")
TEMPORAL_SLACK = {("mssql", "DATETIME"): timedelta(milliseconds=4), ("mssql", "SMALLDATETIME"): timedelta(seconds=30)}


def instant(v):
    """A date, a signed time of day as a timedelta, or a UTC-naive datetime of a temporal value or its text; None when it reads as no instant."""
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo else v
    if isinstance(v, (date, timedelta)):
        return v
    if isinstance(v, dtime):
        return timedelta(hours=v.hour, minutes=v.minute, seconds=v.second, microseconds=v.microsecond)
    if not isinstance(v, str):
        return None
    s, zone = v.strip().replace("T", " "), None
    if m := _ZONE.search(s):
        s, zone = s[:m.start()], ZoneInfo(m.group(1))
    elif (m := _OFFSET.search(s)) and ":" in s[:m.start()] and (m.group(1) or len(s[:m.start()]) > 10 or m.group(2)[0] == "+"):
        sign, hh, mm = (1, 0, 0) if m.group(1) else (1 if m.group(2)[0] == "+" else -1, int(m.group(2)[1:]), int(m.group(3) or 0))
        s, zone = s[:m.start()], timezone(sign * timedelta(hours=hh, minutes=mm))
    try:
        if len(s) == 10 and s[4] == "-":
            return date.fromisoformat(s)
        if m := re.fullmatch(r"(-?)(\d+):(\d{2}):(\d{2})(?:\.(\d+))?", s):
            t = timedelta(hours=int(m[2]), minutes=int(m[3]), seconds=int(m[4]), microseconds=int((m[5] or "")[:6].ljust(6, "0")))
            return -t if m[1] else t
        d = datetime.fromisoformat(s)
    except ValueError:
        return None
    return d.replace(tzinfo=zone).astimezone(timezone.utc).replace(tzinfo=None) if zone else d


def _sub_us(v) -> int:
    """The nanoseconds past the microsecond that a temporal text spells; 0 for a value that holds none."""
    if isinstance(v, str) and (m := re.search(r":\d{2}\.(\d+)", v)):
        return int(m[1][6:9].ljust(3, "0")) if len(m[1]) > 6 else 0
    return getattr(v, "nanosecond", 0)


def _same_instant(engine: str, native: str, exp: str, got) -> str:
    """OK, WRONG or PRESENCE for a temporal source value against a delivered instant or its text."""
    if isinstance(got, str) and exp.startswith("0000-") and got.startswith("0001-") and got.endswith(" (BC)"):
        return "OK" if got[4:10] == exp[4:10] else WRONG
    e, g = instant(exp), instant(got)
    if engine == "oracle" and "TIME ZONE" in native.upper() and isinstance(e, datetime) and not _ZONED.search(exp):
        try:
            e = e.replace(tzinfo=ZoneInfo(ORACLE_SESSION_ZONE)).astimezone(timezone.utc).replace(tzinfo=None)
        except OverflowError:
            return "PRESENCE"
    if isinstance(e, timedelta) and isinstance(got, (int, float, Decimal)) and not isinstance(got, bool):
        return "OK" if Decimal(e.days * 86400 + e.seconds) + Decimal(e.microseconds) / 10**6 == Decimal(str(got)) else WRONG
    if e is None or g is None:
        return WRONG if e is None and isinstance(got, (date, dtime, timedelta)) else "PRESENCE"
    if type(e) is date and type(g) is datetime:
        e = datetime.combine(e, dtime())
    if type(g) is date and type(e) is datetime:
        g = datetime.combine(g, dtime())
    if type(e) is not type(g):
        return "PRESENCE"
    slack = TEMPORAL_SLACK.get((engine, native.upper()))
    if slack is None:
        return "OK" if abs(e - g) < timedelta(microseconds=1) and _sub_us(exp) == _sub_us(got) else WRONG
    return "OK" if abs(e - g) < slack else WRONG


def _same_bytes(exp: bytes, got) -> str:
    """OK, WRONG or PRESENCE for a binary source value; a fixed-width column may pad with zero bytes."""
    if isinstance(got, (bytes, bytearray, memoryview)):
        g = bytes(got)
        return "OK" if g == exp or (g[:len(exp)] == exp and not g[len(exp):].strip(b"\0")) else WRONG
    if isinstance(got, str) and got.strip().lower().removeprefix("\\x").removeprefix("0x").rstrip("0") == exp.hex().rstrip("0"):
        return "OK"
    return "PRESENCE"


def compare_other(engine: str, native: str, exp, target: str, got) -> str:
    """OK, WRONG or PRESENCE for a family-`other` or text source value: as an instant, JSON, UUID, bytes or text."""
    if isinstance(exp, bytes):
        return _same_bytes(exp, got)
    if not isinstance(exp, str):
        return "PRESENCE"
    n = native.upper()
    if TEMPORAL.fullmatch(n) or n.startswith("TIME") or isinstance(got, (date, dtime, timedelta)):
        return _same_instant(engine, native, exp, got)
    if n in ("UUID", "UNIQUEIDENTIFIER") and isinstance(got, bytes) and len(got) == 16:
        return "OK" if str(UUID(bytes=got)) == exp.lower() else WRONG
    if isinstance(got, UUID) or n in ("UUID", "UNIQUEIDENTIFIER"):
        return "OK" if str(got).lower() == exp.lower() else WRONG if isinstance(got, (str, UUID)) else "PRESENCE"
    if n in ("JSON", "JSONB") and isinstance(got, str):
        try:
            return "OK" if json.loads(got) == json.loads(exp) else WRONG
        except ValueError:
            return WRONG
    if isinstance(got, str):
        return "OK" if got.rstrip(" ") == exp.rstrip(" ") else WRONG
    return "PRESENCE"


def _unhex_text(hexed: str) -> str | None:
    """The UTF-8 text a CSV hex rendering of a binary column holds; None when it is no hex of text."""
    try:
        return bytes.fromhex(hexed).decode("utf-8")
    except ValueError:
        return None


def _same_special(exp: float, got) -> str:
    """OK, WRONG or PRESENCE for a NaN, an infinity or a signed zero against a delivered float or its text."""
    if isinstance(got, str):
        t = got.strip().lower().replace("infinity", "inf")
        got = float("nan") if t in ("nan", "+nan", "-nan") else float(t) if t in ("inf", "+inf", "-inf", "-0", "-0.0", "0", "0.0") else got
    if isinstance(got, (int, Decimal)) and not isinstance(got, bool):
        got = float(got)
    if not isinstance(got, float):
        return "PRESENCE"
    if exp != exp:
        return "OK" if got != got else WRONG
    return "OK" if got == exp and math.copysign(1, got) == math.copysign(1, exp) else WRONG


_ARRAY_FN = re.compile(r"ARRAY\[(.*?)\](?:::[A-Za-z_ ]+\[\])?", re.S | re.I)
_ARRAY_TEXT = re.compile(r"'(\{.*\})'(?:::[A-Za-z_ ]+(?:\(\d+(?:,\s*\d+)?\))?\[\])?", re.S)
_ELEM_DELIVERY = {"REAL": "Float32", "FLOAT4": "Float32", "DOUBLE PRECISION": "Float64", "FLOAT8": "Float64", "BOOLEAN": "Boolean"}


def pg_array_elements(lit: str, base: str):
    """The element source literals of a one-dimensional PostgreSQL array literal (`ARRAY[..]` or `'{..}'`); None when it is not one or is multi-dimensional."""
    s = lit.strip()
    if m := _ARRAY_FN.fullmatch(s):
        inner = m.group(1).strip()
        return [] if not inner else te._args(inner)
    if not (m := _ARRAY_TEXT.fullmatch(s)):
        return None
    body, out, i = m.group(1)[1:-1].replace("''", "'"), [], 0
    if not body.strip():
        return []
    bytea = base.upper() == "BYTEA"
    while i <= len(body):
        while i < len(body) and body[i] == " ":
            i += 1
        if i < len(body) and body[i] == "{":
            return None
        if i < len(body) and body[i] == '"':
            j, text = i + 1, ""
            while j < len(body) and body[j] != '"':
                if body[j] == "\\":
                    j += 1
                text += body[j]
                j += 1
            i, null = j + 1, False
        else:
            j = body.find(",", i) if "," in body[i:] else len(body)
            text = body[i:j].strip()
            i, null = j, text.upper() == "NULL"
        out.append("NULL" if null else "'" + text.replace("'", "''") + "'" + ("::bytea" if bytea else ""))
        if i >= len(body):
            break
        i += 1
    return out


def element_got(exp, got, base: str):
    """A delivered element in the type its source literal is read as: bytes of a text value decode, text of a binary value encodes."""
    if isinstance(got, str) and isinstance(exp, bytes):
        return got.encode("utf-8", "surrogateescape")
    if isinstance(got, bytes) and not isinstance(exp, bytes) and base.upper() not in ("UUID", "UNIQUEIDENTIFIER"):
        return got.decode("utf-8", "surrogateescape")
    return got


def same_array(engine: str, lit: str, native: str, target: str, got) -> str:
    """OK, WRONG, NULLED or PRESENCE for a one-dimensional array: each element against its source literal, never as a whole."""
    base = native[:-2]
    elems = pg_array_elements(lit, base)
    if elems is None or not isinstance(got, (list, tuple)):
        return "PRESENCE"
    if len(elems) != len(got):
        return WRONG
    fam = family(base, _ELEM_DELIVERY.get(base.upper(), ""), engine)
    seen = set()
    for e, g in zip(elems, got):
        if fam == "bool" and e.lower() in ("'t'", "'f'"):
            e = "TRUE" if e.lower() == "'t'" else "FALSE"
        elif fam in ("int", "decimal", "float4", "float8") and _NUM.fullmatch(e.strip("'")):
            e = e.strip("'")
        exp = parse_lit(engine, e)
        if exp is None:
            seen.add("OK" if g is None else WRONG)
        elif g is None:
            seen.add(NULLED)
        elif exp is ... and fam not in ("float4", "float8"):
            seen.add("PRESENCE")
        else:
            seen.add(compare(engine, e, fam, target, element_got(exp, g, base), False, base))
    return next((v for v in (WRONG, NULLED, "PRESENCE") if v in seen), "OK")


def _blank(exp) -> bool:
    """Whether a source value is the empty string or bytes, or a CHAR the server pads with blanks."""
    return exp == b"" or (isinstance(exp, str) and exp.rstrip(" ") == "")


def compare(engine: str, src_lit: str, src_family: str, target: str, got, csv: bool = False, native: str = "") -> str:
    """OK, PRESENCE (not comparable), NULLED or WRONG for one delivered cell against its source literal."""
    if src_family in ("float4", "float8") and (sp := te.special_float(src_lit)) is not None:
        return NULLED if got is None else _same_special(sp, got)
    exp = parse_lit(engine, src_lit)
    if exp is None:
        return "OK"
    if got is None:
        return NULLED
    if csv and got == "" and not _blank(exp):
        return WRONG
    if engine == "postgres" and native.endswith("[]"):
        return same_array(engine, src_lit, native, target, got)
    if target == "binary" and csv and isinstance(got, str) and not isinstance(exp, bytes):
        got = _unhex_text(got)
        if got is None:
            return WRONG
    tf = _target_family(target)
    if isinstance(exp, str) and tf in ("int", "float4", "float8", "decimal", "bool"):
        if _NUM.fullmatch(exp.strip()):
            exp = Decimal(exp.strip())
        else:
            return WRONG
    if isinstance(exp, (Decimal, bool)):
        e, g = _num(exp), _num(got)
        if e is None or g is None:
            return "PRESENCE"
        if src_family == "bool" and not isinstance(exp, bool):
            return "OK" if (g != 0) == (e != 0) else WRONG
        if tf == "bool" and not isinstance(exp, bool):
            return "OK" if (g != 0) == (e != 0) and e in (0, 1) else WRONG
        return "OK" if _within_slack(src_family, target, e, g) else WRONG
    if exp is ...:
        return "PRESENCE"
    return compare_other(engine, native, exp, target, got)


def config(engine: str, url_env: str, mode: str, fmt: str, col: str, target: str, out: str, cols: tuple[str, ...] | None = None) -> str:
    """A rivet config for one cell: the seed table, one value column, one override, one mode and one format."""
    fold = str.upper if engine == "oracle" else str
    col = fold(col)
    query = f"SELECT id, {col} FROM {'dbo.' if engine == 'mssql' else ''}{TABLE}"
    if mode == "time_window":
        query = f"SELECT id, {col}, nsn_ts FROM {'dbo.' if engine == 'mssql' else ''}{TABLE}"
    exp = {"name": "nsn", "query": query, "mode": mode, "format": fmt, "destination": {"type": "local", "path": out}}
    if engine == "mongo":
        exp = {"name": "nsn", "table": TABLE, "mode": "full", "format": fmt, "destination": {"type": "local", "path": out}}
    exp |= {k: fold(v) if isinstance(v, str) else v for k, v in MODE_SHAPES[mode].items()} if engine != "mongo" else {}
    if target != "none":
        exp["columns"] = {col: target}
    return yaml.safe_dump({"source": {"type": engine, "url_env": url_env}, "exports": [exp]}, sort_keys=False)


def _rivet_bin() -> str:
    """The binary under test: the gate's release build, or the one `RIVET_BIN_OVERRIDE` names."""
    return os.environ.get("RIVET_BIN_OVERRIDE") or str(core.rivet_bin())


def read_parts(out: Path, fmt: str, key: str = "id"):
    """(types by column, rows by `key` or a list when key is empty, part count) of what a run delivered, read by pyarrow and DuckDB; None when nothing was delivered."""
    return read_files(sorted(str(p) for p in data_files(out, fmt)), fmt, key)


READER = read_parts


def reader_trusted(reader) -> str:
    """Empty when the reader tells a NULL-for-value canary from a faithful one; else why its verdicts cannot be trusted."""
    col = Col("c00", "INT", "Int32", "int", ("7",))
    with tempfile.TemporaryDirectory() as t:
        for name, body, klass in (("nulled", "id,c00\n1,\n", NULLED), ("faithful", "id,c00\n1,7\n", "")):
            d = Path(t) / name
            d.mkdir()
            (d / "p.csv").write_text(body)
            (d / "manifest.json").write_text('{"status": "success", "parts": ["p.csv"]}')
            r = grade_run("mysql", name, _proc(0), _proc(0), "exact", d, "csv", col, 1, "none", list(col.lits), reader=reader)
            if r.klass != klass:
                return f"the second reader graded the {name} canary as `{r.klass or 'ok'}` {r.detail}"[:300]
    return ""


def data_files(out: Path, fmt: str) -> list[Path]:
    """Every data file under the whole output directory, whatever a manifest says."""
    return sorted(out.rglob(f"*.{'parquet' if fmt == 'parquet' else 'csv'}"))


def part_set_diff(out: Path, fmt: str) -> str:
    """The parts the manifests declare that are missing and the parts on disk no manifest declares; empty when the sets agree."""
    from .scenarios import success_part_names

    docs = sorted(out.rglob("manifest-*.json")) or [m for m in (out / "manifest.json",) if m.is_file()]
    declared = {(d.parent / n).resolve() for d in docs for n in success_part_names(json.loads(d.read_text()))}
    present = {p.resolve() for p in data_files(out, fmt)}
    missing, unlisted = sorted(declared - present), sorted(present - declared)
    return "; ".join(f"{k} {[x.name for x in v][:3]}" for k, v in (("declared but missing", missing), ("present but undeclared", unlisted)) if v)


def read_files(parts: list[str], fmt: str, key: str = "id"):
    """As `read_parts`, for an explicit list of part files."""
    import duckdb

    if not parts:
        return None
    with duckdb.connect() as con:
        con.execute("SET TimeZone = 'UTC'")
        wide: dict[str, str] = {}
        if fmt == "parquet":
            import pyarrow as pa
            import pyarrow.parquet as pq

            tbl = pa.concat_tables([pq.read_table(p) for p in parts], promote_options="permissive")
            wide = {f.name.lower(): f"DECIMAL({f.type.precision},{f.type.scale})" for f in tbl.schema if pa.types.is_decimal256(f.type)}
            for f in tbl.schema:
                if f.name.lower() in wide:
                    tbl = tbl.set_column(tbl.schema.get_field_index(f.name), f.name, tbl.column(f.name).cast(pa.string()))
            con.register("nsn_parts", tbl)
            src = "nsn_parts"
        else:
            src = f"read_csv({parts!r}, header=true, all_varchar=true, union_by_name=true, allow_quoted_nulls=false)"
        desc = {r[0].lower(): wide.get(r[0].lower(), r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
        sel = ", ".join(f'CAST("{k}" AS VARCHAR{"[]" if t.endswith("[]") else ""}) AS "{k}"' if "TIME ZONE" in t else f'"{k}"' for k, t in desc.items())
        cur = con.execute(f"SELECT {sel} FROM {src}")
        names = [d[0].lower() for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]
        return desc, ({int(r[key]): r for r in rows} if key else rows), len(parts)


DUCK_TYPE = {
    "bool": ("BOOLEAN",), "int2": ("SMALLINT",), "int4": ("INTEGER",), "int8": ("BIGINT",), "float4": ("FLOAT",),
    "float8": ("DOUBLE",), "text": ("VARCHAR",), "binary": ("BLOB",), "date": ("DATE",), "json": ("VARCHAR", "JSON"),
    "uuid": ("UUID", "VARCHAR"), "timestamp": ("TIMESTAMP",), "timestamp_ns": ("TIMESTAMP_NS",),
    "timestamp_tz": ("TIMESTAMP WITH TIME ZONE",), "timestamp_tz_ns": ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP_NS"),
}


def type_ok(target: str, duck: str) -> bool:
    """Whether the delivered DuckDB type is the one the override declares."""
    if target.startswith("decimal("):
        return duck.startswith("DECIMAL") or (duck == "DOUBLE" and int(target[8:].split(",")[0]) > 38)
    return duck in DUCK_TYPE.get(target, (duck,))


@dataclass
class Result:
    cell: str
    verdict: str
    klass: str = ""
    detail: str = ""
    code: str = ""
    fidelity: str = ""


def fidelity_of(stdout: str, col: str) -> str:
    """The fidelity `rivet check --type-report --json` states for one column."""
    for ln in stdout.splitlines():
        if ln.startswith("{"):
            try:
                for c in json.loads(ln).get("columns", []):
                    if c["column"].lower() == col.lower():
                        return c.get("fidelity", "")
            except (ValueError, KeyError):
                continue
    return ""


def stand_refused(p: core.Proc) -> bool:
    """Whether the process stopped, or its type probe was skipped, because the stand would not serve it: that says nothing about the cell."""
    return (p.returncode != 0 and (CODE.search(p.stderr + p.stdout) or [None, ""])[1] in STAND_REFUSALS) or bool(PROBE_UNREACHED.search(p.stderr))


def rivet(args: list[str], env: dict, cwd: Path) -> core.Proc:
    """One rivet process; a stand that refused it is asked again a few times before the refusal is kept."""
    for i in range(STAND_TRIES):
        p = core.run([_rivet_bin(), *args], env={**CELL_STATE, **env}, cwd=cwd, timeout=RUN_TIMEOUT)
        if not stand_refused(p):
            break
        time.sleep(1 + 2 * i)
    return p


def grade_stop(cell: str, p: core.Proc, check: core.Proc) -> Result | None:
    """The verdict of a run that stopped non-zero: a coded refusal is fine, a crash, an uncoded stop or a check/run split is not."""
    if p.returncode == 0:
        return None
    code = (CODE.search(p.stderr + p.stdout) or [None, ""])[1]
    if stand_refused(p) or stand_refused(check):
        return Result(cell, REFUSED, NOT_GRADED, f"the stand refused the process {STAND_TRIES} times: exit {p.returncode} [{code or 'check'}]", code)
    if p.returncode in (101, 124, 127) or p.returncode < 0:
        return Result(cell, REFUSED, CRASH, f"exit {p.returncode}: {core.first_error(p.stderr)[:160]}", code)
    if code == LEASE_REFUSAL:
        return Result(cell, REFUSED, UNGRADED, f"another cell of this matrix holds the run lease, so no value was read: exit {p.returncode} [{code}]", code)
    if code.startswith("RIVET_CONFIG_") and code not in TYPE_REFUSALS:
        return Result(cell, REFUSED, UNGRADED, f"the harness's own config was refused before any value was read: exit {p.returncode} [{code}]", code)
    if not code or code.startswith("RIVET_INTERNAL"):
        return Result(cell, REFUSED, UNCODED, f"exit {p.returncode} {code or 'no code'}: {core.first_error(p.stderr)[:160]}", code)
    if check.returncode == 0:
        return Result(cell, REFUSED, SPLIT, f"check exit 0, run exit {p.returncode} [{code}]", code)
    return Result(cell, REFUSED, "", f"check exit {check.returncode} [{(CODE.search(check.stderr + check.stdout) or [None, ''])[1]}]", code)


def grade_run(engine: str, cell: str, p: core.Proc, check: core.Proc, fidelity: str, out: Path, fmt: str, col: Col,
              n_rows: int, target: str, src_lits: list[str], want: frozenset[str] = frozenset(), reader=None,
              expect: list[str | None] | None = None) -> Result:
    """Grade one delivered or refused run against the source rows seeded by native SQL: the second reader over the whole output directory, never rivet's own counters or manifest."""
    stop = grade_stop(cell, p, check)
    if stop:
        return stop
    try:
        got = (reader or READER)(out, fmt)
        stray = part_set_diff(out, fmt)
    except Exception as e:
        return Result(cell, DELIVERED, NOT_GRADED, f"the second reader failed: {type(e).__name__}: {e}"[:300])
    if stray:
        return Result(cell, DELIVERED, PARTS, stray, fidelity=fidelity)
    if got is None or len(got[1]) != n_rows:
        return Result(cell, DELIVERED, EMPTY, f"exit 0, {0 if got is None else len(got[1])} of {n_rows} rows delivered")
    desc, rows, _ = got
    key = col.name.lower()
    if want and set(desc) != want:
        return Result(cell, DELIVERED, COLUMNS, f"delivered {sorted(desc)}, expected {sorted(want)}", fidelity=fidelity)
    if fmt == "parquet" and target != "none" and not type_ok(target, desc.get(key, "?")):
        return Result(cell, DELIVERED, IGNORED, f"declared {target}, delivered {desc.get(key)}", fidelity=fidelity)
    nulled, wrong, zeros = [], [], 0
    for i in range(1, n_rows + 1):
        lit = src_lits[i - 1] if i - 1 < len(src_lits) else "NULL"
        v = compare(engine, lit, col.family, target, rows[i][key], fmt == "csv", col.native) if expect is None \
            else server_text_verdict(expect[i - 1], rows[i][key], fmt == "csv")
        if is_zero_date_null(engine, lit, v):
            zeros += 1
        elif v == NULLED:
            nulled.append(lit)
        elif v == WRONG:
            wrong.append(f"{lit}->{rows[i][key]!r}")
    if unwarned := zero_date_unwarned(zeros, warned_counts(p.out).get(key, 0)):
        nulled.append(unwarned)
    if nulled:
        return Result(cell, DELIVERED, NULLED, f"{len(nulled)} non-NULL values delivered as NULL, e.g. {nulled[:3]}", fidelity=fidelity)
    if wrong and fidelity in ("lossy", "logical_string", "unsupported"):
        return Result(cell, DECLARED, "", f"{len(wrong)} values changed under fidelity {fidelity}", fidelity=fidelity)
    if wrong:
        return Result(cell, DELIVERED, WRONG, f"{len(wrong)} values changed under fidelity {fidelity or '?'}, e.g. {wrong[:3]}", fidelity=fidelity)
    return Result(cell, DELIVERED, "", "", fidelity=fidelity)


def recheck(check, cfg: Path, env: dict, d: Path):
    """A check that passed where run refused is repeated; a refusing repeat is the stable verdict, the stand's probe flake is not."""
    for _ in range(RECHECKS):
        again = rivet(["check", "-c", str(cfg), "--type-report", "--json"], env, d)
        if again.returncode != 0 and not stand_refused(again):
            return again
    return check


def format_config(d: Path, fmt: str) -> Path:
    """The config path of one format of a cell, in a directory of its own: the state a run keeps is anchored beside its config and must not carry into the next format."""
    sub = d / fmt
    sub.mkdir(parents=True, exist_ok=True)
    return sub / f"{fmt}.yaml"


def run_unit(engine: str, work: Path, url: str, col: Col, n_rows: int, mode: str, target: str, fmts: list[str]) -> list[Result]:
    """Check then run each format of one (column, override, mode) cell in its own directory."""
    d = work / f"{col.name}-{target.replace('(', '_').replace(')', '').replace(',', '_')}-{mode}"
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    env = {"RIVET_NSN_URL": url}
    out = []
    fid = ""
    for fmt in fmts:
        cell = f"{engine}/{col.native}->{target}/{mode}/{fmt}"
        cfg = format_config(d, fmt)
        cfg.write_text(config(engine, "RIVET_NSN_URL", mode, fmt, col.name, target, str(d / f"out-{fmt}")))
        check = rivet(["check", "-c", str(cfg), "--type-report", "--json"], env, d)
        if stand_refused(check):
            out.append(Result(cell, REFUSED, NOT_GRADED, f"the stand refused the check's type probe {STAND_TRIES} times"))
            continue
        fid = fidelity_of(check.stdout, col.name) or fid
        p = rivet(["run", "-c", str(cfg)], env, d)
        if check.returncode == 0 and p.returncode != 0:
            check = recheck(check, cfg, env, d)
        want = frozenset(["id", col.name.lower(), *(["nsn_ts"] if mode == "time_window" else [])])
        expect = server_text_of(url, col, n_rows) if engine == "postgres" and col.delivery == "server_text" else None
        if engine == "postgres" and col.delivery == "server_text" and expect is None:
            out.append(Result(cell, DELIVERED, NOT_GRADED, "the source's own text of the seeded values could not be read"))
            continue
        r = grade_run(engine, cell, p, check, fid, d / f"out-{fmt}", fmt, col, n_rows, target, list(col.lits), want, expect=expect)
        out.append(r)
        if r.klass in (NULLED, WRONG):
            out.append(integrity(cell, rivet(["validate", "-c", str(cfg), "--depth", "full"], env, d), r))
    shutil.rmtree(d, ignore_errors=True)
    return out


def integrity(cell: str, v: core.Proc, harmed: Result) -> Result:
    """The verdict of `rivet validate --depth full` over a run whose values the second reader found changed: exit 0 is an integrity check blind to the harm."""
    if v.returncode == 0:
        return Result(f"{cell}/integrity", DELIVERED, BLIND, f"validate --depth full exit 0 over a {harmed.klass} cell: {harmed.detail}"[:300])
    return Result(f"{cell}/integrity", REFUSED, "", f"validate exit {v.returncode}", (CODE.search(v.stderr + v.stdout) or [None, ""])[1])


def grid(engine: str, cols: list[Col], tier: str) -> list[tuple[Col, str, str, tuple[str, ...]]]:
    """(column, target, mode, formats) units: the full cross in the full tier; in smoke every pair once plus the whole cross for one column per family."""
    ax = axes()
    fmts, modes = tuple(ax["formats"]), [m for m in ax["modes"] if m in MODE_SHAPES]
    modes = ["full"] if engine == "mongo" else modes
    units: list[tuple[Col, str, str, tuple[str, ...]]] = []
    seen: set[str] = set()
    for c in cols:
        rep = c.family not in seen
        seen.add(c.family)
        for t in ax["targets"]:
            if tier == "full" or rep:
                units += [(c, t, m, fmts if tier == "full" or m == "full" else fmts[:1]) for m in modes]
            else:
                units.append((c, t, "full", fmts[:1]))
    return units


def mongo_seed(url: str) -> int:
    """Create the seed collection from the ledger's Mongo rows: one document per literal index plus a sparse one; returns the document count."""
    from pymongo import MongoClient

    from . import type_matrix as tm

    docs = tm.mongo_grid()[0]
    with MongoClient(url, serverSelectionTimeoutMS=5000) as client:
        coll = client.get_default_database()[TABLE]
        coll.drop()
        coll.insert_many(docs)
        if coll.count_documents({}) != len(docs):
            raise RuntimeError(f"mongo seed refused: {coll.count_documents({})} of {len(docs)} documents")
    return len(docs)


MONGO_COLUMNS = frozenset(["_id", "document"])


def grade_mongo(cell: str, p: core.Proc, check: core.Proc, out: Path, fmt: str, column: str, target: str, n_docs: int, reader=None) -> Result:
    """Grade one Mongo cell: coded refusal, or every document delivered whole in exactly the `_id` and `document` columns and the override honoured."""
    from . import type_matrix as tm

    stop = grade_stop(cell, p, check)
    if stop:
        return stop
    try:
        got = (reader or (lambda o, f: READER(o, f, "")))(out, fmt)
        stray = part_set_diff(out, fmt)
    except Exception as e:
        return Result(cell, DELIVERED, NOT_GRADED, f"the second reader failed: {type(e).__name__}: {e}"[:300])
    if stray:
        return Result(cell, DELIVERED, PARTS, stray)
    if got is None or len(got[1]) != n_docs:
        return Result(cell, DELIVERED, EMPTY, f"exit 0, {0 if got is None else len(got[1])} of {n_docs} documents delivered")
    desc, rows, _ = got
    if set(desc) != MONGO_COLUMNS:
        return Result(cell, DELIVERED, COLUMNS, f"delivered {sorted(desc)}, expected {sorted(MONGO_COLUMNS)}")
    if fmt == "parquet" and target != "none" and not type_ok(target, desc.get(column, "?")):
        return Result(cell, DELIVERED, IGNORED, f"declared {target}, delivered {desc.get(column)}")
    bad: list[tuple[str, str]] = []
    for r in rows:
        if r.get("_id") is None:
            bad.append((NULLED, "_id delivered NULL"))
        try:
            doc = json.loads(r["document"]) if isinstance(r.get("document"), str) else None
        except ValueError:
            doc = None
        if doc is None:
            bad.append((NULLED, "document delivered NULL or unreadable"))
            continue
        bad += tm.mongo_grid_verdict(r["document"], int(doc["n"])) if "n" in doc else [(NULLED, "document lost its n")]
    if bad:
        klass = NULLED if any(k == NULLED for k, _ in bad) else WRONG
        return Result(cell, DELIVERED, klass, f"{len(bad)} fields: {[d for _, d in bad][:3]}")
    return Result(cell, DELIVERED, "")


def run_mongo(work: Path, url: str, tier: str) -> list[Result]:
    """Every override target on both Mongo columns, each format: check once, run, grade by the second reader."""
    n = mongo_seed(url)
    ax = axes()
    out: list[Result] = []
    lock = threading.Lock()

    def one(u):
        col, target = u
        d = work / f"{col}-{target.replace('(', '_').replace(')', '').replace(',', '_')}"
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)
        env = {"RIVET_NSN_URL": url}
        c0 = d / "check.yaml"
        c0.write_text(config("mongo", "RIVET_NSN_URL", "full", "parquet", col, target, str(d / "o-check")))
        check = rivet(["check", "-c", str(c0), "--type-report", "--json"], env, d)
        rs = []
        for fmt in ax["formats"]:
            cfg = format_config(d, fmt)
            cfg.write_text(config("mongo", "RIVET_NSN_URL", "full", fmt, col, target, str(d / f"o-{fmt}")))
            p = rivet(["run", "-c", str(cfg)], env, d)
            rs.append(grade_mongo(f"mongo/{col}->{target}/full/{fmt}", p, check, d / f"o-{fmt}", fmt, col, target, n))
        shutil.rmtree(d, ignore_errors=True)
        with lock:
            out.extend(rs)

    with cf.ThreadPoolExecutor(WORKERS) as ex:
        list(ex.map(one, [(c, t) for c in ("_id", "document") for t in ax["targets"]]))
    return out


def run_engine(engine: str, tier: str, work: Path, progress=lambda s: None) -> tuple[list[Result], list[str]]:
    """Seed one engine, run its grid and drop the seed objects; returns the results and the notes on what could not be graded."""
    url = STAND[engine]
    try:
        return _run_engine(engine, tier, work, url, progress)
    finally:
        cleanup(engine, url)


def cleanup(engine: str, url: str) -> None:
    """Drop the matrix's seed table or collection from the stand."""
    if engine == "mongo":
        cdc._mongosh(url, f"db.{TABLE}.drop()")
    else:
        sql(engine, url, f"DROP TABLE IF EXISTS {TABLE};")


def _run_engine(engine: str, tier: str, work: Path, url: str, progress) -> tuple[list[Result], list[str]]:
    """The seeded grid of one engine; an untrusted second reader grades nothing."""
    untrusted = reader_trusted(READER)
    if untrusted:
        return [Result(f"{engine}/*", REFUSED, NOT_GRADED, untrusted)], []
    if engine == "mongo":
        work.mkdir(parents=True, exist_ok=True)
        return run_mongo(work, url, tier), []
    cols, n = seed(engine, url)
    units = grid(engine, cols, tier)
    results: list[Result] = []
    lock = threading.Lock()
    work.mkdir(parents=True, exist_ok=True)

    def one(u):
        c, t, m, f = u
        rs = run_unit(engine, work, url, c, n, m, t, list(f))
        with lock:
            results.extend(rs)
            if len(results) % 100 < len(rs):
                progress(f"{engine}: {len(results)} cells")

    with cf.ThreadPoolExecutor(WORKERS) as ex:
        list(ex.map(one, units))
    return results, []


def pairs_of(results: list[Result]) -> list[str]:
    """The sorted source->override pairs of the cells, with the mode and format cut off."""
    return sorted({re.sub(r"/(full|incremental|chunked|time_window|cdc-[\w@]+)/\w+(/integrity)?$", "", r.cell.split("/", 1)[1]) for r in results})


def digest(results: list[Result]) -> str:
    """A short stable hash of the failing pairs, so a known-red message changes when a pair opens or closes."""
    return hashlib.sha1("\n".join(pairs_of(results)).encode()).hexdigest()[:8]


def summary(engine: str, results: list[Result]) -> dict[str, list[Result]]:
    """The results grouped by failure class; the empty key holds the cells that passed."""
    out: dict[str, list[Result]] = {k: [] for k in ("", NOT_GRADED, *FAIL_CLASSES)}
    for r in results:
        out.setdefault(r.klass, []).append(r)
    return out


def verdict_line(r: Result) -> str:
    """One `<matrix> <cell> PASS|RED|NOT_GRADED <evidence>` line."""
    state = "NOT_GRADED" if r.klass == NOT_GRADED else "RED" if r.klass else "PASS"
    evidence = " ".join(f"{r.klass or r.verdict}: {r.detail}".split()) if r.klass else f"{r.verdict} {r.code}".strip()
    return f"{SCENARIO} {r.cell.replace(' ', '_')} {state} {evidence[:300]}"


def summary_line(results: list[Result]) -> str:
    """The counts line closing a run: PASS, RED and NOT_GRADED cells."""
    red = sum(1 for r in results if r.klass and r.klass != NOT_GRADED)
    ng = sum(1 for r in results if r.klass == NOT_GRADED)
    return f"{SCENARIO} summary PASS={len(results) - red - ng} RED={red} NOT_GRADED={ng}"


def label(engine: str, tier: str = "", scenario: str = SCENARIO) -> str:
    """The name of one axis run in a ledger row: scenario, engine and, when the run has one, its tier."""
    return f"{scenario}[{engine}]" + (f" {tier}" if tier else "")


def failure_message(engine: str, klass: str, rs: list[Result], scenario: str = SCENARIO, tier: str = "") -> str:
    """The one ledger line of a failure class on one engine at one tier: a tier crosses modes and formats differently, so its pairs and its line differ."""
    return f"{label(engine, tier, scenario)}: {klass} over {len(pairs_of(rs))} source->override pairs #{digest(rs)}"


def exit_code(results: list[Result]) -> int:
    """A matrix CLI's exit: 1 when any cell is red, 2 when no cell was graded, else 0."""
    if any(r.klass and r.klass != NOT_GRADED for r in results):
        return 1
    return 0 if any(not r.klass for r in results) else 2


def record(led: Ledger, engine: str, results: list[Result], tier: str = "", *, scenario: str = SCENARIO,
           cause: SkipCause = SkipCause.ENV, target: str | None = None, what: str = "cells graded by a second reader") -> None:
    """Write one engine's results to the ledger, the one place every no_silent_null axis does it: a skip for cells not graded, a FAIL row per failure class, and a PASS row only when at least one cell was graded and none failed."""
    groups = summary(engine, results)
    graded = [r for r in results if r.klass != NOT_GRADED]
    counts = {v: sum(r.verdict == v for r in graded) for v in (DELIVERED, REFUSED, DECLARED)}
    name = label(engine, tier, scenario)
    bad = {k: v for k, v in groups.items() if k and k != NOT_GRADED and v}
    if groups[NOT_GRADED] or not results:
        why = groups[NOT_GRADED][0].detail[:200] if results else "the axis returned no cell"
        led.skipped(engine, "-", scenario, "-", f"{name}: {len(groups[NOT_GRADED])} of {len(results)} cells not graded: {why}",
                    cause=cause, target=target or STAND.get(engine, engine))
    for klass, rs in bad.items():
        led.failed(engine, "-", scenario, "-", failure_message(engine, klass, rs, scenario, tier),
                   "\n".join(f"{r.cell}: {r.detail}" for r in sorted(rs, key=lambda r: r.cell)[:60]))
    if not bad and graded:
        led.passed(engine, "-", scenario, "-", f"{name}: {len(graded)} {what}; " + ", ".join(f"{v} {n}" for v, n in counts.items()))


def run_tier(led: Ledger, tier: str) -> None:
    """Run the grid of every engine the stand has up and record it; engines it cannot reach are skipped as ENV."""
    work = Path(os.environ.get("RIVET_ORACLE_WORK") or tempfile.gettempdir()) / "no_silent_null" / tier
    for engine in (e for e in axes()["engines"] if e in STAND):
        t0 = time.time()
        try:
            results, _ = run_engine(engine, tier, work / engine, led.phase)
        except RuntimeError as e:
            led.skipped(engine, "-", SCENARIO, "-", f"{SCENARIO}[{engine}] not graded: {e}", cause=SkipCause.ENV, target=STAND[engine])
            continue
        record(led, engine, results, tier)
        led.ok(f"{engine}: {len(results)} cells in {time.time() - t0:.0f}s")
    for mode in (m for m in axes()["modes"] if m not in MODE_SHAPES and m != "cdc"):
        led.skipped("-", "-", SCENARIO, "-", f"{SCENARIO}: mode {mode} not graded (no cell shape yet)", cause=SkipCause.REDUCED)


def verify_no_silent_null(led: Ledger) -> None:
    """Per-run tier: every source/override pair once, the whole mode and format cross for one column per value family."""
    run_tier(led, "smoke")


def verify_no_silent_null_full(led: Ledger) -> None:
    """Weekly tier: every source column x override x mode x format."""
    run_tier(led, "full")


def _proc(rc: int, err: str = "") -> core.Proc:
    """A finished process for the self-test."""
    return core.Proc(["rivet"], rc, "", err, gated=False)


def self_test() -> None:
    """Grade the grader: axes come from the product, every failure class fires on its exact harm, a delivered value passes."""
    ax = axes()
    assert {"postgres", "mysql", "mssql", "oracle", "mongo"} <= set(ax["engines"]), ax["engines"]
    assert {"full", "incremental", "chunked", "cdc"} <= set(ax["modes"]) and set(ax["formats"]) == {"parquet", "csv"}, ax
    assert {"none", "float4", "float8", "int8", "bool", "decimal(38,10)", "text"} <= set(ax["targets"]), ax["targets"]
    assert {"decimal(18,-2)", "decimal(60,-2)"} <= set(ax["targets"]) and negative_scale_targets("fn parse_decimal_params let scale: u8 =") == []
    assert compare("mysql", "-2147483648", "int", "decimal(18,-2)", Decimal("-2147483600")) == WRONG, "a negative scale rounds away digits"
    assert compare("mysql", "-2147483600", "int", "decimal(18,-2)", Decimal("-2147483600")) == "OK"
    assert ZERO_DATE_LIT.fullmatch("'0000-00-00'") and ZERO_DATE_LIT.fullmatch("'2026-00-15 00:00:00'") and not ZERO_DATE_LIT.fullmatch("'2026-11-01 00:00:00'")
    assert parse_lit("mysql", "NULL") is None and parse_lit("oracle", "''") is None and parse_lit("mysql", "''") == ""
    assert parse_lit("mysql", "-5") == Decimal(-5) and parse_lit("mysql", "'a''b'") == "a'b" and parse_lit("postgres", "TRUE") is True
    assert parse_lit("mysql", "UNHEX('00')") == b"\0" and parse_lit("mysql", "JSON_OBJECT('a',1)") is ...
    assert parse_lit("mysql", "'a\\\\b'") == "a\\b" and parse_lit("mysql", "'l\\nf'") == "l\nf" and parse_lit("mysql", "'n\\0l'") == "n\0l"
    assert parse_lit("postgres", "'a\\b'") == "a\\b" and parse_lit("oracle", "TO_DATE('2024-02-29 13:14:15','YYYY-MM-DD HH24:MI:SS')") == "2024-02-29 13:14:15"
    assert parse_lit("postgres", "'\\x00ff01'::bytea") == b"\x00\xff\x01" and parse_lit("postgres", "'{\"n\":1}'::jsonb") == '{"n":1}' and parse_lit("mysql", "b'101'") == 5
    harmed = Result("c", DELIVERED, NULLED, "1 non-NULL value delivered as NULL")
    assert integrity("c", _proc(0), harmed).klass == BLIND and integrity("c", _proc(3, "[RIVET_VALIDATE_CHECKSUM]"), harmed).klass == ""
    sib = {c.native: c for c in columns("mysql")}
    assert {"TIME(1)", "TIME(3)", "TIME(5)"} <= set(sib) and "'12:34:56.5'" in sib["TIME(1)"].lits and "'-00:00:00.5'" not in sib["TIME(1)"].lits, "a refused edge is not seeded into the override grid"
    hostile = next(c for n, c in sib.items() if "\U0001F600" in n)
    assert {parse_lit("mysql", v) for v in hostile.lits} >= {"a\\b", "l\nf", "c\rr", "n\0l", "\U0001F600"}, hostile.lits
    assert "'12abc'" in next(c for c in columns("mysql") if c.native == "TEXT").lits
    assert compare("mysql", "NULL", "int", "float4", None) == "OK"
    assert compare("mysql", "7", "int", "float4", None) == NULLED, "a non-NULL value delivered as NULL is the harm of the MySQL int->float override"
    assert compare("mysql", "7", "int", "float4", None, csv=True) == NULLED and compare("mysql", "7", "int", "float4", "", csv=True) == WRONG
    assert compare("mysql", "''", "text", "none", "", csv=True) == "OK" and compare("mysql", "''", "text", "none", None, csv=True) == NULLED
    assert parse_lit("oracle", "HEXTORAW('')") is None and parse_lit("mysql", "UNHEX('')") == b"" and parse_lit("mysql", "x''") == b""
    assert compare("oracle", "HEXTORAW('')", "other", "none", None, native="RAW(16)") == "OK", "an Oracle empty RAW is NULL at the source"
    with tempfile.TemporaryDirectory() as fc:
        assert format_config(Path(fc), "parquet").parent != format_config(Path(fc), "csv").parent, "two formats of a cell share the state a run keeps beside its config"
    assert compare("mysql", "7", "int", "float4", 7.0) == "OK" and compare("mysql", "7", "int", "float4", 7.5) == WRONG
    assert compare("mysql", "16777217", "int", "float4", 16777216.0) == "OK", "float4 rounds at 2^24 and no further"
    assert compare("mysql", "16777217", "int", "float4", 16777000.0) == WRONG
    assert compare("mysql", "5", "int", "bool", True) == WRONG and compare("mysql", "1", "int", "bool", True) == "OK"
    assert compare("mysql", "5", "bool", "none", True) == "OK" and compare("mysql", "0", "bool", "none", False) == "OK"
    assert compare("mysql", "'abc'", "text", "int4", 3) == WRONG and compare("mysql", "'abc'", "text", "none", "abc") == "OK"
    assert compare("mysql", "-2147483648", "int", "decimal(9,2)", Decimal("0.00")) == WRONG
    assert compare("mysql", "'b'", "other", "none", "c", native="ENUM('a','b','c')") == WRONG and compare("mysql", "'x,z'", "other", "none", "x,z", native="SET('x','y','z')") == "OK"
    assert compare("mysql", "'\U0001F600'", "other", "none", "?", native="ENUM('a')") == WRONG, "a 4-byte label delivered as `?` is a changed value"
    assert compare("mysql", "'üñíçødé'", "text", "none", "Ã¼Ã±", native="VARCHAR(200)") == WRONG, "non-ASCII text is compared, not only seen"
    assert compare("postgres", "'2019-02-03 08:07:06.554433+05'", "other", "none", datetime(2019, 2, 3, 3, 7, 6, 554433, tzinfo=timezone.utc), native="TIMESTAMPTZ") == "OK"
    assert compare("postgres", "'2019-02-03 08:07:06.554433+05'", "other", "none", "2019-02-03 08:07:06.554433", native="TIMESTAMPTZ") == WRONG, "an offset dropped on the way"
    assert compare("mssql", "'2026-06-23 10:00:00 +05:30'", "other", "none", "2026-06-23 04:30:00+00", native="DATETIMEOFFSET") == "OK"
    assert compare("mysql", "'14:30:00.123456'", "other", "none", dtime(14, 30, 0, 123456), native="TIME(6)") == "OK"
    assert compare("mssql", "'23:59:59.9999999'", "other", "none", dtime(23, 59, 59, 999999), native="TIME(7)") == WRONG, "the 100 ns digit dropped"
    assert compare("mssql", "'2000-01-01 00:00:00.1234567'", "other", "none", "2000-01-01 00:00:00.123456700", native="DATETIME2") == "OK"
    assert compare("mssql", "'2000-01-01 00:00:00.1234567'", "other", "none", "2000-01-01 00:00:00.123456", native="DATETIME2") == WRONG
    assert compare("mysql", "'-00:00:00.5'", "other", "none", dtime(0, 0, 0, 500000), native="TIME(1)") == WRONG, "a negative time delivered positive"
    assert compare("mysql", "'-00:00:00.5'", "other", "text", "-00:00:00.5", native="TIME(1)") == "OK"
    assert compare("oracle", "TO_DATE('0001-01-01','YYYY-MM-DD')", "other", "none", datetime(1, 1, 1), native="DATE") == "OK"
    assert compare("oracle", "TO_TIMESTAMP_TZ('2024-07-01 10:00:00 Europe/Berlin','YYYY-MM-DD HH24:MI:SS TZR')", "other", "none", "2024-07-01 08:00:00+00", native="TIMESTAMP(6) WITH TIME ZONE") == "OK"
    assert compare("mssql", "'2026-01-15T13:45:30.127'", "other", "none", datetime(2026, 1, 15, 13, 45, 30, 126667), native="DATETIME") == "OK"
    assert compare("postgres", "'2024-03-15'", "other", "none", date(2024, 3, 16), native="DATE") == WRONG
    assert compare("mysql", "'12'", "text", "date", date(2012, 1, 1), native="TEXT") == WRONG, "text delivered as a date it never was"
    assert compare("postgres", '\'{"k": "v", "n": 42}\'', "other", "none", '{"n":42,"k":"v"}', native="JSONB") == "OK"
    assert compare("postgres", "'{\"n\":1}'::jsonb", "other", "none", '{"n":2}', native="JSONB") == WRONG
    assert compare("mssql", "'12345678-1234-1234-1234-123456789012'", "other", "none", UUID("12345678-1234-1234-1234-123456789012"), native="UNIQUEIDENTIFIER") == "OK"
    assert compare("mssql", "0xAB", "other", "none", b"\xab\0\0", native="BINARY(8)") == "OK" and compare("mysql", "0xDEADBEEF", "other", "none", b"\xde\xad", native="VARBINARY(4)") == WRONG
    assert pg_array_elements("'{a,\"b,c\",NULL}'", "text") == ["'a'", "'b,c'", "NULL"] and pg_array_elements("ARRAY[1, NULL]::int[]", "int") == ["1", "NULL"]
    assert pg_array_elements("'{{1,2},{3}}'", "int") is None and pg_array_elements("'{}'", "int") == []
    arr = lambda lit, got, nat="INTEGER[]": compare("postgres", lit, "other", "none", got, native=nat)
    assert compare("postgres", "'12345678-1234-1234-1234-123456789012'", "other", "none", bytes.fromhex("12345678123412341234123456789012"), native="UUID") == "OK"
    assert compare("postgres", "'12345678-1234-1234-1234-123456789012'", "other", "none", bytes.fromhex("78563412123412341234123456789012"), native="UUID") == WRONG
    assert arr("'{1,NULL,3}'", [1, None, 3]) == "OK" and arr("'{1,NULL,3}'", [1, 0, 3]) == WRONG and arr("'{1,NULL,3}'", [1, None]) == WRONG
    assert arr("'{1,2}'", [1, None]) == NULLED and arr("'{1}'", None) == NULLED and arr("'{}'", []) == "OK"
    assert arr("'{a,b}'", [b"a", b"b"], "TEXT[]") == "OK" and arr("'{2020-01-01}'", ["2020-01-01"], "DATE[]") == "OK"
    assert arr("'{1.5,NaN,-Infinity}'", [1.5, "nan", "-inf"], "REAL[]") == "OK" and arr("'{1.5,NaN}'", [1.5, 0.0], "REAL[]") == WRONG
    assert compare("oracle", "-0.000000000000000000000000000000000000001", "decimal", "float4", -1.0000002153053333e-39) == "OK", "a float4 subnormal rounds at its own step"
    assert compare("mysql", "12345.6789", "decimal", "none", "31323334352e36373839") == WRONG, "text read as a number with a 36-million exponent"
    assert compare("mysql", "12345.6789", "decimal", "binary", "31323334352e36373839", csv=True, native="DECIMAL(18,4)") == "OK", "a binary override writes the hex of the value text to CSV"
    assert compare("mysql", "'\u00fc\u00f1a'", "text", "binary", "c3bcc3b161", csv=True, native="VARCHAR(20)") == "OK" and compare("mysql", "'a'", "text", "binary", "62", csv=True, native="VARCHAR(20)") == WRONG
    assert compare("mysql", "'a'", "text", "binary", "zz", csv=True, native="VARCHAR(20)") == WRONG and compare("mysql", "0xAB", "other", "binary", "ab", csv=True, native="VARBINARY(4)") == "OK"
    assert compare("mysql", "1e2000000", "decimal", "none", "2e2000000") == WRONG and compare("mysql", "1e2000000", "decimal", "none", "1e2000000") == "OK"
    assert compare("mysql", "1.5", "float", "float8", "NaN") == WRONG and compare("mysql", "1.5", "float", "float8", "Infinity") == WRONG
    assert tolerance("int", "float4", Decimal(2 ** 24)) == Decimal(2) and tolerance("int", "int8", Decimal(5)) == 0
    syn = {"native_type": "INT", "delivery": "Int32", "sample": ["1"], "edges": ["2", {"v": "3", "x": "refuse:RIVET_X@extract"}, {"v": "4", "bigquery": "BOOL_COLLAPSE!form:true"}]}
    grp = row_cols("mysql", 0, syn)
    assert grp.lits == ("1", "2", "4") and grp.edges[2].defect == "BOOL_COLLAPSE", grp
    grp = row_cols("mysql", 1, {**syn, "refuses": "refuse:RIVET_Y@extract"})
    assert grp.lits == ("1", "2", "3", "4") and grp.refuses.code == "RIVET_Y", "a refused row keeps every edge in its one column"
    assert te.parse_verdict("TRUNC!form:1.5").defect == "TRUNC" and te.parse_verdict("refuse:C", te.LOAD).stage == te.LOAD
    for bad_verdict in ("", "exactly", "refuse:", "refuse:c@extract", "refuse:C@plan", "X1!exact"):
        try:
            te.parse_verdict(bad_verdict)
            raise SystemExit(f"verdict {bad_verdict!r} parsed")
        except ValueError:
            pass
    ok, no_check = _proc(0), _proc(0)
    assert grade_stop("c", ok, no_check) is None
    assert grade_stop("c", _proc(101, "thread panicked"), no_check).klass == CRASH
    assert grade_stop("c", _proc(1, "error: boom"), no_check).klass == UNCODED
    assert grade_stop("c", _proc(6, "[RIVET_INTERNAL_TYPE_BUILDER] no builder"), no_check).klass == UNCODED
    assert grade_stop("c", _proc(5, "[RIVET_SOURCE_VALUE_UNREPRESENTABLE] x"), no_check).klass == SPLIT, "check exit 0 where run refuses"
    assert grade_stop("c", _proc(5, "[RIVET_SOURCE_CONNECT_FAILED] listener refused"), _proc(0)).klass == NOT_GRADED, "a stand that would not connect is not a graded refusal"
    assert grade_stop("c", _proc(5, "[RIVET_SOURCE_VALUE_UNREPRESENTABLE] x"), _proc(5, "[RIVET_SOURCE_CONNECT_FAILED] x")).klass == NOT_GRADED
    probe = _proc(0, "WARN rivet::preflight] type report for 'nsn' failed: oracle: listener at host \"127.0.0.1\" port 1521 refused connection.")
    assert stand_refused(probe) and not stand_refused(_proc(0, "WARN type report for 'nsn' failed: column X has no mapping"))
    assert grade_stop("c", _proc(1, "[RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH] x"), probe).klass == NOT_GRADED, "a check whose type probe never ran is no evidence of a split"
    assert grade_stop("c", _proc(2, "[RIVET_CONFIG_COLUMN_OVERRIDE_CASE] x"), _proc(2)).klass == UNGRADED, "a refusal of the harness's own config is not a graded refusal"
    assert grade_stop("c", _proc(2, "[RIVET_STATE_RUN_IN_PROGRESS] x"), _proc(0)).klass == UNGRADED, "a lease refusal between two cells is not a graded refusal"
    seen: dict = {}
    real_run = core.run
    core.run = lambda argv, **kw: seen.update(kw["env"]) or _proc(0)
    try:
        rivet(["check"], {"RIVET_NSN_URL": "u"}, Path("."))
    finally:
        core.run = real_run
    assert seen == {**CELL_STATE, "RIVET_NSN_URL": "u"}, "a cell runs on the state beside its config, never on the gate's shared state"
    sent: list[str] = []
    real_mysql, real_psql = cdc._mysql, cdc._psql
    cdc._mysql = lambda url, text: sent.append(text) or _proc(0)
    cdc._psql = lambda url, **kw: sent.append(kw["sql"]) or _proc(0)
    try:
        for stmt in ("INSERT INTO t VALUES ('0000-00-00')", "UPDATE t SET c = '2026-00-00 00:00:00'", "ALTER TABLE t MODIFY n BIGINT", "INSERT INTO t VALUES (1)"):
            sql("mysql", "u", stmt)
        sql("postgres", "u", "ALTER TABLE t ADD n INT")
    finally:
        cdc._mysql, cdc._psql = real_mysql, real_psql
    assert [s.startswith(MYSQL_LAX) for s in sent] == [True, True, True, False, False], "a zero-date or ALTER statement on MySQL runs under an empty sql_mode, any other as it is"
    zoned: list[str] = []
    real_sql = engines.sql
    engines.sql = lambda engine, url, text: zoned.append(text) or _proc(0)
    try:
        sql("oracle", "u", "INSERT INTO t VALUES (1)")
        sql("postgres", "u", "SELECT 1")
    finally:
        engines.sql = real_sql
    assert zoned == [ORACLE_ZONE + "INSERT INTO t VALUES (1)", "SELECT 1"], "an Oracle seed runs in the fixed session zone, not the host zone the known-red keys depend on"
    assert "C00" in config("oracle", "U", "incremental", "parquet", "c00", "int8", "o") and "cursor_column: ID" in config("oracle", "U", "incremental", "parquet", "c00", "none", "o")
    refused = grade_stop("c", _proc(5, "[RIVET_SOURCE_VALUE_UNREPRESENTABLE] x"), _proc(2))
    assert refused.klass == "" and refused.verdict == REFUSED and refused.code == "RIVET_SOURCE_VALUE_UNREPRESENTABLE"
    mongo_ran = _mongo_grader_self_test()
    cols = [Col("c00", "INT", "Int32", "int", ("1", "2")), Col("c01", "TEXT", "Utf8", "text", ("'a'",)), Col("c02", "BIGINT", "Int64", "int", ("3",))]
    smoke, full = grid("mysql", cols, "smoke"), grid("mysql", cols, "full")
    pair = lambda units: {(c.name, t) for c, t, _, _ in units}
    assert pair(smoke) == pair(full) == {(c.name, t) for c in cols for t in ax["targets"]}, "every source column x override pair is in both tiers"
    modes = lambda units, name: {(m, f) for c, t, m, fs in units if c.name == name and t == "float4" for f in fs}
    assert modes(full, "c02") == {(m, f) for m in ax["modes"] if m in MODE_SHAPES for f in ax["formats"]}, "the full tier crosses every mode and format"
    assert ("incremental", "parquet") in modes(smoke, "c00") and ("chunked", "csv") not in modes(smoke, "c00"), "smoke crosses the modes for one column per family"
    assert modes(smoke, "c02") == {("full", "parquet")}, "every other column is one cell per override"
    a = [Result("e/INT->float4/full/parquet", DELIVERED, NULLED)]
    b = a + [Result("e/INT->float8/full/parquet", DELIVERED, NULLED)]
    assert failure_message("mysql", NULLED, a) != failure_message("mysql", NULLED, b), "a new pair changes the known-red message"
    wider = a + [Result("e/INT->float4/chunked/csv", DELIVERED, NULLED)]
    assert failure_message("mysql", NULLED, a) == failure_message("mysql", NULLED, wider), "the same pairs in more modes keep the message"
    led = Ledger(colour=False)
    led._buf = []
    record(led, "mysql", [Result("mysql/INT->none/full/parquet", DELIVERED)], "smoke")
    record(led, "mysql", b, "smoke")
    assert [c.status.name for c in led.cells] == ["PASS", "FAIL"], [c.status for c in led.cells]
    only_in_mode = [Result("mysql/INT->float4/chunked/csv", DELIVERED, NULLED)]
    tiers = Ledger(colour=False)
    tiers._buf = []
    record(tiers, "mysql", only_in_mode, "smoke")
    record(tiers, "mysql", only_in_mode, "full")
    keys = [ln.strip().removeprefix("✗ ") for ln in tiers._buf if "✗" in ln]
    assert len(keys) == 2 and keys[0] != keys[1], "the same failing results graded at two tiers are two ledger keys"
    assert keys[0].startswith("no_silent_null[mysql] smoke: ") and keys[1].startswith("no_silent_null[mysql] full: "), keys
    record(led, "mysql", [Result("mysql/INT->none/full/parquet", DELIVERED), Result("mysql/INT->int8/full/parquet", REFUSED, NOT_GRADED)], "smoke")
    assert [c.status.name for c in led.cells][2:] == ["SKIP", "PASS"], "a not-graded cell is a skip row beside the pass, never a silent pass"
    n0 = len(led.cells)
    record(led, "mysql", [Result("mysql/INT->int8/full/parquet", REFUSED, NOT_GRADED), Result("mysql/INT->int4/full/parquet", REFUSED, NOT_GRADED)], "smoke")
    record(led, "mysql", [], "smoke")
    assert [c.status.name for c in led.cells][n0:] == ["SKIP", "SKIP"], "an axis where no cell was graded, or none returned, never records a PASS"
    assert exit_code([Result("c", DELIVERED)]) == 0 and exit_code([Result("c", DELIVERED), Result("d", REFUSED, NOT_GRADED)]) == 0
    assert exit_code([Result("c", DELIVERED, NULLED)]) == 1 and exit_code([Result("c", REFUSED, NOT_GRADED)]) == 2 and exit_code([]) == 2, "red exits 1, nothing graded exits 2"
    mixed = [Result("m/INT UNSIGNED->none/full/parquet", DELIVERED), Result("m/INT->float4/full/parquet", DELIVERED, NULLED, "20 fields"), Result("m/X->none/full/parquet", REFUSED, NOT_GRADED, "stand")]
    lines = [verdict_line(r) for r in mixed]
    assert [ln.split(" ")[2] for ln in lines] == ["PASS", "RED", "NOT_GRADED"] and all(len(ln.split(" ", 3)) == 4 for ln in lines), lines
    assert summary_line(mixed) == "no_silent_null summary PASS=1 RED=1 NOT_GRADED=1"
    assert "m/INT_UNSIGNED->none" in lines[0], "a cell key has no space"
    assert set(rivet_types()) == set(RIVET_TYPE_FAMILY)
    saved = dict(RIVET_TYPE_FAMILY)
    del RIVET_TYPE_FAMILY["interval"]
    try:
        rivet_types()
        raise SystemExit("a RivetType variant without a sample family built the grid")
    except AssertionError:
        pass
    finally:
        RIVET_TYPE_FAMILY.update(saved)
    liar = lambda out, fmt: ({"id": "INTEGER", "c00": "INTEGER"}, {1: {"id": 1, "c00": 7}}, 1)

    def crasher(out, fmt):
        raise RuntimeError("reader crashed")

    for bad in (liar, crasher):
        assert reader_trusted(bad), f"{bad.__name__ if hasattr(bad, '__name__') else bad} reader graded the canaries as trusted"
        global READER
        saved_reader, READER = READER, bad
        try:
            rs, _ = _run_engine("mysql", "smoke", Path(tempfile.gettempdir()), "mysql://none", lambda s: None)
        finally:
            READER = saved_reader
        assert [r.klass for r in rs] == [NOT_GRADED], "an untrusted reader turns the whole engine into NOT_GRADED, never PASS"
    with tempfile.TemporaryDirectory() as t:
        (Path(t) / "p.csv").write_text("id,c00\n1,\n")
        (Path(t) / "manifest.json").write_text('{"status": "success", "parts": ["p.csv"]}')
        col1 = Col("c00", "INT", "Int32", "int", ("7",))
        assert grade_run("mysql", "x", _proc(0), _proc(0), "exact", Path(t), "csv", col1, 1, "none", ["7"], reader=crasher).klass == NOT_GRADED
        assert grade_run("mysql", "x", _proc(0), _proc(0), "exact", Path(t), "csv", col1, 1, "none", ["7"], reader=liar).klass == "", "a liar reader is caught by the canary, not by one cell"
    ran = _reader_self_test()
    print(f"self-test ok: {len(ax['targets'])} override targets, {len(ax['modes'])} modes, grader classes fire on their harm; second reader {'ran' if ran else 'NOT RUN (no pyarrow or duckdb)'}; mongo grader {'ran' if mongo_ran else 'NOT RUN (no pymongo)'}")


def _mongo_grader_self_test() -> bool:
    """Grade dropped and changed fields of the Mongo grid through BSON relaxed JSON; False when pymongo is not installed."""
    try:
        from bson import json_util
    except ImportError:
        return False
    from . import type_matrix as tm

    docs, want = tm.mongo_grid()
    wired = lambda d: json_util.dumps(d, json_options=json_util.RELAXED_JSON_OPTIONS)
    first = next(k for k in want if "double" in {c.native for c, _, _ in want[k].values()})
    name, (col, _, _) = next((x, y) for x, y in want[first].items() if y[0].native == "double")
    assert tm.mongo_grid_verdict(wired({"n": len(docs)}), len(docs)) == [], "the sparse document owes nothing"
    lost = {k: v for k, v in docs[first - 1].items() if k != name}
    assert (NULLED, True) in {(c, d.startswith("double[")) for c, d in tm.mongo_grid_verdict(wired(lost), first)}, "a field the delivered document dropped"
    changed = tm.mongo_grid_verdict(wired({**lost, name: "changed"}), first)
    assert (WRONG, True) in {(c, d.startswith("double[")) for c, d in changed}, "a field delivered as another value"
    return True


def _reader_self_test() -> bool:
    """Write real Parquet and CSV parts and grade them through the DuckDB reader; False when it is not installed."""
    try:
        import duckdb  # noqa: F401
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        return False
    col = Col("c00", "INT", "Int32", "int", ("1", "16777217", "-5"))
    with tempfile.TemporaryDirectory() as t:
        root = Path(t)

        def parts(name: str, values: list, typ) -> Path:
            d = root / name
            d.mkdir()
            pq.write_table(pa.table({"id": pa.array([1, 2, 3, 4]), "c00": pa.array(values, typ)}), d / "p.parquet")
            (d / "manifest.json").write_text('{"status": "success", "parts": ["p.parquet"]}')
            return d

        def grade(d: Path, target: str, fmt: str = "parquet") -> Result:
            return grade_run("mysql", "x", _proc(0), _proc(0), "exact", d, fmt, col, 4, target, list(col.lits))

        good = parts("good", [1.0, 16777216.0, -5.0, None], pa.float32())
        assert grade(good, "float4").klass == "", grade(good, "float4")
        nulled = parts("nulled", [None, None, None, None], pa.float32())
        assert grade(nulled, "float4").klass == NULLED, "every row NULL for non-NULL input exits 0 on main for int->float4"
        changed = parts("changed", [1.0, 16777216.0, 5.0, None], pa.float32())
        assert grade(changed, "float4").klass == WRONG
        ignored = parts("ignored", [1, 16777217, -5, None], pa.int32())
        assert grade(ignored, "float4").klass == IGNORED and grade(ignored, "none").klass == ""
        short = root / "short"
        short.mkdir()
        pq.write_table(pa.table({"id": pa.array([1]), "c00": pa.array([1], pa.int32())}), short / "p.parquet")
        (short / "manifest.json").write_text('{"status": "success", "parts": ["p.parquet"]}')
        assert grade(short, "none").klass == EMPTY
        csvd = root / "csv"
        csvd.mkdir()
        (csvd / "p.csv").write_text("id,c00\n1,1\n2,\n3,-5\n4,\n")
        (csvd / "manifest.json").write_text('{"status": "success", "parts": ["p.csv"]}')
        assert grade(csvd, "none", "csv").klass == NULLED
        for name, native, lits, body, want in (
            ("csv-empty-bytes", "VARBINARY(4)", ("0x", "0xAB"), 'id,c00\n1,""\n2,ab\n3,\n4,\n', ""),
            ("csv-null-for-empty-bytes", "VARBINARY(4)", ("0x", "0xAB"), "id,c00\n1,\n2,ab\n3,\n4,\n", NULLED),
            ("csv-null-for-empty-text", "VARCHAR(4)", ("''", "'a'"), "id,c00\n1,\n2,a\n3,\n4,\n", NULLED),
            ("csv-padded-char", "CHAR(8)", ("' '", "'a'"), 'id,c00\n1,""\n2,a\n3,\n4,\n', ""),
        ):
            d = root / name
            d.mkdir()
            (d / "p.csv").write_text(body)
            (d / "manifest.json").write_text('{"status": "success", "parts": ["p.csv"]}')
            cc = Col("c00", native, "Binary" if "BINARY" in native else "Utf8", "other" if "BINARY" in native else "text", lits)
            r = grade_run("mysql", name, _proc(0), _proc(0), "exact", d, "csv", cc, 4, "none", list(lits))
            assert r.klass == want, f"{name}: a CSV cell is NULL when bare and empty when quoted: {r}"
        stray = parts("stray", [1.0, 16777216.0, -5.0, None], pa.float32())
        pq.write_table(pa.table({"id": pa.array([9]), "c00": pa.array([1.0], pa.float32())}), stray / "extra.parquet")
        assert grade(stray, "float4").klass == PARTS, "a part on disk no manifest declares"
        gone = parts("gone", [1.0, 16777216.0, -5.0, None], pa.float32())
        (gone / "manifest.json").write_text('{"status": "success", "parts": ["p.parquet", "lost.parquet"]}')
        assert grade(gone, "float4").klass == PARTS, "a part the manifest declares and the disk lacks"
        wide = root / "wide"
        wide.mkdir()
        pq.write_table(pa.table({"id": pa.array([1, 2, 3, 4]), "c00": pa.array([1.0, 16777216.0, -5.0, None], pa.float32()), "extra": pa.array([0] * 4)}), wide / "p.parquet")
        (wide / "manifest.json").write_text('{"status": "success", "parts": ["p.parquet"]}')
        assert grade_run("mysql", "x", _proc(0), _proc(0), "exact", wide, "parquet", col, 4, "float4", list(col.lits), frozenset({"id", "c00"})).klass == COLUMNS
        assert reader_trusted(read_parts) == "", "the real reader tells the canaries apart"
        mg = root / "mongo-wide"
        mg.mkdir()
        pq.write_table(pa.table({"_id": pa.array(["a"]), "document": pa.array(["{}"]), "extra": pa.array([0])}), mg / "p.parquet")
        (mg / "manifest.json").write_text('{"status": "success", "parts": ["p.parquet"]}')
        assert grade_mongo("m", _proc(0), _proc(0), mg, "parquet", "document", "none", 1).klass == COLUMNS, "a Mongo run with an extra column"
        mp = root / "mongo-stray"
        mp.mkdir()
        pq.write_table(pa.table({"_id": pa.array(["a"]), "document": pa.array(["{}"])}), mp / "p.parquet")
        pq.write_table(pa.table({"_id": pa.array(["b"]), "document": pa.array(["{}"])}), mp / "extra.parquet")
        (mp / "manifest.json").write_text('{"status": "success", "parts": ["p.parquet"]}')
        assert grade_mongo("m", _proc(0), _proc(0), mp, "parquet", "document", "none", 1).klass == PARTS, "a Mongo part no manifest declares"
        assert grade_mongo("m", _proc(0), _proc(0), mp, "parquet", "document", "none", 1, reader=lambda o, f: 1 / 0).klass == NOT_GRADED
    return True


def main() -> int:
    """Developer entry: run one engine's grid against the stand and print the failure classes."""
    import argparse

    ap = argparse.ArgumentParser(prog="no_silent_null")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--engine", default="mysql", help="one engine or `all`")
    ap.add_argument("--tier", choices=("smoke", "full"), default="smoke")
    ap.add_argument("--work", default=str(Path(tempfile.gettempdir()) / "no_silent_null"))
    ap.add_argument("--out", default="")
    ns = ap.parse_args()
    if ns.self_test:
        self_test()
        return 0
    t0 = time.time()
    names = [e for e in axes()["engines"] if e in STAND] if ns.engine == "all" else [ns.engine]
    results: list[Result] = []
    for name in names:
        try:
            rs, _ = run_engine(name, ns.tier, Path(ns.work) / name, lambda s: print(s, flush=True))
        except RuntimeError as e:
            rs = [Result(f"{name}/*", REFUSED, NOT_GRADED, f"the stand could not seed: {e}"[:300])]
        results += rs
        groups = summary(name, rs)
        for k, g in groups.items():
            print(f"{name:9} {k or 'ok':18} {len(g)}")
        for k in FAIL_CLASSES:
            for r in sorted(groups[k], key=lambda r: r.cell)[:12]:
                print(f"  {k}: {r.cell}: {r.detail}")
    lines = [verdict_line(r) for r in sorted(results, key=lambda r: r.cell)] + [summary_line(results)]
    print("\n".join(lines))
    if ns.out:
        Path(ns.out).write_text("\n".join(json.dumps(r.__dict__) for r in results))
    print(f"{len(results)} cells in {time.time() - t0:.0f}s")
    return exit_code(results)


if __name__ == "__main__":
    raise SystemExit(main())
