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
import os
import re
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import yaml

from . import cdc, core, engines
from .core import Ledger, SkipCause

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
DELIVERED, REFUSED, DECLARED = "delivered", "refused", "declared_lossy"
HIDDEN, SHAPE = "HIDDEN_COLUMN_DELIVERED", "LEG_SHAPE_DIFFERS"
COLUMNS, PARTS = "COLUMN_SET_DIFFERS", "PART_SET_DIFFERS"
FAIL_CLASSES = (NULLED, WRONG, SPLIT, UNCODED, IGNORED, EMPTY, CRASH, UNGRADED, HIDDEN, SHAPE, COLUMNS, PARTS)
NOT_GRADED = "NOT_GRADED"
LOAD_TARGETS_NOT_GRADED = "load targets that re-type (BigQuery, ClickHouse, Snowflake) need a second client on a warehouse; no cell reads one yet"


def _enum_variants(path: str, name: str) -> list[str]:
    """The variant names of `pub enum <name>` in a product source file, snake_cased."""
    text = (ROOT / path).read_text()
    m = re.search(r"pub enum %s \{(.*?)\n\}" % name, text, re.S)
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
    return [*names, "decimal(9,2)", "decimal(18,4)", "decimal(38,10)", "decimal(60,10)"]


def rivet_types() -> list[str]:
    """The `RivetType` variants of the product; each needs a value family in RIVET_TYPE_FAMILY or the grid refuses to build."""
    found = _enum_variants("src/types/rivet_type.rs", "RivetType")
    if set(found) != set(RIVET_TYPE_FAMILY) or not set(RIVET_TYPE_FAMILY.values()) <= set(FAMILY_EDGES):
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


def family(native: str, delivery: str) -> str:
    """The value family a source column belongs to: what its values can be compared as."""
    n = native.upper()
    if re.fullmatch(r"(TINY|SMALL|MEDIUM|BIG)?INT(EGER)?( UNSIGNED)?|YEAR|NUMBER\(\d+\)", n):
        return "int"
    if re.fullmatch(r"(N|VAR|NVAR)?CHAR2?\(.*\)|N?TEXT|(NVAR|VAR)CHAR2?(\(.*\))?", n):
        return "text"
    if re.fullmatch(r"(DECIMAL|NUMERIC|NUMBER)\(\d+,\s*\d+\)|MONEY|SMALLMONEY|NUMBER|NUMERIC|DECIMAL", n):
        return "decimal"
    if delivery == "Float32":
        return "float4"
    if delivery == "Float64":
        return "float8"
    if n in ("BOOLEAN", "BIT") or delivery == "Boolean":
        return "bool"
    return "other"


def _int_range(native: str) -> tuple[int, int]:
    """The inclusive range of an integer native type."""
    n = native.upper()
    if n == "YEAR":
        return 1901, 2155
    m = re.fullmatch(r"NUMBER\((\d+)\)", n)
    if m:
        return -(10 ** int(m.group(1)) - 1), 10 ** int(m.group(1)) - 1
    bits = {"TINY": 8, "SMALL": 16, "MEDIUM": 24, "BIG": 64}.get(re.match(r"[A-Z]*?(?=INT)", n).group(0), 32) if "INT" in n else 16
    if "UNSIGNED" in n:
        return 0, 2 ** bits - 1
    return -(2 ** (bits - 1)), 2 ** (bits - 1) - 1


def _temporal(engine: str, native: str) -> tuple[str, str]:
    """The earliest and latest instant a temporal native type stores on this engine."""
    n = native.upper()
    if engine == "mysql":
        return ("1970-01-01 00:00:01", "2038-01-19 03:14:07") if n.startswith("TIMESTAMP") else ("1000-01-01 00:00:00", "9999-12-31 23:59:59")
    if engine == "mssql":
        if n == "SMALLDATETIME":
            return "1900-01-01 00:00:00", "2079-06-06 00:00:00"
        if n == "DATETIME":
            return "1753-01-01 00:00:00", "9999-12-31 23:59:59"
    return "0001-01-01 00:00:00", "9999-12-31 23:59:59"


TEMPORAL = re.compile(r"(DATE|TIMESTAMPTZ|TIMESTAMP|DATETIME2|DATETIME|SMALLDATETIME|DATETIMEOFFSET)(\(\d\))?( WITH (LOCAL )?TIME ZONE)?( NULL)?")


def _edges_int(engine: str, native: str, delivery: str) -> list[str]:
    """Integer edges inside the column's own range."""
    lo, hi = _int_range(native)
    marks = {0, 1, 127, 128, 255, 32767, 32768, 2 ** 24, 2 ** 24 + 1, 2 ** 31 - 1, 2 ** 31, 2 ** 53, 2 ** 53 + 1}
    return [str(v) for v in sorted({lo, hi, *marks, *(-m for m in marks)}) if lo <= v <= hi]


def _edges_decimal(engine: str, native: str, delivery: str) -> list[str]:
    """The widest, the narrowest and the zero value of a fixed-point column."""
    n = native.upper()
    m = re.search(r"\((\d+),\s*(\d+)\)", n)
    p, s = (int(m.group(1)), int(m.group(2))) if m else {"MONEY": (19, 4), "SMALLMONEY": (10, 4)}.get(n, (18, 4))
    digits = str(10 ** p - 1).rjust(s + 1, "0")
    top = f"{digits[:-s]}.{digits[-s:]}" if s else digits
    top = {"MONEY": "922337203685477.5807", "SMALLMONEY": "214748.3647"}.get(n, top)
    return [top, f"-{top}", "0", f"0.{'0' * (s - 1)}1" if s else "1"]


def _edges_float(engine: str, native: str, delivery: str) -> list[str]:
    """Float edges: exact binary fractions, the 2^24 and 2^53 steps and the wide exponents."""
    wide = delivery == "Float64"
    out = ["0", "1.5", "-2.25", "0.1", "16777216", "16777217", "1e30"] + (["9007199254740993", "1e300"] if wide else [])
    return [f"{v}{'d' if wide else 'f'}" for v in out] if engine == "oracle" else out


def _edges_bool(engine: str, native: str, delivery: str) -> list[str]:
    """Both truth values in the engine's spelling."""
    return ["TRUE", "FALSE"] if engine == "postgres" else ["1", "0"]


def _edges_text(engine: str, native: str, delivery: str) -> list[str]:
    """Empty, blank, numeric-looking and NULL-looking strings."""
    empty = [] if engine == "oracle" else ["''"]
    return [*empty, "'a'", "' '", "'12'", "'-5'", "'1.5'", "'NULL'", "'0'"]


def _edges_other(engine: str, native: str, delivery: str) -> list[str]:
    """Temporal and JSON edges; every other native type keeps the ledger samples alone."""
    n = native.upper()
    if TEMPORAL.fullmatch(n):
        lo, hi = _temporal(engine, native)
        cut = 10 if n == "DATE" else None
        kw = "DATE " if engine == "oracle" and n == "DATE" else "TIMESTAMP " if engine == "oracle" else ""
        return [f"{kw}'{v[:cut]}'" for v in (lo, hi, "1970-01-02 03:04:05")]
    if n in ("JSON", "JSONB"):
        cast = "::" + n.lower() if engine == "postgres" else ""
        return [f"'{{}}'{cast}", f"'null'{cast}"]
    return []


FAMILY_EDGES = {"int": _edges_int, "decimal": _edges_decimal, "float4": _edges_float, "float8": _edges_float, "bool": _edges_bool,
                "text": _edges_text, "other": _edges_other}
RIVET_TYPE_FAMILY = {
    "bool": "bool", "int16": "int", "int32": "int", "int64": "int", "u_int64": "int", "float32": "float4", "float64": "float8",
    "decimal": "decimal", "date": "other", "time": "other", "timestamp": "other", "string": "text", "text": "text",
    "binary": "other", "json": "other", "uuid": "other", "enum": "other", "interval": "other", "list": "other", "unsupported": "other",
}


def edge_literals(engine: str, native: str, delivery: str) -> list[str]:
    """Non-NULL edge values of a source column, as SQL literals of the engine; a family without an edge function is an error."""
    return FAMILY_EDGES[family(native, delivery)](engine, native, delivery)


def columns(engine: str) -> list[Col]:
    """The seed columns of one SQL engine: every ledger row, its samples and the family edges."""
    cols = []
    for i, r in enumerate(ledger_rows(engine)):
        lits = list(dict.fromkeys([*r["sample"], *edge_literals(engine, r["native_type"], r["delivery"])]))
        cols.append(Col(f"c{i:02d}", r["native_type"], r["delivery"], family(r["native_type"], r["delivery"]), tuple(lits)))
    return cols


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


def seed(engine: str, url: str) -> tuple[list[Col], int]:
    """Create the seed table on the stand; raises when the server refuses a statement."""
    cols = columns(engine)
    stmts, n = seed_sql(engine, cols)
    for s in ledger_setup(engine):
        engines.sql(engine, url, s)
    for st in stmts:
        p = engines.sql(engine, url, st + ";")
        if not p.ok:
            raise RuntimeError(f"{engine} seed refused `{st[:160]}`: exit {p.returncode}: {p.out.strip()[:300]}")
    return cols, n


def ledger_setup(engine: str) -> list[str]:
    """Engine-level DDL the ledger rows need before the table (types they name)."""
    return list(yaml.safe_load(LEDGER.read_text())["engines"][engine].get("setup", []))


_NUM = re.compile(r"[+-]?\d+(\.\d+)?([eE][+-]?\d+)?")
_STR = re.compile(r"N?'((?:[^']|'')*)'")


def parse_lit(engine: str, lit: str):
    """The Python value of a simple SQL literal: None for NULL, Decimal, bool, str; `...` when it cannot be read."""
    s = lit.strip()
    if s.upper() == "NULL" or (engine == "oracle" and s == "''"):
        return None
    if _NUM.fullmatch(s):
        return Decimal(s)
    if s.upper() in ("TRUE", "FALSE"):
        return s.upper() == "TRUE"
    m = _STR.fullmatch(s)
    return m.group(1).replace("''", "'") if m else ...


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


def _target_family(target: str) -> str:
    """The value family a override target delivers."""
    return {"float4": "float4", "float8": "float8", "bool": "bool", "text": "text"}.get(
        target, "int" if target.startswith("int") else "decimal" if target.startswith("decimal") else "other")


def compare(engine: str, src_lit: str, src_family: str, target: str, got, csv: bool = False) -> str:
    """OK, PRESENCE (not comparable), NULLED or WRONG for one delivered cell against its source literal."""
    exp = parse_lit(engine, src_lit)
    if exp is None:
        return "OK"
    if got is None or (csv and got == "" and exp != ""):
        return NULLED
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
        return "OK" if abs(g - e) <= tolerance(src_family, target, e) else WRONG
    if isinstance(exp, str):
        if src_family == "text" and tf in ("text", "other") and isinstance(got, str) and exp.isascii() and got.isascii() and target in ("none", "text"):
            return "OK" if got.rstrip() == exp.rstrip() else WRONG
        return "PRESENCE"
    return "PRESENCE"


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
    con = duckdb.connect()
    wide: dict[str, str] = {}
    if fmt == "parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        tbl = pa.concat_tables([pq.read_table(p) for p in parts], promote_options="default")
        wide = {f.name.lower(): f"DECIMAL({f.type.precision},{f.type.scale})" for f in tbl.schema if pa.types.is_decimal256(f.type)}
        for f in tbl.schema:
            if f.name.lower() in wide:
                tbl = tbl.set_column(tbl.schema.get_field_index(f.name), f.name, tbl.column(f.name).cast(pa.string()))
        con.register("nsn_parts", tbl)
        src = "nsn_parts"
    else:
        src = f"read_csv({parts!r}, header=true, all_varchar=true, union_by_name=true, nullstr='\\x00nsn')"
    desc = {r[0].lower(): wide.get(r[0].lower(), r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
    sel = ", ".join(f'CAST("{k}" AS VARCHAR) AS "{k}"' if "TIME ZONE" in t else f'"{k}"' for k, t in desc.items())
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
        p = core.run([_rivet_bin(), *args], env=env, cwd=cwd, timeout=RUN_TIMEOUT)
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
    if code.startswith("RIVET_CONFIG_") and code not in TYPE_REFUSALS:
        return Result(cell, REFUSED, UNGRADED, f"the harness's own config was refused before any value was read: exit {p.returncode} [{code}]", code)
    if not code or code.startswith("RIVET_INTERNAL"):
        return Result(cell, REFUSED, UNCODED, f"exit {p.returncode} {code or 'no code'}: {core.first_error(p.stderr)[:160]}", code)
    if check.returncode == 0:
        return Result(cell, REFUSED, SPLIT, f"check exit 0, run exit {p.returncode} [{code}]", code)
    return Result(cell, REFUSED, "", f"check exit {check.returncode} [{(CODE.search(check.stderr + check.stdout) or [None, ''])[1]}]", code)


def grade_run(engine: str, cell: str, p: core.Proc, check: core.Proc, fidelity: str, out: Path, fmt: str, col: Col,
              n_rows: int, target: str, src_lits: list[str], want: frozenset[str] = frozenset(), reader=None) -> Result:
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
    nulled, wrong = [], []
    for i in range(1, n_rows + 1):
        lit = src_lits[i - 1] if i - 1 < len(src_lits) else "NULL"
        v = compare(engine, lit, col.family, target, rows[i][key], fmt == "csv")
        if v == NULLED:
            nulled.append(lit)
        elif v == WRONG:
            wrong.append(f"{lit}->{rows[i][key]!r}")
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


def run_unit(engine: str, work: Path, url: str, col: Col, n_rows: int, mode: str, target: str, fmts: list[str]) -> list[Result]:
    """Check once, then run each format of one (column, override, mode) cell in its own directory."""
    d = work / f"{col.name}-{target.replace('(', '_').replace(')', '').replace(',', '_')}-{mode}"
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    env = {"RIVET_NSN_URL": url}
    first = d / "check.yaml"
    first.write_text(config(engine, "RIVET_NSN_URL", mode, fmts[0], col.name, target, str(d / "out-check")))
    check = rivet(["check", "-c", str(first), "--type-report", "--json"], env, d)
    fid = fidelity_of(check.stdout, col.name)
    if stand_refused(check):
        return [Result(f"{engine}/{col.native}->{target}/{mode}/{fmt}", REFUSED, NOT_GRADED, f"the stand refused the check's type probe {STAND_TRIES} times") for fmt in fmts]
    out = []
    for fmt in fmts:
        cell = f"{engine}/{col.native}->{target}/{mode}/{fmt}"
        cfg = d / f"{fmt}.yaml"
        cfg.write_text(config(engine, "RIVET_NSN_URL", mode, fmt, col.name, target, str(d / f"out-{fmt}")))
        p = rivet(["run", "-c", str(cfg)], env, d)
        if check.returncode == 0 and p.returncode != 0:
            check = recheck(check, first, env, d)
        want = frozenset(["id", col.name.lower(), *(["nsn_ts"] if mode == "time_window" else [])])
        r = grade_run(engine, cell, p, check, fid, d / f"out-{fmt}", fmt, col, n_rows, target, list(col.lits), want)
        out.append(r)
    shutil.rmtree(d, ignore_errors=True)
    return out


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


MONGO_FIELDS = {
    "i32_min": ("NumberInt(-2147483648)", Decimal(-2147483648)),
    "i32_max": ("NumberInt(2147483647)", Decimal(2147483647)),
    "i64_max": ('NumberLong("9223372036854775807")', Decimal(9223372036854775807)),
    "i64_2p53": ('NumberLong("9007199254740993")', Decimal(9007199254740993)),
    "dbl_big": ("1e300", Decimal("1e300")),
    "dbl_frac": ("0.1", Decimal("0.1")),
    "dec128": ('NumberDecimal("9999999999999999999999999999999.999")', Decimal("9999999999999999999999999999999.999")),
    "str_empty": ('""', ""),
    "str_uni": ('"\u00fc\u00f1\u00ed\u00e7\u00f8d\u00e9"', "\u00fc\u00f1\u00ed\u00e7\u00f8d\u00e9"),
    "date_min": ('ISODate("0001-01-01T00:00:00Z")', Decimal(-62135596800000)),
    "date_max": ('ISODate("9999-12-31T23:59:59Z")', Decimal(253402300799000)),
    "oid": ('ObjectId("507f1f77bcf86cd799439011")', "507f1f77bcf86cd799439011"),
    "bin": ('BinData(0, "AAEC")', "AAEC"),
    "nested": ("{a: {b: [1, 2, 3]}}", "[1, 2, 3]"),
}
MONGO_IDS = (("NumberInt(1)", "1"), ('NumberLong("9007199254740993")', "9007199254740993"), ('"doc-3"', "doc-3"),
             ('ObjectId("507f1f77bcf86cd799439012")', "507f1f77bcf86cd799439012"))


def mongo_seed(url: str) -> int:
    """Create the seed collection: one full document per id kind plus a sparse one; returns the document count."""
    docs = [", ".join([f"_id: {i}", f"n: NumberInt({k})", *(f"{f}: {e}" for f, (e, _) in MONGO_FIELDS.items()), "nul: null"])
            for k, (i, _) in enumerate(MONGO_IDS, 1)]
    docs.append(f"_id: NumberInt(5), n: NumberInt({len(MONGO_IDS) + 1}), nul: null")
    p = cdc._mongosh(url, f"db.{TABLE}.drop(); db.{TABLE}.insertMany([" + ", ".join("{" + d + "}" for d in docs) + f"]); print('docs=' + db.{TABLE}.countDocuments({{}}))")
    if not p.ok or f"docs={len(docs)}" not in p.out:
        raise RuntimeError(f"mongo seed refused: exit {p.returncode}: {p.out.strip()[:300]}")
    return len(docs)


def _unwrap(v):
    """The plain value of an extended-JSON scalar: numbers become Decimal, dates and ids their text, others their JSON text."""
    if isinstance(v, dict) and len(v) == 1:
        k, x = next(iter(v.items()))
        if k in ("$numberInt", "$numberLong", "$numberDouble", "$numberDecimal"):
            return Decimal(str(x))
        if k == "$date" and isinstance(x, dict):
            return _unwrap(x)
        if k == "$date" and isinstance(x, str):
            return Decimal(int((datetime.fromisoformat(x.replace("Z", "+00:00")) - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds() * 1000))
        if k in ("$date", "$oid", "$binary"):
            return json.dumps(x)
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return Decimal(repr(v))
    return json.dumps(v) if isinstance(v, (dict, list)) else v


def mongo_doc_verdict(doc: dict, n: int) -> list[tuple[str, str]]:
    """(class, detail) for every field of document n that the delivered JSON dropped or changed."""
    bad = []
    full = n <= len(MONGO_IDS)
    for f, (_, exp) in MONGO_FIELDS.items():
        if not full:
            break
        if f not in doc or doc[f] is None:
            bad.append((NULLED, f"{f} missing or null"))
            continue
        got = _unwrap(doc[f])
        if isinstance(exp, Decimal):
            e, g = exp, _num(got)
            tol = abs(e) * Decimal(2) ** -51 if f.startswith("dbl") else Decimal(0)
            if g is None or abs(g - e) > tol:
                bad.append((WRONG, f"{f}: {exp} -> {got!r}"))
        elif exp not in (got if isinstance(got, str) else str(got)):
            bad.append((WRONG, f"{f}: {exp!r} -> {got!r}"))
    return bad


MONGO_COLUMNS = frozenset(["_id", "document"])


def grade_mongo(cell: str, p: core.Proc, check: core.Proc, out: Path, fmt: str, column: str, target: str, n_docs: int, reader=None) -> Result:
    """Grade one Mongo cell: coded refusal, or every document delivered whole in exactly the `_id` and `document` columns and the override honoured."""
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
        bad += mongo_doc_verdict(doc, int(_unwrap(doc.get("n"))))
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
            cfg = d / f"{fmt}.yaml"
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
        engines.sql(engine, url, f"DROP TABLE IF EXISTS {TABLE};")


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
    return sorted({re.sub(r"/(full|incremental|chunked|time_window)/\w+$", "", r.cell.split("/", 1)[1]) for r in results})


def digest(results: list[Result]) -> str:
    """A short stable hash of the failing pairs, so a known-red message is the same on every tier and changes when a pair opens or closes."""
    return hashlib.sha1("\n".join(pairs_of(results)).encode()).hexdigest()[:8]


def summary(engine: str, results: list[Result]) -> dict[str, list[Result]]:
    """The results grouped by failure class; the empty key holds the cells that passed."""
    out: dict[str, list[Result]] = {k: [] for k in ("", NOT_GRADED, *FAIL_CLASSES)}
    for r in results:
        out[r.klass].append(r)
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


def failure_message(engine: str, klass: str, rs: list[Result]) -> str:
    """The one ledger line of a failure class on one engine: the same on every tier for the same failing pairs."""
    return f"{SCENARIO}[{engine}]: {klass} over {len(pairs_of(rs))} source->override pairs #{digest(rs)}"


def record(led: Ledger, engine: str, results: list[Result], tier: str) -> None:
    """Write one engine's results to the ledger: a FAIL row per failure class, or the PASS row with the counts."""
    groups = summary(engine, results)
    counts = {v: sum(r.verdict == v for r in results) for v in (DELIVERED, REFUSED, DECLARED)}
    head = f"{SCENARIO}[{engine}] {tier}: {len(results)} cells graded by a second reader; " + ", ".join(f"{v} {n}" for v, n in counts.items())
    bad = {k: v for k, v in groups.items() if k and k != NOT_GRADED and v}
    if groups[NOT_GRADED]:
        led.skipped(engine, "-", SCENARIO, "-", f"{SCENARIO}[{engine}] {tier}: {len(groups[NOT_GRADED])} of {len(results)} cells not graded: {groups[NOT_GRADED][0].detail[:200]}",
                    cause=SkipCause.ENV, target=STAND.get(engine, engine))
    for klass, rs in bad.items():
        led.failed(engine, "-", SCENARIO, "-", failure_message(engine, klass, rs),
                   "\n".join(f"{r.cell}: {r.detail}" for r in sorted(rs, key=lambda r: r.cell)[:60]))
    if not bad:
        led.passed(engine, "-", SCENARIO, "-", head)


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
    led.skipped("-", "-", SCENARIO, "-", f"{SCENARIO}: {LOAD_TARGETS_NOT_GRADED}", cause=SkipCause.REDUCED)
    for mode in (m for m in axes()["modes"] if m not in MODE_SHAPES):
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
    assert parse_lit("mysql", "NULL") is None and parse_lit("oracle", "''") is None and parse_lit("mysql", "''") == ""
    assert parse_lit("mysql", "-5") == Decimal(-5) and parse_lit("mysql", "'a''b'") == "a'b" and parse_lit("postgres", "TRUE") is True
    assert parse_lit("mysql", "UNHEX('00')") is ...
    assert compare("mysql", "NULL", "int", "float4", None) == "OK"
    assert compare("mysql", "7", "int", "float4", None) == NULLED, "a non-NULL value delivered as NULL is the harm of the MySQL int->float override"
    assert compare("mysql", "7", "int", "float4", "", csv=True) == NULLED and compare("mysql", "''", "text", "none", "", csv=True) == "OK"
    assert compare("mysql", "7", "int", "float4", 7.0) == "OK" and compare("mysql", "7", "int", "float4", 7.5) == WRONG
    assert compare("mysql", "16777217", "int", "float4", 16777216.0) == "OK", "float4 rounds at 2^24 and no further"
    assert compare("mysql", "16777217", "int", "float4", 16777000.0) == WRONG
    assert compare("mysql", "5", "int", "bool", True) == WRONG and compare("mysql", "1", "int", "bool", True) == "OK"
    assert compare("mysql", "5", "bool", "none", True) == "OK" and compare("mysql", "0", "bool", "none", False) == "OK"
    assert compare("mysql", "'abc'", "text", "int4", 3) == WRONG and compare("mysql", "'abc'", "text", "none", "abc") == "OK"
    assert compare("mysql", "-2147483648", "int", "decimal(9,2)", Decimal("0.00")) == WRONG
    assert compare("oracle", "-0.000000000000000000000000000000000000001", "decimal", "float4", -1.0000002153053333e-39) == "OK", "a float4 subnormal rounds at its own step"
    assert tolerance("int", "float4", Decimal(2 ** 24)) == Decimal(2) and tolerance("int", "int8", Decimal(5)) == 0
    assert _int_range("TINYINT") == (-128, 127) and _int_range("INT UNSIGNED") == (0, 2 ** 32 - 1) and _int_range("NUMBER(9)") == (-999999999, 999999999)
    assert "-128" in edge_literals("mysql", "TINYINT", "Int8") and "128" not in edge_literals("mysql", "TINYINT", "Int8")
    assert "99999999.99" in edge_literals("mysql", "DECIMAL(10,2)", "Decimal128(10, 2)")
    assert "214748.3647" in edge_literals("mssql", "SMALLMONEY", "Decimal128(19, 4)") and "1e300d" in edge_literals("oracle", "BINARY_DOUBLE", "Float64")
    assert "''" in edge_literals("mysql", "VARCHAR(10)", "Utf8") and "''" not in edge_literals("oracle", "VARCHAR2(10)", "Utf8")
    assert edge_literals("postgres", "TIMESTAMP[]", "List") == [] and edge_literals("postgres", "DATE", "Date32")[0] == "'0001-01-01'"
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
    assert "C00" in config("oracle", "U", "incremental", "parquet", "c00", "int8", "o") and "cursor_column: ID" in config("oracle", "U", "incremental", "parquet", "c00", "none", "o")
    refused = grade_stop("c", _proc(5, "[RIVET_SOURCE_VALUE_UNREPRESENTABLE] x"), _proc(2))
    assert refused.klass == "" and refused.verdict == REFUSED and refused.code == "RIVET_SOURCE_VALUE_UNREPRESENTABLE"
    doc = {f"f{i}": 1 for i in range(1)}
    assert not mongo_doc_verdict({**{f: _wire(e) for f, (_, e) in MONGO_FIELDS.items()}, "nul": None, "n": 1}, 1)
    dropped = {**{f: _wire(e) for f, (_, e) in MONGO_FIELDS.items()}, "nul": None}
    dropped["i64_max"] = None
    assert [k for k, _ in mongo_doc_verdict(dropped, 1)] == [NULLED]
    dropped["i64_max"] = {"$numberLong": "9223372036854775806"}
    assert [k for k, _ in mongo_doc_verdict(dropped, 1)] == [WRONG] and not mongo_doc_verdict({"nul": None}, 5) and doc
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
    record(led, "mysql", [Result("mysql/INT->none/full/parquet", DELIVERED), Result("mysql/INT->int8/full/parquet", REFUSED, NOT_GRADED)], "smoke")
    assert [c.status.name for c in led.cells][2:] == ["SKIP", "PASS"], "a not-graded cell is a skip row beside the pass, never a silent pass"
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
    print(f"self-test ok: {len(ax['targets'])} override targets, {len(ax['modes'])} modes, grader classes fire on their harm; second reader {'ran' if ran else 'NOT RUN (no pyarrow or duckdb)'}")


def _wire(exp) -> object:
    """The extended-JSON shape of one expected seed value."""
    if isinstance(exp, Decimal):
        return {"$numberLong": str(exp)} if exp == exp.to_integral_value() and abs(exp) < 2 ** 63 else {"$numberDecimal": str(exp)}
    return exp


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
    results.append(Result("load-targets/*", REFUSED, NOT_GRADED, LOAD_TARGETS_NOT_GRADED))
    lines = [verdict_line(r) for r in sorted(results, key=lambda r: r.cell)] + [summary_line(results)]
    print("\n".join(lines))
    if ns.out:
        Path(ns.out).write_text("\n".join(json.dumps(r.__dict__) for r in results))
    print(f"{len(results)} cells in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
