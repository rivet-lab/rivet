"""Read-back of non-ASCII seed literals: the bytes the server holds, against the literal in the seed file.

The probe rides the seed's own client invocation (same session, same charset), so a client that
re-encodes the stream is seen. Expected bytes come from the seed text, never from the loader's exit.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MARK = "RIVET_SEED_PROBE"
CANARY = "café 日本語 \U0001f680"
MAX_SEED_LITERALS = 3
MAX_LITERAL_CHARS = 60
SQL_ENGINES = ("postgres", "mysql", "mssql", "oracle")
CHARSET_FLAG = "--default-character-set=utf8mb4"

_LITERAL = re.compile(r"N?'([^'\\\r\n&]{1,%d})'" % MAX_LITERAL_CHARS)


def literals(seed_text: str) -> list[str]:
    """The canary plus the first non-ASCII string literals of the seed text, comments skipped."""
    code = "\n".join(ln for ln in seed_text.splitlines() if not ln.lstrip().startswith("--"))
    found: list[str] = [CANARY]
    for m in _LITERAL.finditer(code):
        s = m.group(1)
        if any(ord(c) > 127 for c in s) and s not in found and len(found) <= MAX_SEED_LITERALS:
            found.append(s)
    return found


def statements(engine: str, lits: list[str]) -> str:
    """SQL appended to the seed stream: one SELECT per literal returning the hex of the bytes the server holds."""
    if engine not in SQL_ENGINES:
        raise ValueError(f"no seed read-back for engine {engine!r}")
    out = ["\n"]
    if engine == "oracle":
        out.append("SET LINESIZE 2000 PAGESIZE 0 FEEDBACK OFF DEFINE OFF\n")
    for i, lit in enumerate(lits):
        tag = f"{MARK}:{i}:"
        if engine == "postgres":
            out.append(f"SELECT '{tag}' || encode(convert_to('{lit}', 'UTF8'), 'hex');\n")
        elif engine == "mysql":
            out.append(f"SELECT CONCAT('{tag}', HEX(CONVERT('{lit}' USING utf8mb4))) AS seed_probe;\n")
        elif engine == "mssql":
            out.append(f"SELECT '{tag}' + CONVERT(varchar(max), CONVERT(varbinary(max), N'{lit}'), 2);\nGO\n")
        else:
            out.append(f"SELECT '{tag}' || RAWTOHEX(UTL_RAW.CAST_TO_RAW('{lit}')) FROM dual;\n")
    return "".join(out)


def expected_hex(engine: str, lit: str) -> str:
    """The hex of `lit` as the engine must hold it: UTF-16LE for SQL Server's N'' literal, UTF-8 elsewhere."""
    return lit.encode("utf-16-le" if engine == "mssql" else "utf-8").hex()


def verify(engine: str, lits: list[str], out: str) -> str:
    """"" when every literal was read back byte-identical; else the mismatches (one seed error)."""
    bad: list[str] = []
    for i, lit in enumerate(lits):
        m = re.search(rf"{MARK}:{i}:([0-9A-Fa-f]+)", out)
        want = expected_hex(engine, lit)
        if m is None:
            bad.append(f"seed literal #{i} {lit!r} was not read back")
        elif m.group(1).lower() != want:
            bad.append(f"seed literal #{i} {lit!r} held as {m.group(1).lower()}, the seed text is {want} (client charset)")
    return "; ".join(bad)


def hand_spelled_mysql_clients(source: str) -> list[int]:
    """Lines of a `mysql -u…` argv written without the utf8mb4 flag (the harness builds it with `mysql_client`)."""
    lines: list[int] = []
    for node in ast.walk(ast.parse(source)):
        elts = node.elts if isinstance(node, (ast.List, ast.Tuple)) else node.args if isinstance(node, ast.Call) else []
        consts = [e.value for e in elts if isinstance(e, ast.Constant) and isinstance(e.value, str)]
        if "mysql" in consts and "mysqladmin" not in consts and any(c.startswith("-u") for c in consts) and CHARSET_FLAG not in consts:
            lines.append(node.lineno)
    return lines


def self_test() -> None:
    """The probe grades mojibake as a seed error on every SQL engine; no `mysql -u` argv is left without the flag."""
    seed = "-- 'ignored é' comment\nINSERT INTO t VALUES (1, 'unicode: 日本語 \U0001f680'), (2, 'plain'), (3, N'héllo');\n"
    lits = literals(seed)
    assert lits == [CANARY, "unicode: 日本語 \U0001f680", "héllo"], lits
    assert literals("INSERT INTO t VALUES (1, 'plain');") == [CANARY]
    for engine in SQL_ENGINES:
        sql = statements(engine, lits)
        assert sql.count(MARK) == len(lits) and CANARY in sql, engine
        good = "".join(f"{MARK}:{i}:{expected_hex(engine, lit).upper()}\n" for i, lit in enumerate(lits))
        assert verify(engine, lits, good) == "", engine
        mojibake = good.replace(f"{MARK}:1:", f"{MARK}:1:C3").replace(f"{MARK}:0:", f"{MARK}:0:00")
        err = verify(engine, lits, mojibake)
        assert "#0" in err and "#1" in err and "#2" not in err, (engine, err)
        assert verify(engine, lits, "").count("not read back") == len(lits), engine
    assert expected_hex("mysql", "日") == "e697a5" and expected_hex("mssql", "日") == "e565"
    try:
        statements("mongo", lits)
    except ValueError:
        pass
    else:
        raise AssertionError("an engine without a SQL read-back must be refused, not skipped")
    assert hand_spelled_mysql_clients('x = ["docker", "exec", c, "mysql", "-urivet", "-privet", "rivet"]') == [1]
    assert hand_spelled_mysql_clients('f(c, "mysql", "-uroot", "-privet", "--default-character-set=utf8mb4")') == []
    assert hand_spelled_mysql_clients('f(c, "mysqladmin", "ping", "-uroot")') == []
    assert hand_spelled_mysql_clients('f("mysql", "mysqladmin", "ping", "-uroot")') == []
    sources = sorted(p for p in (ROOT / "dev").rglob("*.py") if ".venv" not in p.parts)
    assert len(sources) > 100, len(sources)
    loose = [f"{p.relative_to(ROOT)}:{n}" for p in sources for n in hand_spelled_mysql_clients(p.read_text(encoding="utf-8"))]
    assert not loose, f"a mysql client argv without {CHARSET_FLAG}: {loose}"
    print(f"self-test ok: seed literals are read back as bytes on {len(SQL_ENGINES)} SQL engines and mojibake is a seed error; "
          f"{len(sources)} harness modules spell no mysql client without the utf8mb4 flag")
