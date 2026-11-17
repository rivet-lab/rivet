"""SQL on a stand engine, addressed by (engine, url): the one place the harness picks a client.

The clients themselves are cdc.py's shims (`_psql` / `_mysql` / `_sqlcmd` / `_mongosh`), which
parse credentials from the URL and route to the container that serves it.
"""

from __future__ import annotations

import re

from .core import Proc


def sql(engine: str, url: str, statement: str) -> Proc:
    """Run `statement` on the SQL engine behind `url`."""
    from .cdc import _mysql, _psql, _sqlcmd

    if engine == "oracle":
        return _oracle(url, statement)
    if engine == "postgres":
        return _psql(url, sql=statement)
    if engine == "mysql":
        return _mysql(url, statement)
    return _sqlcmd(url, q=statement)


def rows(engine: str, url: str, table: str) -> int | None:
    """`table`'s row count, read by the engine's own client; None when it could not be read."""
    from .cdc import _mongosh, _mysql, _psql, _sqlcmd

    if engine == "postgres":
        p = _psql(url, "-Atc", f"SELECT count(*) FROM {table}")
    elif engine == "mysql":
        p = _mysql(url, f"SELECT COUNT(*) FROM {table};")
    elif engine == "mssql":
        p = _sqlcmd(url, q=f"SET NOCOUNT ON; SELECT COUNT(*) FROM dbo.{table}")
    else:
        p = _mongosh(url, f"db.{table}.countDocuments({{}})")
    nums = re.findall(r"\d+", p.stdout or "") if p.ok else []
    return int(nums[-1]) if nums else None


def split_statements(text: str) -> list[str]:
    """`text` cut at every `;` outside a single-quoted literal, each part stripped, empty parts dropped."""
    out, cur, quote = [], "", False
    for c in text:
        quote ^= c == "'"
        if c == ";" and not quote:
            out.append(cur)
            cur = ""
        else:
            cur += c
    return [s for s in (x.strip() for x in [*out, cur]) if s]


def _oracle(url: str, statement: str) -> Proc:
    """`;`-separated plain SQL statements through python-oracledb (no PL/SQL blocks), committed together."""
    from urllib.parse import unquote, urlparse

    import oracledb

    u = urlparse(url)
    try:
        with oracledb.connect(user=unquote(u.username or ""), password=unquote(u.password or ""),
                              dsn=f"{u.hostname}:{u.port or 1521}/{u.path.lstrip('/')}") as con:
            cur = con.cursor()
            for s in split_statements(statement):
                cur.execute(s)
            con.commit()
        return Proc(("oracledb",), 0, "", "")
    except oracledb.DatabaseError as e:
        return Proc(("oracledb",), 1, "", str(e))
