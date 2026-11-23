"""Keyset skew: a parallel keyset export over a dense block and a far sparse tail is recut by density.

The key-space cuts of an even split put the whole dense block in one range. The density recut
(`seek+recut` in the run log) moves the cuts so no range holds more than twice the mean rows.
Graded from outside rivet: DuckDB over the DECLARED parts against the source's own count.
"""

from __future__ import annotations

import re
import shutil

try:
    from .core import Ledger
    from .engines import sql
    from .scenarios import (
        Scope, _declared_read, _duckdb_list, _export_local, _failed, _passed, _source_count_distinct,
    )
except ImportError:  # pragma: no cover - depends on how the driver is invoked
    from core import Ledger  # type: ignore
    from engines import sql  # type: ignore
    from scenarios import (  # type: ignore
        Scope, _declared_read, _duckdb_list, _export_local, _failed, _passed, _source_count_distinct,
    )

__all__ = ["sc_keyset_skew"]

SCEN = "keyset_skew"
DENSE = 180_000
SPARSE = 20_000
ROWS = DENSE + SPARSE
PARALLEL = 4
CHUNK = 1000
TABLE = {"postgres": "public.keyset_skew", "mysql": "keyset_skew", "mssql": "dbo.keyset_skew", "oracle": "keyset_skew"}


def _fixture(engine: str) -> list[str]:
    """The statements that build the skewed table on `engine`: ids 1..DENSE, then SPARSE ids 500000 apart."""
    t = TABLE[engine]
    if engine == "postgres":
        return [
            f"DROP TABLE IF EXISTS {t}",
            f"CREATE TABLE {t} (id BIGINT PRIMARY KEY, payload INT NOT NULL)",
            f"INSERT INTO {t} SELECT g, g FROM generate_series(1, {DENSE}) g",
            f"INSERT INTO {t} SELECT {DENSE} + g::bigint * 500000, g FROM generate_series(1, {SPARSE}) g",
            f"ANALYZE {t}",
        ]
    if engine == "mysql":
        return [
            f"DROP TABLE IF EXISTS {t}",
            f"CREATE TABLE {t} (id BIGINT PRIMARY KEY, payload INT NOT NULL)",
            "SET SESSION cte_max_recursion_depth = 300000",
            f"INSERT INTO {t} WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n < {DENSE}) SELECT n, n FROM s",
            f"INSERT INTO {t} WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n < {SPARSE}) SELECT {DENSE} + n * 500000, n FROM s",
            f"ANALYZE TABLE {t}",
        ]
    if engine == "mssql":
        return [
            f"IF OBJECT_ID('{t}') IS NOT NULL DROP TABLE {t}",
            f"CREATE TABLE {t} (id BIGINT PRIMARY KEY, payload INT NOT NULL)",
            f"INSERT INTO {t} SELECT value, value FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST({DENSE} AS BIGINT))",
            f"INSERT INTO {t} SELECT {DENSE} + value * 500000, value FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST({SPARSE} AS BIGINT))",
        ]
    return [
        f"CREATE TABLE {t} (id NUMBER(19) PRIMARY KEY, payload NUMBER(10) NOT NULL)",
        f"INSERT INTO {t} SELECT LEVEL, LEVEL FROM dual CONNECT BY LEVEL <= {DENSE}",
        f"INSERT INTO {t} SELECT {DENSE} + LEVEL * 500000, LEVEL FROM dual CONNECT BY LEVEL <= {SPARSE}",
    ]


def _drop(engine: str, url: str) -> None:
    """Drop the skewed table, ignoring a table that is absent."""
    t = TABLE[engine]
    stmt = {
        "mssql": f"IF OBJECT_ID('{t}') IS NOT NULL DROP TABLE {t}",
        "oracle": f"DROP TABLE {t} PURGE",
    }.get(engine, f"DROP TABLE IF EXISTS {t}")
    sql(engine, url, stmt)


def _seed(engine: str, url: str) -> str:
    """Build the fixture; the failing statement's output, or "" when it was built."""
    _drop(engine, url)
    p = sql(engine, url, ";\n".join(_fixture(engine)) + ";")
    return "" if p.ok else (p.stderr or p.stdout).strip()[-200:]


def _spread(declared: str) -> dict[int, int]:
    """Rows per `_pk_w<range>_` part group, read by DuckDB over the declared parts."""
    out = _duckdb_list(
        "SELECT r || ' ' || n FROM (SELECT regexp_extract(filename, '_pk_w([0-9]+)_', 1) r, count(*) n "
        f"FROM read_parquet({declared}, filename = true) GROUP BY 1)"
    )
    return {int(a): int(b) for a, b in (ln.split() for ln in out.splitlines() if re.fullmatch(r"\d+ \d+", ln.strip()))}


def sc_keyset_skew(led: Ledger, engine: str, tag: str, url: str) -> None:
    """A skewed table under `parallel: 4` is delivered once, recut by density, and no range holds over twice the mean."""
    if engine == "mongo":
        return
    err = _seed(engine, url)
    if err:
        _failed(led, engine, tag, SCEN, "-", f"keyset_skew[{engine}]: fixture not built: {err}", "fixture")
        return
    try:
        dest = Scope(engine, tag).dir("ks") / "skew"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.rmtree(dest, ignore_errors=True)
        run = _export_local(engine, url, TABLE[engine], dest, "chunked", "parquet", PARALLEL, CHUNK, log=True)
        if not run.ok:
            _failed(led, engine, tag, SCEN, "-", f"keyset_skew[{engine}]: export failed", "export")
            return
        fails = ""
        declared = _declared_read(dest, ".parquet")
        source = _source_count_distinct(engine, url, TABLE[engine], "id")
        spread: dict[int, int] = {}
        if declared is None:
            fails += "nothing-declared "
        else:
            got = _duckdb_list(f"SELECT count(*)||' '||count(DISTINCT id) FROM read_parquet({declared})")
            if got != source or source != f"{ROWS} {ROWS}":
                fails += f"loss/dup source[{source}] declared[{got}] want[{ROWS} {ROWS}] "
            spread = _spread(declared)
        if "parallel keyset path: seek+recut (" not in run.out or "not balanced" in run.out:
            fails += "log-not-balanced-seek+recut "
        if len(spread) < 2:
            fails += f"fan-out ranges={len(spread)} "
        elif max(spread.values()) * len(spread) > 2 * sum(spread.values()):
            fails += f"balance heaviest={max(spread.values())} of {sum(spread.values())} in {len(spread)} ranges "
        if fails:
            _failed(led, engine, tag, SCEN, "-", f"keyset_skew: {fails}", fails)
        else:
            _passed(led, engine, tag, SCEN, "-",
                    f"keyset_skew (every row once, seek+recut, {len(spread)} ranges, heaviest {max(spread.values())} of {ROWS})",
                    f"{len(spread)} ranges")
    finally:
        _drop(engine, url)
