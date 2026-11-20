"""Throughput of each engine's batch export (driver read, Arrow build, Parquet write) on one wide table of the common types.

The baseline the converter refactor (ADR-0041) is held against. One table a
SQL engine, N rows × the ten types every engine has:
- int32, int64
- decimal(18,4)
- float64
- text
- timestamp, date, time of day
- bool
- binary

Exported with `rivet run` to local Parquet: one warm-up, then R measured
runs. The minimum of wall and of CPU (user + sys of the child) is kept,
since other activity only ever adds.

  --cdc            also time the CDC `build_column` per type (an ignored release lib test,
                   `cdc_build_column_throughput`), gated on rows/s by RIVET_BENCH_CDC_TOL
                   (default 0.90 of the baseline)
  --record FILE    write the measurement as the baseline
  --compare FILE   fail when an engine's best CPU exceeds the baseline's by more than
                   RIVET_BENCH_CPU_TOL (default 1.10) or its best wall by more than
                   RIVET_BENCH_WALL_TOL (default 1.20)

CPU is the gate: the refactor moves work inside the rivet process, and wall
also carries the server's share. Run both sides on the same stand, the same
binary profile (release) and the same N.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from . import engines
from .property_no_silent_null import STAND

TABLE = "bench_conv"

DDL = {
    "postgres": f"""DROP TABLE IF EXISTS {TABLE};
CREATE TABLE {TABLE} (id INT PRIMARY KEY, i64 BIGINT, dec NUMERIC(18,4), f64 DOUBLE PRECISION, txt VARCHAR(64),
  ts TIMESTAMP, d DATE, t TIME, b BOOLEAN, bin BYTEA);
INSERT INTO {TABLE} SELECT g, g::bigint * 1000, ((g % 100000) / 7.0)::numeric(18,4), g * 1.5, md5(g::text),
  TIMESTAMP '2020-01-01' + g * INTERVAL '1 second', DATE '2020-01-01' + (g % 3000), TIME '00:00' + (g % 86400) * INTERVAL '1 second',
  g % 2 = 0, decode(md5(g::text), 'hex') FROM generate_series(1, {{n}}) g;""",
    "mysql": f"""DROP TABLE IF EXISTS {TABLE};
CREATE TABLE {TABLE} (id INT PRIMARY KEY, i64 BIGINT, `dec` DECIMAL(18,4), f64 DOUBLE, txt VARCHAR(64),
  ts DATETIME(6), d DATE, t TIME(6), b TINYINT(1), bin VARBINARY(16));
SET SESSION cte_max_recursion_depth = {{n1}};
INSERT INTO {TABLE} WITH RECURSIVE s(g) AS (SELECT 1 UNION ALL SELECT g + 1 FROM s WHERE g < {{n}})
  SELECT g, g * 1000, (g % 100000) / 7, g * 1.5, MD5(g), TIMESTAMP('2020-01-01') + INTERVAL g SECOND,
  DATE('2020-01-01') + INTERVAL (g % 3000) DAY, SEC_TO_TIME(g % 86400), g % 2, UNHEX(MD5(g)) FROM s;""",
    "mssql": f"""IF OBJECT_ID('dbo.{TABLE}') IS NOT NULL DROP TABLE dbo.{TABLE};
CREATE TABLE dbo.{TABLE} (id INT PRIMARY KEY, i64 BIGINT, dec DECIMAL(18,4), f64 FLOAT, txt VARCHAR(64),
  ts DATETIME2(6), d DATE, t TIME(6), b BIT, bin VARBINARY(16));
INSERT INTO dbo.{TABLE} SELECT g, CAST(g AS BIGINT) * 1000, CAST((g % 100000) / 7.0 AS DECIMAL(18,4)), g * 1.5,
  CONVERT(VARCHAR(64), HASHBYTES('MD5', CAST(g AS VARCHAR(12))), 2), DATEADD(SECOND, g, CAST('2020-01-01' AS DATETIME2(6))),
  DATEADD(DAY, g % 3000, CAST('2020-01-01' AS DATE)), CAST(DATEADD(SECOND, g % 86400, CAST('00:00' AS TIME(6))) AS TIME(6)),
  g % 2, HASHBYTES('MD5', CAST(g AS VARCHAR(12)))
  FROM (SELECT TOP ({{n}}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS g FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c) s;""",
    "oracle": f"""CREATE TABLE {TABLE} (id NUMBER(9) PRIMARY KEY, i64 NUMBER(18), dec NUMBER(18,4), f64 BINARY_DOUBLE, txt VARCHAR2(64),
  ts TIMESTAMP(6), d DATE, t INTERVAL DAY(0) TO SECOND(6), b BOOLEAN, bin RAW(16));
INSERT INTO {TABLE} SELECT level, level * 1000, MOD(level, 100000) / 7, level * 1.5, RAWTOHEX(STANDARD_HASH(TO_CHAR(level), 'MD5')),
  TIMESTAMP '2020-01-01 00:00:00' + NUMTODSINTERVAL(level, 'SECOND'), DATE '2020-01-01' + MOD(level, 3000),
  NUMTODSINTERVAL(MOD(level, 86400), 'SECOND'), MOD(level, 2) = 0, STANDARD_HASH(TO_CHAR(level), 'MD5')
  FROM dual CONNECT BY level <= {{n}}""",
}
QUERY = {"postgres": f"SELECT * FROM {TABLE}", "mysql": f"SELECT * FROM {TABLE}",
         "mssql": f"SELECT * FROM dbo.{TABLE}", "oracle": f"SELECT * FROM {TABLE.upper()}"}


def seed(engine: str, url: str, n: int) -> None:
    """Create and fill the table; raises when the server refuses."""
    if engine == "oracle":
        engines.sql(engine, url, f"DROP TABLE {TABLE} PURGE")  # absent on the first run: its refusal is the expected one
    p = engines.sql(engine, url, DDL[engine].format(n=n, n1=n + 1))
    if not p.ok:
        raise RuntimeError(f"{engine} seed refused: {p.out.strip()[:300]}")


def one_run(rivet: str, engine: str, url: str, work: Path) -> tuple[float, float]:
    """(wall, cpu) seconds of one `rivet run` of the whole table to local Parquet."""
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    (work / "c.yaml").write_text(json.dumps({
        "source": {"type": engine, "url_env": "RIVET_BENCH_URL"},
        "exports": [{"name": "bench", "query": QUERY[engine], "mode": "full", "format": "parquet",
                     "destination": {"type": "local", "path": str(work / "out")}}]}))
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    t = time.perf_counter()
    p = subprocess.run([rivet, "run", "-c", str(work / "c.yaml")], cwd=work, capture_output=True, text=True,
                       env={**os.environ, "RIVET_BENCH_URL": url})
    wall = time.perf_counter() - t
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    if p.returncode != 0:
        raise RuntimeError(f"{engine}: rivet run exit {p.returncode}: {(p.stderr or p.stdout).strip()[-300:]}")
    return wall, (after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)


def measure(rivet: str, engine: str, url: str, n: int, runs: int) -> dict:
    """Seed, warm up, then the best of `runs`."""
    seed(engine, url, n)
    work = Path(tempfile.gettempdir()) / f"bench_builders_{engine}"
    one_run(rivet, engine, url, work)
    walls, cpus = zip(*(one_run(rivet, engine, url, work) for _ in range(runs)))
    return {"rows": n, "wall_s": round(min(walls), 3), "cpu_s": round(min(cpus), 3),
            "rows_per_cpu_s": int(n / max(min(cpus), 1e-9)), "walls": [round(w, 3) for w in walls], "cpus": [round(c, 3) for c in cpus]}


def measure_cdc(repo: Path) -> dict:
    """{type: rows/s} of `build_column`, best of the test's rounds, in a release build."""
    p = subprocess.run(["cargo", "test", "--release", "--lib", "cdc_build_column_throughput", "--", "--ignored", "--nocapture"],
                       cwd=repo, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"cdc bench exit {p.returncode}: {(p.stderr or p.stdout).strip()[-300:]}")
    return parse_cdc(p.stdout)


def parse_cdc(text: str) -> dict:
    """`cdc_build_column <type> <rows> <rows_per_s>` lines to {type: rows_per_s}."""
    return {f[1]: int(f[3]) for f in (line.split() for line in text.splitlines()) if len(f) == 4 and f[0] == "cdc_build_column"}


def compare(base: dict, now: dict) -> list[str]:
    """Every engine whose best CPU or wall regressed past its tolerance."""
    cpu_tol = float(os.environ.get("RIVET_BENCH_CPU_TOL", "1.10"))
    wall_tol = float(os.environ.get("RIVET_BENCH_WALL_TOL", "1.20"))
    bad = []
    for e, m in now["engines"].items():
        b = base["engines"].get(e)
        if b is None or b["rows"] != m["rows"]:
            bad.append(f"{e}: no comparable baseline (rows {b and b['rows']} vs {m['rows']})")
            continue
        if m["cpu_s"] > b["cpu_s"] * cpu_tol:
            bad.append(f"{e}: cpu {m['cpu_s']}s > baseline {b['cpu_s']}s x {cpu_tol}")
        if m["wall_s"] > b["wall_s"] * wall_tol:
            bad.append(f"{e}: wall {m['wall_s']}s > baseline {b['wall_s']}s x {wall_tol}")
    cdc_tol = float(os.environ.get("RIVET_BENCH_CDC_TOL", "0.90"))
    for t, r in now.get("cdc", {}).items():
        b = base.get("cdc", {}).get(t)
        if b is None:
            bad.append(f"cdc {t}: no baseline")
        elif r < b * cdc_tol:
            bad.append(f"cdc {t}: {r} rows/s < baseline {b} x {cdc_tol}")
    return bad


def self_test() -> None:
    """The comparison flags a regression past each tolerance and nothing inside it."""
    base = {"engines": {"pg": {"rows": 10, "cpu_s": 1.0, "wall_s": 2.0}}}
    assert compare(base, {"engines": {"pg": {"rows": 10, "cpu_s": 1.05, "wall_s": 2.3}}}) == []
    assert compare(base, {"engines": {"pg": {"rows": 10, "cpu_s": 1.2, "wall_s": 2.0}}})[0].startswith("pg: cpu")
    assert compare(base, {"engines": {"pg": {"rows": 10, "cpu_s": 1.0, "wall_s": 2.5}}})[0].startswith("pg: wall")
    assert "no comparable baseline" in compare(base, {"engines": {"pg": {"rows": 11, "cpu_s": 1.0, "wall_s": 2.0}}})[0]
    assert parse_cdc("noise\ncdc_build_column int32 200000 9000000\n") == {"int32": 9000000}
    assert compare({"engines": {}, "cdc": {"int32": 100}}, {"engines": {}, "cdc": {"int32": 95}}) == []
    assert compare({"engines": {}, "cdc": {"int32": 100}}, {"engines": {}, "cdc": {"int32": 80}})[0].startswith("cdc int32")
    assert set(DDL) == set(QUERY) == {"postgres", "mysql", "mssql", "oracle"}
    print("bench_builders self-test ok")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--engines", default="postgres,mysql,mssql,oracle")
    ap.add_argument("--rows", type=int, default=500_000)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--rivet", default=str(Path(__file__).resolve().parents[2] / "target" / "release" / "rivet"))
    ap.add_argument("--cdc", action="store_true")
    ap.add_argument("--record", default="")
    ap.add_argument("--compare", default="")
    a = ap.parse_args()
    if a.self_test:
        self_test()
        return 0
    now = {"rivet": a.rivet, "engines": {}}
    for e in filter(None, a.engines.split(",")):
        now["engines"][e] = measure(a.rivet, e, STAND[e], a.rows, a.runs)
        m = now["engines"][e]
        print(f"bench_builders {e}: {m['rows']} rows, best wall {m['wall_s']}s, best cpu {m['cpu_s']}s, {m['rows_per_cpu_s']} rows/cpu-s", flush=True)
    if a.cdc:
        now["cdc"] = measure_cdc(Path(__file__).resolve().parents[2])
        for t, r in now["cdc"].items():
            print(f"bench_builders cdc {t}: {r} rows/s", flush=True)
    if a.record:
        Path(a.record).write_text(json.dumps(now, indent=2) + "\n")
    if a.compare:
        bad = compare(json.loads(Path(a.compare).read_text()), now)
        for b in bad:
            print(f"bench_builders REGRESSION {b}")
        return 1 if bad else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
