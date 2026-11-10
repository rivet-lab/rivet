"""No silent NULL through `rivet load`: every non-NULL source value staged as Parquet reads back non-NULL and equal from the warehouse."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
import urllib.error
import urllib.request
from base64 import b64encode
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import yaml

from . import core, engines
from . import property_no_silent_null as nsn
from .core import Ledger, SkipCause
from .property_no_silent_null import DELIVERED, EMPTY, NOT_GRADED, NULLED, REFUSED, WRONG, Col, Result

SCENARIO = "no_silent_null_load"
TABLE = "pm_no_silent_null_l"
DB = "pm_no_silent_null_{engine}"
CH_URL = "http://127.0.0.1:8123"
CH_USER, CH_PASSWORD, CH_PASSWORD_ENV = "rivet", "rivet", "RIVET_TEST_CH_PASSWORD"
GCS = "http://127.0.0.1:4443"
BUCKET = "pm-no-silent-null"
NOT_REACHABLE = {
    "bigquery": "no BigQuery project is granted to this matrix's environment (BigQuery is not in CI); the loader's types are graded offline by bq_rest::stand",
    "snowflake": "no Snowflake connection is granted to this matrix's environment; a billed account is needed for a second-client read-back",
}
WORKERS = 6
MONGO_NOT_GRADED = "MongoDB stages the `document` blob as one text column; the batch grid grades its value"


def load_targets() -> list[str]:
    """The warehouses `rivet load` writes, from `LoadTarget`; each needs a stand or a NOT_GRADED reason."""
    found = nsn._enum_variants("src/config/load.rs", "LoadTarget")
    if set(found) - set(NOT_REACHABLE) != {"clickhouse"}:
        raise AssertionError(f"LoadTarget variants {found}: a target has neither a stand nor a NOT_GRADED reason")
    return found


def ch(query: str) -> str:
    """One statement on the stand's ClickHouse, as the harness's own HTTP client; bytes that are not UTF-8 survive as surrogates."""
    req = urllib.request.Request(CH_URL, data=query.encode(), method="POST",
                                 headers={"Authorization": "Basic " + b64encode(f"{CH_USER}:{CH_PASSWORD}".encode()).decode()})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode(errors="surrogateescape")


def ensure_bucket() -> None:
    """Create the staging bucket on the GCS emulator; an existing bucket is fine."""
    req = urllib.request.Request(f"{GCS}/storage/v1/b?project=test", data=json.dumps({"name": BUCKET}).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        urllib.request.urlopen(req, timeout=30).read()
    except urllib.error.HTTPError as e:
        if e.code != 409:
            raise


def config(engine: str, db: str, prefix: str, col: str) -> str:
    """A full Parquet export of one seed column staged on GCS, with a ClickHouse `load:` block."""
    col = col.upper() if engine == "oracle" else col
    exp = {"name": col.lower(), "query": f"SELECT id, {col} FROM {'dbo.' if engine == 'mssql' else ''}{TABLE}", "mode": "full", "format": "parquet",
           "destination": {"type": "gcs", "bucket": BUCKET, "prefix": f"{prefix}/{col.lower()}/", "endpoint": GCS, "allow_anonymous": True}}
    load = {"target": "clickhouse", "url": CH_URL, "database": db, "user": CH_USER, "password_env": CH_PASSWORD_ENV, "pk": ["ID" if engine == "oracle" else "id"]}
    return yaml.safe_dump({"source": {"type": engine, "url_env": "RIVET_NSN_URL"}, "load": load, "exports": [exp]}, sort_keys=False)


def read_back(db: str, name: str) -> dict[int, dict]:
    """Every row of one loaded table by id, read by the harness from ClickHouse."""
    out = ch(f"SELECT * FROM {db}.`{name}` FORMAT JSONEachRow SETTINGS output_format_json_quote_64bit_integers = 0, output_format_json_quote_decimals = 0, output_format_json_quote_denormals = 1")
    rows = [{k.lower(): _raw(v) for k, v in json.loads(ln, parse_float=Decimal).items()} for ln in out.splitlines() if ln.strip()]
    return {int(r["id"]): r for r in rows}


def _raw(v):
    """A JSON string that carried non-UTF-8 bytes back as those bytes; anything else as read."""
    if isinstance(v, str):
        try:
            v.encode()
        except UnicodeEncodeError:
            return v.encode(errors="surrogateescape")
    return v


def grade(engine: str, c: Col, n: int, run: core.Proc, load: core.Proc, check: core.Proc, db: str) -> Result:
    """One source column's cell: the warehouse read-back against the seed literals."""
    cell = f"{engine}/{c.native}->none/load-clickhouse/parquet"
    stop = nsn.grade_stop(cell, run, check) or nsn.grade_stop(cell, load, check)
    if stop:
        return stop
    try:
        rows = read_back(db, c.name.lower())
    except Exception as e:
        return Result(cell, DELIVERED, NOT_GRADED, f"the ClickHouse read-back failed: {type(e).__name__}: {e}"[:300])
    if len(rows) != n:
        return Result(cell, DELIVERED, EMPTY, f"exit 0, {len(rows)} of {n} rows in the warehouse")
    fid = nsn.fidelity_of(check.stdout, c.name)
    nulled, wrong = [], []
    for i in range(1, n + 1):
        lit = c.lits[i - 1] if i - 1 < len(c.lits) else "NULL"
        got = rows.get(i, {}).get(c.name.lower())
        v = nsn.compare(engine, lit, c.family, "none", got, False, c.native)
        if v == NULLED:
            nulled.append(lit)
        elif v == WRONG:
            wrong.append(f"{lit}->{got!r}")
    if nulled:
        return Result(cell, DELIVERED, NULLED, f"{len(nulled)} non-NULL values read back as NULL, e.g. {nulled[:3]}", fidelity=fid)
    if wrong and fid in ("lossy", "logical_string", "unsupported"):
        return Result(cell, nsn.DECLARED, "", f"{len(wrong)} values changed under fidelity {fid}", fidelity=fid)
    if wrong:
        return Result(cell, DELIVERED, WRONG, f"{len(wrong)} values read back changed under fidelity {fid or '?'}, e.g. {wrong[:3]}", fidelity=fid)
    return Result(cell, DELIVERED, "", "", fidelity=fid)


def run_column(engine: str, c: Col, n: int, db: str, d: Path) -> Result:
    """Check, export, load and read back one column."""
    cd = d / c.name
    cd.mkdir(parents=True, exist_ok=True)
    cfg = cd / "load.yaml"
    cfg.write_text(config(engine, db, f"{db}-{int(time.time())}", c.name))
    env = {"RIVET_NSN_URL": nsn.STAND[engine], CH_PASSWORD_ENV: CH_PASSWORD}
    check = nsn.rivet(["check", "-c", str(cfg), "--type-report", "--json"], env, cd)
    run = nsn.rivet(["run", "-c", str(cfg)], env, cd)
    load = nsn.rivet(["load", "-c", str(cfg)], env, cd) if run.returncode == 0 else run
    return grade(engine, c, n, run, load, check, db)


def run_engine(engine: str, work: Path) -> list[Result]:
    """Seed once, then load and read back every column; drops the seed table and the warehouse database after."""
    if engine == "mongo":
        return [Result(f"{engine}/*/load-clickhouse", REFUSED, NOT_GRADED, MONGO_NOT_GRADED)]
    url, db = nsn.STAND[engine], DB.format(engine=engine)
    cols = nsn.columns(engine)
    d = work / engine
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    try:
        stmts, n = nsn.seed_sql(engine, cols, TABLE)
        for s in nsn.ledger_setup(engine):
            engines.sql(engine, url, s)
        for st in stmts:
            p = engines.sql(engine, url, st + ";")
            if not p.ok:
                raise RuntimeError(f"{engine} seed refused `{st[:160]}`: {p.out.strip()[:300]}")
        ensure_bucket()
        ch(f"DROP DATABASE IF EXISTS {db}")
        ch(f"CREATE DATABASE {db}")
        with ThreadPoolExecutor(WORKERS) as pool:
            return list(pool.map(lambda c: run_column(engine, c, n, db, d), cols))
    except (RuntimeError, OSError, urllib.error.URLError) as e:
        return [Result(f"{engine}/*/load-clickhouse", REFUSED, NOT_GRADED, f"the stand could not serve the load leg: {e}"[:300])]
    finally:
        engines.sql(engine, url, f"DROP TABLE IF EXISTS {TABLE};")
        try:
            ch(f"DROP DATABASE IF EXISTS {db}")
        except (OSError, urllib.error.URLError):
            pass
        shutil.rmtree(d, ignore_errors=True)


def run_all(work: Path) -> list[Result]:
    """Every engine into every load target: ClickHouse on the stand, the rest NOT_GRADED."""
    results = [Result(f"*/load-{t}", REFUSED, NOT_GRADED, NOT_REACHABLE[t]) for t in load_targets() if t in NOT_REACHABLE]
    for engine in nsn.axes()["engines"]:
        results += run_engine(engine, work)
    return results


def verify_no_silent_null_load(led: Ledger) -> None:
    """Every engine's columns loaded into ClickHouse and read back by the harness; unreachable warehouses are REDUCED skips."""
    work = Path(os.environ.get("RIVET_ORACLE_WORK") or tempfile.gettempdir()) / "no_silent_null_load"
    for t in (t for t in load_targets() if t in NOT_REACHABLE):
        led.skipped("-", "-", SCENARIO, "-", f"{SCENARIO}[{t}]: {NOT_REACHABLE[t]}", cause=SkipCause.REDUCED)
    for engine in nsn.axes()["engines"]:
        results = run_engine(engine, work)
        groups = nsn.summary(engine, results)
        bad = {k: v for k, v in groups.items() if k and k != NOT_GRADED and v}
        if groups[NOT_GRADED]:
            led.skipped(engine, "-", SCENARIO, "-", f"{SCENARIO}[{engine}]: {len(groups[NOT_GRADED])} of {len(results)} cells not graded: {groups[NOT_GRADED][0].detail[:200]}",
                        cause=SkipCause.REDUCED if engine == "mongo" else SkipCause.ENV, target=CH_URL)
        for klass, rs in bad.items():
            led.failed(engine, "-", SCENARIO, "-", f"{SCENARIO}[{engine}]: {klass} over {len(nsn.pairs_of(rs))} source->override pairs #{nsn.digest(rs)}",
                       "\n".join(f"{r.cell}: {r.detail}" for r in sorted(rs, key=lambda r: r.cell)[:60]))
        if not bad and len(groups[NOT_GRADED]) < len(results):
            led.passed(engine, "-", SCENARIO, "-", f"{SCENARIO}[{engine}]: {len(results)} columns read back from ClickHouse")


def self_test() -> None:
    """Grade the grader: the targets come from the product and a NULL read back from the warehouse is red."""
    assert load_targets() and "clickhouse" in load_targets(), load_targets()
    cfg = yaml.safe_load(config("mssql", "pm_no_silent_null_mssql", "p", "c03"))
    assert cfg["load"]["target"] == "clickhouse" and cfg["exports"][0]["query"] == "SELECT id, c03 FROM dbo.pm_no_silent_null_l" and cfg["exports"][0]["destination"]["type"] == "gcs"
    ora = yaml.safe_load(config("oracle", "x", "p", "c03"))
    assert ora["exports"][0]["query"] == "SELECT id, C03 FROM pm_no_silent_null_l" and ora["load"]["pk"] == ["ID"]
    assert _raw("\udcab\x00") == b"\xab\x00" and _raw("ok") == "ok"
    c = Col("c00", "INT", "int32", "int", ("7",))
    ok = nsn._proc(0)
    global read_back
    saved = read_back
    try:
        read_back = lambda db, name: {1: {"id": 1, "c00": None}, 2: {"id": 2, "c00": None}}
        assert grade("mysql", c, 2, ok, ok, ok, "x").klass == NULLED
        read_back = lambda db, name: {1: {"id": 1, "c00": 7}, 2: {"id": 2, "c00": None}}
        assert grade("mysql", c, 2, ok, ok, ok, "x").klass == ""
        read_back = lambda db, name: {1: {"id": 1, "c00": 8}, 2: {"id": 2, "c00": None}}
        assert grade("mysql", c, 2, ok, ok, ok, "x").klass == WRONG
        read_back = lambda db, name: {1: {"id": 1, "c00": 7}}
        assert grade("mysql", c, 2, ok, ok, ok, "x").klass == EMPTY
    finally:
        read_back = saved
    assert grade("mysql", c, 2, ok, nsn._proc(1, "error: x"), ok, "x").klass == nsn.UNCODED
    print(f"no_silent_null_load self-test ok: targets {load_targets()}")


def main() -> int:
    """Developer entry: load every engine into ClickHouse and print the verdict lines."""
    import argparse

    ap = argparse.ArgumentParser(prog="no_silent_null_load")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--work", default=str(Path(tempfile.gettempdir()) / "no_silent_null_load"))
    ap.add_argument("--out", default="")
    ns = ap.parse_args()
    if ns.self_test:
        self_test()
        return 0
    results = run_all(Path(ns.work))
    lines = [nsn.verdict_line(r).replace(nsn.SCENARIO, SCENARIO, 1) for r in sorted(results, key=lambda r: r.cell)]
    red = sum(1 for r in results if r.klass and r.klass != NOT_GRADED)
    ng = sum(1 for r in results if r.klass == NOT_GRADED)
    print("\n".join([*lines, f"{SCENARIO} summary PASS={len(results) - red - ng} RED={red} NOT_GRADED={ng}"]))
    if ns.out:
        Path(ns.out).write_text("\n".join(json.dumps(r.__dict__) for r in results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
