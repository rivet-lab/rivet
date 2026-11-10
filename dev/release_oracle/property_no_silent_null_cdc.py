"""No silent NULL on the change stream: every snapshot, insert, update, delete and DDL leg delivers each non-NULL source value or refuses with a code."""

from __future__ import annotations

import concurrent.futures as cf
import json
import os
import re
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import yaml

from . import cdc, core, engines
from . import property_no_silent_null as nsn
from .core import Ledger, SkipCause
from .property_no_silent_null import DELIVERED, EMPTY, IGNORED, NOT_GRADED, NULLED, REFUSED, WRONG, Col, Result

SCENARIO = "no_silent_null_cdc"
TABLE = "pm_no_silent_null_c"
CDC_STAND = {
    "postgres": "postgresql://rivet:rivet@127.0.0.1:5434/rivet",
    "mysql": "mysql://root:rivet@127.0.0.1:3307/rivet",
    "mssql": "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1434/rivet",
    "oracle": "oracle://c%23%23rivetcdc:rivet@127.0.0.1:1521/FREEPDB1",
    "mongo": None,
}
DML_STAND = {"oracle": nsn.STAND["oracle"]}
STAND_DROPPED = re.compile(r"DPY-600[05]|Listener refused|Login timeout|Lost connection|Can't connect", re.I)
MONGO_NOT_GRADED = "MongoDB CDC delivers each change as the one `document` blob the batch grid grades; there is no typed column to cross"
GROUP = 8
UNITS_PER_ROUND = {"postgres": 8}
WORKERS = 6
INSERT_AT, DDL_AT = 1000, 2000
DDL_SQL = {
    "added": {"mssql": "ALTER TABLE dbo.{t} ADD nsn_add INT", "oracle": "ALTER TABLE {t} ADD (nsn_add NUMBER(9))", "": "ALTER TABLE {t} ADD COLUMN nsn_add INT"},
    "type_changed": {"postgres": "ALTER TABLE {t} ALTER COLUMN nsn_mid TYPE BIGINT", "mysql": "ALTER TABLE {t} MODIFY nsn_mid BIGINT",
                     "mssql": "ALTER TABLE dbo.{t} ALTER COLUMN nsn_mid BIGINT", "oracle": "ALTER TABLE {t} MODIFY (nsn_mid NUMBER(18))"},
    "removed": {"mssql": "ALTER TABLE dbo.{t} DROP COLUMN nsn_mid", "": "ALTER TABLE {t} DROP COLUMN nsn_mid"},
}


def change_ops() -> list[str]:
    """The change kinds the product writes into `__op`, read from `ChangeOp::as_str`."""
    text = (nsn.ROOT / "src/source/cdc/mod.rs").read_text()
    ops = dict(re.findall(r"ChangeOp::(\w+) => \"(\w+)\"", text))
    variants = nsn._enum_variants("src/source/cdc/mod.rs", "ChangeOp")
    if sorted(o.lower() for o in ops) != sorted(variants):
        raise AssertionError(f"ChangeOp variants {variants} and as_str arms {ops} differ: the leg axis cannot be read")
    return [ops[v] for v in sorted(ops)]


def ddl_kinds() -> list[str]:
    """The schema change kinds the product reports, read from the fields of `SchemaChangeEntry`; `removed` runs last."""
    text = (nsn.ROOT / "src/pipeline/report.rs").read_text()
    m = re.search(r"pub struct SchemaChangeEntry \{(.*?)\}", text, re.S)
    kinds = re.findall(r"pub (\w+):", m.group(1)) if m else []
    if set(kinds) != set(DDL_SQL):
        raise AssertionError(f"SchemaChangeEntry fields {kinds} have no DDL in DDL_SQL {sorted(DDL_SQL)}")
    return sorted(kinds, key=lambda k: k == "removed")


def row_images(text: str | None = None) -> list[str]:
    """The `binlog_row_image` values the MySQL CDC doc block names."""
    text = text if text is not None else (nsn.ROOT / "src/source/mysql/cdc.rs").read_text()
    block = text.split("`binlog_row_image` decides", 1)[-1].split("rivet's change stream", 1)[0]
    out = list(dict.fromkeys(re.findall(r"`([A-Z]{4,})`", block)))
    if "FULL" not in out or len(out) < 2:
        raise AssertionError(f"mysql/cdc.rs: row images {out}: the image axis cannot be read")
    return out


def legs() -> list[str]:
    """Snapshot, every change kind, then every DDL kind."""
    return ["snapshot", *change_ops(), *ddl_kinds()]


@dataclass(frozen=True)
class Unit:
    target: str
    col: Col | None
    fmt: str


def lit(col: Col, k: int) -> str:
    """The literal of value index k of a column, NULL past its edges."""
    return col.lits[k] if 0 <= k < len(col.lits) else "NULL"


def expected(leg: str, n: int) -> list[tuple[str, int, int]]:
    """(__op, id, value index) the leg must deliver for an n-row seed."""
    if leg == "snapshot":
        return [("", i, i - 1) for i in range(1, n + 1)]
    if leg == "insert":
        return [("insert", INSERT_AT + i, i - 1) for i in range(1, n + 1)]
    if leg == "update":
        return [("update", i, i) for i in range(1, n)] + [("update", INSERT_AT + i, i - 1) for i in range(1, n + 1)]
    if leg == "delete":
        return [("delete", i, i) for i in range(1, n + 1)]
    return [("insert", DDL_AT + 1 + ddl_kinds().index(leg), 0)]


def leg_run(leg: str) -> str:
    """The run that delivers a leg: the snapshot run, the stream run, or the run after that DDL."""
    return "snapshot" if leg == "snapshot" else leg if leg in DDL_SQL else "stream"


def dml(engine: str, cols: list[Col], t: str, n: int) -> list[str]:
    """Insert n rows past the seed, rotate the seed values one row up, touch only nsn_mid of the new rows, delete the seed."""
    q = f"dbo.{t}" if engine == "mssql" else t
    ins, _ = nsn.row_inserts(engine, cols, t, INSERT_AT)
    rot = [f"UPDATE {q} SET " + ", ".join(f"{c.name} = {lit(c, i)}" for c in cols) + f" WHERE id = {i}" for i in range(1, n)]
    return [*ins, *rot, f"UPDATE {q} SET nsn_mid = 1 WHERE id > {INSERT_AT}", f"DELETE FROM {q} WHERE id <= {n}"]


def ddl_insert(engine: str, cols: list[Col], t: str, kind: str) -> list[str]:
    """The DDL of one kind, then one row of value index 0."""
    stmt = DDL_SQL[kind].get(engine, DDL_SQL[kind].get("")).format(t=t)
    ins, _ = nsn.row_inserts(engine, [Col(c.name, c.native, c.delivery, c.family, c.lits[:1]) for c in cols], t, DDL_AT + ddl_kinds().index(kind))
    return [stmt, ins[0]]


def sql(engine: str, url: str, stmt: str) -> core.Proc:
    """One source statement, asked again while the stand drops the connection."""
    for i in range(nsn.STAND_TRIES):
        p = engines.sql(engine, url, stmt)
        if p.ok or not STAND_DROPPED.search(p.out):
            break
        time.sleep(1 + 2 * i)
    return p


def exec_all(engine: str, url: str, stmts: list[str], image: str = "") -> None:
    """Run statements on the source; SQL Server waits for its capture job; raises on the first refusal."""
    before = cdc._mssql_max_lsn(url) if engine == "mssql" else ""
    if engine == "mysql" and image:
        p = sql(engine, url, f"SET SESSION binlog_row_image = {image}; " + " ".join(s + ";" for s in stmts))
        if not p.ok:
            raise RuntimeError(f"mysql refused the {image} DML: {p.out.strip()[:300]}")
    else:
        for s in stmts:
            p = sql(engine, url, s + ";")
            if not p.ok:
                raise RuntimeError(f"{engine} refused `{s[:160]}`: exit {p.returncode}: {p.out.strip()[:300]}")
    if engine == "mssql":
        cdc._wait_mssql_captured(url, before)


def capture_on(engine: str, url: str, t: str) -> list[str]:
    """Statements that put a seeded table under capture."""
    if engine == "postgres":
        return [f"ALTER TABLE {t} REPLICA IDENTITY FULL"]
    if engine == "oracle":
        return [f"ALTER TABLE {t} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", f"GRANT SELECT ON {t} TO c##rivetcdc"]
    if engine == "mssql":
        return [f"EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='{t}', @role_name=NULL, @capture_instance='dbo_{t}'"]
    return []


def wait_capture(engine: str, url: str, t: str) -> None:
    """SQL Server: wait until the capture instance is queryable."""
    if engine == "mssql" and not cdc.wait_until(lambda: "NULL" not in cdc._sqlcmd(
            url, q=f"SET NOCOUNT ON; SELECT ISNULL(CONVERT(varchar(64), sys.fn_cdc_get_min_lsn('dbo_{t}'), 1), 'NULL')").stdout, tries=30, delay=1.5):
        raise RuntimeError(f"mssql capture instance dbo_{t} never became queryable")


def drop(engine: str, url: str, t: str, slots: list[str]) -> None:
    """Remove a round's slots, capture instance and table."""
    if engine == "postgres":
        for s in slots:
            cdc._psql(url, sql=f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots WHERE slot_name='{s}';")
    if engine == "mssql":
        cdc._lc_drop("mssql", url, t)
    else:
        engines.sql(engine, DML_STAND.get(engine, url), f"DROP TABLE IF EXISTS {t};")


def cdc_keys(engine: str, d: Path, t: str, slot: str, sid: int) -> dict:
    """The engine's `cdc:` keys of one unit."""
    keys = {"initial": "snapshot", "until_current": True}
    if engine == "postgres":
        return keys | {"slot": slot}
    if engine == "mysql":
        return keys | {"checkpoint": str(d / "cdc.ckpt"), "server_id": sid}
    if engine == "mssql":
        return keys | {"capture_instance": f"dbo_{t}", "checkpoint": str(d / "cdc.ckpt")}
    return keys | {"checkpoint": str(d / "cdc.ckpt")}


def unit_config(engine: str, t: str, u: Unit, d: Path, slot: str, sid: int) -> str:
    """A CDC export of the whole round table, with one override when the unit has a target."""
    fold = str.upper if engine == "oracle" else str
    exp = {"name": "nsn", "table": f"RIVET.{t.upper()}" if engine == "oracle" else t, "mode": "cdc", "format": u.fmt,
           "cdc": cdc_keys(engine, d, t, slot, sid), "destination": {"type": "local", "path": str(d / "out")}}
    if u.target != "none":
        exp["columns"] = {fold(u.col.name): u.target}
    return yaml.safe_dump({"source": {"type": engine, "url_env": "RIVET_NSN_URL"}, "exports": [exp]}, sort_keys=False)


def grade_unit(engine: str, u: Unit, cols: list[Col], n: int, runs: dict[str, core.Proc], checks: dict[str, core.Proc], d: Path, tag: str) -> list[Result]:
    """Grade every leg of one unit from the whole output directory against the seed literals; each run is held to the check made right before it."""
    graded = cols if u.target == "none" else [u.col]
    try:
        got = nsn.read_parts(d / "out", u.fmt, key="")
    except Exception as e:
        got, err = None, f"the second reader failed: {type(e).__name__}: {e}"[:300]
    else:
        err = ""
    desc, rows = (got[0], got[1]) if got else ({}, [])
    seen: dict[tuple[str, int], dict] = {}
    for r in rows:
        if r.get("id") is not None:
            seen[((r.get("__op") or ""), int(r["id"]))] = r
    out = []
    for leg in legs():
        exp = expected(leg, n)
        for c in graded:
            cell = f"{engine}/{c.native}->{u.target}/cdc-{leg}{tag}/{u.fmt}"
            p, check = runs.get(leg_run(leg)), checks.get(leg_run(leg), checks.get("snapshot"))
            stop = nsn.grade_stop(cell, p, check) if p is not None else Result(cell, REFUSED, NOT_GRADED, "the run of this leg never started")
            if stop:
                out.append(stop)
                continue
            if err:
                out.append(Result(cell, DELIVERED, NOT_GRADED, err))
                continue
            fid = nsn.fidelity_of(checks["snapshot"].stdout, c.name)
            key = c.name.lower()
            if u.fmt == "parquet" and u.target != "none" and not nsn.type_ok(u.target, desc.get(key, "?")):
                out.append(Result(cell, DELIVERED, IGNORED, f"declared {u.target}, delivered {desc.get(key)}", fidelity=fid))
                continue
            missing = [f"{op or 'snapshot'}:{i}" for op, i, _ in exp if (op, i) not in seen]
            if missing:
                out.append(Result(cell, DELIVERED, EMPTY, f"{len(missing)} of {len(exp)} change rows not delivered, e.g. {missing[:3]}"))
                continue
            nulled, wrong = [], []
            for op, i, k in exp:
                v = nsn.compare(engine, lit(c, k), c.family, u.target, seen[(op, i)].get(key), u.fmt == "csv", c.native)
                if v == NULLED:
                    nulled.append(f"{op or 'snapshot'}:{i}:{lit(c, k)}")
                elif v == WRONG:
                    wrong.append(f"{op or 'snapshot'}:{i}:{lit(c, k)}->{seen[(op, i)].get(key)!r}")
            if nulled:
                out.append(Result(cell, DELIVERED, NULLED, f"{len(nulled)} non-NULL values delivered as NULL, e.g. {nulled[:3]}", fidelity=fid))
            elif wrong and fid in ("lossy", "logical_string", "unsupported"):
                out.append(Result(cell, nsn.DECLARED, "", f"{len(wrong)} values changed under fidelity {fid}", fidelity=fid))
            elif wrong:
                out.append(Result(cell, DELIVERED, WRONG, f"{len(wrong)} values changed under fidelity {fid or '?'}, e.g. {wrong[:3]}", fidelity=fid))
            else:
                out.append(Result(cell, DELIVERED, "", "", fidelity=fid))
    return out


def run_round(engine: str, r: int, cols: list[Col], units: list[Unit], image: str, work: Path) -> list[Result]:
    """Seed one round table, run every unit's snapshot, change and DDL legs, grade them and drop the round's objects."""
    url, dml_url = CDC_STAND[engine], DML_STAND.get(engine, CDC_STAND[engine])
    t = f"{TABLE}{r}"
    slots = [f"{t}_u{i}" for i in range(len(units))]
    tag = f"@{image}" if image else ""
    drop(engine, url, t, slots)
    try:
        stmts, n = nsn.seed_sql(engine, cols, t)
        for s in nsn.ledger_setup(engine):
            engines.sql(engine, dml_url, s)
        mid = {"oracle": f"ALTER TABLE {t} ADD (nsn_mid NUMBER(9))", "mssql": f"ALTER TABLE dbo.{t} ADD nsn_mid INT"}.get(engine, f"ALTER TABLE {t} ADD nsn_mid INT")
        exec_all(engine, dml_url, [stmts[0], stmts[1], mid, *stmts[2:], *capture_on(engine, url, t)])
        wait_capture(engine, url, t)
    except RuntimeError as e:
        drop(engine, url, t, slots)
        return [Result(f"{engine}/{c.native}->{u.target}/cdc-*{tag}/{u.fmt}", REFUSED, NOT_GRADED, f"the round could not seed: {e}"[:300])
                for u in units for c in (cols if u.target == "none" else [u.col])]
    env = {"RIVET_NSN_URL": url}
    dirs, checks, runs = [], {i: {} for i in range(len(units))}, {i: {} for i in range(len(units))}
    for i, u in enumerate(units):
        d = work / f"r{r}{tag}-u{i}"
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)
        (d / "cfg.yaml").write_text(unit_config(engine, t, u, d, slots[i], 30000 + (r * 64 + i) % 30000))
        dirs.append(d)

    def go(i: int, leg: str) -> None:
        if leg in ("snapshot", *DDL_SQL):
            checks[i][leg] = nsn.rivet(["check", "-c", str(dirs[i] / "cfg.yaml"), "--type-report", "--json"], env, dirs[i])
        runs[i][leg] = nsn.rivet(["run", "-c", str(dirs[i] / "cfg.yaml")], env, dirs[i])

    def each(leg: str) -> None:
        with cf.ThreadPoolExecutor(WORKERS) as ex:
            list(ex.map(lambda i: go(i, leg), range(len(units))))

    try:
        each("snapshot")
        exec_all(engine, dml_url, dml(engine, cols, t, n), image)
        each("stream")
        for kind in ddl_kinds():
            exec_all(engine, dml_url, ddl_insert(engine, cols, t, kind), image)
            each(kind)
    except RuntimeError as e:
        note = f"the round's source DML was refused: {e}"[:300]
        results = [Result(f"{engine}/*{tag}", REFUSED, NOT_GRADED, note)]
    else:
        results = []
    for i, u in enumerate(units):
        results += grade_unit(engine, u, cols, n, runs[i], checks[i], dirs[i], tag)
        shutil.rmtree(dirs[i], ignore_errors=True)
    drop(engine, url, t, slots)
    return results


def rounds(engine: str, cols: list[Col]) -> list[tuple[list[Col], list[Unit], str]]:
    """(columns, units, row image) rounds: per column group the `none` units in every format and one override unit per family representative and target on Parquet; MySQL adds every non-default row image."""
    ax = nsn.axes()
    cap = UNITS_PER_ROUND.get(engine, 64)
    seen: set[str] = set()
    out = []
    for g in range(0, len(cols), GROUP):
        group = cols[g:g + GROUP]
        units = [Unit("none", None, f) for f in ax["formats"]]
        for c in group:
            if c.family not in seen:
                seen.add(c.family)
                units += [Unit(t, c, "parquet") for t in ax["targets"] if t != "none"]
        out += [(group, units[k:k + cap], "") for k in range(0, len(units), cap)]
    if engine == "mysql":
        default = engines.sql("mysql", CDC_STAND["mysql"], "SELECT @@global.binlog_row_image;").out
        for image in (i for i in row_images() if i not in default):
            out += [(cols[g:g + GROUP], [Unit("none", None, "parquet")], image) for g in range(0, len(cols), GROUP)]
    return out


def run_engine(engine: str, work: Path, progress=lambda s: None) -> list[Result]:
    """Every round of one engine; MongoDB is NOT_GRADED with its reason."""
    if CDC_STAND[engine] is None:
        return [Result(f"{engine}/*/cdc-*", REFUSED, NOT_GRADED, MONGO_NOT_GRADED)]
    untrusted = nsn.reader_trusted(nsn.READER)
    if untrusted:
        return [Result(f"{engine}/*/cdc-*", REFUSED, NOT_GRADED, untrusted)]
    cols = nsn.columns(engine)
    results: list[Result] = []
    plan = rounds(engine, cols)
    for r, (group, units, image) in enumerate(plan):
        results += run_round(engine, r, group, units, image, work)
        progress(f"{engine}: round {r + 1}/{len(plan)}, {len(results)} cells")
    return results


def record(led: Ledger, engine: str, results: list[Result]) -> None:
    """Write one engine's results to the ledger: a FAIL row per failure class, or the PASS row with the counts."""
    groups = nsn.summary(engine, results)
    bad = {k: v for k, v in groups.items() if k and k != NOT_GRADED and v}
    if groups[NOT_GRADED]:
        led.skipped(engine, "-", SCENARIO, "-", f"{SCENARIO}[{engine}]: {len(groups[NOT_GRADED])} of {len(results)} cells not graded: {groups[NOT_GRADED][0].detail[:200]}",
                    cause=SkipCause.ENV if CDC_STAND.get(engine) else SkipCause.REDUCED, target=CDC_STAND.get(engine) or engine)
    for klass, rs in bad.items():
        led.failed(engine, "-", SCENARIO, "-", f"{SCENARIO}[{engine}]: {klass} over {len(nsn.pairs_of(rs))} source->override pairs #{nsn.digest(rs)}",
                   "\n".join(f"{r.cell}: {r.detail}" for r in sorted(rs, key=lambda r: r.cell)[:60]))
    if not bad and len(groups[NOT_GRADED]) < len(results):
        led.passed(engine, "-", SCENARIO, "-", f"{SCENARIO}[{engine}]: {len(results)} cells graded by a second reader")


def verify_no_silent_null_cdc(led: Ledger) -> None:
    """Every engine's change stream: snapshot, change and DDL legs, override targets and MySQL row images."""
    work = Path(os.environ.get("RIVET_ORACLE_WORK") or tempfile.gettempdir()) / "no_silent_null_cdc"
    for engine in nsn.axes()["engines"]:
        t0 = time.time()
        results = run_engine(engine, work / engine, led.phase)
        record(led, engine, results)
        led.ok(f"{engine}: {len(results)} cdc cells in {time.time() - t0:.0f}s")


def self_test() -> None:
    """Grade the grader: the axes come from the product and every leg's expectation names the rows the DML wrote."""
    assert set(CDC_STAND) == set(nsn.axes()["engines"]), f"CDC_STAND {sorted(CDC_STAND)} must name every SourceType variant"
    assert change_ops() == ["delete", "insert", "update"], change_ops()
    assert ddl_kinds()[-1] == "removed" and set(ddl_kinds()) == {"added", "removed", "type_changed"}
    assert row_images("x `binlog_row_image` decides `FULL` ... `MINIMAL` and `NOBLOB` y rivet's change stream `ZZZZ`") == ["FULL", "MINIMAL", "NOBLOB"]
    assert set(row_images()) >= {"FULL", "MINIMAL"}
    assert expected("snapshot", 3) == [("", 1, 0), ("", 2, 1), ("", 3, 2)]
    assert expected("update", 3) == [("update", 1, 1), ("update", 2, 2), ("update", 1001, 0), ("update", 1002, 1), ("update", 1003, 2)]
    assert expected("delete", 2) == [("delete", 1, 1), ("delete", 2, 2)] and expected("removed", 9) == [("insert", 2003, 0)]
    assert [leg_run(x) for x in ("snapshot", "insert", "delete", "added")] == ["snapshot", "stream", "stream", "added"]
    c = Col("c00", "INT", "int32", "int", ("1", "2"))
    stmts = dml("mysql", [c], "t", 3)
    assert stmts[3] == "UPDATE t SET c00 = 2 WHERE id = 1" and stmts[4] == "UPDATE t SET c00 = NULL WHERE id = 2" and stmts[-1] == "DELETE FROM t WHERE id <= 3"
    assert ddl_insert("mssql", [c], "t", "added")[0] == "ALTER TABLE dbo.t ADD nsn_add INT"
    assert "VALUES (2001, 1," in ddl_insert("postgres", [c], "t", "added")[1]
    ok = nsn._proc(0)
    d = Path(tempfile.mkdtemp())
    rows = [{"id": 1, "c00": 1}, {"id": 2, "c00": 2}, {"id": 3, "c00": None}, {"id": 1001, "__op": "insert", "c00": 1}, {"id": 1002, "__op": "insert", "c00": 2},
            {"id": 1003, "__op": "insert", "c00": None}, {"id": 1, "__op": "update", "c00": 2}, {"id": 2, "__op": "update", "c00": None},
            {"id": 1001, "__op": "update", "c00": None}, {"id": 1002, "__op": "update", "c00": 2}, {"id": 1003, "__op": "update", "c00": None},
            {"id": 1, "__op": "delete", "c00": 2}, {"id": 2, "__op": "delete", "c00": None}, {"id": 3, "__op": "delete", "c00": None}]
    rows += [{"id": 2001 + k, "__op": "insert", "c00": 1} for k in range(3)]
    saved = nsn.read_parts
    nsn.read_parts = lambda out, fmt, key="": ({"id": "INTEGER", "c00": "INTEGER", "__op": "VARCHAR"}, rows, 1)
    try:
        rs = {x.cell: x for x in grade_unit("mysql", Unit("none", None, "parquet"), [c], 3, {k: ok for k in ("snapshot", "stream", *ddl_kinds())}, {"snapshot": ok}, d, "")}
    finally:
        nsn.read_parts = saved
    assert rs["mysql/INT->none/cdc-update/parquet"].klass == NULLED, rs["mysql/INT->none/cdc-update/parquet"]
    assert all(rs[f"mysql/INT->none/cdc-{x}/parquet"].klass == "" for x in ("snapshot", "insert", "delete", "added")), rs
    rs = grade_unit("mysql", Unit("none", None, "parquet"), [c], 3, {"snapshot": nsn._proc(5, "[RIVET_SOURCE_VALUE_UNREPRESENTABLE]")}, {"snapshot": ok}, d, "@MINIMAL")
    assert rs[0].cell.endswith("cdc-snapshot@MINIMAL/parquet") and rs[0].verdict == REFUSED and rs[1].klass == NOT_GRADED
    shutil.rmtree(d, ignore_errors=True)
    print(f"no_silent_null_cdc self-test ok: legs {legs()}, row images {row_images()}")


def main() -> int:
    """Developer entry: run one engine's change-stream grid against the stand."""
    import argparse

    ap = argparse.ArgumentParser(prog="no_silent_null_cdc")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--engine", default="mysql", help="one engine or `all`")
    ap.add_argument("--work", default=str(Path(tempfile.gettempdir()) / "no_silent_null_cdc"))
    ap.add_argument("--out", default="")
    ns = ap.parse_args()
    if ns.self_test:
        self_test()
        return 0
    names = nsn.axes()["engines"] if ns.engine == "all" else [ns.engine]
    results: list[Result] = []
    for name in names:
        rs = run_engine(name, Path(ns.work) / name, lambda s: print(s, flush=True))
        results += rs
        for k, g in nsn.summary(name, rs).items():
            print(f"{name:9} {k or 'ok':18} {len(g)}")
    lines = [nsn.verdict_line(r).replace(nsn.SCENARIO, SCENARIO, 1) for r in sorted(results, key=lambda r: r.cell)]
    red = sum(1 for r in results if r.klass and r.klass != NOT_GRADED)
    ng = sum(1 for r in results if r.klass == NOT_GRADED)
    print("\n".join([*lines, f"{SCENARIO} summary PASS={len(results) - red - ng} RED={red} NOT_GRADED={ng}"]))
    if ns.out:
        Path(ns.out).write_text("\n".join(json.dumps(r.__dict__) for r in results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
