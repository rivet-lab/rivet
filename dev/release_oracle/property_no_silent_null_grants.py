"""No silent NULL under `source.columns_from: grants` (MySQL CDC): a readable column is delivered, a hidden one is absent, in both legs."""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import time
from pathlib import Path

import yaml

from . import core
from . import property_no_silent_null as nsn
from .core import Ledger, SkipCause

ENGINE = "mysql_cdc_grants"
CDC_URL = "mysql://rivet:rivet@127.0.0.1:3307/rivet"
TABLE = "pm_no_silent_null_g"
STREAM_OFFSET = 1000
HIDDEN, SHAPE = nsn.HIDDEN, nsn.SHAPE
META = re.compile(r"^(__|_rivet)")
ROLES = ("granted", "hidden")


def root_sql(sql: str) -> core.Proc:
    """SQL as the stand's root account, through the container that publishes the CDC port."""
    c = core.container_for_port(int(re.search(r":(\d+)/", CDC_URL).group(1)))
    if c is None:
        return core.Proc(["docker"], 125, "", f"no running container publishes {CDC_URL}")
    return core.docker_exec(c, *nsn_mysql_root(), stdin=sql)


def nsn_mysql_root() -> list[str]:
    """The `mysql` client argv for the stand's root account."""
    from ..pytools.shell import mysql_client

    return mysql_client("rivet", user="root")


def split_columns(cols: list[nsn.Col]) -> dict[str, list[nsn.Col]]:
    """Alternate the columns into two disjoint grant sets so each one is readable under one account and hidden under the other."""
    return {"even": cols[0::2], "odd": cols[1::2]}


def account_sql(user: str, table: str, granted: list[nsn.Col]) -> list[str]:
    """Statements that create a replication account that may SELECT `id` and `granted` of `table` only."""
    names = ", ".join(["id", *(c.name for c in granted)])
    return [
        f"DROP USER IF EXISTS '{user}'@'%'",
        f"CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'",
        f"GRANT SELECT ({names}) ON rivet.{table} TO '{user}'@'%'",
        f"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%'",
    ]


def config(url_env: str, work: Path, server_id: int) -> str:
    """The CDC export of the seed table under `columns_from: grants`: a snapshot, then the stream up to now."""
    exp = {"name": TABLE, "table": TABLE, "mode": "cdc", "format": "parquet",
           "cdc": {"initial": "snapshot", "until_current": True, "checkpoint": str(work / "cdc.ckpt"), "server_id": server_id},
           "destination": {"type": "local", "path": str(work / "out")}}
    return yaml.safe_dump({"source": {"type": "mysql", "url_env": url_env, "columns_from": "grants"}, "exports": [exp]}, sort_keys=False)


def part_files(out: Path) -> list[Path]:
    """Every Parquet part under the destination at any depth, in a stable order."""
    return sorted(out.rglob("*.parquet"))


def leg_of(part: Path) -> str:
    """`stream` for a part with the change meta columns, `snapshot` for a plain table part."""
    import pyarrow.parquet as pq

    return "stream" if "__op" in pq.read_schema(part).names else "snapshot"


def delivered(parts: list[Path]) -> dict[str, tuple[set[str], dict[int, dict]]]:
    """(data column names, rows by id) of the parts of each leg, read by pyarrow and DuckDB."""
    out: dict[str, tuple[set[str], dict[int, dict]]] = {}
    for leg in ("snapshot", "stream"):
        files = [str(p) for p in parts if leg_of(p) == leg]
        got = nsn.read_files(files, "parquet") if files else None
        if got is None:
            continue
        desc, rows, _ = got
        out[leg] = ({k for k in desc if not META.match(k)}, rows)
    return out


def literal(col: nsn.Col, row_id: int, n_rows: int) -> str:
    """The seed literal of `col` in the row `row_id` (the all-NULL row is the last one)."""
    i = row_id - 1
    return col.lits[i] if i < len(col.lits) and row_id < n_rows else "NULL"


def grade_columns(cols: list[nsn.Col], granted: set[str], got: dict, n_rows: int) -> list[nsn.Result]:
    """One Result per (column, role, leg): a granted column keeps every non-NULL value, a hidden one is absent by name."""
    out = []
    for leg, (names, rows) in got.items():
        base = 0 if leg == "snapshot" else STREAM_OFFSET
        for c in cols:
            role = "granted" if c.name in granted else "hidden"
            cell = f"{ENGINE}/{c.native}->{role}/{leg}"
            if role == "hidden":
                out.append(nsn.Result(cell, nsn.DELIVERED, HIDDEN if c.name in names else "", f"hidden column `{c.name}` is a column of the delivered {leg} parts" if c.name in names else ""))
                continue
            if c.name not in names:
                out.append(nsn.Result(cell, nsn.DELIVERED, nsn.EMPTY, f"granted column `{c.name}` is absent from the {leg} parts"))
                continue
            nulled, wrong, missing = [], [], 0
            for i in range(1, n_rows + 1):
                row = rows.get(base + i)
                if row is None:
                    missing += 1
                    continue
                lit = literal(c, i, n_rows)
                v = nsn.compare("mysql", lit, c.family, "none", row[c.name])
                if v == nsn.NULLED:
                    nulled.append(lit)
                elif v == nsn.WRONG:
                    wrong.append(f"{lit}->{row[c.name]!r}")
            if missing:
                out.append(nsn.Result(cell, nsn.DELIVERED, nsn.EMPTY, f"{missing} of {n_rows} rows missing from the {leg} parts"))
            elif nulled:
                out.append(nsn.Result(cell, nsn.DELIVERED, nsn.NULLED, f"{len(nulled)} non-NULL values delivered as NULL, e.g. {nulled[:3]}"))
            elif wrong:
                out.append(nsn.Result(cell, nsn.DELIVERED, nsn.WRONG, f"{len(wrong)} values changed, e.g. {wrong[:3]}"))
            else:
                out.append(nsn.Result(cell, nsn.DELIVERED))
    if {"snapshot", "stream"} <= set(got) and got["snapshot"][0] != got["stream"][0]:
        diff = sorted(got["snapshot"][0] ^ got["stream"][0])
        out.append(nsn.Result(f"{ENGINE}/*->legs/shape", nsn.DELIVERED, SHAPE, f"snapshot and stream parts differ in columns {diff[:8]}"))
    return out


def run_account(work: Path, label: str, cols: list[nsn.Col], granted: list[nsn.Col], n_rows: int, seed_no: int) -> list[nsn.Result]:
    """Snapshot leg, stream leg and a grant change between runs for one account; every result is graded on the delivered bytes."""
    for st in nsn.seed_sql("mysql", cols, TABLE)[0]:
        p = root_sql(st + ";")
        if not p.ok:
            return [nsn.Result(f"{ENGINE}/*->{label}/seed", nsn.REFUSED, nsn.NOT_GRADED, f"the stand refused the seed `{st[:80]}`: {p.out.strip()[:200]}")]
    user = f"pm_no_silent_null_{label}_{os.getpid()}"
    d = work / label
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    for st in account_sql(user, TABLE, granted):
        p = root_sql(st + ";")
        if not p.ok:
            return [nsn.Result(f"{ENGINE}/*->{label}/account", nsn.REFUSED, nsn.NOT_GRADED, f"the stand refused `{st[:80]}`: {p.out.strip()[:200]}")]
    cfg = d / "c.yaml"
    cfg.write_text(config("RIVET_NSN_URL", d, 7000 + seed_no))
    env = {"RIVET_NSN_URL": CDC_URL.replace("rivet:rivet@", f"{user}:pw@")}
    results: list[nsn.Result] = []
    try:
        check = nsn.rivet(["check", "-c", str(cfg)], env, d)
        steps = [("snapshot", None)]
        steps.append(("stream", [*nsn.row_inserts("mysql", cols, TABLE, STREAM_OFFSET)[0]]))
        for step, inserts in steps:
            if inserts:
                p = root_sql(";\n".join(inserts) + ";")
                if not p.ok:
                    return [*results, nsn.Result(f"{ENGINE}/*->{label}/{step}", nsn.REFUSED, nsn.NOT_GRADED, f"the stand refused the stream rows: {p.out.strip()[:200]}")]
            p = nsn.rivet(["run", "-c", str(cfg)], env, d)
            stop = nsn.grade_stop(f"{ENGINE}/*->{label}/{step}", p, check)
            if stop:
                return [*results, stop]
        got = delivered(part_files(d / "out"))
        if "snapshot" not in got or "stream" not in got:
            return [*results, nsn.Result(f"{ENGINE}/*->{label}/delivery", nsn.DELIVERED, nsn.EMPTY, f"exit 0, legs delivered: {sorted(got)}")]
        results += grade_columns(cols, {c.name for c in granted}, got, n_rows)
        results += grant_change(d, cfg, env, user, cols, granted, n_rows, check, label)
    finally:
        root_sql(f"DROP USER IF EXISTS '{user}'@'%';")
        shutil.rmtree(d, ignore_errors=True)
    return results


def log_refusal(stop: nsn.Result | None) -> nsn.Result | None:
    """A coded run refusal after a grant change is the property's second allowed outcome: `check` never reads the log."""
    if stop and stop.klass == nsn.SPLIT:
        return nsn.Result(stop.cell, nsn.REFUSED, "", stop.detail, stop.code)
    return stop


def grant_change(d: Path, cfg: Path, env: dict, user: str, cols: list[nsn.Col], granted: list[nsn.Col], n_rows: int, check: core.Proc, label: str) -> list[nsn.Result]:
    """Revoke one readable column, then grant one hidden column, a new row after each: the next run refuses with a code or keeps the property."""
    out = []
    hidden = [c for c in cols if c not in granted]
    for kind, col in (("revoke", granted[0]), ("grant", hidden[0])):
        cell = f"{ENGINE}/{col.native}->{kind}/after_change"
        root_sql((f"REVOKE SELECT ({col.name}) ON rivet.{TABLE} FROM '{user}'@'%'" if kind == "revoke" else f"GRANT SELECT ({col.name}) ON rivet.{TABLE} TO '{user}'@'%'") + ";")
        rid = 2 * STREAM_OFFSET + (1 if kind == "revoke" else 2)
        stmts, _ = nsn.row_inserts("mysql", cols, TABLE, rid - 1)
        root_sql(stmts[0] + ";")
        before = set(part_files(d / "out"))
        now = nsn.rivet(["check", "-c", str(cfg)], env, d)
        p = nsn.rivet(["run", "-c", str(cfg)], env, d)
        stop = log_refusal(nsn.grade_stop(cell, p, now))
        if stop:
            out.append(stop)
            continue
        new = [x for x in part_files(d / "out") if x not in before]
        got = delivered(new).get("stream")
        row = got[1].get(rid) if got else None
        if row is None:
            out.append(nsn.Result(cell, nsn.DELIVERED, nsn.EMPTY, f"exit 0, the row {rid} written after the {kind} is not in the new parts"))
            continue
        lit = literal(col, 1, n_rows)
        present = col.name in got[0]
        if kind == "revoke" and present:
            out.append(nsn.Result(cell, nsn.DELIVERED, HIDDEN, f"revoked column `{col.name}` is delivered after the {kind} ({row[col.name]!r})"))
        elif kind == "grant" and present and nsn.compare("mysql", lit, col.family, "none", row[col.name]) == nsn.NULLED:
            out.append(nsn.Result(cell, nsn.DELIVERED, nsn.NULLED, f"newly granted column `{col.name}` is present as NULL for the non-NULL {lit}"))
        else:
            out.append(nsn.Result(cell, nsn.DELIVERED))
    return out


def run_grants(work: Path, progress=lambda s: None) -> list[nsn.Result]:
    """Seed the CDC stand's table with every ledger type and run both grant sets."""
    cols = nsn.columns("mysql")
    n_rows = nsn.seed_sql("mysql", cols, TABLE)[1]
    if not root_sql("SELECT 1;").ok:
        raise RuntimeError(f"no running container publishes {CDC_URL}")
    untrusted = nsn.reader_trusted(nsn.READER)
    if untrusted:
        return [nsn.Result(f"{ENGINE}/grants/*", nsn.REFUSED, nsn.NOT_GRADED, untrusted)]
    results: list[nsn.Result] = []
    try:
        for i, (label, granted) in enumerate(split_columns(cols).items()):
            progress(f"{label}: {len(granted)} of {len(cols)} columns granted")
            try:
                results += run_account(work, label, cols, granted, n_rows, i)
            except Exception as e:
                results.append(nsn.Result(f"{ENGINE}/*->{label}/reader", nsn.DELIVERED, nsn.NOT_GRADED, f"the second reader failed: {type(e).__name__}: {e}"[:300]))
    finally:
        root_sql(f"DROP TABLE IF EXISTS {TABLE};")
    return results


def verify_no_silent_null_grants(led: Ledger) -> None:
    """Run the grants-CDC cells against the stand's mysql-cdc and record them; a stand that is down is an ENV skip."""
    work = Path(os.environ.get("RIVET_ORACLE_WORK") or tempfile.gettempdir()) / "no_silent_null" / "grants"
    t0 = time.time()
    try:
        results = run_grants(work, led.phase)
    except RuntimeError as e:
        led.skipped(ENGINE, "-", nsn.SCENARIO, "-", f"{nsn.SCENARIO}[{ENGINE}] not graded: {e}", cause=SkipCause.ENV, target=CDC_URL)
        return
    nsn.record(led, ENGINE, results, "smoke")
    led.ok(f"{ENGINE}: {len(results)} cells in {time.time() - t0:.0f}s")


def self_test() -> None:
    """Grade the grader: the two grant sets partition the columns, hidden presence, NULL and a leg shape change each fire."""
    cols = [nsn.Col(f"c{i:02d}", "INT", "Int32", "int", ("1", "2")) for i in range(5)]
    sets = split_columns(cols)
    assert {c.name for c in sets["even"]} | {c.name for c in sets["odd"]} == {c.name for c in cols} and not {c.name for c in sets["even"]} & {c.name for c in sets["odd"]}
    assert "GRANT SELECT (id, c00, c02, c04) ON rivet.pm_no_silent_null_g" in " ".join(account_sql("u", TABLE, sets["even"]))
    granted = {"c00"}
    rows = {1: {"c00": 1, "c01": None}, 2: {"c00": 2, "c01": None}, 3: {"c00": None, "c01": None}}
    ok = {"snapshot": ({"c00"}, rows), "stream": ({"c00"}, {STREAM_OFFSET + k: v for k, v in rows.items()})}
    two = cols[:2]
    assert [r.klass for r in grade_columns(two, granted, ok, 3)] == [""] * 4
    nulled = {"snapshot": ({"c00"}, {**rows, 1: {"c00": None}}), "stream": ok["stream"]}
    assert [r.klass for r in grade_columns(two, granted, nulled, 3) if r.klass] == [nsn.NULLED], "a non-NULL value delivered as NULL"
    leaked = {"snapshot": ({"c00", "c01"}, rows), "stream": ok["stream"]}
    assert HIDDEN in [r.klass for r in grade_columns(two, granted, leaked, 3)], "a hidden column present as NULL is a leak of its name"
    assert SHAPE in [r.klass for r in grade_columns(two, granted, leaked, 3)], "the legs must carry the same columns"
    assert nsn.EMPTY in [r.klass for r in grade_columns(two, granted, {"snapshot": ({"c00"}, {1: rows[1]}), "stream": ok["stream"]}, 3)]
    split = nsn.Result("c", nsn.REFUSED, nsn.SPLIT, "check exit 0, run exit 5 [RIVET_SOURCE_CDC_UNDECODABLE]", "RIVET_SOURCE_CDC_UNDECODABLE")
    assert log_refusal(split).klass == "" and log_refusal(split).verdict == nsn.REFUSED
    assert log_refusal(nsn.Result("c", nsn.REFUSED, nsn.UNCODED, "x")).klass == nsn.UNCODED, "an uncoded stop after a grant change stays red"
    print("no_silent_null_grants self-test ok: grant sets partition the columns; NULL, hidden-present, leg-shape and missing-row each fire")


def main() -> int:
    """Developer entry: run the grants cells against the stand and print the failure classes."""
    import argparse

    ap = argparse.ArgumentParser(prog="no_silent_null_grants")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--work", default=str(Path(tempfile.gettempdir()) / "no_silent_null" / "grants"))
    ap.add_argument("--out", default="")
    ns = ap.parse_args()
    if ns.self_test:
        self_test()
        return 0
    t0 = time.time()
    try:
        results = run_grants(Path(ns.work), lambda s: print(s, flush=True))
    except RuntimeError as e:
        results = [nsn.Result(f"{ENGINE}/grants/*", nsn.REFUSED, nsn.NOT_GRADED, str(e)[:300])]
    groups = nsn.summary(ENGINE, results)
    for k, rs in groups.items():
        print(f"{k or 'ok':24} {len(rs)}")
    for k in nsn.FAIL_CLASSES:
        for r in sorted(groups[k], key=lambda r: r.cell)[:14]:
            print(f"  {k}: {r.cell}: {r.detail}")
    print("\n".join([*(nsn.verdict_line(r) for r in sorted(results, key=lambda r: r.cell)), nsn.summary_line(results)]))
    if ns.out:
        Path(ns.out).write_text("\n".join(json.dumps(r.__dict__) for r in results))
    print(f"{len(results)} cells in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
