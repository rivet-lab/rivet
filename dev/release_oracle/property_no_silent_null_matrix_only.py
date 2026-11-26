"""Measured delivery verdict of each ledger row that names no delivery: one seeded column, parquet and csv, graded by a second reader."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import re
import sys
import tempfile
from pathlib import Path
from unittest import mock

from . import core
from . import property_no_silent_null as nsn
from . import property_no_silent_null_cdc as cdc_grid
from . import property_no_silent_null_load as load
from . import type_edges as te
from . import type_matrix as tm

RECEIVER_TARGETS = ("clickhouse", "bigquery")
RECEIVERS = ("duck", *RECEIVER_TARGETS)
SNOWFLAKE_NOT_VERIFIED = "NOT VERIFIED: quota error 090073 (RIVET_MONTHLY_CAP), untouched"
UNGRADED_KLASSES = (tm.UNGRADED_VALUE, tm.UNGRADED_SEED, nsn.NOT_GRADED, nsn.UNGRADED)
ENGINES = ("postgres", "mysql", "mssql", "oracle", "mongo")
UNMEASURED_KLASSES = (nsn.NOT_GRADED, nsn.UNGRADED)
GEOMETRY_GAP = frozenset({"GEOMETRY", "POINT", "LINESTRING", "POLYGON", "GEOMETRYCOLLECTION", "MULTIPOINT"})
GAP_LABEL = "refused today, gap against ADR-0038 CP1, fix in flight"


def picked(native: str, only: str) -> bool:
    """Whether a native type contains any of the comma-separated parts of `only`; an empty `only` keeps every type."""
    return any(o.strip().lower() in native.lower() for o in only.split(","))


def unmeasured(engine: str, only: str = "") -> list[tuple[int, dict]]:
    """The ledger rows of one engine with no delivery and not declarable, keeping their ledger index; `only` keeps the rows whose native type contains any of its comma-separated parts."""
    return [(i, r) for i, r in enumerate(nsn.ledger_rows(engine)) if not r.get("delivery") and not r.get("declarable") and picked(r["native_type"], only)]


def cdc_cols(engine: str, only: str = "") -> list[nsn.Col]:
    """The seed columns of the matrix-only rows for the CDC grid; a row whose edge refuses on the change stream is left out."""
    cols = []
    for i, r in unmeasured(engine, only):
        if r["native_type"].split("(")[0] in GEOMETRY_GAP:
            continue
        try:
            cols.append(nsn.row_cols(engine, i, r))
        except ValueError:
            continue
    return cols


SESSION_OVERRIDE_OPEN = "open: check cannot see a session override"


def cdc_cell(x: nsn.Result) -> dict:
    """One CDC cell as a verdict: a run-side value refusal is refused, a session-override prerequisite is open, DEFECT on any other changed, nulled or uncoded value."""
    run_side_value = x.klass == "CHECK_RUN_SPLIT" and x.code == "RIVET_SOURCE_VALUE_UNREPRESENTABLE"
    session_override = x.klass == "CHECK_RUN_SPLIT" and x.code == "RIVET_SOURCE_CDC_PREREQUISITE" and "@" in x.cell.split("/")[2]
    if run_side_value:
        verdict = f"refused [{x.code}]"
    elif session_override:
        verdict = SESSION_OVERRIDE_OPEN
    elif x.klass in nsn.FAIL_CLASSES and x.klass not in UNMEASURED_KLASSES:
        verdict = "DEFECT"
    elif x.verdict == nsn.REFUSED and x.klass not in UNMEASURED_KLASSES:
        verdict = f"refused [{x.code}]"
    elif x.klass in UNMEASURED_KLASSES:
        verdict = "not classified"
    else:
        verdict = "exact"
    return {"cell": x.cell, "verdict": verdict, "klass": x.klass, "code": x.code, "fidelity": x.fidelity, "detail": x.detail[:240]}


def seed_one(engine: str, url: str, col: nsn.Col) -> int:
    """Create the seed table holding this one column; the row count of the seed."""
    stmts, n = nsn.seed_sql(engine, [col])
    for s in nsn.ledger_setup(engine):
        nsn.sql(engine, url, s)
    for st in stmts:
        p = nsn.sql(engine, url, st + ";")
        if not p.ok:
            raise RuntimeError(f"seed refused `{st[:160]}`: exit {p.returncode}: {p.out.strip()[:300]}")
    return n


ARROW_SCALARS = {"bool": "Boolean", "int16": "Int16", "int32": "Int32", "int64": "Int64", "uint64": "UInt64", "float": "Float32", "double": "Float64", "string": "Utf8", "binary": "Binary", "date32[day]": "Date32", "extension<arrow.json>": "arrow.json", "extension<arrow.uuid>": "arrow.uuid"}
ARROW_UNIT = {"us": "µs", "ns": "ns", "ms": "ms", "s": "s"}


def arrow_label(pa_type: str) -> str:
    """The ledger's delivery spelling of a pyarrow type string; empty for a type the ledger has no label for."""
    if pa_type in ARROW_SCALARS:
        return ARROW_SCALARS[pa_type]
    if m := re.fullmatch(r"decimal(128|256)\((\d+), (\d+)\)", pa_type):
        return f"Decimal{m[1]}({m[2]}, {m[3]})"
    if m := re.fullmatch(r"timestamp\[(\w+)(?:, tz=([^\]]+))?\]", pa_type):
        return f"Timestamp({ARROW_UNIT[m[1]]}, \"{m[2]}\")" if m[2] else f"Timestamp({ARROW_UNIT[m[1]]})"
    if m := re.fullmatch(r"time64\[(\w+)\]", pa_type):
        return f"Time64({ARROW_UNIT[m[1]]})"
    if m := re.fullmatch(r"list<item: (.+)>", pa_type):
        inner = arrow_label(m[1])
        return f"List({inner})" if inner else ""
    return ""


def label(results: list[nsn.Result]) -> dict:
    """The row verdict from its graded cells: DEFECT on any changed, nulled or uncoded value; else the delivered fidelity or the refusal code."""
    cells = [{"cell": x.cell, "verdict": x.verdict, "klass": x.klass, "code": x.code, "fidelity": x.fidelity, "detail": x.detail[:240]} for x in results]
    defects = [x for x in results if x.klass in nsn.FAIL_CLASSES and x.klass not in UNMEASURED_KLASSES]
    if defects:
        return {"verdict": "DEFECT", "reason": "; ".join(f"{x.cell}: {x.klass} {x.detail}" for x in defects)[:600], "cells": cells}
    graded = [x for x in results if "/integrity" not in x.cell]
    if not graded or any(x.klass in UNMEASURED_KLASSES for x in graded):
        return {"verdict": "UNMEASURED", "reason": "a cell could not be graded", "cells": cells}
    delivered = [x for x in graded if x.verdict in (nsn.DELIVERED, nsn.DECLARED)]
    if blind := [x for x in delivered if x.verdict == nsn.DELIVERED and (x.checked == 0 or x.presence)]:
        return {"verdict": "UNMEASURED", "reason": "; ".join(f"{x.cell}: {x.checked} values compared by value, {x.presence} by presence only" for x in blind)[:600], "cells": cells}
    refused = sorted({x.code for x in graded if x.verdict == nsn.REFUSED})
    if delivered and refused:
        return {"verdict": "MIXED", "reason": f"delivered {sorted({x.fidelity for x in delivered})}, refused {refused}", "cells": cells}
    if delivered:
        fids = sorted({x.fidelity or "?" for x in delivered})
        verdict = "deliverable" if fids == ["exact"] else "delivered as text" if fids == ["logical_string"] else f"delivered {'/'.join(fids)}"
        return {"verdict": verdict, "reason": "", "cells": cells}
    if refused and all(refused):
        return {"verdict": "refused by design", "code": refused[0], "reason": "", "cells": cells}
    return {"verdict": "UNMEASURED", "reason": "refused without a code", "cells": cells}


def measure(engine: str, url: str, work: Path, i: int, r: dict) -> dict:
    """One row's measured verdict; NOT CLASSIFIED when its edges cannot be seeded as one column, UNMEASURED when the seed is refused."""
    base = {"engine": engine, "index": i, "native_type": r["native_type"]}
    try:
        col = nsn.row_cols(engine, i, r, extract_only=True)
    except ValueError as e:
        return base | {"verdict": "NOT CLASSIFIED", "reason": str(e)[:300], "cells": []}
    if not col.lits:
        return base | {"verdict": "NOT CLASSIFIED", "reason": "no edge of the row is delivered at extract: nothing to seed", "cells": []}
    try:
        n = seed_one(engine, url, col)
    except RuntimeError as e:
        return base | {"verdict": "UNMEASURED", "reason": f"the seed could not be created: {e}"[:300], "cells": []}
    results = nsn.run_unit(engine, work, url, col, n, "full", "none", ["parquet", "csv"])
    out = label(results)
    if out["verdict"] == "refused by design" and r["native_type"].split("(")[0] in GEOMETRY_GAP:
        out["verdict"] = GAP_LABEL
    arrow = {x.arrow for x in results if x.arrow}
    out["arrow"] = arrow_label(arrow.pop()) if len(arrow) == 1 else ""
    return base | out


def dated(reason) -> bool:
    """Whether a reason opens with an ISO date."""
    return isinstance(reason, str) and re.match(r"\d{4}-\d{2}-\d{2}: \S", reason) is not None


def grade_mongo_row(native: str, docs: dict[int, str]) -> tuple[int, list[str], list[str]]:
    """(values compared, defects, known defects) of one Mongo row over the delivered `document` of every grid document."""
    checked, bad, known = 0, [], []
    for k, items in tm.mongo_grid()[1].items():
        for col, lit, edge in items.values():
            if col.native != native:
                continue
            checked += 1
            kl = tm.klass(edge.x, tm.observe_mongo(col, lit, docs.get(k), edge.x))
            if kl.startswith("DEFECT_") and not kl.startswith("DEFECT_GONE_"):
                known.append(f"{native}[{lit[:40]}]: {kl}")
            elif kl:
                bad.append(f"{native}[{lit[:40]}]: {kl}")
    return checked, bad, known


def measure_mongo(url: str, work: Path, rows: list[tuple[int, dict]]) -> list[dict]:
    """Every Mongo ledger row from one run of the seeded grid collection: each field of the delivered `document` graded by value; a row without a dated `na` is NOT CLASSIFIED."""
    work.mkdir(parents=True, exist_ok=True)
    nsn.mongo_seed(url)
    cfg = work / "c.yaml"
    cfg.write_text(nsn.config("mongo", "RIVET_NSN_URL", "full", "parquet", "document", "none", str(work / "o")))
    p = nsn.rivet(["run", "-c", str(cfg)], {"RIVET_NSN_URL": url}, work)
    docs: dict[int, str] = {}
    why = f"the run exited {p.returncode}: {p.out.strip()[-200:]}"
    if p.ok:
        try:
            for r in nsn.READER(work / "o", "parquet", "")[1]:
                docs[int(json.loads(r["document"])["n"])] = r["document"]
        except Exception as e:
            why = f"the second reader failed: {type(e).__name__}: {e}"[:300]
    out = []
    for i, r in rows:
        base = {"engine": "mongo", "index": i, "native_type": r["native_type"], "cells": []}
        checked, bad, known = grade_mongo_row(r["native_type"], docs)
        if not dated(r.get("na")):
            out.append(base | {"verdict": "NOT CLASSIFIED", "reason": "a Mongo row names its dated `na` reason or nothing classifies it"})
        elif not docs:
            out.append(base | {"verdict": "UNMEASURED", "reason": why})
        elif bad:
            out.append(base | {"verdict": "DEFECT", "reason": "; ".join(bad)[:600]})
        elif not checked:
            out.append(base | {"verdict": "inside document, receivers only", "reason": "no field of the row is in the grid collection"})
        else:
            out.append(base | {"verdict": "inside document", "reason": f"{checked} values compared by value, {len(known)} known defects" if known else f"{checked} values compared by value"})
    return out


def _grade(col: nsn.Col, lits: list[str], rows: dict) -> dict:
    """Grade one parquet run of `col` whose second reader returns `rows` (id -> value) at exact fidelity."""
    d = Path(tempfile.mkdtemp())
    desc = {"id": "INTEGER", col.name: col.native}
    p = core.Proc(["rivet", "run"], 0, "", "", gated=False)
    def reader(out, fmt):
        return desc, {k: {"id": k, col.name.lower(): v} for k, v in rows.items()}, None

    r = nsn.grade_run("postgres", "self/cell", p, p, "exact", d, "parquet", col, len(rows), "none", lits, reader=reader)
    return label([r])


def self_test() -> None:
    """A faithful reader is labelled deliverable; a reader that changes or nulls one value is a DEFECT, so a faked lossy conversion cannot pass."""
    col = nsn.Col("c00", "INTEGER", "", "int", ("5", "-7"))
    lits = ["5", "-7"]
    cases = [
        ("deliverable", {1: 5, 2: -7, 3: None}),
        ("DEFECT", {1: 6, 2: -7, 3: None}),
        ("DEFECT", {1: 5, 2: None, 3: None}),
    ]
    for want, rows in cases:
        got = _grade(col, lits, rows)["verdict"]
        if got != want:
            raise SystemExit(f"self-test: rows {rows} labelled {got!r}, expected {want!r}")
    form = nsn.Col("c01", "INTEGER", "", "int", ("7",), (te.edge_of({"v": "7", "x": "form:8"}),))
    for want, rows in (("deliverable", {1: 8, 2: None}), ("DEFECT", {1: 7, 2: None})):
        got = _grade(form, ["7"], rows)["verdict"]
        if got != want:
            raise SystemExit(f"self-test: a form edge read back as {rows[1]} labelled {got!r}, expected {want!r}")
    for blind in (nsn.Result("c", nsn.DELIVERED, "", "", checked=0), nsn.Result("c", nsn.DELIVERED, "", "", checked=3, presence=1)):
        if label([blind])["verdict"] != "UNMEASURED":
            raise SystemExit(f"self-test: a delivered cell with {blind.checked} values compared by value and {blind.presence} by presence is not UNMEASURED")
    if label([nsn.Result("c", nsn.DELIVERED, "", "", fidelity="exact", checked=3)])["verdict"] != "deliverable":
        raise SystemExit("self-test: a delivered cell with every value compared by value is not deliverable")
    row = {"native_type": "T", "sample": [], "edges": ["1", {"v": "2", "x": "form:two"}, {"v": "3", "x": "refuse:RIVET_X@extract"}, {"v": "4", "x": "null"}]}
    if len(nsn.row_cols("mssql", 0, row, extract_only=True).lits) <= len(nsn.row_cols("mssql", 0, row).lits):
        raise SystemExit("self-test: the batch seed holds no edge beyond the load matrix's, so its values go ungraded")
    seen = [{"native": "T", "target": t, "klass": ""} for t in RECEIVERS]
    wrong = [c | {"klass": nsn.WRONG} if c["target"] == "bigquery" else c for c in seen]
    gone = [c | {"klass": "DEFECT_GONE_X"} if c["target"] == "duck" else c for c in seen]
    known = [c | {"klass": "DEFECT_X"} if c["target"] == "duck" else c for c in seen]
    stuck = [nsn.Result("e/T[1]->clickhouse", nsn.DELIVERED, nsn.NOT_GRADED)]
    cases = [(seen, [], "graded", "graded", "graded"), (seen[:2], [], "graded", "graded", "UNMEASURED"), (wrong, [], "graded", "graded", f"DEFECT {nsn.WRONG}"),
             (gone, [], "DEFECT DEFECT_GONE_X", "graded", "graded"), (known, [], "graded, known DEFECT_X", "graded", "graded"), (seen, stuck, "UNMEASURED", "UNMEASURED", "UNMEASURED"),
             ([], [nsn.Result("e/*/matrix", nsn.REFUSED, nsn.NOT_GRADED)], "UNMEASURED", "UNMEASURED", "UNMEASURED"),
             (seen, [nsn.Result("e/T->none/load-clickhouse", nsn.REFUSED, nsn.SPLIT)], "graded", f"graded, known {nsn.SPLIT}", "graded"),
             (seen, [nsn.Result("e/T->none/load-bigquery", nsn.DELIVERED, nsn.WRONG)], "graded", "graded", f"DEFECT {nsn.WRONG}")]
    for sn, rs, *want in cases:
        got = receiver_verdicts("e", "T", sn, rs)
        if [got[t] for t in RECEIVERS] != want or "NOT VERIFIED" not in got["snowflake"]:
            raise SystemExit(f"self-test: receiver verdicts {got} for {[c['klass'] for c in sn]} and {[r.klass for r in rs]}, expected {want}")
    for rows, want in (([{"verdict": "deliverable", "receivers": {"duck": "graded", "bigquery": "UNMEASURED"}}], 2), ([{"verdict": "deliverable", "receivers": {"duck": "DEFECT WRONG_VALUE"}}], 1),
                       ([{"verdict": "deliverable", "receivers": {"duck": "graded", "snowflake": SNOWFLAKE_NOT_VERIFIED}}], 0)):
        if exit_code(rows) != want:
            raise SystemExit(f"self-test: rows {rows} exit {exit_code(rows)}, expected {want}")
    from bson import json_util
    wired = {k + 1: json_util.dumps(d, json_options=json_util.RELAXED_JSON_OPTIONS) for k, d in enumerate(tm.mongo_grid()[0][:-1])}
    n_ok, bad_ok, _ = grade_mongo_row("double", wired)
    first = next(k for k, items in tm.mongo_grid()[1].items() if any(c.native == "double" for c, _, _ in items.values()))
    lost = {**wired, first: json_util.dumps({k: v for k, v in tm.mongo_grid()[0][first - 1].items() if not k.startswith("c")}, json_options=json_util.RELAXED_JSON_OPTIONS)}
    if not n_ok or bad_ok or not grade_mongo_row("double", lost)[1]:
        raise SystemExit(f"self-test: a faithful Mongo document grades {bad_ok} over {n_ok} values, or a document that lost its fields grades clean")
    if not (picked("CLOB", "BLOB,CLOB") and picked("NCLOB", "blob, clob") and picked("RAW", "")) or picked("RAW(2000)", "BLOB,CLOB"):
        raise SystemExit("self-test: --only keeps a row naming any of its comma-separated parts and no other")
    if dated("no date") or not dated("2026-11-22: a reason"):
        raise SystemExit("self-test: a Mongo reason is dated when it opens with an ISO date")
    cols = cdc_cols("postgres")
    planned = {c.name for group, _, _ in cdc_grid.rounds("postgres", cols) for c in group}
    if not cols or planned != {c.name for c in cols}:
        raise SystemExit(f"self-test: the CDC plan holds {len(planned)} of {len(cols)} matrix-only columns")
    if cdc_cell(nsn.Result("x", nsn.DELIVERED, nsn.NULLED, ""))["verdict"] != "DEFECT":
        raise SystemExit("self-test: a nulled CDC value is not a DEFECT")
    value_refusal = nsn.Result("mysql/T->d/cdc-insert/parquet", nsn.REFUSED, "CHECK_RUN_SPLIT", "", "RIVET_SOURCE_VALUE_UNREPRESENTABLE")
    if cdc_cell(value_refusal)["verdict"] != "refused [RIVET_SOURCE_VALUE_UNREPRESENTABLE]":
        raise SystemExit("self-test: a run-side value refusal after a passing check is not refused")
    session = nsn.Result("mysql/T->none/cdc-delete@MINIMAL/parquet", nsn.REFUSED, "CHECK_RUN_SPLIT", "", "RIVET_SOURCE_CDC_PREREQUISITE")
    if cdc_cell(session)["verdict"] != "open: check cannot see a session override":
        raise SystemExit("self-test: a session-override partial image is not its own open class")
    unscoped = nsn.Result("mysql/T->none/cdc-delete/parquet", nsn.REFUSED, "CHECK_RUN_SPLIT", "", "RIVET_SOURCE_CDC_PREREQUISITE")
    if cdc_cell(unscoped)["verdict"] != "DEFECT":
        raise SystemExit("self-test: a prerequisite refusal without a session override is not a DEFECT")
    for pa_type, want in (("int32", "Int32"), ("decimal128(38, 10)", "Decimal128(38, 10)"), ("timestamp[us, tz=UTC]", 'Timestamp(µs, "UTC")'), ("string", "Utf8"), ("list<item: string>", "List(Utf8)"), ("extension<arrow.json>", "arrow.json"), ("struct<a: int32>", "")):
        if arrow_label(pa_type) != want:
            raise SystemExit(f"self-test: arrow type {pa_type!r} labelled {arrow_label(pa_type)!r}, expected {want!r}")
    for rows, want in (([], 2), ([{"verdict": "UNMEASURED"}], 2), ([{"verdict": "deliverable"}, {"verdict": "NOT CLASSIFIED"}], 2),
                       ([{"verdict": "deliverable"}, {"verdict": "DEFECT"}], 1), ([{"verdict": "deliverable"}, {"verdict": "refused by design"}], 0)):
        if exit_code(rows) != want:
            raise SystemExit(f"self-test: rows {rows} exit {exit_code(rows)}, expected {want}")
    for cell, want in ((nsn.Result("c", nsn.DELIVERED), 0), (nsn.Result("c", nsn.DELIVERED, nsn.NULLED, "x"), 1), (nsn.Result("c", nsn.REFUSED, nsn.NOT_GRADED, "x"), 2)):
        with mock.patch.object(cdc_grid, "run_engine", lambda *a, _c=cell, **k: [_c]), contextlib.redirect_stdout(io.StringIO()):
            got = run_cdc(["postgres"], Path(tempfile.gettempdir()))
        if got != want:
            raise SystemExit(f"self-test: the CDC run exits {got} for a {cell.klass or 'passing'} cell, expected {want}")
    print("self-test ok: a faithful run is deliverable; a changed or nulled value is DEFECT; every matrix-only column reaches the CDC plan")


def run_cdc(engines: list[str], work: Path, only: str = "") -> int:
    """The CDC grid over the matrix-only columns of each engine on its CDC stand; exit 1 on any DEFECT, 2 on a not-classified cell."""
    cells: list[dict] = []
    for engine in engines:
        if engine == "mongo":
            cells += [{"cell": f"mongo/{r['native_type']}/cdc", "verdict": "n/a" if dated(r.get("na")) else "not classified", "reason": str(r.get("na", ""))[:120]} for _, r in unmeasured(engine, only)]
            continue
        for x in cdc_grid.run_engine(engine, work / "cdc" / engine, print, cols=cdc_cols(engine, only)):
            cells.append(cdc_cell(x))
            print(json.dumps(cells[-1], ensure_ascii=False), flush=True)
    print("SUMMARY " + json.dumps(tally([c["verdict"] for c in cells]), sort_keys=True), flush=True)
    return exit_code(cells, UNMEASURED_CELLS)


def receiver_verdicts(engine: str, native: str, seen: list[dict], results: list[nsn.Result]) -> dict[str, str]:
    """The verdict per receiver of one ledger row from the load matrix's observations: graded when every value read back holds, DEFECT on one that does not, UNMEASURED when nothing was read back or a cell could not be graded."""
    stuck = any(x.klass in UNGRADED_KLASSES and (x.cell.startswith((f"{engine}/{native}->", f"{engine}/{native}[")) or x.cell == f"{engine}/*/matrix") for x in results)
    out = {}
    for t in RECEIVERS:
        cells = [c for c in seen if c["native"] == native and c["target"] == t]
        klasses = {c["klass"] for c in cells} | {x.klass for x in results if x.cell.startswith(f"{engine}/{native}->") and x.cell.endswith(f"load-{t}")}
        klasses -= {""}
        if not cells or stuck or klasses & set(UNGRADED_KLASSES):
            out[t] = "UNMEASURED"
        elif bad := sorted(k for k in klasses if not (k.startswith("DEFECT_") or k == nsn.SPLIT) or k.startswith("DEFECT_GONE_")):
            out[t] = f"DEFECT {bad[0]}"
        else:
            out[t] = "graded" if not klasses else f"graded, known {sorted(klasses)[0]}"
    return out | {"snowflake": SNOWFLAKE_NOT_VERIFIED}


def receiver_leg(engine: str, url: str, work: Path, rows: list[dict]) -> None:
    """Load every ledger row of the engine into DuckDB, ClickHouse and BigQuery and attach the verdict of the values read back to each measured row."""
    results, seen = load.run_engine(engine, url, work / "receivers", RECEIVER_TARGETS, print)
    for row in rows:
        row["receivers"] = receiver_verdicts(engine, row["native_type"], seen, results)
        print(json.dumps({"engine": engine, "native_type": row["native_type"], "receivers": row["receivers"]}, ensure_ascii=False), flush=True)


def run_engines(engines: list[str], work: Path, only: str = "") -> list[dict]:
    """Measure every matrix-only ledger row of the engines on the stand, printing each row as one JSON line."""
    rows: list[dict] = []
    for engine in engines:
        url = nsn.STAND[engine]
        mine: list[dict] = []
        try:
            if engine == "mongo":
                mine = measure_mongo(url, work / engine, unmeasured(engine, only))
                for row in mine:
                    print(json.dumps(row, ensure_ascii=False), flush=True)
            for i, r in unmeasured(engine, only) if engine != "mongo" else []:
                row = measure(engine, url, work / engine, i, r)
                mine.append(row)
                print(json.dumps(row, ensure_ascii=False), flush=True)
            receiver_leg(engine, url, work / engine, mine)
            rows += mine
        finally:
            nsn.cleanup(engine, url)
    return rows


UNMEASURED_ROWS = frozenset({"UNMEASURED", "NOT CLASSIFIED"})
UNMEASURED_CELLS = frozenset({"not classified"})


def tally(verdicts: list[str]) -> dict[str, int]:
    """How many times each verdict occurs."""
    return {v: verdicts.count(v) for v in sorted(set(verdicts))}


def exit_code(rows: list[dict], unmeasured_verdicts: frozenset[str] = UNMEASURED_ROWS) -> int:
    """1 on any DEFECT, 2 when nothing was measured or any entry carries an unmeasured verdict, else 0."""
    verdicts = {row["verdict"] for row in rows} | {v for row in rows for v in row.get("receivers", {}).values()}
    if any(v == "DEFECT" or v.startswith("DEFECT ") for v in verdicts):
        return 1
    return 2 if not rows or verdicts & unmeasured_verdicts else 0


def main() -> int:
    """Developer entry: measure every matrix-only ledger row of the chosen engines on the stand."""
    ap = argparse.ArgumentParser(prog="property_no_silent_null_matrix_only")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--cdc", action="store_true", help="run the CDC grid over the matrix-only columns instead of the batch rows")
    ap.add_argument("--engine", default=",".join(ENGINES))
    ap.add_argument("--work", default=str(Path(tempfile.gettempdir()) / "no_silent_null_matrix_only"))
    ap.add_argument("--only", default="", help="only the rows whose native type contains any of these comma-separated texts")
    ap.add_argument("--out", default="")
    ns = ap.parse_args()
    if ns.self_test:
        self_test()
        return 0
    print(nsn.binary_line(), flush=True)
    if ns.cdc:
        return run_cdc(ns.engine.split(","), Path(ns.work), ns.only)
    rows = run_engines(ns.engine.split(","), Path(ns.work), ns.only)
    print("SUMMARY " + json.dumps(tally([row["verdict"] for row in rows]), sort_keys=True), flush=True)
    if ns.out:
        Path(ns.out).write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in rows) + "\n")
    return exit_code(rows)


if __name__ == "__main__":
    sys.exit(main())
