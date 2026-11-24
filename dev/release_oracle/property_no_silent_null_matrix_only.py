"""Measured delivery verdict of each ledger row that names no delivery: one seeded column, parquet and csv, graded by a second reader."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from pathlib import Path

from . import core
from . import property_no_silent_null as nsn
from . import property_no_silent_null_cdc as cdc_grid

UNMEASURED_KLASSES = (nsn.NOT_GRADED, nsn.UNGRADED)
GEOMETRY_GAP = frozenset({"GEOMETRY", "POINT", "LINESTRING", "POLYGON", "GEOMETRYCOLLECTION", "MULTIPOINT"})
GAP_LABEL = "refused today, gap against ADR-0038 CP1, fix in flight"


def unmeasured(engine: str) -> list[tuple[int, dict]]:
    """The ledger rows of one engine with no delivery and not declarable, keeping their ledger index."""
    return [(i, r) for i, r in enumerate(nsn.ledger_rows(engine)) if not r.get("delivery") and not r.get("declarable")]


def cdc_cols(engine: str) -> list[nsn.Col]:
    """The seed columns of the matrix-only rows for the CDC grid; a row whose edge refuses on the change stream is left out."""
    cols = []
    for i, r in unmeasured(engine):
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


ARROW_SCALARS = {"bool": "Boolean", "int16": "Int16", "int32": "Int32", "int64": "Int64", "uint64": "UInt64", "float": "Float32", "double": "Float64", "string": "Utf8", "binary": "Binary", "date32[day]": "Date32"}
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
        col = nsn.row_cols(engine, i, r)
    except ValueError as e:
        return base | {"verdict": "NOT CLASSIFIED", "reason": str(e)[:300], "cells": []}
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
    for pa_type, want in (("int32", "Int32"), ("decimal128(38, 10)", "Decimal128(38, 10)"), ("timestamp[us, tz=UTC]", 'Timestamp(µs, "UTC")'), ("string", "Utf8"), ("list<item: string>", "List(Utf8)"), ("struct<a: int32>", "")):
        if arrow_label(pa_type) != want:
            raise SystemExit(f"self-test: arrow type {pa_type!r} labelled {arrow_label(pa_type)!r}, expected {want!r}")
    print("self-test ok: a faithful run is deliverable; a changed or nulled value is DEFECT; every matrix-only column reaches the CDC plan")


def run_cdc(engines: list[str], work: Path) -> int:
    """The CDC grid over the matrix-only columns of each engine on its CDC stand; exit 1 on any DEFECT."""
    cells: list[dict] = []
    for engine in engines:
        for x in cdc_grid.run_engine(engine, work / "cdc" / engine, print, cols=cdc_cols(engine)):
            cells.append(cdc_cell(x))
            print(json.dumps(cells[-1], ensure_ascii=False), flush=True)
    tally: dict[str, int] = {}
    for c in cells:
        tally[c["verdict"]] = tally.get(c["verdict"], 0) + 1
    print("SUMMARY " + json.dumps(tally, sort_keys=True), flush=True)
    return 1 if tally.get("DEFECT") else 0


def main() -> int:
    """Developer entry: measure every matrix-only ledger row of the chosen engines on the stand; exit 1 on any DEFECT."""
    ap = argparse.ArgumentParser(prog="property_no_silent_null_matrix_only")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--cdc", action="store_true", help="run the CDC grid over the matrix-only columns instead of the batch rows")
    ap.add_argument("--engine", default="postgres,mysql")
    ap.add_argument("--work", default=str(Path(tempfile.gettempdir()) / "no_silent_null_matrix_only"))
    ap.add_argument("--out", default="")
    ns = ap.parse_args()
    if ns.self_test:
        self_test()
        return 0
    print(nsn.binary_line(), flush=True)
    if ns.cdc:
        return run_cdc(ns.engine.split(","), Path(ns.work))
    rows: list[dict] = []
    for engine in ns.engine.split(","):
        url = nsn.STAND[engine]
        try:
            for i, r in unmeasured(engine):
                row = measure(engine, url, Path(ns.work) / engine, i, r)
                rows.append(row)
                print(json.dumps(row, ensure_ascii=False), flush=True)
        finally:
            nsn.cleanup(engine, url)
    tally: dict[str, int] = {}
    for row in rows:
        tally[row["verdict"]] = tally.get(row["verdict"], 0) + 1
    print("SUMMARY " + json.dumps(tally, sort_keys=True), flush=True)
    if ns.out:
        Path(ns.out).write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in rows) + "\n")
    return 1 if tally.get("DEFECT") else 0


if __name__ == "__main__":
    sys.exit(main())
