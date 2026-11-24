"""Measured delivery verdict of each ledger row that names no delivery: one seeded column, parquet and csv, graded by a second reader."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

from . import core
from . import property_no_silent_null as nsn

UNMEASURED_KLASSES = (nsn.NOT_GRADED, nsn.UNGRADED)


def unmeasured(engine: str) -> list[tuple[int, dict]]:
    """The ledger rows of one engine with no delivery and not declarable, keeping their ledger index."""
    return [(i, r) for i, r in enumerate(nsn.ledger_rows(engine)) if not r.get("delivery") and not r.get("declarable")]


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
    return base | label(nsn.run_unit(engine, work, url, col, n, "full", "none", ["parquet", "csv"]))


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
    print("self-test ok: a faithful run is deliverable; a changed or nulled value is DEFECT")


def main() -> int:
    """Developer entry: measure every matrix-only ledger row of the chosen engines on the stand; exit 1 on any DEFECT."""
    ap = argparse.ArgumentParser(prog="property_no_silent_null_matrix_only")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--engine", default="postgres,mysql")
    ap.add_argument("--work", default=str(Path(tempfile.gettempdir()) / "no_silent_null_matrix_only"))
    ap.add_argument("--out", default="")
    ns = ap.parse_args()
    if ns.self_test:
        self_test()
        return 0
    print(nsn.binary_line(), flush=True)
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
