"""P5 oracle: native listings and a whole-directory DuckDB glob grade a stopped run and its re-run, never rivet's manifests or `state show`."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
from collections import Counter
from contextlib import closing

import duckdb

PASS, RED, NOT_GRADED = "PASS", "RED", "NOT_GRADED"
CLASSES = (
    "debris:tmp",
    "debris:unowned_file",
    "debris:success_without_manifest",
    "debris:running_row",
    "debris:lease_row",
    "rows:missing",
    "rows:extra",
    "rows:duplicate",
    "rows:vs_clean",
)
OWNED_NAMES = ("manifest.json", "_SUCCESS", ".rivet_doctor_probe")


def owned(rel: str) -> bool:
    """Whether `rel` is a file rivet itself writes beside the parts: a manifest (canonical or per run), the marker, the doctor probe, the quarantine tree."""
    base = os.path.basename(rel)
    run_manifest = base.startswith("manifest-") and base.endswith(".json")
    return base in OWNED_NAMES or run_manifest or "_quarantine" in rel.split(os.sep)


class ShapeError(Exception):
    """An oracle answered in a shape the contract does not define."""


def walk(out_dir: str) -> dict[str, int]:
    """Every file under `out_dir` (relative path -> size) by a plain directory walk."""
    found = {}
    for root, _dirs, names in os.walk(out_dir):
        for n in names:
            p = os.path.join(root, n)
            found[os.path.relpath(p, out_dir)] = os.path.getsize(p)
    return found


def _sqlite_count(path: str, sql: str, args: tuple) -> int:
    """One COUNT over the SQLite state; a state or table not created yet counts zero."""
    if not os.path.exists(path):
        return 0
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as con:
        try:
            return con.execute(sql, args).fetchone()[0]
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                return 0
            raise


def _pg_count(url: str, sql: str, args: tuple) -> int:
    """One COUNT over the Postgres state through DuckDB's postgres scanner."""
    lit = sql
    for a in args:
        lit = lit.replace("?", "'" + str(a).replace("'", "''") + "'", 1)
    with closing(duckdb.connect()) as con:
        con.execute("LOAD postgres")
        con.execute("ATTACH ? AS pg (TYPE postgres, READ_ONLY)", [url])
        return con.execute("SELECT * FROM postgres_query('pg', ?)", [lit]).fetchone()[0]


RUNNING_SQL = "SELECT COUNT(*) FROM run_status WHERE status = 'running' AND export_name = ?"
LEASE_SQL = "SELECT COUNT(*) FROM state_lease WHERE lease_key = ?"


def state_counts(spec: dict) -> tuple[int, int]:
    """`running` run_status rows of the export and lease rows of its key, by raw SQL on the state tables."""
    state = spec["state"]
    kind = state["kind"]
    if kind == "sqlite":
        count = lambda sql, a: _sqlite_count(state["path"], sql, a)  # noqa: E731
    elif kind == "postgres":
        count = lambda sql, a: _pg_count(state["url"], sql, a)  # noqa: E731
    else:
        raise ShapeError(f"unknown state kind {kind!r}")
    return count(RUNNING_SQL, (spec["export"],)), count(LEASE_SQL, (spec["lease_key"],))


def snapshot(spec: dict) -> dict:
    """The destination listing and the state rows of one moment."""
    running, leases = state_counts(spec)
    return {"files": walk(spec["out_dir"]), "running": running, "leases": leases}


def glob_ids(out_dir: str, id_column: str) -> list:
    """Every id the whole output directory holds, read by one DuckDB glob over all its Parquet files."""
    if not any(n.endswith(".parquet") for n in walk(out_dir)):
        return []
    with closing(duckdb.connect()) as con:
        pattern = os.path.join(out_dir, "**", "*.parquet")
        rel = con.execute("SELECT * FROM read_parquet(?, union_by_name = true)", [pattern])
        names = [d[0] for d in rel.description]
        col = next((n for n in names if n.lower() == id_column.lower()), None)
        if col is None:
            raise ShapeError(f"no column {id_column!r} in {names}")
        return [r[0] for r in con.execute(f'SELECT "{col}" FROM read_parquet(?, union_by_name = true)', [pattern]).fetchall()]


def native_oracle(cell: dict) -> list[dict]:
    """The findings on a settled cell: debris in the listings and state, and the row set against the source."""
    out = []
    before = cell["before"]["files"]
    after = snapshot(cell)
    files = after["files"]
    for rel in sorted(files):
        if rel.endswith(".tmp"):
            out.append({"class": "debris:tmp", "detail": rel})
        elif rel not in before and not rel.endswith(".parquet") and not owned(rel):
            out.append({"class": "debris:unowned_file", "detail": rel})
    for rel in sorted(files):
        if os.path.basename(rel) == "_SUCCESS":
            manifest = os.path.join(os.path.dirname(rel), "manifest.json")
            if manifest not in files:
                out.append({"class": "debris:success_without_manifest", "detail": rel})
    if after["running"]:
        out.append({"class": "debris:running_row", "detail": f"{after['running']} running run_status row(s)"})
    if after["leases"]:
        out.append({"class": "debris:lease_row", "detail": f"{after['leases']} lease row(s)"})
    ids = glob_ids(cell["out_dir"], cell["id_column"])
    want = set(cell["expected_ids"])
    seen = Counter(ids)
    missing, extra = sorted(want - set(seen)), sorted(set(seen) - want)
    dup = sorted(i for i, n in seen.items() if n > 1)
    if missing:
        out.append({"class": "rows:missing", "detail": f"{len(missing)} id(s), e.g. {missing[:5]}"})
    if extra:
        out.append({"class": "rows:extra", "detail": f"{len(extra)} id(s), e.g. {extra[:5]}"})
    if dup:
        out.append({"class": "rows:duplicate", "detail": f"{len(dup)} id(s) twice or more, e.g. {dup[:5]}; {len(ids)} rows for {len(want)} source ids"})
    clean = cell.get("clean_ids")
    if clean is not None and sorted(clean) != sorted(ids):
        out.append({"class": "rows:vs_clean", "detail": f"{len(ids)} rows vs {len(clean)} from the uninterrupted run"})
    return out


def checked(findings) -> list[dict]:
    """`findings` if it is a list of known finding classes, else a ShapeError."""
    if not isinstance(findings, list):
        raise ShapeError(f"findings is a {type(findings).__name__}, not a list")
    for f in findings:
        if not isinstance(f, dict) or f.get("class") not in CLASSES or not isinstance(f.get("detail"), str):
            raise ShapeError(f"unknown finding {f!r}")
    return findings


def _write_parquet(path: str, ids: list) -> None:
    """A Parquet file of one `id` column."""
    with closing(duckdb.connect()) as con:
        con.execute("CREATE TABLE t(id BIGINT)")
        con.executemany("INSERT INTO t VALUES (?)", [(i,) for i in ids])
        con.execute(f"COPY t TO '{path}' (FORMAT parquet)")


def _fixture(root: str, debris: bool) -> dict:
    """A cell whose output directory is clean, or carries one planted instance of every debris and row class."""
    out = os.path.join(root, "out_dirty" if debris else "out_clean")
    os.makedirs(out)
    db = os.path.join(root, "dirty.db" if debris else "clean.db")
    with closing(sqlite3.connect(db)) as con:
        con.execute("CREATE TABLE run_status (run_id TEXT, export_name TEXT, status TEXT)")
        con.execute("CREATE TABLE state_lease (lease_key TEXT, holder TEXT, expires_at TEXT)")
        if debris:
            con.execute("INSERT INTO run_status VALUES ('r_1', 'e', 'running')")
            con.execute("INSERT INTO state_lease VALUES ('k', 'h', 'x')")
        con.commit()
    before = {"files": walk(out), "running": 0, "leases": 0}
    _write_parquet(os.path.join(out, "a.parquet"), [1, 2, 3] if not debris else [1, 2, 2, 9])
    if debris:
        open(os.path.join(out, ".a.1.2.tmp"), "w").close()
        open(os.path.join(out, "stray.bin"), "w").close()
        open(os.path.join(out, "_SUCCESS"), "w").close()
    else:
        open(os.path.join(out, "manifest.json"), "w").close()
        open(os.path.join(out, "manifest-r_1_20260101T000000.000_7.json"), "w").close()
        open(os.path.join(out, ".rivet_doctor_probe"), "w").close()
        os.makedirs(os.path.join(out, "_quarantine"))
        open(os.path.join(out, "_quarantine", "p.parquet.bad"), "w").close()
        open(os.path.join(out, "_SUCCESS"), "w").close()
    return {
        "out_dir": out,
        "state": {"kind": "sqlite", "path": db},
        "export": "e",
        "lease_key": "k",
        "expected_ids": [1, 2, 3],
        "id_column": "id",
        "clean_ids": [1, 2, 3],
        "before": before,
    }


def grade_with(oracle, cell: dict) -> dict:
    """PASS, RED or NOT_GRADED for `cell`: an oracle that crashes, answers a strange shape, flags nothing planted or flags a clean control is NOT_GRADED."""
    with tempfile.TemporaryDirectory() as root:
        try:
            control = checked(oracle(_fixture(root, False)))
            planted = {f["class"] for f in checked(oracle(_fixture(root, True)))}
        except Exception as e:  # noqa: BLE001
            return {"verdict": NOT_GRADED, "canary_flagged": False, "evidence": f"oracle failed on its canary: {type(e).__name__}: {e}"}
    blind = sorted(set(CLASSES) - planted)
    if blind or control:
        why = f"blind to {blind}" if blind else f"flagged a clean control: {[f['class'] for f in control]}"
        return {"verdict": NOT_GRADED, "canary_flagged": False, "evidence": f"oracle canary: {why}"}
    try:
        found = checked(oracle(cell))
    except Exception as e:  # noqa: BLE001
        return {"verdict": NOT_GRADED, "canary_flagged": True, "evidence": f"oracle failed: {type(e).__name__}: {e}"}
    if found:
        text = "; ".join(f"{f['class']} {f['detail']}" for f in found)
        return {"verdict": RED, "canary_flagged": True, "evidence": text[:600], "classes": sorted({f["class"] for f in found})}
    return {"verdict": PASS, "canary_flagged": True, "evidence": f"{len(cell['expected_ids'])} ids once each, no debris", "classes": []}


def self_test() -> None:
    """The native oracle tells a clean cell from a dirty one; an always-OK, a crashing and a strange-shaped oracle never give PASS."""
    with tempfile.TemporaryDirectory() as root:
        clean, dirty = _fixture(root, False), _fixture(root, True)
        assert grade_with(native_oracle, clean)["verdict"] == PASS, grade_with(native_oracle, clean)
        got = grade_with(native_oracle, dirty)
        assert got["verdict"] == RED, got
        missing = set(CLASSES) - set(got["classes"])
        assert not missing, f"the native oracle misses planted {missing}"

        def crash(_cell):
            raise RuntimeError("oracle crashed")

        strange = lambda _cell: {"ok": True}  # noqa: E731
        real = {**clean, "export": "real"}

        def crash_on_the_real_cell(cell):
            if cell["export"] == "real":
                raise RuntimeError("oracle crashed on the cell")
            return native_oracle(cell)

        always_red = lambda _cell: [{"class": c, "detail": "x"} for c in CLASSES]  # noqa: E731
        assert grade_with(crash_on_the_real_cell, real)["verdict"] == NOT_GRADED
        assert grade_with(always_red, real)["verdict"] == NOT_GRADED
        for name, fake in (("always-ok", lambda _cell: []), ("crash", crash), ("strange-shape", strange)):
            for cell in (clean, dirty):
                v = grade_with(fake, cell)["verdict"]
                assert v in (NOT_GRADED, RED), f"{name} oracle gave {v}"
                assert v != PASS
    print("no_debris self-test: ok")


def main(argv: list[str]) -> int:
    """`snapshot` and `grade` read a JSON spec on stdin and print one JSON answer; `rows` lists the ids of a directory."""
    if argv[:1] == ["--self-test"]:
        self_test()
        return 0
    verb = argv[0]
    spec = json.load(sys.stdin)
    if verb == "snapshot":
        out = snapshot(spec)
    elif verb == "rows":
        out = {"ids": glob_ids(spec["out_dir"], spec["id_column"])}
    elif verb == "grade":
        out = grade_with(native_oracle, spec)
    else:
        raise SystemExit(f"unknown verb {verb!r}")
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
