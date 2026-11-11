"""Cloud legs of the property matrices: warehouse and object-store cells graded offline from recorded server answers and against a real test project."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import urllib.parse
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

import yaml

from . import cloud_replay as cr
from . import core, engines
from . import property_no_silent_null as nsn
from .core import Ledger, SkipCause
from .property_no_silent_null import NOT_GRADED, Result

ROOT = nsn.ROOT
RECORDINGS = ROOT / "dev" / "cloud_legs" / "recordings"
PG_URL = nsn.STAND["postgres"]
CANARY = "ya29.rivetCanaryTokenMustNeverBeEchoed0123456789"
CLOUD_ENV = ("BIGQUERY_TEST_PROJECT", "RIVET_TEST_GCS_BUCKET", "BIGQUERY_TEST_LOCATION")
PROPS = ("no_silent_null_cloud", "no_secret_in_output_cloud", "check_equals_run_cloud", "no_debris_after_stop_cloud")
TIERS = ("offline", "cloud")
COLUMN_DROPPED, COLUMN_TYPE, PART_NOT_LOADED, SECRET_LEAK, SPLIT, DEBRIS, UNREACHED = (
    "COLUMN_DROPPED", "COLUMN_TYPE", "PART_NOT_LOADED", "SECRET_LEAK", "CHECK_RUN_SPLIT", "DEBRIS", "PATH_NOT_REACHED")
SEED = (
    "CREATE TABLE {t}(id int primary key, n bigint, f double precision, d numeric(20,6), s text, b boolean, ts timestamp, dt date)",
    "INSERT INTO {t} VALUES (1,9007199254740993,1.5,12345.678901,'héllo',true,'2026-11-01 10:20:30.123456','2026-11-01'),"
    "(2,NULL,NULL,NULL,NULL,NULL,NULL,NULL),(3,-5,-0.25,-0.000001,'x',false,'1999-12-31 23:59:59','1999-12-31')",
)
EXPECTED = [
    {"id": 1, "n": 9007199254740993, "f": 1.5, "d": Decimal("12345.678901"), "s": "héllo", "b": True, "ts": "2026-11-01T10:20:30.123456", "dt": "2026-11-01"},
    {"id": 2, "n": None, "f": None, "d": None, "s": None, "b": None, "ts": None, "dt": None},
    {"id": 3, "n": -5, "f": -0.25, "d": Decimal("-0.000001"), "s": "x", "b": False, "ts": "1999-12-31T23:59:59", "dt": "1999-12-31"},
]
BQ_TYPE = {"int64": "INT64", "int32": "INT64", "double": "FLOAT64", "float": "FLOAT64", "string": "STRING", "large_string": "STRING",
           "bool": "BOOL", "date32[day]": "DATE", "binary": "BYTES"}


@dataclass(frozen=True)
class Leg:
    """One cloud leg: the store a warehouse reads, and the reason a tier cannot grade it ('' = graded)."""

    role: str
    store: str = ""
    offline: str = ""
    cloud: str = ""


R_SF_ENDPOINT = ("the Snowflake endpoint is derived from the account and has no override, so a recorded answer cannot be replayed into rivet "
                  "without a src/ hook (RIVET_SF_API_ENDPOINT mirroring RIVET_BQ_API_ENDPOINT)")
R_SF_CLOUD = "no Snowflake test account is granted to this matrix's environment (no `snow` connection or key found)"
R_NO_RECORDING = "no recording: one real run against the store has never been made because no test bucket and credential exist"
R_NO_CREDS = "no cloud credentials: the store has no test bucket or credential in the registry"
R_SECRET_MANAGER = "the Secret Manager success path has no endpoint override (const ENDPOINT in src/config/gcp_secret.rs) and no test secret exists in the project"
WAREHOUSES = {
    "bigquery": Leg("warehouse", store="gcs"),
    "snowflake": Leg("warehouse", store="gcs", offline=R_SF_ENDPOINT, cloud=R_SF_CLOUD),
    "clickhouse": Leg("not_cloud"),
}
STORES = {
    "local": Leg("not_cloud"),
    "s3": Leg("store", offline=R_NO_RECORDING, cloud=R_NO_CREDS),
    "gcs": Leg("store"),
    "azure": Leg("store", offline=R_NO_RECORDING, cloud=R_NO_CREDS),
    "stdout": Leg("not_cloud"),
}
CELLS = {
    "warehouse": {"no_silent_null_cloud": ("typed-columns",), "no_secret_in_output_cloud": ("load-success", "load-refused", "load-hostile"),
                  "check_equals_run_cloud": ("check-vs-load-healthy", "check-vs-load-refused"), "no_debris_after_stop_cloud": ("refused-load-leaves-nothing",)},
    "store": {"no_silent_null_cloud": ("staged-bytes",), "no_secret_in_output_cloud": ("run-output",), "no_debris_after_stop_cloud": ("staging-set-is-manifest",)},
}
SECRET_MANAGER_CELL = ("no_secret_in_output_cloud", "gcp_secret/resolved-value-not-echoed")


def axes() -> dict:
    """The warehouses and stores read from the product enums; a variant with no leg entry is an error."""
    wh = nsn._enum_variants("src/config/load.rs", "LoadTarget")
    st = nsn._enum_variants("src/config/destination.rs", "DestinationType")
    for found, table, what in ((wh, WAREHOUSES, "LoadTarget"), (st, STORES, "DestinationType")):
        if set(found) != set(table):
            raise AssertionError(f"{what} variants {sorted(found)} differ from the cloud legs {sorted(table)}: a new variant needs a leg entry")
    return {"warehouses": wh, "stores": st}


def legs() -> list[tuple[str, str, Leg]]:
    """Every cloud (kind, name, leg) in the product's enum order; a destination or target that is not a cloud leg carries no cell."""
    a = axes()
    out = [("warehouse", w, WAREHOUSES[w]) for w in a["warehouses"]] + [("store", s, STORES[s]) for s in a["stores"]]
    for kind, name, leg in out:
        if leg.role not in (kind, "not_cloud"):
            raise AssertionError(f"{kind} {name}: unknown leg role {leg.role}")
    return [(kind, name, leg) for kind, name, leg in out if leg.role == kind]


def cell_name(tier: str, name: str, cell: str) -> str:
    """`tier/leg/cell`, the name on a verdict line."""
    return f"{tier}/{name}/{cell}"


def ok(cell: str, detail: str = "") -> Result:
    """A passing cell."""
    return Result(cell, "ok", "", detail)


def red(cell: str, klass: str, detail: str) -> Result:
    """A cell that failed in `klass`."""
    return Result(cell, "bad", klass, detail)


def not_graded(cell: str, reason: str) -> Result:
    """A cell that did not run, with the one reason row it falls in."""
    return Result(cell, "ungraded", NOT_GRADED, reason)


@dataclass
class Leg_run:
    """One `rivet load` and what the servers saw of it."""

    proc: core.Proc
    bq: list[dict] | None = None
    gcs: list[dict] | None = None
    summary: str = ""
    state: bytes = b""


@dataclass
class Flow:
    """Everything the cells grade: rivet's outputs, the bytes the stores hold and, live, what a second client reads back."""

    tag: str
    run_out: str
    staged: dict[str, bytes]
    check_ok: core.Proc
    check_refused: core.Proc
    healthy: Leg_run
    refused: Leg_run
    hostile: Leg_run
    run_gcs: list[dict] | None = None
    readback: list[dict] | None = None
    staged_after_refused: list[str] | None = None
    tables_after_refused: list[str] | None = None
    canaries: tuple[str, ...] = (CANARY,)


def sql_columns(job_body: str) -> dict[str, str]:
    """The column -> type list of the `LOAD DATA` statement in a recorded job insert."""
    query = json.loads(job_body)["configuration"]["query"]["query"]
    head = query.split("FROM FILES")[0]
    cols = re.findall(r"^\s+`(\w+)` ([A-Z0-9]+)\s*,?\s*$", head, re.M)
    if not cols:
        raise AssertionError(f"no column list in the load statement: {query[:200]}")
    return dict(cols)


def sql_uris(job_body: str) -> list[str]:
    """The staged part URIs the `LOAD DATA` statement names."""
    return re.findall(r"'(gs://[^']+)'", json.loads(job_body)["configuration"]["query"]["query"])


def job_inserts(log: list[dict]) -> list[dict]:
    """The recorded `jobs.insert` requests."""
    return [e for e in log if e["method"] == "POST" and e["path"].split("?")[0].endswith("/jobs")]


def parquet_file(raw: bytes):
    """A pyarrow reader over staged bytes."""
    import pyarrow.parquet as pq

    return pq.ParquetFile(io.BytesIO(raw))


def parts_of(staged: dict[str, bytes]) -> dict[str, bytes]:
    """The staged parquet parts."""
    return {n: b for n, b in staged.items() if n.endswith(".parquet")}


def arrow_bq(t) -> str:
    """The BigQuery type of an arrow column type; a type the map does not know is an error."""
    s = str(t)
    if s in BQ_TYPE:
        return BQ_TYPE[s]
    if s.startswith("decimal"):
        return "NUMERIC"
    if s.startswith("timestamp"):
        return "TIMESTAMP" if "tz=" in s else "DATETIME"
    raise AssertionError(f"arrow type {s} has no BigQuery type in the cloud legs map")


def text_of(v) -> str | None:
    """A value as BigQuery's REST renders it."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def same(exp, got) -> bool:
    """Does a delivered cell equal the source literal, numerically for numbers."""
    if isinstance(exp, bool):
        return text_of(got) == text_of(exp)
    if isinstance(exp, int):
        return int(str(got)) == exp
    if isinstance(exp, float):
        return float(str(got)) == exp
    if isinstance(exp, Decimal):
        return Decimal(str(got)) == exp
    return text_of(got) == exp


def grade_rows(cell: str, rows: list[dict], what: str) -> Result:
    """Rows read from a second client against the source literals: a NULL where the source had a value is SILENT_NULL."""
    by_id = {int(r["id"]): r for r in rows}
    if sorted(by_id) != [e["id"] for e in EXPECTED]:
        return red(cell, nsn.EMPTY, f"{what}: ids {sorted(by_id)}, expected {[e['id'] for e in EXPECTED]}")
    for e in EXPECTED:
        got = by_id[e["id"]]
        for col, exp in e.items():
            if exp is None:
                continue
            if got.get(col) is None:
                return red(cell, nsn.NULLED, f"{what}: id {e['id']} column {col} is NULL, the source held {exp!r}")
            if not same(exp, got[col]):
                return red(cell, nsn.WRONG, f"{what}: id {e['id']} column {col} is {got[col]!r}, the source held {exp!r}")
    return ok(cell, f"{what}: {len(rows)} rows equal the source")


def staged_rows(staged: dict[str, bytes]) -> list[dict]:
    """The rows of every staged parquet part."""
    out = []
    for raw in parts_of(staged).values():
        out += parquet_file(raw).read().to_pylist()
    return out


def grade_columns(cell: str, bq: list[dict], staged: dict[str, bytes]) -> Result:
    """The load statement's column list and part list against the parquet footers the store holds."""
    jobs = job_inserts(bq)
    if len(jobs) != 1:
        return red(cell, COLUMN_DROPPED, f"{len(jobs)} job inserts, expected the one LOAD DATA")
    declared, uris = sql_columns(jobs[0]["body"]), sql_uris(jobs[0]["body"])
    parts = parts_of(staged)
    if not parts:
        return red(cell, nsn.EMPTY, "no staged parquet part to compare with")
    footer = {}
    for raw in parts.values():
        footer.update({f.name: arrow_bq(f.type) for f in parquet_file(raw).schema_arrow})
    missing = sorted(set(footer) - set(declared))
    if missing:
        return red(cell, COLUMN_DROPPED, f"the load statement drops staged columns {missing}")
    wrong = {c: (footer[c], declared[c]) for c in footer if declared[c] != footer[c]}
    if wrong:
        return red(cell, COLUMN_TYPE, f"declared type differs from the staged footer: {wrong}")
    unloaded = sorted(n for n in parts if not any(u.endswith("/" + n) for u in uris))
    if unloaded:
        return red(cell, PART_NOT_LOADED, f"staged parts {unloaded} are not in the load statement")
    return ok(cell, f"{len(declared)} columns and {len(parts)} parts as staged")


def scan(places: dict[str, str | bytes], canaries: tuple[str, ...]) -> list[str]:
    """The places that hold a canary or a credential shape."""
    hits = []
    for place, blob in places.items():
        text = blob.decode(errors="replace") if isinstance(blob, bytes) else blob
        if any(c and c in text for c in canaries) or cr.leaks(text):
            hits.append(place)
    return hits


def grade_secret(cell: str, run: Leg_run, canaries: tuple[str, ...], reached: str) -> Result:
    """Nothing rivet printed, summarised or stored holds the token it signed with, on a run that reached the path `reached` names."""
    if (reached == "loaded") != (run.proc.returncode == 0):
        return red(cell, UNREACHED, f"the run was to be {reached} and exited {run.proc.returncode}: {(run.proc.stderr or run.proc.stdout).strip()[-200:]}")
    hits = scan({"stdout": run.proc.stdout, "stderr": run.proc.stderr, "summary": run.summary, "state": run.state}, canaries)
    if hits:
        return red(cell, SECRET_LEAK, f"a credential is readable in {hits}")
    return ok(cell, f"exit {run.proc.returncode}, no credential in stdout, stderr, summary or state")


def grade_pair(cell: str, check: core.Proc, load: core.Proc, reached: str) -> Result:
    """`rivet check` and `rivet load` on one config reach the same exit class, on a load that reached the path `reached` names."""
    if reached == "loaded" and load.returncode != 0:
        return red(cell, UNREACHED, f"the load was to succeed and exited {load.returncode}: {(load.stderr or load.stdout).strip()[-200:]}")
    if (check.returncode == 0) != (load.returncode == 0):
        return red(cell, SPLIT, f"check exits {check.returncode}, load exits {load.returncode}: {(load.stderr or load.stdout).strip()[-160:]}")
    return ok(cell, f"check and load both exit {check.returncode}")


def uploaded(log: list[dict], tag: str) -> set[str]:
    """The object names a recorded run uploaded and did not delete, relative to the prefix."""
    up, gone = set(), set()
    for e in log:
        path, _, query = e["path"].partition("?")
        if e["method"] in ("POST", "PUT") and "name" in urllib.parse.parse_qs(query):
            up.add(urllib.parse.parse_qs(query)["name"][0].removeprefix(tag + "/"))
        if e["method"] == "DELETE":
            gone.add(urllib.parse.unquote(path.rsplit("/o/", 1)[-1]).removeprefix(tag + "/"))
    return up - gone


def grade_staging(cell: str, names: set[str], manifest: bytes) -> Result:
    """Every object the run left is a manifest part, a manifest copy or the marker."""
    parts = {p["path"] for p in json.loads(manifest)["parts"]}
    extra = sorted(n for n in names if n not in parts and n != "_SUCCESS" and not re.fullmatch(r"manifest(-[^/]+)?\.json", n))
    gone = sorted(parts - names)
    if extra or gone:
        return red(cell, DEBRIS, f"objects outside the manifest: {extra}; manifest parts missing from the store: {gone}")
    return ok(cell, f"{len(names)} objects, all named by the manifest")


def grade_refused(cell: str, run: Leg_run, before: list[str] | None, after: list[str] | None, tables: list[str] | None) -> Result:
    """A refused load changed nothing: no write to the store, no table or second job at the warehouse."""
    if run.proc.returncode == 0:
        return red(cell, DEBRIS, "the load against a missing dataset exited 0")
    if run.gcs is not None:
        writes = [e["method"] + " " + e["path"][:80] for e in run.gcs if e["method"] != "GET"]
        if writes:
            return red(cell, DEBRIS, f"a refused load wrote to the store: {writes}")
    if run.bq is not None:
        writes = [e["method"] + " " + e["path"][:80] for e in run.bq if e["method"] not in ("GET", "POST")]
        if writes or len(job_inserts(run.bq)) > 1:
            return red(cell, DEBRIS, f"a refused load mutated the warehouse beyond one job: {writes} {len(job_inserts(run.bq))} job inserts")
    if before is not None and sorted(before) != sorted(after or []):
        return red(cell, DEBRIS, f"the staging set changed across the refused load: {sorted(set(after or []) ^ set(before))}")
    if tables:
        return red(cell, DEBRIS, f"the refused load left tables {tables}")
    return ok(cell, f"exit {run.proc.returncode}, nothing written")


def grade_warehouse_offline(name: str, f: Flow) -> dict[str, list[Result]]:
    """The bigquery cells from the replayed flow."""
    c = lambda cell: cell_name("offline", name, cell)
    return {
        "no_silent_null_cloud": [grade_columns(c("typed-columns"), f.healthy.bq or [], f.staged)],
        "no_secret_in_output_cloud": [grade_secret(c("load-success"), f.healthy, f.canaries, "loaded"), grade_secret(c("load-refused"), f.refused, f.canaries, "refused"),
                                       grade_secret(c("load-hostile"), f.hostile, f.canaries, "refused")],
        "check_equals_run_cloud": [grade_pair(c("check-vs-load-healthy"), f.check_ok, f.healthy.proc, "loaded"), grade_pair(c("check-vs-load-refused"), f.check_refused, f.refused.proc, "refused")],
        "no_debris_after_stop_cloud": [grade_refused(c("refused-load-leaves-nothing"), f.refused, None, None, None)],
    }


def grade_warehouse_cloud(name: str, f: Flow) -> dict[str, list[Result]]:
    """The bigquery cells from the live flow: the read-back is a second client's."""
    c = lambda cell: cell_name("cloud", name, cell)
    return {
        "no_silent_null_cloud": [grade_rows(c("typed-columns"), f.readback or [], "BigQuery read-back")],
        "no_secret_in_output_cloud": [grade_secret(c("load-success"), f.healthy, f.canaries, "loaded"), grade_secret(c("load-refused"), f.refused, f.canaries, "refused"),
                                       grade_secret(c("load-hostile"), f.hostile, f.canaries, "refused")],
        "check_equals_run_cloud": [grade_pair(c("check-vs-load-healthy"), f.check_ok, f.healthy.proc, "loaded"), grade_pair(c("check-vs-load-refused"), f.check_refused, f.refused.proc, "refused")],
        "no_debris_after_stop_cloud": [grade_refused(c("refused-load-leaves-nothing"), f.refused, sorted(f.staged), f.staged_after_refused, f.tables_after_refused)],
    }


def grade_store(tier: str, name: str, f: Flow) -> dict[str, list[Result]]:
    """The gcs cells: the bytes the store holds, the run's output and the staging set."""
    c = lambda cell: cell_name(tier, name, cell)
    names = uploaded(f.run_gcs, f.tag) if f.run_gcs is not None else set(f.staged)
    manifest = f.staged["manifest.json"]
    return {
        "no_silent_null_cloud": [grade_rows(c("staged-bytes"), staged_rows(f.staged), "staged parquet")],
        "no_secret_in_output_cloud": [grade_secret(c("run-output"), Leg_run(core.Proc(["rivet", "run"], 0, f.run_out, "", gated=False)), f.canaries, "loaded")],
        "no_debris_after_stop_cloud": [grade_staging(c("staging-set-is-manifest"), names, manifest)],
    }


def load_recording(name: str) -> list[dict]:
    """The exchanges of one recording file."""
    return cr.load(RECORDINGS / "bigquery" / name)["exchanges"]


def bq_key(method: str, path: str) -> str:
    """A request key that ignores query strings and the job id, which differs per run."""
    return f"{method} " + re.sub(r"/(jobs|queries)/[^/]+$", r"/\1/*", path.split("?")[0])


def config_text(meta: dict, dataset: str, gcs_endpoint: str | None, anonymous: bool = False) -> str:
    """The load config of the flow; its destination goes through `gcs_endpoint` when given, unsigned as an emulator when `anonymous`."""
    dest = {"type": "gcs", "bucket": meta["bucket"], "prefix": meta["tag"] + "/"}
    if gcs_endpoint:
        dest["endpoint"] = gcs_endpoint
    if anonymous:
        dest["allow_anonymous"] = True
    return yaml.safe_dump({
        "source": {"type": "postgres", "url_env": "RIVET_NSN_URL"},
        "load": {"target": "bigquery", "project": meta["project"], "dataset": dataset, "pk": ["id"]},
        "exports": [{"name": meta["tag"], "query": f"SELECT * FROM {meta['tag']}", "mode": "full", "format": "parquet", "destination": dest}],
    }, sort_keys=False)


def replay_one(work: Path, meta: dict, dataset: str, gcs_rec: str, bq_players: list[dict], state: Path, key=bq_key) -> Leg_run:
    """One `rivet load` against loopback stubs that answer from a recording; its request logs come back with it."""
    d = work / dataset
    d.mkdir(parents=True)
    shutil.copy(state, d / ".rivet_state.db")
    gcs_ex = load_recording(gcs_rec)
    with cr.Player(gcs_ex, {}) as g, cr.Player(bq_players, {}, key=key) as b:
        (d / "c.yaml").write_text(config_text(meta, dataset, g.url, anonymous=True))
        env = {"RIVET_NSN_URL": PG_URL, "RIVET_BQ_API_ENDPOINT": b.url, "RIVET_BQ_ACCESS_TOKEN": CANARY,
               "HOME": str(d), "RIVET_BQ_LOCATION": meta["location"]}
        proc = nsn.rivet(["load", "-c", str(d / "c.yaml"), "--summary-output", str(d / "summary.json")], env, d)
        gcs_log, bq_log = list(g.log), list(b.log)
        unrecorded = [e["path"] for e in g.unrecorded()]
    if unrecorded:
        raise AssertionError(f"rivet asked the store for {unrecorded}, which the recording has no answer for: re-record with cloud_record.py")
    summary = (d / "summary.json").read_text() if (d / "summary.json").exists() else ""
    state_bytes = b"".join(p.read_bytes() for p in d.glob(".rivet_state.db*") if p.is_file())
    return Leg_run(proc, bq_log, gcs_log, summary, state_bytes)


def recordings_leak(root: Path) -> list[str]:
    """Every file under `root` that holds a credential shape."""
    return sorted(str(f.relative_to(root)) for f in root.rglob("*") if f.is_file() and cr.leaks(f.read_bytes().decode("latin-1")))


def replay_flow(work: Path) -> Flow:
    """The recorded flow replayed: three loads (healthy, refused, one against a server that echoes the token) from the saved state."""
    d = RECORDINGS / "bigquery"
    if leaked := recordings_leak(RECORDINGS):
        raise AssertionError(f"credential shapes in the committed recordings: {leaked}")
    meta = json.loads((d / "meta.json").read_text())
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    state = d / "state_run.sqlite"
    echo = [{"method": "ANY", "path": "*", "status": 401, "binary": False, "request_headers": {}, "reply_headers": {"content-type": "application/json"},
             "reply": json.dumps({"error": {"code": 401, "message": "Invalid credentials: {{AUTHORIZATION}}", "status": "UNAUTHENTICATED"}})}]
    healthy = replay_one(work, meta, meta["dataset"], "load_gcs.json", load_recording("load_bq.json"), state)
    refused = replay_one(work, meta, meta["dataset_missing"], "refused_gcs.json", load_recording("refused_bq.json"), state)
    hostile = replay_one(work, meta, meta["dataset"] + "_hostile", "load_gcs.json", echo, state, key=lambda m, p: "ANY *")
    run_gcs = load_recording("run_gcs.json")
    staged = {}
    for e in run_gcs:
        if e["method"] == "POST" and "name=" in e["path"]:
            name = urllib.parse.parse_qs(e["path"].partition("?")[2])["name"][0].removeprefix(meta["tag"] + "/")
            staged[name] = __import__("base64").b64decode(e["body"]) if e["binary"] else e["body"].encode()
    recorded = lambda k: core.Proc(["rivet", "check"], meta[k]["rc"], meta[k]["out"], "", gated=False)
    return Flow(meta["tag"], meta["run_out"], staged, recorded("check_ok"), recorded("check_refused"), healthy, refused, hostile, run_gcs=run_gcs)


def cloud_env() -> tuple[str, str, str] | None:
    """The registry's project, bucket and location from the environment, or None when a variable is unset."""
    vals = tuple(os.environ.get(v, "") for v in CLOUD_ENV)
    return vals if all(vals) else None


def live_flow(work: Path, rec: dict | None = None) -> Flow:
    """The real flow: stage Postgres rows to a per-run GCS prefix, load them into a `rivet_tmp_*` dataset, read back, then load into a missing dataset; cleaned before and after."""
    from . import gcp
    from ..pytools.registry import bq_tmp

    project, bucket, location = cloud_env() or ("", "", "")
    tag = f"pm_cl_{int(time.time())}"
    ds, missing = bq_tmp(tag), bq_tmp(f"{tag}_missing")
    meta = {"tag": tag, "bucket": bucket, "project": project, "location": location, "dataset": ds, "dataset_missing": missing}
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)

    def clean() -> None:
        engines.sql("postgres", PG_URL, f"DROP TABLE IF EXISTS {tag};")
        gcp.gcs_delete_prefix(bucket, tag + "/")
        for dset in (ds, missing):
            gcp.bq_delete_dataset(project, dset)

    def marks() -> tuple[int, int]:
        return (rec["gcs"].mark(), rec["bq"].mark()) if rec else (0, 0)

    try:
        clean()
        for st in SEED:
            p = engines.sql("postgres", PG_URL, st.format(t=tag) + ";")
            if not p.ok:
                raise RuntimeError(f"seed refused: {p.out.strip()[:300]}")
        gcp.bq_ensure_dataset(project, ds, location)
        env = {"RIVET_NSN_URL": PG_URL, "RIVET_BQ_LOCATION": location}
        if rec:
            env["RIVET_BQ_API_ENDPOINT"] = rec["bq"].url
        gcs_ep = rec["gcs"].url if rec else None
        cfg, cfg_missing = work / "c.yaml", work / "missing.yaml"
        cfg.write_text(config_text(meta, ds, gcs_ep))
        cfg_missing.write_text(config_text(meta, missing, gcs_ep))
        check_ok = nsn.rivet(["check", "-c", str(cfg)], env, work)
        check_refused = nsn.rivet(["check", "-c", str(cfg_missing)], env, work)
        g0, b0 = marks()
        run = nsn.rivet(["run", "-c", str(cfg)], env, work)
        if run.returncode != 0:
            raise RuntimeError(f"rivet run refused: {run.why}")
        g1, b1 = marks()
        state_snapshot = work / "state_run.sqlite"
        _snapshot_state(work / ".rivet_state.db", state_snapshot)
        names = gcp.gcs_list(bucket, tag + "/")
        staged = {}
        for n in names:
            gcp.gcs_download(bucket, n, work / "obj")
            staged[n.removeprefix(tag + "/")] = (work / "obj").read_bytes()
        before = sorted(staged)
        refused = nsn.rivet(["load", "-c", str(cfg_missing), "--summary-output", str(work / "refused.json")], env, work)
        g2, b2 = marks()
        after = sorted(n.removeprefix(tag + "/") for n in gcp.gcs_list(bucket, tag + "/"))
        tables = gcp.bq_tables(project, missing)
        healthy = nsn.rivet(["load", "-c", str(cfg), "--summary-output", str(work / "healthy.json")], env, work)
        g3, b3 = marks()
        state = b"".join(p.read_bytes() for p in work.glob(".rivet_state.db*") if p.is_file())
        readback = gcp.bq_rows(project, f"SELECT * FROM `{project}.{ds}.{tag}` ORDER BY id")
        canary_env = {**env, "RIVET_BQ_ACCESS_TOKEN": CANARY}
        canary_env.pop("RIVET_BQ_API_ENDPOINT", None)
        hostile = nsn.rivet(["load", "-c", str(cfg), "--summary-output", str(work / "hostile.json")], canary_env, work)
        if rec:
            _save_recordings(rec, meta, run, check_ok, check_refused, (g0, g1, g2, g3), (b0, b1, b2, b3), state_snapshot)
        text = lambda n: (work / n).read_text() if (work / n).exists() else ""
        return Flow(tag, run.out, staged, check_ok, check_refused, Leg_run(healthy, summary=text("healthy.json"), state=state),
                    Leg_run(refused, summary=text("refused.json"), state=state), Leg_run(hostile, summary=text("hostile.json"), state=state),
                    readback=readback, staged_after_refused=after, tables_after_refused=tables,
                    canaries=(CANARY, gcp.token()))
    finally:
        clean()


def _snapshot_state(src: Path, dst: Path) -> None:
    """A consistent copy of the state database, one file."""
    con = sqlite3.connect(src)
    try:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        out = sqlite3.connect(dst)
        con.backup(out)
        out.close()
    finally:
        con.close()


def _save_recordings(rec: dict, meta: dict, run: core.Proc, check_ok: core.Proc, check_refused: core.Proc, g: tuple, b: tuple, state: Path) -> None:
    """Write the recordings of the flow, every credential masked; a credential shape left in them refuses the write."""
    out = RECORDINGS / "bigquery"
    out.mkdir(parents=True, exist_ok=True)
    rec["gcs"].save(out / "run_gcs.json", {}, g[0], g[1])
    rec["gcs"].save(out / "refused_gcs.json", {}, g[1], g[2])
    rec["gcs"].save(out / "load_gcs.json", {}, g[2], g[3])
    rec["bq"].save(out / "refused_bq.json", {}, b[1], b[2])
    rec["bq"].save(out / "load_bq.json", {}, b[2], b[3])
    shutil.copy(state, out / "state_run.sqlite")
    full = {**meta, "check_ok": {"rc": check_ok.returncode, "out": cr.mask(check_ok.out)}, "check_refused": {"rc": check_refused.returncode, "out": cr.mask(check_refused.out)},
            "run_out": cr.mask(run.out)}
    (out / "meta.json").write_text(json.dumps(full, indent=1, sort_keys=True) + "\n")


def grade(tier: str, flow: Flow | None) -> dict[str, list[Result]]:
    """Every cell of every leg for one tier, a leg the tier cannot grade as NOT_GRADED with its reason."""
    out: dict[str, list[Result]] = {p: [] for p in PROPS}
    for kind, name, leg in legs():
        reason = leg.offline if tier == "offline" else leg.cloud
        graded = {}
        if not reason:
            if flow is None:
                reason = "no cloud credentials: BIGQUERY_TEST_PROJECT, RIVET_TEST_GCS_BUCKET and BIGQUERY_TEST_LOCATION are not all set"
            elif kind == "store":
                graded = grade_store(tier, name, flow)
            elif name == "bigquery":
                graded = (grade_warehouse_offline if tier == "offline" else grade_warehouse_cloud)(name, flow)
            else:
                raise AssertionError(f"{kind} {name} has no reason and no grader")
        for prop, cells in CELLS[kind].items():
            if reason:
                out[prop] += [not_graded(cell_name(tier, name, c), reason) for c in cells]
            else:
                out[prop] += graded[prop]
    prop, cell = SECRET_MANAGER_CELL
    out[prop].append(not_graded(f"{tier}/{cell}", R_SECRET_MANAGER))
    return out


def run_tier(tier: str, work: Path) -> dict[str, list[Result]]:
    """Build the tier's flow and grade it; the cloud tier without credentials grades nothing and says so."""
    if tier == "offline":
        flow = replay_flow(work)
    else:
        flow = live_flow(work) if cloud_env() else None
    return grade(tier, flow)


def verdict_line(prop: str, r: Result) -> str:
    """One `<property> <cell> PASS|RED|NOT_GRADED <evidence>` line."""
    state = "NOT_GRADED" if r.klass == NOT_GRADED else "RED" if r.klass else "PASS"
    evidence = f"{r.klass}: {r.detail}" if r.klass else r.detail
    return f"{prop} {r.cell} {state} {' '.join(evidence.split())[:300]}"


def counts(results: list[Result]) -> tuple[int, int, int]:
    """(PASS, RED, NOT_GRADED) of a list of cells."""
    ng = sum(1 for r in results if r.klass == NOT_GRADED)
    red_n = sum(1 for r in results if r.klass and r.klass != NOT_GRADED)
    return len(results) - ng - red_n, red_n, ng


def digest(rs: list[Result]) -> str:
    """Eight hex digits over the cell names without their tier prefix."""
    return hashlib.sha1("\n".join(sorted(r.cell.split("/", 1)[-1] for r in rs)).encode()).hexdigest()[:8]


def ledger_message(prop: str, tier: str, klass: str, rs: list[Result]) -> str:
    """The one ledger line of a failure class in one property."""
    return f"{prop}[{tier}]: {klass} over {len(rs)} cells #{digest(rs)}"


def not_graded_table(by_prop: dict[str, list[Result]]) -> list[str]:
    """Markdown rows `reason | cells | legs | what would grade it` over every NOT_GRADED cell."""
    rows: dict[str, list[str]] = {}
    for rs in by_prop.values():
        for r in rs:
            if r.klass == NOT_GRADED:
                rows.setdefault(r.detail, []).append(r.cell)
    return [f"| {reason} | {len(cells)} | {', '.join(sorted({c.split('/')[1] for c in cells}))} | {GRADES_IT[reason]} |" for reason, cells in sorted(rows.items())]


GRADES_IT = {
    R_SF_ENDPOINT: "an owner-approved src/ endpoint override for Snowflake, then one recording against a real account",
    R_SF_CLOUD: "a `snow` connection and key on the machine that runs the nightly gate lane",
    R_NO_RECORDING: "one cloud_record.py run against a real S3 bucket or Azure account",
    R_NO_CREDS: "a test bucket or storage account and its credential in dev/stand/registry.yaml",
    R_SECRET_MANAGER: "an owner-approved src/ endpoint override for gcp_secret and a test secret in the project",
    "no cloud credentials: BIGQUERY_TEST_PROJECT, RIVET_TEST_GCS_BUCKET and BIGQUERY_TEST_LOCATION are not all set": "run the cloud tier where those variables and a Google credential exist",
}


def verify_cloud_legs(led: Ledger) -> None:
    """Gate cell: the cloud tier against the real test project; without credentials it is a skip, never a pass."""
    work = Path(os.environ.get("RIVET_ORACLE_WORK") or tempfile.gettempdir()) / "cloud_legs"
    try:
        by_prop = run_tier("cloud", work)
    except (RuntimeError, OSError) as e:
        led.failed("-", "-", "cloud_legs", "-", "cloud_legs: the cloud flow raised", f"{type(e).__name__}: {e}"[:600])
        return
    record(led, "cloud", by_prop)


def record(led: Ledger, tier: str, by_prop: dict[str, list[Result]]) -> None:
    """Ledger rows for one tier: a failure per RED class, a skip per NOT_GRADED reason, a pass per property with graded cells."""
    for prop, rs in by_prop.items():
        reds: dict[str, list[Result]] = {}
        for r in rs:
            if r.klass and r.klass != NOT_GRADED:
                reds.setdefault(r.klass, []).append(r)
        for klass, group in reds.items():
            led.failed("-", "-", "cloud_legs", "-", ledger_message(prop, tier, klass, group), "\n".join(f"{r.cell}: {r.detail}" for r in group))
        ng = [r for r in rs if r.klass == NOT_GRADED]
        for reason in sorted({r.detail for r in ng}):
            cells = [r for r in ng if r.detail == reason]
            cause = SkipCause.ENV if reason.startswith("no cloud credentials") else SkipCause.REDUCED
            led.skipped("-", "-", "cloud_legs", "-", f"{prop}[{tier}]: {len(cells)} cells not graded: {reason}"[:300], cause=cause)
        if not reds and len(ng) < len(rs):
            led.passed("-", "-", "cloud_legs", "-", f"{prop}[{tier}]: {len(rs) - len(ng)} cells graded")


def self_test() -> None:
    """Grade the grader: the axes come from the product and every failure class fires on its exact harm."""
    a = axes()
    assert "bigquery" in a["warehouses"] and "gcs" in a["stores"], a
    saved = dict(WAREHOUSES)
    try:
        WAREHOUSES["newwarehouse"] = Leg("warehouse")
        try:
            axes()
            raise SystemExit("an extra leg entry went unnoticed")
        except AssertionError:
            pass
    finally:
        WAREHOUSES.clear()
        WAREHOUSES.update(saved)
    assert recordings_leak(RECORDINGS) == [], recordings_leak(RECORDINGS)
    with tempfile.TemporaryDirectory() as t:
        (Path(t) / "r.json").write_text('{"h": "Bearer ya29.' + "a" * 40 + '"}')
        assert recordings_leak(Path(t)) == ["r.json"]
    rows = [dict(e) for e in EXPECTED]
    assert grade_rows("c", rows, "x").klass == ""
    nulled = [dict(r) for r in rows]
    nulled[0]["n"] = None
    assert grade_rows("c", nulled, "x").klass == nsn.NULLED
    wrong = [dict(r) for r in rows]
    wrong[2]["d"] = Decimal("-0.000002")
    assert grade_rows("c", wrong, "x").klass == nsn.WRONG
    assert grade_rows("c", rows[:2], "x").klass == nsn.EMPTY
    proc = lambda rc, out="", err="": core.Proc(["rivet"], rc, out, err, gated=False)
    assert grade_secret("c", Leg_run(proc(1, "", f"BigQuery said {CANARY}")), (CANARY,), "refused").klass == SECRET_LEAK
    assert grade_secret("c", Leg_run(proc(1), summary="token ya29." + "a" * 30), (), "refused").klass == SECRET_LEAK
    assert grade_secret("c", Leg_run(proc(0), state=b"x" + CANARY.encode()), (CANARY,), "loaded").klass == SECRET_LEAK
    assert grade_secret("c", Leg_run(proc(1, "Error: [RIVET_LOAD_WAREHOUSE_REJECTED]")), (CANARY,), "refused").klass == ""
    assert grade_secret("c", Leg_run(proc(5, "no column types recorded")), (CANARY,), "loaded").klass == UNREACHED
    assert grade_secret("c", Leg_run(proc(0)), (CANARY,), "refused").klass == UNREACHED
    assert grade_pair("c", proc(0), proc(1), "refused").klass == SPLIT and grade_pair("c", proc(0), proc(0), "loaded").klass == ""
    assert grade_pair("c", proc(0), proc(5), "loaded").klass == UNREACHED
    manifest = json.dumps({"parts": [{"path": "p.parquet"}]}).encode()
    assert grade_staging("c", {"p.parquet", "manifest.json", "manifest-r.json", "_SUCCESS"}, manifest).klass == ""
    assert grade_staging("c", {"p.parquet", "manifest.json", "p.parquet.tmp"}, manifest).klass == DEBRIS
    assert grade_staging("c", {"manifest.json"}, manifest).klass == DEBRIS
    job = {"method": "POST", "path": "/bigquery/v2/projects/p/jobs", "body": json.dumps({"configuration": {"query": {"query": "LOAD DATA OVERWRITE `p.d.t` (\n  `id` INT64,\n  `s` STRING\n)\nFROM FILES (\n  uris = [\n    'gs://b/t/p.parquet'\n  ]\n);"}}})}
    assert sql_columns(job["body"]) == {"id": "INT64", "s": "STRING"} and sql_uris(job["body"]) == ["gs://b/t/p.parquet"]
    assert len(job_inserts([job, {"method": "GET", "path": "/jobs/x"}])) == 1
    refused = Leg_run(proc(1), [{"method": "GET", "path": "/t"}, job], [{"method": "GET", "path": "/o"}])
    assert grade_refused("c", refused, ["a"], ["a"], []).klass == ""
    assert grade_refused("c", refused, ["a"], ["a", "tmp"], []).klass == DEBRIS
    assert grade_refused("c", refused, ["a"], ["a"], ["t"]).klass == DEBRIS
    assert grade_refused("c", Leg_run(proc(1), [], [{"method": "POST", "path": "/o"}]), None, None, None).klass == DEBRIS
    assert grade_refused("c", Leg_run(proc(0), [], []), None, None, None).klass == DEBRIS
    assert grade_refused("c", Leg_run(proc(1), [job, job], []), None, None, None).klass == DEBRIS
    for t in ("int64", "double", "string", "bool", "date32[day]", "decimal128(20, 6)", "timestamp[us]"):
        assert arrow_bq(t) in ("INT64", "FLOAT64", "STRING", "BOOL", "DATE", "NUMERIC", "DATETIME"), t
    try:
        arrow_bq("list<item: int64>")
        raise SystemExit("an unknown arrow type was mapped")
    except AssertionError:
        pass
    by_prop = grade("cloud", None)
    assert all(r.klass == NOT_GRADED for rs in by_prop.values() for r in rs), "a cloud tier without credentials graded something"
    table = not_graded_table(by_prop)
    assert table and all(len(row.split("|")) == 6 for row in table), table
    print(f"cloud_legs self-test ok: warehouses {a['warehouses']}, stores {a['stores']}")


def main() -> int:
    """Developer entry: print the verdict lines of one tier; non-zero when a RED is not a known red."""
    from . import known_red

    ap = argparse.ArgumentParser(prog="cloud_legs")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--tier", choices=TIERS, default="offline")
    ap.add_argument("--work", default=str(Path(tempfile.gettempdir()) / "cloud_legs"))
    ap.add_argument("--table", action="store_true", help="print the NOT_GRADED table as markdown")
    ns = ap.parse_args()
    if ns.self_test:
        self_test()
        return 0
    by_prop = run_tier(ns.tier, Path(ns.work))
    bad = 0
    for prop, rs in by_prop.items():
        print("\n".join(verdict_line(prop, r) for r in sorted(rs, key=lambda r: r.cell)))
        p, r_n, n = counts(rs)
        print(f"{prop} summary tier={ns.tier} PASS={p} RED={r_n} NOT_GRADED={n}")
        for klass in sorted({r.klass for r in rs if r.klass and r.klass != NOT_GRADED}):
            entry, in_date = known_red.match(ledger_message(prop, ns.tier, klass, [r for r in rs if r.klass == klass]))
            if entry is None or not in_date:
                bad += 1
    if ns.table:
        print("| reason | cells | legs | what would grade it |\n|---|---|---|---|")
        print("\n".join(not_graded_table(by_prop)))
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
