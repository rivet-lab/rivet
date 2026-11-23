"""The edge-value type matrix, live: every ledger type x its edge values x every read of it, graded against docs/type-capability-matrix.yaml.

Each ledger row is a column of a seed table and each literal an id. A column's values that deliver are one unit and a value some
stage refuses is a unit of its own, so one refusal never hides its neighbours. A unit is exported to Parquet and read by DuckDB
(`duck`), then staged and loaded into each warehouse and read back by a client that is not rivet. The observed verdict (exact,
null, form, refuse) is compared with the ledger's; a known defect is a class `known_red.py` excuses and its disappearance fails too.
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from base64 import b64decode, b64encode
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from decimal import Decimal
from itertools import count
from pathlib import Path

import yaml

from . import core, gcp
from . import property_no_silent_null as nsn
from . import type_edges as te
from . import type_matrix as tm
from .core import Ledger, SkipCause
from .property_no_silent_null import DELIVERED, NOT_GRADED, REFUSED, Result
from .type_matrix import Chunk, Column, Unit

SCENARIO = "no_silent_null_load"
CLOUD_SCENARIO = "no_silent_null_load_cloud"
STAND_TARGETS = ("duck", "clickhouse")
CLOUD_TARGETS = ("bigquery", "snowflake")
CH_URL = "http://127.0.0.1:8123"
CH_USER, CH_PASSWORD, CH_PASSWORD_ENV = "rivet", "rivet", "RIVET_TEST_CH_PASSWORD"
GCS = "http://127.0.0.1:4443"
BUCKET = "pm-no-silent-null"
WORKERS = 6
BISECT_RUNS = 40
SNOW = "/opt/homebrew/bin/snow"


class Unreachable(Exception):
    """A server or store the cell needs would not answer: the cell is not graded."""


class HarnessError(Exception):
    """The harness's own config or ledger was refused before any value was read."""


@dataclass
class Got:
    """What one id delivered at one target: a value read back, or a refusal."""

    value: object = None
    refused: te.Verdict | None = None


MISSING = object()


def load_targets() -> list[str]:
    """The warehouses `rivet load` writes, from `LoadTarget`; each is graded here as a stand or a cloud target."""
    found = nsn._enum_variants("src/config/load.rs", "LoadTarget")
    if set(found) != {*STAND_TARGETS, *CLOUD_TARGETS} - {"duck"}:
        raise AssertionError(f"LoadTarget variants {found}: a target is in neither STAND_TARGETS nor CLOUD_TARGETS")
    return found


def ch(query: str) -> str:
    """One statement on the stand's ClickHouse, as the harness's own HTTP client; bytes that are not UTF-8 survive as surrogates."""
    req = urllib.request.Request(CH_URL, data=query.encode(), method="POST",
                                 headers={"Authorization": "Basic " + b64encode(f"{CH_USER}:{CH_PASSWORD}".encode()).decode()})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.read().decode(errors="surrogateescape")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code}: {e.read().decode(errors='replace').strip()[:200]}") from e


def ensure_bucket() -> None:
    """Create the staging bucket on the GCS emulator; an existing bucket is fine."""
    req = urllib.request.Request(f"{GCS}/storage/v1/b?project=test", data=json.dumps({"name": BUCKET}).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            r.read()
    except urllib.error.HTTPError as e:
        if e.code != 409:
            raise


def ch_projection(kind: str, name: str) -> tuple[str, bool]:
    """The select item that reads column `name` of ClickHouse type `kind` exactly, and whether it is base64 of the bytes (a String or FixedString)."""
    if re.fullmatch(r"(?:Nullable\()?(?:Fixed)?String(?:\(\d+\))?\)?", kind):
        return f"base64Encode(`{name}`)", True
    if re.fullmatch(r"Array\((?:Nullable\()?(?:Fixed)?String(?:\(\d+\))?\)?\)", kind):
        return f"arrayMap(x -> base64Encode(x), `{name}`)", True
    return f"`{name}`", False


def read_clickhouse(db: str, table: str, col: str, key: str = "id") -> dict[int, object]:
    """The value of `col` by `key`, read from ClickHouse by the harness's HTTP client; a String or FixedString comes back as its bytes."""
    described = [ln.split("\t")[:2] for ln in ch(f"DESCRIBE TABLE {db}.`{table}` FORMAT TSV").splitlines()]
    name, kind = next(d for d in described if d[0].lower() == col.lower())
    item, binary = ch_projection(kind, name)
    idname = next(d[0] for d in described if d[0].lower() == key)
    out = ch(f"SELECT `{idname}` AS id, {item} AS v FROM {db}.`{table}` FORMAT JSONEachRow SETTINGS output_format_json_quote_64bit_integers = 0, "
             "output_format_json_quote_decimals = 0, output_format_json_quote_denormals = 1")
    rows = [json.loads(ln, parse_float=Decimal, parse_int=lambda t: -0.0 if t == "-0" else int(t)) for ln in out.splitlines() if ln.strip()]
    unb64 = lambda v: None if v is None else [None if x is None else b64decode(x) for x in v] if isinstance(v, list) else b64decode(v)
    return {int(r["id"]): unb64(r["v"]) if binary else r["v"] for r in rows}


def _json_of(kind: str, text: str):
    """The Python value of one BigQuery `TO_JSON_STRING` cell of column type `kind`."""
    if kind == "JSON":
        return text
    v = json.loads(text, parse_float=Decimal if kind in ("NUMERIC", "BIGNUMERIC") else float)
    if kind == "BYTES":
        return b64decode(v)
    if kind == "FLOAT64" and isinstance(v, str):
        return float(v.replace("Infinity", "inf").replace("NaN", "nan"))
    if kind == "INT64" and isinstance(v, str):
        return int(v)
    if kind in ("NUMERIC", "BIGNUMERIC") and isinstance(v, (str, float)):
        return Decimal(str(v))
    return v


def read_bigquery(project: str, dataset: str, table: str, col: str, key: str = "id") -> dict[int, object]:
    """The value of `col` by `key`, read from BigQuery as JSON text by the REST API (never rivet's own client)."""
    kinds = gcp.bq_rows(project, f"SELECT column_name, data_type FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table}'")
    kind = next((r["data_type"] for r in kinds if r["column_name"].lower() == col.lower()), "")
    base = kind.split("<")[0].split("(")[0]
    rows = gcp.bq_rows(project, f"SELECT `{key}` AS id, `{col}` IS NULL AS n, TO_JSON_STRING(`{col}`) AS v FROM `{project}.{dataset}.{table}`")
    return {int(r["id"]): None if r["n"] == "true" else _bq_value(kind, base, r["v"]) for r in rows}


def _bq_value(kind: str, base: str, text: str):
    """One BigQuery cell as a Python value; a Parquet LIST (`ARRAY<STRUCT<item T>>`) comes back as the list of its items."""
    if base == "ARRAY" and (m := re.match(r"ARRAY<STRUCT<item (\w+)", kind)):
        return [None if (x := e["item"]) is None else _json_of(m[1], json.dumps(x)) for e in json.loads(text)]
    return _json_of(base, text)


class SnowflakeReader:
    """A DuckDB session over Snowflake, one statement at a time: the harness's second client of the warehouse."""

    def __init__(self, connection: str, database: str, warehouse: str) -> None:
        from .duck import Oracle

        self.spec = {"connection": connection, "database": database, "warehouse": warehouse}
        self.stack = ExitStack()
        self.ora = self.stack.enter_context(Oracle(snowflake=self.spec))
        self.lock = threading.Lock()

    def query(self, sql: str) -> list[tuple]:
        """Every row of one Snowflake statement."""
        with self.lock:
            return self.ora.rows(f"SELECT * FROM {self.ora.snowflake(sql)}")

    def close(self) -> None:
        """Release the session."""
        self.stack.close()


SF_DOUBLE_MAX_TEXT = re.compile(r"(?<![\d.])1\.797693134862316e\+308(?!\d)")


def sf_json(text: str) -> str:
    """Snowflake's JSON text with its 16-digit spelling of the largest double, which reads back as an infinity, spelled exactly."""
    return SF_DOUBLE_MAX_TEXT.sub("1.7976931348623157e+308", text)


def _sf_value(kind: str, text: str | None):
    """The Python value of one Snowflake `TO_VARCHAR` cell of column type `kind`."""
    if text is None:
        return None
    if kind == "BINARY":
        return bytes.fromhex(text)
    if kind == "BOOLEAN":
        return text.lower() == "true"
    if kind == "FLOAT":
        return float(text)
    if kind == "NUMBER":
        return Decimal(text)
    if kind in ("ARRAY", "OBJECT"):
        return json.loads(sf_json(text))
    return sf_json(text) if kind == "VARIANT" else text


SF_TEXT_FORMATS = {"TIMESTAMP_NTZ": "YYYY-MM-DD HH24:MI:SS.FF9", "TIMESTAMP_LTZ": "YYYY-MM-DD HH24:MI:SS.FF9", "TIMESTAMP_TZ": "YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM", "TIME": "HH24:MI:SS.FF9"}


def sf_text(kind: str, col: str) -> str:
    """A Snowflake column as the harness reads it: a FLOAT as the double itself (its text keeps nine digits), a VARIANT as its JSON text (TO_VARCHAR drops a string's quotes), a time at nine fraction digits (the session formats cut to milliseconds and seconds), the rest as text."""
    if kind == "FLOAT":
        return f'"{col}"'
    if kind in ("ARRAY", "OBJECT", "VARIANT"):
        return f'TO_JSON("{col}")'
    fmt = SF_TEXT_FORMATS.get(kind)
    return f'TO_VARCHAR("{col}", \'{fmt}\')' if fmt else f'TO_VARCHAR("{col}")'


def read_snowflake(sf: SnowflakeReader, schema: str, table: str, col: str, key: str = "id") -> dict[int, object]:
    """The value of `col` by `key`, read from Snowflake as text through DuckDB's ADBC client."""
    db = sf.spec["database"]
    cols = sf.query(f"SELECT table_name, column_name, data_type FROM {db}.INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE table_schema = '{schema.upper()}' AND UPPER(table_name) = '{table.upper()}'")
    names = {c.lower(): (t, c, k) for t, c, k in cols}
    if key not in names or col.lower() not in names:
        return {}
    tname, cname, kind = names[col.lower()]
    iname = names[key][1]
    rows = sf.query(f'SELECT "{iname}", {sf_text(kind, cname)}, "{cname}" IS NULL FROM {db}.{schema}."{tname}"')
    return {int(i): None if isnull else _sf_value(kind, text) for i, text, isnull in rows}


QUOTA = re.compile(r"resource monitor .* has exceeded its quota")


def stop_verdict(p: core.Proc, stage: str) -> te.Verdict:
    """The refusal a stopped process amounts to; a stand that would not answer or a harness config refusal is not a verdict."""
    code = (nsn.CODE.search(p.stderr + p.stdout) or [None, ""])[1]
    if nsn.stand_refused(p):
        raise Unreachable(f"the stand refused the process {nsn.STAND_TRIES} times: exit {p.returncode} [{code}]")
    if QUOTA.search(p.stderr + p.stdout):
        raise Unreachable(f"the warehouse's credit quota is spent: {core.first_error(p.stderr)[:200]}")
    if code == nsn.LEASE_REFUSAL or (code.startswith("RIVET_CONFIG_") and code not in nsn.TYPE_REFUSALS):
        raise HarnessError(f"the harness's own config was refused: exit {p.returncode} [{code}] {core.first_error(p.stderr)[:160]}")
    if p.returncode in (101, 124, 127) or p.returncode < 0:
        return te.Verdict(te.REFUSE, code="CRASH", stage=stage)
    return te.Verdict(te.REFUSE, code=code or te.UNCODED, stage=stage)


class Target:
    """How one warehouse is staged, loaded, read and cleaned."""

    name = ""
    split = False

    def start(self, run: str) -> None:
        """Create what the loads need."""

    def stage(self, prefix: str) -> dict:
        """The export destination for one load."""
        raise NotImplementedError

    def load_block(self) -> dict:
        """The config's `load:` block."""
        raise NotImplementedError

    def env(self) -> dict:
        """Environment the run and load need."""
        return {}

    def read(self, table: str, col: str, key: str = "id") -> dict[int, object]:
        """The values of `col` the warehouse holds, by `key`."""
        raise NotImplementedError

    def clean(self, tables: list[str], prefixes: list[str]) -> None:
        """Drop what the loads made."""


class ClickHouse(Target):
    name = "clickhouse"
    split = True

    def start(self, run: str) -> None:
        """Create the database and the staging bucket on the emulator."""
        self.db = f"pm_nsn_{run}"
        ensure_bucket()
        ch(f"DROP DATABASE IF EXISTS {self.db}")
        ch(f"CREATE DATABASE {self.db}")

    def stage(self, prefix: str) -> dict:
        """A prefix of the emulator's bucket."""
        return {"type": "gcs", "bucket": BUCKET, "prefix": prefix + "/", "endpoint": GCS, "allow_anonymous": True}

    def load_block(self) -> dict:
        """The stand's ClickHouse."""
        return {"target": "clickhouse", "url": CH_URL, "database": self.db, "user": CH_USER, "password_env": CH_PASSWORD_ENV, "pk": ["id"]}

    def env(self) -> dict:
        """The ClickHouse password variable."""
        return {CH_PASSWORD_ENV: CH_PASSWORD}

    def read(self, table: str, col: str, key: str = "id") -> dict[int, object]:
        """Read by the HTTP client."""
        return read_clickhouse(self.db, table, col, key)

    def clean(self, tables: list[str], prefixes: list[str]) -> None:
        """Drop the database."""
        try:
            ch(f"DROP DATABASE IF EXISTS {self.db} SYNC")
        except (OSError, urllib.error.URLError):
            pass


def stage_root() -> str:
    """The bucket prefix every staged object of this property lives under."""
    from ..pytools.registry import bq_tmp

    return bq_tmp("nsn")


class BigQuery(Target):
    name = "bigquery"

    def start(self, run: str) -> None:
        """Create the `rivet_tmp_*` dataset."""
        from ..pytools.registry import bq_tmp

        self.project, self.bucket, self.location = (os.environ.get(v, "") for v in ("BIGQUERY_TEST_PROJECT", "RIVET_TEST_GCS_BUCKET", "BIGQUERY_TEST_LOCATION"))
        if not (self.project and self.bucket and self.location):
            raise Unreachable("no BigQuery stand: BIGQUERY_TEST_PROJECT, RIVET_TEST_GCS_BUCKET and BIGQUERY_TEST_LOCATION are not all set")
        self.dataset = bq_tmp(f"nsn_{run}")
        gcp.bq_ensure_dataset(self.project, self.dataset, self.location)

    def stage(self, prefix: str) -> dict:
        """A prefix of the test bucket."""
        return {"type": "gcs", "bucket": self.bucket, "prefix": f"{stage_root()}/{prefix}/"}

    def load_block(self) -> dict:
        """The test project's dataset."""
        return {"target": "bigquery", "project": self.project, "dataset": self.dataset, "pk": ["id"]}

    def env(self) -> dict:
        """The dataset location."""
        return {"RIVET_BQ_LOCATION": self.location}

    def read(self, table: str, col: str, key: str = "id") -> dict[int, object]:
        """Read by the REST API."""
        return read_bigquery(self.project, self.dataset, table, col, key)

    def clean(self, tables: list[str], prefixes: list[str]) -> None:
        """Drop the dataset and the staged objects."""
        gcp.bq_delete_dataset(self.project, self.dataset)
        for p in prefixes:
            gcp.gcs_delete_prefix(self.bucket, f"{stage_root()}/{p}/")


def sf_drops(schema: str, tables: list[str]) -> list[str]:
    """Every statement that removes what a load of `tables` leaves in a Snowflake schema: the table, its changes table and both stages."""
    return [f"DROP {kind} IF EXISTS {schema}.{name}" for t in tables
            for kind, name in (("TABLE", t), ("TABLE", f"{t}__changes"), ("STAGE", f"rivet_stage_{t}"), ("STAGE", f"rivet_stage_{t}__changes"))]


class Snowflake(Target):
    name = "snowflake"

    def start(self, run: str) -> None:
        """Open the reader on the stand's schema."""
        keys = ("RIVET_SF_CONNECTION", "RIVET_SNOWFLAKE_ACCOUNT", "RIVET_SNOWFLAKE_USER", "RIVET_SNOWFLAKE_KEY", "RIVET_SF_WAREHOUSE",
                "RIVET_SF_DATABASE", "RIVET_SF_SCHEMA", "RIVET_SF_STORAGE_INTEGRATION", "RIVET_TEST_GCS_BUCKET")
        self.e = {k: os.environ.get(k, "") for k in keys}
        missing = [k for k, v in self.e.items() if not v]
        if missing:
            raise Unreachable(f"no Snowflake stand: {', '.join(missing)} unset")
        self.sf = SnowflakeReader(self.e["RIVET_SF_CONNECTION"], self.e["RIVET_SF_DATABASE"], self.e["RIVET_SF_WAREHOUSE"])

    def stage(self, prefix: str) -> dict:
        """A prefix of the test bucket the storage integration reads."""
        return {"type": "gcs", "bucket": self.e["RIVET_TEST_GCS_BUCKET"], "prefix": f"{stage_root()}/{prefix}/"}

    def load_block(self) -> dict:
        """The stand's Snowflake."""
        e = self.e
        block = {"target": "snowflake", "account": e["RIVET_SNOWFLAKE_ACCOUNT"], "user": e["RIVET_SNOWFLAKE_USER"], "private_key_path": e["RIVET_SNOWFLAKE_KEY"],
                 "warehouse": e["RIVET_SF_WAREHOUSE"], "database": e["RIVET_SF_DATABASE"], "schema": e["RIVET_SF_SCHEMA"],
                 "storage_integration": e["RIVET_SF_STORAGE_INTEGRATION"], "pk": ["id"]}
        if os.environ.get("RIVET_SNOWFLAKE_ROLE"):
            block["role"] = os.environ["RIVET_SNOWFLAKE_ROLE"]
        return block

    def read(self, table: str, col: str, key: str = "id") -> dict[int, object]:
        """Read through DuckDB's ADBC client."""
        return read_snowflake(self.sf, self.e["RIVET_SF_SCHEMA"], table, col, key)

    def clean(self, tables: list[str], prefixes: list[str]) -> None:
        """Drop the tables and their stages, close the reader and delete the staged objects."""
        schema = f"{self.e['RIVET_SF_DATABASE']}.{self.e['RIVET_SF_SCHEMA']}"
        self.sf.close()
        drops = sf_drops(schema, tables)
        for i in range(0, len(drops), 50):
            subprocess.run([SNOW, "sql", "-c", self.e["RIVET_SF_CONNECTION"], "-q", "; ".join(drops[i:i + 50])], capture_output=True, timeout=600)
        for p in prefixes:
            gcp.gcs_delete_prefix(self.e["RIVET_TEST_GCS_BUCKET"], f"{stage_root()}/{p}/")


TARGETS = {"clickhouse": ClickHouse, "bigquery": BigQuery, "snowflake": Snowflake}


class Matrix:
    """One engine's matrix: seeds its chunks, cuts them into units, observes every cell at every target."""

    def __init__(self, engine: str, url: str, work: Path, targets: tuple[str, ...]) -> None:
        self.engine, self.url, self.work = engine, url, work
        self.key, self.read_col = ("_id", "document") if engine == "mongo" else ("id", "")
        self.run = f"{int(time.time()) % 10**7}{engine[:2]}"
        self.targets = [TARGETS[t]() for t in targets if t != "duck"]
        self.n = count(1)
        self.tables: list[str] = []
        self.prefixes: list[str] = []
        self.obs: dict[tuple[str, str, int, str], Got] = {}
        self.unheld: dict[tuple[str, str, int], tuple[str, str]] = {}
        self.unmade: list[tuple[Column, str]] = []
        self.texts: dict[tuple[str, str], list[str | None] | None] = {}
        self.checked: dict[tuple[str, str, str], int] = {}
        self.splits: dict[tuple[str, str, str], set[str]] = {}
        self.lock = threading.Lock()

    def config(self, chunk: Chunk, col: Column, ids: tuple[int, ...], name: str, dest: dict, load: dict | None = None) -> str:
        """A rivet config for one export of `ids` of one column, with an optional load block."""
        fold = str.upper if self.engine == "oracle" else str
        query = f"SELECT id, {fold(col.name)} FROM {'dbo.' if self.engine == 'mssql' else ''}{chunk.table} WHERE id IN ({', '.join(map(str, ids))})"
        export = {"name": name, "table": self.collection(chunk)} if self.engine == "mongo" else {"name": name, "query": query}
        cfg = {"source": {"type": self.engine, "url_env": "RIVET_NSN_URL"},
               "exports": [{**export, "mode": "full", "format": "parquet", "destination": dest}]}
        if load:
            cfg["load"] = {**load, "pk": [fold(self.key if k == "id" else k) for k in load["pk"]]}
        return yaml.safe_dump(cfg, sort_keys=False)

    def cwd(self, tag: str) -> Path:
        """A fresh working directory, so no two processes share a state file."""
        d = self.work / f"{tag}{next(self.n)}"
        d.mkdir(parents=True)
        return d

    def seed(self, chunks: list[Chunk]) -> list[Chunk]:
        """Create every chunk's table; a literal this server version will not hold is seeded as NULL and left ungraded."""
        if self.engine == "mongo":
            self.seed_mongo(chunks)
            return chunks
        probe = nsn.sql(self.engine, self.url, "SELECT 1 FROM dual" if self.engine == "oracle" else "SELECT 1")
        if not probe.ok:
            raise Unreachable(f"the {self.engine} stand did not answer: {probe.out.strip()[:200]}")
        for s in nsn.ledger_setup(self.engine):
            nsn.sql(self.engine, self.url, s)
        out = []
        for c in chunks:
            while c.cols and (bad := self.seed_chunk(c)) is not None:
                c = self.mask(c, *bad)
            if c.cols:
                out.append(c)
        for c in out:
            for col in c.cols:
                if nsn.server_text_graded(self.engine, col.delivery):
                    self.texts[(c.table, col.name)] = nsn.server_text_of(self.url, c.table, col.name, c.null_id)
        return out

    def seed_chunk(self, c: Chunk) -> tuple[int, str] | None:
        """Run a chunk's seed statements; the id (0 for the table itself) and the server's words of the first one it refuses, None when all are held."""
        stmts, _ = nsn.seed_sql(self.engine, c.seed_cols(), c.table)
        for k, st in enumerate(stmts):
            p = nsn.sql(self.engine, self.url, st + ";")
            if not p.ok:
                if k < 1 or k - 1 >= c.null_id:
                    raise HarnessError(f"{self.engine} seed refused `{st[:200]}`: exit {p.returncode}: {p.out.strip()[:300]}")
                return max(k - 1, 0), next((x.strip() for x in p.out.splitlines() if x.strip() and "Using a password" not in x), f"exit {p.returncode}")[:200]
        return None

    def mask(self, c: Chunk, i: int, why: str) -> Chunk:
        """The chunk without the columns whose type the server will not create (i = 0), or with every literal of row `i` that it refuses alone as NULL; those cells are noted as not held."""
        cols, found = [], False
        for col in c.cols:
            held = i > len(col.lits) or self.holds(col, i, c.table)
            found |= not held
            if not held and i == 0:
                self.unmade.append((col, why))
                continue
            if not held:
                self.unheld[(c.table, col.name, i)] = (col.lits[i - 1], why)
            cols.append(col if held else replace(col, lits=col.lits[:i - 1] + ("NULL",) + col.lits[i:]))
        if not found:
            raise HarnessError(f"{self.engine} seed of {c.table} row {i} is refused as a whole, no column alone: {why}")
        return Chunk(c.table, tuple(cols))

    def holds(self, col: Column, i: int, table: str) -> bool:
        """Whether the server creates a table of `col`'s type (i = 0) and takes its literal at `i` into it."""
        probe = f"{table}_p"
        stmts, _ = nsn.seed_sql(self.engine, [nsn.Col(col.name, col.native, col.delivery, col.family, col.lits[max(i, 1) - 1:max(i, 1)])], probe)
        ok = all(nsn.sql(self.engine, self.url, st + ";").ok for st in stmts[:2 if i == 0 else 3])
        nsn.sql(self.engine, self.url, f"DROP TABLE IF EXISTS {probe};")
        return ok

    def collection(self, chunk: Chunk) -> str:
        """The run-unique Mongo collection of a chunk; a load names its warehouse table after it."""
        return f"{chunk.table}_{self.run}"

    @contextmanager
    def mongo(self):
        """The stand's database through pymongo, a second client of the source, closed on exit."""
        try:
            from pymongo import MongoClient
        except ImportError as e:
            raise Unreachable("pymongo is not importable: the Mongo matrix cannot seed") from e
        with MongoClient(self.url, serverSelectionTimeoutMS=5000) as client:
            yield client.get_default_database()

    def seed_mongo(self, chunks: list[Chunk]) -> None:
        """One collection per chunk: a document per value with `_id` its id, then the NULL document."""
        with self.mongo() as db:
            try:
                db.client.admin.command("ping")
            except Exception as e:
                raise Unreachable(f"the mongo stand did not answer: {type(e).__name__}: {e}"[:200]) from e
            for c in chunks:
                db.drop_collection(self.collection(c))
                col = c.cols[0]
                docs = [{"_id": i, col.name: te.mongo_value(tm.lit_at(col, i, c))} for i in (*range(1, len(col.lits) + 1), c.null_id)]
                try:
                    db[self.collection(c)].insert_many(docs)
                except Exception as e:
                    raise HarnessError(f"mongo seed of {c.table} refused: {type(e).__name__}: {e}"[:300]) from e

    def drop(self, chunks: list[Chunk]) -> None:
        """Drop the seed tables."""
        if self.engine == "mongo":
            with self.mongo() as db:
                for c in chunks:
                    db.drop_collection(self.collection(c))
            return
        for c in chunks:
            nsn.sql(self.engine, self.url, f"DROP TABLE IF EXISTS {c.table};")

    def extract(self, u: Unit, ids: tuple[int, ...], budget: list[int]) -> dict[int, Got]:
        """Export `ids` to local Parquet and read it with DuckDB; a coded refusal of a group is bisected to the ids that cause it."""
        d = self.cwd("x")
        out = d / "out"
        (d / "c.yaml").write_text(self.config(u.chunk, u.col, ids, "nsn", {"type": "local", "path": str(out)}))
        p = nsn.rivet(["run", "-c", str(d / "c.yaml")], {"RIVET_NSN_URL": self.url}, d)
        if p.returncode == 0:
            read = nsn.read_parts(out, "parquet", self.key)
            rows = read[1] if read else {}
            return {i: Got(rows[i].get(self.read_col or u.col.name)) if i in rows else Got(MISSING) for i in ids}
        v = stop_verdict(p, te.EXTRACT)
        budget[0] -= 1
        if len(ids) == 1 or self.key == "_id" or budget[0] <= 0 or v.code == "CRASH":
            return {i: Got(refused=v) for i in ids}
        half = len(ids) // 2
        return {**self.extract(u, ids[:half], budget), **self.extract(u, ids[half:], budget)}

    def load(self, t: Target, u: Unit, ids: tuple[int, ...], budget: list[int]) -> dict[int, Got]:
        """Stage `ids`, load them into `t` and read them back; a group the load refuses is bisected like an extract."""
        table = self.collection(u.chunk) if self.key == "_id" else f"tc_nsn_{self.run}_{next(self.n)}"
        prefix = f"{self.run}/{table}"
        d = self.cwd("l")
        (d / "c.yaml").write_text(self.config(u.chunk, u.col, ids, table, t.stage(prefix), t.load_block()))
        env = {"RIVET_NSN_URL": self.url, **t.env()}
        with self.lock:
            self.tables.append(table)
            self.prefixes.append(prefix)
        p = nsn.rivet(["run", "-c", str(d / "c.yaml")], env, d)
        if p.returncode != 0:
            return {i: Got(refused=stop_verdict(p, te.EXTRACT)) for i in ids}
        p = nsn.rivet(["load", "-c", str(d / "c.yaml")], env, d)
        if p.returncode == 0:
            try:
                rows = t.read(table, self.read_col or u.col.name, self.key)
            except (OSError, RuntimeError, ValueError) as e:
                raise Unreachable(f"the {t.name} read-back failed: {type(e).__name__}: {e}"[:300]) from e
            return {i: Got(rows[i]) if i in rows else Got(MISSING) for i in ids}
        v = stop_verdict(p, te.LOAD)
        budget[0] -= 1
        if len(ids) == 1 or self.key == "_id" or budget[0] <= 0 or v.code == "CRASH":
            return {i: Got(refused=v) for i in ids}
        half = len(ids) // 2
        return {**self.load(t, u, ids[:half], budget), **self.load(t, u, ids[half:], budget)}

    def split(self, t: Target, u: Unit, have: dict[int, Got]) -> None:
        """Note the coded load-stage refusals of a unit at `t` that `rivet check` of the same config let through (it reads neither the values nor the format); asked once per column."""
        codes = {g.refused.code for g in have.values() if g.refused is not None and g.refused.stage == te.LOAD and g.refused.code not in (te.UNCODED, "CRASH")}
        key = (u.chunk.table, u.col.name, t.name)
        if not codes:
            return
        with self.lock:
            rc = self.checked.get(key)
        if rc is None:
            d = self.cwd("k")
            (d / "c.yaml").write_text(self.config(u.chunk, u.col, u.ids, "chk", t.stage(f"{self.run}/chk"), t.load_block()))
            p = nsn.rivet(["check", "-c", str(d / "c.yaml"), "--type-report", "--json"], {"RIVET_NSN_URL": self.url, **t.env()}, d)
            if nsn.stand_refused(p):
                raise Unreachable(f"the stand refused `rivet check` {nsn.STAND_TRIES} times: exit {p.returncode}")
            rc = p.returncode
            with self.lock:
                self.checked[key] = rc
        if rc == 0:
            with self.lock:
                self.splits.setdefault(key, set()).update(codes)

    def unit(self, u: Unit) -> None:
        """Observe one unit at the extract and at every target."""
        got = self.extract(u, u.ids, [BISECT_RUNS])
        with self.lock:
            for i, g in got.items():
                self.obs[(u.chunk.table, u.col.name, i, "duck")] = g
        deliverable = tuple(i for i in u.ids if got[i].refused is None)
        for t in self.targets:
            have = {i: got[i] for i in u.ids if got[i].refused is not None}
            if deliverable:
                have |= self.load(t, u, deliverable, [BISECT_RUNS])
            with self.lock:
                for i, g in have.items():
                    self.obs[(u.chunk.table, u.col.name, i, t.name)] = g
            if t.split:
                self.split(t, u, have)

    def results(self, chunks: list[Chunk]) -> tuple[list[Result], list[dict]]:
        """Every observed cell graded against the ledger: Result rows for the ledger and the observed verdicts as JSON-able dicts."""
        out, seen = [], []
        names = ("duck", *(t.name for t in self.targets))
        for c in chunks:
            for col in c.cols:
                out += [Result(f"{self.engine}/{col.native}->none/load-{t}", REFUSED, nsn.SPLIT, f"check exit 0, run refused {sorted(codes)}")
                        for t in names if (codes := self.splits.get((c.table, col.name, t)))]
                ids = range(1, len(col.lits) + 1) if col.whole else (*range(1, len(col.lits) + 1), c.null_id)
                for i, target in ((i, t) for i in ids for t in names):
                    g = self.obs.get((c.table, col.name, i, target))
                    if g is None:
                        continue
                    if (held := self.unheld.get((c.table, col.name, i))) is not None:
                        out.append(Result(tm.cell_name(self.engine, col, held[0], target), REFUSED, NOT_GRADED, f"the {self.engine} server does not hold the literal: {held[1]}"))
                        continue
                    lit, want = tm.lit_at(col, i, c), tm.expected(col, i, c, target)
                    text = self.texts.get((c.table, col.name), MISSING)
                    if text is None:
                        out.append(Result(tm.cell_name(self.engine, col, lit, target), DELIVERED, NOT_GRADED, "the source's own text of the seeded values could not be read"))
                        continue
                    if g.refused is not None:
                        got = g.refused
                    elif g.value is MISSING:
                        got = te.Verdict(te.FORM, text="ROW_MISSING")
                    else:
                        got = tm.observe(self.engine, col, lit, g.value, want) if text is MISSING else tm.observe_server_text(text[i - 1], g.value)
                    k = tm.klass(want, got)
                    out.append(Result(tm.cell_name(self.engine, col, lit, target), REFUSED if got.kind == te.REFUSE else DELIVERED, k,
                                      f"expected {tm.spell(want)}, observed {tm.spell(got)}"))
                    seen.append({"engine": self.engine, "native": col.native, "lit": lit, "id": i, "target": target, "expected": want.spelled(),
                                 "observed": tm.spell(got), "klass": k})
        for col, why in self.unmade:
            out += [Result(tm.cell_name(self.engine, col, lit, t), REFUSED, NOT_GRADED, f"the {self.engine} server does not create the type: {why}") for lit in col.lits for t in names]
        return out, seen


def run_engine(engine: str, url: str, work: Path, targets: tuple[str, ...], progress=lambda s: None, only: str = "") -> tuple[list[Result], list[dict]]:
    """Seed, observe and grade one engine at `targets`; drops its seed tables and every warehouse object after."""
    m = Matrix(engine, url, work, targets)
    chunks = [Chunk(c.table, tuple(x for x in c.cols if only.lower() in x.native.lower())) for c in tm.chunks(engine)]
    chunks = [c for c in chunks if c.cols]
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    started: list[Target] = []
    try:
        chunks = m.seed(chunks)
        for t in m.targets:
            t.start(m.run)
            started.append(t)
        errors: list[Result] = []

        def one(u: Unit) -> None:
            try:
                m.unit(u)
                progress(f"{engine} {u.label()}")
            except Unreachable as e:
                errors.append(Result(f"{engine}/{u.col.native}->?", REFUSED, NOT_GRADED, str(e)[:300]))
            except HarnessError as e:
                errors.append(Result(f"{engine}/{u.col.native}->?", REFUSED, nsn.UNGRADED, str(e)[:300]))

        with cf.ThreadPoolExecutor(WORKERS) as ex:
            list(ex.map(one, [u for c in chunks for u in tm.units(c)]))
        results, seen = m.results(chunks)
        return results + errors, seen
    except Unreachable as e:
        return [Result(f"{engine}/*/matrix", REFUSED, NOT_GRADED, str(e)[:300])], []
    except HarnessError as e:
        return [Result(f"{engine}/*/matrix", REFUSED, tm.UNGRADED_SEED, str(e)[:300])], []
    finally:
        m.drop(chunks)
        for t in started:
            try:
                t.clean(m.tables, m.prefixes)
            except Exception as e:
                progress(f"cleanup of {t.name} failed: {type(e).__name__}: {e}")
        shutil.rmtree(work, ignore_errors=True)


def work_dir(name: str) -> Path:
    """The scratch directory of one lane."""
    return Path(os.environ.get("RIVET_ORACLE_WORK") or tempfile.gettempdir()) / name


def record_lane(led: Ledger, scenario: str, targets: tuple[str, ...], what: str) -> None:
    """Run every engine of the stand at `targets` and write its results to the ledger."""
    for engine in nsn.axes()["engines"]:
        if engine not in nsn.STAND:
            continue
        try:
            results, _ = run_engine(engine, nsn.STAND[engine], work_dir(scenario) / engine, targets, led.phase)
        except Exception as e:
            led.failed(engine, "-", scenario, "-", f"{scenario}[{engine}]: the matrix raised", f"{type(e).__name__}: {e}"[:600])
            continue
        nsn.record(led, engine, results, scenario=scenario, target=CH_URL if scenario == SCENARIO else "cloud", what=what,
                   cause=SkipCause.REDUCED if engine == "mongo" else SkipCause.ENV)


def verify_no_silent_null_load(led: Ledger) -> None:
    """Every ledger edge value of every engine, extracted to Parquet and loaded into ClickHouse, read back by a second client."""
    load_targets()
    record_lane(led, SCENARIO, STAND_TARGETS, "ledger cells read back")


def verify_no_silent_null_load_cloud(led: Ledger) -> None:
    """The same cells loaded into BigQuery and Snowflake; without credentials a skip the census grades, never a pass."""
    record_lane(led, CLOUD_SCENARIO, CLOUD_TARGETS, "ledger cells read back from the cloud")


def self_test() -> None:
    """Grade the grader: the targets come from the product, a refusal is read from its code, a value from the reader that holds it."""
    assert "clickhouse" in load_targets(), load_targets()
    assert ch_projection("Nullable(FixedString(16))", "c") == ("base64Encode(`c`)", True) and ch_projection("Array(Nullable(String))", "c")[1] and ch_projection("Array(Int32)", "c") == ("`c`", False)
    assert _json_of("BYTES", '"AQI="') == b"\x01\x02" and _json_of("NUMERIC", "1.50") == Decimal("1.50") and _json_of("INT64", '"7"') == 7
    assert _bq_value("ARRAY<STRUCT<item INT64>>", "ARRAY", '[{"item":"7"},{"item":null}]') == [7, None] and _bq_value("INT64", "INT64", "7") == 7
    from unittest import mock

    def paged(method, path, body=None):
        row = lambda v: {"f": [{"v": v}]}
        return (200, {"jobComplete": True, "jobReference": {"jobId": "j"}, "schema": {"fields": [{"name": "v"}]}, "rows": [row("a")], "pageToken": "t"}) if method == "POST" else (200, {"rows": [row("b")]})
    with mock.patch.object(gcp, "_bq", paged):
        assert gcp.bq_rows("p", "SELECT 1") == [{"v": "a"}, {"v": "b"}], "a query result page past the first is read"
    assert _json_of("FLOAT64", '"Infinity"') == float("inf") and _json_of("STRING", '"x"') == "x" and _json_of("JSON", "null") == "null"
    assert _sf_value("BINARY", "0102") == b"\x01\x02" and _sf_value("BOOLEAN", "true") is True and _sf_value("NUMBER", "12.50") == Decimal("12.50")
    assert sf_text("FLOAT", "c") == '"c"' and sf_text("TIME", "c") == 'TO_VARCHAR("c", \'HH24:MI:SS.FF9\')' and sf_text("TEXT", "c") == 'TO_VARCHAR("c")' and sf_text("VARIANT", "c") == 'TO_JSON("c")'
    assert sf_drops("D.S", ["t"]) == ["DROP TABLE IF EXISTS D.S.t", "DROP TABLE IF EXISTS D.S.t__changes", "DROP STAGE IF EXISTS D.S.rivet_stage_t", "DROP STAGE IF EXISTS D.S.rivet_stage_t__changes"]
    assert _sf_value("VARIANT", '{"a":-1.797693134862316e+308,"b":21.797693134862316e+308}') == '{"a":-1.7976931348623157e+308,"b":21.797693134862316e+308}' and _sf_value("ARRAY", "[1.797693134862316e+308]") == [1.7976931348623157e+308], "Snowflake's 16-digit largest double reads as itself"
    assert _sf_value("FLOAT", "inf") == float("inf") and _sf_value("ARRAY", "[1,2]") == [1, 2] and _sf_value("TEXT", None) is None
    assert stop_verdict(nsn._proc(5, "error: [RIVET_SOURCE_VALUE_UNREPRESENTABLE] x"), te.EXTRACT) == te.Verdict(te.REFUSE, code="RIVET_SOURCE_VALUE_UNREPRESENTABLE", stage=te.EXTRACT)
    assert stop_verdict(nsn._proc(1, "error: boom"), te.LOAD).code == te.UNCODED and stop_verdict(nsn._proc(101, ""), te.LOAD).code == "CRASH"
    quota = "error: [RIVET_LOAD_WAREHOUSE_REJECTED] HTTP 422: 090073 (22000): Warehouse 'W' cannot be resumed because resource monitor 'M' has exceeded its quota."
    for refused, exc in ((nsn._proc(1, quota), Unreachable), (nsn._proc(1, "error: [RIVET_SOURCE_CONNECT_FAILED]"), Unreachable), (nsn._proc(1, "error: [RIVET_CONFIG_X]"), HarnessError)):
        try:
            stop_verdict(refused, te.EXTRACT)
            raise SystemExit("a stand or harness refusal read as a verdict")
        except exc:
            pass
    col = Column("c001", "NUMBER(5)", "Int64", "int", None, ("1",), (te.Edge("1"),), te.Edge("NULL"))
    ch_ = Chunk("pm_tm0", (col,))
    m = Matrix("oracle", "u", Path("/nonexistent"), ("clickhouse",))
    cfg = yaml.safe_load(m.config(ch_, col, (1, 3), "t", {"type": "local", "path": "o"}, {"target": "clickhouse", "pk": ["id"]}))
    assert cfg["exports"][0]["query"] == "SELECT id, C001 FROM pm_tm0 WHERE id IN (1, 3)" and cfg["load"]["pk"] == ["ID"], cfg
    mg = Matrix("mongo", "u", Path("/nonexistent"), ())
    mcfg = yaml.safe_load(mg.config(ch_, col, (1,), "t", {"type": "local", "path": "o"}, {"target": "clickhouse", "pk": ["id"]}))
    assert mcfg["exports"][0]["table"] == f"pm_tm0_{mg.run}" and "query" not in mcfg["exports"][0] and mcfg["load"]["pk"] == ["_id"], mcfg
    ms = Matrix("mssql", "u", Path("/nonexistent"), ())
    assert "FROM dbo.pm_tm0" in yaml.safe_load(ms.config(ch_, col, (1,), "t", {"type": "local", "path": "o"}))["exports"][0]["query"]
    ms.obs = {("pm_tm0", "c001", 1, "duck"): Got(1), ("pm_tm0", "c001", 2, "duck"): Got(None)}
    rs, seen = ms.results([ch_])
    assert [r.klass for r in rs] == ["", ""] and seen[0]["observed"] == "exact", (rs, seen)
    ms.obs = {("pm_tm0", "c001", 1, "duck"): Got(None), ("pm_tm0", "c001", 2, "duck"): Got(None)}
    assert [r.klass for r in ms.results([ch_])[0]] == [nsn.NULLED, ""], "a value read back as NULL is red"
    ms.obs = {("pm_tm0", "c001", 1, "duck"): Got(refused=te.Verdict(te.REFUSE, code="RIVET_X", stage=te.EXTRACT))}
    assert [r.klass for r in ms.results([ch_])[0]] == [tm.UNEXPECTED_REFUSAL]
    two = Chunk("pm_tm0", (Column("c001", "INT", "Int64", "int", None, ("1", "2"), (te.Edge("1"), te.Edge("2")), te.Edge("NULL")),))
    real_sql, nsn.sql = nsn.sql, lambda e, u, st: nsn._proc(1 if ", 2, " in st else 0, "ERROR: out of range")
    try:
        held = ms.seed([two])[0]
    finally:
        nsn.sql = real_sql
    assert held.cols[0].lits == ("1", "NULL") and set(ms.unheld) == {("pm_tm0", "c001", 2)}, (held, ms.unheld)
    ms.obs = {("pm_tm0", "c001", i, "duck"): Got(None) for i in (1, 2)}
    assert [r.klass for r in ms.results([held])[0]] == [nsn.NULLED, NOT_GRADED], "a literal the server does not hold is not graded, never graded as NULL"
    json_col = Column("c002", "JSON", "Utf8", "text", None, ("1",), (te.Edge("1"),), te.Edge("NULL"))
    nsn.sql = lambda e, u, st: nsn._proc(1 if "JSON" in st and "CREATE" in st else 0, "ORA-43853: JSON type cannot be used")
    try:
        mk = Matrix("mssql", "u", Path("/nonexistent"), ())
        left = mk.seed([Chunk("pm_tm0", (col, json_col))])
    finally:
        nsn.sql = real_sql
    assert [x.name for x in left[0].cols] == ["c001"] and [c.name for c, _ in mk.unmade] == ["c002"], (left, mk.unmade)
    assert [r.klass for r in mk.results(left)[0]] == [NOT_GRADED], "a type the server will not create is a recorded skip, never green"
    ms.obs = {("pm_tm0", "c001", 1, "duck"): Got(MISSING)}
    assert [r.klass for r in ms.results([ch_])[0]] == [nsn.WRONG], "a row the reader lacks is not exact"
    money = Column("c002", "MONEY", "server_text", "decimal", None, ("12.34",), (te.Edge("12.34"),), te.Edge("NULL"))
    mp, mc = Matrix("postgres", "u", Path("/nonexistent"), ()), Chunk("pm_tm0", (money,))
    mp.texts = {("pm_tm0", "c002"): ["$12.34", None]}
    for delivered, klass_ in (("$12.34", ""), ("12.34", nsn.WRONG), (None, nsn.NULLED)):
        mp.obs = {("pm_tm0", "c002", 1, "duck"): Got(delivered), ("pm_tm0", "c002", 2, "duck"): Got(None)}
        assert mp.results([mc])[0][0].klass == klass_, (delivered, klass_, "a server_text column is graded against the server's text, not the seed literal")
    mp.texts = {("pm_tm0", "c002"): ["$12.34", None]}
    mp.obs = {("pm_tm0", "c002", 1, "duck"): Got(b"$12.34"), ("pm_tm0", "c002", 2, "duck"): Got(None)}
    assert mp.results([mc])[0][0].klass == "", "a server text a reader returns as bytes is read as text"
    mp.texts = {("pm_tm0", "c002"): None}
    assert [r.klass for r in mp.results([mc])[0]] == [NOT_GRADED, NOT_GRADED], "a server text that could not be read is not graded, never green"
    plain = Column("c003", "TEXT", "Utf8", "text", None, ("a",), (te.Edge("'a'"),), te.Edge("NULL"))
    real_text, nsn.sql, nsn.server_text_of = nsn.server_text_of, lambda e, u, st: nsn._proc(0), lambda u, t, c, n: [f"{t}.{c}.{n}"]
    try:
        mq = Matrix("postgres", "u", Path("/nonexistent"), ())
        mq.seed([Chunk("pm_tm0", (money, plain))])
    finally:
        nsn.sql, nsn.server_text_of = real_sql, real_text
    assert mq.texts == {("pm_tm0", "c002"): ["pm_tm0.c002.2"]}, "the server's text is read for the server_text columns of a seeded chunk, and only for them"
    assert nsn.server_text_graded("postgres", "server_text") and not nsn.server_text_graded("postgres", "inet_text") and not nsn.server_text_graded("mysql", "server_text")
    coded = {1: Got(refused=te.Verdict(te.REFUSE, code="RIVET_X", stage=te.LOAD))}
    uncoded = {1: Got(refused=te.Verdict(te.REFUSE, code=te.UNCODED, stage=te.LOAD))}
    extract_stage = {1: Got(refused=te.Verdict(te.REFUSE, code="RIVET_Y", stage=te.EXTRACT))}
    key = ("pm_tm0", "c001", "clickhouse")
    real_rivet = nsn.rivet
    try:
        with tempfile.TemporaryDirectory() as td:
            for rc, have, want in ((0, coded, {key: {"RIVET_X"}}), (2, coded, {}), (0, uncoded, {}), (0, extract_stage, {}), (0, extract_stage | {2: coded[1]}, {key: {"RIVET_X"}}), (0, {1: Got(1)}, {})):
                mc = Matrix("mssql", "u", Path(tempfile.mkdtemp(dir=td)), ("clickhouse",))
                mc.targets[0].db = "x"
                nsn.rivet = lambda a, e, c: nsn._proc(rc)
                mc.split(mc.targets[0], Unit(ch_, col, (1,), True), have)
                assert mc.splits == want, (rc, have, mc.splits)
            nsn.rivet = lambda a, e, c: nsn._proc(1, "error: [RIVET_SOURCE_CONNECT_FAILED] x")
            try:
                mc.split(mc.targets[0], Unit(ch_, col, (1,), True), coded)
                raise SystemExit("a stand that refused `rivet check` was read as a clean check")
            except Unreachable:
                pass
    finally:
        nsn.rivet = real_rivet
    ms.targets = [ClickHouse()]
    ms.splits, ms.obs = {key: {"RIVET_X"}}, {}
    assert [(r.cell, r.klass) for r in ms.results([ch_])[0]] == [("mssql/NUMBER(5)->none/load-clickhouse", nsn.SPLIT)], "a refusal `rivet check` let through is a split"
    print(f"no_silent_null_load self-test ok: targets {load_targets()}")


def main() -> int:
    """Developer entry: run the matrix at chosen engines and targets; `--observe` writes every cell's observed verdict as JSON."""
    import argparse

    ap = argparse.ArgumentParser(prog="no_silent_null_load")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--engines", default="postgres,mysql,mssql,oracle,mongo")
    ap.add_argument("--targets", default="duck,clickhouse")
    ap.add_argument("--url", action="append", default=[], help="engine=URL to run an engine on another server")
    ap.add_argument("--only", default="", help="only the columns whose native type contains this text")
    ap.add_argument("--work", default=str(Path(tempfile.gettempdir()) / "no_silent_null_load"))
    ap.add_argument("--observe", default="", help="write every cell's observed verdict to this JSON file")
    ns = ap.parse_args()
    if ns.self_test:
        tm.self_test()
        self_test()
        return 0
    print(nsn.binary_line(), flush=True)
    urls = {**nsn.STAND, **dict(u.split("=", 1) for u in ns.url)}
    targets = tuple(ns.targets.split(","))
    results, seen = [], []
    for engine in ns.engines.split(","):
        r, s = run_engine(engine, urls[engine], Path(ns.work) / engine, targets, lambda m: print(m, flush=True), ns.only)
        results += r
        seen += s
    if ns.observe:
        Path(ns.observe).write_text(json.dumps(seen, indent=0))
    for r in sorted(results, key=lambda r: r.cell):
        print(f"{SCENARIO} {r.cell.replace(' ', '_')} {'NOT_GRADED' if r.klass == NOT_GRADED else 'RED' if r.klass else 'PASS'} {r.klass} {r.detail}"[:400].encode("utf-8", "backslashreplace").decode())
    print(nsn.summary_line(results).replace(nsn.SCENARIO, SCENARIO, 1))
    return nsn.exit_code(results)


if __name__ == "__main__":
    raise SystemExit(main())
