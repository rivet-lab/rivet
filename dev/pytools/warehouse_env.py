"""The cloud env every live warehouse cell reads, filled from dev/stand/registry.yaml.

    python -m dev.pytools.warehouse_env -- CMD ...   fill the env, say which, then exec CMD
    python -m dev.pytools.warehouse_env --self-test  check which changed paths wake which warehouse

A run outside the release gate fills a warehouse only when the branch changed that warehouse's code
(OWN_PATHS, or SHARED_PATHS for both) or RIVET_WAREHOUSE_LIVE names it (bigquery, snowflake, all, none).
A variable the shell already set wins. BigQuery/GCS are filled when a Google credential is at
hand, Snowflake when the `snow` connection logs in and sees the schema; otherwise the line says
SKIP and the cells report that nothing was verified. The release gate and `make test-live` both
fill it here, so a full live run outside the gate checks the warehouse cells as the gate does.
"""

from __future__ import annotations

import json
import os
import re
import sys

from . import registry, shell

OWN_PATHS = {
    "bigquery": ("src/load/bigquery/", "src/load/bq_rest.rs", "tests/common/bigquery.rs",
                 "tests/type_roundtrip/bigquery_load.rs", "dev/release_oracle/bigquery.py", "dev/bigquery/"),
    "snowflake": ("src/load/snowflake.rs", "src/load/snowflake_rest.rs", "src/config/snowflake_account.rs",
                  "tests/common/snowflake.rs", "tests/live/live_snowflake_", "tests/type_roundtrip/snowflake_load.rs"),
}
SHARED_PATHS = ("src/load/", "src/destination/gcs.rs", "src/types/target.rs", "tests/common/warehouse.rs",
                "tests/harness/", "tests/oracle_fixture_matrix.rs", "tests/live/live_mongo_order_warehouses.rs",
                "dev/pytools/warehouse_env.py", "dev/stand/registry.yaml")
OTHER_TARGETS = ("src/load/clickhouse.rs",)


def bigquery() -> str:
    """Fill the BigQuery/GCS env where empty when a Google credential is at hand; the line saying which."""
    if not shell.run(["gcloud", "auth", "print-access-token"], timeout=120).ok:
        return ("BigQuery/GCS legs SKIP: no Google credential (`gcloud auth print-access-token` failed: "
                "`gcloud auth login` and `gcloud auth application-default login`)")
    project, bucket, location = registry.warehouse()
    for var, value in (("BQ_ORACLE_PROJECT", project), ("BQ_ORACLE_BUCKET", bucket),
                       ("BQ_ORACLE_DATASET", registry.bq_tmp("gate"))):
        os.environ[var] = os.environ.get(var) or value
    for var, value in (("BIGQUERY_TEST_PROJECT", os.environ["BQ_ORACLE_PROJECT"]),
                       ("RIVET_TEST_GCS_BUCKET", os.environ["BQ_ORACLE_BUCKET"]), ("BIGQUERY_TEST_LOCATION", location)):
        os.environ[var] = os.environ.get(var) or value
    return f"warehouse: BigQuery {os.environ['BQ_ORACLE_PROJECT']} ({location}), GCS bucket {os.environ['BQ_ORACLE_BUCKET']}"


def _snow_login(conn: str) -> dict:
    """The key-pair parameters of one `snow` connection (rivet signs in with the same account, user and key)."""
    listing = shell.run(["snow", "connection", "list", "--format", "json"], timeout=120)
    try:
        rows = json.loads((listing.stdout or "")[(listing.stdout or "").index("["):])
    except ValueError:
        return {}
    return next((r.get("parameters") or {} for r in rows if r.get("connection_name") == conn), {})


def snowflake() -> str:
    """Fill the Snowflake env where empty when the `snow` connection logs in and sees the schema (rivet's own login comes from that connection's account, user and key); the line saying which."""
    sf = registry.snowflake()
    conn = os.environ.get("RIVET_SF_CONNECTION") or sf["connection"]
    database = os.environ.get("RIVET_SF_DATABASE") or sf["database"]
    schema = os.environ.get("RIVET_SF_SCHEMA") or sf["schema"]
    probe = shell.run(["snow", "sql", "-c", conn, "--format", "json", "-q",
                       f"SHOW SCHEMAS LIKE '{schema}' IN DATABASE {database}"], timeout=120)
    if not probe.ok:
        why = " ".join(re.sub("[─-╿]", " ", probe.stderr or probe.stdout or "").split())[:200]
        return (f"Snowflake legs SKIP: the `snow` connection `{conn}` did not log in "
                f"(`snow connection test -c {conn}`): {why}")
    try:
        found = json.loads(probe.stdout or "[]")
    except json.JSONDecodeError:
        found = []
    if not found:
        return f"Snowflake legs SKIP: no schema {database}.{schema} for the `snow` connection `{conn}`"
    login = _snow_login(conn)
    for var, value in (("RIVET_SNOWFLAKE_ACCOUNT", login.get("account")), ("RIVET_SNOWFLAKE_USER", login.get("user")),
                       ("RIVET_SNOWFLAKE_KEY", os.path.expanduser(login.get("private_key_file") or "") or None),
                       ("RIVET_SNOWFLAKE_ROLE", login.get("role"))):
        if value:
            os.environ[var] = os.environ.get(var) or value
    missing = [v for v in ("RIVET_SNOWFLAKE_ACCOUNT", "RIVET_SNOWFLAKE_USER", "RIVET_SNOWFLAKE_KEY") if not os.environ.get(v)]
    if missing:
        return (f"Snowflake legs SKIP: the `snow` connection `{conn}` carries no key-pair login for rivet "
                f"(set {', '.join(missing)}, or add account, user and private_key_file to the connection)")
    for var, value in (("RIVET_SF_CONNECTION", conn), ("RIVET_SF_WAREHOUSE", sf["warehouse"]),
                       ("RIVET_SF_DATABASE", database), ("RIVET_SF_SCHEMA", schema),
                       ("RIVET_SF_STORAGE_INTEGRATION", sf["storage_integration"]),
                       ("SNOWFLAKE_TEST_CONNECTION", conn), ("SNOWFLAKE_TEST_DATABASE", database),
                       ("SNOWFLAKE_TEST_SCHEMA", schema), ("RIVET_SF_TEST_WAREHOUSE", sf["warehouse"]),
                       ("RIVET_SF_TEST_DATABASE", database), ("RIVET_SF_TEST_SCHEMA", schema)):
        os.environ[var] = os.environ.get(var) or value
    return (f"warehouse: Snowflake {os.environ['RIVET_SF_DATABASE']}.{os.environ['RIVET_SF_SCHEMA']} "
            f"(connection {os.environ['RIVET_SF_CONNECTION']}, warehouse {os.environ['RIVET_SF_WAREHOUSE']})")


def touches(kind: str, paths: list[str]) -> bool:
    """Whether any changed path is this warehouse's own code or code every warehouse load runs."""
    for path in paths:
        if path.startswith(OWN_PATHS[kind]):
            return True
        if path.startswith(SHARED_PATHS) and not path.startswith(OTHER_TARGETS + sum(OWN_PATHS.values(), ())):
            return True
    return False


def changed_paths() -> list[str] | None:
    """The paths this branch changed since origin/main, committed, staged, unstaged and untracked; None when git cannot say."""
    base = shell.run(["git", "merge-base", "HEAD", "origin/main"], timeout=60)
    diff = shell.run(["git", "diff", "--name-only", base.stdout.strip()], timeout=60) if base.ok else base
    new = shell.run(["git", "ls-files", "--others", "--exclude-standard"], timeout=60)
    if not (diff.ok and new.ok):
        return None
    return [line for line in (diff.stdout + new.stdout).splitlines() if line]


def wanted(kind: str, switch: str, paths: list[str] | None) -> str | None:
    """None when this warehouse runs; else the SKIP reason naming how to run it."""
    named = {w.strip() for w in switch.lower().split(",") if w.strip()}
    if kind in named or "all" in named:
        return None
    if "none" in named:
        return f"RIVET_WAREHOUSE_LIVE={switch} leaves {kind} out"
    if named:
        return f"RIVET_WAREHOUSE_LIVE={switch} does not name {kind}"
    if paths is None:
        return None
    if touches(kind, paths):
        return None
    return f"no {kind} code changed since origin/main (RIVET_WAREHOUSE_LIVE={kind} runs it)"


def fill(kind: str, paths: list[str] | None) -> str:
    """Fill one warehouse's env when this run wants it; the line saying which."""
    why = wanted(kind, os.environ.get("RIVET_WAREHOUSE_LIVE", ""), paths)
    label = {"bigquery": "BigQuery/GCS", "snowflake": "Snowflake"}[kind]
    if why:
        return f"{label} legs SKIP: {why}"
    return bigquery() if kind == "bigquery" else snowflake()


def self_test() -> int:
    """Check the path rule on fixed change lists."""
    cases = [
        ("bigquery", [], False), ("snowflake", [], False),
        ("bigquery", ["src/load/bigquery/ddl.rs"], True), ("snowflake", ["src/load/bigquery/ddl.rs"], False),
        ("snowflake", ["src/load/snowflake_rest.rs"], True), ("bigquery", ["src/load/snowflake_rest.rs"], False),
        ("snowflake", ["tests/live/live_snowflake_compact.rs"], True),
        ("bigquery", ["src/load/orchestrate.rs"], True), ("snowflake", ["src/load/orchestrate.rs"], True),
        ("snowflake", ["src/destination/gcs.rs"], True), ("bigquery", ["src/load/clickhouse.rs"], False),
        ("snowflake", ["src/load/clickhouse.rs"], False), ("bigquery", ["src/source/mysql/cdc.rs", "README.md"], False),
    ]
    bad = [(k, p) for k, p, want in cases if touches(k, p) != want]
    switches = [
        ("bigquery", "all", [], None), ("snowflake", "bigquery", ["src/load/snowflake.rs"], "does not name"),
        ("snowflake", "none", ["src/load/snowflake.rs"], "leaves"), ("bigquery", "", None, None),
        ("bigquery", "", [], "no bigquery code changed"), ("snowflake", "Snowflake", [], None),
    ]
    for kind, switch, paths, want in switches:
        got = wanted(kind, switch, paths)
        if (want is None) != (got is None) or (want and want not in got):
            bad.append((kind, switch))
    if bad:
        print(f"warehouse_env self-test FAILED: {bad}", file=sys.stderr)
        return 1
    print("warehouse_env self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    """Fill the env this branch's changes call for, print both lines, exec the command after `--`."""
    if argv == ["--self-test"]:
        return self_test()
    cmd = argv[1:] if argv[:1] == ["--"] else argv
    if not cmd:
        print(__doc__, file=sys.stderr)
        return 2
    paths = changed_paths()
    for line in (fill("bigquery", paths), fill("snowflake", paths)):
        print(f"  {line}", file=sys.stderr, flush=True)
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
