"""Apply a native fixture seed (seeds/fixture/<engine>.sql) and verify row counts and a content hash."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

try:
    from . import registry, shell
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import registry  # type: ignore[no-redef]
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
Fail = shell.Fail

ENGINES = {"postgresql": "postgres", "postgres": "postgres", "mysql": "mysql", "sqlserver": "mssql"}
KNOBS = ("users", "orders_per_user", "events_per_user", "page_views", "content_items",
         "coalesce_rows", "sparse_rows", "sparse_gap")
TIERS = {
    "standard": dict(users=1000, orders_per_user=5, events_per_user=5, page_views=5000,
                     content_items=60000, coalesce_rows=2000, sparse_rows=0, sparse_gap=2000000),
    "release": dict(users=1000, orders_per_user=5, events_per_user=5, page_views=5000,
                    content_items=1000000, coalesce_rows=2000, sparse_rows=0, sparse_gap=2000000),
}
TABLES = {
    "postgres": ("users", "orders", "events", "page_views", "content_items", "orders_sparse", "orders_coalesce"),
    "mysql": ("users", "orders", "events", "page_views", "content_items", "orders_sparse", "orders_coalesce"),
    "mssql": ("users", "orders", "events", "page_views", "content_items"),
}


def expected_counts(engine: str, k: dict[str, int]) -> dict[str, int]:
    """Row count each fixture table must hold after the seed, from the knobs alone."""
    all_counts = {
        "users": k["users"],
        "orders": k["users"] * k["orders_per_user"],
        "events": k["users"] * k["events_per_user"],
        "page_views": k["page_views"],
        "content_items": k["content_items"],
        "orders_sparse": k["sparse_rows"],
        "orders_coalesce": k["coalesce_rows"],
    }
    return {t: all_counts[t] for t in TABLES[engine]}


def header(engine: str, k: dict[str, int]) -> str:
    """The parameter lines in the engine's own dialect that precede the seed file."""
    fmt = {"postgres": "\\set {} {}\n", "mysql": "SET @{} = {};\n", "mssql": ":setvar {} {}\n"}[engine]
    return "".join(fmt.format(n, k[n]) for n in KNOBS)


def container_of(port: int) -> str:
    """The one running container publishing `port`."""
    out = shell.docker("ps", "--filter", f"publish={port}", "--format", "{{.Names}}", timeout=30).stdout.split()
    if len(out) != 1:
        raise Fail(f"port {port} is published by {len(out)} containers ({out}); pass --container")
    return out[0]


def client(engine: str, container: str, user: str, password: str, db: str) -> tuple[list[str], list[str]]:
    """`(env flags, argv)` of the engine's SQL client reading statements from stdin inside `container`."""
    if engine == "postgres":
        return ["-e", f"PGPASSWORD={password}"], ["psql", "-X", "-q", "-U", user, "-d", db, "-v", "ON_ERROR_STOP=1"]
    if engine == "mysql":
        return ["-e", f"MYSQL_PWD={password}"], ["mysql", "--default-character-set=utf8mb4", "-u", user, db]
    for path, extra in (("/opt/mssql-tools18/bin/sqlcmd", ["-C"]), ("/opt/mssql-tools/bin/sqlcmd", [])):
        if shell.docker_exec(container, "test", "-x", path, timeout=30).ok:
            return ["-e", f"SQLCMDPASSWORD={password}"], [path, *extra, "-S", "localhost", "-U", user, "-d", db, "-b", "-w", "4000"]
    raise Fail(f"{container}: no sqlcmd at tools18 or tools")


def sql(engine: str, container: str, creds: tuple[str, str, str], text: str, *, raw: bool = False) -> str:
    """Run `text` through the engine's client in `container`; `Fail` on any error, stdout otherwise."""
    flags, argv = client(engine, container, *creds)
    if raw:
        argv += {"postgres": ["-At", "-F", "|"], "mysql": ["-N", "-B"], "mssql": ["-h", "-1", "-W", "-s", "|"]}[engine]
    p = shell.run(["docker", "exec", "-i", *flags, container, *argv], stdin=text, timeout=3600)
    if not p.ok or (engine == "mssql" and "Msg " in p.stdout):
        raise Fail(f"{engine} client failed: {(p.stderr + p.stdout).strip()[-600:]}")
    return p.stdout


def measure(engine: str, container: str, creds: tuple[str, str, str]) -> dict[str, tuple[int, str]]:
    """`{table: (rows, content hash)}` read back from the server."""
    tables = TABLES[engine]
    out: dict[str, tuple[int, str]] = {}
    if engine == "postgres":
        q = " UNION ALL ".join(
            f"SELECT '{t}', count(*), coalesce(md5(string_agg(md5(x::text), '' ORDER BY id)), '') FROM {t} x"
            for t in tables)
        for line in sql(engine, container, creds, q + ";\n", raw=True).splitlines():
            t, n, h = line.split("|")
            out[t] = (int(n), h)
    elif engine == "mysql":
        counts = " UNION ALL ".join(f"SELECT '{t}', COUNT(*) FROM {t}" for t in tables)
        for line in sql(engine, container, creds, counts + ";\n", raw=True).splitlines():
            t, n = line.split("\t")
            out[t] = (int(n), "")
        for line in sql(engine, container, creds, f"CHECKSUM TABLE {', '.join(tables)};\n", raw=True).splitlines():
            t, h = line.split("\t")
            t = t.rsplit(".", 1)[-1]
            out[t] = (out[t][0], h)
    else:
        q = "SET NOCOUNT ON;\n" + " UNION ALL ".join(
            f"SELECT '{t}', COUNT_BIG(*), CHECKSUM_AGG(BINARY_CHECKSUM(*)) FROM {t}" for t in tables) + ";\n"
        for line in sql(engine, container, creds, q, raw=True).splitlines():
            parts = line.strip().split("|")
            if len(parts) == 3:
                out[parts[0]] = (int(parts[1]), parts[2])
    missing = set(tables) - set(out)
    if missing:
        raise Fail(f"{engine}: no measurement for {sorted(missing)}")
    return out


def seed(engine: str, container: str, creds: tuple[str, str, str], k: dict[str, int]) -> dict[str, tuple[int, str]]:
    """Apply the seed once and return what the server reads back; `Fail` if a row count is off."""
    body = (ROOT / "seeds" / "fixture" / f"{engine}.sql").read_text(encoding="utf-8")
    sql(engine, container, creds, header(engine, k) + body)
    got = measure(engine, container, creds)
    want = expected_counts(engine, k)
    bad = {t: (got[t][0], n) for t, n in want.items() if got[t][0] != n}
    if bad:
        raise Fail(f"row counts differ from the knobs (got, want): {bad}")
    return got


def parse(argv: list[str]) -> argparse.Namespace:
    """The command line."""
    p = argparse.ArgumentParser(prog="seed_fixture", description=__doc__)
    where = p.add_mutually_exclusive_group(required=True)
    where.add_argument("--source", help="stand registry key (postgres, mysql, mssql, ...)")
    where.add_argument("--url", help="postgresql://, mysql:// or sqlserver:// URL of the database to overwrite")
    p.add_argument("--container", help="container running the database (default: the registry's, else the one publishing the URL's port)")
    p.add_argument("--db", help="database name (default: the URL's)")
    p.add_argument("--tier", choices=sorted(TIERS), default="standard")
    for n in KNOBS:
        p.add_argument("--" + n.replace("_", "-"), type=int, dest=n)
    p.add_argument("--repeat", type=int, default=1, help="apply this many times; the read-back must be identical every time")
    p.add_argument("--yes", action="store_true", help="confirm that the fixture tables are truncated (SQL Server: dropped and recreated)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Seed one database from a tier plus overrides, verify it, and print `table rows hash`."""
    a = parse(sys.argv[1:] if argv is None else argv)
    url = a.url or registry.source(a.source)["url"]
    u = urlsplit(url)
    if u.scheme not in ENGINES:
        raise Fail(f"unsupported URL scheme: {u.scheme}", code=2)
    engine = ENGINES[u.scheme]
    db = a.db or u.path.lstrip("/")
    creds = (unquote(u.username or ""), unquote(u.password or ""), db)
    container = a.container or (registry.source(a.source)["container"] if a.source else container_of(u.port or 0))
    k = {n: (getattr(a, n) if getattr(a, n) is not None else TIERS[a.tier][n]) for n in KNOBS}
    if engine == "mssql" and (a.sparse_rows or a.coalesce_rows):
        raise Fail("SQL Server has no orders_sparse / orders_coalesce fixture tables", code=2)
    if not a.yes:
        raise Fail(f"refusing to overwrite the fixture tables {TABLES[engine]} in {container}/{db}: pass --yes")
    runs = [seed(engine, container, creds, k) for _ in range(a.repeat)]
    for t, (n, h) in runs[0].items():
        print(f"{t} {n} {h}")
    if any(r != runs[0] for r in runs[1:]):
        raise Fail("two applications of the same seed read back different rows or hashes")
    print(f"seed_fixture: {engine} {container}/{db} OK x{a.repeat}")
    return 0


if __name__ == "__main__":
    shell.main(main)
