"""Server-truth corpus of statement text: one engine module per source, one fixture directory per engine, one check."""
import argparse
import gzip
import io
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "dev/fixtures/statement_truth"
sys.path.insert(0, str(Path(__file__).resolve().parent))

import statement_truth_mysql  # noqa: E402

ENGINES = {"mysql": statement_truth_mysql}


def engine_module(engine: str):
    """The generator module of one engine, from the static table."""
    if engine not in ENGINES:
        sys.exit(f"unknown engine {engine!r}; known: {sorted(ENGINES)}")
    return ENGINES[engine]


def fixture_path(engine: str, server: str) -> Path:
    """Where the corpus of one server of one engine lives."""
    return FIXTURES / engine / f"{server}.jsonl.gz"


def dump(header: dict, rows: list[dict]) -> bytes:
    """The fixture bytes: a header line then one row per line, compressed without a timestamp."""
    lines = [json.dumps(header, ensure_ascii=False, sort_keys=True, separators=(",", ":"))]
    lines += [json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(",", ":")) for r in rows]
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=9, mtime=0) as gz:
        gz.write(("\n".join(lines) + "\n").encode())
    return buf.getvalue()


def load(path: Path) -> tuple[dict, list[dict]]:
    """The header and rows of a fixture."""
    with gzip.open(path, "rt", encoding="utf-8") as f:
        lines = f.read().splitlines()
    return json.loads(lines[0]), [json.loads(l) for l in lines[1:]]


def diff_rows(old: list[dict], new: list[dict]) -> dict:
    """Rows only in the fixture, only in the regenerated corpus, and with the same id but another body."""
    a = {r["id"]: r for r in old}
    b = {r["id"]: r for r in new}
    return {
        "removed": sorted(a.keys() - b.keys()),
        "added": sorted(b.keys() - a.keys()),
        "changed": sorted(k for k in a.keys() & b.keys() if a[k] != b[k]),
    }


def generate_one(engine: str, server: str, subset: str | None) -> tuple[str, dict, list[dict], float]:
    """Regenerate the corpus of one server and time it."""
    start = time.monotonic()
    header, rows = engine_module(engine).generate(server, subset)
    return server, header, rows, time.monotonic() - start


def generate_all(engine: str, servers: list[str], subset: str | None) -> list[tuple[str, dict, list[dict], float]]:
    """Regenerate the servers side by side; every server is its own container."""
    with ThreadPoolExecutor(len(servers)) as pool:
        return list(pool.map(lambda s: generate_one(engine, s, subset), servers))


def cmd_generate(engine: str, servers: list[str], subset: str | None) -> int:
    """Rewrite the fixture of each server."""
    for server, header, rows, secs in generate_all(engine, servers, subset):
        path = fixture_path(engine, server)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = dump(header, rows)
        path.write_bytes(data)
        print(f"{engine} {server}: {len(rows)} rows, {len(data)} bytes, generated in {secs:.0f}s, server {header['version']}")
    return 0


def cmd_check(engine: str, servers: list[str], subset: str | None) -> int:
    """Regenerate each server's corpus and diff it against its fixture; the server version alone never fails."""
    bad = 0
    for server, header, rows, secs in generate_all(engine, servers, subset):
        path = fixture_path(engine, server)
        if not path.exists():
            print(f"{engine} {server}: no fixture at {path}")
            bad += 1
            continue
        old_header, old_rows = load(path)
        d = diff_rows(old_rows, rows)
        head = {k: v for k, v in header.items() if k != "version"} == {k: v for k, v in old_header.items() if k != "version"}
        drift = "" if header["version"] == old_header["version"] else f" (server {old_header['version']} -> {header['version']})"
        if head and not any(d.values()):
            print(f"{engine} {server}: corpus matches the server, {len(rows)} rows, {secs:.0f}s{drift}")
            continue
        bad += 1
        print(f"{engine} {server}: corpus differs from the server{drift}: {len(d['removed'])} removed, {len(d['added'])} added, {len(d['changed'])} changed; header {'same' if head else 'differs'}")
        for kind in ("removed", "added", "changed"):
            for k in d[kind][:5]:
                print(f"  {kind}: {k}")
    return 1 if bad else 0


def compose(profile: str, *args: str) -> int:
    """One docker compose call against the repo compose file under the shared project name."""
    env = {**os.environ, "COMPOSE_PROJECT_NAME": os.environ.get("COMPOSE_PROJECT_NAME", "rivet")}
    return subprocess.call(["docker", "compose", "--profile", profile, *args], cwd=ROOT, env=env)


def cmd_up(engine: str, servers: list[str], subset: str | None) -> int:
    """Start the stand services of the servers and wait for their healthchecks."""
    mod = engine_module(engine)
    start = time.monotonic()
    rc = compose(mod.PROFILE, "up", "-d", "--wait", *[mod.SERVERS[s].service for s in servers])
    print(f"stand up in {time.monotonic() - start:.0f}s")
    return rc


def cmd_down(engine: str, servers: list[str], subset: str | None) -> int:
    """Stop and remove only the stand services of the servers."""
    mod = engine_module(engine)
    return compose(mod.PROFILE, "rm", "-fsv", *[mod.SERVERS[s].service for s in servers])


def cmd_seed(engine: str, servers: list[str], subset: str | None) -> int:
    """Re-apply the SQL seed files to each server."""
    mod = engine_module(engine)
    for s in servers:
        mod.reseed(s)
        print(f"{engine} {s}: seed applied")
    return 0


def run_slice(engine: str, servers: list[str], subset: str, dump_to: str | None = None) -> int:
    """Generate a slice and print its size and speed; the fixture is never touched."""
    for server, header, rows, secs in generate_all(engine, servers, subset):
        if dump_to:
            Path(dump_to.replace("{server}", server)).write_bytes(dump(header, rows))
        kinds: dict[str, int] = {}
        for r in rows:
            kinds[r["expect"]] = kinds.get(r["expect"], 0) + 1
        print(f"{engine} {server}: slice {subset}: {len(rows)} rows in {secs:.0f}s, expect {kinds}, refused {header['refused']}")
    return 0


COMMANDS = {"up": cmd_up, "down": cmd_down, "seed": cmd_seed, "generate": cmd_generate, "check": cmd_check}


def self_test() -> int:
    """Every engine of the table has the generator surface and a fixture for each of its servers."""
    for engine, mod in ENGINES.items():
        for name in ("PROFILE", "SERVERS", "generate", "reseed"):
            assert hasattr(mod, name), f"{engine}: module has no {name}"
        for server in mod.SERVERS:
            assert fixture_path(engine, server).is_file(), f"{engine} {server}: no fixture at {fixture_path(engine, server)}"
        assert (FIXTURES / engine).is_dir()
    assert set(ENGINES) == {p.name for p in FIXTURES.iterdir() if p.is_dir()}, "fixture directory without an engine in the table"
    print(f"statement_truth self-test ok: {sorted(ENGINES)}")
    return 0


def main() -> int:
    """Command line: <engine> <up|down|seed|generate|check> [--server NAME ...] or --self-test."""
    if sys.argv[1:] == ["--self-test"]:
        return self_test()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("engine")
    ap.add_argument("command", choices=sorted(COMMANDS))
    ap.add_argument("--server", action="append")
    ap.add_argument("--dump", help="with --subset: write the slice to this path; {server} is replaced")
    ap.add_argument("--subset", help="an engine-defined slice of the corpus; the fixture is only written or checked whole")
    args = ap.parse_args()
    mod = engine_module(args.engine)
    servers = args.server or list(mod.SERVERS)
    unknown = [s for s in servers if s not in mod.SERVERS]
    if unknown:
        print(f"unknown server {unknown}; known: {list(mod.SERVERS)}", file=sys.stderr)
        return 2
    if args.subset and args.command in ("check", "generate"):
        return run_slice(args.engine, servers, args.subset, args.dump)
    return COMMANDS[args.command](args.engine, servers, args.subset)


if __name__ == "__main__":
    sys.exit(main())
