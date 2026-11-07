"""Which engine, warehouse target or destination a live test module exercises, and the name rule that narrows a module to one owner."""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

#: What in a live module's text says it exercises an owner (case-insensitive); a module naming none belongs to every owner.
MODULE_MARKERS = {
    "postgres": r"\bpg_|Rig::pg|SqlEngine::Pg\b|POSTGRES_|postgres",
    "mysql": r"mysql",
    "mssql": r"mssql|sqlserver|sql server",
    "oracle": r"oracle",
    "mongo": r"mongo",
    "bigquery": r"bigquery|BqLive|\bbq_",
    "snowflake": r"snowflake|SfLive",
    "clickhouse": r"clickhouse",
    "azure": r"azure",
    "gcs": r"\bgcs|fake-gcs|fake_gcs",
    "s3": r"\bs3\b|minio",
    "init": r"\"init\"",
}
#: Owners of one kind: a test whose NAME names only others of the changed owner's kind cannot reach the change.
KINDS = (
    {"postgres": "pg|postgres|postgresql", "mysql": "mysql", "mssql": "mssql|sqlserver|sql_server",
     "oracle": "oracle", "mongo": "mongo|mongodb"},
    {"bigquery": "bigquery|bq", "snowflake": "snowflake|sf", "clickhouse": "clickhouse|ch"},
    {"azure": "azure", "gcs": "gcs", "s3": "s3"},
)
#: The source engines, which the release gate's `--engines` also names.
SOURCE_ENGINES = tuple(KINDS[0])
#: A test function and its body, up to the next item at column 0.
TEST_FN = re.compile(r"^#\[test\][^\n]*\n(?:#\[[^\n]*\n)*(?:pub )?fn ([a-z0-9_]+)\(\)[^\n]*\n(.*?)(?=^\S)", re.M | re.S)


def name_token(alts: str) -> str:
    """A regex for any of `alts` as a whole `_`-separated word of a test's full name."""
    return f"(?:^|_|::)(?:{alts})(?:_|$)"


def foreign_names(owners: set[str]) -> str | None:
    """The test-name regex of every owner that shares a kind with a changed owner and did not change; `None` when there is none."""
    others = [alts for kind in KINDS if owners & set(kind) for o, alts in kind.items() if o not in owners]
    return name_token("|".join(others)) if others else None


def own_names(owners: set[str]) -> str:
    """The test-name regex of the changed owners."""
    alts = [kind[o] for kind in KINDS for o in kind if o in owners]
    return name_token("|".join(alts)) if alts else "(?!)"


def module_owners(root: Path = ROOT) -> dict[str, set[str]]:
    """Every live module (`tests/live/<m>.rs`) and the owners its code names; an empty set means no owner."""
    out = {}
    for f in sorted((root / "tests" / "live").glob("*.rs")):
        text = "\n".join(l for l in f.read_text(errors="ignore").splitlines() if not l.lstrip().startswith("//"))
        out[f.stem] = {o for o, pat in MODULE_MARKERS.items() if re.search(pat, text, re.I)}
    return out


def kept_by_body(chosen: list[str], owners: set[str], root: Path = ROOT) -> list[str]:
    """The tests of `chosen` the name rule would drop whose own body names a changed owner: they stay."""
    foreign = foreign_names(owners)
    if not foreign:
        return []
    f_re, o_re = re.compile(foreign), re.compile(own_names(owners))
    markers = [re.compile(MODULE_MARKERS[o], re.I) for o in owners if o in MODULE_MARKERS]
    keep = []
    for mod in chosen:
        text = (root / "tests" / "live" / f"{mod}.rs").read_text(errors="ignore") + "\n}\n"
        for name, body in TEST_FN.findall(text):
            full = f"{mod}::{name}"
            if f_re.search(full) and not o_re.search(full) and any(m.search(body) for m in markers):
                keep.append(full)
    return sorted(keep)


def narrow_foreign(picked: str, chosen: list[str], owners: set[str]) -> str:
    """`picked` minus the tests of `chosen` named for another owner of the changed owners' kind, the ones whose body names a changed owner kept."""
    foreign = foreign_names(set(owners))
    if not foreign:
        return picked
    back = "".join(f" | test(={t})" for t in kept_by_body(chosen, set(owners)))
    return f"({picked} - (test(/{foreign}/) - test(/{own_names(set(owners))}/)){back})"


def self_test() -> None:
    """The name rule keeps what it must, kept_by_body reads bodies, and the module files are the live_suite modules."""
    foreign = re.compile(foreign_names({"mysql"}))
    own = re.compile(own_names({"mysql"}))
    keep = lambda n: not foreign.search(n) or bool(own.search(n))
    assert keep("live_cdc::x_mysql") and keep("live_cdc::engine_agnostic") and keep("live_x::y_gcs")
    assert not keep("live_cdc::x_mssql") and not keep("live_cdc::pg_cdc_x") and not keep("live_x::y_mongo")
    assert keep("live_x::upgrade_continues"), "a word inside another word is not a token"
    assert keep("live_x::from_pg_to_mysql"), "a test naming the changed owner stays"
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "tests" / "live").mkdir(parents=True)
        (Path(d) / "tests" / "live" / "m.rs").write_text(
            "#[test]\nfn a_cell_mssql() {\n    run(SqlEngine::Mssql);\n}\n"
            "#[test]\nfn a_parity_mssql() {\n    compare(SqlEngine::Mssql, SqlEngine::Mysql);\n}\n")
        assert kept_by_body(["m"], {"mysql"}, Path(d)) == ["m::a_parity_mssql"], \
            "a foreign-named test whose body drives the changed engine is kept"
    mods = module_owners()
    suite = set(re.findall(r"^\s*mod ([a-z0-9_]+);", (ROOT / "tests/live_suite.rs").read_text(), re.M)) - {"common"}
    assert suite == set(mods), ("every live module file is a live_suite module, and back", suite ^ set(mods))
