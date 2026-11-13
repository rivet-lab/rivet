"""Seam map for bug hunters: every site of each concept a process rule names, by static scan.

    python -m dev.pytools.seams [--out DIR] [--since REV] [--all]   write DIR/seams.md (capped, prod only) and DIR/seams.json (full)
    python -m dev.pytools.seams --seam NAME [--all]                 print one seam's section
    python -m dev.pytools.seams --check [--rules PATH]              every rule is mapped and every row finds its witness line
    python -m dev.pytools.seams --self-test
"""

from __future__ import annotations

import argparse
import bisect
import fnmatch
import json
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import NamedTuple

from dev.pytools.error_sites import TEST_MOD, blank, is_test_file
from dev.pytools.error_sites import sites as exit_sites

ROOT = Path(__file__).resolve().parents[2]
RULES = Path.home() / ".claude" / "rivet-process-rules.md"
GIT = "/usr/bin/git"
SCAN_ROOTS = ("src", "tests", "dev", "seeds", ".cargo")
SKIP_DIRS = {"target", ".claude", "node_modules", ".venv", "worktrees", "__pycache__", ".git"}
SUFFIXES = {".rs", ".py", ".sql", ".yaml", ".yml", ".toml", ".sh"}
COMMENT = {".rs": "//", ".py": "#", ".sh": "#", ".yaml": "#", ".yml": "#", ".toml": "#", ".sql": "--"}
MD_CAP = 25
MAX_BYTES = 2_000_000

ENGINES = {
    "source": {"postgres": ("postgres", "postgresql", "pg"), "mysql": ("mysql",), "mssql": ("mssql", "sqlserver"), "mongo": ("mongo", "mongodb"), "oracle": ("oracle", "oracledb"), "clickhouse": ("clickhouse",), "duckdb": ["duckdb"]},
    "warehouse": {"bigquery": ("bigquery", "bq"), "snowflake": ("snowflake",), "clickhouse": ("clickhouse",)},
    "store": {"gcs": ("gcs",), "s3": ("s3",), "azure": ("azure",), "local": ("local",), "stdout": ("stdout",)},
    "state": {"sqlite": ("sqlite",), "postgres": ("postgres", "postgresql", "pg")},
}
OPTIONAL = {"source": {"clickhouse", "duckdb"}, "store": {"stdout"}}

STATE_TABLES = r"(?:cdc_snapshot|chunk_run|chunk_task|export_harm|export_load_spec(?:_run)?|export_metrics|export_progression|export_schema|export_shape|export_state|file_manifest|keyset_range|load_run|load_spec_version|loaded_source_run|rivet_schema_version|run_aggregate|run_journal|run_shape|run_status|schema_version|state_lease|strategy_snapshot)\b"
STATE_NOUNS = r"(?:chunk|cursor|progress|progression|state|resume|journal|run|lease|spec|snapshot|shape|schema|manifest|metrics|harm|aggregate|status|checkpoint|keyset|committed|verified|export_names)"


class Seam(NamedTuple):
    """One row of the map: what to find, where, how to group it, the rules it serves and the incident line it must find."""

    name: str
    rules: tuple[str, ...]
    group: str
    pattern: str = ""
    globs: tuple[str, ...] = ("src/*",)
    engines: str = ""
    near: tuple[str, int] | None = None
    witness: tuple[tuple[str, str], ...] = ()
    unless: str = ""
    exclude: tuple[str, ...] = ()


SEAMS: tuple[Seam, ...] = (
    Seam("source engine copies (files)", (), "engine_paths", globs=("src/*", "tests/*", "dev/*"), engines="source"),
    Seam("source engine copies (match arms)", (), "engine_arms", r"\bSourceType::(\w+)", engines="source"),
    Seam("warehouse targets (files)", (), "engine_paths", globs=("src/*", "tests/*", "dev/*"), engines="warehouse"),
    Seam("warehouse targets (match arms)", (), "engine_arms", r"\b(?:Warehouse|LoadTarget|LoadTargetKind|ExportTarget|TargetType)::(\w+)", engines="warehouse"),
    Seam("object-store destinations (files)", (), "engine_paths", globs=("src/*", "tests/*", "dev/*"), engines="store"),
    Seam("object-store destinations (match arms)", (), "engine_arms", r"\bDestinationType::(\w+)", engines="store"),
    Seam("state backends (functions)", (), "engine_fns", engines="state"),
    Seam("state backends (match arms)", (), "engine_arms", r"\b(?:StateConn|StateRef)::(\w+)", engines="state"),
    Seam("refusal sites", ("A refusal that prints a remedy is raised through its coded seam",), "exits", witness=(("src/source/oracle/cdc.rs", r"return Err\(log_gap\(format!\("),)),
    Seam("guard reads its own history", ("A guard's own refusal must not become the evidence that lifts it",), "lines", r"\.has_landed_load\(|\bownership_of\(|\bhas_load_attempt\w*\(|(?i:\bload_run\b.*\bstatus\b|\bstatus\b.*\bload_run\b)", witness=(("src/load/ledger.rs", r"s\.has_landed_load\(fqtn\)"),)),
    Seam("stored-artifact guard refusals", ("A refusal that guards one entry point guards every sibling that reads the same record",), "lines", r"\bcodes::STATE_\w+", witness=(("src/pipeline/chunked/mod.rs", r"codes::STATE_CHUNK_PLAN_CHANGED"),)),
    Seam("lock acquire/release", ("A lock held through a file descriptor is released by an explicit unlock, never by close",), "lines", r"\bflock\b|\bfs2::|lock_exclusive|lock_shared|\bunlock\w*\(|\bLOCK_(?:EX|SH|UN|NB)\b|pg_(?:try_)?advisory_\w*|\bGET_LOCK\b|\bRELEASE_LOCK\b|sp_getapplock|sp_releaseapplock|DBMS_LOCK|\b(?:acquire|release|renew|take|drop)_\w*(?:lock|lease)\w*\(|\bstate_lease\b|\bLoadLease\w*", witness=(("src/state/load_lease.rs", r"libc::flock\(file\.as_raw_fd\(\), libc::LOCK_UN\)"),)),
    Seam("lock keys", ("A lock is keyed by exactly what it protects",), "lines", r"\b\w*lock_(?:key|name|id|path)\b|\b\w*lease_(?:key|name|id|scope)\b|advisory\w*\(\s*\w*hash|\bhashtext\(|\block_file\w*", witness=(("src/pipeline/chunked/mod.rs", r"pub fn run_lease_key\("),)),
    Seam("state/ledger/progress writes", ("Persisted progress records WHAT it is, not just its value",), "lines", r"(?i:\b(?:INSERT(?:\s+OR\s+REPLACE)?\s+INTO|UPDATE|DELETE\s+FROM|REPLACE\s+INTO)\s+)" + STATE_TABLES + r"|\.(?:record|set|update|insert|complete|fail|finalize|reset|clear|create|claim|settle|delete|accept|mark|save|write|put|advance)_\w*" + STATE_NOUNS + r"\w*\(", witness=(("src/state/cursor.rs", r'let sql = "INSERT INTO export_state \\'),)),
    Seam("state store reads", ("Persisted progress records WHAT it is, not just its value",), "lines", r"(?i:(?<!DELETE )\bFROM\s+)" + STATE_TABLES + r"|\.(?:get|list|count|latest|has|holds|load|read|fetch|stored)_\w*" + STATE_NOUNS + r"\w*\(|\.get_owned\(", witness=(("src/state/cursor.rs", r"\.get_owned\(&self\.key\.export_name"),)),
    Seam("stop/progress proxy", (), "lines", r"\.claim\w*\(|\babandon\w*|\brelease_\w*anchor\w*|\bno_oracle\b", ("src/*", "tests/*")),
    Seam("journal/ledger context", ("A pre-write stop is journaled where the ledger context is born",), "lines", r"\bbefore_write\b|\b\w*journal\w*\(|\bLoadJournal\w*|\bLedger\w*|\bledger_\w+\(|\bwrite_turn\w*", witness=(("src/load/bigquery/mod.rs", r"crate::load::before_write\(self\.api\(\)\)"),)),
    Seam("run liveness", ("A record of a run in progress is closed by whoever can prove the run dead",), "lines", r"\brun_status\w*|\bheartbeat\w*|\bliveness\b|\bis_alive\b|\bpid_alive\w*|\bprocess_alive\w*|\bbegin_run_under_lease\b|'interrupted'|\bmark_\w*(?:dead|abandon|stale|crash)\w*|\bstale_\w*run\w*|\breset_stale\w*|\breap\w*\(", witness=(("src/state/run_status_store.rs", r"UPDATE run_status SET status = 'interrupted'"),)),
    Seam("catalog reads", ("A generated config names server objects the catalog holds, not a convention",), "lines", r"(?i:information_schema|pg_catalog|\bpg_(?:class|attribute|index|namespace|constraint|type|inherits|partitioned_table|publication\w*|replication_slots|stat_\w+)\b|\bsys\.(?:tables|columns|indexes|objects|schemas|types|index_columns|key_constraints|partitions|identity_columns|computed_columns|change_tracking\w*)\b|\bcdc\.(?:change_tables|captured_columns)\b|\b(?:all|user|dba)_(?:tab_columns|tables|constraints|cons_columns|indexes|ind_columns|tab_partitions|objects)\b|\bsystem\.(?:tables|columns|parts|databases)\b|list_collection\w*|listCollections|\bSHOW\s+(?:COLUMNS|INDEX|KEYS|TABLES|CREATE\s+TABLE)\b)", ("src/*", "dev/*"), witness=(("src/init/mssql.rs", r"capture_instance FROM cdc\.change_tables"),)),
    Seam("order/choice near a catalog read", ("A choice the product makes from a catalog read is ordered by the product",), "lines", r"(?i:\bORDER\s+BY\b|\bLIMIT\s+1\b|\bTOP\s+1\b|\bFETCH\s+FIRST\b)|\.sort\w*\(|\.first\(\)|\.last\(\)|\.next\(\)|\.max_by\w*\(|\.min_by\w*\(|\[0\]", near=("catalog reads", 25), witness=(("src/preflight/mysql.rs", r'^\s*LIMIT 1",'),)),
    Seam("catalog order seam", ("A choice the product makes from a catalog read is ordered by the product",), "lines", r"\b\w+_primary_first\b|\bfn \w*(?:order|sort)\w*_keys\w*", witness=(("src/source/mysql/mod.rs", r"super::keyset_keys_primary_first\("),)),
    Seam("operator/source text reads", ("Code that reads operator or source text survives any text",), "lines", r"\w\[\w*\s*\.\.|\bquote_ident\w*|\bescape_\w+\(|\.split(?:_once|n|_whitespace|_at)?\(|\.char_indices\(|\.as_bytes\(\)\[|\bfrom_utf8\w*|\.strip_(?:prefix|suffix)\(|serde_yaml::from_|toml::from_|\.parse::<|\.find\(\s*['\"]|\.rfind\(|\.eq_ignore_ascii_case\(", ("src/config/*", "src/sql.rs", "src/url_text.rs", "src/redact.rs", "src/secret.rs", "src/plan/*", "src/preflight/*", "src/init/*", "src/source/*", "src/cli/*"), witness=(("src/sql.rs", r"after_select\[\.\.from_at\]"), ("src/sql.rs", r"let ident = &rest\[\.\.end\];"), ("src/sql.rs", r"rest\[i\.\.\]\.starts_with\(b\"\*/\"\)"))),
    Seam("credential mask sites", ("A credential is masked before anything is cut from the text that holds it",), "lines", r"\bredact\w*\(|\bmask\w*\(|\bscrub\w*\(|\bsanitiz\w*\(|\bexpose_secret\b|\bSecretString\b|\bSecret<|\bRedacted\w*", witness=(("src/config/source.rs", r"let url = &crate::redact::redact_keyword_passwords\(url\);"),)),
    Seam("text cut sites", ("A credential is masked before anything is cut from the text that holds it",), "lines", r"\.truncate\(|\.chars\(\)\.take\(|\btruncat\w*\(|\bshorten\w*\(|\bellipsi\w*|\bserver_and_path\(|\.split_at\(|\.split_once\(\"://\"\)|\bfloor_char_boundary\b", witness=(("src/config/source.rs", r"let rest = crate::url_text::server_and_path\(url\);"),)),
    Seam("env reads (src)", (), "lines", r"\benv::var(?:_os|s)?\b|\bvar_os\(|\bstd::env::vars\b"),
    Seam("env reads (tests/dev)", (), "lines", r"\benv::var(?:_os|s)?\b|\bvar_os\(|\bos\.environ\b|\bos\.getenv\(|\bgetenv\(", ("tests/*", "dev/*")),
    Seam("test-hook points", ("A fix for a race is proven by a cell that loses the race every time",), "lines", r"\btest_hook::\w+|\bmaybe_\w+_at\(|\bRIVET_TEST_(?:PAUSE|PANIC|ERROR)_AT\b|\bRIVET_TEST_HOOK\w*", ("src/*", "tests/*"), witness=(("tests/common/refusal.rs", r"const FAULT_ENVS: &\[&str\] = &\[\"RIVET_TEST_PANIC_AT\""),)),
    Seam("child-process polling (race harness)", ("A fix for a race is proven by a cell that loses the race every time",), "lines", r"\.try_wait\(", ("tests/common/*",), witness=(("tests/common/runner.rs", r"child\.try_wait\(\)\.expect\(\"try_wait rivet\"\)"),)),
    Seam("retries/backoff", ("A store's typed verdict decides a retry before any word of its text",), "lines", r"\bRetry\w*|\bretry_\w+|\b\w+_retry\b|\bretryable\b|\bmax_retries\b|\bbackoff\w*\(|\bBackoff\w*|\bis_transient\w*|\bis_temporary\b", witness=(("src/error/retry.rs", r"if e\.is_temporary\(\)"),)),
    Seam("typed error classification", ("A store's typed verdict decides a retry before any word of its text",), "lines", r"\.downcast_ref::<(?:CodedError|\w*Error\w*)>|\bSqlState::\w+|\.code\(\)\s*==\s*Some|\bErrorKind::\w+|\bclassify_\w+\(|\bstop_rank\(|\bis_(?:retryable|deadlock|timeout|not_found|permission)\w*\(", witness=(("src/destination/cloud.rs", r"Err\(e\) if e\.kind\(\) == opendal::ErrorKind::NotFound"),)),
    Seam("error text matching", ("A store's typed verdict decides a retry before any word of its text",), "lines", r"\.to_string\(\)\.(?:to_lowercase\(\)\.)?(?:contains|starts_with|ends_with)\(|\b(?:msg|message|text|err|error|e|lower)\.(?:contains|starts_with)\(\s*\"", witness=(("src/error/retry.rs", r"msg\.contains\(\"lost connection\"\)"),)),
    Seam("worker failure collection", ("A failure collected from a worker keeps its type until the run decides its exit",), "lines", r"\bFanIn\b|\bfn fail\w*\(|\bJoinSet\b|\bJoinHandle\b|\bJoinError\b|\bjoin_all\b|\btry_join\w*|\.join\(\)|\bthread::spawn\b|\btokio::spawn\b|\bspawn_blocking\b|\bscope\.spawn\b", witness=(("src/pipeline/fan_in.rs", r"pub\(crate\) fn fail\(&self, label: &str"),)),
    Seam("failure stringified into a list", ("A failure collected from a worker keeps its type until the run decides its exit",), "lines", r"\.push\(\s*(?:\([^()]*,\s*)?(?:Err\()?format!\(\"[^\"]*\{\w+:#\}|\.fail_chunk_task\(", witness=(("src/pipeline/chunked/sequential_checkpoint.rs", r"state\.fail_chunk_task\(&run_id, chunk_index, &msg"),)),
    Seam("panic prevention and catch sites", ("A dependency's panic is prevented before the call, never caught after it",), "lines", r"\bcatch_unwind\b|\bAssertUnwindSafe\b|\.is_panic\(\)|\binto_panic\b|\bpanic::set_hook\b|\bresume_unwind\b|\bfn connect_client\b|\bdriver_runtime\(", witness=(("src/connect/postgres.rs", r"pub\(crate\) fn connect_client\("),)),
    Seam("warehouse overwrite/replace", ("An incremental load never replaces an existing warehouse table",), "lines", r"(?i:\bOVERWRITE\b|\bWRITE_TRUNCATE\b|\bCREATE\s+OR\s+REPLACE\b|\bREPLACE\s+TABLE\b|\bTRUNCATE\s+TABLE\b|\bDROP\s+TABLE\b|\bwrite_disposition\b|\bWriteDisposition\b|\bEXCHANGE\s+TABLES\b)", ("src/load/*", "src/destination/*", "src/pipeline/*"), witness=(("src/load/bigquery/ddl.rs", r"let kw = if overwrite \{ \"OVERWRITE\" \}"),)),
    Seam("state-vs-destination checks", ("A state-vs-destination check is argued by the read it costs",), "lines", r"\blast_success_cursor_high\b|\bSTATE_CURSOR_PAST_MANIFEST\b|\breconcile_\w+\(|\bhead_object\b|\blist_objects\w*|\bobject_exists\w*|\.list_prefix\(|\bexists_in_\w+|\bverify_destination\w*", witness=(("src/pipeline/single.rs", r"super::finalize::last_success_cursor_high\(&plan\.destination, export\)"),)),
    Seam("destination reads (cost per kind)", ("A state-vs-destination check is argued by the read it costs",), "lines", r"\bimpl(?:<[^>]*>)?\s+(?:super::|crate::destination::)?Destination\s+for\b|\bfn (?:head|exists|stat|list_prefix|read\w*|get\w*)\(|\.head\(|\.exists\(|\.stat\(", ("src/destination/*",), witness=(("src/destination/local.rs", r"impl super::Destination for LocalDestination"),)),
    Seam("value-range planning", ("Work planned from a value range is bounded by what the range can hold",), "lines", r"\bchecked_(?:add|sub|mul|div)\b|\bsaturating_\w+|\bwrapping_\w+|\boverflowing_\w+|\b[iu](?:32|64|128)::(?:MAX|MIN)\b|\bdiv_ceil\b|\bstep_by\b|\bwhile \w+ <=? \w+_max\b|\bchunk_count\w*|\bPARTITIONED_\w+|\b\w+_windows\(|\.clamp\(", ("src/plan/*", "src/pipeline/*", "src/source/*", "src/state/keyset_range.rs", "src/tuning/*", "src/load/*"), witness=(("src/load/cdc.rs", r"parse\(PARTITIONED_DAYS\.0\)"),)),
    Seam("empty-source branches", ("An empty source is a result, never a failure",), "lines", r"\bSOURCE_PROBE_UNREADABLE\b|\baggregate_sql\(|\bquery_scalar\(|\bread_key_span\b|(?i:\brows?_?(?:count|read|written|total|fetched)\s*==\s*0|\bEmptySource\b|\bempty_source\w*)", witness=(("src/pipeline/keyset.rs", r"crate::error::codes::SOURCE_PROBE_UNREADABLE,"), ("src/pipeline/partition_expand.rs", r"crate::error::codes::SOURCE_PROBE_UNREADABLE,"))),
    Seam("result layout reads", ("A result is laid out by what it returned, not by what an earlier statement described",), "lines", r"\bresult_columns\(|\.columns\(\)|\.column_count\(\)|\bnum_columns\b|\.try_get\(\s*\d+|\.get::<[^>]+>\(\s*\d+|\.get\(\s*\d+\s*\)|\bdescribe\w*\(|\bResultSet\w*|\bstatement_truth\w*|\.column_names?\(", ("src/source/*", "src/preflight/*", "src/init/*", "src/load/*"), witness=(("src/source/oracle/mod.rs", r"let Some\(\(row_index, metas\)\) = result_columns\("),)),
    Seam("type and size boundaries", ("A type boundary is written once and tested at both sides of it",), "lines", r"\bdata_precision\b|\bNUMBER\(\d+|\bprecision\s*<=|\bconst \w+_(?:MAX|MIN)_\w+|\b(?:to|from)_arrow\w*|\barrow_type\w*|\bmap_type\w*|\btype_mapping\w*|\bddl_type\w*|\bbq_type\w*", ("src/sql.rs", "src/types/*", "src/source/*", "src/load/*", "src/format/*", "src/preflight/*", "src/init/*"), witness=(("src/sql.rs", r"pub\(crate\) const ORACLE_RANGE_INT_MAX_PRECISION: u32"),)),
    Seam("generated config", ("A generated config names server objects the catalog holds, not a convention",), "lines", r"\bcdc\.(?:change_tables|captured_columns)\b|\byaml_scaffold\w*|\bscaffold\w*\(|\brender_\w*(?:yaml|config)\w*|\bserde_yaml::to_\w+|\bto_yaml\w*|\bwrite_config\w*", ("src/init/*", "src/cli/*", "src/config/*"), witness=(("src/init/mssql.rs", r"capture_instance FROM cdc\.change_tables"),)),
    Seam("write-location upgrade", ("A change to where rivet writes is guarded at the upgrade, not only shimmed in the state",), "lines", r"\bDEST_RELATIVE_PATH_MOVED\b|\.path\b.*(?:join|resolve|canonicalize)|\bconfig_dir\b|\bcurrent_dir\(|(?i:\bfn\s+migrate\w*|\bADD\s+COLUMN\b|\bALTER\s+TABLE\b|\buser_version\b|\bschema_version\b|\bstate_path\w*|\bstate_url\w*|\bcopy_\w+_state\w*)", ("src/state/*", "src/cli/*", "src/config/*"), witness=(("src/config/mod.rs", r"crate::error::codes::DEST_RELATIVE_PATH_MOVED,"),)),
    Seam("seed load and read-back", ("A seed is graded by the bytes the server holds, not by the loader's exit",), "lines", r"\bdef seed_\w+|\bseed_engine\(|\bdef mysql_client\b|\bmysql_client\(|\bread_back\w*|\bcanary\w*", ("dev/release_oracle/*.py", "dev/pytools/shell.py"), witness=(("dev/release_oracle/__main__.py", r"^def seed_engine\("), ("dev/pytools/shell.py", r"^def mysql_client\("))),
    Seam("seed files", ("A seed is graded by the bytes the server holds, not by the loader's exit",), "files", "", ("seeds/*",), witness=(("seeds/common/mysql.sql", ""),)),
    Seam("silent return in live tests without skip_live", (), "lines", r"^\s*return;\s*$", ("tests/live/*",), unless=r"\bskip_live\b"),
    Seam("Command::new outside tests/common", (), "lines", r"\bCommand::new\(", ("tests/*",), exclude=("tests/common/*",)),
    Seam("ratchet pins", (), "lines", r"ratchet-pin:", ("src/*", "tests/*", "dev/*")),
    Seam("mutants exclusions", (), "lines", r"^\s*\"", (".cargo/mutants.toml",)),
    Seam("accepted-red ledgers", (), "files", r"\bKNOWN_RED\b|\bOPEN_DEFECTS\b|\bXFAIL\w*|\bskip_census\b|\b[A-Z_]+_CEILING\s*=", ("src/*", "tests/*", "dev/*")),
    Seam("seeded defects corpus", (), "files", "", ("dev/seeded/*.py", "dev/seeded/*.yaml")),
)

UNMAPPED: dict[str, str] = {
    "A scan over operator or source text reads it once": "semantic predicate, stays with the agent (no line marks a re-reading loop)",
    "A warehouse load is graded by the values a second client reads back": "semantic predicate, stays with the agent (an oracle property, not a product line)",
    "A refusal is reached on every engine, not only where the query happens to run": "semantic predicate, stays with the agent; proxy: the engine match-arms rows",
    "A stop keeps stored progress only when the progress holds something": "semantic predicate, stays with the agent; proxy: the stop/progress proxy row (claim, abandon, release anchor, no_oracle)",
    "A generated config is proven in the shape it is deployed in": "semantic predicate, stays with the agent (a cell-shape property; no witness line found)",
}

GROUPS = ("lines", "exits", "files", "engine_paths", "engine_fns", "engine_arms")
TOKEN = re.compile(r"[A-Za-z0-9]+")
FN = re.compile(r"\b(?:fn|def)\s+(\w+)")


def norm(title: str) -> str:
    """A rule title compared without case, spacing or quote style."""
    return re.sub(r"\s+", " ", title.replace("\u2019", "'").replace("`", "")).strip().lower()


def walk(root: Path) -> dict[str, str]:
    """Repo-relative path to text for every scannable file under the scan roots."""
    out = {}
    for top in SCAN_ROOTS:
        base = root / top
        if not base.is_dir():
            continue
        for p in sorted(base.rglob("*")):
            rel = p.relative_to(root)
            if p.suffix not in SUFFIXES or SKIP_DIRS.intersection(rel.parts) or not p.is_file() or p.stat().st_size > MAX_BYTES:
                continue
            out[rel.as_posix()] = p.read_text(errors="replace")
    return out


class Tree:
    """The scanned files and their per-line kind, computed once."""

    def __init__(self, files: dict[str, str]):
        """Hold the files; test-module line sets are filled on demand."""
        self.files = files
        self._tests: dict[str, set[int]] = {}
        self._lines: dict[str, list[str]] = {}

    def lines(self, path: str) -> list[str]:
        """The file split into lines."""
        if path not in self._lines:
            self._lines[path] = self.files[path].split("\n")
        return self._lines[path]

    def test_lines(self, path: str) -> set[int]:
        """1-based lines inside a `#[cfg(test)] mod { ... }` of a Rust file."""
        if path not in self._tests:
            out: set[int] = set()
            if path.endswith(".rs"):
                text = blank(self.files[path])
                starts = [i for i, c in enumerate(text) if c == "\n"]
                for m in TEST_MOD.finditer(text):
                    depth, end = 0, len(text)
                    for j in range(m.end() - 1, len(text)):
                        depth += (text[j] == "{") - (text[j] == "}")
                        if depth == 0:
                            end = j
                            break
                    out.update(range(bisect.bisect_left(starts, m.start()) + 1, bisect.bisect_left(starts, end) + 2))
            self._tests[path] = out
        return self._tests[path]

    def kind(self, path: str, line: int) -> str:
        """prod, test or dev for one site."""
        if not path.startswith(("src/", "tests/")):
            return "dev"
        if path.startswith("tests/") or is_test_file(path) or line in self.test_lines(path):
            return "test"
        return "prod"

    def site(self, path: str, line: int) -> dict:
        """One site record."""
        text = self.lines(path)[line - 1].strip() if line - 1 < len(self.lines(path)) else ""
        return {"path": path, "line": line, "kind": self.kind(path, line), "text": text[:160], "changed": False}


def selected(tree: Tree, seam: Seam) -> list[str]:
    """Paths matching any glob of the seam and none of its excludes, minus files that match its `unless`."""
    out = []
    for p in tree.files:
        if any(fnmatch.fnmatchcase(p, g) for g in seam.globs) and not any(fnmatch.fnmatchcase(p, g) for g in seam.exclude):
            if not seam.unless or not re.search(seam.unless, tree.files[p]):
                out.append(p)
    return out


def engine_lookup(engines: str) -> dict[str, str]:
    """Token to engine name for one engine set."""
    return {tok: eng for eng, toks in ENGINES[engines].items() for tok in toks}


def scan_lines(seam: Seam, tree: Tree, done: dict[str, dict]) -> dict:
    """Every line matching the seam's pattern, optionally only near another seam's sites."""
    rx = re.compile(seam.pattern)
    anchors: dict[str, list[int]] = {}
    if seam.near:
        for s in done[seam.near[0]]["sites"]:
            anchors.setdefault(s["path"], []).append(s["line"])
    out = []
    for path in selected(tree, seam):
        if seam.near and path not in anchors:
            continue
        marker = COMMENT.get(Path(path).suffix)
        for n, line in enumerate(tree.lines(path), 1):
            if marker and line.lstrip().startswith(marker):
                continue
            if rx.search(line) and (not seam.near or any(abs(n - a) <= seam.near[1] for a in anchors[path])):
                out.append(tree.site(path, n))
    return {"sites": out}


def scan_exits(seam: Seam, tree: Tree, done: dict[str, dict]) -> dict:
    """Early-exit sites of product Rust, as dev/pytools/error_sites.py finds them."""
    out = []
    for path in selected(tree, seam):
        if path.endswith(".rs") and not is_test_file(path) and not path.startswith("tests/"):
            out.extend(tree.site(path, n) | {"exit": k} for n, _, k in exit_sites(tree.files[path]))
    return {"sites": out}


def scan_files(seam: Seam, tree: Tree, done: dict[str, dict]) -> dict:
    """One site per file that matches the globs and, when given, the pattern (its first matching line)."""
    rx = re.compile(seam.pattern) if seam.pattern else None
    out = []
    for path in selected(tree, seam):
        if rx is None:
            out.append(tree.site(path, 1))
        elif m := rx.search(tree.files[path]):
            out.append(tree.site(path, tree.files[path].count("\n", 0, m.start()) + 1))
    return {"sites": out}


def concepts_of(found: dict[str, dict[str, list[dict]]], engines: str, wild: set[str] = frozenset()) -> dict:
    """Concepts present on two or more engines, each with its present and missing engines."""
    expected = set(ENGINES[engines]) - OPTIONAL.get(engines, set())
    out, sites = [], []
    for concept, per in sorted(found.items()):
        if len(per) < 2:
            continue
        rows = [s for eng in sorted(per) for s in per[eng]]
        sites.extend(rows)
        out.append({"concept": concept, "present": sorted(per), "missing": sorted(expected - set(per)), "wildcard": concept in wild, "prod": any(s["kind"] == "prod" for s in rows), "sites": rows})
    out.sort(key=lambda c: (not c["missing"], c["wildcard"], not c["prod"], -len(c["present"]), c["concept"]))
    return {"sites": sites, "concepts": out}


def scan_engine_paths(seam: Seam, tree: Tree, done: dict[str, dict]) -> dict:
    """Sibling files whose path differs only by an engine token, grouped by the templated path; an engine directory also stands for `<dir>.<ext>`."""
    look = engine_lookup(seam.engines)
    found: dict[str, dict[str, list[dict]]] = {}
    for path in selected(tree, seam):
        for m in TOKEN.finditer(path):
            if eng := look.get(m.group().lower()):
                concept = path[: m.start()] + "{engine}" + path[m.end() :]
                found.setdefault(concept, {}).setdefault(eng, []).append(tree.site(path, 1))
                if m.start() > 0 and path[m.start() - 1] == "/" and path[m.end() : m.end() + 1] == "/":
                    as_file = path[: m.start()] + "{engine}" + Path(path).suffix
                    found.setdefault(as_file, {}).setdefault(eng, []) or found[as_file][eng].append(tree.site(path, 1))
    return concepts_of(found, seam.engines)


def scan_engine_fns(seam: Seam, tree: Tree, done: dict[str, dict]) -> dict:
    """Sibling product functions whose name differs only by an engine token, grouped by the templated name."""
    look = engine_lookup(seam.engines)
    found: dict[str, dict[str, list[dict]]] = {}
    for path in selected(tree, seam):
        text = blank(tree.files[path]) if path.endswith(".rs") else tree.files[path]
        for m in FN.finditer(text):
            site = tree.site(path, text.count("\n", 0, m.start()) + 1)
            if site["kind"] != "prod":
                continue
            parts = m.group(1).split("_")
            for i, part in enumerate(parts):
                if eng := look.get(part.lower()):
                    concept = "_".join(parts[:i] + ["{engine}"] + parts[i + 1 :])
                    found.setdefault(concept, {}).setdefault(eng, []).append(site)
    return concepts_of(found, seam.engines)


def scan_engine_arms(seam: Seam, tree: Tree, done: dict[str, dict]) -> dict:
    """Rust functions that name engine variants of an enum, grouped per function; `_ =>` in the body is flagged."""
    look = engine_lookup(seam.engines)
    rx = re.compile(seam.pattern)
    found: dict[str, dict[str, list[dict]]] = {}
    wild: set[str] = set()
    for path in selected(tree, seam):
        if not path.endswith(".rs"):
            continue
        text = blank(tree.files[path])
        fns = [(m.start(), m.group(1)) for m in FN.finditer(text)]
        starts = [s for s, _ in fns]
        for m in rx.finditer(text):
            eng = look.get(m.group(1).lower())
            i = bisect.bisect_right(starts, m.start()) - 1
            if not eng or i < 0:
                continue
            concept = f"{path}::{fns[i][1]}"
            found.setdefault(concept, {}).setdefault(eng, []).append(tree.site(path, text.count("\n", 0, m.start()) + 1))
            end = starts[i + 1] if i + 1 < len(starts) else len(text)
            if re.search(r"(?<![\w.])_\s*=>", text[starts[i] : end]):
                wild.add(concept)
    return concepts_of(found, seam.engines, wild)


SCANNERS = {"lines": scan_lines, "exits": scan_exits, "files": scan_files, "engine_paths": scan_engine_paths, "engine_fns": scan_engine_fns, "engine_arms": scan_engine_arms}


def changed_files(root: Path, rev: str) -> set[str]:
    """Paths changed since REV in the working tree, untracked files included."""
    diff = subprocess.run([GIT, "diff", "--name-only", rev, "--"], cwd=root, capture_output=True, text=True, check=True).stdout
    new = subprocess.run([GIT, "ls-files", "--others", "--exclude-standard"], cwd=root, capture_output=True, text=True, check=True).stdout
    return {line for line in (diff + new).splitlines() if line}


def build(files: dict[str, str], seams: tuple[Seam, ...] = SEAMS, changed: set[str] | None = None) -> list[dict]:
    """Every seam's sites, restricted to and marked by the changed files when given."""
    tree, done, out = Tree(files), {}, []
    for seam in seams:
        got = SCANNERS[seam.group](seam, tree, done)
        done[seam.name] = got
        entry = {"name": seam.name, "rules": list(seam.rules), "group": seam.group, "engines": seam.engines, **got}
        if changed is not None:
            for s in entry["sites"]:
                s["changed"] = s["path"] in changed
            if "concepts" in entry:
                entry["concepts"] = [c for c in entry["concepts"] if any(s["changed"] for s in c["sites"])]
                entry["sites"] = [s for c in entry["concepts"] for s in c["sites"]]
            else:
                entry["sites"] = [s for s in entry["sites"] if s["changed"]]
        entry["count"] = len(entry["sites"])
        entry["by_kind"] = {k: sum(s["kind"] == k for s in entry["sites"]) for k in ("prod", "test", "dev")}
        out.append(entry)
    return out


def section(e: dict, cap: int = MD_CAP, all_kinds: bool = False) -> list[str]:
    """One seam's Markdown section; prod sites only unless `all_kinds` or the seam has no prod site."""
    order = {"prod": 0, "dev": 1, "test": 2}
    every = all_kinds or e["by_kind"]["prod"] == 0
    out = ["", f"## {e['name']}", ""] + [f"rule: {r}" for r in e["rules"]]
    if "concepts" in e:
        shown = [c for c in e["concepts"] if every or c["prod"] or any(s["changed"] for s in c["sites"])]
        gapped = sum(bool(c["missing"]) for c in shown)
        out.append(f"{len(e['concepts'])} concepts on 2+ engines; {len(shown)} {'in all' if every else 'with a prod site'}, {gapped} of those with a missing engine (listed first, {min(cap, len(shown))} shown)")
        for c in shown[:cap]:
            mark = "*" if any(s["changed"] for s in c["sites"]) else ""
            where = ", ".join(list(dict.fromkeys(f"{s['path']}:{s['line']}" for s in c["sites"]))[:2])
            miss = f" MISSING: {', '.join(c['missing'])}" if c["missing"] else ""
            wild = " (has `_ =>`)" if c["wildcard"] else ""
            out.append(f"- {mark}`{c['concept']}` has {', '.join(c['present'])};{miss}{wild} [{where}]")
    else:
        shown = [s for s in e["sites"] if every or s["kind"] == "prod" or s["changed"]]
        out.append(f"{e['count']} sites, {e['by_kind']['prod']} prod ({min(cap, len(shown))} of {len(shown)} {'sites' if every else 'prod sites'} shown)")
        for s in sorted(shown, key=lambda s: (not s["changed"], order[s["kind"]], s["path"], s["line"]))[:cap]:
            mark = "*" if s["changed"] else ""
            out.append(f"- {mark}`{s['path']}:{s['line']}` {s['kind']}: {s['text'][:72]}")
    return out


def render(seams: list[dict], rev: str, since: str | None, cap: int = MD_CAP, all_kinds: bool = False) -> str:
    """The compact Markdown map: a summary table, then one `## <seam name>` section per seam."""
    out = [f"# Seam map at {rev}" + (f", files changed since {since} only (`*` = changed)" if since else ""), "", "Generated by `python -m dev.pytools.seams`; prod sites only unless `--all`; the full list is seams.json; one section: `--seam NAME`.", "", "| seam | sites | prod | test | dev | gaps |", "|---|---|---|---|---|---|"]
    for e in seams:
        gaps = sum(bool(c["missing"]) for c in e["concepts"]) if "concepts" in e else ""
        out.append(f"| {e['name']} | {e['count']} | {e['by_kind']['prod']} | {e['by_kind']['test']} | {e['by_kind']['dev']} | {gaps} |")
    for e in seams:
        out += section(e, cap, all_kinds)
    return "\n".join(out) + "\n"


def rule_titles(text: str) -> list[str]:
    """The `## ` headings of the process-rules file."""
    return [m.group(1).strip() for m in re.finditer(r"^## (.+)$", text, re.M)]


def witness_problems(files: dict[str, str], seams: tuple[Seam, ...]) -> list[str]:
    """Rows that map a rule without a witness, and witnesses whose line is gone or that the row does not find."""
    problems = [f"seam row '{s.name}' maps a rule but names no witness line" for s in seams if s.rules and not s.witness]
    built = {e["name"]: e for e in build(files, seams)}
    for s in seams:
        for path, rx in s.witness:
            if path not in files:
                problems.append(f"witness of '{s.name}': {path} is not in the scanned tree")
                continue
            lines = [n for n, line in enumerate(files[path].split("\n"), 1) if re.search(rx, line)] if rx else [0]
            if not lines:
                problems.append(f"witness of '{s.name}': no line of {path} matches {rx!r}")
                continue
            hits = [x for x in built[s.name]["sites"] if x["path"] == path and (not rx or x["line"] in lines)]
            if not hits:
                problems.append(f"seam row '{s.name}' does not find its witness {path}:{lines[0] or 1}")
    return problems


def check(rules_path: Path, seams: tuple[Seam, ...] = SEAMS, unmapped: dict[str, str] | None = None, files: dict[str, str] | None = None) -> int:
    """0 when every rule is mapped by a seam row or UNMAPPED, every row names a real title and finds its witness; 1 otherwise; 2 without the rules file."""
    unmapped = UNMAPPED if unmapped is None else unmapped
    if not rules_path.is_file():
        print(f"seams --check: rules file {rules_path} is absent; pass --rules PATH")
        return 2
    titles = {norm(t): t for t in rule_titles(rules_path.read_text())}
    if not titles:
        print(f"seams --check: no `## ` rule title in {rules_path}")
        return 1
    mapped = {norm(r) for s in seams for r in s.rules}
    problems = [f"rule not mapped by any seam row or UNMAPPED: {t}" for k, t in titles.items() if k not in mapped and k not in {norm(u) for u in unmapped}]
    problems += [f"seam row '{s.name}' names a title the rules file does not hold: {r}" for s in seams for r in s.rules if norm(r) not in titles]
    problems += [f"UNMAPPED names a title the rules file does not hold: {t}" for t in unmapped if norm(t) not in titles]
    problems += [f"UNMAPPED lists a title a seam row maps: {t}" for t in unmapped if norm(t) in mapped]
    problems += witness_problems(walk(ROOT) if files is None else files, seams)
    witnesses = sum(len(s.witness) for s in seams)
    for p in problems:
        print(f"SEAMS: {p}")
    print(f"seams --check: {len(titles)} rule titles, {len([k for k in titles if k in mapped])} mapped by a row, {len(unmapped)} unmapped, {witnesses} witnesses, {len(problems)} problems")
    return 1 if problems else 0


def head_rev(root: Path) -> str:
    """The short HEAD revision, or `unknown` outside git."""
    p = subprocess.run([GIT, "rev-parse", "--short", "HEAD"], cwd=root, capture_output=True, text=True)
    return p.stdout.strip() or "unknown"


def write_map(root: Path, out_dir: Path, since: str | None, all_kinds: bool) -> list[dict]:
    """Scan the tree and write seams.md and seams.json into out_dir."""
    changed = changed_files(root, since) if since else None
    seams = build(walk(root), SEAMS, changed)
    out_dir.mkdir(parents=True, exist_ok=True)
    rev = head_rev(root)
    (out_dir / "seams.json").write_text(json.dumps({"rev": rev, "since": since, "seams": seams}, indent=1))
    (out_dir / "seams.md").write_text(render(seams, rev, since, all_kinds=all_kinds))
    return seams


def self_test() -> int:
    """A tiny fixture tree: engine gaps, prod-only sections, --since marking, --check verdicts and witness recall."""
    for s in SEAMS:
        assert s.group in GROUPS, s
        assert not s.pattern or re.compile(s.pattern)
        assert not s.engines or s.engines in ENGINES, s
        assert not s.near or s.near[0] in [t.name for t in SEAMS[: SEAMS.index(s)]], s
        assert not s.rules or s.witness, f"{s.name} maps a rule without a witness"
        assert all(not rx or re.compile(rx) for _, rx in s.witness), s
    assert len({s.name for s in SEAMS}) == len(SEAMS)
    files = {
        "src/source/postgres/cdc.rs": "fn start() -> Result<()> { bail!(\"no slot\"); }\n",
        "src/source/mysql/cdc.rs": "fn start() {}\n",
        "src/source/mongo/cdc.rs": "fn start() {}\n",
        "src/source/mssql/cdc.rs": "fn start() {}\n",
        "src/load/clickhouse.rs": "fn load() {}\n",
        "src/load/snowflake.rs": "fn load() {}\n",
        "src/load/bigquery/mod.rs": "fn load() {}\n",
        "src/load/bigquery/ddl.rs": "fn ddl() {}\n",
        "src/plan/mod.rs": "fn pick(t: SourceType) {\n    match t {\n        SourceType::Postgres => a(),\n        SourceType::Mysql => b(),\n    }\n}\nfn all(t: SourceType) {\n    match t {\n        SourceType::Postgres | SourceType::Oracle => a(),\n        _ => b(),\n    }\n}\n",
        "src/state/mod.rs": "fn open_sqlite() { std::env::var(\"X\"); }\nfn open_postgres() {}\n// std::env::var(\"COMMENTED\")\n#[cfg(test)]\nmod tests {\n    fn t() { std::env::var(\"Y\"); }\n    fn on_sqlite_state_postgres() {}\n    fn on_postgres_state_sqlite() {}\n}\n",
        "tests/live/a.rs": "fn t() {\n    if x {\n        return;\n    }\n    Command::new(\"rivet\");\n}\n",
        "tests/live/b.rs": "fn t() {\n    if x {\n        skip_live(\"ENV\");\n        return;\n    }\n}\n",
        "tests/common/run.rs": "fn r() { Command::new(\"rivet\"); }\n",
        "dev/release_oracle/known_red.py": "KNOWN_RED = {}\n",
    }
    got = {e["name"]: e for e in build(files)}
    paths = {c["concept"]: c for c in got["source engine copies (files)"]["concepts"]}
    assert paths["src/source/{engine}/cdc.rs"]["missing"] == ["oracle"], paths
    assert paths["src/source/{engine}/cdc.rs"]["present"] == ["mongo", "mssql", "mysql", "postgres"]
    wh = {c["concept"]: c for c in got["warehouse targets (files)"]["concepts"]}
    assert wh["src/load/{engine}.rs"]["present"] == ["bigquery", "clickhouse", "snowflake"] and not wh["src/load/{engine}.rs"]["missing"], wh
    assert len([s for s in wh["src/load/{engine}.rs"]["sites"] if "bigquery" in s["path"]]) == 1
    arms = {c["concept"]: c for c in got["source engine copies (match arms)"]["concepts"]}
    assert arms["src/plan/mod.rs::pick"]["missing"] == ["mongo", "mssql", "oracle"] and not arms["src/plan/mod.rs::pick"]["wildcard"], arms
    assert arms["src/plan/mod.rs::all"]["wildcard"], arms
    fns = {c["concept"]: c for c in got["state backends (functions)"]["concepts"]}
    assert list(fns) == ["open_{engine}"] and fns["open_{engine}"]["present"] == ["postgres", "sqlite"], fns
    env = got["env reads (src)"]["sites"]
    assert [(s["path"], s["line"], s["kind"]) for s in env] == [("src/state/mod.rs", 1, "prod"), ("src/state/mod.rs", 6, "test")], env
    assert [(s["path"], s["line"]) for s in got["refusal sites"]["sites"]] == [("src/source/postgres/cdc.rs", 1)]
    assert [(s["path"], s["line"]) for s in got["silent return in live tests without skip_live"]["sites"]] == [("tests/live/a.rs", 3)]
    assert [s["path"] for s in got["Command::new outside tests/common"]["sites"]] == ["tests/live/a.rs"]
    assert [s["path"] for s in got["accepted-red ledgers"]["sites"]] == ["dev/release_oracle/known_red.py"]
    md = render(list(got.values()), "abc", None)
    assert "MISSING: oracle" in md and "MISSING: mongo, mssql, oracle" in md
    env_md = "\n".join(section(got["env reads (src)"]))
    assert "src/state/mod.rs:1`" in env_md and "src/state/mod.rs:6`" not in env_md, env_md
    assert "src/state/mod.rs:6`" in "\n".join(section(got["env reads (src)"], all_kinds=True))
    assert "tests/live/a.rs:3`" in "\n".join(section(got["silent return in live tests without skip_live"]))
    capped = "\n".join(section(got["source engine copies (match arms)"], cap=1))
    assert "MISSING: mongo, mssql, oracle" in capped and "::all" not in capped, capped
    many = {**got["env reads (src)"], "sites": [dict(got["env reads (src)"]["sites"][0], line=n) for n in range(100)], "count": 100, "by_kind": {"prod": 100, "test": 0, "dev": 0}}
    assert "\n".join(section(many)).count("`src/state/mod.rs:") == MD_CAP
    seen = {"src/a.rs": "fn f() {\n    let s = &t[..n];\n}\n"}
    ok = (Seam("cut", ("T",), "lines", r"\w\[\w*\.\.", witness=(("src/a.rs", r"let s = &t\[\.\.n\]"),)),)
    assert witness_problems(seen, ok) == []
    assert any("does not find its witness src/a.rs:2" in p for p in witness_problems(seen, (ok[0]._replace(pattern=r"&\w+\[\w+\.\."),)))
    assert any("no line of src/a.rs matches" in p for p in witness_problems(seen, (ok[0]._replace(witness=(("src/a.rs", "gone"),)),)))
    assert any("names no witness line" in p for p in witness_problems(seen, (ok[0]._replace(witness=()),)))
    assert any("not in the scanned tree" in p for p in witness_problems(seen, (ok[0]._replace(witness=(("src/b.rs", "x"),)),)))
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for rel, text in files.items():
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text(text)
        assert set(walk(root)) == set(files)
        (root / "target").mkdir()
        (root / "src/.claude").mkdir()
        (root / "src/.claude/x.rs").write_text("std::env::var(\"Z\");\n")
        assert set(walk(root)) == set(files)
        run = lambda *a: subprocess.run([GIT, *a], cwd=root, capture_output=True, text=True, check=True)
        run("init", "-q")
        run("-c", "user.email=t@t", "-c", "user.name=t", "add", "-A")
        run("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base")
        (root / "src/source/mysql/cdc.rs").write_text("fn start() { let v = std::env::var(\"M\"); }\n")
        (root / "src/new.rs").write_text("fn n() {}\n")
        changed = changed_files(root, "HEAD")
        assert changed == {"src/source/mysql/cdc.rs", "src/new.rs"}, changed
        since = {e["name"]: e for e in build(walk(root), SEAMS, changed)}
        assert [(s["path"], s["changed"]) for s in since["env reads (src)"]["sites"]] == [("src/source/mysql/cdc.rs", True)], since["env reads (src)"]
        got_c = [c["concept"] for c in since["source engine copies (files)"]["concepts"]]
        assert sorted(got_c) == ["src/source/{engine}.rs", "src/source/{engine}/cdc.rs"], got_c
        assert [s["changed"] for s in since["source engine copies (files)"]["sites"]].count(True) == 2
        assert since["source engine copies (match arms)"]["concepts"] == [] and since["refusal sites"]["sites"] == []
        assert "*`src/source/mysql/cdc.rs:1`" in render(list(since.values()), "abc", "HEAD")
        rules = root / "rules.md"
        assert check(root / "absent.md") == 2
        rules.write_text("# rivet\n\n## Mapped title\n\nbody\n\n## Unmapped title\n")
        mapped = (ok[0]._replace(rules=("Mapped title",)),)
        assert check(rules, mapped, {}, seen) == 1
        assert check(rules, mapped, {"Unmapped title": "no product seam"}, seen) == 0
        assert check(rules, mapped + (ok[0]._replace(name="y", rules=("Unmapped title",)),), {}, seen) == 0
        assert check(rules, mapped + (ok[0]._replace(name="y", rules=("Gone title",)),), {"Unmapped title": "r"}, seen) == 1
        assert check(rules, (mapped[0]._replace(pattern=r"&\w+\[\w+\.\."),), {"Unmapped title": "r"}, seen) == 1
    print("seams self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    """Write the map, print one section, check rule coverage and witnesses, or run the self-test."""
    ap = argparse.ArgumentParser(prog="python -m dev.pytools.seams", description="Seam map for bug hunters.")
    ap.add_argument("--out", type=Path, default=ROOT / "target" / "seams")
    ap.add_argument("--since")
    ap.add_argument("--all", action="store_true", dest="all_kinds")
    ap.add_argument("--seam")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--rules", type=Path, default=RULES)
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args(argv)
    if args.self_test:
        return self_test()
    if args.check:
        return check(args.rules)
    if args.seam:
        names = [s.name for s in SEAMS]
        if args.seam not in names:
            print(f"seams: no seam named {args.seam!r}; one of: {', '.join(names)}")
            return 2
        changed = changed_files(ROOT, args.since) if args.since else None
        entry = next(e for e in build(walk(ROOT), SEAMS, changed) if e["name"] == args.seam)
        print("\n".join(section(entry, all_kinds=args.all_kinds)).lstrip("\n"))
        return 0
    t0 = time.monotonic()
    seams = write_map(ROOT, args.out, args.since, args.all_kinds)
    print(f"seams: {len(seams)} seams, {sum(e['count'] for e in seams)} sites in {time.monotonic() - t0:.1f}s -> {args.out / 'seams.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
