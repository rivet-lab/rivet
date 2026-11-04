"""Known reds: gate failures acknowledged with a reason and an expiry, so a red gate on main
is red ONLY for recorded reasons. A matching failure is reported as KNOWN, not blocking; an
expired entry blocks again; an entry that no failure matched in a FULL run blocks too (fixed —
remove it). Matched by the whole failure message, one entry per acknowledged defect.

An open product defect is pinned by a live_suite cell that asserts the CORRECT behaviour and so
fails until the fix lands: the cell is named `open_defect_*` and marked `live+gate-only` (CI has no
such ledger and would only go red), and its entry here is `<module>::<fn> — <its first panic line>`,
the defect's own symptom. The fix PR deletes the entry; the cell stays as the regression guard.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass


@dataclass(frozen=True)
class KnownRed:
    match: str
    reason: str
    expires: str  # YYYY-MM-DD


_EXPIRES = "2026-11-15"
_ORACLE_CDC_LOAD = (
    "Oracle CDC does not load yet (ADR-0037; the Oracle GA work). The previous release's `rivet init --mode cdc` writes no `load:` block, and this cell still drives the previous release through `load`, which refuses. `upgrade[oracle/cdc-load/init=this]` grades what the scaffold does promise (the baseline Parquet equals the source). The cycle stays unsupported until Oracle CDC loads. Pinned 2026-11-01 to the whole refusal line"
)


_TS9 = (
    "Oracle TIMESTAMP(9) is delivered at microseconds today: docs/type-capability-matrix.yaml's oracle TIMESTAMP(9) row is a known_defect "
    "(exact native is Timestamp(ns), ADR-0038 CP1, Oracle engine step), so the 1 ns sentinel lands truncated. "
    "Renewed 2026-11-01: still live (target/triage/sent_oracle_alone.log); closes when the Oracle engine step lands Timestamp(ns). "
    "Pinned 2026-11-01 to the whole symptom: a NULL, epoch or other wrong value no longer matches"
)


def _open(cell: str, symptom: str, reason: str) -> KnownRed:
    """The entry of one `open_defect_*` live cell: its test path and the first line of its panic."""
    return KnownRed(f"{cell} — {symptom}", reason, _EXPIRES)

_HDW1_CONTRACT = (
    "HDW-1 (seam run-closing, 2026-11-03): a CDC run begins its run-status row warn-only, so over a state that refuses the row it reads its stream and writes anyway; closes with fix/run-closing-cdc-begin, which opens the row through ledger_begin_run and refuses by code"
)
_HDW1_LOAD = (
    "HDW-1 (seam run-still-writing, 2026-11-03): a load from a state that holds no ledger row for a CDC run in flight counts the run finished, loads the parts of one roll and records the run consumed, so the rolls that follow are never loaded; closes with fix/run-closing-cdc-begin, where still_writing owns the CDC no-row rule"
)

_CDC_START = (
    "CDC-RUN-START (seam run-still-writing, 2026-11-03): a CDC run writes no running marker and no manifest before its first roll, so a load from another state DB with gc_orphans removes the part the run uploaded and has not manifested yet; closes with fix/cdc-run-start-marker, which writes a running manifest under the run's own key at start"
)

_KEYSET_KEY_CURSOR = (
    "ER-3/ER-6 (seam keyset-key-type, 2026-11-03): the probes admit every unique NOT NULL key but decimal, while the cursor reader (extract_last_cursor_value) has no Binary, Boolean or Time64 arm and a MySQL ENUM orders by member index but seeks by string; the run writes page 0 and stops with exit 5 RIVET_SOURCE_PROBE_UNREADABLE (or skips rows); closes with fix/seam-keyset-key-type, which refuses the key at plan with RIVET_PLAN_CHUNK_KEY_UNUSABLE"
)
_KEYSET_KEY_SUB_US = (
    "ER-1 (seam keyset-key-type, 2026-11-03): an MSSQL datetime2(7)/datetimeoffset(7) chunk_by_key is admitted although the default batch holds microseconds, so the seek re-reads the row at the end of every page (exit 0 with duplicates) or cannot advance (exit 5); closes with fix/seam-keyset-key-type, which refuses the key at plan unless a timestamp_ns override is set"
)
_KEYSET_KEY_ONE_CHUNK = (
    "RHS-3 (seam keyset-key-type, owner decision Q11 2026-11-03): a table that fits one chunk, under mode: chunked with a key the cursor cannot advance, runs with exit 0 (an auto-picked MySQL key downgrades to a snapshot, an explicit chunk_by_key reads one short page) while the same config on a large table fails or duplicates; closes with fix/seam-keyset-key-type, which refuses it with exit 1 RIVET_PLAN_CHUNK_KEY_UNUSABLE"
)


KNOWN_RED: tuple[KnownRed, ...] = (
<<<<<<< HEAD
    _open("live_destination_credential::open_defect_an_s3_profile_beside_env_keys_is_refused",
          "an S3 destination naming aws_profile beside env keys is refused by code, run",
          "HFF-2 (consolidated-2026-11-03): an S3 destination naming aws_profile beside access_key_env and secret_key_env signs with the profile alone, because opendal's customized credential loader ignores the static keys, so the operator's keys are never used and nothing says so; opened 2026-11-04; closes with fix/destination-credential-every-leg, which names the conflict at config load as RIVET_CONFIG_DESTINATION_AUTH_INVALID"),
=======
    _open("live_stop_class::open_defect_a_missing_relation_exits_by_its_code_whatever_its_name_holds_mssql",
          "refusal contract: the invocation ended with [RIVET_SOURCE_TABLE_NOT_FOUND] (exit 2), expected [RIVET_SOURCE_TABLE_NOT_FOUND] (exit 1)",
          "VP-8 (seam stop-class, 2026-11-04): a missing table refused by a coded Environment stop exits by the words of the name the operator chose, because stop_class returns None for the kind and classify_exit reads the message text, which holds the name; a name with a transient word (login_timeouts, dns_zones, deadlock_log) exits 2 where PG and MySQL exit 1 for the same refusal. Opened 2026-11-04; closes with fix/stop-class-by-code"),
    _open("live_stop_class::open_defect_a_missing_relation_exits_by_its_code_whatever_its_name_holds_oracle",
          "refusal contract: the invocation ended with [RIVET_SOURCE_TABLE_NOT_FOUND] (exit 2), expected [RIVET_SOURCE_TABLE_NOT_FOUND] (exit 1)",
          "VP-8 (seam stop-class, 2026-11-04): a missing table refused by a coded Environment stop exits by the words of the name the operator chose, because stop_class returns None for the kind and classify_exit reads the message text, which holds the name; a name with a transient word (login_timeouts, dns_zones, deadlock_log) exits 2 where PG and MySQL exit 1 for the same refusal. Opened 2026-11-04; closes with fix/stop-class-by-code"),
    _open("live_stop_class::open_defect_a_missing_relation_exits_by_its_code_whatever_its_name_holds_mongo",
          "refusal contract: the invocation ended with [RIVET_SOURCE_COLLECTION_NOT_FOUND] (exit 2), expected [RIVET_SOURCE_COLLECTION_NOT_FOUND] (exit 1)",
          "VP-8 (seam stop-class, 2026-11-04): a missing collection refused by a coded Environment stop exits by the words of the name the operator chose, because stop_class returns None for the kind and classify_exit reads the message text, which holds the name; a name with a transient word (login_timeouts, dns_zones, deadlock_log) exits 2 where PG and MySQL exit 1 for the same refusal. Opened 2026-11-04; closes with fix/stop-class-by-code"),
    _open("live_stop_class::open_defect_an_mssql_read_blocked_past_its_lock_timeout_exits_retryable",
          "assertion `left == right` failed: a lock wait that timed out clears when the lock does: retryable",
          "MT-F-F (seam stop-class, 2026-11-04): MSSQL error 1222 (lock request time out) reaches the stop as a stringified SOURCE_QUERY_FAILED whose text the retry classifier does not know, so a read that timed out on a held lock exits 1 where the same wait on MySQL (1205) exits 2. Opened 2026-11-04; closes with fix/stop-class-by-code"),
>>>>>>> origin/main
    _open("live_cdc_mbt::open_defect_mysql_cdc_negative_time_fraction_matches_the_snapshot",
          "assertion `left == right` failed: stream differs from the source on negative TIME(1)/TIME(2) with a fraction",
          "TF-1 (consolidated-2026-11-03 section 2): mysql_common 0.37.3 binlog/misc.rs:35-39 subtracts 0x100 from a u32 fraction for a negative TIME(1)/TIME(2) with a fraction, so a debug rivet panics and a release rivet writes 625:04:19.277216 for -00:00:00.50 in a string column; a row of any table in the binlog panics every MySQL CDC run that reads it. Opened 2026-11-04; closes with fix/mysql-binlog-time-fraction"),
    _open("live_cdc_mbt::open_defect_mysql_cdc_enum_set_labels_match_the_snapshot",
          'rig oracle: VALUES: 6 source row(s) not delivered, 6 delivered row(s) not in the source; differing column(s) [\'e\', \'s\'] of [\'id\', \'e\', \'s\']; source-only [\'[["num", "1"], "C:\\\\\\\\temp", "p\\\\\\\\q,r\\\\ns"]\', \'[["num", "2"], "a\\\\nb", "r\\\\ns,t\\\\ru"]\', \'[["num", "3"], "c\\\\rd", "t\\\\ru,v\\\\u0000w"]\', \'[["num", ',
          "TF-2 (consolidated-2026-11-03 section 2): parse_enum_labels (src/source/cdc/value.rs) reverses only the doubled quote of MySQL's append_unescaped, so a label holding a backslash, LF, CR or NUL reaches the stream with the catalog's escaping while the snapshot holds the stored label. Opened 2026-11-03; closes with fix/mysql-enum-label-escapes"),
    KnownRed("upgrade[oracle/cdc-load]: cycle1/load: prev exit 1 [RIVET_LOAD_BLOCK_MISSING]: Error: [RIVET_LOAD_BLOCK_MISSING] config has no top-level `load:` block — add `load: { target, ... }` to load into a warehouse",
             _ORACLE_CDC_LOAD,
             _EXPIRES),
    KnownRed("upgrade[oracle/cdc-load/tz=Asia/Tokyo]: cycle1/load: prev exit 1 [RIVET_LOAD_BLOCK_MISSING]: Error: [RIVET_LOAD_BLOCK_MISSING] config has no top-level `load:` block — add `load: { target, ... }` to load into a warehouse",
             _ORACLE_CDC_LOAD,
             _EXPIRES),
    KnownRed("sentinels[rivet_sent_ts9/full]: SUCCEEDED WITH A CHANGED VALUE: `V` differs at ID=('num', '1'): source ' 2024-01-01 00:00:00.000000001' vs warehouse datetime.datetime(2024, 1, 1, 0, 0)",
             _TS9,
             _EXPIRES),
    KnownRed("sentinels[rivet_sent_ts9/keyset]: SUCCEEDED WITH A CHANGED VALUE: `V` differs at ID=('num', '1'): source ' 2024-01-01 00:00:00.000000001' vs warehouse datetime.datetime(2024, 1, 1, 0, 0)",
             _TS9,
             _EXPIRES),
    _open("live_operator_contract::open_defect_a_cdc_run_over_a_state_that_refuses_its_ledger_row_is_refused_postgres",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_STATE_NOT_WRITABLE] (exit 1)",
          _HDW1_CONTRACT),
    _open("live_operator_contract::open_defect_a_cdc_run_over_a_state_that_refuses_its_ledger_row_is_refused_mysql",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_STATE_NOT_WRITABLE] (exit 1)",
          _HDW1_CONTRACT),
    _open("live_operator_contract::open_defect_a_cdc_run_over_a_state_that_refuses_its_ledger_row_is_refused_mssql",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_STATE_NOT_WRITABLE] (exit 1)",
          _HDW1_CONTRACT),
    _open("live_operator_contract::open_defect_a_cdc_run_over_a_state_that_refuses_its_ledger_row_is_refused_mongo",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_STATE_NOT_WRITABLE] (exit 1)",
          _HDW1_CONTRACT),
    _open("live_operator_contract::open_defect_a_cdc_run_over_a_state_that_refuses_its_ledger_row_is_refused_oracle",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_STATE_NOT_WRITABLE] (exit 1)",
          _HDW1_CONTRACT),
    _open("live_clickhouse_load::open_defect_a_load_from_another_state_db_does_not_strand_a_rolling_cdc_run_mysql",
          "assertion `left == right` failed: the view's live rows must equal the source",
          _HDW1_LOAD),
    _open("live_clickhouse_load::open_defect_a_gc_orphans_load_from_another_state_db_spares_a_cdc_runs_first_part_mysql",
          "the load beside the rolling run:",
          _CDC_START),
    _open("live_clickhouse_load::open_defect_a_gc_orphans_load_from_another_state_db_spares_a_cdc_runs_first_part_postgres",
          "the load beside the rolling run:",
          _CDC_START),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_mysql_binary16_primary_key",
          "keyset key MySQL BINARY(16) primary key over several pages: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_CURSOR),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_mysql_time_unique_key",
          "keyset key MySQL TIME unique key over several pages: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_CURSOR),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_mysql_tinyint1_unique_key",
          "keyset key MySQL TINYINT(1) unique key over several pages: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_CURSOR),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_mysql_enum_unique_key",
          "keyset key MySQL ENUM unique key over several pages: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_CURSOR),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_postgres_bytea_unique_key",
          "keyset key PostgreSQL bytea unique key over several pages: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_CURSOR),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_postgres_time_unique_key",
          "keyset key PostgreSQL time unique key over several pages: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_CURSOR),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_mssql_varbinary_unique_key",
          "keyset key SQL Server varbinary unique key over several pages: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_CURSOR),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_mssql_time_unique_key",
          "keyset key SQL Server time unique key over several pages: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_CURSOR),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_mssql_datetime2_7_distinct_microseconds",
          "keyset key SQL Server datetime2(7) key, a tick past each microsecond over several pages: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_SUB_US),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_mssql_datetime2_7_inside_one_microsecond",
          "keyset key SQL Server datetime2(7) key, nine ticks inside one microsecond over several pages: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_SUB_US),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_mssql_datetimeoffset_7_distinct_microseconds",
          "keyset key SQL Server datetimeoffset(7) key, a tick past each microsecond over several pages: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_SUB_US),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_on_a_one_chunk_table_mysql_binary16_primary_key",
          "keyset key MySQL BINARY(16) primary key on a table that fits one chunk: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_ONE_CHUNK),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_on_a_one_chunk_table_postgres_bytea_unique_key",
          "keyset key PostgreSQL bytea unique key on a table that fits one chunk: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_ONE_CHUNK),
    _open("live_keyset_key_type::open_defect_keyset_key_is_refused_on_a_one_chunk_table_mssql_datetime2_7_key",
          "keyset key SQL Server datetime2(7) key, a tick past each microsecond on a table that fits one chunk: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command",
          _KEYSET_KEY_ONE_CHUNK),
)


def unframed(msg: str) -> str:
    """The failure text of a live-cell message `<label> FAILED · <cell> — <why> (see <log>)`, else `msg`."""
    head, sep, rest = msg.partition(" FAILED · ")
    return rest.rpartition(" (see ")[0] or rest if sep else msg


def match(msg: str, today: _dt.date | None = None) -> tuple[KnownRed | None, bool]:
    """The entry `msg` matches and whether it is still in date."""
    today = today or _dt.date.today()
    for k in KNOWN_RED:
        if k.match in (msg, unframed(msg)):
            return k, today <= _dt.date.fromisoformat(k.expires)
    return None, False
