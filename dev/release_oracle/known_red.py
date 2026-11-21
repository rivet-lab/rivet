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
import re
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class KnownRed:
    match: str
    reason: str
    expires: str  # YYYY-MM-DD
    alt: tuple[str, ...] = ()

    def keys(self) -> tuple[str, ...]:
        """Every whole failure message this one defect may print."""
        return (self.match, *self.alt)


_EXPIRES = "2026-11-22"
GATE, ISLAND, OFFLINE, NIGHTLY = "gate", "island", "offline", "nightly"
_ISLAND_TIER = re.compile(r"\] full: ")
_OFFLINE_TIER = re.compile(r"\[offline\]: ")
_NIGHTLY_KEY = re.compile(r"^nightly\[")
_GATE_CLASS = re.compile(r"^(?P<matrix>[a-z_]+)\[(?P<kind>[a-z_]+)\](?: [a-z]+)?: (?P<cause>[A-Z_]+) over ")
_CELL = re.compile(r"^(?P<cell>[A-Za-z_]\w*::\w[\w:]*) — ")
_DATED = re.compile(r"\b20\d\d-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b")
_ORACLE_CDC_LOAD = (
    "Oracle CDC does not load yet (ADR-0037; the Oracle GA work). The previous release's `rivet init --mode cdc` writes no `load:` block, and this cell still drives the previous release through `load`, which refuses. `upgrade[oracle/cdc-load/init=this]` grades what the scaffold does promise (the baseline Parquet equals the source). The cycle stays unsupported until Oracle CDC loads. Pinned 2026-11-01 to the whole refusal line; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it"
)


def nightly_key(matrix: str, kind: str, cause: str) -> str:
    """The class of a nightly red: the matrix, the engine or kind its cell names, and the first word of its cause."""
    return f"nightly[{matrix}|{kind}] {cause}"


def _open(cell: str, symptom: str, reason: str) -> KnownRed:
    """The entry of one `open_defect_*` live cell: its test path and the first line of its panic."""
    return KnownRed(f"{cell} — {symptom}", reason, _EXPIRES)



KNOWN_RED: tuple[KnownRed, ...] = (
    KnownRed("upgrade[oracle/cdc-load]: cycle1/load: prev exit 1 [RIVET_LOAD_BLOCK_MISSING]: Error: [RIVET_LOAD_BLOCK_MISSING] config has no top-level `load:` block — add `load: { target, ... }` to load into a warehouse",
             _ORACLE_CDC_LOAD,
             _EXPIRES),
    KnownRed("upgrade[oracle/cdc-load/tz=Asia/Tokyo]: cycle1/load: prev exit 1 [RIVET_LOAD_BLOCK_MISSING]: Error: [RIVET_LOAD_BLOCK_MISSING] config has no top-level `load:` block — add `load: { target, ... }` to load into a warehouse",
             _ORACLE_CDC_LOAD,
             _EXPIRES),
    KnownRed("no_silent_null[mssql] smoke: CHECK_RUN_SPLIT over 6 source->override pairs #520f3f12",
             "2026-11-19: batch leg, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null[mysql] smoke: CHECK_RUN_SPLIT over 9 source->override pairs #72410b51",
             "2026-11-19: batch leg, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null[oracle] smoke: CHECK_RUN_SPLIT over 15 source->override pairs #9fb19b01",
             "2026-11-19: batch leg, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null[postgres] smoke: CHECK_RUN_SPLIT over 4 source->override pairs #75e0db83",
             "2026-11-19: batch leg, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null[mssql] full: WRONG_VALUE over 5 source->override pairs #01b25485",
             "2026-11-11: full tier, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way, DATETIME to date truncated, Oracle TIMESTAMP WITH TIME ZONE shifted); open, closes when the override refuses a value it cannot hold; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 from the full tier on main 7dc3e850 over the pairs left: DATETIME2 to none, timestamp, timestamp_ns, timestamp_tz, timestamp_tz_ns",
             _EXPIRES),
    KnownRed("no_silent_null[mssql] full: CHECK_RUN_SPLIT over 6 source->override pairs #520f3f12",
             "2026-11-19: full tier, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null[mssql] full: INTEGRITY_BLIND over 5 source->override pairs #01b25485",
             "2026-11-11: full tier, PR-SELF-GRADE: validate --depth full exits 0 over a cell whose values changed, it answers from rivet own fingerprint; open, closes when validate compares against the source; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 from the full tier on main 7dc3e850 over the pairs left: DATETIME2 to none, timestamp, timestamp_ns, timestamp_tz, timestamp_tz_ns",
             _EXPIRES),
    KnownRed("no_silent_null[mysql] full: WRONG_VALUE over 2 source->override pairs #8e570de1",
             "2026-11-11: full tier, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way, DATETIME to date truncated, Oracle TIMESTAMP WITH TIME ZONE shifted); open, closes when the override refuses a value it cannot hold; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-13 from island reads on devbox, stable over 3 runs; re-keyed 2026-11-19 from the full tier on main 7dc3e850 over the pairs left: BIT(8) and BIT(64) to binary",
             _EXPIRES),
    KnownRed("no_silent_null[mysql] full: CHECK_RUN_SPLIT over 9 source->override pairs #72410b51",
             "2026-11-19: full tier, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null[mysql] full: INTEGRITY_BLIND over 2 source->override pairs #8e570de1",
             "2026-11-11: full tier, PR-SELF-GRADE: validate --depth full exits 0 over a cell whose values changed, it answers from rivet own fingerprint; open, closes when validate compares against the source; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-13 from island reads on devbox, stable over 3 runs; re-keyed 2026-11-19 from the full tier on main 7dc3e850 over the pairs left: BIT(8) and BIT(64) to binary",
             _EXPIRES),
    KnownRed("no_silent_null[oracle] full: WRONG_VALUE over 2 source->override pairs #9a439c62",
             "2026-11-11: full tier, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way, DATETIME to date truncated, Oracle TIMESTAMP WITH TIME ZONE shifted); open, closes when the override refuses a value it cannot hold; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 from the full tier on main 7dc3e850 over the pairs left: TIMESTAMP(9) to timestamp and timestamp_tz",
             _EXPIRES),
    KnownRed("no_silent_null[oracle] full: CHECK_RUN_SPLIT over 6 source->override pairs #96a3fe45",
             "2026-11-19: full tier, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the DATE and TIMESTAMP(9) to date pairs end in RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH (exit 1) and flap between check refusing (no red) and check exit 0, so the count may read 2 lower and the digest shift; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES,
             alt=("no_silent_null[oracle] full: CHECK_RUN_SPLIT over 4 source->override pairs #867c0680",)),
    KnownRed("no_silent_null[oracle] full: INTEGRITY_BLIND over 2 source->override pairs #9a439c62",
             "2026-11-11: full tier, PR-SELF-GRADE: validate --depth full exits 0 over a cell whose values changed, it answers from rivet own fingerprint; open, closes when validate compares against the source; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 from the full tier on main 7dc3e850 over the pairs left: TIMESTAMP(9) to timestamp and timestamp_tz",
             _EXPIRES),
    KnownRed("no_silent_null[postgres] full: CHECK_RUN_SPLIT over 4 source->override pairs #75e0db83",
             "2026-11-19: full tier, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mssql]: CHECK_RUN_SPLIT over 33 source->override pairs #d9724f13",
             "2026-11-19: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_CDC_PREREQUISITE on the added and type_changed legs, a prerequisite the stream reads from the server at run time, and RIVET_SOURCE_VALUE_UNREPRESENTABLE, exit 5, because check reads no values); the format, unmapped-column and declared-type refusals of the pending baseline leg are decided by check and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides the remaining refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mysql]: CHECK_RUN_SPLIT over 39 source->override pairs #58266b5d",
             "2026-11-19: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_CDC_PREREQUISITE on the MINIMAL and NOBLOB row-image legs, a prerequisite the stream reads from the server at run time, and RIVET_SOURCE_VALUE_UNREPRESENTABLE, exit 5, because check reads no values); the format, unmapped-column and declared-type refusals of the pending baseline leg are decided by check and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides the remaining refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[oracle]: CHECK_RUN_SPLIT over 15 source->override pairs #5b995454",
             "2026-11-19: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_VALUE_UNREPRESENTABLE, exit 5, because check reads no values, and RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH for a NUMBER column declared uuid on the snapshot leg, which check still passes); the format, unmapped-column and declared-type refusals of the pending baseline leg are decided by check and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides the remaining refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[postgres]: CHECK_RUN_SPLIT over 2 source->override pairs #da166c32",
             "2026-11-19: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_VALUE_UNREPRESENTABLE, exit 5, because check reads no values); the format, unmapped-column and declared-type refusals of the pending baseline leg are decided by check and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides the remaining refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("check_equals_run_cloud[offline]: CHECK_RUN_SPLIT over 1 cells #3aec5ed8",
             "2026-11-10: BigQuery load leg, PR-PRECHECK: check exits 0 and load refuses by code (RIVET_LOAD_WAREHOUSE_REJECTED, dataset not found) because check never contacts the warehouse; open, closes when check probes the target dataset or the matrix accepts the split; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-13: the gate grades the cloud tier and reads the same line there, the offline CLI test the offline one",
             _EXPIRES,
             alt=("check_equals_run_cloud[cloud]: CHECK_RUN_SPLIT over 1 cells #3aec5ed8",)),
    KnownRed("no_secret_in_output_cloud[offline]: SECRET_LEAK over 1 cells #6e8be2c9",
             "2026-11-10: BigQuery load leg, PR-CAUSE: a server that echoes the Authorization header in its 401 body gets it printed verbatim in stderr and --summary-output because the warehouse error text is not masked; open, closes when load masks server error text; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it",
             _EXPIRES),
    KnownRed("no_silent_null_load[mssql]: CHECK_RUN_SPLIT over 15 source->override pairs #b27aa94f",
             "2026-11-10: ClickHouse load leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, RIVET_LOAD_WAREHOUSE_REJECTED, RIVET_SOURCE_QUERY_FAILED, RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_TYPE_UNSAFE_MAPPING) because check reads neither the values nor the format; open, closes when check refuses the same pair; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-measured 2026-11-17: the ledger-driven matrix restored the check-vs-run comparison and re-keyed the pairs to the ledger rows",
             _EXPIRES),
    KnownRed("no_silent_null_load[mysql]: CHECK_RUN_SPLIT over 16 source->override pairs #fade8bc5",
             "2026-11-10: ClickHouse load leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, RIVET_LOAD_WAREHOUSE_REJECTED, RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_TYPE_UNSAFE_MAPPING) because check reads neither the values nor the format; open, closes when check refuses the same pair; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-measured 2026-11-17: the ledger-driven matrix restored the check-vs-run comparison and re-keyed the pairs to the ledger rows",
             _EXPIRES),
    KnownRed("no_silent_null_load[oracle]: CHECK_RUN_SPLIT over 8 source->override pairs #e69fe066",
             "2026-11-10: ClickHouse load leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE) because check reads neither the values nor the format; open, closes when check refuses the same pair; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-measured 2026-11-17: the ledger-driven matrix restored the check-vs-run comparison and re-keyed the pairs to the ledger rows",
             _EXPIRES),
    KnownRed("no_silent_null_load[postgres]: CHECK_RUN_SPLIT over 57 source->override pairs #2ce7b4f8",
             "2026-11-10: ClickHouse load leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, RIVET_LOAD_WAREHOUSE_REJECTED, RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_TYPE_UNSAFE_MAPPING) because check reads neither the values nor the format; open, closes when check refuses the same pair; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-measured 2026-11-17: the ledger-driven matrix restored the check-vs-run comparison and re-keyed the pairs to the ledger rows",
             _EXPIRES),
    KnownRed("no_silent_null_load[oracle]: DEFECT_ORACLE_NUMBER_INF_FINITE over 4 source->override pairs #7dbfcb1b",
             "2026-11-16: type matrix, class ORACLE_NUMBER_INF_FINITE: an Oracle NUMBER or FLOAT of plus or minus infinity is delivered as a finite value; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_ORACLE_NUMBER_INF_FINITE when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[oracle]: DEFECT_ORACLE_INTERVAL_DS_NS over 4 source->override pairs #a8f0c052",
             "2026-11-16: type matrix, class ORACLE_INTERVAL_DS_NS: an INTERVAL DAY TO SECOND(9) loses its nanoseconds; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_ORACLE_INTERVAL_DS_NS when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[postgres]: DEFECT_PG_NULL_LIST_EMPTY_CH over 15 source->override pairs #9d72e5fb",
             "2026-11-16: type matrix, class PG_NULL_LIST_EMPTY_CH: a NULL list is delivered as an empty list at ClickHouse; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_PG_NULL_LIST_EMPTY_CH when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[postgres]: DEFECT_PG_UNMAPPED_TYPE over 166 source->override pairs #ecde8ca6",
             "2026-11-16: type matrix, class PG_UNMAPPED_TYPE: a PostgreSQL type with no Arrow mapping is refused with RIVET_TYPE_UNSAFE_MAPPING; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_PG_UNMAPPED_TYPE when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[postgres]: DEFECT_PG_ARRAY_ONE_DIM_MULTIDIM_MSG over 28 source->override pairs #c44d213b",
             "2026-11-16: type matrix, class PG_ARRAY_ONE_DIM_MULTIDIM_MSG: a 1-D array of enum, json, jsonb, interval or oid is refused with the multi-dimensional message; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_PG_ARRAY_ONE_DIM_MULTIDIM_MSG when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load_cloud[postgres]: DEFECT_PG_NULL_LIST_EMPTY_BQ over 15 source->override pairs #6c47fd86",
             "2026-11-16: type matrix, class PG_NULL_LIST_EMPTY_BQ: a NULL list is delivered as an empty list at BigQuery; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_PG_NULL_LIST_EMPTY_BQ when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load_cloud[postgres]: DEFECT_PG_UNMAPPED_TYPE over 249 source->override pairs #08e7d92e",
             "2026-11-16: type matrix, class PG_UNMAPPED_TYPE: a PostgreSQL type with no Arrow mapping is refused with RIVET_TYPE_UNSAFE_MAPPING; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_PG_UNMAPPED_TYPE when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load_cloud[postgres]: DEFECT_PG_ARRAY_ONE_DIM_MULTIDIM_MSG over 42 source->override pairs #59bf8ecc",
             "2026-11-16: type matrix, class PG_ARRAY_ONE_DIM_MULTIDIM_MSG: a 1-D array of enum, json, jsonb, interval or oid is refused with the multi-dimensional message; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_PG_ARRAY_ONE_DIM_MULTIDIM_MSG when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load_cloud[oracle]: DEFECT_ORACLE_NUMBER_INF_FINITE over 6 source->override pairs #8ad6a116",
             "2026-11-16: type matrix, class ORACLE_NUMBER_INF_FINITE: an Oracle NUMBER or FLOAT of plus or minus infinity is delivered as a finite value; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_ORACLE_NUMBER_INF_FINITE when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load_cloud[oracle]: DEFECT_ORACLE_INTERVAL_DS_NS over 6 source->override pairs #499faafb",
             "2026-11-16: type matrix, class ORACLE_INTERVAL_DS_NS: an INTERVAL DAY TO SECOND(9) loses its nanoseconds; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_ORACLE_INTERVAL_DS_NS when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load_cloud[mongo]: DEFECT_SF_DOUBLE_MAX_INF over 2 source->override pairs #5b3215ab",
             "2026-11-16: type matrix, class SF_DOUBLE_MAX_INF: Snowflake reads the largest finite double back as an infinity; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_SF_DOUBLE_MAX_INF when it changes",
             _EXPIRES),
    KnownRed(nightly_key("refusal_names_true_cause", "cause", "uncoded"),
             "2026-11-14: #917, the nightly refusal-names-true-cause part: refusals stay uncoded, name the wrong cause or a remedy flag that does not exist, or exit 0 on a refused config; open, closes with the fix of #917",
             _EXPIRES,
             alt=tuple(nightly_key("refusal_names_true_cause", kind, cause) for kind, cause in (
                 ("cmd", "printed"), ("cmd", "uncoded"), ("decoy", "uncoded"), ("field", "uncoded"), ("guard", "printed"), ("reader", "the"),
                 ("remedy", "remedy"), ("site", "remedy"), ("source", "named"), ("word", "printed"), ("wording", "remedy")))),
)


def cells(entries: tuple[KnownRed, ...] | None = None) -> dict[str, KnownRed]:
    """The live-cell entries of the ledger (or of `entries`) by cell (`module::test`)."""
    return {c: k for k in (KNOWN_RED if entries is None else entries) if (c := cell_of(k.match))}


def nightly_classes(entries: tuple[KnownRed, ...] | None = None) -> dict[str, KnownRed]:
    """Every red class the ledger (or `entries`) acknowledges: its live cells by name, its own `nightly[...]` entries and the per-cell-set entries of the gate, cut to (matrix, engine, cause)."""
    entries = KNOWN_RED if entries is None else entries
    out: dict[str, KnownRed] = dict(cells(entries))
    for k in entries:
        for key in k.keys():
            if _NIGHTLY_KEY.match(key):
                out[key] = k
            elif m := _GATE_CLASS.match(key):
                out[nightly_key(m["matrix"], m["kind"], m["cause"])] = k
    return out


def lane_of(key: str) -> str:
    """The run that grades a failure message: the nightly workflow for a `nightly[...]` class, the weekly island for the full tier, the offline CLI test for the offline tier, else the per-run gate."""
    if _NIGHTLY_KEY.match(key):
        return NIGHTLY
    if _ISLAND_TIER.search(key):
        return ISLAND
    return OFFLINE if _OFFLINE_TIER.search(key) else GATE


def owed_by(k: KnownRed, lane: str) -> bool:
    """Whether a full run of `lane` must see this entry fire: one of its messages is graded by that lane."""
    return any(lane_of(key) == lane for key in k.keys())


def unframed(msg: str) -> str:
    """The failure text of a live-cell message `<label> FAILED · <cell> — <why> (see <log>)`, else `msg`."""
    head, sep, rest = msg.partition(" FAILED · ")
    return rest.rpartition(" (see ")[0] or rest if sep else msg


def cell_of(line: str) -> str | None:
    """The `module::test` of a live-cell failure line or ledger message (`<label> FAILED · <cell> — <why>`), else None; the head of `<why>` is never read."""
    m = _CELL.match(unframed(line).strip())
    return m["cell"] if m else None


def cell_key(msg: str) -> str:
    """The identity of a red message across runs and slices: its live cell, else the message cut before ` over ` (its count and digest change per run)."""
    return cell_of(msg) or msg.partition(" over ")[0]


def match(msg: str, today: _dt.date | None = None) -> tuple[KnownRed | None, bool]:
    """The entry `msg` matches and whether it is still in date."""
    today = today or _dt.date.today()
    for k in KNOWN_RED:
        if msg in k.keys() or unframed(msg) in k.keys():
            return k, today <= _dt.date.fromisoformat(k.expires)
    return None, False


def problems(entries: tuple[KnownRed, ...] | None = None) -> list[str]:
    """Entries with an empty match, a reason that carries no ISO date, or an expiry that is not a date."""
    out = []
    for k in KNOWN_RED if entries is None else entries:
        name = k.match[:80] or "<empty match>"
        if not k.match.strip():
            out.append(f"{name}: empty match")
        if not _DATED.search(k.reason):
            out.append(f"{name}: the reason carries no date (YYYY-MM-DD)")
        try:
            _dt.date.fromisoformat(k.expires)
        except ValueError:
            out.append(f"{name}: expiry {k.expires!r} is not a date")
    return out


def self_test() -> int:
    """Each rule of [`problems`] goes RED on the entry it exists for and stays green on a dated one."""
    ok = KnownRed("m::open_defect_x — boom", "X-1 (2026-11-04): why; closes with fix/x", "2026-11-15")
    assert problems((ok,)) == [], problems((ok,))
    assert problems((KnownRed("m", "why; closes with fix/x", "2026-11-15"),)), "an undated reason must fail"
    assert problems((KnownRed("m", "opened 2026-13-40", "2026-11-15"),)), "a date that is no date must fail"
    assert problems((KnownRed(" ", "2026-11-04 why", "2026-11-15"),)), "an empty match must fail"
    assert problems((KnownRed("m", "2026-11-04 why", "soon"),)), "an expiry that is no date must fail"
    assert problems() == [], "the ledger itself must be clean"
    own = nightly_key("refusal_names_true_cause", "cmd", "printed")
    assert lane_of(own) == NIGHTLY and not any(owed_by(k, lane) for k in KNOWN_RED if own in k.keys() for lane in (GATE, ISLAND, OFFLINE)), \
        "a nightly class is owed by no gate run: a full gate would block on it"
    cut = nightly_classes()
    assert nightly_key("no_silent_null", "mysql", "CHECK_RUN_SPLIT") in cut and nightly_key("no_silent_null", "mssql", "WRONG_VALUE") in cut, \
        "a gate entry is cut to its (matrix, engine, cause) class"
    from .property_no_silent_null import Result, failure_message
    rs = [Result("mysql/a->b/full/parquet", "x")]
    for scenario, tier in (("no_silent_null", "smoke"), ("no_silent_null", "full"), ("no_silent_null_load", ""), ("no_silent_null_cdc", ""), ("no_silent_null_load_cloud", "")):
        msg = failure_message("mysql", "CHECK_RUN_SPLIT", rs, scenario, tier)
        assert nightly_key(scenario, "mysql", "CHECK_RUN_SPLIT") in nightly_classes((KnownRed(msg, "2026-11-19: x", _EXPIRES),)), f"a producer line is cut to its class: {msg}"
    assert all(nightly_classes((k,)) for k in KNOWN_RED if " over " in k.match), "every per-cell-set entry of the ledger is cut to a class"
    held = KnownRed("m::open_defect_x — boom", "2026-11-16: why", "2026-11-22")
    assert cells((held,)) == {"m::open_defect_x": held} == nightly_classes((held,)), "a live-cell entry is held under its cell"
    framed = "audit suspects FAILED · m::t — [2026-11-16T00:13:32Z WARN rivet::config::source] inline `url:` plaintext password (see log)"
    assert cell_of(framed) == "m::t" == cell_of("m::t — boom") == cell_key(framed), "the cell is read before the head, whatever warning heads it"
    assert cell_of("no_silent_null[mysql] smoke: SILENT_NULL over 3 pairs #ab") is None and cell_of("x FAILED · no cell here") is None
    assert cell_key("no_silent_null[mysql] smoke: SILENT_NULL over 3 pairs #ab") == cell_key("no_silent_null[mysql] smoke: SILENT_NULL over 9 pairs #cd"), \
        "a gate class keeps its key when its count and digest change"
    print("known_red self-test: ok")
    return 0


def main() -> int:
    """Print the entry count when every entry is well formed, else the problems and exit 1."""
    if sys.argv[1:] == ["--self-test"]:
        return self_test()
    bad = problems()
    for b in bad:
        print(f"known_red: {b}", file=sys.stderr)
    if not bad:
        print(f"known_red: {len(KNOWN_RED)} entries, every reason dated")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
