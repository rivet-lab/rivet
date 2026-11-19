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
_GATE_CLASS = re.compile(r"^(?P<matrix>[a-z_]+)\[(?P<kind>[a-z_]+)\] (?:smoke|full): (?P<cause>[A-Z_]+) over ")
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
    KnownRed("no_silent_null[mssql] full: WRONG_VALUE over 3 source->override pairs #5e462b16",
             "2026-11-11: full tier, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way, DATETIME to date truncated, Oracle TIMESTAMP WITH TIME ZONE shifted); open, closes when the override refuses a value it cannot hold; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it",
             _EXPIRES),
    KnownRed("no_silent_null[mssql] full: CHECK_RUN_SPLIT over 6 source->override pairs #520f3f12",
             "2026-11-19: full tier, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null[mssql] full: INTEGRITY_BLIND over 1 source->override pairs #8f3cccc4",
             "2026-11-11: full tier, PR-SELF-GRADE: validate --depth full exits 0 over a cell whose values changed, it answers from rivet own fingerprint; open, closes when validate compares against the source; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it",
             _EXPIRES),
    KnownRed("no_silent_null[mysql] full: SILENT_NULL over 146 source->override pairs #3fb9d013",
             "2026-11-11: full tier, PR-NULL: a non-NULL value is delivered as NULL at exit 0 (MySQL text, binary, bit or decimal under a date, timestamp, int or float override, the `_ => append_null()` arms of src/source/mysql/arrow_convert.rs); open, closes when the pair is refused by code; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-13 from island reads on devbox, stable over 3 runs",
             _EXPIRES),
    KnownRed("no_silent_null[mysql] full: WRONG_VALUE over 34 source->override pairs #9ea89264",
             "2026-11-11: full tier, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way, DATETIME to date truncated, Oracle TIMESTAMP WITH TIME ZONE shifted); open, closes when the override refuses a value it cannot hold; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-13 from island reads on devbox, stable over 3 runs",
             _EXPIRES),
    KnownRed("no_silent_null[mysql] full: CHECK_RUN_SPLIT over 9 source->override pairs #72410b51",
             "2026-11-19: full tier, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null[mysql] full: INTEGRITY_BLIND over 176 source->override pairs #8b2d238c",
             "2026-11-11: full tier, PR-SELF-GRADE: validate --depth full exits 0 over a cell whose values changed, it answers from rivet own fingerprint; open, closes when validate compares against the source; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-13 from island reads on devbox, stable over 3 runs",
             _EXPIRES),
    KnownRed("no_silent_null[oracle] full: WRONG_VALUE over 12 source->override pairs #9194cbc1",
             "2026-11-11: full tier, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way, DATETIME to date truncated, Oracle TIMESTAMP WITH TIME ZONE shifted); open, closes when the override refuses a value it cannot hold; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it",
             _EXPIRES),
    KnownRed("no_silent_null[oracle] full: CHECK_RUN_SPLIT over 6 source->override pairs #96a3fe45",
             "2026-11-19: full tier, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the DATE and TIMESTAMP(9) to date pairs end in RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH (exit 1) and flap between check refusing (no red) and check exit 0, so the count may read 2 lower and the digest shift; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES,
             alt=("no_silent_null[oracle] full: CHECK_RUN_SPLIT over 4 source->override pairs #867c0680",)),
    KnownRed("no_silent_null[oracle] full: UNCODED_STOP over 9 source->override pairs #2e8a05bb",
             "2026-11-11: full tier, PR-CAUSE: run stops with exit 1 and no code on `invalid digit found in string`, `number too large to fit in target type` or `number too small to fit in target type` (Oracle NUMBER parsed under an override); open, closes when the pair is refused by a coded cause at check; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it",
             _EXPIRES),
    KnownRed("no_silent_null[oracle] full: INTEGRITY_BLIND over 9 source->override pairs #9fbb1219",
             "2026-11-11: full tier, PR-SELF-GRADE: validate --depth full exits 0 over a cell whose values changed, it answers from rivet own fingerprint; open, closes when validate compares against the source; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it",
             _EXPIRES),
    KnownRed("no_silent_null[postgres] full: WRONG_VALUE over 2 source->override pairs #04533e41",
             "2026-11-11: full tier, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way, DATETIME to date truncated, Oracle TIMESTAMP WITH TIME ZONE shifted); open, closes when the override refuses a value it cannot hold; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it",
             _EXPIRES),
    KnownRed("no_silent_null[postgres] full: CHECK_RUN_SPLIT over 4 source->override pairs #75e0db83",
             "2026-11-19: full tier, PR-PRECHECK: check exits 0 and run refuses with RIVET_SOURCE_VALUE_UNREPRESENTABLE (exit 5) because check reads no values; the format and unmapped-column refusals are decided by check, plan and doctor since 2026-11-19 and no pair of them remains; open, closes when check decides a value-dependent refusal or the owner accepts it; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-19 to the pairs left on the branch build",
             _EXPIRES),
    KnownRed("no_silent_null[postgres] full: UNCODED_STOP over 35 source->override pairs #a0cca18b",
             "2026-11-11: full tier, PR-CAUSE: run stops with exit 1 and no code on `error deserializing column N: cannot convert between the Rust type Option<PgUuidBytes> and ...` (BIGINT or BOOLEAN under a uuid override, 280 cells); open, closes when the pair is refused by a coded cause at check; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mssql]: CHECK_RUN_SPLIT over 135 source->override pairs #3a328fff",
             "2026-11-10: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH, RIVET_SOURCE_CDC_PREREQUISITE) because check reads neither the values nor the format; open, closes when check refuses the same pair; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-18: the previous key predates the typed override table, the pairs read on a release build of the fixed tree are a subset of the old pairs, the other classes of the old key no longer occur",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mysql]: CHECK_RUN_SPLIT over 129 source->override pairs #3358b746",
             "2026-11-10: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH, RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_SOURCE_CDC_PREREQUISITE) because check reads neither the values nor the format; open, closes when check refuses the same pair; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-18: the previous key predates the typed override table, the pairs read on a release build of the fixed tree are a subset of the old pairs, the other classes of the old key no longer occur",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[oracle]: CHECK_RUN_SPLIT over 74 source->override pairs #dca35c3f",
             "2026-11-10: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH) because check reads neither the values nor the format; open, closes when check refuses the same pair; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-18: the previous key predates the typed override table, the pairs read on a release build of the fixed tree are a subset of the old pairs, the other classes of the old key no longer occur",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[postgres]: CHECK_RUN_SPLIT over 135 source->override pairs #2ffdcb25",
             "2026-11-10: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH, RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_TYPE_UNSAFE_MAPPING, RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED) because check reads neither the values nor the format; open, closes when check refuses the same pair; accepted 2026-11-12: 0.33 ships it as a known issue, 0.34 closes it; re-keyed 2026-11-18: the previous key predates the typed override table, the pairs read on a release build of the fixed tree are a subset of the old pairs, the other classes of the old key no longer occur",
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
    KnownRed("no_silent_null_load[mssql]: DEFECT_MSSQL_SCALE_SEVEN_TRUNC over 23 source->override pairs #f6cd5c61",
             "2026-11-16: type matrix, class MSSQL_SCALE_SEVEN_TRUNC: a scale-7 time value loses its 100 ns digit to microseconds; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_MSSQL_SCALE_SEVEN_TRUNC when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[mssql]: DEFECT_CH_DATE_CLAMP over 1 source->override pairs #c493c49f",
             "2026-11-16: type matrix, class CH_DATE_CLAMP: ClickHouse clamps the DATE 2300-01-01 to 2299-12-31; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_CH_DATE_CLAMP when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[mssql]: DEFECT_MSSQL_MONEY_BIG over 10 source->override pairs #d44aa9fa",
             "2026-11-16: type matrix, class MSSQL_MONEY_BIG: a money value of 9e11 or more is refused as unrepresentable; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_MSSQL_MONEY_BIG when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[mysql]: DEFECT_MYSQL_TINYINT_BOOL over 14 source->override pairs #d8fb7660",
             "2026-11-16: type matrix, class MYSQL_TINYINT_BOOL: a TINYINT(1) holding a value outside 0 and 1 is delivered as boolean true; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_MYSQL_TINYINT_BOOL when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[mysql]: DEFECT_MYSQL_BIT_TOPBIT_REFUSED over 4 source->override pairs #47598a94",
             "2026-11-16: type matrix, class MYSQL_BIT_TOPBIT_REFUSED: a BIT(64) with the top bit set is refused as unrepresentable; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_MYSQL_BIT_TOPBIT_REFUSED when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[mysql]: DEFECT_CH_DATE_CLAMP over 1 source->override pairs #c493c49f",
             "2026-11-16: type matrix, class CH_DATE_CLAMP: ClickHouse clamps the DATE 2300-01-01 to 2299-12-31; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_CH_DATE_CLAMP when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[mysql]: DEFECT_MYSQL_TIME_RANGE_REFUSED over 48 source->override pairs #d2d6953d",
             "2026-11-16: type matrix, class MYSQL_TIME_RANGE_REFUSED: a TIME of 24 hours or more, or a negative one, is refused as unrepresentable; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_MYSQL_TIME_RANGE_REFUSED when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[oracle]: DEFECT_ORACLE_NUMBER_INF_FINITE over 4 source->override pairs #7dbfcb1b",
             "2026-11-16: type matrix, class ORACLE_NUMBER_INF_FINITE: an Oracle NUMBER or FLOAT of plus or minus infinity is delivered as a finite value; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_ORACLE_NUMBER_INF_FINITE when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[oracle]: DEFECT_ORACLE_INTERVAL_DS_NS over 4 source->override pairs #a8f0c052",
             "2026-11-16: type matrix, class ORACLE_INTERVAL_DS_NS: an INTERVAL DAY TO SECOND(9) loses its nanoseconds; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_ORACLE_INTERVAL_DS_NS when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load[postgres]: DEFECT_CH_DATE_CLAMP over 1 source->override pairs #c493c49f",
             "2026-11-16: type matrix, class CH_DATE_CLAMP: ClickHouse clamps the DATE 2300-01-01 to 2299-12-31; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_CH_DATE_CLAMP when it changes",
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
    KnownRed("no_silent_null_load_cloud[mysql]: DEFECT_MYSQL_TINYINT_BOOL over 21 source->override pairs #fd0cc323",
             "2026-11-16: type matrix, class MYSQL_TINYINT_BOOL: a TINYINT(1) holding a value outside 0 and 1 is delivered as boolean true; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_MYSQL_TINYINT_BOOL when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load_cloud[mysql]: DEFECT_MYSQL_BIT_TOPBIT_REFUSED over 6 source->override pairs #18e21fc9",
             "2026-11-16: type matrix, class MYSQL_BIT_TOPBIT_REFUSED: a BIT(64) with the top bit set is refused as unrepresentable; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_MYSQL_BIT_TOPBIT_REFUSED when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load_cloud[mysql]: DEFECT_MYSQL_DATE_ZERO_BQ_RAW over 1 source->override pairs #bf670169",
             "2026-11-16: type matrix, class MYSQL_DATE_ZERO_BQ_RAW: DATE 0000-01-01 is staged raw and BigQuery rejects the load; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_MYSQL_DATE_ZERO_BQ_RAW when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load_cloud[mysql]: DEFECT_MYSQL_TIME_RANGE_REFUSED over 72 source->override pairs #74c6d2d0",
             "2026-11-16: type matrix, class MYSQL_TIME_RANGE_REFUSED: a TIME of 24 hours or more, or a negative one, is refused as unrepresentable; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_MYSQL_TIME_RANGE_REFUSED when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load_cloud[mssql]: DEFECT_MSSQL_SCALE_SEVEN_TRUNC over 45 source->override pairs #5bc27ffb",
             "2026-11-16: type matrix, class MSSQL_SCALE_SEVEN_TRUNC: a scale-7 time value loses its 100 ns digit to microseconds; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_MSSQL_SCALE_SEVEN_TRUNC when it changes",
             _EXPIRES),
    KnownRed("no_silent_null_load_cloud[mssql]: DEFECT_MSSQL_MONEY_BIG over 15 source->override pairs #1c080c28",
             "2026-11-16: type matrix, class MSSQL_MONEY_BIG: a money value of 9e11 or more is refused as unrepresentable; open, closes when the product delivers it exact or refuses it by a coded cause, and the line reads DEFECT_GONE_MSSQL_MONEY_BIG when it changes",
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
