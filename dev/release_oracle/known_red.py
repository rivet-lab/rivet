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


_EXPIRES = "2026-11-15"
_DATED = re.compile(r"\b20\d\d-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b")
_ORACLE_CDC_LOAD = (
    "Oracle CDC does not load yet (ADR-0037; the Oracle GA work). The previous release's `rivet init --mode cdc` writes no `load:` block, and this cell still drives the previous release through `load`, which refuses. `upgrade[oracle/cdc-load/init=this]` grades what the scaffold does promise (the baseline Parquet equals the source). The cycle stays unsupported until Oracle CDC loads. Pinned 2026-11-01 to the whole refusal line"
)


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
    KnownRed("no_silent_null[mongo]: OVERRIDE_IGNORED over 38 source->override pairs #dbdffe5c",
             "2026-11-10: batch leg, NEW-OVERRIDE-IGNORED: a type override on a Mongo `_id` or `document` column is accepted and the column is delivered as VARCHAR at exit 0; open, closes when the override is delivered or refused by code",
             _EXPIRES),
    KnownRed("no_silent_null[mssql]: WRONG_VALUE over 3 source->override pairs #5e462b16",
             "2026-11-10: batch leg, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way); open, closes when the override refuses a value it cannot hold",
             _EXPIRES),
    KnownRed("no_silent_null[mssql]: CHECK_RUN_SPLIT over 52 source->override pairs #237d1a88",
             "2026-11-10: batch leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED, RIVET_SOURCE_VALUE_UNREPRESENTABLE) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("no_silent_null[mssql]: UNCODED_STOP over 232 source->override pairs #78d8cf40",
             "2026-11-10: batch leg, PR-CAUSE: run stops on a wrong-type override with RIVET_INTERNAL_TYPE_BUILDER, nothing silent; open, closes when the pair is refused by a coded cause at check",
             _EXPIRES),
    KnownRed("no_silent_null[mssql]: INTEGRITY_BLIND over 1 source->override pairs #8f3cccc4",
             "2026-11-10: batch leg, PR-SELF-GRADE: validate --depth full exits 0 over a cell whose values changed, it answers from rivet own fingerprint; open, closes when validate compares against the source",
             _EXPIRES),
    KnownRed("no_silent_null[mysql]: SILENT_NULL over 131 source->override pairs #b7658548",
             "2026-11-10: batch leg, PR-NULL: a non-NULL value is delivered as NULL at exit 0 (MySQL text, binary, bit or decimal under a date, timestamp, int or float override, the `_ => append_null()` arms of src/source/mysql/arrow_convert.rs); open, closes when the pair is refused by code",
             _EXPIRES),
    KnownRed("no_silent_null[mysql]: WRONG_VALUE over 32 source->override pairs #9844668d",
             "2026-11-10: batch leg, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way); open, closes when the override refuses a value it cannot hold",
             _EXPIRES),
    KnownRed("no_silent_null[mysql]: CHECK_RUN_SPLIT over 144 source->override pairs #c580dfee",
             "2026-11-10: batch leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED, RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("no_silent_null[mysql]: UNCODED_STOP over 70 source->override pairs #17da41fb",
             "2026-11-10: batch leg, PR-CAUSE: run stops on a wrong-type override with RIVET_INTERNAL_TYPE_BUILDER, nothing silent; open, closes when the pair is refused by a coded cause at check",
             _EXPIRES),
    KnownRed("no_silent_null[mysql]: INTEGRITY_BLIND over 159 source->override pairs #c30a1779",
             "2026-11-10: batch leg, PR-SELF-GRADE: validate --depth full exits 0 over a cell whose values changed, it answers from rivet own fingerprint; open, closes when validate compares against the source",
             _EXPIRES),
    KnownRed("no_silent_null[oracle]: WRONG_VALUE over 12 source->override pairs #9194cbc1",
             "2026-11-10: batch leg, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way); open, closes when the override refuses a value it cannot hold",
             _EXPIRES),
    KnownRed("no_silent_null[oracle]: CHECK_RUN_SPLIT over 231 source->override pairs #f53d4ed8",
             "2026-11-10: batch leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_QUERY_FAILED, RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED, RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("no_silent_null[oracle]: UNCODED_STOP over 24 source->override pairs #f65d2680",
             "2026-11-10: batch leg, PR-CAUSE: run stops on a wrong-type override with RIVET_INTERNAL_TYPE_BUILDER, nothing silent; open, closes when the pair is refused by a coded cause at check",
             _EXPIRES),
    KnownRed("no_silent_null[oracle]: INTEGRITY_BLIND over 9 source->override pairs #9fbb1219",
             "2026-11-10: batch leg, PR-SELF-GRADE: validate --depth full exits 0 over a cell whose values changed, it answers from rivet own fingerprint; open, closes when validate compares against the source",
             _EXPIRES),
    KnownRed("no_silent_null[postgres]: WRONG_VALUE over 2 source->override pairs #04533e41",
             "2026-11-10: batch leg, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way); open, closes when the override refuses a value it cannot hold",
             _EXPIRES),
    KnownRed("no_silent_null[postgres]: CHECK_RUN_SPLIT over 218 source->override pairs #ba6edeb0",
             "2026-11-10: batch leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED, RIVET_TYPE_UNSAFE_MAPPING) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("no_silent_null[postgres]: UNCODED_STOP over 35 source->override pairs #a0cca18b",
             "2026-11-10: batch leg, PR-CAUSE: run stops on a wrong-type override with exit 1 and no code, nothing silent; open, closes when the pair is refused by a coded cause at check",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mssql]: WRONG_VALUE over 2 source->override pairs #04533e41",
             "2026-11-10: CDC leg, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way); open, closes when the override refuses a value it cannot hold",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mssql]: CHECK_RUN_SPLIT over 93 source->override pairs #e528c304",
             "2026-11-10: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH, RIVET_SOURCE_CDC_PREREQUISITE, RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mssql]: UNCODED_STOP over 54 source->override pairs #ffec78b9",
             "2026-11-10: CDC leg, PR-CAUSE: run stops on a wrong-type override with RIVET_INTERNAL_TYPE_BUILDER, nothing silent; open, closes when the pair is refused by a coded cause at check",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mysql]: SILENT_NULL over 11 source->override pairs #23a2d157",
             "2026-11-10: CDC leg, PR-NULL: a non-NULL value is delivered as NULL at exit 0 (MySQL text, binary, bit or decimal under a date, timestamp, int or float override, the `_ => append_null()` arms of src/source/mysql/arrow_convert.rs); open, closes when the pair is refused by code",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mysql]: WRONG_VALUE over 5 source->override pairs #6544e32c",
             "2026-11-10: CDC leg, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way); open, closes when the override refuses a value it cannot hold",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mysql]: CHECK_RUN_SPLIT over 128 source->override pairs #34ffa969",
             "2026-11-10: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH, RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_SOURCE_CDC_PREREQUISITE, RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mysql]: UNCODED_STOP over 14 source->override pairs #8ded2658",
             "2026-11-10: CDC leg, PR-CAUSE: run stops on a wrong-type override with RIVET_INTERNAL_TYPE_BUILDER, nothing silent; open, closes when the pair is refused by a coded cause at check",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[oracle]: WRONG_VALUE over 1 source->override pairs #63a430dd",
             "2026-11-10: CDC leg, PR-NULL: exit 0 with fidelity exact while the value changed (integer to a narrower decimal wraps, text or integer to bool, NUMBER 1E125 to float4 as inf, a negative-scale decimal scaled the wrong way); open, closes when the override refuses a value it cannot hold",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[oracle]: CHECK_RUN_SPLIT over 72 source->override pairs #92d2b111",
             "2026-11-10: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_QUERY_FAILED, RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED, RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[oracle]: UNCODED_STOP over 9 source->override pairs #56a76715",
             "2026-11-10: CDC leg, PR-CAUSE: run stops on a wrong-type override with RIVET_INTERNAL_TYPE_BUILDER, nothing silent; open, closes when the pair is refused by a coded cause at check",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[postgres]: CHECK_RUN_SPLIT over 140 source->override pairs #d4b5fc06",
             "2026-11-10: CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH, RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_TYPE_UNSAFE_MAPPING, RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[postgres]: UNCODED_STOP over 7 source->override pairs #aa3425f5",
             "2026-11-10: CDC leg, PR-CAUSE: run stops on a wrong-type override with exit 1 and no code, nothing silent; open, closes when the pair is refused by a coded cause at check",
             _EXPIRES),
    KnownRed("no_silent_null_load[mssql]: CHECK_RUN_SPLIT over 5 source->override pairs #078f0729",
             "2026-11-10: ClickHouse load leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, RIVET_LOAD_WAREHOUSE_REJECTED, RIVET_SOURCE_VALUE_UNREPRESENTABLE) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("no_silent_null_load[mysql]: CHECK_RUN_SPLIT over 5 source->override pairs #a1d9f398",
             "2026-11-10: ClickHouse load leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_VALUE_UNREPRESENTABLE, RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, RIVET_LOAD_WAREHOUSE_REJECTED) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("no_silent_null_load[oracle]: CHECK_RUN_SPLIT over 4 source->override pairs #a42d322e",
             "2026-11-10: ClickHouse load leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("no_silent_null_load[postgres]: CHECK_RUN_SPLIT over 8 source->override pairs #3017caa9",
             "2026-11-10: ClickHouse load leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_TYPE_UNSAFE_MAPPING, RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, RIVET_LOAD_WAREHOUSE_REJECTED) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
    KnownRed("check_equals_run_cloud[offline]: CHECK_RUN_SPLIT over 1 cells #3aec5ed8",
             "2026-11-10: BigQuery load leg, PR-PRECHECK: check exits 0 and load refuses by code (RIVET_LOAD_WAREHOUSE_REJECTED, dataset not found) because check never contacts the warehouse; open, closes when check probes the target dataset or the matrix accepts the split",
             _EXPIRES),
    KnownRed("no_secret_in_output_cloud[offline]: SECRET_LEAK over 1 cells #6e8be2c9",
             "2026-11-10: BigQuery load leg, PR-CAUSE: a server that echoes the Authorization header in its 401 body gets it printed verbatim in stderr and --summary-output because the warehouse error text is not masked; open, closes when load masks server error text",
             _EXPIRES),
    KnownRed("no_silent_null[mysql_cdc_grants]: CHECK_RUN_SPLIT over 2 source->override pairs #3c225a11",
             "2026-11-10: grants-CDC leg, PR-PRECHECK: check exits 0 and run refuses by code (RIVET_SOURCE_VALUE_UNREPRESENTABLE) because check reads neither the values nor the format; open, closes when check refuses the same pair",
             _EXPIRES),
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
