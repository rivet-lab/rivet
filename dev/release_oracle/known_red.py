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
    KnownRed("no_silent_null[mysql]: SILENT_NULL over 244 source->override pairs #d48b99aa",
             "2026-11-08: a non-NULL MySQL value is delivered as NULL by the `_ => append_null()` arms of src/source/mysql/arrow_convert.rs:690-960. The int, float and double pairs are closed by #904 (see #904); 123 other pairs stay open (date, timestamp, float and int targets from text, binary, bit and enum sources). The digest changes when #904 merges: swap this entry then",
             _EXPIRES),
    KnownRed("no_silent_null[mysql]: WRONG_VALUE over 27 source->override pairs #0ddc558f",
             "2026-11-08: exit 0 with fidelity exact while a value changed (integer or decimal to bool, to a narrower decimal or to int8, unsigned to signed decimal, DOUBLE 1e300 to float4 as inf); open, new defect, closes when the override refuses a value it cannot hold",
             _EXPIRES),
    KnownRed("no_silent_null[mysql]: CHECK_RUN_SPLIT over 84 source->override pairs #230f1ac3",
             "2026-11-08: check exits 0 and run refuses with a code (VALUE_UNREPRESENTABLE, FORMAT_COLUMN_UNSUPPORTED, OVERRIDE_WIRE_MISMATCH) because check reads neither the values nor the format; open, the pair set shifts by one when #904 merges (see #904)",
             _EXPIRES),
    KnownRed("no_silent_null[mysql]: UNCODED_STOP over 62 source->override pairs #19da4a27",
             "2026-11-08: a timestamp_ns or timestamp_tz_ns override stops with RIVET_INTERNAL_TYPE_BUILDER (src/source/mysql/arrow_convert.rs:970, no converter), exit 6, nothing silent; open, closes when the pair is refused by code at check",
             _EXPIRES),
    KnownRed("no_silent_null[postgres]: WRONG_VALUE over 2 source->override pairs #04533e41",
             "2026-11-08: BIGINT to decimal(9,2) or decimal(18,4) wraps the value at exit 0 with fidelity exact; open, new defect, same class as mssql and mysql",
             _EXPIRES),
    KnownRed("no_silent_null[postgres]: CHECK_RUN_SPLIT over 200 source->override pairs #d14d6e5a",
             "2026-11-08: check exits 0 and run refuses with a code (VALUE_UNREPRESENTABLE, TYPE_UNSAFE_MAPPING, FORMAT_COLUMN_UNSUPPORTED) for 200 pairs, CIDR, INET and MONEY with no override among them; open, new defect",
             _EXPIRES),
    KnownRed("no_silent_null[postgres]: UNCODED_STOP over 35 source->override pairs #a0cca18b",
             "2026-11-08: a uuid override on a non-uuid column stops with exit 1 and no code (error deserializing column, PgUuidBytes in src/source/postgres/arrow_convert.rs); open, new defect, nothing silent",
             _EXPIRES),
    KnownRed("no_silent_null[mssql]: WRONG_VALUE over 2 source->override pairs #04533e41",
             "2026-11-08: BIGINT to decimal(9,2) or decimal(18,4) wraps the value at exit 0 with fidelity exact; open, new defect, same class as postgres and mysql",
             _EXPIRES),
    KnownRed("no_silent_null[mssql]: CHECK_RUN_SPLIT over 27 source->override pairs #4a007177",
             "2026-11-08: check exits 0 and run refuses with a code (VALUE_UNREPRESENTABLE, FORMAT_COLUMN_UNSUPPORTED) for 27 pairs; open, new defect",
             _EXPIRES),
    KnownRed("no_silent_null[mssql]: UNCODED_STOP over 232 source->override pairs #78d8cf40",
             "2026-11-08: a row of the wrong wire type reaches the builder (src/source/mssql/arrow_convert.rs:467-493, `expected BooleanBuilder but row carried I64`) and stops with RIVET_INTERNAL_TYPE_BUILDER, exit 6; open, new defect, nothing silent, closes when the pair is refused by code at check",
             _EXPIRES),
    KnownRed("no_silent_null[oracle]: WRONG_VALUE over 4 source->override pairs #fafb060a",
             "2026-11-08: NUMBER(9) and NUMBER(18) to decimal(9,2) or decimal(18,4) wrap the value, and NUMBER 1E125 to float4 becomes inf, at exit 0 with fidelity exact; open, new defect",
             _EXPIRES),
    KnownRed("no_silent_null[oracle]: CHECK_RUN_SPLIT over 199 source->override pairs #31252431",
             "2026-11-08: check exits 0 where run stops with RIVET_SOURCE_QUERY_FAILED unsupported conversion or RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH on 199 Oracle pairs; open, new defect, nothing silent",
             _EXPIRES),
    KnownRed("no_silent_null[oracle]: UNCODED_STOP over 24 source->override pairs #f65d2680",
             "2026-11-08: a decimal(60,10) override stops with RIVET_INTERNAL_TYPE_BUILDER (src/source/oracle/arrow_convert.rs:540, no row decoder for Decimal256), exit 6, and NUMBER to int2 or int4 stops with exit 1 and no code; open, new defect, nothing silent",
             _EXPIRES),
    KnownRed("no_silent_null[mongo]: OVERRIDE_IGNORED over 34 source->override pairs #b4b9d56b",
             "2026-11-08: a type override on a Mongo `_id` or `document` column is accepted and the column is delivered as VARCHAR, exit 0; open, new defect, closes when the override is delivered or refused by code",
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
