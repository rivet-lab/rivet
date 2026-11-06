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


_TS9 = (
    "Oracle TIMESTAMP(9) is delivered at microseconds today: docs/type-capability-matrix.yaml's oracle TIMESTAMP(9) row is a known_defect "
    "(exact native is Timestamp(ns), ADR-0038 CP1, Oracle engine step), so the 1 ns sentinel lands truncated. "
    "Renewed 2026-11-01: still live (target/triage/sent_oracle_alone.log); closes when the Oracle engine step lands Timestamp(ns). "
    "Pinned 2026-11-01 to the whole symptom: a NULL, epoch or other wrong value no longer matches"
)


def _open(cell: str, symptom: str, reason: str) -> KnownRed:
    """The entry of one `open_defect_*` live cell: its test path and the first line of its panic."""
    return KnownRed(f"{cell} — {symptom}", reason, _EXPIRES)



KNOWN_RED: tuple[KnownRed, ...] = (
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
