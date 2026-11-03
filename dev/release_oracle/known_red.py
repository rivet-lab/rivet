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


_HCF1 = (
    "HCF-1, 2026-11-03: a chunk_size of 2^63 or more wraps to a window size of 0 or less, plans no window and exits 0 with 0 rows, "
    "where validate, plan and run name no refusal. Closes with fix/config-knob-quantity-counts (RIVET_CONFIG_VALUE_OUT_OF_RANGE from the one Knob)"
)
_HCF8 = (
    "HCF-8, 2026-11-03: windows over a key range as wide as i64, sized by chunk_count or by a chunk_size near i64::MAX, overflow i64 in the "
    "plan (src/preflight/analysis.rs) or the run (src/pipeline/chunked/detect.rs): a debug panic, exit 101. "
    "Closes with fix/config-knob-quantity-counts (the key span in u128)"
)
_HCF4 = (
    "HCF-4, 2026-11-03: `rivet cdc --rollover 0` is accepted and runs while `cdc.rollover: 0` is refused RIVET_CONFIG_CDC_ROLLOVER_INVALID. "
    "Closes with fix/config-knob-quantity-counts (the flag is read through the same Knob)"
)
_HCF4_MONGO = (
    "HCF-4, 2026-11-03: Mongo `page_size: 0` is clamped to 1 (src/plan/build.rs) and the run succeeds while every other count knob refuses 0. "
    "Closes with fix/config-knob-quantity-counts (RIVET_CONFIG_VALUE_OUT_OF_RANGE, the clamp removed)"
)
_HCF3 = (
    "HCF-3, 2026-11-03: quality.null_ratio_max accepts NaN, infinity, values above 1 and values below 0 (src/quality.rs), "
    "so a threshold that can never fire passes validate. Closes with fix/config-knob-quantity-counts (a ratio outside [0, 1] is RIVET_CONFIG_VALUE_OUT_OF_RANGE)"
)


KNOWN_RED: tuple[KnownRed, ...] = (
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
    _open("live_config_knob_counts::open_defect_cdc_rollover_zero_is_refused_on_the_flag_mongo",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_CONFIG_CDC_ROLLOVER_INVALID] (exit 1)",
          _HCF4),
    _open("live_config_knob_counts::open_defect_cdc_rollover_zero_is_refused_on_the_flag_mssql",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_CONFIG_CDC_ROLLOVER_INVALID] (exit 1)",
          _HCF4),
    _open("live_config_knob_counts::open_defect_cdc_rollover_zero_is_refused_on_the_flag_mysql",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_CONFIG_CDC_ROLLOVER_INVALID] (exit 1)",
          _HCF4),
    _open("live_config_knob_counts::open_defect_cdc_rollover_zero_is_refused_on_the_flag_oracle",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_CONFIG_CDC_ROLLOVER_INVALID] (exit 1)",
          _HCF4),
    _open("live_config_knob_counts::open_defect_cdc_rollover_zero_is_refused_on_the_flag_postgres",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_CONFIG_CDC_ROLLOVER_INVALID] (exit 1)",
          _HCF4),
    _open("live_config_knob_counts::open_defect_chunk_size_past_i64_is_refused_mssql",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_CONFIG_VALUE_OUT_OF_RANGE] (exit 1)",
          _HCF1),
    _open("live_config_knob_counts::open_defect_chunk_size_past_i64_is_refused_mysql",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_CONFIG_VALUE_OUT_OF_RANGE] (exit 1)",
          _HCF1),
    _open("live_config_knob_counts::open_defect_chunk_size_past_i64_is_refused_oracle",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_CONFIG_VALUE_OUT_OF_RANGE] (exit 1)",
          _HCF1),
    _open("live_config_knob_counts::open_defect_chunk_size_past_i64_is_refused_postgres",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_CONFIG_VALUE_OUT_OF_RANGE] (exit 1)",
          _HCF1),
    _open("live_config_knob_counts::open_defect_mongo_page_size_zero_is_refused",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_CONFIG_VALUE_OUT_OF_RANGE] (exit 1)",
          _HCF4_MONGO),
    _open("live_config_knob_counts::open_defect_null_ratio_max_outside_the_unit_interval_is_refused",
          "refusal contract: the invocation ended with success (exit 0), expected [RIVET_CONFIG_VALUE_OUT_OF_RANGE] (exit 1)",
          _HCF3),
    _open("live_config_knob_counts::open_defect_wide_key_windows_plan_and_deliver_mssql",
          "exit Some(101): attempt to add with overflow",
          _HCF8),
    _open("live_config_knob_counts::open_defect_wide_key_windows_plan_and_deliver_mysql",
          "exit Some(101): attempt to subtract with overflow",
          _HCF8),
    _open("live_config_knob_counts::open_defect_wide_key_windows_plan_and_deliver_oracle",
          "exit Some(101): attempt to subtract with overflow",
          _HCF8),
    _open("live_config_knob_counts::open_defect_wide_key_windows_plan_and_deliver_postgres",
          "exit Some(101): attempt to add with overflow",
          _HCF8),
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
