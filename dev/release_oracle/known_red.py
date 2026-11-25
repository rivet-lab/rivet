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
    by_class: bool = False

    def keys(self) -> tuple[str, ...]:
        """Every whole failure message this one defect may print."""
        return (self.match, *self.alt)


_EXPIRES = "2026-11-29"
_BASELINE_EXPIRES = "2026-12-15"
GATE, ISLAND, OFFLINE, NIGHTLY = "gate", "island", "offline", "nightly"
_ISLAND_TIER = re.compile(r"\] full: ")
_OFFLINE_TIER = re.compile(r"\[offline\]: ")
_NIGHTLY_KEY = re.compile(r"^nightly\[")
_GATE_CLASS = re.compile(r"^(?P<matrix>[a-z_]+)\[(?P<kind>[a-z_]+)\](?: (?P<tier>[a-z]+))?: (?P<cause>[A-Z_]+) over ")
_CELL = re.compile(r"^(?P<cell>[A-Za-z_]\w*::\w[\w:]*) — ")
_DATED = re.compile(r"\b20\d\d-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b")
_ORACLE_CDC_LOAD = (
    "2026-11-25: the previous release's `rivet init --mode cdc` for Oracle writes no `load:` block and Oracle CDC does not load yet, so the cell's `load` refuses; "
    "closes when Oracle CDC loads (ADR-0037, Oracle GA); re-review 2026-11-29"
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
    KnownRed("no_silent_null_cdc[mssql]: CHECK_RUN_SPLIT over 33 source->override pairs #aee2fb98",
             "2026-11-25: CDC leg: check exits 0 and run refuses RIVET_SOURCE_CDC_PREREQUISITE on the added and type_changed legs, a prerequisite the stream reads from the server at run time; measured 2026-11-25 on main 772aa68d, 33 pairs, no other code; closes with #912 (check decides the prerequisite); re-review 2026-11-29",
             _EXPIRES),
    KnownRed("no_silent_null_cdc[mysql]: CHECK_RUN_SPLIT over 37 source->override pairs #bae71028",
             "2026-11-25: CDC leg: check exits 0 and run refuses RIVET_SOURCE_CDC_PREREQUISITE on the MINIMAL and NOBLOB row-image legs, a prerequisite the stream reads from the server at run time; measured 2026-11-25 on main 772aa68d, 37 pairs, no other code; closes with #912 (check decides the prerequisite); re-review 2026-11-29",
             _EXPIRES),
    KnownRed("check_equals_run_cloud[offline]: CHECK_RUN_SPLIT over 1 cells #3aec5ed8",
             "2026-11-25: BigQuery load leg: check exits 0 and load refuses RIVET_LOAD_WAREHOUSE_REJECTED (dataset not found) because check never contacts the warehouse; replayed 2026-11-25 on main 772aa68d with cloud_legs --tier offline, still RED; closes with #1180; re-review 2026-11-29",
             _EXPIRES,
             alt=("check_equals_run_cloud[cloud]: CHECK_RUN_SPLIT over 1 cells #3aec5ed8",)),
    KnownRed("no_secret_in_output_cloud[offline]: SECRET_LEAK over 1 cells #6e8be2c9",
             "2026-11-25: BigQuery load leg: a 401 body that echoes the Authorization header is printed verbatim in stderr and the summary because the warehouse error text is not masked; replayed 2026-11-25 on main 772aa68d with cloud_legs --tier offline, still RED; closes with #1146; re-review 2026-11-29",
             _EXPIRES),
    KnownRed("ab-diff[keyset_parallel]: DIFFERS from the previous release in files, manifest.fingerp, manifest.part_co, manifest.fingerprints[len], manifest.fingerprints[0], manifest.part_count — an observable behaviour change; it needs a verdict (intended and changelogged, or a regression)",
             "2026-11-24: the parallel keyset export (chunk_size 5000, parallel 4, 50000 rows) writes 64 parts where 0.33.0 wrote 12, rows read back identical (50000); intended by #1125 (changelog.d/keyset-uuid-recut.md); closes when 0.34.0 is the previous release the ab-diff compares against; re-review 2026-12-15",
             _BASELINE_EXPIRES),
    KnownRed("ab-diff: 8 scenarios compared, 1 DIFFER (keyset_parallel) — see the rows above",
             "2026-11-24: the count row of the ab-diff stage that reports the keyset_parallel difference above; closes with it; re-review 2026-12-15",
             _BASELINE_EXPIRES),
    KnownRed("no_silent_null_load_cloud[postgres]: DEFECT_PG_NULL_LIST_EMPTY_BQ over 20 source->override pairs #cf3f250f",
             "2026-11-25: class PG_NULL_LIST_EMPTY_BQ: a NULL list is delivered as an empty list at BigQuery; accepted by owner 2026-11-21; unverified on the stand, the cloud tier needs BigQuery credentials; closes with #1188; re-review 2026-11-29",
             _EXPIRES),
    KnownRed("byte-identical[mysql/full]: 1 of 1 parts differ in bytes",
             "2026-11-24: v0.33.0 against this binary: the Arrow field metadata of every MySQL date, datetime and timestamp column reads rivet.fidelity compatible where v0.33.0 wrote exact; rows, schema and statistics are equal (#1084, a declared Behaviour change); closes when the release the gate grades against carries #1084; re-review 2026-11-29",
             _EXPIRES,
             ("byte-identical[mysql/chunked]: 3 of 3 parts differ in bytes",)),
    KnownRed("byte-identical[mssql/full]: 1 of 1 parts differ in bytes",
             "2026-11-24: v0.33.0 against this binary: a DATETIME2(7) column is Timestamp(ns) where v0.33.0 wrote Timestamp(us) (#1083, ADR-0038 CP1, a declared Behaviour change); closes when the release the gate grades against carries #1083; re-review 2026-11-29",
             _EXPIRES,
             ("byte-identical[mssql/chunked]: 3 of 3 parts differ in bytes",
              "byte-identical[mssql/full_as_init_writes_it]: the schemas differ: previous [('id', 'BIGINT'), ('v', 'BIGINT'), ('updated_at', 'TIMESTAMP'), ('_rivet_exported_at', 'TIMESTAMP WITH TIME ZONE'), ('_rivet_row_hash', 'BIGINT')], this [('id', 'BIGINT'), ('v', 'BIGINT'), ('updated_at', 'TIMESTAMP_NS'), ('_rivet_exported_at', 'TIMESTAMP WITH TIME ZONE'), ('_rivet_row_hash', 'BIGINT')]",
              "byte-identical[mssql/chunked_as_init_writes_it]: the schemas differ: previous [('id', 'BIGINT'), ('v', 'BIGINT'), ('updated_at', 'TIMESTAMP'), ('_rivet_exported_at', 'TIMESTAMP WITH TIME ZONE'), ('_rivet_row_hash', 'BIGINT')], this [('id', 'BIGINT'), ('v', 'BIGINT'), ('updated_at', 'TIMESTAMP_NS'), ('_rivet_exported_at', 'TIMESTAMP WITH TIME ZONE'), ('_rivet_row_hash', 'BIGINT')]")),
    KnownRed("byte-identical[mongo/full]: 1 of 1 parts differ in bytes",
             "2026-11-24: v0.33.0 against this binary: the flat _id column of a collection holding int keys is labelled logical_string where v0.33.0 wrote objectid and exact; values and Arrow type unchanged (#1045, a declared Behaviour change); closes when the release the gate grades against carries #1045; re-review 2026-11-29",
             _EXPIRES),
    KnownRed(nightly_key("refusal_names_true_cause", "reader", "the"),
             "2026-11-25: #917, the nightly refusal-names-true-cause part: 27 reader cells, 21 source cells (a RIVET_VERIFY_* code named in src and in neither errors.md nor codes::ALL), the STATEMENT_TIMEOUT word cell and the CHUNK_CHECKPOINT_GONE remedy stay red; replayed on main 772aa68d (offline and live); closes with the fix of #917; re-review 2026-11-29",
             _EXPIRES,
             alt=tuple(nightly_key("refusal_names_true_cause", kind, cause) for kind, cause in (("source", "named"), ("word", "printed"), ("wording", "remedy")))),
    _open("live_cdc::open_defect_a_pg_timestamp_declared_text_is_one_verdict_on_batch_and_the_stream",
          "batch and the stream disagree on a timestamp declared text: batch refused[RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH exit 1], stream delivered",
          "2026-11-25: a PostgreSQL timestamp declared `text` is refused by batch (RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH) and delivered by the stream; replayed 2026-11-25 on main 772aa68d, as recorded; found in #1162, closes with #1233 (PG declared text on batch) or a stream refusal by the same code"),
    _open("live_cdc::open_defect_a_mysql_zero_datetime_declared_text_is_the_batch_text_on_the_stream",
          "assertion `left == right` failed: a zero datetime declared text reads differently on the stream and on batch",
          "2026-11-25: a MySQL `0000-00-00 00:00:00` declared `text` is `0000-00-00 00:00:00.000000` on batch and NULL on the stream; replayed 2026-11-25 on main 772aa68d, as recorded; found in #1162, no open PR, closes when the stream writes the batch text"),
    _open("live_cdc_oracledb::open_defect_an_oracle_julian_date_declared_text_is_delivered_by_the_stream_as_batch_delivers_it",
          "stream was refused: refused[RIVET_SOURCE_CDC_CELL_UNSUPPORTED exit 5]",
          "2026-11-25: an Oracle DATE only the Julian calendar holds (1500-02-29) declared `text` is delivered by batch and refused by the stream; replayed 2026-11-25 on main 772aa68d, as recorded; found in #1162, no open PR, closes when the stream delivers the text batch delivers"),
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


def _of_class(msg: str, k: KnownRed) -> bool:
    """Whether `msg` is a per-cell-set message of the class `k` is keyed by, whatever its pair count and digest."""
    head, sep, _ = msg.partition(" over ")
    return bool(sep) and head == k.match.partition(" over ")[0]


def match(msg: str, today: _dt.date | None = None) -> tuple[KnownRed | None, bool]:
    """The entry `msg` matches and whether it is still in date."""
    today = today or _dt.date.today()
    for k in KNOWN_RED:
        if msg in k.keys() or unframed(msg) in k.keys() or (k.by_class and _of_class(msg, k)):
            return k, today <= _dt.date.fromisoformat(k.expires)
    return None, False


def problems(entries: tuple[KnownRed, ...] | None = None) -> list[str]:
    """Entries with an empty match, a reason that carries no ISO date, or an expiry that is not a date."""
    out = []
    for k in KNOWN_RED if entries is None else entries:
        name = k.match[:80] or "<empty match>"
        if not k.match.strip():
            out.append(f"{name}: empty match")
        if k.by_class and " over " not in k.match:
            out.append(f"{name}: keyed by class but not a per-cell-set message")
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
    assert problems((KnownRed("m::t — boom", "2026-11-04 why", "2026-11-15", by_class=True),)), "a class key needs a per-cell-set message"
    assert problems() == [], "the ledger itself must be clean"
    cls = KnownRed("no_silent_null_cdc[e]: CHECK_RUN_SPLIT over 4 source->override pairs #aa", "2026-11-21: x", "2026-11-22", by_class=True)
    exact = KnownRed(cls.match, cls.reason, cls.expires)
    other = "no_silent_null_cdc[e]: CHECK_RUN_SPLIT over 9 source->override pairs #bb"
    assert _of_class(other, cls) and _of_class(cls.match, cls), "a class entry matches any pair count and digest of its class"
    for near in ("no_silent_null_cdc[f]: CHECK_RUN_SPLIT over 9 pairs #bb", "no_silent_null_cdc[e]: WRONG_VALUE over 9 pairs #bb", "no_silent_null[e] full: CHECK_RUN_SPLIT over 9 pairs #bb", "no_silent_null_cdc[e]: CHECK_RUN_SPLIT"):
        assert not _of_class(near, cls), f"a class entry never covers another engine, cause or scenario: {near}"
    assert exact.by_class is False and not any(k.by_class and " over " not in k.match for k in KNOWN_RED)
    own = nightly_key("refusal_names_true_cause", "cmd", "printed")
    nightly = KnownRed(own, "2026-11-19: x", _EXPIRES)
    assert lane_of(own) == NIGHTLY and not any(owed_by(nightly, lane) for lane in (GATE, ISLAND, OFFLINE)), \
        "a nightly class is owed by no gate run: a full gate would block on it"
    cut = nightly_classes((KnownRed("no_silent_null[mysql] smoke: CHECK_RUN_SPLIT over 3 source->override pairs #ab", "2026-11-19: x", _EXPIRES), KnownRed("no_silent_null[oracle] full: CHECK_RUN_SPLIT over 3 source->override pairs #ab", "2026-11-19: x", _EXPIRES)))
    assert nightly_key("no_silent_null", "mysql", "CHECK_RUN_SPLIT") in cut and nightly_key("no_silent_null", "oracle", "CHECK_RUN_SPLIT") in cut, \
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
    _byte_identical_keys_are_the_stage_messages()
    _class_entries_hold_the_stage_messages()
    print("known_red self-test: ok")
    return 0


def _byte_identical_keys_are_the_stage_messages() -> None:
    """Every `byte-identical[...]` key of the ledger is a message the stage prints, built from its own graders."""
    from .guarantees import Stamped, parts_differ, stamped_differ

    held = {k for e in KNOWN_RED for k in e.keys() if k.startswith("byte-identical[")}
    printed = {f"byte-identical[{engine}/{mode}]: {parts_differ(['a'] * n, ['b'] * n)}" for engine, mode, n in (("mysql", "full", 1), ("mysql", "chunked", 3), ("mssql", "full", 1), ("mssql", "chunked", 3), ("mongo", "full", 1))}
    cols = [("id", "BIGINT"), ("v", "BIGINT"), ("updated_at", "TIMESTAMP"), ("_rivet_exported_at", "TIMESTAMP WITH TIME ZONE"), ("_rivet_row_hash", "BIGINT")]
    now = [(n, "TIMESTAMP_NS" if n == "updated_at" else t) for n, t in cols]
    why = stamped_differ(Stamped((1, 1), ((cols, []), (now, [])), (250000, 250000), None, None, None), 250000)
    printed |= {f"byte-identical[mssql/{mode}_as_init_writes_it]: {why}" for mode in ("full", "chunked")}
    assert held == printed, f"the ledger and the stage disagree: only in the ledger {sorted(held - printed)}, only in the stage {sorted(printed - held)}"
    assert match("byte-identical[postgres/full]: 1 of 1 parts differ in bytes", _dt.date(2026, 11, 24))[0] is None and match("byte-identical[mysql/full]: 2 of 2 parts differ in bytes", _dt.date(2026, 11, 24))[0] is None, \
        "an entry holds its own engine and cell only"


def _class_entries_hold_the_stage_messages() -> None:
    """Every `by_class` entry holds the message `failure_message` prints for any pair set of its own scenario, engine, tier and cause."""
    from .property_no_silent_null import Result, failure_message

    day = _dt.date(2026, 11, 24)
    for k in KNOWN_RED:
        if not k.by_class:
            continue
        m = _GATE_CLASS.match(k.match)
        assert m, f"a class entry is a gate message: {k.match}"
        for n in (1, 7, 9):
            rs = [Result(f"{m['kind']}/p{i}->none/full/parquet", "x") for i in range(n)]
            msg = failure_message(m["kind"], m["cause"], rs, m["matrix"], m["tier"] or "")
            assert match(msg, day)[0] is k, f"the class entry {k.match} does not hold the stage message {msg}"
    assert all(k.by_class for k in KNOWN_RED if "flap" in k.reason and _GATE_CLASS.match(k.match)), \
        "a gate entry whose reason says the pair set flaps between runs is keyed by class"

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
