#!/usr/bin/env python3
"""One verdict for a night's parts: red only on a red class the ledger does not know, a lost part, an exit nothing explains, or an expired entry.

    nightly_delta.py --workflow FILE --job NAME --parts DIR [--prefix gate-part-] [--json FILE] [--today YYYY-MM-DD]
    nightly_delta.py --self-test

The parts are the matrix names of `jobs.NAME.strategy.matrix.part` in FILE, read from the workflow and never listed here.
A part NAME leaves `DIR/<prefix>NAME/part.txt` and `exit-code.txt`. Its red classes are read from the matrix verdict lines,
the test runner's failure lines and the gate's FAIL rows; the baseline is the ledger the gate already keeps,
dev/release_oracle/known_red.py (`nightly_classes`), with its reason and its expiry.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dev.release_oracle import known_red  # noqa: E402
from dev.release_oracle.core import DEVBOX  # noqa: E402
from dev.release_oracle.halves import STAGE_HALF  # noqa: E402

ANSI = re.compile(r"\x1b\[[0-9;]*m")
MATRIX_RED = re.compile(r"^(?P<matrix>[a-z][a-z0-9_]*) (?P<cell>\S+) RED (?P<why>.*)$")
NEXTEST = re.compile(r"^\s*(?P<what>FAIL|TIMEOUT|SIGABRT|SIGSEGV|SIGKILL|SIGTERM|LEAK-FAIL)\s+\[\s*[\d.]+s\]\s+(?:\(\s*\d+/\s*\d+\)\s+)?(?:\S+\s+)?(?P<test>\S+)\s*$")
LIBTEST = re.compile(r"^test (?P<test>\S+) \.\.\. FAILED$")
GATE_FAIL = re.compile(r"^  (?P<engine>\S+)\s+\S+\s+(?P<scenario>\S+)\s+\S+\s+FAIL\b")
CRASH = (
    ("python traceback", re.compile(r"^Traceback \(most recent call last\)")),
    ("compile error", re.compile(r"^error(\[E\d+\]: |: (could not compile|aborting due to|failed to run custom build command|linking with))")),
    ("runner shutdown", re.compile(r"The runner has received a shutdown signal")),
)
LOST_EXITS = (137, 143)
LOST, GREEN, KNOWN, NEW = "LOST", "GREEN", "KNOWN", "NEW"


def parts_of(workflow: Path, job: str) -> list[str]:
    """The part names the workflow's own matrix of `job` defines."""
    doc = yaml.safe_load(workflow.read_text())
    return [p["name"] for p in doc["jobs"][job]["strategy"]["matrix"]["part"]]


def first_word(text: str) -> str:
    """The first word of a cause, without its closing punctuation."""
    words = text.split()
    return words[0].rstrip(":,;") if words else "-"


def red_keys(text: str) -> tuple[set[str], set[str]]:
    """The red classes a part printed, and the crash markers that no class explains."""
    keys: set[str] = set()
    crashes: set[str] = set()
    for line in ANSI.sub("", text).splitlines():
        if m := MATRIX_RED.match(line.strip()):
            keys.add(known_red.nightly_key(m["matrix"], m["cell"].split("/")[0], first_word(m["why"])))
        elif m := NEXTEST.match(line):
            keys.add(known_red.nightly_key("test", m["test"], m["what"]))
        elif m := LIBTEST.match(line):
            keys.add(known_red.nightly_key("test", m["test"], "FAIL"))
        elif cell := known_red.cell_of(line):
            keys.add(cell)
        elif m := GATE_FAIL.match(line):
            keys.add(known_red.nightly_key("gate", f"{m['engine']}/{m['scenario']}", "FAIL"))
        for label, rx in CRASH:
            if rx.search(line):
                crashes.add(known_red.nightly_key("crash", label, "-"))
    return keys, crashes


def grade_part(code: int | None, text: str, baseline: dict[str, known_red.KnownRed], today: dt.date) -> dict:
    """The verdict of one part: LOST without a clean exit, GREEN on exit 0 with no red line, KNOWN when every red class is acknowledged and in date, else NEW."""
    if code is None or code in LOST_EXITS:
        return {"exit": code, "verdict": LOST, "new": [f"no verdict: exit {code}"], "known": []}
    keys, crashes = red_keys(text)
    if code == 0 and not keys:
        return {"exit": 0, "verdict": GREEN, "new": [], "known": []}
    new = sorted(crashes)
    known = []
    for key in sorted(keys):
        entry = baseline.get(key)
        if entry is None:
            new.append(key)
        elif today > dt.date.fromisoformat(entry.expires):
            new.append(f"{key} (acknowledged until {entry.expires}, expired)")
        else:
            known.append(key)
    if code == 0:
        new.append("red cells under exit 0")
    elif not keys:
        new.append(f"exit {code}: no red class explains it")
    return {"exit": code, "verdict": NEW if new else KNOWN, "new": new, "known": known}


def read_part(root: Path, prefix: str, name: str) -> tuple[int | None, str]:
    """The exit code and the output of part `name`; the code is None when the part left none."""
    d = root / f"{prefix}{name}"
    try:
        code = int((d / "exit-code.txt").read_text().strip())
    except (OSError, ValueError):
        code = None
    try:
        text = (d / "part.txt").read_text(errors="replace")
    except OSError:
        text = ""
    return code, text


def verdict(names: list[str], root: Path, prefix: str, today: dt.date) -> dict:
    """The night: every part the workflow defines graded, red when any part is NEW or LOST."""
    baseline = known_red.nightly_classes()
    graded = {n: grade_part(*read_part(root, prefix, n), baseline, today) for n in names}
    return {"parts": graded, "verdict": "red" if any(g["verdict"] in (NEW, LOST) for g in graded.values()) else "green"}


def devbox_line() -> str:
    """The stages this night does not grade and where they are graded."""
    n = sum(h == DEVBOX for h in STAGE_HALF.values())
    return (f"- not graded by this night: the {n} devbox-half stages (dev/release_oracle/halves.py), graded on the devbox by "
            "`make release-oracle-full ARGS=\"--half devbox\"` and read at the tag by `release.py --halves`; the hosted live modules by Gate hosted half")


def render(night: dict, baseline: dict[str, known_red.KnownRed]) -> str:
    """One table for the night: a row per part, then every new class and every acknowledged class with its reason."""
    rows = ["| part | exit | verdict | known classes | new |", "|---|---|---|---|---|"]
    for name, g in night["parts"].items():
        rows.append(f"| {name} | {g['exit'] if g['exit'] is not None else 'none'} | {g['verdict']} | {len(g['known'])} | {len(g['new'])} |")
    out = [f"### Nightly delta: {night['verdict']}", *rows, devbox_line()]
    for name, g in night["parts"].items():
        out += [f"- NEW {name}: {n}" for n in g["new"][:20]]
    seen = sorted({k for g in night["parts"].values() for k in g["known"]})
    out += [f"- known {k}: {baseline[k].reason[:160]} (until {baseline[k].expires})" for k in seen]
    return "\n".join(out)


def self_test() -> int:
    """Each rule of the verdict goes RED on the mutant it exists to catch."""
    today = dt.date(2026, 11, 14)
    held_by = lambda match: known_red.KnownRed(match, "2026-11-14: x", "2026-12-31")  # noqa: E731
    base = known_red.nightly_classes(tuple(held_by(m) for m in (
        "no_silent_null[mysql] full: CHECK_RUN_SPLIT over 3 source->override pairs #ab",
        "no_silent_null_load[postgres]: CHECK_RUN_SPLIT over 3 source->override pairs #ab",
        "no_silent_null_cdc[oracle]: CHECK_RUN_SPLIT over 3 source->override pairs #ab",
        known_red.nightly_key("refusal_names_true_cause", "cmd", "printed"))))
    known_line = "no_silent_null mysql/ts->date/full/parquet RED CHECK_RUN_SPLIT: check exit 0, run exit 5\n"
    other_line = "no_silent_null mysql/ts->date/full/parquet RED BRAND_NEW_CAUSE: x\n"
    refusal = "refusal_names_true_cause cmd/load/X/postgres RED printed RIVET_A, injected RIVET_B\n"
    assert known_red.nightly_key("no_silent_null", "mysql", "CHECK_RUN_SPLIT") in base, "the gate's per-cell-set entries are cut to classes"
    assert known_red.nightly_key("refusal_names_true_cause", "cmd", "printed") in base, "a nightly entry is in the baseline"
    load_line = "no_silent_null_load postgres/ts->date/full/parquet RED CHECK_RUN_SPLIT: x\n"
    cdc_line = "no_silent_null_cdc oracle/ts->date/cdc-x/parquet RED CHECK_RUN_SPLIT: check exit 0, run exit 5\n"
    for key in ("nightly[no_silent_null_load|postgres] CHECK_RUN_SPLIT", "nightly[no_silent_null_cdc|oracle] CHECK_RUN_SPLIT"):
        assert key in base, f"the tierless load and cdc entries are cut to classes: {key}"
    g = lambda code, text, when=today: grade_part(code, text, base, when)  # noqa: E731
    assert g(0, "") ["verdict"] == GREEN
    assert g(1, known_line + refusal)["verdict"] == KNOWN, "acknowledged classes are not a red night"
    assert g(1, load_line + cdc_line)["verdict"] == KNOWN, "the load and cdc lanes' acknowledged classes are not a red night"
    assert g(1, known_line + other_line)["verdict"] == NEW, "an unknown class is NEW"
    assert g(2, "")["verdict"] == NEW, "an exit no class explains is NEW, never green"
    assert g(1, known_line + "Traceback (most recent call last)\n")["verdict"] == NEW, "a crash beside acknowledged reds is NEW"
    assert g(1, known_line + "error[E0432]: unresolved import\n")["verdict"] == NEW, "a compile error beside acknowledged reds is NEW"
    assert g(1, known_line + "error: could not compile `rivet` (lib) due to 1 previous error\n")["verdict"] == NEW, "cargo's own failure line is a compile error"
    clap = "── init --table t --clickhouse-url u -o c.yaml (exit 2) ──\nerror: unexpected argument '--clickhouse-url' found\n"
    assert g(1, known_line + clap)["verdict"] == KNOWN, "a child's refusal the harness echoes into the log is no compile error"
    assert g(0, known_line)["verdict"] == NEW, "red cells under exit 0 are an inconsistent part, NEW"
    assert g(143, known_line)["verdict"] == LOST and g(None, "")["verdict"] == LOST, "a killed or absent part is LOST"
    assert g(1, known_line, dt.date(2027, 1, 1))["verdict"] == NEW, "an expired acknowledgement blocks again"
    nextest = "        FAIL [   1.2s] (3/9) rivet-cli::live_suite live_x::a_test_mysql\n"
    assert g(100, nextest)["new"] == ["nightly[test|live_x::a_test_mysql] FAIL"], g(100, nextest)
    assert g(100, "     TIMEOUT [1200.0s] (9/9) rivet-cli::live_suite m::slow\n")["verdict"] == NEW, "a timed-out test is NEW"
    assert g(101, "test m::t ... FAILED\n")["verdict"] == NEW, "a libtest failure is NEW"
    assert g(2, "  oracle     -      no_silent_null   -        FAIL oracle/NUMBER->float4\n")["new"] == ["nightly[gate|oracle/no_silent_null] FAIL"]
    assert g(2, "  oracle     -      no_silent_null   -        PASS ok\n⊘ KNOWN RED (until 2026-11-22: x)\n")["new"] == ["exit 2: no red class explains it"], \
        "KNOWN rows of the gate's own table are not red classes"
    live = lambda cell: f"  live       batch  -                -        FAIL live modules FAILED · {cell} — [2026-11-16T00:13:32Z WARN rivet::config::source] inline url: plaintext password (see log)\n"  # noqa: E731
    assert red_keys(live("m::a") + live("m::b"))[0] == {"m::a", "m::b"}, "two live cells are two red classes, whatever warning heads their first line"
    assert red_keys("  ✗ audit suspects FAILED · m::a — boom (see log)\n" + live("m::a"))[0] == {"m::a"}, "one cell on its two lines is one class"
    held = {"m::a": known_red.KnownRed("m::a — boom", "2026-11-16: why", "2026-11-22")}
    assert grade_part(1, live("m::a"), held, today)["verdict"] == KNOWN, "a live cell the ledger holds is KNOWN"
    assert grade_part(1, live("m::a") + live("m::b"), held, today)["new"] == ["m::b"], "a live cell the ledger does not hold is NEW by its own name"
    names = parts_of(ROOT / ".github" / "workflows" / "nightly-gate.yml", "part")
    assert "no-silent-null" in names and len(names) == len(set(names)), names
    with tempfile.TemporaryDirectory() as t:
        root = Path(t)
        for n in names[:-1]:
            (root / f"gate-part-{n}").mkdir()
            (root / f"gate-part-{n}" / "exit-code.txt").write_text("0\n")
        night = verdict(names, root, "gate-part-", today)
        assert night["verdict"] == "red" and night["parts"][names[-1]]["verdict"] == LOST, "a part with no artifact is LOST, not green"
        (root / f"gate-part-{names[-1]}").mkdir()
        (root / f"gate-part-{names[-1]}" / "exit-code.txt").write_text("0\n")
        assert verdict(names, root, "gate-part-", today)["verdict"] == "green"
    wf = (ROOT / ".github" / "workflows" / "nightly-gate.yml").read_text()
    assert names == re.findall(r"- \{ name: ([\w-]+),", wf), "every part the workflow's matrix defines is graded, in its order"
    assert "nightly_delta.py" in wf, "the nightly gate verdict job must call the delta verdict"
    table = render({"parts": {}, "verdict": "green"}, base)
    assert f"the {sum(h == DEVBOX for h in STAGE_HALF.values())} devbox-half stages" in table and "--half devbox" in table, "the table does not say where the devbox half is graded"
    print(f"self-test ok: {len(names)} parts read from the workflow; acknowledged, new, lost, unexplained, crashed and expired each decide")
    return 0


def main(argv: list[str]) -> int:
    """Print the night's table; exit 1 when it is red."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--workflow", type=Path)
    ap.add_argument("--job")
    ap.add_argument("--parts", type=Path)
    ap.add_argument("--prefix", default="gate-part-")
    ap.add_argument("--json", type=Path)
    ap.add_argument("--today", default=dt.datetime.now(dt.timezone.utc).date().isoformat())
    ns = ap.parse_args(argv)
    if ns.self_test:
        return self_test()
    if not (ns.workflow and ns.job and ns.parts):
        ap.error("--workflow, --job and --parts are required")
    night = verdict(parts_of(ns.workflow, ns.job), ns.parts, ns.prefix, dt.date.fromisoformat(ns.today))
    print(render(night, known_red.nightly_classes()))
    if ns.json:
        ns.json.write_text(json.dumps(night, indent=1, sort_keys=True) + "\n")
    return 1 if night["verdict"] == "red" else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
