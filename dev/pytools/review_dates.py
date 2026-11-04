#!/usr/bin/env python3
"""Review dates on accepted reds: every ledger entry says when it is looked at again, and a passed date fails.

    review_dates.py check [--no-expiry]   grade the ledgers; --no-expiry grades the shape only (CI)
    review_dates.py --self-test

Places read (a fixed list, not a repo walk): every `ratchet-pin:` marker line (skip_census ceilings, the
rig-oracle ratchets and the XFAIL / known-defect lists carry one), `KNOWN_RED[*].expires`
(dev/release_oracle/known_red.py, the date its gate already enforces) and `SKIP_ALLOWED`
(dev/release_oracle/core.py). A marker line and a SKIP_ALLOWED reason carry `(review YYYY-MM-DD)`.
"""

from __future__ import annotations

import ast
import datetime as dt
import re
import subprocess
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MARKER = r"ratchet-pin:\s*([\w.-]+)"
REVIEW = re.compile(r"\(review (\d{4}-\d{2}-\d{2})\)")
LOOSE = re.compile(r"\(review\b", re.I)
MIN_WORDS = 3
NOT_ENTRIES = ("dev/pytools/pr_ready.py", "dev/pytools/review_dates.py")


def date_problem(raw: str, today: dt.date, expiry: bool = True) -> str | None:
    """Why `raw` is no usable review date (not a calendar day, or already passed), else None."""
    try:
        day = dt.datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return f"{raw!r} is not a calendar date"
    if day.strftime("%Y-%m-%d") != raw:
        return f"{raw!r} is not a calendar date in YYYY-MM-DD"
    if expiry and day < today:
        return f"review date {raw} has passed"
    return None


def text_problem(text: str, today: dt.date, expiry: bool) -> str | None:
    """Why `text` has no usable `(review YYYY-MM-DD)`: absent, malformed, or a bad date."""
    found = REVIEW.findall(text)
    if len(found) > 1:
        return "more than one `(review YYYY-MM-DD)`"
    if not found:
        return "malformed `(review ...)`, want `(review YYYY-MM-DD)`" if LOOSE.search(text) else "no `(review YYYY-MM-DD)`"
    return date_problem(found[0], today, expiry)


def words(reason: str) -> int:
    """Words of a reason once its review date is taken out."""
    return len(REVIEW.sub("", reason).split())


def marker_problems(lines: list[tuple[str, int, str]], today: dt.date, expiry: bool) -> list[str]:
    """Problems of the (path, line, text) rows that carry a `ratchet-pin:` marker; `end` closers are not entries."""
    out = []
    for path, n, text in lines:
        m = re.search(MARKER, text)
        if not m or m.group(1) == "end":
            continue
        why = text_problem(text[m.end():], today, expiry)
        if why:
            out.append(f"{path}:{n}: ratchet-pin {m.group(1)}: {why}")
    return out


def known_red_problems(entries: list[tuple[str, str, str]], today: dt.date, expiry: bool) -> list[str]:
    """Problems of (match, reason, expires): the expiry is the review date, the reason must say something."""
    out = []
    for match, reason, expires in entries:
        why = date_problem(expires, today, expiry) or (None if words(reason) >= MIN_WORDS else f"reason under {MIN_WORDS} words")
        if why:
            out.append(f"dev/release_oracle/known_red.py: {match[:70]}: {why}")
    return out


def skip_allowed_problems(allowed: dict[str, str], today: dt.date, expiry: bool) -> list[str]:
    """Problems of the SKIP_ALLOWED reasons: each carries a review date and more than a word or two."""
    out = []
    for cell, reason in allowed.items():
        why = text_problem(reason, today, expiry) or (None if words(reason) >= MIN_WORDS else f"reason under {MIN_WORDS} words")
        if why:
            out.append(f"dev/release_oracle/core.py: SKIP_ALLOWED {cell}: {why}")
    return out


def pin_rows(root: Path) -> list[tuple[str, int, str]]:
    """(path, line, text) of every tracked line with a `ratchet-pin:` marker, bar the files that only name it."""
    git = "/usr/bin/git" if Path("/usr/bin/git").exists() else "git"
    hits = subprocess.run([git, "grep", "-n", "-I", "-e", "ratchet-pin:"], cwd=root, capture_output=True, text=True).stdout
    rows = []
    for hit in hits.splitlines():
        path, n, text = hit.split(":", 2)
        if path not in NOT_ENTRIES:
            rows.append((path, int(n), text))
    return rows


def known_red_entries(root: Path) -> list[tuple[str, str, str]]:
    """(match, reason, expires) of the real ledger, executed from its source so no stale bytecode answers."""
    path = root / "dev/release_oracle/known_red.py"
    mod = types.ModuleType("review_known_red")
    sys.modules[mod.__name__] = mod
    exec(compile(path.read_text(), str(path), "exec"), mod.__dict__)
    return [(k.match, k.reason, k.expires) for k in mod.KNOWN_RED]


def skip_allowed(root: Path) -> dict[str, str]:
    """The SKIP_ALLOWED literal of core.py, read from the source."""
    tree = ast.parse((root / "dev/release_oracle/core.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", "") == "SKIP_ALLOWED":
            return ast.literal_eval(node.value)
    raise SystemExit("review_dates: SKIP_ALLOWED moved out of dev/release_oracle/core.py")


def tree_problems(root: Path, today: dt.date, expiry: bool) -> tuple[list[str], str]:
    """Every problem in the ledgers of `root` and a one-line count."""
    rows, red, skips = pin_rows(root), known_red_entries(root), skip_allowed(root)
    pins = sum(1 for _, _, t in rows if (m := re.search(MARKER, t)) and m.group(1) != "end")
    bad = marker_problems(rows, today, expiry) + known_red_problems(red, today, expiry) + skip_allowed_problems(skips, today, expiry)
    return bad, f"{pins} ratchet pins, {len(red)} known-red entries, {len(skips)} allowed skips"


def check(argv: list[str]) -> int:
    """Print the problems and the count; 1 when any."""
    bad, counted = tree_problems(ROOT, dt.date.today(), "--no-expiry" not in argv)
    for b in bad:
        print(f"REVIEW DATE: {b}", file=sys.stderr)
    print(f"review dates: {counted}; {len(bad)} problem(s)")
    return 1 if bad else 0


def self_test() -> int:
    """Each rule goes RED on the entry it exists for and stays green on its near miss."""
    today = dt.date(2026, 11, 4)
    for bad in ("2026-02-31", "2026-13-01", "2026-1-5", "20261105", "2026-11-5", "abcd-ef-gh"):
        assert date_problem(bad, today), f"{bad} must fail"
    assert date_problem("2026-11-03", today) == "review date 2026-11-03 has passed"
    assert date_problem("2026-11-03", today, expiry=False) is None, "the shape check does not read the clock"
    assert date_problem("2026-11-04", today) is None and date_problem("2027-02-01", today) is None
    assert date_problem("2028-02-29", today) is None and date_problem("2027-02-29", today)
    assert "no `(review" in text_problem(" min", today, True)
    assert "malformed" in text_problem(" (review 2027-2-1)", today, True) and "malformed" in text_problem(" (review soon)", today, True)
    assert "more than one" in text_problem(" (review 2027-02-01) (review 2027-03-01)", today, True)
    assert "not a calendar" in text_problem(" (review 2026-02-31)", today, True)
    assert "passed" in text_problem(" (review 2026-01-01)", today, True) and text_problem(" (review 2027-02-01)", today, True) is None
    rows = [
        ("a.rs", 1, "const X: usize = 3; // ratchet-pin: x (review 2027-02-01)"),
        ("a.rs", 2, "const Y: usize = 3; // ratchet-pin: y"),
        ("a.rs", 3, "]; // ratchet-pin: end"),
        ("a.rs", 4, "const Z: usize = 3; // ratchet-pin: z (review 2026-02-31)"),
        ("a.rs", 5, "const W: usize = 3; // ratchet-pin: w (review 2026-01-01)"),
    ]
    got = marker_problems(rows, today, True)
    assert [g.split(": ratchet-pin ")[1][0] for g in got] == ["y", "z", "w"], got
    assert len(marker_problems(rows, today, False)) == 2, "without expiry only the shape fails"
    red = [("cell — line", "a reason of words", "2027-02-01"), ("c2 — l", "two words", "2027-02-01"), ("c3 — l", "fine reason here", "2026-11-01")]
    assert [p.split(": ")[1][:2] for p in known_red_problems(red, today, True)] == ["c2", "c3"], known_red_problems(red, today, True)
    allowed = {"m::ok": "a fixture generator (review 2027-02-01)", "m::none": "needs a stand", "m::short": "no (review 2027-02-01)"}
    got = skip_allowed_problems(allowed, today, True)
    assert len(got) == 2 and "m::none: no `(review" in got[0] and "m::short: reason under" in got[1], got
    bad, counted = tree_problems(ROOT, dt.date(2000, 1, 1), expiry=True)
    assert not bad, f"the real ledgers carry a malformed or missing review date: {bad[:3]}"
    assert counted.split()[0].isdigit() and int(counted.split()[0]) > 20, "pin discovery found almost nothing"
    assert tree_problems(ROOT, dt.date(2100, 1, 1), expiry=True)[0], "every date has passed in 2100"
    print("review_dates self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    """Dispatch `check` and `--self-test`."""
    if argv == ["--self-test"]:
        return self_test()
    if argv[:1] == ["check"]:
        return check(argv[1:])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
