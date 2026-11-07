#!/usr/bin/env python3
"""The merges a red nightly run covers: every first-parent commit after the last green nightly's commit, up to this run's.

    nightly_range.py --green SHA|"" --head SHA   print a markdown list of the merged PRs in (green, head]
    nightly_range.py --self-test
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

#: How many merges to list when no green nightly is on record.
NO_GREEN_WINDOW = 30
PR_NUMBER = re.compile(r"\(#(\d+)\)\s*$")


def merges(green: str, head: str, repo: Path | None = None) -> list[str]:
    """The first-parent subjects in `(green, head]`, newest first; the last `NO_GREEN_WINDOW` when `green` is empty."""
    cmd = ["git", "log", "--first-parent", "--format=%h %s"]
    cmd += [f"{green}..{head}"] if green else [f"-{NO_GREEN_WINDOW}", head]
    return subprocess.run(cmd, cwd=repo, capture_output=True, text=True, check=True).stdout.splitlines()


def report(green: str, head: str, repo: Path | None = None) -> str:
    """The markdown the verdict job prints: the range, then one line per merge naming its PR number."""
    lines = merges(green, head, repo)
    span = f"`{green[:9]}..{head[:9]}`" if green else f"the last {NO_GREEN_WINDOW} merges (no green nightly on record)"
    out = [f"Nightly red since {span}: {len(lines)} merge(s) to look at"]
    for line in lines:
        sha, subject = line.split(" ", 1)
        m = PR_NUMBER.search(subject)
        out.append(f"- {'#' + m.group(1) if m else sha} {subject}")
    return "\n".join(out)


def self_test() -> int:
    """The range is exclusive of the green commit, inclusive of the head, first-parent, and bounded when no green exists."""
    with tempfile.TemporaryDirectory() as d:
        repo = Path(d)
        git = lambda *a: subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *a], cwd=repo, check=True,
                                        capture_output=True, text=True).stdout.strip()
        git("init", "-q", "-b", "main")
        shas = []
        for n in range(1, 5):
            git("commit", "-q", "--allow-empty", "-m", f"change {n} (#{n})")
            shas.append(git("rev-parse", "HEAD"))
        out = report(shas[1], shas[3], repo)
        assert out.splitlines()[0].endswith("2 merge(s) to look at"), out
        assert "#3 " in out and "#4 " in out and "#2 " not in out and "#1 " not in out, out
        assert report(shas[3], shas[3], repo).splitlines()[0].endswith("0 merge(s) to look at")
        every = report("", shas[3], repo)
        assert "no green nightly on record" in every and "#1 " in every and "#4 " in every, every
        base = git("rev-parse", "HEAD")
        git("checkout", "-q", "-b", "side")
        git("commit", "-q", "--allow-empty", "-m", "side commit (#90)")
        git("checkout", "-q", "main")
        git("merge", "-q", "--no-ff", "-m", "merge side (#91)", "side")
        out = report(base, git("rev-parse", "HEAD"), repo)
        assert "#91 " in out and "#90 " not in out, out
        git("commit", "-q", "--allow-empty", "-m", "no pr number here")
        assert f"- {git('rev-parse', '--short', 'HEAD')} " in report("", git("rev-parse", "HEAD"), repo)
    print("self-test ok: nightly range lists the merges after the green commit, newest first, by PR number")
    return 0


def main(argv: list[str]) -> int:
    """Print the report for `--green` and `--head`."""
    if argv == ["--self-test"]:
        return self_test()
    if len(argv) != 4 or argv[0] != "--green" or argv[2] != "--head":
        print(__doc__, file=sys.stderr)
        return 2
    print(report(argv[1], argv[3]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
