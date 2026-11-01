#!/usr/bin/env python3
"""Run the `--self-test` of every gate script the tree holds, found rather than listed.

    gate_self_tests.py            run each, print one verdict line per script, exit 1 if any failed
    gate_self_tests.py --list     print the scripts that would run
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HANDLES = re.compile(r"""["']--self-test["']|--self-test\)""")
TIMEOUT_S = 300


def discovered() -> list[tuple[str, list[str]]]:
    """`(path, argv)` for every script under the gate directories that handles `--self-test`."""
    found = []
    for rel in (".github/scripts", ".githooks", "dev/pytools"):
        for path in sorted((ROOT / rel).iterdir()):
            if not path.is_file() or path.suffix not in ("", ".py") or path.name == Path(__file__).name:
                continue
            if not HANDLES.search(path.read_text(errors="replace")):
                continue
            name = path.relative_to(ROOT).as_posix()
            if rel == "dev/pytools":
                argv = [sys.executable, "-m", f"dev.pytools.{path.stem}", "--self-test"]
            elif path.suffix == ".py":
                argv = [sys.executable, name, "--self-test"]
            else:
                argv = [name, "--self-test"]
            found.append((name, argv))
    return found


def main(argv: list[str]) -> int:
    """List or run the discovered self-tests."""
    scripts = discovered()
    if not scripts:
        print("no script under the gate directories handles --self-test", file=sys.stderr)
        return 1
    if argv == ["--list"]:
        print("\n".join(name for name, _ in scripts))
        return 0
    failed = []
    for name, cmd in scripts:
        try:
            p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=TIMEOUT_S)
            code, out = p.returncode, p.stdout + p.stderr
        except subprocess.TimeoutExpired as e:
            code, out = 124, f"timed out after {TIMEOUT_S}s\n{e.stdout or ''}"
        print(f"{'ok  ' if code == 0 else 'FAIL'} {name}", flush=True)
        if code != 0:
            failed.append(name)
            print(out[-2000:], file=sys.stderr)
    print(f"{len(scripts) - len(failed)}/{len(scripts)} self-tests passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
