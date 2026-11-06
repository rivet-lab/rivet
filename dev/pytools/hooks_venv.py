#!/usr/bin/env python3
"""The git hooks and `make pr-ready` run python3 and cargo from this checkout's .venv, found rather than listed.

    hooks_venv.py --self-test

Every file in .githooks that names python3 or cargo is run in a fixture checkout whose PATH holds a decoy python3 and cargo.
The decoys trace what `python3` resolves to and fail, so the first call of each hook shows which interpreter it would use.
"""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
USES = re.compile(r"\b(python3|cargo)\b")
SYSTEM_PY = "SYSTEM-PYTHON"
VENV_PY = "VENV-PYTHON"
MAKE = "make pr-ready"


def script(path: Path, body: str) -> None:
    """Write an executable shell script."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def hooks() -> list[Path]:
    """Every executable file under .githooks that names python3 or cargo."""
    found = [p for p in sorted((ROOT / ".githooks").iterdir())
             if p.is_file() and os.access(p, os.X_OK) and USES.search(p.read_text(errors="replace"))]
    if not found:
        raise SystemExit("hooks_venv: no executable file under .githooks names python3 or cargo: nothing graded")
    return found


def checkout(base: Path, name: str, venv: bool) -> Path:
    """A git work tree holding a copy of the repo's hooks and Makefile, with or without a venv."""
    top = base / name
    top.mkdir()
    subprocess.run(["git", "init", "-q", str(top)], check=True, env={"PATH": os.environ["PATH"], "HOME": str(base)})
    shutil.copytree(ROOT / ".githooks", top / ".githooks")
    shutil.copy(ROOT / "Makefile", top / "Makefile")
    if venv:
        script(top / ".venv/bin/python3", f'echo {VENV_PY} "$0" >> "$TRACE"\nexit 1\n')
    return top


def run(cmd: list[str], cwd: Path, bin_dir: Path, trace: Path) -> subprocess.CompletedProcess:
    """Run `cmd` with the decoy bin dir first on PATH and TRACE naming the trace file."""
    trace.unlink(missing_ok=True)
    env = {"PATH": f"{bin_dir}:/usr/bin:/bin", "TRACE": str(trace), "HOME": str(cwd)}
    return subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True, timeout=60)


def seen(trace: Path) -> str:
    """The text the decoys traced."""
    return trace.read_text() if trace.exists() else ""


def invocation(name: str, hooks_dir: Path, msg: Path) -> list[str]:
    """The command line that runs `name`: a hook with a message file, or the make target."""
    return ["make", "pr-ready"] if name == MAKE else [str(hooks_dir / name), str(msg)]


def self_test() -> int:
    """Grade every hook and the pr-ready recipe: the work tree's venv python first, a refusal naming the remedy without one."""
    bad: list[str] = []
    names = [p.name for p in hooks()] + [MAKE]
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp).resolve()
        trace = base / "trace"
        decoys = base / "decoys"
        script(decoys / "python3", f'echo {SYSTEM_PY} "$0" >> "$TRACE"\nexit 1\n')
        script(decoys / "cargo", 'echo "cargo-sees $(command -v python3)" >> "$TRACE"\nexit 1\n')
        msg = base / "msg"
        msg.write_text("feat: x\n")
        home, other, bare = (checkout(base, n, v) for n, v in (("home", True), ("other", True), ("bare", False)))
        for name in names:
            cases = [("own hooks", home, home / ".githooks", home)]
            if name != MAKE:
                cases.append(("hooks from another checkout", other, home / ".githooks", other))
            for label, cwd, hooks_dir, owner in cases:
                run(invocation(name, hooks_dir, msg), cwd, decoys, trace)
                got, want = seen(trace), str(owner / ".venv/bin/python3")
                if want not in got or SYSTEM_PY in got:
                    bad.append(f"{name} ({label}): the first python3 was not {want}: {got.strip() or 'nothing ran'}")
            p = run(invocation(name, bare / ".githooks", msg), bare, decoys, trace)
            if p.returncode == 0 or seen(trace) or "REFUSING" not in p.stderr or "uv sync --frozen --python 3.12" not in p.stderr:
                bad.append(f"{name} (no venv): want a refusal naming `uv sync --frozen --python 3.12` before any python3 or cargo ran; "
                           f"exit {p.returncode}, traced {seen(trace).strip() or 'nothing'}, stderr {p.stderr.strip()[:200]!r}")
    if bad:
        print("hooks_venv self-test FAILED:\n  " + "\n  ".join(bad), file=sys.stderr)
        return 1
    print(f"hooks_venv self-test: ok ({len(names)} entries: {', '.join(names)}; venv python first, also from another checkout's hooks, a refusal without a venv)")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if sys.argv[1:] == ["--self-test"] else sys.exit(__doc__))
