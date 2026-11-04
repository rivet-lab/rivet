#!/usr/bin/env python3
"""The pinned lint tools of make pr-ready and the nightly: one table, one runner, one verdict shape.

    lint_tools.py run NAME     run one tool over the repo; exit 0 PASS, 1 FAIL, 2 NOT RUN
    lint_tools.py --self-test

A tool that is absent or is not the pinned version is NOT RUN, never PASS. Adding a tool is one TOOLS entry:
its `bad` fixture must fail and its `good` fixture must pass, and --self-test grades every entry that way.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPO = "@repo"


@dataclass(frozen=True)
class Tool:
    """A linter: executable, pinned version, repo-wide argv, the tier that runs it and its two self-test fixtures."""

    exe: str
    version: str
    argv: tuple[str, ...]
    install: str
    tier: str
    good: dict[str, str]
    bad: dict[str, str]
    needle: str


AGENT = ".claude/agents/tidy-agent.md"
AGENT_OK = "---\nname: tidy-agent\ndescription: Tidies a file when asked.\ntools: Read, Edit\nmodel: sonnet\n---\nTidy the file named in the task.\n"
AGENT_BROKEN = "---\ndescription: Tidies a file when asked.\ntools: Read, Edit\nTidy the file named in the task.\n"

TOOLS = {
    "agnix": Tool(
        "agnix",
        "0.59.0",
        ("--strict", "."),
        "the release v0.59.0 of agent-sh/agnix, or cargo install agnix-cli --version 0.59.0",
        "pr",
        {AGENT: AGENT_OK},
        {AGENT: AGENT_BROKEN},
        "no 'name' field",
    ),
}


def tools(tier: str) -> list[str]:
    """The names of the tools that run in `tier` ("pr" or "nightly")."""
    return [name for name, tool in TOOLS.items() if tool.tier == tier]


def installed(tool: Tool) -> str | None:
    """The version `tool.exe` reports, or None when it is not on PATH."""
    exe = shutil.which(tool.exe)
    if not exe:
        return None
    out = subprocess.run([exe, "--version"], capture_output=True, text=True).stdout
    m = re.search(r"\d+\.\d+\.\d+", out)
    return m.group(0) if m else "unknown"


def run(name: str, root: Path = ROOT) -> tuple[str, str]:
    """Run one tool over `root`: (PASS | FAIL | NOT RUN, one line)."""
    tool = TOOLS[name]
    have = installed(tool)
    if have != tool.version:
        why = f"{tool.exe} is not installed" if have is None else f"{tool.exe} {have} is installed, the pin is {tool.version}"
        return "NOT RUN", f"{why} ({tool.install})"
    done = subprocess.run([tool.exe, *tool.argv], cwd=root, capture_output=True, text=True)
    if done.returncode == 0:
        return "PASS", f"{tool.exe} {tool.version}: clean"
    lines = [ln for ln in (done.stdout + done.stderr).splitlines() if ln.strip()]
    return "FAIL", f"{tool.exe} {tool.version} exit {done.returncode}: " + "; ".join(lines[:3])[:300]


def fixture(files: dict[str, str]) -> Path:
    """A scratch directory holding `files`; a value of REPO copies that file from the repo."""
    root = Path(tempfile.mkdtemp(prefix="lint-tools-"))
    for rel, text in files.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text((ROOT / rel).read_text() if text == REPO else text)
    return root


def self_test() -> int:
    """Every tool fails on its bad fixture and passes its good one; an absent tool is NOT RUN, never PASS."""
    for name, tool in TOOLS.items():
        assert tool.tier in ("pr", "nightly"), f"{name}: tier {tool.tier}"
        TOOLS[name] = Tool("lint-tool-that-does-not-exist", tool.version, (), "n/a", tool.tier, {}, {}, "")
        try:
            assert run(name)[0] == "NOT RUN", f"{name}: an absent tool must not read as PASS"
        finally:
            TOOLS[name] = tool
    graded = []
    for name, tool in TOOLS.items():
        if installed(tool) != tool.version:
            print(f"lint_tools self-test: {name} fixtures NOT RUN ({installed(tool) or 'absent'}, pin {tool.version})")
            continue
        good, bad = fixture(tool.good), fixture(tool.bad)
        try:
            assert run(name, good)[0] == "PASS", (name, run(name, good))
            verdict, line = run(name, bad)
            assert verdict == "FAIL" and tool.needle in line, (name, verdict, line)
        finally:
            shutil.rmtree(good, ignore_errors=True)
            shutil.rmtree(bad, ignore_errors=True)
        graded.append(name)
    print(f"lint_tools self-test: ok ({len(TOOLS)} tools table-checked; fixtures graded: {', '.join(graded) or 'none'})")
    return 0


def main(argv: list[str]) -> int:
    """Dispatch `run NAME` and `--self-test`."""
    if argv == ["--self-test"]:
        return self_test()
    if len(argv) == 2 and argv[0] == "run" and argv[1] in TOOLS:
        verdict, line = run(argv[1])
        print(f"{verdict}: {line}", file=sys.stdout if verdict == "PASS" else sys.stderr)
        return {"PASS": 0, "FAIL": 1}.get(verdict, 2)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
