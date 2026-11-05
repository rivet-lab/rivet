#!/usr/bin/env python3
"""The pinned tools of make pr-ready, the nightly and CI: one table, one verifier, one installer, one verdict shape.

    lint_tools.py run NAME [--require]   run one tool over the repo; exit 0 PASS, 1 FAIL, 2 NOT RUN
    lint_tools.py tier TIER...           run every tool of those tiers as under CI (NOT RUN fails); exit 0 only if all PASS
    lint_tools.py install DIR [NAME...]  download the pinned release of each tool, check its sha256, put it in DIR
    lint_tools.py --self-test [--require]   --require: a fixture that cannot run is a failure

A tool is the binary whose sha256 the table pins for this platform; a version string is never the evidence. An absent or
different binary is NOT RUN, never PASS, and a FAIL under --require or CI (the CI job installs the pins itself, so there
a missing pin is a broken job). Adding a tool is one TOOLS entry: its `good` fixture must pass and every `bad` fixture
must fail, and --self-test grades every entry that way.
"""

from __future__ import annotations

import hashlib
import io
import os
import platform
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPO = "@repo"
TIERS = ("pr", "nightly", "pin")
INSTALL = "python3 dev/pytools/lint_tools.py install DIR, with DIR on PATH"


@dataclass(frozen=True)
class Tool:
    """A pinned tool: executable, version, per-platform (url, sha256 of the binary), repo-wide argv, tier, fixtures, timeout."""

    exe: str
    version: str
    pins: dict[str, tuple[str, str]]
    argv: tuple[str, ...] = ()
    tier: str = "pr"
    good: dict[str, str] = field(default_factory=dict)
    bad: tuple[tuple[dict[str, str], str], ...] = ()
    timeout: int = 300


MISSPELT = "rec" + "ieves"
WIDTH = "wid" + "ht"
FRAGMENTS = "t" + "hs"
CHECKPT = "checkp" + "ont"

TOOLS = {
    "typos": Tool(
        "typos",
        "1.51.1",
        {
            "darwin-arm64": (
                "https://github.com/crate-ci/typos/releases/download/v{v}/typos-v{v}-aarch64-apple-darwin.tar.gz",
                "4bbd6348235106ad321de4481e1201f2641189d880f9fa15a2a5da3849f27b12",
            ),
            "linux-x86_64": (
                "https://github.com/crate-ci/typos/releases/download/v{v}/typos-v{v}-x86_64-unknown-linux-musl.tar.gz",
                "4f9c222875860b0fb3651cb76c8b10b0bd6513d486db7608e8f5ace7fba7757a",
            ),
        },
        ("--config", "_typos.toml", "--format", "brief"),
        "pr",
        {"_typos.toml": REPO, "a.md": "A recipient receives the message.\n", "src/a.rs": "let column_width = 1;\n"},
        (
            ({"_typos.toml": REPO, "a.md": f"A recipient {MISSPELT} the message.\n"}, MISSPELT),
            ({"_typos.toml": REPO, "src/a.rs": f"let column_{WIDTH} = 1;\n"}, WIDTH),
            ({"_typos.toml": REPO, "docs/deep/a.md": f"A recipient {MISSPELT} the message.\n"}, MISSPELT),
            ({"_typos.toml": REPO, "src/a.rs": f"let my_{CHECKPT} = 1;\n"}, CHECKPT),
            ({"_typos.toml": REPO, "dev/a.py": f"# A recipient {MISSPELT} the message.\n"}, MISSPELT),
            ({"_typos.toml": REPO, "a.md": f"Read {FRAGMENTS} page f" + "o details n" + "d more.\n"}, FRAGMENTS),
        ),
    ),
    "gitleaks": Tool(
        "gitleaks",
        "8.30.1",
        {
            "darwin-arm64": (
                "https://github.com/gitleaks/gitleaks/releases/download/v{v}/gitleaks_{v}_darwin_arm64.tar.gz",
                "ba52fb1bfabbcde42f032afad3d6e0b19dff8ed105229a16e7caa338bbc0e84f",
            ),
            "linux-x86_64": (
                "https://github.com/gitleaks/gitleaks/releases/download/v{v}/gitleaks_{v}_linux_x64.tar.gz",
                "88f91962aa2f93ac6ab281d553b9e125f5197bbbce38f9f2437f7299c32e5509",
            ),
        },
        tier="pin",
    ),
}


def tools(tier: str) -> list[str]:
    """The names of the tools that run in `tier` ("pr" or "nightly")."""
    return [name for name, tool in TOOLS.items() if tool.tier == tier]


def platform_key() -> str:
    """This machine as `system-arch`, the key of a tool's pins."""
    arch = {"aarch64": "arm64", "amd64": "x86_64"}.get(platform.machine().lower(), platform.machine().lower())
    return f"{platform.system().lower()}-{arch}"


def sha256(data: bytes) -> str:
    """The hex sha256 of `data`."""
    return hashlib.sha256(data).hexdigest()


def find(tool: Tool, path: str | None = None) -> tuple[str | None, str]:
    """(the executable on PATH that is the pinned binary, "") or (None, why it cannot be used)."""
    exe = shutil.which(tool.exe, path=path)
    if not exe:
        return None, f"{tool.exe} is not installed"
    pin = tool.pins.get(platform_key())
    if pin is None:
        return None, f"{tool.exe} has no pin for {platform_key()}"
    got = sha256(Path(exe).read_bytes())
    if got != pin[1]:
        return None, f"{exe} is not the pinned {tool.exe} {tool.version} (sha256 {got[:12]}, pin {pin[1][:12]})"
    return exe, ""


def verified(name: str, path: str | None = None) -> tuple[str | None, str]:
    """`find` for a table entry; secrets_scan reads gitleaks through it."""
    return find(TOOLS[name], path)


def in_ci() -> bool:
    """True under CI (GitHub sets CI=true), where NOT RUN must fail."""
    return bool(os.environ.get("CI"))


def execute(tool: Tool, root: Path = ROOT, path: str | None = None, strict: bool | None = None) -> tuple[str, str]:
    """Run one tool over `root`: (PASS | FAIL | NOT RUN, one line)."""
    exe, why = find(tool, path)
    if exe is None:
        if in_ci() if strict is None else strict:
            return "FAIL", f"{why}; NOT RUN is a FAIL in CI ({INSTALL})"
        return "NOT RUN", f"{why} ({INSTALL})"
    try:
        done = subprocess.run([exe, *tool.argv], cwd=root, capture_output=True, text=True, timeout=tool.timeout)
    except subprocess.TimeoutExpired:
        return "FAIL", f"{tool.exe} {tool.version} timed out after {tool.timeout} s"
    if done.returncode == 0:
        return "PASS", f"{tool.exe} {tool.version}: clean"
    lines = [ln for ln in (done.stdout + done.stderr).splitlines() if ln.strip()]
    return "FAIL", f"{tool.exe} {tool.version} exit {done.returncode}: " + "; ".join(lines[:3])[:300]


def run(name: str, root: Path = ROOT, path: str | None = None, strict: bool | None = None) -> tuple[str, str]:
    """`execute` for a table entry."""
    return execute(TOOLS[name], root, path, strict)


def run_tier(tiers: list[str], table: dict[str, Tool], path: str | None = None) -> int:
    """Run every tool of `tiers` strictly; 0 only when each one PASSes."""
    rc = 0
    for name, tool in table.items():
        if tool.tier in tiers:
            verdict, line = execute(tool, path=path, strict=True)
            print(f"{name}: {verdict}: {line}")
            rc |= verdict != "PASS"
    return rc


def release_binary(tool: Tool, archive: bytes) -> bytes:
    """The tool's executable out of a release tar.gz."""
    tar = tarfile.open(fileobj=io.BytesIO(archive))
    for member in tar.getmembers():
        if member.isfile() and member.name.rsplit("/", 1)[-1] == tool.exe:
            return tar.extractfile(member).read()
    raise SystemExit(f"no {tool.exe} in the archive")


def download(url: str) -> bytes:
    """The body of `url`, three attempts."""
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=120) as response:
                return response.read()
        except OSError:
            if attempt == 2:
                raise
    raise AssertionError("unreachable")


def install_one(tool: Tool, archive: bytes, dest: Path) -> int:
    """Write the archive's executable to `dest` only when its sha256 equals this platform's pin."""
    pin = tool.pins[platform_key()]
    binary = release_binary(tool, archive)
    if sha256(binary) != pin[1]:
        print(f"{tool.exe}: the release binary is sha256 {sha256(binary)[:12]}, the pin is {pin[1][:12]}; nothing written", file=sys.stderr)
        return 1
    target = dest / tool.exe
    target.write_bytes(binary)
    target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    print(f"{tool.exe} {tool.version} installed to {target}")
    return 0


def install(names: list[str], dest: Path) -> int:
    """Download each pinned release and install it into `dest` through `install_one`."""
    dest.mkdir(parents=True, exist_ok=True)
    for name in names:
        tool = TOOLS[name]
        pin = tool.pins.get(platform_key())
        if pin is None:
            print(f"{name}: no pin for {platform_key()}", file=sys.stderr)
            return 2
        rc = install_one(tool, download(pin[0].format(v=tool.version)), dest)
        if rc:
            return rc
    return 0


def fixture(files: dict[str, str]) -> Path:
    """A scratch directory holding `files`; a value of REPO copies that file from the repo."""
    root = Path(tempfile.mkdtemp(prefix="lint-tools-"))
    for rel, text in files.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text((ROOT / rel).read_text() if text == REPO else text)
    return root


def fake(body: str) -> tuple[Path, Tool]:
    """A scratch executable running `body` and a table entry that pins its sha256 on this platform."""
    bin_dir = Path(tempfile.mkdtemp(prefix="lint-tools-bin-"))
    exe = bin_dir / "faketool"
    exe.write_text(f"#!/bin/sh\n{body}\n")
    exe.chmod(0o755)
    pins = {platform_key(): ("https://example.invalid/{v}", sha256(exe.read_bytes()))}
    return bin_dir, Tool("faketool", "9.9.9", pins, ("x",), timeout=2)


def self_test(require: bool = False) -> int:
    """The verifier trusts only the pinned bytes, a hang or an absence never reads as PASS, and every entry's fixtures grade."""
    for name, tool in TOOLS.items():
        assert tool.tier in TIERS, f"{name}: tier {tool.tier}"
        assert tool.pins and all(len(sha) == 64 and "{v}" in url for url, sha in tool.pins.values()), f"{name}: pins"
        assert tool.tier == "pin" or (tool.good and tool.bad), f"{name}: fixtures"
        absent = Tool("lint-tool-that-does-not-exist", tool.version, tool.pins, tier=tool.tier)
        assert execute(absent, strict=False)[0] == "NOT RUN", f"{name}: an absent tool must not read as PASS"
        assert execute(absent, strict=True)[0] == "FAIL", f"{name}: NOT RUN must be a FAIL in CI"
    bin_dir, pinned = fake("exit 0")
    liar = Path(tempfile.mkdtemp(prefix="lint-tools-liar-"))
    (liar / "faketool").write_text(f"#!/bin/sh\necho 'faketool {pinned.version}'\nexit 0\n")
    (liar / "faketool").chmod(0o755)
    hang_dir, hang = fake("exec sleep 30")
    fail_dir, failing = fake("echo broken-input >&2; exit 1")
    try:
        assert execute(pinned, path=str(bin_dir), strict=False)[0] == "PASS"
        verdict, line = execute(pinned, path=str(liar), strict=False)
        assert verdict == "NOT RUN" and "not the pinned" in line, f"a binary that only prints the pinned version: {verdict} {line}"
        verdict, line = execute(failing, path=str(fail_dir), strict=False)
        assert verdict == "FAIL" and "broken-input" in line, (verdict, line)
        assert run_tier(["pr"], {"pinned": pinned}, path=str(bin_dir)) == 0
        assert run_tier(["pr"], {"pinned": pinned}, path=str(liar)) == 1, "a tier holding a tool that is not the pin must fail"
        assert run_tier(["pr"], {"failing": failing}, path=str(fail_dir)) == 1
        assert run_tier(["nightly"], {"failing": failing}, path=str(fail_dir)) == 0, "only the named tiers run"
        verdict, line = execute(hang, path=str(hang_dir), strict=False)
        assert verdict == "FAIL" and "timed out" in line, f"a hung tool must fail, not hold the run: {verdict} {line}"
    finally:
        for d in (bin_dir, liar, hang_dir, fail_dir):
            shutil.rmtree(d, ignore_errors=True)
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        body = b"#!/bin/sh\nexit 0\n"
        info = tarfile.TarInfo("faketool-1/faketool")
        info.size = len(body)
        tar.addfile(info, io.BytesIO(body))
    dest = Path(tempfile.mkdtemp(prefix="lint-tools-dest-"))
    try:
        right = Tool("faketool", "9.9.9", {platform_key(): ("u", sha256(body))})
        wrong = Tool("faketool", "9.9.9", {platform_key(): ("u", sha256(body + b"x"))})
        assert install_one(wrong, archive.getvalue(), dest) == 1 and not list(dest.iterdir()), "a binary that is not the pin must not be installed"
        assert install_one(right, archive.getvalue(), dest) == 0 and (dest / "faketool").stat().st_mode & stat.S_IXUSR
    finally:
        shutil.rmtree(dest, ignore_errors=True)
    graded, skipped = [], []
    for name, tool in TOOLS.items():
        if tool.tier == "pin":
            continue
        exe, why = find(tool)
        if exe is None:
            print(f"lint_tools self-test: {name} fixtures NOT RUN ({why})")
            skipped.append(name)
            continue
        good = fixture(tool.good)
        try:
            assert execute(tool, good)[0] == "PASS", (name, execute(tool, good))
        finally:
            shutil.rmtree(good, ignore_errors=True)
        for files, needle in tool.bad:
            bad = fixture(files)
            try:
                verdict, line = execute(tool, bad)
                assert verdict == "FAIL" and needle in line, (name, sorted(files), verdict, line)
            finally:
                shutil.rmtree(bad, ignore_errors=True)
        graded.append(name)
    print(f"lint_tools self-test: ok ({len(TOOLS)} tools table-checked; fixtures graded: {', '.join(graded) or 'none'})")
    return 1 if require and skipped else 0


def main(argv: list[str]) -> int:
    """Dispatch `run NAME [--require]`, `install DIR [NAME...]` and `--self-test`."""
    if argv in (["--self-test"], ["--self-test", "--require"]):
        return self_test(require=len(argv) == 2)
    if argv[:1] == ["run"] and len(argv) in (2, 3) and argv[1] in TOOLS and argv[2:] in ([], ["--require"]):
        verdict, line = run(argv[1], strict=True if len(argv) == 3 else None)
        print(f"{verdict}: {line}", file=sys.stdout if verdict == "PASS" else sys.stderr)
        return {"PASS": 0, "FAIL": 1}.get(verdict, 2)
    if argv[:1] == ["tier"] and len(argv) >= 2 and all(a in TIERS for a in argv[1:]):
        return run_tier(argv[1:], TOOLS)
    if argv[:1] == ["install"] and len(argv) >= 2 and all(n in TOOLS for n in argv[2:]):
        return install(argv[2:] or list(TOOLS), Path(argv[1]).expanduser())
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
