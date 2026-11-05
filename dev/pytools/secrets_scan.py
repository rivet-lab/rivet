#!/usr/bin/env python3
"""A secrets scan with the pinned gitleaks: the staged content in pre-commit, the whole tracked tree in pr-ready.

    secrets_scan.py staged            the staged diff (the pre-commit hook)
    secrets_scan.py tree [--require]  every file tracked at HEAD
    secrets_scan.py --self-test [--require]

Exit 0 clean, 1 a finding, 2 not run (gitleaks absent or not the pinned binary, see lint_tools.py; 1 under --require or CI).
The allowlist is `.gitleaks.toml`: path or value plus rule, each with its reason in `description`.
"""

from __future__ import annotations

import secrets
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

if __package__:
    from . import lint_tools
else:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import lint_tools  # type: ignore[no-redef]

ROOT = Path(__file__).resolve().parents[2]
VERSION = lint_tools.TOOLS["gitleaks"].version
CONFIG = ROOT / ".gitleaks.toml"
GIT = "/usr/bin/git" if Path("/usr/bin/git").exists() else (shutil.which("git") or "git")


def command(mode: str, target: Path, config: Path = CONFIG, exe: str = "gitleaks") -> list[str]:
    """The gitleaks argv of one mode: `staged` (the repo at `target`) or `tree` (a directory)."""
    common = ["--no-banner", "-v", "--redact", "--exit-code", "1", "-c", str(config)]
    if mode == "staged":
        return [exe, "git", "--staged", *common, str(target)]
    return [exe, "dir", *common, "."]


def not_run(why: str, require: bool) -> int:
    """Say why nothing was scanned: exit 2 on a dev machine, 1 under --require or CI."""
    print(f"secrets scan NOT RUN: {why} ({lint_tools.INSTALL})", file=sys.stderr)
    return 1 if require or lint_tools.in_ci() else 2


def scan(mode: str, root: Path, require: bool = False, config: Path = CONFIG) -> int:
    """Run one mode over the repo at `root`."""
    exe, why = lint_tools.verified("gitleaks")
    if exe is None:
        return not_run(why, require)
    if mode == "staged":
        return subprocess.run(command(mode, root, config, exe), cwd=root).returncode
    with tempfile.TemporaryDirectory(prefix="secrets-scan-") as tmp:
        archive = subprocess.Popen([GIT, "archive", "HEAD"], cwd=root, stdout=subprocess.PIPE)
        subprocess.run(["tar", "-x", "-C", tmp], stdin=archive.stdout, check=True)
        if archive.wait() != 0:
            return 1
        return subprocess.run(command(mode, Path(tmp), config, exe), cwd=tmp).returncode


def repo(files: dict[str, str]) -> Path:
    """A scratch git repo whose index holds `files`."""
    root = Path(tempfile.mkdtemp(prefix="secrets-scan-fixture-"))
    subprocess.run([GIT, "init", "-q"], cwd=root, check=True)
    for rel, text in files.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(text)
    subprocess.run([GIT, "add", "-A"], cwd=root, check=True)
    return root


def self_test(require: bool = False) -> int:
    """A fake token and a fake private key fail in the staged and the tree mode; clean and allowlisted content passes."""
    assert command("staged", Path("/r"))[:4] == ["gitleaks", "git", "--staged", "--no-banner"]
    assert command("tree", Path("/r"))[:2] == ["gitleaks", "dir"] and command("tree", Path("/r"))[-1] == "."
    exe, why = lint_tools.verified("gitleaks")
    if exe is None:
        print(f"secrets_scan self-test: argv ok; fixtures NOT RUN ({why})")
        return 1 if require else 0
    token = "ghp_" + secrets.token_hex(18)
    pem = "-----BEGIN " + "PRIVATE KEY-----\n" + "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCbCRikdpLmY2r2\n" * 4 + "-----END " + "PRIVATE KEY-----\n"
    azurite = "AccountKey=" + "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq" + "/K1SZFPTOtr/KBHBeksoGMGw==\n"
    cases = (
        ("a clean file", {"a.py": "x = 1\n"}, 0),
        ("a token in a script", {"dev/a.py": f'GITHUB_TOKEN = "{token}"\n'}, 1),
        ("a private key in an unlisted path", {"src/other.rs": pem}, 1),
        ("a private key in the allowlisted test file", {"src/gcp_auth.rs": pem}, 0),
        ("a token inside the allowlisted test file", {"src/gcp_auth.rs": f'T = "{token}"\n'}, 1),
        ("the Azurite emulator key", {"tests/common/env.rs": f'const C: &str = "{azurite.strip()}";\n'}, 0),
    )
    for name, files, want in cases:
        root = repo(files)
        try:
            for mode in ("staged", "tree"):
                if mode == "tree":
                    subprocess.run([GIT, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "x"], cwd=root, check=True)
                got = scan(mode, root)
                assert got == want, f"{name} ({mode}): exit {got}, want {want}"
        finally:
            shutil.rmtree(root, ignore_errors=True)
    print(f"secrets_scan self-test: ok ({len(cases)} fixtures x staged and tree, gitleaks {VERSION})")
    return 0


def main(argv: list[str]) -> int:
    """Dispatch the modes."""
    if argv in (["--self-test"], ["--self-test", "--require"]):
        return self_test(require=len(argv) == 2)
    if argv[:1] in (["staged"], ["tree"]):
        return scan(argv[0], ROOT, require="--require" in argv)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
