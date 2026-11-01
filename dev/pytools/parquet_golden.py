"""Regenerate `tests/fixtures/parquet_bytes_0_32_2.txt` from the Parquet writer of a released tag.

    python3 dev/pytools/parquet_golden.py regen [--tag v0.32.2]

The test `tests/parquet_bytes_golden.rs` is copied into a detached worktree of the tag and its
ignored dump test writes the digests there. Commit a changed file only as a deliberate byte
change, with a CHANGELOG entry that says so.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TEST = "tests/parquet_bytes_golden.rs"
DIGESTS = "tests/fixtures/parquet_bytes_0_32_2.txt"


def run(cmd: list[str], cwd: Path) -> None:
    """Run `cmd` in `cwd` and raise on a non-zero exit."""
    subprocess.run(cmd, cwd=cwd, check=True)


def regen(tag: str) -> int:
    """Dump the digests of `tag`'s writer into this tree's digest file."""
    work = Path(tempfile.mkdtemp(prefix="parquet-golden-"))
    tree = work / "tree"
    run(["git", "worktree", "add", "--detach", str(tree), tag], ROOT)
    try:
        shutil.copy(ROOT / TEST, tree / TEST)
        (tree / DIGESTS).parent.mkdir(parents=True, exist_ok=True)
        env = {**os.environ, "CARGO_TARGET_DIR": str(ROOT / "target" / "parquet-golden")}
        subprocess.run(
            ["cargo", "test", "--locked", "--test", "parquet_bytes_golden", "parquet_bytes_dump",
             "--", "--ignored"],
            cwd=tree, env=env, check=True,
        )
        shutil.copy(tree / DIGESTS, ROOT / DIGESTS)
    finally:
        run(["git", "worktree", "remove", "--force", str(tree)], ROOT)
        shutil.rmtree(work, ignore_errors=True)
    print((ROOT / DIGESTS).read_text(), end="")
    return 0


def main() -> int:
    """Parse the command line and regenerate."""
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("regen")
    r.add_argument("--tag", default="v0.32.2")
    args = p.parse_args()
    return regen(args.tag)


if __name__ == "__main__":
    sys.exit(main())
