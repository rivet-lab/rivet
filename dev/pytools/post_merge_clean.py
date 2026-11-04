"""The post-merge hook body: keep a worktree's cargo target dir from growing after a merge.

    python3 dev/pytools/post_merge_clean.py              cargo clean when the merge changed Cargo.lock, else drop incremental/
    python3 dev/pytools/post_merge_clean.py --self-test
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

INCREMENTAL = ("debug/incremental", "release/incremental")
CARGO = ["cargo"]


def git(root: Path, *args: str) -> str:
    """Stdout of `git <args>` in `root`; a failing git raises."""
    p = subprocess.run(["git", *args], cwd=root, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {p.stderr.strip()}")
    return p.stdout


def target_dir(root: Path, env: dict[str, str]) -> Path:
    """The cargo target dir of this worktree: CARGO_TARGET_DIR when set, else `<root>/target`."""
    raw = env.get("CARGO_TARGET_DIR")
    return Path(os.path.abspath(root / raw if raw else root / "target"))


def lock_changed(root: Path) -> bool:
    """Whether the merge that just finished changed Cargo.lock."""
    return bool(git(root, "diff", "--name-only", "ORIG_HEAD", "HEAD", "--", "Cargo.lock").strip())


def holders(target: Path, ps_text: str) -> list[str]:
    """The `ps` lines whose argv names the target dir, in either spelling."""
    spellings = {str(target), os.path.realpath(target)}
    pat = re.compile("|".join(re.escape(s) + r"(?=[/\s]|$)" for s in spellings))
    me = str(os.getpid())
    return [ln.strip() for ln in ps_text.splitlines() if pat.search(ln) and ln.split(None, 1)[0] != me]


def ps_text() -> str:
    """`ps` with pid and full argv of every process, the same call on macOS and Linux."""
    return subprocess.run(["ps", "-axww", "-o", "pid=,command="], capture_output=True, text=True).stdout


def disk_bytes(path: Path) -> int:
    """Allocated bytes under `path`, each hard-linked file counted once."""
    seen: set[tuple[int, int]] = set()
    total = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for name in [*dirnames, *filenames]:
            try:
                st = os.lstat(os.path.join(dirpath, name))
            except OSError:
                continue
            if (st.st_dev, st.st_ino) not in seen:
                seen.add((st.st_dev, st.st_ino))
                total += st.st_blocks * 512
    return total


def human(n: int) -> str:
    """Bytes as a short binary-unit string."""
    x = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if x < 1024 or unit == "GiB":
            return f"{x:.1f} {unit}"
        x /= 1024
    return f"{n} B"


def run(root: Path, env: dict[str, str], cargo: list[str] | None = None) -> str:
    """Clean the target dir as the merge warrants and return the one line to print."""
    cargo = cargo or CARGO
    target = target_dir(root, env)
    if not target.is_dir():
        return f"post-merge: skipped, no target dir at {target}"
    real, top = Path(os.path.realpath(target)), Path(os.path.realpath(root))
    if real == top or real in top.parents:
        return f"post-merge: skipped, {target} holds the worktree itself"
    held = holders(target, ps_text())
    if held:
        return f"post-merge: skipped, a running process holds {target}: {held[0][:160]}"
    if lock_changed(root):
        before = disk_bytes(target)
        p = subprocess.run([*cargo, "clean", "--target-dir", str(target)], cwd=root, capture_output=True, text=True)
        if p.returncode != 0:
            return f"post-merge: cargo clean failed ({p.stderr.strip()[-200:]})"
        what = "Cargo.lock changed, cargo clean"
        freed = before - (disk_bytes(target) if target.exists() else 0)
    else:
        dirs = [target / rel for rel in INCREMENTAL if (target / rel).is_dir()]
        freed = sum(disk_bytes(d) for d in dirs)
        for d in dirs:
            shutil.rmtree(d)
        what = "Cargo.lock unchanged, removed " + (", ".join(str(d.relative_to(target)) for d in dirs) or "nothing")
    return f"post-merge: {what} in {target}: freed {freed} bytes ({human(freed)})"


def main(argv: list[str]) -> int:
    """Hook entry: print what was done; never fail the merge."""
    if argv == ["--self-test"]:
        return self_test()
    try:
        print(run(Path(git(Path.cwd(), "rev-parse", "--show-toplevel").strip()), dict(os.environ)))
    except Exception as e:
        print(f"post-merge: skipped, {type(e).__name__}: {e}")
    return 0


FAKE_CARGO = (
    "import shutil, sys\n"
    "a = sys.argv[1:]\n"
    "assert a[0] == 'clean' and a[1] == '--target-dir', a\n"
    "shutil.rmtree(a[2])\n"
)


def merged_repo(base: Path, lock_changes: bool) -> Path:
    """A repo with a real merge behind it, so ORIG_HEAD is the git one; the merge changes Cargo.lock or not."""
    root = base / "wt"
    root.mkdir(parents=True)

    def g(*a: str) -> None:
        subprocess.run(
            ["git", "-c", "core.hooksPath=/dev/null", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", *a],
            cwd=root, check=True, capture_output=True,
        )

    g("init", "-q", "-b", "main")
    (root / "Cargo.lock").write_text("a\n")
    g("add", "."), g("commit", "-q", "-m", "base")
    g("checkout", "-q", "-b", "side")
    (root / ("Cargo.lock" if lock_changes else "other.txt")).write_text("b\n")
    g("add", "."), g("commit", "-q", "-m", "side")
    g("checkout", "-q", "main")
    (root / "main.txt").write_text("m\n")
    g("add", "."), g("commit", "-q", "-m", "main")
    g("merge", "-q", "--no-edit", "side")
    return root


def fake_target(target: Path) -> None:
    """A target dir with deps, incremental and fingerprint content."""
    for rel in ("debug/deps/x.rlib", "debug/incremental/a/b.bin", "debug/.fingerprint/f", "release/deps/y.rlib", "release/incremental/c/d.bin"):
        p = target / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x" * 20000)


def self_test() -> int:
    """Lock changed, lock unchanged, a holder, a missing target and a target that is the worktree."""
    with tempfile.TemporaryDirectory() as d:
        base = Path(d)
        fake = base / "fake_cargo.py"
        fake.write_text(FAKE_CARGO)
        cargo = [sys.executable, str(fake)]

        root = merged_repo(base / "a", True)
        fake_target(root / "target")
        line = run(root, {}, cargo)
        assert "cargo clean" in line and "freed 0 bytes" not in line, line
        assert not (root / "target").exists(), "lock changed: the whole target must go"

        root = merged_repo(base / "b", False)
        other = base / "elsewhere"
        fake_target(other)
        fake_target(root / "target")
        line = run(root, {"CARGO_TARGET_DIR": str(other)}, cargo)
        assert "Cargo.lock unchanged" in line and "freed 0 bytes" not in line, line
        assert not (other / "debug/incremental").exists() and not (other / "release/incremental").exists()
        assert (other / "debug/deps/x.rlib").exists() and (other / "release/deps/y.rlib").exists() and (other / "debug/.fingerprint/f").exists()
        assert (root / "target/debug/incremental").exists(), "CARGO_TARGET_DIR must be respected"

        for lock in (True, False):
            root = merged_repo(base / f"c{lock}", lock)
            fake_target(root / "target")
            holder = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", str(root / "target/debug/deps")])
            try:
                for _ in range(50):
                    if holders(root / "target", ps_text()):
                        break
                    time.sleep(0.1)
                line = run(root, {}, cargo)
            finally:
                holder.kill()
                holder.wait()
            assert "skipped, a running process" in line, line
            assert (root / "target/debug/incremental").exists() and (root / "target/debug/deps/x.rlib").exists(), "a holder: nothing may be removed"

        root = merged_repo(base / "d", True)
        assert "no target dir" in run(root, {}, cargo)
        assert "holds the worktree" in run(root, {"CARGO_TARGET_DIR": "."}, cargo)
        assert (root / "Cargo.lock").exists() and (root / ".git").exists()
    print("post_merge_clean self-test: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
