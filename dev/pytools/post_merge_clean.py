"""The post-merge hook body: keep a worktree's cargo target dir from growing after a merge.

    python3 dev/pytools/post_merge_clean.py                 cargo clean when the merge changed Cargo.lock, else drop incremental/
    python3 dev/pytools/post_merge_clean.py --after-commit  the same for a merge finished by `git commit`; silent for any other commit
    python3 dev/pytools/post_merge_clean.py --self-test
"""

from __future__ import annotations

import os
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


def own_target(root: Path, env: dict[str, str]) -> tuple[Path | None, str]:
    """This worktree's own `<root>/target`, or None and the reason it is not to be cleaned."""
    target = Path(os.path.abspath(root / "target"))
    raw = env.get("CARGO_TARGET_DIR")
    if raw and os.path.realpath(root / raw) != os.path.realpath(target):
        return None, f"CARGO_TARGET_DIR names {os.path.abspath(root / raw)}, not this worktree's {target}"
    if target.is_symlink():
        return None, f"{target} is a symlink"
    if not target.is_dir():
        return None, f"no target dir at {target}"
    if not (target / "CACHEDIR.TAG").is_file():
        return None, f"{target} has no CACHEDIR.TAG, cargo did not make it"
    return target, ""


def lock_changed(root: Path, base: str) -> bool:
    """Whether Cargo.lock differs between `base` and HEAD."""
    return bool(git(root, "diff", "--name-only", base, "HEAD", "--", "Cargo.lock").strip())


def holders(target: Path, lsof_out: str) -> list[str]:
    """The `lsof -F pcn` entries whose cwd, executable, mapped or open file lies under the target dir."""
    roots = {str(target), os.path.realpath(target)}
    me = str(os.getpid())
    found: list[str] = []
    pid = cmd = ""
    for ln in lsof_out.splitlines():
        tag, val = ln[:1], ln[1:]
        if tag == "p":
            pid, cmd = val, ""
        elif tag == "c":
            cmd = val
        elif tag == "n" and pid != me and any(val == r or val.startswith(r + "/") for r in roots):
            entry = f"{pid} {cmd} {val}"
            if entry not in found:
                found.append(entry)
    return found


def lsof_text() -> str:
    """`lsof -F pcn` of every process, the same call on macOS and Linux; a missing or empty lsof raises."""
    p = subprocess.run(["lsof", "-nP", "-w", "-F", "pcn"], capture_output=True, text=True)
    if not p.stdout:
        raise RuntimeError(f"lsof listed nothing (rc {p.returncode}): {p.stderr.strip()[-200:]}")
    return p.stdout


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


def run(root: Path, env: dict[str, str], base: str = "ORIG_HEAD", cargo: list[str] | None = None) -> str:
    """Clean the target dir as the merge warrants and return the one line to print."""
    cargo = cargo or CARGO
    target, why = own_target(root, env)
    if target is None:
        return f"post-merge: skipped, {why}"
    held = holders(target, lsof_text())
    if held:
        return f"post-merge: skipped, a running process holds {target}: {held[0][:160]}"
    if lock_changed(root, base):
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
        root = Path(git(Path.cwd(), "rev-parse", "--show-toplevel").strip())
        base = "ORIG_HEAD"
        if argv == ["--after-commit"]:
            if len(git(root, "rev-list", "--parents", "-1", "HEAD").split()) < 3:
                return 0
            base = "HEAD^1"
        print(run(root, dict(os.environ), base))
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
    """A target dir as cargo leaves it: tag, deps, incremental and fingerprint content."""
    for rel in ("CACHEDIR.TAG", "debug/deps/x.rlib", "debug/incremental/a/b.bin", "debug/.fingerprint/f", "release/deps/y.rlib", "release/incremental/c/d.bin"):
        p = target / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x" * 20000)


def start_holder(root: Path, how: str, alias: Path) -> subprocess.Popen:
    """A long-lived process held in the target dir: by relative argv, by a symlinked spelling of the argv, or by its cwd."""
    exe = root / "target/debug/holdbin"
    exe.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(shutil.which("bash") or "/bin/bash", exe)
    if sys.platform == "darwin":
        subprocess.run(["codesign", "--force", "--sign", "-", str(exe)], check=True, capture_output=True)
    wait = ["-c", "read line"]
    pipe = {"stdin": subprocess.PIPE}
    if how == "relative":
        return subprocess.Popen(["./target/debug/holdbin", *wait], cwd=root, **pipe)
    if how == "symlinked":
        return subprocess.Popen([str(alias / "target/debug/holdbin"), *wait], cwd="/", **pipe)
    return subprocess.Popen([str(exe), *wait], cwd=root / "target/debug/deps", **pipe)


def self_test() -> int:
    """Lock changed or not, a holder started three ways, a target that is not this worktree's own, a merge finished by commit."""
    with tempfile.TemporaryDirectory() as d:
        base = Path(d)
        fake = base / "fake_cargo.py"
        fake.write_text(FAKE_CARGO)
        cargo = [sys.executable, str(fake)]

        root = merged_repo(base / "a", True)
        fake_target(root / "target")
        line = run(root, {"CARGO_TARGET_DIR": str(root / "target")}, cargo=cargo)
        assert "cargo clean" in line and "freed 0 bytes" not in line, line
        assert not (root / "target").exists(), "lock changed: the whole target must go"

        root = merged_repo(base / "b", False)
        fake_target(root / "target")
        line = run(root, {}, cargo=cargo)
        assert "Cargo.lock unchanged" in line and "freed 0 bytes" not in line, line
        assert not (root / "target/debug/incremental").exists() and not (root / "target/release/incremental").exists()
        assert (root / "target/debug/deps/x.rlib").exists() and (root / "target/release/deps/y.rlib").exists() and (root / "target/debug/.fingerprint/f").exists()

        for lock in (True, False):
            for how in ("relative", "symlinked", "cwd"):
                root = merged_repo(base / f"c{lock}{how}", lock)
                alias = base / f"alias{lock}{how}"
                alias.symlink_to(root)
                fake_target(root / "target")
                holder = start_holder(root, how, alias)
                try:
                    for _ in range(50):
                        if holders(root / "target", lsof_text()):
                            break
                        time.sleep(0.1)
                    line = run(root, {}, cargo=cargo)
                finally:
                    holder.kill()
                    holder.wait()
                    holder.stdin.close()
                assert "skipped, a running process" in line, (how, line)
                assert (root / "target/debug/incremental").exists() and (root / "target/debug/deps/x.rlib").exists(), (how, "a holder: nothing may be removed")

        root = merged_repo(base / "d", True)
        assert "no target dir" in run(root, {}, cargo=cargo)
        other = merged_repo(base / "e", True)
        fake_target(other / "target")
        for env in ({"CARGO_TARGET_DIR": str(other / "target")}, {"CARGO_TARGET_DIR": "."}, {"CARGO_TARGET_DIR": str(base)}):
            fake_target(root / "target")
            line = run(root, env, cargo=cargo)
            assert "skipped, CARGO_TARGET_DIR names" in line, line
            assert (other / "target/debug/incremental").exists() and (root / "target/debug/incremental").exists(), env
        shutil.rmtree(root / "target")
        shared = base / "shared"
        fake_target(shared)
        (root / "target").symlink_to(shared)
        assert "is a symlink" in run(root, {}, cargo=cargo) and (shared / "debug/incremental").exists()
        (root / "target").unlink()
        fake_target(root / "target")
        (root / "target/CACHEDIR.TAG").unlink()
        assert "no CACHEDIR.TAG" in run(root, {}, cargo=cargo) and (root / "target/debug/incremental").exists()
        assert (root / "Cargo.lock").exists() and (root / ".git").exists()

        saved = os.getcwd(), os.environ.pop("CARGO_TARGET_DIR", None), CARGO[:]
        try:
            CARGO[:] = cargo
            for lock in (True, False):
                root = merged_repo(base / f"f{lock}", lock)
                fake_target(root / "target")
                os.chdir(root)
                subprocess.run(["git", "update-ref", "ORIG_HEAD", "HEAD"], cwd=root, check=True)
                assert main(["--after-commit"]) == 0
                assert (root / "target").exists() is (not lock) and ((root / "target/debug/incremental").exists() is False), lock
                fake_target(root / "target")
                subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", "commit", "-q", "--allow-empty", "-m", "plain"], cwd=root, check=True, capture_output=True)
                assert main(["--after-commit"]) == 0 and (root / "target/debug/incremental").exists(), "a plain commit must leave the target alone"
        finally:
            os.chdir(saved[0])
            if saved[1] is not None:
                os.environ["CARGO_TARGET_DIR"] = saved[1]
            CARGO[:] = saved[2]
    print("post_merge_clean self-test: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
