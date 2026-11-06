#!/usr/bin/env python3
"""Remove finished git worktrees and clean idle cargo target dirs; dry-run unless --apply. `--self-test` grades the keep rules."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dev.pytools import devbox_load  # noqa: E402


def run(cmd, cwd=None, check=False):
    """Run a command and return (code, stdout)."""
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if check and p.returncode != 0:
        raise SystemExit(f"{' '.join(cmd)} failed: {p.stderr.strip()}")
    return p.returncode, p.stdout.strip()


def worktrees(repo):
    """Parse `git worktree list --porcelain` into dicts; first entry is the main worktree."""
    _, out = run(["git", "worktree", "list", "--porcelain"], cwd=repo, check=True)
    items, cur = [], {}
    for line in out.splitlines() + [""]:
        if not line:
            if cur:
                items.append(cur)
            cur = {}
            continue
        key, _, val = line.partition(" ")
        cur[key] = val or True
    return items


def busy_paths():
    """Return cwd paths of all live processes."""
    _, out = run(["lsof", "-a", "-d", "cwd", "-F", "n"])
    return [l[1:] for l in out.splitlines() if l.startswith("n/")]


def mounted_paths():
    """Return bind-mount sources and compose working dirs of running containers."""
    docker = shutil.which("docker") or next((d for d in ("/opt/homebrew/bin/docker", "/usr/local/bin/docker") if os.path.exists(d)), None)
    if not docker:
        return []
    _, ids = run([docker, "ps", "-q"])
    if not ids:
        return []
    fmt = '{{range .Mounts}}{{if eq .Type "bind"}}{{println .Source}}{{end}}{{end}}{{println (index .Config.Labels "com.docker.compose.project.working_dir")}}'
    _, out = run([docker, "inspect", "--format", fmt, *ids.split()])
    return [l for l in out.splitlines() if l.startswith("/")]


def in_use(path, cwds):
    """True if any process works inside path."""
    p = os.path.realpath(path)
    return any(os.path.realpath(c) == p or os.path.realpath(c).startswith(p + "/") for c in cwds)


def queued_paths(night=None):
    """Worktree paths that the tasks of every night-queue file run in."""
    paths = set()
    for queue in sorted((night or devbox_load.NIGHT).glob("*.json")):
        try:
            tasks = json.loads(queue.read_text())
        except (OSError, ValueError):
            continue
        if isinstance(tasks, list):
            paths |= {os.path.realpath(os.path.expanduser(t["cwd"])) for t in tasks if isinstance(t, dict) and t.get("cwd")}
    return paths


def kept(wt, queued, cwds, gate=None):
    """Why a worktree stays whatever its git state says ("" = it may go)."""
    path = wt["worktree"]
    if wt.get("locked"):
        return "locked"
    if os.path.realpath(path) == os.path.realpath(gate or devbox_load.GATE_WORKTREE):
        return "the gate worktree"
    if os.path.realpath(path) in queued:
        return "night queue task runs here"
    if in_use(path, cwds):
        return "process inside"
    return ""


def last_activity(path):
    """Newest mtime of the worktree's HEAD/index (git) or the dir itself."""
    _, gitdir = run(["git", "rev-parse", "--absolute-git-dir"], cwd=path)
    probes = [Path(path)] + [Path(gitdir) / n for n in ("HEAD", "index", "logs/HEAD")]
    return max((p.stat().st_mtime for p in probes if p.exists()), default=0)


def merged_pr(repo, branch, head):
    """Return the merged PR number whose head is exactly this commit, else None."""
    code, out = run(["gh", "pr", "list", "--head", branch, "--state", "merged",
                     "--json", "number,headRefOid", "--limit", "5"], cwd=repo)
    if code != 0 or not out:
        return None
    for pr in json.loads(out):
        if pr["headRefOid"] == head:
            return pr["number"]
    return None


def finished(repo, wt):
    """Return (done, reason) for a non-main worktree."""
    path, head = wt["worktree"], wt.get("HEAD", "")
    code, dirty = run(["git", "status", "--porcelain"], cwd=path)
    if code != 0:
        return False, "git status failed"
    if dirty:
        return False, "dirty"
    if run(["git", "merge-base", "--is-ancestor", head, "origin/main"], cwd=repo)[0] == 0:
        return True, "HEAD is in origin/main"
    branch = wt.get("branch", "").removeprefix("refs/heads/")
    if not branch:
        return False, "detached, not in origin/main"
    pr = merged_pr(repo, branch, head)
    if pr:
        return True, f"PR #{pr} merged at this HEAD"
    return False, "no merged PR at this HEAD"


def target_idle_days(target):
    """Days since the last build wrote into target (fingerprint dirs)."""
    stamps = [p.stat().st_mtime for p in target.glob("*/.fingerprint")]
    stamps += [p.stat().st_mtime for p in target.glob("*/*/.fingerprint")]
    newest = max(stamps, default=target.stat().st_mtime)
    return (time.time() - newest) / 86400


def size_gb(path):
    """Disk usage of path in GB."""
    _, out = run(["du", "-sk", str(path)])
    return int(out.split()[0]) / 1048576 if out else 0.0


def self_test():
    """The keep rules: the gate worktree, queued cwds from every queue file, a process inside, a lock."""
    with tempfile.TemporaryDirectory() as t:
        night, gate, other = Path(t) / "night", Path(t) / "wt" / "gate", Path(t) / "wt" / "other"
        night.mkdir()
        gate.mkdir(parents=True)
        other.mkdir(parents=True)
        (night / "queue.json").write_text(json.dumps([{"id": "a", "cwd": str(other)}]))
        (night / "weekly.json").write_text(json.dumps([{"id": "b", "cwd": str(Path(t) / "wt" / "weekly")}]))
        (night / "queue.json.bak").write_text("[not json")
        (night / "status.json").write_text(json.dumps({"id": "x"}))
        queued = queued_paths(night)
        assert queued == {os.path.realpath(other), os.path.realpath(Path(t) / "wt" / "weekly")}, queued
        free = {"worktree": str(Path(t) / "wt" / "free")}
        assert kept(free, queued, [], gate) == ""
        assert kept({"worktree": str(gate)}, queued, [], gate) == "the gate worktree"
        assert kept({"worktree": str(gate)}, set(), [str(gate)], gate) == "the gate worktree"
        assert kept({"worktree": str(other)}, queued, [], gate) == "night queue task runs here"
        assert kept({"worktree": str(Path(t) / "wt" / "weekly")}, queued, [], gate) == "night queue task runs here"
        assert kept(free, queued, [str(free["worktree"]) + "/target"], gate) == "process inside"
        assert kept({**free, "locked": True}, queued, [], gate) == "locked"
    print("wt_sweep self-test: ok")
    return 0


def main():
    if sys.argv[1:] == ["--self-test"]:
        return self_test()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo", default=str(Path.home() / "rivet"))
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--min-idle-hours", type=float, default=6)
    ap.add_argument("--target-idle-days", type=float, default=3)
    a = ap.parse_args()

    repo = a.repo
    run(["git", "fetch", "--prune", "--quiet", "origin"], cwd=repo)
    cwds = busy_paths() + mounted_paths()
    queued = queued_paths()
    mode = "APPLY" if a.apply else "DRY"
    freed, removed, cleaned = 0.0, 0, 0
    print(f"== wt_sweep {time.strftime('%Y-%m-%d %H:%M')} {mode} repo={repo}")

    wts = worktrees(repo)
    survivors = [wts[0]["worktree"]]
    for wt in wts[1:]:
        path = wt["worktree"]
        if wt.get("prunable") or not os.path.isdir(path):
            print(f"PRUNE  {path} (gone)")
            continue
        why = kept(wt, queued, cwds)
        if why:
            print(f"KEEP   {path}: {why}")
            survivors.append(path)
            continue
        idle_h = (time.time() - last_activity(path)) / 3600
        if idle_h < a.min_idle_hours:
            print(f"KEEP   {path}: active {idle_h:.1f}h ago")
            survivors.append(path)
            continue
        done, why = finished(repo, wt)
        if not done:
            print(f"KEEP   {path}: {why}")
            survivors.append(path)
            continue
        gb = size_gb(path)
        print(f"REMOVE {path}: {why}, {gb:.1f}G")
        if a.apply:
            target = Path(path) / "target"
            if target.is_dir():
                shutil.rmtree(target)
            code, _ = run(["git", "worktree", "remove", path], cwd=repo)
            if code != 0:
                print(f"  ! git worktree remove refused, left in place")
                continue
            branch = wt.get("branch", "").removeprefix("refs/heads/")
            if branch:
                run(["git", "branch", "-D", branch], cwd=repo)
        freed += gb
        removed += 1

    for path in survivors:
        target = Path(path) / "target"
        if not target.is_dir() or in_use(path, cwds):
            continue
        idle = target_idle_days(target)
        if idle < a.target_idle_days:
            continue
        gb = size_gb(target)
        print(f"CLEAN  {target}: idle {idle:.1f}d, {gb:.1f}G")
        if a.apply:
            shutil.rmtree(target)
        freed += gb
        cleaned += 1

    if a.apply:
        run(["git", "worktree", "prune"], cwd=repo)
    print(f"== {mode}: {removed} worktrees removed, {cleaned} targets cleaned, {freed:.1f}G freed")


if __name__ == "__main__":
    sys.exit(main())
