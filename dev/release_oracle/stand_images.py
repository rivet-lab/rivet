"""The image of every running container, with its RepoDigest, recorded beside the gate's logs.

A gate verdict names the commit but not the images the stand ran. The record is one JSON file, merged
by container name each time a container comes up and once more at the end, so a container torn down
mid-run still appears.

    python3 -m dev.release_oracle.stand_images --self-test
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from typing import Callable

__all__ = ["running_images", "record", "set_target", "record_to_target"]

Runner = Callable[[list[str]], str]
_LOCK = threading.Lock()
_TARGET: list[Path] = []


def _docker(args: list[str]) -> str:
    """Stdout of a docker command; raises on a non-zero exit."""
    p = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=120, check=True)
    return p.stdout


def running_images(run: Runner = _docker) -> list[dict]:
    """One row per running container: name, the image string it was started with, its image ID and RepoDigests."""
    ids = run(["ps", "-q"]).split()
    if not ids:
        return []
    rows = [json.loads(ln) for ln in run(["inspect", "--format", '{"name":{{json .Name}},"image":{{json .Config.Image}},"id":{{json .Image}}}', *ids]).splitlines() if ln.strip()]
    digests = {}
    for img in json.loads(run(["image", "inspect", *sorted({r["id"] for r in rows})])):
        digests[img["Id"]] = sorted(img.get("RepoDigests") or [])
    return sorted(
        ({"container": r["name"].lstrip("/"), "image": r["image"], "image_id": r["id"], "repo_digests": digests.get(r["id"], [])} for r in rows),
        key=lambda r: r["container"],
    )


def record(path: Path, run: Runner = _docker) -> list[dict]:
    """Merge the running containers into the JSON file at `path` by container name and return the whole record."""
    with _LOCK:
        known = {}
        if path.is_file():
            known = {r["container"]: r for r in json.loads(path.read_text())}
        for r in running_images(run):
            known[r["container"]] = r
        merged = sorted(known.values(), key=lambda r: r["container"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(merged, indent=1) + "\n")
        return merged


def set_target(path: Path) -> None:
    """Name the file `record_to_target` writes; the gate sets it once from `--images-file`."""
    _TARGET[:] = [path]


def record_to_target() -> None:
    """Record into the named file; a docker failure is printed, never swallowed into a clean-looking run."""
    if not _TARGET:
        raise RuntimeError("stand_images.set_target was not called: the gate would record no images")
    try:
        record(_TARGET[0])
    except (OSError, subprocess.SubprocessError, ValueError, KeyError) as e:
        print(f"  stand images NOT recorded to {_TARGET[0]}: {e}", file=sys.stderr)


def self_test() -> None:
    """Grade the record on a scripted docker: rows, merge by name, a vanished container kept, a bad reply refused."""
    ps = {"ids": "a1\nb2\n"}

    def run(args: list[str]) -> str:
        if args[0] == "ps":
            return ps["ids"]
        if args[0] == "inspect":
            return "".join(
                json.dumps({"name": f"/{n}", "image": im, "id": iid}) + "\n"
                for n, im, iid in [("db", "pg:1@sha256:" + "a" * 64, "sha256:i1"), ("kv", "mongo:7", "sha256:i2")]
            )
        return json.dumps([{"Id": "sha256:i1", "RepoDigests": ["pg@sha256:" + "a" * 64]}, {"Id": "sha256:i2", "RepoDigests": []}])

    rows = running_images(run)
    assert [r["container"] for r in rows] == ["db", "kv"], rows
    assert rows[0]["repo_digests"] == ["pg@sha256:" + "a" * 64] and rows[1]["repo_digests"] == [], rows
    assert running_images(lambda a: "") == [], "no containers is an empty record, not an error"
    with tempfile.TemporaryDirectory() as d:
        path = Path(d, "sub", "images.json")
        first = record(path, run)
        assert len(first) == 2 and json.loads(path.read_text()) == first
        ps["ids"] = ""
        again = record(path, run)
        assert [r["container"] for r in again] == ["db", "kv"], "a container torn down mid-run must stay recorded"
        path.write_text("not json")
        try:
            record(path, run)
        except ValueError:
            pass
        else:
            raise AssertionError("a corrupt record must raise, not be overwritten silently")
        _TARGET.clear()
        try:
            record_to_target()
        except RuntimeError:
            pass
        else:
            raise AssertionError("recording with no target set must raise")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        print("stand_images self-test OK")
    else:
        print("usage: python3 -m dev.release_oracle.stand_images --self-test", file=sys.stderr)
        raise SystemExit(2)
