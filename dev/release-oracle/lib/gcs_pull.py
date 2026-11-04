#!/usr/bin/env python3
"""Download every .parquet under a prefix from the fake-gcs emulator into a local
dir — an INDEPENDENT readback for the load gate when `gsutil` is not installed.

Uses only the fake-gcs JSON API (list) + its media download endpoint, so it never
routes through rivet: a rivet write bug cannot rubber-stamp its own output.

Usage: gcs_pull.py <endpoint> <bucket> <prefix> <dest_dir> [--all]
Prints the number of objects downloaded; a list or a download that fails after its
retries exits non-zero with the reason on stderr, never a count of 0.

`--all` pulls EVERY object and preserves its path under the prefix, rather than
only `.parquet` flattened to `part_N.parquet`.
"""
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

TIMEOUT = 60.0
ATTEMPTS = 3
PAUSE = 1.0


class PullError(RuntimeError):
    """A list or a download that kept failing; the store was not read."""


def _get(url, read, timeout, attempts, pause):
    """`read(response)` of `url`, retrying a timeout, a transport error or a bad body; a 404 is None."""
    last = None
    for i in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return read(r)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            last = e
        except (urllib.error.URLError, OSError, ValueError) as e:
            last = e
        if i + 1 < attempts:
            time.sleep(pause)
    raise PullError(f"{url}: {last} after {attempts} attempts")


def list_names(endpoint, bucket, prefix, timeout=TIMEOUT, attempts=ATTEMPTS, pause=PAUSE):
    """Every object name under `prefix`, all pages; a missing bucket holds none."""
    names, token = [], ""
    while True:
        url = f"{endpoint}/storage/v1/b/{bucket}/o?prefix={urllib.parse.quote(prefix, safe='')}"
        if token:
            url += f"&pageToken={urllib.parse.quote(token, safe='')}"
        doc = _get(url, json.load, timeout, attempts, pause)
        if doc is None:
            return names
        names += [it.get("name", "") for it in doc.get("items", [])]
        token = doc.get("nextPageToken", "")
        if not token:
            return names


def pull(endpoint, bucket, prefix, dest, pull_all=False, timeout=TIMEOUT, attempts=ATTEMPTS, pause=PAUSE):
    """Download the objects under `prefix` into `dest`; the count, or PullError."""
    os.makedirs(dest, exist_ok=True)
    n = 0
    for name in list_names(endpoint, bucket, prefix, timeout, attempts, pause):
        if not pull_all and not name.endswith(".parquet"):
            continue
        media = f"{endpoint}/download/storage/v1/b/{bucket}/o/{urllib.parse.quote(name, safe='')}?alt=media"
        if pull_all:
            rel = name[len(prefix):].lstrip("/") or os.path.basename(name)
            out = os.path.join(dest, rel)
            os.makedirs(os.path.dirname(out) or dest, exist_ok=True)
        else:
            out = os.path.join(dest, f"part_{n}.parquet")

        def save(r, out=out):
            with open(out, "wb") as f:
                shutil.copyfileobj(r, f)
            return True

        if _get(media, save, timeout, attempts, pause) is None:
            raise PullError(f"{media}: listed but not downloadable (404)")
        n += 1
    return n


if __name__ == "__main__":
    endpoint, bucket, prefix, dest = sys.argv[1:5]
    try:
        print(pull(endpoint, bucket, prefix, dest, "--all" in sys.argv[5:]))
    except PullError as e:
        sys.exit(f"gcs_pull: {e}")
