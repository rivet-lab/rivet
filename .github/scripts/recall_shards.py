"""Print the nightly recall matrix: the seeds of dev/seeded/seeds.yaml (or $SEEDS) dealt round-robin into N shards."""

import json
import os
import sys

import yaml


def deal(names: list[str], n: int) -> list[dict]:
    """Round-robin the sorted names into at most n shards; an empty shard is dropped."""
    shards = [{"index": i, "seeds": ",".join(names[i::n])} for i in range(n)]
    return [s for s in shards if s["seeds"]]


def main() -> int:
    """Write `shards=<json>` for the workflow matrix."""
    n = int(sys.argv[1])
    given = [s for s in os.environ.get("SEEDS", "").split(",") if s]
    names = sorted(given or [s["name"] for s in yaml.safe_load(open("dev/seeded/seeds.yaml"))["seeds"]])
    print("shards=" + json.dumps(deal(names, n)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
