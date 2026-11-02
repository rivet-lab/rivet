"""Tag a Rivet release and (optionally) push the tag — port of `dev/scripts/release.sh`.

Pushing the tag is what triggers `.github/workflows/release.yml`, which builds
the four target tarballs, publishes the crate, writes `SHA256SUMS.txt`, and
regenerates the Homebrew formula. Everything this module does happens BEFORE
that point, so every check here is a gate on an immutable artifact: once the tag
is pushed, a bad tag can only be fixed by a new patch release.

Preconditions the script enforces, in order (1-5 unchanged from the bash):

1. `Cargo.toml` has a parseable `version`.
2. `--tag`, when given, looks like `vX.Y.Z` AND matches that version.
3. `CHANGELOG.md` exists and has a `## <version> ` section (the release
   workflow extracts the release notes from exactly that heading — no section
   means an empty release body, and the workflow fails at that step *after* the
   tag is already public).
4. The tag does not already exist locally.
5. The worktree is clean (tracked files only — see the notes below).
6. The gate record (`dev/release-oracle/timings.jsonl`, or `--gate-record FILE`
   copied from the machine that ran the gate) holds a `release-oracle-full` run
   of exactly HEAD's commit, on a clean tree, and the latest such run says
   RELEASE-READY. A reduced run (`make release-oracle`, an escape, a narrowed
   scope, no seeded recall) is never enough.
7. The seeded recall's run record (`recall-runs.jsonl` in the seeded state dir,
   or `--recall-record FILE`) holds a complete, green recall of exactly HEAD's
   commit from a clean tree.
8. The same gate record (or `--island-record FILE`) holds a `release-oracle-island`
   run of exactly HEAD's commit (the measured stages, `make release-oracle-island`)
   under the same rules as 6.

WHAT THIS SCRIPT DOES **NOT** CHECK (reported, not invented):

* The `cargo chef prepare` strict-TOML parse that broke 0.16.0's Docker images
  at the tag. That guard exists, but as an offline test —
  `tests/offline/cargo_manifest_chef.rs` — not here.
* The committed-`Cargo.lock` / `cargo publish --locked` sync that aborted
  0.16.1 after the tag was cut. That guard is a CI step
  (`.github/workflows/ci.yml`, `cargo metadata --locked`), deliberately NOT a
  runtime file read, because `cargo` reconciles the working-tree lock before any
  in-process check could observe the staleness.
* Anything `gh`-shaped: the bash script never invoked `gh`, so neither does this.

Both belong to the release path but sit in other layers; this port keeps its
behaviour identical rather than growing new refusals. If they should gate the
tag too, that is a deliberate follow-up, not a silent addition here.

Usage mirrors the bash exactly:

    python dev/pytools/release.py --dry-run
    python dev/pytools/release.py
    python dev/pytools/release.py --push
    python dev/pytools/release.py --message "summary" --push
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

if __package__:
    from . import shell
else:  # executed as a plain script: `python3 dev/pytools/release.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
CARGO_TOML = ROOT / "Cargo.toml"
CHANGELOG = ROOT / "CHANGELOG.md"

USAGE = """Usage: python3 -m dev.pytools.release [--dry-run] [--push] [--tag vX.Y.Z] [--message TEXT] [--gate-record FILE] [--recall-record FILE] [--island-record FILE]

Options:
  --dry-run     Validate only; do not create or push a tag
  --push        Push the tag to origin after creating it
  --tag vX.Y.Z  Override tag (default: v<version from Cargo.toml>)
  --message     Annotated tag message (default: vX.Y.Z: <CHANGELOG subtitle>)
  --gate-record The gate's verdict lines (default: dev/release-oracle/timings.jsonl)
  --recall-record  The seeded recall's run lines (default: recall-runs.jsonl in the seeded state dir)
  --island-record  The measured island's verdict lines (default: the gate record)
  --self-test   Check the gate-record precondition on fixtures; tags nothing
  -h, --help    Show this help"""

# The bash hard-codes this owner in the "watch:" line even though the Homebrew
# generator derives owner/repo from Cargo.toml. Kept verbatim so the transcript
# diffs clean; flagged rather than silently "improved".
ACTIONS_URL = "https://github.com/rivet-lab/rivet/actions/workflows/release.yml"
#: Where `make release-oracle-full` appends its verdict line (dev/release_oracle/core.py TIMINGS_HISTORY).
GATE_RECORD = ROOT / "dev" / "release-oracle" / "timings.jsonl"
#: The only gate kind whose green verdict supports a tag (dev/release_oracle/core.py FULL_GATE).
FULL_GATE = "release-oracle-full"
#: The gate kind of the measured stages alone (`make release-oracle-island`): the other record a tag needs.
ISLAND_GATE = "release-oracle-island"
#: The configuration a full gate runs under: no bless mode, no tolerance override, both state backends graded.
CANONICAL_CONFIG: dict = {"bless": [], "knobs": {}, "state": ["postgres", "sqlite"]}


class _Usage(Exception):
    """`usage()` in the bash: print the help and exit 1 — note it exits 1 even
    for `-h`, which argparse would not do. Raised so the parse helpers can bail
    from anywhere without a second return channel."""


@dataclass
class Args:
    dry_run: bool = False
    push: bool = False
    tag_override: str = ""
    message: str = ""
    gate_record: str = ""
    recall_record: str = ""
    island_record: str = ""


def parse_args(argv: list[str]) -> Args:
    """Hand-rolled to match the bash `while/case` loop exactly.

    argparse is avoided on purpose here: it exits 0 on `-h` and 2 on a bad flag,
    while the bash exits 1 for both, and it would accept abbreviations the bash
    never did. A release gate is the wrong place to change an exit code.
    """
    a = Args()
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--dry-run":
            a.dry_run = True
            i += 1
        elif arg == "--push":
            a.push = True
            i += 1
        elif arg == "--tag":
            # `${2:-}` then a non-empty test: a missing OR empty value is usage.
            value = argv[i + 1] if i + 1 < len(argv) else ""
            if not value:
                raise _Usage()
            a.tag_override = value
            i += 2
        elif arg == "--message":
            value = argv[i + 1] if i + 1 < len(argv) else ""
            if not value:
                raise _Usage()
            a.message = value
            i += 2
        elif arg == "--gate-record":
            value = argv[i + 1] if i + 1 < len(argv) else ""
            if not value:
                raise _Usage()
            a.gate_record = value
            i += 2
        elif arg == "--recall-record":
            value = argv[i + 1] if i + 1 < len(argv) else ""
            if not value:
                raise _Usage()
            a.recall_record = value
            i += 2
        elif arg == "--island-record":
            value = argv[i + 1] if i + 1 < len(argv) else ""
            if not value:
                raise _Usage()
            a.island_record = value
            i += 2
        elif arg in ("-h", "--help"):
            raise _Usage()
        else:
            print(f"Unknown option: {arg}", file=sys.stderr)
            raise _Usage()
    return a


# ── Cargo.toml / CHANGELOG parsing ─────────────────────────────────────────────
_VERSION_LINE = re.compile(r"^version\s*=")
_VERSION_VALUE = re.compile(r'^version\s*=\s*"([^"]*)"')


def cargo_version(cargo_toml: Path = CARGO_TOML) -> str:
    """First column-0 `version = "…"` in Cargo.toml, like `grep | head -1 | sed`.

    Same window as the bash — the FIRST such line wins, which is the `[package]`
    version only because `[package]` is the first table in this manifest. A
    workspace-style `version = { workspace = true }` has no quoted value, so the
    extraction finds nothing and we fail loudly instead of tagging `v`.

    (The bash pipeline `grep … | head -1 | sed …` is also a live `pipefail`
    hazard: `head` closing the pipe early can leave `grep` killed by SIGPIPE,
    which `set -o pipefail` propagates as 141 and `set -e` turns into a silent,
    message-less abort. Reading the file once removes that failure mode.)
    """
    if not cargo_toml.is_file():
        # The bash does not test for this file (the Homebrew script does); it
        # would die inside the pipeline with grep's own message and exit 2. Same
        # refusal, one clear message.
        raise shell.Fail(
            "error: could not parse version from Cargo.toml",
            hint=f"{cargo_toml} does not exist",
        )
    for line in cargo_toml.read_text().splitlines():
        if _VERSION_LINE.match(line):
            m = _VERSION_VALUE.match(line)
            return m.group(1) if m else ""
    return ""


def changelog_has_section(version: str, changelog: Path = CHANGELOG) -> bool:
    """Is there a `## <version> ` heading?

    The awk built its regex by string concatenation, so the version's dots were
    wildcards. We escape them: a heading that only matched through a wildcard was
    never the right heading, so escaping can turn a false PASS into a fail, never
    the reverse — the safe direction for a release gate.
    """
    pattern = re.compile(r"^## " + re.escape(version) + " ")
    for line in changelog.read_text().splitlines():
        if pattern.match(line):
            return True
    return False


# The three heading shapes the awk strips, applied in order to the same line.
# NOTE (faithful bug-for-bug): `[0-9.]+` does not match a pre-release version
# such as `0.2.0-beta.1`, so for those none of the three substitutions fire and
# the ENTIRE heading line becomes the "subtitle" — the tag message reads
# `v0.2.0-beta.1: ## 0.2.0-beta.1 — …`. Reproduced deliberately; see the report.
_SUB_PATTERNS = (
    re.compile(r"^## [0-9.]+ \(unreleased\) — "),
    re.compile(r"^## [0-9.]+ — "),
    re.compile(r"^## [0-9.]+ "),
)


def changelog_subtitle(version: str, changelog: Path = CHANGELOG) -> str:
    """The `## <version> ` heading with its version prefix stripped.

    With the current CHANGELOG shape (`## 0.24.0 — 2026-07-29`) this yields the
    DATE, so the default tag message is `v0.24.0: 2026-07-29`. That is what the
    bash produces today; pass `--message` for a real summary.
    """
    pattern = re.compile(r"^## " + re.escape(version) + " ")
    for line in changelog.read_text().splitlines():
        if not pattern.match(line):
            continue
        text = line
        for sub in _SUB_PATTERNS:
            text = sub.sub("", text, count=1)
        # `$(awk …)` strips trailing newlines but NOT trailing spaces, and the
        # emptiness test below is `-n`, so a space-only subtitle stays truthy.
        return text
    return ""


# ── the gate's verdict ─────────────────────────────────────────────────────────
def binary_is_tree(stamp: object, sha: str) -> bool:
    """Is a binary stamped `stamp` (the commit `rivet --version` prints) built from the commit `sha`?"""
    return isinstance(stamp, str) and stamp != "" and sha.startswith(stamp)


def gate_refusal(sha: str, record: Path, kind: str = FULL_GATE) -> str | None:
    """Why `sha` may not be tagged by the `kind` runs in `record`, or None when the latest such run is RELEASE-READY on a clean tree with a binary built from `sha`, under the canonical configuration."""
    if not record.is_file():
        return f"no gate record at {record}"
    runs, other = [], 0
    for line in record.read_text().splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if not isinstance(rec, dict) or rec.get("sha") != sha:
            continue
        if rec.get("gate") == kind:
            runs.append(rec)
        else:
            other += 1
    if not runs:
        also = f" ({other} run(s) of other kinds, which cannot support a tag)" if other else ""
        return f"no {kind} run of {sha[:12]} in {record}{also}"
    last = runs[-1]
    if last.get("dirty") is not False:
        return f"the latest {kind} run of {sha[:12]} ({last.get('at')}) graded a tree with uncommitted changes"
    if last.get("verdict") != "RELEASE-READY":
        return f"the latest {kind} run of {sha[:12]} ({last.get('at')}) says {last.get('verdict')}"
    if not binary_is_tree(last.get("binary"), sha):
        return (f"the latest {kind} run of {sha[:12]} ({last.get('at')}) graded a binary stamped "
                f"{last.get('binary')!r}, not this commit")
    if last.get("config") != CANONICAL_CONFIG:
        return (f"the latest {kind} run of {sha[:12]} ({last.get('at')}) ran under {last.get('config')!r}, "
                f"not the canonical configuration {CANONICAL_CONFIG!r}")
    return None


def island_refusal(sha: str, record: Path) -> str | None:
    """Why `sha` may not be tagged on the measured island's account: no clean, canonical, green `release-oracle-island` run of exactly this commit."""
    return gate_refusal(sha, record, ISLAND_GATE)


def recall_refusal(sha: str, record: Path | None) -> str | None:
    """Why `sha` may not be tagged on the seeded recall's account: no complete green recall of exactly this commit from a clean tree."""
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from dev.seeded import run_record

    return run_record.refusal(sha, record or run_record.seeded_dir() / run_record.RUNS_FILE)


def tag_refusal(sha: str, gate: Path, recall: Path | None, island: Path | None = None) -> tuple[str, str] | None:
    """The first record precondition `sha` fails, as (why, hint), in the order gate record, recall record, island record; None when all hold."""
    why = gate_refusal(sha, gate)
    if why:
        return why, ("run `make release-oracle-full` on this commit and tag it once the gate says RELEASE-READY; "
                     "pass --gate-record FILE when the gate ran on another machine")
    why = recall_refusal(sha, recall)
    if why:
        return why, ("a tag needs a complete green seeded recall of this commit; "
                     "pass --recall-record FILE when it ran on another machine")
    why = island_refusal(sha, island or gate)
    if why:
        return why, ("a tag needs the measured island green on this commit: run `make release-oracle-island`; "
                     "pass --island-record FILE when it ran on another machine")
    return None


def self_test() -> int:
    """The gate-record precondition on fixture lines: only the latest clean full-gate green run of this exact commit passes."""
    import tempfile

    sha, other = "a" * 40, "b" * 40

    def rec(**kw) -> str:
        base = {"at": "t", "sha": sha, "binary": sha[:9], "gate": FULL_GATE, "config": CANONICAL_CONFIG, "dirty": False, "verdict": "RELEASE-READY"}
        return json.dumps({**base, **kw})

    cases = [
        ([], "no gate record"),
        ([rec(sha=other)], "no release-oracle-full run"),
        ([rec(sha=sha[:8])], "no release-oracle-full run"),
        ([rec(gate="release-oracle")], "1 run(s) of other kinds"),
        ([rec(verdict="NOT RELEASABLE")], "says NOT RELEASABLE"),
        ([rec(verdict="NOT GRADED")], "says NOT GRADED"),
        ([rec(dirty=True)], "uncommitted changes"),
        ([rec(dirty=None)], "uncommitted changes"),
        ([rec(binary=other[:9])], "graded a binary stamped"),
        ([rec(binary="")], "graded a binary stamped"),
        ([rec(binary=None)], "graded a binary stamped"),
        ([rec(binary="unknown")], "graded a binary stamped"),
        ([rec(binary=sha[:7])], None),
        ([rec(config=None)], "not the canonical configuration"),
        ([rec(config={"bless": ["gifs"], "knobs": {}, "state": ["postgres", "sqlite"]})], "not the canonical configuration"),
        ([rec(config={"bless": [], "knobs": {"RIVET_PERF_WALL_TOL": "10"}, "state": ["postgres", "sqlite"]})], "not the canonical configuration"),
        ([rec(config={"bless": [], "knobs": {}, "state": ["sqlite"]})], "not the canonical configuration"),
        ([rec(), rec(verdict="NOT RELEASABLE")], "says NOT RELEASABLE"),
        (["not json", rec()], None),
        ([rec(verdict="NOT RELEASABLE"), rec(), rec(gate="release-oracle", verdict="NOT RELEASABLE")], None),
    ]
    with tempfile.TemporaryDirectory() as d:
        for i, (lines, want) in enumerate(cases):
            path = Path(d) / f"{i}.jsonl"
            if lines:
                path.write_text("\n".join(lines) + "\n")
            got = gate_refusal(sha, path)
            assert (got is None) if want is None else (got is not None and want in got), (i, lines, got)
    def run(**kw) -> str:
        base = {"sha": sha, "complete": True, "green": True, "dirty": False}
        return json.dumps({**base, **kw})

    recall_cases = [
        ([], "no seeded-recall record"),
        ([run(sha=other)], "no complete green"),
        ([run(complete=False)], "1 incomplete, red or dirty"),
        ([run(green=False)], "1 incomplete, red or dirty"),
        ([run(dirty=True)], "1 incomplete, red or dirty"),
        ([run(dirty=None)], "1 incomplete, red or dirty"),
        (["not json", run()], None),
        ([run(complete=False), run()], None),
    ]
    with tempfile.TemporaryDirectory() as d:
        for i, (lines, want) in enumerate(recall_cases):
            path = Path(d) / f"r{i}.jsonl"
            if lines:
                path.write_text("\n".join(lines) + "\n")
            got = recall_refusal(sha, path)
            assert (got is None) if want is None else (got is not None and want in got), (i, lines, got)
    isl = lambda **kw: rec(gate=ISLAND_GATE, **kw)  # noqa: E731
    island_cases = [
        ([], "no gate record"),
        ([isl(sha=other)], "no release-oracle-island run"),
        ([rec()], "1 run(s) of other kinds"),
        ([isl(verdict="NOT RELEASABLE")], "says NOT RELEASABLE"),
        ([isl(), isl(verdict="NOT RELEASABLE")], "says NOT RELEASABLE"),
        ([isl(dirty=True)], "uncommitted changes"),
        ([isl(binary=other[:9])], "graded a binary stamped"),
        ([isl(config={"bless": [], "knobs": {"RIVET_PERF_WALL_TOL": "10"}, "state": ["postgres", "sqlite"]})], "not the canonical configuration"),
        ([rec(gate="release-oracle")], "1 run(s) of other kinds"),
    ]
    with tempfile.TemporaryDirectory() as d:
        gate, good, bad = Path(d) / "gate.jsonl", Path(d) / "good.jsonl", Path(d) / "bad.jsonl"
        gate.write_text(rec() + "\n")
        good.write_text(run() + "\n")
        bad.write_text(run(complete=False) + "\n")
        island = Path(d) / "island.jsonl"
        island.write_text(rec(gate=ISLAND_GATE) + "\n")
        assert tag_refusal(sha, gate, good, island) is None
        gate.write_text(rec() + "\n" + rec(gate=ISLAND_GATE) + "\n")
        assert tag_refusal(sha, gate, good) is None, "one record file holds both kinds"
        gate.write_text(rec() + "\n")
        narrowed, ungated = tag_refusal(sha, gate, bad, island), tag_refusal(sha, Path(d) / "none.jsonl", good, island)
        assert narrowed and "seeded recall" in narrowed[1], "a gate-green sha with a narrowed recall must be refused by the tag path"
        assert ungated and "release-oracle-full" in ungated[1], "a recall-green sha with no gate record must be refused by the tag path"
        unmeasured = tag_refusal(sha, gate, good)
        assert unmeasured and "release-oracle-island" in unmeasured[1], "a gate- and recall-green sha with no island run must be refused by the tag path"
        for i, (lines, want) in enumerate(island_cases):
            path = Path(d) / f"i{i}.jsonl"
            if lines:
                path.write_text("\n".join(lines) + "\n")
            got = tag_refusal(sha, gate, good, path)
            assert got is not None and want in got[0] and "release-oracle-island" in got[1], (i, lines, got)
    print(f"self-test ok: release gate record — {len(cases)} cases, only a clean green full-gate run of this commit tags; "
          f"{len(recall_cases)} recall cases, only a complete green recall of it; {len(island_cases)} island cases")
    return 0


# ── git ────────────────────────────────────────────────────────────────────────
def _git(*args: str) -> shell.Proc:
    return shell.run(["git", "-C", str(ROOT), *args])


def _git_stream(*args: str) -> shell.Proc:
    """For the two mutating commands, so `git push`'s progress stays visible."""
    return shell.stream(["git", "-C", str(ROOT), *args])


def tag_exists(tag: str) -> bool:
    """`git rev-parse <tag>` — as in the bash, this resolves ANY ref or object,
    so a branch named `v0.24.0` also counts as "already exists". Kept: the
    over-broad refusal blocks a release, it never lets a bad one through."""
    return _git("rev-parse", tag).ok


def worktree_dirty() -> bool:
    """Unstaged or staged changes. Untracked files are NOT considered dirty —
    same as the bash, and defensible (nothing untracked ends up in the tag), but
    worth knowing: a new-but-unadded source file will not block a release."""
    return not _git("diff", "--quiet").ok or not _git("diff", "--cached", "--quiet").ok


# ── main ───────────────────────────────────────────────────────────────────────
def main_cli(argv: list[str] | None = None) -> int:
    if list(sys.argv[1:] if argv is None else argv) == ["--self-test"]:
        return self_test()
    try:
        args = parse_args(list(sys.argv[1:] if argv is None else argv))
    except _Usage:
        print(USAGE)
        return 1

    # `git` was never checked in the bash — it would have died at the first call
    # with a shell "command not found". Same refusal, named.
    shell.require("git", hint="install git, then re-run")

    version = cargo_version()
    if not version:
        raise shell.Fail("error: could not parse version from Cargo.toml")

    if args.tag_override:
        tag = args.tag_override
        if tag.startswith("v"):
            tag_version = tag[1:]
        else:
            raise shell.Fail(f"error: --tag must look like vX.Y.Z (got: {tag})")
        # The tag and the crate version must agree: the workflow builds from the
        # tagged tree and names every asset after the TAG, while crates.io
        # publishes the Cargo.toml VERSION. A mismatch ships a formula whose
        # URLs and crate version disagree.
        if tag_version != version:
            raise shell.Fail(
                f"error: Cargo.toml version is {version}, but --tag is {tag}"
            )
    else:
        tag = f"v{version}"

    if not CHANGELOG.is_file():
        raise shell.Fail("error: CHANGELOG.md not found")

    if not changelog_has_section(version):
        raise shell.Fail(
            f"error: CHANGELOG.md has no section starting with '## {version} '"
        )

    if tag_exists(tag):
        raise shell.Fail(f"error: tag {tag} already exists locally")

    if worktree_dirty():
        raise shell.Fail(
            "error: working tree is not clean — commit or stash before tagging"
        )

    sha = _git("rev-parse", "HEAD").check("git rev-parse HEAD").stdout.strip()
    record = Path(args.gate_record) if args.gate_record else GATE_RECORD
    refused = tag_refusal(sha, record, Path(args.recall_record) if args.recall_record else None,
                          Path(args.island_record) if args.island_record else None)
    if refused:
        raise shell.Fail(f"error: {refused[0]}", hint=refused[1])
    head = sha[:8]
    branch = (
        _git("rev-parse", "--abbrev-ref", "HEAD")
        .check("git rev-parse --abbrev-ref HEAD")
        .stdout.strip()
    )

    message = args.message
    if not message:
        subtitle = changelog_subtitle(version)
        message = f"{tag}: {subtitle}" if subtitle else tag

    print(
        f"""Rivet release
  tag:     {tag}
  version: {version}
  commit:  {head} ({branch})
  gate:    {FULL_GATE} RELEASE-READY ({record})
  recall:  complete green seeded recall of this commit
  island:  {ISLAND_GATE} RELEASE-READY
  message: {message}
  push:    {"yes" if args.push else "no"}
  CI:      .github/workflows/release.yml (on tag push)"""
    )

    if args.dry_run:
        print()
        print("Dry run — no tag created.")
        return 0

    _git_stream("tag", "-a", tag, "-m", message).check(f"git tag -a {tag}")
    print(f"Created tag {tag}")

    if args.push:
        # The point of no return: the workflow starts here.
        _git_stream("push", "origin", tag).check(f"git push origin {tag}")
        print(f"Pushed {tag} — watch: {ACTIONS_URL}")
    else:
        print()
        print(f"Next: git push origin {tag}")

    return 0


if __name__ == "__main__":
    shell.main(lambda: main_cli())
