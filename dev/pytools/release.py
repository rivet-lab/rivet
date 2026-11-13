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
   under the same rules as 6, env-skip rule included.
9. The gate host's logs directory (or `--satellite-record DIR`) holds, for each satellite
   profile, a `verdict.json` of exactly HEAD's commit (`dev/pytools/satellite.py`) whose
   `blocking` list is empty.

10. `--graded-sha A` reads 6-9 for the earlier commit A instead of HEAD, and requires HEAD to descend
   from A, differ only under the harness paths, and build a release binary (`--binary`) whose SHA-256
   equals the `binary_sha256` the latest full-gate and island runs of A recorded.

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
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

if __package__:
    from . import evidence, shell
else:  # executed as a plain script: `python3 dev/pytools/release.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import evidence  # type: ignore[no-redef]
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from dev.pytools import satellite  # noqa: E402
from dev.release_oracle.core import FULL_GATE, ISLAND_GATE, TAG_GATES, file_sha256, rivet_bin  # noqa: E402

CARGO_TOML = ROOT / "Cargo.toml"
CHANGELOG = ROOT / "CHANGELOG.md"

USAGE = """Usage: python3 -m dev.pytools.release [--dry-run] [--push] [--tag vX.Y.Z] [--message TEXT] [--gate-record FILE] [--recall-record FILE] [--island-record FILE] [--satellite-record DIR] [--graded-sha SHA] [--binary FILE]

Options:
  --dry-run     Validate only; do not create or push a tag
  --push        Push the tag to origin after creating it
  --tag vX.Y.Z  Override tag (default: v<version from Cargo.toml>)
  --message     Annotated tag message (default: vX.Y.Z: <CHANGELOG subtitle>)
  --gate-record The gate's verdict lines (default: dev/release-oracle/timings.jsonl)
  --recall-record  The seeded recall's run lines (default: recall-runs.jsonl in the seeded state dir)
  --island-record  The measured island's verdict lines (default: the gate record)
  --satellite-record  The gate's logs directory holding <sha>/satellite-<profile>/verdict.json (default: $RIVET_GATE_LOGS_DIR or ~/wt/gate-logs)
  --graded-sha  Tag HEAD on the account of the gates of this earlier commit: HEAD must descend from it, differ from it only under harness paths, and build the byte-identical release binary
  --binary      The release binary built from HEAD for --graded-sha (default: target/release/rivet under $CARGO_TARGET_DIR); build it with RIVET_GIT_SHA set to the graded binary's stamp
  --self-test   Check the gate-record precondition on fixtures; tags nothing
  -h, --help    Show this help"""

# The bash hard-codes this owner in the "watch:" line even though the Homebrew
# generator derives owner/repo from Cargo.toml. Kept verbatim so the transcript
# diffs clean; flagged rather than silently "improved".
ACTIONS_URL = "https://github.com/rivet-lab/rivet/actions/workflows/release.yml"
#: Where `make release-oracle-full` appends its verdict line (dev/release_oracle/core.py TIMINGS_HISTORY).
GATE_RECORD = ROOT / "dev" / "release-oracle" / "timings.jsonl"
#: The satellite profiles a tag needs a verdict of (`Run(<profile>, ...)` in dev/pytools/satellite.py; the self-test compares).
SATELLITE_PROFILES = ("linux", "mac")
#: The configuration a full gate runs under: no bless mode, no tolerance override, both state backends graded.
CANONICAL_CONFIG: dict = {"bless": [], "knobs": {}, "state": ["postgres", "sqlite"]}
#: Path prefixes and files whose change cannot alter what ships beside the binary: harness, ledgers, tests, docs, changelog fragments.
HARNESS_PREFIXES = ("dev/", "docs/", "tests/", "changelog.d/", ".githooks/")
HARNESS_FILES = ("CHANGELOG.md", ".cargo/mutants.toml")


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
    satellite_record: str = ""
    graded_sha: str = ""
    binary: str = ""


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
        elif arg == "--satellite-record":
            value = argv[i + 1] if i + 1 < len(argv) else ""
            if not value:
                raise _Usage()
            a.satellite_record = value
            i += 2
        elif arg == "--graded-sha":
            value = argv[i + 1] if i + 1 < len(argv) else ""
            if not value:
                raise _Usage()
            a.graded_sha = value
            i += 2
        elif arg == "--binary":
            value = argv[i + 1] if i + 1 < len(argv) else ""
            if not value:
                raise _Usage()
            a.binary = value
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


def gate_runs(sha: str, record: Path, kind: str) -> tuple[list[dict], int]:
    """The `kind` runs of exactly `sha` in `record`, oldest first, and the count of its runs of other kinds."""
    runs, other = [], 0
    for line in record.read_text().splitlines() if record.is_file() else []:
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
    return runs, other


def gate_refusal(sha: str, record: Path, kind: str = FULL_GATE) -> str | None:
    """Why `sha` may not be tagged by the `kind` runs in `record`, or None when the latest such run is RELEASE-READY on a clean tree with a binary built from `sha`, under the canonical configuration."""
    if not record.is_file():
        return f"no gate record at {record}"
    runs, other = gate_runs(sha, record, kind)
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
    if kind in TAG_GATES and (why := env_skip_refusal(last.get("skipped_env"))):
        return f"the latest {kind} run of {sha[:12]} ({last.get('at')}) {why}"
    return None


def env_skip_refusal(ids: object) -> str | None:
    """Why a run's recorded env skips bar a tag: no record of them, or a cell no excuse in skip_allow.py covers today."""
    if not (isinstance(ids, list) and all(isinstance(i, list) and len(i) == 3 and all(isinstance(x, str) for x in i) for i in ids)):
        return "recorded no skipped_env list, so its env skips cannot be checked: run the gate again"
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from dev.release_oracle import skip_allow

    open_ = ["/".join(i) for i in ids if skip_allow.excused(*i) is None]
    return f"skipped {len(open_)} cell(s) for the environment with no excuse in skip_allow.py: {', '.join(open_)}" if open_ else None


def outside_harness(paths: list[str]) -> list[str]:
    """The paths that are not harness, ledger, test, docs or changelog paths."""
    return [p for p in paths if not (p in HARNESS_FILES or p.startswith(HARNESS_PREFIXES))]


def changed_between(graded: str, head: str, root: Path = ROOT) -> list[str] | None:
    """Every path `graded..head` adds, changes, deletes or renames (both ends), or None when `graded` is not an ancestor of `head`."""
    git = lambda *a: subprocess.run(["git", "-C", str(root), *a], capture_output=True, text=True)  # noqa: E731
    if git("merge-base", "--is-ancestor", graded, head).returncode != 0:
        return None
    out = git("diff", "--name-only", "--no-renames", "-z", graded, head)
    return [p for p in out.stdout.split("\0") if p] if out.returncode == 0 else None


def reuse_refusal(graded: str, head: str, changed: list[str] | None, built: str, gate: Path, island: Path) -> str | None:
    """Why `head` may not be tagged on the gates of the earlier commit `graded`: not its descendant, a diff outside the harness paths, or a built binary whose SHA-256 is not the one the latest full-gate and island runs of `graded` recorded."""
    if changed is None:
        return f"{graded[:12]} is not an ancestor of {head[:12]}"
    if outside := outside_harness(changed):
        return f"{graded[:12]}..{head[:12]} touches {len(outside)} path(s) outside the harness paths: {', '.join(outside[:5])}"
    for kind, record in ((FULL_GATE, gate), (ISLAND_GATE, island)):
        runs, _ = gate_runs(graded, record, kind)
        if not runs:
            return f"no {kind} run of {graded[:12]} to take a binary SHA-256 from"
        recorded = runs[-1].get("binary_sha256")
        if not (isinstance(recorded, str) and re.fullmatch(r"[0-9a-f]{64}", recorded)):
            return f"the latest {kind} run of {graded[:12]} recorded no binary SHA-256, so its binary cannot be compared: run the gate again"
        if recorded != built:
            return (f"the latest {kind} run of {graded[:12]} graded a binary with SHA-256 {recorded[:12]}, "
                    f"the one built from {head[:12]} is {built[:12] or 'missing'}")
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


def satellite_logs() -> Path:
    """The gate host's logs directory the satellites publish into: `RIVET_GATE_LOGS_DIR` (else `wt/gate-logs`), relative to the home directory."""
    return Path.home() / os.environ.get("RIVET_GATE_LOGS_DIR", "wt/gate-logs")


def satellite_refusal(sha: str, logs: Path) -> str | None:
    """Why `sha` may not be tagged on the satellites' account: a profile with no `verdict.json` of exactly this commit under `logs`, or whose latest one lists a blocking check or ran no pilot or release-notes check."""
    found: dict[str, list[dict]] = {}
    for path in logs.glob("*/satellite-*/verdict.json"):
        try:
            body = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if isinstance(body, dict) and body.get("sha") == sha and body.get("profile") in SATELLITE_PROFILES:
            found.setdefault(body["profile"], []).append(body)
    for profile in SATELLITE_PROFILES:
        if not found.get(profile):
            return f"no satellite {profile} verdict of {sha[:12]} under {logs}"
        last = max(found[profile], key=lambda b: str(b.get("started")))
        at, blocking, checks = last.get("started"), last.get("blocking"), last.get("checks")
        if not isinstance(blocking, list):
            return f"the latest satellite {profile} verdict of {sha[:12]} ({at}) lists no `blocking` array"
        if blocking:
            return f"the latest satellite {profile} verdict of {sha[:12]} ({at}) blocks the release ({len(blocking)}): {blocking[0]}"
        names = [c.get("name") for c in checks if isinstance(c, dict)] if isinstance(checks, list) else []
        if not any(isinstance(n, str) and n.startswith(satellite.HARD) for n in names):
            return f"the latest satellite {profile} verdict of {sha[:12]} ({at}) ran none of the pilot or release-notes checks"
    return None


def tag_refusal(sha: str, gate: Path, recall: Path | None, island: Path | None = None,
                satellite_dir: Path | None = None) -> tuple[str, str] | None:
    """The first record precondition `sha` fails, as (why, hint), in the order gate record, recall record, island record, satellite verdicts; None when all hold."""
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
    why = satellite_refusal(sha, satellite_dir or satellite_logs())
    if why:
        return why, ("a tag needs both satellite verdicts of this commit with nothing blocking: "
                     "`python3 dev/pytools/satellite.py remote --sha SHA --publish` and `... mac --sha SHA --repo DIR --publish`; "
                     "pass --satellite-record DIR when they were published to another machine")
    return None


def graded_refusal(sha: str, graded: str, changed: list[str] | None, built: str, gate: Path, recall: Path | None,
                   island: Path | None = None, satellite_dir: Path | None = None) -> tuple[str, str] | None:
    """`tag_refusal` of the records of `graded`, after `reuse_refusal` when `graded` is not `sha` itself."""
    if graded != sha and (why := reuse_refusal(graded, sha, changed, built, gate, island or gate)):
        return why, ("tagging a descendant of the graded commit needs a harness-only diff and the byte-identical release binary; "
                     "otherwise run `make release-oracle-full` on this commit")
    return tag_refusal(graded, gate, recall, island, satellite_dir)


def reuse_self_test(sha: str, rec, run, write_satellites) -> int:  # noqa: ANN001
    """The reuse of a graded commit's gates on a real scratch repository and on fixture records: each guard refuses its own mutant."""
    import tempfile
    from unittest import mock

    other = "b" * 40
    digest, differing = "c" * 64, "d" * 64
    with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {}, clear=False):
        for key in [k for k in os.environ if k.startswith("GIT_")]:
            del os.environ[key]
        root = Path(d) / "repo"
        git = lambda *a: subprocess.run(["git", "-C", str(root), "-c", "user.name=t", "-c", "user.email=t@t", *a], capture_output=True, text=True, check=True).stdout.strip()  # noqa: E731
        for rel in ("src/main.rs", "tests/t.rs", "docs/d.md", "dev/x.py", "Cargo.toml", ".github/workflows/release.yml", "CHANGELOG.md"):
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text("1\n")
        git("init", "-q", "-b", "main")
        git("add", "-A")
        git("commit", "-q", "-m", "A")
        a = git("rev-parse", "HEAD")

        def commit(*edits: str, move: tuple[str, str] | None = None) -> str:
            git("checkout", "-q", "-B", "work", a)
            for rel in edits:
                (root / rel).write_text("2\n")
            if move:
                (root / move[1]).parent.mkdir(parents=True, exist_ok=True)
                git("mv", *move)
            git("add", "-A")
            git("commit", "-q", "-m", "B")
            return git("rev-parse", "HEAD")

        harness = commit("tests/t.rs", "docs/d.md", "dev/x.py", "CHANGELOG.md")
        assert sorted(changed_between(a, harness, root) or []) == ["CHANGELOG.md", "dev/x.py", "docs/d.md", "tests/t.rs"]
        assert outside_harness(changed_between(a, harness, root) or []) == []
        assert changed_between(a, a, root) == []
        for edits, move, bad in (
            (("src/main.rs",), None, "src/main.rs"), (("Cargo.toml",), None, "Cargo.toml"),
            ((".github/workflows/release.yml",), None, ".github/workflows/release.yml"),
            (("tests/t.rs",), ("src/main.rs", "tests/main.rs"), "src/main.rs"),
            ((), ("tests/t.rs", "src/t.rs"), "src/t.rs"),
        ):
            head = commit(*edits, move=move)
            assert bad in outside_harness(changed_between(a, head, root) or []), (edits, move, changed_between(a, head, root))
        git("checkout", "-q", "-B", "side", a)
        (root / "docs/d.md").write_text("3\n")
        git("commit", "-q", "-am", "side")
        git("checkout", "-q", "-B", "work2", a)
        git("commit", "-q", "--allow-empty", "-m", "C")
        assert changed_between(git("rev-parse", "side"), git("rev-parse", "work2"), root) is None, "a commit that is not an ancestor is never graded"

    def records(path: Path, full: dict | None = {}, island: dict | None = {}, extra: tuple[str, ...] = ()) -> Path:  # noqa: B006
        """A gate record of `sha`: the full-gate and island lines (None leaves one out) with `digest` unless overridden."""
        lines = [] if full is None else [rec(sha=sha, **{"binary_sha256": digest, **full})]
        lines += [] if island is None else [rec(sha=sha, gate=ISLAND_GATE, **{"binary_sha256": digest, **island})]
        path.write_text("\n".join([*lines, *extra]) + "\n")
        return path

    ok_paths = ["tests/t.rs", "docs/d.md"]
    cases = [
        ({}, {}, ok_paths, digest, None),
        ({}, {}, [*ok_paths, "src/main.rs"], digest, "outside the harness paths: src/main.rs"),
        ({}, {}, [*ok_paths, "Cargo.lock"], digest, "outside the harness paths: Cargo.lock"),
        ({}, {}, ok_paths, differing, "graded a binary with SHA-256 cccccccccccc"),
        ({}, {}, ok_paths, "", "the one built from"),
        ({}, {}, ok_paths, "C" * 64, "graded a binary with SHA-256"),
        ({"binary_sha256": None}, {}, ok_paths, digest, "recorded no binary SHA-256"),
        ({"binary_sha256": ""}, {}, ok_paths, digest, "recorded no binary SHA-256"),
        ({"binary_sha256": "abc"}, {}, ok_paths, digest, "recorded no binary SHA-256"),
        ({"binary_sha256": differing}, {}, ok_paths, digest, "graded a binary with SHA-256"),
        ({}, {"binary_sha256": None}, ok_paths, digest, "release-oracle-island run"),
        ({}, {"binary_sha256": differing}, ok_paths, digest, "release-oracle-island run"),
        (None, {}, ok_paths, digest, "no release-oracle-full run"),
        ({}, None, ok_paths, digest, "no release-oracle-island run"),
    ]
    with tempfile.TemporaryDirectory() as d:
        sats = Path(d) / "sats"
        write_satellites(sats, sha)
        good = Path(d) / "recall.jsonl"
        good.write_text(run() + "\n")
        for i, (full, island, changed, built, want) in enumerate(cases):
            path = records(Path(d) / f"g{i}.jsonl", full, island)
            got = graded_refusal(other, sha, changed, built, path, good, None, sats)
            assert (got is None) if want is None else (got is not None and want in got[0]), (i, full, island, changed, built, got)
        path = records(Path(d) / "latest.jsonl", extra=(rec(sha=sha, binary_sha256=differing),))
        got = graded_refusal(other, sha, ok_paths, digest, path, good, None, sats)
        assert got and "graded a binary with SHA-256" in got[0], "the latest run decides, not an older one that matches"
        path = records(Path(d) / "older.jsonl", extra=(rec(sha=sha, binary_sha256=digest, verdict="NOT RELEASABLE"),))
        got = graded_refusal(other, sha, ok_paths, digest, path, good, None, sats)
        assert got and "says NOT RELEASABLE" in got[0], "a red latest run of the graded commit still refuses"
        path = records(Path(d) / "chain.jsonl")
        got = graded_refusal(other, sha, None, digest, path, good, None, sats)
        assert got and "is not an ancestor" in got[0], "a graded commit that is not an ancestor is refused"
        bad = Path(d) / "bad.jsonl"
        bad.write_text(run(complete=False) + "\n")
        got = graded_refusal(other, sha, ok_paths, digest, path, bad, None, sats)
        assert got and "seeded recall" in got[1], "the graded commit's recall is still required"
        got = graded_refusal(other, sha, ok_paths, digest, path, good, None, Path(d) / "no-sats")
        assert got and "satellite linux verdict" in got[0], "the graded commit's satellite verdicts are still required"
        assert graded_refusal(other, other, [], "", path, good, None, sats), "no --graded-sha reads the records of the commit itself"
        assert graded_refusal(sha, sha, [], "", path, good, None, sats) is None, "the graded commit itself needs no binary comparison"
    print(f"self-test ok: graded-commit reuse — {len(cases)} record cases, git diff paths, ancestry")
    return 0


def self_test() -> int:
    """The gate-record precondition on fixture lines: only the latest clean full-gate green run of this exact commit passes."""
    import tempfile

    sha, other = "a" * 40, "b" * 40

    def rec(**kw) -> str:
        base = {"at": "t", "sha": sha, "binary": sha[:9], "gate": FULL_GATE, "config": CANONICAL_CONFIG, "dirty": False, "verdict": "RELEASE-READY", "skipped_env": []}
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
        ([rec(skipped_env=[["load", "mongo", "gcs"]])], "no excuse in skip_allow.py: load/mongo/gcs"),
        ([rec(skipped_env=[["load", "mongo", "gcs"], ["x", "pg", "-"]])], "skipped 2 cell(s)"),
        ([rec(skipped_env=[["fixture", "pg", "-"]])], None),
        ([rec(skipped_env=[["expired", "pg", "-"]])], "no excuse in skip_allow.py: expired/pg/-"),
        ([rec(skipped_env=None)], "no skipped_env list"),
        ([rec(skipped_env=[["a", "b"]])], "no skipped_env list"),
        ([rec(skipped_env="load/mongo/gcs")], "no skipped_env list"),
        ([rec(skipped_env=[["a", "b", 3]])], "no skipped_env list"),
    ]
    from dev.release_oracle import skip_allow

    def sat(profile: str = "linux", **kw) -> dict:
        hard = "pilot-upgrade[postgres]" if profile == "linux" else "release-notes-assemble"
        return {"profile": profile, "sha": sha, "started": "2026-11-10T08:00:00+00:00", "overall": "PASS", "blocking": [],
                "checks": [{"name": "checkout", "status": "PASS"}, {"name": hard, "status": "PASS"}], **kw}

    def write_satellites(logs: Path, at: str, profiles: dict | None = None) -> None:
        for profile, verdicts in ({p: [sat(p)] for p in SATELLITE_PROFILES} if profiles is None else profiles).items():
            for n, v in enumerate(verdicts):
                where = logs / at[:8] / f"satellite-{profile}{n or ''}"
                where.mkdir(parents=True)
                (where / "verdict.json").write_text(json.dumps(v))

    kept = skip_allow.EXCUSES
    skip_allow.EXCUSES = (
        skip_allow.Excuse("fixture", "*", "*", "self-test fixture", "2999-01-01"),
        skip_allow.Excuse("expired", "*", "*", "self-test fixture", "2000-01-01"),
    )
    with tempfile.TemporaryDirectory() as d:
        for i, (lines, want) in enumerate(cases):
            path = Path(d) / f"{i}.jsonl"
            if lines:
                path.write_text("\n".join(lines) + "\n")
            got = gate_refusal(sha, path)
            assert (got is None) if want is None else (got is not None and want in got), (i, lines, got)
    skip_allow.EXCUSES = kept

    def run(**kw) -> str:
        base = {"sha": sha, "complete": True, "green": True, "dirty": False}
        return json.dumps({**base, **kw})

    recall_cases = [
        ([], "no seeded-recall record"),
        ([run(sha=other)], "no complete green"),
        ([run(complete=False)], "latest of 1 run(s)"),
        ([run(green=False)], "latest of 1 run(s)"),
        ([run(dirty=True)], "latest of 1 run(s)"),
        ([run(dirty=None)], "latest of 1 run(s)"),
        (["not json", run()], None),
        ([run(complete=False), run()], None),
        ([run(green=False), run()], None),
        ([run(), run(green=False)], "latest of 2 run(s)"),
        ([run(), run(complete=False)], "latest of 2 run(s)"),
        ([run(), run(dirty=True)], "latest of 2 run(s)"),
        ([run(), run(green=False), run(sha=other)], "latest of 2 run(s)"),
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
        ([isl(skipped_env=[["perf", "bigquery", "load"], ["flat_rss", "postgres", "batch"]])], "skipped 2 cell(s) for the environment"),
        ([isl(skipped_env=None)], "no skipped_env list"),
    ]
    with tempfile.TemporaryDirectory() as d:
        gate, good, bad = Path(d) / "gate.jsonl", Path(d) / "good.jsonl", Path(d) / "bad.jsonl"
        gate.write_text(rec() + "\n")
        good.write_text(run() + "\n")
        bad.write_text(run(complete=False) + "\n")
        island = Path(d) / "island.jsonl"
        island.write_text(rec(gate=ISLAND_GATE) + "\n")
        sats = Path(d) / "sats"
        write_satellites(sats, sha)
        assert tag_refusal(sha, gate, good, island, sats) is None
        gate.write_text(rec() + "\n" + rec(gate=ISLAND_GATE) + "\n")
        assert tag_refusal(sha, gate, good, None, sats) is None, "one record file holds both kinds"
        gate.write_text(rec() + "\n")
        narrowed, ungated = tag_refusal(sha, gate, bad, island, sats), tag_refusal(sha, Path(d) / "none.jsonl", good, island, sats)
        assert narrowed and "seeded recall" in narrowed[1], "a gate-green sha with a narrowed recall must be refused by the tag path"
        assert ungated and "release-oracle-full" in ungated[1], "a recall-green sha with no gate record must be refused by the tag path"
        unmeasured = tag_refusal(sha, gate, good, None, sats)
        assert unmeasured and "release-oracle-island" in unmeasured[1], "a gate- and recall-green sha with no island run must be refused by the tag path"
        for i, (lines, want) in enumerate(island_cases):
            path = Path(d) / f"i{i}.jsonl"
            if lines:
                path.write_text("\n".join(lines) + "\n")
            got = tag_refusal(sha, gate, good, path, sats)
            assert got is not None and want in got[0] and "release-oracle-island" in got[1], (i, lines, got)
        gate.write_text(rec() + "\n" + rec(gate=ISLAND_GATE) + "\n")
        unwitnessed = tag_refusal(sha, gate, good, None, Path(d) / "no-sats")
        assert unwitnessed and "satellite linux verdict" in unwitnessed[0] and "--satellite-record" in unwitnessed[1], unwitnessed
    satellite_cases = [
        ({}, "no satellite linux verdict"),
        ({"linux": [sat()]}, "no satellite mac verdict"),
        ({"mac": [sat("mac")]}, "no satellite linux verdict"),
        ({"linux": [sat(sha=other)], "mac": [sat("mac")]}, "no satellite linux verdict"),
        ({"linux": [sat()], "mac": [sat("mac", sha=other)]}, "no satellite mac verdict"),
        ({"linux": [sat(blocking=["pilot-upgrade[postgres]: FAIL: gap"])], "mac": [sat("mac")]}, "satellite linux verdict"),
        ({"linux": [sat()], "mac": [sat("mac", blocking=["release-notes-assemble: FAIL: x"])]}, "blocks the release"),
        ({"linux": [sat(blocking=None)], "mac": [sat("mac")]}, "no `blocking` array"),
        ({"linux": [sat(blocking="")], "mac": [sat("mac")]}, "no `blocking` array"),
        ({"linux": [sat(checks=[{"name": "checkout"}])], "mac": [sat("mac")]}, "ran none of the pilot"),
        ({"linux": [sat(checks=None)], "mac": [sat("mac")]}, "ran none of the pilot"),
        ({"linux": [sat(), sat(started="2026-11-10T09:00:00+00:00", blocking=["pilot-lease[mysql]: NOT-RUN: x"])], "mac": [sat("mac")]}, "blocks the release"),
        ({"linux": [sat(blocking=["pilot-lease[mysql]: NOT-RUN: x"]), sat(started="2026-11-10T09:00:00+00:00")], "mac": [sat("mac")]}, None),
        ({"linux": [sat(started="2026-11-10T09:00:00+00:00", blocking=["pilot-lease[mysql]: NOT-RUN: x"]), sat()], "mac": [sat("mac")]}, "blocks the release"),
        ({"linux": [sat(started="2026-11-10T09:00:00+00:00"), sat(blocking=["pilot-lease[mysql]: NOT-RUN: x"])], "mac": [sat("mac")]}, None),
        ({"linux": [sat(profile="mac")], "mac": [sat("mac")]}, "no satellite linux verdict"),
        ({"linux": [sat()], "mac": [sat("mac")]}, None),
    ]
    with tempfile.TemporaryDirectory() as d:
        for i, (profiles, want) in enumerate(satellite_cases):
            logs = Path(d) / f"s{i}"
            logs.mkdir()
            write_satellites(logs, sha, profiles)
            (logs / "junk" / "satellite-linux").mkdir(parents=True)
            (logs / "junk" / "satellite-linux" / "verdict.json").write_text("not json")
            got = satellite_refusal(sha, logs)
            assert (got is None) if want is None else (got is not None and want in got), (i, profiles, got)
        assert satellite_refusal(sha, Path(d) / "absent") is not None, "a missing logs directory is a refusal"
    parsed = parse_args(["--graded-sha", "abc", "--binary", "bin"])
    assert (parsed.graded_sha, parsed.binary) == ("abc", "bin")
    reuse_self_test(sha, rec, run, write_satellites)
    declared = set(re.findall(r'Run\("(\w+)", args\.sha', Path(satellite.__file__).read_text()))
    assert declared == set(SATELLITE_PROFILES), f"satellite.py runs profiles {sorted(declared)}, release.py requires {SATELLITE_PROFILES}"
    print(f"self-test ok: release gate record — {len(cases)} cases, only a clean green full-gate run of this commit tags; "
          f"{len(recall_cases)} recall cases, only a complete green recall of it; {len(island_cases)} island cases; "
          f"{len(satellite_cases)} satellite cases, only a blocking-free verdict of each profile for this commit")
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
    """A tracked file differs from HEAD, staged or not, the evidence ledger apart; untracked files do not count."""
    return evidence.tree_dirty(ROOT)


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
    island = Path(args.island_record) if args.island_record else None
    graded = sha
    if args.graded_sha:
        graded = _git("rev-parse", "--verify", f"{args.graded_sha}^{{commit}}").check(f"git rev-parse {args.graded_sha}").stdout.strip()
    reused = graded != sha
    refused = graded_refusal(sha, graded, changed_between(graded, sha) if reused else [],
                             file_sha256(Path(args.binary) if args.binary else rivet_bin()) if reused else "",
                             record, Path(args.recall_record) if args.recall_record else None, island,
                             Path(args.satellite_record) if args.satellite_record else None)
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
  gate:    {FULL_GATE} RELEASE-READY ({record}){f" of {graded[:8]}, binary byte-identical, harness-only diff" if reused else ""}
  recall:  complete green seeded recall of this commit
  island:  {ISLAND_GATE} RELEASE-READY
  satellites: {", ".join(SATELLITE_PROFILES)} verdicts of this commit, nothing blocking
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
