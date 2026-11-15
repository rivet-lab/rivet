"""The two-halves tag (`release.py --halves`): the gate verdict read from the hosted half and the devbox half instead of one `release-oracle-full` record.

    python3 -m dev.pytools.release_halves --self-test

The devbox half is a `release-oracle-full-devbox` line of the gate record. The hosted half is the `hosted-verdict-<ref>` artifact of one
run of `.github/workflows/gate-hosted.yml` on main, plus that run's own record (`run.json`) and job list (`jobs.json`) as GitHub
reports them. Each half builds its own binary from the sha, so the binaries' hashes are not compared; the sha, the clean tree, the
stamp, the run's origin, every slice and the stage coverage are. Every refusal starts with a stable `HALVES_*` code.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from dev.release_oracle.core import DEVBOX, HOSTED, HOSTED_SHARD_GATE, half_gate  # noqa: E402

WORKFLOW = ".github/workflows/gate-hosted.yml"
VERDICT_SCRIPT = ROOT / ".github" / "scripts" / "hosted_gate_verdict.py"
BRANCH = "main"
RUN_LIMIT = 30
#: The (system, machine) each half's gate must have run on (platform.system(), platform.machine()); owner 2026-11-14: the hosted half on a GitHub x86_64 Linux runner, the devbox half on the arm64 macOS devbox. Moving a half to another host changes this line.
HALF_HOST: dict[str, tuple[str, str]] = {HOSTED: ("Linux", "x86_64"), DEVBOX: ("Darwin", "arm64")}
Gh = Callable[[Sequence[str]], str]


def refuse(code: str, text: str) -> str:
    """One refusal: its stable code, then what is wrong."""
    return f"HALVES_{code}: {text}"


def verdict_script():  # noqa: ANN201
    """The module of `hosted_gate_verdict.py`, the one place that names the verdict record's kind and fields."""
    spec = importlib.util.spec_from_file_location("hosted_gate_verdict", VERDICT_SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def repo_slug(cargo_toml: Path | None = None) -> str:
    """`owner/name` of the repository `Cargo.toml` names."""
    text = (cargo_toml or ROOT / "Cargo.toml").read_text()
    m = re.search(r'^repository\s*=\s*"https://github\.com/([^/"]+/[^/"]+?)(?:\.git)?/?"', text, re.M)
    if not m:
        raise SystemExit("Cargo.toml names no github.com repository")
    return m[1]


def workflow_plan(path: Path | None = None) -> dict:
    """What the hosted workflow declares: its triggers, the name of its plan, slice and verdict jobs, and the name of the verdict artifact."""
    import yaml

    doc = yaml.safe_load((path or ROOT / WORKFLOW).read_text())
    jobs = doc["jobs"]
    uploads = [s["with"]["name"] for s in jobs["verdict"]["steps"] if str(s.get("uses", "")).startswith("actions/upload-artifact@")]
    if len(uploads) != 1:
        raise SystemExit(f"{WORKFLOW}: the verdict job must upload exactly one artifact, it uploads {uploads}")
    return {"events": sorted((doc.get("on") or doc.get(True)).keys()), "plan": jobs["plan"]["name"],
            "slice": jobs["slice"]["name"], "verdict": jobs["verdict"]["name"], "artifact": uploads[0]}


def fill(template: str, slice_: int | None = None) -> str:
    """A workflow name as GitHub renders it for a run on `BRANCH`: the slice, the ref name and the ref tag filled; any other expression stays, so no job matches it."""
    known = {"github.ref_name": BRANCH, "needs.plan.outputs.tag": BRANCH, **({"matrix.slice": str(slice_)} if slice_ is not None else {})}
    return re.sub(r"\$\{\{\s*([^}]*?)\s*\}\}", lambda m: known.get(m[1], m[0]), template)


def job_names(plan: dict, slices: int) -> list[str]:
    """The jobs one run with `slices` live slices must have: plan, slices 0..`slices`, verdict."""
    return [fill(plan["plan"]), *(fill(plan["slice"], i) for i in range(slices + 1)), fill(plan["verdict"])]


def stage_cuts(slices: int, stage_names: Callable[[Sequence[str]], list[str]]) -> tuple[list[str], dict[int, list[str]], list[str]]:
    """The stages of the whole gate, of each hosted slice, and of the devbox half, cut by the very selector a run uses."""
    whole = stage_names([])
    return whole, {i: stage_names(["--half", HOSTED, "--live-shard", f"{i}/{slices}"]) for i in range(slices + 1)}, stage_names(["--half", DEVBOX])


def default_stage_names(argv: Sequence[str]) -> list[str]:
    """`dev.release_oracle.__main__.stage_names`, imported when first needed (that module imports `release`)."""
    from dev.release_oracle.__main__ import stage_names

    return stage_names(argv)


def record_refusal(rec: dict | None, sha: str, kind: str, half: str, what: str) -> str | None:
    """Why one half record does not support a tag: no record, another commit, a dirty tree, a verdict short of RELEASE-READY, a binary of another commit, the wrong configuration, an unexcused env skip, no stage list, the wrong host."""
    from dev.pytools import release

    if rec is None:
        return refuse("SHA", f"{what} has no {kind} record of {sha[:12]}")
    if rec.get("sha") != sha or rec.get("gate") != kind:
        return refuse("SHA", f"{what} is a {rec.get('gate')!r} record of {str(rec.get('sha'))[:12]}, not {kind} of {sha[:12]}")
    if problem := release.run_problem(rec, sha, kind):
        return refuse(problem[0], f"{what} ({rec.get('at')}) {problem[1]}")
    ran = rec.get("stages")
    if not (isinstance(ran, list) and all(isinstance(n, str) for n in ran)):
        return refuse("STAGES", f"{what} recorded no stage list, so its coverage cannot be checked: run the gate again")
    host = rec.get("host")
    got = (host.get("system"), host.get("machine")) if isinstance(host, dict) else None
    return None if got == HALF_HOST[half] else refuse("HOST", f"{what} ran on {got!r}, the {half} half belongs on {HALF_HOST[half]!r}")


def stage_list_refusal(ran: Sequence[str], expected: Sequence[str], what: str) -> str | None:
    """Why the stages `what` ran are not exactly `expected`, each once."""
    if sorted(ran) == sorted(expected):
        return None
    return refuse("STAGES", f"{what} ran {len(ran)} stage(s) of {len(expected)}: missing {sorted(set(expected) - set(ran))}, "
                  f"unexpected {sorted(set(ran) - set(expected))}, repeated {sorted({n for n in ran if ran.count(n) > 1})}")


def coverage_refusal(whole: Sequence[str], devbox: Sequence[str], hosted: Sequence[str]) -> str | None:
    """Why the two halves together are not the whole gate: a stage in neither, in both, or outside the gate."""
    for name in whole:
        if name not in devbox and name not in hosted:
            return refuse("STAGE_MISSING", f"stage {name!r} was run by neither half")
        if name in devbox and name in hosted:
            return refuse("STAGE_IN_BOTH", f"stage {name!r} was run by both halves, whose verdicts for it are not kept apart")
    if extra := sorted((set(devbox) | set(hosted)) - set(whole)):
        return refuse("STAGE_UNKNOWN", f"the halves ran {extra}, which the gate does not have")
    return None


def devbox_refusal(sha: str, record: Path, ran: list[str]) -> str | None:
    """Why the devbox half in the gate record `record` does not support a tag; `ran` receives the stages it ran."""
    from dev.pytools import release

    runs, _ = release.gate_runs(sha, record, half_gate(DEVBOX))
    if why := record_refusal(runs[-1] if runs else None, sha, half_gate(DEVBOX), DEVBOX, f"the devbox half in {record}"):
        return why
    ran.extend(runs[-1]["stages"])
    return None


def load_json(path: Path) -> object:
    """The JSON file `path`, or None when it is absent or unreadable."""
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def provenance_refusal(verdict: dict, run: object, jobs: object, plan: dict, repo: str) -> str | None:
    """Why the verdict artifact is not the word of one finished, wholly green run of the hosted workflow on main of `repo`."""
    gh = verdict.get("github") if isinstance(verdict.get("github"), dict) else {}
    if not isinstance(run, dict) or not isinstance(jobs, dict) or not isinstance(jobs.get("jobs"), list):
        return refuse("RUN", "run.json or jobs.json is missing or not GitHub's answer")
    if str(run.get("id")) != str(gh.get("run_id")) or str(run.get("run_attempt")) != str(gh.get("run_attempt")):
        return refuse("RUN", f"the artifact is of run {gh.get('run_id')} attempt {gh.get('run_attempt')}, run.json is of {run.get('id')} attempt {run.get('run_attempt')}")
    if run.get("path") != WORKFLOW or gh.get("workflow_ref") != f"{repo}/{WORKFLOW}@refs/heads/{BRANCH}":
        return refuse("WORKFLOW", f"the run is of {run.get('path')!r} ({gh.get('workflow_ref')!r}), not {WORKFLOW} on {BRANCH}")
    if run.get("head_branch") != BRANCH or gh.get("ref") != f"refs/heads/{BRANCH}":
        return refuse("REF", f"the run started on {run.get('head_branch')!r} ({gh.get('ref')!r}), not {BRANCH}")
    repos = [(run.get("repository") or {}).get("full_name"), (run.get("head_repository") or {}).get("full_name"), gh.get("repository")]
    if repos != [repo] * 3:
        return refuse("REF", f"the run belongs to {repos}, not {repo} alone (a fork or another repository)")
    if run.get("event") not in plan["events"] or gh.get("event") != run.get("event"):
        return refuse("EVENT", f"the run's event is {run.get('event')!r} ({gh.get('event')!r}), the workflow declares {plan['events']}")
    if run.get("status") != "completed" or run.get("conclusion") != "success":
        return refuse("RUN", f"the run is {run.get('status')} with conclusion {run.get('conclusion')!r}, not completed with success")
    conclusions = {j.get("name"): j.get("conclusion") for j in jobs["jobs"] if isinstance(j, dict)}
    for name in job_names(plan, verdict["slices"]):
        if name not in conclusions:
            return refuse("SLICE_MISSING", f"the run has no job {name!r}")
        if conclusions[name] != "success":
            return refuse("SLICE_FAILED", f"job {name!r} concluded {conclusions[name]!r}")
    return None


def hosted_refusal(sha: str, hosted: Path, plan: dict, repo: str, ran: dict[int, list[str]]) -> str | None:
    """Why the hosted half in `hosted` (verdict.json, run.json, jobs.json) does not support a tag; `ran` receives the stages of each slice."""
    verdict = load_json(hosted / "verdict.json")
    if not isinstance(verdict, dict) or verdict.get("kind") != verdict_script().VERDICT_KIND or verdict.get("schema") != 1:
        return refuse("HOSTED", f"{hosted / 'verdict.json'} is not a schema 1 hosted verdict")
    if verdict.get("sha") != sha:
        return refuse("SHA", f"the hosted verdict is of {str(verdict.get('sha'))[:12]}, not {sha[:12]}")
    n, records = slices_of(verdict), verdict.get("records")
    if not (n and isinstance(records, list)):
        return refuse("SLICE_MISSING", "the hosted verdict names no live slices")
    by_slice = {r.get("slice"): r for r in records if isinstance(r, dict)}
    for i in range(n + 1):
        what, entry = f"hosted slice {i}/{n}", by_slice.get(i)
        if not entry or not isinstance(entry.get("record"), dict):
            return refuse("SLICE_MISSING", f"{what} has no record")
        if entry.get("exit_code") != 0:
            return refuse("SLICE_FAILED", f"{what} exited {entry.get('exit_code')!r}")
        rec = entry["record"]
        if rec.get("shard") != f"{i}/{n}":
            return refuse("SLICE_MISSING", f"{what} holds the record of shard {rec.get('shard')!r}")
        if why := record_refusal(rec, sha, HOSTED_SHARD_GATE, HOSTED, what):
            return why
        ran[i] = rec["stages"]
    return provenance_refusal(verdict, load_json(hosted / "run.json"), load_json(hosted / "jobs.json"), plan, repo)


def slices_of(verdict: object) -> int:
    """The live slice count a hosted verdict names, 0 when it names none."""
    n = verdict.get("slices") if isinstance(verdict, dict) else None
    return n if isinstance(n, int) and not isinstance(n, bool) and n >= 1 else 0


def halves_refusal(sha: str, gate_record: Path, hosted: Path, *, plan: dict | None = None, repo: str | None = None,
                   stage_names: Callable[[Sequence[str]], list[str]] = default_stage_names) -> str | None:
    """Why `sha` may not be tagged on the account of the two halves, or None when both are green on this commit, built from it, from the right runs, and together run every stage of the gate once."""
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        return refuse("SHA", f"the gate identity is a full 40-hex commit sha, got {sha!r}")
    n = slices_of(load_json(hosted / "verdict.json")) or 1
    try:
        whole, hosted_cut, devbox_cut = stage_cuts(n, stage_names)
    except SystemExit as e:
        return refuse("STAGE_UNPLACED", str(e))
    devbox: list[str] = []
    slices: dict[int, list[str]] = {}
    why = (devbox_refusal(sha, gate_record, devbox)
           or hosted_refusal(sha, hosted, plan or workflow_plan(), repo or repo_slug(), slices)
           or coverage_refusal(whole, devbox, [n for ran in slices.values() for n in ran]))
    if why:
        return why
    return (stage_list_refusal(devbox, devbox_cut, "the devbox half")
            or next((w for i, ran in sorted(slices.items()) if (w := stage_list_refusal(ran, hosted_cut[i], f"hosted slice {i}/{n}"))), None))


def run_gh(args: Sequence[str]) -> str:
    """The output of `gh ARGS`; a failure raises."""
    return subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout


def tag_refusal(sha: str, gate: Path, hosted: Path | None, gh: Gh = run_gh) -> str | None:
    """`halves_refusal` of the gate record `gate` and the hosted half in `hosted`, fetched with `gh` into a scratch directory when `hosted` is None."""
    if hosted is not None:
        return halves_refusal(sha, gate, hosted)
    with tempfile.TemporaryDirectory() as tmp:
        return fetch_hosted(sha, Path(tmp), repo_slug(), gh) or halves_refusal(sha, gate, Path(tmp))


def hosted_runs(repo: str, gh: Gh = run_gh) -> list[dict]:
    """The last `RUN_LIMIT` runs of the hosted workflow on main, newest first, as `gh run list` reports them."""
    return json.loads(gh(["run", "list", "--repo", repo, "--workflow", Path(WORKFLOW).name, "--branch", BRANCH, "--limit", str(RUN_LIMIT),
                          "--json", "databaseId,status,headSha,conclusion"]))


def run_records(repo: str, run_id: object, gh: Gh = run_gh) -> tuple[str, str]:
    """The run record and the job list of run `run_id` exactly as GitHub reports them."""
    base = f"repos/{repo}/actions/runs/{run_id}"
    return gh(["api", base]), gh(["api", f"{base}/jobs?filter=latest&per_page=100"])


def fetch_hosted(sha: str, out: Path, repo: str, gh: Gh = run_gh, run_id: object = None) -> str | None:
    """Download into `out` the newest hosted-workflow run on main (or run `run_id`) whose verdict artifact is of `sha`, with its run.json and jobs.json; the refusal when there is none."""
    artifact = fill(workflow_plan()["artifact"])
    try:
        runs = [{"databaseId": run_id, "status": "completed"}] if run_id else hosted_runs(repo, gh)
    except (subprocess.CalledProcessError, OSError, ValueError) as e:
        return refuse("HOSTED", f"could not list the runs of {WORKFLOW} with gh ({e}); pass --hosted-record DIR instead")
    for run in runs:
        if run.get("status") != "completed":
            continue
        got = out / str(run["databaseId"])
        try:
            gh(["run", "download", str(run["databaseId"]), "--repo", repo, "--name", artifact, "--dir", str(got)])
        except (subprocess.CalledProcessError, OSError):
            continue
        verdict = load_json(got / "hosted-verdict.json")
        if isinstance(verdict, dict) and verdict.get("sha") == sha:
            run_text, jobs_text = run_records(repo, run["databaseId"], gh)
            (out / "verdict.json").write_text(json.dumps(verdict))
            (out / "run.json").write_text(run_text)
            (out / "jobs.json").write_text(jobs_text)
            return None
    if run_id:
        return refuse("HOSTED", f"run {run_id} of {WORKFLOW} holds no artifact {artifact!r} with a hosted verdict of {sha[:12]}")
    return refuse("HOSTED", f"none of the last {RUN_LIMIT} completed runs of {WORKFLOW} on {BRANCH} in {repo} holds a hosted verdict of {sha[:12]}: dispatch `gate-hosted` with sha={sha}")


SHA = "a" * 40
REPO = "rivet-lab/rivet"
RUN_ID = 43625455951


def green(slices: int = 3, sha: str = SHA) -> dict:
    """A green pair of halves of `sha` as plain data: the devbox gate line and the hosted verdict, run and job list, with the stages the real selector cuts."""
    from dev.pytools import release

    whole, hosted_cut, devbox_cut = stage_cuts(slices, default_stage_names)
    base = {"at": "2026-11-14T10:00:00Z", "sha": sha, "binary": sha[:9], "config": release.CANONICAL_CONFIG, "dirty": False,
            "verdict": "RELEASE-READY", "skipped_env": []}
    plan = workflow_plan()
    records = [{"slice": i, "exit_code": 0, "record": {**base, "gate": HOSTED_SHARD_GATE, "shard": f"{i}/{slices}", "binary_sha256": f"{i:x}" * 64,
                                                      "stages": hosted_cut[i], "host": {"system": "Linux", "machine": "x86_64"}}} for i in range(slices + 1)]
    flow = {"run_id": str(RUN_ID), "run_attempt": "1", "repository": REPO, "ref": "refs/heads/main", "event": "workflow_dispatch",
            "workflow_ref": f"{REPO}/{WORKFLOW}@refs/heads/main"}
    return {"plan": plan, "whole": whole, "devbox": [{**base, "gate": half_gate(DEVBOX), "binary_sha256": "d" * 64, "stages": devbox_cut,
                                                      "host": {"system": "Darwin", "machine": "arm64"}}],
            "verdict": {"kind": verdict_script().VERDICT_KIND, "schema": 1, "sha": sha, "slices": slices, "github": flow, "records": records},
            "run": {"id": RUN_ID, "run_attempt": 1, "path": WORKFLOW, "head_branch": BRANCH, "event": "workflow_dispatch", "status": "completed",
                    "conclusion": "success", "repository": {"full_name": REPO}, "head_repository": {"full_name": REPO}},
            "jobs": {"jobs": [{"name": n, "conclusion": "success"} for n in job_names(plan, slices)]}}


def write(root: Path, d: dict) -> tuple[Path, Path]:
    """The gate record file and the hosted directory of the fixture `d`; a key set to None is not written."""
    gate, hosted = root / "timings.jsonl", root / "hosted"
    hosted.mkdir()
    gate.write_text("".join(json.dumps(line) + "\n" for line in d["devbox"]))
    for key, name in (("verdict", "verdict.json"), ("run", "run.json"), ("jobs", "jobs.json")):
        if d[key] is not None:
            (hosted / name).write_text(json.dumps(d[key]))
    return gate, hosted


def refusal_of(d: dict, sha: str = SHA) -> str | None:
    """`halves_refusal` of the fixture `d`, written to a scratch directory."""
    with tempfile.TemporaryDirectory() as tmp:
        gate, hosted = write(Path(tmp), d)
        return halves_refusal(sha, gate, hosted, plan=d["plan"], repo=REPO)


def drop(d: dict, slice_: int, stage: str) -> None:
    """Remove `stage` from the record of hosted slice `slice_`."""
    d["verdict"]["records"][slice_]["record"]["stages"].remove(stage)


def mutants() -> list[tuple[str, str, Callable[[dict], None]]]:
    """(name, the code its refusal carries, how it breaks the green fixture): one row per way a half can fail to support a tag."""
    rec = lambda d, i=0: d["verdict"]["records"][i]["record"]  # noqa: E731
    flow = lambda d: d["verdict"]["github"]  # noqa: E731
    return [
        ("devbox half of another commit", "SHA", lambda d: d["devbox"][0].update(sha="b" * 40)),
        ("hosted verdict of another commit", "SHA", lambda d: d["verdict"].update(sha="b" * 40)),
        ("hosted slice of another commit", "SHA", lambda d: rec(d, 2).update(sha="b" * 40)),
        ("no devbox line", "SHA", lambda d: d["devbox"].clear()),
        ("the full gate's line is not the devbox half's", "SHA", lambda d: d["devbox"][0].update(gate="release-oracle-full")),
        ("devbox tree dirty", "DIRTY", lambda d: d["devbox"][0].update(dirty=True)),
        ("devbox tree dirty unknown", "DIRTY", lambda d: d["devbox"][0].pop("dirty")),
        ("hosted tree dirty", "DIRTY", lambda d: rec(d, 1).update(dirty=True)),
        ("devbox binary of another commit", "BINARY", lambda d: d["devbox"][0].update(binary="c" * 9)),
        ("hosted binary of another commit", "BINARY", lambda d: rec(d, 3).update(binary="c" * 9)),
        ("devbox verdict red", "VERDICT", lambda d: d["devbox"][0].update(verdict="NOT RELEASABLE")),
        ("hosted verdict red", "VERDICT", lambda d: rec(d).update(verdict="NOT RELEASABLE")),
        ("a later red devbox run of the sha", "VERDICT", lambda d: d["devbox"].append({**d["devbox"][0], "verdict": "NOT RELEASABLE"})),
        ("non-canonical configuration", "CONFIG", lambda d: rec(d).update(config={"bless": ["local"], "knobs": {}, "state": ["postgres"]})),
        ("env skip nothing excuses", "ENV_SKIP", lambda d: rec(d, 1).update(skipped_env=[["no-such-engine", "9", "no-such-scenario"]])),
        ("hosted half run on the devbox", "HOST", lambda d: rec(d, 1).update(host={"system": "Darwin", "machine": "arm64"})),
        ("hosted half on an arm runner", "HOST", lambda d: rec(d, 1).update(host={"system": "Linux", "machine": "aarch64"})),
        ("devbox half run on Linux", "HOST", lambda d: d["devbox"][0].update(host={"system": "Linux", "machine": "x86_64"})),
        ("no host recorded", "HOST", lambda d: d["devbox"][0].pop("host")),
        ("no stage list", "STAGES", lambda d: d["devbox"][0].pop("stages")),
        ("hosted verdict missing", "HOSTED", lambda d: d.update(verdict=None)),
        ("hosted verdict of another kind", "HOSTED", lambda d: d["verdict"].update(kind="something-else")),
        ("run started on another branch", "REF", lambda d: d["run"].update(head_branch="feature")),
        ("artifact says another ref", "REF", lambda d: flow(d).update(ref="refs/pull/7/merge")),
        ("run of a fork", "REF", lambda d: d["run"].update(head_repository={"full_name": "someone/rivet"})),
        ("artifact of another repository", "REF", lambda d: flow(d).update(repository="someone/rivet")),
        ("run of another workflow", "WORKFLOW", lambda d: d["run"].update(path=".github/workflows/ci.yml")),
        ("artifact names another workflow ref", "WORKFLOW", lambda d: flow(d).update(workflow_ref=f"{REPO}/{WORKFLOW}@refs/heads/feature")),
        ("run of an event the workflow does not declare", "EVENT", lambda d: (d["run"].update(event="pull_request"), flow(d).update(event="pull_request"))),
        ("run not finished", "RUN", lambda d: d["run"].update(status="in_progress", conclusion=None)),
        ("run concluded failure", "RUN", lambda d: d["run"].update(conclusion="failure")),
        ("artifact of another run", "RUN", lambda d: flow(d).update(run_id="1")),
        ("artifact of another attempt", "RUN", lambda d: flow(d).update(run_attempt="2")),
        ("run record missing", "RUN", lambda d: d.update(run=None)),
        ("job list missing", "RUN", lambda d: d.update(jobs=None)),
        ("verdict names no live slices", "SLICE_MISSING", lambda d: (d["verdict"].update(slices=0), d["verdict"].update(records=d["verdict"]["records"][:1]), rec(d).update(shard="0/0"))),
        ("verdict holds no record list", "SLICE_MISSING", lambda d: d["verdict"].update(records=None)),
        ("slice record missing", "SLICE_MISSING", lambda d: d["verdict"]["records"].pop(2)),
        ("slice record empty", "SLICE_MISSING", lambda d: d["verdict"]["records"][1].update(record=None)),
        ("slice holds another shard's record", "SLICE_MISSING", lambda d: rec(d, 2).update(shard="1/3")),
        ("a slice job never ran", "SLICE_MISSING", lambda d: d["jobs"]["jobs"].pop(3)),
        ("the verdict job never ran", "SLICE_MISSING", lambda d: d["jobs"]["jobs"].pop()),
        ("slice gate exited non-zero", "SLICE_FAILED", lambda d: d["verdict"]["records"][3].update(exit_code=2)),
        ("slice gate exit unknown", "SLICE_FAILED", lambda d: d["verdict"]["records"][3].update(exit_code=None)),
        ("a slice job failed", "SLICE_FAILED", lambda d: d["jobs"]["jobs"][2].update(conclusion="failure")),
        ("a slice job cancelled", "SLICE_FAILED", lambda d: d["jobs"]["jobs"][1].update(conclusion="cancelled")),
        ("stage missing from the hosted half", "STAGE_MISSING", lambda d: drop(d, 0, "cdc harm")),
        ("stage missing from the devbox half", "STAGE_MISSING", lambda d: d["devbox"][0]["stages"].pop(0)),
        ("the live stage missing from every slice", "STAGE_MISSING", lambda d: [rec(d, i)["stages"].remove("live modules") for i in (1, 2, 3)] and None),
        ("stage run by both halves", "STAGE_IN_BOTH", lambda d: d["devbox"][0]["stages"].append("cdc harm")),
        ("hosted stage run by the devbox only", "STAGES", lambda d: (drop(d, 0, "cdc harm"), d["devbox"][0]["stages"].append("cdc harm"))),
        ("stage run twice in one record", "STAGES", lambda d: rec(d).update(stages=rec(d)["stages"] + ["cdc harm"])),
        ("stage the gate does not have", "STAGE_UNKNOWN", lambda d: d["devbox"][0]["stages"].append("a stage nobody wrote")),
        ("live stage in the stage-zero slice", "STAGES", lambda d: rec(d).update(stages=rec(d)["stages"] + ["live modules"])),
    ]


def fetch_self_test(good: dict) -> None:
    """`fetch_hosted` takes the newest completed main run whose artifact is of the sha, skips runs without one, and leaves what `halves_refusal` reads."""
    other = {**good["verdict"], "sha": "b" * 40}

    def fake(artifacts: dict[int, dict | None], status: dict[int, str]) -> Gh:
        def gh(args: Sequence[str]) -> str:
            if args[:2] == ["run", "list"]:
                assert "--branch" in args and args[args.index("--branch") + 1] == BRANCH and "--workflow" in args, args
                return json.dumps([{"databaseId": i, "status": status[i]} for i in artifacts])
            if args[:2] == ["run", "download"]:
                assert args[args.index("--name") + 1] == "hosted-verdict-main", args
                body = artifacts.get(int(args[2]))
                if body is None:
                    raise subprocess.CalledProcessError(1, args)
                out = Path(args[args.index("--dir") + 1])
                out.mkdir(parents=True)
                (out / "hosted-verdict.json").write_text(json.dumps(body))
                return ""
            assert args[0] == "api", args
            return json.dumps(good["run"] if args[1].endswith(str(RUN_ID)) else good["jobs"])
        return gh

    for name, arts, status, want in (
        ("newest run of the sha", {RUN_ID: good["verdict"], 9: other, 8: good["verdict"]}, {RUN_ID: "completed", 9: "completed", 8: "completed"}, None),
        ("a run without an artifact is skipped", {7: None, RUN_ID: good["verdict"]}, {7: "completed", RUN_ID: "completed"}, None),
        ("a run in progress is skipped", {7: good["verdict"], 6: other}, {7: "in_progress", 6: "completed"}, "HALVES_HOSTED"),
        ("no run of the sha", {9: other}, {9: "completed"}, "HALVES_HOSTED"),
        ("no run at all", {}, {}, "HALVES_HOSTED"),
    ):
        with tempfile.TemporaryDirectory() as tmp:
            got = fetch_hosted(SHA, Path(tmp), REPO, fake(arts, status))
            assert (got is None and want is None) or (got and want and got.startswith(want)), (name, got)
            if want is None:
                assert sorted(p.name for p in Path(tmp).glob("*.json")) == ["jobs.json", "run.json", "verdict.json"], name
                gate = Path(tmp) / "timings.jsonl"
                gate.write_text(json.dumps(good["devbox"][0]) + "\n")
                assert halves_refusal(SHA, gate, Path(tmp), plan=good["plan"], repo=REPO) is None, name
    pinned = fake({RUN_ID: good["verdict"], 9: other}, {RUN_ID: "completed", 9: "completed"})
    no_list = lambda args: pinned(args) if args[:2] != ["run", "list"] else (_ for _ in ()).throw(AssertionError("a pinned run is never searched for"))  # noqa: E731
    for run_id, want in ((RUN_ID, None), (9, "HALVES_HOSTED"), (77, "HALVES_HOSTED")):
        with tempfile.TemporaryDirectory() as tmp:
            got = fetch_hosted(SHA, Path(tmp), REPO, no_list, run_id=run_id)
            assert (got is None and want is None) or (got and want and got.startswith(want) and f"run {run_id}" in got), (run_id, got)


def self_test() -> int:
    """The green fixture passes on the real stage table, each mutant of it refuses with its own code, and the fetch picks the right run."""
    good = green()
    assert refusal_of(good) is None, refusal_of(good)
    assert refusal_of(green(1)) is None, "one live slice is a legal cut"
    for name, code, mutate in mutants():
        d = green()
        mutate(d)
        got = refusal_of(d)
        assert got and got.startswith(f"HALVES_{code}: "), f"{name}: wanted HALVES_{code}, got {got}"
    for odd in ("a" * 12, "B" * 40, "a" * 41):
        assert (refusal_of(green(sha=odd), odd) or "").startswith("HALVES_SHA: "), f"{odd!r} is not a gate identity, though both halves name it"
    with tempfile.TemporaryDirectory() as tmp:
        d = green()
        gate, hosted = write(Path(tmp), d)
        got = halves_refusal(SHA, gate, hosted, plan=d["plan"], repo=REPO, stage_names=lambda argv: (_ for _ in ()).throw(SystemExit("stage 'x' is placed in neither half")))
        assert got and got.startswith("HALVES_STAGE_UNPLACED: "), got
        assert (halves_refusal(SHA, Path(tmp) / "absent.jsonl", hosted, plan=d["plan"], repo=REPO) or "").startswith("HALVES_SHA: ")
    fetch_self_test(good)
    plan = workflow_plan()
    assert "workflow_dispatch" in plan["events"] and set(plan["events"]) <= {"workflow_dispatch", "schedule"}, plan
    assert job_names(plan, 2) == [f"Resolve the commit and the slices ({BRANCH})", f"slice 0 ({BRANCH})", f"slice 1 ({BRANCH})", f"slice 2 ({BRANCH})", f"Hosted half verdict ({BRANCH})"], job_names(plan, 2)
    assert fill(plan["artifact"]) == f"hosted-verdict-{BRANCH}", "the verdict artifact and every check run of the hosted half carry the ref"
    assert repo_slug() == REPO, repo_slug()
    print(f"self-test ok: two halves of one commit — green control, {len(mutants())} mutants each refused under its own HALVES_ code, run selection")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if sys.argv[1:] == ["--self-test"] else 2)
