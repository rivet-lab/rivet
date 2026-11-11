//! Property check_equals_run: every pre-run command passes only what `run` (or `apply`) will do; one generated matrix, graded by exit class and registry code.

use crate::common::*;
use rivet::config::{ExportMode, SourceType};
use rivet::types::target::ExportTarget;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Output;

const MATRIX: &str = "check_equals_run";

const R_WRAP: &str = "2026-11-08: doctor and plan wrap a stop in their own code where run names it; closes when each passes the stop's code through";
const R_CODES: &str = "2026-11-08: check and run name one stop with two codes; closes when both read the stop from one seam";
const R_CHECK: &str =
    "2026-11-08: check passes a config run refuses; closes when check probes what run reads";
const R_DOCTOR: &str = "2026-11-08: doctor passes a config run refuses; closes when doctor runs the preflight run runs";
const R_PLAN: &str =
    "2026-11-08: plan passes a config apply refuses; closes when plan runs the checks apply runs";
const R_STRICT: &str = "2026-11-08: check --strict refuses a config target run accepts; closes when run and check agree on config target";

/// RED signatures accepted on main: `<pair> pre[..]` or `<pair> pre[..] run[..]` and a dated reason with what closes it; the list only shrinks.
const ACCEPTED_RED: &[(&str, &str)] = &[
    (
        "check-run pre[exit=0 code=-] run[exit=5 code=RIVET_SOURCE_PROBE_UNREADABLE]",
        R_CHECK,
    ),
    (
        "check-run pre[exit=1 code=RIVET_CLI_CHECK_REPORTED_ERRORS] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_CODES,
    ),
    (
        "check-run pre[exit=1 code=RIVET_SOURCE_QUERY_FAILED] run[exit=1 code=-]",
        R_CODES,
    ),
    (
        "check-run pre[exit=1 code=RIVET_SOURCE_QUERY_FAILED] run[exit=1 code=RIVET_SOURCE_COLUMNS_UNREADABLE]",
        R_CODES,
    ),
    (
        "check-run pre[exit=1 code=RIVET_SOURCE_TABLE_NOT_FOUND] run[exit=1 code=RIVET_CONFIG_CHUNK_COLUMN_NOT_INTEGER]",
        R_CODES,
    ),
    (
        "check-run pre[exit=1 code=RIVET_SOURCE_TABLE_NOT_FOUND] run[exit=1 code=RIVET_SOURCE_CDC_PREREQUISITE]",
        R_CODES,
    ),
    (
        "check-run pre[exit=1 code=RIVET_SOURCE_TABLE_NOT_FOUND] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_CODES,
    ),
    (
        "check-strict-run pre[exit=0 code=-] run[exit=5 code=RIVET_SOURCE_PROBE_UNREADABLE]",
        R_CHECK,
    ),
    (
        "check-strict-run pre[exit=1 code=RIVET_CLI_CHECK_REPORTED_ERRORS] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_CODES,
    ),
    (
        "check-strict-run pre[exit=1 code=RIVET_CONFIG_TARGET_UNKNOWN] run[exit=0 code=-]",
        R_STRICT,
    ),
    (
        "check-strict-run pre[exit=1 code=RIVET_SOURCE_QUERY_FAILED] run[exit=1 code=-]",
        R_CODES,
    ),
    (
        "check-strict-run pre[exit=1 code=RIVET_SOURCE_QUERY_FAILED] run[exit=1 code=RIVET_SOURCE_COLUMNS_UNREADABLE]",
        R_CODES,
    ),
    (
        "check-strict-run pre[exit=1 code=RIVET_SOURCE_TABLE_NOT_FOUND] run[exit=1 code=RIVET_CONFIG_CHUNK_COLUMN_NOT_INTEGER]",
        R_CODES,
    ),
    (
        "check-strict-run pre[exit=1 code=RIVET_SOURCE_TABLE_NOT_FOUND] run[exit=1 code=RIVET_SOURCE_CDC_PREREQUISITE]",
        R_CODES,
    ),
    (
        "check-strict-run pre[exit=1 code=RIVET_SOURCE_TABLE_NOT_FOUND] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_CODES,
    ),
    ("doctor-run pre[exit=0 code=-] run[exit=1 code=-]", R_DOCTOR),
    (
        "doctor-run pre[exit=0 code=-] run[exit=1 code=RIVET_CONFIG_CHUNK_COLUMN_NOT_INTEGER]",
        R_DOCTOR,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=1 code=RIVET_SOURCE_COLLECTION_NOT_FOUND]",
        R_DOCTOR,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=1 code=RIVET_SOURCE_COLUMNS_UNREADABLE]",
        R_DOCTOR,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=1 code=RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH]",
        R_DOCTOR,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_DOCTOR,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=1 code=RIVET_SOURCE_TABLE_NOT_FOUND]",
        R_DOCTOR,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=5 code=RIVET_SOURCE_PROBE_UNREADABLE]",
        R_DOCTOR,
    ),
    (
        "doctor-run pre[exit=1 code=RIVET_CLI_PREFLIGHT_FAILED] run[exit=1 code=RIVET_CONFIG_GCP_SECRET_UNREADABLE]",
        R_WRAP,
    ),
    (
        "doctor-run pre[exit=1 code=RIVET_CLI_PREFLIGHT_FAILED] run[exit=1 code=RIVET_CONFIG_SOURCE_CONNECTION_INVALID]",
        R_WRAP,
    ),
    (
        "doctor-run pre[exit=1 code=RIVET_CLI_PREFLIGHT_FAILED] run[exit=1 code=RIVET_SOURCE_CDC_PREREQUISITE]",
        R_WRAP,
    ),
    (
        "doctor-run pre[exit=1 code=RIVET_CLI_PREFLIGHT_FAILED] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_WRAP,
    ),
    (
        "plan-apply pre[exit=0 code=-] run[exit=1 code=RIVET_PLAN_PARTITION_TOKEN_UNEXPANDED]",
        R_PLAN,
    ),
    (
        "plan-apply pre[exit=0 code=-] run[exit=1 code=RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH]",
        R_PLAN,
    ),
    (
        "plan-apply pre[exit=0 code=-] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_PLAN,
    ),
    (
        "plan-apply pre[exit=1 code=RIVET_PLAN_NOTHING_TO_PLAN] run[exit=1 code=RIVET_SOURCE_CDC_PREREQUISITE]",
        R_WRAP,
    ),
    (
        "plan-apply pre[exit=1 code=RIVET_PLAN_NOTHING_TO_PLAN] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_WRAP,
    ),
    (
        "plan-apply pre[exit=1 code=RIVET_PLAN_SOURCE_UNREADABLE] run[exit=1 code=-]",
        R_WRAP,
    ),
    (
        "plan-apply pre[exit=1 code=RIVET_PLAN_SOURCE_UNREADABLE] run[exit=1 code=RIVET_CONFIG_GCP_SECRET_UNREADABLE]",
        R_WRAP,
    ),
    (
        "plan-apply pre[exit=1 code=RIVET_PLAN_SOURCE_UNREADABLE] run[exit=1 code=RIVET_CONFIG_SOURCE_CONNECTION_INVALID]",
        R_WRAP,
    ),
    (
        "plan-apply pre[exit=1 code=RIVET_PLAN_SOURCE_UNREADABLE] run[exit=1 code=RIVET_SOURCE_COLLECTION_NOT_FOUND]",
        R_WRAP,
    ),
    (
        "plan-apply pre[exit=1 code=RIVET_PLAN_SOURCE_UNREADABLE] run[exit=1 code=RIVET_SOURCE_COLUMNS_UNREADABLE]",
        R_WRAP,
    ),
    (
        "plan-apply pre[exit=1 code=RIVET_PLAN_SOURCE_UNREADABLE] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_WRAP,
    ),
    (
        "plan-apply pre[exit=1 code=RIVET_PLAN_SOURCE_UNREADABLE] run[exit=1 code=RIVET_SOURCE_TABLE_NOT_FOUND]",
        R_WRAP,
    ),
];

/// Codes that mean the stand, not the config, decided the stop.
const INFRA_CODES: &[&str] = &["RIVET_SOURCE_CONNECT_FAILED"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Clean,
    OverrideRefused,
    CursorMissing,
    PartitionByInvalid,
    UnknownTable,
    GrantGap,
    SecretMalformed,
    SecretMissing,
    SecretMock,
    UnknownTarget,
}

/// The next shape after `s`; `None` after the last. A new variant fails to compile here.
fn next_shape(s: Option<Shape>) -> Option<Shape> {
    match s {
        None => Some(Shape::Clean),
        Some(Shape::Clean) => Some(Shape::OverrideRefused),
        Some(Shape::OverrideRefused) => Some(Shape::CursorMissing),
        Some(Shape::CursorMissing) => Some(Shape::PartitionByInvalid),
        Some(Shape::PartitionByInvalid) => Some(Shape::UnknownTable),
        Some(Shape::UnknownTable) => Some(Shape::GrantGap),
        Some(Shape::GrantGap) => Some(Shape::SecretMalformed),
        Some(Shape::SecretMalformed) => Some(Shape::SecretMissing),
        Some(Shape::SecretMissing) => Some(Shape::SecretMock),
        Some(Shape::SecretMock) => Some(Shape::UnknownTarget),
        Some(Shape::UnknownTarget) => None,
    }
}

fn shapes() -> Vec<Shape> {
    std::iter::successors(next_shape(None), |s| next_shape(Some(*s))).collect()
}

/// The variant idents of enum `name` in product file `rel`, read from the source text.
fn enum_idents(rel: &str, name: &str) -> Vec<String> {
    let text = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(rel))
        .unwrap_or_else(|e| panic!("read {rel}: {e}"));
    let needle = format!("enum {name} {{");
    let body = &text[text
        .find(&needle)
        .unwrap_or_else(|| panic!("{needle} not in {rel}"))
        + needle.len()..];
    let mut out = Vec::new();
    let mut depth = 1usize;
    for line in body.lines() {
        let t = line.trim_start();
        if depth == 1 && !t.starts_with("//") && !t.starts_with('#') {
            let id: String = t
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if id.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
                out.push(id);
            }
        }
        depth = depth + t.matches('{').count() - t.matches('}').count().min(depth);
        if depth == 0 {
            break;
        }
    }
    out
}

fn snake(camel: &str) -> String {
    let mut s = String::new();
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            s.push('_');
        }
        s.push(c.to_ascii_lowercase());
    }
    s
}

/// Every engine, read from `SourceType` and deserialised through its own serde.
fn engines() -> Vec<SourceType> {
    enum_idents("src/config/source.rs", "SourceType")
        .iter()
        .map(|v| {
            serde_yaml_ng::from_str::<SourceType>(&v.to_lowercase())
                .unwrap_or_else(|e| panic!("SourceType::{v}: {e}"))
        })
        .collect()
}

/// Every export mode, read from `ExportMode` and deserialised through its own serde.
fn modes() -> Vec<ExportMode> {
    enum_idents("src/config/export.rs", "ExportMode")
        .iter()
        .map(|v| {
            serde_yaml_ng::from_str::<ExportMode>(&snake(v))
                .unwrap_or_else(|e| panic!("ExportMode::{v}: {e}"))
        })
        .collect()
}

/// Every warehouse target, read from `ExportTarget` and parsed through its own parser.
fn targets() -> Vec<ExportTarget> {
    enum_idents("src/types/target.rs", "ExportTarget")
        .iter()
        .map(|v| {
            ExportTarget::parse(&v.to_lowercase()).unwrap_or_else(|| panic!("ExportTarget::{v}"))
        })
        .collect()
}

fn engine_key(e: SourceType) -> &'static str {
    match e {
        SourceType::Postgres => "postgres",
        SourceType::Mysql => "mysql",
        SourceType::Mssql => "mssql",
        SourceType::Oracle => "oracle",
        SourceType::Mongo => "mongo",
    }
}

fn mode_key(m: ExportMode) -> &'static str {
    m.as_str()
}

fn target_key(t: ExportTarget) -> &'static str {
    match t {
        ExportTarget::DuckDb => "duckdb",
        ExportTarget::BigQuery => "bigquery",
        ExportTarget::Snowflake => "snowflake",
        ExportTarget::ClickHouse => "clickhouse",
    }
}

fn shape_key(s: Shape) -> &'static str {
    match s {
        Shape::Clean => "clean",
        Shape::OverrideRefused => "override-refused",
        Shape::CursorMissing => "cursor-missing",
        Shape::PartitionByInvalid => "partition-by-invalid",
        Shape::UnknownTable => "unknown-table",
        Shape::GrantGap => "grant-gap",
        Shape::SecretMalformed => "secret-malformed",
        Shape::SecretMissing => "secret-missing",
        Shape::SecretMock => "secret-mock",
        Shape::UnknownTarget => "unknown-target",
    }
}

/// What the cell's own config shape should do when both commands agree.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Expect {
    Pass,
    Refuse,
    Either,
}

fn expect_of(engine: SourceType, mode: ExportMode, shape: Shape) -> Expect {
    match shape {
        Shape::Clean => match (engine, mode) {
            (SourceType::Mongo, ExportMode::Full) => Expect::Pass,
            (SourceType::Mongo, _) => Expect::Either,
            (_, _) => Expect::Pass,
        },
        Shape::OverrideRefused
        | Shape::CursorMissing
        | Shape::PartitionByInvalid
        | Shape::UnknownTable
        | Shape::SecretMalformed
        | Shape::SecretMissing
        | Shape::UnknownTarget => Expect::Refuse,
        Shape::GrantGap => Expect::Refuse,
        Shape::SecretMock => Expect::Either,
    }
}

/// Whether the shape is a cell for this engine and mode; the rest are not part of the matrix.
fn applies(engine: SourceType, mode: ExportMode, shape: Shape) -> bool {
    let sql = !matches!(engine, SourceType::Mongo);
    match shape {
        Shape::Clean | Shape::UnknownTable => true,
        Shape::UnknownTarget => sql && matches!(mode, ExportMode::Full),
        Shape::OverrideRefused => sql && !matches!(mode, ExportMode::Cdc),
        Shape::CursorMissing => sql && matches!(mode, ExportMode::Incremental),
        Shape::PartitionByInvalid => {
            sql && matches!(
                mode,
                ExportMode::Full | ExportMode::Incremental | ExportMode::Chunked
            )
        }
        Shape::GrantGap | Shape::SecretMalformed | Shape::SecretMissing | Shape::SecretMock => {
            matches!(mode, ExportMode::Full)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Verdict {
    exit: i32,
    code: Option<String>,
    first: String,
}

fn code_of(stderr: &str) -> Option<String> {
    let at = stderr.find("[RIVET_")?;
    let rest = &stderr[at + 1..];
    Some(rest[..rest.find(']')?].to_string())
}

fn verdict_of(out: &Output) -> Verdict {
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    let first = err
        .lines()
        .find(|l| l.contains("RIVET_") || l.to_lowercase().contains("error"))
        .or_else(|| err.lines().next())
        .unwrap_or("")
        .chars()
        .take(140)
        .collect();
    Verdict {
        exit: out.status.code().unwrap_or(-1),
        code: code_of(&err),
        first,
    }
}

/// Run one invocation; a panic of the second reader becomes a verdict that grades as NOT_GRADED.
fn guarded(f: impl FnOnce() -> Output) -> Verdict {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(out) => verdict_of(&out),
        Err(p) => {
            let why = p
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            Verdict {
                exit: READER_FAILED,
                code: None,
                first: why.lines().next().unwrap_or("").chars().take(140).collect(),
            }
        }
    }
}

const READER_FAILED: i32 = -2;

fn registry() -> BTreeSet<String> {
    let text = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/reference/errors.md"),
    )
    .expect("read errors.md");
    text.lines()
        .filter_map(|l| l.strip_prefix("| `RIVET_"))
        .filter_map(|l| l.split('`').next())
        .map(|c| format!("RIVET_{c}"))
        .collect()
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Outcome {
    Pass,
    Red,
    NotGraded,
}

fn show(v: &Verdict) -> String {
    format!("exit={} code={}", v.exit, v.code.as_deref().unwrap_or("-"))
}

/// The verdict of one pre-run command against its executor.
fn grade(
    pre: &Verdict,
    exec: &Verdict,
    expect: Expect,
    codes: &BTreeSet<String>,
) -> (Outcome, String) {
    let ev = format!("pre[{}] run[{}]", show(pre), show(exec));
    let infra = |v: &Verdict| v.code.as_deref().is_some_and(|c| INFRA_CODES.contains(&c));
    if infra(pre) || infra(exec) {
        return (
            Outcome::NotGraded,
            format!("{ev} the stand decided the stop"),
        );
    }
    if pre.exit == READER_FAILED || exec.exit == READER_FAILED {
        return (
            Outcome::NotGraded,
            format!(
                "{ev} the second reader failed: {} {}",
                pre.first, exec.first
            ),
        );
    }
    if pre.exit < 0 || exec.exit < 0 {
        return (Outcome::Red, format!("{ev} killed by a signal"));
    }
    match (pre.exit == 0, exec.exit == 0) {
        (true, true) => match expect {
            Expect::Refuse => (
                Outcome::NotGraded,
                format!("{ev} shape not provoked: both passed"),
            ),
            _ => (Outcome::Pass, ev),
        },
        (true, false) => (
            Outcome::Red,
            format!("{ev} pre-run passed, executor refused: {}", exec.first),
        ),
        (false, true) => (
            Outcome::Red,
            format!("{ev} pre-run refused, executor passed: {}", pre.first),
        ),
        (false, false) => {
            if pre.code != exec.code {
                return (
                    Outcome::Red,
                    format!(
                        "{ev} the refusals carry different codes: pre `{}` run `{}`",
                        pre.first, exec.first
                    ),
                );
            }
            let Some(code) = &pre.code else {
                return (
                    Outcome::Red,
                    format!("{ev} both refusals are uncoded: `{}`", pre.first),
                );
            };
            if !codes.contains(code) {
                return (
                    Outcome::Red,
                    format!("{ev} code {code} is not in docs/reference/errors.md"),
                );
            }
            if pre.exit != exec.exit {
                return (
                    Outcome::Red,
                    format!("{ev} same code, different exit class"),
                );
            }
            match expect {
                Expect::Pass => (
                    Outcome::NotGraded,
                    format!("{ev} control refused: {}", pre.first),
                ),
                _ => (Outcome::Pass, ev),
            }
        }
    }
}

/// A pre-run command run over the cell's config.
type CliFn = Box<dyn Fn(&[&str]) -> Output>;

/// How one cell's config is driven: a pre-run command and an executor over the same config.
struct Harness {
    cli: CliFn,
    run: Box<dyn Fn() -> Output>,
    apply: Box<dyn Fn(&Path) -> Output>,
    export: String,
    config: PathBuf,
}

/// A pre-run invocation and the executor leaf it is paired with.
struct Pair {
    pre: String,
    exec: String,
}

/// One leaf of `rivet schema cli --json`: its role, its pair and whether `check_equals_run` applies.
struct RegistryLeaf {
    path: String,
    role: String,
    pairs_with: Option<String>,
    applies: bool,
}

/// Every leaf command with its role, read from the binary's own registry.
fn role_registry() -> Vec<RegistryLeaf> {
    let out = rivet_command(&["schema", "cli", "--json"], &[])
        .output()
        .expect("rivet schema cli --json");
    assert!(out.status.success(), "rivet schema cli --json failed");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).expect("registry json");
    json["leaves"]
        .as_array()
        .expect("leaves")
        .iter()
        .map(|l| RegistryLeaf {
            path: l["path"].as_str().expect("path").to_string(),
            role: l["role"].as_str().expect("role").to_string(),
            pairs_with: l["pairs_with"].as_str().map(str::to_string),
            applies: l["invariants"]["check_equals_run"]
                .as_bool()
                .expect("check_equals_run flag"),
        })
        .collect()
}

/// The invocations that drive a pre-run leaf; none means no cell drives it yet.
fn pre_variants(path: &str) -> &'static [&'static str] {
    match path {
        "check" => &["check", "check-strict"],
        "doctor" => &["doctor"],
        "plan" => &["plan"],
        _ => &[],
    }
}

fn pairs() -> Vec<Pair> {
    role_registry()
        .iter()
        .filter_map(|l| l.pairs_with.as_ref().map(|exec| (l, exec)))
        .flat_map(|(l, exec)| {
            pre_variants(&l.path).iter().map(|pre| Pair {
                pre: pre.to_string(),
                exec: exec.clone(),
            })
        })
        .collect()
}

/// A NOT_GRADED reason for each leaf whose role says `check_equals_run` applies and that no pair drives.
fn ungraded_leaves() -> Vec<(String, String)> {
    let leaves = role_registry();
    let execs: BTreeSet<&str> = leaves
        .iter()
        .filter(|l| !pre_variants(&l.path).is_empty())
        .filter_map(|l| l.pairs_with.as_deref())
        .collect();
    leaves
        .iter()
        .filter(|l| l.applies)
        .filter_map(|l| match l.pairs_with {
            Some(_) if pre_variants(&l.path).is_empty() => Some((
                l.path.clone(),
                format!(
                    "role {}: pre-run leaf with no invocation in pre_variants",
                    l.role
                ),
            )),
            Some(_) => None,
            None if execs.contains(l.path.as_str()) => None,
            None => Some((
                l.path.clone(),
                format!("role {}: no graded pre-run pairs with this leaf", l.role),
            )),
        })
        .collect()
}

fn pre_args(pre: &str, export: &str, plan: &Path) -> Vec<String> {
    let v = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    match pre {
        "check" => v(&["check", "--export", export]),
        "check-strict" => v(&["check", "--export", export, "--strict"]),
        "doctor" => v(&["doctor"]),
        "plan" => v(&[
            "plan",
            "--export",
            export,
            "--format",
            "json",
            "--output",
            plan.to_str().unwrap(),
        ]),
        other => panic!("no invocation for pre-run command {other}"),
    }
}

/// Run every pair of one cell and return `(pair, outcome, evidence)` rows.
fn grade_cell(
    h: &Harness,
    expect: Expect,
    codes: &BTreeSet<String>,
) -> Vec<(String, Outcome, String)> {
    let tmp = tempfile::tempdir().expect("plan dir");
    let plan = tmp.path().join("plan.json");
    let pairs = pairs();
    let pres: Vec<(&Pair, Verdict)> = pairs
        .iter()
        .map(|p| {
            let args = pre_args(&p.pre, &h.export, &plan);
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            (p, guarded(|| (h.cli)(&args)))
        })
        .collect();
    let mut execs: std::collections::BTreeMap<&str, Verdict> = Default::default();
    for p in &pairs {
        execs
            .entry(p.exec.as_str())
            .or_insert_with(|| match p.exec.as_str() {
                "run" => guarded(|| (h.run)()),
                "apply" => {
                    let target = if plan.is_file() {
                        plan.clone()
                    } else {
                        h.config.clone()
                    };
                    guarded(|| (h.apply)(&target))
                }
                other => panic!("no executor {other}"),
            });
    }
    pres.iter()
        .map(|(p, pre)| {
            let (o, ev) = grade(pre, &execs[p.exec.as_str()], expect, codes);
            (format!("{}-{}", p.pre, p.exec), o, ev)
        })
        .collect()
}

const URL_ENV: &str = "PM_CHECK_EQUALS_RUN_URL";

/// A harness over a rig whose source URL travels in the environment, so a sealed plan can be applied.
fn rig_harness(rig: Rig) -> Harness {
    let url = rig.cdc_source_url().to_string();
    let rig = std::rc::Rc::new(rig.source_url_env(URL_ENV));
    let (a, b, c) = (rig.clone(), rig.clone(), rig.clone());
    let (u1, u2, u3) = (url.clone(), url.clone(), url);
    let export = rig.export_name().to_string();
    let e2 = export.clone();
    Harness {
        config: rig.config_path(),
        export,
        cli: Box::new(move |args| a.cli_env(args, &[(URL_ENV, &u1)])),
        run: Box::new(move || b.run_args_env(&["--export", &e2], &[(URL_ENV, &u2)])),
        apply: Box::new(move |p| c.apply_env(p, &[], &[(URL_ENV, &u3)])),
    }
}

fn raw_harness(yaml: String, dir: PathBuf, url: Option<String>) -> Harness {
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("rivet.yaml");
    std::fs::write(&config, yaml).unwrap();
    let (c1, c2) = (config.clone(), config.clone());
    let (d1, d2, d3) = (dir.clone(), dir.clone(), dir);
    let go = move |args: Vec<String>, d: &Path| {
        let env: Vec<(&str, &str)> = url.as_deref().map(|u| (URL_ENV, u)).into_iter().collect();
        rivet_command(&args, &env)
            .current_dir(d)
            .output()
            .expect("spawn rivet")
    };
    let go = std::rc::Rc::new(go);
    let (go2, go3) = (go.clone(), go.clone());
    Harness {
        export: "pm_check_equals_run".into(),
        config,
        cli: Box::new(move |args| {
            let mut v: Vec<String> = args.iter().map(|s| s.to_string()).collect();
            v.extend(["--config".into(), c1.to_str().unwrap().into()]);
            go(v, &d1)
        }),
        run: Box::new(move || {
            go2(
                vec![
                    "run".into(),
                    "--config".into(),
                    c2.to_str().unwrap().into(),
                    "--export".into(),
                    "pm_check_equals_run".into(),
                ],
                &d2,
            )
        }),
        apply: Box::new(move |p| go3(vec!["apply".into(), p.to_str().unwrap().into()], &d3)),
    }
}

fn stand_free_url(e: SourceType) -> &'static str {
    match e {
        SourceType::Postgres => "postgresql://rivet:rivet@127.0.0.1:1/rivet",
        SourceType::Mysql => "mysql://rivet:rivet@127.0.0.1:1/rivet",
        SourceType::Mssql => "sqlserver://sa:x@127.0.0.1:1/master",
        SourceType::Oracle => "oracle://rivet:rivet@127.0.0.1:1/FREEPDB1",
        SourceType::Mongo => "mongodb://127.0.0.1:1/pm",
    }
}

fn raw_query(e: SourceType) -> &'static str {
    match e {
        SourceType::Postgres | SourceType::Mysql | SourceType::Mssql => "query: \"SELECT 1 AS id\"",
        SourceType::Oracle => "query: \"SELECT 1 AS id FROM dual\"",
        SourceType::Mongo => "table: pm_check_equals_run_t",
    }
}

fn engine_label(e: SourceType) -> &'static str {
    engine_key(e)
}

/// A hand-written config for the shapes that need no fixture; `url` is the stand's or an unreachable one.
fn raw_yaml(e: SourceType, shape: Shape, dir: &Path) -> String {
    let conn = match shape {
        Shape::SecretMalformed => "url_secret: not-a-secret".to_string(),
        Shape::SecretMissing => {
            "url_secret: projects/pm-check-equals-run-nonexistent/secrets/pm-check-equals-run-none/versions/latest".to_string()
        }
        Shape::Clean
        | Shape::OverrideRefused
        | Shape::CursorMissing
        | Shape::PartitionByInvalid
        | Shape::UnknownTable
        | Shape::GrantGap
        | Shape::SecretMock
        | Shape::UnknownTarget => "url_env: PM_CHECK_EQUALS_RUN_URL".to_string(),
    };
    let grants = if shape == Shape::GrantGap {
        "\n  columns_from: grants"
    } else {
        ""
    };
    let target = if shape == Shape::UnknownTarget {
        "\n    target: pm_nonesuch"
    } else {
        ""
    };
    format!(
        "source:\n  type: {}\n  {conn}{grants}\nexports:\n  - name: pm_check_equals_run\n    {}\n    mode: full\n    format: parquet{target}\n    destination: {{ type: local, path: \"{}\" }}\n",
        engine_label(e),
        raw_query(e),
        dir.join("out").display()
    )
}

fn raw_cell(e: SourceType, shape: Shape, url: &str) -> Harness {
    let dir = tempfile::tempdir().expect("raw dir").keep();
    let yaml = raw_yaml(e, shape, &dir);
    let url =
        (!matches!(shape, Shape::SecretMalformed | Shape::SecretMissing)).then(|| url.to_string());
    raw_harness(yaml, dir, url)
}

fn emit(rows: &mut Vec<String>, key: &str, o: Outcome, ev: &str) {
    let tag = match o {
        Outcome::Pass => "PASS",
        Outcome::Red => "RED",
        Outcome::NotGraded => "NOT_GRADED",
    };
    let line = format!("{MATRIX} {key} {tag} {ev}");
    eprintln!("{line}");
    rows.push(line);
}

fn summary(rows: &[String]) -> (usize, usize, usize, String) {
    let n = |t: &str| {
        rows.iter()
            .filter(|r| r.split(' ').nth(2) == Some(t))
            .count()
    };
    let (p, r, g) = (n("PASS"), n("RED"), n("NOT_GRADED"));
    let line = format!("{MATRIX} SUMMARY pass={p} red={r} not_graded={g}");
    eprintln!("{line}");
    (p, r, g, line)
}

/// The ledger signature of a verdict line: its pair and the two verdicts.
fn signature(line: &str) -> String {
    let mut f = line.splitn(4, ' ');
    let key = f.nth(1).unwrap_or("");
    let ev = f.nth(1).unwrap_or("");
    let pair = key.rsplit('/').next().unwrap_or("");
    let at = ev
        .find("run[")
        .and_then(|i| ev[i..].find(']').map(|j| i + j + 1))
        .unwrap_or(ev.len());
    format!("{pair} {}", &ev[..at])
}

fn red_signatures(rows: &[String]) -> BTreeSet<String> {
    rows.iter()
        .filter(|r| r.split(' ').nth(2) == Some("RED"))
        .map(|r| signature(r))
        .collect()
}

fn accepted(sig: &str) -> bool {
    ACCEPTED_RED.iter().any(|(a, _)| sig.starts_with(a))
}

/// Fails on a RED signature outside the ledger; with `whole`, also on a ledger entry no RED cell shows.
fn assert_ledger(rows: &[String], whole: bool) {
    let red = red_signatures(rows);
    let fresh: Vec<_> = red.iter().filter(|s| !accepted(s)).collect();
    assert!(
        fresh.is_empty(),
        "RED signatures outside the accepted ledger:\n{}",
        fresh
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    );
    if whole {
        let stale: Vec<_> = ACCEPTED_RED
            .iter()
            .filter(|(a, _)| !red.iter().any(|s| s.starts_with(a)))
            .map(|(a, _)| *a)
            .collect();
        assert!(
            stale.is_empty(),
            "accepted-RED entries no cell shows any more: {stale:?}; shrink ACCEPTED_RED"
        );
    }
}

/// Whether the shape is refused before the source is reached, so no stand is needed.
fn stand_free(engine: SourceType, shape: Shape) -> bool {
    match shape {
        Shape::SecretMalformed => true,
        Shape::GrantGap => !matches!(engine, SourceType::Mysql),
        Shape::Clean
        | Shape::OverrideRefused
        | Shape::CursorMissing
        | Shape::PartitionByInvalid
        | Shape::UnknownTable
        | Shape::SecretMissing
        | Shape::SecretMock
        | Shape::UnknownTarget => false,
    }
}

/// The engines this run covers: all, or the one named by `PM_CHECK_EQUALS_RUN_ENGINE`.
fn selected_engines() -> Vec<SourceType> {
    let only = std::env::var("PM_CHECK_EQUALS_RUN_ENGINE").ok();
    engines()
        .into_iter()
        .filter(|e| only.as_deref().is_none_or(|o| o == engine_key(*e)))
        .collect()
}

fn run_raw_cells(
    only_stand_free: bool,
    url_of: &dyn Fn(SourceType) -> String,
    rows: &mut Vec<String>,
    codes: &BTreeSet<String>,
) {
    for e in selected_engines() {
        for s in shapes() {
            if !applies(e, ExportMode::Full, s)
                || !matches!(
                    s,
                    Shape::SecretMalformed
                        | Shape::SecretMissing
                        | Shape::SecretMock
                        | Shape::UnknownTarget
                        | Shape::GrantGap
                )
            {
                continue;
            }
            if only_stand_free && !stand_free(e, s) {
                continue;
            }
            let key = format!("{}/full/{}", engine_key(e), shape_key(s));
            if s == Shape::SecretMock {
                emit(
                    rows,
                    &format!("{key}/-"),
                    Outcome::NotGraded,
                    "Secret Manager has a fixed endpoint: a mock secret needs a product hook",
                );
                continue;
            }
            if s == Shape::GrantGap && e == SourceType::Mysql {
                continue;
            }
            let h = raw_cell(e, s, &url_of(e));
            for (pair, o, ev) in grade_cell(&h, expect_of(e, ExportMode::Full, s), codes) {
                emit(rows, &format!("{key}/{pair}"), o, &ev);
            }
        }
    }
}

#[test]
fn the_axes_come_from_the_code() {
    let leaves: Vec<String> = role_registry().into_iter().map(|l| l.path).collect();
    assert!(
        leaves.iter().any(|c| c == "run") && leaves.iter().any(|c| c == "state reset-chunks"),
        "{leaves:?}"
    );
    let pairs: Vec<String> = pairs()
        .iter()
        .map(|p| format!("{}-{}", p.pre, p.exec))
        .collect();
    assert_eq!(
        pairs,
        ["check-run", "check-strict-run", "doctor-run", "plan-apply"].map(String::from),
        "pre-run pairs"
    );
    assert_eq!(engines().len(), 5, "{:?}", engines());
    assert_eq!(modes().len(), 5, "{:?}", modes());
    assert_eq!(targets().len(), 4, "{:?}", targets());
    assert_eq!(shapes().len(), 10);
    assert!(
        registry().contains("RIVET_SOURCE_CONNECT_FAILED"),
        "errors.md registry"
    );
}

#[test]
fn the_grade_reads_exit_class_and_code() {
    let codes: BTreeSet<String> = ["RIVET_A", "RIVET_B"].map(String::from).into();
    let v = |exit, code: Option<&str>| Verdict {
        exit,
        code: code.map(String::from),
        first: String::new(),
    };
    let g = |p: Verdict, x: Verdict, e| grade(&p, &x, e, &codes).0;
    assert_eq!(g(v(0, None), v(0, None), Expect::Pass), Outcome::Pass);
    assert_eq!(
        g(v(0, None), v(0, None), Expect::Refuse),
        Outcome::NotGraded
    );
    assert_eq!(
        g(v(0, None), v(1, Some("RIVET_A")), Expect::Refuse),
        Outcome::Red
    );
    assert_eq!(
        g(v(1, Some("RIVET_A")), v(0, None), Expect::Refuse),
        Outcome::Red
    );
    assert_eq!(
        g(v(1, Some("RIVET_A")), v(1, Some("RIVET_B")), Expect::Refuse),
        Outcome::Red
    );
    assert_eq!(
        g(v(1, Some("RIVET_A")), v(1, Some("RIVET_A")), Expect::Refuse),
        Outcome::Pass
    );
    assert_eq!(
        g(v(1, Some("RIVET_A")), v(1, Some("RIVET_A")), Expect::Pass),
        Outcome::NotGraded
    );
    assert_eq!(g(v(1, None), v(1, None), Expect::Refuse), Outcome::Red);
    assert_eq!(
        g(v(1, Some("RIVET_Z")), v(1, Some("RIVET_Z")), Expect::Refuse),
        Outcome::Red
    );
    assert_eq!(
        g(v(1, Some("RIVET_A")), v(2, Some("RIVET_A")), Expect::Refuse),
        Outcome::Red
    );
    assert_eq!(
        g(
            v(1, Some("RIVET_SOURCE_CONNECT_FAILED")),
            v(1, Some("RIVET_SOURCE_CONNECT_FAILED")),
            Expect::Refuse
        ),
        Outcome::NotGraded
    );
}

#[test]
fn a_ledger_entry_names_a_signature_not_a_cell() {
    let line = "check-equals-run postgres/full/clean/-/doctor-run RED pre[exit=1 code=RIVET_A] run[exit=1 code=RIVET_B] the refusals carry different codes";
    assert_eq!(
        signature(line),
        "doctor-run pre[exit=1 code=RIVET_A] run[exit=1 code=RIVET_B]"
    );
}

#[test]
fn stand_free_cells_agree() {
    let codes = registry();
    let mut rows = Vec::new();
    run_raw_cells(true, &|e| stand_free_url(e).to_string(), &mut rows, &codes);
    summary(&rows);
    assert_ledger(&rows, false);
}

fn sql_of(e: SourceType) -> Option<SqlEngine> {
    match e {
        SourceType::Postgres => Some(SqlEngine::Pg),
        SourceType::Mysql => Some(SqlEngine::Mysql),
        SourceType::Mssql => Some(SqlEngine::Mssql),
        SourceType::Oracle => oracle_engine(),
        SourceType::Mongo => None,
    }
}

#[cfg(feature = "oracle")]
fn oracle_engine() -> Option<SqlEngine> {
    Some(SqlEngine::Oracle)
}

#[cfg(not(feature = "oracle"))]
fn oracle_engine() -> Option<SqlEngine> {
    None
}

fn live_url(e: SourceType) -> String {
    match e {
        SourceType::Postgres => POSTGRES_URL.to_string(),
        SourceType::Mysql => MYSQL_URL.to_string(),
        SourceType::Mssql => MSSQL_URL.to_string(),
        SourceType::Oracle => ORACLE_URL.to_string(),
        SourceType::Mongo => format!("{MONGO_URL}/pm_check_equals_run_db"),
    }
}

struct Fixture {
    table: String,
    mongo_url: Option<String>,
    _guard: Box<dyn std::any::Any>,
}

/// A table with rows on the stand for `engine`, or the reason it cannot be made.
fn fixture(engine: SourceType, mode: ExportMode) -> Result<Fixture, String> {
    let made = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match sql_of(engine) {
        Some(eng) => {
            eng.alive();
            let (table, guard) = match mode {
                ExportMode::Chunked => eng.range_table("pm_check_equals_run_t"),
                _ => eng.table("pm_check_equals_run_t"),
            };
            eng.insert(&table, 1..=5, 10, Some(3));
            Fixture {
                table,
                mongo_url: None,
                _guard: guard,
            }
        }
        None if engine == SourceType::Mongo => {
            require_alive(LiveService::Mongo);
            let db = unique_name("pm_check_equals_run_db");
            let m = MongoTest::connect(27017, &db);
            let table = unique_name("pm_check_equals_run_c");
            m.seed_int_id(&table, 5);
            Fixture {
                table,
                mongo_url: Some(MongoTest::url(27017, &db)),
                _guard: Box::new(MongoDbGuard { port: 27017, db }),
            }
        }
        None => panic!("{} engine unavailable in this build", engine_key(engine)),
    }));
    made.map_err(|p| {
        p.downcast_ref::<String>()
            .cloned()
            .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default()
            .lines()
            .next()
            .unwrap_or("")
            .chars()
            .take(120)
            .collect()
    })
}

fn base_rig(engine: SourceType, table: &str, mongo_url: Option<&str>) -> Rig {
    match engine {
        SourceType::Postgres => Rig::pg_batch(table),
        SourceType::Mysql => Rig::mysql_batch(table),
        SourceType::Mssql => Rig::mssql_batch(table),
        SourceType::Oracle => Rig::oracle_batch(table),
        SourceType::Mongo => Rig::mongo_batch(table).source_url(mongo_url.expect("mongo url")),
    }
}

fn cdc_rig(engine: SourceType, table: &str, slot: &str) -> Rig {
    match engine {
        SourceType::Postgres => Rig::pg_cdc(table, slot),
        SourceType::Mysql => Rig::mysql_cdc(table),
        SourceType::Mssql => Rig::mssql_cdc(table, &format!("dbo_{table}")),
        SourceType::Oracle => Rig::oracle_cdc(table),
        SourceType::Mongo => Rig::mongo_cdc(table),
    }
}

/// The export lines the mode needs, with the cursor named `cursor`.
fn mode_lines(engine: SourceType, mode: ExportMode, cursor: Option<&str>) -> Vec<String> {
    let col = |c: &str| spell(engine, c);
    match mode {
        ExportMode::Full | ExportMode::Cdc => vec![],
        ExportMode::Incremental => vec![format!(
            "cursor_column: {}",
            cursor.map(str::to_string).unwrap_or_else(|| match engine {
                SourceType::Mongo => "_id".into(),
                _ => col("updated_at"),
            })
        )],
        ExportMode::Chunked => vec![
            format!("chunk_column: {}", col("id")),
            "chunk_size: 2".into(),
        ],
        ExportMode::TimeWindow => vec![
            format!("time_column: {}", col("server_time")),
            "days_window: 7".into(),
        ],
    }
}

/// A fixture column as the engine's catalog holds it (Oracle: upper case).
fn spell(engine: SourceType, name: &str) -> String {
    match engine {
        SourceType::Oracle => name.to_uppercase(),
        SourceType::Postgres | SourceType::Mysql | SourceType::Mssql | SourceType::Mongo => {
            name.to_string()
        }
    }
}

/// A fixture column and the type its wire value cannot be declared as, per engine.
fn refused_override(engine: SourceType) -> (&'static str, &'static str) {
    match engine {
        SourceType::Postgres | SourceType::Mssql | SourceType::Mongo => ("time_spent", "string"),
        SourceType::Mysql => ("label", "uuid"),
        SourceType::Oracle => ("server_time", "date"),
    }
}

fn cell_rig(
    engine: SourceType,
    mode: ExportMode,
    shape: Shape,
    target: Option<ExportTarget>,
    fx: Option<&Fixture>,
    out: &Path,
    slot: &str,
) -> Rig {
    let missing = match engine {
        SourceType::Oracle => "PM_CHECK_EQUALS_RUN_MISSING",
        _ => "pm_check_equals_run_missing",
    };
    let table = match (&fx, shape) {
        (_, Shape::UnknownTable) => missing.to_string(),
        (Some(f), _) => f.table.clone(),
        (None, _) => missing.to_string(),
    };
    let mongo = fx.and_then(|f| f.mongo_url.as_deref());
    let mut rig = match mode {
        ExportMode::Cdc => cdc_rig(engine, &table, slot),
        _ => base_rig(engine, &table, mongo).mode(mode.as_str()),
    };
    if let (Some(url), true) = (mongo, matches!(mode, ExportMode::Cdc)) {
        rig = rig.source_url(url);
    }
    let cursor = (shape == Shape::CursorMissing).then_some("pm_check_equals_run_missing_col");
    for l in mode_lines(engine, mode, cursor) {
        rig = rig.export_line(&l);
    }
    rig = rig.dest_path(out.to_path_buf());
    match shape {
        Shape::OverrideRefused => {
            let (name, ty) = refused_override(engine);
            let col = spell(engine, name);
            rig = rig
                .export_line("columns:")
                .export_line(&format!("  {col}: \"{ty}\""));
        }
        Shape::PartitionByInvalid => {
            rig = rig
                .export_line(&format!("partition_by: {}", spell(engine, "id")))
                .dest_path(out.join("{partition}"));
        }
        Shape::Clean
        | Shape::CursorMissing
        | Shape::UnknownTable
        | Shape::GrantGap
        | Shape::SecretMalformed
        | Shape::SecretMissing
        | Shape::SecretMock
        | Shape::UnknownTarget => {}
    }
    if let Some(t) = target {
        rig = rig.export_line(&format!("target: {}", target_key(t)));
    }
    rig
}

struct MysqlUser(String);

impl Drop for MysqlUser {
    fn drop(&mut self) {
        use ::mysql::prelude::Queryable;
        let _ = mysql_root_connect().query_drop(format!("DROP USER IF EXISTS '{}'@'%'", self.0));
    }
}

/// A MySQL login that may read only `id` of `table`, with its URL.
fn mysql_gap(table: &str) -> (String, MysqlUser) {
    use ::mysql::prelude::Queryable;
    let user = unique_name("pm_check_equals_run_u");
    let db = MYSQL_URL.rsplit('/').next().unwrap().to_string();
    let mut root = mysql_root_connect();
    root.query_drop(format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'rivet'"))
        .expect("create user");
    root.query_drop(format!("GRANT SELECT (id) ON {db}.{table} TO '{user}'@'%'"))
        .expect("grant column");
    (
        MYSQL_URL.replace("rivet:rivet@", &format!("{user}:rivet@")),
        MysqlUser(user),
    )
}

fn cell_key(e: SourceType, m: ExportMode, s: Shape, t: Option<ExportTarget>) -> String {
    format!(
        "{}/{}/{}/{}",
        engine_key(e),
        mode_key(m),
        shape_key(s),
        t.map_or("-", target_key)
    )
}

fn run_rig_cell(
    rows: &mut Vec<String>,
    codes: &BTreeSet<String>,
    e: SourceType,
    m: ExportMode,
    s: Shape,
    t: Option<ExportTarget>,
) {
    let key = cell_key(e, m, s, t);
    let fx = match m {
        ExportMode::Cdc => None,
        _ => match fixture(e, m) {
            Ok(f) => Some(f),
            Err(why) => {
                return emit(
                    rows,
                    &key,
                    Outcome::NotGraded,
                    &format!("no fixture: {why}"),
                );
            }
        },
    };
    if m == ExportMode::Cdc && s == Shape::Clean {
        return emit(
            rows,
            &key,
            Outcome::NotGraded,
            "a clean CDC cell needs a CDC-prepared table: graded by the CDC suites",
        );
    }
    let out = tempfile::tempdir().expect("out dir");
    let slot = unique_name("pm_check_equals_run_slot");
    let _slot =
        (e == SourceType::Postgres && m == ExportMode::Cdc).then(|| Slot::new(slot.clone()));
    let h = rig_harness(cell_rig(e, m, s, t, fx.as_ref(), out.path(), &slot));
    let expect = if t.is_some() {
        Expect::Either
    } else {
        expect_of(e, m, s)
    };
    for (pair, o, ev) in grade_cell(&h, expect, codes) {
        emit(rows, &format!("{key}/{pair}"), o, &ev);
    }
}

fn run_mysql_grant_gap(rows: &mut Vec<String>, codes: &BTreeSet<String>) {
    let key = "mysql/full/grant-gap/-";
    let fx = match fixture(SourceType::Mysql, ExportMode::Full) {
        Ok(f) => f,
        Err(why) => return emit(rows, key, Outcome::NotGraded, &format!("no fixture: {why}")),
    };
    let (url, _user) = mysql_gap(&fx.table);
    let dir = tempfile::tempdir().expect("dir").keep();
    let yaml = format!(
        "source:\n  type: mysql\n  url_env: PM_CHECK_EQUALS_RUN_URL\n  columns_from: grants\nexports:\n  - name: pm_check_equals_run\n    query: \"SELECT * FROM {}\"\n    mode: full\n    format: parquet\n    destination: {{ type: local, path: \"{}\" }}\n",
        fx.table,
        dir.join("out").display()
    );
    let h = raw_harness(yaml, dir, Some(url));
    for (pair, o, ev) in grade_cell(&h, Expect::Refuse, codes) {
        emit(rows, &format!("{key}/{pair}"), o, &ev);
    }
}

#[test]
#[ignore = "live: every engine, takes the stand through dev/pytools/live_slot.py"]
fn the_live_matrix_agrees() {
    let codes = registry();
    let mut rows = Vec::new();
    for e in selected_engines() {
        for m in modes() {
            for s in shapes() {
                if applies(e, m, s)
                    && !matches!(
                        s,
                        Shape::GrantGap
                            | Shape::SecretMalformed
                            | Shape::SecretMissing
                            | Shape::SecretMock
                            | Shape::UnknownTarget
                    )
                {
                    run_rig_cell(&mut rows, &codes, e, m, s, None);
                }
            }
        }
        for t in targets() {
            for s in [Shape::Clean, Shape::OverrideRefused] {
                if applies(e, ExportMode::Full, s) {
                    run_rig_cell(&mut rows, &codes, e, ExportMode::Full, s, Some(t));
                }
            }
        }
    }
    if selected_engines().contains(&SourceType::Mysql) {
        run_mysql_grant_gap(&mut rows, &codes);
    }
    run_raw_cells(false, &live_url, &mut rows, &codes);
    for (leaf, why) in ungraded_leaves() {
        emit(
            &mut rows,
            &format!("any/any/any/-/{}", leaf.replace(' ', "-")),
            Outcome::NotGraded,
            &why,
        );
    }
    summary(&rows);
    if let Ok(path) = std::env::var("PM_CHECK_EQUALS_RUN_LOG") {
        std::fs::write(path, rows.join("\n") + "\n").expect("write the verdict log");
    }
    assert_ledger(&rows, std::env::var("PM_CHECK_EQUALS_RUN_ENGINE").is_err());
}
