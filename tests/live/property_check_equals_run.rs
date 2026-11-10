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
const R_LOADS: &str = "2026-11-10: state loads on a config path that does not exist prints an empty list and exits 0; closes when it opens the config through the seam its state siblings use";
const R_STRICT: &str = "2026-11-08: check --strict refuses a config target run accepts; closes when run and check agree on config target";

const R_TYPE: &str = "2026-11-10: check, doctor, plan and init pass a column type or override run refuses (unsafe mapping, unrepresentable value, exit 6 type builder); closes when the pre-run commands type each column through the seam run writes with";
const R_PRIOR: &str = "2026-11-10: check, doctor and plan pass over stored state run refuses (a cursor another destination or export owns, a lost CDC checkpoint); closes when the pre-run commands read state and checkpoint through run's guard";
const R_UNCODED: &str = "2026-11-10: run or a pre-run command refuses without a RIVET_ code, so the pair cannot agree on the stop; closes when each refusal is raised through its coded seam";
const R_URL: &str = "2026-11-10: check passes a mongodb URL without a database that run refuses as invalid; closes when check parses the source URL through run's parser";
const R_STRICT_TYPE: &str = "2026-11-10: check --strict refuses an unsafe type mapping run delivers; closes when run and check --strict read one type-safety verdict";

/// RED signatures accepted on main: `<pair> pre[..]` or `<pair> pre[..] run[..]` and a dated reason with what closes it; the list only shrinks.
const ACCEPTED_RED: &[(&str, &str)] = &[
    (
        "state-loads-run pre[exit=0 code=-] run[exit=1 code=RIVET_CONFIG_FILE_UNREADABLE]",
        R_LOADS,
    ),
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
    (
        "cdc-run pre[exit=1 code=-] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_UNCODED,
    ),
    ("check-run pre[exit=0 code=-] run[exit=1 code=-]", R_CHECK),
    (
        "check-run pre[exit=0 code=-] run[exit=1 code=RIVET_CONFIG_SOURCE_URL_INVALID]",
        R_URL,
    ),
    (
        "check-run pre[exit=0 code=-] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_CHECK,
    ),
    (
        "check-run pre[exit=0 code=-] run[exit=1 code=RIVET_TYPE_UNSAFE_MAPPING]",
        R_TYPE,
    ),
    (
        "check-run pre[exit=0 code=-] run[exit=5 code=RIVET_SOURCE_CDC_CHECKPOINT_INVALID]",
        R_PRIOR,
    ),
    (
        "check-run pre[exit=0 code=-] run[exit=5 code=RIVET_SOURCE_VALUE_UNREPRESENTABLE]",
        R_TYPE,
    ),
    (
        "check-run pre[exit=0 code=-] run[exit=5 code=RIVET_STATE_CURSOR_DESTINATION_MISMATCH]",
        R_PRIOR,
    ),
    (
        "check-run pre[exit=0 code=-] run[exit=5 code=RIVET_STATE_CURSOR_OWNER_MISMATCH]",
        R_PRIOR,
    ),
    (
        "check-run pre[exit=0 code=-] run[exit=6 code=RIVET_INTERNAL_TYPE_BUILDER]",
        R_TYPE,
    ),
    (
        "check-run pre[exit=1 code=RIVET_PLAN_REJECTED] run[exit=1 code=-]",
        R_UNCODED,
    ),
    (
        "check-strict-run pre[exit=0 code=-] run[exit=1 code=-]",
        R_CHECK,
    ),
    (
        "check-strict-run pre[exit=0 code=-] run[exit=1 code=RIVET_SOURCE_QUERY_FAILED]",
        R_CHECK,
    ),
    (
        "check-strict-run pre[exit=0 code=-] run[exit=5 code=RIVET_SOURCE_CDC_CHECKPOINT_INVALID]",
        R_PRIOR,
    ),
    (
        "check-strict-run pre[exit=0 code=-] run[exit=5 code=RIVET_SOURCE_VALUE_UNREPRESENTABLE]",
        R_TYPE,
    ),
    (
        "check-strict-run pre[exit=0 code=-] run[exit=5 code=RIVET_STATE_CURSOR_DESTINATION_MISMATCH]",
        R_PRIOR,
    ),
    (
        "check-strict-run pre[exit=0 code=-] run[exit=5 code=RIVET_STATE_CURSOR_OWNER_MISMATCH]",
        R_PRIOR,
    ),
    (
        "check-strict-run pre[exit=0 code=-] run[exit=6 code=RIVET_INTERNAL_TYPE_BUILDER]",
        R_TYPE,
    ),
    (
        "check-strict-run pre[exit=1 code=RIVET_CLI_PREFLIGHT_FAILED] run[exit=1 code=-]",
        R_UNCODED,
    ),
    (
        "check-strict-run pre[exit=1 code=RIVET_CLI_PREFLIGHT_FAILED] run[exit=1 code=RIVET_CONFIG_SOURCE_URL_INVALID]",
        R_WRAP,
    ),
    (
        "check-strict-run pre[exit=1 code=RIVET_TYPE_UNSAFE_MAPPING] run[exit=0 code=-]",
        R_STRICT_TYPE,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=1 code=RIVET_TYPE_UNSAFE_MAPPING]",
        R_TYPE,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=5 code=RIVET_SOURCE_CDC_CHECKPOINT_INVALID]",
        R_PRIOR,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=5 code=RIVET_SOURCE_VALUE_UNREPRESENTABLE]",
        R_TYPE,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=5 code=RIVET_STATE_CURSOR_DESTINATION_MISMATCH]",
        R_PRIOR,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=5 code=RIVET_STATE_CURSOR_OWNER_MISMATCH]",
        R_PRIOR,
    ),
    (
        "doctor-run pre[exit=0 code=-] run[exit=6 code=RIVET_INTERNAL_TYPE_BUILDER]",
        R_TYPE,
    ),
    (
        "doctor-run pre[exit=1 code=RIVET_CLI_PREFLIGHT_FAILED] run[exit=1 code=RIVET_CONFIG_SECRET_UNSET]",
        R_WRAP,
    ),
    (
        "doctor-run pre[exit=1 code=RIVET_CLI_PREFLIGHT_FAILED] run[exit=1 code=RIVET_CONFIG_SOURCE_URL_INVALID]",
        R_WRAP,
    ),
    (
        "doctor-run pre[exit=1 code=RIVET_CLI_PREFLIGHT_FAILED] run[exit=1 code=RIVET_CONFIG_TLS_CA_INVALID]",
        R_WRAP,
    ),
    (
        "init-run pre[exit=0 code=-] run[exit=1 code=RIVET_TYPE_UNSAFE_MAPPING]",
        R_TYPE,
    ),
    ("plan-apply pre[exit=0 code=-] run[exit=1 code=-]", R_PLAN),
    (
        "plan-apply pre[exit=0 code=-] run[exit=1 code=RIVET_TYPE_UNSAFE_MAPPING]",
        R_TYPE,
    ),
    (
        "plan-apply pre[exit=0 code=-] run[exit=5 code=RIVET_SOURCE_VALUE_UNREPRESENTABLE]",
        R_TYPE,
    ),
    (
        "plan-apply pre[exit=0 code=-] run[exit=5 code=RIVET_STATE_CURSOR_DESTINATION_MISMATCH]",
        R_PRIOR,
    ),
    (
        "plan-apply pre[exit=0 code=-] run[exit=5 code=RIVET_STATE_CURSOR_OWNER_MISMATCH]",
        R_PRIOR,
    ),
    (
        "plan-apply pre[exit=0 code=-] run[exit=6 code=RIVET_INTERNAL_TYPE_BUILDER]",
        R_TYPE,
    ),
    (
        "plan-apply pre[exit=1 code=-] run[exit=1 code=-]",
        R_UNCODED,
    ),
    (
        "plan-apply pre[exit=1 code=RIVET_PLAN_SOURCE_UNREADABLE] run[exit=1 code=RIVET_CONFIG_SECRET_UNSET]",
        R_WRAP,
    ),
    (
        "plan-apply pre[exit=1 code=RIVET_PLAN_SOURCE_UNREADABLE] run[exit=1 code=RIVET_CONFIG_TLS_CA_INVALID]",
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
type CliFn<'a> = Box<dyn Fn(&[&str]) -> Output + 'a>;

/// One invocation with no arguments of its own.
type CallFn<'a> = Box<dyn Fn() -> Output + 'a>;

/// How one cell's config is driven: pre-run commands and executors over the same config.
struct Harness<'a> {
    cli: CliFn<'a>,
    run: CallFn<'a>,
    apply: Box<dyn Fn(&Path) -> Output + 'a>,
    cdc: Option<CallFn<'a>>,
    init: Option<CallFn<'a>>,
    stream: bool,
    export: Option<String>,
    config: PathBuf,
}

/// A pre-run command of the CLI tree and the executor it is paired with.
struct Pair {
    pre: &'static str,
    exec: &'static str,
}

/// What the matrix does with one leaf of the CLI tree: grade it as pre-run pairs, or print why it has none.
enum Role {
    Pairs(Vec<Pair>),
    Unpaired(&'static str),
}

/// The role of one leaf command (`state reset-chunks`); an unclassified leaf fails the matrix loudly.
fn role_of(leaf: &str) -> Role {
    const STATE: &str = "state reads or edits the state DB, not an export's pre-run; its lifecycle rows need a lease fixture";
    match leaf {
        "check" => Role::Pairs(vec![
            Pair {
                pre: "check",
                exec: "run",
            },
            Pair {
                pre: "check-strict",
                exec: "run",
            },
        ]),
        "doctor" => Role::Pairs(vec![Pair {
            pre: "doctor",
            exec: "run",
        }]),
        "plan" => Role::Pairs(vec![Pair {
            pre: "plan",
            exec: "apply",
        }]),
        "init" => Role::Pairs(vec![Pair {
            pre: "init",
            exec: "run",
        }]),
        "validate" => Role::Unpaired(
            "validate reads a delivery after a run; it has no run to agree with before one",
        ),
        "run" | "apply" => Role::Unpaired("the executor side of a pair, not a pre-run command"),
        "load" => Role::Unpaired(
            "load writes a warehouse; its adapter executor is not driven by this matrix",
        ),
        "cdc" => Role::Pairs(vec![Pair {
            pre: "cdc",
            exec: "run",
        }]),
        "compact" | "repair" | "reconcile" => {
            Role::Unpaired("writes or repairs output; no pre-run to agree with")
        }
        "completions" | "metrics" | "journal" | "schema config" | "schema cli"
        | "schema errors" => Role::Unpaired("reads or prints; no pre-run to agree with"),
        "state show" | "state reset" | "state accept" | "state files" | "state reset-chunks"
        | "state chunks" | "state progression" | "state runs" | "state finish-run"
        | "state copy" | "state vacuum" | "state loads" => Role::Unpaired(STATE),
        other => panic!(
            "rivet {other}: a new command; classify it in role_of as a pre-run pair or with a reason"
        ),
    }
}

/// The leaf commands of the CLI tree (no subcommands of their own), from `rivet schema cli`.
fn cli_leaves() -> Vec<String> {
    let out = rivet_command(&["schema", "cli"], &[])
        .output()
        .expect("rivet schema cli");
    assert!(out.status.success(), "rivet schema cli failed");
    let all: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.strip_prefix("## `rivet "))
        .filter_map(|l| l.strip_suffix('`'))
        .map(str::to_string)
        .collect();
    all.iter()
        .filter(|c| !all.iter().any(|o| o.starts_with(&format!("{c} "))))
        .cloned()
        .collect()
}

fn pairs() -> Vec<Pair> {
    cli_leaves()
        .iter()
        .flat_map(|c| match role_of(c) {
            Role::Pairs(p) => p,
            Role::Unpaired(_) => vec![],
        })
        .collect()
}

fn pre_args(pre: &str, export: Option<&str>, plan: &Path) -> Vec<String> {
    let mut v: Vec<String> = match pre {
        "check" | "check-strict" => vec!["check".into()],
        "doctor" => return vec!["doctor".into()],
        "plan" => [
            "plan",
            "--format",
            "json",
            "--output",
            plan.to_str().unwrap(),
        ]
        .map(String::from)
        .to_vec(),
        other => panic!("no invocation for pre-run command {other}"),
    };
    if let Some(e) = export {
        v.extend(["--export".into(), e.into()]);
    }
    if pre == "check-strict" {
        v.push("--strict".into());
    }
    v
}

/// Whether the cell's harness can drive `pre`: an init cell drives init alone, a CDC cell adds `rivet cdc` and drops the batch-only plan.
fn drives(h: &Harness, pre: &str) -> bool {
    match pre {
        "init" => h.init.is_some(),
        "cdc" => h.cdc.is_some() && h.init.is_none(),
        "plan" => !h.stream && h.init.is_none(),
        _ => h.init.is_none(),
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
    let pairs: Vec<Pair> = pairs().into_iter().filter(|p| drives(h, p.pre)).collect();
    let pres: Vec<(&Pair, Verdict)> = pairs
        .iter()
        .map(|p| {
            let v = match p.pre {
                "init" => guarded(|| (h.init.as_ref().unwrap())()),
                "cdc" => guarded(|| (h.cdc.as_ref().unwrap())()),
                pre => {
                    let args = pre_args(pre, h.export.as_deref(), &plan);
                    let args: Vec<&str> = args.iter().map(String::as_str).collect();
                    guarded(|| (h.cli)(&args))
                }
            };
            (p, v)
        })
        .collect();
    let mut execs: std::collections::BTreeMap<&str, Verdict> = Default::default();
    for p in &pairs {
        execs.entry(p.exec).or_insert_with(|| match p.exec {
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
            let (o, ev) = grade(pre, &execs[p.exec], expect, codes);
            (format!("{}-{}", p.pre, p.exec), o, ev)
        })
        .collect()
}

const URL_ENV: &str = "PM_CHECK_EQUALS_RUN_URL";
const NO_ORACLE: &str =
    "the matrix grades exit and code of each command pair, not the delivered rows";

/// The env every invocation of a rig cell carries: the source URL and the cell's own extras.
fn cell_env(url: &str, extra: &[(String, String)]) -> Vec<(String, String)> {
    let mut v = vec![(URL_ENV.to_string(), url.to_string())];
    v.extend(extra.iter().cloned());
    v
}

fn env_refs(v: &[(String, String)]) -> Vec<(&str, &str)> {
    v.iter().map(|(k, x)| (k.as_str(), x.as_str())).collect()
}

/// A harness over a rig built with `source_url_env(URL_ENV)`, so a sealed plan can be applied; `all` drives every export.
fn rig_harness<'a>(rig: &'a Rig, extra: &[(String, String)], all: bool) -> Harness<'a> {
    let env = cell_env(rig.cdc_source_url(), extra);
    let (e1, e2, e3) = (env.clone(), env.clone(), env);
    let export = (!all).then(|| rig.export_name().to_string());
    let only = export.clone();
    Harness {
        config: rig.config_path(),
        export,
        cli: Box::new(move |args| rig.cli_env(args, &env_refs(&e1))),
        run: Box::new(move || {
            let sel: Vec<&str> = only.iter().flat_map(|e| ["--export", e.as_str()]).collect();
            rig.run_args_env(&sel, &env_refs(&e2))
        }),
        apply: Box::new(move |p| rig.apply_env(p, &[], &env_refs(&e3))),
        cdc: None,
        init: None,
        stream: false,
    }
}

/// [`rig_harness`] with `rivet cdc` over the rig's own stream as a second pre-run.
fn cdc_harness(rig: &Rig) -> Harness<'_> {
    let mut h = rig_harness(rig, &[], false);
    let own = rig.checkpoint().with_file_name("cdc-cli.ckpt");
    h.stream = true;
    h.cdc = Some(Box::new(move || {
        rig.cli_cdc_raw(false, &["--checkpoint", &own.display().to_string()])
    }));
    h
}

/// A config at `dir/rivet.yaml` run from `cwd`; the URL travels in the env when given.
fn raw_harness_in(
    yaml: String,
    dir: PathBuf,
    cwd: PathBuf,
    url: Option<String>,
) -> Harness<'static> {
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("rivet.yaml");
    std::fs::write(&config, yaml).unwrap();
    let (c1, c2) = (config.clone(), config.clone());
    let go = move |args: Vec<String>| {
        let env: Vec<(&str, &str)> = url.as_deref().map(|u| (URL_ENV, u)).into_iter().collect();
        rivet_command(&args, &env)
            .current_dir(&cwd)
            .output()
            .expect("spawn rivet")
    };
    let go = std::rc::Rc::new(go);
    let (go2, go3) = (go.clone(), go.clone());
    Harness {
        export: Some("pm_check_equals_run".into()),
        config,
        cli: Box::new(move |args| {
            let mut v: Vec<String> = args.iter().map(|s| s.to_string()).collect();
            v.extend(["--config".into(), c1.to_str().unwrap().into()]);
            go(v)
        }),
        run: Box::new(move || {
            go2([
                "run",
                "--config",
                c2.to_str().unwrap(),
                "--export",
                "pm_check_equals_run",
            ]
            .map(String::from)
            .to_vec())
        }),
        apply: Box::new(move |p| go3(vec!["apply".into(), p.to_str().unwrap().into()])),
        cdc: None,
        init: None,
        stream: false,
    }
}

fn raw_harness(yaml: String, dir: PathBuf, url: Option<String>) -> Harness<'static> {
    let cwd = dir.clone();
    raw_harness_in(yaml, dir, cwd, url)
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

fn raw_cell(e: SourceType, shape: Shape, url: &str) -> Harness<'static> {
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
    let leaves = cli_leaves();
    assert!(
        leaves.iter().any(|c| c == "run") && leaves.iter().any(|c| c == "state reset-chunks"),
        "{leaves:?}"
    );
    for leaf in &leaves {
        role_of(leaf);
    }
    let pairs: Vec<String> = pairs()
        .iter()
        .map(|p| format!("{}-{}", p.pre, p.exec))
        .collect();
    assert_eq!(
        pairs,
        [
            "check-run",
            "check-strict-run",
            "doctor-run",
            "cdc-run",
            "init-run",
            "plan-apply"
        ]
        .map(String::from),
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
    let types = source_types();
    assert!(types.len() >= 20, "{types:?}");
    for e in engines() {
        for ty in &types {
            let _ = native(e, ty);
        }
    }
    let spellings = override_spellings();
    assert!(spellings.len() >= 17, "{spellings:?}");
    for sp in &spellings {
        rivet::types::parse_type_str(sp).unwrap_or_else(|e| panic!("override {sp}: {e}"));
    }
    let fields = path_fields();
    assert!(
        fields.iter().any(|f| f == "ExportConfig.query_file"),
        "{fields:?}"
    );
    for f in &fields {
        let _ = path_case(f, Path::new("/nonexistent"));
    }
    for ty in destination_types() {
        assert!(
            DESTS.contains(&ty.as_str()),
            "DestinationType::{ty}: a new destination; give it a rig in dest_rig"
        );
    }
    assert_eq!(enum_idents("src/config/load.rs", "LoadTargetKind").len(), 3);
    let leaves = config_leaves("/nonexistent.yaml");
    assert!(
        leaves.iter().any(|(l, _)| l == "state finish-run"),
        "{leaves:?}"
    );
    assert!(prior_codes().iter().any(|c| recipe_of(c).is_some()));
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
    run_missing_config_cells(&mut rows, &codes);
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

/// The first line of a panic payload, cut to a verdict field.
fn panic_line(p: Box<dyn std::any::Any + Send>) -> String {
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
}

/// `f`'s value, or the first line of its panic when the stand cannot provide it.
fn caught<T>(f: impl FnOnce() -> T) -> Result<T, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(panic_line)
}

/// A table with rows on the stand for `engine`, or the reason it cannot be made.
fn fixture(engine: SourceType, mode: ExportMode) -> Result<Fixture, String> {
    caught(|| match sql_of(engine) {
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
    if m == ExportMode::Cdc && s == Shape::Clean && t.is_none() {
        return run_cdc_scenario_cell(rows, codes, e, false);
    }
    let out = tempfile::tempdir().expect("out dir");
    let slot = unique_name("pm_check_equals_run_slot");
    let _slot =
        (e == SourceType::Postgres && m == ExportMode::Cdc).then(|| Slot::new(slot.clone()));
    let rig = cell_rig(e, m, s, t, fx.as_ref(), out.path(), &slot)
        .source_url_env(URL_ENV)
        .no_oracle(NO_ORACLE);
    let h = match m {
        ExportMode::Cdc => cdc_harness(&rig),
        _ => rig_harness(&rig, &[], false),
    };
    let expect = if t.is_some() {
        Expect::Either
    } else {
        expect_of(e, m, s)
    };
    for (pair, o, ev) in grade_cell(&h, expect, codes) {
        emit(rows, &format!("{key}/{pair}"), o, &ev);
    }
    if let (ExportMode::Full, Shape::Clean, None, Some(f)) = (m, s, t, fx.as_ref()) {
        let url = f.mongo_url.clone().unwrap_or_else(|| live_url(e));
        run_init_cell(rows, codes, &key, &f.table, &url, &rig);
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

/// `rivet init` over `table` into `dir/rivet.yaml`, paired with a run of the scaffold, or of `fallback` when init wrote none.
fn init_harness<'a>(table: &str, url: &str, dir: &Path, fallback: &'a Rig) -> Harness<'a> {
    std::fs::create_dir_all(dir).unwrap();
    let config = dir.join("rivet.yaml");
    let env = cell_env(url, &[]);
    let (e1, e2, e3) = (env.clone(), env.clone(), env);
    let (c1, c2) = (config.clone(), config.clone());
    let (d1, d2) = (dir.to_path_buf(), dir.to_path_buf());
    let t = table.to_string();
    Harness {
        config,
        export: None,
        cli: Box::new(|_: &[&str]| -> Output { unreachable!("an init cell drives init alone") }),
        run: Box::new(move || {
            if c2.is_file() {
                rivet_command(&["run", "--config", c2.to_str().unwrap()], &env_refs(&e2))
                    .current_dir(&d2)
                    .output()
                    .expect("spawn rivet run")
            } else {
                fallback.run_args_env(&[], &env_refs(&e3))
            }
        }),
        apply: Box::new(|_: &Path| -> Output { unreachable!("an init cell drives init alone") }),
        cdc: None,
        stream: false,
        init: Some(Box::new(move || {
            rivet_command(
                &[
                    "init",
                    "--source-env",
                    URL_ENV,
                    "--table",
                    &t,
                    "-o",
                    c1.to_str().unwrap(),
                ],
                &env_refs(&e1),
            )
            .current_dir(&d1)
            .output()
            .expect("spawn rivet init")
        })),
    }
}

/// Grade `rivet init` over `table` against a run of what it wrote.
fn run_init_cell(
    rows: &mut Vec<String>,
    codes: &BTreeSet<String>,
    key: &str,
    table: &str,
    url: &str,
    fallback: &Rig,
) {
    let dir = tempfile::tempdir().expect("init dir");
    let h = init_harness(table, url, dir.path(), fallback);
    for (pair, o, ev) in grade_cell(&h, Expect::Pass, codes) {
        emit(rows, &format!("{key}/{pair}"), o, &ev);
    }
}

#[cfg(feature = "oracle")]
fn oracle_scenario(label: &str, shape: impl FnOnce(Rig, &str) -> Rig) -> CdcScenario {
    CdcScenario::oracle_with(label, shape)
}

#[cfg(not(feature = "oracle"))]
fn oracle_scenario(_: &str, _: impl FnOnce(Rig, &str) -> Rig) -> CdcScenario {
    panic!("oracle engine unavailable in this build")
}

/// A CDC table with its stream and a rig whose URL travels in [`URL_ENV`], or the reason the stand cannot make it.
fn cdc_scenario(e: SourceType) -> Result<CdcScenario, String> {
    const LABEL: &str = "pm_check_equals_run_cdc";
    const COLS: &str = "id INT PRIMARY KEY, v BIGINT";
    let shape = |r: Rig, _: &str| r.source_url_env(URL_ENV).no_oracle(NO_ORACLE);
    caught(|| match e {
        SourceType::Postgres => CdcScenario::pg_with(LABEL, COLS, shape),
        SourceType::Mysql => CdcScenario::mysql_with(LABEL, COLS, shape),
        SourceType::Mssql => CdcScenario::mssql_with(LABEL, COLS, shape),
        SourceType::Mongo => CdcScenario::mongo_with(LABEL, shape),
        SourceType::Oracle => oracle_scenario(LABEL, shape),
    })
}

/// The PostgreSQL slot a CDC rig's config names.
fn slot_of(rig: &Rig) -> String {
    let text = std::fs::read_to_string(rig.config_path()).expect("read the rig config");
    text.split(['\n', '{', ',', '}'])
        .map(str::trim)
        .find_map(|l| l.strip_prefix("slot:"))
        .map(|v| v.trim().trim_matches('"').to_string())
        .expect("a pg cdc config names its slot")
}

/// Remove the stream position a prior run stored: the PostgreSQL slot, or the checkpoint file elsewhere.
fn lose_checkpoint(e: SourceType, rig: &Rig) {
    match e {
        SourceType::Postgres => {
            let mut c = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls)
                .expect("connect postgres-cdc");
            c.execute("SELECT pg_drop_replication_slot($1)", &[&slot_of(rig)])
                .expect("drop the slot");
        }
        SourceType::Mysql | SourceType::Mssql | SourceType::Oracle | SourceType::Mongo => {
            std::fs::remove_file(rig.checkpoint()).expect("remove the checkpoint");
        }
    }
}

/// One CDC cell over a fresh stream: clean after one anchoring run, or with the stored position lost.
fn run_cdc_scenario_cell(
    rows: &mut Vec<String>,
    codes: &BTreeSet<String>,
    e: SourceType,
    lost: bool,
) {
    let key = format!(
        "{}/cdc/{}/-",
        engine_key(e),
        if lost {
            "prior-checkpoint-lost"
        } else {
            "clean"
        }
    );
    let mut sc = match cdc_scenario(e) {
        Ok(s) => s,
        Err(why) => {
            return emit(
                rows,
                &key,
                Outcome::NotGraded,
                &format!("no fixture: {why}"),
            );
        }
    };
    let env = cell_env(sc.rig.cdc_source_url(), &[]);
    let first = sc.rig.run_args_env(&[], &env_refs(&env));
    if !first.status.success() {
        let v = verdict_of(&first);
        return emit(
            rows,
            &key,
            Outcome::NotGraded,
            &format!("the anchoring run failed: {} {}", show(&v), v.first),
        );
    }
    let prep = caught(|| {
        sc.insert(1);
        sc.settle();
        if lost {
            sc.rig.run_args_env(&[], &env_refs(&env));
            lose_checkpoint(e, &sc.rig);
            sc.insert(2);
            sc.settle();
        }
    });
    if let Err(why) = prep {
        return emit(
            rows,
            &key,
            Outcome::NotGraded,
            &format!("no fixture: {why}"),
        );
    }
    let mut h = cdc_harness(&sc.rig);
    if lost {
        h.cdc = None;
    }
    let expect = if lost { Expect::Refuse } else { Expect::Pass };
    for (pair, o, ev) in grade_cell(&h, expect, codes) {
        emit(rows, &format!("{key}/{pair}"), o, &ev);
    }
}

/// Every type rivet maps a source column to, read from `RivetType`.
fn source_types() -> Vec<String> {
    enum_idents("src/types/rivet_type.rs", "RivetType")
}

type Native = Option<(&'static str, &'static str)>;

/// The native column and a literal for `ty` on postgres, mysql, mssql and oracle; `None` where the engine has no such column.
fn natives(ty: &str) -> [Native; 4] {
    match ty {
        "Bool" => [
            Some(("BOOLEAN", "TRUE")),
            Some(("TINYINT(1)", "1")),
            Some(("BIT", "1")),
            Some(("BOOLEAN", "TRUE")),
        ],
        "Int16" => [
            Some(("SMALLINT", "1")),
            Some(("SMALLINT", "1")),
            Some(("SMALLINT", "1")),
            Some(("NUMBER(4)", "1")),
        ],
        "Int32" => [
            Some(("INTEGER", "1")),
            Some(("INT", "1")),
            Some(("INT", "1")),
            Some(("NUMBER(9)", "1")),
        ],
        "Int64" => [
            Some(("BIGINT", "1")),
            Some(("BIGINT", "1")),
            Some(("BIGINT", "1")),
            Some(("NUMBER(18)", "1")),
        ],
        "UInt64" => [
            None,
            Some(("BIGINT UNSIGNED", "18446744073709551615")),
            None,
            None,
        ],
        "Float32" => [
            Some(("REAL", "1.5")),
            Some(("FLOAT", "1.5")),
            Some(("REAL", "1.5")),
            Some(("BINARY_FLOAT", "1.5")),
        ],
        "Float64" => [
            Some(("DOUBLE PRECISION", "1.5")),
            Some(("DOUBLE", "1.5")),
            Some(("FLOAT", "1.5")),
            Some(("BINARY_DOUBLE", "1.5")),
        ],
        "Decimal" => [
            Some(("NUMERIC(12,3)", "1.5")),
            Some(("DECIMAL(12,3)", "1.5")),
            Some(("DECIMAL(12,3)", "1.5")),
            Some(("NUMBER(12,3)", "1.5")),
        ],
        "Date" => [
            Some(("DATE", "'2024-01-02'")),
            Some(("DATE", "'2024-01-02'")),
            Some(("DATE", "'2024-01-02'")),
            Some(("DATE", "DATE '2024-01-02'")),
        ],
        "Time" => [
            Some(("TIME", "'01:02:03'")),
            Some(("TIME", "'01:02:03'")),
            Some(("TIME", "'01:02:03'")),
            None,
        ],
        "Timestamp" => [
            Some(("TIMESTAMPTZ", "'2024-01-02 03:04:05+00'")),
            Some(("DATETIME(6)", "'2024-01-02 03:04:05'")),
            Some(("DATETIME2", "'2024-01-02T03:04:05'")),
            Some(("TIMESTAMP", "TIMESTAMP '2024-01-02 03:04:05'")),
        ],
        "String" => [
            Some(("VARCHAR(20)", "'a'")),
            Some(("VARCHAR(20)", "'a'")),
            Some(("NVARCHAR(20)", "N'a'")),
            Some(("VARCHAR2(20)", "'a'")),
        ],
        "Text" => [
            Some(("TEXT", "'a'")),
            Some(("TEXT", "'a'")),
            Some(("NVARCHAR(MAX)", "N'a'")),
            Some(("CLOB", "'a'")),
        ],
        "Binary" => [
            Some(("BYTEA", "decode('01', 'hex')")),
            Some(("VARBINARY(8)", "X'01'")),
            Some(("VARBINARY(8)", "0x01")),
            Some(("RAW(8)", "HEXTORAW('01')")),
        ],
        "Json" => [
            Some(("JSONB", "'{\"a\":1}'")),
            Some(("JSON", "'{\"a\":1}'")),
            None,
            Some(("JSON", "'{\"a\":1}'")),
        ],
        "Uuid" => [
            Some(("UUID", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'")),
            None,
            Some(("UNIQUEIDENTIFIER", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'")),
            None,
        ],
        "Enum" => [None, Some(("ENUM('a','b')", "'a'")), None, None],
        "Interval" => [
            Some(("INTERVAL", "'1 day'")),
            None,
            None,
            Some(("INTERVAL DAY TO SECOND", "INTERVAL '1' DAY")),
        ],
        "List" => [Some(("INTEGER[]", "ARRAY[1,2]")), None, None, None],
        "Unsupported" => [
            Some(("POINT", "point(1,2)")),
            Some(("GEOMETRY", "ST_GeomFromText('POINT(1 2)')")),
            Some(("SQL_VARIANT", "1")),
            Some(("UROWID", "NULL")),
        ],
        other => panic!(
            "RivetType::{other}: a new variant; give it a native column per engine in natives()"
        ),
    }
}

/// The native column and literal of `ty` on `e`, or why the cell is not graded.
fn native(e: SourceType, ty: &str) -> Result<(&'static str, &'static str), String> {
    let i = match e {
        SourceType::Postgres => 0,
        SourceType::Mysql => 1,
        SourceType::Mssql => 2,
        SourceType::Oracle => 3,
        SourceType::Mongo => {
            return Err("a collection has no column DDL; its types come from documents".into());
        }
    };
    natives(ty)[i]
        .ok_or_else(|| format!("{} has no native column rivet reads as {ty}", engine_key(e)))
}

/// One cell per source type: a one-row table with a native column of that type, plus `rivet init` over it.
fn run_type_cells(rows: &mut Vec<String>, codes: &BTreeSet<String>, e: SourceType) {
    for ty in source_types() {
        let key = format!("{}/full/type-{}/-", engine_key(e), snake(&ty));
        let (ddl, lit) = match native(e, &ty) {
            Ok(x) => x,
            Err(why) => {
                emit(rows, &key, Outcome::NotGraded, &why);
                continue;
            }
        };
        let Some(eng) = sql_of(e) else {
            emit(
                rows,
                &key,
                Outcome::NotGraded,
                "engine unavailable in this build",
            );
            continue;
        };
        let id = if e == SourceType::Oracle {
            "NUMBER(9)"
        } else {
            "INT"
        };
        let made = caught(|| {
            eng.alive();
            let (t, g) = eng.create(
                "pm_check_equals_run_ty",
                &format!("id {id} PRIMARY KEY, v {ddl}"),
            );
            eng.exec(&format!("INSERT INTO {t} (id, v) VALUES (1, {lit})"));
            (t, g)
        });
        let (table, _guard) = match made {
            Ok(x) => x,
            Err(why) => {
                emit(
                    rows,
                    &key,
                    Outcome::NotGraded,
                    &format!("no fixture: {why}"),
                );
                continue;
            }
        };
        let out = tempfile::tempdir().expect("out dir");
        let rig = base_rig(e, &table, None)
            .mode("full")
            .dest_path(out.path().to_path_buf())
            .source_url_env(URL_ENV)
            .no_oracle(NO_ORACLE);
        let h = rig_harness(&rig, &[], false);
        for (pair, o, ev) in grade_cell(&h, Expect::Either, codes) {
            emit(rows, &format!("{key}/{pair}"), o, &ev);
        }
        run_init_cell(rows, codes, &key, &table, &live_url(e), &rig);
    }
}

/// Every spelling `parse_type_str` accepts by name: the first literal of each arm, and `<name>(12,3)` for each prefix form.
fn override_spellings() -> Vec<String> {
    let text = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/types/override_type.rs"),
    )
    .expect("read override_type.rs");
    let body = &text[text.find("fn parse_type_str").expect("parse_type_str")..];
    let mut out = Vec::new();
    for line in body.lines() {
        let t = line.trim();
        if t.starts_with("_ =>") {
            break;
        }
        if let Some(rest) = t.strip_prefix(".strip_prefix(\"") {
            out.push(format!("{}(12,3)", rest.split('(').next().unwrap()));
        } else if t.starts_with('"') && t.contains("=>") {
            out.push(t[1..].split('"').next().unwrap().to_string());
        }
    }
    out
}

/// The fixture columns an override is declared on, per engine.
fn override_probes(e: SourceType) -> &'static [&'static str] {
    match e {
        SourceType::Mongo => &["_id"],
        SourceType::Postgres | SourceType::Mysql | SourceType::Mssql | SourceType::Oracle => {
            &["time_spent", "server_time", "label"]
        }
    }
}

/// One cell per override spelling and probe column over the fixture table.
fn run_override_cells(rows: &mut Vec<String>, codes: &BTreeSet<String>, e: SourceType) {
    let fx = match fixture(e, ExportMode::Full) {
        Ok(f) => f,
        Err(why) => {
            return emit(
                rows,
                &format!("{}/full/override-any/-", engine_key(e)),
                Outcome::NotGraded,
                &format!("no fixture: {why}"),
            );
        }
    };
    for probe in override_probes(e) {
        for sp in override_spellings() {
            let key = format!(
                "{}/full/override-{probe}-{}/-",
                engine_key(e),
                sp.replace(['(', ')', ','], "_")
            );
            let out = tempfile::tempdir().expect("out dir");
            let rig = base_rig(e, &fx.table, fx.mongo_url.as_deref())
                .mode("full")
                .dest_path(out.path().to_path_buf())
                .export_line("columns:")
                .export_line(&format!("  {}: \"{sp}\"", spell(e, probe)))
                .source_url_env(URL_ENV)
                .no_oracle(NO_ORACLE);
            let h = rig_harness(&rig, &[], false);
            for (pair, o, ev) in grade_cell(&h, Expect::Either, codes) {
                emit(rows, &format!("{key}/{pair}"), o, &ev);
            }
        }
    }
}

/// How a prior run's stored state is provoked before a cell is graded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Recipe {
    CursorSwitched,
    DestinationMoved,
    CheckpointLost,
}

/// The stored-state codes of the registry: what a prior run can leave for the next one to refuse.
fn prior_codes() -> Vec<String> {
    const PREFIXES: &[&str] = &[
        "RIVET_STATE_CURSOR_",
        "RIVET_STATE_CHUNK_",
        "RIVET_STATE_CDC_",
        "RIVET_STATE_INTERRUPTED_",
        "RIVET_STATE_KEYSET_",
        "RIVET_SOURCE_CDC_CHECKPOINT_",
        "RIVET_SOURCE_CDC_FOREIGN_",
        "RIVET_SOURCE_CDC_LOG_GAP",
        "RIVET_SOURCE_CDC_TRUNCATED",
    ];
    registry()
        .into_iter()
        .filter(|c| PREFIXES.iter().any(|p| c.starts_with(p)))
        .collect()
}

fn recipe_of(code: &str) -> Option<Recipe> {
    match code {
        "RIVET_STATE_CURSOR_OWNER_MISMATCH" => Some(Recipe::CursorSwitched),
        "RIVET_STATE_CURSOR_DESTINATION_MISMATCH" => Some(Recipe::DestinationMoved),
        "RIVET_SOURCE_CDC_CHECKPOINT_INVALID" | "RIVET_SOURCE_CDC_LOG_GAP" => {
            Some(Recipe::CheckpointLost)
        }
        _ => None,
    }
}

fn recipe_key(r: Recipe) -> &'static str {
    match r {
        Recipe::CursorSwitched => "prior-cursor-switched",
        Recipe::DestinationMoved => "prior-destination-moved",
        Recipe::CheckpointLost => "prior-checkpoint-lost",
    }
}

/// An incremental config over `table` with cursor `cursor` writing to `out`.
fn incremental_yaml(e: SourceType, table: &str, cursor: &str, out: &Path) -> String {
    format!(
        "source:\n  type: {}\n  url_env: {URL_ENV}\nexports:\n  - name: pm_check_equals_run\n    table: {table}\n    mode: incremental\n    cursor_column: {cursor}\n    format: parquet\n    destination: {{ type: local, path: \"{}\" }}\n",
        engine_label(e),
        out.display()
    )
}

/// A batch prior-state cell: one incremental run, then the config changed under the stored cursor.
fn run_prior_batch_cell(
    rows: &mut Vec<String>,
    codes: &BTreeSet<String>,
    e: SourceType,
    r: Recipe,
) {
    let key = format!("{}/incremental/{}/-", engine_key(e), recipe_key(r));
    if e == SourceType::Mongo {
        return emit(
            rows,
            &key,
            Outcome::NotGraded,
            "a collection has one cursor (_id); the recipe needs a second column",
        );
    }
    let fx = match fixture(e, ExportMode::Incremental) {
        Ok(f) => f,
        Err(why) => {
            return emit(
                rows,
                &key,
                Outcome::NotGraded,
                &format!("no fixture: {why}"),
            );
        }
    };
    let dir = tempfile::tempdir().expect("dir").keep();
    let url = live_url(e);
    let first = incremental_yaml(e, &fx.table, &spell(e, "server_time"), &dir.join("out"));
    let h = raw_harness(first, dir.clone(), Some(url.clone()));
    let v = guarded(|| (h.run)());
    if v.exit != 0 {
        return emit(
            rows,
            &key,
            Outcome::NotGraded,
            &format!("the prior run failed: {} {}", show(&v), v.first),
        );
    }
    let second = match r {
        Recipe::CursorSwitched => {
            incremental_yaml(e, &fx.table, &spell(e, "ext_id"), &dir.join("out"))
        }
        Recipe::DestinationMoved => {
            incremental_yaml(e, &fx.table, &spell(e, "server_time"), &dir.join("out2"))
        }
        Recipe::CheckpointLost => unreachable!("a CDC recipe"),
    };
    let h = raw_harness(second, dir, Some(url));
    for (pair, o, ev) in grade_cell(&h, Expect::Refuse, codes) {
        emit(rows, &format!("{key}/{pair}"), o, &ev);
    }
}

/// The prior-state rows of one engine: each recipe once, graded.
fn run_prior_cells(rows: &mut Vec<String>, codes: &BTreeSet<String>, e: SourceType) {
    let recipes: BTreeSet<_> = prior_codes()
        .iter()
        .filter_map(|c| recipe_of(c))
        .map(|r| recipe_key(r))
        .collect();
    for r in [
        Recipe::CursorSwitched,
        Recipe::DestinationMoved,
        Recipe::CheckpointLost,
    ] {
        if !recipes.contains(recipe_key(r)) {
            continue;
        }
        match r {
            Recipe::CheckpointLost => run_cdc_scenario_cell(rows, codes, e, true),
            _ => run_prior_batch_cell(rows, codes, e, r),
        }
    }
}

/// A NOT_GRADED row for each stored-state code no recipe provokes yet.
fn emit_unprovoked_prior(rows: &mut Vec<String>) {
    for c in prior_codes() {
        if recipe_of(&c).is_none() {
            emit(
                rows,
                &format!(
                    "any/any/prior-{}/-",
                    c.trim_start_matches("RIVET_")
                        .to_lowercase()
                        .replace('_', "-")
                ),
                Outcome::NotGraded,
                "no recipe provokes this stored state from the CLI yet",
            );
        }
    }
}

/// Every config field that names a file or directory, from `rivet schema config`, as `Def.field`.
fn path_fields() -> Vec<String> {
    let out = rivet_command(&["schema", "config"], &[])
        .output()
        .expect("rivet schema config");
    assert!(out.status.success(), "rivet schema config failed");
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("schema json");
    let mut fields = Vec::new();
    for (def, body) in v["$defs"].as_object().expect("$defs") {
        for prop in body["properties"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, _)| k)
        {
            if prop.ends_with("_file")
                || prop.ends_with("_path")
                || prop == "path"
                || prop == "checkpoint"
            {
                fields.push(format!("{def}.{prop}"));
            }
        }
    }
    fields
}

/// A path cell: the file the field names, its content, the config naming it relatively, and the expected verdict per place.
struct PathCase {
    file: &'static str,
    content: String,
    yaml: String,
    places: Vec<(&'static str, Expect)>,
}

/// The cell of one path field on postgres, or why it is not graded; an unknown field panics.
fn path_case(field: &str, out: &Path) -> Result<PathCase, &'static str> {
    let dest = format!(
        "destination: {{ type: local, path: \"{}\" }}",
        out.display()
    );
    let yaml = |src: &str, query: &str, dest: &str| {
        format!(
            "source:\n  type: postgres\n  {src}\nexports:\n  - name: pm_check_equals_run\n    {query}\n    mode: full\n    format: parquet\n    {dest}\n"
        )
    };
    let both = vec![("beside", Expect::Pass), ("cwd-only", Expect::Refuse)];
    let env = format!("url_env: {URL_ENV}");
    let q = "query: \"SELECT 1 AS id\"";
    match field {
        "SourceConfig.url_file" => Ok(PathCase {
            file: "url.txt",
            content: POSTGRES_URL.to_string(),
            yaml: yaml("url_file: url.txt", q, &dest),
            places: both,
        }),
        "ExportConfig.query_file" => Ok(PathCase {
            file: "q.sql",
            content: "SELECT 1 AS id".into(),
            yaml: yaml(&env, "query_file: q.sql", &dest),
            places: both,
        }),
        "DestinationConfig.path" => Ok(PathCase {
            file: "out/.keep",
            content: String::new(),
            yaml: yaml(&env, q, "destination: { type: local, path: out }"),
            places: vec![("beside", Expect::Pass)],
        }),
        "TlsConfig.ca_file" => Ok(PathCase {
            file: "ca.pem",
            content: "-----BEGIN CERTIFICATE-----\nnot a certificate\n-----END CERTIFICATE-----\n"
                .into(),
            yaml: yaml(
                &format!("{env}\n  tls: {{ mode: verify-full, ca_file: ca.pem }}"),
                q,
                &dest,
            ),
            places: vec![("beside", Expect::Refuse)],
        }),
        "DestinationConfig.credentials_file" => Ok(PathCase {
            file: "creds.json",
            content: "{}".into(),
            yaml: yaml(
                &env,
                q,
                "destination: { type: gcs, bucket: pm-check-equals-run, prefix: pm_check_equals_run/, credentials_file: creds.json }",
            ),
            places: vec![("beside", Expect::Refuse)],
        }),
        "CdcExportConfig.checkpoint" => Err(
            "a CDC checkpoint path is written by the run, not read before it; the cdc rows cover it",
        ),
        "LoadSection.private_key_path" | "LoadSection.ca_file" => {
            Err("a load: block path is read by rivet load, which this matrix does not drive")
        }
        other => panic!("config field {other}: a new path field; classify it in path_case"),
    }
}

/// The cwd x path cells on postgres: each relative path beside the config and only in the cwd.
fn run_path_cells(rows: &mut Vec<String>, codes: &BTreeSet<String>) {
    for field in path_fields() {
        let root = tempfile::tempdir().expect("path dir").keep();
        let key = format!("postgres/full/path-{}", field.replace('.', "-"));
        let case = match path_case(&field, &root.join("out")) {
            Ok(c) => c,
            Err(why) => {
                emit(rows, &format!("{key}/-"), Outcome::NotGraded, why);
                continue;
            }
        };
        for (place, expect) in &case.places {
            let base = root.join(place);
            let (dir, cwd) = (base.join("cfg"), base.join("cwd"));
            let at = if *place == "beside" { &dir } else { &cwd };
            let file = at.join(case.file);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::create_dir_all(&cwd).unwrap();
            std::fs::write(&file, &case.content).unwrap();
            let h = raw_harness_in(
                case.yaml.clone(),
                dir,
                cwd,
                Some(live_url(SourceType::Postgres)),
            );
            for (pair, o, ev) in grade_cell(&h, *expect, codes) {
                emit(rows, &format!("{key}-{place}/-/{pair}"), o, &ev);
            }
        }
    }
}

const BUCKET: &str = "pm-check-equals-run";

const DESTS: &[&str] = &["Local", "S3", "Gcs", "Azure", "Stdout"];

/// Every destination type, read from `DestinationType`.
fn destination_types() -> Vec<String> {
    enum_idents("src/config/destination.rs", "DestinationType")
}

/// The rig over `table` writing to destination `ty`, with the env it needs.
fn dest_rig(ty: &str, table: &str, out: &Path) -> (Rig, Vec<(String, String)>) {
    let prefix = format!("{}/", unique_name("pm_check_equals_run_d"));
    let rig = base_rig(SourceType::Postgres, table, None).mode("full");
    let kv = |k: &str, v: &str| (k.to_string(), v.to_string());
    match ty {
        "Local" => (rig.dest_path(out.to_path_buf()), vec![]),
        "S3" => {
            ensure_minio_bucket(BUCKET);
            (
                rig.dest_s3(BUCKET, &prefix, MINIO_ENDPOINT),
                vec![
                    kv("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
                    kv("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
                    kv("AWS_EC2_METADATA_DISABLED", "true"),
                ],
            )
        }
        "Gcs" => {
            ensure_gcs_bucket(BUCKET);
            (rig.dest_gcs(BUCKET, &prefix, FAKE_GCS_ENDPOINT), vec![])
        }
        "Azure" => {
            ensure_azure_container(BUCKET);
            (
                rig.dest_azure(BUCKET, &prefix),
                vec![kv("RIVET_TEST_AZURITE_KEY", AZURITE_KEY)],
            )
        }
        "Stdout" => (rig.dest_stdout(), vec![]),
        other => panic!("DestinationType::{other}: a new destination; give it a rig in dest_rig"),
    }
}

/// The destination x export-count cells on postgres, every export driven at once.
fn run_dest_cells(rows: &mut Vec<String>, codes: &BTreeSet<String>) {
    let fx = match fixture(SourceType::Postgres, ExportMode::Full) {
        Ok(f) => f,
        Err(why) => {
            return emit(
                rows,
                "postgres/full/dest-any/-",
                Outcome::NotGraded,
                &format!("no fixture: {why}"),
            );
        }
    };
    for ty in destination_types() {
        for n in [1, 2] {
            let key = format!("postgres/full/dest-{}-x{n}/-", ty.to_lowercase());
            let out = tempfile::tempdir().expect("out dir");
            let made = caught(|| dest_rig(&ty, &fx.table, out.path()));
            let (rig, env) = match made {
                Ok(x) => x,
                Err(why) => {
                    emit(
                        rows,
                        &key,
                        Outcome::NotGraded,
                        &format!("no fixture: {why}"),
                    );
                    continue;
                }
            };
            let rig = match n {
                2 => rig.also_export("pm_check_equals_run_b", "SELECT 1 AS id"),
                _ => rig,
            };
            let rig = rig.source_url_env(URL_ENV).no_oracle(NO_ORACLE);
            let h = rig_harness(&rig, &env, true);
            for (pair, o, ev) in grade_cell(&h, Expect::Either, codes) {
                emit(rows, &format!("{key}/{pair}"), o, &ev);
            }
        }
    }
}

/// A NOT_GRADED row per warehouse adapter of `load:`, read from `LoadTargetKind`.
fn emit_load_rows(rows: &mut Vec<String>) {
    for k in enum_idents("src/config/load.rs", "LoadTargetKind") {
        emit(
            rows,
            &format!("any/full/load-{}/-/load-run", k.to_lowercase()),
            Outcome::NotGraded,
            "rivet load has no pre-run command of its own; its adapter needs a warehouse fixture per target",
        );
    }
}

/// The value the matrix gives a required flag of a config leaf; an unknown flag panics.
fn value_for(flag: &str) -> &'static str {
    match flag {
        "--export" => "pm_check_equals_run",
        "--run-id" => "1",
        other => panic!("required flag {other}: give it a value in value_for"),
    }
}

/// Every leaf whose usage takes `--config <CONFIG>`, with its argv over `config`, from `rivet schema cli`.
fn config_leaves(config: &str) -> Vec<(String, Vec<String>)> {
    let out = rivet_command(&["schema", "cli"], &[])
        .output()
        .expect("rivet schema cli");
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    let mut v = Vec::new();
    for l in text.lines() {
        let Some(u) = l
            .strip_prefix("**Usage:** `rivet ")
            .and_then(|u| u.strip_suffix('`'))
        else {
            continue;
        };
        if !u.contains("--config <CONFIG>") {
            continue;
        }
        let toks: Vec<&str> = u.split_whitespace().collect();
        let leaf: Vec<&str> = toks
            .iter()
            .take_while(|t| !t.starts_with(['[', '-', '<']))
            .copied()
            .collect();
        let mut argv: Vec<String> = leaf.iter().map(|s| s.to_string()).collect();
        let mut i = leaf.len();
        while i < toks.len() {
            let t = toks[i].trim_start_matches('<');
            if t.starts_with('[') {
                i += 1;
                continue;
            }
            if t == "--config" {
                argv.extend(["--config".into(), config.into()]);
                i += 2;
                continue;
            }
            if t.starts_with("--") {
                argv.extend([t.to_string(), value_for(t).to_string()]);
                i += 2;
                continue;
            }
            panic!(
                "usage of rivet {}: token {t} not understood",
                leaf.join(" ")
            );
        }
        v.push((leaf.join(" "), argv));
    }
    v
}

/// Each config leaf on a config path that does not exist, against `run` on the same path.
fn run_missing_config_cells(rows: &mut Vec<String>, codes: &BTreeSet<String>) {
    let dir = tempfile::tempdir().expect("dir");
    let missing = dir.path().join("missing.yaml");
    let m = missing.to_str().unwrap();
    let call = |argv: &[String]| {
        guarded(|| {
            rivet_command(argv, &[])
                .current_dir(dir.path())
                .output()
                .expect("spawn rivet")
        })
    };
    let run = call(&["run", "--config", m, "--export", "pm_check_equals_run"].map(String::from));
    for (leaf, argv) in config_leaves(m) {
        if leaf == "run" {
            continue;
        }
        let pre = call(&argv);
        let (o, ev) = grade(&pre, &run, Expect::Refuse, codes);
        emit(
            rows,
            &format!("any/any/config-missing/-/{}-run", leaf.replace(' ', "-")),
            o,
            &ev,
        );
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
    for e in selected_engines() {
        run_prior_cells(&mut rows, &codes, e);
        run_type_cells(&mut rows, &codes, e);
        run_override_cells(&mut rows, &codes, e);
    }
    if selected_engines().contains(&SourceType::Mysql) {
        run_mysql_grant_gap(&mut rows, &codes);
    }
    if selected_engines().contains(&SourceType::Postgres) {
        run_path_cells(&mut rows, &codes);
        run_dest_cells(&mut rows, &codes);
    }
    run_raw_cells(false, &live_url, &mut rows, &codes);
    run_missing_config_cells(&mut rows, &codes);
    emit_unprovoked_prior(&mut rows);
    emit_load_rows(&mut rows);
    for leaf in cli_leaves() {
        if let Role::Unpaired(why) = role_of(&leaf) {
            emit(
                &mut rows,
                &format!("any/any/any/-/{}", leaf.replace(' ', "-")),
                Outcome::NotGraded,
                why,
            );
        }
    }
    summary(&rows);
    if let Ok(path) = std::env::var("PM_CHECK_EQUALS_RUN_LOG") {
        std::fs::write(path, rows.join("\n") + "\n").expect("write the verdict log");
    }
    assert_ledger(&rows, std::env::var("PM_CHECK_EQUALS_RUN_ENGINE").is_err());
}
