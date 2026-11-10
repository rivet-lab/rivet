//! no_debris_after_stop axes (no debris after a stop, and a re-run equals an uninterrupted run): every axis is read from the code, never typed here.
//! Hooks come from `src/` call sites and the `src/test_hook.rs` table, engines and modes from their enums; per-value data is a `match` with no `_` arm.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// A `src/test_hook.rs` primitive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    PanicAt,
    PanicAtChunk,
    ExitAtIndex,
    FailAt,
    ErrorAtIndex,
    TransientOnce,
    PauseAt,
    BlockAt,
}

impl Kind {
    pub const ALL: [Kind; 8] = [
        Kind::PanicAt,
        Kind::PanicAtChunk,
        Kind::ExitAtIndex,
        Kind::FailAt,
        Kind::ErrorAtIndex,
        Kind::TransientOnce,
        Kind::PauseAt,
        Kind::BlockAt,
    ];

    /// The primitive's function name in `src/test_hook.rs`.
    pub fn function(self) -> &'static str {
        match self {
            Kind::PanicAt => "maybe_panic_at",
            Kind::PanicAtChunk => "maybe_panic_at_chunk",
            Kind::ExitAtIndex => "maybe_exit_at_index",
            Kind::FailAt => "maybe_fail_at",
            Kind::ErrorAtIndex => "maybe_error_at_index",
            Kind::TransientOnce => "maybe_transient_once",
            Kind::PauseAt => "maybe_pause_at",
            Kind::BlockAt => "maybe_block_at",
        }
    }

    /// The environment variable that arms it.
    pub fn env(self) -> &'static str {
        match self {
            Kind::PanicAt | Kind::PanicAtChunk | Kind::ExitAtIndex => "RIVET_TEST_PANIC_AT",
            Kind::FailAt | Kind::ErrorAtIndex => "RIVET_TEST_ERROR_AT",
            Kind::TransientOnce => "RIVET_TEST_TRANSIENT_ONCE",
            Kind::PauseAt => "RIVET_TEST_PAUSE_AT",
            Kind::BlockAt => "RIVET_TEST_BLOCK_AT",
        }
    }

    /// Whether the armed value is `point:index`.
    pub fn indexed(self) -> bool {
        match self {
            Kind::PanicAtChunk | Kind::ExitAtIndex | Kind::ErrorAtIndex => true,
            Kind::PanicAt | Kind::FailAt | Kind::TransientOnce | Kind::PauseAt | Kind::BlockAt => {
                false
            }
        }
    }

    /// Whether arming it makes the run fail by itself (a stop with no signal).
    pub fn faults(self) -> bool {
        match self {
            Kind::PanicAt
            | Kind::PanicAtChunk
            | Kind::ExitAtIndex
            | Kind::FailAt
            | Kind::TransientOnce
            | Kind::ErrorAtIndex => true,
            Kind::PauseAt | Kind::BlockAt => false,
        }
    }

    /// Whether the run waits there, so a test can send a real signal at the point.
    pub fn parks(self) -> bool {
        !self.faults()
    }
}

/// The source engines of `SqlEngine`, lower case.
pub const ENGINES: [&str; 4] = ["mysql", "pg", "mssql", "oracle"];

/// The part of the product a hook lives in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Family {
    Commit,
    Single,
    Chunked,
    Keyset,
    Cdc,
    MysqlCdc,
    Load,
    Mongo,
    PgSource,
}

/// A way an export runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Shape {
    Full,
    Incremental,
    TimeWindow,
    Range,
    Keyset,
    Cdc,
    Load,
    MongoFull,
}

impl Shape {
    pub const ALL: [Shape; 8] = [
        Shape::Full,
        Shape::Incremental,
        Shape::TimeWindow,
        Shape::Range,
        Shape::Keyset,
        Shape::Cdc,
        Shape::Load,
        Shape::MongoFull,
    ];

    /// The `ExportMode` it runs as (`load` for the load legs).
    pub fn mode(self) -> &'static str {
        match self {
            Shape::Full | Shape::MongoFull => "full",
            Shape::Incremental => "incremental",
            Shape::TimeWindow => "time_window",
            Shape::Range | Shape::Keyset => "chunked",
            Shape::Cdc => "cdc",
            Shape::Load => "load",
        }
    }

    /// Its label in a cell key.
    pub fn label(self) -> &'static str {
        match self {
            Shape::Full => "full",
            Shape::Incremental => "incremental",
            Shape::TimeWindow => "time_window",
            Shape::Range => "range",
            Shape::Keyset => "keyset",
            Shape::Cdc => "cdc",
            Shape::Load => "load",
            Shape::MongoFull => "mongo_full",
        }
    }

    /// The `mode:` line and export lines of a batch shape over the standard table; `None` for a shape this matrix does not build.
    pub fn export(self) -> Option<(&'static str, &'static [&'static str])> {
        match self {
            Shape::Full => Some(("full", &[])),
            Shape::Incremental => Some(("incremental", &["cursor_column: id"])),
            Shape::Range => Some((
                "chunked",
                &[
                    "chunk_column: id",
                    "chunk_size: 5",
                    "chunk_checkpoint: true",
                ],
            )),
            Shape::Keyset => Some((
                "chunked",
                &[
                    "chunk_by_key: id",
                    "chunk_size: 4",
                    "chunk_checkpoint: true",
                ],
            )),
            Shape::TimeWindow | Shape::Cdc | Shape::Load | Shape::MongoFull => None,
        }
    }

    /// Why a cell of this shape cannot run here, else `None`.
    pub fn not_built(self) -> Option<&'static str> {
        match self {
            Shape::Full | Shape::Incremental | Shape::Range | Shape::Keyset => None,
            Shape::TimeWindow => Some("the time_window shape is not built in this matrix"),
            Shape::Cdc => Some(
                "the CDC runner needs the cdc stand profile and a change stream, not built in this matrix",
            ),
            Shape::Load => Some(
                "the load legs need a warehouse stage listing (BigQuery, Snowflake, ClickHouse), not built in this matrix",
            ),
            Shape::MongoFull => {
                Some("MongoDB is not a SqlEngine, its cells are not built in this matrix")
            }
        }
    }
}

impl Family {
    /// The shapes whose runs reach this family's hooks.
    pub fn shapes(self) -> &'static [Shape] {
        match self {
            Family::Commit => &[
                Shape::Full,
                Shape::Incremental,
                Shape::TimeWindow,
                Shape::Range,
                Shape::Keyset,
            ],
            Family::Single => &[Shape::Full, Shape::Incremental, Shape::TimeWindow],
            Family::Chunked => &[Shape::Range],
            Family::Keyset => &[Shape::Keyset],
            Family::Cdc | Family::MysqlCdc => &[Shape::Cdc],
            Family::Load => &[Shape::Load],
            Family::Mongo => &[Shape::MongoFull],
            Family::PgSource => &[
                Shape::Full,
                Shape::Incremental,
                Shape::TimeWindow,
                Shape::Range,
                Shape::Keyset,
            ],
        }
    }

    /// The one engine this family belongs to, if it belongs to one.
    pub fn only_engine(self) -> Option<&'static str> {
        match self {
            Family::PgSource => Some("pg"),
            Family::MysqlCdc => Some("mysql"),
            Family::Commit
            | Family::Single
            | Family::Chunked
            | Family::Keyset
            | Family::Cdc
            | Family::Load
            | Family::Mongo => None,
        }
    }
}

/// The family of a product file (path under the repo root), else `None`.
pub fn family_of(rel: &str) -> Option<Family> {
    const TABLE: &[(&str, Family)] = &[
        ("src/pipeline/commit.rs", Family::Commit),
        ("src/pipeline/run_store.rs", Family::Commit),
        ("src/destination/local.rs", Family::Commit),
        ("src/pipeline/single.rs", Family::Single),
        ("src/pipeline/chunked/", Family::Chunked),
        ("src/pipeline/keyset.rs", Family::Keyset),
        ("src/pipeline/cdc/", Family::Cdc),
        ("src/source/mysql/cdc.rs", Family::MysqlCdc),
        ("src/load/", Family::Load),
        ("src/pipeline/mongo_parallel.rs", Family::Mongo),
        ("src/source/postgres/", Family::PgSource),
    ];
    TABLE
        .iter()
        .find(|(prefix, _)| rel.starts_with(prefix))
        .map(|(_, f)| *f)
}

/// A fault point a product call site names.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Hook {
    pub name: String,
    pub kind: Kind,
    pub file: String,
    pub family: Option<Family>,
    pub indexed: bool,
}

impl Hook {
    /// The value that arms this hook for a fault or a park.
    pub fn armed(&self, ms: &str) -> String {
        match self.kind {
            Kind::PauseAt => format!("{}:{ms}", self.name),
            Kind::BlockAt => self.name.clone(),
            _ if self.indexed => format!("{}:0", self.name),
            _ => self.name.clone(),
        }
    }
}

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.filter_map(Result::ok) {
        let p = e.path();
        if p.is_dir() {
            rs_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// The first string literal of the call whose `(` is at `at`: its text, and whether it sits in a `format!` with a `{}` hole.
fn first_literal(text: &str, at: usize) -> Option<(String, bool)> {
    let rest = text[at..].trim_start();
    let (rest, formatted) = match rest.strip_prefix('&') {
        Some(r) => (r.trim_start(), true),
        None => (rest, false),
    };
    let rest = if formatted {
        rest.strip_prefix("format!(")?.trim_start()
    } else {
        rest
    };
    let body = rest.strip_prefix('"')?;
    let name = &body[..body.find('"')?];
    let hole = name.find('{');
    Some((
        name.split(['{', ':']).next()?.to_string(),
        formatted && hole.is_some(),
    ))
}

/// The names of the `src/test_hook.rs` doc table (`//! | \`name\` | ...`), `:{N}` stripped.
pub fn documented(root: &Path) -> BTreeSet<String> {
    let text = std::fs::read_to_string(root.join("src/test_hook.rs")).expect("read test_hook.rs");
    text.lines()
        .filter_map(|l| l.strip_prefix("//! | `"))
        .filter_map(|l| l.split('`').next())
        .map(|n| n.split(':').next().unwrap_or(n).to_string())
        .collect()
}

/// Every hook the product's call sites name, plus every documented name found as a literal in `src/` (a name passed through a variable).
pub fn hooks(root: &Path) -> Vec<Hook> {
    let mut files = vec![];
    rs_files(&root.join("src"), &mut files);
    files.sort();
    let mut found: BTreeSet<Hook> = BTreeSet::new();
    let mut texts = vec![];
    for f in &files {
        let rel = f
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "src/test_hook.rs" {
            continue;
        }
        let text = std::fs::read_to_string(f).expect("read a source file");
        for kind in Kind::ALL {
            let needle = format!("{}(", kind.function());
            for (i, _) in text.match_indices(&needle) {
                let before = text[..i].chars().next_back();
                if before.is_some_and(|c| c.is_alphanumeric() || c == '_') {
                    continue;
                }
                if let Some((name, hole)) = first_literal(&text, i + needle.len()) {
                    found.insert(Hook {
                        name,
                        kind,
                        file: rel.clone(),
                        family: family_of(&rel),
                        indexed: kind.indexed() || hole,
                    });
                }
            }
        }
        texts.push((rel, text));
    }
    for name in documented(root) {
        if found.iter().any(|h| h.name == name) {
            continue;
        }
        if let Some((rel, _)) = texts
            .iter()
            .find(|(_, t)| t.contains(&format!("\"{name}\"")))
        {
            let kind = if name.contains("pause") {
                Kind::PauseAt
            } else {
                Kind::PanicAt
            };
            found.insert(Hook {
                name,
                kind,
                file: rel.clone(),
                family: family_of(rel),
                indexed: false,
            });
        }
    }
    found.into_iter().collect()
}

/// How a cell stops the run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stop {
    Fault,
    Term,
    Kill,
}

impl Stop {
    pub const ALL: [Stop; 3] = [Stop::Kill, Stop::Term, Stop::Fault];

    /// Its label in a cell key.
    pub fn label(self) -> &'static str {
        match self {
            Stop::Fault => "fault",
            Stop::Term => "sigterm",
            Stop::Kill => "sigkill",
        }
    }

    /// Why this stop cannot be made at a hook of `kind`, else `None`.
    pub fn impossible_at(self, kind: Kind) -> Option<&'static str> {
        match (self, kind.faults()) {
            (Stop::Fault, false) => Some("the hook only parks the run, it injects no fault"),
            (Stop::Term | Stop::Kill, true) => Some(
                "the hook only faults: nothing parks the run there for a real signal (option: let maybe_panic_at honour RIVET_TEST_BLOCK_AT)",
            ),
            _ => None,
        }
    }
}

/// One cell of the matrix.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cell {
    pub engine: String,
    pub shape: Shape,
    pub hook: Hook,
    pub stop: Stop,
}

impl Cell {
    /// `engine/shape/hook/stop`.
    pub fn key(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.engine,
            self.shape.label(),
            self.hook.name,
            self.stop.label()
        )
    }

    /// Why this cell cannot run, else `None`.
    pub fn not_graded(&self) -> Option<String> {
        self.stop
            .impossible_at(self.hook.kind)
            .map(String::from)
            .or_else(|| self.shape.not_built().map(String::from))
    }
}

/// Every cell: each hook at each shape that reaches it, on each engine the hook's family allows, at each stop.
pub fn cells(root: &Path, engines: &[String]) -> Vec<Cell> {
    let mut out = vec![];
    for hook in hooks(root) {
        let Some(family) = hook.family else { continue };
        for engine in engines {
            if family.only_engine().is_some_and(|only| only != engine) {
                continue;
            }
            for shape in family.shapes() {
                for stop in Stop::ALL {
                    out.push(Cell {
                        engine: engine.clone(),
                        shape: *shape,
                        hook: hook.clone(),
                        stop,
                    });
                }
            }
        }
    }
    out.sort();
    let mut by_key: BTreeMap<String, Cell> = BTreeMap::new();
    for cell in out {
        let slot = by_key.entry(cell.key()).or_insert_with(|| cell.clone());
        if slot.not_graded().is_some() && cell.not_graded().is_none() {
            *slot = cell;
        }
    }
    by_key.into_values().collect()
}

/// A verdict of one cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Red,
    NotGraded,
}

impl Verdict {
    /// The word on a verdict line.
    pub fn word(self) -> &'static str {
        match self {
            Verdict::Pass => "PASS",
            Verdict::Red => "RED",
            Verdict::NotGraded => "NOT_GRADED",
        }
    }
}

/// `no_debris_after_stop <key> <verdict> <evidence>`, one line.
pub fn line(key: &str, v: Verdict, evidence: &str) -> String {
    let one = evidence.replace('\n', " ");
    format!("no_debris_after_stop {key} {} {one}", v.word())
}

/// `no_debris_after_stop SUMMARY pass=.. red=.. not_graded=..`.
pub fn summary(verdicts: &[Verdict]) -> String {
    let n = |w| verdicts.iter().filter(|v| **v == w).count();
    format!(
        "no_debris_after_stop SUMMARY pass={} red={} not_graded={}",
        n(Verdict::Pass),
        n(Verdict::Red),
        n(Verdict::NotGraded)
    )
}

/// The verdict an oracle's stdout means: anything but a well-formed answer that proves the oracle saw its canary is NOT_GRADED, never PASS.
pub fn oracle_verdict(stdout: Result<&str, String>) -> (Verdict, String) {
    let text = match stdout {
        Ok(t) => t,
        Err(why) => return (Verdict::NotGraded, format!("oracle did not answer: {why}")),
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text.trim().lines().last().unwrap_or(""))
    else {
        return (
            Verdict::NotGraded,
            format!("oracle answered no JSON: {text:?}"),
        );
    };
    let evidence = v["evidence"].as_str().unwrap_or("").to_string();
    if v["canary_flagged"] != serde_json::Value::Bool(true) && v["verdict"] != "NOT_GRADED" {
        return (
            Verdict::NotGraded,
            format!("oracle did not prove it sees planted debris: {text:?}"),
        );
    }
    match v["verdict"].as_str() {
        Some("PASS") => (Verdict::Pass, evidence),
        Some("RED") => (Verdict::Red, evidence),
        Some("NOT_GRADED") => (Verdict::NotGraded, evidence),
        other => (
            Verdict::NotGraded,
            format!("oracle answered the unknown verdict {other:?}"),
        ),
    }
}

/// What a stopped run left, counted from the native listing and the state rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Left {
    pub staged: usize,
    pub running: i64,
    pub leases: i64,
}

/// Why a graceful stop left more than the SIGKILL of the same point, else `None`.
pub fn term_worse_than_kill(term: &Left, kill: &Left) -> Option<String> {
    let more = |what: &str, t: i64, k: i64| {
        (t > k).then(|| format!("SIGTERM left {t} {what}, SIGKILL left {k}"))
    };
    more(
        "staged .tmp file(s)",
        term.staged as i64,
        kill.staged as i64,
    )
    .or_else(|| more("running run_status row(s)", term.running, kill.running))
    .or_else(|| more("run lease row(s)", term.leases, kill.leases))
}
