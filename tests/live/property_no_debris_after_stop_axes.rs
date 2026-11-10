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

/// The source engines of `SourceType`, as cell keys spell them.
pub const ENGINES: [&str; 5] = ["mysql", "pg", "mssql", "oracle", "mongo"];

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
            Shape::Load | Shape::MongoFull => Some(("full", &[])),
            Shape::TimeWindow | Shape::Cdc => None,
        }
    }

    /// Why a cell of this shape cannot run here, else `None`.
    pub fn not_built(self) -> Option<&'static str> {
        match self {
            Shape::Full
            | Shape::Incremental
            | Shape::Range
            | Shape::Keyset
            | Shape::Load
            | Shape::MongoFull => None,
            Shape::TimeWindow => Some("the time_window shape is not built in this matrix"),
            Shape::Cdc => Some(
                "the CDC leg has no equal-re-run rule yet: a change log delivered at least once may repeat a change, and which repeat is debris is not decided",
            ),
        }
    }

    /// Whether a run of `engine` can take this shape.
    pub fn fits(self, engine: &str) -> bool {
        match self {
            Shape::MongoFull => engine == "mongo",
            Shape::Full | Shape::Incremental | Shape::TimeWindow | Shape::Range | Shape::Keyset => {
                engine != "mongo"
            }
            Shape::Cdc | Shape::Load => true,
        }
    }

    /// Why `engine` cannot run a cell of this shape on the stand, else `None`.
    pub fn off_stand_for(self, engine: &str) -> Option<&'static str> {
        match self {
            Shape::Load => (engine == "mongo").then_some(
                "a MongoDB export is _id plus a JSON document column; the ClickHouse id read of the load cell is not built for it",
            ),
            Shape::Full
            | Shape::Incremental
            | Shape::TimeWindow
            | Shape::Range
            | Shape::Keyset
            | Shape::Cdc
            | Shape::MongoFull => None,
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
                Shape::MongoFull,
            ],
            Family::Single => &[
                Shape::Full,
                Shape::Incremental,
                Shape::TimeWindow,
                Shape::MongoFull,
            ],
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

/// A `load: target` of `LoadTarget`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Warehouse {
    Bigquery,
    Snowflake,
    Clickhouse,
}

impl Warehouse {
    pub const ALL: [Warehouse; 3] = [
        Warehouse::Bigquery,
        Warehouse::Snowflake,
        Warehouse::Clickhouse,
    ];

    /// Its `target:` word.
    pub fn label(self) -> &'static str {
        match self {
            Warehouse::Bigquery => "bigquery",
            Warehouse::Snowflake => "snowflake",
            Warehouse::Clickhouse => "clickhouse",
        }
    }

    /// Why a load into it is not graded on the stand, else `None`.
    pub fn off_stand(self) -> Option<&'static str> {
        match self {
            Warehouse::Bigquery => Some("BigQuery is a cloud warehouse, the stand has none"),
            Warehouse::Snowflake => Some("Snowflake is a cloud warehouse, the stand has none"),
            Warehouse::Clickhouse => None,
        }
    }

    /// The warehouses a load hook in `file` serves: the one its path names, or all for shared load code.
    pub fn serving(file: &str) -> Vec<Warehouse> {
        let named: Vec<Warehouse> = Warehouse::ALL
            .into_iter()
            .filter(|w| file.contains(w.label()))
            .collect();
        if named.is_empty() {
            Warehouse::ALL.to_vec()
        } else {
            named
        }
    }
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
    HostCrash,
    SourceEvent,
}

impl Stop {
    pub const ALL: [Stop; 5] = [
        Stop::Kill,
        Stop::Term,
        Stop::Fault,
        Stop::HostCrash,
        Stop::SourceEvent,
    ];

    /// Its label in a cell key.
    pub fn label(self) -> &'static str {
        match self {
            Stop::Fault => "fault",
            Stop::Term => "sigterm",
            Stop::Kill => "sigkill",
            Stop::HostCrash => "host_crash",
            Stop::SourceEvent => "source_event",
        }
    }

    /// Why this stop cannot be made at a hook of `kind`, else `None`.
    pub fn impossible_at(self, kind: Kind) -> Option<&'static str> {
        match (self, kind.faults()) {
            (Stop::HostCrash, _) => Some(
                "a host crash loses the page cache and unsynced writes; the stand has no VM or fault-injecting filesystem to make one",
            ),
            (Stop::SourceEvent, _) => Some(
                "a source event (failover, slot or log loss) stops a CDC stream; the CDC leg is not built",
            ),
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
            .or_else(|| self.shape.off_stand_for(&self.engine).map(String::from))
            .or_else(|| self.off_stand_load())
    }

    /// Why a load cell's hook serves no warehouse of the stand, else `None`.
    fn off_stand_load(&self) -> Option<String> {
        if self.shape != Shape::Load {
            return None;
        }
        let serving = Warehouse::serving(&self.hook.file);
        if serving.iter().any(|w| w.off_stand().is_none()) {
            return None;
        }
        Some(
            serving
                .iter()
                .filter_map(|w| w.off_stand())
                .collect::<Vec<_>>()
                .join("; "),
        )
    }
}

/// The CamelCase variants of `enum_name` in `rel`, one per line of the enum body.
pub fn enum_variants(root: &Path, rel: &str, enum_name: &str) -> BTreeSet<String> {
    let text = std::fs::read_to_string(root.join(rel)).expect("read the enum's file");
    let needle = format!("enum {enum_name} {{");
    let body = &text[text.find(&needle).expect("enum not found") + needle.len()..];
    let body = &body[..body.find("\n}").expect("enum end")];
    body.lines()
        .map(str::trim)
        .filter(|l| !l.starts_with("//") && !l.starts_with('#') && !l.is_empty())
        .filter_map(|l| {
            let id: String = l
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            id.chars()
                .next()
                .filter(char::is_ascii_uppercase)
                .map(|_| id)
        })
        .collect()
}

/// `CamelCase` as `snake_case`.
pub fn snake(camel: &str) -> String {
    let mut out = String::new();
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// The codes of `docs/reference/errors.md` whose kind is `kind`.
pub fn codes_of_kind(root: &Path, kind: &str) -> Vec<String> {
    let text =
        std::fs::read_to_string(root.join("docs/reference/errors.md")).expect("read errors.md");
    text.lines()
        .filter_map(|l| {
            let cols: Vec<&str> = l.split('|').map(str::trim).collect();
            (cols.len() > 3 && cols[2] == kind).then(|| cols[1].trim_matches('`').to_string())
        })
        .filter(|c| c.starts_with("RIVET_"))
        .collect()
}

/// The mutations a source takes between a stop and its re-run that are not a `ChangeOp`.
pub const SCHEMA_MUTATIONS: [&str; 5] = [
    "add_column",
    "drop_recreate",
    "truncate",
    "quiet_source",
    "multi_statement_txn",
];

/// The ways a re-run meets its state that are not a `StateRef` backend.
pub const STATE_TOPOLOGIES: [&str; 3] =
    ["second_state_db", "foreign_state_load", "prior_checkpoint"];

/// The stops every axis below crosses: a fault or a parked SIGKILL at the points a run commits through.
pub const POINTS: [(&str, Stop); 4] = [
    ("after_file_write", Stop::Fault),
    ("before_commit_rename", Stop::Kill),
    ("after_source_read", Stop::Kill),
    ("after_chunk_file", Stop::Fault),
];

/// An axis the base cells hold fixed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Axis {
    Warehouse,
    Mutation,
    Refusal,
    RunEnd,
    Entry,
    State,
    Parallel,
    Destination,
}

impl Axis {
    pub const AT_POINTS: [Axis; 7] = [
        Axis::Mutation,
        Axis::Refusal,
        Axis::RunEnd,
        Axis::Entry,
        Axis::State,
        Axis::Parallel,
        Axis::Destination,
    ];

    /// Its label in a variant key.
    pub fn label(self) -> &'static str {
        match self {
            Axis::Warehouse => "warehouse",
            Axis::Mutation => "mutation",
            Axis::Refusal => "refusal",
            Axis::RunEnd => "run_end",
            Axis::Entry => "entry",
            Axis::State => "state",
            Axis::Parallel => "parallel",
            Axis::Destination => "destination",
        }
    }

    /// Its values other than the one the base cells hold, read from the code.
    pub fn values(self, root: &Path) -> Vec<String> {
        match self {
            Axis::Warehouse => Warehouse::ALL
                .iter()
                .map(|w| w.label().to_string())
                .collect(),
            Axis::Mutation => enum_variants(root, "src/source/cdc/mod.rs", "ChangeOp")
                .iter()
                .map(|v| snake(v))
                .chain(SCHEMA_MUTATIONS.iter().map(|m| m.to_string()))
                .collect(),
            Axis::Refusal => codes_of_kind(root, "busy"),
            Axis::RunEnd => vec!["failure_webhook".into()],
            Axis::Entry => enum_variants(root, "src/cli/args.rs", "Commands")
                .iter()
                .map(|v| snake(v))
                .filter(|v| v != "run")
                .collect(),
            Axis::State => enum_variants(root, "src/state/mod.rs", "StateRef")
                .iter()
                .map(|v| snake(v))
                .filter(|v| v != "sqlite")
                .chain(STATE_TOPOLOGIES.iter().map(|t| t.to_string()))
                .collect(),
            Axis::Parallel => {
                let schema = std::fs::read_to_string(root.join("schemas/rivet.schema.json"))
                    .expect("read the config schema");
                if schema.contains("\"parallel\": {") {
                    vec!["4".into()]
                } else {
                    vec![]
                }
            }
            Axis::Destination => {
                enum_variants(root, "src/config/destination.rs", "DestinationType")
                    .iter()
                    .map(|v| snake(v))
                    .filter(|v| v != "local")
                    .collect()
            }
        }
    }

    /// Why `value` of this axis cannot be graded at `cell`, else `None`.
    pub fn why_not(self, value: &str, cell: &Cell) -> Option<String> {
        let parked = cell.stop == Stop::Kill;
        let sql = cell.engine != "mongo";
        let why = match self {
            Axis::Warehouse => return None,
            Axis::Mutation => match value {
                "quiet_source" | "multi_statement_txn" => {
                    Some("a CDC-only mutation; the CDC leg is not built")
                }
                _ if !sql => Some("the mutation statements are SQL; MongoDB takes none"),
                _ if !parked => {
                    Some("a mutation lands while the run is parked; a fault point does not park")
                }
                "insert" | "update" | "delete" | "add_column" | "drop_recreate" | "truncate" => {
                    None
                }
                _ => Some("no mutation statement is built for this value"),
            },
            Axis::Refusal => match value {
                "RIVET_STATE_RUN_IN_PROGRESS" if parked => None,
                "RIVET_STATE_RUN_IN_PROGRESS" => Some(
                    "a second run beside a live one needs the first parked; a fault point does not park",
                ),
                _ => Some("no trigger is built for this busy code in this matrix"),
            },
            Axis::RunEnd => match value {
                "failure_webhook" if cell.stop == Stop::Fault => None,
                "failure_webhook" => {
                    Some("a killed process sends no webhook; only a fault ends in one")
                }
                _ => Some("no run-end leg is built for this value"),
            },
            Axis::Entry => match value {
                "apply" if !sql => Some(
                    "the plan/apply leg reconnects through source_url_env, built for the SQL rigs only",
                ),
                "apply" if cell.stop == Stop::Fault => None,
                "apply" => Some(
                    "the rig spawns only `rivet run` to park and signal; apply is stopped by a fault",
                ),
                "state" if parked => None,
                "state" => Some("state finish-run closes a dead run; a fault point leaves none"),
                "load" => Some("a load is graded on the warehouse axis"),
                "cdc" => Some("the CDC leg is not built"),
                "compact" => Some("compact merges a warehouse change log, it runs no export"),
                _ => Some("this command runs no export"),
            },
            Axis::State => match value {
                "postgres" => None,
                "second_state_db" if sql => None,
                "second_state_db" => {
                    Some("a second rig over the same MongoDB database is not built")
                }
                "foreign_state_load" => {
                    Some("loading a state written by another config or release is not built")
                }
                "prior_checkpoint" => Some(
                    "a checkpoint from an earlier run is a CDC resume; the CDC leg is not built",
                ),
                _ => Some("no state topology is built for this value"),
            },
            Axis::Parallel => match cell.shape {
                Shape::Range | Shape::Keyset => None,
                _ => Some("parallel fans out range and keyset chunks only"),
            },
            Axis::Destination => match value {
                "stdout" => Some("a stdout destination keeps nothing to grade"),
                _ if cell.hook.file == "src/destination/local.rs" => Some(
                    "the hook is in the local destination; a cloud destination never reaches it",
                ),
                "s3" | "gcs" | "azure" => None,
                _ => Some("no puller is built for this destination"),
            },
        };
        why.map(String::from)
    }
}

/// A base cell crossed with one value of an axis the base cells hold fixed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Variant {
    pub base: Cell,
    pub axis: Axis,
    pub value: String,
    pub why_not: Option<String>,
}

impl Variant {
    /// `engine/shape/hook/stop@axis=value`.
    pub fn key(&self) -> String {
        format!("{}@{}={}", self.base.key(), self.axis.label(), self.value)
    }

    /// Why this variant cannot run, else `None`.
    pub fn not_graded(&self) -> Option<String> {
        self.why_not.clone().or_else(|| self.base.not_graded())
    }
}

/// The load cells of `base` crossed with each other warehouse their hook serves; the base cell runs on ClickHouse.
pub fn warehouse_variants(base: &[Cell]) -> Vec<Variant> {
    let mut out = vec![];
    for cell in base.iter().filter(|c| c.shape == Shape::Load) {
        let serving = Warehouse::serving(&cell.hook.file);
        if !serving.contains(&Warehouse::Clickhouse) {
            continue;
        }
        for w in serving {
            let why_not = match w {
                Warehouse::Clickhouse => continue,
                Warehouse::Bigquery | Warehouse::Snowflake => w.off_stand(),
            };
            out.push(Variant {
                base: cell.clone(),
                axis: Axis::Warehouse,
                value: w.label().to_string(),
                why_not: why_not.map(String::from),
            });
        }
    }
    out
}

/// The runnable base cells at the [`POINTS`].
pub fn points(base: &[Cell]) -> Vec<&Cell> {
    base.iter()
        .filter(|c| c.not_graded().is_none())
        .filter(|c| POINTS.contains(&(c.hook.name.as_str(), c.stop)))
        .collect()
}

/// Every variant of every axis over the base cells.
pub fn variants(root: &Path, base: &[Cell]) -> Vec<Variant> {
    let mut out = warehouse_variants(base);
    for axis in Axis::AT_POINTS {
        let values = axis.values(root);
        for cell in points(base) {
            for value in &values {
                out.push(Variant {
                    base: cell.clone(),
                    axis,
                    value: value.clone(),
                    why_not: axis.why_not(value, cell),
                });
            }
        }
    }
    out.sort();
    out
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
            for shape in family.shapes().iter().filter(|s| s.fits(engine)) {
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

/// An accepted RED: the cell keys it covers (`*` matches any run of characters), the finding text it covers, and a dated reason that names what closes it.
pub struct KnownRed {
    pub keys: &'static str,
    pub finding: &'static str,
    pub reason: &'static str,
}

/// The accepted REDs of this matrix; a RED no entry covers fails the matrix, and so does an entry no RED meets in a whole run.
pub const KNOWN_RED: &[KnownRed] = &[];

/// Whether `key` matches `pattern`, where `*` matches any run of characters.
pub fn glob(pattern: &str, key: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    let mut rest = key;
    for (i, part) in parts.iter().enumerate() {
        if i == 0 {
            match rest.strip_prefix(part) {
                Some(r) => rest = r,
                None => return false,
            }
        } else if i == parts.len() - 1 {
            return rest.ends_with(part);
        } else {
            match rest.find(part) {
                Some(at) => rest = &rest[at + part.len()..],
                None => return false,
            }
        }
    }
    rest.is_empty()
}

/// The entry of [`KNOWN_RED`] that covers a RED of `key` with `evidence`, else `None`.
pub fn known_red(key: &str, evidence: &str) -> Option<&'static KnownRed> {
    KNOWN_RED
        .iter()
        .find(|k| glob(k.keys, key) && evidence.contains(k.finding))
}
