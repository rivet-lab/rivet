//! Engine-neutral replay of the statement-truth corpus recorded by `dev/pytools/statement_truth.py`.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// What a statement did to the captured set, as the server or rivet judged it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Class {
    None,
    Truncate,
    Drop,
    RenameTable,
    RenameColumn,
    StatementWrite,
    Privilege,
}

impl Class {
    /// The class a corpus names; a name this reader does not know stops the run.
    pub(crate) fn named(name: &str) -> Class {
        match name {
            "none" => Class::None,
            "truncate" => Class::Truncate,
            "drop" => Class::Drop,
            "rename_table" => Class::RenameTable,
            "rename_column" => Class::RenameColumn,
            "statement_write" => Class::StatementWrite,
            "privilege" => Class::Privilege,
            other => panic!("statement truth: unknown class `{other}` in the corpus"),
        }
    }
}

/// The first line of a fixture: what produced it.
#[derive(Debug, Deserialize)]
pub(crate) struct Header {
    pub(crate) engine: String,
    pub(crate) schema: u32,
    pub(crate) server: String,
    pub(crate) version: String,
    pub(crate) rows: usize,
    pub(crate) fold: bool,
    pub(crate) stand: usize,
    pub(crate) captured: Vec<String>,
}

/// One statement the server was sent: its bytes in the client charset and the verdict the server's behaviour owes.
#[derive(Debug, Deserialize)]
pub(crate) struct Row {
    pub(crate) engine: String,
    pub(crate) id: String,
    pub(crate) case: String,
    pub(crate) errno: u32,
    pub(crate) kind: String,
    pub(crate) touched: Vec<String>,
    pub(crate) src: String,
    pub(crate) cs: u16,
    pub(crate) conn: u16,
    pub(crate) mode: u64,
    pub(crate) db: String,
    pub(crate) sql: String,
    pub(crate) expect: String,
}

impl Row {
    /// The statement bytes exactly as the log carried them.
    pub(crate) fn bytes(&self) -> Vec<u8> {
        assert!(
            self.sql.len().is_multiple_of(2),
            "row {}: odd hex length",
            self.id
        );
        (0..self.sql.len())
            .step_by(2)
            .map(|i| {
                self.sql
                    .get(i..i + 2)
                    .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                    .unwrap_or_else(|| panic!("row {}: bad hex", self.id))
            })
            .collect()
    }

    /// The row as a failure message shows it: what the server did and the statement as sent.
    pub(crate) fn describe(&self) -> String {
        format!(
            "{} [{}] errno {} server {} touched {:?} charset {}/{} sql {:?}",
            self.id,
            self.src,
            self.errno,
            self.kind,
            self.touched,
            self.cs,
            self.conn,
            String::from_utf8_lossy(&self.bytes())
        )
    }

    /// The class the server's behaviour owes.
    pub(crate) fn owed(&self) -> Class {
        Class::named(&self.expect)
    }
}

/// A fixture: its header and every row.
#[derive(Debug)]
pub(crate) struct Corpus {
    pub(crate) header: Header,
    pub(crate) rows: Vec<Row>,
}

/// The directory holding the fixtures of one engine.
pub(crate) fn fixture_dir(engine: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("dev/fixtures/statement_truth")
        .join(engine)
}

/// Every fixture of an engine; a missing directory or an empty one stops the run, never passes it.
pub(crate) fn corpora(engine: &str) -> Vec<Corpus> {
    let dir = fixture_dir(engine);
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| {
            panic!(
                "statement truth: no fixture directory {}: {e}",
                dir.display()
            )
        })
        .map(|e| e.expect("fixture entry").path())
        .filter(|p| p.to_string_lossy().ends_with(".jsonl.gz"))
        .collect();
    paths.sort();
    assert!(
        !paths.is_empty(),
        "statement truth: {} holds no fixture",
        dir.display()
    );
    let all: Vec<Corpus> = paths.iter().map(|p| load(engine, p)).collect();
    for c in &all {
        assert_eq!(
            c.header.stand,
            all.len(),
            "statement truth: {} was generated for a stand of {} servers, {} fixtures stand in {}",
            c.header.server,
            c.header.stand,
            all.len(),
            dir.display()
        );
    }
    all
}

/// One fixture read whole; the header must name the engine and the file, and count its rows.
pub(crate) fn load(engine: &str, path: &Path) -> Corpus {
    let file = std::fs::File::open(path)
        .unwrap_or_else(|e| panic!("statement truth: cannot open {}: {e}", path.display()));
    let mut text = String::new();
    flate2::read::GzDecoder::new(file)
        .read_to_string(&mut text)
        .unwrap_or_else(|e| panic!("statement truth: cannot read {}: {e}", path.display()));
    let mut lines = text.lines();
    let header: Header = serde_json::from_str(lines.next().expect("fixture header"))
        .unwrap_or_else(|e| panic!("{}: bad header: {e}", path.display()));
    assert_eq!(
        header.schema,
        1,
        "{}: unknown fixture schema",
        path.display()
    );
    let rows: Vec<Row> = lines
        .map(|l| {
            serde_json::from_str(l).unwrap_or_else(|e| panic!("{}: bad row: {e}", path.display()))
        })
        .collect();
    assert_eq!(header.engine, engine, "{}: wrong engine", path.display());
    assert_eq!(
        path.file_name().and_then(|n| n.to_str()),
        Some(format!("{}.jsonl.gz", header.server).as_str()),
        "{}: header names another server",
        path.display()
    );
    assert_eq!(
        header.rows,
        rows.len(),
        "{}: fixture lost rows",
        path.display()
    );
    assert!(
        rows.iter().all(|r| r.engine == engine),
        "{}: foreign row",
        path.display()
    );
    Corpus { header, rows }
}

/// Where a reader and the server disagreed on a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Direction {
    Missed,
    Over,
    Wrong,
}

impl Direction {
    /// The disagreement between the class owed and the class the reader gave, if any.
    pub(crate) fn of(owed: Class, got: Class) -> Option<Direction> {
        match (owed, got) {
            (a, b) if a == b => None,
            (_, Class::None) => Some(Direction::Missed),
            (Class::None, _) => Some(Direction::Over),
            _ => Some(Direction::Wrong),
        }
    }
}

/// An accepted disagreement: the cases it covers, why it is accepted, and the most rows it may hold.
pub(crate) struct Known {
    pub(crate) direction: Direction,
    pub(crate) server: Option<&'static str>,
    pub(crate) case: &'static str,
    pub(crate) rows: usize,
    pub(crate) reason: &'static str,
}

impl Known {
    fn covers(&self, server: &str, case: &str, direction: Direction) -> bool {
        self.direction == direction
            && self.server.is_none_or(|s| s == server)
            && case.starts_with(self.case)
    }
}

/// Every row of a corpus replayed through `read`: the totals and the disagreements.
#[derive(Debug, Default)]
pub(crate) struct Tally {
    pub(crate) total: usize,
    pub(crate) agree: usize,
    pub(crate) missed: Vec<String>,
    pub(crate) over: Vec<String>,
    pub(crate) over_unlogged: usize,
    pub(crate) wrong: Vec<String>,
}

/// Replays each row of `corpus` through `read` and sorts the result by direction.
pub(crate) fn replay(corpus: &Corpus, read: impl Fn(&Row) -> Class) -> Tally {
    let mut t = Tally {
        total: corpus.rows.len(),
        ..Tally::default()
    };
    for row in &corpus.rows {
        match Direction::of(row.owed(), read(row)) {
            None => t.agree += 1,
            Some(Direction::Missed) => t.missed.push(row.id.clone()),
            Some(Direction::Over) => {
                t.over_unlogged += usize::from(row.src == "sent");
                t.over.push(row.id.clone());
            }
            Some(Direction::Wrong) => t.wrong.push(row.id.clone()),
        }
    }
    t
}

/// The line a run prints for one fixture.
pub(crate) fn summary(corpus: &Corpus, t: &Tally) -> String {
    format!(
        "statement truth {} {} (server {}, name case {}): {} rows, agree {}, missed {}, over {} ({} of them never logged by the server), wrong {}",
        corpus.header.engine,
        corpus.header.server,
        corpus.header.version,
        if corpus.header.fold { "folded" } else { "kept" },
        t.total,
        t.agree,
        t.missed.len(),
        t.over.len(),
        t.over_unlogged,
        t.wrong.len()
    )
}

/// Fails unless every disagreement is in a ledger entry and every entry covers exactly its row count.
pub(crate) fn grade_ledger(corpus: &Corpus, t: &Tally, ledger: &[Known]) -> Result<(), String> {
    let server = corpus.header.server.as_str();
    let by_id: BTreeMap<&str, &Row> = corpus.rows.iter().map(|r| (r.id.as_str(), r)).collect();
    let mut used = vec![0usize; ledger.len()];
    let mut loose: BTreeMap<(Direction, &str), (usize, usize, String)> = BTreeMap::new();
    for (direction, ids) in [
        (Direction::Missed, &t.missed),
        (Direction::Over, &t.over),
        (Direction::Wrong, &t.wrong),
    ] {
        for id in ids {
            let row = by_id[id.as_str()];
            let case = row.case.as_str();
            match ledger
                .iter()
                .position(|k| k.covers(server, case, direction))
            {
                Some(i) => used[i] += 1,
                None => {
                    let e = loose
                        .entry((direction, case))
                        .or_insert((0, 0, row.describe()));
                    e.0 += 1;
                    e.1 += usize::from(row.src == "event");
                }
            }
        }
    }
    if !loose.is_empty() {
        return Err(format!(
            "{}: disagreements in no ledger entry (direction, case, rows, rows the server logged, first row): {:#?}",
            summary(corpus, t),
            loose
                .iter()
                .map(|((d, c), (n, e, id))| (d, c, n, e, id))
                .collect::<Vec<_>>()
        ));
    }
    for (k, n) in ledger.iter().zip(&used) {
        if k.server.is_none_or(|s| s == server) {
            if *n == 0 {
                return Err(format!(
                    "{server}: ledger `{}` covers no row any more; remove it: {}",
                    k.case, k.reason
                ));
            }
            if *n > k.rows {
                return Err(format!(
                    "{server}: ledger `{}` covers {n} rows, ceiling {}: {}",
                    k.case, k.rows, k.reason
                ));
            }
            if *n < k.rows {
                return Err(format!(
                    "{server}: ledger `{}` covers {n} rows, below its {}; lower the ceiling to {n}: {}",
                    k.case, k.rows, k.reason
                ));
            }
        }
    }
    Ok(())
}

/// Grades every corpus against the ledger and returns what each server that disagrees reports.
pub(crate) fn ledger_failures(
    corpora: &[Corpus],
    read: impl Fn(&Corpus, &Row) -> Class,
    ledger: &[Known],
) -> Vec<String> {
    let mut failures = Vec::new();
    for corpus in corpora {
        let t = replay(corpus, |row| read(corpus, row));
        println!("{}", summary(corpus, &t));
        if let Err(e) = grade_ledger(corpus, &t, ledger) {
            failures.push(e);
        }
    }
    failures
}
