//! Replays the MySQL statement-truth corpus through the QUERY-event readers of the binlog reader, with and without the grants policy.

use super::cdc::{QueryRefusal, query_refusal, statement_frame};
use super::statement::SqlMode;
use crate::source::statement_truth::{Class, Direction, Known, Row, corpora, ledger_failures};

/// The refusal the event loop gives one logged statement.
fn refusal(row: &Row, policy: bool, fold: bool, configured: &[String]) -> Option<QueryRefusal> {
    let raw = row.bytes();
    let sql = String::from_utf8_lossy(&raw).into_owned();
    let mode = SqlMode::from_bits(row.mode);
    query_refusal(policy, fold, &sql, &raw, &row.db, mode, configured)
}

/// The class of a refusal.
fn class_of(refusal: &QueryRefusal) -> Class {
    match refusal {
        QueryRefusal::Truncate(_) => Class::Truncate,
        QueryRefusal::ColumnRename(_) => Class::RenameColumn,
        QueryRefusal::TableRename(_) => Class::RenameTable,
        QueryRefusal::Drop(_) => Class::Drop,
        QueryRefusal::StatementWrite(_) | QueryRefusal::Mention(_) => Class::StatementWrite,
    }
}

/// Whether the reader ever sees the statement: the server logged it; one it acted on is always logged.
fn reaches_reader(row: &Row) -> bool {
    assert!(
        row.src == "event" || row.owed() == Class::None,
        "{}: the server acted on a statement it did not log",
        row.describe()
    );
    row.src == "event"
}

/// The class a run without `columns_from: grants` gives a statement; renames and privilege changes are not read without the policy.
fn read_without_policy(row: &Row, fold: bool, configured: &[String]) -> Class {
    if !reaches_reader(row) {
        return Class::None;
    }
    match refusal(row, false, fold, configured).as_ref().map(class_of) {
        Some(class) => class,
        None if matches!(
            row.owed(),
            Class::RenameTable | Class::RenameColumn | Class::Privilege
        ) =>
        {
            row.owed()
        }
        None => Class::None,
    }
}

/// Whether a run under `columns_from: grants` stops on the statement or marks a grants re-read for a privilege change.
fn caught_with_policy(row: &Row, fold: bool, configured: &[String]) -> bool {
    refusal(row, true, fold, configured).is_some()
        || (row.owed() == Class::Privilege && statement_frame(false, &row.bytes()).1)
}

/// The class a run under `columns_from: grants` gives a statement: the class owed when it is caught, a write when it is stopped without cause.
fn read_with_policy(row: &Row, fold: bool, configured: &[String]) -> Class {
    if !reaches_reader(row) {
        return Class::None;
    }
    match (caught_with_policy(row, fold, configured), row.owed()) {
        (false, _) => Class::None,
        (true, Class::None) => Class::StatementWrite,
        (true, owed) => owed,
    }
}

macro_rules! unreachable_name {
    () => {
        "2026-10-31: touches a non-ASCII table name; validate_table_ident (src/source/cdc/mod.rs) allows only [A-Za-z0-9_.] and refuses it when the stream opens, so rivet cannot capture it and the row is unreachable today"
    };
}

macro_rules! follow_up {
    () => {
        "no-policy TRUNCATE/DROP/STATEMENT-write fold on lctn!=0 (.claude/handoff/2026-10-30/issue-no-policy-truncate-fold.md)"
    };
}

macro_rules! benign {
    () => {
        "accepted: the policy stops on any statement that names a captured table, a benign one continues after .claude/handoff/2026-10-30/issue-grants-benign-ddl-continues.md"
    };
}

const fn missed(
    server: Option<&'static str>,
    case: &'static str,
    rows: usize,
    reason: &'static str,
) -> Known {
    Known {
        direction: Direction::Missed,
        server,
        case,
        rows,
        reason,
    }
}

const fn over(
    server: Option<&'static str>,
    case: &'static str,
    rows: usize,
    reason: &'static str,
) -> Known {
    Known {
        direction: Direction::Over,
        server,
        case,
        rows,
        reason,
    }
}

const LEDGER_WITHOUT_POLICY: &[Known] = &[
    missed(None, "coll.dml_native", 73, unreachable_name!()),
    missed(None, "coll.drop_native", 73, unreachable_name!()),
    missed(None, "coll.trunc_comment_native", 73, unreachable_name!()),
    missed(None, "coll.trunc_native_bare", 73, unreachable_name!()),
    missed(None, "coll.trunc_native", 73, unreachable_name!()),
    missed(
        None,
        "sweep.after_name",
        971,
        concat!(
            "2026-10-31: a NUL or a lone lead byte after the name breaks the lossy text read; open, follow-up ",
            follow_up!()
        ),
    ),
    missed(
        None,
        "sweep.between",
        13,
        concat!(
            "2026-10-31: 0xA0 or 0xFF between keywords is a space to the server in some charsets only; open, follow-up ",
            follow_up!()
        ),
    ),
    missed(
        Some("8.4-lctn0"),
        "pos.",
        12,
        concat!(
            "2026-10-31: the 6-digit version comment /*!080000 */ runs on 8.4 and reads as a plain comment; open, follow-up ",
            follow_up!()
        ),
    ),
    missed(
        Some("8.4-lctn1"),
        "pos.",
        12,
        concat!(
            "2026-10-31: the 6-digit version comment /*!080000 */ runs on 8.4 and reads as a plain comment; open, follow-up ",
            follow_up!()
        ),
    ),
    missed(
        Some("8.4-lctn1"),
        "spell.",
        24,
        concat!(
            "2026-10-31: names are compared unfolded under lower_case_table_names=1; open, follow-up ",
            follow_up!()
        ),
    ),
    missed(
        Some("8.4-lctn1"),
        "gadget.fold_",
        135,
        concat!(
            "2026-10-31: U+0130 and U+212A fold to i and k under lower_case_table_names=1 and a near-name resolves to the captured table unseen; open, follow-up ",
            follow_up!()
        ),
    ),
];

const LEDGER_WITH_POLICY: &[Known] = &[
    missed(None, "coll.alter_rename_native", 73, unreachable_name!()),
    missed(None, "coll.col_change_native", 73, unreachable_name!()),
    missed(None, "coll.col_native", 73, unreachable_name!()),
    missed(None, "coll.dml_native", 73, unreachable_name!()),
    missed(None, "coll.drop_native", 73, unreachable_name!()),
    missed(None, "coll.rename_native", 73, unreachable_name!()),
    missed(None, "coll.trunc_comment_native", 73, unreachable_name!()),
    missed(None, "coll.trunc_native_bare", 73, unreachable_name!()),
    missed(None, "coll.trunc_native", 73, unreachable_name!()),
    over(None, "coll.ddl_literal_native", 118, unreachable_name!()),
    over(None, "coll.dml_literal_native", 118, unreachable_name!()),
    over(
        None,
        "gadget.",
        40,
        concat!(
            "2026-10-31: a near-name or a no-op column swap whose bytes hold the captured name; ",
            benign!()
        ),
    ),
    over(
        None,
        "op.",
        49,
        concat!(
            "2026-10-31: a no-op change, a read or a literal that names a captured table; ",
            benign!()
        ),
    ),
    over(
        None,
        "otherdb.",
        49,
        concat!(
            "2026-10-31: a no-op change, a read or a literal that names a captured table; ",
            benign!()
        ),
    ),
    over(
        None,
        "otherdb_bare.",
        16,
        concat!(
            "2026-10-31: a no-op change or a drop of a missing table whose bare name is captured; ",
            benign!()
        ),
    ),
    over(
        Some("8.0-lctn0"),
        "pos.",
        180,
        concat!(
            "2026-10-31: a no-op change or a literal at 18 positions that names a captured table; ",
            benign!()
        ),
    ),
    over(
        Some("8.4-lctn0"),
        "pos.",
        192,
        concat!(
            "2026-10-31: a no-op change or a literal at 18 positions that names a captured table; ",
            benign!()
        ),
    ),
    over(
        Some("8.4-lctn1"),
        "pos.",
        192,
        concat!(
            "2026-10-31: a no-op change or a literal at 18 positions that names a captured table; ",
            benign!()
        ),
    ),
    over(
        Some("8.0-lctn0"),
        "spell.",
        84,
        concat!(
            "2026-10-31: a no-op change or a literal in a spelling that names a captured table; ",
            benign!()
        ),
    ),
    over(
        Some("8.4-lctn0"),
        "spell.",
        84,
        concat!(
            "2026-10-31: a no-op change or a literal in a spelling that names a captured table; ",
            benign!()
        ),
    ),
    over(
        Some("8.4-lctn1"),
        "spell.",
        108,
        concat!(
            "2026-10-31: a no-op change or a literal in a spelling that names a captured table; ",
            benign!()
        ),
    ),
    over(
        None,
        "uncaptured.",
        10,
        concat!(
            "2026-10-31: a literal that names a captured table; ",
            benign!()
        ),
    ),
];

#[test]
fn every_server_fixture_agrees_with_the_reader_without_the_policy() {
    let failures = ledger_failures(
        &corpora("mysql"),
        |c, row| read_without_policy(row, c.header.fold, &c.header.captured),
        LEDGER_WITHOUT_POLICY,
    );
    assert_eq!(failures, Vec::<String>::new(), "{}", failures.join("\n"));
}

#[test]
fn every_server_fixture_agrees_with_the_reader_under_the_policy() {
    let failures = ledger_failures(
        &corpora("mysql"),
        |c, row| read_with_policy(row, c.header.fold, &c.header.captured),
        LEDGER_WITH_POLICY,
    );
    assert_eq!(failures, Vec::<String>::new(), "{}", failures.join("\n"));
}

#[test]
fn every_server_fixture_has_no_ascii_statement_that_passes_under_the_policy() {
    for c in corpora("mysql") {
        let passed: Vec<String> = c
            .rows
            .iter()
            .filter(|r| r.owed() != Class::None && r.bytes().is_ascii())
            .filter(|r| read_with_policy(r, c.header.fold, &c.header.captured) == Class::None)
            .map(Row::describe)
            .collect();
        assert_eq!(
            passed,
            Vec::<String>::new(),
            "{}: ASCII statements owed a stop that pass under the policy",
            c.header.server
        );
    }
}
