//! Replays the MySQL statement-truth corpus through the QUERY-event readers of the binlog reader.

use super::cdc::{
    drop_table_targets, is_privilege_statement, refused_column_rename, refused_table_rename,
    statement_dml_target, truncate_target, undecodable_event_is_ours,
};
use super::statement::SqlMode;
use crate::source::statement_truth::{Class, Direction, Known, Row, assert_ledgers, corpora};

/// The class of refusal the reader gives one logged statement, in the order its event loop tries them.
fn read(row: &Row, configured: &[String]) -> Class {
    let sql = String::from_utf8_lossy(&row.bytes()).into_owned();
    let mode = SqlMode::from_bits(row.mode);
    let db = row.db.as_str();
    let ours = |t: &Option<(String, String)>| {
        undecodable_event_is_ours(
            t.as_ref().map(|(s, t)| (s.as_str(), t.as_str())),
            configured,
        )
    };
    if truncate_target(&sql, db, mode).is_some_and(|(s, t)| ours(&Some((s, t)))) {
        Class::Truncate
    } else if refused_column_rename(true, &sql, db, mode, configured).is_some() {
        Class::RenameColumn
    } else if refused_table_rename(true, &sql, db, mode, configured).is_some() {
        Class::RenameTable
    } else if drop_table_targets(&sql, db, mode).is_some_and(|ts| ts.iter().any(ours)) {
        Class::Drop
    } else if statement_dml_target(&sql, db, mode).is_some_and(|t| ours(&t)) {
        Class::StatementWrite
    } else if is_privilege_statement(&sql, mode) {
        Class::Privilege
    } else {
        Class::None
    }
}

/// The disagreements with the server accepted on 2026-10-30, each with a ceiling and what closes it.
const LEDGER: &[Known] = &[
    Known {
        direction: Direction::Missed,
        server: None,
        case: "coll.",
        rows: 657,
        reason: "2026-10-30: lossy UTF-8 read; a name sent in a non-UTF-8 client charset is not found; closed by PR #728 (client_text by charset id)",
    },
    Known {
        direction: Direction::Missed,
        server: None,
        case: "op.col_change_case",
        rows: 4,
        reason: "2026-10-30: CHANGE amount AMOUNT reads as the same column; open, PR #728 keeps it",
    },
    Known {
        direction: Direction::Missed,
        server: None,
        case: "otherdb.col_change_case",
        rows: 4,
        reason: "2026-10-30: CHANGE amount AMOUNT reads as the same column; open, PR #728 keeps it",
    },
    Known {
        direction: Direction::Missed,
        server: Some("8.4-lctn0"),
        case: "pos.",
        rows: 36,
        reason: "2026-10-30: the 6-digit version comment /*!080000 */ runs on 8.4 and reads as a plain comment; closed by PR #728",
    },
    Known {
        direction: Direction::Missed,
        server: Some("8.4-lctn1"),
        case: "pos.",
        rows: 36,
        reason: "2026-10-30: the 6-digit version comment /*!080000 */ runs on 8.4 and reads as a plain comment; closed by PR #728",
    },
    Known {
        direction: Direction::Missed,
        server: Some("8.4-lctn1"),
        case: "spell.",
        rows: 72,
        reason: "2026-10-30: names are compared unfolded under lower_case_table_names=1; closed by PR #728 (fold)",
    },
    Known {
        direction: Direction::Missed,
        server: None,
        case: "sweep.after_name",
        rows: 971,
        reason: "2026-10-30: a NUL or a lone lead byte after the name breaks the lossy text read; closed by PR #728",
    },
    Known {
        direction: Direction::Missed,
        server: None,
        case: "sweep.between",
        rows: 13,
        reason: "2026-10-30: 0xA0 or 0xFF between keywords is a space to the server in some charsets only; closed by PR #728",
    },
    Known {
        direction: Direction::Missed,
        server: None,
        case: "sweep.pair_backslash",
        rows: 461,
        reason: "2026-10-30: a lead byte pairs with the backslash in sjis, big5 and gbk, the reader sees an escaped quote; closed by PR #728",
    },
    Known {
        direction: Direction::Over,
        server: None,
        case: "op.",
        rows: 12,
        reason: "2026-10-30: a zero-row UPDATE and a rejected statement are never logged; the reader refuses by text; accepted, no event exists",
    },
    Known {
        direction: Direction::Over,
        server: None,
        case: "otherdb.",
        rows: 12,
        reason: "2026-10-30: a zero-row UPDATE and a rejected statement are never logged; the reader refuses by text; accepted, no event exists",
    },
    Known {
        direction: Direction::Over,
        server: None,
        case: "otherdb_bare.",
        rows: 12,
        reason: "2026-10-30: a GRANT or REVOKE the server rejects (1146, 1147) is never logged; refused by kind; accepted, no event exists",
    },
    Known {
        direction: Direction::Over,
        server: None,
        case: "pos.",
        rows: 60,
        reason: "2026-10-30: a zero-row UPDATE is never logged; the reader refuses by text; accepted, no event exists",
    },
    Known {
        direction: Direction::Over,
        server: None,
        case: "spell.",
        rows: 88,
        reason: "2026-10-30: a statement rejected under the session sql_mode, or run on no row, is never logged; accepted, no event exists",
    },
    Known {
        direction: Direction::Over,
        server: None,
        case: "uncaptured.",
        rows: 4,
        reason: "2026-10-30: a REVOKE the server rejects (1147) is never logged; refused by kind; accepted, no event exists",
    },
];

#[test]
fn every_server_fixture_agrees_with_the_binlog_reader() {
    assert_ledgers(
        &corpora("mysql"),
        |c, row| read(row, &c.header.captured),
        LEDGER,
    );
}
