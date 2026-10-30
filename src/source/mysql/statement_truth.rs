//! Replays the MySQL statement-truth corpus through the QUERY-event reader of the binlog reader.

use super::cdc::{QueryScope, QueryVerdict, query_verdict};
use super::statement::{SqlMode, client_text};
use crate::source::statement_truth::{
    Class, Direction, Header, Known, Row, assert_ledgers, corpora,
};

/// The class of refusal the reader gives one logged statement, as its event loop reaches it.
fn read(row: &Row, header: &Header) -> Class {
    let bytes = row.bytes();
    let sql = client_text(&bytes, Some(row.cs));
    let scope = QueryScope {
        projecting: true,
        fold: header.fold,
        configured: &header.captured,
    };
    class_of(&query_verdict(
        &sql,
        &row.db,
        SqlMode::from_bits(row.mode),
        &scope,
    ))
}

/// The class a verdict stands for; an unreadable statement counts as the refusal it hides.
fn class_of(verdict: &QueryVerdict) -> Class {
    match verdict {
        QueryVerdict::Truncated(..) => Class::Truncate,
        QueryVerdict::ColumnRenamed(..) => Class::RenameColumn,
        QueryVerdict::TableRenamed(..) => Class::RenameTable,
        QueryVerdict::Dropped(..) => Class::Drop,
        QueryVerdict::StatementWrite(..) => Class::StatementWrite,
        QueryVerdict::Unreadable(inner) => class_of(inner),
        QueryVerdict::Privilege => Class::Privilege,
        QueryVerdict::Commit | QueryVerdict::Pass => Class::None,
    }
}

/// The disagreements with the server accepted on 2026-10-30, each with a ceiling and what closes it.
const LEDGER: &[Known] = &[
    Known {
        direction: Direction::Missed,
        server: None,
        case: "coll.",
        rows: 9,
        reason: "2026-10-30: collation 84 (big5_bin) decodes 0xC7xx as kana in encoding_rs and as Cyrillic in MySQL, the captured name is not found; open",
    },
    Known {
        direction: Direction::Missed,
        server: None,
        case: "op.col_change_case",
        rows: 4,
        reason: "2026-10-30: CHANGE amount AMOUNT reads as the same column; open",
    },
    Known {
        direction: Direction::Missed,
        server: None,
        case: "otherdb.col_change_case",
        rows: 4,
        reason: "2026-10-30: CHANGE amount AMOUNT reads as the same column; open",
    },
    Known {
        direction: Direction::Over,
        server: None,
        case: "coll.",
        rows: 44,
        reason: "2026-10-30: an uncaptured ujis name that cannot be placed has the shape of a captured one and is refused; accepted, refusing is the safe side",
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
        rows: 164,
        reason: "2026-10-30: 8.0 rejects the 6-digit version comment (1064) and a zero-row UPDATE is never logged; the reader runs both; accepted, no event exists",
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
    Known {
        direction: Direction::Over,
        server: None,
        case: "sweep.after_name",
        rows: 4751,
        reason: "2026-10-30: a statement with an unreadable byte after the name is rejected by the server (1064) and never logged; refused as unreadable; accepted",
    },
    Known {
        direction: Direction::Over,
        server: None,
        case: "sweep.between",
        rows: 5261,
        reason: "2026-10-30: a statement with an unreadable byte between keywords is rejected by the server (1064) and never logged; refused as unreadable; accepted",
    },
];

#[test]
fn every_server_fixture_agrees_with_the_binlog_reader() {
    assert_ledgers(&corpora("mysql"), |c, row| read(row, &c.header), LEDGER);
}
