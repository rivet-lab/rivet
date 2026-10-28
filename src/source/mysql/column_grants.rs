//! `cdc.columns_from: grants`: the columns a MySQL account may `SELECT`, asked of the server.

use std::collections::HashMap;

use mysql::prelude::Queryable as _;

use crate::error::Result;
use crate::source::cdc::ReadableColumns;

/// ER_TABLEACCESS_DENIED_ERROR, ER_COLUMNACCESS_DENIED_ERROR: the server's "not yours to read".
const DENIED: [u16; 2] = [1142, 1143];

/// `name` as a MySQL identifier, safe for any text.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `SELECT` of `columns` from `db.table`, every name quoted.
pub(crate) fn projection_sql(db: Option<&str>, table: &str, columns: &[String]) -> String {
    let cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
    let rel = match db {
        Some(db) => format!("{}.{}", quote_ident(db), quote_ident(table)),
        None => quote_ident(table),
    };
    format!("SELECT {} FROM {rel}", cols.join(", "))
}

/// The columns of `db.table` the connected account may `SELECT`, in table order.
///
/// Each column is judged by preparing a `SELECT` of it alone, so roles, column grants
/// and partial revokes are decided by the server, never re-derived from grant text.
pub(crate) fn readable_columns(
    conn: &mut mysql::Conn,
    db: &str,
    table: &str,
) -> Result<Vec<String>> {
    let names: Vec<String> = conn.exec(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        (db, table),
    )?;
    let mut readable = Vec::with_capacity(names.len());
    for name in names {
        let sql = projection_sql(Some(db), table, std::slice::from_ref(&name));
        match conn.prep(&sql) {
            Ok(stmt) => {
                conn.close(stmt)?;
                readable.push(name);
            }
            Err(mysql::Error::MySqlError(e)) if DENIED.contains(&e.code) => {}
            Err(e) => return Err(e.into()),
        }
    }
    if readable.is_empty() {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_COLUMNS_UNREADABLE,
            "mysql cdc: `{db}.{table}`: `columns_from: grants` found no column this account \
             may SELECT (or no such table), so nothing of it could be delivered; nothing was \
             read or written"
        );
    }
    let key: Vec<String> = conn.exec(
        "SELECT COLUMN_NAME FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX",
        (db, table),
    )?;
    if let Some(missing) = crate::source::cdc::unreadable(&key, &readable) {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_COLUMNS_UNREADABLE,
            "mysql cdc: `{db}.{table}`: key column `{missing}` is not readable under \
             `columns_from: grants`, so changes to a row could not be told apart; nothing was \
             read or written"
        );
    }
    Ok(readable)
}

/// The readable columns of every configured table, keyed by its configured name.
///
/// A bare name is read in `bare_db`'s database for it, else in `own_db`.
pub(crate) fn readable_columns_of(
    conn: &mut mysql::Conn,
    configured: &[String],
    own_db: &str,
    bare_db: &HashMap<String, String>,
) -> Result<HashMap<String, ReadableColumns>> {
    let mut out = HashMap::with_capacity(configured.len());
    for cfg in configured {
        let (db, table) = match cfg.split_once('.') {
            Some((db, t)) => (db.to_string(), t.to_string()),
            None => match bare_db.get(cfg).map(String::as_str).unwrap_or(own_db) {
                "" => crate::rivet_bail!(
                    crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
                    "mysql cdc: `{cfg}` is a bare name and the source URL names no database, so \
                     `columns_from: grants` cannot tell whose grants to read: write `db.{cfg}`"
                ),
                db => (db.to_string(), cfg.clone()),
            },
        };
        let columns = readable_columns(conn, &db, &table)?;
        let select = projection_sql(Some(&db), &table, &columns);
        out.insert(cfg.clone(), ReadableColumns { columns, select });
    }
    Ok(out)
}

/// The positions in `names` of the columns in `readable` (MySQL column names fold case).
pub(crate) fn kept_positions(names: &[String], readable: &[String]) -> Vec<usize> {
    names
        .iter()
        .enumerate()
        .filter(|(_, n)| readable.iter().any(|r| r.eq_ignore_ascii_case(n)))
        .map(|(i, _)| i)
        .collect()
}

/// A projected update whose images are equal and fully decoded: only a hidden column changed.
pub(crate) fn changes_only_hidden<T: PartialEq>(
    projected: bool,
    before: Option<&(Vec<T>, Vec<usize>)>,
    after: Option<&(Vec<T>, Vec<usize>)>,
) -> bool {
    projected
        && matches!((before, after), (Some((b, bb)), Some((a, ab))) if bb.is_empty() && ab.is_empty() && b == a)
}

/// `row` cut to `keep`, its undecodable positions renumbered into the cut row.
pub(crate) fn project_row<T>(
    (vals, bad): (Vec<T>, Vec<usize>),
    keep: &[usize],
) -> (Vec<T>, Vec<usize>) {
    let bad = keep
        .iter()
        .enumerate()
        .filter(|(_, k)| bad.contains(k))
        .map(|(i, _)| i)
        .collect();
    let mut slots: Vec<Option<T>> = vals.into_iter().map(Some).collect();
    let vals = keep
        .iter()
        .filter_map(|&k| slots.get_mut(k).and_then(Option::take))
        .collect();
    (vals, bad)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn an_identifier_is_quoted_for_any_text() {
        assert_eq!(quote_ident("a`b"), "`a``b`");
        assert_eq!(
            projection_sql(Some("d b"), "t", &s(&["id", "x`y", "ім'я"])), // allow-cyrillic
            "SELECT `id`, `x``y`, `ім'я` FROM `d b`.`t`"                  // allow-cyrillic
        );
        assert_eq!(
            projection_sql(None, "t", &s(&["id"])),
            "SELECT `id` FROM `t`"
        );
    }

    #[test]
    fn a_row_keeps_only_readable_columns_and_their_own_poison() {
        let names = s(&["id", "secret", "Name", "pass"]);
        let keep = kept_positions(&names, &s(&["ID", "name"]));
        assert_eq!(keep, vec![0, 2]);
        let row = (vec!["1", "tok", "ann", "pw"], vec![1, 2]);
        assert_eq!(project_row(row, &keep), (vec!["1", "ann"], vec![1]));
        assert_eq!(
            project_row((vec!["1"], vec![]), &[]),
            (Vec::<&str>::new(), vec![])
        );
    }

    #[test]
    fn only_an_equal_fully_decoded_update_is_hidden_only() {
        let row = |v: &[&'static str], bad: &[usize]| (v.to_vec(), bad.to_vec());
        let same = row(&["1", "ann"], &[]);
        assert!(changes_only_hidden(true, Some(&same), Some(&same)));
        assert!(
            !changes_only_hidden(false, Some(&same), Some(&same)),
            "no projection, no hiding"
        );
        assert!(!changes_only_hidden(
            true,
            Some(&same),
            Some(&row(&["1", "bob"], &[]))
        ));
        assert!(
            !changes_only_hidden(true, None, Some(&same)),
            "an insert is delivered"
        );
        assert!(
            !changes_only_hidden(true, Some(&same), None),
            "a delete is delivered"
        );
        let poisoned = row(&["1", "ann"], &[1]);
        assert!(!changes_only_hidden(true, Some(&poisoned), Some(&poisoned)));
        assert!(!changes_only_hidden(true, Some(&same), Some(&poisoned)));
        assert!(
            !changes_only_hidden(true, Some(&row(&["1"], &[])), Some(&same)),
            "a thin before image proves nothing"
        );
    }
}
