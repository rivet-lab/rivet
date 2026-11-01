//! `source.columns_from: grants`: the columns a MySQL account may `SELECT`, asked of the server.

use std::collections::HashMap;

use mysql::prelude::Queryable as _;

use crate::config::SourceType;
use crate::error::Result;
use crate::source::cdc::ReadableColumns;
use crate::sql::quote_ident;

/// What `columns_from: grants` guarantees, said once to the operator at `rivet check` and at the start of a change-stream run.
pub(crate) const BOUNDARY: &str = "columns_from: grants - batch reads are enforced by the server (a column the \
account may not SELECT is never selected). The MySQL change stream is a client-side projection: the binlog \
carries every column of a changed row to this process, which drops the unreadable ones before anything is \
written. That is data minimisation, not access control: restrict the replication account itself if the values \
must not reach this host.";

/// ER_TABLEACCESS_DENIED_ERROR, ER_COLUMNACCESS_DENIED_ERROR: the server's "not yours to read".
const DENIED: [u16; 2] = [1142, 1143];

/// The server's text when `err` is its typed "not yours to read" verdict (1142, 1143).
pub(crate) fn denied_by_server(err: &anyhow::Error) -> Option<String> {
    err.chain()
        .find_map(|e| e.downcast_ref::<mysql::Error>())
        .and_then(|e| match e {
            mysql::Error::MySqlError(me) if DENIED.contains(&me.code) => Some(me.message.clone()),
            _ => None,
        })
}

/// `SELECT` of `columns` from `db.table`, every name quoted.
pub(crate) fn projection_sql(db: Option<&str>, table: &str, columns: &[String]) -> String {
    let quote = |name: &str| quote_ident(SourceType::Mysql, name);
    let cols: Vec<String> = columns.iter().map(|c| quote(c)).collect();
    let rel = match db {
        Some(db) => format!("{}.{}", quote(db), quote(table)),
        None => quote(table),
    };
    format!("SELECT {} FROM {rel}", cols.join(", "))
}

/// The columns of `db.table` the account may `SELECT` in table order, each judged by the server.
pub(crate) fn readable_columns(
    conn: &mut mysql::Conn,
    db: &str,
    table: &str,
) -> Result<Vec<String>> {
    let names: Vec<String> = conn.exec(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND EXTRA NOT LIKE '%INVISIBLE%' \
         ORDER BY ORDINAL_POSITION",
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
            crate::error::codes::SOURCE_COLUMNS_UNREADABLE,
            "mysql: `{db}.{table}`: `columns_from: grants` found no column this account \
             may SELECT (or no such table), so nothing of it could be delivered; nothing was \
             read or written"
        );
    }
    Ok(readable)
}

/// The database and table a configured name means: a bare name is read in `bare_db`'s database for it, else in `own_db`.
fn relation_of(
    cfg: &str,
    own_db: &str,
    bare_db: &HashMap<String, String>,
) -> Result<(String, String)> {
    Ok(match cfg.split_once('.') {
        Some((db, t)) => (db.to_string(), t.to_string()),
        None => match bare_db.get(cfg).map(String::as_str).unwrap_or(own_db) {
            "" => crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
                "mysql: `{cfg}` is a bare name and the source URL names no database, so \
                 `columns_from: grants` cannot tell whose grants to read: write `db.{cfg}`"
            ),
            db => (db.to_string(), cfg.to_string()),
        },
    })
}

/// The readable columns of every configured table, keyed by its configured name.
pub(crate) fn readable_columns_of(
    conn: &mut mysql::Conn,
    configured: &[String],
    own_db: &str,
    bare_db: &HashMap<String, String>,
) -> Result<HashMap<String, ReadableColumns>> {
    let mut out = HashMap::with_capacity(configured.len());
    for cfg in configured {
        let (db, table) = relation_of(cfg, own_db, bare_db)?;
        let columns = readable_columns(conn, &db, &table)?;
        let select = projection_sql(Some(&db), &table, &columns);
        out.insert(cfg.clone(), ReadableColumns { columns, select });
    }
    Ok(out)
}

/// The primary key of `db.table`, in key order; empty for a table without one.
fn primary_key_of(conn: &mut mysql::Conn, db: &str, table: &str) -> Result<Vec<String>> {
    Ok(conn.exec(
        "SELECT COLUMN_NAME FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX",
        (db, table),
    )?)
}

/// The readable columns of every configured table, and the key each table is told apart by must be readable.
pub(crate) fn resolve_grants(
    conn: &mut mysql::Conn,
    configured: &[String],
    own_db: &str,
    bare_db: &HashMap<String, String>,
    declared: &HashMap<String, Vec<String>>,
) -> Result<HashMap<String, ReadableColumns>> {
    let readable = readable_columns_of(conn, configured, own_db, bare_db)?;
    for cfg in configured {
        let (db, table) = relation_of(cfg, own_db, bare_db)?;
        let (key, from) = match declared.get(cfg) {
            Some(k) => (k.clone(), "the `load.pk` of this export"),
            None => (primary_key_of(conn, &db, &table)?, "the primary key"),
        };
        let shown = &readable[cfg].columns;
        for k in key
            .iter()
            .filter(|k| !shown.iter().any(|c| c.eq_ignore_ascii_case(k)))
        {
            let sql = projection_sql(Some(&db), &table, std::slice::from_ref(k));
            match conn.prep(&sql) {
                Ok(stmt) => conn.close(stmt)?,
                Err(mysql::Error::MySqlError(e)) if DENIED.contains(&e.code) => {
                    crate::rivet_bail!(
                        crate::error::codes::SOURCE_COLUMNS_UNREADABLE,
                        "mysql: `{db}.{table}`: key column `{k}` ({from}) is not readable under \
                         `columns_from: grants`, so changes to a row could not be told apart; \
                         nothing was read or written. GRANT SELECT on it, or name a readable \
                         unique key in `load.pk`"
                    );
                }
                Err(mysql::Error::MySqlError(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(readable)
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

/// A present image that does not carry one value per column of the table map.
pub(crate) fn is_partial<T>(full: usize, row: Option<&(Vec<T>, Vec<usize>)>) -> bool {
    row.is_some_and(|(vals, _)| vals.len() != full)
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
        assert_eq!(quote_ident(SourceType::Mysql, "a`b"), "`a``b`");
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

    #[test]
    fn an_image_is_partial_when_it_holds_fewer_values_than_the_table_has_columns() {
        let full = (vec![1, 2, 3], vec![]);
        let minimal = (vec![1], vec![]);
        assert!(!is_partial(3, Some(&full)));
        assert!(is_partial(3, Some(&minimal)));
        assert!(
            !is_partial::<i32>(3, None),
            "an absent image is not a partial one"
        );
        assert!(
            is_partial(3, Some(&(vec![1, 2, 3, 4], vec![]))),
            "more values than names is not whole either"
        );
    }
    fn server_said(code: u16) -> anyhow::Error {
        mysql::Error::MySqlError(mysql::MySqlError {
            state: "42000".into(),
            message: "SELECT command denied to user 'u'@'h' for column 'pass' in table 't'".into(),
            code,
        })
        .into()
    }

    #[test]
    fn only_the_servers_typed_denial_is_a_denial() {
        for code in [1142, 1143] {
            let said = denied_by_server(&server_said(code)).expect("a denial");
            assert!(said.contains("denied"), "{said}");
        }
        assert!(
            denied_by_server(&server_said(1054)).is_none(),
            "an unknown column is not a denial"
        );
        let wrapped = server_said(1143).context("while reading the page");
        assert!(denied_by_server(&wrapped).is_some(), "found down the chain");
        assert!(denied_by_server(&anyhow::anyhow!("ERROR 1143 in the text")).is_none());
    }

    #[test]
    fn a_denied_read_of_a_snapshot_leg_is_a_grant_change_with_the_rerun_remedy() {
        let code = crate::error::codes::SOURCE_CDC_GRANTS_CHANGED;
        let coded = crate::source::code_a_revoked_column(server_said(1143), "orders", true, true);
        assert_eq!(crate::error::code_of(&coded), Some(code));
        let text = format!("{coded:#}");
        assert!(
            text.contains("orders") && text.contains("Re-run") && !text.contains("rivet plan"),
            "{text}"
        );
        let other = crate::source::code_a_revoked_column(server_said(1054), "orders", true, true);
        assert!(crate::error::code_of(&other).is_none());
        let off = crate::source::code_a_revoked_column(server_said(1143), "orders", false, true);
        assert!(crate::error::code_of(&off).is_none());
    }

    #[test]
    fn a_denied_read_under_the_policy_is_coded_with_the_replan_remedy_and_nothing_else_is() {
        let code = crate::error::codes::SOURCE_COLUMNS_UNREADABLE;
        let coded = crate::source::code_a_revoked_column(server_said(1143), "orders", true, false);
        assert_eq!(crate::error::code_of(&coded), Some(code));
        let text = format!("{coded:#}");
        assert!(
            text.contains("orders") && text.contains("rivet plan") && text.contains("GRANT SELECT"),
            "{text}"
        );
        assert!(
            !text.contains("1143"),
            "the raw driver error does not stay in the chain: {text}"
        );
        let off = crate::source::code_a_revoked_column(server_said(1143), "orders", false, false);
        assert!(
            crate::error::code_of(&off).is_none(),
            "no policy, no recoding"
        );
        let other = crate::source::code_a_revoked_column(server_said(1054), "orders", true, false);
        assert!(crate::error::code_of(&other).is_none());
        let already = crate::error::coded(crate::error::codes::SOURCE_CDC_GRANTS_CHANGED, "moved");
        let kept = crate::source::code_a_revoked_column(already, "orders", true, false);
        assert_eq!(
            crate::error::code_of(&kept),
            Some(crate::error::codes::SOURCE_CDC_GRANTS_CHANGED),
            "a coded error keeps its code"
        );
    }
}
