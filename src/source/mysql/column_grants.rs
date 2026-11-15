//! `source.columns_from: grants`: the columns a MySQL account may `SELECT`, asked of the server.

use std::collections::HashMap;

use mysql::prelude::Queryable as _;

use crate::config::SourceType;
use crate::error::Result;
use crate::source::cdc::ReadableColumns;
use crate::source::column_grants::{KeyCatalog, NamesMatch, projection_sql, require_readable_keys};

/// What `columns_from: grants` guarantees, said once to the operator at `rivet check` and at the start of a change-stream run.
pub(crate) const BOUNDARY: &str = "columns_from: grants - batch reads are enforced by the server (a column the \
account may not SELECT is never selected). The MySQL change stream is a client-side projection: the binlog \
carries every column of a changed row to this process, which drops the unreadable ones before anything is \
written. That is data minimisation, not access control: restrict the replication account itself if the values \
must not reach this host.";

/// MySQL column names fold ASCII case.
pub(crate) const NAMES_MATCH: NamesMatch = str::eq_ignore_ascii_case;

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

/// The columns of `db.table` the account may `SELECT` in table order, each judged by the server.
pub(crate) fn readable_columns(
    conn: &mut mysql::Conn,
    db: &str,
    table: &str,
) -> Result<Vec<String>> {
    let names: Vec<String> = conn.exec(
        seeded!(
            "column-grants-invisible-columns-read",
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
         ORDER BY ORDINAL_POSITION",
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND EXTRA NOT LIKE '%INVISIBLE%' \
         ORDER BY ORDINAL_POSITION"
        ),
        (db, table),
    )?;
    let mut readable = Vec::with_capacity(names.len());
    for name in names {
        let sql = projection_sql(
            SourceType::Mysql,
            Some(db),
            table,
            std::slice::from_ref(&name),
        );
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

/// The database and table a configured name means.
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
        let select = projection_sql(SourceType::Mysql, Some(&db), &table, &columns);
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

/// The MySQL catalog reads behind the key-readability check.
struct MysqlKeys<'a> {
    conn: &'a mut mysql::Conn,
    own_db: &'a str,
    bare_db: &'a HashMap<String, String>,
}

impl KeyCatalog for MysqlKeys<'_> {
    fn relation(&self, configured: &str) -> Result<String> {
        let (db, table) = relation_of(configured, self.own_db, self.bare_db)?;
        Ok(format!("{db}.{table}"))
    }

    fn primary_key(&mut self, configured: &str) -> Result<Vec<String>> {
        let (db, table) = relation_of(configured, self.own_db, self.bare_db)?;
        primary_key_of(self.conn, &db, &table)
    }

    fn denies(&mut self, configured: &str, column: &str) -> Result<bool> {
        let (db, table) = relation_of(configured, self.own_db, self.bare_db)?;
        let sql = projection_sql(
            SourceType::Mysql,
            Some(&db),
            &table,
            std::slice::from_ref(&column.to_string()),
        );
        match seeded!(
            "column-grants-invisible-key-judged-by-name",
            Err::<mysql::Statement, mysql::Error>(mysql::Error::MySqlError(mysql::MySqlError {
                state: "42000".into(),
                message: "x".into(),
                code: 1143
            })),
            self.conn.prep(&sql)
        ) {
            Ok(stmt) => {
                self.conn.close(stmt)?;
                Ok(false)
            }
            Err(mysql::Error::MySqlError(e)) => Ok(DENIED.contains(&e.code)),
            Err(e) => Err(e.into()),
        }
    }
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
    let mut catalog = MysqlKeys {
        conn,
        own_db,
        bare_db,
    };
    require_readable_keys(
        "mysql",
        &mut catalog,
        configured,
        &readable,
        declared,
        NAMES_MATCH,
    )?;
    Ok(readable)
}

#[cfg(test)]
pub(crate) fn server_said(code: u16) -> anyhow::Error {
    mysql::Error::MySqlError(mysql::MySqlError {
        state: "42000".into(),
        message: "SELECT command denied to user 'u'@'h' for column 'pass' in table 't'".into(),
        code,
    })
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
