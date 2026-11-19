//! `source.columns_from: grants` on Oracle: the columns an account may `SELECT`, asked of the server column by column.

use std::collections::HashMap;
use std::sync::Arc;

use super::{Ora as _, OracleSource};
use crate::config::{SourceType, TlsConfig};
use crate::error::Result;
use crate::source::Source;
use crate::source::cdc::ReadableColumns;
use crate::sql::quote_ident;

/// What `columns_from: grants` guarantees, said once to the operator at `rivet check` and at the start of a change-stream run.
pub(crate) const BOUNDARY: &str = "columns_from: grants - batch reads are enforced by the server (a column the \
account may not SELECT is never selected). The Oracle change stream asks LogMiner for the readable columns \
only, so a hidden column's value is never fetched by rivet's own query; that is data minimisation, not \
access control: the capture account's LogMiner views and the redo itself still carry every column, so \
restrict the capture account if the values must not be readable with another client. Oracle has no \
column-level SELECT privilege: a table is readable whole or not at all, so the readable set of a table you \
may SELECT is every column and a table you may not SELECT is refused.";

/// ORA-01031, ORA-00942, ORA-00904, ORA-41900: the server's "not yours to read".
const DENIED: [usize; 4] = [1031, 942, 904, 41900];

/// One table's columns in table order, as the catalog lists them for the account.
fn catalog_sql(configured: &str) -> String {
    let (owner, table) = crate::sql::oracle_catalog_preds(configured);
    let hidden = seeded!(
        "column-grants-invisible-columns-read",
        "",
        " AND hidden_column = 'NO'"
    );
    format!(
        "SELECT owner, table_name, column_name FROM all_tab_cols \
         WHERE owner = {owner} AND table_name = {table}{hidden} \
         ORDER BY column_id"
    )
}

/// `SELECT` of `columns` from `owner.table`, every name quoted.
pub(crate) fn projection_sql(owner: &str, table: &str, columns: &[String]) -> String {
    let quote = |name: &str| quote_ident(SourceType::Oracle, name);
    let cols: Vec<String> = columns.iter().map(|c| quote(c)).collect();
    format!(
        "SELECT {} FROM {}.{}",
        cols.join(", "),
        quote(owner),
        quote(table)
    )
}

/// Whether the server lets this account read `column` of `owner.table`; any other failure is an error.
fn may_select(src: &OracleSource, owner: &str, table: &str, column: &str) -> Result<bool> {
    let sql = format!(
        "{} WHERE 1 = 0",
        projection_sql(owner, table, &[column.to_string()])
    );
    match src.conn.query(&sql, &[]) {
        Ok(_) => Ok(true),
        Err(e)
            if matches!(
                e.kind(),
                oracledb::ErrorKind::DbError(db) if DENIED.contains(&db.code())
            ) =>
        {
            Ok(false)
        }
        Err(e) => Err(e).ora(),
    }
}

/// The relation a configured name means, and the columns this account may `SELECT` of it in table order.
pub(crate) fn readable_relation(
    src: &mut OracleSource,
    configured: &str,
) -> Result<(String, String, ReadableColumns)> {
    let catalog = src.query_rows(&catalog_sql(configured))?;
    let mut relation: Option<(String, String)> = None;
    let mut columns = Vec::new();
    for row in &catalog {
        let [Some(owner), Some(table), Some(name)] = row.as_slice() else {
            continue;
        };
        relation.get_or_insert_with(|| (owner.clone(), table.clone()));
        if seeded!(
            "column-grants-oracle-every-column-read-as-readable",
            true,
            may_select(src, owner, table, name)?
        ) {
            columns.push(name.clone());
        }
    }
    let Some((owner, table)) = relation.filter(|_| !columns.is_empty()) else {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_COLUMNS_UNREADABLE,
            "oracle: `{configured}`: `columns_from: grants` found no column this account \
             may SELECT (or no such table), so nothing of it could be delivered; nothing was \
             read or written. GRANT SELECT on the table to the account (Oracle grants SELECT \
             on a whole table, not on columns)"
        );
    };
    let select = projection_sql(&owner, &table, &columns);
    Ok((owner, table, ReadableColumns { columns, select }))
}

/// Refuse a key column the account may not read: without it a row cannot be told apart from another.
fn refuse_unreadable_key(
    owner: &str,
    table: &str,
    readable: &[String],
    key: &[String],
    from: &str,
) -> Result<()> {
    if seeded!("column-grants-unreadable-key-accepted", false, true)
        && let Some(k) = key.iter().find(|k| !readable.contains(k))
    {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_COLUMNS_UNREADABLE,
            "oracle: `{owner}.{table}`: key column `{k}` ({from}) is not readable under \
             `columns_from: grants`, so changes to a row could not be told apart; nothing was \
             read or written. GRANT SELECT on the table, or name a readable unique key in `load.pk`"
        );
    }
    Ok(())
}

/// The readable columns of every configured table, keyed by its configured name; each table's key must be readable.
pub(crate) fn resolve_grants(
    url: &str,
    tls: Option<&TlsConfig>,
    configured: &[String],
    declared: &HashMap<String, Vec<String>>,
) -> Result<Arc<HashMap<String, ReadableColumns>>> {
    let mut src = OracleSource::connect_with_tls(url, tls)?;
    let mut out = HashMap::with_capacity(configured.len());
    for cfg in configured {
        let (owner, table, readable) = readable_relation(&mut src, cfg)?;
        let (key, from) = match declared.get(cfg) {
            Some(k) => (k.clone(), "the `load.pk` of this export"),
            None => (src.primary_key(cfg)?.unwrap_or_default(), "the primary key"),
        };
        refuse_unreadable_key(&owner, &table, &readable.columns, &key, from)?;
        out.insert(cfg.clone(), readable);
    }
    Ok(Arc::new(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn an_identifier_is_quoted_for_any_text() {
        assert_eq!(
            projection_sql("O\"W", "t x", &s(&["ID", "a\"b", "ім'я"])), // allow-cyrillic
            "SELECT \"ID\", \"a\"\"b\", \"ім'я\" FROM \"O\"\"W\".\"t x\""  // allow-cyrillic
        );
    }

    #[test]
    fn the_catalog_read_names_the_table_by_its_catalog_spelling_and_skips_hidden_columns() {
        let sql = catalog_sql("rivet.orders");
        assert!(
            sql.contains("owner = 'RIVET' AND table_name = 'ORDERS' AND hidden_column = 'NO'")
                && sql.ends_with("ORDER BY column_id"),
            "{sql}"
        );
        assert!(catalog_sql("o'x").contains("table_name = 'O''X'"));
    }

    #[test]
    fn a_key_the_account_cannot_read_is_refused_naming_the_column() {
        let err = refuse_unreadable_key("O", "T", &s(&["NAME"]), &s(&["ID"]), "the primary key")
            .expect_err("refused");
        assert_eq!(
            crate::error::code_of(&err),
            Some(crate::error::codes::SOURCE_COLUMNS_UNREADABLE)
        );
        assert!(format!("{err:#}").contains("key column `ID`"), "{err:#}");
        assert!(
            refuse_unreadable_key(
                "O",
                "T",
                &s(&["ID", "NAME"]),
                &s(&["ID"]),
                "the primary key"
            )
            .is_ok()
        );
        assert!(refuse_unreadable_key("O", "T", &s(&["NAME"]), &[], "the primary key").is_ok());
    }

    #[test]
    fn the_boundary_says_what_oracle_does_and_does_not_enforce() {
        for said in [
            "never fetched by rivet's own query",
            "not access control",
            "no column-level SELECT privilege",
        ] {
            assert!(BOUNDARY.contains(said), "{said}");
        }
    }
}
