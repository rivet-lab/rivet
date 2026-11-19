//! `source.columns_from: grants` on PostgreSQL: the columns an account may `SELECT`, asked of the server.

use std::collections::HashMap;
use std::sync::Arc;

use postgres::Client;
use postgres::error::SqlState;

use crate::config::{SourceType, TlsConfig};
use crate::error::Result;
use crate::source::cdc::ReadableColumns;
use crate::source::cdc::value::RivetValue;
use crate::source::column_grants::{KeyCatalog, NamesMatch, projection_sql, require_readable_keys};

/// What `columns_from: grants` guarantees, said once to the operator at `rivet check` and at the start of a change-stream run.
pub(crate) const BOUNDARY: &str = "columns_from: grants - batch reads are enforced by the server (a column the \
account may not SELECT is never selected). The PostgreSQL change stream is a client-side projection: logical \
decoding does no column privilege check, so every column of a changed row of a captured table reaches this process, which drops \
the unreadable ones before anything is buffered, spilled or written. That is data minimisation, not access \
control: restrict the replication account itself if the values must not reach this host. The readable \
columns are the server's own answer (has_column_privilege: column-level and table-level grants). A grant \
change is seen when rivet reads each batch of the slot, not when it happens: the stream is then refused and \
nothing of that batch is delivered, but a REVOKE undone before the next read is not seen.";

/// PostgreSQL column names are case-sensitive.
pub(crate) const NAMES_MATCH: NamesMatch = same_name;

/// Whether two PostgreSQL column names are the same column.
fn same_name(a: &str, b: &str) -> bool {
    a == b
}

/// The server's text when `err` is its typed "not yours to read" verdict (insufficient_privilege, 42501).
pub(crate) fn denied_by_server(err: &anyhow::Error) -> Option<String> {
    err.chain()
        .find_map(|e| e.downcast_ref::<postgres::Error>())
        .and_then(|e| {
            let db = e.as_db_error()?;
            (db.code() == &SqlState::INSUFFICIENT_PRIVILEGE).then(|| db.message().to_string())
        })
}

/// The relation a configured name means and the columns of it the account may `SELECT`, in table order, each judged by the server; `None` when it may read none or there is no such table.
fn read_columns(
    client: &mut Client,
    configured: &str,
) -> Result<Option<(String, String, ReadableColumns)>> {
    let rows = client.query(
        "SELECT n.nspname::text, c.relname::text, a.attname::text \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         JOIN pg_attribute a ON a.attrelid = c.oid \
         WHERE c.oid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped \
           AND has_column_privilege(c.oid, a.attnum, 'SELECT') \
         ORDER BY a.attnum",
        &[&configured],
    )?;
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let (schema, table): (String, String) = (first.get(0), first.get(1));
    let columns: Vec<String> = rows.iter().map(|r| r.get(2)).collect();
    let select = projection_sql(SourceType::Postgres, Some(&schema), &table, &columns);
    Ok(Some((schema, table, ReadableColumns { columns, select })))
}

/// The relation a configured name means and the columns of it the account may `SELECT`; refused when it may read none.
pub(crate) fn readable_columns(
    client: &mut Client,
    configured: &str,
) -> Result<(String, String, ReadableColumns)> {
    let Some(read) = read_columns(client, configured)? else {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_COLUMNS_UNREADABLE,
            "postgres: `{configured}`: `columns_from: grants` found no column this account \
             may SELECT (or no such table), so nothing of it could be delivered; nothing was \
             read or written. GRANT SELECT on the table or on its columns to the account"
        );
    };
    Ok(read)
}

/// The readable columns of every configured table, keyed by its configured name.
pub(crate) fn readable_columns_of(
    client: &mut Client,
    configured: &[String],
) -> Result<HashMap<String, ReadableColumns>> {
    let mut out = HashMap::with_capacity(configured.len());
    for cfg in configured {
        crate::source::cdc::validate_table_ident(cfg)?;
        let (_, _, readable) = readable_columns(client, cfg)?;
        out.insert(cfg.clone(), readable);
    }
    Ok(out)
}

/// The readable columns of every configured table as they are now; a table the account may read nothing of is absent.
pub(crate) fn readable_columns_now(
    client: &mut Client,
    configured: &[String],
) -> Result<HashMap<String, ReadableColumns>> {
    let mut out = HashMap::with_capacity(configured.len());
    for cfg in configured {
        if let Some((_, _, readable)) = read_columns(client, cfg)? {
            out.insert(cfg.clone(), readable);
        }
    }
    Ok(out)
}

/// The PostgreSQL catalog reads behind the key-readability check.
struct PgKeys<'a> {
    client: &'a mut Client,
}

impl KeyCatalog for PgKeys<'_> {
    fn relation(&self, configured: &str) -> Result<String> {
        Ok(configured.to_string())
    }

    fn primary_key(&mut self, configured: &str) -> Result<Vec<String>> {
        let joined: Option<String> = self
            .client
            .query_one(&super::primary_key_sql(configured), &[])?
            .get(0);
        Ok(crate::source::split_key_list(joined).unwrap_or_default())
    }

    fn denies(&mut self, configured: &str, column: &str) -> Result<bool> {
        match self.client.query_one(
            "SELECT has_column_privilege(to_regclass($1), $2, 'SELECT')",
            &[&configured, &column],
        ) {
            Ok(row) => Ok(seeded!(
                "column-grants-unreadable-key-accepted",
                false,
                !row.get::<_, bool>(0)
            )),
            Err(e) if e.code() == Some(&SqlState::UNDEFINED_COLUMN) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

/// The readable columns of every configured table, and the key each table is told apart by must be readable.
pub(crate) fn resolve_grants_on(
    client: &mut Client,
    configured: &[String],
    declared: &HashMap<String, Vec<String>>,
) -> Result<HashMap<String, ReadableColumns>> {
    let readable = readable_columns_of(client, configured)?;
    require_readable_keys(
        "postgres",
        &mut PgKeys { client },
        configured,
        &readable,
        declared,
        NAMES_MATCH,
    )?;
    Ok(readable)
}

/// The grants of a `columns_from: grants` run, read once on a connection of their own before the anchor.
pub(crate) fn resolve_grants(
    url: &str,
    tls: Option<&TlsConfig>,
    configured: &[String],
    declared: &HashMap<String, Vec<String>>,
) -> Result<Arc<HashMap<String, ReadableColumns>>> {
    if configured.is_empty() {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
            "postgres cdc: `columns_from: grants` needs the captured tables named"
        );
    }
    let mut client = super::connect_client(url, tls)?;
    resolve_grants_on(&mut client, configured, declared).map(Arc::new)
}

/// The columns the run may deliver of the captured table `schema.table`: the entry of the first configured name that routes to it.
pub(crate) fn readable_of<'a>(
    resolved: &'a HashMap<String, ReadableColumns>,
    configured: &[String],
    bare_schema: &HashMap<String, String>,
    schema: &str,
    table: &str,
) -> Option<&'a [String]> {
    configured
        .iter()
        .find(|c| {
            crate::source::cdc::sink::table_matches(
                crate::source::cdc::CdcEngine::Postgres,
                c,
                schema,
                table,
            ) && bare_schema.get(*c).is_none_or(|s| s == schema)
        })
        .and_then(|c| resolved.get(c))
        .map(|r| r.columns.as_slice())
}

/// Whether an update changed only columns outside `names`: both images carry every column and agree on all of them.
pub(crate) fn only_unread_columns_changed(
    names: &[String],
    after: &[RivetValue],
    before_names: &[String],
    before: &[RivetValue],
) -> bool {
    before_names.len() == names.len()
        && names.iter().zip(after).all(|(n, a)| {
            before_names
                .iter()
                .position(|b| b == n)
                .and_then(|i| before.get(i))
                .is_some_and(|b| b == a)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::postgres::cdc::parse_test_decoding_keeping;

    fn names(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|c| c.to_string()).collect()
    }

    fn readable(cols: &[&str]) -> ReadableColumns {
        ReadableColumns {
            columns: names(cols),
            select: String::new(),
        }
    }

    fn keep_id_name(line: &str) -> Option<crate::source::cdc::ChangeEvent> {
        let resolved = HashMap::from([("public.t".to_string(), readable(&["id", "name"]))]);
        let configured = names(&["public.t"]);
        let bare = HashMap::new();
        let keep =
            |schema: &str, table: &str| readable_of(&resolved, &configured, &bare, schema, table);
        parse_test_decoding_keeping("0/1", line, &Default::default(), Some(&keep)).unwrap()
    }

    #[test]
    fn an_insert_keeps_the_readable_columns_and_their_values_only() {
        let ev = keep_id_name(
            "table public.t: INSERT: id[integer]:1 name[text]:'ann' secret[text]:'hush'",
        )
        .unwrap();
        assert_eq!(ev.image_names.as_deref(), Some(&names(&["id", "name"])[..]));
        assert_eq!(
            ev.after,
            Some(vec![RivetValue::Int(1), RivetValue::Bytes(b"ann".to_vec())])
        );
    }

    #[test]
    fn a_table_the_run_does_not_capture_is_parsed_whole() {
        let ev =
            keep_id_name("table public.other: INSERT: id[integer]:1 secret[text]:'hush'").unwrap();
        assert_eq!(
            ev.image_names.as_deref(),
            Some(&names(&["id", "secret"])[..])
        );
    }

    #[test]
    fn a_hidden_column_of_an_unsupported_type_poisons_nothing() {
        let ev = keep_id_name(
            "table public.t: INSERT: id[integer]:1 name[text]:'ann' flags[bit(3)]:'101'",
        )
        .unwrap();
        assert_eq!(ev.image_names.as_deref(), Some(&names(&["id", "name"])[..]));
        assert!(ev.poison.is_none());
    }

    #[test]
    fn an_update_of_hidden_columns_only_under_both_images_is_no_change() {
        let line = "table public.t: UPDATE: old-key: id[integer]:1 name[text]:'ann' secret[text]:'a' \
                    new-tuple: id[integer]:1 name[text]:'ann' secret[text]:'b'";
        assert!(keep_id_name(line).is_none());
    }

    #[test]
    fn an_update_of_a_readable_column_is_delivered_with_readable_columns_in_both_images() {
        let line = "table public.t: UPDATE: old-key: id[integer]:1 name[text]:'ann' secret[text]:'a' \
                    new-tuple: id[integer]:1 name[text]:'bob' secret[text]:'a'";
        let ev = keep_id_name(line).unwrap();
        assert_eq!(
            ev.before_names.as_deref(),
            Some(&names(&["id", "name"])[..])
        );
        assert_eq!(ev.image_names.as_deref(), Some(&names(&["id", "name"])[..]));
        assert_eq!(
            ev.after,
            Some(vec![RivetValue::Int(1), RivetValue::Bytes(b"bob".to_vec())])
        );
    }

    #[test]
    fn an_update_with_no_old_image_cannot_be_told_hidden_only_and_is_delivered() {
        let line = "table public.t: UPDATE: id[integer]:1 name[text]:'ann' secret[text]:'b'";
        let ev = keep_id_name(line).unwrap();
        assert_eq!(ev.image_names.as_deref(), Some(&names(&["id", "name"])[..]));
        assert!(ev.before.is_none());
    }

    #[test]
    fn a_delete_pre_image_keeps_the_readable_columns_only() {
        let ev =
            keep_id_name("table public.t: DELETE: id[integer]:2 name[text]:'bob' secret[text]:'x'")
                .unwrap();
        assert_eq!(ev.image_names.as_deref(), Some(&names(&["id", "name"])[..]));
        assert_eq!(ev.before.as_ref().map(Vec::len), Some(2));
    }

    #[test]
    fn readable_of_routes_a_bare_name_by_its_schema() {
        let resolved = HashMap::from([("t".to_string(), readable(&["id"]))]);
        let configured = names(&["t"]);
        let bare = HashMap::from([("t".to_string(), "app".to_string())]);
        assert!(readable_of(&resolved, &configured, &bare, "app", "t").is_some());
        assert!(readable_of(&resolved, &configured, &bare, "public", "t").is_none());
        assert!(readable_of(&resolved, &configured, &bare, "app", "u").is_none());
    }

    #[test]
    fn only_unread_columns_changed_needs_both_full_images_and_equal_readable_values() {
        let n = names(&["id", "name"]);
        let a = vec![RivetValue::Int(1), RivetValue::Int(2)];
        let b = vec![RivetValue::Int(1), RivetValue::Int(3)];
        assert!(only_unread_columns_changed(&n, &a, &n, &a));
        assert!(!only_unread_columns_changed(&n, &a, &n, &b));
        assert!(!only_unread_columns_changed(
            &n,
            &a,
            &names(&["id"]),
            &a[..1]
        ));
    }
}
