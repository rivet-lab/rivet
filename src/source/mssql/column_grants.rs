//! `source.columns_from: grants` on SQL Server: the columns an account may `SELECT`, asked of the server.

use std::collections::HashMap;
use std::sync::Arc;

use super::{MssqlClient, MssqlSource};
use crate::config::{SourceType, TlsConfig};
use crate::error::Result;
use crate::source::Source;
use crate::source::cdc::ReadableColumns;
use crate::source::column_grants::{KeyCatalog, NamesMatch, projection_sql, require_readable_keys};
use crate::sql::quote_ident;

/// What `columns_from: grants` guarantees, said once to the operator at `rivet check` and at the start of a change-stream run.
pub(crate) const BOUNDARY: &str = "columns_from: grants - batch reads are enforced by the server (a column the \
account may not SELECT is never selected). The SQL Server change stream is enforced by the server too: \
the capture instance's change function returns nothing to an account that may not SELECT every column \
the instance captures, so rivet refuses an instance wider than the account's grants and reads one made \
of exactly the columns it may SELECT (sys.sp_cdc_enable_table @captured_column_list). A grant change \
is seen when rivet reads each batch of the change table, not when it happens: the stream is then refused \
and nothing of that batch is delivered, but a REVOKE undone before the next read is not seen.";

/// SQL Server column names fold case.
pub(crate) const NAMES_MATCH: NamesMatch = str::eq_ignore_ascii_case;

/// One table's columns in table order, each with the server's verdict on whether this account may `SELECT` it.
const READABLE_SQL: &str = "SELECT OBJECT_SCHEMA_NAME(c.object_id), OBJECT_NAME(c.object_id), c.name, \
     CAST(ISNULL(HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(c.object_id)) + N'.' \
     + QUOTENAME(OBJECT_NAME(c.object_id)), N'OBJECT', N'SELECT', c.name, N'COLUMN'), 0) AS int) \
     FROM sys.columns c WHERE c.object_id = OBJECT_ID(@P1) AND c.is_hidden = 0 ORDER BY c.column_id";

/// The server's verdict (1 may, 0 may not, NULL no such column) on whether this account may `SELECT` one column of a table.
const KEY_VERDICT_SQL: &str = "SELECT CAST(HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(OBJECT_ID(@P1))) \
     + N'.' + QUOTENAME(OBJECT_NAME(OBJECT_ID(@P1))), N'OBJECT', N'SELECT', @P2, N'COLUMN') AS int)";

/// The columns a capture instance captures, in capture order, with the relation it reads.
const CAPTURED_SQL: &str = "SELECT OBJECT_SCHEMA_NAME(ct.source_object_id), \
     OBJECT_NAME(ct.source_object_id), cc.column_name \
     FROM cdc.change_tables ct JOIN cdc.captured_columns cc ON cc.object_id = ct.object_id \
     WHERE ct.capture_instance = @P1 ORDER BY cc.column_ordinal";

/// The relation a configured name means and the columns of it the account may `SELECT` in table order, each judged by the server; `None` when it may read none or there is no such table.
fn read_columns(
    rt: &tokio::runtime::Runtime,
    client: &mut MssqlClient,
    configured: &str,
) -> Result<Option<(String, String, ReadableColumns)>> {
    let rows = rt
        .block_on(async {
            client
                .query(READABLE_SQL, &[&configured])
                .await?
                .into_first_result()
                .await
        })
        .map_err(|e| {
            crate::error::coded_driver(
                crate::error::codes::SOURCE_QUERY_FAILED,
                &format!("mssql: `{configured}`: reading the columns this account may SELECT"),
                e,
            )
        })?;
    let mut relation: Option<(String, String)> = None;
    let mut columns = Vec::new();
    for row in &rows {
        let (Some(schema), Some(table), Some(name)) = (
            row.get::<&str, _>(0),
            row.get::<&str, _>(1),
            row.get::<&str, _>(2),
        ) else {
            continue;
        };
        relation.get_or_insert_with(|| (schema.to_string(), table.to_string()));
        if seeded!(
            "column-grants-mssql-every-column-read-as-readable",
            true,
            row.get::<i32, _>(3) == Some(1)
        ) {
            columns.push(name.to_string());
        }
    }
    Ok(relation
        .filter(|_| !columns.is_empty())
        .map(|(schema, table)| {
            let select = projection_sql(SourceType::Mssql, Some(&schema), &table, &columns);
            (schema, table, ReadableColumns { columns, select })
        }))
}

/// The relation a configured name means and the columns of it the account may `SELECT`; refused when it may read none.
pub(crate) fn readable_relation(
    src: &mut MssqlSource,
    configured: &str,
) -> Result<(String, String, ReadableColumns)> {
    let MssqlSource { rt, client, .. } = src;
    let Some(read) = read_columns(rt, client, configured)? else {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_COLUMNS_UNREADABLE,
            "mssql: `{configured}`: `columns_from: grants` found no column this account \
             may SELECT (or no such table), so nothing of it could be delivered; nothing was \
             read or written"
        );
    };
    Ok(read)
}

/// The readable columns of every configured table as they are now; a table the account may read nothing of is absent.
pub(crate) fn readable_now(
    rt: &tokio::runtime::Runtime,
    client: &mut MssqlClient,
    configured: &[String],
) -> Result<HashMap<String, ReadableColumns>> {
    let mut out = HashMap::with_capacity(configured.len());
    for cfg in configured {
        if let Some((_, _, readable)) = read_columns(rt, client, cfg)? {
            out.insert(cfg.clone(), readable);
        }
    }
    Ok(out)
}

/// A capture instance's relation and captured columns.
pub(crate) struct Capture {
    pub instance: String,
    pub schema: String,
    pub table: String,
    pub columns: Vec<String>,
}

/// The capture instance `instance` as the catalog holds it; `None` when it names no instance.
pub(crate) fn captured_columns(src: &mut MssqlSource, instance: &str) -> Result<Option<Capture>> {
    let MssqlSource { rt, client, .. } = src;
    let rows = rt
        .block_on(async {
            client
                .query(CAPTURED_SQL, &[&instance])
                .await?
                .into_first_result()
                .await
        })
        .map_err(|e| {
            crate::error::coded_driver(
                crate::error::codes::SOURCE_QUERY_FAILED,
                &format!(
                    "sqlserver cdc: reading cdc.captured_columns for capture instance \
                     '{instance}' (the login needs SELECT on the cdc schema)"
                ),
                e,
            )
        })?;
    let mut capture: Option<Capture> = None;
    for row in &rows {
        let (Some(schema), Some(table), Some(name)) = (
            row.get::<&str, _>(0),
            row.get::<&str, _>(1),
            row.get::<&str, _>(2),
        ) else {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_PREREQUISITE,
                "sqlserver cdc: capture instance '{instance}' reads a table this login cannot \
                 see, so `columns_from: grants` cannot tell which of its columns the instance \
                 holds; nothing was read or written. GRANT SELECT on the columns the account \
                 may read of that table"
            );
        };
        capture
            .get_or_insert_with(|| Capture {
                instance: instance.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
                columns: Vec::new(),
            })
            .columns
            .push(name.to_string());
    }
    Ok(capture)
}

/// The statement that creates an instance of `schema.table` over exactly `columns`.
fn enable_statement(schema: &str, table: &str, instance: &str, columns: &[String]) -> String {
    let quote = |name: &str| quote_ident(SourceType::Mssql, name);
    let list = columns
        .iter()
        .map(|c| quote(c))
        .collect::<Vec<_>>()
        .join(", ")
        .replace('\'', "''");
    let lit = |s: &str| s.replace('\'', "''");
    format!(
        "EXEC sys.sp_cdc_enable_table @source_schema = N'{}', @source_name = N'{}', \
         @role_name = NULL, @capture_instance = N'{}', @captured_column_list = N'{list}'",
        lit(schema),
        lit(table),
        lit(instance),
    )
}

/// The columns of `a` that `b` lacks (SQL Server column names fold case).
fn missing_from(a: &[String], b: &[String]) -> Vec<String> {
    a.iter()
        .filter(|x| !b.iter().any(|y| y.eq_ignore_ascii_case(x)))
        .cloned()
        .collect()
}

/// The refusal of a capture instance whose columns are not exactly the columns the account may `SELECT`; `None` when they are.
pub(crate) fn capture_refusal(capture: &Capture, readable: &[String]) -> Option<String> {
    let forbidden = missing_from(&capture.columns, readable);
    let left_out = missing_from(readable, &capture.columns);
    let list = |cols: &[String]| {
        cols.iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let Capture {
        instance,
        schema,
        table,
        ..
    } = capture;
    let fault = if !forbidden.is_empty() {
        format!(
            "capture instance '{instance}' of `{schema}.{table}` holds {} that this account may \
             not SELECT, and SQL Server withholds the changes of an instance wider than the \
             account's grants (its change function returns nothing to a reader that may not \
             SELECT every captured column)",
            list(&forbidden)
        )
    } else if !left_out.is_empty() {
        format!(
            "capture instance '{instance}' of `{schema}.{table}` leaves out {} that this account \
             may SELECT, so every change event would miss it while the baseline snapshot carries \
             it (the rows would be recorded as NULL rather than as absent)",
            list(&left_out)
        )
    } else {
        return None;
    };
    let remedy = enable_statement(schema, table, &format!("{instance}_grants"), readable);
    Some(format!(
        "sqlserver cdc: under `columns_from: grants` {fault}; nothing was read or written. \
         Create an instance over exactly the readable columns and point `cdc.capture_instance` \
         at it: {remedy}. A checkpoint taken before the new instance existed cannot resume on \
         it (it is refused as a log gap): re-baseline with `cdc.initial: snapshot` and a fresh \
         checkpoint"
    ))
}

/// The SQL Server catalog reads behind the key-readability check.
struct MssqlKeys<'a> {
    src: &'a mut MssqlSource,
}

impl KeyCatalog for MssqlKeys<'_> {
    fn relation(&self, configured: &str) -> Result<String> {
        Ok(configured.to_string())
    }

    fn primary_key(&mut self, configured: &str) -> Result<Vec<String>> {
        Ok(self.src.primary_key(configured)?.unwrap_or_default())
    }

    fn denies(&mut self, configured: &str, column: &str) -> Result<bool> {
        let MssqlSource { rt, client, .. } = &mut *self.src;
        let rows = rt
            .block_on(async {
                client
                    .query(KEY_VERDICT_SQL, &[&configured, &column])
                    .await?
                    .into_first_result()
                    .await
            })
            .map_err(|e| {
                crate::error::coded_driver(
                    crate::error::codes::SOURCE_QUERY_FAILED,
                    &format!(
                        "mssql: `{configured}`: asking the server whether `{column}` may be read"
                    ),
                    e,
                )
            })?;
        let verdict = rows.first().and_then(|r| r.get::<i32, _>(0));
        Ok(seeded!(
            "column-grants-unreadable-key-accepted",
            false,
            verdict == Some(0)
        ))
    }
}

/// The readable columns of every configured table, keyed by its configured name; each table's key must be readable and its capture instance must hold exactly its readable columns.
pub(crate) fn resolve_grants(
    url: &str,
    tls: Option<&TlsConfig>,
    configured: &[String],
    capture_instance: Option<&str>,
    declared: &HashMap<String, Vec<String>>,
) -> Result<Arc<HashMap<String, ReadableColumns>>> {
    let mut src = MssqlSource::connect_with_tls(url, tls)?;
    let capture = match capture_instance {
        Some(ci) => captured_columns(&mut src, ci)?,
        None => None,
    };
    let mut readable = HashMap::with_capacity(configured.len());
    for cfg in configured {
        let (_, _, columns) = readable_relation(&mut src, cfg)?;
        readable.insert(cfg.clone(), columns);
    }
    require_readable_keys(
        "mssql",
        &mut MssqlKeys { src: &mut src },
        configured,
        &readable,
        declared,
        NAMES_MATCH,
    )?;
    for cfg in configured {
        let Some(c) = capture.as_ref().filter(|c| {
            crate::source::cdc::sink::table_matches(
                crate::source::cdc::CdcEngine::Mssql,
                cfg,
                &c.schema,
                &c.table,
            )
        }) else {
            continue;
        };
        if seeded!("cdc-capture-wider-than-grants-accepted", false, true)
            && let Some(why) = capture_refusal(c, &readable[cfg].columns)
        {
            crate::rivet_bail!(crate::error::codes::SOURCE_CDC_PREREQUISITE, "{why}");
        }
    }
    Ok(Arc::new(readable))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn capture(columns: &[&str]) -> Capture {
        Capture {
            instance: "dbo_t".into(),
            schema: "dbo".into(),
            table: "t".into(),
            columns: s(columns),
        }
    }

    #[test]
    fn a_capture_equal_to_the_readable_columns_is_accepted_in_any_case_or_order() {
        assert!(capture_refusal(&capture(&["id", "name"]), &s(&["id", "name"])).is_none());
        assert!(capture_refusal(&capture(&["NAME", "Id"]), &s(&["id", "name"])).is_none());
    }

    #[test]
    fn a_capture_holding_a_forbidden_column_is_refused_with_the_statement_that_fixes_it() {
        let said = capture_refusal(&capture(&["id", "name", "pass"]), &s(&["id", "name"]))
            .expect("refused");
        assert_eq!(
            said,
            "sqlserver cdc: under `columns_from: grants` capture instance 'dbo_t' of `dbo.t` \
             holds `pass` that this account may not SELECT, and SQL Server withholds the \
             changes of an instance wider than the account's grants (its change function \
             returns nothing to a reader that may not SELECT every captured column); nothing \
             was read or written. Create an instance over exactly the readable columns and \
             point `cdc.capture_instance` at it: EXEC sys.sp_cdc_enable_table @source_schema = \
             N'dbo', @source_name = N't', @role_name = NULL, @capture_instance = N'dbo_t_grants', \
             @captured_column_list = N'[id], [name]'. A checkpoint taken before the new \
             instance existed cannot resume on it (it is refused as a log gap): re-baseline \
             with `cdc.initial: snapshot` and a fresh checkpoint"
        );
    }

    #[test]
    fn a_capture_leaving_out_a_readable_column_is_refused_with_the_same_remedy() {
        let said = capture_refusal(&capture(&["id"]), &s(&["id", "name"])).expect("refused");
        assert!(
            said.contains("leaves out `name` that this account may SELECT")
                && said.contains("@captured_column_list = N'[id], [name]'"),
            "{said}"
        );
    }

    #[test]
    fn the_remedy_statement_escapes_quotes_in_every_name() {
        let stmt = enable_statement("s'x", "t'y", "ci", &s(&["a'b", "c]d"]));
        assert_eq!(
            stmt,
            "EXEC sys.sp_cdc_enable_table @source_schema = N's''x', @source_name = N't''y', \
             @role_name = NULL, @capture_instance = N'ci', \
             @captured_column_list = N'[a''b], [c]]d]'"
        );
    }
}
