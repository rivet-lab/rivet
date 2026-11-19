//! SQL Server preflight diagnostics for `rivet check` / `rivet plan` (parity with [`super::postgres`] / [`super::mysql`]).

use super::ExportDiagnostic;
use super::analysis::*;
use super::cursor_expr::incremental_key_expr;
use super::schema_error::PreflightSchemaError;
use crate::config::{ExportConfig, ExportMode, SourceType, TlsConfig};
use crate::error::Result;
use crate::source::Source;
use crate::source::mssql::MssqlSource;

/// Connect once and build one [`ExportDiagnostic`] per export.
pub(super) fn check_mssql(
    url: &str,
    tls: Option<&TlsConfig>,
    exports: &[&ExportConfig],
    grants: bool,
) -> Result<Vec<ExportDiagnostic>> {
    let mut conn = MssqlSource::connect_with_tls(url, tls)?;

    super::collect_diagnostics(exports, |export| diagnose_mssql(&mut conn, export, grants))
}

/// Diagnose a single export without printing — used by `rivet plan`.
pub(super) fn diagnose_export_mssql(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
    grants: bool,
) -> Result<super::ExportDiagnostic> {
    let mut conn = MssqlSource::connect_with_tls(url, tls)?;
    diagnose_mssql(&mut conn, export, grants)
}

/// `@@MAX_CONNECTIONS` — SQL Server's configured `user connections` cap and the portable analogue of PG `max_connections` / MySQL `@@max_connections`.
fn fetch_max_connections_mssql(conn: &mut MssqlSource) -> Option<u32> {
    conn.query_scalar("SELECT @@MAX_CONNECTIONS")
        .ok()
        .flatten()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .filter(|&n| n > 0)
}

/// The `columns_from: grants` projection of a `table:`, which the run reads instead of `SELECT *`.
fn grants_projection(
    conn: &mut MssqlSource,
    export: &ExportConfig,
    grants: bool,
) -> Result<Option<String>> {
    let Some(table) = export.table.as_ref().filter(|_| grants) else {
        return Ok(None);
    };
    Ok(conn.readable_columns(table)?.map(|r| r.select))
}

fn diagnose_mssql(
    conn: &mut MssqlSource,
    export: &ExportConfig,
    grants: bool,
) -> Result<ExportDiagnostic> {
    let base_query = match grants_projection(conn, export, grants)? {
        Some(select) => select,
        None => resolve_preflight_base_query(export),
    };
    let base_query = base_query.as_str();

    let probe = format!("SELECT TOP 0 1 AS _ok FROM ({base_query}) AS _rivet_probe");
    if let Some(fail) = conn
        .query_scalar(&probe)
        .err()
        .and_then(|e| mssql_schema_verdict(e, export))
    {
        return Err(fail);
    }

    let base_table_owned = strip_select_star_from(base_query)
        .map(std::borrow::Cow::Borrowed)
        .or_else(|| table_from_simple_query(base_query));
    let base_table = base_table_owned.as_deref();
    let unfiltered_table = whole_table_relation(base_table, base_query);

    let auto_pk: Option<String> =
        auto_pk_probe_target(export, base_table).and_then(|t| single_int_pk_mssql(conn, t));

    let range_col = preflight_range_col_resolved(export, auto_pk.as_deref());

    let row_estimate = unfiltered_table.and_then(|table| row_estimate_mssql(conn, table));

    let avg_row_bytes = unfiltered_table.and_then(|table| avg_row_bytes_mssql(conn, table));

    let (range_min, range_max) = if export.mode == ExportMode::Incremental {
        match incremental_key_expr(export, SourceType::Mssql) {
            Some(expr) => range_min_max_mssql(conn, base_query, unfiltered_table, &expr),
            None => (None, None),
        }
    } else if let Some(col) = range_col {
        let expr = crate::sql::quote_ident(SourceType::Mssql, col);
        range_min_max_mssql(conn, base_query, unfiltered_table, &expr)
    } else {
        (None, None)
    };

    let catalog_index = index_probe_target(export, auto_pk.as_deref(), base_table)
        .and_then(|(table, col)| column_has_index_mssql(conn, table, col));

    let db_max_connections = fetch_max_connections_mssql(conn);

    Ok(assemble_diagnostic(
        export,
        ProbeFacts {
            auto_pk,
            row_estimate,
            avg_row_bytes,
            range_min,
            range_max,
            scan_type: None,
            plan_uses_index: false,
            catalog_index,
            db_max_connections,
        },
    ))
}

/// Row estimate from the `sys.partitions` catalog for `[schema.]table`.
fn mssql_schema_verdict(e: anyhow::Error, export: &ExportConfig) -> Option<anyhow::Error> {
    if crate::source::names_a_missing_relation(&e) {
        return Some(crate::source::code_a_missing_relation(
            e,
            &export.name,
            export.table.as_deref(),
            export.query.as_deref(),
        ));
    }
    let m = format!("{e:#}");
    let detail = if m.contains("Invalid column name") {
        "a column in the export's query does not exist"
    } else {
        return None;
    };
    let code_label = mssql_error_code(&m)
        .map(|c| format!("error {c}"))
        .unwrap_or_else(|| "SQL Server schema error".into());
    Some(PreflightSchemaError::new(detail, code_label).into_error())
}

/// Pull the SQL Server error number from a tiberius error's Display string (`"… (code.
fn mssql_error_code(msg: &str) -> Option<u16> {
    msg.split("code: ")
        .nth(1)?
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()
}

fn row_estimate_mssql(conn: &mut MssqlSource, qualified_table: &str) -> Option<i64> {
    let (schema, table) = split_qualified(qualified_table);
    let sql = format!(
        "SELECT SUM(p.rows) FROM sys.partitions p \
         JOIN sys.objects o ON o.object_id = p.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE s.name = N'{}' AND o.name = N'{}' AND p.index_id IN (0,1)",
        schema.replace('\'', "''"),
        table.replace('\'', "''"),
    );
    match conn.query_scalar(&sql) {
        Ok(opt) => opt.and_then(|s| s.parse::<i64>().ok()).map(|n| n.max(0)),
        Err(e) => {
            log::debug!("preflight: row-estimate probe failed for {qualified_table}: {e}");
            None
        }
    }
}

/// Average bytes/row from `dm_db_partition_stats` for `[schema.]table`.
fn avg_row_bytes_mssql(conn: &mut MssqlSource, qualified_table: &str) -> Option<i64> {
    let (schema, table) = split_qualified(qualified_table);
    let sql = format!(
        "SELECT CASE WHEN SUM(p.row_count) > 0 \
                THEN SUM(p.used_page_count) * 8192 / SUM(p.row_count) ELSE NULL END \
         FROM sys.dm_db_partition_stats p \
         JOIN sys.objects o ON o.object_id = p.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE s.name = N'{}' AND o.name = N'{}' AND p.index_id IN (0,1)",
        schema.replace('\'', "''"),
        table.replace('\'', "''"),
    );
    match conn.query_scalar(&sql) {
        Ok(opt) => opt.and_then(|s| s.parse::<i64>().ok()).filter(|n| *n > 0),
        Err(e) => {
            log::debug!("preflight: row-width probe failed for {qualified_table}: {e}");
            None
        }
    }
}

/// `MIN`/`MAX` of `expr` over the export's base relation, as display strings.
fn range_min_max_mssql(
    conn: &mut MssqlSource,
    base_query: &str,
    base_table: Option<&str>,
    expr: &str,
) -> (Option<String>, Option<String>) {
    const US: char = '\u{1f}';
    let select_list = format!(
        "CONCAT(CONVERT(varchar(64), MIN({expr})), CHAR(31), CONVERT(varchar(64), MAX({expr})))"
    );
    let sql = match base_table {
        Some(table) => format!("SELECT {select_list} FROM {table}"),
        None => format!("SELECT {select_list} FROM ({base_query}) AS _rivet"),
    };
    match conn.query_scalar(&sql) {
        Ok(Some(agg)) => {
            let mut parts = agg.splitn(2, US);
            let min_v = parts.next().filter(|s| !s.is_empty()).map(str::to_string);
            let max_v = parts.next().filter(|s| !s.is_empty()).map(str::to_string);
            (min_v, max_v)
        }
        Ok(None) => (None, None),
        Err(e) => {
            log::debug!("preflight: range probe on '{expr}' failed: {e}");
            (None, None)
        }
    }
}

/// `Some(true)` when `column` is the single leading key of *some* index on `[schema.]table`, `Some(false)` when the catalog probe ran cleanly and found none, `None` when the probe itself failed.
fn single_int_pk_mssql(conn: &mut MssqlSource, qualified_table: &str) -> Option<String> {
    let (schema, table) = split_qualified(qualified_table);
    conn.query_scalar(&crate::source::mssql::single_int_pk_sql(schema, table))
        .ok()
        .flatten()
}

fn column_has_index_mssql(
    conn: &mut MssqlSource,
    qualified_table: &str,
    column: &str,
) -> Option<bool> {
    let (schema, table) = split_qualified(qualified_table);
    let sql = format!(
        "SELECT COUNT(*) FROM sys.indexes i \
         JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
         JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
         JOIN sys.objects o ON o.object_id = i.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE ic.key_ordinal = 1 AND i.index_id > 0 \
           AND s.name = N'{}' AND o.name = N'{}' AND c.name = N'{}'",
        schema.replace('\'', "''"),
        table.replace('\'', "''"),
        column.replace('\'', "''"),
    );
    match conn.query_scalar(&sql) {
        Ok(opt) => Some(opt.and_then(|s| s.parse::<i64>().ok()).unwrap_or(0) > 0),
        Err(e) => {
            log::debug!("preflight: index probe failed for {qualified_table}.{column}: {e}");
            None
        }
    }
}

/// Split a `[schema.]table` name into `(schema, table)`, defaulting the schema to `dbo` when unqualified (SQL Server's default schema.
fn split_qualified(qualified_table: &str) -> (&str, &str) {
    match qualified_table.split_once('.') {
        Some((s, t)) => (s, t),
        None => ("dbo", qualified_table),
    }
}

use super::postgres::table_from_simple_query;
use crate::sql::strip_select_star_from;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_qualified_defaults_schema_to_dbo() {
        assert_eq!(split_qualified("orders"), ("dbo", "orders"));
    }

    #[test]
    fn split_qualified_keeps_explicit_schema() {
        assert_eq!(split_qualified("dbo.orders"), ("dbo", "orders"));
        assert_eq!(split_qualified("sales.line_items"), ("sales", "line_items"));
    }

    fn base_table_of(q: &str) -> Option<String> {
        strip_select_star_from(q)
            .map(str::to_string)
            .or_else(|| table_from_simple_query(q).map(|t| t.into_owned()))
    }

    #[test]
    fn base_table_from_select_star_shortcut() {
        assert_eq!(
            base_table_of("SELECT * FROM dbo.orders").as_deref(),
            Some("dbo.orders")
        );
        assert_eq!(
            base_table_of("SELECT * FROM orders").as_deref(),
            Some("orders")
        );
    }

    #[test]
    fn base_table_from_init_column_list_query() {
        let q = "SELECT id, user_id, product FROM dbo.orders";
        assert_eq!(base_table_of(q).as_deref(), Some("dbo.orders"));
    }

    #[test]
    fn base_table_none_for_multi_relation_query() {
        assert_eq!(
            base_table_of("SELECT * FROM orders JOIN users ON orders.user_id = users.id"),
            None
        );
        assert_eq!(base_table_of("SELECT * FROM orders, users"), None);
        assert_eq!(base_table_of("SELECT * FROM (SELECT 1 AS x) AS s"), None);
    }

    #[test]
    fn the_schema_verdict_codes_a_missing_relation_and_names_a_missing_column() {
        let orders = ExportConfig {
            table: Some("orders".to_string()),
            ..crate::config::sample_export("orders")
        };
        let probe = |text: &str| {
            crate::error::coded(
                crate::error::codes::SOURCE_QUERY_FAILED,
                format!("mssql: scalar query failed: {text}"),
            )
        };
        let missing = mssql_schema_verdict(
            probe("Token error: 'Invalid object name 'orders'.' on server x executing  on line 1 (code: 208, state: 1, class: 16)"),
            &orders,
        )
        .expect("a missing relation is a verdict");
        assert_eq!(
            crate::error::error_code(&missing),
            Some("RIVET_SOURCE_TABLE_NOT_FOUND"),
            "{missing:#}"
        );
        assert!(
            format!("{missing:#}").contains("(`table: orders`)"),
            "{missing:#}"
        );
        let column = mssql_schema_verdict(
            probe("Token error: 'Invalid column name 'totl'.' on server x executing  on line 1 (code: 207, state: 1, class: 16)"),
            &orders,
        )
        .expect("a missing column is a verdict");
        let text = format!("{column:#}");
        assert!(
            text.starts_with(
                "preflight: a column in the export's query does not exist (error 207)."
            ) && crate::error::error_code(&column) == Some("RIVET_SOURCE_QUERY_FAILED"),
            "{text}"
        );
        assert!(
            mssql_schema_verdict(probe("connection reset by peer"), &orders).is_none(),
            "an operational failure stays fail-soft"
        );
    }

    #[test]
    fn mssql_error_code_parsed_from_tiberius_display() {
        let m = "mssql: scalar query failed: Token error: 'Invalid object name \
                 'ordrs'.' on server x executing  on line 1 (code: 208, state: 1, class: 16)";
        assert_eq!(mssql_error_code(m), Some(208));
        assert_eq!(
            mssql_error_code("Invalid column name 'totl'. (code: 207)"),
            Some(207)
        );
        assert_eq!(mssql_error_code("connection reset; no code here"), None);
    }
}
