use super::ExportDiagnostic;
use super::analysis::*;
use super::cursor_expr::incremental_key_expr;
use super::schema_error::PreflightSchemaError;
use crate::config::{ExportConfig, ExportMode, SourceType, TlsConfig};
use crate::connect::postgres::read_pinned;
use crate::error::Result;

/// Connect once and build one [`ExportDiagnostic`] per export.
pub(super) fn check_postgres(
    url: &str,
    tls: Option<&TlsConfig>,
    exports: &[&ExportConfig],
) -> Result<Vec<ExportDiagnostic>> {
    let mut client = crate::source::postgres::connect_client(url, tls)?;
    let db_max_connections = fetch_max_connections_pg(&mut client);

    super::collect_diagnostics(exports, |export| {
        diagnose_pg(&mut client, export, db_max_connections)
    })
}

/// Diagnose a single export without printing — used by `rivet plan`.
pub(super) fn diagnose_export_pg(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
) -> Result<super::ExportDiagnostic> {
    let mut client = crate::source::postgres::connect_client(url, tls)?;
    let db_max_connections = fetch_max_connections_pg(&mut client);
    diagnose_pg(&mut client, export, db_max_connections)
}

fn fetch_max_connections_pg(client: &mut postgres::Client) -> Option<u32> {
    let rows = match client.query("SELECT current_setting('max_connections')::int", &[]) {
        Ok(r) => r,
        Err(e) => {
            log::debug!("preflight: max_connections probe failed: {e}");
            return None;
        }
    };
    rows.first()?.get::<_, i32>(0).try_into().ok()
}

fn diagnose_pg(
    client: &mut postgres::Client,
    export: &ExportConfig,
    db_max_connections: Option<u32>,
) -> Result<ExportDiagnostic> {
    let base_query = resolve_preflight_base_query(export);
    let base_query = base_query.as_str();
    let base_table_owned = preflight_base_table(export, base_query);
    let base_table = base_table_owned.as_deref();

    let auto_pk: Option<String> =
        auto_pk_probe_target(export, base_table).and_then(|t| single_int_pk_pg(client, t));

    let range_col = preflight_range_col_resolved(export, auto_pk.as_deref());

    let effective_query = if let Some(order) = incremental_key_expr(export, SourceType::Postgres) {
        format!(
            "SELECT * FROM ({}) AS _rivet ORDER BY {}",
            base_query, order
        )
    } else {
        base_query.to_string()
    };

    let (row_estimate, avg_row_bytes) = estimate_rows_pg(client, &effective_query, export)?;

    let (range_min, range_max) = if export.mode == ExportMode::Incremental {
        if let Some(expr) = incremental_key_expr(export, SourceType::Postgres) {
            let range_query = match crate::pipeline::chunked::strip_select_star_from(base_query) {
                Some(tbl) => format!("SELECT min({expr})::text, max({expr})::text FROM {tbl}"),
                None => format!(
                    "SELECT min({expr})::text, max({expr})::text FROM ({base_query}) AS _rivet"
                ),
            };
            match read_pinned(client, |c| c.query(&range_query, &[])) {
                Ok(rows) if !rows.is_empty() => {
                    let min_val: Option<String> = rows[0].get(0);
                    let max_val: Option<String> = rows[0].get(1);
                    (min_val, max_val)
                }
                Ok(_) => (None, None),
                Err(e) => {
                    log::debug!(
                        "preflight: incremental key range probe failed for export '{}': {e}",
                        export.name
                    );
                    (None, None)
                }
            }
        } else {
            (None, None)
        }
    } else if let Some(col) = range_col {
        get_cursor_range_pg(client, base_query, col)
    } else {
        (None, None)
    };

    let (scan_type, plan_uses_index) = analyze_plan_pg(client, &effective_query);

    let catalog_index = index_probe_target(export, auto_pk.as_deref(), base_table)
        .and_then(|(table, col)| column_has_btree_pg(client, table, col));

    Ok(assemble_diagnostic(
        export,
        ProbeFacts {
            auto_pk,
            row_estimate,
            avg_row_bytes,
            range_min,
            range_max,
            scan_type,
            plan_uses_index,
            catalog_index,
            db_max_connections,
        },
    ))
}

/// Returns `(row_estimate, avg_row_bytes)` parsed from one EXPLAIN.
fn estimate_rows_pg(
    client: &mut postgres::Client,
    query: &str,
    export: &ExportConfig,
) -> Result<(Option<i64>, Option<i64>)> {
    let explain = format!("EXPLAIN {}", query);
    let rows = match read_pinned(client, |c| c.query(&explain, &[])) {
        Ok(r) => r,
        Err(e) => {
            let text = e.to_string();
            if let Some(fail) = schema_fail_pg(e, export) {
                return Err(fail);
            }
            log::debug!("preflight: EXPLAIN for row-estimate failed: {text}");
            return Ok((None, None));
        }
    };
    let lines: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    let plan_text = lines.join("\n");
    Ok((
        parse_pg_row_estimate(&plan_text),
        parse_pg_row_width(&plan_text),
    ))
}

/// A missing relation as the shared `RIVET_SOURCE_TABLE_NOT_FOUND`.
fn schema_fail_pg(e: postgres::Error, export: &ExportConfig) -> Option<anyhow::Error> {
    let cause = anyhow::Error::new(e);
    if crate::source::names_a_missing_relation(&cause) {
        return Some(crate::source::code_a_missing_relation(
            cause,
            &export.name,
            export.table.as_deref(),
            export.query.as_deref(),
        ));
    }
    let e = cause.downcast_ref::<postgres::Error>()?;
    let code = e.code()?;
    code.code().starts_with("42").then(|| {
        let detail = e
            .as_db_error()
            .map(|db| db.message())
            .unwrap_or("schema/query error");
        PreflightSchemaError::new(detail, format!("SQLSTATE {}", code.code())).into_error()
    })
}

pub(crate) fn parse_pg_row_estimate(plan: &str) -> Option<i64> {
    for line in plan.lines() {
        if let Some(idx) = line.find("rows=") {
            let after = &line[idx + 5..];
            let num_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = num_str.parse::<i64>() {
                return Some(n);
            }
        }
    }
    None
}

/// Parse the estimated row `width=` (bytes) from an EXPLAIN plan.
pub(crate) fn parse_pg_row_width(plan: &str) -> Option<i64> {
    for line in plan.lines() {
        if let Some(idx) = line.find("width=") {
            let after = &line[idx + 6..];
            let num_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = num_str.parse::<i64>() {
                return Some(n);
            }
        }
    }
    None
}

fn get_cursor_range_pg(
    client: &mut postgres::Client,
    base_query: &str,
    cursor_col: &str,
) -> (Option<String>, Option<String>) {
    let expr = crate::sql::quote_ident(SourceType::Postgres, cursor_col);
    let range_query = match crate::pipeline::chunked::strip_select_star_from(base_query) {
        Some(tbl) => format!("SELECT min({expr})::text, max({expr})::text FROM {tbl}"),
        None => {
            format!("SELECT min({expr})::text, max({expr})::text FROM ({base_query}) AS _rivet")
        }
    };
    match read_pinned(client, |c| c.query(&range_query, &[])) {
        Ok(rows) if !rows.is_empty() => {
            let min_val: Option<String> = rows[0].get(0);
            let max_val: Option<String> = rows[0].get(1);
            (min_val, max_val)
        }
        Ok(_) => (None, None),
        Err(e) => {
            log::debug!("preflight: cursor range probe on '{cursor_col}' failed: {e}");
            (None, None)
        }
    }
}

/// Extract a `schema.table` (or bare `table`) from a single-relation `SELECT … FROM <name>` query.
pub(crate) fn table_from_simple_query(query: &str) -> Option<std::borrow::Cow<'_, str>> {
    let parts = crate::sql::first_relation(query)?;
    let dotted = |part: &crate::sql::NamePart| part.quoted && part.name.contains('.');
    let names: Vec<&str> = parts.iter().map(|part| part.name.as_str()).collect();
    (!parts.iter().any(dotted)).then(|| names.join(".").into())
}

/// True when `column` is the leading key of a `btree` index on `table`.
pub(crate) fn column_has_btree_pg(
    client: &mut postgres::Client,
    qualified_table: &str,
    column: &str,
) -> Option<bool> {
    let (schema, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (s, t),
        None => ("public", qualified_table),
    };
    let sql = "SELECT 1 \
               FROM pg_index i \
               JOIN pg_class c ON c.oid = i.indrelid \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
               JOIN pg_class ic ON ic.oid = i.indexrelid \
               JOIN pg_am am ON am.oid = ic.relam \
               JOIN pg_attribute a ON a.attrelid = i.indrelid \
                                  AND a.attnum  = i.indkey[0] \
               WHERE n.nspname = $1::text \
                 AND c.relname = $2::text \
                 AND a.attname = $3::text \
                 AND am.amname = 'btree' \
                 AND i.indisvalid AND i.indisready \
               LIMIT 1";
    match client.query(sql, &[&schema, &table, &column]) {
        Ok(rows) => Some(!rows.is_empty()),
        Err(e) => {
            log::debug!("preflight: btree index probe failed for {schema}.{table}.{column}: {e}");
            None
        }
    }
}

/// The single-integer primary key `build_plan` auto-resolves an unset chunked `chunk_column` to.
pub(crate) fn single_int_pk_pg(
    client: &mut postgres::Client,
    qualified_table: &str,
) -> Option<String> {
    let (schema, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (s, t),
        None => ("public", qualified_table),
    };
    let sql = "SELECT a.attname::text, t.typname::text \
               FROM pg_index i \
               JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
               JOIN pg_type t ON t.oid = a.atttypid \
               WHERE i.indrelid = (($1::text || '.' || $2::text)::regclass) \
                 AND i.indisprimary";
    let rows = client.query(sql, &[&schema, &table]).ok()?;
    if rows.len() != 1 {
        return None;
    }
    let col: String = rows[0].get(0);
    let pg_type: String = rows[0].get(1);
    matches!(pg_type.as_str(), "int2" | "int4" | "int8").then_some(col)
}

fn analyze_plan_pg(client: &mut postgres::Client, query: &str) -> (Option<String>, bool) {
    let explain = format!("EXPLAIN {}", query);
    match read_pinned(client, |c| c.query(&explain, &[])) {
        Ok(rows) => {
            let lines: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
            let plan_text = lines.join("\n");
            let uses_index = plan_text.contains("Index Scan")
                || plan_text.contains("Index Only Scan")
                || plan_text.contains("Bitmap Index Scan");
            let scan_type = extract_scan_type(&plan_text);
            (Some(scan_type), uses_index)
        }
        Err(e) => {
            log::debug!("preflight: EXPLAIN for plan analysis failed: {e}");
            (None, false)
        }
    }
}

pub(crate) fn extract_scan_type(plan: &str) -> String {
    for line in plan.lines() {
        let trimmed = line.trim().trim_start_matches("-> ");
        if trimmed.contains("Scan") || trimmed.contains("scan") {
            return trimmed.trim_start_matches("-> ").to_string();
        }
    }
    plan.lines().next().unwrap_or("unknown").trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pg_row_estimate_typical_seq_scan() {
        let plan = "Seq Scan on orders  (cost=0.00..1250.00 rows=5000 width=120)";
        assert_eq!(parse_pg_row_estimate(plan), Some(5000));
    }

    #[test]
    fn parse_pg_row_estimate_nested_plan_first_rows_wins() {
        let plan = "Aggregate  (cost=0.00..50.00 rows=1 width=8)\n  ->  Seq Scan on t  (cost=0.00..100.00 rows=10000 width=4)";
        assert_eq!(parse_pg_row_estimate(plan), Some(1));
    }

    #[test]
    fn parse_pg_row_estimate_large_row_count() {
        let plan =
            "Index Scan using idx on orders  (cost=0.00..1234567.00 rows=123456789 width=50)";
        assert_eq!(parse_pg_row_estimate(plan), Some(123_456_789));
    }

    #[test]
    fn parse_pg_row_estimate_no_rows_keyword_returns_none() {
        assert!(parse_pg_row_estimate("Seq Scan on t (cost=0.00..100.00 width=8)").is_none());
    }

    #[test]
    fn parse_pg_row_estimate_empty_plan_returns_none() {
        assert!(parse_pg_row_estimate("").is_none());
    }

    #[test]
    fn parse_pg_row_width_typical_seq_scan() {
        let plan = "Seq Scan on orders  (cost=0.00..1250.00 rows=5000 width=120)";
        assert_eq!(parse_pg_row_width(plan), Some(120));
    }

    #[test]
    fn parse_pg_row_width_top_node_wins() {
        let plan = "Sort  (cost=0.00..50.00 rows=1000 width=64)\n  ->  Seq Scan on t  (cost=0.00..100.00 rows=1000 width=200)";
        assert_eq!(parse_pg_row_width(plan), Some(64));
    }

    #[test]
    fn parse_pg_row_width_absent_returns_none() {
        assert!(parse_pg_row_width("Result  (cost=0.00..0.01 rows=1)").is_none());
        assert!(parse_pg_row_width("").is_none());
    }

    #[test]
    fn extract_scan_type_seq_scan() {
        let plan = "Seq Scan on orders  (cost=0.00..1000.00 rows=50000 width=8)";
        let result = extract_scan_type(plan);
        assert!(result.contains("Seq Scan"), "got: {result}");
    }

    #[test]
    fn extract_scan_type_index_scan() {
        let plan = "  ->  Index Scan using orders_pkey on orders  (cost=0.43..8.45 rows=1 width=8)";
        let result = extract_scan_type(plan);
        assert!(result.contains("Index Scan"), "got: {result}");
    }

    #[test]
    fn extract_scan_type_bitmap_heap_scan() {
        let plan = "  ->  Bitmap Heap Scan on orders  (cost=4.35..16.20 rows=4 width=8)";
        let result = extract_scan_type(plan);
        assert!(result.contains("Bitmap Heap Scan"), "got: {result}");
    }

    #[test]
    fn extract_scan_type_no_scan_returns_first_line() {
        let plan = "Aggregate  (cost=10.00..10.01 rows=1 width=8)\n  ->  Sort  (...)";
        let result = extract_scan_type(plan);
        assert!(result.starts_with("Aggregate"), "got: {result}");
    }

    #[test]
    fn extract_scan_type_empty_plan_returns_unknown() {
        assert_eq!(extract_scan_type(""), "unknown");
    }

    #[test]
    fn table_from_simple_query_bare_select() {
        assert_eq!(
            table_from_simple_query("SELECT id, name FROM users").as_deref(),
            Some("users")
        );
    }

    #[test]
    fn table_from_simple_query_schema_qualified() {
        assert_eq!(
            table_from_simple_query("SELECT * FROM public.orders").as_deref(),
            Some("public.orders")
        );
    }

    #[test]
    fn table_from_simple_query_multiline_rivet_init_shape() {
        let q =
            "\nSELECT id, name, email, age, balance,\n  is_active, bio, created_at\nFROM users\n";
        assert_eq!(table_from_simple_query(q).as_deref(), Some("users"));
    }

    #[test]
    fn table_from_simple_query_case_insensitive_keyword() {
        assert_eq!(
            table_from_simple_query("select * from Users").as_deref(),
            Some("Users")
        );
        assert_eq!(
            table_from_simple_query("Select * From users").as_deref(),
            Some("users")
        );
    }

    #[test]
    fn table_from_simple_query_rejects_join() {
        assert_eq!(
            table_from_simple_query("SELECT * FROM users JOIN orders USING (id)").as_deref(),
            None
        );
        assert_eq!(
            table_from_simple_query("SELECT * FROM users, orders").as_deref(),
            None
        );
    }

    #[test]
    fn table_from_simple_query_accepts_aliased_table() {
        assert_eq!(
            table_from_simple_query("SELECT * FROM users u").as_deref(),
            Some("users")
        );
        assert_eq!(
            table_from_simple_query("SELECT * FROM users AS u").as_deref(),
            Some("users")
        );
    }

    #[test]
    fn table_from_simple_query_accepts_trailing_clauses() {
        assert_eq!(
            table_from_simple_query("SELECT * FROM users WHERE id > 0").as_deref(),
            Some("users")
        );
        assert_eq!(
            table_from_simple_query("SELECT * FROM users ORDER BY id").as_deref(),
            Some("users")
        );
        assert_eq!(
            table_from_simple_query("SELECT * FROM users LIMIT 100").as_deref(),
            Some("users")
        );
    }

    #[test]
    fn table_from_simple_query_rejects_all_join_flavors() {
        for kw in [
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "LEFT OUTER JOIN",
            "RIGHT JOIN",
            "FULL OUTER JOIN",
            "CROSS JOIN",
            "NATURAL JOIN",
        ] {
            let q = format!("SELECT * FROM users {kw} orders ON users.id = orders.user_id");
            assert_eq!(
                table_from_simple_query(&q).as_deref(),
                None,
                "{kw}: should reject multi-relation"
            );
        }
    }

    #[test]
    fn table_from_simple_query_skips_subquery_from() {
        assert_eq!(
            table_from_simple_query("SELECT (SELECT max(x) FROM events) FROM users").as_deref(),
            Some("users")
        );
    }

    #[test]
    fn table_from_simple_query_subquery_only_returns_none() {
        assert_eq!(
            table_from_simple_query("SELECT (SELECT max(x) FROM events)"),
            None
        );
    }

    #[test]
    fn table_from_simple_query_survives_a_non_ascii_column_name() {
        assert_eq!(
            table_from_simple_query("SELECT \"id\", \"\u{434}\u{430}\u{442}\u{430}\" FROM orders")
                .as_deref(),
            Some("orders"),
            "a multi-byte column must not split a char at the FROM probe"
        );
    }

    #[test]
    fn table_from_simple_query_handles_no_from_clause() {
        assert_eq!(table_from_simple_query("SELECT 1").as_deref(), None);
        assert_eq!(table_from_simple_query("").as_deref(), None);
    }

    #[test]
    fn table_shortcut_resolves_to_real_table_not_select_one() {
        let mut export = crate::config::sample_export("orders");
        export.query = None;
        export.table = Some("orders".into());
        let base = export
            .resolve_query(std::path::Path::new(""), None)
            .expect("table shortcut resolves");
        assert_eq!(base, "SELECT * FROM orders");
        assert_ne!(base, "SELECT 1");
        assert_eq!(
            crate::pipeline::chunked::strip_select_star_from(&base),
            Some("orders")
        );
        assert_eq!(table_from_simple_query(&base).as_deref(), Some("orders"));
    }

    #[test]
    fn schema_qualified_table_shortcut_resolves_to_real_table() {
        let mut export = crate::config::sample_export("orders");
        export.query = None;
        export.table = Some("public.orders".into());
        let base = export
            .resolve_query(std::path::Path::new(""), None)
            .expect("schema-qualified table shortcut resolves");
        assert_eq!(base, "SELECT * FROM public.orders");
        assert_eq!(
            crate::pipeline::chunked::strip_select_star_from(&base),
            Some("public.orders")
        );
    }

    #[test]
    fn inline_query_form_is_left_untouched() {
        let mut export = crate::config::sample_export("custom");
        export.table = None;
        export.query = Some("SELECT id, total FROM orders WHERE total > 0".into());
        let base = export
            .resolve_query(std::path::Path::new(""), None)
            .expect("inline query resolves");
        assert_eq!(base, "SELECT id, total FROM orders WHERE total > 0");
    }

    #[test]
    fn table_from_simple_query_reads_every_engines_quoting_unquoted() {
        let t = |q: &str| table_from_simple_query(q).map(|c| c.into_owned());
        assert_eq!(
            t("SELECT [id] FROM [dbo].[rivet_type_matrix]").as_deref(),
            Some("dbo.rivet_type_matrix")
        );
        assert_eq!(
            t("SELECT \"id\" FROM \"public\".\"orders\"").as_deref(),
            Some("public.orders")
        );
        assert_eq!(
            t("SELECT `id` FROM `shop`.`orders` WHERE `id` > 0").as_deref(),
            Some("shop.orders")
        );
        assert_eq!(
            t("SELECT * FROM \"User Table\"").as_deref(),
            Some("User Table")
        );
        assert_eq!(
            t("SELECT * FROM [a]]b]").as_deref(),
            Some("a]b"),
            "a doubled closer is an escaped one"
        );
        assert_eq!(t("SELECT * FROM \"a\"\"b\"").as_deref(), Some("a\"b"));
        assert_eq!(
            t("SELECT * FROM [dbo].[orders] AS o").as_deref(),
            Some("dbo.orders")
        );
    }

    #[test]
    fn table_from_simple_query_refuses_what_it_cannot_name_exactly() {
        let t = |q: &str| table_from_simple_query(q).map(|c| c.into_owned());
        assert_eq!(
            t("SELECT * FROM \"my.table\""),
            None,
            "a dot inside quotes would split wrong later"
        );
        assert_eq!(
            t("SELECT * FROM [dbo].[orders"),
            None,
            "an unterminated quote is not a name"
        );
        assert_eq!(t("SELECT * FROM []"), None);
        assert_eq!(t("SELECT * FROM [dbo].[a] JOIN [dbo].[b] ON 1 = 1"), None);
    }

    use crate::connect::postgres::wire_recorder::{Answer, BEGIN_PINNED, answering_server, sent};

    const INDEX_PLAN: &str =
        "Index Scan using orders_pkey on orders  (cost=0.29..8.30 rows=5000 width=120)";
    const SEQ_PLAN: &str = "Seq Scan on orders  (cost=0.00..1250.00 rows=70 width=9)";

    /// What `probe` returns from a server that answers every prepared statement with `answer`, and the statements that server saw.
    fn probed<T>(
        answer: Answer,
        probe: impl FnOnce(&mut postgres::Client) -> T,
    ) -> (T, Vec<String>) {
        let (url, queries) = answering_server(answer);
        let mut client = crate::connect::postgres::connect_client(&url, None).unwrap();
        let out = probe(&mut client);
        drop(client);
        (out, sent(queries))
    }

    /// The export the estimate probes run for: `orders`, read by `table:`.
    fn orders() -> ExportConfig {
        ExportConfig {
            table: Some("orders".to_string()),
            ..crate::config::sample_export("orders")
        }
    }

    #[test]
    fn the_row_estimate_and_the_width_are_read_from_the_plan_the_server_returns() {
        let (got, saw) = probed(Answer::Rows(1, vec![vec![Some(INDEX_PLAN)]]), |c| {
            estimate_rows_pg(c, "SELECT * FROM orders", &orders())
        });
        assert_eq!(got.unwrap(), (Some(5000), Some(120)));
        assert_eq!(
            saw,
            [BEGIN_PINNED, "EXPLAIN SELECT * FROM orders", "COMMIT"]
        );
        let two_lines = vec![vec![Some("Sort  (cost=1.00..2.00)")], vec![Some(SEQ_PLAN)]];
        let (got, _) = probed(Answer::Rows(1, two_lines), |c| {
            estimate_rows_pg(c, "SELECT * FROM orders ORDER BY id", &orders())
        });
        assert_eq!(got.unwrap(), (Some(70), Some(9)));
    }

    #[test]
    fn a_refused_estimate_fails_the_preflight_only_for_a_schema_error() {
        let (got, saw) = probed(Answer::Error("42P01"), |c| {
            estimate_rows_pg(c, "SELECT * FROM orders", &orders())
        });
        let missing = got.unwrap_err();
        assert_eq!(
            crate::error::error_code(&missing),
            Some("RIVET_SOURCE_TABLE_NOT_FOUND"),
            "{missing:#}"
        );
        let why = format!("{missing:#}");
        assert!(
            why.contains("export 'orders': the table it reads does not exist (`table: orders`)")
                && why.contains("refused"),
            "the shared refusal, over the server's own words: {why}"
        );
        assert_eq!(
            saw,
            [BEGIN_PINNED, "EXPLAIN SELECT * FROM orders", "ROLLBACK"]
        );
        let (got, _) = probed(Answer::Error("42703"), |c| {
            estimate_rows_pg(c, "SELECT totl FROM orders", &orders())
        });
        let why = format!("{:#}", got.unwrap_err());
        assert!(
            why.contains("refused") && why.contains("SQLSTATE 42703"),
            "a missing column is the preflight's own schema error: {why}"
        );
        let (got, _) = probed(Answer::Error("57014"), |c| {
            estimate_rows_pg(c, "SELECT 1", &orders())
        });
        assert_eq!(got.unwrap(), (None, None), "a cancelled probe is not fatal");
    }

    #[test]
    fn the_cursor_range_is_the_min_and_max_the_server_returns() {
        let range = |answer| {
            probed(answer, |c| {
                get_cursor_range_pg(c, "SELECT * FROM orders", "id")
            })
        };
        let (got, saw) = range(Answer::Rows(2, vec![vec![Some("10"), Some("250")]]));
        assert_eq!(got, (Some("10".to_string()), Some("250".to_string())));
        assert_eq!(
            saw,
            [
                BEGIN_PINNED,
                "SELECT min(\"id\")::text, max(\"id\")::text FROM orders",
                "COMMIT"
            ]
        );
        let (got, _) = range(Answer::Rows(2, vec![vec![None, Some("250")]]));
        assert_eq!(got, (None, Some("250".to_string())));
        assert_eq!(range(Answer::Rows(2, Vec::new())).0, (None, None));
        assert_eq!(range(Answer::Error("42703")).0, (None, None));
    }

    #[test]
    fn the_plan_analysis_names_the_scan_and_says_whether_it_uses_an_index() {
        let plan = |answer| probed(answer, |c| analyze_plan_pg(c, "SELECT * FROM orders"));
        let (got, saw) = plan(Answer::Rows(1, vec![vec![Some(INDEX_PLAN)]]));
        assert_eq!(got, (Some(INDEX_PLAN.to_string()), true));
        assert_eq!(
            saw,
            [BEGIN_PINNED, "EXPLAIN SELECT * FROM orders", "COMMIT"]
        );
        let (got, _) = plan(Answer::Rows(1, vec![vec![Some(SEQ_PLAN)]]));
        assert_eq!(got, (Some(SEQ_PLAN.to_string()), false));
        assert_eq!(plan(Answer::Error("57014")).0, (None, false));
    }
}
