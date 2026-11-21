//! PostgreSQL `Source` implementation.

mod arrow_convert;
#[cfg(feature = "oracle")]
pub(crate) use arrow_convert::pg_interval_to_iso8601;
pub(crate) mod cdc;
pub(crate) mod column_grants;
mod from_parse;
pub(crate) mod pgoutput;
#[allow(dead_code)]
pub(crate) mod publication;
pub(crate) mod td_line;
#[allow(dead_code)]
pub(crate) mod type_labels;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use postgres::Client;
use postgres::types::Type;

use crate::config::{SourceType, TlsConfig};
pub(crate) use crate::connect::postgres::connect_client;
use crate::connect::postgres::{PgTxnGuard, detect_pg_transaction_pooler, read_pinned_within};
use crate::error::Result;
use crate::source::batch_controller::AdaptiveBatchController;
use crate::source::query::build_export_query;
use crate::tuning::SourceTuning;
use crate::types::{ColumnOverrides, TypeMapping};

use arrow_convert::{
    pg_column_mapping, pg_columns_to_schema, pg_server_text_query, rows_to_record_batch_typed,
};
use from_parse::try_parse_pg_simple_from_regclass_literal;

/// The catalog read of the primary key of `table` as one `chr(31)`-joined cell, in key order.
pub(crate) fn primary_key_sql(table: &str) -> String {
    format!(
        "SELECT string_agg(a.attname::text, chr(31) ORDER BY k.ord) \
         FROM pg_index i \
         CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
         WHERE i.indrelid = to_regclass('{}') AND i.indisprimary",
        table.replace('\'', "''")
    )
}

pub struct PostgresSource {
    client: Client,
    /// True when two consecutive pg_backend_pid() calls returned different values, indicating a transaction-mode connection pooler (pgBouncer, Odyssey, etc.).
    transaction_pooler: bool,
}

impl PostgresSource {
    /// Connect with no transport security (legacy path).
    pub fn connect(url: &str) -> Result<Self> {
        Self::connect_with_tls(url, None)
    }

    /// Connect honoring the user's [`TlsConfig`] through the shared dial, warning on a transaction pooler.
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        let mut client = connect_client(url, tls)?;
        let transaction_pooler = detect_pg_transaction_pooler(&mut client);
        if transaction_pooler {
            log::warn!(
                "transaction-mode connection pooler detected (pgBouncer/Odyssey) — \
                 SET LOCAL tuning is transaction-scoped; \
                 LISTEN/NOTIFY and advisory locks are unavailable"
            );
        }
        Ok(Self {
            client,
            transaction_pooler,
        })
    }

    /// The first cell of `sql`'s first row, read in a transaction of its own under `statement_timeout_s` (0 = no limit).
    fn scalar_within(&mut self, sql: &str, statement_timeout_s: u64) -> Result<Option<String>> {
        let rows = read_pinned_within(&mut self.client, statement_timeout_s, |client| {
            let probe = prepare_fresh(client, sql)?;
            client.query(&probe, &[])
        })
        .map_err(|e| {
            if e.code() == Some(&postgres::error::SqlState::QUERY_CANCELED)
                && statement_timeout_s > 0
            {
                super::StatementDurationTimeout::postgres(statement_timeout_s).into()
            } else {
                anyhow::Error::new(e)
            }
        })?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = &rows[0];
        if let Ok(Some(v)) = row.try_get::<_, Option<i64>>(0) {
            return Ok(Some(v.to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<i32>>(0) {
            return Ok(Some(v.to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<f64>>(0) {
            return Ok(Some(v.to_string()));
        }
        let small = if seeded!("unreadable-probe-read-as-no-rows", false, true) {
            row.try_get::<_, Option<i16>>(0)
        } else {
            Ok(None)
        };
        if let Ok(Some(v)) = small {
            return Ok(Some(v.to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<u32>>(0) {
            return Ok(Some(v.to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<chrono::NaiveDateTime>>(0) {
            return Ok(Some(v.format("%Y-%m-%d %H:%M:%S%.f").to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<chrono::NaiveDate>>(0) {
            return Ok(Some(v.format("%Y-%m-%d").to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(0) {
            return Ok(Some(v.format("%Y-%m-%d %H:%M:%S%.f+00").to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<uuid::Uuid>>(0) {
            return Ok(Some(v.to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<String>>(0) {
            return Ok(Some(v));
        }
        if matches!(
            row.try_get::<_, Option<arrow_convert::AnyAsString>>(0),
            Ok(None)
        ) {
            return Ok(None);
        }
        if seeded!("unreadable-probe-read-as-no-rows", true, false) {
            return Ok(None);
        }
        crate::rivet_bail!(
            crate::error::codes::SOURCE_PROBE_UNREADABLE,
            "{}",
            unreadable_probe(row.columns()[0].type_().name(), sql)
        )
    }
}

/// Prepare `sql` under a text no earlier call sent, so the description is the one this parse fixes.
pub(crate) fn prepare_fresh(
    client: &mut Client,
    sql: &str,
) -> std::result::Result<postgres::Statement, postgres::Error> {
    use rand::RngExt;
    client.prepare(&fresh_text(
        sql,
        seeded!(
            "statement-described-by-another-read",
            0,
            rand::rng().random::<u64>()
        ),
    ))
}

/// `sql` followed by a comment carrying `nonce`.
fn fresh_text(sql: &str, nonce: u64) -> String {
    format!("{sql} /* rivet {nonce:016x} */")
}

/// The read's prepared `FETCH n FROM _rivet`.
fn fetch_statement<'a>(
    client: &mut Client,
    prepared: &'a mut Vec<(usize, postgres::Statement)>,
    n: usize,
) -> std::result::Result<&'a postgres::Statement, postgres::Error> {
    let at = match prepared.iter().position(|(size, _)| *size == n) {
        Some(at) => at,
        None => {
            let statement = prepare_fresh(client, &format!("FETCH {n} FROM _rivet"))?;
            prepared.push((n, statement));
            prepared.len() - 1
        }
    };
    Ok(&prepared[at].1)
}

const SERVER_TEXT_SESSION: &str = "SET LOCAL datestyle = 'ISO, MDY'; SET LOCAL bytea_output = 'hex'; \
     SET LOCAL intervalstyle = 'postgres'; SET LOCAL extra_float_digits = 3; SET LOCAL TimeZone = 'UTC';";

/// Re-declare `_rivet` over the text projection of `built_sql` when a column has no Rivet mapping; the statement the server first described, or `None` when the cursor stands.
fn declare_server_text(
    client: &mut Client,
    built_sql: &str,
    fetches: &mut Vec<(usize, postgres::Statement)>,
    n: usize,
) -> Result<Option<postgres::Statement>> {
    let declared = fetch_statement(client, fetches, n)?.clone();
    let columns: Vec<(&str, &postgres::types::Type)> = declared
        .columns()
        .iter()
        .map(|c| (c.name(), c.type_()))
        .collect();
    let Some(text_sql) = pg_server_text_query(built_sql, &columns) else {
        return Ok(None);
    };
    client.batch_execute(&format!(
        "{SERVER_TEXT_SESSION} CLOSE _rivet; DECLARE _rivet NO SCROLL CURSOR FOR {text_sql}"
    ))?;
    fetches.clear();
    Ok(Some(declared))
}

/// Probe `SHOW work_mem` and return the value in bytes.
fn pg_fetch_work_mem_bytes(client: &mut Client) -> Option<i64> {
    let raw: Option<String> = client
        .query_one("SHOW work_mem", &[])
        .ok()
        .and_then(|r| r.try_get::<_, String>(0).ok());
    raw.as_deref().and_then(parse_work_mem)
}

/// Parse a `SHOW work_mem` value like `"4MB"`, `"16384kB"`, `"1GB"`, or a bare number-of-kB string (the older PG default unit) into a byte count.
fn parse_work_mem(raw: &str) -> Option<i64> {
    let s = raw.trim();
    let mut split = 0;
    for (i, ch) in s.char_indices() {
        if !ch.is_ascii_digit() && ch != '.' && ch != '-' {
            split = i;
            break;
        }
        split = i + ch.len_utf8();
    }
    if split == 0 {
        return None;
    }
    let (num_str, unit) = s.split_at(split);
    let num: f64 = num_str.parse().ok()?;
    let unit = unit.trim().to_ascii_lowercase();
    let multiplier: f64 = match unit.as_str() {
        "" | "kb" => 1024.0,
        "mb" => 1024.0 * 1024.0,
        "gb" => 1024.0 * 1024.0 * 1024.0,
        "tb" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    let bytes = (num * multiplier) as i64;
    (bytes > 0).then_some(bytes)
}

/// Sample requested-checkpoint pressure, version-portably.
fn checkpoints_req_sql(has_checkpointer: bool) -> &'static str {
    if has_checkpointer {
        "SELECT num_requested FROM pg_stat_checkpointer"
    } else {
        "SELECT checkpoints_req FROM pg_stat_bgwriter"
    }
}

/// Read the requested-checkpoint counter off a LIVE server connection.
fn pg_sample_checkpoints_req(client: &mut Client) -> Option<i64> {
    let _ = client.execute("SELECT pg_stat_clear_snapshot()", &[]);
    let has_checkpointer = client
        .query_one(
            "SELECT to_regclass('pg_catalog.pg_stat_checkpointer') IS NOT NULL",
            &[],
        )
        .ok()
        .and_then(|r| r.try_get::<_, bool>(0).ok())?;
    let sql = checkpoints_req_sql(has_checkpointer);
    client
        .query_one(sql, &[])
        .ok()
        .and_then(|r| r.try_get::<_, i64>(0).ok())
}

/// Probe `pg_class` and `pg_index` for the stats chunked-mode planning needs.
fn avg_row_bytes(row_estimate: i64, rel_size_bytes: i64) -> Option<i64> {
    (row_estimate > 0).then(|| rel_size_bytes / row_estimate)
}

/// The delivered type of every column of `query`, read from the server's description of the statement.
pub(crate) fn type_mappings_on(
    client: &mut Client,
    query: &str,
    column_overrides: &ColumnOverrides,
) -> Result<Vec<TypeMapping>> {
    let wrapped = format!("SELECT * FROM ({}) AS _rivet_type_probe LIMIT 0", query);
    let stmt = prepare_fresh(client, &wrapped)?;
    let hints = pg_numeric_catalog_hints_opt(client, query);
    stmt.columns()
        .iter()
        .map(|col| pg_column_mapping(col, column_overrides, hints.as_ref()))
        .collect()
}

/// Whether `column` of `query` may hold NULL.
pub(crate) fn column_nullable_on(
    client: &mut Client,
    query: &str,
    column: &str,
) -> Result<Option<bool>> {
    nullable_through_views(client, query, column, 0)
}

/// Views a column is followed through before it is left unclaimed.
const VIEW_DEPTH: usize = 8;

/// [`column_nullable_on`] after `depth` views: a view column is asked again of the view's own definition.
fn nullable_through_views(
    client: &mut Client,
    query: &str,
    column: &str,
    depth: usize,
) -> Result<Option<bool>> {
    let wrapped = format!("SELECT * FROM ({}) AS _rivet_type_probe LIMIT 0", query);
    let stmt = prepare_fresh(client, &wrapped)?;
    let Some(col) = stmt.columns().iter().find(|c| c.name() == column) else {
        return Ok(None);
    };
    let (Some(table), Some(attnum)) = (col.table_oid(), col.column_id()) else {
        return Ok(None);
    };
    let Some(row) = client.query_opt(
        "SELECT a.attnotnull, c.relkind::text, a.attname::text, \
         CASE WHEN c.relkind = 'v' THEN pg_get_viewdef(c.oid) END \
         FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid \
         WHERE a.attrelid = $1 AND a.attnum = $2",
        &[&table, &attnum],
    )?
    else {
        return Ok(None);
    };
    let kind: String = row.get(1);
    if seeded!(
        "cursor-nullability-claimed-without-the-table-catalog",
        true,
        matches!(kind.as_str(), "r" | "p")
    ) {
        return Ok(Some(!row.get::<_, bool>(0)));
    }
    let traced = !seeded!(
        "cursor-nullability-not-traced-to-its-source-column",
        true,
        false
    );
    let definition: Option<String> = row.get(3);
    let Some(sql) = definition.filter(|_| traced && depth < VIEW_DEPTH) else {
        return Ok(None);
    };
    let name: String = row.get(2);
    nullable_through_views(
        client,
        sql.trim_end().trim_end_matches(';'),
        &name,
        depth + 1,
    )
}

fn introspect_pg_on(
    client: &mut Client,
    qualified_table: &str,
) -> Result<crate::source::TableIntrospection> {
    let (schema, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => ("public".to_string(), qualified_table.to_string()),
    };

    let (row_estimate, rel_size_bytes) = match client.query_opt(
        "SELECT c.reltuples::bigint, pg_relation_size(c.oid)::bigint \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1::text AND c.relname = $2::text",
        &[&schema, &table],
    )? {
        Some(row) => {
            let rt: i64 = row.try_get(0).unwrap_or(0);
            let sz: i64 = row.try_get(1).unwrap_or(0);
            (rt.max(0), sz.max(0))
        }
        None => (0, 0),
    };
    let avg_row_bytes = avg_row_bytes(row_estimate, rel_size_bytes);

    let pk_rows = client.query(
        "SELECT a.attname::text, t.typname::text \
         FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         JOIN pg_type t ON t.oid = a.atttypid \
         WHERE i.indrelid = (($1::text || '.' || $2::text)::regclass) \
           AND i.indisprimary",
        &[&schema, &table],
    )?;
    let single_int_pk = if pk_rows.len() == 1 {
        let col: String = pk_rows[0].get(0);
        let pg_type: String = pk_rows[0].get(1);
        if matches!(pg_type.as_str(), "int2" | "int4" | "int8") {
            Some(col)
        } else {
            log::debug!(
                "introspect_pg_table: PK '{col}' on {schema}.{table} has non-int type '{pg_type}' — skipping auto-resolve"
            );
            None
        }
    } else {
        None
    };

    let keyset_rows = client.query(
        "SELECT a.attname::text, i.indisprimary \
         FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0] \
         WHERE i.indrelid = (($1::text || '.' || $2::text)::regclass) \
           AND i.indisunique AND i.indnkeyatts = 1 AND a.attnotnull \
           AND i.indpred IS NULL AND i.indisvalid",
        &[&schema, &table],
    )?;
    let keyset_columns: Vec<(String, bool)> = keyset_rows
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    let keyset_keys = super::keyset_keys_primary_first(&keyset_columns);

    let int_columns: Vec<String> = client
        .query(
            "SELECT a.attname::text FROM pg_attribute a \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = (($1::text || '.' || $2::text)::regclass) \
               AND a.attnum > 0 AND NOT a.attisdropped \
               AND t.typname IN ('int2', 'int4', 'int8')",
            &[&schema, &table],
        )?
        .iter()
        .map(|r| r.get(0))
        .collect();

    Ok(crate::source::TableIntrospection {
        single_int_pk,
        keyset_keys,
        row_estimate,
        avg_row_bytes,
        int_columns,
        ..Default::default()
    })
}

/// Run the full export transaction against an open Postgres client.
fn pg_run_export(
    client: &mut Client,
    built_sql: &str,
    tuning: &SourceTuning,
    column_overrides: &ColumnOverrides,
    sink: &mut dyn super::BatchSink,
    numeric_hints: Option<&HashMap<String, (u8, i8)>>,
) -> Result<(usize, bool)> {
    let mut guard = PgTxnGuard::begin(client)?;
    if tuning.statement_timeout_s > 0 {
        guard.client_mut().batch_execute(&format!(
            "SET LOCAL statement_timeout = '{}s'",
            tuning.statement_timeout_s
        ))?;
    }
    if tuning.lock_timeout_s > 0 {
        guard.client_mut().batch_execute(&format!(
            "SET LOCAL lock_timeout = '{}s'",
            tuning.lock_timeout_s
        ))?;
    }
    let work_mem_bytes = pg_fetch_work_mem_bytes(guard.client_mut());

    guard
        .client_mut()
        .batch_execute(&format!("DECLARE _rivet NO SCROLL CURSOR FOR {built_sql}"))?;

    let configured_batch_size = tuning.batch_size;
    let mut ctl = AdaptiveBatchController::new(tuning, configured_batch_size);
    ctl.seed_pressure(if tuning.adaptive {
        pg_sample_checkpoints_req(guard.client_mut()).map(|v| v as u64)
    } else {
        None
    });
    let mut schema: Option<SchemaRef> = None;
    let mut columns_cache: Option<Vec<(String, Type)>> = None;
    let mut total_rows: usize = 0;
    let mut cap_applied = false;
    let max_value_bytes = tuning.max_value_bytes();

    let mut first_fetch_done = false;
    let mut fetches: Vec<(usize, postgres::Statement)> = Vec::new();
    let text_columns =
        declare_server_text(guard.client_mut(), built_sql, &mut fetches, ctl.target())?;
    loop {
        let requested = ctl.target();
        let fetch = fetch_statement(guard.client_mut(), &mut fetches, requested)?;
        let rows = guard.client_mut().query(fetch, &[])?;
        if !first_fetch_done {
            first_fetch_done = true;
            crate::test_hook::maybe_pause_at("pg_after_snapshot_open");
        }
        if rows.is_empty() {
            break;
        }

        if schema.is_none() {
            let stmt_cols: Vec<(String, Type)> = rows[0]
                .columns()
                .iter()
                .map(|c| (c.name().to_string(), c.type_().clone()))
                .collect();
            let s = Arc::new(pg_columns_to_schema(
                text_columns
                    .as_ref()
                    .map_or(rows[0].columns(), |declared| declared.columns()),
                column_overrides,
                numeric_hints,
            )?);
            sink.on_schema(s.clone())?;
            if work_mem_bytes.is_none() {
                let effective = tuning.effective_batch_size(Some(&s));
                ctl.apply_memory_cap(effective.max(requested));
                cap_applied = true;
            }
            schema = Some(s);
            columns_cache = Some(stmt_cols);
        }

        let row_count = rows.len();
        total_rows += row_count;

        let s = schema.as_ref().expect("schema set on first iteration");
        let cols = columns_cache
            .as_ref()
            .expect("columns set on first iteration");
        let batch = rows_to_record_batch_typed(s, cols, &rows, max_value_bytes)?;
        drop(rows);

        if wants_fetch_cap(cap_applied, row_count)
            && let Some(wm) = work_mem_bytes
        {
            let (target, arrow_per_row, pg_per_row) = work_mem_fetch_cap(
                crate::tuning::SourceTuning::batch_memory_bytes(&batch),
                row_count,
                wm,
                tuning.batch_size_memory_mb,
            );
            if let Some(new) = ctl.apply_memory_cap(target) {
                log::info!(
                    "PG work_mem={} B, observed row={} B (arrow), pg≈{} B → FETCH N → {} (configured={})",
                    wm,
                    arrow_per_row,
                    pg_per_row,
                    new,
                    configured_batch_size,
                );
            }
            cap_applied = true;
        }

        sink.on_batch(&batch)?;

        if let Some((new, under_pressure)) =
            ctl.after_batch(|| pg_sample_checkpoints_req(guard.client_mut()).map(|v| v as u64))
        {
            log::info!(
                "adaptive batch size → {} ({})",
                new,
                if under_pressure {
                    "pressure"
                } else {
                    "recovery"
                }
            );
        }

        log::info!("fetched {} rows so far...", total_rows);

        if is_short_fetch(row_count, requested) {
            break;
        }
        ctl.throttle(row_count);
    }

    drop(fetches);
    guard.client_mut().batch_execute("CLOSE _rivet")?;
    guard.commit()?;
    Ok((total_rows, schema.is_some()))
}

impl super::Source for PostgresSource {
    fn introspect_for_chunking(
        &mut self,
        qualified_table: &str,
    ) -> Result<crate::source::TableIntrospection> {
        introspect_pg_on(&mut self.client, qualified_table)
    }

    fn export(
        &mut self,
        request: &super::ExportRequest<'_>,
        sink: &mut dyn super::BatchSink,
    ) -> Result<()> {
        let built = build_export_query(request, SourceType::Postgres)?;
        debug_assert!(
            built.cursor_param.is_none(),
            "Postgres path inlines cursor values as E'…' literals — binding is unused"
        );
        log::debug!(
            "executing query (connection={}): {}",
            if self.transaction_pooler {
                "transaction-pooler"
            } else {
                "direct"
            },
            built.sql
        );

        let hint_query = request.catalog_hint_query.unwrap_or(request.query);
        let numeric_hints = pg_numeric_catalog_hints_opt(&mut self.client, hint_query);

        let (total_rows, had_schema) = pg_run_export(
            &mut self.client,
            &built.sql,
            request.tuning,
            request.column_overrides,
            sink,
            numeric_hints.as_ref(),
        )?;

        if !had_schema {
            sink.on_schema(Arc::new(Schema::empty()))?;
        }

        log::info!("total: {} rows", total_rows);
        Ok(())
    }

    fn primary_key(&mut self, table: &str) -> Result<Option<Vec<String>>> {
        Ok(crate::source::split_key_list(
            self.query_scalar(&primary_key_sql(table))?,
        ))
    }

    fn readable_columns(
        &mut self,
        table: &str,
    ) -> Result<Option<crate::source::cdc::ReadableColumns>> {
        column_grants::readable_columns(&mut self.client, table).map(|(_, _, r)| Some(r))
    }

    fn replication_slot_exists(&mut self, slot: &str) -> Result<bool> {
        cdc::slot_exists_on(&mut self.client, slot)
    }

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        self.scalar_within(sql, 0)
    }

    fn query_scalar_within(
        &mut self,
        sql: &str,
        statement_timeout_s: u64,
    ) -> Result<Option<String>> {
        self.scalar_within(sql, statement_timeout_s)
    }

    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>> {
        type_mappings_on(&mut self.client, query, column_overrides)
    }

    fn column_nullable(&mut self, query: &str, column: &str) -> Result<Option<bool>> {
        column_nullable_on(&mut self.client, query, column)
    }

    /// Snapshot the source-harm counters from `pg_stat_database` for the current database.
    fn harm_counters(&mut self) -> Option<Vec<(String, i64)>> {
        let client = &mut self.client;
        let row = client
            .query_one(
                "SELECT blks_read::bigint, blks_hit::bigint, tup_returned::bigint, \
             tup_fetched::bigint, temp_files::bigint, deadlocks::bigint, temp_bytes::bigint \
             FROM pg_stat_database WHERE datname = current_database()",
                &[],
            )
            .ok()?;
        let names = [
            "pg_blks_read",
            "pg_blks_hit",
            "pg_tup_returned",
            "pg_tup_fetched",
            "pg_temp_files",
            "pg_deadlocks",
            crate::source::PG_TEMP_BYTES_KEY,
        ];
        let mut out = Vec::with_capacity(names.len() + 3);
        for (i, name) in names.iter().enumerate() {
            if let Ok(v) = row.try_get::<_, i64>(i) {
                out.push(((*name).to_string(), v));
            }
        }
        if let Ok(slots) = client.query_one(
            "SELECT COALESCE(SUM(spill_txns), 0)::bigint, COALESCE(SUM(spill_bytes), 0)::bigint, \
             COALESCE(SUM(total_bytes), 0)::bigint FROM pg_stat_replication_slots",
            &[],
        ) {
            for (i, name) in [
                "pg_slot_spill_txns",
                "pg_slot_spill_bytes",
                "pg_slot_decoded_bytes",
            ]
            .iter()
            .enumerate()
            {
                if let Ok(v) = slots.try_get::<_, i64>(i) {
                    out.push(((*name).to_string(), v));
                }
            }
        }
        Some(out)
    }

    /// Governor pressure proxy: `checkpoints_req` — WAL volume forcing checkpoints.
    fn sample_governor_pressure(&mut self) -> Option<u64> {
        pg_sample_checkpoints_req(&mut self.client).map(|v| v.max(0) as u64)
    }

    fn server_context(&mut self) -> Option<String> {
        let version = self.query_scalar("SELECT version()").ok().flatten();
        let stmt_timeout = self
            .query_scalar("SELECT current_setting('statement_timeout')")
            .ok()
            .flatten();
        let idle_timeout = self
            .query_scalar("SELECT current_setting('idle_in_transaction_session_timeout')")
            .ok()
            .flatten();
        let time_zone = self
            .query_scalar("SELECT current_setting('TimeZone')")
            .ok()
            .flatten();
        let max_conns = self
            .query_scalar("SELECT current_setting('max_connections')")
            .ok()
            .flatten();
        Some(
            serde_json::json!({
                "engine": "postgres",
                "version": version,
                "statement_timeout": stmt_timeout,
                "idle_in_transaction_session_timeout": idle_timeout,
                "time_zone": time_zone,
                "max_connections": max_conns,
            })
            .to_string(),
        )
    }
}

/// When the query is a single-table `SELECT … FROM rel` (no joins, no subquery in `FROM`), PostgreSQL result metadata does not carry `NUMERIC` typmod.
fn pg_numeric_catalog_hints_opt(
    client: &mut Client,
    query: &str,
) -> Option<HashMap<String, (u8, i8)>> {
    match pg_fetch_numeric_catalog_hints(client, query) {
        Ok(m) => m,
        Err(e) => {
            log::warn!(
                "PG numeric catalog lookup failed — NUMERIC columns will require explicit `columns:` overrides: {e}"
            );
            None
        }
    }
}

fn pg_fetch_numeric_catalog_hints(
    client: &mut Client,
    query: &str,
) -> crate::error::Result<Option<HashMap<String, (u8, i8)>>> {
    let Some(regclass_lit) = try_parse_pg_simple_from_regclass_literal(query) else {
        return Ok(None);
    };
    let locate_sql = "SELECT n.nspname::text, c.relname::text \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.oid = ($1::text)::regclass";
    let row_opt = match client.query_opt(locate_sql, &[&regclass_lit]) {
        Ok(r) => r,
        Err(e) => {
            log::warn!("PG numeric catalog: '{regclass_lit}' regclass lookup failed: {e}");
            return Ok(None);
        }
    };
    let Some(row) = row_opt else {
        return Ok(None);
    };
    let schema: String = row.get(0);
    let table: String = row.get(1);
    let rows = client.query(
        "SELECT column_name::text, data_type::text, numeric_precision, numeric_scale \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
        &[&schema, &table],
    )?;

    let mut map = HashMap::new();
    for row in rows {
        let col: String = row.get(0);
        let dt: String = row.get(1);
        if !is_pg_numeric_information_type(&dt) {
            continue;
        }
        let p: Option<i32> = row.get(2);
        let s: Option<i32> = row.get(3);
        if let (Some(p), Some(s)) = (p, s)
            && let Some(pair) = catalog_numeric_to_decimal_params(p, s)
        {
            map.insert(col, pair);
        }
    }

    if map.is_empty() {
        Ok(None)
    } else {
        log::debug!(
            "PG numeric catalog: resolved {} DECIMAL/NUMERIC column(s) for relation {regclass_lit}",
            map.len(),
        );
        Ok(Some(map))
    }
}

fn is_pg_numeric_information_type(dt: &str) -> bool {
    let d = dt.trim().to_ascii_lowercase();
    matches!(d.as_str(), "numeric" | "decimal")
        || d.starts_with("numeric(")
        || d.starts_with("decimal(")
}

/// Match Rivet YAML `decimal(p,s)` / Arrow limits (same bound as overrides).
fn catalog_numeric_to_decimal_params(precision: i32, scale: i32) -> Option<(u8, i8)> {
    if precision <= 0 || precision > 76 {
        return None;
    }
    let precision_u = precision as u8;
    if scale < i32::from(i8::MIN) || scale > i32::from(i8::MAX) {
        return None;
    }
    let scale_i = scale as i8;
    if scale_i > precision as i8 {
        return None;
    }
    Some((precision_u, scale_i))
}

/// Cap the FETCH size once, on the first batch that has rows to measure.
fn wants_fetch_cap(cap_applied: bool, row_count: usize) -> bool {
    !cap_applied && row_count > 0
}

/// FETCH size that keeps the cursor under `work_mem × 0.7`, from the first batch's observed width (pg row ≈ arrow row × 1.2, floored at 64 B), clamped to the arrow memory budget when set.
fn work_mem_fetch_cap(
    arrow_bytes: usize,
    row_count: usize,
    work_mem: i64,
    batch_size_memory_mb: Option<usize>,
) -> (usize, usize, usize) {
    let arrow_per_row = (arrow_bytes / row_count).max(1);
    let pg_per_row = ((arrow_per_row * 12) / 10).max(64);
    let safe = (((work_mem as f64) * 0.7) as usize / pg_per_row).max(100);
    let target = match batch_size_memory_mb {
        Some(mb) => safe.min(((mb * 1024 * 1024) / arrow_per_row).max(100)),
        None => safe,
    };
    (target, arrow_per_row, pg_per_row)
}

/// A FETCH that returned fewer rows than asked for drained the cursor.
fn is_short_fetch(row_count: usize, requested: usize) -> bool {
    row_count < requested
}

/// The refusal for a non-NULL probe value of a type `query_scalar` has no reader for.
fn unreadable_probe(pg_type: &str, sql: &str) -> String {
    format!(
        "postgres: cannot read a planner probe's `{pg_type}` value. rivet reads a probe as an \
         integer (int2, int4, int8, oid), float8, a string, a date, a timestamp or a uuid, and \
         never takes another type for \"no rows\". Chunk, page or partition this export on a \
         column of one of those types, remove `parallel:` from a keyset export, or use \
         `mode: full`. Probe: {sql}"
    )
}

#[cfg(test)]
mod tests {
    /// The primary-key read names the catalog, the key order and the table it resolves, quoting a quote in the name.
    #[test]
    fn the_primary_key_read_resolves_the_table_by_its_quoted_name_in_key_order() {
        let sql = super::primary_key_sql("public.o'rders");
        assert!(
            sql.contains("string_agg(a.attname::text, chr(31) ORDER BY k.ord)"),
            "{sql}"
        );
        assert!(sql.contains("i.indisprimary"), "{sql}");
        assert!(sql.contains("to_regclass('public.o''rders')"), "{sql}");
    }

    #[test]
    fn a_row_takes_the_heap_size_over_the_row_estimate() {
        use super::avg_row_bytes;
        assert_eq!(avg_row_bytes(100, 8_192), Some(81));
        assert_eq!(avg_row_bytes(1, 8_192), Some(8_192));
        assert_eq!(avg_row_bytes(3, 2), Some(0));
        assert_eq!(avg_row_bytes(0, 8_192), None);
    }

    /// `information_schema` spells a decimal column `numeric`.
    #[test]
    fn only_numeric_and_decimal_are_numeric_information_types() {
        use super::is_pg_numeric_information_type;
        for dt in [
            "numeric",
            "decimal",
            " NUMERIC ",
            "numeric(10,2)",
            "Decimal(5)",
        ] {
            assert!(is_pg_numeric_information_type(dt), "{dt}");
        }
        for dt in ["integer", "double precision", "numericx", "text", ""] {
            assert!(!is_pg_numeric_information_type(dt), "{dt}");
        }
    }

    #[test]
    fn a_fresh_text_is_the_statement_and_a_comment_no_other_nonce_gives() {
        let sql = "SELECT * FROM (SELECT 1) AS _rivet_type_probe LIMIT 0";
        assert_eq!(
            super::fresh_text(sql, 0xab),
            "SELECT * FROM (SELECT 1) AS _rivet_type_probe LIMIT 0 /* rivet 00000000000000ab */"
        );
        assert_ne!(super::fresh_text(sql, 1), super::fresh_text(sql, 2));
    }

    #[test]
    fn an_unreadable_probe_names_the_type_the_remedy_and_the_probe() {
        assert_eq!(
            super::unreadable_probe("numeric", "SELECT max(\"id\") FROM t"),
            "postgres: cannot read a planner probe's `numeric` value. rivet reads a probe as an \
             integer (int2, int4, int8, oid), float8, a string, a date, a timestamp or a uuid, \
             and never takes another type for \"no rows\". Chunk, page or partition this export \
             on a column of one of those types, remove `parallel:` from a keyset export, or use \
             `mode: full`. Probe: SELECT max(\"id\") FROM t"
        );
    }

    #[test]
    fn the_fetch_cap_applies_once_to_a_measurable_batch() {
        use super::wants_fetch_cap;
        assert!(wants_fetch_cap(false, 10));
        assert!(!wants_fetch_cap(true, 10), "already applied");
        assert!(!wants_fetch_cap(false, 0), "nothing to measure");
    }

    #[test]
    fn the_fetch_cap_keeps_the_cursor_under_work_mem() {
        use super::work_mem_fetch_cap;
        assert_eq!(
            work_mem_fetch_cap(1_000_000, 1_000, 4 << 20, None),
            (2446, 1000, 1200)
        );
        assert_eq!(
            work_mem_fetch_cap(1_000_000, 1_000, 4 << 20, Some(2)).0,
            2097
        );
        assert_eq!(work_mem_fetch_cap(10, 10, 4 << 20, None).1, 1);
        assert_eq!(work_mem_fetch_cap(10, 10, 4 << 20, None).2, 64);
        assert_eq!(work_mem_fetch_cap(1_000_000, 1_000, 1024, None).0, 100);
    }

    #[test]
    fn a_short_fetch_drains_the_cursor() {
        use super::is_short_fetch;
        assert!(is_short_fetch(9, 10));
        assert!(
            !is_short_fetch(10, 10),
            "a full fetch may have more behind it"
        );
    }

    /// The version branch of the checkpoint sampler, pinned offline.
    #[test]
    fn checkpoints_req_sql_matches_the_servers_catalog() {
        use super::checkpoints_req_sql;
        let modern = checkpoints_req_sql(true);
        assert!(
            modern.contains("pg_stat_checkpointer") && modern.contains("num_requested"),
            "a PG17+ server must be asked for num_requested: {modern}"
        );
        assert!(
            !modern.contains("pg_stat_bgwriter"),
            "the removed relation must never be named on PG17+: {modern}"
        );
        let legacy = checkpoints_req_sql(false);
        assert!(
            legacy.contains("pg_stat_bgwriter") && legacy.contains("checkpoints_req"),
            "a pre-17 server must be asked for checkpoints_req: {legacy}"
        );
        assert!(
            !legacy.contains("pg_stat_checkpointer"),
            "the not-yet-existing relation must never be named pre-17: {legacy}"
        );
    }
    use super::catalog_numeric_to_decimal_params;

    #[test]
    fn catalog_decimal_bounds() {
        assert_eq!(catalog_numeric_to_decimal_params(18, 2), Some((18, 2)));
        assert!(catalog_numeric_to_decimal_params(0, 2).is_none());
        assert!(catalog_numeric_to_decimal_params(77, 0).is_none());
        assert!(catalog_numeric_to_decimal_params(18, 19).is_none());
        assert_eq!(catalog_numeric_to_decimal_params(76, 0), Some((76, 0)));
        assert_eq!(catalog_numeric_to_decimal_params(18, 18), Some((18, 18)));
        assert_eq!(
            catalog_numeric_to_decimal_params(10, -128),
            Some((10, -128))
        );
        assert!(catalog_numeric_to_decimal_params(10, -129).is_none());
        assert!(catalog_numeric_to_decimal_params(76, 200).is_none());
    }

    #[test]
    fn parse_work_mem_handles_pg_units() {
        use super::parse_work_mem;
        assert_eq!(parse_work_mem("4MB"), Some(4 * 1024 * 1024));
        assert_eq!(parse_work_mem("16384kB"), Some(16384 * 1024));
        assert_eq!(parse_work_mem("1GB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_work_mem("  4MB  "), Some(4 * 1024 * 1024));
        assert_eq!(parse_work_mem("4mb"), Some(4 * 1024 * 1024));
        assert_eq!(parse_work_mem("65536"), Some(65536 * 1024));
        assert_eq!(parse_work_mem(""), None);
        assert_eq!(parse_work_mem("garbage"), None);
        assert_eq!(parse_work_mem("4s"), None);
        assert_eq!(
            parse_work_mem("2TB"),
            Some(2 * 1024 * 1024 * 1024 * 1024),
            "terabytes are 1024^4"
        );
        assert_eq!(parse_work_mem("0.5MB"), Some(512 * 1024));
        assert_eq!(parse_work_mem("0"), None);
        assert_eq!(parse_work_mem("0MB"), None);
    }
}
