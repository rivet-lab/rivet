//! **Layer: Execution** — MSSQL / SQL Server source engine.

mod arrow_convert;
pub(crate) mod cdc;
pub(crate) mod column_grants;
mod proxy;

pub use proxy::MssqlProxyKind;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use proxy::{detect_mssql_proxy_kind, warn_proxy_kind};

use crate::config::{TlsConfig, TlsMode};
use crate::error::Result;
use crate::source::batch_controller::{
    AdaptiveBatchController, DEFAULT_BATCH_TARGET_MB, PROBE_BATCH_SIZE,
};
use crate::source::query::build_export_query;
use crate::source::{BatchSink, ExportRequest, Source, TableIntrospection};
use crate::types::{ColumnOverrides, TypeMapping};

type MssqlClient = Client<Compat<TcpStream>>;

/// SQL Server source.
const SERVER_COUNTERS_GRANTED: &str = "SELECT CAST(CASE WHEN HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER PERFORMANCE STATE') = 1 \
     OR HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE') = 1 THEN 1 ELSE 0 END AS NVARCHAR(1))";

pub struct MssqlSource {
    rt: Runtime,
    client: MssqlClient,
    /// Pooler/gateway classification, sampled once at connect time.
    proxy_kind: MssqlProxyKind,
    /// Whether the export issued `SET LOCK_TIMEOUT` on this connection, so the `Drop` teardown knows to reset it (Epic 18 B2.
    lock_timeout_applied: bool,
    /// Whether an incremental read switched this session to `SNAPSHOT`, so `Drop` resets it.
    snapshot_applied: bool,
    /// Whether the locking-read warning was already emitted on this connection.
    locking_read_warned: bool,
    /// Whether this login may read the server's DMVs, asked once; see [`MssqlSource::server_counters_readable`].
    server_counters: Option<bool>,
}

/// How an incremental read can see one transaction-consistent state on this database.
#[derive(Debug, PartialEq, Eq)]
enum IncrementalReadIsolation {
    /// `READ_COMMITTED_SNAPSHOT` is on: every statement already reads one snapshot.
    StatementSnapshot,
    /// `ALLOW_SNAPSHOT_ISOLATION` is on: rivet switches the read to `SNAPSHOT`.
    Snapshot,
    /// Neither: locking READ COMMITTED, which can read a concurrent transaction half-applied.
    Locking,
}

/// Classify `sys.databases.snapshot_isolation_state` (1 = ON) and `is_read_committed_snapshot_on`.
fn incremental_read_isolation(snapshot_state: u8, rcsi: bool) -> IncrementalReadIsolation {
    if rcsi {
        IncrementalReadIsolation::StatementSnapshot
    } else if snapshot_state == 1 {
        IncrementalReadIsolation::Snapshot
    } else {
        IncrementalReadIsolation::Locking
    }
}

/// Run a session `SET` as a plain batch: under `sp_executesql` (tiberius `execute`) it reverts on return.
async fn session_set(client: &mut MssqlClient, set: &str) -> tiberius::Result<()> {
    client
        .simple_query(set)
        .await?
        .into_results()
        .await
        .map(drop)
}

/// The warning an incremental read on a locking-READ-COMMITTED database emits.
fn locking_incremental_read_warning(db: &str) -> String {
    let ident = db.replace(']', "]]");
    format!(
        "mssql: database '{db}' has neither READ_COMMITTED_SNAPSHOT nor ALLOW_SNAPSHOT_ISOLATION on, \
         so this incremental read runs under locking READ COMMITTED: a transaction that commits \
         while the scan runs can be read half-applied, and the rows the scan had already passed \
         can fall behind the new cursor and be skipped by every later run. Fix: \
         `ALTER DATABASE [{ident}] SET ALLOW_SNAPSHOT_ISOLATION ON` (rivet then reads each \
         incremental window from one snapshot), or add a `settle:` window longer than your \
         longest write transaction"
    )
}

impl Drop for MssqlSource {
    /// Pooler-safe session teardown (Epic 18 B2).
    fn drop(&mut self) {
        let reset = match (self.lock_timeout_applied, self.snapshot_applied) {
            (false, false) => return,
            (true, false) => "SET LOCK_TIMEOUT -1",
            (false, true) => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            (true, true) => "SET LOCK_TIMEOUT -1; SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        };
        let Self { rt, client, .. } = self;
        let _ = rt.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                client.simple_query(reset).await?.into_results().await
            })
            .await
        });
    }
}

/// Parsed `sqlserver://user[:password]@host[:port]/db` connection parts.
pub struct MssqlUrl {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: crate::secret::Secret,
    pub database: String,
}

/// The tiberius `Config` for a login, with the shared TLS policy mapped onto its cert-trust knobs.
pub(crate) fn tiberius_config(
    host: &str,
    port: u16,
    database: &str,
    user: &str,
    password: &crate::secret::Secret,
    tls: Option<&TlsConfig>,
) -> Config {
    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.database(database);
    config.authentication(AuthMethod::sql_server(user, password.expose()));
    config.command_timeout(None);

    config.encryption(EncryptionLevel::Required);
    match tls {
        Some(cfg) if mssql_trusts_cert_without_verify(cfg) => config.trust_cert(),
        Some(cfg) => {
            if let Some(ca) = cfg.ca_file.as_deref() {
                config.trust_cert_ca(ca);
            }
        }
        None => {
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                log::warn!(
                    "mssql: connecting with TLS certificate validation disabled \
                     (no `source.tls:` block) — the connection is encrypted but the \
                     server certificate is not verified (MITM not detected). Add \
                     `source.tls: {{ mode: verify-full, ca_file: <ca.pem> }}` to enable \
                     strict validation (or `mode: verify-ca` to skip only hostname checks)."
                );
            });
            config.trust_cert();
        }
    }
    config
}

/// Open TCP to `config`'s address and log in; a refused or unresolvable address names `at`.
pub(crate) async fn dial(config: Config, at: &str) -> Result<MssqlClient> {
    let tcp = TcpStream::connect(config.get_addr()).await.map_err(|e| {
        crate::source::describe_connect_error(
            at,
            crate::error::coded(
                crate::error::codes::SOURCE_CONNECT_FAILED,
                format!("mssql: TCP connect failed: {e}"),
            ),
        )
    })?;
    tcp.set_nodelay(true).ok();
    Client::connect(config, tcp.compat_write())
        .await
        .map_err(|e| {
            crate::error::coded(
                crate::error::codes::SOURCE_CONNECT_FAILED,
                format!("mssql: login failed: {e}"),
            )
        })
}

/// Does this TLS posture mean "encrypt, but do NOT verify the certificate"?
pub(crate) fn mssql_trusts_cert_without_verify(cfg: &TlsConfig) -> bool {
    cfg.mode == TlsMode::Disable || cfg.mode == TlsMode::Require || cfg.accept_invalid_certs
}

pub fn parse_mssql_url(url: &str) -> Result<MssqlUrl> {
    let authority = url
        .strip_prefix("sqlserver://")
        .or_else(|| url.strip_prefix("mssql://"))
        .and_then(|_| crate::url_text::authority(url))
        .ok_or_else(|| {
            crate::error::coded(
                crate::error::codes::CONFIG_SOURCE_URL_INVALID,
                format!("mssql url must start with sqlserver:// — got {url}"),
            )
        })?;
    let userinfo = authority.userinfo.ok_or_else(|| {
        crate::error::coded(
            crate::error::codes::CONFIG_SOURCE_URL_INVALID,
            format!("mssql url missing user@host: {url}"),
        )
    })?;
    let (user, password) = match userinfo.split_once(':') {
        Some((u, p)) => (
            crate::url_text::pct_decode(u),
            crate::url_text::pct_decode(p),
        ),
        None => (crate::url_text::pct_decode(userinfo), String::new()),
    };
    let hostport = authority.hosts;
    let database = {
        let raw = authority.rest.strip_prefix('/').unwrap_or_default();
        seeded!(
            "url-part-read-raw",
            raw.to_string(),
            crate::url_text::pct_decode(raw)
        )
    };
    let (host, port) = crate::url_text::split_host_port(hostport, 1433).map_err(|p| {
        crate::error::coded(
            crate::error::codes::CONFIG_SOURCE_URL_INVALID,
            format!("mssql url port not a number: {p}"),
        )
    })?;
    if database.is_empty() {
        crate::rivet_bail!(
            crate::error::codes::CONFIG_SOURCE_URL_INVALID,
            "mssql url must include a database: sqlserver://user:pass@host:port/<db>"
        );
    }
    Ok(MssqlUrl {
        host: host.to_string(),
        port,
        user,
        password: password.into(),
        database,
    })
}

impl MssqlSource {
    /// Connect to SQL Server, honouring the shared `TlsConfig`.
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        crate::source::require_tls_or_loopback(url, tls)?;
        let parts = parse_mssql_url(url)?;
        let config = tiberius_config(
            &parts.host,
            parts.port,
            &parts.database,
            &parts.user,
            &parts.password,
            tls,
        );

        let rt = crate::source::driver_runtime(
            "mssql",
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build(),
        )?;

        let client = rt.block_on(dial(config, url))?;

        let mut src = Self {
            rt,
            client,
            proxy_kind: MssqlProxyKind::Direct,
            lock_timeout_applied: false,
            snapshot_applied: false,
            locking_read_warned: false,
            server_counters: None,
        };
        src.query_scalar("SELECT 1")?;
        let kind = detect_mssql_proxy_kind(&src.rt, &mut src.client);
        warn_proxy_kind(kind);
        src.proxy_kind = kind;
        Ok(src)
    }

    /// Expose the proxy classification for diagnostics (preflight, integration tests).
    #[allow(dead_code)]
    pub fn proxy_kind(&self) -> MssqlProxyKind {
        self.proxy_kind
    }

    /// Decide whether this incremental read runs under `SNAPSHOT`; warn once when it cannot.
    fn prepare_incremental_isolation(&mut self) -> Result<bool> {
        let Self { rt, client, .. } = self;
        let (db, snapshot_state, rcsi) = rt.block_on(async {
            let row = client
                .query(
                    "SELECT DB_NAME(), snapshot_isolation_state, is_read_committed_snapshot_on \
                     FROM sys.databases WHERE database_id = DB_ID()",
                    &[],
                )
                .await
                .map_err(|e| {
                    crate::error::coded_driver(
                        crate::error::codes::SOURCE_QUERY_FAILED,
                        "mssql: reading the database isolation options failed",
                        e,
                    )
                })?
                .into_row()
                .await
                .map_err(|e| {
                    crate::error::coded_driver(
                        crate::error::codes::SOURCE_QUERY_FAILED,
                        "mssql: reading the database isolation options failed",
                        e,
                    )
                })?
                .ok_or_else(|| {
                    crate::error::coded(
                        crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                        "mssql: sys.databases has no row for the current database".to_string(),
                    )
                })?;
            Ok::<_, anyhow::Error>((
                row.get::<&str, _>(0).unwrap_or_default().to_string(),
                row.get::<u8, _>(1).unwrap_or(0),
                row.get::<bool, _>(2).unwrap_or(false),
            ))
        })?;
        match incremental_read_isolation(snapshot_state, rcsi) {
            IncrementalReadIsolation::Snapshot => {
                self.snapshot_applied = true;
                Ok(true)
            }
            IncrementalReadIsolation::StatementSnapshot => Ok(false),
            IncrementalReadIsolation::Locking => {
                if !self.locking_read_warned {
                    self.locking_read_warned = true;
                    log::warn!("{}", locking_incremental_read_warning(&db));
                }
                Ok(false)
            }
        }
    }

    /// Declared `(precision, scale)` per decimal/numeric column, read from `sys.columns`, for a simple single-table `SELECT … FROM [schema.]table`.
    fn mssql_decimal_catalog_hints_opt(
        &mut self,
        query: &str,
    ) -> Option<HashMap<String, (u8, i8)>> {
        let (schema, table) = parse_mssql_simple_from_table(query)?;
        match self.fetch_mssql_decimal_catalog_hints(&schema, &table) {
            Ok(m) => m,
            Err(e) => {
                log::warn!(
                    "mssql decimal catalog lookup failed for {schema}.{table} — decimal scale \
                     will fall back to first-batch inference (declare it with a `columns:` \
                     override if an all-NULL first batch truncates it): {e}"
                );
                None
            }
        }
    }

    /// Probe `sys.columns` for each `decimal`/`numeric` column's declared `(precision, scale)`.
    fn fetch_mssql_decimal_catalog_hints(
        &mut self,
        schema: &str,
        table: &str,
    ) -> Result<Option<HashMap<String, (u8, i8)>>> {
        let sql = format!(
            "SELECT c.name, c.precision, c.scale \
             FROM sys.columns c \
             JOIN sys.types t ON t.user_type_id = c.user_type_id \
             JOIN sys.objects o ON o.object_id = c.object_id \
             JOIN sys.schemas s ON s.schema_id = o.schema_id \
             WHERE s.name = N'{}' AND o.name = N'{}' \
             AND t.name IN ('decimal', 'numeric')",
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );
        let Self { rt, client, .. } = self;
        let rows = rt.block_on(async {
            client
                .query(sql.as_str(), &[])
                .await
                .map_err(|e| {
                    crate::error::coded_driver(
                        crate::error::codes::SOURCE_QUERY_FAILED,
                        "mssql: sys.columns probe failed",
                        e,
                    )
                })?
                .into_first_result()
                .await
                .map_err(|e| {
                    crate::error::coded_driver(
                        crate::error::codes::SOURCE_QUERY_FAILED,
                        "mssql: reading sys.columns rows failed",
                        e,
                    )
                })
        })?;

        let mut map = HashMap::new();
        for row in &rows {
            let name: Option<&str> = row.try_get(0).ok().flatten();
            let precision: Option<u8> = row.try_get(1).ok().flatten();
            let scale: Option<u8> = row.try_get(2).ok().flatten();
            if let (Some(name), Some(p), Some(s)) = (name, precision, scale)
                && let Some(pair) = catalog_decimal_to_params(p, s)
            {
                map.insert(name.to_string(), pair);
            }
        }

        if map.is_empty() {
            Ok(None)
        } else {
            log::debug!(
                "mssql decimal catalog: resolved {} DECIMAL/NUMERIC column(s) for {schema}.{table}",
                map.len(),
            );
            Ok(Some(map))
        }
    }
}

/// Convert `sys.columns` `(precision, scale)` into Rivet `decimal(p, s)` parameters, rejecting anything outside the bounds the YAML overrides accept.
fn catalog_decimal_to_params(precision: u8, scale: u8) -> Option<(u8, i8)> {
    if precision == 0 || precision > 38 {
        return None;
    }
    if scale > precision || scale > i8::MAX as u8 {
        return None;
    }
    Some((precision, scale as i8))
}

/// Extract the `(schema, table)` of a simple single-table T-SQL `SELECT … FROM [schema.]table` (no joins, no comma list, no subquery in `FROM`).
fn parse_mssql_simple_from_table(query: &str) -> Option<(String, String)> {
    let mut names: Vec<String> = crate::sql::single_relation(query, false)?
        .into_iter()
        .map(|part| part.name)
        .collect();
    let table = names.pop()?;
    match names.len() {
        0 => Some(("dbo".to_string(), table)),
        1 | 2 => Some((names.pop()?, table)),
        _ => None,
    }
}

/// The first cell of `sql`'s first row on `client`.
async fn scalar_on(client: &mut MssqlClient, sql: &str) -> Result<Option<String>> {
    let row = client
        .query(sql, &[])
        .await
        .map_err(|e| {
            crate::error::coded_driver(
                crate::error::codes::SOURCE_QUERY_FAILED,
                "mssql: scalar query failed",
                e,
            )
        })?
        .into_row()
        .await
        .map_err(|e| {
            crate::error::coded_driver(
                crate::error::codes::SOURCE_QUERY_FAILED,
                "mssql: reading scalar row failed",
                e,
            )
        })?;
    Ok(row.and_then(|r| scalar_to_string(&r)))
}

impl Source for MssqlSource {
    fn introspect_for_chunking(&mut self, qualified_table: &str) -> Result<TableIntrospection> {
        introspect_mssql_on(self, qualified_table)
    }

    fn readable_columns(
        &mut self,
        table: &str,
    ) -> Result<Option<crate::source::cdc::ReadableColumns>> {
        column_grants::readable_relation(self, table).map(|(_, _, r)| Some(r))
    }

    fn finer_than_microsecond_columns(&mut self, table: &str) -> Result<Vec<String>> {
        let (schema, table) = table.split_once('.').unwrap_or(("dbo", table));
        Ok(finer_than_microsecond_columns(self, schema, table)?
            .into_iter()
            .map(|(column, _)| column)
            .collect())
    }

    fn finer_than_microsecond_in_query(&mut self, query: &str) -> Result<Vec<String>> {
        Ok(self
            .query_scalar(&finer_than_microsecond_in_query_sql(query))?
            .map(|s| split_char31(&s))
            .unwrap_or_default())
    }

    fn export(&mut self, request: &ExportRequest<'_>, sink: &mut dyn BatchSink) -> Result<()> {
        let built = build_export_query(request, crate::config::SourceType::Mssql)?;
        let sql = built.sql.clone();
        let overrides = request.column_overrides.clone();
        let mut ctl =
            AdaptiveBatchController::new(request.tuning, request.tuning.batch_size.max(1));
        let mut cap_applied = false;
        let lock_timeout_ms = request.tuning.lock_timeout_s.saturating_mul(1000);
        let stmt_timeout = (request.tuning.statement_timeout_s > 0)
            .then(|| std::time::Duration::from_secs(request.tuning.statement_timeout_s));

        let hint_query = request.catalog_hint_query.unwrap_or(request.query);
        let decimal_hints = self.mssql_decimal_catalog_hints_opt(hint_query);

        if lock_timeout_ms > 0 {
            self.lock_timeout_applied = true;
        }
        let snapshot_read =
            request.incremental.is_some() && self.prepare_incremental_isolation()?;

        let Self { rt, client, .. } = self;
        rt.block_on(async {
            use futures_util::stream::TryStreamExt;
            use tiberius::QueryItem;

            if lock_timeout_ms > 0 {
                session_set(client, &format!("SET LOCK_TIMEOUT {lock_timeout_ms}"))
                    .await
                    .map_err(|e| crate::error::coded_driver(crate::error::codes::SOURCE_QUERY_FAILED, "mssql: SET LOCK_TIMEOUT failed", e))?;
            }
            if snapshot_read {
                session_set(client, "SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
                    .await
                    .map_err(|e| crate::error::coded_driver(crate::error::codes::SOURCE_QUERY_FAILED, "mssql: SET TRANSACTION ISOLATION LEVEL SNAPSHOT failed", e))?;
            }

            let started = std::time::Instant::now();
            let mut stream = client
                .query(sql.as_str(), &[])
                .await
                .map_err(|e| crate::error::coded_driver(crate::error::codes::SOURCE_QUERY_FAILED, "mssql: query failed", e))?;

            let mut columns: Vec<tiberius::Column> = Vec::new();
            let mut buf: Vec<tiberius::Row> = Vec::with_capacity(ctl.target());
            let mut schema: Option<SchemaRef> = None;
            let max_value_bytes = request.tuning.max_value_bytes();

            while let Some(item) = stream
                .try_next()
                .await
                .map_err(|e| crate::error::coded_driver(crate::error::codes::SOURCE_QUERY_FAILED, "mssql: streaming rows failed", e))?
            {
                if let Some(budget) = stmt_timeout
                    && started.elapsed() > budget
                {
                    return Err(crate::source::StatementDurationTimeout::mssql(
                        budget.as_secs(),
                    )
                    .into());
                }
                match item {
                    QueryItem::Metadata(meta) if columns.is_empty() => {
                        columns = meta.columns().to_vec();
                        if let Ok((provisional, _)) = arrow_convert::mssql_columns_to_schema(
                            &columns,
                            &overrides,
                            &[],
                            decimal_hints.as_ref(),
                        ) {
                            let eff = request
                                .tuning
                                .effective_batch_size(Some(&Arc::new(provisional)));
                            ctl.raise_configured_ceiling(eff);
                        }
                    }
                    QueryItem::Metadata(_) => {}
                    QueryItem::Row(row) => {
                        buf.push(row);
                        if buf.len() >= ctl.target() {
                            let arrow_bytes = emit_mssql_batch(
                                &columns,
                                &overrides,
                                decimal_hints.as_ref(),
                                &mut schema,
                                &buf,
                                sink,
                                max_value_bytes,
                            )?;
                            let n = buf.len();
                            buf.clear();
                            if !cap_applied && n > 0 {
                                let arrow_per_row = (arrow_bytes / n).max(64);
                                let target_mb = request
                                    .tuning
                                    .batch_size_memory_mb
                                    .unwrap_or(DEFAULT_BATCH_TARGET_MB);
                                let safe = ((target_mb * 1024 * 1024) / arrow_per_row)
                                    .max(PROBE_BATCH_SIZE);
                                if let Some(new) = ctl.apply_memory_cap(safe) {
                                    log::info!(
                                        "MSSQL batch cap: arrow≈{} B/row, target={} MB → batch_size → {}",
                                        arrow_per_row,
                                        target_mb,
                                        new
                                    );
                                    buf.reserve(new.saturating_sub(buf.capacity()));
                                }
                                cap_applied = true;
                            }
                            ctl.after_batch(|| None);
                            ctl.throttle(n);
                        }
                    }
                }
            }
            if !buf.is_empty() || schema.is_none() {
                emit_mssql_batch(
                    &columns,
                    &overrides,
                    decimal_hints.as_ref(),
                    &mut schema,
                    &buf,
                    sink,
                    max_value_bytes,
                )?;
            }
            drop(stream);
            if snapshot_read {
                session_set(client, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    .await
                    .map_err(|e| crate::error::coded_driver(crate::error::codes::SOURCE_QUERY_FAILED, "mssql: resetting the isolation level failed", e))?;
            }
            Ok::<_, anyhow::Error>(())
        })?;
        self.snapshot_applied = false;
        Ok(())
    }

    fn primary_key(&mut self, table: &str) -> Result<Option<Vec<String>>> {
        let sql = format!(
            "SELECT STRING_AGG(c.name, CHAR(31)) WITHIN GROUP (ORDER BY ic.key_ordinal) \
             FROM sys.indexes i \
             JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
             JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
             WHERE i.is_primary_key = 1 AND i.object_id = OBJECT_ID(N'{}')",
            table.replace('\'', "''")
        );
        Ok(crate::source::split_key_list(self.query_scalar(&sql)?))
    }

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        let Self { rt, client, .. } = self;
        rt.block_on(scalar_on(client, sql))
    }

    fn query_scalar_within(
        &mut self,
        sql: &str,
        statement_timeout_s: u64,
    ) -> Result<Option<String>> {
        if statement_timeout_s == 0 {
            return self.query_scalar(sql);
        }
        let budget = std::time::Duration::from_secs(statement_timeout_s);
        let Self { rt, client, .. } = self;
        rt.block_on(async {
            match tokio::time::timeout(budget, scalar_on(client, sql)).await {
                Ok(read) => read,
                Err(_) => {
                    client.cancel_query().await.map_err(|e| {
                        crate::error::coded_driver(
                            crate::error::codes::SOURCE_QUERY_FAILED,
                            "mssql: cancelling the timed-out scalar query failed",
                            e,
                        )
                    })?;
                    Err(crate::source::StatementDurationTimeout::mssql(statement_timeout_s).into())
                }
            }
        })
    }

    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>> {
        let decimal_hints = self.mssql_decimal_catalog_hints_opt(query);
        let wrapped = format!("SELECT * FROM ({query}) AS _rivet_q WHERE 1 = 0");
        let overrides = column_overrides.clone();
        let Self { rt, client, .. } = self;
        rt.block_on(async {
            let mut stream = client.query(wrapped.as_str(), &[]).await.map_err(|e| {
                crate::error::coded_driver(
                    crate::error::codes::SOURCE_QUERY_FAILED,
                    "mssql: type-probe query failed",
                    e,
                )
            })?;
            let columns = stream
                .columns()
                .await
                .map_err(|e| {
                    crate::error::coded_driver(
                        crate::error::codes::SOURCE_QUERY_FAILED,
                        "mssql: type-probe metadata failed",
                        e,
                    )
                })?
                .map(<[_]>::to_vec)
                .unwrap_or_default();
            let _ = stream.into_first_result().await;
            arrow_convert::mssql_type_mappings(&columns, &overrides, decimal_hints.as_ref())
        })
    }

    fn column_nullable(&mut self, query: &str, column: &str) -> Result<Option<bool>> {
        let describe = format!(
            "EXEC sp_describe_first_result_set @tsql = N'{}', @params = NULL, @browse_information_mode = 0",
            query.replace('\'', "''")
        );
        let Self { rt, client, .. } = self;
        rt.block_on(async {
            let rows = client
                .query(describe.as_str(), &[])
                .await
                .map_err(|e| {
                    crate::error::coded_driver(
                        crate::error::codes::SOURCE_QUERY_FAILED,
                        "mssql: result-set description failed",
                        e,
                    )
                })?
                .into_first_result()
                .await
                .map_err(|e| {
                    crate::error::coded_driver(
                        crate::error::codes::SOURCE_QUERY_FAILED,
                        "mssql: reading the result-set description failed",
                        e,
                    )
                })?;
            let described = rows
                .iter()
                .find(|r| r.get::<&str, _>("name") == Some(column))
                .and_then(|r| r.get::<bool, _>("is_nullable"));
            if seeded!("cursor-nullability-claimed-without-the-table-catalog", true, described != Some(true)) {
                return Ok(described);
            }
            let Some((schema, table)) = parse_mssql_simple_from_table(query) else {
                return Ok(None);
            };
            let base = match crate::sql::bare_column_source(query, column) {
                _ if seeded!(
                    "cursor-nullability-not-traced-to-its-source-column",
                    true,
                    false
                ) =>
                {
                    column.to_string()
                }
                Some(part) => part.name,
                None => return Ok(None),
            };
            let catalog = format!(
                "SELECT c.is_nullable FROM sys.columns c WHERE c.object_id = OBJECT_ID(N'[{}].[{}]') AND c.name = N'{}'",
                schema.replace(']', "]]").replace('\'', "''"),
                table.replace(']', "]]").replace('\'', "''"),
                base.replace('\'', "''")
            );
            let row = client
                .query(catalog.as_str(), &[])
                .await
                .map_err(|e| {
                    crate::error::coded_driver(
                        crate::error::codes::SOURCE_QUERY_FAILED,
                        "mssql: column catalog read failed",
                        e,
                    )
                })?
                .into_row()
                .await
                .map_err(|e| {
                    crate::error::coded_driver(
                        crate::error::codes::SOURCE_QUERY_FAILED,
                        "mssql: reading the column catalog failed",
                        e,
                    )
                })?;
            Ok(row.and_then(|r| r.get::<bool, _>(0)))
        })
    }

    fn harm_counters(&mut self) -> Option<Vec<(String, i64)>> {
        MssqlSource::harm_counters(self)
    }

    fn sample_governor_pressure(&mut self) -> Option<u64> {
        if !self.server_counters_readable() {
            return None;
        }
        let Self { rt, client, .. } = self;
        let sql = "SELECT cntr_value FROM sys.dm_os_performance_counters \
                   WHERE counter_name LIKE 'Log Flush Waits%' AND instance_name = '_Total'";
        rt.block_on(async {
            let row = client.query(sql, &[]).await.ok()?.into_row().await.ok()??;
            row.get::<i64, _>(0).map(|v| v.max(0) as u64)
        })
    }

    fn server_context(&mut self) -> Option<String> {
        let version = self
            .query_scalar("SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))")
            .ok()
            .flatten();
        let edition = self
            .query_scalar("SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128))")
            .ok()
            .flatten();
        let lock_timeout = self
            .query_scalar("SELECT CAST(@@LOCK_TIMEOUT AS NVARCHAR(32))")
            .ok()
            .flatten();
        let max_conns = self
            .query_scalar("SELECT CAST(@@MAX_CONNECTIONS AS NVARCHAR(32))")
            .ok()
            .flatten();
        let collation = self
            .query_scalar("SELECT CAST(SERVERPROPERTY('Collation') AS NVARCHAR(128))")
            .ok()
            .flatten();
        Some(
            serde_json::json!({
                "engine": "mssql",
                "version": version,
                "edition": edition,
                "lock_timeout_ms": lock_timeout,
                "max_connections": max_conns,
                "collation": collation,
            })
            .to_string(),
        )
    }
}

impl MssqlSource {
    /// Snapshot lock-wait counters from `sys.dm_os_wait_stats` (LCK_* waits).
    pub(crate) fn harm_counters(&mut self) -> Option<Vec<(String, i64)>> {
        if !self.server_counters_readable() {
            return None;
        }
        let Self { rt, client, .. } = self;
        let sql = "SELECT (SELECT SUM(waiting_tasks_count) FROM sys.dm_os_wait_stats \
                           WHERE wait_type LIKE 'LCK%'), \
                          (SELECT SUM(wait_time_ms) FROM sys.dm_os_wait_stats \
                           WHERE wait_type LIKE 'LCK%'), \
                          (SELECT SUM(total_logical_reads) FROM sys.dm_exec_query_stats), \
                          SUM(CASE WHEN RTRIM(counter_name) = 'Page lookups/sec' \
                               AND object_name LIKE '%Buffer Manager%' THEN cntr_value END), \
                          SUM(CASE WHEN RTRIM(counter_name) = 'Page reads/sec' \
                               AND object_name LIKE '%Buffer Manager%' THEN cntr_value END), \
                          SUM(CASE WHEN RTRIM(counter_name) = 'Worktables Created/sec' \
                               AND object_name LIKE '%Access Methods%' THEN cntr_value END), \
                          SUM(CASE WHEN RTRIM(counter_name) = 'Workfiles Created/sec' \
                               AND object_name LIKE '%Access Methods%' THEN cntr_value END) \
                   FROM sys.dm_os_performance_counters";
        rt.block_on(async {
            let row = client.query(sql, &[]).await.ok()?.into_row().await.ok()??;
            let names = [
                "mssql_lock_waits",
                "mssql_lock_wait_ms",
                "mssql_logical_reads",
                "mssql_page_lookups",
                "mssql_page_reads",
                "mssql_worktables_created",
                "mssql_workfiles_created",
            ];
            Some(
                names
                    .iter()
                    .enumerate()
                    .filter_map(|(i, n)| row.get::<i64, _>(i).map(|v| ((*n).to_string(), v)))
                    .collect(),
            )
        })
    }

    /// Whether this login may read `sys.dm_os_performance_counters` and the other server DMVs, asked once per connection.
    fn server_counters_readable(&mut self) -> bool {
        if let Some(known) = self.server_counters {
            return known;
        }
        let readable = self
            .query_scalar(SERVER_COUNTERS_GRANTED)
            .ok()
            .flatten()
            .is_some_and(|v| {
                seeded!(
                    "mssql-denied-counters-queried",
                    v.trim() != "x",
                    v.trim() == "1"
                )
            });
        self.server_counters = Some(readable);
        readable
    }

    /// Does the current login hold `VIEW SERVER STATE` — the permission [`harm_counters`] needs?
    pub(crate) fn has_view_server_state(&mut self) -> Option<bool> {
        let Self { rt, client, .. } = self;
        rt.block_on(async {
            let row = client
                .query(
                    "SELECT HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE')",
                    &[],
                )
                .await
                .ok()?
                .into_row()
                .await
                .ok()??;
            row.get::<i32, _>(0).map(|v| v == 1)
        })
    }

    /// One-shot CDC health probe for `rivet doctor` (see `preflight::cdc_health`).
    pub(crate) fn cdc_health(&mut self, capture_instance: Option<&str>) -> Result<MssqlCdcProbe> {
        let Self { rt, client, .. } = self;
        rt.block_on(async {
            let max: Option<String> = client
                .query(
                    "SELECT CONVERT(varchar(24), sys.fn_cdc_get_max_lsn(), 1)",
                    &[],
                )
                .await?
                .into_row()
                .await?
                .and_then(|r| r.get::<&str, _>(0).map(|s| s.to_string()));

            let min: Option<String> = match capture_instance {
                None => None,
                Some(ci) => client
                    .query(
                        "SELECT CONVERT(varchar(24), sys.fn_cdc_get_min_lsn(@P1), 1)",
                        &[&ci],
                    )
                    .await?
                    .into_row()
                    .await?
                    .and_then(|r| r.get::<&str, _>(0).map(|s| s.to_string()))
                    .filter(|s| s.trim_start_matches("0x").chars().any(|c| c != '0')),
            };

            let agent: Option<bool> = async {
                let row = client
                    .query(
                        "SELECT status_desc FROM sys.dm_server_services \
                         WHERE servicename LIKE 'SQL Server Agent%'",
                        &[],
                    )
                    .await
                    .ok()?
                    .into_row()
                    .await
                    .ok()??;
                row.get::<&str, _>(0)
                    .map(|s| s.eq_ignore_ascii_case("Running"))
            }
            .await;

            Ok(MssqlCdcProbe {
                cdc_enabled: max.is_some(),
                max_lsn_hex: max.clone(),
                instance_min_lsn: min,
                agent_running: agent,
            })
        })
    }
}

/// Result of [`MssqlSource::cdc_health`].
pub(crate) struct MssqlCdcProbe {
    pub cdc_enabled: bool,
    /// The database's current max LSN (`0x…` hex) — the `initial: snapshot` anchor position.
    pub max_lsn_hex: Option<String>,
    /// Hex LSN (`0x…`) of the capture instance's retained minimum.
    pub instance_min_lsn: Option<String>,
    /// `None` ⇒ could not verify (no `VIEW SERVER STATE`).
    pub agent_running: Option<bool>,
}

/// Connect and check whether the login has `VIEW SERVER STATE`.
pub(crate) fn sample_view_server_state(url: &str, tls: Option<&TlsConfig>) -> Option<bool> {
    let mut src = MssqlSource::connect_with_tls(url, tls).ok()?;
    src.has_view_server_state()
}

/// Emit one Arrow batch from `rows`, building (and emitting) the schema on the first call and reusing it thereafter.
fn emit_mssql_batch(
    columns: &[tiberius::Column],
    overrides: &ColumnOverrides,
    decimal_hints: Option<&HashMap<String, (u8, i8)>>,
    schema: &mut Option<SchemaRef>,
    rows: &[tiberius::Row],
    sink: &mut dyn BatchSink,
    max_value_bytes: Option<usize>,
) -> Result<usize> {
    let schema_ref = match schema {
        Some(s) => s.clone(),
        None => {
            let (built, _decoders) =
                arrow_convert::mssql_columns_to_schema(columns, overrides, rows, decimal_hints)?;
            let s: SchemaRef = Arc::new(built);
            sink.on_schema(s.clone())?;
            *schema = Some(s.clone());
            s
        }
    };
    if !rows.is_empty() {
        let batch = arrow_convert::mssql_rows_to_record_batch(
            &schema_ref,
            rows,
            max_value_bytes,
            overrides,
        )?;
        let bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
        sink.on_batch(&batch)?;
        return Ok(bytes);
    }
    Ok(0)
}

/// Render a T-SQL `NUMERIC` as exact decimal text.
fn numeric_to_display(raw: i128, scale: usize) -> String {
    if scale == 0 {
        raw.to_string()
    } else {
        let neg = raw < 0;
        let digits = raw.unsigned_abs().to_string();
        let digits = format!("{digits:0>width$}", width = scale + 1);
        let (int, frac) = digits.split_at(digits.len() - scale);
        format!("{}{int}.{frac}", if neg { "-" } else { "" })
    }
}

/// Render a row's first column as a display string for `query_scalar` (min/max bounds, COUNT(*), SELECT 1).
fn scalar_to_string(row: &tiberius::Row) -> Option<String> {
    scalar_text(row.cells().next().map(|(_, d)| d)?)
}

/// A scalar cell as text SQL Server casts back to its own type: the shared ISO renderer for every type with no arm of its own.
fn scalar_text(cell: &tiberius::ColumnData<'static>) -> Option<String> {
    use tiberius::{ColumnData, FromSql};
    match cell {
        ColumnData::U8(v) => v.map(|x| x.to_string()),
        ColumnData::I16(v) => v.map(|x| x.to_string()),
        ColumnData::I32(v) => v.map(|x| x.to_string()),
        ColumnData::I64(v) => v.map(|x| x.to_string()),
        ColumnData::F32(v) => v.map(|x| x.to_string()),
        ColumnData::F64(v) => v.map(|x| x.to_string()),
        ColumnData::Bit(v) => v.map(|x| x.to_string()),
        ColumnData::String(v) => v.as_ref().map(|s| s.to_string()),
        ColumnData::Numeric(v) => v.map(|n| numeric_to_display(n.value(), n.scale() as usize)),
        ColumnData::Guid(v) => v.map(|g| g.to_string()),
        ColumnData::Date(_) => chrono::NaiveDate::from_sql(cell)
            .ok()
            .flatten()
            .map(|d| d.format("%Y-%m-%d").to_string()),
        ColumnData::DateTime(_) | ColumnData::DateTime2(_) | ColumnData::SmallDateTime(_) => {
            chrono::NaiveDateTime::from_sql(cell)
                .ok()
                .flatten()
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.f").to_string())
        }
        other => seeded!(
            "mssql-scalar-text-debug-dump",
            Some(format!("{other:?}")),
            arrow_convert::cell_text(other)
        ),
    }
}

/// SQL for the CHAR(31)-joined single-column, NOT NULL UNIQUE keys of `schema.table`, PK first.
pub(crate) fn keyset_keys_sql(schema: &str, table: &str) -> String {
    format!(
        "SELECT STRING_AGG(CONVERT(nvarchar(max), col), CHAR(31)) WITHIN GROUP (ORDER BY is_pk DESC, col) FROM ( \
           SELECT col, MAX(is_pk) AS is_pk FROM ( \
             SELECT MIN(c.name) AS col, MAX(CONVERT(int, i.is_primary_key)) AS is_pk \
             FROM sys.indexes i \
             JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0 \
             JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
             JOIN sys.objects o ON o.object_id = i.object_id \
             JOIN sys.schemas s ON s.schema_id = o.schema_id \
             WHERE i.is_unique = 1 AND i.has_filter = 0 AND i.is_disabled = 0 \
               AND s.name = N'{}' AND o.name = N'{}' \
             GROUP BY i.object_id, i.index_id \
             HAVING COUNT(*) = 1 AND MAX(CONVERT(int, c.is_nullable)) = 0 \
           ) per_index GROUP BY col \
         ) deduped",
        schema.replace('\'', "''"),
        table.replace('\'', "''")
    )
}

/// SQL for the CHAR(31)-joined `datetime2` / `datetimeoffset` columns of `schema.table` declared finer than a microsecond, through an alias type too.
pub(crate) fn finer_than_microsecond_sql(schema: &str, table: &str) -> String {
    let (types_join, type_name) = seeded!(
        "keyset-key-alias-type-by-name",
        (
            "JOIN sys.types t ON t.user_type_id = c.user_type_id ",
            "t.name"
        ),
        ("", "TYPE_NAME(c.system_type_id)")
    );
    format!(
        "SELECT STRING_AGG(CONVERT(nvarchar(max), c.name), CHAR(31)) FROM sys.columns c \
         {types_join}JOIN sys.objects o ON o.object_id = c.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE s.name = N'{}' AND o.name = N'{}' \
           AND {type_name} IN ('datetime2', 'datetimeoffset') AND c.scale > {}",
        schema.replace('\'', "''"),
        table.replace('\'', "''"),
        crate::source::keyset_key::MICROSECOND_DIGITS
    )
}

/// SQL for the CHAR(31)-joined `datetime2` / `datetimeoffset` columns a query's first result set types finer than a microsecond, through an alias type too.
pub(crate) fn finer_than_microsecond_in_query_sql(query: &str) -> String {
    format!(
        "SELECT STRING_AGG(CONVERT(nvarchar(max), name), CHAR(31)) \
         FROM sys.dm_exec_describe_first_result_set(N'{}', NULL, 0) \
         WHERE system_type_id IN (42, 43) AND scale > {}",
        query.replace('\'', "''"),
        crate::source::keyset_key::MICROSECOND_DIGITS
    )
}

/// The `datetime2` / `datetimeoffset` columns of `schema.table` declared finer than a microsecond, as the catalog fact the keyset seam reads.
pub(crate) fn finer_than_microsecond_columns(
    src: &mut MssqlSource,
    schema: &str,
    table: &str,
) -> Result<Vec<(String, crate::source::keyset_key::Wider)>> {
    Ok(src
        .query_scalar(&finer_than_microsecond_sql(schema, table))?
        .map(|s| split_char31(&s))
        .unwrap_or_default()
        .into_iter()
        .map(|c| (c, crate::source::keyset_key::Wider::FinerTimestamp))
        .collect())
}

/// Splits a CHAR(31)-joined catalog list into its non-empty members.
fn split_char31(joined: &str) -> Vec<String> {
    joined
        .split('\u{1f}')
        .filter(|c| !c.is_empty())
        .map(str::to_string)
        .collect()
}

/// SQL for the column of `schema.table`'s primary key when that key is exactly one integer-family column.
pub(crate) fn single_int_pk_sql(schema: &str, table: &str) -> String {
    format!(
        "SELECT MIN(c.name) FROM sys.indexes i \
         JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0 \
         JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
         JOIN sys.types t ON t.user_type_id = c.user_type_id \
         JOIN sys.objects o ON o.object_id = i.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE i.is_primary_key = 1 AND s.name = N'{}' AND o.name = N'{}' \
         GROUP BY i.object_id, i.index_id \
         HAVING COUNT(*) = 1 AND MIN(t.name) IN ('tinyint', 'smallint', 'int', 'bigint')",
        schema.replace('\'', "''"),
        table.replace('\'', "''")
    )
}

/// Probe `sys.*` for the stats chunked-mode planning needs (ADR-0015 seam).
fn introspect_mssql_on(src: &mut MssqlSource, qualified_table: &str) -> Result<TableIntrospection> {
    let (schema, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => ("dbo".to_string(), qualified_table.to_string()),
    };

    let count_sql = format!(
        "SELECT SUM(p.rows) FROM sys.partitions p \
         JOIN sys.objects o ON o.object_id = p.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE s.name = N'{}' AND o.name = N'{}' AND p.index_id IN (0,1)",
        schema.replace('\'', "''"),
        table.replace('\'', "''")
    );
    let row_estimate = src
        .query_scalar(&count_sql)?
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    let keyset_sql = keyset_keys_sql(&schema, &table);
    let keyset_keys: Vec<String> = src
        .query_scalar(&keyset_sql)?
        .map(|s| split_char31(&s))
        .unwrap_or_default();

    let finer_than_microsecond = finer_than_microsecond_columns(src, &schema, &table)?;

    let single_int_pk = src.query_scalar(&single_int_pk_sql(&schema, &table))?;

    let int_sql = format!(
        "SELECT STRING_AGG(CONVERT(nvarchar(max), c.name), CHAR(31)) FROM sys.columns c \
         JOIN sys.types t ON t.user_type_id = c.user_type_id \
         JOIN sys.objects o ON o.object_id = c.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE s.name = N'{}' AND o.name = N'{}' \
           AND t.name IN ('tinyint', 'smallint', 'int', 'bigint')",
        schema.replace('\'', "''"),
        table.replace('\'', "''")
    );
    let int_columns: Vec<String> = src
        .query_scalar(&int_sql)?
        .map(|s| {
            s.split('\u{1f}')
                .filter(|c| !c.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    let mut introspection = TableIntrospection {
        single_int_pk,
        keyset_keys,
        row_estimate,
        avg_row_bytes: None,
        int_columns,
        ..Default::default()
    };
    introspection.declared_wider = finer_than_microsecond;
    Ok(introspection)
}

#[cfg(test)]
mod tls_posture_tests {
    use super::mssql_trusts_cert_without_verify;
    use crate::config::{TlsConfig, TlsMode};

    fn cfg(mode: TlsMode, accept: bool) -> TlsConfig {
        TlsConfig {
            mode,
            ca_file: None,
            accept_invalid_certs: accept,
            ..Default::default()
        }
    }

    #[test]
    fn incremental_read_isolation_prefers_rcsi_then_snapshot_then_locking() {
        use super::IncrementalReadIsolation::*;
        use super::incremental_read_isolation;
        assert_eq!(incremental_read_isolation(0, true), StatementSnapshot);
        assert_eq!(incremental_read_isolation(1, true), StatementSnapshot);
        assert_eq!(incremental_read_isolation(1, false), Snapshot);
        assert_eq!(incremental_read_isolation(0, false), Locking);
        assert_eq!(
            incremental_read_isolation(2, false),
            Locking,
            "IN_TRANSITION_TO_OFF"
        );
        assert_eq!(
            incremental_read_isolation(3, false),
            Locking,
            "IN_TRANSITION_TO_ON"
        );
    }

    #[test]
    fn locking_read_warning_quotes_the_database_identifier() {
        let w = super::locking_incremental_read_warning("a]b");
        assert!(
            w.contains("`ALTER DATABASE [a]]b] SET ALLOW_SNAPSHOT_ISOLATION ON`"),
            "{w}"
        );
        assert!(w.contains("database 'a]b'"), "{w}");
    }

    /// Require must trust-without-verify, matching its documented contract and the three sibling engines.
    #[test]
    fn require_trusts_like_its_siblings_verify_modes_do_not() {
        assert!(
            mssql_trusts_cert_without_verify(&cfg(TlsMode::Require, false)),
            "Require = encrypt+accept-any, same as PG/MySQL/Mongo"
        );
        assert!(mssql_trusts_cert_without_verify(&cfg(
            TlsMode::Disable,
            false
        )));
        assert!(
            mssql_trusts_cert_without_verify(&cfg(TlsMode::VerifyFull, true)),
            "accept_invalid_certs forces trust regardless of mode"
        );
        assert!(!mssql_trusts_cert_without_verify(&cfg(
            TlsMode::VerifyCa,
            false
        )));
        assert!(!mssql_trusts_cert_without_verify(&cfg(
            TlsMode::VerifyFull,
            false
        )));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        catalog_decimal_to_params, parse_mssql_simple_from_table, parse_mssql_url, scalar_text,
    };

    /// Every scalar a sampler reads back is text SQL Server casts, never a `ColumnData` debug dump.
    #[test]
    fn a_scalar_cell_reads_back_as_text_the_server_casts() {
        use tiberius::ColumnData;
        use tiberius::numeric::Numeric;
        use tiberius::time::{Date, DateTime2, DateTimeOffset, Time};
        let dt2 = |days, inc, scale| DateTime2::new(Date::new(days), Time::new(inc, scale));
        let cases: Vec<(ColumnData<'static>, &str)> = vec![
            (ColumnData::I32(Some(-7)), "-7"),
            (ColumnData::I64(Some(i64::MAX)), "9223372036854775807"),
            (ColumnData::F64(Some(0.1)), "0.1"),
            (ColumnData::Bit(Some(true)), "true"),
            (ColumnData::String(Some("a'b".into())), "a'b"),
            (
                ColumnData::Numeric(Some(Numeric::new_with_scale(-5, 3))),
                "-0.005",
            ),
            (
                ColumnData::Guid(Some(tiberius::Uuid::from_bytes([0xab; 16]))),
                "abababab-abab-abab-abab-abababababab",
            ),
            (ColumnData::Date(Some(Date::new(693_595))), "1900-01-01"),
            (
                ColumnData::DateTime2(Some(dt2(693_595, 452_961_234_567, 7))),
                "1900-01-01 12:34:56.123456700",
            ),
            (
                ColumnData::DateTimeOffset(Some(DateTimeOffset::new(
                    dt2(693_595, 36_000_000_000, 6),
                    330,
                ))),
                "1900-01-01 15:30:00.000000 +05:30",
            ),
            (
                ColumnData::DateTimeOffset(Some(DateTimeOffset::new(
                    dt2(693_595, 123_456, 6),
                    -480,
                ))),
                "1899-12-31 16:00:00.123456 -08:00",
            ),
            (ColumnData::Time(Some(Time::new(1_500, 3))), "00:00:01.500"),
            (ColumnData::Binary(Some(vec![0xDE, 0xAD].into())), "dead"),
        ];
        for (cell, want) in cases {
            assert_eq!(scalar_text(&cell).as_deref(), Some(want), "{cell:?}");
        }
        assert_eq!(scalar_text(&ColumnData::DateTimeOffset(None)), None);
        assert_eq!(scalar_text(&ColumnData::I32(None)), None);
    }

    #[test]
    fn mssql_url_percent_decodes_userinfo() {
        let parsed =
            parse_mssql_url("sqlserver://dom%5Csvc:P%40ss%2Fw0rd%21@db.internal:1433/rivet")
                .expect("well-formed encoded url must parse");
        assert_eq!(parsed.user, r"dom\svc", "user must be percent-decoded");
        assert_eq!(
            parsed.password.expose(),
            "P@ss/w0rd!",
            "password must be percent-decoded, not passed as literal %-bytes"
        );
        assert_eq!(parsed.host, "db.internal");
        assert_eq!(parsed.port, 1433);
        assert_eq!(parsed.database, "rivet");
    }

    /// A bracketed IPv6 host with no port defaults to 1433, and the database is percent-decoded like the userinfo.
    #[test]
    fn mssql_url_reads_a_portless_bracketed_host_and_decodes_the_database() {
        for (url, host, port, database) in [
            ("sqlserver://u:p@[::1]/db", "[::1]", 1433, "db"),
            (
                "sqlserver://u:p@[2001:db8::1]/db",
                "[2001:db8::1]",
                1433,
                "db",
            ),
            ("sqlserver://u:p@[::1]:1444/db", "[::1]", 1444, "db"),
            ("sqlserver://u:p@h/Sales%20DB", "h", 1433, "Sales DB"),
            (
                "sqlserver://u:p@[::1]/Sales%20DB",
                "[::1]",
                1433,
                "Sales DB",
            ),
            ("mssql://u:p@h:1444/caf%C3%A9", "h", 1444, "caf\u{e9}"),
        ] {
            let p = parse_mssql_url(url).unwrap_or_else(|e| panic!("{url}: {e}"));
            assert_eq!(
                (p.host.as_str(), p.port, p.database.as_str()),
                (host, port, database),
                "{url}"
            );
        }
    }

    /// A host form other than the portless bracket reads as the legacy parser read it.
    #[test]
    fn mssql_url_host_forms_the_split_does_not_change_read_as_before() {
        for host in [
            "h",
            "h:1444",
            "10.0.0.5",
            "10.0.0.5:1444",
            "[::1]:1444",
            "[::1",
            "h:",
            "h:zz",
            "[::1]:zz",
            "h1:1,h2:2",
            ":1444",
        ] {
            let url = format!("sqlserver://u:p@{host}/db");
            let old = legacy_parse(&url).map(|o| (o.0, o.1, o.4));
            let new = parse_mssql_url(&url)
                .ok()
                .map(|n| (n.host, n.port, n.database));
            assert_eq!(old, new, "{url}");
        }
    }

    #[test]
    fn an_at_sign_after_the_authority_stays_out_of_the_host() {
        for tail in ["/rivet?application_name=svc@corp", "/rivet#f@corp"] {
            let parsed = parse_mssql_url(&format!("sqlserver://sa:pw@h1:1433{tail}"))
                .expect("a query holding @ parses");
            assert_eq!((parsed.host.as_str(), parsed.port), ("h1", 1433), "{tail}");
            assert_eq!(
                (parsed.user.as_str(), parsed.password.expose()),
                ("sa", "pw")
            );
        }
        let err = parse_mssql_url("sqlserver://h1:1433/rivet?application_name=svc@corp")
            .err()
            .expect("no userinfo is refused, not read from the query");
        assert!(err.to_string().contains("missing user@host"), "{err}");
    }

    struct Parts(String, u16, String, String, String);

    fn legacy_parse(url: &str) -> Option<Parts> {
        let rest = url
            .strip_prefix("sqlserver://")
            .or_else(|| url.strip_prefix("mssql://"))?;
        let (userinfo, hostpart) = rest.rsplit_once('@')?;
        let (user, password) = match userinfo.split_once(':') {
            Some((u, p)) => (
                crate::url_text::pct_decode(u),
                crate::url_text::pct_decode(p),
            ),
            None => (crate::url_text::pct_decode(userinfo), String::new()),
        };
        let (hostport, database) = hostpart
            .split_once('/')
            .map(|(h, d)| (h, d.to_string()))
            .unwrap_or((hostpart, String::new()));
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse::<u16>().ok()?),
            None => (hostport.to_string(), 1433),
        };
        (!database.is_empty()).then_some(Parts(host, port, user, password, database))
    }

    #[test]
    fn the_parser_reads_what_the_legacy_parser_read_unless_an_at_or_a_delimiter_moved_the_authority()
     {
        let (mut same, mut moved) = (0, 0);
        for url in crate::url_text::corpus::urls("sqlserver") {
            let (old, new) = (legacy_parse(&url), parse_mssql_url(&url).ok());
            match (old, new) {
                (Some(o), Some(n)) if !crate::url_text::corpus::at_after_authority(&url) => {
                    assert_eq!(
                        (o.0, o.1, o.2, o.3, o.4),
                        (
                            n.host,
                            n.port,
                            n.user,
                            n.password.expose().to_string(),
                            n.database
                        ),
                        "{url}"
                    );
                    same += 1;
                }
                (None, None) => same += 1,
                _ => {
                    assert!(
                        crate::url_text::corpus::at_after_authority(&url)
                            || url.contains("u:p/w@")
                            || url.contains("u:p?w@")
                            || url.contains("u:p#w@"),
                        "{url}"
                    );
                    moved += 1;
                }
            }
        }
        assert!(same > 300 && moved > 30, "{same} {moved}");
    }

    #[test]
    fn the_loopback_gate_and_the_parser_dial_the_same_host() {
        for url in [
            "sqlserver://127.0.0.1:1/x@evil.com/db",
            "sqlserver://sa:pw@127.0.0.1:1433/db?x=@evil.com",
            "sqlserver://sa:pw@127.0.0.1:1433/db#@evil.com",
        ] {
            let gate = crate::url_text::host_port_span(url);
            match parse_mssql_url(url) {
                Ok(p) => assert!(
                    gate == format!("{}:{}", p.host, p.port) || gate == p.host,
                    "{url}: gate {gate} parser {}",
                    p.host
                ),
                Err(e) => assert!(e.to_string().contains("missing user@host"), "{url}: {e}"),
            }
        }
        assert!(parse_mssql_url("sqlserver://127.0.0.1:1/x@evil.com/db").is_err());
    }

    /// The JDBC-property and ADO forms are refused as before, and the refusal.
    #[test]
    fn a_keyword_value_string_is_refused_and_its_error_redacts_the_password() {
        for (conn, said) in [
            (
                "sqlserver://127.0.0.1:1433;databaseName=app;user=sa;password=S3cr3tPw",
                "mssql url missing user@host: sqlserver://127.0.0.1:1433;databaseName=app;user=sa;password=***",
            ),
            (
                "Server=127.0.0.1,1433;Database=d;User Id=u;Password=S3cr3tPw;",
                "mssql url must start with sqlserver:// — got Server=127.0.0.1,1433;Database=d;User Id=u;Password=***;",
            ),
            (
                "Server=127.0.0.1,1433;Database=d;UID=u;PWD=S3cr3tPw;",
                "mssql url must start with sqlserver:// — got Server=127.0.0.1,1433;Database=d;UID=u;PWD=***;",
            ),
        ] {
            let Err(err) = parse_mssql_url(conn) else {
                panic!("not a sqlserver:// URL, must be refused: {conn}")
            };
            assert_eq!(crate::redact::redact_error(&err), said, "{conn}");
        }
    }

    fn parse(q: &str) -> Option<(String, String)> {
        parse_mssql_simple_from_table(q)
    }

    #[test]
    fn numeric_display_inserts_the_point_exactly() {
        use super::numeric_to_display as n;
        assert_eq!(n(12345, 2), "123.45");
        assert_eq!(n(5, 3), "0.005", "zero-pad below 1");
        assert_eq!(n(-12345, 2), "-123.45");
        assert_eq!(n(-5, 3), "-0.005");
        assert_eq!(n(42, 0), "42", "scale 0 is the bare integer");
        assert_eq!(n(0, 2), "0.00");
        assert_eq!(n(10, 1), "1.0");
    }

    /// A name in any quoting, a database prefix, a trailing `;` and a comment that mentions FROM all read as the one table.
    #[test]
    fn the_table_is_read_past_quotes_comments_and_a_database_prefix() {
        let t = |s: &str, t: &str| Some((s.to_string(), t.to_string()));
        for (q, want) in [
            (
                "SELECT a FROM [Sales].[Order Details] WITH (NOLOCK)",
                t("Sales", "Order Details"),
            ),
            ("SELECT a FROM \"s\".\"t\"", t("s", "t")),
            ("SELECT a FROM db.s.t;", t("s", "t")),
            ("SELECT a FROM srv.db.s.t", None),
            (
                "SELECT amount -- rows from the last week\nFROM t",
                t("dbo", "t"),
            ),
            ("SELECT amount\n-- taken from legacy\nFROM s.t", t("s", "t")),
            (
                "SELECT amount /* from b */ FROM t /* from c */ WHERE a = 1",
                t("dbo", "t"),
            ),
            ("SELECT a FROM t x JOIN u ON 1 = 1", None),
        ] {
            assert_eq!(parse(q), want, "{q:?}");
        }
    }

    #[test]
    fn parse_unqualified_table_defaults_to_dbo() {
        assert_eq!(
            parse("SELECT id, amount FROM transactions ORDER BY id"),
            Some(("dbo".into(), "transactions".into()))
        );
    }

    #[test]
    fn parse_schema_qualified() {
        assert_eq!(
            parse("SELECT id FROM sales.orders WHERE id > 1"),
            Some(("sales".into(), "orders".into()))
        );
    }

    #[test]
    fn parse_db_schema_table_takes_last_two() {
        assert_eq!(
            parse("SELECT * FROM mydb.sales.orders"),
            Some(("sales".into(), "orders".into()))
        );
    }

    #[test]
    fn parse_bracketed_identifiers() {
        assert_eq!(
            parse("SELECT * FROM [my schema].[order items]"),
            Some(("my schema".into(), "order items".into()))
        );
    }

    #[test]
    fn parse_table_with_alias() {
        assert_eq!(
            parse("SELECT t.id FROM transactions AS t WHERE t.x = 1"),
            Some(("dbo".into(), "transactions".into()))
        );
        assert_eq!(
            parse("SELECT t.id FROM transactions t ORDER BY t.id"),
            Some(("dbo".into(), "transactions".into()))
        );
    }

    #[test]
    fn parse_rejects_join() {
        assert_eq!(parse("SELECT * FROM a INNER JOIN b ON a.id = b.id"), None);
        assert_eq!(parse("SELECT * FROM a JOIN b ON a.id = b.id"), None);
    }

    #[test]
    fn parse_rejects_comma_list() {
        assert_eq!(parse("SELECT * FROM a, b WHERE a.id = b.id"), None);
    }

    #[test]
    fn parse_rejects_subquery_from() {
        assert_eq!(parse("SELECT * FROM (SELECT * FROM t) AS s"), None);
    }

    #[test]
    fn parse_ignores_from_inside_string_literal() {
        assert_eq!(
            parse("SELECT 'from x', amount FROM ledger WHERE note = 'paid from cash'"),
            Some(("dbo".into(), "ledger".into()))
        );
    }

    #[test]
    fn catalog_bounds_accept_well_formed_and_reject_degenerate() {
        assert_eq!(catalog_decimal_to_params(10, 2), Some((10, 2)));
        assert_eq!(catalog_decimal_to_params(38, 0), Some((38, 0)));
        assert_eq!(catalog_decimal_to_params(38, 38), Some((38, 38)));
        assert_eq!(catalog_decimal_to_params(0, 0), None);
        assert_eq!(catalog_decimal_to_params(39, 0), None);
        assert_eq!(catalog_decimal_to_params(10, 11), None);
    }
}

#[cfg(test)]
mod key_probe_sql_tests {
    use super::{
        finer_than_microsecond_in_query_sql, finer_than_microsecond_sql, keyset_keys_sql,
        single_int_pk_sql, split_char31,
    };

    /// A catalog list splits on its separator and drops empty members.
    #[test]
    fn a_char31_list_splits_into_its_non_empty_members() {
        assert_eq!(split_char31("a\u{1f}b\u{1f}"), vec!["a", "b"]);
        assert!(split_char31("").is_empty());
    }

    /// The text between the per-index WHERE and its GROUP BY — the rows a HAVING COUNT(*) sees.
    fn where_clause(sql: &str) -> &str {
        let w = sql.find("WHERE i.").expect("WHERE");
        let g = sql[w..].find("GROUP BY i.object_id").expect("GROUP BY") + w;
        &sql[w..g]
    }

    /// The query probe reads the system type under an alias, quotes the query, and compares the scale with the microsecond digits.
    #[test]
    fn the_query_sub_microsecond_probe_quotes_the_query_and_reads_the_system_type() {
        let sql = finer_than_microsecond_in_query_sql("SELECT 'a' AS x");
        assert!(sql.contains("N'SELECT ''a'' AS x'"), "{sql}");
        assert!(
            sql.contains("system_type_id IN (42, 43) AND scale > 6"),
            "{sql}"
        );
    }

    /// H24: a composite UNIQUE(a NOT NULL, b NULL) must not surface `a` as a keyset key, so the per-column filters live in HAVING where COUNT(*) still counts every key column.
    #[test]
    fn keyset_probe_counts_every_key_column_of_the_index() {
        let sql = keyset_keys_sql("dbo", "orders");
        let w = where_clause(&sql);
        assert!(!w.contains("is_nullable"), "nullable filter in WHERE: {w}");
        assert!(!w.contains("kt.name"), "type filter in WHERE: {w}");
        assert!(
            sql.contains("HAVING COUNT(*) = 1 AND MAX(CONVERT(int, c.is_nullable)) = 0"),
            "{sql}"
        );
    }

    /// A column declared through an alias type over datetime2(7) is found by the system type under it, not by the alias name.
    #[test]
    fn the_sub_microsecond_probe_reads_the_system_type_under_an_alias() {
        let sql = finer_than_microsecond_sql("dbo", "orders");
        assert!(
            sql.contains("TYPE_NAME(c.system_type_id) IN ('datetime2', 'datetimeoffset')"),
            "{sql}"
        );
        assert!(!sql.contains("sys.types"), "{sql}");
    }

    /// H25: a composite PK is one index with COUNT(*) = 2.
    #[test]
    fn single_int_pk_probe_groups_per_index_not_per_column() {
        let sql = single_int_pk_sql("dbo", "orders");
        assert!(
            sql.contains(
                "GROUP BY i.object_id, i.index_id \
                 HAVING COUNT(*) = 1 AND MIN(t.name) IN ('tinyint', 'smallint', 'int', 'bigint')"
            ),
            "{sql}"
        );
        assert!(!sql.contains("GROUP BY c.name"), "{sql}");
        assert!(!sql.contains("TOP 1"), "{sql}");
    }
}

#[cfg(test)]
mod error_code_tests {
    /// Every malformed SQL Server URL: one code, usage, exit 1.
    #[test]
    fn a_malformed_mssql_url_is_refused_by_code() {
        for url in [
            "foo://u:p@h/db",
            "sqlserver://h/db",
            "sqlserver://u:p@h:zz/db",
            "sqlserver://u:p@h",
        ] {
            let err = super::parse_mssql_url(url).err().expect(url);
            crate::error::assert_coded(&err, "RIVET_CONFIG_SOURCE_URL_INVALID", 1);
        }
    }
}
