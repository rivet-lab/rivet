//! MySQL `Source` implementation.

mod arrow_convert;
pub(crate) use arrow_convert::{time_outside_day, zero_date_counts, zero_date_warning};
pub(crate) mod cdc;
pub(crate) mod column_grants;
mod folded_name;
mod proxy;
mod statement;
#[cfg(test)]
mod statement_truth;

use std::sync::Arc;

use arrow::datatypes::Schema;
use mysql::prelude::*;
use mysql::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};

use crate::config::{SourceType, TlsConfig, TlsMode};
use crate::error::Result;
use crate::source::batch_controller::{
    AdaptiveBatchController, DEFAULT_BATCH_TARGET_MB, PROBE_BATCH_SIZE,
};
use crate::source::query::build_export_query;
use crate::tuning::SourceTuning;
use crate::types::ColumnOverrides;

#[cfg(test)]
use arrow_convert::bit_bytes_to_u64;
use arrow_convert::{
    mysql_mapping, mysql_native_type_name, mysql_schema_and_arrow_types, mysql_type_to_rivet,
    nulled_dates, rows_to_record_batch_typed,
};
use proxy::{detect_mysql_proxy_kind, warn_proxy_kind};

pub use proxy::MysqlProxyKind;

pub struct MysqlSource {
    pool: Pool,
    proxy_kind: MysqlProxyKind,
    session: std::sync::Arc<SessionPlan>,
}

/// Pool options that prevent eager pre-connection.
fn lean_pool_opts() -> PoolOpts {
    PoolOpts::default()
        .with_constraints(PoolConstraints::new(1, 100).expect("valid pool constraints"))
        .with_reset_connection(false)
}

/// Sample the FOREIGN-pressure proxy for the concurrency governor.
fn mysql_sample_foreign_pressure(pool: &Pool) -> Option<u64> {
    let mut conn = pool.get_conn().ok()?;
    let rows: Vec<(String, u64)> = conn
        .query("SHOW GLOBAL STATUS LIKE 'Innodb_log_waits'")
        .ok()?;
    if rows.is_empty() {
        return None;
    }
    Some(rows.iter().map(|(_, v)| *v).sum())
}

/// Sample an **extraction-pressure** proxy (Epic 18 C1).
fn mysql_sample_extraction_pressure(pool: &Pool) -> Option<u64> {
    let mut conn = pool.get_conn().ok()?;
    let rows: Vec<(String, u64)> = conn
        .query(
            "SHOW GLOBAL STATUS WHERE Variable_name IN \
             ('Created_tmp_disk_tables', 'Innodb_buffer_pool_wait_free')",
        )
        .ok()?;
    if rows.is_empty() {
        return None;
    }
    Some(rows.iter().map(|(_, v)| *v).sum())
}

impl MysqlSource {
    /// Build a source from an existing pool: the single place that detects the proxy kind, warns once, and wraps the pool.
    pub fn from_pool(pool: Pool) -> Self {
        Self::over(pool, false)
    }

    /// A source over `pool`; `keeps_session` when the pool hands connections back unreset (rivet's own).
    fn over(pool: Pool, keeps_session: bool) -> Self {
        let proxy_kind = detect_mysql_proxy_kind(&pool);
        Self::with_kind(pool, keeps_session, proxy_kind)
    }

    /// Connect rivet's own pool to `url`.
    fn connect_own(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        let pool = connect_pool(url, tls)?;
        let proxy_kind = seeded!(
            "mysql-proxy-probe-dials-its-own-connection",
            match dial_conn(url, tls) {
                Ok(mut probe) => proxy::detect_proxy_on_conn(&mut probe),
                Err(_) => proxy::proxy_kind_of(url, &pool),
            },
            proxy::proxy_kind_of(url, &pool)
        );
        Ok(Self::with_kind(pool, true, proxy_kind))
    }

    /// A source over `pool`, whose proxy kind is known.
    fn with_kind(pool: Pool, keeps_session: bool, proxy_kind: MysqlProxyKind) -> Self {
        warn_proxy_kind(proxy_kind);
        Self {
            pool,
            proxy_kind,
            session: std::sync::Arc::new(SessionPlan {
                keeps_session,
                ..Default::default()
            }),
        }
    }

    /// Connect with no transport security (legacy path).
    #[allow(dead_code)]
    pub fn connect(url: &str) -> Result<Self> {
        Self::connect_own(url, None)
    }

    /// Connect honoring the user's [`TlsConfig`].
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        Self::connect_own(url, tls)
    }

    /// Expose the proxy classification for diagnostic tools (preflight, integration tests).
    #[allow(dead_code)]
    pub fn proxy_kind(&self) -> MysqlProxyKind {
        self.proxy_kind
    }
    /// The first cell of `sql`'s first row under `max_execution_time` of `statement_timeout_s` seconds (0 = the server's).
    fn scalar_within(&mut self, sql: &str, statement_timeout_s: u64) -> Result<Option<String>> {
        let mut conn = self.pool.get_conn()?;
        let max_exec_ms = max_execution_ms(statement_timeout_s);
        let mut guard = MysqlSessionGuard::apply(&mut conn, &self.session, max_exec_ms)?;
        let row: Option<mysql::Row> = guard.conn().query_first(sql).map_err(|e| {
            if statement_timeout_s > 0 && is_statement_timeout(&e) {
                super::StatementDurationTimeout::mysql(statement_timeout_s).into()
            } else {
                anyhow::Error::new(e)
            }
        })?;
        match row {
            Some(r) => {
                let val: Option<mysql::Value> = r.get(0);
                match val {
                    Some(mysql::Value::Bytes(b)) => {
                        Ok(Some(String::from_utf8_lossy(&b).into_owned()))
                    }
                    Some(mysql::Value::Int(v)) => Ok(Some(v.to_string())),
                    Some(mysql::Value::UInt(v)) => Ok(Some(v.to_string())),
                    Some(mysql::Value::Float(v)) => Ok(Some(v.to_string())),
                    Some(mysql::Value::Double(v)) => Ok(Some(v.to_string())),
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        }
    }
}

/// Build a MySQL connection pool honoring the configured TLS policy.
pub(crate) fn connect_pool(url: &str, tls: Option<&TlsConfig>) -> Result<Pool> {
    crate::source::require_tls_or_loopback(url, tls)?;
    let opts = with_tls(
        session_opts(OptsBuilder::from_opts(Opts::from_url(url)?)).pool_opts(lean_pool_opts()),
        tls,
    );
    let pool = Pool::new(opts).map_err(|e| crate::source::describe_connect_error(url, e.into()))?;
    pool.get_conn()
        .map_err(|e| crate::source::describe_connect_error(url, e.into()))?;
    Ok(pool)
}

/// Dial one MySQL `Conn` under the same TLS gate and host:port-naming error as [`connect_pool`].
pub(crate) fn dial_conn(url: &str, tls: Option<&TlsConfig>) -> Result<Conn> {
    crate::source::require_tls_or_loopback(url, tls)?;
    let opts = with_tls(
        session_opts(OptsBuilder::from_opts(Opts::from_url(url)?)),
        tls,
    );
    Conn::new(opts).map_err(|e| crate::source::describe_connect_error(url, e.into()))
}

/// The largest packet the driver splits its own writes at, given so it does not ask the server.
const CLIENT_MAX_ALLOWED_PACKET: usize = 64 * 1024 * 1024;

/// Options under which the driver's handshake reads no `@@` variable (`@@socket`, `@@max_allowed_packet`).
fn session_opts(builder: OptsBuilder) -> OptsBuilder {
    builder
        .prefer_socket(false)
        .max_allowed_packet(Some(CLIENT_MAX_ALLOWED_PACKET))
}

/// Apply an enforced [`TlsConfig`] to the driver's SSL options; anything else stays plaintext.
fn with_tls(builder: OptsBuilder, tls: Option<&TlsConfig>) -> Opts {
    match tls {
        Some(cfg) if cfg.mode.is_enforced() => {
            Opts::from(builder.ssl_opts(Some(build_mysql_ssl_opts(cfg))))
        }
        _ => Opts::from(builder),
    }
}

/// Threshold above which `AVG_ROW_LENGTH` is treated as inflated by InnoDB BLOB overflow pages and divided down.
const INNODB_BLOB_OVERFLOW_THRESHOLD_BYTES: i64 = 8 * 1024;

/// Empirical divisor for InnoDB BLOB-page inflation.
const INNODB_BLOB_OVERFLOW_DIVISOR: i64 = 3;

/// Apply the InnoDB BLOB-overflow correction to a raw `AVG_ROW_LENGTH` value.
fn correct_innodb_avg_row_length(raw_bytes: i64) -> i64 {
    if raw_bytes > INNODB_BLOB_OVERFLOW_THRESHOLD_BYTES {
        (raw_bytes / INNODB_BLOB_OVERFLOW_DIVISOR).max(INNODB_BLOB_OVERFLOW_THRESHOLD_BYTES / 2)
    } else {
        raw_bytes
    }
}

/// The delivered type of every column of `query`, read from the server's description of the statement.
pub(crate) fn type_mappings_on(
    conn: &mut mysql::PooledConn,
    query: &str,
    column_overrides: &ColumnOverrides,
) -> Result<Vec<crate::types::TypeMapping>> {
    let wrapped = format!("SELECT * FROM ({}) AS _rivet_type_probe LIMIT 0", query);
    let result = conn.exec_iter(&wrapped, ())?;
    let columns = result.columns().as_ref().to_vec();
    drop(result);
    columns
        .iter()
        .map(|col| {
            let native = mysql_native_type_name(col);
            let rivet = crate::types::resolve_column(
                crate::types::override_table::Engine::Mysql,
                column_overrides,
                col.name_str().as_ref(),
                &native,
                || mysql_type_to_rivet(col),
            )?;
            let source = crate::types::SourceColumn::simple(col.name_str().as_ref(), native, true);
            Ok(mysql_mapping(&source, rivet))
        })
        .collect()
}

/// Whether `column` of `query` may hold NULL: the `NOT NULL` flag the server sets on the described column.
pub(crate) fn column_nullable_on(
    conn: &mut mysql::PooledConn,
    query: &str,
    column: &str,
) -> Result<Option<bool>> {
    let wrapped = format!("SELECT * FROM ({}) AS _rivet_type_probe LIMIT 0", query);
    let result = conn.exec_iter(&wrapped, ())?;
    let columns = result.columns().as_ref().to_vec();
    drop(result);
    Ok(columns.iter().find(|c| c.name_str() == column).map(|c| {
        !c.flags()
            .contains(mysql::consts::ColumnFlags::NOT_NULL_FLAG)
    }))
}

/// Probe `information_schema` for stats chunked-mode planning needs.
fn introspect_mysql_on(
    conn: &mut mysql::PooledConn,
    qualified_table: &str,
) -> Result<crate::source::TableIntrospection> {
    let default_db: Option<String> = conn.query_first("SELECT DATABASE()")?;
    let default_db = default_db.unwrap_or_default();

    let (schema, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => (default_db, qualified_table.to_string()),
    };

    let row_stats: Option<(i64, i64, i64)> = conn.exec_first(
        "SELECT CAST(IFNULL(TABLE_ROWS, 0) AS SIGNED), \
                CAST(IFNULL(AVG_ROW_LENGTH, 0) AS SIGNED), \
                CAST(IFNULL(DATA_LENGTH, 0) AS SIGNED) \
         FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        (&schema, &table),
    )?;
    let (row_estimate, avg_row_bytes) = match row_stats {
        Some((rows, avg, data_len)) => mysql_row_stats(rows, avg, data_len),
        None => (0, None),
    };

    let pk_first: Option<(String,)> = conn.exec_first(
        "SELECT COLUMN_NAME \
         FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' AND SEQ_IN_INDEX = 1",
        (&schema, &table),
    )?;
    let single_int_pk = if let Some((col,)) = pk_first {
        let composite: Option<(String,)> = conn.exec_first(
            "SELECT COLUMN_NAME FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' AND SEQ_IN_INDEX = 2 \
             LIMIT 1",
            (&schema, &table),
        )?;
        if composite.is_some() {
            log::debug!(
                "introspect_mysql_table: composite PK on {schema}.{table} — skipping auto-resolve"
            );
            None
        } else {
            let type_row: Option<(String,)> = conn.exec_first(
                "SELECT DATA_TYPE FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                (&schema, &table, &col),
            )?;
            match type_row.map(|(t,)| t.to_ascii_lowercase()) {
                Some(t) if is_mysql_integer_type(&t) => Some(col),
                Some(t) => {
                    log::debug!(
                        "introspect_mysql_table: PK '{col}' on {schema}.{table} has non-int type '{t}' — skipping auto-resolve"
                    );
                    None
                }
                None => None,
            }
        }
    } else {
        None
    };

    let keyset_rows: Vec<(String, String, String)> = conn.exec(
        "SELECT s.COLUMN_NAME, s.INDEX_NAME, c.IS_NULLABLE \
         FROM information_schema.STATISTICS s \
         JOIN information_schema.COLUMNS c \
           ON c.TABLE_SCHEMA = s.TABLE_SCHEMA AND c.TABLE_NAME = s.TABLE_NAME \
              AND c.COLUMN_NAME = s.COLUMN_NAME \
         WHERE s.TABLE_SCHEMA = ? AND s.TABLE_NAME = ? AND s.NON_UNIQUE = 0 \
           AND s.SEQ_IN_INDEX = 1 \
           AND NOT EXISTS ( \
             SELECT 1 FROM information_schema.STATISTICS s2 \
             WHERE s2.TABLE_SCHEMA = s.TABLE_SCHEMA AND s2.TABLE_NAME = s.TABLE_NAME \
               AND s2.INDEX_NAME = s.INDEX_NAME AND s2.SEQ_IN_INDEX = 2)",
        (&schema, &table),
    )?;
    let keyset_keys = super::keyset_keys_primary_first(&mysql_keyset_columns(keyset_rows));

    let int_columns: Vec<String> = conn
        .exec::<(String,), _, _>(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
               AND DATA_TYPE IN ('tinyint', 'smallint', 'mediumint', 'int', 'bigint')",
            (&schema, &table),
        )?
        .into_iter()
        .map(|(c,)| c)
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

/// The row count and the bytes one row takes, from a table's `information_schema.TABLES` row.
fn mysql_row_stats(rows: i64, avg: i64, data_len: i64) -> (i64, Option<i64>) {
    let row_count = rows.max(0);
    let raw_per_row = if avg > 0 {
        Some(avg)
    } else if row_count > 0 {
        Some(data_len / row_count)
    } else {
        None
    };
    let per_row = raw_per_row.map(correct_innodb_avg_row_length);
    (row_count, per_row.filter(|b| *b > 0))
}

/// Whether a lower-cased MySQL `DATA_TYPE` is of the integer family, the only one a range chunk may slice.
fn is_mysql_integer_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
    )
}

/// The NOT NULL columns among (column, index name, IS_NULLABLE) rows, each with whether its index is the primary key.
fn mysql_keyset_columns(rows: Vec<(String, String, String)>) -> Vec<(String, bool)> {
    rows.into_iter()
        .filter(|(_, _, is_nullable)| is_nullable.eq_ignore_ascii_case("NO"))
        .map(|(col, index_name, _)| (col, index_name == "PRIMARY"))
        .collect()
}

/// `max_execution_time` in milliseconds for a statement timeout in seconds; none when the timeout is off.
fn max_execution_ms(statement_timeout_s: u64) -> Option<u64> {
    (statement_timeout_s > 0).then(|| statement_timeout_s * 1000)
}

fn build_mysql_ssl_opts(cfg: &TlsConfig) -> SslOpts {
    let mut ssl = SslOpts::default();
    if let Some(path) = &cfg.ca_file {
        ssl = ssl.with_root_cert_path(Some(std::path::PathBuf::from(path)));
    }
    match cfg.mode {
        TlsMode::Require => {
            ssl = ssl
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true);
        }
        TlsMode::VerifyCa => {
            ssl = ssl.with_danger_skip_domain_validation(true);
        }
        TlsMode::VerifyFull => {}
        TlsMode::Disable => {}
    }
    if cfg.accept_invalid_certs {
        ssl = ssl.with_danger_accept_invalid_certs(true);
    }
    if cfg.accept_invalid_hostnames {
        ssl = ssl.with_danger_skip_domain_validation(true);
    }
    ssl
}

/// Refuse a session whose clock is not UTC after rivet set `time_zone = '+00:00'`.
fn refuse_a_session_off_utc(zone: &str, offset_s: i64) -> Result<()> {
    if offset_s == 0 || seeded!("mysql-session-off-utc-read", !zone.is_empty(), false) {
        return Ok(());
    }
    let hours = offset_s as f64 / 3600.0;
    crate::rivet_bail!(
        crate::error::codes::SOURCE_SESSION_TIME_ZONE_NOT_APPLIED,
        "mysql: after `SET time_zone = '+00:00'` the session that reads is at `{zone}`, \
         {hours:+} h from UTC: the SET did not reach the backend connection (a proxy such as \
         ProxySQL answered it without forwarding it, or sent the read to another backend), so \
         every TIMESTAMP would be written {hours:+} h off as UTC. Nothing was read. Set \
         `default_time_zone = '+00:00'` on the server, let the proxy keep rivet's session \
         variables on one backend (ProxySQL: session-variable tracking, no multiplexing for \
         rivet's user), or connect rivet to MySQL directly."
    )
}

#[cfg(test)]
mod session_time_zone_tests {
    #[test]
    fn a_session_off_utc_is_refused_by_code_and_a_utc_one_reads() {
        super::refuse_a_session_off_utc("+00:00", 0).unwrap();
        super::refuse_a_session_off_utc("SYSTEM", 0).unwrap();
        for (zone, offset, hours) in [
            ("SYSTEM", 10_800, "+3 h"),
            ("+03:00", 10_800, "+3 h"),
            ("-05:30", -19_800, "-5.5 h"),
        ] {
            let err = super::refuse_a_session_off_utc(zone, offset).unwrap_err();
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_SOURCE_SESSION_TIME_ZONE_NOT_APPLIED")
            );
            assert_eq!(crate::error::classify_exit(&err), 5);
            let text = format!("{err:#}");
            assert!(text.contains(hours), "{text}");
            assert!(
                text.contains(zone) && text.contains("default_time_zone"),
                "{text}"
            );
        }
    }
}

/// The server defaults a session is derived from, read once per pool.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerDefaults {
    sql_mode: String,
    max_exec_ms: u64,
}

/// Reads the server's `sql_mode` and `max_execution_time` without a `@@` variable, whose read makes ProxySQL stop multiplexing the session.
const SERVER_DEFAULTS: &str =
    "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('sql_mode', 'max_execution_time')";

/// The session clock's offset from UTC, read without a session variable so a multiplexing proxy keeps multiplexing.
const SESSION_OFFSET: &str = "SELECT TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), NOW(6))";

/// The session settings of one pool's connections.
#[derive(Debug, Default)]
struct SessionPlan {
    /// Whether the pool keeps a connection's session when it is handed back (rivet's own pool).
    keeps_session: bool,
    defaults: std::sync::OnceLock<ServerDefaults>,
    given: std::sync::Mutex<std::collections::HashMap<u32, u64>>,
}

impl SessionPlan {
    /// The `max_execution_time` connection `id` still holds, or `None` when it must be set from scratch.
    fn given(&self, id: u32) -> Option<u64> {
        if seeded!("mysql-session-set-per-chunk", true, !self.keeps_session) {
            return None;
        }
        self.given
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .copied()
    }

    /// Record that connection `id` was given `max_exec_ms`.
    fn record(&self, id: u32, max_exec_ms: u64) {
        self.given
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, max_exec_ms);
    }

    /// The server defaults, read on `conn` the first time they are asked for.
    fn defaults(&self, conn: &mut mysql::PooledConn) -> Result<ServerDefaults> {
        if let Some(d) = self.defaults.get() {
            return Ok(d.clone());
        }
        let rows: Vec<(String, String)> = conn.query(SERVER_DEFAULTS)?;
        let value = |name: &str| {
            rows.iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };
        let read = ServerDefaults {
            sql_mode: value("sql_mode").unwrap_or_default(),
            max_exec_ms: value("max_execution_time")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        };
        Ok(self.defaults.get_or_init(|| read).clone())
    }
}

/// The server's `sql_mode` without `NO_BACKSLASH_ESCAPES`, which the inline keyset boundary literals need off.
fn wanted_sql_mode(global: &str) -> String {
    global
        .split(',')
        .map(str::trim)
        .filter(|m| !m.is_empty() && !m.eq_ignore_ascii_case("NO_BACKSLASH_ESCAPES"))
        .collect::<Vec<_>>()
        .join(",")
}

/// The literal SETs a connection needs: all three the first time (`given` is `None`), else only `max_execution_time` when this read wants another one than the connection was given.
fn session_sets(
    defaults: &ServerDefaults,
    given: Option<u64>,
    max_exec_ms: Option<u64>,
) -> Vec<String> {
    let max = max_exec_ms.unwrap_or(defaults.max_exec_ms);
    let mut sets = Vec::new();
    if given.is_none() {
        sets.push("SET time_zone = '+00:00'".to_string());
        sets.push(format!(
            "SET SESSION sql_mode = '{}'",
            wanted_sql_mode(&defaults.sql_mode)
                .replace('\\', "\\\\")
                .replace('\'', "''")
        ));
    }
    if given != Some(max) {
        sets.push(format!("SET SESSION max_execution_time = {max}"));
    }
    sets
}

/// A pooled connection in the session a read needs.
struct MysqlSessionGuard<'a> {
    conn: &'a mut mysql::PooledConn,
}

impl<'a> MysqlSessionGuard<'a> {
    /// Bring the session to what a read needs and refuse one whose clock is not UTC.
    fn apply(
        conn: &'a mut mysql::PooledConn,
        plan: &SessionPlan,
        max_exec_ms: Option<u64>,
    ) -> Result<Self> {
        let defaults = plan.defaults(conn)?;
        let id = conn.connection_id();
        let sets = session_sets(&defaults, plan.given(id), max_exec_ms);
        for set in &sets {
            conn.query_drop(set)?;
        }
        if !sets.is_empty() {
            plan.record(id, max_exec_ms.unwrap_or(defaults.max_exec_ms));
        }
        let offset_s: i64 = conn.query_first(SESSION_OFFSET)?.unwrap_or(0);
        if offset_s != 0 {
            let zone: Option<String> = conn.query_first("SELECT @@session.time_zone")?;
            refuse_a_session_off_utc(zone.as_deref().unwrap_or(""), offset_s)?;
        }
        Ok(Self { conn })
    }

    fn conn(&mut self) -> &mut mysql::PooledConn {
        self.conn
    }
}

#[cfg(test)]
mod session_set_tests {
    use super::*;

    fn defaults() -> ServerDefaults {
        ServerDefaults {
            sql_mode: "STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES".into(),
            max_exec_ms: 0,
        }
    }

    /// A new connection gets three literal SETs; a reused one none, or one when its timeout changes.
    #[test]
    fn a_connection_is_set_once_by_literals_and_again_only_for_another_timeout() {
        assert_eq!(
            session_sets(&defaults(), None, Some(300_000)),
            [
                "SET time_zone = '+00:00'",
                "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'",
                "SET SESSION max_execution_time = 300000",
            ]
        );
        assert!(session_sets(&defaults(), Some(300_000), Some(300_000)).is_empty());
        assert_eq!(
            session_sets(&defaults(), Some(300_000), None),
            ["SET SESSION max_execution_time = 0"],
            "an export without a timeout gets the server's back, as a literal"
        );
        assert!(session_sets(&defaults(), Some(0), None).is_empty());
        for sql in session_sets(&defaults(), None, Some(1)) {
            assert!(!sql.contains('@'), "a literal, never an expression: {sql}");
        }
    }

    /// The driver's options read no `@@` at the handshake.
    #[test]
    fn the_session_options_ask_the_server_for_no_variable() {
        let opts = Opts::from(session_opts(OptsBuilder::new()));
        assert!(!opts.get_prefer_socket());
        assert_eq!(opts.get_max_allowed_packet(), Some(64 * 1024 * 1024));
        assert_eq!(CLIENT_MAX_ALLOWED_PACKET, 67_108_864);
    }

    /// A source over rivet's own pool keeps each connection's settings, so a reused connection is not set again.
    #[test]
    fn only_a_pool_that_keeps_sessions_skips_the_sets_on_a_reused_connection() {
        let pool = || {
            let lazy = PoolOpts::default()
                .with_constraints(PoolConstraints::new(0, 1).expect("valid pool constraints"));
            Pool::new(
                OptsBuilder::new()
                    .ip_or_hostname(Some("127.0.0.1"))
                    .tcp_port(1)
                    .pool_opts(lazy),
            )
            .expect("a pool with no minimum dials nothing")
        };
        for keeps in [true, false] {
            let source = MysqlSource::with_kind(pool(), keeps, MysqlProxyKind::Direct);
            let plan = &source.session;
            assert_eq!(plan.keeps_session, keeps);
            assert_eq!(plan.given(7), None, "a new connection is set");
            plan.record(7, 300_000);
            let again = session_sets(&defaults(), plan.given(7), Some(300_000));
            assert_eq!(again.is_empty(), keeps, "keeps_session={keeps}: {again:?}");
        }
    }

    #[test]
    fn the_wanted_sql_mode_is_the_servers_without_no_backslash_escapes() {
        assert_eq!(wanted_sql_mode("NO_BACKSLASH_ESCAPES"), "");
        assert_eq!(
            wanted_sql_mode("ANSI_QUOTES,no_backslash_escapes,STRICT_ALL_TABLES"),
            "ANSI_QUOTES,STRICT_ALL_TABLES"
        );
        assert_eq!(wanted_sql_mode(""), "");
    }
}

/// Execute the MySQL query and stream results to sink.
fn is_statement_timeout(e: &mysql::Error) -> bool {
    if let mysql::Error::MySqlError(db) = e
        && (db.code == 3024 || db.code == 1969)
    {
        return true;
    }
    let msg = e.to_string();
    msg.contains("max_execution_time")
        || msg.contains("max_statement_time")
        || msg.contains("maximum statement execution time exceeded")
}

fn mysql_run_export(
    conn: &mut mysql::PooledConn,
    sample_pool: Option<Pool>,
    sql: &str,
    cursor_param: Option<&str>,
    tuning: &SourceTuning,
    column_overrides: &ColumnOverrides,
    sink: &mut dyn super::BatchSink,
) -> Result<usize> {
    let wrap_timeout = |e: mysql::Error| -> anyhow::Error {
        if tuning.statement_timeout_s > 0 && is_statement_timeout(&e) {
            super::StatementDurationTimeout::mysql(tuning.statement_timeout_s).into()
        } else {
            anyhow::Error::new(e)
        }
    };

    let mut result = match cursor_param {
        Some(val) => conn.exec_iter(sql, (val,)).map_err(&wrap_timeout)?,
        None => conn.exec_iter(sql, ()).map_err(&wrap_timeout)?,
    };
    let columns = result.columns().as_ref().to_vec();

    let (schema, arrow_types) = mysql_schema_and_arrow_types(&columns, column_overrides)?;
    let schema = Arc::new(schema);

    sink.on_schema(schema.clone())?;

    let configured_batch_size = tuning.effective_batch_size(Some(&schema));
    let mut ctl = AdaptiveBatchController::new(tuning, configured_batch_size);
    ctl.seed_pressure(if tuning.adaptive {
        sample_pool
            .as_ref()
            .and_then(mysql_sample_extraction_pressure)
    } else {
        None
    });
    let row_set = result.iter().ok_or_else(|| {
        anyhow::Error::new(crate::error::CodedError::new(
            crate::error::codes::INTERNAL_SOURCE_INVARIANT,
            "no result set".to_string(),
        ))
    })?;
    let mut row_buf: Vec<mysql::Row> = Vec::with_capacity(ctl.target());
    let mut total_rows: usize = 0;
    let mut memory_cap_applied = false;
    let max_value_bytes = tuning.max_value_bytes();

    for row_result in row_set {
        let row = row_result.map_err(&wrap_timeout)?;
        row_buf.push(row);

        if row_buf.len() >= ctl.target() {
            total_rows += row_buf.len();
            let batch =
                rows_to_record_batch_typed(&schema, &arrow_types, &row_buf, max_value_bytes)?;
            for (column, n) in nulled_dates(&schema, &arrow_types, &row_buf) {
                sink.note_nulled(&column, n);
            }
            let batch_rows = row_buf.len();
            row_buf.clear();

            if !memory_cap_applied && batch_rows > 0 {
                let arrow_bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
                let arrow_per_row = (arrow_bytes / batch_rows).max(64);
                let target_mb = tuning
                    .batch_size_memory_mb
                    .unwrap_or(DEFAULT_BATCH_TARGET_MB);
                let safe = ((target_mb * 1024 * 1024) / arrow_per_row).max(PROBE_BATCH_SIZE);
                if let Some(new) = ctl.apply_memory_cap(safe) {
                    log::info!(
                        "MySQL row_buf cap: arrow≈{} B/row, target={} MB → batch_size → {} (configured={})",
                        arrow_per_row,
                        target_mb,
                        new,
                        configured_batch_size
                    );
                    row_buf.reserve(new.saturating_sub(row_buf.capacity()));
                }
                memory_cap_applied = true;
            }

            sink.on_batch(&batch)?;

            if let Some((new, under_pressure)) = ctl.after_batch(|| {
                sample_pool
                    .as_ref()
                    .and_then(mysql_sample_extraction_pressure)
            }) {
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
            ctl.throttle(batch.num_rows());
        }
    }

    if !row_buf.is_empty() {
        total_rows += row_buf.len();
        let batch = rows_to_record_batch_typed(&schema, &arrow_types, &row_buf, max_value_bytes)?;
        for (column, n) in nulled_dates(&schema, &arrow_types, &row_buf) {
            sink.note_nulled(&column, n);
        }
        sink.on_batch(&batch)?;
    }

    drop(result);
    Ok(total_rows)
}

impl super::Source for MysqlSource {
    fn introspect_for_chunking(
        &mut self,
        qualified_table: &str,
    ) -> Result<crate::source::TableIntrospection> {
        introspect_mysql_on(&mut self.pool.get_conn()?, qualified_table)
    }

    fn readable_columns(
        &mut self,
        table: &str,
    ) -> Result<Option<crate::source::cdc::ReadableColumns>> {
        let tables = [table.to_string()];
        let mut conn = self.pool.get_conn()?;
        let mut readable = cdc::MysqlChangeStream::readable_for_batch(conn.as_mut(), &tables)?;
        Ok(readable.remove(table))
    }

    fn export(
        &mut self,
        request: &super::ExportRequest<'_>,
        sink: &mut dyn super::BatchSink,
    ) -> Result<()> {
        let built = build_export_query(request, SourceType::Mysql)?;
        log::debug!(
            "executing query (connection={}): {}",
            self.proxy_kind.log_label(),
            built.sql
        );

        let mut conn = self.pool.get_conn()?;

        let max_exec_ms = max_execution_ms(request.tuning.statement_timeout_s);
        let mut guard = MysqlSessionGuard::apply(&mut conn, &self.session, max_exec_ms)?;

        let sample_pool = if request.tuning.adaptive {
            Some(self.pool.clone())
        } else {
            None
        };
        let result = mysql_run_export(
            guard.conn(),
            sample_pool,
            &built.sql,
            built.cursor_param.as_deref(),
            request.tuning,
            request.column_overrides,
            sink,
        );

        let total_rows = result?;
        if total_rows == 0 {
            sink.on_schema(Arc::new(Schema::empty()))?;
        }
        log::info!("total: {} rows", total_rows);
        Ok(())
    }

    fn primary_key(&mut self, table: &str) -> Result<Option<Vec<String>>> {
        let bare = table.replace('`', "");
        let (schema, name) = match bare.split_once('.') {
            Some((s, t)) => (format!("'{}'", s.replace('\'', "''")), t.to_string()),
            None => ("DATABASE()".to_string(), bare.clone()),
        };
        let sql = format!(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
             WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = {schema} \
             AND TABLE_NAME = '{}' ORDER BY ORDINAL_POSITION",
            name.replace('\'', "''")
        );
        let mut conn = self.pool.get_conn()?;
        let mut guard = MysqlSessionGuard::apply(&mut conn, &self.session, None)?;
        let cols: Vec<String> = guard.conn().query(sql)?;
        Ok(crate::source::split_key_list(Some(cols.join("\u{1f}"))))
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
    ) -> Result<Vec<crate::types::TypeMapping>> {
        let mut conn = self.pool.get_conn()?;
        type_mappings_on(&mut conn, query, column_overrides)
    }

    fn column_nullable(&mut self, query: &str, column: &str) -> Result<Option<bool>> {
        let mut conn = self.pool.get_conn()?;
        column_nullable_on(&mut conn, query, column)
    }

    /// Governor FOREIGN-pressure proxy: `Innodb_log_waits` (redo-write pressure a read-only export cannot move).
    fn sample_governor_pressure(&mut self) -> Option<u64> {
        mysql_sample_foreign_pressure(&self.pool)
    }

    /// Snapshot the broader source-harm counters from `SHOW GLOBAL STATUS`.
    fn harm_counters(&mut self) -> Option<Vec<(String, i64)>> {
        let pool = &self.pool;
        let mut conn = pool.get_conn().ok()?;
        let rows: Vec<(String, i64)> = conn
            .query(
                "SHOW GLOBAL STATUS WHERE Variable_name IN \
             ('Innodb_rows_read', 'Innodb_buffer_pool_reads', 'Created_tmp_disk_tables', \
              'Handler_read_rnd_next', 'Innodb_row_lock_waits', 'Innodb_row_lock_time')",
            )
            .ok()?;
        if rows.is_empty() {
            return None;
        }
        Some(
            rows.into_iter()
                .map(|(k, v)| (format!("mysql_{}", k.to_lowercase()), v))
                .collect(),
        )
    }

    fn native_column_types(&mut self, table: &str) -> Result<Vec<(String, String)>> {
        let mut conn = self.pool.get_conn()?;
        let default_db: Option<String> = if table.contains('.') {
            None
        } else {
            conn.query_first("SELECT DATABASE()")?
        };
        let (schema, bare) =
            crate::source::cdc::enrich_schema_and_table(table, default_db.as_deref());
        Ok(conn.exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (&schema, &bare),
        )?)
    }

    fn server_context(&mut self) -> Option<String> {
        let rows: Vec<(String, String)> = (|| -> Result<Vec<(String, String)>> {
            let mut conn = self.pool.get_conn()?;
            let mut guard = MysqlSessionGuard::apply(&mut conn, &self.session, None)?;
            Ok(guard.conn().query(
                "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('version', 'max_execution_time', \
                 'sql_mode', 'time_zone', 'wait_timeout', 'max_connections')",
            )?)
        })()
        .unwrap_or_default();
        let var = |name: &str| {
            rows.iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };
        Some(
            serde_json::json!({
                "engine": "mysql",
                "version": var("version"),
                "max_execution_time_ms": var("max_execution_time"),
                "sql_mode": var("sql_mode"),
                "time_zone": var("time_zone"),
                "wait_timeout_s": var("wait_timeout"),
                "max_connections": var("max_connections"),
            })
            .to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{bit_bytes_to_u64, correct_innodb_avg_row_length};

    /// The row size is the reported average, else the data length over the rows, and nothing when neither is positive.
    #[test]
    fn row_stats_prefer_the_reported_average_and_fall_back_to_data_over_rows() {
        use super::mysql_row_stats;
        assert_eq!(mysql_row_stats(100, 50, 9_000), (100, Some(50)));
        assert_eq!(mysql_row_stats(100, 0, 9_000), (100, Some(90)));
        assert_eq!(mysql_row_stats(1, 0, 7), (1, Some(7)));
        assert_eq!(mysql_row_stats(0, 0, 9_000), (0, None));
        assert_eq!(mysql_row_stats(-5, 0, 9_000), (0, None));
        assert_eq!(
            mysql_row_stats(100, 0, 0),
            (100, None),
            "zero bytes is no estimate"
        );
        assert_eq!(
            mysql_row_stats(100, 0, 50),
            (100, None),
            "under a byte a row is no estimate"
        );
        assert_eq!(mysql_row_stats(0, 12, 0), (0, Some(12)));
        assert_eq!(
            mysql_row_stats(10, 41_000, 0),
            (10, Some(correct_innodb_avg_row_length(41_000))),
            "an off-page row length is corrected"
        );
    }

    #[test]
    fn only_the_integer_family_is_an_integer_type() {
        use super::is_mysql_integer_type;
        for t in ["tinyint", "smallint", "mediumint", "int", "bigint"] {
            assert!(is_mysql_integer_type(t), "{t}");
        }
        for t in ["decimal", "varchar", "bit", "double", "integer", ""] {
            assert!(!is_mysql_integer_type(t), "{t}");
        }
    }

    /// A nullable column is no keyset key, and only the index named PRIMARY is the primary key.
    #[test]
    fn keyset_columns_are_the_not_null_ones_marked_by_the_primary_index() {
        use super::mysql_keyset_columns;
        let row = |c: &str, i: &str, n: &str| (c.to_string(), i.to_string(), n.to_string());
        assert_eq!(
            mysql_keyset_columns(vec![
                row("email", "uq_email", "NO"),
                row("nick", "uq_nick", "YES"),
                row("id", "PRIMARY", "no"),
                row("code", "primary_like", "NO"),
            ]),
            [
                ("email".to_string(), false),
                ("id".to_string(), true),
                ("code".to_string(), false)
            ]
        );
    }

    #[test]
    fn a_statement_timeout_becomes_milliseconds_and_zero_is_no_timeout() {
        use super::max_execution_ms;
        assert_eq!(max_execution_ms(0), None);
        assert_eq!(max_execution_ms(1), Some(1_000));
        assert_eq!(max_execution_ms(300), Some(300_000));
    }

    /// The server's statement timeout is known by either error code or by any one of its three message spellings.
    #[test]
    fn a_statement_timeout_is_known_by_its_code_or_by_its_message() {
        use super::is_statement_timeout;
        let server = |code: u16, message: &str| {
            mysql::Error::MySqlError(mysql::MySqlError {
                state: "HY000".into(),
                message: message.into(),
                code,
            })
        };
        assert!(is_statement_timeout(&server(3024, "interrupted")));
        assert!(is_statement_timeout(&server(1969, "interrupted")));
        for message in [
            "Query execution was interrupted, max_execution_time exceeded",
            "max_statement_time exceeded",
            "Query execution was interrupted, maximum statement execution time exceeded",
        ] {
            assert!(is_statement_timeout(&server(1, message)), "{message}");
        }
        assert!(!is_statement_timeout(&server(
            1205,
            "Lock wait timeout exceeded"
        )));
        assert!(!is_statement_timeout(&server(
            1064,
            "You have an error in your SQL syntax"
        )));
    }

    #[test]
    fn the_pool_opens_one_connection_until_more_are_asked_for() {
        let constraints = super::lean_pool_opts().constraints();
        assert_eq!((constraints.min(), constraints.max()), (1, 100));
    }

    /// Each TLS mode and each override sets exactly the driver options it names.
    #[test]
    fn tls_options_follow_the_mode_the_ca_file_and_each_override() {
        use crate::config::{TlsConfig, TlsMode};
        let opts = |cfg: TlsConfig| {
            let ssl = super::build_mysql_ssl_opts(&cfg);
            (
                ssl.accept_invalid_certs(),
                ssl.skip_domain_validation(),
                ssl.root_cert_path()
                    .map(|p| p.to_string_lossy().into_owned()),
            )
        };
        let mode = |mode| TlsConfig {
            mode,
            ..Default::default()
        };
        assert_eq!(opts(mode(TlsMode::Require)), (true, true, None));
        assert_eq!(opts(mode(TlsMode::VerifyCa)), (false, true, None));
        assert_eq!(opts(mode(TlsMode::VerifyFull)), (false, false, None));
        assert_eq!(
            opts(TlsConfig {
                ca_file: Some("/etc/ca.pem".into()),
                ..mode(TlsMode::VerifyFull)
            }),
            (false, false, Some("/etc/ca.pem".to_string()))
        );
        assert_eq!(
            opts(TlsConfig {
                accept_invalid_certs: true,
                ..mode(TlsMode::VerifyFull)
            }),
            (true, false, None)
        );
        assert_eq!(
            opts(TlsConfig {
                accept_invalid_hostnames: true,
                ..mode(TlsMode::VerifyFull)
            }),
            (false, true, None)
        );
    }

    #[test]
    fn only_an_enforced_tls_mode_turns_on_the_driver_ssl_options() {
        use crate::config::{TlsConfig, TlsMode};
        let ssl = |mode: Option<TlsMode>| {
            let cfg = mode.map(|mode| TlsConfig {
                mode,
                ..Default::default()
            });
            let builder =
                mysql::OptsBuilder::from_opts(mysql::Opts::from_url("mysql://h/db").unwrap());
            super::with_tls(builder, cfg.as_ref())
                .get_ssl_opts()
                .is_some()
        };
        assert!(ssl(Some(TlsMode::Require)));
        assert!(!ssl(Some(TlsMode::Disable)));
        assert!(!ssl(None));
    }

    #[test]
    fn bit_bytes_single_byte() {
        assert_eq!(bit_bytes_to_u64(&[0x00]), 0);
        assert_eq!(bit_bytes_to_u64(&[0x01]), 1);
        assert_eq!(bit_bytes_to_u64(&[0xFF]), 255);
    }

    #[test]
    fn bit_bytes_multi_byte() {
        assert_eq!(bit_bytes_to_u64(&[0x01, 0x02]), 258);
        assert_eq!(bit_bytes_to_u64(&[0xFF; 8]), u64::MAX);
    }

    #[test]
    fn bit_bytes_empty() {
        assert_eq!(bit_bytes_to_u64(&[]), 0);
    }

    #[test]
    fn bit_bytes_ascii_digit_bytes_are_bits_not_text() {
        assert_eq!(bit_bytes_to_u64(&[0x39]), 57);
        assert_eq!(bit_bytes_to_u64(&[0x31, 0x32]), 0x3132);
        assert_eq!(bit_bytes_to_u64(&[0x31, 0xFF]), 12799);
    }

    #[test]
    fn innodb_correction_below_threshold_is_identity() {
        assert_eq!(correct_innodb_avg_row_length(82), 82);
        assert_eq!(correct_innodb_avg_row_length(314), 314);
        assert_eq!(correct_innodb_avg_row_length(2_048), 2_048);
        assert_eq!(correct_innodb_avg_row_length(8 * 1024), 8 * 1024);
    }

    #[test]
    fn innodb_correction_above_threshold_divides_by_three() {
        assert_eq!(correct_innodb_avg_row_length(40_978), 40_978 / 3);
        assert_eq!(correct_innodb_avg_row_length(120_000), 40_000);
    }

    #[test]
    fn innodb_correction_does_not_undershoot_floor() {
        let just_above = 8 * 1024 + 1;
        let divided = correct_innodb_avg_row_length(just_above);
        assert!(divided >= 4 * 1024, "got {divided}");
    }
}
