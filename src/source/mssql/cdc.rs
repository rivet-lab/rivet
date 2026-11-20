//! SQL Server CDC adapter — `cdc.*` change-table poll → canonical [`cdc::ChangeEvent`].
#![allow(dead_code)]

use std::collections::VecDeque;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde_json::json;
use tiberius::{Client, ColumnData, Row};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

use super::arrow_convert::{cell_text, numeric_to_decimal_string, renders_as_text};
use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::cdc::checkpoint_identity::IdentityVerdict;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, DrainMode, Position};
use crate::source::require_tls_or_loopback;
use crate::types::hex_bytes;

/// The query for the captured vs expected column count of the capture instance rivet reads, as `name:got/all`; `all` is the table's column count, or the readable count under `columns_from: grants`.
fn row_image_sql(capture_instance: &str, readable: Option<usize>) -> String {
    let all = match readable {
        Some(n) => n.to_string(),
        None => "(SELECT COUNT(*) FROM sys.columns sc WHERE sc.object_id = t.object_id)".into(),
    };
    format!(
        "SELECT t.name + ':' + CAST(COUNT(cc.column_name) AS varchar(12)) + '/' + \
                CAST({all} AS varchar(12)) \
         FROM cdc.change_tables ct \
         JOIN sys.tables t ON t.object_id = ct.source_object_id \
         JOIN cdc.captured_columns cc ON cc.object_id = ct.object_id \
         WHERE ct.capture_instance = '{ci}' \
         GROUP BY t.name, t.object_id",
        ci = capture_instance.replace('\'', "''"),
    )
}

/// The query for captured char/varchar/text columns whose codepage differs from the database's, as `schema.table.column`.
fn lossy_columns_sql(capture_instance: &str) -> String {
    format!(
        "SELECT s.name + '.' + t.name + '.' + sc.name \
         FROM cdc.change_tables ct \
         JOIN sys.tables t ON t.object_id = ct.source_object_id \
         JOIN sys.schemas s ON s.schema_id = t.schema_id \
         JOIN cdc.captured_columns cc ON cc.object_id = ct.object_id \
         JOIN sys.columns sc ON sc.object_id = t.object_id AND sc.name = cc.column_name \
         WHERE ct.capture_instance = '{ci}' AND sc.system_type_id IN (35, 167, 175) \
           AND COLLATIONPROPERTY(sc.collation_name, 'CodePage') <> \
               COLLATIONPROPERTY(CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(), 'Collation')), 'CodePage')",
        ci = capture_instance.replace('\'', "''"),
    )
}

/// Pure verdict half of [`row_image`]: `rows` are `name:got/all` column counts, `lossy` the columns the capture job stores in the database codepage, `grants` judges against the readable columns.
pub(crate) fn row_image_verdict(
    rows: &[String],
    lossy: &[String],
    grants: bool,
) -> crate::source::cdc::RowImage {
    use crate::source::cdc::RowImage;
    let short: Vec<String> = rows
        .iter()
        .filter_map(|r| {
            let (name, counts) = r.split_once(':')?;
            let (got, all) = counts.split_once('/')?;
            let (g, a) = (
                got.trim().parse::<i64>().ok()?,
                all.trim().parse::<i64>().ok()?,
            );
            (if grants { g != a } else { g < a })
                .then(|| format!("{name} ({got} of {all} columns)"))
        })
        .collect();
    if !lossy.is_empty() {
        return RowImage::Partial {
            why: format!(
                "the capture job stores every char/varchar/text column in the DATABASE codepage, \
                 so a character outside it is written to the change table as '?'. These columns \
                 use another codepage: {}. Change them to nvarchar (or give them the database \
                 collation), re-enable the capture instance and re-run",
                lossy.join(", ")
            ),
        };
    }
    if short.is_empty() {
        return RowImage::Whole;
    }
    if grants {
        return RowImage::Partial {
            why: format!(
                "the capture instance(s) for {} do not carry exactly the columns this account \
                 may SELECT under `columns_from: grants`. Create an instance over exactly the \
                 readable columns (sys.sp_cdc_enable_table @captured_column_list) and re-run",
                short.join(", ")
            ),
        };
    }
    RowImage::Partial {
        why: format!(
            "the capture instance(s) for {} do not carry every column of their table (a column \
             added after the instance was created, or a PARTIAL @captured_column_list), so every \
             change event is missing those columns — the rows would be recorded as NULL rather \
             than as absent. Re-enable the table with the full column \
             list (omit @captured_column_list) and re-run",
            short.join(", ")
        ),
    }
}

/// Build one poll's T-SQL.
struct Poll<'a> {
    /// The capture instance whose change function is read.
    ci: &'a str,
    /// SQL expression yielding `@from` — the position to read AFTER.
    from_expr: &'a str,
    /// Row cap for one batch (`TOP (n)`).
    batch: i64,
    /// `Some(hex)` pins `@max` at the open-time LSN (a bounded drain).
    bound: Option<&'a str>,
}

/// The user error number `fill_sql` THROWs when the resume position fell below retention.
const RETENTION_GAP_ERROR: u32 = 51000;

/// Whether a server error number is `fill_sql`'s retention-gap THROW.
fn is_retention_gap(server_code: Option<u32>) -> bool {
    server_code == Some(RETENTION_GAP_ERROR)
}

fn fill_sql(p: Poll<'_>) -> String {
    let Poll {
        ci,
        from_expr,
        batch,
        bound,
    } = p;
    let max_expr = match bound {
        Some(hex) => format!("0x{hex}"),
        None => "sys.fn_cdc_get_max_lsn()".to_string(),
    };
    let recover = crate::source::cdc::checkpoint_identity::RECOVER.replace('\'', "''");
    let lift = seeded!(
        "position-below-log-start-moved-up",
        "IF @from IS NOT NULL AND @min IS NOT NULL AND @from < @min SET @from = @min; ",
        ""
    );
    format!(
        "DECLARE @from binary(10) = {from_expr}; \
         DECLARE @min binary(10) = (SELECT start_lsn FROM cdc.change_tables \
             WHERE capture_instance = '{ci}'); \
         DECLARE @max binary(10) = {max_expr}; \
         {lift}IF @from IS NOT NULL AND @min IS NOT NULL AND @from < @min \
            THROW {RETENTION_GAP_ERROR}, 'rivet cdc: the resume position is older than the SQL Server \
CDC change-table retention (the cleanup job removed it). Resuming would silently skip changes. {recover}', 1; \
         DECLARE @to binary(10) = NULL; \
         IF @from IS NOT NULL AND @max IS NOT NULL AND @from <= @max \
            SELECT @to = MAX(s) FROM (SELECT TOP ({batch}) __$start_lsn AS s \
                FROM cdc.fn_cdc_get_all_changes_{ci}(@from, @max, N'all') \
                ORDER BY __$start_lsn) q; \
         IF @to IS NOT NULL \
            SELECT * FROM cdc.fn_cdc_get_all_changes_{ci}(@from, @to, N'all') \
            ORDER BY __$start_lsn, __$seqval;"
    )
}

/// Connection parameters for a SQL Server CDC poll stream.
#[derive(Debug)]
pub(crate) struct Resume {
    /// Hex LSN to read after, or `None` for "from the change table's min".
    pub from_lsn: Option<String>,
    /// The database identity the checkpoint recorded, when it recorded one.
    pub identity: Option<DbIdentity>,
}

/// A database's log lineage: `family_guid` names the database, `recovery_fork_guid` the history since its last restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DbIdentity {
    pub family: String,
    pub fork: String,
}

impl DbIdentity {
    /// Read from a checkpoint's JSON; `None` when it predates identity recording.
    fn from_checkpoint(pos: &crate::source::cdc::Position) -> Option<Self> {
        let get = |k| pos.0.get(k).and_then(|v| v.as_str()).map(str::to_string);
        Some(Self {
            family: get("family_guid")?,
            fork: get("recovery_fork_guid")?,
        })
    }
}

/// Judge a resume: another database's LSNs, or a log rewound by a restore, are refused.
pub(crate) fn identity_verdict(
    checkpoint: Option<&DbIdentity>,
    server: Option<&DbIdentity>,
) -> IdentityVerdict {
    match (checkpoint, server) {
        (None, _) => IdentityVerdict::Unverifiable(
            "mssql cdc: this checkpoint carries no database identity (it predates rivet \
             recording one), so rivet cannot confirm it belongs to this database. LSNs are \
             positions in ONE database's log; if the source moved, resuming skips changes."
                .into(),
        ),
        (Some(_), None) => IdentityVerdict::Unverifiable(
            "mssql cdc: this login cannot read sys.database_recovery_status for the \
             database, so rivet cannot confirm the checkpoint belongs to it."
                .into(),
        ),
        (Some(c), Some(s)) if c.family != s.family => IdentityVerdict::Foreign(format!(
            "mssql cdc: this checkpoint was written against a different database (family \
             {}, the connection's is {}). LSNs are positions in one database's log: \
             resuming here would start at an arbitrary point in another log and skip \
             whatever lies below it, silently.",
            c.family, s.family
        )),
        (Some(c), Some(s)) if c.fork != s.fork => IdentityVerdict::Foreign(format!(
            "mssql cdc: this database was restored from a backup since the checkpoint was \
             written (recovery fork {} is now {}). The log was rewound: changes the \
             checkpoint already covers may no longer exist in the source, and the \
             destination still holds them.",
            c.fork, s.fork
        )),
        _ => IdentityVerdict::Ok,
    }
}

/// The connected database's identity, or `None` when the login cannot see it.
async fn db_identity(client: &mut Client<Compat<TcpStream>>) -> Result<Option<DbIdentity>> {
    let row = client
        .query(
            "SELECT CONVERT(varchar(36), family_guid), CONVERT(varchar(36), recovery_fork_guid) \
             FROM sys.database_recovery_status WHERE database_id = DB_ID()",
            &[],
        )
        .await?
        .into_row()
        .await?;
    Ok(row.and_then(|r| {
        Some(DbIdentity {
            family: r.get::<&str, _>(0)?.to_string(),
            fork: r.get::<&str, _>(1)?.to_string(),
        })
    }))
}

/// `position` with the database identity added, as a checkpoint records it.
fn with_identity(
    position: &crate::source::cdc::Position,
    id: Option<&DbIdentity>,
) -> crate::source::cdc::Position {
    let mut v = position.json().clone();
    if let (Some(id), Some(o)) = (id, v.as_object_mut()) {
        o.insert("family_guid".into(), id.family.clone().into());
        o.insert("recovery_fork_guid".into(), id.fork.clone().into());
    }
    crate::source::cdc::Position::new(v)
}

/// Read a resume position out of a checkpoint that has already PARSED.
pub(crate) fn resume_from_checkpoint(
    pos: Option<&crate::source::cdc::Position>,
    path: &str,
) -> Result<Resume> {
    let Some(pos) = pos else {
        return Ok(Resume {
            from_lsn: None,
            identity: None,
        });
    };
    let from_lsn = pos
        .0
        .get("lsn")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            crate::error::CodedError::new(
                crate::error::codes::SOURCE_CDC_CHECKPOINT_INVALID,
                format!(
                    "checkpoint '{path}' parses as JSON but carries no 'lsn' — refusing to treat \
                     it as absent, which would re-read and re-deliver the ENTIRE retained change \
                     table from fn_cdc_get_min_lsn under a successful exit. Restore the file, or: {}",
                    crate::source::cdc::checkpoint_identity::RECOVER
                ),
            )
        })?
        .to_string();
    Ok(Resume {
        from_lsn: Some(from_lsn),
        identity: DbIdentity::from_checkpoint(pos),
    })
}

pub(crate) struct MssqlCdcConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: crate::secret::Secret,
    /// CDC capture instance, e.g.
    pub capture_instance: String,
    /// Resume LSN — the hex of the last durably-written `__$start_lsn`.
    pub from_lsn: Option<String>,
    /// The identity the checkpoint recorded; checked against the server at open.
    pub checkpoint_identity: Option<DbIdentity>,
}

/// Polls a CDC change table and yields canonical changes.
pub(crate) fn source_object_of_capture_instance(
    url: &str,
    capture_instance: &str,
    tls: Option<&TlsConfig>,
) -> Result<Option<(String, String)>> {
    require_tls_or_loopback(url, tls)?;
    let p = crate::source::mssql::parse_mssql_url(url)?;
    let cfg = &MssqlCdcConfig {
        host: p.host,
        port: p.port,
        database: p.database,
        user: p.user,
        password: p.password,
        capture_instance: capture_instance.to_string(),
        from_lsn: None,
        checkpoint_identity: None,
    };
    let rt = crate::source::driver_runtime(
        "mssql cdc",
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build(),
    )?;
    let mut client = rt.block_on(connect(cfg, tls))?;

    let rows = rt
        .block_on(async {
            client
                .query(
                    "SELECT OBJECT_SCHEMA_NAME(source_object_id), \
                            OBJECT_NAME(source_object_id) \
                     FROM cdc.change_tables WHERE capture_instance = @P1",
                    &[&cfg.capture_instance.as_str()],
                )
                .await
        })
        .map_err(|e| {
            crate::error::coded_driver(
                crate::error::codes::SOURCE_QUERY_FAILED,
                &format!(
                    "sqlserver cdc: reading cdc.change_tables for capture instance \
                 '{capture_instance}' (the login needs SELECT on the cdc schema)"
                ),
                e,
            )
        })?;
    let row = rt.block_on(async { rows.into_row().await }).map_err(|e| {
        crate::error::coded_driver(
            crate::error::codes::SOURCE_QUERY_FAILED,
            &format!(
                "sqlserver cdc: reading the cdc.change_tables row for capture \
                 instance '{capture_instance}'"
            ),
            e,
        )
    })?;
    let named: Option<(Option<String>, Option<String>)> = row.map(|row| {
        let s: Option<&str> = row.get(0);
        let t: Option<&str> = row.get(1);
        (s.map(str::to_string), t.map(str::to_string))
    });
    classify_source_object(capture_instance, named)
}

/// What a `cdc.change_tables` lookup MEANS — the decision, pulled out of the I/O.
fn classify_source_object(
    capture_instance: &str,
    named: Option<(Option<String>, Option<String>)>,
) -> Result<Option<(String, String)>> {
    match named {
        None => Ok(None),
        Some((Some(s), Some(t))) => Ok(Some((s, t))),
        Some(_) => crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            "sqlserver cdc: cdc.change_tables has a row for capture instance \
             '{capture_instance}', but its source object's schema and name read back \
             as NULL, so rivet cannot tell which relation it captures. Two causes, \
             in order of likelihood: (1) this login lacks permission on the source \
             TABLE — SQL Server hides object names from a principal that cannot see \
             the object, and a reader granted only `SELECT ON SCHEMA::cdc` hits this \
             on a perfectly healthy instance; grant it SELECT on the captured table. \
             (2) the table was dropped without disabling capture first, leaving the \
             row briefly orphaned — SQL Server Agent's capture job clears that within \
             seconds, so re-run shortly. Do NOT run `sp_cdc_disable_table` here: under \
             cause (1) it succeeds and destroys a working capture instance along with \
             every change it holds, and under cause (2) it fails with Msg 22931."
        ),
    }
}

pub(crate) struct MssqlChangeStream {
    rt: tokio::runtime::Runtime,
    client: Client<Compat<TcpStream>>,
    capture_instance: String,
    schema: String,
    table: String,
    /// Internal read cursor — advanced per bounded batch so the next poll reads the following window.
    from_lsn: Option<String>,
    /// The database's identity at open, recorded into every checkpoint.
    identity: Option<DbIdentity>,
    pending: VecDeque<ChangeEvent>,
    /// Where an oversized poll batch's tail goes.
    spill_dir: Option<std::path::PathBuf>,
    /// A spilled batch tail, still being handed out.
    spooled: Option<crate::source::cdc::tx_buffer::SpooledTail>,
    /// Max changes to pull per poll — bounds drain memory to O(batch) instead of O(total change-table window).
    batch_limit: i64,
    /// A poll that returns no rows has drained the window up to the current max LSN.
    exhausted: bool,
    /// Open-time max-LSN ceiling (bare hex) for a bounded run.
    bound: Option<String>,
    /// The grants the run resolved before its anchor.
    readable: Option<
        std::sync::Arc<std::collections::HashMap<String, crate::source::cdc::ReadableColumns>>,
    >,
}

impl MssqlChangeStream {
    /// Connect and bind to a capture instance.
    pub(crate) fn open(
        cfg: &MssqlCdcConfig,
        tls: Option<&TlsConfig>,
        peek: crate::source::cdc::PeekBound,
        mode: DrainMode,
        configured_tables: &[String],
    ) -> Result<Self> {
        if !cfg
            .capture_instance
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            crate::rivet_bail!(
                crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
                "invalid CDC capture instance name: {:?}",
                cfg.capture_instance
            );
        }
        if let Some(lsn) = &cfg.from_lsn
            && (lsn.is_empty() || lsn.len() % 2 != 0 || !lsn.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(crate::source::cdc::checkpoint_identity::checkpoint_invalid(
                format!("mssql cdc: malformed resume LSN {lsn:?}"),
            ));
        }
        let rt = crate::source::driver_runtime(
            "mssql cdc",
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build(),
        )?;
        let mut client = rt.block_on(connect(cfg, tls))?;
        let identity = rt.block_on(db_identity(&mut client))?;
        if cfg.from_lsn.is_some() {
            identity_verdict(cfg.checkpoint_identity.as_ref(), identity.as_ref()).enforce()?;
        }
        let from_lsn = cfg.from_lsn.clone();

        let meta: Option<(String, String)> = rt.block_on(async {
            let row = client
                .query(
                    "SELECT OBJECT_SCHEMA_NAME(source_object_id), \
                            OBJECT_NAME(source_object_id) \
                     FROM cdc.change_tables WHERE capture_instance = @P1",
                    &[&cfg.capture_instance.as_str()],
                )
                .await
                .ok()?
                .into_row()
                .await
                .ok()??;
            let s: Option<&str> = row.get(0);
            let t: Option<&str> = row.get(1);
            Some((s?.to_string(), t?.to_string()))
        });
        let (schema, table) = meta.unwrap_or_else(|| {
            cfg.capture_instance
                .split_once('_')
                .map(|(s, t)| (s.to_string(), t.to_string()))
                .unwrap_or_else(|| (String::new(), cfg.capture_instance.clone()))
        });

        if !configured_tables.is_empty()
            && !configured_tables.iter().any(|c| {
                crate::source::cdc::sink::table_matches(
                    crate::source::cdc::CdcEngine::Mssql,
                    c,
                    &schema,
                    &table,
                )
            })
        {
            crate::rivet_bail!(
                crate::error::codes::CONFIG_CDC_CAPTURE_INSTANCE_MISMATCH,
                "sqlserver cdc: capture instance '{}' emits changes for `{}.{}` (from \
                 cdc.change_tables), but no configured table matches it — the config asks for \
                 {:?}. Routing is byte-exact while SQL Server's collation is not, so every \
                 event would be dropped AND the checkpoint would advance past it, losing the \
                 changes for good. Set `table:` to `{}.{}` exactly as the catalog spells it.",
                cfg.capture_instance,
                schema,
                table,
                configured_tables,
                schema,
                table
            );
        }

        let bound = if mode.is_bounded() {
            let max: Option<String> = rt.block_on(async {
                Ok::<_, anyhow::Error>(
                    client
                        .query(
                            "SELECT CONVERT(varchar(24), sys.fn_cdc_get_max_lsn(), 1)",
                            &[],
                        )
                        .await?
                        .into_row()
                        .await?
                        .and_then(|r| r.get::<&str, _>(0).map(|s| s.to_string())),
                )
            })?;
            let max = max.map(|s| s.trim_start_matches("0x").to_ascii_lowercase());
            if let Some(hex) = &max
                && (hex.is_empty()
                    || hex.len() % 2 != 0
                    || !hex.bytes().all(|b| b.is_ascii_hexdigit()))
            {
                crate::rivet_bail!(
                    crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                    "mssql cdc: malformed open-time max LSN {hex:?}"
                );
            }
            max
        } else {
            None
        };
        Ok(Self {
            rt,
            client,
            capture_instance: cfg.capture_instance.clone(),
            schema,
            table,
            from_lsn,
            identity,
            pending: VecDeque::new(),
            spill_dir: None,
            spooled: None,
            batch_limit: peek.rows_capped() as i64,
            exhausted: false,
            bound,
            readable: None,
        })
    }

    /// Open from a `sqlserver://user:pass@host:port/db` URL + a capture instance (the factory path).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_url(
        url: &str,
        capture_instance: &str,
        resume: Resume,
        tls: Option<&TlsConfig>,
        peek: crate::source::cdc::PeekBound,
        mode: DrainMode,
        configured_tables: &[String],
        spill_dir: Option<std::path::PathBuf>,
        readable: Option<
            std::sync::Arc<std::collections::HashMap<String, crate::source::cdc::ReadableColumns>>,
        >,
    ) -> Result<Self> {
        require_tls_or_loopback(url, tls)?;
        let p = crate::source::mssql::parse_mssql_url(url)?;
        Self::open(
            &MssqlCdcConfig {
                host: p.host,
                port: p.port,
                database: p.database,
                user: p.user,
                password: p.password,
                capture_instance: capture_instance.to_string(),
                from_lsn: resume.from_lsn,
                checkpoint_identity: resume.identity,
            },
            tls,
            peek,
            mode,
            configured_tables,
        )
        .map(|mut s| {
            s.spill_dir = spill_dir;
            s.readable = readable;
            s
        })
    }

    /// Refuse a batch read while the readable columns of the captured table differ from the ones the run resolved.
    fn refuse_grants_moved(&mut self) -> Result<()> {
        if seeded!("column-grants-mssql-batch-grants-not-reread", true, false) {
            return Ok(());
        }
        let Some(resolved) = self.readable.clone() else {
            return Ok(());
        };
        let mut configured: Vec<String> = resolved.keys().cloned().collect();
        configured.sort();
        let Self { rt, client, .. } = self;
        let now = crate::source::mssql::column_grants::readable_now(rt, client, &configured)?;
        match crate::source::column_grants::first_grants_change(&configured, &resolved, &now) {
            Some(why) => Err(crate::error::coded(
                crate::error::codes::SOURCE_CDC_GRANTS_CHANGED,
                format!(
                    "sqlserver cdc: {why}; nothing read since the last delivered change was \
                     delivered. Re-run: the next run reads the grants as they are now and \
                     resumes from the checkpoint, so nothing is lost"
                ),
            )),
            None => Ok(()),
        }
    }

    /// Poll ONE **bounded batch** of the change table into `pending`.
    fn fill(&mut self) -> Result<()> {
        self.refuse_grants_moved()?;
        let ci = self.capture_instance.clone();
        let from_expr = match &self.from_lsn {
            Some(hex) => format!("sys.fn_cdc_increment_lsn(0x{hex})"),
            None => format!("sys.fn_cdc_get_min_lsn('{ci}')"),
        };
        let sql = fill_sql(Poll {
            ci: &ci,
            from_expr: &from_expr,
            batch: self.batch_limit,
            bound: self.bound.as_deref(),
        });
        let rows = {
            let Self { rt, client, .. } = self;
            rt.block_on(async { client.simple_query(sql).await?.into_first_result().await })
                .map_err(|e| {
                    if is_retention_gap(e.code()) {
                        crate::error::CodedError::new(
                            crate::error::codes::SOURCE_CDC_LOG_GAP,
                            e.to_string(),
                        )
                        .into()
                    } else {
                        anyhow::Error::new(e).context(crate::source::cdc::MSSQL_CDC_HINT)
                    }
                })?
        };
        let mut max_lsn: Option<String> = None;
        let mut batch = crate::source::cdc::tx_buffer::TxBuffer::new(
            crate::source::cdc::CdcEngine::Mssql,
            self.spill_dir.clone(),
        );
        for r in &rows {
            let mut op_code = 0i32;
            let mut lsn = String::new();
            let mut values: Vec<RivetValue> = Vec::new();
            let mut names: Vec<String> = Vec::new();
            for (idx, (col, data)) in r.cells().enumerate() {
                match col.name() {
                    "__$operation" => {
                        if let ColumnData::I32(Some(v)) = data {
                            op_code = *v;
                        }
                    }
                    "__$start_lsn" => {
                        if let ColumnData::Binary(Some(b)) = data {
                            lsn = hex_bytes(b);
                        }
                    }
                    n if n.starts_with("__$") => {}
                    n => {
                        names.push(n.to_string());
                        values.push(if renders_as_text(col.column_type()) {
                            match cell_text(data) {
                                Some(t) => RivetValue::Bytes(t.into_bytes()),
                                None => cell_fallthrough(data)?,
                            }
                        } else {
                            cell_to_rivet(r, idx, data)?
                        });
                    }
                }
            }
            if !lsn.is_empty() {
                max_lsn = Some(lsn.clone());
            }
            let Some(op) = map_op(op_code) else { continue };
            let (before, after) = if op.values_live_in_before() {
                (Some(values), None)
            } else {
                (None, Some(values))
            };
            let ev = ChangeEvent {
                op,
                schema: self.schema.clone(),
                table: self.table.clone(),
                before,
                after,
                position: Position::new(json!({ "lsn": lsn })),
                committed: false,
                image_names: Some(std::sync::Arc::from(names)),
                seq: 0,
                poison: None,
                row_id: None,
                before_names: None,
                before_poison: None,
                image_labels: None,
            };
            batch.push(ev, crate::source::cdc::spill::encode_event, &lsn)?;
        }
        let (evs, spooled) = batch.close_groups(
            crate::source::cdc::spill::decode_event,
            &max_lsn.as_deref().unwrap_or(""),
        )?;
        self.pending.extend(seeded!(
            "committed-every-event",
            evs.into_iter()
                .map(|mut e| {
                    e.committed = true;
                    e
                })
                .collect::<Vec<_>>(),
            evs
        ));
        self.spooled = spooled;
        match max_lsn {
            Some(l) => self.from_lsn = Some(l),
            None => self.exhausted = true,
        }
        Ok(())
    }

    /// The number of columns the run may deliver of this stream's table; `None` without `columns_from: grants`.
    fn readable_count(&self) -> Option<usize> {
        self.readable
            .as_ref()?
            .iter()
            .find(|(cfg, _)| {
                crate::source::cdc::sink::table_matches(
                    crate::source::cdc::CdcEngine::Mssql,
                    cfg,
                    &self.schema,
                    &self.table,
                )
            })
            .map(|(_, r)| r.columns.len())
    }

    /// One row of a spilled tail, group-aware: `committed` comes from the row that FOLLOWS it, which is why the tail reads one record ahead.
    fn next_spooled(&mut self) -> Result<Option<ChangeEvent>> {
        crate::source::cdc::tx_buffer::replay(
            &mut self.spooled,
            crate::source::cdc::spill::decode_event,
        )
    }
}

impl ChangeStream for MssqlChangeStream {
    fn row_image(&mut self, _tables: &[String]) -> crate::source::cdc::RowImage {
        use crate::source::cdc::RowImage;
        if seeded!("cdc-stdout-partial-image", _tables.is_empty(), false) {
            return RowImage::Whole;
        }
        let readable = self.readable_count();
        let sql = row_image_sql(&self.capture_instance, readable);
        let Self { rt, client, .. } = self;
        let rows = rt.block_on(async { client.simple_query(sql).await?.into_first_result().await });
        let Ok(rows) = rows else {
            return RowImage::Whole;
        };
        let named: Vec<String> = rows
            .iter()
            .filter_map(|r| r.get::<&str, _>(0).map(str::to_string))
            .collect();
        let lossy_sql = lossy_columns_sql(&self.capture_instance);
        let Self { rt, client, .. } = self;
        let lossy = rt
            .block_on(async {
                client
                    .simple_query(lossy_sql)
                    .await?
                    .into_first_result()
                    .await
            })
            .map(|rows| {
                rows.iter()
                    .filter_map(|r| r.get::<&str, _>(0).map(str::to_string))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        row_image_verdict(&named, &lossy, readable.is_some())
    }

    fn engine(&self) -> crate::source::cdc::CdcEngine {
        crate::source::cdc::CdcEngine::Mssql
    }

    fn readable_columns(&self, configured: &str) -> Option<crate::source::cdc::ReadableColumns> {
        self.readable.as_ref()?.get(configured).cloned()
    }

    /// The capture instance names its source object in `cdc.change_tables`, so this engine ALWAYS knows the real pair.
    fn resolved_identity(&self, configured: &str) -> Option<(String, String)> {
        crate::source::cdc::sink::table_matches(
            crate::source::cdc::CdcEngine::Mssql,
            configured,
            &self.schema,
            &self.table,
        )
        .then(|| (self.schema.clone(), self.table.clone()))
    }

    fn checkpoint_of(
        &self,
        position: &crate::source::cdc::Position,
    ) -> crate::source::cdc::Position {
        with_identity(position, self.identity.as_ref())
    }

    fn drained_frontier(&self) -> Option<crate::source::cdc::Position> {
        drained_frontier(
            self.exhausted,
            self.bound.as_deref(),
            self.from_lsn.as_deref(),
        )
        .map(|lsn| Position::new(json!({ "lsn": lsn })))
    }

    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        while self.pending.is_empty() && self.spooled.is_none() && !self.exhausted {
            if let Err(e) = self.fill() {
                return Some(Err(e));
            }
        }
        if let Some(ev) = self.pending.pop_front() {
            if seeded!(
                "pk-move-without-delete-mssql",
                ev.op == crate::source::cdc::ChangeOp::Delete
                    && self.pending.front().is_some_and(|n| {
                        n.op == crate::source::cdc::ChangeOp::Insert
                            && n.table == ev.table
                            && n.position == ev.position
                    }),
                false
            ) {
                return self.next_change();
            }
            return Some(Ok(ev));
        }
        if self.spooled.is_some() {
            match self.next_spooled() {
                Ok(Some(ev)) => return Some(Ok(ev)),
                Ok(None) => return self.next_change(),
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

/// The open-time bound a drained bounded run may checkpoint at, when it is past a resumed cursor.
fn drained_frontier(exhausted: bool, bound: Option<&str>, from: Option<&str>) -> Option<String> {
    let (b, f) = (bound?.to_ascii_lowercase(), from?.to_ascii_lowercase());
    (exhausted && b.len() == f.len() && b > f).then_some(b)
}

/// `__$operation` → canonical op.
fn map_op(code: i32) -> Option<ChangeOp> {
    match code {
        1 => Some(ChangeOp::Delete),
        2 => Some(ChangeOp::Insert),
        4 => Some(ChangeOp::Update),
        _ => None,
    }
}

/// Map a captured source cell to a typed [`RivetValue`].
fn cell_from_data(data: &ColumnData<'_>) -> Option<RivetValue> {
    Some(match data {
        ColumnData::Bit(Some(b)) => RivetValue::Bool(*b),
        ColumnData::U8(Some(v)) => RivetValue::Int(*v as i64),
        ColumnData::I16(Some(v)) => RivetValue::Int(*v as i64),
        ColumnData::I32(Some(v)) => RivetValue::Int(*v as i64),
        ColumnData::I64(Some(v)) => RivetValue::Int(*v),
        ColumnData::F32(Some(v)) => RivetValue::Float(*v as f64),
        ColumnData::F64(Some(v)) => RivetValue::Float(*v),
        ColumnData::String(Some(s)) => RivetValue::Bytes(s.as_bytes().to_vec()),
        ColumnData::Guid(Some(g)) => RivetValue::Bytes(g.as_bytes().to_vec()),
        ColumnData::Binary(Some(b)) => RivetValue::Bytes(b.to_vec()),
        ColumnData::Numeric(Some(n)) => {
            RivetValue::Bytes(numeric_to_decimal_string(n.value(), n.scale()).into_bytes())
        }
        _ => return None,
    })
}

/// Microseconds since midnight, truncating sub-microsecond nanos.
/// Nanoseconds since midnight: a `time(7)` keeps its 100ns tick, and the column's builder decides the unit.
fn naive_time_to_nanos(t: NaiveTime) -> i64 {
    t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64
}

fn cell_to_rivet(row: &Row, idx: usize, data: &ColumnData<'_>) -> Result<RivetValue> {
    if let Some(v) = cell_from_data(data) {
        return Ok(v);
    }
    let unreadable = |e: tiberius::error::Error| {
        anyhow::Error::new(crate::error::CodedError::new(
            crate::error::codes::SOURCE_CDC_CELL_UNSUPPORTED,
            format!("mssql cdc: column {idx}: cannot read {data:?}: {e}"),
        ))
    };
    Ok(match data {
        ColumnData::DateTimeOffset(_) => row
            .try_get::<chrono::DateTime<chrono::FixedOffset>, _>(idx)
            .map_err(unreadable)?
            .map_or(RivetValue::Null, |dt| RivetValue::DateTime(dt.naive_utc())),
        ColumnData::DateTime(_) => row
            .try_get::<NaiveDateTime, _>(idx)
            .map_err(unreadable)?
            .map_or(RivetValue::Null, |dt| {
                RivetValue::DateTime(super::arrow_convert::nearest_micro(dt))
            }),
        ColumnData::DateTime2(_) | ColumnData::SmallDateTime(_) => row
            .try_get::<NaiveDateTime, _>(idx)
            .map_err(unreadable)?
            .map_or(RivetValue::Null, RivetValue::DateTime),
        ColumnData::Date(_) => row
            .try_get::<NaiveDate, _>(idx)
            .map_err(unreadable)?
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map_or(RivetValue::Null, RivetValue::DateTime),
        ColumnData::Time(_) => row
            .try_get::<NaiveTime, _>(idx)
            .map_err(unreadable)?
            .map_or(RivetValue::Null, |t| {
                seeded!(
                    "mssql-time7-lands-at-microseconds",
                    RivetValue::TimeMicros(naive_time_to_nanos(t) / 1_000),
                    RivetValue::TimeNanos(naive_time_to_nanos(t))
                )
            }),
        other => cell_fallthrough(other)?,
    })
}

/// NULL for a true SQL NULL; a refusal for a valued cell no arm above decodes, never a silent NULL.
fn cell_fallthrough(data: &ColumnData<'_>) -> Result<RivetValue> {
    if super::arrow_convert::is_null_cell(data) {
        return Ok(RivetValue::Null);
    }
    crate::rivet_bail!(
        crate::error::codes::SOURCE_CDC_CELL_UNSUPPORTED,
        "mssql cdc: no decoder for a captured {data:?} value — refusing rather than writing NULL"
    )
}

async fn connect(
    cfg: &MssqlCdcConfig,
    tls: Option<&TlsConfig>,
) -> Result<Client<Compat<TcpStream>>> {
    let config = crate::source::mssql::tiberius_config(
        &cfg.host,
        cfg.port,
        &cfg.database,
        &cfg.user,
        &cfg.password,
        tls,
    );
    crate::source::mssql::dial(config, &format!("mssql://{}:{}", cfg.host, cfg.port)).await
}

/// Persist the anchor for `cdc.initial: snapshot` to `ckpt`, BEFORE the snapshot read.
pub(crate) fn pin_checkpoint_at_max_lsn(
    url: &str,
    capture_instance: &str,
    ckpt: &std::path::Path,
    tls: Option<&TlsConfig>,
) -> Result<()> {
    let mut src = crate::source::mssql::MssqlSource::connect_with_tls(url, tls)?;
    let probe = src.cdc_health(None)?;
    if probe_max_lsn(&probe).is_none() {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            "mssql cdc initial snapshot: sys.fn_cdc_get_max_lsn() is NULL — enable CDC first \
             (EXEC sys.sp_cdc_enable_db) so the anchor exists before the snapshot"
        );
    }
    let p = crate::source::mssql::parse_mssql_url(url)?;
    let cfg = MssqlCdcConfig {
        host: p.host,
        port: p.port,
        database: p.database,
        user: p.user,
        password: p.password,
        capture_instance: capture_instance.to_string(),
        from_lsn: None,
        checkpoint_identity: None,
    };
    let rt = crate::source::driver_runtime(
        "mssql cdc",
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build(),
    )?;
    let (identity, anchor) = rt.block_on(async {
        let mut client = connect(&cfg, tls).await?;
        let identity = db_identity(&mut client).await?;
        let anchor = client
            .query(
                "DECLARE @max binary(10) = sys.fn_cdc_get_max_lsn(); \
                 SELECT CONVERT(varchar(24), ISNULL((SELECT sys.fn_cdc_decrement_lsn(start_lsn) \
                     FROM cdc.change_tables WHERE capture_instance = @P1 AND start_lsn > @max), \
                     @max), 1)",
                &[&capture_instance],
            )
            .await?
            .into_row()
            .await?
            .and_then(|r| r.get::<&str, _>(0).map(bare_lsn));
        Ok::<_, anyhow::Error>((identity, anchor))
    })?;
    let anchor = anchor.ok_or_else(|| {
        crate::error::coded(
            crate::error::codes::INTERNAL_SOURCE_INVARIANT,
            "mssql cdc initial snapshot: the server returned no anchor LSN".to_string(),
        )
    })?;
    with_identity(
        &Position::new(serde_json::json!({ "lsn": anchor })),
        identity.as_ref(),
    )
    .save(ckpt)
}

/// A server-rendered LSN (`0x…`) as the bare lowercase hex a checkpoint stores.
fn bare_lsn(s: &str) -> String {
    s.trim_start_matches("0x")
        .trim_start_matches("0X")
        .to_ascii_lowercase()
}

/// The probe's max LSN as the bare hex the checkpoint stores (strip `0x`).
fn probe_max_lsn(probe: &crate::source::mssql::MssqlCdcProbe) -> Option<String> {
    if !probe.cdc_enabled {
        return None;
    }
    probe.max_lsn_hex.as_deref().map(bare_lsn)
}

#[cfg(test)]
mod tests {
    #[test]
    fn only_a_drained_resumed_cursor_below_the_bound_yields_a_frontier() {
        use super::drained_frontier as f;
        assert_eq!(f(true, Some("00FF"), Some("0010")), Some("00ff".into()));
        assert_eq!(f(false, Some("00ff"), Some("0010")), None, "not drained");
        assert_eq!(f(true, None, Some("0010")), None, "daemon: no bound");
        assert_eq!(f(true, Some("00ff"), None), None, "no cursor");
        assert_eq!(f(true, Some("00ff"), Some("00ff")), None, "already there");
        assert_eq!(f(true, Some("0010"), Some("00ff")), None, "never backwards");
        assert_eq!(f(true, Some("0fff"), Some("ff")), None, "unequal widths");
    }

    #[test]
    fn the_pinned_anchor_uses_the_event_lsn_case_so_warehouse_views_rank_it_right() {
        let probe = crate::source::mssql::MssqlCdcProbe {
            cdc_enabled: true,
            max_lsn_hex: Some("0x0000002D000000D80194".into()),
            instance_min_lsn: None,
            agent_running: None,
        };
        let anchor = super::probe_max_lsn(&probe).unwrap();
        assert_eq!(
            anchor,
            crate::types::hex_bytes(&[0, 0, 0, 0x2d, 0, 0, 0, 0xd8, 0x01, 0x94])
        );
        let older_event = crate::types::hex_bytes(&[0, 0, 0, 0x2a, 0, 0, 0x01, 0xa8, 0, 0x04]);
        assert!(
            anchor > older_event,
            "BigQuery/Snowflake order __pos.lsn as a string: {anchor} vs {older_event}"
        );
    }

    /// The three outcomes of a `cdc.change_tables` lookup, kept apart.
    #[test]
    fn a_lookup_tells_a_missing_instance_from_unreadable_object_names() {
        assert_eq!(
            super::classify_source_object("dbo_orders", None).expect("no row is not an error"),
            None,
            "no row means the catalog does not know this instance — the caller then \
             says so and points at sp_cdc_enable_table, which is the right repair \
             for THAT state"
        );
        assert_eq!(
            super::classify_source_object(
                "dbo_orders",
                Some((Some("dbo".into()), Some("orders".into()))),
            )
            .expect("a complete row resolves"),
            Some(("dbo".to_string(), "orders".to_string()))
        );
        let err = super::classify_source_object("dbo_orders", Some((None, None)))
            .expect_err("a row whose source object is gone is neither a resolution nor an absence")
            .to_string();
        assert!(
            err.to_lowercase().contains("permission"),
            "NULL object names are produced by METADATA VISIBILITY far more often \
             than by a dropped table — rivet's own documented least-privilege reader \
             (`GRANT SELECT ON SCHEMA::cdc`) reads NULL/NULL on a healthy instance \
             (MEASURED against `sa` on the same row). A message that names only the \
             rare cause sends the operator to repair something that is not broken: \
             {err}"
        );
        let permission_at = err.find("permission").expect("the likely cause is named");
        let dropped_at = err.find("dropped").expect("the rare cause is named");
        assert!(
            permission_at < dropped_at,
            "the causes must be ordered by LIKELIHOOD — metadata visibility is what a \
             least-privilege reader hits every run, a dropped table is a few-second \
             window. Leading with the rare one sends the operator to repair something \
             that is not broken: {err}"
        );
        assert!(
            err.contains("grant it SELECT on the captured table"),
            "and the repair for that likely cause must be IN the message — measured, \
             granting SELECT on the base table makes OBJECT_SCHEMA_NAME return the \
             name and rivet proceed. Without it the operator is told what is wrong and \
             not what to do: {err}"
        );
        assert!(
            err.contains("Do NOT run `sp_cdc_disable_table`"),
            "the message must carry the explicit warning, not merely avoid one \
             phrasing of the advice. MEASURED both branches: under the permission \
             cause that command SUCCEEDS and destroys a working capture instance with \
             every change it still holds; under the orphan cause it FAILS with Msg \
             22931. Destructive on one branch, impossible on the other — an operator \
             has to be told so in the message itself: {err}"
        );
        assert!(
            !err.contains("does not know"),
            "and it must not claim the instance is unknown when the row is right \
             there — that is a different state with a different repair: {err}"
        );
    }

    fn ckpt(j: serde_json::Value) -> crate::source::cdc::Position {
        crate::source::cdc::Position::new(j)
    }

    /// The whole point: absence of `lsn` is an ERROR, absence of `pinned` is a documented default.
    #[test]
    fn a_checkpoint_without_an_lsn_is_refused_not_read_as_absent() {
        let fresh = resume_from_checkpoint(None, "/x").expect("no checkpoint is not an error");
        assert_eq!(fresh.from_lsn, None);

        let r = resume_from_checkpoint(Some(&ckpt(serde_json::json!({"lsn": "0a0b"}))), "/x")
            .expect("an lsn without `pinned` is a legacy checkpoint, still valid");
        assert_eq!(r.from_lsn.as_deref(), Some("0a0b"));
        let legacy = resume_from_checkpoint(
            Some(&ckpt(serde_json::json!({"lsn": "0a0b", "pinned": true}))),
            "/x",
        )
        .unwrap();
        assert_eq!(
            legacy.from_lsn.as_deref(),
            Some("0a0b"),
            "an anchor written by rivet 0.31 or older is an ordinary position: below the instance's start it is refused like any other"
        );

        for missing in [
            serde_json::json!({}),
            serde_json::json!({"pinned": false}),
            serde_json::json!({"lsn_renamed_by_a_future_version": "0a0b"}),
            serde_json::json!({"lsn": 2571}),
            serde_json::json!({"lsn": null}),
        ] {
            let err = resume_from_checkpoint(Some(&ckpt(missing.clone())), "/tmp/c.ckpt")
                .expect_err(&format!(
                    "{missing} carries no readable position — treating it as absent re-reads \
                     the ENTIRE retained change table under a green exit (measured: ids \
                     [1,2,3,4] delivered where [4] was owed)"
                ));
            assert_eq!(crate::error::classify_exit(&err), 5, "{err}");
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_SOURCE_CDC_CHECKPOINT_INVALID")
            );
            let err = err.to_string();
            assert!(
                err.contains("lsn") && err.contains("/tmp/c.ckpt"),
                "the refusal must name the key AND the file, or an operator cannot act on \
                 it: {err}"
            );
            assert!(
                err.ends_with(&format!(
                    "Restore the file, or: {}",
                    crate::source::cdc::checkpoint_identity::RECOVER
                )),
                "the remedy must be anchor FIRST, then re-snapshot: {err}"
            );
        }
    }

    use super::*;

    #[test]
    fn a_valued_cell_without_a_decoder_is_refused_not_nulled() {
        use std::borrow::Cow;
        let xml = ColumnData::Xml(Some(Cow::Owned(tiberius::xml::XmlData::new("<a/>"))));
        let err = cell_fallthrough(&xml).expect_err("a valued xml cell must not become NULL");
        assert_eq!(crate::error::classify_exit(&err), 5, "{err:#}");
        for null in [
            ColumnData::Xml(None),
            ColumnData::Date(None),
            ColumnData::DateTimeOffset(None),
            ColumnData::String(None),
        ] {
            assert_eq!(
                cell_fallthrough(&null).unwrap(),
                RivetValue::Null,
                "{null:?}"
            );
        }
    }

    /// Every data-only `ColumnData` arm, with an INDEPENDENT expectation.
    #[test]
    fn every_data_only_column_data_arm_maps_to_its_documented_value() {
        use std::borrow::Cow;

        let uuid = tiberius::Uuid::from_bytes([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ]);
        let cases: Vec<(&str, ColumnData<'_>, RivetValue)> = vec![
            (
                "bit_true",
                ColumnData::Bit(Some(true)),
                RivetValue::Bool(true),
            ),
            (
                "bit_false",
                ColumnData::Bit(Some(false)),
                RivetValue::Bool(false),
            ),
            ("u8", ColumnData::U8(Some(200)), RivetValue::Int(200)),
            (
                "i16",
                ColumnData::I16(Some(-30_000)),
                RivetValue::Int(-30_000),
            ),
            (
                "i32",
                ColumnData::I32(Some(2_000_000_000)),
                RivetValue::Int(2_000_000_000),
            ),
            (
                "i64",
                ColumnData::I64(Some(9_000_000_000_000_000_000)),
                RivetValue::Int(9_000_000_000_000_000_000),
            ),
            ("f32", ColumnData::F32(Some(0.5)), RivetValue::Float(0.5)),
            (
                "f64",
                ColumnData::F64(Some(-1.25)),
                RivetValue::Float(-1.25),
            ),
            (
                "string",
                ColumnData::String(Some(Cow::Borrowed("héllo"))),
                RivetValue::Bytes("héllo".as_bytes().to_vec()),
            ),
            (
                "guid",
                ColumnData::Guid(Some(uuid)),
                RivetValue::Bytes(vec![
                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                    0x0e, 0x0f, 0x10,
                ]),
            ),
            (
                "binary",
                ColumnData::Binary(Some(Cow::Borrowed(&[0xde, 0xad, 0xbe, 0xef]))),
                RivetValue::Bytes(vec![0xde, 0xad, 0xbe, 0xef]),
            ),
            (
                "numeric",
                ColumnData::Numeric(Some(tiberius::numeric::Numeric::new_with_scale(15005, 2))),
                RivetValue::Bytes(b"150.05".to_vec()),
            ),
        ];

        let mut wrong = Vec::new();
        for (label, data, want) in &cases {
            match cell_from_data(data) {
                Some(got) if got == *want => {}
                got => wrong.push(format!("{label}: want {want:?}, got {got:?}")),
            }
        }
        assert!(
            wrong.is_empty(),
            "data-only ColumnData arms diverged from their documented mapping:\n  {}",
            wrong.join("\n  ")
        );

        assert_eq!(cell_from_data(&ColumnData::Bit(None)), None, "NULL defers");
        assert_eq!(
            cell_from_data(&ColumnData::Date(None)),
            None,
            "a temporal arm must NOT be answered from data — it needs the Row"
        );
    }

    /// Pins every arithmetic step of the `time` conversion.
    #[test]
    fn naive_time_to_nanos_pins_every_arithmetic_step() {
        let t = NaiveTime::from_hms_nano_opt(1, 2, 3, 456_789_000).unwrap();
        assert_eq!(naive_time_to_nanos(t), 3723 * 1_000_000_000 + 456_789_000);
        let t = NaiveTime::from_hms_nano_opt(0, 0, 0, 100).unwrap();
        assert_eq!(
            naive_time_to_nanos(t),
            100,
            "the 100ns tick of a time(7) is kept"
        );
        let t = NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_900).unwrap();
        assert_eq!(naive_time_to_nanos(t), 86_399 * 1_000_000_000 + 999_999_900);
    }

    #[test]
    fn numeric_renders_exact_decimal() {
        assert_eq!(numeric_to_decimal_string(15005, 2), "150.05");
        assert_eq!(numeric_to_decimal_string(-7500, 3), "-7.500");
        assert_eq!(numeric_to_decimal_string(42, 0), "42");

        assert_eq!(
            numeric_to_decimal_string(5, 1),
            "0.5",
            "one digit at scale 1 is the boundary; `<` emits `.5`"
        );
        assert_eq!(numeric_to_decimal_string(-5, 1), "-0.5");
        assert_eq!(numeric_to_decimal_string(5, 3), "0.005");
        assert_eq!(numeric_to_decimal_string(0, 2), "0.00");
        assert_eq!(numeric_to_decimal_string(5, 2), "0.05");
    }

    #[test]
    fn only_the_retention_throw_number_is_a_log_gap() {
        assert!(is_retention_gap(Some(51000)));
        assert!(!is_retention_gap(Some(208)));
        assert!(!is_retention_gap(None));
    }

    #[test]
    fn fill_sql_bounded_pins_max_and_daemon_chases_it() {
        let bounded = fill_sql(Poll {
            ci: "dbo_orders",
            from_expr: "sys.fn_cdc_get_min_lsn('dbo_orders')",
            batch: 500,
            bound: Some("0000002f000004d80005"),
        });
        assert!(
            bounded.contains("DECLARE @max binary(10) = 0x0000002f000004d80005;"),
            "bounded poll must pin @max to the open-time LSN: {bounded}"
        );
        assert!(
            !bounded.contains("fn_cdc_get_max_lsn"),
            "bounded poll must not consult the moving max LSN: {bounded}"
        );
        assert!(bounded.contains("@from < @min THROW 51000"));
        assert!(bounded.contains("TOP (500)"));

        let daemon = fill_sql(Poll {
            ci: "dbo_orders",
            from_expr: "sys.fn_cdc_increment_lsn(0xabcd)",
            batch: 500,
            bound: None,
        });
        assert!(
            daemon.contains("DECLARE @max binary(10) = sys.fn_cdc_get_max_lsn();"),
            "daemon poll keeps chasing the head: {daemon}"
        );
    }

    /// The poll has no way to move a position up: below the low watermark is always the refusal.
    #[test]
    fn a_position_below_the_low_watermark_always_reaches_the_refusal() {
        let sql = fill_sql(Poll {
            ci: "dbo_orders",
            from_expr: "sys.fn_cdc_increment_lsn(0xabcd)",
            batch: 500,
            bound: None,
        });
        assert!(!sql.contains("SET @from"), "no floor: {sql}");
        assert!(
            sql.contains(
                "DECLARE @min binary(10) = (SELECT start_lsn FROM cdc.change_tables WHERE \
                 capture_instance = 'dbo_orders');"
            ),
            "the guard reads the instance's start, which exists before the capture job reaches it: {sql}"
        );
        assert!(
            sql.contains(
                "IF @from IS NOT NULL AND @min IS NOT NULL AND @from < @min THROW 51000, \
                 'rivet cdc: the resume position is older than the SQL Server CDC change-table \
                 retention (the cleanup job removed it). Resuming would silently skip changes. \
                 Re-baseline the stream in one run:"
            ),
            "{sql}"
        );
    }

    fn cfg(capture_instance: &str) -> MssqlCdcConfig {
        MssqlCdcConfig {
            host: "127.0.0.1".into(),
            port: 1434,
            database: "rivet".into(),
            user: "sa".into(),
            password: "Rivet_Passw0rd!".into(),
            capture_instance: capture_instance.into(),
            from_lsn: None,
            checkpoint_identity: None,
        }
    }

    /// Run arbitrary T-SQL on a throwaway connection (test setup helper).
    fn exec(sql: &str) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut c = connect(&cfg("dbo_cdc_unit"), None).await.unwrap();
            for batch in sql.split(";\n") {
                if !batch.trim().is_empty() {
                    c.simple_query(batch)
                        .await
                        .unwrap()
                        .into_results()
                        .await
                        .unwrap();
                }
            }
        });
    }

    #[test]
    #[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC enabled"]
    fn streams_change_operations() {
        exec(
            "IF OBJECT_ID('cdc_unit','U') IS NOT NULL DROP TABLE cdc_unit;\n\
             CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT);\n\
             IF (SELECT is_cdc_enabled FROM sys.databases WHERE name='rivet')=0 EXEC sys.sp_cdc_enable_db;\n\
             IF EXISTS(SELECT 1 FROM cdc.change_tables WHERE capture_instance='dbo_cdc_unit') \
               EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='cdc_unit',@capture_instance='dbo_cdc_unit';\n\
             EXEC sys.sp_cdc_enable_table @source_schema='dbo',@source_name='cdc_unit',@role_name=NULL,@capture_instance='dbo_cdc_unit',@supports_net_changes=0",
        );
        exec(
            "INSERT INTO cdc_unit VALUES (1,10);\n\
             UPDATE cdc_unit SET v=20 WHERE id=1;\n\
             DELETE FROM cdc_unit WHERE id=1",
        );
        std::thread::sleep(std::time::Duration::from_secs(8));

        let mut s = MssqlChangeStream::open(
            &cfg("dbo_cdc_unit"),
            None,
            crate::source::cdc::PeekBound::Sized(10_000),
            DrainMode::Continuous,
            &[],
        )
        .unwrap();
        let mut ops = Vec::new();
        while let Some(ev) = s.next_change() {
            ops.push(ev.unwrap().op);
        }
        exec(
            "EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='cdc_unit',@capture_instance='dbo_cdc_unit'",
        );
        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete],
            "CDC change table must yield insert(2), update-after(4), delete(1)"
        );
    }

    /// The catalog query is scoped by the capture instance alone, quote-escaped, whatever tables are listed.
    #[test]
    fn the_row_image_query_names_the_capture_instance_and_escapes_it() {
        let sql = row_image_sql("dbo_orders", None);
        assert!(
            sql.contains("WHERE ct.capture_instance = 'dbo_orders' GROUP BY t.name, t.object_id"),
            "{sql}"
        );
        assert!(
            row_image_sql("a'b", None).contains("ct.capture_instance = 'a''b'"),
            "a quote in the instance name is doubled"
        );
    }

    /// The denominator is the table's column count, or the readable count under `columns_from: grants`.
    #[test]
    fn the_row_image_query_counts_against_the_table_or_the_readable_columns() {
        assert!(
            row_image_sql("ci", None)
                .contains("CAST((SELECT COUNT(*) FROM sys.columns sc WHERE sc.object_id = t.object_id) AS varchar(12))")
        );
        let grants = row_image_sql("ci", Some(2));
        assert!(grants.contains("'/' + CAST(2 AS varchar(12))"), "{grants}");
        assert!(!grants.contains("sys.columns"), "{grants}");
    }

    /// Under `columns_from: grants` a capture of exactly the readable columns is whole and any other count is partial.
    #[test]
    fn the_grants_verdict_accepts_a_subset_capture_and_refuses_any_other_count() {
        use crate::source::cdc::RowImage;
        assert!(matches!(
            row_image_verdict(&["t:2/2".into()], &[], true),
            RowImage::Whole
        ));
        for bad in ["t:3/2", "t:1/2"] {
            match row_image_verdict(&[bad.into()], &[], true) {
                RowImage::Partial { why } => assert!(
                    why.contains("do not carry exactly the columns this account may SELECT")
                        && why.contains("@captured_column_list"),
                    "{why}"
                ),
                other => panic!("{bad} must refuse, got {other:?}"),
            }
        }
        assert!(
            matches!(
                row_image_verdict(&["t:2/3".into()], &[], false),
                RowImage::Partial { .. }
            ),
            "without the policy a subset capture is still partial"
        );
    }

    /// #161: both directions of the got/all capture-column verdict.
    #[test]
    fn row_image_verdict_both_directions() {
        use crate::source::cdc::RowImage;
        assert!(matches!(
            row_image_verdict(&["orders_ci:5/5".into(), "garbage".into()], &[], false),
            RowImage::Whole
        ));
        assert!(matches!(
            row_image_verdict(&[], &[], false),
            RowImage::Whole
        ));
        match row_image_verdict(&["t:2/2".into()], &["dbo.t.v".into()], false) {
            RowImage::Partial { why } => assert!(why.contains("dbo.t.v") && why.contains('?')),
            other => panic!("a column in another codepage must refuse, got {other:?}"),
        }
        match row_image_verdict(&["orders_ci:3/5".into(), "items_ci:4/4".into()], &[], false) {
            RowImage::Partial { why } => {
                assert!(why.contains("orders_ci (3 of 5 columns)"), "{why}");
                assert!(
                    why.contains("Re-enable the table with the full column list"),
                    "{why}"
                );
                assert!(
                    !why.contains("items_ci"),
                    "complete instance must not be named: {why}"
                );
            }
            other => panic!("partial capture must refuse, got {other:?}"),
        }
    }

    /// Every `__$operation` code, both directions.
    #[test]
    fn map_op_maps_the_three_ops_and_skips_the_before_image() {
        assert_eq!(map_op(1), Some(ChangeOp::Delete));
        assert_eq!(map_op(2), Some(ChangeOp::Insert));
        assert_eq!(map_op(4), Some(ChangeOp::Update));
        for skipped in [0, 3, 5, -1, i32::MAX] {
            assert_eq!(
                map_op(skipped),
                None,
                "code {skipped} must be SKIPPED — op 3 is the update before-image \
                 (mapping it doubles every update), and an unknown code mapped to \
                 anything invents an operation"
            );
        }
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    use crate::source::cdc::Position;

    fn id(family: &str, fork: &str) -> DbIdentity {
        DbIdentity {
            family: family.into(),
            fork: fork.into(),
        }
    }

    #[test]
    fn a_resume_is_refused_on_another_database_or_a_rewound_log_and_only_then() {
        let here = id("F1", "K1");
        assert_eq!(
            identity_verdict(Some(&here), Some(&here)),
            IdentityVerdict::Ok
        );
        let IdentityVerdict::Foreign(why) = identity_verdict(Some(&here), Some(&id("F2", "K1")))
        else {
            panic!("another family must refuse");
        };
        assert!(why.starts_with("mssql cdc: this checkpoint was written against a different database (family F1, the connection's is F2)"), "{why}");
        let IdentityVerdict::Foreign(why) = identity_verdict(Some(&here), Some(&id("F1", "K2")))
        else {
            panic!("another fork must refuse");
        };
        assert!(why.starts_with("mssql cdc: this database was restored from a backup since the checkpoint was written (recovery fork K1 is now K2)"), "{why}");
        assert!(matches!(
            identity_verdict(None, Some(&here)),
            IdentityVerdict::Unverifiable(w) if w.contains("carries no database identity")
        ));
        assert!(matches!(
            identity_verdict(Some(&here), None),
            IdentityVerdict::Unverifiable(w) if w.contains("cannot read sys.database_recovery_status")
        ));
    }

    #[test]
    fn a_foreign_or_rewound_checkpoint_refuses_with_exit_5_and_the_anchor_first_order() {
        let here = id("F1", "K1");
        for server in [id("F2", "K1"), id("F1", "K2")] {
            let err = identity_verdict(Some(&here), Some(&server))
                .enforce()
                .unwrap_err();
            assert_eq!(crate::error::classify_exit(&err), 5, "{err}");
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_SOURCE_CDC_FOREIGN_CHECKPOINT")
            );
            assert!(
                err.to_string()
                    .ends_with(crate::source::cdc::checkpoint_identity::RECOVER),
                "{err}"
            );
        }
    }

    #[test]
    fn a_checkpoint_records_the_identity_and_reads_it_back() {
        let pos = Position::new(serde_json::json!({ "lsn": "0a0b", "pinned": true }));
        let saved = with_identity(&pos, Some(&id("F1", "K1")));
        assert_eq!(
            saved.json(),
            &serde_json::json!({ "lsn": "0a0b", "pinned": true, "family_guid": "F1", "recovery_fork_guid": "K1" })
        );
        assert_eq!(DbIdentity::from_checkpoint(&saved), Some(id("F1", "K1")));
        assert_eq!(
            with_identity(&pos, None).0,
            pos.0,
            "no identity leaves the position as is"
        );
        assert_eq!(DbIdentity::from_checkpoint(&pos), None);
    }
}
