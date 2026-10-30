//! MySQL CDC adapter — binlog ROW stream → canonical [`cdc::ChangeEvent`].
//!
//! The sync `mysql` crate (already a rivet dependency) performs the replica
//! handshake + event decoding under the `binlog` feature; this adapts its
//! `Event` stream to the shared [`ChangeStream`] seam (no async runtime, unlike
//! `mysql_async`). The driver, checkpoint, and output live in
//! [`crate::source::cdc`]; this file is only the MySQL adapter.
//!
//! Source prerequisites: `log_bin = ON`, `binlog_format = ROW` (ideally
//! `binlog_row_image = FULL` for complete UPDATE pre-images), a `REPLICATION
//! SLAVE` grant, and a `server_id` distinct from the source's.
//!
//! `#![allow(dead_code)]`: the consumer is `cli::dispatch` (the `rivet cdc`
//! command) in the binary crate; the lib crate compiles `source` for tests but
//! has no CDC consumer of its own.
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::path::Path;

use mysql::binlog::events::{
    EventData, QueryEvent, RowsEventData, StatusVarVal, StatusVarsIterator, TableMapEvent,
};
use mysql::binlog::value::BinlogValue;
use mysql::prelude::Queryable;
use mysql::{BinlogDumpFlags, BinlogRequest, BinlogStream, Conn};
use serde_json::{Value as Json, json};

use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::cdc::checkpoint_identity::IdentityVerdict;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, DrainMode, Position};
use crate::source::mysql::statement::{SqlMode, Tok, UNPLACED, charset_name, client_text, tokens};

/// A blocking iterator of canonical [`ChangeEvent`]s over a MySQL binlog stream.
///
/// A single binlog ROWS event carries many rows, so decoded changes are buffered
/// in `pending` and drained one at a time. A `TableMapEvent` (which always
/// precedes its ROWS event and carries the table identity + column metadata) is
/// cached by `table_id` and applied to the following rows. `file` tracks the
/// current binlog file across `ROTATE` events — it plus the event header's
/// position form the [`Position`] checkpoint.
/// One TABLE_MAP's decode context: the owned event + its parsed column names.
type CachedTableMap = (
    std::sync::Arc<TableMapEvent<'static>>,
    Option<std::sync::Arc<[String]>>,
);

/// How long the server keeps the binlog dump open while rivet is busy flushing a roll to the store (the server default is 60 s).
const DUMP_WRITE_TIMEOUT_SECS: u32 = 3600;

pub(crate) struct MysqlChangeStream {
    stream: BinlogStream,
    /// TABLE_MAP cache: the event (Arc — rows-event decode borrows it) plus
    /// the column names parsed from its optional metadata ONCE. The first cut
    /// re-parsed the metadata and re-allocated every name on EVERY rows-event
    /// — thousands of identical parses inside a single large transaction.
    tables: HashMap<u64, CachedTableMap>,
    pending: VecDeque<ChangeEvent>,
    /// The current transaction, held until its commit marker so it is released
    /// atomically with the commit position; past the memory cap its tail spills
    /// through the general tagged frame (the crate hands over PARSED events, so there
    /// is no wire to keep). A FIELD, not a local: a MySQL transaction spans many
    /// `fill` calls, one per binlog event.
    tx: crate::source::cdc::tx_buffer::TxBuffer,
    /// A committed transaction whose tail is still being handed out from disk.
    spooled: Option<crate::source::cdc::tx_buffer::SpooledTail>,
    /// A failure while SEALING a spill, raised on the next `next()`.
    ///
    /// `close_transaction_at` returns a bool (it means "keep reading"), so it has
    /// nowhere to put an error. Dropping one would deliver the head of a
    /// transaction without its tail — a half-transaction the sink would flush and
    /// checkpoint past, which is the loss this whole path exists to avoid.
    spill_error: Option<anyhow::Error>,
    file: String,
    /// Open-time `(binlog_file, pos)` ceiling for a bounded run — the first
    /// commit past it ends the stream (`BINLOG_DUMP_NON_BLOCK`'s EOF stays as
    /// the catch-up backstop, which backpressure alone can push out
    /// indefinitely); `None` (daemon) streams forever. The contract lives on
    /// [`DrainMode`].
    bound: Option<(String, u64)>,
    /// The `table:` values this run captures — the set an UNDECODABLE rows event is
    /// checked against before it is allowed to fail the run. Empty means "capture
    /// whatever the stream emits" (`rivet cdc` with no `--table`), which makes every
    /// event ours. See [`undecodable_event_is_ours`].
    configured_tables: Vec<String>,
    /// Bound tripped — the stream ended at the open-time ceiling. Sticky, so the
    /// sink's re-drain pass (which re-calls `next_change` after acking) returns
    /// `None` at once instead of consuming — and re-deferring — the past-bound
    /// events the next run will re-read from the checkpoint.
    past_bound: bool,
    /// The connection's own database — what a BARE configured name means. Compared
    /// against each event's schema on the wire; see `bare_name_spans_databases`.
    own_db: String,
    /// The database each bare configured name means ([`Self::bare_name_database`]), read at open.
    bare_db: HashMap<String, String>,
    /// Whether the server compares table and schema names without case (`lower_case_table_names` 1 or 2), read at open.
    fold_names: bool,
    /// The server's `(server_uuid, gtid_executed)` at open, written into EVERY
    /// position this stream persists — the anchor wrote it and the commit save
    /// dropped it, so one captured transaction disarmed the foreign-server refusal.
    identity: (String, String),
    /// `binlog_row_image`'s verdict, read at open on the connection that dumps.
    row_image: crate::source::cdc::RowImage,
    /// `columns_from: grants`: each configured table's readable columns; `None` = every column.
    readable: Option<HashMap<String, crate::source::cdc::ReadableColumns>>,
    /// `columns_from: grants`: the source the readable columns are re-read from at a grant change.
    grants_source: Option<(String, Option<TlsConfig>)>,
    /// `(file, position)` of the first privilege statement not yet answered by a re-read: consecutive statements share one.
    grants_pending: Option<(String, u64)>,
}

impl MysqlChangeStream {
    /// A checkpoint position at `log_pos` in the current file, carrying the server identity.
    fn position_at(&self, log_pos: u64) -> Position {
        Position::new(json!({
            "file": self.file,
            "pos": log_pos,
            "server_uuid": self.identity.0,
            "gtid_executed": self.identity.1,
        }))
    }

    /// Open a stream from an explicit `(binlog_file, pos)` coordinate.
    ///
    /// A [`DrainMode::BoundedAtOpen`] run sets `BINLOG_DUMP_NON_BLOCK` (the
    /// server streams to the current end and sends EOF instead of blocking —
    /// the catch-up backstop) AND pins the open-time coordinates as the stop
    /// ceiling (see [`Self::bound`]).
    /// Refuse to capture from a binlog that cannot carry a whole row.
    ///
    /// `binlog_row_image` decides what MySQL writes per row event. At `FULL` (the
    /// server default) every column is present in both images. At `MINIMAL` — and at
    /// `NOBLOB` for the blob columns — MySQL writes only the key and the columns
    /// that CHANGED, because that is all a replica needs to apply the statement.
    ///
    /// rivet's change stream is a FULL-ROW image per event: the parquet has every
    /// column, and a consumer builds current state by taking the latest row per key.
    /// A partial image cannot be turned into that, so the setting is not a
    /// performance knob here — it decides whether the output means anything.
    ///
    /// Measured against a real server at `MINIMAL`, three changes to a three-column
    /// table:
    ///
    /// ```text
    /// insert | 1 | 10.5000 | alpha        <- fine
    /// update |   |         |              <- NO KEY, no value: 99.9 is gone
    /// delete | 2 |         |              <- key only
    /// ```
    ///
    /// and the run printed `status: success`, `rows: 4`, with no warning of any
    /// kind. An UPDATE row with a NULL key cannot even be applied to the right row,
    /// so this is not degradation, it is a corrupt event that counts as captured.
    ///
    /// Hence a refusal rather than a warning: there is no partially-correct outcome
    /// to let the operator choose. The check is one query at open, on the connection
    /// that is about to dump.
    /// Pure verdict half of [`Self::row_image`] (#161, the compression_refusal
    /// split): `@@global.binlog_row_image` → keep or refuse. `None` (older
    /// MySQL / MariaDB without the variable) is NOT evidence of a bad setting —
    /// refusing on absence would lock out binlogs that are fine. Unit-tested in
    /// both directions; the connect+query half stays live-guarded.
    pub(crate) fn row_image_verdict(image: Option<&str>) -> super::super::cdc::RowImage {
        use super::super::cdc::RowImage;
        let Some(image) = image else {
            return RowImage::Whole;
        };
        if image.eq_ignore_ascii_case("FULL") {
            return RowImage::Whole;
        }
        RowImage::Partial {
            why: format!(
                "the server has binlog_row_image = {image}, which writes only the key and the \
                 CHANGED columns per row event. Measured against a real server at MINIMAL, an \
                 UPDATE arrives with NULL for every column it did not touch — including the KEY, \
                 which makes the event impossible to apply. Set `binlog_row_image = FULL` (the \
                 server default) and re-run"
            ),
        }
    }

    /// Read a resume position out of a checkpoint that has already PARSED.
    ///
    /// The MSSQL peer of this (`mssql::cdc::resume_from_checkpoint`) exists because
    /// the runner and `doctor` had drifted into two readings of one file. MySQL had
    /// the same split and the doctor side was the LOOSER one: it read `file` with
    /// `.unwrap_or_default()` and `pos` with `.unwrap_or(0)`, so a malformed
    /// checkpoint rendered as `Loaded { file: "", pos: 0 }` and was then graded
    /// "below binlog retention (file purged) — the next run fails with ERROR 1236",
    /// hinting RE-SNAPSHOT. Wrong cause, and a destructive remedy for a file that is
    /// merely malformed — the run's actual error is `checkpoint missing 'file'`.
    ///
    /// One decoder, so the two cannot disagree again.
    /// Does this checkpoint belong to the server we are about to resume against?
    ///
    /// A MySQL checkpoint is `{file, pos}` — binlog COORDINATES, which mean nothing
    /// outside the server that wrote them. Point rivet at a different host with the
    /// same config (a failover, a restored dump, a copied `url:`, a stand rebuilt
    /// under the same name) and `binlog.000042:12345` still parses, still looks
    /// plausible, and lands at an arbitrary point in a DIFFERENT binlog. Whatever
    /// is there gets captured; whatever was between gets skipped. Silent both ways.
    ///
    /// PostgreSQL cannot have this problem: its resume anchor is a slot, which is
    /// server-side, so a foreign one simply does not exist. MySQL's anchor is a
    /// client-side file, so the check has to be explicit — the same asymmetry
    /// the process rules's anchor-model rule already records for the idle-first-run case.
    ///
    /// TWO tiers, because one of them is not enough on a default install:
    ///
    /// * `server_uuid` is unique per server and stable across restarts, and it is
    ///   present on EVERY MySQL. This is the floor.
    /// * a GTID set catches what the uuid cannot: the SAME server after a
    ///   `RESET MASTER`, where the coordinates are stale but the identity matches.
    ///   Only when `gtid_mode` is ON — which is OFF by default, measured on the
    ///   stand, so it can never be the only check.
    ///
    /// A checkpoint with NO identity is accepted with a warning rather than
    /// refused: it was written before this existed, and refusing it would strand a
    /// stream that is probably fine. Saying nothing would be the silent half of the
    /// bug this closes.
    pub(crate) fn checkpoint_identity_verdict(
        ckpt_uuid: Option<&str>,
        ckpt_gtid: Option<&str>,
        server_uuid: &str,
        server_gtid_executed: Option<&str>,
        gtid_contained: Option<bool>,
    ) -> CheckpointIdentity {
        let Some(ckpt_uuid) = ckpt_uuid else {
            return CheckpointIdentity::Unverifiable;
        };
        if ckpt_uuid != server_uuid {
            return CheckpointIdentity::ForeignServer {
                checkpoint: ckpt_uuid.to_string(),
                server: server_uuid.to_string(),
            };
        }
        // Same server. If the checkpoint recorded a GTID set and the server can
        // answer, the set must still be CONTAINED in what the server has executed —
        // a `RESET MASTER` empties it while leaving the uuid untouched.
        match (ckpt_gtid, gtid_contained) {
            (Some(g), Some(false)) if !g.is_empty() => CheckpointIdentity::GtidNotContained {
                checkpoint: g.to_string(),
                server: server_gtid_executed.unwrap_or_default().to_string(),
            },
            (Some(g), None) if !g.is_empty() => CheckpointIdentity::GtidUnanswered,
            _ => CheckpointIdentity::Ok,
        }
    }

    pub(crate) fn resume_from_checkpoint(
        pos: Option<&super::super::cdc::Position>,
        path: &str,
    ) -> Result<Option<(String, u64)>> {
        let Some(pos) = pos else {
            return Ok(None);
        };
        let file = pos
            .0
            .get("file")
            .and_then(Json::as_str)
            .ok_or_else(|| {
                crate::source::cdc::checkpoint_identity::checkpoint_invalid(format!(
                    "checkpoint '{path}' parses as JSON but carries no 'file' — refusing to \
                         treat it as absent, which would re-anchor at the CURRENT binlog \
                         position and silently skip every change since it was written. Restore \
                         the file, or: {}",
                    crate::source::cdc::checkpoint_identity::RECOVER
                ))
            })?
            .to_string();
        let p = pos.0.get("pos").and_then(Json::as_u64).ok_or_else(|| {
            crate::source::cdc::checkpoint_identity::checkpoint_invalid(format!(
                "checkpoint '{path}' parses as JSON but carries no 'pos'. Restore the file, \
                     or: {}",
                crate::source::cdc::checkpoint_identity::RECOVER
            ))
        })?;
        Ok(Some((file, p)))
    }

    /// A BARE configured name has matched an event from a schema that is NOT the
    /// connection's own database.
    ///
    /// DETERMINISTIC, and the first cut was not. It compared against the FIRST schema
    /// seen in the current run and kept that memory in a per-run `HashMap`, so a run
    /// in which only the foreign database wrote recorded IT as "first" and captured
    /// its rows in silence. MEASURED: `table: hunt_ord` with `rivet.hunt_ord` and
    /// `hunt_other.hunt_ord` present, one insert into the foreign one, `status:
    /// success, rows: 1` and an independent read showing `insert | 99 |
    /// FOREIGN-ROW` — a row from a database the config never named and the
    /// connection cannot see. Under `until_current` on a schedule, "only one of the
    /// two wrote this cycle" is the ORDINARY case, not the rare one.
    ///
    /// The rule now matches what the rest of the product already means by a bare
    /// name: rivet's own routability check reads it as `TABLE_SCHEMA = DATABASE()`,
    /// so an event from any other schema is not what the operator asked for. No
    /// memory, no run boundary, no ordering.
    ///
    /// The catalog cannot answer this on MySQL, which is why it is checked on the
    /// WIRE: `information_schema` is filtered by PRIVILEGE while `REPLICATION SLAVE`
    /// hands over the whole server's binlog — as root the catalog listed
    /// `other_db, rivet`, as the `rivet` user it listed `rivet` alone, and the export
    /// received other_db's row regardless.
    pub(crate) fn bare_name_spans_databases(
        configured: &str,
        own_db: &str,
        saw: &str,
    ) -> Option<String> {
        if configured.contains('.') || saw == own_db {
            return None;
        }
        Some(format!(
            "mysql cdc: `{configured}` is unqualified, so it means the connection's own \
             database (`{own_db}`) — but an event for that name arrived from `{saw}`. The \
             binlog dump is server-wide and routing matches a bare name in any schema, so \
             `{saw}.{configured}`'s rows would land in this export under THIS table's \
             name. No catalog check can warn about \
             this: information_schema is filtered by privilege, so a database this \
             connection cannot see still reaches the binlog. Qualify it \
             (`{saw}.{configured}`) if that is the one you meant."
        ))
    }

    /// The database a BARE configured name means, from the catalog's relations of that name: the
    /// connection's own database when it holds one, else the only database holding a base table of
    /// it; `None` when none or several do. The one rule the catalog check, the binlog routing and
    /// the baseline read all take.
    pub(crate) fn bare_name_database(
        own_db: &str,
        matches: &[crate::source::cdc::identity::CatalogMatch],
    ) -> Option<String> {
        if !own_db.is_empty() && matches.iter().any(|m| m.schema == own_db) {
            return Some(own_db.to_string());
        }
        let mut base: Vec<&str> = matches
            .iter()
            .filter(|m| m.capturable)
            .map(|m| m.schema.as_str())
            .collect();
        base.sort_unstable();
        base.dedup();
        match base.as_slice() {
            [one] => Some((*one).to_string()),
            _ => None,
        }
    }

    /// Every relation outside the system databases the catalog names `table`, views included.
    fn catalog_matches_of(
        conn: &mut mysql::Conn,
        table: &str,
    ) -> Vec<crate::source::cdc::identity::CatalogMatch> {
        use mysql::prelude::Queryable as _;
        conn.exec_map(
            // NO kind filter: a VIEW sharing the name is a CANDIDATE, not something to hide.
            "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
             FROM information_schema.TABLES \
             WHERE TABLE_NAME = ? \
               AND TABLE_SCHEMA NOT IN \
                   ('mysql','information_schema','performance_schema','sys')",
            (table,),
            |(schema, table, kind): (String, String, String)| {
                // Only a BASE TABLE reaches a binlog TABLE_MAP; a VIEW never emits under its own name.
                let capturable = kind == "BASE TABLE";
                crate::source::cdc::identity::CatalogMatch {
                    schema,
                    table,
                    kind,
                    capturable,
                }
            },
        )
        .unwrap_or_default()
    }

    /// The database each BARE configured name means on this connection, by [`Self::bare_name_database`].
    fn bare_name_databases(
        conn: &mut mysql::Conn,
        own_db: &str,
        configured: &[String],
    ) -> HashMap<String, String> {
        configured
            .iter()
            .filter(|c| !c.contains('.'))
            .filter_map(|c| {
                Self::bare_name_database(own_db, &Self::catalog_matches_of(conn, c))
                    .map(|db| (c.clone(), db))
            })
            .collect()
    }

    /// The relation each configured name reads for its baseline: `<db>.<table>` for a bare name
    /// the rule resolves, the name itself otherwise; the stream routes by the same answer.
    pub(crate) fn baseline_reads(
        url: &str,
        tls: Option<&TlsConfig>,
        configured: &[String],
    ) -> Result<HashMap<String, String>> {
        use mysql::prelude::Queryable as _;
        let mut conn = connect_conn(url, tls)?;
        let own_db: String = conn
            .query_first::<Option<String>, _>("SELECT DATABASE()")
            .ok()
            .flatten()
            .flatten()
            .unwrap_or_default();
        Ok(Self::bare_name_databases(&mut conn, &own_db, configured)
            .into_iter()
            .map(|(cfg, db)| {
                let read = format!("{db}.{cfg}");
                (cfg, read)
            })
            .collect())
    }

    /// Refusal text unless `@@global.binlog_row_metadata` is FULL (`None`: the server has no such variable).
    pub(crate) fn row_metadata_refusal(metadata: Option<&str>) -> Option<String> {
        let setting = match metadata {
            Some(m) if m.eq_ignore_ascii_case("FULL") => return None,
            Some(m) => format!("the server has binlog_row_metadata = {m}"),
            None => "the server has no binlog_row_metadata variable (MySQL before 8.0.1)".into(),
        };
        Some(format!(
            "mysql cdc: {setting}, so binlog row events carry no column NAMES and values could \
             only be mapped by POSITION: after a column reorder or ALTER they land under the \
             wrong names (source `(1, b='BBB', a='AAA')` arrives as `b='AAA', a='BBB'`). rivet \
             refuses to capture that. Fix: `SET PERSIST binlog_row_metadata = FULL;` (needs \
             SYSTEM_VARIABLES_ADMIN or SUPER), or `binlog_row_metadata = FULL` under [mysqld] in \
             the server's config file and a restart. Events already written under MINIMAL stay \
             nameless after the switch and cannot be read safely, so if this export already has \
             a checkpoint, after the switch: {}",
            crate::source::cdc::checkpoint_identity::RECOVER
        ))
    }

    /// The `binlog_row_metadata` read: a server that predates the variable (ER_UNKNOWN_SYSTEM_VARIABLE) reads as unset.
    fn row_metadata_or_legacy(
        read: std::result::Result<Option<String>, mysql::Error>,
    ) -> std::result::Result<Option<String>, mysql::Error> {
        match read {
            Err(mysql::Error::MySqlError(e)) if e.code == 1193 => Ok(None),
            other => other,
        }
    }

    /// Live half of [`Self::row_metadata_refusal`]: refuse at open unless the server writes column names.
    fn refuse_nameless_binlog(conn: &mut mysql::Conn) -> Result<()> {
        use mysql::prelude::Queryable;
        let m =
            Self::row_metadata_or_legacy(conn.query_first("SELECT @@global.binlog_row_metadata"))?;
        if let Some(why) = Self::row_metadata_refusal(m.as_deref()) {
            crate::rivet_bail!(crate::error::codes::SOURCE_CDC_PREREQUISITE, "{why}");
        }
        Ok(())
    }

    /// The open's refusals (row metadata, compression, replica re-log, routing), asked before any anchor or snapshot part is written.
    pub(crate) fn refuse_unmet_prerequisites(
        url: &str,
        tls: Option<&TlsConfig>,
        configured_tables: &[String],
        columns_from_grants: bool,
    ) -> Result<()> {
        let mut conn = connect_conn(url, tls)?;
        Self::refuse_unmet_prerequisites_on(&mut conn, configured_tables)?;
        if columns_from_grants {
            return Ok(());
        }
        Self::refuse_absent_tables_on(&mut conn, configured_tables)
    }

    /// Every refusal of the open, asked on `conn` before the anchor is written or the dump starts.
    fn refuse_unmet_prerequisites_on(
        conn: &mut mysql::Conn,
        configured_tables: &[String],
    ) -> Result<()> {
        Self::refuse_nameless_binlog(conn)?;
        refuse_compressed_binlog(conn)?;
        refuse_replica_without_relog(conn)?;
        Self::check_configured_tables_are_routable(conn, configured_tables)
    }

    /// Refuse a table the schema probe could not read, by preparing the probe's own statement: asked before a first run's anchor, never on a resume (a table dropped under a stream is the stream's own refusal).
    fn refuse_absent_tables_on(conn: &mut mysql::Conn, configured_tables: &[String]) -> Result<()> {
        use mysql::prelude::Queryable as _;
        let own_db: String = conn
            .query_first::<Option<String>, _>("SELECT DATABASE()")
            .ok()
            .flatten()
            .flatten()
            .unwrap_or_default();
        let bare_db = Self::bare_name_databases(conn, &own_db, configured_tables);
        for table in configured_tables {
            crate::source::cdc::validate_table_ident(table)?;
            // The relation the stream routes the name to, by the one rule.
            let read = bare_db
                .get(table)
                .map_or_else(|| table.clone(), |db| format!("{db}.{table}"));
            conn.prep(format!("SELECT * FROM {read}"))
                .map_err(|e| table_probe_refusal(table, server_error_code(&e), &e.to_string()))?;
        }
        Ok(())
    }

    /// The binlog row-image verdict, asked on a connection the caller holds.
    fn row_image_on(conn: &mut mysql::Conn) -> super::super::cdc::RowImage {
        use mysql::prelude::Queryable;
        let image: Option<String> = conn
            .query_first("SELECT @@global.binlog_row_image")
            .unwrap_or(None);
        Self::row_image_verdict(image.as_deref())
    }

    /// Live half: ask `information_schema` what each configured name IS, before
    /// the stream is read and long before any checkpoint advance. Decisions live
    /// in [`classify_mysql_routing`]; this is glue.
    ///
    /// Best-effort on the QUERY itself — a catalog permission error must not fail
    /// a capture that is otherwise fine, the same posture `row_image` takes. What
    /// is NOT best-effort is the verdict: once the catalog answers, a non-base
    /// relation bails.
    pub(crate) fn check_configured_tables_are_routable(
        conn: &mut mysql::Conn,
        configured: &[String],
    ) -> Result<()> {
        use mysql::prelude::Queryable as _;
        // The connection's own database — what a BARE configured name means.
        let own_db_for_resolution: String = conn
            .query_first::<Option<String>, _>("SELECT DATABASE()")
            .ok()
            .flatten()
            .flatten()
            .unwrap_or_default();
        for cfg in configured {
            let (schema, table) = match cfg.split_once('.') {
                Some((s, t)) => (Some(s.to_string()), t.to_string()),
                None => (None, cfg.clone()),
            };
            // RESOLUTION-FIRST, through the SAME decision PostgreSQL uses. An
            // operator meeting this class on two engines should not have to learn it
            // twice, and one refusal is one place a mutant can grade.
            //
            // The engine difference that stays, because it is real: MySQL's
            // `information_schema` is filtered by PRIVILEGE while `REPLICATION SLAVE`
            // hands over the whole server's binlog — MEASURED, as root the catalog
            // listed `other_db, rivet` and as the `rivet` user it listed `rivet`
            // alone, while the export captured other_db's row anyway. So resolution
            // closes what the catalog CAN see, and `bare_name_spans_databases` stays
            // as the wire-side backstop for what it cannot. Two guards, two different
            // sources of truth, deliberately.
            let matches: Vec<crate::source::cdc::identity::CatalogMatch> = match &schema {
                // A qualified name names one database; ambiguity cannot apply.
                Some(sch) => conn
                    .exec_map(
                        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
                         FROM information_schema.TABLES \
                         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                        (sch, &table),
                        |(schema, table, kind): (String, String, String)| {
                            // Only a BASE TABLE reaches a binlog TABLE_MAP; a VIEW
                            // never emits under its own name.
                            let capturable = kind == "BASE TABLE";
                            crate::source::cdc::identity::CatalogMatch {
                                schema,
                                table,
                                kind,
                                capturable,
                            }
                        },
                    )
                    .unwrap_or_default(),
                None => Self::catalog_matches_of(conn, &table),
            };
            // The resolved relation, and classification asks about THAT — not about a
            // second lookup of the configured string. The two queries used different
            // predicates (this one scans every non-system schema; the TABLE_TYPE one
            // pinned `TABLE_SCHEMA = DATABASE()`), so they could name DIFFERENT
            // relations. MEASURED: with a VIEW `rivet.vv` and a base table
            // `other_db.vv`, resolution names `other_db.vv` — unambiguously, since
            // the identity query filters BASE TABLE — and classification then refused
            // with "vv is a VIEW", sending the operator to fix a relation resolution
            // had already ruled out.
            let resolved = if matches.is_empty() {
                None
            } else {
                // A BARE name means the CONNECTION'S OWN database — the same rule
                // `bare_name_spans_databases` enforces on the wire, and the one
                // rivet's routability check has always used. So a candidate outside
                // it may never be ELECTED as the identity, only reported.
                //
                // MEASURED when it could be: with a VIEW `rivet.hunt_v2` and a base
                // table `hunt_other.hunt_v2`, resolution elected the foreign table —
                // the only capturable candidate — classification asked IT for a type,
                // got BASE TABLE, and the run went `status: success, rows: 0` while
                // the view the operator meant was skipped in silence. Before this it
                // was a hard refusal naming that exact harm.
                //
                // A LIVE foreign twin is still a refusal, because routing would
                // genuinely capture it — that is the catalog-visible half of the
                // ambiguity, and the wire guard covers the half the catalog cannot
                // see (privilege-filtered `information_schema`).
                let chosen = match schema {
                    Some(_) => None,
                    None => Self::bare_name_database(&own_db_for_resolution, &matches),
                };
                // A qualified twin in the SAME stream names the other database's table, while the
                // bare name routes by its own: one event would reach both.
                if let Some(db) = &chosen
                    && let Some(twin) = configured.iter().find(|c| {
                        c.split_once('.')
                            .is_some_and(|(s, t)| t == table && s != db)
                    })
                {
                    crate::rivet_bail!(
                        crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
                        "mysql cdc: `{cfg}` is unqualified, so it means `{db}.{cfg}`, while \
                         `{twin}` in the same stream names another database's table of that \
                         name. Qualify `{cfg}` (`{db}.{cfg}`) so each table is named once."
                    );
                }
                // Resolve within the database the bare name means; with no such database, among all.
                let for_resolution: Vec<_> = match &chosen {
                    Some(db) => matches
                        .iter()
                        .filter(|m| &m.schema == db)
                        .cloned()
                        .collect(),
                    None => matches.clone(),
                };
                let r = crate::source::cdc::identity::resolve_captured_table(cfg, &for_resolution)?;
                if let Some(note) = &r.note {
                    log::warn!("mysql cdc: {note}");
                }
                Some(r)
            };
            let row: Option<String> = match (&resolved, &schema) {
                // The relation resolution named. One catalog fact, one relation.
                (Some(r), _) => conn
                    .exec_first(
                        "SELECT TABLE_TYPE FROM information_schema.TABLES \
                         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                        (&r.schema, &r.table),
                    )
                    .unwrap_or(None),
                // Resolution found nothing — the catalog is privilege-filtered here,
                // so absence is normal and the old scoped lookups still answer.
                (None, Some(s)) => conn
                    .exec_first(
                        "SELECT TABLE_TYPE FROM information_schema.TABLES \
                         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                        (s, &table),
                    )
                    .unwrap_or(None),
                (None, None) => conn
                    .exec_first(
                        "SELECT TABLE_TYPE FROM information_schema.TABLES \
                         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
                        (&table,),
                    )
                    .unwrap_or(None),
            };
            if let MysqlRoutingVerdict::Never(why) = classify_mysql_routing(cfg, row.as_deref()) {
                crate::rivet_bail!(crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED, "{why}");
            }
        }
        Ok(())
    }

    pub(crate) fn open(
        url: &str,
        server_id: u32,
        file: String,
        pos: u64,
        mode: DrainMode,
        tls: Option<&TlsConfig>,
        configured_tables: Vec<String>,
    ) -> Result<Self> {
        let mut conn = connect_conn(url, tls)?;
        Self::refuse_unmet_prerequisites_on(&mut conn, &configured_tables)?;
        let identity = Self::server_identity(&mut conn)?;
        Self::open_on(
            conn,
            server_id,
            file,
            pos,
            mode,
            identity,
            configured_tables,
        )
    }

    /// The readable columns of each configured table when the run takes them from grants.
    pub(crate) fn readable_on(
        conn: &mut mysql::Conn,
        configured: &[String],
        columns_from_grants: bool,
    ) -> Result<Option<HashMap<String, crate::source::cdc::ReadableColumns>>> {
        if !columns_from_grants {
            return Ok(None);
        }
        if configured.is_empty() {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
                "mysql cdc: `columns_from: grants` needs the captured tables named"
            );
        }
        let own_db = Self::own_db_on(conn);
        let bare_db = Self::bare_name_databases(conn, &own_db, configured);
        super::column_grants::readable_columns_of(conn, configured, &own_db, &bare_db).map(Some)
    }

    /// The readable columns of `configured` as a batch read sees them: a bare name is the connection's own database.
    pub(crate) fn readable_for_batch(
        conn: &mut mysql::Conn,
        configured: &[String],
    ) -> Result<HashMap<String, crate::source::cdc::ReadableColumns>> {
        let own_db = Self::own_db_on(conn);
        super::column_grants::readable_columns_of(conn, configured, &own_db, &HashMap::new())
    }

    /// The grants of a `columns_from: grants` run, read once on one connection before the anchor.
    pub(crate) fn resolve_grants(
        url: &str,
        tls: Option<&TlsConfig>,
        configured: &[String],
        declared: &HashMap<String, Vec<String>>,
    ) -> Result<std::sync::Arc<HashMap<String, crate::source::cdc::ReadableColumns>>> {
        if configured.is_empty() {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
                "mysql cdc: `columns_from: grants` needs the captured tables named"
            );
        }
        let mut conn = connect_conn(url, tls)?;
        let own_db = Self::own_db_on(&mut conn);
        let bare_db = Self::bare_name_databases(&mut conn, &own_db, configured);
        super::column_grants::resolve_grants(&mut conn, configured, &own_db, &bare_db, declared)
            .map(std::sync::Arc::new)
    }

    /// Re-read the grants at this event unless it lets a pending re-read wait.
    fn refuse_changed_grants_at(&mut self, data: &Option<EventData<'_>>) -> Result<()> {
        if defers_the_grants_re_read(data) {
            return Ok(());
        }
        self.refuse_changed_grants()
    }

    /// Re-read the grants for the privilege statements met since the last re-read; refuse when a table's readable columns differ.
    fn refuse_changed_grants(&mut self) -> Result<()> {
        let Some((file, log_pos)) = self.grants_pending.take() else {
            return Ok(());
        };
        let (Some(before), Some((url, tls))) = (&self.readable, &self.grants_source) else {
            return Ok(());
        };
        let mut conn = connect_conn(url, tls.as_ref())?;
        let read = Self::readable_on(&mut conn, &self.configured_tables, true)?;
        let now = read.unwrap_or_default();
        let columns = |r: &HashMap<_, crate::source::cdc::ReadableColumns>, t: &String| {
            r.get(t).map(|c| c.columns.clone())
        };
        let Some(table) = self
            .configured_tables
            .iter()
            .find(|t| columns(before, t) != columns(&now, t))
        else {
            return Ok(());
        };
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_GRANTS_CHANGED,
            "mysql cdc: {} at {}:{log_pos}; nothing past it was delivered. Re-run: the run then \
             reads the columns as they are now",
            grants_difference(
                table,
                before
                    .get(table)
                    .map(|c| c.columns.as_slice())
                    .unwrap_or(&[]),
                now.get(table).map(|c| c.columns.as_slice()).unwrap_or(&[]),
            ),
            file
        );
    }

    /// Refuse when the grants the stream opens under are not the ones this run resolved before its anchor: one policy value per run.
    fn refuse_grants_moved_since_resolved(
        configured: &[String],
        resolved: &HashMap<String, crate::source::cdc::ReadableColumns>,
        now: &HashMap<String, crate::source::cdc::ReadableColumns>,
    ) -> Result<()> {
        for table in configured {
            let (before, after) = (resolved.get(table), now.get(table));
            if before.map(|c| &c.columns) == after.map(|c| &c.columns) {
                continue;
            }
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_GRANTS_CHANGED,
                "mysql cdc: {} between the start of this run and the stream's open; nothing was \
                 delivered by the stream. Re-run: the run then reads the columns as they are now",
                grants_difference(
                    table,
                    before.map(|c| c.columns.as_slice()).unwrap_or(&[]),
                    after.map(|c| c.columns.as_slice()).unwrap_or(&[]),
                )
            );
        }
        Ok(())
    }

    /// The readable columns of `tables` on a fresh connection: the snapshot legs' read, asked before the anchor.
    pub(crate) fn readable_columns_at(
        url: &str,
        tls: Option<&TlsConfig>,
        tables: &[String],
    ) -> Result<HashMap<String, crate::source::cdc::ReadableColumns>> {
        let mut conn = connect_conn(url, tls)?;
        Ok(Self::readable_on(&mut conn, tables, true)?.unwrap_or_default())
    }

    /// Whether the server folds the case of table and schema names (`lower_case_table_names` other than 0).
    fn fold_names_on(conn: &mut mysql::Conn) -> Result<bool> {
        use mysql::prelude::Queryable as _;
        let row: Option<(String, String)> = conn
            .query_first("SHOW GLOBAL VARIABLES WHERE Variable_name = 'lower_case_table_names'")
            .map_err(|e| {
                crate::error::coded(
                    crate::error::codes::SOURCE_QUERY_FAILED,
                    format!("mysql cdc: reading lower_case_table_names: {e}"),
                )
            })?;
        Ok(fold_names_from(row.as_ref().map(|(_, v)| v.as_str())))
    }

    /// `SELECT DATABASE()`, empty for a db-less URL.
    fn own_db_on(conn: &mut mysql::Conn) -> String {
        use mysql::prelude::Queryable as _;
        conn.query_first::<Option<String>, _>("SELECT DATABASE()")
            .ok()
            .flatten()
            .flatten()
            .unwrap_or_default()
    }

    /// Open the stream on `conn`: every question asked before the dump rides the
    /// connection that then dumps, so a bounded run costs the server ONE session.
    fn open_on(
        mut conn: mysql::Conn,
        server_id: u32,
        file: String,
        pos: u64,
        mode: DrainMode,
        identity: (String, String),
        configured_tables: Vec<String>,
    ) -> Result<Self> {
        // Snapshot the ceiling BEFORE the dump starts: every commit already in
        // the log is ≤ it, and anything racing in after is the next run's work.
        let bound = if mode.is_bounded() {
            Some(Self::current_coordinates(&mut conn)?)
        } else {
            None
        };
        let row_image = Self::row_image_on(&mut conn);
        // Read the connection's own database BEFORE the binlog stream consumes the
        // connection — it is the meaning of a bare configured name.
        // `Option<String>`, like the two sibling call sites (`mysql/mod.rs`,
        // `preflight/mysql.rs`). A db-less URL is a DELIBERATELY supported shape —
        // `cdc/mod.rs` says so in prose and skips its own query for a qualified name
        // — and `SELECT DATABASE()` then returns NULL. Deserialising that into
        // `String` PANICS inside `FromRow`, which `.ok().flatten()` cannot catch:
        // MEASURED `exit 101`, no run summary, no error path, no ledger finalize.
        // The value is not even consulted for a qualified name, so the crash was
        // pure collateral.
        let own_db: String = conn
            .query_first::<Option<String>, _>("SELECT DATABASE()")
            .ok()
            .flatten()
            .flatten()
            .unwrap_or_default();
        let bare_db = Self::bare_name_databases(&mut conn, &own_db, &configured_tables);
        let fold_names = Self::fold_names_on(&mut conn)?;
        conn.query_drop(format!(
            "SET SESSION net_write_timeout = {DUMP_WRITE_TIMEOUT_SECS}"
        ))
        .map_err(|e| {
            crate::error::coded(
                crate::error::codes::SOURCE_QUERY_FAILED,
                format!("mysql cdc: raising net_write_timeout on the binlog connection: {e}"),
            )
        })?;
        let mut req = BinlogRequest::new(server_id)
            .with_filename(file.clone().into_bytes())
            .with_pos(pos);
        if mode.is_bounded() {
            req = req.with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK);
        }
        let stream = conn
            .get_binlog_stream(req)
            .map_err(|e| binlog_read_error(e, &file))?;
        Ok(Self {
            stream,
            configured_tables,
            tables: HashMap::new(),
            pending: VecDeque::new(),
            spooled: None,
            spill_error: None,
            // Given its spill directory by `open_or_resume`, the one production constructor.
            tx: crate::source::cdc::tx_buffer::TxBuffer::new(
                crate::source::cdc::CdcEngine::Mysql,
                None,
            ),
            file,
            bound,
            past_bound: false,
            // The connection's own database — the meaning of a bare configured
            // name, and the wire-side comparison's fixed reference.
            own_db,
            bare_db,
            fold_names,
            identity,
            row_image,
            readable: None,
            grants_source: None,
            grants_pending: None,
        })
    }

    /// The source's current `(binlog_file, position)` coordinate.
    /// MySQL 8.4 REMOVED `SHOW MASTER STATUS` (finding #36, caught by the
    /// version scout); its replacement `SHOW BINARY LOG STATUS` does not exist
    /// before 8.2 — try the new form first, fall back to the old.
    /// The server's own identity, recorded into every checkpoint rivet writes.
    ///
    /// `server_uuid` is present on every MySQL and stable across restarts;
    /// `gtid_executed` is empty unless `gtid_mode` is ON (which is OFF by default —
    /// measured, and the reason the uuid is the floor and GTID only ever an extra).
    fn server_identity(c: &mut mysql::Conn) -> Result<(String, String)> {
        use mysql::prelude::Queryable;
        // Fault hook: the identity query's transient failure is the state the
        // no-swallowing contract exists for, and a healthy stand can never
        // produce it — without this, the mutant restoring `unwrap_or_default`
        // is equivalent on every test environment.
        crate::test_hook::maybe_fail_at("mysql_identity_query")?;
        let uuid: String = c.query_first("SELECT @@server_uuid")?.ok_or_else(|| {
            crate::error::coded(
                crate::error::codes::SOURCE_CDC_PREREQUISITE,
                "mysql: server reported no @@server_uuid".to_string(),
            )
        })?;
        let gtid: String = c.query_first("SELECT @@gtid_executed")?.unwrap_or_default();
        Ok((uuid, gtid))
    }

    /// Ask the SERVER whether the checkpoint's GTID set is still contained in what
    /// it has executed. `GTID_SUBSET` is the server's own comparison — reimplementing
    /// set containment over GTID ranges here would be a second definition to drift.
    fn gtid_is_contained(
        c: &mut mysql::Conn,
        subset: &str,
        superset: &str,
    ) -> Result<Option<bool>> {
        use mysql::prelude::Queryable;
        if subset.is_empty() {
            return Ok(None);
        }
        crate::test_hook::maybe_fail_at("mysql_gtid_subset_query")?;
        Ok(c.exec_first("SELECT GTID_SUBSET(?, ?)", (subset, superset))?)
    }

    fn current_coordinates(c: &mut mysql::Conn) -> Result<(String, u64)> {
        let row: mysql::Row = match c.query_first("SHOW BINARY LOG STATUS") {
            Ok(Some(row)) => row,
            // The 8.2+ statement EXISTS and returned nothing ⇒ binlog is
            // definitively disabled — falling back would surface 8.4's
            // "SHOW MASTER STATUS" syntax error instead of the real cause.
            Ok(None) => return Err(binlog_disabled("binlog status empty")),
            // Err ⇒ pre-8.2 server without the new statement — legacy form.
            Err(_) => c
                .query_first("SHOW MASTER STATUS")?
                .ok_or_else(|| binlog_disabled("SHOW MASTER STATUS empty"))?,
        };
        let file: String = row.get(0).expect("binlog file column");
        let pos: u64 = row.get(1).expect("binlog pos column");
        Ok((file, pos))
    }

    /// Persist the source's CURRENT position to `ckpt` — the anchor for
    /// `cdc.initial: snapshot` (taken BEFORE the snapshot read, so everything
    /// changed during the snapshot also appears in the stream). Idempotent-by-
    /// caller: only invoked when no checkpoint exists yet.
    pub(crate) fn pin_checkpoint_at_current(
        url: &str,
        ckpt: &Path,
        tls: Option<&TlsConfig>,
    ) -> Result<()> {
        let mut conn = connect_conn(url, tls)?;
        let (file, pos) = Self::current_coordinates(&mut conn)?;
        Self::write_anchor(ckpt, &file, pos, &Self::server_identity(&mut conn))
    }

    /// Persist `file:pos` with the server's identity, refusing to write an anchor it cannot verify later.
    fn write_anchor(
        ckpt: &Path,
        file: &str,
        pos: u64,
        identity: &Result<(String, String)>,
    ) -> Result<()> {
        // `?`, never `unwrap_or_default`: a transient blip of the identity query
        // wrote a checkpoint with an EMPTY uuid, silently disabling the
        // foreign-server refusal (measured on a loaded CI runner). Refusing is
        // retryable and loud; a half-anchor is forever.
        let (uuid, gtid) = identity.as_ref().map_err(|e| {
            crate::error::recoded(
                e,
                crate::error::codes::SOURCE_QUERY_FAILED,
                format!(
                    "mysql cdc: could not record the server's identity for the new \
                 checkpoint ({e:#}) — refusing to write an UNVERIFIABLE anchor: a \
                 checkpoint without a server_uuid cannot be checked against a \
                 foreign server on resume"
                ),
            )
        })?;
        Position::new(serde_json::json!({
            "file": file, "pos": pos, "server_uuid": uuid, "gtid_executed": gtid
        }))
        .save(ckpt)
    }

    /// Open a stream from the source's *current* position (`SHOW MASTER STATUS`).
    /// Test-only convenience: every production path goes through
    /// [`Self::open_or_resume`].
    #[cfg(test)]
    pub(crate) fn open_from_current(
        url: &str,
        server_id: u32,
        mode: DrainMode,
        tls: Option<&TlsConfig>,
        tables: &[&str],
    ) -> Result<Self> {
        let (file, pos) = Self::current_coordinates(&mut connect_conn(url, tls)?)?;
        // Each test captures only its OWN table: tests run concurrently, and a stream
        // over every table would take a sibling test's DROP as a captured table's.
        let tables = tables.iter().map(|t| t.to_string()).collect();
        Self::open(url, server_id, file, pos, mode, tls, tables)
    }

    /// Resume from a persisted [`Position`] checkpoint, or start from the current
    /// position on the first run (no checkpoint yet). The MySQL position shape is
    /// `{"file": String, "pos": u64}`. Bounded/continuous per `mode` (see
    /// [`Self::open`]).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_or_resume(
        url: &str,
        server_id: u32,
        ckpt: Option<&Path>,
        mode: DrainMode,
        tls: Option<&TlsConfig>,
        configured_tables: Vec<String>,
        resolved_grants: Option<
            std::sync::Arc<HashMap<String, crate::source::cdc::ReadableColumns>>,
        >,
        spill_dir: Option<std::path::PathBuf>,
    ) -> Result<Self> {
        // PASSED IN, not derived: resolving the directory needs the CONFIG's dir
        // (the anchor for every relative path this run resolves), which only the
        // caller holds. An earlier version derived it here from the checkpoint
        // alone and fell back to a cwd-relative path — the shipped image runs at
        // `/`, where that is an EACCES at the exact moment the cap is crossed.
        let mut conn = connect_conn(url, tls)?;
        Self::refuse_unmet_prerequisites_on(&mut conn, &configured_tables)?;
        let readable = Self::readable_on(&mut conn, &configured_tables, resolved_grants.is_some())?;
        if let (Some(read), Some(resolved)) = (&readable, &resolved_grants) {
            Self::refuse_grants_moved_since_resolved(&configured_tables, resolved, read)?;
        }
        let grants_source = readable.is_some().then(|| (url.to_string(), tls.cloned()));
        let projecting = readable.is_some();
        let with_dir = |mut s: Self| {
            s.tx = crate::source::cdc::tx_buffer::TxBuffer::new(
                crate::source::cdc::CdcEngine::Mysql,
                spill_dir.clone(),
            );
            s.readable = readable;
            s.grants_source = grants_source;
            s
        };
        if let Some(path) = ckpt
            && let Some(pos) = Position::load(path)?
            && let Some((file, p)) =
                Self::resume_from_checkpoint(Some(&pos), &path.display().to_string())?
        {
            // The checkpoint's coordinates address a SPECIFIC server's binlog. Verify
            // it is this one before resuming: `binlog.000042:12345` parses on every
            // host and means something different on each, so a foreign checkpoint
            // starts the stream at an arbitrary point — capturing whatever is there
            // and skipping whatever was between, silently in both directions.
            let ckpt_uuid = pos.0.get("server_uuid").and_then(Json::as_str);
            let ckpt_gtid = pos.0.get("gtid_executed").and_then(Json::as_str);
            let (server_uuid, server_gtid) = Self::server_identity(&mut conn)?;
            let contained = ckpt_gtid
                .filter(|g| !g.is_empty())
                .map(|g| Self::gtid_is_contained(&mut conn, g, &server_gtid))
                .transpose()?
                .flatten();
            let verdict = Self::checkpoint_identity_verdict(
                ckpt_uuid,
                ckpt_gtid,
                &server_uuid,
                Some(server_gtid.as_str()),
                contained,
            );
            verdict.verdict().enforce()?;
            let identity = (server_uuid, server_gtid);
            return Self::open_on(conn, server_id, file, p, mode, identity, configured_tables)
                .map(with_dir);
        }
        // First run (no checkpoint yet): anchor at the current position and persist
        // it IMMEDIATELY. PostgreSQL pins its anchor server-side at open (slot
        // creation); MySQL's only anchor is this file, and the sink writes it only
        // at a part commit — so an idle bounded run (zero changes drained) would
        // otherwise leave no checkpoint, the next run would re-anchor to a newer
        // "current" position, and every change in between would be silently skipped.
        if !projecting {
            Self::refuse_absent_tables_on(&mut conn, &configured_tables)?;
        }
        let (file, pos) = Self::current_coordinates(&mut conn)?;
        let identity = Self::server_identity(&mut conn);
        if let Some(path) = ckpt {
            // LOUD, and at `warn` — this branch is reached both on a genuine first
            // run and when a previously-written checkpoint is simply NOT THERE, and
            // rivet cannot tell those apart. Round 9 measured the second case end to
            // end: a relative `checkpoint:` (what `rivet init` scaffolds) is resolved
            // against the process CWD, so running the same config from a different
            // working directory found no file, re-anchored HERE at the current
            // binlog coordinates, and reported `status: success, rows: 0` — three
            // green runs delivering ids [3] of a source holding [1,2,3], with no line
            // at any log level. PostgreSQL has said this for its own re-anchor
            // (`slot_created_warning`) since the equivalent bug there; MySQL's peer
            // was missing, and MySQL is the engine with NO server-side anchor to fall
            // back on.
            log::warn!(
                "mysql cdc: no checkpoint at `{}` — anchoring at the CURRENT binlog \
                 position ({file}:{pos}), so anything written before now is NOT \
                 captured. On a first run that is expected. If this checkpoint existed \
                 before, it was deleted or the config moved: a RELATIVE \
                 `cdc.checkpoint:` is resolved against the CONFIG FILE's directory, so \
                 the path above is where rivet looked — and the changes since it was \
                 written are gone from this stream. {}",
                path.display(),
                crate::source::cdc::checkpoint_identity::RECOVER
            );
            Self::write_anchor(path, &file, pos, &identity)?;
        }
        Self::open_on(
            conn,
            server_id,
            file,
            pos,
            mode,
            identity?,
            configured_tables,
        )
        .map(with_dir)
    }

    /// One row of a spilled tail: decode it through the general frame, stamp it,
    /// and drop the reader once the last row is out.
    fn next_spooled(&mut self) -> Result<Option<ChangeEvent>> {
        crate::source::cdc::tx_buffer::replay(
            &mut self.spooled,
            crate::source::cdc::spill::decode_event,
        )
    }

    /// Release the buffered transaction at `log_pos`, or end the stream when the
    /// commit sits past the open-time ceiling.
    ///
    /// Returns `false` when the caller must stop: a commit past the bound belongs to
    /// the NEXT run, so the transaction is dropped un-released and the checkpoint
    /// never advances past it — the resume re-reads it in full. `past_bound` is
    /// sticky so the sink's re-drain pass returns `None` at once rather than
    /// consuming (and re-deferring) more past-bound events.
    ///
    /// Shared by the `Xid` and `Query('COMMIT')` arms: two commit markers, one
    /// framing. Duplicating it is how the second one drifts.
    fn close_transaction_at(&mut self, log_pos: u64) -> bool {
        if commit_past_bound(&self.file, log_pos, self.bound.as_ref()) {
            self.tx.clear();
            self.past_bound = true;
            return false;
        }
        let commit = self.position_at(log_pos);
        // #158: commit position on all, committed on the transaction's LAST event —
        // on disk when it spilled (`TxBuffer::close_transaction`).
        match self
            .tx
            .close_transaction(&commit, &format_args!("{}:{log_pos}", self.file))
        {
            Ok((head, tail)) => {
                self.pending.extend(head);
                self.spooled = tail;
            }
            // Sealing the spill can fail, and a silent drop would deliver the head
            // without its tail — a half-transaction the sink would checkpoint past.
            Err(e) => self.spill_error = Some(e),
        }
        true
    }

    /// Pull one binlog event and expand it into `pending`. `Ok(false)` ⇒ stream
    /// ended; `Ok(true)` ⇒ consumed an event.
    fn fill(&mut self) -> Result<bool> {
        if self.past_bound {
            return Ok(false); // ended at the open-time ceiling — stay ended
        }
        let ev = match self.stream.next() {
            Some(ev) => ev.map_err(|e| binlog_read_error(e, &self.file))?,
            None => {
                self.refuse_changed_grants()?;
                return Ok(false);
            }
        };
        let log_pos = ev.header().log_pos() as u64;
        let data = ev.read_data()?;
        self.refuse_changed_grants_at(&data)?;
        match data {
            // ROTATE → the next binlog file; subsequent positions are in it.
            Some(EventData::RotateEvent(re)) => {
                self.file = re.name().to_string();
            }
            Some(EventData::TableMapEvent(tme)) => {
                let tme = tme.into_owned();
                let names: Option<std::sync::Arc<[String]>> =
                    tme.iter_optional_meta().find_map(|f| match f {
                        Ok(mysql::binlog::events::OptionalMetadataField::ColumnName(names)) => {
                            Some(
                                names
                                    .iter_names()
                                    .filter_map(|n| n.ok())
                                    .map(|n| String::from_utf8_lossy(n.name_raw()).into_owned())
                                    .collect(),
                            )
                        }
                        _ => None,
                    });
                self.tables
                    .insert(tme.table_id(), (std::sync::Arc::new(tme), names));
            }
            Some(EventData::RowsEvent(re)) => {
                let op = match &re {
                    RowsEventData::WriteRowsEvent(_) => ChangeOp::Insert,
                    RowsEventData::UpdateRowsEvent(_) => ChangeOp::Update,
                    RowsEventData::DeleteRowsEvent(_) => ChangeOp::Delete,
                    // Anything else is a rows event this reader cannot decode, and the
                    // old `return Ok(true)` DISCARDED it silently — the surrounding
                    // transaction still committed, so the checkpoint advanced past
                    // changes that never reached the destination. Unrecoverable, and
                    // reported as success.
                    //
                    // Two real sources, neither exotic: MariaDB emits only the v1
                    // rows events, so on MariaDB EVERY insert, update and delete was
                    // dropped while the pipeline stayed green at zero rows; and MySQL
                    // 8 with `binlog_row_value_options=PARTIAL_JSON` emits
                    // `PartialUpdateRowsEvent` for JSON column updates, where the
                    // surrounding transactions ARE captured — so the loss is a subset,
                    // which is the shape no count check can see.
                    //
                    // Refuse loudly instead, exactly as the compressed-payload arm
                    // below does: the un-acked span is re-read once the source is
                    // configured for a reader that can decode it.
                    other => {
                        // WHOSE event is it? The binlog carries every table on the
                        // server, so refusing without asking turns one PARTIAL_JSON
                        // table into a server-wide outage — measured: this refusal
                        // failed a concurrent export on a table it does not capture.
                        // The resolve is the same TABLE_MAP lookup the decode path
                        // does below; `None` (no TABLE_MAP yet) deliberately counts
                        // as OURS, because a silent skip is the one outcome this
                        // path exists to prevent.
                        let resolved = self.tables.get(&other.table_id());
                        let named =
                            resolved.map(|(tme, _)| (tme.database_name(), tme.table_name()));
                        let named = named.as_ref().map(|(d, t)| (d.as_ref(), t.as_ref()));
                        if undecodable_event_is_ours(
                            named,
                            &self.configured_tables,
                            self.fold_names,
                        ) {
                            return Err(undecodable_rows_event_refusal(other));
                        }
                        return Ok(true); // another table's — the routing filter would drop it anyway
                    }
                };
                let Some((tme, image_names)) = self.tables.get(&re.table_id()).cloned() else {
                    return Ok(true); // started mid-stream, no TABLE_MAP yet — skip
                };
                let schema = tme.database_name().to_string();
                let table = tme.table_name().to_string();
                let Some(image_names) = image_names else {
                    if undecodable_event_is_ours(
                        Some((&schema, &table)),
                        &self.configured_tables,
                        self.fold_names,
                    ) {
                        return Err(nameless_rows_refusal(&schema, &table, &self.file, log_pos));
                    }
                    return Ok(true); // another table's — the routing filter would drop it anyway
                };
                // WIRE-side ambiguity check: a bare configured name that has now
                // matched events from two databases. The catalog cannot see this —
                // information_schema is privilege-filtered while the binlog dump is
                // server-wide — so the first event from a second schema is the first
                // evidence that exists.
                for cfg in &self.configured_tables {
                    if cfg.contains('.')
                        || !crate::source::cdc::sink::table_matches(
                            crate::source::cdc::CdcEngine::Mysql,
                            cfg,
                            &schema,
                            &table,
                        )
                    {
                        continue;
                    }
                    match bare_event_route(self.bare_db.get(cfg).map(String::as_str), &schema) {
                        BareRoute::Captured => {}
                        // Another database's table of that name: the bare name means another's.
                        BareRoute::Elsewhere => return Ok(true),
                        BareRoute::Unresolved => {
                            if let Some(why) =
                                Self::bare_name_spans_databases(cfg, &self.own_db, &schema)
                            {
                                crate::rivet_bail!(
                                    crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
                                    "{why}"
                                );
                            }
                        }
                    }
                }
                let captured_keep = self.readable.as_ref().map(|readable| {
                    self.configured_tables
                        .iter()
                        .find(|cfg| {
                            crate::source::cdc::sink::table_matches(
                                crate::source::cdc::CdcEngine::Mysql,
                                cfg,
                                &schema,
                                &table,
                            )
                        })
                        .and_then(|cfg| readable.get(cfg))
                        .map(|r| super::column_grants::kept_positions(&image_names, &r.columns))
                });
                let captured = matches!(captured_keep, Some(Some(_)));
                let keep = captured_keep.map(Option::unwrap_or_default);
                let full_arity = image_names.len();
                let image_names: std::sync::Arc<[String]> = match &keep {
                    Some(k) => k.iter().map(|&i| image_names[i].clone()).collect(),
                    None => image_names,
                };
                let project = |r| match &keep {
                    Some(k) => super::column_grants::project_row(r, k),
                    None => r,
                };
                // Provisional position; rewritten to the commit position at XID.
                let position = self.position_at(log_pos);
                for row in re.rows(&tme) {
                    let (before, after) = row?;
                    let before = before.map(render_row);
                    let after = after.map(render_row);
                    if captured {
                        for img in [before.as_ref(), after.as_ref()] {
                            if super::column_grants::is_partial(full_arity, img) {
                                let n = img.map_or(0, |(v, _)| v.len());
                                return Err(crate::source::cdc::partial_image_refusal(
                                    &schema, &table, n, full_arity,
                                ));
                            }
                        }
                    }
                    let before = before.map(project);
                    let after = after.map(project);
                    if super::column_grants::changes_only_hidden(
                        captured,
                        before.as_ref(),
                        after.as_ref(),
                    ) {
                        self.tx.push(
                            boundary_marker(op, &position),
                            crate::source::cdc::spill::encode_event,
                            &format_args!("{}:{log_pos}", self.file),
                        )?;
                        continue;
                    }
                    // A cell with no faithful representation is a DEFERRED refusal,
                    // exactly like PostgreSQL's unrecoverable unchanged-TOAST datum:
                    // the binlog dump is server-wide, so bailing here would poison
                    // capture of unrelated tables that merely share the stream. The
                    // sink raises it only when the event routes to a captured table.
                    let undecodable: Vec<usize> = before
                        .iter()
                        .chain(after.iter())
                        .flat_map(|(_, bad)| bad.iter().copied())
                        .collect();
                    let poison = (!undecodable.is_empty()).then(|| {
                        let cols: Vec<String> = undecodable
                            .iter()
                            .map(|i| {
                                image_names
                                    .get(*i)
                                    .cloned()
                                    .unwrap_or_else(|| format!("column {i}"))
                            })
                            .collect();
                        format!(
                            "mysql cdc: {schema}.{table}: column(s) [{}] hold a JSON \
                             document with an OPAQUE leaf — a DECIMAL, DATE, TIME or \
                             DATETIME that MySQL materialised server-side and wrote to \
                             the binlog as a type tag plus raw bytes, not as JSON text. \
                             rivet cannot render those bytes the way the server does, \
                             and refuses rather than writing NULL (which discards the \
                             whole document while every count agrees) or the crate's \
                             `base64:type<N>:…` placeholder (a marker where a number \
                             belongs). A snapshot read renders it correctly — the server \
                             does the rendering there — so store it as a JSON string in \
                             the source so the binlog carries text, then: {}",
                            cols.join(", "),
                            crate::source::cdc::checkpoint_identity::RECOVER
                        )
                    });
                    let ev = ChangeEvent {
                        op,
                        schema: schema.clone(),
                        table: table.clone(),
                        before: before.map(|(v, _)| v),
                        after: after.map(|(v, _)| v),
                        position: position.clone(),
                        committed: false,
                        image_names: Some(image_names.clone()),
                        seq: 0, // stamped by TxnSeq as the stream is consumed
                        poison,
                        row_id: None,
                        before_names: None,
                        before_poison: None,
                    };
                    // PER ROW, not per binlog event. One `WriteRows` event carries
                    // MANY rows, so a check after the loop lets the whole event land
                    // in memory first — with a transaction written as a single large
                    // event that is the entire transaction, and the cap bounds
                    // nothing. (Only rows in LATER events would have spilled, which
                    // depends on `binlog_row_event_max_size` rather than on the cap
                    // the operator set.)
                    self.tx.push(
                        ev,
                        crate::source::cdc::spill::encode_event,
                        &format_args!("{}:{log_pos}", self.file),
                    )?;
                }
            }
            // XID = transaction commit. Stamp the commit position on every change
            // in the transaction and mark the last one committed, then release the
            // whole transaction atomically.
            Some(EventData::XidEvent(_)) => {
                if !self.close_transaction_at(log_pos) {
                    return Ok(false);
                }
            }
            // `XA PREPARE` arrives as a BINARY `XaPrepareLogEvent` on 5.7.7+, never
            // as a QueryEvent — so `is_commit_statement`'s "deliberately not XA
            // PREPARE" arm grades a string this reader cannot receive, and the
            // prepared rows fell through `_ => {}` and stayed in `self.tx`. The next
            // UNRELATED transaction's Xid then drained them: `close_transaction_at`
            // empties the whole buffer, one buffer per stream.
            //
            // MEASURED on mysql 8.0.46 — `XA START/INSERT(1)/XA END/XA PREPARE`, then
            // an ordinary `INSERT(2)` committing, then `XA ROLLBACK`. The source held
            // {2}; rivet delivered {1, 2}, both stamped `binlog.000011:55090448` (the
            // SECOND transaction's Xid) at `__seq` 0 and 1 — the rolled-back row
            // published as committed, and two transactions fused into one group.
            //
            // Table-addressed for the reason #281 measured: the dump is server-wide,
            // so an XA branch on a table nobody captures must not be an outage for
            // exports that never read it.
            Some(EventData::XaPrepareLogEvent(_)) => {
                if let Some(ev) = self.tx.head().iter().find(|ev| {
                    undecodable_event_is_ours(
                        Some((&ev.schema, &ev.table)),
                        &self.configured_tables,
                        self.fold_names,
                    )
                }) {
                    return Err(crate::source::cdc::checkpoint_identity::undecodable(
                        xa_prepare_refusal_message(&ev.schema, &ev.table),
                    ));
                }
                // A SPILLED tail makes the "nothing of ours" verdict below
                // unreachable honestly: the scan above covers only the in-memory
                // head, and past the cap every later row went to disk without
                // entering `tx` — so a branch whose captured rows sit only in the
                // tail would pass the scan and `self.tx.clear()` would DELETE
                // them, permanently and silently. Refuse instead: nothing has been
                // handed to the sink, so the bail loses nothing (at-least-once
                // re-reads from the checkpoint), and the message names both facts
                // the operator needs.
                if let Some(rows) = self.tx.spilled_rows() {
                    crate::rivet_bail!(
                        crate::error::codes::SOURCE_CDC_TRANSACTION_TOO_LARGE,
                        "{}",
                        xa_prepare_spilled_tail_refusal_message(rows)
                    );
                }
                // Nothing of ours in the branch: drop it rather than leave it for the
                // next commit to stamp and fuse. The sink would route these rows away,
                // but only after they had already corrupted that transaction's framing.
                self.tx.clear();
            }
            Some(EventData::QueryEvent(qe)) => {
                let (sql, mode) = event_statement(&qe);
                let verdict = query_verdict(
                    &sql,
                    &qe.schema(),
                    mode,
                    &QueryScope {
                        projecting: self.readable.is_some(),
                        fold: self.fold_names,
                        configured: &self.configured_tables,
                    },
                );
                if let Some(refusal) = refusal_of(&verdict, charset_of(qe.status_vars_raw())) {
                    return Err(refusal);
                }
                match verdict {
                    QueryVerdict::Privilege => {
                        self.grants_pending
                            .get_or_insert_with(|| (self.file.clone(), log_pos));
                    }
                    QueryVerdict::Commit => return Ok(self.close_transaction_at(log_pos)),
                    QueryVerdict::Truncated(..)
                    | QueryVerdict::ColumnRenamed(..)
                    | QueryVerdict::TableRenamed(..)
                    | QueryVerdict::Dropped(..)
                    | QueryVerdict::StatementWrite(..)
                    | QueryVerdict::Unreadable(..)
                    | QueryVerdict::Pass => {}
                }
            }
            // #200-2: a compressed transaction. `binlog_transaction_compression`
            // can be turned on AFTER this run opened (the open-time
            // `refuse_compressed_binlog` guard saw it OFF and passed), or a
            // compressed span can already sit in the binlog before the checkpoint
            // (written while it was ON, then turned OFF). Either way a
            // `Transaction_payload_event` reaches here, and this reader cannot
            // expand it. The `_ => {}` arm below would SILENTLY skip it —
            // capturing NOTHING for that transaction while the checkpoint advances
            // past it (an at-least-once break, and the open-time refusal's
            // remediation replays over the already-skipped span). Refuse loudly
            // instead; the un-acked span is re-read from the checkpoint once
            // compression is off.
            Some(EventData::TransactionPayloadEvent(_)) => {
                return Err(compressed_payload_refusal());
            }
            _ => {}
        }
        Ok(true)
    }
}

/// The stream-time sibling of [`compression_refusal`]: a `Transaction_payload_event`
/// reached the reader even though the OPEN-time check passed. Pure so both the
/// message and the fact that it IS an error are testable offline — deleting the
/// match arm that calls it (falling through to `_ => {}`) is exactly the silent-skip
/// bug, which only a live compressed server would otherwise flip.
/// Which undecodable shape a rows event is. Separated from the message so the
/// message — the ONLY thing an operator sees, since the alternative was a green run
/// with missing rows — is testable without constructing binlog events the crate does
/// not let a test build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UndecodableRows {
    /// Pre-5.6 row format. MariaDB emits only these.
    V1,
    /// `binlog_row_value_options=PARTIAL_JSON` logs a JSON diff, not a row image.
    PartialJson,
    Other,
}

/// What differs between the columns of `table` a run delivers and the ones its account may `SELECT` now: the columns gained and lost, and the two ways it happens.
pub(crate) fn grants_difference(table: &str, before: &[String], now: &[String]) -> String {
    let missing = |from: &[String], of: &[String]| -> Vec<String> {
        from.iter()
            .filter(|c| !of.iter().any(|o| o.eq_ignore_ascii_case(c)))
            .map(|c| format!("`{c}`"))
            .collect()
    };
    let (gained, lost) = (missing(now, before), missing(before, now));
    let mut changes = Vec::new();
    if !gained.is_empty() {
        changes.push(format!("gained {}", gained.join(", ")));
    }
    if !lost.is_empty() {
        changes.push(format!("lost {}", lost.join(", ")));
    }
    format!(
        "the columns this account may SELECT of `{table}` changed ({}): a grant change, or a \
         schema change under a table-level grant",
        changes.join("; ")
    )
}

/// An event that routes to no table and carries no row: it holds the commit boundary of a transaction whose rows the policy did not deliver.
fn boundary_marker(op: ChangeOp, position: &Position) -> ChangeEvent {
    ChangeEvent {
        op,
        schema: String::new(),
        table: String::new(),
        before: None,
        after: None,
        position: position.clone(),
        committed: false,
        image_names: None,
        seq: 0,
        poison: None,
        row_id: None,
        before_names: None,
        before_poison: None,
    }
}

/// Does an UNDECODABLE rows event belong to a table this run captures?
///
/// The binlog is ONE stream per server, so it carries every table's changes,
/// including tables nobody configured. Refusing on an undecodable event without
/// asking whose it is turns one PARTIAL_JSON table into a server-wide outage:
/// every other export refuses too, on data it was never going to read. Measured
/// 2026-08-24 — the live PARTIAL_JSON fixture failed a concurrent, unrelated
/// MySQL CDC test with this exact error, on a table it does not capture.
///
/// The asymmetry is deliberate and it is the whole safety argument:
/// - a **captured** table: REFUSE. Skipping loses the change while the surrounding
///   transaction commits and the checkpoint advances past it.
/// - **another** table: skip. The run would have dropped it at the routing filter
///   anyway, so skipping loses nothing.
/// - table **unknown**: REFUSE. No TABLE_MAP means the stream started
///   mid-transaction; guessing "not mine" there would be a silent drop of exactly
///   the kind this path exists to prevent. Over-refusing is recoverable; a skip is
///   not.
/// - **nothing configured**: REFUSE. `rivet cdc` with no `--table` captures
///   whatever the stream emits, so every event is "mine".
///
/// Pure so every branch is graded offline; the caller resolves the name.
pub(crate) fn undecodable_event_is_ours(
    resolved: Option<(&str, &str)>,
    configured: &[String],
    fold: bool,
) -> bool {
    if configured.is_empty() {
        return true;
    }
    match resolved {
        None => true,
        Some((schema, table)) if fold => {
            let (schema, table) = (schema.to_lowercase(), table.to_lowercase());
            configured.iter().any(|c| {
                crate::source::cdc::sink::table_matches(
                    crate::source::cdc::CdcEngine::Mysql,
                    &c.to_lowercase(),
                    &schema,
                    &table,
                )
            })
        }
        Some((schema, table)) => configured.iter().any(|c| {
            crate::source::cdc::sink::table_matches(
                crate::source::cdc::CdcEngine::Mysql,
                c,
                schema,
                table,
            )
        }),
    }
}

/// Whether a `lower_case_table_names` value folds names: any value but `0`; an absent value folds, because over-refusing is recoverable and a missed rename is not.
fn fold_names_from(value: Option<&str>) -> bool {
    value.is_none_or(|v| v.trim() != "0")
}

fn classify_rows_event(ev: &RowsEventData<'_>) -> UndecodableRows {
    match ev {
        RowsEventData::WriteRowsEventV1(_)
        | RowsEventData::UpdateRowsEventV1(_)
        | RowsEventData::DeleteRowsEventV1(_) => UndecodableRows::V1,
        RowsEventData::PartialUpdateRowsEvent(_) => UndecodableRows::PartialJson,
        _ => UndecodableRows::Other,
    }
}

/// Refuse a rows event this reader cannot decode, naming WHICH one and what to do.
///
/// Silence here is worse than on most refusal paths: the transaction around the
/// dropped rows still commits, so the checkpoint advances and the changes are gone
/// from both the binlog window and the destination while the run reports success.
pub(crate) fn undecodable_rows_refusal_message(kind: UndecodableRows) -> String {
    let (what, cause) = match kind {
        UndecodableRows::V1 => (
            "a v1 rows event",
            "the source writes the pre-5.6 row format — MariaDB emits only v1 rows events. \
             Point rivet at a MySQL replica, or use a MariaDB-native CDC reader",
        ),
        UndecodableRows::PartialJson => (
            "a partial-JSON update",
            "the WRITING session had binlog_row_value_options=PARTIAL_JSON, which logs a \
             JSON diff rather than the row image. That is usually the server global, but \
             it is a SESSION variable too — so check `SELECT @@GLOBAL.binlog_row_value_options` \
             first and, if it is already empty, the writer set it per-session. Clear it on \
             the replica rivet reads (and on whatever writes to it), then re-run",
        ),
        UndecodableRows::Other => (
            "an unrecognised rows event",
            "this reader decodes only the v2 write/update/delete rows events",
        ),
    };
    format!(
        "mysql cdc: encountered {what} in the binlog and this reader cannot decode it. \
         Skipping it would capture NOTHING for those rows while the surrounding \
         transaction commits and the checkpoint advances past them — a silent, \
         unrecoverable loss reported as success. {cause}: the un-acked span is re-read \
         from the checkpoint."
    )
}

fn undecodable_rows_event_refusal(ev: &RowsEventData<'_>) -> anyhow::Error {
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::SOURCE_CDC_PREREQUISITE,
        undecodable_rows_refusal_message(classify_rows_event(ev)),
    ))
}

/// Refuse a captured table's rows event whose TABLE_MAP carries no column names (written under MINIMAL).
fn nameless_rows_refusal(schema: &str, table: &str, file: &str, log_pos: u64) -> anyhow::Error {
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::SOURCE_CDC_UNDECODABLE,
        format!(
            "mysql cdc: `{schema}`.`{table}`: the rows event ending at binlog position \
             {file}:{log_pos} has a TABLE_MAP with no column names: it was written while the \
             server had binlog_row_metadata = MINIMAL, and the events keep the setting in force \
             when they were WRITTEN. rivet never maps values by position, which puts them under \
             the wrong names after a column reorder or ALTER. Make sure the server now has \
             `binlog_row_metadata = FULL` (`SET PERSIST binlog_row_metadata = FULL;`), then: {}",
            crate::source::cdc::checkpoint_identity::RECOVER
        ),
    ))
}

fn compressed_payload_refusal() -> anyhow::Error {
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::SOURCE_CDC_PREREQUISITE,
        "mysql cdc: encountered a Transaction_payload_event in the binlog — the source has \
         binlog_transaction_compression enabled (turned on after this run opened, or a compressed \
         span already in the binlog written while it was on), and this reader cannot expand it. \
         Skipping it would capture NOTHING for that transaction while the checkpoint advanced past \
         it — a silent data loss. Turn compression off for the replica rivet reads \
         (SET GLOBAL binlog_transaction_compression = OFF; and remove it from my.cnf), then \
         re-run: the un-acked span is re-read from the checkpoint.",
    ))
}

/// Refuse to stream when the source packs transactions into
/// `Transaction_payload_event` (MySQL 8.0.20+ `binlog_transaction_compression`).
///
/// This reader matches `Rotate`/`TableMap`/`Rows`/`Xid` and ignores anything
/// else, so with compression ON every row event — they are all inside the
/// payload — is skipped WITHOUT A WORD: the run reports success having
/// captured nothing, and a bounded pipeline then reports "no changes" forever
/// while the destination never receives a row. Measured on MySQL 8.0.46:
/// anchor, insert two rows, resume ⇒ 0 of 2 events captured, empty stderr.
///
/// Compression is a natural thing for a DBA to enable (it shrinks the binlog
/// 50-75% and the replication traffic with it), which is exactly why this
/// must fail loudly instead of degrading. Expanding the payload is a
/// supportable feature — `mysql_common` already ships
/// `TransactionPayloadEvent::decompressed()` and
/// `EventStreamReader::read_decompressed()`, and the inner events carry the
/// OUTER payload's `end_log_pos`, so the commit-position semantics carry over
/// unchanged — but until that lands, refusing is the only honest option.
/// Ask the server whether it replicates and whether it re-logs what it applies.
fn refuse_replica_without_relog(conn: &mut Conn) -> Result<()> {
    let replicating = conn
        .query_first::<mysql::Row, _>("SHOW REPLICA STATUS")
        .or_else(|_| conn.query_first::<mysql::Row, _>("SHOW SLAVE STATUS"))
        .ok()
        .flatten()
        .is_some();
    let relog: Option<String> = conn
        .query_first("SELECT @@global.log_replica_updates")
        .or_else(|_| conn.query_first("SELECT @@global.log_slave_updates"))
        .ok()
        .flatten();
    replica_relog_refusal(replicating, relog.as_deref())
}

/// The server's error number of a driver error, when the server sent one.
fn server_error_code(e: &mysql::Error) -> Option<u16> {
    match e {
        mysql::Error::MySqlError(m) => Some(m.code),
        _ => None,
    }
}

/// Why a configured table the schema probe cannot prepare is refused: absent (ER_NO_SUCH_TABLE 1146, ER_BAD_DB_ERROR 1049) by code, anything else in the server's words.
pub(crate) fn table_probe_refusal(table: &str, code: Option<u16>, said: &str) -> anyhow::Error {
    if matches!(code, Some(1146 | 1049)) {
        return crate::error::CodedError::new(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            format!(
                "mysql cdc: table `{table}` does not exist on this server (a bare name is read in \
                 the connection's database), so no change to it could ever be captured; nothing \
                 was read or written. Create the table, or fix `table:` in the export, then re-run."
            ),
        )
        .into();
    }
    crate::source::cdc::table_unreadable("mysql", table, said)
}

/// A replica whose binlog omits replicated changes would capture nothing and report success.
fn replica_relog_refusal(replicating: bool, relog: Option<&str>) -> Result<()> {
    let relogs = matches!(relog, Some("1") | Some("ON") | Some("on") | None);
    if !replicating || relogs {
        return Ok(());
    }
    crate::rivet_bail!(
        crate::error::codes::SOURCE_CDC_PREREQUISITE,
        "mysql cdc: this server is a replica with log_replica_updates = OFF, so the changes it \
         applies from its source never reach its own binlog — reading it would capture NOTHING of \
         them and report success. Set log_replica_updates = ON (log_slave_updates before 8.0.26; \
         it needs a restart), or read the source instead."
    )
}

fn refuse_compressed_binlog(conn: &mut Conn) -> Result<()> {
    // Pre-8.0.20 servers (and MariaDB) have no such variable: the query errors
    // or returns nothing, and both mean "not compressed". Never let the ABSENCE
    // of the setting block a source that cannot have the problem.
    let raw: Option<String> = conn
        .query_first("SELECT @@global.binlog_transaction_compression")
        .ok()
        .flatten();
    compression_refusal(raw.as_deref())
}

/// The verdict on the server's reply, split from the connection so both
/// DIRECTIONS are testable offline. Folded into `refuse_compressed_binlog` this
/// was a `!` that only a live server could flip: deleting it (refuse when the
/// binlog is PLAIN, stream when it is compressed — the exact inversion of the
/// guard) survived the whole offline suite while breaking every MySQL CDC run.
fn compression_refusal(raw: Option<&str>) -> Result<()> {
    if !binlog_compression_is_on(raw) {
        return Ok(());
    }
    crate::rivet_bail!(
        crate::error::codes::SOURCE_CDC_PREREQUISITE,
        "mysql cdc: the source has binlog_transaction_compression = ON, and this reader cannot \
         expand a Transaction_payload_event — it would capture NOTHING and report success. \
         Turn it off for the replica rivet reads (SET GLOBAL binlog_transaction_compression = OFF; \
         and remove it from my.cnf), then re-run. To shrink the binlog instead, cut RETENTION \
         (binlog_expire_logs_seconds, or `CALL mysql.rds_set_configuration('binlog retention \
         hours', N)` on RDS) — rivet's bounded cycle keeps lag at minutes, so retention only has \
         to cover the longest outage you want to survive."
    )
}

/// Whether the server's reply to `@@global.binlog_transaction_compression`
/// means ON. MySQL renders a boolean system variable as `1`/`0` over the text
/// protocol, but `ON`/`OFF` is what a human sees and what some proxies pass
/// through — accept both, and treat anything unrecognized as OFF so a reply
/// shape we did not anticipate cannot block a healthy source.
fn binlog_compression_is_on(raw: Option<&str>) -> bool {
    matches!(
        raw.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1") | Some("on") | Some("true")
    )
}

/// The server keeps no binlog, so there is nothing to capture; `detail` is which status query said so.
fn binlog_disabled(detail: &str) -> anyhow::Error {
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::SOURCE_CDC_PREREQUISITE,
        format!("mysql: binlog disabled ({detail})"),
    ))
}

/// Dial through the batch path's [`super::dial_conn`], then refuse a proxy that cannot carry the binlog.
fn connect_conn(url: &str, tls: Option<&TlsConfig>) -> Result<Conn> {
    let mut conn = super::dial_conn(url, tls)?;
    // CDC streams the binlog (COM_BINLOG_DUMP) — a replication protocol that
    // ProxySQL / MaxScale / transaction-mode multiplexers do not carry. Detect the
    // proxy here and fail with the fix, not a cryptic dump-protocol error or a hang.
    let kind = super::proxy::detect_proxy_on_conn(&mut conn);
    if kind.is_proxy() {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            "CDC cannot stream the binlog through {} — it does not proxy the MySQL \
             replication protocol (COM_BINLOG_DUMP). Point the source at the MySQL host \
             directly (the replication endpoint), not the proxy.",
            kind.log_label()
        );
    }
    Ok(conn)
}

/// What a configured MySQL `table:` actually IS, and whether the binlog can ever
/// name it — the MySQL half of the PostgreSQL routing guard (#283).
///
/// A binlog `Table_map` names the relation the rows PHYSICALLY landed in. For a
/// RANGE-partitioned table that is the logical table (measured — partitioning is
/// fine here, unlike PostgreSQL). For an updatable VIEW it is the BASE table, and
/// the sink routes byte-exact, so every change is dropped.
///
/// MEASURED 2026-08-24 by an adversarial re-check of a matrix cell that claimed
/// MySQL had no such relation: 3 rows committed through a view gave
/// `rows: 0, files: 0, status: success, exit 0` with the checkpoint advanced past
/// the commit (54945184 -> 54945514), and fixing the config recovered NOTHING
/// from that checkpoint.
///
/// Severity differs from PostgreSQL and the message says so: MySQL binlog
/// retention is reader-independent, so the events are still in the log and
/// deleting the checkpoint re-reads them. On PostgreSQL the slot had already
/// freed the WAL, which is why that one is unrecoverable.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MysqlRoutingVerdict {
    Routable,
    Never(String),
}

/// Pure half of the guard. `table_type` is `information_schema.TABLES.TABLE_TYPE`
/// for the configured name, or `None` when the catalog has no row for it — which
/// the schema probe reports on its own, loudly, so this stays quiet about it
/// rather than producing a second, competing error for one cause.
pub(crate) fn classify_mysql_routing(
    configured: &str,
    table_type: Option<&str>,
) -> MysqlRoutingVerdict {
    match table_type {
        None => MysqlRoutingVerdict::Routable,
        Some(t) if t.eq_ignore_ascii_case("BASE TABLE") => MysqlRoutingVerdict::Routable,
        Some(t) => MysqlRoutingVerdict::Never(format!(
            "mysql cdc: `{configured}` is a {t}, not a base table. The binlog names the relation \
             rows physically landed in — for a view that is the BASE table — and routing is \
             byte-exact, so every change would be dropped while the checkpoint advanced past it: \
             a run reporting success with 0 rows. Capture the base table(s) the view reads \
             instead. If a run already did this, the changes are NOT lost — MySQL binlog \
             retention does not depend on the reader, so deleting the checkpoint re-reads them."
        )),
    }
}

/// The statement of a QUERY event as its client character set spells it, and the `sql_mode` it was logged under.
fn event_statement<'a>(qe: &'a QueryEvent<'_>) -> (std::borrow::Cow<'a, str>, SqlMode) {
    statement_of(qe.query_raw(), qe.status_vars_raw())
}

/// The statement bytes read in the character set the status variables name, and the `sql_mode` among them.
fn statement_of<'a>(raw: &'a [u8], status_vars: &[u8]) -> (std::borrow::Cow<'a, str>, SqlMode) {
    (
        client_text(raw, charset_of(status_vars)),
        mode_of(status_vars),
    )
}

/// The character set id the client of a QUERY event sent its statement in, among its raw status variables.
fn charset_of(status_vars: &[u8]) -> Option<u16> {
    StatusVarsIterator::new(status_vars).find_map(|var| match var.get_value() {
        Ok(StatusVarVal::Charset { charset_client, .. }) => Some(charset_client),
        _ => None,
    })
}

/// The `sql_mode` among the raw status variables of a QUERY event; the default lexing when there is none.
fn mode_of(status_vars: &[u8]) -> SqlMode {
    StatusVarsIterator::new(status_vars)
        .find_map(|var| match var.get_value() {
            Ok(StatusVarVal::SqlMode(flags)) => Some(SqlMode::from_bits(flags.0)),
            _ => None,
        })
        .unwrap_or_default()
}

/// The pieces of `sql` that are not blanks, without the semicolons that end it.
fn significant(sql: &str, mode: SqlMode) -> Vec<Tok<'_>> {
    let mut toks: Vec<_> = tokens(sql, mode)
        .into_iter()
        .filter(|t| !matches!(t, Tok::Blank(_)))
        .collect();
    while toks.last() == Some(&Tok::Punct(';')) {
        toks.pop();
    }
    toks
}

/// Whether the piece at `at` is the unquoted word `w`, in any case.
fn word_is(toks: &[Tok<'_>], at: usize, w: &str) -> bool {
    matches!(toks.get(at), Some(Tok::Word(t)) if t.eq_ignore_ascii_case(w))
}

/// The table name read at `at` as `(schema when given, table, index past it)`; `None` when no name stands there.
fn table_name_at(toks: &[Tok<'_>], at: usize) -> Option<(Option<String>, String, usize)> {
    let part = |i: usize| match toks.get(i)? {
        Tok::Word(w) => Some((*w).to_string()),
        quoted => quoted.identifier(),
    };
    let first = part(at)?;
    match toks.get(at + 1) {
        Some(Tok::Punct('.')) => Some((Some(first), part(at + 2)?, at + 3)),
        _ => Some((None, first, at + 1)),
    }
}

/// The `(schema, table)` a name read by [`table_name_at`] stands for, given the event's database.
fn qualified(schema: Option<String>, table: String, event_db: &str) -> (String, String) {
    (schema.unwrap_or_else(|| event_db.to_string()), table)
}

/// The table a `TRUNCATE` QUERY event names, as `(schema, table)`; `None` for any other statement or a shape this reader does not claim.
pub(crate) fn truncate_target(
    sql: &str,
    event_db: &str,
    mode: SqlMode,
) -> Option<(String, String)> {
    let toks = significant(sql, mode);
    if !word_is(&toks, 0, "truncate") {
        return None;
    }
    let at = 1 + usize::from(word_is(&toks, 1, "table"));
    let (schema, table, end) = table_name_at(&toks, at)?;
    (end == toks.len()).then(|| qualified(schema, table, event_db))
}

/// One token of an `ALTER TABLE` statement: a word (`true` when it was backticked), a quoted string, or one punctuation character.
#[derive(Debug, PartialEq, Eq)]
enum AlterToken {
    Word(String, bool),
    Text,
    Punct(char),
}

/// `sql` cut into [`AlterToken`]s; `None` for an unterminated quote.
fn alter_tokens(sql: &str, mode: SqlMode) -> Option<Vec<AlterToken>> {
    let mut out = Vec::new();
    for tok in tokens(sql, mode) {
        match tok {
            Tok::Blank(_) => {}
            Tok::Word(w) => out.push(AlterToken::Word(w.to_string(), false)),
            Tok::Punct(c) => out.push(AlterToken::Punct(c)),
            Tok::Quoted { closed: false, .. } => return None,
            quoted => out.push(match quoted.identifier() {
                Some(name) => AlterToken::Word(name, true),
                None => AlterToken::Text,
            }),
        }
    }
    Some(out)
}

/// Whether the token at `i` is the unquoted word `w`, in any case.
fn alter_word(tokens: &[AlterToken], i: usize, w: &str) -> bool {
    matches!(tokens.get(i), Some(AlterToken::Word(t, false)) if t.eq_ignore_ascii_case(w))
}

/// The word at `i`, quoted or not.
fn alter_name(tokens: &[AlterToken], i: usize) -> Option<String> {
    match tokens.get(i) {
        Some(AlterToken::Word(t, _)) => Some(t.clone()),
        _ => None,
    }
}

/// The table name read at `at` as `((schema, table), index past it)`; the event's database stands in for a missing qualifier.
fn alter_table_name(
    tokens: &[AlterToken],
    at: usize,
    event_db: &str,
) -> Option<((String, String), usize)> {
    let first = alter_name(tokens, at)?;
    if tokens.get(at + 1) == Some(&AlterToken::Punct('.')) {
        Some(((first, alter_name(tokens, at + 2)?), at + 3))
    } else {
        Some(((event_db.to_string(), first), at + 1))
    }
}

/// The table `ALTER [IGNORE] TABLE name` alters and the index of its first specification.
fn alter_header(tokens: &[AlterToken], event_db: &str) -> Option<((String, String), usize)> {
    let mut at = 0;
    if !alter_word(tokens, at, "alter") {
        return None;
    }
    at += 1;
    if alter_word(tokens, at, "ignore") {
        at += 1;
    }
    if !alter_word(tokens, at, "table") {
        return None;
    }
    alter_table_name(tokens, at + 1, event_db)
}

/// The index at which each comma-separated specification of an `ALTER TABLE` starts, from `at` on.
fn alter_spec_starts(tokens: &[AlterToken], at: usize) -> Vec<usize> {
    let (mut depth, mut starts) = (0usize, vec![at]);
    for (i, tok) in tokens.iter().enumerate().skip(at) {
        match tok {
            AlterToken::Punct('(') => depth += 1,
            AlterToken::Punct(')') => depth = depth.saturating_sub(1),
            AlterToken::Punct(',') if depth == 0 => starts.push(i + 1),
            _ => {}
        }
    }
    starts
}

/// The `(schema, table)` an `ALTER TABLE` QUERY event gives a column a new name; `None` for any other statement.
pub(crate) fn column_rename_target(
    sql: &str,
    event_db: &str,
    mode: SqlMode,
) -> Option<(String, String)> {
    let tokens = alter_tokens(sql, mode)?;
    let (table, at) = alter_header(&tokens, event_db)?;
    alter_spec_starts(&tokens, at)
        .into_iter()
        .any(|start| {
            if alter_word(&tokens, start, "rename") && alter_word(&tokens, start + 1, "column") {
                true
            } else if alter_word(&tokens, start, "change") {
                let from = start + 1 + usize::from(alter_word(&tokens, start + 1, "column"));
                match (alter_name(&tokens, from), alter_name(&tokens, from + 1)) {
                    (Some(old), Some(new)) => !old.eq_ignore_ascii_case(&new),
                    _ => false,
                }
            } else {
                false
            }
        })
        .then_some(table)
}

/// Every table a `RENAME TABLE[S]` or an `ALTER TABLE ... RENAME [TO|AS] new` QUERY event touches, source and target (`None` for a name that cannot be read); `None` when the event renames no table.
pub(crate) fn renamed_tables(
    sql: &str,
    event_db: &str,
    mode: SqlMode,
) -> Option<Vec<Option<(String, String)>>> {
    let tokens = alter_tokens(sql, mode)?;
    if alter_word(&tokens, 0, "rename")
        && (alter_word(&tokens, 1, "table") || alter_word(&tokens, 1, "tables"))
    {
        let (mut out, mut at) = (Vec::new(), 2);
        loop {
            let Some((from, next)) = alter_table_name(&tokens, at, event_db) else {
                out.push(None);
                break;
            };
            out.push(Some(from));
            if !alter_word(&tokens, next, "to") {
                out.push(None);
                break;
            }
            let Some((to, next)) = alter_table_name(&tokens, next + 1, event_db) else {
                out.push(None);
                break;
            };
            out.push(Some(to));
            if tokens.get(next) != Some(&AlterToken::Punct(',')) {
                break;
            }
            at = next + 1;
        }
        return Some(out);
    }
    let (table, at) = alter_header(&tokens, event_db)?;
    let mut out = vec![Some(table)];
    for start in alter_spec_starts(&tokens, at) {
        if !alter_word(&tokens, start, "rename") {
            continue;
        }
        let mut from = start + 1;
        if alter_word(&tokens, from, "to")
            || alter_word(&tokens, from, "as")
            || tokens.get(from) == Some(&AlterToken::Punct('='))
        {
            from += 1;
        } else if ["column", "index", "key"]
            .iter()
            .any(|k| alter_word(&tokens, from, k))
        {
            continue;
        }
        out.push(alter_table_name(&tokens, from, event_db).map(|(to, _)| to));
    }
    (out.len() > 1).then_some(out)
}

/// A name with each run of non-ASCII characters collapsed to one marker, lowercased when the server folds case.
fn name_shape(name: &str, fold: bool) -> String {
    let mut out = String::with_capacity(name.len());
    let mut in_run = false;
    for c in name.chars() {
        if c.is_ascii() {
            in_run = false;
            out.push(if fold { c.to_ascii_lowercase() } else { c });
        } else {
            if !in_run {
                out.push(UNPLACED);
            }
            in_run = true;
        }
    }
    out
}

/// Whether a name holding bytes the reader could not place can be a captured table: it can when some configured table has the same ASCII skeleton.
fn unplaced_name_is_ours(schema: &str, table: &str, scope: &QueryScope<'_>) -> bool {
    let shaped: Vec<String> = scope
        .configured
        .iter()
        .map(|c| name_shape(c, scope.fold))
        .collect();
    let name = (
        name_shape(schema, scope.fold),
        name_shape(table, scope.fold),
    );
    undecodable_event_is_ours(Some((&name.0, &name.1)), &shaped, false)
}

/// What a QUERY event means to the reader; the table of a refusal is `None` when its name could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueryVerdict {
    Truncated(String, String),
    ColumnRenamed(String, String),
    TableRenamed(Option<(String, String)>),
    Dropped(Option<(String, String)>),
    StatementWrite(Option<(String, String)>),
    Unreadable(Box<QueryVerdict>),
    Privilege,
    Commit,
    Pass,
}

/// What a run knows when it judges a QUERY event: whether it projects columns, whether the server folds name case, and the tables it captures.
pub(crate) struct QueryScope<'a> {
    pub(crate) projecting: bool,
    pub(crate) fold: bool,
    pub(crate) configured: &'a [String],
}

/// The verdict of the statement as read, then as read with every U+FFFD (a byte the reader cannot place) a blank: a refusal only the second reading reaches stops the run as unreadable.
pub(crate) fn query_verdict(
    sql: &str,
    event_db: &str,
    mode: SqlMode,
    scope: &QueryScope<'_>,
) -> QueryVerdict {
    let read = read_verdict(sql, event_db, mode, scope);
    if !sql.contains(UNPLACED) {
        return read;
    }
    if !matches!(
        read,
        QueryVerdict::Pass | QueryVerdict::Commit | QueryVerdict::Privilege
    ) {
        return read;
    }
    match read_verdict(&sql.replace(UNPLACED, " "), event_db, mode, scope) {
        QueryVerdict::Pass | QueryVerdict::Commit => read,
        QueryVerdict::Privilege => QueryVerdict::Privilege,
        refused => QueryVerdict::Unreadable(Box::new(refused)),
    }
}

/// The verdict of one reading of a QUERY event: the first of truncate, column rename, table rename, drop, statement write, privilege change, commit that applies.
fn read_verdict(sql: &str, event_db: &str, mode: SqlMode, scope: &QueryScope<'_>) -> QueryVerdict {
    let ours = |t: Option<&(String, String)>| match t {
        Some((sc, tb)) if sc.contains(UNPLACED) || tb.contains(UNPLACED) => {
            unplaced_name_is_ours(sc, tb, scope)
        }
        _ => undecodable_event_is_ours(
            t.map(|(sc, tb)| (sc.as_str(), tb.as_str())),
            scope.configured,
            scope.fold,
        ),
    };
    if let Some((sc, tb)) = truncate_target(sql, event_db, mode).filter(|t| ours(Some(t))) {
        return QueryVerdict::Truncated(sc, tb);
    }
    if scope.projecting {
        if let Some((sc, tb)) = column_rename_target(sql, event_db, mode).filter(|t| ours(Some(t)))
        {
            return QueryVerdict::ColumnRenamed(sc, tb);
        }
        if let Some(touched) = renamed_tables(sql, event_db, mode)
            .and_then(|ts| ts.into_iter().find(|t| ours(t.as_ref())))
        {
            return QueryVerdict::TableRenamed(touched);
        }
    }
    if let Some(dropped) = drop_table_targets(sql, event_db, mode)
        .and_then(|ts| ts.into_iter().find(|t| ours(t.as_ref())))
    {
        return QueryVerdict::Dropped(dropped);
    }
    if let Some(written) = statement_dml_target(sql, event_db, mode).filter(|t| ours(t.as_ref())) {
        return QueryVerdict::StatementWrite(written);
    }
    if is_privilege_statement(sql, mode) {
        QueryVerdict::Privilege
    } else if is_commit_statement(sql) {
        QueryVerdict::Commit
    } else {
        QueryVerdict::Pass
    }
}

/// The refusal a verdict that stops the run raises, `None` for one that lets the event pass; an unreadable one says so before the refusal it hides.
fn refusal_of(verdict: &QueryVerdict, charset: Option<u16>) -> Option<anyhow::Error> {
    let (truncating, message) = refusal_parts(verdict)?;
    let message = match verdict {
        QueryVerdict::Unreadable(_) => unreadable_refusal_message(charset, &message),
        _ => message,
    };
    Some(if truncating {
        crate::source::cdc::checkpoint_identity::truncated(message)
    } else {
        crate::source::cdc::checkpoint_identity::undecodable(message)
    })
}

/// Whether the refusal of a verdict is a `SOURCE_CDC_TRUNCATED` one, and its message; `None` for a verdict that stops nothing.
fn refusal_parts(verdict: &QueryVerdict) -> Option<(bool, String)> {
    match verdict {
        QueryVerdict::Truncated(sc, tb) => Some((true, truncate_refusal_message(sc, tb))),
        QueryVerdict::ColumnRenamed(sc, tb) => Some((false, column_rename_refusal_message(sc, tb))),
        QueryVerdict::TableRenamed(target) => {
            Some((false, table_rename_refusal_message(target.as_ref())))
        }
        QueryVerdict::Dropped(target) => Some((true, drop_refusal_message(target.as_ref()))),
        QueryVerdict::StatementWrite(target) => {
            Some((false, statement_dml_refusal_message(target.as_ref())))
        }
        QueryVerdict::Unreadable(inner) => refusal_parts(inner),
        QueryVerdict::Privilege | QueryVerdict::Commit | QueryVerdict::Pass => None,
    }
}

/// The refusal of a statement whose bytes the reader cannot place: it names the client character set, then the refusal the statement is if those bytes are blanks.
pub(crate) fn unreadable_refusal_message(charset: Option<u16>, inner: &str) -> String {
    let set = match charset {
        Some(id) => match charset_name(id) {
            Some(name) => format!("`{name}` (collation id {id})"),
            None => format!("collation id {id}, which this reader does not know"),
        },
        None => "not recorded in the event".to_string(),
    };
    format!(
        "mysql cdc: a statement in the log being read holds bytes this reader cannot tell from \
         blanks, sent in the client character set {set}; the server may read it as the following, \
         so the run stops instead of guessing. {}",
        inner.strip_prefix("mysql cdc: ").unwrap_or(inner)
    )
}

/// Refuse a captured table renamed under `columns_from: grants`: privileges stay with the name the table had.
pub(crate) fn table_rename_refusal_message(target: Option<&(String, String)>) -> String {
    let what = match target {
        Some((schema, table)) if !schema.is_empty() => format!("`{schema}.{table}`"),
        Some((_, table)) => format!("`{table}`"),
        None => "a table this reader could not identify".to_string(),
    };
    format!(
        "mysql cdc: {what} was renamed (RENAME TABLE, or ALTER TABLE ... RENAME) in the log \
         being read, under `columns_from: grants`. MySQL does not move privileges when a table \
         is renamed: they stay with the name, so the table now under a name is not the one \
         its readable columns were granted on, and the rows logged before the rename and \
         after it would be read through one table's columns and delivered as another's. \
         Nothing past the rename was delivered. GRANT SELECT on the columns of the table that \
         holds the name now, then: {}",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

/// Refuse a column renamed under `columns_from: grants`: privileges do not follow a rename.
pub(crate) fn column_rename_refusal_message(schema: &str, table: &str) -> String {
    format!(
        "mysql cdc: `{schema}.{table}` had a column renamed (ALTER TABLE ... RENAME COLUMN or \
         CHANGE) in the log being read, under `columns_from: grants`. MySQL does not carry \
         column privileges across a rename, so the rows logged before it would have been read \
         without the column and delivered with it missing, and nothing past the rename was \
         delivered. GRANT SELECT on the column's new name if it is to be delivered, then: {}",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

/// The tables a `DROP TABLE[S]` QUERY event removes (`None` for an unreadable name); `None` when the event is not one.
pub(crate) fn drop_table_targets(
    sql: &str,
    event_db: &str,
    mode: SqlMode,
) -> Option<Vec<Option<(String, String)>>> {
    let toks = significant(sql, mode);
    if !(word_is(&toks, 0, "drop") && (word_is(&toks, 1, "table") || word_is(&toks, 1, "tables"))) {
        return None;
    }
    let at = 2 + 2 * usize::from(word_is(&toks, 2, "if") && word_is(&toks, 3, "exists"));
    Some(
        toks[at..]
            .split(|t| *t == Tok::Punct(','))
            .map(|group| {
                let group = match group {
                    [rest @ .., Tok::Word(k)]
                        if (k.eq_ignore_ascii_case("restrict")
                            || k.eq_ignore_ascii_case("cascade"))
                            && !matches!(rest.last(), None | Some(Tok::Punct('.'))) =>
                    {
                        rest
                    }
                    _ => group,
                };
                let (schema, table, end) = table_name_at(group, 0)?;
                (end == group.len()).then(|| qualified(schema, table, event_db))
            })
            .collect(),
    )
}

/// Whether a binlog dump error says the server purged the log the resume position needs (ER_MASTER_FATAL_ERROR_READING_BINLOG with either wording: by file, by GTID).
pub(crate) fn names_purged_binlog(code: u16, message: &str) -> bool {
    code == 1236
        && (message.contains("Could not find first log file name")
            || message.contains("purged required binary logs"))
}

/// A binlog dump error as it is shown: a purged log is a `SOURCE_CDC_LOG_GAP` refusal, anything else the driver's own error.
fn binlog_read_error(e: mysql::Error, file: &str) -> anyhow::Error {
    match &e {
        mysql::Error::MySqlError(m) if names_purged_binlog(m.code, &m.message) => {
            crate::source::cdc::checkpoint_identity::log_gap(purged_binlog_message(
                file, &m.message,
            ))
        }
        _ => e.into(),
    }
}

/// Refuse a resume position whose binlog the server purged.
pub(crate) fn purged_binlog_message(file: &str, server_said: &str) -> String {
    format!(
        "mysql cdc: the server no longer holds binlog `{file}`, where the checkpoint resumes \
         (ERROR 1236: {server_said}) — it was purged past the checkpoint, and the changes \
         between the checkpoint and the oldest binlog the server still has are no longer in \
         the log. {}",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

/// Refuse a DROP of a captured table: its rows leave the source without events, and a table re-created under the name is another table's history.
pub(crate) fn drop_refusal_message(target: Option<&(String, String)>) -> String {
    let what = match target {
        Some((schema, table)) if !schema.is_empty() => format!("`{schema}.{table}`"),
        Some((_, table)) => format!("`{table}`"),
        None => "a table this reader could not identify".to_string(),
    };
    format!(
        "mysql cdc: captured table {what} was DROPped, and this reader cannot represent that as a \
         change. Skipping it would leave every row it held live in the destination, and a table \
         re-created under the same name would continue that history as if it were one table. {}",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

/// The table a statement-logged DML event writes: `None` when the event is not DML, `Some(None)` when it is but its target cannot be read.
pub(crate) fn statement_dml_target(
    sql: &str,
    event_db: &str,
    mode: SqlMode,
) -> Option<Option<(String, String)>> {
    let toks = significant(sql, mode);
    let Some(Tok::Word(verb)) = toks.first() else {
        return None;
    };
    let skip: &[&str] = match verb.to_ascii_lowercase().as_str() {
        "insert" | "replace" => &["low_priority", "delayed", "high_priority", "ignore", "into"],
        "update" => &["low_priority", "ignore"],
        "delete" => &["low_priority", "quick", "ignore", "from"],
        "load" => return Some(None),
        _ => return None,
    };
    let at = 1 + toks[1..]
        .iter()
        .take_while(
            |t| matches!(t, Tok::Word(w) if skip.contains(&w.to_ascii_lowercase().as_str())),
        )
        .count();
    if at >= toks.len() {
        return None;
    }
    let Some((schema, table, end)) = table_name_at(&toks, at) else {
        return Some(None);
    };
    if toks.get(end) == Some(&Tok::Punct(',')) {
        return Some(None);
    }
    Some(Some(qualified(schema, table, event_db)))
}

/// Refuse a change a writer logged as a SQL statement: this reader decodes only row events.
pub(crate) fn statement_dml_refusal_message(target: Option<&(String, String)>) -> String {
    let what = match target {
        Some((schema, table)) if !schema.is_empty() => format!("`{schema}.{table}`"),
        Some((_, table)) => format!("`{table}`"),
        None => "a table this reader could not identify".to_string(),
    };
    format!(
        "mysql cdc: a change to {what} was written to the binlog as a SQL STATEMENT (the writing \
         session ran with binlog_format=STATEMENT or MIXED), and this reader decodes only row \
         events. Skipping it would drop the change while the checkpoint moves past it. Set \
         binlog_format=ROW for every writer — the global setting does not bind a session that \
         changes its own — then: {}",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

/// Refuse a TRUNCATE on a captured table. Same contract as the PostgreSQL arm —
/// see `postgres::cdc::truncate_refusal_message` for why this bails rather than
/// warns: the rows leave the source with no DELETE events to carry them, so the
/// destination disagrees with its source permanently and no later capture can
/// reconcile it.
pub(crate) fn truncate_refusal_message(schema: &str, table: &str) -> String {
    let qualified = if schema.is_empty() {
        table.to_string()
    } else {
        format!("{schema}.{table}")
    };
    format!(
        "mysql cdc: `{qualified}` was TRUNCATEd, and this reader cannot represent that as a \
         change. Skipping it would leave every row the truncate removed sitting in the \
         destination with no DELETE to retract it — the source empty, the destination \
         not, permanently, because those rows left the source without events and no \
         later capture can reconcile them. {}",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

/// A prepared XA branch this reader cannot frame — and must not let another
/// transaction publish.
///
/// This is a HARD STOP, not a deferral, and the message says so. The TRUNCATE
/// refusal beside it can promise recovery because re-running re-reads the same
/// rows; this one cannot. The `XA_prepare_log_event` stays in the binlog forever,
/// so every re-run from the same checkpoint hits it again — MEASURED: run, refuse,
/// re-run, refuse. The only way past is a checkpoint on the far side of the branch,
/// which skips whatever else happened in between, which is why the remedy is the
/// same re-snapshot TRUNCATE asks for.
///
/// It also refuses a branch that goes on to COMMIT, because at `XA PREPARE` there is
/// nothing to distinguish the two — the outcome is a Query event that has not been
/// read yet. Over-refusing is the safe direction: the alternative shipped a
/// The XA+spill refusal — a HARD STOP, and the message says so.
///
/// The first version prescribed "commit the XA branch promptly so it frames
/// normally", which clears the condition in NO world: the refusal fires at the
/// `XA_prepare_log_event`, the checkpoint has not advanced, and an `XA COMMIT` is
/// appended AFTER the PREPARE — so every re-run replays the same rows from the
/// checkpoint, re-crosses the same cap, re-spills, and bails at the SAME immutable
/// PREPARE before ever reaching the commit. The sibling plain-XA refusal had
/// already measured exactly this loop ("run, refuse, re-run, refuse") and its
/// message says "does NOT clear by re-running"; this one said neither. A refusal
/// that can persist for hours (XA's whole point) must tell the operator the
/// stream is STOPPED, and name the ladder honestly: raising the caps un-wedges
/// only a branch that turns out NOT to touch a captured table — if it is ours,
/// the next rung is the plain refusal, whose escape (re-anchor FIRST, then
/// re-snapshot) is the one that always works.
pub(crate) fn xa_prepare_spilled_tail_refusal_message(tail_rows: usize) -> String {
    format!(
        "mysql cdc: an XA PREPARE arrived while this transaction's tail \
         ({tail_rows} row(s)) is spilled to disk, so rivet cannot see whether the \
         branch touches a captured table — and dropping a branch whose captured \
         rows sit only in the tail would lose them silently. This is a HARD STOP: \
         the PREPARE event is permanent in the binlog, so re-running refuses again \
         at the same event; committing the branch does not clear it (the commit \
         lands AFTER the PREPARE the re-read stops at). To proceed: raise \
         RIVET_CDC_MAX_TX_ROWS/_BYTES past this branch's size so the whole branch \
         is scanned in memory — if the branch does not touch a captured table, \
         capture then continues; if it does, the plain XA refusal fires next and \
         names the re-baseline that moves the stream past the branch."
    )
}

/// rolled-back row as committed.
pub(crate) fn xa_prepare_refusal_message(schema: &str, table: &str) -> String {
    let qualified = if schema.is_empty() {
        table.to_string()
    } else {
        format!("{schema}.{table}")
    };
    format!(
        "mysql cdc: `{qualified}` was written inside an XA transaction that reached \
         `XA PREPARE`, and this reader cannot frame a prepared branch. Prepared is not \
         committed — the branch may still `XA ROLLBACK` — and rivet holds one \
         transaction buffer per stream, so the NEXT transaction's commit would drain \
         these rows too and publish them under ITS position: rows that never existed, \
         stamped with someone else's commit. Fabricating a row is worse than failing, \
         so the run stops with the checkpoint unmoved.\n\n\
         This does NOT clear by re-running: the prepare stays in the binlog and every \
         run from this checkpoint reaches it again. To move past it: {} To avoid it, \
         do not drive captured tables through an XA transaction manager until rivet \
         frames XA branches by their xid; those tables can be exported with `mode: \
         full` meanwhile.",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

/// Whether a pending grants re-read may wait past this event.
fn defers_the_grants_re_read(data: &Option<EventData<'_>>) -> bool {
    match data {
        Some(EventData::QueryEvent(qe)) => {
            let (sql, mode) = event_statement(qe);
            is_privilege_statement(&sql, mode)
        }
        Some(
            EventData::GtidEvent(_)
            | EventData::AnonymousGtidEvent(_)
            | EventData::PreviousGtidsEvent(_)
            | EventData::RotateEvent(_)
            | EventData::FormatDescriptionEvent(_)
            | EventData::HeartbeatEvent
            | EventData::IgnorableEvent(_),
        ) => true,
        _ => false,
    }
}

/// A statement that can change what an account may read.
fn is_privilege_statement(sql: &str, mode: SqlMode) -> bool {
    let mut w = tokens(sql, mode).into_iter().filter_map(|t| match t {
        Tok::Blank(_) => None,
        Tok::Word(w) => Some(Some(w.to_ascii_uppercase())),
        _ => Some(None),
    });
    let mut next = || w.next().flatten();
    matches!(
        (next().as_deref(), next().as_deref(), next().as_deref()),
        (Some("GRANT" | "REVOKE"), ..)
            | (
                Some("CREATE" | "DROP" | "ALTER" | "RENAME"),
                Some("USER" | "ROLE"),
                _
            )
            | (Some("SET"), Some("DEFAULT"), Some("ROLE"))
            | (Some("FLUSH"), Some("PRIVILEGES"), _)
    )
}

/// Does this QUERY event close a transaction?
///
/// `COMMIT` on its own (the non-transactional-engine and binlog-format path) and
/// `XA COMMIT <xid>`. Deliberately NOT `XA PREPARE` — prepared is not committed, and
/// releasing there would publish rows a later `XA ROLLBACK` erases. Not `BEGIN`,
/// `SAVEPOINT` or `ROLLBACK` either.
///
/// That last exclusion is MEASURED, so nobody re-derives it: a `ROLLBACK` arm here would be dead code, and
/// a dangerous one. It was written and then removed after two `SHOW BINLOG EVENTS`
/// probes on the stand (MySQL 8.0.46, `binlog_format=ROW`):
///
/// * a mixed InnoDB+MyISAM transaction rolled back logs ONLY the MyISAM rows — the
///   InnoDB ones are rolled back UNWRITTEN, so a captured InnoDB table has nothing
///   buffered when the rollback arrives;
/// * a pure MyISAM transaction rolled back logs `BEGIN … Write_rows … `**`COMMIT`** —
///   the rows are permanent, so the server frames them as their own transaction.
///
/// So no shape reaches this file with rows buffered AND a `ROLLBACK` marker, and
/// treating one as "discard the buffer" would DELETE rows that are still in the
/// source. `roast_mysql_cdc_a_rolled_back_myisam_statement_is_framed_as_its_own_
/// transaction` pins the real contract instead.
fn is_commit_statement(sql: &str) -> bool {
    let t = sql.trim().trim_end_matches(';').trim();
    if t.eq_ignore_ascii_case("commit") {
        return true;
    }
    let mut w = t.split_whitespace();
    matches!(
        (w.next(), w.next()),
        (Some(a), Some(b)) if a.eq_ignore_ascii_case("xa") && b.eq_ignore_ascii_case("commit")
    )
}

/// Is the commit at `(file, pos)` past the open-time bound? Binlog files order by their numeric suffix; an unparseable name is never past.
fn commit_past_bound(file: &str, pos: u64, bound: Option<&(String, u64)>) -> bool {
    let Some((bound_file, bound_pos)) = bound else {
        return false;
    };
    let (Some(cur), Some(bnd)) = (binlog_file_ordinal(file), binlog_file_ordinal(bound_file))
    else {
        return false;
    };
    cur > bnd || (cur == bnd && pos > *bound_pos)
}

/// The numeric suffix of a binlog file name (`mysql-bin.000042` → 42).
pub(crate) fn binlog_file_ordinal(name: &str) -> Option<u64> {
    name.rsplit_once('.')?.1.parse().ok()
}

/// Decode a binlog row's cells to typed [`RivetValue`]s (structural — no string
/// reparse of temporals).
/// Returns the row's values plus the INDICES of cells that could not be decoded —
/// a cell that has no faithful representation, never one that is genuinely absent.
/// The caller turns those indices into a deferred poison naming the columns.
fn render_row(r: mysql::binlog::row::BinlogRow) -> (Vec<RivetValue>, Vec<usize>) {
    let mut undecodable = Vec::new();
    let vals = r
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, bv)| match binlog_value_to_rivet(bv) {
            Some(v) => v,
            None => {
                undecodable.push(i);
                RivetValue::Null
            }
        })
        .collect();
    (vals, undecodable)
}

/// `None` ⇒ this cell has no faithful representation — the caller must refuse, not
/// write the placeholder NULL this returns alongside it.
fn binlog_value_to_rivet(bv: &BinlogValue) -> Option<RivetValue> {
    match bv {
        BinlogValue::Value(v) => Some(RivetValue::from_mysql(v)),
        // A JSON column arrives as MySQL's internal JSONB binary — convert it to
        // JSON text in the SAME rendering the server itself produces (", " and
        // ": " separators), so CDC and batch outputs of one value are
        // byte-identical; compact serde output would differ only in whitespace.
        // A JSONB tree MySQL materialised server-side carries OPAQUE leaves — a
        // DECIMAL, DATE, TIME or DATETIME stored with a type tag and raw bytes rather
        // than as JSON text. `serde_json::Value::try_from` errors on the first one and
        // the object/array arms propagate it, so ONE opaque leaf discarded the WHOLE
        // document. Measured: `JSON_OBJECT('amt', CAST(1.50 AS DECIMAL(10,2)), 'sku',
        // 'A1')` and `JSON_OBJECT('created', CAST('2024-01-02' AS DATE))` both came
        // back `doc: null` under `status: success`, while the plain text literal
        // beside them was fine — and a `mode: full` export of the SAME table wrote all
        // three correctly. So a snapshot-plus-CDC pipeline shows the column populated
        // by the snapshot leg and NULL for every change row that touches it.
        //
        // Nulling is not an option and neither is the crate's own fallback, which
        // renders an opaque leaf as `base64:type<N>:<bytes>` — writing a marker where
        // a number belongs is the same class as the `unchanged-toast-datum` refusal on
        // PostgreSQL. rivet cannot render these bytes the way MySQL does, so it says
        // so and refuses this cell; the caller defers the refusal to routing.
        BinlogValue::Jsonb(j) => match serde_json::Value::try_from(j.clone()) {
            Ok(json) => Some(RivetValue::Bytes(mysql_style_json(&json).into_bytes())),
            Err(_) => None,
        },
        // JSONB partial-update diffs (rare) — carry the debug bytes as text.
        other => Some(RivetValue::Bytes(format!("{other:?}").into_bytes())),
    }
}

/// Serialise with MySQL's own JSON text spacing: `", "` between elements and
/// `": "` after keys (`{"n": 1, "tier": "gold"}`), which is what the batch
/// export reads from the server as text.
fn mysql_style_json(v: &serde_json::Value) -> String {
    struct MySqlFmt;
    impl serde_json::ser::Formatter for MySqlFmt {
        fn begin_array_value<W: ?Sized + std::io::Write>(
            &mut self,
            w: &mut W,
            first: bool,
        ) -> std::io::Result<()> {
            if !first {
                w.write_all(b", ")?;
            }
            Ok(())
        }
        fn begin_object_key<W: ?Sized + std::io::Write>(
            &mut self,
            w: &mut W,
            first: bool,
        ) -> std::io::Result<()> {
            if !first {
                w.write_all(b", ")?;
            }
            Ok(())
        }
        fn begin_object_value<W: ?Sized + std::io::Write>(
            &mut self,
            w: &mut W,
        ) -> std::io::Result<()> {
            w.write_all(b": ")
        }
    }
    let mut out = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut out, MySqlFmt);
    use serde::Serialize as _;
    if v.serialize(&mut ser).is_err() {
        return v.to_string();
    }
    String::from_utf8(out).unwrap_or_else(|_| v.to_string())
}

impl Iterator for MysqlChangeStream {
    type Item = Result<ChangeEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(e) = self.spill_error.take() {
                return Some(Err(e));
            }
            if let Some(ev) = self.pending.pop_front() {
                return Some(Ok(ev));
            }
            // A spilled tail outranks another `fill`: its rows belong to a
            // transaction already framed, and reading more binlog before finishing
            // it would interleave a later transaction into it.
            if self.spooled.is_some() {
                match self.next_spooled() {
                    Ok(Some(ev)) => return Some(Ok(ev)),
                    Ok(None) => {}
                    Err(e) => return Some(Err(e)),
                }
            }
            match self.fill() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// What a binlog event of `schema` is to a bare configured name the rule resolved to `resolved`.
#[derive(Debug, PartialEq, Eq)]
enum BareRoute {
    /// The database the name means: the event is captured.
    Captured,
    /// Another database's table of that name: not captured.
    Elsewhere,
    /// The rule resolved nothing at open: the wire guard decides.
    Unresolved,
}

/// Route a bare name's event of `schema` by the database the rule resolved it to.
fn bare_event_route(resolved: Option<&str>, schema: &str) -> BareRoute {
    match resolved {
        Some(db) if db == schema => BareRoute::Captured,
        Some(_) => BareRoute::Elsewhere,
        None => BareRoute::Unresolved,
    }
}

impl ChangeStream for MysqlChangeStream {
    fn row_image(&mut self, _tables: &[String]) -> crate::source::cdc::RowImage {
        self.row_image.clone()
    }

    fn resolved_identity(&self, configured: &str) -> Option<(String, String)> {
        self.bare_db
            .get(configured)
            .map(|db| (db.clone(), configured.to_string()))
    }

    fn readable_columns(&self, configured: &str) -> Option<crate::source::cdc::ReadableColumns> {
        self.readable.as_ref()?.get(configured).cloned()
    }

    fn engine(&self) -> crate::source::cdc::CdcEngine {
        crate::source::cdc::CdcEngine::Mysql
    }

    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        self.next()
    }
}

#[cfg(test)]
mod tests {
    /// Every arm of the checkpoint-identity verdict.
    ///
    /// This closes a hole rather than tightening one: a MySQL checkpoint is binlog
    /// COORDINATES, and nothing stopped a checkpoint written by one server from
    /// being used against another. `binlog.000042:12345` parses on any host and
    /// addresses a different binlog on each.
    /// The XA+spill refusal tells the operator the truth: hard stop, re-running
    /// refuses again, committing does not clear it, and the ladder out.
    ///
    /// The first version said "commit the XA branch promptly so it frames
    /// normally" — impossible in every world (the commit lands AFTER the PREPARE
    /// the re-read stops at), on a refusal that can legitimately persist for
    /// hours. Each pinned phrase below is a CLAIM the code makes true; drift the
    /// message and this names which promise broke.
    #[test]
    fn privilege_statements_are_told_from_every_other_statement() {
        for sql in [
            "GRANT SELECT (id) ON rivet.t TO 'u'@'%'",
            "  revoke select (pass) on rivet.t from u",
            "/* xid=1 */ /* note */ GRANT r TO u",
            "/*!80000 REVOKE SELECT (pass) ON rivet.t FROM u */",
            "/*!REVOKE SELECT (pass) ON rivet.t FROM u */",
            "/*!80000 -- note\nREVOKE SELECT (pass) ON rivet.t FROM u */",
            "/*!80000 # note\nREVOKE SELECT (pass) ON rivet.t FROM u */",
            "/*!80000 /* note */ REVOKE SELECT (pass) ON rivet.t FROM u */",
            "-- note\nGRANT r TO u",
            "# note\nGRANT r TO u",
            "--\tnote\nGRANT r TO u",
            "/*!80000 GRANT SELECT (id) ON rivet.t TO u */",
            "/* xid=1 */ /*!50000 CREATE USER u */",
            "/*!80000 FLUSH */ PRIVILEGES",
            "CREATE USER 'u'@'%' IDENTIFIED BY 'p'",
            "drop role r",
            "ALTER USER u DEFAULT ROLE r",
            "RENAME USER a TO b",
            "SET DEFAULT ROLE r TO u",
            "FLUSH PRIVILEGES;",
        ] {
            assert!(is_privilege_statement(sql, SqlMode::default()), "{sql}");
        }
        for sql in [
            "",
            "/* unterminated",
            "BEGIN",
            "COMMIT",
            "CREATE TABLE grants (id INT)",
            "INSERT INTO grant_log VALUES (1)",
            "FLUSH TABLES",
            "SET ROLE r",
            "ALTER TABLE t ADD COLUMN revoke_at INT",
            "/* GRANT */ UPDATE t SET a = 1",
            "/*+ SET_VAR(x=1) */ UPDATE t SET a = 1",
            "/*!80000 UPDATE t SET revoke_at = 1 */",
            "-- GRANT r TO u\nUPDATE t SET a = 1",
            "# GRANT r TO u\nUPDATE t SET a = 1",
            "/*!80000 -- GRANT r TO u\nUPDATE t SET a = 1 */",
            "'GRANT' r TO u",
            "\"REVOKE\" x FROM u",
            "`GRANT` r TO u",
            "/*! unterminated",
            "ГРАНТ", // allow-cyrillic
        ] {
            assert!(!is_privilege_statement(sql, SqlMode::default()), "{sql}");
        }
    }

    #[test]
    fn the_xa_spill_refusal_names_the_stop_the_loop_and_the_ladder() {
        let msg = xa_prepare_spilled_tail_refusal_message(7);
        assert!(msg.contains("7 row(s)"), "names the spilled tail size");
        assert!(msg.contains("HARD STOP"), "says the stream is stopped");
        assert!(
            msg.contains("re-running refuses again"),
            "says the loop out loud — the PREPARE is permanent in the binlog"
        );
        assert!(
            msg.contains("committing the branch does not clear it"),
            "the first version prescribed exactly this non-remedy"
        );
        assert!(
            msg.contains("RIVET_CDC_MAX_TX_ROWS"),
            "the one rung that can un-wedge a not-ours branch"
        );
        assert!(
            msg.contains("the plain XA refusal fires next and names the re-baseline"),
            "the always-working escape is the plain refusal's re-baseline"
        );
        assert!(
            !msg.contains("frames normally"),
            "the retired false promise must not drift back in"
        );
    }

    #[test]
    fn a_checkpoint_from_another_server_is_refused_and_says_why() {
        use super::CheckpointIdentity as C;
        let a = "feada5c8-8818-11f1-a5e3-0242c0a8e406";
        let b = "00000000-1111-2222-3333-444444444444";

        // SAME server, no GTID in play — the ordinary resume.
        assert_eq!(
            MysqlChangeStream::checkpoint_identity_verdict(Some(a), None, a, None, None),
            C::Ok
        );
        assert!(C::Ok.refusal().is_none() && C::Ok.warning().is_none());

        // A DIFFERENT server. The whole point.
        let v = MysqlChangeStream::checkpoint_identity_verdict(Some(a), None, b, None, None);
        assert_eq!(
            v,
            C::ForeignServer {
                checkpoint: a.into(),
                server: b.into()
            }
        );
        let msg = v.refusal().expect("a foreign server must REFUSE");
        assert!(
            msg.contains(a) && msg.contains(b) && msg.contains("per-server"),
            "the refusal must name BOTH servers and the reason — an operator whose \
             failover just happened needs to know which is which: {msg}"
        );
        let err = v.verdict().enforce().expect_err("enforced, it refuses");
        assert!(
            err.to_string()
                .ends_with(crate::source::cdc::checkpoint_identity::RECOVER),
            "and name the recovery, which is a re-snapshot: the old coordinates \
             cannot be carried to a new server at all"
        );

        // NO identity: accepted, but SAID. Refusing would strand a checkpoint
        // written before this existed; saying nothing is the silent half of the bug.
        let v = MysqlChangeStream::checkpoint_identity_verdict(None, None, a, None, None);
        assert_eq!(v, C::Unverifiable);
        assert!(v.refusal().is_none(), "an old checkpoint must still resume");
        assert!(
            v.warning()
                .expect("but it must SAY so")
                .contains("per-server")
                && v.warning()
                    .unwrap()
                    .ends_with(crate::source::cdc::checkpoint_identity::RECOVER),
            "the warning has to name the hazard, not just admit ignorance"
        );

        // SAME server, GTID set NO LONGER contained — a `RESET MASTER` keeps the
        // uuid and empties the history, which the uuid check alone cannot see.
        let g = "feada5c8-8818-11f1-a5e3-0242c0a8e406:1-100";
        let v = MysqlChangeStream::checkpoint_identity_verdict(
            Some(a),
            Some(g),
            a,
            Some(""),
            Some(false),
        );
        assert_eq!(
            v,
            C::GtidNotContained {
                checkpoint: g.into(),
                server: String::new()
            }
        );
        assert!(v.refusal().expect("must refuse").contains("RESET MASTER"));

        // …and contained is fine.
        assert_eq!(
            MysqlChangeStream::checkpoint_identity_verdict(
                Some(a),
                Some(g),
                a,
                Some(g),
                Some(true)
            ),
            C::Ok
        );
        // An EMPTY recorded set is not a claim about anything — gtid_mode was off
        // when it was written, so containment says nothing and must not refuse.
        assert_eq!(
            MysqlChangeStream::checkpoint_identity_verdict(
                Some(a),
                Some(""),
                a,
                Some(""),
                Some(false)
            ),
            C::Ok,
            "an empty GTID set records `gtid_mode was off`, not `no transactions` — \
             refusing on it would break every non-GTID deployment, which is the \
             DEFAULT one"
        );
        // No containment answer for a non-empty set: resume on the uuid, but SAY so.
        assert_eq!(
            MysqlChangeStream::checkpoint_identity_verdict(Some(a), Some(g), a, None, None),
            C::GtidUnanswered
        );
        assert!(matches!(
            C::GtidUnanswered.verdict(),
            IdentityVerdict::Unverifiable(w) if w.contains("GTID_SUBSET")
        ));
    }

    /// The rule is DETERMINISTIC — a bare name means the connection's own database,
    /// and any other schema is the ambiguity.
    ///
    /// The first cut compared against the FIRST schema seen in the current run and
    /// kept that in a per-run map, so a run in which only the foreign database wrote
    /// recorded IT as "first" and captured its rows in silence. MEASURED: `status:
    /// success, rows: 1` with an independent read showing `insert | 99 |
    /// FOREIGN-ROW`. Under `until_current` on a schedule, "only one of the two wrote
    /// this cycle" is the ORDINARY case.
    ///
    /// Checked on the WIRE because no catalog can answer it here: MySQL's
    /// `information_schema` is filtered by PRIVILEGE while `REPLICATION SLAVE` hands
    /// over the whole server's binlog.
    /// A bare name means the connection's database when that holds the name, else the one other
    /// database holding a base table of it, else nothing: one answer for check, wire and baseline.
    #[test]
    fn a_bare_name_resolves_to_its_own_database_then_to_the_only_other() {
        let m = |schema: &str, kind: &str| crate::source::cdc::identity::CatalogMatch {
            schema: schema.into(),
            table: "orders".into(),
            kind: kind.into(),
            capturable: kind == "BASE TABLE",
        };
        let db = MysqlChangeStream::bare_name_database;
        let (base, view) = ("BASE TABLE", "VIEW");
        assert_eq!(
            db("shop", &[m("shop", base), m("archive", base)]),
            Some("shop".into())
        );
        assert_eq!(
            db("shop", &[m("shop", view), m("archive", base)]),
            Some("shop".into()),
            "a view of our own is what the name means; the foreign table is never elected"
        );
        assert_eq!(db("shop", &[m("archive", base)]), Some("archive".into()));
        assert_eq!(db("shop", &[m("archive", base), m("old", base)]), None);
        assert_eq!(db("shop", &[m("archive", view)]), None);
        assert_eq!(db("shop", &[]), None);
        assert_eq!(db("", &[m("archive", base)]), Some("archive".into()));
        assert_eq!(db("", &[m("archive", base), m("old", base)]), None);
        assert_eq!(bare_event_route(Some("shop"), "shop"), BareRoute::Captured);
        assert_eq!(
            bare_event_route(Some("shop"), "archive"),
            BareRoute::Elsewhere
        );
        assert_eq!(bare_event_route(None, "archive"), BareRoute::Unresolved);
    }

    #[test]
    fn a_bare_name_means_the_connections_own_database_and_any_other_is_refused() {
        // The ordinary case: the event is from our own database. Silent — this runs
        // on EVERY event, so a false positive here is an outage.
        assert_eq!(
            MysqlChangeStream::bare_name_spans_databases("orders", "shop", "shop"),
            None
        );

        // A QUALIFIED name is never ambiguous: routing compares the whole string.
        assert_eq!(
            MysqlChangeStream::bare_name_spans_databases("shop.orders", "shop", "archive"),
            None,
            "`database.table` names exactly one relation, whatever else exists"
        );

        // The defect. Note what makes this stronger than the version it replaces:
        // there is no "first seen" state, so the verdict does not depend on which
        // database happened to write first, or on the run boundary.
        let why = MysqlChangeStream::bare_name_spans_databases("orders", "shop", "archive")
            .expect("an event from another database is not what a bare name asked for");
        assert!(
            why.contains("shop") && why.contains("archive"),
            "name the database the config MEANT and the one the event came from: {why}"
        );
        assert!(
            why.contains("archive.orders"),
            "and hand over the fix in the form they must type: {why}"
        );
        assert!(
            why.contains("privilege"),
            "say WHY no catalog check warned them, or the next reader adds one and it \
             does not fire either: {why}"
        );
    }

    #[test]
    fn a_statement_that_gives_a_column_a_new_name_names_its_table() {
        let t = |sql: &str| column_rename_target(sql, "shop", SqlMode::default());
        let orders = Some(("shop".to_string(), "orders".to_string()));
        for renames in [
            "ALTER TABLE orders RENAME COLUMN name TO nm",
            "alter table `orders` rename column `name` to `nm`",
            "ALTER TABLE shop.orders CHANGE name nm VARCHAR(20)",
            "ALTER TABLE orders CHANGE COLUMN name nm VARCHAR(20) NOT NULL",
            "ALTER TABLE orders ADD COLUMN x INT, RENAME COLUMN name TO nm",
            "ALTER TABLE orders ALGORITHM=INPLACE, CHANGE `name` `Name2` INT",
            "ALTER IGNORE TABLE orders /* c */ RENAME COLUMN name TO nm;",
            "ALTER TABLE orders ADD COLUMN c VARCHAR(5) DEFAULT 'a,b', CHANGE name nm INT",
        ] {
            assert_eq!(t(renames), orders, "{renames}");
        }
        let other = t("ALTER TABLE `odd.name` RENAME COLUMN a TO b");
        assert_eq!(other, Some(("shop".to_string(), "odd.name".to_string())));
        assert_eq!(
            t("ALTER TABLE données RENAME COLUMN a TO b"),
            Some(("shop".to_string(), "données".to_string()))
        );
        for not_a_rename in [
            "ALTER TABLE orders ADD COLUMN extra INT",
            "ALTER TABLE orders DROP COLUMN name",
            "ALTER TABLE orders RENAME TO orders2",
            "ALTER TABLE orders RENAME INDEX a TO b",
            "ALTER TABLE orders CHANGE name NAME VARCHAR(30)",
            "ALTER TABLE orders MODIFY COLUMN name VARCHAR(30)",
            "ALTER TABLE orders ADD COLUMN c VARCHAR(5) COMMENT 'RENAME COLUMN a TO b'",
            "ALTER TABLE orders ADD (a INT, change INT)",
            "ALTER TABLE orders ADD COLUMN `rename` INT, ADD COLUMN `column` INT",
            "CREATE TABLE t (a INT, `change` INT)",
            "UPDATE orders SET name = 'RENAME COLUMN a TO b'",
            "ALTER TABLE orders RENAME COLUMN `unterminated",
            "",
        ] {
            assert_eq!(t(not_a_rename), None, "{not_a_rename}");
        }
    }

    #[test]
    fn every_scan_of_a_statement_reads_an_executable_comment_as_the_server_runs_it() {
        let orders = Some(("shop".to_string(), "orders".to_string()));
        for wrapped in [
            "/*!80000 ALTER TABLE orders RENAME COLUMN name TO nm */",
            "/*!ALTER TABLE orders CHANGE name nm VARCHAR(20) */",
            "ALTER TABLE orders /*!80000 RENAME COLUMN name TO nm */",
            "/* app */ /*!80000 ALTER TABLE orders RENAME COLUMN name TO nm */ /* xid=7 */",
        ] {
            assert_eq!(
                column_rename_target(wrapped, "shop", SqlMode::default()),
                orders,
                "{wrapped}"
            );
        }
        assert_eq!(
            truncate_target(
                "/*!50000 TRUNCATE TABLE orders */",
                "shop",
                SqlMode::default()
            ),
            orders
        );
        assert_eq!(
            drop_table_targets("/*!50000 DROP TABLE orders */", "shop", SqlMode::default()),
            Some(vec![orders.clone()])
        );
        assert_eq!(
            statement_dml_target(
                "/*!50000 INSERT INTO orders VALUES (1) */",
                "shop",
                SqlMode::default()
            ),
            Some(orders)
        );
        for plain_comment in [
            "/* ALTER TABLE orders RENAME COLUMN name TO nm */",
            "ALTER TABLE orders ADD COLUMN x INT /* RENAME COLUMN name TO nm */",
        ] {
            assert_eq!(
                column_rename_target(plain_comment, "shop", SqlMode::default()),
                None,
                "{plain_comment}"
            );
        }
        assert_eq!(
            truncate_target("/* TRUNCATE TABLE orders */", "shop", SqlMode::default()),
            None
        );
    }

    #[test]
    fn a_grant_difference_names_the_columns_gained_and_lost_folding_case() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let said = grants_difference("t", &s(&["id", "Name"]), &s(&["ID", "name", "pass"]));
        assert!(
            said.contains("gained `pass`") && !said.contains("lost"),
            "{said}"
        );
        let said = grants_difference("t", &s(&["id", "name"]), &s(&["id"]));
        assert!(
            said.contains("lost `name`") && !said.contains("gained"),
            "{said}"
        );
        let said = grants_difference("t", &s(&["id", "a"]), &s(&["id", "b"]));
        assert!(said.contains("gained `b`; lost `a`"), "{said}");
    }

    fn bare(t: &str) -> AlterToken {
        AlterToken::Word(t.to_string(), false)
    }

    fn ticked(t: &str) -> AlterToken {
        AlterToken::Word(t.to_string(), true)
    }

    #[test]
    fn an_alter_statement_is_cut_into_names_strings_and_punctuation_whatever_it_holds() {
        use AlterToken::{Punct, Text};
        let cut = |sql: &str| alter_tokens(sql, SqlMode::default());
        assert_eq!(cut("`a` x"), Some(vec![ticked("a"), bare("x")]));
        assert_eq!(cut("`a``b` x"), Some(vec![ticked("a`b"), bare("x")]));
        assert_eq!(cut("`a b.c`"), Some(vec![ticked("a b.c")]));
        assert_eq!(cut("`a``"), None, "a doubled tick opens nothing new");
        assert_eq!(cut("`ab"), None);
        assert_eq!(cut("'a''b' x"), Some(vec![Text, bare("x")]));
        assert_eq!(cut("\"a'b\" x"), Some(vec![ticked("a'b"), bare("x")]));
        assert_eq!(cut("\"a\"\"b\" x"), Some(vec![ticked("a\"b"), bare("x")]));
        assert_eq!(cut("\"ab"), None);
        assert_eq!(cut("'a\\'b' x"), Some(vec![Text, bare("x")]));
        assert_eq!(cut("'a,b' , c"), Some(vec![Text, Punct(','), bare("c")]));
        assert_eq!(cut("'' x"), Some(vec![Text, bare("x")]));
        assert_eq!(cut("'abc"), None);
        assert_eq!(cut("'a\\"), None);
        assert_eq!(
            cut("a.b,(c)"),
            Some(vec![
                bare("a"),
                Punct('.'),
                bare("b"),
                Punct(','),
                Punct('('),
                bare("c"),
                Punct(')')
            ])
        );
        for start in ["a", "7", "_", "$", "\u{20ac}", "\u{540d}"] {
            let word = format!("{start}x_$\u{20ac}\u{540d}1");
            assert_eq!(
                cut(&format!("{word} y")),
                Some(vec![bare(&word), bare("y")]),
                "a word starting with {start:?}"
            );
        }
    }

    #[test]
    fn a_renamed_column_is_found_in_any_spec_of_an_alter_statement_and_nowhere_else() {
        let at = |s: &str, t: &str| Some((s.to_string(), t.to_string()));
        for (sql, want) in [
            (
                "ALTER TABLE orders RENAME COLUMN a TO b",
                at("shop", "orders"),
            ),
            (
                "alter ignore table orders rename column a to b;",
                at("shop", "orders"),
            ),
            ("ALTER TABLE s.t CHANGE a b INT", at("s", "t")),
            ("ALTER TABLE t CHANGE COLUMN a b INT", at("shop", "t")),
            ("ALTER TABLE t CHANGE `a` `b` INT", at("shop", "t")),
            ("ALTER TABLE t ADD x INT, CHANGE a b INT", at("shop", "t")),
            (
                "ALTER TABLE t ADD x INT, RENAME COLUMN a TO b",
                at("shop", "t"),
            ),
            (
                "ALTER TABLE t ADD y INT DEFAULT (f(1, 2)), CHANGE a b INT",
                at("shop", "t"),
            ),
            (
                "ALTER TABLE `my.db`.`o``rd` RENAME COLUMN a TO b",
                at("my.db", "o`rd"),
            ),
            (
                "ALTER TABLE \u{8868} CHANGE \u{540d} \u{540d}\u{524d} INT",
                at("shop", "\u{8868}"),
            ),
            (
                "/* app */ ALTER /* x */ TABLE t /* y */ RENAME COLUMN a TO b",
                at("shop", "t"),
            ),
            (
                "ALTER TABLE db.t -- c\nRENAME COLUMN name TO nm",
                at("db", "t"),
            ),
            (
                "ALTER TABLE db.t # c\nRENAME COLUMN name TO nm",
                at("db", "t"),
            ),
            (
                "ALTER TABLE db.t --\tc\nRENAME COLUMN a TO b",
                at("db", "t"),
            ),
            (
                "/*!80000 ALTER TABLE db.t /* c */ RENAME COLUMN name TO nm */",
                at("db", "t"),
            ),
            (
                "/*!80000 ALTER TABLE t -- c\nRENAME COLUMN a TO b */",
                at("shop", "t"),
            ),
            (
                "ALTER TABLE \"db\".\"t\" RENAME COLUMN name TO nm",
                at("db", "t"),
            ),
            (
                "ALTER TABLE t CHANGE \"name\" \"nm\" VARCHAR(20)",
                at("shop", "t"),
            ),
            (
                "ALTER TABLE \"o\"\"rd\" RENAME COLUMN \"a\" TO \"b\"",
                at("shop", "o\"rd"),
            ),
            (
                "ALTER TABLE \"\u{8868}\" CHANGE \"\u{540d}\" \"\u{540d}\u{524d}\" INT",
                at("shop", "\u{8868}"),
            ),
            (
                "ALTER TABLE t ADD x INT COMMENT 'a -- b', CHANGE a b INT",
                at("shop", "t"),
            ),
            (
                "ALTER TABLE t ADD x INT COMMENT '#', RENAME COLUMN a TO b",
                at("shop", "t"),
            ),
            (
                "ALTER TABLE t ADD x INT COMMENT '/*', RENAME COLUMN a TO b",
                at("shop", "t"),
            ),
            (
                "ALTER TABLE t ADD x INT COMMENT \"it's\", RENAME COLUMN a TO b",
                at("shop", "t"),
            ),
        ] {
            assert_eq!(
                column_rename_target(sql, "shop", SqlMode::default()),
                want,
                "{sql}"
            );
        }
        for sql in [
            "ALTER TABLE t CHANGE a a INT",
            "ALTER TABLE t CHANGE COLUMN a A INT",
            "ALTER TABLE t CHANGE `a` a VARCHAR(5)",
            "ALTER TABLE t CHANGE",
            "ALTER TABLE t CHANGE COLUMN",
            "ALTER TABLE t RENAME TO u",
            "ALTER TABLE t RENAME u",
            "ALTER TABLE t ADD INDEX i (a, change b c)",
            "ALTER TABLE t ADD c VARCHAR(5) COMMENT 'rename column a to b'",
            "ALTER TABLE t ADD x INT /* RENAME COLUMN a TO b */",
            "ALTER TABLE t ADD x INT -- RENAME COLUMN a TO b",
            "ALTER TABLE t ADD x INT # RENAME COLUMN a TO b",
            "ALTER TABLE t ADD x INT /*! -- RENAME COLUMN a TO b\n*/",
            "ALTER TABLE t \"rename\" \"column\" a to b",
            "ALTER TABLE t CHANGE \"a\" \"a\" INT",
            "ALTER TABLE t ADD \"x",
            "ALTER TABLE t `rename` `column` a to b",
            "ALTER TABLE t ADD `x",
            "ALTER TABLE",
            "ALTER TABLE .t RENAME COLUMN a TO b",
            "ALTER VIEW v RENAME COLUMN a TO b",
            "CREATE TABLE t (rename column a to b)",
            "UPDATE t SET a = 'alter table t rename column a to b'",
            "",
        ] {
            assert_eq!(
                column_rename_target(sql, "shop", SqlMode::default()),
                None,
                "{sql}"
            );
        }
    }

    fn verdict_of(sql: &str, db: &str, projecting: bool, cfg: &[String]) -> QueryVerdict {
        query_verdict(
            sql,
            db,
            SqlMode::default(),
            &QueryScope {
                projecting,
                fold: false,
                configured: cfg,
            },
        )
    }

    #[test]
    fn a_run_refuses_a_column_rename_only_when_it_projects_and_captures_the_table() {
        let rename = "ALTER TABLE orders RENAME COLUMN a TO b";
        let only = |t: &str| vec![t.to_string()];
        let renamed = QueryVerdict::ColumnRenamed("shop".into(), "orders".into());
        assert_eq!(
            verdict_of(rename, "shop", true, &only("shop.orders")),
            renamed
        );
        assert_eq!(verdict_of(rename, "shop", true, &[]), renamed);
        for (projecting, db, cfg) in [
            (false, "shop", only("shop.orders")),
            (true, "shop", only("shop.other")),
            (true, "other", only("shop.orders")),
        ] {
            assert_eq!(
                verdict_of(rename, db, projecting, &cfg),
                QueryVerdict::Pass,
                "{projecting} {db} {cfg:?}"
            );
        }
        assert_eq!(
            verdict_of(
                "ALTER TABLE orders ADD x INT",
                "shop",
                true,
                &only("shop.orders")
            ),
            QueryVerdict::Pass
        );
    }

    const ANSI: SqlMode = SqlMode {
        no_backslash_escapes: false,
        ansi_quotes: true,
    };
    const NO_BACKSLASH: SqlMode = SqlMode {
        no_backslash_escapes: true,
        ansi_quotes: false,
    };

    fn sql_mode_var(bits: u64) -> Vec<u8> {
        [&[1u8][..], &bits.to_le_bytes()].concat()
    }

    #[test]
    fn a_sql_mode_status_var_is_read_from_the_event_and_found_among_others() {
        assert_eq!(mode_of(&sql_mode_var(0x4)), ANSI);
        assert_eq!(mode_of(&sql_mode_var(0x10_0000)), NO_BACKSLASH);
        assert_eq!(mode_of(&sql_mode_var(0)), SqlMode::default());
        let after_flags2 = [&[0u8, 0, 0, 0, 0][..], &sql_mode_var(0x4)].concat();
        assert_eq!(mode_of(&after_flags2), ANSI);
    }

    #[test]
    fn an_event_with_no_sql_mode_status_var_is_read_with_the_default_lexing() {
        assert_eq!(mode_of(&[]), SqlMode::default());
        assert_eq!(mode_of(&[0u8, 0, 0, 0, 0]), SqlMode::default());
        assert_eq!(
            mode_of(&sql_mode_var(0x4)[..5]),
            SqlMode::default(),
            "a sql_mode cut short is no sql_mode"
        );
    }

    #[test]
    fn a_rename_behind_a_quote_the_mode_makes_a_name_or_a_plain_string_is_found() {
        let at = Some(("shop".to_string(), "orders".to_string()));
        let cases = [
            (
                "ALTER TABLE orders COMMENT = 'x\\', RENAME COLUMN a TO b",
                NO_BACKSLASH,
                at.clone(),
            ),
            (
                "ALTER TABLE orders COMMENT = 'x\\', RENAME COLUMN a TO b",
                SqlMode::default(),
                None,
            ),
            (
                "ALTER TABLE orders ADD COLUMN \"x\\\" INT, RENAME COLUMN a TO b",
                ANSI,
                at.clone(),
            ),
            (
                "ALTER TABLE orders ADD COLUMN \"x\\\" INT, RENAME COLUMN a TO b",
                SqlMode::default(),
                None,
            ),
            (
                "ALTER TABLE orders COMMENT = 'it\\'s', RENAME COLUMN a TO b",
                SqlMode::default(),
                at.clone(),
            ),
            (
                "ALTER TABLE orders COMMENT = 'it\\'s', RENAME COLUMN a TO b",
                ANSI,
                at.clone(),
            ),
            (
                "ALTER TABLE orders COMMENT = 'it\\'s', RENAME COLUMN a TO b",
                NO_BACKSLASH,
                None,
            ),
            (
                "ALTER TABLE orders COMMENT = 'x\\', ADD COLUMN extra INT",
                NO_BACKSLASH,
                None,
            ),
            (
                "ALTER TABLE orders COMMENT = 'rename column a to b'",
                NO_BACKSLASH,
                None,
            ),
        ];
        for (sql, mode, want) in cases {
            assert_eq!(
                column_rename_target(sql, "shop", mode),
                want,
                "{sql} {mode:?}"
            );
        }
    }

    #[test]
    fn a_rename_of_a_table_is_read_from_both_statements_that_carry_one() {
        let at = |d: &str, n: &str| Some((d.to_string(), n.to_string()));
        let t = |sql: &str| renamed_tables(sql, "shop", SqlMode::default());
        assert_eq!(
            t("RENAME TABLE orders TO orders_old"),
            Some(vec![at("shop", "orders"), at("shop", "orders_old")])
        );
        assert_eq!(
            t("rename table `shop`.`orders` to `shop`.`orders_old`, orders_new to orders"),
            Some(vec![
                at("shop", "orders"),
                at("shop", "orders_old"),
                at("shop", "orders_new"),
                at("shop", "orders")
            ])
        );
        assert_eq!(
            t("/*!80000 RENAME TABLE a.t -- c\n TO b.t */"),
            Some(vec![at("a", "t"), at("b", "t")])
        );
        assert_eq!(
            t("ALTER TABLE orders RENAME TO archive.orders"),
            Some(vec![at("shop", "orders"), at("archive", "orders")])
        );
        assert_eq!(
            t("ALTER TABLE orders RENAME AS orders_old"),
            Some(vec![at("shop", "orders"), at("shop", "orders_old")])
        );
        assert_eq!(
            t("ALTER TABLE orders RENAME orders_old"),
            Some(vec![at("shop", "orders"), at("shop", "orders_old")])
        );
        assert_eq!(
            t("ALTER TABLE orders ADD COLUMN k INT, RENAME TO orders_old"),
            Some(vec![at("shop", "orders"), at("shop", "orders_old")])
        );
        assert_eq!(
            t("RENAME TABLE orders orders_old"),
            Some(vec![at("shop", "orders"), None])
        );
        assert_eq!(
            t("RENAME TABLE orders TO"),
            Some(vec![at("shop", "orders"), None])
        );
        assert_eq!(t("RENAME TABLE"), Some(vec![None]));
    }

    #[test]
    fn a_statement_that_renames_no_table_is_not_read_as_one() {
        let t = |sql: &str, mode: SqlMode| renamed_tables(sql, "shop", mode);
        for sql in [
            "ALTER TABLE orders RENAME COLUMN a TO b",
            "ALTER TABLE orders RENAME INDEX a TO b",
            "ALTER TABLE orders RENAME KEY a TO b",
            "ALTER TABLE orders ADD COLUMN `rename` INT",
            "ALTER TABLE orders COMMENT = 'rename to somewhere'",
            "ALTER TABLE orders ADD COLUMN k INT COMMENT 'x', ADD INDEX (a)",
            "ALTER TABLE orders PARTITION BY HASH (rename) PARTITIONS 2",
            "ALTER TABLE orders ADD CONSTRAINT c CHECK (rename = 1)",
            "ALTER TABLE orders /* RENAME TO x */ ADD COLUMN k INT",
            "ALTER TABLE orders ADD COLUMN k INT -- RENAME TO x",
            "RENAME USER a TO b",
            "RENAME ROLE a TO b",
            "CREATE TABLE rename (a INT)",
            "SELECT 'RENAME TABLE a TO b'",
            "DROP TABLE orders",
            "TRUNCATE orders",
            "",
            "ALTER TABLE",
            "ALTER TABLE `unclosed RENAME TO x",
        ] {
            assert_eq!(t(sql, SqlMode::default()), None, "{sql}");
        }
        assert_eq!(
            t(
                "ALTER TABLE orders COMMENT = 'x\\', RENAME TO y",
                NO_BACKSLASH
            ),
            Some(vec![
                Some(("shop".to_string(), "orders".to_string())),
                Some(("shop".to_string(), "y".to_string()))
            ]),
            "a backslash is no escape under NO_BACKSLASH_ESCAPES"
        );
        assert_eq!(
            t(
                "ALTER TABLE orders COMMENT = 'x\\', RENAME TO y",
                SqlMode::default()
            ),
            None,
            "a backslash escapes the quote by default, so the rest is one string"
        );
    }

    #[test]
    fn a_run_refuses_a_table_rename_only_when_it_projects_and_captures_a_table_it_touches() {
        let only = |t: &str| vec![t.to_string()];
        let swap = "RENAME TABLE orders TO orders_old, orders_new TO orders";
        let at = |t: &str| QueryVerdict::TableRenamed(Some(("shop".to_string(), t.to_string())));
        let refuse = |projecting: bool, sql: &str, db: &str, cfg: &[String]| {
            verdict_of(sql, db, projecting, cfg)
        };
        assert_eq!(
            refuse(true, swap, "shop", &only("shop.orders")),
            at("orders")
        );
        assert_eq!(refuse(true, swap, "shop", &[]), at("orders"));
        assert_eq!(
            refuse(false, swap, "shop", &only("shop.orders")),
            QueryVerdict::Pass
        );
        assert_eq!(
            refuse(true, swap, "shop", &only("shop.other")),
            QueryVerdict::Pass
        );
        assert_eq!(
            refuse(true, swap, "other", &only("shop.orders")),
            QueryVerdict::Pass
        );
        assert_eq!(
            refuse(
                true,
                "RENAME TABLE staging TO orders",
                "shop",
                &only("shop.orders")
            ),
            at("orders"),
            "a table renamed onto a captured name is touched"
        );
        assert_eq!(
            refuse(
                true,
                "ALTER TABLE orders RENAME TO orders_old",
                "shop",
                &only("shop.orders_old")
            ),
            at("orders_old"),
            "a table renamed from a captured name is touched"
        );
        assert_eq!(
            refuse(true, "RENAME TABLE a TO", "shop", &only("shop.orders")),
            QueryVerdict::TableRenamed(None),
            "a name this reader cannot read is refused, as every other statement's"
        );
        assert_eq!(
            refuse(
                true,
                "ALTER TABLE orders RENAME COLUMN a TO b",
                "shop",
                &only("shop.orders")
            ),
            QueryVerdict::ColumnRenamed("shop".into(), "orders".into())
        );
        assert_eq!(
            refuse(
                true,
                "ALTER TABLE orders RENAME INDEX a TO b",
                "shop",
                &only("shop.orders")
            ),
            QueryVerdict::Pass
        );
    }

    #[test]
    fn the_table_rename_refusal_names_the_table_the_cause_and_the_rebaseline() {
        let said = table_rename_refusal_message(Some(&("shop".to_string(), "orders".to_string())));
        assert!(
            said.starts_with(
                "mysql cdc: `shop.orders` was renamed (RENAME TABLE, or ALTER TABLE ... RENAME) in the log being read, under `columns_from: grants`."
            ),
            "{said}"
        );
        assert!(
            said.contains("MySQL does not move privileges when a table is renamed"),
            "{said}"
        );
        assert!(
            said.contains("GRANT SELECT on the columns of the table that holds the name now"),
            "{said}"
        );
        assert!(
            said.ends_with(crate::source::cdc::checkpoint_identity::RECOVER),
            "{said}"
        );
        assert!(
            table_rename_refusal_message(None).contains("a table this reader could not identify")
        );
    }

    #[test]
    fn truncate_drop_and_statement_writes_read_their_names_the_same_as_before_for_plain_and_backticked_names()
     {
        let at = |d: &str, n: &str| (d.to_string(), n.to_string());
        let names = [
            ("t", at("shop", "t")),
            ("Orders", at("shop", "Orders")),
            ("rivet.t", at("rivet", "t")),
            ("`t`", at("shop", "t")),
            ("`rivet`.`t`", at("rivet", "t")),
            ("rivet.`t`", at("rivet", "t")),
            ("`odd.name`", at("shop", "odd.name")),
            ("`a``b`", at("shop", "a`b")),
            ("`a`.`b``c`", at("a", "b`c")),
            ("donn\u{e9}es", at("shop", "donn\u{e9}es")),
            ("`\u{540d}`.`\u{8868}`", at("\u{540d}", "\u{8868}")),
            ("$t", at("shop", "$t")),
        ];
        let m = SqlMode::default();
        for (name, want) in names {
            for tail in ["", " /* c */", " -- c", " # c"] {
                for head in [
                    "TRUNCATE ",
                    "TRUNCATE TABLE ",
                    "/* c */ truncate table ",
                    "/*!50000 TRUNCATE */ ",
                ] {
                    let sql = format!("{head}{name}{tail}");
                    assert_eq!(
                        truncate_target(&sql, "shop", m),
                        Some(want.clone()),
                        "{sql}"
                    );
                }
                for head in ["DROP TABLE ", "drop table if exists "] {
                    let sql = format!("{head}{name}{tail}");
                    assert_eq!(
                        drop_table_targets(&sql, "shop", m),
                        Some(vec![Some(want.clone())]),
                        "{sql}"
                    );
                }
                for (head, rest) in [
                    ("INSERT INTO ", " VALUES (1)"),
                    ("INSERT IGNORE INTO ", " (id) VALUES (1)"),
                    ("REPLACE INTO ", " SELECT 1"),
                    ("UPDATE ", " SET v = 1"),
                    ("UPDATE LOW_PRIORITY IGNORE ", " SET v = 1"),
                    ("DELETE FROM ", " WHERE id = 1"),
                    ("DELETE QUICK FROM ", " WHERE id = 1"),
                ] {
                    let sql = format!("{head}{name}{rest}{tail}");
                    assert_eq!(
                        statement_dml_target(&sql, "shop", m),
                        Some(Some(want.clone())),
                        "{sql}"
                    );
                }
            }
        }
    }

    #[test]
    fn truncate_drop_and_statement_writes_read_double_quoted_names_as_identifiers() {
        let at = |d: &str, n: &str| (d.to_string(), n.to_string());
        for mode in [ANSI, SqlMode::default()] {
            assert_eq!(
                truncate_target("TRUNCATE \"rivet\".\"t\"", "shop", mode),
                Some(at("rivet", "t"))
            );
            assert_eq!(
                truncate_target("TRUNCATE TABLE \"t\"", "shop", mode),
                Some(at("shop", "t"))
            );
            assert_eq!(
                truncate_target("TRUNCATE \"a\"\"b\"", "shop", mode),
                Some(at("shop", "a\"b"))
            );
            assert_eq!(
                drop_table_targets(
                    "DROP TABLE IF EXISTS \"rivet\".\"t\", \"u\" RESTRICT",
                    "shop",
                    mode
                ),
                Some(vec![Some(at("rivet", "t")), Some(at("shop", "u"))])
            );
            assert_eq!(
                statement_dml_target("UPDATE \"rivet\".\"t\" SET v = 1", "shop", mode),
                Some(Some(at("rivet", "t")))
            );
            assert_eq!(
                statement_dml_target("INSERT INTO \"t\" (id) VALUES (1)", "shop", mode),
                Some(Some(at("shop", "t")))
            );
        }
    }

    #[test]
    fn a_name_that_cannot_be_read_whole_is_not_a_truncate_drop_or_write_target() {
        let m = SqlMode::default();
        for sql in [
            "TRUNCATE",
            "TRUNCATE TABLE",
            "TRUNCATE `unclosed",
            "TRUNCATE a b",
            "TRUNCATE a, b",
            "TRUNCATE a.b.c",
            "TRUNCATE .t",
            "TRUNCATE a.",
        ] {
            assert_eq!(truncate_target(sql, "shop", m), None, "{sql}");
        }
        assert_eq!(
            drop_table_targets("DROP TABLE", "shop", m),
            Some(vec![None])
        );
        assert_eq!(
            drop_table_targets("DROP TABLE `unclosed", "shop", m),
            Some(vec![None])
        );
        assert_eq!(
            drop_table_targets("DROP TABLE a, b c", "shop", m),
            Some(vec![Some(("shop".to_string(), "a".to_string())), None])
        );
        assert_eq!(
            drop_table_targets("DROP TABLE restrict", "shop", m),
            Some(vec![Some(("shop".to_string(), "restrict".to_string()))])
        );
        assert_eq!(
            drop_table_targets("DROP TEMPORARY TABLE a", "shop", m),
            None
        );
        assert_eq!(statement_dml_target("UPDATE", "shop", m), None);
        assert_eq!(statement_dml_target("UPDATE (", "shop", m), Some(None));
        assert_eq!(
            statement_dml_target("UPDATE a,b SET v = 1", "shop", m),
            Some(None)
        );
    }

    #[test]
    fn the_rename_refusal_names_the_table_the_cause_and_the_rebaseline() {
        let said = column_rename_refusal_message("shop", "orders");
        assert!(
            said.starts_with("mysql cdc: `shop.orders` had a column renamed"),
            "{said}"
        );
        assert!(
            said.contains("GRANT SELECT on the column's new name"),
            "{said}"
        );
        assert!(
            said.ends_with(crate::source::cdc::checkpoint_identity::RECOVER),
            "{said}"
        );
        assert!(column_rename_refusal_message("a", "b").contains("`a.b`"));
    }

    #[test]
    fn an_executable_comment_is_statement_text_and_every_other_comment_is_a_space() {
        for (sql, want) in [
            ("", ""),
            ("plain", "plain"),
            ("a/* c */b", "a b"),
            ("/*!80000 REVOKE x */", "  REVOKE x  "),
            ("/*!ALTER*/ t", " ALTER  t"),
            ("/*!*/", "  "),
            ("/*!50000*/", "  "),
            ("/*a*//*!b*/", "  b "),
            ("/* a /* b */ c */", "  c */"),
            ("x /*!80000 DROP", "x   DROP"),
            ("x /* y", "x  "),
            ("\u{e9}/*\u{fc}*/\u{f6}", "\u{e9} \u{f6}"),
            ("/*!80000 \u{540d} */", "  \u{540d}  "),
            ("a /*!80000 b */ c /* d */ e", "a   b   c   e"),
        ] {
            assert_eq!(
                crate::source::mysql::statement::executable_text(sql, SqlMode::default()),
                want,
                "{sql:?}"
            );
        }
    }

    #[test]
    fn a_pending_grants_re_read_waits_only_past_events_that_carry_nothing_to_deliver() {
        assert!(defers_the_grants_re_read(&Some(EventData::HeartbeatEvent)));
        assert!(!defers_the_grants_re_read(&None));
        assert!(!defers_the_grants_re_read(&Some(EventData::UnknownEvent)));
        assert!(!defers_the_grants_re_read(&Some(EventData::StopEvent)));
    }

    #[test]
    fn a_drop_table_names_every_table_it_removes() {
        let t = |sql: &str| drop_table_targets(sql, "shop", SqlMode::default());
        let at = |d: &str, n: &str| Some((d.to_string(), n.to_string()));
        assert_eq!(
            t("DROP TABLE `orders` /* generated by server */"),
            Some(vec![at("shop", "orders")])
        );
        assert_eq!(
            t("drop table if exists a, `b`.`c`, `odd, name`"),
            Some(vec![at("shop", "a"), at("b", "c"), at("shop", "odd, name")])
        );
        assert_eq!(
            t("DROP TABLE orders CASCADE"),
            Some(vec![at("shop", "orders")])
        );
        assert_eq!(t("DROP DATABASE shop"), None);
        assert_eq!(t("DROP TABLESPACE ts"), None);
        assert_eq!(t("TRUNCATE orders"), None);
    }

    #[test]
    fn only_a_replica_that_does_not_relog_is_refused() {
        assert!(replica_relog_refusal(true, Some("0")).is_err());
        assert!(replica_relog_refusal(true, Some("OFF")).is_err());
        assert!(replica_relog_refusal(true, Some("1")).is_ok());
        assert!(
            replica_relog_refusal(false, Some("0")).is_ok(),
            "a primary needs no re-logging"
        );
        assert!(
            replica_relog_refusal(true, None).is_ok(),
            "an unreadable setting must not block a source that may be fine"
        );
    }

    #[test]
    fn a_statement_logged_dml_names_the_table_it_writes() {
        let t = |sql: &str| statement_dml_target(sql, "shop", SqlMode::default());
        let at = |d: &str, n: &str| Some(Some((d.to_string(), n.to_string())));
        assert_eq!(t("INSERT INTO orders VALUES (1)"), at("shop", "orders"));
        assert_eq!(
            t("insert ignore into rivet.orders(id) values (1)"),
            at("rivet", "orders")
        );
        assert_eq!(
            t("/* app */ REPLACE INTO `odd name` VALUES (1)"),
            at("shop", "odd name"),
            "a quoted name is read whole, spaces and all"
        );
        assert_eq!(t("UPDATE `odd, name` SET v = 1"), at("shop", "odd, name"));
        assert_eq!(
            t("UPDATE `a`.`b`.`c` SET v = 1"),
            at("a", "b"),
            "a third part is not read"
        );
        assert_eq!(t("UPDATE `unclosed SET v = 1"), Some(None));
        assert_eq!(t("UPDATE orders , items SET v = 1"), Some(None));
        assert_eq!(t("DELETE"), None);
        assert_eq!(t("UPDATE `a`.`b` SET v = 1"), at("a", "b"));
        assert_eq!(t("DELETE FROM orders WHERE id = 1"), at("shop", "orders"));
        assert_eq!(
            t("UPDATE orders, items SET v = 1"),
            Some(None),
            "multi-table is unknown"
        );
        assert_eq!(t("LOAD DATA INFILE 'x' INTO TABLE orders"), Some(None));
        assert_eq!(t("BEGIN"), None);
        assert_eq!(t("COMMIT"), None);
        assert_eq!(
            t("CREATE TABLE orders (id INT)"),
            None,
            "DDL is not row data"
        );
        assert_eq!(t("TRUNCATE orders"), None, "TRUNCATE has its own refusal");
    }

    /// MySQL's TRUNCATE parser had ZERO offline coverage while the evidence matrix
    /// claimed it was graded by `a_mysql_truncate_statement_resolves_the_table_it_names`
    /// — a test that exists nowhere in the tree. PROVEN by stubbing this function to
    /// `return None` (the exact pre-fix silent divergence) and watching the whole lib
    /// suite stay green at 2616 passed. A cited grader nobody resolved against the
    /// source is a `sound` cell resting on a name.
    ///
    /// The shapes that matter are the ones a real server emits: a bare name resolved
    /// against the EVENT's database, a db-qualified one, backticks, and the comment
    /// forms every ORM appends.
    #[test]
    fn a_mysql_truncate_statement_resolves_the_table_it_names() {
        let t = |sql: &str| truncate_target(sql, "shop", SqlMode::default());

        // Bare name ⇒ the event's own database, which is the only place it can come
        // from: a binlog QUERY event carries the schema separately from the SQL.
        assert_eq!(t("TRUNCATE orders"), Some(("shop".into(), "orders".into())));
        assert_eq!(
            t("TRUNCATE TABLE orders"),
            Some(("shop".into(), "orders".into())),
            "the optional TABLE keyword is not part of the name"
        );
        assert_eq!(
            t("truncate table Orders;"),
            Some(("shop".into(), "Orders".into())),
            "the keyword is case-insensitive and the trailing semicolon is not a token; \
             the NAME's case is preserved because MySQL table names can be \
             case-sensitive depending on lower_case_table_names"
        );

        // Qualified and quoted — a db-qualified truncate names another schema, and
        // routing must see that schema, not the event's.
        assert_eq!(
            t("TRUNCATE archive.orders"),
            Some(("archive".into(), "orders".into())),
            "an explicit database must win over the event's — routing it to `shop` \
             would refuse the wrong export, or none"
        );
        assert_eq!(
            t("TRUNCATE TABLE `archive`.`odd name`"),
            Some(("archive".into(), "odd name".into())),
            "backticks are quoting, not part of the identifier"
        );

        // Comments: measured NOT to reach this reader on 8.0, handled anyway because
        // the failure mode is a silent divergence rather than an error.
        assert_eq!(
            t("TRUNCATE TABLE orders /* xid=42 */"),
            Some(("shop".into(), "orders".into()))
        );
        assert_eq!(
            t("/* app='api' */ TRUNCATE TABLE orders"),
            Some(("shop".into(), "orders".into()))
        );

        // NOT a truncate, and the boundary matters: claiming one of these would fail
        // an export over a statement that removes nothing.
        for not_one in [
            "DELETE FROM orders",
            "DROP TABLE orders",
            "TRUNCATE",
            "TRUNCATE TABLE",
            "SELECT 'TRUNCATE orders'",
            "CREATE TABLE truncate_log(id int)",
        ] {
            assert_eq!(t(not_one), None, "{not_one} is not a truncate of a table");
        }

        // A shape this reader does not claim to understand must be None rather than a
        // guess — a wrong guess refuses the wrong table.
        assert_eq!(
            t("TRUNCATE TABLE a, b"),
            None,
            "MySQL does not accept a list here, and inventing an answer for an \
             unrecognised shape is how a reader refuses an export it should not"
        );

        // A DOT inside backticks is part of the name, not a qualifier. The old
        // `split_once('.')` over the raw text cut this into a database and a table
        // that do not exist, so routing found neither and the truncate was not
        // refused for the table it actually names.
        assert_eq!(
            t("TRUNCATE TABLE `odd.name`"),
            Some(("shop".into(), "odd.name".into())),
            "a quoted dot is a character in the identifier"
        );
        assert_eq!(
            t("TRUNCATE TABLE `db.x`.`t.y`"),
            Some(("db.x".into(), "t.y".into())),
            "...and the qualifier splits on the dot BETWEEN the quoted parts"
        );
        // A doubled backtick is an escaped one inside the name.
        assert_eq!(
            t("TRUNCATE TABLE `we``ird`"),
            Some(("shop".into(), "we`ird".into()))
        );
        // An unterminated quote is not a name.
        assert_eq!(t("TRUNCATE TABLE `unclosed"), None);
    }

    /// Any `binlog_row_metadata` but FULL (or none at all) is refused, and the text names the fix and the order.
    #[test]
    fn row_metadata_other_than_full_is_refused_with_the_sql_fix_and_the_recovery_order() {
        assert_eq!(MysqlChangeStream::row_metadata_refusal(Some("FULL")), None);
        assert_eq!(MysqlChangeStream::row_metadata_refusal(Some("full")), None);
        for m in [Some("MINIMAL"), Some("minimal"), Some("NO_LOG"), None] {
            let why = MysqlChangeStream::row_metadata_refusal(m)
                .unwrap_or_else(|| panic!("{m:?} carries no column names and must be refused"));
            assert!(why.contains("binlog_row_metadata"), "{why}");
            assert!(
                why.contains("`SET PERSIST binlog_row_metadata = FULL;`"),
                "{why}"
            );
            assert!(why.contains("by POSITION"), "{why}");
            assert!(
                why.ends_with(crate::source::cdc::checkpoint_identity::RECOVER),
                "the recovery must anchor FIRST, then re-snapshot: {why}"
            );
        }
        assert!(
            MysqlChangeStream::row_metadata_refusal(Some("MINIMAL"))
                .unwrap()
                .contains("binlog_row_metadata = MINIMAL")
        );
    }

    /// Only ER_UNKNOWN_SYSTEM_VARIABLE (a pre-8.0.1 server) reads as unset; any other error stays an error.
    #[test]
    fn only_an_unknown_variable_error_reads_as_an_unset_row_metadata() {
        let err = |code| {
            Err(mysql::Error::MySqlError(mysql::MySqlError {
                state: "HY000".into(),
                message: "x".into(),
                code,
            }))
        };
        let read = MysqlChangeStream::row_metadata_or_legacy;
        assert_eq!(read(err(1193)).unwrap(), None);
        assert!(
            read(err(1227)).is_err(),
            "a permission error is not an old server"
        );
        assert_eq!(read(Ok(Some("FULL".into()))).unwrap(), Some("FULL".into()));
    }

    /// A nameless rows event is refused as UNDECODABLE, naming the table and the binlog position.
    #[test]
    fn a_nameless_rows_event_refusal_names_table_position_and_code() {
        let err = nameless_rows_refusal("shop", "orders", "binlog.000007", 4321);
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_SOURCE_CDC_UNDECODABLE")
        );
        let text = err.to_string();
        assert!(text.contains("`shop`.`orders`"), "{text}");
        assert!(text.contains("binlog.000007:4321"), "{text}");
        assert!(text.contains("binlog_row_metadata = MINIMAL"), "{text}");
        assert!(
            text.contains("`SET PERSIST binlog_row_metadata = FULL;`"),
            "{text}"
        );
    }

    /// The MySQL routing guard's decision surface.
    ///
    /// `None` (the catalog has no row) must stay ROUTABLE, deliberately: the schema
    /// probe reports a missing table with a message that already names it, and a
    /// second competing error for one cause is worse than none.
    #[test]
    fn a_non_base_relation_is_refused_and_a_base_table_is_not() {
        assert_eq!(
            classify_mysql_routing("rivet.orders", Some("BASE TABLE")),
            MysqlRoutingVerdict::Routable,
            "an ordinary table must stay routable — refusing it would break every config"
        );
        assert_eq!(
            classify_mysql_routing("rivet.orders", Some("base table")),
            MysqlRoutingVerdict::Routable,
            "TABLE_TYPE casing is not something to depend on"
        );
        assert_eq!(
            classify_mysql_routing("rivet.orders", None),
            MysqlRoutingVerdict::Routable,
            "an absent catalog row is the schema probe's error to report, not this \
             guard's — two errors for one cause is worse than one"
        );

        let MysqlRoutingVerdict::Never(why) =
            classify_mysql_routing("rivet.orders_v", Some("VIEW"))
        else {
            panic!("a VIEW can never be routed — the binlog names its BASE table");
        };
        assert!(
            why.contains("rivet.orders_v") && why.contains("VIEW"),
            "the refusal must name the relation and what it is: {why}"
        );
        assert!(
            why.contains("base table(s)"),
            "it must say what to capture instead: {why}"
        );
        assert!(
            why.contains("NOT lost"),
            "MySQL binlog retention is reader-independent, so this IS recoverable by \
             deleting the checkpoint — saying so is the difference between a config \
             fix and a re-snapshot the operator does not need: {why}"
        );
        // SYSTEM VIEW is the other type information_schema reports; it must refuse
        // for the same reason rather than fall through to Routable.
        assert!(matches!(
            classify_mysql_routing("x", Some("SYSTEM VIEW")),
            MysqlRoutingVerdict::Never(_)
        ));
    }

    /// The refusal message IS the remediation — an operator has nothing else to go
    /// on, because the alternative to this error was a green run with missing rows.
    /// The refusal must be TABLE-ADDRESSED. Every arm here was a live failure
    /// first: the un-filtered refusal failed a concurrent, unrelated MySQL CDC
    /// export on a table it does not capture (measured 2026-08-24 on #281 — the
    /// PARTIAL_JSON fixture and `mysql_cdc_cli_stream_with_a_cap…` in one run,
    /// which is how the over-refusal was found at all).
    #[test]
    fn an_undecodable_event_fails_only_the_runs_that_capture_that_table() {
        let cfg = vec!["rivet.orders".to_string()];

        assert!(
            undecodable_event_is_ours(Some(("rivet", "orders")), &cfg, false),
            "our own table must refuse — skipping loses the change while the \
             transaction commits and the checkpoint advances past it"
        );
        assert!(
            !undecodable_event_is_ours(Some(("rivet", "audit")), &cfg, false),
            "another table must NOT fail this run: the routing filter would have \
             dropped it anyway, so refusing turns one PARTIAL_JSON table into a \
             server-wide outage"
        );
        assert!(
            undecodable_event_is_ours(None, &cfg, false),
            "an UNRESOLVED table must refuse — guessing 'not mine' with no TABLE_MAP \
             is the silent drop this path exists to prevent"
        );
        assert!(
            undecodable_event_is_ours(Some(("rivet", "audit")), &[], false),
            "`rivet cdc` with no --table captures whatever the stream emits, so \
             every event is ours"
        );
        // A bare (schema-less) config entry matches any schema, exactly as the sink
        // routes it — the guard must ask the SAME question the router does, or it
        // protects a set the run does not actually capture.
        assert!(
            undecodable_event_is_ours(Some(("otherdb", "orders")), &["orders".to_string()], false),
            "a bare config name matches any schema, like sink::table_matches"
        );
    }

    #[test]
    fn an_undecodable_rows_event_refusal_names_the_cause_and_the_way_out() {
        let v1 = undecodable_rows_refusal_message(UndecodableRows::V1);
        assert!(v1.contains("v1 rows event"), "must name the variant: {v1}");
        assert!(v1.contains("MariaDB"), "must name who emits it: {v1}");

        let partial = undecodable_rows_refusal_message(UndecodableRows::PartialJson);
        assert!(
            partial.contains("binlog_row_value_options"),
            "must name the SETTING to change, not just the symptom: {partial}"
        );

        for msg in [&v1, &partial] {
            assert!(
                msg.contains("re-read"),
                "must say the un-acked span is re-read — an operator who believes the data \
                 is already gone has no reason to fix the source and re-run: {msg}"
            );
            assert!(
                msg.contains("checkpoint advances"),
                "must say WHY silence was unacceptable: the surrounding transaction \
                 commits and the checkpoint moves past the dropped rows: {msg}"
            );
        }
    }

    /// `Xid` is written only by a transactional engine's own commit. A MyISAM or
    /// MEMORY write, and every `XA COMMIT`, close the transaction with a QUERY event
    /// instead — and with no arm for those the rows stayed buffered FOREVER: the run
    /// captured zero, exited 0, and the checkpoint never moved. Silent, total, and
    /// indistinguishable from an idle source.
    ///
    /// The negative half is the load-bearing one. `XA PREPARE` must NOT release:
    /// prepared is not committed, and publishing there hands downstream rows that a
    /// later `XA ROLLBACK` erases — a fabrication, which is worse than the loss this
    /// fixes.
    #[test]
    fn a_commit_query_closes_a_transaction_but_a_prepare_does_not() {
        for yes in [
            "COMMIT",
            "commit",
            "  COMMIT ;",
            "XA COMMIT 'x1'",
            "xa commit 0x01",
        ] {
            assert!(
                is_commit_statement(yes),
                "`{yes}` must close the transaction"
            );
        }
        for no in [
            "BEGIN",
            "XA START 'x1'",
            // KEEP, but it grades a string this reader does not receive: on 5.7.7+
            // the prepare is a BINARY `XaPrepareLogEvent`, so the arm that actually
            // stops it is in `fill`, proven live by
            // `a_prepared_xa_branch_is_never_published_by_another_transactions_commit`.
            // Left here for the 5.7.0-5.7.6 spelling and as a second lock.
            "XA PREPARE 'x1'",
            "XA ROLLBACK 'x1'",
            "ROLLBACK",
            "SAVEPOINT s1",
            "COMMIT_LOG_ENTRY",
            "INSERT INTO commit VALUES (1)",
        ] {
            assert!(
                !is_commit_statement(no),
                "`{no}` must NOT release a transaction — releasing an uncommitted or \
                 unrelated statement publishes rows that may never be committed"
            );
        }
    }

    use super::*;
    use mysql::Opts;

    /// The compression guard's decision, offline. Anything unrecognized — an
    /// absent variable (pre-8.0.20, MariaDB), an empty reply, a shape we did
    /// not anticipate — must read as OFF: the guard exists to stop a silent
    /// LOSS, and blocking a healthy source because a reply looked unfamiliar
    /// would be its own outage.
    #[test]
    fn binlog_compression_is_recognized_in_both_renderings_and_fails_open() {
        for on in ["1", "ON", "on", " on ", "TRUE"] {
            assert!(binlog_compression_is_on(Some(on)), "{on:?} means ON");
        }
        for off in ["0", "OFF", "off", "", "NONE", "ZSTD_unexpected"] {
            assert!(
                !binlog_compression_is_on(Some(off)),
                "{off:?} must not block"
            );
        }
        assert!(
            !binlog_compression_is_on(None),
            "no such variable (pre-8.0.20 / MariaDB) is not a compressed source"
        );
    }

    /// The predicate above says what the reply MEANS; this says what the guard
    /// DOES with it, which is the half a live server used to hold hostage. Both
    /// directions, because a guard that refuses everything and a guard that
    /// refuses nothing are the same bug from opposite sides — deleting the `!`
    /// turns one into the other and this is what notices.
    #[test]
    fn compression_refusal_blocks_a_compressed_binlog_and_only_that() {
        let err = compression_refusal(Some("ON")).expect_err("ON must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains("binlog_transaction_compression") && msg.contains("capture NOTHING"),
            "the refusal must name the setting and the harm: {msg}"
        );
        for ok in [Some("OFF"), Some("0"), Some(""), None] {
            assert!(
                compression_refusal(ok).is_ok(),
                "{ok:?} is not a compressed binlog — the run must proceed"
            );
        }
    }

    /// #200-2: the open-time guard is not the whole story — compression turned on
    /// AFTER open (or a compressed span already in the binlog) reaches `fill()` as
    /// a `Transaction_payload_event`, which the reader cannot expand. Its arm must
    /// REFUSE (naming the setting + the harm), never fall through to `_ => {}` and
    /// silently skip the transaction while the checkpoint advances past it. Pure
    /// message check here; the routing (arm calls this, not `_ => {}`) is proven
    /// live by `mysql_cdc_compressed_payload_in_stream_refuses_not_skips`.
    #[test]
    fn compressed_payload_refusal_names_the_setting_and_the_harm() {
        let msg = compressed_payload_refusal().to_string();
        assert!(
            msg.contains("Transaction_payload_event")
                && msg.contains("binlog_transaction_compression")
                && msg.contains("capture NOTHING"),
            "the stream-time refusal must name the event, the setting, and the harm: {msg}"
        );
    }

    // The `mysql-cdc` instance (cdc profile, :3307) — binlog + a REPLICATION grant.
    const URL: &str = "mysql://rivet:rivet@127.0.0.1:3307/rivet";

    // The until_current termination contract, as a pure matrix: at the bound is
    // in scope, past it (by pos or by a later file) is not, the file compare is
    // ordinal (survives the 999999 → 1000000 suffix-width rollover where a
    // lexicographic compare inverts), and unparseable names fail open.
    #[test]
    fn commit_past_bound_matrix() {
        let bound = ("binlog.000042".to_string(), 1000u64);
        // Daemon (no bound): never past.
        assert!(!commit_past_bound("binlog.000099", u64::MAX, None));
        // Same file: at the bound is in scope, one byte past is not.
        assert!(!commit_past_bound("binlog.000042", 999, Some(&bound)));
        assert!(!commit_past_bound("binlog.000042", 1000, Some(&bound)));
        assert!(commit_past_bound("binlog.000042", 1001, Some(&bound)));
        // Earlier / later file: pos is irrelevant.
        assert!(!commit_past_bound("binlog.000041", u64::MAX, Some(&bound)));
        assert!(commit_past_bound("binlog.000043", 4, Some(&bound)));
        // Suffix-width rollover: 1000000 > 999999 ordinally, though it is
        // lexicographically SMALLER ("1…" < "9…").
        let wide = ("mysql-bin.999999".to_string(), 1000u64);
        assert!(commit_past_bound("mysql-bin.1000000", 4, Some(&wide)));
        assert!(!commit_past_bound("mysql-bin.999999", 500, Some(&wide)));
        // Unparseable names fail open (never end the run early).
        assert!(!commit_past_bound("garbage", 99, Some(&bound)));
        assert!(!commit_past_bound(
            "binlog.000042",
            99,
            Some(&("garbage".to_string(), 0))
        ));
    }

    #[test]
    fn binlog_file_ordinal_parses_the_numeric_suffix() {
        assert_eq!(binlog_file_ordinal("mysql-bin.000042"), Some(42));
        assert_eq!(binlog_file_ordinal("binlog.1000000"), Some(1000000));
        // Dotted basenames take the LAST dot (rsplit).
        assert_eq!(binlog_file_ordinal("my.replica.000007"), Some(7));
        assert_eq!(binlog_file_ordinal("no-suffix"), None);
        assert_eq!(binlog_file_ordinal("binlog.notanum"), None);
    }

    /// The dump thread must outlive a long store flush: its session `net_write_timeout` is
    /// rivet's, not the server's 60 s default that dropped a real run mid-flush.
    #[test]
    #[ignore = "live: requires docker compose mysql (binlog_format=ROW)"]
    fn the_binlog_dump_session_outlives_a_long_flush() {
        // Its own server_id: a second dump with the same id makes MySQL drop the first (1236).
        let _stream =
            MysqlChangeStream::open_from_current(URL, 4250, DrainMode::Continuous, None, &[])
                .unwrap();
        // performance_schema is root-only on the stand; the stream itself runs as `rivet`.
        let mut c =
            Conn::new(Opts::from_url("mysql://root:rivet@127.0.0.1:3307/rivet").unwrap()).unwrap();
        let seen: Option<u32> = c
            .query_first(
                "SELECT CAST(v.VARIABLE_VALUE AS UNSIGNED) \
                 FROM performance_schema.threads t \
                 JOIN performance_schema.variables_by_thread v USING (THREAD_ID) \
                 WHERE t.PROCESSLIST_COMMAND LIKE 'Binlog Dump%' \
                   AND v.VARIABLE_NAME = 'net_write_timeout' \
                 ORDER BY t.THREAD_ID DESC LIMIT 1",
            )
            .unwrap();
        assert_eq!(
            seen,
            Some(DUMP_WRITE_TIMEOUT_SECS),
            "the dump thread runs on the server default, so a flush longer than it kills the stream"
        );
    }

    #[test]
    #[ignore = "live: requires docker compose mysql (binlog_format=ROW)"]
    fn streams_typed_insert_update_delete() {
        let mut c = Conn::new(Opts::from_url(URL).unwrap()).unwrap();
        c.query_drop("DROP TABLE IF EXISTS cdc_unit").unwrap();
        c.query_drop("CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT)")
            .unwrap();

        let mut stream = MysqlChangeStream::open_from_current(
            URL,
            4243,
            DrainMode::Continuous,
            None,
            &["cdc_unit"],
        )
        .unwrap();
        c.query_drop("INSERT INTO cdc_unit VALUES (1, 10)").unwrap();
        c.query_drop("UPDATE cdc_unit SET v = 20 WHERE id = 1")
            .unwrap();
        c.query_drop("DELETE FROM cdc_unit WHERE id = 1").unwrap();

        let mut events = Vec::new();
        for ev in stream.by_ref() {
            let ev = ev.unwrap();
            if ev.table == "cdc_unit" {
                events.push(ev);
            }
            if events.len() >= 3 {
                break;
            }
        }

        let ops: Vec<ChangeOp> = events.iter().map(|e| e.op).collect();
        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete]
        );
        // Typed values: the INSERT after-image is [1, 10] as JSON numbers.
        assert_eq!(
            events[0].after.as_deref(),
            Some([RivetValue::Int(1), RivetValue::Int(10)].as_slice())
        );
        assert_eq!(events[1].before.as_ref().unwrap()[1], RivetValue::Int(10));
        assert_eq!(events[1].after.as_ref().unwrap()[1], RivetValue::Int(20));
        // Every event carries a canonical position for checkpointing.
        assert!(
            events[2]
                .position
                .0
                .get("pos")
                .and_then(Json::as_u64)
                .unwrap()
                > 0
        );
    }

    // RED test for the idle-first-run hole: PostgreSQL pins its resume anchor
    // server-side at open (slot creation); MySQL has no server-side anchor — the
    // checkpoint file is the ONLY resume position, and it was written only at a
    // part commit. So a bounded first run that drains ZERO changes left no
    // checkpoint, the next run re-anchored to a newer "current" position, and
    // every change in between was silently skipped. Enable-CDC-during-a-quiet-
    // period is exactly the realistic ops sequence that hits this.
    #[test]
    #[ignore = "live: requires docker compose mysql (binlog_format=ROW)"]
    fn first_run_with_zero_changes_pins_the_checkpoint_at_open() {
        let mut c = Conn::new(Opts::from_url(URL).unwrap()).unwrap();
        c.query_drop("DROP TABLE IF EXISTS cdc_idle_first").unwrap();
        c.query_drop("CREATE TABLE cdc_idle_first (id INT PRIMARY KEY, v INT)")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("mysql.ckpt.json");

        // Run 1: checkpoint path configured, no file yet, nothing to drain
        // (BoundedAtOpen ⇒ bounded drain-and-exit, the scheduler model).
        let mut s = MysqlChangeStream::open_or_resume(
            URL,
            4245,
            Some(&ckpt),
            DrainMode::BoundedAtOpen,
            None,
            vec!["cdc_idle_first".to_string()], // only its own table: a sibling test's DROP must not reach it
            None,
            None,
        )
        .unwrap();
        assert!(
            s.by_ref()
                .map(|e| e.unwrap())
                .all(|e| e.table != "cdc_idle_first"),
            "run 1 must drain zero changes for this table"
        );
        drop(s);
        assert!(
            Position::load(&ckpt).unwrap().is_some(),
            "a first run with no prior checkpoint must pin the open position, \
             even when it captures nothing"
        );

        // A change made BETWEEN the idle run 1 and run 2.
        c.query_drop("INSERT INTO cdc_idle_first VALUES (1, 100)")
            .unwrap();

        // Run 2 must resume from the pinned position and capture it.
        let mut s2 = MysqlChangeStream::open_or_resume(
            URL,
            4245,
            Some(&ckpt),
            DrainMode::BoundedAtOpen,
            None,
            vec!["cdc_idle_first".to_string()], // only its own table: a sibling test's DROP must not reach it
            None,
            None,
        )
        .unwrap();
        let got = s2
            .by_ref()
            .map(|e| e.unwrap())
            .find(|e| e.table == "cdc_idle_first");
        assert!(
            got.is_some(),
            "the change between an idle run and the next run must be captured, not skipped"
        );
    }

    #[test]
    #[ignore = "live: requires docker compose mysql (binlog_format=ROW)"]
    fn resumes_from_checkpoint() {
        // Distinct table + server_id so this runs safely in parallel with the
        // other live test (cargo runs tests concurrently).
        let mut c = Conn::new(Opts::from_url(URL).unwrap()).unwrap();
        c.query_drop("DROP TABLE IF EXISTS cdc_resume").unwrap();
        c.query_drop("CREATE TABLE cdc_resume (id INT PRIMARY KEY, v INT)")
            .unwrap();

        // Read change A and checkpoint at its position.
        let mut s = MysqlChangeStream::open_from_current(
            URL,
            4244,
            DrainMode::Continuous,
            None,
            &["cdc_resume"],
        )
        .unwrap();
        c.query_drop("INSERT INTO cdc_resume VALUES (1, 100)")
            .unwrap();
        let a = s
            .by_ref()
            .map(|e| e.unwrap())
            .find(|e| e.table == "cdc_resume")
            .unwrap();
        assert_eq!(a.after.as_ref().unwrap()[0], RivetValue::Int(1));

        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("mysql.ckpt.json");
        a.position.save(&ckpt).unwrap();
        assert_eq!(Position::load(&ckpt).unwrap().as_ref(), Some(&a.position));
        drop(s);

        // A change made AFTER the checkpoint.
        c.query_drop("INSERT INTO cdc_resume VALUES (2, 200)")
            .unwrap();

        // Resuming from the checkpoint must yield B (id=2), never re-read A.
        let mut s2 = MysqlChangeStream::open_or_resume(
            URL,
            4244,
            Some(&ckpt),
            DrainMode::Continuous,
            None,
            vec!["cdc_resume".to_string()],
            None,
            None,
        )
        .unwrap();
        let b = s2
            .by_ref()
            .map(|e| e.unwrap())
            .find(|e| e.table == "cdc_resume")
            .unwrap();
        assert_eq!(b.op, ChangeOp::Insert);
        assert_eq!(
            b.after.as_ref().unwrap()[0],
            RivetValue::Int(2),
            "resumed stream must start after the checkpoint, not re-read A"
        );
    }

    /// #161: both directions of the pure row-image verdict, RED against an
    /// inverted/`Ok(())`-style mutant (the compression_refusal precedent).
    #[test]
    fn row_image_verdict_both_directions() {
        use crate::source::cdc::RowImage;
        // keep: FULL (any case) and ABSENT (older MySQL / MariaDB).
        assert!(matches!(
            MysqlChangeStream::row_image_verdict(Some("FULL")),
            RowImage::Whole
        ));
        assert!(matches!(
            MysqlChangeStream::row_image_verdict(Some("full")),
            RowImage::Whole
        ));
        assert!(matches!(
            MysqlChangeStream::row_image_verdict(None),
            RowImage::Whole
        ));
        // refuse: MINIMAL / NOBLOB write partial rows.
        for bad in ["MINIMAL", "noblob"] {
            match MysqlChangeStream::row_image_verdict(Some(bad)) {
                RowImage::Partial { why } => assert!(why.contains(bad), "{why}"),
                other => panic!("{bad} must refuse, got {other:?}"),
            }
        }
    }

    fn only(t: &str) -> Vec<String> {
        vec![t.to_string()]
    }

    fn folded_verdict_of(sql: &str, db: &str, cfg: &[String], fold: bool) -> QueryVerdict {
        query_verdict(
            sql,
            db,
            SqlMode::default(),
            &QueryScope {
                projecting: true,
                fold,
                configured: cfg,
            },
        )
    }

    fn pair(d: &str, t: &str) -> Option<(String, String)> {
        Some((d.to_string(), t.to_string()))
    }

    #[test]
    fn a_table_rename_is_read_from_the_plural_and_the_equals_spellings() {
        let t = |sql: &str| renamed_tables(sql, "shop", SqlMode::default());
        assert_eq!(
            t("RENAME TABLES orders TO orders_old"),
            Some(vec![pair("shop", "orders"), pair("shop", "orders_old")])
        );
        assert_eq!(
            t("rename tables a TO b, c.d to e.f"),
            Some(vec![
                pair("shop", "a"),
                pair("shop", "b"),
                pair("c", "d"),
                pair("e", "f")
            ])
        );
        assert_eq!(
            t("ALTER TABLE orders RENAME = orders_old"),
            Some(vec![pair("shop", "orders"), pair("shop", "orders_old")])
        );
        assert_eq!(
            t("ALTER TABLE orders ADD COLUMN k INT, RENAME = archive.orders"),
            Some(vec![pair("shop", "orders"), pair("archive", "orders")])
        );
        for sql in [
            "RENAME TABLESPACE a TO b",
            "RENAME USERS a TO b",
            "ALTER TABLE orders RENAME COLUMN a TO b",
            "ALTER TABLE orders RENAME INDEX a TO b",
            "ALTER TABLE orders RENAME KEY a TO b",
            "ALTER TABLE orders COMMENT = 'x', ADD COLUMN k INT",
        ] {
            assert_eq!(t(sql), None, "{sql}");
        }
    }

    #[test]
    fn a_drop_is_read_from_the_plural_spelling() {
        let t = |sql: &str| drop_table_targets(sql, "shop", SqlMode::default());
        assert_eq!(
            t("DROP TABLES IF EXISTS a, b.c"),
            Some(vec![pair("shop", "a"), pair("b", "c")])
        );
        assert_eq!(t("drop tables orders"), Some(vec![pair("shop", "orders")]));
        assert_eq!(t("DROP TABLE orders"), Some(vec![pair("shop", "orders")]));
        for sql in [
            "DROP TABLESPACE ts",
            "DROP VIEW v",
            "DROP DATABASE shop",
            "",
        ] {
            assert_eq!(t(sql), None, "{sql}");
        }
    }

    #[test]
    fn a_name_in_another_case_is_ours_exactly_when_the_server_folds_names() {
        let cfg = only("shop.orders");
        let ours = |resolved: Option<(&str, &str)>, cfg: &[String], fold: bool| {
            undecodable_event_is_ours(resolved, cfg, fold)
        };
        assert!(ours(Some(("shop", "orders")), &cfg, false));
        assert!(ours(Some(("shop", "orders")), &cfg, true));
        assert!(!ours(Some(("SHOP", "orders")), &cfg, false));
        assert!(!ours(Some(("shop", "ORDERS")), &cfg, false));
        assert!(ours(Some(("SHOP", "orders")), &cfg, true));
        assert!(ours(Some(("shop", "ORDERS")), &cfg, true));
        assert!(ours(Some(("shop", "orders")), &only("SHOP.Orders"), true));
        assert!(!ours(Some(("shop", "orders")), &only("SHOP.Orders"), false));
        for fold in [false, true] {
            assert!(!ours(Some(("shop", "other")), &cfg, fold), "{fold}");
            assert!(!ours(Some(("other", "orders")), &cfg, fold), "{fold}");
            assert!(ours(None, &cfg, fold), "{fold}");
            assert!(ours(Some(("x", "y")), &[], fold), "{fold}");
        }
    }

    #[test]
    fn the_server_value_of_lower_case_table_names_decides_the_folding() {
        assert!(!fold_names_from(Some("0")));
        assert!(!fold_names_from(Some(" 0\n")));
        assert!(fold_names_from(Some("1")));
        assert!(fold_names_from(Some("2")));
        assert!(fold_names_from(Some("")));
        assert!(fold_names_from(None));
    }

    #[test]
    fn every_statement_the_run_judges_follows_the_folding_of_the_server() {
        let cfg = only("shop.orders");
        let upper = [
            "TRUNCATE SHOP.ORDERS",
            "ALTER TABLE SHOP.ORDERS RENAME COLUMN a TO b",
            "RENAME TABLE SHOP.ORDERS TO SHOP.OLD",
            "DROP TABLE SHOP.ORDERS",
            "UPDATE SHOP.ORDERS SET a = 1",
        ];
        let on = [
            QueryVerdict::Truncated("SHOP".into(), "ORDERS".into()),
            QueryVerdict::ColumnRenamed("SHOP".into(), "ORDERS".into()),
            QueryVerdict::TableRenamed(pair("SHOP", "ORDERS")),
            QueryVerdict::Dropped(pair("SHOP", "ORDERS")),
            QueryVerdict::StatementWrite(pair("SHOP", "ORDERS")),
        ];
        for (sql, want) in upper.iter().zip(on) {
            assert_eq!(folded_verdict_of(sql, "shop", &cfg, true), want, "{sql}");
            assert_eq!(
                folded_verdict_of(sql, "shop", &cfg, false),
                QueryVerdict::Pass,
                "{sql}"
            );
        }
        assert_eq!(
            folded_verdict_of("TRUNCATE ORDERS", "SHOP", &cfg, true),
            QueryVerdict::Truncated("SHOP".into(), "ORDERS".into())
        );
        assert_eq!(
            folded_verdict_of("TRUNCATE ORDERS", "SHOP", &cfg, false),
            QueryVerdict::Pass
        );
    }

    #[test]
    fn a_version_comment_takes_the_digits_the_server_takes_through_every_verdict() {
        let cfg = only("shop.orders");
        let cases = [
            (
                "/*!080400 ALTER TABLE orders RENAME COLUMN a TO b */",
                QueryVerdict::ColumnRenamed("shop".into(), "orders".into()),
            ),
            (
                "/*!80400 ALTER TABLE orders RENAME COLUMN a TO b */",
                QueryVerdict::ColumnRenamed("shop".into(), "orders".into()),
            ),
            (
                "/*!080400 RENAME TABLE orders TO orders_old */",
                QueryVerdict::TableRenamed(pair("shop", "orders")),
            ),
            (
                "/*!080400\nRENAME TABLE orders TO orders_old */",
                QueryVerdict::TableRenamed(pair("shop", "orders")),
            ),
            (
                "/*!080400 REVOKE SELECT (a) ON shop.orders FROM u */",
                QueryVerdict::Privilege,
            ),
            (
                "/*!80400 REVOKE SELECT (a) ON shop.orders FROM u */",
                QueryVerdict::Privilege,
            ),
            (
                "/*!080400 TRUNCATE orders */",
                QueryVerdict::Truncated("shop".into(), "orders".into()),
            ),
            (
                "/*!080400 DROP TABLE orders */",
                QueryVerdict::Dropped(pair("shop", "orders")),
            ),
            (
                "/*!080400 UPDATE orders SET a = 1 */",
                QueryVerdict::StatementWrite(pair("shop", "orders")),
            ),
            (
                "/*!080400 ALTER TABLE orders ADD COLUMN k INT */",
                QueryVerdict::Pass,
            ),
        ];
        for (sql, want) in cases {
            assert_eq!(verdict_of(sql, "shop", true, &cfg), want, "{sql}");
        }
    }

    #[test]
    fn each_statement_gets_its_verdict_and_only_its_verdict() {
        let cfg = only("shop.orders");
        let t = |s: &str| pair("shop", s);
        let cases: Vec<(&str, bool, &[String], QueryVerdict)> = vec![
            (
                "TRUNCATE orders",
                false,
                &cfg,
                QueryVerdict::Truncated("shop".into(), "orders".into()),
            ),
            ("TRUNCATE other", true, &cfg, QueryVerdict::Pass),
            (
                "ALTER TABLE orders RENAME COLUMN a TO b",
                true,
                &cfg,
                QueryVerdict::ColumnRenamed("shop".into(), "orders".into()),
            ),
            (
                "ALTER TABLE orders RENAME COLUMN a TO b",
                false,
                &cfg,
                QueryVerdict::Pass,
            ),
            (
                "RENAME TABLE orders TO old",
                true,
                &cfg,
                QueryVerdict::TableRenamed(t("orders")),
            ),
            (
                "RENAME TABLE orders TO old",
                false,
                &cfg,
                QueryVerdict::Pass,
            ),
            (
                "RENAME TABLE x TO",
                true,
                &cfg,
                QueryVerdict::TableRenamed(None),
            ),
            (
                "DROP TABLE orders",
                false,
                &cfg,
                QueryVerdict::Dropped(t("orders")),
            ),
            ("DROP TABLE other", false, &cfg, QueryVerdict::Pass),
            (
                "DROP TABLE other, orders",
                false,
                &cfg,
                QueryVerdict::Dropped(t("orders")),
            ),
            ("DROP TABLE", false, &cfg, QueryVerdict::Dropped(None)),
            (
                "UPDATE orders SET a = 1",
                false,
                &cfg,
                QueryVerdict::StatementWrite(t("orders")),
            ),
            ("UPDATE other SET a = 1", false, &cfg, QueryVerdict::Pass),
            ("UPDATE (", false, &cfg, QueryVerdict::StatementWrite(None)),
            (
                "REVOKE SELECT (a) ON shop.orders FROM u",
                false,
                &cfg,
                QueryVerdict::Privilege,
            ),
            ("COMMIT", false, &cfg, QueryVerdict::Commit),
            ("XA COMMIT 'x'", false, &cfg, QueryVerdict::Commit),
            ("BEGIN", false, &cfg, QueryVerdict::Pass),
            ("XA PREPARE 'x'", false, &cfg, QueryVerdict::Pass),
            ("SET NAMES gbk", false, &cfg, QueryVerdict::Pass),
            ("", false, &cfg, QueryVerdict::Pass),
        ];
        for (sql, projecting, cfg, want) in cases {
            assert_eq!(verdict_of(sql, "shop", projecting, cfg), want, "{sql}");
        }
    }

    fn charset_var(id: u16) -> Vec<u8> {
        let mut v = vec![4u8];
        for _ in 0..3 {
            v.extend_from_slice(&id.to_le_bytes());
        }
        v
    }

    #[test]
    fn the_client_character_set_is_read_from_the_status_variables_of_the_event() {
        assert_eq!(charset_of(&charset_var(28)), Some(28));
        assert_eq!(charset_of(&charset_var(255)), Some(255));
        let among = [sql_mode_var(0x4), charset_var(13)].concat();
        assert_eq!(charset_of(&among), Some(13));
        assert_eq!(charset_of(&sql_mode_var(0x4)), None);
        assert_eq!(charset_of(&[]), None);
        assert_eq!(charset_of(&charset_var(28)[..4]), None);
    }

    fn verdict_in(raw: &[u8], charset: u16, cfg: &[String]) -> QueryVerdict {
        let (sql, mode) = statement_of(raw, &charset_var(charset));
        query_verdict(
            &sql,
            "shop",
            mode,
            &QueryScope {
                projecting: true,
                fold: false,
                configured: cfg,
            },
        )
    }

    #[test]
    fn a_byte_the_reader_cannot_place_stops_the_run_only_when_blank_it_would_be_a_refusal() {
        let cfg = only("shop.orders");
        let unreadable = |v| QueryVerdict::Unreadable(Box::new(v));
        let cases = [
            (
                &b"TRUNCATE\xE9orders"[..],
                unreadable(QueryVerdict::Truncated("shop".into(), "orders".into())),
            ),
            (
                &b"RENAME\xE9TABLE orders TO o2"[..],
                unreadable(QueryVerdict::TableRenamed(pair("shop", "orders"))),
            ),
            (
                &b"DROP\xE9TABLE orders"[..],
                unreadable(QueryVerdict::Dropped(pair("shop", "orders"))),
            ),
            (
                &b"UPDATE\xE9orders SET a = 1"[..],
                unreadable(QueryVerdict::StatementWrite(pair("shop", "orders"))),
            ),
            (
                &b"ALTER\xE9TABLE orders RENAME COLUMN a TO b"[..],
                unreadable(QueryVerdict::ColumnRenamed("shop".into(), "orders".into())),
            ),
            (
                &b"GRANT\xE9SELECT ON shop.orders TO u"[..],
                QueryVerdict::Privilege,
            ),
            (
                &b"GRANT SELECT ON shop.orders\xE9 TO u"[..],
                QueryVerdict::Privilege,
            ),
            (&b"TRUNCATE\xE9other"[..], QueryVerdict::Pass),
            (
                &b"INSERT INTO other VALUES ('\xE9')"[..],
                QueryVerdict::Pass,
            ),
            (
                &b"TRUNCATE orders /* \xE9 */"[..],
                QueryVerdict::Truncated("shop".into(), "orders".into()),
            ),
            (
                &b"DROP TABLE orders\xE9"[..],
                unreadable(QueryVerdict::Dropped(pair("shop", "orders"))),
            ),
        ];
        for (raw, want) in cases {
            assert_eq!(
                verdict_in(raw, 60000, &cfg),
                want,
                "{}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    #[test]
    fn no_byte_above_ascii_in_a_character_set_without_pairs_hides_a_refused_statement() {
        let cfg = only("shop.t");
        let ids: Vec<u16> = crate::source::mysql::client_charsets::COLLATION_CHARSET
            .iter()
            .filter(|(_, name)| {
                !matches!(
                    *name,
                    "utf8mb3"
                        | "utf8mb4"
                        | "binary"
                        | "ascii"
                        | "big5"
                        | "gbk"
                        | "sjis"
                        | "cp932"
                        | "gb18030"
                )
            })
            .map(|(id, _)| *id)
            .collect();
        assert!(ids.len() > 100, "{}", ids.len());
        let plain: [(&[u8], &[u8], bool); 4] = [
            (b"TRUNCATE", b"t", false),
            (b"RENAME", b"TABLE t TO u", false),
            (b"ALTER TABLE t RENAME", b"COLUMN a TO b", false),
            (b"REVOKE", b"SELECT ON t FROM u", true),
        ];
        let mut missed = Vec::new();
        for id in ids {
            for x in 0x80u8..=0xFF {
                for (head, tail, privilege) in plain {
                    let direct = [head, &[x], tail].concat();
                    let statement = [head, b" ", tail].concat();
                    let commented = [&b"--"[..], &[x], b"\n", &statement].concat();
                    for raw in [direct, commented] {
                        let got = verdict_in(&raw, id, &cfg);
                        let refused = if privilege {
                            got == QueryVerdict::Privilege
                        } else {
                            !matches!(
                                got,
                                QueryVerdict::Pass | QueryVerdict::Commit | QueryVerdict::Privilege
                            )
                        };
                        if !refused {
                            missed.push(format!("{id} {}", String::from_utf8_lossy(&raw)));
                        }
                    }
                }
            }
        }
        assert_eq!(
            missed.len(),
            0,
            "passed: {:?}",
            &missed[..missed.len().min(8)]
        );
    }

    #[test]
    fn a_control_byte_never_hides_a_refused_statement() {
        let cfg = only("shop.t");
        let mut missed = Vec::new();
        for id in [8u16, 255, 63] {
            for x in (0u8..0x20).filter(|x| !matches!(x, 9..=13)) {
                for (raw, privilege) in [
                    ([&b"TRUNCATE"[..], &[x], b"t"].concat(), false),
                    ([&b"RENAME"[..], &[x], b"TABLE t TO u"].concat(), false),
                    ([&b"REVOKE"[..], &[x], b"SELECT ON t FROM u"].concat(), true),
                    ([&b"--"[..], &[x], b"\nTRUNCATE t"].concat(), false),
                ] {
                    let got = verdict_in(&raw, id, &cfg);
                    let refused = if privilege {
                        got == QueryVerdict::Privilege
                    } else {
                        !matches!(got, QueryVerdict::Pass | QueryVerdict::Commit)
                    };
                    if !refused {
                        missed.push(format!("{id} {x:#x} {}", String::from_utf8_lossy(&raw)));
                    }
                }
            }
        }
        assert_eq!(
            missed.len(),
            0,
            "passed: {:?}",
            &missed[..missed.len().min(8)]
        );
    }

    #[test]
    fn the_shape_of_a_name_is_folded_with_the_server_and_a_qualified_entry_keeps_its_schema() {
        let (sql, _) = statement_of(b"TRUNCATE CAF\xE9", &charset_var(8));
        let verdict = |cfg: &[String], fold| folded_verdict_of(&sql, "shop", cfg, fold);
        let truncated = QueryVerdict::Truncated("shop".into(), "CAF\u{FFFD}".into());
        assert_eq!(verdict(&only("shop.caf\u{e9}"), true), truncated);
        assert_eq!(verdict(&only("shop.caf\u{e9}"), false), QueryVerdict::Pass);
        assert_eq!(verdict(&only("caf\u{e9}"), true), truncated);
        assert_eq!(verdict(&only("other.caf\u{e9}"), true), QueryVerdict::Pass);
        assert_eq!(
            verdict(&only("shop.caf"), true),
            QueryVerdict::Unreadable(Box::new(QueryVerdict::Truncated(
                "shop".into(),
                "CAF".into()
            )))
        );
    }

    #[test]
    fn a_name_the_reader_placed_is_compared_exactly_not_by_shape() {
        let caf = QueryVerdict::Truncated("shop".into(), "caf\u{e9}".into());
        assert_eq!(
            verdict_in(b"TRUNCATE caf\xC3\xA9", 255, &only("shop.caf\u{e9}")),
            caf
        );
        assert_eq!(
            verdict_in(b"TRUNCATE caf\xC3\xA9", 255, &only("shop.caf\u{e8}")),
            QueryVerdict::Pass
        );
    }

    #[test]
    fn a_pair_that_decodes_to_a_blank_stops_a_run_that_captures_the_table_behind_it() {
        let unreadable =
            QueryVerdict::Unreadable(Box::new(QueryVerdict::Truncated("shop".into(), "t".into())));
        assert_eq!(
            verdict_in(b"TRUNCATE\x81\x40t", 13, &only("shop.t")),
            unreadable
        );
    }

    #[test]
    fn a_name_with_a_byte_the_reader_cannot_place_is_ours_only_when_a_captured_name_has_its_shape()
    {
        let unreadable = |v| QueryVerdict::Unreadable(Box::new(v));
        let accented = only("shop.caf\u{e9}");
        for (raw, cfg, want) in [
            (
                &b"TRUNCATE caf\xE9"[..],
                &accented,
                QueryVerdict::Truncated("shop".into(), "caf\u{FFFD}".into()),
            ),
            (
                &b"TRUNCATE caf\xC3\xA9"[..],
                &accented,
                QueryVerdict::Truncated("shop".into(), "caf\u{FFFD}\u{FFFD}".into()),
            ),
            (&b"TRUNCATE cafe\xE9"[..], &accented, QueryVerdict::Pass),
            (&b"TRUNCATE other\xE9"[..], &accented, QueryVerdict::Pass),
            (
                &b"TRUNCATE orders\xE9"[..],
                &only("shop.orders"),
                unreadable(QueryVerdict::Truncated("shop".into(), "orders".into())),
            ),
            (
                &b"TRUNCATE orders_d\xE9"[..],
                &only("shop.orders"),
                QueryVerdict::Pass,
            ),
            (
                &b"DROP TABLE orders_d\xE9"[..],
                &only("shop.orders"),
                QueryVerdict::Pass,
            ),
            (
                &b"TRUNCATE any\xE9"[..],
                &Vec::<String>::new(),
                QueryVerdict::Truncated("shop".into(), "any\u{FFFD}".into()),
            ),
        ] {
            assert_eq!(
                verdict_in(raw, 8, cfg),
                want,
                "{}",
                String::from_utf8_lossy(raw)
            );
        }
        for (id, raw) in [
            (19u16, &b"TRUNCATE orders_d\xB0\xA1"[..]),
            (19, b"DROP TABLE orders_d\xB0\xA1"),
            (40, b"TRUNCATE orders_d\x82"),
            (40, b"DROP TABLE orders_d\x82"),
        ] {
            assert_eq!(
                verdict_in(raw, id, &only("shop.orders")),
                QueryVerdict::Pass,
                "{}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    #[test]
    fn an_unreadable_statement_is_refused_in_the_code_of_the_refusal_it_hides() {
        let hides = |v: QueryVerdict| {
            refusal_of(&QueryVerdict::Unreadable(Box::new(v)), Some(8)).expect("refuses")
        };
        let truncated = hides(QueryVerdict::Truncated("shop".into(), "orders".into()));
        assert_eq!(
            crate::error::error_code(&truncated),
            Some(crate::error::codes::SOURCE_CDC_TRUNCATED.id)
        );
        let renamed = hides(QueryVerdict::TableRenamed(pair("shop", "orders")));
        assert_eq!(
            crate::error::error_code(&renamed),
            Some(crate::error::codes::SOURCE_CDC_UNDECODABLE.id)
        );
        for err in [&truncated, &renamed] {
            assert_eq!(crate::error::classify_exit(err), 5);
            assert!(
                format!("{err:#}").contains(
                    "holds bytes this reader cannot tell from blanks, sent in the client \
                     character set `latin1` (collation id 8)"
                ),
                "{err:#}"
            );
        }
        let plain = refusal_of(&QueryVerdict::TableRenamed(pair("shop", "orders")), Some(8))
            .expect("refuses");
        assert!(
            !format!("{plain:#}").contains("cannot tell from blanks"),
            "{plain:#}"
        );
        assert_eq!(
            unreadable_refusal_message(Some(8), "mysql cdc: X"),
            "mysql cdc: a statement in the log being read holds bytes this reader cannot tell \
             from blanks, sent in the client character set `latin1` (collation id 8); the server \
             may read it as the following, so the run stops instead of guessing. X"
        );
        assert!(
            unreadable_refusal_message(Some(60000), "Y")
                .contains("collation id 60000, which this reader does not know")
        );
        assert!(
            unreadable_refusal_message(None, "Y")
                .contains("character set not recorded in the event")
        );
        for passes in [
            QueryVerdict::Pass,
            QueryVerdict::Commit,
            QueryVerdict::Privilege,
        ] {
            assert!(refusal_of(&passes, None).is_none());
        }
    }

    #[test]
    fn a_statement_is_read_in_the_character_set_the_event_names() {
        let hidden = b"ALTER TABLE t COMMENT = '\xE4\xB8\xAD\\', RENAME COLUMN a TO b";
        let column_renamed = |status: &[u8]| {
            let (sql, mode) = statement_of(hidden, status);
            column_rename_target(&sql, "shop", mode)
        };
        assert_eq!(column_renamed(&charset_var(28)), pair("shop", "t"));
        assert_eq!(column_renamed(&charset_var(255)), None);
        assert_eq!(column_renamed(&[]), None);
        let (sql, mode) = statement_of(b"x", &[charset_var(28), sql_mode_var(0x4)].concat());
        assert_eq!((sql.as_ref(), mode), ("x", ANSI));
    }
}

#[cfg(test)]
mod identity_recovery_order {
    use super::{CheckpointIdentity, IdentityVerdict};

    #[test]
    fn every_identity_refusal_names_the_anchor_before_the_snapshot() {
        for v in [
            CheckpointIdentity::ForeignServer {
                checkpoint: "a".into(),
                server: "b".into(),
            },
            CheckpointIdentity::GtidNotContained {
                checkpoint: "a:1-5".into(),
                server: "a:1-2".into(),
            },
        ] {
            let err = v.verdict().enforce().expect_err("refuses");
            assert_eq!(crate::error::classify_exit(&err), 5, "{v:?}: {err}");
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_SOURCE_CDC_FOREIGN_CHECKPOINT")
            );
            let why = err.to_string();
            assert!(
                why.ends_with(crate::source::cdc::checkpoint_identity::RECOVER),
                "{v:?}: {why}"
            );
        }
        assert_eq!(CheckpointIdentity::Ok.verdict(), IdentityVerdict::Ok);
        assert!(matches!(
            CheckpointIdentity::Unverifiable.verdict(),
            IdentityVerdict::Unverifiable(w) if w.contains("per-server")
        ));
    }

    #[test]
    fn a_checkpoint_without_file_or_pos_is_refused_as_invalid_with_exit_5() {
        use crate::source::cdc::Position;
        for hollow in [
            serde_json::json!({"pos": 4}),
            serde_json::json!({"file": "binlog.000001"}),
        ] {
            let err = super::MysqlChangeStream::resume_from_checkpoint(
                Some(&Position::new(hollow)),
                "/x",
            )
            .unwrap_err();
            assert_eq!(crate::error::classify_exit(&err), 5, "{err}");
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_SOURCE_CDC_CHECKPOINT_INVALID")
            );
            assert!(
                err.to_string().ends_with(&format!(
                    "Restore the file, or: {}",
                    crate::source::cdc::checkpoint_identity::RECOVER
                )),
                "the remedy must be anchor FIRST, then re-snapshot: {err}"
            );
        }
    }
}

/// The outcome of [`MysqlChangeStream::checkpoint_identity_verdict`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CheckpointIdentity {
    /// Same server, and any recorded GTID set is still present.
    Ok,
    /// The checkpoint predates identity recording. Accepted, but SAID — an
    /// unverifiable resume that says nothing is the silent half of the defect.
    Unverifiable,
    /// Written by a DIFFERENT server. Its coordinates address another binlog.
    ForeignServer { checkpoint: String, server: String },
    /// Same server, but the transactions the checkpoint names are gone —
    /// `RESET MASTER`, or a rebuild that kept the uuid.
    GtidNotContained { checkpoint: String, server: String },
    /// Same server, but the server gave no containment answer for a non-empty GTID set.
    GtidUnanswered,
}

impl CheckpointIdentity {
    /// The refusal text, or `None` when the resume may proceed.
    pub(crate) fn refusal(&self) -> Option<String> {
        match self {
            Self::Ok | Self::Unverifiable | Self::GtidUnanswered => None,
            Self::ForeignServer { checkpoint, server } => Some(format!(
                "mysql cdc: this checkpoint was written by server `{checkpoint}` and \
                 the connection is to `{server}`. Binlog coordinates are per-server: \
                 resuming would start at an arbitrary point in a DIFFERENT binlog, \
                 capturing whatever is there and skipping whatever was between — \
                 silently, in both directions. If the source genuinely moved \
                 (a failover, a restore), the old coordinates cannot be carried over."
            )),
            Self::GtidNotContained { checkpoint, server } => Some(format!(
                "mysql cdc: the checkpoint's GTID set `{checkpoint}` is not contained \
                 in the server's executed set `{server}` — the same server no longer \
                 has those transactions, which is what a `RESET MASTER` or a rebuild \
                 leaves behind. The coordinates address a binlog that no longer \
                 exists."
            )),
        }
    }

    /// The shared verdict this outcome enforces.
    pub(crate) fn verdict(&self) -> IdentityVerdict {
        match (self.refusal(), self.warning()) {
            (Some(why), _) => IdentityVerdict::Foreign(why),
            (None, Some(warn)) => IdentityVerdict::Unverifiable(warn),
            (None, None) => IdentityVerdict::Ok,
        }
    }

    /// What to WARN about when the resume proceeds but could not be verified.
    pub(crate) fn warning(&self) -> Option<String> {
        match self {
            Self::Unverifiable => Some(format!(
                "mysql cdc: this checkpoint carries no server identity, so rivet cannot \
                 confirm it belongs to the server it is resuming against. It was written \
                 before rivet recorded one. Binlog coordinates are per-server — if this \
                 config has ever been pointed at a different host, do not trust the resume. {}",
                crate::source::cdc::checkpoint_identity::RECOVER
            )),
            Self::GtidUnanswered => Some(
                "mysql cdc: the server gave no answer to GTID_SUBSET for this checkpoint's \
                 GTID set, so a RESET MASTER since it was written cannot be ruled out; \
                 resuming on the server uuid alone."
                    .to_string(),
            ),
            _ => None,
        }
    }
}

#[cfg(test)]
mod refusal_text_tests {
    use super::*;
    use crate::source::cdc::checkpoint_identity::RECOVER;

    /// An unknown table or database is the coded absent-table refusal; any other probe failure keeps the server's words.
    #[test]
    fn a_table_the_probe_cannot_prepare_is_absent_only_by_its_error_number() {
        for code in [1146, 1049] {
            let absent = table_probe_refusal("shop.orders", Some(code), "Table doesn't exist");
            assert_eq!(
                crate::error::error_code(&absent),
                Some("RIVET_SOURCE_CDC_PREREQUISITE"),
                "{code}"
            );
            assert_eq!(
                absent.to_string(),
                "mysql cdc: table `shop.orders` does not exist on this server (a bare name is read \
                 in the connection's database), so no change to it could ever be captured; nothing \
                 was read or written. Create the table, or fix `table:` in the export, then re-run."
            );
        }
        for other in [Some(1142), None] {
            let denied = table_probe_refusal("orders", other, "SELECT command denied");
            assert_eq!(
                crate::error::error_code(&denied),
                Some("RIVET_SOURCE_QUERY_FAILED")
            );
            assert_eq!(
                denied.to_string(),
                "mysql cdc: cannot read `orders` (SELECT command denied); nothing was read or written."
            );
        }
        let server = mysql::Error::MySqlError(mysql::error::MySqlError {
            state: "42S02".into(),
            message: "Table 'rivet.t' doesn't exist".into(),
            code: 1146,
        });
        assert_eq!(server_error_code(&server), Some(1146));
        let io = mysql::Error::IoError(std::io::Error::other("gone"));
        assert_eq!(server_error_code(&io), None);
    }

    fn target(schema: &str, table: &str) -> Option<(String, String)> {
        Some((schema.to_string(), table.to_string()))
    }

    /// A DROP refusal names the table, says why skipping is wrong, and ends in the recovery steps.
    #[test]
    fn a_drop_refusal_names_the_table_the_harm_and_the_recovery() {
        let said = drop_refusal_message(target("shop", "orders").as_ref());
        assert!(
            said.starts_with("mysql cdc: captured table `shop.orders` was DROPped"),
            "{said}"
        );
        assert!(
            said.contains("leave every row it held live in the destination"),
            "{said}"
        );
        assert!(said.ends_with(RECOVER), "{said}");
        let bare = drop_refusal_message(target("", "orders").as_ref());
        assert!(
            bare.contains("captured table `orders` was DROPped"),
            "{bare}"
        );
        let unknown = drop_refusal_message(None);
        assert!(
            unknown.contains("a table this reader could not identify"),
            "{unknown}"
        );
        assert!(unknown.ends_with(RECOVER), "{unknown}");
    }

    /// ERROR 1236 is a log gap only under the two purge wordings; the same code for another cause stays the driver's error.
    #[test]
    fn a_purged_binlog_is_a_log_gap_and_another_1236_is_not() {
        const BY_FILE: &str = "Could not find first log file name in binary log index file";
        const BY_GTID: &str = "Cannot replicate because the source purged required binary logs.";
        const SAME_ID: &str = "A replica with the same server_uuid/server_id as this replica \
                               has connected to the source";
        assert!(names_purged_binlog(1236, BY_FILE));
        assert!(names_purged_binlog(1236, BY_GTID));
        assert!(!names_purged_binlog(1236, SAME_ID));
        assert!(!names_purged_binlog(1045, BY_FILE));
        let server = |code, message: &str| {
            mysql::Error::MySqlError(mysql::error::MySqlError {
                state: "HY000".into(),
                message: message.into(),
                code,
            })
        };
        let gap = binlog_read_error(server(1236, BY_FILE), "binlog.000007");
        assert_eq!(crate::error::classify_exit(&gap), 5);
        assert_eq!(
            crate::error::error_code(&gap),
            Some("RIVET_SOURCE_CDC_LOG_GAP")
        );
        assert_eq!(
            gap.to_string(),
            format!(
                "mysql cdc: the server no longer holds binlog `binlog.000007`, where the \
                 checkpoint resumes (ERROR 1236: {BY_FILE}) — it was purged past the checkpoint, \
                 and the changes between the checkpoint and the oldest binlog the server still \
                 has are no longer in the log. {RECOVER}"
            )
        );
        let other = binlog_read_error(server(1236, SAME_ID), "binlog.000007");
        assert_eq!(crate::error::error_code(&other), None);
        assert!(other.to_string().contains(SAME_ID), "{other}");
    }

    /// A statement-logged change names the table, the setting to change, and the recovery steps.
    #[test]
    fn a_statement_logged_change_names_the_table_the_setting_and_the_recovery() {
        let said = statement_dml_refusal_message(target("shop", "orders").as_ref());
        assert!(
            said.starts_with(
                "mysql cdc: a change to `shop.orders` was written to the binlog as a SQL STATEMENT"
            ),
            "{said}"
        );
        assert!(
            said.contains("Set binlog_format=ROW for every writer"),
            "{said}"
        );
        assert!(
            said.contains("drop the change while the checkpoint moves past it"),
            "{said}"
        );
        assert!(said.ends_with(RECOVER), "{said}");
        let bare = statement_dml_refusal_message(target("", "orders").as_ref());
        assert!(bare.contains("a change to `orders` was written"), "{bare}");
        let unknown = statement_dml_refusal_message(None);
        assert!(
            unknown.contains("a table this reader could not identify"),
            "{unknown}"
        );
    }

    /// A TRUNCATE refusal names the table, says the destination would keep the rows, and ends in the recovery steps.
    #[test]
    fn a_truncate_refusal_names_the_table_the_harm_and_the_recovery() {
        let said = truncate_refusal_message("shop", "orders");
        assert!(
            said.starts_with("mysql cdc: `shop.orders` was TRUNCATEd"),
            "{said}"
        );
        assert!(said.contains("with no DELETE to retract it"), "{said}");
        assert!(said.ends_with(RECOVER), "{said}");
        let bare = truncate_refusal_message("", "orders");
        assert!(
            bare.starts_with("mysql cdc: `orders` was TRUNCATEd"),
            "{bare}"
        );
    }

    /// An XA PREPARE refusal names the table, says a re-run does not clear it, and gives both ways out.
    #[test]
    fn an_xa_prepare_refusal_names_the_table_that_a_rerun_will_not_help_and_the_ways_out() {
        let said = xa_prepare_refusal_message("shop", "orders");
        assert!(said.starts_with("mysql cdc: `shop.orders` was written inside an XA transaction that reached `XA PREPARE`"), "{said}");
        assert!(
            said.contains("the run stops with the checkpoint unmoved"),
            "{said}"
        );
        assert!(said.contains("This does NOT clear by re-running"), "{said}");
        assert!(
            said.contains(&format!("To move past it: {RECOVER} To avoid it")),
            "{said}"
        );
        assert!(
            said.contains("exported with `mode: full` meanwhile"),
            "{said}"
        );
        let bare = xa_prepare_refusal_message("", "orders");
        assert!(
            bare.starts_with("mysql cdc: `orders` was written inside an XA transaction"),
            "{bare}"
        );
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    /// An anchor refused because the identity query failed keeps the query's own code when it
    /// has one, else the query-failed code; a server with no binlog is a prerequisite.
    #[test]
    fn an_unverifiable_anchor_and_a_disabled_binlog_are_refused_by_code() {
        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("c.ckpt");
        let err = MysqlChangeStream::write_anchor(
            &ckpt,
            "binlog.000001",
            4,
            &Err(anyhow::anyhow!("boom")),
        )
        .unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_QUERY_FAILED", 1);
        let inner = crate::error::coded(crate::error::codes::SOURCE_CDC_PREREQUISITE, "x");
        let err =
            MysqlChangeStream::write_anchor(&ckpt, "binlog.000001", 4, &Err(inner)).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_PREREQUISITE", 1);
        assert!(!ckpt.exists(), "a refused anchor writes nothing");
        let err = binlog_disabled("binlog status empty");
        crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_PREREQUISITE", 1);
        assert_eq!(
            err.to_string(),
            "mysql: binlog disabled (binlog status empty)"
        );
    }
}
