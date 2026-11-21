//! PostgreSQL CDC adapter — logical replication slot → canonical [`cdc::ChangeEvent`].
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};

use postgres::Client;
use serde_json::json;

use crate::config::TlsConfig;
use crate::connect::postgres::read_pinned_text;
use crate::error::Result;
use crate::source::cdc::tx_buffer::{SpooledTail, TxBuffer, replay};
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{
    ChangeEvent, ChangeOp, ChangeStream, DrainMode, Position, PriorRun, ReadableColumns,
};
use crate::source::postgres::td_line::{
    self, split_qualified_ident, split_top_level_commas, utf8_len,
};

/// Polls a logical slot and yields canonical changes.
pub(crate) type Domains = std::collections::HashMap<String, String>;

pub(crate) struct PgChangeStream {
    client: Client,
    /// The database's domains, so a domain-typed cell decodes as its base type.
    domains: Domains,
    slot: String,
    pending: VecDeque<ChangeEvent>,
    /// Directory for an oversized transaction's spill, or `None` when the run has nowhere it knows is sized for it.
    spill_dir: Option<std::path::PathBuf>,
    /// A committed transaction whose tail is on disk, still being handed out.
    spooled: Option<SpooledTail>,
    /// Wire budget per `peek` — the memory bound of the drain (O(batch), not O(total backlog)).
    batch_limit: i32,
    /// Largest COMMIT LSN already yielded THIS run.
    frontier: u64,
    /// A peek that yields no NEW transaction, or returns fewer than a full batch, has drained everything readable *from the current slot position*.
    exhausted: bool,
    /// A TRUNCATE of a captured table, seen but NOT yet raised.
    pending_truncate_refusal: Option<String>,
    /// Open-time COMMIT-LSN ceiling for a bounded run.
    bound: Option<u64>,
    /// COMMIT LSN of the last DATA-bearing transaction yielded and not yet covered by an ack.
    owed_through: Option<u64>,
    /// Rendered LSN of the last frontier advance — the zero-yield release target.
    frontier_text: Option<String>,
    /// The `table:` values this run captures — what a TRUNCATE is checked against before it is allowed to fail the run.
    configured_tables: Vec<String>,
    /// The schema each bare configured name resolves to through the search_path, read at open.
    bare_schema: HashMap<String, String>,
    /// The nonce of the BARRIER this bounded run emitted at open, if it could.
    barrier_nonce: Option<String>,
    /// The export's checkpoint file, kept at the slot's position before every advance rivet makes.
    checkpoint: Option<std::path::PathBuf>,
    /// `columns_from: grants`: the columns each captured table may deliver, as the run resolved them.
    readable: Option<std::sync::Arc<HashMap<String, ReadableColumns>>>,
}

/// The run-start warning emitted when a logical replication slot has to be CREATED — i.e.
pub(crate) fn slot_created_warning(slot: &str) -> String {
    format!(
        "pg cdc: creating replication slot '{slot}' — capture starts at the CURRENT WAL \
         position, so changes written before now are NOT captured. On a first run this is \
         expected. If this slot existed before, it was dropped or invalidated and the changes \
         since then are not in this stream. {} Set `cdc.checkpoint:` to turn this case into a \
         hard error instead of a warning.",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

/// Whether the replication slot `slot` exists on the server at `url`.
pub(crate) fn slot_exists(url: &str, slot: &str, tls: Option<&TlsConfig>) -> Result<bool> {
    slot_exists_on(&mut super::connect_client(url, tls)?, slot)
}

/// Whether the replication slot `slot` exists on the server `client` is connected to.
pub(crate) fn slot_exists_on(client: &mut Client, slot: &str) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot],
        )?
        .get(0))
}

/// The output plugin a logical slot was created with; fixed for the slot's life.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SlotPlugin {
    TestDecoding,
    Pgoutput,
    Other(String),
}

impl SlotPlugin {
    /// The plugin named by `pg_replication_slots.plugin`.
    pub(crate) fn from_name(name: &str) -> Self {
        match name {
            "test_decoding" => Self::TestDecoding,
            "pgoutput" => Self::Pgoutput,
            other => Self::Other(other.to_string()),
        }
    }

    /// The `plugin` text the server holds for this plugin.
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::TestDecoding => "test_decoding",
            Self::Pgoutput => "pgoutput",
            Self::Other(n) => n,
        }
    }
}

/// Text rows of a catalog query: one `Vec<Option<String>>` per row, cells in select order.
pub(crate) trait CatalogRows {
    /// The rows of `sql` run with the one text parameter `arg`.
    fn text_rows(&mut self, sql: &str, arg: &str) -> Result<Vec<Vec<Option<String>>>>;
}

impl CatalogRows for Client {
    fn text_rows(&mut self, sql: &str, arg: &str) -> Result<Vec<Vec<Option<String>>>> {
        let rows = self.query(sql, &[&arg])?;
        Ok(rows
            .iter()
            .map(|r| {
                (0..r.len())
                    .map(|i| r.get::<_, Option<String>>(i))
                    .collect()
            })
            .collect())
    }
}

/// The plugin of the logical slot `slot` on `client`, or `None` when the slot does not exist.
pub(crate) fn slot_plugin_on<C: CatalogRows>(
    client: &mut C,
    slot: &str,
) -> Result<Option<SlotPlugin>> {
    let rows = client.text_rows(
        "SELECT plugin::text FROM pg_replication_slots WHERE slot_name = $1",
        slot,
    )?;
    Ok(slot_plugin_from_rows(&rows))
}

/// The plugin the first `pg_replication_slots` row names; a NULL plugin reads as the empty name.
pub(crate) fn slot_plugin_from_rows(rows: &[Vec<Option<String>>]) -> Option<SlotPlugin> {
    rows.first()
        .map(|r| SlotPlugin::from_name(r.first().cloned().flatten().unwrap_or_default().as_str()))
}

/// The refusal for an existing slot whose plugin the `test_decoding` stream cannot read.
pub(crate) fn slot_plugin_refusal(slot: &str, plugin: &SlotPlugin) -> String {
    format!(
        "pg cdc: slot '{slot}' uses the '{}' output plugin, but this stream reads 'test_decoding' slots only. \
         Drop the slot (`SELECT pg_drop_replication_slot('{slot}')`) and re-baseline the stream, or point `cdc.slot` at a different name.",
        plugin.name()
    )
}

/// An existing slot is reused only when its plugin is `test_decoding`; any other plugin is refused by code.
pub(crate) fn refuse_a_foreign_slot_plugin(slot: &str, existing: Option<SlotPlugin>) -> Result<()> {
    match existing.filter(|p| *p != SlotPlugin::TestDecoding) {
        Some(plugin) => crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            "{}",
            slot_plugin_refusal(slot, &plugin)
        ),
        None => Ok(()),
    }
}

/// Drop the replication slot `slot`, waiting for the server to release a slot the closed stream still holds.
pub(crate) fn drop_slot(url: &str, slot: &str, tls: Option<&TlsConfig>) -> Result<()> {
    let mut client = super::connect_client(url, tls)?;
    retry_drop(
        || {
            client
                .execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
                    &[&slot],
                )
                .map(|_| ())
        },
        |e| log::debug!("pg cdc: slot '{slot}' not dropped yet: {e}"),
        || std::thread::sleep(std::time::Duration::from_millis(200)),
    )
    .map_err(Into::into)
}

/// How many times a failed slot drop is retried before its error is returned.
const DROP_SLOT_RETRIES: u32 = 25;

/// Runs `attempt`, retrying a failure up to [`DROP_SLOT_RETRIES`] times with `pause` between tries.
fn retry_drop<E>(
    mut attempt: impl FnMut() -> std::result::Result<(), E>,
    mut note: impl FnMut(&E),
    mut pause: impl FnMut(),
) -> std::result::Result<(), E> {
    let mut tries = 0;
    loop {
        match attempt() {
            Ok(()) => return Ok(()),
            Err(e) if tries < DROP_SLOT_RETRIES => {
                note(&e);
                tries += 1;
                pause();
            }
            Err(e) => return Err(e),
        }
    }
}

/// The refusal for a missing slot, naming exactly the prior-run evidence the caller checked.
pub(crate) fn pg_slot_missing_refusal(slot: &str, prior: PriorRun) -> String {
    const CKPT: &str = "the checkpoint file holds a position from a prior run";
    const SNAP: &str = "a prior run completed this export's snapshot (the state DB's \
                        `cdc_snapshot` row or the destination's snapshot/_SUCCESS marker)";
    let evidence = match (prior.checkpoint, prior.snapshot) {
        (true, true) => format!("{CKPT} and {SNAP}"),
        (true, false) => CKPT.to_string(),
        (false, _) => SNAP.to_string(),
    };
    format!(
        "pg cdc: slot '{slot}' is missing but {evidence} — the slot was dropped or invalidated, \
         and the changes since then are no longer in the log. {}",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

/// What `open` does about the slot: reuse it, create it, or refuse with the text to show.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SlotAction {
    Reuse,
    Create,
    Refuse(String),
}

/// Decides [`SlotAction`] from whether the slot exists and the prior-run evidence found.
pub(crate) fn slot_action(slot: &str, exists: bool, prior: PriorRun) -> SlotAction {
    if exists {
        SlotAction::Reuse
    } else if prior.any() {
        SlotAction::Refuse(pg_slot_missing_refusal(slot, prior))
    } else {
        SlotAction::Create
    }
}

/// What one CONFIGURED table can contribute to a `test_decoding` stream.
pub(crate) struct RelationRouting<'a> {
    /// The `table:` string the config asked for — what the sink routes BY.
    pub configured: &'a str,
    /// `pg_class.relkind` of the relation that name resolves to.
    pub relkind: char,
    /// `pg_class.relpersistence` — `u` for UNLOGGED.
    pub relpersistence: char,
    /// The CATALOG's spelling of the relation the config string resolved to, as `(schema, table)`.
    pub resolved: (String, String),
    /// Relations that inherit from it — partitions, or legacy `INHERITS` children.
    pub children: Vec<String>,
    /// For a partitioned parent: the LEAF partitions, from `pg_partition_tree`.
    pub leaves: Vec<String>,
}

/// The verdict for one configured table.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RoutingVerdict {
    Routable,
    Never(String),
    Partial(String),
}

/// Classify one configured relation.
pub(crate) fn classify_routing(rel: &RelationRouting<'_>) -> RoutingVerdict {
    let cfg = rel.configured;
    let (rs, rt) = (&rel.resolved.0, &rel.resolved.1);

    if !crate::source::cdc::sink::table_matches(
        crate::source::cdc::CdcEngine::Postgres,
        cfg,
        rs,
        rt,
    ) {
        if seeded!(
            "pg-dotted-relname-routing-refusal-names-a-spelling-that-cannot-match",
            false,
            true
        ) && rt.contains('.')
        {
            return RoutingVerdict::Never(format!(
                "pg cdc: `{cfg}` resolves to the relation `{rs}.{rt}`, whose NAME holds a dot. \
                 Routing reads a dotted `table:` as `schema.table`, so no spelling of `table:` \
                 can route it, and its changes would be dropped while the slot advanced past \
                 them. Rename the relation to a name without a dot, or drop it from the config."
            ));
        }
        return RoutingVerdict::Never(format!(
            "pg cdc: `{cfg}` resolves to the relation `{rs}.{rt}`, but routing compares the              config string BYTE-EXACT against the name test_decoding emits — and those two              disagree here. The schema probe would read `{rs}.{rt}`'s columns while events              carrying a different spelling route nowhere, so the run would either capture              NOTHING or write rows under the wrong table's schema, silently, past an              advancing slot. Set `table:` to `{rs}.{rt}` exactly as the catalog spells it."
        ));
    }

    if rel.relpersistence == 'u' {
        return RoutingVerdict::Never(format!(
            "pg cdc: `{cfg}` is an UNLOGGED table. Unlogged tables write no WAL, so logical              decoding never produces a single event for them and the capture would report              success with zero rows forever while the slot advanced past other traffic.              `ALTER TABLE {rs}.{rt} SET LOGGED` to capture it, or drop it from the config."
        ));
    }

    match rel.relkind {
        'p' => {
            let named = if rel.leaves.is_empty() {
                "it has no partitions yet".to_string()
            } else {
                format!("currently {}", rel.leaves.join(", "))
            };
            RoutingVerdict::Never(format!(
                "pg cdc: `{cfg}` is a PARTITIONED table — it stores no rows itself, and \
                 test_decoding names the PARTITION a row landed in, never the parent. Routing is \
                 byte-exact, so every change would be dropped AND the slot would advance past it: \
                 PostgreSQL frees the WAL and the changes are gone from the log and the \
                 destination alike, while the run reports success with 0 rows. Capture the \
                 partitions by name ({named}) — a partition added later needs its own entry — or \
                 snapshot the parent with `mode: full`, which reads through the parent normally."
            ))
        }
        'm' => RoutingVerdict::Partial(format!(
            "pg cdc: `{cfg}` is a MATERIALIZED VIEW. It CAN be captured — a `REFRESH \
             MATERIALIZED VIEW CONCURRENTLY` decodes as ordinary row events under its \
             own name — but know what that delivers: REFRESH DELTAS, not the base \
             table's own changes, and a PLAIN `REFRESH MATERIALIZED VIEW` emits \
             nothing at all, so a capture whose refresh job uses the plain form sits \
             at 0 rows forever while reporting success. Capture the base table(s) \
             instead if you want the source's changes."
        )),
        'v' | 'f' => {
            let what = match rel.relkind {
                'v' => "a VIEW",
                _ => "a FOREIGN TABLE",
            };
            RoutingVerdict::Never(format!(
                "pg cdc: `{cfg}` is {what}, which writes no WAL under its own name — no change \
                 event can ever carry it, so capture would drop everything while advancing the \
                 slot past it (a success with 0 rows, unrecoverable). Capture the BASE table(s) it \
                 reads instead."
            ))
        }
        'r' | 'P' if !rel.children.is_empty() => RoutingVerdict::Partial(format!(
            "pg cdc: `{cfg}` has {} inheritance child table(s) ({}). test_decoding names the CHILD \
             a row landed in, so changes written directly to a child route NOWHERE while the \
             parent's own rows capture normally — a partial, silent gap whose size depends on how \
             the writers address the table. List the children as captured tables if their changes \
             matter.",
            rel.children.len(),
            rel.children.join(", ")
        )),
        _ => RoutingVerdict::Routable,
    }
}

/// What the prerequisite probe prepares for each captured table.
pub(crate) enum Probe<'a> {
    Whole,
    Readable(Option<&'a HashMap<String, ReadableColumns>>),
}

impl<'a> Probe<'a> {
    /// The probe a stream opened with `readable` runs.
    fn of(readable: Option<&'a HashMap<String, ReadableColumns>>) -> Self {
        match readable {
            Some(r) => Self::Readable(Some(r)),
            None => Self::Whole,
        }
    }

    /// The statement that describes `table`: every column, or only the ones the account may read.
    fn select_of(&self, client: &mut Client, table: &str) -> Result<String> {
        match self {
            Self::Whole => Ok(format!("SELECT * FROM {table}")),
            Self::Readable(resolved) => match (*resolved).and_then(|r| r.get(table)) {
                Some(r) => Ok(r.select.clone()),
                None => super::column_grants::readable_columns(client, table.trim())
                    .map(|(_, _, r)| r.select),
            },
        }
    }
}

/// Refuse a run whose captured tables' readable columns are no longer the ones it resolved before its anchor.
fn refuse_grants_moved_since_resolved(
    client: &mut Client,
    configured: &[String],
    resolved: &HashMap<String, ReadableColumns>,
) -> Result<()> {
    let now = super::column_grants::readable_columns_now(client, configured)?;
    match crate::source::column_grants::first_grants_change(configured, resolved, &now) {
        Some(why) => Err(crate::error::coded(
            crate::error::codes::SOURCE_CDC_GRANTS_CHANGED,
            format!(
                "pg cdc: {why}; nothing read since the last delivered change was delivered. \
                 Re-run: the next run reads the grants as they are now and resumes from the \
                 checkpoint, so nothing is lost"
            ),
        )),
        None => Ok(()),
    }
}

impl PgChangeStream {
    /// Each captured table's old image, from its replica identity and primary key; a catalog error answers nothing.
    pub(crate) fn old_images_on(client: &mut Client, tables: &[String]) -> Vec<(String, OldImage)> {
        let Ok(rows) = client.query(
            "SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname), c.relreplident::text, \
                    (SELECT i.indimmediate FROM pg_index i \
                      WHERE i.indrelid = c.oid AND i.indisprimary), \
                    EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident) \
               FROM unnest($1::text[]) AS t(name) \
               JOIN pg_class c ON c.oid = to_regclass(t.name) \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
              WHERE c.relkind IN ('r', 'p')",
            &[&tables],
        ) else {
            return Vec::new();
        };
        rows.iter()
            .map(|r| {
                (
                    r.get(0),
                    old_image(&r.get::<_, String>(1), r.get(2), r.get(3)),
                )
            })
            .collect()
    }

    /// Live half of the routing guard: ask the CATALOG what each configured table actually is, and refuse a capture that could only ever drop everything.
    pub(crate) fn check_configured_tables_are_routable(
        client: &mut Client,
        configured: &[String],
        say_note: bool,
    ) -> Result<()> {
        use anyhow::Context as _;
        for cfg in configured {
            let matches: Vec<crate::source::cdc::identity::CatalogMatch> = client
                .query(
                    "SELECT n.nspname::text, c.relname::text, c.relkind::text, \
                            (c.relkind IN ('r','m') AND c.relpersistence = 'p') \
                              AS capturable \
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.relkind IN ('r','p','m','v','f') \
                       AND n.nspname NOT IN ('pg_catalog','information_schema') \
                       AND n.nspname NOT LIKE 'pg_temp%' \
                       AND n.nspname NOT LIKE 'pg_toast%' \
                       AND (c.oid = to_regclass($1) \
                            OR (position('.' in $1) = 0 AND c.relname = $1) \
                            OR (position('.' in $1) > 0 \
                                AND n.nspname = split_part($1, '.', 1) \
                                AND c.relname = substr($1, position('.' in $1) + 1)))",
                    &[&cfg.as_str()],
                )
                .map_err(|e| {
                    if seeded!("pg-dotted-relname-catalog-error-uncoded", false, true)
                        && e.code() == Some(&postgres::error::SqlState::FEATURE_NOT_SUPPORTED)
                    {
                        crate::error::coded(
                            crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
                            format!(
                                "pg cdc: `{cfg}` names a relation in another database, or a relation whose name holds a dot ({e}). `table:` takes `table` or `schema.table` of the connected database, and a dotted `table:` is always read as `schema.table`; a relation whose name holds a dot cannot be captured by name: rename it."
                            ),
                        )
                    } else {
                        anyhow::Error::new(e)
                            .context(format!("pg cdc: listing catalog matches for `{cfg}`"))
                    }
                })?
                .iter()
                .map(|r| crate::source::cdc::identity::CatalogMatch {
                    schema: r.get(0),
                    table: r.get(1),
                    kind: r.get::<_, String>(2),
                    capturable: r.get::<_, bool>(3),
                })
                .collect();
            let matches: Vec<_> = match search_path_schema(client, cfg)? {
                Some(sch) => matches.into_iter().filter(|m| m.schema == sch).collect(),
                None => matches,
            };
            let mut resolved: Option<(String, String)> = None;
            if !matches.is_empty() {
                let elected = crate::source::cdc::identity::resolve_captured_table(cfg, &matches)?;
                if let Some(note) = &elected.note
                    && say_note
                {
                    log::warn!("pg cdc: {note}");
                }
                resolved = Some((elected.schema, elected.table));
            }
            let (rs, rt): (Option<&str>, Option<&str>) = match &resolved {
                Some((sch, tbl)) => (Some(sch.as_str()), Some(tbl.as_str())),
                None => (None, None),
            };
            let Some(row) = client
                .query_opt(
                    "SELECT c.relkind::text, c.relpersistence::text, \
                            n.nspname::text, c.relname::text, \
                            coalesce(array_agg(n2.nspname || '.' || c2.relname) \
                                     FILTER (WHERE c2.oid IS NOT NULL), '{}'), \
                            CASE WHEN c.relkind = 'p' THEN ( \
                                SELECT coalesce(array_agg(pt.relid::regclass::text), '{}') \
                                FROM pg_partition_tree(c.oid) pt WHERE pt.isleaf) \
                            ELSE '{}' END \
                     FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     LEFT JOIN pg_inherits i ON i.inhparent = c.oid \
                     LEFT JOIN pg_class c2 ON c2.oid = i.inhrelid \
                     LEFT JOIN pg_namespace n2 ON n2.oid = c2.relnamespace \
                     WHERE (($2::text IS NULL AND c.oid = to_regclass($1)) \
                        OR (n.nspname = $2 AND c.relname = $3)) \
                     GROUP BY c.relkind, c.relpersistence, n.nspname, c.relname, c.oid",
                    &[&cfg.as_str(), &rs, &rt],
                )
                .with_context(|| {
                    format!("pg cdc: reading pg_class to check that `{cfg}` is routable")
                })?
            else {
                continue;
            };
            let relkind: String = row.get(0);
            let relpersistence: String = row.get(1);
            let resolved_schema: String = row.get(2);
            let resolved_table: String = row.get(3);
            let children: Vec<String> = row.get(4);
            let leaves: Vec<String> = row.get(5);
            let rel = RelationRouting {
                configured: cfg,
                relkind: relkind.chars().next().unwrap_or('?'),
                relpersistence: relpersistence.chars().next().unwrap_or('p'),
                resolved: (resolved_schema, resolved_table),
                leaves,
                children,
            };
            match classify_routing(&rel) {
                RoutingVerdict::Routable => {}
                RoutingVerdict::Partial(why) => {
                    if say_note {
                        log::warn!("{why}");
                    }
                }
                RoutingVerdict::Never(why) => {
                    crate::rivet_bail!(crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED, "{why}")
                }
            }
        }
        Ok(())
    }

    /// The open's refusals, asked on a connection of their own before any slot or snapshot part is written.
    pub(crate) fn refuse_unmet_prerequisites(
        conn_str: &str,
        tls: Option<&TlsConfig>,
        configured_tables: &[String],
        mode: DrainMode,
        columns_from_grants: bool,
    ) -> Result<()> {
        let mut client = super::connect_client(conn_str, tls)?;
        let probe = if columns_from_grants {
            Probe::Readable(None)
        } else {
            Probe::Whole
        };
        Self::refuse_unmet_prerequisites_on(&mut client, configured_tables, mode, false, probe)
    }

    /// Every refusal of the open (routing, an absent table, `wal_level`, a bounded drain on a standby), asked on `client`.
    fn refuse_unmet_prerequisites_on(
        client: &mut Client,
        configured_tables: &[String],
        mode: DrainMode,
        say_note: bool,
        probe: Probe<'_>,
    ) -> Result<()> {
        let wal_level: String = client.query_one("SHOW wal_level", &[])?.get(0);
        let in_recovery: bool = client.query_one("SELECT pg_is_in_recovery()", &[])?.get(0);
        if let Some(why) = server_refusal(&wal_level, mode.is_bounded(), in_recovery) {
            crate::rivet_bail!(crate::error::codes::SOURCE_CDC_PREREQUISITE, "{why}");
        }
        Self::check_configured_tables_are_routable(client, configured_tables, say_note)?;
        for table in configured_tables {
            crate::source::cdc::validate_table_ident(table)?;
            let select = probe.select_of(client, table)?;
            super::prepare_fresh(client, &select).map_err(|e| {
                table_probe_refusal(table, e.code().map(|c| c.code()), &e.to_string())
            })?;
        }
        Ok(())
    }

    /// Connect and ensure a `test_decoding` logical slot named `slot` exists (idempotent.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open(
        conn_str: &str,
        slot: &str,
        prior: PriorRun,
        tls: Option<&TlsConfig>,
        peek: crate::source::cdc::PeekBound,
        mode: DrainMode,
        configured_tables: &[String],
        spill_dir: Option<&std::path::Path>,
        checkpoint: Option<&std::path::Path>,
        readable: Option<std::sync::Arc<HashMap<String, ReadableColumns>>>,
    ) -> Result<Self> {
        let mut client = super::connect_client(conn_str, tls)?;
        if let Some(resolved) = &readable {
            refuse_grants_moved_since_resolved(&mut client, configured_tables, resolved)?;
        }
        if seeded!("session-set-on-a-pooled-cdc-connection", true, false) {
            client.batch_execute(
                "SET datestyle = 'ISO, MDY'; SET bytea_output = 'hex'; SET intervalstyle = 'postgres'; \
                 SET extra_float_digits = 3; SET TimeZone = 'UTC';",
            )?;
        }
        if seeded!("cdc-anchor-written-before-the-refusal", false, true) {
            Self::refuse_unmet_prerequisites_on(
                &mut client,
                configured_tables,
                mode,
                true,
                Probe::of(readable.as_deref()),
            )?;
        }
        let mut bare_schema = HashMap::new();
        for cfg in configured_tables {
            if let Some(sch) = search_path_schema(&mut client, cfg)? {
                bare_schema.insert(cfg.clone(), sch);
            }
        }
        let domains = load_domains(&mut client)?;
        let existing = slot_plugin_on(&mut client, slot)?;
        match slot_action(slot, existing.is_some(), prior) {
            SlotAction::Reuse => {
                refuse_a_foreign_slot_plugin(slot, existing)?;
                refuse_a_slot_another_reader_advanced(&mut client, slot, checkpoint)?
            }
            SlotAction::Refuse(why) => {
                return Err(crate::source::cdc::checkpoint_identity::log_gap(why));
            }
            SlotAction::Create => {
                log::warn!("{}", slot_created_warning(slot));
                client.execute(
                    "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
                    &[&slot],
                )?;
            }
        }
        if seeded!("cdc-anchor-written-before-the-refusal", true, false) {
            Self::refuse_unmet_prerequisites_on(
                &mut client,
                configured_tables,
                mode,
                true,
                Probe::of(readable.as_deref()),
            )?;
        }
        let bound = if mode.is_bounded() {
            use anyhow::Context as _;
            let lsn: String = client
                .query_one("SELECT pg_current_wal_lsn()::text", &[])
                .context(
                    "bounded (until_current) CDC pins its ceiling with pg_current_wal_lsn(), \
                     which is unavailable during recovery — on a standby, stream continuously \
                     (until_current: false) or point the source at the primary",
                )?
                .get(0);
            parse_lsn(&lsn)
        } else {
            None
        };
        let barrier_nonce = mode
            .is_bounded()
            .then(|| {
                let nonce = format!("{}-{}", slot, run_nonce());
                match client.execute(
                    "SELECT pg_logical_emit_message(false, $1, $2), txid_current()",
                    &[&BARRIER_PREFIX, &nonce],
                ) {
                    Ok(_) => Some(nonce),
                    Err(e) => {
                        log::warn!(
                            "pg cdc: could not emit the drain barrier ({e}); falling back to \
                         the pg_current_wal_lsn() ceiling, which stops at the first commit \
                         past an open-time snapshot rather than at an exact marker. The \
                         run still terminates and loses nothing — the boundary is just \
                         approximate."
                        );
                        None
                    }
                }
            })
            .flatten();
        Ok(Self {
            client,
            domains,
            slot: slot.to_string(),
            pending: VecDeque::new(),
            spill_dir: spill_dir.map(std::path::Path::to_path_buf),
            spooled: None,
            batch_limit: wire_budget(peek),
            frontier: 0,
            exhausted: false,
            pending_truncate_refusal: None,
            bound,
            owed_through: None,
            frontier_text: None,
            configured_tables: configured_tables.to_vec(),
            bare_schema,
            barrier_nonce,
            checkpoint: checkpoint.map(std::path::Path::to_path_buf),
            readable,
        })
    }

    /// Peek **one bounded batch** into `pending` **without consuming it** (`pg_logical_slot_peek_changes(slot, NULL, batch_limit)`).
    fn fill(&mut self) -> Result<()> {
        if let Some(why) = self.pending_truncate_refusal.take() {
            return Err(crate::source::cdc::checkpoint_identity::truncated(why));
        }
        let rows = read_pinned_text(&mut self.client, |c| {
            c.query(
                "SELECT lsn::text, data FROM pg_logical_slot_peek_changes($1, NULL, $2)",
                &[&self.slot, &self.batch_limit],
            )
        })?;
        self.refuse_grants_moved()?;
        let n_rows = rows.len();
        let mut tx = TxBuffer::new(
            crate::source::cdc::CdcEngine::Postgres,
            self.spill_dir.clone(),
        );
        let mut stopped_to_spool = false;
        let mut yielded_any = false;
        for r in rows {
            let lsn: String = r.get(0);
            let data: String = r.get(1);
            if data.starts_with("COMMIT") {
                let commit_lsn = parse_lsn(&lsn).unwrap_or(0);
                match tx_disposition(commit_lsn, self.frontier, self.bound) {
                    TxDisposition::Yield => {
                        let commit = Position::new(json!({ "lsn": lsn }));
                        let (head, tail) = tx.close_transaction(&commit, &lsn)?;
                        let head = seeded!(
                            "committed-every-event",
                            head.into_iter()
                                .map(|mut e| {
                                    e.committed = true;
                                    e
                                })
                                .collect::<Vec<_>>(),
                            head
                        );
                        self.owed_through = owed_after_yield(
                            self.owed_through,
                            commit_lsn,
                            head.len(),
                            tail.is_some(),
                        );
                        self.pending.extend(head);
                        self.frontier = commit_lsn;
                        self.frontier_text = Some(lsn.clone());
                        yielded_any = true;
                        if tail.is_some() {
                            self.spooled = tail;
                            stopped_to_spool = true;
                            break;
                        }
                    }
                    TxDisposition::AlreadyYielded => tx.clear(),
                    TxDisposition::PastBound => {
                        tx.clear();
                        self.exhausted = true;
                        break;
                    }
                }
            } else if self
                .barrier_nonce
                .as_deref()
                .is_some_and(|n| is_barrier_line(&data, n))
            {
                tx.clear();
                self.exhausted = true;
                break;
            } else if is_standalone_message(&data) {
                if past_the_bound(&lsn, self.bound) {
                    self.exhausted = true;
                    break;
                }
                self.frontier_text = seeded!(
                    "message-only-window-ends-the-drain",
                    self.frontier_text.take(),
                    Some(lsn)
                );
            } else if data.starts_with("BEGIN") {
                tx.clear();
            } else if let Some((schema, table)) = truncate_targets(&data)
                .into_iter()
                .find(|(sc, tb)| routes(&self.configured_tables, &self.bare_schema, sc, tb))
            {
                let why = truncate_refusal_message(&schema, &table);
                if !yielded_any {
                    return Err(crate::source::cdc::checkpoint_identity::truncated(why));
                }
                self.pending_truncate_refusal = Some(why);
                self.exhausted = true;
                break;
            } else if let Some(ev) = self.parse(&lsn, &data)? {
                if is_a_foreign_twin(
                    &self.configured_tables,
                    &self.bare_schema,
                    &ev.schema,
                    &ev.table,
                ) {
                    continue;
                }
                if self.readable.is_some() {
                    tx.push(ev, crate::source::cdc::spill::encode_event, &lsn)?;
                } else {
                    tx.push(ev, |_| encode_wire_row(&lsn, &data), &lsn)?;
                }
            }
        }
        if !stopped_to_spool && (n_rows < self.batch_limit as usize || !yielded_any) {
            self.exhausted = true;
        }
        Ok(())
    }

    /// One wire row as a change, cut to the columns the run may deliver.
    fn parse(&self, lsn: &str, data: &str) -> Result<Option<ChangeEvent>> {
        let projected = self
            .readable
            .as_deref()
            .filter(|_| seeded!("column-grants-pg-stream-unprojected", false, true));
        let Some(resolved) = projected else {
            return parse_test_decoding(lsn, data, &self.domains);
        };
        let keep = |schema: &str, table: &str| {
            super::column_grants::readable_of(
                resolved,
                &self.configured_tables,
                &self.bare_schema,
                schema,
                table,
            )
        };
        parse_test_decoding_keeping(lsn, data, &self.domains, Some(&keep))
    }

    /// Refuse a batch read while the readable columns of a captured table differ from the ones the run resolved.
    fn refuse_grants_moved(&mut self) -> Result<()> {
        if seeded!("column-grants-pg-batch-grants-not-reread", true, false) {
            return Ok(());
        }
        match self.readable.clone() {
            Some(resolved) => refuse_grants_moved_since_resolved(
                &mut self.client,
                &self.configured_tables,
                &resolved,
            ),
            None => Ok(()),
        }
    }

    /// One row of a spilled tail: decode it, stamp it, and drop the reader once the last row is out.
    fn next_spooled(&mut self) -> Result<Option<ChangeEvent>> {
        if self.readable.is_some() {
            return replay(&mut self.spooled, crate::source::cdc::spill::decode_event);
        }
        replay(&mut self.spooled, |rec| {
            let (lsn, data) = decode_wire_row(rec)?;
            parse_test_decoding(&lsn, &data, &self.domains)?.ok_or_else(|| {
                crate::error::coded(
                    crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                    "pg cdc spill: a spilled row decodes to no change, though only \
                     rows that decoded were spilled. The tail and the decoder \
                     disagree, and delivering the rest would move the transaction's \
                     commit boundary onto the wrong row.",
                )
            })
        })
    }

    /// Zero-yield release: called at clean exhaust.
    fn release_empty_frontier(&mut self) {
        if self.owed_through.is_some() {
            return;
        }
        let Some(lsn) = self.frontier_text.take() else {
            return;
        };
        if let Err(e) = self.record_confirmed(&lsn) {
            log::warn!("pg cdc: could not record the empty-transaction span at {lsn}: {e:#}");
            return;
        }
        if let Err(e) = self.advance_slot(&lsn) {
            log::warn!("pg cdc: could not release the empty-transaction span at {lsn}: {e:#}");
        }
    }

    /// Save `lsn` as the export's checkpoint, the position rivet confirmed, when it has one.
    fn record_confirmed(&self, lsn: &str) -> Result<()> {
        match &self.checkpoint {
            Some(ck) => confirmed_first(&Position::new(json!({ "lsn": lsn }))).save(ck),
            None => Ok(()),
        }
    }

    /// Advance the slot's `confirmed_flush_lsn` to `lsn`, validated to the pg_lsn charset before interpolation.
    fn advance_slot(&mut self, lsn: &str) -> Result<()> {
        if lsn.is_empty() || !lsn.bytes().all(|b| b.is_ascii_hexdigit() || b == b'/') {
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                "pg cdc: refusing to advance to a malformed LSN {lsn:?}"
            );
        }
        self.client.execute(
            &format!("SELECT pg_replication_slot_advance($1, '{lsn}'::pg_lsn)"),
            &[&self.slot],
        )?;
        self.exhausted = false;
        Ok(())
    }
}

/// The checkpoint key that says it was written by a release that saves the checkpoint before every slot advance (0.32.2 and later).
const CONFIRMED_FIRST: &str = "confirmed_first";

/// `position` as a checkpoint this release writes: marked [`CONFIRMED_FIRST`].
fn confirmed_first(position: &Position) -> Position {
    let mut json = position.json().clone();
    if let Some(obj) = json.as_object_mut() {
        obj.insert(CONFIRMED_FIRST.to_string(), json!(true));
    }
    Position::new(json)
}

/// Refuse a slot another reader advanced past the last position rivet confirmed for this export.
fn refuse_a_slot_another_reader_advanced(
    client: &mut postgres::Client,
    slot: &str,
    checkpoint: Option<&std::path::Path>,
) -> Result<()> {
    let Some(path) = checkpoint else {
        return Ok(());
    };
    let Some(saved) = Position::load(path)? else {
        return Ok(());
    };
    let Some(own) = saved
        .json()
        .get("lsn")
        .and_then(|v| v.as_str())
        .map(str::to_string)
    else {
        return Ok(());
    };
    let confirmed: Option<String> = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )?
        .get(0);
    match open_verdict(slot, &saved, &own, confirmed.as_deref()) {
        OpenVerdict::Refuse(why) => Err(crate::source::cdc::checkpoint_identity::log_gap(why)),
        OpenVerdict::Adopt(at) => {
            log::warn!(
                "pg cdc: slot '{slot}' is confirmed at {at}, past {own}, the checkpoint an earlier \
                 rivet wrote: that release advanced its slot over spans without a change before \
                 it saved the checkpoint, so the slot's position is taken this once and the \
                 checkpoint rewritten; from now on a slot past the checkpoint is refused"
            );
            adopted(&saved, &at).save(path)
        }
        OpenVerdict::Mark => confirmed_first(&saved).save(path),
        OpenVerdict::Keep => Ok(()),
    }
}

/// What the open does with a reused slot and the checkpoint `saved` at `own`.
#[derive(Debug, PartialEq)]
enum OpenVerdict {
    /// Another reader advanced the slot past a checkpoint this release wrote.
    Refuse(String),
    /// An older release's checkpoint behind its slot: take the slot's position, once.
    Adopt(String),
    /// An older release's checkpoint at or past its slot: rewrite it in this release's form.
    Mark,
    /// This release's checkpoint, the slot not past it.
    Keep,
}

fn open_verdict(slot: &str, saved: &Position, own: &str, confirmed: Option<&str>) -> OpenVerdict {
    let marked = saved.json().get(CONFIRMED_FIRST).and_then(|v| v.as_bool()) == Some(true);
    match (slot_advanced_by_another(slot, own, confirmed), marked) {
        (Some(why), true) => seeded!(
            "pg-foreign-advance-adopted",
            OpenVerdict::Adopt(confirmed.unwrap_or_default().to_string()),
            OpenVerdict::Refuse(why)
        ),
        (Some(_why), false) => seeded!(
            "pg-upgrade-false-log-gap",
            OpenVerdict::Refuse(_why),
            OpenVerdict::Adopt(confirmed.unwrap_or_default().to_string())
        ),
        (None, false) => OpenVerdict::Mark,
        (None, true) => OpenVerdict::Keep,
    }
}

/// `saved` moved to the slot's position `lsn`, in this release's form.
fn adopted(saved: &Position, lsn: &str) -> Position {
    let mut json = saved.json().clone();
    if let Some(obj) = json.as_object_mut() {
        obj.insert("lsn".to_string(), json!(lsn));
    }
    confirmed_first(&Position::new(json))
}

/// The refusal for a slot confirmed past `own`, the checkpoint's position; `None` when it is not.
fn slot_advanced_by_another(slot: &str, own: &str, confirmed: Option<&str>) -> Option<String> {
    let (own_n, conf_n) = (parse_lsn(own)?, parse_lsn(confirmed?)?);
    (conf_n > own_n
        && seeded!(
            "pg-slot-advanced-by-another-reader",
            own_n == u64::MAX,
            true
        ))
    .then(|| {
        format!(
            "pg cdc: slot '{slot}' is confirmed at {}, past {own}, the last position rivet \
             confirmed for this export (its checkpoint): another reader consumed the changes \
             between them from this slot (a second config of this cluster with the same slot \
             name), so they are lost to this stream. Give every config its own slot, \
             checkpoint and state, then {}",
            confirmed.unwrap_or_default(),
            crate::source::cdc::checkpoint_identity::RECOVER
        )
    })
}

/// Why a configured table no relation answers to cannot be captured.
pub(crate) fn absent_table_refusal(configured: &str) -> String {
    format!(
        "pg cdc: table `{configured}` does not exist on this server (as this connection's \
         search_path resolves it), so no change to it could ever be captured; nothing was read \
         or written. Create the table, or fix `table:` in the export, then re-run."
    )
}

/// Why a configured table the schema probe cannot prepare is refused.
pub(crate) fn table_probe_refusal(
    table: &str,
    sqlstate: Option<&str>,
    said: &str,
) -> anyhow::Error {
    if sqlstate == Some("42P01") {
        return crate::error::CodedError::new(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            absent_table_refusal(table),
        )
        .into();
    }
    crate::source::cdc::table_unreadable("pg", table, said)
}

/// Why this server cannot host the drain: `wal_level` below `logical`, or a bounded drain on a standby.
pub(crate) fn server_refusal(wal_level: &str, bounded: bool, in_recovery: bool) -> Option<String> {
    if wal_level != "logical" {
        return Some(format!(
            "pg cdc: this server runs wal_level = {wal_level}, and logical decoding needs \
             wal_level = logical; nothing was read or written. Set wal_level = logical in \
             postgresql.conf (ALTER SYSTEM SET wal_level = logical), restart the server, then \
             re-run."
        ));
    }
    (bounded && in_recovery).then(|| {
        "pg cdc: bounded (until_current) CDC cannot run on a PostgreSQL standby — it is in \
         recovery, where pg_current_wal_lsn() is unavailable; nothing was read or written. \
         Stream continuously (until_current: false; PostgreSQL 16+, where a standby can host a \
         logical slot) or point the source at the primary."
            .to_string()
    })
}

/// Where one decoded transaction goes, given its COMMIT LSN.
#[derive(Debug, PartialEq)]
enum TxDisposition {
    /// New and in-bound — yield it and advance the frontier.
    Yield,
    /// At/below the frontier: an un-acked re-read from a prior peek — drop.
    AlreadyYielded,
    /// Past the open-time bound: the bounded run stops here.
    PastBound,
}

/// Bound check FIRST: a commit past the ceiling must stop the run even on the very first peek (frontier still 0).
fn tx_disposition(commit_lsn: u64, frontier: u64, bound: Option<u64>) -> TxDisposition {
    if bound.is_some_and(|b| commit_lsn > b) {
        TxDisposition::PastBound
    } else if commit_lsn > frontier {
        TxDisposition::Yield
    } else {
        TxDisposition::AlreadyYielded
    }
}

/// What is owed downstream once a transaction is yielded.
fn owed_after_yield(
    owed_through: Option<u64>,
    commit_lsn: u64,
    buffered_rows: usize,
    spilled: bool,
) -> Option<u64> {
    if buffered_rows > 0 || spilled {
        Some(commit_lsn)
    } else {
        owed_through
    }
}

/// What stays owed downstream after the sink acked `acked`.
fn owed_after_ack(owed_through: Option<u64>, acked: Option<u64>) -> Option<u64> {
    owed_through.filter(|owed| {
        seeded!(
            "ack-past-unwritten",
            acked.is_none(),
            acked.is_none_or(|a| a < *owed)
        )
    })
}

/// Wire budget per peek: the sink's ack cadence (the part rollover), clamped to the `pg_logical_slot_peek_changes` int4 arg.
fn wire_budget(peek: crate::source::cdc::PeekBound) -> i32 {
    peek.rows_capped().min(i32::MAX as usize) as i32
}

/// A per-run nonce for the drain barrier.
fn run_nonce() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros())
    )
}

/// Is this `test_decoding` line rivet's own barrier, carrying `nonce`?
pub(crate) fn is_barrier_line(line: &str, nonce: &str) -> bool {
    let Some((head, content)) = td_line::message_parts(line) else {
        return false;
    };
    head.contains(&format!("prefix: {BARRIER_PREFIX},")) && content.trim_end() == nonce
}

/// Whether a WAL position lies past a bounded drain's open-time ceiling; an unbounded drain has none.
fn past_the_bound(lsn: &str, bound: Option<u64>) -> bool {
    tx_disposition(parse_lsn(lsn).unwrap_or(0), 0, bound) == TxDisposition::PastBound
}

/// Whether a `test_decoding` line is a logical message emitted outside any transaction.
pub(crate) fn is_standalone_message(line: &str) -> bool {
    line.starts_with("message: transactional: 0 ")
}

/// The prefix rivet stamps on its own logical messages.
pub(crate) use crate::source::postgres::pgoutput::BARRIER_PREFIX;

/// Parse a `pg_lsn` rendering `X/Y` (two hex halves of a 64-bit position) into a comparable `u64`.
fn parse_lsn(lsn: &str) -> Option<u64> {
    let (hi, lo) = lsn.split_once('/')?;
    let hi = u32::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u32::from_str_radix(lo.trim(), 16).ok()?;
    Some((u64::from(hi) << 32) | u64::from(lo))
}

/// Frame one raw `test_decoding` wire row — `(lsn, data)` — as spill bytes.
fn encode_wire_row(lsn: &str, data: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + lsn.len() + data.len());
    out.extend_from_slice(&(lsn.len() as u32).to_be_bytes());
    out.extend_from_slice(lsn.as_bytes());
    out.extend_from_slice(data.as_bytes());
    out
}

/// Inverse of [`encode_wire_row`].
fn decode_wire_row(rec: &[u8]) -> Result<(String, String)> {
    if rec.len() < 4 {
        crate::rivet_bail!(
            crate::error::codes::INTERNAL_SPILL,
            "pg cdc spill: a record of {} bytes holds no frame",
            rec.len()
        );
    }
    let n = u32::from_be_bytes([rec[0], rec[1], rec[2], rec[3]]) as usize;
    let end = 4usize
        .checked_add(n)
        .filter(|e| *e <= rec.len())
        .ok_or_else(|| {
            crate::error::coded(
                crate::error::codes::INTERNAL_SPILL,
                format!(
                    "pg cdc spill: a record declares a {n}-byte lsn the {}-byte frame \
                 does not hold",
                    rec.len()
                ),
            )
        })?;
    let lsn = std::str::from_utf8(&rec[4..end]).map_err(|e| {
        crate::error::coded(
            crate::error::codes::INTERNAL_SPILL,
            format!("pg cdc spill: the lsn is not utf-8: {e}"),
        )
    })?;
    let data = std::str::from_utf8(&rec[end..]).map_err(|e| {
        crate::error::coded(
            crate::error::codes::INTERNAL_SPILL,
            format!("pg cdc spill: the row is not utf-8: {e}"),
        )
    })?;
    Ok((lsn.to_string(), data.to_string()))
}

impl ChangeStream for PgChangeStream {
    fn row_image(&mut self, tables: &[String]) -> crate::source::cdc::RowImage {
        let images = Self::old_images_on(&mut self.client, tables);
        for (t, img) in &images {
            if let OldImage::NoKey(why) = img {
                log::warn!("postgres cdc: {}", no_old_key_warning(t, *why));
            }
        }
        row_image_verdict(&images)
    }

    fn retention_warnings(&mut self) -> Vec<String> {
        crate::source::cdc::pg_retention_warnings_on(&mut self.client, &self.slot)
    }

    fn checkpoint_of(&self, position: &Position) -> Position {
        confirmed_first(position)
    }

    fn engine(&self) -> crate::source::cdc::CdcEngine {
        crate::source::cdc::CdcEngine::Postgres
    }

    fn readable_columns(&self, configured: &str) -> Option<ReadableColumns> {
        self.readable.as_ref()?.get(configured).cloned()
    }

    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        loop {
            while self.pending.is_empty() && self.spooled.is_none() && !self.exhausted {
                if let Err(e) = self.fill() {
                    return Some(Err(e));
                }
            }
            if !self.pending.is_empty() {
                return self.pending.pop_front().map(Ok);
            }
            if self.spooled.is_some() {
                match self.next_spooled() {
                    Ok(Some(ev)) => return Some(Ok(ev)),
                    Ok(None) => continue,
                    Err(e) => return Some(Err(e)),
                }
            }
            self.release_empty_frontier();
            if self.exhausted {
                return None;
            }
        }
    }

    /// Advance the slot's `confirmed_flush_lsn` to the last durably-written change.
    fn ack(&mut self, position: &Position) -> Result<()> {
        let lsn = position
            .0
            .get("lsn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                crate::error::coded(
                    crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                    "pg cdc ack: position missing 'lsn'".to_string(),
                )
            })?
            .to_string();
        self.advance_slot(&lsn)?;
        self.owed_through = owed_after_ack(self.owed_through, parse_lsn(&lsn));
        Ok(())
    }
}

/// The relations a `test_decoding` TRUNCATE line names; empty if the line is not a TRUNCATE.
pub(crate) fn truncate_targets(data: &str) -> Vec<(String, String)> {
    td_line::truncate_relations(data).map_or_else(Vec::new, |relations| {
        split_top_level_commas(relations)
            .into_iter()
            .map(|q| split_qualified_ident(q.trim()))
            .collect()
    })
}

/// Refuse a TRUNCATE on a captured table, naming why re-running cannot fix it.
pub(crate) fn truncate_refusal_message(schema: &str, table: &str) -> String {
    let qualified = if schema.is_empty() {
        table.to_string()
    } else {
        format!("{schema}.{table}")
    };
    format!(
        "pg cdc: `{qualified}` was TRUNCATEd, and this reader cannot represent that as a \
         change. Skipping it would leave every row the truncate removed sitting in the \
         destination with no DELETE to retract it — the source empty, the destination \
         not, permanently, because those rows left the source without events and no \
         later capture can reconcile them. \
         \
         AND THIS RUN WILL NOT MOVE ON ITS OWN. The peek is non-consuming, so the \
         slot still sits on this commit: every later run meets it first and clean \
         changes queued BEHIND it are blocked too, while the un-acked slot pins WAL. \
         MEASURED — a truncate followed by an ordinary INSERT leaves the second run \
         failing identically. \
         \
         First get the slot past this commit: \
         SELECT pg_replication_slot_advance('<slot>', '<lsn past the truncate>'); \
         re-baselining ALONE leaves the capture wedged, because the slot has not moved. \
         Then: {}",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

/// The schema a BARE configured name means: the one the connection's `search_path` resolves it in (`to_regclass`), as every SQL statement of the run reads it, when that relation can carry a change event.
fn search_path_schema(client: &mut Client, cfg: &str) -> Result<Option<String>> {
    use anyhow::Context as _;
    if cfg.contains('.')
        || seeded!(
            "pg-cdc-bare-name-routes-every-schema",
            !cfg.is_empty(),
            false
        )
    {
        return Ok(None);
    }
    Ok(client
        .query_opt(
            "SELECT n.nspname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.oid = to_regclass($1) AND c.relkind IN ('r','m') AND c.relpersistence = 'p'",
            &[&cfg],
        )
        .with_context(|| format!("pg cdc: resolving `{cfg}` through the search_path"))?
        .map(|r| r.get(0)))
}

/// The catalog's `(schema, table)` for a configured name through `to_regclass`, for a base or partitioned table; `None` when it resolves nowhere.
pub(crate) fn search_path_relation<C: CatalogRows>(
    client: &mut C,
    cfg: &str,
) -> Result<Option<(String, String)>> {
    use anyhow::Context as _;
    let rows = client
        .text_rows(
            "SELECT n.nspname::text, c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.oid = to_regclass($1) AND c.relkind IN ('r','p') AND c.relpersistence = 'p'",
            cfg,
        )
        .with_context(|| format!("pg cdc: resolving `{cfg}` through the catalog"))?;
    Ok(relation_from_rows(&rows))
}

/// The `(schema, table)` the first catalog row names; a row short of either cell resolves nowhere.
pub(crate) fn relation_from_rows(rows: &[Vec<Option<String>>]) -> Option<(String, String)> {
    match rows.first().map(Vec::as_slice) {
        Some([Some(schema), Some(table), ..]) => Some((schema.clone(), table.clone())),
        None | Some([] | [_] | [None, ..] | [Some(_), None, ..]) => None,
    }
}

/// Whether a change of `schema.table` is captured.
pub(crate) fn routes(
    configured: &[String],
    bare_schema: &HashMap<String, String>,
    schema: &str,
    table: &str,
) -> bool {
    configured.is_empty()
        || configured.iter().any(|c| {
            crate::source::cdc::sink::table_matches(
                crate::source::cdc::CdcEngine::Postgres,
                c,
                schema,
                table,
            ) && bare_schema.get(c).is_none_or(|s| s == schema)
        })
}

/// Whether a change of `schema.table` reaches a configured name only as a bare name's same-named relation of another schema than the one the search_path resolved.
pub(crate) fn is_a_foreign_twin(
    configured: &[String],
    bare_schema: &HashMap<String, String>,
    schema: &str,
    table: &str,
) -> bool {
    !bare_schema.is_empty()
        && !routes(configured, bare_schema, schema, table)
        && routes(configured, &HashMap::new(), schema, table)
}

/// Is a TRUNCATE this run's problem?
pub(crate) fn truncate_is_ours(schema: &str, table: &str, configured: &[String]) -> bool {
    configured.is_empty()
        || configured.iter().any(|c| {
            crate::source::cdc::sink::table_matches(
                crate::source::cdc::CdcEngine::Postgres,
                c,
                schema,
                table,
            )
        })
}

/// Parse one `test_decoding` line into a canonical change, or `None` for the `BEGIN`/`COMMIT` markers and anything unrecognised.
pub(crate) fn parse_test_decoding(
    lsn: &str,
    data: &str,
    domains: &Domains,
) -> Result<Option<ChangeEvent>> {
    parse_test_decoding_keeping(lsn, data, domains, None)
}

type Keep<'a, 'r> = Option<&'r dyn Fn(&str, &str) -> Option<&'a [String]>>;

/// [`parse_test_decoding`] keeping only the columns `keep` names for the line's table (all when it names none); an update that changed none of them is no change.
pub(crate) fn parse_test_decoding_keeping<'a>(
    lsn: &str,
    data: &str,
    domains: &Domains,
    keep: Keep<'a, '_>,
) -> Result<Option<ChangeEvent>> {
    let Some(line) = td_line::tokenize(data) else {
        return Ok(None);
    };
    let (schema, table) = split_qualified_ident(line.relations);
    let op = match line.op {
        td_line::LineOp::Insert => ChangeOp::Insert,
        td_line::LineOp::Update => ChangeOp::Update,
        td_line::LineOp::Delete => ChangeOp::Delete,
    };
    let mut named = map_columns(line.new, domains);
    let mut old_named = line.old.map(|o| map_columns(o, domains));
    let keep = keep.and_then(|of| of(&schema, &table));
    if let Some(keep) = keep {
        let readable = |c: &ParsedColumn| keep.contains(&c.name);
        named.retain(readable);
        if let Some(o) = old_named.as_mut() {
            o.retain(readable);
        }
    }

    let unrecovered = recover_unchanged_toast(&mut named, old_named.as_deref());
    let undecodable = |cols: &[ParsedColumn]| {
        let bad: Vec<&str> = cols
            .iter()
            .filter(|c| c.unrepresentable)
            .map(|c| c.name.as_str())
            .collect();
        (!bad.is_empty()).then(|| {
            format!(
                "pg cdc: {schema}.{table}: column(s) [{}] hold a value rivet cannot decode \
                 faithfully (e.g. `infinity`, a BC date, a time of 24:00:00). rivet \
                 refuses rather than writing NULL, because a NULL here is \
                 indistinguishable from a genuinely absent value and every count and \
                 checksum would agree about the loss. Map such values to representable \
                 ones in the source, or exclude the column from capture.",
                bad.join(", ")
            )
        })
    };
    let infinity_poison = undecodable(&named);
    let before_poison = old_named.as_deref().and_then(undecodable);
    let poison = (!unrecovered.is_empty()).then(|| {
        format!(
            "pg cdc: {schema}.{table}: column(s) [{}] arrived as an unchanged-TOAST \
             datum with no pre-image value — logical decoding does not re-log an \
             externally stored value that an UPDATE leaves unchanged, so rivet cannot \
             recover it and refuses to write the literal `unchanged-toast-datum` marker \
             as data. Capture the full pre-image so the value is preserved: \
             ALTER TABLE {schema}.{table} REPLICA IDENTITY FULL;",
            unrecovered.join(", ")
        )
    });

    let names: std::sync::Arc<[String]> = named.iter().map(|c| c.name.clone()).collect();
    let cols: Vec<RivetValue> = named.into_iter().map(|c| c.value).collect();
    let old: Option<(Vec<RivetValue>, std::sync::Arc<[String]>)> = old_named.map(|o| {
        let kept: Vec<ParsedColumn> = o.into_iter().filter(|c| !c.toast_unchanged).collect();
        let old_names = kept.iter().map(|c| c.name.clone()).collect();
        (kept.into_iter().map(|c| c.value).collect(), old_names)
    });
    let (before, after, image_names, before_names) = match op {
        ChangeOp::Delete => (Some(cols), None, Some(names), None),
        ChangeOp::Update => {
            let (before, before_names) = old.unzip();
            let (before, before_names) = seeded!(
                "absent-old-cell-read-as-moved-key",
                (
                    before.zip(before_names).map(|(v, bn)| {
                        names
                            .iter()
                            .map(|n| match bn.iter().position(|b| b == n) {
                                Some(i) => v[i].clone(),
                                None => RivetValue::Null,
                            })
                            .collect::<Vec<RivetValue>>()
                    }),
                    None
                ),
                (before, before_names)
            );
            (before, Some(cols), Some(names), before_names)
        }
        ChangeOp::Insert => (None, Some(cols), Some(names), None),
    };
    let poisoned = [&poison, &infinity_poison, &before_poison]
        .iter()
        .any(|p| p.is_some());
    if keep.is_some()
        && seeded!("column-grants-pg-hidden-only-update-delivered", false, true)
        && op == ChangeOp::Update
        && !poisoned
        && let (Some(before), Some(after), Some(names), Some(before_names)) =
            (&before, &after, &image_names, &before_names)
        && super::column_grants::only_unread_columns_changed(names, after, before_names, before)
    {
        return Ok(None);
    }
    Ok(Some(ChangeEvent {
        op,
        schema,
        table,
        before,
        after,
        image_names,
        position: Position::new(json!({ "lsn": lsn })),
        committed: false,
        seq: 0,
        poison: poison.or(infinity_poison),
        row_id: None,
        before_names,
        before_poison,
        image_labels: None,
    }))
}

/// What a table's replica identity puts in the old image of an UPDATE and a DELETE.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum OldImage {
    WholeRow,
    PrimaryKey,
    IndexColumns,
    NoKey(NoOldKey),
}

/// Why a table's changes carry no old image at all.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NoOldKey {
    IdentityNothing,
    DroppedIdentityIndex,
    DeferrableKey,
    NoPrimaryKey,
}

/// The old image `relreplident` gives: `pk_immediate` is the primary key's `indimmediate`, `identity_index` whether an `indisreplident` index exists.
pub(crate) fn old_image(
    relreplident: &str,
    pk_immediate: Option<bool>,
    identity_index: bool,
) -> OldImage {
    match (relreplident, pk_immediate) {
        ("f", _) => OldImage::WholeRow,
        ("i", _) if identity_index => OldImage::IndexColumns,
        ("i", _) => OldImage::NoKey(NoOldKey::DroppedIdentityIndex),
        ("n", _) => OldImage::NoKey(NoOldKey::IdentityNothing),
        (_, Some(true)) => OldImage::PrimaryKey,
        (_, Some(false)) => OldImage::NoKey(NoOldKey::DeferrableKey),
        (_, None) => OldImage::NoKey(NoOldKey::NoPrimaryKey),
    }
}

/// The run-start warning for a captured table whose changes carry no old image, with its remedy.
pub(crate) fn no_old_key_warning(table: &str, why: NoOldKey) -> String {
    let (cause, remedy) = match why {
        NoOldKey::IdentityNothing => (
            "has REPLICA IDENTITY NOTHING".to_string(),
            format!(
                "Run `ALTER TABLE {table} REPLICA IDENTITY FULL;`, or `ALTER TABLE {table} \
                 REPLICA IDENTITY DEFAULT;` if it has a primary key"
            ),
        ),
        NoOldKey::DroppedIdentityIndex => (
            "has REPLICA IDENTITY USING INDEX on an index that no longer exists, which PostgreSQL \
             treats as NOTHING"
                .to_string(),
            format!(
                "Run `ALTER TABLE {table} REPLICA IDENTITY FULL;`, or `ALTER TABLE {table} \
                 REPLICA IDENTITY USING INDEX <index>;` on an existing unique index of NOT NULL \
                 columns"
            ),
        ),
        NoOldKey::DeferrableKey => (
            "has a DEFERRABLE primary key, which PostgreSQL does not use as the replica identity"
                .to_string(),
            format!(
                "Run `ALTER TABLE {table} REPLICA IDENTITY FULL;` (the key stays deferrable), or \
                 `ALTER TABLE {table} REPLICA IDENTITY USING INDEX <index>;` on a non-deferrable \
                 unique index of the key columns (that index checks uniqueness row by row, so it \
                 rejects a statement such as `SET id = id + 1`)"
            ),
        ),
        NoOldKey::NoPrimaryKey => (
            "has no primary key and REPLICA IDENTITY DEFAULT".to_string(),
            format!("Run `ALTER TABLE {table} REPLICA IDENTITY FULL;`, or give it a primary key"),
        ),
    };
    format!(
        "table {table} {cause}, so an UPDATE carries no old values and a DELETE carries no \
         columns at all: a DELETE retracts nothing downstream, and an UPDATE that changes the key \
         leaves the old key live. {remedy}"
    )
}

/// The delete-image verdict over the captured tables whose DELETE carries only a key.
pub(crate) fn row_image_verdict(images: &[(String, OldImage)]) -> crate::source::cdc::RowImage {
    use crate::source::cdc::RowImage;
    let keyed: Vec<String> = images
        .iter()
        .filter_map(|(t, img)| match img {
            OldImage::PrimaryKey => Some(format!("{t} (its primary key)")),
            OldImage::IndexColumns => Some(format!(
                "{t} (its replica identity index's columns; an UPDATE that changes a key column \
                 outside that index stays one update)"
            )),
            OldImage::WholeRow | OldImage::NoKey(_) => None,
        })
        .collect();
    if keyed.is_empty() {
        return RowImage::Whole;
    }
    RowImage::KeyOnlyDeletes {
        why: format!(
            "{} of the captured table(s) put only a key in a DELETE, while INSERT and UPDATE keep \
             their whole new-row image: {}. `ALTER TABLE <t> REPLICA IDENTITY FULL` if the \
             before-image matters",
            keyed.len(),
            keyed.join(", ")
        ),
    }
}

/// One parsed `test_decoding` column: name, typed value, and the wire forms that decide whether it may be delivered.
struct ParsedColumn {
    name: String,
    value: RivetValue,
    toast_unchanged: bool,
    unrepresentable: bool,
}

/// Whether batch delivers an array of this catalog type as the server's text, which CDC then keeps as the raw literal.
fn array_is_server_text(oid: u32, elem_typtype: &str) -> bool {
    let server_text = match postgres::types::Type::from_oid(oid) {
        Some(t) => super::arrow_convert::pg_server_text(&t),
        None => elem_typtype != "e",
    };
    server_text && !seeded!("cdc-array-text-parsed-as-list", true, false)
}

/// Every domain's label mapped to its base type's label, and every array label batch delivers as the server's text mapped to `text`.
fn domain_map(
    domains: impl IntoIterator<Item = (String, String)>,
    arrays: impl IntoIterator<Item = (u32, String, String)>,
) -> Domains {
    let mut map: Domains = domains.into_iter().collect();
    for (oid, label, elem_typtype) in arrays {
        if array_is_server_text(oid, &elem_typtype) {
            map.insert(label, "text".to_string());
        }
    }
    map
}

/// The catalog's domains and arrays read on the reader's session, as `domain_map` takes them.
fn load_domains(client: &mut Client) -> Result<Domains> {
    let domains: Vec<(String, String)> = client
        .query(
            "SELECT format_type(oid, NULL), format_type(typbasetype, NULL) \
             FROM pg_type WHERE typtype = 'd'",
            &[],
        )?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    let arrays: Vec<(u32, String, String)> = client
        .query(
            "SELECT a.oid, format_type(a.oid, NULL), e.typtype::text \
               FROM pg_type a JOIN pg_type e ON e.oid = a.typelem \
              WHERE a.typcategory = 'A' AND a.typelem <> 0",
            &[],
        )?
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    Ok(domain_map(domains, arrays))
}

/// The built-in type a (possibly nested) domain label decodes as; any other label as is.
fn domain_base<'a>(domains: &'a Domains, mut typ: &'a str) -> &'a str {
    while let Some(base) = domains.get(typ) {
        typ = base;
    }
    typ
}

/// Type each tokenized cell: a typed value, the unchanged-TOAST marker, an unrepresentable value, or a refused type.
fn map_columns(raw: Vec<td_line::RawColumn>, domains: &Domains) -> Vec<ParsedColumn> {
    raw.into_iter()
        .map(|c| {
            let base = domain_base(domains, &c.typ);
            let toast_unchanged = !c.quoted && c.value == "unchanged-toast-datum";
            let mapped = map_pg_value(base, &c.value, c.quoted);
            let unrepresentable = mapped.is_none() && !toast_unchanged;
            ParsedColumn {
                name: c.name,
                value: mapped.unwrap_or(RivetValue::Null),
                toast_unchanged,
                unrepresentable,
            }
        })
        .collect()
}

/// Substitute each NEW-tuple `unchanged-toast-datum` column with the real value from the pre-image (`before`, present under REPLICA IDENTITY FULL), matched by column NAME.
fn recover_unchanged_toast(
    after: &mut [ParsedColumn],
    before: Option<&[ParsedColumn]>,
) -> Vec<String> {
    let mut unrecovered = Vec::new();
    for col in after.iter_mut() {
        if !col.toast_unchanged {
            continue;
        }
        let recovered = before.and_then(|b| {
            b.iter()
                .find(|pre| pre.name == col.name && !pre.toast_unchanged)
                .map(|pre| pre.value.clone())
        });
        match recovered {
            Some(v) => {
                col.value = v;
                col.toast_unchanged = false;
            }
            None => unrecovered.push(col.name.clone()),
        }
    }
    unrecovered
}

/// Map a `test_decoding` `(type, value)` to a typed [`RivetValue`].
fn map_pg_value(typ: &str, val: &str, quoted: bool) -> Option<RivetValue> {
    if !quoted && val == "null" {
        return Some(RivetValue::Null);
    }
    if let Some(inner) = typ.strip_suffix("[]") {
        if !array_elem_is_listed(inner) {
            return Some(RivetValue::Bytes(val.as_bytes().to_vec()));
        }
        return Some(parse_pg_array_literal(inner, val).map_or_else(
            || RivetValue::Bytes(val.as_bytes().to_vec()),
            RivetValue::Array,
        ));
    }
    let t = typ;
    if t == "integer" || t == "bigint" || t == "smallint" || t == "oid" {
        return val.parse::<i64>().ok().map(RivetValue::Int);
    }
    if t == "boolean" {
        return Some(RivetValue::Bool(val == "t" || val == "true"));
    }
    if t == "double precision" || t == "real" {
        return val.parse::<f64>().ok().map(RivetValue::Float);
    }
    if t.starts_with("timestamp") {
        return parse_pg_timestamp(val);
    }
    if t == "time" || t == "time without time zone" {
        return parse_pg_time_micros(val).map(RivetValue::TimeMicros);
    }
    if t == "interval" {
        return Some(
            parse_pg_interval(val)
                .map(|(months, days, us)| {
                    RivetValue::Bytes(
                        crate::source::postgres::arrow_convert::pg_interval_to_iso8601(
                            months, days, us,
                        )
                        .into_bytes(),
                    )
                })
                .unwrap_or_else(|| RivetValue::Bytes(val.as_bytes().to_vec())),
        );
    }
    if t == "date" {
        return chrono::NaiveDate::parse_from_str(val, "%Y-%m-%d")
            .ok()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(RivetValue::DateTime);
    }
    if t == "uuid" {
        return decode_hex(&val.replace('-', ""))
            .filter(|b| b.len() == 16)
            .map(RivetValue::Bytes);
    }
    if t == "bit" || t == "bit varying" {
        return seeded!(
            "cdc-bit-literal-delivered-as-server-text",
            Some(val),
            val.strip_prefix("B'").and_then(|v| v.strip_suffix('\''))
        )
        .map(|bits| RivetValue::Bytes(bits.as_bytes().to_vec()));
    }
    if t == "bytea" {
        if let Some(hex) = val.strip_prefix("\\x")
            && let Some(b) = decode_hex(hex)
        {
            return Some(RivetValue::Bytes(b));
        }
        return Some(RivetValue::Bytes(val.as_bytes().to_vec()));
    }
    Some(RivetValue::Bytes(val.as_bytes().to_vec()))
}

/// True when an array of `inner` becomes a CDC list column; a `numeric` array travels as the server's own literal text.
fn array_elem_is_listed(inner: &str) -> bool {
    !inner.starts_with("numeric")
}

/// Parse a PG array literal (`{alpha,"with,comma","he said \"hi\"",NULL}`) into element values, mapped through [`map_pg_value`] with the element type.
fn parse_pg_array_literal(inner_type: &str, val: &str) -> Option<Vec<RivetValue>> {
    let body = val.strip_prefix('{')?.strip_suffix('}')?;
    if body.is_empty() {
        return Some(Vec::new());
    }
    let b = body.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i <= b.len() {
        if b.get(i) == Some(&b'"') {
            let mut elem = String::new();
            i += 1;
            while i < b.len() && b[i] != b'"' {
                if b[i] == b'\\' && i + 1 < b.len() {
                    let n = utf8_len(b[i + 1]);
                    let end = (i + 1 + n).min(b.len());
                    elem.push_str(&body[i + 1..end]);
                    i = end;
                } else {
                    let n = utf8_len(b[i]);
                    elem.push_str(&body[i..i + n]);
                    i += n;
                }
            }
            i += 1;
            out.push(map_pg_value(inner_type, &elem, true)?);
            if b.get(i) == Some(&b',') {
                i += 1;
            } else {
                break;
            }
        } else if b.get(i) == Some(&b'{') {
            return None;
        } else {
            let end = body[i..].find(',').map(|p| i + p).unwrap_or(body.len());
            let tok = &body[i..end];
            out.push(if tok == "NULL" {
                RivetValue::Null
            } else {
                map_pg_value(inner_type, tok, false)?
            });
            if end == body.len() {
                break;
            }
            i = end + 1;
        }
    }
    Some(out)
}

/// Parse "HH:MM:SS[.ffffff]" into microseconds since midnight.
fn parse_pg_time_micros(val: &str) -> Option<i64> {
    let (hms, frac) = match val.split_once('.') {
        Some((h, f)) => (h, f),
        None => (val, ""),
    };
    let mut parts = hms.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    if parts.next().is_some()
        || !(0..24).contains(&h)
        || !(0..60).contains(&m)
        || !(0..60).contains(&s)
    {
        return None;
    }
    let us: i64 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<6}").get(..6)?.parse().ok()?
    };
    Some(((h * 3600 + m * 60 + s) * 1_000_000) + us)
}

/// Parse PostgreSQL's `postgres`-style interval text rendering.
fn parse_pg_interval(val: &str) -> Option<(i32, i32, i64)> {
    let (mut months, mut days, mut micros) = (0i32, 0i32, 0i64);
    let mut tokens = val.split_whitespace().peekable();
    while let Some(tok) = tokens.next() {
        if tok.contains(':') {
            let (sign, rest) = match tok.strip_prefix('-') {
                Some(r) => (-1i64, r),
                None => (1i64, tok),
            };
            let t = parse_pg_time_micros_unbounded(rest)?;
            micros = t.checked_mul(sign)?;
            continue;
        }
        let n: i32 = tok.parse().ok()?;
        match tokens.next()? {
            u if u.starts_with("year") => months = months.checked_add(n.checked_mul(12)?)?,
            u if u.starts_with("mon") => months = months.checked_add(n)?,
            u if u.starts_with("day") => days = days.checked_add(n)?,
            _ => return None,
        }
    }
    Some((months, days, micros))
}

/// As [`parse_pg_time_micros`] but without the 24h bound — an interval's time component may exceed a day (e.g.
fn parse_pg_time_micros_unbounded(val: &str) -> Option<i64> {
    let (hms, frac) = match val.split_once('.') {
        Some((h, f)) => (h, f),
        None => (val, ""),
    };
    let mut parts = hms.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    let us: i64 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<6}").get(..6)?.parse().ok()?
    };
    let secs = h
        .checked_mul(3600)?
        .checked_add(m.checked_mul(60)?)?
        .checked_add(s)?;
    secs.checked_mul(1_000_000)?.checked_add(us)
}

/// Decode an even-length hex string to bytes; `None` on any non-hex input.
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.is_ascii() || !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Parse a PostgreSQL timestamp rendering (`YYYY-MM-DD HH:MM:SS[.ffffff][±TZ]`).
fn parse_pg_timestamp(val: &str) -> Option<RivetValue> {
    let v = val.trim_end();
    for fmt in ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%#z"] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(v, fmt) {
            return Some(RivetValue::DateTime(dt.naive_utc()));
        }
    }
    let naive = v.trim_end_matches('Z');
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(naive, fmt) {
            return Some(RivetValue::DateTime(dt));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    /// A failed slot drop is retried, pausing once per retry, until it succeeds.
    #[test]
    fn a_slot_drop_that_fails_then_succeeds_is_retried_until_it_does() {
        let (mut calls, mut pauses, mut noted) = (0u32, 0u32, 0u32);
        let out = retry_drop(
            || {
                calls += 1;
                if calls <= 3 { Err("held") } else { Ok(()) }
            },
            |_| noted += 1,
            || pauses += 1,
        );
        assert_eq!(out, Ok(()));
        assert_eq!((calls, pauses, noted), (4, 3, 3));
    }

    /// A slot drop that never succeeds is tried once plus every retry, then its last error is returned.
    #[test]
    fn a_slot_drop_that_never_succeeds_gives_up_after_its_retries_with_the_last_error() {
        let (mut calls, mut pauses) = (0u32, 0u32);
        let out = retry_drop(
            || {
                calls += 1;
                Err(calls)
            },
            |_| {},
            || {
                pauses += 1;
                assert!(pauses <= DROP_SLOT_RETRIES + 5, "the retry loop never ends");
            },
        );
        assert_eq!(out, Err(DROP_SLOT_RETRIES + 1));
        assert_eq!((calls, pauses), (DROP_SLOT_RETRIES + 1, DROP_SLOT_RETRIES));
    }

    /// A slot drop that succeeds at once neither pauses nor notes.
    #[test]
    fn a_slot_drop_that_succeeds_at_once_does_not_pause() {
        let (mut pauses, mut noted) = (0u32, 0u32);
        let out: std::result::Result<(), &str> =
            retry_drop(|| Ok(()), |_| noted += 1, || pauses += 1);
        assert_eq!(out, Ok(()));
        assert_eq!((pauses, noted), (0, 0));
    }

    /// A bare name routes only the schema the search_path resolved it to.
    #[test]
    fn a_bare_name_routes_only_its_search_path_schema() {
        let cfg = vec![
            "orders".to_string(),
            "audit.log".to_string(),
            "loose".to_string(),
        ];
        let bare: HashMap<String, String> = [("orders".to_string(), "public".to_string())].into();
        assert!(routes(&cfg, &bare, "public", "orders"));
        assert!(
            !routes(&cfg, &bare, "sales", "orders"),
            "a same-named table of another schema"
        );
        assert!(routes(&cfg, &bare, "audit", "log"));
        assert!(!routes(&cfg, &bare, "public", "log"));
        assert!(
            routes(&cfg, &bare, "anywhere", "loose"),
            "unresolved: routes as before"
        );
        assert!(
            routes(&[], &bare, "x", "y"),
            "no configured table captures everything"
        );
        assert!(is_a_foreign_twin(&cfg, &bare, "sales", "orders"));
        assert!(!is_a_foreign_twin(&cfg, &bare, "public", "orders"));
        assert!(
            !is_a_foreign_twin(&cfg, &bare, "public", "unrelated"),
            "another table is the sink to drop"
        );
        assert!(
            !is_a_foreign_twin(&cfg, &HashMap::new(), "sales", "orders"),
            "nothing resolved: as before"
        );
    }

    /// The barrier line, against what the SERVER actually renders.
    #[test]
    fn the_server_is_refused_for_wal_level_or_a_bounded_drain_in_recovery() {
        assert_eq!(server_refusal("logical", true, false), None);
        assert_eq!(server_refusal("logical", false, true), None);
        assert_eq!(server_refusal("logical", false, false), None);
        assert_eq!(
            server_refusal("replica", false, false).as_deref(),
            Some(
                "pg cdc: this server runs wal_level = replica, and logical decoding needs \
                 wal_level = logical; nothing was read or written. Set wal_level = logical in \
                 postgresql.conf (ALTER SYSTEM SET wal_level = logical), restart the server, then \
                 re-run."
            )
        );
        assert!(
            server_refusal("minimal", true, true)
                .unwrap()
                .contains("wal_level = minimal")
        );
        assert_eq!(
            server_refusal("logical", true, true).as_deref(),
            Some(
                "pg cdc: bounded (until_current) CDC cannot run on a PostgreSQL standby — it is in \
                 recovery, where pg_current_wal_lsn() is unavailable; nothing was read or written. \
                 Stream continuously (until_current: false; PostgreSQL 16+, where a standby can \
                 host a logical slot) or point the source at the primary."
            )
        );
    }

    /// The absent-table refusal names the configured string and both remedies.
    #[test]
    fn an_absent_table_is_named_with_its_remedy() {
        assert_eq!(
            absent_table_refusal("public.orders"),
            "pg cdc: table `public.orders` does not exist on this server (as this connection's \
             search_path resolves it), so no change to it could ever be captured; nothing was \
             read or written. Create the table, or fix `table:` in the export, then re-run."
        );
    }

    /// A position is past the bound only when there is one and it lies strictly beyond it.
    #[test]
    fn a_position_is_past_the_bound_only_strictly_beyond_one() {
        assert!(past_the_bound("0/11", Some(0x10)));
        assert!(!past_the_bound("0/10", Some(0x10)));
        assert!(!past_the_bound("0/F", Some(0x10)));
        assert!(!past_the_bound("FF/0", None));
        assert!(!past_the_bound("not an lsn", Some(0x10)));
    }

    /// An undefined table is the coded absent-table refusal; any other probe failure keeps the server's words.
    #[test]
    fn a_table_the_probe_cannot_prepare_is_absent_only_by_its_sqlstate() {
        let absent = table_probe_refusal(
            "orders",
            Some("42P01"),
            "relation \"orders\" does not exist",
        );
        assert_eq!(
            crate::error::error_code(&absent),
            Some("RIVET_SOURCE_CDC_PREREQUISITE")
        );
        assert_eq!(absent.to_string(), absent_table_refusal("orders"));
        for other in [Some("42501"), None] {
            let denied = table_probe_refusal("orders", other, "permission denied for table orders");
            assert_eq!(
                crate::error::error_code(&denied),
                Some("RIVET_SOURCE_QUERY_FAILED")
            );
            assert_eq!(
                denied.to_string(),
                "pg cdc: cannot read `orders` (permission denied for table orders); nothing was \
                 read or written."
            );
        }
    }

    /// Only a message outside a transaction is a span of its own; a transactional one rides its commit.
    #[test]
    fn only_a_non_transactional_message_stands_alone() {
        assert!(is_standalone_message(
            "message: transactional: 0 prefix: rivet, sz: 20 content:rivet-barrier-abc123"
        ));
        assert!(is_standalone_message(
            "message: transactional: 0 prefix: other-tool, sz: 1 content:x"
        ));
        assert!(!is_standalone_message(
            "message: transactional: 1 prefix: rivet, sz: 1 content:x"
        ));
        assert!(!is_standalone_message(
            "table public.t: INSERT: v[text]:'message: transactional: 0 prefix'"
        ));
    }

    #[test]
    fn only_rivets_own_barrier_with_this_nonce_ends_a_drain() {
        let real = "message: transactional: 0 prefix: rivet, sz: 20 content:rivet-barrier-abc123";
        assert!(is_barrier_line(real, "rivet-barrier-abc123"));

        assert!(
            !is_barrier_line(real, "rivet-barrier-DIFFERENT"),
            "the nonce is the only thing separating one cycle's barrier from the next"
        );

        assert!(
            !is_barrier_line(
                "message: transactional: 0 prefix: someone_else, sz: 20 content:rivet-barrier-abc123",
                "rivet-barrier-abc123"
            ),
            "a foreign prefix must never end a rivet drain, whatever it carries"
        );

        assert!(!is_barrier_line(
            "message: transactional: 0 prefix: rivet_other, sz: 20 content:rivet-barrier-abc123",
            "rivet-barrier-abc123"
        ));

        for line in [
            "table public.t: INSERT: id[integer]:1",
            "BEGIN 12345",
            "COMMIT 12345",
            "table public.t: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:2",
        ] {
            assert!(!is_barrier_line(line, "rivet-barrier-abc123"), "{line}");
        }

        assert!(
            !is_barrier_line(
                "table public.t: INSERT: id[integer]:1 note[text]:'prefix: rivet, sz: 20 content:rivet-barrier-abc123'",
                "rivet-barrier-abc123"
            ),
            "a ROW is never a barrier, however its values are spelled"
        );
        assert!(
            !is_barrier_line(
                "table public.t: INSERT: note[text]:'prefix: rivet, sz: 1 content:x'",
                "x"
            ),
            "even a minimal row rendering keeps its closing quote"
        );

        assert!(
            !is_barrier_line(
                "message: transactional: 0 prefix: rivet, sz: 24 content:rivet-barrier-abc123-x",
                "rivet-barrier-abc123"
            ),
            "a prefix-match on the content would let a longer nonce end the wrong drain"
        );
    }

    /// Every arm of the infinity predicate, and the doubled-quote path of both scanners.
    #[test]
    fn a_value_the_decoder_cannot_read_is_unrepresentable_never_null() {
        for (typ, val) in [
            ("date", "0044-03-15 BC"),
            ("timestamp without time zone", "0044-03-15 00:00:00 BC"),
            ("timestamp with time zone", "1800-01-01 00:00:00+09:18:59"),
            ("time without time zone", "24:00:00"),
            ("integer", "12x"),
        ] {
            assert_eq!(map_pg_value(typ, val, false), None, "{typ} {val}");
        }
        assert_eq!(map_pg_value("date", "null", false), Some(RivetValue::Null));
        let cols = map_columns(
            td_line::columns("d[date]:'0044-03-15 BC' n[integer]:unchanged-toast-datum").0,
            &Domains::new(),
        );
        assert!(cols[0].unrepresentable && cols[0].value == RivetValue::Null);
        assert!(
            !cols[1].unrepresentable,
            "an unchanged-TOAST marker is recovered elsewhere, not refused as unreadable"
        );
    }

    #[test]
    fn the_infinity_predicate_and_the_escaped_quote_path_are_graded() {
        let inf = |typ: &str, val: &str| super::map_pg_value(typ, val, true).is_none();

        assert!(inf("timestamp with time zone", "infinity"));
        assert!(inf("timestamp without time zone", "-infinity"));
        assert!(inf("date", "infinity"));
        assert!(inf("date", "-infinity"));

        assert!(!inf("timestamp with time zone", "2026-01-02 03:04:05+00"));
        assert!(!inf("text", "infinity"));
        assert!(!inf("datemark", "infinity"));

        for typ in ["integer", "bigint", "smallint", "oid"] {
            assert_eq!(
                map_pg_value(typ, "42", false),
                Some(RivetValue::Int(42)),
                "{typ}"
            );
        }
        assert_eq!(
            map_pg_value("boolean", "t", false),
            Some(RivetValue::Bool(true))
        );
        assert_eq!(
            map_pg_value("boolean", "true", false),
            Some(RivetValue::Bool(true))
        );
        assert_eq!(
            map_pg_value("boolean", "f", false),
            Some(RivetValue::Bool(false))
        );
        for typ in ["double precision", "real"] {
            assert_eq!(
                map_pg_value(typ, "1.5", false),
                Some(RivetValue::Float(1.5)),
                "{typ}"
            );
        }
        for typ in ["numeric(10,2)", "decimal"] {
            assert_eq!(
                map_pg_value(typ, "1.50", false),
                Some(RivetValue::Bytes(b"1.50".to_vec())),
                "{typ}"
            );
        }

        let line = "table public.\"o''brien\": TRUNCATE: (no-flags)";
        assert!(
            super::td_line::find_outside_quotes(line, ": TRUNCATE:").is_some(),
            "a doubled quote is an escape, not a terminator — mutating the `i += 2` \
             that skips it desynchronises the scanner and the marker goes unseen, \
             which is how one apostrophe blinded the whole TRUNCATE guard before"
        );
        let hits = super::truncate_targets("table public.t: TRUNCATE: (no-flags)");
        assert_eq!(
            hits,
            vec![("public".to_string(), "t".to_string())],
            "and the ordinary form must still resolve"
        );
    }

    use super::*;
    use postgres::NoTls;

    /// The TRUNCATE recogniser and its addressing, graded offline.
    #[test]
    fn a_truncate_is_recognised_across_a_relation_list_and_refused_only_for_ours() {
        let one = |d: &str| truncate_targets(d);

        assert_eq!(
            one("table public.orders: TRUNCATE: (no-flags)"),
            vec![("public".to_string(), "orders".to_string())],
            "the exact line test_decoding emits for a single relation (measured on pg14)"
        );
        assert_eq!(
            one("table public.a, public.b: TRUNCATE: (no-flags)"),
            vec![
                ("public".to_string(), "a".to_string()),
                ("public".to_string(), "b".to_string())
            ],
            "a multi-relation TRUNCATE must yield EVERY relation — missing the list \
             is how the first version of this guard let the statement through"
        );
        assert_eq!(
            one("table \"My Schema\".\"Odd.Name\": TRUNCATE: (no-flags)"),
            vec![("My Schema".to_string(), "Odd.Name".to_string())],
            "a quoted identifier unquotes like every other wire identity (#279) — a \
             dot INSIDE quotes belongs to the name"
        );
        assert_eq!(
            one("table public.\"odd,name\": TRUNCATE: (no-flags)"),
            vec![("public".to_string(), "odd,name".to_string())],
            "splitting on a quoted comma would invent two relations that do not exist"
        );
        assert_eq!(
            one("table public.\"od\"\"d\", public.b: TRUNCATE: (no-flags)"),
            vec![
                ("public".to_string(), "od\"d".to_string()),
                ("public".to_string(), "b".to_string())
            ],
            "a doubled quote is an ESCAPE; mishandling it swallows the separator and \
             loses every relation after it — including possibly ours"
        );
        assert_eq!(
            one("table public.\"a\"\",b\": TRUNCATE: (no-flags)"),
            vec![("public".to_string(), "a\",b".to_string())],
            "escape then comma: still ONE relation"
        );

        assert_eq!(
            one("table public.\"a\"\"\", public.b: TRUNCATE: (no-flags)"),
            vec![
                ("public".to_string(), "a\"".to_string()),
                ("public".to_string(), "b".to_string())
            ],
            "a name ending in an escaped quote must still close, or the following \
             comma is read as part of it and every later relation disappears"
        );

        assert_eq!(
            one("table public.tord, public.\"o'brien\": TRUNCATE: (no-flags)"),
            vec![
                ("public".to_string(), "tord".to_string()),
                ("public".to_string(), "o'brien".to_string())
            ],
            "a quoted identifier containing an apostrophe must not blind the marker \
             scan — the truncate is then silently dropped and the destination keeps \
             rows the source no longer has"
        );
        assert!(
            one("table public.t: INSERT: v[text]:'not a : TRUNCATE: marker'").is_empty(),
            "a marker inside a string literal is data, not an operation"
        );

        assert!(
            one("table public.orders: INSERT: id[integer]:1").is_empty(),
            "an ordinary change must not be mistaken for a truncate"
        );
        assert!(one("COMMIT 123").is_empty(), "markers are not truncates");
        assert!(
            one("table public.t: DELETE: id[integer]:1").is_empty(),
            "DELETE stays a change — the guard is for the op with NO rows"
        );

        let cfg = vec!["public.orders".to_string()];
        assert!(
            truncate_is_ours("public", "orders", &cfg),
            "our own table must fail the run — the divergence it leaves is permanent"
        );
        assert!(
            !truncate_is_ours("public", "audit", &cfg),
            "another table's truncate must NOT fail this run: the slot decodes the \
             whole database, so refusing would make one truncated table an outage \
             for every export on it (the MySQL undecodable-rows lesson, #281)"
        );
        assert!(
            truncate_is_ours("public", "audit", &[]),
            "`rivet cdc` with no --table captures whatever the stream emits"
        );
        assert!(
            truncate_is_ours("otherschema", "orders", &["orders".to_string()]),
            "a bare config name matches any schema, exactly as sink::table_matches \
             routes it — a guard asking a different question protects the wrong set"
        );
        let multi = one("table public.other, public.orders: TRUNCATE: (no-flags)");
        assert!(
            multi.iter().any(|(s, t)| truncate_is_ours(s, t, &cfg)),
            "a truncate naming ours SECOND must still be found: {multi:?}"
        );
        assert!(
            !one("table public.x, public.y: TRUNCATE: (no-flags)")
                .iter()
                .any(|(s, t)| truncate_is_ours(s, t, &cfg)),
            "a list naming only OTHER tables must not fail this run"
        );

        let msg = truncate_refusal_message("public", "orders");
        assert!(
            msg.contains("public.orders")
                && msg.ends_with(crate::source::cdc::checkpoint_identity::RECOVER),
            "the refusal must name the table AND the re-snapshot that recovers from \
             it; a bail with no way forward just moves the operator's problem: {msg}"
        );
        assert!(
            truncate_refusal_message("", "orders").contains("`orders`"),
            "a schema-less identity must not render a leading dot"
        );
    }

    #[test]
    fn classify_routing_refuses_only_the_relations_no_event_can_ever_name() {
        let rel = |kind: char, children: &[&str]| RelationRouting {
            configured: "public.t",
            relkind: kind,
            relpersistence: 'p',
            resolved: ("public".to_string(), "t".to_string()),
            leaves: children.iter().map(|c| c.to_string()).collect(),
            children: children.iter().map(|c| c.to_string()).collect(),
        };

        let RoutingVerdict::Never(why) = classify_routing(&rel('p', &["public.t_2026_01"])) else {
            panic!("a partitioned parent can never be routed — it must be refused");
        };
        assert!(
            why.contains("public.t_2026_01"),
            "the refusal must name the partitions, because listing them IS the \
             remediation: {why}"
        );

        let sub = RelationRouting {
            configured: "public.ml",
            relkind: 'p',
            relpersistence: 'p',
            resolved: ("public".to_string(), "ml".to_string()),
            children: vec!["public.ml_2026".to_string()],
            leaves: vec!["public.ml_2026_01".to_string()],
        };
        let RoutingVerdict::Never(why) = classify_routing(&sub) else {
            panic!("a sub-partitioned parent is still unroutable");
        };
        assert!(
            why.contains("public.ml_2026_01") && !why.contains("public.ml_2026,"),
            "the remediation must name the LEAF — naming the intermediate sends the \
             operator to a config this same guard refuses next run: {why}"
        );

        let RoutingVerdict::Never(why) = classify_routing(&rel('p', &[])) else {
            panic!("a partitioned parent with no partitions yet is still unroutable");
        };
        assert!(why.contains("no partitions yet"), "{why}");

        for (kind, word) in [('v', "VIEW"), ('f', "FOREIGN TABLE")] {
            let RoutingVerdict::Never(why) = classify_routing(&rel(kind, &[])) else {
                panic!("{kind} writes no WAL under its own name — it must be refused");
            };
            assert!(why.contains(word), "the refusal must say what it is: {why}");
        }

        let RoutingVerdict::Partial(why) = classify_routing(&rel('m', &[])) else {
            panic!(
                "a matview refreshed CONCURRENTLY emits row events under its own \
                 name — refusing it refuses a capture that works"
            );
        };
        assert!(
            why.contains("MATERIALIZED VIEW") && why.to_uppercase().contains("CONCURRENTLY"),
            "and the warning must name the ONE refresh form that decodes, since a \
             capture whose refresh job uses the plain form sits at 0 rows forever \
             while reporting success: {why}"
        );
        assert!(
            why.contains("not the base table") || why.contains("REFRESH DELTAS"),
            "it must also say WHAT is delivered — refresh deltas, not the source \
             table's changes — or an operator reads a working matview capture as \
             source CDC: {why}"
        );

        assert_eq!(classify_routing(&rel('r', &[])), RoutingVerdict::Routable);

        let mismatched = RelationRouting {
            configured: "MixedCase",
            relkind: 'r',
            relpersistence: 'p',
            resolved: ("public".to_string(), "mixedcase".to_string()),
            leaves: vec![],
            children: vec![],
        };
        let RoutingVerdict::Never(why) = classify_routing(&mismatched) else {
            panic!(
                "a config whose resolved identity differs from what routing compares \
                 must be refused — measured, it wrote rows under the WRONG table's \
                 schema with the real column absent entirely, exit 0"
            );
        };
        assert!(
            why.contains("public.mixedcase"),
            "the refusal must name the CATALOG's spelling — that spelling is the \
             remediation: {why}"
        );

        let three_part = RelationRouting {
            configured: "rivet.public.orders",
            relkind: 'r',
            relpersistence: 'p',
            resolved: ("public".to_string(), "orders".to_string()),
            leaves: vec![],
            children: vec![],
        };
        assert!(
            matches!(classify_routing(&three_part), RoutingVerdict::Never(_)),
            "a 3-part name resolves for the probe and never matches the router"
        );

        let unlogged = RelationRouting {
            configured: "public.t",
            relkind: 'r',
            relpersistence: 'u',
            resolved: ("public".to_string(), "t".to_string()),
            leaves: vec![],
            children: vec![],
        };
        let RoutingVerdict::Never(why) = classify_routing(&unlogged) else {
            panic!("an UNLOGGED table writes no WAL — capture is a guaranteed zero");
        };
        assert!(
            why.contains("SET LOGGED"),
            "the refusal must name the one-line fix: {why}"
        );
        assert_eq!(classify_routing(&rel('r', &[])), RoutingVerdict::Routable);

        let RoutingVerdict::Partial(why) = classify_routing(&rel('r', &["public.t_child"])) else {
            panic!("an inheritance parent routes its OWN rows — warn, never refuse");
        };
        assert!(
            why.contains("public.t_child") && why.contains('1'),
            "the warning must name the children and how many: {why}"
        );
    }

    #[test]
    fn parse_pg_array_backslash_before_multibyte_does_not_panic() {
        let got = parse_pg_array_literal("text", "{\"a\\éb\"}").expect("must parse, not panic");
        assert_eq!(
            got.len(),
            1,
            "one element decoded from the escaped multibyte"
        );
        for lit in ["{\"\\🎉\"}", "{\"x\\é\"}", "{\"a\",\"\\é\"}", "{\"z\\"] {
            let _ = parse_pg_array_literal("text", lit);
        }
    }

    const CONN: &str = "postgresql://rivet:rivet@127.0.0.1:5434/rivet";
    const SLOT: &str = "rivet_cdc_test";

    #[test]
    fn tx_disposition_bound_frontier_matrix() {
        use TxDisposition::*;
        assert_eq!(tx_disposition(10, 0, None), Yield);
        assert_eq!(tx_disposition(10, 10, None), AlreadyYielded);
        assert_eq!(tx_disposition(9, 10, None), AlreadyYielded);
        assert_eq!(tx_disposition(10, 0, Some(10)), Yield);
        assert_eq!(tx_disposition(11, 0, Some(10)), PastBound);
        assert_eq!(tx_disposition(11, 11, Some(10)), PastBound);
        assert_eq!(tx_disposition(0, 0, Some(10)), AlreadyYielded);
        assert_eq!(tx_disposition(0, 0, None), AlreadyYielded);
    }

    #[test]
    fn a_yielded_transaction_is_owed_only_when_it_carried_data() {
        assert_eq!(owed_after_yield(None, 7, 1, false), Some(7));
        assert_eq!(
            owed_after_yield(None, 7, 0, true),
            Some(7),
            "a spilled tail is data"
        );
        assert_eq!(owed_after_yield(Some(3), 7, 2, false), Some(7));
        assert_eq!(
            owed_after_yield(None, 7, 0, false),
            None,
            "an empty transaction owes nothing"
        );
        assert_eq!(
            owed_after_yield(Some(3), 7, 0, false),
            Some(3),
            "and keeps what was owed"
        );
    }

    #[test]
    fn an_ack_discharges_only_what_it_covers() {
        assert_eq!(
            owed_after_ack(Some(5), Some(4)),
            Some(5),
            "a mid-window ack leaves the rest owed"
        );
        assert_eq!(owed_after_ack(Some(5), Some(5)), None);
        assert_eq!(owed_after_ack(Some(5), Some(6)), None);
        assert_eq!(
            owed_after_ack(Some(5), None),
            Some(5),
            "an unparseable ack discharges nothing"
        );
        assert_eq!(owed_after_ack(None, Some(9)), None);
    }

    #[test]
    fn wire_budget_is_the_ack_cadence_clamped_to_int4() {
        use crate::source::cdc::PeekBound;
        assert_eq!(wire_budget(PeekBound::Sized(100_000)), 100_000);
        assert_eq!(wire_budget(PeekBound::Sized(0)), 1);
        assert_eq!(wire_budget(PeekBound::Unbounded), i32::MAX);
    }

    #[test]
    fn map_pg_value_non_ascii_uuid_bytea_never_panics() {
        assert_eq!(decode_hex("€€"), None);
        assert_eq!(decode_hex("aa"), Some(vec![0xaa]));
        let _ = map_pg_value("uuid", "€€€€€€€€€€€€€€€€€€", false);
        let _ = map_pg_value("bytea", "\\x€€", false);
        assert_eq!(map_pg_value("uuid", "not-hex", false), None);
    }

    /// A numeric array keeps the server's literal; an integer array parses.
    /// A catalog array is kept as the server's text exactly when batch delivers it as server text.
    #[test]
    fn an_array_is_server_text_where_batch_delivers_it_so() {
        use postgres::types::Type;
        for (t, elem, want) in [
            (Type::INET_ARRAY, "b", true),
            (Type::MONEY_ARRAY, "b", true),
            (Type::INT4_ARRAY, "b", false),
            (Type::TEXT_ARRAY, "b", false),
        ] {
            assert_eq!(array_is_server_text(t.oid(), elem), want, "{t}");
        }
        assert!(array_is_server_text(900_001, "c"), "a composite array");
        assert!(array_is_server_text(900_002, "b"), "a user base type array");
        assert!(
            !array_is_server_text(900_003, "e"),
            "an enum array stays a list"
        );
    }

    /// The map holds each domain at its base label, each server-text array at `text`, and nothing for a mapped array.
    #[test]
    fn the_domain_map_holds_domains_and_server_text_arrays_only() {
        use postgres::types::Type;
        let map = domain_map(
            [("money_d".to_string(), "numeric".to_string())],
            [
                (
                    Type::INET_ARRAY.oid(),
                    "inet[]".to_string(),
                    "b".to_string(),
                ),
                (
                    Type::INT4_ARRAY.oid(),
                    "integer[]".to_string(),
                    "b".to_string(),
                ),
                (900_003, "mood[]".to_string(), "e".to_string()),
                (900_001, "pair[]".to_string(), "c".to_string()),
            ],
        );
        let mut got: Vec<_> = map.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        got.sort();
        assert_eq!(
            got,
            [
                ("inet[]", "text"),
                ("money_d", "numeric"),
                ("pair[]", "text")
            ]
        );
        assert_eq!(domain_base(&map, "inet[]"), "text");
        assert_eq!(domain_base(&map, "integer[]"), "integer[]");
    }

    #[test]
    fn an_unlisted_array_keeps_the_servers_own_literal() {
        assert_eq!(
            map_pg_value("numeric[]", "{1.50,2}", false),
            Some(RivetValue::Bytes(b"{1.50,2}".to_vec()))
        );
        assert_eq!(
            map_pg_value("integer[]", "{1,NULL}", false),
            Some(RivetValue::Array(vec![
                RivetValue::Int(1),
                RivetValue::Null
            ]))
        );
    }

    /// Date, time, timestamp, uuid and bytea arrays parse to the typed elements their list column holds.
    #[test]
    fn temporal_uuid_and_bytea_arrays_parse_to_typed_elements() {
        let day = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(
            map_pg_value("date[]", "{2024-01-01,NULL}", false),
            Some(RivetValue::Array(vec![
                RivetValue::DateTime(day.and_hms_opt(0, 0, 0).unwrap()),
                RivetValue::Null,
            ]))
        );
        assert_eq!(
            map_pg_value(
                "timestamp without time zone[]",
                "{\"2024-01-01 12:00:00.5\"}",
                false
            ),
            Some(RivetValue::Array(vec![RivetValue::DateTime(
                day.and_hms_micro_opt(12, 0, 0, 500_000).unwrap()
            )]))
        );
        assert_eq!(
            map_pg_value(
                "timestamp with time zone[]",
                "{\"2024-01-01 12:00:00+02\"}",
                false
            ),
            Some(RivetValue::Array(vec![RivetValue::DateTime(
                day.and_hms_opt(10, 0, 0).unwrap()
            )]))
        );
        assert_eq!(
            map_pg_value("time without time zone[]", "{01:02:03.25}", false),
            Some(RivetValue::Array(vec![RivetValue::TimeMicros(
                3_723_250_000
            )]))
        );
        assert_eq!(
            map_pg_value(
                "uuid[]",
                "{550e8400-e29b-41d4-a716-446655440000,NULL}",
                false
            ),
            Some(RivetValue::Array(vec![
                RivetValue::Bytes(vec![
                    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55,
                    0x44, 0x00, 0x00
                ]),
                RivetValue::Null,
            ]))
        );
        assert_eq!(
            map_pg_value("bytea[]", "{\"\\\\x00ff\",\"\\\\x\"}", false),
            Some(RivetValue::Array(vec![
                RivetValue::Bytes(vec![0x00, 0xff]),
                RivetValue::Bytes(Vec::new()),
            ]))
        );
    }

    /// An infinite date or timestamp inside an array leaves the literal whole, so the sink refuses the cell by name.
    #[test]
    fn an_infinite_element_keeps_the_array_literal_whole() {
        for (typ, lit) in [
            ("date[]", "{2024-01-01,infinity}"),
            ("timestamp without time zone[]", "{\"-infinity\"}"),
        ] {
            assert_eq!(
                map_pg_value(typ, lit, false),
                Some(RivetValue::Bytes(lit.as_bytes().to_vec())),
                "{typ}"
            );
        }
    }

    /// A `numeric` array keeps travelling as the server's literal text; every other element type is parsed.
    #[test]
    fn array_elem_is_listed_excludes_only_numeric() {
        for inner in ["date", "text", "integer", "uuid", "bytea", "tstzrange"] {
            assert!(array_elem_is_listed(inner), "{inner}");
        }
        assert!(!array_elem_is_listed("numeric"));
    }

    /// A bit or bit varying cell loses the decoder's `B'..'` wrapper and keeps the server's text; any other shape is unrepresentable.
    #[test]
    fn a_bit_literal_is_unwrapped_to_the_servers_text() {
        for (typ, lit, text) in [
            ("bit", "B'110'", "110"),
            ("bit varying", "B'10101'", "10101"),
            ("bit varying", "B''", ""),
            ("bit", "B'1'", "1"),
        ] {
            assert_eq!(
                map_pg_value(typ, lit, false),
                Some(RivetValue::Bytes(text.as_bytes().to_vec())),
                "{typ} {lit}"
            );
        }
        assert_eq!(map_pg_value("bit", "110", false), None);
        assert_eq!(map_pg_value("bit", "null", false), Some(RivetValue::Null));
    }

    /// A line whose array type label is in the domain map decodes as text and keeps the server's literal whole.
    #[test]
    fn an_array_label_mapped_to_text_keeps_the_servers_literal() {
        let domains: Domains = [("inet[]".to_string(), "text".to_string())].into();
        let ev = parse_test_decoding(
            "0/ABC",
            "table public.t: INSERT: id[integer]:1 ips[inet[]]:'{10.0.0.1,\"::1\"}'",
            &domains,
        )
        .unwrap()
        .expect("an insert");
        assert_eq!(
            ev.after.expect("after image")[1],
            RivetValue::Bytes(b"{10.0.0.1,\"::1\"}".to_vec())
        );
    }

    fn td_ident(n: &str) -> String {
        let bare = n.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
            && n.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if bare {
            n.to_string()
        } else {
            format!("\"{}\"", n.replace('"', "\"\""))
        }
    }

    fn td_ident_strategy() -> impl proptest::strategy::Strategy<Value = String> {
        use proptest::prelude::*;
        let atoms = vec![
            "\"",
            "'",
            " ",
            ":",
            "[",
            "]",
            "]:",
            ": ",
            ".",
            ",",
            "-",
            "é",
            "日",
            "a",
            "Z",
            "0",
            "new-tuple",
            "old-key: ",
            "[text]:",
            "table ",
            "_",
        ];
        prop_oneof![
            "[a-z_][a-z0-9_]{0,6}",
            proptest::collection::vec(proptest::sample::select(atoms), 1..9)
                .prop_map(|v| v.concat()),
        ]
    }

    #[derive(Debug, Clone)]
    enum TdCell {
        Text(String),
        Int(i64),
        Null,
    }

    fn td_cells_strategy()
    -> impl proptest::strategy::Strategy<Value = Vec<(String, String, TdCell)>> {
        use proptest::prelude::*;
        let cell = prop_oneof![
            "[^\u{0}]{0,40}".prop_map(TdCell::Text),
            any::<i64>().prop_map(TdCell::Int),
            Just(TdCell::Null),
        ];
        proptest::collection::vec((td_ident_strategy(), td_ident_strategy(), cell), 1..5)
    }

    fn td_render(cells: &[(String, String, TdCell)]) -> String {
        cells
            .iter()
            .map(|(name, typ, cell)| match cell {
                TdCell::Text(v) => format!(
                    "{}[{}]:'{}'",
                    td_ident(name),
                    td_ident(&format!("t_{typ}")),
                    v.replace('\'', "''")
                ),
                TdCell::Int(i) => format!("{}[integer]:{i}", td_ident(name)),
                TdCell::Null => format!("{}[text]:null", td_ident(name)),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn td_expect(cells: &[(String, String, TdCell)]) -> (Vec<String>, Vec<RivetValue>) {
        let sql_null = RivetValue::Null;
        (
            cells.iter().map(|(n, _, _)| n.clone()).collect(),
            cells
                .iter()
                .map(|(_, _, c)| match c {
                    TdCell::Text(v) => RivetValue::Bytes(v.clone().into_bytes()),
                    TdCell::Int(i) => RivetValue::Int(*i),
                    TdCell::Null => sql_null.clone(),
                })
                .collect(),
        )
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 512, ..Default::default()
        })]

        #[test]
        fn a_rendered_test_decoding_line_parses_back_to_exactly_what_was_rendered(
            schema in td_ident_strategy(),
            table in td_ident_strategy(),
            old in td_cells_strategy(),
            new in td_cells_strategy(),
            op in 0usize..3,
        ) {
            let rel = format!("{}.{}", td_ident(&schema), td_ident(&table));
            let line = match op {
                0 => format!("table {rel}: INSERT: {}", td_render(&new)),
                1 => format!("table {rel}: DELETE: {}", td_render(&new)),
                _ => format!(
                    "table {rel}: UPDATE: old-key: {} new-tuple: {}",
                    td_render(&old),
                    td_render(&new)
                ),
            };
            let ev = parse_test_decoding("0/1", &line, &Domains::new())
                .unwrap()
                .expect("a rendered line parses to a change");
            proptest::prop_assert_eq!(&ev.schema, &schema, "{}", line);
            proptest::prop_assert_eq!(&ev.table, &table, "{}", line);
            let (names, values) = td_expect(&new);
            proptest::prop_assert_eq!(ev.image_names.as_deref(), Some(&names[..]), "{}", line);
            match op {
                0 => proptest::prop_assert_eq!(ev.after.as_ref(), Some(&values), "{}", line),
                1 => proptest::prop_assert_eq!(ev.before.as_ref(), Some(&values), "{}", line),
                _ => {
                    let (old_names, old_values) = td_expect(&old);
                    proptest::prop_assert_eq!(ev.after.as_ref(), Some(&values), "{}", line);
                    proptest::prop_assert_eq!(ev.before.as_ref(), Some(&old_values), "{}", line);
                    proptest::prop_assert_eq!(
                        ev.before_names.as_deref(),
                        Some(&old_names[..]),
                        "{}",
                        line
                    );
                }
            }
        }
    }

    /// The text of the file above its test modules.
    fn production_text() -> &'static str {
        let all = include_str!("cdc.rs");
        &all[..all.find("\n#[cfg(test)]\nmod tests {").unwrap()]
    }

    /// Functions that read a VALUE or an LSN, not the shape of a `test_decoding` line.
    const VALUE_PARSERS: &[&str] = &[
        "parse_lsn",
        "parse_pg_array_literal",
        "parse_pg_time_micros",
        "parse_pg_time_micros_unbounded",
        "parse_pg_interval",
    ];

    /// The functions of `src` that cut text with `split*`, `find` or `rfind`, by name.
    fn functions_cutting_text(src: &str) -> std::collections::BTreeSet<String> {
        let cut = [
            ".split_once(",
            ".rsplit_once(",
            ".splitn(",
            ".rsplitn(",
            ".split(",
            ".rsplit(",
            ".rfind(",
            ".match_indices(",
            ".find('",
            ".find(\"",
            ".find(&",
        ];
        let mut found = std::collections::BTreeSet::new();
        let mut current = String::new();
        for line in src.lines() {
            let t = line.trim_start();
            let after_vis = t
                .strip_prefix("pub(crate) ")
                .or_else(|| t.strip_prefix("pub "))
                .unwrap_or(t);
            if let Some(name) = after_vis.strip_prefix("fn ") {
                current = name
                    .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .next()
                    .unwrap()
                    .to_string();
            }
            let code = line.split("//").next().unwrap();
            if cut.iter().any(|c| code.contains(c)) {
                found.insert(current.clone());
            }
        }
        found
    }

    /// No function of the CDC reader other than a value or LSN parser cuts text with a split or a find.
    #[test]
    fn only_value_parsers_cut_text_outside_the_one_line_tokenizer() {
        let found = functions_cutting_text(production_text());
        assert!(
            found.contains("parse_lsn"),
            "the scan sees the file: {found:?}"
        );
        let stray: Vec<&String> = found
            .iter()
            .filter(|f| !VALUE_PARSERS.contains(&f.as_str()))
            .collect();
        assert!(
            stray.is_empty(),
            "{stray:?} cut text with split/find in cdc.rs; a test_decoding line is read only by td_line::tokenize"
        );
    }

    /// The scan is graded: a function that splits a line is found.
    #[test]
    fn the_cut_scan_finds_a_split_in_a_function() {
        let src = "fn header(d: &str) {\n    let _ = d.split_once(\": \");\n}\nfn quiet(d: &str) {\n    let _ = d.len();\n}\n";
        let found = functions_cutting_text(src);
        assert_eq!(
            found.into_iter().collect::<Vec<_>>(),
            vec!["header".to_string()]
        );
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 256, ..Default::default()
        })]

        #[test]
        fn parse_test_decoding_never_panics(s in ".{0,200}") {
            let _ = parse_test_decoding("0/ABC", &s, &Domains::new());
        }

        #[test]
        fn map_pg_value_never_panics(
            typ in proptest::prop_oneof![
                "[a-z ]{1,20}(\\[\\])?",
                proptest::sample::select(vec![
                    "uuid".to_string(), "bytea".to_string(), "date".to_string(),
                    "timestamp".to_string(), "timestamptz".to_string(),
                    "numeric".to_string(), "json".to_string(), "jsonb".to_string(),
                    "int4".to_string(), "int8".to_string(), "bool".to_string(),
                ]),
            ],
            val in ".{0,120}",
            quoted in proptest::prelude::any::<bool>(),
        ) {
            let _ = map_pg_value(&typ, &val, quoted);
        }

        #[test]
        fn pg_timestamp_parse_total_and_offset_correct(
            h in 0u32..24, mi in 0u32..60, sec in 0u32..60,
            off_h in -12i32..=14, junk in ".{0,40}",
        ) {
            let _ = parse_pg_timestamp(&junk);
            let rendered = format!("2024-06-15 {h:02}:{mi:02}:{sec:02}{off_h:+03}");
            if let Some(RivetValue::DateTime(dt)) = parse_pg_timestamp(&rendered) {
                let wall = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
                    .unwrap()
                    .and_hms_opt(h, mi, sec)
                    .unwrap();
                let expect = wall - chrono::Duration::hours(off_h as i64);
                proptest::prop_assert_eq!(dt, expect);
            } else {
                proptest::prop_assert!(false, "well-formed rendering must parse: {}", rendered);
            }
        }

        #[test]
        fn array_literal_roundtrips_arbitrary_text_elements(
            elems in proptest::collection::vec(
                proptest::option::of("[^\u{0}]{0,24}"), 0..6
            )
        ) {
            let body: Vec<String> = elems
                .iter()
                .map(|e| match e {
                    None => "NULL".to_string(),
                    Some(t) => format!(
                        "\"{}\"",
                        t.replace('\\', "\\\\").replace('"', "\\\"")
                    ),
                })
                .collect();
            let lit = format!("{{{}}}", body.join(","));
            let parsed = parse_pg_array_literal("text", &lit)
                .expect("a rendered literal always parses");
            proptest::prop_assert_eq!(parsed.len(), elems.len());
            for (p, e) in parsed.iter().zip(&elems) {
                match (p, e) {
                    (RivetValue::Null, None) => {}
                    (RivetValue::Bytes(b), Some(t)) => {
                        proptest::prop_assert_eq!(b.as_slice(), t.as_bytes())
                    }
                    other => proptest::prop_assert!(false, "mismatch: {:?}", other),
                }
            }
        }
    }

    #[test]
    fn pk_changing_update_splits_old_key_from_new_tuple() {
        let line = "table public.t: UPDATE: old-key: id[integer]:1 \
                    new-tuple: id[integer]:2 v[text]:'a'";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .unwrap()
            .unwrap();
        assert_eq!(
            ev.after,
            Some(vec![RivetValue::Int(2), RivetValue::Bytes(b"a".to_vec())]),
            "after-image is the NEW tuple only"
        );
        assert_eq!(
            (ev.before, ev.before_names.as_deref()),
            (
                Some(vec![RivetValue::Int(1)]),
                Some(&["id".to_string()][..])
            ),
            "the old key rides before under its own name; `v` is absent, not NULL"
        );
        let ev = parse_test_decoding(
            "0/ABC",
            "table public.t: UPDATE: old-key: id[integer]:1 new-tuple: v[text]:'a' id[integer]:2",
            &Domains::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            (ev.before, ev.before_names.as_deref()),
            (
                Some(vec![RivetValue::Int(1)]),
                Some(&["id".to_string()][..])
            )
        );
        let ev = parse_test_decoding(
            "0/ABC",
            "table public.t: UPDATE: id[integer]:1 v[text]:'b'",
            &Domains::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            ev.after,
            Some(vec![RivetValue::Int(1), RivetValue::Bytes(b"b".to_vec())])
        );
        assert_eq!(ev.before, None);
    }

    /// An undecodable old-image cell is a deferred refusal of the OLD image only; the event itself is clean.
    #[test]
    fn an_undecodable_old_image_cell_poisons_the_update() {
        let line = "table public.t: UPDATE: old-key: id[integer]:1 \
                    valid_until[timestamp with time zone]:'infinity' new-tuple: id[integer]:2 \
                    valid_until[timestamp with time zone]:'2025-01-01 00:00:00+00'";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .unwrap()
            .unwrap();
        assert_eq!(ev.poison, None, "the new row decodes");
        let poison = ev
            .before_poison
            .expect("the old `infinity` is refused where the old image is written");
        assert!(
            poison.contains("column(s) [valid_until] hold a value"),
            "{poison}"
        );
    }

    #[test]
    fn pk_change_split_ignores_new_tuple_literal_inside_a_quoted_key() {
        let line = "table public.t: UPDATE: old-key: k[text]:'a new-tuple: b' \
                    new-tuple: k[text]:'c' v[integer]:9";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .unwrap()
            .unwrap();
        assert_eq!(
            ev.before,
            Some(vec![RivetValue::Bytes(b"a new-tuple: b".to_vec())]),
            "the old key keeps its FULL text value, separator substring included"
        );
        assert_eq!(
            ev.after,
            Some(vec![RivetValue::Bytes(b"c".to_vec()), RivetValue::Int(9)]),
            "the after-image is the real new tuple only, not cut mid-value"
        );
    }

    #[test]
    fn a_domain_typed_cell_decodes_as_its_base_type() {
        use crate::source::cdc::value::build_column;
        use arrow::array::{Array, BooleanArray, Date32Array, Int32Array};
        use arrow::datatypes::DataType;
        let domains: Domains = [
            ("posint", "integer"),
            ("flag", "boolean"),
            ("day", "date"),
            ("workday", "day"),
        ]
        .map(|(d, b)| (d.to_string(), b.to_string()))
        .into();
        let line = "table public.t: INSERT: id[integer]:1 qty[posint]:'5' ok[flag]:'t' \
                    d[workday]:'2024-02-29'";
        let ev = parse_test_decoding("0/1", line, &domains).unwrap().unwrap();
        let after = ev.after.unwrap();
        let qty = build_column(&DataType::Int32, &[Some(&after[1])]).unwrap();
        let qty = qty.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(
            qty.is_valid(0),
            "a domain over integer must not build a NULL"
        );
        assert_eq!(qty.value(0), 5);
        let ok = build_column(&DataType::Boolean, &[Some(&after[2])]).unwrap();
        assert!(ok.as_any().downcast_ref::<BooleanArray>().unwrap().value(0));
        let d = build_column(&DataType::Date32, &[Some(&after[3])]).unwrap();
        let d = d.as_any().downcast_ref::<Date32Array>().unwrap();
        assert!(d.is_valid(0), "a nested date domain must not build a NULL");
        assert_eq!(d.value(0), 19_782, "2024-02-29 is day 19782 of the epoch");
    }

    /// A stream over `client` on a slot it never opened.
    fn stream_on(client: Client) -> PgChangeStream {
        PgChangeStream {
            client,
            domains: Domains::new(),
            slot: "s".into(),
            pending: VecDeque::new(),
            spill_dir: None,
            spooled: None,
            batch_limit: 10,
            frontier: 0,
            exhausted: false,
            pending_truncate_refusal: None,
            bound: None,
            owed_through: None,
            frontier_text: None,
            configured_tables: Vec::new(),
            bare_schema: HashMap::new(),
            barrier_nonce: None,
            checkpoint: None,
            readable: None,
        }
    }

    #[test]
    fn the_open_sends_no_statement_before_its_first_read() {
        use crate::connect::postgres::wire_recorder::{recording_server, sent};
        let (url, queries) = recording_server();
        let open = PgChangeStream::open(
            &url,
            "s",
            PriorRun::default(),
            None,
            crate::source::cdc::PeekBound::Sized(10),
            DrainMode::Continuous,
            &[],
            None,
            None,
            None,
        );
        assert!(open.is_err(), "the recorder answers no prepared statement");
        assert_eq!(sent(queries), ["SHOW wal_level"]);
    }

    #[test]
    fn a_peek_sets_the_five_formats_for_its_own_transaction() {
        use crate::connect::postgres::wire_recorder::{BEGIN_PINNED_TEXT, recording_server, sent};
        let (url, queries) = recording_server();
        let mut stream = stream_on(crate::connect::postgres::connect_client(&url, None).unwrap());
        assert!(
            stream.fill().is_err(),
            "the recorder answers no prepared statement"
        );
        assert_eq!(
            sent(queries),
            [
                BEGIN_PINNED_TEXT,
                "SELECT lsn::text, data FROM pg_logical_slot_peek_changes($1, NULL, $2)"
            ]
        );
    }

    #[test]
    fn timestamptz_offset_is_data_not_decoration() {
        use chrono::NaiveDate;
        let cases = [
            ("2024-06-15 12:00:00+09", (2024, 6, 15, 3, 0, 0, 0)),
            ("2024-06-14 22:00:00-05", (2024, 6, 15, 3, 0, 0, 0)),
            (
                "2024-06-15 08:30:00.123456+05:30",
                (2024, 6, 15, 3, 0, 0, 123_456),
            ),
            ("2024-06-15 03:00:00+00", (2024, 6, 15, 3, 0, 0, 0)),
        ];
        for (rendered, (y, mo, d, h, mi, s, us)) in cases {
            let expected = NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_micro_opt(h, mi, s, us)
                .unwrap();
            assert_eq!(
                parse_pg_timestamp(rendered),
                Some(RivetValue::DateTime(expected)),
                "offset must convert to the UTC instant for {rendered:?}"
            );
        }
    }

    #[test]
    fn time_parses_to_micros_and_interval_canonicalises_to_iso8601() {
        let line = "table public.t: INSERT: \
                    t1[time without time zone]:'14:30:00.123456' \
                    iv1[interval]:'1 year 2 mons 3 days' \
                    iv2[interval]:'-1 years' \
                    iv3[interval]:'00:00:00' \
                    iv4[interval]:'3 days 04:05:06.789'";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .unwrap()
            .unwrap();
        let after = ev.after.unwrap();
        assert_eq!(
            after[0],
            RivetValue::TimeMicros((14 * 3600 + 30 * 60) * 1_000_000 + 123456),
            "TIME must parse to microseconds-since-midnight"
        );
        assert_eq!(after[1], RivetValue::Bytes(b"P1Y2M3D".to_vec()));
        assert_eq!(after[2], RivetValue::Bytes(b"P-1Y".to_vec()));
        assert_eq!(after[3], RivetValue::Bytes(b"PT0S".to_vec()));
        assert_eq!(after[4], RivetValue::Bytes(b"P3DT4H5M6.789000S".to_vec()));
    }

    #[test]
    fn uuid_and_bytea_decode_to_raw_bytes_not_their_text_rendering() {
        let line = "table public.t: INSERT: \
                    u[uuid]:'0b0e0af9-27ec-4c33-b428-a01b27fdd576' \
                    b[bytea]:'\\x48656c6c6f'";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .unwrap()
            .unwrap();
        let after = ev.after.unwrap();
        let RivetValue::Bytes(u) = &after[0] else {
            panic!("uuid must be Bytes, got {:?}", after[0]);
        };
        assert_eq!(
            u.len(),
            16,
            "uuid must be the raw 16 bytes, not 36-char text"
        );
        assert_eq!(
            u[..4],
            [0x0b, 0x0e, 0x0a, 0xf9],
            "uuid bytes must match the hyphenated hex"
        );
        assert_eq!(
            after[1],
            RivetValue::Bytes(b"Hello".to_vec()),
            "bytea must decode the \\x hex rendering to raw bytes"
        );
    }

    #[test]
    fn array_literals_parse_to_typed_elements() {
        let line = "table public.t: INSERT: \
                    tags[text[]]:'{alpha,\"with,comma\",\"he said \\\"hi\\\"\",NULL}' \
                    nums[integer[]]:'{1,NULL,3}' \
                    empty[text[]]:'{}'";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .unwrap()
            .unwrap();
        let after = ev.after.unwrap();
        assert_eq!(
            after[0],
            RivetValue::Array(vec![
                RivetValue::Bytes(b"alpha".to_vec()),
                RivetValue::Bytes(b"with,comma".to_vec()),
                RivetValue::Bytes(b"he said \"hi\"".to_vec()),
                RivetValue::Null,
            ])
        );
        assert_eq!(
            after[1],
            RivetValue::Array(vec![
                RivetValue::Int(1),
                RivetValue::Null,
                RivetValue::Int(3),
            ])
        );
        assert_eq!(after[2], RivetValue::Array(Vec::new()));
    }

    #[test]
    fn multidim_array_literal_is_refused_not_flattened_to_bogus_nulls() {
        assert_eq!(parse_pg_array_literal("integer", "{{1,2},{3,4}}"), None);
        assert_eq!(parse_pg_array_literal("text", "{{a,b},{c,d}}"), None);
        assert_eq!(
            parse_pg_array_literal("integer", "{1,2,3}"),
            Some(vec![
                RivetValue::Int(1),
                RivetValue::Int(2),
                RivetValue::Int(3),
            ])
        );
        assert_eq!(
            parse_pg_array_literal("text", "{\"{not nested}\"}"),
            Some(vec![RivetValue::Bytes(b"{not nested}".to_vec())])
        );
        let line = "table public.t: INSERT: grid[integer[]]:'{{1,2},{3,4}}'";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .unwrap()
            .unwrap();
        assert_eq!(
            ev.after.unwrap()[0],
            RivetValue::Bytes(b"{{1,2},{3,4}}".to_vec()),
            "multi-dim literal preserved as text, not flattened to Array([Null; 4])"
        );
    }

    #[test]
    fn parses_typed_columns_from_test_decoding() {
        let line = "table public.t: INSERT: id[integer]:1 name[text]:'alice o''brien' \
                    amount[numeric]:150.05 ts[timestamp without time zone]:'2026-06-23 11:58:01' \
                    flag[boolean]:t maybe[integer]:null";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .unwrap()
            .unwrap();
        assert_eq!(ev.op, ChangeOp::Insert);
        assert_eq!(ev.table, "t");
        let after = ev.after.unwrap();
        assert_eq!(after[0], RivetValue::Int(1));
        assert_eq!(after[1], RivetValue::Bytes(b"alice o'brien".to_vec()));
        assert_eq!(after[2], RivetValue::Bytes(b"150.05".to_vec()));
        assert!(matches!(after[3], RivetValue::DateTime(_)));
        assert_eq!(after[4], RivetValue::Bool(true));
        assert_eq!(after[5], RivetValue::Null);
    }

    #[test]
    fn unchanged_toast_recovers_from_full_pre_image() {
        let line = "table public.t: UPDATE: \
                    old-key: id[integer]:1 small[text]:'a' big[text]:'REAL-VALUE' \
                    new-tuple: id[integer]:1 small[text]:'b' big[text]:unchanged-toast-datum";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .unwrap()
            .unwrap();
        let after = ev.after.unwrap();
        assert_eq!(after[2], RivetValue::Bytes(b"REAL-VALUE".to_vec()));
        assert_eq!(after[1], RivetValue::Bytes(b"b".to_vec()));
    }

    /// A pre-image that is ITSELF unchanged-TOAST is not a recovery source.
    #[test]
    fn a_pre_image_that_is_also_unchanged_toast_cannot_recover_anything() {
        let line = "table public.t: UPDATE: \
                    old-key: id[integer]:1 big[text]:unchanged-toast-datum \
                    new-tuple: id[integer]:1 big[text]:unchanged-toast-datum";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .expect("the refusal is deferred, never an immediate bail")
            .expect("the event is still produced for commit-boundary tracking");
        let msg = ev.poison.expect(
            "nothing was recovered, so this column must be POISONED — accepting the \
             marker from a pre-image that is itself a marker writes the literal \
             text `unchanged-toast-datum` into the destination and reports success",
        );
        assert!(msg.contains("big"), "must name the column, got: {msg}");

        assert!(
            ev.after.is_some(),
            "the event is still produced; only its consumption is gated"
        );
    }

    #[test]
    fn unchanged_toast_without_pre_image_is_deferred_to_poison_not_an_immediate_bail() {
        let line = "table public.t: UPDATE: \
                    id[integer]:1 small[text]:'b' big[text]:unchanged-toast-datum";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .expect("must NOT bail — the refusal is deferred to the sink")
            .expect("the event is still produced (for commit-boundary tracking)");
        let msg = ev
            .poison
            .expect("an unrecoverable TOAST column must set poison");
        assert!(msg.contains("unchanged-TOAST"), "got: {msg}");
        assert!(msg.contains("big"), "must name the column, got: {msg}");
        assert!(
            msg.contains("REPLICA IDENTITY FULL"),
            "must name the upstream fix, got: {msg}"
        );
    }

    #[test]
    fn recoverable_toast_update_leaves_poison_none() {
        let line = "table public.t: UPDATE: old-key: id[integer]:1 big[text]:'real' \
                    new-tuple: id[integer]:1 small[text]:'b' big[text]:unchanged-toast-datum";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .unwrap()
            .unwrap();
        assert!(
            ev.poison.is_none(),
            "a full pre-image recovers the value → no poison: {:?}",
            ev.poison
        );
    }

    #[test]
    fn hostile_time_and_interval_text_never_panics_returns_none() {
        assert_eq!(parse_pg_time_micros("12:9999999999999999:00"), None);
        assert_eq!(parse_pg_time_micros("12:00:9999999999999999"), None);
        assert_eq!(parse_pg_time_micros("99:00:00"), None);
        assert_eq!(parse_pg_time_micros("01:02:03"), Some(3_723_000_000));
        assert_eq!(
            parse_pg_time_micros_unbounded("9999999999999999:00:00"),
            None
        );
        assert_eq!(
            parse_pg_time_micros_unbounded("25:00:00"),
            Some(90_000_000_000)
        );
        assert_eq!(parse_pg_interval("9999999999 years"), None);
        assert_eq!(parse_pg_interval("2 years 3 mons 4 days"), Some((27, 4, 0)));
        let line = "table public.t: INSERT: id[integer]:1 t[time]:12:9999999999999999:00";
        let _ = parse_test_decoding("0/ABC", line, &Domains::new());
    }

    #[test]
    fn quoted_marker_text_is_real_data_not_the_sentinel() {
        let line = "table public.t: INSERT: id[integer]:1 note[text]:'unchanged-toast-datum'";
        let ev = parse_test_decoding("0/ABC", line, &Domains::new())
            .unwrap()
            .unwrap();
        let after = ev.after.unwrap();
        assert_eq!(
            after[1],
            RivetValue::Bytes(b"unchanged-toast-datum".to_vec())
        );
    }

    /// A plugin name from `pg_replication_slots` maps to its reader; an unknown one keeps its name.
    #[test]
    fn a_slot_plugin_is_read_from_its_catalog_name() {
        assert_eq!(
            SlotPlugin::from_name("test_decoding"),
            SlotPlugin::TestDecoding
        );
        assert_eq!(SlotPlugin::from_name("pgoutput"), SlotPlugin::Pgoutput);
        assert_eq!(
            SlotPlugin::from_name("wal2json"),
            SlotPlugin::Other("wal2json".into())
        );
        for name in ["test_decoding", "pgoutput", "wal2json"] {
            assert_eq!(SlotPlugin::from_name(name).name(), name);
        }
    }

    /// The refusal for a foreign plugin names the slot, the plugin and the way out.
    #[test]
    fn the_foreign_plugin_refusal_names_the_slot_the_plugin_and_the_remedy() {
        let text = slot_plugin_refusal("s1", &SlotPlugin::Pgoutput);
        assert_eq!(
            text,
            "pg cdc: slot 's1' uses the 'pgoutput' output plugin, but this stream reads 'test_decoding' slots only. \
             Drop the slot (`SELECT pg_drop_replication_slot('s1')`) and re-baseline the stream, or point `cdc.slot` at a different name."
        );
    }

    /// A reused slot passes only for no slot or `test_decoding`; any other plugin is refused by code with the slot's name.
    #[test]
    fn a_reused_slot_is_refused_unless_it_is_test_decoding_or_absent() {
        assert!(refuse_a_foreign_slot_plugin("s1", None).is_ok());
        assert!(refuse_a_foreign_slot_plugin("s1", Some(SlotPlugin::TestDecoding)).is_ok());
        for plugin in [SlotPlugin::Pgoutput, SlotPlugin::Other("wal2json".into())] {
            let err = refuse_a_foreign_slot_plugin("s1", Some(plugin.clone())).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_PREREQUISITE", 1);
            assert!(format!("{err:#}").contains(&slot_plugin_refusal("s1", &plugin)));
        }
    }

    struct FakeCatalog {
        rows: Vec<Vec<Option<String>>>,
        fail: bool,
        seen: Vec<(String, String)>,
    }

    impl CatalogRows for FakeCatalog {
        fn text_rows(&mut self, sql: &str, arg: &str) -> Result<Vec<Vec<Option<String>>>> {
            self.seen.push((sql.to_string(), arg.to_string()));
            if self.fail {
                anyhow::bail!("catalog down");
            }
            Ok(self.rows.clone())
        }
    }

    fn fake(rows: Vec<Vec<Option<String>>>) -> FakeCatalog {
        FakeCatalog {
            rows,
            fail: false,
            seen: Vec::new(),
        }
    }

    fn row(cells: &[Option<&str>]) -> Vec<Option<String>> {
        cells.iter().map(|c| c.map(str::to_string)).collect()
    }

    /// The first `pg_replication_slots` row names the plugin; no row is no slot, a NULL plugin is the empty name.
    #[test]
    fn a_slot_plugin_is_read_from_the_first_catalog_row() {
        assert_eq!(slot_plugin_from_rows(&[]), None);
        assert_eq!(
            slot_plugin_from_rows(&[row(&[Some("pgoutput")])]),
            Some(SlotPlugin::Pgoutput)
        );
        assert_eq!(
            slot_plugin_from_rows(&[row(&[Some("test_decoding")])]),
            Some(SlotPlugin::TestDecoding)
        );
        assert_eq!(
            slot_plugin_from_rows(&[row(&[Some("wal2json")])]),
            Some(SlotPlugin::Other("wal2json".into()))
        );
        assert_eq!(
            slot_plugin_from_rows(&[row(&[None])]),
            Some(SlotPlugin::Other(String::new()))
        );
        assert_eq!(
            slot_plugin_from_rows(&[row(&[Some("pgoutput")]), row(&[Some("wal2json")])]),
            Some(SlotPlugin::Pgoutput)
        );
    }

    /// `slot_plugin_on` asks the catalog for the slot by name and returns what the rows say, or the catalog's error.
    #[test]
    fn the_slot_plugin_lookup_queries_the_slot_and_maps_its_rows() {
        let mut c = fake(vec![row(&[Some("pgoutput")])]);
        assert_eq!(
            slot_plugin_on(&mut c, "s_orders").unwrap(),
            Some(SlotPlugin::Pgoutput)
        );
        assert_eq!(c.seen.len(), 1);
        assert!(c.seen[0].0.contains("pg_replication_slots"));
        assert_eq!(c.seen[0].1, "s_orders");
        assert_eq!(slot_plugin_on(&mut fake(vec![]), "s_orders").unwrap(), None);
        let mut down = fake(vec![row(&[Some("pgoutput")])]);
        down.fail = true;
        let err = slot_plugin_on(&mut down, "s_orders").unwrap_err();
        assert!(format!("{err:#}").contains("catalog down"));
    }

    /// The first catalog row names the relation as `(schema, table)`; no row, a short row or a NULL cell resolves nowhere.
    #[test]
    fn a_relation_is_read_from_the_first_catalog_row() {
        assert_eq!(relation_from_rows(&[]), None);
        assert_eq!(
            relation_from_rows(&[row(&[Some("app"), Some("orders")])]),
            Some(("app".to_string(), "orders".to_string()))
        );
        assert_eq!(
            relation_from_rows(&[row(&[Some("app"), Some("orders"), Some("extra")])]),
            Some(("app".to_string(), "orders".to_string()))
        );
        assert_eq!(
            relation_from_rows(&[
                row(&[Some("tenant_b"), Some("orders")]),
                row(&[Some("public"), Some("orders")]),
                row(&[Some("archive"), Some("orders")]),
            ]),
            Some(("tenant_b".to_string(), "orders".to_string()))
        );
        assert_eq!(relation_from_rows(&[row(&[Some("app")])]), None);
        assert_eq!(relation_from_rows(&[row(&[])]), None);
        assert_eq!(relation_from_rows(&[row(&[None, Some("orders")])]), None);
        assert_eq!(relation_from_rows(&[row(&[Some("app"), None])]), None);
    }

    /// `search_path_relation` resolves the configured name through the catalog, returns the row's spelling, and names the name on a catalog error.
    #[test]
    fn the_search_path_lookup_queries_the_name_and_maps_its_row() {
        let mut c = fake(vec![row(&[Some("tenant_b"), Some("Orders")])]);
        assert_eq!(
            search_path_relation(&mut c, "orders").unwrap(),
            Some(("tenant_b".to_string(), "Orders".to_string()))
        );
        assert_eq!(c.seen.len(), 1);
        assert!(c.seen[0].0.contains("to_regclass($1)"));
        assert_eq!(c.seen[0].1, "orders");
        assert_eq!(
            search_path_relation(&mut fake(vec![]), "orders").unwrap(),
            None
        );
        let mut down = fake(vec![]);
        down.fail = true;
        let err = search_path_relation(&mut down, "orders").unwrap_err();
        let text = format!("{err:#}");
        assert!(text.contains("pg cdc: resolving `orders` through the catalog"));
        assert!(text.contains("catalog down"));
    }

    /// Live: an existing `pgoutput` slot is refused by name before any read; a `test_decoding` slot is reused.
    #[test]
    #[ignore = "live: requires docker compose postgres (wal_level=logical)"]
    fn live_open_refuses_a_pgoutput_slot_and_reuses_a_test_decoding_one() {
        let mut admin = Client::connect(CONN, NoTls).unwrap();
        let slot = format!("rivet_plugin_{}", std::process::id());
        let drop = |c: &mut Client| {
            c.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .unwrap();
        };
        drop(&mut admin);
        admin
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .unwrap();
        assert_eq!(
            slot_plugin_on(&mut admin, &slot).unwrap(),
            Some(SlotPlugin::Pgoutput)
        );
        let open = || {
            PgChangeStream::open(
                CONN,
                &slot,
                PriorRun::default(),
                None,
                crate::source::cdc::PeekBound::Sized(10),
                DrainMode::Continuous,
                &[],
                None,
                None,
                None,
            )
        };
        let err = open().err().expect("a pgoutput slot is refused");
        let text = format!("{err:#}");
        assert_eq!(
            crate::error::code_of(&err).map(|c| c.id),
            Some("RIVET_SOURCE_CDC_PREREQUISITE"),
            "{text}"
        );
        assert!(
            text.contains(&slot_plugin_refusal(&slot, &SlotPlugin::Pgoutput)),
            "{text}"
        );
        drop(&mut admin);
        assert_eq!(slot_plugin_on(&mut admin, &slot).unwrap(), None);
        admin
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
                &[&slot],
            )
            .unwrap();
        assert!(open().is_ok(), "a test_decoding slot is reused as before");
        drop(&mut admin);
    }

    /// Live: `text_rows` on a real client returns the server's rows, text cells and NULLs; the slot and relation lookups read them.
    #[test]
    #[ignore = "live: requires docker compose postgres (wal_level=logical)"]
    fn live_pg_catalog_rows_are_the_servers_text_cells() {
        let mut c = Client::connect(CONN, NoTls).unwrap();
        let id = std::process::id();
        let slot = format!("rivet_catrows_slot_{id}");
        let schema = format!("rivet_catrows_s_{id}");
        let table = format!("Rivet_Catrows_T_{id}");
        let cleanup = |c: &mut Client| {
            c.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .unwrap();
            c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                .unwrap();
        };
        cleanup(&mut c);
        c.execute(
            "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
            &[&slot],
        )
        .unwrap();
        c.batch_execute(&format!(
            "CREATE SCHEMA {schema}; CREATE TABLE {schema}.\"{table}\" (id int)"
        ))
        .unwrap();
        assert_eq!(
            c.text_rows("SELECT $1::text, NULL::text, 'b'::text", "abc")
                .unwrap(),
            vec![vec![Some("abc".to_string()), None, Some("b".to_string())]]
        );
        assert_eq!(
            c.text_rows("SELECT $1::text WHERE false", "abc").unwrap(),
            Vec::<Vec<Option<String>>>::new()
        );
        assert_eq!(
            c.text_rows(
                "SELECT x::text FROM generate_series(1, 3) x WHERE $1 = 'go'",
                "go"
            )
            .unwrap(),
            vec![
                vec![Some("1".to_string())],
                vec![Some("2".to_string())],
                vec![Some("3".to_string())]
            ]
        );
        assert_eq!(
            slot_plugin_on(&mut c, &slot).unwrap(),
            Some(SlotPlugin::TestDecoding)
        );
        assert_eq!(
            search_path_relation(&mut c, &format!("{schema}.\"{table}\"")).unwrap(),
            Some((schema.clone(), table.clone()))
        );
        cleanup(&mut c);
        assert_eq!(slot_plugin_on(&mut c, &slot).unwrap(), None);
    }

    /// Live: the label resolver agrees with the reader's own domain map for every domain on the server.
    #[test]
    #[ignore = "live: requires docker compose postgres (wal_level=logical)"]
    fn live_type_labels_resolve_every_domain_as_the_domain_map_does() {
        let mut c = Client::connect(CONN, NoTls).unwrap();
        c.batch_execute(
            "DROP DOMAIN IF EXISTS pgo_label_d2; DROP DOMAIN IF EXISTS pgo_label_d1; DROP DOMAIN IF EXISTS pgo_label_v; \
             CREATE DOMAIN pgo_label_d1 AS integer; CREATE DOMAIN pgo_label_d2 AS pgo_label_d1; \
             CREATE DOMAIN pgo_label_v AS varchar(10)",
        )
        .unwrap();
        let domains = load_domains(&mut c).unwrap();
        let labels = crate::source::postgres::type_labels::TypeLabels::load(&mut c).unwrap();
        let rows = c
            .query(
                "SELECT oid, format_type(oid, NULL) FROM pg_type WHERE typtype = 'd'",
                &[],
            )
            .unwrap();
        let mut seen = 0;
        for r in &rows {
            let (oid, label): (u32, String) = (r.get(0), r.get(1));
            assert_eq!(
                labels.label(oid),
                Some(domain_base(&domains, &label)),
                "domain {label}"
            );
            seen += 1;
        }
        let d2: u32 = c
            .query_one("SELECT 'pgo_label_d2'::regtype::oid", &[])
            .unwrap()
            .get(0);
        let int4: u32 = c
            .query_one("SELECT 'integer'::regtype::oid", &[])
            .unwrap()
            .get(0);
        assert_eq!(labels.label(d2), Some("integer"));
        assert_eq!(labels.label(int4), Some("integer"));
        assert!(
            seen >= 3,
            "the fixture domains must be among the {seen} read"
        );
        c.batch_execute(
            "DROP DOMAIN pgo_label_d2; DROP DOMAIN pgo_label_d1; DROP DOMAIN pgo_label_v",
        )
        .unwrap();
    }

    #[test]
    #[ignore = "live: requires docker compose postgres (wal_level=logical)"]
    fn streams_insert_update_delete() {
        let mut admin = Client::connect(CONN, NoTls).unwrap();
        admin
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
                &[&SLOT],
            )
            .unwrap();

        let mut s = PgChangeStream::open(
            CONN,
            SLOT,
            PriorRun::default(),
            None,
            crate::source::cdc::PeekBound::Sized(10_000),
            DrainMode::Continuous,
            &[],
            None,
            None,
            None,
        )
        .unwrap();
        admin
            .batch_execute(
                "DROP TABLE IF EXISTS cdc_unit; CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT)",
            )
            .unwrap();
        admin
            .batch_execute(
                "INSERT INTO cdc_unit VALUES (1, 10); \
                 UPDATE cdc_unit SET v = 20 WHERE id = 1; \
                 DELETE FROM cdc_unit WHERE id = 1",
            )
            .unwrap();

        let mut ops = Vec::new();
        while let Some(ev) = s.next_change() {
            let ev = ev.unwrap();
            if ev.table == "cdc_unit" {
                ops.push(ev.op);
            }
        }

        admin
            .execute("SELECT pg_drop_replication_slot($1)", &[&SLOT])
            .ok();

        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete],
            "logical slot must decode INSERT, UPDATE, DELETE in commit order"
        );
    }

    /// The class: `test_decoding` prints identifiers through `quote_ident`, rivet consumed them raw, every downstream compare is byte-exact, and the config validator refuses `"`.
    #[test]
    fn quoted_identifiers_are_unquoted_so_routing_can_match_them() {
        let ev = parse_test_decoding(
            "0/1",
            "table app.\"order\": INSERT: id[integer]:1",
            &Domains::new(),
        )
        .expect("parses")
        .expect("an event");
        assert_eq!(ev.schema, "app", "schema must be usable as a plain name");
        assert_eq!(
            ev.table, "order",
            "the table name must be the IDENTIFIER, not its quoted rendering — a byte-exact \
             router cannot match `\"order\"`, and no config string can spell it either"
        );

        let ev = parse_test_decoding(
            "0/2",
            "table public.\"User\": INSERT: id[integer]:7",
            &Domains::new(),
        )
        .expect("parses")
        .expect("an event");
        assert_eq!((ev.schema.as_str(), ev.table.as_str()), ("public", "User"));
    }

    /// A dot INSIDE a quoted part is not a qualifier separator.
    #[test]
    fn a_dot_inside_a_quoted_identifier_is_not_a_qualifier_split() {
        let ev = parse_test_decoding(
            "0/3",
            "table \"my.schema\".t: INSERT: id[integer]:1",
            &Domains::new(),
        )
        .expect("parses")
        .expect("an event");
        assert_eq!((ev.schema.as_str(), ev.table.as_str()), ("my.schema", "t"));
    }

    /// PostgreSQL doubles an embedded quote inside a quoted identifier.
    #[test]
    fn a_doubled_quote_inside_an_identifier_collapses_to_one() {
        assert_eq!(td_line::unquote_ident("\"a\"\"b\""), "a\"b");
        assert_eq!(td_line::unquote_ident("plain"), "plain");
        assert_eq!(td_line::unquote_ident("\"\""), "");
    }

    /// The COLUMN twin.
    #[test]
    fn a_quoted_column_name_is_unquoted_so_a_key_only_delete_maps_by_name() {
        let ev = parse_test_decoding(
            "0/4",
            "table app.t: DELETE: \"userId\"[integer]:7",
            &Domains::new(),
        )
        .expect("parses")
        .expect("an event");
        assert_eq!(
            ev.image_names.as_deref(),
            Some(&["userId".to_string()][..]),
            "the key column must carry its identifier, not its quoted rendering"
        );
    }

    /// The byte walk's ESCAPE branch, which the direct `unquote_ident` cases cannot reach.
    #[test]
    fn the_qualifier_walk_treats_a_doubled_quote_as_an_escape_not_a_terminator() {
        let cases: &[(&str, &str, &str)] = &[
            ("\"a\"\"b\".t", "a\"b", "t"),
            ("app.\"c\"\"d\"", "app", "c\"d"),
            ("\"a\"\".b\".t", "a\".b", "t"),
            ("\"x\"\"\".t", "x\"", "t"),
            ("\"order\"", "", "order"),
        ];
        for (input, schema, table) in cases {
            let ev = parse_test_decoding(
                "0/9",
                &format!("table {input}: INSERT: id[integer]:1"),
                &Domains::new(),
            )
            .expect("parses")
            .expect("an event");
            assert_eq!(
                (ev.schema.as_str(), ev.table.as_str()),
                (*schema, *table),
                "qualifier `{input}` split wrongly — a mis-split yields identifiers that \
                 never existed, and the router then drops every event for the table"
            );
        }
    }
}

#[cfg(test)]
mod spill_tests {
    use super::*;

    /// The wire-row codec round-trips text that would break any delimiter.
    #[test]
    fn a_wire_row_round_trips_through_the_spill_frame() {
        let cases: &[(&str, &str)] = &[
            ("0/16B2E00", "table public.t: INSERT: id[integer]:1"),
            (
                "0/16B2E00",
                "table public.t: INSERT: v[text]:'line1\nline2\tand\u{0}more'",
            ),
            ("0/1", "table public.t: INSERT: v[text]:'Ω≈ç√∫˜µ 日本語'"),
            ("0/1", ""),
        ];
        for (lsn, data) in cases {
            let back = decode_wire_row(&encode_wire_row(lsn, data)).expect("decode");
            assert_eq!(
                (back.0.as_str(), back.1.as_str()),
                (*lsn, *data),
                "the frame must be transparent to the payload"
            );
        }
    }

    /// A frame that does not hold what it declares is an ERROR, never a partial row.
    #[test]
    fn a_malformed_spill_frame_is_refused() {
        assert!(
            decode_wire_row(b"\x00\x00").is_err(),
            "too short to hold a length at all"
        );
        assert!(
            decode_wire_row(&[0, 0, 0, 99, b'a', b'b']).is_err(),
            "declares a 99-byte lsn in a 6-byte record"
        );
    }

    /// A transaction split between memory and disk carries EXACTLY ONE `committed`, on its true last row.
    #[test]
    fn a_split_transaction_closes_on_its_true_last_row() {
        use crate::source::cdc::TxnFramer;
        const N: usize = 5;
        let commit = Position::new(json!({ "lsn": "0/DEAD" }));
        let row = |i: usize| {
            let mut ev = parse_test_decoding(
                "0/1",
                &format!("table public.t: INSERT: id[integer]:{i}"),
                &Domains::new(),
            )
            .expect("parse")
            .expect("an event");
            ev.committed = true;
            ev
        };
        for head_len in 0..=N {
            let tail_len = N - head_len;
            let mut whole: Vec<ChangeEvent> = (0..head_len).map(row).collect();
            TxnFramer::close_head_of_group(&mut whole, &commit, tail_len);
            for i in 0..tail_len {
                let mut ev = row(i);
                TxnFramer::close_tail_event(&mut ev, &commit, i, tail_len);
                whole.push(ev);
            }
            assert_eq!(whole.len(), N);
            let flags: Vec<bool> = whole.iter().map(|e| e.committed).collect();
            assert_eq!(
                flags.iter().filter(|f| **f).count(),
                1,
                "head={head_len} tail={tail_len}: exactly one row closes the \
                 transaction, got {flags:?}"
            );
            assert!(
                whole.last().expect("N > 0").committed,
                "head={head_len} tail={tail_len}: the LAST row is the one that \
                 closes it — a flag anywhere else lets the sink ack mid-transaction"
            );
            assert!(
                whole.iter().all(|e| e.position == commit),
                "head={head_len} tail={tail_len}: every row of a transaction carries \
                 its COMMIT position, spilled or not"
            );
        }
    }
}

#[cfg(test)]
mod slot_creation_warning_tests {
    use super::slot_created_warning;

    /// A created slot means capture starts at the CURRENT WAL position, so anything written earlier is unreachable.
    #[test]
    fn the_created_slot_warning_names_the_loss_and_the_remedy() {
        assert_eq!(
            slot_created_warning("rivet_orders"),
            "pg cdc: creating replication slot 'rivet_orders' — capture starts at the CURRENT \
             WAL position, so changes written before now are NOT captured. On a first run this \
             is expected. If this slot existed before, it was dropped or invalidated and the \
             changes since then are not in this stream. Re-baseline the stream in one run: \
             delete the checkpoint file if there is one; move every file out of the export's \
             destination (for a `tables:` export, every table's directory under it): the parts \
             there still hold rows the source may no longer have, and each table's \
             snapshot/_SUCCESS marker goes with them; delete the export's `cdc_snapshot` rows \
             (one per table) from the state DB; give the export `cdc.initial: snapshot` if it \
             has none; and if a warehouse load consumes this stream, truncate its \
             `<table>__changes` table before the next load. That run anchors FIRST and re-reads \
             every table after, so nothing falls between the two. A separate `mode: full` export \
             does not re-baseline the stream. Set `cdc.checkpoint:` to turn this case into a \
             hard error instead of a warning."
        );
    }

    /// The anchor step (`initial: snapshot` / `backfill:`) can find both kinds of evidence; the refusal names both.
    #[test]
    fn the_missing_slot_refusal_names_the_checkpoint_and_the_snapshot_when_both_were_found() {
        let both = super::PriorRun {
            checkpoint: true,
            snapshot: true,
        };
        assert_eq!(
            super::pg_slot_missing_refusal("rivet_orders", both),
            format!(
                "pg cdc: slot 'rivet_orders' is missing but the checkpoint file holds a position from \
             a prior run and a prior run completed this export's snapshot (the state DB's \
             `cdc_snapshot` row or the destination's snapshot/_SUCCESS marker) — the slot was \
             dropped or invalidated, and the changes since then are no longer in the log. {}",
                crate::source::cdc::checkpoint_identity::RECOVER
            )
        );
    }

    /// Every (slot exists × checkpoint × snapshot) cell: reuse, create on no evidence, refuse naming what was found.
    #[test]
    fn slot_action_reuses_creates_or_refuses_by_existence_and_evidence() {
        use super::{PriorRun, SlotAction, slot_action};
        let prior = |checkpoint, snapshot| PriorRun {
            checkpoint,
            snapshot,
        };
        for (c, n) in [(false, false), (true, false), (false, true), (true, true)] {
            assert_eq!(slot_action("s", true, prior(c, n)), SlotAction::Reuse);
        }
        assert_eq!(
            slot_action("s", false, prior(false, false)),
            SlotAction::Create,
            "no prior-run evidence is a first run: the slot is created"
        );
        for (c, n, lead) in [
            (
                true,
                false,
                "pg cdc: slot 's' is missing but the checkpoint file holds a position from a prior run — ",
            ),
            (
                false,
                true,
                "pg cdc: slot 's' is missing but a prior run completed this export's snapshot (the state DB's `cdc_snapshot` row or the destination's snapshot/_SUCCESS marker) — ",
            ),
            (
                true,
                true,
                "pg cdc: slot 's' is missing but the checkpoint file holds a position from a prior run and a prior run completed",
            ),
        ] {
            match slot_action("s", false, prior(c, n)) {
                SlotAction::Refuse(why) => assert!(why.starts_with(lead), "({c}, {n}): {why}"),
                other => panic!("({c}, {n}) must refuse, got {other:?}"),
            }
        }
    }

    /// Each replica identity and primary key maps to the old image PostgreSQL logs for it.
    #[test]
    fn old_image_follows_the_replica_identity_and_the_key() {
        use crate::source::postgres::cdc::{NoOldKey::*, OldImage::*, old_image};
        for (ident, pk, index, want) in [
            ("f", Some(false), false, WholeRow),
            ("f", None, false, WholeRow),
            ("i", Some(true), true, IndexColumns),
            ("i", Some(true), false, NoKey(DroppedIdentityIndex)),
            ("n", Some(true), false, NoKey(IdentityNothing)),
            ("d", Some(true), false, PrimaryKey),
            ("d", Some(false), false, NoKey(DeferrableKey)),
            ("d", None, false, NoKey(NoPrimaryKey)),
        ] {
            assert_eq!(old_image(ident, pk, index), want, "{ident} {pk:?} {index}");
        }
    }

    /// Each no-old-image warning names the table, the cause, what is lost, and a remedy.
    #[test]
    fn the_no_old_key_warnings_name_the_table_the_cause_and_the_remedy() {
        use crate::source::postgres::cdc::{NoOldKey::*, no_old_key_warning};
        let lost = "so an UPDATE carries no old values and a DELETE carries no columns at all: a \
                    DELETE retracts nothing downstream, and an UPDATE that changes the key leaves \
                    the old key live.";
        assert_eq!(
            no_old_key_warning("s.t1", DeferrableKey),
            format!(
                "table s.t1 has a DEFERRABLE primary key, which PostgreSQL does not use as the \
                 replica identity, {lost} Run `ALTER TABLE s.t1 REPLICA IDENTITY FULL;` (the key \
                 stays deferrable), or `ALTER TABLE s.t1 REPLICA IDENTITY USING INDEX <index>;` on \
                 a non-deferrable unique index of the key columns (that index checks uniqueness \
                 row by row, so it rejects a statement such as `SET id = id + 1`)"
            )
        );
        assert_eq!(
            no_old_key_warning("s.t1", IdentityNothing),
            format!(
                "table s.t1 has REPLICA IDENTITY NOTHING, {lost} Run `ALTER TABLE s.t1 REPLICA \
                 IDENTITY FULL;`, or `ALTER TABLE s.t1 REPLICA IDENTITY DEFAULT;` if it has a \
                 primary key"
            )
        );
        assert_eq!(
            no_old_key_warning("s.t1", DroppedIdentityIndex),
            format!(
                "table s.t1 has REPLICA IDENTITY USING INDEX on an index that no longer exists, \
                 which PostgreSQL treats as NOTHING, {lost} Run `ALTER TABLE s.t1 REPLICA \
                 IDENTITY FULL;`, or `ALTER TABLE s.t1 REPLICA IDENTITY USING INDEX <index>;` on \
                 an existing unique index of NOT NULL columns"
            )
        );
        assert_eq!(
            no_old_key_warning("s.t1", NoPrimaryKey),
            format!(
                "table s.t1 has no primary key and REPLICA IDENTITY DEFAULT, {lost} Run `ALTER \
                 TABLE s.t1 REPLICA IDENTITY FULL;`, or give it a primary key"
            )
        );
    }

    /// #161: the delete-image verdict names only the tables whose DELETE carries just a key.
    #[test]
    fn row_image_verdict_both_directions() {
        use crate::source::cdc::RowImage;
        use crate::source::postgres::cdc::{NoOldKey, OldImage, row_image_verdict};
        assert!(matches!(row_image_verdict(&[]), RowImage::Whole));
        let whole_or_none = [
            ("a.full".to_string(), OldImage::WholeRow),
            (
                "a.none".to_string(),
                OldImage::NoKey(NoOldKey::IdentityNothing),
            ),
        ];
        assert!(matches!(row_image_verdict(&whole_or_none), RowImage::Whole));
        let mut images = whole_or_none.to_vec();
        images.push(("public.orders".into(), OldImage::PrimaryKey));
        images.push(("public.items".into(), OldImage::IndexColumns));
        match row_image_verdict(&images) {
            RowImage::KeyOnlyDeletes { why } => assert_eq!(
                why,
                "2 of the captured table(s) put only a key in a DELETE, while INSERT and UPDATE \
                 keep their whole new-row image: public.orders (its primary key), public.items \
                 (its replica identity index's columns; an UPDATE that changes a key column \
                 outside that index stays one update). `ALTER TABLE <t> REPLICA IDENTITY FULL` \
                 if the before-image matters"
            ),
            other => panic!("a key-only identity must warn, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod foreign_advance_tests {
    use super::slot_advanced_by_another;

    /// A checkpoint an older release wrote (no `confirmed_first`) behind its slot is adopted once and rewritten.
    #[test]
    fn an_older_releases_checkpoint_behind_its_slot_is_adopted_once_then_the_guard_is_strict() {
        use super::{CONFIRMED_FIRST, OpenVerdict, adopted, confirmed_first, open_verdict};
        use crate::source::cdc::Position;
        use serde_json::json;
        let old = Position::new(json!({ "lsn": "0/16B3000" }));
        assert_eq!(
            open_verdict("s", &old, "0/16B3000", Some("0/16B3780")),
            OpenVerdict::Adopt("0/16B3780".to_string())
        );
        assert_eq!(
            open_verdict("s", &old, "0/16B3000", Some("0/16B3000")),
            OpenVerdict::Mark
        );
        let rewritten = adopted(&old, "0/16B3780");
        assert_eq!(rewritten.json()["lsn"], "0/16B3780");
        assert_eq!(rewritten.json()[CONFIRMED_FIRST], true);
        assert_eq!(
            open_verdict("s", &rewritten, "0/16B3780", Some("0/16B3780")),
            OpenVerdict::Keep
        );
        for _ in 0..2 {
            assert!(matches!(
                open_verdict("s", &rewritten, "0/16B3780", Some("0/16B4000")),
                OpenVerdict::Refuse(why) if why.contains("another reader consumed")
            ));
        }
        let written_here = confirmed_first(&Position::new(json!({ "lsn": "0/1" })));
        assert!(matches!(
            open_verdict("s", &written_here, "0/1", Some("0/2")),
            OpenVerdict::Refuse(_)
        ));
    }

    #[test]
    fn a_slot_confirmed_past_the_checkpoint_is_refused_and_one_at_or_before_it_reads() {
        let why = slot_advanced_by_another("s", "0/16B3740", Some("0/16B3780")).expect("past");
        assert!(
            why.contains("another reader consumed") && why.contains("0/16B3780"),
            "{why}"
        );
        assert!(slot_advanced_by_another("s", "0/16B3740", Some("0/16B3740")).is_none());
        assert!(slot_advanced_by_another("s", "1/0", Some("0/FFFFFFFF")).is_none());
        assert!(slot_advanced_by_another("s", "0/10", None).is_none());
        assert!(slot_advanced_by_another("s", "junk", Some("0/20")).is_none());
    }
}
