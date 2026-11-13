//! The CDC capture: open the change stream, resolve each table, drive the file sink.

pub(crate) mod sink;

use crate::error::Result;
use crate::source::cdc::{
    CdcConfig, CdcSchemaResolver, PeekBound, RowImage, create_change_stream, partition_guard,
    undeclared_key_column,
};

/// One table's destination wiring for a capture — see [`CdcCapture::outputs`].
pub(crate) struct CaptureOutput<'a> {
    pub table: String,
    pub dest: &'a dyn crate::destination::Destination,
    pub dest_uri: String,
    /// The export's `columns:` type overrides for THIS table.
    pub overrides: crate::types::ColumnOverrides,
    /// `exports[].meta_columns.row_hash` — the drain must emit the SAME hash column the snapshot leg does, or the warehouse table ends up half-populated.
    pub row_hash: crate::config::RowHash,
    /// The partition budget this table's change parts keep (changelog layout only).
    pub partition: Option<crate::plan::rollover::PartitionRollover>,
    /// The partition key a change must not move (base-and-buffer layout only).
    pub partition_guard: Option<partition_guard::PartitionGuard>,
    /// The load's declared merge key; `None` reads the table's primary key at open.
    pub key: Option<Vec<String>>,
}

/// Everything needed to capture a change stream to typed files, assembled once.
pub(crate) type SchemaGate<'a> = dyn Fn(&str, &[crate::types::TypeMapping]) -> Result<()> + 'a;

pub(crate) struct CdcCapture<'a> {
    /// `exports[].name` — recorded into each manifest's `export_family` so the load's shared-prefix guard groups the drain with its snapshot leg by what was WRITTEN, not by re-deriving it from the table string.
    pub export_name: String,
    pub cdc_cfg: CdcConfig,
    pub outputs: Vec<CaptureOutput<'a>>,
    pub format: crate::config::FormatType,
    pub max_events: Option<usize>,
    pub rollover: usize,
    pub rollover_memory_bytes: Option<usize>,
    /// RFC3339 stamps the caller owns (`Utc::now()` is theirs to call).
    pub run_id: String,
    pub started_at: String,
    /// The central ledger.
    pub state: Option<&'a crate::state::StateStore>,
    /// Judges each table's resolved columns before any change is read; an error ends the run unacknowledged.
    pub schema_gate: Option<&'a SchemaGate<'a>>,
    /// The run's metadata connection, lent for schema resolution; `None` opens one here.
    pub meta: Option<&'a mut (dyn crate::source::Source + 'static)>,
    /// The type policy each table's resolved columns pass before any change is read.
    pub policy: crate::types::policy::TypePolicy,
}

/// A table's probe query, its `columns:` overrides and its `columns_from: grants` readable set.
type Probe = (
    String,
    std::collections::HashMap<String, crate::types::RivetType>,
    Option<crate::source::cdc::ReadableColumns>,
);

/// Open the change stream (with the engine's permission/TLS gate), resolve each table's typed schema, and drive the commit-seam file sink.
pub(crate) fn run_capture(
    cap: CdcCapture<'_>,
    read_bytes: &std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> (Vec<crate::manifest::RunManifest>, Result<()>) {
    let url = cap.cdc_cfg.url.clone();
    let tls = cap.cdc_cfg.tls.clone();
    let checkpoint = cap.cdc_cfg.checkpoint.clone();
    let mut stream = match create_change_stream(&cap.cdc_cfg, PeekBound::Sized(cap.rollover)) {
        Ok(s) => s,
        Err(e) => return (Vec::new(), Err(e)),
    };
    crate::test_hook::maybe_panic_at("cdc_after_open");
    crate::test_hook::maybe_pause_at("cdc_after_open");
    let engine = cap.cdc_cfg.engine.engine();
    let cap_tables: Vec<String> = cap.outputs.iter().map(|o| o.table.clone()).collect();
    let mut outputs = Vec::with_capacity(cap.outputs.len());
    let mut probes: std::collections::HashMap<String, Probe> = std::collections::HashMap::new();
    crate::test_hook::maybe_panic_at("cdc_before_resolve");
    let image = stream.row_image(&cap_tables);
    match image {
        RowImage::Whole => {}
        RowImage::KeyOnlyDeletes { why } => log::warn!(
            "{} cdc: {why}. Counts still reconcile, but a per-row hash over deletes will differ \
             from the same table's batch export.",
            engine.label()
        ),
        RowImage::Partial { why } => {
            return (
                Vec::new(),
                Err(crate::source::cdc::open_partial_image_refusal(engine, &why)),
            );
        }
    }
    let retention = stream.retention_warnings();
    for why in retention {
        log::warn!("{} cdc: {why}.", engine.label());
    }
    let mut resolver = match cap.meta {
        Some(src) => CdcSchemaResolver::lent(src),
        None => match CdcSchemaResolver::connect_as(engine, url.expose(), tls.as_ref()) {
            Ok(r) => r,
            Err(e) => return (Vec::new(), Err(e)),
        },
    };
    for o in cap.outputs {
        let probe = match stream.resolved_identity(&o.table) {
            Some((schema, table)) => format!("{schema}.{table}"),
            None => o.table.clone(),
        };
        let subject = format!("export '{}' table '{}'", cap.export_name, o.table);
        let readable = stream.readable_columns(&o.table);
        probes.insert(
            o.table.clone(),
            (probe.clone(), o.overrides.clone(), readable.clone()),
        );
        let columns = match resolver
            .resolve(&probe, &o.overrides, readable.as_ref())
            .and_then(|c| crate::types::plan_columns(c, &cap.policy, &subject))
        {
            Ok(c) => c,
            Err(e) => return (Vec::new(), Err(e)),
        };
        if let Err(e) = cap
            .schema_gate
            .map_or(Ok(()), |gate| gate(&o.table, &columns))
        {
            return (Vec::new(), Err(e));
        }
        let key = match o.key {
            Some(k) => match undeclared_key_column(&k, &columns) {
                Some(missing) => {
                    return (
                        Vec::new(),
                        Err(crate::error::coded(
                            crate::error::codes::LOAD_KEY_COLUMN_INVALID,
                            format!(
                                "export '{}' table '{}': `load.pk` names `{missing}`, which the table \
                             does not have, so rivet cannot tell an UPDATE that changes the key \
                             from one that does not. Fix `pk:` in the `load:` block.",
                                cap.export_name, o.table
                            ),
                        )),
                    );
                }
                None => k,
            },
            None => match resolver.source().primary_key(&probe) {
                Ok(k) => k.unwrap_or_default(),
                Err(e) => return (Vec::new(), Err(e)),
            },
        };
        if let Some(w) =
            sink::unbudgetable_partition_warning(&o.table, o.partition.as_ref(), &columns)
        {
            log::warn!("{w}");
        }
        outputs.push(sink::TableOutput {
            table: o.table,
            columns,
            dest: o.dest,
            dest_uri: o.dest_uri,
            row_hash: o.row_hash,
            partition: o.partition,
            partition_guard: o.partition_guard,
            key,
            overridden: o.overrides.keys().cloned().collect(),
        });
    }
    let (policy, schema_gate, export_name) =
        (&cap.policy, cap.schema_gate, cap.export_name.clone());
    let reschema: sink::Reschema<'_> = Box::new(move |table: &str| {
        let (probe, overrides, readable) = probes.get(table).ok_or_else(|| {
            crate::error::coded(
                crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                format!("cdc: no probe recorded for table `{table}`"),
            )
        })?;
        let subject = format!("export '{export_name}' table '{table}'");
        let columns = resolver
            .resolve(probe, overrides, readable.as_ref())
            .and_then(|c| crate::types::plan_columns(c, policy, &subject))?;
        if let Some(gate) = schema_gate {
            gate(table, &columns)?;
        }
        Ok(columns)
    });
    let sink_cfg = sink::SinkConfig {
        export_name: cap.export_name,
        outputs,
        engine,
        format: cap.format,
        checkpoint,
        max_events: cap.max_events,
        rollover: cap.rollover,
        rollover_memory_bytes: cap.rollover_memory_bytes,
        started_at: cap.started_at,
        run_id: cap.run_id,
        state: cap.state,
        read_bytes: std::sync::Arc::clone(read_bytes),
        reschema: Some(reschema),
    };
    sink::run_to_files(stream.as_mut(), sink_cfg)
}
