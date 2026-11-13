//! CDC file sink — canonical change stream → typed Arrow `RecordBatch` → a temp part → the existing **commit seam** (`write_part_file` → `Destination`), then a `RunManifest` + `_SUCCESS` at clean end.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tempfile::NamedTempFile;

use crate::config::{CompressionType, FormatType};
use crate::destination::Destination;
use crate::error::Result;
use crate::manifest::{
    MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, ManifestStatus,
    PartStatus, RunManifest, file_token,
};
use crate::pipeline::commit::{PartRecord, write_part_file};
use crate::pipeline::manifest_writer::{write_manifest, write_manifest_without_success_marker};
use crate::source::cdc::identity::table_matches;
use crate::source::cdc::value::{self, RivetValue};
use crate::source::cdc::{
    CdcEngine, ChangeEvent, ChangeStream, Position, ResidentBytes, TxnSeq,
    named_image_arity_mismatch, partition_guard,
};
use crate::types::{TypeMapping, build_arrow_field};

/// One table's wiring in a (possibly multi-table) CDC run: where its parts go and its resolved column types.
pub(crate) struct TableOutput<'a> {
    pub table: String,
    /// Resolved source column type mappings — carry the Arrow type *and* its logical-type metadata (`json`/`uuid`/…), so the sink writes the same typed columns the batch export does (via [`build_arrow_field`]).
    pub columns: Vec<TypeMapping>,
    pub dest: &'a dyn Destination,
    pub dest_uri: String,
    /// `_rivet_row_hash` on the drain, using the same Rust render the snapshot leg applies (repair-design.md §5h).
    pub row_hash: crate::config::RowHash,
    /// Keep each part within this many distinct partitions and note the count in its footer.
    pub partition: Option<crate::plan::rollover::PartitionRollover>,
    /// The partition key a change must not move (base-and-buffer layout only).
    pub partition_guard: Option<partition_guard::PartitionGuard>,
    /// The key the load merges by; an UPDATE that changes it is written as a delete and an insert.
    pub key: Vec<String>,
    /// Columns this table's `columns:` overrides name; a refused cell of one is an override mismatch.
    pub overridden: std::collections::HashSet<String>,
}

/// Everything the sink needs that isn't the stream itself.
pub(crate) struct SinkConfig<'a> {
    /// The rivet EXPORT these tables belong to (`exports[].name`).
    pub export_name: String,
    pub outputs: Vec<TableOutput<'a>>,
    pub engine: CdcEngine,
    pub format: FormatType,
    pub checkpoint: Option<PathBuf>,
    pub max_events: Option<usize>,
    pub rollover: usize,
    /// Roll a part once the buffered changes reach this many bytes (estimated), whichever comes first with `rollover`.
    pub rollover_memory_bytes: Option<usize>,
    /// RFC3339 start time (passed in — `Utc::now()` is the caller's to stamp).
    pub started_at: String,
    /// RFC3339 stamp used as both `finished_at` and the run id seed.
    pub run_id: String,
    /// The central ledger, so every part reaches the DATABASE as it becomes durable.
    pub state: Option<&'a crate::state::StateStore>,
    /// Cumulative DECODED bytes READ from the source this run (#196).
    pub read_bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// A captured table's columns as the source holds them NOW, judged by `on_schema_drift`.
    pub reschema: Option<Reschema<'a>>,
}

/// Re-resolve one captured table's columns (and gate them by `on_schema_drift`) mid-run.
pub(crate) type Reschema<'a> = Box<dyn FnMut(&str) -> Result<Vec<TypeMapping>> + 'a>;

/// The columns `ev` names that `columns` does not have and that are not in `gone` (names the table's catalog no longer has).
fn columns_the_schema_lacks(
    columns: &[TypeMapping],
    gone: &std::collections::HashSet<String>,
    ev: &ChangeEvent,
) -> Vec<String> {
    let mut lacking: Vec<String> = Vec::new();
    for name in ev
        .image_names
        .iter()
        .chain(ev.before_names.iter())
        .flat_map(|n| n.iter())
    {
        if !columns.iter().any(|c| &c.column_name == name)
            && !gone.contains(name)
            && !lacking.contains(name)
        {
            lacking.push(name.clone());
        }
    }
    lacking
}

/// What to do with the names a change carries that the run's schema lacks, given the table's columns as the catalog holds them now (`fresh`, `None` when they cannot be read again).
fn place_lacking(
    lacking: &[String],
    fresh: Option<&[TypeMapping]>,
    schema_arity: usize,
    image_arity: usize,
) -> std::result::Result<(Vec<String>, Vec<String>), ()> {
    match fresh {
        Some(fresh) => Ok(lacking
            .iter()
            .cloned()
            .partition(|n| fresh.iter().any(|c| &c.column_name == n))),
        None if image_arity == schema_arity => Ok((Vec::new(), lacking.to_vec())),
        None => Err(()),
    }
}

/// When to roll a part: at a transaction boundary, once the buffer reaches the row count OR the memory budget.
struct RolloverPolicy {
    rollover_rows: usize,
    rollover_bytes: Option<usize>,
}

impl RolloverPolicy {
    /// Never split a transaction across parts, so a part can only roll on a committed event.
    fn should_roll(&self, buf_rows: usize, buf_bytes: usize, committed: bool) -> bool {
        committed
            && (buf_rows >= self.rollover_rows
                || self.rollover_bytes.is_some_and(|b| buf_bytes >= b))
    }
}

/// Per-table sink state: the lazily-built schema, the buffered (not yet flushed) changes, and the committed parts.
struct TableSink<'a> {
    out: TableOutput<'a>,
    schema: Option<SchemaRef>,
    buf: Vec<ChangeEvent>,
    parts: Vec<PartRecord>,
    seq: usize,
    /// Finding #38: per-column value checksums, sum-accumulated across parts (the same combining rule `validate_recorded_checksums` applies on re-read.
    column_sums: std::collections::BTreeMap<String, u64>,
    /// How many of `parts` the last per-roll manifest already declared.
    manifested_parts: usize,
    /// Whether this table's `running` marker is already at the destination.
    marked: bool,
    /// The open transaction's moved-in keys, which order a statement's key moves.
    moved: partition_guard::MovedIn,
    /// Names a change carried that the table's catalog no longer has (renamed or dropped).
    gone: std::collections::HashSet<String>,
}

/// The facts one sink run shares across every table and every roll: who it is, how it writes, where it records.
struct SinkRun<'a> {
    engine: CdcEngine,
    format: FormatType,
    export_name: &'a str,
    run_token: &'a str,
    run_id: &'a str,
    started_at: &'a str,
    checkpoint: Option<&'a Path>,
    state: Option<&'a crate::state::StateStore>,
}

impl TableSink<'_> {
    /// This table's `running` marker: its manifest before any part exists, under the run's own key.
    fn running_marker(&self, run: &SinkRun<'_>) -> RunManifest {
        RunManifest {
            status: ManifestStatus::Running,
            finished_at: String::new(),
            ..self.manifest(run)
        }
    }

    /// Closes this table's `running` marker of a failed run.
    fn close_marker(&self, run: &SinkRun<'_>) -> Result<()> {
        let marker = self.running_marker(run);
        if seeded!("cdc-run-close-marker", true, !self.parts.is_empty()) {
            let failed = RunManifest {
                status: ManifestStatus::Failed,
                ..marker
            };
            return write_manifest_without_success_marker(self.out.dest, &failed).map(|_| ());
        }
        if !self
            .out
            .dest
            .capabilities()
            .commit_protocol
            .leaves_objects_at_rest()
        {
            return Ok(());
        }
        self.out
            .dest
            .remove(&crate::manifest::run_unique_manifest_name(&marker.run_id))
    }

    /// This table's run manifest over the parts it has so far.
    fn manifest(&self, run: &SinkRun<'_>) -> RunManifest {
        build_manifest(
            run.engine,
            &self.column_sums,
            &self.out,
            run.export_name,
            run.format,
            run.run_id,
            run.started_at,
            &self.parts,
        )
    }

    /// Build this table's schema before its first flush, so the concurrent encode only reads the sink.
    fn prepare_schema(&mut self) -> Result<()> {
        ensure_schema(
            &mut self.schema,
            &mut self.out.columns,
            &self.buf,
            &self.out.row_hash,
        )
        .map(|_| ())
    }

    /// Encode + upload this table's buffered changes as one part per partition-budget slice.
    fn encode_and_upload(
        &self,
        engine: CdcEngine,
        format: FormatType,
        run_token: &str,
    ) -> FlushedParts {
        let sch = self
            .schema
            .as_ref()
            .expect("prepare_schema runs before every flush");
        match flush(
            &self.buf,
            sch,
            &self.out.columns,
            engine,
            &self.out.row_hash,
            self.out.partition.as_ref(),
            &self.out.overridden,
        ) {
            Ok((batch, slices)) => upload_slices(
                &batch,
                slices,
                sch,
                &self.out.columns,
                format,
                run_token,
                self.seq,
                self.out.dest,
            ),
            Err(e) => (Vec::new(), Err(e)),
        }
    }

    /// Account for a part that is durable at the destination: checksums, ledger, part list, buffer.
    fn record_part(
        &mut self,
        part: PartRecord,
        sums: Vec<(String, u64)>,
        format: FormatType,
        ledger: Option<(&crate::state::StateStore, &str, &str)>,
    ) {
        for (name, sum) in sums {
            let e = self.column_sums.entry(name).or_insert(0);
            *e = e.wrapping_add(sum);
        }
        let ledger_name = ledger_part_name(&self.out.table, &part.file_name);
        if let Some((state, export_name, run_id)) = ledger
            && let Err(e) = state.record_durable_part(crate::state::DurablePart {
                run_id,
                export_name,
                file_name: &ledger_name,
                rows: part.rows,
                bytes: part.bytes as i64,
                format: format.label(),
                compression: Some(part_compression(format).label()),
                mode: "cdc",
                cursor_high: None,
            })
        {
            log::warn!(
                "cdc: ledger write failed for '{}' (the part IS durable): {e:#}",
                part.file_name
            );
        }
        self.parts.push(part);
        self.seq += 1;
        self.buf.clear();
        self.moved.reset();
    }
}

/// The **durable sequence** for one roll — the invariant that makes the run at-least-once.
pub(crate) fn roll_wrote_a_part(part_counts: &[usize]) -> bool {
    part_counts.iter().any(|&n| n > 0)
}

#[allow(clippy::too_many_arguments)]
fn roll_all(
    sinks: &mut [TableSink<'_>],
    stream: &mut dyn ChangeStream,
    run: &SinkRun<'_>,
    last_commit: &Option<Position>,
    unacked_commit: &mut bool,
) -> Result<()> {
    let pending: Vec<usize> = (0..sinks.len())
        .filter(|&i| !sinks[i].buf.is_empty())
        .collect();
    for &i in &pending {
        sinks[i].prepare_schema()?;
    }
    let unmarked: Vec<usize> = pending
        .iter()
        .copied()
        .filter(|&i| !sinks[i].marked)
        .collect();
    let markers: Vec<(&dyn crate::destination::Destination, RunManifest)> = unmarked
        .iter()
        .map(|&i| (sinks[i].out.dest, sinks[i].running_marker(run)))
        .collect();
    crate::workers::run_each(&markers, |(dest, marker)| {
        write_manifest_without_success_marker(*dest, marker).map(|_| ())
    })
    .into_iter()
    .collect::<Result<()>>()?;
    drop(markers);
    for i in unmarked {
        sinks[i].marked = true;
    }
    let uploaded = {
        let view: Vec<&TableSink<'_>> = pending.iter().map(|&i| &sinks[i]).collect();
        let (engine, format, run_token) = (run.engine, run.format, run.run_token);
        crate::workers::run_each(
            &view,
            |s| Ok(s.encode_and_upload(engine, format, run_token)),
        )
    };
    let mut first_err = None;
    let mut part_counts = Vec::new();
    for (i, flushed) in pending.into_iter().zip(uploaded) {
        let (parts, outcome) = flushed.unwrap_or_else(|panicked| (Vec::new(), Err(panicked)));
        part_counts.push(parts.len());
        for (part, sums) in parts {
            sinks[i].record_part(
                part,
                sums,
                run.format,
                run.state.map(|st| (st, run.export_name, run.run_id)),
            );
        }
        if let Err(e) = outcome {
            first_err.get_or_insert(e);
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    let wrote_parts = roll_wrote_a_part(&part_counts);
    crate::test_hook::maybe_panic_at("cdc_after_flush_before_ack");
    if wrote_parts {
        crate::test_hook::maybe_pause_at("cdc_after_parts_before_ack");
    }
    if *unacked_commit && let Some(p) = last_commit {
        let dirty: Vec<usize> = (0..sinks.len())
            .filter(|&i| sinks[i].parts.len() != sinks[i].manifested_parts)
            .collect();
        let pending: Vec<(&dyn crate::destination::Destination, RunManifest)> = dirty
            .iter()
            .map(|&i| (sinks[i].out.dest, sinks[i].manifest(run)))
            .collect();
        crate::workers::run_each(&pending, |(dest, manifest)| {
            write_manifest_without_success_marker(*dest, manifest).map(|_| ())
        })
        .into_iter()
        .collect::<Result<()>>()?;
        drop(pending);
        for i in dirty {
            sinks[i].manifested_parts = sinks[i].parts.len();
        }
        if let Some(ck) = run.checkpoint {
            stream.checkpoint_of(p).save(ck)?;
        }
        crate::test_hook::maybe_panic_at("cdc_after_checkpoint_before_ack");
        stream.ack(p)?;
        crate::test_hook::maybe_panic_at("cdc_after_ack");
        if wrote_parts {
            crate::test_hook::maybe_panic_at("cdc_after_ack_of_parts");
        }
        *unacked_commit = false;
    }
    Ok(())
}

/// Stream canonical changes to typed Parquet/CSV parts.
pub(crate) fn run_to_files(
    stream: &mut dyn ChangeStream,
    cfg: SinkConfig<'_>,
) -> (Vec<RunManifest>, Result<()>) {
    let run_token = file_token(&cfg.run_id);
    let read_bytes = std::sync::Arc::clone(&cfg.read_bytes);
    let mut reschema = cfg.reschema;
    let mut sinks: Vec<TableSink<'_>> = cfg
        .outputs
        .into_iter()
        .map(|out| TableSink {
            out,
            schema: None,
            buf: Vec::new(),
            parts: Vec::new(),
            seq: 0,
            column_sums: std::collections::BTreeMap::new(),
            manifested_parts: 0,
            marked: false,
            moved: Default::default(),
            gone: Default::default(),
        })
        .collect();

    let policy = RolloverPolicy {
        rollover_rows: cfg.rollover,
        rollover_bytes: cfg.rollover_memory_bytes,
    };
    let checkpoint = cfg.checkpoint.as_deref();
    let run = SinkRun {
        engine: cfg.engine,
        format: cfg.format,
        export_name: &cfg.export_name,
        run_token: &run_token,
        run_id: &cfg.run_id,
        started_at: &cfg.started_at,
        checkpoint,
        state: cfg.state,
    };
    let (mut total_rows, mut emitted) = (0usize, 0usize);
    let mut total_bytes = ResidentBytes::default();
    let mut last_commit: Option<Position> = None;
    let mut unacked_commit = false;
    let mut txn_seq = TxnSeq::default();

    let mut hit_max = false;
    let drain: Result<()> = (|| {
        loop {
            let mut yielded_this_pass = 0usize;
            while let Some(ev) = stream.next_change() {
                let mut ev = ev?;
                yielded_this_pass += 1;
                txn_seq.stamp(&mut ev);
                let committed = ev.committed;
                let commit_at = committed.then(|| ev.position.clone());
                let mut buffered = false;
                if let Some(at) = sinks
                    .iter()
                    .position(|s| table_matches(cfg.engine, &s.out.table, &ev.schema, &ev.table))
                {
                    ev.raise_poison()?;
                    let lacking =
                        columns_the_schema_lacks(&sinks[at].out.columns, &sinks[at].gone, &ev);
                    if !lacking.is_empty() {
                        let table = sinks[at].out.table.clone();
                        let fresh = match reschema.as_mut() {
                            Some(re) => Some(re(&table)?),
                            None => None,
                        };
                        let image_arity = ev.image_names.as_ref().map_or(0, |n| n.len());
                        let Ok((added, gone)) = place_lacking(
                            &lacking,
                            fresh.as_deref(),
                            sinks[at].out.columns.len(),
                            image_arity,
                        ) else {
                            crate::rivet_bail!(
                                crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                                "cdc: table `{table}` gained column(s) {} while the run was open, and \
                                 its catalog could not be read again, so their values cannot be typed",
                                lacking.join(", ")
                            );
                        };
                        if !gone.is_empty() {
                            log::warn!(
                                "cdc: table `{table}`: change(s) carry column(s) {} the table no \
                                 longer has (renamed or dropped while the run read them)",
                                gone.join(", ")
                            );
                            sinks[at].gone.extend(gone);
                        }
                        if let (false, Some(fresh)) = (added.is_empty(), fresh) {
                            roll_all(&mut sinks, stream, &run, &last_commit, &mut unacked_commit)?;
                            total_rows = 0;
                            total_bytes.reset();
                            log::warn!(
                                "cdc: table `{table}` gained column(s) {} while the run was open: \
                                 its earlier changes are in a part without them, the rest continue \
                                 in a new part with them",
                                added.join(", ")
                            );
                            sinks[at].out.columns = fresh;
                            sinks[at].schema = None;
                            sinks[at].gone.clear();
                        }
                    }
                    let sink = &mut sinks[at];
                    read_bytes.fetch_add(
                        ev.payload_bytes() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    let split = partition_guard::split_move(
                        &ev,
                        &sink.out.key,
                        sink.out.partition_guard.as_ref(),
                        &sink.out.columns,
                        cfg.engine,
                    )?;
                    let routed = match split {
                        Some((delete, mut insert)) => {
                            insert.seq = txn_seq.next(&insert.position);
                            vec![(delete, false), (insert, true)]
                        }
                        None => vec![(ev, false)],
                    };
                    for (ev, moved_in) in routed {
                        total_bytes.add(&ev);
                        sink.moved.push(&mut sink.buf, ev, &sink.out.key, moved_in);
                        total_rows += 1;
                        emitted += 1;
                    }
                    buffered = true;
                }
                if let Some(at) = commit_at {
                    last_commit = Some(at);
                    unacked_commit = true;
                }
                if buffered && policy.should_roll(total_rows, total_bytes.bytes(), committed) {
                    roll_all(&mut sinks, stream, &run, &last_commit, &mut unacked_commit)?;
                    total_rows = 0;
                    total_bytes.reset();
                }
                if committed && cfg.max_events.is_some_and(|m| emitted >= m) {
                    hit_max = true;
                    break;
                }
            }
            let buffered_rows: usize = sinks.iter().map(|s| s.buf.len()).sum();
            if pass_must_roll(unacked_commit, buffered_rows) {
                roll_all(&mut sinks, stream, &run, &last_commit, &mut unacked_commit)?;
                total_rows = 0;
                total_bytes.reset();
            }
            if drain_is_complete(hit_max, yielded_this_pass) {
                break;
            }
        }
        let tables: Vec<String> = sinks.iter().map(|s| s.out.table.clone()).collect();
        if let crate::source::cdc::RowImage::Partial { why } = stream.row_image(&tables) {
            return Err(crate::source::cdc::open_partial_image_refusal(
                run.engine, &why,
            ));
        }
        if !hit_max
            && !unacked_commit
            && let (Some(ck), Some(frontier)) = (run.checkpoint, stream.drained_frontier())
        {
            stream.checkpoint_of(&frontier).save(ck)?;
        }
        Ok(())
    })();

    crate::test_hook::maybe_panic_at("cdc_before_manifest");

    let manifests: Vec<RunManifest> = sinks.iter().map(|s| s.manifest(&run)).collect();
    let write_err: Option<anyhow::Error> = if drain.is_ok() {
        let jobs: Vec<(&dyn crate::destination::Destination, &RunManifest)> = sinks
            .iter()
            .zip(&manifests)
            .map(|(s, m)| (s.out.dest, m))
            .collect();
        crate::workers::run_each(&jobs, |(dest, manifest)| {
            write_manifest(*dest, manifest).map(|_| ())
        })
        .into_iter()
        .collect::<Result<()>>()
        .err()
    } else {
        for s in sinks.iter().filter(|s| s.marked && s.manifested_parts == 0) {
            if let Err(e) = s.close_marker(&run) {
                log::warn!(
                    "cdc: could not close the running marker of '{}': {e:#}",
                    s.out.table
                );
            }
        }
        None
    };
    match (drain, write_err) {
        (Err(e), _) => (manifests, Err(e)),
        (Ok(()), Some(e)) => (manifests, Err(e)),
        (Ok(()), None) => (manifests, Ok(())),
    }
}

/// Build the sink schema once, on the first flush.
fn ensure_schema(
    schema: &mut Option<SchemaRef>,
    columns: &mut [TypeMapping],
    events: &[ChangeEvent],
    row_hash: &crate::config::RowHash,
) -> Result<SchemaRef> {
    if schema.is_none() {
        refine_decimal_scales(columns, events);
        let mut fields = vec![
            Field::new("__op", DataType::Utf8, false),
            Field::new("__pos", DataType::Utf8, false),
            Field::new("__seq", DataType::Int64, false),
        ];
        for m in columns.iter() {
            match build_arrow_field(m) {
                Some(f) if value::is_buildable(f.data_type()) => fields.push(f),
                _ => crate::rivet_bail!(
                    crate::error::codes::INTERNAL_TYPE_BUILDER,
                    "cdc: column '{}' is planned as {:?}, which the CDC builder cannot build",
                    m.column_name,
                    m.arrow_type
                ),
            }
        }
        if row_hash.enabled() {
            fields.push(Field::new(
                crate::enrich::COL_ROW_HASH,
                DataType::Int64,
                false,
            ));
        }
        *schema = Some(Arc::new(Schema::new(fields)));
    }
    Ok(schema.clone().unwrap())
}

/// Fill a `Decimal128` column's scale from the data when the resolved scale is the `0` placeholder (SQL Server).
fn refine_decimal_scales(columns: &mut [TypeMapping], events: &[ChangeEvent]) {
    for (i, m) in columns.iter_mut().enumerate() {
        let Some(DataType::Decimal128(p, 0)) = m.arrow_type else {
            continue;
        };
        let scale = events
            .iter()
            .filter_map(|e| {
                let img = if e.op.values_live_in_before() {
                    e.before.as_ref()
                } else {
                    e.after.as_ref()
                };
                img.and_then(|v| v.get(i))
            })
            .filter_map(|rv| match rv {
                RivetValue::Bytes(b) => std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.split_once('.').map(|(_, f)| f.len())),
                _ => None,
            })
            .max();
        if let Some(s) = scale {
            m.arrow_type = Some(DataType::Decimal128(p, s as i8));
        }
    }
}

/// One cell of a row image, resolved by NAME (`flush` refuses a nameless image first).
pub(crate) fn image_cell<'e>(
    e: &'e ChangeEvent,
    i: usize,
    col: &str,
    schema: &[&str],
    memo: Option<(&std::sync::Arc<[String]>, Option<usize>)>,
) -> Option<&'e RivetValue> {
    let vals = if e.op.values_live_in_before() {
        e.before.as_ref()?
    } else {
        e.after.as_ref()?
    };
    let names = e.image_names.as_ref()?;
    match memo
        .filter(|(m, _)| std::sync::Arc::ptr_eq(m, names))
        .map(|(_, j)| j)
        .unwrap_or_else(|| names.iter().position(|n| n == col))
    {
        Some(j) => vals.get(j),
        None if vals.len() == schema.len()
            && names
                .get(i)
                .is_some_and(|old| !schema.contains(&old.as_str())) =>
        {
            vals.get(i)
        }
        None => None,
    }
}

/// Must this drain pass ROLL — flush, checkpoint and ack?
pub(crate) fn pass_must_roll(unacked_commit: bool, buffered_rows: usize) -> bool {
    unacked_commit || buffered_rows > 0
}

/// Has the drain loop finished — the cap, or a pass that yielded nothing?
pub(crate) fn drain_is_complete(hit_max: bool, yielded_this_pass: usize) -> bool {
    hit_max || yielded_this_pass == 0
}

/// The O(1) name-lookup memo `image_cell` takes.
pub(crate) fn image_name_memo<'a>(
    events: &'a [ChangeEvent],
    col: &str,
) -> Option<(&'a std::sync::Arc<[String]>, Option<usize>)> {
    let names = events.iter().find_map(|e| e.image_names.as_ref())?;
    Some((names, names.iter().position(|n| n == col)))
}

#[allow(clippy::too_many_arguments)]
fn flush(
    events: &[ChangeEvent],
    schema: &SchemaRef,
    columns: &[TypeMapping],
    engine: CdcEngine,
    row_hash: &crate::config::RowHash,
    partition: Option<&crate::plan::rollover::PartitionRollover>,
    overridden: &std::collections::HashSet<String>,
) -> Result<EncodedBatch> {
    let ops: ArrayRef = Arc::new(
        events
            .iter()
            .map(|e| Some(e.op.as_str()))
            .collect::<StringArray>(),
    );
    let poss: ArrayRef = Arc::new(
        events
            .iter()
            .map(|e| Some(e.position.json().to_string()))
            .collect::<StringArray>(),
    );
    let seqs: ArrayRef = Arc::new(events.iter().map(|e| e.seq as i64).collect::<Int64Array>());
    for ev in events {
        if ev.image_names.is_none() {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_UNDECODABLE,
                "cdc: {}.{}: a change image carries no column names; rivet never maps values \
                 by position, which puts them under the wrong names after a column reorder or ALTER.",
                ev.schema,
                ev.table
            );
        }
        if let Some((n, nnames)) = named_image_arity_mismatch(ev) {
            return Err(crate::source::cdc::partial_image_refusal(
                &ev.schema, &ev.table, n, nnames,
            ));
        }
    }

    let mut arrays: Vec<ArrayRef> = vec![ops, poss, seqs];
    let schema_names: Vec<&str> = columns.iter().map(|m| m.column_name.as_str()).collect();
    for (i, m) in columns.iter().enumerate() {
        let dt = schema.field(arrays.len()).data_type();
        let fix = value::mysql_cell_fix(engine, &m.source_native_type).filter(|f| f.applies_to(dt));
        let memo = image_name_memo(events, &m.column_name);
        let refuse = |r: value::CellRefusal| {
            r.into_error(&m.column_name, dt, overridden.contains(&m.column_name))
        };
        let owned: Option<Vec<Option<RivetValue>>> = match &fix {
            Some(fix) => Some(
                events
                    .iter()
                    .enumerate()
                    .map(|(row, e)| {
                        image_cell(e, i, &m.column_name, &schema_names, memo)
                            .map(|v| {
                                fix.apply(v).map_err(|r| value::CellRefusal {
                                    row,
                                    value: v.clone(),
                                    reason: r.reason,
                                    code: r.code,
                                })
                            })
                            .transpose()
                    })
                    .collect::<std::result::Result<_, _>>()
                    .map_err(refuse)?,
            ),
            None => None,
        };
        if let (Some(o), Some(fix)) = (&owned, &fix) {
            let nulled = events
                .iter()
                .zip(o)
                .filter(|(e, out)| {
                    match (out, image_cell(e, i, &m.column_name, &schema_names, memo)) {
                        (Some(out), Some(input)) => fix.nulled_a_date(input, out),
                        _ => false,
                    }
                })
                .count();
            if nulled > 0 {
                log::warn!(
                    "{}",
                    crate::source::mysql::zero_date_warning(&m.column_name, nulled as u64)
                );
            }
        }
        let cells: Vec<Option<&RivetValue>> = match &owned {
            Some(o) => o.iter().map(|c| c.as_ref()).collect(),
            None => events
                .iter()
                .map(|e| image_cell(e, i, &m.column_name, &schema_names, memo))
                .collect(),
        };
        let arr = value::build_column(dt, &cells).map_err(refuse)?;
        let source_sum = value::cells_checksum(dt, &cells);
        let arrow_sum = crate::source::value_checksum::array_checksum(arr.as_ref());
        if source_sum != arrow_sum {
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_VALUE_CONVERTER,
                "cdc value checksum mismatch in column '{}': source={source_sum} \
                 arrow={arrow_sum} — the value converter changed a value between \
                 decode and Arrow build",
                m.column_name
            );
        }
        arrays.push(arr);
    }
    let batch = if row_hash.enabled() {
        let data: Vec<String> = columns.iter().map(|m| m.column_name.clone()).collect();
        let covered = crate::enrich::row_hash_columns_of(&data, row_hash)?;
        let base = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .take(arrays.len())
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let bare = RecordBatch::try_new(base, arrays.clone())?;
        arrays.push(crate::enrich::row_hash_array(&bare, &covered)?);
        RecordBatch::try_new(schema.clone(), arrays)?
    } else {
        RecordBatch::try_new(schema.clone(), arrays)?
    };
    crate::source::refuse_decimal_overflow(&batch)?;

    let slices = match partition
        .and_then(|r| crate::pipeline::batch_partition_buckets(&batch, r).map(|b| (r, b)))
    {
        Some((r, buckets)) => budget_slices(&buckets, r.cap)
            .into_iter()
            .map(|(rows, n)| {
                let note =
                    crate::plan::rollover::partition_buckets_note(&r.column, r.granularity, n);
                (rows, Some(note))
            })
            .collect(),
        None => vec![(0..batch.num_rows(), None)],
    };
    Ok((batch, slices))
}

/// Write + upload each slice as one part, stopping at the first failure.
#[allow(clippy::too_many_arguments)]
fn upload_slices(
    batch: &RecordBatch,
    slices: Vec<Slice>,
    schema: &SchemaRef,
    columns: &[TypeMapping],
    format: FormatType,
    run_token: &str,
    seq: usize,
    dest: &dyn Destination,
) -> FlushedParts {
    let mut parts = Vec::with_capacity(slices.len());
    for (k, (rows, note)) in slices.into_iter().enumerate() {
        let one = || -> Result<(PartRecord, Vec<(String, u64)>)> {
            let data = batch.slice(rows.start, rows.len());
            let tmp = NamedTempFile::new()?;
            let fmt = crate::format::create_format(format, part_compression(format), None, None);
            let writer: Box<dyn std::io::Write + Send> = Box::new(tmp.reopen()?);
            let mut w = fmt.create_writer(schema, writer)?;
            w.write_batch(&data)?;
            if let Some(note) = &note {
                w.note(crate::plan::rollover::PARTITION_BUCKETS_KEY, note);
            }
            w.finish()?;
            let file_name = format!("cdc-{run_token}-{:06}.{}", seq + k, format.label());
            let landed = std::sync::atomic::AtomicU64::new(0);
            let part = write_part_file(dest, tmp.path(), rows.len() as i64, file_name, &landed)?;
            Ok((part, slice_column_sums(&data, columns)))
        };
        match one() {
            Ok(p) => parts.push(p),
            Err(e) => return (parts, Err(e)),
        }
    }
    (parts, Ok(()))
}

/// The source-column checksums of the rows one part holds (the meta columns and row hash excluded).
fn slice_column_sums(part: &RecordBatch, columns: &[TypeMapping]) -> Vec<(String, u64)> {
    columns
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let sum = crate::source::value_checksum::array_checksum(part.column(3 + i).as_ref());
            (m.column_name.clone(), sum)
        })
        .collect()
}

/// The warning for a declared partition this table's parts cannot be budgeted by.
pub(crate) fn unbudgetable_partition_warning(
    table: &str,
    rollover: Option<&crate::plan::rollover::PartitionRollover>,
    columns: &[TypeMapping],
) -> Option<String> {
    use arrow::datatypes::DataType;
    let r = rollover?;
    let dated = columns.iter().any(|c| {
        c.column_name == r.column
            && matches!(
                c.arrow_type,
                Some(DataType::Date32 | DataType::Date64 | DataType::Timestamp(..))
            )
    });
    (!dated).then(|| {
        format!(
            "cdc table '{table}': the load partitions by `{}`, which this stream does not write \
             as a date or timestamp column — its change parts cannot be kept within the \
             {}-partition load budget, and a wide history will be refused at load time",
            r.column, r.cap
        )
    })
}

/// Consecutive row ranges of `buckets`, each within `cap` distinct partitions, with the count it holds.
fn budget_slices(buckets: &[i64], cap: usize) -> Vec<(std::ops::Range<usize>, usize)> {
    let mut out = Vec::new();
    let mut start = 0;
    while start < buckets.len() {
        let fit = crate::plan::rollover::rows_that_fit(
            &std::collections::HashSet::new(),
            &buckets[start..],
            cap,
        );
        let end = start + fit;
        let held: std::collections::HashSet<i64> = buckets[start..end].iter().copied().collect();
        out.push((start..end, held.len()));
        start = end;
    }
    out
}

/// One part's row range and its partition note.
type Slice = (std::ops::Range<usize>, Option<String>);

/// A flush's encoded batch and the slices it is cut into.
type EncodedBatch = (RecordBatch, Vec<Slice>);

/// The parts a flush made durable, each with its own column checksums, and whether it finished.
type FlushedParts = (Vec<(PartRecord, Vec<(String, u64)>)>, Result<()>);

/// The name a CDC part is recorded under in `file_log`.
pub(crate) fn ledger_part_name(table: &str, file_name: &str) -> String {
    format!("{table}/{file_name}")
}

/// How a CDC part is compressed, per format — the ONE place that decides.
pub(crate) fn part_compression(format: FormatType) -> CompressionType {
    match format {
        FormatType::Csv => CompressionType::None,
        FormatType::Parquet => CompressionType::Zstd,
    }
}

/// Assemble one table's `RunManifest` from its committed parts (hand-built.
#[allow(clippy::too_many_arguments)]
fn build_manifest(
    engine: CdcEngine,
    column_sums: &std::collections::BTreeMap<String, u64>,
    out: &TableOutput<'_>,
    export_name: &str,
    format: FormatType,
    run_id: &str,
    started_at: &str,
    parts: &[PartRecord],
) -> RunManifest {
    RunManifest {
        manifest_version: MANIFEST_VERSION,
        split_window: None,
        mode: "cdc".to_string(),
        run_id: run_id.to_string(),
        export_family: export_name.to_string(),
        export_name: out.table.clone(),
        started_at: started_at.to_string(),
        finished_at: chrono::Utc::now().to_rfc3339(),
        status: ManifestStatus::Success,
        source: ManifestSource {
            extraction: Some(crate::manifest::ExtractionMetadata {
                strategy: "cdc".to_string(),
                cursor_column: None,
                cursor_type: None,
                cursor_low: None,
                cursor_high: None,
                source_row_count: None,
            }),
            engine: engine.label().to_string(),
            schema: None,
            table: Some(out.table.clone()),
        },
        destination: ManifestDestination {
            kind: "cdc".to_string(),
            uri: out.dest_uri.clone(),
        },
        row_hash: crate::enrich::RowHashContract::of(&out.row_hash),
        format: format.label().to_string(),
        compression: part_compression(format).label().to_string(),
        schema_fingerprint: String::new(),
        row_count: parts.iter().map(|p| p.rows).sum(),
        part_count: parts.len() as u32,
        parts: parts
            .iter()
            .enumerate()
            .map(|(i, p)| ManifestPart {
                part_id: (i + 1) as u32,
                path: p.file_name.clone(),
                rows: p.rows,
                size_bytes: p.bytes,
                content_fingerprint: p.fingerprint.clone(),
                content_md5: p.md5.clone(),
                status: PartStatus::Committed,
            })
            .collect(),
        checksum_render: Some(crate::source::value_checksum::CHECKSUM_RENDER_ID.to_string()),
        column_checksums: Some(
            column_sums
                .iter()
                .map(|(name, sum)| crate::manifest::ColumnChecksum {
                    name: name.clone(),
                    checksum: sum.to_string(),
                })
                .collect(),
        ),
        checksum_key_column: None,
    }
}

#[cfg(test)]
mod partition_budget_tests {
    use super::{budget_slices, unbudgetable_partition_warning};

    #[test]
    fn each_part_checksum_is_read_from_its_own_source_column_past_the_meta_columns() {
        use crate::source::value_checksum::array_checksum;
        use crate::types::{RivetType, TypeFidelity, TypeMapping};
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::DataType;
        let col = |name: &str| TypeMapping {
            column_name: name.into(),
            source_native_type: String::new(),
            rivet_type: RivetType::Int64,
            arrow_type: Some(DataType::Int64),
            fidelity: TypeFidelity::Exact,
            nullable: true,
            warnings: vec![],
            delivery: crate::types::Delivery::Native,
            refusal: None,
        };
        let arrays: Vec<ArrayRef> = (0..5i64)
            .map(|k| {
                std::sync::Arc::new(Int64Array::from(vec![k * 10 + 1, k * 10 + 2])) as ArrayRef
            })
            .collect();
        let batch = arrow::record_batch::RecordBatch::try_from_iter(
            ["m0", "m1", "m2", "a", "b"].into_iter().zip(arrays.clone()),
        )
        .unwrap();
        assert_eq!(
            super::slice_column_sums(&batch, &[col("a"), col("b")]),
            vec![
                ("a".to_string(), array_checksum(arrays[3].as_ref())),
                ("b".to_string(), array_checksum(arrays[4].as_ref())),
            ]
        );
    }

    #[test]
    fn a_partition_the_stream_cannot_budget_is_said_and_one_it_can_is_not() {
        use crate::types::{RivetType, TypeFidelity, TypeMapping};
        use arrow::datatypes::DataType;
        let col = |name: &str, t: DataType| TypeMapping {
            column_name: name.into(),
            source_native_type: String::new(),
            rivet_type: RivetType::Int64,
            arrow_type: Some(t),
            fidelity: TypeFidelity::Exact,
            nullable: true,
            warnings: vec![],
            delivery: crate::types::Delivery::Native,
            refusal: None,
        };
        let r = crate::plan::rollover::PartitionRollover {
            column: "created_at".into(),
            granularity: crate::config::load::Granularity::Day,
            cap: 4000,
        };
        let dated = [
            col("id", DataType::Int64),
            col("created_at", DataType::Date32),
        ];
        assert_eq!(unbudgetable_partition_warning("t", Some(&r), &dated), None);
        assert_eq!(
            unbudgetable_partition_warning("t", None, &[]),
            None,
            "nothing declared"
        );
        let mongo = [col("_id", DataType::Utf8), col("document", DataType::Utf8)];
        let w = unbudgetable_partition_warning("orders", Some(&r), &mongo).expect("absent column");
        assert!(
            w.starts_with("cdc table 'orders': the load partitions by `created_at`, which this stream does not write as a date or timestamp column"),
            "{w}"
        );
        let text = [col("created_at", DataType::Utf8)];
        assert!(
            unbudgetable_partition_warning("t", Some(&r), &text).is_some(),
            "a text column of that name is not a date"
        );
    }

    #[test]
    fn a_flush_is_cut_where_the_next_row_would_open_a_partition_past_the_cap() {
        assert_eq!(
            budget_slices(&[1, 1, 2, 3, 3, 4], 2),
            vec![(0..3, 2), (3..6, 2)]
        );
        assert_eq!(budget_slices(&[1, 2, 1, 3], 2), vec![(0..3, 2), (3..4, 1)]);
        assert_eq!(budget_slices(&[], 2), vec![]);
        assert_eq!(budget_slices(&[1, 2, 3], 0), vec![(0..3, 3)]);
    }
}

#[cfg(test)]
mod tests {
    use crate::source::cdc::CdcEngine;
    use std::collections::VecDeque;

    use super::*;
    use crate::source::cdc::value::RivetValue;
    use crate::source::cdc::{ChangeEvent, ChangeOp, Position};

    #[test]
    fn rollover_policy_count_budget_and_commit_gate() {
        let by_count = RolloverPolicy {
            rollover_rows: 3,
            rollover_bytes: None,
        };
        assert!(!by_count.should_roll(2, 0, true), "under the row count");
        assert!(by_count.should_roll(3, 0, true), "hits the row count");
        assert!(
            !by_count.should_roll(9, 0, false),
            "never roll mid-transaction (uncommitted)"
        );

        let by_budget = RolloverPolicy {
            rollover_rows: 1_000_000,
            rollover_bytes: Some(100),
        };
        assert!(!by_budget.should_roll(2, 50, true), "under the byte budget");
        assert!(
            by_budget.should_roll(2, 100, true),
            "byte budget rolls before the (huge) row count"
        );
    }

    /// A fake stream that yields a fixed list of changes and records every `ack`, so the test can assert the durable sequence ran once per committed part.
    fn runaway_fuse() {
        static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        assert!(
            CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) < 100_000,
            "a test stream was polled 100_000 times — the drive loop is not \
             terminating (drain_is_complete never fired)"
        );
    }

    struct FakeStream {
        events: VecDeque<ChangeEvent>,
        acked: Vec<Position>,
    }

    impl ChangeStream for FakeStream {
        fn engine(&self) -> CdcEngine {
            CdcEngine::Postgres
        }

        fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
            runaway_fuse();
            static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            assert!(
                CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) < 10_000,
                "FakeStream polled 10_000 times — the drive loop is not \
                 terminating (drain_is_complete never fired)"
            );
            self.events.pop_front().map(Ok)
        }
        fn ack(&mut self, position: &Position) -> Result<()> {
            self.acked.push(position.clone());
            Ok(())
        }
    }

    /// A `FakeStream` that also reports a drained frontier.
    struct FrontierStream(FakeStream, Position);

    impl ChangeStream for FrontierStream {
        fn engine(&self) -> CdcEngine {
            CdcEngine::Mssql
        }
        fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
            self.0.next_change()
        }
        fn ack(&mut self, position: &Position) -> Result<()> {
            self.0.ack(position)
        }
        fn drained_frontier(&self) -> Option<Position> {
            Some(self.1.clone())
        }
    }

    #[test]
    fn an_idle_drain_checkpoints_the_drained_frontier_but_a_max_events_stop_does_not() {
        let (dir, ck_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (dest, cols) = (local_dest(&dir), int_col());
        let ck = ck_dir.path().join("ck.json");
        let frontier = Position::new(serde_json::json!({ "lsn": "FF" }));
        let run = |events: Vec<ChangeEvent>, max_events: Option<usize>| {
            let mut s = FrontierStream(
                FakeStream {
                    events: VecDeque::from(events),
                    acked: Vec::new(),
                },
                frontier.clone(),
            );
            let mut c = cfg(dest.as_ref(), &cols, FormatType::Parquet, 100);
            c.checkpoint = Some(ck.clone());
            c.max_events = max_events;
            run_to_files(&mut s, c).1.unwrap();
            Position::load(&ck).unwrap().expect("a checkpoint").0
        };
        assert_eq!(
            run(Vec::new(), None),
            frontier.0,
            "an idle bounded drain must move the checkpoint to the drained frontier"
        );
        assert_eq!(
            run(vec![insert(1), insert(2)], Some(1)),
            insert(1).position.0,
            "a max_events stop has not drained the window, so the frontier must not be saved"
        );
    }

    /// `rollover_memory_bytes` really rolls — the BYTE cap, driven end to end.
    #[test]
    fn the_byte_rollover_cap_really_rolls_and_is_not_a_dead_accumulator() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let events: Vec<ChangeEvent> = (1..=6).map(insert).collect();
        let one = {
            let mut one = crate::source::cdc::ResidentBytes::default();
            one.add(&events[0]);
            one.bytes()
        };
        assert!(
            one > 0,
            "the fixture is inert: an event must weigh something"
        );

        let mut stream = FakeStream {
            events: events.into(),
            acked: Vec::new(),
        };
        let cfg = SinkConfig {
            rollover_memory_bytes: Some(one * 2),
            ..cfg(dest.as_ref(), &cols, FormatType::Parquet, 100)
        };
        let (m, r) = run_to_files(&mut stream, cfg);
        r.unwrap();
        let manifest = m.into_iter().next().expect("one table, one manifest");
        assert_eq!(manifest.row_count, 6, "every event must still be delivered");
        assert_eq!(
            manifest.parts.iter().map(|p| p.part_id).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "CDC manifest part ids are 1-based and contiguous; a silent renumbering \
             is a wire-format change for anything joining on them"
        );
        assert_eq!(
            manifest.part_count, 3,
            "6 events at a byte cap of exactly two events' worth must land in three \
             parts: one part means the byte accumulator never moved and the cap is \
             decoration, two means it rolls one event late"
        );
    }

    #[test]
    fn commit_boundary_on_an_uncaptured_table_still_advances_the_checkpoint() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let ckpt = d.path().join("ckpt");
        let mut captured = insert(1);
        captured.committed = false;
        let mut foreign = insert(2);
        foreign.table = "audit_log".into();
        foreign.committed = true;
        let mut stream = FakeStream {
            events: vec![captured, foreign].into(),
            acked: Vec::new(),
        };
        let cfg = SinkConfig {
            export_name: "t".into(),
            checkpoint: Some(ckpt.clone()),
            ..cfg(dest.as_ref(), &cols, FormatType::Parquet, 10)
        };
        {
            let (m, r) = run_to_files(&mut stream, cfg);
            r.unwrap();
            m
        };
        assert!(
            Position::load(&ckpt).unwrap().is_some(),
            "the stream's commit boundary must advance the checkpoint even when \
             its event routes to an uncaptured table"
        );
        assert_eq!(stream.acked.len(), 1, "and the source must be acked");
    }

    #[test]
    fn poison_on_an_uncaptured_table_is_dropped_never_bails_the_run() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let mut foreign = insert(2);
        foreign.table = "audit_log".into();
        foreign.poison = Some("pg cdc: s.audit_log: column [big] unchanged-TOAST".into());
        let captured = insert(1);
        let mut stream = FakeStream {
            events: vec![foreign, captured].into(),
            acked: Vec::new(),
        };
        let cfg = cfg(dest.as_ref(), &cols, FormatType::Parquet, 10);
        {
            let (mm, r) = run_to_files(&mut stream, cfg);
            r.expect("poison on an uncaptured table must be dropped, never bail the run");
            mm
        };
    }

    #[test]
    fn poison_on_a_captured_table_bails_with_its_message() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let mut poisoned = insert(1);
        poisoned.poison = Some(
            "pg cdc: s.t: column [big] unchanged-TOAST — ALTER TABLE ... REPLICA IDENTITY FULL"
                .into(),
        );
        let mut stream = FakeStream {
            events: vec![poisoned].into(),
            acked: Vec::new(),
        };
        let cfg = cfg(dest.as_ref(), &cols, FormatType::Parquet, 10);
        let err = run_to_files(&mut stream, cfg)
            .1
            .expect_err("poison on a captured table must bail");
        assert!(
            format!("{err:#}").contains("REPLICA IDENTITY FULL"),
            "must surface the deferred message, got: {err:#}"
        );
    }

    /// A nameless image is refused even at matching arity: rivet never maps by position.
    #[test]
    fn a_nameless_image_fails_the_flush_and_writes_no_part() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let mut ev = insert(1);
        ev.image_names = None;
        let mut stream = FakeStream {
            events: vec![ev].into(),
            acked: Vec::new(),
        };
        let cfg = cfg(dest.as_ref(), &cols, FormatType::Parquet, 10);
        let (manifests, outcome) = run_to_files(&mut stream, cfg);
        let err = outcome.expect_err("a nameless image must fail the flush");
        assert!(
            manifests.iter().all(|m| m.part_count == 0),
            "no part may be written from a nameless image"
        );
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_SOURCE_CDC_UNDECODABLE")
        );
        assert!(
            err.to_string()
                .contains("s.t: a change image carries no column names"),
            "{err}"
        );
        assert!(
            stream.acked.is_empty(),
            "nothing may be acked past a refused image"
        );
    }

    struct ManifestBeforeAckStream {
        events: VecDeque<ChangeEvent>,
        dest_dir: std::path::PathBuf,
        ack_count: usize,
    }
    impl ChangeStream for ManifestBeforeAckStream {
        fn engine(&self) -> CdcEngine {
            CdcEngine::Postgres
        }

        fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
            runaway_fuse();
            self.events.pop_front().map(Ok)
        }
        fn ack(&mut self, _position: &Position) -> Result<()> {
            self.ack_count += 1;
            let manifest_path = std::fs::read_dir(&self.dest_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .find(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("manifest-") && n.ends_with(".json"))
                })
                .expect("a run-unique manifest must be durable BEFORE the slot ack (#11)");
            let m: RunManifest =
                serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
            assert_eq!(
                m.status,
                ManifestStatus::Success,
                "the pre-ack manifest must be Success — the loader ignores non-Success runs"
            );
            assert!(
                m.row_count >= 1,
                "the pre-ack manifest must cover the acked part's rows, got {}",
                m.row_count
            );
            Ok(())
        }
    }

    #[test]
    fn roast_manifest_is_durable_before_the_slot_is_acked() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = ManifestBeforeAckStream {
            events: VecDeque::from(vec![insert(1), insert(2), insert(3)]),
            dest_dir: dir.path().to_path_buf(),
            ack_count: 0,
        };
        {
            let (m, r) = run_to_files(
                &mut stream,
                cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
            );
            r.unwrap();
            m
        };
        assert!(
            stream.ack_count >= 1,
            "the fixture must exercise at least one ack"
        );
        assert!(
            dir.path().join("_SUCCESS").exists(),
            "_SUCCESS at clean end"
        );
    }

    /// A drain that commits parts and THEN fails must report those parts.
    #[test]
    fn a_failed_drain_still_reports_the_parts_it_made_durable() {
        struct FailAfter {
            events: VecDeque<ChangeEvent>,
            acked: Vec<Position>,
        }
        impl ChangeStream for FailAfter {
            fn engine(&self) -> CdcEngine {
                CdcEngine::Postgres
            }

            fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
                runaway_fuse();
                match self.events.pop_front() {
                    Some(e) => Some(Ok(e)),
                    None => Some(Err(anyhow::anyhow!("stream read failed mid-drain"))),
                }
            }
            fn ack(&mut self, position: &Position) -> Result<()> {
                self.acked.push(position.clone());
                Ok(())
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = FailAfter {
            events: VecDeque::from(vec![insert(1), insert(2), insert(3)]),
            acked: Vec::new(),
        };
        let (manifests, outcome) = run_to_files(
            &mut stream,
            cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
        );

        assert!(outcome.is_err(), "the drain must report the failure");
        assert!(
            !stream.acked.is_empty(),
            "fixture is inert — nothing was acked, so there is no durable work to forget"
        );
        let rows: i64 = manifests.iter().map(|m| m.row_count).sum();
        let parts: usize = manifests.iter().map(|m| m.part_count as usize).sum();
        assert!(
            rows > 0 && parts > 0,
            "a failed drain reported {rows} rows / {parts} parts — the durable work was \
             discarded with the error, which is exactly what made `cdc_job` record zeros"
        );
        assert!(
            !dir.path().join("_SUCCESS").exists(),
            "_SUCCESS must not be written for a run that failed"
        );
    }

    fn insert(id: i64) -> ChangeEvent {
        ChangeEvent {
            op: ChangeOp::Insert,
            schema: "s".into(),
            table: "t".into(),
            before: None,
            after: Some(vec![RivetValue::Int(id)]),
            position: Position::new(serde_json::json!({ "lsn": format!("{id:08X}") })),
            committed: true,
            image_names: Some(std::sync::Arc::from(vec!["v".to_string()])),
            seq: 0,
            poison: None,
            row_id: None,
            before_names: None,
            before_poison: None,
        }
    }

    /// Every decision `image_cell` makes, one case each.
    #[test]
    fn image_cell_resolves_by_name_and_falls_back_only_at_matching_arity() {
        use std::sync::Arc;
        let names: Arc<[String]> = Arc::from(vec!["a".to_string(), "b".to_string()]);
        let ev = |vals: Vec<RivetValue>, names: Option<Arc<[String]>>| ChangeEvent {
            after: Some(vals),
            image_names: names,
            ..insert(0)
        };

        let e = ev(
            vec![RivetValue::Int(10), RivetValue::Int(20)],
            Some(names.clone()),
        );
        assert_eq!(
            image_cell(&e, 0, "b", &["a", "b"], None),
            Some(&RivetValue::Int(20)),
            "a named image resolves by NAME; matching the first column that is NOT \
             the one asked for hands every cell its neighbour's value"
        );
        assert_eq!(
            image_cell(&e, 1, "a", &["a", "b"], None),
            Some(&RivetValue::Int(10))
        );

        let batch = [e.clone()];
        let memo = image_name_memo(&batch, "b");
        assert_eq!(
            memo.expect("the batch carries names").1,
            Some(1),
            "the memo must point at `b`'s own index; matching the first name that is \
             NOT `b` maps every row of that column to a neighbour's value"
        );
        assert_eq!(
            image_name_memo(&batch, "absent")
                .expect("the batch carries names")
                .1,
            None,
            "a column the image does not name has no memo — a Some here would index \
             the wrong cell for the whole flush"
        );
        assert!(
            image_name_memo(
                &[ChangeEvent {
                    image_names: None,
                    ..insert(0)
                }],
                "v"
            )
            .is_none(),
            "no event carries names ⇒ no memo"
        );
        assert_eq!(
            image_cell(&e, 0, "b", &["a", "b"], memo),
            Some(&RivetValue::Int(20))
        );
        let other: Arc<[String]> = Arc::from(vec!["b".to_string(), "a".to_string()]);
        assert_eq!(
            image_cell(&e, 9, "b", &["a", "b"], Some((&other, Some(0)))),
            Some(&RivetValue::Int(20)),
            "the memo is keyed by Arc identity; a stale one must fall back to the \
             search rather than index another table's layout"
        );

        assert_eq!(
            image_cell(&e, 1, "renamed", &["a", "renamed"], None),
            Some(&RivetValue::Int(20)),
            "a renamed column keeps its value at the same position while the arity \
             agrees — returning None here is a column that silently becomes NULL"
        );

        let abc = ev(
            vec![RivetValue::Int(1), RivetValue::Int(10), RivetValue::Int(20)],
            Some(Arc::from(vec![
                "id".to_string(),
                "a".to_string(),
                "b".to_string(),
            ])),
        );
        assert_eq!(
            image_cell(&abc, 2, "c", &["id", "b", "c"], None),
            None,
            "after DROP a + ADD c the image's position 2 is `b`, a live column — \
             reading it by position writes b's value into c"
        );
        assert_eq!(
            image_cell(&abc, 1, "b", &["id", "b", "c"], None),
            Some(&RivetValue::Int(20)),
            "`b` itself still resolves by name"
        );

        let three: std::sync::Arc<[String]> =
            Arc::from(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let short = ev(
            vec![RivetValue::Int(10), RivetValue::Int(20)],
            Some(three.clone()),
        );
        assert_eq!(
            image_cell(&short, 0, "absent", &["absent", "b", "c"], None),
            None,
            "a short image has no value for a column it does not name; reading it by \
             position returns the FIRST column's value under another column's name"
        );
        assert_eq!(
            image_cell(&short, 2, "absent", &["a", "b", "absent"], None),
            None
        );

        let del = ChangeEvent {
            op: ChangeOp::Delete,
            before: Some(vec![RivetValue::Int(7), RivetValue::Int(8)]),
            after: None,
            image_names: Some(names.clone()),
            ..insert(0)
        };
        assert_eq!(
            image_cell(&del, 0, "b", &["a", "b"], None),
            Some(&RivetValue::Int(8))
        );
        assert_eq!(
            image_cell(&del, 0, "a", &["a", "b"], None),
            Some(&RivetValue::Int(7))
        );

        let anon = ev(vec![RivetValue::Int(10), RivetValue::Int(20)], None);
        assert_eq!(image_cell(&anon, 1, "b", &["a", "b"], None), None);
    }

    /// The drain loop's two decisions, every combination.
    #[test]
    fn a_roll_wrote_a_part_only_when_some_sink_flushed_one() {
        assert!(!roll_wrote_a_part(&[]));
        assert!(!roll_wrote_a_part(&[0, 0]));
        assert!(roll_wrote_a_part(&[0, 1]));
        assert!(roll_wrote_a_part(&[2, 0]));
    }

    #[test]
    fn a_pass_rolls_for_rows_or_for_an_unacked_commit_and_the_drain_ends_only_when_dry() {
        assert!(
            pass_must_roll(true, 0),
            "an unacked commit boundary with NO buffered rows must still roll — that \
             is what advances the slot past an uncaptured or empty span"
        );
        assert!(pass_must_roll(false, 1), "buffered rows must reach a part");
        assert!(pass_must_roll(true, 3));
        assert!(
            !pass_must_roll(false, 0),
            "nothing consumed and nothing buffered is not a roll; rolling here would \
             checkpoint a position the run never reached"
        );

        assert!(
            drain_is_complete(false, 0),
            "a pass that yielded nothing has drained to the bound"
        );
        assert!(
            !drain_is_complete(false, 1),
            "a pass that yielded events must be followed by another — stopping here \
             is one pass per run, which is the starvation the re-drain loop replaced"
        );
        assert!(
            drain_is_complete(true, 5),
            "the cap stops the run even mid-stream, or `max_events` is advisory"
        );
        assert!(drain_is_complete(true, 0));
    }

    /// A DELETE's decimal scale comes from the BEFORE image.
    #[test]
    fn a_deletes_decimal_scale_is_read_from_the_before_image() {
        let mut cols = vec![TypeMapping {
            column_name: "amount".into(),
            source_native_type: "numeric".into(),
            rivet_type: crate::types::RivetType::Decimal {
                precision: 18,
                scale: 0,
            },
            arrow_type: Some(DataType::Decimal128(18, 0)),
            ..int_col().remove(0)
        }];
        let del = ChangeEvent {
            op: ChangeOp::Delete,
            before: Some(vec![RivetValue::Bytes(b"12.345".to_vec())]),
            after: None,
            ..insert(0)
        };
        refine_decimal_scales(&mut cols, std::slice::from_ref(&del));
        assert_eq!(
            cols[0].arrow_type,
            Some(DataType::Decimal128(18, 3)),
            "a delete carries its values in `before`; reading `after` finds None and \
             leaves scale 0, which writes 12.345 as 12"
        );

        let mut cols2 = cols.clone();
        cols2[0].arrow_type = Some(DataType::Decimal128(18, 0));
        let ins = ChangeEvent {
            after: Some(vec![RivetValue::Bytes(b"9.87".to_vec())]),
            ..insert(0)
        };
        refine_decimal_scales(&mut cols2, std::slice::from_ref(&ins));
        assert_eq!(cols2[0].arrow_type, Some(DataType::Decimal128(18, 2)));

        let mut cols3 = cols.clone();
        cols3[0].arrow_type = Some(DataType::Decimal128(18, 0));
        let batch = [
            ChangeEvent {
                after: Some(vec![RivetValue::Bytes(b"1.5".to_vec())]),
                ..insert(0)
            },
            ChangeEvent {
                after: Some(vec![RivetValue::Bytes(b"2.0625".to_vec())]),
                ..insert(1)
            },
            ChangeEvent {
                after: Some(vec![RivetValue::Bytes(b"3.25".to_vec())]),
                ..insert(2)
            },
        ];
        refine_decimal_scales(&mut cols3, &batch);
        assert_eq!(
            cols3[0].arrow_type,
            Some(DataType::Decimal128(18, 4)),
            "a narrower scale than the batch's widest silently truncates the widest row"
        );
    }

    /// A refused cell names its column once, with the override code only when `columns:` names the column.
    #[test]
    fn a_refused_cell_names_the_column_and_picks_the_code_by_override() {
        let mut ev = insert(0);
        ev.after = Some(vec![RivetValue::Int(1), RivetValue::Bytes(b"1.5".to_vec())]);
        ev.image_names = Some(std::sync::Arc::from(vec![
            "v".to_string(),
            "amount".to_string(),
        ]));
        let mut cols = int_col();
        cols.push(TypeMapping {
            column_name: "amount".into(),
            ..int_col().remove(0)
        });
        let mut schema = None;
        let sch = ensure_schema(
            &mut schema,
            &mut cols,
            std::slice::from_ref(&ev),
            &crate::config::RowHash::default(),
        )
        .unwrap();
        let run = |overridden: &[&str]| {
            let set = overridden.iter().map(|s| s.to_string()).collect();
            let Err(err) = flush(
                std::slice::from_ref(&ev),
                &sch,
                &cols,
                crate::source::cdc::CdcEngine::Postgres,
                &crate::config::RowHash::default(),
                None,
                &set,
            ) else {
                panic!("a non-integer cell in an Int64 column is refused")
            };
            let code = err
                .downcast_ref::<crate::error::CodedError>()
                .map(|c| c.code().to_string());
            (format!("{err:#}"), code)
        };
        let (msg, code) = run(&["amount"]);
        assert_eq!(code.as_deref(), Some("RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH"));
        assert_eq!(msg.matches("'amount'").count(), 2, "column + remedy: {msg}");
        assert!(msg.contains("column 'amount' is Int64"), "{msg}");
        assert!(msg.contains("\"1.5\""), "{msg}");
        assert!(msg.contains("re-snapshot"), "{msg}");
        assert!(!msg.contains("query:"), "CDC has no query: {msg}");
        let (msg, code) = run(&["v"]);
        assert_eq!(code.as_deref(), Some("RIVET_SOURCE_CDC_CELL_UNSUPPORTED"));
        assert!(msg.contains("column 'amount' is Int64"), "{msg}");
    }

    /// The schema declares exactly the planned type.
    #[test]
    fn the_schema_declares_the_planned_type_and_refuses_an_unbuildable_one_by_column() {
        let unbuildable = DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Decimal128(10, 2),
            true,
        )));
        assert!(
            !value::is_buildable(&unbuildable),
            "the fixture must be unbuildable"
        );
        let mut cols = vec![TypeMapping {
            column_name: "amounts".into(),
            source_native_type: "numeric[]".into(),
            arrow_type: Some(unbuildable),
            ..int_col().remove(0)
        }];
        let mut schema = None;
        let err = ensure_schema(
            &mut schema,
            &mut cols,
            &[insert(0)],
            &crate::config::RowHash::default(),
        )
        .expect_err("an unbuildable planned type is refused at schema time");
        let msg = format!("{err:#}");
        assert!(msg.contains("column 'amounts'"), "{msg}");
        assert_eq!(
            err.downcast_ref::<crate::error::CodedError>()
                .map(|c| c.code()),
            Some("RIVET_INTERNAL_TYPE_BUILDER")
        );
        assert!(schema.is_none(), "no schema is cached for a refused plan");

        let mut cols2 = vec![
            int_col().remove(0),
            TypeMapping {
                column_name: "n".into(),
                source_native_type: "numeric".into(),
                ..int_col().remove(0)
            }
            .with_text(crate::types::TextForm::ServerText),
        ];
        let mut schema2 = None;
        let sch2 = ensure_schema(
            &mut schema2,
            &mut cols2,
            &[insert(0)],
            &crate::config::RowHash::default(),
        )
        .unwrap();
        assert_eq!(
            sch2.field_with_name("v").expect("declared").data_type(),
            &DataType::Int64,
            "a buildable type keeps its own width"
        );
        let n = sch2.field_with_name("n").expect("declared");
        assert_eq!(n.data_type(), &DataType::Utf8);
        assert_eq!(
            n.metadata().get("rivet.text_form").map(String::as_str),
            Some("server_text")
        );
    }

    /// Both arity guards, every arm — four mutants that survived while these were `if` conditions inside `flush`.
    #[test]
    fn a_partial_named_image_is_refused() {
        use std::sync::Arc;
        let names: Arc<[String]> =
            Arc::from(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let named = |vals: Vec<i64>| ChangeEvent {
            after: Some(vals.into_iter().map(RivetValue::Int).collect()),
            image_names: Some(names.clone()),
            ..insert(0)
        };

        assert_eq!(
            named_image_arity_mismatch(&named(vec![1, 2, 3])),
            None,
            "a complete named image is mapped by NAME and is arity-proof — refusing \
             it stops every stream rivet can actually read"
        );
        assert_eq!(
            named_image_arity_mismatch(&named(vec![1])),
            Some((1, 3)),
            "one value under three names is a MINIMAL row image; mapping it resolves \
             every name past the end of the vector and the row lands all NULL"
        );
        assert_eq!(
            named_image_arity_mismatch(&named(vec![1, 2, 3, 4])),
            Some((4, 3))
        );
        let key_only: Arc<[String]> = Arc::from(vec!["a".to_string()]);
        let del = ChangeEvent {
            op: ChangeOp::Delete,
            before: Some(vec![RivetValue::Int(1)]),
            after: None,
            image_names: Some(key_only),
            ..insert(0)
        };
        assert_eq!(
            named_image_arity_mismatch(&del),
            None,
            "PostgreSQL names only the key columns it also supplies — refusing this \
             would refuse every PG delete"
        );
        assert_eq!(
            named_image_arity_mismatch(&ChangeEvent {
                after: Some(vec![RivetValue::Int(1)]),
                image_names: None,
                ..insert(0)
            }),
            None
        );
    }

    /// `flush` warns once per column with the count of MySQL zero dates it wrote as NULL, and stays silent when it nulled none.
    #[test]
    fn flush_warns_with_the_count_of_zero_dates_it_nulled_and_is_silent_otherwise() {
        use crate::pipeline::run::run_tail_tests::{
            captured_warnings_mentioning, install_warn_capture,
        };
        install_warn_capture();
        let names =
            |col: &str| -> std::sync::Arc<[String]> { std::sync::Arc::from(vec![col.to_string()]) };
        let run = |col: &str, cells: Vec<RivetValue>| {
            let events: Vec<ChangeEvent> = cells
                .into_iter()
                .map(|v| ChangeEvent {
                    after: Some(vec![v]),
                    image_names: Some(names(col)),
                    ..insert(0)
                })
                .collect();
            let mut cols = vec![TypeMapping {
                column_name: col.into(),
                source_native_type: "date".into(),
                rivet_type: crate::types::RivetType::String,
                arrow_type: Some(DataType::Utf8),
                ..int_col().remove(0)
            }];
            let mut schema = None;
            let sch = ensure_schema(
                &mut schema,
                &mut cols,
                &events,
                &crate::config::RowHash::default(),
            )
            .unwrap();
            flush(
                &events,
                &sch,
                &cols,
                crate::source::cdc::CdcEngine::Mysql,
                &crate::config::RowHash::default(),
                None,
                &Default::default(),
            )
            .unwrap();
            captured_warnings_mentioning(&format!("`{col}`"))
        };
        let zero = || RivetValue::Bytes(value::ZERO_DATE.to_vec());
        let real = || RivetValue::Bytes(b"2026-01-02".to_vec());
        assert_eq!(
            run(
                "zd_nulled_col",
                vec![zero(), real(), zero(), RivetValue::Null]
            ),
            vec![crate::source::mysql::zero_date_warning("zd_nulled_col", 2)]
        );
        assert_eq!(
            run("zd_clean_col", vec![real(), RivetValue::Null, real()]),
            Vec::<String>::new()
        );
    }

    /// The file sink's `flush` refuses a before image short of its names under a whole after image (the stdout loop's rule), and writes a key-only one named by `before_names` and a whole one.
    #[test]
    fn flush_refuses_a_partial_before_image_and_writes_a_named_or_whole_one() {
        use std::sync::Arc;
        let names = |v: &[&str]| -> Arc<[String]> {
            Arc::from(v.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        };
        let update = |before: Vec<i64>, before_names: Option<Arc<[String]>>| ChangeEvent {
            op: ChangeOp::Update,
            before: Some(before.into_iter().map(RivetValue::Int).collect()),
            before_names,
            after: Some(vec![RivetValue::Int(1), RivetValue::Int(2)]),
            image_names: Some(names(&["a", "b"])),
            ..insert(0)
        };
        let mut cols = int_col();
        cols[0].column_name = "a".into();
        cols.push(TypeMapping {
            column_name: "b".into(),
            ..int_col().remove(0)
        });
        let drive = |ev: ChangeEvent| {
            let mut schema = None;
            let mut cols = cols.clone();
            let sch = ensure_schema(
                &mut schema,
                &mut cols,
                std::slice::from_ref(&ev),
                &crate::config::RowHash::default(),
            )
            .unwrap();
            flush(
                std::slice::from_ref(&ev),
                &sch,
                &cols,
                crate::source::cdc::CdcEngine::Mysql,
                &crate::config::RowHash::default(),
                None,
                &Default::default(),
            )
            .map(|_| ())
        };
        let err = drive(update(vec![1], None)).expect_err("one before value under two names");
        assert_eq!(
            crate::error::code_of(&err),
            Some(crate::error::codes::SOURCE_CDC_PREREQUISITE)
        );
        assert!(
            format!("{err:#}").contains("carries 1 value(s) under 2 column name(s)"),
            "{err:#}"
        );
        assert!(
            drive(update(vec![1, 2, 3], None)).is_err(),
            "wider than its names"
        );
        assert!(
            drive(update(vec![1, 2], None)).is_ok(),
            "a whole before image is written"
        );
        assert!(
            drive(update(vec![1], Some(names(&["a"])))).is_ok(),
            "a key-only before image named by before_names is written"
        );
        assert!(
            drive(update(vec![1, 2], Some(names(&["a"])))).is_err(),
            "a before image wider than its own names is refused"
        );
    }

    fn int_col() -> Vec<TypeMapping> {
        vec![TypeMapping {
            column_name: "v".into(),
            source_native_type: "bigint".into(),
            rivet_type: crate::types::RivetType::Int64,
            arrow_type: Some(DataType::Int64),
            fidelity: crate::types::TypeFidelity::Exact,
            nullable: true,
            warnings: vec![],
            delivery: crate::types::Delivery::Native,
            refusal: None,
        }]
    }

    #[test]
    fn a_flush_past_the_partition_budget_ships_one_noted_part_per_slice() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = vec![TypeMapping {
            column_name: "d".into(),
            source_native_type: "date".into(),
            rivet_type: crate::types::RivetType::Date,
            arrow_type: Some(DataType::Date32),
            fidelity: crate::types::TypeFidelity::Exact,
            nullable: true,
            warnings: vec![],
            delivery: crate::types::Delivery::Native,
            refusal: None,
        }];
        let day = |n: u32| {
            let mut e = insert(i64::from(n));
            e.after = Some(vec![RivetValue::DateTime(
                chrono::NaiveDate::from_ymd_opt(2026, 1, n)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            )]);
            e.committed = n == 5;
            e
        };
        let mut stream = FakeStream {
            events: VecDeque::from((1..=5).map(day).collect::<Vec<_>>()),
            acked: Vec::new(),
        };
        let mut sc = cfg(dest.as_ref(), &cols, FormatType::Parquet, 100);
        sc.outputs[0].partition = Some(crate::plan::rollover::PartitionRollover {
            column: "d".into(),
            granularity: crate::config::load::Granularity::Day,
            cap: 2,
        });
        let (_, r) = run_to_files(&mut stream, sc);
        r.unwrap();

        let mut notes = Vec::new();
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "parquet") {
                use parquet::file::reader::FileReader as _;
                let reader = parquet::file::reader::SerializedFileReader::new(
                    std::fs::File::open(&path).unwrap(),
                )
                .unwrap();
                let meta = reader.metadata().file_metadata();
                let note = meta
                    .key_value_metadata()
                    .and_then(|kv| {
                        kv.iter()
                            .find(|k| k.key == crate::plan::rollover::PARTITION_BUCKETS_KEY)
                            .and_then(|k| k.value.clone())
                    })
                    .unwrap_or_default();
                notes.push((meta.num_rows(), note));
            }
        }
        notes.sort();
        assert_eq!(
            notes,
            vec![
                (1, "d|day|1".to_string()),
                (2, "d|day|2".to_string()),
                (2, "d|day|2".to_string())
            ],
            "five days under a budget of two: three parts, each named apart and noting what it holds"
        );
    }

    #[test]
    fn run_to_files_rolls_parts_and_acks_each_committed_part() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2), insert(3)]),
            acked: Vec::new(),
        };
        let manifests = {
            let (m, r) = run_to_files(
                &mut stream,
                cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
            );
            r.unwrap();
            m
        };
        let manifest = &manifests[0];

        assert_eq!(manifest.part_count, 2, "rollover=2 over 3 events ⇒ 2 parts");
        assert_eq!(manifest.row_count, 3);
        assert_eq!(
            stream.acked.len(),
            2,
            "the durable sequence acked once per committed part"
        );
        assert!(
            dir.path().join("_SUCCESS").exists(),
            "_SUCCESS marks the clean end"
        );
    }

    /// Every durable CDC part reaches the DATABASE, not only the manifest.
    #[test]
    fn two_tables_in_one_cdc_export_do_not_erase_each_others_ledger_rows() {
        let dir = tempfile::tempdir().unwrap();
        let dest_a = local_dest(&dir);
        let dir_b = tempfile::tempdir().unwrap();
        let dest_b = local_dest(&dir_b);
        let cols = int_col();
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");

        let ev = |table: &str, id: i64| {
            let mut e = insert(id);
            e.table = table.into();
            e
        };
        let mut stream = FakeStream {
            events: VecDeque::from(vec![ev("a", 1), ev("b", 2), ev("a", 3), ev("b", 4)]),
            acked: Vec::new(),
        };

        let base = cfg(dest_a.as_ref(), &cols, FormatType::Parquet, 1);
        let manifests = {
            let (m, r) = run_to_files(
                &mut stream,
                SinkConfig {
                    state: Some(&state),
                    outputs: vec![
                        TableOutput {
                            table: "a".into(),
                            columns: cols.clone(),
                            dest: dest_a.as_ref(),
                            dest_uri: String::new(),
                            row_hash: crate::config::RowHash::All(false),
                            partition: None,
                            partition_guard: None,
                            key: Vec::new(),
                            overridden: Default::default(),
                        },
                        TableOutput {
                            table: "b".into(),
                            columns: cols.clone(),
                            dest: dest_b.as_ref(),
                            dest_uri: String::new(),
                            row_hash: crate::config::RowHash::All(false),
                            partition: None,
                            partition_guard: None,
                            key: Vec::new(),
                            overridden: Default::default(),
                        },
                    ],
                    ..base
                },
            );
            r.unwrap();
            m
        };

        let manifest_parts: usize = manifests.iter().map(|m| m.parts.len()).sum();
        assert!(
            manifests.len() == 2 && manifest_parts >= 2,
            "fixture is inert — both tables must have produced parts, or there is no sibling \
             row to erase; got {} manifest(s), {manifest_parts} part(s)",
            manifests.len()
        );

        let logged = state.list_files_for_run("r").expect("read file_log");
        assert_eq!(
            logged.len(),
            manifest_parts,
            "the two manifests list {manifest_parts} part(s) between them and the ledger kept \
             {} — a table's parts were erased by its sibling's, so every sum over file_log \
             under-reports the run",
            logged.len()
        );
        for table in ["a", "b"] {
            assert!(
                logged
                    .iter()
                    .any(|r| r.file_name.starts_with(&format!("{table}/"))),
                "table '{table}' has no row in the ledger at all"
            );
        }
    }

    #[test]
    fn every_durable_cdc_part_is_recorded_in_the_state_db_not_just_the_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2), insert(3), insert(4), insert(5)]),
            acked: Vec::new(),
        };
        let manifest = {
            let (m, r) = run_to_files(
                &mut stream,
                SinkConfig {
                    state: Some(&state),
                    ..cfg(dest.as_ref(), &cols, FormatType::Parquet, 2)
                },
            );
            r.unwrap();
            m
        };

        let logged = state.list_files_for_run("r").expect("read file_log");
        let parts = &manifest[0].parts;
        assert!(
            parts.len() > 1,
            "fixture must produce SEVERAL parts, or a per-part ledger write and a single \
             write-at-the-end are indistinguishable; got {}",
            parts.len()
        );
        assert_eq!(
            logged.len(),
            parts.len(),
            "the manifest lists {} part(s) and the ledger recorded {} — every part the run made \
             durable must be IN THE DATABASE, or a crashed run leaves rows nothing in the state \
             store knows about",
            parts.len(),
            logged.len()
        );
        for p in parts {
            let row = logged
                .iter()
                .find(|r| r.file_name == ledger_part_name("t", &p.path))
                .unwrap_or_else(|| {
                    panic!("part '{}' is in the manifest but not in file_log", p.path)
                });
            assert_eq!(
                row.row_count, p.rows,
                "ledger and manifest disagree about how many rows '{}' holds",
                p.path
            );
            assert_eq!(row.export_name, "t", "the ledger row must name the export");
        }
    }

    /// A run in flight has an aggregate in the DATABASE, and a finished run has exactly ONE.
    #[test]
    fn a_cdc_run_has_exactly_one_metrics_row_and_it_exists_before_the_run_ends() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2), insert(3), insert(4), insert(5)]),
            acked: Vec::new(),
        };
        let manifest = {
            let (m, r) = run_to_files(
                &mut stream,
                SinkConfig {
                    state: Some(&state),
                    ..cfg(dest.as_ref(), &cols, FormatType::Parquet, 2)
                },
            );
            r.unwrap();
            m
        };
        assert!(
            manifest[0].parts.len() > 1,
            "fixture must roll more than once, or neither half of this test can fail"
        );

        let rows = state
            .get_metrics(Some("t"), 100)
            .expect("read export_metrics");
        assert_eq!(
            rows.len(),
            1,
            "a run must leave ONE aggregate row, not one per roll — {} rows would make every \
             sum over export_metrics count this run {} times",
            rows.len(),
            rows.len()
        );
        assert_eq!(
            rows[0].status, "running",
            "an unfinished run reads as running"
        );
        assert_eq!(
            rows[0].total_rows, 5,
            "the in-flight aggregate must carry the rows made durable so far"
        );

        state
            .record_metric_full(&crate::state::MetricRow {
                export_name: "t".into(),
                run_id: "r".into(),
                total_rows: 5,
                status: "success".into(),
                ..Default::default()
            })
            .unwrap();
        let after = state
            .get_metrics(Some("t"), 100)
            .expect("read export_metrics");
        assert_eq!(
            after.len(),
            1,
            "the terminal row must REPLACE the in-flight one; {} rows means a finished run is \
             double-counted forever",
            after.len()
        );
        assert_eq!(after[0].status, "success");
    }

    /// The manifest must describe the compression the part actually has.
    #[test]
    fn a_cdc_manifest_declares_the_compression_the_part_actually_has() {
        for (format, ext) in [(FormatType::Csv, "csv"), (FormatType::Parquet, "parquet")] {
            let dir = tempfile::tempdir().unwrap();
            let dest = local_dest(&dir);
            let cols = int_col();
            let mut stream = FakeStream {
                events: VecDeque::from(vec![insert(1), insert(2)]),
                acked: Vec::new(),
            };
            let manifest = {
                let (m, r) = run_to_files(&mut stream, cfg(dest.as_ref(), &cols, format, 10));
                r.unwrap();
                m
            };
            let declared = &manifest[0].compression;

            let part = std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok().map(|e| e.path()))
                .find(|p| p.extension().is_some_and(|e| e == ext))
                .unwrap_or_else(|| panic!("{format:?}: no .{ext} part was written"));
            let bytes = std::fs::read(&part).unwrap();

            let zstd_framed = bytes.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]);
            match format {
                FormatType::Csv => {
                    assert!(
                        !zstd_framed && bytes.starts_with(b"__op"),
                        "the CSV part is plain text, so the manifest cannot say it is compressed"
                    );
                    assert_eq!(
                        declared, "none",
                        "the CSV part on disk is uncompressed plain text and the manifest \
                         declares `{declared}` — the sidecar is describing a different file \
                         than the one beside it"
                    );
                }
                FormatType::Parquet => {
                    assert!(bytes.starts_with(b"PAR1"), "not a parquet file");
                    assert_eq!(
                        declared, "zstd",
                        "parquet parts are written with the zstd codec and the manifest must \
                         still say so — a fix for the CSV arm that flips this one has moved \
                         the defect rather than removed it"
                    );
                }
            }
        }
    }

    #[test]
    fn roast_second_run_into_same_prefix_must_not_clobber_prior_parts() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();

        let mut run1 = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2)]),
            acked: Vec::new(),
        };
        {
            let (m, r) = run_to_files(
                &mut run1,
                SinkConfig {
                    export_name: "t".into(),
                    run_id: "t_cdc_20260702T100000000".into(),
                    ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
                },
            );
            r.unwrap();
            m
        };

        let mut run2 = FakeStream {
            events: VecDeque::from(vec![insert(3)]),
            acked: Vec::new(),
        };
        {
            let (m, r) = run_to_files(
                &mut run2,
                SinkConfig {
                    export_name: "t".into(),
                    run_id: "t_cdc_20260702T100500000".into(),
                    ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
                },
            );
            r.unwrap();
            m
        };

        let mut data_rows = 0usize;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "csv") {
                data_rows += std::fs::read_to_string(&path)
                    .unwrap()
                    .lines()
                    .count()
                    .saturating_sub(1);
            }
        }
        assert_eq!(
            data_rows, 3,
            "run 2 must append its parts alongside run 1's in the same prefix — \
             a fixed part name silently overwrites already-acked changes"
        );
    }

    #[test]
    fn roast_second_run_into_same_prefix_must_not_clobber_prior_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();

        let mut run1 = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2)]),
            acked: Vec::new(),
        };
        {
            let (m, r) = run_to_files(
                &mut run1,
                SinkConfig {
                    export_name: "t".into(),
                    run_id: "t_cdc_20260702T100000000".into(),
                    ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
                },
            );
            r.unwrap();
            m
        };

        let mut run2 = FakeStream {
            events: VecDeque::from(vec![insert(3)]),
            acked: Vec::new(),
        };
        {
            let (m, r) = run_to_files(
                &mut run2,
                SinkConfig {
                    export_name: "t".into(),
                    run_id: "t_cdc_20260702T100500000".into(),
                    ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
                },
            );
            r.unwrap();
            m
        };

        let run_unique: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("manifest-") && n.ends_with(".json"))
            .collect();
        assert_eq!(
            run_unique.len(),
            2,
            "each cycle must leave its own run-unique manifest copy so a \
             cross-run consumer sums both — got {run_unique:?}"
        );
    }

    fn decimal_col(name: &str, precision: u8, scale: i8) -> TypeMapping {
        TypeMapping {
            column_name: name.into(),
            source_native_type: "decimal".into(),
            rivet_type: crate::types::RivetType::Int64,
            arrow_type: Some(DataType::Decimal128(precision, scale)),
            fidelity: crate::types::TypeFidelity::Exact,
            nullable: true,
            warnings: Vec::new(),
            delivery: crate::types::Delivery::Native,
            refusal: None,
        }
    }

    #[test]
    fn refine_decimal_scales_fills_placeholder_keeps_real_scale() {
        let event = ChangeEvent {
            op: ChangeOp::Insert,
            schema: "s".into(),
            table: "t".into(),
            before: None,
            after: Some(vec![
                RivetValue::Bytes(b"150.05".to_vec()),
                RivetValue::Bytes(b"7.5".to_vec()),
            ]),
            position: Position::new(serde_json::json!({})),
            committed: true,
            image_names: None,
            seq: 0,
            poison: None,
            row_id: None,
            before_names: None,
            before_poison: None,
        };
        let mut cols = vec![
            decimal_col("placeholder", 38, 0),
            decimal_col("real", 10, 2),
        ];
        refine_decimal_scales(&mut cols, std::slice::from_ref(&event));
        assert_eq!(cols[0].arrow_type, Some(DataType::Decimal128(38, 2)));
        assert_eq!(cols[1].arrow_type, Some(DataType::Decimal128(10, 2)));
    }

    fn local_dest(dir: &tempfile::TempDir) -> Box<dyn crate::destination::Destination> {
        Box::new(
            crate::destination::local::LocalDestination::new(&crate::config::DestinationConfig {
                destination_type: crate::config::DestinationType::Local,
                path: Some(dir.path().to_string_lossy().into_owned()),
                ..Default::default()
            })
            .unwrap(),
        )
    }

    fn cfg<'a>(
        dest: &'a dyn crate::destination::Destination,
        cols: &'a [TypeMapping],
        format: FormatType,
        rollover: usize,
    ) -> SinkConfig<'a> {
        SinkConfig {
            export_name: "t".into(),
            outputs: vec![TableOutput {
                table: "t".into(),
                columns: cols.to_vec(),
                dest,
                dest_uri: String::new(),
                row_hash: crate::config::RowHash::All(false),
                partition: None,
                partition_guard: None,
                key: Vec::new(),
                overridden: Default::default(),
            }],
            engine: crate::source::cdc::CdcEngine::Mysql,
            format,
            checkpoint: None,
            max_events: None,
            rollover,
            rollover_memory_bytes: None,
            started_at: "2026-06-23T00:00:00Z".into(),
            run_id: "r".into(),
            state: None,
            read_bytes: Default::default(),
            reschema: None,
        }
    }

    /// A destination that delegates to the real local one but FAILS exactly the terminal writes (`manifest*` / `_SUCCESS`).
    struct ManifestFailingDest {
        inner: Box<dyn crate::destination::Destination>,
        marker_through: std::sync::atomic::AtomicBool,
    }
    impl crate::destination::Destination for ManifestFailingDest {
        fn write(
            &self,
            local: &std::path::Path,
            key: &str,
        ) -> Result<crate::destination::WriteOutcome> {
            let name = key.rsplit('/').next().unwrap_or(key);
            let marker = name.starts_with("manifest-")
                && !self
                    .marker_through
                    .swap(true, std::sync::atomic::Ordering::SeqCst);
            if !marker && (name.starts_with("manifest") || name == "_SUCCESS") {
                anyhow::bail!("injected: terminal write of '{key}' refused");
            }
            self.inner.write(local, key)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.inner.capabilities()
        }
    }

    /// Records every object key written through it, delegating to a real destination.
    struct RecordingDest {
        inner: Box<dyn crate::destination::Destination>,
        keys: std::sync::Mutex<Vec<String>>,
    }
    impl crate::destination::Destination for RecordingDest {
        fn write(
            &self,
            local: &std::path::Path,
            key: &str,
        ) -> Result<crate::destination::WriteOutcome> {
            self.keys.lock().unwrap().push(key.to_string());
            self.inner.write(local, key)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.inner.capabilities()
        }
    }

    /// Reads the run-unique manifests under `dir` at every part write; part writes after the first `ok_parts` fail.
    struct AtPartWriteDest {
        inner: Box<dyn crate::destination::Destination>,
        dir: std::path::PathBuf,
        ok_parts: usize,
        parts_seen: std::sync::atomic::AtomicUsize,
        seen: std::sync::Mutex<Vec<Vec<RunManifest>>>,
        removed: std::sync::Mutex<Vec<String>>,
    }
    impl AtPartWriteDest {
        fn new(dir: &tempfile::TempDir, ok_parts: usize) -> Self {
            Self {
                inner: local_dest(dir),
                dir: dir.path().to_path_buf(),
                ok_parts,
                parts_seen: Default::default(),
                seen: Default::default(),
                removed: Default::default(),
            }
        }
        fn statuses_seen(&self) -> Vec<Vec<ManifestStatus>> {
            let seen = self.seen.lock().unwrap();
            seen.iter()
                .map(|at| at.iter().map(|m| m.status).collect())
                .collect()
        }
    }
    impl crate::destination::Destination for AtPartWriteDest {
        fn write(
            &self,
            local: &std::path::Path,
            key: &str,
        ) -> Result<crate::destination::WriteOutcome> {
            if key.ends_with(".parquet") {
                self.seen
                    .lock()
                    .unwrap()
                    .push(run_unique_manifests(&self.dir));
                let n = self
                    .parts_seen
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n >= self.ok_parts {
                    anyhow::bail!("injected: part write of '{key}' refused");
                }
            }
            self.inner.write(local, key)
        }
        fn remove(&self, key: &str) -> Result<()> {
            self.removed.lock().unwrap().push(key.to_string());
            self.inner.remove(key)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.inner.capabilities()
        }
    }

    fn run_unique_manifests(dir: &std::path::Path) -> Vec<RunManifest> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                crate::manifest::is_run_unique_manifest_name(&e.file_name().to_string_lossy())
            })
            .map(|e| {
                serde_json::from_slice::<RunManifest>(&std::fs::read(e.path()).unwrap()).unwrap()
            })
            .collect()
    }

    fn run_unique_statuses(dir: &std::path::Path) -> Vec<ManifestStatus> {
        run_unique_manifests(dir).iter().map(|m| m.status).collect()
    }

    /// A CDC table's `running` marker is at the destination, unfinished, before its first part is written.
    #[test]
    fn a_cdc_table_marks_itself_running_before_its_first_part_lands() {
        let dir = tempfile::tempdir().unwrap();
        let dest = AtPartWriteDest::new(&dir, usize::MAX);
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2), insert(3)]),
            acked: Vec::new(),
        };
        let (_, r) = run_to_files(&mut stream, cfg(&dest, &cols, FormatType::Parquet, 2));
        r.unwrap();
        assert_eq!(
            dest.statuses_seen(),
            vec![vec![ManifestStatus::Running], vec![ManifestStatus::Success]],
            "the first part lands beside a running marker, the second beside the roll's Success manifest"
        );
        let first = dest.seen.lock().unwrap()[0][0].clone();
        assert_eq!(first.finished_at, "", "a running marker has not finished");
        assert_eq!(first.parts.len(), 0, "a running marker declares no part");
        assert_eq!(
            run_unique_statuses(dir.path()),
            vec![ManifestStatus::Success]
        );
        assert!(dest.removed.lock().unwrap().is_empty());
    }

    /// A run that fails before any part reached the store leaves no manifest.
    #[test]
    fn a_cdc_run_that_fails_before_its_first_part_removes_its_running_marker() {
        let dir = tempfile::tempdir().unwrap();
        let dest = AtPartWriteDest::new(&dir, 0);
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2)]),
            acked: Vec::new(),
        };
        let (m, r) = run_to_files(&mut stream, cfg(&dest, &cols, FormatType::Parquet, 2));
        assert!(r.is_err(), "the part write was refused");
        assert_eq!(
            dest.statuses_seen(),
            vec![vec![ManifestStatus::Running]],
            "the marker was there when the part write failed"
        );
        assert_eq!(m[0].parts.len(), 0);
        assert_eq!(
            run_unique_statuses(dir.path()),
            Vec::<ManifestStatus>::new()
        );
        assert_eq!(dest.removed.lock().unwrap().len(), 1);
        assert!(!dir.path().join("_SUCCESS").exists());
    }

    /// A later roll's failure keeps the earlier roll's `Success` manifest.
    #[test]
    fn a_cdc_run_that_fails_after_a_manifested_roll_keeps_that_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let dest = AtPartWriteDest::new(&dir, 1);
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2)]),
            acked: Vec::new(),
        };
        let (m, r) = run_to_files(&mut stream, cfg(&dest, &cols, FormatType::Parquet, 1));
        assert!(r.is_err(), "the second part write was refused");
        assert_eq!(m[0].parts.len(), 1, "fixture is inert — one part landed");
        let kept = run_unique_manifests(dir.path());
        assert_eq!(
            kept.iter().map(|k| k.status).collect::<Vec<_>>(),
            vec![ManifestStatus::Success]
        );
        assert_eq!(kept[0].parts.len(), 1);
        assert!(dest.removed.lock().unwrap().is_empty());
    }

    /// A roll rewrites the run-unique manifest only for a table that gained a part since the last one.
    #[test]
    fn a_roll_rewrites_the_manifest_only_for_tables_that_gained_a_part() {
        let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let busy = RecordingDest {
            inner: local_dest(&da),
            keys: Default::default(),
        };
        let idle = RecordingDest {
            inner: local_dest(&db),
            keys: Default::default(),
        };
        let cols = int_col();
        let ev = |table: &str, id: i64| {
            let mut e = insert(id);
            e.table = table.into();
            e.committed = true;
            e
        };
        let mut stream = FakeStream {
            events: VecDeque::from(vec![ev("b", 4), ev("a", 1), ev("a", 2), ev("a", 3)]),
            acked: Vec::new(),
        };
        fn output<'a>(
            table: &str,
            cols: &[TypeMapping],
            dest: &'a dyn crate::destination::Destination,
        ) -> TableOutput<'a> {
            TableOutput {
                table: table.into(),
                columns: cols.to_vec(),
                dest,
                dest_uri: String::new(),
                row_hash: crate::config::RowHash::All(false),
                partition: None,
                partition_guard: None,
                key: Vec::new(),
                overridden: Default::default(),
            }
        }
        let base = cfg(&busy, &cols, FormatType::Parquet, 1);
        let (m, r) = run_to_files(
            &mut stream,
            SinkConfig {
                outputs: vec![output("a", &cols, &busy), output("b", &cols, &idle)],
                ..base
            },
        );
        r.unwrap();
        assert_eq!(
            (m[0].parts.len(), m[1].parts.len()),
            (3, 1),
            "fixture is inert — table a must roll three parts and b one"
        );
        let run_unique = |d: &RecordingDest| {
            d.keys
                .lock()
                .unwrap()
                .iter()
                .filter(|k| k.rsplit('/').next().unwrap().starts_with("manifest-"))
                .count()
        };
        assert_eq!(
            run_unique(&idle),
            3,
            "b gained its part on the first roll and stayed idle for the next three: its running \
             marker, one per-roll manifest and the terminal one, not one per roll of the whole stream"
        );
        assert_eq!(
            run_unique(&busy),
            5,
            "a wrote its running marker, gained a part on every roll, then the terminal"
        );
    }

    /// Lets a data part through only once `want` parts are uploading at once, across every table sharing it.
    struct RendezvousDest {
        inner: Box<dyn crate::destination::Destination>,
        in_flight: std::sync::Arc<(std::sync::Mutex<usize>, std::sync::Condvar)>,
        want: usize,
    }
    impl crate::destination::Destination for RendezvousDest {
        fn write(
            &self,
            local: &std::path::Path,
            key: &str,
        ) -> Result<crate::destination::WriteOutcome> {
            if key.ends_with(".parquet") {
                let (count, arrived) = &*self.in_flight;
                let mut n = count.lock().unwrap();
                *n += 1;
                arrived.notify_all();
                let (n, timeout) = arrived
                    .wait_timeout_while(n, std::time::Duration::from_secs(10), |n| *n < self.want)
                    .unwrap();
                anyhow::ensure!(
                    !timeout.timed_out(),
                    "only {} part upload(s) in flight at once",
                    *n
                );
            }
            self.inner.write(local, key)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.inner.capabilities()
        }
    }

    /// A part that landed, announced to whoever waits on it.
    type Landed = std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>;

    /// Fails every data-part upload — after the sibling's part has landed, so the race is not the subject.
    struct PartFailingDest {
        inner: Box<dyn crate::destination::Destination>,
        after: Landed,
    }
    impl crate::destination::Destination for PartFailingDest {
        fn write(
            &self,
            local: &std::path::Path,
            key: &str,
        ) -> Result<crate::destination::WriteOutcome> {
            if key.ends_with(".parquet") {
                let (flag, cv) = &*self.after;
                let _ = cv
                    .wait_timeout_while(
                        flag.lock().unwrap(),
                        std::time::Duration::from_secs(10),
                        |landed| !*landed,
                    )
                    .unwrap();
                anyhow::bail!("injected: part upload of '{key}' refused");
            }
            self.inner.write(local, key)
        }
        fn remove(&self, key: &str) -> Result<()> {
            self.inner.remove(key)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.inner.capabilities()
        }
    }

    /// Delegates, and announces once a data part has landed.
    struct AnnouncingDest {
        inner: Box<dyn crate::destination::Destination>,
        landed: Landed,
    }
    impl crate::destination::Destination for AnnouncingDest {
        fn write(
            &self,
            local: &std::path::Path,
            key: &str,
        ) -> Result<crate::destination::WriteOutcome> {
            let out = self.inner.write(local, key)?;
            if key.ends_with(".parquet") {
                *self.landed.0.lock().unwrap() = true;
                self.landed.1.notify_all();
            }
            Ok(out)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.inner.capabilities()
        }
    }

    fn two_table_outputs<'a>(
        cols: &[TypeMapping],
        a: &'a dyn crate::destination::Destination,
        b: &'a dyn crate::destination::Destination,
    ) -> Vec<TableOutput<'a>> {
        [("a", a), ("b", b)]
            .into_iter()
            .map(|(table, dest)| TableOutput {
                table: table.into(),
                columns: cols.to_vec(),
                dest,
                dest_uri: String::new(),
                row_hash: crate::config::RowHash::All(false),
                partition: None,
                partition_guard: None,
                key: Vec::new(),
                overridden: Default::default(),
            })
            .collect()
    }

    /// One transaction touching two tables: a roll uploads both tables' parts at once.
    fn one_commit_over_two_tables() -> FakeStream {
        let mut a = insert(1);
        a.table = "a".into();
        let mut b = insert(2);
        b.table = "b".into();
        b.committed = true;
        FakeStream {
            events: VecDeque::from(vec![a, b]),
            acked: Vec::new(),
        }
    }

    /// The tables of one roll upload their parts concurrently, not one PUT after another.
    #[test]
    fn a_roll_uploads_every_tables_part_at_once() {
        let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let in_flight = std::sync::Arc::new((std::sync::Mutex::new(0), std::sync::Condvar::new()));
        let (a, b) = (
            RendezvousDest {
                inner: local_dest(&da),
                in_flight: in_flight.clone(),
                want: 2,
            },
            RendezvousDest {
                inner: local_dest(&db),
                in_flight,
                want: 2,
            },
        );
        let cols = int_col();
        let mut stream = one_commit_over_two_tables();
        let (m, r) = run_to_files(
            &mut stream,
            SinkConfig {
                outputs: two_table_outputs(&cols, &a, &b),
                ..cfg(&a, &cols, FormatType::Parquet, 100)
            },
        );
        r.expect("both parts must be in flight together");
        assert_eq!((m[0].parts.len(), m[1].parts.len()), (1, 1));
        assert_eq!(
            stream.acked.len(),
            1,
            "the commit is acked once both parts landed"
        );
    }

    /// A sibling's failed upload fails the roll without an ack.
    #[test]
    fn a_failed_part_upload_still_records_the_siblings_durable_part() {
        let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let landed: Landed = Default::default();
        let failing = PartFailingDest {
            inner: local_dest(&da),
            after: landed.clone(),
        };
        let fine = AnnouncingDest {
            inner: local_dest(&db),
            landed,
        };
        let cols = int_col();
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let mut stream = one_commit_over_two_tables();
        let (m, r) = run_to_files(
            &mut stream,
            SinkConfig {
                state: Some(&state),
                outputs: two_table_outputs(&cols, &failing, &fine),
                ..cfg(&fine, &cols, FormatType::Parquet, 100)
            },
        );
        assert!(r.is_err(), "a failed part upload is the roll's outcome");
        assert!(
            stream.acked.is_empty(),
            "nothing may be acked past a part that never landed"
        );
        let landed: Vec<_> = std::fs::read_dir(db.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
            .collect();
        assert_eq!(
            landed.len(),
            1,
            "fixture is inert — b's part must have reached the store"
        );
        assert_eq!(
            m[1].parts.len(),
            1,
            "b's durable part is missing from its manifest"
        );
        let logged = state.list_files_for_run("r").expect("read file_log");
        assert!(
            logged.iter().any(|f| f.file_name.starts_with("b/")),
            "b's durable part never reached the ledger: {logged:?}"
        );
        assert_eq!(
            run_unique_statuses(da.path()),
            Vec::<ManifestStatus>::new(),
            "a's marker declared nothing and is removed"
        );
        let closed = run_unique_manifests(db.path());
        assert_eq!(
            closed.iter().map(|m| m.status).collect::<Vec<_>>(),
            vec![ManifestStatus::Failed],
            "b's marker is rewritten Failed over the part that reached the store"
        );
        assert_eq!(closed[0].parts.len(), 1);
    }

    /// Fails every data-part upload after the first `ok` of them.
    struct NthPartFailingDest {
        inner: Box<dyn crate::destination::Destination>,
        ok: usize,
        seen: std::sync::atomic::AtomicUsize,
    }
    impl crate::destination::Destination for NthPartFailingDest {
        fn write(
            &self,
            local: &std::path::Path,
            key: &str,
        ) -> Result<crate::destination::WriteOutcome> {
            if key.ends_with(".parquet")
                && self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= self.ok
            {
                return Err(anyhow::anyhow!("injected: part upload of '{key}' refused"));
            }
            self.inner.write(local, key)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.inner.capabilities()
        }
    }

    /// One date column, one transaction spanning `days` days.
    fn one_commit_over_days(days: u32) -> (Vec<TypeMapping>, FakeStream) {
        let cols = vec![TypeMapping {
            column_name: "d".into(),
            source_native_type: "date".into(),
            rivet_type: crate::types::RivetType::Date,
            arrow_type: Some(DataType::Date32),
            fidelity: crate::types::TypeFidelity::Exact,
            nullable: true,
            warnings: vec![],
            delivery: crate::types::Delivery::Native,
            refusal: None,
        }];
        let day = |n: u32| {
            let mut e = insert(i64::from(n));
            e.after = Some(vec![RivetValue::DateTime(
                chrono::NaiveDate::from_ymd_opt(2026, 1, n)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            )]);
            e.committed = n == days;
            e
        };
        let stream = FakeStream {
            events: VecDeque::from((1..=days).map(day).collect::<Vec<_>>()),
            acked: Vec::new(),
        };
        (cols, stream)
    }

    /// The `d` column checksum of every parquet part under `dir`, re-read from disk.
    fn on_disk_d_checksums(dir: &std::path::Path) -> Vec<u64> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "parquet") {
                let reader =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                        std::fs::File::open(&path).unwrap(),
                    )
                    .unwrap()
                    .build()
                    .unwrap();
                let mut sum = 0u64;
                for b in reader {
                    let b = b.unwrap();
                    let d = b.column(b.schema().index_of("d").unwrap());
                    sum =
                        sum.wrapping_add(crate::source::value_checksum::array_checksum(d.as_ref()));
                }
                out.push(sum);
            }
        }
        out
    }

    /// The manifest's recorded checksum for column `d`.
    fn manifest_d_checksum(m: &RunManifest) -> u64 {
        m.column_checksums
            .as_ref()
            .and_then(|c| c.iter().find(|c| c.name == "d"))
            .map(|c| c.checksum.parse().unwrap())
            .expect("manifest records a checksum for d")
    }

    /// Budget the sink's one output at one day per part.
    fn one_day_per_part(sc: &mut SinkConfig<'_>) {
        sc.outputs[0].partition = Some(crate::plan::rollover::PartitionRollover {
            column: "d".into(),
            granularity: crate::config::load::Granularity::Day,
            cap: 1,
        });
    }

    /// A table whose flush is cut into slices records the slice that landed before a later slice's upload failed.
    #[test]
    fn a_failed_slice_upload_still_records_the_tables_earlier_durable_slice() {
        let dir = tempfile::tempdir().unwrap();
        let dest = NthPartFailingDest {
            inner: local_dest(&dir),
            ok: 1,
            seen: Default::default(),
        };
        let (cols, mut stream) = one_commit_over_days(2);
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let mut sc = SinkConfig {
            state: Some(&state),
            ..cfg(&dest, &cols, FormatType::Parquet, 100)
        };
        one_day_per_part(&mut sc);
        let (m, r) = run_to_files(&mut stream, sc);
        assert!(r.is_err(), "the failed slice is the roll's outcome");
        assert!(
            stream.acked.is_empty(),
            "nothing is acked past a lost slice"
        );
        let disk = on_disk_d_checksums(dir.path());
        assert_eq!(
            disk.len(),
            1,
            "fixture is inert — one slice must have landed"
        );
        assert_eq!(
            m[0].parts.len(),
            1,
            "the durable slice is missing from the manifest"
        );
        assert_eq!(m[0].parts[0].rows, 1);
        assert_eq!(
            manifest_d_checksum(&m[0]),
            disk[0],
            "the checksum must cover the durable slice alone, not the whole batch"
        );
        let logged = state.list_files_for_run("r").expect("read file_log");
        assert_eq!(
            logged.len(),
            1,
            "the durable slice never reached the ledger: {logged:?}"
        );
    }

    /// Per-slice checksums add up to the checksum of every part that landed.
    #[test]
    fn every_slice_of_a_flush_contributes_its_own_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let (cols, mut stream) = one_commit_over_days(3);
        let mut sc = cfg(dest.as_ref(), &cols, FormatType::Parquet, 100);
        one_day_per_part(&mut sc);
        let (m, r) = run_to_files(&mut stream, sc);
        r.unwrap();
        let disk = on_disk_d_checksums(dir.path());
        assert_eq!(disk.len(), 3, "fixture is inert — three days, three parts");
        assert_eq!(
            manifest_d_checksum(&m[0]),
            disk.iter().fold(0u64, |a, s| a.wrapping_add(*s))
        );
    }

    /// A failed TERMINAL manifest write is the run's OUTCOME — never a success with a stale manifest.
    #[test]
    fn a_failed_terminal_manifest_write_fails_the_run_and_writes_no_success() {
        let d = tempfile::tempdir().unwrap();
        let dest = ManifestFailingDest {
            inner: local_dest(&d),
            marker_through: Default::default(),
        };
        let cols = int_col();
        let mut e1 = insert(1);
        e1.committed = true;
        let mut stream = FakeStream {
            events: vec![e1].into(),
            acked: Vec::new(),
        };
        let (m, r) = run_to_files(&mut stream, cfg(&dest, &cols, FormatType::Parquet, 100));
        assert!(
            r.is_err(),
            "the terminal write failed — reporting Ok would let metrics and the \
             journal record a success no completion marker supports"
        );
        assert!(
            !m.is_empty(),
            "the manifests are still RETURNED — discarding them on a write error \
             would throw away the record of what IS durable"
        );
        assert!(
            !d.path().join("_SUCCESS").exists(),
            "no _SUCCESS may exist for a run whose terminal write failed"
        );
        let parts: Vec<_> = std::fs::read_dir(d.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
            .collect();
        assert_eq!(parts.len(), 1, "the flushed part must survive");
    }

    /// The cross-part checksum fold is SUM, recorded with the Sum render stamp.
    #[test]
    fn the_cross_part_checksum_fold_survives_two_equal_parts() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let mut e1 = insert(7);
        e1.committed = true;
        let mut e2 = insert(7);
        e2.committed = true;
        let mut stream = FakeStream {
            events: vec![e1, e2].into(),
            acked: Vec::new(),
        };
        let (m, r) = run_to_files(
            &mut stream,
            cfg(dest.as_ref(), &cols, FormatType::Parquet, 1),
        );
        r.unwrap();
        let manifest = m.into_iter().next().expect("one table");
        assert!(
            manifest.parts.len() >= 2,
            "the fixture is inert below TWO parts — every fold operator is \
             identical on one"
        );
        let sums = manifest
            .column_checksums
            .as_ref()
            .expect("the sink records Form B checksums");
        let v: u64 = sums
            .iter()
            .find(|c| c.name == "v")
            .map(|c| c.checksum.parse().expect("a numeric checksum"))
            .expect("the data column is summed");
        assert_ne!(
            v, 0,
            "two equal parts must NOT cancel — zero here is the annihilating XOR \
             fold back from the dead"
        );
        assert_eq!(
            manifest.checksum_render.as_deref(),
            Some(crate::source::value_checksum::CHECKSUM_RENDER_ID),
            "without the render stamp, validate's re-read falls back to the v1 \
             XOR fold and false-reports healthy multi-part prefixes corrupt"
        );
    }

    /// `read_bytes` counts PAYLOAD, produced by the real producer — not resident.
    #[test]
    fn read_bytes_counts_payload_not_resident() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let events: Vec<ChangeEvent> = (1..=4).map(insert).collect();
        let payload: u64 = events.iter().map(|e| e.payload_bytes() as u64).sum();
        let resident: u64 = events.iter().map(|e| e.estimated_bytes() as u64).sum();
        assert!(
            resident > payload * 2,
            "the fixture is inert unless the two units DIFFER visibly — if they \
             converge, this test can no longer tell which one the sink counted"
        );

        let mut stream = FakeStream {
            events: events.into(),
            acked: Vec::new(),
        };
        let config = cfg(dest.as_ref(), &cols, FormatType::Parquet, 100);
        let counter = std::sync::Arc::clone(&config.read_bytes);
        let (_m, r) = run_to_files(&mut stream, config);
        r.unwrap();
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            payload,
            "bytes_read must be the DECODED payload — the number comparable with \
             the batch path — never the resident bookkeeping cost"
        );
    }

    /// `max_events` must be a SOFT cap that lands on a commit boundary.
    #[test]
    fn max_events_fires_at_a_commit_boundary_on_an_uncaptured_table() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let ckpt = dir.path().join("ckpt");

        let mut events: VecDeque<ChangeEvent> = (1..=3)
            .map(|id| ChangeEvent {
                committed: false,
                ..insert(id)
            })
            .collect();
        events.push_back(ChangeEvent {
            table: "other".into(),
            ..insert(4)
        });
        events.push_back(ChangeEvent {
            committed: false,
            ..insert(5)
        });
        events.push_back(insert(6));
        let mut stream = FakeStream {
            events,
            acked: Vec::new(),
        };

        let mut c = cfg(dest.as_ref(), &cols, FormatType::Parquet, 100);
        c.max_events = Some(3);
        c.checkpoint = Some(ckpt);
        let manifests = {
            let (mm, r) = run_to_files(&mut stream, c);
            r.expect("run");
            mm
        };

        let rows: i64 = manifests.iter().map(|m| m.row_count).sum();
        assert_eq!(
            rows, 3,
            "the cap must fire at the uncaptured-table boundary after 3 routed \
             events — capturing tx2 means the cap is dead on this stream shape"
        );
    }

    #[test]
    fn max_events_stops_at_a_commit_boundary_never_inside_a_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let ckpt = dir.path().join("ckpt");

        let mut events: VecDeque<ChangeEvent> = (1..=4)
            .map(|id| ChangeEvent {
                committed: false,
                ..insert(id)
            })
            .collect();
        events.push_back(insert(5));
        let mut stream = FakeStream {
            events,
            acked: Vec::new(),
        };

        let mut c = cfg(dest.as_ref(), &cols, FormatType::Parquet, 100);
        c.max_events = Some(3);
        c.checkpoint = Some(ckpt.clone());
        let manifests = {
            let (mm, r) = run_to_files(&mut stream, c);
            r.expect("run");
            mm
        };

        let rows: i64 = manifests.iter().map(|m| m.row_count).sum();
        assert_eq!(
            rows, 5,
            "the cap must include the whole transaction, not cut it at 3"
        );
        assert_eq!(
            stream.acked.len(),
            1,
            "the commit boundary must be acked exactly once"
        );
        assert!(
            ckpt.exists(),
            "the checkpoint must be written — without it every later run re-reads \
             and re-writes this same transaction forever"
        );
    }

    #[test]
    fn a_transaction_is_never_split_across_parts() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut events: VecDeque<ChangeEvent> = (1..=4)
            .map(|id| ChangeEvent {
                committed: false,
                ..insert(id)
            })
            .collect();
        events.push_back(insert(5));
        let mut stream = FakeStream {
            events,
            acked: Vec::new(),
        };
        let manifest = &{
            let (m, r) = run_to_files(
                &mut stream,
                cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
            );
            r.unwrap();
            m
        }[0];
        assert_eq!(
            manifest.part_count, 1,
            "the 5-row transaction must not split at rollover=2"
        );
        assert_eq!(manifest.row_count, 5);
        assert_eq!(
            stream.acked.len(),
            1,
            "one ack, at the single commit boundary"
        );
    }

    #[test]
    fn delete_carries_the_before_image_not_an_empty_after() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let del = ChangeEvent {
            op: ChangeOp::Delete,
            before: Some(vec![RivetValue::Int(7)]),
            after: None,
            ..insert(0)
        };
        let mut stream = FakeStream {
            events: VecDeque::from(vec![del]),
            acked: Vec::new(),
        };
        {
            let (m, r) = run_to_files(&mut stream, cfg(dest.as_ref(), &cols, FormatType::Csv, 10));
            r.unwrap();
            m
        };
        let csv = std::fs::read_to_string(dir.path().join("cdc-r-000000.csv")).unwrap();
        assert!(csv.contains("delete"), "row marked __op=delete:\n{csv}");
        assert!(
            csv.lines().any(|l| l.contains("delete") && l.contains('7')),
            "the delete row carries the before-image key 7, not an empty after:\n{csv}"
        );
    }

    #[test]
    fn csv_output_has_a_header_and_one_row_per_change() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(10), insert(20)]),
            acked: Vec::new(),
        };
        let manifest = &{
            let (m, r) = run_to_files(&mut stream, cfg(dest.as_ref(), &cols, FormatType::Csv, 10));
            r.unwrap();
            m
        }[0];
        assert_eq!(manifest.row_count, 2);
        let csv = std::fs::read_to_string(dir.path().join("cdc-r-000000.csv")).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines.len(), 3, "header + 2 data rows:\n{csv}");
        assert!(
            lines[0].contains("__op") && lines[0].contains('v'),
            "header carries the meta + source columns: {}",
            lines[0]
        );
    }

    #[test]
    fn empty_stream_writes_a_zero_row_manifest_and_success() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::new(),
            acked: Vec::new(),
        };
        let manifest = &{
            let (m, r) = run_to_files(
                &mut stream,
                cfg(dest.as_ref(), &cols, FormatType::Parquet, 10),
            );
            r.unwrap();
            m
        }[0];
        assert_eq!(manifest.row_count, 0);
        assert_eq!(manifest.part_count, 0);
        assert!(
            dir.path().join("_SUCCESS").exists(),
            "a clean no-change run still marks _SUCCESS"
        );
    }

    #[test]
    fn table_filter_drops_changes_for_other_tables() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let ev = |id: i64, table: &str| ChangeEvent {
            table: table.into(),
            ..insert(id)
        };
        let mut stream = FakeStream {
            events: VecDeque::from(vec![ev(1, "t"), ev(2, "other"), ev(3, "t")]),
            acked: Vec::new(),
        };
        let c = cfg(dest.as_ref(), &cols, FormatType::Parquet, 10);
        let manifest = &{
            let (m, r) = run_to_files(&mut stream, c);
            r.unwrap();
            m
        }[0];
        assert_eq!(
            manifest.row_count, 2,
            "only the two 't' changes are kept; 'other' is filtered out"
        );
    }

    fn ev_for(id: i64, table: &str, committed: bool) -> ChangeEvent {
        ChangeEvent {
            table: table.into(),
            committed,
            ..insert(id)
        }
    }

    fn two_outputs<'a>(
        dest_a: &'a dyn crate::destination::Destination,
        dest_b: &'a dyn crate::destination::Destination,
        cols: &[TypeMapping],
        format: FormatType,
        rollover: usize,
    ) -> SinkConfig<'a> {
        SinkConfig {
            export_name: "t".into(),
            outputs: vec![
                TableOutput {
                    table: "a".into(),
                    columns: cols.to_vec(),
                    dest: dest_a,
                    dest_uri: "a".into(),
                    row_hash: crate::config::RowHash::All(false),
                    partition: None,
                    partition_guard: None,
                    key: Vec::new(),
                    overridden: Default::default(),
                },
                TableOutput {
                    table: "b".into(),
                    columns: cols.to_vec(),
                    dest: dest_b,
                    dest_uri: "b".into(),
                    row_hash: crate::config::RowHash::All(false),
                    partition: None,
                    partition_guard: None,
                    key: Vec::new(),
                    overridden: Default::default(),
                },
            ],
            engine: crate::source::cdc::CdcEngine::Mysql,
            format,
            checkpoint: None,
            max_events: None,
            rollover,
            rollover_memory_bytes: None,
            started_at: "2026-06-23T00:00:00Z".into(),
            run_id: "r".into(),
            state: None,
            read_bytes: Default::default(),
            reschema: None,
        }
    }

    #[test]
    fn multi_table_stream_routes_to_per_table_outputs_with_own_manifests() {
        let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (dest_a, dest_b) = (local_dest(&da), local_dest(&db));
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![
                ev_for(1, "a", true),
                ev_for(2, "b", true),
                ev_for(3, "a", true),
            ]),
            acked: Vec::new(),
        };
        let manifests = {
            let (m, r) = run_to_files(
                &mut stream,
                two_outputs(
                    dest_a.as_ref(),
                    dest_b.as_ref(),
                    &cols,
                    FormatType::Csv,
                    100,
                ),
            );
            r.unwrap();
            m
        };

        assert_eq!(manifests.len(), 2, "one manifest per table");
        assert_eq!(manifests[0].export_name, "a");
        assert_eq!(manifests[0].export_family, "t");
        assert_eq!(manifests[1].export_family, "t");
        assert_eq!(manifests[0].row_count, 2);
        assert_eq!(manifests[1].export_name, "b");
        assert_eq!(manifests[1].row_count, 1);
        assert!(da.path().join("_SUCCESS").exists());
        assert!(db.path().join("_SUCCESS").exists());
        assert_eq!(
            stream.acked.len(),
            1,
            "one final roll ⇒ one ack for the whole stream"
        );
    }

    #[test]
    fn multi_table_roll_flushes_every_table_before_the_single_ack() {
        let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (dest_a, dest_b) = (local_dest(&da), local_dest(&db));
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![
                ev_for(1, "a", false),
                ev_for(2, "b", false),
                ev_for(3, "a", true),
            ]),
            acked: Vec::new(),
        };
        let manifests = {
            let (m, r) = run_to_files(
                &mut stream,
                two_outputs(dest_a.as_ref(), dest_b.as_ref(), &cols, FormatType::Csv, 3),
            );
            r.unwrap();
            m
        };

        assert_eq!(manifests[0].row_count, 2, "table a: both rows in its part");
        assert_eq!(
            manifests[1].row_count, 1,
            "table b: flushed at the same roll"
        );
        assert_eq!(stream.acked.len(), 1, "exactly one ack for the roll");
        assert_eq!(
            stream.acked[0].0.get("lsn").and_then(|v| v.as_str()),
            Some(format!("{:08X}", 3).as_str()),
            "acked at the commit boundary"
        );
    }

    #[test]
    fn multi_table_trailing_uncommitted_tail_is_flushed_but_not_acked_past() {
        let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (dest_a, dest_b) = (local_dest(&da), local_dest(&db));
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![ev_for(1, "a", true), ev_for(2, "b", false)]),
            acked: Vec::new(),
        };
        let manifests = {
            let (m, r) = run_to_files(
                &mut stream,
                two_outputs(
                    dest_a.as_ref(),
                    dest_b.as_ref(),
                    &cols,
                    FormatType::Csv,
                    100,
                ),
            );
            r.unwrap();
            m
        };

        assert_eq!(manifests[0].row_count, 1);
        assert_eq!(manifests[1].row_count, 1, "the tail is still made durable");
        assert_eq!(stream.acked.len(), 1);
        assert_eq!(
            stream.acked[0].0.get("lsn").and_then(|v| v.as_str()),
            Some(format!("{:08X}", 1).as_str()),
            "ack stays at the last commit boundary, not the uncommitted tail"
        );
    }

    /// A destination whose every write fails — to drive the *failure* branch of the durable sequence, which the other sink tests (all happy-path) never reach.
    struct FailingDestination;
    impl crate::destination::Destination for FailingDestination {
        fn write(&self, _local: &Path, _key: &str) -> Result<crate::destination::WriteOutcome> {
            Err(anyhow::anyhow!("injected destination write failure"))
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            crate::destination::DestinationCapabilities {
                commit_protocol: crate::destination::WriteCommitProtocol::Atomic,
                idempotent_overwrite: true,
                retry_safe: true,
                partial_write_risk: false,
            }
        }
    }

    /// A change naming a column the run's schema lacks closes the part under the old schema and continues in a new one with the column re-resolved, its value kept.
    #[test]
    fn a_column_added_while_the_run_is_open_continues_in_a_new_part_with_its_values() {
        use arrow::array::{Array, StringArray};
        let with_c = |id: i64, c: &str| {
            let mut e = insert(id);
            e.after = Some(vec![
                RivetValue::Int(id),
                RivetValue::Bytes(c.as_bytes().to_vec()),
            ]);
            e.image_names = Some(std::sync::Arc::from(vec!["v".to_string(), "c".to_string()]));
            e
        };
        let mut grown = int_col();
        grown.push(TypeMapping {
            column_name: "c".into(),
            source_native_type: "varchar".into(),
            rivet_type: crate::types::RivetType::String,
            arrow_type: Some(DataType::Utf8),
            ..int_col().remove(0)
        });
        let events = || VecDeque::from(vec![insert(1), with_c(2, "two"), with_c(3, "three")]);

        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = FakeStream {
            events: events(),
            acked: Vec::new(),
        };
        let asked = std::rc::Rc::new(std::cell::Cell::new(0));
        let mut config = cfg(dest.as_ref(), &cols, FormatType::Parquet, 100);
        let (fresh, counter) = (grown.clone(), std::rc::Rc::clone(&asked));
        config.reschema = Some(Box::new(move |table: &str| {
            assert_eq!(table, "t");
            counter.set(counter.get() + 1);
            Ok(fresh.clone())
        }));
        let (manifests, r) = run_to_files(&mut stream, config);
        r.unwrap();
        assert_eq!(asked.get(), 1, "asked once, when the column first appears");
        assert_eq!((manifests[0].part_count, manifests[0].row_count), (2, 3));
        let mut files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .collect();
        files.sort();
        let schemas_and_c: Vec<(bool, Vec<String>)> = files
            .iter()
            .map(|path| {
                let b = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                    std::fs::File::open(path).unwrap(),
                )
                .unwrap()
                .build()
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
                match b.schema().index_of("c") {
                    Ok(i) => {
                        let c = b
                            .column(i)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .clone();
                        (true, (0..c.len()).map(|r| c.value(r).to_string()).collect())
                    }
                    Err(_) => (false, Vec::new()),
                }
            })
            .collect();
        assert_eq!(
            schemas_and_c,
            vec![(false, vec![]), (true, vec!["two".into(), "three".into()])]
        );

        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let mut stream = FakeStream {
            events: events(),
            acked: Vec::new(),
        };
        let (manifests, r) = run_to_files(
            &mut stream,
            cfg(dest.as_ref(), &cols, FormatType::Parquet, 100),
        );
        let err = r.expect_err("no re-resolve: a stop, not a dropped value");
        crate::error::assert_coded(&err, "RIVET_INTERNAL_SOURCE_INVARIANT", 6);
        assert!(format!("{err:#}").contains("gained column(s) c"), "{err:#}");
        assert_eq!(
            (manifests[0].row_count, stream.acked.len()),
            (0, 0),
            "nothing is acknowledged past the change the run could not place"
        );
    }

    /// A stream that fails once its events are spent: the run stopped right after what it read.
    struct StopStream(FakeStream);
    impl ChangeStream for StopStream {
        fn engine(&self) -> CdcEngine {
            CdcEngine::Postgres
        }
        fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
            self.0
                .next_change()
                .or_else(|| Some(Err(anyhow::anyhow!("stopped"))))
        }
        fn ack(&mut self, position: &Position) -> Result<()> {
            self.0.ack(position)
        }
    }

    /// An insert of `id` carrying the column `c`, which the run's schema lacks.
    fn insert_with_c(id: i64, committed: bool) -> ChangeEvent {
        let mut e = insert(id);
        e.committed = committed;
        e.after = Some(vec![RivetValue::Int(id), RivetValue::Bytes(b"x".to_vec())]);
        e.image_names = Some(std::sync::Arc::from(vec!["v".to_string(), "c".to_string()]));
        e
    }

    fn grown_columns() -> Vec<TypeMapping> {
        let mut grown = int_col();
        grown.push(TypeMapping {
            column_name: "c".into(),
            source_native_type: "varchar".into(),
            rivet_type: crate::types::RivetType::String,
            arrow_type: Some(DataType::Utf8),
            ..int_col().remove(0)
        });
        grown
    }

    /// One run over `events` that re-reads the catalog when `c` appears, then ends by `stop` (`Err` after the last event) or cleanly.
    fn reschema_run(
        events: Vec<ChangeEvent>,
        rollover: usize,
        stop: bool,
    ) -> (Vec<i64>, Vec<i64>, Option<String>, Vec<String>) {
        let (dir, ck_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (dest, cols, ck) = (local_dest(&dir), int_col(), ck_dir.path().join("ck.json"));
        let source: Vec<i64> = events
            .iter()
            .map(|e| match &e.after.as_ref().unwrap()[0] {
                RivetValue::Int(i) => *i,
                other => panic!("{other:?}"),
            })
            .collect();
        let mut config = cfg(dest.as_ref(), &cols, FormatType::Parquet, rollover);
        config.checkpoint = Some(ck.clone());
        let grown = grown_columns();
        config.reschema = Some(Box::new(move |_: &str| Ok(grown.clone())));
        let fake = FakeStream {
            events: events.into(),
            acked: Vec::new(),
        };
        let (acked, result) = if stop {
            let mut s = StopStream(fake);
            let (_, r) = run_to_files(&mut s, config);
            (s.0.acked, r)
        } else {
            let mut s = fake;
            let (_, r) = run_to_files(&mut s, config);
            (s.acked, r)
        };
        assert_eq!(result.is_err(), stop, "{result:?}");
        let lsn = |p: &Position| p.0.get("lsn").and_then(|v| v.as_str()).unwrap().to_string();
        let mut delivered: Vec<i64> = part_rows(dir.path()).into_iter().map(|r| r.1).collect();
        delivered.sort();
        (
            source,
            delivered,
            Position::load(&ck).unwrap().map(|p| lsn(&p)),
            acked.iter().map(lsn).collect(),
        )
    }

    /// The roll a new column forces closes what is buffered.
    #[test]
    fn a_roll_for_an_added_column_never_checkpoints_the_event_that_caused_it() {
        let shapes: Vec<(&str, Vec<ChangeEvent>)> = vec![
            (
                "trigger after a buffered commit",
                vec![insert(1), insert_with_c(2, true)],
            ),
            ("trigger is the first event", vec![insert_with_c(1, true)]),
            (
                "trigger after two commits",
                vec![insert(1), insert(2), insert_with_c(3, true)],
            ),
        ];
        for (shape, events) in shapes {
            for rollover in [100, 1] {
                let (source, delivered, ck, acked) = reschema_run(events.clone(), rollover, true);
                for id in source.iter().filter(|id| !delivered.contains(id)) {
                    let at = format!("{id:08X}");
                    assert!(
                        ck.as_ref().is_none_or(|c| *c < at) && acked.iter().all(|a| *a < at),
                        "{shape} (rollover {rollover}): row {id} is in no part but the checkpoint \
                         {ck:?} / acks {acked:?} are at or past it"
                    );
                }
            }
        }
    }

    /// What the column-added roll did right before stays.
    #[test]
    fn a_roll_for_an_added_column_leaves_a_clean_run_and_a_mid_transaction_trigger_as_they_were() {
        let (source, delivered, ck, acked) = reschema_run(
            vec![insert(1), insert_with_c(2, false), insert_with_c(3, true)],
            100,
            false,
        );
        assert_eq!((source, delivered), (vec![1, 2, 3], vec![1, 2, 3]));
        assert_eq!(ck.as_deref(), Some("00000003"));
        assert_eq!(acked, vec!["00000001", "00000003"]);

        let (_, delivered, ck, acked) = reschema_run(
            vec![insert(1), insert_with_c(2, true), insert_with_c(3, true)],
            100,
            false,
        );
        assert_eq!(delivered, vec![1, 2, 3]);
        assert_eq!(ck.as_deref(), Some("00000003"));
        assert_eq!(acked.last().map(String::as_str), Some("00000003"));
    }

    #[test]
    fn a_name_the_catalog_holds_is_a_new_column_and_one_it_lacks_is_gone() {
        let mut fresh = int_col();
        fresh[0].column_name = "c".into();
        let lacking = vec!["c".to_string(), "old".to_string()];
        assert_eq!(
            place_lacking(&lacking, Some(&fresh), 1, 2),
            Ok((vec!["c".to_string()], vec!["old".to_string()]))
        );
        assert_eq!(
            place_lacking(&lacking, Some(&int_col()), 1, 2),
            Ok((Vec::new(), lacking.clone())),
            "a catalog that names neither: both are gone"
        );
        assert_eq!(
            place_lacking(&lacking, None, 2, 2),
            Ok((Vec::new(), lacking.clone())),
            "no catalog, the same arity: names only, left to the positional map"
        );
        assert_eq!(place_lacking(&lacking, None, 1, 2), Err(()));
    }

    #[test]
    fn a_name_the_table_no_longer_has_is_asked_of_the_catalog_once() {
        let with_b = |id: i64| {
            let mut e = insert(id);
            e.after = Some(vec![RivetValue::Int(id), RivetValue::Int(7)]);
            e.image_names = Some(std::sync::Arc::from(vec!["v".to_string(), "b".to_string()]));
            e
        };
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![with_b(1), with_b(2), with_b(3)]),
            acked: Vec::new(),
        };
        let asked = std::rc::Rc::new(std::cell::Cell::new(0));
        let mut config = cfg(dest.as_ref(), &cols, FormatType::Parquet, 100);
        let counter = std::rc::Rc::clone(&asked);
        config.reschema = Some(Box::new(move |_: &str| {
            counter.set(counter.get() + 1);
            Ok(int_col())
        }));
        let (manifests, r) = run_to_files(&mut stream, config);
        r.unwrap();
        assert_eq!(
            asked.get(),
            1,
            "the gone name is remembered, not asked again per change"
        );
        assert_eq!((manifests[0].part_count, manifests[0].row_count), (1, 3));
        let vs: Vec<i64> = part_rows(dir.path()).into_iter().map(|r| r.1).collect();
        assert_eq!(vs, vec![1, 2, 3]);
    }

    /// `(file, v, __op, __pos, __seq)` of every row of every parquet part under `dir`, in file order.
    fn part_rows(dir: &Path) -> Vec<(String, i64, String, String, i64)> {
        use arrow::array::{Int64Array, StringArray};
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .collect();
        files.sort();
        let mut rows = Vec::new();
        for path in files {
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                std::fs::File::open(&path).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
            for b in reader {
                let b = b.unwrap();
                let col = |n: &str| b.column(b.schema().index_of(n).unwrap()).clone();
                let (v, op, pos, seq) = (col("v"), col("__op"), col("__pos"), col("__seq"));
                let v = v.as_any().downcast_ref::<Int64Array>().unwrap();
                let op = op.as_any().downcast_ref::<StringArray>().unwrap();
                let pos = pos.as_any().downcast_ref::<StringArray>().unwrap();
                let seq = seq.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..b.num_rows() {
                    rows.push((
                        path.display().to_string(),
                        v.value(i),
                        op.value(i).into(),
                        pos.value(i).into(),
                        seq.value(i),
                    ));
                }
            }
        }
        rows
    }

    /// A key move is a delete then an insert at one position, the insert's `__seq` after, in one part.
    #[test]
    fn a_change_that_moves_its_key_is_written_as_a_delete_then_an_insert_in_one_part() {
        let out = tempfile::tempdir().unwrap();
        let cols = int_col();
        let update = |b: i64, a: i64| ChangeEvent {
            op: ChangeOp::Update,
            before: Some(vec![RivetValue::Int(b)]),
            ..insert(a)
        };
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1), update(1, 9), update(9, 9)]),
            acked: Vec::new(),
        };
        let dest = local_dest(&out);
        let mut c = cfg(dest.as_ref(), &cols, FormatType::Parquet, 1);
        c.outputs[0].key = vec!["v".into()];
        run_to_files(&mut stream, c)
            .1
            .expect("a key move is delivered");

        let rows = part_rows(out.path());
        let ops: Vec<(i64, &str)> = rows.iter().map(|r| (r.1, r.2.as_str())).collect();
        assert_eq!(
            ops,
            vec![(1, "insert"), (1, "delete"), (9, "insert"), (9, "update")],
            "1 -> 9 retracts 1 and inserts 9; 9 -> 9 keeps its key and stays an update"
        );
        let (del, ins) = (&rows[1], &rows[2]);
        assert!(
            del.0 == ins.0 && del.3 == ins.3 && ins.4 > del.4,
            "one part, one position, the insert after the delete: {rows:?}"
        );
    }

    /// An UPDATE that moves its row to another partition of a base-and-buffer table is written as a delete of the old row and an insert of the new one, the insert ordered after the delete.
    #[test]
    fn a_change_that_moves_its_partition_is_written_as_a_delete_then_an_insert() {
        let out = tempfile::tempdir().unwrap();
        let cols = int_col();
        let update = |b: i64, a: i64| ChangeEvent {
            op: ChangeOp::Update,
            before: Some(vec![RivetValue::Int(b)]),
            ..insert(a)
        };
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1), update(1, 9), update(9, 15)]),
            acked: Vec::new(),
        };
        let dest = local_dest(&out);
        let mut c = cfg(dest.as_ref(), &cols, FormatType::Parquet, 10);
        c.outputs[0].partition_guard = Some(super::partition_guard::PartitionGuard {
            column: "v".into(),
            unit: super::partition_guard::GuardUnit::Range {
                start: 0,
                end: 100,
                interval: 10,
            },
        });

        let res = run_to_files(&mut stream, c);
        res.1.expect("a partition move is delivered, not refused");

        let rows: Vec<(i64, String, String, i64)> = part_rows(out.path())
            .into_iter()
            .map(|(_, v, op, pos, seq)| (v, op, pos, seq))
            .collect();
        let ops: Vec<(i64, &str)> = rows.iter().map(|(v, op, _, _)| (*v, op.as_str())).collect();
        assert_eq!(
            ops,
            vec![(1, "insert"), (9, "update"), (9, "delete"), (15, "insert")],
            "1 -> 9 stays in its partition; 9 -> 15 is the old row deleted, the new one inserted"
        );
        let (del, ins) = (&rows[2], &rows[3]);
        assert!(
            del.2 == ins.2 && ins.3 > del.3,
            "same source position, the insert's __seq after the delete's so it wins the merge: {rows:?}"
        );
        assert!(
            !stream.acked.is_empty(),
            "the move's transaction is acked like any other"
        );
    }

    #[test]
    fn flush_failure_never_checkpoints_or_acks() {
        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("cdc.ckpt");
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1)]),
            acked: Vec::new(),
        };
        let dest = FailingDestination;
        let mut c = cfg(&dest, &cols, FormatType::Parquet, 10);
        c.checkpoint = Some(ckpt.clone());

        let res = run_to_files(&mut stream, c);

        assert!(
            res.1.is_err(),
            "a destination write failure must fail the run"
        );
        assert!(
            stream.acked.is_empty(),
            "the source must NOT be acked when the part never became durable"
        );
        assert!(
            !ckpt.exists(),
            "the checkpoint must NOT be persisted when the part never became durable"
        );
    }
}
