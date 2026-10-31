//! CDC export runner — the `mode: cdc` arm of `rivet run`.

use super::cdc::{CdcCapture, run_capture};
use super::finalize::finalize_run_report;
use super::job::classify_error_message;
use super::summary::RunSummary;
use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::source::cdc::{CdcConfig, CdcEngine, CdcEngineOpts, DrainMode};
use crate::state::StateStore;

/// The byte-rollover budget one export runs under.
pub(crate) fn cdc_rollover_memory_bytes(mb: Option<usize>) -> Option<usize> {
    mb.map(|mb| mb * 1024 * 1024)
        .or(Some(DEFAULT_CDC_ROLLOVER_MEMORY_BYTES))
}

/// Default memory budget for one CDC part — the byte half of `rollover`.
pub(crate) const DEFAULT_CDC_ROLLOVER_MEMORY_BYTES: usize = 256 * 1024 * 1024;

/// Run one `mode: cdc` export end to end, then record + report it like a batch export.
fn cdc_ignored_meta_warning(export: &ExportConfig) -> Option<String> {
    export.meta_columns.exported_at.then(|| {
        format!(
            "export '{}': mode: cdc ignores meta_columns.exported_at — the CDC output carries \
             its own __op/__pos/__seq columns plus the typed after-image, and a per-run export \
             stamp would differ between the snapshot leg and the drain for the same row. \
             Remove it from this export, or use a batch mode if you need it.",
            export.name
        )
    })
}

/// The run-start warning naming batch-only knobs a CDC export set: the drain ignores them.
fn cdc_ignored_knobs_warning(export: &ExportConfig) -> Option<String> {
    let set = [
        ("quality", export.quality.is_some()),
        ("parquet", export.parquet.is_some()),
        (
            "compression",
            export.compression != crate::config::CompressionType::default(),
        ),
        ("compression_level", export.compression_level.is_some()),
        ("compression_profile", export.compression_profile.is_some()),
        ("max_file_size", export.max_file_size.is_some()),
        (
            "shape_drift_warn_factor",
            export.shape_drift_warn_factor.is_some(),
        ),
        ("skip_empty", export.skip_empty),
    ];
    let named: Vec<&str> = set.iter().filter(|(_, on)| *on).map(|(k, _)| *k).collect();
    (!named.is_empty()).then(|| {
        format!(
            "export '{}': mode: cdc ignores {} on the change stream — the drain writes its own \
             parts (fixed compression per format, rolled by `cdc.rollover`) and runs no quality checks. They apply only \
             to an `initial: snapshot` / `backfill:` baseline. Remove them, or use a batch mode \
             if the captured changes need them.",
            export.name,
            named.join(", ")
        )
    })
}

/// The run-start warning for `run --validate` / `--reconcile` on a CDC export.
pub(super) fn cdc_ignored_run_flags_warning(
    export: &str,
    validate: bool,
    reconcile: bool,
) -> Option<String> {
    let named: Vec<&str> = [("--validate", validate), ("--reconcile", reconcile)]
        .iter()
        .filter(|(_, on)| *on)
        .map(|(k, _)| *k)
        .collect();
    (!named.is_empty()).then(|| {
        format!(
            "export '{export}': {} checks only an `initial: snapshot` / `backfill:` baseline — \
             the change stream is not validated or reconciled by `rivet run`. Run `rivet \
             validate` over the destination to check the captured parts.",
            named.join(" and ")
        )
    })
}

pub(super) fn run_cdc_export(
    config_path: &str,
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    strict: bool,
    grants: Option<RunGrants>,
) -> (Result<()>, RunSummary) {
    for msg in [
        cdc_ignored_meta_warning(export),
        cdc_ignored_knobs_warning(export),
    ]
    .into_iter()
    .flatten()
    {
        log::warn!("{msg}");
    }
    let started = std::time::Instant::now();
    let run_id = super::summary::fresh_run_id(&export.name);

    let expanded_export = {
        let ctx = crate::destination::placeholder::PlaceholderContext::for_today(&export.name);
        let mut e = export.clone();
        e.destination = crate::destination::placeholder::expand_destination(e.destination, &ctx);
        e
    };
    let export = &expanded_export;

    let started_at = chrono::Utc::now().to_rfc3339();
    let run_prefix = super::finalize::destination_uri_for_manifest(&export.destination);
    if let Err(e) = state.begin_run_under_lease(
        &run_id,
        &export.name,
        &config.source.state_key(),
        &run_prefix,
        &started_at,
    ) {
        log::warn!("cdc: run-status begin failed for '{}': {e:#}", export.name);
    }

    let read_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let config_dir = std::path::Path::new(config_path)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let rss_before = crate::resource::get_rss_mb();
    let rss_sampler = crate::resource::RssPeakSampler::start(rss_before, 100);
    let mut meta = crate::source::create_source(&config.source).ok();
    let server_context = meta.as_mut().and_then(|s| s.server_context());
    let harm = super::job::HarmBracket::open_on(meta.as_deref_mut());
    let result = run_cdc_inner(
        config,
        export,
        &run_id,
        state,
        &read_bytes,
        config_dir,
        meta.as_deref_mut(),
        strict,
        grants,
    );
    let duration_ms = started.elapsed().as_millis() as i64;
    let peak_rss_mb = rss_sampler
        .stop()
        .max(crate::resource::get_rss_mb())
        .max(rss_before) as i64;

    let (manifests, outcome) = result;
    let bytes_read = read_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let bytes: u64 = manifests
        .iter()
        .flat_map(|m| &m.parts)
        .map(|p| p.size_bytes)
        .sum();
    let mut summary = cdc_summary(
        &run_id,
        export,
        if outcome.is_ok() { "success" } else { "failed" },
        manifests.iter().map(|m| m.row_count).sum(),
        manifests.iter().map(|m| m.part_count as usize).sum(),
        bytes,
        bytes_read,
        duration_ms,
        outcome.as_ref().err().map(crate::redact::redact_error),
    );
    summary.peak_rss_mb = peak_rss_mb;
    summary.server_context_json = server_context;
    let harm_deltas = harm.close_on(meta.as_deref_mut(), state, &mut summary);
    if let Some(line) = super::job::run_diagnosis(
        &summary,
        &harm_deltas,
        super::run::multi_export_concurrent(),
    ) {
        log::warn!("{line}");
    }

    if let Err(e) = state.finish_run(&run_id, &summary.status, &chrono::Utc::now().to_rfc3339()) {
        log::warn!("cdc: run-status finish failed for '{}': {e:#}", export.name);
    }

    for (i, part) in manifests.iter().flat_map(|m| &m.parts).enumerate() {
        summary
            .journal
            .record(crate::journal::RunEvent::FileWritten {
                file_name: part.path.clone(),
                rows: part.rows,
                bytes: part.size_bytes,
                part_index: i,
            });
    }
    summary
        .journal
        .record(crate::journal::RunEvent::RunCompleted {
            status: summary.status.clone(),
            error_message: summary.error_message.clone(),
            duration_ms,
        });
    if let Err(e) = state.store_journal(&summary.journal) {
        log::warn!(
            "cdc: journal persist failed for export '{}': {:#}",
            export.name,
            e
        );
    }

    record_metric(state, config, export, &summary);
    finalize_run_report(config_path, &summary, "cdc");
    crate::notify::maybe_send(config.notifications.as_ref(), &summary);
    (outcome, summary)
}

/// The anchor half of a first CDC run, run by the orchestrator BEFORE the drain.
fn capture_instance_mismatch(
    export: &str,
    t: &str,
    ci: &str,
    schema: &str,
    table: &str,
) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::CONFIG_CDC_CAPTURE_INSTANCE_MISMATCH,
        format!(
            "sqlserver cdc: export '{export}' configures `table: {t}` while capture \
             instance '{ci}' emits changes for `{schema}.{table}` — the two \
             name different relations, so the snapshot would back up one and \
             the drain capture the other. Set `table:` to `{schema}.{table}`, \
             or point `capture_instance:` at the table you meant."
        ),
    )
}

/// Claim the export's `cdc.checkpoint` for the config at `config_path` (none: nothing to claim).
pub(super) fn claim_checkpoint(config_path: &str, export: &ExportConfig) -> Result<()> {
    let Some(raw) = export.cdc.as_ref().and_then(|c| c.checkpoint.as_deref()) else {
        return Ok(());
    };
    let config = std::path::Path::new(config_path);
    let dir = config.parent().unwrap_or_else(|| std::path::Path::new("."));
    let ckpt = crate::source::cdc::resolve_checkpoint(raw, dir);
    crate::source::cdc::checkpoint_identity::claim_checkpoint(&ckpt, config, &export.name)
}

/// The checkpoint path and state keys of a file-anchored stream, or `None`.
fn file_anchor_of(
    config: &Config,
    export: &ExportConfig,
    config_path: &str,
) -> Option<(std::path::PathBuf, crate::state::ProgressKey, String)> {
    if CdcEngine::from(config.source.source_type) == CdcEngine::Postgres {
        return None;
    }
    let raw = export.cdc.as_ref()?.checkpoint.as_deref()?;
    let dir = std::path::Path::new(config_path)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let key = crate::state::ProgressKey::cdc(
        &export.name,
        &config.source.state_key(),
        &crate::config::cdc_captured_tables(export),
    );
    Some((
        crate::source::cdc::resolve_checkpoint(raw, dir),
        key,
        export.destination.state_key(),
    ))
}

/// Refuse a run whose recorded checkpoint file is gone; writes nothing.
pub(super) fn refuse_lost_file_anchor(
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_path: &str,
) -> Result<()> {
    let Some((ckpt, key, dest)) = file_anchor_of(config, export, config_path) else {
        return Ok(());
    };
    match crate::source::cdc::Position::load(&ckpt) {
        Ok(None) if state.captured_tables(&key, &dest)?.is_some() => {
            Err(crate::source::cdc::checkpoint_identity::lost_file_anchor(
                CdcEngine::from(config.source.source_type).label(),
                &ckpt,
                config_path,
                &export.name,
            ))
        }
        _ => Ok(()),
    }
}

/// Record after a successful run that the stream's checkpoint file exists.
pub(super) fn record_file_anchor(
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_path: &str,
    outcome: &Result<()>,
) {
    let Some((ckpt, key, dest)) =
        file_anchor_of(config, export, config_path).filter(|_| outcome.is_ok())
    else {
        return;
    };
    if matches!(crate::source::cdc::Position::load(&ckpt), Ok(Some(_)))
        && let Err(e) = state.record_captured_tables(&key, &dest)
    {
        log::warn!(
            "cdc: could not record the anchor of export '{}' in the state DB: {e:#}",
            export.name
        );
    }
}

/// The grants every part of a `columns_from: grants` run takes, read once before the anchor.
pub(super) type RunGrants =
    std::sync::Arc<std::collections::HashMap<String, crate::source::cdc::ReadableColumns>>;

/// Read the account's grants once for a `columns_from: grants` MySQL CDC run.
pub(super) fn resolve_grants(config: &Config, export: &ExportConfig) -> Result<Option<RunGrants>> {
    if !config.source.columns_from_grants() {
        return Ok(None);
    }
    let tables = match (&export.tables, &export.table) {
        (Some(ts), _) => ts.clone(),
        (None, Some(t)) => vec![t.clone()],
        (None, None) => return Ok(None),
    };
    log::info!("{}", crate::source::mysql::column_grants::BOUNDARY);
    let declared = tables
        .iter()
        .filter_map(|t| {
            crate::load::plan::effective_load(config, export, Some(t))
                .and_then(|l| l.pk.declared())
                .map(|k| (t.clone(), k))
        })
        .collect();
    crate::source::mysql::cdc::MysqlChangeStream::resolve_grants(
        &config.source.resolve_url()?,
        config.source.tls.as_ref(),
        &tables,
        &declared,
    )
    .map(Some)
}

/// Whether every configured table is among those the last run of the stream recorded as captured.
fn every_table_was_captured_before(configured: &[String], recorded: Option<&[String]>) -> bool {
    recorded.is_some_and(|r| !configured.is_empty() && configured.iter().all(|t| r.contains(t)))
}

/// The error of the grants read at the start of a run: a table of a stream that captured them all before and has no readable column now is refused as a drop, any other failure as it came.
pub(super) fn grants_unreadable_at_start(
    err: anyhow::Error,
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_path: &str,
) -> anyhow::Error {
    let Some((_, key, dest)) = file_anchor_of(config, export, config_path) else {
        return err;
    };
    let recorded = state.captured_tables(&key, &dest).ok().flatten();
    let configured = crate::config::cdc_captured_tables(export);
    match every_table_was_captured_before(&configured, recorded.as_deref()) {
        true => crate::source::mysql::cdc::captured_table_gone(
            err,
            "the grants read at the start of this run",
        ),
        false => err,
    }
}

pub(super) fn initial_snapshot_pending(
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_dir: &std::path::Path,
    grants: Option<&RunGrants>,
) -> Result<Vec<ExportConfig>> {
    let cdc = export.cdc.clone().unwrap_or_default();
    if cdc.captures_changes_only() {
        return Ok(Vec::new());
    }
    let url = config.source.resolve_url()?;
    let tls = config.source.tls.as_ref();

    let slot = cdc
        .slot
        .clone()
        .unwrap_or_else(|| crate::config::DEFAULT_PG_SLOT.to_string());

    let (tables, multi) = match (&export.tables, &export.table) {
        (Some(ts), _) => (ts.clone(), true),
        (None, Some(t)) => (vec![t.clone()], false),
        (None, None) => crate::rivet_bail!(
            crate::error::codes::CONFIG_MODE_SETTING_INVALID,
            "export '{}': cdc mode requires `table:`",
            export.name
        ),
    };
    let engine = CdcEngine::from(config.source.source_type);
    let catalog_read: Option<String> = match (engine, &cdc.capture_instance) {
        (CdcEngine::Mssql, Some(ci)) => {
            let Some((schema, table)) =
                crate::source::mssql::cdc::source_object_of_capture_instance(&url, ci, tls)?
            else {
                crate::rivet_bail!(
                    crate::error::codes::SOURCE_CDC_PREREQUISITE,
                    "sqlserver cdc: export '{}' names capture instance '{ci}', which \
                     `cdc.change_tables` does not know. Enable CDC on the table \
                     (`sys.sp_cdc_enable_table`) or fix the name — continuing would \
                     snapshot whatever `{}` resolves to in the connection's default \
                     schema and record that as this table's baseline.",
                    export.name,
                    tables.first().map(String::as_str).unwrap_or("the table"),
                );
            };
            for t in &tables {
                if crate::source::cdc::sink::table_matches(CdcEngine::Mssql, t, &schema, &table) {
                    continue;
                }
                return Err(capture_instance_mismatch(
                    &export.name,
                    t,
                    ci,
                    &schema,
                    &table,
                ));
            }
            Some(format!("{schema}.{table}"))
        }
        _ => None,
    };
    let mysql_reads = match engine {
        CdcEngine::Mysql => {
            crate::source::mysql::cdc::MysqlChangeStream::baseline_reads(&url, tls, &tables)?
        }
        _ => Default::default(),
    };
    let mut table_dests = Vec::with_capacity(tables.len());
    let mut done_flags = Vec::with_capacity(tables.len());
    for t in &tables {
        let table_dcfg = if multi {
            dest_for_table(&export.destination, t)
        } else {
            export.destination.clone()
        };
        let snap_dcfg = dest_for_table(&table_dcfg, "snapshot");
        let dest = crate::destination::create_destination(&snap_dcfg)?;
        let done = state.snapshot_done(&export.name, t, &snap_dcfg.state_key())?
            || dest.head("_SUCCESS")?.is_some();
        table_dests.push((
            t.clone(),
            catalog_read
                .clone()
                .or_else(|| mysql_reads.get(t).cloned())
                .unwrap_or_else(|| t.clone()),
            snap_dcfg,
        ));
        done_flags.push(done);
    }
    let capture = crate::state::ProgressKey::cdc(&export.name, &config.source.state_key(), &tables);
    let capture_dest = export.destination.state_key();
    let captured = state
        .captured_tables(&capture, &capture_dest)?
        .unwrap_or_else(|| tables.clone());
    if let Some(i) = rejoins_over_a_baseline(&captured, &tables, &done_flags) {
        let t = &tables[i];
        let snap = super::finalize::destination_uri_for_manifest(&table_dests[i].2);
        let snap = snap.trim_end_matches('/');
        let dir = snap.strip_suffix("/snapshot").unwrap_or(snap);
        crate::rivet_bail!(
            crate::error::codes::STATE_CDC_TABLE_REJOINED,
            "export '{}': table `{t}` is not among the tables this stream captured on its last \
             run ({}), but a baseline is already recorded complete where its own would go (the \
             state DB's `cdc_snapshot` row or `{snap}/_SUCCESS`) — its baseline would be \
             skipped, and no change made to `{t}` while it was out of the capture was read; \
             nothing was read or written.\n  \
             Hint: re-baseline `{t}`: move every file out of `{dir}` (the snapshot/_SUCCESS \
             marker goes with them); delete the `cdc_snapshot` row of export '{}', table `{t}` \
             from the state DB; and if a warehouse load consumes this stream, truncate that \
             table's `__changes` table before the next load. The next run reads `{t}` in full \
             and captures it from there.",
            export.name,
            captured.join(", "),
            export.name,
        );
    }

    let ckpt_path = cdc
        .checkpoint
        .as_deref()
        .map(|raw| crate::source::cdc::resolve_checkpoint(raw, config_dir));
    let ckpt_resume = match ckpt_path.as_deref() {
        Some(p) => crate::source::cdc::Position::load(p)?.is_some(),
        None => false,
    };
    let (pending_idx, prior) = snapshot_plan(&done_flags, ckpt_resume);
    if writes_before_the_drain(&pending_idx) {
        engine.refuse_unmet_prerequisites(
            &url,
            tls,
            &tables,
            DrainMode::from_until_current(cdc.until_current),
            config.source.columns_from_grants(),
        )?;
    }

    let recipes = crate::config::resolve_backfill(export, &config.exports)
        .map_err(|why| crate::error::coded(crate::error::codes::CONFIG_CDC_SHAPE_INVALID, why))?;

    engine.ensure_anchor(
        &url,
        &slot,
        cdc.capture_instance.as_deref(),
        ckpt_path.as_deref(),
        tls,
        prior,
    )?;
    state.record_captured_tables(&capture, &capture_dest)?;

    let anchor_pos = ckpt_path
        .as_deref()
        .and_then(|p| crate::source::cdc::Position::load(p).ok().flatten())
        .map(|p| p.0.to_string());
    let mut pending = Vec::new();
    for idx in pending_idx {
        let (label, read, snap_dcfg) = &table_dests[idx];
        let mut synth = synth_snapshot_export(export, label, read, snap_dcfg);
        synth.table_select = grants.and_then(|g| g.get(label)).map(|r| r.select.clone());
        if let Some(recipe) = crate::config::backfill_recipe_for(&recipes, label) {
            apply_backfill_recipe(&mut synth, recipe, export, label)?;
        }
        let base_layout =
            crate::load::plan::resolved_layout(config, export, Some(label)).compacts();
        synth.meta_columns.deleted_flag = crate::load::plan::base_carries_delete_flag(
            base_layout,
            crate::load::plan::resolved_deleted_flag(config, export, Some(label)),
        );
        synth.meta_columns.cdc_snapshot_pos = if base_layout {
            None
        } else {
            anchor_pos.clone()
        };
        pending.push(synth);
    }
    Ok(pending)
}

/// Borrow one table's READ from the export that already describes it.
fn apply_backfill_recipe(
    leg: &mut ExportConfig,
    recipe: &ExportConfig,
    cdc_export: &ExportConfig,
    label: &str,
) -> Result<()> {
    leg.mode = recipe.mode;
    leg.chunk_column = recipe.chunk_column.clone();
    leg.chunk_by_key = recipe.chunk_by_key.clone();
    leg.chunk_size = recipe.chunk_size;
    leg.chunk_size_memory_mb = recipe.chunk_size_memory_mb;
    leg.chunk_count = recipe.chunk_count;
    leg.chunk_by_days = recipe.chunk_by_days;
    leg.chunk_checkpoint = recipe.chunk_checkpoint;
    leg.chunk_max_attempts = recipe.chunk_max_attempts;
    leg.parallel = recipe.parallel;
    if recipe.tuning.is_some() {
        leg.tuning = recipe.tuning.clone();
    }
    let mut columns = crate::types::overrides_for_unit(&recipe.columns, Some(label));
    columns.extend(crate::types::overrides_for_unit(
        &cdc_export.columns,
        Some(label),
    ));
    leg.columns = columns;
    Ok(())
}

/// Synthesize the `mode: full` snapshot export for one CDC table.
#[cfg(test)]
/// Test door onto [`synth_snapshot_export`], taking BOTH strings.
pub(crate) fn synth_snapshot_export_for_test(
    export: &ExportConfig,
    table: &str,
    read: &str,
) -> ExportConfig {
    synth_snapshot_export(export, table, read, &export.destination)
}

fn synth_snapshot_export(
    export: &ExportConfig,
    table: &str,
    read: &str,
    snap_dcfg: &crate::config::DestinationConfig,
) -> ExportConfig {
    let mut synth = export.clone();
    synth.snapshot_parent = Some(export.name.clone());
    synth.snapshot_label = Some(table.to_string());
    synth.name = crate::manifest::snapshot_leg_name(&export.name, table);
    synth.mode = crate::config::ExportMode::Full;
    synth.table = Some(read.to_string());
    synth.tables = None;
    synth.cdc = None;
    synth.destination = snap_dcfg.clone();
    synth.skip_empty = false;
    synth.meta_columns.exported_at = false;
    debug_assert_eq!(synth.meta_columns.row_hash, export.meta_columns.row_hash);
    synth
}

/// Whether this run writes (a pending snapshot leg) before the drain opens.
fn writes_before_the_drain(pending_idx: &[usize]) -> bool {
    !pending_idx.is_empty()
}

/// The pure `initial: snapshot` decision.
fn snapshot_plan(
    done_flags: &[bool],
    ckpt_resume: bool,
) -> (Vec<usize>, crate::source::cdc::PriorRun) {
    let pending = done_flags
        .iter()
        .enumerate()
        .filter_map(|(i, &done)| (!done).then_some(i))
        .collect();
    let prior = crate::source::cdc::PriorRun {
        checkpoint: ckpt_resume,
        snapshot: done_flags.iter().any(|&d| d),
    };
    (pending, prior)
}

/// The first table that joins a stream whose last run did not capture it while its baseline is recorded done.
fn rejoins_over_a_baseline(captured: &[String], tables: &[String], done: &[bool]) -> Option<usize> {
    (0..tables.len()).find(|&i| done[i] && crate::state::joins_capture(captured, &tables[i]))
}

pub(crate) use crate::destination::dest_for_table;

/// Build the capture from the config and export; drive it through [`crate::source::cdc::run_capture`].
#[allow(clippy::too_many_arguments)]
/// The schema-baseline key of one captured table.
fn cdc_drift_key(export: &str, table: &str, multi: bool) -> String {
    if multi {
        format!("{export}/{table}")
    } else {
        export.to_string()
    }
}

#[allow(clippy::too_many_arguments)]
fn run_cdc_inner(
    config: &Config,
    export: &ExportConfig,
    run_id: &str,
    state: &StateStore,
    read_bytes: &std::sync::Arc<std::sync::atomic::AtomicU64>,
    config_dir: &std::path::Path,
    meta: Option<&mut (dyn crate::source::Source + 'static)>,
    strict: bool,
    grants: Option<RunGrants>,
) -> (Vec<crate::manifest::RunManifest>, Result<()>) {
    let url = match config.source.resolve_url() {
        Ok(u) => u,
        Err(e) => return (Vec::new(), Err(e)),
    };
    let cdc = export.cdc.clone().unwrap_or_default();
    let (tables, multi) = match (&export.tables, &export.table) {
        (Some(ts), _) => (ts.clone(), true),
        (None, Some(t)) => (vec![t.clone()], false),
        (None, None) => {
            return (
                Vec::new(),
                Err(crate::error::coded(
                    crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                    format!("export '{}': cdc mode requires `table:`", export.name),
                )),
            );
        }
    };
    let mut wired: Vec<(String, Box<dyn crate::destination::Destination>, String)> =
        Vec::with_capacity(tables.len());
    for t in &tables {
        let dcfg = if multi {
            dest_for_table(&export.destination, t)
        } else {
            export.destination.clone()
        };
        let dest = match crate::destination::create_destination(&dcfg) {
            Ok(d) => d,
            Err(e) => return (Vec::new(), Err(e)),
        };
        let uri = dcfg
            .path
            .clone()
            .or_else(|| dcfg.prefix.clone())
            .unwrap_or_default();
        wired.push((t.clone(), dest, uri));
    }
    if let Err(e) = crate::workers::run_each(&wired, |(_, dest, _)| {
        crate::manifest::guard_manifest_mode(dest.as_ref(), "cdc")
    })
    .into_iter()
    .collect::<Result<()>>()
    {
        return (Vec::new(), Err(e));
    }
    let columns = crate::config::effective_columns(export, &config.exports);
    let all_overrides = match crate::plan::build::parse_column_overrides_pub(&columns, &export.name)
    {
        Ok(v) => v,
        Err(e) => return (Vec::new(), Err(e)),
    };
    let outputs = wired
        .iter()
        .map(|(t, d, u)| super::cdc::CaptureOutput {
            table: t.clone(),
            dest: d.as_ref(),
            dest_uri: u.clone(),
            overrides: crate::types::overrides_for_unit(&all_overrides, Some(t)),
            row_hash: export.meta_columns.row_hash.clone(),
            partition: crate::plan::build::cdc_partition_rollover(config, export, t),
            partition_guard: crate::load::plan::resolved_layout(config, export, Some(t))
                .log_is_disposable()
                .then(|| crate::load::plan::resolved_partition(config, export, Some(t)))
                .flatten()
                .and_then(|s| crate::source::cdc::partition_guard::PartitionGuard::of(&s.form)),
            key: crate::load::plan::effective_load(config, export, Some(t))
                .and_then(|l| l.pk.declared()),
        })
        .collect();
    let now = chrono::Utc::now().to_rfc3339();

    if cdc.runs_until_stopped() {
        let shape = match config.source.source_type {
            crate::config::SourceType::Mysql => "blocks on the binlog and stays up until stopped",
            crate::config::SourceType::Mongo => {
                "blocks on the change stream awaiting events and stays up until stopped \
                 (it ends only if the stream is invalidated or closed)"
            }
            _ => {
                concat!(
                    "removes the open-time ceiling but STILL EXITS ON CATCH-UP — on ",
                    "this engine it is one unbounded pass, not a daemon; run it under ",
                    "a supervisor that restarts it, or use the scheduler model instead",
                )
            }
        };
        log::warn!(
            concat!(
                "cdc: `until_current: false` opts into the continuous model, which on {:?} {}. ",
                "For the scheduler model, omit it or set `until_current: true` (the default) ",
                "to drain to the log end and exit.",
            ),
            config.source.source_type,
            shape
        );
    }

    let schema_gate = |table: &str, cols: &[crate::types::TypeMapping]| {
        let key = cdc_drift_key(&export.name, table, multi);
        super::schema_drift::check_from_cdc_mappings(state, &key, cols, export.on_schema_drift)
    };
    run_capture(
        CdcCapture {
            export_name: export.name.clone(),
            cdc_cfg: CdcConfig {
                config_dir: config_dir.to_path_buf(),
                url,
                checkpoint: cdc
                    .checkpoint
                    .as_deref()
                    .map(|raw| crate::source::cdc::resolve_checkpoint(raw, config_dir)),
                drain: DrainMode::from_until_current(cdc.until_current),
                tls: config.source.tls.clone(),
                engine: match config.source.source_type {
                    crate::config::SourceType::Mysql => CdcEngineOpts::Mysql {
                        server_id: cdc
                            .server_id
                            .unwrap_or(crate::config::DEFAULT_MYSQL_SERVER_ID),
                        configured_tables: wired.iter().map(|(t, _, _)| t.clone()).collect(),
                        resolved_grants: grants,
                    },
                    crate::config::SourceType::Postgres => CdcEngineOpts::Postgres {
                        slot: cdc
                            .slot
                            .clone()
                            .unwrap_or_else(|| crate::config::DEFAULT_PG_SLOT.to_string()),
                        configured_tables: wired.iter().map(|(t, _, _)| t.clone()).collect(),
                    },
                    crate::config::SourceType::Mssql => CdcEngineOpts::Mssql {
                        capture_instance: cdc.capture_instance.clone(),
                        configured_tables: wired.iter().map(|(t, _, _)| t.clone()).collect(),
                    },
                    crate::config::SourceType::Oracle => CdcEngineOpts::Oracle {
                        configured_tables: wired.iter().map(|(t, _, _)| t.clone()).collect(),
                    },
                    crate::config::SourceType::Mongo => CdcEngineOpts::Mongo {
                        canonical: config.source.mongo.as_ref().is_some_and(|m| {
                            matches!(m.json, crate::config::MongoJsonMode::Canonical)
                        }),
                        configured_tables: wired.iter().map(|(t, _, _)| t.clone()).collect(),
                    },
                },
            },
            outputs,
            format: export.format,
            max_events: cdc.max_events,
            rollover: cdc.rollover.unwrap_or(100_000),
            rollover_memory_bytes: cdc_rollover_memory_bytes(cdc.rollover_memory_mb),
            run_id: run_id.to_string(),
            started_at: now,
            state: Some(state),
            schema_gate: Some(&schema_gate),
            meta,
            policy: crate::types::policy::TypePolicy::from_strict(strict),
        },
        read_bytes,
    )
}

/// Build the per-run summary (mirrors `synthetic_failed_summary`'s shape, for a CDC run).
#[allow(clippy::too_many_arguments)]
fn cdc_summary(
    run_id: &str,
    export: &ExportConfig,
    status: &str,
    total_rows: i64,
    files: usize,
    bytes: u64,
    bytes_read: u64,
    duration_ms: i64,
    error_message: Option<String>,
) -> RunSummary {
    RunSummary {
        cursor_column: None,
        cursor_low: None,
        cursor_high: None,
        run_id: run_id.to_string(),
        export_name: export.name.clone(),
        status: status.to_string(),
        total_rows,
        files_produced: files,
        bytes_written: bytes,
        bytes_read,
        files_committed: files,
        duration_ms,
        error_message,
        tuning_profile: "cdc".into(),
        format: export.format.label().to_string(),
        mode: "cdc".into(),
        destination_uri: export.destination.path.clone(),
        journal: crate::journal::RunJournal::new(run_id, &export.name),
        ..Default::default()
    }
}

/// Write the `export_metrics` row.
fn cdc_metric_row(
    summary: &RunSummary,
    source_type: Option<String>,
    destination_type: Option<String>,
) -> crate::state::MetricRow {
    crate::state::MetricRow {
        export_name: summary.export_name.clone(),
        run_id: summary.run_id.clone(),
        duration_ms: summary.duration_ms,
        total_rows: summary.total_rows,
        peak_rss_mb: Some(summary.peak_rss_mb),
        status: summary.status.clone(),
        error_message: summary.error_message.clone(),
        tuning_profile: Some("cdc".to_string()),
        format: Some(summary.format.clone()),
        mode: Some("cdc".to_string()),
        files_produced: summary.files_produced as i64,
        bytes_written: summary.bytes_written as i64,
        bytes_read: summary.bytes_read as i64,
        files_committed: summary.files_committed as i64,
        source_type,
        destination_type,
        rivet_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        longest_chunk_ms: summary.journal.longest_chunk_ms(),
        error_class: summary
            .error_message
            .as_deref()
            .and_then(classify_error_message)
            .map(str::to_string),
        server_context_json: summary.server_context_json.clone(),
        ..Default::default()
    }
}

fn record_metric(state: &StateStore, config: &Config, export: &ExportConfig, summary: &RunSummary) {
    let source_type = Some(config.source.source_type.ledger_label());
    let dest_type = Some(export.destination.destination_type.label().to_string());
    let row = cdc_metric_row(summary, source_type, dest_type);
    if let Err(e) = state.record_metric_full(&row) {
        log::warn!(
            "cdc: failed to record metric for export '{}': {:#}",
            export.name,
            e
        );
    }
}

#[cfg(test)]
mod tests {

    /// A `mode: cdc` config in `dir` with checkpoint `./cdc/o.ckpt`; its path, config and export.
    fn anchored(
        dir: &std::path::Path,
        source_type: &str,
        out: &str,
    ) -> (String, crate::config::Config, crate::config::ExportConfig) {
        let yaml = format!(
            "source:\n  type: {source_type}\n  url: \"{source_type}://u:p@localhost/app\"\nexports:\n  - name: o\n    \
             table: orders\n    mode: cdc\n    format: parquet\n    cdc:\n      checkpoint: ./cdc/o.ckpt\n    \
             destination: {{ type: local, path: {out} }}\n"
        );
        let path = dir.join("rivet.yaml");
        std::fs::write(&path, yaml).unwrap();
        let path = path.display().to_string();
        let mut config = crate::config::Config::load(&path).unwrap();
        let export = config.exports.remove(0);
        (path, config, export)
    }

    /// Write a checkpoint at `cdc/o.ckpt`.
    fn write_checkpoint(dir: &std::path::Path) {
        crate::source::cdc::Position::new(serde_json::json!({"file": "bin.000001", "pos": 4}))
            .save(&dir.join("cdc/o.ckpt"))
            .unwrap();
    }

    /// A first run anchors; the loss of a recorded checkpoint is refused on every later run.
    #[test]
    fn a_recorded_file_anchor_that_is_gone_is_refused_and_a_first_run_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = anchored(dir.path(), "mysql", "./out");
        let state = StateStore::open_in_memory().unwrap();
        super::refuse_lost_file_anchor(&config, &export, &state, &path).unwrap();
        super::record_file_anchor(&config, &export, &state, &path, &Ok(()));
        super::refuse_lost_file_anchor(&config, &export, &state, &path).unwrap();
        write_checkpoint(dir.path());
        super::record_file_anchor(&config, &export, &state, &path, &Ok(()));
        super::refuse_lost_file_anchor(&config, &export, &state, &path).unwrap();
        std::fs::remove_file(dir.path().join("cdc/o.ckpt")).unwrap();
        for _ in 0..2 {
            let err = super::refuse_lost_file_anchor(&config, &export, &state, &path).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_CHECKPOINT_INVALID", 5);
            let text = format!("{err:#}");
            assert!(
                text.contains("is missing, but the state DB records that export 'o'")
                    && text.contains(&format!("`rivet state reset -c {path} -e o`"))
                    && text.contains(crate::source::cdc::checkpoint_identity::RECOVER),
                "{text}"
            );
        }
        state.reset("o", &config.source.state_key()).unwrap();
        super::refuse_lost_file_anchor(&config, &export, &state, &path).unwrap();
    }

    /// Every configured table must be among those the last run recorded.
    #[test]
    fn a_table_is_captured_before_only_when_the_last_run_recorded_every_configured_one() {
        let t = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        assert!(super::every_table_was_captured_before(
            &t(&["a.x", "a.y"]),
            Some(&t(&["a.y", "a.x", "a.z"]))
        ));
        assert!(!super::every_table_was_captured_before(
            &t(&["a.x", "a.new"]),
            Some(&t(&["a.x"]))
        ));
        assert!(!super::every_table_was_captured_before(&t(&["a.x"]), None));
        assert!(!super::every_table_was_captured_before(
            &t(&[]),
            Some(&t(&["a.x"]))
        ));
    }

    /// A table with no readable column is a drop only for a stream that recorded it as captured; any other error is as it came.
    #[test]
    fn a_table_unreadable_at_the_start_is_a_drop_only_for_a_stream_that_captured_it() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = anchored(dir.path(), "mysql", "./out");
        let state = StateStore::open_in_memory().unwrap();
        let unreadable = || {
            crate::error::coded(
                crate::error::codes::SOURCE_COLUMNS_UNREADABLE,
                "mysql: `app.orders`: found no column".to_string(),
            )
        };
        let first =
            super::grants_unreadable_at_start(unreadable(), &config, &export, &state, &path);
        crate::error::assert_coded(&first, "RIVET_SOURCE_COLUMNS_UNREADABLE", 1);
        write_checkpoint(dir.path());
        super::record_file_anchor(&config, &export, &state, &path, &Ok(()));
        let later =
            super::grants_unreadable_at_start(unreadable(), &config, &export, &state, &path);
        crate::error::assert_coded(&later, "RIVET_SOURCE_CDC_TRUNCATED", 5);
        let text = format!("{later:#}");
        assert!(
            text.contains("the grants read at the start of this run")
                && text.contains("app.orders")
                && text.contains(crate::source::cdc::checkpoint_identity::RECOVER),
            "{text}"
        );
        let other = super::grants_unreadable_at_start(
            anyhow::anyhow!("connection lost"),
            &config,
            &export,
            &state,
            &path,
        );
        assert_eq!(crate::error::code_of(&other), None);
    }

    /// A run records its checkpoint only when the file exists.
    #[test]
    fn a_run_records_its_anchor_only_when_the_checkpoint_exists() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = anchored(dir.path(), "mongo", "./out");
        let state = StateStore::open_in_memory().unwrap();
        super::record_file_anchor(&config, &export, &state, &path, &Ok(()));
        std::fs::create_dir_all(dir.path().join("cdc")).unwrap();
        super::refuse_lost_file_anchor(&config, &export, &state, &path).unwrap();
        write_checkpoint(dir.path());
        super::record_file_anchor(&config, &export, &state, &path, &Ok(()));
        std::fs::remove_file(dir.path().join("cdc/o.ckpt")).unwrap();
        let err = super::refuse_lost_file_anchor(&config, &export, &state, &path).unwrap_err();
        assert!(
            format!("{err:#}").starts_with("mongo cdc: checkpoint"),
            "{err:#}"
        );
    }

    /// The record is keyed per destination; an unparsable checkpoint is left to the run.
    #[test]
    fn the_record_binds_to_the_destination_and_leaves_a_corrupt_file_to_the_run() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = anchored(dir.path(), "mysql", "./out");
        let state = StateStore::open_in_memory().unwrap();
        write_checkpoint(dir.path());
        super::record_file_anchor(&config, &export, &state, &path, &Ok(()));
        let mut moved = export.clone();
        moved.destination = anchored(dir.path(), "mysql", "./elsewhere").2.destination;
        std::fs::write(dir.path().join("cdc/o.ckpt"), "{not json").unwrap();
        super::refuse_lost_file_anchor(&config, &export, &state, &path).unwrap();
        std::fs::remove_file(dir.path().join("cdc/o.ckpt")).unwrap();
        super::refuse_lost_file_anchor(&config, &moved, &state, &path).unwrap();
        assert!(super::refuse_lost_file_anchor(&config, &export, &state, &path).is_err());
    }

    /// Postgres records nothing: its anchor is the slot.
    #[test]
    fn postgres_has_no_file_anchor_to_lose() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = anchored(dir.path(), "postgres", "./out");
        let state = StateStore::open_in_memory().unwrap();
        write_checkpoint(dir.path());
        super::record_file_anchor(&config, &export, &state, &path, &Ok(()));
        std::fs::remove_file(dir.path().join("cdc/o.ckpt")).unwrap();
        super::refuse_lost_file_anchor(&config, &export, &state, &path).unwrap();
    }

    /// Only a successful run, and never the check itself, records the anchor.
    #[test]
    fn a_failed_run_and_the_check_record_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = anchored(dir.path(), "mysql", "./out");
        let state = StateStore::open_in_memory().unwrap();
        write_checkpoint(dir.path());
        super::refuse_lost_file_anchor(&config, &export, &state, &path).unwrap();
        super::record_file_anchor(&config, &export, &state, &path, &Err(anyhow::anyhow!("x")));
        std::fs::remove_file(dir.path().join("cdc/o.ckpt")).unwrap();
        super::refuse_lost_file_anchor(&config, &export, &state, &path).unwrap();
    }

    /// A second config naming the same checkpoint file is refused by code.
    #[test]
    fn a_run_claims_the_checkpoint_its_export_names() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = |out: &str| {
            format!(
                "source:\n  type: mysql\n  url: \"mysql://u:p@localhost/app\"\nexports:\n  - name: o\n    \
                 table: orders\n    mode: cdc\n    format: parquet\n    cdc:\n      checkpoint: ./cdc/o.ckpt\n    \
                 destination: {{ type: local, path: {out} }}\n"
            )
        };
        for (file, out) in [("a.yaml", "./a"), ("b.yaml", "./b")] {
            std::fs::write(dir.path().join(file), yaml(out)).unwrap();
        }
        let at = |f: &str| dir.path().join(f).display().to_string();
        let export = |f: &str| {
            crate::config::Config::load(&at(f))
                .unwrap()
                .exports
                .remove(0)
        };
        super::claim_checkpoint(&at("a.yaml"), &export("a.yaml")).unwrap();
        assert!(dir.path().join("cdc/o.ckpt.owner").exists());
        let err = super::claim_checkpoint(&at("b.yaml"), &export("b.yaml")).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_CHECKPOINT_SHARED", 5);
    }

    /// `resolve_checkpoint` prefers the config dir and keeps an existing cwd file.
    #[test]
    fn resolve_checkpoint_prefers_the_config_dir_and_keeps_an_existing_cwd_file() {
        let cfg_dir = tempfile::tempdir().expect("config dir");
        let cwd_dir = tempfile::tempdir().expect("cwd");
        struct CwdGuard(std::path::PathBuf);
        impl Drop for CwdGuard {
            fn drop(&mut self) {
                let _ = std::env::set_current_dir(&self.0);
            }
        }
        let _guard = CwdGuard(std::env::current_dir().expect("cwd"));
        std::env::set_current_dir(cwd_dir.path()).expect("enter the temp cwd");

        let abs = cfg_dir.path().join("abs.ckpt");
        assert_eq!(
            crate::source::cdc::resolve_checkpoint(abs.to_str().unwrap(), cfg_dir.path()),
            abs
        );

        assert_eq!(
            crate::source::cdc::resolve_checkpoint("./cdc/t.ckpt", cfg_dir.path()),
            cfg_dir.path().join("cdc").join("t.ckpt"),
            "a `.` component must not survive into the path rivet reports"
        );

        assert_eq!(
            crate::source::cdc::resolve_checkpoint("rel.ckpt", cfg_dir.path()),
            cfg_dir.path().join("rel.ckpt"),
            "a relative path follows the config — resolving it against the process \
             working directory is what made one config look in two places"
        );

        std::fs::write(cwd_dir.path().join("legacy.ckpt"), b"{}").expect("seed legacy");
        assert_eq!(
            crate::source::cdc::resolve_checkpoint("legacy.ckpt", cfg_dir.path()),
            std::path::Path::new("legacy.ckpt"),
            "an existing CWD-relative checkpoint is kept — `&&` becoming `||`, or the \
             `!` disappearing, both break this compatibility arm"
        );

        std::fs::write(cfg_dir.path().join("legacy.ckpt"), b"{}").expect("seed config-side");
        assert_eq!(
            crate::source::cdc::resolve_checkpoint("legacy.ckpt", cfg_dir.path()),
            cfg_dir.path().join("legacy.ckpt")
        );
    }

    use super::*;
    use crate::config::{DestinationConfig, DestinationType};

    /// #196: cdc_summary must carry bytes_read into the RunSummary (the CDC read leg).
    #[test]
    fn a_cdc_run_is_recorded_in_the_metrics_ledger() {
        let d = tempfile::tempdir().unwrap();
        let cfg_path = d.path().join("rivet.yaml");
        let store = crate::state::StateStore::open(cfg_path.to_str().unwrap()).unwrap();
        let config = crate::config::Config::from_yaml(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/t\"\n\
             exports:\n  - name: a\n    table: t\n    format: parquet\n\
             \x20   destination:\n      type: local\n      path: ./out\n",
        )
        .unwrap();
        let export = crate::config::sample_export("cdc_t");
        let summary = super::cdc_summary("r_m1", &export, "success", 7, 1, 100, 10, 50, None);
        super::record_metric(&store, &config, &export, &summary);
        let got = store.get_metrics(Some("cdc_t"), 10).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].run_id.as_deref(), Some("r_m1"));
        assert_eq!(got[0].total_rows, 7);
    }

    #[test]
    fn cdc_summary_carries_bytes_read() {
        let export = crate::config::sample_export("t");
        let s = super::cdc_summary("r1", &export, "success", 100, 2, 5_000, 12_345, 50, None);
        assert_eq!(s.bytes_read, 12_345, "read leg must reach the summary");
        assert_eq!(s.bytes_written, 5_000);
    }

    /// A CDC failure must GROUP like a batch one.
    #[test]
    fn cdc_metric_row_classifies_a_failure_the_way_the_batch_path_does() {
        let export = crate::config::sample_export("t");
        let failed = super::cdc_summary(
            "r1",
            &export,
            "failed",
            0,
            0,
            0,
            0,
            50,
            Some("Deadlock found when trying to get lock".to_string()),
        );
        let row = super::cdc_metric_row(&failed, Some("mysql".into()), Some("local".into()));
        assert_eq!(
            row.error_class.as_deref(),
            Some("deadlock"),
            "a CDC failure must carry the same error_class a batch failure would"
        );

        let ok = super::cdc_summary("r2", &export, "success", 100, 2, 5_000, 12_345, 50, None);
        let ok_row = super::cdc_metric_row(&ok, Some("mysql".into()), Some("local".into()));
        assert_eq!(
            ok_row.error_class, None,
            "a run with no error message must not invent a class"
        );
    }

    /// #196: the CDC metrics row must carry bytes_read from the summary.
    #[test]
    fn cdc_metric_row_carries_bytes_read() {
        let export = crate::config::sample_export("t");
        let summary = super::cdc_summary("r1", &export, "success", 100, 2, 5_000, 98_765, 50, None);
        let row = super::cdc_metric_row(&summary, Some("mysql".into()), Some("local".into()));
        assert_eq!(
            row.bytes_read, 98_765,
            "read leg must reach the export_metrics row"
        );
        assert_eq!(row.bytes_written, 5_000);
        assert_eq!(
            row.source_type.as_deref(),
            Some("mysql"),
            "source engine must reach the export_metrics row"
        );
        assert_eq!(
            row.destination_type.as_deref(),
            Some("local"),
            "destination engine must reach the export_metrics row"
        );
    }

    #[test]
    fn cdc_metric_row_carries_the_server_context() {
        let export = crate::config::sample_export("t");
        let mut summary =
            super::cdc_summary("r1", &export, "failed", 0, 0, 0, 0, 50, Some("boom".into()));
        summary.server_context_json = Some(r#"{"version":"8.0"}"#.into());
        let row = super::cdc_metric_row(&summary, None, None);
        assert_eq!(
            row.server_context_json.as_deref(),
            Some(r#"{"version":"8.0"}"#)
        );
    }

    /// The documented default: a CDC part rolls at 256 MiB of buffered changes.
    #[test]
    fn the_default_rollover_budget_is_256_mib() {
        assert_eq!(super::cdc_rollover_memory_bytes(None), Some(268_435_456));
        assert_eq!(
            super::cdc_rollover_memory_bytes(Some(256)),
            Some(268_435_456)
        );
    }

    /// The metrics row carries the longest chunk the run journaled.
    #[test]
    fn cdc_metric_row_carries_the_longest_chunk() {
        use crate::journal::RunEvent;
        let export = crate::config::sample_export("t");
        let mut summary =
            super::cdc_summary("r1", &export, "success", 100, 2, 5_000, 12_345, 50, None);
        summary.journal.record(RunEvent::ChunkStarted {
            chunk_index: 0,
            start_key: String::new(),
            end_key: String::new(),
        });
        summary.journal.record(RunEvent::ChunkCompleted {
            chunk_index: 0,
            rows: 1,
            file_name: None,
        });
        let expected = summary.journal.longest_chunk_ms();
        assert!(expected.is_some(), "a paired start and completion is timed");
        let row = super::cdc_metric_row(&summary, None, None);
        assert_eq!(row.longest_chunk_ms, expected);
    }

    /// A capture-instance export resolves its source object before the baseline.
    #[test]
    fn a_capture_instance_is_resolved_before_the_snapshot_baseline() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "source:\n  type: mssql\n  url: \"mssql://u:p@127.0.0.1:1/app\"\nexports:\n  - name: o\n    \
             table: orders\n    mode: cdc\n    format: parquet\n    cdc:\n      checkpoint: ./cdc/o.ckpt\n      \
             initial: snapshot\n      capture_instance: dbo_orders\n    destination: { type: local, path: ./out }\n";
        let path = dir.path().join("rivet.yaml");
        std::fs::write(&path, yaml).unwrap();
        std::fs::create_dir_all(dir.path().join("cdc")).unwrap();
        std::fs::write(dir.path().join("cdc/o.ckpt"), "{}").unwrap();
        let config = crate::config::Config::load(&path.display().to_string()).unwrap();
        let export = config.exports[0].clone();
        let state = StateStore::open_in_memory().unwrap();
        let table_dest = super::dest_for_table(&export.destination, "orders");
        let snap_prefix = super::dest_for_table(&table_dest, "snapshot").state_key();
        state
            .mark_snapshot_done("o", "orders", &snap_prefix, "run-1")
            .unwrap();
        let err = super::initial_snapshot_pending(&config, &export, &state, dir.path(), None)
            .expect_err("the capture-instance lookup dials the server before the baseline");
        assert!(format!("{err:#}").contains("TCP connect failed"), "{err:#}");
    }

    #[test]
    fn cdc_warns_when_meta_columns_are_requested_on_a_cdc_export() {
        let mut e = crate::config::sample_export("orders");
        e.meta_columns = Default::default();
        assert!(
            cdc_ignored_meta_warning(&e).is_none(),
            "no warning without meta_columns"
        );
        e.meta_columns.exported_at = true;
        let msg = cdc_ignored_meta_warning(&e).expect("must warn on exported_at");
        assert!(msg.contains("orders"), "names the export: {msg}");
        assert!(
            msg.contains("meta_columns"),
            "names the ignored knob: {msg}"
        );
        assert!(
            msg.contains("__op"),
            "points at the CDC-native columns: {msg}"
        );
        e.meta_columns.exported_at = false;
        e.meta_columns.row_hash = crate::config::RowHash::All(true);
        assert!(
            cdc_ignored_meta_warning(&e).is_none(),
            "row_hash is emitted on the CDC leg now, so claiming it is ignored would be a lie"
        );
    }

    #[test]
    fn cdc_names_every_batch_only_knob_it_ignores_and_only_those() {
        let mut e = crate::config::sample_export("orders");
        e.compression = crate::config::CompressionType::default();
        assert!(
            cdc_ignored_knobs_warning(&e).is_none(),
            "defaults must not warn"
        );
        e.quality = serde_yaml_ng::from_str("row_count_min: 1").ok();
        e.compression = crate::config::CompressionType::Gzip;
        e.max_file_size = Some("256MB".into());
        let msg = cdc_ignored_knobs_warning(&e).expect("set knobs must warn");
        assert!(
            msg.contains("ignores quality, compression, max_file_size on the change stream"),
            "names exactly the set knobs, in order: {msg}"
        );
        assert!(msg.contains("'orders'"), "names the export: {msg}");
        e.skip_empty = true;
        let msg = cdc_ignored_knobs_warning(&e).expect("set knobs must warn");
        assert!(
            msg.contains("max_file_size, skip_empty on the change stream"),
            "{msg}"
        );
    }

    #[test]
    fn cdc_warns_that_run_validate_and_reconcile_skip_the_change_stream() {
        assert!(cdc_ignored_run_flags_warning("orders", false, false).is_none());
        let msg = cdc_ignored_run_flags_warning("orders", true, true).expect("must warn");
        assert!(
            msg.starts_with("export 'orders': --validate and --reconcile checks only"),
            "names both flags: {msg}"
        );
        let msg = cdc_ignored_run_flags_warning("orders", false, true).expect("must warn");
        assert!(msg.contains("'orders': --reconcile checks only"), "{msg}");
    }

    /// The split `cdc.backfill` rests on.
    #[test]
    fn backfill_leg_borrows_the_recipe_read_and_keeps_its_own_identity() {
        let dcfg = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("/tmp/cdc/orders/snapshot".into()),
            ..Default::default()
        };
        let mut stream = crate::config::sample_export("stand_cdc");
        stream.mode = crate::config::ExportMode::Cdc;
        stream.table = None;
        stream.tables = Some(vec!["orders".into()]);
        stream.columns = std::collections::HashMap::new();

        let mut recipe = crate::config::sample_export("orders");
        recipe.mode = crate::config::ExportMode::Chunked;
        recipe.table = Some("orders".into());
        recipe.tables = None;
        recipe.cdc = None;
        recipe.chunk_by_key = Some("id".into());
        recipe.chunk_column = None;
        recipe.chunk_size = 250_000;
        recipe.parallel = 4;
        recipe.chunk_checkpoint = true;
        recipe.columns =
            std::collections::HashMap::from([("price".into(), "decimal(10,2)".into())]);
        stream.tuning = Some(crate::tuning::TuningConfig::default());
        recipe.tuning = None;

        let mut leg = synth_snapshot_export(&stream, "orders", "orders", &dcfg);
        apply_backfill_recipe(&mut leg, &recipe, &stream, "orders").expect("no type conflict");

        assert_eq!(leg.mode, crate::config::ExportMode::Chunked);
        assert!(
            leg.tuning.is_some(),
            "a recipe without `tuning:` must not erase the CDC export's budget"
        );
        assert_eq!(leg.chunk_by_key.as_deref(), Some("id"));
        assert_eq!(leg.chunk_size, 250_000);
        assert_eq!(leg.parallel, 4);
        assert!(leg.chunk_checkpoint, "the baseline must be resumable");
        assert_eq!(
            leg.columns.get("price").map(String::as_str),
            Some("decimal(10,2)")
        );

        let mut range = recipe.clone();
        range.chunk_by_key = None;
        range.chunk_column = Some("ref_id".into());
        range.chunk_count = Some(7);
        range.chunk_by_days = Some(3);
        range.chunk_size_memory_mb = Some(64);
        range.chunk_max_attempts = Some(9);
        let mut range_leg = synth_snapshot_export(&stream, "orders", "orders", &dcfg);
        apply_backfill_recipe(&mut range_leg, &range, &stream, "orders").expect("no conflict");
        assert_eq!(range_leg.chunk_by_key, None);
        assert_eq!(range_leg.chunk_column.as_deref(), Some("ref_id"));
        assert_eq!(range_leg.chunk_count, Some(7));
        assert_eq!(range_leg.chunk_by_days, Some(3));
        assert_eq!(range_leg.chunk_size_memory_mb, Some(64));
        assert_eq!(range_leg.chunk_max_attempts, Some(9));

        assert_eq!(leg.snapshot_parent.as_deref(), Some("stand_cdc"));
        assert_eq!(leg.snapshot_label.as_deref(), Some("orders"));
        assert!(
            leg.name.contains(crate::manifest::SNAPSHOT_LEG_INFIX),
            "{}",
            leg.name
        );
        assert_eq!(
            leg.destination.path.as_deref(),
            Some("/tmp/cdc/orders/snapshot")
        );
        assert!(leg.cdc.is_none(), "the leg is a batch export, not a stream");
        assert!(
            !leg.skip_empty,
            "an empty table must still publish its marker"
        );

        let mut conflicting = stream.clone();
        conflicting.columns =
            std::collections::HashMap::from([("orders.price".into(), "decimal(12,4)".into())]);
        let err = format!(
            "{:#}",
            crate::config::refuse_backfill_type_conflict(&conflicting, "orders", &recipe)
                .expect_err("the baseline and the stream disagree about `price`")
        );
        assert!(err.contains("price") && err.contains("two types"), "{err}");

        let mut agreeing = stream.clone();
        agreeing.columns =
            std::collections::HashMap::from([("orders.price".into(), "decimal(10,2)".into())]);
        crate::config::refuse_backfill_type_conflict(&agreeing, "orders", &recipe)
            .expect("agreement is not a conflict");
    }

    /// SQL Server: the stream narrows `columns:` by the configured LABEL.
    #[test]
    fn backfill_leg_keeps_a_label_cased_column_type_when_the_catalog_read_differs() {
        let dcfg = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("/tmp/cdc/Orders/snapshot".into()),
            ..Default::default()
        };
        let mut stream = crate::config::sample_export("stand_cdc");
        stream.mode = crate::config::ExportMode::Cdc;
        stream.table = None;
        stream.tables = Some(vec!["Orders".into()]);
        stream.columns =
            std::collections::HashMap::from([("Orders.price".into(), "decimal(10,2)".into())]);
        let mut recipe = crate::config::sample_export("Orders");
        recipe.table = Some("Orders".into());
        recipe.tables = None;
        recipe.cdc = None;
        recipe.columns = std::collections::HashMap::from([("Orders.qty".into(), "int32".into())]);

        let mut leg = synth_snapshot_export(&stream, "Orders", "dbo.orders", &dcfg);
        apply_backfill_recipe(&mut leg, &recipe, &stream, "Orders").expect("no type conflict");

        let parsed = crate::plan::build::parse_column_overrides_pub(&leg.columns, &leg.name)
            .expect("valid types");
        let kept = crate::types::overrides_for_unit(&parsed, leg.table.as_deref());
        assert_eq!(
            kept.get("price"),
            Some(&crate::types::RivetType::Decimal {
                precision: 10,
                scale: 2
            })
        );
        assert_eq!(kept.get("qty"), Some(&crate::types::RivetType::Int32));
    }

    /// A qualified recipe key and a bare CDC key name ONE column.
    #[test]
    fn backfill_type_conflict_is_refused_when_the_recipe_key_is_qualified_and_the_cdc_key_bare() {
        let mut stream = crate::config::sample_export("stand_cdc");
        stream.mode = crate::config::ExportMode::Cdc;
        stream.table = None;
        stream.tables = Some(vec!["orders".into()]);
        stream.columns =
            std::collections::HashMap::from([("price".into(), "decimal(14,6)".into())]);
        let mut recipe = crate::config::sample_export("orders");
        recipe.table = Some("orders".into());
        recipe.tables = None;
        recipe.cdc = None;
        recipe.columns =
            std::collections::HashMap::from([("orders.price".into(), "decimal(12,4)".into())]);
        let err = format!(
            "{:#}",
            crate::config::refuse_backfill_type_conflict(&stream, "orders", &recipe)
                .expect_err("one column, two types")
        );
        assert!(err.contains("price") && err.contains("two types"), "{err}");
        recipe.columns =
            std::collections::HashMap::from([("orders.price".into(), "decimal(14,6)".into())]);
        crate::config::refuse_backfill_type_conflict(&stream, "orders", &recipe)
            .expect("agreement");
    }

    #[test]
    fn snapshot_leg_clears_exported_at_but_keeps_the_row_hash() {
        let mut e = crate::config::sample_export("orders");
        e.meta_columns.exported_at = true;
        e.meta_columns.row_hash =
            crate::config::RowHash::Columns(vec!["id".into(), "status".into()]);
        let dcfg = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("/tmp/snap".into()),
            ..Default::default()
        };
        let synth = synth_snapshot_export(&e, "orders", "orders", &dcfg);
        assert!(
            !synth.meta_columns.exported_at,
            "a per-run stamp must not ride along onto the snapshot leg"
        );
        assert_eq!(
            synth.meta_columns.row_hash, e.meta_columns.row_hash,
            "both legs write one __changes log, so both must hash the same columns"
        );
        assert_eq!(synth.mode, crate::config::ExportMode::Full);
        assert!(
            synth.cdc.is_none(),
            "snapshot leg is a plain batch, not CDC"
        );
        assert!(!synth.skip_empty, "snapshot must complete even when empty");
        assert_eq!(synth.table.as_deref(), Some("orders"));
    }

    /// `adopt` is a paid-tier mode that the OSS config must refuse.
    #[test]
    fn oss_rejects_the_pro_only_initial_adopt() {
        let parse = |line: &str| {
            crate::config::Config::from_yaml(&format!(
                "source:\n  type: postgres\n  url: \"postgresql://localhost/t\"\n\
                 exports:\n  - name: a\n    table: t\n    mode: cdc\n    format: parquet\n\
                 \x20   destination:\n      type: gcs\n      bucket: b\n      prefix: p/\n\
                 \x20   cdc:\n      checkpoint: /tmp/ck\n{line}"
            ))
        };
        let err = format!(
            "{:#}",
            parse("      initial: adopt\n").expect_err("`adopt` is not an OSS mode")
        );
        assert!(
            err.contains("adopt") || err.contains("initial"),
            "the refusal must point at the offending value; got: {err}"
        );
        assert_eq!(
            parse("      initial: snapshot\n").unwrap().exports[0]
                .cdc
                .clone()
                .unwrap()
                .initial,
            Some(crate::config::CdcInitialMode::Snapshot)
        );
        assert_eq!(
            parse("").unwrap().exports[0].cdc.clone().unwrap().initial,
            None,
            "omitted is its own third state — capture changes only, no anchor step"
        );
    }

    /// LABEL and READ are different strings, and each consumer must get the right one.
    #[test]
    fn the_snapshot_leg_carries_the_label_and_reads_the_catalog_relation() {
        let e = crate::config::sample_export("orders");
        let synth = synth_snapshot_export_for_test(&e, "orders", "sales.orders");
        assert_eq!(
            synth.table.as_deref(),
            Some("sales.orders"),
            "the relation READ follows the catalog — that is the whole point of the \
             resolution"
        );
        assert_eq!(
            synth.snapshot_label.as_deref(),
            Some("orders"),
            "and the LABEL travels with the leg, because everything a later run keys \
             on — the snapshot-done row above all — asks with the configured string"
        );
        assert!(
            synth.name.contains("orders") && !synth.name.contains("sales.orders"),
            "the leg's NAME is built from the label too, so the destination \
             sub-prefix and the manifest identity do not move when a catalog \
             resolution changes: {}",
            synth.name
        );
    }

    #[test]
    fn snapshot_leg_inherits_the_row_hash() {
        let mut e = crate::config::sample_export("orders");
        e.meta_columns.row_hash =
            crate::config::RowHash::Columns(vec!["id".into(), "status".into()]);
        e.meta_columns.exported_at = true;
        let dcfg = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("/tmp/snap".into()),
            ..Default::default()
        };
        let synth = synth_snapshot_export(&e, "orders", "orders", &dcfg);
        assert_eq!(
            synth.meta_columns.row_hash, e.meta_columns.row_hash,
            "the snapshot leg must emit the SAME hash column the drain does"
        );
        assert!(
            !synth.meta_columns.exported_at,
            "clearing the per-run stamp must not take the row hash with it"
        );
    }

    #[test]
    fn only_a_pending_snapshot_leg_writes_before_the_drain() {
        assert!(!writes_before_the_drain(&[]));
        assert!(writes_before_the_drain(&[0]));
        assert!(writes_before_the_drain(&[1, 2]));
    }

    use crate::source::cdc::PriorRun;

    const SNAPSHOT_ONLY: PriorRun = PriorRun {
        checkpoint: false,
        snapshot: true,
    };

    /// Refused: a table the last run did not capture whose baseline is done.
    #[test]
    fn only_a_table_outside_the_last_capture_with_a_done_baseline_rejoins() {
        let t = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let (a, ab) = (t(&["a"]), t(&["a", "b"]));
        assert_eq!(rejoins_over_a_baseline(&a, &ab, &[true, true]), Some(1));
        assert_eq!(rejoins_over_a_baseline(&a, &ab, &[true, false]), None);
        assert_eq!(rejoins_over_a_baseline(&ab, &ab, &[true, true]), None);
        assert_eq!(rejoins_over_a_baseline(&ab, &a, &[true]), None);
        assert_eq!(
            rejoins_over_a_baseline(&t(&["b"]), &ab, &[true, true]),
            Some(0)
        );
    }

    #[test]
    fn snapshot_plan_first_run_snapshots_all_with_no_resume_evidence() {
        assert_eq!(
            snapshot_plan(&[false, false], false),
            (vec![0, 1], PriorRun::default())
        );
    }

    #[test]
    fn snapshot_plan_all_done_snapshots_nothing_but_keeps_resume_evidence() {
        assert_eq!(
            snapshot_plan(&[true, true], false),
            (Vec::<usize>::new(), SNAPSHOT_ONLY)
        );
    }

    #[test]
    fn snapshot_plan_partial_snapshots_only_the_undone() {
        assert_eq!(
            snapshot_plan(&[true, false], false),
            (vec![1], SNAPSHOT_ONLY)
        );
    }

    #[test]
    fn snapshot_plan_checkpoint_alone_is_resume_evidence() {
        assert_eq!(
            snapshot_plan(&[false], true),
            (
                vec![0],
                PriorRun {
                    checkpoint: true,
                    snapshot: false
                }
            )
        );
    }

    #[test]
    fn snapshot_plan_no_evidence_is_a_legitimate_first_anchor() {
        assert_eq!(
            snapshot_plan(&[false], false),
            (vec![0], PriorRun::default())
        );
    }

    /// Both arms of the byte-budget supplier.
    #[test]
    fn the_byte_budget_defaults_on_absence_and_converts_on_presence() {
        assert_eq!(
            cdc_rollover_memory_bytes(None),
            Some(DEFAULT_CDC_ROLLOVER_MEMORY_BYTES),
            "absence must get the PROTECTIVE default, never read as 'no budget' \
             — losing this arm silently removes the wide-row memory ceiling"
        );
        assert_eq!(
            cdc_rollover_memory_bytes(Some(32)),
            Some(32 * 1024 * 1024),
            "a configured value is megabytes, converted — not passed through raw"
        );
    }

    #[test]
    fn one_captured_table_keeps_the_export_key_several_get_their_own() {
        assert_eq!(cdc_drift_key("orders", "orders", false), "orders");
        assert_eq!(
            cdc_drift_key("shop", "public.orders", true),
            "shop/public.orders"
        );
        assert_ne!(
            cdc_drift_key("shop", "a", true),
            cdc_drift_key("shop", "b", true),
            "two tables of one stream must not share a baseline"
        );
    }

    #[test]
    fn a_table_contradicting_its_capture_instance_is_refused_by_code() {
        let err = capture_instance_mismatch("orders", "dbo.orders", "dbo_sales", "dbo", "sales");
        crate::error::assert_coded(&err, "RIVET_CONFIG_CDC_CAPTURE_INSTANCE_MISMATCH", 1);
        assert!(
            format!("{err}").contains("Set `table:` to `dbo.sales`"),
            "{err}"
        );
    }
}
