//! Multi-worker chunk-checkpoint runner — twin of `sequential_checkpoint.rs`.

use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::{RunSummary, progress::ChunkProgress, sink::ExportSink};
use super::{ChunkSource, chunked_plan, config_hint, ensure_chunk_checkpoint_plan};
use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::source;
use crate::state::StateStore;
use crate::{format, resource};

use super::math::build_chunk_query_sql;

use super::ChunkOutcome;

/// The label of this runner's failures, the one `summary::compact_error` and `job::classify_error_message` read.
const RUNNER: &str = "parallel checkpoint worker";

pub(in crate::pipeline) fn run_chunked_parallel_checkpoint(
    config_path: &str,
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    chunk_source: ChunkSource,
    meta: &mut crate::pipeline::job::MetaConn<'_>,
    progress: &crate::state::ProgressClaim<'_>,
) -> Result<()> {
    summary.state_backed = true;
    let cp = chunked_plan(plan);

    let chunks = if plan.resume {
        super::resumed_chunk_plan(meta.require()?, plan, state, summary, &chunk_source)?
    } else {
        match chunk_source {
            ChunkSource::Detect => {
                super::prepare_chunk_plan(meta.require()?, plan, Some(state), summary)?
            }
            ChunkSource::Precomputed(ranges) => {
                summary.chunks_precomputed = true;
                if super::precomputed_ranges_need_the_drift_gate(&ranges) {
                    super::check_drift_only(meta.require()?, plan, Some(state), summary)?;
                }
                ranges
            }
        }
    };

    let run_id =
        ensure_chunk_checkpoint_plan(state, plan, cp, summary, &chunks, config_path, progress)?;

    if plan.resume {
        let stats = super::apply_m8_resume_decisions(state, &run_id, plan, summary)?;
        summary.resumed = stats.adopted_prior_work();
    }

    let planned = state.list_chunk_tasks_for_run(&run_id)?.len();
    let total_tasks = planned.max(1);
    let parallel = cp.parallel.min(total_tasks);
    let pb_cp = ChunkProgress::new(&plan.export_name, total_tasks);
    let pb_cp_handle = pb_cp.handle();
    log::info!(
        "export '{}': chunk checkpoint parallel: {} workers, run_id={}",
        plan.export_name,
        parallel,
        run_id
    );

    let state_ref = state.state_ref().clone();
    let run_id_arc = std::sync::Arc::new(run_id.clone());
    let streamed_rows = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
    let tally = super::attempt::RetryTally::default();
    let idle_sources = super::IdleSources::default();
    let fan = crate::pipeline::fan_in::FanIn::default();
    let shared_fingerprint: std::sync::OnceLock<String> = std::sync::OnceLock::new();

    let plan_for_workers = plan.clone();
    let cp_for_workers = cp.clone();
    let fmt_label = plan.format.label();
    let comp_label = plan.compression.label();
    let mode_label = plan.strategy.mode_label();

    let (shared_destination, _frame_ext) = crate::pipeline::frame::RunnerFrame::open_shared(plan)?;

    let semaphore = resource::Semaphore::new(parallel.max(1));
    let governor = crate::pipeline::governor::GovernorHarness::arm(plan, parallel);

    std::thread::scope(|s| {
        governor.spawn_into(s, &semaphore, fan.finished(), parallel, &plan.export_name);

        for _ in 0..parallel {
            let state_ref = state_ref.clone();
            let shared_destination = std::sync::Arc::clone(&shared_destination);
            let run_id_arc = std::sync::Arc::clone(&run_id_arc);
            let tally = &tally;
            let idle_sources = &idle_sources;
            let fan_r = &fan;
            let shared_fingerprint = &shared_fingerprint;
            let plan_w = plan_for_workers.clone();
            let cp_w = cp_for_workers.clone();
            let fmt_label_w = fmt_label;
            let comp_label_w = comp_label;
            let mode_label_w = mode_label;
            let pb_w = pb_cp_handle.clone();
            let streamed_rows = std::sync::Arc::clone(&streamed_rows);
            let semaphore = &semaphore;

            s.spawn(move || {
                let _finish = crate::pipeline::governor::WorkerFinished::new(fan_r.finished());
                let shared_destination = shared_destination;
                let st = match StateStore::open_at_ref(&state_ref) {
                    Ok(st) => st,
                    Err(e) => {
                        fan_r.fail("state open", e);
                        return;
                    }
                };
                loop {
                    let _permit = crate::pipeline::governor::TaskPermit::acquire(semaphore);
                    let claimed = match st.claim_next_chunk_task(run_id_arc.as_str()) {
                        Ok(c) => c,
                        Err(e) => {
                            fan_r.fail("claim error", e);
                            break;
                        }
                    };
                    let Some((chunk_index, sk, ek)) = claimed else {
                        break;
                    };

                    if !resource::check_memory(plan_w.tuning.memory_threshold_mb) {
                        log::warn!("memory threshold exceeded in worker; pausing 2s");
                        std::thread::sleep(Duration::from_secs(2));
                    }

                    let (start, end): (i64, i64) = match (sk.parse(), ek.parse()) {
                        (Ok(s), Ok(e)) => (s, e),
                        _ => {
                            let e = crate::error::coded(
                                crate::error::codes::STATE_CHUNK_WINDOWS_BROKEN,
                                format!("chunk {chunk_index}: invalid window {sk:?}..{ek:?}"),
                            );
                            fan_r.fail_on_err(
                                &format!("chunk {chunk_index} state"),
                                st.fail_chunk_task(
                                    run_id_arc.as_str(),
                                    chunk_index,
                                    &format!("{e:#}"),
                                    false,
                                ),
                            );
                            fan_r.fail(&format!("chunk {chunk_index}"), e);
                            continue;
                        }
                    };

                    let chunk_query = build_chunk_query_sql(
                        &plan_w.base_query,
                        &cp_w.column,
                        start,
                        end,
                        cp_w.by_days.is_some(),
                        plan_w.source.source_type,
                    );

                    let mut debris: Vec<super::super::commit::PartRecord> = Vec::new();
                    let result = super::attempt::run_with_retries(
                        &plan_w,
                        chunk_index,
                        tally,
                        |a| super::attempt::open_for_attempt(idle_sources, &plan_w.source, a),
                        |thread_src| -> Result<ChunkOutcome> {
                            let mut sink = ExportSink::new(&plan_w)?.with_row_progress(
                                pb_w.clone(),
                                std::sync::Arc::clone(&streamed_rows),
                            );
                            thread_src.export(
                                &source::ExportRequest::wrapped(
                                    &chunk_query,
                                    &plan_w.base_query,
                                    &plan_w.tuning,
                                    &plan_w.column_overrides,
                                )
                                .with_catalog_hint_query(plan_w.catalog_query.as_deref()),
                                &mut sink,
                            )?;
                            sink.finish_writer()?;
                            if let Some(s) = sink.dest_schema.as_deref() {
                                let columns = crate::state::arrow_schema_to_columns(s);
                                let _ = shared_fingerprint
                                    .set(crate::state::schema_fingerprint(&columns));
                            }
                            if sink.is_empty() {
                                return Ok((0, Vec::new(), Default::default(), Default::default()));
                            }
                            let fmt = format::create_format(
                                plan_w.format,
                                plan_w.compression,
                                plan_w.compression_level,
                                None,
                            );
                            let base = super::chunk_part_filename(
                                &plan_w.export_name,
                                chunk_index,
                                fmt.file_extension(),
                            );
                            let (mut recs, wrote) = super::super::commit::write_sink_parts(
                                &**shared_destination,
                                &mut sink,
                                plan_w.validate.then_some(plan_w.format),
                                |idx, count| {
                                    super::super::commit::part_indexed_name(&base, idx, count)
                                },
                            );
                            if let Err(e) = wrote {
                                debris.append(&mut recs);
                                return Err(e);
                            }
                            Ok((
                                sink.total_rows,
                                recs,
                                sink.take_checksums(),
                                sink.take_shape(),
                            ))
                        },
                        |src| idle_sources.give(src),
                    );

                    let result = match (
                        result,
                        crate::test_hook::maybe_error_at_index("chunk_export", chunk_index),
                    ) {
                        (Ok((_, parts, ..)), Err(e)) => {
                            debris.extend(parts);
                            Err(e)
                        }
                        (result, _) => result,
                    };
                    match result {
                        Ok((rows, parts, chunk_checksums, chunk_shape)) => {
                            let fname_for_state: Option<String> = if parts.is_empty() {
                                None
                            } else {
                                record_durable_parts(
                                    &st,
                                    crate::state::DurablePart {
                                        run_id: run_id_arc.as_str(),
                                        export_name: &plan_w.export_name,
                                        file_name: "",
                                        rows: 0,
                                        bytes: 0,
                                        format: fmt_label_w,
                                        compression: Some(comp_label_w),
                                        mode: mode_label_w,
                                        cursor_high: None,
                                    },
                                    &parts,
                                );
                                let first = parts[0].file_name.clone();
                                let unit = super::super::commit::UnitId::Chunk(chunk_index);
                                for rec in parts {
                                    fan_r.part(unit, rec);
                                }
                                fan_r.observe(chunk_shape);
                                fan_r.contribute(unit, chunk_checksums);
                                Some(first)
                            };
                            crate::test_hook::maybe_panic_at_chunk("after_chunk_file", chunk_index);
                            fan_r.fail_on_err(
                                &format!("chunk {chunk_index} state"),
                                st.complete_chunk_task(
                                    run_id_arc.as_str(),
                                    chunk_index,
                                    rows as i64,
                                    fname_for_state.as_deref(),
                                ),
                            );
                            crate::test_hook::maybe_panic_at_chunk(
                                "after_chunk_complete",
                                chunk_index,
                            );
                            pb_w.inc(streamed_rows.load(Ordering::Relaxed));
                        }
                        Err(e) => {
                            let msg = crate::redact::redact_error(&e);
                            if !debris.is_empty() {
                                record_durable_parts(
                                    &st,
                                    crate::state::DurablePart {
                                        run_id: run_id_arc.as_str(),
                                        export_name: &plan_w.export_name,
                                        file_name: "",
                                        rows: 0,
                                        bytes: 0,
                                        format: fmt_label_w,
                                        compression: Some(comp_label_w),
                                        mode: mode_label_w,
                                        cursor_high: None,
                                    },
                                    &debris,
                                );
                                let unit = super::super::commit::UnitId::Chunk(chunk_index);
                                for rec in debris.drain(..) {
                                    fan_r.part(unit, rec);
                                }
                            }
                            fan_r.fail_on_err(
                                &format!("chunk {chunk_index} state"),
                                st.fail_chunk_task(
                                    run_id_arc.as_str(),
                                    chunk_index,
                                    &msg,
                                    crate::pipeline::retry::is_transient(&e),
                                ),
                            );
                            fan_r.fail(&format!("chunk {chunk_index}"), e);
                        }
                    }
                }
            });
        }
    });

    tally.fold_into(summary);
    if let Some(fp) = shared_fingerprint.into_inner() {
        summary.schema_fingerprint = Some(fp);
    }

    let drained = fan.finish(
        plan,
        summary,
        None,
        Some(governor),
        |_, unit| match unit {
            super::super::commit::UnitId::Chunk(chunk_index) => {
                super::super::commit::PartKind::Chunk { chunk_index }
            }
            other => unreachable!("chunked parts are recorded under a chunk unit, not {other:?}"),
        },
        |errs| super::chunk_failures(&plan.export_name, RUNNER, errs),
    );
    pb_cp.finish(summary.total_rows);

    let (pending, tasks) = state.settle_chunk_run(&run_id, planned)?;
    super::incomplete_run_check(
        &plan.export_name,
        RUNNER,
        pending,
        &tasks,
        &config_hint(config_path),
        drained,
    )?;

    super::prune_superseded_attempts(summary, &tasks);
    state.finalize_chunk_run_completed(&run_id)?;
    super::super::run_store::RunStore::finalize(state, plan, summary)
        .with_progression(super::super::run_store::Progression::Chunked)
        .commit()?;
    log::info!(
        "export '{}': chunk checkpoint parallel run completed",
        plan.export_name
    );
    Ok(())
}

/// Write one `file_log` row per durable part on the worker's store (the drain records with no state).
fn record_durable_parts(
    store: &StateStore,
    template: crate::state::DurablePart<'_>,
    parts: &[super::super::commit::PartRecord],
) {
    for rec in parts {
        if let Err(e) = store.record_durable_part(crate::state::DurablePart {
            file_name: &rec.file_name,
            rows: rec.rows,
            bytes: rec.bytes as i64,
            ..template
        }) {
            log::warn!(
                "export '{}': file_log write failed for parallel checkpoint part '{}' (file was produced): {:#}",
                template.export_name,
                rec.file_name,
                e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn every_durable_part_gets_its_own_file_log_row() {
        use crate::pipeline::commit::PartRecord;
        let d = tempfile::tempdir().unwrap();
        let cfg = d.path().join("rivet.yaml");
        let store = crate::state::StateStore::open(cfg.to_str().unwrap()).unwrap();
        let part = |name: &str, rows: i64| PartRecord {
            file_name: name.into(),
            rows,
            bytes: 10,
            fingerprint: String::new(),
            md5: String::new(),
        };
        super::record_durable_parts(
            &store,
            crate::state::DurablePart {
                run_id: "r1",
                export_name: "orders",
                file_name: "",
                rows: 0,
                bytes: 0,
                format: "parquet",
                compression: None,
                mode: "chunked",
                cursor_high: None,
            },
            &[part("a.parquet", 3), part("b.parquet", 4)],
        );
        let mut got: Vec<(String, i64)> = store
            .list_files_for_run("r1")
            .unwrap()
            .into_iter()
            .map(|f| (f.file_name, f.row_count))
            .collect();
        got.sort();
        assert_eq!(got, vec![("a.parquet".into(), 3), ("b.parquet".into(), 4)]);
    }
}
