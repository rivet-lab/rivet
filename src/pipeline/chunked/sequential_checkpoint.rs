//! Single-threaded chunk-checkpoint runner.

use std::time::Duration;

use super::super::{RunSummary, progress::ChunkProgress, sink::ExportSink};
use super::{ChunkSource, chunked_plan, config_hint, ensure_chunk_checkpoint_plan};
use crate::error::Result;
use crate::journal::RunEvent;
use crate::plan::{ChunkedPlan, ResolvedRunPlan};
use crate::resource;
use crate::source::{self, Source};
use crate::state::StateStore;

use super::math::build_chunk_query_sql;

use super::ChunkOutcome;

/// Returns `(rows, parts)` — one [`PartRecord`] per written part.
#[allow(clippy::too_many_arguments)]
fn export_one_chunk_range(
    src: &mut dyn Source,
    base_query: &str,
    cp: &ChunkedPlan,
    start: i64,
    end: i64,
    chunk_index: i64,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    row_progress: Option<(
        &crate::pipeline::progress::ChunkProgressHandle,
        &std::sync::Arc<std::sync::atomic::AtomicI64>,
    )>,
    debris: &mut Vec<super::super::commit::PartRecord>,
) -> Result<ChunkOutcome> {
    let chunk_query = build_chunk_query_sql(
        base_query,
        &cp.column,
        start,
        end,
        cp.by_days.is_some(),
        plan.source.source_type,
    );

    let mut sink = ExportSink::new(plan)?;
    if let Some((h, sr)) = row_progress {
        sink = sink.with_row_progress(h.clone(), std::sync::Arc::clone(sr));
    }
    src.export(
        &source::ExportRequest::wrapped(
            &chunk_query,
            base_query,
            &plan.tuning,
            &plan.column_overrides,
        )
        .with_catalog_hint_query(plan.catalog_query.as_deref()),
        &mut sink,
    )?;
    sink.finish_writer()?;
    if let Some(s) = sink.dest_schema.as_deref() {
        super::super::manifest_writer::record_run_schema_fingerprint(summary, s);
    }

    if sink.total_rows == 0 {
        return Ok((0, Vec::new(), Default::default(), Default::default()));
    }

    let frame = crate::pipeline::frame::RunnerFrame::open_unguarded(plan)?;
    let base = super::chunk_part_filename(&plan.export_name, chunk_index, &frame.ext);
    let dest = frame.dest;
    let (mut recs, wrote) = super::super::commit::write_sink_parts(
        dest.as_ref(),
        &mut sink,
        plan.validate.then_some(plan.format),
        |idx, count| super::super::commit::part_indexed_name(&base, idx, count),
    );
    if let Err(e) = wrote {
        debris.append(&mut recs);
        return Err(e);
    }

    let rows = sink.total_rows;
    Ok((rows, recs, sink.take_checksums(), sink.take_shape()))
}

/// The label of this runner's failures.
const RUNNER: &str = "chunk checkpoint";

pub(crate) fn run_chunked_sequential_checkpoint(
    src: &mut dyn Source,
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    config_path: &str,
    chunk_source: ChunkSource,
    progress: &crate::state::ProgressClaim<'_>,
) -> Result<()> {
    let cp = chunked_plan(plan);

    {
        let _ = crate::pipeline::frame::RunnerFrame::open(plan)?;
    }

    let chunks = if plan.resume {
        super::resumed_chunk_plan(src, plan, state, summary, &chunk_source)?
    } else {
        match chunk_source {
            ChunkSource::Detect => super::prepare_chunk_plan(src, plan, Some(state), summary)?,
            ChunkSource::Precomputed(ranges) => {
                summary.chunks_precomputed = true;
                super::check_drift_only(src, plan, Some(state), summary)?;
                ranges
            }
        }
    };

    let run_id =
        ensure_chunk_checkpoint_plan(state, plan, cp, summary, &chunks, config_path, progress)?;

    if plan.resume {
        let stats = super::apply_m8_resume_decisions(state, &run_id, plan, summary)?;
        summary.resumed = stats.adopted_prior_work();
        crate::test_hook::maybe_transient_once("after_resume_adopt")?;
    }

    let planned = state.count_chunk_tasks_total(&run_id)?;
    let pb = ChunkProgress::new(&plan.export_name, planned);
    let pb_handle = pb.handle();
    let streamed_rows = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

    if !plan.resume && !resource::check_memory(plan.tuning.memory_threshold_mb) {
        log::warn!("memory threshold exceeded before chunk export; pausing 5s");
        std::thread::sleep(Duration::from_secs(5));
    }

    let idle = super::IdleSources::default();
    let tally = super::attempt::RetryTally::default();
    let mut failures: Vec<(String, anyhow::Error)> = Vec::new();
    while let Some((chunk_index, sk, ek)) = state.claim_next_chunk_task(&run_id)? {
        if !resource::check_memory(plan.tuning.memory_threshold_mb) {
            log::warn!(
                "memory threshold exceeded, pausing 5s before chunk {}",
                chunk_index
            );
            std::thread::sleep(Duration::from_secs(5));
        }

        let start: i64 = sk.parse().map_err(|_| {
            crate::error::coded(
                crate::error::codes::STATE_CHUNK_WINDOWS_BROKEN,
                format!("chunk {}: invalid start_key {:?}", chunk_index, sk),
            )
        })?;
        let end: i64 = ek.parse().map_err(|_| {
            crate::error::coded(
                crate::error::codes::STATE_CHUNK_WINDOWS_BROKEN,
                format!("chunk {}: invalid end_key {:?}", chunk_index, ek),
            )
        })?;

        log::info!(
            "export '{}': checkpoint chunk {} ({}..{})",
            plan.export_name,
            chunk_index,
            start,
            end
        );

        summary.journal.record(RunEvent::ChunkStarted {
            chunk_index,
            start_key: sk.clone(),
            end_key: ek.clone(),
        });

        let mut debris = Vec::new();
        let chunk_result = match crate::test_hook::maybe_error_at_index("chunk_export", chunk_index)
        {
            Err(e) => Err(e),
            Ok(()) => {
                let r = super::attempt::run_with_retries(
                    plan,
                    chunk_index,
                    &tally,
                    |a| super::attempt::open_for_attempt(&idle, &plan.source, a),
                    |src| {
                        export_one_chunk_range(
                            &mut **src,
                            &plan.base_query,
                            cp,
                            start,
                            end,
                            chunk_index,
                            plan,
                            summary,
                            Some((&pb_handle, &streamed_rows)),
                            &mut debris,
                        )
                    },
                    |src| idle.give(src),
                );
                tally.fold_into(summary);
                r
            }
        };
        let unit = super::super::commit::UnitId::Chunk(chunk_index);
        let kind = |_| super::super::commit::PartKind::Chunk { chunk_index };
        match chunk_result {
            Ok((rows, parts, chunk_checksums, chunk_shape)) => {
                pb.inc(summary.total_rows + rows as i64);
                let fname = parts.first().map(|p| p.file_name.clone());
                crate::pipeline::fan_in::commit_unit(
                    plan,
                    summary,
                    Some(state),
                    unit,
                    parts,
                    kind,
                    chunk_shape,
                    Ok(chunk_checksums),
                    |_| {
                        crate::test_hook::maybe_panic_at_chunk("after_chunk_file", chunk_index);
                        state.complete_chunk_task(
                            &run_id,
                            chunk_index,
                            rows as i64,
                            fname.as_deref(),
                        )?;
                        crate::test_hook::maybe_panic_at_chunk("after_chunk_complete", chunk_index);
                        Ok(())
                    },
                )?;
            }
            Err(e) => {
                let msg = crate::redact::redact_error(&e);
                let retryable = crate::pipeline::retry::is_transient(&e);
                log::error!(
                    "export '{}': chunk {} failed: {}",
                    plan.export_name,
                    chunk_index,
                    msg
                );
                summary.journal.record(RunEvent::ChunkFailed {
                    chunk_index,
                    error: msg.clone(),
                    attempt: 1,
                });
                let failed = crate::pipeline::fan_in::commit_unit(
                    plan,
                    summary,
                    Some(state),
                    unit,
                    debris,
                    kind,
                    Default::default(),
                    Err(e),
                    |_| Ok(()),
                );
                state.fail_chunk_task(&run_id, chunk_index, &msg, retryable)?;
                if let Err(e) = failed {
                    failures.push((format!("chunk {chunk_index}"), e));
                }
            }
        }
    }

    let (pending, tasks) = state.settle_chunk_run(&run_id, planned)?;
    let met = if failures.is_empty() {
        Ok(())
    } else {
        Err(crate::pipeline::fan_in::unit_failures(failures, |lines| {
            super::chunk_failures(&plan.export_name, RUNNER, lines)
        }))
    };
    super::incomplete_run_check(
        &plan.export_name,
        RUNNER,
        pending,
        &tasks,
        &config_hint(config_path),
        met,
    )?;

    super::prune_superseded_attempts(summary, &tasks);
    pb.finish(summary.total_rows);
    state.finalize_chunk_run_completed(&run_id)?;
    super::super::run_store::RunStore::finalize(state, plan, summary)
        .with_progression(super::super::run_store::Progression::Chunked)
        .commit()?;
    log::info!(
        "export '{}': chunk checkpoint run completed (run_id={})",
        plan.export_name,
        run_id
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// A source that streams three `id` rows whatever the query.
    struct ThreeRows;

    impl Source for ThreeRows {
        fn export(
            &mut self,
            _request: &source::ExportRequest<'_>,
            sink: &mut dyn source::BatchSink,
        ) -> Result<()> {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            sink.on_schema(Arc::clone(&schema))?;
            let ids = Int64Array::from(vec![1, 2, 3]);
            sink.on_batch(&arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![Arc::new(ids)],
            )?)
        }
        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>> {
            Ok(None)
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    #[test]
    fn a_chunk_with_rows_reports_them_and_commits_its_part() {
        let out = tempfile::tempdir().unwrap();
        let mut plan = crate::plan::test_support::test_plan();
        plan.destination.path = Some(out.path().to_string_lossy().into_owned());
        let cp = ChunkedPlan {
            column: "id".into(),
            chunk_size: 10,
            chunk_count: None,
            parallel: 1,
            by_days: None,
            checkpoint: true,
            max_attempts: 1,
        };
        let mut summary = RunSummary::stub_for_testing("r1", "orders");
        let mut debris = Vec::new();
        let (rows, parts, _, _) = export_one_chunk_range(
            &mut ThreeRows,
            "SELECT id FROM t",
            &cp,
            1,
            10,
            0,
            &plan,
            &mut summary,
            None,
            &mut debris,
        )
        .unwrap();
        assert_eq!(rows, 3);
        assert_eq!(parts.len(), 1);
        assert!(out.path().join(&parts[0].file_name).is_file());
    }
}
