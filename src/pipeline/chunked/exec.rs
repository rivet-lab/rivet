//! **Layer: Execution** — stateless chunked execution paths.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::super::{RunSummary, progress::ChunkProgress, sink::ExportSink};
use super::math::build_chunk_query_sql;
use crate::error::Result;
use crate::journal::RunEvent;
use crate::plan::ResolvedRunPlan;
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::{format, resource};

pub(crate) fn run_chunked_sequential(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    state: Option<&StateStore>,
    chunk_source: super::ChunkSource,
) -> Result<()> {
    let cp = super::chunked_plan(plan);

    let chunks = match chunk_source {
        super::ChunkSource::Detect => super::prepare_chunk_plan(src, plan, state, summary)?,
        super::ChunkSource::Precomputed(ranges) => {
            summary.chunks_precomputed = true;
            super::check_drift_only(src, plan, state, summary)?;
            ranges
        }
    };

    let is_date = cp.by_days.is_some();

    log::info!(
        "export '{}': {} chunks to process sequentially",
        plan.export_name,
        chunks.len()
    );

    let pb = ChunkProgress::new(&plan.export_name, chunks.len());
    let streamed_rows = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

    let _ = crate::pipeline::frame::RunnerFrame::open(plan)?;

    for (i, (start, end)) in chunks.iter().enumerate() {
        if !resource::check_memory(plan.tuning.memory_threshold_mb) {
            log::warn!("memory threshold exceeded, pausing 5s before chunk {}", i);
            std::thread::sleep(Duration::from_secs(5));
        }

        let chunk_query = build_chunk_query_sql(
            &plan.base_query,
            &cp.column,
            *start,
            *end,
            is_date,
            plan.source.source_type,
        );
        log::info!(
            "export '{}': chunk {}/{} ({}..{})",
            plan.export_name,
            i + 1,
            chunks.len(),
            start,
            end
        );

        summary.journal.record(RunEvent::ChunkStarted {
            chunk_index: i as i64,
            start_key: start.to_string(),
            end_key: end.to_string(),
        });

        let mut sink = ExportSink::new(plan)?
            .with_row_progress(pb.handle(), std::sync::Arc::clone(&streamed_rows));
        src.export(
            &source::ExportRequest::wrapped(
                &chunk_query,
                &plan.base_query,
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

        pb.inc(summary.total_rows + sink.total_rows as i64);
        log::info!(
            "export '{}': chunk {} -- {} rows",
            plan.export_name,
            i + 1,
            sink.total_rows
        );

        let (parts, wrote) = if sink.total_rows > 0 {
            crate::test_hook::maybe_transient_once("chunk_write")?;
            let frame = crate::pipeline::frame::RunnerFrame::open_unguarded(plan)?;
            let base = super::chunk_part_filename(&plan.export_name, i, &frame.ext);
            super::super::commit::write_sink_parts(
                frame.dest.as_ref(),
                &mut sink,
                plan.validate.then_some(plan.format),
                |idx, count| super::super::commit::part_indexed_name(&base, idx, count),
            )
        } else {
            (Vec::new(), Ok(()))
        };
        let shape = sink.take_shape();
        let checksums = wrote.map(|()| sink.take_checksums());
        let chunk_index = i as i64;
        crate::pipeline::fan_in::commit_unit(
            plan,
            summary,
            state,
            super::super::commit::UnitId::Chunk(chunk_index),
            parts,
            |_| super::super::commit::PartKind::Chunk { chunk_index },
            shape,
            checksums,
            |_| Ok(()),
        )?;
    }

    pb.finish(summary.total_rows);
    log::info!("export '{}': all chunks completed", plan.export_name);
    Ok(())
}

pub(in crate::pipeline) fn run_chunked_parallel(
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    chunk_source: super::ChunkSource,
    meta: &mut crate::pipeline::job::MetaConn<'_>,
) -> Result<()> {
    summary.state_backed = true;
    let cp = super::chunked_plan(plan);

    let chunks = match chunk_source {
        super::ChunkSource::Detect => {
            super::prepare_chunk_plan(meta.require()?, plan, Some(state), summary)?
        }
        super::ChunkSource::Precomputed(ranges) => {
            summary.chunks_precomputed = true;
            if super::precomputed_ranges_need_the_drift_gate(&ranges) {
                super::check_drift_only(meta.require()?, plan, Some(state), summary)?;
            }
            ranges
        }
    };

    let is_date = cp.by_days.is_some();

    let total_chunks = chunks.len();
    let parallel = cp.parallel.min(total_chunks);
    log::info!(
        "export '{}': {} chunks, {} parallel threads",
        plan.export_name,
        total_chunks,
        parallel
    );

    let completed = AtomicUsize::new(0);
    let idle_sources = super::IdleSources::default();
    let streamed_rows = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
    let fan = crate::pipeline::fan_in::FanIn::default();
    let shared_fingerprint: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let semaphore = resource::Semaphore::new(parallel);
    let pb = ChunkProgress::new(&plan.export_name, total_chunks);
    let pb_handle = pb.handle();

    let governor = crate::pipeline::governor::GovernorHarness::arm(plan, parallel);

    let (shared_destination, _frame_ext) = crate::pipeline::frame::RunnerFrame::open_shared(plan)?;

    std::thread::scope(|s| {
        governor.spawn_into(
            s,
            &semaphore,
            fan.finished(),
            total_chunks,
            &plan.export_name,
        );

        for (i, (start, end)) in chunks.iter().enumerate() {
            semaphore.acquire();

            if !resource::check_memory(plan.tuning.memory_threshold_mb) {
                log::warn!("memory threshold exceeded, waiting before chunk {}", i);
                while !resource::check_memory(plan.tuning.memory_threshold_mb) {
                    std::thread::sleep(Duration::from_secs(2));
                }
            }

            let plan_for_worker = plan.clone();
            let export_name = &plan.export_name;
            let base_query = &plan.base_query;
            let col = &cp.column;
            let completed = &completed;
            let idle_sources = &idle_sources;
            let fan_r = &fan;
            let shared_fingerprint = &shared_fingerprint;
            let semaphore = &semaphore;
            let pb_thread = pb_handle.clone();
            let streamed_rows = std::sync::Arc::clone(&streamed_rows);
            let start = *start;
            let end = *end;
            let shared_destination = std::sync::Arc::clone(&shared_destination);

            fan.spawn(s, format!("chunk {i}"), move || {
                let _permit = crate::pipeline::governor::TaskPermit::adopt(semaphore);
                {
                    crate::test_hook::maybe_error_at_index("chunk_export", i as i64)?;

                    let chunk_query = build_chunk_query_sql(
                        base_query,
                        col,
                        start,
                        end,
                        is_date,
                        plan_for_worker.source.source_type,
                    );

                    let mut thread_src =
                        idle_sources.take(&plan_for_worker.source).map_err(|e| {
                            crate::pipeline::single::attach_connect_hint(e, &plan_for_worker.source)
                        })?;
                    let mut sink = ExportSink::new(&plan_for_worker)?.with_row_progress(
                        pb_thread.clone(),
                        std::sync::Arc::clone(&streamed_rows),
                    );
                    thread_src.export(
                        &source::ExportRequest::wrapped(
                            &chunk_query,
                            &plan_for_worker.base_query,
                            &plan_for_worker.tuning,
                            &plan_for_worker.column_overrides,
                        )
                        .with_catalog_hint_query(plan_for_worker.catalog_query.as_deref()),
                        &mut sink,
                    )?;
                    idle_sources.give(thread_src);
                    sink.finish_writer()?;
                    if let Some(s) = sink.dest_schema.as_deref() {
                        let columns = crate::state::arrow_schema_to_columns(s);
                        let _ = shared_fingerprint.set(crate::state::schema_fingerprint(&columns));
                    }

                    if sink.total_rows > 0 {
                        let fmt = format::create_format(
                            plan_for_worker.format,
                            plan_for_worker.compression,
                            plan_for_worker.compression_level,
                            None,
                        );
                        let base = super::chunk_part_filename(export_name, i, fmt.file_extension());
                        let (recs, wrote) = super::super::commit::write_sink_parts(
                            &**shared_destination,
                            &mut sink,
                            plan_for_worker.validate.then_some(plan_for_worker.format),
                            |idx, count| super::super::commit::part_indexed_name(&base, idx, count),
                        );
                        let unit = super::super::commit::UnitId::Chunk(i as i64);
                        for rec in recs {
                            fan_r.part(unit, rec);
                        }
                        wrote?;
                        fan_r.observe(sink.take_shape());
                        fan_r.contribute(unit, sink.take_checksums());
                    }
                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    pb_thread.inc(streamed_rows.load(Ordering::Relaxed));
                    log::info!(
                        "export '{}': chunk {}/{} done ({} rows)",
                        export_name,
                        done,
                        total_chunks,
                        sink.total_rows
                    );
                    Ok(())
                }
            });
        }
    });

    if let Some(fp) = shared_fingerprint.into_inner() {
        summary.schema_fingerprint = Some(fp);
    }

    let drained = fan.finish(
        plan,
        summary,
        Some(state),
        Some(governor),
        |_, unit| match unit {
            super::super::commit::UnitId::Chunk(chunk_index) => {
                super::super::commit::PartKind::Chunk { chunk_index }
            }
            other => unreachable!("chunked parts are recorded under a chunk unit, not {other:?}"),
        },
        |errs| {
            crate::error::coded(
                crate::error::codes::RUN_UNITS_FAILED,
                format!(
                    "export '{}': {} chunks failed:\n{}",
                    plan.export_name,
                    errs.len(),
                    errs.join("\n")
                ),
            )
        },
    );
    pb.finish(summary.total_rows);
    drained?;

    log::info!(
        "export '{}': all {} chunks completed",
        plan.export_name,
        total_chunks
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::test_support::test_plan;
    use crate::plan::{ChunkedPlan, ExtractionStrategy, ResolvedRunPlan};
    use crate::source::BatchSink;
    use crate::state::StateStore;

    use super::super::ChunkSource;

    struct EmptySource;

    impl crate::source::Source for EmptySource {
        fn query_scalar(&mut self, _sql: &str) -> crate::error::Result<Option<String>> {
            unimplemented!("not needed in chunked exec tests")
        }
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn BatchSink,
        ) -> crate::error::Result<()> {
            Ok(())
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _column_overrides: &crate::types::ColumnOverrides,
        ) -> crate::error::Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    fn chunked_plan_struct() -> ResolvedRunPlan {
        ResolvedRunPlan {
            base_query: "SELECT id FROM orders".into(),
            strategy: ExtractionStrategy::Chunked(ChunkedPlan {
                column: "id".into(),
                chunk_size: 100,
                chunk_count: None,
                parallel: 1,
                by_days: None,
                checkpoint: false,
                max_attempts: 3,
            }),
            ..test_plan()
        }
    }

    /// `rivet apply` must REPLAY the artifact's chunk boundaries on the SEQUENTIAL path, not re-derive them.
    #[test]
    fn apply_replays_precomputed_chunks_on_the_sequential_path() {
        let plan = chunked_plan_struct();
        let mut summary = empty_summary(&plan);
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let mut src = EmptySource;

        let progress = state.claim(plan.progress_key()).expect("claim");
        crate::pipeline::single::run_export(
            &mut src,
            &state,
            &progress,
            &plan,
            &mut summary,
            "",
            ChunkSource::Precomputed(vec![(1, 100)]),
        )
        .expect("precomputed apply must not touch the source for boundaries");

        assert_eq!(summary.total_rows, 0);

        assert!(
            summary.chunks_precomputed,
            "a precomputed chunk source must be recorded on the summary, or              `check_post_run_invariants` cannot tell a by-contract skip from a bypass"
        );
    }

    fn empty_summary(plan: &ResolvedRunPlan) -> RunSummary {
        let mut s = RunSummary::stub_for_testing("test_run", plan.export_name.clone());
        s.batch_size = 10_000;
        s.mode = "chunked".into();
        s.compression = "none".into();
        s
    }

    /// Three `id` rows on the first export call, a schema with no rows after it.
    struct RowsThenEmpty(usize);

    impl crate::source::Source for RowsThenEmpty {
        fn query_scalar(&mut self, _sql: &str) -> crate::error::Result<Option<String>> {
            unimplemented!("not needed in chunked exec tests")
        }
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            sink: &mut dyn BatchSink,
        ) -> crate::error::Result<()> {
            use arrow::datatypes::{DataType, Field, Schema};
            let schema =
                std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            sink.on_schema(std::sync::Arc::clone(&schema))?;
            self.0 += 1;
            if self.0 > 1 {
                return Ok(());
            }
            let ids = arrow::array::Int64Array::from(vec![1, 2, 3]);
            sink.on_batch(&arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![std::sync::Arc::new(ids)],
            )?)
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _column_overrides: &crate::types::ColumnOverrides,
        ) -> crate::error::Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    /// A chunk with rows commits its part; a chunk that saw a schema but no rows writes nothing and journals its completion.
    #[test]
    fn sequential_writes_only_the_chunk_that_has_rows() {
        let out = tempfile::tempdir().unwrap();
        let mut plan = chunked_plan_struct();
        plan.destination.path = Some(out.path().to_string_lossy().into_owned());
        let mut summary = empty_summary(&plan);
        run_chunked_sequential(
            &mut RowsThenEmpty(0),
            &plan,
            &mut summary,
            None,
            ChunkSource::Precomputed(vec![(1, 100), (101, 200)]),
        )
        .unwrap();
        assert_eq!((summary.total_rows, summary.files_committed), (3, 1));
        let empty_done = summary.journal.entries.iter().any(|e| {
            matches!(
                e.event,
                RunEvent::ChunkCompleted {
                    chunk_index: 1,
                    rows: 0,
                    file_name: None
                }
            )
        });
        assert!(empty_done, "the empty chunk journals its completion");
    }

    #[test]
    fn sequential_empty_precomputed_returns_ok_with_zero_rows() {
        let plan = chunked_plan_struct();
        let mut summary = empty_summary(&plan);
        let mut src = EmptySource;
        run_chunked_sequential(
            &mut src,
            &plan,
            &mut summary,
            None,
            ChunkSource::Precomputed(vec![]),
        )
        .unwrap();
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
    }

    #[test]
    fn sequential_one_chunk_zero_rows_skips_destination_write() {
        let plan = chunked_plan_struct();
        let mut summary = empty_summary(&plan);
        let mut src = EmptySource;
        run_chunked_sequential(
            &mut src,
            &plan,
            &mut summary,
            None,
            ChunkSource::Precomputed(vec![(1, 100)]),
        )
        .unwrap();
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
    }

    #[test]
    fn sequential_multiple_empty_chunks_all_processed() {
        let plan = chunked_plan_struct();
        let mut summary = empty_summary(&plan);
        let mut src = EmptySource;
        run_chunked_sequential(
            &mut src,
            &plan,
            &mut summary,
            None,
            ChunkSource::Precomputed(vec![
                (1, 100),
                (101, 200),
                (201, 300),
                (301, 400),
                (401, 500),
            ]),
        )
        .unwrap();
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
    }

    #[test]
    fn parallel_empty_precomputed_returns_ok() {
        let plan = chunked_plan_struct();
        let mut summary = empty_summary(&plan);
        let state = StateStore::open_in_memory().unwrap();
        run_chunked_parallel(
            &state,
            &plan,
            &mut summary,
            ChunkSource::Precomputed(vec![]),
            &mut crate::pipeline::job::MetaConn::empty(&plan.source),
        )
        .unwrap();
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
    }
}
