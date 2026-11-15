//! **Layer: Execution**

use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::JoinHandle;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::ExportSink;
use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::source::BatchSink;

/// Channel depth: how many batches may sit between fetch and encode.
const DEFAULT_CHANNEL_DEPTH: usize = 3;

enum SinkMsg {
    Schema(SchemaRef),
    Batch(RecordBatch),
    /// A lossless keyset token from the source (Mongo's typed BSON `_id`).
    SourceCursor(String),
}

/// A `BatchSink` that forwards schema/batches to a background encode worker.
pub(crate) struct PipelinedSink {
    tx: Option<SyncSender<SinkMsg>>,
    worker: Option<JoinHandle<Result<ExportSink>>>,
    nulled_cells: std::sync::Arc<crate::plan::NulledCells>,
}

impl PipelinedSink {
    /// Returns `true` when the pipelined write path is requested.
    pub fn enabled() -> bool {
        Self::configured_depth().is_some()
    }

    /// Resolve the encode-worker channel depth.
    fn configured_depth() -> Option<usize> {
        match std::env::var("RIVET_PIPELINE_WRITES") {
            Ok(v) if v == "0" => None,
            Ok(v) if v.is_empty() => Some(DEFAULT_CHANNEL_DEPTH),
            Ok(v) => Some(
                v.parse::<usize>()
                    .ok()
                    .filter(|&n| n > 0)
                    .unwrap_or(DEFAULT_CHANNEL_DEPTH),
            ),
            Err(_) => Some(DEFAULT_CHANNEL_DEPTH),
        }
    }

    /// Spawn the encode worker.
    pub fn spawn(plan: &ResolvedRunPlan) -> Result<Self> {
        let nulled_cells = std::sync::Arc::clone(&plan.nulled_cells);
        let plan = plan.clone();
        let depth = Self::configured_depth().unwrap_or(DEFAULT_CHANNEL_DEPTH);
        let (tx, rx) = sync_channel::<SinkMsg>(depth);
        let worker = std::thread::Builder::new()
            .name("rivet-encode".to_string())
            .spawn(move || -> Result<ExportSink> {
                let mut sink = ExportSink::new(&plan)?;
                while let Ok(msg) = rx.recv() {
                    match msg {
                        SinkMsg::Schema(s) => sink.on_schema(s)?,
                        SinkMsg::Batch(b) => sink.on_batch(&b)?,
                        SinkMsg::SourceCursor(t) => sink.set_source_cursor(t),
                    }
                }
                Ok(sink)
            })?;
        Ok(Self {
            tx: Some(tx),
            worker: Some(worker),
            nulled_cells,
        })
    }

    /// Test-only: wrap an already-built `ExportSink` (bypasses plan construction).
    #[cfg(test)]
    pub(crate) fn spawn_with_sink(sink: ExportSink) -> Self {
        let nulled_cells = std::sync::Arc::clone(&sink.nulled_cells);
        let (tx, rx) = sync_channel::<SinkMsg>(DEFAULT_CHANNEL_DEPTH);
        let worker = std::thread::Builder::new()
            .name("rivet-encode-test".to_string())
            .spawn(move || -> Result<ExportSink> {
                let mut sink = sink;
                while let Ok(msg) = rx.recv() {
                    match msg {
                        SinkMsg::Schema(s) => sink.on_schema(s)?,
                        SinkMsg::Batch(b) => sink.on_batch(&b)?,
                        SinkMsg::SourceCursor(t) => sink.set_source_cursor(t),
                    }
                }
                Ok(sink)
            })
            .expect("spawn test encode worker");
        Self {
            tx: Some(tx),
            worker: Some(worker),
            nulled_cells,
        }
    }

    fn send(&mut self, msg: SinkMsg) -> Result<()> {
        let Some(tx) = self.tx.as_ref() else {
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_RUN_INVARIANT,
                "pipelined sink already finished"
            );
        };
        if tx.send(msg).is_err() {
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_RUN_INVARIANT,
                "pipelined encode worker stopped"
            );
        }
        Ok(())
    }

    /// Close the channel, join the worker, and recover the populated `ExportSink`.
    pub fn finish(mut self) -> Result<ExportSink> {
        self.tx.take();
        let Some(handle) = self.worker.take() else {
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_RUN_INVARIANT,
                "pipelined sink has no worker"
            );
        };
        match handle.join() {
            Ok(result) => result,
            Err(_) => crate::rivet_bail!(
                crate::error::codes::INTERNAL_RUN_INVARIANT,
                "pipelined encode worker panicked"
            ),
        }
    }
}

impl BatchSink for PipelinedSink {
    fn note_nulled(&mut self, column: &str, n: u64) {
        self.nulled_cells.add(column, n);
    }

    fn on_schema(&mut self, schema: SchemaRef) -> Result<()> {
        self.send(SinkMsg::Schema(schema))
    }

    fn set_source_cursor(&mut self, token: String) {
        let _ = self.send(SinkMsg::SourceCursor(token));
    }

    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.send(SinkMsg::Batch(batch.clone()))
    }
}
