//! **Layer: Observability**

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

use super::ipc::{self, ChildEvent};

/// Chunk progress bar shown during chunked export runs.
pub(crate) struct ChunkProgress {
    bar: Arc<ProgressBar>,
    inner: Arc<HandleInner>,
}

/// Handle that worker threads hold.
#[derive(Clone)]
pub(crate) struct ChunkProgressHandle {
    bar: Arc<ProgressBar>,
    inner: Arc<HandleInner>,
}

struct HandleInner {
    export_name: String,
    /// Position counter used as the truth source for the `chunks_done` field on emitted events.
    chunks_done: AtomicU64,
    /// True iff this process is publishing events to a unified UI (either stdout-IPC for parallel-export-processes children, or the in-process channel for `--parallel-exports`).
    capturing: bool,
    started_at: Instant,
}

impl ChunkProgress {
    pub(crate) fn new(export_name: &str, total_chunks: usize) -> Self {
        let capturing = ipc::capturing_events();
        let bar = if capturing {
            ProgressBar::with_draw_target(Some(total_chunks as u64), ProgressDrawTarget::hidden())
        } else {
            let b = ProgressBar::new(total_chunks as u64);
            b.set_style(
                ProgressStyle::with_template(
                    "  {prefix:.bold} [{bar:30.cyan/blue}] {pos}/{len} chunks | {msg} | {elapsed_precise} | ETA {eta}",
                )
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("=>-"),
            );
            b.set_prefix(export_name.to_string());
            b.set_message("0 rows");
            b.enable_steady_tick(Duration::from_millis(200));
            b
        };

        let bar = Arc::new(bar);
        let inner = Arc::new(HandleInner {
            export_name: export_name.to_string(),
            chunks_done: AtomicU64::new(0),
            capturing,
            started_at: Instant::now(),
        });

        if capturing {
            ipc::emit_event(&ChildEvent::ProgressInit {
                export_name: export_name.to_string(),
                total_chunks: total_chunks as u64,
            });
        }

        Self { bar, inner }
    }

    /// Advance by one chunk and update the running row count (single-threaded caller).
    pub(crate) fn inc(&self, total_rows_so_far: i64) {
        self.handle_view().inc(total_rows_so_far);
    }

    /// Cloneable handle for worker threads.
    pub(crate) fn handle(&self) -> ChunkProgressHandle {
        ChunkProgressHandle {
            bar: Arc::clone(&self.bar),
            inner: Arc::clone(&self.inner),
        }
    }

    /// Internal handle without cloning the `Arc` ref — used by `Self::inc`.
    fn handle_view(&self) -> ChunkProgressHandle {
        ChunkProgressHandle {
            bar: Arc::clone(&self.bar),
            inner: Arc::clone(&self.inner),
        }
    }

    pub(crate) fn finish(&self, total_rows: i64) {
        self.bar.set_message(fmt_rows(total_rows));
        self.bar.finish_and_clear();
    }
}

impl Drop for ChunkProgress {
    /// Make sure the underlying indicatif bar is finished even when the owning function bailed before reaching the success path.
    fn drop(&mut self) {
        if !self.bar.is_finished() {
            self.bar.finish_and_clear();
        }
    }
}

impl ChunkProgressHandle {
    /// Advance by one chunk.
    pub(crate) fn inc(&self, total_rows_so_far: i64) {
        self.bar.inc(1);
        self.inner.chunks_done.fetch_add(1, Ordering::Relaxed);
        self.set_rows(total_rows_so_far);
    }

    /// Update the running row count / rate WITHOUT advancing the chunk count.
    pub(crate) fn set_rows(&self, total_rows_so_far: i64) {
        let elapsed = self.inner.started_at.elapsed().as_secs_f64();
        let msg = if elapsed >= 0.5 && total_rows_so_far > 0 {
            let rps = total_rows_so_far as f64 / elapsed;
            format!("{}  {}", fmt_rows(total_rows_so_far), fmt_rate(rps))
        } else {
            fmt_rows(total_rows_so_far)
        };
        self.bar.set_message(msg);
        if self.inner.capturing {
            ipc::emit_event(&ChildEvent::Progress {
                export_name: self.inner.export_name.clone(),
                chunks_done: self.inner.chunks_done.load(Ordering::Relaxed),
                rows: total_rows_so_far,
            });
        }
    }
}

fn fmt_rows(rows: i64) -> String {
    if rows >= 1_000_000 {
        format!("{:.1}M rows", rows as f64 / 1_000_000.0)
    } else if rows >= 1_000 {
        format!("{:.0}K rows", rows as f64 / 1_000.0)
    } else {
        format!("{rows} rows")
    }
}

fn fmt_rate(rps: f64) -> String {
    if rps >= 1_000_000.0 {
        format!("{:.1}M r/s", rps / 1_000_000.0)
    } else if rps >= 1_000.0 {
        format!("{:.1}K r/s", rps / 1_000.0)
    } else {
        format!("{:.0} r/s", rps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_inc_advances_counter() {
        let pb = ChunkProgress::new("orders", 10);
        let h = pb.handle();
        h.inc(100);
        h.inc(250);
        assert_eq!(pb.inner.chunks_done.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn set_rows_updates_rows_without_advancing_the_chunk_counter() {
        let pb = ChunkProgress::new("orders", 10);
        let h = pb.handle();
        h.set_rows(500);
        h.set_rows(1_200);
        assert_eq!(pb.inner.chunks_done.load(Ordering::Relaxed), 0);
        h.inc(1_500);
        assert_eq!(pb.inner.chunks_done.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn fmt_rows_picks_unit() {
        assert_eq!(fmt_rows(0), "0 rows");
        assert_eq!(fmt_rows(500), "500 rows");
        assert_eq!(fmt_rows(1_500), "2K rows");
        assert_eq!(fmt_rows(2_500_000), "2.5M rows");
    }

    #[test]
    fn fmt_rate_picks_unit() {
        assert_eq!(fmt_rate(42.0), "42 r/s");
        assert_eq!(fmt_rate(1_500.0), "1.5K r/s");
        assert_eq!(fmt_rate(2_500_000.0), "2.5M r/s");
    }
}
