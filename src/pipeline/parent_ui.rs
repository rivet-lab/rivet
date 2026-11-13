//! **Layer: Observability** — parent-side renderer for
//! `rivet run --parallel-export-processes`.
//!
//! Receives [`ipc::ChildEvent`]s from spawned children over an `mpsc` channel
//! and renders one *line* per export directly to stderr.  Per-run metadata
//! that is identical for every export (config path, tuning profile, batch
//! size, `run_id` formats) is intentionally not repeated per row — the run
//! aggregator at end-of-run already prints it once.
//!
//! Running:
//! ```text
//! ▸ orders            chunked   3/20 chunks   300K rows   1m 53.8s  ETA 10m 44.9s
//! ```
//!
//! Finished (success):
//! ```text
//! ✓ orders            chunked   1,000,908 rows  11 files  32.4 MB  1m 44.4s  RSS 50 MB
//! ```
//!
//! Finished (failed):
//! ```text
//! ✗ metric_samples    chunked   chunk checkpoint run 'metric_samples_2026…' still in progress
//! ```
//!
//! Recovery commands for failed exports are NOT inlined here (they would
//! wrap and corrupt the cursor-up math).  The run aggregator prints one
//! consolidated `recovery:` block at the end instead.
//!
//! ## Why a hand-rolled renderer (and not `indicatif`)?
//!
//! An earlier revision used `indicatif::MultiProgress` with one
//! `ProgressBar` per card line (header + 5 meta + progress).  In real ttys
//! the result was correct, but inside `vhs`/`ttyd` (the recording stack we
//! use for documentation GIFs) `indicatif` silently produced no output —
//! `console::Term::stderr().features().is_attended()` returned `true`, so
//! draws should have happened, but apparently no escape sequences ever
//! reached the captured frame.  The behaviour was reproducible across
//! several variants (`new(0)` vs `new_spinner()`, with/without
//! `enable_steady_tick`, `mp.println` vs `set_message`, etc.).
//!
//! Given that we already need to:
//!
//! - keep cards in a stable insertion order,
//! - keep them on screen permanently after the run (so they survive in
//!   scrollback),
//! - update progress / ETA on a fixed cadence even when no IPC event has
//!   arrived,
//!
//! the indicatif coordination buys us very little here.  This module owns
//! the screen for the duration of a `--parallel-export-processes` run,
//! redraws all cards on every event and on a 200 ms idle timer, and never
//! calls `indicatif`'s code paths.  That makes the rendering deterministic
//! across real terminals, recorded `vhs` sessions, and CI pipes (where the
//! ANSI sequences become harmless no-ops because no cursor exists to
//! reposition).

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use super::ipc::ChildEvent;
use super::{clamp_line, format_bytes, strip_chunked_recovery_hint};

pub(crate) use crate::redact::sanitize_terminal;

/// One unit of work for the UI thread.
#[derive(Debug)]
pub(crate) enum UiMessage {
    Event(ChildEvent),
    /// A formatted `log` line from this process while the renderer owns the screen.
    Log(String),
    /// Child's stdout closed without a `Finished` event.
    ChildClosed {
        export_name: String,
        wait_status: ChildWaitStatus,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum ChildWaitStatus {
    Success,
    Failed(String),
}

/// How often the UI redraws when no event arrives (so elapsed-time and ETA keep ticking on idle children).
const IDLE_REDRAW_INTERVAL: Duration = Duration::from_millis(200);

/// Minimum padding for the name column so names stay aligned even when only short ones have been observed so far.
const MIN_NAME_COL: usize = 12;

/// Minimum padding for the mode column.
const MIN_MODE_COL: usize = 7;

/// Drain `rx` until the channel closes, rendering events as multi-line "cards" on stderr.
pub(crate) fn run_ui(rx: Receiver<UiMessage>, name_floor: usize, n_cards: usize) {
    let width = pick_width();
    let term = console::Term::stderr();
    let (rows, _) = term.size();
    if should_render_interactive(term.features().is_attended(), rows as usize, n_cards) {
        run_ui_interactive(rx, width, name_floor);
    } else {
        run_ui_linear(rx, width, name_floor);
    }
}

/// Lines the in-place renderer needs BESIDES the one-line-per-card block.
const RESERVED_LINES: usize = 1;

/// Decide the render mode.
fn should_render_interactive(attended: bool, term_rows: usize, n_cards: usize) -> bool {
    attended && term_rows > n_cards + RESERVED_LINES
}

/// In-place card-stack renderer for tty stderr.
fn run_ui_interactive(rx: Receiver<UiMessage>, width: usize, name_floor: usize) {
    let mut renderer = Renderer::new(width, name_floor);

    loop {
        match rx.recv_timeout(IDLE_REDRAW_INTERVAL) {
            Ok(msg) => renderer.process_message(msg),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        renderer.redraw();
    }

    for card in renderer.cards.values_mut() {
        if !card.finished {
            card.finalize_synthetic("failed", Some("child exited without a final summary"));
        }
    }
    renderer.redraw();
    renderer.commit();
}

/// Linear renderer for non-tty stderr.
fn run_ui_linear(rx: Receiver<UiMessage>, width: usize, name_floor: usize) {
    let mut renderer = Renderer::new(width, name_floor);
    let mut printed: HashSet<String> = HashSet::new();

    while let Ok(msg) = rx.recv() {
        renderer.process_message(msg);
        flush_finished_cards(&renderer, &mut printed, width);
    }

    let pending: Vec<String> = renderer
        .order
        .iter()
        .filter(|n| !printed.contains(n.as_str()))
        .cloned()
        .collect();
    for name in &pending {
        if let Some(card) = renderer.cards.get_mut(name)
            && !card.finished
        {
            card.finalize_synthetic("failed", Some("child exited without a final summary"));
        }
    }
    flush_finished_cards(&renderer, &mut printed, width);
}

/// Append every card that has reached its terminal state but hasn't been printed yet to stderr, preserving the `Started`-event observation order.
fn flush_finished_cards(renderer: &Renderer, printed: &mut HashSet<String>, width: usize) {
    let (name_col, mode_col) = column_widths(&renderer.cards, renderer.name_floor);
    let mut out = String::new();
    for name in &renderer.order {
        if printed.contains(name) {
            continue;
        }
        let Some(card) = renderer.cards.get(name) else {
            continue;
        };
        if !card.finished {
            continue;
        }
        for line in card.live_lines(width, name_col, mode_col) {
            out.push_str(&line);
            out.push('\n');
        }
        printed.insert(name.clone());
    }
    if out.is_empty() {
        return;
    }
    let mut handle = std::io::stderr().lock();
    let _ = handle.write_all(out.as_bytes());
    let _ = handle.flush();
}

/// Pick stable column widths for the name and mode columns by combining the observed maxima with `MIN_NAME_COL` / `MIN_MODE_COL` floors so the layout stays aligned even when only short names / modes have been observed yet.
fn column_widths(cards: &HashMap<String, CardState>, name_floor: usize) -> (usize, usize) {
    let mut name = name_floor;
    let mut mode = 0usize;
    for c in cards.values() {
        name = name.max(c.export_name.chars().count());
        mode = mode.max(c.mode.chars().count());
    }
    (name.max(MIN_NAME_COL), mode.max(MIN_MODE_COL))
}

/// Owns the on-screen state and knows how to repaint it.
struct Renderer {
    cards: HashMap<String, CardState>,
    order: Vec<String>,
    /// Number of lines printed to stderr by the last `redraw()`.
    last_drawn_lines: usize,
    width: usize,
    /// Lower bound on the name column, seeded from the known export names so the table is aligned from the first redraw (not widening as long names arrive).
    name_floor: usize,
}

impl Renderer {
    fn new(width: usize, name_floor: usize) -> Self {
        Self {
            cards: HashMap::new(),
            order: Vec::new(),
            last_drawn_lines: 0,
            width,
            name_floor,
        }
    }

    fn process_message(&mut self, msg: UiMessage) {
        match msg {
            UiMessage::Event(ev) => self.handle_event(ev),
            UiMessage::Log(line) => {
                let out = log_frame(self.last_drawn_lines, &line);
                let mut handle = std::io::stderr().lock();
                let _ = handle.write_all(out.as_bytes());
                let _ = handle.flush();
                self.last_drawn_lines = 0;
            }
            UiMessage::ChildClosed {
                export_name,
                wait_status,
            } => {
                if let Some(card) = self.cards.get_mut(&export_name)
                    && !card.finished
                {
                    let (status, err) = match wait_status {
                        ChildWaitStatus::Success => ("success".to_string(), None),
                        ChildWaitStatus::Failed(msg) => ("failed".to_string(), Some(msg)),
                    };
                    card.finalize_synthetic(&status, err.as_deref());
                }
            }
        }
    }

    fn handle_event(&mut self, ev: ChildEvent) {
        match ev {
            ChildEvent::Started {
                export_name,
                run_id: _,
                mode,
                tuning_profile: _,
                batch_size: _,
            } => {
                if !self.cards.contains_key(&export_name) {
                    self.order.push(export_name.clone());
                }
                let card = CardState {
                    export_name: export_name.clone(),
                    mode,
                    status: "running".into(),
                    chunks_done: 0,
                    total_chunks: 0,
                    rows: 0,
                    started_at: Instant::now(),
                    finished: false,
                    final_line: String::new(),
                };
                self.cards.insert(export_name, card);
            }
            ChildEvent::ProgressInit {
                export_name,
                total_chunks,
            } => {
                if let Some(card) = self.cards.get_mut(&export_name) {
                    card.total_chunks = total_chunks;
                }
            }
            ChildEvent::Progress {
                export_name,
                chunks_done,
                rows,
            } => {
                if let Some(card) = self.cards.get_mut(&export_name) {
                    card.chunks_done = chunks_done;
                    card.rows = rows;
                }
            }
            ChildEvent::Finished {
                export_name,
                run_id: _,
                status,
                total_rows,
                files_produced,
                bytes_written,
                duration_ms,
                peak_rss_mb,
                error_message,
                dest_retries: _,
                bytes_read: _,
                mode: _,
            } => {
                if let Some(card) = self.cards.get_mut(&export_name) {
                    card.finalize(
                        &status,
                        total_rows,
                        files_produced,
                        bytes_written,
                        duration_ms,
                        peak_rss_mb,
                        error_message.as_deref(),
                    );
                }
            }
        }
    }

    /// Repaint the card stack on stderr.
    fn redraw(&mut self) {
        let mut out = String::new();
        if self.last_drawn_lines > 0 {
            out.push_str(&format!("\x1b[{}A", self.last_drawn_lines));
            out.push('\r');
        }

        let (name_col, mode_col) = column_widths(&self.cards, self.name_floor);
        let mut new_lines = 0usize;
        for name in &self.order {
            if let Some(card) = self.cards.get(name) {
                for line in card.live_lines(self.width, name_col, mode_col) {
                    out.push_str("\x1b[2K");
                    out.push_str(&line);
                    out.push('\n');
                    new_lines += 1;
                }
            }
        }

        if self.last_drawn_lines > new_lines {
            let extra = self.last_drawn_lines - new_lines;
            for _ in 0..extra {
                out.push_str("\x1b[2K\n");
            }
            out.push_str(&format!("\x1b[{extra}A"));
        }

        let mut handle = std::io::stderr().lock();
        let _ = handle.write_all(out.as_bytes());
        let _ = handle.flush();

        self.last_drawn_lines = new_lines;
    }

    /// Lock the rendered cards into scrollback.
    fn commit(&mut self) {
        self.last_drawn_lines = 0;
    }
}

/// Pure data for one export's card.
struct CardState {
    export_name: String,
    mode: String,
    status: String,
    chunks_done: u64,
    total_chunks: u64,
    rows: i64,
    started_at: Instant,
    finished: bool,
    final_line: String,
}

impl CardState {
    #[allow(clippy::too_many_arguments)]
    fn finalize(
        &mut self,
        status: &str,
        total_rows: i64,
        files_produced: u64,
        bytes_written: u64,
        duration_ms: i64,
        peak_rss_mb: i64,
        error_message: Option<&str>,
    ) {
        self.finished = true;
        self.status = status.to_string();
        self.rows = total_rows;
        self.final_line = render_final_line(
            total_rows,
            files_produced,
            bytes_written,
            duration_ms,
            peak_rss_mb,
            error_message,
        );
    }

    fn finalize_synthetic(&mut self, status: &str, error_message: Option<&str>) {
        self.finished = true;
        self.status = format!("synthetic-{}", status);
        self.final_line = match error_message {
            Some(msg) => sanitize_terminal(msg),
            None => "child exited without a final summary".to_string(),
        };
    }

    /// Lines this card contributes to the current frame.
    fn live_lines(&self, width: usize, name_col: usize, mode_col: usize) -> Vec<String> {
        vec![clamp_line(&self.compact_line(name_col, mode_col), width)]
    }

    /// Render the single per-export status line.
    fn compact_line(&self, name_col: usize, mode_col: usize) -> String {
        let icon = self.status_icon();
        let body = if self.finished {
            self.final_line.clone()
        } else {
            self.running_body()
        };
        format!(
            "{} {:<name$}  {:<mode$}  {}",
            icon,
            self.export_name,
            self.mode,
            body,
            name = name_col,
            mode = mode_col,
        )
    }

    fn status_icon(&self) -> &'static str {
        if !self.finished {
            return "▸";
        }
        match self.status.as_str() {
            "success" => "✓",
            "failed" => "✗",
            s if s.starts_with("synthetic-") => "⚠",
            _ => "•",
        }
    }

    /// Body used while the export is still running.
    fn running_body(&self) -> String {
        let elapsed_ms = self.started_at.elapsed().as_millis() as i64;
        let chunks_label = if self.total_chunks > 0 {
            format!("{}/{} chunks", self.chunks_done, self.total_chunks)
        } else {
            "preparing…".to_string()
        };
        let eta_label = if self.chunks_done > 0 && self.total_chunks > self.chunks_done {
            let total_ms_est =
                elapsed_ms as f64 * (self.total_chunks as f64 / self.chunks_done as f64);
            let remaining = (total_ms_est - elapsed_ms as f64).max(0.0) as i64;
            fmt_duration_ms(remaining)
        } else if self.total_chunks > 0 && self.chunks_done >= self.total_chunks {
            "0s".to_string()
        } else {
            "—".to_string()
        };
        let rate_label = if elapsed_ms >= 500 && self.rows > 0 {
            let rps = self.rows as f64 * 1000.0 / elapsed_ms as f64;
            format!("  {}", fmt_rate(rps))
        } else {
            String::new()
        };
        format!(
            "{chunks}   {rows}{rate}   {elapsed}  ETA {eta}",
            chunks = chunks_label,
            rows = fmt_rows(self.rows),
            rate = rate_label,
            elapsed = fmt_duration_ms(elapsed_ms),
            eta = eta_label,
        )
    }
}

fn render_final_line(
    total_rows: i64,
    files_produced: u64,
    bytes_written: u64,
    duration_ms: i64,
    peak_rss_mb: i64,
    error_message: Option<&str>,
) -> String {
    if let Some(err) = error_message {
        let (cause, _) = strip_chunked_recovery_hint(err);
        return sanitize_terminal(cause);
    }
    let rss = if peak_rss_mb > 0 {
        format!("  RSS {:>3} MB", fmt_thousands(peak_rss_mb))
    } else {
        String::new()
    };
    format!(
        "{:>11} rows  {:>3} files  {:>9}  {:>8}{}",
        fmt_thousands(total_rows),
        fmt_thousands(files_produced as i64),
        format_bytes(bytes_written),
        fmt_duration_ms(duration_ms),
        rss
    )
}

/// The bytes that put one log line ABOVE a card block of `last_drawn_lines` lines.
fn log_frame(last_drawn_lines: usize, line: &str) -> String {
    let mut out = String::new();
    if last_drawn_lines > 0 {
        out.push_str(&format!("\x1b[{last_drawn_lines}A\r\x1b[2K"));
    }
    out.push_str(&sanitize_terminal(line));
    out.push('\n');
    out
}

fn pick_width() -> usize {
    let (_, cols) = console::Term::stderr().size();
    (cols as usize).clamp(60, 100)
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

fn fmt_thousands(n: i64) -> String {
    let abs = n.unsigned_abs();
    let raw = abs.to_string();
    let mut buf = String::with_capacity(raw.len() + raw.len() / 3);
    for (from_end, ch) in raw.chars().rev().enumerate() {
        if from_end > 0 && from_end.is_multiple_of(3) {
            buf.push(',');
        }
        buf.push(ch);
    }
    let s: String = buf.chars().rev().collect();
    if n < 0 { format!("-{s}") } else { s }
}

fn fmt_duration_ms(ms: i64) -> String {
    if ms < 0 {
        return "0s".into();
    }
    let total_secs_f = ms as f64 / 1000.0;
    if ms < 60_000 {
        return format!("{:.1}s", total_secs_f);
    }
    let mut secs = total_secs_f as i64;
    let hours = secs / 3600;
    secs %= 3600;
    let mins = secs / 60;
    let rem_secs = total_secs_f - (hours as f64 * 3600.0) - (mins as f64 * 60.0);
    if hours > 0 {
        format!("{}h {:02}m {:04.1}s", hours, mins, rem_secs)
    } else {
        format!("{}m {:04.1}s", mins, rem_secs)
    }
}

/// Serialises the tests that assert on the process-wide `destination::TRANSIENT_RETRIES` counter by DELTA.
#[cfg(test)]
pub(crate) fn retry_counter_test_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    &LOCK
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `log` line during an interactive run lands ABOVE the block (cursor up to the anchor, print, newline) and resets the anchor, so the next redraw repaints below it instead of walking up past a line it did not draw.
    #[test]
    fn a_log_line_is_printed_above_the_block_and_resets_the_anchor() {
        assert_eq!(
            log_frame(3, "[WARN rivet] slow"),
            "\x1b[3A\r\x1b[2K[WARN rivet] slow\n"
        );
        assert_eq!(
            log_frame(0, "x"),
            "x\n",
            "no block (linear renderer, or before the first frame): the bare line, no escape bytes"
        );
        assert_eq!(
            log_frame(3, "y"),
            "\x1b[3A\r\x1b[2Ky\n",
            "over a block: walk up, erase, print"
        );
        assert!(
            !log_frame(0, "\x1b]0;evil\x07").contains("\x1b]0;"),
            "control bytes inside the line are neutralised before they reach the tty"
        );
        let mut r = Renderer::new(80, 4);
        r.last_drawn_lines = 3;
        r.process_message(UiMessage::Log("x".into()));
        assert_eq!(
            r.last_drawn_lines, 0,
            "the anchor is reset for the next redraw"
        );
    }

    /// The renderer must NOT fold a retry counter that arrived over the IN-PROCESS channel.
    #[test]
    fn in_process_finished_event_must_not_refold_this_processs_own_retry_counter() {
        let _serial = super::retry_counter_test_lock()
            .lock()
            .unwrap_or_else(|p| p.into_inner());

        const SEED: u64 = 1_000_000;
        crate::destination::add_transient_retries(SEED);

        let (tx, rx) = std::sync::mpsc::channel::<UiMessage>();
        super::super::ipc::install_in_process_tx(tx);
        for name in ["orders", "events"] {
            let s = crate::pipeline::summary::RunSummary::stub_for_testing("run-refold", name);
            s.print();
        }
        super::super::ipc::clear_in_process_tx();

        let before_drain = crate::destination::transient_retries_total();
        let mut renderer = Renderer::new(80, 8);
        let mut finished = 0usize;
        while let Ok(msg) = rx.recv() {
            if let UiMessage::Event(ChildEvent::Finished { dest_retries, .. }) = &msg {
                assert!(
                    *dest_retries >= SEED,
                    "in-process Finished must carry the process total, got {dest_retries}"
                );
                finished += 1;
            }
            renderer.process_message(msg);
        }
        assert_eq!(finished, 2, "both exports must have emitted a Finished");

        let grew_by = crate::destination::transient_retries_total() - before_drain;
        assert!(
            grew_by < 1_000,
            "rendering the process's OWN events must not fold their counter back \
             into it (grew by {grew_by}; the mutant folds >= {} per event)",
            SEED
        );
    }

    /// A child's `Started` opens its card and its `Finished` closes it with the child's counts.
    #[test]
    fn a_childs_started_and_finished_events_open_and_close_its_card() {
        let mut r = Renderer::new(80, 8);
        r.process_message(UiMessage::Event(ChildEvent::Started {
            export_name: "orders".into(),
            run_id: "orders_run".into(),
            mode: "full".into(),
            tuning_profile: "balanced".into(),
            batch_size: 10,
        }));
        assert!(!r.cards["orders"].finished, "Started opens a running card");
        r.process_message(UiMessage::Event(ChildEvent::Finished {
            export_name: "orders".into(),
            run_id: "orders_run".into(),
            status: "success".into(),
            total_rows: 42,
            files_produced: 1,
            bytes_written: 100,
            duration_ms: 5,
            peak_rss_mb: 1,
            error_message: None,
            dest_retries: 0,
            bytes_read: 200,
            mode: "full".into(),
        }));
        let card = &r.cards["orders"];
        assert_eq!(
            (card.finished, card.status.as_str(), card.rows),
            (true, "success", 42)
        );
    }

    fn fresh_card(name: &str, mode: &str) -> CardState {
        CardState {
            export_name: name.into(),
            mode: mode.into(),
            status: "running".into(),
            chunks_done: 0,
            total_chunks: 0,
            rows: 0,
            started_at: Instant::now(),
            finished: false,
            final_line: String::new(),
        }
    }

    #[test]
    fn final_line_includes_metrics() {
        let s = render_final_line(123_456, 5, 1024 * 1024, 9_400, 30, None);
        assert!(s.contains("123,456 rows"));
        assert!(s.contains("5 files"));
        assert!(s.contains("9.4s"));
        assert!(s.contains("RSS") && s.contains("30 MB"));
    }

    #[test]
    fn final_line_uses_error_message() {
        let s = render_final_line(0, 0, 0, 0, 0, Some("connection reset"));
        assert_eq!(s, "connection reset");
    }

    #[test]
    fn interactive_only_when_the_card_block_fits_the_viewport() {
        assert!(should_render_interactive(true, 50, 10));
        assert!(!should_render_interactive(true, 24, 30));
        assert!(!should_render_interactive(true, 11, 10));
        assert!(should_render_interactive(true, 12, 10));
        assert!(!should_render_interactive(false, 100, 1));
        assert!(!should_render_interactive(true, 0, 1));
    }

    #[test]
    fn final_line_strips_chunked_recovery_hint() {
        let s = render_final_line(
            0,
            0,
            0,
            0,
            0,
            Some(
                "export 'x': chunk checkpoint run 'rid' still in progress; \
                  use `rivet run --config foo.yaml --export x --resume` or \
                  `rivet state reset-chunks --config foo.yaml --export x`",
            ),
        );
        assert_eq!(
            s,
            "export 'x': chunk checkpoint run 'rid' still in progress"
        );
    }

    #[test]
    fn pick_width_in_bounds() {
        let w = pick_width();
        assert!((60..=100).contains(&w));
    }

    #[test]
    fn fmt_rows_buckets() {
        assert_eq!(fmt_rows(0), "0 rows");
        assert_eq!(fmt_rows(999), "999 rows");
        assert_eq!(fmt_rows(1_500), "2K rows");
        assert_eq!(fmt_rows(1_500_000), "1.5M rows");
    }

    #[test]
    fn fmt_thousands_basic() {
        assert_eq!(fmt_thousands(0), "0");
        assert_eq!(fmt_thousands(1234), "1,234");
        assert_eq!(fmt_thousands(-1234), "-1,234");
        assert_eq!(fmt_thousands(1_000_000), "1,000,000");
    }

    #[test]
    fn fmt_duration_buckets() {
        assert_eq!(fmt_duration_ms(0), "0.0s");
        assert_eq!(fmt_duration_ms(9_400), "9.4s");
        assert!(fmt_duration_ms(60_000).starts_with("1m"));
        assert!(fmt_duration_ms(3_600_000).starts_with("1h"));
    }

    #[test]
    fn fmt_rate_picks_unit() {
        assert_eq!(fmt_rate(42.0), "42 r/s");
        assert_eq!(fmt_rate(1_500.0), "1.5K r/s");
        assert_eq!(fmt_rate(2_500_000.0), "2.5M r/s");
    }

    #[test]
    fn running_body_shows_rate_after_warmup() {
        let mut card = fresh_card("orders", "chunked");
        card.total_chunks = 10;
        card.chunks_done = 5;
        card.rows = 50_000;
        let line = card.running_body();
        assert!(line.contains("50K rows"));
        assert!(!line.contains("r/s"), "rate shown too early: {line}");
    }

    #[test]
    fn compact_running_card_shows_progress_or_preparing() {
        let mut card = fresh_card("orders", "chunked");
        let line = card.compact_line(MIN_NAME_COL, MIN_MODE_COL);
        assert!(line.starts_with("▸ "), "running uses ▸: {}", line);
        assert!(line.contains("orders"));
        assert!(line.contains("chunked"));
        assert!(line.contains("preparing"));

        card.total_chunks = 10;
        card.chunks_done = 4;
        card.rows = 40_000;
        let line = card.compact_line(MIN_NAME_COL, MIN_MODE_COL);
        assert!(line.contains("4/10 chunks"));
        assert!(line.contains("40K rows"));
        assert!(line.contains("ETA "));
    }

    #[test]
    fn compact_finished_card_shows_metrics_with_check() {
        let mut card = fresh_card("orders", "chunked");
        card.finalize("success", 100, 1, 1024, 1234, 30, None);
        let line = card.compact_line(MIN_NAME_COL, MIN_MODE_COL);
        assert!(line.starts_with("✓ "), "success uses ✓: {}", line);
        assert!(line.contains("100 rows"));
        assert!(line.contains("1 files"));
        assert!(line.contains("1.2s"));
        assert!(line.contains("RSS") && line.contains("30 MB"));
    }

    #[test]
    fn compact_failed_card_shows_cause_only() {
        let mut card = fresh_card("metric_samples", "chunked");
        card.finalize(
            "failed",
            0,
            0,
            0,
            0,
            0,
            Some(
                "export 'metric_samples': chunk checkpoint run 'rid' still in progress; \
                  use `rivet run --config foo.yaml --export metric_samples --resume` or \
                  `rivet state reset-chunks --config foo.yaml --export metric_samples`",
            ),
        );
        let line = card.compact_line(MIN_NAME_COL, MIN_MODE_COL);
        assert!(line.starts_with("✗ "), "failed uses ✗: {}", line);
        assert!(line.contains("still in progress"));
        assert!(!line.contains("`rivet "), "hint must be stripped: {}", line);
    }

    #[test]
    fn compact_synthetic_failure_uses_warning_glyph() {
        let mut card = fresh_card("orders", "chunked");
        card.finalize_synthetic("failed", Some("child crashed"));
        let line = card.compact_line(MIN_NAME_COL, MIN_MODE_COL);
        assert!(card.finished);
        assert!(line.starts_with("⚠ "), "synthetic uses ⚠: {}", line);
        assert!(line.contains("child crashed"));
    }

    #[test]
    fn compact_line_pads_columns_for_alignment() {
        let mut a = fresh_card("a", "full");
        a.finalize("success", 1, 1, 0, 1000, 0, None);
        let mut b = fresh_card("longer_name", "incremental");
        b.finalize("success", 1, 1, 0, 1000, 0, None);
        let name_col = "longer_name".chars().count();
        let mode_col = "incremental".chars().count();
        let la = a.compact_line(name_col, mode_col);
        let lb = b.compact_line(name_col, mode_col);
        let pa = la.find(" rows").expect("body present in a");
        let pb = lb.find(" rows").expect("body present in b");
        assert_eq!(pa, pb, "alignment broken: a={la:?} b={lb:?}");
    }

    /// Dangerous terminal control characters that must not survive sanitisation, mirroring `sanitize_terminal`'s contract.
    fn dangerous_control_bytes(s: &str) -> Vec<char> {
        s.chars()
            .filter(|&c| {
                let cp = c as u32;
                (cp <= 0x1f && c != '\t' && c != '\n') || cp == 0x7f || (0x80..=0x9f).contains(&cp)
            })
            .collect()
    }

    #[test]
    fn sanitize_terminal_escapes_ansi_and_osc_payload() {
        let payload = "\u{1b}]0;pwned\u{07}\u{1b}[2Jboom";
        let clean = sanitize_terminal(payload);
        assert!(
            dangerous_control_bytes(&clean).is_empty(),
            "escape bytes survived: {clean:?}"
        );
        assert!(clean.contains("pwned"));
        assert!(clean.contains("boom"));
    }

    #[test]
    fn sanitize_terminal_preserves_printable_and_layout_whitespace() {
        let s = "ok\tline1\nline2 ✓ … ──";
        assert_eq!(sanitize_terminal(s), s);
    }

    #[test]
    fn sanitize_terminal_strips_del_c1_and_cr() {
        let s = "a\u{7f}b\u{85}c\rd";
        let clean = sanitize_terminal(s);
        assert!(dangerous_control_bytes(&clean).is_empty());
        assert!(clean.contains('a') && clean.contains('d'));
    }

    #[test]
    fn render_final_line_sanitises_db_error_escapes() {
        let err = "db error: invalid input: \u{1b}]0;pwned\u{07}\u{1b}[2Jboom";
        let line = render_final_line(0, 0, 0, 0, 0, Some(err));
        assert!(
            dangerous_control_bytes(&line).is_empty(),
            "render_final_line leaked control bytes: {line:?}"
        );
    }

    #[test]
    fn finalize_synthetic_sanitises_escapes() {
        let mut card = fresh_card("orders", "chunked");
        card.finalize_synthetic("failed", Some("crash \u{1b}[2J\u{07}"));
        let line = card.compact_line(MIN_NAME_COL, MIN_MODE_COL);
        assert!(
            dangerous_control_bytes(&line).is_empty(),
            "synthetic final line leaked control bytes: {line:?}"
        );
    }
}
