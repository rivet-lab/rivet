//! IPC protocol between a parent `rivet run --parallel-export-processes` and its child processes.

use std::sync::Mutex;
use std::sync::mpsc::Sender;

use serde::{Deserialize, Serialize};

use super::parent_ui::UiMessage;

/// Env var that toggles emitter mode in children.
pub const ENV_IPC_EVENTS: &str = "RIVET_IPC_EVENTS";

/// In-process event channel.
pub(crate) static IN_PROCESS_TX: Mutex<Option<Sender<UiMessage>>> = Mutex::new(None);

pub(crate) fn install_in_process_tx(tx: Sender<UiMessage>) {
    *IN_PROCESS_TX.lock().expect("ipc tx mutex poisoned") = Some(tx);
}

pub(crate) fn clear_in_process_tx() {
    *IN_PROCESS_TX.lock().unwrap_or_else(|p| p.into_inner()) = None;
}

/// Hand a formatted `log` line to the in-process renderer while one owns the screen.
pub(crate) fn route_log_line(line: String) -> bool {
    let guard = IN_PROCESS_TX.lock().unwrap_or_else(|p| p.into_inner());
    guard
        .as_ref()
        .is_some_and(|tx| tx.send(UiMessage::Log(line)).is_ok())
}

pub(crate) fn in_process_events_enabled() -> bool {
    IN_PROCESS_TX
        .lock()
        .expect("ipc tx mutex poisoned")
        .is_some()
}

/// True if the current process is publishing events somewhere (stdout IPC for the `--parallel-export-processes` child path or the in-process channel for `--parallel-exports`).
pub(crate) fn capturing_events() -> bool {
    ipc_events_enabled() || in_process_events_enabled()
}

/// One observable event from a child.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChildEvent {
    /// `RunSummary::new` was called — the export is about to start.
    Started {
        export_name: String,
        run_id: String,
        mode: String,
        tuning_profile: String,
        batch_size: usize,
    },
    /// A `ChunkProgress` was created — total chunk count is now known.
    ProgressInit {
        export_name: String,
        total_chunks: u64,
    },
    /// One chunk finished — emit running totals.
    Progress {
        export_name: String,
        chunks_done: u64,
        rows: i64,
    },
    /// Terminal verdict reached.
    Finished {
        export_name: String,
        run_id: String,
        status: String,
        total_rows: i64,
        files_produced: u64,
        bytes_written: u64,
        duration_ms: i64,
        peak_rss_mb: i64,
        error_message: Option<String>,
        /// Transient destination retries the child's RetryLayer absorbed.
        #[serde(default)]
        dest_retries: u64,
        /// Decoded bytes read from the source; `#[serde(default)]` so an older child's event still parses.
        #[serde(default)]
        bytes_read: u64,
        /// The run's mode label; `#[serde(default)]` so an older child's event still parses.
        #[serde(default)]
        mode: String,
    },
}

impl ChildEvent {
    /// Name of the export this event refers to (used by the parent to route the event to the right card).
    #[allow(dead_code)]
    pub fn export_name(&self) -> &str {
        match self {
            ChildEvent::Started { export_name, .. }
            | ChildEvent::ProgressInit { export_name, .. }
            | ChildEvent::Progress { export_name, .. }
            | ChildEvent::Finished { export_name, .. } => export_name,
        }
    }
}

/// `true` iff `RIVET_IPC_EVENTS` is set to a non-empty value (i.e., this process is a child spawned by a parent that wants events).
pub fn ipc_events_enabled() -> bool {
    ipc_events_enabled_from(std::env::var(ENV_IPC_EVENTS).ok().as_deref())
}

/// The decision itself, split from the env read so it can be tested.
pub(crate) fn ipc_events_enabled_from(raw: Option<&str>) -> bool {
    matches!(raw, Some(v) if !v.is_empty())
}

/// Emit one event to stdout as a single JSON line.
fn emit(event: &ChildEvent) {
    use std::io::Write;
    let line = match serde_json::to_string(event) {
        Ok(s) => s,
        Err(e) => {
            log::debug!("ipc: failed to serialize event {:?}: {:#}", event, e);
            return;
        }
    };
    let stdout = std::io::stdout();
    let mut h = stdout.lock();
    if let Err(e) = writeln!(h, "{line}") {
        log::debug!("ipc: failed to write event to stdout: {:#}", e);
        return;
    }
    let _ = h.flush();
}

/// Route an event to whichever sink is active in this process.
pub(crate) fn emit_event(event: &ChildEvent) {
    if ipc_events_enabled() {
        emit(event);
        return;
    }
    if let Ok(guard) = IN_PROCESS_TX.lock()
        && let Some(tx) = guard.as_ref()
    {
        let _ = tx.send(UiMessage::Event(event.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_started() {
        let ev = ChildEvent::Started {
            export_name: "orders".into(),
            run_id: "orders_20260427T120000".into(),
            mode: "chunked".into(),
            tuning_profile: "balanced (default)".into(),
            batch_size: 10_000,
        };
        let json = serde_json::to_string(&ev).unwrap();
        let back: ChildEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(ev, back);
        assert!(json.contains("\"type\":\"started\""));
    }

    #[test]
    fn round_trip_progress() {
        let ev = ChildEvent::Progress {
            export_name: "events".into(),
            chunks_done: 7,
            rows: 1_234_567,
        };
        let s = serde_json::to_string(&ev).unwrap();
        let back: ChildEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn round_trip_finished_with_error() {
        let ev = ChildEvent::Finished {
            export_name: "users".into(),
            run_id: "users_20260427T120000".into(),
            status: "failed".into(),
            total_rows: 0,
            files_produced: 0,
            bytes_written: 0,
            duration_ms: 1234,
            peak_rss_mb: 12,
            error_message: Some("connection reset".into()),
            dest_retries: 3,
            bytes_read: 0,
            mode: "full".into(),
        };
        let s = serde_json::to_string(&ev).unwrap();
        let back: ChildEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn export_name_routing() {
        let ev = ChildEvent::ProgressInit {
            export_name: "orders".into(),
            total_chunks: 20,
        };
        assert_eq!(ev.export_name(), "orders");
    }

    /// An UNSET or EMPTY variable means the in-process UI channel, not the child-event JSON stream.
    #[test]
    fn ipc_events_are_off_unless_the_variable_is_set_and_non_empty() {
        assert!(!ipc_events_enabled_from(None), "unset ⇒ off");
        assert!(!ipc_events_enabled_from(Some("")), "empty ⇒ off");
        assert!(ipc_events_enabled_from(Some("1")), "set ⇒ on");
        assert!(
            ipc_events_enabled_from(Some("0")),
            "any non-empty value is on — the variable is a presence flag, not a \
             boolean, and pinning that stops a future reader from 'fixing' it into \
             a parser that would silently disable an operator's RIVET_IPC_EVENTS=0"
        );
    }
}
