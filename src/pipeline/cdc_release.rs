//! What a stream's first CDC run created is released when that run ends in a stop.

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::source::cdc::{CdcEngine, Position};
use crate::state::{ProgressKey, StateStore};

/// The server-side slot a first run may create, and how to ask about and remove it.
pub(super) trait SlotServer {
    /// Whether the slot exists on the server.
    fn exists(&self, slot: &str) -> Result<bool>;
    /// Drop the slot.
    fn drop_slot(&self, slot: &str) -> Result<()>;
}

/// The PostgreSQL server of a run's source.
pub(super) struct PgSlots<'a> {
    url: crate::secret::Secret,
    tls: Option<&'a crate::config::TlsConfig>,
}

impl SlotServer for PgSlots<'_> {
    fn exists(&self, slot: &str) -> Result<bool> {
        crate::source::postgres::cdc::slot_exists(self.url.expose(), slot, self.tls)
    }

    fn drop_slot(&self, slot: &str) -> Result<()> {
        crate::source::postgres::cdc::drop_slot(self.url.expose(), slot, self.tls)
    }
}

/// A stream's anchors proven absent from the stored record and the server before its first run.
pub(super) struct FirstRun {
    slot: Option<String>,
    checkpoint: Option<std::path::PathBuf>,
    key: ProgressKey,
    destination: String,
    export: String,
}

impl FirstRun {
    /// The proof that `export` has never run: no record, no delivery, no checkpoint file, no slot; `None` when any is found or cannot be read.
    pub(super) fn of(
        config: &Config,
        export: &ExportConfig,
        state: &StateStore,
        config_path: &str,
    ) -> Option<Self> {
        let server = match CdcEngine::from(config.source.source_type) {
            CdcEngine::Postgres => Some(PgSlots {
                url: config.source.resolve_url().ok()?,
                tls: config.source.tls.as_ref(),
            }),
            _ => None,
        };
        Self::proven(
            config,
            export,
            state,
            config_path,
            server.as_ref().map(|s| s as &dyn SlotServer),
        )
    }

    /// [`FirstRun::of`] against `server` for the slot.
    fn proven(
        config: &Config,
        export: &ExportConfig,
        state: &StateStore,
        config_path: &str,
        server: Option<&dyn SlotServer>,
    ) -> Option<Self> {
        let cdc = export.cdc.as_ref()?;
        let key = ProgressKey::cdc(
            &export.name,
            &config.source.state_key(),
            &crate::config::cdc_captured_tables(export),
        );
        let destination = export.destination.state_key();
        if state.captured_tables(&key, &destination).ok()?.is_some()
            || state.cdc_delivery_recorded(&export.name).ok()?
        {
            return None;
        }
        let checkpoint = match cdc.checkpoint.as_deref() {
            Some(raw) => {
                let dir = std::path::Path::new(config_path)
                    .parent()
                    .unwrap_or_else(|| std::path::Path::new("."));
                let path = crate::source::cdc::resolve_checkpoint(raw, dir);
                if Position::load(&path).ok()?.is_some() {
                    return None;
                }
                Some(path)
            }
            None => None,
        };
        let slot = match server {
            Some(server) => {
                let slot = cdc
                    .slot
                    .clone()
                    .unwrap_or_else(|| crate::config::DEFAULT_PG_SLOT.to_string());
                if server.exists(&slot).ok()? {
                    return None;
                }
                Some(slot)
            }
            None => None,
        };
        Some(Self {
            slot,
            checkpoint,
            key,
            destination,
            export: export.name.clone(),
        })
    }

    /// When `outcome` is a stop and the run delivered nothing, drop the slot, delete the checkpoint and forget the capture this run created.
    pub(super) fn release_after(self, config: &Config, state: &StateStore, outcome: &Result<()>) {
        let server = match (&self.slot, CdcEngine::from(config.source.source_type)) {
            (Some(_), CdcEngine::Postgres) => config.source.resolve_url().ok().map(|url| PgSlots {
                url,
                tls: config.source.tls.as_ref(),
            }),
            _ => None,
        };
        self.release(
            state,
            outcome,
            server.as_ref().map(|s| s as &dyn SlotServer),
        );
    }

    /// [`FirstRun::release_after`] against `server` for the slot.
    fn release(self, state: &StateStore, outcome: &Result<()>, server: Option<&dyn SlotServer>) {
        if outcome.is_ok() {
            return;
        }
        match state.cdc_delivery_recorded(&self.export) {
            Ok(false) => {}
            Ok(true) => return,
            Err(e) => {
                not_released(
                    &self.export,
                    "anchor",
                    &e,
                    "re-run the export once the state DB is readable",
                );
                return;
            }
        }
        if let (Some(slot), Some(server)) = (&self.slot, server)
            && let Err(e) = server.exists(slot).and_then(|there| match there {
                true => server.drop_slot(slot),
                false => Ok(()),
            })
        {
            let remedy = format!("run `SELECT pg_drop_replication_slot('{slot}')` on the source");
            not_released(
                &self.export,
                &format!("replication slot '{slot}'"),
                &e,
                &remedy,
            );
        }
        if let Some(path) = &self.checkpoint
            && let Err(e) = remove_file(path)
        {
            let remedy = format!("delete the file '{}'", path.display());
            not_released(
                &self.export,
                &format!("checkpoint '{}'", path.display()),
                &e,
                &remedy,
            );
        }
        if let Err(e) = state.forget_captured_tables(&self.key, &self.destination) {
            let remedy = format!(
                "delete the `export_state` row of export '{}' from the state DB",
                self.export
            );
            not_released(&self.export, "capture record", &e, &remedy);
        }
    }
}

/// Remove `path`; an absent file is released already.
fn remove_file(path: &std::path::Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

/// The warning for a part of a first run's anchor that stayed, with the remedy that removes it by hand.
fn not_released(export: &str, what: &str, cause: &anyhow::Error, remedy: &str) {
    log::warn!("{}", not_released_message(export, what, cause, remedy));
}

/// The text of [`not_released`].
fn not_released_message(export: &str, what: &str, cause: &anyhow::Error, remedy: &str) -> String {
    format!(
        "[{}] cdc: export '{export}' stopped on its first run and its {what} could not be released: {cause:#}; {remedy}, or leave it: the next run of the export reuses it",
        crate::error::codes::SOURCE_CDC_ANCHOR_NOT_RELEASED.id
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    struct FakeSlots {
        held: RefCell<Vec<String>>,
        drops: RefCell<Vec<String>>,
        broken: bool,
    }

    impl FakeSlots {
        fn holding(slots: &[&str]) -> Self {
            Self {
                held: RefCell::new(slots.iter().map(|s| s.to_string()).collect()),
                drops: RefCell::new(Vec::new()),
                broken: false,
            }
        }
    }

    impl SlotServer for FakeSlots {
        fn exists(&self, slot: &str) -> Result<bool> {
            match self.broken {
                true => Err(anyhow::anyhow!("server unreachable")),
                false => Ok(self.held.borrow().iter().any(|s| s == slot)),
            }
        }

        fn drop_slot(&self, slot: &str) -> Result<()> {
            match self.broken {
                true => Err(anyhow::anyhow!("slot is active")),
                false => {
                    self.held.borrow_mut().retain(|s| s != slot);
                    self.drops.borrow_mut().push(slot.to_string());
                    Ok(())
                }
            }
        }
    }

    const PG_SOURCE: &str = "type: postgres\n  url: \"postgres://u:p@localhost/app\"";
    const PG_DOWN_SOURCE: &str = "type: postgres\n  url: \"postgres://u:p@127.0.0.1:1/app\"";
    const MYSQL_SOURCE: &str = "type: mysql\n  url: \"mysql://u:p@localhost/app\"";

    /// A `mode: cdc` config in `dir` with checkpoint `./cdc/o.ckpt` and slot `s_o`; its path, config and export.
    fn config_in(dir: &std::path::Path) -> (String, Config, ExportConfig) {
        config_with(dir, "o", PG_SOURCE)
    }

    /// [`config_in`] for the export `name` over the `source:` body `source`.
    fn config_with(
        dir: &std::path::Path,
        name: &str,
        source: &str,
    ) -> (String, Config, ExportConfig) {
        let yaml = format!(
            "source:\n  {source}\nexports:\n  - name: {name}\n    table: orders\n    mode: cdc\n    \
             format: parquet\n    cdc:\n      slot: s_o\n      checkpoint: ./cdc/o.ckpt\n    \
             destination: {{ type: local, path: ./out }}\n"
        );
        let path = dir.join("rivet.yaml");
        std::fs::write(&path, yaml).unwrap();
        let path = path.display().to_string();
        let mut config = Config::load(&path).unwrap();
        let export = config.exports.remove(0);
        (path, config, export)
    }

    fn write_checkpoint(dir: &std::path::Path) {
        Position::new(serde_json::json!({"file": "bin.000001", "pos": 4}))
            .save(&dir.join("cdc/o.ckpt"))
            .unwrap();
    }

    fn key_of(config: &Config, export: &ExportConfig) -> ProgressKey {
        ProgressKey::cdc(
            &export.name,
            &config.source.state_key(),
            &crate::config::cdc_captured_tables(export),
        )
    }

    fn stopped() -> Result<()> {
        Err(anyhow::anyhow!("refused"))
    }

    /// An export nothing has touched is proven a first run, with its slot and checkpoint path.
    #[test]
    fn an_untouched_export_is_proven_a_first_run() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = config_in(dir.path());
        let state = StateStore::open_in_memory().unwrap();
        let slots = FakeSlots::holding(&["another"]);
        let first = FirstRun::proven(&config, &export, &state, &path, Some(&slots)).unwrap();
        assert_eq!(first.slot.as_deref(), Some("s_o"));
        assert_eq!(first.checkpoint, Some(dir.path().join("./cdc/o.ckpt")));
        assert_eq!(first.export, "o");
    }

    /// Each stored trace of an earlier run withholds the proof.
    #[test]
    fn every_trace_of_an_earlier_run_withholds_the_proof() {
        let slots = FakeSlots::holding(&[]);
        let prove = |setup: &dyn Fn(&std::path::Path, &Config, &ExportConfig, &StateStore)| {
            let dir = tempfile::tempdir().unwrap();
            let (path, config, export) = config_in(dir.path());
            let state = StateStore::open_in_memory().unwrap();
            setup(dir.path(), &config, &export, &state);
            FirstRun::proven(&config, &export, &state, &path, Some(&slots)).is_some()
        };
        assert!(prove(&|_, _, _, _| {}));
        assert!(!prove(&|_, c, e, s| {
            s.record_captured_tables(&key_of(c, e), &e.destination.state_key())
                .unwrap();
        }));
        assert!(!prove(&|_, _, _, s| {
            s.mark_snapshot_done("o", "orders", "out/snapshot", "r1")
                .unwrap();
        }));
        assert!(!prove(&|_, _, _, s| {
            s.record_file(crate::state::FilePart {
                run_id: "r1",
                export_name: "o",
                file_name: "cdc-0",
                rows: 1,
                bytes: 1,
                format: "parquet",
                compression: None,
                cursor_high: None,
            })
            .unwrap();
        }));
        assert!(!prove(&|d, _, _, _| write_checkpoint(d)));
    }

    /// A slot that exists, a probe that fails and a checkpoint that cannot be read all withhold the proof.
    #[test]
    fn a_slot_that_exists_or_cannot_be_probed_withholds_the_proof() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = config_in(dir.path());
        let state = StateStore::open_in_memory().unwrap();
        let held = FakeSlots::holding(&["s_o"]);
        assert!(FirstRun::proven(&config, &export, &state, &path, Some(&held)).is_none());
        let broken = FakeSlots {
            broken: true,
            ..FakeSlots::holding(&[])
        };
        assert!(FirstRun::proven(&config, &export, &state, &path, Some(&broken)).is_none());
        std::fs::create_dir_all(dir.path().join("cdc")).unwrap();
        std::fs::write(dir.path().join("cdc/o.ckpt"), "{not json").unwrap();
        let none = FakeSlots::holding(&[]);
        assert!(FirstRun::proven(&config, &export, &state, &path, Some(&none)).is_none());
    }

    /// A stop on a first run drops the slot, deletes the checkpoint and forgets the capture; an `Ok` run keeps all three.
    #[test]
    fn a_stop_on_a_first_run_releases_what_the_run_created_and_a_success_keeps_it() {
        for outcome_is_stop in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let (path, config, export) = config_in(dir.path());
            let state = StateStore::open_in_memory().unwrap();
            let slots = FakeSlots::holding(&[]);
            let first = FirstRun::proven(&config, &export, &state, &path, Some(&slots)).unwrap();
            slots.held.borrow_mut().push("s_o".into());
            write_checkpoint(dir.path());
            let key = key_of(&config, &export);
            let dest = export.destination.state_key();
            state.record_captured_tables(&key, &dest).unwrap();
            let outcome = if outcome_is_stop { stopped() } else { Ok(()) };
            first.release(&state, &outcome, Some(&slots));
            let kept = !outcome_is_stop;
            assert_eq!(slots.exists("s_o").unwrap(), kept);
            assert_eq!(dir.path().join("cdc/o.ckpt").exists(), kept);
            assert_eq!(state.captured_tables(&key, &dest).unwrap().is_some(), kept);
        }
    }

    /// A stop after a baseline or a durable part keeps the anchor those deliveries are tied to.
    #[test]
    fn a_stop_after_a_delivery_keeps_the_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = config_in(dir.path());
        let state = StateStore::open_in_memory().unwrap();
        let slots = FakeSlots::holding(&[]);
        let first = FirstRun::proven(&config, &export, &state, &path, Some(&slots)).unwrap();
        slots.held.borrow_mut().push("s_o".into());
        write_checkpoint(dir.path());
        state
            .mark_snapshot_done("o", "orders", "out/snapshot", "r1")
            .unwrap();
        first.release(&state, &stopped(), Some(&slots));
        assert!(slots.exists("s_o").unwrap());
        assert!(dir.path().join("cdc/o.ckpt").exists());
        assert!(slots.drops.borrow().is_empty());
    }

    /// A slot that cannot be dropped does not stop the checkpoint and the capture record from being released.
    #[test]
    fn a_slot_that_cannot_be_dropped_leaves_the_rest_released() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = config_in(dir.path());
        let state = StateStore::open_in_memory().unwrap();
        let first = FirstRun::proven(
            &config,
            &export,
            &state,
            &path,
            Some(&FakeSlots::holding(&[])),
        )
        .unwrap();
        write_checkpoint(dir.path());
        let key = key_of(&config, &export);
        let dest = export.destination.state_key();
        state.record_captured_tables(&key, &dest).unwrap();
        let broken = FakeSlots {
            broken: true,
            ..FakeSlots::holding(&["s_o"])
        };
        first.release(&state, &stopped(), Some(&broken));
        assert!(!dir.path().join("cdc/o.ckpt").exists());
        assert!(state.captured_tables(&key, &dest).unwrap().is_none());
    }

    /// The warning names the export, the part that stayed, its cause, and the remedy.
    #[test]
    fn the_warning_for_a_part_that_stayed_names_its_code_cause_and_remedy() {
        assert_eq!(
            not_released_message(
                "o",
                "replication slot 's_o'",
                &anyhow::anyhow!("slot is active"),
                "run `SELECT pg_drop_replication_slot('s_o')` on the source"
            ),
            "[RIVET_SOURCE_CDC_ANCHOR_NOT_RELEASED] cdc: export 'o' stopped on its first run and its replication slot 's_o' could not be released: slot is active; run `SELECT pg_drop_replication_slot('s_o')` on the source, or leave it: the next run of the export reuses it"
        );
    }

    /// A checkpoint that is already gone counts as released; one that is there is deleted; any other failure is surfaced.
    #[test]
    fn removing_an_absent_file_is_released_already() {
        let dir = tempfile::tempdir().unwrap();
        assert!(remove_file(&dir.path().join("none")).is_ok());
        assert!(remove_file(dir.path()).is_err());
        let there = dir.path().join("there");
        std::fs::write(&there, b"x").unwrap();
        remove_file(&there).unwrap();
        assert!(!there.exists());
    }

    /// An engine with no slot to probe is still proven a first run, and its slot stays absent from the proof.
    #[test]
    fn an_engine_without_slots_is_proven_a_first_run_by_the_real_entry() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = config_with(dir.path(), "o", MYSQL_SOURCE);
        let state = StateStore::open_in_memory().unwrap();
        let first = FirstRun::of(&config, &export, &state, &path).unwrap();
        assert_eq!(first.slot, None);
        assert_eq!(first.checkpoint, Some(dir.path().join("./cdc/o.ckpt")));
        assert_eq!(first.export, "o");
        write_checkpoint(dir.path());
        assert!(FirstRun::of(&config, &export, &state, &path).is_none());
    }

    /// A PostgreSQL source whose server cannot be asked is not proven a first run.
    #[test]
    fn a_postgres_source_that_cannot_be_probed_is_not_proven_a_first_run() {
        let dir = tempfile::tempdir().unwrap();
        let (path, config, export) = config_with(dir.path(), "o", PG_DOWN_SOURCE);
        let state = StateStore::open_in_memory().unwrap();
        assert!(FirstRun::of(&config, &export, &state, &path).is_none());
    }

    /// The server handle reports a server it cannot reach as an error, never as a slot that is there or gone.
    #[test]
    fn an_unreachable_server_is_an_error_for_both_slot_calls() {
        let server = PgSlots {
            url: "postgres://u:p@127.0.0.1:1/app".into(),
            tls: None,
        };
        assert!(server.exists("s_o").is_err());
        assert!(server.drop_slot("s_o").is_err());
    }

    /// The real release on an engine with no slot deletes the checkpoint and forgets the capture on a stop, and keeps both on `Ok`.
    #[test]
    fn the_real_release_on_an_engine_without_slots_follows_the_outcome() {
        for outcome_is_stop in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let (path, config, export) = config_with(dir.path(), "o", MYSQL_SOURCE);
            let state = StateStore::open_in_memory().unwrap();
            let first = FirstRun::of(&config, &export, &state, &path).unwrap();
            write_checkpoint(dir.path());
            let key = key_of(&config, &export);
            let dest = export.destination.state_key();
            state.record_captured_tables(&key, &dest).unwrap();
            let outcome = if outcome_is_stop { stopped() } else { Ok(()) };
            first.release_after(&config, &state, &outcome);
            let kept = !outcome_is_stop;
            assert_eq!(dir.path().join("cdc/o.ckpt").exists(), kept);
            assert_eq!(state.captured_tables(&key, &dest).unwrap().is_some(), kept);
        }
    }

    /// A slot the real server handle cannot reach is warned with its code and remedy, and the rest is still released.
    #[test]
    fn the_real_release_warns_for_a_slot_it_cannot_reach_and_releases_the_rest() {
        use crate::pipeline::run::run_tail_tests::{
            captured_warnings_mentioning, install_warn_capture,
        };
        install_warn_capture();
        let dir = tempfile::tempdir().unwrap();
        let (_, config, export) = config_with(dir.path(), "anchor_release_warn", PG_DOWN_SOURCE);
        let state = StateStore::open_in_memory().unwrap();
        let first = FirstRun {
            slot: Some("s_o".to_string()),
            checkpoint: Some(dir.path().join("cdc/o.ckpt")),
            key: key_of(&config, &export),
            destination: export.destination.state_key(),
            export: export.name.clone(),
        };
        write_checkpoint(dir.path());
        let key = key_of(&config, &export);
        let dest = export.destination.state_key();
        state.record_captured_tables(&key, &dest).unwrap();
        first.release_after(&config, &state, &stopped());
        let said = captured_warnings_mentioning("export 'anchor_release_warn'");
        assert_eq!(said.len(), 1, "{said:?}");
        assert!(
            said[0].starts_with("[RIVET_SOURCE_CDC_ANCHOR_NOT_RELEASED] "),
            "{said:?}"
        );
        assert!(
            said[0].contains("its replication slot 's_o' could not be released"),
            "{said:?}"
        );
        assert!(
            said[0].contains("SELECT pg_drop_replication_slot('s_o')"),
            "{said:?}"
        );
        assert!(!dir.path().join("cdc/o.ckpt").exists());
        assert!(state.captured_tables(&key, &dest).unwrap().is_none());
    }
}
