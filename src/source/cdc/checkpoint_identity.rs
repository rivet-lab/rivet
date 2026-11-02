//! One verdict for "does this checkpoint belong to this source".

use crate::error::Result;

/// The re-baseline remedy every CDC data-loss message ends with, in the order the product runs it.
pub(crate) const RECOVER: &str = "Re-baseline the stream in one run: delete the checkpoint \
     file if there is one; move every file out of the export's destination (for a `tables:` \
     export, every table's directory under it): the parts there still hold rows the source may \
     no longer have, and each table's snapshot/_SUCCESS marker goes with them; delete the \
     export's `cdc_snapshot` rows (one per table) from the state DB; give the export \
     `cdc.initial: snapshot` if it has none; and if a warehouse load consumes this stream, \
     truncate its `<table>__changes` table before the next load. That run anchors FIRST and \
     re-reads every table after, so nothing falls between the two. A separate `mode: full` \
     export does not re-baseline the stream.";

/// A refusal of `code` whose text is `why`.
fn refusal(code: crate::error::Code, why: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(crate::error::CodedError::new(code, why))
}

/// Refuse a checkpoint that cannot be resumed.
pub(crate) fn checkpoint_invalid(why: String) -> anyhow::Error {
    refusal(crate::error::codes::SOURCE_CDC_CHECKPOINT_INVALID, why)
}

/// Refuse a lost checkpoint file the state DB recorded: `SOURCE_CDC_CHECKPOINT_INVALID`, exit 5.
pub(crate) fn lost_file_anchor(
    engine: &str,
    checkpoint: &std::path::Path,
    config: &str,
    export: &str,
) -> anyhow::Error {
    checkpoint_invalid(format!(
        "{engine} cdc: checkpoint '{}' is missing, but the state DB records that export '{export}' \
         wrote it on an earlier run — anchoring at the current position would silently skip every \
         change committed since. Restore the checkpoint file from the volume or worker that held \
         it; or, to accept the loss of those changes and anchor at the current position, run \
         `rivet state reset -c {config} -e {export}`; or: {RECOVER}",
        checkpoint.display()
    ))
}

/// Refuse a resume position the source's log no longer reaches.
pub(crate) fn log_gap(why: String) -> anyhow::Error {
    refusal(crate::error::codes::SOURCE_CDC_LOG_GAP, why)
}

/// Refuse a captured table whose rows left the source without change events (TRUNCATE, DROP).
pub(crate) fn truncated(why: impl Into<String>) -> anyhow::Error {
    refusal(crate::error::codes::SOURCE_CDC_TRUNCATED, why)
}

/// Refuse a log span this reader cannot decode into events, so the stream re-baselines past it.
pub(crate) fn undecodable(why: String) -> anyhow::Error {
    refusal(crate::error::codes::SOURCE_CDC_UNDECODABLE, why)
}

/// The config and export a checkpoint belongs to, kept beside it as `<checkpoint>.owner`; an empty `config` is a `rivet cdc` command.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CheckpointOwner {
    pub(crate) config: String,
    pub(crate) export: String,
}

impl CheckpointOwner {
    /// Export `export` of the config at `config`.
    pub(crate) fn export(config: &std::path::Path, export: &str) -> Self {
        Self {
            config: canonical_place(config).display().to_string(),
            export: export.to_string(),
        }
    }

    /// The `rivet cdc` command, which has no config.
    pub(crate) fn command() -> Self {
        Self {
            config: String::new(),
            export: "rivet cdc".to_string(),
        }
    }

    fn is_command(&self) -> bool {
        self.config.is_empty()
    }

    /// Who this is, as a refusal names it.
    fn describe(&self) -> String {
        match self.is_command() {
            true => "a `rivet cdc` command".to_string(),
            false => format!("export '{}' of config '{}'", self.export, self.config),
        }
    }
}

/// The owner record beside the checkpoint at `ckpt`.
pub(crate) fn owner_path(ckpt: &std::path::Path) -> std::path::PathBuf {
    let mut name = ckpt.as_os_str().to_os_string();
    name.push(".owner");
    std::path::PathBuf::from(name)
}

/// `p` spelled one way whatever exists of it.
pub(crate) fn canonical_place(p: &std::path::Path) -> std::path::PathBuf {
    let lexical: std::path::PathBuf = p.components().collect();
    let mut head = lexical.as_path();
    let mut tail = Vec::new();
    loop {
        if let Ok(real) = std::fs::canonicalize(head) {
            return tail.iter().rev().fold(real, |acc, c| acc.join(c));
        }
        match (head.parent(), head.file_name()) {
            (Some(up), Some(name)) => {
                tail.push(name.to_os_string());
                head = up;
            }
            _ => return lexical,
        }
    }
}

/// The owner record beside the checkpoint at `ckpt`, when one was written and reads.
pub(crate) fn owner_of(ckpt: &std::path::Path) -> Option<CheckpointOwner> {
    let text = std::fs::read_to_string(owner_path(ckpt)).ok()?;
    serde_json::from_str(&text).ok()
}

/// Whether the stream `owner` records still reads the checkpoint `ckpt`.
fn still_reads(owner: &CheckpointOwner, ckpt: &std::path::Path) -> bool {
    if owner.is_command() {
        return true;
    }
    let config = std::path::Path::new(&owner.config);
    if !config.exists() {
        return false;
    }
    let Ok(cfg) = crate::config::Config::load(&config.display().to_string()) else {
        return true;
    };
    let dir = config.parent().unwrap_or(std::path::Path::new("."));
    let same = |p: &std::path::Path| canonical_place(p) == canonical_place(ckpt);
    cfg.exports.iter().any(|e| {
        e.name == owner.export
            && e.cdc
                .as_ref()
                .and_then(|c| c.checkpoint.as_deref())
                .is_some_and(|raw| same(&super::resolve_checkpoint(raw, dir)))
    })
}

/// The refusal of `me` reading `ckpt`, which `held` still reads.
fn shared(ckpt: &std::path::Path, held: &CheckpointOwner, me: &CheckpointOwner) -> anyhow::Error {
    let what = ckpt.display();
    let (held_by, me_as) = (held.describe(), me.describe());
    let tail = match (me.is_command(), held.is_command()) {
        (false, false) => "Give this export a `cdc.checkpoint` of its own (`rivet init -o` names it after the \
             config file) and keep this file: it is the other export's position. Give it \
             `cdc.initial: snapshot` if it has none, so its first run on the new checkpoint re-reads every \
             table, and move any file it wrote before out of its destination first: those may miss the \
             changes the other stream read. If this config replaces that one (an edited copy of it), delete \
             or rename that config, or point its `cdc.checkpoint` elsewhere, and run this one again"
            .to_string(),
        (true, _) => "Give `rivet cdc` a `--checkpoint` of its own and keep this file: it is the other \
             stream's position"
            .to_string(),
        (false, true) => format!(
            "Give this export a `cdc.checkpoint` of its own and keep this file: it is that command's \
             position. If that command is finished and this export should continue from where it \
             stopped, delete '{}' and run this one again",
            owner_path(ckpt).display()
        ),
    };
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::SOURCE_CDC_CHECKPOINT_SHARED,
        format!(
            "cdc: checkpoint '{what}' belongs to {held_by}, which still reads it; {me_as} would continue \
             from where that one stopped, and each stream would skip the changes the other read. Nothing \
             was read. {tail}"
        ),
    ))
}

/// Claim the checkpoint `ckpt` for the stream `me`: refuse one another stream still reads, record `me` beside it otherwise.
pub(crate) fn claim_checkpoint(ckpt: &std::path::Path, me: &CheckpointOwner) -> Result<()> {
    let record = owner_path(ckpt);
    if let Some(held) = owner_of(ckpt) {
        if &held == me {
            return Ok(());
        }
        if still_reads(&held, ckpt) {
            return Err(shared(ckpt, &held, me));
        }
    }
    if let Some(dir) = record.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&record, serde_json::to_vec(me)?)?;
    Ok(())
}

/// What a resume may do given the checkpoint's recorded identity and the server's.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IdentityVerdict {
    Ok,
    /// Resume proceeds, but nothing could be verified; the text says why.
    Unverifiable(String),
    /// Resume is refused: another source's position, or a log rewound since; the text says which.
    Foreign(String),
}

impl IdentityVerdict {
    /// Refuse a foreign checkpoint as `SOURCE_CDC_FOREIGN_CHECKPOINT` (exit 5).
    pub(crate) fn enforce(self) -> Result<()> {
        match self {
            Self::Ok => Ok(()),
            Self::Unverifiable(why) => {
                log::warn!("{why}");
                Ok(())
            }
            Self::Foreign(why) => crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_FOREIGN_CHECKPOINT,
                "{why} {RECOVER}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The remedy text, pinned against a hand-written copy.
    #[test]
    fn the_rebaseline_remedy_names_the_steps_the_product_runs() {
        assert_eq!(
            RECOVER,
            "Re-baseline the stream in one run: delete the checkpoint file if there is one; move \
             every file out of the export's destination (for a `tables:` export, every table's \
             directory under it): the parts there still hold rows the source may no longer have, \
             and each table's snapshot/_SUCCESS marker goes with them; delete the export's \
             `cdc_snapshot` rows (one per table) from the state DB; give the export \
             `cdc.initial: snapshot` if it has none; and if a warehouse load consumes this \
             stream, truncate its `<table>__changes` table before the next load. That run anchors \
             FIRST and re-reads every table after, so nothing falls between the two. A separate \
             `mode: full` export does not re-baseline the stream."
        );
    }

    #[test]
    fn a_foreign_verdict_refuses_with_exit_5_and_the_recovery_order() {
        let err = IdentityVerdict::Foreign("x cdc: elsewhere.".into())
            .enforce()
            .unwrap_err();
        assert_eq!(crate::error::classify_exit(&err), 5);
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_SOURCE_CDC_FOREIGN_CHECKPOINT")
        );
        assert_eq!(err.to_string(), format!("x cdc: elsewhere. {RECOVER}"));
        assert!(
            IdentityVerdict::Unverifiable("old".into())
                .enforce()
                .is_ok()
        );
        assert!(IdentityVerdict::Ok.enforce().is_ok());
    }

    #[test]
    fn each_failure_class_refuses_with_exit_5_under_its_own_code_and_keeps_its_text() {
        type Class = fn(String) -> anyhow::Error;
        let classes: [(Class, &str); 3] = [
            (checkpoint_invalid, "RIVET_SOURCE_CDC_CHECKPOINT_INVALID"),
            (log_gap, "RIVET_SOURCE_CDC_LOG_GAP"),
            (truncated, "RIVET_SOURCE_CDC_TRUNCATED"),
        ];
        for (class, code) in classes {
            let err = class(format!("x cdc: {code} happened. {RECOVER}"));
            assert_eq!(crate::error::classify_exit(&err), 5, "{code}");
            assert_eq!(crate::error::error_code(&err), Some(code));
            assert_eq!(
                err.to_string(),
                format!("x cdc: {code} happened. {RECOVER}")
            );
        }
    }
}

#[cfg(test)]
mod owner_tests {
    use super::*;

    /// A config in `dir` whose export `export` reads `ckpt`.
    fn config(dir: &std::path::Path, file: &str, export: &str, ckpt: &str) -> std::path::PathBuf {
        let path = dir.join(file);
        std::fs::write(
            &path,
            format!(
                "source:\n  type: mysql\n  url: \"mysql://u:p@localhost/app\"\nexports:\n  - name: {export}\n    \
                 table: orders\n    mode: cdc\n    format: parquet\n    cdc:\n      checkpoint: {ckpt}\n    \
                 destination: {{ type: local, path: ./out_{export} }}\n"
            ),
        )
        .unwrap();
        path
    }

    /// A checkpoint is claimed by its first reader; a second config's export is refused by code.
    #[test]
    fn a_first_claim_makes_the_checkpoint_directory() {
        let dir = tempfile::tempdir().unwrap();
        let a = config(dir.path(), "a.yaml", "orders_cdc", "./cdc/new/a.ckpt");
        let ckpt = dir.path().join("cdc/new/a.ckpt");
        claim_checkpoint(&ckpt, &CheckpointOwner::export(&a, "orders_cdc")).unwrap();
        let owner = owner_of(&ckpt).expect("the owner record");
        assert_eq!(owner.export, "orders_cdc");
        assert!(owner.config.ends_with("a.yaml"), "{owner:?}");
    }

    #[test]
    fn a_checkpoint_another_config_still_reads_is_refused_on_every_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("cdc/shared.ckpt");
        std::fs::create_dir_all(ckpt.parent().unwrap()).unwrap();
        std::fs::write(&ckpt, r#"{"file":"bin.000001","pos":4}"#).unwrap();
        let a = config(dir.path(), "a.yaml", "orders_cdc", "./cdc/shared.ckpt");
        let b = config(dir.path(), "b.yaml", "orders_cdc", "./cdc/shared.ckpt");
        claim_checkpoint(&ckpt, &CheckpointOwner::export(&a, "orders_cdc"))
            .expect("a checkpoint with no owner record is claimed");
        let unwritten = dir.path().join("cdc/not_yet.ckpt");
        config(dir.path(), "c.yaml", "orders_cdc", "./cdc/not_yet.ckpt");
        config(dir.path(), "d.yaml", "orders_cdc", "./cdc/not_yet.ckpt");
        claim_checkpoint(
            &unwritten,
            &CheckpointOwner::export(&dir.path().join("c.yaml"), "orders_cdc"),
        )
        .unwrap();
        let other_spelling = canonical_place(dir.path())
            .join(".")
            .join("cdc/not_yet.ckpt");
        let err = claim_checkpoint(
            &other_spelling,
            &CheckpointOwner::export(&dir.path().join("d.yaml"), "orders_cdc"),
        )
        .expect_err("a checkpoint not written yet is still the other config's");
        crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_CHECKPOINT_SHARED", 5);
        std::fs::remove_file(dir.path().join("d.yaml")).unwrap();
        claim_checkpoint(&ckpt, &CheckpointOwner::export(&a, "orders_cdc"))
            .expect("its owner reads it again");
        for _ in 0..2 {
            let err =
                claim_checkpoint(&ckpt, &CheckpointOwner::export(&b, "orders_cdc")).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_CHECKPOINT_SHARED", 5);
            assert!(format!("{err:#}").contains("a.yaml"), "{err:#}");
        }
        config(dir.path(), "a.yaml", "orders_cdc", "./cdc/a.ckpt");
        claim_checkpoint(&ckpt, &CheckpointOwner::export(&b, "orders_cdc"))
            .expect("a checkpoint its owner no longer names");
        std::fs::remove_file(&b).unwrap();
        claim_checkpoint(&ckpt, &CheckpointOwner::export(&a, "orders_cdc"))
            .expect("a checkpoint whose owner is gone");
        let owner: CheckpointOwner =
            serde_json::from_str(&std::fs::read_to_string(owner_path(&ckpt)).unwrap()).unwrap();
        assert!(owner.config.ends_with("a.yaml"), "{owner:?}");
        assert_eq!(
            std::fs::read_to_string(&ckpt).unwrap(),
            r#"{"file":"bin.000001","pos":4}"#,
            "the position itself is never rewritten"
        );
    }

    /// A `rivet cdc` command claims a free checkpoint, is idempotent, and is refused one an export still reads.
    #[test]
    fn a_command_claims_a_checkpoint_and_is_refused_one_an_export_reads() {
        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("c.ckpt");
        let cmd = CheckpointOwner::command();
        claim_checkpoint(&ckpt, &cmd).unwrap();
        claim_checkpoint(&ckpt, &cmd).expect("the same command again");
        assert_eq!(owner_of(&ckpt), Some(cmd.clone()));
        let cfg = dir.path().join("a.yaml");
        std::fs::write(
            &cfg,
            "source:\n  type: mysql\n  url: \"mysql://u@h/d\"\nexports:\n  - name: o\n    table: t\n    mode: cdc\n    \
             format: parquet\n    cdc:\n      checkpoint: ./c.ckpt\n    destination: { type: local, path: ./out }\n",
        )
        .unwrap();
        let export = CheckpointOwner::export(&cfg, "o");
        let err = claim_checkpoint(&ckpt, &export).expect_err("a command holds it");
        crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_CHECKPOINT_SHARED", 5);
        assert!(
            format!("{err:#}").contains("belongs to a `rivet cdc` command"),
            "{err:#}"
        );
        std::fs::remove_file(owner_path(&ckpt)).unwrap();
        claim_checkpoint(&ckpt, &export).unwrap();
        let err = claim_checkpoint(&ckpt, &cmd).expect_err("an export reads it");
        crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_CHECKPOINT_SHARED", 5);
        let text = format!("{err:#}");
        assert!(
            text.contains("export 'o' of config")
                && text.contains("a `rivet cdc` command would continue"),
            "{text}"
        );
        assert!(
            text.contains("Give `rivet cdc` a `--checkpoint` of its own"),
            "{text}"
        );
        std::fs::remove_file(&cfg).unwrap();
        claim_checkpoint(&ckpt, &cmd).expect("an export whose config is gone no longer reads it");
    }
}
