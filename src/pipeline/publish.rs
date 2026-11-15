//! Per-run staging: a run writes its temp files under `_staging/<owner>/`, removes that subtree itself, and the next run settles what a dead one left.

use std::collections::BTreeSet;

use crate::config::DestinationConfig;
use crate::destination::{Destination, STAGING_DIR, create_destination};
use crate::state::StateStore;

/// `destination` as the staging owner `owner` writes it.
pub(crate) fn staged_for(destination: &DestinationConfig, owner: &str) -> DestinationConfig {
    DestinationConfig {
        staging_owner: Some(owner.to_string()),
        ..destination.clone()
    }
}

/// The destination to settle, or None for a target that leaves nothing at rest.
fn at_rest(destination: &DestinationConfig) -> Option<Box<dyn Destination>> {
    let dest = create_destination(destination).ok()?;
    dest.capabilities()
        .commit_protocol
        .leaves_objects_at_rest()
        .then_some(dest)
}

/// Remove the staging subtree of the run that owns `destination`.
pub(crate) fn close(destination: &DestinationConfig) {
    let Some(owner) = destination.staging_owner.as_deref() else {
        return;
    };
    if let Some(dest) = at_rest(destination)
        && let Err(e) = dest.remove_staged(owner)
    {
        log::warn!("staging subtree {STAGING_DIR}/{owner} was not removed: {e:#}");
    }
}

/// Remove the staging subtree of every owner whose run is no longer running; an owner the state does not know is left, with a warning.
pub(crate) fn settle(state: &StateStore, destination: &DestinationConfig) {
    let Some(dest) = at_rest(destination) else {
        return;
    };
    let listed = match dest.list_prefix(STAGING_DIR) {
        Ok(listed) => listed,
        Err(e) => {
            log::warn!("staging subtrees under {STAGING_DIR}/ were not listed: {e:#}");
            return;
        }
    };
    let owners: BTreeSet<&str> = listed
        .iter()
        .filter_map(|m| m.key.split('/').nth(1))
        .collect();
    for owner in owners {
        match state.run_status_of(owner) {
            Ok(Some(status)) if status == "running" => {}
            Ok(Some(_)) => {
                if let Err(e) = dest.remove_staged(owner) {
                    log::warn!("staging subtree {STAGING_DIR}/{owner} was not removed: {e:#}");
                }
            }
            Ok(None) => log::warn!(
                "staging subtree {STAGING_DIR}/{owner} belongs to a run this state does not know; left in place"
            ),
            Err(e) => log::warn!("staging subtree {STAGING_DIR}/{owner} was not settled: {e:#}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leave(dir: &std::path::Path, owner: &str) {
        let d = dir.join(STAGING_DIR).join(owner);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join(".part.tmp"), b"x").unwrap();
    }

    /// Settle removes a finished owner's subtree and keeps a running one's and an unknown one's.
    #[test]
    fn settle_removes_only_the_subtrees_of_runs_that_are_not_running() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("rivet.yaml");
        std::fs::write(&cfg, "# test").unwrap();
        let state = StateStore::open(cfg.to_str().unwrap()).unwrap();
        let out = dir.path().join("out");
        for owner in ["dead", "live", "stranger"] {
            leave(&out, owner);
        }
        let now = "2026-11-15T00:00:00Z";
        state.begin_run("dead", "e", "p", now).unwrap();
        state.finish_run("dead", "failed", now).unwrap();
        state.begin_run("live", "e", "p", now).unwrap();
        let destination = DestinationConfig {
            path: Some(out.to_string_lossy().into_owned()),
            ..Default::default()
        };
        settle(&state, &destination);
        assert!(!out.join("_staging/dead").exists());
        assert!(out.join("_staging/live/.part.tmp").exists());
        assert!(out.join("_staging/stranger/.part.tmp").exists());
    }
}
