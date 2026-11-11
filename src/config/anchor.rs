use super::{credential_file, destination};
use std::path::{Path, PathBuf};

/// The one place a path a config names resolves: against the config file's directory, never the working directory.
pub(crate) struct Anchor {
    dir: PathBuf,
    cwd: Option<PathBuf>,
}

impl Anchor {
    /// The anchor of the config at `config_path`, or `None` where its directory cannot be told.
    pub(crate) fn of(config_path: &str, cwd: Option<PathBuf>) -> Option<Self> {
        let dir = std::path::absolute(config_path)
            .ok()
            .and_then(|p| p.parent().map(destination::normalized))?;
        Some(Self { dir, cwd })
    }

    /// Anchor `written` beside the config; refuses while only the working directory (where earlier releases read it) holds a file there, so an upgrade never reads another file silently.
    pub(crate) fn file(&self, field: &str, written: &str) -> crate::error::Result<String> {
        let anchored = credential_file::anchor(&self.dir, written);
        let old = self
            .cwd
            .as_ref()
            .filter(|_| !Path::new(written).is_absolute())
            .map(|c| destination::normalized(&c.join(written)));
        if !Path::new(&anchored).exists()
            && let Some(old) = old.filter(|o| o.exists())
        {
            crate::rivet_bail!(
                crate::error::codes::CONFIG_SECRET_UNSET,
                "{field} '{written}' is relative and now names {anchored} (beside the config), while {} (under the working directory, where earlier releases read it) holds a file and {anchored} holds none: move the file to {anchored}, or make the path absolute",
                old.display()
            );
        }
        Ok(anchored)
    }
}
