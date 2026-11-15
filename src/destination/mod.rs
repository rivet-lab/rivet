//! **Layer: Execution**

pub mod azure;
mod cloud;
#[cfg(test)]
pub(crate) use cloud::retry_layer;
pub use cloud::{add_transient_retries, transient_retries_summary, transient_retries_total};
pub mod gcs;
pub mod local;
pub mod placeholder;
pub mod s3;
pub mod stdout;

use std::path::Path;

use crate::config::DestinationConfig;
use crate::error::Result;

/// The point at which a write is considered durably committed and visible to readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteCommitProtocol {
    /// The write is atomic from the caller's perspective.
    Atomic,
    /// The object is not committed until the internal writer handle is closed.
    FinalizeOnClose,
    /// Data is streamed to an unbuffered output with no atomic commit boundary.
    Streaming,
}

impl WriteCommitProtocol {
    /// Does this destination leave NAMED OBJECTS behind that a later process can list, stat, or overwrite?
    pub fn leaves_objects_at_rest(self) -> bool {
        match self {
            Self::Atomic | Self::FinalizeOnClose => true,
            Self::Streaming => false,
        }
    }
}

/// Operational guarantees provided by a destination backend.
#[derive(Debug, Clone)]
pub struct DestinationCapabilities {
    /// When the written output is considered durably committed.
    pub commit_protocol: WriteCommitProtocol,
    /// Whether writing the same key twice produces a clean replacement with no leftover artifacts from the previous attempt.
    pub idempotent_overwrite: bool,
    /// Whether a failed `write()` can be retried without manual cleanup.
    pub retry_safe: bool,
    /// Whether a failed `write()` can leave a partial file or object at the destination.
    pub partial_write_risk: bool,
}

/// Read-side metadata for a single object at the destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Object key relative to the destination's configured prefix.
    pub key: String,
    pub size_bytes: u64,
    /// The object's content MD5 from listing / stat metadata, when the backend exposes one — `None` otherwise.
    pub content_md5: Option<String>,
}

/// What a [`Destination::write`] reports about the object it just stored.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteOutcome {
    /// The store-reported content checksum (base64 MD5 or hex ETag), or `None`.
    pub content_md5: Option<String>,
}

impl WriteOutcome {
    /// A write that reported no content checksum (local FS, streamed uploads).
    pub fn opaque() -> Self {
        Self { content_md5: None }
    }
}

/// Object-safe surface for upload backends.
pub trait Destination: Send + Sync {
    /// Write `local_path` to `remote_key`, returning what the store reported about the stored object (see [`WriteOutcome`]).
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<WriteOutcome>;

    /// Describe the operational guarantees of this destination backend.
    fn capabilities(&self) -> DestinationCapabilities;

    /// List every object at or below `prefix`, in **unspecified** order.
    fn list_prefix(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let _ = prefix;
        crate::rivet_bail!(
            crate::error::codes::INTERNAL_DEST_INVARIANT,
            "list_prefix is not supported by this destination backend"
        )
    }

    /// Read the full body of `key` into memory.
    fn read(&self, key: &str) -> Result<Vec<u8>> {
        let _ = key;
        crate::rivet_bail!(
            crate::error::codes::INTERNAL_DEST_INVARIANT,
            "read is not supported by this destination backend"
        )
    }

    /// Stat `key`, returning `None` if the object does not exist and the underlying backend can disambiguate "absent" from a hard error.
    fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let _ = key;
        crate::rivet_bail!(
            crate::error::codes::INTERNAL_DEST_INVARIANT,
            "head is not supported by this destination backend"
        )
    }

    /// Delete the object at `key` under the destination prefix; an absent key is Ok.
    fn remove(&self, key: &str) -> Result<()> {
        let _ = key;
        crate::rivet_bail!(
            crate::error::codes::INTERNAL_DEST_INVARIANT,
            "remove is not supported by this destination backend"
        )
    }

    /// Move `from` to `to` at the destination prefix.
    fn r#move(&self, from: &str, to: &str) -> Result<()> {
        let _ = (from, to);
        crate::rivet_bail!(
            crate::error::codes::INTERNAL_DEST_INVARIANT,
            "move is not supported by this destination backend"
        )
    }
}

/// Log destination capabilities at DEBUG level and emit a WARN when the backend is not retry-safe but retries are configured.
pub fn log_capabilities(
    export_name: &str,
    dest: &dyn Destination,
    dest_kind: crate::config::DestinationType,
    max_retries: u32,
) {
    let caps = dest.capabilities();
    log::debug!(
        "export '{}': destination commit_protocol={:?} idempotent={} retry_safe={} partial_risk={}",
        export_name,
        caps.commit_protocol,
        caps.idempotent_overwrite,
        caps.retry_safe,
        caps.partial_write_risk,
    );
    if !caps.retry_safe && max_retries > 0 {
        log::warn!(
            "export '{}': {} destination is not retry-safe (max_retries={}); \
             partial artifacts may exist at destination on failure — manual cleanup may be needed",
            export_name,
            dest_kind.label(),
            max_retries,
        );
    }
}

pub fn create_destination(config: &DestinationConfig) -> Result<Box<dyn Destination>> {
    use crate::config::DestinationType;
    match config.destination_type {
        DestinationType::Local => Ok(Box::new(local::LocalDestination::new(config)?)),
        DestinationType::S3 => Ok(Box::new(s3::S3Destination::new(config)?)),
        DestinationType::Gcs => Ok(Box::new(gcs::GcsDestination::new(config)?)),
        DestinationType::Azure => Ok(Box::new(azure::AzureDestination::new(config)?)),
        DestinationType::Stdout => Ok(Box::new(stdout::StdoutDestination::new()?)),
    }
}

/// Build a destination for a preflight connectivity probe (`rivet doctor`).
pub fn create_destination_for_probe(config: &DestinationConfig) -> Result<Box<dyn Destination>> {
    use crate::config::DestinationType;
    const PROBE_MAX_RETRIES: usize = 0;
    match config.destination_type {
        DestinationType::S3 => Ok(Box::new(s3::S3Destination::new_with_retries(
            config,
            PROBE_MAX_RETRIES,
        )?)),
        DestinationType::Gcs => Ok(Box::new(gcs::GcsDestination::new_with_retries(
            config,
            PROBE_MAX_RETRIES,
        )?)),
        DestinationType::Azure => Ok(Box::new(azure::AzureDestination::new_with_retries(
            config,
            PROBE_MAX_RETRIES,
        )?)),
        DestinationType::Local | DestinationType::Stdout => create_destination(config),
    }
}

/// A multi-table stream lands each table under its own sub-prefix of the export's destination (`<base>/<table>/`), so every table's prefix is self-describing (its own parts + `manifest.json` + `_SUCCESS`), exactly like N single-table exports.
pub(crate) fn dest_for_table(
    base: &crate::config::DestinationConfig,
    table: &str,
) -> crate::config::DestinationConfig {
    let mut d = base.clone();
    match d.destination_type {
        crate::config::DestinationType::Local => {
            let p = d.path.take().unwrap_or_else(|| ".".into());
            d.path = Some(format!("{}/{}", p.trim_end_matches('/'), table));
        }
        crate::config::DestinationType::Stdout => {}
        _ => {
            let pfx = d.prefix.take().unwrap_or_default();
            let base = pfx.trim_end_matches('/');
            d.prefix = Some(if base.is_empty() {
                format!("{table}/")
            } else {
                format!("{base}/{table}/")
            });
        }
    }
    d
}

#[cfg(test)]
mod tests {

    /// One answer for "is there anything at rest here", asserted against the protocol every REAL destination reports.
    #[test]
    fn only_a_streaming_destination_leaves_nothing_at_rest() {
        use super::WriteCommitProtocol as P;

        assert!(
            P::Atomic.leaves_objects_at_rest(),
            "a local file is at rest"
        );
        assert!(
            P::FinalizeOnClose.leaves_objects_at_rest(),
            "a cloud object is at rest once the handle closes — the prefix can be listed"
        );
        assert!(
            !P::Streaming.leaves_objects_at_rest(),
            "stdout has no prefix to collide on, no manifest to read back, no prior run to warn \
             about — every guard that probes a destination must skip it"
        );

        let tmp = tempfile::tempdir().expect("tempdir");
        let of = |t: crate::config::DestinationType, path: Option<String>| {
            let cfg = crate::config::DestinationConfig {
                destination_type: t,
                path,
                ..Default::default()
            };
            super::create_destination(&cfg)
                .expect("destination builds")
                .capabilities()
                .commit_protocol
        };
        assert!(
            of(
                crate::config::DestinationType::Local,
                Some(tmp.path().to_string_lossy().into_owned())
            )
            .leaves_objects_at_rest(),
            "the local destination is the one every rerun/manifest guard actually protects"
        );
        assert!(
            !of(crate::config::DestinationType::Stdout, None).leaves_objects_at_rest(),
            "stdout is the one they must all skip"
        );
    }

    use super::*;

    /// The not-retry-safe WARN must not fire for a LOCAL destination.
    #[test]
    fn a_local_destination_is_retry_safe_so_the_partial_artifact_warning_cannot_fire() {
        let cfg = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some("/tmp/rivet-caps-probe".to_string()),
            ..Default::default()
        };
        let local = local::LocalDestination::new(&cfg).expect("local destination");
        let caps = local.capabilities();

        assert!(
            caps.retry_safe,
            "temp-then-rename leaves nothing at the final key on failure, so a local \
             destination IS retry-safe — reporting otherwise makes log_capabilities \
             warn about partial artifacts on every ordinary local run"
        );
        assert!(
            !caps.partial_write_risk,
            "…and for the same reason there is no partial-write window"
        );
    }

    #[test]
    fn create_destination_local_succeeds() {
        use crate::config::{DestinationConfig, DestinationType};
        let dir = tempfile::TempDir::new().unwrap();
        let config = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(dir.path().to_str().unwrap().to_string()),
            ..Default::default()
        };
        let dest = create_destination(&config).unwrap();
        let caps = dest.capabilities();
        assert_eq!(caps.commit_protocol, WriteCommitProtocol::Atomic);
        assert!(caps.idempotent_overwrite);
    }

    #[test]
    fn create_destination_stdout_succeeds() {
        use crate::config::{DestinationConfig, DestinationType};
        let config = DestinationConfig {
            destination_type: DestinationType::Stdout,
            ..Default::default()
        };
        let dest = create_destination(&config).unwrap();
        let caps = dest.capabilities();
        assert_eq!(caps.commit_protocol, WriteCommitProtocol::Streaming);
    }
}

#[cfg(test)]
mod dest_for_table_tests {
    use super::dest_for_table;
    use crate::config::{DestinationConfig, DestinationType};

    #[test]
    fn a_stdout_destination_gets_no_per_table_location() {
        let stdout = DestinationConfig {
            destination_type: DestinationType::Stdout,
            ..Default::default()
        };
        let d = dest_for_table(&stdout, "orders");
        assert_eq!((d.path, d.prefix), (None, None));
    }

    #[test]
    fn cloud_table_sub_prefix_carries_its_own_trailing_slash() {
        let gcs = DestinationConfig {
            destination_type: DestinationType::Gcs,
            bucket: Some("b".into()),
            prefix: Some("exports".into()),
            ..Default::default()
        };
        let d = dest_for_table(&gcs, "orders");
        let prefix = d.prefix.unwrap();
        let key = format!("{prefix}{}", "cdc-r-000000.parquet");
        assert_eq!(
            key, "exports/orders/cdc-r-000000.parquet",
            "cloud keys are prefix ++ name — the sub-prefix must end with '/'"
        );

        let gcs2 = DestinationConfig {
            prefix: Some("exports/".into()),
            ..gcs.clone()
        };
        assert_eq!(
            dest_for_table(&gcs2, "orders").prefix.unwrap(),
            "exports/orders/"
        );

        let gcs3 = DestinationConfig {
            prefix: None,
            ..gcs
        };
        assert_eq!(dest_for_table(&gcs3, "orders").prefix.unwrap(), "orders/");
    }

    #[test]
    fn local_table_sub_path_is_a_plain_directory_join() {
        let local = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("/data/cdc/".into()),
            ..Default::default()
        };
        assert_eq!(
            dest_for_table(&local, "orders").path.unwrap(),
            "/data/cdc/orders",
            "local paths go through the filesystem join — no trailing slash needed"
        );
    }
}

#[cfg(test)]
mod error_code_tests {
    /// A backend with only the required methods: every optional one is a bug to reach.
    struct Bare;
    impl super::Destination for Bare {
        fn write(&self, _: &std::path::Path, _: &str) -> super::Result<super::WriteOutcome> {
            Ok(super::WriteOutcome { content_md5: None })
        }
        fn capabilities(&self) -> super::DestinationCapabilities {
            super::DestinationCapabilities {
                commit_protocol: super::WriteCommitProtocol::FinalizeOnClose,
                idempotent_overwrite: true,
                retry_safe: true,
                partial_write_risk: false,
            }
        }
    }

    #[test]
    fn an_unsupported_backend_operation_is_an_internal_invariant() {
        use super::Destination as _;
        for err in [
            Bare.list_prefix("").unwrap_err(),
            Bare.read("k").unwrap_err(),
            Bare.head("k").unwrap_err(),
            Bare.remove("k").unwrap_err(),
            Bare.r#move("a", "b").unwrap_err(),
        ] {
            crate::error::assert_coded(&err, "RIVET_INTERNAL_DEST_INVARIANT", 6);
        }
    }
}
