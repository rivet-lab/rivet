//! **Layer: Execution** — shared base for OpenDAL-backed cloud destinations.

use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use opendal::Operator;
use opendal::blocking;
use opendal::layers::RetryLayer;

use crate::config::DestinationConfig;
use crate::error::Result;

/// Process-wide count of transient destination-side retry ATTEMPTS the [`RetryLayer`] scheduled.
pub(crate) static TRANSIENT_RETRIES: AtomicU64 = AtomicU64::new(0);

/// Total transient destination retry attempts made so far in this process.
pub fn transient_retries_total() -> u64 {
    TRANSIENT_RETRIES.load(Ordering::Relaxed)
}

/// The run-summary value for `n` transient destination retries.
pub fn transient_retries_summary(n: u64) -> Option<String> {
    (n > 0).then(|| format!("{n} transient retry attempts (RUST_LOG=debug for detail)"))
}

/// Fold a CHILD process's retry total into this process's counter.
pub fn add_transient_retries(n: u64) {
    TRANSIENT_RETRIES.fetch_add(n, Ordering::Relaxed);
}

/// The production retry layer every object-store operator carries; `max_times == 0` makes a single attempt.
pub(crate) fn retry_layer(max_times: usize) -> RetryLayer<RivetRetryNotify> {
    RetryLayer::new()
        .with_max_times(max_times)
        .with_min_delay(Duration::from_millis(200))
        .with_max_delay(Duration::from_secs(10))
        .with_jitter()
        .with_notify(RivetRetryNotify)
}

/// Rivet's [`opendal::layers::RetryInterceptor`]: keep the truth, drop the spam.
pub(crate) struct RivetRetryNotify;

impl opendal::layers::RetryInterceptor for RivetRetryNotify {
    fn intercept(&self, err: &opendal::Error, dur: Duration) {
        let n = TRANSIENT_RETRIES.fetch_add(1, Ordering::Relaxed);
        if n == 0 {
            log::warn!(
                "destination: transient error, retrying in {:.1}s (further retries log at \
                 DEBUG; the run summary reports the total): {err}",
                dur.as_secs_f64(),
            );
        } else {
            log::debug!(
                "destination: transient error, retrying in {:.1}s: {err}",
                dur.as_secs_f64(),
            );
        }
    }
}

/// Default ceiling on RAM held in one-shot upload buffers.
const DEFAULT_ONESHOT_BUDGET_MB: u64 = 64;

/// The process-wide one-shot pool for a budget size, shared by every destination configured with it.
fn oneshot_pool(config: &DestinationConfig) -> &'static AtomicI64 {
    static POOLS: std::sync::LazyLock<
        std::sync::Mutex<std::collections::HashMap<u64, &'static AtomicI64>>,
    > = std::sync::LazyLock::new(Default::default);
    let mb = config
        .oneshot_budget_mb
        .unwrap_or(DEFAULT_ONESHOT_BUDGET_MB);
    let mut pools = POOLS.lock().unwrap_or_else(|e| e.into_inner());
    pools.entry(mb).or_insert_with(|| {
        let bytes = i64::try_from(mb)
            .unwrap_or(i64::MAX)
            .saturating_mul(1024 * 1024);
        Box::leak(Box::new(AtomicI64::new(bytes)))
    })
}

/// Releases the reserved bytes back to the pool it was taken from on drop.
struct OneShotReservation<'a>(&'a AtomicI64, i64);
impl Drop for OneShotReservation<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(self.1, Ordering::Relaxed);
    }
}

/// Optimistic atomic reserve: subtract `size`; if that would overdraw, undo and fail.
fn take_from(budget: &AtomicI64, size: i64) -> bool {
    if budget.fetch_sub(size, Ordering::Relaxed) >= size {
        true
    } else {
        budget.fetch_add(size, Ordering::Relaxed);
        false
    }
}

/// A backend's contribution to a cloud destination.
pub(crate) trait CloudBackend {
    /// URI scheme logged after a successful upload (`"s3"`, `"gs"`, `"az"`).
    const SCHEME: &'static str;

    /// Build the configured, **un-layered** async operator from `config`.
    fn build_operator(config: &DestinationConfig) -> Result<Operator>;

    /// Is the `content_md5` a LISTING reports actually the object's MD5?
    const LIST_MD5_IS_TRUSTWORTHY: bool = true;

    /// The error kinds a listing ends with when the bucket or container itself is absent.
    const ABSENT_CONTAINER: &'static [opendal::ErrorKind] = &[opendal::ErrorKind::NotFound];
}

/// OpenDAL-backed object-store destination, generic over the backend `B`.
pub(crate) struct CloudDestination<B: CloudBackend> {
    _runtime: Arc<tokio::runtime::Runtime>,
    op: blocking::Operator,
    prefix: String,
    /// The bucket or container, as the config names it.
    container: String,
    /// Set once a listing has answered for the container (see [`CloudDestination::require_container`]).
    container_answered: std::sync::OnceLock<()>,
    /// The one-shot upload pool this destination draws from (see [`oneshot_pool`]).
    oneshot_budget: &'static AtomicI64,
    _backend: PhantomData<fn() -> B>,
}

/// Default retry budget for real exports: OpenDAL retries individual HTTP calls this many times before giving up to the chunk worker's outer loop.
const DEFAULT_MAX_RETRIES: usize = 5;

/// Normalize a destination prefix to the object-store trailing-slash convention.
fn normalize_prefix(p: String) -> String {
    let p = p.trim_start_matches('/').to_string();
    if p.is_empty() || p.ends_with('/') {
        p
    } else {
        format!("{p}/")
    }
}

/// Whether the error a listing's first page ended with says the container is absent.
fn listing_says_container_absent<B: CloudBackend>(
    first_page_error: Option<opendal::ErrorKind>,
) -> bool {
    first_page_error.is_some_and(|kind| B::ABSENT_CONTAINER.contains(&kind))
}

/// The opendal kind of one error in a chain.
fn opendal_kind(e: &(dyn std::error::Error + 'static)) -> Option<opendal::ErrorKind> {
    crate::error::retry::opendal_error(e).map(opendal::Error::kind)
}

/// Whether a failed write may have hit an absent container.
fn write_error_may_mean_absent_container<B: CloudBackend>(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|c| opendal_kind(c).is_some_and(|k| B::ABSENT_CONTAINER.contains(&k)))
}

/// What an operator reads when the configured bucket or container does not exist.
fn container_absent_message(scheme: &str, container: &str) -> String {
    format!(
        "{scheme}://{container}: the bucket or container does not exist (the store answered 404 to a \
         listing of it). Create it, or correct `destination.bucket`"
    )
}

/// The one tokio runtime every object-store operator runs on.
pub(crate) fn io_runtime() -> Result<Arc<tokio::runtime::Runtime>> {
    static RUNTIME: std::sync::OnceLock<Arc<tokio::runtime::Runtime>> = std::sync::OnceLock::new();
    if let Some(rt) = RUNTIME.get() {
        return Ok(Arc::clone(rt));
    }
    let built = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("rivet-io")
            .enable_all()
            .build()
            .map_err(io_runtime_unavailable)?,
    );
    Ok(Arc::clone(RUNTIME.get_or_init(|| built)))
}

/// The OS refused the object-store runtime a thread or a descriptor.
fn io_runtime_unavailable(e: std::io::Error) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::DEST_IO_RUNTIME_UNAVAILABLE,
        format!("failed to create the object-store tokio runtime: {e}"),
    )
}

impl<B: CloudBackend> CloudDestination<B> {
    pub fn new(config: &DestinationConfig) -> Result<Self> {
        Self::new_with_retries(config, DEFAULT_MAX_RETRIES)
    }

    /// Build the destination with an explicit OpenDAL retry budget.
    pub fn new_with_retries(config: &DestinationConfig, max_times: usize) -> Result<Self> {
        let runtime = io_runtime()?;
        let _guard = runtime.enter();

        let async_op = B::build_operator(config)?.layer(retry_layer(max_times));
        let op = blocking::Operator::new(async_op)?;

        let prefix = normalize_prefix(config.prefix.clone().unwrap_or_default());

        Ok(Self {
            _runtime: runtime,
            op,
            prefix,
            container: config.bucket.clone().unwrap_or_default(),
            container_answered: std::sync::OnceLock::new(),
            oneshot_budget: oneshot_pool(config),
            _backend: PhantomData,
        })
    }

    /// Fail when the bucket or container itself is absent, which a 404 on a key cannot tell from an absent key.
    fn require_container(&self) -> Result<()> {
        if self.container_answered.get().is_some() {
            return Ok(());
        }
        self.refuse_an_absent_container()?;
        let _ = self.container_answered.set(());
        Ok(())
    }

    /// Ask the store now whether the bucket or container exists, whatever it answered before.
    fn refuse_an_absent_container(&self) -> Result<()> {
        let first_page = self
            .op
            .lister_options(
                &self.prefix,
                opendal::options::ListOptions {
                    limit: Some(1),
                    ..Default::default()
                },
            )
            .and_then(|mut entries| entries.next().transpose());
        if let Some(e) = first_page.as_ref().err() {
            log::debug!("{}://{}: container probe: {e}", B::SCHEME, self.container);
        }
        if listing_says_container_absent::<B>(first_page.as_ref().err().map(opendal::Error::kind)) {
            crate::rivet_bail!(
                crate::error::codes::DEST_CONTAINER_NOT_FOUND,
                "{}",
                container_absent_message(B::SCHEME, &self.container)
            );
        }
        Ok(())
    }

    /// Reserve `size` bytes for a one-shot buffer from this destination's budget if it allows, else `None` (caller streams).
    fn reserve_oneshot(&self, size: u64) -> Option<OneShotReservation<'_>> {
        let size = i64::try_from(size).unwrap_or(i64::MAX);
        take_from(self.oneshot_budget, size).then(|| OneShotReservation(self.oneshot_budget, size))
    }
}

impl<B: CloudBackend> CloudDestination<B> {
    /// `err` as `RIVET_DEST_CONTAINER_NOT_FOUND` when the operation failed with a kind the backend reports an absent container by and the bucket itself is absent now (it may have gone since it last answered).
    fn absent_container_or(&self, err: anyhow::Error) -> anyhow::Error {
        if seeded!("missing-bucket-uncoded-on-write", true, false) {
            return err;
        }
        match write_error_may_mean_absent_container::<B>(&err)
            .then(|| self.refuse_an_absent_container())
        {
            Some(Err(absent)) => absent,
            _ => err,
        }
    }

    /// Upload one local part to `remote_key` under the prefix.
    fn write_part(&self, local_path: &Path, remote_key: &str) -> Result<super::WriteOutcome> {
        let key = format!("{}{}", self.prefix, remote_key);
        let size = std::fs::metadata(local_path)?.len();
        let outcome = if let Some(_reservation) = self.reserve_oneshot(size) {
            let body = std::fs::read(local_path)?;
            let meta = self.op.write(&key, body)?;
            super::WriteOutcome {
                content_md5: meta.content_md5().map(str::to_string),
            }
        } else {
            let mut src = std::fs::File::open(local_path)?;
            let mut dst = self.op.writer(&key)?.into_std_write();
            std::io::copy(&mut src, &mut dst)?;
            dst.close()?;
            super::WriteOutcome::opaque()
        };
        log::info!("uploaded {}://{} ({size} bytes)", B::SCHEME, key);
        Ok(outcome)
    }
}

impl<B: CloudBackend> super::Destination for CloudDestination<B> {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<super::WriteOutcome> {
        self.write_part(local_path, remote_key)
            .map_err(|e| self.absent_container_or(e))
    }

    fn capabilities(&self) -> super::DestinationCapabilities {
        super::DestinationCapabilities {
            commit_protocol: super::WriteCommitProtocol::FinalizeOnClose,
            idempotent_overwrite: true,
            retry_safe: true,
            partial_write_risk: false,
        }
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<super::ObjectMeta>> {
        let full = format!("{}{}", self.prefix, prefix);
        let listed = if full.is_empty() || full.ends_with('/') {
            self.op
                .list_options(
                    &full,
                    opendal::options::ListOptions {
                        recursive: true,
                        ..Default::default()
                    },
                )
                .map_err(|e| self.absent_container_or(e.into()))?
        } else {
            self.op
                .list_options(
                    &format!("{}/", full),
                    opendal::options::ListOptions {
                        recursive: true,
                        ..Default::default()
                    },
                )
                .map_err(|e| self.absent_container_or(e.into()))?
        };
        let mut out = Vec::with_capacity(listed.len());
        for entry in listed {
            if entry.metadata().mode() != opendal::EntryMode::FILE {
                continue;
            }
            let abs = entry.path().to_string();
            let rel = abs
                .strip_prefix(self.prefix.as_str())
                .unwrap_or(abs.as_str())
                .to_string();
            out.push(super::ObjectMeta {
                key: rel,
                size_bytes: entry.metadata().content_length(),
                content_md5: if B::LIST_MD5_IS_TRUSTWORTHY {
                    entry.metadata().content_md5().map(str::to_string)
                } else {
                    None
                },
            });
        }
        Ok(out)
    }

    fn read(&self, key: &str) -> Result<Vec<u8>> {
        let full = format!("{}{}", self.prefix, key);
        let buf = self
            .op
            .read(&full)
            .map_err(|e| self.absent_container_or(e.into()))?;
        Ok(buf.to_vec())
    }

    fn remove(&self, key: &str) -> Result<()> {
        self.op.delete(&format!("{}{}", self.prefix, key))?;
        Ok(())
    }

    fn r#move(&self, from: &str, to: &str) -> Result<()> {
        let from_full = format!("{}{}", self.prefix, from);
        let to_full = format!("{}{}", self.prefix, to);
        self.op.copy(&from_full, &to_full)?;
        self.op.delete(&from_full)?;
        Ok(())
    }

    fn head(&self, key: &str) -> Result<Option<super::ObjectMeta>> {
        let full = format!("{}{}", self.prefix, key);
        match self.op.stat(&full) {
            Ok(meta) => Ok(Some(super::ObjectMeta {
                key: key.to_string(),
                size_bytes: meta.content_length(),
                content_md5: meta.content_md5().map(str::to_string),
            })),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                seeded!(
                    "missing-bucket-read-as-no-manifest",
                    (),
                    self.require_container()?
                );
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AtomicI64, CloudDestination, OneShotReservation, Ordering, normalize_prefix, oneshot_pool,
        take_from,
    };
    /// The RELEASE half: a finished reservation must return its bytes, or the pool drains for the life of the process and every later part silently streams.
    #[test]
    fn a_finished_reservation_returns_its_bytes_so_the_next_part_can_one_shot() {
        let pool = AtomicI64::new(8 * 1024 * 1024);
        let whole = 8 * 1024 * 1024;

        assert!(
            take_from(&pool, whole),
            "the whole budget is available to start with"
        );
        {
            let _held = OneShotReservation(&pool, whole);
            assert!(
                !take_from(&pool, whole),
                "while the whole budget is held, a second part of the same size must stream"
            );
        }
        assert!(
            take_from(&pool, whole),
            "after the first reservation finished, the next part must one-shot again"
        );
    }

    use crate::config::{DestinationConfig, DestinationType};
    use crate::destination::gcs::GcsBackend;

    #[test]
    fn normalize_prefix_appends_a_trailing_slash_only_when_needed() {
        assert_eq!(normalize_prefix("exports/mydata".into()), "exports/mydata/");
        assert_eq!(
            normalize_prefix("exports/mydata/".into()),
            "exports/mydata/"
        );
        assert_eq!(normalize_prefix(String::new()), "");
        assert_eq!(normalize_prefix("a".into()), "a/");
        assert_eq!(normalize_prefix("/lead/x/".into()), "lead/x/");
        assert_eq!(normalize_prefix("/lead/x".into()), "lead/x/");
        assert_eq!(
            normalize_prefix("/".into()),
            "",
            "a lone slash is the bucket ROOT, not a directory named empty"
        );
    }

    #[test]
    fn new_with_retries_zero_builds_no_retry_probe_destination() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Gcs,
            bucket: Some("rivet-fastfail-probe".into()),
            allow_anonymous: true,
            endpoint: Some("http://127.0.0.1:4443".into()),
            ..Default::default()
        };
        CloudDestination::<GcsBackend>::new_with_retries(&cfg, 0)
            .expect("no-retry probe destination must build");
    }

    /// A GCS destination config with the given budget (no network is touched).
    fn gcs_cfg(budget_mb: Option<u64>) -> DestinationConfig {
        DestinationConfig {
            destination_type: DestinationType::Gcs,
            bucket: Some("rivet-oneshot-pool".into()),
            allow_anonymous: true,
            endpoint: Some("http://127.0.0.1:4443".into()),
            oneshot_budget_mb: budget_mb,
            ..Default::default()
        }
    }

    #[test]
    fn an_unset_budget_is_the_64mb_pool_and_a_value_sizes_its_own() {
        assert!(
            std::ptr::eq(
                oneshot_pool(&gcs_cfg(None)),
                oneshot_pool(&gcs_cfg(Some(64)))
            ),
            "unset must be the historical process-wide 64 MB pool"
        );
        assert_eq!(oneshot_pool(&gcs_cfg(Some(0))).load(Ordering::Relaxed), 0);
        assert_eq!(
            oneshot_pool(&gcs_cfg(Some(131))).load(Ordering::Relaxed),
            131 * 1024 * 1024
        );
    }

    #[test]
    fn configured_budget_moves_the_oneshot_switch_point() {
        let part = 40 * 1024 * 1024;
        let small =
            CloudDestination::<GcsBackend>::new_with_retries(&gcs_cfg(Some(33)), 0).unwrap();
        assert!(
            small.reserve_oneshot(part).is_none(),
            "a 40 MB part streams under a 33 MB budget"
        );
        let big = CloudDestination::<GcsBackend>::new_with_retries(&gcs_cfg(Some(129)), 0).unwrap();
        assert!(
            big.reserve_oneshot(part).is_some(),
            "the same part one-shots under a 129 MB budget"
        );
    }

    /// A refused reservation must not touch the pool.
    #[test]
    fn a_refused_reservation_leaves_the_pool_unchanged() {
        let dest = CloudDestination::<GcsBackend>::new_with_retries(&gcs_cfg(Some(71)), 0).unwrap();
        assert!(dest.reserve_oneshot(72 * 1024 * 1024).is_none());
        assert_eq!(
            oneshot_pool(&gcs_cfg(Some(71))).load(Ordering::Relaxed),
            71 * 1024 * 1024,
            "a part that streams must not grow the one-shot budget"
        );
    }

    /// One CDC export builds a destination per table.
    #[test]
    fn destinations_with_the_same_budget_share_one_pool() {
        let whole = 67 * 1024 * 1024;
        let a = CloudDestination::<GcsBackend>::new_with_retries(&gcs_cfg(Some(67)), 0).unwrap();
        let b = CloudDestination::<GcsBackend>::new_with_retries(&gcs_cfg(Some(67)), 0).unwrap();
        let other =
            CloudDestination::<GcsBackend>::new_with_retries(&gcs_cfg(Some(68)), 0).unwrap();

        let _held = a
            .reserve_oneshot(whole)
            .expect("a takes the whole 67 MB pool");
        assert!(
            b.reserve_oneshot(1).is_none(),
            "b shares a's drained pool: the budget must not multiply per destination"
        );
        assert!(
            other.reserve_oneshot(whole).is_some(),
            "a different budget value is a different pool"
        );
    }

    #[test]
    fn oneshot_budget_reserves_until_exhausted_then_streams() {
        let budget = AtomicI64::new(100);
        assert!(take_from(&budget, 60), "first fits");
        assert!(take_from(&budget, 30), "second fits (10 left)");
        assert!(!take_from(&budget, 30), "third overdraws → stream");
        assert_eq!(
            budget.load(Ordering::Relaxed),
            10,
            "budget intact after overdraw"
        );
        budget.fetch_add(60, Ordering::Relaxed);
        assert!(take_from(&budget, 30), "fits again after release");
    }

    #[test]
    fn part_larger_than_whole_budget_never_reserves() {
        let budget = AtomicI64::new(64);
        assert!(
            !take_from(&budget, 1_000),
            "part bigger than budget streams"
        );
        assert_eq!(budget.load(Ordering::Relaxed), 64, "budget untouched");
    }

    /// The retry interceptor must COUNT every transient retry it absorbs.
    #[test]
    fn retry_interceptor_counts_every_absorbed_retry() {
        use super::{RivetRetryNotify, transient_retries_total};
        use opendal::layers::RetryInterceptor as _;
        use std::time::Duration;
        let before = transient_retries_total();
        let err = opendal::Error::new(opendal::ErrorKind::Unexpected, "transient blip");
        RivetRetryNotify.intercept(&err, Duration::from_millis(200));
        RivetRetryNotify.intercept(&err, Duration::from_millis(400));
        assert!(
            transient_retries_total() >= before + 2,
            "both retries must be counted"
        );
    }

    /// The summary line must state what was MEASURED — retry ATTEMPTS — and must never claim the retries recovered.
    #[test]
    fn transient_retries_line_reports_attempts_and_never_claims_recovery() {
        use super::transient_retries_summary;
        assert_eq!(
            transient_retries_summary(0),
            None,
            "no row at all when the destination never retried"
        );
        for n in [1u64, 7] {
            let line = transient_retries_summary(n).expect("a row once retries > 0");
            assert!(
                line.contains(&format!("{n} transient retry attempts")),
                "line must report the attempt COUNT: {line:?}"
            );
            assert!(
                !line.to_ascii_lowercase().contains("recover"),
                "line must not claim recovery — the interceptor cannot observe \
                 the outcome of the attempt it counted: {line:?}"
            );
            assert!(
                line.contains("RUST_LOG=debug"),
                "line must still point at the per-attempt detail: {line:?}"
            );
        }
    }

    /// A store that answers every object stat with `stat_status` and every listing with `list_status`, recording each listing's request target.
    fn stub_store(
        stat_status: u16,
        list_status: u16,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        stub_store_answering(stat_status, &[list_status])
    }

    /// A store answering `stat_status` to every key and `list_statuses` to listings in order, the last one from then on.
    fn stub_store_answering(
        stat_status: u16,
        list_statuses: &[u16],
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use std::io::{Read, Write};
        let mut list_statuses: std::collections::VecDeque<u16> =
            list_statuses.iter().copied().collect();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let listings = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = std::sync::Arc::clone(&listings);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut head = [0u8; 4096];
                let n = stream.read(&mut head).unwrap_or(0);
                let request = String::from_utf8_lossy(&head[..n]).into_owned();
                let target = request.split_whitespace().nth(1).unwrap_or("");
                let is_listing = target.contains("/o?") && !target.starts_with("/upload/");
                let (status, body) = if is_listing {
                    seen.lock().unwrap().push(target.to_string());
                    let status = list_statuses.front().copied().unwrap_or(stat_status);
                    if list_statuses.len() > 1 {
                        list_statuses.pop_front();
                    }
                    (
                        status,
                        r#"{"items":[{"name":"p/part.parquet","size":"5","md5Hash":"bWQ1","etag":"e","updated":"2026-10-16T00:00:00.000Z","contentType":"application/octet-stream"}],"prefixes":["p/dir/"]}"#,
                    )
                } else {
                    (stat_status, "{}")
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        (endpoint, listings)
    }

    fn stub_destination(endpoint: &str) -> CloudDestination<GcsBackend> {
        CloudDestination::<GcsBackend>::new_with_retries(
            &DestinationConfig {
                destination_type: DestinationType::Gcs,
                bucket: Some("exports".into()),
                prefix: Some("p".into()),
                allow_anonymous: true,
                endpoint: Some(endpoint.into()),
                ..Default::default()
            },
            0,
        )
        .unwrap()
    }

    /// A write the store answers 404 is the missing bucket's coded error when the bucket is absent, and the store's own error otherwise.
    #[test]
    fn a_write_into_a_missing_container_is_refused_by_code() {
        use crate::destination::Destination;
        let part = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(part.path(), b"bytes").unwrap();
        let (endpoint, _) = stub_store(404, 404);
        let err = stub_destination(&endpoint)
            .write(part.path(), "part.parquet")
            .expect_err("the bucket is absent");
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_DEST_CONTAINER_NOT_FOUND")
        );
        assert!(err.to_string().starts_with("gs://exports:"), "{err:#}");

        let (endpoint, listings) = stub_store(404, 200);
        let err = stub_destination(&endpoint)
            .write(part.path(), "part.parquet")
            .expect_err("the store refused the write");
        assert_eq!(crate::error::error_code(&err), None, "{err:#}");
        assert_eq!(
            listings.lock().unwrap().len(),
            1,
            "the 404 asked about the bucket"
        );

        let (endpoint, listings) = stub_store(500, 404);
        let err = stub_destination(&endpoint)
            .write(part.path(), "part.parquet")
            .expect_err("the store failed the write");
        assert_eq!(crate::error::error_code(&err), None, "{err:#}");
        assert!(
            listings.lock().unwrap().is_empty(),
            "only a 404 asks about the bucket"
        );
    }

    /// A container that answered once may be gone by the next failed operation.
    #[test]
    fn an_operation_that_fails_after_the_container_once_answered_asks_the_store_again() {
        use crate::destination::Destination;
        let part = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(part.path(), b"bytes").unwrap();
        let (endpoint, listings) = stub_store_answering(404, &[200, 404]);
        let dest = stub_destination(&endpoint);
        assert_eq!(
            dest.head("manifest.json").unwrap(),
            None,
            "the container answered"
        );
        assert_eq!(listings.lock().unwrap().len(), 1);
        for (what, err) in [
            (
                "write",
                dest.write(part.path(), "part.parquet")
                    .map(|_| ())
                    .unwrap_err(),
            ),
            ("list", dest.list_prefix("").map(|_| ()).unwrap_err()),
            ("read", dest.read("part.parquet").map(|_| ()).unwrap_err()),
        ] {
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_DEST_CONTAINER_NOT_FOUND"),
                "{what}: {err:#}"
            );
            assert_eq!(
                crate::error::retry::classify_error(&err),
                crate::error::retry::RetryClass::Permanent,
                "{what}"
            );
        }
        assert_eq!(
            listings.lock().unwrap().len(),
            1 + 1 + 2 + 1,
            "the write and the read each asked again; the listing failed and asked again"
        );
        assert!(
            dest.remove("part.parquet").is_ok(),
            "a delete the store answers 404 is complete by opendal's contract: nothing to classify"
        );
    }

    /// A listing walks the prefix recursively (no delimiter), under the configured prefix as it is, and hands back files only, keys relative to that prefix.
    #[test]
    fn a_listing_is_recursive_under_the_prefix_and_hands_back_relative_file_keys() {
        use crate::destination::Destination;
        let (endpoint, listings) = stub_store(404, 200);
        let dest = stub_destination(&endpoint);
        let listed = dest.list_prefix("").unwrap();
        let asked = listings.lock().unwrap().clone();
        assert_eq!(asked.len(), 1, "{asked:?}");
        assert!(
            asked[0].contains("prefix=p%2F&") || asked[0].ends_with("prefix=p%2F"),
            "the configured prefix as it is, one slash: {asked:?}"
        );
        assert!(!asked[0].contains("delimiter"), "recursive: {asked:?}");
        assert_eq!(
            listed.len(),
            1,
            "the directory entry is not an object: {listed:?}"
        );
        assert_eq!(listed[0].key, "part.parquet");
        assert_eq!(listed[0].size_bytes, 5);
        assert_eq!(listed[0].content_md5.as_deref(), Some("bWQ1"));
        let under = dest.list_prefix("dir").unwrap();
        let asked = listings.lock().unwrap().clone();
        assert!(
            asked[1].contains("prefix=p%2Fdir%2F") && !asked[1].contains("delimiter"),
            "a sub-prefix gets its slash and is walked recursively too: {asked:?}"
        );
        assert_eq!(under.len(), 1);
    }

    /// `head` answers "absent" only for a key: a bucket that does not exist is an error with its own code.
    #[test]
    fn head_refuses_a_missing_container_and_reads_a_missing_key_as_absent() {
        use crate::destination::Destination;
        let (endpoint, _) = stub_store(404, 404);
        let err = stub_destination(&endpoint)
            .head("manifest.json")
            .expect_err("a 404 listing means the bucket is absent");
        assert_eq!(
            err.to_string(),
            "gs://exports: the bucket or container does not exist (the store answered 404 to a \
             listing of it). Create it, or correct `destination.bucket`"
        );
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_DEST_CONTAINER_NOT_FOUND")
        );
        assert_eq!(crate::error::classify_exit(&err), 1);

        let (endpoint, listings) = stub_store(404, 200);
        let dest = stub_destination(&endpoint);
        assert_eq!(dest.head("manifest.json").unwrap(), None);
        assert_eq!(dest.head("_SUCCESS").unwrap(), None);
        let asked = listings.lock().unwrap().clone();
        assert_eq!(
            asked.len(),
            1,
            "the container is asked about once per destination: {asked:?}"
        );
        assert!(
            asked[0].contains("maxResults=1") && asked[0].contains("prefix=p%2F"),
            "the probe asks for one result under the prefix: {asked:?}"
        );

        let (endpoint, listings) = stub_store(403, 404);
        stub_destination(&endpoint)
            .head("manifest.json")
            .expect_err("a stat the store refuses is an error, never an absent key");
        assert!(
            listings.lock().unwrap().is_empty(),
            "only a 404 on the key asks about the container"
        );

        let (endpoint, _) = stub_store(404, 403);
        assert_eq!(
            stub_destination(&endpoint).head("manifest.json").unwrap(),
            None,
            "a listing the principal may not make says nothing about the bucket"
        );
    }

    /// Which listing errors mean an absent container is the backend's to say: S3's `NoSuchBucket` is `ConfigInvalid`.
    #[test]
    fn each_backend_names_the_listing_errors_of_an_absent_container() {
        use super::listing_says_container_absent as absent;
        use crate::destination::azure::AzureBackend;
        use crate::destination::s3::S3Backend;
        use opendal::ErrorKind::{ConfigInvalid, NotFound, PermissionDenied};
        assert!(absent::<GcsBackend>(Some(NotFound)));
        assert!(absent::<AzureBackend>(Some(NotFound)));
        assert!(absent::<S3Backend>(Some(NotFound)));
        assert!(absent::<S3Backend>(Some(ConfigInvalid)));
        assert!(!absent::<GcsBackend>(Some(ConfigInvalid)));
        assert!(!absent::<AzureBackend>(Some(ConfigInvalid)));
        assert!(!absent::<S3Backend>(Some(PermissionDenied)));
        assert!(!absent::<S3Backend>(None));
    }

    /// A write is probed for an absent container when an error in its chain, or the opendal error a std-writer `io::Error` wraps, has one of the backend's absent-container kinds.
    #[test]
    fn a_failed_write_is_probed_by_the_backends_absent_container_kinds() {
        use super::write_error_may_mean_absent_container as probed;
        use crate::destination::azure::AzureBackend;
        use crate::destination::s3::S3Backend;
        use opendal::ErrorKind::{ConfigInvalid, NotFound, PermissionDenied};
        let op = |k| anyhow::Error::new(opendal::Error::new(k, "write")).context("upload");
        let wrapped = |k| {
            anyhow::Error::new(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                opendal::Error::new(k, "write"),
            ))
        };
        assert!(
            probed::<S3Backend>(&op(ConfigInvalid)),
            "S3 NoSuchBucket on a one-shot PUT"
        );
        assert!(
            probed::<S3Backend>(&wrapped(ConfigInvalid)),
            "S3 NoSuchBucket on a streamed part"
        );
        assert!(probed::<GcsBackend>(&op(NotFound)));
        assert!(probed::<AzureBackend>(&wrapped(NotFound)));
        assert!(!probed::<GcsBackend>(&op(ConfigInvalid)));
        assert!(!probed::<AzureBackend>(&wrapped(PermissionDenied)));
        let local = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::NotFound));
        assert!(
            !probed::<GcsBackend>(&local),
            "a missing LOCAL part file is not a store answer"
        );
    }
}

#[cfg(test)]
mod error_code_tests {
    #[test]
    fn a_runtime_the_os_refuses_is_reported_by_code() {
        let err = super::io_runtime_unavailable(std::io::Error::other(
            "Resource temporarily unavailable",
        ));
        crate::error::assert_coded(&err, "RIVET_DEST_IO_RUNTIME_UNAVAILABLE", 1);
    }
}
