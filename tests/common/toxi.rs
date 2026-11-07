//! Toxiproxy admin client + cross-binary serialisation guard.
//!
//! We avoid pulling in `reqwest` for the admin path because (a) it is already a
//! dependency but its blocking client would spin up a tokio runtime per call
//! and (b) our needs are tiny — create/delete a proxy, add a toxic, remove it.
//! A raw HTTP/1.0 writer + one-shot TcpStream read is plenty.

#![allow(dead_code)]

use std::net::TcpStream;

use super::env::{LiveService, require_alive};
use super::unique_name;

/// Execute a one-shot HTTP/1.0 request against the Toxiproxy admin API and
/// return the (status_code, body) pair.  Panics on IO errors — this is a
/// test harness, not production code.
fn toxi_admin(method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    use std::io::{Read, Write};
    let mut s = TcpStream::connect("127.0.0.1:8474").expect("connect toxiproxy api");
    let body_bytes = body.unwrap_or("").as_bytes();
    let mut req = format!(
        "{method} {path} HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body_bytes.len()
    )
    .into_bytes();
    req.extend_from_slice(body_bytes);
    s.write_all(&req).expect("write toxi request");
    let mut raw = String::new();
    let _ = s.read_to_string(&mut raw);
    // Parse status line + body split at the first blank line.
    let status: u16 = raw
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, rest)| rest)
        .unwrap_or("")
        .to_string();
    (status, body)
}

/// Ensure that Toxiproxy has a proxy named `name` forwarding `listen_port` to
/// `upstream` (e.g. `"postgres:5432"`).  If the proxy already exists it is
/// left in place (idempotent).  Returns when the proxy is ready to accept
/// traffic.
///
/// Toxiproxy starts with an empty runtime configuration; without this helper
/// every live test routed through :15432 / :13306 hits a reset connection.
pub fn ensure_toxi_proxy(name: &str, listen_port: u16, upstream: &str) {
    require_alive(LiveService::Toxiproxy);
    let payload = format!(
        r#"{{"name":"{name}","listen":"0.0.0.0:{listen_port}","upstream":"{upstream}","enabled":true}}"#
    );
    let (code, body) = toxi_admin("POST", "/proxies", Some(&payload));
    // 201 Created = first time. 409 Conflict or 500-with-existing = already
    // present from an earlier test (toxiproxy does not expose a native
    // "create or update", so we treat both as success).
    assert!(
        code == 201 || code == 409 || body.contains("already exists"),
        "toxiproxy POST /proxies for '{name}' returned unexpected status {code}: {body}"
    );
}

/// Remove every toxic from the named proxy (does NOT delete the proxy itself).
/// Use at the start of a test to reset state left by a previous flaky run.
pub fn toxi_reset_toxics(name: &str) {
    require_alive(LiveService::Toxiproxy);
    let (code, body) = toxi_admin("GET", &format!("/proxies/{name}/toxics"), None);
    if code != 200 {
        return;
    }
    // Crude JSON parse: each toxic is preceded by a `"name":"..."` field.
    // Extract those names and DELETE each.
    for toxic_name in body
        .split("\"name\":\"")
        .skip(1)
        .filter_map(|chunk| chunk.split('"').next())
    {
        let _ = toxi_admin(
            "DELETE",
            &format!("/proxies/{name}/toxics/{toxic_name}"),
            None,
        );
    }
}

/// Add a latency toxic to a proxy: every byte in the downstream direction is
/// delayed by `latency_ms` milliseconds.  Returns the toxic name (so tests
/// can remove it explicitly); use `toxi_reset_toxics` for broad cleanup.
pub fn toxi_add_latency(proxy: &str, latency_ms: u64) -> String {
    let toxic_name = unique_name("rivet_latency");
    let payload = format!(
        r#"{{"name":"{toxic_name}","type":"latency","stream":"downstream","toxicity":1.0,"attributes":{{"latency":{latency_ms},"jitter":0}}}}"#
    );
    let (code, body) = toxi_admin("POST", &format!("/proxies/{proxy}/toxics"), Some(&payload));
    assert!(
        code == 200,
        "add latency to {proxy}: status {code}, body:\n{body}"
    );
    toxic_name
}

/// Cap the proxy's throughput at `rate_kbps` KB/s — the SHARED-LINK simulator
/// (#166 GA): the client field run showed 60 concurrent exports moving ~0.25
/// MB/s AGGREGATE over one tunnel, i.e. per-connection parallelism split one
/// ceiling instead of adding throughput. A pool run through this toxic is the
/// honest test of the LPT model on a bandwidth-bound source: it must still
/// complete correctly, and its makespan claim must not be trusted blindly.
/// `stream`: "downstream" caps server→client (query results — the read leg).
pub fn toxi_add_bandwidth(proxy: &str, rate_kbps: u64, stream: &str) -> String {
    let toxic_name = unique_name("rivet_bw");
    let payload = format!(
        r#"{{"name":"{toxic_name}","type":"bandwidth","stream":"{stream}","toxicity":1.0,"attributes":{{"rate":{rate_kbps}}}}}"#
    );
    let (code, body) = toxi_admin("POST", &format!("/proxies/{proxy}/toxics"), Some(&payload));
    assert!(
        code == 200,
        "add bandwidth to {proxy}: status {code}, body:\n{body}"
    );
    toxic_name
}

/// Cut the connection after `bytes` of downstream data — a DETERMINISTIC
/// mid-stream network failure (unlike latency/timeout, the cut point does not
/// depend on timing). Simulates a binlog/replication connection dying partway
/// through a drain, or an upload dying partway through a part.
/// `stream`: "downstream" cuts server→client (a binlog/replication read);
/// "upstream" cuts client→server (an upload). NOTE: the byte budget is
/// PER-CONNECTION — pick a limit smaller than a single transfer if the client
/// may open a fresh connection per request.
pub fn toxi_add_limit_data(proxy: &str, bytes: u64, stream: &str) -> String {
    let toxic_name = unique_name("rivet_limit");
    let payload = format!(
        r#"{{"name":"{toxic_name}","type":"limit_data","stream":"{stream}","toxicity":1.0,"attributes":{{"bytes":{bytes}}}}}"#
    );
    let (code, body) = toxi_admin("POST", &format!("/proxies/{proxy}/toxics"), Some(&payload));
    assert!(
        code == 200,
        "add limit_data to {proxy}: status {code}, body:\n{body}"
    );
    toxic_name
}

/// Reset every connection through `proxy` (TCP RST) on its next `stream` data, until the toxic is removed.
pub fn toxi_add_reset_peer(proxy: &str, stream: &str) -> String {
    let toxic_name = unique_name("rivet_reset");
    let payload = format!(
        r#"{{"name":"{toxic_name}","type":"reset_peer","stream":"{stream}","toxicity":1.0,"attributes":{{"timeout":0}}}}"#
    );
    let (code, body) = toxi_admin("POST", &format!("/proxies/{proxy}/toxics"), Some(&payload));
    assert!(
        code == 200,
        "add reset_peer to {proxy}: status {code}, body:\n{body}"
    );
    toxic_name
}

/// Remove one toxic by the name its `toxi_add_*` returned.
pub fn toxi_remove_toxic(proxy: &str, toxic: &str) {
    let (code, body) = toxi_admin("DELETE", &format!("/proxies/{proxy}/toxics/{toxic}"), None);
    assert!(
        code == 204,
        "remove toxic {toxic} from {proxy}: status {code}, body:\n{body}"
    );
}

/// Once the paused run raises `marker`, reset every connection through `proxy` until `lifted()` holds, then remove the toxic; false when the run never paused.
pub fn reset_peer_until(
    proxy: &str,
    marker: &std::path::Path,
    mut lifted: impl FnMut() -> bool,
) -> bool {
    let limit = std::time::Duration::from_secs(90);
    let nap = std::time::Duration::from_millis(25);
    let t0 = std::time::Instant::now();
    while !marker.exists() {
        if t0.elapsed() > limit {
            return false;
        }
        std::thread::sleep(nap);
    }
    let toxic = toxi_add_reset_peer(proxy, "downstream");
    let t1 = std::time::Instant::now();
    while !lifted() && t1.elapsed() < limit {
        std::thread::sleep(nap);
    }
    toxi_remove_toxic(proxy, &toxic);
    true
}

/// Disable the proxy — every open connection is closed and new ones refused
/// until re-enabled.  Simulates a hard "database went away" event.
pub fn toxi_disable(proxy: &str) {
    let payload = r#"{"enabled":false}"#;
    let (code, body) = toxi_admin("POST", &format!("/proxies/{proxy}"), Some(payload));
    assert!(code == 200, "disable {proxy}: status {code}, body:\n{body}");
}

/// Re-enable a disabled proxy.
pub fn toxi_enable(proxy: &str) {
    let payload = r#"{"enabled":true}"#;
    let (code, body) = toxi_admin("POST", &format!("/proxies/{proxy}"), Some(payload));
    assert!(code == 200, "enable {proxy}: status {code}, body:\n{body}");
}

/// RAII cross-process lock on the Toxiproxy admin API, released by an explicit unlock.
pub struct ToxiproxyGuard {
    file: std::fs::File,
}

impl Drop for ToxiproxyGuard {
    /// Unlock the flock itself: a child forked while it was held keeps a copy of the descriptor.
    fn drop(&mut self) {
        use std::os::unix::io::AsRawFd;
        // SAFETY: `file` owns the fd; `flock` does not transfer ownership.
        unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

/// Take the suite-wide `flock(2)` on a well-known temp file, blocking until it is free (Unix only).
pub fn toxiproxy_guard() -> ToxiproxyGuard {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("rivet_qa_toxiproxy.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .unwrap_or_else(|e| panic!("open toxiproxy lock {}: {e}", path.display()));
    // SAFETY: `file` owns the fd; `flock` does not transfer ownership.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if rc != 0 {
        panic!(
            "flock(LOCK_EX) on {} failed: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }
    ToxiproxyGuard { file }
}

/// Run `rig` with a byte-budget cut armed on `proxy`, lift the cut once the first retry starts, and grade exactly-once delivery.
pub fn assert_midrow_cut_retried_and_delivered_exactly_once(
    rig: &super::Rig,
    export: &str,
    proxy: &str,
    cut_after_bytes: u64,
    rows: i64,
    pad_bytes: usize,
    out_container: &str,
) {
    use std::time::{Duration, Instant};

    toxi_add_limit_data(proxy, cut_after_bytes, "downstream");
    let mut run = rig.spawn_args_env(&["--export", export], &[]);
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        if run.stderr_so_far().contains("retry 1/") {
            toxi_reset_toxics(proxy);
            break;
        }
        if run.try_wait().expect("poll the run").is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let out_run = run.wait_with_output().expect("reap the run");
    toxi_reset_toxics(proxy);

    let stderr = String::from_utf8_lossy(&out_run.stderr);
    assert!(
        out_run.status.success(),
        "a connection cut mid-row must be retried and the run must exit 0; stderr:\n{stderr}"
    );
    let state = super::StateDb::next_to_config(&rig.config_path());
    let metrics = state.metrics_row(&state.latest_run_id(export));
    assert!(
        metrics.retries.unwrap_or(0) >= 1,
        "export_metrics.retries must record the retry; got {:?}; stderr:\n{stderr}",
        metrics.retries
    );
    super::duckdb_declared_assert_complete(
        out_container,
        "id",
        rows,
        "a retry across a connection cut in the middle of a row",
    );
    assert_eq!(
        super::duckdb_declared_distinct_set(out_container, "id"),
        (1..=rows).map(|i| i.to_string()).collect(),
        "the delivered id set must equal the source id set"
    );
    let intact = super::duckdb_run_sql_json(&format!(
        "SELECT count(*) FROM read_parquet('{out_container}/**/*.parquet') \
         WHERE length(pad) = {pad_bytes}"
    ));
    assert_eq!(
        intact["rows"][0][0].as_str(),
        Some(rows.to_string().as_str()),
        "every delivered pad must be whole"
    );
}
