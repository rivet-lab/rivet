//! A loopback TCP forwarder in front of a stand service: the same server at another address.

use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

/// Relays every connection accepted on `addr()` to the upstream until dropped.
pub struct TcpForward {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    accepted: Arc<AtomicUsize>,
}

impl TcpForward {
    /// `url` with its `host:port` replaced by this forwarder's address.
    pub fn respell(&self, url: &str) -> String {
        let (scheme, rest) = url.split_once("://").expect("a URL with a scheme");
        let end = rest.find(['/', '?']).unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(end);
        let at = authority
            .rsplit_once('@')
            .map_or(String::new(), |(u, _)| format!("{u}@"));
        format!("{scheme}://{at}{}{tail}", self.addr)
    }

    /// Listen on 127.0.0.1 at a free port and relay to `upstream`.
    pub fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind the forwarder");
        let addr = listener.local_addr().expect("forwarder address");
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = accepted.clone();
        std::thread::spawn(move || {
            for client in listener.incoming().flatten() {
                if stopped.load(SeqCst) {
                    break;
                }
                counted.fetch_add(1, SeqCst);
                let Ok(server) = TcpStream::connect(upstream) else {
                    continue;
                };
                relay(
                    client.try_clone().expect("clone"),
                    server.try_clone().expect("clone"),
                );
                relay(server, client);
            }
        });
        TcpForward {
            addr,
            stop,
            accepted,
        }
    }

    /// The connections this forwarder has accepted so far.
    pub fn accepted(&self) -> usize {
        self.accepted.load(SeqCst)
    }
}

impl Drop for TcpForward {
    /// Stop accepting: flag the loop and wake it with one connection.
    fn drop(&mut self) {
        self.stop.store(true, SeqCst);
        let _ = TcpStream::connect(self.addr);
    }
}

/// Copy `from` into `to` on its own thread, then close both directions.
fn relay(mut from: TcpStream, mut to: TcpStream) {
    std::thread::spawn(move || {
        let _ = std::io::copy(&mut from, &mut to);
        let _ = to.shutdown(Shutdown::Both);
        let _ = from.shutdown(Shutdown::Both);
    });
}

/// Source connections one `rivet run --config cfg` opens through `forward`; its exit is asserted, and nothing but the run is counted.
pub fn connections_opened_by_run(forward: &TcpForward, cfg: &std::path::Path) -> usize {
    let before = forward.accepted();
    let (_, out) = super::runner::output_of(&mut super::runner::rivet_command(
        &["run", "--config", cfg.to_str().unwrap()],
        &[],
    ));
    let opened = forward.accepted() - before;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    opened
}

/// `url` with its `host:port` replaced by a forwarder to it on another loopback port: the same server under another source key.
pub fn respelled_source(url: &str) -> (TcpForward, String) {
    let (_, rest) = url.split_once("://").expect("a URL with a scheme");
    let end = rest.find(['/', '?']).unwrap_or(rest.len());
    let authority = &rest[..end];
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let upstream = host_port
        .to_socket_addrs()
        .expect("the URL names host:port")
        .next()
        .expect("the host resolves");
    let forward = TcpForward::start(upstream);
    let respelled = forward.respell(url);
    (forward, respelled)
}
