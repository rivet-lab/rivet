//! A loopback TCP forwarder in front of a stand service: the same server at another address.

use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

/// Relays every connection accepted on `addr()` to the upstream until dropped.
pub struct TcpForward {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
}

impl TcpForward {
    /// Listen on 127.0.0.1 at a free port and relay to `upstream`.
    pub fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind the forwarder");
        let addr = listener.local_addr().expect("forwarder address");
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        std::thread::spawn(move || {
            for client in listener.incoming().flatten() {
                if stopped.load(SeqCst) {
                    break;
                }
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
        TcpForward { addr, stop }
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

/// `url` with its `host:port` replaced by a forwarder to it on another loopback port: the same server under another source key.
pub fn respelled_source(url: &str) -> (TcpForward, String) {
    let (scheme, rest) = url.split_once("://").expect("a URL with a scheme");
    let end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    let (userinfo, host_port) = authority
        .rsplit_once('@')
        .map_or(("", authority), |(u, h)| (u, h));
    let upstream = host_port
        .to_socket_addrs()
        .expect("the URL names host:port")
        .next()
        .expect("the host resolves");
    let forward = TcpForward::start(upstream);
    let at = if userinfo.is_empty() {
        String::new()
    } else {
        format!("{userinfo}@")
    };
    let respelled = format!("{scheme}://{at}{}{tail}", forward.addr);
    (forward, respelled)
}
