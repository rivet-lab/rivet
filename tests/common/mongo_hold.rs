//! A wire proxy in front of the MongoDB stand that holds the commands of a live run until the test lets them go.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use mongodb::bson::Document;

/// The OP_MSG opcode of a wire message header.
const OP_MSG: i32 = 2013;

/// The longest a command is held before the proxy forwards it unasked.
const HELD_AT_MOST: Duration = Duration::from_secs(60);

#[derive(Default)]
struct Gate {
    armed: bool,
    held: usize,
    released: bool,
}

struct Shared {
    gate: Mutex<Gate>,
    changed: Condvar,
    stop: AtomicBool,
}

/// A proxy to `127.0.0.1:upstream` that holds every client command `pick` accepts until released.
pub struct WireHold {
    port: u16,
    shared: Arc<Shared>,
}

/// The command document of an OP_MSG message, `None` for any other message.
fn command_of(message: &[u8]) -> Option<Document> {
    let opcode = i32::from_le_bytes(message.get(12..16)?.try_into().ok()?);
    (opcode == OP_MSG && *message.get(20)? == 0)
        .then(|| Document::from_reader(&mut &message[21..]).ok())
        .flatten()
}

/// Read one whole wire message: its length prefix and the rest.
fn read_message(from: &mut TcpStream) -> Option<Vec<u8>> {
    let mut head = [0u8; 4];
    from.read_exact(&mut head).ok()?;
    let len = i32::from_le_bytes(head) as usize;
    let mut message = head.to_vec();
    message.resize(len, 0);
    from.read_exact(&mut message[4..]).ok()?;
    Some(message)
}

/// Forward the client's messages, holding each one `pick` accepts until [`WireHold::release`].
fn pump_client(
    mut client: TcpStream,
    mut server: TcpStream,
    shared: Arc<Shared>,
    pick: Arc<dyn Fn(&Document) -> bool + Send + Sync>,
) {
    while let Some(message) = read_message(&mut client) {
        let hit =
            shared.gate.lock().unwrap().armed && command_of(&message).is_some_and(|c| pick(&c));
        if hit {
            let mut gate = shared.gate.lock().unwrap();
            if !gate.released {
                gate.held += 1;
                shared.changed.notify_all();
                let _ = shared
                    .changed
                    .wait_timeout_while(gate, HELD_AT_MOST, |g| !g.released)
                    .unwrap();
            }
        }
        if server.write_all(&message).is_err() {
            break;
        }
    }
    let _ = server.shutdown(Shutdown::Both);
}

impl WireHold {
    /// A proxy in front of the stand on `upstream` that holds every command `pick` accepts.
    pub fn holding(
        upstream: u16,
        pick: impl Fn(&Document) -> bool + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the wire proxy");
        let port = listener.local_addr().expect("proxy address").port();
        let shared = Arc::new(Shared {
            gate: Mutex::new(Gate::default()),
            changed: Condvar::new(),
            stop: AtomicBool::new(false),
        });
        let pick: Arc<dyn Fn(&Document) -> bool + Send + Sync> = Arc::new(pick);
        let accepting = shared.clone();
        std::thread::spawn(move || {
            for client in listener.incoming().flatten() {
                if accepting.stop.load(Ordering::Relaxed) {
                    return;
                }
                let Ok(server) = TcpStream::connect(("127.0.0.1", upstream)) else {
                    continue;
                };
                let (mut back_from, mut back_to) =
                    (server.try_clone().unwrap(), client.try_clone().unwrap());
                std::thread::spawn(move || {
                    let _ = std::io::copy(&mut back_from, &mut back_to);
                    let _ = back_to.shutdown(Shutdown::Both);
                });
                let (shared, pick) = (accepting.clone(), pick.clone());
                std::thread::spawn(move || pump_client(client, server, shared, pick));
            }
        });
        Self { port, shared }
    }

    /// The port rivet connects to instead of the stand's.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Start holding: until then every command is forwarded.
    pub fn arm(&self) {
        self.shared.gate.lock().unwrap().armed = true;
    }

    /// Whether `n` commands are held.
    pub fn holds(&self, n: usize) -> bool {
        self.shared.gate.lock().unwrap().held >= n
    }

    /// Let every held command through, and any later one.
    pub fn release(&self) {
        self.shared.gate.lock().unwrap().released = true;
        self.shared.changed.notify_all();
    }
}

impl Drop for WireHold {
    /// Release anything held and unblock the accept loop.
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Relaxed);
        self.release();
        let _ = TcpStream::connect(("127.0.0.1", self.port));
    }
}

/// Whether `command` is a `find` whose `_id` filter carries `operator`.
fn find_with_id_operator(command: &Document, operator: &str) -> bool {
    command.contains_key("find")
        && command
            .get_document("filter")
            .and_then(|f| f.get_document("_id"))
            .is_ok_and(|id| id.contains_key(operator))
}

/// The `find` of a keyset page after the first (its filter has `_id: {$gt: ..}`).
pub fn a_later_keyset_page(command: &Document) -> bool {
    find_with_id_operator(command, "$gt")
}

/// The `find` of a full scan: no sort, no limit, no filter.
pub fn the_full_scan(command: &Document) -> bool {
    command.contains_key("find")
        && !command.contains_key("limit")
        && !command.contains_key("sort")
        && command.get_document("filter").is_ok_and(|f| f.is_empty())
}

/// The `find` of an `_id`-range worker (its filter has `_id: {$lt: ..}`).
pub fn an_id_range_worker(command: &Document) -> bool {
    find_with_id_operator(command, "$lt")
}
