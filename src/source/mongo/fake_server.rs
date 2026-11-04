//! A scripted in-process MongoDB server: the offline stand for the reads that name a collection by its listing.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use mongodb::bson::{Binary, Bson, Document, doc, spec::BinarySubtype};

/// The OP_MSG opcode of a wire message header.
const OP_MSG: i32 = 2013;

/// A hook run on every `find` before it is answered.
type OnFind = Box<dyn FnMut(&mut Shelf, &Document) + Send>;

/// What the server lists and finds right now.
#[derive(Default)]
pub(super) struct Shelf {
    pub uuid: Option<Vec<u8>>,
    pub docs: Vec<Document>,
    pub listing_denied: bool,
    pub finds: usize,
    pub on_find: Option<OnFind>,
}

/// A server on a loopback port, answering from a [`Shelf`].
pub(super) struct FakeMongo {
    port: u16,
    pub shelf: Arc<Mutex<Shelf>>,
}

/// One whole wire message from `from`.
fn read_message(from: &mut TcpStream) -> Option<Vec<u8>> {
    let mut head = [0u8; 4];
    from.read_exact(&mut head).ok()?;
    let len = i32::from_le_bytes(head) as usize;
    let mut message = head.to_vec();
    message.resize(len, 0);
    from.read_exact(&mut message[4..]).ok()?;
    Some(message)
}

/// The command document of an OP_MSG request.
fn command_of(message: &[u8]) -> Option<Document> {
    let opcode = i32::from_le_bytes(message.get(12..16)?.try_into().ok()?);
    (opcode == OP_MSG && *message.get(20)? == 0)
        .then(|| Document::from_reader(&mut &message[21..]).ok())
        .flatten()
}

/// An OP_MSG reply carrying `body`, answering request `to`.
fn reply_message(to: i32, body: &Document) -> Vec<u8> {
    let mut payload = vec![0u8; 4];
    payload.push(0);
    body.to_writer(&mut payload).expect("serialise a reply");
    let mut message = Vec::with_capacity(16 + payload.len());
    message.extend_from_slice(&((16 + payload.len()) as i32).to_le_bytes());
    message.extend_from_slice(&1i32.to_le_bytes());
    message.extend_from_slice(&to.to_le_bytes());
    message.extend_from_slice(&OP_MSG.to_le_bytes());
    message.extend_from_slice(&payload);
    message
}

/// The reply the shelf gives `command`.
fn answer(shelf: &mut Shelf, db: &str, command: &Document) -> Document {
    let first = command.keys().next().map(String::as_str).unwrap_or("");
    match first.to_ascii_lowercase().as_str() {
        "ismaster" | "hello" => doc! {
            "ismaster": true, "isWritablePrimary": true, "helloOk": true,
            "maxBsonObjectSize": 16_777_216, "maxMessageSizeBytes": 48_000_000,
            "maxWriteBatchSize": 100_000, "minWireVersion": 0, "maxWireVersion": 17, "ok": 1.0,
        },
        "listcollections" if shelf.listing_denied => doc! {
            "ok": 0.0, "errmsg": "not authorized on db to execute command listCollections",
            "code": 13, "codeName": "Unauthorized",
        },
        "listcollections" => {
            let name = command
                .get_document("filter")
                .and_then(|f| f.get_str("name"))
                .unwrap_or("t")
                .to_string();
            let rows: Vec<Bson> = shelf
                .uuid
                .iter()
                .map(|bytes| {
                    let uuid = Binary {
                        subtype: BinarySubtype::Uuid,
                        bytes: bytes.clone(),
                    };
                    Bson::Document(
                        doc! { "name": name.clone(), "type": "collection", "info": { "uuid": uuid } },
                    )
                })
                .collect();
            doc! { "cursor": { "id": 0i64, "ns": format!("{db}.$cmd.listCollections"), "firstBatch": rows }, "ok": 1.0 }
        }
        "find" => {
            shelf.finds += 1;
            if let Some(mut hook) = shelf.on_find.take() {
                hook(shelf, command);
                shelf.on_find = Some(hook);
            }
            let rows: Vec<Bson> = shelf.docs.iter().cloned().map(Bson::Document).collect();
            let coll = command.get_str("find").unwrap_or("t").to_string();
            doc! { "cursor": { "id": 0i64, "ns": format!("{db}.{coll}"), "firstBatch": rows }, "ok": 1.0 }
        }
        _ => doc! { "ok": 1.0 },
    }
}

/// Serve one client connection until it closes.
fn serve(mut client: TcpStream, shelf: Arc<Mutex<Shelf>>) {
    while let Some(message) = read_message(&mut client) {
        let request_id = i32::from_le_bytes(message[4..8].try_into().expect("request id"));
        let Some(command) = command_of(&message) else {
            continue;
        };
        let db = command.get_str("$db").unwrap_or("db").to_string();
        let body = answer(&mut shelf.lock().unwrap(), &db, &command);
        if client.write_all(&reply_message(request_id, &body)).is_err() {
            break;
        }
    }
}

impl FakeMongo {
    /// A server listing one collection under `uuid` that finds `docs`.
    pub fn serving(uuid: &[u8], docs: Vec<Document>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the fake server");
        let port = listener.local_addr().expect("fake server address").port();
        let shelf = Arc::new(Mutex::new(Shelf {
            uuid: Some(uuid.to_vec()),
            docs,
            ..Shelf::default()
        }));
        let accepting = shelf.clone();
        std::thread::spawn(move || {
            for client in listener.incoming().flatten() {
                let shelf = accepting.clone();
                std::thread::spawn(move || serve(client, shelf));
            }
        });
        Self { port, shelf }
    }

    /// The connection URL of database `db` on this server.
    pub fn url(&self, db: &str) -> String {
        format!(
            "mongodb://127.0.0.1:{}/{db}?directConnection=true&serverSelectionTimeoutMS=5000",
            self.port
        )
    }
}
