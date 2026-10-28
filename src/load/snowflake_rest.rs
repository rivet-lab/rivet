//! The Snowflake **SQL API v2 transport**, in process.
//!
//! Everything the loader does is a SQL script, so one verb serves all of it:
//!
//! 1. `POST /api/v2/statements?requestId=<uuid>` — the script as one multi-statement request
//!    (`MULTI_STATEMENT_COUNT=0`); the session settings a script opens with (`ALTER SESSION`,
//!    `USE`) hold for the statements after them, within the request.
//! 2. `GET /api/v2/statements/<handle>` — poll a `202` until it is a `200` or an error.
//! 3. One `GET` per child handle of a multi-statement answer, and one per extra result partition.
//!
//! The answer is handed back as an array with one block per statement, because the loader
//! reads its counts BY NAME out of it: each block is an array of row objects keyed by column
//! name, a whole number as a JSON number.
//!
//! ## Auth
//!
//! Key-pair JWT (RS256). `iss` is `<ACCOUNT>.<USER>.SHA256:<base64 of the SHA-256 of the public
//! key's DER>`, `sub` is `<ACCOUNT>.<USER>`, both upper-cased; the token lives under an hour and
//! is minted again before it ends. The key file is read once, into memory that is zeroed on drop.
//!
//! ## Retries
//!
//! What is retried is decided by the HTTP status (429 and 5xx) or a failed transport — never by
//! the words of an answer. The `POST` is retried under the SAME `requestId` with `retry=true`, so
//! a statement that did land is not run twice: a duplicate `COPY INTO` is silent row inflation.
//! A `401` mints a fresh token and repeats the request once.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
use serde_json::{Map, Number, Value, json};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

#[cfg(not(test))]
use super::bq_rest::poll_interval;
use super::bq_rest::{is_transient_status, no_attempt_made, should_back_off};
use crate::config::snowflake_account::Account;
use crate::error::codes;

#[cfg(not(test))]
const HTTP_TIMEOUT: Duration = Duration::from_secs(120);
/// One second in the unit tests, where a request is watched reaching the ceiling.
#[cfg(test)]
const HTTP_TIMEOUT: Duration = Duration::from_secs(1);

/// How long to wait before poll or retry `attempt`: [`poll_interval`], a millisecond in the unit tests.
#[cfg(not(test))]
fn pause(attempt: u32) -> Duration {
    poll_interval(attempt)
}
#[cfg(test)]
fn pause(_attempt: u32) -> Duration {
    Duration::from_millis(1)
}

/// Consecutive transient failures tolerated on one request before the load gives up.
const MAX_TRANSIENT_RETRIES: u32 = 5;

/// How long the poll loop waits for ONE statement before giving up on it.
const STATEMENT_POLL_DEADLINE: Duration = Duration::from_secs(4 * 60 * 60);

/// How long a token lives. Snowflake refuses a JWT valid for more than an hour.
const JWT_LIFETIME_SECS: u64 = 55 * 60;

/// A token this close to its end is minted again before the next request.
const JWT_REFRESH_MARGIN_SECS: u64 = 5 * 60;

// ── the signer ───────────────────────────────────────────────────────────────

#[derive(serde::Serialize)]
struct Claims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
}

struct CachedToken {
    token: Zeroizing<String>,
    refresh_at: u64,
}

/// Mints and caches the key-pair JWT.
pub(crate) struct KeyPairSigner {
    subject: String,
    issuer: String,
    key: jsonwebtoken::EncodingKey,
    cached: Mutex<Option<CachedToken>>,
}

impl std::fmt::Debug for KeyPairSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyPairSigner")
            .field("subject", &self.subject)
            .finish_non_exhaustive()
    }
}

impl KeyPairSigner {
    /// A signer for `user` of `account` from an unencrypted PEM private key (PKCS#8 or PKCS#1).
    pub(crate) fn new(account: &Account, user: &str, pem: &Zeroizing<String>) -> Result<Self> {
        if pem.contains("ENCRYPTED") {
            return Err(crate::error::coded(
                codes::LOAD_WAREHOUSE_AUTH_FAILED,
                "the Snowflake private key is passphrase-protected and rivet reads only an \
                 unencrypted one — remove the passphrase with `openssl pkcs8 -topk8 -nocrypt \
                 -in <encrypted key> -out <unencrypted key>` and point `load.private_key_path` \
                 at the new file (keep it readable by its owner only)",
            ));
        }
        let unreadable = || {
            crate::error::coded(
                codes::LOAD_WAREHOUSE_AUTH_FAILED,
                "the Snowflake private key is not a PEM-encoded RSA private key — Snowflake \
                 key-pair auth takes a 2048-bit RSA key in PKCS#8 form (`BEGIN PRIVATE KEY`)",
            )
        };
        let key =
            jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).map_err(|_| unreadable())?;
        let fingerprint = public_key_fingerprint(pem).ok_or_else(unreadable)?;
        let qualified = format!("{}.{}", account.jwt_name, user.to_ascii_uppercase());
        Ok(Self {
            issuer: format!("{qualified}.{fingerprint}"),
            subject: qualified,
            key,
            cached: Mutex::new(None),
        })
    }

    fn claims(&self, now: u64) -> Claims {
        Claims {
            iss: self.issuer.clone(),
            sub: self.subject.clone(),
            iat: now,
            exp: now + JWT_LIFETIME_SECS,
        }
    }

    /// The token to send at `now` (unix seconds): the cached one until it nears its end.
    pub(crate) fn token(&self, now: u64) -> Result<Zeroizing<String>> {
        let mut cached = self.cached.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = cached.as_ref()
            && now < c.refresh_at
        {
            return Ok(c.token.clone());
        }
        let signed = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &self.claims(now),
            &self.key,
        )
        .map_err(|_| {
            crate::error::coded(
                codes::LOAD_WAREHOUSE_AUTH_FAILED,
                "signing the Snowflake JWT with the private key failed",
            )
        })?;
        let token = Zeroizing::new(signed);
        *cached = Some(CachedToken {
            token: token.clone(),
            refresh_at: now + JWT_LIFETIME_SECS - JWT_REFRESH_MARGIN_SECS,
        });
        Ok(token)
    }

    /// Drop the cached token, so the next request carries a fresh one.
    fn forget(&self) {
        *self.cached.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

/// `SHA256:<base64 of the SHA-256 of the public key's DER>` — how Snowflake names a registered key.
fn public_key_fingerprint(pem: &str) -> Option<String> {
    use base64::Engine as _;
    let private = rsa::RsaPrivateKey::from_pkcs8_pem(pem)
        .or_else(|_| rsa::RsaPrivateKey::from_pkcs1_pem(pem))
        .ok()?;
    let der = private.to_public_key().to_public_key_der().ok()?;
    let digest = Sha256::digest(der.as_bytes());
    Some(format!(
        "SHA256:{}",
        base64::engine::general_purpose::STANDARD.encode(digest)
    ))
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Read the key file once, into memory that is zeroed on drop.
pub(crate) fn read_key_file(path: &str) -> Result<Zeroizing<String>> {
    std::fs::read_to_string(path)
        .map(Zeroizing::new)
        .map_err(|e| {
            crate::error::coded(
                codes::LOAD_WAREHOUSE_AUTH_FAILED,
                format!(
                    "cannot read the Snowflake private key `{}`: {} — set `load.private_key_path` \
                 (or RIVET_SNOWFLAKE_KEY) to a readable PKCS#8 key file",
                    path.escape_default(),
                    e.kind()
                ),
            )
        })
}

// ── the client ───────────────────────────────────────────────────────────────

/// Where a statement runs, sent with every request.
#[derive(Debug, Clone, Default)]
pub(crate) struct Context {
    pub(crate) database: String,
    pub(crate) schema: String,
    pub(crate) role: Option<String>,
}

/// A blocking Snowflake SQL API client for one account, user and key.
#[derive(Clone)]
pub(crate) struct SnowflakeApi {
    endpoint: String,
    http: reqwest::blocking::Client,
    signer: Arc<KeyPairSigner>,
    context: Context,
}

impl std::fmt::Debug for SnowflakeApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // SecOps: never render `signer` — it holds the private key and the live token.
        f.debug_struct("SnowflakeApi")
            .field("endpoint", &self.endpoint)
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

/// The one Snowflake HTTP client of this process: every loader holds a clone of it.
fn shared_http() -> Result<reqwest::blocking::Client> {
    static HTTP: std::sync::OnceLock<reqwest::blocking::Client> = std::sync::OnceLock::new();
    if let Some(http) = HTTP.get() {
        return Ok(http.clone());
    }
    let built = reqwest::blocking::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .user_agent(concat!("rivet/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building the Snowflake HTTP client")?;
    Ok(HTTP.get_or_init(|| built).clone())
}

/// One answer of a retried request.
struct Reply {
    status: u16,
    body: String,
}

impl SnowflakeApi {
    /// A client of `user` at `account`, signing with the key file at `key_path`.
    pub(crate) fn connect(
        account: &str,
        user: &str,
        key_path: &str,
        context: Context,
    ) -> Result<Self> {
        let account = Account::parse(account).map_err(|m| {
            crate::error::coded(
                codes::CONFIG_LOAD_BLOCK_INVALID,
                format!("`load.account`: {m}"),
            )
        })?;
        let pem = read_key_file(key_path)?;
        Ok(Self {
            endpoint: account.endpoint(),
            http: shared_http()?,
            signer: Arc::new(KeyPairSigner::new(&account, user, &pem)?),
            context,
        })
    }

    /// Run a script and return one block of row objects per statement.
    pub(crate) fn run(&self, sql: &str) -> Result<Value> {
        let body = statement_body(sql, &self.context);
        let request_id = new_request_id();
        let first = self.call(
            reqwest::Method::POST,
            |resend| {
                let retry = if resend { "&retry=true" } else { "" };
                format!("/api/v2/statements?requestId={request_id}{retry}")
            },
            Some(&body),
            "statement",
        )?;
        let top = self.settle(first, STATEMENT_POLL_DEADLINE)?;
        let handles: Vec<String> = match top.get("statementHandles").and_then(Value::as_array) {
            Some(children) => children
                .iter()
                .map(|h| h.as_str().map(str::to_string))
                .collect::<Option<_>>()
                .ok_or_else(unreadable_answer)?,
            None => return Ok(Value::Array(vec![self.rows_block(top)?])),
        };
        let mut blocks = Vec::with_capacity(handles.len());
        for handle in &handles {
            let reply = self.get_statement(handle, None)?;
            blocks.push(self.rows_block(self.settle(reply, STATEMENT_POLL_DEADLINE)?)?);
        }
        Ok(Value::Array(blocks))
    }

    /// The rows of one finished statement, every result partition read.
    fn rows_block(&self, result: Value) -> Result<Value> {
        let meta = result
            .get("resultSetMetaData")
            .ok_or_else(unreadable_answer)?;
        let columns = column_types(meta)?;
        let mut data: Vec<Value> = result
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let partitions = meta
            .get("partitionInfo")
            .and_then(Value::as_array)
            .map_or(1, Vec::len);
        if partitions > 1 {
            let handle = result
                .get("statementHandle")
                .and_then(Value::as_str)
                .ok_or_else(unreadable_answer)?;
            for partition in 1..partitions {
                let reply = self.get_statement(handle, Some(partition))?;
                let page = self.settle(reply, STATEMENT_POLL_DEADLINE)?;
                if let Some(rows) = page.get("data").and_then(Value::as_array) {
                    data.extend(rows.iter().cloned());
                }
            }
        }
        data.iter().map(|row| row_object(&columns, row)).collect()
    }

    fn get_statement(&self, handle: &str, partition: Option<usize>) -> Result<Reply> {
        let handle = safe_handle(handle)?;
        let query = partition
            .map(|p| format!("?partition={p}"))
            .unwrap_or_default();
        self.call(
            reqwest::Method::GET,
            |_| format!("/api/v2/statements/{handle}{query}"),
            None,
            "statement result",
        )
    }

    /// A `200` answer as JSON; a `202` polled until it settles; any other answer as its error.
    fn settle(&self, first: Reply, budget: Duration) -> Result<Value> {
        let started = std::time::Instant::now();
        let mut reply = first;
        for attempt in 0.. {
            match reply.status {
                200 => return parse_json(&reply.body),
                202 => {
                    let running = parse_json(&reply.body)?;
                    let handle = running
                        .get("statementHandle")
                        .and_then(Value::as_str)
                        .ok_or_else(unreadable_answer)?
                        .to_string();
                    if started.elapsed() >= budget {
                        return Err(crate::load::JobWaitTimeout::snowflake(
                            &handle,
                            budget.as_secs(),
                        )
                        .into());
                    }
                    std::thread::sleep(pause(attempt));
                    reply = self.get_statement(&handle, None)?;
                }
                status => return Err(statement_error(status, &reply.body)),
            }
        }
        unreachable!("the poll loop only exits through an answer, an error or the budget")
    }

    /// One request, with the transient failures retried and a refused token minted again once.
    fn call(
        &self,
        method: reqwest::Method,
        path: impl Fn(bool) -> String,
        body: Option<&Value>,
        what: &str,
    ) -> Result<Reply> {
        let mut last: anyhow::Error = no_attempt_made();
        let mut renewed = false;
        let mut attempt = 0;
        while attempt <= MAX_TRANSIENT_RETRIES {
            if should_back_off(attempt) {
                std::thread::sleep(pause(attempt));
            }
            let token = self.signer.token(unix_now())?;
            let url = format!("{}{}", self.endpoint, path(attempt > 0));
            let mut req = self
                .http
                .request(method.clone(), url)
                .header("Authorization", format!("Bearer {}", token.as_str()))
                .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
                .header("Accept", "application/json");
            if let Some(b) = body {
                req = req.json(b);
            }
            match req.send() {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let text = resp.text().unwrap_or_default();
                    if status == 401 && !renewed {
                        renewed = true;
                        self.signer.forget();
                        continue;
                    }
                    if is_transient_status(status) {
                        last = transient_reply(status, &text);
                    } else {
                        return Ok(Reply { status, body: text });
                    }
                }
                Err(e) => {
                    last =
                        anyhow::Error::new(e).context(format!("Snowflake {what} request failed"));
                }
            }
            attempt += 1;
        }
        Err(last).context(crate::error::CodedError::new(
            codes::LOAD_WAREHOUSE_UNAVAILABLE,
            format!("Snowflake {what} kept failing after {MAX_TRANSIENT_RETRIES} retries"),
        ))
    }
}

// ── pure helpers ─────────────────────────────────────────────────────────────

/// The request body for `sql` run in `context`.
pub(crate) fn statement_body(sql: &str, context: &Context) -> Value {
    let mut body = json!({
        "statement": sql,
        "parameters": { "MULTI_STATEMENT_COUNT": "0", "TIMEZONE": "UTC" },
    });
    for (key, value) in [
        ("database", Some(context.database.as_str())),
        ("schema", Some(context.schema.as_str())),
        ("role", context.role.as_deref()),
    ] {
        if let Some(v) = value.filter(|v| !v.is_empty()) {
            body[key] = Value::String(v.to_string());
        }
    }
    body
}

/// A random version-4 UUID, the `requestId` that makes a resent statement the same statement.
pub(crate) fn new_request_id() -> String {
    use rand::RngExt;
    let mut n: u128 = rand::rng().random();
    n = (n & !(0xf << 76)) | (4 << 76);
    n = (n & !(0x3 << 62)) | (2 << 62);
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (n >> 96) as u32,
        (n >> 80) as u16,
        (n >> 64) as u16,
        (n >> 48) as u16,
        n & 0xffff_ffff_ffff
    )
}

/// A statement handle that is safe to put in a URL path.
fn safe_handle(handle: &str) -> Result<&str> {
    if !handle.is_empty()
        && handle
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
    {
        Ok(handle)
    } else {
        Err(unreadable_answer())
    }
}

fn unreadable_answer() -> anyhow::Error {
    crate::error::coded(
        codes::LOAD_WAREHOUSE_ANSWER_UNREADABLE,
        "Snowflake answered in a shape rivet does not read",
    )
}

fn parse_json(body: &str) -> Result<Value> {
    serde_json::from_str(body).map_err(|_| unreadable_answer())
}

/// One transient answer of a retried request; the last one is what the caller reports.
fn transient_reply(status: u16, body: &str) -> anyhow::Error {
    crate::error::coded(
        codes::LOAD_WAREHOUSE_UNAVAILABLE,
        format!("Snowflake {} (transient)", error_detail(status, body)),
    )
}

/// A non-success answer of the statements API, coded by what its status says.
fn statement_error(status: u16, body: &str) -> anyhow::Error {
    let detail = error_detail(status, body);
    let message = if status == 401 {
        format!(
            "Snowflake refused the key-pair JWT: {detail} — check `load.account` and \
             `load.user`, and that the public key of `load.private_key_path` is registered on \
             the user (`ALTER USER <user> SET RSA_PUBLIC_KEY='...'`)"
        )
    } else {
        format!("Snowflake rejected the request: {detail}")
    };
    super::warehouse_http_error(status, message)
}

/// `HTTP 422: 002003 (42S02): <message>` from the API's error envelope, else the bounded body.
fn error_detail(status: u16, body: &str) -> String {
    let parsed: Option<Value> = serde_json::from_str(body).ok();
    let field = |k: &str| {
        parsed
            .as_ref()
            .and_then(|v| v.get(k))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    };
    if let Some(message) = field("message") {
        let code = field("code").map(|c| format!("{c} ")).unwrap_or_default();
        let state = field("sqlState")
            .map(|s| format!("({s}): "))
            .unwrap_or_default();
        return format!("HTTP {status}: {code}{state}{message}");
    }
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return format!("HTTP {status} (empty response body)");
    }
    format!("HTTP {status}: {}", super::bq_rest::truncate(trimmed, 500))
}

/// A column of a result set: its name and how its text cell is typed.
struct Column {
    name: String,
    kind: CellKind,
}

enum CellKind {
    Whole,
    Real,
    Boolean,
    Text,
}

fn column_types(meta: &Value) -> Result<Vec<Column>> {
    meta.get("rowType")
        .and_then(Value::as_array)
        .ok_or_else(unreadable_answer)?
        .iter()
        .map(|c| {
            let name = c.get("name").and_then(Value::as_str)?.to_string();
            let scale = c.get("scale").and_then(Value::as_i64).unwrap_or(0);
            let kind = match c.get("type").and_then(Value::as_str)? {
                "fixed" if scale == 0 => CellKind::Whole,
                "fixed" | "real" => CellKind::Real,
                "boolean" => CellKind::Boolean,
                _ => CellKind::Text,
            };
            Some(Column { name, kind })
        })
        .collect::<Option<_>>()
        .ok_or_else(unreadable_answer)
}

/// One row of text cells as an object keyed by column name; a cell is typed by its column.
fn row_object(columns: &[Column], row: &Value) -> Result<Value> {
    let cells = row.as_array().ok_or_else(unreadable_answer)?;
    if cells.len() != columns.len() {
        return Err(unreadable_answer());
    }
    let mut object = Map::new();
    for (column, cell) in columns.iter().zip(cells) {
        object.insert(column.name.clone(), typed_cell(&column.kind, cell));
    }
    Ok(Value::Object(object))
}

fn typed_cell(kind: &CellKind, cell: &Value) -> Value {
    let Some(text) = cell.as_str() else {
        return cell.clone();
    };
    let typed = match kind {
        CellKind::Whole => text
            .parse::<i64>()
            .ok()
            .map(Number::from)
            .or_else(|| text.parse::<u64>().ok().map(Number::from))
            .map(Value::Number),
        CellKind::Real => text
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map(Value::Number),
        CellKind::Boolean => match text {
            "true" | "1" => Some(Value::Bool(true)),
            "false" | "0" => Some(Value::Bool(false)),
            _ => None,
        },
        CellKind::Text => None,
    };
    typed.unwrap_or_else(|| Value::String(text.to_string()))
}

// ── the offline stand ────────────────────────────────────────────────────────

/// A loopback Snowflake that answers from a script, for the tests of every layer above it.
#[cfg(test)]
pub(crate) mod stand {
    use super::{Account, Context, KeyPairSigner, SnowflakeApi};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::sync::{Arc, Mutex};
    use zeroize::Zeroizing;

    /// One request the stand received.
    #[derive(Debug, Clone)]
    pub(crate) struct Seen {
        pub(crate) method: String,
        /// The path without its query string.
        pub(crate) path: String,
        pub(crate) query: String,
        pub(crate) authorization: String,
        pub(crate) token_type: String,
        /// The JSON body, `null` when there was none.
        pub(crate) body: serde_json::Value,
    }

    impl Seen {
        /// The SQL of a statements `POST`.
        pub(crate) fn statement(&self) -> Option<&str> {
            self.body.get("statement").and_then(|s| s.as_str())
        }
    }

    pub(crate) struct Stand {
        endpoint: String,
        seen: Arc<Mutex<Vec<Seen>>>,
    }

    /// The signer every stand client uses: the throwaway test key, for `MYORG-ACCT` and `RIVET`.
    pub(crate) fn test_signer() -> KeyPairSigner {
        let account = Account::parse("myorg-acct").expect("the test account");
        let pem = Zeroizing::new(crate::destination::gcs_auth::TEST_RSA_PKCS8_PEM.to_string());
        KeyPairSigner::new(&account, "rivet", &pem).expect("the test key")
    }

    impl Stand {
        /// Serve `script`, which is handed each request and how many earlier requests had the same method and path.
        pub(crate) fn start(
            script: impl Fn(&Seen, usize) -> (u16, String) + Send + 'static,
        ) -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind the stand");
            let endpoint = format!(
                "http://{}",
                listener.local_addr().expect("the stand's port")
            );
            let seen = Arc::new(Mutex::new(Vec::<Seen>::new()));
            let log = Arc::clone(&seen);
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { continue };
                    let Some(request) = read_request(&mut stream) else {
                        continue;
                    };
                    let nth = {
                        let mut log = log.lock().unwrap();
                        let same = |r: &&Seen| r.method == request.method && r.path == request.path;
                        let nth = log.iter().filter(same).count();
                        log.push(request.clone());
                        nth
                    };
                    let (status, body) = script(&request, nth);
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {status} stand\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                }
            });
            Self { endpoint, seen }
        }

        /// A client that talks to this stand, running in `database.schema`.
        pub(crate) fn api(&self) -> SnowflakeApi {
            SnowflakeApi {
                endpoint: self.endpoint.clone(),
                http: reqwest::blocking::Client::new(),
                signer: Arc::new(test_signer()),
                context: Context {
                    database: "DB".into(),
                    schema: "SC".into(),
                    role: None,
                },
            }
        }

        /// Every request so far, in order.
        pub(crate) fn seen(&self) -> Vec<Seen> {
            self.seen.lock().unwrap().clone()
        }

        /// The SQL of every statements `POST` so far, in order.
        pub(crate) fn statements(&self) -> Vec<String> {
            self.seen()
                .iter()
                .filter_map(|r| r.statement().map(str::to_string))
                .collect()
        }

        /// How many requests had `fragment` in their path.
        pub(crate) fn asked(&self, fragment: &str) -> usize {
            self.seen()
                .iter()
                .filter(|r| r.path.contains(fragment))
                .count()
        }
    }

    fn read_request(stream: &mut std::net::TcpStream) -> Option<Seen> {
        let mut reader = BufReader::new(stream.try_clone().ok()?);
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        let mut first = line.split_whitespace();
        let (method, target) = (first.next()?.to_string(), first.next()?.to_string());
        let (mut length, mut authorization, mut token_type) =
            (0usize, String::new(), String::new());
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).ok()?;
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            let (name, value) = header.split_once(':')?;
            match name.to_ascii_lowercase().as_str() {
                "content-length" => length = value.trim().parse().ok()?,
                "authorization" => authorization = value.trim().to_string(),
                "x-snowflake-authorization-token-type" => token_type = value.trim().to_string(),
                _ => {}
            }
        }
        let mut body = vec![0u8; length];
        reader.read_exact(&mut body).ok()?;
        let (path, query) = target.split_once('?').unwrap_or((&target, ""));
        Some(Seen {
            method,
            path: path.to_string(),
            query: query.to_string(),
            authorization,
            token_type,
            body: serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::stand::{Seen, Stand, test_signer};
    use super::*;
    use crate::destination::gcs_auth::TEST_RSA_PKCS8_PEM;
    use crate::error::{codes, error_code};

    /// The SHA-256 fingerprint of the test key's public half, computed with `openssl` and not by this module.
    const FINGERPRINT: &str = "SHA256:d2NBmwH552sXWCQPWGmKNzUj0TZo2HKZ0AMrBo4LxO4=";
    const T0: u64 = 1_700_000_000;

    fn result(row_type: Value, data: Value, partitions: usize) -> String {
        json!({
            "resultSetMetaData": {
                "numRows": data.as_array().map_or(0, Vec::len),
                "partitionInfo": vec![json!({}); partitions],
                "rowType": row_type,
            },
            "data": data,
            "statementHandle": "hh",
        })
        .to_string()
    }

    fn count_result(n: u64) -> String {
        result(
            json!([{ "name": "ROWS_", "type": "fixed", "scale": 0 }]),
            json!([[n.to_string()]]),
            1,
        )
    }

    fn code_of(e: &anyhow::Error) -> Option<&'static str> {
        error_code(e)
    }

    fn request_id(seen: &Seen) -> String {
        seen.query
            .split('&')
            .find_map(|kv| kv.strip_prefix("requestId="))
            .expect("a requestId")
            .to_string()
    }

    #[test]
    fn the_token_is_an_rs256_jwt_over_the_account_user_and_key_fingerprint() {
        let signer = test_signer();
        assert_eq!(
            public_key_fingerprint(TEST_RSA_PKCS8_PEM).as_deref(),
            Some(FINGERPRINT)
        );
        let token = signer.token(T0).unwrap();
        let public = rsa::RsaPrivateKey::from_pkcs8_pem(TEST_RSA_PKCS8_PEM)
            .unwrap()
            .to_public_key()
            .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap();
        let mut rules = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
        rules.validate_exp = false;
        rules.required_spec_claims.clear();
        let decoded = jsonwebtoken::decode::<Value>(
            token.as_str(),
            &jsonwebtoken::DecodingKey::from_rsa_pem(public.as_bytes()).unwrap(),
            &rules,
        )
        .expect("signed by the key, verified by its public half");
        assert_eq!(decoded.header.alg, jsonwebtoken::Algorithm::RS256);
        assert_eq!(
            decoded.claims,
            json!({
                "iss": format!("MYORG-ACCT.RIVET.{FINGERPRINT}"),
                "sub": "MYORG-ACCT.RIVET",
                "iat": T0,
                "exp": T0 + 3300,
            })
        );
        assert_eq!(super::JWT_LIFETIME_SECS, 3300);
        const _: () = assert!(super::JWT_LIFETIME_SECS <= 3600);
    }

    #[test]
    fn the_token_is_cached_until_five_minutes_before_it_ends_and_forgotten_on_demand() {
        let signer = test_signer();
        let first = signer.token(T0).unwrap();
        assert_eq!(signer.token(T0 + 2999).unwrap().as_str(), first.as_str());
        let renewed = signer.token(T0 + 3000).unwrap();
        assert_ne!(renewed.as_str(), first.as_str());
        signer.forget();
        let after_forget = signer.token(T0 + 3001).unwrap();
        assert_ne!(after_forget.as_str(), renewed.as_str());
        let claims = |t: &str| {
            use base64::Engine as _;
            let payload = t.split('.').nth(1).unwrap();
            let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(payload)
                .unwrap();
            serde_json::from_slice::<Value>(&raw).unwrap()
        };
        assert_eq!(claims(renewed.as_str())["exp"], json!(T0 + 3000 + 3300));
        assert_eq!(claims(after_forget.as_str())["iat"], json!(T0 + 3001));
    }

    #[test]
    fn a_key_that_cannot_sign_is_refused_with_its_remedy_and_never_echoed() {
        let account = Account::parse("myorg-acct").unwrap();
        let pem = |text: &str| Zeroizing::new(text.to_string());
        let e = KeyPairSigner::new(
            &account,
            "rivet",
            &pem("-----BEGIN ENCRYPTED PRIVATE KEY-----\nSECRETMARKER\n-----END ENCRYPTED PRIVATE KEY-----\n"),
        )
        .unwrap_err();
        assert_eq!(code_of(&e), Some("RIVET_LOAD_WAREHOUSE_AUTH_FAILED"));
        let shown = format!("{e:#}");
        assert!(shown.contains("openssl pkcs8 -topk8 -nocrypt"), "{shown}");
        assert!(shown.contains("load.private_key_path"), "{shown}");
        assert!(!shown.contains("SECRETMARKER"), "{shown}");
        let e = KeyPairSigner::new(
            &account,
            "rivet",
            &pem("-----BEGIN PRIVATE KEY-----\nSECRETMARKER\n-----END PRIVATE KEY-----\n"),
        )
        .unwrap_err();
        assert_eq!(code_of(&e), Some("RIVET_LOAD_WAREHOUSE_AUTH_FAILED"));
        let shown = format!("{e:#}");
        assert!(shown.contains("PKCS#8"), "{shown}");
        assert!(!shown.contains("SECRETMARKER"), "{shown}");
    }

    #[test]
    fn the_key_file_is_read_once_and_a_missing_one_names_its_path_and_the_remedy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rsa_key.p8");
        std::fs::write(&path, "KEY BYTES").unwrap();
        assert_eq!(
            read_key_file(path.to_str().unwrap()).unwrap().as_str(),
            "KEY BYTES"
        );
        let missing = dir.path().join("absent.p8");
        let e = read_key_file(missing.to_str().unwrap()).unwrap_err();
        assert_eq!(code_of(&e), Some("RIVET_LOAD_WAREHOUSE_AUTH_FAILED"));
        let shown = format!("{e:#}");
        assert!(shown.contains("absent.p8"), "{shown}");
        assert!(shown.contains("RIVET_SNOWFLAKE_KEY"), "{shown}");
    }

    #[test]
    fn nothing_renders_the_key_or_the_live_token() {
        let stand = Stand::start(|_, _| (200, count_result(1)));
        let api = stand.api();
        api.run("SELECT 1").unwrap();
        let token = api.signer.token(unix_now()).unwrap();
        let shown = format!("{api:?} {:?}", api.signer);
        assert!(!shown.contains(token.as_str()), "{shown}");
        assert!(!shown.contains("PRIVATE KEY"), "{shown}");
        assert!(!shown.contains(&token.as_str()[..40]), "{shown}");
        assert!(shown.contains("MYORG-ACCT.RIVET"), "{shown}");
    }

    #[test]
    fn a_request_carries_the_script_where_it_runs_and_the_session_settings() {
        let stand = Stand::start(|_, _| (200, count_result(1)));
        let mut api = stand.api();
        api.run("SELECT 1").unwrap();
        api.context.role = Some("LOADER".into());
        api.run("SELECT 2").unwrap();
        let seen = stand.seen();
        let (a, b) = (&seen[0], &seen[1]);
        assert_eq!(
            (a.method.as_str(), a.path.as_str()),
            ("POST", "/api/v2/statements")
        );
        assert!(a.authorization.starts_with("Bearer "), "{a:?}");
        assert_eq!(a.token_type, "KEYPAIR_JWT");
        assert!(!a.query.contains("retry"), "{a:?}");
        assert_eq!(
            a.body,
            json!({
                "statement": "SELECT 1",
                "database": "DB",
                "schema": "SC",
                "parameters": { "MULTI_STATEMENT_COUNT": "0", "TIMEZONE": "UTC" },
            })
        );
        assert_eq!(b.body["role"], json!("LOADER"));
        assert_ne!(request_id(a), request_id(b));
    }

    #[test]
    fn a_request_id_is_a_fresh_version_four_uuid() {
        let ids: std::collections::HashSet<String> = (0..64).map(|_| new_request_id()).collect();
        assert_eq!(ids.len(), 64);
        for id in &ids {
            let b = id.as_bytes();
            assert_eq!(id.len(), 36, "{id}");
            assert!([8, 13, 18, 23].iter().all(|&i| b[i] == b'-'), "{id}");
            assert_eq!(b[14], b'4', "{id}");
            assert!(matches!(b[19], b'8' | b'9' | b'a' | b'b'), "{id}");
            assert!(
                id.chars().all(|c| c == '-' || c.is_ascii_hexdigit()),
                "{id}"
            );
        }
    }

    #[test]
    fn a_single_statement_answer_is_one_block_of_typed_rows() {
        let stand = Stand::start(|_, _| {
            (
                200,
                result(
                    json!([
                        { "name": "N", "type": "fixed", "scale": 0 },
                        { "name": "S", "type": "text" },
                        { "name": "B", "type": "boolean" },
                        { "name": "R", "type": "real" },
                        { "name": "D", "type": "fixed", "scale": 2 },
                    ]),
                    json!([
                        ["2", "a", "true", "1.5", "3.25"],
                        [null, "b", "false", "2", "4.00"]
                    ]),
                    1,
                ),
            )
        });
        let got = stand.api().run("SELECT 1").unwrap();
        assert_eq!(
            got,
            json!([[
                { "N": 2, "S": "a", "B": true, "R": 1.5, "D": 3.25 },
                { "N": null, "S": "b", "B": false, "R": 2.0, "D": 4.0 },
            ]])
        );
        assert_eq!(stand.statements(), ["SELECT 1"]);
    }

    #[test]
    fn a_whole_number_is_a_json_number_and_everything_else_keeps_its_text() {
        use CellKind::*;
        let cell = |kind: &CellKind, text: &str| typed_cell(kind, &json!(text));
        assert_eq!(cell(&Whole, "42"), json!(42));
        assert_eq!(cell(&Whole, "-7"), json!(-7));
        assert_eq!(
            cell(&Whole, "18446744073709551615"),
            json!(18446744073709551615u64)
        );
        assert_eq!(
            cell(&Whole, "123456789012345678901234567890"),
            json!("123456789012345678901234567890")
        );
        assert_eq!(cell(&Whole, "abc"), json!("abc"));
        assert_eq!(cell(&Real, "0.5"), json!(0.5));
        assert_eq!(cell(&Real, "NaN"), json!("NaN"));
        assert_eq!(cell(&Boolean, "true"), json!(true));
        assert_eq!(cell(&Boolean, "0"), json!(false));
        assert_eq!(cell(&Boolean, "maybe"), json!("maybe"));
        assert_eq!(cell(&Text, "42"), json!("42"));
        assert_eq!(typed_cell(&Whole, &Value::Null), Value::Null);
    }

    #[test]
    fn a_multi_statement_answer_is_read_child_by_child_in_order() {
        let stand = Stand::start(|req, _| {
            if req.method == "POST" {
                return (
                    200,
                    json!({ "statementHandle": "p", "statementHandles": ["c0", "c1", "c2"] })
                        .to_string(),
                );
            }
            let n: u64 = req
                .path
                .trim_start_matches("/api/v2/statements/c")
                .parse()
                .unwrap();
            (200, count_result(n * 10))
        });
        let got = stand.api().run("A; B; C").unwrap();
        assert_eq!(
            got,
            json!([[{ "ROWS_": 0 }], [{ "ROWS_": 10 }], [{ "ROWS_": 20 }]])
        );
        let paths: Vec<_> = stand.seen().iter().map(|r| r.path.clone()).collect();
        assert_eq!(
            paths,
            [
                "/api/v2/statements",
                "/api/v2/statements/c0",
                "/api/v2/statements/c1",
                "/api/v2/statements/c2"
            ]
        );
    }

    #[test]
    fn every_result_partition_is_read_and_the_rows_keep_their_order() {
        let stand = Stand::start(|req, _| {
            let column = json!([{ "name": "N", "type": "fixed", "scale": 0 }]);
            match req.query.as_str() {
                q if q.starts_with("requestId=") => (200, result(column, json!([["1"], ["2"]]), 3)),
                "partition=1" => (200, json!({ "data": [["3"]] }).to_string()),
                "partition=2" => (200, json!({ "data": [["4"], ["5"]] }).to_string()),
                other => panic!("unexpected request {other}"),
            }
        });
        let got = stand.api().run("SELECT n").unwrap();
        let ns: Vec<u64> = got[0]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["N"].as_u64().unwrap())
            .collect();
        assert_eq!(ns, [1, 2, 3, 4, 5]);
        assert_eq!(stand.asked("/api/v2/statements/hh"), 2);
    }

    #[test]
    fn a_running_statement_is_polled_until_it_settles() {
        let stand = Stand::start(|req, nth| match (req.method.as_str(), nth) {
            ("POST", _) => (
                202,
                json!({ "statementHandle": "hh", "message": "running" }).to_string(),
            ),
            (_, 0) => (202, json!({ "statementHandle": "hh" }).to_string()),
            _ => (200, count_result(7)),
        });
        let got = stand.api().run("COPY INTO t").unwrap();
        assert_eq!(got, json!([[{ "ROWS_": 7 }]]));
        assert_eq!(stand.asked("/api/v2/statements/hh"), 2);
        assert_eq!(
            stand.statements().len(),
            1,
            "polling never resends the statement"
        );
    }

    #[test]
    fn a_statement_still_running_past_the_budget_is_a_wait_timeout_naming_it() {
        let stand = Stand::start(|_, _| (202, json!({ "statementHandle": "hh" }).to_string()));
        let api = stand.api();
        let first = Reply {
            status: 202,
            body: json!({ "statementHandle": "hh" }).to_string(),
        };
        let e = api.settle(first, Duration::ZERO).unwrap_err();
        let timeout = e
            .downcast_ref::<crate::load::JobWaitTimeout>()
            .expect("a wait timeout");
        assert!(timeout.to_string().contains("`hh`"), "{timeout}");
        assert!(stand.seen().is_empty(), "no poll once the budget is spent");
    }

    #[test]
    fn a_refused_token_is_minted_again_once_and_a_second_refusal_is_final() {
        let stand = Stand::start(|_, nth| {
            if nth == 0 {
                (
                    401,
                    json!({ "message": "JWT token is invalid.", "code": "390144" }).to_string(),
                )
            } else {
                (200, count_result(1))
            }
        });
        stand.api().run("SELECT 1").unwrap();
        assert_eq!(stand.statements().len(), 2);

        let stand = Stand::start(|_, _| {
            (
                401,
                json!({ "message": "JWT token is invalid.", "code": "390144" }).to_string(),
            )
        });
        let e = stand.api().run("SELECT 1").unwrap_err();
        assert_eq!(code_of(&e), Some("RIVET_LOAD_WAREHOUSE_AUTH_FAILED"));
        let shown = format!("{e:#}");
        assert!(shown.contains("390144"), "{shown}");
        assert!(
            shown.contains("ALTER USER <user> SET RSA_PUBLIC_KEY"),
            "{shown}"
        );
        assert_eq!(stand.statements().len(), 2, "one renewal, then the refusal");
    }

    #[test]
    fn the_verdict_to_retry_is_the_status_never_the_words_of_the_answer() {
        // A 503 whose body reads like a permanent error is still a 503: retried, under one request id.
        let stand = Stand::start(|_, nth| match nth {
            0 => (
                503,
                json!({ "message": "SQL compilation error: invalid identifier" }).to_string(),
            ),
            1 => (429, String::new()),
            _ => (200, count_result(1)),
        });
        stand.api().run("SELECT 1").unwrap();
        let seen = stand.seen();
        assert_eq!(seen.len(), 3);
        let ids: Vec<_> = seen.iter().map(request_id).collect();
        assert!(ids.iter().all(|i| *i == ids[0]), "{ids:?}");
        assert!(!seen[0].query.contains("retry=true"), "{:?}", seen[0]);
        assert!(seen[1].query.contains("retry=true") && seen[2].query.contains("retry=true"));

        // A 422 whose body says "try again" is the warehouse's answer: refused once, not retried.
        let stand = Stand::start(|_, _| {
            (
                422,
                json!({
                    "code": "000625",
                    "message": "Service temporarily unavailable, please try again later",
                })
                .to_string(),
            )
        });
        let e = stand.api().run("SELECT 1").unwrap_err();
        assert_eq!(code_of(&e), Some("RIVET_LOAD_WAREHOUSE_REJECTED"));
        assert_eq!(stand.statements().len(), 1);
    }

    #[test]
    fn transient_failures_end_unavailable_after_the_retry_budget() {
        let stand = Stand::start(|_, _| (503, "upstream down".into()));
        let e = stand.api().run("SELECT 1").unwrap_err();
        assert_eq!(code_of(&e), Some("RIVET_LOAD_WAREHOUSE_UNAVAILABLE"));
        assert_eq!(stand.statements().len(), MAX_TRANSIENT_RETRIES as usize + 1);
        assert!(
            format!("{e:#}").contains("HTTP 503: upstream down"),
            "{e:#}"
        );

        let down = Stand::start(|_, _| (200, count_result(1)));
        let mut api = down.api();
        api.endpoint = "http://127.0.0.1:1".into();
        let e = api.run("SELECT 1").unwrap_err();
        assert_eq!(code_of(&e), Some("RIVET_LOAD_WAREHOUSE_UNAVAILABLE"));
        assert!(down.seen().is_empty());
    }

    #[test]
    fn a_status_is_coded_by_what_it_says_and_its_detail_is_bounded() {
        let envelope = json!({
            "code": "002003",
            "sqlState": "42S02",
            "message": "SQL compilation error: Object 'T' does not exist or not authorized.",
        })
        .to_string();
        for (status, code) in [
            (401, "RIVET_LOAD_WAREHOUSE_AUTH_FAILED"),
            (403, "RIVET_LOAD_WAREHOUSE_ACCESS_DENIED"),
            (404, "RIVET_LOAD_WAREHOUSE_OBJECT_NOT_FOUND"),
            (422, "RIVET_LOAD_WAREHOUSE_REJECTED"),
            (400, "RIVET_LOAD_WAREHOUSE_REJECTED"),
        ] {
            assert_eq!(
                code_of(&statement_error(status, &envelope)),
                Some(code),
                "{status}"
            );
        }
        assert_eq!(
            error_detail(422, &envelope),
            "HTTP 422: 002003 (42S02): SQL compilation error: Object 'T' does not exist or not authorized."
        );
        assert_eq!(
            error_detail(502, "<html>bad gateway</html>"),
            "HTTP 502: <html>bad gateway</html>"
        );
        assert_eq!(error_detail(500, "  "), "HTTP 500 (empty response body)");
        let long = "x".repeat(5000);
        assert!(error_detail(500, &long).chars().count() < 600);
    }

    #[test]
    fn an_answer_in_a_shape_rivet_does_not_read_is_refused_not_guessed() {
        let unreadable = |body: String| {
            let stand = Stand::start(move |_, _| (200, body.clone()));
            let e = stand.api().run("SELECT 1").unwrap_err();
            assert_eq!(
                code_of(&e),
                Some("RIVET_LOAD_WAREHOUSE_ANSWER_UNREADABLE"),
                "{e:#}"
            );
        };
        unreadable("not json".into());
        unreadable(json!({ "data": [] }).to_string());
        unreadable(result(
            json!([{ "name": "N", "type": "fixed" }]),
            json!([["1", "2"]]),
            1,
        ));
        unreadable(result(json!([{ "type": "fixed" }]), json!([["1"]]), 1));

        let stand = Stand::start(|_, _| {
            (
                200,
                json!({ "statementHandle": "p", "statementHandles": ["../escape"] }).to_string(),
            )
        });
        let e = stand.api().run("A; B").unwrap_err();
        assert_eq!(code_of(&e), Some("RIVET_LOAD_WAREHOUSE_ANSWER_UNREADABLE"));
        assert_eq!(
            stand.seen().len(),
            1,
            "a handle that is not a path segment is never requested"
        );
    }

    #[test]
    fn connecting_reads_the_account_and_the_key_before_any_request() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("k.p8");
        std::fs::write(&key, TEST_RSA_PKCS8_PEM).unwrap();
        let key = key.to_str().unwrap();
        let ctx = || Context {
            database: "DB".into(),
            schema: "SC".into(),
            role: None,
        };
        let api = SnowflakeApi::connect("myorg-acct", "rivet", key, ctx()).unwrap();
        assert_eq!(api.endpoint, "https://myorg-acct.snowflakecomputing.com");
        let e = SnowflakeApi::connect("bad account", "rivet", key, ctx()).unwrap_err();
        assert_eq!(code_of(&e), Some(codes::CONFIG_LOAD_BLOCK_INVALID.id));
        assert!(format!("{e:#}").starts_with("`load.account`: "), "{e:#}");
        let e = SnowflakeApi::connect("myorg-acct", "rivet", "/nope/k.p8", ctx()).unwrap_err();
        assert_eq!(code_of(&e), Some("RIVET_LOAD_WAREHOUSE_AUTH_FAILED"));
    }
}
