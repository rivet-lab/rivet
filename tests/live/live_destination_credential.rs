//! One destination, one credential: an S3 destination that names two credential modes is refused, and a destination with no named key loads as the ambient identity.
//!
//!     docker compose up -d postgres fake-gcs minio
//!     cargo test --test live_suite destination_credential -- --ignored

use crate::common::*;
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

const GCS_BUCKET: &str = "rivet-qa-destination-credential";
const S3_BUCKET: &str = "rivet-qa-destination-credential-s3";
const LOAD: &str = "load: { target: bigquery, project: p, dataset: d, pk: [id] }";
const TEST_KEY_PEM: &str = include_str!("../fixtures/destination_credential/test_rsa_pkcs8.pem");
const CONFLICT: &str = "[RIVET_CONFIG_DESTINATION_AUTH_INVALID]";

struct Seen {
    path: String,
    auth: Option<String>,
}

struct Stand {
    port: u16,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Stand {
    /// A loopback server that mints `tok-<who>` at `/<who>/token` and answers every other path as a failing BigQuery API.
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the stand");
        let port = listener.local_addr().expect("the stand's address").port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        std::thread::spawn(move || {
            for conn in listener.incoming().flatten() {
                let log = log.clone();
                std::thread::spawn(move || serve(conn, &log));
            }
        });
        Stand { port, seen }
    }

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// The identities BigQuery API requests were signed with, by the token each carried.
    fn api_identities(&self) -> Vec<String> {
        let seen = self.seen.lock().unwrap();
        let who: BTreeSet<String> = seen
            .iter()
            .filter(|s| !s.path.ends_with("/token"))
            .map(|s| {
                s.auth
                    .as_deref()
                    .and_then(|a| a.strip_prefix("Bearer tok-"))
                    .unwrap_or("none")
                    .to_string()
            })
            .collect();
        who.into_iter().collect()
    }
}

/// Answer one request: a token at `/<who>/token`, a 400 anywhere else; the path and `Authorization` are logged.
fn serve(mut conn: TcpStream, log: &Mutex<Vec<Seen>>) {
    let mut reader = BufReader::new(conn.try_clone().expect("clone the connection"));
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let path = line.split_whitespace().nth(1).unwrap_or("").to_string();
    let (mut auth, mut len) = (None, 0usize);
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).unwrap_or(0) == 0 {
            break;
        }
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let lower = header.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            len = v.trim().parse().unwrap_or(0);
        }
        if lower.starts_with("authorization:") {
            auth = Some(header["authorization:".len()..].trim().to_string());
        }
    }
    let mut body = vec![0u8; len];
    let _ = reader.read_exact(&mut body);
    let (status, reply) = match path.strip_suffix("/token") {
        Some(who) => (
            "200 OK",
            format!(
                r#"{{"access_token":"tok-{}","expires_in":3600,"token_type":"Bearer"}}"#,
                who.trim_start_matches('/')
            ),
        ),
        None => (
            "400 Bad Request",
            r#"{"error":{"code":400,"message":"stand"}}"#.to_string(),
        ),
    };
    log.lock().unwrap().push(Seen { path, auth });
    let _ = write!(
        conn,
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
        reply.len()
    );
}

/// A service-account key file for `who` whose token endpoint is `token_uri`.
fn key_json(who: &str, token_uri: &str) -> String {
    serde_json::json!({
        "type": "service_account",
        "project_id": "p",
        "private_key_id": "1",
        "private_key": TEST_KEY_PEM,
        "client_email": format!("{who}@x.iam.gserviceaccount.com"),
        "client_id": "1",
        "token_uri": token_uri,
    })
    .to_string()
}

struct Fixture {
    rig: Rig,
    stand: Stand,
    ambient: String,
    _tmp: tempfile::TempDir,
    _table: PgTable,
}

impl Fixture {
    /// A Postgres export to fake-gcs with a BigQuery `load:` beside it, and an ambient ADC key that mints at the stand.
    fn gcs(label: &str) -> Self {
        require_alive(LiveService::Postgres);
        ensure_gcs_bucket(GCS_BUCKET);
        let stand = Stand::start();
        let tmp = tempfile::tempdir().expect("a temp dir");
        let ambient_path = tmp.path().join("ambient.json");
        let uri = format!("{}/ambient/token", stand.base());
        std::fs::write(&ambient_path, key_json("ambient", &uri)).expect("write the ambient key");
        let ambient = ambient_path.to_string_lossy().into_owned();
        let table = seed_pg_numeric_table(20);
        let rig = Rig::pg_batch(table.name()).dest_gcs(
            GCS_BUCKET,
            &unique_name(label),
            FAKE_GCS_ENDPOINT,
        );
        Fixture {
            rig: rig.top_line(LOAD),
            stand,
            ambient,
            _tmp: tmp,
            _table: table,
        }
    }

    /// `rivet load` against the stand, with the ambient ADC key and `token` as `RIVET_BQ_ACCESS_TOKEN` (blank is unset).
    fn load(&self, token: &str) -> std::process::Output {
        let base = self.stand.base();
        self.rig.load_args_env(
            &[],
            &[
                ("RIVET_BQ_API_ENDPOINT", &base),
                ("GOOGLE_APPLICATION_CREDENTIALS", &self.ambient),
                ("RIVET_BQ_ACCESS_TOKEN", token),
            ],
        )
    }
}

fn stderr_of(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// A destination with no named key loads as the ambient ADC identity.
#[test]
#[ignore = "live: requires postgres + fake-gcs"]
fn an_anonymous_destination_loads_as_the_ambient_identity() {
    let f = Fixture::gcs("dc_ambient");
    f.rig.run_ok();
    let _ = f.load("");
    assert_eq!(
        f.stand.api_identities(),
        vec!["ambient".to_string()],
        "with no named key the BigQuery leg signs with the ambient ADC identity"
    );
}

/// A destination with no named key loads with a static `RIVET_BQ_ACCESS_TOKEN` as given.
#[test]
#[ignore = "live: requires postgres + fake-gcs"]
fn an_anonymous_destination_loads_with_the_static_token_as_given() {
    let f = Fixture::gcs("dc_static");
    f.rig.run_ok();
    let _ = f.load("tok-static");
    assert_eq!(
        f.stand.api_identities(),
        vec!["static".to_string()],
        "with no named key the static token signs the BigQuery leg"
    );
}

struct S3Fixture {
    rig: Rig,
    env: Vec<(&'static str, String)>,
    prefix: String,
    _tmp: tempfile::TempDir,
    _table: PgTable,
}

impl S3Fixture {
    /// A Postgres export to MinIO naming `aws_profile: dc` (whose keys MinIO accepts), beside env keys MinIO rejects when `keys`.
    fn s3(label: &str, keys: bool) -> Self {
        require_alive(LiveService::Postgres);
        ensure_minio_bucket(S3_BUCKET);
        let tmp = tempfile::tempdir().expect("a temp dir");
        let credentials = tmp.path().join("credentials");
        std::fs::write(
            &credentials,
            format!(
                "[dc]\naws_access_key_id = {MINIO_ACCESS_KEY}\naws_secret_access_key = {MINIO_SECRET_KEY}\n"
            ),
        )
        .expect("write the profile");
        let config = tmp.path().join("config");
        std::fs::write(&config, "").expect("write the AWS config");
        let table = seed_pg_numeric_table(20);
        let prefix = unique_name(label);
        let rig = Rig::pg_batch(table.name()).dest_s3_with_profile(
            S3_BUCKET,
            &prefix,
            MINIO_ENDPOINT,
            "dc",
            keys,
        );
        let env = vec![
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                credentials.to_string_lossy().into_owned(),
            ),
            ("AWS_CONFIG_FILE", config.to_string_lossy().into_owned()),
            ("RIVET_TEST_MINIO_AK", "env-key-minio-rejects".to_string()),
            (
                "RIVET_TEST_MINIO_SK",
                "env-secret-minio-rejects".to_string(),
            ),
        ];
        S3Fixture {
            rig,
            env,
            prefix,
            _tmp: tmp,
            _table: table,
        }
    }

    fn envs(&self) -> Vec<(&str, &str)> {
        self.env.iter().map(|(k, v)| (*k, v.as_str())).collect()
    }
}

/// An S3 destination naming `aws_profile` beside `access_key_env` and `secret_key_env` is refused by code on `run` and `check`.
#[test]
#[ignore = "live+gate-only: open defect HFF-2; closes with fix/destination-credential-every-leg"]
fn open_defect_an_s3_profile_beside_env_keys_is_refused() {
    let f = S3Fixture::s3("dc_s3_both", true);
    let envs = f.envs();
    let run = f.rig.run_args_env(&[], &envs);
    assert!(
        stderr_of(&run).contains(CONFLICT),
        "an S3 destination naming aws_profile beside env keys is refused by code, run"
    );
    let check = f.rig.cli_env(
        &["check", "--config", &f.rig.config_path().to_string_lossy()],
        &envs,
    );
    assert!(
        stderr_of(&check).contains(CONFLICT),
        "an S3 destination naming aws_profile beside env keys is refused by code, check"
    );
}

/// An S3 destination naming only `aws_profile` signs with that profile's keys.
#[test]
#[ignore = "live: requires postgres + minio"]
fn an_s3_profile_alone_signs_with_the_profile() {
    let f = S3Fixture::s3("dc_s3_profile", false);
    let envs = f.envs();
    let run = f.rig.run_args_env(&[], &envs);
    assert!(
        run.status.success(),
        "a profile alone signs the S3 files; stderr:\n{}",
        stderr_of(&run)
    );
    assert_eq!(
        minio_parquet_total_rows(S3_BUCKET, &f.prefix),
        20,
        "every seeded row is in the bucket the profile signed"
    );
}
