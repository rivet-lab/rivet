//! A named `aws_profile` is used or refused by code; an ambient identity never replaces it.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use rivet::config::{DestinationConfig, DestinationType};
use rivet::destination_for_tests::create_destination;

static ENV: Mutex<()> = Mutex::new(());

const AWS_ENV: &[&str] = &[
    "AWS_PROFILE",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_EC2_METADATA_DISABLED",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT_URL",
    "AWS_STS_REGIONAL_ENDPOINTS",
    "RIVET_TEST_S3_NAMED_AK",
    "RIVET_TEST_S3_NAMED_SK",
];

struct Stand {
    _lock: std::sync::MutexGuard<'static, ()>,
    saved: Vec<(&'static str, Option<String>)>,
    _dir: tempfile::TempDir,
}

impl Stand {
    /// Scrub every AWS variable, point the shared files at a temp dir, then apply `env`.
    fn new(credentials: &str, config: &str, env: &[(&'static str, &str)]) -> Self {
        let lock = ENV.lock().unwrap_or_else(|p| p.into_inner());
        let saved = AWS_ENV
            .iter()
            .map(|k| (*k, std::env::var(k).ok()))
            .collect();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("credentials"), credentials).unwrap();
        std::fs::write(dir.path().join("config"), config).unwrap();
        unsafe {
            for k in AWS_ENV {
                std::env::remove_var(k);
            }
            std::env::set_var(
                "AWS_SHARED_CREDENTIALS_FILE",
                dir.path().join("credentials"),
            );
            std::env::set_var("AWS_CONFIG_FILE", dir.path().join("config"));
            for (k, v) in env {
                std::env::set_var(k, v);
            }
        }
        Self {
            _lock: lock,
            saved,
            _dir: dir,
        }
    }
}

impl Drop for Stand {
    fn drop(&mut self) {
        unsafe {
            for (k, v) in &self.saved {
                match v {
                    Some(v) => std::env::set_var(k, v),
                    None => std::env::remove_var(k),
                }
            }
        }
    }
}

struct Mock {
    endpoint: String,
    requests: Arc<Mutex<Vec<String>>>,
}

impl Mock {
    /// A loopback S3 that answers every request 404 and records its head.
    fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut head = [0u8; 8192];
                let n = stream.read(&mut head).unwrap_or(0);
                seen.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&head[..n]).into_owned());
                let _ = stream.write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
            }
        });
        Self { endpoint, requests }
    }

    fn signing_key(&self) -> Option<String> {
        let requests = self.requests.lock().unwrap();
        let lower = requests.first()?.to_ascii_lowercase();
        let at = lower.find("credential=")? + "credential=".len();
        let key = requests.first()?[at..].split('/').next()?.to_string();
        Some(key)
    }

    fn sent_a_session_token(&self) -> bool {
        self.requests
            .lock()
            .unwrap()
            .first()
            .is_some_and(|r| r.to_ascii_lowercase().contains("x-amz-security-token"))
    }

    fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

fn s3(endpoint: &str, profile: Option<&str>) -> DestinationConfig {
    DestinationConfig {
        destination_type: DestinationType::S3,
        bucket: Some("b".into()),
        region: Some("us-east-1".into()),
        endpoint: Some(endpoint.into()),
        aws_profile: profile.map(Into::into),
        ..Default::default()
    }
}

/// Build the store, send one HEAD, and return the access key the request was signed with.
fn signed_with(mock: &Mock, cfg: &DestinationConfig) -> anyhow::Result<Option<String>> {
    let dest = create_destination(cfg)?;
    let _ = dest.head("probe");
    Ok(mock.signing_key())
}

fn refusal(err: &anyhow::Error) -> String {
    assert_eq!(
        rivet::error::error_code(err),
        Some("RIVET_DEST_CREDENTIALS_INVALID"),
        "{err:#}"
    );
    format!("{err:#}")
}

const NAMED_AND_AMBIENT: &str = "[named-prod]\naws_access_key_id = AKIANAMEDPROD\naws_secret_access_key = named-secret\n\n[ambient-dev]\naws_access_key_id = AKIAAMBIENTDEV\naws_secret_access_key = ambient-secret\n";

#[test]
fn a_named_profile_signs_with_its_own_keys() {
    let _s = Stand::new(NAMED_AND_AMBIENT, "", &[]);
    let mock = Mock::start();
    let key = signed_with(&mock, &s3(&mock.endpoint, Some("named-prod"))).unwrap();
    assert_eq!(key.as_deref(), Some("AKIANAMEDPROD"));
}

#[test]
fn a_named_profile_in_the_config_file_signs_with_its_keys_and_session_token() {
    let _s = Stand::new(
        "",
        "[profile named-prod]\naws_access_key_id = ASIANAMEDCFG\naws_secret_access_key = s\naws_session_token = tok\nregion = eu-west-1\n",
        &[],
    );
    let mock = Mock::start();
    let key = signed_with(&mock, &s3(&mock.endpoint, Some("named-prod"))).unwrap();
    assert_eq!(key.as_deref(), Some("ASIANAMEDCFG"));
    assert!(mock.sent_a_session_token());
}

#[test]
fn an_ambient_aws_profile_equal_to_the_named_one_is_the_same_profile() {
    let _s = Stand::new(NAMED_AND_AMBIENT, "", &[("AWS_PROFILE", "named-prod")]);
    let mock = Mock::start();
    let key = signed_with(&mock, &s3(&mock.endpoint, Some("named-prod"))).unwrap();
    assert_eq!(key.as_deref(), Some("AKIANAMEDPROD"));
}

#[test]
fn ambient_access_keys_never_replace_a_named_profile() {
    let _s = Stand::new(
        NAMED_AND_AMBIENT,
        "",
        &[
            ("AWS_ACCESS_KEY_ID", "AKIAENVAMBIENT"),
            ("AWS_SECRET_ACCESS_KEY", "env-secret"),
        ],
    );
    let mock = Mock::start();
    let key = signed_with(&mock, &s3(&mock.endpoint, Some("named-prod"))).unwrap();
    assert_eq!(key.as_deref(), Some("AKIANAMEDPROD"));
}

#[test]
fn an_ambient_aws_profile_that_differs_is_refused_before_any_request() {
    let _s = Stand::new(NAMED_AND_AMBIENT, "", &[("AWS_PROFILE", "ambient-dev")]);
    let mock = Mock::start();
    let err = match signed_with(&mock, &s3(&mock.endpoint, Some("named-prod"))) {
        Err(e) => e,
        Ok(key) => panic!("the store was built and signed with {key:?}"),
    };
    let shown = refusal(&err);
    for needle in ["named-prod", "ambient-dev", "AWS_PROFILE", "unset"] {
        assert!(shown.contains(needle), "{needle}: {shown}");
    }
    assert_eq!(mock.request_count(), 0);
}

#[test]
fn a_missing_named_profile_is_refused_before_any_request() {
    let _s = Stand::new(
        NAMED_AND_AMBIENT,
        "",
        &[
            ("AWS_ACCESS_KEY_ID", "AKIAENVAMBIENT"),
            ("AWS_SECRET_ACCESS_KEY", "env-secret"),
        ],
    );
    let mock = Mock::start();
    let err = match signed_with(&mock, &s3(&mock.endpoint, Some("ghost"))) {
        Err(e) => e,
        Ok(key) => panic!("the store was built and signed with {key:?}"),
    };
    let shown = refusal(&err);
    for needle in ["ghost", "aws_profile", "credentials", "static"] {
        assert!(shown.contains(needle), "{needle}: {shown}");
    }
    assert_eq!(mock.request_count(), 0);
}

#[test]
fn a_named_profile_without_static_keys_is_refused_not_sent_to_the_instance_metadata() {
    let _s = Stand::new(
        "",
        "[profile sso-prod]\nsso_session = corp\nregion = eu-west-1\n",
        &[],
    );
    let mock = Mock::start();
    let err = match signed_with(&mock, &s3(&mock.endpoint, Some("sso-prod"))) {
        Err(e) => e,
        Ok(key) => panic!("the store was built and signed with {key:?}"),
    };
    let shown = refusal(&err);
    assert!(
        shown.contains("sso-prod") && shown.contains("export-credentials"),
        "{shown}"
    );
    assert_eq!(mock.request_count(), 0);
}

#[test]
fn a_named_profile_that_assumes_a_web_identity_role_is_accepted() {
    let _s = Stand::new(
        "",
        "[profile irsa]\nrole_arn = arn:aws:iam::1:role/r\nweb_identity_token_file = /tmp/none\n",
        &[],
    );
    let mock = Mock::start();
    let cfg = s3(&mock.endpoint, Some("irsa"));
    assert!(create_destination(&cfg).is_ok());
}

#[test]
fn no_named_profile_keeps_the_ambient_chain() {
    let _s = Stand::new(
        NAMED_AND_AMBIENT,
        "",
        &[
            ("AWS_ACCESS_KEY_ID", "AKIAENVAMBIENT"),
            ("AWS_SECRET_ACCESS_KEY", "env-secret"),
        ],
    );
    let mock = Mock::start();
    let key = signed_with(&mock, &s3(&mock.endpoint, None)).unwrap();
    assert_eq!(key.as_deref(), Some("AKIAENVAMBIENT"));
}

#[test]
fn named_key_env_vars_keep_signing_with_their_values() {
    let _s = Stand::new(
        NAMED_AND_AMBIENT,
        "",
        &[
            ("RIVET_TEST_S3_NAMED_AK", "AKIANAMEDENV"),
            ("RIVET_TEST_S3_NAMED_SK", "named-env-secret"),
            ("AWS_ACCESS_KEY_ID", "AKIAENVAMBIENT"),
            ("AWS_SECRET_ACCESS_KEY", "env-secret"),
        ],
    );
    let mock = Mock::start();
    let mut cfg = s3(&mock.endpoint, None);
    cfg.access_key_env = Some("RIVET_TEST_S3_NAMED_AK".into());
    cfg.secret_key_env = Some("RIVET_TEST_S3_NAMED_SK".into());
    let key = signed_with(&mock, &cfg).unwrap();
    assert_eq!(key.as_deref(), Some("AKIANAMEDENV"));
}
