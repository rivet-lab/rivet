use chrono::{DateTime, Utc};
use opendal::Operator;
use opendal::services::Azblob;

use super::cloud::{CloudBackend, CloudDestination};
use crate::config::{DestinationConfig, DestinationCredential};
use crate::error::Result;

/// Threshold under which a SAS token is considered "near expiry" and a warning is logged.
const SAS_NEAR_EXPIRY_THRESHOLD: chrono::Duration = chrono::Duration::minutes(60);

/// Result of parsing `se=` (signed-expiry) out of an Azure SAS token.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SasExpiryStatus {
    /// `se=` parsed and is in the future by more than `SAS_NEAR_EXPIRY_THRESHOLD`.
    Healthy { expires_at: DateTime<Utc> },
    /// `se=` parsed and is within `SAS_NEAR_EXPIRY_THRESHOLD` of `now` (still in the future).
    NearExpiry {
        expires_at: DateTime<Utc>,
        remaining: chrono::Duration,
    },
    /// `se=` parsed and is in the past — the destination cannot be opened.
    Expired { expires_at: DateTime<Utc> },
    /// `se=` is missing (legacy SAS signed by a stored access policy, or a non-standard token shape).
    NoExpiry,
    /// `se=` is present but the value did not parse as RFC3339.
    Unparseable { raw: String },
}

/// Parse `se=` (signed-expiry) out of a SAS token and classify it.
pub(crate) fn parse_sas_expiry_status(token: &str, now: DateTime<Utc>) -> SasExpiryStatus {
    let body = token.trim_start_matches('?');

    for pair in body.split('&') {
        let mut kv = pair.splitn(2, '=');
        let Some(key) = kv.next() else { continue };
        if key != "se" {
            continue;
        }
        let Some(raw) = kv.next() else {
            return SasExpiryStatus::Unparseable { raw: String::new() };
        };

        let decoded = raw
            .replace("%3A", ":")
            .replace("%3a", ":")
            .replace("%2B", "+")
            .replace("%2b", "+");

        match DateTime::parse_from_rfc3339(&decoded) {
            Ok(dt) => {
                let expires_at = dt.with_timezone(&Utc);
                if expires_at <= now {
                    return SasExpiryStatus::Expired { expires_at };
                }
                let remaining = expires_at - now;
                if remaining <= SAS_NEAR_EXPIRY_THRESHOLD {
                    return SasExpiryStatus::NearExpiry {
                        expires_at,
                        remaining,
                    };
                }
                return SasExpiryStatus::Healthy { expires_at };
            }
            Err(_) => {
                return SasExpiryStatus::Unparseable {
                    raw: decoded.to_string(),
                };
            }
        }
    }

    SasExpiryStatus::NoExpiry
}

/// Inspect a SAS token's `se=` and act on it.
fn enforce_sas_expiry(token: &str) -> Result<()> {
    match parse_sas_expiry_status(token, Utc::now()) {
        SasExpiryStatus::Healthy { .. } | SasExpiryStatus::NoExpiry => Ok(()),
        SasExpiryStatus::NearExpiry {
            expires_at,
            remaining,
        } => {
            let mins = remaining.num_minutes().max(0);
            log::warn!(
                "Azure SAS token expires in {} minute{} ({}). Long exports may fail mid-run; rotate the token before extraction.",
                mins,
                if mins == 1 { "" } else { "s" },
                expires_at.to_rfc3339()
            );
            Ok(())
        }
        SasExpiryStatus::Expired { expires_at } => {
            crate::rivet_bail!(
                crate::error::codes::DEST_CREDENTIALS_INVALID,
                "Azure SAS token already expired (se={}). Generate a new SAS and re-export.",
                expires_at.to_rfc3339()
            )
        }
        SasExpiryStatus::Unparseable { raw } => {
            log::warn!(
                "Azure SAS token has unparseable 'se=' value ({:?}); skipping expiry check. The token may still authenticate, but Rivet cannot warn on near-expiry.",
                raw
            );
            Ok(())
        }
    }
}

/// Azure Blob Storage destination.
pub type AzureDestination = CloudDestination<AzureBackend>;

/// Zero-sized backend marker carrying Azure's operator construction.
pub struct AzureBackend;

/// Read a credential from an env var into a [`crate::secret::Secret`].
fn read_credential_env(env_name: &str, label: &str) -> Result<crate::secret::Secret> {
    let value = std::env::var(env_name).map_err(|_| {
        crate::error::coded(
            crate::error::codes::CONFIG_SECRET_UNSET,
            format!("env var '{}' not set for Azure {}", env_name, label),
        )
    })?;
    let secret = crate::secret::Secret::new(value);
    secret.mask_in_output();
    Ok(secret)
}

impl CloudBackend for AzureBackend {
    const SCHEME: &'static str = "az";

    fn build_operator(config: &DestinationConfig) -> Result<Operator> {
        let container = config.bucket.as_deref().ok_or_else(|| {
            crate::error::coded(
                crate::error::codes::CONFIG_DESTINATION_SETTING_MISSING,
                "Azure destination requires 'bucket' (container name)",
            )
        })?;

        let mut builder = Azblob::default().container(container);

        match (&config.endpoint, &config.account_name) {
            (Some(endpoint), _) => {
                builder = builder.endpoint(endpoint);
            }
            (None, Some(account_name)) => {
                let derived = format!("https://{account_name}.blob.core.windows.net");
                builder = builder.endpoint(&derived);
            }
            (None, None) => {}
        }

        match DestinationCredential::choose(config, "destination")? {
            DestinationCredential::Anonymous => {
                log::info!("Azure: allow_anonymous (Azurite emulator or public container)");
            }
            DestinationCredential::AzureKey {
                account_name,
                key_env,
            } => {
                builder = builder.account_name(&account_name);
                let key = read_credential_env(&key_env, "account key")?;
                builder = builder.account_key(key.expose());
            }
            DestinationCredential::AzureSas {
                account_name,
                sas_env,
            } => {
                builder = builder.account_name(&account_name);
                let raw = read_credential_env(&sas_env, "SAS token")?;
                let token = raw.expose().trim_start_matches('?');
                enforce_sas_expiry(token)?;
                builder = builder.sas_token(token);
            }
            DestinationCredential::Ambient
            | DestinationCredential::GcsKeyFile(_)
            | DestinationCredential::S3Env { .. }
            | DestinationCredential::S3Profile(_) => {}
        }

        Ok(Operator::new(builder)?.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DestinationConfig, DestinationType};

    /// Helper: run `AzureDestination::new` expecting an error.
    fn expect_err(cfg: &DestinationConfig) -> String {
        match AzureDestination::new(cfg) {
            Err(e) => format!("{e:#}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn azure_destination_requires_bucket() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            account_name: Some("acct".into()),
            account_key_env: Some("AZURE_TEST_KEY_X".into()),
            ..Default::default()
        };
        // SAFETY: test-only; binary is single-threaded in this test context.
        unsafe { std::env::set_var("AZURE_TEST_KEY_X", "fake") };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("bucket") || msg.contains("container"),
            "missing bucket should be reported: {msg}"
        );
        unsafe { std::env::remove_var("AZURE_TEST_KEY_X") };
    }

    #[test]
    fn azure_destination_requires_account_name_when_not_anonymous() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_key_env: Some("AZURE_TEST_KEY_Y".into()),
            ..Default::default()
        };
        unsafe { std::env::set_var("AZURE_TEST_KEY_Y", "fake") };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("account_name"),
            "missing account_name should be reported: {msg}"
        );
        unsafe { std::env::remove_var("AZURE_TEST_KEY_Y") };
    }

    #[test]
    fn azure_destination_requires_account_key_env_when_not_anonymous() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("acct".into()),
            ..Default::default()
        };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("account_key_env"),
            "missing account_key_env should be reported: {msg}"
        );
    }

    /// The error text of a real failed request against a closed port with `sas` as the SAS token, redacted as the log sink redacts it.
    fn failed_request_text(sas: &str, var: &str) -> String {
        unsafe { std::env::set_var(var, sas) };
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("acct".into()),
            sas_token_env: Some(var.into()),
            endpoint: Some("http://127.0.0.1:1/acct".into()),
            ..Default::default()
        };
        let op = AzureBackend::build_operator(&cfg).expect("operator");
        let rt = crate::destination::cloud::io_runtime().expect("runtime");
        let err = rt.block_on(op.stat("probe")).expect_err("closed port");
        crate::redact::redact_secrets(&format!("{err:#}"))
    }

    #[test]
    fn a_sas_token_never_reaches_the_text_of_a_failed_request() {
        let plain = "QxPlainSasCanary0123VmPlainSasCanary4567";
        let signed = "sv=2022-11-02&se=2099-01-01T00:00:00Z&sp=rw&sr=c&sig=QxKeyCanary0123Vm";
        for (sas, var, canary) in [
            (plain, "RIVET_TEST_SAS_PLAIN_922", plain),
            (signed, "RIVET_TEST_SAS_SIGNED_922", "QxKeyCanary0123Vm"),
        ] {
            let text = failed_request_text(sas, var);
            assert!(!text.contains(canary), "SAS canary in error text: {text}");
        }
    }

    #[test]
    fn azure_destination_account_key_env_missing_var_errors() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("acct".into()),
            account_key_env: Some("RIVET_TEST_AZURE_KEY_UNSET_Z".into()),
            ..Default::default()
        };
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_KEY_UNSET_Z") };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("RIVET_TEST_AZURE_KEY_UNSET_Z"),
            "missing env var name in error: {msg}"
        );
        assert!(
            msg.contains("account key"),
            "missing credential label in error: {msg}"
        );
    }

    #[test]
    fn azure_destination_allow_anonymous_skips_credential_requirements() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            endpoint: Some("http://127.0.0.1:10000/devstoreaccount1".into()),
            allow_anonymous: true,
            ..Default::default()
        };
        let err = AzureDestination::new(&cfg).err().map(|e| format!("{e:#}"));
        if let Some(msg) = err {
            assert!(
                !msg.contains("account_name") && !msg.contains("account_key_env"),
                "allow_anonymous must bypass credential checks, got: {msg}"
            );
        }
    }

    #[test]
    fn azure_destination_auto_derives_endpoint_from_account_name() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("mystorageacct".into()),
            account_key_env: Some("RIVET_TEST_AZURE_ENDPOINT_FAKE".into()),
            ..Default::default()
        };
        // SAFETY: test-only; binary is single-threaded in this test context.
        unsafe { std::env::set_var("RIVET_TEST_AZURE_ENDPOINT_FAKE", "fake-key-value") };
        let result = AzureDestination::new(&cfg);
        if let Err(e) = result {
            let msg = format!("{e:#}").to_lowercase();
            assert!(
                !msg.contains("endpoint is empty"),
                "auto-derive must populate endpoint from account_name; got: {msg}"
            );
        }
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_ENDPOINT_FAKE") };
    }

    #[test]
    fn azure_destination_explicit_endpoint_wins_over_derived() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            endpoint: Some("http://127.0.0.1:10000/devstoreaccount1".into()),
            account_name: Some("devstoreaccount1".into()),
            account_key_env: Some("RIVET_TEST_AZURE_EXPLICIT_EP_FAKE".into()),
            ..Default::default()
        };
        unsafe { std::env::set_var("RIVET_TEST_AZURE_EXPLICIT_EP_FAKE", "fake") };
        let result = AzureDestination::new(&cfg);
        if let Err(e) = result {
            let msg = format!("{e:#}").to_lowercase();
            assert!(
                !msg.contains("endpoint is empty"),
                "explicit endpoint must reach builder; got: {msg}"
            );
        }
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_EXPLICIT_EP_FAKE") };
    }

    #[test]
    fn azure_destination_config_parses_from_yaml() {
        let yaml = r#"
type: azure
bucket: my-container
account_name: mystorageacct
account_key_env: AZURE_STORAGE_KEY
"#;
        let cfg: DestinationConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.destination_type, DestinationType::Azure);
        assert_eq!(cfg.bucket.as_deref(), Some("my-container"));
        assert_eq!(cfg.account_name.as_deref(), Some("mystorageacct"));
        assert_eq!(cfg.account_key_env.as_deref(), Some("AZURE_STORAGE_KEY"));
    }

    #[test]
    fn azure_destination_sas_token_env_satisfies_auth() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("mystorageacct".into()),
            sas_token_env: Some("RIVET_TEST_AZURE_SAS_OK".into()),
            ..Default::default()
        };
        // SAFETY: test-only; binary is single-threaded in this test context.
        unsafe { std::env::set_var("RIVET_TEST_AZURE_SAS_OK", "sv=2021-08-06&sig=fake") };
        let result = AzureDestination::new(&cfg);
        if let Err(e) = result {
            let msg = format!("{e:#}");
            assert!(
                !msg.contains("account_key_env"),
                "SAS-token auth must bypass account_key_env requirement; got: {msg}"
            );
        }
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_SAS_OK") };
    }

    #[test]
    fn azure_destination_rejects_account_key_and_sas_token_together() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("acct".into()),
            account_key_env: Some("RIVET_TEST_AZURE_BOTH_KEY".into()),
            sas_token_env: Some("RIVET_TEST_AZURE_BOTH_SAS".into()),
            ..Default::default()
        };
        unsafe { std::env::set_var("RIVET_TEST_AZURE_BOTH_KEY", "fake") };
        unsafe { std::env::set_var("RIVET_TEST_AZURE_BOTH_SAS", "fake") };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("account_key_env") && msg.contains("sas_token_env"),
            "error must name both fields: {msg}"
        );
        assert!(
            msg.contains("mutually exclusive"),
            "error must say mutually exclusive: {msg}"
        );
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_BOTH_KEY") };
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_BOTH_SAS") };
    }

    #[test]
    fn azure_destination_missing_both_account_key_and_sas_token_errors() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("acct".into()),
            ..Default::default()
        };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("account_key_env") && msg.contains("sas_token_env"),
            "error must name both auth modes: {msg}"
        );
    }

    #[test]
    fn azure_destination_sas_token_env_missing_var_errors_with_label() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("acct".into()),
            sas_token_env: Some("RIVET_TEST_AZURE_SAS_UNSET".into()),
            ..Default::default()
        };
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_SAS_UNSET") };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("RIVET_TEST_AZURE_SAS_UNSET"),
            "missing env var name in error: {msg}"
        );
        assert!(
            msg.contains("SAS token"),
            "credential label must mention SAS token: {msg}"
        );
    }

    #[test]
    fn azure_destination_sas_token_config_parses_from_yaml() {
        let yaml = r#"
type: azure
bucket: my-container
account_name: mystorageacct
sas_token_env: AZURE_STORAGE_SAS_TOKEN
"#;
        let cfg: DestinationConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.destination_type, DestinationType::Azure);
        assert_eq!(cfg.bucket.as_deref(), Some("my-container"));
        assert_eq!(cfg.account_name.as_deref(), Some("mystorageacct"));
        assert_eq!(
            cfg.sas_token_env.as_deref(),
            Some("AZURE_STORAGE_SAS_TOKEN")
        );
        assert!(cfg.account_key_env.is_none());
    }

    use chrono::TimeZone;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn sas_expiry_parses_far_future_as_healthy() {
        let token = "sv=2021-08-06&se=2099-01-01T00:00:00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        match parse_sas_expiry_status(token, now) {
            SasExpiryStatus::Healthy { expires_at } => {
                assert_eq!(expires_at, ts("2099-01-01T00:00:00Z"));
            }
            other => panic!("expected Healthy, got {other:?}"),
        }
    }

    #[test]
    fn sas_expiry_parses_within_threshold_as_near_expiry() {
        let token = "sv=2021-08-06&se=2026-05-23T12:30:00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        match parse_sas_expiry_status(token, now) {
            SasExpiryStatus::NearExpiry {
                expires_at,
                remaining,
            } => {
                assert_eq!(expires_at, ts("2026-05-23T12:30:00Z"));
                assert_eq!(remaining.num_minutes(), 30);
            }
            other => panic!("expected NearExpiry, got {other:?}"),
        }
    }

    #[test]
    fn an_expired_sas_and_a_doubled_auth_block_are_refused_by_code() {
        let err = enforce_sas_expiry("sv=2021-08-06&se=2024-01-01T00:00:00Z&sig=fake").unwrap_err();
        crate::error::assert_coded(&err, "RIVET_DEST_CREDENTIALS_INVALID", 1);
        let cfg = DestinationConfig {
            destination_type: crate::config::DestinationType::Azure,
            bucket: Some("c".into()),
            account_name: Some("acct".into()),
            account_key_env: Some("RIVET_TEST_AZ_KEY".into()),
            sas_token_env: Some("RIVET_TEST_AZ_SAS".into()),
            ..Default::default()
        };
        let err = AzureBackend::build_operator(&cfg).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_CONFIG_DESTINATION_AUTH_INVALID", 1);
        let err = AzureBackend::build_operator(&DestinationConfig {
            destination_type: crate::config::DestinationType::Azure,
            ..Default::default()
        })
        .unwrap_err();
        crate::error::assert_coded(&err, "RIVET_CONFIG_DESTINATION_SETTING_MISSING", 1);
    }

    #[test]
    fn sas_expiry_parses_past_as_expired() {
        let token = "sv=2021-08-06&se=2024-01-01T00:00:00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        match parse_sas_expiry_status(token, now) {
            SasExpiryStatus::Expired { expires_at } => {
                assert_eq!(expires_at, ts("2024-01-01T00:00:00Z"));
            }
            other => panic!("expected Expired, got {other:?}"),
        }
    }

    #[test]
    fn sas_expiry_treats_now_equals_se_as_expired() {
        let token = "sv=2021-08-06&se=2026-05-23T12:00:00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        match parse_sas_expiry_status(token, now) {
            SasExpiryStatus::Expired { .. } => {}
            other => panic!("expected Expired, got {other:?}"),
        }
    }

    #[test]
    fn sas_expiry_handles_url_encoded_colons() {
        let token = "sv=2021-08-06&se=2099-01-01T00%3A00%3A00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        assert!(matches!(
            parse_sas_expiry_status(token, now),
            SasExpiryStatus::Healthy { .. }
        ));
    }

    #[test]
    fn sas_expiry_handles_url_encoded_plus_in_timezone_offset() {
        let token = "sv=2021-08-06&se=2024-01-01T00%3A00%3A00%2B00%3A00&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        assert!(matches!(
            parse_sas_expiry_status(token, now),
            SasExpiryStatus::Expired { .. }
        ));
    }

    #[test]
    fn sas_expiry_returns_no_expiry_when_se_missing() {
        let token = "sv=2021-08-06&si=mypolicy&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        assert_eq!(
            parse_sas_expiry_status(token, now),
            SasExpiryStatus::NoExpiry
        );
    }

    #[test]
    fn sas_expiry_returns_unparseable_for_bad_format() {
        let token = "sv=2021-08-06&se=NOT_A_DATE&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        match parse_sas_expiry_status(token, now) {
            SasExpiryStatus::Unparseable { raw } => assert_eq!(raw, "NOT_A_DATE"),
            other => panic!("expected Unparseable, got {other:?}"),
        }
    }

    #[test]
    fn sas_expiry_tolerates_leading_question_mark() {
        let token = "?sv=2021-08-06&se=2099-01-01T00:00:00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        assert!(matches!(
            parse_sas_expiry_status(token, now),
            SasExpiryStatus::Healthy { .. }
        ));
    }

    #[test]
    fn sas_expiry_at_threshold_is_near_expiry() {
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        let exactly_at_threshold = now + SAS_NEAR_EXPIRY_THRESHOLD;
        let token = format!(
            "sv=2021-08-06&se={}&sig=fake",
            exactly_at_threshold.to_rfc3339()
        );
        assert!(matches!(
            parse_sas_expiry_status(&token, now),
            SasExpiryStatus::NearExpiry { .. }
        ));
    }

    #[test]
    fn sas_expiry_one_second_above_threshold_is_healthy() {
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        let above_threshold = now + SAS_NEAR_EXPIRY_THRESHOLD + chrono::Duration::seconds(1);
        let token = format!("sv=2021-08-06&se={}&sig=fake", above_threshold.to_rfc3339());
        assert!(matches!(
            parse_sas_expiry_status(&token, now),
            SasExpiryStatus::Healthy { .. }
        ));
    }

    #[test]
    fn azure_destination_rejects_expired_sas_token() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("mystorageacct".into()),
            sas_token_env: Some("RIVET_TEST_AZURE_SAS_EXPIRED".into()),
            ..Default::default()
        };
        unsafe {
            std::env::set_var(
                "RIVET_TEST_AZURE_SAS_EXPIRED",
                "sv=2021-08-06&se=2024-01-01T00:00:00Z&sig=fake",
            )
        };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("expired") || msg.contains("Expired"),
            "expired SAS must surface as expiry error: {msg}"
        );
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_SAS_EXPIRED") };
    }

    #[test]
    fn azure_destination_accepts_far_future_sas_token() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("mystorageacct".into()),
            sas_token_env: Some("RIVET_TEST_AZURE_SAS_HEALTHY".into()),
            ..Default::default()
        };
        unsafe {
            std::env::set_var(
                "RIVET_TEST_AZURE_SAS_HEALTHY",
                "sv=2021-08-06&se=2099-01-01T00:00:00Z&sig=fake",
            )
        };
        if let Err(e) = AzureDestination::new(&cfg) {
            let msg = format!("{e:#}");
            assert!(
                !msg.contains("expired") && !msg.contains("Expired"),
                "healthy SAS must pass expiry preflight; got: {msg}"
            );
        }
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_SAS_HEALTHY") };
    }
}
