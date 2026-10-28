//! Google Secret Manager: a source credential read by its secret name, once per process.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use base64::Engine as _;
use zeroize::Zeroizing;

use crate::destination::gcs_auth;
use anyhow::{Context as _, Result};

use crate::error::codes;

const ENDPOINT: &str = "https://secretmanager.googleapis.com/v1";

/// The value of the secret version `reference` names, fetched at most once per process.
///
/// `reference` is `projects/<project>/secrets/<secret>` (the `latest` version) or
/// `projects/<project>/secrets/<secret>/versions/<n|latest>`. The token comes from the
/// same ADC credential the BigQuery and GCS legs use (`GOOGLE_APPLICATION_CREDENTIALS`).
pub(crate) fn gcp_secret(reference: &str) -> Result<Zeroizing<String>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Zeroizing<String>>>> = OnceLock::new();
    let name = version_name(reference)?;
    let mut cache = CACHE
        .get_or_init(Default::default)
        .lock()
        .expect("secret cache poisoned");
    if let Some(value) = cache.get(&name) {
        return Ok(value.clone());
    }
    // reqwest's blocking client panics inside a tokio runtime, and a source URL is
    // resolved from async callers too; a fresh thread is never inside one.
    let fetched = {
        let name = name.clone();
        std::thread::spawn(move || fetch_with_adc(ENDPOINT, &name))
            .join()
            .map_err(|_| anyhow::anyhow!("Secret Manager fetch thread panicked"))??
    };
    cache.insert(name, fetched.clone());
    Ok(fetched)
}

/// The full version resource name `reference` names; refuses anything else.
fn version_name(reference: &str) -> Result<String> {
    let parts: Vec<&str> = reference.trim().split('/').collect();
    let version = match parts.as_slice() {
        ["projects", p, "secrets", s] if valid_id(p) && valid_id(s) => "latest",
        ["projects", p, "secrets", s, "versions", v]
            if valid_id(p)
                && valid_id(s)
                && (*v == "latest" || (!v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))) =>
        {
            v
        }
        _ => crate::config_bail!(
            codes::CONFIG_SOURCE_CONNECTION_INVALID,
            "source: '{reference}' is not a Secret Manager secret name.\n  Hint: write \
             `projects/<project>/secrets/<secret>` (the latest version) or \
             `projects/<project>/secrets/<secret>/versions/<n>`"
        ),
    };
    Ok(format!(
        "projects/{}/secrets/{}/versions/{version}",
        parts[1], parts[3]
    ))
}

fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 255
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn fetch_with_adc(endpoint: &str, name: &str) -> Result<Zeroizing<String>> {
    let Some(creds) = gcs_auth::load_adc_credentials()? else {
        crate::rivet_bail!(
            codes::CONFIG_GCP_SECRET_UNREADABLE,
            "source: no Google credentials to read Secret Manager secret '{name}' — point \
             GOOGLE_APPLICATION_CREDENTIALS at a service-account key file (or run \
             `gcloud auth application-default login`)"
        );
    };
    let http = reqwest::blocking::Client::new();
    let tokens = gcs_auth::shared_blocking_source(creds, &http);
    let project = name.split('/').nth(1).unwrap_or_default();
    let token = tokens.access_token()?;
    fetch(
        &http,
        endpoint,
        name,
        &token,
        tokens.quota_project_header(project),
    )
}

/// One `versions.access` call: the decoded payload, or a coded refusal naming the secret.
fn fetch(
    http: &reqwest::blocking::Client,
    endpoint: &str,
    name: &str,
    token: &str,
    quota_project: Option<&str>,
) -> Result<Zeroizing<String>> {
    let mut req = http
        .get(format!("{endpoint}/{name}:access"))
        .bearer_auth(token);
    if let Some(p) = quota_project {
        req = req.header("x-goog-user-project", p);
    }
    let resp = req
        .send()
        .with_context(|| format!("Secret Manager request for '{name}' failed"))?;
    let status = resp.status();
    if !status.is_success() {
        let hint = match status.as_u16() {
            403 => {
                "grant the credential rivet runs as `roles/secretmanager.secretAccessor` on this secret"
            }
            404 => "check the project, secret name and version",
            _ => "check the secret and the Secret Manager API in this project",
        };
        crate::rivet_bail!(
            codes::CONFIG_GCP_SECRET_UNREADABLE,
            "source: Secret Manager refused '{name}' (HTTP {}).\n  Hint: {hint}",
            status.as_u16()
        );
    }
    let body: serde_json::Value = resp
        .json()
        .with_context(|| format!("Secret Manager reply for '{name}' is not JSON"))?;
    let data = body["payload"]["data"].as_str().unwrap_or_default();
    let bytes = Zeroizing::new(
        base64::engine::general_purpose::STANDARD
            .decode(data)
            .with_context(|| format!("Secret Manager payload of '{name}' is not base64"))?,
    );
    let text = std::str::from_utf8(&bytes).map_err(|_| {
        crate::error::coded(
            codes::CONFIG_GCP_SECRET_UNREADABLE,
            format!("source: Secret Manager secret '{name}' is not UTF-8 text"),
        )
    })?;
    Ok(Zeroizing::new(text.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load::bq_rest::stand::{Stand, json};

    #[test]
    fn a_secret_name_resolves_to_its_version_and_anything_else_is_refused() {
        assert_eq!(
            version_name("projects/p-1/secrets/db_url").unwrap(),
            "projects/p-1/secrets/db_url/versions/latest"
        );
        assert_eq!(
            version_name(" projects/p/secrets/s/versions/3 ").unwrap(),
            "projects/p/secrets/s/versions/3"
        );
        for bad in [
            "",
            "db_url",
            "projects/p/secrets/",
            "projects/p/secrets/s/versions/",
            "projects/p/secrets/s/versions/x",
            "projects/p/secrets/../versions/1",
            "projects/p/secrets/s?alt=media",
            "projects/p/secrets/s/versions/1/extra",
        ] {
            let err = version_name(bad).expect_err(bad);
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_CONFIG_SOURCE_CONNECTION_INVALID"),
                "{bad}"
            );
        }
    }

    #[test]
    fn the_payload_is_decoded_and_a_refusal_names_the_secret_and_the_role() {
        let stand = Stand::start(|seen, _| match seen.path.as_str() {
            "/projects/p/secrets/ok/versions/latest:access" => {
                assert_eq!(seen.authorization, "Bearer tok");
                // "postgresql://u:p%40ss@h/db\n"
                json(
                    200,
                    r#"{"payload":{"data":"cG9zdGdyZXNxbDovL3U6cCU0MHNzQGgvZGIK"}}"#,
                )
            }
            "/projects/p/secrets/denied/versions/latest:access" => json(403, "{}"),
            _ => json(404, "{}"),
        });
        let http = reqwest::blocking::Client::new();
        let get = |n: &str| {
            fetch(
                &http,
                stand.endpoint(),
                &format!("projects/p/secrets/{n}/versions/latest"),
                "tok",
                None,
            )
        };
        assert_eq!(get("ok").unwrap().as_str(), "postgresql://u:p%40ss@h/db\n");
        for (n, hint) in [("denied", "secretAccessor"), ("absent", "secret name")] {
            let err = get(n).expect_err(n);
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_CONFIG_GCP_SECRET_UNREADABLE")
            );
            let msg = format!("{err:#}");
            assert!(msg.contains(n) && msg.contains(hint), "{msg}");
        }
    }
}
