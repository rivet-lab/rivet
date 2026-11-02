use std::path::Path;
use zeroize::Zeroizing;

/// Anchor a relative `written` path to `config_dir`, so one config names one key file from every working directory.
pub(crate) fn anchor(config_dir: &Path, written: &str) -> String {
    if Path::new(written).is_absolute() {
        return written.to_string();
    }
    super::destination::normalized(&config_dir.join(written))
        .to_string_lossy()
        .into_owned()
}

/// Read the Google key file a config names: missing is `CONFIG_SECRET_UNSET`; a directory, an unreadable, empty or malformed file, or JSON that is no service-account, impersonated or external-account key is `DEST_CREDENTIALS_INVALID`; every refusal names `field` and the path, and none contacts a network.
pub(crate) fn google_key(field: &str, path: &str) -> crate::error::Result<Zeroizing<Vec<u8>>> {
    let invalid = |why: String| {
        crate::error::coded(
            crate::error::codes::DEST_CREDENTIALS_INVALID,
            format!(
                "{field} '{path}' {why}.\n  Fix: point {field} at a service-account key JSON, or remove it to use the ambient Google credentials (ADC); a named key is never replaced by another identity."
            ),
        )
    };
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(crate::error::coded(
                crate::error::codes::CONFIG_SECRET_UNSET,
                format!("{field} '{path}' does not exist"),
            ));
        }
        Err(e) => return Err(invalid(format!("cannot be read: {e}"))),
    };
    if !meta.is_file() {
        return Err(invalid("is a directory, not a key file".into()));
    }
    let bytes =
        Zeroizing::new(std::fs::read(path).map_err(|e| invalid(format!("cannot be read: {e}")))?);
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Err(invalid("is empty".into()));
    }
    match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(serde_json::Value::Object(_)) => {}
        Ok(_) => return Err(invalid("is not a JSON object".into())),
        Err(e) => return Err(invalid(format!("is not valid JSON ({e})"))),
    }
    reqsign::GoogleCredential::from_slice(&bytes).map_err(|_| {
        invalid(
            "is JSON but not a service_account, impersonated_service_account or external_account key (an authorized_user file from `gcloud auth application-default login` is not accepted here)"
                .into(),
        )
    })?;
    Ok(bytes)
}
