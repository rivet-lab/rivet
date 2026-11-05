use super::*;
use crate::config::DestinationCredential as C;

fn dest(yaml: &str) -> DestinationConfig {
    serde_yaml_ng::from_str(yaml).expect("a destination")
}

fn chosen(yaml: &str) -> String {
    match C::choose(&dest(yaml), "export 'e'") {
        Ok(c) => format!("{c:?}"),
        Err(e) => format!("{}: {e}", crate::error::error_code(&e).unwrap_or("uncoded")),
    }
}

const REFUSED: &str = "RIVET_CONFIG_DESTINATION_AUTH_INVALID";

/// A destination naming no credential, or one complete credential, is chosen as written.
#[test]
fn one_named_credential_is_chosen_as_written() {
    assert_eq!(chosen("{type: gcs, bucket: b}"), "Ambient");
    assert_eq!(chosen("{type: local, path: /tmp/x}"), "Ambient");
    assert_eq!(
        chosen("{type: gcs, bucket: b, allow_anonymous: true}"),
        "Anonymous"
    );
    assert_eq!(
        chosen("{type: gcs, bucket: b, credentials_file: /k.json}"),
        "GcsKeyFile(\"/k.json\")"
    );
    assert_eq!(chosen("{type: s3, bucket: b}"), "Ambient");
    assert_eq!(
        chosen("{type: s3, bucket: b, aws_profile: p}"),
        "S3Profile(\"p\")"
    );
    assert_eq!(
        chosen("{type: s3, bucket: b, access_key_env: A, secret_key_env: S, session_token_env: T}"),
        "S3Env { access_key_env: Some(\"A\"), secret_key_env: Some(\"S\"), session_token_env: Some(\"T\") }"
    );
    assert_eq!(
        chosen("{type: azure, bucket: c, account_name: a, account_key_env: K}"),
        "AzureKey { account_name: \"a\", key_env: \"K\" }"
    );
    assert_eq!(
        chosen("{type: azure, bucket: c, account_name: a, sas_token_env: T}"),
        "AzureSas { account_name: \"a\", sas_env: \"T\" }"
    );
    assert_eq!(
        chosen("{type: azure, bucket: c, allow_anonymous: true}"),
        "Anonymous"
    );
}

/// A destination naming two identities is refused by the coded config error, naming the subject.
#[test]
fn two_named_identities_are_refused_by_code() {
    for (yaml, text) in [
        (
            "{type: s3, bucket: b, aws_profile: p, access_key_env: A, secret_key_env: S}",
            "export 'e': S3 aws_profile cannot be combined with access_key_env, secret_key_env or session_token_env: the profile alone would sign and the env keys would be ignored.\n  Fix: remove aws_profile to sign with the env keys, or remove the env key fields to sign with the profile.",
        ),
        (
            "{type: s3, bucket: b, aws_profile: p, session_token_env: T}",
            "export 'e': S3 aws_profile cannot be combined with access_key_env, secret_key_env or session_token_env",
        ),
        (
            "{type: s3, bucket: b, access_key_env: A}",
            "export 'e': S3 requires both access_key_env and secret_key_env, or neither (use default AWS credential chain)",
        ),
        (
            "{type: gcs, bucket: b, allow_anonymous: true, credentials_file: /k.json}",
            "export 'e': GCS allow_anonymous cannot be used together with credentials_file",
        ),
        (
            "{type: azure, bucket: c, account_name: a, account_key_env: K, sas_token_env: T}",
            "export 'e': Azure account_key_env and sas_token_env are mutually exclusive — pick one auth mode",
        ),
        (
            "{type: azure, bucket: c, allow_anonymous: true, account_name: a}",
            "export 'e': Azure allow_anonymous cannot be combined with account_name/account_key_env/sas_token_env",
        ),
        (
            "{type: azure, bucket: c, account_key_env: K}",
            "export 'e': Azure requires account_name (plus account_key_env or sas_token_env), or allow_anonymous: true for Azurite",
        ),
        (
            "{type: azure, bucket: c, account_name: a}",
            "export 'e': Azure requires account_key_env or sas_token_env (or allow_anonymous: true for Azurite)",
        ),
    ] {
        let got = chosen(yaml);
        assert!(
            got.starts_with(&format!("{REFUSED}: ")) && got.contains(text),
            "{yaml}\n got: {got}\nwant: {text}"
        );
    }
}
