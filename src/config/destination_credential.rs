use super::{DestinationConfig, DestinationType};
use crate::error::Result;

/// The one credential a destination names; every leg that signs with it reads it from here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DestinationCredential {
    Ambient,
    Anonymous,
    GcsKeyFile(String),
    S3Env {
        access_key_env: Option<String>,
        secret_key_env: Option<String>,
        session_token_env: Option<String>,
    },
    S3Profile(String),
    AzureKey {
        account_name: String,
        key_env: String,
    },
    AzureSas {
        account_name: String,
        sas_env: String,
    },
}

impl DestinationCredential {
    /// The credential `config` names, or the `RIVET_CONFIG_DESTINATION_AUTH_INVALID` refusal of a combination that names two identities; `subject` opens the message.
    pub(crate) fn choose(config: &DestinationConfig, subject: &str) -> Result<Self> {
        let refuse = |why: &str| {
            crate::error::coded(
                crate::error::codes::CONFIG_DESTINATION_AUTH_INVALID,
                format!("{subject}: {why}"),
            )
        };
        match config.destination_type {
            DestinationType::S3 => Self::s3(config, &refuse),
            DestinationType::Gcs => match (config.allow_anonymous, &config.credentials_file) {
                (true, Some(_)) => Err(refuse(
                    "GCS allow_anonymous cannot be used together with credentials_file",
                )),
                (true, None) => Ok(Self::Anonymous),
                (false, Some(file)) => Ok(Self::GcsKeyFile(file.clone())),
                (false, None) => Ok(Self::Ambient),
            },
            DestinationType::Azure => Self::azure(config, &refuse),
            DestinationType::Local | DestinationType::Stdout => Ok(Self::Ambient),
        }
    }

    fn s3(config: &DestinationConfig, refuse: &dyn Fn(&str) -> anyhow::Error) -> Result<Self> {
        let (ak, sk, st) = (
            &config.access_key_env,
            &config.secret_key_env,
            &config.session_token_env,
        );
        if ak.is_some() != sk.is_some() {
            return Err(refuse(
                "S3 requires both access_key_env and secret_key_env, or neither (use default AWS credential chain)",
            ));
        }
        match &config.aws_profile {
            Some(_) if ak.is_some() || st.is_some() => Err(refuse(
                "S3 aws_profile cannot be combined with access_key_env, secret_key_env or session_token_env: the profile alone would sign and the env keys would be ignored.\n  Fix: remove aws_profile to sign with the env keys, or remove the env key fields to sign with the profile.",
            )),
            Some(profile) => Ok(Self::S3Profile(profile.clone())),
            None if ak.is_some() || st.is_some() => Ok(Self::S3Env {
                access_key_env: ak.clone(),
                secret_key_env: sk.clone(),
                session_token_env: st.clone(),
            }),
            None => Ok(Self::Ambient),
        }
    }

    fn azure(config: &DestinationConfig, refuse: &dyn Fn(&str) -> anyhow::Error) -> Result<Self> {
        let (name, key, sas) = (
            &config.account_name,
            &config.account_key_env,
            &config.sas_token_env,
        );
        if config.allow_anonymous {
            return if name.is_some() || key.is_some() || sas.is_some() {
                Err(refuse(
                    "Azure allow_anonymous cannot be combined with account_name/account_key_env/sas_token_env",
                ))
            } else {
                Ok(Self::Anonymous)
            };
        }
        match (name, key, sas) {
            (_, Some(_), Some(_)) => Err(refuse(
                "Azure account_key_env and sas_token_env are mutually exclusive — pick one auth mode",
            )),
            (None, ..) => Err(refuse(
                "Azure requires account_name (plus account_key_env or sas_token_env), or allow_anonymous: true for Azurite",
            )),
            (Some(account_name), Some(key_env), None) => Ok(Self::AzureKey {
                account_name: account_name.clone(),
                key_env: key_env.clone(),
            }),
            (Some(account_name), None, Some(sas_env)) => Ok(Self::AzureSas {
                account_name: account_name.clone(),
                sas_env: sas_env.clone(),
            }),
            (Some(_), None, None) => Err(refuse(
                "Azure requires account_key_env or sas_token_env (or allow_anonymous: true for Azurite)",
            )),
        }
    }
}
