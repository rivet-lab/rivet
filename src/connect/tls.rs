//! Postgres transport security: [`TlsConfig`] into a `native_tls::TlsConnector`.

use crate::config::{TlsConfig, TlsMode};
use crate::error::Result;

/// Build a `native_tls::TlsConnector` from a [`TlsConfig`].
///
/// Caller is responsible for branching on `cfg.mode == TlsMode::Disable` and
/// using plaintext (`NoTls` / no `ssl_opts`) in that path — this function
/// assumes TLS is enforced.
pub(crate) fn build_native_tls(cfg: &TlsConfig) -> Result<native_tls::TlsConnector> {
    debug_assert!(
        cfg.mode.is_enforced(),
        "build_native_tls called with Disable mode"
    );

    let mut builder = native_tls::TlsConnector::builder();

    match cfg.mode {
        TlsMode::Require => {
            builder.danger_accept_invalid_certs(true);
            builder.danger_accept_invalid_hostnames(true);
        }
        TlsMode::VerifyCa => {
            builder.danger_accept_invalid_hostnames(true);
        }
        TlsMode::VerifyFull => {}
        TlsMode::Disable => unreachable!(),
    }

    if let Some(path) = &cfg.ca_file {
        let pem = std::fs::read(path).map_err(|e| {
            crate::error::coded(
                crate::error::codes::CONFIG_TLS_CA_INVALID,
                format!("tls: cannot read ca_file: {}", e),
            )
        })?;
        let cert = native_tls::Certificate::from_pem(&pem).map_err(|e| {
            crate::error::coded(
                crate::error::codes::CONFIG_TLS_CA_INVALID,
                format!("tls: ca_file is not valid PEM: {}", e),
            )
        })?;
        builder.disable_built_in_roots(true);
        builder.add_root_certificate(cert);
    }

    if cfg.accept_invalid_certs {
        builder.danger_accept_invalid_certs(true);
    }
    if cfg.accept_invalid_hostnames {
        builder.danger_accept_invalid_hostnames(true);
    }

    builder.build().map_err(|e| {
        crate::error::coded(
            crate::error::codes::SOURCE_CONNECT_FAILED,
            format!("tls: could not initialize native-tls connector: {}", e),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(mode: TlsMode) -> TlsConfig {
        TlsConfig {
            mode,
            ca_file: None,
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
        }
    }

    #[test]
    fn an_unreadable_or_non_pem_ca_file_is_refused_by_code() {
        let mut c = cfg(TlsMode::VerifyFull);
        c.ca_file = Some("/nonexistent/rivet-test-ca.pem".into());
        let err = build_native_tls(&c).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_CONFIG_TLS_CA_INVALID", 1);
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(f.path(), b"not a certificate").unwrap();
        c.ca_file = Some(f.path().to_string_lossy().into_owned());
        let err = build_native_tls(&c).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_CONFIG_TLS_CA_INVALID", 1);
    }

    #[test]
    fn builder_succeeds_for_enforced_modes() {
        assert!(build_native_tls(&cfg(TlsMode::Require)).is_ok());
        assert!(build_native_tls(&cfg(TlsMode::VerifyCa)).is_ok());
        assert!(build_native_tls(&cfg(TlsMode::VerifyFull)).is_ok());
    }

    #[test]
    fn missing_ca_file_surfaces_clear_error() {
        let c = TlsConfig {
            mode: TlsMode::VerifyFull,
            ca_file: Some("/nonexistent/rivet-test-ca.pem".into()),
            ..Default::default()
        };
        let err = build_native_tls(&c).unwrap_err();
        assert!(err.to_string().contains("cannot read ca_file"), "{err}");
    }

    #[test]
    fn invalid_pem_is_rejected() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"not a pem cert").unwrap();
        let c = TlsConfig {
            mode: TlsMode::VerifyFull,
            ca_file: Some(tmp.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        let err = build_native_tls(&c).unwrap_err();
        assert!(err.to_string().contains("not valid PEM"), "{err}");
    }
}
