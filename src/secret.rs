//! A credential that prints as a fixed marker; its value leaves only through [`Secret::expose`].

use std::fmt;

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Deserializer};
use zeroize::Zeroizing;

/// What every formatting of a [`Secret`] prints.
pub const REDACTED: &str = "REDACTED";

/// A credential value: Display and Debug print [`REDACTED`], the text is wiped on drop, nothing serializes it.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Secret(Zeroizing<String>);

impl Secret {
    /// Register this value so the shared redactor masks it wherever it appears in output text.
    pub fn mask_in_output(&self) {
        crate::redact::mask_value_in_output(&self.0);
    }

    /// A secret holding `value`, registered with the shared redactor so it is masked wherever it reaches output (ADR-0040 I1: by construction, not per call site).
    pub fn new(value: impl Into<String>) -> Self {
        Self::registered(Zeroizing::new(value.into()))
    }

    /// `value` as a secret, registered for masking unless it is a connection string: the URL and keyword scanners mask the password inside one, and the rest of it is not a credential.
    fn registered(value: Zeroizing<String>) -> Self {
        let secret = Self(value);
        if seeded!(
            "secret-value-not-registered",
            false,
            !is_connection_string(&secret.0)
        ) {
            secret.mask_in_output();
        }
        secret
    }

    /// The raw text: call it at the connect or request site that sends it, nowhere else.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Whether the secret holds no text.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A URL (`scheme://…`) or a keyword/value DSN (`host=h password=p`, `Server=h;Password=p`).
fn is_connection_string(value: &str) -> bool {
    value.contains("://") || (value.contains('=') && value.contains([' ', ';']))
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl PartialEq<&str> for Secret {
    fn eq(&self, other: &&str) -> bool {
        self.expose() == *other
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl std::str::FromStr for Secret {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(value))
    }
}

impl From<Zeroizing<String>> for Secret {
    fn from(value: Zeroizing<String>) -> Self {
        Self::registered(value)
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d).map(Self::new)
    }
}

impl JsonSchema for Secret {
    fn inline_schema() -> bool {
        String::inline_schema()
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        String::schema_name()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let mut schema = String::json_schema(generator);
        schema.insert("writeOnly".into(), true.into());
        schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_formatting_of_a_secret_prints_the_marker() {
        let s = Secret::new("p@ss/word?#:%é");
        for text in [
            format!("{s}"),
            format!("{s:?}"),
            format!("{s:#?}"),
            format!("{:>20}", s.to_string()),
            format!("{:?}", Some(&s)),
            format!("{:?}", vec![s.clone()]),
        ] {
            assert!(text.contains(REDACTED), "{text}");
            assert!(!text.contains("ss/word") && !text.contains('é'), "{text}");
        }
        assert_eq!(s.expose(), "p@ss/word?#:%é");
    }

    #[test]
    fn a_secret_deserializes_from_a_string_and_its_schema_is_a_write_only_string() {
        let s: Secret = serde_json::from_str("\"abc\"").unwrap();
        assert_eq!(s.expose(), "abc");
        assert!(serde_json::from_str::<Secret>("5").is_err());
        let mut generator = SchemaGenerator::default();
        assert_eq!(
            serde_json::to_value(Secret::json_schema(&mut generator)).unwrap(),
            serde_json::json!({"type": "string", "writeOnly": true})
        );
    }

    /// A token that reaches output inside text rivet does not control (a warehouse echoing the Authorization header in its 401 body) is masked because the Secret holding it registered itself.
    #[test]
    fn a_secret_is_masked_in_output_by_construction() {
        let token = "ya29.secret-is-masked-by-construction-0123456789";
        let _held = Secret::new(token);
        let echoed = anyhow::anyhow!("HTTP 401: Invalid credentials: Bearer {token}");
        let shown = crate::redact::redact_error(&echoed);
        assert!(!shown.contains(token), "{shown}");
        assert!(shown.contains("Invalid credentials: Bearer ***"), "{shown}");
        let _from_zeroizing: Secret =
            Zeroizing::new("zeroized-token-0123456789".to_string()).into();
        assert!(
            !crate::redact::redact_secrets("got zeroized-token-0123456789")
                .contains("zeroized-token-0123456789")
        );
        let url = "postgresql://reader@db.internal:5432/app-by-construction";
        let _plain = Secret::new(url);
        assert_eq!(
            crate::redact::redact_secrets(url),
            url,
            "a URL is left to the URL scanner"
        );
        let dsn = "host=h user=u password=DsnPassword7 dbname=by-construction";
        let _dsn = Secret::new(dsn);
        assert_eq!(
            crate::redact::redact_secrets(dsn),
            "host=h user=u password=*** dbname=by-construction",
            "a keyword DSN is left to the keyword scanner"
        );
    }

    #[test]
    fn a_struct_deriving_debug_over_a_secret_prints_no_value() {
        #[derive(Debug)]
        struct Holder {
            _url: Option<Secret>,
        }
        let h = Holder {
            _url: Some(Secret::new("postgresql://u:hunter2@h/d")),
        };
        assert_eq!(format!("{h:?}"), "Holder { _url: Some(REDACTED) }");
    }
}
