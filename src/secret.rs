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

/// Shortest secret registered for masking: a shorter one would blank ordinary text.
const MIN_MASKED_LEN: usize = 8;

/// The text of every secret registered by [`Secret::mask_in_output`], raw and percent-encoded.
static MASKED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// `text` with every registered secret value replaced by `***`.
pub(crate) fn mask_registered(text: &str) -> String {
    let known = MASKED.lock().unwrap_or_else(|e| e.into_inner());
    known
        .iter()
        .fold(text.to_string(), |acc, v| acc.replace(v.as_str(), "***"))
}

impl Secret {
    /// Register this value so the shared redactor masks it wherever it appears in output text.
    pub fn mask_in_output(&self) {
        if self.0.len() < MIN_MASKED_LEN {
            return;
        }
        let encoded =
            percent_encoding::utf8_percent_encode(&self.0, percent_encoding::NON_ALPHANUMERIC)
                .to_string();
        let mut known = MASKED.lock().unwrap_or_else(|e| e.into_inner());
        for v in [self.0.to_string(), encoded] {
            if !known.contains(&v) {
                known.push(v);
            }
        }
    }

    /// A secret holding `value`.
    pub fn new(value: impl Into<String>) -> Self {
        Self(Zeroizing::new(value.into()))
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
        Self(value)
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
