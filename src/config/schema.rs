//! **Layer: Config support** (JSON Schema generation, v0.7.3 P0.1/P0.2)

use schemars::Schema;
use schemars::generate::SchemaSettings;

use crate::config::Config;

/// Generate the JSON Schema for the top-level [`Config`] struct.
pub fn generate_config_schema() -> Schema {
    let settings = SchemaSettings::draft2020_12();
    let generator = settings.into_generator();
    let mut schema = generator.into_root_schema_for::<Config>();
    schema.insert(
        "title".into(),
        serde_json::Value::String(format!(
            "Rivet config (rivet-cli {})",
            env!("CARGO_PKG_VERSION")
        )),
    );
    schema
}

/// Render the schema as a pretty-printed JSON string, terminated by a trailing newline (POSIX text-file convention.
pub fn generate_config_schema_pretty() -> crate::error::Result<String> {
    let schema = generate_config_schema();
    let mut s = serde_json::to_string_pretty(&schema)?;
    if !s.ends_with('\n') {
        s.push('\n');
    }
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_carries_binary_version_in_title() {
        let schema = generate_config_schema();
        let title = schema
            .as_object()
            .and_then(|m| m.get("title"))
            .and_then(|v| v.as_str())
            .expect("schema must have a title");
        assert!(
            title.contains(env!("CARGO_PKG_VERSION")),
            "title must embed CARGO_PKG_VERSION ({}): {title}",
            env!("CARGO_PKG_VERSION"),
        );
    }

    #[test]
    fn schema_is_valid_json_object() {
        let pretty = generate_config_schema_pretty().expect("schema must serialize");
        let v: serde_json::Value =
            serde_json::from_str(&pretty).expect("rendered schema must parse as JSON");
        assert!(v.is_object(), "schema root must be a JSON object");
    }

    #[test]
    fn schema_ends_with_newline() {
        let pretty = generate_config_schema_pretty().unwrap();
        assert!(
            pretty.ends_with('\n'),
            "schema artifact must end with a trailing newline so POSIX tools / diffs behave",
        );
    }

    #[test]
    fn schema_mentions_top_level_required_fields() {
        let pretty = generate_config_schema_pretty().unwrap();
        assert!(pretty.contains("\"source\""), "must include 'source' field");
        assert!(
            pretty.contains("\"exports\""),
            "must include 'exports' field"
        );
    }
}
