//! Error-message quality and secret redaction contract tests.
//!
//! QA backlog Task 5.4. The `redact_for_artifact` round-trip is covered by
//! unit tests in `src/config/tests.rs`.  This file asserts the complementary
//! property: when config parsing or placeholder resolution *fails*, the
//! plaintext password must not surface in the resulting `anyhow::Error`
//! Display output.
//!
//! These checks protect CLI output, logs, and Slack/webhook payloads that
//! render `error_message` verbatim.

use rivet::config::{Config, resolve_vars};

/// Magic marker string used as a stand-in for a real secret.  Any test that
/// fails because this marker appears in an error message indicates a leak.
const SECRET_MARKER: &str = "TOP_SECRET_DO_NOT_LEAK_42";

#[test]
fn misplaced_tuning_error_does_not_leak_plaintext_password_in_url() {
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "postgresql://user:{SECRET_MARKER}@host:5432/db"
  batch_size: 1000
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#
    );
    let err = Config::from_yaml(&yaml).expect_err("misplaced batch_size must be rejected");
    let msg = format!("{err:#}");
    assert!(
        !msg.contains(SECRET_MARKER),
        "error message must not leak URL-embedded password: {msg}"
    );
}

#[test]
fn validation_error_does_not_leak_plaintext_password_field() {
    let yaml = format!(
        r#"
source:
  type: postgres
  host: db.example.com
  user: rivet
  password: "{SECRET_MARKER}"
  password_env: DB_PASS
  database: prod
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#
    );
    let err = Config::from_yaml(&yaml).expect_err("password + password_env must be rejected");
    let msg = format!("{err:#}");
    assert!(
        !msg.contains(SECRET_MARKER),
        "validation error must not echo plaintext password: {msg}"
    );
}

#[test]
fn missing_env_var_error_names_var_but_does_not_leak_surrounding_context() {
    // `resolve_vars` is called by `Config::load_with_params` before the YAML
    // parser sees any input.  A missing env var must be named (actionable)
    // but the resolver must not echo the full surrounding context — a secret
    // downstream of the placeholder would leak.
    unsafe {
        std::env::remove_var("RIVET_QA_MISSING_VAR_5_4");
    }
    let input =
        format!("url: postgresql://user:${{RIVET_QA_MISSING_VAR_5_4}}@host/db_{SECRET_MARKER}");
    let err = resolve_vars(&input, None).expect_err("missing env var must be rejected");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("RIVET_QA_MISSING_VAR_5_4"),
        "error must name the missing variable (actionable): {msg}"
    );
    assert!(
        !msg.contains(SECRET_MARKER),
        "error must not leak surrounding URL/db name: {msg}"
    );
}

#[test]
fn validation_error_for_exports_is_actionable() {
    // When validation fails for an export, the message must mention the
    // export name (operators run configs with dozens of exports).
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: critical_orders_export
    query: "SELECT * FROM orders"
    mode: incremental
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).expect_err("incremental without cursor_column is invalid");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("critical_orders_export"),
        "validation error must mention the failing export name: {msg}"
    );
    assert!(
        msg.contains("cursor_column"),
        "validation error must point at the missing field: {msg}"
    );
}

const SECRET_URL: &str = "postgresql://rivet:TOP_SECRET_DO_NOT_LEAK_42@db.example.com:5432/prod?sslmode=require&token=QUERY_TOKEN_42";

fn config_holding_every_literal_credential() -> Config {
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{SECRET_URL}"
notifications:
  slack:
    webhook_url: "https://hooks.slack.com/services/T0/B0/WEBHOOK_TOKEN_42"
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#
    );
    Config::from_yaml(&yaml).expect("config parses")
}

#[test]
fn debug_of_a_loaded_config_prints_no_credential() {
    let cfg = config_holding_every_literal_credential();
    for text in [format!("{cfg:?}"), format!("{cfg:#?}")] {
        for needle in [SECRET_MARKER, "QUERY_TOKEN_42", "WEBHOOK_TOKEN_42"] {
            assert!(!text.contains(needle), "{needle} reached Debug: {text}");
        }
    }
}

/// The source a plan artifact carries, as main (7eb33f93) serialized it for each source shape.
const MAIN_ARTIFACT_SOURCES: &[(&str, bool, &str)] = &[
    (
        "  type: postgres\n  url: \"postgresql://rivet:TOP_SECRET_DO_NOT_LEAK_42@db.example.com:5432/prod?sslmode=require&token=QUERY_TOKEN_42\"\n",
        true,
        r#"{"type":"postgres","url":"postgresql://REDACTED@db.example.com:5432/prod?sslmode=require&token=***","url_env":null,"url_file":null,"host":null,"port":null,"user":null,"password":null,"password_env":null,"database":null,"environment":null,"tuning":null,"tls":null,"mongo":null}"#,
    ),
    (
        "  type: postgres\n  host: db.example.com\n  user: rivet\n  password: \"TOP_SECRET_DO_NOT_LEAK_42\"\n  database: prod\n",
        true,
        r#"{"type":"postgres","url":null,"url_env":null,"url_file":null,"host":"db.example.com","port":null,"user":"rivet","password":null,"password_env":null,"database":"prod","environment":null,"tuning":null,"tls":null,"mongo":null}"#,
    ),
    (
        "  type: mysql\n  url: \"mysql://db.example.com:3306/prod\"\n",
        false,
        r#"{"type":"mysql","url":"mysql://db.example.com:3306/prod","url_env":null,"url_file":null,"host":null,"port":null,"user":null,"password":null,"password_env":null,"database":null,"environment":null,"tuning":null,"tls":null,"mongo":null}"#,
    ),
    (
        "  type: mssql\n  url: \"sqlserver://h;user=sa;password=p@TOP_SECRET_DO_NOT_LEAK_42;database=d\"\n",
        true,
        r#"{"type":"mssql","url":"sqlserver://h;user=sa;password=***;database=d","url_env":null,"url_file":null,"host":null,"port":null,"user":null,"password":null,"password_env":null,"database":null,"environment":null,"tuning":null,"tls":null,"mongo":null}"#,
    ),
];

#[test]
fn a_plan_artifact_source_serializes_byte_equal_to_main() {
    for (source, flag, expected) in MAIN_ARTIFACT_SOURCES {
        let yaml = format!(
            "source:\n{source}exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: csv\n    destination:\n      type: local\n      path: ./out\n"
        );
        let cfg = Config::from_yaml(&yaml).expect("config parses");
        let (safe, redacted) = cfg.source.redact_for_artifact();
        assert_eq!(redacted, *flag, "{source}");
        assert_eq!(
            serde_json::to_string(&safe).expect("source serializes"),
            *expected
        );
    }
}

#[test]
fn a_source_serialized_without_redaction_still_holds_no_credential() {
    let cfg = config_holding_every_literal_credential();
    let text = serde_json::to_string(&cfg.source).expect("source serializes");
    assert!(
        !text.contains(SECRET_MARKER) && !text.contains("QUERY_TOKEN_42"),
        "{text}"
    );
}

/// Whether a config property name holds a credential value rather than a reference to one.
fn holds_a_credential(name: &str) -> bool {
    let reference = ["_env", "_file", "_path", "_secret"]
        .iter()
        .any(|suffix| name.ends_with(suffix));
    let credential = name == "url"
        || name.ends_with("_url")
        || name.contains("password")
        || name.contains("token")
        || name.contains("private_key")
        || name.contains("account_key");
    credential && !reference
}

/// Every property under `node` named as a credential, with whether its schema is write-only.
fn credential_properties(node: &serde_json::Value, path: &str, out: &mut Vec<(String, bool)>) {
    match node {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::Object(props)) = map.get("properties") {
                for (name, sub) in props {
                    if holds_a_credential(name) {
                        out.push((format!("{path}.{name}"), sub["writeOnly"] == true));
                    }
                }
            }
            for (key, sub) in map {
                credential_properties(sub, &format!("{path}/{key}"), out);
            }
        }
        serde_json::Value::Array(items) => {
            for (i, sub) in items.iter().enumerate() {
                credential_properties(sub, &format!("{path}[{i}]"), out);
            }
        }
        _ => {}
    }
}

#[test]
fn every_credential_property_of_the_config_schema_is_a_write_only_secret() {
    let schema: serde_json::Value = serde_json::from_str(
        &rivet::config::generate_config_schema_pretty().expect("schema generates"),
    )
    .expect("schema is JSON");
    let mut found = Vec::new();
    credential_properties(&schema, "#", &mut found);
    let names: Vec<&str> = found.iter().map(|(p, _)| p.as_str()).collect();
    for expected in [".url", ".password", ".webhook_url"] {
        assert!(
            names.iter().any(|p| p.ends_with(expected)),
            "the walk reached the {expected} credentials: {names:?}"
        );
    }
    let plain: Vec<&str> = found
        .iter()
        .filter(|(_, write_only)| !write_only)
        .map(|(p, _)| p.as_str())
        .collect();
    assert!(
        plain.is_empty(),
        "a config credential is a plain string, not a Secret: {plain:?}"
    );
}
