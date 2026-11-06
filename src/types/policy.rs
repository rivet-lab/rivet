//! TypePolicy — strict / warn / allow decisions for unsafe type mappings.
//!
//! Roadmap §7 ("TypePolicy"). A `TypePolicy` is created from config or CLI
//! flags (e.g. `--strict`) and applied to the `Vec<TypeMapping>` that the
//! driver computes before the first row is read. Violations are collected
//! rather than raised immediately so the caller can print all problems at once.

use serde::Serialize;

use super::{RivetType, TypeFidelity, TypeMapping};

/// What to do when a mapping is classified as lossy or unsupported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyAction {
    /// Exit non-zero before the export starts.
    Fail,
    /// Print a warning and continue.
    Warn,
    /// Silently continue.
    Allow,
}

/// Per-export type-safety policy (roadmap §7).
///
/// The default is `strict` — both lossy and unsupported mappings fail.
/// `--no-strict` / future config can relax individual axes.
#[derive(Debug, Clone)]
pub struct TypePolicy {
    /// Action when a column mapping is [`TypeFidelity::Lossy`].
    pub on_lossy_mapping: PolicyAction,
    /// Action when a column mapping is [`TypeFidelity::Unsupported`].
    pub on_unsupported_type: PolicyAction,
}

impl Default for TypePolicy {
    fn default() -> Self {
        Self::strict()
    }
}

impl TypePolicy {
    /// Strict mode: both lossy and unsupported mappings are fatal.
    pub fn strict() -> Self {
        Self {
            on_lossy_mapping: PolicyAction::Fail,
            on_unsupported_type: PolicyAction::Fail,
        }
    }

    /// `strict()` under `--strict`, else `warn_only()`: the one default `check` and `run` share.
    pub fn from_strict(strict: bool) -> Self {
        if strict {
            Self::strict()
        } else {
            Self::warn_only()
        }
    }

    /// Permissive mode: warn only, never fail. Useful for `--type-report`
    /// when the user just wants to see the table without aborting.
    pub fn warn_only() -> Self {
        Self {
            on_lossy_mapping: PolicyAction::Warn,
            on_unsupported_type: PolicyAction::Warn,
        }
    }
}

/// One policy violation produced by [`TypePolicy::validate`].
#[derive(Debug, Clone, Serialize)]
pub struct PolicyViolation {
    /// Column name in the export query.
    pub column_name: String,
    /// Fidelity that triggered the violation.
    pub fidelity: TypeFidelity,
    /// Human-readable description (printed by the CLI and `--json` output).
    pub message: String,
    /// Whether this violation is fatal under the active policy.
    pub fatal: bool,
    /// The mapping itself refuses the column, whatever the policy.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub refused: bool,
}

impl TypePolicy {
    /// Validate `mappings` and return all violations (both warn and fail).
    pub fn validate(&self, mappings: &[TypeMapping]) -> Vec<PolicyViolation> {
        let mut out = Vec::new();
        for m in mappings {
            if let Some(remedy) = &m.refusal {
                out.push(PolicyViolation {
                    column_name: m.column_name.clone(),
                    fidelity: m.fidelity,
                    message: format!(
                        "column '{}' (source type '{}'): {remedy}",
                        m.column_name, m.source_native_type
                    ),
                    fatal: true,
                    refused: true,
                });
                continue;
            }
            let (action, fidelity) = match m.fidelity {
                TypeFidelity::Lossy => (self.on_lossy_mapping, TypeFidelity::Lossy),
                TypeFidelity::Unsupported => (self.on_unsupported_type, TypeFidelity::Unsupported),
                _ => continue,
            };
            if action == PolicyAction::Allow {
                continue;
            }
            let detail = match &m.rivet_type {
                RivetType::Unsupported { reason, .. } => format!(": {}", reason),
                _ => String::new(),
            };
            out.push(PolicyViolation {
                column_name: m.column_name.clone(),
                fidelity,
                message: format!(
                    "column '{}' (source type '{}'): fidelity={}{}",
                    m.column_name,
                    m.source_native_type,
                    fidelity.label(),
                    detail
                ),
                fatal: action == PolicyAction::Fail,
                refused: false,
            });
        }
        out
    }
}

/// The column plan: `mappings` under `policy`, each non-fatal violation logged, every fatal one in one coded error.
pub fn plan_columns(
    mappings: Vec<TypeMapping>,
    policy: &TypePolicy,
    subject: &str,
) -> crate::error::Result<Vec<TypeMapping>> {
    let violations = policy.validate(&mappings);
    for w in warnings(&violations, subject) {
        log::warn!("{w}");
    }
    refuse_fatal(&violations, subject)?;
    Ok(mappings)
}

/// The warning line for each non-fatal violation, prefixed with `subject`.
pub fn warnings(violations: &[PolicyViolation], subject: &str) -> Vec<String> {
    violations
        .iter()
        .filter(|v| !v.fatal)
        .map(|v| format!("{subject}: {}", v.message))
        .collect()
}

/// `Err` naming every fatal violation (`RIVET_TYPE_UNSAFE_MAPPING`), `Ok` when none is fatal.
pub fn refuse_fatal(violations: &[PolicyViolation], subject: &str) -> crate::error::Result<()> {
    let fatal: Vec<&PolicyViolation> = violations.iter().filter(|v| v.fatal).collect();
    if fatal.is_empty() {
        return Ok(());
    }
    let lines = fatal
        .iter()
        .map(|v| v.message.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    if fatal.iter().all(|v| v.refused) {
        crate::rivet_bail!(
            crate::error::codes::TYPE_UNSAFE_MAPPING,
            "{subject}: {} column(s) need a `columns:` declaration:\n{lines}",
            fatal.len()
        );
    }
    crate::rivet_bail!(
        crate::error::codes::TYPE_UNSAFE_MAPPING,
        "{subject}: strict mode: {} unsafe type mapping(s):\n{lines}",
        fatal.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SourceColumn, TypeMapping};

    fn unsupported_mapping(name: &str, native: &str) -> TypeMapping {
        let col = SourceColumn::simple(name, native, true);
        TypeMapping::from_source(
            &col,
            RivetType::Unsupported {
                native_type: native.into(),
                reason: "test reason".into(),
            },
        )
    }

    fn exact_mapping(name: &str, native: &str) -> TypeMapping {
        let col = SourceColumn::simple(name, native, true);
        TypeMapping::from_source(&col, crate::types::RivetType::Int64)
    }

    #[test]
    fn strict_policy_fails_on_unsupported() {
        let policy = TypePolicy::strict();
        let mappings = vec![
            exact_mapping("id", "int8"),
            unsupported_mapping("location", "geometry"),
        ];
        let violations = policy.validate(&mappings);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].fatal);
        assert_eq!(violations[0].column_name, "location");
        assert!(refuse_fatal(&violations, "export 'e'").is_err());
    }

    #[test]
    fn warn_only_policy_does_not_fail() {
        let policy = TypePolicy::warn_only();
        let mappings = vec![unsupported_mapping("dur", "interval")];
        let violations = policy.validate(&mappings);
        assert_eq!(violations.len(), 1);
        assert!(!violations[0].fatal);
        assert!(refuse_fatal(&violations, "export 'e'").is_ok());
    }

    #[test]
    fn allow_policy_produces_no_violations() {
        let policy = TypePolicy {
            on_lossy_mapping: PolicyAction::Allow,
            on_unsupported_type: PolicyAction::Allow,
        };
        let mappings = vec![unsupported_mapping("x", "hstore")];
        assert!(policy.validate(&mappings).is_empty());
    }

    #[test]
    fn exact_mappings_never_produce_violations() {
        let policy = TypePolicy::strict();
        let mappings = vec![
            exact_mapping("id", "int8"),
            TypeMapping::from_source(
                &SourceColumn::simple("name", "text", true),
                crate::types::RivetType::Text,
            ),
        ];
        assert!(policy.validate(&mappings).is_empty());
    }

    fn lossy_mapping(name: &str) -> TypeMapping {
        let mut m = exact_mapping(name, "timestamp(9)");
        m.fidelity = TypeFidelity::Lossy;
        m
    }

    #[test]
    fn plan_columns_under_warn_only_keeps_every_column() {
        let mappings = vec![
            exact_mapping("id", "int8"),
            unsupported_mapping("amount", "numeric"),
            lossy_mapping("ts"),
        ];
        let planned = plan_columns(mappings, &TypePolicy::from_strict(false), "export 'e'")
            .expect("warn_only never refuses");
        let names: Vec<&str> = planned.iter().map(|m| m.column_name.as_str()).collect();
        assert_eq!(names, ["id", "amount", "ts"]);
    }

    #[test]
    fn plan_columns_under_strict_refuses_naming_every_fatal_column_with_one_code() {
        let mappings = vec![
            exact_mapping("id", "int8"),
            unsupported_mapping("amount", "numeric"),
            lossy_mapping("ts"),
        ];
        let err = plan_columns(mappings, &TypePolicy::from_strict(true), "export 'e'")
            .expect_err("strict refuses lossy and unsupported");
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_TYPE_UNSAFE_MAPPING")
        );
        assert_eq!(
            crate::error::classify_exit(&err),
            1,
            "check --strict exits 1"
        );
        let msg = format!("{err:#}");
        assert!(
            msg.starts_with("export 'e': strict mode: 2 unsafe type mapping(s):"),
            "{msg}"
        );
        assert!(
            msg.contains("column 'amount' (source type 'numeric'): fidelity=unsupported"),
            "{msg}"
        );
        assert!(
            msg.contains("column 'ts' (source type 'timestamp(9)'): fidelity=lossy"),
            "{msg}"
        );
        assert!(!msg.contains("column 'id'"), "{msg}");
    }

    #[test]
    fn plan_columns_under_strict_passes_a_plan_with_no_unsafe_column() {
        let mappings = vec![exact_mapping("id", "int8")];
        assert_eq!(
            plan_columns(mappings, &TypePolicy::strict(), "export 'e'")
                .unwrap()
                .len(),
            1
        );
    }

    /// Only the non-fatal violations become warning lines, each prefixed with the subject.
    #[test]
    fn warnings_are_the_non_fatal_violations_prefixed_with_the_subject() {
        let v = |c: &str, fatal: bool| PolicyViolation {
            column_name: c.into(),
            fidelity: TypeFidelity::Lossy,
            message: format!("m-{c}"),
            fatal,
            refused: false,
        };
        assert_eq!(
            warnings(&[v("a", false), v("b", true), v("c", false)], "export 'e'"),
            vec!["export 'e': m-a".to_string(), "export 'e': m-c".to_string()]
        );
    }

    fn refused_mapping(name: &str) -> TypeMapping {
        lossy_mapping(name).with_refusal("declare it under `columns:`")
    }

    #[test]
    fn a_mapping_that_refuses_itself_is_fatal_under_every_policy() {
        let allow = TypePolicy {
            on_lossy_mapping: PolicyAction::Allow,
            on_unsupported_type: PolicyAction::Allow,
        };
        for policy in [TypePolicy::strict(), TypePolicy::warn_only(), allow] {
            let v = policy.validate(&[refused_mapping("ts")]);
            assert_eq!(v.len(), 1);
            assert!(v[0].fatal && v[0].refused, "{:?}", v[0]);
            assert_eq!(
                v[0].message,
                "column 'ts' (source type 'timestamp(9)'): declare it under `columns:`"
            );
        }
    }

    #[test]
    fn a_plain_lossy_mapping_is_still_only_a_warning_under_warn_only() {
        let v = TypePolicy::warn_only().validate(&[lossy_mapping("ts")]);
        assert_eq!(v.len(), 1);
        assert!(!v[0].fatal && !v[0].refused);
    }

    #[test]
    fn a_refusal_without_strict_names_no_strict_mode_and_carries_the_code() {
        let err = plan_columns(
            vec![refused_mapping("ts")],
            &TypePolicy::from_strict(false),
            "export 'e'",
        )
        .expect_err("a refusal is raised without --strict");
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_TYPE_UNSAFE_MAPPING")
        );
        assert_eq!(crate::error::classify_exit(&err), 1);
        let msg = format!("{err:#}");
        assert!(
            msg.starts_with("export 'e': 1 column(s) need a `columns:` declaration:\n"),
            "{msg}"
        );
        assert!(!msg.contains("strict mode"), "{msg}");
    }

    #[test]
    fn a_refusal_beside_a_strict_violation_keeps_the_strict_header_and_lists_both() {
        let err = plan_columns(
            vec![
                refused_mapping("ts"),
                unsupported_mapping("amount", "numeric"),
            ],
            &TypePolicy::from_strict(true),
            "export 'e'",
        )
        .expect_err("refused");
        let msg = format!("{err:#}");
        assert!(
            msg.starts_with("export 'e': strict mode: 2 unsafe type mapping(s):"),
            "{msg}"
        );
        assert!(
            msg.contains("column 'ts'") && msg.contains("column 'amount'"),
            "{msg}"
        );
    }
}
