//! The shared shape of a *preflight schema error*.

/// A permanent, author-fixable schema error surfaced by preflight.
pub(crate) struct PreflightSchemaError {
    detail: String,
    code_label: String,
}

impl PreflightSchemaError {
    pub(crate) fn new(detail: impl Into<String>, code_label: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
            code_label: code_label.into(),
        }
    }

    /// Render to the single, shared author-facing preflight error.
    pub(crate) fn into_error(self) -> anyhow::Error {
        crate::error::coded(
            crate::error::codes::SOURCE_QUERY_FAILED,
            format!(
                "preflight: {} ({}). Check the table/column names in the export's query, \
             and that the connecting user has SELECT on them.",
                self.detail, self.code_label
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_the_single_actionable_sentence() {
        let msg = format!(
            "{:#}",
            PreflightSchemaError::new("relation \"ordrs\" does not exist", "SQLSTATE 42P01")
                .into_error()
        );
        assert_eq!(
            msg,
            "preflight: relation \"ordrs\" does not exist (SQLSTATE 42P01). \
             Check the table/column names in the export's query, and that the \
             connecting user has SELECT on them."
        );
    }

    #[test]
    fn every_engine_lands_in_the_same_sentence() {
        for (detail, code) in [
            ("Unknown column 'totl' in 'field list'", "MySQL error 1054"),
            (
                "a table/view in the export's query does not exist",
                "Invalid object name 'ordrs'.",
            ),
        ] {
            let msg = format!("{:#}", PreflightSchemaError::new(detail, code).into_error());
            assert!(msg.starts_with(&format!("preflight: {detail} ({code}).")));
            assert!(
                msg.ends_with("and that the connecting user has SELECT on them."),
                "shared trailer drifted: {msg}"
            );
        }
    }
}
