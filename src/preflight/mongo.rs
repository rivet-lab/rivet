//! MongoDB preflight diagnostics — the document-store analogue of [`super::postgres`]/[`super::mysql`]/[`super::mssql`].

use super::ExportDiagnostic;
use super::analysis::*;
use crate::config::{ExportConfig, TlsConfig};
use crate::error::Result;

/// Connect and build one [`ExportDiagnostic`] per export (multi-export `check`).
pub(super) fn check_mongo(
    url: &str,
    tls: Option<&TlsConfig>,
    exports: &[&ExportConfig],
    mongo: Option<&crate::config::MongoConfig>,
) -> Result<Vec<ExportDiagnostic>> {
    super::collect_diagnostics(exports, |export| diagnose_mongo(url, tls, export, mongo))
}

/// Diagnose a single export without printing — used by `rivet plan`.
pub(super) fn diagnose_export_mongo(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
    mongo: Option<&crate::config::MongoConfig>,
) -> Result<ExportDiagnostic> {
    diagnose_mongo(url, tls, export, mongo)
}

/// The collection a batch export reads; `None` for a CDC stream, which may capture a collection before its first write.
fn batch_collection(export: &ExportConfig) -> Option<&str> {
    export
        .table
        .as_deref()
        .filter(|_| export.mode != crate::config::ExportMode::Cdc)
}

fn diagnose_mongo(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
    mongo: Option<&crate::config::MongoConfig>,
) -> Result<ExportDiagnostic> {
    if let Some(collection) = batch_collection(export) {
        crate::source::mongo::MongoSession::connect(url, tls)?.require_collection(collection)?;
    }
    let row_estimate = export
        .table
        .as_deref()
        .and_then(|coll| crate::source::mongo::estimated_count(url, tls, coll));

    let keyset = mongo.is_some_and(|m| m.page_size.is_some());
    let uses_index = keyset;
    let strategy = if keyset {
        format!(
            "keyset(_id, page_size={})",
            mongo.and_then(|m| m.page_size).unwrap_or(0)
        )
    } else {
        derive_strategy(export)
    };
    let verdict = compute_verdict(row_estimate, uses_index, false, None, export.parallel);
    let recommended_profile = recommend_profile(row_estimate, uses_index, export);
    let recommended_parallel = recommend_parallelism(export, row_estimate, uses_index);
    let db_max_connections = crate::source::mongo::max_connections(url, tls);
    let warnings = collect_warnings(export, row_estimate, None, None, None, db_max_connections);

    Ok(ExportDiagnostic {
        row_source: None,
        export_name: export.name.clone(),
        strategy,
        mode: "full".to_string(),
        cursor_column: None,
        row_estimate,
        avg_row_bytes: None,
        cursor_min: None,
        cursor_max: None,
        scan_type: Some("collection scan (full)".to_string()),
        uses_index,
        verdict,
        recommended_profile,
        recommended_parallel,
        warnings,
        suggestion: None,
        chunk_min: None,
        chunk_max: None,
        db_max_connections,
    })
}

#[cfg(test)]
mod tests {
    use super::batch_collection;
    use crate::config::ExportMode;

    /// A batch export's collection must exist before it is diagnosed; a CDC stream's may not exist yet.
    #[test]
    fn only_a_batch_export_requires_its_collection() {
        let mut e = crate::config::sample_export("e");
        e.table = Some("orders".into());
        e.mode = ExportMode::Full;
        assert_eq!(batch_collection(&e), Some("orders"));
        e.mode = ExportMode::Cdc;
        assert_eq!(batch_collection(&e), None);
        e.mode = ExportMode::Full;
        e.table = None;
        assert_eq!(batch_collection(&e), None);
    }
}
