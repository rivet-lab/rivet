//! `rivet init` introspection for MongoDB.

use mongodb::bson::Document;

use super::TableInfo;
use crate::error::Result;
use crate::source::mongo::MongoSession;

/// Connect once (GATED — refuses remote plaintext) so one session serves the whole `list_tables` + per-collection `introspect` scan.
pub(super) fn connect(url: &str, tls: Option<&crate::config::TlsConfig>) -> Result<MongoSession> {
    MongoSession::connect(url, tls)
}

/// List the user collections in the URL's database, skipping the internal `system.*` namespaces so the scaffold never emits an export for them.
pub(super) fn list_tables(session: &MongoSession) -> Result<Vec<String>> {
    let db = session.client().database(session.db());
    let all = session.block_on(db.list_collection_names().into_future())?;
    let plain = session.block_on(
        db.list_collection_names()
            .filter(mongodb::bson::doc! { "type": "collection" })
            .into_future(),
    )?;
    let skipped: Vec<&String> = all
        .iter()
        .filter(|n| !n.starts_with("system.") && !plain.contains(n))
        .collect();
    if !skipped.is_empty() {
        eprintln!(
            "rivet: {} view(s)/time-series collection(s) left out — a scan of them cannot keep \
             its cursor open (noCursorTimeout); export their source collections instead: {}",
            skipped.len(),
            skipped
                .iter()
                .map(|n| n.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(plain
        .into_iter()
        .filter(|n| !n.starts_with("system."))
        .collect())
}

/// One collection → a [`TableInfo`] with a scan-free row estimate and no columns (schemaless).
pub(super) fn introspect(session: &MongoSession, collection: &str) -> Result<TableInfo> {
    let count = session
        .block_on(async {
            session
                .client()
                .database(session.db())
                .collection::<Document>(collection)
                .estimated_document_count()
                .await
        })
        .unwrap_or(0);
    Ok(TableInfo {
        cdc_capture_instance: None,
        cdc_unmineable: None,
        density: None,
        schema: session.db().to_string(),
        table: collection.to_string(),
        row_estimate: i64::try_from(count).unwrap_or(0),
        total_bytes: None,
        columns: Vec::new(),
    })
}
