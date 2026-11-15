use crate::error::Result;

use super::StateStore;

/// One column in a schema snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

/// Compute the canonical schema fingerprint for a set of columns.
pub fn schema_fingerprint(columns: &[SchemaColumn]) -> String {
    use xxhash_rust::xxh3::Xxh3;

    let mut sorted: Vec<&SchemaColumn> = columns.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));

    let mut h = Xxh3::new();
    for c in &sorted {
        h.update(c.name.as_bytes());
        h.update(b"\t");
        h.update(c.data_type.as_bytes());
        h.update(b"\n");
    }
    format!("xxh3:{:016x}", h.digest())
}

/// Convert the dest-facing Arrow schema into the `Vec<SchemaColumn>` the trust contract uses.
pub fn arrow_schema_to_columns(schema: &arrow::datatypes::Schema) -> Vec<SchemaColumn> {
    schema
        .fields()
        .iter()
        .map(|f| SchemaColumn {
            name: f.name().clone(),
            data_type: format!("{:?}", f.data_type()),
        })
        .collect()
}

/// Diff between two schema snapshots.
#[derive(Debug)]
pub struct SchemaChange {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub type_changed: Vec<(String, String, String)>,
}

impl SchemaChange {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.type_changed.is_empty()
    }
}

/// Schema history store — reads and writes `export_schema`.
impl StateStore {
    pub fn get_stored_schema(&self, export_name: &str) -> Result<Option<Vec<SchemaColumn>>> {
        let json = self.query_opt(
            "SELECT columns_json FROM export_schema WHERE export_name = ?1",
            &[export_name.into()],
            |r| r.text(0),
        )?;
        Ok(match json {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        })
    }

    pub fn store_schema(&self, export_name: &str, columns: &[SchemaColumn]) -> Result<()> {
        let json = serde_json::to_string(columns)?;
        let now = chrono::Utc::now().to_rfc3339();
        self.execute(
            "INSERT INTO export_schema (export_name, columns_json, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(export_name) DO UPDATE SET
                columns_json = excluded.columns_json,
                updated_at = excluded.updated_at",
            &[export_name.into(), json.into(), now.into()],
        )?;
        Ok(())
    }

    /// Store the schema ONLY when no baseline exists yet.
    pub fn store_schema_if_absent(
        &self,
        export_name: &str,
        columns: &[SchemaColumn],
    ) -> Result<bool> {
        if self.get_stored_schema(export_name)?.is_some() {
            return Ok(false);
        }
        self.store_schema(export_name, columns)?;
        Ok(true)
    }

    /// Detect structural drift versus the stored snapshot.
    pub fn detect_schema_change(
        &self,
        export_name: &str,
        current: &[SchemaColumn],
    ) -> Result<Option<SchemaChange>> {
        let stored = match self.get_stored_schema(export_name)? {
            Some(s) => s,
            None => {
                self.store_schema(export_name, current)?;
                return Ok(None);
            }
        };

        let stored_map: std::collections::HashMap<&str, &str> = stored
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.as_str()))
            .collect();
        let current_map: std::collections::HashMap<&str, &str> = current
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.as_str()))
            .collect();

        let added: Vec<String> = current
            .iter()
            .filter(|c| !stored_map.contains_key(c.name.as_str()))
            .map(|c| format!("{} ({})", c.name, c.data_type))
            .collect();

        let removed: Vec<String> = stored
            .iter()
            .filter(|c| !current_map.contains_key(c.name.as_str()))
            .map(|c| c.name.clone())
            .collect();

        let type_changed: Vec<(String, String, String)> = current
            .iter()
            .filter_map(|c| {
                stored_map.get(c.name.as_str()).and_then(|old_type| {
                    if *old_type != c.data_type.as_str() {
                        Some((c.name.clone(), old_type.to_string(), c.data_type.clone()))
                    } else {
                        None
                    }
                })
            })
            .collect();

        let change = SchemaChange {
            added,
            removed,
            type_changed,
        };

        if change.is_empty() {
            Ok(None)
        } else {
            Ok(Some(change))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    #[test]
    fn first_schema_stored_no_change() {
        let s = store();
        let cols = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "name".into(),
                data_type: "Utf8".into(),
            },
        ];
        let change = s.detect_schema_change("orders", &cols).unwrap();
        assert!(change.is_none(), "first run should detect no change");
        assert!(s.get_stored_schema("orders").unwrap().is_some());
    }

    #[test]
    fn same_schema_no_change() {
        let s = store();
        let cols = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        s.detect_schema_change("t", &cols).unwrap();
        let change = s.detect_schema_change("t", &cols).unwrap();
        assert!(change.is_none());
    }

    #[test]
    fn store_schema_if_absent_preserves_the_drift_baseline() {
        let s = store();
        let v1 = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        let v2 = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "added".into(),
                data_type: "Utf8".into(),
            },
        ];
        assert!(
            s.store_schema_if_absent("t", &v1).unwrap(),
            "first open: baseline absent → stored",
        );
        assert!(
            !s.store_schema_if_absent("t", &v2).unwrap(),
            "baseline present → open-forensics must preserve it, not overwrite",
        );
        let change = s.detect_schema_change("t", &v2).unwrap();
        assert!(
            change.is_some(),
            "an added column must be detected — a store-at-open must not clobber the baseline",
        );
    }

    #[test]
    fn added_column_detected() {
        let s = store();
        let v1 = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "email".into(),
                data_type: "Utf8".into(),
            },
        ];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.added.len(), 1);
        assert!(change.added[0].contains("email"));
    }

    #[test]
    fn removed_column_detected() {
        let s = store();
        let v1 = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "old_field".into(),
                data_type: "Utf8".into(),
            },
        ];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.removed, vec!["old_field"]);
    }

    #[test]
    fn type_change_detected() {
        let s = store();
        let v1 = vec![SchemaColumn {
            name: "price".into(),
            data_type: "Float64".into(),
        }];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![SchemaColumn {
            name: "price".into(),
            data_type: "Utf8".into(),
        }];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.type_changed.len(), 1);
        assert_eq!(
            change.type_changed[0],
            ("price".into(), "Float64".into(), "Utf8".into())
        );
    }

    #[test]
    fn fail_policy_does_not_store_new_schema() {
        let s = store();
        let v1 = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "new_col".into(),
                data_type: "Utf8".into(),
            },
        ];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.added.len(), 1);

        let stored = s.get_stored_schema("t").unwrap().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].name, "id");

        let change2 = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(
            change2.added.len(),
            1,
            "fail policy must re-detect on next run"
        );
    }

    #[test]
    fn warn_policy_stores_new_schema_after_change() {
        let s = store();
        let v1 = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "extra".into(),
                data_type: "Utf8".into(),
            },
        ];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.added.len(), 1);

        s.store_schema("t", &v2).unwrap();

        let no_change = s.detect_schema_change("t", &v2).unwrap();
        assert!(
            no_change.is_none(),
            "after store, same schema must produce no change"
        );
    }

    fn col(name: &str, ty: &str) -> SchemaColumn {
        SchemaColumn {
            name: name.into(),
            data_type: ty.into(),
        }
    }

    #[test]
    fn fingerprint_format_is_xxh3_prefix_plus_16_hex() {
        let fp = schema_fingerprint(&[col("id", "Int64")]);
        assert!(fp.starts_with("xxh3:"), "fp = {fp}");
        let hex = &fp["xxh3:".len()..];
        assert_eq!(hex.len(), 16, "fp = {fp}");
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "fp = {fp}"
        );
    }

    #[test]
    fn fingerprint_is_order_independent() {
        let a = vec![col("id", "Int64"), col("name", "Utf8")];
        let b = vec![col("name", "Utf8"), col("id", "Int64")];
        assert_eq!(schema_fingerprint(&a), schema_fingerprint(&b));
    }

    #[test]
    fn fingerprint_changes_on_rename() {
        let a = vec![col("id", "Int64")];
        let b = vec![col("user_id", "Int64")];
        assert_ne!(schema_fingerprint(&a), schema_fingerprint(&b));
    }

    #[test]
    fn fingerprint_changes_on_retype() {
        let a = vec![col("price", "Int64")];
        let b = vec![col("price", "Float64")];
        assert_ne!(schema_fingerprint(&a), schema_fingerprint(&b));
    }

    #[test]
    fn fingerprint_changes_on_column_add_or_remove() {
        let a = vec![col("id", "Int64")];
        let b = vec![col("id", "Int64"), col("email", "Utf8")];
        assert_ne!(schema_fingerprint(&a), schema_fingerprint(&b));
    }

    #[test]
    fn fingerprint_is_stable_across_invocations() {
        let cols = vec![col("a", "Int64"), col("b", "Utf8"), col("c", "Float64")];
        let fp1 = schema_fingerprint(&cols);
        let fp2 = schema_fingerprint(&cols);
        let fp3 = schema_fingerprint(&cols);
        assert_eq!(fp1, fp2);
        assert_eq!(fp2, fp3);
    }

    #[test]
    fn fingerprint_distinguishes_split_columns() {
        let a = vec![col("ab", "Int64"), col("c", "Utf8")];
        let b = vec![col("a", "Int64"), col("bc", "Utf8")];
        assert_ne!(schema_fingerprint(&a), schema_fingerprint(&b));
    }

    #[test]
    fn fingerprint_empty_input_is_well_defined() {
        let fp = schema_fingerprint(&[]);
        assert!(fp.starts_with("xxh3:"));
    }
}
