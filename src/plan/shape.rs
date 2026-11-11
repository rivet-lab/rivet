//! The byte-shaping projection of a plan: every field that decides the bytes of the parts a run writes, derived from the plan itself.

use std::collections::BTreeSet;

use serde_json::Value;
use xxhash_rust::xxh3::xxh3_64;

use super::ResolvedRunPlan;
use crate::state::RunShape;

/// Plan fields that do not shape the bytes a run writes; a field absent here is pinned by default.
pub(crate) const NOT_SHAPING: &[&str] = &[
    "export_name",
    "bytes_read",
    "parts_landed",
    "source_table",
    "query_template",
    "catalog_query",
    "destination_written",
    "is_split_unit",
    "split_window",
    "single_pass_of",
    "collection_incarnation",
    "strategy",
    "destination",
    "quality",
    "tuning",
    "tuning_profile_label",
    "validate",
    "reconcile",
    "resume",
    "verify",
    "source",
    "schema_drift_policy",
    "shape_drift_warn_factor",
];

/// Plan fields that shape the bytes a run writes, recorded even when the plan omits them.
pub(crate) const SHAPING: &[&str] = &[
    "base_query",
    "format",
    "compression",
    "compression_level",
    "max_file_size_bytes",
    "skip_empty",
    "meta_columns",
    "column_overrides",
    "parquet",
    "partition_rollover",
];

/// The config key an operator edits to change a plan field, where it is not the field's own name.
const KEYS: &[(&str, &str)] = &[
    ("base_query", "query"),
    ("column_overrides", "columns"),
    ("max_file_size_bytes", "max_file_size"),
];

/// `value` with every object's keys in sorted order, as one string.
fn canonical(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let sorted: BTreeSet<&String> = map.keys().collect();
            let fields: Vec<String> = sorted
                .into_iter()
                .map(|k| format!("{}:{}", Value::from(k.as_str()), canonical(&map[k])))
                .collect();
            format!("{{{}}}", fields.join(","))
        }
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(canonical).collect();
            format!("[{}]", items.join(","))
        }
        other => other.to_string(),
    }
}

/// The digest of one plan field; a field the plan omits has its own.
fn digest(value: Option<&Value>) -> String {
    let text = value.map_or_else(|| "absent".to_string(), canonical);
    format!("{:016x}", xxh3_64(text.as_bytes()))
}

impl ResolvedRunPlan {
    /// What shapes the bytes this plan writes: a digest per field that is not listed as non-shaping, by the config key that sets it.
    pub(crate) fn run_shape(&self) -> RunShape {
        let Value::Object(mut fields) = serde_json::to_value(self)
            .expect("a plan serializes: sealed plans hold the same bytes")
        else {
            unreachable!("a plan serializes as an object");
        };
        fields.insert(
            "base_query".into(),
            Value::from(self.query_template.as_deref().unwrap_or(&self.base_query)),
        );
        let names: BTreeSet<&str> = fields
            .keys()
            .map(String::as_str)
            .filter(|name| !NOT_SHAPING.contains(name))
            .chain(SHAPING.iter().copied())
            .collect();
        RunShape::new(
            names
                .into_iter()
                .map(|name| {
                    let key = KEYS
                        .iter()
                        .find(|(field, _)| *field == name)
                        .map_or(name, |(_, key)| key);
                    (key.to_string(), digest(fields.get(name)))
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompressionType, FormatType};
    use crate::plan::test_support::test_plan;

    /// The field names of `ResolvedRunPlan`, read from its declaration.
    fn plan_fields() -> Vec<String> {
        let source = include_str!("contract.rs");
        let start = source
            .find("pub struct ResolvedRunPlan {")
            .expect("the plan struct");
        source[start..]
            .lines()
            .skip(1)
            .take_while(|l| *l != "}")
            .filter_map(|l| l.strip_prefix("    pub "))
            .filter_map(|l| l.split_once(':').map(|(name, _)| name.to_string()))
            .collect()
    }

    #[test]
    fn every_plan_field_is_listed_as_shaping_or_not_exactly_once() {
        let fields = plan_fields();
        assert!(fields.len() > 20, "the declaration was read: {fields:?}");
        for field in &fields {
            let listed = NOT_SHAPING.contains(&field.as_str()) as u8
                + SHAPING.contains(&field.as_str()) as u8;
            assert_eq!(
                listed, 1,
                "plan field `{field}` must be in exactly one of SHAPING and NOT_SHAPING (src/plan/shape.rs)"
            );
        }
        for listed in NOT_SHAPING.iter().chain(SHAPING) {
            assert!(
                fields.iter().any(|f| f == listed),
                "`{listed}` is listed in src/plan/shape.rs but is no field of the plan"
            );
        }
    }

    #[test]
    fn the_projection_records_exactly_the_shaping_fields() {
        let keys: BTreeSet<String> = test_plan().run_shape().names();
        let want: BTreeSet<String> = SHAPING
            .iter()
            .map(|f| {
                KEYS.iter()
                    .find(|(field, _)| field == f)
                    .map_or(*f, |(_, key)| key)
                    .to_string()
            })
            .collect();
        assert_eq!(keys, want);
    }

    #[test]
    fn each_shaping_field_edited_changes_the_shape_and_no_other_does() {
        let base = test_plan();
        type Edit = Box<dyn Fn(&mut ResolvedRunPlan)>;
        let edits: Vec<(&str, Edit)> = vec![
            ("query", Box::new(|p| p.base_query = "SELECT 2".into())),
            ("format", Box::new(|p| p.format = FormatType::Csv)),
            (
                "compression",
                Box::new(|p| p.compression = CompressionType::Zstd),
            ),
            (
                "compression_level",
                Box::new(|p| p.compression_level = Some(3)),
            ),
            (
                "max_file_size",
                Box::new(|p| p.max_file_size_bytes = Some(1 << 20)),
            ),
            ("skip_empty", Box::new(|p| p.skip_empty = true)),
            (
                "meta_columns",
                Box::new(|p| p.meta_columns.exported_at = true),
            ),
            (
                "columns",
                Box::new(|p| {
                    p.column_overrides
                        .insert("a".into(), crate::types::RivetType::Int64);
                }),
            ),
            (
                "parquet",
                Box::new(|p| p.parquet = Some(Default::default())),
            ),
        ];
        for (key, edit) in &edits {
            let mut edited = base.clone();
            edit(&mut edited);
            let changed = edited.run_shape();
            let was = base.run_shape();
            assert_eq!(changed.changed_from(&was), vec![*key], "editing `{key}`");
        }
        let mut quiet = base.clone();
        quiet.validate = true;
        quiet.resume = true;
        quiet.reconcile = true;
        quiet.tuning_profile_label = "safe".into();
        quiet.shape_drift_warn_factor = 2.0;
        quiet.export_name = "other".into();
        assert!(quiet.run_shape().changed_from(&base.run_shape()).is_empty());
    }

    #[test]
    fn a_value_substituted_into_the_query_is_not_an_edit() {
        let mut a = test_plan();
        a.query_template = Some("SELECT ${n}".into());
        a.base_query = "SELECT 1".into();
        let mut b = a.clone();
        b.base_query = "SELECT 2".into();
        assert_eq!(
            b.run_shape().changed_from(&a.run_shape()),
            Vec::<&str>::new()
        );
        b.query_template = Some("SELECT ${n} + 1".into());
        assert_eq!(b.run_shape().changed_from(&a.run_shape()), vec!["query"]);
    }

    #[test]
    fn column_overrides_digest_does_not_depend_on_insertion_order() {
        use crate::types::RivetType;
        let mut a = test_plan();
        let mut b = test_plan();
        for name in ["a", "b", "c", "d", "e", "f"] {
            a.column_overrides.insert(name.into(), RivetType::Int64);
        }
        for name in ["f", "e", "d", "c", "b", "a"] {
            b.column_overrides.insert(name.into(), RivetType::Int64);
        }
        assert_eq!(a.run_shape(), b.run_shape());
    }
}
