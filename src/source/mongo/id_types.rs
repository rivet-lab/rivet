//! The `_id` BSON types a keyed MongoDB CDC collection has delivered, persisted in the checkpoint.

use std::collections::{BTreeMap, BTreeSet};

use mongodb::bson::{Bson, Document, doc};

use super::{MongoSession, id_bracket};
use crate::error::Result;
use crate::source::cdc::Position;

/// The checkpoint key holding the seen `_id` brackets per collection.
pub(crate) const CHECKPOINT_KEY: &str = "idt";

/// The bracket recorded for an `_id` type no keyset bracket covers.
const UNPLACED: u8 = u8::MAX;

/// The `_id` brackets seen per collection; numeric types share one bracket.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct IdTypes(BTreeMap<String, BTreeSet<u8>>);

/// The bracket of one `_id` value.
fn bracket_of(id: &Bson) -> u8 {
    id_bracket(id).unwrap_or(UNPLACED)
}

/// The name of a bracket as the operator knows the BSON type.
fn bracket_name(bracket: u8) -> &'static str {
    match bracket {
        0 => "number (int, long, double or decimal)",
        1 => "null",
        2 => "string",
        3 => "document",
        4 => "array",
        5 => "binData",
        6 => "objectId",
        7 => "bool",
        8 => "date",
        9 => "timestamp",
        10 => "regex",
        _ => "other",
    }
}

impl IdTypes {
    /// The set a checkpoint holds; a checkpoint without the key (or an unreadable one) holds none.
    pub(crate) fn from_checkpoint(v: &serde_json::Value) -> Self {
        let Some(map) = v.get(CHECKPOINT_KEY).and_then(serde_json::Value::as_object) else {
            return Self::default();
        };
        Self(
            map.iter()
                .map(|(coll, list)| {
                    let brackets = list
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(serde_json::Value::as_u64)
                        .filter_map(|b| u8::try_from(b).ok())
                        .collect();
                    (coll.clone(), brackets)
                })
                .collect(),
        )
    }

    /// `v` with this set under the checkpoint key; an empty set leaves `v` byte-identical.
    pub(crate) fn stamp_checkpoint(&self, mut v: serde_json::Value) -> serde_json::Value {
        if self.0.is_empty() {
            return v;
        }
        if let Some(obj) = v.as_object_mut() {
            obj.insert(
                CHECKPOINT_KEY.to_string(),
                serde_json::json!(
                    self.0
                        .iter()
                        .map(|(coll, set)| (coll.clone(), set.iter().collect::<Vec<_>>()))
                        .collect::<BTreeMap<_, _>>()
                ),
            );
        }
        v
    }

    /// Add `id`'s bracket to `collection` unless that makes the collection mixed, which refuses.
    pub(crate) fn admit(&mut self, collection: &str, id: &Bson) -> Result<()> {
        let mut set = self.0.get(collection).cloned().unwrap_or_default();
        set.insert(bracket_of(id));
        if set.len() > 1 {
            return Err(mixed_refusal(collection, &set));
        }
        self.0.insert(collection.to_string(), set);
        Ok(())
    }

    /// Add the brackets of `ids` to `collection`, refusing if the collection is then mixed.
    fn absorb(&mut self, collection: &str, ids: &[&Bson]) -> Result<()> {
        let set = self.0.entry(collection.to_string()).or_default();
        set.extend(ids.iter().map(|id| bracket_of(id)));
        match set.len() > 1 {
            true => Err(mixed_refusal(collection, set)),
            false => Ok(()),
        }
    }

    /// Refuse when the set already holds more than one bracket for `collection`.
    pub(crate) fn refuse_if_mixed(&self, collection: &str) -> Result<()> {
        match self.0.get(collection) {
            Some(set) if set.len() > 1 => Err(mixed_refusal(collection, set)),
            _ => Ok(()),
        }
    }

    /// The brackets seen for `collection`, for tests.
    #[cfg(test)]
    pub(crate) fn brackets(&self, collection: &str) -> Vec<u8> {
        self.0
            .get(collection)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }
}

/// The refusal of a keyed collection that holds `_id` values of more than one BSON type.
fn mixed_refusal(collection: &str, brackets: &BTreeSet<u8>) -> anyhow::Error {
    let types = brackets
        .iter()
        .map(|b| bracket_name(*b))
        .collect::<Vec<_>>()
        .join(", ");
    anyhow::Error::new(crate::error::CodedError::new(
        crate::error::codes::SOURCE_ID_TYPES_MIXED,
        format!(
            "mongodb collection `{collection}` holds `_id` values of more than one BSON type \
             ({types}), and its `load:` block keys the current-state view on the flat `_id` \
             column, which renders different types to the same text (int 1 and string \"1\" \
             both become \"1\"): the view would merge distinct documents and a delete of one \
             would remove the other. This run wrote no change of a second type. Load the \
             collection with `mode: full`, or make its `_id` values one BSON type, drop the \
             change-log table, delete the CDC checkpoint and re-run for a fresh snapshot \
             (loading mixed-type `_id` under a keyed load is tracked in #926)"
        ),
    ))
}

/// The export's tables whose `load:` block merges by declared key columns.
pub(crate) fn keyed_tables(
    config: &crate::config::Config,
    export: &crate::config::ExportConfig,
) -> Vec<String> {
    crate::config::cdc_captured_tables(export)
        .into_iter()
        .filter(|t| {
            crate::plan::load_settings::effective_load(config, export, Some(t))
                .and_then(|l| l.pk.declared())
                .is_some_and(|cols| !cols.is_empty())
        })
        .collect()
}

/// The min and max `_id` of `collection` by BSON order, or `None` for an empty one.
pub(crate) fn id_span_on(
    session: &MongoSession,
    collection: &str,
) -> Result<Option<(Document, Document)>> {
    let coll = session
        .client()
        .database(session.db())
        .collection::<Document>(collection);
    session.block_on(async {
        let lo = coll.find_one(doc! {}).sort(doc! { "_id": 1 }).await?;
        let hi = coll.find_one(doc! {}).sort(doc! { "_id": -1 }).await?;
        Ok::<_, anyhow::Error>(lo.zip(hi))
    })
}

/// The persisted set of a checkpoint file; an absent file holds none.
pub(crate) fn persisted(checkpoint: Option<&std::path::Path>) -> Result<IdTypes> {
    Ok(match checkpoint {
        Some(p) => Position::load(p)?
            .map(|pos| IdTypes::from_checkpoint(pos.json()))
            .unwrap_or_default(),
        None => IdTypes::default(),
    })
}

/// Admit one change's `_id` into `types` when its collection is keyed; other collections and keyless events pass.
pub(crate) fn admit_change(
    types: &mut IdTypes,
    keyed: &[String],
    collection: Option<&str>,
    document_key: Option<&Document>,
) -> Result<()> {
    match (collection, document_key.and_then(|k| k.get("_id"))) {
        (Some(coll), Some(id)) if keyed.iter().any(|k| k == coll) => types.admit(coll, id),
        _ => Ok(()),
    }
}

/// Refuse when a keyed collection already holds more than one `_id` type, read through `span_of` and from `known`.
pub(crate) fn guard_keyed(
    mut known: IdTypes,
    keyed: &[String],
    mut span_of: impl FnMut(&str) -> Result<Option<(Document, Document)>>,
) -> Result<IdTypes> {
    for collection in keyed {
        let span = match span_of(collection) {
            Ok(span) => span,
            Err(e) => {
                log::warn!(
                    "mongodb cdc: could not read the `_id` range of `{collection}` to check it \
                     for mixed `_id` types ({e:#}); every change of the stream is still checked"
                );
                None
            }
        };
        let ends: Vec<&Bson> = span
            .iter()
            .flat_map(|(lo, hi)| [lo.get("_id"), hi.get("_id")])
            .flatten()
            .collect();
        known.absorb(collection, &ends)?;
    }
    Ok(known)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::oid::ObjectId;
    use serde_json::json;

    #[test]
    fn a_checkpoint_without_the_key_holds_no_types() {
        let old = json!({"_data": "82", "rt": "0e", "ct": 7, "ord": 3});
        assert_eq!(IdTypes::from_checkpoint(&old), IdTypes::default());
        assert_eq!(IdTypes::default().stamp_checkpoint(old.clone()), old);
    }

    #[test]
    fn the_set_round_trips_through_the_checkpoint_and_leaves_the_resume_fields() {
        let mut t = IdTypes::default();
        t.admit("orders", &Bson::Int32(1)).unwrap();
        t.admit("orders", &Bson::Int64(2)).unwrap();
        t.admit("logs", &Bson::ObjectId(ObjectId::new())).unwrap();
        let ckpt = t.stamp_checkpoint(json!({"_data": "82", "rt": "0e", "ct": 7, "ord": 3}));
        assert_eq!(ckpt["_data"], "82");
        assert_eq!(ckpt["ct"], 7);
        assert_eq!(ckpt[CHECKPOINT_KEY]["orders"], json!([0]));
        assert_eq!(ckpt[CHECKPOINT_KEY]["logs"], json!([6]));
        assert_eq!(IdTypes::from_checkpoint(&ckpt), t);
        assert_eq!(
            crate::source::mongo::cdc::OrdinalClock::from_checkpoint(&ckpt),
            crate::source::mongo::cdc::OrdinalClock::from_checkpoint(&json!({"ct": 7, "ord": 3}))
        );
    }

    #[test]
    fn a_malformed_key_reads_as_no_types() {
        for bad in [
            json!({"idt": 5}),
            json!({"idt": {"a": "x"}}),
            json!({"idt": []}),
        ] {
            let t = IdTypes::from_checkpoint(&bad);
            assert!(t.brackets("a").is_empty(), "{bad}");
        }
    }

    #[test]
    fn a_second_type_is_refused_and_not_recorded() {
        let mut t = IdTypes::default();
        t.admit("c", &Bson::Int32(1)).unwrap();
        let err = t.admit("c", &Bson::String("1".into())).unwrap_err();
        assert_eq!(t.brackets("c"), vec![0]);
        assert_eq!(
            crate::error::code_of(&err).map(|c| c.id),
            Some(crate::error::codes::SOURCE_ID_TYPES_MIXED.id)
        );
        assert_eq!(
            err.to_string(),
            "mongodb collection `c` holds `_id` values of more than one BSON type (number (int, \
             long, double or decimal), string), and its `load:` block keys the current-state \
             view on the flat `_id` column, which renders different types to the same text \
             (int 1 and string \"1\" both become \"1\"): the view would merge distinct \
             documents and a delete of one would remove the other. This run wrote no change of \
             a second type. Load the collection with `mode: full`, or make its `_id` values one \
             BSON type, drop the change-log table, delete the CDC checkpoint and re-run for a \
             fresh snapshot (loading mixed-type `_id` under a keyed load is tracked in #926)"
        );
    }

    fn span(lo: Bson, hi: Bson) -> Result<Option<(Document, Document)>> {
        Ok(Some((doc! { "_id": lo }, doc! { "_id": hi })))
    }

    #[test]
    fn a_keyed_collection_whose_ends_differ_in_type_is_refused() {
        let keyed = vec!["c".to_string()];
        let err = guard_keyed(IdTypes::default(), &keyed, |_| {
            span(Bson::Int32(1), Bson::String("1".into()))
        })
        .unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_ID_TYPES_MIXED", 5);
    }

    #[test]
    fn a_uniform_empty_or_unreadable_collection_is_admitted_and_recorded() {
        let keyed = vec!["c".to_string()];
        let uniform = guard_keyed(IdTypes::default(), &keyed, |_| {
            span(Bson::Int32(1), Bson::Int64(9))
        })
        .unwrap();
        assert_eq!(uniform.brackets("c"), vec![0]);
        let empty = guard_keyed(IdTypes::default(), &keyed, |_| Ok(None)).unwrap();
        assert!(empty.brackets("c").is_empty());
        let unreadable =
            guard_keyed(IdTypes::default(), &keyed, |_| Err(anyhow::anyhow!("down"))).unwrap();
        assert!(unreadable.brackets("c").is_empty());
    }

    #[test]
    fn a_type_known_from_the_checkpoint_joins_the_server_ends() {
        let mut known = IdTypes::default();
        known.admit("c", &Bson::String("a".into())).unwrap();
        let keyed = vec!["c".to_string()];
        let err = guard_keyed(known, &keyed, |_| span(Bson::Int32(1), Bson::Int32(2))).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_ID_TYPES_MIXED", 5);
    }

    #[test]
    fn a_change_is_admitted_only_for_a_keyed_collection() {
        let keyed = vec!["c".to_string()];
        let mut t = IdTypes::default();
        let key = |id: Bson| doc! { "_id": id };
        admit_change(&mut t, &keyed, Some("c"), Some(&key(Bson::Int32(1)))).unwrap();
        assert_eq!(t.brackets("c"), vec![0]);
        let err = admit_change(
            &mut t,
            &keyed,
            Some("c"),
            Some(&key(Bson::String("1".into()))),
        )
        .unwrap_err();
        crate::error::assert_coded(&err, "RIVET_SOURCE_ID_TYPES_MIXED", 5);
        admit_change(
            &mut t,
            &keyed,
            Some("other"),
            Some(&key(Bson::String("x".into()))),
        )
        .unwrap();
        assert!(t.brackets("other").is_empty());
        admit_change(&mut t, &keyed, None, Some(&key(Bson::Null))).unwrap();
        admit_change(&mut t, &keyed, Some("c"), None).unwrap();
        admit_change(&mut t, &keyed, Some("c"), Some(&doc! { "x": 1 })).unwrap();
        assert_eq!(t.brackets("c"), vec![0]);
    }

    #[test]
    fn every_bracket_is_named_for_the_type_it_holds() {
        use mongodb::bson::spec::BinarySubtype;
        use mongodb::bson::{Binary, DateTime, Regex, Timestamp};
        let cases: Vec<(Bson, &str)> = vec![
            (Bson::Int32(1), "number (int, long, double or decimal)"),
            (Bson::Null, "null"),
            (Bson::String("s".into()), "string"),
            (Bson::Document(doc! {"k": 1}), "document"),
            (Bson::Array(vec![Bson::Int32(1)]), "array"),
            (
                Bson::Binary(Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: vec![1],
                }),
                "binData",
            ),
            (Bson::ObjectId(ObjectId::new()), "objectId"),
            (Bson::Boolean(true), "bool"),
            (Bson::DateTime(DateTime::from_millis(0)), "date"),
            (
                Bson::Timestamp(Timestamp {
                    time: 1,
                    increment: 1,
                }),
                "timestamp",
            ),
            (
                Bson::RegularExpression(Regex {
                    pattern: "a".into(),
                    options: String::new(),
                }),
                "regex",
            ),
            (Bson::MinKey, "other"),
        ];
        for (id, name) in cases {
            assert_eq!(bracket_name(bracket_of(&id)), name, "{id:?}");
        }
    }

    #[test]
    fn numeric_types_are_one_bracket() {
        let mut t = IdTypes::default();
        for id in [
            Bson::Int32(1),
            Bson::Int64(2),
            Bson::Double(3.5),
            Bson::Decimal128("4".parse().unwrap()),
        ] {
            t.admit("c", &id).unwrap();
        }
        assert_eq!(t.brackets("c"), vec![0]);
    }

    #[test]
    fn collections_are_kept_apart() {
        let mut t = IdTypes::default();
        t.admit("a", &Bson::Int32(1)).unwrap();
        t.admit("b", &Bson::String("1".into())).unwrap();
        assert_eq!(t.brackets("a").len(), 1);
        assert_eq!(t.brackets("b").len(), 1);
        assert_ne!(t.brackets("a"), t.brackets("b"));
    }
}
