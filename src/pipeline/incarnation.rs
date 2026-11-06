//! **Layer: Execution** — binds the progress of a MongoDB export to the collection incarnation it reads.

use crate::error::Result;
use crate::plan::ResolvedRunPlan;

/// The collection a MongoDB plan that continues stored progress reads; `None` when the plan has nothing to bind.
fn collection_to_bind(plan: &ResolvedRunPlan) -> Option<&str> {
    let holds_progress = plan.strategy.continues_stored_cursor() || plan.strategy.is_resumable();
    if plan.source.source_type != crate::config::SourceType::Mongo || !holds_progress {
        return None;
    }
    crate::sql::strip_select_star_from(&plan.base_query)
}

/// The plan carrying `incarnation`; `None` when the database lists none.
fn with_incarnation(
    plan: &ResolvedRunPlan,
    incarnation: Option<String>,
) -> Option<ResolvedRunPlan> {
    incarnation.map(|incarnation| ResolvedRunPlan {
        collection_incarnation: Some(incarnation),
        ..plan.clone()
    })
}

/// The plan with the collection UUID the database lists now, for a MongoDB plan that continues stored progress; `None` when the plan has nothing to bind.
pub(crate) fn bound_to_collection(plan: &ResolvedRunPlan) -> Result<Option<ResolvedRunPlan>> {
    let Some(collection) = collection_to_bind(plan) else {
        return Ok(None);
    };
    let url = plan.source.resolve_url()?;
    let session = crate::source::mongo::MongoSession::connect(&url, plan.source.tls.as_ref())?;
    Ok(with_incarnation(
        plan,
        session.collection_incarnation(collection)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::test_support::test_plan;
    use crate::plan::{ExtractionStrategy, KeysetPlan};

    /// A SQL plan and a plan with no stored progress are returned unbound, with no connection made.
    #[test]
    fn a_plan_with_nothing_to_bind_is_left_as_planned() {
        let sql = test_plan();
        assert!(bound_to_collection(&sql).unwrap().is_none());
        let mut mongo = test_plan();
        mongo.source.source_type = crate::config::SourceType::Mongo;
        mongo.base_query = "SELECT * FROM t".into();
        assert!(bound_to_collection(&mongo).unwrap().is_none());
    }

    /// A plan without an incarnation keys its progress exactly as before the incarnation existed.
    #[test]
    fn a_plan_without_an_incarnation_keeps_its_progress_key() {
        let plan = test_plan();
        let key = plan.progress_key();
        assert_eq!(key.population, crate::sql::row_set(&plan.base_query));
        let bound = ResolvedRunPlan {
            collection_incarnation: Some("ab".into()),
            ..plan
        };
        assert_ne!(bound.progress_key().population, key.population);
        assert_eq!(bound.progress_key().export_name, key.export_name);
    }

    fn mongo_plan(strategy: ExtractionStrategy) -> ResolvedRunPlan {
        let mut plan = test_plan();
        plan.source.source_type = crate::config::SourceType::Mongo;
        plan.source.url = Some("mongodb://127.0.0.1:1/db?serverSelectionTimeoutMS=300".into());
        plan.base_query = "SELECT * FROM t".into();
        plan.strategy = strategy;
        plan
    }

    fn keyset(checkpoint: bool, incremental: bool) -> ExtractionStrategy {
        ExtractionStrategy::Keyset(KeysetPlan {
            key_column: "_id".into(),
            chunk_size: 100,
            checkpoint,
            incremental,
            parallel: 0,
            key_rest: Vec::new(),
        })
    }

    /// A MongoDB plan binds its collection when it continues a cursor or resumes a checkpoint, and either alone is enough.
    #[test]
    fn a_mongo_plan_with_progress_names_its_collection() {
        assert_eq!(
            collection_to_bind(&mongo_plan(keyset(false, true))),
            Some("t")
        );
        assert_eq!(
            collection_to_bind(&mongo_plan(keyset(true, false))),
            Some("t")
        );
        assert_eq!(
            collection_to_bind(&mongo_plan(keyset(true, true))),
            Some("t")
        );
        assert_eq!(collection_to_bind(&mongo_plan(keyset(false, false))), None);
        assert_eq!(
            collection_to_bind(&mongo_plan(ExtractionStrategy::Snapshot)),
            None
        );
    }

    /// A plan with progress on a SQL source, or a Mongo plan whose query names no bare collection, binds nothing.
    #[test]
    fn a_plan_that_is_not_a_mongo_collection_with_progress_binds_nothing() {
        let mut sql = mongo_plan(keyset(true, true));
        sql.source.source_type = crate::config::SourceType::Postgres;
        assert_eq!(collection_to_bind(&sql), None);
        assert!(bound_to_collection(&sql).unwrap().is_none());
        let mut filtered = mongo_plan(keyset(true, true));
        filtered.base_query = "SELECT * FROM t WHERE x = 1".into();
        assert_eq!(collection_to_bind(&filtered), None);
        assert!(bound_to_collection(&filtered).unwrap().is_none());
    }

    /// A Mongo plan with progress goes to the database, so an unreachable one is an error and never an unbound plan.
    #[test]
    fn a_mongo_plan_with_progress_asks_the_database() {
        assert!(bound_to_collection(&mongo_plan(keyset(true, false))).is_err());
        assert!(bound_to_collection(&mongo_plan(keyset(false, true))).is_err());
    }

    /// The incarnation the database lists is the one the plan carries, and none listed leaves no plan.
    #[test]
    fn the_listed_incarnation_is_carried_by_the_plan() {
        let plan = mongo_plan(keyset(true, false));
        let bound = with_incarnation(&plan, Some("ab01".into())).unwrap();
        assert_eq!(bound.collection_incarnation.as_deref(), Some("ab01"));
        assert_eq!(bound.export_name, plan.export_name);
        assert!(with_incarnation(&plan, None).is_none());
    }
}
