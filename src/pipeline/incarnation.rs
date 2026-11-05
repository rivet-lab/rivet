//! **Layer: Execution** — binds the progress of a MongoDB export to the collection incarnation it reads.

use crate::error::Result;
use crate::plan::ResolvedRunPlan;

/// The plan with the collection UUID the database lists now, for a MongoDB plan that continues stored progress; `None` when the plan has nothing to bind.
pub(crate) fn bound_to_collection(plan: &ResolvedRunPlan) -> Result<Option<ResolvedRunPlan>> {
    let holds_progress = plan.strategy.continues_stored_cursor() || plan.strategy.is_resumable();
    if plan.source.source_type != crate::config::SourceType::Mongo || !holds_progress {
        return Ok(None);
    }
    let Some(collection) = crate::sql::strip_select_star_from(&plan.base_query) else {
        return Ok(None);
    };
    let url = plan.source.resolve_url()?;
    let session = crate::source::mongo::MongoSession::connect(&url, plan.source.tls.as_ref())?;
    Ok(session
        .collection_incarnation(collection)?
        .map(|incarnation| ResolvedRunPlan {
            collection_incarnation: Some(incarnation),
            ..plan.clone()
        }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::test_support::test_plan;

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
}
