//! The byte-shaping identity of a run (`run_shape`): what a run was opened with, checked before it is continued.

use std::collections::BTreeMap;

use crate::error::Result;

use super::StateStore;

/// What shapes the bytes a run writes, one digest per field by the config key that sets it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunShape(BTreeMap<String, String>);

impl RunShape {
    /// A shape of the given digests; an empty shape names nothing and is never compared.
    pub fn new(fields: BTreeMap<String, String>) -> Self {
        Self(fields)
    }

    /// Whether the shape names no field.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The fields recorded in both shapes whose digests differ; a field only one side records is not compared.
    pub fn changed_from(&self, stored: &RunShape) -> Vec<&str> {
        self.0
            .iter()
            .filter(|(name, digest)| stored.0.get(*name).is_some_and(|was| was != *digest))
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// The names of the recorded fields.
    #[cfg(test)]
    pub(crate) fn names(&self) -> std::collections::BTreeSet<String> {
        self.0.keys().cloned().collect()
    }

    fn to_json(&self) -> String {
        serde_json::to_string(&self.0).expect("a map of strings serializes")
    }

    fn from_json(json: &str) -> Option<Self> {
        serde_json::from_str(json).ok().map(Self)
    }
}

impl StateStore {
    /// Record `shape` for `run_id` unless the run already has one: a resume keeps the shape the run was opened with.
    pub(super) fn record_run_shape(&self, run_id: &str, shape: &RunShape) -> Result<()> {
        if shape.is_empty() {
            return Ok(());
        }
        self.execute(
            "INSERT INTO run_shape (run_id, shape_json, recorded_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT(run_id) DO NOTHING",
            &[
                run_id.into(),
                shape.to_json().into(),
                chrono::Utc::now().to_rfc3339().into(),
            ],
        )?;
        Ok(())
    }

    /// The shape `run_id` was opened with; `None` for a run opened before shapes were recorded.
    pub(super) fn stored_run_shape(&self, run_id: &str) -> Result<Option<RunShape>> {
        Ok(self
            .query_opt(
                "SELECT shape_json FROM run_shape WHERE run_id = ?1",
                &[run_id.into()],
                |r| r.text(0),
            )?
            .and_then(|json| RunShape::from_json(&json)))
    }

    /// Forget the shape of `run_id`, as its run is abandoned.
    pub(super) fn forget_run_shape(&self, run_id: &str) -> Result<()> {
        self.execute("DELETE FROM run_shape WHERE run_id = ?1", &[run_id.into()])?;
        Ok(())
    }

    /// The fields of `now` that differ from what `run_id` was opened with; a run with no recorded shape adopts `now` once, with a warning.
    pub(crate) fn run_shape_changes(
        &self,
        export_name: &str,
        run_id: &str,
        now: &RunShape,
    ) -> Result<Vec<String>> {
        if now.is_empty() {
            return Ok(Vec::new());
        }
        match self.stored_run_shape(run_id)? {
            Some(was) => Ok(now
                .changed_from(&was)
                .into_iter()
                .map(String::from)
                .collect()),
            None => {
                log::warn!(
                    "export '{export_name}': run {run_id} predates shape tracking — its settings \
                     are recorded as the ones in the config now, and a later edit of them is refused"
                );
                self.record_run_shape(run_id, now)?;
                Ok(Vec::new())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(fields: &[(&str, &str)]) -> RunShape {
        RunShape::new(
            fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn a_field_only_one_side_records_is_not_compared() {
        let stored = shape(&[("compression", "a"), ("format", "p")]);
        let now = shape(&[("compression", "b"), ("columns", "x")]);
        assert_eq!(now.changed_from(&stored), vec!["compression"]);
        assert!(stored.changed_from(&stored).is_empty());
    }

    #[test]
    fn an_empty_shape_changes_nothing() {
        let stored = shape(&[("compression", "a")]);
        assert_eq!(
            RunShape::default().changed_from(&stored),
            Vec::<&str>::new()
        );
        assert_eq!(
            stored.changed_from(&RunShape::default()),
            Vec::<&str>::new()
        );
        assert_eq!(
            stored.changed_from(&shape(&[("compression", "b")])),
            vec!["compression"]
        );
    }

    #[test]
    fn a_recorded_shape_is_kept_by_the_next_record_and_compared_by_the_next_check() {
        let s = StateStore::open_in_memory().expect("in-memory state");
        let (a, b) = (
            shape(&[("compression", "a")]),
            shape(&[("compression", "b")]),
        );
        assert!(s.run_shape_changes("e", "r1", &a).unwrap().is_empty());
        s.record_run_shape("r1", &b).unwrap();
        assert_eq!(s.stored_run_shape("r1").unwrap(), Some(a.clone()));
        assert_eq!(
            s.run_shape_changes("e", "r1", &b).unwrap(),
            vec!["compression"]
        );
        assert!(s.run_shape_changes("e", "r1", &a).unwrap().is_empty());
        s.forget_run_shape("r1").unwrap();
        assert_eq!(s.stored_run_shape("r1").unwrap(), None);
    }

    #[test]
    fn a_run_with_no_recorded_shape_adopts_the_first_one_it_meets() {
        let s = StateStore::open_in_memory().expect("in-memory state");
        let (a, b) = (shape(&[("columns", "a")]), shape(&[("columns", "b")]));
        assert!(s.run_shape_changes("e", "legacy", &a).unwrap().is_empty());
        assert_eq!(
            s.run_shape_changes("e", "legacy", &b).unwrap(),
            vec!["columns"]
        );
    }
}
