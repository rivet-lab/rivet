//! **Layer: Coordinator** — the single home for the runner-level post-finalize state writes.

use super::summary::RunSummary;
use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::state::StateStore;

/// How the run's progression boundary is recorded.
pub(crate) enum Progression {
    Incremental { last_value: String },
    Chunked,
}

/// Build a post-finalize state-write batch and commit it in the ADR-0001 I3 → ADR-0008 PG2 canonical order.
pub(crate) struct RunStore<'a> {
    state: &'a StateStore,
    plan: &'a ResolvedRunPlan,
    summary: &'a RunSummary,
    cursor: Option<String>,
    progression: Option<Progression>,
}

impl<'a> RunStore<'a> {
    /// Start a finalize batch.
    pub fn finalize(
        state: &'a StateStore,
        plan: &'a ResolvedRunPlan,
        summary: &'a RunSummary,
    ) -> Self {
        Self {
            state,
            plan,
            summary,
            cursor: None,
            progression: None,
        }
    }

    /// Attach a cursor advance.
    pub fn with_cursor(mut self, value: String) -> Self {
        self.cursor = Some(value);
        self
    }

    /// Attach the progression boundary update.
    pub fn with_progression(mut self, p: Progression) -> Self {
        self.progression = Some(p);
        self
    }

    /// Apply the batched writes in the canonical order.
    pub fn commit(self) -> Result<()> {
        if let Some(cursor_val) = self.cursor.as_deref() {
            self.state
                .update_with_column(&self.plan.progress_key(), cursor_val)?;

            crate::test_hook::maybe_panic_at("after_cursor_commit");

            log::info!(
                "export '{}': cursor updated to '{}'",
                self.plan.export_name,
                cursor_val
            );
        }

        if let Some(p) = self.progression {
            match p {
                Progression::Incremental { last_value } => {
                    if let Err(e) = self.state.record_committed_incremental(
                        &self.plan.export_name,
                        &last_value,
                        &self.summary.run_id,
                    ) {
                        log::warn!(
                            "export '{}': committed boundary update failed: {:#}",
                            self.plan.export_name,
                            e
                        );
                    }
                }
                Progression::Chunked => {
                    super::chunked::record_chunked_commit(
                        self.state,
                        &self.plan.export_name,
                        &self.summary.run_id,
                    );
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::test_support::test_plan;
    use crate::plan::{ExtractionStrategy, ResolvedRunPlan};

    fn test_summary(plan: &ResolvedRunPlan, run_id: &str) -> RunSummary {
        let mut s = RunSummary::stub_for_testing(run_id, plan.export_name.clone());
        s.batch_size = 10_000;
        s.mode = "snapshot".into();
        s.compression = "none".into();
        s
    }

    #[test]
    fn finalize_with_no_writes_is_a_noop() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = test_plan();
        let summary = test_summary(&plan, "run-noop");

        RunStore::finalize(&state, &plan, &summary)
            .commit()
            .unwrap();
    }

    /// The cursor-carrying shape: an incremental plan on `updated_at`.
    fn incremental_plan(export_name: &str) -> ResolvedRunPlan {
        let mut plan = ResolvedRunPlan {
            export_name: export_name.into(),
            ..test_plan()
        };
        plan.strategy = ExtractionStrategy::Incremental(crate::plan::IncrementalCursorPlan {
            primary_column: "updated_at".into(),
            fallback_column: None,
            mode: crate::config::IncrementalCursorMode::SingleColumn,
            settle: None,
        });
        plan
    }

    #[test]
    fn the_progress_key_carries_every_identity_part_of_the_plan() {
        let mut plan = incremental_plan("orders");
        plan.base_query = "SELECT id, updated_at FROM \"shop\".\"orders_src\" WHERE id > 0".into();
        let key = plan.progress_key();
        assert_eq!(key.export_name, "orders");
        assert_eq!(key.source, "postgres://127.0.0.1:9999/nonexistent");
        assert_eq!(key.stream, "shop.orders_src");
        assert_eq!(key.column.as_deref(), Some("updated_at"));
        assert!(key.continues_high_water);

        plan.strategy = ExtractionStrategy::Keyset(crate::plan::KeysetPlan {
            key_column: "id".into(),
            chunk_size: 1000,
            checkpoint: true,
            incremental: false,
            parallel: 1,
            key_rest: Vec::new(),
        });
        let key = plan.progress_key();
        assert_eq!(key.column.as_deref(), Some("id"));
        assert!(!key.continues_high_water, "a crash-recovery keyset");

        plan.strategy = ExtractionStrategy::Snapshot;
        plan.base_query = "SELECT 1".into();
        let key = plan.progress_key();
        assert_eq!((key.stream.as_str(), key.column), ("", None));
    }

    #[test]
    fn finalize_with_cursor_only_advances_state_cursor() {
        let state = StateStore::open_in_memory().unwrap();
        let mut plan = incremental_plan("orders");
        plan.base_query = "SELECT id, updated_at FROM \"shop\".\"orders_src\" WHERE id > 0".into();
        let summary = test_summary(&plan, "run-1");

        RunStore::finalize(&state, &plan, &summary)
            .with_cursor("2026-05-30T12:00:00Z".into())
            .commit()
            .unwrap();

        let cursor = state.get("orders", &plan.source.state_key()).unwrap();
        assert_eq!(
            cursor.last_cursor_value.as_deref(),
            Some("2026-05-30T12:00:00Z"),
            "I3: the cursor value is persisted"
        );
        assert_eq!(
            cursor.cursor_column.as_deref(),
            Some("updated_at"),
            "MT4: every cursor write records its identity"
        );
        let elsewhere = crate::state::ProgressKey {
            stream: "shop.customers".into(),
            ..plan.progress_key()
        };
        let said = state
            .claim(elsewhere)
            .err()
            .expect("MT8: the cursor write recorded the relation the plan reads")
            .to_string();
        assert!(said.contains("reading `shop.orders_src`"), "{said}");
        assert!(state.claim(plan.progress_key()).is_ok());
    }

    #[test]
    fn a_cursor_from_a_strategy_without_an_identity_is_refused() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = test_plan();
        let summary = test_summary(&plan, "run-1");
        let err = RunStore::finalize(&state, &plan, &summary)
            .with_cursor("99".into())
            .commit()
            .unwrap_err()
            .to_string();
        assert!(err.contains("no cursor identity"), "{err}");
        assert_eq!(
            state
                .get("orders", &plan.source.state_key())
                .unwrap()
                .last_cursor_value,
            None
        );
    }

    #[test]
    fn finalize_with_progression_incremental_writes_progression_row() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = test_plan();
        let summary = test_summary(&plan, "run-2");

        RunStore::finalize(&state, &plan, &summary)
            .with_progression(Progression::Incremental {
                last_value: "42".into(),
            })
            .commit()
            .unwrap();

        let prog = state.get_progression("orders").unwrap();
        let boundary = prog.committed.expect("committed boundary written");
        assert_eq!(boundary.strategy, "incremental");
        assert_eq!(boundary.cursor.as_deref(), Some("42"));
        assert_eq!(boundary.run_id.as_deref(), Some("run-2"));
    }

    #[test]
    fn finalize_with_progression_chunked_picks_highest_completed_chunk_index() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = test_plan();
        let summary = test_summary(&plan, "run-3");

        state.create_chunk_run("run-3", "orders", "h", 3).unwrap();
        state
            .insert_chunk_tasks("run-3", &[(1, 10), (11, 20), (21, 30)])
            .unwrap();
        state
            .complete_chunk_task("run-3", 0, 10, Some("c0.parquet"))
            .unwrap();
        state
            .complete_chunk_task("run-3", 2, 30, Some("c2.parquet"))
            .unwrap();

        RunStore::finalize(&state, &plan, &summary)
            .with_progression(Progression::Chunked)
            .commit()
            .unwrap();

        let prog = state.get_progression("orders").unwrap();
        let boundary = prog.committed.expect("committed boundary written");
        assert_eq!(boundary.strategy, "chunked");
        assert_eq!(
            boundary.chunk_index,
            Some(2),
            "must pick the highest *completed* chunk_index, not the highest claimed"
        );
        assert_eq!(boundary.run_id.as_deref(), Some("run-3"));
    }

    #[test]
    fn finalize_with_cursor_and_progression_writes_both() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = incremental_plan("orders");
        let summary = test_summary(&plan, "run-4");

        RunStore::finalize(&state, &plan, &summary)
            .with_cursor("99".into())
            .with_progression(Progression::Incremental {
                last_value: "99".into(),
            })
            .commit()
            .unwrap();

        assert_eq!(
            state
                .get("orders", &plan.source.state_key())
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("99")
        );
        let prog = state.get_progression("orders").unwrap();
        let boundary = prog.committed.expect("committed boundary written");
        assert_eq!(boundary.cursor.as_deref(), Some("99"));
    }

    #[test]
    fn finalize_chunked_with_no_completed_tasks_is_a_silent_noop() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = test_plan();
        let summary = test_summary(&plan, "run-empty");

        state
            .create_chunk_run("run-empty", "orders", "h", 3)
            .unwrap();
        state
            .insert_chunk_tasks("run-empty", &[(1, 10), (11, 20)])
            .unwrap();

        RunStore::finalize(&state, &plan, &summary)
            .with_progression(Progression::Chunked)
            .commit()
            .unwrap();

        let prog = state.get_progression("orders").unwrap();
        assert!(
            prog.committed.is_none(),
            "empty chunk_task → no committed boundary"
        );
    }
}
