use crate::error::Result;

use std::collections::HashSet;

use super::StateStore;

/// The `run_status` ledger — a best-effort ADVISORY record of each export run's lifecycle.
pub struct RunStatusRow {
    pub run_id: String,
    pub export_name: String,
    pub prefix: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: String,
}

/// What `finish_run_checked` did — the CLI's three honest answers.
#[derive(Debug, PartialEq, Eq)]
pub enum FinishOutcome {
    /// The row was `running` and is now `interrupted`.
    Stamped,
    /// Nothing to do — the row already carries this terminal status.
    AlreadyTerminal(String),
    /// No such run id: refuse loudly, never "success" on a typo.
    NotFound,
}

impl StateStore {
    /// Record an export run as `running` at its START.
    pub fn begin_run(
        &self,
        run_id: &str,
        export_name: &str,
        prefix: &str,
        started_at: &str,
    ) -> Result<()> {
        let (stamp, params) = match &self.state_ref {
            super::StateRef::Postgres(_) => (
                "to_char(clock_timestamp() AT TIME ZONE 'UTC', \
                 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"')",
                vec![run_id.into(), export_name.into(), prefix.into()],
            ),
            super::StateRef::Sqlite(_) => (
                "?4",
                vec![
                    run_id.into(),
                    export_name.into(),
                    prefix.into(),
                    started_at.into(),
                ],
            ),
        };
        self.execute(
            &format!(
                "INSERT INTO run_status
                   (run_id, export_name, prefix, status, started_at, finished_at)
                 VALUES (?1, ?2, ?3, 'running', {stamp}, NULL)
                 ON CONFLICT(run_id) DO UPDATE SET
                     export_name = excluded.export_name,
                     prefix      = excluded.prefix,
                     status      = 'running',
                     started_at  = excluded.started_at,
                     finished_at = NULL"
            ),
            &params,
        )?;
        Ok(())
    }

    /// Transition a run to a terminal status (`success` | `failed` | `interrupted`) at finalize.
    pub fn finish_run(&self, run_id: &str, status: &str, finished_at: &str) -> Result<()> {
        self.execute(
            "UPDATE run_status SET status = ?2, finished_at = ?3 WHERE run_id = ?1",
            &[run_id.into(), status.into(), finished_at.into()],
        )?;
        Ok(())
    }

    /// Is a LIVE extract writing `prefix`?
    pub fn has_active_run_on_prefix(&self, prefix: &str) -> Result<bool> {
        let sql = format!(
            "SELECT 1 FROM run_status r WHERE {} LIMIT 1",
            live_on_prefix("r", "?1")
        );
        Ok(self.query_opt(&sql, &[prefix.into()], |_| ())?.is_some())
    }

    /// The status of ONE run-status row, or `None` when no row exists.
    pub fn run_status_of(&self, run_id: &str) -> Result<Option<String>> {
        let sql = "SELECT status FROM run_status WHERE run_id = ?1";
        Ok(self
            .query(sql, &[run_id.into()], |r| r.text(0))?
            .into_iter()
            .next())
    }

    /// Whether a run of `export` finished `success` at the destination `uri`.
    pub fn has_recorded_success(&self, export: &str, uri: &str) -> Result<bool> {
        let recorded = self.query(
            "SELECT DISTINCT prefix FROM run_status WHERE export_name = ?1 AND status = 'success'",
            &[export.into()],
            |r| r.text(0),
        )?;
        let want = uri.trim_end_matches('/');
        Ok(recorded.iter().any(|p| {
            let p = p.trim_end_matches('/');
            p == want || crate::config::destination::anchors(p, want)
        }))
    }

    /// The destination prefix one run recorded at its start, or `None` when the ledger never saw it.
    pub fn run_prefix_of(&self, run_id: &str) -> Result<Option<String>> {
        let sql = "SELECT prefix FROM run_status WHERE run_id = ?1";
        Ok(self
            .query(sql, &[run_id.into()], |r| r.text(0))?
            .into_iter()
            .next())
    }

    /// The newest `running` row of `export_name`: the run a held run lease belongs to.
    pub fn newest_running_run(&self, export_name: &str, source: &str) -> Result<Option<String>> {
        self.query_opt(
            "SELECT run_id FROM run_status WHERE export_name = ?1 AND status = 'running' \
             AND (source = ?2 OR source IS NULL) ORDER BY started_at DESC LIMIT 1",
            &[export_name.into(), source.into()],
            |r| r.text(0),
        )
    }

    /// The run-status rows, newest first — `rivet state runs`.
    pub fn recent_run_status(&self, last: usize, running_only: bool) -> Result<Vec<RunStatusRow>> {
        const COLS: &str =
            "run_id, export_name, prefix, status, started_at, COALESCE(finished_at, '')";
        let sql = if running_only {
            format!(
                "SELECT {COLS} FROM run_status WHERE status = 'running' \
                 ORDER BY started_at DESC LIMIT ?1"
            )
        } else {
            format!("SELECT {COLS} FROM run_status ORDER BY started_at DESC LIMIT ?1")
        };
        self.query(&sql, &[(last as i64).into()], |r| RunStatusRow {
            run_id: r.text(0),
            export_name: r.text(1),
            prefix: r.text(2),
            status: r.text(3),
            started_at: r.text(4),
            finished_at: r.text(5),
        })
    }

    /// [`Self::begin_run`] by a run that holds the export's run lease on `source`.
    pub fn begin_run_under_lease(
        &self,
        run_id: &str,
        export_name: &str,
        source: &str,
        prefix: &str,
        started_at: &str,
    ) -> Result<()> {
        self.transaction(|| {
            seeded!(
                "dead-runs-stay-running",
                0,
                self.interrupt_dead_runs(export_name, source, started_at)?
            );
            self.begin_run(run_id, export_name, prefix, started_at)?;
            self.record_run_source(run_id, source)
        })
    }

    /// Record that `run_id` applied the sealed plan `plan_id` to exit 0; returns whether a run row took it.
    pub fn record_applied_plan(&self, run_id: &str, plan_id: &str) -> Result<bool> {
        let n = self.execute(
            "UPDATE run_status SET plan_id = ?2 WHERE run_id = ?1",
            &[run_id.into(), plan_id.into()],
        )?;
        Ok(n > 0)
    }

    /// The newest finished run of `export_name` that applied the sealed plan `plan_id` to exit 0.
    pub fn run_that_applied_plan(
        &self,
        export_name: &str,
        plan_id: &str,
    ) -> Result<Option<String>> {
        self.query_opt(
            "SELECT run_id FROM run_status WHERE export_name = ?1 AND plan_id = ?2 \
             AND status IN ('success', 'skipped') ORDER BY started_at DESC LIMIT 1",
            &[export_name.into(), plan_id.into()],
            |r| r.text(0),
        )
    }

    /// Whether the newest successful run of `export_name` from `source` applied the sealed plan `plan_id`.
    pub fn cursor_last_moved_by_plan(
        &self,
        export_name: &str,
        source: &str,
        plan_id: &str,
    ) -> Result<bool> {
        let newest = self.query_opt(
            "SELECT COALESCE(plan_id, '') FROM run_status WHERE export_name = ?1 \
             AND (source = ?2 OR source IS NULL) AND status = 'success' \
             ORDER BY started_at DESC LIMIT 1",
            &[export_name.into(), source.into()],
            |r| r.text(0),
        )?;
        Ok(newest.as_deref() == Some(plan_id))
    }

    /// Record the source `run_id` reads, which scopes the run lease that closes its row if it dies.
    pub fn record_run_source(&self, run_id: &str, source: &str) -> Result<()> {
        self.execute(
            "UPDATE run_status SET source = ?2 WHERE run_id = ?1",
            &[run_id.into(), source.into()],
        )?;
        Ok(())
    }

    /// Stamp `interrupted` on every `running` row of `export_name` reading `source` or recorded before sources were (run status, and the metrics of those runs or of runs with no status row).
    pub fn interrupt_dead_runs(
        &self,
        export_name: &str,
        source: &str,
        finished_at: &str,
    ) -> Result<usize> {
        self.transaction(|| {
            self.execute(
                "UPDATE export_metrics SET status = 'interrupted' \
                 WHERE export_name = ?1 AND status = 'running' AND ( \
                     run_id IN (SELECT run_id FROM run_status WHERE export_name = ?1 \
                                AND status = 'running' AND (source = ?2 OR source IS NULL)) \
                     OR run_id IS NULL \
                     OR run_id NOT IN (SELECT run_id FROM run_status))",
                &[export_name.into(), source.into()],
            )?;
            self.execute(
                "UPDATE run_status SET status = 'interrupted', finished_at = ?3 \
                 WHERE export_name = ?1 AND status = 'running' AND (source = ?2 OR source IS NULL)",
                &[export_name.into(), source.into(), finished_at.into()],
            )
        })
    }

    /// The `rivet state finish-run` escape hatch.
    pub fn finish_run_checked(&self, run_id: &str, finished_at: &str) -> Result<FinishOutcome> {
        let n = self.execute(
            "UPDATE run_status SET status = 'interrupted', finished_at = ?2
             WHERE run_id = ?1 AND status = 'running'",
            &[run_id.into(), finished_at.into()],
        )?;
        if n > 0 {
            return Ok(FinishOutcome::Stamped);
        }
        let sql = "SELECT status FROM run_status WHERE run_id = ?1";
        let status = self.query(sql, &[run_id.into()], |r| r.text(0))?;
        match status.into_iter().next() {
            Some(status) => Ok(FinishOutcome::AlreadyTerminal(status)),
            None => Ok(FinishOutcome::NotFound),
        }
    }

    /// Terminal-stamp the `running` rows of split units a `--split --resume` RECONSTRUCTION has ceased to exist.
    pub fn interrupt_ceased_split_units(
        &self,
        giant: &str,
        prefix: &str,
        kept: usize,
        finished_at: &str,
    ) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT r.run_id, r.export_name FROM run_status r
             WHERE r.export_name LIKE ?1 || '#%' AND r.status = 'running' AND {}",
            prefix_overlaps("r", "?2")
        );
        let rows = self.query(&sql, &[giant.into(), prefix.into()], |r| {
            (r.text(0), r.text(1))
        })?;
        let mut stamped = Vec::new();
        for (run_id, name) in rows {
            let ceased = name
                .strip_prefix(giant)
                .and_then(|s| s.strip_prefix('#'))
                .and_then(|s| s.parse::<usize>().ok())
                .is_some_and(|ord| ord >= kept);
            if !ceased {
                continue;
            }
            self.finish_run(&run_id, "interrupted", finished_at)?;
            stamped.push(name);
        }
        Ok(stamped)
    }

    /// The run_ids of the runs [`has_active_run_on_prefix`] answers `true` for.
    pub fn active_run_ids_on_prefix(&self, prefix: &str) -> Result<HashSet<String>> {
        let sql = format!(
            "SELECT r.run_id FROM run_status r WHERE {}",
            live_on_prefix("r", "?1")
        );
        Ok(self
            .query(&sql, &[prefix.into()], |r| r.text(0))?
            .into_iter()
            .collect())
    }
}

/// The prefix-containment predicate on LITERAL path text, either direction.
fn prefix_overlaps(alias: &str, param: &str) -> String {
    if seeded!("state-prefix-match-reads-text-as-pattern", true, false) {
        return format!(
            "(rtrim({alias}.prefix, '/') = rtrim({param}, '/') \
             OR {alias}.prefix LIKE rtrim({param}, '/') || '/%' \
             OR rtrim({param}, '/') LIKE rtrim({alias}.prefix, '/') || '/%')"
        );
    }
    format!(
        "(rtrim({alias}.prefix, '/') = rtrim({param}, '/') \
         OR substr({alias}.prefix, 1, length(rtrim({param}, '/')) + 1) = rtrim({param}, '/') || '/' \
         OR substr(rtrim({param}, '/'), 1, length(rtrim({alias}.prefix, '/')) + 1) = rtrim({alias}.prefix, '/') || '/')"
    )
}

/// A `running` row on the prefix that no newer SUCCESS of the same export has superseded.
fn live_on_prefix(alias: &str, param: &str) -> String {
    format!(
        "{overlap} AND {alias}.status = 'running' AND NOT EXISTS (\
           SELECT 1 FROM run_status r2 WHERE r2.export_name = {alias}.export_name \
           AND {newer_overlap} AND r2.started_at > {alias}.started_at \
           AND r2.status = 'success')",
        overlap = prefix_overlaps(alias, param),
        newer_overlap = prefix_overlaps("r2", param),
    )
}

#[cfg(test)]
impl StateStore {
    /// The Postgres state named by `RIVET_TEST_STATE_URL`, or `None` after recording the skip.
    pub(crate) fn postgres_for_test() -> Option<Self> {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            crate::test_hook::skip_live("RIVET_TEST_STATE_URL unset");
            return None;
        };
        if !url.starts_with("postgres") {
            crate::test_hook::skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
            return None;
        }
        Some(
            Self::open_at_ref(&crate::state::StateRef::Postgres(url.into()))
                .expect("open the Postgres test state"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lease holder on one source closes its export's dead `running` rows of that source and of no recorded source, in both tables, and leaves finished rows, another source's live run and other exports alone.
    #[test]
    fn the_dead_runs_of_an_export_on_its_source_are_stamped_interrupted_and_nothing_else() {
        let s = StateStore::open_in_memory().unwrap();
        for (run, export, source) in [
            ("o1", "orders", Some("pg://a/db")),
            ("o2", "orders", None),
            ("o3", "orders", Some("pg://a/db")),
            ("o4", "orders", Some("pg://b/db")),
            ("u1", "users", Some("pg://a/db")),
        ] {
            s.begin_run(run, export, "file:///out", "2026-01-01T00:00:00Z")
                .unwrap();
            if let Some(source) = source {
                s.record_run_source(run, source).unwrap();
            }
            if run != "o3" {
                s.project_running_aggregate(run, export, "full", "parquet")
                    .unwrap();
            }
        }
        s.finish_run("o3", "success", "2026-01-01T00:01:00Z")
            .unwrap();
        let statuses = |table: &str| {
            s.query(
                &format!("SELECT run_id || '=' || status FROM {table} ORDER BY run_id"),
                &[],
                |r| r.text(0),
            )
            .unwrap()
        };
        assert_eq!(
            s.interrupt_dead_runs("orders", "pg://a/db", "2026-01-02T00:00:00Z")
                .unwrap(),
            2
        );
        assert_eq!(
            statuses("run_status"),
            [
                "o1=interrupted",
                "o2=interrupted",
                "o3=success",
                "o4=running",
                "u1=running"
            ]
        );
        assert_eq!(
            statuses("export_metrics"),
            [
                "o1=interrupted",
                "o2=interrupted",
                "o4=running",
                "u1=running"
            ]
        );
        assert_eq!(
            s.interrupt_dead_runs("orders", "pg://a/db", "2026-01-03T00:00:00Z")
                .unwrap(),
            0
        );
    }

    /// The run a refusal names is the export's newest row still `running` on its source.
    #[test]
    fn the_newest_running_run_of_an_export_is_its_own_latest_unfinished_row() {
        let s = StateStore::open_in_memory().unwrap();
        assert_eq!(s.newest_running_run("orders", "pg://a/db").unwrap(), None);
        for (run, export, at) in [
            ("orders_1", "orders", "2026-01-01T00:00:00Z"),
            ("orders_2", "orders", "2026-01-02T00:00:00Z"),
            ("orders_3", "orders", "2026-01-03T00:00:00Z"),
            ("orders_4", "orders", "2026-01-04T00:00:00Z"),
            ("users_9", "users", "2026-01-09T00:00:00Z"),
        ] {
            s.begin_run(run, export, "file:///out", at).unwrap();
            s.record_run_source(run, "pg://a/db").unwrap();
        }
        s.record_run_source("orders_4", "pg://b/db").unwrap();
        s.finish_run("orders_3", "success", "2026-01-03T00:01:00Z")
            .unwrap();
        assert_eq!(
            s.newest_running_run("orders", "pg://a/db")
                .unwrap()
                .as_deref(),
            Some("orders_2")
        );
        assert_eq!(
            s.newest_running_run("orders", "pg://b/db")
                .unwrap()
                .as_deref(),
            Some("orders_4")
        );
    }

    /// The three queries that decide "is this prefix live?" compose ONE containment fragment and ONE supersession fragment.
    #[test]
    fn the_liveness_predicate_is_one_fragment() {
        let overlap = prefix_overlaps("r", "?1");
        assert_eq!(
            overlap,
            "(rtrim(r.prefix, '/') = rtrim(?1, '/') \
             OR substr(r.prefix, 1, length(rtrim(?1, '/')) + 1) = rtrim(?1, '/') || '/' \
             OR substr(rtrim(?1, '/'), 1, length(rtrim(r.prefix, '/')) + 1) = rtrim(r.prefix, '/') || '/')"
        );
        let live = live_on_prefix("r", "?1");
        assert!(live.starts_with(&overlap), "{live}");
        assert!(live.contains("r.status = 'running'"), "{live}");
        assert!(
            live.contains("r2.started_at > r.started_at AND r2.status = 'success'"),
            "supersession needs a newer SUCCESS: {live}"
        );
        assert!(
            live.contains(&prefix_overlaps("r2", "?1")),
            "the successor is matched by the SAME containment: {live}"
        );
    }

    /// The escape hatch's three answers, each decisive.
    #[test]
    fn finish_run_checked_stamps_only_a_live_row_and_says_why_otherwise() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("r1", "orders", "gs://b/p", "2026-08-21T00:00:00Z")
            .unwrap();
        assert_eq!(
            s.finish_run_checked("nope", "2026-08-21T01:00:00Z")
                .unwrap(),
            FinishOutcome::NotFound
        );
        assert_eq!(
            s.finish_run_checked("r1", "2026-08-21T01:00:00Z").unwrap(),
            FinishOutcome::Stamped
        );
        assert!(!s.has_active_run_on_prefix("gs://b/p").unwrap());
        assert_eq!(
            s.finish_run_checked("r1", "2026-08-21T02:00:00Z").unwrap(),
            FinishOutcome::AlreadyTerminal("interrupted".into())
        );
        let rows = s.recent_run_status(10, false).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].run_id, "r1");
        assert_eq!(rows[0].status, "interrupted");
        assert!(s.recent_run_status(10, true).unwrap().is_empty());
    }

    /// Round-10 mutants: `run_status_of` stubbed to Some("xyzzy") survived.
    #[test]
    fn run_status_of_reports_the_row_or_none() {
        let s = StateStore::open_in_memory().unwrap();
        assert_eq!(s.run_status_of("nope").unwrap(), None);
        s.begin_run("r1", "orders", "gs://b/p", "2026-08-21T00:00:00Z")
            .unwrap();
        assert_eq!(s.run_status_of("r1").unwrap().as_deref(), Some("running"));
        s.finish_run("r1", "success", "2026-08-21T01:00:00Z")
            .unwrap();
        assert_eq!(s.run_status_of("r1").unwrap().as_deref(), Some("success"));
    }

    /// Round-4 split-wedge closure, ledger half.
    #[test]
    fn ceased_split_units_are_stamped_and_live_ordinals_survive() {
        let s = StateStore::open_in_memory().unwrap();
        for ord in 0..8 {
            s.begin_run(
                &format!("r{ord}"),
                &format!("orders#{ord}"),
                "gs://b/orders",
                "2026-08-21T00:00:00Z",
            )
            .unwrap();
        }
        s.begin_run("rx", "unrelated", "gs://b/other", "2026-08-21T00:00:00Z")
            .unwrap();
        s.begin_run(
            "rf",
            "orders#7",
            "gs://other/orders",
            "2026-08-21T00:00:00Z",
        )
        .unwrap();
        let stamped = s
            .interrupt_ceased_split_units("orders", "gs://b/orders", 5, "2026-08-21T01:00:00Z")
            .unwrap();
        let mut names = stamped.clone();
        names.sort();
        assert_eq!(names, ["orders#5", "orders#6", "orders#7"]);
        let active = s.active_run_ids_on_prefix("gs://b/orders").unwrap();
        for ord in 0..5 {
            assert!(active.contains(&format!("r{ord}")), "r{ord} must stay live");
        }
        for ord in 0..5 {
            s.finish_run(&format!("r{ord}"), "success", "2026-08-21T02:00:00Z")
                .unwrap();
        }
        assert!(
            !s.has_active_run_on_prefix("gs://b/orders").unwrap(),
            "with every kept unit finished and every ceased one stamped, the \
             prefix must read idle — this is the wedge the stamp exists to close"
        );
        assert!(s.has_active_run_on_prefix("gs://b/other").unwrap());
        assert!(
            s.has_active_run_on_prefix("gs://other/orders").unwrap(),
            "the OTHER config's same-name giant is out of scope — prefix-scoping is the guard"
        );
    }

    /// The load's skip set is keyed on `run_id` alone, and a CDC run's manifest GROWS under one id (the sink rewrites a `Success` superset at every commit-boundary roll).
    #[test]
    fn active_run_ids_names_the_in_flight_run_and_forgets_the_finished_one() {
        let dir = tempfile::tempdir().unwrap();
        let st = crate::state::StateStore::open_sqlite(dir.path().join("s.db").to_str().unwrap())
            .unwrap();
        let prefix = "gs://b/exports/orders/";
        st.begin_run("run_a", "orders", prefix, "2026-08-01T10:00:00Z")
            .unwrap();
        st.begin_run("run_b", "customers", prefix, "2026-08-01T10:00:01Z")
            .unwrap();
        let active = st.active_run_ids_on_prefix(prefix).unwrap();
        assert!(
            active.contains("run_a") && active.contains("run_b"),
            "both in-flight runs must be named; got {active:?}"
        );
        st.finish_run("run_a", "success", "2026-08-01T10:05:00Z")
            .unwrap();
        let active = st.active_run_ids_on_prefix(prefix).unwrap();
        assert!(
            !active.contains("run_a"),
            "a finished run is no longer active — it must be recordable as consumed"
        );
        assert!(
            active.contains("run_b"),
            "the still-running sibling stays active"
        );
    }

    const P: &str = "gs://b/exports/orders/";

    /// Two independent StateStore connections to ONE shared Postgres state db behave like two rivet PROCESSES on a shared-Postgres deployment.
    #[test]
    fn a_running_successor_does_not_supersede_but_a_finished_one_does() {
        let st = StateStore::open_in_memory().expect("state");
        let (pa, pb) = ("gs://b/e/runA/", "gs://b/e/runB/");
        st.begin_run("r1", "e", pa, "2026-01-01T00:00:01Z").unwrap();
        assert!(st.has_active_run_on_prefix(pa).unwrap(), "r1 is running");

        st.begin_run("r2", "e", pb, "2026-01-01T00:00:02Z").unwrap();
        assert!(
            st.has_active_run_on_prefix(pa).unwrap(),
            "a RUNNING successor has re-done nothing, so r1's prefix must stay \
             active — releasing it here is what lets the collector delete a live \
             writer's committed parts"
        );

        st.finish_run("r2", "success", "2026-01-01T00:00:09Z")
            .unwrap();
        assert!(
            !st.has_active_run_on_prefix("gs://b/e/").unwrap(),
            "a FINISHED successor under the same base supersedes it — no age timer, no second clock"
        );
        assert!(
            st.has_active_run_on_prefix(pa).unwrap(),
            "a success that never touched r1's prefix is not r1's successor"
        );
    }

    #[test]
    fn pg_shared_state_cross_connection_visibility_and_supersession() {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL unset");
        };
        if !url.starts_with("postgres") {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
        }
        let pg = crate::state::StateRef::Postgres(url.into());
        let a = StateStore::open_at_ref(&pg).expect("conn A");
        let b = StateStore::open_at_ref(&pg).expect("conn B");

        let pid = std::process::id();
        let exp = format!("conc_test_{pid}");
        let (r1, r2) = (format!("r1_{pid}"), format!("r2_{pid}"));
        let pa = format!("gs://b/{exp}/runA/");
        let pb = format!("gs://b/{exp}/runB/");

        a.begin_run(&r1, &exp, &pa, "2026-01-01T00:00:01Z").unwrap();
        assert!(
            b.has_active_run_on_prefix(&pa).unwrap(),
            "conn B must see conn A's active run on the shared Postgres state (multi-process gc signal)"
        );

        b.begin_run(&r2, &exp, &pb, "2026-01-01T00:00:02Z").unwrap();
        assert!(
            a.has_active_run_on_prefix(&pa).unwrap(),
            "run1 is STILL active: a newer run that is itself running has not \
             re-done anything, so it cannot release run1's prefix to the collector"
        );

        b.finish_run(&r2, "success", "2026-01-01T00:00:09Z")
            .unwrap();
        assert!(
            !a.has_active_run_on_prefix(&format!("gs://b/{exp}/"))
                .unwrap(),
            "a FINISHED successor under the same base supersedes the crashed run1 — that \
             is what makes a stale `running` row releasable without comparing two clocks"
        );
        assert!(
            a.has_active_run_on_prefix(&pa).unwrap(),
            "a success that never wrote under run1's own prefix is not its successor"
        );
        b.begin_run(&r2, &exp, &pb, "2026-01-01T00:00:02Z").unwrap();
        assert!(
            a.has_active_run_on_prefix(&pb).unwrap(),
            "run2 (newest, same export) is the active run"
        );

        b.finish_run(&r2, "success", "2026-01-01T00:00:03Z")
            .unwrap();
        assert!(
            !a.has_active_run_on_prefix(&pb).unwrap(),
            "a finished run must not count as active"
        );
        a.finish_run(&r1, "success", "2026-01-01T00:00:04Z").ok();
    }

    #[test]
    fn begin_marks_active_then_finish_clears_it() {
        let s = StateStore::open_in_memory().unwrap();
        assert!(
            !s.has_active_run_on_prefix(P).unwrap(),
            "empty → not active"
        );
        s.begin_run("r1", "orders", P, "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix(P).unwrap(),
            "a running run makes the prefix active"
        );
        s.finish_run("r1", "success", "2026-01-01T00:01:00Z")
            .unwrap();
        assert!(
            !s.has_active_run_on_prefix(P).unwrap(),
            "a finished run leaves the prefix inactive"
        );
    }

    #[test]
    fn active_is_scoped_to_the_prefix() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("r1", "orders", P, "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(
            !s.has_active_run_on_prefix("gs://b/exports/users/").unwrap(),
            "a run on a DIFFERENT prefix does not make this one active"
        );
    }

    /// A newer FAILED run does not supersede a live one — only a SUCCESS does.
    #[test]
    fn a_newer_failed_run_does_not_supersede_a_live_one() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("old", "orders", P, "2026-08-28T10:00:00Z")
            .unwrap();
        s.begin_run("new", "orders", P, "2026-08-28T10:05:00Z")
            .unwrap();
        s.finish_run("new", "failed", "2026-08-28T10:05:01Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix(P).unwrap(),
            "a FAILED successor proves somebody tried, not that the old run is \
             dead — treating it as supersession lets gc delete a live run's \
             just-flushed parts"
        );
        assert_eq!(
            s.active_run_ids_on_prefix(P).unwrap(),
            std::collections::HashSet::from(["old".to_string()]),
            "the live run must still be NAMED active for the load's exclusion"
        );
        s.begin_run("new2", "orders", P, "2026-08-28T10:10:00Z")
            .unwrap();
        s.finish_run("new2", "success", "2026-08-28T10:11:00Z")
            .unwrap();
        assert!(
            !s.has_active_run_on_prefix(P).unwrap(),
            "a newer SUCCESS is the one honest supersession signal"
        );
    }

    #[test]
    fn a_superseded_running_row_no_longer_counts_as_active() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("r1", "orders", P, "2026-01-01T00:00:00Z")
            .unwrap();
        s.begin_run("r2", "orders", P, "2026-01-01T00:00:05Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix(P).unwrap(),
            "r2 is running and newest → active"
        );
        s.finish_run("r2", "success", "2026-01-01T00:01:00Z")
            .unwrap();
        assert!(
            !s.has_active_run_on_prefix(P).unwrap(),
            "r1 is `running` but SUPERSEDED by the finished r2 → NOT active (no clock)"
        );
    }

    /// Shared state DB, two configs both exporting `users` into DIFFERENT prefixes.
    #[test]
    fn a_success_of_a_same_named_export_on_another_prefix_does_not_supersede() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("a", "users", "pa/", "2026-01-01T00:00:00Z")
            .unwrap();
        s.begin_run("b", "users", "pb/", "2026-01-01T00:00:05Z")
            .unwrap();
        s.finish_run("b", "success", "2026-01-01T00:01:00Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix("pa/").unwrap(),
            "a newer success on ANOTHER prefix is not a successor of `a`"
        );
        assert_eq!(
            s.active_run_ids_on_prefix("pa/").unwrap(),
            HashSet::from(["a".to_string()])
        );
    }

    #[test]
    fn a_lone_crashed_running_row_still_counts_as_active() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("r1", "orders", P, "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix(P).unwrap(),
            "a lone crashed running run keeps the prefix active (deferred, not deleted)"
        );
    }

    #[test]
    fn begin_rearms_running_on_a_resumed_run_id() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("r1", "orders", P, "2026-01-01T00:00:00Z")
            .unwrap();
        s.finish_run("r1", "failed", "2026-01-01T00:01:00Z")
            .unwrap();
        assert!(!s.has_active_run_on_prefix(P).unwrap());
        s.begin_run("r1", "orders", P, "2026-01-01T00:02:00Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix(P).unwrap(),
            "re-begin on the same run_id re-arms running"
        );
    }

    #[test]
    fn finish_on_an_absent_run_is_a_noop() {
        let s = StateStore::open_in_memory().unwrap();
        s.finish_run("ghost", "success", "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(!s.has_active_run_on_prefix(P).unwrap());
    }

    #[test]
    fn active_matches_a_partitioned_child_prefix() {
        let s = StateStore::open_in_memory().unwrap();
        let child = "gs://b/exports/orders/created_at=2023-01-01/";
        s.begin_run("r1", "orders", child, "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix("gs://b/exports/orders/")
                .unwrap(),
            "a partitioned child run makes its base prefix active"
        );
    }

    #[test]
    fn active_matches_a_load_prefix_under_a_cdc_base() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("cdc1", "cdc", "gs://b/exports/", "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix("gs://b/exports/orders/")
                .unwrap(),
            "a per-table child of a live CDC base prefix counts as active"
        );
        assert!(
            !s.has_active_run_on_prefix("gs://b/other/orders/").unwrap(),
            "an unrelated prefix outside the CDC base is NOT active"
        );
    }

    #[test]
    fn active_is_boundary_safe_against_a_string_sibling() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run(
            "r1",
            "arch",
            "gs://b/exports/orders_archive/p.parquet",
            "2026-01-01T00:00:00Z",
        )
        .unwrap();
        assert!(
            !s.has_active_run_on_prefix("gs://b/exports/orders").unwrap(),
            "a sibling prefix that merely string-starts-with must not count as active"
        );
    }

    /// The path containment the ledger must answer, spelled on literal characters.
    fn literal_overlap(run: &str, query: &str) -> bool {
        let (r, q) = (run.trim_end_matches('/'), query.trim_end_matches('/'));
        r == q || r.starts_with(&format!("{q}/")) || q.starts_with(&format!("{r}/"))
    }

    /// Hostile path segments paired with a look-alike that differs in exactly the characters a LIKE pattern or an escape would swallow.
    const HOSTILE_SEGMENTS: &[(&str, &str)] = &[
        ("a\\b", "ab"),
        ("p\\q", "pq"),
        ("a\\", "a"),
        ("\\", "x"),
        ("a\\\\b", "a\\b"),
        ("a\\_b", "a_b"),
        ("a\\%b", "a%b"),
        ("a_b", "aXb"),
        ("a%b", "aXYb"),
        ("%", "x"),
        ("_", "x"),
        ("a'b", "a\"b"),
        ("caf\u{e9}", "cafe"),
        ("\u{65e5}\u{672c}", "\u{672c}\u{672c}"),
        ("a b", "a  b"),
        ("Up", "up"),
    ];

    /// Every (run prefix, query prefix) pair the hostile segments produce, in the shapes gc and cleanup ask.
    fn hostile_prefix_pairs() -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        for (seg, twin) in HOSTILE_SEGMENTS {
            let base = format!("gs://b/v1/{seg}/");
            let other = format!("gs://b/v1/{twin}/");
            pairs.push((base.clone(), base.clone()));
            pairs.push((base.clone(), format!("{base}orders/")));
            pairs.push((format!("{base}d=1/"), base.clone()));
            pairs.push((format!("{base}d=1/"), format!("{base}d=1")));
            pairs.push((base.clone(), format!("{other}orders/")));
            pairs.push((other.clone(), format!("{base}orders/")));
            pairs.push((format!("{other}d=1/"), base.clone()));
            pairs.push((base.clone(), format!("{base}x")));
            pairs.push((format!("{base}x"), base.clone()));
        }
        pairs
    }

    /// The decision gc and cleanup take from the ledger equals literal path containment for every hostile pair.
    fn assert_ledger_decides_by_literal_containment(s: &StateStore, tag: &str) {
        let mut wrong = Vec::new();
        for (i, (run_prefix, query)) in hostile_prefix_pairs().iter().enumerate() {
            let run = format!("{tag}_{i}");
            let export = format!("{tag}_exp_{i}");
            s.begin_run(&run, &export, run_prefix, "2026-01-01T00:00:00Z")
                .unwrap();
            let want = literal_overlap(run_prefix, query);
            let single = s.has_active_run_on_prefix(query).unwrap();
            let named = s.active_run_ids_on_prefix(query).unwrap().contains(&run);
            s.finish_run(&run, "success", "2026-01-01T00:00:01Z")
                .unwrap();
            if single != want || named != want {
                wrong.push(format!(
                    "run {run_prefix:?} query {query:?}: want {want}, has_active {single}, active_ids {named}"
                ));
            }
        }
        assert!(
            wrong.is_empty(),
            "the ledger's liveness answer must be path containment on the literal text:\n{}",
            wrong.join("\n")
        );
    }

    /// The split path closes the ceased units of a giant under exactly the prefixes that literally contain its run's prefix.
    fn assert_split_stamp_decides_by_literal_containment(s: &StateStore, tag: &str) {
        let mut wrong = Vec::new();
        for (i, (run_prefix, query)) in hostile_prefix_pairs().iter().enumerate() {
            let giant = format!("{tag}_g{i}");
            let run = format!("{tag}_s{i}");
            s.begin_run(
                &run,
                &format!("{giant}#4"),
                run_prefix,
                "2026-01-01T00:00:00Z",
            )
            .unwrap();
            let want = literal_overlap(run_prefix, query);
            let stamped = !s
                .interrupt_ceased_split_units(&giant, query, 2, "2026-01-01T00:00:01Z")
                .unwrap()
                .is_empty();
            s.finish_run(&run, "success", "2026-01-01T00:00:02Z").ok();
            if stamped != want {
                wrong.push(format!(
                    "run {run_prefix:?} query {query:?}: want {want}, stamped {stamped}"
                ));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    fn unique_tag(kind: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{kind}_{}_{nanos}", std::process::id())
    }

    #[test]
    fn sqlite_state_decides_liveness_by_literal_path_containment() {
        let s = StateStore::open_in_memory().unwrap();
        assert_ledger_decides_by_literal_containment(&s, &unique_tag("lit"));
        assert_split_stamp_decides_by_literal_containment(&s, &unique_tag("spl"));
    }

    #[test]
    fn postgres_state_decides_liveness_by_literal_path_containment() {
        let Some(s) = StateStore::postgres_for_test() else {
            return;
        };
        assert_ledger_decides_by_literal_containment(&s, &unique_tag("lit"));
        assert_split_stamp_decides_by_literal_containment(&s, &unique_tag("spl"));
    }
}
