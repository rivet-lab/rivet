//! Pool policy for the per-table `load` / `compact` loops: the ceiling warning and when a lost ledger is fatal.

pub(crate) use crate::workers::{MAX_POOL, effective_pool, run_workers};

/// What to tell an operator who asked for more workers than the ceiling allows.
pub(crate) fn pool_ceiling_warning(
    requested: Option<usize>,
    items: usize,
    ledger: LedgerKind,
) -> Option<String> {
    let asked = requested?;
    if asked <= MAX_POOL {
        return None;
    }
    let running = effective_pool(requested, items);
    if ledger == LedgerKind::Absent {
        return Some(format!(
            "--pool {asked} exceeds the ceiling of {MAX_POOL}; running {running} worker(s). \
             This run has NO state ledger — its open failed above — so no ledger bound \
             applies; what remains is the warehouse's own: BigQuery allows 100 concurrent \
             interactive queries per PROJECT, shared with everything else running there."
        ));
    }
    if ledger == LedgerKind::Sqlite {
        return Some(format!(
            "--pool {asked} exceeds the ceiling of {MAX_POOL}; running {running} worker(s). \
             On a SQLite ledger rivet cannot usefully go higher in any case: WAL gives many \
             readers but exactly ONE writer, so workers queue on the write lock and stop \
             gaining past that point. The ceiling is {MAX_POOL} on every backend: a Postgres \
             state store removes the single writer, not the ceiling."
        ));
    }
    Some(format!(
        "--pool {asked} exceeds the ceiling of {MAX_POOL}; running {running} worker(s). \
         Each worker opens its own ledger connection and one more connection renews their \
         leases, so N workers are N + 1 connections to the Postgres state backend, counted \
         against its `max_connections` (100 by default, minus the superuser reserve and \
         whatever else is connected). The warehouse has its \
         own budget as well: BigQuery allows 100 concurrent interactive queries per PROJECT, \
         shared with everything else running there."
    ))
}

/// Which ledger the workers will open — including having NONE.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum LedgerKind {
    Sqlite,
    Postgres,
    Absent,
}

/// Whether a worker that has no ledger must REFUSE its table rather than load it.
pub(crate) fn reconnect_failure_is_fatal(parent_had_state: bool, worker_has_state: bool) -> bool {
    parent_had_state && !worker_has_state
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ceiling warning fires only when the ASK exceeds it, and names both bounds.
    #[test]
    fn the_ceiling_warns_only_above_it_and_says_what_to_do_per_backend() {
        assert!(
            pool_ceiling_warning(None, 50, LedgerKind::Postgres).is_none(),
            "no request, no warning"
        );
        assert!(
            pool_ceiling_warning(Some(MAX_POOL), 500, LedgerKind::Postgres).is_none(),
            "asking for exactly the ceiling is allowed, not excessive"
        );

        let none = pool_ceiling_warning(Some(MAX_POOL + 1), 500, LedgerKind::Absent)
            .expect("the ceiling still binds without a ledger");
        assert!(
            none.contains("NO state ledger") && !none.contains("max_connections"),
            "a run with no ledger must not be sent to a state DB's limits: {none}"
        );
        assert!(
            none.contains("BigQuery"),
            "the warehouse budget is the one that still applies: {none}"
        );

        let pg = pool_ceiling_warning(Some(MAX_POOL + 1), 500, LedgerKind::Postgres)
            .expect("one over the ceiling must warn");
        assert!(
            pg.contains(&(MAX_POOL + 1).to_string()),
            "the warning must quote what was asked: {pg}"
        );
        assert!(
            pg.contains("N workers are N + 1 connections to the Postgres state backend"),
            "the count is the workers plus the lease keeper: {pg}"
        );
        assert!(
            pg.contains("max_connections") && pg.contains("BigQuery"),
            "on Postgres it must name BOTH bounds — the state backend runs out first, and \
             the error arrives from a database the operator was not thinking about: {pg}"
        );

        let lite = pool_ceiling_warning(Some(MAX_POOL + 1), 500, LedgerKind::Sqlite)
            .expect("the SQLite ledger must warn too");
        assert!(
            lite.contains(&format!("The ceiling is {MAX_POOL} on every backend"))
                && !lite.contains("RIVET_STATE_URL"),
            "moving the state to Postgres buys no workers past the ceiling: {lite}"
        );
        assert!(
            lite.contains("ONE writer"),
            "on SQLite the limit is the storage engine, not a quota — say so: {lite}"
        );
    }

    /// Only a worker that lost a ledger the RUN started with is fatal.
    #[test]
    fn only_a_worker_that_lost_a_ledger_the_run_had_is_fatal() {
        assert!(
            reconnect_failure_is_fatal(true, false),
            "the run had a ledger and this worker lost it — that is THIS table's failure"
        );
        assert!(
            !reconnect_failure_is_fatal(true, true),
            "both have a ledger — ordinary work"
        );
        assert!(
            !reconnect_failure_is_fatal(false, false),
            "the run never had a ledger — the documented stateless degradation, warned once"
        );
        assert!(
            !reconnect_failure_is_fatal(false, true),
            "no parent ledger yet the worker opened one — not a failure, and not a state \
             the caller can produce; pinned so a flipped operator cannot hide here"
        );
    }
}
