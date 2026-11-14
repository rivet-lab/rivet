//! The load ledger's vocabulary: what a `load_run` row says, and what the rows say about a table.

use crate::load;
use crate::state::{LoadStatus, StateStore};

/// Whether the ledger holds a successful rivet load into `fqtn`; `Unreadable` (warned) when the probe fails.
pub(crate) fn ownership_of(state: Option<&StateStore>, fqtn: &str, op: &str) -> load::Ownership {
    match state {
        Some(s) => match s.has_landed_load(fqtn) {
            Ok(true) => load::Ownership::Own,
            Ok(false) => load::Ownership::Foreign,
            Err(e) => {
                log::warn!(
                    "{op}: the ownership probe for {fqtn} failed ({e:#}) — refusing rather \
                     than treating it as a stateless {op}"
                );
                load::Ownership::Unreadable
            }
        },
        None => load::Ownership::Unknown,
    }
}

/// The `load_run` PRIMARY KEY for one table's row in one invocation.
pub(crate) fn ledger_load_id(run_id: &str, op: &str, table: &str) -> String {
    format!("{run_id}:{op}:{table}")
}

/// How the ledger records a load that did not complete.
pub(crate) fn ledger_status(e: &anyhow::Error) -> &'static str {
    match e.downcast_ref::<load::Refused>() {
        Some(_) => LoadStatus::Refused.as_str(),
        None => LoadStatus::Failed.as_str(),
    }
}

/// The status a load's closing ledger row carries after the load failed: `failed` once a leg landed.
pub(crate) fn closing_status(e: &anyhow::Error, consumed: &[String]) -> &'static str {
    if consumed.is_empty() {
        ledger_status(e)
    } else {
        LoadStatus::Failed.as_str()
    }
}
