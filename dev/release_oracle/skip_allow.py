"""Excused env skips: a full gate grades every ENV-caused SKIP as INFRA unless a dated entry here names it.

An entry is keyed by the skipped cell's (scenario, engine, store); `*` matches any engine or store. It carries a dated
reason and what closes it, and stops excusing on its expiry day, at the gate and again at the tag.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass


@dataclass(frozen=True)
class Excuse:
    scenario: str
    engine: str
    store: str
    reason: str
    expires: str  # YYYY-MM-DD


FOLD_CELLS = (
    "a_drop_spelled_in_another_case_stops_the_stream_on_a_folding_server_mysql",
    "a_statement_logged_write_in_another_case_stops_the_stream_on_a_folding_server_mysql",
    "a_truncate_spelled_in_another_case_stops_the_stream_on_a_folding_server_mysql",
    "a_truncate_spelled_with_a_character_the_server_folds_stops_the_stream_mysql",
)

PG_STATE_CELLS = (
    "one_keeper_renews_every_lease_and_reports_the_one_that_is_gone",
    "pg_a_chunk_run_whose_task_insert_fails_is_not_left_open",
    "pg_chunk_checkpoint_claim_complete",
    "pg_load_spec_round_trips",
    "pg_several_writers_migrating_one_database_at_once_all_succeed",
)

BY_DESIGN_SKIPS = (
    ("warehouse-load-success-path", "`rivet load` refuses a local destination and no object-store emulator stages a bucket; the config-time refusals are graded",
     "the stand gets an object-store emulator"),
    ("gcp-secret-resolved", "the Secret Manager endpoint is a const https URL (src/config/gcp_secret.rs) with no mock seam; the fetch-fail and token paths are graded",
     "the endpoint becomes overridable for a stub"),
    *((f"ambient-metadata/{m}", f"reqsign reads a fixed link-local {m} metadata URL with no environment override and no stub can stand on it without root; the other ambient sources are graded",
       "the stand grants a stub the link-local address or reqsign reads an override") for m in ("aws", "azure", "google")),
)

STAND_SKIPS = (
    ("no_secret_in_output/success-path", "-", "the stand holds no provisioned canary users, so the right-password success path of the source engines is not graded",
     "the seed provisions a canary user per source engine"),
    ("no_secret_in_output/state-tls-success", "-", "the stand postgres runs ssl=off, so state over TLS success is not graded; the sslmode=require error path is",
     "the stand postgres serves TLS"),
    ("no_secret_in_output/wrong-password-live", "oracle", "no Oracle stand answers on the gate host, so a wrong password against a real Oracle server is not graded",
     "the stand brings up Oracle on the gate host"),
    ("no_secret_in_output/oracle-cdc-events", "-", "no Oracle stand answers, so Oracle CDC with events is not graded",
     "the stand brings up Oracle on the gate host"),
    *((f"no_secret_in_output/slot-reach/{sl}", "-", f"no command connects to the {sl} credential slot on the stand, so its cells are clean but ungraded",
       f"a stub or cloud account answers the {sl} slot") for sl in ("ambient", "bigquery", "load", "snowflake")),
    ("cloud_legs", "-", "the gate host has no cloud credentials or bucket, so the cloud tier (no_silent_null_cloud, no_secret_in_output_cloud, no_debris_after_stop_cloud) is not graded",
     "the gate host is given a test project and bucket"),
)

EXCUSES: tuple[Excuse, ...] = (
    Excuse("coverage", "infra", "-",
           "2026-11-03: cargo-llvm-cov is not installed on the gate host (b0446717 gate log: `no llvm-cov`), so the live-only coverage adjudication does not run. "
           "Closes when `cargo install cargo-llvm-cov --locked` is run on the host",
           "2026-11-17"),
    *(Excuse("fix_cells", "all", cell,
             "2026-11-03: the cell self-skips on v0.32.2 (the stand's lower_case_table_names=0, b0446717 gate log), so it is not proven to fail there. "
             "Closes when the stand runs a folding MySQL or the next release leaves it out of the fix window",
             "2026-11-17") for cell in FOLD_CELLS),
    *(Excuse("fix_cells", "all", cell,
             "2026-11-12: the stage runs the previous binary on the SQLite state only, so a Postgres-state cell self-skips there and is not proven to fail on 0.32.2; "
             "it passes on the current binary in the live modules. Closes when the stage hands the previous binary a scratch Postgres state or the baseline moves past 0.32.2",
             "2026-11-22") for cell in PG_STATE_CELLS),
    *(Excuse(f"no_secret_in_output/{name}", "-", "-", f"2026-11-12: {why}. Closes when {closes}", "2026-11-22")
      for name, why, closes in BY_DESIGN_SKIPS),
    *(Excuse(scenario, engine, "-", f"2026-11-13: {why}. Closes when {closes}; 0.33 ships it ungraded, 0.34 closes it", "2026-11-22")
      for scenario, engine, why, closes in STAND_SKIPS),
)


def excused(scenario: str, engine: str, store: str, today: _dt.date | None = None) -> Excuse | None:
    """The entry that excuses an env skip of this cell today, if any."""
    today = today or _dt.date.today()
    for a in EXCUSES:
        if a.scenario == scenario and a.engine in ("*", engine) and a.store in ("*", store) \
                and today <= _dt.date.fromisoformat(a.expires):
            return a
    return None
