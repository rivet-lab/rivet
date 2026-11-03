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


EXCUSES: tuple[Excuse, ...] = (
    Excuse("coverage", "infra", "-",
           "2026-11-03: cargo-llvm-cov is not installed on the gate host (b0446717 gate log: `no llvm-cov`), so the live-only coverage adjudication does not run. "
           "Closes when `cargo install cargo-llvm-cov --locked` is run on the host",
           "2026-11-17"),
    Excuse("fix_cells", "all", "-",
           "2026-11-03: four MySQL fold cells self-skip on v0.32.2 (the stand's lower_case_table_names=0, b0446717 gate log), so they are not proven to fail there. "
           "Closes when the stand runs a folding MySQL or the next release leaves them out of the fix window",
           "2026-11-17"),
)


def excused(scenario: str, engine: str, store: str, today: _dt.date | None = None) -> Excuse | None:
    """The entry that excuses an env skip of this cell today, if any."""
    today = today or _dt.date.today()
    for a in EXCUSES:
        if a.scenario == scenario and a.engine in ("*", engine) and a.store in ("*", store) \
                and today <= _dt.date.fromisoformat(a.expires):
            return a
    return None
