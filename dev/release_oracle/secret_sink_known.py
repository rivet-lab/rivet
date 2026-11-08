"""Accepted credential leaks of the secret-sink matrix: each entry names the cells it covers, why it is accepted and when it expires."""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass

ANY = ("*",)


@dataclass(frozen=True)
class KnownLeak:
    """A class of cells whose leak is recorded: the filters are conjunctive, `*` matches everything."""

    reason: str
    expires: str
    commands: tuple[str, ...] = ANY
    engines: tuple[str, ...] = ANY
    slots: tuple[str, ...] = ANY
    spellings: tuple[str, ...] = ANY
    sinks: tuple[str, ...] = ANY

    def covers(self, cell: dict[str, str], sink_kind: str) -> bool:
        """Whether this entry accepts a leak of `sink_kind` in `cell`."""
        pairs = ((self.commands, cell["command"]), (self.engines, cell["engine"]), (self.slots, cell["slot"]),
                 (self.spellings, cell["spelling"]), (self.sinks, sink_kind))
        return all(f == ANY or v in f for f, v in pairs)


KNOWN_LEAKS: tuple[KnownLeak, ...] = (
    KnownLeak(
        reason="2026-11-08: percent-encoded `pass%77ord` key of a PostgreSQL state URL printed by every state error; closed by #903 (redact_query_secrets decodes the key), remove this entry when #903 is on main",
        expires="2026-11-22", engines=("postgres",), slots=("state",), spellings=("query-pct-key",)),
    KnownLeak(
        reason="2026-11-08: query-pair password of a reachable PostgreSQL state URL printed by the `doctor` State line and the `state copy` done note (redact_pg_url masks userinfo only, src/state/mod.rs:181); closed by #903, remove this entry when #903 is on main",
        expires="2026-11-22", commands=("doctor", "state copy"), engines=("postgres",), slots=("state",),
        spellings=("query-password", "query-pct-value")),
)


def match(cell: dict[str, str], sink_kinds: set[str], today: _dt.date | None = None) -> tuple[list[KnownLeak], bool]:
    """The entries that together accept every sink kind the cell leaked into, and whether all are in date."""
    today = today or _dt.date.today()
    used: list[KnownLeak] = []
    for kind in sorted(sink_kinds):
        hit = next((k for k in KNOWN_LEAKS if k.covers(cell, kind)), None)
        if hit is None:
            return [], False
        used.append(hit)
    return used, all(_dt.date.fromisoformat(k.expires) >= today for k in used)
