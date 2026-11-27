"""Accepted credential leaks of the no_secret_in_output matrix: each entry names the cells it covers, why it is accepted and when it expires."""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass

try:
    from .known_red import _EXPIRES
except ImportError:  # pragma: no cover
    from known_red import _EXPIRES  # type: ignore

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
    paths: tuple[str, ...] = ANY

    def covers(self, cell: dict[str, str], sink_kind: str) -> bool:
        """Whether this entry accepts a leak of `sink_kind` in `cell`."""
        pairs = ((self.commands, cell["command"]), (self.engines, cell["engine"]), (self.slots, cell["slot"]),
                 (self.spellings, cell["spelling"]), (self.sinks, sink_kind), (self.paths, cell["path"]))
        return all(f == ANY or v in f for f, v in pairs)


KNOWN_LEAKS: tuple[KnownLeak, ...] = (
    KnownLeak("2026-11-26: BigQuery load leg, a server that echoes the Authorization header in its 401 body gets the token printed verbatim in stderr and the summary file because api_error_detail (src/load/bq_rest.rs) copies the warehouse body unmasked; "
              "the same leak as known_red no_secret_in_output_cloud[offline]; open, closes when load masks server error text (see #1146)",
              _EXPIRES, commands=("load",), engines=("postgres",), slots=("bigquery",), spellings=("access-token-env",),
              sinks=("stderr", "file"), paths=("staged-hostile",)),
    KnownLeak("2026-11-26: ClickHouse load leg, a server that echoes the Authorization header in its 401 body gets the Basic credential printed verbatim in stderr and the summary file because trim_ch_error (src/load/clickhouse.rs) copies the warehouse body unmasked; "
              "the same class as the BigQuery entry, a second site; open, closes when load masks server error text",
              _EXPIRES, commands=("load",), engines=("postgres",), slots=("load",), spellings=("url-userinfo", "url-query-password", "password-env"),
              sinks=("stderr", "file"), paths=("staged-hostile",)),
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
