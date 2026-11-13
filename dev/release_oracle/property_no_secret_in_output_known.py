"""Accepted credential leaks of the no_secret_in_output matrix: each entry names the cells it covers, why it is accepted and when it expires."""

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
        reason="2026-11-10 PR-IDENTITY: the S3 operator (src/destination/s3.rs build_operator) leaves reqsign reading the "
               "ambient AWS_* env and never builds an anonymous signer, so a configured key is signed with the ambient "
               "session token and allow_anonymous still signs; closes when the S3 builder disables env/config load and "
               "honours allow_anonymous",
        expires="2026-12-10",
        commands=("run", "validate", "doctor"),
        slots=("dest",),
        spellings=("s3:access_key_env", "s3:access_key_env+secret_key_env", "s3:allow_anonymous",
                   "s3:access_key_env+allow_anonymous", "s3:session_token_env+allow_anonymous"),
        sinks=("request",),
    ),
    KnownLeak(
        reason="2026-11-10 PR-CRED: the Azure SAS token rides the request URI that the OpenDAL error context carries into "
               "the retry WARN and the error text; closes when destination error text masks the SAS query",
        expires="2026-12-10",
        commands=("run", "validate"),
        slots=("dest",),
        spellings=("azure:sas_token_env",),
        sinks=("stderr", "file"),
    ),
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
