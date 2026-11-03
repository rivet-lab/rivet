"""The CDC leg of the default oracle: rivet's stored checkpoint graded against the server's own position.

One body, `position_findings`, runs after every graded exit-0 CDC run. Each engine supplies a
`Reader` (the server's head, and what else it can say about a stored position); the order of
two positions is the oracle's own `pos_order` expression, so a checkpoint is compared the way
the delivered `__pos` values are folded. Graded, and no more:

  * the checkpoint file exists and parses when the run delivered events,
  * it is not behind an event this run delivered,
  * it is not ahead of the server's head,
  * the engine's own acknowledgement of it (`Reader.refusals`).

Equality to the head is not a documented guarantee on PostgreSQL (non-transactional WAL moves
the head while idle) or MySQL (a checkpoint advances at commit boundaries), so it is not graded.
"""

from __future__ import annotations

import json
import os
from typing import Protocol


class Reader(Protocol):
    """One engine's view of the server's position."""

    def head(self, ora, spec: dict) -> dict:
        """The server's current end position, shaped like the engine's `__pos`."""

    def refusals(self, ora, spec: dict, ckpt: dict, order) -> list[str]:
        """What else the server says against the stored position `ckpt` (`order` maps a position to its comparable text)."""


def _lit(s: str) -> str:
    """A single-quoted SQL literal."""
    return "'" + s.replace("'", "''") + "'"


class PostgresReader:
    """The WAL insert position and the slot's `confirmed_flush_lsn`."""

    def head(self, ora, spec: dict) -> dict:
        """`pg_current_wal_insert_lsn()`."""
        sql = "SELECT pg_current_wal_insert_lsn()::text"
        return {"lsn": ora.scalar(f"SELECT * FROM postgres_query('pg', {_lit(sql)})")}

    def refusals(self, ora, spec: dict, ckpt: dict, order) -> list[str]:
        """A marked checkpoint is never behind its slot: the slot's confirmed_flush_lsn above it would resume past undelivered changes."""
        slot = spec.get("slot")
        if not slot or ckpt.get("confirmed_first") is not True:
            return []
        sql = f"SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = {_lit(slot)}"
        rows = ora.rows(f"SELECT * FROM postgres_query('pg', {_lit(sql)})")
        if not rows or rows[0][0] is None:
            return []
        flushed = {"lsn": rows[0][0]}
        if order(flushed) > order(ckpt):
            return [f"the slot {slot!r} confirmed_flush_lsn {rows[0][0]} is ahead of the stored checkpoint {ckpt['lsn']}: a resume would skip the changes in between"]
        return []


def mysql_binary_logs(url: str) -> list[tuple[str, int]]:
    """(file, size) per binlog file the server holds, oldest first, from `SHOW BINARY LOGS` (DuckDB's mysql scanner cannot prepare a SHOW)."""
    from urllib.parse import unquote, urlsplit

    import pymysql

    u = urlsplit(url)
    with pymysql.connect(host=u.hostname, port=u.port or 3306, user=unquote(u.username or ""), password=unquote(u.password or ""), connect_timeout=10) as c:
        with c.cursor() as cur:
            cur.execute("SHOW BINARY LOGS")
            return [(str(r[0]), int(r[1])) for r in cur.fetchall()]


class MysqlReader:
    """The current binlog file and its size, from `SHOW BINARY LOGS` (the same statement on every server version)."""

    def __init__(self, logs=mysql_binary_logs) -> None:
        self.logs = logs

    def head(self, ora, spec: dict) -> dict:
        """The last binlog file at its size."""
        file, size = self.logs(spec["url"])[-1]
        return {"file": file, "pos": size}

    def refusals(self, ora, spec: dict, ckpt: dict, order) -> list[str]:
        """The stored file must be one the server holds, and the offset inside it."""
        sizes = dict(self.logs(spec["url"]))
        file, pos = ckpt.get("file"), ckpt.get("pos")
        if file not in sizes:
            return [f"the stored checkpoint names binlog file {file!r}, which the server does not hold"]
        if pos is None or int(pos) > sizes[file]:
            return [f"the stored checkpoint offset {pos} is past the end ({sizes[file]}) of binlog file {file!r}"]
        return []


class MssqlReader:
    """The higher of the capture job's max LSN and the newest capture instance's start, the two places an anchor may sit."""

    def head(self, ora, spec: dict) -> dict:
        """The highest LSN a stored position may hold, as lowercase hex."""
        seen = [
            ora.scalar(f"SELECT * FROM mssql_scan('ms', {_lit(sql)})")
            for sql in (
                "SELECT lower(CONVERT(varchar(max), sys.fn_cdc_get_max_lsn(), 2))",
                "SELECT lower(CONVERT(varchar(max), max(start_lsn), 2)) FROM cdc.change_tables",
            )
        ]
        return {"lsn": max((lsn for lsn in seen if lsn), default=None)}

    def refusals(self, ora, spec: dict, ckpt: dict, order) -> list[str]:
        """SQL Server has no second statement about a stored LSN."""
        return []


READERS: dict[str, Reader] = {"postgres": PostgresReader(), "mysql": MysqlReader(), "mssql": MssqlReader()}


def position_findings(ora, spec: dict, new_events: str | None, order_sql: str, partial: list[str], readers: dict[str, Reader] = READERS) -> list[str]:
    """The disagreements between the stored checkpoint and the server; `new_events` is a relation of this run's change rows carrying `__ord`."""
    reader = readers.get(spec["engine"])
    ckpt_path = spec.get("checkpoint")
    if reader is None or not ckpt_path:
        return []

    def order(pos: dict) -> str:
        """The comparable text of a position."""
        return ora.db.execute(f"SELECT {order_sql} FROM (SELECT ?::VARCHAR AS __pos)", [json.dumps(pos)]).fetchone()[0]

    delivered = ora.scalar(f"SELECT max(__ord) FROM {new_events}") if new_events else None
    if not os.path.isfile(ckpt_path):
        return [f"CHECKPOINT: the run delivered events but {ckpt_path} does not exist"] if delivered else []
    try:
        with open(ckpt_path) as f:
            ckpt = json.load(f)
    except ValueError as e:
        return [f"CHECKPOINT: {ckpt_path} does not parse: {e}"]
    head = reader.head(ora, spec)
    found = []
    if delivered and order(ckpt) < delivered:
        found.append(f"CHECKPOINT: the stored position {json.dumps(ckpt, sort_keys=True)} is behind an event this run delivered (order {delivered}): a resume would deliver it again")
    if order(ckpt) > order(head):
        found.append(f"CHECKPOINT: the stored position {json.dumps(ckpt, sort_keys=True)} is ahead of the server's {json.dumps(head, sort_keys=True)}")
    found += [f"CHECKPOINT: {why}" for why in reader.refusals(ora, spec, ckpt, order)]
    if spec["engine"] == "postgres" and ckpt.get("confirmed_first") is not True:
        partial.append("the PostgreSQL checkpoint carries no confirmed_first mark (written by an older release): its slot is not compared")
    return found
