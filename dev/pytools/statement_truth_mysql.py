"""MySQL engine of the statement-truth corpus: a stdlib wire client, a binlog reader and the case matrix."""
import bisect
import itertools
import os
import re
import socket
import struct
import subprocess
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENGINE = "mysql"
PROFILE = "mysql-stmt"
SEED_DIR = ROOT / "dev/stmt_truth/mysql/seed"
SCHEMA = 1
SERVER_ID = 4242
HOST = "127.0.0.1"
UTF8MB4 = 255
ASCII_DB = "st_ascii"
PROBE_DB = "st_probe"


@dataclass(frozen=True)
class Server:
    """One stand service: its compose name, host port and lower_case_table_names."""

    service: str
    port: int
    lctn: int


SERVERS = {
    "8.0-lctn0": Server("mysql-stmt-80", 3331, 0),
    "8.4-lctn0": Server("mysql-stmt-84", 3332, 0),
    "8.4-lctn1": Server("mysql-stmt-84-lctn1", 3333, 1),
}

CODECS = {
    "big5": "big5", "cp1250": "cp1250", "cp1251": "cp1251", "cp1256": "cp1256", "cp1257": "cp1257",
    "cp850": "cp850", "cp852": "cp852", "cp866": "cp866", "cp932": "cp932", "eucjpms": "euc_jp",
    "euckr": "euc_kr", "gb18030": "gb18030", "gb2312": "gb2312", "gbk": "gbk", "greek": "iso8859_7",
    "hebrew": "iso8859_8", "koi8r": "koi8_r", "koi8u": "koi8_u", "latin1": "cp1252", "latin2": "iso8859_2",
    "latin5": "iso8859_9", "latin7": "iso8859_13", "macce": "mac_latin2", "macroman": "mac_roman",
    "sjis": "cp932", "tis620": "cp874", "ujis": "euc_jp", "utf8": "utf-8", "utf8mb3": "utf-8",
    "utf8mb4": "utf-8", "binary": "utf-8", "ascii": "ascii",
}

MODES = {
    "default": "",
    "ansi": ",ANSI_QUOTES",
    "nobs": ",NO_BACKSLASH_ESCAPES",
    "both": ",ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
}

PRIV_VARS = (
    "Com_grant", "Com_revoke", "Com_create_user", "Com_drop_user", "Com_alter_user", "Com_rename_user",
    "Com_grant_roles", "Com_revoke_roles", "Com_alter_user_default_role", "Com_create_role", "Com_drop_role",
    "Com_flush", "Com_revoke_all",
)
DML_VARS = (
    "Com_insert", "Com_insert_select", "Com_update", "Com_update_multi", "Com_delete", "Com_delete_multi",
    "Com_replace", "Com_replace_select", "Com_load",
)
COM_VARS = ("Com_truncate", "Com_drop_table", "Com_rename_table", *PRIV_VARS, *DML_VARS)
REFUSING = ("truncate", "drop", "rename_table", "rename_column", "statement_write")


class ServerError(Exception):
    """An ERR packet: the server's error number and message."""

    def __init__(self, errno: int, msg: str):
        super().__init__(f"{errno}: {msg}")
        self.errno = errno
        self.msg = msg


@dataclass
class Result:
    """The reply of one COM_QUERY: OK or ERR, and the rows of a result set."""

    errno: int = 0
    msg: str = ""
    rows: list = field(default_factory=list)


def lenenc(buf: bytes, at: int) -> tuple[int | None, int]:
    """A length-encoded integer at an offset, as (value, next offset); None for NULL."""
    b = buf[at]
    if b < 0xFB:
        return b, at + 1
    if b == 0xFB:
        return None, at + 1
    if b == 0xFC:
        return struct.unpack_from("<H", buf, at + 1)[0], at + 3
    if b == 0xFD:
        return int.from_bytes(buf[at + 1:at + 4], "little"), at + 4
    return struct.unpack_from("<Q", buf, at + 1)[0], at + 9


def err_of(pkt: bytes) -> tuple[int, str]:
    """The error number and message of an ERR packet."""
    return struct.unpack_from("<H", pkt, 1)[0], pkt[9:].decode(errors="replace")


class Conn:
    """A MySQL client connection speaking the wire protocol with raw statement bytes."""

    def __init__(self, port: int, collation: int = UTF8MB4):
        self.sock = socket.create_connection((HOST, port), timeout=300)
        self.seq = 0
        self.log_command = "SHOW BINARY LOG STATUS"
        self._handshake(collation)

    def close(self) -> None:
        """Close the socket."""
        self.sock.close()

    def _recv(self, n: int) -> bytes:
        out = bytearray()
        while len(out) < n:
            chunk = self.sock.recv(n - len(out))
            if not chunk:
                raise ConnectionError("server closed the connection")
            out += chunk
        return bytes(out)

    def read(self) -> bytes:
        """One logical packet, joining 16 MiB continuation packets."""
        payload = b""
        while True:
            head = self._recv(4)
            size = int.from_bytes(head[:3], "little")
            self.seq = (head[3] + 1) & 0xFF
            payload += self._recv(size)
            if size < 0xFFFFFF:
                return payload

    def write(self, payload: bytes) -> None:
        """Send one packet with the running sequence number."""
        self.sock.sendall(len(payload).to_bytes(3, "little") + bytes([self.seq]) + payload)
        self.seq = (self.seq + 1) & 0xFF

    def _handshake(self, collation: int) -> None:
        pkt = self.read()
        if pkt[0] == 0xFF:
            raise ServerError(*err_of(pkt))
        at = pkt.index(b"\x00", 1) + 1 + 4 + 8 + 1 + 2 + 1 + 2 + 2
        auth_len = pkt[at]
        at += 1 + 10 + max(13, auth_len - 8)
        plugin = pkt[at:pkt.index(b"\x00", at)] if at < len(pkt) else b"mysql_native_password"
        caps = 0x1 | 0x4 | 0x200 | 0x2000 | 0x8000 | 0x80000 | 0x200000
        body = struct.pack("<IIB", caps, 1 << 24, collation) + b"\x00" * 23 + b"root\x00" + b"\x00"
        self.write(body + plugin + b"\x00")
        while True:
            reply = self.read()
            if reply[0] == 0x00:
                return
            if reply[0] == 0xFF:
                raise ServerError(*err_of(reply))
            if reply[0] == 0xFE:
                self.write(b"")
            elif reply[0] != 0x01:
                raise ConnectionError(f"unexpected handshake reply {reply[:8]!r}")

    def query(self, sql: bytes | str) -> Result:
        """Run one statement given as raw bytes (or UTF-8 text) and return its reply."""
        self.seq = 0
        self.write(b"\x03" + (sql if isinstance(sql, bytes) else sql.encode()))
        first = self.read()
        if first[0] == 0x00:
            return Result()
        if first[0] == 0xFF:
            return Result(*err_of(first))
        ncols, _ = lenenc(first, 0)
        for _ in range(ncols + 1):
            self.read()
        rows = []
        while True:
            row = self.read()
            if row[0] == 0xFE and len(row) < 9:
                return Result(rows=rows)
            if row[0] == 0xFF:
                return Result(*err_of(row))
            at, cells = 0, []
            while at < len(row):
                n, at = lenenc(row, at)
                if n is None:
                    cells.append(None)
                else:
                    cells.append(row[at:at + n])
                    at += n
            rows.append(cells)

    def must(self, sql: bytes | str) -> list:
        """Run a statement that has to succeed and return its rows."""
        res = self.query(sql)
        if res.errno:
            raise ServerError(res.errno, f"{res.msg} <- {sql[:160]!r}")
        return res.rows

    def scalar(self, sql: str) -> str:
        """The first cell of the first row of a query, as text."""
        return self.must(sql)[0][0].decode()

    def position(self) -> tuple[str, int]:
        """The binlog file and offset the server is writing at."""
        res = self.query(self.log_command)
        if res.errno:
            self.log_command = "SHOW MASTER STATUS"
            res = self.query(self.log_command)
        if res.errno or not res.rows:
            raise ServerError(res.errno, res.msg or "no binlog position")
        return res.rows[0][0].decode(), int(res.rows[0][1])

    def binlog_dump(self, file: str, pos: int):
        """Yield the raw events of the binlog from a position to the end of the log."""
        self.must("SET @master_binlog_checksum = @@global.binlog_checksum")
        self.seq = 0
        self.write(b"\x12" + struct.pack("<IHI", pos, 1, SERVER_ID) + file.encode())
        while True:
            pkt = self.read()
            if pkt[0] == 0xFF:
                raise ServerError(*err_of(pkt))
            if pkt[0] == 0xFE and len(pkt) < 9:
                return
            yield pkt[1:]


@dataclass(frozen=True)
class QueryEvent:
    """A QUERY_EVENT of the binlog: where it ends and what the server logged."""

    where: tuple[str, int]
    db: str
    cs: int
    conn: int
    mode: int
    sql: bytes


def status_vars(raw: bytes) -> tuple[int, int, int]:
    """The (charset_client, collation_connection, sql_mode) of a QUERY_EVENT status block."""
    at, mode = 0, 0
    skip = {0: 4, 3: 4, 7: 2, 8: 2}
    while at < len(raw):
        code = raw[at]
        at += 1
        if code == 1:
            mode = struct.unpack_from("<Q", raw, at)[0]
            at += 8
        elif code == 4:
            cs, cn, _ = struct.unpack_from("<HHH", raw, at)
            return cs, cn, mode
        elif code in skip:
            at += skip[code]
        elif code == 2:
            at += 1 + raw[at] + 1
        elif code in (5, 6):
            at += 1 + raw[at]
        else:
            break
    return 0, 0, mode


def query_events(conn: Conn, file: str, pos: int, checksum: bool):
    """Every QUERY_EVENT from a binlog position to the end of the log."""
    current = file
    for ev in conn.binlog_dump(file, pos):
        kind = ev[4]
        end = struct.unpack_from("<I", ev, 13)[0]
        body = ev[19:len(ev) - 4] if checksum else ev[19:]
        if kind == 4:
            current = body[8:].decode()
        elif kind == 2:
            db_len = body[8]
            vars_len = struct.unpack_from("<H", body, 11)[0]
            at = 13 + vars_len
            cs, cn, mode = status_vars(body[13:at])
            yield QueryEvent((current, end), body[at:at + db_len].decode(), cs, cn, mode, body[at + db_len + 1:])


def compose_exec(server: str, args: list[str], stdin: bytes) -> subprocess.CompletedProcess:
    """Run a command inside the stand container of a server."""
    env = {**os.environ, "COMPOSE_PROJECT_NAME": os.environ.get("COMPOSE_PROJECT_NAME", "rivet")}
    cmd = ["docker", "compose", "--profile", PROFILE, "exec", "-T", SERVERS[server].service, *args]
    return subprocess.run(cmd, cwd=ROOT, env=env, input=stdin, capture_output=True, check=False)


def reseed(server: str) -> None:
    """Re-apply every SQL seed file, in order, through the server's own mysql client."""
    for path in sorted(SEED_DIR.glob("*.sql")):
        res = compose_exec(server, ["mysql", "-uroot", "--default-character-set=utf8mb4"], path.read_bytes())
        if res.returncode:
            raise RuntimeError(f"{server}: seed {path.name} failed: {res.stderr.decode(errors='replace')[:400]}")


TABLE_LINE = re.compile(r"^(?:DROP TABLE IF EXISTS|CREATE TABLE|INSERT INTO) `([^`]+)`\.`([^`]+)`")


def seed_tables() -> dict[tuple[str, str], list[str]]:
    """The seed statements that rebuild each table, read back from the SQL files."""
    out: dict[tuple[str, str], list[str]] = defaultdict(list)
    for path in sorted(SEED_DIR.glob("*.sql")):
        for line in path.read_text(encoding="utf-8").splitlines():
            m = TABLE_LINE.match(line)
            if m and m.group(1) != "st_meta" and not line.startswith("DROP"):
                out[(m.group(1), m.group(2))].append(line)
    return dict(out)


def seed_accounts() -> list[str]:
    """The seed statements that rebuild the fixed accounts."""
    return [l for l in (SEED_DIR / "99_account.sql").read_text(encoding="utf-8").splitlines() if l.strip()]


def q(name: str) -> str:
    """A name in backticks."""
    return "`" + name.replace("`", "``") + "`"


def lit(text: str) -> str:
    """A string literal."""
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


Table = tuple[tuple, str, int | None]
Snap = dict[str, dict[str, Table]]


def snapshot(adm: Conn, dbs) -> Snap:
    """Per database and table: column names, a fingerprint of the definition and the CHECKSUM TABLE of its rows."""
    names = ",".join(lit(d) for d in dbs)
    cols: dict[tuple[str, str], list[str]] = defaultdict(list)
    fingerprint: dict[tuple[str, str], list[str]] = defaultdict(list)
    for r in adm.must(
        "SELECT table_schema, table_name, column_name, column_type, is_nullable, IFNULL(column_default, '~'), "
        f"column_comment FROM information_schema.columns WHERE table_schema IN ({names}) "
        "ORDER BY table_schema, table_name, ordinal_position"
    ):
        key = (r[0].decode(), r[1].decode())
        cols[key].append(r[2].decode())
        fingerprint[key].append("|".join(c.decode() for c in r[2:]))
    for r in adm.must(
        "SELECT table_schema, table_name, index_name, seq_in_index, column_name FROM information_schema.statistics "
        f"WHERE table_schema IN ({names}) ORDER BY table_schema, table_name, index_name, seq_in_index"
    ):
        fingerprint[(r[0].decode(), r[1].decode())].append("ix:" + "|".join(c.decode() for c in r[2:]))
    for r in adm.must(
        "SELECT table_schema, table_name, table_comment, IFNULL(create_options, '') FROM information_schema.tables "
        f"WHERE table_schema IN ({names})"
    ):
        fingerprint[(r[0].decode(), r[1].decode())].append("tb:" + "|".join(c.decode() for c in r[2:]))
    out: Snap = {db: {} for db in dbs}
    for db in dbs:
        tables = sorted(t for (d, t) in cols if d == db)
        sums: dict[str, int | None] = {}
        if tables:
            for name, total in adm.must("CHECKSUM TABLE " + ", ".join(f"{q(db)}.{q(t)}" for t in tables)):
                sums[name.decode().split(".", 1)[1]] = None if total is None else int(total)
        for t in tables:
            out[db][t] = (tuple(cols[(db, t)]), "\n".join(fingerprint[(db, t)]), sums.get(t))
    return out


@dataclass(frozen=True)
class Delta:
    """How a snapshot differs from the seed: tables gone and new, renamed columns, changed rows, emptied tables."""

    gone: tuple = ()
    new: tuple = ()
    columns: tuple = ()
    data: tuple = ()
    emptied: tuple = ()


def delta(base: Snap, now: Snap) -> Delta:
    """The difference between the seed snapshot and a later one."""
    gone, new, columns, data, emptied = [], [], [], [], []
    for db in now:
        b, a = base[db], now[db]
        gone += [(db, t) for t in sorted(set(b) - set(a))]
        new += [(db, t) for t in sorted(set(a) - set(b))]
        for t in sorted(set(a) & set(b)):
            if b[t][0] != a[t][0] and len(b[t][0]) == len(a[t][0]) and set(b[t][0]) != set(a[t][0]):
                columns.append((db, t))
            if b[t][2] != a[t][2]:
                data.append((db, t))
                if a[t][2] == 0:
                    emptied.append((db, t))
    return Delta(tuple(gone), tuple(new), tuple(columns), tuple(data), tuple(emptied))


def classify(d: Delta, com: dict[str, int], errno: int) -> tuple[str, list[str]]:
    """What the server did: the kind of change and the tables it touched, as `db.table`."""
    full = lambda items: sorted(f"{db}.{t}" for db, t in items)
    ok = errno == 0
    if ok and any(com[v] for v in PRIV_VARS):
        return "privilege", []
    if d.gone and d.new:
        return "rename_table", full(d.gone + d.new)
    if d.gone:
        return "drop", full(d.gone)
    if ok and com["Com_rename_table"] and d.data:
        return "rename_table", full(d.data)
    if d.columns:
        return "rename_column", full(d.columns)
    if ok and com["Com_truncate"] and d.emptied:
        return "truncate", full(d.emptied)
    if d.data and any(com[v] for v in DML_VARS):
        return "statement_write", full(d.data)
    return "none", []


def expectation(kind: str, touched: list[str], cap: set) -> str:
    """The verdict rivet owes: the change kind when it touched a captured table, else none."""
    if kind == "privilege":
        return kind
    if kind in REFUSING and any(tuple(t.split(".", 1)) in cap for t in touched):
        return kind
    return "none"


@dataclass
class Case:
    """One statement to send: its bytes, the session it is sent in and the databases it may touch."""

    id: str
    tag: str
    coll: int
    mode: str
    db: str
    sql: bytes
    dbs: tuple[str, ...]


def encode(text: str, charset: str) -> bytes | None:
    """The text in a client charset, None when the charset cannot hold it."""
    codec = CODECS.get(charset)
    if codec is None:
        return text.encode("ascii") if text.isascii() else None
    try:
        return text.encode(codec)
    except UnicodeEncodeError:
        return None


@dataclass
class Family:
    """A seeded database with a captured and an uncaptured table of native names."""

    db: str
    captured: str
    other: str
    column: str


def families(base: Snap, cap: set) -> list[Family]:
    """The seeded databases that hold native-named tables, in name order."""
    out = []
    for db in sorted(base):
        native = sorted(t for t in base[db] if not t.isascii())
        if native:
            first = next(t for t in native if (db, t) in cap)
            out.append(Family(db, first, next(t for t in native if (db, t) not in cap), base[db][first][0][1]))
    return out


SPELLINGS = {
    "bare": "{t}", "tick": "`{t}`", "dot": "{d}.{t}", "tickdot": "`{d}`.`{t}`", "ansi": '"{t}"',
    "ansidot": '"{d}"."{t}"', "spaced": "`{d}` . `{t}`", "cmt": "{d}./**/{t}", "upper": "{D}.{T}",
    "mixed": "`{D}`.`{T}`",
}

POSITIONS = {
    "direct": "{s}",
    "lead_block": "/* lead */ {s}",
    "lead_dash": "-- lead\n{s}",
    "lead_hash": "# lead\n{s}",
    "lead_nonascii": "/* ünï 日本 */ {s}",
    "lead_quotes": "/* it's \"q\" \\ */ {s}",
    "ver5": "/*!50000 {s} */",
    "ver6": "/*!080000 {s} */",
    "ver_future": "/*!999999 {s} */",
    "ver_bare": "/*! {s} */",
    "ver_maria": "/*M! {s} */",
    "trail_block": "{s} /* tail */",
    "trail_dash": "{s} -- tail",
    "trail_semi": "{s};",
    "space_block": "{w}/**/{rest}",
    "space_dash": "{w} -- gap\n{rest}",
    "ws_lead": "\n\t {s}",
    "ver_part": "/*!50000 {w} */ {rest}",
}

OPS = {
    "trunc_table": "TRUNCATE TABLE {T}",
    "trunc": "TRUNCATE {T}",
    "drop": "DROP TABLE {T}",
    "drop_if": "DROP TABLE IF EXISTS {T}",
    "drop_two": "DROP TABLE {U}, {T}",
    "drop_missing_first": "DROP TABLE IF EXISTS st_zz_missing, {T}",
    "drop_tables": "DROP TABLES {T}",
    "rename": "RENAME TABLE {T} TO {A}",
    "rename_two": "RENAME TABLE {U} TO {B}, {T} TO {A}",
    "rename_swap": "RENAME TABLE {U} TO {B}, {T} TO {U}, {B} TO {T}",
    "alter_rename_to": "ALTER TABLE {T} RENAME TO {A}",
    "alter_rename": "ALTER TABLE {T} RENAME {A}",
    "alter_rename_as": "ALTER TABLE {T} RENAME AS {A}",
    "alter_add_rename": "ALTER TABLE {T} ADD COLUMN extra INT, RENAME TO {A}",
    "alter_rename_add": "ALTER TABLE {T} RENAME TO {A}, ADD COLUMN extra INT",
    "col_rename": "ALTER TABLE {T} RENAME COLUMN amount TO amount2",
    "col_change": "ALTER TABLE {T} CHANGE amount amount2 INT NOT NULL",
    "col_change_column": "ALTER TABLE {T} CHANGE COLUMN amount amount2 INT NOT NULL",
    "col_change_same": "ALTER TABLE {T} CHANGE amount amount INT NOT NULL",
    "col_change_case": "ALTER TABLE {T} CHANGE amount AMOUNT INT NOT NULL",
    "col_modify": "ALTER TABLE {T} MODIFY amount BIGINT NOT NULL",
    "col_add": "ALTER TABLE {T} ADD COLUMN extra INT",
    "col_default": "ALTER TABLE {T} ALTER COLUMN amount SET DEFAULT 1",
    "grant": "GRANT SELECT ON {T} TO st_tmp",
    "grant_col": "GRANT SELECT (id) ON {T} TO st_tmp",
    "revoke": "REVOKE SELECT (amount) ON {T} FROM st_acct",
    "dml_insert": "INSERT INTO {T} VALUES (100, 1, 'x')",
    "dml_update": "UPDATE {T} SET amount = amount + 1 WHERE id = 1",
    "dml_delete": "DELETE FROM {T} WHERE id = 1",
    "dml_replace": "REPLACE INTO {T} VALUES (1, 5, 'x')",
    "dml_select_into": "INSERT INTO {T} SELECT id + 100, amount, note FROM {T}",
    "dml_delete_all": "DELETE FROM {T}",
    "dml_read_captured": "INSERT INTO {U} SELECT id + 100, amount, note FROM {T}",
    "dml_noop": "UPDATE {T} SET amount = amount WHERE 1 = 0",
    "like": "CREATE TABLE {T}_copy LIKE {T}",
    "index": "CREATE INDEX ix_amount ON {T} (amount)",
    "analyze": "ANALYZE TABLE {T}",
    "optimize": "OPTIMIZE TABLE {T}",
    "lit_trunc": "UPDATE {U} SET note = 'TRUNCATE TABLE orders' WHERE id = 1",
    "lit_rename": "ALTER TABLE {U} COMMENT = 'RENAME TABLE orders TO x'",
    "lit_drop": "ALTER TABLE {U} COMMENT = 'DROP TABLE orders'",
    "lit_ansi": 'ALTER TABLE {U} COMMENT = "TRUNCATE TABLE orders"',
    "hazard_bs": "ALTER TABLE {T} COMMENT = 'a\\' , RENAME TO {A} , COMMENT = \\'b'",
    "hazard_bs2": "ALTER TABLE {T} COMMENT = 'a\\\\', RENAME TO {A}, COMMENT = 'b'",
    "hazard_dq": 'ALTER TABLE {T} COMMENT = "a", RENAME TO {A}, COMMENT = "b"',
    "hazard_quote": "ALTER TABLE {T} COMMENT = 'a''', RENAME TO {A}, COMMENT = 'b'",
    "hazard_nbe": "ALTER TABLE {T} COMMENT = 'a\\', RENAME TO {A}",
    "hazard_ansi_esc": 'ALTER TABLE {T} ADD COLUMN "a\\" INT, RENAME TO {A}',
    "hazard_dq_str": 'ALTER TABLE {T} COMMENT = "x\\" , RENAME TO {A} , COMMENT = \\"y"',
}

CORE_OPS = (
    "trunc_table", "trunc", "drop", "drop_two", "rename", "rename_swap", "alter_rename_to", "alter_add_rename",
    "col_rename", "col_change", "col_change_same", "grant", "dml_update", "dml_noop", "col_add", "lit_trunc",
)

ACCOUNT_OPS = {
    "user_create": "CREATE USER st_new IDENTIFIED BY 'x'",
    "user_alter": "ALTER USER st_tmp IDENTIFIED BY 'y'",
    "user_rename": "RENAME USER st_tmp TO st_tmp2",
    "user_drop": "DROP USER st_tmp",
    "role_default": "SET DEFAULT ROLE NONE TO st_tmp",
    "flush_priv": "FLUSH PRIVILEGES",
    "grant_all": "GRANT ALL ON *.* TO st_tmp",
    "revoke_all": "REVOKE ALL PRIVILEGES, GRANT OPTION FROM st_acct",
    "grant_lower": "grant select on st_ascii.orders to st_tmp",
    "revoke_lower": "revoke select (id) on st_ascii.orders from st_acct",
}

SWEEP_BYTES = [*range(0x00, 0x20), 0x7F, *range(0x80, 0x100)]

SWEEP_TEMPLATES = {
    "comment": (b"TRUNCATE TABLE orders /*", b"*/"),
    "after_name": (b"TRUNCATE TABLE orders", b""),
    "between": (b"TRUNCATE", b"TABLE orders"),
    "in_literal": (b"UPDATE audit_log SET note = '", b"' WHERE id = 1"),
    "pair_backslash": (b"ALTER TABLE orders COMMENT = '", b"\\', RENAME TO st_zz"),
}


def spell(style: str, db: str, table: str) -> str:
    """A table named in one of the spellings a statement can use."""
    return SPELLINGS[style].format(d=db, t=table, D=db.upper(), T=table.capitalize())


def fill(op: str, style: str, db: str, tbl: str, other: str) -> str:
    """An operation with its table placeholders replaced by one spelling."""
    return OPS[op].format(
        T=spell(style, db, tbl), U=spell(style, db, other), A=spell(style, db, "st_zz"), B=spell(style, db, "st_zy")
    )


def place(position: str, stmt: str) -> str:
    """A statement put into one of the comment positions."""
    word, _, rest = stmt.partition(" ")
    return POSITIONS[position].format(s=stmt, w=word, rest=rest)


def cases_matrix() -> list[Case]:
    """Operation x comment position x sql_mode x spelling x target class, in utf8mb4."""
    out: list[Case] = []

    def add(cid: str, tag: str, mode: str, db: str, text: str, dbs: tuple[str, ...]) -> None:
        out.append(Case(cid, tag, UTF8MB4, mode, db, text.encode(), dbs))

    for mode in MODES:
        for op in OPS:
            add(f"C.op.{mode}.{op}", f"op.{op}", mode, ASCII_DB, fill(op, "tick", ASCII_DB, "orders", "audit_log"), (ASCII_DB,))
        for pos, op in itertools.product(POSITIONS, CORE_OPS):
            text = place(pos, fill(op, "tick", ASCII_DB, "orders", "audit_log"))
            add(f"C.pos.{mode}.{pos}.{op}", f"pos.{op}", mode, ASCII_DB, text, (ASCII_DB,))
        for style, op in itertools.product(SPELLINGS, CORE_OPS):
            text = fill(op, style, ASCII_DB, "orders", "audit_log")
            add(f"C.spell.{mode}.{style}.{op}", f"spell.{op}", mode, ASCII_DB, text, (ASCII_DB,))
        for op, text in ACCOUNT_OPS.items():
            add(f"C.acct.{mode}.{op}", f"acct.{op}", mode, ASCII_DB, text, (ASCII_DB,))
        for op in OPS:
            text = fill(op, "tickdot", ASCII_DB, "audit_log", "orders")
            add(f"C.uncaptured.{mode}.{op}", f"uncaptured.{op}", mode, ASCII_DB, text, (ASCII_DB,))
            text = fill(op, "tickdot", ASCII_DB, "orders", "audit_log")
            add(f"C.otherdb.{mode}.{op}", f"otherdb.{op}", mode, PROBE_DB, text, (ASCII_DB, PROBE_DB))
            text = fill(op, "bare", ASCII_DB, "orders", "audit_log")
            add(f"C.otherdb_bare.{mode}.{op}", f"otherdb_bare.{op}", mode, PROBE_DB, text, (ASCII_DB, PROBE_DB))
    return out


def cases_collations(colls: list[tuple[int, str, str]], fams: list[Family]) -> list[Case]:
    """For every client collation id: ASCII, native-name and native-literal statements in that charset."""
    out: list[Case] = []
    for coll, charset, _ in colls:
        usable = [f for f in fams if encode(f.captured + f.other + f.column + f.db, charset) is not None]
        target = usable[coll % len(usable)] if usable else None

        def add(tag: str, text: str, db: str) -> None:
            raw = encode(text, charset)
            if raw is not None:
                out.append(Case(f"A.{coll}.{tag}", f"coll.{tag}", coll, "default", db, raw, (db,)))

        add("trunc_ascii", "TRUNCATE TABLE `st_ascii`.`orders`", ASCII_DB)
        add("trunc_ascii_comment", "TRUNCATE TABLE orders /* éüñ */", ASCII_DB)
        add("drop_ascii", "DROP TABLE `st_ascii`.`orders`", ASCII_DB)
        add("rename_ascii", "RENAME TABLE `st_ascii`.`orders` TO `st_ascii`.`st_zz`", ASCII_DB)
        add("col_ascii", "ALTER TABLE `st_ascii`.`orders` RENAME COLUMN `amount` TO `amount2`", ASCII_DB)
        add("grant_ascii", "GRANT SELECT ON `st_ascii`.`orders` TO st_tmp", ASCII_DB)
        add("dml_ascii", "UPDATE `st_ascii`.`orders` SET `amount` = `amount` + 1 WHERE `id` = 1", ASCII_DB)
        if target is None:
            continue
        d, c, o, col = target.db, q(target.captured), q(target.other), q(target.column)
        text = f"{c}_{target.column}"
        add("trunc_native", f"TRUNCATE TABLE {q(d)}.{c}", d)
        add("trunc_native_bare", f"TRUNCATE TABLE {c}", d)
        add("drop_native", f"DROP TABLE {q(d)}.{c}", d)
        add("rename_native", f"RENAME TABLE {q(d)}.{c} TO {q(d)}.{q(target.captured + '_r')}", d)
        add("alter_rename_native", f"ALTER TABLE {q(d)}.{c} RENAME TO {q(d)}.{q(target.captured + '_r')}", d)
        add("col_native", f"ALTER TABLE {q(d)}.{c} RENAME COLUMN {col} TO {q(target.column + '2')}", d)
        add("col_change_native", f"ALTER TABLE {q(d)}.{c} CHANGE {col} {q(target.column + '2')} INT NOT NULL", d)
        add("grant_native", f"GRANT SELECT ON {q(d)}.{c} TO st_tmp", d)
        add("grant_col_native", f"GRANT SELECT ({col}) ON {q(d)}.{c} TO st_tmp", d)
        add("dml_native", f"UPDATE {q(d)}.{c} SET {col} = {col} + 1 WHERE `id` = 1", d)
        add("trunc_other_native", f"TRUNCATE TABLE {q(d)}.{o}", d)
        add("drop_other_native", f"DROP TABLE {q(d)}.{o}", d)
        add("trunc_comment_native", f"TRUNCATE TABLE {q(d)}.{c} /* {text} */", d)
        add("dml_literal_native", f"INSERT INTO `st_ascii`.`audit_log` VALUES (100, 1, {lit(text)})", ASCII_DB)
        add("ddl_literal_native", f"ALTER TABLE `st_ascii`.`audit_log` COMMENT = {lit(text)}", ASCII_DB)
    return out


def cases_sweep(defaults: list[tuple[int, str, str]]) -> list[Case]:
    """Every control byte and every byte 0x80-0xFF in each position of a statement, once per client charset."""
    return [
        Case(f"B.{coll}.{tag}.{b:02x}", f"sweep.{tag}", coll, "default", ASCII_DB, head + bytes([b]) + tail, (ASCII_DB,))
        for coll, _, _ in defaults
        for tag, (head, tail) in SWEEP_TEMPLATES.items()
        for b in SWEEP_BYTES
    ]


class Session:
    """The case connection: changes charset, sql_mode and database only when a case needs another."""

    def __init__(self, port: int, names: dict[int, tuple[str, str]]):
        self.port = port
        self.names = names
        self.conn: Conn | None = None
        self.key: tuple | None = None
        self.refused: set[int] = set()
        self.last: dict[str, int] = {}
        self.charset = "utf8mb4"

    def open(self) -> Conn:
        """A fresh case connection logging statements, not rows."""
        if self.conn is not None:
            self.conn.close()
        self.conn = Conn(self.port)
        self.conn.must("SET SESSION binlog_format = 'STATEMENT'")
        self.key = None
        self.charset = "utf8mb4"
        self.last = com_status(self.conn)
        return self.conn

    def ready(self, case: Case) -> Conn | None:
        """The connection set up for a case, None when the server refuses its client charset."""
        if case.coll in self.refused:
            return None
        conn = self.conn or self.open()
        want = (case.coll, case.mode, case.db)
        if self.key == want:
            return conn
        old = self.key or (None, None, None)
        if old[1] != case.mode:
            conn.must(f"SET SESSION sql_mode = CONCAT(@@global.sql_mode, '{MODES[case.mode]}')")
        if old[2] != case.db or old[0] != case.coll:
            charset, collation = self.names[case.coll]
            conn.must("SET NAMES utf8mb4")
            conn.must(f"USE {q(case.db)}")
            if conn.query(f"SET NAMES {charset} COLLATE {collation}").errno:
                self.refused.add(case.coll)
                self.key = None
                conn.must("SET NAMES utf8mb4")
                self.charset = "utf8mb4"
                return None
            self.charset = charset
        self.key = want
        return conn

    def spent(self) -> dict[str, int]:
        """The statement counters the last case moved."""
        if self.charset == "swe7":
            self.conn.must("SET character_set_client = utf8mb4")
        now = com_status(self.conn)
        if self.charset == "swe7":
            self.conn.must("SET character_set_client = swe7")
        out = {k: now[k] - self.last[k] for k in now}
        self.last = now
        return out


def com_status(conn: Conn) -> dict[str, int]:
    """The statement counters of the session that the classification reads."""
    names = ",".join(lit(v) for v in COM_VARS)
    return {r[0].decode(): int(r[1]) for r in conn.must(f"SHOW SESSION STATUS WHERE Variable_name IN ({names})")}


def restore(adm: Conn, base: Snap, now: Snap, tables: dict) -> None:
    """Rebuild every table that differs from the seed, then check the databases match it again."""
    for db, tbls in now.items():
        for t in sorted(set(tbls) - set(base[db])):
            adm.must(f"DROP TABLE IF EXISTS {q(db)}.{q(t)}")
        for t in sorted(base[db]):
            if tbls.get(t) != base[db][t]:
                adm.must(f"DROP TABLE IF EXISTS {q(db)}.{q(t)}")
                for line in tables[(db, t)]:
                    adm.must(line)
    again = snapshot(adm, list(now))
    if again != {db: base[db] for db in now}:
        raise RuntimeError(f"restore left {sorted(db for db in now if again[db] != base[db])} different from the seed")


@dataclass
class Outcome:
    """What the server did for one case: its reply, its log range and the effect it had."""

    case: Case
    errno: int
    start: tuple[str, int]
    end: tuple[str, int]
    delta: Delta
    com: dict[str, int]


def run_cases(server: str, cases: list[Case], adm: Conn, base: Snap, tables: dict, accounts: list[str], names: dict):
    """Send every case in order, restoring the seed whenever the server changed it."""
    sess = Session(SERVERS[server].port, names)
    outcomes: list[Outcome] = []
    start = adm.position()
    for case in cases:
        try:
            conn = sess.ready(case)
        except (ConnectionError, OSError):
            sess.open()
            conn = sess.ready(case)
        if conn is None:
            continue
        try:
            res = conn.query(case.sql)
            com = sess.spent()
        except (ConnectionError, OSError):
            sess.open()
            res, com = Result(2013, "connection lost"), {k: 0 for k in COM_VARS}
        end = adm.position()
        if res.errno == 0 or com["Com_drop_table"]:
            snap = snapshot(adm, case.dbs)
            eff = delta(base, snap)
        else:
            snap, eff = None, Delta()
        outcomes.append(Outcome(case, res.errno, start, end, eff, com))
        start = end
        dirty = False
        if snap is not None and any(snap[db] != base[db] for db in snap):
            restore(adm, base, snap, tables)
            dirty = True
        if any(com[v] for v in PRIV_VARS) or case.tag.startswith("acct"):
            for line in accounts:
                adm.must(line)
            dirty = True
        if dirty:
            start = adm.position()
    return outcomes, sorted(sess.refused)


def events_by_case(outcomes: list[Outcome], events: list[QueryEvent]) -> list[list[QueryEvent]]:
    """The logged statements of each case, found by the log range it was sent in."""
    starts = [o.start for o in outcomes]
    out: list[list[QueryEvent]] = [[] for _ in outcomes]
    for ev in events:
        i = bisect.bisect_left(starts, ev.where) - 1
        if 0 <= i and ev.where <= outcomes[i].end and ev.sql.strip().upper() != b"BEGIN":
            out[i].append(ev)
    return out


PASSWORD_HASH = re.compile(rb"( AS ').*('\s*)$", re.S)


def stable(sql: bytes) -> bytes:
    """The statement with a logged password hash replaced by a fixed marker; every hash carries a random salt."""
    return PASSWORD_HASH.sub(rb"\1<hash>\2", sql) if b" IDENTIFIED WITH " in sql else sql


def rows_from(outcomes: list[Outcome], logged: list[list[QueryEvent]], cap: set) -> list[dict]:
    """One corpus row per logged statement, or one per sent statement when the server logged none."""
    session: dict[tuple[int, str], tuple[int, int, int]] = {}
    for out, evs in zip(outcomes, logged):
        if out.case.tag == "cal" and evs:
            session[(out.case.coll, out.case.mode)] = (evs[0].cs, evs[0].conn, evs[0].mode)
    rows = []
    for out, evs in zip(outcomes, logged):
        if out.case.tag == "cal":
            continue
        kind, touched = classify(out.delta, out.com, out.errno)
        expect = expectation(kind, touched, cap)
        common = {"engine": ENGINE, "case": out.case.tag, "errno": out.errno, "kind": kind, "touched": touched}
        if evs:
            for j, e in enumerate(evs):
                rows.append({**common, "id": out.case.id if j == 0 else f"{out.case.id}#{j}", "src": "event", "cs": e.cs,
                             "conn": e.conn, "mode": e.mode, "db": e.db, "sql": stable(e.sql).hex(), "expect": expect})
        else:
            cs, conn, mode = session.get((out.case.coll, out.case.mode), (0, 0, 0))
            rows.append({**common, "id": out.case.id, "src": "sent", "cs": cs, "conn": conn, "mode": mode,
                         "db": out.case.db, "sql": out.case.sql.hex(), "expect": "none" if out.errno else expect})
    return rows


def generate(server: str, subset: str | None = None) -> tuple[dict, list[dict]]:
    """Regenerate the corpus of one server: its header and rows."""
    cfg = SERVERS[server]
    reseed(server)
    adm = Conn(cfg.port)
    version = adm.scalar("SELECT @@version")
    lctn = int(adm.scalar("SELECT @@lower_case_table_names"))
    if lctn != cfg.lctn:
        raise RuntimeError(f"{server}: lower_case_table_names is {lctn}, the stand says {cfg.lctn}")
    checksum = adm.scalar("SELECT @@global.binlog_checksum") != "NONE"
    meta = adm.must("SELECT db, tbl, captured FROM st_meta.targets ORDER BY db, tbl")
    cap = {(r[0].decode(), r[1].decode()) for r in meta if r[2] == b"1"}
    dbs = sorted({r[0].decode() for r in meta} | {PROBE_DB})
    base = snapshot(adm, dbs)
    if any(s in (None, 0) for tbls in base.values() for _, _, s in tbls.values()):
        raise RuntimeError(f"{server}: a seeded table is empty after the seed ran")
    colls = [(int(r[0]), r[1].decode(), r[2].decode(), r[3] == b"Yes") for r in adm.must(
        "SELECT id, character_set_name, collation_name, is_default FROM information_schema.collations ORDER BY id")]
    names = {cid: (cs, name) for cid, cs, name, _ in colls}
    pairs = [(cid, cs, name) for cid, cs, name, _ in colls]
    defaults = [(cid, cs, name) for cid, cs, name, d in colls if d]
    which = subset or "ABC"
    cases: list[Case] = []
    if "A" in which:
        cases += cases_collations(pairs, families(base, cap))
    if "B" in which:
        cases += cases_sweep(defaults)
    if "C" in which:
        cases += cases_matrix()
    if len({c.id for c in cases}) != len(cases):
        raise RuntimeError("case ids must be unique")
    groups: dict[tuple[int, str], list[Case]] = defaultdict(list)
    for c in cases:
        groups[(c.coll, c.mode)].append(c)
    ordered: list[Case] = []
    for (coll, mode), group in sorted(groups.items()):
        group.sort(key=lambda c: (c.db, c.id))
        calibration = Case(f"cal.{coll}.{mode}", "cal", coll, mode, group[0].db,
                           f"ALTER TABLE {PROBE_DB}.p COMMENT = ''".encode(), (PROBE_DB,))
        ordered += [calibration, *group]
    start = adm.position()
    outcomes, refused = run_cases(server, ordered, adm, base, seed_tables(), seed_accounts(), names)
    events = list(query_events(Conn(cfg.port), start[0], start[1], checksum))
    rows = rows_from(outcomes, events_by_case(outcomes, events), cap)
    adm.close()
    header = {
        "engine": ENGINE, "schema": SCHEMA, "server": server, "version": version, "lctn": lctn, "fold": lctn != 0,
        "captured": sorted(f"{d}.{t}" for d, t in cap), "collations": len(colls), "refused": refused,
        "subset": which, "rows": len(rows), "stand": len(SERVERS),
    }
    rows.sort(key=lambda r: r["id"])
    return header, rows
