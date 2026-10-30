# Statement truth

A statement reader is graded against what the server did with the same bytes, not against what a
test author thought the server does. Each engine has a stand (servers that run the statements), a
generator (sends them and records what changed) and a fixture (the recording, committed). An offline
Rust test replays every recorded statement through the reader and compares the two verdicts.

MySQL is the first engine: the binlog reader's QUERY-event verdict (`src/source/mysql/cdc.rs`).
Another engine adds a `dev/pytools/statement_truth_<engine>.py` module, a fixture directory and an
adapter next to its reader; `dev/pytools/statement_truth.py` and `src/source/statement_truth.rs` do not change.

## The stand

```sh
make statement-truth-up      # starts the three servers, applies the SQL seed, waits until healthy
make statement-truth-down    # removes those three containers only
```

| service | server | `lower_case_table_names` | port |
| --- | --- | --- | --- |
| `mysql-stmt-80` | MySQL 8.0 | 0 | 3331 |
| `mysql-stmt-84` | MySQL 8.4 | 0 | 3332 |
| `mysql-stmt-84-lctn1` | MySQL 8.4 | 1 | 3333 |

Profile `mysql-stmt`; data on tmpfs, a 48 MB buffer pool, binlog `ROW`, `root` with no password on
127.0.0.1. The seed is plain SQL under `dev/stmt_truth/mysql/seed/`, run by the image's init scripts
(`mysql < file.sql` re-applies it: `python3 dev/pytools/statement_truth.py mysql seed`). It holds
one database per client charset family, each with a captured table, an uncaptured table and tables
named in that charset, plus two databases whose own names are not ASCII, and the accounts the
privilege cases use. The rows come from a recursive CTE over the row number. The seed files carry
non-ASCII identifiers on purpose.

## Regenerating and checking

```sh
make statement-truth-generate   # rewrites dev/fixtures/statement_truth/mysql/<server>.jsonl.gz
make statement-truth-check      # regenerates and diffs; exit 1 on any difference
```

`generate` and `check` take `--server NAME` (repeatable) to run one server. `--subset A|B|C` (any
combination) runs a slice of the corpus and prints its size and speed without touching a fixture;
`--dump PATH` writes the slice for inspection. `check` prints the removed, added and changed rows;
a server patch release that changes only its version string is reported, not failed.

A fixture holds about 36,000 rows (300 KB gzipped) per server. The stand is up in about ten seconds
with the images present; the three servers regenerate side by side in 9 to 14 minutes, which `check`
repeats. The password hash a `CREATE USER` or `ALTER USER` event logs is replaced by `<hash>` because it
carries a random salt; nothing else in a regeneration varies.

What is sent, per server:

- A: every collation id the server lists as a client charset (the ones it refuses are recorded in
  the header), against ASCII and native-named tables.
- B: every control byte and every byte 0x80-0xFF in five positions of a statement, once per client
  charset.
- C: every operation (TRUNCATE, DROP, RENAME TABLE, ALTER RENAME TABLE/COLUMN, CHANGE, GRANT,
  REVOKE, FLUSH PRIVILEGES, benign DDL and DML on captured and uncaptured tables, with non-ASCII
  names, literals and comments) at 18 positions (direct, after `-- `, `#`, inside `/* */`, in
  version comments of 5 and 6 digits, spelled with quotes and qualifiers), under `ANSI_QUOTES` and
  `NO_BACKSLASH_ESCAPES` on and off.

The verdict of a row is read off the server: the `Com_*` counters of the statement, the table
fingerprints before and after, and the bytes the binlog logged. A row is
`{engine, id, case, errno, kind, touched, src, cs, conn, mode, db, sql (hex), expect}`; `src` says
whether `sql` is the logged event or, when the server logged nothing, the bytes sent.

## The replay test

`src/source/statement_truth.rs` loads every fixture of an engine, replays each row through the adapter's
reader and sorts each disagreement: `missed` (the server changed what the reader captures and the
reader passed it), `over` (the reader refused a statement that touched nothing it captures, or
that failed) and `wrong` (both refused, differently). An adapter is a function from a row to a
class; the MySQL one is `src/source/mysql/statement_truth.rs`.

Every disagreement must be in the adapter's ledger with a ceiling and a dated reason; an entry that
no longer covers a row fails the test, so the ledger shrinks when the reader is fixed. A missing
fixture directory, a fixture that lost rows and a stand of another size fail the test; none skips.
