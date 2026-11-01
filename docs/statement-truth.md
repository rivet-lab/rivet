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

`generate` and `check` take `--server NAME` (repeatable) to run one server. `--subset A|B|C|D|E` (any
combination) runs a slice of the corpus and prints its size and speed without touching a fixture;
`--dump PATH` writes the slice for inspection. `check` prints the removed, added and changed rows;
a server patch release that changes only its version string is reported, not failed.

A fixture holds 38,733 rows (322 KB gzipped) per server. The stand is up in about ten seconds
with the images present; the three servers regenerate side by side in about 18 minutes, which `check`
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
- D: gadgets from reviews of the reader. A control byte after `--` inside a multi-table `DELETE` or
  `UPDATE` (statement format) and inside mixed `CHANGE` / `RENAME COLUMN` specs that hide a real
  rename behind a `/*` in a line comment, in utf8mb4 and latin1, under three sql_modes. A blank byte
  (probed on the server per client charset, `SELECT<byte>1000`) after `--` and between `CHANGE`
  and the column, for every collation. Tables whose names only differ from a captured one by a
  non-ASCII suffix (`client` captured, `client` plus an accented letter not), renamed, altered,
  dropped, truncated and written, in utf8mb4, latin1, cp1250 and cp1251.
  Fold near-names: under `lower_case_table_names=1` the server lowers a table name through the utf8mb3
  general_ci table, so a character outside ASCII can resolve to a captured table (`U+212A` KELVIN SIGN
  to `k`, `U+0130` to `i`). The set is read off the server at generation (every BMP code point whose
  `LOWER(CONVERT(CHAR(i USING ucs2) USING utf8mb3))` is one of `[a-z0-9_]`) and recorded in the header
  as `folds`; seed `26_st_fold.sql` holds the captured `st_fold.kinds` and an uncaptured `st_fold.tmp`.
  For every client charset that can encode the character, each fold spelling of `kinds` is sent as
  TRUNCATE, DROP, RENAME TABLE, ALTER RENAME, RENAME COLUMN, CHANGE, a column swap, a swap-in of
  `tmp` over the captured name, and INSERT, UPDATE and DELETE in statement format; a write after a
  swap-in is sent on the same connection as the `RENAME TABLE kinds TO kinds_old, tmp TO kinds` that
  precedes it. The row says which table the server touched, so a spelling that does not fold (lctn0)
  is owed no refusal and one that does (lctn1) is. The client charsets that can encode a fold character
  are utf8mb3, utf8mb4, binary, gb18030, ujis, eucjpms and latin5 (`U+0130` only in the last three).
  The server logs a `DROP TABLE` already spelled with the stored name, so a near-name DROP reaches the
  log as the captured name; the other operations are logged as sent.

- E: partition operations and database drops. Seed `27_st_part.sql` holds `st_part.kinds` (captured, RANGE-partitioned), `swap`, `plain` and `other` (an uncaptured partitioned table), and the databases `st_dropdb` (holds a captured table) and `st_dropother`. Sent in four name spellings and two client collations: `TRUNCATE PARTITION` (one, ALL), `DROP PARTITION` (one, two), `EXCHANGE PARTITION` with a partitioned table, a plain table and `WITHOUT VALIDATION`, `REORGANIZE`, `REBUILD`, `OPTIMIZE` and `ANALYZE PARTITION`, the same on the uncaptured table, a literal that spells one, and the fold spellings of `kinds` for the first three. `DROP DATABASE` and `DROP SCHEMA` of the captured database in seven spellings, of another database, `IF EXISTS` of a missing one, and a literal. A row whose server effect is a loss of rows without a row event and a definition change is classed `data_change`; the reader owes it a stop.

The verdict of a row is read off the server: the `Com_*` counters of the statement, the table
fingerprints before and after, and the bytes the binlog logged. A row is
`{engine, id, case, errno, kind, touched, src, cs, conn, mode, db, sql (hex), expect}`; `src` says
whether `sql` is the logged event or, when the server logged nothing, the bytes sent.

## The replay test

`src/source/statement_truth.rs` loads every fixture of an engine, replays each row through the adapter's
reader and sorts each disagreement: `missed` (the server changed what the reader captures and the
reader passed it), `over` (the reader refused a statement that touched nothing it captures) and `wrong`
(both refused, differently). An adapter is a function from a row to a class; the MySQL one is
`src/source/mysql/statement_truth.rs`. A row the server did not log never reaches a reader and is not graded.

The MySQL adapter calls the binlog reader's own `query_refusal` and `statement_frame`, the way the event loop does, twice:

- Without `columns_from: grants`: the TRUNCATE, DROP TABLE, DROP DATABASE, partition-data-change and statement-format write readers decide, with the exact server fold under `lower_case_table_names` != 0. A rename or a privilege change is not read without the policy, so it is not owed a stop.
- Under the policy: a statement is caught when `query_refusal` refuses it, or, for a privilege row, when `statement_frame` marks a grants re-read. A refusal of a statement that changes nothing is an over-stop. A separate test fails on any ASCII statement owed a stop that passes.

Every disagreement must be in the adapter's ledger with a row count and a dated reason. The ledger equals the replay exactly: an entry that covers more rows, fewer rows or none fails the test, so the ledger shrinks when the reader is fixed and a partial fix cannot leave room for a later regression. The `coll.*_native` rows touch non-ASCII table names; `validate_table_ident` allows only `[A-Za-z0-9_.]`, so rivet cannot capture such a table and the rows are unreachable today. A missing
fixture directory, a fixture that lost rows and a stand of another size fail the test; none skips.
