# Rivet type mapping contract

This document describes how Rivet maps source column types to logical
[`RivetType`](https://github.com/rivet-lab/rivet/blob/main/src/types/rivet_type.rs) values and Arrow/Parquet/CSV
representations. It is aligned with the automated suite in
[`tests/type_roundtrip/`](https://github.com/rivet-lab/rivet/tree/main/tests/type_roundtrip) and
[`tests/live_type_golden.rs`](https://github.com/rivet-lab/rivet/blob/main/tests/live_type_golden.rs).

## The short version

Wondering "will my decimals / UUIDs / JSON / timestamps silently break on the way
out?" — the short answer is no:

- **Decimals never become floats** — `DECIMAL`/`NUMERIC` export as exact Arrow
  `Decimal128`/`Decimal256` when precision and scale are known.
- **UUID and JSON keep native types** — Parquet gets native `LogicalType::Uuid` /
  `LogicalType::Json`, not opaque strings.
- **Timestamps preserve the instant** — the point in time round-trips (Parquet
  keeps the zone; CSV emits the instant normalised to UTC with a trailing `Z`,
  while naive timestamps render bare).
- **Rivet fails loud, not silent** — an unmapped type is named by
  `rivet check` and aborts the run, and a lossy one is named by `rivet check` and aborts the run under `--strict`, never quietly truncated. The one value-level
  exception is a source value that is not a value of the target type at all
  (a MySQL `0000-00-00`): it is written as NULL and counted, see
  [Values with no Arrow representation](#values-with-no-arrow-representation).

The per-engine tables below are the precise contract; this is just the gist.
[Types by kind](#types-by-kind) lists, for each type that means the same on every
engine, which native type of which engine it comes from.

## Types by kind

Types that mean the same on every engine are decided once, in one shared place
(`CommonKind` in [`src/types/kind.rs`](https://github.com/rivet-lab/rivet/blob/main/src/types/kind.rs)),
and delivered as the same `RivetType` whichever engine the column comes from.
A type only one engine (or some engines) carries is decided in that engine's own
table and is listed in the engine's section. How a value is read off the wire
stays per engine, in the engine's own decoder
([ADR-0038](adr/0038-column-plan.md), amendment of 2026-11-20).

| Kind | Delivered as | [PostgreSQL](#postgresql) | [MySQL](#mysql) | [SQL Server](#sql-server-mssql) | [Oracle](#oracle) |
|------|--------------|------------|-------|------------|--------|
| bool | `bool` | `boolean` | `tinyint(1)` | `bit` | `BOOLEAN` |
| int16 | `int16` | `smallint` | `tinyint`, `smallint`, `tinyint unsigned` | `tinyint`, `smallint` | none |
| int32 | `int32` | `integer` | `mediumint`, `int`, `smallint unsigned` | `int` | `NUMBER(1..9, 0)` |
| int64 | `int64` | `bigint` | signed `bigint`, `mediumint unsigned`, `int unsigned` | `bigint` | `NUMBER(10..18, 0)` |
| float32 | `float32` | `real` | `float` | `real` | `BINARY_FLOAT` |
| float64 | `float64` | `double precision` | `double` | `float` | `BINARY_DOUBLE` |
| decimal | `decimal(p,s)` | `numeric(p,s)` | `decimal(p,s)` | `decimal(p,s)`, `numeric(p,s)` | `NUMBER(19..38, 0)`, `NUMBER(p,s)` with a scale |
| date | `date` | `date` | `date` | `date` | none (`DATE` carries a time) |
| timestamp | `timestamp(µs, none)` | `timestamp` | `datetime` | `datetime`, `smalldatetime`, `datetime2(0..6)` | `DATE`, `TIMESTAMP(0..6)` |
| timestamp_tz | `timestamp_tz(µs, UTC)` | `timestamptz` | `timestamp` | `datetimeoffset(0..6)` | `TIMESTAMP WITH TIME ZONE`, `WITH LOCAL TIME ZONE` |
| text | `string` | `text`, `varchar`, `char`, `name` | `char`, `varchar`, `text` family | `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext` | `VARCHAR2`, `NVARCHAR2`, `CHAR`, `NCHAR`, `LONG`, `CLOB`, `NCLOB`; a `NUMBER` no decimal holds |
| binary | `binary` | `bytea` | `binary`, `varbinary`, `blob` | `binary`, `varbinary`, `image` | `RAW`, `LONG RAW`, `BLOB` |
| json | `json` | `json`, `jsonb` | `json` | none | `JSON` |
| time | `time(µs)` | `time` | `time` | `time` | none |
| uuid | `uuid` | `uuid` | none | `uniqueidentifier` | none |
| interval | `interval` | `interval` | none | none | `INTERVAL DAY TO SECOND`, `INTERVAL YEAR TO MONTH` |

Types an engine carries alone stay in its own table: PostgreSQL `oid`, `timetz`, enums,
arrays and the types it does not map; MySQL `bigint unsigned`, `bit`, `year`, `enum`,
`set`; SQL Server `money`, `smallmoney`, `sql_variant`, CLR types. Within a shared
kind the engine keeps its own reading: MySQL `time` beyond 24 hours and negative,
SQL Server `time(7)` precision, Oracle interval units, PostgreSQL `interval` months,
days and microseconds. MongoDB has no native-type
mapping to route: its two columns are fixed, see [MongoDB](#mongodb-json-blob-model).
CDC does not classify types a second time: it reads the batch mapping of the
same columns, and its value decoders are per engine.

## Values with no Arrow representation

Each source type maps to the closest Arrow type, and Parquet stores that type as the
[Parquet logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)
specification defines it. Arrow `Date32` is a count of days since 1970-01-01 and
`Timestamp` a count of time units since the epoch
([Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html),
[`Schema.fbs`](https://github.com/apache/arrow/blob/main/format/Schema.fbs)), so a
date with month 0 or day 0 has no value in either type, and no warehouse `DATE` holds
one either.

| Source value | What rivet writes | Signal |
|--------------|-------------------|--------|
| MySQL zero, partial or invalid date (`0000-00-00`, `2026-00-15`, `2024-02-31` under `ALLOW_INVALID_DATES`) in `DATE`, `DATETIME`, `TIMESTAMP` | NULL, exit 0, batch and CDC | a WARN per column with the count; the batch summary has a `fidelity` row; `nulled_cells` in the manifest; `validate` warns |
| A date or instant the target type cannot hold (PostgreSQL `infinity` or an out-of-range date, a `timestamp_ns` value outside 1677..2262) | nothing: the run is refused; declaring the column `text` under `columns:` delivers the server's own text for every row, timestamps rendered in UTC (PostgreSQL batch exports; a CDC export with a pending baseline refuses the declaration, because the change stream renders the same cell in its own form; when the source cannot be connected or described that check does not run and a WARN says so) | `RIVET_SOURCE_VALUE_UNREPRESENTABLE`, exit 5 |
| An Oracle `NUMBER` or `FLOAT` of infinity (or an exact `1E125`, which the client prints with the same bytes), an Oracle `INTERVAL DAY TO SECOND` with a sub-microsecond fraction | nothing: the run is refused; `TO_CHAR(col) AS col` in the query exports the exact text | `RIVET_SOURCE_VALUE_UNREPRESENTABLE`, exit 5 |

The columns are labelled `rivet.fidelity: compatible`, never `exact`. The run records
the per-column count in its manifest (`nulled_cells`); `validate` prints it as a
`warning` (`RIVET_VERIFY_CELLS_WRITTEN_AS_NULL`, never a failure, exit unchanged) and
the `reconcile` row of a run with `reconcile: true` appends it to its MATCH or MISMATCH line.

To deliver the exact source text instead, select the column as text in the export
query; the column then lands as a string and loses its date type downstream. This is
the operator's choice per column:

```yaml
query: "SELECT id, CAST(d AS CHAR) AS d FROM t"
```

## Guarantees (v0.18.0)

- **DECIMAL / NUMERIC** are never silently converted to float. They export as
  Arrow `Decimal128` / `Decimal256` in Parquet when precision and scale are known
  (column override, catalog hint, or PostgreSQL wire metadata).
- **Binary** (`BYTEA`, `BLOB`, `BINARY`/`VARBINARY` with charset 63) stays
  Arrow `Binary` in Parquet.
- **Float `NaN` / `±Infinity`** are preserved. Parquet stores them natively
  (IEEE-754); CSV emits the literal `NaN` / `inf` / `-inf` (and `-0` keeps its
  sign) rather than an empty cell — an empty cell would silently conflate a real
  special value with `NULL`. Note these are *float* values: `decimal` / `NUMERIC`
  has no NaN representation and a NaN/Infinity payload there is rejected at
  extract time, not coerced. Strict CSV loaders that expect `Infinity` over `inf`
  should configure their float parser accordingly; Parquet needs no such care.
- **JSON / JSONB** is valid JSON text in the file, paired with the Arrow
  `arrow.json` canonical extension type so parquet-rs emits native
  `LogicalType::Json` in the Parquet footer. The Rivet field metadata
  (`rivet.logical_type=json`) stays for Rivet-aware consumers.
- **UUID** exports as canonical 16-byte `FixedSizeBinary(16)` paired with the
  Arrow `arrow.uuid` canonical extension; parquet-rs emits native
  `LogicalType::Uuid`. Downstream Parquet readers (DuckDB, ClickHouse 25.x+,
  pyarrow, BigQuery autodetect) recover the UUID type without a cast.
- **Timestamps**: `TIMESTAMPTZ` and MySQL `TIMESTAMP` use UTC semantics
  (`timezone: Some("UTC")`); naive `TIMESTAMP` / `DATETIME` have no timezone.
- **Nullability** is preserved in Arrow schema and export.

### What `rivet init` auto-detects (and what needs an override)

`rivet init` reads the source database's catalog / wire-protocol metadata to
build the initial `rivet.yaml`. Whether a column lands as a native logical
type in the resulting Parquet depends on whether the source server
*advertises* the semantic — Rivet never guesses from column names or sample
values, by design (a wrong guess is silent corruption; an honest "I don't
know" is a config knob).

| Semantic | PostgreSQL | MySQL | SQL Server | MongoDB |
|----------|------------|-------|------------|---------|
| `JSON` / `JSONB` | **auto** — `Type::JSON` (OID 114) and `Type::JSONB` (OID 3802) are native PG wire types. | **auto** — `MYSQL_TYPE_JSON` is native since MySQL 5.7. | **manual override required** — SQL Server stores JSON in `nvarchar`; the catalog reports only `nvarchar`. | **always** — the whole document exports as one `document` column (`Utf8` + `arrow.json`), typed downstream by `PARSE_JSON`. |
| `UUID` | **auto** — `Type::UUID` (OID 2950) is a native PG type. | **manual override required.** MySQL has no native UUID; they are stored in `VARCHAR(36)` or `BINARY(16)`. The catalog reports only `varchar`/`binary` — semantic UUID information is gone before the driver ever sees the column. Operators add an explicit override (see below). | **auto** — `uniqueidentifier` is a native type; exports as `FixedSizeBinary(16)` + `LogicalType::Uuid`. | n/a — MongoDB does no per-field typing; `_id` is a stringified `Utf8` key, values live inside the blob. |
| `DECIMAL(p,s)` | **auto when declared** — PG's catalog returns `numeric_precision`/`numeric_scale` for table-qualified queries; an ad-hoc `numeric` expression is delivered as the server's text (`string`, text form `server_text`) unless `columns:` declares a `decimal(p,s)`. | **auto** — precision/scale derive from the wire column definition (display width + decimals), including ad-hoc queries; a `columns:` override is needed only in the rare case derivation fails (Rivet then reports the column Unsupported and names the override). | **auto** — precision/scale recovered from the data (tiberius drops the declared scale). | n/a — numbers stay inside the JSON `document` blob. |

Adding overrides looks like:

```yaml
exports:
  - name: users
    query: "SELECT id, uid, amount FROM users"
    columns:
      uid: uuid              # MySQL VARCHAR(36) → UUID semantic
      amount: decimal(18,2)  # also valid for MySQL DECIMAL without catalog
      event_ts: timestamp    # SQL Server datetime2(7) in microseconds over the full range (see gap 4)
```

Supported override types: `bool`, `int2/int4/int8`, `float4/float8`,
`decimal(p,s)`, `date`, `timestamp`, `timestamp_ns`, `timestamp_tz`,
`timestamp_tz_ns`, `text`, `binary`, `json`, `uuid`. The `_ns` timestamp
variants preserve sub-microsecond precision (range 1677–2262; see gap 4) — the
plain `timestamp` is microsecond with full date range. An undeclared Oracle
`TIMESTAMP(7..9)` column is delivered as nanosecond text (`YYYY-MM-DDTHH:MM:SS.fffffffff`, a trailing `Z` when zoned) in any year, on batch and on CDC, rather than truncated.

A `timestamp` or `timestamp_tz` declared over a result column finer than a
microsecond (an Oracle `TIMESTAMP(7..9)` or `TIMESTAMP(7..9) WITH TIME ZONE`, a SQL Server `datetime2(7)` or
`datetimeoffset(7)`), on batch and on CDC, truncates the finer digits: the type
report says `lossy` with the cause and the remedy, the default policy runs, and `--strict` refuses it
(`RIVET_TYPE_UNSAFE_MAPPING`). The verdict follows the column the query returns:
an alias keeps it, a `CAST` to a coarser type or a column that is not finer is
`exact`. When the source cannot say whether the column is finer (the query cannot be described), the column is graded `lossy` with that cause. To keep the digits, drop the override on SQL Server, or declare
`timestamp_ns` on an Oracle `TIMESTAMP(7..9)` and `timestamp_tz_ns` on an Oracle `TIMESTAMP(7..9) WITH TIME ZONE`. SQL Server refuses `timestamp_tz_ns` over `datetimeoffset(7)` on batch (`RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH`). A `date` declared over such a column is refused by code, on `check` and on `run`, with the same `RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH`: a time column keeps its time type or is read as text.

An override the source reader cannot build is refused by code, not run:
`RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH` (exit 1) names the column, the declared
type and the remedy. MySQL has no nanosecond timestamp (DATETIME and TIMESTAMP
hold microseconds), so `timestamp_ns` / `timestamp_tz_ns` on a MySQL column is
refused before any row is read, even over an empty result; declare `timestamp`
/ `timestamp_tz`. A `decimal(p,s)` override past precision 38 is refused the
same way on SQL Server and Oracle (declare the source column's own `decimal(p,s)` with p <= 38, or remove the override). On SQL
Server a value whose wire type the declared type cannot read is refused at the
first batch with the same code.

A value of a readable wire type that the declared type cannot hold (an Oracle `DATE`
with a time of day under `date`, an Oracle `RAW` of the wrong length or SQL Server
text that is not a uuid under `uuid`, a SQL Server decimal a narrower scale would
round (on a CDC stream as on batch), a MySQL text cell that is not a whole integer under an integer override (`1.0` and
`1 ` are not), an integer past the declared integer width) is refused
`RIVET_SOURCE_VALUE_UNREPRESENTABLE` (exit 5) naming the column and the value, on a
batch run and on a CDC snapshot or stream alike. A refusal decided by the column's
type alone (PostgreSQL, SQL Server and Oracle numeric, float or text under an integer
override; MySQL DECIMAL or DOUBLE under one) is `RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH`
(exit 1) at plan time on every leg: a batch run, the baseline leg of a CDC export and a stream-only CDC export read the same table, so a `mode: cdc` export refuses the pair before it reads a change. A chunked run refused at a later chunk keeps the
parts of the chunks it finished and a manifest whose run is marked `failed`; fix the
value or declare a wider type, and re-run.

A `bool` override over an integer column is a flag only for 0 and 1. PostgreSQL, SQL Server and Oracle refuse it for a batch export at plan time; MySQL, which checks each value, refuses a value outside 0 and 1 at the cell. A CDC stream reads every engine the way MySQL does: 0, 1 and NULL are delivered as `false`, `true` and NULL, and any other integer (2, 5, -1) is refused `RIVET_SOURCE_VALUE_UNREPRESENTABLE` (exit 5) naming the column, the value and its row, before a part is written and before the checkpoint moves. Declare the column `int64` in `columns:` to carry the number.

The override path is safe: a `uid: uuid` declaration tries to parse each
cell — either 16 raw bytes (BINARY(16) layout) or a 36-char canonical text
form. **Text that does not parse emits `NULL`, never silent garbage.** Same
convention as `decimal` overrides whose values do not parse cleanly. A value
the declared type cannot read at all is a different case and is never `NULL`:

- **MySQL integer under `float4`/`float8`** is delivered as the same number
  while the float holds it exactly (up to 2^24 for `float4`, 2^53 for
  `float8`). Past that the run stops with `RIVET_SOURCE_VALUE_UNREPRESENTABLE`
  (exit 5) naming the column and the value; the remedy is `decimal(20,0)` or
  `float8`, or removing the override.
- **A decimal under `float8`** (MySQL `DECIMAL`, SQL Server `money`, Oracle `NUMBER`,
  batch and CDC) is delivered as the same number while the double prints back the
  same digits (always for up to 15 significant digits, and for the integers up to
  2^53). Past that the run stops with `RIVET_SOURCE_VALUE_UNREPRESENTABLE` (exit 5)
  naming the column and the value; the remedy is `decimal(p,s)`, or removing the
  override. `rivet check` reports a `float8` over a source decimal of precision
  above 15 as `lossy`, never `exact`.
- **MySQL value of a kind the declared type does not read** (a `DOUBLE`
  under `int8`, a `TIME` under `text`, an integer under `date`) stops with
  `RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH` (exit 1); the remedy is removing the
  override or a `CAST` in the export's `query:`. A source `NULL` stays `NULL`.
- **PostgreSQL** refuses the same mismatch at `check` with
  `RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH`; **SQL Server** and **Oracle** stop
  on the run (the table of outcomes per engine is pinned by
  `a_declared_override_delivers_the_source_or_refuses_by_code_*`).

`rivet init` does **not** apply heuristics ("column is 36 chars wide and
named `uid_*` → probably UUID") — guessing risks misclassifying a non-UUID
varchar and silently producing wrong-shape Parquet. Operators who want UUID
semantics on a MySQL column add the override explicitly.

## The change stream per type

`docs/type-capability-matrix.yaml` carries one CDC verdict per delivering row, beside its batch verdicts: `cdc: exact` (the change stream delivers what batch delivers, in the same Arrow type and value) or `cdc: refuse:CODE` with a dated `cdc_reason` (rivet stops by that registered code, never silently). A row without a verdict, a form or a known defect spelled there, or an unregistered code fails `type_capability_matrix_guard`. The release oracle's change-stream grid (`property_no_silent_null_cdc`) grades each column against it: an `exact` column that stops, or a `refuse` column that delivers or stops by another code, is `CDC_VERDICT_DIFFERS`; `live_cdc_type_parity` reads the same rows and compares batch, snapshot, stream and the ClickHouse load value for value.

## Test commands

```bash
make test-types              # offline mapping contracts (PR-fast)
make test-types-live         # full matrix; requires docker compose
make test-types-validators   # PG/MySQL/SQL Server → Parquet → {DuckDB, ClickHouse} round-trip
```

`test-types-validators` re-runs the same canonical PG / MySQL / SQL Server type matrix used
by `test-types-live`, but writes the Parquet into the shared bind-mount under
`tests/.live-tmp/` and feeds it through **three independent readers**: DuckDB,
ClickHouse (both from `docker-compose.yaml`), and pyarrow (installed alongside
duckdb in the same container). Each reader catches what the others cannot:

| Reader | What it pins |
|--------|--------------|
| DuckDB | Autoload physical types (`DECIMAL`, `TIMESTAMPTZ`, `INTEGER[]`, `BLOB`, …), decimal sums, JSON validity, UUID parseability, byte-exact BLOB, list lengths (including empty), null-bitmap propagation |
| ClickHouse | Independent confirmation of the above through a second decoder; native `UInt64` round-trip for `BIGINT UNSIGNED`; tz-aware timestamps as `DateTime64(6, 'UTC')` |
| pyarrow | Arrow field metadata (`rivet.*` keys) reaches the Parquet footer; row-group statistics (`min`/`max`/`null_count`) are correct; Decimal256 (precision > 38) round-trips exactly where DuckDB downgrades to DOUBLE |

Local-reader type fidelity is now covered by the cross-tool harness'
**type-loss matrix** ([`dev/bench/smoke.py`](https://github.com/rivet-lab/rivet/blob/main/dev/bench/smoke.py), rendered in
[`report.html`](bench/report.html)): every source column vs each tool's Parquet
type family. A BigQuery cloud-load type-diff (load each Rivet Parquet via
`bq load --autodetect`, assert decimal sums round-trip) is **not** currently in
the harness — it needs a GCP project + `bq` auth and is tracked for a future
cloud dimension (notably the earlier findings were:
`LogicalType::Json` does **not** autoload as native BQ `JSON` — it
falls back to `BYTES`/`STRING`; values are valid JSON but operators
need an explicit `--schema='attrs:JSON,...'` to query the structure).

In addition, two structural-only tests pin the Parquet layout itself —
[`parquet_schema.rs`](https://github.com/rivet-lab/rivet/blob/main/tests/type_roundtrip/parquet_schema.rs) for
physical + logical types per column, and
[`parquet_metadata.rs`](https://github.com/rivet-lab/rivet/blob/main/tests/type_roundtrip/parquet_metadata.rs) for the
`rivet.native_type` / `rivet.fidelity` / `rivet.logical_type` field metadata.

Coverage extensions live in dedicated files:
[`compression_matrix.rs`](https://github.com/rivet-lab/rivet/blob/main/tests/type_roundtrip/compression_matrix.rs)
re-runs the export under `zstd` / `snappy` / `gzip` / `none` and asserts
value parity across all four codecs;
[`csv_load.rs`](https://github.com/rivet-lab/rivet/blob/main/tests/type_roundtrip/csv_load.rs) replays the matrix
through DuckDB `read_csv_auto`;
[`pg_edge_cases.rs`](https://github.com/rivet-lab/rivet/blob/main/tests/type_roundtrip/pg_edge_cases.rs) covers
decimal precision boundaries (38, 39 / Decimal128↔Decimal256), tz timestamps
pre-epoch and far-future, JSON deep nesting + unicode keys + i64 edges,
arrays with NULL elements, large single-cell strings.

### v0.7.8 breaking change: UUID export layout

UUID columns (PG native `uuid` type and MySQL columns explicitly overridden
as `columns: { col: uuid }`) now export as `FixedSizeBinary(16)` instead of
hyphenated `Utf8`. This is what lets parquet-rs emit native
`LogicalType::Uuid` so downstream readers (DuckDB, ClickHouse 25.x+,
pyarrow, BigQuery autodetect) recover the UUID type without a cast.

What this changes:

- **Parquet schema**: `BYTE_ARRAY + LogicalType::String` → `FIXED_LEN_BYTE_ARRAY + LogicalType::Uuid`.
- **On-disk bytes**: 36-char canonical ASCII (`a0eebc99-9c0b-…`) → 16 raw bytes (the same UUID, compact encoding).
- **CSV**: unchanged — the CSV writer still emits hyphenated lowercase text.
- **DuckDB autoload**: `VARCHAR` → `UUID` native type. A view like
  `SELECT uid::VARCHAR FROM read_parquet(...)` keeps working because
  DuckDB's `UUID::VARCHAR` cast yields the canonical form.
- **ClickHouse autoload**: a plain Parquet read gives `FixedString(16)` (the bytes,
  not the text) on 24.8 and 26.8. `rivet load` into ClickHouse reads the column as
  those bytes and converts it with `toUUID(UUIDNumToString(...))`, so the table holds
  a native `UUID` (`Array(UUID)` for a list); only a direct read of the file needs
  the conversion.

Consumers reading the old Utf8-shaped Parquet files keep working — only
files produced by Rivet ≥ v0.7.8 carry the new layout.

### Findings & fixes from triangulating against external readers

Driving the matrix through DuckDB + ClickHouse + pyarrow exposed three real
defects in the PG / MySQL drivers; all have been fixed in v0.7.8:

* **PG arrays with NULL elements were silently lost.** The driver decoded
  `ARRAY[1, NULL, 3]` via `try_get::<Vec<i32>>`, which errors on a NULL
  element; the error was swallowed and a whole-row NULL was written. Fixed
  by deserializing as `Vec<Option<T>>` and pushing nulls through
  `ListBuilder::append_null` ([`src/source/postgres/arrow_convert.rs`](https://github.com/rivet-lab/rivet/blob/main/src/source/postgres/arrow_convert.rs)).
* **MySQL ENUM / SET were misclassified as `String`.** They arrive on the
  wire as `MYSQL_TYPE_STRING` / `MYSQL_TYPE_VAR_STRING` with the
  `ENUM_FLAG` / `SET_FLAG` set, not as `MYSQL_TYPE_ENUM`. The mapper now
  checks the flag and emits `RivetType::Enum` so the
  `rivet.logical_type=enum` Parquet metadata is preserved.
* **MySQL `native_type` lost precision.** `tinyint unsigned`, `tinyint(1)`,
  `bit(1)`, `char` vs `varchar`, `binary` vs `varbinary` all collapsed to a
  single label. The mapper now distinguishes them via `column_type()` +
  `flags()` + `character_set`.

### Known limitations

* PG `numeric(p, -s)` (negative scale) cannot be written to Parquet — the
  spec requires non-negative DECIMAL scale. The catalog does not report a
  negative scale, so a bare column is a bare `numeric` and is delivered as the
  server's own text labelled `server_text`
  (`pg_edge_decimal_negative_scale_without_override_is_delivered_as_the_servers_text`) and a
  `decimal(p,-s)` override is refused as
  `RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH`
  (`pg_edge_decimal_negative_scale_override_is_refused_by_the_override_table`).
* DuckDB's `DECIMAL` is capped at precision 38 (HUGEINT-backed). For
  Parquet files with `precision > 38` DuckDB silently returns DOUBLE.
  Our file still carries `LogicalType::Decimal(p, s)` correctly — pyarrow
  decodes it as `Decimal256`. Verified in
  `pg_edge_decimal_boundaries_round_trip`.

## PostgreSQL

| Source type | Rivet logical | Parquet (Arrow) | CSV | Notes | Tested |
|-------------|---------------|-----------------|-----|-------|--------|
| `smallint` | `int16` | `Int16` | integer text | | golden |
| `integer` | `int32` | `Int32` | integer text | | golden |
| `bigint` | `int64` | `Int64` | integer text | | golden |
| `numeric(p,s)` | `decimal(p,s)` | `DECIMAL(p,s)` | exact decimal text | a `numeric` with no precision is the server's text; declare `decimal(p,s)` to type it | contract + live matrix |
| `real` | `float32` | `Float32` | float text | | golden |
| `double precision` | `float64` | `Float64` | float text | | golden |
| `date` | `date` | `Date32` | ISO date | | golden |
| `time` | `time(microsecond)` | `Time64(µs)` | time text | | partial |
| `timestamp` | `timestamp(microsecond)` | `Timestamp(µs, None)` | datetime text | naive wall clock | live matrix |
| `timestamptz` | `timestamp_tz(µs, UTC)` | `Timestamp(µs, UTC)` | datetime text | | live matrix |
| `text` / `varchar` | `string` | `Utf8` | escaped UTF-8 | newlines/quotes escaped | live matrix |
| `bytea` | `binary` | `Binary` | lowercase hex | | live matrix |
| `json` / `jsonb` | `json` | `Utf8` + Parquet `LogicalType::Json` (via `arrow.json` extension) | JSON string | | live matrix |
| `uuid` | `uuid` | `FixedSizeBinary(16)` + Parquet `LogicalType::Uuid` (via `arrow.uuid` extension) | canonical UUID text | downstream readers autoload as native UUID type | golden |
| `boolean` | `bool` | `Boolean` | true/false | | type_roundtrip |
| `numeric(10,2)` | `decimal(10,2)` | `DECIMAL(10,2)` | exact decimal text | second precision tier | type_roundtrip |
| `char` / `bpchar` | `string` | `Utf8` | escaped UTF-8 | padded char | type_roundtrip |
| `interval` | `interval` | `Utf8` labelled `rivet.text_form = iso8601_duration` | ISO 8601 duration text, six fraction digits when the value has a fraction | not Parquet Interval type; a reader tells the duration text from a plain string by the field label | type_roundtrip, type capability ledger |
| `enum` | `enum` | `Utf8` + logical=enum | label text | custom PG enum | type_roundtrip |
| `text[]` | `list<string>` | `List<Utf8>` | — | 1-D arrays | type_roundtrip |
| `integer[]` | `list<int32>` | `List<Int32>` | — | 1-D arrays | type_roundtrip |
| `date[]` | `list<date>` | `List<Date32>` | — | 1-D arrays; an element of `infinity` or `-infinity` is refused, declare the column `text` under `columns:` for the server's own text, or map it to a bound in the query | type capability ledger |
| `timestamp[]` / `timestamptz[]` | `list<timestamp>` | `List<Timestamp(µs)>` / `List<Timestamp(µs, UTC)>` | — | 1-D arrays; the same `infinity` refusal and remedy | type capability ledger |
| `time[]` | `list<time>` | `List<Time64(µs)>` | — | 1-D arrays; an element of `24:00:00` is refused, declare the column `text` under `columns:` for the server's own text | type capability ledger |
| `uuid[]` | `list<uuid>` | `List<FixedSizeBinary(16)>` with the `arrow.uuid` item | — | read as native UUID lists downstream | type capability ledger |
| `bytea[]` | `list<binary>` | `List<Binary>` | — | 1-D arrays | type capability ledger |
| `json[]` / `jsonb[]` | `list<json>` | `List<Utf8>` | — | 1-D arrays; each element is its JSON text | type capability ledger |
| `interval[]` | `list<interval>` | `List<Utf8>` | — | 1-D arrays; each element is its ISO 8601 duration text | type capability ledger |
| `oid[]` | `list<int64>` | `List<Int64>` | — | 1-D arrays; read at the unsigned 32-bit width | type capability ledger |
| enum array | `list<enum>` | `List<Utf8>` | — | 1-D arrays; each element is its label | type capability ledger |
| any other type with no Arrow mapping: `inet`, `cidr`, `macaddr`, `macaddr8`, `bit`, `varbit`, `money`, `"char"`, `timetz`, `xml`, `tsvector`, `tsquery`, `point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle`, ranges and multiranges, `regclass`, `regtype`, `xid`, `xid8`, `cid`, `tid`, `pg_lsn`, composites, extension types, and arrays of them | `string` (text form `server_text`) | `Utf8` labelled `rivet.text_form = server_text` | escaped UTF-8 | batch delivers the text the server prints for the value (`inet` without a `/32` or `/128` host mask, as the server prints it); the session is fixed to `datestyle ISO, MDY`, `TimeZone UTC`, `extra_float_digits 3`, `intervalstyle postgres`, `bytea_output hex`, so a range of dates or timestamps reads as ISO text; `money` is printed in the server's `lc_monetary`; a column declared in `columns:` keeps its declaration; a `numeric` with no declared precision (an ad-hoc expression, or a precision the catalog hint cannot carry) and an array of `numeric` are delivered as the server prints them, `NaN` and `Infinity` included; a `numeric(p,s)` the catalog types, or a `numeric` declared in `columns:`, keeps its decimal; `rivet check --strict` passes these columns (they are not refused) and still exits 1 for a type that is refused; a drift baseline the previous release stored for such a column under its native name (`numeric`, `_numeric`) is rewritten to `Utf8` on the first run after the upgrade without reporting drift. CDC delivers the same text, `bit` and `bit varying` (`B'110'` unwrapped to `110`) and every array of these types included | live `live_pg_server_text`, `live_cdc_value_parity` |
| nullable / all-null | — | null bitmap preserved | empty cells | `note_nullable`, `note_all_null` | type_roundtrip |
| large `text` | `string` | `Utf8` | escaped | 2k–5k chars | type_roundtrip |

## MySQL

| Source type | Rivet logical | Parquet (Arrow) | CSV | Notes | Tested |
|-------------|---------------|-----------------|-----|-------|--------|
| `tinyint` (not width 1) | `int16` | `Int16` | integer text | widened signed | golden |
| `tinyint(1)` | `bool` | `Boolean` | 0/1 | MySQL boolean convention; a value outside 0 and 1 is refused `RIVET_SOURCE_VALUE_UNREPRESENTABLE` (exit 5), declare `int64` in `columns:` to carry the number | golden |
| `smallint` | `int16` | `Int16` | integer text | | golden |
| `int` | `int32` | `Int32` | integer text | | golden |
| `bigint` | `int64` | `Int64` | integer text | signed | golden |
| `bigint unsigned` | `u_int64` → `UInt64` | `UInt64` | integer text | values &gt; i64::MAX | type_roundtrip |
| `decimal(p,s)` | `decimal(p,s)` | `DECIMAL(p,s)` | exact decimal text | p/s auto-resolved from the wire column definition (override only as fallback) | live matrix |
| `float` / `double` | `float32` / `float64` | `Float32` / `Float64` | float text | | golden |
| `date` | `date` | `Date32` | ISO date | a zero or partial date (`0000-00-00`, `2026-00-00`) is written as NULL, the run WARNs per column with the count and the summary has a `fidelity` row; the column is labelled `rivet.fidelity: compatible`, never `exact`; see [Values with no Arrow representation](#values-with-no-arrow-representation) | golden |
| `datetime` | `timestamp(µs, none)` | `Timestamp(µs, None)` | datetime text | naive; same zero-date rule and `compatible` label as `date` | live matrix |
| `timestamp` | `timestamp_tz(µs, UTC)` | `Timestamp(µs, UTC)` | datetime text | `SET time_zone = '+00:00'`; same zero-date rule and `compatible` label as `date` | live matrix |
| `time` | `string` | `Utf8` | `[-]hh:mm:ss.ffffff` text (`rivet.text_form: time_beyond_day`) | a MySQL `TIME` is a signed duration of -838:59:59 to 838:59:59, which `Time64(µs)` (00:00..24:00) cannot hold, so every value is delivered as text, batch and CDC, in a `logical_string` column | live matrix |
| `varchar` / `text` | `string` / `text` | `Utf8` | escaped UTF-8 | | live matrix |
| `json` | `json` | `Utf8` + Parquet `LogicalType::Json` (via `arrow.json` extension) | JSON string | | live matrix |
| `binary` / `varbinary` / `blob` | `binary` | `Binary` | hex in CSV | charset 63 / binary payload; under `columns: {c: string}` a value that is valid UTF-8 is delivered as that text and any other byte stops the run with `RIVET_SOURCE_VALUE_UNREPRESENTABLE` (declare `binary`, or select `HEX(c)` / `ST_AsText(c)`), never U+FFFD | type_roundtrip, live matrix |
| `geometry` / `point` / `linestring` / `polygon` / `multipoint` / `multilinestring` / `multipolygon` / `geometrycollection` | `string` | `Utf8` | lowercase hex (`rivet.text_form: hex_wkb`) | the standard WKB of the value as lowercase hex, equal to `LOWER(HEX(ST_AsBinary(col)))`, batch and CDC; the SRID is not carried, so a value whose SRID is not 0 stops the run with `RIVET_SOURCE_VALUE_UNREPRESENTABLE` (exit 5) naming the column: declare the column `binary` in `columns:` to carry MySQL's internal bytes (4-byte little-endian SRID, then WKB), or in a batch export select `ST_AsBinary(col)` and `ST_SRID(col)` in the query. A warehouse recovers the geometry from the text: BigQuery `ST_GEOGFROMWKB(x)`, Snowflake `TO_GEOMETRY(x)`, ClickHouse `readWKB*(unhex(x))` (25.8 or later; 24.8 has no `readWKB*`) | live (batch + CDC) |
| `bit(1)` | `bool` | `Boolean` | | | golden |
| `bit(n)`, 1 < n <= 63 | `int64` | `Int64` | | avoids silent truncation | type_roundtrip |
| `bit(64)` | `u_int64` → `UInt64` | `UInt64` | integer text | every value, including those with bit 63 set; `Int64` holds half of them | type_roundtrip |
| `tinyint unsigned` | `int16` | `Int16` | integer text | 0–255 | type_roundtrip |
| `smallint unsigned` | `int32` | `Int32` | integer text | up to 65535 | type_roundtrip |
| `int unsigned` | `int64` | `Int64` | integer text | up to 4294967295 | type_roundtrip |
| `decimal(10,2)` | `decimal(10,2)` | `DECIMAL(10,2)` | exact decimal text | p/s auto-resolved from the wire column definition (override only as fallback) | type_roundtrip |
| `char` | `string` | `Utf8` | escaped | fixed `CHAR(n)` | type_roundtrip |
| `mediumtext` / `longtext` | `text` | `Utf8` | escaped | large payloads | type_roundtrip |
| `enum` / `set` | `enum` | `Utf8` + logical=enum | label text | SET comma-separated | type_roundtrip |
| `year` | `int16` | `Int16` | integer text | calendar year | type_roundtrip |
| `boolean` (native) | `bool` | `Boolean` | true/false | not only `TINYINT(1)` | type_roundtrip |
| nullable / all-null | — | preserved | empty cells | edge columns | type_roundtrip |

## SQL Server (MSSQL)

| Source type | Rivet logical | Parquet (Arrow) | CSV | Notes | Tested |
|-------------|---------------|-----------------|-----|-------|--------|
| `tinyint` (0–255) | `int16` | `Int16` | integer text | widened (unsigned source) | live matrix |
| `smallint` | `int16` | `Int16` | integer text | | live matrix |
| `int` | `int32` | `Int32` | integer text | | live matrix |
| `bigint` | `int64` | `Int64` | integer text | | live matrix |
| `bit` | `bool` | `Boolean` | 0/1 | | live matrix |
| `decimal(p,s)` / `numeric(p,s)` | `decimal(p,s)` | `DECIMAL(p,s)` | exact decimal text | scale recovered from the data (tiberius drops declared scale) | live matrix |
| `money` | `decimal(19,4)` | `DECIMAL(19,4)` | exact decimal text | fixed scale; the whole range (±922337203685477.5807) is exact on batch and CDC; a `float64` override is delivered while the double prints back the same digits and refused `RIVET_SOURCE_VALUE_UNREPRESENTABLE` for a value it would round, a narrower `decimal(p,s)` override refuses a value it would round (`RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH`) | live matrix |
| `smallmoney` | `decimal(10,4)` | `DECIMAL(10,4)` | exact decimal text | fixed scale; exact over the whole range on batch and CDC | type_roundtrip |
| `real` | `float32` | `Float32` | float text | | live matrix |
| `float` | `float64` | `Float64` | float text | | live matrix |
| `date` | `date` | `Date32` | ISO date | | live matrix |
| `time(0..6)` | `time(µs)` | `Time64(µs)` | time text | µs precision | live matrix |
| `time(7)` (the bare `time`) | `time(ns)` | `Time64(ns)` | time text | the 100 ns tick is kept; ClickHouse `Decimal(18, 9)`, BigQuery INT64 nanoseconds of day, Snowflake NUMBER recovered with `TIME_FROM_PARTS` | live matrix |
| `datetime2(0..6)` / `datetime` / `smalldatetime` | `timestamp(µs, none)` | `Timestamp(µs, None)` | datetime text | naive; µs, full range | live matrix |
| `datetime2(7)` | `timestamp(ns, none)` | `Timestamp(ns, None)` | datetime text | naive; the 100 ns tick is kept, 1677-09-21..2262-04-11 (a value outside it is refused); declare `timestamp` for µs over the full range, see known gap 4 | live matrix |
| `datetimeoffset(0..6)` | `timestamp_tz(µs, UTC)` | `Timestamp(µs, UTC)` | datetime text | normalised to UTC | type_roundtrip |
| `datetimeoffset(7)` | `timestamp_tz(ns, UTC)` | `Timestamp(ns, UTC)` | datetime text | normalised to UTC; the 100 ns tick is kept, 1677-09-21..2262-04-11 (a value outside it is refused); declare `timestamp_tz` for µs over the full range, see known gap 4; BigQuery lands it as INT64 UTC nanoseconds since the epoch | live matrix |
| `nvarchar` / `varchar` / `nchar` / `char` / `text` / `ntext` | `string` | `Utf8` | escaped UTF-8 | | live matrix |
| `varbinary` / `binary` / `image` | `binary` | `Binary` | hex in CSV | | live matrix |
| `uniqueidentifier` | `uuid` | `FixedSizeBinary(16)` + Parquet `LogicalType::Uuid` | canonical UUID text | native UUID downstream | live matrix |
| `sql_variant` | `string` | `Utf8` | the value as text | the base value rendered as text: integers and exact decimals as digits, temporals as ISO (`CONVERT(…, 121)`), uuid upper-case, bytes lowercase hex, floats as the shortest round-trip form | live (batch + CDC) |
| `hierarchyid` / `geometry` / `geography` (CLR UDTs) | `string` | `Utf8` | lowercase hex | the serialized UDT bytes as lowercase hex, equal to `CAST(col AS varbinary(max))` | live (batch + CDC) |
| nullable / all-null | — | preserved | empty cells | | live matrix |

Unmapped SQL Server types resolve to `Unsupported` and fail loudly at schema
build unless a `columns:` override maps them.

## Oracle

Behind the `oracle` feature (on by default). The common kinds are read from the
driver's column description, the declared native type, precision and scale
(see [Types by kind](#types-by-kind)); a `NUMBER` picks its kind by precision
and scale.

| Source type | Rivet logical | Parquet (Arrow) | CSV | Notes | Tested |
|-------------|---------------|-----------------|-----|-------|--------|
| `NUMBER(p,0)`, p 1..9 | `int32` | `Int32` | integer text | | type table |
| `NUMBER(p,0)`, p 10..18 | `int64` | `Int64` | integer text | | type table |
| `NUMBER(p,0)`, p 19..38 | `decimal(p,0)` | `DECIMAL(p,0)` | exact decimal text | | type table |
| `NUMBER(p,s)`, s > 0 | `decimal(p,s)` | `DECIMAL(p,s)` | exact decimal text | a precision below the scale is widened to the scale | type table |
| `NUMBER(p,s)`, s < 0 | `decimal(p-s,0)` | `DECIMAL(p-s,0)` | exact decimal text | the scale is folded into the integer digits, up to 38 | type table |
| bare `NUMBER`, `FLOAT(n)`, a `NUMBER` past 38 digits or with a scale above 38 | `string` | `Utf8` labelled `rivet.text_form = decimal_plain` | plain decimal text | no fixed-scale decimal holds every value; declare `decimal(p,s)` with p <= 38 in `columns:` to get a native decimal; a `NUMBER` of infinity is refused, see [Values with no Arrow representation](#values-with-no-arrow-representation) | type table |
| `BINARY_FLOAT` / `BINARY_DOUBLE` | `float32` / `float64` | `Float32` / `Float64` | float text | | type table |
| `BOOLEAN` | `bool` | `Boolean` | true/false | | type table |
| `DATE` | `timestamp(µs, none)` | `Timestamp(µs, None)` | datetime text | an Oracle `DATE` holds a time of day | type table |
| `TIMESTAMP(0..6)` | `timestamp(µs, none)` | `Timestamp(µs, None)` | datetime text | naive | type table |
| `TIMESTAMP(7..9)` | `timestamp(µs, none)` | `Utf8` (`text:iso_timestamp_nanos`) | datetime text | undeclared, delivered as nanosecond text (`Utf8`, `logical_string`); declared `timestamp` it is `Timestamp(µs, None)` and labelled `lossy`; declared `timestamp_ns` it is a native nanosecond timestamp | type table |
| `TIMESTAMP WITH TIME ZONE` / `WITH LOCAL TIME ZONE` | `timestamp_tz(µs, UTC)` | `Timestamp(µs, UTC)` | datetime text | normalised to UTC; a `WITH LOCAL TIME ZONE` value Oracle stored outside 4712 BC to 9999 is refused with `RIVET_SOURCE_VALUE_UNREPRESENTABLE` (exit 5) under every mapping, and so is any timestamp or date value read past 9999 outside the 1 January 10000 window whatever produced it or stored it (a stored `DATE`, `TIMESTAMP` or `TIMESTAMP WITH TIME ZONE` value past the window, a `CAST` to `TIMESTAMP`, `TIMESTAMP WITH TIME ZONE` or `DATE`, a view, `TRUNC`, `ADD_MONTHS` including `ADD_MONTHS(l, -11)` over a shifted row, `FROM_TZ`, `COALESCE`, `UNION ALL`, the `text`, `json`, `timestamp_ns` and `date` overrides, and a value Oracle computes in a `query:` from a real year-10000 instant), except a real instant on 1 January 10000 before 16:00, which is delivered as a `TIMESTAMP WITH TIME ZONE` column delivers it; limit: `TRUNC(l, 'YYYY')` over a shifted row lands inside that window and is delivered a year off, exactly as on main, because it cannot be told apart from `TRUNC` of a real instant; an undeclared `(7..9)` is `Utf8` nanosecond text (`logical_string`) with a trailing `Z`; declared `timestamp_tz` it is `lossy` | type table |
| `INTERVAL DAY TO SECOND`, `INTERVAL YEAR TO MONTH` | `interval` | `Utf8` labelled `rivet.text_form = iso8601_duration` | ISO 8601 duration text | a day-to-second value with a sub-microsecond fraction is refused (`RIVET_SOURCE_VALUE_UNREPRESENTABLE`) | type table |
| `VARCHAR2` / `NVARCHAR2` / `CHAR` / `NCHAR` / `LONG` / `CLOB` / `NCLOB` | `string` | `Utf8` | escaped UTF-8 | | type table |
| `JSON` | `json` | `Utf8` + Parquet `LogicalType::Json` (via `arrow.json` extension) | JSON string | labelled `logical_string` | type table |
| `RAW` / `LONG RAW` / `BLOB` | `binary` | `Binary` | hex in CSV | | type table |

Any other Oracle type (`BFILE`, ...) resolves to `Unsupported` and is named by
`rivet check`; select a convertible expression of it in a `query:`, or drop it.
The type table is the offline test `the_oracle_type_table_is_pinned`
(`src/source/oracle/arrow_convert/type_table.rs`).

## MongoDB (JSON-blob model)

MongoDB has no fixed per-collection schema and no `information_schema`, so Rivet
does not map per-field SQL types the way the three SQL engines do. Every document
exports as exactly two columns:

| Source | Rivet logical | Parquet (Arrow) | CSV | Notes | Tested |
|--------|---------------|-----------------|-----|-------|--------|
| document key (`_id`) | `string` | `Utf8` | stringified key | ObjectId → hex, int → decimal string, … | live |
| whole document | `json` | `Utf8` + Parquet `LogicalType::Json` (via `arrow.json` extension) | JSON string | full BSON as extended JSON — relaxed by default, canonical opt-in (`source.mongo.json`) | live |

Per-field typing is deferred to the warehouse (`PARSE_JSON` → `VARIANT` on
Snowflake, native `JSON` on BigQuery / DuckDB). This is lossless and
schema-drift-proof: a new field in a document never breaks a load. Schema
inference / auto-discovery into typed columns is deliberately out of OSS scope.
CDC (change streams) emits the same two-column shape prefixed with the
`__op` / `__pos` / `__seq` meta columns. See
[reference/mongodb.md](reference/mongodb.md) for the full contract.

## Known gaps (tracked)

1. **Multi-dimensional arrays**: not in the type matrix; they fail at the read
   (`RIVET_SOURCE_VALUE_UNREPRESENTABLE`, exit 5) unless a `columns:` override maps
   them; declaring the column `text` delivers the server's own array text. A PostgreSQL type with no Arrow
   mapping (ranges, `inet`, geometric types, PostGIS and the rest of the
   PostgreSQL row above) is delivered as the server's text.
2. **Nullability**: every exported column is `OPTIONAL`; a source `NOT NULL`
   constraint is not propagated into the Parquet schema (ADR-0016, deferred to
   v0.8 Phase A).
3. **CSV complex types**: a flat array (`List` of a scalar), `Decimal256` (precision > 38) and a
   nanosecond timestamp (an Oracle `TIMESTAMP(9)` declared `timestamp_ns`; undeclared it is already text) is written as text (see [CSV serialization](#csv-serialization)); a nested
   array and non-UUID fixed binary have no CSV cell — the export **fails loudly** naming the column
   rather than silently writing empty values.
4. **SQL Server `datetime2(7)` / `datetimeoffset(7)` sub-microsecond precision** — *default is
   nanosecond; microsecond is the per-column opt-out.* rivet delivers a column whose declared
   scale is above 6 (a bare `datetime2` is `datetime2(7)`) as `Timestamp(ns)` on batch and on CDC,
   so the 7th fractional digit (the 100 ns tick) is kept. Arrow nanosecond timestamps are i64 ns
   and span only **1677-09-21 .. 2262-04-11**, while `datetime2` spans 0001–9999, so a value outside
   that window is refused by code (below), never written as NULL or clamped. The scale comes from the
   catalog for a single-table `SELECT` (and for every CDC table); a query that is not one is read from
   its first non-null value, and a first batch that is all NULL stays microseconds with a warning.
   `datetime2(0..6)`, `datetime` and `smalldatetime` are microseconds as before, lossless.

   To get **microseconds over the full 0001..9999 range** instead (dropping the 100 ns tick), declare
   the column per export:

   ```yaml
   columns:
     event_ts: timestamp          # naive; use timestamp_tz for datetimeoffset
   ```

   `timestamp_ns` / `timestamp_tz_ns` stay accepted and mean the same as the default.
   **Caveats (verified live 2026-06-07; the default now takes this path):**
   - A value outside 1677-09-21 .. 2262-04-11 (Arrow's nanosecond range) is **refused**: the run stops with `RIVET_SOURCE_VALUE_UNREPRESENTABLE` (exit 5) naming the column and the value, before any part is written. Declare the column `timestamp` / `timestamp_tz` (microseconds, full range). Before 0.33.0 such a value was written as NULL with exit 0; before this release a `datetime2(7)` landed at microseconds over the full range with no override.
   - **DuckDB** — native `TIMESTAMP_NS`, fully lossless.
   - **Snowflake** — autoloads as `NUMBER(38,0)` (raw nanos); `TO_TIMESTAMP_NTZ(col, 9)`
     recovers a **lossless** `TIMESTAMP_NTZ` (it holds 9 digits — the 7th survives).
   - **BigQuery** — autoloads as `INT64` (raw nanos, lossless as an integer); a
     native `TIMESTAMP_MICROS(DIV(col,1000))` is **lossy** (BigQuery `TIMESTAMP`
     is microsecond — the 7th digit drops). Keep the default `timestamp` for
     BigQuery unless you carry the raw nanos.

   **Keyset key:** a µs cursor cannot page a `datetime2`/`datetimeoffset` key with
   more than 6 fractional digits, so
   `chunk_by_key:` refuses it with `RIVET_PLAN_CHUNK_KEY_UNUSABLE` when the column is declared
   `timestamp` / `timestamp_tz`; by default a `datetime2(7)` / `datetimeoffset(7)` key is delivered as a nanosecond timestamp and admitted; for a BigQuery
   `TIMESTAMP` use a `datetime2(6)`, an integer key or `mode: full` instead. On Oracle no override lifts the refusal of a `TIMESTAMP(7..9)` key.

   **Incremental mode:** a µs cursor on a `datetime2(7)` (declared `timestamp`) lands one tick
   below the source max, re-exporting the boundary row every run; the default nanosecond cursor
   literal carries all 9 digits, as does a `datetime2(6)` (or coarser) or an integer/identity cursor.

   **Primary key of an incremental or CDC load:** a `datetime2(7)` / `datetimeoffset(7)` (or Oracle `TIMESTAMP(7..9)` declared `timestamp`) key column
   lands at microseconds when declared `timestamp`, so two rows that differ only below a microsecond would collapse into one row of the current-state view.
   `rivet load` and `rivet check` refuse it with `RIVET_LOAD_KEY_COLUMN_LANDS_INEXACT` (exit 5) until `timestamp_ns` / `timestamp_tz_ns` is declared (the SQL Server default) or a `pk:` that
   does not depend on it is named; parts already extracted hold microseconds, so run `rivet state reset --export <name>` and the export again before the reload. A table already loaded keeps its column type: drop its change log and view first.
   A cursor that lands as text (a bare Oracle `NUMBER`) is refused the same way for an incremental load: declare `int64` or `decimal(p,s)`.

(MySQL `DECIMAL` now resolves its precision/scale from the wire column
definition — no override needed; see the MySQL section.)

## Fidelity labels

| Label | Meaning |
|-------|---------|
| `exact` | Value and type semantics preserved |
| `compatible` | Value preserved; physical type differs (e.g. UUID as Utf8) |
| `logical_string` | Valid text; native JSON tree semantics not enforced in Arrow |
| `lossy` | Rejected in strict mode |
| `unsupported` | Requires policy override |

See [`src/types/fidelity.rs`](https://github.com/rivet-lab/rivet/blob/main/src/types/fidelity.rs).

## CSV serialization

CSV shares the same Arrow `RecordBatch` as Parquet, so values are identical —
only the text rendering differs ([`src/format/csv.rs`](https://github.com/rivet-lab/rivet/blob/main/src/format/csv.rs)):

| RivetType | CSV rendering |
|-----------|---------------|
| ints / `float` / `bool` / `decimal` | plain text (`decimal` exact, never via float) |
| `string` / `text` / `json` / `enum` / `interval` | text, RFC-4180 quoted/escaped when needed |
| `uuid` | canonical hyphenated lowercase (`a0eebc99-…`) |
| `binary` (`bytea` / `BLOB`) | lowercase hex (`deadbeef`) |
| `date` / `time` / `timestamp` | ISO 8601 (`2026-01-01T12:00:00.000000`) |
| `timestamp_tz` | ISO 8601 normalised to UTC with a trailing `Z` (`2026-01-01T12:00:00.000000Z`) — distinguishable from a naive timestamp, which renders bare |
| `list` (flat array of a scalar) | one JSON array text in one quoted field: `["a,b",null,""]`; each element keeps its scalar text (dates, timestamps, uuid, hex bytes and decimals as JSON strings, ints, bools and finite floats bare), a NULL element is `null`, a non-finite float is the string `"NaN"`, `"inf"` or `"-inf"`; an empty list is `[]`, distinct from the empty NULL cell |
| `Decimal256` (precision > 38) | exact plain decimal text, never via float |
| `timestamp` / `timestamp_tz` at nanosecond precision | ISO 8601 with nine fraction digits, `Z` for an instant (`2026-01-01T12:00:00.123456789Z`) |

CSV has **no cell representation** for a nested `list` (an array of arrays) or
non-UUID fixed binary. Rather than silently write an empty value, the export
**fails at writer creation** naming the column — use `format: parquet` or drop
the column from the query.

**Loading CSV into a warehouse:** unlike Parquet (whose loader will not coerce a
declared type — see below), BigQuery's CSV loader *honors* a declared `--schema`.
Bare `--autodetect` infers `decimal` text as `FLOAT` (precision-lossy) and every
semantic type (`uuid` / `json` / `bytea`) as `STRING`; declare `NUMERIC` / `JSON`
in the load schema, or recover post-load with `PARSE_JSON` / `FROM_HEX` as for
Parquet.

## Downstream targets (autoload vs native)

Parquet/CSV preserve values and Arrow physical types. Warehouse engines infer types from
physical schema on autoload (e.g. JSON columns appear as `STRING` / `VARCHAR`). Native
target types (`JSON`, `VARIANT`, `UUID`, …) require a **materialization** step (cast SQL,
load schema, or typed view).

See [ADR-0014: Target type materialization](adr/0014-target-type-materialization.md) for
the full matrix (DuckDB, BigQuery, Snowflake, ClickHouse) and planned
`rivet check --type-report --target <engine>` extensions.

### Verified physical autoload (DuckDB + ClickHouse, v0.18.0)

`make test-types-validators` re-reads every PG / MySQL matrix column through
two independent engines and pins the autoload type. The full matrix:

| RivetType (PG / MySQL source) | DuckDB `DESCRIBE` | ClickHouse `DESCRIBE TABLE file()` |
|-------------------------------|--------------------|------------------------------------|
| `int16` / `smallint`          | `SMALLINT`         | `Nullable(Int16)`                  |
| `int32` / `integer`           | `INTEGER`          | `Nullable(Int32)`                  |
| `int64` / `bigint`            | `BIGINT`           | `Nullable(Int64)`                  |
| `u_int64` (MySQL `BIGINT UNSIGNED`) | `UBIGINT`    | `Nullable(UInt64)`                 |
| `decimal(p,s)`                | `DECIMAL(p,s)`     | `Nullable(Decimal(p, s))`          |
| `float32` / `real`            | `FLOAT`            | `Nullable(Float32)`                |
| `float64` / `double precision`| `DOUBLE`           | `Nullable(Float64)`                |
| `date`                        | `DATE`             | `Nullable(Date32)`                 |
| `time(µs)`                    | `TIME`             | `Nullable(DateTime64(6))`          |
| `timestamp(µs)` (naive)       | `TIMESTAMP`        | `Nullable(DateTime64(6))`          |
| `timestamp_tz(µs, UTC)`       | `TIMESTAMP WITH TIME ZONE` | `Nullable(DateTime64(6, 'UTC'))` |
| `string` / `text` / `enum` / `interval` | `VARCHAR` | `Nullable(String)` |
| `json` (PG `JSON`/`JSONB`, MySQL `JSON`) | `JSON` | `Nullable(String)` (ClickHouse 24.8) — DuckDB autoloads as native `JSON` |
| `uuid` (PG native; MySQL via override) | `UUID` | `Nullable(UUID)` through `rivet load` (a plain ClickHouse 24.8 Parquet read gives `Nullable(FixedString(16))`) — DuckDB autoloads as native `UUID` |
| `binary` (`bytea` / `BLOB`)   | `BLOB`             | `Nullable(String)` (raw bytes)     |
| `bool`                        | `BOOLEAN`          | `Nullable(Bool)`                   |
| `list<inner>`                 | `inner[]`          | `Array(Nullable(inner))`           |

A NULL list and an empty list are indistinguishable at BigQuery (a `REPEATED` field cannot hold a NULL list); `columns: {col: string}` keeps NULL. A ClickHouse `Array` cannot hold a NULL list either, so `rivet load` refuses a part holding one with `RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE` before creating a table; in a batch export read the column as `COALESCE(col, '{}')` to load `[]` on purpose, or as text to keep the NULL; a `mode: cdc` export has no `query:`, so give every source row a list and re-baseline the stream.

Round-trip values that the suite pins per row: `sum(decimal * 10^scale)`
matches the in-process Arrow check; `json_valid` / `isValidJSON` returns true
for every row; `TRY_CAST(uid AS UUID)` / `toUUID(uid)` parses every row; BLOB
columns compare byte-for-byte (`hex(...)`); the empty list survives; null
bitmaps propagate. The MySQL `BIGINT UNSIGNED` max value (`2^64 − 1`) is the
load-bearing assertion that exact-width unsigned ints are not silently
overflowed to i64.

### BigQuery autoload & recovery (verified live)

BigQuery's Parquet loader is weaker than DuckDB's: it ignores several Parquet
logical types on autoload and — critically — **will not coerce a column to a
different declared type on load**. A `bq load` into a table that declares
`JSON`/`DATETIME` is *rejected* (`Field x has changed type from JSON to BYTES`),
so native types are recovered with a **post-load transform, not a load schema**.

| RivetType | BigQuery autoload | Native | Recovery (post-load) |
|-----------|-------------------|--------|----------------------|
| `json`    | `BYTES`           | `JSON` | `PARSE_JSON(SAFE_CONVERT_BYTES_TO_STRING(col))` |
| `uuid`    | `BYTES` (16 raw)  | `BYTES` (BigQuery has no UUID type; `rivet load` keeps the bytes) | none — render text in a view: `TO_HEX(col)` |
| `timestamp` (naive) | `TIMESTAMP` (instant) | `DATETIME` | `DATETIME(col)` |
| `list<inner>` | `RECORD{item}` | `REPEATED inner` | load staging with `--parquet_enable_list_inference`, then `ARRAY(SELECT el.item FROM UNNEST(col) AS el)` |
| `u_int64` | `INT64` (overflows > 2^63−1) | `NUMERIC` | none post-load — fix at source: `columns: { c: decimal(20,0) }` |
| `timestamp_tz`, `decimal`, `string`, `binary`, `bool`, ints | native | same | — |

Rivet writes the Parquet list element as `item` (arrow-rs default, not the
spec's `element`), so even `--parquet_enable_list_inference` yields
`REPEATED RECORD{item}` rather than a clean `REPEATED <scalar>` — the `UNNEST`
flatten above is required.

`rivet check --type-report --target bigquery` prints the per-column autoload
type, the native type, and a ready-to-run recovery `CREATE TABLE … AS SELECT`
over the autoloaded `<table>__staging`. Set `exports[].target: bigquery` to get
it without the CLI flag. DuckDB needs none of this — it autoloads every logical
type natively.

### Snowflake autoload & recovery (verified live)

Snowflake's `INFER_SCHEMA` + `COPY` infers physical types only, so the same
semantic types degrade — and the `INFER_SCHEMA` column names come back
**lowercase and case-sensitive**, so the recovery `SELECT` must double-quote
every source reference (`"col"`).

| RivetType | Snowflake autoload | Native | Recovery (post-load) |
|-----------|--------------------|--------|----------------------|
| `json`    | `TEXT`             | `VARIANT` | `PARSE_JSON("col")` |
| `uuid`    | `BINARY` (16 raw)  | `TEXT` | `REGEXP_REPLACE(LOWER(HEX_ENCODE("col")), …)` → canonical UUID |
| `timestamp` (naive) | `NUMBER` (µs) | `TIMESTAMP_NTZ` | `TO_TIMESTAMP_NTZ("col", 6)` |
| `time`    | `NUMBER` (µs of day) | `TIME` | `TIME_FROM_PARTS(0,0,FLOOR("col"/1000000),MOD("col",1000000)*1000)` |
| `binary`  | `BINARY` *(needs `BINARY_AS_TEXT=FALSE`)* | `BINARY` | — (set the file-format option) |
| `timestamp_tz` | `TIMESTAMP_TZ` *(pin session `TIMEZONE='UTC'`)* | `TIMESTAMP_TZ` | — (autoload uses session offset otherwise) |
| `u_int64` | `NUMBER` (overflows > 2^63−1) | `NUMBER(20,0)` | none post-load — fix at source: `columns: { c: decimal(20,0) }` |
| `list<inner>` | `VARIANT` (the JSON array) | `ARRAY` | `"col"::ARRAY` |
| `decimal`, `string`, `bool`, `date`, ints | native | same | — |

The load preamble the recovery depends on: `CREATE FILE FORMAT … TYPE=PARQUET
BINARY_AS_TEXT=FALSE`, `ALTER SESSION SET TIMEZONE='UTC'`, `CREATE TABLE … USING
TEMPLATE (… INFER_SCHEMA …)`, `COPY … MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE`.

`rivet check --type-report --target snowflake` (`--target sf`) emits the
per-column autoload/native types and the post-load `CREATE OR REPLACE TABLE …`
recovery over `<table>__staging`. Set `exports[].target: snowflake` to skip the
flag.
