# Type contract draft (report-only, not committed)

- Base: `origin/main` e84a0c50 (detached worktree `~/wt/typecontract`). Branch `origin/docs/994-unrepresentable-values` (8e65de8a) read with `git show`; its "Values with no Arrow representation" section is not on main.
- Method: read-only. Ledger rows from `docs/type-capability-matrix.yaml`, code read at file:line on main, GitHub state via `gh`. No build, no `make live` run (the machine is loaded and a live stand run was not started). Lead items from the coordinator are re-checked in code where possible; live results are marked "lead, not re-run".
- Ranges: the Arrow and warehouse value ranges below are the published type definitions (Arrow `Schema.fbs`, the warehouse docs), not measured by this run. Cells that depend on them are marked **spec**.
- Legend: **OK** = satisfies points 1-4 on current code. **GAP** = no rule covers it. **DEFECT** = breaks a point (see C). **unverified** = no evidence read.
- Point 1: exact native type when the whole chain holds the value, otherwise canonical text. Point 2: a non-value (MySQL zero date) becomes NULL, counted per column, WARN. Point 3: a real out-of-range value is refused with a code naming the fix. Point 4: warehouse overflow is exact or refused at load with a code; never clamp.

---

## A. Delivery matrix (source native type -> delivered Arrow type)

Evidence key: `L:<engine>/<native_type>` = row in `docs/type-capability-matrix.yaml` (graded by `tests/offline/type_capability_matrix_guard.rs` and `tests/live/live_cdc_type_parity.rs`; the batch leg is graded by the same rows). "live" means the row is graded against the live stand in CI; this run did not re-execute it.

### A.1 PostgreSQL (36 ledger rows)

| Native type | Delivered Arrow | Arrow range (spec) | Source range | All source values fit | Out-of-range policy | Evidence |
|---|---|---|---|---|---|---|
| TEXT, VARCHAR(n), CHAR(n) | Utf8 | 2 GiB per value offset | text | yes | n/a | L:postgres/TEXT, /VARCHAR(50), /CHAR(8) |
| NUMERIC(18,2) / (18,4) | Decimal128(18,s) | 10^18 digits | same | yes | n/a | L:postgres/NUMERIC(18,2) |
| NUMERIC(60,10) | Decimal256(60,10) | 10^60 | same | yes | n/a | L:postgres/NUMERIC(60,10) (render duck `arrow`) |
| NUMERIC (no precision) | `decimal_plain` text (`Text(DecimalPlain)`) when profiled over 38 digits; else declared by `init` (CP11) | text | up to 131072 digits | yes as text | batch refuses today, CDC carries `server_text` (DEFECT, C-6) | L:postgres/NUMERIC |
| NUMERIC[] | `server_text` | text | n/a | yes as text | batch refuses today (DEFECT, C-6) | L:postgres/NUMERIC[] |
| MONEY | `server_text` | text | money | yes as text | batch refuses today (DEFECT, C-6) | L:postgres/MONEY |
| INET / CIDR | `inet_text` (addresses only; CIDR prefix not in the form) | text | inet/cidr | INET yes as text; CIDR prefix is in the text | batch refuses today (DEFECT, C-6) | L:postgres/INET, /CIDR |
| TIMESTAMP | Timestamp(µs) | µs, +-292k years (spec) | µs | yes | n/a | L:postgres/TIMESTAMP |
| TIMESTAMPTZ | Timestamp(µs, UTC) | same | µs | yes | n/a | L:postgres/TIMESTAMPTZ |
| TIMESTAMP[] / TIMESTAMPTZ[] | List(Timestamp) | n/a | µs | yes; NULL array loads as `[]` in ClickHouse (DEFECT, C-4) | L:postgres/TIMESTAMP[] |
| DATE | Date32 | days, +-5.8M years (spec) | 4713 BC .. 5874897 AD | yes except `infinity` | `infinity` or out-of-range: refused, `RIVET_SOURCE_VALUE_UNREPRESENTABLE` (exit 5), `src/source/postgres/arrow_convert.rs:133,174` | L:postgres/DATE; type-mapping.md "Values with no Arrow representation" |
| DATE[] | List(Date32) | n/a | same | as DATE | as DATE | L:postgres/DATE[] |
| TIME | Time64(µs) | 0..24h | µs | yes | n/a | L:postgres/TIME |
| TIME[] | List(Time64(µs)) | n/a | µs | yes; NULL array -> `[]` in ClickHouse (C-4) | L:postgres/TIME[] |
| INTERVAL | `iso8601_duration` text | text | months/days/µs | yes as text (canonical form) | n/a | L:postgres/INTERVAL |
| BOOLEAN | Boolean | 2 values | 2 values | yes | n/a | L:postgres/BOOLEAN |
| SMALLINT | Int16 | +-32767 (spec) | same | yes | n/a | L:postgres/SMALLINT |
| BIGINT | Int64 | +-2^63 | same | yes | n/a | L:postgres/BIGINT |
| INTEGER | Int32 | +-2^31 | same | yes | n/a | unverified (no ledger row for PG INTEGER; only INTEGER[] element) |
| REAL | Float32 | IEEE binary32 | same | yes | n/a | L:postgres/REAL |
| DOUBLE PRECISION | Float64 | IEEE binary64 | same | yes (NaN/Inf included) | n/a | L:postgres/DOUBLE PRECISION |
| BYTEA | Binary | 2 GiB | bytea | yes | n/a | L:postgres/BYTEA |
| BYTEA[] | List(Binary) | n/a | same | yes; NULL array -> `[]` in ClickHouse (C-4) | L:postgres/BYTEA[] |
| UUID | arrow.uuid (FixedSizeBinary 16) | 16 bytes | 128-bit | yes | n/a | L:postgres/UUID; ClickHouse leg C-5 |
| UUID[] | List(arrow.uuid) | n/a | same | yes; ClickHouse FixedString(16) and NULL array (C-4, C-5) | L:postgres/UUID[] |
| JSON, JSONB | arrow.json (Utf8 + JSON logical type) | text | text | yes | n/a | L:postgres/JSON, /JSONB |
| enum types (`rivet_status`) | Utf8 | text | label | yes | n/a | L:postgres/rivet_status |
| `NUMERIC` with `'NaN'` | (in NUMERIC row) | | | | Parquet decimal cannot hold NaN: fails loudly on batch and CDC (cdc.md:940) | docs/reference/cdc.md:938-942 |
| Composite, hstore, tsvector, multi-dim arrays, range, XML, geometric, bit varying, interval[] | not in the ledger | | | | GAP: unverified | - |

### A.2 MySQL (35 ledger rows)

| Native type | Delivered Arrow | Arrow range (spec) | Source range | All values fit | Out-of-range / bad-value policy | Evidence |
|---|---|---|---|---|---|---|
| VARCHAR(n), CHAR(n), TEXT, ENUM, SET | Utf8 | | | yes | n/a | L:mysql/VARCHAR(200), /TEXT, /ENUM, /SET |
| DECIMAL(18,2) / (18,4) | Decimal128(18,s) | 10^18 | same | yes | n/a | L:mysql/DECIMAL(18,2) |
| DATETIME(6) | Timestamp(µs) | | 1000-01-01..9999-12-31 | yes | zero or partial date: NULL + count + WARN (`src/source/mysql/arrow_convert.rs:229-250`, `:685-687`); point 2 met in code | L:mysql/DATETIME(6); docs type-mapping "Values with no Arrow representation" |
| TIMESTAMP(6) NULL, TIMESTAMP NULL | Timestamp(µs, UTC) | | 1970..2038 | yes | zero date: NULL + count + WARN; `validate`/`reconcile` do not see the NULLs (#994, OPEN) | L:mysql/TIMESTAMP(6) NULL |
| DATE | Date32 | | 1000-01-01..9999-12-31 | yes | zero or partial date: NULL + WARN (point 2) | L:mysql/DATE |
| TIME(6) | Time64(µs) | -838:59:59..838:59:59 (MySQL) | Time64 is 0..24h (spec) | NO: a MySQL TIME outside one day (`-838..838 h`) is outside Time64; refusal or NULL not verified | unverified (not in the ledger's sample) | L:mysql/TIME(6) (sample is in-day only) |
| YEAR | Int16 | 1901..2155 | same | yes | n/a | L:mysql/YEAR |
| BOOLEAN / TINYINT(1) | Boolean (native path) | 2 values | -128..127 | **NO**: 5 and -1 become `true` | silent value change; `src/source/mysql/arrow_convert.rs:302-305` (native_flag), `:753-757` (`*v != 0`); the ledger's own known_defect (C-1) | L:mysql/BOOLEAN (today_delivery Boolean, known_defect) |
| TINYINT | Int16 | -128..127 | same | yes | n/a | L:mysql/TINYINT |
| TINYINT UNSIGNED | Int16 | 0..255 | same | yes | n/a | type-mapping.md row `tinyint unsigned` (no ledger row) |
| SMALLINT | Int16 | | | yes | n/a | L:mysql/SMALLINT |
| MEDIUMINT | Int32 | | | yes | n/a | L:mysql/MEDIUMINT |
| INT | Int32 | | | yes | n/a | L:mysql/INT |
| INT UNSIGNED | Int64 | 0..2^32-1 fits | | yes | n/a | L:mysql/INT UNSIGNED |
| BIGINT | Int64 (not in ledger) | | | yes | n/a | unverified (no ledger row) |
| BIGINT UNSIGNED | UInt64 | 0..2^64-1 | same | yes in Arrow; NOT in BigQuery/Snowflake targets (see B) | value above 2^63-1 under a type override: NULL or changed value (#995, OPEN) | L:mysql/BIGINT UNSIGNED |
| FLOAT | Float32 | | | yes | n/a | L:mysql/FLOAT |
| DOUBLE | Float64 | | | yes | n/a | L:mysql/DOUBLE |
| BIT(1) | Boolean | | | yes | n/a | L:mysql/BIT(1) |
| BIT(8) | Int64 | | 0..255 | yes | n/a | L:mysql/BIT(8) |
| BIT(64) | Int64 | 0..2^63-1 | 0..2^64-1 | **NO** for values with the top bit set; batch under a type override NULLs or changes the value (#995, OPEN) | DEFECT (C-2) | L:mysql/BIT(64) (sample is 2^63-1 only, so the ledger does not exercise the gap) |
| BINARY(n), VARBINARY(n), BLOB | Binary | | | yes | n/a | L:mysql/BINARY(4), /VARBINARY(4), /BLOB |
| JSON | arrow.json | text | | yes | n/a | L:mysql/JSON |
| DECIMAL / NUMERIC with precision > 38 | Decimal256 only for `NUMERIC(60,10)`-class overrides; not in ledger for MySQL DECIMAL(65,0) | | DECIMAL(65,0) max | unverified | L:not present for MySQL |
| GEOMETRY, VECTOR, other spatial | not in the ledger | | | | GAP: type-mapping mentions them; no ledger row | - |

### A.3 SQL Server (21 ledger rows)

| Native type | Delivered Arrow | Arrow range (spec) | Source range | All values fit | Policy | Evidence |
|---|---|---|---|---|---|---|
| BIGINT | Int64 | | | yes | n/a | L:mssql/BIGINT |
| DECIMAL(18,4), NUMERIC(10,3) | Decimal128 | | | yes | n/a | L:mssql/DECIMAL(18,4), /NUMERIC(10,3) |
| MONEY, SMALLMONEY | Decimal128(19,4) | | 4 decimals, +-922337203685477.5807 | yes | n/a | L:mssql/MONEY, /SMALLMONEY |
| BIT | Boolean | | | yes | n/a | L:mssql/BIT |
| VARCHAR, NVARCHAR, CHAR, NCHAR | Utf8 | | | yes (NCHAR padding preserved, see render) | n/a | L:mssql/VARCHAR(50), /NVARCHAR(50), /CHAR(8), /NCHAR(8) |
| DATETIME2(7) | Timestamp(µs) delivered; ADR target Timestamp(ns) | ns | 100 ns | **NO**: the 100 ns digit is dropped | silent truncation, exit 0 (C-7) | L:mssql/DATETIME2 (today_delivery µs, known_defect) |
| DATETIMEOFFSET(7) | Timestamp(µs, UTC) delivered; target Timestamp(ns, UTC) | | 100 ns | **NO** | silent truncation (C-7) | L:mssql/DATETIMEOFFSET |
| TIME(7) | Time64(µs) delivered; target Time64(ns) | | 100 ns | **NO** | silent truncation (C-7) | L:mssql/TIME |
| DATETIME | Timestamp(µs) | 3.33 ms steps | | yes after round to micros (render `round_micros`) | ledger uses round_micros, which is a rounding (C-7 class) | L:mssql/DATETIME |
| SMALLDATETIME | Timestamp(µs) | minute | | yes | n/a | L:mssql/SMALLDATETIME |
| DATE | Date32 | | 0001..9999 | yes | n/a | L:mssql/DATE |
| UNIQUEIDENTIFIER | arrow.uuid | | | yes (byte order: see C-8) | unverified byte order | L:mssql/UNIQUEIDENTIFIER |
| BINARY(n), VARBINARY(n) | Binary | | | yes | n/a | L:mssql/BINARY(8), /VARBINARY(16), /VARBINARY(4) |
| FLOAT | Float64 | | | yes | n/a | L:mssql/FLOAT |
| sql_variant, hierarchyid, geometry, geography, xml, image, text, ntext | not in the ledger | | | | GAP: CP5 names ServerText for sql_variant and hierarchyid; no ledger row | - |

### A.4 Oracle (15 ledger rows)

| Native type | Delivered Arrow | Arrow range (spec) | Source range | All values fit | Policy | Evidence |
|---|---|---|---|---|---|---|
| NUMBER (no precision) | `decimal_plain` text | text | up to 40 digits, 1E-130..9.99E125 | yes as text | `1E125` to float4 is inf (#913); decimal_plain is the native path only when the profile fits (CP11) | L:oracle/NUMBER |
| NUMBER(38,10) | Decimal128(38,10) | 10^38 | same | yes | n/a | L:oracle/NUMBER(38,10) |
| NUMBER(9) | Int32 | +-2^31 | 9 digits | yes | n/a | L:oracle/NUMBER(9) |
| NUMBER(18) | Int64 | +-2^63 | 18 digits | yes | n/a | L:oracle/NUMBER(18) |
| BINARY_FLOAT | Float32 | | | yes | n/a | L:oracle/BINARY_FLOAT |
| BINARY_DOUBLE | Float64 | | | yes | n/a | L:oracle/BINARY_DOUBLE |
| DATE | Timestamp(µs) | | 0001..9999 with time | yes | n/a | L:oracle/DATE |
| TIMESTAMP(6) | Timestamp(µs) | | | yes | n/a | L:oracle/TIMESTAMP(6) |
| TIMESTAMP(9) | Timestamp(ns) (batch, `timestamp_ns` override only); undeclared: refused (RIVET_TYPE_UNSAFE_MAPPING); declared `timestamp`: delivered at µs | | 1 ns | no at µs | sub-µs digits dropped deliberately when declared (C-9) | L:oracle/TIMESTAMP(9) |
| TIMESTAMP WITH TIME ZONE, WITH LOCAL TIME ZONE | Timestamp(µs, UTC) | | | yes | n/a | L:oracle/TIMESTAMP(6) WITH [LOCAL] TIME ZONE |
| VARCHAR2, NVARCHAR2, CHAR | Utf8 | | | yes | n/a | L:oracle/VARCHAR2(100 CHAR), /NVARCHAR2(50), /CHAR(5 CHAR) |
| RAW(16) | Binary | | | yes | n/a | L:oracle/RAW(16) |
| BLOB, CLOB, NCLOB, LONG, LONG RAW, INTERVAL, XMLTYPE, BFILE, nested tables | not in the ledger | | | | GAP: LogMiner note in ADR-0038 covers BFILE/nested tables for CDC only; batch unverified | - |

### A.5 MongoDB

Ledger `mongo.rows` is `[]`. Batch exports a document blob. `_id` and `document` columns: an override is accepted and ignored (#915 OPEN, DEFECT C-10, `known_red` `no_silent_null[mongo]`). Per-type table: **unverified (no ledger rows)**.

### A.6 ClickHouse

Not a source in this repository. Its types appear only as warehouse targets (section B).

---

## B. Target matrix (delivered Arrow or text form -> warehouse)

Overflow column: "possible" means the delivered value range is wider than the landing type's range.

| Delivered | BigQuery (resolver `src/types/target.rs:~615-660`) | Snowflake (`target.rs:~820-850`) | ClickHouse (`target.rs:~980-990`; load guard `src/load/clickhouse.rs:1011-1100`) | DuckDB (`target.rs:~757`) | Parquet / CSV |
|---|---|---|---|---|---|
| Int16/Int32/Int64 | INT64; range +-2^63; no overflow | NUMBER(38,0); no overflow | native | native | Parquet native; CSV text: unverified |
| **UInt64** (BIGINT UNSIGNED) | **INT64 autoload, target NUMERIC; overflow possible above 2^63-1; status Warn only** (`target.rs:629-635`). Rivet does not refuse at load; the lead says BigQuery fails the whole load with no rivet code (lead, not re-run). **DEFECT C-3** | NUMBER(20,0) target; autoload NUMBER(38,0); overflow not possible for 2^64-1 (20 digits) — the Warn note says "UINT64 > INT64_MAX overflows the Parquet read" (`target.rs:835-840`); live assertion exists (`tests/live/live_snowflake_load.rs:321-324`, value 18446744073709551615) | native UInt64 (`target.rs:982-985`); exact | UBIGINT; exact | Parquet UINT_64 native; CSV text |
| Float32 / Float64 | FLOAT64 (Float32 widened: exact) | FLOAT (IEEE) | Float32/64 native | native | native |
| Decimal128(p,s) | NUMERIC for p<=29, s<=9; BIGNUMERIC beyond; precision above 38 digits **refused or cast**: unverified for every (p,s) | NUMBER(p,s), p<=38 | Decimal(p,s) native up to 76 | DECIMAL(p,s), up to 38 digits | native |
| Decimal256(60,10) | BIGNUMERIC (unverified) | refused: p>38 (`target.rs:~850`, guarded by the precision branch) — unverified exact refusal code | Decimal256 **wraps on overflow in arithmetic** (lead); column itself holds 60 digits | DuckDB reads as DOUBLE (lead) — render `arrow` in the ledger grades it through the Arrow display, not DuckDB SQL | native |
| `decimal_plain` (text) | `CAST(x AS BIGNUMERIC)` **rounds past 38 fractional digits** (ledger recover, `docs/type-capability-matrix.yaml` forms/decimal_plain) — **DEFECT C-6** | text (no 38-digit limit on text) | `toDecimal256(x,S)`, "no overflow check on arithmetic" (ledger) | text | text |
| Timestamp(µs) and Timestamp(µs, UTC) | TIMESTAMP / DATETIME; range 0001..9999 (spec); no overflow for µs | TIMESTAMP_NTZ / TZ(6) | DateTime64(6); the load guard refuses by footer stats outside 1900-01-01..2299-12-31 (`clickhouse.rs:1050-1100`) with `RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE`; **Date32 columns are not checked** (`partition_budget.rs:260-272` only checks Parquet Timestamp logical types) — **DEFECT C-4b** | native | native |
| Timestamp(ns) (`timestamp_ns`) | INT64 raw nanos (autoload); no native temporal; lossless as integer (`target.rs:~650-665`). Lead: TIMESTAMP NANOS autodetect -> INTEGER, TIME NANOS declared loads wrong (lead, not re-run) | TIMESTAMP_NTZ(9) (ledger iso_timestamp_nanos) | DateTime64(9) holds 1677..2262 only; load guard covers Timestamp logical types at 1900..2299 seconds (the guard is in seconds for DateTime64(6)): **gap for 1677..1900 and 2262..2299 at ns**: unverified | TIMESTAMP_NS | native |
| Time64(µs) | TIME (0..24h) | TIME | Decimal(18,6) (ledger: Time64 lands as Decimal) | TIME | native |
| Time64(ns) (SQL Server TIME(7) target) | INT64 or text: unverified | TIME(9)? unverified | Decimal(18,7) (ledger) | TIME (µs only): unverified | native |
| Binary | BYTES | BINARY | String (ledger mysql Binary -> String); holds bytes | BLOB | native |
| arrow.uuid | STRING (ledger uuid36 recover `text`) or NUMERIC: lead says a declared STRING over UUID FLBA16 loads raw bytes | `TO_UUID(x)` | UUID; **FixedString(16) bytes today** (ledger postgres/UUID, C-5) | UUID via `CAST(x AS UUID)` | native FLBA16 |
| arrow.json | JSON via `PARSE_JSON` (ledger) | `PARSE_JSON` | `CAST(x AS JSON)`; Nullable(String) today (ledger) | JSON | native |
| List(T) | ARRAY; NULL array -> []? unverified in BQ | ARRAY; unverified | Array(Nullable(T)); **NULL array becomes []** (ledger, all List rows) — **DEFECT C-4** | list / to_json text (ledger render) | native |
| iso8601_duration | `CAST(x AS INTERVAL)` | text | text | text | text |
| hex_wkb (geometry) | `ST_GEOGFROMWKB` | `TO_GEOMETRY` | `readWKB*` per kind | `ST_GeomFromHEXWKB` | unverified |
| inet_text | `NET.IP_FROM_STRING` (no CIDR) | `PARSE_IP(x,'INET')` | `toIPv6` (no CIDR) | `CAST AS INET` | text |
| server_text | text | text | text | text | text |

Parquet and CSV: Parquet holds the delivered Arrow type natively (CP1 chain). CSV is the rendered text; every value that is exact in Arrow is exact as text only if the CSV renderer is the canonical renderer. **Unverified** for CSV overflow (CSV has no overflow; the risk is rendering, C-11).

Overflow summary for B: **possible** and **not exact** for UInt64 on BigQuery (C-3), `decimal_plain` rounding on BigQuery (C-6), Date32 on ClickHouse (C-4b, gap), DateTime64(9) outside 1677..2262 (gap, unverified). **Exact** for ClickHouse UInt64 and Snowflake NUMBER(20,0).

---

## C. Violations of points 1-4 today (ranked by data-loss risk)

| # | Class (point) | Where | What happens | Tracked? (gh, this run) |
|---|---|---|---|---|
| C-1 | silent value change (1, 3) | MySQL TINYINT(1) native Boolean: `src/source/mysql/arrow_convert.rs:302-305`, `:753-757` (`*v != 0`), test `:1905` pins `u64::MAX -> true` | 5 and -1 land as `true` at exit 0, in batch, CDC and ClickHouse. Ledger row `mysql/BOOLEAN` known_defect; default TINYINT(1) is Boolean | No open issue found by `gh issue list --search "tinyint"`, `"boolean"` (0 hits; search syntax retried with `in:title`). #913 is about wildcard arms, not this path. **Untracked as far as searched** |
| C-2 | silent NULL or changed value (2, 3) | MySQL BIT(64), TIMESTAMP NULL, BIGINT UNSIGNED under a type override | NULL or changed at exit 0 | #995 OPEN |
| C-3 | refusal without a rivet code, or a whole-load failure (4) | BigQuery UInt64 > 2^63-1 (`src/types/target.rs:629-635`) | Warn note only; INT64 autoload. Lead: the BigQuery load fails the whole load (not re-run here). A load refusal with no code breaks point 4. No `RIVET_LOAD_*` code guards it | No open issue found by `gh issue list --search "unsigned"` (only #995 which is MySQL source side). **Untracked as far as searched** |
| C-4 | NULL becomes a value (1, 2: "NULL only for a source NULL") | ClickHouse: every `List(T)` row: NULL array loads as `[]` (ledger rows TEXT[], INTEGER[], DOUBLE PRECISION[], DATE[], TIMESTAMP[], TIMESTAMPTZ[], TIME[], BYTEA[], UUID[]), defect_samples NULL | NULL and empty merge in ClickHouse | #851 OPEN covers "ClickHouse UUID and NULL arrays" for PostgreSQL |
| C-4b | clamp (4) | ClickHouse Date32 columns: `src/load/clickhouse.rs:1050-1100` checks DateTime64 range only via `timestamp_outside` (`src/load/partition_budget.rs:266-268` only Parquet Timestamp logical type). Lead: ClickHouse 24.8 clamps a timestamp outside 1900..2299 silently on read and insert | A Date32 before 1900 or after 2299 may be clamped at the warehouse with exit 0 (lead for the clamp, code gap confirmed) | Not searched further; "clamp ClickHouse" search returned 0 hits. **Untracked as far as searched** |
| C-5 | wrong representation (3, a narrower or wrong type) | ClickHouse UUID: FixedString(16) bytes where native UUID exists (ledger postgres/UUID, mssql/UNIQUEIDENTIFIER). Lead: `reinterpretAsUUID` byte order wrong | exit 0 with different bytes | #851 OPEN |
| C-6 | rounding (3) | BigQuery `decimal_plain` recover `CAST(x AS BIGNUMERIC)` rounds past 38 fractional digits (ledger forms/decimal_plain, lead: half away from zero) | a value changes at the warehouse; the rivet side is text | No issue found by search; **untracked as far as searched**. Also CP11 says text is used only when the profile exceeds 38 digits, so this is the case it is for |
| C-7 | silent truncation (1, 3) | SQL Server DATETIME2(7), DATETIMEOFFSET(7), TIME(7) delivered at µs (ledger known_defect, `today_render` server LEFT(...,26)); DATETIME `round_micros` | 100 ns digit dropped at exit 0 | No open issue found by `gh` search "datetime2" / "nanosecond" (0 hits). **Untracked as far as searched**. ADR-0038 CP1 names this as the exact target, so it is a known gap, not a new finding |
| C-8 | byte order (3) | SQL Server UNIQUEIDENTIFIER into arrow.uuid: byte order versus the GUID text (C-5 neighbour) | unverified | unverified |
| C-9 | silent truncation by declaration (1, 3) | Oracle TIMESTAMP(9) declared `timestamp`: sub-µs digits dropped on purpose (ledger known_defect, `oracle/TIMESTAMP(9)`); undeclared: refused (RIVET_TYPE_UNSAFE_MAPPING) | exit 0 under the declared override | #912/#918 (check vs run), not the truncation itself. **Untracked as far as searched** |
| C-10 | override accepted and ignored (3) | MongoDB `_id` and `document` type override delivered as VARCHAR | exit 0 | #915 OPEN |
| C-11 | override that changes value at exit 0 (1, 3) | `NUMBER 1E125` to float4 becomes `inf`; integer to narrower decimal wraps; text or integer to bool; negative-scale decimal scaled the wrong way | exit 0, `fidelity: exact` | #913 OPEN |
| C-12 | refusal where a text form exists (1, 3) | PostgreSQL NUMERIC bare, MONEY, INET, CIDR, NUMERIC[]: batch refuses by column name; CDC carries `server_text` | batch refusal; CDC text labelled `server_text` (a labelled text, not exact on warehouse) | #851 OPEN (remaining C7 rows after #846) |
| C-13 | DuckDB DECIMAL(50,10) read as DOUBLE (4) | DuckDB 1.1 and 1.5.6, decimal max width 38 (lead, measured 2026-09-30, not re-run). Ledger handles Decimal256 with `render: duck: arrow` (grades the Arrow display) | a live DuckDB read of a warehouse-side DECIMAL(50,10) gives wrong values | Covered by CP11; no issue searched |
| C-14 | warehouse-side wrap (4, user query) | ClickHouse Decimal128 `*` and `sum()` wrap with `decimal_check_overflow=1` (lead) | a query result, not a delivered row; rivet not on the path | unverified |
| C-15 | silent NULL on a zero MySQL date (2) | MySQL zero date: NULL + count + WARN is code-compliant (`arrow_convert.rs:229-250`), but `validate` and `reconcile` do not count it | point 2 met in batch and CDC; the count is not in the summary's `validate` path | #994 OPEN |

Open issues checked with `gh issue view` (state at this run): #994 OPEN, #995 OPEN, #913 OPEN, #915 OPEN, #996 OPEN, #926 OPEN, #912 OPEN, #916 OPEN, #918 OPEN, #851 OPEN, #910 MERGED (`no_silent_null` matrix). Other 0.33.0 CHANGELOG known-issue numbers (#914, #919, #920-922, #963-965) were not re-checked.

Known-red and open-defect ledgers referenced: `dev/release_oracle/known_red.py` entries `no_silent_null[mysql|oracle|mssql|postgres|mongo] smoke|full` (WRONG_VALUE, SILENT_NULL, OVERRIDE_IGNORED, UNCODED_STOP, CHECK_RUN_SPLIT, INTEGRITY_BLIND), `no_silent_null_load[mysql|postgres|oracle|mssql]` (SILENT_NULL for MySQL TIMESTAMP 0000-00-00 read back NULL), `no_silent_null_cdc[*]`. `dev/release_oracle/open_defects.py` lists `open_defect_` cells; their entries were not enumerated in this run.

---

## D. Doc and ADR contradictions with points 1-4

| Doc:line | Says | Contradicts | Amendment (one line) |
|---|---|---|---|
| `docs/adr/0038-column-plan.md` CP1 | "A NULL is written only for a source NULL." | point 2 and MySQL zero date NULL (`docs/type-mapping.md`, branch 994 section) | Replace the sentence with: "A NULL is written only for a source NULL, or for a source value that is not a value of its declared type (MySQL zero or partial date), which is counted per column and WARNed." |
| `docs/adr/0038-column-plan.md` CP1 | "A value is never rounded, clamped or dropped without a refusal." | C-6 (BigQuery BIGNUMERIC rounding), C-7 (SQL Server µs), C-9 (Oracle TIMESTAMP(9) declared), C-4b (ClickHouse clamp) | Add: "Known open exceptions, each with an issue: #913, #995 and the SQL Server `datetime2`/`time` µs rows of the ledger; they close to a refusal or exact delivery in 0.34." |
| `docs/adr/0040-invariants-by-construction.md` I2 | "A non-NULL source value is delivered equal, or refused with a code" | `known_red` `no_silent_null[*]` SILENT_NULL / WRONG_VALUE; C-1 TINYINT(1); #913 | Append to I2: "Status: not yet true on main (`no_silent_null` red cells, #910 merged, #913 and #995 open); the MySQL zero-date NULL is the one allowed NULL that is not a source NULL, counted per column with a WARN." |
| `docs/reference/cdc.md:942` | "a value unrepresentable in the declared column (PostgreSQL `'NaN'::numeric` in a Parquet decimal) fails loudly on both paths, never a silent NULL." | MySQL zero date is NULL on CDC too (type-mapping branch 994; ADR-0038 point 2) | Add: "A MySQL zero or partial date is the one exception: it is NULL on both paths, counted per column and WARNed, and it is not a silent NULL." |
| `docs/guarantee-matrix.yaml:819-822` (`a_lossy_or_unmapped_type_fails_loud`) | "a lossy or unmapped type is named by `rivet check` and aborts the run, never quietly truncated." | C-1 (TINYINT(1) Boolean delivers 5 as true), C-7 (SQL Server datetime2 truncated, exit 0), C-9 (Oracle declared TIMESTAMP(9) truncated) | Change the sentence to: "A lossy type is named by `rivet check` and aborts the run unless the column is declared with `columns:` (the declaration is the operator's choice); the µs truncations of SQL Server and Oracle are the known exceptions in the ledger until 0.34." |
| `docs/type-mapping.md` "UUID and JSON keep native types" (`guarantee-matrix.yaml` `uuid_and_json_keep_native_types`) | Parquet gets native UUID | Holds at Parquet; ClickHouse lands FixedString(16) (C-5) | Add: "Native at Parquet; ClickHouse holds it as FixedString(16) until #851 closes." |
| `docs/type-mapping.md` "Values with no Arrow representation" (branch 994, not on main) | "a date or instant the target type cannot hold ... nothing: the run is refused" | Clickhouse Date32 has no range check (C-4b) | Add the Date32 row, or state "Date32 pre-1900 is not checked by the ClickHouse load guard (unverified)." |

---

## E. Keeping the matrix as the single contract

Generate sections A and B from `docs/type-capability-matrix.yaml` instead of writing them by hand.

Fields the ledger needs to add per row (source side): `source_range` (boundary samples: min, max, the one above max), `arrow_range` (per delivered type), `fits` (computed or asserted), `out_of_range` (one of `null_counted`, `refuse:<CODE>`, `exact`, `gap`). Per target, add `targets:` keyed by warehouse: `landing` (warehouse type), `landing_range`, `overflow` (`impossible|possible`), `on_overflow` (`exact|refuse:<CODE>|clamp|wrap|gap`). A `clamp` or `wrap` value is a DEFECT, graded to fail the guard.

Grading: a guard test in `tests/offline/type_capability_matrix_guard.rs` fails when a row lacks any of these fields, when `overflow: possible` has no `on_overflow` refusal code, or when `on_overflow` is `clamp|wrap`. The live leg writes the boundary samples through the existing parity test and asserts the exact or refused outcome per warehouse. Sections A and B are then rendered from the YAML by a small script, and the release gate diffs the rendered file against the committed one.

---

## Unverified cells (summary)

- Every live result: no `make live` run in this session. C-3 (BigQuery UInt64), C-4b (ClickHouse Date32 clamp), C-6 (BigQuery rounding), C-13 (DuckDB DECIMAL(50,10)), C-14 (ClickHouse decimal wrap), and the lead items BigQuery TIME/TIMESTAMP NANOS, UUID FLBA16, autodetect types, DECIMAL(50,10) falls to STRING, BIGNUMERIC rounding: all lead, not re-run.
- Snowflake: not probed by the coordinator and not re-checked beyond the resolver; every Snowflake overflow cell is unverified except UInt64 (NUMBER(20,0), live test present but not run here).
- Missing from the ledger entirely (no delivery row): PostgreSQL composite, hstore, tsvector, range, XML, geometric, bit varying, interval[]; MySQL GEOMETRY, VECTOR, BIGINT (signed), TINYINT UNSIGNED, DECIMAL(65,0); Oracle CLOB, BLOB, NCLOB, LONG, INTERVAL, XMLTYPE, BFILE; SQL Server sql_variant, hierarchyid, geometry, geography, xml, image, text, ntext; MongoDB (no rows at all).
- Arrow and warehouse ranges: taken from the type definitions, not measured.
- MySQL TIME outside one day: unverified (not in the ledger sample).
- CSV rendering exactness: unverified.
- SQL Server UUID byte order: unverified.
- The "validate does not count zero dates" claim: taken from the 0.33.0 CHANGELOG and #994, not re-run.
