# Type contract, live: rivet load to BigQuery and Snowflake

Binary: `~/wt/n995/target/release/rivet`, `rivet 0.33.0 (5240ede7d)`. Built by another session; I did not build.
Diff check: `git diff 5240ede7..e84a0c50 -- src/load src/types` is EMPTY (no commits touch src/load or src/types in that range), so the binary measures origin/main for these paths.
Source: PostgreSQL 14 (`stand-pg14-batch-1`, 127.0.0.1:5514), table `tc_bqsf_pg` / `tc_bqsf_main`; MySQL 5.7 (`stand-mysql57-batch-1`, 127.0.0.1:3557), table `tc_bqsf_my`.
Destinations: BigQuery `rivet-data-tool.rivet_tmp_tc_bqsf` (location US, created and dropped by this run); Snowflake `RIVET_LOAD_TEST.DEVBOX` (tables dropped).
Oracle: source read by DuckDB 2.0.0 (postgres extension, ATTACH) and by the MySQL client; warehouse read by `bq query` (UNIX_MICROS, TO_HEX, CAST) and `snow sql` (EPOCH_MICROSECOND, TO_VARCHAR with FF6/FF9). rivet's own `integrity` line was not used as a grade.
Existing cells: `tests/type_roundtrip/bigquery_load.rs` and `snowflake_load.rs` (`bigquery_validates_*`, `snowflake_validates_*`) load Parquet via `bq load` / direct COPY, not `rivet load`, so they were not run. `dev/release_oracle/rig_oracle.py` grades warehouses but was not invoked (no cell run through it). Everything below is hand-run.

Commands (each row below cites the command pattern; the full runs used `rivet run -c X.yaml` then `rivet load -c X.yaml`):
- `rivet run -c bq_main.yaml` / `rivet load -c bq_main.yaml` (PG table, BQ): `integrity ✓ ... warehouse 5 rows`, `LOAD OK`, exit 0.
- `rivet load -c sf_main.yaml` (PG table, SF): `LOAD OK ... 5 row(s)`, exit 0.
- `rivet load -c bq.yaml` on the 15-column table with `c_num50`: `RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED`, exit 1.
- `rivet load -c sf50.yaml` / `bq50.yaml` (table `tc_bqsf_pg50`, id + numeric(50,10)): `RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED: 1 column(s) do not map to the warehouse: c_num50`, exit 1 on both.
- `rivet run -c my.yaml` with MySQL TIME `-838:59:58.999999`: `RIVET_SOURCE_VALUE_UNREPRESENTABLE` at extract (exit code not captured; the run stopped before any load).
- MySQL table `tc_bqsf_my` (after changing the TIME outlier to `23:59:59.999999`): `rivet run` exit 0, `rivet load` to BQ and SF exit 0, `warehouse 3 rows`.
- Note: a second `rivet load -c bq.yaml` after the PG table was split into two exports sharing a prefix returned exit 5 (`RIVET_LOAD_SOURCES_SHARED`). That run was an artefact of my setup, not a type result.

## PostgreSQL source, table tc_bqsf_main (5 rows: boundary rows 1-4, all-NULL row 5)

| Delivered type (rivet) | BigQuery column | Snowflake column | Values tried | BQ verdict | SF verdict | Evidence |
|---|---|---|---|---|---|---|
| int8 (Int64) | INTEGER | NUMBER(38,0) | min, max, 0, 1, NULL | EXACT | EXACT | bq query JSON rows 1-5; snow sql rows 1-5 |
| numeric(38,10) (Decimal128(38,10)) | BIGNUMERIC | NUMBER(38,10) | -9999..9999.9999999999 (28+10 digits), 1e-10, -0.5, NULL | EXACT | EXACT | CAST AS STRING equal to DuckDB source text |
| numeric(50,10) (Decimal256 / over 38) | not created | not created | 40-digit int part, 1.5, NULL | REFUSED `RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED` exit 1 | REFUSED `RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED` exit 1 | `rivet load -c bq50.yaml` / `sf50.yaml`. Extract succeeded (5 rows in parquet). DuckDB reads this column as DOUBLE, so an exact source compare was not possible. |
| float8 (Float64) | FLOAT | FLOAT | NaN, Infinity, -Infinity, 2.2250738585072014e-308, NULL | EXACT | EXACT | bq `c_f8`: NaN, Infinity, -Infinity, 2.2250738585072014E-308; snow: NaN, Infinity, -Infinity |
| date | DATE | DATE | 0001-01-01, 9999-12-31, 1969-12-31, 2000-02-29, NULL | EXACT | EXACT | CAST AS STRING |
| timestamp(6) | DATETIME | TIMESTAMP_NTZ | epoch+1us, 0001-01-01 00:00:00, 1969-12-31 23:59:59.999999, 2000-02-29 12:34:56.789012, NULL | EXACT | EXACT | bq CAST AS STRING; snow FF6 |
| timestamptz(6) | TIMESTAMP | TIMESTAMP_TZ | epoch+1us (1), 9999-12-31 23:59:59.999999 (253402300799999999), -1us, 2000-02-29 (951827696789012), NULL | EXACT | EXACT | UNIX_MICROS / EPOCH_MICROSECOND equal to DuckDB epoch_us. (`bq` JSON display shows `<date out of range for display>` for the 9999 row; the value itself is exact.) |
| time(6) | TIME | TIME | 00:00:00.000001, 23:59:59.999999, 12:00:00, 01:02:03.456789, NULL | EXACT | EXACT | bq CAST AS STRING; snow `FF9` shows `00:00:00.000001000`, `23:59:59.999999000` |
| interval | STRING (ISO 8601 text) | TEXT (ISO 8601) | `1 day 02:00:03.000004`, `-1 month`, `00:00:00`, `-1 year -9 months`, NULL | EXACT (as ISO text `P1DT2H3.000004S`, `P-1M`, `PT0S`, `P-1Y-9M`) | EXACT (same text) | Text form (iso8601_duration), not native INTERVAL; same value. |
| uuid | BYTES (16 bytes) | TEXT (32 uppercase hex, no dashes) | nil, ffffffff-..., 6ba7b810-9dad-11d1-80b4-00c04fd430c8, NULL | EXACT (TO_HEX equal to source hex) | EXACT value; text form differs from uuid36 target | bq TO_HEX; snow TO_VARCHAR. SF stores 32-hex text, not the uuid36 form the matrix target (TO_UUID) names. Value not lost. |
| jsonb | JSON | VARIANT | `{"a":1}`, `[1,"x"]`, JSON `null`, SQL NULL | EXACT for values; JSON `null` renders as `null`; SQL-NULL vs JSON-null not distinguished in BQ | EXACT (JSON `null` = VARIANT NULL_VALUE; SQL NULL = SQL NULL) | bq TO_JSON_STRING renders both SQL NULL and JSON null as `null`. IS NULL check on BQ not run: UNMEASURED for that split. |
| bytea | BYTES | BINARY | `\x00ff`, empty, `\xdeadbeef`, NULL | EXACT | EXACT | TO_HEX / TO_VARCHAR HEX equal |
| bigint[] (int8 array) | ARRAY<INT64> | ARRAY | `{}`, `{max,min}`, `{1,NULL}`, NULL array, NULL | EXACT for `{}`, `{max,min}`, `{1,NULL}`. NULL array -> `[]` DEFECT. | EXACT (NULL array -> SQL NULL) | bq ARRAY_LENGTH 0 where source is NULL (rows 4 and 5). snow `IA_NULL` true. |
| text[] | ARRAY<STRING> | ARRAY | `{}`, `{a,NULL}`, `{1,NULL}`-shaped text, NULL array, NULL | EXACT for non-NULL arrays. NULL array -> `[]` DEFECT. | EXACT (NULL array -> SQL NULL) | bq ARRAY_LENGTH 0 for NULL arrays (rows 3, 4, 5). snow `TA_NULL` true. |

## MySQL 5.7 source, table tc_bqsf_my (3 rows, row 3 all-NULL)

| Delivered type | BigQuery column | Snowflake column | Values tried | BQ verdict | SF verdict | Evidence |
|---|---|---|---|---|---|---|
| BIGINT UNSIGNED (UInt64) | NUMERIC | NUMBER(20,0) | 18446744073709551615 (u64 max), 9223372036854775808 (2^63), 9223372036854775807, NULL | EXACT | EXACT | bq CAST AS STRING = `18446744073709551615`; snow TO_VARCHAR equal |
| DECIMAL(38,0) | BIGNUMERIC | NUMBER(38,0) | 99999999999999999999999999999999999999 (38 digits), 0, NULL | EXACT | EXACT | string compare |
| TIME(6) | TIME | TIME | 00:00:00.000001, 23:59:59.999999, NULL; -838:59:58.999999 (negative MySQL TIME) | EXACT for in-range. Negative/out-of-range: REFUSED at extract `RIVET_SOURCE_VALUE_UNREPRESENTABLE` (code seen; exit code not captured) | EXACT for in-range | bq CAST AS STRING; snow FF6 |
| DATETIME(6) | DATETIME | TIMESTAMP_NTZ | 1970-01-01 00:00:00.000001, 1000-01-01 00:00:00.000000, NULL | EXACT | EXACT | string compare |

## Not measured (UNMEASURED)

- Nanosecond timestamp (`iso_timestamp_nanos`): none of the sources used here produces ns (PG and MySQL have at most µs). MSSQL datetime2(7) and Oracle TIMESTAMP(9) were not run. UNMEASURED.
- TIME(NANOS) (lead: BQ TIME over TIME(NANOS) loaded wrong): not reachable from these sources. UNMEASURED.
- Decimal256 above 38 digits (lead: DECIMAL(50,10) fell to STRING): measured today as REFUSED, not STRING. The BIGNUMERIC half-away rounding for >38 fractional digits was not reached. MySQL DECIMAL(39..65) was not run. UNMEASURED.
- Unsigned bigint >2^63-1 failing the whole load (lead): did NOT reproduce on rivet parts. Loaded as NUMERIC/NUMBER, exact.
- UUID FLBA16 loaded as raw bytes (lead): on rivet parts uuid lands as BYTES(16), correct bytes. Not reproduced as a defect.
- MSSQL and Oracle sources: not run. UNMEASURED.
- Clickhouse: out of scope.

## DEFECT list (ranked by data-loss risk)

1. **NULL list becomes empty list on BigQuery (silent NULL -> [])**. Both `bigint[]` and `text[]`, every NULL-array row. Actual: `[]` (ARRAY_LENGTH 0). Expected: NULL (Snowflake keeps NULL). BigQuery's ARRAY type cannot hold NULL, so the loader writes an empty array; this is a silent value change, not a refusal. Cause: not located in src/load/bigquery in this session (not traced to a line). No known_red entry found in dev/release_oracle/known_red.py for arrays; `gh search issues` for "BigQuery array NULL" returned no issues. Not commented on GitHub (search only).
2. **numeric(50,10) (Decimal256 / over 38 digits) is refused, not delivered**. Refusal is coded (`RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED`, exit 1), so not silent. It is still a support gap for a declared Decimal256 target. Cause site: src/load/mod.rs:621 (`cannot load ... column(s) do not map to the warehouse`), emitted by the column-type mapping; the BigQuery type map (src/types/target.rs:29 documents BIGNUMERIC precision 1-76, scale 0-38) does not map 39+ digits. Error code: src/error.rs:1235 (LOAD_COLUMN_TYPE_UNSUPPORTED).
3. **Snowflake uuid is stored as 32-hex uppercase text, not the uuid36 form** (value EXACT, form differs from the matrix target `TO_UUID(x)`). Low risk: no value lost; a text-form contract mismatch to record in the matrix. Cause not traced.
4. **JSON null vs SQL NULL is not distinguished on BigQuery** (unverified: TO_JSON_STRING renders both as `null`). Needs an `IS NULL` check; UNMEASURED for the split.

Nothing in this run was graded DEFECT on Snowflake.

## Cleanup

- PG (`stand-pg14-batch-1`): dropped `tc_bqsf_pg`, `tc_bqsf_pg50`, `tc_bqsf_main`.
- MySQL (`stand-mysql57-batch-1`): dropped `tc_bqsf_my`.
- BigQuery: dataset `rivet_tmp_tc_bqsf` created by this run and removed (`bq rm -r -f -d`); verified absent.
- Snowflake: dropped `RIVET_LOAD_TEST.DEVBOX.TC_BQSF_MAIN` and `TC_BQSF_MY`; `SHOW TABLES LIKE 'TC_BQSF%'` returns none.
- GCS (`gs://rivet_data_test/rivet-live/tc_bqsf_*`): removed all nine prefixes this run wrote; verified none remain.
- Worktree ~/wt/tc-bqsf: see handback for removal status.
