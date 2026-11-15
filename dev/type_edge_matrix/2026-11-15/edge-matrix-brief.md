# Edge-value type matrix: shared brief

Goal: for ONE source engine (named in your prompt), every native column type the engine has, with its edge values, extracted by `rivet run` to Parquet and loaded by `rivet load` into EVERY warehouse target rivet supports (BigQuery, Snowflake, ClickHouse local, DuckDB; check `docs/` and `src/load/` for the exact target list and config keys). Every delivered value is graded against the source. This is the product's type contract; the owner reads the result cell by cell.

## The contract being tested (owner decision 2026-11-15)
- Each native type lands in the closest Arrow/Parquet type, or a wider native one that holds every value of the source type.
- A source value that is not a value of its type (MySQL `0000-00-00`) -> NULL plus a per-column WARN. Documented exception.
- A real value outside what the chosen Arrow type holds -> coded refusal, never a silent change.
- A warehouse that cannot hold a value -> exact, or refused at load with a code. Never clamped, wrapped, rounded or turned into NULL/[].
- Already known: MySQL BIT(64) with the top bit set refuses the WHOLE RUN (`value 18446744073709551615 overflows bit column`, RIVET_SOURCE_VALUE_UNREPRESENTABLE) because `bit(n>1)` maps to Int64 although Arrow UInt64 holds it. Grade that class as DEFECT_NATIVE_REFUSED.

## Edge values: every type gets ALL that apply
NULL; zero / empty string / empty binary / empty array; type minimum; type maximum; one step inside each bound (max-1, min+1); for unsigned: 2^63-1, 2^63, 2^64-1 where the width allows; for decimals: max precision at max scale, max precision at scale 0, smallest non-zero (1e-scale), negative max; for floats: NaN, +Inf, -Inf, -0.0, smallest subnormal, smallest normal, max finite; for strings: max declared length, multi-byte UTF-8 (4-byte emoji), embedded NUL if the engine allows, leading/trailing spaces (CHAR padding); for binary: all 256 byte values, max length; for dates/times: engine minimum and maximum, 1970-01-01, 1969-12-31 23:59:59.999999, 1677-09-21 / 2262-04-11 (ns limits), 1900-01-01 / 2299-12-31 (ClickHouse Date32/DateTime64 limits), 9999-12-31, leap day, max fractional precision the type has, the engine's special values (PG infinity/-infinity, MySQL zero/partial dates with sql_mode relaxed, TIME > 24h and negative TIME); for intervals: max and min, mixed sign; for json: null literal vs SQL NULL, deep nesting, big numbers (2^64, 1e400); for uuid: nil and max; for arrays: NULL array vs empty array vs array with NULL element; for enum/set/bit/geo/xml/money/etc.: min, max, empty.
Default mapping first (no `columns:` override). Overrides are out of scope unless the default refuses a native value.

## How
- Binary: use `~/wt/n995/target/release/rivet` (rivet 0.33.0, main 5240ede7). Do NOT build. Do not modify ~/wt/n995 (no git commands, no files) — only execute the binary.
- Work in your own scratch directory under /private/tmp/claude-501/-Users-andriipanchenko-rivet/e8e83f19-32d1-4d33-95da-8f865eb7d0f9/scratchpad/edge-<engine>/. No worktree needed; no commits, no PRs, no GitHub comments or issues.
- Sources: the stand in dev/stand/docker-compose.yaml (read it from ~/wt/n995; credentials rivet/rivet unless the file says otherwise). Every version the stand runs for your engine. Never stop, recreate or down a container. Create only tables named `tc_edge_<engine>_*`; drop them at the end.
- Targets: BigQuery project `rivet-data-tool`, location US, dataset `rivet_tmp_edge_<engine>` (create, then `bq rm -r -f -d` at the end; nothing else in BigQuery may be touched — `rivet_e2e` is permanent and off-limits). GCS bucket `rivet_data_test`, prefix `rivet-live/rivet_tmp_edge_<engine>/` (bucket `rivet-live` does not exist) (remove at the end). Snowflake `RIVET_LOAD_TEST.DEVBOX`, tables `TC_EDGE_<ENGINE>_*` (drop at the end). ClickHouse: the local stand container, database or tables named `tc_edge_<engine>*` (drop at the end). DuckDB: a file in your scratch dir. The previous BQ/SF run's exact config shapes are in scratchpad/type-contract-bq-sf.md.
- Use full paths: /opt/homebrew/bin/bq, /opt/homebrew/bin/snow, /opt/homebrew/bin/gsutil (the rtk wrapper mangles output). ClickHouse via `docker exec <container> clickhouse-client`.
- Oracle for the grade: read the source independently (DuckDB ATTACH for PG/MySQL; the engine's own client for MSSQL/Oracle), read the Parquet with DuckDB (`parquet_schema` + values), read each warehouse with its own client, casting to an exact text form (UNIX_MICROS, TO_HEX, CAST AS STRING, FF9 etc.). Never use rivet's own `integrity`/validate line as the grade.
- If one value refuses the whole run, split it into its own table so the other values of the type still get measured, and record the refusal as its own cell.
- Do not start a second agent. Keep heavy work sequential.

## Verdicts (one per cell = engine version x native type x edge value x stage)
EXACT; NULL_NONVALUE (documented non-value -> NULL); REFUSED(code, stage: extract|load, exit code) — acceptable only if the target type cannot hold the value; DEFECT_NATIVE_REFUSED (refused although a native Arrow/Parquet/warehouse type holds it); DEFECT_SILENT_NULL; DEFECT_WRONG_VALUE (clamped, wrapped, rounded, truncated precision, NULL->[], text-form change counts as FORM_ONLY if the value is intact); UNMEASURED(reason).

## Output
Write scratchpad/edge-matrix-<engine>.md:
1. One table per native type: rows = edge values, columns = source value | Parquet physical/logical type + value | BigQuery | Snowflake | ClickHouse | DuckDB. Each cell: verdict + delivered value when not EXACT.
2. A DEFECT list ranked by data-loss risk (silent first), each with the exact command, the delivered vs source value, and the src file:line of the mapping when found.
3. Commands used, and a cleanup list (everything created, everything dropped, verified absent).
Hand back a short summary: counts per verdict per target, the DEFECT list, cleanup confirmation.

## Added facts (2026-11-15, from an earlier agent)
- ClickHouse target: container `rivet-clickhouse` (24.8, UTC) at 127.0.0.1:8123. Load config shape: top-level `load: {target: clickhouse, url, database, user, password_env, pk}` (see ~/wt/n995/tests/common/rig/render.rs). Use only `tc_edge_<engine>` databases there.
- ClickHouse code to read first: src/types/target.rs `mod clickhouse` (UUID -> FixedString(16), JSON -> String, TIME -> Decimal64, NULL list -> []) and src/load/clickhouse.rs `refuse_unholdable_timestamps` (DateTime64 1900..2299).
- Accepted reds already in the ledger: dev/release_oracle/known_red.py lines ~187-199 (no_silent_null_load per engine). Note when a cell you measure matches one.
