# SQL Server edge-value type matrix

rivet 0.33.0 (`~/wt/n995/target/release/rivet`), stands mssql2019-batch (1519) and mssql2022-batch (1522). Work dir: `/private/tmp/claude-501/-Users-andriipanchenko-rivet/e8e83f19-32d1-4d33-95da-8f865eb7d0f9/scratchpad/edge-mssql/`. Per-cell data: `srcver.json` (source vs Parquet), `rec_bq.json`, `rec_sf.json`, `rec_ch.json` (warehouse vs Parquet).

Result in one line: every loaded warehouse cell equals the Parquet cell, so the defects are all at the extract stage or at a refusal. The 2022 Parquet is identical to 2019 for all 14 tables, and the refusals are identical on 2022.

## Ranked DEFECT list

1. DEFECT_NATIVE_REFUSED, money/smallmoney at or above about 9.007e11 (2^53/10^4): the extract is refused with RIVET_SOURCE_VALUE_UNREPRESENTABLE, exit 5, stage extract. tiberius decodes MONEY as f64 and `f64_to_scaled_i128` refuses past 2^53 (`src/source/mssql/arrow_convert.rs` ~728-745; mapping 175-182). Measured: 900719925474.0991 loads exact, 1000000000000.0001 and 922337203685477.5807 refuse; -922337203685477.5808 refuses. Valid money range is up to 9.22e14, so about 99.9% of the declared range is refused. Refusal is loud, not silent.
2. DEFECT_NATIVE_REFUSED, xml: refused at extract, exit 1, uncoded ("no Rivet mapping yet", `arrow_convert.rs:213`). Not listed as unsupported in docs/type-mapping.md.
3. DEFECT_NATIVE_REFUSED, lone UTF-16 surrogate in nchar, nvarchar, nvarchar(max), ntext: extract fails at the driver, exit 1, no RIVET_ code. Valid surrogate pairs (emoji) are EXACT.
4. DEFECT_WRONG_VALUE, 100 ns digit: time(7), datetime2(7), datetimeoffset(7) are delivered at microseconds, the 7th fractional digit is truncated (5 time cells, 7 datetime2 cells, 5 datetimeoffset cells). E.g. 23:59:59.9999999 becomes 23:59:59.999999, 00:00:00.0000001 becomes 00:00:00. `arrow_convert.rs` ~196-210. Known as documented gap 4 and `known_defect` in the ledger; the `timestamp_ns` override exists. Truncation, not rounding. Silent. Scales 0..6 are EXACT.
5. Snowflake, decimal(38,38): CREATE TABLE fails (NUMBER scale max 37), RIVET_LOAD_WAREHOUSE_REJECTED, and the whole load run aborts (other tables in the config not loaded in that run). `src/types/target.rs:391`. BigQuery maps it to BIGNUMERIC and ClickHouse to Decimal(38, 38), both exact.
6. ClickHouse, dates outside 1900..2299 (date 0001-01-01 and 9999-12-31, datetime, smalldatetime, datetime2, datetimeoffset at the extremes): the whole table is refused, RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, exit 5, stage load (`src/load/clickhouse.rs` 1075 `refuse_unholdable_timestamps`; call sites 307, 1028). Coded and loud. The in-range subset (tables dateB, dt2B, dtoB) loaded and graded exact.

FORM_ONLY (the value is right, the form differs from the source):
- datetime 1/300 s ticks (.003, .007) are rounded to microseconds (5 cells); ledger canon `round_micros`.
- datetimeoffset keeps the instant, not the offset: values are normalised to UTC (38 cells with a non-UTC offset).
- sql_variant is delivered as text: the base type is lost (24 cells).
- ClickHouse: uniqueidentifier lands as FixedString(16) (known ledger defect), time as Decimal(18, 6) seconds, tinyint as Int16 (Parquet INT_16).
- hierarchyid, geometry, geography are delivered as hex text of the CLR bytes (documented in type-mapping.md 313-314).
- smallmoney is delivered as Decimal(19, 4); docs/type-mapping.md 302-303 says decimal(10, 4) (stale doc).
- BigQuery: uuid lands as BYTES, decimal(38,x) as BIGNUMERIC.

No overlap with the accepted entries in dev/release_oracle/known_red.py (those are override-pair reds only, lines ~65-72, 111-119, 156-160).

## Verdict counts

Cells are non-edge-id cells (edge row x column). NULL_NONVALUE means the source holds NULL and the target holds NULL.

| target | EXACT | FORM_ONLY | DEFECT_WRONG_VALUE | NULL_NONVALUE | REFUSED | DEFECT_SILENT_NULL |
|---|---|---|---|---|---|---|
| Parquet / DuckDB | 379 | 67 | 17 | 94 | money x3 cells, xml, 4 surrogate cells (see list) | 0 |
| BigQuery | 379 | 67 | 17 | 94 | as Parquet | 0 |
| Snowflake | 374 | 67 | 17 | 92 | as Parquet, plus the decimal(38,38) column (7 cells, load abort) | 0 |
| ClickHouse | 292 | 49 | 12 | 90 | as Parquet, plus the out-of-range tables (graded on the in-range subset only) | 0 |

DEFECT_SILENT_NULL: none found. UNMEASURED: none. Warehouse cells not graded are the refused ones.

## Commands

- Source: `docker exec -i stand-mssql2019-batch-1 /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -d rivet -W ...` (`q.sh`); tables from `setup.sql`, `setup2.sql`, `setup3.sql`, `setupB.sql` (all literals as inserted).
- Config: `rivet init --source-url-file url19 ...` generated, only the prefix edited; `rivet run -c c.yaml` then `rivet load` per warehouse (logs `*/run.log`, `*/load.log`).
- Grading: `srcver.py` (source vs Parquet via DuckDB `read_parquet`), `rbq.py`, `rsf.py`, `rch.py` (warehouse vs Parquet), `bqchk.py`, `sfchk.py`, `chchk.py`.
- Observed Parquet types: `duckdb -c "select * from parquet_schema('<file>')"`.

## Observed delivery

bit Boolean; tinyint, smallint Int16; int Int32; bigint Int64; decimal(p,s) Decimal128(p,s); money, smallmoney Decimal128(19,4); real Float32; float Float64; char..ntext Utf8; binary..image, rowversion Binary; date Date32; datetime, smalldatetime, datetime2(n) Timestamp(us); datetimeoffset(n) Timestamp(us, UTC); time(n) Time64(us); uniqueidentifier uuid (FixedSizeBinary 16); sql_variant, hierarchyid, geometry, geography Utf8.

## Cleanup

Dropped and absence verified: source `tc_edge_mssql_*` tables on both stands (count 0 on 2019 and 2022); BigQuery dataset `rivet_tmp_edge_mssql` (`bq ls -a` count 0); Snowflake `RIVET_LOAD_TEST.DEVBOX.TC_EDGE_MSSQL_*` (14 dropped, information_schema count 0); ClickHouse database `tc_edge_mssql` (count 0); GCS `gs://rivet_data_test/rivet-live/rivet_tmp_edge_mssql/` (ls: no objects). Local Parquet and state DBs remain only under the scratchpad work dir.

# Per-cell tables

Columns: edge id|column, verdict, source value, Parquet value, BQ, SF, CH (=P equal to Parquet, - not loaded), DuckDB (the Parquet value).

## int: bit, tinyint, smallint, int, bigint

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|c_bit | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_tiny | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_small | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_int | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_big | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|c_bit | EXACT | 0 | false | =P | =P | =P | false |
| 2|c_tiny | EXACT | 0 | 0 | =P | =P | =P | 0 |
| 2|c_small | EXACT | -32768 | -32768 | =P | =P | =P | -32768 |
| 2|c_int | EXACT | -2147483648 | -2147483648 | =P | =P | =P | -2147483648 |
| 2|c_big | EXACT | -9223372036854775808 | -9223372036854775808 | =P | =P | =P | -9223372036854775808 |
| 3|c_bit | EXACT | 1 | true | =P | =P | =P | true |
| 3|c_tiny | EXACT | 1 | 1 | =P | =P | =P | 1 |
| 3|c_small | EXACT | -32767 | -32767 | =P | =P | =P | -32767 |
| 3|c_int | EXACT | -2147483647 | -2147483647 | =P | =P | =P | -2147483647 |
| 3|c_big | EXACT | -9223372036854775807 | -9223372036854775807 | =P | =P | =P | -9223372036854775807 |
| 4|c_bit | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 4|c_tiny | EXACT | 254 | 254 | =P | =P | =P | 254 |
| 4|c_small | EXACT | 0 | 0 | =P | =P | =P | 0 |
| 4|c_int | EXACT | 0 | 0 | =P | =P | =P | 0 |
| 4|c_big | EXACT | 0 | 0 | =P | =P | =P | 0 |
| 5|c_bit | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 5|c_tiny | EXACT | 255 | 255 | =P | =P | =P | 255 |
| 5|c_small | EXACT | 32766 | 32766 | =P | =P | =P | 32766 |
| 5|c_int | EXACT | 2147483646 | 2147483646 | =P | =P | =P | 2147483646 |
| 5|c_big | EXACT | 9223372036854775806 | 9223372036854775806 | =P | =P | =P | 9223372036854775806 |
| 6|c_bit | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 6|c_tiny | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 6|c_small | EXACT | 32767 | 32767 | =P | =P | =P | 32767 |
| 6|c_int | EXACT | 2147483647 | 2147483647 | =P | =P | =P | 2147483647 |
| 6|c_big | EXACT | 9223372036854775807 | 9223372036854775807 | =P | =P | =P | 9223372036854775807 |

## dec: decimal/numeric, money, smallmoney

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|d38_0 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|d38_38 | NULL_NONVALUE | NULL | NULL | =P | - | =P | NULL |
| 1|d38_10 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|d1_0 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|d18_6 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|n10_2 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_money | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_smallmoney | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|d38_0 | EXACT | 0 | 0 | =P | =P | =P | 0 |
| 2|d38_38 | EXACT | 0.00000000000000000000000000000000000000 | .00000000000000000000000000000000000000 | =P | - | =P | .00000000000000000000000000000000000000 |
| 2|d38_10 | EXACT | 0.0000000000 | 0.0000000000 | =P | =P | =P | 0.0000000000 |
| 2|d1_0 | EXACT | 0 | 0 | =P | =P | =P | 0 |
| 2|d18_6 | EXACT | 0.000000 | 0.000000 | =P | =P | =P | 0.000000 |
| 2|n10_2 | EXACT | 0.00 | 0.00 | =P | =P | =P | 0.00 |
| 2|c_money | EXACT | 0.0000 | 0.0000 | =P | =P | =P | 0.0000 |
| 2|c_smallmoney | EXACT | 0.0000 | 0.0000 | =P | =P | =P | 0.0000 |
| 3|d38_0 | EXACT | 99999999999999999999999999999999999999 | 99999999999999999999999999999999999999 | =P | =P | =P | 99999999999999999999999999999999999999 |
| 3|d38_38 | EXACT | 0.99999999999999999999999999999999999999 | .99999999999999999999999999999999999999 | =P | - | =P | .99999999999999999999999999999999999999 |
| 3|d38_10 | EXACT | 9999999999999999999999999999.9999999999 | 9999999999999999999999999999.9999999999 | =P | =P | =P | 9999999999999999999999999999.9999999999 |
| 3|d1_0 | EXACT | 9 | 9 | =P | =P | =P | 9 |
| 3|d18_6 | EXACT | 999999999999.999999 | 999999999999.999999 | =P | =P | =P | 999999999999.999999 |
| 3|n10_2 | EXACT | 99999999.99 | 99999999.99 | =P | =P | =P | 99999999.99 |
| 3|c_money | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 3|c_smallmoney | EXACT | 214748.3647 | 214748.3647 | =P | =P | =P | 214748.3647 |
| 4|d38_0 | EXACT | -99999999999999999999999999999999999999 | -99999999999999999999999999999999999999 | =P | =P | =P | -99999999999999999999999999999999999999 |
| 4|d38_38 | EXACT | -0.99999999999999999999999999999999999999 | -.99999999999999999999999999999999999999 | =P | - | =P | -.99999999999999999999999999999999999999 |
| 4|d38_10 | EXACT | -9999999999999999999999999999.9999999999 | -9999999999999999999999999999.9999999999 | =P | =P | =P | -9999999999999999999999999999.9999999999 |
| 4|d1_0 | EXACT | -9 | -9 | =P | =P | =P | -9 |
| 4|d18_6 | EXACT | -999999999999.999999 | -999999999999.999999 | =P | =P | =P | -999999999999.999999 |
| 4|n10_2 | EXACT | -99999999.99 | -99999999.99 | =P | =P | =P | -99999999.99 |
| 4|c_money | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 4|c_smallmoney | EXACT | -214748.3648 | -214748.3648 | =P | =P | =P | -214748.3648 |
| 5|d38_0 | EXACT | 1 | 1 | =P | =P | =P | 1 |
| 5|d38_38 | EXACT | 0.00000000000000000000000000000000000001 | .00000000000000000000000000000000000001 | =P | - | =P | .00000000000000000000000000000000000001 |
| 5|d38_10 | EXACT | 0.0000000001 | 0.0000000001 | =P | =P | =P | 0.0000000001 |
| 5|d1_0 | EXACT | 1 | 1 | =P | =P | =P | 1 |
| 5|d18_6 | EXACT | 0.000001 | 0.000001 | =P | =P | =P | 0.000001 |
| 5|n10_2 | EXACT | 0.01 | 0.01 | =P | =P | =P | 0.01 |
| 5|c_money | EXACT | 0.0001 | 0.0001 | =P | =P | =P | 0.0001 |
| 5|c_smallmoney | EXACT | 0.0001 | 0.0001 | =P | =P | =P | 0.0001 |
| 6|d38_0 | EXACT | -1 | -1 | =P | =P | =P | -1 |
| 6|d38_38 | EXACT | -0.00000000000000000000000000000000000001 | -.00000000000000000000000000000000000001 | =P | - | =P | -.00000000000000000000000000000000000001 |
| 6|d38_10 | EXACT | -0.0000000001 | -0.0000000001 | =P | =P | =P | -0.0000000001 |
| 6|d1_0 | EXACT | -1 | -1 | =P | =P | =P | -1 |
| 6|d18_6 | EXACT | -0.000001 | -0.000001 | =P | =P | =P | -0.000001 |
| 6|n10_2 | EXACT | -0.01 | -0.01 | =P | =P | =P | -0.01 |
| 6|c_money | EXACT | -0.0001 | -0.0001 | =P | =P | =P | -0.0001 |
| 6|c_smallmoney | EXACT | -0.0001 | -0.0001 | =P | =P | =P | -0.0001 |
| 7|d38_0 | EXACT | 99999999999999999999999999999999999998 | 99999999999999999999999999999999999998 | =P | =P | =P | 99999999999999999999999999999999999998 |
| 7|d38_38 | NULL_NONVALUE | NULL | NULL | =P | - | =P | NULL |
| 7|d38_10 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 7|d1_0 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 7|d18_6 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 7|n10_2 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 7|c_money | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 7|c_smallmoney | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |

## flt: real, float

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|c_real | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_float | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|c_real | EXACT | 0.0000000000000000e+000 | None | =P | =P | =P | None |
| 2|c_float | EXACT | 0.0000000000000000e+000 | None | =P | =P | =P | None |
| 3|c_real | EXACT | 3.4028234663852886e+038 | 3.4028234663852886e+38 | =P | =P | =P | 3.4028234663852886e+38 |
| 3|c_float | EXACT | 1.7976931348623157e+308 | 1.7976931348623157e+308 | =P | =P | =P | 1.7976931348623157e+308 |
| 4|c_real | EXACT | -3.4028234663852886e+038 | -3.4028234663852886e+38 | =P | =P | =P | -3.4028234663852886e+38 |
| 4|c_float | EXACT | -1.7976931348623157e+308 | -1.7976931348623157e+308 | =P | =P | =P | -1.7976931348623157e+308 |
| 5|c_real | EXACT | 1.1754943508222875e-038 | 1.1754943508222875e-38 | =P | =P | =P | 1.1754943508222875e-38 |
| 5|c_float | EXACT | 2.2250738585072014e-308 | 2.2250738585072014e-308 | =P | =P | =P | 2.2250738585072014e-308 |
| 6|c_real | EXACT | 1.0000000000000000e+000 | 1.0 | =P | =P | =P | 1.0 |
| 6|c_float | EXACT | 1.0000000000000000e+000 | 1.0 | =P | =P | =P | 1.0 |
| 7|c_real | EXACT | -1.0000000000000000e+000 | -1.0 | =P | =P | =P | -1.0 |
| 7|c_float | EXACT | -1.0000000000000000e+000 | -1.0 | =P | =P | =P | -1.0 |
| 8|c_real | EXACT | 1.6777216000000000e+007 | 16777216.0 | =P | =P | =P | 16777216.0 |
| 8|c_float | EXACT | 9.0071992547409920e+015 | 9007199254740992.0 | =P | =P | =P | 9007199254740992.0 |
| 9|c_real | EXACT | 1.0000000149011612e-001 | 0.10000000149011612 | =P | =P | =P | 0.10000000149011612 |
| 9|c_float | EXACT | 1.0000000000000001e-001 | 0.1 | =P | =P | =P | 0.1 |
| 10|c_real | EXACT | 1.4012984643248171e-045 | 1.401298464324817e-45 | =P | =P | =P | 1.401298464324817e-45 |
| 10|c_float | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |

## str: char..ntext

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|c_char | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_varchar | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_varchar_max | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_nchar | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_nvarchar | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_nvarchar_max | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_text | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_ntext | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|c_char | EXACT | len=10 pre=20202020202020202020 | len=10 pre=20202020202020202020 | =P | =P | =P | len=10 pre=20202020202020202020 |
| 2|c_varchar | EXACT | len=0 pre= | len=0 pre= | =P | =P | =P | len=0 pre= |
| 2|c_varchar_max | EXACT | len=0 pre= | len=0 pre= | =P | =P | =P | len=0 pre= |
| 2|c_nchar | EXACT | len=20 pre=200020002000200020002000 | len=20 pre=200020002000200020002000 | =P | =P | =P | len=20 pre=200020002000200020002000 |
| 2|c_nvarchar | EXACT | len=0 pre= | len=0 pre= | =P | =P | =P | len=0 pre= |
| 2|c_nvarchar_max | EXACT | len=0 pre= | len=0 pre= | =P | =P | =P | len=0 pre= |
| 2|c_text | EXACT | len=0 pre= | len=0 pre= | =P | =P | =P | len=0 pre= |
| 2|c_ntext | EXACT | len=0 pre= | len=0 pre= | =P | =P | =P | len=0 pre= |
| 3|c_char | EXACT | len=10 pre=61626320202020202020 | len=10 pre=61626320202020202020 | =P | =P | =P | len=10 pre=61626320202020202020 |
| 3|c_varchar | EXACT | len=3 pre=616263 | len=3 pre=616263 | =P | =P | =P | len=3 pre=616263 |
| 3|c_varchar_max | EXACT | len=3 pre=616263 | len=3 pre=616263 | =P | =P | =P | len=3 pre=616263 |
| 3|c_nchar | EXACT | len=20 pre=610062006300200020002000 | len=20 pre=610062006300200020002000 | =P | =P | =P | len=20 pre=610062006300200020002000 |
| 3|c_nvarchar | EXACT | len=6 pre=610062006300 | len=6 pre=610062006300 | =P | =P | =P | len=6 pre=610062006300 |
| 3|c_nvarchar_max | EXACT | len=6 pre=610062006300 | len=6 pre=610062006300 | =P | =P | =P | len=6 pre=610062006300 |
| 3|c_text | EXACT | len=3 pre=616263 | len=3 pre=616263 | =P | =P | =P | len=3 pre=616263 |
| 3|c_ntext | EXACT | len=6 pre=610062006300 | len=6 pre=610062006300 | =P | =P | =P | len=6 pre=610062006300 |
| 4|c_char | EXACT | len=10 pre=20782020202020202020 | len=10 pre=20782020202020202020 | =P | =P | =P | len=10 pre=20782020202020202020 |
| 4|c_varchar | EXACT | len=3 pre=207820 | len=3 pre=207820 | =P | =P | =P | len=3 pre=207820 |
| 4|c_varchar_max | EXACT | len=3 pre=207820 | len=3 pre=207820 | =P | =P | =P | len=3 pre=207820 |
| 4|c_nchar | EXACT | len=20 pre=200078002000200020002000 | len=20 pre=200078002000200020002000 | =P | =P | =P | len=20 pre=200078002000200020002000 |
| 4|c_nvarchar | EXACT | len=6 pre=200078002000 | len=6 pre=200078002000 | =P | =P | =P | len=6 pre=200078002000 |
| 4|c_nvarchar_max | EXACT | len=6 pre=200078002000 | len=6 pre=200078002000 | =P | =P | =P | len=6 pre=200078002000 |
| 4|c_text | EXACT | len=3 pre=207820 | len=3 pre=207820 | =P | =P | =P | len=3 pre=207820 |
| 4|c_ntext | EXACT | len=6 pre=200078002000 | len=6 pre=200078002000 | =P | =P | =P | len=6 pre=200078002000 |
| 5|c_char | EXACT | len=10 pre=6162636465666768696A | len=10 pre=6162636465666768696A | =P | =P | =P | len=10 pre=6162636465666768696A |
| 5|c_varchar | EXACT | len=20 pre=6162636465666768696A6B6C | len=20 pre=6162636465666768696A6B6C | =P | =P | =P | len=20 pre=6162636465666768696A6B6C |
| 5|c_varchar_max | EXACT | len=100000 pre=787878787878787878787878 | len=100000 pre=787878787878787878787878 | =P | =P | =P | len=100000 pre=787878787878787878787878 |
| 5|c_nchar | EXACT | len=20 pre=610062006300640065006600 | len=20 pre=610062006300640065006600 | =P | =P | =P | len=20 pre=610062006300640065006600 |
| 5|c_nvarchar | EXACT | len=40 pre=610062006300640065006600 | len=40 pre=610062006300640065006600 | =P | =P | =P | len=40 pre=610062006300640065006600 |
| 5|c_nvarchar_max | EXACT | len=200000 pre=790079007900790079007900 | len=200000 pre=790079007900790079007900 | =P | =P | =P | len=200000 pre=790079007900790079007900 |
| 5|c_text | EXACT | len=100000 pre=747474747474747474747474 | len=100000 pre=747474747474747474747474 | =P | =P | =P | len=100000 pre=747474747474747474747474 |
| 5|c_ntext | EXACT | len=200000 pre=6E006E006E006E006E006E00 | len=200000 pre=6E006E006E006E006E006E00 | =P | =P | =P | len=200000 pre=6E006E006E006E006E006E00 |
| 6|c_char | EXACT | len=10 pre=61006220202020202020 | len=10 pre=61006220202020202020 | =P | =P | =P | len=10 pre=61006220202020202020 |
| 6|c_varchar | EXACT | len=3 pre=610062 | len=3 pre=610062 | =P | =P | =P | len=3 pre=610062 |
| 6|c_varchar_max | EXACT | len=3 pre=610062 | len=3 pre=610062 | =P | =P | =P | len=3 pre=610062 |
| 6|c_nchar | EXACT | len=20 pre=610000006200200020002000 | len=20 pre=610000006200200020002000 | =P | =P | =P | len=20 pre=610000006200200020002000 |
| 6|c_nvarchar | EXACT | len=6 pre=610000006200 | len=6 pre=610000006200 | =P | =P | =P | len=6 pre=610000006200 |
| 6|c_nvarchar_max | EXACT | len=6 pre=610000006200 | len=6 pre=610000006200 | =P | =P | =P | len=6 pre=610000006200 |
| 6|c_text | EXACT | len=3 pre=610062 | len=3 pre=610062 | =P | =P | =P | len=3 pre=610062 |
| 6|c_ntext | EXACT | len=6 pre=610000006200 | len=6 pre=610000006200 | =P | =P | =P | len=6 pre=610000006200 |
| 7|c_char | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 7|c_varchar | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 7|c_varchar_max | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 7|c_nchar | EXACT | len=20 pre=3DD800DE2000200020002000 | len=20 pre=3DD800DE2000200020002000 | =P | =P | =P | len=20 pre=3DD800DE2000200020002000 |
| 7|c_nvarchar | EXACT | len=8 pre=3DD800DEE9002D4E | len=8 pre=3DD800DEE9002D4E | =P | =P | =P | len=8 pre=3DD800DEE9002D4E |
| 7|c_nvarchar_max | EXACT | len=200000 pre=3DD800DE3DD800DE3DD800DE | len=200000 pre=3DD800DE3DD800DE3DD800DE | =P | =P | =P | len=200000 pre=3DD800DE3DD800DE3DD800DE |
| 7|c_text | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 7|c_ntext | EXACT | len=4 pre=3DD800DE | len=4 pre=3DD800DE | =P | =P | =P | len=4 pre=3DD800DE |
| 9|c_char | EXACT | len=10 pre=E9202020202020202020 | len=10 pre=E9202020202020202020 | =P | =P | =P | len=10 pre=E9202020202020202020 |
| 9|c_varchar | EXACT | len=1 pre=E9 | len=1 pre=E9 | =P | =P | =P | len=1 pre=E9 |
| 9|c_varchar_max | EXACT | len=1 pre=E9 | len=1 pre=E9 | =P | =P | =P | len=1 pre=E9 |
| 9|c_nchar | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 9|c_nvarchar | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 9|c_nvarchar_max | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 9|c_text | EXACT | len=1 pre=E9 | len=1 pre=E9 | =P | =P | =P | len=1 pre=E9 |
| 9|c_ntext | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |

## bin: binary..image

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|c_binary | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_varbinary | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_varbinary_max | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_image | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|c_binary | EXACT | len=16 pre=000000000000000000000000 | len=16 pre=000000000000000000000000 | =P | =P | =P | len=16 pre=000000000000000000000000 |
| 2|c_varbinary | EXACT | len=0 pre= | len=0 pre= | =P | =P | =P | len=0 pre= |
| 2|c_varbinary_max | EXACT | len=0 pre= | len=0 pre= | =P | =P | =P | len=0 pre= |
| 2|c_image | EXACT | len=0 pre= | len=0 pre= | =P | =P | =P | len=0 pre= |
| 3|c_binary | EXACT | len=16 pre=FFFFFFFFFFFFFFFFFFFFFFFF | len=16 pre=FFFFFFFFFFFFFFFFFFFFFFFF | =P | =P | =P | len=16 pre=FFFFFFFFFFFFFFFFFFFFFFFF |
| 3|c_varbinary | EXACT | len=1 pre=00 | len=1 pre=00 | =P | =P | =P | len=1 pre=00 |
| 3|c_varbinary_max | EXACT | len=1 pre=00 | len=1 pre=00 | =P | =P | =P | len=1 pre=00 |
| 3|c_image | EXACT | len=1 pre=00 | len=1 pre=00 | =P | =P | =P | len=1 pre=00 |
| 4|c_binary | EXACT | len=16 pre=010000000000000000000000 | len=16 pre=010000000000000000000000 | =P | =P | =P | len=16 pre=010000000000000000000000 |
| 4|c_varbinary | EXACT | len=16 pre=FFFFFFFFFFFFFFFFFFFFFFFF | len=16 pre=FFFFFFFFFFFFFFFFFFFFFFFF | =P | =P | =P | len=16 pre=FFFFFFFFFFFFFFFFFFFFFFFF |
| 4|c_varbinary_max | EXACT | len=256 pre=000102030405060708090A0B | len=256 pre=000102030405060708090A0B | =P | =P | =P | len=256 pre=000102030405060708090A0B |
| 4|c_image | EXACT | len=256 pre=000102030405060708090A0B | len=256 pre=000102030405060708090A0B | =P | =P | =P | len=256 pre=000102030405060708090A0B |
| 5|c_binary | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 5|c_varbinary | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 5|c_varbinary_max | EXACT | len=100000 pre=ABABABABABABABABABABABAB | len=100000 pre=ABABABABABABABABABABABAB | =P | =P | =P | len=100000 pre=ABABABABABABABABABABABAB |
| 5|c_image | EXACT | len=100000 pre=ABABABABABABABABABABABAB | len=100000 pre=ABABABABABABABABABABABAB | =P | =P | =P | len=100000 pre=ABABABABABABABABABABABAB |

## date: date, datetime, smalldatetime

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|c_date | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_datetime | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|c_smalldatetime | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|c_date | EXACT | 0001-01-01 | 0001-01-01 | =P | =P | - | 0001-01-01 |
| 2|c_datetime | EXACT | 1753-01-01 00:00:00.000 | 1753-01-01 00:00:00.000000 | =P | =P | - | 1753-01-01 00:00:00.000000 |
| 2|c_smalldatetime | EXACT | 1900-01-01 00:00:00 | 1900-01-01 00:00:00.000000 | =P | =P | - | 1900-01-01 00:00:00.000000 |
| 3|c_date | EXACT | 9999-12-31 | 9999-12-31 | =P | =P | - | 9999-12-31 |
| 3|c_datetime | FORM_ONLY | 9999-12-31 23:59:59.997 | 9999-12-31 23:59:59.996667 | =P | =P | - | 9999-12-31 23:59:59.996667 |
| 3|c_smalldatetime | EXACT | 2079-06-06 23:59:00 | 2079-06-06 23:59:00.000000 | =P | =P | - | 2079-06-06 23:59:00.000000 |
| 4|c_date | EXACT | 1970-01-01 | 1970-01-01 | =P | =P | =P | 1970-01-01 |
| 4|c_datetime | EXACT | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 4|c_smalldatetime | EXACT | 1970-01-01 00:00:00 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 5|c_date | EXACT | 1969-12-31 | 1969-12-31 | =P | =P | =P | 1969-12-31 |
| 5|c_datetime | FORM_ONLY | 1969-12-31 23:59:59.997 | 1969-12-31 23:59:59.996667 | =P | =P | =P | 1969-12-31 23:59:59.996667 |
| 5|c_smalldatetime | EXACT | 1969-12-31 23:59:00 | 1969-12-31 23:59:00.000000 | =P | =P | =P | 1969-12-31 23:59:00.000000 |
| 6|c_date | EXACT | 1677-09-21 | 1677-09-21 | =P | =P | - | 1677-09-21 |
| 6|c_datetime | EXACT | 1900-01-01 00:00:00.000 | 1900-01-01 00:00:00.000000 | =P | =P | - | 1900-01-01 00:00:00.000000 |
| 6|c_smalldatetime | EXACT | 2000-02-29 12:34:00 | 2000-02-29 12:34:00.000000 | =P | =P | - | 2000-02-29 12:34:00.000000 |
| 7|c_date | EXACT | 2262-04-11 | 2262-04-11 | =P | =P | =P | 2262-04-11 |
| 7|c_datetime | FORM_ONLY | 2000-02-29 12:34:56.003 | 2000-02-29 12:34:56.003333 | =P | =P | =P | 2000-02-29 12:34:56.003333 |
| 7|c_smalldatetime | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 8|c_date | EXACT | 1900-01-01 | 1900-01-01 | =P | =P | =P | 1900-01-01 |
| 8|c_datetime | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 8|c_smalldatetime | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 9|c_date | EXACT | 2299-12-31 | 2299-12-31 | =P | =P | - | 2299-12-31 |
| 9|c_datetime | FORM_ONLY | 2299-12-31 23:59:59.997 | 2299-12-31 23:59:59.996667 | =P | =P | - | 2299-12-31 23:59:59.996667 |
| 9|c_smalldatetime | NULL_NONVALUE | NULL | NULL | =P | =P | - | NULL |
| 10|c_date | EXACT | 2000-02-29 | 2000-02-29 | =P | =P | =P | 2000-02-29 |
| 10|c_datetime | EXACT | 2262-04-11 23:47:16.000 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 10|c_smalldatetime | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 11|c_date | EXACT | 1899-12-31 | 1899-12-31 | =P | =P | - | 1899-12-31 |
| 11|c_datetime | FORM_ONLY | 2000-02-29 12:34:56.007 | 2000-02-29 12:34:56.006667 | =P | =P | - | 2000-02-29 12:34:56.006667 |
| 11|c_smalldatetime | NULL_NONVALUE | NULL | NULL | =P | =P | - | NULL |
| 12|c_date | EXACT | 2300-01-01 | 2300-01-01 | =P | =P | - | 2300-01-01 |
| 12|c_datetime | NULL_NONVALUE | NULL | NULL | =P | =P | - | NULL |
| 12|c_smalldatetime | NULL_NONVALUE | NULL | NULL | =P | =P | - | NULL |

## time: time(0..7)

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|t0 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|t1 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|t2 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|t3 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|t4 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|t5 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|t6 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|t7 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|t0 | EXACT | 00:00:00 | 00:00:00 | =P | =P | =P | 00:00:00 |
| 2|t1 | EXACT | 00:00:00.0 | 00:00:00 | =P | =P | =P | 00:00:00 |
| 2|t2 | EXACT | 00:00:00.00 | 00:00:00 | =P | =P | =P | 00:00:00 |
| 2|t3 | EXACT | 00:00:00.000 | 00:00:00 | =P | =P | =P | 00:00:00 |
| 2|t4 | EXACT | 00:00:00.0000 | 00:00:00 | =P | =P | =P | 00:00:00 |
| 2|t5 | EXACT | 00:00:00.00000 | 00:00:00 | =P | =P | =P | 00:00:00 |
| 2|t6 | EXACT | 00:00:00.000000 | 00:00:00 | =P | =P | =P | 00:00:00 |
| 2|t7 | EXACT | 00:00:00.0000000 | 00:00:00 | =P | =P | =P | 00:00:00 |
| 3|t0 | EXACT | 23:59:59 | 23:59:59 | =P | =P | =P | 23:59:59 |
| 3|t1 | EXACT | 23:59:59.9 | 23:59:59.9 | =P | =P | =P | 23:59:59.9 |
| 3|t2 | EXACT | 23:59:59.99 | 23:59:59.99 | =P | =P | =P | 23:59:59.99 |
| 3|t3 | EXACT | 23:59:59.999 | 23:59:59.999 | =P | =P | =P | 23:59:59.999 |
| 3|t4 | EXACT | 23:59:59.9999 | 23:59:59.9999 | =P | =P | =P | 23:59:59.9999 |
| 3|t5 | EXACT | 23:59:59.99999 | 23:59:59.99999 | =P | =P | =P | 23:59:59.99999 |
| 3|t6 | EXACT | 23:59:59.999999 | 23:59:59.999999 | =P | =P | =P | 23:59:59.999999 |
| 3|t7 | DEFECT_WRONG_VALUE | 23:59:59.9999999 | 23:59:59.999999 | =P | =P | =P | 23:59:59.999999 |
| 4|t0 | EXACT | 12:34:56 | 12:34:56 | =P | =P | =P | 12:34:56 |
| 4|t1 | EXACT | 12:34:56.1 | 12:34:56.1 | =P | =P | =P | 12:34:56.1 |
| 4|t2 | EXACT | 12:34:56.12 | 12:34:56.12 | =P | =P | =P | 12:34:56.12 |
| 4|t3 | EXACT | 12:34:56.123 | 12:34:56.123 | =P | =P | =P | 12:34:56.123 |
| 4|t4 | EXACT | 12:34:56.1234 | 12:34:56.1234 | =P | =P | =P | 12:34:56.1234 |
| 4|t5 | EXACT | 12:34:56.12345 | 12:34:56.12345 | =P | =P | =P | 12:34:56.12345 |
| 4|t6 | EXACT | 12:34:56.123456 | 12:34:56.123456 | =P | =P | =P | 12:34:56.123456 |
| 4|t7 | DEFECT_WRONG_VALUE | 12:34:56.1234567 | 12:34:56.123456 | =P | =P | =P | 12:34:56.123456 |
| 5|t0 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 5|t1 | EXACT | 00:00:00.1 | 00:00:00.1 | =P | =P | =P | 00:00:00.1 |
| 5|t2 | EXACT | 00:00:00.01 | 00:00:00.01 | =P | =P | =P | 00:00:00.01 |
| 5|t3 | EXACT | 00:00:00.001 | 00:00:00.001 | =P | =P | =P | 00:00:00.001 |
| 5|t4 | EXACT | 00:00:00.0001 | 00:00:00.0001 | =P | =P | =P | 00:00:00.0001 |
| 5|t5 | EXACT | 00:00:00.00001 | 00:00:00.00001 | =P | =P | =P | 00:00:00.00001 |
| 5|t6 | EXACT | 00:00:00.000001 | 00:00:00.000001 | =P | =P | =P | 00:00:00.000001 |
| 5|t7 | DEFECT_WRONG_VALUE | 00:00:00.0000001 | 00:00:00 | =P | =P | =P | 00:00:00 |
| 6|t0 | EXACT | 01:00:00 | 01:00:00 | =P | =P | =P | 01:00:00 |
| 6|t1 | EXACT | 01:00:00.5 | 01:00:00.5 | =P | =P | =P | 01:00:00.5 |
| 6|t2 | EXACT | 01:00:00.50 | 01:00:00.5 | =P | =P | =P | 01:00:00.5 |
| 6|t3 | EXACT | 01:00:00.500 | 01:00:00.5 | =P | =P | =P | 01:00:00.5 |
| 6|t4 | EXACT | 01:00:00.5000 | 01:00:00.5 | =P | =P | =P | 01:00:00.5 |
| 6|t5 | EXACT | 01:00:00.50000 | 01:00:00.5 | =P | =P | =P | 01:00:00.5 |
| 6|t6 | EXACT | 01:00:00.500000 | 01:00:00.5 | =P | =P | =P | 01:00:00.5 |
| 6|t7 | EXACT | 01:00:00.5000000 | 01:00:00.5 | =P | =P | =P | 01:00:00.5 |
| 7|t0 | EXACT | 23:59:59 | 23:59:59 | =P | =P | =P | 23:59:59 |
| 7|t1 | EXACT | 23:59:59.9 | 23:59:59.9 | =P | =P | =P | 23:59:59.9 |
| 7|t2 | EXACT | 23:59:59.99 | 23:59:59.99 | =P | =P | =P | 23:59:59.99 |
| 7|t3 | EXACT | 23:59:59.999 | 23:59:59.999 | =P | =P | =P | 23:59:59.999 |
| 7|t4 | EXACT | 23:59:59.9999 | 23:59:59.9999 | =P | =P | =P | 23:59:59.9999 |
| 7|t5 | EXACT | 23:59:59.99999 | 23:59:59.99999 | =P | =P | =P | 23:59:59.99999 |
| 7|t6 | EXACT | 23:59:59.999999 | 23:59:59.999999 | =P | =P | =P | 23:59:59.999999 |
| 7|t7 | DEFECT_WRONG_VALUE | 23:59:59.9999999 | 23:59:59.999999 | =P | =P | =P | 23:59:59.999999 |
| 8|t0 | EXACT | 12:34:56 | 12:34:56 | =P | =P | =P | 12:34:56 |
| 8|t1 | EXACT | 12:34:56.1 | 12:34:56.1 | =P | =P | =P | 12:34:56.1 |
| 8|t2 | EXACT | 12:34:56.12 | 12:34:56.12 | =P | =P | =P | 12:34:56.12 |
| 8|t3 | EXACT | 12:34:56.123 | 12:34:56.123 | =P | =P | =P | 12:34:56.123 |
| 8|t4 | EXACT | 12:34:56.1235 | 12:34:56.1235 | =P | =P | =P | 12:34:56.1235 |
| 8|t5 | EXACT | 12:34:56.12346 | 12:34:56.12346 | =P | =P | =P | 12:34:56.12346 |
| 8|t6 | EXACT | 12:34:56.123457 | 12:34:56.123457 | =P | =P | =P | 12:34:56.123457 |
| 8|t7 | DEFECT_WRONG_VALUE | 12:34:56.1234567 | 12:34:56.123456 | =P | =P | =P | 12:34:56.123456 |

## dt2: datetime2(0..7)

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|d0 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|d1 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|d2 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|d3 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|d4 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|d5 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|d6 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|d7 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|d0 | EXACT | 0001-01-01 00:00:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|d1 | EXACT | 0001-01-01 00:00:00.0 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|d2 | EXACT | 0001-01-01 00:00:00.00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|d3 | EXACT | 0001-01-01 00:00:00.000 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|d4 | EXACT | 0001-01-01 00:00:00.0000 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|d5 | EXACT | 0001-01-01 00:00:00.00000 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|d6 | EXACT | 0001-01-01 00:00:00.000000 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|d7 | EXACT | 0001-01-01 00:00:00.0000000 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 3|d0 | EXACT | 9999-12-31 23:59:59 | 9999-12-31 23:59:59.000000 | =P | =P | - | 9999-12-31 23:59:59.000000 |
| 3|d1 | EXACT | 9999-12-31 23:59:59.9 | 9999-12-31 23:59:59.900000 | =P | =P | - | 9999-12-31 23:59:59.900000 |
| 3|d2 | EXACT | 9999-12-31 23:59:59.99 | 9999-12-31 23:59:59.990000 | =P | =P | - | 9999-12-31 23:59:59.990000 |
| 3|d3 | EXACT | 9999-12-31 23:59:59.999 | 9999-12-31 23:59:59.999000 | =P | =P | - | 9999-12-31 23:59:59.999000 |
| 3|d4 | EXACT | 9999-12-31 23:59:59.9999 | 9999-12-31 23:59:59.999900 | =P | =P | - | 9999-12-31 23:59:59.999900 |
| 3|d5 | EXACT | 9999-12-31 23:59:59.99999 | 9999-12-31 23:59:59.999990 | =P | =P | - | 9999-12-31 23:59:59.999990 |
| 3|d6 | EXACT | 9999-12-31 23:59:59.999999 | 9999-12-31 23:59:59.999999 | =P | =P | - | 9999-12-31 23:59:59.999999 |
| 3|d7 | DEFECT_WRONG_VALUE | 9999-12-31 23:59:59.9999999 | 9999-12-31 23:59:59.999999 | =P | =P | - | 9999-12-31 23:59:59.999999 |
| 4|d0 | EXACT | 1970-01-01 00:00:00 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 4|d1 | EXACT | 1970-01-01 00:00:00.0 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 4|d2 | EXACT | 1970-01-01 00:00:00.00 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 4|d3 | EXACT | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 4|d4 | EXACT | 1970-01-01 00:00:00.0000 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 4|d5 | EXACT | 1970-01-01 00:00:00.00000 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 4|d6 | EXACT | 1970-01-01 00:00:00.000000 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 4|d7 | EXACT | 1970-01-01 00:00:00.0000000 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 5|d0 | EXACT | 1969-12-31 23:59:59 | 1969-12-31 23:59:59.000000 | =P | =P | =P | 1969-12-31 23:59:59.000000 |
| 5|d1 | EXACT | 1969-12-31 23:59:59.9 | 1969-12-31 23:59:59.900000 | =P | =P | =P | 1969-12-31 23:59:59.900000 |
| 5|d2 | EXACT | 1969-12-31 23:59:59.99 | 1969-12-31 23:59:59.990000 | =P | =P | =P | 1969-12-31 23:59:59.990000 |
| 5|d3 | EXACT | 1969-12-31 23:59:59.999 | 1969-12-31 23:59:59.999000 | =P | =P | =P | 1969-12-31 23:59:59.999000 |
| 5|d4 | EXACT | 1969-12-31 23:59:59.9999 | 1969-12-31 23:59:59.999900 | =P | =P | =P | 1969-12-31 23:59:59.999900 |
| 5|d5 | EXACT | 1969-12-31 23:59:59.99999 | 1969-12-31 23:59:59.999990 | =P | =P | =P | 1969-12-31 23:59:59.999990 |
| 5|d6 | EXACT | 1969-12-31 23:59:59.999999 | 1969-12-31 23:59:59.999999 | =P | =P | =P | 1969-12-31 23:59:59.999999 |
| 5|d7 | DEFECT_WRONG_VALUE | 1969-12-31 23:59:59.9999999 | 1969-12-31 23:59:59.999999 | =P | =P | =P | 1969-12-31 23:59:59.999999 |
| 6|d0 | EXACT | 1677-09-21 00:00:00 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 6|d1 | EXACT | 1677-09-21 00:00:00.0 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 6|d2 | EXACT | 1677-09-21 00:00:00.00 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 6|d3 | EXACT | 1677-09-21 00:00:00.000 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 6|d4 | EXACT | 1677-09-21 00:00:00.0000 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 6|d5 | EXACT | 1677-09-21 00:00:00.00000 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 6|d6 | EXACT | 1677-09-21 00:00:00.000000 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 6|d7 | EXACT | 1677-09-21 00:00:00.0000000 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 7|d0 | EXACT | 2262-04-11 23:47:16 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 7|d1 | EXACT | 2262-04-11 23:47:16.0 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 7|d2 | EXACT | 2262-04-11 23:47:16.00 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 7|d3 | EXACT | 2262-04-11 23:47:16.000 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 7|d4 | EXACT | 2262-04-11 23:47:16.0000 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 7|d5 | EXACT | 2262-04-11 23:47:16.00000 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 7|d6 | EXACT | 2262-04-11 23:47:16.000000 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 7|d7 | EXACT | 2262-04-11 23:47:16.0000000 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 8|d0 | EXACT | 1900-01-01 00:00:00 | 1900-01-01 00:00:00.000000 | =P | =P | =P | 1900-01-01 00:00:00.000000 |
| 8|d1 | EXACT | 1900-01-01 00:00:00.0 | 1900-01-01 00:00:00.000000 | =P | =P | =P | 1900-01-01 00:00:00.000000 |
| 8|d2 | EXACT | 1900-01-01 00:00:00.00 | 1900-01-01 00:00:00.000000 | =P | =P | =P | 1900-01-01 00:00:00.000000 |
| 8|d3 | EXACT | 1900-01-01 00:00:00.000 | 1900-01-01 00:00:00.000000 | =P | =P | =P | 1900-01-01 00:00:00.000000 |
| 8|d4 | EXACT | 1900-01-01 00:00:00.0000 | 1900-01-01 00:00:00.000000 | =P | =P | =P | 1900-01-01 00:00:00.000000 |
| 8|d5 | EXACT | 1900-01-01 00:00:00.00000 | 1900-01-01 00:00:00.000000 | =P | =P | =P | 1900-01-01 00:00:00.000000 |
| 8|d6 | EXACT | 1900-01-01 00:00:00.000000 | 1900-01-01 00:00:00.000000 | =P | =P | =P | 1900-01-01 00:00:00.000000 |
| 8|d7 | EXACT | 1900-01-01 00:00:00.0000000 | 1900-01-01 00:00:00.000000 | =P | =P | =P | 1900-01-01 00:00:00.000000 |
| 9|d0 | EXACT | 2299-12-31 23:59:59 | 2299-12-31 23:59:59.000000 | =P | =P | - | 2299-12-31 23:59:59.000000 |
| 9|d1 | EXACT | 2299-12-31 23:59:59.9 | 2299-12-31 23:59:59.900000 | =P | =P | - | 2299-12-31 23:59:59.900000 |
| 9|d2 | EXACT | 2299-12-31 23:59:59.99 | 2299-12-31 23:59:59.990000 | =P | =P | - | 2299-12-31 23:59:59.990000 |
| 9|d3 | EXACT | 2299-12-31 23:59:59.999 | 2299-12-31 23:59:59.999000 | =P | =P | - | 2299-12-31 23:59:59.999000 |
| 9|d4 | EXACT | 2299-12-31 23:59:59.9999 | 2299-12-31 23:59:59.999900 | =P | =P | - | 2299-12-31 23:59:59.999900 |
| 9|d5 | EXACT | 2299-12-31 23:59:59.99999 | 2299-12-31 23:59:59.999990 | =P | =P | - | 2299-12-31 23:59:59.999990 |
| 9|d6 | EXACT | 2299-12-31 23:59:59.999999 | 2299-12-31 23:59:59.999999 | =P | =P | - | 2299-12-31 23:59:59.999999 |
| 9|d7 | DEFECT_WRONG_VALUE | 2299-12-31 23:59:59.9999999 | 2299-12-31 23:59:59.999999 | =P | =P | - | 2299-12-31 23:59:59.999999 |
| 10|d0 | EXACT | 2000-02-29 12:34:56 | 2000-02-29 12:34:56.000000 | =P | =P | =P | 2000-02-29 12:34:56.000000 |
| 10|d1 | EXACT | 2000-02-29 12:34:56.1 | 2000-02-29 12:34:56.100000 | =P | =P | =P | 2000-02-29 12:34:56.100000 |
| 10|d2 | EXACT | 2000-02-29 12:34:56.12 | 2000-02-29 12:34:56.120000 | =P | =P | =P | 2000-02-29 12:34:56.120000 |
| 10|d3 | EXACT | 2000-02-29 12:34:56.123 | 2000-02-29 12:34:56.123000 | =P | =P | =P | 2000-02-29 12:34:56.123000 |
| 10|d4 | EXACT | 2000-02-29 12:34:56.1234 | 2000-02-29 12:34:56.123400 | =P | =P | =P | 2000-02-29 12:34:56.123400 |
| 10|d5 | EXACT | 2000-02-29 12:34:56.12345 | 2000-02-29 12:34:56.123450 | =P | =P | =P | 2000-02-29 12:34:56.123450 |
| 10|d6 | EXACT | 2000-02-29 12:34:56.123456 | 2000-02-29 12:34:56.123456 | =P | =P | =P | 2000-02-29 12:34:56.123456 |
| 10|d7 | DEFECT_WRONG_VALUE | 2000-02-29 12:34:56.1234567 | 2000-02-29 12:34:56.123456 | =P | =P | =P | 2000-02-29 12:34:56.123456 |
| 11|d0 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 11|d1 | EXACT | 1970-01-01 00:00:00.1 | 1970-01-01 00:00:00.100000 | =P | =P | =P | 1970-01-01 00:00:00.100000 |
| 11|d2 | EXACT | 1970-01-01 00:00:00.01 | 1970-01-01 00:00:00.010000 | =P | =P | =P | 1970-01-01 00:00:00.010000 |
| 11|d3 | EXACT | 1970-01-01 00:00:00.001 | 1970-01-01 00:00:00.001000 | =P | =P | =P | 1970-01-01 00:00:00.001000 |
| 11|d4 | EXACT | 1970-01-01 00:00:00.0001 | 1970-01-01 00:00:00.000100 | =P | =P | =P | 1970-01-01 00:00:00.000100 |
| 11|d5 | EXACT | 1970-01-01 00:00:00.00001 | 1970-01-01 00:00:00.000010 | =P | =P | =P | 1970-01-01 00:00:00.000010 |
| 11|d6 | EXACT | 1970-01-01 00:00:00.000001 | 1970-01-01 00:00:00.000001 | =P | =P | =P | 1970-01-01 00:00:00.000001 |
| 11|d7 | DEFECT_WRONG_VALUE | 1970-01-01 00:00:00.0000001 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 12|d0 | EXACT | 1899-12-31 23:59:59 | 1899-12-31 23:59:59.000000 | =P | =P | - | 1899-12-31 23:59:59.000000 |
| 12|d1 | EXACT | 1899-12-31 23:59:59.9 | 1899-12-31 23:59:59.900000 | =P | =P | - | 1899-12-31 23:59:59.900000 |
| 12|d2 | EXACT | 1899-12-31 23:59:59.99 | 1899-12-31 23:59:59.990000 | =P | =P | - | 1899-12-31 23:59:59.990000 |
| 12|d3 | EXACT | 1899-12-31 23:59:59.999 | 1899-12-31 23:59:59.999000 | =P | =P | - | 1899-12-31 23:59:59.999000 |
| 12|d4 | EXACT | 1899-12-31 23:59:59.9999 | 1899-12-31 23:59:59.999900 | =P | =P | - | 1899-12-31 23:59:59.999900 |
| 12|d5 | EXACT | 1899-12-31 23:59:59.99999 | 1899-12-31 23:59:59.999990 | =P | =P | - | 1899-12-31 23:59:59.999990 |
| 12|d6 | EXACT | 1899-12-31 23:59:59.999999 | 1899-12-31 23:59:59.999999 | =P | =P | - | 1899-12-31 23:59:59.999999 |
| 12|d7 | DEFECT_WRONG_VALUE | 1899-12-31 23:59:59.9999999 | 1899-12-31 23:59:59.999999 | =P | =P | - | 1899-12-31 23:59:59.999999 |
| 13|d0 | EXACT | 2300-01-01 00:00:00 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|d1 | EXACT | 2300-01-01 00:00:00.0 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|d2 | EXACT | 2300-01-01 00:00:00.00 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|d3 | EXACT | 2300-01-01 00:00:00.000 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|d4 | EXACT | 2300-01-01 00:00:00.0000 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|d5 | EXACT | 2300-01-01 00:00:00.00000 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|d6 | EXACT | 2300-01-01 00:00:00.000000 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|d7 | EXACT | 2300-01-01 00:00:00.0000000 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 14|d0 | EXACT | 2000-01-01 00:00:00 | 2000-01-01 00:00:00.000000 | =P | =P | =P | 2000-01-01 00:00:00.000000 |
| 14|d1 | EXACT | 2000-01-01 00:00:00.0 | 2000-01-01 00:00:00.000000 | =P | =P | =P | 2000-01-01 00:00:00.000000 |
| 14|d2 | EXACT | 2000-01-01 00:00:00.00 | 2000-01-01 00:00:00.000000 | =P | =P | =P | 2000-01-01 00:00:00.000000 |
| 14|d3 | EXACT | 2000-01-01 00:00:00.000 | 2000-01-01 00:00:00.000000 | =P | =P | =P | 2000-01-01 00:00:00.000000 |
| 14|d4 | EXACT | 2000-01-01 00:00:00.0000 | 2000-01-01 00:00:00.000000 | =P | =P | =P | 2000-01-01 00:00:00.000000 |
| 14|d5 | EXACT | 2000-01-01 00:00:00.00000 | 2000-01-01 00:00:00.000000 | =P | =P | =P | 2000-01-01 00:00:00.000000 |
| 14|d6 | EXACT | 2000-01-01 00:00:00.000001 | 2000-01-01 00:00:00.000001 | =P | =P | =P | 2000-01-01 00:00:00.000001 |
| 14|d7 | DEFECT_WRONG_VALUE | 2000-01-01 00:00:00.0000005 | 2000-01-01 00:00:00.000000 | =P | =P | =P | 2000-01-01 00:00:00.000000 |

## dto: datetimeoffset(0..7)

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|o0 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|o1 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|o2 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|o3 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|o4 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|o5 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|o6 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|o7 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|o0 | EXACT | 0001-01-01 00:00:00 +00:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|o1 | EXACT | 0001-01-01 00:00:00.0 +00:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|o2 | EXACT | 0001-01-01 00:00:00.00 +00:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|o3 | EXACT | 0001-01-01 00:00:00.000 +00:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|o4 | EXACT | 0001-01-01 00:00:00.0000 +00:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|o5 | EXACT | 0001-01-01 00:00:00.00000 +00:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|o6 | EXACT | 0001-01-01 00:00:00.000000 +00:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 2|o7 | EXACT | 0001-01-01 00:00:00.0000000 +00:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 3|o0 | EXACT | 9999-12-31 23:59:59 +00:00 | 9999-12-31 23:59:59.000000 | =P | =P | - | 9999-12-31 23:59:59.000000 |
| 3|o1 | EXACT | 9999-12-31 23:59:59.9 +00:00 | 9999-12-31 23:59:59.900000 | =P | =P | - | 9999-12-31 23:59:59.900000 |
| 3|o2 | EXACT | 9999-12-31 23:59:59.99 +00:00 | 9999-12-31 23:59:59.990000 | =P | =P | - | 9999-12-31 23:59:59.990000 |
| 3|o3 | EXACT | 9999-12-31 23:59:59.999 +00:00 | 9999-12-31 23:59:59.999000 | =P | =P | - | 9999-12-31 23:59:59.999000 |
| 3|o4 | EXACT | 9999-12-31 23:59:59.9999 +00:00 | 9999-12-31 23:59:59.999900 | =P | =P | - | 9999-12-31 23:59:59.999900 |
| 3|o5 | EXACT | 9999-12-31 23:59:59.99999 +00:00 | 9999-12-31 23:59:59.999990 | =P | =P | - | 9999-12-31 23:59:59.999990 |
| 3|o6 | EXACT | 9999-12-31 23:59:59.999999 +00:00 | 9999-12-31 23:59:59.999999 | =P | =P | - | 9999-12-31 23:59:59.999999 |
| 3|o7 | DEFECT_WRONG_VALUE | 9999-12-31 23:59:59.9999999 +00:00 | 9999-12-31 23:59:59.999999 | =P | =P | - | 9999-12-31 23:59:59.999999 |
| 4|o0 | FORM_ONLY | 9999-12-31 09:59:59 -14:00 | 9999-12-31 23:59:59.000000 | =P | =P | - | 9999-12-31 23:59:59.000000 |
| 4|o1 | FORM_ONLY | 9999-12-31 09:59:59.9 -14:00 | 9999-12-31 23:59:59.900000 | =P | =P | - | 9999-12-31 23:59:59.900000 |
| 4|o2 | FORM_ONLY | 9999-12-31 09:59:59.99 -14:00 | 9999-12-31 23:59:59.990000 | =P | =P | - | 9999-12-31 23:59:59.990000 |
| 4|o3 | FORM_ONLY | 9999-12-31 09:59:59.999 -14:00 | 9999-12-31 23:59:59.999000 | =P | =P | - | 9999-12-31 23:59:59.999000 |
| 4|o4 | FORM_ONLY | 9999-12-31 09:59:59.9999 -14:00 | 9999-12-31 23:59:59.999900 | =P | =P | - | 9999-12-31 23:59:59.999900 |
| 4|o5 | FORM_ONLY | 9999-12-31 09:59:59.99999 -14:00 | 9999-12-31 23:59:59.999990 | =P | =P | - | 9999-12-31 23:59:59.999990 |
| 4|o6 | FORM_ONLY | 9999-12-31 09:59:59.999999 -14:00 | 9999-12-31 23:59:59.999999 | =P | =P | - | 9999-12-31 23:59:59.999999 |
| 4|o7 | DEFECT_WRONG_VALUE | 9999-12-31 09:59:59.9999999 -14:00 | 9999-12-31 23:59:59.999999 | =P | =P | - | 9999-12-31 23:59:59.999999 |
| 5|o0 | FORM_ONLY | 0001-01-01 14:00:00 +14:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 5|o1 | FORM_ONLY | 0001-01-01 14:00:00.0 +14:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 5|o2 | FORM_ONLY | 0001-01-01 14:00:00.00 +14:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 5|o3 | FORM_ONLY | 0001-01-01 14:00:00.000 +14:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 5|o4 | FORM_ONLY | 0001-01-01 14:00:00.0000 +14:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 5|o5 | FORM_ONLY | 0001-01-01 14:00:00.00000 +14:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 5|o6 | FORM_ONLY | 0001-01-01 14:00:00.000000 +14:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 5|o7 | FORM_ONLY | 0001-01-01 14:00:00.0000000 +14:00 | 0001-01-01 00:00:00.000000 | =P | =P | - | 0001-01-01 00:00:00.000000 |
| 6|o0 | FORM_ONLY | 1970-01-01 00:00:00 +05:30 | 1969-12-31 18:30:00.000000 | =P | =P | =P | 1969-12-31 18:30:00.000000 |
| 6|o1 | FORM_ONLY | 1970-01-01 00:00:00.0 +05:30 | 1969-12-31 18:30:00.000000 | =P | =P | =P | 1969-12-31 18:30:00.000000 |
| 6|o2 | FORM_ONLY | 1970-01-01 00:00:00.00 +05:30 | 1969-12-31 18:30:00.000000 | =P | =P | =P | 1969-12-31 18:30:00.000000 |
| 6|o3 | FORM_ONLY | 1970-01-01 00:00:00.000 +05:30 | 1969-12-31 18:30:00.000000 | =P | =P | =P | 1969-12-31 18:30:00.000000 |
| 6|o4 | FORM_ONLY | 1970-01-01 00:00:00.0000 +05:30 | 1969-12-31 18:30:00.000000 | =P | =P | =P | 1969-12-31 18:30:00.000000 |
| 6|o5 | FORM_ONLY | 1970-01-01 00:00:00.00000 +05:30 | 1969-12-31 18:30:00.000000 | =P | =P | =P | 1969-12-31 18:30:00.000000 |
| 6|o6 | FORM_ONLY | 1970-01-01 00:00:00.000000 +05:30 | 1969-12-31 18:30:00.000000 | =P | =P | =P | 1969-12-31 18:30:00.000000 |
| 6|o7 | FORM_ONLY | 1970-01-01 00:00:00.0000000 +05:30 | 1969-12-31 18:30:00.000000 | =P | =P | =P | 1969-12-31 18:30:00.000000 |
| 7|o0 | EXACT | 1969-12-31 23:59:59 +00:00 | 1969-12-31 23:59:59.000000 | =P | =P | =P | 1969-12-31 23:59:59.000000 |
| 7|o1 | EXACT | 1969-12-31 23:59:59.9 +00:00 | 1969-12-31 23:59:59.900000 | =P | =P | =P | 1969-12-31 23:59:59.900000 |
| 7|o2 | EXACT | 1969-12-31 23:59:59.99 +00:00 | 1969-12-31 23:59:59.990000 | =P | =P | =P | 1969-12-31 23:59:59.990000 |
| 7|o3 | EXACT | 1969-12-31 23:59:59.999 +00:00 | 1969-12-31 23:59:59.999000 | =P | =P | =P | 1969-12-31 23:59:59.999000 |
| 7|o4 | EXACT | 1969-12-31 23:59:59.9999 +00:00 | 1969-12-31 23:59:59.999900 | =P | =P | =P | 1969-12-31 23:59:59.999900 |
| 7|o5 | EXACT | 1969-12-31 23:59:59.99999 +00:00 | 1969-12-31 23:59:59.999990 | =P | =P | =P | 1969-12-31 23:59:59.999990 |
| 7|o6 | EXACT | 1969-12-31 23:59:59.999999 +00:00 | 1969-12-31 23:59:59.999999 | =P | =P | =P | 1969-12-31 23:59:59.999999 |
| 7|o7 | DEFECT_WRONG_VALUE | 1969-12-31 23:59:59.9999999 +00:00 | 1969-12-31 23:59:59.999999 | =P | =P | =P | 1969-12-31 23:59:59.999999 |
| 8|o0 | FORM_ONLY | 2000-02-29 12:34:56 -08:00 | 2000-02-29 20:34:56.000000 | =P | =P | =P | 2000-02-29 20:34:56.000000 |
| 8|o1 | FORM_ONLY | 2000-02-29 12:34:56.1 -08:00 | 2000-02-29 20:34:56.100000 | =P | =P | =P | 2000-02-29 20:34:56.100000 |
| 8|o2 | FORM_ONLY | 2000-02-29 12:34:56.12 -08:00 | 2000-02-29 20:34:56.120000 | =P | =P | =P | 2000-02-29 20:34:56.120000 |
| 8|o3 | FORM_ONLY | 2000-02-29 12:34:56.123 -08:00 | 2000-02-29 20:34:56.123000 | =P | =P | =P | 2000-02-29 20:34:56.123000 |
| 8|o4 | FORM_ONLY | 2000-02-29 12:34:56.1234 -08:00 | 2000-02-29 20:34:56.123400 | =P | =P | =P | 2000-02-29 20:34:56.123400 |
| 8|o5 | FORM_ONLY | 2000-02-29 12:34:56.12345 -08:00 | 2000-02-29 20:34:56.123450 | =P | =P | =P | 2000-02-29 20:34:56.123450 |
| 8|o6 | FORM_ONLY | 2000-02-29 12:34:56.123456 -08:00 | 2000-02-29 20:34:56.123456 | =P | =P | =P | 2000-02-29 20:34:56.123456 |
| 8|o7 | DEFECT_WRONG_VALUE | 2000-02-29 12:34:56.1234567 -08:00 | 2000-02-29 20:34:56.123456 | =P | =P | =P | 2000-02-29 20:34:56.123456 |
| 9|o0 | FORM_ONLY | 2000-01-01 00:00:00 +14:00 | 1999-12-31 10:00:00.000000 | =P | =P | =P | 1999-12-31 10:00:00.000000 |
| 9|o1 | FORM_ONLY | 2000-01-01 00:00:00.0 +14:00 | 1999-12-31 10:00:00.000000 | =P | =P | =P | 1999-12-31 10:00:00.000000 |
| 9|o2 | FORM_ONLY | 2000-01-01 00:00:00.00 +14:00 | 1999-12-31 10:00:00.000000 | =P | =P | =P | 1999-12-31 10:00:00.000000 |
| 9|o3 | FORM_ONLY | 2000-01-01 00:00:00.000 +14:00 | 1999-12-31 10:00:00.000000 | =P | =P | =P | 1999-12-31 10:00:00.000000 |
| 9|o4 | FORM_ONLY | 2000-01-01 00:00:00.0000 +14:00 | 1999-12-31 10:00:00.000000 | =P | =P | =P | 1999-12-31 10:00:00.000000 |
| 9|o5 | FORM_ONLY | 2000-01-01 00:00:00.00000 +14:00 | 1999-12-31 10:00:00.000000 | =P | =P | =P | 1999-12-31 10:00:00.000000 |
| 9|o6 | FORM_ONLY | 2000-01-01 00:00:00.000000 +14:00 | 1999-12-31 10:00:00.000000 | =P | =P | =P | 1999-12-31 10:00:00.000000 |
| 9|o7 | FORM_ONLY | 2000-01-01 00:00:00.0000000 +14:00 | 1999-12-31 10:00:00.000000 | =P | =P | =P | 1999-12-31 10:00:00.000000 |
| 10|o0 | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 10|o1 | EXACT | 1970-01-01 00:00:00.1 +00:00 | 1970-01-01 00:00:00.100000 | =P | =P | =P | 1970-01-01 00:00:00.100000 |
| 10|o2 | EXACT | 1970-01-01 00:00:00.01 +00:00 | 1970-01-01 00:00:00.010000 | =P | =P | =P | 1970-01-01 00:00:00.010000 |
| 10|o3 | EXACT | 1970-01-01 00:00:00.001 +00:00 | 1970-01-01 00:00:00.001000 | =P | =P | =P | 1970-01-01 00:00:00.001000 |
| 10|o4 | EXACT | 1970-01-01 00:00:00.0001 +00:00 | 1970-01-01 00:00:00.000100 | =P | =P | =P | 1970-01-01 00:00:00.000100 |
| 10|o5 | EXACT | 1970-01-01 00:00:00.00001 +00:00 | 1970-01-01 00:00:00.000010 | =P | =P | =P | 1970-01-01 00:00:00.000010 |
| 10|o6 | EXACT | 1970-01-01 00:00:00.000001 +00:00 | 1970-01-01 00:00:00.000001 | =P | =P | =P | 1970-01-01 00:00:00.000001 |
| 10|o7 | DEFECT_WRONG_VALUE | 1970-01-01 00:00:00.0000001 +00:00 | 1970-01-01 00:00:00.000000 | =P | =P | =P | 1970-01-01 00:00:00.000000 |
| 11|o0 | EXACT | 1677-09-21 00:00:00 +00:00 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 11|o1 | EXACT | 1677-09-21 00:00:00.0 +00:00 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 11|o2 | EXACT | 1677-09-21 00:00:00.00 +00:00 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 11|o3 | EXACT | 1677-09-21 00:00:00.000 +00:00 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 11|o4 | EXACT | 1677-09-21 00:00:00.0000 +00:00 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 11|o5 | EXACT | 1677-09-21 00:00:00.00000 +00:00 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 11|o6 | EXACT | 1677-09-21 00:00:00.000000 +00:00 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 11|o7 | EXACT | 1677-09-21 00:00:00.0000000 +00:00 | 1677-09-21 00:00:00.000000 | =P | =P | - | 1677-09-21 00:00:00.000000 |
| 12|o0 | EXACT | 2262-04-11 23:47:16 +00:00 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 12|o1 | EXACT | 2262-04-11 23:47:16.0 +00:00 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 12|o2 | EXACT | 2262-04-11 23:47:16.00 +00:00 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 12|o3 | EXACT | 2262-04-11 23:47:16.000 +00:00 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 12|o4 | EXACT | 2262-04-11 23:47:16.0000 +00:00 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 12|o5 | EXACT | 2262-04-11 23:47:16.00000 +00:00 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 12|o6 | EXACT | 2262-04-11 23:47:16.000000 +00:00 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 12|o7 | EXACT | 2262-04-11 23:47:16.0000000 +00:00 | 2262-04-11 23:47:16.000000 | =P | =P | =P | 2262-04-11 23:47:16.000000 |
| 13|o0 | EXACT | 2300-01-01 00:00:00 +00:00 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|o1 | EXACT | 2300-01-01 00:00:00.0 +00:00 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|o2 | EXACT | 2300-01-01 00:00:00.00 +00:00 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|o3 | EXACT | 2300-01-01 00:00:00.000 +00:00 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|o4 | EXACT | 2300-01-01 00:00:00.0000 +00:00 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|o5 | EXACT | 2300-01-01 00:00:00.00000 +00:00 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|o6 | EXACT | 2300-01-01 00:00:00.000000 +00:00 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |
| 13|o7 | EXACT | 2300-01-01 00:00:00.0000000 +00:00 | 2300-01-01 00:00:00.000000 | =P | =P | - | 2300-01-01 00:00:00.000000 |

## uid: uniqueidentifier

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|u | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|u | EXACT | 00000000-0000-0000-0000-000000000000 | 00000000-0000-0000-0000-000000000000 | =P | =P | =P | 00000000-0000-0000-0000-000000000000 |
| 3|u | EXACT | ffffffff-ffff-ffff-ffff-ffffffffffff | ffffffff-ffff-ffff-ffff-ffffffffffff | =P | =P | =P | ffffffff-ffff-ffff-ffff-ffffffffffff |
| 4|u | EXACT | 6ba7b810-9dad-11d1-80b4-00c04fd430c8 | 6ba7b810-9dad-11d1-80b4-00c04fd430c8 | =P | =P | =P | 6ba7b810-9dad-11d1-80b4-00c04fd430c8 |
| 5|u | EXACT | 01020304-0506-0708-090a-0b0c0d0e0f10 | 01020304-0506-0708-090a-0b0c0d0e0f10 | =P | =P | =P | 01020304-0506-0708-090a-0b0c0d0e0f10 |

## var: sql_variant

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|v | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|v | FORM_ONLY | int:2147483647 | 2147483647 | =P | =P | =P | 2147483647 |
| 3|v | FORM_ONLY | bigint:-9223372036854775808 | -9223372036854775808 | =P | =P | =P | -9223372036854775808 |
| 4|v | FORM_ONLY | tinyint:255 | 255 | =P | =P | =P | 255 |
| 5|v | FORM_ONLY | bit:1 | 1 | =P | =P | =P | 1 |
| 6|v | FORM_ONLY | decimal:99999999999999999999999999999999999~ | 99999999999999999999999999999999999999 | =P | =P | =P | 99999999999999999999999999999999999999 |
| 7|v | FORM_ONLY | decimal:-0.0000000001 | -0.0000000001 | =P | =P | =P | -0.0000000001 |
| 8|v | FORM_ONLY | money:12345.6789 | 12345.6789 | =P | =P | =P | 12345.6789 |
| 9|v | FORM_ONLY | float:1.7976931348623157e+308 | 1797693134862315700000000000000000000000000~ | =P | =P | =P | 1797693134862315700000000000000000000000000~ |
| 10|v | FORM_ONLY | real:3.3999999521443642e+038 | 340000000000000000000000000000000000000 | =P | =P | =P | 340000000000000000000000000000000000000 |
| 11|v | FORM_ONLY | float:1.0000000000000001e-001 | 0.1 | =P | =P | =P | 0.1 |
| 12|v | FORM_ONLY | nvarchar:ü😀 | ü😀 | =P | =P | =P | ü😀 |
| 13|v | FORM_ONLY | varchar:abc | abc | =P | =P | =P | abc |
| 14|v | FORM_ONLY | varchar: | None | =P | =P | =P | None |
| 15|v | FORM_ONLY | varbinary:00ff | 00ff | =P | =P | =P | 00ff |
| 16|v | FORM_ONLY | varbinary: | None | =P | =P | =P | None |
| 17|v | FORM_ONLY | uniqueidentifier:6ba7b810-9dad-11d1-80b4-00~ | 6BA7B810-9DAD-11D1-80B4-00C04FD430C8 | =P | =P | =P | 6BA7B810-9DAD-11D1-80B4-00C04FD430C8 |
| 18|v | FORM_ONLY | date:0001-01-01 | 0001-01-01 | =P | =P | =P | 0001-01-01 |
| 19|v | FORM_ONLY | date:9999-12-31 | 9999-12-31 | =P | =P | =P | 9999-12-31 |
| 20|v | FORM_ONLY | datetime2:2000-02-29 12:34:56.1234567 | 2000-02-29 12:34:56.1234567 | =P | =P | =P | 2000-02-29 12:34:56.1234567 |
| 21|v | FORM_ONLY | datetimeoffset:9999-12-31 09:59:59.9999999 ~ | 9999-12-31 09:59:59.9999999 -14:00 | =P | =P | =P | 9999-12-31 09:59:59.9999999 -14:00 |
| 22|v | FORM_ONLY | datetimeoffset:2000-02-29 12:34:56.1234567 ~ | 2000-02-29 12:34:56.1234567 +05:30 | =P | =P | =P | 2000-02-29 12:34:56.1234567 +05:30 |
| 23|v | FORM_ONLY | time:23:59:59.9999999 | 23:59:59.9999999 | =P | =P | =P | 23:59:59.9999999 |
| 24|v | FORM_ONLY | datetime:2000-01-01 12:34:56.003 | 2000-01-01 12:34:56.003 | =P | =P | =P | 2000-01-01 12:34:56.003 |
| 25|v | FORM_ONLY | smalldatetime:2000-01-01 12:34:00 | 2000-01-01 12:34:00 | =P | =P | =P | 2000-01-01 12:34:00 |

## clr: hierarchyid, geometry, geography

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|h | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|gm | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 1|gg | NULL_NONVALUE | NULL | NULL | =P | =P | =P | NULL |
| 2|h | EXACT | len=0 pre= | len=0 pre= | =P | =P | =P | len=0 pre= |
| 2|gm | EXACT | len=22 pre=00000000010C000000000000 | len=22 pre=00000000010C000000000000 | =P | =P | =P | len=22 pre=00000000010C000000000000 |
| 2|gg | EXACT | len=22 pre=E6100000010CCDCCCCCCCCCC | len=22 pre=E6100000010CCDCCCCCCCCCC | =P | =P | =P | len=22 pre=E6100000010CCDCCCCCCCCCC |
| 3|h | EXACT | len=1 pre=58 | len=1 pre=58 | =P | =P | =P | len=1 pre=58 |
| 3|gm | EXACT | len=27 pre=000000000104000000000000 | len=27 pre=000000000104000000000000 | =P | =P | =P | len=27 pre=000000000104000000000000 |
| 3|gg | EXACT | len=27 pre=E61000000104000000000000 | len=27 pre=E61000000104000000000000 | =P | =P | =P | len=27 pre=E61000000104000000000000 |
| 4|h | EXACT | len=2 pre=635E | len=2 pre=635E | =P | =P | =P | len=2 pre=635E |
| 4|gm | EXACT | len=112 pre=E61000000104050000000000 | len=112 pre=E61000000104050000000000 | =P | =P | =P | len=112 pre=E61000000104050000000000 |
| 4|gg | EXACT | len=112 pre=E61000000104050000000000 | len=112 pre=E61000000104050000000000 | =P | =P | =P | len=112 pre=E61000000104050000000000 |
| 5|h | EXACT | len=2 pre=3B80 | len=2 pre=3B80 | =P | =P | =P | len=2 pre=3B80 |
| 5|gm | EXACT | len=38 pre=00000000010F000000000000 | len=38 pre=00000000010F000000000000 | =P | =P | =P | len=38 pre=00000000010F000000000000 |
| 5|gg | EXACT | len=38 pre=E61000000114000000000000 | len=38 pre=E61000000114000000000000 | =P | =P | =P | len=38 pre=E61000000114000000000000 |

## rv: rowversion

| edge|column | verdict | source | Parquet | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|---|
| 1|rv | EXACT | 0000000000000844 | 0000000000000844 | =P | =P | =P | 0000000000000844 |
| 1|v | EXACT | 1 | 1 | =P | =P | =P | 1 |
| 2|rv | EXACT | 0000000000000845 | 0000000000000845 | =P | =P | =P | 0000000000000845 |
| 2|v | EXACT | 2 | 2 | =P | =P | =P | 2 |
| 3|rv | EXACT | 0000000000000846 | 0000000000000846 | =P | =P | =P | 0000000000000846 |
| 3|v | EXACT | 3 | 3 | =P | =P | =P | 3 |

## Verdict counts

- parquet: {'NULL_NONVALUE': 94, 'EXACT': 379, 'FORM_ONLY': 67, 'DEFECT_WRONG_VALUE': 17}
- bq: {'NULL_NONVALUE': 94, 'EXACT': 379, 'FORM_ONLY': 67, 'DEFECT_WRONG_VALUE': 17}
- sf: {'NULL_NONVALUE': 92, 'EXACT': 374, 'FORM_ONLY': 67, 'DEFECT_WRONG_VALUE': 17}
- ch: {'NULL_NONVALUE': 90, 'EXACT': 292, 'FORM_ONLY': 49, 'DEFECT_WRONG_VALUE': 12}
