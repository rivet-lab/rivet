# Edge-value type matrix: MySQL 5.7 and 8.0 (rivet 0.33.0, main 5240ede7)

Measured 2026-11-15 on mysql57-batch (3557) and mysql80-batch (3580). Default mapping, no `columns:` override.
Stopped early by the 21:15 CET machine freeze. UNMEASURED(freeze) items are listed at the end.

Oracles: source via the mysql CLI (text, md5+length for strings and binary, +0 for bit/float, HEX of JSON text); Parquet via pyarrow; BigQuery via `bq query` text casts; Snowflake via `snow sql`; ClickHouse via HTTP `toString` / `lower(hex(MD5()))`; DuckDB 1.1.0 CLI on the Parquet.
Float columns: the warehouse double is compared with the Parquet double, because MySQL prints doubles with only 16 digits.
Results are identical on 5.7 and 8.0 for every cell measured on both, except where a version is named. Scripts and logs: scratchpad/edge-mysql/.

## Summary counts (columns graded, value-level grades roll up to the column)

| stage / target | EXACT | NULL_NONVALUE (inside EXACT columns) | REFUSED (acceptable) | DEFECT |
|---|---|---|---|---|
| Parquet 5.7 | 75 cols | 6 cols | geo (RIVET_TYPE_UNSAFE_MAPPING) | 4 tables refused at extract: bit(64) top bit (3 tables), TIME >=24h or negative (8 tables) = DEFECT_NATIVE_REFUSED; tinyint(1) x3 cols DEFECT_WRONG_VALUE |
| Parquet 8.0 | 72 cols | 6 | geo | same as 5.7 |
| BigQuery 5.7 and 8.0 | 94 cols | 6 | dec(39..65,0) table (RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | DEFECT_NATIVE_REFUSED: DATE 0000-01-01 and 0000-12-31 (RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1); DEFECT_WRONG_VALUE: 3 tinyint(1) cols |
| Snowflake 5.7 and 8.0 | 94 cols | 6 | 3 tables with decimal p>38 (RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | DEFECT_WRONG_VALUE: 3 tinyint(1) cols |
| ClickHouse 5.7 and 8.0 | 91 cols (+2 split tables on 5.7) | 6 | DATE < 1900 or > 2299 (table `date`, `date_ok0`: RIVET_LOAD_WAREHOUSE_REJECTED, raw ClickHouse Code 321, exit 1 alone, exit 5 in the batch); DATETIME < 1900 (RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, exit 5 in the batch) | DEFECT_WRONG_VALUE: 3 tinyint(1) cols |
| DuckDB 1.1.0 (reads the Parquet) | all columns equal Parquet except the reader findings below | | | DuckDB reader: decimal(p>38) read as garbage DOUBLE; year-0 date rendered `0001-01-01 (BC)` (FORM_ONLY) |

Per-column WARN is emitted for zero and partial dates: "column `d`: 3 MySQL zero or partial date value(s) written as NULL" (also for datetime and timestamp). All NULL_NONVALUE cells have it.

## DEFECT list, ranked

1. DEFECT_WRONG_VALUE (silent, all targets): `TINYINT(1)`, `TINYINT(1) UNSIGNED`, `BOOLEAN` map to Arrow Boolean; every value other than 0 and 1 arrives `true`. Source 2, -1, 127, -128, 255, 254, 128 -> true (BQ, Snowflake, ClickHouse, Parquet, DuckDB). Command: `rivet run -c v57/run.yaml -e tc_edge_mysql_bool_57`, then `rivet load`. src: src/source/mysql/arrow_convert.rs:90 (TINY with column_length 1 -> Bool), :306-315 (`native_flag` exempts it from `build_bool_strict`), :753-757 (`b.append_value(*v != 0)`). The strict builder at :692-706 would refuse 2, but it is bypassed. Already in the ledger as known_defect (5 and -1 arrive true); the rest of the values and the unsigned form are new. On 8.0, `tinyint(1) unsigned` reports as `tinyint unsigned` but still maps to Boolean (measured).
2. DEFECT_NATIVE_REFUSED, whole run refused: BIT(64) with the top bit set (b'1'x64 = 2^64-1, 0x8000000000000000 = 2^63, 0xFFFFFFFFFFFFFFFE). `rivet run -c v57/run.yaml -e tc_edge_mysql_bit64_top_57` (also _2p63, _fffe): RIVET_SOURCE_VALUE_UNREPRESENTABLE, extract, exit 5. src: arrow_convert.rs:158 (bit(n>1) -> Int64), :523-536 (`bit_bytes_to_u64` then `narrow::<i64>` via `int64_from_bytes`), :510-518 (the overflow message). UInt64 holds every value. Values up to 0x7FFF...F (bit(63) max, bit(64) 2^63-1) are EXACT.
3. DEFECT_NATIVE_REFUSED, whole run refused: TIME outside 00:00..24:00 (24:00:00, 25:30:15, 100:00:00, 838:59:59, -00:00:01, -00:00:00.000001, -01:00:00, -838:59:59). RIVET_SOURCE_VALUE_UNREPRESENTABLE, extract, exit 5, with the remedy text "Cast it in a query, e.g. TIME_TO_SEC(col)". src: arrow_convert.rs:565-590 (`time_of_day_in_range`). The refusal is coded and not silent, but ClickHouse (Decimal64 seconds) and BigQuery/Snowflake-as-string could hold the values; the Parquet TIME type cannot. Grade as DEFECT_NATIVE_REFUSED against the owner contract (a wider native type, e.g. Int64 microseconds or Interval, holds every MySQL TIME).
4. DEFECT_NATIVE_REFUSED at BigQuery load: DATE `0000-01-01` and `0000-12-31` (valid MySQL dates; Parquet Date32 -719528 is correct). `rivet load -c v57/bq.yaml -e tc_edge_mysql_date_ok0_57`: RIVET_LOAD_WAREHOUSE_REJECTED, "Invalid date value -719528", load, exit 1. Snowflake loads them EXACT. No pre-check names the range (ClickHouse and DATETIME have one).
5. Message gap, ClickHouse DATE out of Date32 range (1899-12-31, 1677-09-21, 0000-xx, 9999-12-31, 2300-01-01): refused, never clamped, but through the raw warehouse error `RIVET_LOAD_WAREHOUSE_REJECTED` ("Input value 2932896 ... out of allowed Date32 range"). DATETIME gets the specific RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE from `refuse_unholdable_timestamps` (src/load/clickhouse.rs:1075). The DATE path has no equivalent pre-check; Date32 range is 1900-01-01..2299-12-31.
6. REFUSED but worth the owner's eye: geometry family (GEOMETRY, POINT, LINESTRING, POLYGON, GEOMETRYCOLLECTION, MULTIPOINT) -> RIVET_TYPE_UNSAFE_MAPPING, extract, exit 1, no `columns:`-free path (arrow_convert.rs:160-163 falls into `Unsupported`). Acceptable by the contract only if a native type cannot hold it; WKB in Binary can.
7. REFUSED, acceptable: decimal(39..65,0) BigQuery (BIGNUMERIC holds at most 38 integer digits, src/types/target.rs:611-615, :704-711) and Snowflake NUMBER max precision 38 (target.rs:850). decimal(65,30) is EXACT on BigQuery (35 integer digits) and ClickHouse; refused on Snowflake.
8. DuckDB 1.1.0 reader (not rivet): Parquet `Decimal(39..65)` fixed_len_byte_array is read as DOUBLE with a wrong value (999...9 (39 digits) -> 7.205759403792793e+55). Rivet's Parquet is correct (pyarrow EXACT, ClickHouse EXACT).

Accepted reds in dev/release_oracle/known_red.py ~187-199: not reproduced as a data change in this run. The MySQL TIMESTAMP(6) zero-date NULL is the documented NULL_NONVALUE and matched on all targets (graded EXACT with the NONVALUE regex). Zero and partial dates -> NULL with the WARN is the documented exception.

## Per-type tables

Source column = the value as inserted. Parquet = Arrow type delivered and verdict. BQ / SF / CH / Duck = verdict (EXACT unless written). "5.7=8.0" unless stated.

### Integers (table tc_edge_mysql_int; all values in the same table)
Edge values for every width: NULL, 0, min, max, min+1, max-1, -1, 2^63-1 / 2^63 (bigint unsigned), 2^64-1.

| type | Parquet | edge values | BQ | SF | CH | Duck |
|---|---|---|---|---|---|---|
| TINYINT | Int16 | NULL,0,-128,127,-127,126,-1 | EXACT | EXACT | Int16 EXACT | EXACT |
| TINYINT UNSIGNED | Int16 | NULL,0,255,254,1,128,127 | EXACT | EXACT | Int16 EXACT | EXACT |
| SMALLINT | Int16 | -32768,32767,-32767,32766,-1 | EXACT | EXACT | EXACT | EXACT |
| SMALLINT UNSIGNED | Int32 | 65535,65534,1,32768,32767 | EXACT | EXACT | Int32 EXACT | EXACT |
| MEDIUMINT | Int32 | -8388608,8388607,... | EXACT | EXACT | EXACT | EXACT |
| MEDIUMINT UNSIGNED | Int64 | 16777215,16777214,8388608,8388607 | EXACT | EXACT | Int64 EXACT | EXACT |
| INT | Int32 | -2147483648,2147483647,... | EXACT | EXACT | EXACT | EXACT |
| INT UNSIGNED | Int64 | 4294967295,4294967294,2147483648 | EXACT | EXACT | Int64 EXACT | EXACT |
| BIGINT | Int64 | -2^63, 2^63-1, +-1 inner | EXACT | EXACT | EXACT | EXACT |
| BIGINT UNSIGNED | UInt64 | 2^64-1, 2^64-2, 2^63, 2^63-1, 1 | EXACT (BQ INT64? see note) | EXACT (NUMBER) | UInt64 EXACT | EXACT |
| ZEROFILL (tinyint u, int(5), bigint(20) u) | Int16/Int64/UInt64 | 0,255,4294967295,2^64-1,7,42,1 | FORM_ONLY (width dropped) | FORM_ONLY | FORM_ONLY | FORM_ONLY |

Note: BigQuery loaded UInt64 2^64-1 EXACT (as NUMERIC); verified by the text cast.

### Boolean family (tc_edge_mysql_bool)
| native | value | Parquet | BQ | SF | CH | Duck |
|---|---|---|---|---|---|---|
| TINYINT(1) / BOOLEAN | NULL,0,1 | Boolean | EXACT | EXACT | EXACT | EXACT |
| | 2, -1, 127, -128 | Boolean true | DEFECT_WRONG_VALUE true | same | same | same |
| TINYINT(1) UNSIGNED | 0,1 | Boolean | EXACT | EXACT | EXACT | EXACT |
| | 2, 255, 254, 128 | true | DEFECT_WRONG_VALUE | same | same | same |
| BIT(1) | NULL,0,1 | Boolean | EXACT | EXACT | EXACT | EXACT |

### BIT(n)
| native | value | Parquet | BQ / SF / CH / Duck |
|---|---|---|---|
| BIT(2), BIT(8), BIT(16), BIT(32) | NULL,0,1,all ones,top bit alone,10101010 pattern | Int64 EXACT | EXACT (all) |
| BIT(63) | 0,1,2^62,2^63-2,2^63-1 | Int64 EXACT | EXACT |
| BIT(64) | NULL,0,1,2^62,2^63-1 | Int64 EXACT | EXACT |
| BIT(64) | 2^63 (0x8000...) | REFUSED RIVET_SOURCE_VALUE_UNREPRESENTABLE, extract, exit 5: DEFECT_NATIVE_REFUSED | UNMEASURED(not extracted) |
| BIT(64) | 2^64-2 | same | |
| BIT(64) | 2^64-1 (b'1'x64) | same | |

### DECIMAL
| native | edge values | Parquet | BQ | SF | CH | Duck |
|---|---|---|---|---|---|---|
| DECIMAL(1,0) | NULL,0,9,-9,1 | Decimal128 | EXACT | EXACT | EXACT | EXACT |
| DECIMAL(10,2), (10,2) UNSIGNED | 99999999.99, 0.01, 0 | Decimal128 | EXACT | EXACT | EXACT | EXACT |
| DECIMAL(18,9) | +-999999999.999999999, 1e-9 | Decimal128 | EXACT | EXACT | EXACT | EXACT |
| DECIMAL(38,0), (38,8), (38,30) | max, -max, 1e-scale | Decimal128 | EXACT | EXACT | EXACT | EXACT |
| DECIMAL(30,30) | 0.999..9, 1e-30, 0.5 | Decimal128 | EXACT | EXACT | EXACT | EXACT |
| DECIMAL(39,30) | NULL, 123456789.123456789012345678901234567890, 1e-30 | Decimal256 EXACT | EXACT? (BQ loaded, see note) | REFUSED RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED load exit 1 | EXACT Decimal(39,30) | DuckDB reader WRONG (finding 8) |
| DECIMAL(39,0), (65,0) | +-max, 1, 0 | Decimal256 EXACT | REFUSED RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED load exit 1 | REFUSED same | EXACT Decimal(39,0)/(65,0) | reader WRONG |
| DECIMAL(65,30) | +-99999999999999999999999999999999999.999999999999999999999999999999, +-1e-30, 12345678901234567890123456789012345.123... | Decimal256 EXACT | EXACT (BIGNUMERIC) | REFUSED same | EXACT Decimal(65,30) | reader WRONG |

Note: the BigQuery load of tc_edge_mysql_dec39_38 loaded (only dec65_0 failed in the load summary: 26 LOAD OK).

### FLOAT / DOUBLE (MySQL cannot store NaN or Inf; -0.0 measured)
| native | edge values | Parquet | BQ | SF | CH | Duck |
|---|---|---|---|---|---|---|
| FLOAT, FLOAT UNSIGNED, FLOAT(10,2) | 0, 3.402823466e38, -max, 1.175494351e-38 (min normal), 1, 0.1, 16777217 (rounds to 16777216), 1.4e-45 (subnormal) | Float32 EXACT | FLOAT64 EXACT | FLOAT EXACT | Float32 EXACT | EXACT |
| DOUBLE, REAL, DOUBLE UNSIGNED, DOUBLE(20,5) | 0, 1.7976931348623157e308, -max, 2.2250738585072014e-308, 9007199254740993 (rounds to ...992), 4.9e-324 (subnormal) | Float64 EXACT | EXACT | EXACT | EXACT | EXACT |
| -0.0 (tc_edge_mysql_float_negzero) | FLOAT/DOUBLE | EXACT (sign compared via float equality; -0.0 == 0.0 so the sign bit is UNMEASURED) | | | | |

### CHAR / VARCHAR / TEXT (latin1 and utf8mb4)
Edge values (table tc_edge_mysql_str): NULL, '', max length (CHAR(10), VARCHAR(255), VARCHAR(16000), TINYTEXT 255, TEXT 65535, MEDIUMTEXT 70000, LONGTEXT 70000), 4-byte emoji x10, latin1 high bytes, Cyrillic, CJK, leading/trailing spaces, embedded NUL, quotes/backslash/newline/tab/CRLF, 1 MB MEDIUMTEXT (tc_edge_mysql_str_lt16m).
Parquet Utf8 EXACT, BQ STRING EXACT, SF TEXT EXACT, CH String EXACT, DuckDB EXACT (compared by byte length and md5). Latin1 data arrives as UTF-8. CHAR padding: MySQL strips trailing spaces on read; the source text and the delivered text match. LONGTEXT above 16 MB: UNMEASURED(freeze).

### BINARY / VARBINARY / BLOB
Edge values: NULL, empty, 0x00, 0xFFFFFFFF, 255 x 0xFF, 60000 bytes, 65535, 100000 (MEDIUMBLOB, LONGBLOB), embedded 00 and 0d0a, all 256 byte values (VARBINARY(255) row 6).
Parquet Binary EXACT; BQ BYTES EXACT; SF BINARY EXACT (note: Snowflake BINARY caps at 8 MB; larger UNMEASURED); CH String EXACT; Duck BLOB EXACT.

### ENUM / SET
| native | values | Parquet | BQ | SF | CH | Duck |
|---|---|---|---|---|---|---|
| ENUM('a','b','c') | NULL,a,b,c | Utf8 | EXACT | EXACT | EXACT | EXACT |
| ENUM with emoji, 'é', space, '' | each | Utf8 | EXACT | EXACT | EXACT | EXACT |
| ENUM invalid insert with sql_mode='' (stored as '' index 0) | 'zzz' | Utf8 '' | EXACT | EXACT | EXACT | EXACT |
| SET('a','b','c') | '', a,b,c, b, a,c | Utf8 | EXACT | EXACT | EXACT | EXACT |
| SET(64 members) | m1, m1,m64, m64, all 64 | Utf8 | EXACT | EXACT | EXACT | EXACT |
| SET with emoji and space | 😀,é,x y | Utf8 | EXACT | EXACT | EXACT | EXACT |

### JSON (text-compared as decoded JSON, numbers as Decimal)
Edge values: SQL NULL vs JSON null vs {} vs [] , nested object/array, 99-deep array, 50-deep object, 18446744073709551615, 18446744073709551616 (MySQL stores a double 1.8446744073709552e19), -2^63, 1.7976931348623157e308, 1e-400 (stored 0), "x", true, 0.1, emoji, "\u0000".
Parquet Utf8; BQ JSON EXACT (SQL NULL distinct from JSON null: id1 IS NULL, id2 not); Snowflake VARIANT EXACT (the text render of doubles in TO_JSON shows 16 digits, the VARIANT value equals the double: checked `J::DOUBLE = ...`); ClickHouse String EXACT; Duck EXACT. FORM_ONLY: JSON numbers beyond 2^64 become doubles at the source, not in rivet.

### DATE
| value | Parquet | BQ | SF | CH | Duck |
|---|---|---|---|---|---|
| NULL, 1000-01-01, 1970-01-01, 1969-12-31, 2000-02-29, 1900-02-28, 1900-01-01, 2299-12-31, 2262-04-11/12 | Date32 EXACT | EXACT | EXACT | EXACT (table `date_in`, 5.7) | EXACT |
| 1899-12-31, 1677-09-20/21, 9999-12-31, 2300-01-01 (table `date`) | EXACT | EXACT | EXACT | REFUSED RIVET_LOAD_WAREHOUSE_REJECTED (Date32 range), load | EXACT |
| 0000-01-01, 0000-12-31 (valid in MySQL) | Date32 -719528, -719163 EXACT | DEFECT_NATIVE_REFUSED RIVET_LOAD_WAREHOUSE_REJECTED load exit 1 | EXACT | REFUSED RIVET_LOAD_WAREHOUSE_REJECTED | FORM_ONLY ("0001-01-01 (BC)") |
| 0000-00-00 | NULL + WARN, NULL_NONVALUE | NULL_NONVALUE | NULL_NONVALUE | NULL_NONVALUE | NULL_NONVALUE |
| 2020-00-00, 2020-01-00, 2020-00-15 | NULL + WARN, NULL_NONVALUE | NULL_NONVALUE | NULL_NONVALUE | NULL_NONVALUE | NULL_NONVALUE |

### DATETIME(0..6) and TIMESTAMP(0..6) NULL
Edge values: NULL; 1000-01-01 00:00:00 with every fraction width (.1 .12 ... .123456); 1970-01-01 00:00:01 (timestamp min); 2038-01-19 03:14:07 (timestamp max); 1969-12-31 23:59:59.999999; 2000-02-29 12:34:56; 1677-09-21, 2262-04-11; 1900-01-01; 2299-12-31; 9999-12-31 23:59:59.999999 (datetime); zero and partial datetimes.
| native | Parquet | BQ | SF | CH | Duck |
|---|---|---|---|---|---|
| DATETIME(0..6) all rows | Timestamp(us) EXACT | DATETIME EXACT | TIMESTAMP_NTZ EXACT | DateTime64(6,'UTC'): rows in 1900..2299 EXACT (table `datetime_in`, 5.7); table `datetime` REFUSED RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE ("0001-01-01 00:00 outside 1900-01-01 to 2299-12-31") | EXACT |
| TIMESTAMP(0..6) NULL | Timestamp(us,UTC) EXACT | TIMESTAMP EXACT | TIMESTAMP_TZ EXACT | DateTime64(6,'UTC') EXACT | EXACT (session time zone renders +01; instant equal) |
| 0000-00-00 00:00:00(.000000), 2020-00-00 00:00:00, 2020-01-00 12:00:00.5 (DATETIME and TIMESTAMP) | NULL + WARN | NULL_NONVALUE | NULL_NONVALUE | NULL_NONVALUE | NULL_NONVALUE |

### TIME(0..6)
| value | Parquet | BQ | SF | CH | Duck |
|---|---|---|---|---|---|
| NULL, 00:00:00, 00:00:01, 12:34:56, 23:59:59 with .1 to .123456 and 00:00:00.000001 | Time64(us) EXACT | TIME EXACT | TIME EXACT | Decimal(18,6) seconds: FORM_ONLY (value equal after conversion) | EXACT |
| 24:00:00, 25:30:15(.5), 100:00:00, 838:59:59 | REFUSED RIVET_SOURCE_VALUE_UNREPRESENTABLE extract exit 5: DEFECT_NATIVE_REFUSED | UNMEASURED(not extracted) | | | |
| -00:00:01, -00:00:00.000001, -01:00:00, -838:59:59 | same refusal | | | | |

### YEAR
| value | Parquet | BQ | SF | CH | Duck |
|---|---|---|---|---|---|
| NULL, 1901, 2155, 1902, 2000, 1970, 2154 | Int16 EXACT | EXACT | EXACT | Int16 EXACT | EXACT |
| 0000 | Int16 0 (YEAR 0000 is stored as 0): FORM_ONLY | FORM_ONLY | FORM_ONLY | FORM_ONLY | FORM_ONLY |

### GEOMETRY, POINT, LINESTRING, POLYGON, GEOMETRYCOLLECTION, MULTIPOINT
All: REFUSED RIVET_TYPE_UNSAFE_MAPPING, extract, exit 1. Not delivered to any target. 5.7 = 8.0.

### 8.0-only types
MySQL 8.0 adds no new column type beyond 5.7 (functional indexes, JSON partial updates, no VECTOR in 8.0.x). UNMEASURED(none exist in this server); `INT UNSIGNED ZEROFILL` and display widths are deprecated but accepted, and measured above.

## Commands used
```
mysql -h127.0.0.1 -P3557|3580 -urivet -privet rivet < seed.sql seed2.sql seed3.sql   (SET SESSION sql_mode=''; time_zone='+00:00')
python3 gen.py                                  # v57|v80/{run,bq,sf,ch}.yaml
rivet run  -c v57/run.yaml -e <table>_57       # one export at a time (logs/run_*.log, run_summary.txt)
rivet load -c v57/bq.yaml --summary-output logs/bq57.json   # also sf.yaml, ch.yaml (CH_PW=rivet), v80 likewise, sequentially per version
python3 cmp2.py | bq_grade.py <ver> | sf_grade.py <ver> | ch_grade.py <ver> | duck_grade.py <ver>
python3 regrade.py <bq|sf|ch> <ver> <table...>
```
Config shape: top-level `load: {target: bigquery|snowflake|clickhouse, ..., pk: none}`; ClickHouse database tc_edge_mysql.

## UNMEASURED
- UNMEASURED(freeze): the DuckDB 5.7 grade is partial (render artifacts in my script: string octet_length, decimal JSON, timestamp zone); DuckDB 8.0 not run; Parquet cells for ClickHouse date_in and datetime_in only on 5.7; BigQuery 5.7 per-column grade is from an earlier pass and was not re-run after the float oracle was changed (no differences expected); the table-level splits for BIT(64) top-bit and TIME refusals were not loaded anywhere (the values are not extractable).
- UNMEASURED(no value): NaN/Inf (MySQL cannot store), -0.0 sign bit, LONGTEXT/LONGBLOB above 16 MB, Snowflake BINARY above 8 MB.

## Cleanup (all verified absent)
- MySQL 5.7 and 8.0: all `tc_edge_mysql_*` tables dropped (SHOW TABLES count 0 on both ports).
- BigQuery: `bq rm -r -f -d rivet-data-tool:rivet_tmp_edge_mysql` (no dataset named edge_mysql listed).
- GCS: `gsutil rm -r gs://rivet_data_test/rivet-live/rivet_tmp_edge_mysql/` (listing: no match).
- Snowflake: all `TC_EDGE_MYSQL_*` tables dropped (information_schema count 0).
- ClickHouse: `DROP DATABASE tc_edge_mysql SYNC` (count 0).
- Local: scratchpad/edge-mysql/ (scripts, logs, parquet copies) remains; delete it to reclaim space.
