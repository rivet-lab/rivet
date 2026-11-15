# Edge-value matrix: Oracle 26ai Free 23.26.3 (FREEPDB1), rivet 0.33.0 (5240ede7d)

Date 2026-11-15. Binary `~/wt/n995/target/release/rivet`. Config from `rivet init` (`gen.yaml`), with `columns:` declarations only where `run` demands them. Source graded with python-oracledb; delivered value graded from the Parquet with pyarrow and again with DuckDB (`read_parquet`, `epoch_us`, `epoch_ns`, `json_valid`). rivet's own integrity line is not used.

## Scope and what is UNMEASURED

- Load targets BigQuery, Snowflake and ClickHouse are UNMEASURED(gcs-write-denied) for every cell. All three loads stage through GCS. The only bucket the stand offers is `rivet_data_test` (prefix `rivet-live/`; the `rivet-live` bucket in the brief does not exist), and the permission layer denied `rivet run` with `destination: gcs` ("Modify Shared Resources"). I did not work around it; the extract went to a local destination. Unblock with an explicit user grant to write `gs://rivet_data_test/rivet-live/rivet_tmp_edge_oracle/`, then run `rivet load` per table.
- DuckDB has no load target in `src/load`; the DuckDB column below is "DuckDB reads the delivered Parquet".
- Oracle facts that shape the matrix: `''` is stored as NULL in CHAR/VARCHAR2/NCHAR/NVARCHAR2/RAW (recorded below as NULL_NONVALUE); Oracle cannot store BINARY_FLOAT/DOUBLE -0 (`TO_BINARY_FLOAT('-0')` reads back +0); dates before 1582-10-15 are Julian in Oracle and delivered as the same calendar label in proleptic Gregorian.

Legend: EXACT, NULL_NONVALUE, REFUSED(code, stage, exit), DEFECT_SILENT_NULL, DEFECT_WRONG_VALUE, FORM_ONLY, UNMEASURED. Columns: PQ = Parquet from `rivet run` graded against Oracle; DDB = DuckDB reading that Parquet; BQ/SF/CH = UNMEASURED(gcs) in every row, not repeated.

## Counts (graded cells; exact totals per verdict were not tallied for the EXACT class, only the non-EXACT cells)

| Verdict | PQ extract | DuckDB read of that Parquet |
|---|---|---|
| EXACT | every edge not listed below | same |
| FORM_ONLY | bare NUMBER and FLOAT (text), intervals, VECTOR, XMLTYPE, JSON, ROWID | same |
| NULL_NONVALUE | the NULL row of every type, and `''` / `HEXTORAW('')` stored as NULL by Oracle | same |
| REFUSED (coded, exit 1) | TIMESTAMP(7..9), TZ/LTZ(7..9) undeclared (3 tables), `timestamp_ns` out of range (4 TS rows, 2 TZ rows), `decimal(5,7)` (1 table) | n/a |
| SUBMICRO_FLOOR (declared `timestamp`, by design) | 22 cells | 22 |
| DEFECT_WRONG_VALUE | 6 cells: NUMBER infinity x2, INTERVAL ns x4 | same |
| DEFECT_SILENT_NULL | 0 | 0 |
| DuckDB-reader artefact | n/a | 10 cells (TIMESTAMPTZ(ns) read truncated) |
| BQ / SF / CH | UNMEASURED(gcs-write-denied), all cells | |

## Ranked DEFECT list

1. **DEFECT_WRONG_VALUE, silent. NUMBER +/-Infinity delivered as a finite number.** Oracle stores NUMBER infinity (internal `~`, `-~`; `SELECT` shows `~`) and reaches it from a FLOAT(10) overflow of 9.99..E125. Delivered: `1` followed by 125 zeros, and `-` followed by zeros-run (`-00000...`, 127 chars) in the text column; source `TO_CHAR(.., 'TM9')` gives `~` / `-~`. Rows `TC_EDGE_ORA_NUM` id 4 and 5, column `N_FL10` (the same shape applies to any unconstrained NUMBER/FLOAT holding infinity). rivet logs nothing. Command: `rivet run -c local.yaml -e TC_EDGE_ORA_NUM`; grade: `g1.py`, `g3.py`. Suspect site (not mutated to prove it): the NUMBER-to-text path in `src/source/oracle/arrow_convert.rs` (`deliver_number_text` line 156, `number(r)` line 556).
2. **DEFECT_WRONG_VALUE (WARN only, fidelity=lossy). INTERVAL DAY(9) TO SECOND(9) loses nanoseconds.** `INTERVAL '-1 02:03:04.000000005' DAY(9) TO SECOND(9)` is delivered `-1 02:03:04.000000`; `0 00:00:00.000000001` becomes `0 00:00:00.000000`. rivet WARNs at run time. Sites: `src/source/oracle/arrow_convert.rs:202` and `:277`. The text is ISO so a reader cannot see the loss.
3. **`rivet init` writes a config that `rivet run` refuses (NUMBER(5,7)).** `init` emits `N_SGP: decimal(5,7)` under `columns:`; `run` fails `RIVET_TYPE_UNSAFE_MAPPING` "scale 7 exceeds precision 5 in 'decimal(5,7)'" (exit 1) from `src/types/override_type.rs:136`. The emitter is `src/init/yaml_scaffold.rs:791` / `:1669-1672`. Loud, but breaks the "generated config runs" contract. `decimal(7,7)` run exactly, value-checked.
4. REFUSED, acceptable: TIMESTAMP(7..9) and TZ/LTZ(7..9) undeclared fail `RIVET_TYPE_UNSAFE_MAPPING` at planning with the declaration to add; `timestamp_ns` refuses values outside 1677-09-21..2262-04-11 (year 0001, 2299, 9999, -4712) with `RIVET_SOURCE_VALUE_UNREPRESENTABLE`. Declared `timestamp` floors sub-microsecond digits on purpose (SUBMICRO_FLOOR, 22 cells, expected).
5. FORM_ONLY notes: NUMBER with no precision and FLOAT arrive as `decimal_plain` text; intervals as ISO text; VECTOR/XMLTYPE/ROWID/UROWID as Utf8; JSON as arrow.json. All values exact in the text.
6. DuckDB reader limitation, not a rivet defect: `epoch_us()` on Parquet `TIMESTAMP(NANOS, UTC)` truncates ns to us (10 cells, TZ9/LTZ9); pyarrow reads the exact int64 ns.

Not defects, checked: BINARY_FLOAT/DOUBLE NaN, +/-Inf, denormals, max/min are bit-exact in Float32/Float64; EMPTY_CLOB/EMPTY_BLOB are delivered as `''`/`b''` (non-NULL, correct; python-oracledb returns None for them, which my first grader mis-flagged); DATE -4712 is the same label as the source (Julian vs proleptic Gregorian labels differ by days as instants; the label is preserved).

## Per-type tables (PQ = DDB unless noted)

### NUMBER (no precision) - delivery decimal_plain (Utf8)
| Edge | PQ | DDB |
|---|---|---|
| 0, 1, -1, 0.1 | FORM_ONLY (exact text) | FORM_ONLY |
| 1E-130, -1E-130 | FORM_ONLY (exact) | FORM_ONLY |
| 9.9999999999999999999999999999999999999E125 and negative | FORM_ONLY (exact, 126/127 chars) | FORM_ONLY |
| 123456789012345678901234567890123456789 | FORM_ONLY (exact) | FORM_ONLY |
| infinity `~` / `-~` | DEFECT_WRONG_VALUE | DEFECT_WRONG_VALUE |
| NULL | NULL_NONVALUE | NULL_NONVALUE |

### FLOAT / FLOAT(10) - decimal_plain
Edges as NUMBER. FLOAT: FORM_ONLY all. FLOAT(10): max edges become infinity, see defect 1. NULL: NULL_NONVALUE.

### NUMBER(38,0) / INTEGER / SMALLINT - Decimal128(38,0)
0, +/-99999999999999999999999999999999999999, 2^63-1, 2^63, -2^63, 2^64-1: EXACT. NULL: NULL_NONVALUE.

### NUMBER(38,10) - Decimal128(38,10)
0, +/-9999999999999999999999999999.9999999999, +/-1E-10, 123456789012345678.1234567891: EXACT. NULL: NULL_NONVALUE.

### NUMBER(10,-3) - Decimal128(38,18)
0, +/-1000, +/-9999999000, 1234567890 (rounds to 1234568000 in Oracle), +/-1500: EXACT against Oracle's stored value. NULL: NULL_NONVALUE. Note the delivered scale 18 for a negative-scale column.

### NUMBER(10,2) - Decimal128(10,2): all EXACT; NUMBER(5,7): decimal(5,7) REFUSED (defect 3); with decimal(7,7) EXACT; NUMBER(18,0) Int64 EXACT; NUMBER(19,0) Decimal128(19,0) EXACT incl. 9999999999999999999 and +/-2^63; NUMBER(9) Int32 EXACT.

### BINARY_FLOAT - Float32 / BINARY_DOUBLE - Float64
0, NaN, +Inf, -Inf, min denormal, min normal, max, -max, 0.1: EXACT (NaN compared as NaN). -0: UNMEASURED(oracle cannot store it). NULL: NULL_NONVALUE.

### CHAR(10), CHAR(2000), VARCHAR2(4000), VARCHAR2(10 CHAR), NCHAR(10), NVARCHAR2(2000) - Utf8
'ab', max length, padded/trailing blanks (CHAR keeps its blank padding as Oracle returns it), `'  lead and trail  '`, `' '`, embedded NUL and tab/newline/CR LF, quote/comma/backslash, emoji + e-acute + CJK: EXACT. `''` inserted: stored as NULL by Oracle, delivered NULL: NULL_NONVALUE.

### CLOB / NCLOB - Utf8, BLOB - Binary
small, 100000 chars, about 4.29M chars (CLOB), emoji/CJK, embedded NUL: EXACT. EMPTY_CLOB/EMPTY_BLOB: EXACT (`''`, `b''`). 256 byte values and 100000 x 0xAB BLOB: EXACT. NULL: NULL_NONVALUE.

### RAW(16), RAW(2000), LONG, LONG RAW - Binary / Utf8
0x00, 0xFF x16, 0xFF x2000, all 256 values, DEADBEEF: EXACT. `HEXTORAW('')` stored as NULL: NULL_NONVALUE. LONG text and LONG RAW 00FF: EXACT. NULL: NULL_NONVALUE.

### DATE - Timestamp(us)
-4712-01-01, 0001-01-01, 1677-09-21 00:12:43, 1900-01-01, 1969-12-31 23:59:59, 1970-01-01, 2000-02-29 12:34:56, 2262-04-11 23:47:16, 2299-12-31 23:59:59, 9999-12-31 23:59:59: EXACT (label; pre-1582 Julian label kept). NULL: NULL_NONVALUE.

### TIMESTAMP(0), (3), (6) - Timestamp(us)
Same 10 dates, fractions .001/.999/.999999 and 0001/-4712 minima: EXACT. NULL: NULL_NONVALUE.

### TIMESTAMP(9)
Undeclared: REFUSED(RIVET_TYPE_UNSAFE_MAPPING, planning, exit 1). `timestamp_ns`: in-range 1677-09-21 00:12:43.145224192, 1900, 1969, 1970-01-01 00:00:00.000000001, 2000, 2262-04-11 23:47:16.854775807: EXACT Timestamp(ns); 0001, 2299, 9999, -4712: REFUSED(RIVET_SOURCE_VALUE_UNREPRESENTABLE, run, exit 1). Declared `timestamp`: Timestamp(us), 8 of 11 cells floored (SUBMICRO_FLOOR, by design), -4712 label kept. NULL: NULL_NONVALUE.

### TIMESTAMP(0/6) WITH TIME ZONE, WITH LOCAL TIME ZONE - Timestamp(us, UTC)
+00:00, +05:30, -14:00, +14:00, America/New_York, Europe/Kyiv, 0001, 9999, 1677, 2262: EXACT (instant correct in UTC). NULL: NULL_NONVALUE.

### TIMESTAMP(9) WITH (LOCAL) TIME ZONE
Undeclared: REFUSED(RIVET_TYPE_UNSAFE_MAPPING, planning, exit 1). `timestamp_tz_ns`: in range EXACT Timestamp(ns, UTC) read with pyarrow; DDB cells truncate ns (DuckDB reader limit, 10 cells). Out-of-range years REFUSED(RIVET_SOURCE_VALUE_UNREPRESENTABLE). Declared `timestamp_tz`: SUBMICRO_FLOOR as above.

### INTERVAL YEAR(9) TO MONTH - Utf8 ISO
0-0, +/-999999999-11, 1-1, 0-1, -0-1: FORM_ONLY (exact). NULL: NULL_NONVALUE.

### INTERVAL DAY(9) TO SECOND(9) - Utf8 ISO
0, +/-999999999 23:59:59 (whole seconds), 2 02:03:04: FORM_ONLY exact. Any nonzero nanosecond field: DEFECT_WRONG_VALUE (defect 2): 4 cells. NULL: NULL_NONVALUE.

### ROWID, UROWID - Utf8: 2 values each EXACT (string equal). NULL: NULL_NONVALUE.
### JSON - arrow.json: 9 documents incl. null, [], {}, 2^64 and -2^63-1 numbers, 1.5e300, emoji, 20-deep nesting, scalar "str", 1e400: EXACT on the JSON value (FORM_ONLY text). NULL: NULL_NONVALUE.
### XMLTYPE - Utf8: `<a/>`, attribute/comment/emoji document: EXACT (Oracle serialises with newlines; delivered identical to the serialised source). NULL: NULL_NONVALUE.
### BOOLEAN - Boolean: TRUE, FALSE EXACT; NULL: NULL_NONVALUE.
### VECTOR(3,FLOAT32), VECTOR(2,FLOAT64), VECTOR(2,INT8) - Utf8: values equal the oracledb text form: FORM_ONLY. NULL: NULL_NONVALUE.

## Commands used
- `./osql.sh < setup.sql` (+ fix.sql, fix2.sql, split.sql, split2.sql): create and fill `tc_edge_ora_*` (sqlplus inside `rivet-oracle-1`, user rivet).
- `rivet init --source oracle://rivet:rivet@127.0.0.1:1521/FREEPDB1 -o gen.yaml`; destination switched to local; `rivet run -c local.yaml` (16 of 19 exports ok); `local2.yaml` adds `timestamp_ns`/`timestamp_tz_ns`/`timestamp`/`timestamp_tz`/`decimal(7,7)` declarations as the refusals instruct.
- Grading: `grade.py`, `g1.py` .. `g7.py` (oracledb vs pyarrow vs DuckDB) in `edge-oracle/`.
- Files: `scratchpad/edge-oracle/`; ledger candidate `edge-ledger-oracle.yaml`.

## Cleanup (verified)
- Oracle: `drop.sql` dropped every `TC_EDGE_ORA%` table with PURGE; `SELECT count(*) FROM user_tables WHERE table_name LIKE 'TC_EDGE%'` = 0.
- BigQuery: `rivet_tmp_edge_oracle` removed (`bq ls` shows no dataset of that name; the `rivet_tmp_edge_mssql`/`_mysql` datasets belong to peers and were not touched).
- GCS: nothing written (`gsutil ls gs://rivet_data_test/rivet-live/` shows no oracle prefix). Snowflake, ClickHouse: nothing created.
- Local Parquet and scripts remain under the scratchpad only.
