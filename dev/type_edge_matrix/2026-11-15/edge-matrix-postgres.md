# Edge matrix: PostgreSQL 14 (pg18 differs only where noted)

Binary rivet 0.33.0 (main 5240ede7). Targets: BigQuery rivet-data-tool, Snowflake RIVET_LOAD_TEST.DEVBOX, ClickHouse 24.8 local, DuckDB reads the Parquet. Bucket rivet-live does not exist; used gs://rivet_data_test/rivet_tmp_edge_pg/.

FREEZE NOTE: the coordinator froze the machine at 21:15 CET. The BigQuery value dump covered 19 of 87 loaded tables and the ClickHouse value dump 0 of 100 (no docker exec after the freeze), so most BQ/CH cells are the LOAD stage only (load exit and code) with the value grade UNMEASURED(freeze). Snowflake (97 tables) and DuckDB (97 tables) are value-graded. Snowflake cells marked UNVERIFIED differ in the dump but were not re-checked before the tables were dropped.

## Extract and load stage per table

| table | native type | extract (pg14) | BQ load | SF load | CH load |
|---|---|---|---|---|---|
| ad_d | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) |
| ad_iv | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| ad_t | ? | exit 0 | ok | ok | ok |
| ad_ts | ? | exit 0 | ok | ok | ok |
| ad_tz | ? | exit 0 | ok | ok | ok |
| ai_a2 | ? | exit 0 | ok | ok | ok |
| ai_a4 | ? | exit 0 | ok | ok | ok |
| ai_a8 | ? | exit 0 | ok | ok | ok |
| ai_b | ? | exit 0 | ok | ok | ok |
| ai_f4 | ? | exit 0 | ok | ok | ok |
| ai_f8 | ? | exit 0 | ok | ok | ok |
| ai_n | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| ai_n102 | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| am_a1 | ? | exit 0 | ok | ok | ok |
| am_a2 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| am_b1 | ? | exit 0 | ok | ok | ok |
| am_b3 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| ao_bi | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| ao_cp | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| ao_i | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| ao_iv | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| ao_m | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| ao_mo | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| ao_o | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| ao_pt | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| ao_x | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| arr_dt | date[], time[], timestamp[], timestamptz[], interval[] | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| arr_dt_inf | date[] | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| arr_int | smallint[], int[], bigint[], real[], double precision[], boolean[], numeric[], numeric(10,2)[] | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| arr_multi | int[][], text[][][] | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| arr_other | inet[], macaddr[], bit(3)[], money[], int4range[], tc_edge_pg_ctype[], xml[], point[], oid[] | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| arr_str | text[], varchar(5)[], char(3)[], bytea[], json[], jsonb[], uuid[], tc_edge_pg_mood[] | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| arr_ts_inf | timestamptz[] | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| as_by | ? | exit 0 | ok | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | ok |
| as_c | ? | exit 0 | ok | ok | ok |
| as_e | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| as_j | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| as_jb | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| as_t | ? | exit 0 | ok | ok | ok |
| as_u | ? | exit 0 | ok | ok | ok |
| as_vc | ? | exit 0 | ok | ok | ok |
| bit | bit(8), bit(1), bit(64), bit(65) | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| bit_v | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| bit_w | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| bit_x | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| bit_y | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| bool | boolean | exit 0 | ok | ok | ok |
| bpchar | bpchar | exit 0 | ok | ok | ok |
| bytea | bytea | exit 0 | ok | ok | ok |
| char | char(10) | exit 0 | ok | ok | ok |
| cidr | cidr | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| comp | tc_edge_pg_ctype | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| date | date | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| date_fin | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| date_ninf | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| date_ok | ? | exit 0 | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | ok | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) |
| date_pinf | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| date_r02 | ? | exit 0 | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | ok | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) |
| date_r03 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) |
| date_r04 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) |
| date_r05 | ? | exit 0 | ok | ok | ok |
| date_r06 | ? | exit 0 | ok | ok | ok |
| date_r07 | ? | exit 0 | ok | ok | ok |
| date_r08 | ? | exit 0 | ok | ok | ok |
| date_r09 | ? | exit 0 | ok | ok | ok |
| date_r10 | ? | exit 0 | ok | ok | ok |
| date_r11 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) |
| date_r12 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| date_r13 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| date_r14 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| date_r15 | ? | exit 0 | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | ok | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) |
| date_r16 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) |
| date_r17 | ? | exit 0 | ok | ok | ok |
| domain | tc_edge_pg_dom | exit 0 | ok | ok | ok |
| enum | tc_edge_pg_mood | exit 0 | ok | ok | ok |
| float4 | real | exit 0 | ok | ok | ok |
| float8 | double precision | exit 0 | ok | ok | ok |
| geo_b | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| geo_c | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| geo_l | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| geo_ls | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| geo_p | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| geo_pa | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| geo_po | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| inet | inet | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| int2 | smallint | exit 0 | ok | ok | ok |
| int4 | int | exit 0 | ok | ok | ok |
| int8 | bigint | exit 0 | ok | ok | ok |
| interval | interval | exit 0 | ok | ok | ok |
| json | json | exit 0 | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | ok |
| jsonb | jsonb | exit 0 | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | ok | ok |
| macaddr | macaddr | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| macaddr8 | macaddr8 | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| money | money | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| money_arr_note | ? | exit 0 | - | - | - |
| multirange | int4multirange | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| name | name | exit 0 | ok | ok | ok |
| num_1000 | numeric(1000,0) | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| num_38_0 | numeric(38,0) | exit 0 | ok | ok | ok |
| num_38_10 | numeric(38,10) | exit 0 | ok | ok | ok |
| num_38_38 | numeric(38,38) | exit 0 | ok | REFUSED(RIVET_LOAD_TEST, load, exit 1) | ok |
| num_76_0 | numeric(76,0) | exit 0 | REFUSED(RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | REFUSED(RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | ok |
| num_inf | numeric | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| num_nan | numeric | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| num_ninf | numeric | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| num_p5 | numeric(5,2) | exit 0 | ok | ok | ok |
| num_unb | numeric | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| oid | oid, regclass, regtype, xid, cid, xid8, pg_lsn, tid | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| oid_c | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| oid_l | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| oid_r | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| oid_t | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| oid_tid_ | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| oid_v | ? | exit 0 | ok | ok | ok |
| oid_x | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| oid_xx | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| point | point, line, lseg, box, path, polygon, circle | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| qchar | "char" | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| range | int4range, int8range, numrange, daterange, tsrange, tstzrange | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| rng_d | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| rng_i4 | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| rng_i8 | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| rng_nr | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| rng_t | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| rng_tz | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| text | text | exit 0 | ok | ok | ok |
| time | time | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| time_24 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| time_fin | ? | exit 0 | ok | ok | ok |
| timetz | timetz | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| ts | timestamp | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| ts_fin | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| ts_ninf | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| ts_ok | ? | exit 0 | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| ts_p | timestamp(0), timestamp(1), timestamp(2), timestamp(3), timestamp(4), timestamp(5), timestamp(6), timestamptz(0), timestamptz(3), timestamptz(6), time(0), time(3), time(6) | exit 0 | ok | ok | ok |
| ts_pinf | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| ts_r02 | ? | exit 0 | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| ts_r03 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| ts_r04 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| ts_r05 | ? | exit 0 | ok | ok | ok |
| ts_r06 | ? | exit 0 | ok | ok | ok |
| ts_r07 | ? | exit 0 | ok | ok | ok |
| ts_r08 | ? | exit 0 | ok | ok | ok |
| ts_r09 | ? | exit 0 | ok | ok | ok |
| ts_r10 | ? | exit 0 | ok | ok | ok |
| ts_r11 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| ts_r12 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| ts_r13 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| ts_r14 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| ts_r15 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| ts_r16 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| ts_r17 | ? | exit 0 | ok | ok | ok |
| tsp_p0 | ? | exit 0 | ok | ok | ok |
| tsp_p1 | ? | exit 0 | ok | ok | ok |
| tsp_p2 | ? | exit 0 | ok | ok | ok |
| tsp_p3 | ? | exit 0 | ok | ok | ok |
| tsp_p4 | ? | exit 0 | ok | ok | ok |
| tsp_p5 | ? | exit 0 | ok | ok | ok |
| tsp_p6 | ? | exit 0 | ok | ok | ok |
| tsp_t0 | ? | exit 0 | ok | ok | ok |
| tsp_t3 | ? | exit 0 | ok | ok | ok |
| tsp_t6 | ? | exit 0 | ok | ok | ok |
| tsp_z0 | ? | exit 0 | ok | ok | ok |
| tsp_z3 | ? | exit 0 | ok | ok | ok |
| tsp_z6 | ? | exit 0 | ok | ok | ok |
| tstz | timestamptz | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| tstz_fin | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| tstz_ninf | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| tstz_ok | ? | exit 0 | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| tstz_pinf | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| tstz_r02 | ? | exit 0 | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| tstz_r03 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| tstz_r04 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| tstz_r05 | ? | exit 0 | ok | ok | ok |
| tstz_r06 | ? | exit 0 | ok | ok | ok |
| tstz_r07 | ? | exit 0 | ok | ok | ok |
| tstz_r08 | ? | exit 0 | ok | ok | ok |
| tstz_r09 | ? | exit 0 | ok | ok | ok |
| tstz_r10 | ? | exit 0 | ok | ok | ok |
| tstz_r11 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| tstz_r12 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| tstz_r13 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| tstz_r14 | ? | exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE | - | - | - |
| tstz_r15 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| tstz_r16 | ? | exit 0 | ok | ok | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) |
| tsv_q | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| tsv_v | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| tsvector | tsvector, tsquery | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| uuid | uuid | exit 0 | ok | ok | ok |
| varbit | varbit(70), varbit | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| varbit_v | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| varbit_w | ? | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |
| varchar | varchar(10) | exit 0 | ok | ok | ok |
| xml | xml | exit 1 RIVET_TYPE_UNSAFE_MAPPING | - | - | - |

## Per-row cells (tables that were extracted)

### tc_edge_pg_ad_d  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| d#1 | `null` | list<item: date32[day]> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| d#2 | `[]` | list<item: date32[day]> | EXACT | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| d#3 | `["0001-01-01", null, "9999-12-31"]` | list<item: date32[day]> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
### tc_edge_pg_ad_t  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| t#1 | `null` | list<item: time64[us]> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t#2 | `[]` | list<item: time64[us]> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t#3 | `["00:00:00.000000", null, "23:59:59.999999"]` | list<item: time64[us]> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | UNVERIFIED(sf differs: ["00:00:00", null, "23:59:59"]; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ad_ts  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| ts#1 | `null` | list<item: timestamp[us]> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| ts#2 | `[]` | list<item: timestamp[us]> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| ts#3 | `["1970-01-01 00:00:00.000000", null, "1969-12-31 2` | list<item: timestamp[us]> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | UNVERIFIED(sf differs: ["1970-01-01 00:00:00.000", null, "1969-; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ad_tz  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| tz#1 | `null` | list<item: timestamp[us, tz=UTC]> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| tz#2 | `[]` | list<item: timestamp[us, tz=UTC]> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| tz#3 | `["1970-01-01 00:00:00.000000+00", null]` | list<item: timestamp[us, tz=UTC]> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | UNVERIFIED(sf differs: ["1969-12-31 16:00:00.000 -0800", null]; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ai_a2  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| a2#1 | `null` | list<item: int16> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a2#2 | `[]` | list<item: int16> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a2#3 | `["1", null]` | list<item: int16> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a2#4 | `["-32768", "32767"]` | list<item: int16> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a2#5 | `[null]` | list<item: int16> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ai_a4  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| a4#1 | `null` | list<item: int32> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a4#2 | `[]` | list<item: int32> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a4#3 | `["1", null]` | list<item: int32> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a4#4 | `["-2147483648", "2147483647"]` | list<item: int32> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a4#5 | `[null]` | list<item: int32> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ai_a8  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| a8#1 | `null` | list<item: int64> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a8#2 | `[]` | list<item: int64> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a8#3 | `["1", null]` | list<item: int64> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a8#4 | `["-9223372036854775808", "9223372036854775807"]` | list<item: int64> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a8#5 | `[null]` | list<item: int64> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ai_b  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| b#1 | `null` | list<item: bool> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| b#2 | `[]` | list<item: bool> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| b#3 | `["t", null]` | list<item: bool> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| b#4 | `["f", "t"]` | list<item: bool> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| b#5 | `[null]` | list<item: bool> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ai_f4  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| f4#1 | `null` | list<item: float> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| f4#2 | `[]` | list<item: float> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| f4#3 | `["1.0", null]` | list<item: float> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| f4#4 | `["nan", "inf", "-inf", "-0.0"]` | list<item: float> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| f4#5 | `[null]` | list<item: float> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ai_f8  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| f8#1 | `null` | list<item: double> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| f8#2 | `[]` | list<item: double> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| f8#3 | `["1.0", null]` | list<item: double> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| f8#4 | `["nan", "inf", "-inf", "-0.0", "5e-324", "1.797693` | list<item: double> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | UNVERIFIED(sf differs: [NaN, Infinity, -Infinity, -0.0, 5e-324,; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
| f8#5 | `[null]` | list<item: double> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_am_a1  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| a#1 | `null` | list<item: int32> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| a#5 | `["1", "2"]` | list<item: int32> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_am_b1  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| b#1 | `null` | list<item: string> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| b#4 | `null` | list<item: string> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| b#5 | `null` | list<item: string> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_as_by  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| by#1 | `null` | list<item: binary> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| by#2 | `[]` | list<item: binary> | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| by#3 | `["\\x00", null]` | list<item: binary> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| by#4 | `["\\xff"]` | list<item: binary> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| by#5 | `[null]` | list<item: binary> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_as_c  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| c#1 | `null` | list<item: string> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| c#2 | `[]` | list<item: string> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| c#3 | `["a  ", null]` | list<item: string> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| c#4 | `["  a"]` | list<item: string> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| c#5 | `[null]` | list<item: string> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_as_t  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| t#1 | `null` | list<item: string> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t#2 | `[]` | list<item: string> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t#3 | `["a", null, ""]` | list<item: string> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t#4 | `["\ud83d\ude00", "NULL", "a,b", "{x}", "\"q\"", "\` | list<item: string> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t#5 | `[null]` | list<item: string> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_as_u  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| u#1 | `null` | list<item: extension<arrow.uuid>> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| u#2 | `[]` | list<item: extension<arrow.uuid>> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| u#3 | `["00000000-0000-0000-0000-000000000000", null]` | list<item: extension<arrow.uuid>> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| u#4 | `["ffffffff-ffff-ffff-ffff-ffffffffffff"]` | list<item: extension<arrow.uuid>> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| u#5 | `[null]` | list<item: extension<arrow.uuid>> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_as_vc  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| vc#1 | `null` | list<item: string> | DEFECT_WRONG_VALUE(NULL list -> [])  [known_red] | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| vc#2 | `[]` | list<item: string> | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| vc#3 | `["abc", null]` | list<item: string> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| vc#4 | `["abcde"]` | list<item: string> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| vc#5 | `[null]` | list<item: string> | FORM_ONLY(list element wrapped as REPEATED RECORD{item}) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_bool  (boolean)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | bool | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `t` | bool | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `f` | bool | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_bpchar  (bpchar)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | string | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `x` | string | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | ` ` | string | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_bytea  (bytea)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | binary | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `\x` | binary | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `\x00` | binary | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `\xff` | binary | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `\x000102030405060708090a0b0c0d0e0f1011121314151617` | binary | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `\xabababababababababababababababababababababababab` | binary | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_char  (char(10))

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | string | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `          ` | string | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `a         ` | string | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `abcdefghij` | string | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `  ab      ` | string | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `😀         ` | string | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_date_ok  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#2 | `4713-01-01 BC` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT(astronomical year) | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#3 | `0001-01-01` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#4 | `1677-09-21` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#5 | `1900-01-01` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#6 | `1969-12-31` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#7 | `1970-01-01` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#8 | `2000-02-29` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#9 | `2262-04-11` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#10 | `2299-12-31` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#11 | `9999-12-31` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#15 | `0001-12-31 BC` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT(astronomical year) | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#16 | `1899-12-31` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
| v#17 | `2300-01-01` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
### tc_edge_pg_date_r02  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#2 | `4713-01-01 BC` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNVERIFIED(sf differs: 4713-01-01; table dropped before recheck) | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
### tc_edge_pg_date_r03  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#3 | `0001-01-01` | date32[day] | EXACT | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
### tc_edge_pg_date_r04  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#4 | `1677-09-21` | date32[day] | EXACT | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
### tc_edge_pg_date_r05  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#5 | `1900-01-01` | date32[day] | EXACT | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_date_r06  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#6 | `1969-12-31` | date32[day] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_date_r07  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#7 | `1970-01-01` | date32[day] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_date_r08  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#8 | `2000-02-29` | date32[day] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_date_r09  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#9 | `2262-04-11` | date32[day] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_date_r10  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#10 | `2299-12-31` | date32[day] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_date_r11  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#11 | `9999-12-31` | date32[day] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
### tc_edge_pg_date_r15  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#15 | `0001-12-31 BC` | date32[day] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNVERIFIED(sf differs: 0001-12-31; table dropped before recheck) | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
### tc_edge_pg_date_r16  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#16 | `1899-12-31` | date32[day] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT |
### tc_edge_pg_date_r17  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#17 | `2300-01-01` | date32[day] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_domain  (tc_edge_pg_dom)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | int32 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `5` | int32 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_enum  (tc_edge_pg_mood)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `sad` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `happy` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_float4  (real)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | float | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `0.0` | float | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `nan` | float | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `inf` | float | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `-inf` | float | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `-0.0` | float | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#7 | `1.401298464324817e-45` | float | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#8 | `1.1754943508222875e-38` | float | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#9 | `3.4028234663852886e+38` | float | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#10 | `-3.4028234663852886e+38` | float | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_float8  (double precision)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | double | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `0.0` | double | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `nan` | double | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `inf` | double | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `-inf` | double | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `-0.0` | double | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#7 | `5e-324` | double | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#8 | `2.2250738585072014e-308` | double | UNMEASURED(freeze: dump not run) | UNVERIFIED(sf differs: 2.225073858507201e-308; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
| v#9 | `1.7976931348623157e+308` | double | UNMEASURED(freeze: dump not run) | UNVERIFIED(sf differs: inf; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
| v#10 | `-1.7976931348623157e+308` | double | UNMEASURED(freeze: dump not run) | UNVERIFIED(sf differs: -inf; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_int2  (smallint)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | int16 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `0` | int16 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `-32768` | int16 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `32767` | int16 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `-32767` | int16 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `32766` | int16 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_int4  (int)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | int32 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `0` | int32 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `-2147483648` | int32 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `2147483647` | int32 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `-2147483647` | int32 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `2147483646` | int32 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_int8  (bigint)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | int64 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `0` | int64 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `-9223372036854775808` | int64 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `9223372036854775807` | int64 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `-9223372036854775807` | int64 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `9223372036854775806` | int64 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_interval  (interval)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `PT0S` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `P178000000Y` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `P-178000000Y` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `P178956970Y7M` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `P-178956970Y-8M` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#7 | `P2147483647D` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#8 | `P-2147483648D` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#11 | `P1M-1DT0.000001S` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#12 | `P1Y2M3DT4H5M6.789012S` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#13 | `P-1M1DT-0.000001S` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#16 | `PT2147483647H` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#17 | `PT-2147483648H` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#18 | `PT0.000001S` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#19 | `PT-0.000001S` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_json  (json)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `null` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `{}` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `[]` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `{"a":18446744073709551616,"b":1e400,"c":1.00000000` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `""` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#7 | `"\u0000"` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#8 | `{"k":"日本語 \ud83d\ude00"}` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#9 | `  {"a" :   1 }  ` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#10 | `{"a":1,"a":2}` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#11 | `[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_jsonb  (jsonb)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `null` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNVERIFIED(sf NULL for json null literal?) | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `{}` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | FORM_ONLY({}) | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `[]` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | FORM_ONLY([]) | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `{"a": 18446744073709551616, "b": 10000000000000000` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNVERIFIED(sf differs: {'a': 18446744073709551616, 'b': inf, 'c; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `""` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNVERIFIED(sf differs: ; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
| v#7 | `{"k": "日本語 😀"}` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | FORM_ONLY({'k': '日本語 😀'}) | UNMEASURED(freeze: dump not run) | EXACT |
| v#8 | `{"a": 2}` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | FORM_ONLY({'a': 2}) | UNMEASURED(freeze: dump not run) | EXACT |
| v#9 | `{"a": 2, "b": 1}` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | FORM_ONLY({'a': 2, 'b': 1}) | UNMEASURED(freeze: dump not run) | EXACT |
| v#10 | `[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[` | extension<arrow.json> | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | FORM_ONLY([[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[) | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_name  (name)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `abc` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_num_38_0  (numeric(38,0))

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | decimal128(38, 0) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `0` | decimal128(38, 0) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `99999999999999999999999999999999999999` | decimal128(38, 0) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `-99999999999999999999999999999999999999` | decimal128(38, 0) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_num_38_10  (numeric(38,10))

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | decimal128(38, 10) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `0.0000000000` | decimal128(38, 10) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `9999999999999999999999999999.9999999999` | decimal128(38, 10) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `-9999999999999999999999999999.9999999999` | decimal128(38, 10) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `0.0000000001` | decimal128(38, 10) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `-0.0000000001` | decimal128(38, 10) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_num_38_38  (numeric(38,38))

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | decimal128(38, 38) | UNMEASURED(freeze: dump not run) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `0.00000000000000000000000000000000000000` | decimal128(38, 38) | UNMEASURED(freeze: dump not run) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `0.99999999999999999999999999999999999999` | decimal128(38, 38) | UNMEASURED(freeze: dump not run) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `0.00000000000000000000000000000000000001` | decimal128(38, 38) | UNMEASURED(freeze: dump not run) | REFUSED(RIVET_LOAD_TEST, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_num_76_0  (numeric(76,0))

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | decimal256(76, 0) | REFUSED(RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | REFUSED(RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `0` | decimal256(76, 0) | REFUSED(RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | REFUSED(RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `99999999999999999999999999999999999999999999999999` | decimal256(76, 0) | REFUSED(RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | REFUSED(RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | UNMEASURED(freeze: dump not run) | DEFECT_WRONG_VALUE(duck reads decimal256 as DOUBLE: 1e+76) |
| v#4 | `-9999999999999999999999999999999999999999999999999` | decimal256(76, 0) | REFUSED(RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | REFUSED(RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED, load, exit 1) | UNMEASURED(freeze: dump not run) | DEFECT_WRONG_VALUE(duck reads decimal256 as DOUBLE: -1e+76) |
### tc_edge_pg_num_p5  (numeric(5,2))

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | decimal128(5, 2) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `0.00` | decimal128(5, 2) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `999.99` | decimal128(5, 2) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `-999.99` | decimal128(5, 2) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `0.01` | decimal128(5, 2) | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_oid_v  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | int64 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `0` | int64 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `4294967295` | int64 | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_text  (text)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `a😀b` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `  lead and trail  ` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `tab	here
nl "q" 's' \ b` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_time_fin  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `00:00:00.000000` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `23:59:59.999999` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `00:00:00.000001` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `12:34:56.789012` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ts_ok  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#2 | `4713-01-01 00:00:00.000000 BC` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT(astronomical year) | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#3 | `0001-01-01 00:00:00.000000` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#4 | `1677-09-21 00:12:43.145224` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#5 | `1900-01-01 00:00:00.000000` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#6 | `1969-12-31 23:59:59.999999` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#7 | `1970-01-01 00:00:00.000000` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#8 | `2000-02-29 12:34:56.789012` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#9 | `2262-04-11 23:47:16.854775` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#10 | `2299-12-31 23:59:59.999999` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#11 | `9999-12-31 23:59:59.999999` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#15 | `1899-12-31 23:59:59.999999` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#16 | `2300-01-01 00:00:00.000000` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#17 | `1970-01-01 00:00:00.000001` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_ts_p  (timestamp(0), timestamp(1), timestamp(2), timestamp(3), timestamp(4), timestamp(5), timestamp(6), timestamptz(0), timestamptz(3), timestamptz(6), time(0), time(3), time(6))

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| p0#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p0#2 | `2000-01-01 00:00:00.000000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p1#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p1#2 | `2000-01-01 00:00:00.100000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p2#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p2#2 | `2000-01-01 00:00:00.120000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p3#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p3#2 | `2000-01-01 00:00:00.123000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p4#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p4#2 | `2000-01-01 00:00:00.123500` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p5#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p5#2 | `2000-01-01 00:00:00.123460` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p6#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p6#2 | `2000-01-01 00:00:00.123456` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| z0#1 | `<NULL>` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| z0#2 | `2000-01-01 00:00:00.000000+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| z3#1 | `<NULL>` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| z3#2 | `2000-01-01 00:00:00.123000+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| z6#1 | `<NULL>` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| z6#2 | `2000-01-01 00:00:00.123456+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t0#1 | `<NULL>` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t0#2 | `12:00:00.000000` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t3#1 | `<NULL>` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t3#2 | `12:00:00.123000` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t6#1 | `<NULL>` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t6#2 | `12:00:00.123456` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ts_r02  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#2 | `4713-01-01 00:00:00.000000 BC` | timestamp[us] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNVERIFIED(sf differs: 4713-01-01 00:00:00.000000000; table dropped before recheck) | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_ts_r03  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#3 | `0001-01-01 00:00:00.000000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_ts_r04  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#4 | `1677-09-21 00:12:43.145224` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_ts_r05  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#5 | `1900-01-01 00:00:00.000000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ts_r06  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#6 | `1969-12-31 23:59:59.999999` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ts_r07  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#7 | `1970-01-01 00:00:00.000000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ts_r08  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#8 | `2000-02-29 12:34:56.789012` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ts_r09  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#9 | `2262-04-11 23:47:16.854775` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ts_r10  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#10 | `2299-12-31 23:59:59.999999` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_ts_r11  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#11 | `9999-12-31 23:59:59.999999` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_ts_r15  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#15 | `1899-12-31 23:59:59.999999` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_ts_r16  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#16 | `2300-01-01 00:00:00.000000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_ts_r17  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#17 | `1970-01-01 00:00:00.000001` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_p0  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| p0#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p0#2 | `2000-01-01 00:00:00.000000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_p1  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| p1#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p1#2 | `2000-01-01 00:00:00.100000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_p2  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| p2#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p2#2 | `2000-01-01 00:00:00.120000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_p3  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| p3#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p3#2 | `2000-01-01 00:00:00.123000` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_p4  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| p4#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p4#2 | `2000-01-01 00:00:00.123500` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_p5  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| p5#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p5#2 | `2000-01-01 00:00:00.123460` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_p6  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| p6#1 | `<NULL>` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| p6#2 | `2000-01-01 00:00:00.123456` | timestamp[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_t0  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| t0#1 | `<NULL>` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t0#2 | `12:00:00.000000` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_t3  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| t3#1 | `<NULL>` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t3#2 | `12:00:00.123000` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_t6  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| t6#1 | `<NULL>` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| t6#2 | `12:00:00.123456` | time64[us] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_z0  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| z0#1 | `<NULL>` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| z0#2 | `2000-01-01 00:00:00.000000+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_z3  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| z3#1 | `<NULL>` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| z3#2 | `2000-01-01 00:00:00.123000+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tsp_z6  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| z6#1 | `<NULL>` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| z6#2 | `2000-01-01 00:00:00.123456+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tstz_ok  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#2 | `4713-01-01 00:00:00.000000+00 BC` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT(astronomical year) | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#3 | `0001-01-01 00:00:00.000000+00` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#4 | `1677-09-21 00:12:43.145224+00` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#5 | `1900-01-01 00:00:00.000000+00` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#6 | `1969-12-31 23:59:59.999999+00` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#7 | `1970-01-01 00:00:00.000000+00` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#8 | `2000-02-29 07:04:56.789012+00` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#9 | `2262-04-11 23:47:16.854775+00` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#10 | `2299-12-31 23:59:59.999999+00` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#11 | `9999-12-31 23:59:59.999999+00` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#15 | `1899-12-31 23:59:59.999999+00` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
| v#16 | `2300-01-01 00:00:00.000000+00` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_tstz_r02  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#2 | `4713-01-01 00:00:00.000000+00 BC` | timestamp[us, tz=UTC] | REFUSED(RIVET_LOAD_WAREHOUSE_REJECTED, load, exit 1) | UNVERIFIED(sf differs: 4713-01-01 00:00:00.000000000; table dropped before recheck) | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_tstz_r03  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#3 | `0001-01-01 00:00:00.000000+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_tstz_r04  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#4 | `1677-09-21 00:12:43.145224+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_tstz_r05  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#5 | `1900-01-01 00:00:00.000000+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tstz_r06  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#6 | `1969-12-31 23:59:59.999999+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tstz_r07  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#7 | `1970-01-01 00:00:00.000000+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tstz_r08  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#8 | `2000-02-29 07:04:56.789012+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tstz_r09  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#9 | `2262-04-11 23:47:16.854775+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tstz_r10  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#10 | `2299-12-31 23:59:59.999999+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_tstz_r11  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#11 | `9999-12-31 23:59:59.999999+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_tstz_r15  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#15 | `1899-12-31 23:59:59.999999+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_tstz_r16  (?)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#16 | `2300-01-01 00:00:00.000000+00` | timestamp[us, tz=UTC] | UNMEASURED(freeze: dump not run) | EXACT | REFUSED(RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE, load, exit 5) | EXACT |
### tc_edge_pg_uuid  (uuid)

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | extension<arrow.uuid> | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `00000000-0000-0000-0000-000000000000` | extension<arrow.uuid> | UNMEASURED(freeze: dump not run) | UNVERIFIED(sf differs: 3030303030303030303030303030303030303030; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `ffffffff-ffff-ffff-ffff-ffffffffffff` | extension<arrow.uuid> | UNMEASURED(freeze: dump not run) | UNVERIFIED(sf differs: 4646464646464646464646464646464646464646; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `6ba7b810-9dad-11d1-80b4-00c04fd430c8` | extension<arrow.uuid> | UNMEASURED(freeze: dump not run) | UNVERIFIED(sf differs: 3642413742383130394441443131443138304234; table dropped before recheck) | UNMEASURED(freeze: dump not run) | EXACT |
### tc_edge_pg_varchar  (varchar(10))

| col#row | source | arrow | BQ | SF | CH | DuckDB |
|---|---|---|---|---|---|---|
| v#1 | `<NULL>` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#2 | `` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#3 | `abcdefghij` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#4 | `  ab  ` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#5 | `😀😀` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |
| v#6 | `日本語` | string | UNMEASURED(freeze: dump not run) | EXACT | UNMEASURED(freeze: dump not run) | EXACT |

## DEFECT list (silent first)

Measured on PG14; PG18 differs only in `interval` (extra rows not loaded) and `numeric(5,-2)` (exit 1, catalog-lookup refusal).

1. BigQuery: a NULL list column lands as `[]` (DEFECT_WRONG_VALUE, silent; matches the known_red NULL-list item). Seen in every `ad_*`, `ai_*`, `am_*`, `as_*` table, row with a NULL array. BQ also receives lists as `REPEATED RECORD{item}` (FORM_ONLY, value intact), and uuid[] as base64 BYTES (FORM_ONLY). Command: `rivet run` then `rivet load` with `target: bigquery`, table `tc_edge_pg_ai_a2`.
2. Snowflake (UNVERIFIED, tables dropped before recheck; dumps differ from source): float8 `1.7976931348623157e+308` and `-1.797...e+308` read back as `inf`/`-inf`, and `2.2250738585072014e-308` as `2.225073858507201e-308` (tc_edge_pg_float8 rows 8-10); jsonb `1e400` read as `inf`; jsonb JSON `null` literal read as SQL NULL (tc_edge_pg_jsonb row 2); list of time/timestamp loses fractional digits in the dump. Each may be a dump-format artifact (to_json of FLOAT/VARIANT): re-measure with a direct `SELECT TO_VARCHAR` and `HEX_ENCODE`.
3. DuckDB reads `decimal256(76,0)` (numeric(76,0)) as DOUBLE: `1e+76` for a 76-digit value (DEFECT_WRONG_VALUE on the reader, tc_edge_pg_num_76_0 rows 3-4). Rivet itself wrote a correct Decimal256 Parquet; BQ and SF refuse the load with RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED (acceptable REFUSED).
4. DEFECT_NATIVE_REFUSED (coded, extract): 1-D arrays of enum/json/jsonb/interval/oid fail with RIVET_SOURCE_VALUE_UNREPRESENTABLE and a misleading "MULTI-dimensional" message (tables as_e, as_j, as_jb, ad_iv, ao_o, arr_str). Cause: `src/source/postgres/arrow_convert.rs` Utf8 arm of `build_pg_list_array` (~line 970) decodes `Vec<Option<String>>`; message at `array_decode_message` ~888-899.
5. DEFECT_NATIVE_REFUSED (coded, extract): `numeric` unbounded, NaN, +-Inf, numeric(1000,0) and `numeric[]` give RIVET_TYPE_UNSAFE_MAPPING exit 1 (ai_n, ai_n102, ledger `batch_refuses`: accepted). Money, inet, cidr, macaddr, macaddr8, bit/varbit, timetz, geometric, ranges, multirange, composite, oid family, "char" and their arrays: RIVET_TYPE_UNSAFE_MAPPING exit 1 ("no Rivet mapping for this PostgreSQL type", arrow_convert.rs ~302/350). Most are in `known_red` no_silent_null_load[postgres]; money/inet/cidr/numeric[] are ledger `batch_refuses`.
6. REFUSED (coded, extract, exit 5 RIVET_SOURCE_VALUE_UNREPRESENTABLE): date/time/timestamp/timestamptz `infinity`/`-infinity` and `time 24:00:00` (arrow_convert.rs ~222), arrays containing them, and genuine multi-dim arrays. Acceptable by contract (Arrow cannot hold infinity), but a whole run is refused; split tables r12-r14 hold the rest.
7. REFUSED (coded, load): BigQuery cannot hold dates/timestamps before 0001-01-01 or the 4713 BC minimum (RIVET_LOAD_WAREHOUSE_REJECTED: `Invalid date value -2440550`, `Invalid datetime value`), tables date_ok/date_r02/date_r15, ts_ok/ts_r02, tstz_ok/tstz_r02; json 1e400 and 2^64 numbers (json, jsonb; RIVET_LOAD_WAREHOUSE_REJECTED); ClickHouse refuses outside 1900..2299 with RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE (src/load/clickhouse.rs:1075) and RIVET_LOAD_WAREHOUSE_REJECTED for dates. Snowflake refuses json (RIVET_LOAD_WAREHOUSE_REJECTED HTTP 422), numeric(38,38) and numeric(76,0).
8. Known ledger items not re-graded here (ClickHouse value dump frozen): CH uuid as FixedString(16), CH TIME as Decimal(18,6), CH NULL list as [].

## Verdict counts (cells = row x column x target)

See counts.json equivalent below; BQ/CH value grades are mostly UNMEASURED(freeze).

- bq: {'DEFECT_WRONG_VALUE': 19, 'EXACT': 36, 'FORM_ONLY': 38, 'REFUSED': 70, 'UNMEASURED': 188}
- sf: {'EXACT': 304, 'UNVERIFIED': 17, 'REFUSED': 24, 'FORM_ONLY': 6}
- ch: {'REFUSED': 62, 'UNMEASURED': 289}
- duck: {'EXACT': 349, 'DEFECT_WRONG_VALUE': 2}

## Commands used

- Create/insert: `docker exec -i stand-pg14-batch-1 psql -U rivet -d rivet` and `stand-pg18-batch-1` with types.sql, split.sql etc. (scratch edge-pg/).
- Extract: `~/wt/n995/target/release/rivet init` then `run` per table (PG14 :5514, PG18 :5518), default mapping.
- Load: `rivet run` with `destination: gcs rivet_data_test/rivet_tmp_edge_pg/v14/<table>/` then `rivet load` with `target: bigquery|snowflake|clickhouse` (edge-pg/load.sh, loadall.sh).
- Grade: DuckDB CLI over the Parquet (ddb.py), bq query with TO_JSON_STRING (wh.py), snowflake connector (sfdump.py), cmp.py comparator.

## Cleanup (verified absent)

- PG14, PG18: all `tc_edge_pg_*` relations and types dropped (0 left in pg_class and pg_type).
- BigQuery: `bq rm -r -f -d rivet-data-tool:rivet_tmp_edge_pg`, dataset list shows 0 edge_pg.
- GCS: `gsutil rm -r gs://rivet_data_test/rivet_tmp_edge_pg/`, ls: no matches.
- Snowflake: 93 `TC_EDGE_PG*` tables dropped, 0 left.
- ClickHouse: `DROP DATABASE tc_edge_pg`, 0 left.
- Local scratch left in edge-pg/ (no repo or ~/wt/n995 changes).
