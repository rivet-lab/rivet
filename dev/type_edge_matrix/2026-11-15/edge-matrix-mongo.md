# Edge-value matrix: MongoDB (4.4.30 on 27104, 5.0.33 on 27105, 8.0.13 on 27108), rivet 0.33.0 (5240ede7d)

Date 2026-11-15. Binary `~/wt/n995/target/release/rivet`. Config from `rivet init --source mongodb://127.0.0.1:<port>/tc_edge_mongo` (only the local destination path changed), `rivet run`, 21 collections per version, all exits 0.

## Delivery shape

Every collection is delivered as two columns: `_id` (Utf8) and `document` (Parquet JSON, Arrow extension `arrow.json`) holding the whole document as extended JSON, plus the two `_rivet_*` meta columns. There is no per-field typed column, so a field with mixed types across documents never casts. `source.mongo.json: canonical` switches the number rendering (tested on 27108).

## Grading method and scope

Source read with pymongo (`datetime_conversion=DATETIME_MS`); the delivered `document` string parsed with `bson.json_util.loads` (canonical, DATETIME_MS) and compared to the source document type by type and value, NaN equal to NaN, floats by `str`. DuckDB: `json_valid(document)` over every row, `json_extract(...)::HUGEINT` for Int64, `length(json_extract_string)` for the 1 MB string and the 16 MB document. All three server versions deliver byte-identical `_id` and `document` for all 21 collections.

BigQuery, Snowflake, ClickHouse: UNMEASURED(gcs-write-denied) for every cell. Loads stage through GCS and the permission layer denied the `rivet run` GCS write to `rivet_data_test` ("Modify Shared Resources"); not worked around. DuckDB has no load target; its column is "reads the delivered Parquet".

## Counts

| Verdict | Relaxed (default) | Canonical |
|---|---|---|
| EXACT (value and BSON type) | every edge except those listed next | every edge |
| FORM_ONLY: Int64 type tag lost, value exact | 10 values in 4 collections (i64 6, mixed 2, array 1, doc 1), identical on all three versions | 0 |
| NULL_NONVALUE | null and missing field rows are distinct and exact in the document | same |
| DEFECT_SILENT_NULL / WRONG_VALUE / NATIVE_REFUSED | 0 / 0 / 0 on the document column | 0 / 0 / 0 |
| `_id` column ambiguity (heterogeneous `_id`) | 1 pair collides (int 1 vs string "1"); WARN at run | same |
| BQ / SF / CH | UNMEASURED(gcs-write-denied), all cells | |

Grader note: UUID subtype 4 and subtype 3 compare as `Binary` with the same subtype and bytes; pymongo's UUID decode vs Binary is a grader artefact (1 cell), the delivered value is exact.

## Ranked DEFECT list

No silent loss found on the `document` column. Ranked observations:

1. **FORM_ONLY / type tag loss (relaxed mode).** `Int64(2)` is delivered as `2`, identical to an int32; `Int64(0)`, `9223372036854775807`, `-9223372036854775808`, `9007199254740993` are plain JSON numbers (documented: a JSON-number reader that clamps beyond 2^53 loses them; DuckDB `::HUGEINT` read all of them exactly). `source.mongo.json: canonical` keeps `$numberLong`/`$numberInt`/`$numberDouble`, graded exact. Site: `src/source/mongo/mod.rs:566-576` (`document_to_json`), option `src/config/source.rs:105-111`.
2. **`_id` column collisions on heterogeneous `_id`** (not on `document._id`). Int 1 and string "1" both render to `1`; the other ten `_id` types render distinct text (ObjectId hex, `{"$binary":..}`, `{"$date":..}`, `{"$numberDecimal":..}`, `{"k":1}`, `true`, `1099511627776`, `1.5`). Site: `src/source/mongo/mod.rs:429-434` (`id_to_string`). rivet warns at run time ("heterogeneous `_id` types ... both become ..."). A merge keyed on `_id` would conflate the two documents; key on `document._id`.
3. Observation, not rivet: `Timestamp(0,0)` at the top level of an inserted document is replaced by the server's current time (server rule); the edge was re-seeded as `Timestamp(0,1)`.
4. Observation: DuckDB `json_extract` maps JSON null and a missing field both to NULL; the `document` string keeps them distinct.

## Per-BSON-type tables (PQ document graded = DDB json_valid + extracts; relaxed unless noted)

| BSON type | Edges inserted | Verdict |
|---|---|---|
| double | 0.0, -0.0, 1.5, NaN, +Inf, -Inf, 5e-324, 2.2250738585072014e-308, +/-1.7976931348623157e308, 0.1, 9007199254740992.0 | EXACT (-0.0 kept as `-0.0`; NaN/Inf as `$numberDouble`) |
| int32 | 0, 1, -1, 2147483647, -2147483648 | EXACT |
| int64 | 0, 2^63-1, -2^63, 2^31, -2^31-1, 2^53+1 | FORM_ONLY relaxed (tag lost, value exact); EXACT canonical |
| decimal128 | +/-9.999..E+6144 (34 digits), 1E-6176, 0, -0, 0E-6176, NaN, +Inf, -Inf, 34-digit integer, 0.1, 12345678901234567890.12345678901234, 1.0E+40 | EXACT (`$numberDecimal` text, trailing zeros and sign of zero kept) |
| string | `''`, `' '`, plain, padded, NUL inside, tab/NL/CR, quotes+backslash+comma, emoji+e-acute+CJK, 1,000,000 chars, U+D7FF U+E000 | EXACT (Mongo keeps `''` distinct from null) |
| binData subtype 0 | empty, 0x00, 256 byte values, DEADBEEF, 100000 x 0xFF | EXACT (`$binary` base64 + subType) |
| binData 3 (legacy uuid), 4 (uuid), 2, 5, 9, 128 | one value each | EXACT (subtype and bytes preserved; uuid is not rewritten to `$uuid`) |
| objectId | 000..0, fff..f, 507f1f77bcf86cd799439011 | EXACT (`$oid`) |
| bool | true, false | EXACT |
| date | epoch, +1 ms, -1 ms, 0001-01-01 and one ms before, 9999-12-31T23:59:59.999Z and one ms after, +/-8.64e15 ms, +/-2^63 ms, 1900-01-01, +/-9.22e12 ms (1677/2262), ms-precision 2000-02-29T12:34:56.789, 2020-02-29T23:46:40.123 | EXACT (years 1970..9999 as ISO with ms, others as `$numberLong`) |
| timestamp | (0,1), (1,1), (4294967295,4294967295), (1700000000,5) | EXACT (`$timestamp`) |
| array | `[]`, `[1,2,3]`, nested 4 deep, `[1,"a",null,true,1.5]`, `[null]`, `[[]]`, docs with differing field types, NaN/Inf, decimal, objectId, binary, date members | EXACT (member types kept; int64 member FORM_ONLY relaxed) |
| object | `{}`, nested 4 deep with null leaf, array-in-doc, empty key `""`, key `a.b`, key `$x`, mixed BSON members | EXACT (4.4, 5.0 and 8.0 accepted the dotted and `$` keys inserted inside a sub-document) |
| regex | `^a.*b$` flags imxs, empty, `a\/b`, emoji with u | EXACT (options delivered sorted, same set) |
| minKey, maxKey | one each | EXACT (`$minKey`, `$maxKey`) |
| javascript, javascriptWithScope | one each | EXACT (`$code`, `$scope`) |
| undefined, symbol, dbPointer (deprecated) | one each via raw BSON | EXACT (`$undefined`, `$symbol`, `$dbPointer`) |
| null vs missing | explicit null, absent field, value | EXACT, distinguishable |
| mixed types in one field | 17 documents: int, long, double, string, bool, null, object, array, decimal, date, objectId, binary, timestamp, regex, NaN, int64 max, int32 max | EXACT; no cast, FORM_ONLY for the int64 tag |
| document at the 16 MB limit | one string of 16,777,016 chars | EXACT (DuckDB length equals source) |
| `_id` types | 1, 'a', ObjectId, int64, 1.5, binary, date, `{k:1}`, true, '1', decimal | EXACT for `document._id`; flat `_id` collides for 1 vs '1' (defect 2) |

## Commands used
- `~/rivet/.venv/bin/python edge-mongo/seed.py 27104 27105 27108`: creates database `tc_edge_mongo`, collections `tc_edge_mongo_*` (dbl, i32, i64, dec, str, bin, oid, bool, date, ts, arr, doc, rx, mm, js, nul, mixed, dep, keydot, keydollar, big, ids).
- `rivet init --source mongodb://127.0.0.1:<port>/tc_edge_mongo -o gen<port>.yaml`; `DATABASE_URL=... rivet run -c gen<port>.yaml` per port.
- Canonical: `gencanon.yaml` = `gen27108.yaml` with `source.mongo.json: canonical`.
- `edge-mongo/grade.py <outdir> [port]`, `edge-mongo/dump.py`; DuckDB checks inline.
- Files: `scratchpad/edge-mongo/`; ledger candidate `edge-ledger-mongo.yaml` (23 rows, all `new: true`).

## Cleanup (verified)
- `drop_database('tc_edge_mongo')` on 27104, 27105, 27108; `list_database_names()` afterwards shows no `tc_edge_mongo` on any.
- Nothing written to GCS, BigQuery, Snowflake or ClickHouse by the Mongo pass. Parquet outputs remain only in the scratchpad.
