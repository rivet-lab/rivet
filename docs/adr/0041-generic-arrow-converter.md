# ADR-0041: One Generic Arrow Column Builder, Engines Supply Cell Readers and Declared Overrides

- **Status:** Proposed. Only the regression nets exist at this ADR's date: the measured delivery of the type matrix, `dev/release_oracle/bench_builders.py`, and the CDC timing `cdc_build_column_throughput`. The builder itself is the target state.
- **Date:** 2026-11-20
- **Context:** The type layer is already generic: `RivetType`, `TypeMapping` and the per-target type mapping are shared by every engine. The step after it is not. Each batch converter has its own `match` over the target Arrow `DataType` that builds the column:
  - `build_array` in `src/source/postgres/arrow_convert.rs` (33 arms);
  - `build_array` in `src/source/mysql/arrow_convert.rs` (15 arms);
  - `build_array` in `src/source/mssql/arrow_convert.rs` (14 arms);
  - `build_column` in `src/source/oracle/arrow_convert.rs` (13 arms).

  The CDC path has a fifth copy, `build_column` in `src/source/cdc/value.rs` (49 arms over `RivetValue`). The value checksum (`src/source/value_checksum.rs`) reads cells through yet another per-engine accessor.

  Everything below the type is therefore written once per engine:
  - the null rule;
  - the `max_value_bytes` ceiling;
  - the refusal of a non-NULL value no arm reads (I2 of ADR-0040);
  - decimal scale;
  - timestamp unit and zone;
  - list recursion.

  The defects found while building the measured type matrix (2026-11-12..19) are this shape:
  - PG `json[]`/`interval[]`/`oid[]`/enum arrays: the export built the column, but the checksum accessor read nothing for the element, and the run crashed.
  - `json[]` lost the `arrow.json` item extension in one builder only.
  - SQL Server TIME(7) was built at microseconds in batch but would have gone to text in CDC.
  - MySQL TINYINT(1) was strict in no path.
  - Oracle NUMBER ±infinity was finite text in the batch builder only.

  Each fix touched one copy. The other copies stayed correct or wrong by luck.

---

## Decision

### D1 - One generic builder over the Arrow type

A single `build_column<R: CellReader>(dt, rows, idx, ctx) -> Result<ArrayRef>` holds one arm for each Arrow type rivet delivers:
- Boolean;
- Int8..Int64 and UInt8..UInt64;
- Float32/64;
- Decimal128/256, built from `(i128, scale)` or canonical text;
- Utf8/LargeUtf8;
- Binary/LargeBinary/FixedSizeBinary (uuid);
- Date32;
- Timestamp(unit, tz);
- Time64(µs | ns);
- the TextForm types of ADR-0038 (interval ISO, JSON with its extension);
- List(item), which recurses into the same function for the item.

The rules every engine shares live only in this function:
- **null:** absent or SQL NULL ⇒ `append_null`, and nothing else does;
- **ceiling:** `max_value_bytes` refuses with `RIVET_SOURCE_VALUE_TOO_LARGE`;
- **unread value:** a non-NULL value the reader cannot produce for the asked type refuses with `SOURCE_VALUE_UNREPRESENTABLE` and the column name (I2). There is no wildcard arm;
- **range:** unit conversion and range checks (ns overflow, Date32 range).

The function is generic over `R`, monomorphised per engine. There is no `dyn` dispatch per cell.

### D2 - Engines supply a `CellReader`, not a builder

```rust
trait CellReader {
    type Row;
    fn is_null(&self, row: &Self::Row, idx: usize) -> Result<bool>;
    fn int(&self, row: &Self::Row, idx: usize) -> Read<i64>   { Read::Unread }
    fn uint(&self, row: &Self::Row, idx: usize) -> Read<u64>  { Read::Unread }
    fn float(&self, row: &Self::Row, idx: usize) -> Read<f64> { Read::Unread }
    fn decimal(&self, row: &Self::Row, idx: usize) -> Read<DecimalCell<'_>> { Read::Unread }
    fn text(&self, row: &Self::Row, idx: usize) -> Read<Cow<'_, str>> { Read::Unread }
    fn bytes(&self, row: &Self::Row, idx: usize) -> Read<Cow<'_, [u8]>> { Read::Unread }
    fn datetime(&self, row: &Self::Row, idx: usize) -> Read<DateTimeCell> { Read::Unread }
    fn date(&self, row: &Self::Row, idx: usize) -> Read<NaiveDate> { Read::Unread }
    fn time_nanos(&self, row: &Self::Row, idx: usize) -> Read<i64> { Read::Unread }
    fn bool(&self, row: &Self::Row, idx: usize) -> Read<bool> { Read::Unread }
    fn list(&self, row: &Self::Row, idx: usize) -> Read<ListCell<'_, Self>> { Read::Unread }
}
```

`Read<T>` is `Value(T) | Unread | Refused(CellRefusal)`. A default `Unread` turns into the coded refusal of D1, never into NULL.

The engine readers:
- PG reads through `tokio_postgres::Row`;
- MySQL through `mysql::Row`;
- SQL Server through tiberius `ColumnData`;
- Oracle through `oracledb::Row` with its flag columns;
- CDC through `RivetValue`. `RivetValue` is the owned reader, so today's CDC `build_column` becomes this builder instantiated for it.

The value checksum's side A reads through the same reader. A column the export can build is a column the checksum can read, by construction (the PG special-array class).

### D3 - Engine semantics are declared overrides, listed in one table per engine

Where an engine's value means something the generic arm cannot know, the engine declares an override for a `(native type, Arrow type)` pair: `fn overrides() -> &'static [Override]`. Each `Override` names its pair, its builder and its `seeded!` switch.

The overrides at this ADR's date:

| Engine | Override | Why it is not the default arm |
|---|---|---|
| PostgreSQL | interval ⇒ ISO text; `json[]`/`jsonb[]` ⇒ List(arrow.json); text-protocol arrays; enum/oid elements | the wire value needs a PG decoder |
| MySQL | TINYINT(1)/BIT ⇒ strict bool; zero/partial dates; TIME outside a day ⇒ TimeBeyondDay text | the value decides the refusal |
| SQL Server | TIME(7)/DATETIME2(7) ⇒ ns; DATETIMEOFFSET; MONEY; sql_variant | scale is per column, read from the catalog hints |
| Oracle | unbounded NUMBER/FLOAT ±infinity via the row flag column; LOB empty flags; INTERVAL DS(7..9) ns; BINARY_FLOAT/DOUBLE specials | the value is decided by a server-side flag, not by the cell |
| CDC | spill tags; MySQL cell fixes | already decided before the builder |

A pair not in the table goes to the generic arm. An override that duplicates the generic arm is deleted. The matrix records, per row, whether delivery took an override or the default, by the measured delivery, not by reading the code.

### D4 - Migration: CDC first, then one engine per branch

The order is CDC, PostgreSQL, SQL Server, MySQL, Oracle.
- CDC first: it is already generic over a value enum, so it is the builder's first instance.
- Oracle last: it carries the most server-side flags.

Each branch replaces one engine's `build_array` with its `CellReader` plus overrides. An engine's old arms are deleted only when all of these hold:
1. **Measured delivery is unchanged.** `property_no_silent_null_load --engines E --targets duck,clickhouse --observe` before and after produces the same delivery and verdict for every row and edge value (DELIVERY_DIFFERS = 0, no new RED). The BigQuery and Snowflake legs run on the owner's machine (`MAC_CLOUD_RUN.md`) before the branch merges.
2. **The seeds still bite.** Every `seeded!` switch the engine's converter holds turns its cell RED when flipped (the seeded run of the release harness).
3. **CDC parity is unchanged.** The CDC type-fidelity matrix produces the same per-type result as the batch path.
4. **Throughput holds.** `bench_builders.py --compare baseline.json` passes on the same stand and the same N:
   - best CPU of the batch export ≤ baseline × 1.10 per engine;
   - best wall ≤ baseline × 1.20;
   - with `--cdc`, `build_column` rows/s ≥ baseline × 0.90 per type.

   The baseline is recorded from the commit before the branch, by the same release binary profile.

### Rejected

- **`dyn CellReader` per cell.** One virtual call per cell on the hottest loop. Monomorphisation keeps one builder in the source and lets the compiler inline each reader.
- **Converting every batch cell to `RivetValue` and reusing the CDC builder.** It allocates an owned `Vec<u8>` per text/bytes cell and copies each value once more. The reader borrows (`Cow`) instead.
- **Deduplicating with macros only.** It shortens the copies but keeps four of them. The null, ceiling and refusal rules would still be written per engine.
- **Big-bang rewrite of all engines.** It loses the per-engine before/after proof of D4.

## Consequences

- A new Arrow type is one arm, delivered by every engine whose reader can produce it. A new engine is a reader plus its overrides.
- The I2 refusal, the ceiling and the null rule have one implementation. The wildcard-arm count of ADR-0040 for the converters goes to zero by construction.
- The checksum cannot read less than the export builds.
- The per-engine override tables are the documented list of where engines differ. A reviewer reads one table, not four `match` blocks.
- Cost: the reader trait is a new seam that every engine change passes through. A value the generic arm reads wrongly is now wrong for every engine at once. That is why D4 holds every branch to the measured matrix on all engines, not only the one migrated.
