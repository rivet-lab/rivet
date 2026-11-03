- **Behaviour change: a count, span or ratio knob outside its range is now refused by name, and a key range as wide as `bigint` plans instead of panicking.**
  Before, `chunk_size` of 2^63 or more wrapped to a zero window, planned no chunk and the run ended exit 0 with 0 rows; `chunk_count`
  (or a `chunk_size` near `i64::MAX`) over a key range as wide as `bigint` overflowed 64-bit arithmetic (a panic in a debug build, a refusal that blamed a knob already set in a release build); `rivet cdc --rollover 0` ran although `cdc.rollover: 0` is refused; Mongo `page_size: 0` was clamped
  to 1; and `quality.null_ratio_max` accepted NaN, infinity, a value above 1 and a value below 0 (a threshold that can never fire).
  **Now** one table (`src/config/quantity.rs`) holds the range of each knob and every reader goes through it, and the key span and the
  window are computed in 128-bit width.
  **Changed stops:** `chunk_size` or `chunk_count` above 9223372036854775807 ends `RIVET_CONFIG_VALUE_OUT_OF_RANGE` (exit 1) at
  validate, plan and run (a sealed plan too); `rivet cdc --rollover 0` ends `RIVET_CONFIG_CDC_ROLLOVER_INVALID` (exit 1) before the stream
  is read; Mongo `source.mongo.page_size: 0` ends `RIVET_CONFIG_VALUE_OUT_OF_RANGE` (exit 1); a `quality.null_ratio_max` threshold outside
  0 to 1 or not a number ends `RIVET_CONFIG_VALUE_OUT_OF_RANGE` (exit 1) at validate and at run. The existing zero refusals (`chunk_size: 0`, `chunk_count: 0`, `cdc.rollover: 0`) keep their codes and their text.
  **What to do:** set the knob inside its range (omit `chunk_size` and `rollover` for the default 100000; give `null_ratio_max` a share
  from 0 to 1; omit Mongo `page_size` to read with one cursor). A config that ran with a value inside the range keeps its windows,
  rows and files byte for byte; no state or table written before the upgrade changes shape.
