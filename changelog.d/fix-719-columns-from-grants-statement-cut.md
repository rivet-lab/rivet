- **Behaviour change, fourth review of `source.columns_from: grants` (MySQL, not yet released): a statement sent
  in a client character set other than UTF-8 is cut into characters and names as the server does, and a
  `CHANGE` that only changes the case of a column name is a rename.** Measured against 38,261 statements
  per server on MySQL 8.0 and 8.4 (`lower_case_table_names` 0 and 1), the previous reading let 988 refusals
  through per server: a name written after a lead byte that starts no character (the server drops it with
  the rest of the name), in `ujis`, `eucjpms`, `euckr`, `gb2312` and the other pair layouts it did not cut,
  with a single-byte name read as an unreadable byte instead of the character the set gives it, and with
  the NUL that ends a statement counted as a character. The reading now follows the server on all of
  them (missed 0, and no statement the server logged is refused without cause). A `CHANGE name NAME`
  (the server keeps the new spelling, so later changes of that column carry the new name) is now refused
  with `RIVET_SOURCE_CDC_UNDECODABLE` (exit 5) like any other rename; it used to pass as the same column.
  **What to do:** nothing for a config without `columns_from`. For a config with it, a stream stopped by
  the new refusal follows the remedy the message ends with (re-baseline the table); nothing in state or in
  written tables changes shape, and a run resumed from a checkpoint written by the previous build reads
  its backlog with the new reading.
