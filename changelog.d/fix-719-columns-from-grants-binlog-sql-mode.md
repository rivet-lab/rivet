- **Behaviour change, fourth review of `source.columns_from: grants` (MySQL, not yet released): a table
  rename of a captured table is refused, and the binlog does carry `sql_mode`.** An earlier note said the
  binlog does not carry `sql_mode`; it does (the QUERY event status variables hold it), and the reading of a
  statement now follows it. Under `NO_BACKSLASH_ESCAPES` a literal `'x\'` ended at the backslash for the
  server but not for rivet, so a `RENAME COLUMN` after it was never seen and the renamed column's later
  changes were lost with exit 0; under `ANSI_QUOTES` a `"x\"` name did the same. Both are now read as the
  server reads them, and the rename is refused with `RIVET_SOURCE_CDC_UNDECODABLE` (exit 5). The same
  refusal now covers `RENAME TABLE t TO t_old, t_new TO t` and `ALTER TABLE t RENAME [TO] t2` when either
  name is a captured table: MySQL keeps privileges with the name, so after a swap the grants read for `t`
  belong to another table. A rename between tables the run does not capture is read as before. A
  `TRUNCATE`, `DROP TABLE` or statement-logged write whose name is double-quoted (`ANSI_QUOTES`) or holds a
  backtick now names its table, so `columns_from` refuses it like the plain form; the `/*!NNNNN` version
  comment takes exactly five digits, as the server reads it. **What to do:** nothing for a config without
  `columns_from`; after a stop, `GRANT SELECT` on the columns of the table now under the name, then follow
  the remedy the message prints.
