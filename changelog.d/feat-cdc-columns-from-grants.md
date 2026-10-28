- **New: `source.columns_from: grants` (MySQL) delivers only the columns the source account may `SELECT`.**
  With it, every export of the source (batch `table:` reads in every mode, CDC snapshots, `backfill: auto`
  recipes and the binlog stream) reads and delivers the columns the server lets the account read; a hidden
  column never reaches the destination by name or by value, and an update that changed only a hidden column
  is not delivered. A `GRANT`/`REVOKE` in the binlog of an open run re-reads the grants and stops the run
  with `RIVET_SOURCE_CDC_GRANTS_CHANGED` (exit 2) when the readable set changed. A table whose key or every
  column is hidden is refused with `RIVET_SOURCE_COLUMNS_UNREADABLE`. In binlog CDC the server still sends
  the full row to rivet's memory (a MySQL protocol limit). Other engines are refused for now (#711).
  **What to do:** nothing; without `columns_from` nothing changes. To hide columns, grant `SELECT (cols)`
  to the account and add `columns_from: grants` under `source:`.
