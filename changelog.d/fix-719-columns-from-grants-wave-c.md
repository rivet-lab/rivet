- **Behaviour change, second review of `source.columns_from: grants` (MySQL, not yet released): the grants
  re-read is a fresh login, a rename written as a version comment is refused, and a snapshot leg that
  loses a column mid-run is a grant change.** The mid-run re-read used to reuse one session, which
  kept database-level and global privileges and login-time roles as they were when it opened, so a
  `REVOKE ... ON db.*` or a default role taken away never stopped the run; each re-read now logs in again,
  and consecutive privilege statements in the binlog share one re-read (taken at the next event that is
  not one, or at the end of the stream). A `/*!80000 ALTER TABLE t RENAME COLUMN a TO b */` is read as the
  statement the server runs and refused with `RIVET_SOURCE_CDC_UNDECODABLE` (exit 5) like the plain form.
  A snapshot leg whose `SELECT` is denied because a column was revoked after the grants were read ends
  with `RIVET_SOURCE_CDC_GRANTS_CHANGED` (exit 2) instead of `RIVET_SOURCE_COLUMNS_UNREADABLE` (exit 1).
  The partial-image refusal now ends with a remedy that works from the degraded state: set
  `binlog_row_image = FULL` for every writer of the table, then re-baseline the stream. On PostgreSQL CDC a
  relation whose name holds a dot is refused with `RIVET_SOURCE_CDC_TABLE_UNRESOLVED` (exit 1) naming
  `table:` as `table` or `schema.table`, instead of a raw catalog error. **What to do:** nothing for a
  config without `columns_from`; after a stop, re-run (exit 2) or follow the remedy the message names.
