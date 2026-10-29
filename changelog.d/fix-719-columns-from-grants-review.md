- **`source.columns_from: grants` (MySQL) is refused on every other engine, and the MySQL projection no
  longer trusts a partial row image, an executable comment or a dotted table name.** A new key, so no
  existing config changes meaning: a PostgreSQL, SQL Server, Oracle or MongoDB source that writes it is
  refused by every command that loads the config (`run`, `check`, `plan`, `doctor`, `validate`, `load`,
  `state`; `apply` rejects a plan that carries it) with `RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED` (exit 1, nothing connected or written);
  remove `columns_from:` from `source:` or point `source:` at a MySQL server. On MySQL, a change written
  with `binlog_row_image` = `MINIMAL` or `NOBLOB` is refused with `RIVET_SOURCE_CDC_PREREQUISITE` before
  it is projected (the remedy is the one the message names: `binlog_row_image = FULL`; the change is still
  in the binlog), and a `REVOKE` written as `/*!80000 REVOKE … */` stops an open run with
  `RIVET_SOURCE_CDC_GRANTS_CHANGED` like the plain form. **Behaviour change on every SQL CDC engine:**
  a qualified `table: schema.table` now matches only that schema and table; it no longer also captures a
  table literally named `schema.table` in another schema or database (its rows were delivered into the
  export). No working config is affected: a table literally named with a dot was already refused at the
  schema read (`cannot read`) on MySQL. A dotted MongoDB collection name is unchanged.
- **Behaviour change, review of `source.columns_from: grants` (MySQL, not yet released): the policy now judges
  the grants once per run and refuses, by code, what it used to deliver wrong or fail on with a driver
  error.** The readable columns and the key are read once before the anchor and shared by the snapshot legs,
  the stream open and the mid-run re-reads; a grant change in between stops the run with
  `RIVET_SOURCE_CDC_GRANTS_CHANGED` (exit 2, re-run). A change that touched only a hidden column, or a table
  the export does not capture, now moves the checkpoint as it does without the policy (it used to hold it
  back and re-read the same span). A column of a captured table renamed in the backlog is refused with
  `RIVET_SOURCE_CDC_UNDECODABLE` (exit 5; column privileges do not follow a rename, so the old name's rows
  would be delivered without the column): GRANT SELECT on the new name, then follow the re-baseline the
  message ends with. The key is judged by the server: a declared `load.pk` the account reads is accepted
  while the primary key is hidden, a generated invisible primary key is accepted, and an unreadable key is
  refused before anything is written with `RIVET_SOURCE_COLUMNS_UNREADABLE`. A hidden `cursor_column`,
  `chunk_column`, `chunk_by_key`, `time_column` or `partition_by` column, a column revoked after the plan
  (a sealed plan applied later) and a table-level schema change under a table grant are refused by the same
  or the grants-changed code, each naming the column and the remedy (`rivet plan` again, or GRANT SELECT).
  A bare table name in a batch export reads the connection's own database, as the export's `SELECT` does.
  `rivet check` and the start of a change-stream run now say that the MySQL stream is a client-side
  projection (data minimisation, not access control). **What to do:** nothing for a config without
  `columns_from`; every export of that config is read as before (verified old binary, then this one, on one
  checkpoint). A stream stopped by the new refusals: GRANT SELECT or re-plan as the message says, or re-run.
