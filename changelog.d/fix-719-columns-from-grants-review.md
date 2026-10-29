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
