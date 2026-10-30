- **Behaviour change, `source.columns_from: grants` (MySQL, not yet released): any statement in the binlog
  that names a captured table stops the stream and needs a re-baseline, `ADD INDEX` and `ADD COLUMN`
  included; a privilege change is found by a re-read, not by reading the statement.** The stream used
  to read each statement to tell a rename, a drop or a privilege change from the rest, and a statement
  written behind a comment, a quote, a version comment or a client character set could be read wrongly.
  It no longer reads statements for this. Under `columns_from: grants`, a statement (other than the frame
  of a transaction) whose bytes contain the name of a captured table, in any case and in any database, is
  refused with `RIVET_SOURCE_CDC_UNDECODABLE` (exit 5), naming the table; nothing past it is delivered.
  That covers `ALTER TABLE orders ADD INDEX ...` and `ALTER TABLE orders ADD COLUMN ...` as it covers a
  rename or a drop, and a table name inside a longer name or next to a multibyte character: an accepted
  over-stop. Every other statement outside a transaction marks one re-read of the grants (consecutive ones
  share it; row changes inside `BEGIN ... COMMIT` cost none), and the stream stops with
  `RIVET_SOURCE_CDC_GRANTS_CHANGED` (exit 2) only when a captured table's readable columns changed.
  Streams without `columns_from` are unchanged. **State written before the upgrade:** the feature is
  unreleased, so no released state holds it; a checkpoint written by an earlier build of this branch is
  read as before, and a backlog that already holds such a statement stops at it on the first run.
  **What to do:** after a stop, `GRANT SELECT` on the columns the export needs if they are missing, then
  follow the re-baseline the message prints (verified: the re-baselined stream delivers the table as it
  is now, and the statement is behind its new anchor). A cheaper path that lets a benign change such as
  `ADD INDEX` continue is a separate change.
