- **Behaviour change, MySQL CDC statement reading (the `columns_from: grants` rename refusals are not yet
  released; the case, comment and character-set parts below also apply without it): a rename, truncate or
  drop is no longer missed behind a six-digit version comment, in another case, after a multibyte
  character, or behind a byte the client character set reads as a blank or as the start of a comment.**
  The binlog statement is now read the way the server reads it. A version comment takes five digits, or
  six when the seventh character is a blank (`/*!080400 RENAME TABLE ... */` on MySQL 8.4); on a server
  with `lower_case_table_names` 1 or 2 (read once when the stream opens) `TRUNCATE SHOP.ORDERS`, `DROP
  TABLE`, a rename and a statement-logged write name the captured `shop.orders`, and on a server with 0 a
  table in another case is another table (a column rename of it is no longer refused). A statement is cut
  into characters where the server cuts it in the client character set it was sent in: a multibyte
  character ending in a backslash (or a `sjis`/`cp932` pair the encoding standard cannot map) no longer
  hides a `RENAME COLUMN`, and `euckr`, `ujis`, `gb2312` and `eucjpms` are read byte by byte, as the
  server reads an ASCII byte after their lead bytes. What a single byte means is read from a table the
  server produced for every client collation (`dev/pytools/mysql_client_charsets.py`, the same on MySQL
  8.0 and 8.4): `0xA0` is a blank in `latin1`, `0x88`-`0x8C` and `0x9F` are blanks in `latin2_czech_cs`,
  every byte below `0x20` opens a comment after `--` in every collation, `0x7F` does so in all but 22,
  and `cp1250` and a few others open one after `0x81` and some more bytes. A statement is read once, in
  that one reading: a `CHANGE` is compared with the column it follows, a table name holding a byte that is
  not a letter is the run's only when a captured table has the same ASCII letters around it (`RENAME
  TABLE clientèle TO clientèle_old` in `latin1` is not a rename of a captured `client`), and a
  `RENAME TABLES` or `ALTER TABLE t RENAME = t2` is a table rename like `RENAME TABLE` and `ALTER TABLE t
  RENAME TO t2`. A UTF-8, `ascii` or `binary` statement is read as before, except for what the version
  comment and case rules above change. Each of these was delivered or ignored with exit 0 and the renamed
  table's or column's later changes were lost; each is now refused with `RIVET_SOURCE_CDC_UNDECODABLE` or
  `RIVET_SOURCE_CDC_TRUNCATED` (exit 5) at the statement. The checkpoint and the state written before the
  upgrade are read as before: a stream whose backlog already holds such a statement stops at it on the
  first run of the new version. **What to do:** nothing unless a run stops; then follow the remedy the
  message prints (the re-baseline), after a `GRANT SELECT` on the columns of the table now under the name
  when `columns_from: grants` is set.
