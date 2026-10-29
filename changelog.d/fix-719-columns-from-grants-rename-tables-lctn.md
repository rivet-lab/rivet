- **Behaviour change, MySQL CDC statement reading (the `columns_from: grants` rename refusals are not yet
  released; the case, comment and character-set parts below also apply without it): a rename, truncate or
  drop is no longer missed behind a six-digit version comment, in another case, or after a multibyte
  character.** The binlog statement is now read the way the server reads it. A version comment takes five
  digits, or six when the seventh character is a blank (`/*!080400 RENAME TABLE ... */` on MySQL 8.4); on
  a server with `lower_case_table_names` 1 or 2 (read once when the stream opens) `TRUNCATE SHOP.ORDERS`,
  `DROP TABLE`, a rename and a statement-logged write name the captured `shop.orders`, and on a server with
  0 a table in another case is another table (a column rename of it is no longer refused); a statement sent
  in `gbk`, `gb18030`, `sjis`, `cp932` or `big5` is decoded in that character set, so a multibyte character
  ending in a backslash no longer hides a `RENAME COLUMN`; `RENAME TABLES` and `ALTER TABLE t RENAME = t2`
  are table renames like `RENAME TABLE` and `ALTER TABLE t RENAME TO t2`. Each of these was delivered or
  ignored with exit 0 and the renamed table's or column's later changes were lost; each is now refused
  with `RIVET_SOURCE_CDC_UNDECODABLE` or `RIVET_SOURCE_CDC_TRUNCATED` (exit 5) at the statement. The
  checkpoint and the state written before the upgrade are read as before: a stream whose backlog already
  holds such a statement stops at it on the first run of the new version. **What to do:** nothing unless a
  run stops; then follow the remedy the message prints (the re-baseline), after a `GRANT SELECT` on the
  columns of the table now under the name when `columns_from: grants` is set.
