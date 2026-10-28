- **A PostgreSQL CDC config written by `rivet init --mode cdc` was refused when another schema held a
  table of the same name.** `init` writes a `public` table unqualified, and every run stopped with
  `RIVET_SOURCE_CDC_TABLE_UNRESOLVED` ("could mean 2 different relations"); a `TRUNCATE` of the other
  schema's table was also taken for this stream's (0.32.2 and earlier). **Now** a bare name means the
  relation the connection's `search_path` resolves, as every statement of the run reads it, when that
  relation can carry a change; the other schema's changes and truncates are not this stream's. A
  bare name the search_path resolves to a view, or to nothing, is elected as before. **What to do:**
  nothing; a config that was refused for this runs as generated.
- **Behaviour change: a bare PostgreSQL CDC table name that two schemas hold is no longer refused as
  ambiguous.** With `orders` in both the `$user` schema and `public` and `table: orders`, a run stopped
  with `RIVET_SOURCE_CDC_TABLE_UNRESOLVED` (0.32.2 and earlier). **Now** rivet captures the table the
  connection's `search_path` resolves, without a refusal, as MySQL does since #677; the other schema's
  table is not captured. **What to do:** if the stream should read the other schema's table, write the
  name qualified (`schema.orders`); a config that relied on the refusal to catch a wrong search_path
  now reads the table the search_path names.
