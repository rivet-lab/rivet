- **A credential was held as plain text, so any `Debug` or `Display` of the value that held it printed it.**
  Every credential slot (source `url` and `password`, the CDC and state URLs, the ClickHouse `load.url`,
  the Slack `webhook_url`, `--source`, `--to`, `--clickhouse-url`, Secret Manager values, S3 and Azure keys,
  Google and Snowflake tokens and keys) was a `String` the text scanner had to recognise after the fact.
  **Now** each is a `Secret` that formats as `REDACTED`; its value is read out only where it is sent, and a
  test fails if a new credential field, config property or URL argument is a plain string. `rivet schema
  config` marks every credential property `"writeOnly": true`. Config files and state databases written by
  earlier versions read unchanged. **What to do:** nothing.
