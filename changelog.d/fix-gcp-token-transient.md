- **Behaviour change: a 5xx, 429 or 408 from Google's token endpoint is retried and then stops as unavailable (exit 2), not as bad credentials (exit 1); a refused grant is asked once.**
  A BigQuery load, or a GCS destination (an export, or a Snowflake, ClickHouse or BigQuery load that reads GCS), whose
  token request to Google's OAuth endpoint was answered with HTTP 500/502/503, 429 or 408, or got no answer, stopped
  with `RIVET_DEST_CREDENTIALS_INVALID` ("ADC token request failed (HTTP 500 Internal Server Error)"), exit 1, no retry,
  and the next run with the same credential succeeded (0.32.2 and earlier). On GCS the stop was worse: opendal wrapped
  the loader's error, so the run stopped with no code at all, exit 1 or 2 by opendal's own flag, after about a minute
  of opendal's backoff and up to 144 asks of the endpoint for one answer. **Now** both token requests (service-account
  key and `gcloud auth application-default login` credential) retry such an answer and a lost connection with rivet's
  bounded backoff (5 retries, a WARN line per retry naming the endpoint and status), and when they run out the stop is
  `RIVET_DEST_TOKEN_ENDPOINT_UNAVAILABLE`, kind `transient`, exit 2, naming the endpoint and the last status.
  HTTP 400, 401 and 403 (`invalid_grant`, a revoked or expired credential), a request that cannot be built and a
  200 answer that is not a token stay `RIVET_DEST_CREDENTIALS_INVALID`, exit 1. An error code a credential loader
  raises is now read through opendal's wrapper, so the GCS stop carries its code and exit, and the token endpoint is
  asked once for a refused grant and at most six times for an outage per five minutes and credential, under the export
  retry layer (5) and the load retry layer (3) alike. Not covered: a GCS `credentials_file` is exchanged by opendal's
  own signer, which keeps its own retries and codes. **What to do:** let the scheduler retry the same command (Airflow:
  set `retries` on the task); a script that reads exit 1 as final needs no change, and one that retries on 2 now
  retries these. State and tables written before the upgrade are unaffected.
