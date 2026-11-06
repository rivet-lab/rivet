- **Behaviour change: a 5xx, 429 or 408 from Google's token endpoint is retried and then stops as unavailable (exit 2), not as bad credentials (exit 1).**
  A `rivet load` into BigQuery whose token request to Google's OAuth endpoint was answered
  with HTTP 500/502/503, 429 or 408, or got no answer, stopped at once with `RIVET_DEST_CREDENTIALS_INVALID`
  ("ADC token request failed (HTTP 500 Internal Server Error)"), exit 1, no retry; the next run with the same
  credential succeeded (0.32.2 and earlier). **Now** both token requests (service-account key and
  `gcloud auth application-default login` credential; the BigQuery load and the GCS destination share them) retry such an answer and a lost
  connection with rivet's bounded backoff (5 retries), and when they run out the stop is
  `RIVET_DEST_TOKEN_ENDPOINT_UNAVAILABLE`, kind `transient`, exit 2, naming the endpoint and the last status.
  HTTP 400, 401 and 403 (`invalid_grant`, a revoked or expired credential) stay
  `RIVET_DEST_CREDENTIALS_INVALID`, exit 1, asked once. **What to do:** let the scheduler retry the same command
  (Airflow: set `retries` on the task); a script that reads exit 1 as final needs no change, and one that retries
  on 2 now retries these. State and tables written before the upgrade are unaffected.
