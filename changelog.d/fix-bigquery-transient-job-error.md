- **Behaviour change: a BigQuery job that fails with a transient error now exits 2 (retryable) instead of 1 (final).**
  A `rivet load` or `rivet compact` whose job BigQuery ended with `errorResult.reason` `backendError`,
  `internalError`, `jobBackendError`, `jobInternalError`, `rateLimitExceeded` or `jobRateLimitExceeded`
  ("Error encountered during execution. Retrying may solve the problem.") stopped with
  `RIVET_LOAD_WAREHOUSE_REJECTED`, exit 1, and a scheduler failed the task for good (0.32.2 and earlier;
  seen on a CDC compact MERGE under the Airflow provider). **Now** that stop is
  `RIVET_LOAD_WAREHOUSE_UNAVAILABLE`, exit 2, kind `transient`, with the job id, BigQuery's message and
  the reason in the text. The reason alone decides it, never the message; every other reason, including
  `quotaExceeded`, stays `RIVET_LOAD_WAREHOUSE_REJECTED`, exit 1. The same code also exits 2 now when
  BigQuery answers HTTP 429/5xx past rivet's retries (it exited 1 before). Nothing is re-submitted in
  process: a failed script may have run part of its statements, and the re-run recovers it (a compact
  merges the buffer a dead job left). **What to do:** let the scheduler retry the same command (Airflow:
  set `retries` on the load/compact task); a script that reads exit 1 as final needs no change, and one
  that retries on 2 now retries these. State and tables written before the upgrade are unaffected.
