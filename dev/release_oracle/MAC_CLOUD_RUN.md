# Cloud legs of the type matrix on a Mac (BigQuery, Snowflake)

This guide runs the type matrix against live BigQuery and Snowflake. It covers the delivery
measurement, the edge values at load and the read-back. It needs the cloud credentials,
so it runs on a machine that has them.

```sh
git fetch origin claude/integration-all-fixes && git checkout claude/integration-all-fixes   # every fix of this round, merged
docker compose up -d postgres mysql mssql oracle           # the source stand (the same pinned images)
cargo build --release
uv sync                                                    # duckdb 1.5.5, pyarrow 18.1.0, oracledb, pytz (pyproject.toml)

# BigQuery: ADC (`gcloud auth application-default login`) or RIVET_BQ_ACCESS_TOKEN, plus
export BIGQUERY_TEST_PROJECT=... RIVET_TEST_GCS_BUCKET=... BIGQUERY_TEST_LOCATION=...
# Snowflake: key-pair auth, plus
export RIVET_SF_CONNECTION=... RIVET_SNOWFLAKE_ACCOUNT=... RIVET_SNOWFLAKE_USER=... RIVET_SNOWFLAKE_KEY=/path/key.p8 \
       RIVET_SF_WAREHOUSE=... RIVET_SF_DATABASE=... RIVET_SF_SCHEMA=... RIVET_SF_STORAGE_INTEGRATION=...

for e in postgres mysql mssql oracle; do
  uv run python -m dev.release_oracle.property_no_silent_null_load --engines $e \
    --targets duck,bigquery,snowflake --observe /tmp/obs_cloud_$e.json | tee /tmp/run_cloud_$e.log
done
grep -h " RED \| NOT_GRADED " /tmp/run_cloud_*.log | sort | uniq -c | sort -rn > /tmp/cloud_reds.txt
```

Send back `/tmp/cloud_reds.txt` and the four `/tmp/obs_cloud_*.json` files. Every red is
either:
- a known-red class already in `dev/release_oracle/known_red.py`, or
- a finding.

The `->delivery` cells are the measured Arrow type of each row.
