- **Source credentials from Google Secret Manager.** `source:` takes `url_secret:` (the whole URL) or
  `password_secret:` (with structured `host`/`user`/`database`), each naming a secret as
  `projects/<project>/secrets/<secret>` (the latest version) or `.../versions/<n>`. rivet reads it with
  the Google credential it already uses for BigQuery and GCS (`GOOGLE_APPLICATION_CREDENTIALS` or
  gcloud ADC), once per process, so the database password is never stored on the machine. The
  credential needs `roles/secretmanager.secretAccessor` on the secret. A secret that cannot be read
  stops the run with the new `RIVET_CONFIG_GCP_SECRET_UNREADABLE` (exit 1), naming the secret and the
  role to grant; a sealed plan keeps the secret name, never its value. The name may be a `${VAR}`
  filled by `--param` or the environment, so a scheduler passes the name and never the value.
- **A config refusal raised while connecting to the source was relabelled a connectivity or auth
  error from words in its text** (0.32.2 and earlier). **Now** a `RIVET_CONFIG_*` refusal keeps its
  own hint in `rivet check` and `doctor`. **What to do:** nothing.
