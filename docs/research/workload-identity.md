# Keyless cloud identity: what rivet supports, what it lacks, how to test it

**Status:** research, 2026-11-02 · **Scope:** OSS first (the identity code lives
here; Rivet Pro inherits it) · **Question:** can rivet run with a cloud identity
instead of a key file — GKE / Cloud Run Workload Identity, Workload Identity
Federation, EKS IRSA / Pod Identity — and read its secrets from Secret Manager
or AWS Secrets Manager; and how do we prove it without a cloud bill on every
commit?

## 1. Short answer

- **Not consistently.** Each rivet leg resolves credentials differently. The
  GCS destination and the AWS S3 destination go through `reqsign`, which knows
  the keyless paths. The BigQuery load and **Secret Manager** go through
  rivet's own `gcp_auth`, which knows only key files: a pod with Workload
  Identity cannot read a Secret Manager secret today, although
  `docs/cloud-auth.md` said it could.
- **AWS Secrets Manager is not supported at all**; only Google Secret Manager is.
- **Emulators prove the client code path, not the identity.** No emulator
  reproduces GKE's metadata server, a Workload Identity pool's trust rules or
  EKS Pod Identity. The proof that matters is a keyless run in a real project,
  driven from CI by GitHub's OIDC token — which is itself Workload Identity
  Federation.
- **LocalStack is no longer free for us.** Since 2026-03-23 its single image
  requires an account token, the community edition is archived, the free plan
  is non-commercial only, and IAM policy enforcement is a paid feature.
  `moto` (Apache-2.0) covers S3, STS, IAM and Secrets Manager in server mode
  for the code-path tests; MinIO already covers S3.

## 2. The mechanisms

### Google Cloud

| Mechanism | Where | What the client does | File on disk |
|---|---|---|---|
| Service-account key | anywhere | signs a JWT with the key, `jwt-bearer` grant | key JSON (long-lived secret) |
| ADC `authorized_user` | developer laptop | refresh-token grant | `application_default_credentials.json` |
| **Metadata server** | GCE, **GKE Workload Identity**, **Cloud Run**, Cloud Functions | `GET http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token` with `Metadata-Flavor: Google` | none |
| **Workload Identity Federation** (`external_account`) | outside Google: AWS, Azure, on-prem OIDC, **GitHub Actions** | reads a subject token (file, URL, executable, or AWS signed request) → `sts.googleapis.com/v1/token` exchange → optional `generateAccessToken` impersonation of a service account | a config JSON with no secret in it |
| Impersonation | any of the above | `iamcredentials.googleapis.com …:generateAccessToken` as a target service account | — |

Access to a secret is then `roles/secretmanager.secretAccessor` granted to
the identity on that secret (not the project).

### AWS

| Mechanism | Where | What the client does | Env / endpoint |
|---|---|---|---|
| Static keys | anywhere | sign with the key | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` |
| Profile / SSO | laptop | read `~/.aws/config`, SSO cache | `AWS_PROFILE` |
| **IRSA** | EKS | `AssumeRoleWithWebIdentity` with the projected service-account token | `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN` |
| **EKS Pod Identity** | EKS | HTTP GET to the node agent with the projected token as `Authorization` | `AWS_CONTAINER_CREDENTIALS_FULL_URI` (169.254.170.23), `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` |
| ECS task role | ECS / Fargate | HTTP GET to the task metadata endpoint | `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` |
| Instance profile | EC2 | IMDSv2 token, then credentials | 169.254.169.254 |

Access to a secret is `secretsmanager:GetSecretValue` (plus `kms:Decrypt` for a
customer-managed key) on the secret's ARN.

## 3. What rivet supports today (read from the code)

| Leg | Key file | ADC user | Metadata server (GKE WI, Cloud Run) | WIF `external_account` | Where |
|---|---|---|---|---|---|
| GCS destination | ✅ | ✅ | ✅ via `reqsign` default chain | 🔶 `reqsign` parses it; never run | `src/destination/gcs.rs` |
| BigQuery load | ✅ | ✅ | 🔶 only through the `gcloud` CLI fallback | 🔶 same | `src/load/bq_rest.rs` |
| **Secret Manager** (`url_secret`, `password_secret`) | ✅ | ✅ | ❌ "no Google credentials" | ❌ | `src/config/gcp_secret.rs`, `src/gcp_auth.rs` |
| Pro: BigQuery audit / repair | ✅ | ✅ | ✅ via `reqsign` | 🔶 untested | `rivet-pro/…/warehouse/bigquery_api.rs` |

`src/gcp_auth.rs` says so itself: *"A third shape (`external_account`/workload
identity …, and GCE metadata) still reads as `Ok(None)` here."*

| Leg | Static | Profile | IRSA | Pod Identity | ECS | IMDSv2 | Where |
|---|---|---|---|---|---|---|---|
| S3 destination | ✅ | ✅ | 🔶 in `reqsign`'s chain, never run | 🔶 "general HTTP credentials" in the chain; token-file header unverified | 🔶 | 🔶 | `reqsign 0.16.5` `aws/credential.rs` |
| **AWS Secrets Manager** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | not implemented |

`docs/cloud-auth.md` tells AWS users to bridge IRSA into env vars by hand; the
chain may already do it. That is unproven either way until a test runs it.

Azure managed identity: not supported (documented).

## 4. How to test it

Three layers, cheapest first. Each catches what the one before cannot.

| Layer | Runs | Proves | Cannot prove |
|---|---|---|---|
| **1. In-process fakes** | every commit, no network | rivet's own code: which endpoint it calls, which headers, token caching and refresh, error codes for 403/404/timeouts | that the cloud behaves like the fake |
| **2. Emulators** | every commit, a container | the SDK-level flow against a third-party implementation | IAM evaluation, trust policies, metadata servers |
| **3. Real cloud, keyless** | nightly + release gate | the real identity, the real policy, a real deny | — (this is the proof) |

### Layer 1 — fakes we own

Small HTTP servers in the test harness, each a few dozen lines:

- **GCE/GKE metadata server**: answers the token path only when
  `Metadata-Flavor: Google` is sent; reachable through `GCE_METADATA_HOST`
  (the variable Google's client libraries honour), so rivet's resolver must
  honour it too.
- **Google STS + impersonation**: `external_account` files carry their own
  `token_url` and `service_account_impersonation_url`, so a test file points
  them at the fake. Asserts the subject token, audience and scopes sent.
- **Secret Manager**: `fetch()` in `gcp_secret.rs` already takes the endpoint
  as a parameter.
- **AWS container credentials**: serves the Pod Identity JSON only when the
  `Authorization` header equals the token file's content.
- **AWS STS** `AssumeRoleWithWebIdentity`: asserts the token and role ARN.

### Layer 2 — emulators

| Option | License / cost (Nov 2026) | Covers | Verdict |
|---|---|---|---|
| LocalStack | single image since 2026-03-23, needs `LOCALSTACK_AUTH_TOKEN`; free plan non-commercial only; community edition archived; IAM enforcement (`ENFORCE_IAM`) reported as paid | S3, STS (incl. `AssumeRoleWithWebIdentity`), IAM, Secrets Manager | only with a paid plan; not a dependency the OSS CI can take |
| **moto** server mode | Apache-2.0, free | S3, STS, IAM, Secrets Manager; no IAM enforcement | **use for the AWS code path** |
| MinIO | AGPL, already in the stand | S3 | keep for S3 |
| Google | no official emulator for Secret Manager, STS or the metadata server; community emulators exist (`blackwell-systems/gcp-emulator`, `coxley/gsm`) and skip auth by design | Secret Manager API shape | prefer our own fake (layer 1); an emulator adds little |

### Layer 3 — real cloud, keyless, from CI

- **GitHub Actions → GCP**: `google-github-actions/auth` with a Workload
  Identity pool trusting this repository writes an `external_account` file.
  rivet must then: load BigQuery, write GCS, read a Secret Manager secret —
  with no key file anywhere. A second identity **without**
  `secretAccessor` must fail with `RIVET_CONFIG_GCP_SECRET_UNREADABLE` (403):
  least privilege is proven by a deny, not by a success.
- **GitHub Actions → AWS**: `aws-actions/configure-aws-credentials` with an
  OIDC role → web identity → S3 write and a Secrets Manager read, plus the
  same deny case.
- **Pods (optional, release gate)**: one GKE Autopilot job with Workload
  Identity and one EKS job with Pod Identity, run per release, torn down after.
  This is the only way to exercise the metadata server and the Pod Identity
  agent for real.

## 5. Plan (OSS first)

| # | Item | Notes |
|---|---|---|
| 1 | **One Google credential resolver for every leg**: key file, ADC user, metadata server, `external_account` (file / URL / executable / AWS sources, impersonation) | BigQuery and Secret Manager stop depending on the `gcloud` CLI; logs which principal it is |
| 2 | **AWS Secrets Manager source credentials** — `url_secret: arn:aws:secretsmanager:…` alongside the Google form, same AWS chain as S3 | coded errors for denied / not found / KMS |
| 3 | **Prove the AWS chain**: IRSA and Pod Identity through `reqsign`, or replace the loader | then rewrite `docs/cloud-auth.md` Path B |
| 4 | **Test harness**: layer-1 fakes, `moto` in the stand, keyless nightly in a GCP project and an AWS account | include the deny cases |
| 5 | Azure managed identity | later, same pattern |

Pro gets all of it through the OSS seam; the Pro-only piece is the
Enterprise air-gapped bundle (#44 in rivet-pro), where the metadata server and
STS endpoints must be configurable (private Google access, VPC endpoints).
