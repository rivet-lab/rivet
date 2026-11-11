# ADR-0040: Product Invariants Hold by Construction, Each at One Seam, Each Graded by a Generated Matrix

- **Status:** Proposed. At this ADR's date only D3 (the PR-body check), D5 (the freezing ratchets) and the matrix registry are implemented; the owner seams of D1 are the target state.
- **Date:** 2026-11-10
- **Context:** The handover audits from 2026-10-28 to 2026-11-09 hold 627 unique findings in 41 patterns (`.claude/handoff/2026-11-10/pattern-catalogue.md`, not re-verified there). Five product families carry 201 of the 346 product findings: a credential (13), a value (27), entry parity (62), message truth (76) and a stop (23). Each fix closed its instance and the class came back next to it. The property matrices measure it, as their PRs report: `refusal_names_true_cause` 126 accepted RED cells (#906), `no_debris_after_stop` 16 to 17 RED per SQL engine (#909), `check_equals_run` `red=207` (#908), `no_silent_null` 6435 RED cells over its batch, CDC and load legs (#910); `no_secret_in_output` is #907.

  The cause is structural: each property is enforced at every call site separately instead of at one place every call site passes through.
  - Credentials are `String`: `url` and `password` (`src/config/source.rs:26`, `:36`), the raw load `url` (`src/config/load.rs:87`) and the ClickHouse load `url` (`src/config/load.rs:350`), the notification `webhook_url` (`src/config/notifications.rs:15`). Masking is a text scan, `redact_secrets` (`src/redact.rs:74`), over keyword pairs (`is_password_keyword`, `src/redact.rs:124`) and URL query keys (`redact_query_secrets`, `src/redact.rs:213`), applied by each sink that calls it. No `Secret` type exists in `src/`.
  - A converter decides a value it cannot read inside its own `match`. The delivery builders now refuse a non-NULL value no arm reads (`src/source/mssql/arrow_convert.rs:536` refuses before the `_ => b.append_null()` at `:545`; the decimal, uuid and bytes cells refuse at `:703`, `:723`, `:732`; MySQL since 43099914). What remains are 59 wildcard arms that answer `None`, `Ok(None)`, NULL or a default in the product code under `src/source/`: 35 in the Arrow converters, most in the value-checksum cell accessors of the MySQL, Postgres and SQL Server converters; one in the checksum of a list element (`src/source/value_checksum.rs:198`); 13 in the CDC decoders and the CDC partition guard; the rest in source modules where the arm answers no value (a warning, a proxy or grant probe).
  - check, doctor and plan re-implement decisions of run instead of calling them: 20 PR-SEAM-COPY findings (9 confirmed) in the catalogue.
  - The exit class of an uncoded error comes partly from message text: `classify_exit` (`src/error.rs:570`) returns Retryable when `retry::classify_error` calls it transient (`src/error.rs:581`), and `classify_error` (`src/error/retry.rs:128`) falls back to `msg.contains` needles after its typed checks (`src/error/retry.rs:215`, plus the ORA code lists of `is_oracle_capacity` `:465` and `is_oracle_lost_session` `:507`).
  - Output parts are not owned by a run, publication is not atomic, and cleanup depends on whoever remembers it (the RED cells of #909).

  The tests had the same shape as the code: one hand-written test per instance and hand-synced ledgers. A new command, type, sink or leg had no cell until someone wrote one.

---

## Decision

### D1 - Five invariants, one owner seam each

| # | Invariant | Owner seam (the only place that decides) | Made impossible to bypass by | Matrix |
|---|---|---|---|---|
| I1 | No credential reaches any output | a `Secret` newtype: `Display`/`Debug` print `REDACTED`; the raw value only via `.expose()` at connect | the type: no `String` credential slot in source, state, load or notification config | `no_secret_in_output` |
| I2 | A non-NULL source value is delivered equal, or refused with a code | one compatibility table `source type x override -> read / refuse`, read by check and run before any read | converters have no wildcard arm that answers NULL; an unlisted pair is a compile error or a coded refusal | `no_silent_null` |
| I3 | A pre-run command passes only what run will do, and refuses with run's code | one planning function that run executes; check, doctor and plan call it and stop before the first write | no pre-run command holds its own copy of a run decision | `check_equals_run` |
| I4 | Every refusal is coded, names the true cause, and its remedy works | typed errors; one precheck pipeline in a fixed order for every command; a remedy names a command from the clap tree | the exit class is decided by the error type, never by message text; remedies are checked against `rivet schema cli` (`SchemaKind::Cli`, `src/cli/args.rs:675`) | `refusal_names_true_cause` |
| I5 | After any stop no debris is left, and a re-run equals an uninterrupted run | a per-run staging directory; atomic publish by manifest; the run that owns the staging removes it | no writer writes into the published location directly | `no_debris_after_stop` |

Rejected for I5: cleaning orphan parts on the next run. It keeps "who owns this file" a guess made after the fact; a per-run directory makes ownership a property of the path.

The five names are registered once, in `PROPERTIES` (`dev/release_oracle/properties.py`). A matrix is found on the tree by its `property_<name>` files under `tests/offline`, `tests/live`, `dev/release_oracle` and `dev/pytools`. The gate stage `verify_property_matrices` writes a FAIL row for a registered property with no matrix on the tree, and for a `property_*` file that grades no registered property.

### D2 - The matrix is generated from the code, not from a list

Each matrix takes its axes from the product itself: the CLI tree (`rivet schema cli`), `schemas/rivet.schema.json`, the engine, mode, target and type enums, the error registry, the sink list. Per-value data sits in a `match` with no `_` arm. A new command, type, sink or leg joins the matrix by existing; until a cell grades it, it is a `NOT_GRADED` row that is never counted green.

### D3 - A new feature declares its invariants before code

A PR that changes a path where a command (`src/cli/`), a config field (`src/config/`), a source or type converter (`src/source/`, `src/types/`), a destination or output sink (`src/destination/`, `src/format/`) or a write leg (`src/load/`) lives answers, for each of I1-I5 under `## Invariants`: does it touch the invariant, which seam it goes through, and which matrix cell grades it. "Not applicable" is an answer; silence is not. `dev/pytools/pr_ready.py` (`invariants_verdict`, paths in `SEAMS`) fails the PR-rules job when the section or an answer is missing, and fails when a path in `SEAMS` is gone from the tree.

### D4 - A fix closes the class at the seam, or it is not a fix

A defect in one of the five classes is fixed in the owner seam, not at the call site, and is proven by the whole matrix: RED on main, GREEN on the branch. A call-site patch is accepted only as a stop-gap with a ledger row naming the seam PR that removes it.

### D5 - Text scanners and call-site shapes only shrink

Until the seams land, the call-site shapes they replace stay as a safety net and may only shrink. `tests/offline/invariant_ratchets.rs` counts each by a structural read of the product code (comments, literals' contents and `#[cfg(test)]` items left out) and holds each site as one line of `tests/offline/ledgers/<pin>.txt` (a stable id, a date and the reason it stays):

| Ledger | Sites |
|---|---|
| `wildcard-null-arms` | wildcard arms (`_`, `Some(_)`, `Err(_)`, guards dropped) answering NULL, `None`, `Ok(None)` or a default, in every file under `src/source/` |
| `credential-string-fields` | struct and enum-variant fields in `src/config/` typed `String` / `Option<String>` whose name ends in a credential word (`*_env`, `*_file`, `*_path` and secret-manager names such as `url_secret` are references, not credentials) Held empty: no ledger file, any site fails. |
| `redaction-text-needles` | literals the redaction scanner searches for, compares to or lists in `src/redact.rs` |
| `retry-text-needles` | literals the retry classifier searches the message for or lists in `src/error/retry.rs` (the text fallback that also decides an uncoded exit class, and the value-conversion detector) |

A site with no ledger line fails with its id and the seam to use; a ledger line whose site is gone fails until the line is deleted. No number is kept, so two PRs touching different sites touch different lines. `tests/offline/silent_degrade_ratchet.rs` keeps its own hand list of degrade shapes under `src/source/`; the new count is per match arm and catches a shape that list does not spell. The hand ledgers (`docs/*-matrix.yaml`) are not counted here.

There is no count for I3: the copies of a run decision in check, doctor and plan have no syntactic shape a scanner can read; `check_equals_run` grades them by behaviour.

---

## Consequences

- Five root PRs, one per invariant, each proven by its matrix. I1 and I2 were approved on 2026-11-08; I3, I4 and I5 follow.
- Release 0.33 ships when the five matrices are green on main with their ledgers empty or each remaining row tied to an open seam PR. Until each matrix is on main, the gate's `property matrices` stage is red for it.
- A seam PR lowers the D5 pin it makes redundant in the same PR.
- Not covered by this ADR, and candidates for the same treatment: state, lease and checkpoint identity (one key for export + source + destination); docs checked against clap, schema and the error registry; network calls x silent peer.
- Cost: a seam refactor touches every call site of its class once. The alternative measured over two weeks is one new instance per audit per class.
