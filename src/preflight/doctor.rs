use crate::config::{Config, DestinationType, SourceType};
use crate::error::Result;

/// One `rivet doctor` probe result.
#[derive(Debug, Clone, serde::Serialize)]
pub(super) struct DoctorCheck {
    pub(super) name: String,
    pub(super) ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) hint: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct DoctorReport {
    config_path: String,
    all_ok: bool,
    checks: Vec<DoctorCheck>,
}

fn print_doctor_json(config_path: &str, all_ok: bool, checks: &[DoctorCheck]) {
    let report = DoctorReport {
        config_path: config_path.to_string(),
        all_ok,
        checks: checks.to_vec(),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).unwrap_or_else(|_| "{}".into())
    );
}

/// Record a check and, in text mode, render its `[OK]/[FAIL]` line **from the same struct**.
fn emit_check(checks: &mut Vec<DoctorCheck>, json: bool, mut check: DoctorCheck) {
    check.detail = check.detail.map(|d| crate::redact::redact_secrets(&d));
    if !json {
        if check.ok {
            match &check.detail {
                Some(d) => println!("[OK]  {} — {}", check.name, d),
                None => println!("[OK]  {}", check.name),
            }
        } else {
            println!(
                "[FAIL] {}: {}",
                check.name,
                check.detail.as_deref().unwrap_or("")
            );
            if let Some(h) = &check.hint {
                println!("       Hint: {}", h);
            }
        }
    }
    checks.push(check);
}

/// The first transient failure a doctor probe met: its text and the typed verdict that made it transient.
type Transient = (String, crate::error::retry::RetryClass);

/// Keep the first transient failure of `e` in `slot`, so the doctor's exit stays retryable.
fn keep_first_transient(slot: &mut Option<Transient>, e: &anyhow::Error) {
    let class = crate::error::retry::classify_error(e);
    if slot.is_none() && class.is_transient() {
        *slot = Some((format!("{e:#}"), class));
    }
}

#[cfg(test)]
mod transient_tests {
    #[test]
    fn the_first_transient_failure_is_kept_and_a_permanent_one_is_not() {
        let transient = anyhow::anyhow!("connection refused (os error 111)");
        let permanent = anyhow::anyhow!("permission denied for table orders");
        let mut slot = None;
        super::keep_first_transient(&mut slot, &permanent);
        assert_eq!(slot, None);
        super::keep_first_transient(&mut slot, &transient);
        assert!(slot.as_ref().is_some_and(|(s, _)| s.contains("refused")));
        super::keep_first_transient(
            &mut slot,
            &anyhow::anyhow!("timed out, then connection reset"),
        );
        assert!(
            slot.as_ref().is_some_and(|(s, _)| s.contains("refused")),
            "the first is kept"
        );
    }

    #[test]
    fn a_probe_failure_is_kept_by_its_typed_verdict_not_by_its_words() {
        use crate::error::{coded_over, codes, retry};
        let server = |code: u16| {
            anyhow::Error::new(mysql::Error::MySqlError(mysql::error::MySqlError {
                state: "HY000".to_string(),
                message: "x".to_string(),
                code,
            }))
        };
        let shutdown = server(1053);
        let transient = coded_over(codes::SOURCE_QUERY_FAILED, "permission denied", &shutdown);
        let mut slot = None;
        super::keep_first_transient(&mut slot, &transient);
        assert!(slot.as_ref().is_some_and(|(_, c)| c.is_transient()));
        let denied = server(1045);
        let permanent = coded_over(
            codes::SOURCE_QUERY_FAILED,
            "login timeout, connection reset, dns",
            &denied,
        );
        assert!(!retry::classify_error(&permanent).is_transient());
        let mut slot = None;
        super::keep_first_transient(&mut slot, &permanent);
        assert!(slot.is_none());
    }
}

/// What `run` would plan for every batch export of a config, one verdict per export; supplied by the caller above the preflight layer.
pub type RunPlanProbe<'a> =
    &'a dyn Fn(&Config, &std::path::Path) -> Result<Vec<(String, Result<()>)>>;

/// Emit one check that carries no hint.
fn emit_named(
    checks: &mut Vec<DoctorCheck>,
    json: bool,
    name: impl Into<String>,
    ok: bool,
    detail: Option<String>,
) {
    let check = DoctorCheck {
        name: name.into(),
        ok,
        detail,
        hint: None,
    };
    emit_check(checks, json, check);
}

/// One `Run plan <export>` check per verdict the probe returns, and the first refusal among them.
fn run_plan_checks(
    config: &Config,
    config_dir: &std::path::Path,
    run_plan: RunPlanProbe<'_>,
    json: bool,
    checks: &mut Vec<DoctorCheck>,
) -> Option<anyhow::Error> {
    if !seeded!("doctor-skips-cursor-admission", false, true) {
        return None;
    }
    let mut refusal: Option<anyhow::Error> = None;
    let planned =
        run_plan(config, config_dir).unwrap_or_else(|e| vec![("config".to_string(), Err(e))]);
    for (name, verdict) in planned {
        let (ok, detail) = match verdict {
            Ok(()) => (true, None),
            Err(e) => {
                let detail = trim_probe_error(&e);
                refusal.get_or_insert(e);
                (false, Some(detail))
            }
        };
        emit_named(checks, json, format!("Run plan {name}"), ok, detail);
    }
    refusal
}

pub fn doctor(config_path: &str, json: bool, run_plan: RunPlanProbe<'_>) -> Result<()> {
    if !json {
        println!("rivet doctor: verifying auth for config '{}'", config_path);
        println!();
    }
    let mut checks: Vec<DoctorCheck> = Vec::new();

    let config = match Config::load(config_path) {
        Ok(c) => {
            emit_named(&mut checks, json, "Config parsed successfully", true, None);
            c
        }
        Err(e) => {
            emit_named(
                &mut checks,
                json,
                "Config error",
                false,
                Some(trim_probe_error(&e)),
            );
            if json {
                print_doctor_json(config_path, false, &checks);
            }
            return Err(crate::error::recoded(
                &e,
                crate::error::codes::CLI_PREFLIGHT_FAILED,
                "doctor: config check failed (see output above)",
            ));
        }
    };

    let mut all_ok = true;
    let mut source_ok = false;
    let mut transient: Option<Transient> = None;
    let config_dir = std::path::Path::new(config_path)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));

    match crate::state::StateStore::probe(config_path) {
        Ok(at) => emit_named(&mut checks, json, format!("State ({at})"), true, None),
        Err(e) => {
            all_ok = false;
            keep_first_transient(&mut transient, &e);
            emit_named(
                &mut checks,
                json,
                "State",
                false,
                Some(trim_probe_error(&e)),
            );
        }
    }

    match check_source_auth(&config) {
        Ok(()) => {
            source_ok = true;
            emit_check(
                &mut checks,
                json,
                DoctorCheck {
                    name: format!("Source auth ({:?})", config.source.source_type),
                    ok: true,
                    detail: None,
                    hint: None,
                },
            );
            if !json {
                note_mssql_harm_permission(&config);
                note_oracle_harm_permission(&config);
            }
        }
        Err(e) => {
            all_ok = false;
            keep_first_transient(&mut transient, &e);
            let category = categorize_source_error(&e);
            let hint =
                source_error_hint(category, &e, &config.source.source_type).map(|h| h.to_string());
            emit_check(
                &mut checks,
                json,
                DoctorCheck {
                    name: format!("Source {}", category),
                    ok: false,
                    detail: Some(trim_probe_error(&e)),
                    hint,
                },
            );
        }
    }

    let mut cursor_refusal: Option<anyhow::Error> = None;
    if source_ok && seeded!("doctor-skips-cursor-admission", false, true) {
        let declared = crate::config::declared_exports(&config, None)?;
        for a in crate::plan::cursor_admission::admit_exports(&config, &declared, config_dir) {
            let (ok, detail) = match &a.verdict {
                Ok(crate::plan::cursor_admission::Admission::Admitted) => (true, None),
                Ok(crate::plan::cursor_admission::Admission::Unverified(w)) => {
                    (true, Some(w.clone()))
                }
                Err(e) => (false, Some(trim_probe_error(e))),
            };
            if let Err(e) = a.verdict {
                all_ok = false;
                cursor_refusal.get_or_insert(e);
            }
            emit_check(
                &mut checks,
                json,
                DoctorCheck {
                    name: format!("Incremental cursor {}.{}", a.export, a.column),
                    ok,
                    detail,
                    hint: None,
                },
            );
        }
    }

    let run_refusal = source_ok
        .then(|| run_plan_checks(&config, config_dir, run_plan, json, &mut checks))
        .flatten();
    all_ok &= run_refusal.is_none();

    let mut seen_destinations: Vec<String> = Vec::new();
    let doctored = if seeded!("check-reads-declared-list-not-run-set", true, false) {
        config.exports.clone()
    } else {
        crate::config::declared_exports(&config, None)?
    };
    for export in &doctored {
        let dest_key = super::destination_identity(&export.destination);
        if seen_destinations.contains(&dest_key) {
            continue;
        }
        seen_destinations.push(dest_key);

        let label = match export.destination.destination_type {
            DestinationType::Local => format!(
                "Local({})",
                export.destination.path.as_deref().unwrap_or(".")
            ),
            DestinationType::S3 => format!(
                "S3({})",
                export.destination.bucket.as_deref().unwrap_or("?")
            ),
            DestinationType::Gcs => format!(
                "GCS({})",
                export.destination.bucket.as_deref().unwrap_or("?")
            ),
            DestinationType::Azure => format!(
                "Azure({})",
                export.destination.bucket.as_deref().unwrap_or("?")
            ),
            DestinationType::Stdout => {
                emit_check(
                    &mut checks,
                    json,
                    DoctorCheck {
                        name: "Destination Stdout (streaming; no preflight needed)".into(),
                        ok: true,
                        detail: None,
                        hint: None,
                    },
                );
                continue;
            }
        };

        let expanded_dest = crate::plan::build::expand_destination_templates(
            export.destination.clone(),
            &export.name,
        );
        match check_destination_auth(&expanded_dest) {
            Ok(()) => {
                emit_check(
                    &mut checks,
                    json,
                    DoctorCheck {
                        name: format!("Destination {}", label),
                        ok: true,
                        detail: None,
                        hint: None,
                    },
                );
            }
            Err(e) => {
                all_ok = false;
                keep_first_transient(&mut transient, &e);
                let category = categorize_dest_error(&e, &expanded_dest);
                let hint = destination_error_hint(category, &expanded_dest).map(|h| h.to_string());
                emit_check(
                    &mut checks,
                    json,
                    DoctorCheck {
                        name: format!("Destination {} -- {}", label, category),
                        ok: false,
                        detail: Some(trim_probe_error(&e)),
                        hint,
                    },
                );
            }
        }
    }

    let (cdc_checks, cdc_probe) = super::cdc_health::collect(&config, config_dir);
    if let Some((detail, class)) = cdc_probe
        && class.is_transient()
        && transient.is_none()
    {
        transient = Some((detail, class));
    }
    for c in cdc_checks {
        if !c.ok {
            all_ok = false;
        }
        emit_check(&mut checks, json, c);
    }

    if json {
        print_doctor_json(config_path, all_ok, &checks);
    } else {
        println!();
        if all_ok {
            println!("All checks passed.");
            println!("Next: rivet check -c {config_path}   # column-type & schema report");
        } else {
            println!("Some checks failed. Fix the issues above before running exports.");
        }
    }
    if let Some(refusal) = cursor_refusal.or(run_refusal) {
        return Err(refusal);
    }
    if all_ok {
        Ok(())
    } else {
        Err(doctor_failure_error(transient))
    }
}

/// The doctor's terminal error when a check failed: retryable only when a probe's typed verdict was transient.
fn doctor_failure_error(transient: Option<Transient>) -> anyhow::Error {
    match transient {
        Some((detail, class)) => anyhow::Error::new(
            crate::error::CodedError::new(
                crate::error::codes::CLI_PREFLIGHT_UNVERIFIED,
                format!(
                    "doctor: preflight failed on a transient error (see output above): {detail}"
                ),
            )
            .over(class),
        ),
        None => crate::error::coded(
            crate::error::codes::CLI_PREFLIGHT_FAILED,
            "doctor: one or more preflight checks failed (see output above)",
        ),
    }
}

fn check_source_auth(config: &Config) -> Result<()> {
    let url = config.source.resolve_url()?;
    let url = url.expose();
    let tls = config.source.tls.as_ref();
    crate::source::warn_if_tls_disabled(&config.source);
    match config.source.source_type {
        SourceType::Postgres => {
            let mut client = crate::source::postgres::connect_client(url, tls)?;
            client.simple_query("SELECT 1")?;
            Ok(())
        }
        SourceType::Mysql => {
            let pool = crate::source::mysql::connect_pool(url, tls)?;
            let mut conn = pool.get_conn()?;
            use mysql::prelude::Queryable;
            conn.query_drop("SELECT 1")?;
            Ok(())
        }
        SourceType::Mssql => {
            crate::source::mssql::MssqlSource::connect_with_tls(url, tls)?;
            Ok(())
        }
        SourceType::Oracle => {
            crate::source::create_source(&config.source)?;
            Ok(())
        }
        SourceType::Mongo => {
            crate::source::mongo::MongoSource::connect(url, tls, config.source.mongo.as_ref())?;
            Ok(())
        }
    }
}

/// Advisory (never a `[FAIL]`): a SQL Server login that can extract data fine but lacks `VIEW SERVER STATE` will have its source-harm metrics (lock waits, via `sys.dm_os_wait_stats`) silently skipped.
fn note_mssql_harm_permission(config: &Config) {
    if config.source.source_type != SourceType::Mssql {
        return;
    }
    let Ok(url) = config.source.resolve_url() else {
        return;
    };
    if let Some(false) =
        crate::source::mssql::sample_view_server_state(url.expose(), config.source.tls.as_ref())
    {
        println!(
            "[note] Source-harm metrics need VIEW SERVER STATE — this SQL Server login lacks it, \
             so lock-wait metrics will be skipped. Data extraction is unaffected. \
             Grant with: GRANT VIEW SERVER STATE TO [your_login];"
        );
    }
}

/// Advisory (never a `[FAIL]`): an Oracle user that cannot read V$SYSSTAT / V$SYSTEM_EVENT gets no harm metrics and no governor pressure, silently.
#[cfg(not(feature = "oracle"))]
fn note_oracle_harm_permission(_config: &Config) {}

/// Advisory (never a `[FAIL]`): an Oracle user that cannot read V$SYSSTAT / V$SYSTEM_EVENT gets no harm metrics and no governor pressure, silently.
#[cfg(feature = "oracle")]
fn note_oracle_harm_permission(config: &Config) {
    if config.source.source_type != SourceType::Oracle {
        return;
    }
    let Ok(url) = config.source.resolve_url() else {
        return;
    };
    if let Some(false) = crate::source::oracle::OracleSource::sample_harm_views(
        url.expose(),
        config.source.tls.as_ref(),
    ) {
        println!(
            "[note] This Oracle user cannot read V$SYSSTAT / V$SYSTEM_EVENT, so source-harm \
             metrics and governor pressure will be absent. Data extraction is unaffected. \
             Grant with: GRANT SELECT_CATALOG_ROLE TO your_user; (or SELECT on V_$SYSSTAT and \
             V_$SYSTEM_EVENT)"
        );
    }
}

fn check_destination_auth(dest: &crate::config::DestinationConfig) -> Result<()> {
    let d = crate::destination::create_destination_for_probe(dest)?;
    write_destination_probe(d.as_ref(), dest)
}

/// Write the doctor probe through `d` from a file only this call owns, then remove the destination copy.
fn write_destination_probe(
    d: &dyn crate::destination::Destination,
    dest: &crate::config::DestinationConfig,
) -> Result<()> {
    use std::io::Write;
    let probe_key = crate::manifest::DOCTOR_PROBE_FILENAME;
    let mut tmp = tempfile::NamedTempFile::new()?;
    tmp.write_all(b"ok")?;
    tmp.flush()?;
    seeded!(
        "doctor-probe-fixed-temp-path",
        crate::seeded::write_through_shared_probe(d, probe_key),
        d.write(tmp.path(), probe_key)
    )?;
    log::debug!("doctor: probe write succeeded, cleaning up");
    remove_destination_probe(dest, probe_key);
    Ok(())
}

/// Resolve the local destination root the way `LocalDestination::new` does.
fn local_base_path(dest: &crate::config::DestinationConfig) -> String {
    dest.path
        .clone()
        .or_else(|| dest.prefix.clone())
        .unwrap_or_else(|| ".".to_string())
}

/// Best-effort removal of the destination-side write-probe (FINDING #26).
fn remove_destination_probe(dest: &crate::config::DestinationConfig, probe_key: &str) {
    match dest.destination_type {
        DestinationType::Local => {
            let probe_path = std::path::Path::new(&local_base_path(dest)).join(probe_key);
            match std::fs::remove_file(&probe_path) {
                Ok(()) => log::debug!("doctor: removed destination probe {}", probe_path.display()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => log::warn!(
                    "doctor: could not remove destination probe {} (left at prefix): {e}",
                    probe_path.display()
                ),
            }
        }
        DestinationType::Stdout => {}
        DestinationType::S3 | DestinationType::Gcs | DestinationType::Azure => {
            log::debug!(
                "doctor: destination probe '{probe_key}' left at the {:?} prefix \
                 (no object-delete on this backend); manifest reconcile filters it from listings",
                dest.destination_type
            );
        }
    }
}

/// L21: trim a raw probe error to its root cause for the `[FAIL]` line.
pub(super) fn trim_probe_error(err: &anyhow::Error) -> String {
    let raw = format!("{err:#}");
    let flat = raw.replace(['\n', '\r'], " ");
    let lower = flat.to_ascii_lowercase();
    let cut = [
        ", context: {",
        " context: {",
        " parts {",
        ", headers: {",
        " headers: {",
        ", response:",
    ]
    .iter()
    .filter_map(|m| lower.find(m))
    .min();
    let mut out = match cut {
        Some(i) => flat[..i].trim_end_matches([' ', ',']).to_string(),
        None => raw.trim().to_string(),
    };
    const MAX: usize = 1200;
    if out.chars().count() > MAX {
        out = out.chars().take(MAX).collect::<String>();
        out.push('…');
    }
    out
}

pub(crate) fn categorize_source_error(err: &anyhow::Error) -> &'static str {
    if crate::error::error_code(err).is_some_and(|c| c.starts_with("RIVET_CONFIG_")) {
        return "error";
    }
    let msg = format!("{err:#}").to_lowercase();
    if (msg.contains("does not exist") && msg.contains("database"))
        || msg.contains("cannot open database")
        || msg.contains("unknown database")
    {
        return "unknown database";
    }
    if msg.contains("ora-12514") || msg.contains("not registered with the listener") {
        return "unknown service";
    }
    if msg.contains("too many connections")
        || msg.contains("too many clients")
        || msg.contains("connection slots are reserved")
        || msg.contains("starting up")
    {
        return "transient";
    }
    if msg.contains("password")
        || msg.contains("authentication")
        || msg.contains("access denied")
        || msg.contains("login failed")
        || msg.contains("ora-01017")
        || msg.contains("logon denied")
        || msg.contains("db error")
    {
        "auth error"
    } else if msg.contains("connect")
        || msg.contains("refused")
        || msg.contains("timed out")
        || msg.contains("could not translate host")
        || msg.contains("name or service not known")
    {
        "connectivity error"
    } else {
        "error"
    }
}

pub(super) fn categorize_dest_error(
    err: &anyhow::Error,
    dest: &crate::config::DestinationConfig,
) -> &'static str {
    let msg = format!("{err:#}").to_lowercase();
    if msg.contains("already expired") && msg.contains("sas") {
        return "sas expired";
    }
    if matches!(
        dest.destination_type,
        DestinationType::Local | DestinationType::Stdout
    ) && (msg.contains("permission denied")
        || msg.contains("permissiondenied")
        || msg.contains("os error 13"))
    {
        return "permission error";
    }
    if msg.contains("credential")
        || msg.contains("permission denied")
        || msg.contains("permissiondenied")
        || msg.contains("access denied")
        || msg.contains("unauthorized")
        || msg.contains("forbidden")
        || msg.contains("invalid_grant")
        || msg.contains("token")
        || msg.contains("invalidaccesskeyid")
        || msg.contains("403")
    {
        "auth error"
    } else if msg.contains("not found") || msg.contains("nosuchbucket") || msg.contains("404") {
        match dest.destination_type {
            DestinationType::S3 => "bucket not found",
            DestinationType::Gcs => "bucket not found",
            DestinationType::Azure => "container not found",
            DestinationType::Local | DestinationType::Stdout => "path not found",
        }
    } else if msg.contains("connect")
        || msg.contains("refused")
        || msg.contains("timed out")
        || msg.contains("dns")
        || msg.contains("endpoint")
        || msg.contains("error sending request")
        || msg.contains("send http request")
    {
        "connectivity error"
    } else {
        "error"
    }
}

/// TLS hint for PostgreSQL and MySQL; every `tls.mode` it names must parse as a `TlsMode`.
const SQL_TLS_HINT: &str = "TLS handshake failed. Set `tls.ca_file: /path/to/ca-bundle.pem` if your DB uses a private CA, or `tls.mode: require` to encrypt without verifying the certificate; if the server has no TLS at all, `tls.mode: disable` (trusted networks only).";

/// Map a categorised source error (+ raw text) to an actionable hint.
pub(crate) fn source_error_hint(
    category: &'static str,
    err: &anyhow::Error,
    source_type: &crate::config::SourceType,
) -> Option<&'static str> {
    use crate::config::SourceType;
    let msg = err.to_string().to_lowercase();

    if (msg.contains("tls") && !msg.contains("tls required"))
        || msg.contains("ssl")
        || msg.contains("certificate")
        || msg.contains("handshake")
    {
        return Some(match source_type {
            SourceType::Postgres | SourceType::Mysql => SQL_TLS_HINT,
            SourceType::Mssql => {
                "TLS handshake failed. SQL Server forces TLS on the login handshake; set `tls.ca_file: /path/to/ca-bundle.pem` to trust a private CA, or `tls.accept_invalid_certs: true` for a self-signed dev cert."
            }
            SourceType::Oracle => {
                "TLS handshake failed. Oracle over TLS uses `tcps`: check the listener's TCPS port and that its certificate chains to a public CA in the bundle compiled into the driver (not the system trust store; a private `tls.ca_file` is not supported yet)."
            }
            SourceType::Mongo => {
                "TLS handshake failed. If the MongoDB server has no TLS, set `tls.mode: disable` (trusted networks only); for a private CA set `tls.ca_file: /path/to/ca-bundle.pem`, or `tls.accept_invalid_certs: true` for a self-signed dev cert."
            }
        });
    }

    match category {
        "unknown service" => Some(
            "The listener does not know the service named in the URL path \
             (oracle://USER@HOST:PORT/SERVICE) — the login was never attempted. \
             List the registered services with `lsnrctl services` on the server; \
             Oracle Free's pluggable database is FREEPDB1.",
        ),
        "unknown database" => Some(
            "The database named in the URL does not exist on this server. Check \
             the path segment of the connection URL — credentials are fine (the \
             server authenticated you far enough to say so).",
        ),
        "transient" => Some(
            "The server refused the connection for a TRANSIENT reason (connection \
             cap reached, or still starting up). Retry shortly; if it persists, \
             check max_connections / the pooler, not the credentials.",
        ),
        "auth error" => Some(match source_type {
            SourceType::Postgres => {
                "Verify the user/password and that pg_hba.conf permits your client IP. The user also needs SELECT on the target tables and USAGE on the schema."
            }
            SourceType::Mysql => {
                "Verify the user/password and that the user has SELECT grants on the target tables. MySQL `GRANT SELECT ON db.* TO 'user'@'host'` plus `FLUSH PRIVILEGES`."
            }
            SourceType::Mssql => {
                "Verify the SQL login/password and that the login maps to a database user with SELECT on the target tables (`GRANT SELECT ON dbo.tbl TO [user]`). Check you are pointed at the right database — contained-DB users and server logins are resolved differently."
            }
            SourceType::Oracle => {
                "Verify the user/password (ORA-01017) and that the URL path is the SERVICE name (not a SID). The user needs CREATE SESSION and SELECT on the target tables; SELECT_CATALOG_ROLE (or SELECT on V$SYSSTAT and V$SYSTEM_EVENT) lets rivet read source-harm counters and governor pressure; row estimates come from ALL_TABLES and need no extra grant."
            }
            SourceType::Mongo => {
                "Verify the user/password and `authSource`. MongoDB scopes users to an auth database — add `?authSource=admin` (or the DB where the user was created) to the connection URL, and grant the user the `read` role on the target database."
            }
        }),
        "connectivity error" => Some(
            "Verify host/port reachability from this machine. If the DB is behind a bastion or VPN, ensure the tunnel is up before running rivet. `rivet doctor` must run from the same network as `rivet run` will.",
        ),
        _ => None,
    }
}

/// Map a categorised destination error (+ raw text) to an actionable hint.
pub(super) fn destination_error_hint(
    category: &'static str,
    dest: &crate::config::DestinationConfig,
) -> Option<&'static str> {
    match category {
        "sas expired" => Some(
            "Azure SAS token is expired or near-expiry. Generate a new SAS via `az storage container generate-sas --permissions rwdlc --expiry <future-date>` and re-export AZURE_STORAGE_SAS_TOKEN.",
        ),
        "permission error" => Some("Verify filesystem permissions on the destination directory."),
        "auth error" => Some(match dest.destination_type {
            DestinationType::S3 => {
                "Verify AWS credentials resolve (env / profile / instance role) and that the role has s3:PutObject + s3:GetObject + s3:ListBucket on the prefix. See docs/cloud-permissions.md."
            }
            DestinationType::Gcs => {
                "Verify the service account credentials resolve (ADC / env / explicit credentials_file) and that the principal has storage.objects.{create,get,list} on the bucket. See docs/cloud-permissions.md."
            }
            DestinationType::Azure => {
                "Verify Azure credentials. Account-key auth: check account_key_env. SAS auth: regenerate the SAS with rwdlc permissions and a future expiry. See docs/cloud-permissions.md."
            }
            DestinationType::Local | DestinationType::Stdout => {
                "Verify filesystem permissions on the destination directory."
            }
        }),
        "bucket not found" | "container not found" => Some(match dest.destination_type {
            DestinationType::S3 => {
                "Bucket must already exist; rivet does NOT auto-create. `aws s3 mb s3://<bucket>` (with the right region) before running."
            }
            DestinationType::Gcs => {
                "Bucket must already exist; rivet does NOT auto-create. `gcloud storage buckets create gs://<bucket>` before running."
            }
            DestinationType::Azure => {
                "Container must already exist; rivet does NOT auto-create. `az storage container create --account-name <acct> --name <container>` before running."
            }
            _ => "Path / bucket / container must already exist.",
        }),
        "connectivity error" => Some(match dest.destination_type {
            DestinationType::S3 => {
                "Verify endpoint and region. For non-AWS endpoints (MinIO / R2 / Wasabi) set `endpoint:` explicitly. For AWS, ensure `region:` matches the bucket's region — cross-region writes fail with a confusing redirect error."
            }
            DestinationType::Gcs => {
                "Verify network reachability to storage.googleapis.com. If using a custom endpoint, set `endpoint:` explicitly."
            }
            DestinationType::Azure => {
                "Verify network reachability to <account>.blob.core.windows.net. For Azurite or sovereign clouds, set `endpoint:` explicitly."
            }
            _ => "Verify network reachability to the destination.",
        }),
        "path not found" => Some(
            "Parent directory must exist. Create it with `mkdir -p` before running, or use a different `path:` in your config.",
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A check's detail is redacted where it is recorded, so the text line and the JSON report carry the same masked string.
    #[test]
    fn a_recorded_check_carries_no_password() {
        let mut checks = Vec::new();
        emit_check(
            &mut checks,
            true,
            DoctorCheck {
                name: "Source auth error".into(),
                ok: false,
                detail: Some(
                    "nothing is listening on host=h port=1 user=u password=S3cr3tPw dbname=d"
                        .into(),
                ),
                hint: None,
            },
        );
        assert_eq!(
            checks[0].detail.as_deref(),
            Some("nothing is listening on host=h port=1 user=u password=*** dbname=d")
        );
    }

    /// `run_plan_checks` records one `Run plan <export>` check per verdict, returns the first refusal, and turns a failed probe into a `config` check.
    #[test]
    fn run_plan_checks_records_each_verdict_and_returns_the_first_refusal() {
        let config = Config::from_yaml(
            "source: { type: postgres, url: \"postgresql://u:p@127.0.0.1:1/db\" }\n\
             exports:\n\
             \x20 - name: a\n    table: t\n    mode: full\n    format: parquet\n\
             \x20   destination: { type: local, path: ./out }\n",
        )
        .expect("the fixture config loads");
        let dir = std::path::Path::new(".");
        let mixed = |_: &Config, _: &std::path::Path| -> Result<Vec<(String, Result<()>)>> {
            Ok(vec![
                ("a".to_string(), Ok(())),
                ("b".to_string(), Err(anyhow::anyhow!("first refusal"))),
                ("c".to_string(), Err(anyhow::anyhow!("second refusal"))),
            ])
        };
        let mut checks = Vec::new();
        let refusal = run_plan_checks(&config, dir, &mixed, true, &mut checks)
            .expect("the first refusal is returned");
        assert_eq!(format!("{refusal:#}"), "first refusal");
        let seen: Vec<_> = checks
            .iter()
            .map(|c| {
                (
                    c.name.as_str(),
                    c.ok,
                    c.detail.as_deref(),
                    c.hint.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            seen,
            vec![
                ("Run plan a", true, None, None),
                ("Run plan b", false, Some("first refusal"), None),
                ("Run plan c", false, Some("second refusal"), None),
            ]
        );

        let clean = |_: &Config, _: &std::path::Path| -> Result<Vec<(String, Result<()>)>> {
            Ok(vec![("a".to_string(), Ok(()))])
        };
        let mut checks = Vec::new();
        assert!(run_plan_checks(&config, dir, &clean, true, &mut checks).is_none());
        assert_eq!(checks.len(), 1);
        assert!(checks[0].ok);

        let broken = |_: &Config, _: &std::path::Path| -> Result<Vec<(String, Result<()>)>> {
            Err(anyhow::anyhow!("probe failed"))
        };
        let mut checks = Vec::new();
        let refusal = run_plan_checks(&config, dir, &broken, true, &mut checks)
            .expect("a failed probe is a refusal");
        assert_eq!(format!("{refusal:#}"), "probe failed");
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].name, "Run plan config");
        assert!(!checks[0].ok);
    }

    #[test]
    fn every_tls_mode_a_tls_hint_names_is_one_the_loader_accepts() {
        let err = anyhow::anyhow!("TLS handshake failed: certificate verify failed");
        for engine in [
            SourceType::Postgres,
            SourceType::Mysql,
            SourceType::Mssql,
            SourceType::Oracle,
            SourceType::Mongo,
        ] {
            let hint = source_error_hint("error", &err, &engine).expect("a TLS hint");
            for named in hint.split("`tls.mode: ").skip(1) {
                let mode = named.split('`').next().unwrap();
                serde_yaml_ng::from_str::<crate::config::TlsMode>(mode).unwrap_or_else(|e| {
                    panic!(
                        "{engine:?} hint names `tls.mode: {mode}`, which the loader refuses: {e}"
                    )
                });
            }
        }
        assert_eq!(
            source_error_hint("error", &err, &SourceType::Postgres),
            Some(
                "TLS handshake failed. Set `tls.ca_file: /path/to/ca-bundle.pem` if your DB uses a private CA, or `tls.mode: require` to encrypt without verifying the certificate; if the server has no TLS at all, `tls.mode: disable` (trusted networks only)."
            )
        );
    }

    fn transient_probe(detail: &str) -> Transient {
        (
            detail.to_string(),
            crate::error::retry::RetryClass::Transient {
                needs_reconnect: true,
                extra_delay_ms: 0,
            },
        )
    }

    #[test]
    fn doctor_stops_carry_their_codes() {
        let err = doctor_failure_error(None);
        crate::error::assert_coded(&err, "RIVET_CLI_PREFLIGHT_FAILED", 1);
        let err = doctor_failure_error(Some(transient_probe("connection refused (os error 61)")));
        crate::error::assert_coded(&err, "RIVET_CLI_PREFLIGHT_UNVERIFIED", 2);
        // A config that does not load keeps the config error's own code.
        let err = doctor("/nonexistent/rivet-doctor-test.yaml", false, &|_, _| {
            Ok(vec![])
        })
        .unwrap_err();
        crate::error::assert_coded(&err, "RIVET_CONFIG_FILE_UNREADABLE", 1);
    }

    #[test]
    fn doctor_transient_failure_classifies_as_retryable_exit_2() {
        let transient =
            doctor_failure_error(Some(transient_probe("connection refused (os error 61)")));
        assert!(
            crate::error::retry::classify_error(&transient).is_transient(),
            "a transient probe failure must classify Retryable (exit 2): {transient:#}"
        );
        let generic = doctor_failure_error(None);
        assert!(
            !crate::error::retry::classify_error(&generic).is_transient(),
            "a non-transient failure must stay Generic (exit 1): {generic:#}"
        );
    }

    #[test]
    fn roast_doctor_write_probes_each_distinct_local_destination_path() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let config_dir = tempfile::tempdir().unwrap();

        let leaf_a = dir_a.path().join("probe_here");
        let leaf_b = dir_b.path().join("probe_here");

        let yaml = format!(
            r#"
source:
  type: postgres
  url_env: RIVET_ROAST_DOCTOR_DEDUP_UNSET_URL_ENV
exports:
  - name: roast_dest_a
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: "{a}"
  - name: roast_dest_b
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: "{b}"
"#,
            a = leaf_a.display(),
            b = leaf_b.display(),
        );
        let config_path = config_dir.path().join("rivet.yaml");
        std::fs::write(&config_path, yaml).unwrap();

        // Source auth fails on the unset env var, and doctor must say so — then still probe both paths.
        let err = doctor(config_path.to_str().unwrap(), false, &|_, _| Ok(vec![]))
            .expect_err("an unresolvable source must fail doctor, not pass it");
        assert!(
            format!("{err:#}").contains("one or more preflight checks failed"),
            "{err:#}"
        );

        let probe = crate::manifest::DOCTOR_PROBE_FILENAME;

        for (label, leaf) in [("first", &leaf_a), ("second", &leaf_b)] {
            assert!(
                leaf.exists(),
                "doctor never write-probed the {label} local destination {} — its dedup key \
                 must include `path`; a key that omits it collapses both local destinations to \
                 one entry and only probes the first, so an unwritable second directory would \
                 pass doctor and fail at run time",
                leaf.display()
            );
            assert!(
                !leaf.join(probe).exists(),
                "doctor left its write-probe `{probe}` at the {label} destination {} \
                 (FINDING #26: it must remove the destination-side probe, not only the local temp)",
                leaf.display()
            );
            assert!(
                std::fs::read_dir(leaf).unwrap().next().is_none(),
                "doctor must leave the {label} destination {} exactly as it created it (empty)",
                leaf.display()
            );
        }
    }

    fn dest_of(t: DestinationType) -> crate::config::DestinationConfig {
        crate::config::DestinationConfig {
            destination_type: t,
            ..Default::default()
        }
    }

    #[test]
    fn audit_pg_db_error_is_auth_with_hint() {
        let err = anyhow::anyhow!("db error");
        let cat = categorize_source_error(&err);
        assert_eq!(
            cat, "auth error",
            "Postgres wrong-password surfaces as 'db error'; categorizer returned {:?} instead of 'auth error'",
            cat
        );
        let hint = source_error_hint(cat, &err, &SourceType::Postgres);
        assert!(
            hint.is_some(),
            "no actionable hint produced for Postgres 'db error' (category {:?}); operator gets no next step",
            cat
        );
    }

    #[test]
    fn the_oracle_tls_hint_names_the_compiled_in_bundle_and_skips_the_policy_refusal() {
        let handshake = anyhow::anyhow!("oracle: TLS handshake failed: invalid peer certificate");
        let hint = source_error_hint("error", &handshake, &SourceType::Oracle).unwrap();
        assert!(
            hint.contains(
                "public CA in the bundle compiled into the driver (not the system trust store"
            ),
            "{hint}"
        );
        let policy = anyhow::anyhow!(
            "source: TLS required — refusing to connect to a remote (non-loopback) host"
        );
        assert_ne!(
            source_error_hint("error", &policy, &SourceType::Oracle),
            Some(hint)
        );
    }

    /// A coded config refusal whose text names a connect needle keeps its own hint.
    #[test]
    fn a_config_refusal_is_not_relabelled_by_its_text() {
        let err = crate::error::coded(
            crate::error::codes::CONFIG_GCP_SECRET_UNREADABLE,
            "source: Secret Manager refused 'projects/p/secrets/s/versions/latest' (HTTP 403)",
        );
        assert_eq!(categorize_source_error(&err), "error");
        let uncoded = anyhow::anyhow!("connection refused");
        assert_eq!(categorize_source_error(&uncoded), "connectivity error");
    }

    #[test]
    fn oracle_bad_credentials_and_unknown_service_get_their_own_categories() {
        let auth = anyhow::anyhow!(
            "oracle: ORA-01017: invalid credential or not authorized; logon denied"
        );
        assert_eq!(categorize_source_error(&auth), "auth error");
        let service = anyhow::anyhow!(
            "oracle: ORA-12514: Cannot connect to database. Service FREE_NOPE is not registered \
             with the listener at host 127.0.0.1 port 1521. (CONNECTION_ID=abc)"
        );
        assert_eq!(categorize_source_error(&service), "unknown service");
        assert!(source_error_hint("unknown service", &service, &SourceType::Oracle).is_some());
    }

    #[test]
    fn audit_mssql_login_failed_is_auth_with_hint() {
        let err = anyhow::anyhow!("login failed for user 'sa'");
        let cat = categorize_source_error(&err);
        assert_eq!(
            cat, "auth error",
            "MSSQL bad login surfaces as 'Login failed for user ...'; categorizer returned {:?} instead of 'auth error'",
            cat
        );
        let hint = source_error_hint(cat, &err, &SourceType::Mssql);
        assert!(
            hint.is_some(),
            "no actionable hint produced for MSSQL 'login failed for user' (category {:?})",
            cat
        );
    }

    #[test]
    fn audit_mysql_access_denied_is_auth_with_hint() {
        let err = anyhow::anyhow!("access denied for user");
        let cat = categorize_source_error(&err);
        assert_eq!(
            cat, "auth error",
            "MySQL 'access denied for user' must stay auth; categorizer returned {:?}",
            cat
        );
        let hint = source_error_hint(cat, &err, &SourceType::Mysql);
        assert!(
            hint.is_some(),
            "no actionable hint produced for MySQL 'access denied for user' (category {:?})",
            cat
        );
    }

    #[test]
    fn audit_s3_permission_denied_403_is_auth_with_hint() {
        let dest = dest_of(DestinationType::S3);
        let err = anyhow::anyhow!(
            "PermissionDenied at write => InvalidAccessKeyId, status: 403, https://bucket.s3.amazonaws.com/probe"
        );
        let cat = categorize_dest_error(&err, &dest);
        assert_eq!(
            cat, "auth error",
            "S3 'PermissionDenied/InvalidAccessKeyId/403' must categorize as auth; categorizer returned {:?}",
            cat
        );
        let hint = destination_error_hint(cat, &dest);
        assert!(
            hint.is_some(),
            "no actionable hint produced for S3 auth failure (category {:?}); operator gets no next step",
            cat
        );
    }

    #[test]
    fn audit_azure_send_request_error_is_connectivity_with_hint() {
        let dest = dest_of(DestinationType::Azure);
        let err = anyhow::anyhow!(
            "error sending request for url (https://x.blob.core.windows.net/probe)"
        );
        let cat = categorize_dest_error(&err, &dest);
        assert_eq!(
            cat, "connectivity error",
            "Azure 'error sending request for url' must categorize as connectivity; categorizer returned {:?}",
            cat
        );
        let hint = destination_error_hint(cat, &dest);
        assert!(
            hint.is_some(),
            "no actionable hint produced for Azure connectivity failure (category {:?})",
            cat
        );
    }

    #[test]
    fn audit_dest_connection_refused_is_connectivity_with_hint() {
        let dest = dest_of(DestinationType::S3);
        let err = anyhow::anyhow!("connection refused");
        let cat = categorize_dest_error(&err, &dest);
        assert_eq!(
            cat, "connectivity error",
            "'connection refused' must stay connectivity; categorizer returned {:?}",
            cat
        );
        let hint = destination_error_hint(cat, &dest);
        assert!(
            hint.is_some(),
            "no actionable hint produced for 'connection refused' (category {:?})",
            cat
        );
    }

    /// One input per OR-ARM (round-10 mutants: each `||` in the needle chains was un-graded.
    #[test]
    fn every_needle_arm_has_its_own_fixture_and_a_hint() {
        for msg in [
            "FATAL: database \"nope\" does not exist",
            "Unknown database 'nope'",
            "Cannot open database \"nope\" requested by the login",
        ] {
            assert_eq!(
                categorize_source_error(&anyhow::anyhow!("{msg}")),
                "unknown database",
                "{msg}"
            );
        }
        assert_ne!(
            categorize_source_error(&anyhow::anyhow!("FATAL: role \"rivet\" does not exist")),
            "unknown database"
        );
        for msg in [
            "FATAL: sorry, too many clients already",
            "FATAL: remaining connection slots are reserved",
            "the database system is starting up",
        ] {
            assert_eq!(
                categorize_source_error(&anyhow::anyhow!("{msg}")),
                "transient",
                "{msg}"
            );
        }
        for cat in ["unknown database", "transient"] {
            let err = anyhow::anyhow!("probe");
            let hint = source_error_hint(cat, &err, &crate::config::SourceType::Postgres)
                .unwrap_or_else(|| panic!("category {cat} must carry a hint"));
            assert!(hint.len() > 40, "{cat}: a one-word hint helps nobody");
        }
    }

    /// MSSQL 4060 routes to "unknown database", NOT auth.
    #[test]
    fn mssql_wrong_database_is_not_an_auth_error() {
        let err = anyhow::anyhow!(
            "Cannot open database \"prod_reporting\" requested by the login. \
             The login failed. Login failed for user 'rivet'."
        );
        assert_eq!(categorize_source_error(&err), "unknown database");
    }

    #[test]
    fn source_pg_nested_password_cause_via_alternate_is_auth() {
        let root = anyhow::anyhow!("password authentication failed for user \"rivet\"");
        let wrapped = root.context("db error");
        assert_eq!(categorize_source_error(&wrapped), "auth error");
        assert_eq!(
            categorize_source_error(&anyhow::anyhow!("db error")),
            "auth error"
        );
    }

    #[test]
    fn source_connection_refused_stays_connectivity_not_auth() {
        let err = anyhow::anyhow!("error connecting to server: Connection refused (os error 61)");
        assert_eq!(categorize_source_error(&err), "connectivity error");
    }

    #[test]
    fn dest_no_space_permissiondenied_is_auth() {
        let dest = dest_of(DestinationType::Gcs);
        let err = anyhow::anyhow!("PermissionDenied (persistent) at write");
        assert_eq!(categorize_dest_error(&err, &dest), "auth error");
        assert!(destination_error_hint("auth error", &dest).is_some());
    }

    #[test]
    fn dest_send_http_request_is_connectivity() {
        let dest = dest_of(DestinationType::S3);
        let err = anyhow::anyhow!("failed to send http request to the store");
        assert_eq!(categorize_dest_error(&err, &dest), "connectivity error");
    }

    #[test]
    fn dest_404_stays_bucket_not_found_after_403_needle_added() {
        let dest = dest_of(DestinationType::S3);
        let err = anyhow::anyhow!("NoSuchBucket, status: 404");
        assert_eq!(categorize_dest_error(&err, &dest), "bucket not found");
    }

    #[test]
    fn remove_destination_probe_local_deletes_the_probe_object() {
        let dir = tempfile::tempdir().unwrap();
        let probe_key = crate::manifest::DOCTOR_PROBE_FILENAME;
        std::fs::write(dir.path().join(probe_key), b"ok").unwrap();
        let dest = crate::config::DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        remove_destination_probe(&dest, probe_key);
        assert!(
            std::fs::read_dir(dir.path()).unwrap().next().is_none(),
            "destination prefix must be left exactly as doctor found it (empty)"
        );
    }

    #[test]
    fn remove_destination_probe_local_uses_prefix_when_path_unset() {
        let dir = tempfile::tempdir().unwrap();
        let probe_key = crate::manifest::DOCTOR_PROBE_FILENAME;
        std::fs::write(dir.path().join(probe_key), b"ok").unwrap();
        let dest = crate::config::DestinationConfig {
            destination_type: DestinationType::Local,
            prefix: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        remove_destination_probe(&dest, probe_key);
        assert!(
            !dir.path().join(probe_key).exists(),
            "cleanup must follow the same base-path resolution as the writer"
        );
    }

    fn local_dest_at(path: &std::path::Path) -> crate::config::DestinationConfig {
        crate::config::DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(path.to_string_lossy().into_owned()),
            ..Default::default()
        }
    }

    #[test]
    fn parallel_destination_probes_on_separate_local_destinations_all_succeed() {
        let failures: Vec<String> = std::thread::scope(|s| {
            let workers: Vec<_> = (0..12)
                .map(|_| {
                    s.spawn(|| {
                        let mut errs = Vec::new();
                        for _ in 0..150 {
                            let dir = tempfile::tempdir().unwrap();
                            if let Err(e) = check_destination_auth(&local_dest_at(dir.path())) {
                                errs.push(format!("{e:#}"));
                            }
                        }
                        errs
                    })
                })
                .collect();
            workers
                .into_iter()
                .flat_map(|w| w.join().unwrap())
                .collect()
        });
        assert!(
            failures.is_empty(),
            "{} of 1800 probes failed: {:?}",
            failures.len(),
            failures.first()
        );
    }

    struct ReadingStore(std::sync::atomic::AtomicUsize);

    impl crate::destination::Destination for ReadingStore {
        fn write(
            &self,
            local_path: &std::path::Path,
            _remote_key: &str,
        ) -> Result<crate::destination::WriteOutcome> {
            let body = std::fs::read(local_path)?;
            assert_eq!(body, b"ok");
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::destination::WriteOutcome::default())
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            crate::destination::DestinationCapabilities {
                commit_protocol: crate::destination::WriteCommitProtocol::FinalizeOnClose,
                idempotent_overwrite: true,
                retry_safe: true,
                partial_write_risk: false,
            }
        }
    }

    #[test]
    fn parallel_destination_probes_through_a_store_that_reads_the_file_all_succeed() {
        let store = ReadingStore(Default::default());
        let dest = dest_of(DestinationType::S3);
        let failures: usize = std::thread::scope(|s| {
            (0..12)
                .map(|_| {
                    s.spawn(|| {
                        (0..150)
                            .filter(|_| write_destination_probe(&store, &dest).is_err())
                            .count()
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|w| w.join().unwrap())
                .sum()
        });
        assert_eq!(failures, 0);
        assert_eq!(store.0.load(std::sync::atomic::Ordering::SeqCst), 1800);
    }

    #[test]
    fn destination_probe_on_a_local_prefix_leaves_the_prefix_empty() {
        let dir = tempfile::tempdir().unwrap();
        check_destination_auth(&local_dest_at(dir.path())).unwrap();
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn destination_probe_on_a_prefix_that_is_a_file_fails_with_the_write_error() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let err = check_destination_auth(&local_dest_at(&blocker)).unwrap_err();
        let text = trim_probe_error(&err);
        assert_eq!(text, "File exists (os error 17)");
        assert!(blocker.is_file());
    }

    #[test]
    fn remove_destination_probe_missing_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let dest = crate::config::DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        remove_destination_probe(&dest, crate::manifest::DOCTOR_PROBE_FILENAME);
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn local_permission_denied_is_permission_error_not_auth() {
        let dest = dest_of(DestinationType::Local);
        let err = anyhow::anyhow!("Permission denied (os error 13)");
        let cat = categorize_dest_error(&err, &dest);
        assert_eq!(
            cat, "permission error",
            "a local FS `os error 13` is a directory-permission problem, not auth; got {cat:?}"
        );
        assert!(
            destination_error_hint(cat, &dest).is_some(),
            "permission error must still surface the filesystem-permissions hint"
        );
    }

    #[test]
    fn cloud_permission_denied_stays_auth_error() {
        for t in [
            DestinationType::S3,
            DestinationType::Gcs,
            DestinationType::Azure,
        ] {
            let dest = dest_of(t);
            let err = anyhow::anyhow!("PermissionDenied at write");
            assert_eq!(
                categorize_dest_error(&err, &dest),
                "auth error",
                "cloud permission denial must remain auth for {t:?}"
            );
        }
    }

    #[test]
    fn trim_probe_error_strips_http_response_parts_and_headers() {
        let raw = "PermissionDenied (persistent) at write, context: { uri: https://b.s3.amazonaws.com/probe, response: Parts { status: 403, version: HTTP/1.1, headers: {\"x-amz-request-id\": \"ABC123\", \"content-type\": \"application/xml\"} }, service: s3 } => InvalidAccessKeyId";
        let err = anyhow::anyhow!(raw);
        let out = trim_probe_error(&err);
        assert!(
            !out.contains("Parts {") && !out.to_lowercase().contains("headers: {"),
            "trimmed error still leaks the HTTP response dump: {out:?}"
        );
        assert!(
            !out.contains('\n'),
            "trimmed error must be a single line: {out:?}"
        );
        assert!(
            out.starts_with("PermissionDenied (persistent) at write"),
            "trimmed error must keep the meaningful root-cause prefix: {out:?}"
        );
    }

    #[test]
    fn trim_probe_error_leaves_clean_message_intact() {
        let err = anyhow::anyhow!("error connecting to server: Connection refused (os error 61)");
        assert_eq!(
            trim_probe_error(&err),
            "error connecting to server: Connection refused (os error 61)"
        );
    }

    #[test]
    fn trim_probe_error_caps_unbounded_line() {
        let err = anyhow::anyhow!("x".repeat(5000));
        let out = trim_probe_error(&err);
        assert!(
            out.chars().count() <= 1201,
            "line not capped: {} chars",
            out.chars().count()
        );
        assert!(
            out.ends_with('…'),
            "capped line must signal truncation: {out:?}"
        );
    }

    #[test]
    fn trim_probe_error_preserves_multiline_hint_verbatim() {
        let err = anyhow::anyhow!(
            "chunked mode needs one of:\n  - chunk_column: <int col>\n  - chunk_by_key: <col>\n  - chunk_count: <n>"
        );
        let out = trim_probe_error(&err);
        assert!(out.contains("chunk_column"), "got: {out:?}");
        assert!(out.contains("chunk_by_key"), "got: {out:?}");
        assert!(
            out.contains("chunk_count"),
            "all options preserved: {out:?}"
        );
        assert!(out.contains('\n'), "newlines preserved: {out:?}");
    }

    #[test]
    fn config_load_failure_returns_pointer_not_duplicate_message() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("rivet.yaml");
        std::fs::write(&cfg, "source: not-a-mapping\n").unwrap();
        let err = doctor(cfg.to_str().unwrap(), false, &|_, _| Ok(vec![]))
            .expect_err("doctor must return Err when the config fails to load");
        let msg = err.to_string();
        assert!(
            msg.contains("doctor: config check failed") && msg.contains("see output above"),
            "returned error must be the one-line pointer (so `main` does not double-print the \
             config error); got {msg:?}"
        );
    }
    /// A destination that fails for good, behind a source that failed for good, is not reported as worth a retry.
    #[test]
    fn a_permanent_destination_failure_is_not_reported_as_transient() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("a_file");
        std::fs::write(&blocker, b"x").unwrap();
        let yaml = format!(
            r#"
source:
  type: postgres
  url_env: RIVET_DOCTOR_PERMANENT_DEST_UNSET_URL_ENV
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: "{}"
"#,
            blocker.join("under_a_file").display(),
        );
        let config_path = dir.path().join("rivet.yaml");
        std::fs::write(&config_path, yaml).unwrap();

        let err = doctor(config_path.to_str().unwrap(), true, &|_, _| Ok(vec![]))
            .expect_err("a directory under a regular file cannot be written");
        assert_eq!(
            format!("{err:#}"),
            "doctor: one or more preflight checks failed (see output above)"
        );
    }

    /// A CDC health check that fails for good, behind a source that failed for good and a destination that answers, is not reported as worth a retry.
    #[test]
    fn a_permanent_cdc_health_failure_is_not_reported_as_transient() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
source:
  type: postgres
  url_env: RIVET_DOCTOR_PERMANENT_CDC_UNSET_URL_ENV
exports:
  - name: t
    table: t
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: ./t.ckpt }}
    destination:
      type: local
      path: "{}"
"#,
            dir.path().join("out").display(),
        );
        let config_path = dir.path().join("rivet.yaml");
        std::fs::write(&config_path, yaml).unwrap();
        let config = Config::load(config_path.to_str().unwrap()).expect("the config loads");
        let (cdc, _) = crate::preflight::cdc_health::collect(&config, dir.path());
        assert!(
            cdc.iter().any(|c| !c.ok && c.detail.is_some()),
            "fixture: a failed CDC health check with a detail: {cdc:?}"
        );

        let err = doctor(config_path.to_str().unwrap(), true, &|_, _| Ok(vec![]))
            .expect_err("the source URL is not set");
        assert_eq!(
            format!("{err:#}"),
            "doctor: one or more preflight checks failed (see output above)"
        );
    }
}
