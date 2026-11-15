//! CDC health probes for `rivet doctor` — automates the monitoring the CDC reference tells operators to do by hand (docs/reference/cdc.md):

use crate::config::{Config, DEFAULT_PG_SLOT, ExportConfig, ExportMode, SourceType};
use crate::error::Result;

use super::doctor::DoctorCheck;

/// A slot pinning more retained WAL than this fails the check.
const PG_RETAINED_WAL_FAIL_BYTES: i64 = 1 << 30;

/// The bar, with a test seam.
fn retained_wal_bar() -> i64 {
    std::env::var("RIVET_TEST_SLOT_WAL_BAR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(PG_RETAINED_WAL_FAIL_BYTES)
}

fn mib(bytes: i64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

fn check(name: String, ok: bool, detail: Option<String>, hint: Option<String>) -> DoctorCheck {
    DoctorCheck {
        name,
        ok,
        detail,
        hint,
    }
}

/// The hint for a checkpoint the run refuses: restore it, or recover in the order that loses nothing.
fn restore_or_recover() -> String {
    format!(
        "restore the file, or: {}",
        crate::source::cdc::checkpoint_identity::RECOVER
    )
}

/// The text and typed verdict of the error a probe stopped at.
fn probe_verdict(e: &anyhow::Error) -> (String, crate::error::retry::RetryClass) {
    (format!("{e:#}"), crate::error::retry::classify_error(e))
}

fn probe_failed(e: &anyhow::Error) -> DoctorCheck {
    check(
        "CDC health probe".into(),
        false,
        Some(super::doctor::trim_probe_error(e)),
        Some(
            "the CDC probe stopped at this error — fix what it reports; any CDC checks listed \
             above this line already ran and still apply"
                .into(),
        ),
    )
}

/// Entry point: every CDC health check for the config, or empty when the config has no `mode: cdc` exports.
pub(super) fn collect(
    config: &Config,
    config_dir: &std::path::Path,
) -> (
    Vec<DoctorCheck>,
    Option<(String, crate::error::retry::RetryClass)>,
) {
    let cdc: Vec<&ExportConfig> = config
        .exports
        .iter()
        .filter(|e| e.mode == ExportMode::Cdc)
        .collect();
    if cdc.is_empty() {
        return (Vec::new(), None);
    }
    let url = match config.source.resolve_url() {
        Ok(u) => u,
        Err(e) => return (vec![probe_failed(&e)], Some(probe_verdict(&e))),
    };
    let url = url.expose();
    let tls = config.source.tls.as_ref();
    let mut checks = Vec::new();
    let result = match config.source.source_type {
        SourceType::Postgres => pg_checks(url, tls, &cdc, config_dir, &mut checks),
        SourceType::Mysql => mysql_checks(url, tls, &cdc, config_dir, &mut checks),
        SourceType::Mssql => mssql_checks(url, tls, &cdc, config_dir, &mut checks),
        SourceType::Mongo => mongo_checks(config, url, tls, &cdc, config_dir, &mut checks),
        #[cfg(feature = "oracle")]
        SourceType::Oracle => oracle_checks(url, tls, &cdc, config_dir, &mut checks),
        #[cfg(not(feature = "oracle"))]
        SourceType::Oracle => Err(crate::source::oracle_feature_missing()),
    };
    let mut probe = None;
    match result {
        Err(e) => {
            checks.push(probe_failed(&e));
            probe = Some(probe_verdict(&e));
        }
        Ok(()) => checks.extend(run_refusals(
            config,
            seeded!("doctor-green-where-the-run-refuses", &cdc[..0], &cdc[..]),
            url,
        )),
    }
    (checks, probe)
}

/// One failed check per CDC export whose prerequisites the run itself would refuse, asked through the run's own function.
fn run_refusals(config: &Config, exports: &[&ExportConfig], url: &str) -> Vec<DoctorCheck> {
    let engine = crate::source::cdc::CdcEngine::from(config.source.source_type);
    exports
        .iter()
        .filter_map(|e| {
            let cdc = e.cdc.clone().unwrap_or_default();
            let tables: Vec<String> = e
                .table
                .iter()
                .chain(e.tables.iter().flatten())
                .cloned()
                .collect();
            let drain = crate::source::cdc::DrainMode::from_until_current(cdc.until_current);
            let refused = engine
                .refuse_unmet_prerequisites(
                    url,
                    config.source.tls.as_ref(),
                    &tables,
                    drain,
                    config.source.columns_from_grants(),
                )
                .err()?;
            Some(run_refusal_check(&e.name, &refused))
        })
        .collect()
}

/// The failed check for a prerequisite the run refuses: the refusal's own text, which names its remedy.
fn run_refusal_check(export: &str, refused: &anyhow::Error) -> DoctorCheck {
    check(
        format!("CDC run prerequisites (export '{export}')"),
        false,
        Some(super::doctor::trim_probe_error(refused)),
        None,
    )
}

struct PgSlot {
    active: bool,
    retained_bytes: i64,
}

/// Verdict for the export's own slot.
pub(crate) fn pg_retained_wal_warning(
    slot: &str,
    retained_bytes: i64,
    active: bool,
) -> Option<String> {
    if retained_bytes < retained_wal_bar() {
        return None;
    }
    Some(format!(
        "slot '{slot}' is pinning {} of WAL (active={active}) — the source disk is filling. \
         This run will drain it, but a drain that far behind takes time and the WAL keeps \
         growing meanwhile. If capture here is retired instead: \
         SELECT pg_drop_replication_slot('{slot}'); consider max_slot_wal_keep_size as a \
         blast-radius bound",
        mib(retained_bytes)
    ))
}

/// The same question for slots the CALLER did not name as its own.
pub(crate) fn pg_foreign_slots_warning(
    foreign: &[(String, i64, String)],
    may_be_owned_elsewhere: bool,
) -> Option<String> {
    let bar = retained_wal_bar();
    let total: i64 = foreign.iter().map(|(_, b, _)| *b).sum();
    if foreign.is_empty() {
        return None;
    }
    if total < bar {
        return None;
    }
    let mut sorted: Vec<&(String, i64, String)> = foreign.iter().collect();
    sorted.sort_by_key(|(_, b, _)| -*b);
    let listing = sorted
        .iter()
        .take(10)
        .map(|(n, b, kind)| {
            if kind == "physical" {
                format!(
                    "{n} ({}, PHYSICAL — a standby's, not a CDC leftover)",
                    mib(*b)
                )
            } else {
                format!("{n} ({})", mib(*b))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let more = foreign.len().saturating_sub(10);
    let tail = if more > 0 {
        format!(" and {more} more")
    } else {
        String::new()
    };
    let head = format!(
        "inactive slot(s) are pinning WAL: {listing}{tail} — {} across {} slot(s)",
        mib(total),
        foreign.len()
    );
    Some(if may_be_owned_elsewhere {
        format!(
            "{head}. This run drains ONE export's slot and cannot tell whether another \
             export in your config, or another tool, owns the rest — `rivet doctor -c \
             <your config>` sees them all and will say which are genuinely unclaimed. \
             Do NOT drop a slot on this warning alone"
        )
    } else {
        format!(
            "{head}, and none belongs to this config. If the consumer is gone for good: \
             SELECT pg_drop_replication_slot('{}') — but never for a slot marked \
             PHYSICAL, which is a standby's",
            sorted
                .iter()
                .find(|(_, _, k)| k != "physical")
                .map(|(n, _, _)| n.as_str())
                .unwrap_or("<slot>")
        )
    })
}

/// `resume_ckpt`: a checkpoint file exists AND carries a position.
fn pg_slot_verdict(
    export: &str,
    slot: &str,
    state: Option<PgSlot>,
    resume_ckpt: bool,
) -> DoctorCheck {
    let name = format!("CDC slot '{slot}' (export '{export}')");
    match state {
        None if resume_ckpt => check(
            name,
            false,
            Some(crate::source::postgres::cdc::pg_slot_missing_refusal(
                slot,
                crate::source::cdc::PriorRun {
                    checkpoint: true,
                    snapshot: false,
                },
            )),
            None,
        ),
        None => check(
            name,
            true,
            Some("slot absent — created on the first run".into()),
            None,
        ),
        Some(s) if s.retained_bytes < PG_RETAINED_WAL_FAIL_BYTES => check(
            name,
            true,
            Some(format!(
                "retained WAL {}, active={}",
                mib(s.retained_bytes),
                s.active
            )),
            None,
        ),
        Some(s) => check(
            name,
            false,
            pg_retained_wal_warning(slot, s.retained_bytes, s.active),
            Some(
                "run the CDC export to drain it (advancing the slot releases WAL), or drop it \
                 if capture is retired: SELECT pg_drop_replication_slot('<slot>'); consider \
                 max_slot_wal_keep_size as a blast-radius bound"
                    .into(),
            ),
        ),
    }
}

/// Verdict over *other* inactive slots on the instance.
fn pg_foreign_slots_verdict(foreign: &[(String, i64, String)]) -> DoctorCheck {
    let name = "CDC other inactive slots".to_string();
    if foreign.is_empty() {
        return check(name, true, Some("none".into()), None);
    }
    match pg_foreign_slots_warning(foreign, false) {
        None => {
            let total: i64 = foreign.iter().map(|(_, b, _)| *b).sum();
            let listing = foreign
                .iter()
                .map(|(n, b, _)| format!("{n} ({})", mib(*b)))
                .collect::<Vec<_>>()
                .join(", ");
            check(
                name,
                true,
                Some(format!(
                    "inactive but small: {listing} — {} across {} slot(s)",
                    mib(total),
                    foreign.len()
                )),
                None,
            )
        }
        Some(why) => check(name, false, Some(why), None),
    }
}

fn pg_checks(
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    exports: &[&ExportConfig],
    config_dir: &std::path::Path,
    checks: &mut Vec<DoctorCheck>,
) -> Result<()> {
    let mut client = crate::source::postgres::connect_client(url, tls)?;
    let mut ours: Vec<String> = Vec::new();
    for e in exports {
        let slot = e
            .cdc
            .as_ref()
            .and_then(|c| c.slot.clone())
            .unwrap_or_else(|| DEFAULT_PG_SLOT.to_string());
        let row = client.query_opt(
            "SELECT active, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )?;
        let state = row.map(|r| PgSlot {
            active: r.get(0),
            retained_bytes: r.get(1),
        });
        let resume_ckpt = match e.cdc.as_ref().and_then(|c| c.checkpoint.as_deref()) {
            None => false,
            Some(raw) => {
                let p = &crate::source::cdc::resolve_checkpoint(raw, config_dir);
                crate::source::cdc::Position::load(p)?.is_some()
            }
        };
        checks.push(pg_slot_verdict(&e.name, &slot, state, resume_ckpt));
        ours.push(slot);
    }
    let rows = client.query(
        "SELECT slot_name, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint, \
                slot_type \
         FROM pg_replication_slots WHERE NOT active AND slot_name <> ALL($1)",
        &[&ours],
    )?;
    let foreign: Vec<(String, i64, String)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    checks.push(pg_foreign_slots_verdict(&foreign));
    Ok(())
}

/// `true` when the server has binary logging OFF.
fn binlog_disabled(vars: &[(String, String)]) -> bool {
    let log_bin = vars
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("log_bin"))
        .map(|(_, v)| v.as_str())
        .unwrap_or("OFF");
    !log_bin.eq_ignore_ascii_case("ON") && log_bin != "1"
}

/// The binlog server config CDC needs; anything else breaks capture quietly (STATEMENT rows never arrive.
fn mysql_binlog_config_verdict(vars: &[(String, String)]) -> DoctorCheck {
    let get = |k: &str| {
        vars.iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(k))
            .map(|(_, v)| v.as_str())
    };
    let name = "CDC binlog server config".to_string();
    let format = get("binlog_format").unwrap_or("?");
    let row_image = get("binlog_row_image").unwrap_or("FULL");
    if binlog_disabled(vars) {
        return check(
            name,
            false,
            Some("log_bin is OFF — there is no binlog to capture".into()),
            Some("enable binary logging (log_bin=ON) and restart".into()),
        );
    }
    if !format.eq_ignore_ascii_case("ROW") {
        return check(
            name,
            false,
            Some(format!(
                "binlog_format={format} — rivet needs row images; STATEMENT/MIXED will not work"
            )),
            Some("SET GLOBAL binlog_format=ROW (and my.cnf for restarts)".into()),
        );
    }
    if !row_image.eq_ignore_ascii_case("FULL") {
        return check(
            name,
            false,
            Some(format!(
                "binlog_row_image={row_image} — the after-image / MERGE shape needs FULL; \
                 MINIMAL drops unchanged columns"
            )),
            Some("SET GLOBAL binlog_row_image=FULL".into()),
        );
    }
    if let Some(why) = crate::source::mysql::cdc::MysqlChangeStream::row_metadata_refusal(get(
        "binlog_row_metadata",
    )) {
        return check(
            name,
            false,
            Some(why),
            Some("SET PERSIST binlog_row_metadata = FULL;".into()),
        );
    }
    check(
        name,
        true,
        Some(
            "log_bin=ON, binlog_format=ROW, binlog_row_image=FULL, binlog_row_metadata=FULL".into(),
        ),
        None,
    )
}

enum MysqlCkpt {
    /// `cdc.checkpoint` not configured at all — every bounded run re-anchors to "now" and changes between runs are skipped.
    NoPathConfigured,
    /// Path configured, file not written yet (no run has happened).
    NotYetWritten,
    Loaded {
        file: String,
        pos: u64,
    },
}

/// Verdict for one export's checkpoint vs the server's retained binlogs.
fn mysql_ckpt_verdict(export: &str, ckpt: MysqlCkpt, logs: &[(String, u64)]) -> DoctorCheck {
    let name = format!("CDC checkpoint (export '{export}')");
    match ckpt {
        MysqlCkpt::NoPathConfigured => check(
            name,
            false,
            Some(
                "no `cdc.checkpoint` configured — MySQL has no server-side anchor, so every \
                 bounded run re-anchors to the current position and changes between runs are \
                 silently skipped"
                    .into(),
            ),
            Some("set cdc.checkpoint to a persistent path".into()),
        ),
        MysqlCkpt::NotYetWritten => check(
            name,
            true,
            Some("no checkpoint yet — the first run pins the open position".into()),
            None,
        ),
        MysqlCkpt::Loaded { file, pos } => {
            let Some(idx) = logs.iter().position(|(f, _)| *f == file) else {
                return check(
                    name,
                    false,
                    Some(format!(
                        "checkpoint {file}:{pos} is below binlog retention (file purged) — the \
                         next run fails with ERROR 1236"
                    )),
                    Some(format!(
                        "{} Size binlog retention above your CDC cadence.",
                        crate::source::cdc::checkpoint_identity::RECOVER
                    )),
                );
            };
            let lag: i64 = (logs[idx].1 as i64 - pos as i64).max(0)
                + logs[idx + 1..].iter().map(|(_, s)| *s as i64).sum::<i64>();
            check(
                name,
                true,
                Some(format!("{file}:{pos}, backlog ≈ {}", mib(lag))),
                None,
            )
        }
    }
}

fn mysql_checks(
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    exports: &[&ExportConfig],
    config_dir: &std::path::Path,
    checks: &mut Vec<DoctorCheck>,
) -> Result<()> {
    use mysql::prelude::Queryable;
    let pool = crate::source::mysql::connect_pool(url, tls)?;
    let mut conn = pool.get_conn()?;

    let vars: Vec<(String, String)> = conn.query(
        "SHOW GLOBAL VARIABLES WHERE Variable_name IN \
         ('log_bin','binlog_format','binlog_row_image','binlog_row_metadata')",
    )?;
    checks.push(mysql_binlog_config_verdict(&vars));
    if binlog_disabled(&vars) {
        return Ok(());
    }

    let logs: Vec<(String, u64)> = conn
        .query_iter("SHOW BINARY LOGS")?
        .filter_map(|r| r.ok())
        .filter_map(|row| {
            let file: Option<String> = row.get(0);
            let size: Option<u64> = row.get(1);
            Some((file?, size?))
        })
        .collect();

    for e in exports {
        let ckpt = match e.cdc.as_ref().and_then(|c| c.checkpoint.as_deref()) {
            None => MysqlCkpt::NoPathConfigured,
            Some(raw) => {
                let p = &crate::source::cdc::resolve_checkpoint(raw, config_dir);
                match crate::source::cdc::Position::load(p)? {
                    None => MysqlCkpt::NotYetWritten,
                    Some(pos) => {
                        match crate::source::mysql::cdc::MysqlChangeStream::resume_from_checkpoint(
                            Some(&pos),
                            &p.display().to_string(),
                        ) {
                            Ok(Some((file, at))) => MysqlCkpt::Loaded { file, pos: at },
                            Ok(None) => MysqlCkpt::NotYetWritten,
                            Err(why) => {
                                checks.push(check(
                                    format!("CDC checkpoint (export '{}')", e.name),
                                    false,
                                    Some(why.to_string()),
                                    Some(restore_or_recover()),
                                ));
                                continue;
                            }
                        }
                    }
                }
            }
        };
        checks.push(mysql_ckpt_verdict(&e.name, ckpt, &logs));
    }
    Ok(())
}

/// Normalise an LSN hex string (`0x…` or bare, any case) to a fixed-width uppercase form so string comparison equals numeric comparison.
fn norm_lsn(s: &str) -> String {
    let h = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    format!("{:0>20}", h.to_ascii_uppercase())
}

struct MssqlHealth {
    cdc_enabled: bool,
    /// The capture instance's min LSN (hex) — `None` ⇒ instance unknown.
    instance_min_lsn: Option<String>,
    /// `None` ⇒ could not verify (no VIEW SERVER STATE).
    agent_running: Option<bool>,
}

fn mssql_verdicts(
    export: &str,
    ci: Option<&str>,
    health: &MssqlHealth,
    ckpt_lsn: Option<Option<String>>,
) -> Vec<DoctorCheck> {
    let mut out = Vec::new();
    if !health.cdc_enabled {
        out.push(check(
            format!("CDC enabled on database (export '{export}')"),
            false,
            Some("sys.fn_cdc_get_max_lsn() is NULL — CDC is not enabled".into()),
            Some(
                "EXEC sys.sp_cdc_enable_db (requires db_owner); Express/Web editions have no CDC"
                    .into(),
            ),
        ));
        return out;
    }
    let Some(ci) = ci else {
        out.push(check(
            format!("CDC capture instance (export '{export}')"),
            false,
            Some("`cdc.capture_instance` is not set — required for sqlserver://".into()),
            Some("set cdc.capture_instance (e.g. dbo_orders) to the instance created by sp_cdc_enable_table".into()),
        ));
        return out;
    };
    match &health.instance_min_lsn {
        None => out.push(check(
            format!("CDC capture instance '{ci}' (export '{export}')"),
            false,
            Some("fn_cdc_get_min_lsn returned NULL — the capture instance does not exist".into()),
            Some("EXEC sys.sp_cdc_enable_table @capture_instance=… for the table".into()),
        )),
        Some(min) => {
            out.push(check(
                format!("CDC capture instance '{ci}' (export '{export}')"),
                true,
                None,
                None,
            ));
            match ckpt_lsn {
                None => out.push(check(
                    format!("CDC checkpoint (export '{export}')"),
                    false,
                    Some(
                        "no `cdc.checkpoint` configured — every run re-reads the full retained \
                         change window (duplicates on every cycle, no resume)"
                            .into(),
                    ),
                    Some("set cdc.checkpoint to a persistent path".into()),
                )),
                Some(None) => out.push(check(
                    format!("CDC checkpoint (export '{export}')"),
                    true,
                    Some("no checkpoint yet — the first run starts at the retained minimum".into()),
                    None,
                )),
                Some(Some(ckpt)) => {
                    if norm_lsn(&ckpt) < norm_lsn(min) {
                        out.push(check(
                            format!("CDC checkpoint (export '{export}')"),
                            false,
                            Some(format!(
                                "checkpoint LSN {ckpt} is below the retained minimum {min} — the \
                                 cleanup job removed changes past it; the next run fails loudly"
                            )),
                            Some(crate::source::cdc::checkpoint_identity::RECOVER.into()),
                        ));
                    } else {
                        out.push(check(
                            format!("CDC checkpoint (export '{export}')"),
                            true,
                            Some(format!("LSN {ckpt} within retention")),
                            None,
                        ));
                    }
                }
            }
        }
    }
    out.push(match health.agent_running {
        Some(true) => check("CDC Agent (SQL Server Agent)".into(), true, None, None),
        Some(false) => check(
            "CDC Agent (SQL Server Agent)".into(),
            false,
            Some(
                "the Agent service is not running — change tables are frozen and the \
                 transaction log cannot truncate"
                    .into(),
            ),
            Some("start the SQL Server Agent service (the capture job lives there)".into()),
        ),
        None => check(
            "CDC Agent (SQL Server Agent)".into(),
            true,
            Some(
                "could not verify (needs VIEW SERVER STATE) — watch for a non-advancing max LSN"
                    .into(),
            ),
            None,
        ),
    });
    out
}

fn mssql_checks(
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    exports: &[&ExportConfig],
    config_dir: &std::path::Path,
    checks: &mut Vec<DoctorCheck>,
) -> Result<()> {
    let mut src = crate::source::mssql::MssqlSource::connect_with_tls(url, tls)?;
    for e in exports {
        let ci = e.cdc.as_ref().and_then(|c| c.capture_instance.as_deref());
        let health = src.cdc_health(ci)?;
        let mut ckpt_error: Option<String> = None;
        let ckpt_state = match e.cdc.as_ref().and_then(|c| c.checkpoint.as_deref()) {
            None => None,
            Some(raw) => {
                let p = &crate::source::cdc::resolve_checkpoint(raw, config_dir);
                let pos = crate::source::cdc::Position::load(p)?;
                match crate::source::mssql::cdc::resume_from_checkpoint(
                    pos.as_ref(),
                    &p.display().to_string(),
                ) {
                    Ok(r) => Some(r.from_lsn),
                    Err(e) => {
                        ckpt_error = Some(e.to_string());
                        Some(None)
                    }
                }
            }
        };
        let mssql_health = MssqlHealth {
            cdc_enabled: health.cdc_enabled,
            instance_min_lsn: health.instance_min_lsn,
            agent_running: health.agent_running,
        };
        if let Some(why) = ckpt_error {
            checks.push(check(
                format!("CDC checkpoint (export '{}')", e.name),
                false,
                Some(why),
                Some(restore_or_recover()),
            ));
        }
        checks.extend(mssql_verdicts(&e.name, ci, &mssql_health, ckpt_state));
    }
    Ok(())
}

/// Change-stream health: the replica-set requirement (a standalone cannot `watch()`) and the DECLARED capture-fidelity tier.
#[cfg(feature = "oracle")]
fn oracle_checks(
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    exports: &[&ExportConfig],
    config_dir: &std::path::Path,
    checks: &mut Vec<DoctorCheck>,
) -> Result<()> {
    for e in exports {
        let Some(raw) = e.cdc.as_ref().and_then(|c| c.checkpoint.as_deref()) else {
            continue;
        };
        let path = crate::source::cdc::resolve_checkpoint(raw, config_dir);
        let name = format!("CDC checkpoint (export '{}')", e.name);
        let problem = match crate::source::cdc::Position::load(&path) {
            Ok(None) => None,
            Ok(Some(pos)) => {
                crate::source::oracle::cdc::checkpoint_problem(&pos, &path.display().to_string())
            }
            Err(why) => Some(why.to_string()),
        };
        let ok = problem.is_none();
        checks.push(check(
            name,
            ok,
            problem.or(Some("readable (or not written yet)".into())),
            (!ok).then(restore_or_recover),
        ));
    }
    let tables: Vec<String> = exports
        .iter()
        .flat_map(|e| e.table.iter().chain(e.tables.iter().flatten()).cloned())
        .collect();
    for (name, verdict) in crate::source::oracle::cdc::prerequisites(url, tls, &tables)? {
        let ok = verdict.is_ok();
        let detail = verdict.unwrap_or_else(|problem| problem);
        checks.push(check(
            name.into(),
            ok,
            Some(detail),
            (!ok).then(|| "see the 'Oracle — LogMiner' section of docs/reference/cdc.md".into()),
        ));
    }
    Ok(())
}

/// The replica-set requirement and the declared capture-fidelity tier of a probed server.
fn mongo_capability_verdicts(
    cap: &crate::source::mongo::cdc::MongoCdcCapability,
) -> Vec<DoctorCheck> {
    let replica_set = check(
        "CDC replica set".into(),
        cap.is_replica_set,
        Some(if cap.is_replica_set {
            format!("replica set (server {})", cap.server_version)
        } else {
            format!(
                "server {} is standalone — change streams unavailable",
                cap.server_version
            )
        }),
        (!cap.is_replica_set).then(|| {
            "MongoDB change streams require a replica set (a single-node one is fine): restart \
                 mongod with --replSet and run rs.initiate()"
                .to_string()
        }),
    );
    let tier = check(
        "CDC capture tier".into(),
        true,
        Some(cap.tier().to_string()),
        (cap.major < 6).then(|| {
            "upgrade to MongoDB 6.0+ and enable changeStreamPreAndPostImages for point-in-time \
                 post-images and delete pre-images"
                .to_string()
        }),
    );
    vec![replica_set, tier]
}

fn mongo_checks(
    config: &Config,
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    exports: &[&ExportConfig],
    config_dir: &std::path::Path,
    checks: &mut Vec<DoctorCheck>,
) -> Result<()> {
    let cap = crate::source::mongo::cdc::probe_capability(url, tls)?;

    for e in exports {
        let Some(raw) = e.cdc.as_ref().and_then(|c| c.checkpoint.as_deref()) else {
            continue;
        };
        let path = &crate::source::cdc::resolve_checkpoint(raw, config_dir);
        let name = format!("CDC checkpoint (export '{}')", e.name);
        match crate::source::cdc::Position::load(path) {
            Ok(None) => checks.push(check(
                name,
                true,
                Some("not written yet — the first run pins its own anchor".into()),
                None,
            )),
            Ok(Some(pos)) => match crate::source::mongo::cdc::decode_resume_token(&pos.0) {
                Ok(_) => {
                    let seen = crate::source::mongo::id_types::IdTypes::from_checkpoint(&pos.0);
                    let mixed = crate::source::mongo::id_types::keyed_tables(config, e)
                        .into_iter()
                        .find_map(|t| seen.refuse_if_mixed(&t).err());
                    match mixed {
                        None => checks.push(check(
                            name,
                            true,
                            Some("resume token readable".into()),
                            None,
                        )),
                        Some(why) => checks.push(check(
                            name,
                            false,
                            Some(format!("{why:#} — the run refuses this export")),
                            Some(crate::error::codes::SOURCE_ID_TYPES_MIXED.action.into()),
                        )),
                    }
                }
                Err(why) => checks.push(check(
                    name,
                    false,
                    Some(format!("{why} — the run refuses this file")),
                    Some(restore_or_recover()),
                )),
            },
            Err(why) => checks.push(check(
                name,
                false,
                Some(why.to_string()),
                Some(restore_or_recover()),
            )),
        }
    }
    checks.extend(mongo_capability_verdicts(&cap));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A standalone fails with the replica-set remedy; the tier hints an upgrade only below 6.0.
    #[test]
    fn mongo_capability_verdicts_fail_a_standalone_and_hint_only_below_six() {
        let cap = |is_replica_set, major| crate::source::mongo::cdc::MongoCdcCapability {
            is_replica_set,
            server_version: format!("{major}.0.1"),
            major,
        };
        let v = mongo_capability_verdicts(&cap(true, 6));
        assert_eq!(v.len(), 2);
        assert_eq!((v[0].name.as_str(), v[0].ok), ("CDC replica set", true));
        assert_eq!(v[0].detail.as_deref(), Some("replica set (server 6.0.1)"));
        assert_eq!(v[0].hint, None);
        assert_eq!((v[1].name.as_str(), v[1].ok), ("CDC capture tier", true));
        assert_eq!(v[1].detail.as_deref(), Some(cap(true, 6).tier()));
        assert_eq!(v[1].hint, None, "6.0 is the full tier: no upgrade hint");
        assert_eq!(mongo_capability_verdicts(&cap(true, 7))[1].hint, None);

        let v = mongo_capability_verdicts(&cap(false, 5));
        assert!(!v[0].ok, "a standalone cannot open a change stream");
        assert_eq!(
            v[0].detail.as_deref(),
            Some("server 5.0.1 is standalone — change streams unavailable")
        );
        assert!(
            v[0].hint
                .as_deref()
                .is_some_and(|h| h.contains("--replSet"))
        );
        assert!(v[1].ok, "the tier is informational");
        assert!(
            v[1].hint
                .as_deref()
                .is_some_and(|h| h.contains("upgrade to MongoDB 6.0+"))
        );
    }

    /// `collect` is the doctor's CDC entry point.
    #[test]
    fn collect_reports_a_probe_that_cannot_start_and_nothing_for_a_batch_config() {
        let cfg = |mode: &str| {
            Config::from_yaml(&format!(
                "source:\n  type: postgres\n  url_env: RIVET_DOCTOR_TEST_UNSET_URL\nexports:\n\
                 \x20 - name: t\n    table: t\n    mode: {mode}\n    format: parquet\n\
                 \x20   {}destination: {{ type: local, path: ./out }}\n",
                if mode == "cdc" {
                    "cdc: { checkpoint: ./t.ckpt }\n    "
                } else {
                    ""
                }
            ))
            .expect("a config")
        };
        let dir = std::path::Path::new(".");
        assert!(collect(&cfg("full"), dir).0.is_empty());
        let checks = collect(&cfg("cdc"), dir).0;
        assert_eq!(checks.len(), 1, "{checks:?}");
        assert!(
            !checks[0].ok && checks[0].name == "CDC health probe",
            "{checks:?}"
        );
    }

    /// Doctor fails an export whose prerequisites the run's own function refuses, and adds nothing where it asks nothing.
    #[test]
    fn a_prerequisite_the_run_refuses_is_a_failed_check_named_for_its_export() {
        let cfg = |source: &str| {
            Config::from_yaml(&format!(
                "source:\n  {source}\nexports:\n  - name: t\n    table: t\n    mode: cdc\n    \
                 format: parquet\n    cdc: {{ checkpoint: ./t.ckpt, capture_instance: dbo_t }}\n    \
                 destination: {{ type: local, path: ./out }}\n"
            ))
            .expect("a config")
        };
        let run = |c: &Config| {
            let exports: Vec<&ExportConfig> = c.exports.iter().collect();
            run_refusals(c, &exports, c.source.resolve_url().unwrap().expose())
        };
        for source in [
            "type: postgres\n  url: postgresql://rivet:rivet@127.0.0.1:1/rivet",
            "type: mysql\n  url: mysql://rivet:rivet@127.0.0.1:1/rivet",
        ] {
            let refused = run(&cfg(source));
            assert_eq!(refused.len(), 1, "{refused:?}");
            assert!(!refused[0].ok, "{refused:?}");
            assert_eq!(refused[0].name, "CDC run prerequisites (export 't')");
            assert!(
                refused[0]
                    .detail
                    .as_deref()
                    .is_some_and(|d| d.contains("127.0.0.1:1")),
                "{source}: {refused:?}"
            );
            assert!(refused[0].hint.is_none(), "{refused:?}");
        }
        assert!(
            run(&cfg(
                "type: mssql\n  url: sqlserver://sa:x@127.0.0.1:1/rivet"
            ))
            .is_empty()
        );
    }

    #[test]
    fn pg_absent_slot_with_no_checkpoint_is_healthy_created_on_first_run() {
        let c = pg_slot_verdict("orders", "rivet_orders", None, false);
        assert!(c.ok);
        assert!(c.detail.unwrap().contains("first run"));
    }

    /// The case the old signature could not express, and the reason it was added.
    #[test]
    fn pg_absent_slot_with_a_resume_checkpoint_fails_and_names_the_recovery_order() {
        let c = pg_slot_verdict("orders", "rivet_orders", None, true);
        assert!(
            !c.ok,
            "a dropped slot under an existing checkpoint is not a first run: {c:?}"
        );
        assert_eq!(
            c.detail,
            Some(format!(
                "pg cdc: slot 'rivet_orders' is missing but the checkpoint file holds a position \
                 from a prior run — the slot was dropped or invalidated, and the changes since \
                 then are no longer in the log. {}",
                crate::source::cdc::checkpoint_identity::RECOVER
            )),
            "doctor checks the checkpoint only, so it must name the checkpoint and nothing else"
        );
    }

    #[test]
    fn pg_small_retained_wal_is_healthy_large_fails_with_drop_hint() {
        let ok = pg_slot_verdict(
            "orders",
            "s",
            Some(PgSlot {
                active: false,
                retained_bytes: 10 << 20,
            }),
            false,
        );
        assert!(ok.ok, "10 MiB retained is healthy");

        let bad = pg_slot_verdict(
            "orders",
            "s",
            Some(PgSlot {
                active: false,
                retained_bytes: 2 << 30,
            }),
            false,
        );
        assert!(!bad.ok, "2 GiB retained fails");
        assert!(bad.hint.unwrap().contains("pg_drop_replication_slot"));
    }

    #[test]
    fn pg_foreign_inactive_slots_fail_only_when_pinning_wal() {
        assert!(pg_foreign_slots_verdict(&[]).ok);
        let small =
            pg_foreign_slots_verdict(&[("ingestr_leftover".into(), 1 << 20, "logical".into())]);
        assert!(small.ok, "a small inactive slot is a note, not a failure");
        let d = small.detail.unwrap();
        assert!(
            d.contains("ingestr_leftover") && d.contains("1 slot(s)"),
            "the healthy note carries BOTH — the name, because a lingering slot is \
             worth knowing about while it is still small, and the total, because \
             reporting only the max is what let 1.96 GiB across eight read as OK: {d}"
        );
        let big =
            pg_foreign_slots_verdict(&[("ingestr_leftover".into(), 5 << 30, "logical".into())]);
        assert!(!big.ok, "an abandoned slot pinning GiBs fails");
        assert!(big.detail.unwrap().contains("ingestr_leftover"));
    }

    /// The hazard is a FILLING DISK, and a disk is filled by the total.
    #[test]
    fn a_fleet_of_sub_threshold_slots_still_fills_the_disk() {
        let third = PG_RETAINED_WAL_FAIL_BYTES / 3;
        let fleet: Vec<(String, i64, String)> = (0..8)
            .map(|i| (format!("orphan_{i}"), third, "logical".to_string()))
            .collect();
        assert!(
            fleet
                .iter()
                .all(|(_, b, _)| *b < PG_RETAINED_WAL_FAIL_BYTES),
            "the fixture must be under the bar per-slot or it proves nothing about the sum"
        );
        let w = pg_foreign_slots_warning(&fleet, false)
            .expect("8 slots at a third of the bar is 2.6x the bar in total");
        assert!(
            w.contains("8 slot(s)"),
            "say how many, or an operator cannot tell an aggregate finding from a \
             single fat slot: {w}"
        );
    }

    /// A PHYSICAL slot belongs to a standby.
    #[test]
    fn a_physical_slot_is_reported_but_never_the_one_the_drop_hint_names() {
        let w = pg_foreign_slots_warning(
            &[
                (
                    "standby_1".into(),
                    PG_RETAINED_WAL_FAIL_BYTES * 4,
                    "physical".into(),
                ),
                (
                    "dead_logical".into(),
                    PG_RETAINED_WAL_FAIL_BYTES,
                    "logical".into(),
                ),
            ],
            false,
        )
        .expect("both are past the bar");
        assert!(
            w.contains("standby_1") && w.contains("PHYSICAL"),
            "report it — it really is pinning WAL: {w}"
        );
        assert!(
            w.contains("pg_drop_replication_slot('dead_logical')"),
            "but the ready-to-paste command must name the LOGICAL one, even though the \
             physical slot is the biggest: {w}"
        );
    }

    /// A `run` sees ONE export, so a sibling export's slot is indistinguishable from an abandoned one.
    #[test]
    fn the_run_path_reports_the_wal_but_never_the_verdict() {
        let one = [(
            "someone_elses".to_string(),
            PG_RETAINED_WAL_FAIL_BYTES,
            "logical".to_string(),
        )];
        let from_run = pg_foreign_slots_warning(&one, true).expect("past the bar");
        assert!(
            !from_run.contains("pg_drop_replication_slot"),
            "a run must not hand over a command that can destroy a sibling export's \
             resume anchor: {from_run}"
        );
        assert!(
            from_run.contains("rivet doctor"),
            "...it must point at the tool that CAN answer the ownership question: \
             {from_run}"
        );
        let from_doctor = pg_foreign_slots_warning(&one, false).expect("past the bar");
        assert!(
            from_doctor.contains("pg_drop_replication_slot"),
            "doctor was handed the whole config and excluded every slot in it, so it \
             may say so: {from_doctor}"
        );
    }

    fn vars(format: &str, image: &str) -> Vec<(String, String)> {
        vec![
            ("log_bin".into(), "ON".into()),
            ("binlog_format".into(), format.into()),
            ("binlog_row_image".into(), image.into()),
            ("binlog_row_metadata".into(), "FULL".into()),
        ]
    }

    /// `log_bin=OFF` must be RECOGNISED as the skip condition, in both spellings the server uses.
    #[test]
    fn binlog_off_is_detected_in_both_spellings_and_named_in_the_verdict() {
        let off = |v: &str| {
            vec![
                ("log_bin".to_string(), v.to_string()),
                ("binlog_format".to_string(), "ROW".to_string()),
                ("binlog_row_image".to_string(), "FULL".to_string()),
            ]
        };
        assert!(binlog_disabled(&off("OFF")), "`OFF` is binlog disabled");
        assert!(binlog_disabled(&off("0")), "8.x renders @@log_bin as 0/1");
        assert!(!binlog_disabled(&off("ON")), "`ON` must probe retention");
        assert!(!binlog_disabled(&off("1")), "`1` must probe retention");
        assert!(
            binlog_disabled(&[]),
            "a server that did not report the variable is not provably logging"
        );
        let verdict = mysql_binlog_config_verdict(&off("0"));
        assert!(!verdict.ok);
        assert!(
            verdict.detail.unwrap().contains("log_bin is OFF"),
            "the failing verdict must name the cause, not the format/image checks below it"
        );
    }

    #[test]
    fn mysql_binlog_config_requires_row_and_full() {
        assert!(mysql_binlog_config_verdict(&vars("ROW", "FULL")).ok);
        let stmt = mysql_binlog_config_verdict(&vars("STATEMENT", "FULL"));
        assert!(!stmt.ok);
        assert!(stmt.detail.unwrap().contains("STATEMENT"));
        let minimal = mysql_binlog_config_verdict(&vars("ROW", "MINIMAL"));
        assert!(!minimal.ok, "MINIMAL breaks the after-image / MERGE shape");
    }

    /// `binlog_row_metadata` other than FULL (or absent) fails the doctor, as it refuses the run.
    #[test]
    fn mysql_binlog_config_fails_on_row_metadata_other_than_full() {
        let with_meta = |m: Option<&str>| {
            let mut v = vars("ROW", "FULL");
            v.retain(|(n, _)| n != "binlog_row_metadata");
            v.extend(m.map(|m| ("binlog_row_metadata".to_string(), m.to_string())));
            mysql_binlog_config_verdict(&v)
        };
        assert!(with_meta(Some("FULL")).ok);
        for m in [Some("MINIMAL"), None] {
            let verdict = with_meta(m);
            assert!(!verdict.ok, "{m:?} must fail the doctor");
            assert!(verdict.detail.unwrap().contains("binlog_row_metadata"));
            assert_eq!(
                verdict.hint.as_deref(),
                Some("SET PERSIST binlog_row_metadata = FULL;")
            );
        }
    }

    #[test]
    fn mysql_missing_checkpoint_config_fails_purged_file_fails_with_1236() {
        let logs = vec![
            ("binlog.000003".to_string(), 1000u64),
            ("binlog.000004".to_string(), 500u64),
        ];
        let none = mysql_ckpt_verdict("orders", MysqlCkpt::NoPathConfigured, &logs);
        assert!(
            !none.ok,
            "a config-driven cdc export without a checkpoint skips changes between runs"
        );

        let fresh = mysql_ckpt_verdict("orders", MysqlCkpt::NotYetWritten, &logs);
        assert!(fresh.ok);

        let purged = mysql_ckpt_verdict(
            "orders",
            MysqlCkpt::Loaded {
                file: "binlog.000001".into(),
                pos: 4,
            },
            &logs,
        );
        assert!(!purged.ok);
        assert!(purged.detail.unwrap().contains("1236"));
    }

    #[test]
    fn mysql_backlog_sums_remainder_of_ckpt_file_plus_later_files() {
        let logs = vec![
            ("binlog.000003".to_string(), 1000u64),
            ("binlog.000004".to_string(), 500u64),
        ];
        let c = mysql_ckpt_verdict(
            "orders",
            MysqlCkpt::Loaded {
                file: "binlog.000003".into(),
                pos: 400,
            },
            &logs,
        );
        assert!(c.ok);
        assert!(c.detail.unwrap().contains("0.0 MiB"));
    }

    fn healthy() -> MssqlHealth {
        MssqlHealth {
            cdc_enabled: true,
            instance_min_lsn: Some("0x00000028000009F00005".into()),
            agent_running: Some(true),
        }
    }

    #[test]
    fn mssql_cdc_disabled_fails_with_enable_hint() {
        let h = MssqlHealth {
            cdc_enabled: false,
            instance_min_lsn: None,
            agent_running: None,
        };
        let out = mssql_verdicts("orders", Some("dbo_orders"), &h, None);
        assert_eq!(out.len(), 1);
        assert!(!out[0].ok);
        assert!(out[0].hint.as_ref().unwrap().contains("sp_cdc_enable_db"));
    }

    #[test]
    fn mssql_ckpt_below_retention_fails_within_retention_ok() {
        let h = healthy();
        let below = mssql_verdicts(
            "orders",
            Some("dbo_orders"),
            &h,
            Some(Some("0x00000010000000010001".into())),
        );
        let ckpt = below
            .iter()
            .find(|c| c.name.contains("checkpoint"))
            .unwrap();
        assert!(!ckpt.ok, "LSN below min must fail");

        let above = mssql_verdicts(
            "orders",
            Some("dbo_orders"),
            &h,
            Some(Some("0x00000030000000010001".into())),
        );
        let ckpt = above
            .iter()
            .find(|c| c.name.contains("checkpoint"))
            .unwrap();
        assert!(ckpt.ok, "LSN above min is healthy");
    }

    #[test]
    fn mssql_agent_stopped_fails_unknown_is_a_note() {
        let mut h = healthy();
        h.agent_running = Some(false);
        let out = mssql_verdicts("orders", Some("dbo_orders"), &h, Some(None));
        let agent = out.iter().find(|c| c.name.contains("Agent")).unwrap();
        assert!(!agent.ok);

        h.agent_running = None;
        let out = mssql_verdicts("orders", Some("dbo_orders"), &h, Some(None));
        let agent = out.iter().find(|c| c.name.contains("Agent")).unwrap();
        assert!(agent.ok, "unverifiable Agent is a note, not a failure");
        assert!(agent.detail.as_ref().unwrap().contains("VIEW SERVER STATE"));
    }

    #[test]
    fn norm_lsn_compares_across_prefix_and_case() {
        assert!(norm_lsn("0x0000001000000001") < norm_lsn("00000028000009f0"));
        assert_eq!(norm_lsn("0xABC"), norm_lsn("abc"));
    }
}

#[cfg(test)]
mod slot_retention_warning_tests {
    use super::*;

    /// The threshold has ONE definition, and both callers must sit on the same side of it.
    #[test]
    fn an_ordinary_backlog_is_silent_and_a_pinned_slot_names_its_escape() {
        assert_eq!(pg_retained_wal_warning("s", 0, true), None);
        assert_eq!(
            pg_retained_wal_warning("s", PG_RETAINED_WAL_FAIL_BYTES - 1, false),
            None,
            "one byte under the bar must stay silent — the boundary is the whole \
             contract, and an off-by-one here makes the warning fire on every run"
        );

        let w = pg_retained_wal_warning("mine", PG_RETAINED_WAL_FAIL_BYTES, true)
            .expect("at the bar the operator must be told");
        assert!(w.contains("mine"), "name the slot: {w}");
        assert!(
            w.contains("pg_drop_replication_slot") && w.contains("max_slot_wal_keep_size"),
            "name what to DO — a warning without an escape is noise: {w}"
        );
        assert!(
            w.contains("This run will drain it"),
            "and be honest that the run itself fixes this case, or an operator drops a \
             slot that was about to be drained: {w}"
        );
    }

    /// The foreign slots are the dangerous half: nobody is draining them, so their WAL is pinned until a human acts.
    #[test]
    fn foreign_slots_warn_past_the_bar_and_report_the_aggregate() {
        assert_eq!(pg_foreign_slots_warning(&[], false), None);
        assert_eq!(
            pg_foreign_slots_warning(&[("small".into(), 1024, "logical".into())], false),
            None,
            "an inactive slot holding a KiB is not a hazard; warning about it teaches \
             operators to ignore the message"
        );

        assert!(
            pg_foreign_slots_warning(
                &[(
                    "at_the_bar".into(),
                    PG_RETAINED_WAL_FAIL_BYTES,
                    "logical".into()
                )],
                false
            )
            .is_some(),
            "a slot sitting exactly ON the bar must be reported — `<=` here would let \
             the single most common boundary value through in silence"
        );
        assert_eq!(
            pg_foreign_slots_warning(
                &[(
                    "under".into(),
                    PG_RETAINED_WAL_FAIL_BYTES - 1,
                    "logical".into()
                )],
                false
            ),
            None,
            "...and one byte under it must not be"
        );

        let phys = pg_foreign_slots_warning(
            &[(
                "standby".into(),
                PG_RETAINED_WAL_FAIL_BYTES + 1,
                "physical".into(),
            )],
            false,
        )
        .expect("past the bar");
        assert!(
            phys.contains("PHYSICAL — a standby's"),
            "a physical slot must be labelled — the drop hint must never name a \
             standby's slot as a CDC leftover: {phys}"
        );
        let logi = pg_foreign_slots_warning(
            &[(
                "leftover".into(),
                PG_RETAINED_WAL_FAIL_BYTES + 1,
                "logical".into(),
            )],
            false,
        )
        .expect("past the bar");
        assert!(
            !logi.contains("PHYSICAL — a standby's"),
            "...and a logical one must NOT be. Asserted on the per-slot LABEL, not on \
             the word: the message's closing caveat says `never for a slot marked \
             PHYSICAL` regardless, so a bare substring check passes on every input \
             and grades nothing: {logi}"
        );

        let many = |n: usize| -> String {
            let slots: Vec<(String, i64, String)> = (0..n)
                .map(|i| {
                    (
                        format!("s{i}"),
                        PG_RETAINED_WAL_FAIL_BYTES + 1 + i as i64,
                        "logical".to_string(),
                    )
                })
                .collect();
            pg_foreign_slots_warning(&slots, false).expect("past the bar")
        };
        assert!(
            !many(10).contains("more"),
            "exactly ten slots all fit in the listing — a tail here would claim \
             something was hidden when nothing was: {}",
            many(10)
        );
        assert!(
            many(11).contains("and 1 more"),
            "and the eleventh must be counted, or the operator reads a truncated \
             listing as the whole set: {}",
            many(11)
        );

        let big = PG_RETAINED_WAL_FAIL_BYTES + 1;
        let w = pg_foreign_slots_warning(
            &[
                ("small".into(), 4096, "logical".into()),
                ("orphan_a".into(), big, "logical".into()),
                ("orphan_b".into(), big * 2, "logical".into()),
            ],
            false,
        )
        .expect("two slots past the bar must be reported");
        assert!(
            w.contains("orphan_a") && w.contains("orphan_b"),
            "list every offender, not just the worst — an operator fixing one and \
             re-running should not have to discover the rest one at a time: {w}"
        );
        assert!(
            w.contains("small"),
            "every contributor is listed — the total is what matters: {w}"
        );
        assert!(
            w.contains("pg_drop_replication_slot('orphan_b')"),
            "the ready-to-paste command should name the WORST one: {w}"
        );
        assert!(
            w.contains("none belongs to this config"),
            "doctor's form may state ownership, which is what makes its drop hint \
             safe to offer: {w}"
        );
    }
}
