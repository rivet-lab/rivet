//! `rivet check --type-report` — tabular and JSON output.

use serde::Serialize;

use crate::config::{Config, ExportConfig, FormatType, SourceType};
use crate::error::Result;
use crate::source;
use crate::types::narrowing::downgrade_narrowing_overrides;
use crate::types::{
    ColumnOverrides, TypeFidelity,
    policy::{PolicyAction, PolicyViolation, TypePolicy},
    target::{ExportTarget, TargetInput, TargetStatus, TargetType},
};

/// One row in the type report (and the JSON output — roadmap §9).
#[derive(Serialize)]
pub struct TypeReportRow {
    pub column: String,
    pub source_type: String,
    pub rivet_type: String,
    pub arrow_type: String,
    pub fidelity: TypeFidelity,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Present when `--target` is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_type: Option<TargetType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_status: Option<TargetStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_note: Option<String>,
    /// Type a generic Parquet reader infers without a declared schema, surfaced only when it diverges from `target_type` (e.g.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub autoload_type: Option<TargetType>,
    /// Materialization / load-schema hint (L5) to recover the native type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cast_sql: Option<String>,
    /// How the column lands against what a load key or order needs of it.
    #[serde(skip)]
    pub key_landing: crate::preflight::key_admission::KeyLanding,
}

/// One export's type-report data — or, for a multiplex `tables:` CDC export, ONE TABLE'S (see [`collect_reports`]).
#[derive(Serialize)]
pub struct ExportTypeReport {
    pub export: String,
    /// The source table this report describes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    pub columns: Vec<TypeReportRow>,
    pub violations: Vec<PolicyViolation>,
    /// True when any column failed target-compatibility.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub target_failures: bool,
    /// Target-native recovery SQL (ADR-0014 L5): a post-load transform that recovers types bare autoload degrades.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_sql: Option<String>,
}

impl ExportTypeReport {
    pub fn has_fatal(&self) -> bool {
        self.violations.iter().any(|v| v.fatal)
    }

    pub fn has_refused(&self) -> bool {
        self.violations.iter().any(|v| v.refused)
    }

    pub fn has_target_fail(&self) -> bool {
        self.target_failures
    }

    /// How this report addresses itself in operator-facing output.
    pub fn display_name(&self) -> String {
        match &self.table {
            Some(t) => format!("{}/{}", self.export, t),
            None => self.export.clone(),
        }
    }
}

/// The `(table, query)` units one export resolves to for type resolution.
fn report_units(
    export: &ExportConfig,
    config_dir: &std::path::Path,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Result<Vec<(Option<String>, String)>> {
    match export.multiplex_tables() {
        Some(tables) => tables
            .iter()
            .map(|t| {
                crate::source::cdc::validate_table_ident(t)?;
                Ok((Some(t.clone()), format!("SELECT * FROM {t}")))
            })
            .collect(),
        None => Ok(vec![(None, export.resolve_query(config_dir, params)?)]),
    }
}

/// Refuses an export whose `columns:` overrides the run's converter refuses on the first row of a unit.
pub(crate) fn refuse_override_wire_mismatch(
    config: &Config,
    export: &ExportConfig,
    column_overrides: &ColumnOverrides,
    config_dir: &std::path::Path,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Result<()> {
    if column_overrides.is_empty() {
        return Ok(());
    }
    let url = config.source.resolve_url()?;
    let url = url.expose();
    let mut src = connect_source(config, url, config.source.tls.as_ref())?;
    for (table, query) in report_units(export, config_dir, params)? {
        let overrides = unit_overrides(export, table.as_deref(), column_overrides);
        if is_stream(export) {
            source::cdc::stream_override_refusal(src.as_mut(), &query, &overrides)?;
            continue;
        }
        source::refuse_override_wire_mismatch(
            src.as_mut(),
            config.source.source_type,
            &query,
            &overrides,
        )?;
    }
    Ok(())
}

/// Whether the export's types are the CDC change-event builder's, not the batch reader's.
pub(crate) fn is_stream(export: &ExportConfig) -> bool {
    export.mode == crate::config::ExportMode::Cdc
}

/// The `columns:` overrides that apply to ONE resolver unit.
fn unit_overrides(
    export: &ExportConfig,
    table: Option<&str>,
    all: &ColumnOverrides,
) -> ColumnOverrides {
    crate::types::overrides_for_unit(all, table.or(export.table.as_deref()))
}

/// What to do when a CDC capture instance cannot be resolved to its relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnUnresolvedCapture {
    /// Type the configured relation and warn — the preflight's stance.
    Degrade,
    /// Refuse: a wrong warehouse schema is worse than no answer.
    Fail,
}

/// Where a report is being collected FROM, and on whose behalf.
pub struct ReportContext<'a> {
    pub target: Option<ExportTarget>,
    pub config_dir: &'a std::path::Path,
    pub params: Option<&'a std::collections::HashMap<String, String>>,
    pub on_unresolved: OnUnresolvedCapture,
}

/// Why a capture instance did not resolve — the input to WHICH repair to name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unresolved {
    Resolved,
    /// The catalog was readable and holds no row for this instance.
    NoRow,
    /// The catalog itself could not be read.
    Unreadable,
}

/// Can the configured `table:` name its relation WITHOUT the catalog?
pub(crate) fn degrade_is_unambiguous(configured: Option<&str>) -> bool {
    configured.is_some_and(|t| t.contains('.'))
}

/// Must an unresolvable capture instance REFUSE, rather than degrade?
pub(crate) fn unresolved_capture_is_fatal(
    on_unresolved: OnUnresolvedCapture,
    configured: Option<&str>,
) -> bool {
    on_unresolved == OnUnresolvedCapture::Fail && !degrade_is_unambiguous(configured)
}

/// Collect type mappings for one export from a live connection.
pub fn collect_reports(
    config: &Config,
    export: &ExportConfig,
    column_overrides: &ColumnOverrides,
    policy: &TypePolicy,
    ctx: ReportContext<'_>,
) -> Result<Vec<ExportTypeReport>> {
    let ReportContext {
        target,
        config_dir,
        params,
        on_unresolved,
    } = ctx;
    let url = config.source.resolve_url()?;
    let url = url.expose();
    let tls = config.source.tls.as_ref();
    let units = resolved_units(config, export, url, tls, config_dir, params, on_unresolved)?;
    let mut src = connect_source(config, url, tls)?;

    units
        .into_iter()
        .map(|unit| {
            let overrides = unit_overrides(export, unit.table.as_deref(), column_overrides);
            collect_one(
                src.as_mut(),
                export,
                unit.table,
                &unit.query,
                &overrides,
                policy,
                target,
            )
        })
        .collect()
}

/// One resolver unit: the report's table, its probe query, and the relation that holds its primary key.
struct ReportUnit {
    table: Option<String>,
    query: String,
    relation: Option<String>,
}

/// The readable columns of each named table, as the source's own `readable_columns` answers them.
fn readable_through_source(
    src: &mut dyn crate::source::Source,
    tables: &[String],
) -> Result<std::collections::HashMap<String, crate::source::cdc::ReadableColumns>> {
    let mut by_table = std::collections::HashMap::new();
    for t in tables {
        if let Some(r) = src.readable_columns(t)? {
            by_table.insert(t.clone(), r);
        }
    }
    Ok(by_table)
}

/// The readable columns of each named table for a `columns_from: grants` source.
fn readable_by_table(
    source_type: SourceType,
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    tables: &[String],
) -> Result<std::collections::HashMap<String, crate::source::cdc::ReadableColumns>> {
    match source_type {
        SourceType::Mysql => {
            crate::source::mysql::cdc::MysqlChangeStream::readable_columns_at(url, tls, tables)
        }
        SourceType::Mssql => {
            let mut src = crate::source::mssql::MssqlSource::connect_with_tls(url, tls)?;
            readable_through_source(&mut src, tables)
        }
        #[cfg(feature = "oracle")]
        SourceType::Oracle => {
            let mut src = crate::source::oracle::OracleSource::connect_with_tls(url, tls)?;
            readable_through_source(&mut src, tables)
        }
        SourceType::Postgres => {
            let mut client = crate::source::postgres::connect_client(url, tls)?;
            crate::source::postgres::column_grants::readable_columns_of(&mut client, tables)
        }
        _ => Ok(std::collections::HashMap::new()),
    }
}

/// The export's resolver units, a SQL Server capture instance resolved to the relation it captures.
fn resolved_units(
    config: &Config,
    export: &ExportConfig,
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    config_dir: &std::path::Path,
    params: Option<&std::collections::HashMap<String, String>>,
    on_unresolved: OnUnresolvedCapture,
) -> Result<Vec<ReportUnit>> {
    let mut units: Vec<ReportUnit> = report_units(export, config_dir, params)?
        .into_iter()
        .map(|(table, query)| ReportUnit {
            relation: table.clone().or_else(|| export.table.clone()),
            table,
            query,
        })
        .collect();

    if config.source.source_type == SourceType::Mssql
        && let Some(cdc) = &export.cdc
        && let Some(ci) = &cdc.capture_instance
    {
        let resolved = crate::source::mssql::cdc::source_object_of_capture_instance(url, ci, tls);
        let resolved_kind = match &resolved {
            Ok(Some(_)) => Unresolved::Resolved,
            Ok(None) => Unresolved::NoRow,
            Err(_) => Unresolved::Unreadable,
        };
        let why = match &resolved {
            Ok(Some(_)) => None,
            Ok(None) => Some(format!(
                "capture instance '{ci}' is not in cdc.change_tables (CDC not enabled \
                 yet, disabled since, or a typo)"
            )),
            Err(e) => Some(format!(
                "could not read cdc.change_tables for '{ci}': {e:#}"
            )),
        };
        match (resolved, why) {
            (Ok(Some((schema, table))), _) => {
                for unit in units.iter_mut().filter(|u| u.table.is_none()) {
                    unit.query = format!("SELECT * FROM {schema}.{table}");
                    unit.relation = Some(format!("{schema}.{table}"));
                }
            }
            (_, Some(why)) => {
                let configured = export.table.as_deref();
                if unresolved_capture_is_fatal(on_unresolved, configured) {
                    let remedy = if matches!(resolved_kind, Unresolved::NoRow) {
                        "Enable capture on the table (`sys.sp_cdc_enable_table`) or \
                         correct `capture_instance:`"
                    } else {
                        "Restore this login's SELECT on the cdc schema"
                    };
                    crate::rivet_bail!(
                        crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
                        "mssql: {why}, and `table: {}` is a BARE name — which relation it \
                         means depends on the connection's default schema, so rivet cannot \
                         say what these artifacts hold. The warehouse schema is planned \
                         from this report, so guessing would create the load table with \
                         one relation's columns and feed it another's parquet, with every \
                         name matching and every count agreeing. {remedy}. Qualifying \
                         `table:` as `schema.table` also works, but change it knowingly: \
                         the warehouse table name is derived from it (a dot folds to an \
                         underscore), so the load would start writing to a DIFFERENT, \
                         empty table and leave the current one behind.",
                        configured.unwrap_or("<none>")
                    );
                }
                log::warn!(
                    "mssql: {why} — typing `{}` as the connection resolves it. {}",
                    configured.unwrap_or("the export"),
                    if degrade_is_unambiguous(configured) {
                        "The name is schema-qualified, so it means one relation on any \
                         connection — but rivet could not confirm it is the one the \
                         capture instance emits; `rivet run` checks that against the \
                         catalog and will refuse if they disagree."
                    } else {
                        "The name is BARE, so this is whatever the connection's default \
                         schema resolves it to — qualify it as `schema.table` if the \
                         captured relation lives elsewhere (note that changes the \
                         warehouse table name, which is derived from it)."
                    }
                );
            }
            (Err(_), None) | (Ok(None), None) => unreachable!("every non-resolution has a reason"),
        }
    }
    if config.source.columns_from_grants() {
        let tables: Vec<String> = units.iter().filter_map(|u| u.relation.clone()).collect();
        let readable = readable_by_table(config.source.source_type, url, tls, &tables)?;
        for unit in &mut units {
            if let Some(r) = unit.relation.as_ref().and_then(|t| readable.get(t)) {
                unit.query = r.select.clone();
            }
        }
    }
    Ok(units)
}

/// Open the configured source for scan-free probes.
pub(crate) fn connect_source(
    config: &Config,
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
) -> Result<Box<dyn source::Source>> {
    source::connect(
        config.source.source_type,
        url,
        tls,
        config.source.mongo.as_ref(),
    )
}

/// The columns the source declares finer than a microsecond: of `relation` by the catalog, of `query` by its description when there is no relation.
pub(crate) fn finer_than_microsecond_of(
    src: &mut dyn source::Source,
    relation: Option<&str>,
    query: &str,
    export: &str,
) -> Vec<String> {
    let (read, what) = match relation {
        Some(relation) => (src.finer_than_microsecond_columns(relation), relation),
        None => (
            seeded!(
                "load-key-query-finer-unread",
                Ok(Vec::new()),
                src.finer_than_microsecond_in_query(query)
            ),
            "the export query",
        ),
    };
    read.unwrap_or_else(|e| {
        log::warn!(
            "export '{export}': could not read the sub-microsecond columns of `{what}`: {e:#}"
        );
        Vec::new()
    })
}

/// What a successful run records for the load about one unit.
pub struct CapturedUnit {
    pub table: Option<String>,
    pub mappings: Vec<crate::types::TypeMapping>,
    pub primary_key: Option<Vec<String>>,
    pub finer_than_microsecond: Vec<String>,
}

/// Capture from the source, at run time, everything `rivet load` later plans from without it.
pub fn capture_load_units(
    config: &Config,
    export: &ExportConfig,
    column_overrides: &ColumnOverrides,
    config_dir: &std::path::Path,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Result<Vec<CapturedUnit>> {
    let url = config.source.resolve_url()?;
    let url = url.expose();
    let tls = config.source.tls.as_ref();
    let units = resolved_units(
        config,
        export,
        url,
        tls,
        config_dir,
        params,
        OnUnresolvedCapture::Fail,
    )?;
    let mut src = connect_source(config, url, tls)?;
    units
        .into_iter()
        .map(|unit| {
            let overrides = unit_overrides(export, unit.table.as_deref(), column_overrides);
            let mappings =
                probe_mappings(src.as_mut(), is_stream(export), &unit.query, &overrides)?;
            let primary_key = match unit.relation.as_deref() {
                Some(relation) => src.primary_key(relation).unwrap_or_else(|e| {
                    log::warn!(
                        "export '{}': could not read the primary key of `{relation}`: {e:#}",
                        export.name
                    );
                    None
                }),
                None => None,
            };
            let finer_than_microsecond = finer_than_microsecond_of(
                src.as_mut(),
                unit.relation.as_deref(),
                &unit.query,
                &export.name,
            );
            Ok(CapturedUnit {
                table: unit.table,
                mappings,
                primary_key,
                finer_than_microsecond,
            })
        })
        .collect()
}

/// Refuses an incremental export whose load orders or keys by a column that lands inexactly, from what the source resolves now.
pub(crate) fn refuse_inexact_load_keys(
    config: &Config,
    export: &ExportConfig,
    column_overrides: &ColumnOverrides,
    config_dir: &std::path::Path,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Result<()> {
    if admits_load_keys(config, export) {
        refuse_captured_units(
            config,
            export,
            &capture_load_units(config, export, column_overrides, config_dir, params)?,
        )
    } else {
        Ok(())
    }
}

/// Whether the export loads in a mode whose keys are admitted by their landing, the same predicate `rivet load` refuses by.
fn admits_load_keys(config: &Config, export: &ExportConfig) -> bool {
    let mode = crate::plan::load_settings::load_mode_of(config, export);
    config.load.is_some()
        && super::key_admission::admits_keys(mode)
        && !seeded!(
            "load-key-check-skips-cdc",
            mode == crate::plan::load_settings::LoadMode::Cdc,
            false
        )
}

/// Refuses the captured units whose cursor or primary key lands inexactly.
fn refuse_captured_units(
    config: &Config,
    export: &ExportConfig,
    units: &[CapturedUnit],
) -> Result<()> {
    let mode = crate::plan::load_settings::load_mode_of(config, export);
    for unit in units {
        let Some(load) =
            crate::plan::load_settings::effective_load(config, export, unit.table.as_deref())
        else {
            continue;
        };
        let pk: Vec<String> = match &load.pk {
            crate::config::load::KeyColumns::Columns(cols) => cols.clone(),
            crate::config::load::KeyColumns::Auto => unit.primary_key.clone().unwrap_or_default(),
            crate::config::load::KeyColumns::None => Vec::new(),
        };
        crate::preflight::key_admission::admit_load_keys(
            &export.name,
            mode,
            crate::plan::build::continued_key(config, export).or(export.cursor_column.as_deref()),
            &pk,
            &crate::preflight::key_admission::landings_of_mappings(
                &unit.mappings,
                &unit.finer_than_microsecond,
            ),
        )?;
    }
    Ok(())
}

/// One unit's report, against an already-open source connection.
fn collect_one(
    src: &mut dyn source::Source,
    export: &ExportConfig,
    table: Option<String>,
    query: &str,
    column_overrides: &ColumnOverrides,
    policy: &TypePolicy,
    target: Option<ExportTarget>,
) -> Result<ExportTypeReport> {
    let mappings = probe_mappings(src, is_stream(export), query, column_overrides)?;
    Ok(report_from_mappings(
        export,
        table,
        mappings,
        policy,
        target,
        &[],
    ))
}

/// One unit's resolved mappings, with overrides that narrow the source type downgraded to lossy.
pub(crate) fn probe_mappings(
    src: &mut dyn source::Source,
    stream: bool,
    query: &str,
    column_overrides: &ColumnOverrides,
) -> Result<Vec<crate::types::TypeMapping>> {
    let mut mappings = if stream {
        src.type_mappings(query, &ColumnOverrides::new())?
            .into_iter()
            .map(|m| source::cdc::retype_for_stream(m, column_overrides))
            .collect::<Result<Vec<_>>>()?
    } else {
        src.type_mappings(query, column_overrides)?
    };

    let src = std::cell::RefCell::new(src);
    downgrade_narrowing_overrides(
        &mut mappings,
        column_overrides,
        || {
            src.borrow_mut()
                .type_mappings(query, &ColumnOverrides::new())
        },
        || src.borrow_mut().finer_than_microsecond_in_query(query),
    )?;
    if !stream && seeded!("check-run-refusal-split", false, true) {
        mappings = mappings.into_iter().map(refuse_unmapped).collect();
    }
    Ok(mappings)
}

/// A batch column no Arrow type carries is refused, under every policy, with the reason its mapping names.
fn refuse_unmapped(m: crate::types::TypeMapping) -> crate::types::TypeMapping {
    if m.arrow_type.is_some() || m.refusal.is_some() {
        return m;
    }
    let reason = match &m.rivet_type {
        crate::types::RivetType::Unsupported { reason, .. } => reason.clone(),
        _ => "no Rivet mapping for this source type".to_string(),
    };
    m.with_refusal(reason)
}

/// One unit's report from already-resolved mappings, with no source connection.
pub fn report_from_mappings(
    export: &ExportConfig,
    table: Option<String>,
    mappings: Vec<crate::types::TypeMapping>,
    policy: &TypePolicy,
    target: Option<ExportTarget>,
    finer_than_microsecond: &[String],
) -> ExportTypeReport {
    let mut violations = policy.validate(&mappings);

    if export.format == FormatType::Csv {
        let fatal = policy.on_unsupported_type == PolicyAction::Fail;
        for m in &mappings {
            if let Some(dt) = m.arrow_type.as_ref()
                && !crate::format::csv::csv_serializable(dt)
            {
                violations.push(PolicyViolation {
                    column_name: m.column_name.clone(),
                    fidelity: TypeFidelity::Unsupported,
                    message: format!(
                        "column '{}' (Arrow {dt:?}) cannot be serialized to CSV — \
                         use `format: parquet` or drop it from the query",
                        m.column_name
                    ),
                    fatal,
                    refused: false,
                });
            }
        }
    }

    let mut target_failures = false;
    let rows = mappings
        .iter()
        .map(|m| {
            let (target_type, target_status, target_note, autoload_type, cast_sql) =
                if let Some(tgt) = target {
                    let spec = tgt.resolve_column(TargetInput::from(m));
                    if spec.status == TargetStatus::Fail {
                        target_failures = true;
                    }
                    let autoload =
                        (spec.autoload_type != spec.target_type).then_some(spec.autoload_type);
                    (
                        Some(spec.target_type),
                        Some(spec.status),
                        spec.note,
                        autoload,
                        spec.cast_sql,
                    )
                } else {
                    (None, None, None, None, None)
                };
            TypeReportRow {
                column: m.column_name.clone(),
                source_type: m.source_native_type.clone(),
                rivet_type: rivet_type_label(&m.rivet_type),
                arrow_type: m
                    .arrow_type
                    .as_ref()
                    .map(|t| format!("{t:?}"))
                    .unwrap_or_else(|| "-".into()),
                fidelity: m.fidelity,
                warnings: m.warnings.clone(),
                target_type,
                target_status,
                target_note,
                autoload_type,
                cast_sql,
                key_landing: crate::preflight::key_admission::KeyLanding::of(
                    m,
                    finer_than_microsecond.contains(&m.column_name),
                ),
            }
        })
        .collect();

    let sql_table = table.as_deref().unwrap_or(&export.name);
    let recovery_sql = target.and_then(|t| t.recovery_sql(&t.resolve_table(&mappings), sql_table));

    ExportTypeReport {
        export: export.name.clone(),
        table,
        columns: rows,
        violations,
        target_failures,
        recovery_sql,
    }
}

/// Print the report as a human-readable table to stdout.
pub fn print_table(report: &ExportTypeReport, target: Option<ExportTarget>) {
    print!("{}", render_table(report, target));
}

/// The report as the human-readable table [`print_table`] prints.
fn render_table(report: &ExportTypeReport, target: Option<ExportTarget>) -> String {
    let mut out = String::new();
    macro_rules! ln {
        ($($arg:tt)*) => {{
            out.push_str(&format!($($arg)*));
            out.push('\n');
        }};
    }
    let col_w = col_width(&report.columns, |r| r.column.len());
    let src_w = col_width(&report.columns, |r| r.source_type.len()).max("Source type".len());
    let rv_w = col_width(&report.columns, |r| r.rivet_type.len()).max("Rivet type".len());
    let arr_w = col_width(&report.columns, |r| r.arrow_type.len()).max("Arrow type".len());
    let fid_w = "logical_string".len();

    ln!("");
    if let Some(tgt) = target {
        ln!(
            "Export: {}  [target: {}]",
            report.display_name(),
            tgt.label()
        );
    } else {
        ln!("Export: {}", report.display_name());
    }

    if target.is_some() {
        let tgt_w = col_width(&report.columns, |r| {
            r.target_type
                .as_ref()
                .map_or_else(|| "-".to_string(), ToString::to_string)
                .len()
        })
        .max("Target type".len());
        let sta_w = "Status".len();

        ln!(
            "  {:<col_w$}  {:<src_w$}  {:<rv_w$}  {:<arr_w$}  {:<fid_w$}  {:<tgt_w$}  {:<sta_w$}",
            "Column",
            "Source type",
            "Rivet type",
            "Arrow type",
            "Fidelity",
            "Target type",
            "Status"
        );
        ln!(
            "  {:-<col_w$}  {:-<src_w$}  {:-<rv_w$}  {:-<arr_w$}  {:-<fid_w$}  {:-<tgt_w$}  {:-<sta_w$}",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
        );
        for row in &report.columns {
            let status_label = row.target_status.as_ref().map(|s| s.label()).unwrap_or("-");
            let tgt_type = row
                .target_type
                .as_ref()
                .map_or_else(|| "-".to_string(), ToString::to_string);
            let status_marker = match &row.target_status {
                Some(TargetStatus::Fail) => " ✗",
                Some(TargetStatus::Warn) => " ~",
                _ => "",
            };
            ln!(
                "  {:<col_w$}  {:<src_w$}  {:<rv_w$}  {:<arr_w$}  {}{:<rest$}  {:<tgt_w$}  {}{}",
                row.column,
                row.source_type,
                row.rivet_type,
                row.arrow_type,
                row.fidelity.label(),
                "",
                tgt_type,
                status_label,
                status_marker,
                rest = fid_w - row.fidelity.label().len(),
            );
            if let Some(autoload) = &row.autoload_type {
                ln!("  {:<col_w$}    autoload: {}", "", autoload);
            }
            if let Some(note) = &row.target_note {
                ln!("  {:<col_w$}    note: {}", "", note);
            }
            if let Some(cast) = &row.cast_sql {
                ln!("  {:<col_w$}    recover: {}", "", cast);
            }
            for w in &row.warnings {
                ln!("  {:<col_w$}    warning: {}", "", w);
            }
        }
    } else {
        ln!(
            "  {:<col_w$}  {:<src_w$}  {:<rv_w$}  {:<arr_w$}  {:<fid_w$}",
            "Column",
            "Source type",
            "Rivet type",
            "Arrow type",
            "Fidelity"
        );
        ln!(
            "  {:-<col_w$}  {:-<src_w$}  {:-<rv_w$}  {:-<arr_w$}  {:-<fid_w$}",
            "",
            "",
            "",
            "",
            ""
        );
        for row in &report.columns {
            ln!(
                "  {:<col_w$}  {:<src_w$}  {:<rv_w$}  {:<arr_w$}  {}{}",
                row.column,
                row.source_type,
                row.rivet_type,
                row.arrow_type,
                row.fidelity.label(),
                fidelity_marker(row.fidelity),
            );
            for w in &row.warnings {
                ln!("  {:<col_w$}    warning: {}", "", w);
            }
        }
    }

    if !report.violations.is_empty() {
        ln!("");
        for v in &report.violations {
            let prefix = if v.fatal { "  FAIL" } else { "  WARN" };
            ln!("{}: {}", prefix, v.message);
        }
    }

    if let Some(sql) = &report.recovery_sql {
        ln!("");
        let label = target.map(|t| t.label()).unwrap_or("target");
        if target == Some(ExportTarget::BigQuery) {
            ln!("  {label} type recovery — bare autoload degrades JSON/UUID→BYTES, naive");
            ln!("  timestamp→TIMESTAMP, array→RECORD; load with --autodetect then run:");
        } else {
            ln!("  {label} type recovery for Parquet loaded without `rivet load` (which declares");
            ln!("  these types itself):");
        }
        for line in sql.lines() {
            ln!("    {line}");
        }
    }
    out
}

fn col_width(rows: &[TypeReportRow], f: impl Fn(&TypeReportRow) -> usize) -> usize {
    rows.iter().map(f).max().unwrap_or(8).max(8)
}

fn fidelity_marker(f: TypeFidelity) -> &'static str {
    match f {
        TypeFidelity::Lossy | TypeFidelity::Unsupported => " ✗",
        TypeFidelity::LogicalString => " ~",
        _ => "",
    }
}

fn rivet_type_label(t: &crate::types::RivetType) -> String {
    use crate::types::RivetType::*;
    match t {
        Bool => "bool".into(),
        Int16 => "int2".into(),
        Int32 => "int4".into(),
        Int64 => "int8".into(),
        UInt64 => "uint8".into(),
        Float32 => "float4".into(),
        Float64 => "float8".into(),
        Decimal { precision, scale } => format!("decimal({precision},{scale})"),
        Date => "date".into(),
        Time { .. } => "time".into(),
        Timestamp {
            timezone: Some(_), ..
        } => "timestamp_tz".into(),
        Timestamp { timezone: None, .. } => "timestamp".into(),
        String => "text".into(),
        Text => "text".into(),
        Binary => "binary".into(),
        Json => "json".into(),
        Uuid => "uuid".into(),
        Enum => "enum".into(),
        Interval => "interval".into(),
        List { inner } => format!("list<{}>", rivet_type_label(inner)),
        Unsupported { native_type, .. } => format!("unsupported({native_type})"),
    }
}

#[cfg(test)]
mod tests {
    /// A batch column with no Arrow type is refused under every policy, with its reason; the rest is untouched.
    #[test]
    fn an_unmapped_batch_column_is_refused_under_every_policy_and_nothing_else_is() {
        use crate::types::policy::TypePolicy;
        use crate::types::{RivetType, SourceColumn, TypeMapping};
        let col = |name: &str, t: RivetType| {
            TypeMapping::from_source(&SourceColumn::simple(name, "native", true), t)
        };
        let unmapped = col(
            "net",
            RivetType::Unsupported {
                native_type: "cidr".into(),
                reason: "no arrow type".into(),
            },
        );
        let refused = super::refuse_unmapped(unmapped);
        assert_eq!(refused.refusal.as_deref(), Some("no arrow type"));
        for policy in [TypePolicy::warn_only(), TypePolicy::strict()] {
            let v = policy.validate(std::slice::from_ref(&refused));
            assert!(v.len() == 1 && v[0].fatal && v[0].refused, "{v:?}");
        }
        let mapped = col("id", RivetType::Int64);
        assert_eq!(super::refuse_unmapped(mapped.clone()).refusal, None);
        let own = col(
            "x",
            RivetType::Unsupported {
                native_type: "t".into(),
                reason: "r".into(),
            },
        )
        .with_refusal("own remedy");
        assert_eq!(
            super::refuse_unmapped(own).refusal.as_deref(),
            Some("own remedy")
        );
    }

    /// A column no Arrow type carries is refused on the batch probe and kept as it is on the stream probe.
    #[test]
    fn the_probe_refuses_an_unmapped_column_for_batch_and_not_for_a_stream() {
        use crate::types::{ColumnOverrides, RivetType, SourceColumn, TypeMapping};
        struct Unmapped;
        impl crate::source::Source for Unmapped {
            fn export(
                &mut self,
                _request: &crate::source::ExportRequest<'_>,
                _sink: &mut dyn crate::source::BatchSink,
            ) -> anyhow::Result<()> {
                Ok(())
            }
            fn query_scalar(&mut self, _sql: &str) -> anyhow::Result<Option<String>> {
                Ok(None)
            }
            fn type_mappings(
                &mut self,
                _query: &str,
                _overrides: &ColumnOverrides,
            ) -> anyhow::Result<Vec<TypeMapping>> {
                Ok(vec![TypeMapping::from_source(
                    &SourceColumn::simple("net", "cidr", true),
                    RivetType::Unsupported {
                        native_type: "cidr".into(),
                        reason: "no arrow type".into(),
                    },
                )])
            }
        }
        let overrides = ColumnOverrides::new();
        let batch = super::probe_mappings(&mut Unmapped, false, "q", &overrides).unwrap();
        assert_eq!(batch[0].refusal.as_deref(), Some("no arrow type"));
        let stream = super::probe_mappings(&mut Unmapped, true, "q", &overrides).unwrap();
        assert_eq!(stream[0].refusal, None);
    }

    /// The two callers must DIVERGE, and only where divergence is warranted.
    #[test]
    fn only_a_bare_name_makes_an_unresolvable_capture_fatal_and_only_for_the_load() {
        use super::{
            OnUnresolvedCapture as On, degrade_is_unambiguous, unresolved_capture_is_fatal,
        };

        assert!(unresolved_capture_is_fatal(On::Fail, Some("orders")));
        assert!(!unresolved_capture_is_fatal(On::Fail, Some("dbo.orders")));

        assert!(!unresolved_capture_is_fatal(On::Degrade, Some("orders")));
        assert!(!unresolved_capture_is_fatal(
            On::Degrade,
            Some("dbo.orders")
        ));

        assert!(unresolved_capture_is_fatal(On::Fail, None));
        assert!(!degrade_is_unambiguous(None));
    }

    use super::*;
    use crate::types::narrowing::override_narrows;
    use crate::types::{RivetType, TypeFidelity};

    fn mapping(column: &str, native: &str, t: RivetType) -> crate::types::TypeMapping {
        crate::types::TypeMapping {
            column_name: column.into(),
            source_native_type: native.into(),
            rivet_type: t,
            arrow_type: None,
            fidelity: TypeFidelity::Exact,
            nullable: true,
            warnings: vec![],
            delivery: crate::types::Delivery::Native,
            refusal: None,
        }
    }

    /// Both directions of the re-probe guard, and the downgrade itself.
    #[test]
    fn narrowing_downgrade_reprobes_only_when_overrides_exist() {
        let source = || {
            vec![
                mapping("price", "numeric(10,2)", dec(10, 2)),
                mapping("id", "int8", RivetType::Int64),
            ]
        };

        let mut untouched = source();
        let probed = std::cell::Cell::new(false);
        downgrade_narrowing_overrides(
            &mut untouched,
            &ColumnOverrides::new(),
            || {
                probed.set(true);
                Ok(source())
            },
            || Ok(Vec::new()),
        )
        .unwrap();
        assert!(
            !probed.get(),
            "no `columns:` overrides ⇒ no second type probe — the round-trip is \
             per REPORT, and a multiplex export has one per captured table"
        );
        assert!(
            untouched.iter().all(|m| m.fidelity == TypeFidelity::Exact),
            "nothing to downgrade without an override"
        );

        let mut overrides = ColumnOverrides::new();
        overrides.insert("price".into(), dec(20, 0));
        overrides.insert("id".into(), RivetType::Int64);
        let mut mapped = vec![
            mapping("price", "numeric(10,2)", dec(20, 0)),
            mapping("id", "int8", RivetType::Int64),
        ];
        let probed = std::cell::Cell::new(false);
        downgrade_narrowing_overrides(
            &mut mapped,
            &overrides,
            || {
                probed.set(true);
                Ok(source())
            },
            || Ok(Vec::new()),
        )
        .unwrap();
        assert!(
            probed.get(),
            "an override present ⇒ the source IS re-probed"
        );
        assert_eq!(
            mapped[0].fidelity,
            TypeFidelity::Lossy,
            "decimal(20,0) over numeric(10,2) drops two fractional digits at run \
             — reporting it `exact` is the check↔run gap #32 closed"
        );
        assert!(
            mapped[0].warnings.iter().any(|w| w.contains("scale")),
            "the downgrade must say WHY: {:?}",
            mapped[0].warnings
        );
        assert_eq!(
            mapped[1].fidelity,
            TypeFidelity::Exact,
            "a non-narrowing override is not downgraded — the rule is narrowing, \
             not overridden-at-all"
        );
    }

    /// A `float8` override reads `lossy` over a source decimal of precision above 15 and stays `exact` over a decimal that fits and over an integer.
    #[test]
    fn a_float8_override_over_a_wide_decimal_is_reported_lossy() {
        let source = vec![
            mapping("cash", "money", dec(19, 4)),
            mapping("cents", "numeric(10,2)", dec(10, 2)),
            mapping("id", "int8", RivetType::Int64),
            mapping("exact15", "numeric(15,0)", dec(15, 0)),
        ];
        let mut overrides = ColumnOverrides::new();
        for c in ["cash", "cents", "id", "exact15"] {
            overrides.insert(c.into(), RivetType::Float64);
        }
        let mut mapped = ["cash", "cents", "id", "exact15"]
            .map(|c| mapping(c, "float8", RivetType::Float64))
            .to_vec();
        downgrade_narrowing_overrides(&mut mapped, &overrides, || Ok(source), || Ok(Vec::new()))
            .unwrap();
        let verdicts: Vec<_> = mapped.iter().map(|m| m.fidelity).collect();
        assert_eq!(
            verdicts,
            vec![
                TypeFidelity::Lossy,
                TypeFidelity::Exact,
                TypeFidelity::Exact,
                TypeFidelity::Exact
            ]
        );
        assert!(
            mapped[0]
                .warnings
                .iter()
                .any(|w| w.contains("RIVET_SOURCE_VALUE_UNREPRESENTABLE")),
            "{:?}",
            mapped[0].warnings
        );
    }

    fn cfg_from(yaml: &str) -> Config {
        Config::from_yaml(yaml).unwrap()
    }

    const OVERRIDE_PROBE_CFG: &str = r#"
source:
  type: postgres
  url: "postgresql://rivet:rivet@127.0.0.1:1/none"
exports:
  - name: batch
    query: "SELECT 1 AS a"
    mode: full
    format: parquet
    columns:
      a: "string"
    destination:
      type: local
      path: /tmp/x
  - name: stream
    tables: [orders]
    mode: cdc
    format: parquet
    columns:
      a: "string"
    cdc:
      checkpoint: ./cdc.ckpt
    destination:
      type: local
      path: /tmp/x
"#;

    fn probe(config: &Config, export: &ExportConfig, overrides: &ColumnOverrides) -> Result<()> {
        refuse_override_wire_mismatch(config, export, overrides, std::path::Path::new("."), None)
    }

    #[test]
    fn only_a_cdc_export_is_typed_by_the_stream_builder() {
        let config = cfg_from(OVERRIDE_PROBE_CFG);
        assert!(!super::is_stream(&config.exports[0]));
        assert!(super::is_stream(&config.exports[1]));
    }

    #[test]
    fn the_override_probe_connects_for_every_export_with_overrides_cdc_included() {
        let config = cfg_from(OVERRIDE_PROBE_CFG);
        let with =
            crate::plan::parse_column_overrides_pub(&config.exports[0].columns, "batch").unwrap();
        assert!(!with.is_empty());
        assert!(
            probe(&config, &config.exports[0], &with).is_err(),
            "a batch export with overrides opens the source, which is unreachable here"
        );
        assert!(
            probe(&config, &config.exports[0], &ColumnOverrides::new()).is_ok(),
            "no override, nothing to read"
        );
        assert!(
            probe(&config, &config.exports[1], &with).is_err(),
            "a CDC export with overrides opens the source too, so the stream builder grades its pairs"
        );
    }

    /// A multiplex `tables:` CDC export must resolve ONE unit per captured table.
    #[test]
    fn every_narrowing_site_keys_on_the_bare_table_name() {
        let overrides: crate::types::ColumnOverrides = [
            ("orders.amount".to_string(), crate::types::RivetType::Int64),
            ("note".to_string(), crate::types::RivetType::Int64),
        ]
        .into_iter()
        .collect();

        let unit = "public.orders";
        let from_capture = crate::types::overrides_for_unit(&overrides, Some(unit));
        let from_resolver = crate::types::overrides_for_unit(&overrides, Some(unit));

        assert_eq!(
            from_capture.len(),
            from_resolver.len(),
            "the capture and the resolver must narrow a schema-qualified unit the same way"
        );
        assert!(
            from_resolver.contains_key("amount"),
            "the qualified override `orders.amount` must reach a `public.orders` unit — \
             dropping it types the warehouse column from the raw catalog while the \
             Parquet already holds the overridden type; got {from_resolver:?}"
        );
        assert!(
            from_resolver.contains_key("note"),
            "a bare key still applies to every table: {from_resolver:?}"
        );
        assert!(
            !crate::types::overrides_for_table(&overrides, unit).contains_key("amount"),
            "guard the guard: passing the QUALIFIED name really does drop the override, \
             so this test is not vacuous"
        );
    }

    /// The SINGLE-table arm of the same rule.
    #[test]
    fn a_qualified_override_reaches_a_single_table_export_the_way_the_run_applies_it() {
        let config = cfg_from(
            r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: orders
    table: public.orders
    mode: full
    format: parquet
    columns:
      orders.amount: "decimal(20,4)"
      note: "text"
    destination:
      type: local
      path: /tmp/x
"#,
        );
        let export = &config.exports[0];
        let all = crate::plan::parse_column_overrides_pub(&export.columns, &export.name)
            .expect("the fixture's `columns:` must parse");
        assert!(
            all.contains_key("orders.amount"),
            "fixture is inert: the qualified key must survive parsing, else this              test cannot distinguish narrowing from an empty map — got {all:?}"
        );

        let narrowed = unit_overrides(export, None, &all);

        assert!(
            narrowed.contains_key("amount"),
            "a qualified override on this export's OWN table must apply: `run`              writes the overridden type (plan::build narrows the same way), so a              resolver that drops it types the warehouse column from the raw              catalog and the load disagrees with the Parquet; got {narrowed:?}"
        );
        assert!(
            narrowed.contains_key("note"),
            "a bare key still applies: {narrowed:?}"
        );
        assert!(
            !all.contains_key("amount"),
            "the raw map is keyed `orders.amount`, so a BARE `amount` lookup — the              only lookup the drivers make — misses it: {all:?}"
        );
    }

    #[test]
    fn report_units_yields_one_unit_per_multiplex_table() {
        let config = cfg_from(
            r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: cdc
    tables: [orders, customers, line_items]
    mode: cdc
    format: parquet
    cdc:
      checkpoint: ./cdc.ckpt
    destination:
      type: local
      path: /tmp/x
"#,
        );
        let units = report_units(&config.exports[0], std::path::Path::new("."), None).unwrap();
        assert_eq!(
            units,
            vec![
                (Some("orders".into()), "SELECT * FROM orders".to_string()),
                (Some("customers".into()), "SELECT * FROM customers".into()),
                (Some("line_items".into()), "SELECT * FROM line_items".into()),
            ],
            "each captured table is its own resolver unit, probed the way the \
             capture probes it"
        );
    }

    /// The other side of the same branch: a single-table export is ONE unit whose `table` is `None`.
    #[test]
    fn report_units_leaves_a_single_table_export_as_one_unnamed_unit() {
        let config = cfg_from(
            r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: orders
    table: public.orders
    mode: full
    format: parquet
    destination:
      type: local
      path: /tmp/x
"#,
        );
        let units = report_units(&config.exports[0], std::path::Path::new("."), None).unwrap();
        assert_eq!(
            units,
            vec![(None, "SELECT * FROM public.orders".to_string())],
            "a `table:` export resolves its own query, unnamed — the export is the unit"
        );
    }

    /// `display_name` is what an operator reads in the type-report header, and it must distinguish the N documents a multiplex export now emits.
    #[test]
    fn display_name_addresses_a_multiplex_unit_by_export_and_table() {
        let base = ExportTypeReport {
            export: "cdc".into(),
            table: None,
            columns: vec![],
            violations: vec![],
            target_failures: false,
            recovery_sql: None,
        };
        assert_eq!(base.display_name(), "cdc");
        let unit = ExportTypeReport {
            table: Some("orders".into()),
            ..base
        };
        assert_eq!(unit.display_name(), "cdc/orders");
    }

    fn dec(precision: u8, scale: i8) -> RivetType {
        RivetType::Decimal { precision, scale }
    }

    #[test]
    fn narrows_flags_scale_reduction_as_lossy() {
        let reason =
            override_narrows(&dec(10, 2), &dec(20, 0), false).expect("scale drop is lossy");
        assert!(
            reason.contains("scale"),
            "reason should name scale: {reason}"
        );
        assert!(
            reason.contains("lossy"),
            "reason should say lossy: {reason}"
        );
    }

    #[test]
    fn narrows_none_when_scale_preserved() {
        assert!(override_narrows(&dec(10, 2), &dec(20, 2), false).is_none());
        assert!(override_narrows(&dec(10, 2), &dec(10, 2), false).is_none());
    }

    #[test]
    fn narrows_none_when_scale_widened() {
        assert!(override_narrows(&dec(10, 2), &dec(12, 4), false).is_none());
    }

    #[test]
    fn narrows_flags_integer_digit_reduction_as_lossy() {
        let reason =
            override_narrows(&dec(20, 0), &dec(10, 0), false).expect("integer-digit drop is lossy");
        assert!(
            reason.contains("integer-digit") && reason.contains("lossy"),
            "reason: {reason}"
        );
    }

    #[test]
    fn narrows_flags_a_decimal_float8_override_past_fifteen_digits_as_lossy() {
        for (p, s) in [(19, 4), (38, 0), (16, 0), (20, 6)] {
            let reason = override_narrows(&dec(p, s), &RivetType::Float64, false)
                .unwrap_or_else(|| panic!("decimal({p},{s}) into float8 is lossy"));
            assert!(
                reason.contains("lossy") && reason.contains("RIVET_SOURCE_VALUE_UNREPRESENTABLE"),
                "reason: {reason}"
            );
        }
        for (p, s) in [(15, 2), (10, 4), (9, 0), (1, 0)] {
            assert!(override_narrows(&dec(p, s), &RivetType::Float64, false).is_none());
        }
        assert!(override_narrows(&RivetType::Int64, &RivetType::Float64, false).is_none());
        assert!(override_narrows(&RivetType::Float64, &RivetType::Float64, false).is_none());
        assert!(override_narrows(&dec(38, 0), &RivetType::String, false).is_none());
    }

    #[test]
    fn narrows_none_for_non_decimal_overrides() {
        assert!(override_narrows(&RivetType::Int32, &RivetType::Int64, false).is_none());
        assert!(override_narrows(&RivetType::Int64, &RivetType::String, false).is_none());
    }

    #[test]
    fn fidelity_marker_lossy_is_cross() {
        assert_eq!(fidelity_marker(TypeFidelity::Lossy), " ✗");
    }

    #[test]
    fn fidelity_marker_unsupported_is_cross() {
        assert_eq!(fidelity_marker(TypeFidelity::Unsupported), " ✗");
    }

    #[test]
    fn fidelity_marker_logical_string_is_tilde() {
        assert_eq!(fidelity_marker(TypeFidelity::LogicalString), " ~");
    }

    #[test]
    fn fidelity_marker_exact_is_empty() {
        assert_eq!(fidelity_marker(TypeFidelity::Exact), "");
    }

    #[test]
    fn fidelity_marker_compatible_is_empty() {
        assert_eq!(fidelity_marker(TypeFidelity::Compatible), "");
    }

    #[test]
    fn label_bool() {
        assert_eq!(rivet_type_label(&RivetType::Bool), "bool");
    }

    #[test]
    fn label_int64() {
        assert_eq!(rivet_type_label(&RivetType::Int64), "int8");
    }

    #[test]
    fn label_float64() {
        assert_eq!(rivet_type_label(&RivetType::Float64), "float8");
    }

    #[test]
    fn label_decimal_with_precision_and_scale() {
        assert_eq!(
            rivet_type_label(&RivetType::Decimal {
                precision: 18,
                scale: 2
            }),
            "decimal(18,2)"
        );
    }

    #[test]
    fn label_text() {
        assert_eq!(rivet_type_label(&RivetType::Text), "text");
    }

    #[test]
    fn label_uuid() {
        assert_eq!(rivet_type_label(&RivetType::Uuid), "uuid");
    }

    #[test]
    fn label_list_of_int64() {
        let t = RivetType::List {
            inner: Box::new(RivetType::Int64),
        };
        assert_eq!(rivet_type_label(&t), "list<int8>");
    }

    #[test]
    fn label_unsupported_native_type() {
        let t = RivetType::Unsupported {
            native_type: "tsvector".into(),
            reason: "not supported".into(),
        };
        assert_eq!(rivet_type_label(&t), "unsupported(tsvector)");
    }

    #[test]
    fn col_width_empty_returns_minimum_8() {
        let rows: Vec<TypeReportRow> = vec![];
        assert_eq!(col_width(&rows, |_r| 0), 8);
    }

    #[test]
    fn col_width_short_values_returns_minimum_8() {
        let row = TypeReportRow {
            key_landing: Default::default(),
            column: "a".into(),
            source_type: "b".into(),
            rivet_type: "c".into(),
            arrow_type: "d".into(),
            fidelity: TypeFidelity::Exact,
            warnings: vec![],
            target_type: None,
            target_status: None,
            target_note: None,
            autoload_type: None,
            cast_sql: None,
        };
        assert_eq!(col_width(&[row], |r| r.column.len()), 8);
    }

    /// With a target, the table shows each column's typed target type, its autoload and its recovery cast.
    #[test]
    fn the_target_table_renders_the_typed_target_and_autoload() {
        use crate::types::target::BqType;
        let row = TypeReportRow {
            key_landing: Default::default(),
            column: "payload".into(),
            source_type: "jsonb".into(),
            rivet_type: "json".into(),
            arrow_type: "Utf8".into(),
            fidelity: TypeFidelity::Exact,
            warnings: vec![],
            target_type: Some(BqType::Json.into()),
            target_status: Some(TargetStatus::Warn),
            target_note: None,
            autoload_type: Some(BqType::Bytes.into()),
            cast_sql: Some("PARSE_JSON(SAFE_CONVERT_BYTES_TO_STRING(payload))".into()),
        };
        let report = ExportTypeReport {
            export: "e".into(),
            table: None,
            columns: vec![row],
            violations: vec![PolicyViolation {
                column_name: "payload".into(),
                fidelity: TypeFidelity::Lossy,
                message: "payload is lossy".into(),
                fatal: false,
                refused: false,
            }],
            target_failures: false,
            recovery_sql: None,
        };
        let text = render_table(&report, Some(ExportTarget::BigQuery));
        assert!(text.ends_with("\n  WARN: payload is lossy\n"), "{text}");
        assert!(text.contains("Export: e  [target: bigquery]"), "{text}");
        assert!(
            text.contains("  exact           JSON         warn ~"),
            "{text}"
        );
        assert!(text.contains("    autoload: BYTES"), "{text}");
        assert!(
            text.contains("    recover: PARSE_JSON(SAFE_CONVERT_BYTES_TO_STRING(payload))"),
            "{text}"
        );
    }

    /// The recovery header names BigQuery's autoload degradations only for BigQuery.
    #[test]
    fn the_recovery_header_speaks_for_its_own_target() {
        let report = ExportTypeReport {
            export: "e".into(),
            table: None,
            columns: vec![],
            violations: vec![],
            target_failures: false,
            recovery_sql: Some("SELECT 1".into()),
        };
        let bq = render_table(&report, Some(ExportTarget::BigQuery));
        assert!(
            bq.contains("bigquery type recovery — bare autoload degrades"),
            "{bq}"
        );
        assert!(bq.contains("    SELECT 1\n"), "{bq}");
        let ch = render_table(&report, Some(ExportTarget::ClickHouse));
        assert!(
            ch.contains("type recovery for Parquet loaded without `rivet load`"),
            "{ch}"
        );
        assert!(
            !ch.contains("BYTES") && !ch.contains("--autodetect"),
            "{ch}"
        );
    }

    #[test]
    fn col_width_long_value_returns_that_length() {
        let row = TypeReportRow {
            key_landing: Default::default(),
            column: "a_very_long_column_name".into(),
            source_type: "int8".into(),
            rivet_type: "int8".into(),
            arrow_type: "Int64".into(),
            fidelity: TypeFidelity::Exact,
            warnings: vec![],
            target_type: None,
            target_status: None,
            target_note: None,
            autoload_type: None,
            cast_sql: None,
        };
        let w = col_width(&[row], |r| r.column.len());
        assert_eq!(w, "a_very_long_column_name".len());
    }
    fn keyed_cfg(mode: &str, load: bool, pk: &str) -> Config {
        let load = if load {
            format!(
                "load: {{ target: clickhouse, url: \"http://127.0.0.1:1\", database: d, user: u, password_env: P{pk} }}\n"
            )
        } else {
            String::new()
        };
        let cursor = match mode {
            "incremental" => "    cursor_column: VER\n",
            "cdc" => "    cdc: { checkpoint: ./cdc.ckpt }\n",
            _ => "",
        };
        let source = if mode == "cdc" {
            "type: mssql\n  url: \"mssql://u:p@127.0.0.1:1/x\""
        } else {
            "type: oracle\n  url: \"oracle://u:p@127.0.0.1:1/x\""
        };
        cfg_from(&format!(
            "source:\n  {source}\n{load}exports:\n  - name: e\n    table: T\n    mode: {mode}\n{cursor}    format: parquet\n    destination:\n      type: local\n      path: /tmp/x\n"
        ))
    }

    fn unit(
        native: &str,
        rivet: crate::types::RivetType,
        pk: Option<&[&str]>,
        finer: &[&str],
    ) -> CapturedUnit {
        let col = |name: &str| {
            crate::types::TypeMapping::from_source(
                &crate::types::SourceColumn::simple(name, native, false),
                rivet.clone(),
            )
        };
        CapturedUnit {
            table: None,
            mappings: vec![col("VER"), col("TS")],
            primary_key: pk.map(|p| p.iter().map(|s| s.to_string()).collect()),
            finer_than_microsecond: finer.iter().map(|s| s.to_string()).collect(),
        }
    }

    const INEXACT: &str = "RIVET_LOAD_KEY_COLUMN_LANDS_INEXACT";

    /// Key admission at check reads an incremental or cdc export that has a load block.
    #[test]
    fn key_admission_reads_only_an_incremental_or_cdc_export_with_a_load() {
        for (mode, load, want) in [
            ("incremental", true, true),
            ("incremental", false, false),
            ("cdc", true, true),
            ("cdc", false, false),
            ("full", true, false),
            ("full", false, false),
        ] {
            let c = keyed_cfg(mode, load, "");
            assert_eq!(
                admits_load_keys(&c, &c.exports[0]),
                want,
                "{mode} load={load}"
            );
        }
    }

    /// A captured unit whose cursor lands as text is refused by code; a numeric landing is not.
    #[test]
    fn a_captured_cursor_that_lands_as_text_is_refused_and_a_number_is_not() {
        let c = keyed_cfg("incremental", true, "");
        let text = unit("NUMBER", crate::types::RivetType::String, None, &[]);
        let err = refuse_captured_units(&c, &c.exports[0], &[text]).unwrap_err();
        assert_eq!(crate::error::error_code(&err), Some(INEXACT), "{err:#}");
        let number = unit("NUMBER", crate::types::RivetType::Int64, None, &[]);
        refuse_captured_units(&c, &c.exports[0], &[number]).unwrap();
        refuse_captured_units(&c, &c.exports[0], &[]).unwrap();
    }

    /// A key declared finer than a microsecond is refused whether `pk:` names it, leaves it to the recorded key, or the unit has none.
    #[test]
    fn a_captured_key_finer_than_a_microsecond_is_refused_by_the_key_the_load_uses() {
        let micros = crate::types::RivetType::Timestamp {
            unit: crate::types::TimeUnit::Microsecond,
            timezone: None,
        };
        let finer = |pk: Option<&[&str]>| unit("datetime2", micros.clone(), pk, &["TS"]);
        let refused = |load_pk: &str, u: CapturedUnit| {
            let c = keyed_cfg("incremental", true, load_pk);
            refuse_captured_units(&c, &c.exports[0], &[u])
                .err()
                .and_then(|e| crate::error::error_code(&e))
        };
        assert_eq!(refused(", pk: [TS]", finer(None)), Some(INEXACT));
        assert_eq!(refused("", finer(Some(&["TS"]))), Some(INEXACT));
        assert_eq!(
            refused("", finer(None)),
            None,
            "no key recorded, none to collapse"
        );
        assert_eq!(refused(", pk: none", finer(Some(&["TS"]))), None);
        assert_eq!(refused(", pk: [VER]", finer(Some(&["TS"]))), None);
    }

    const DEAD_MYSQL: &str = "mysql://u:p@127.0.0.1:1/app";
    const DEAD_ORACLE: &str = "oracle://u:p@127.0.0.1:1/app";

    /// A multiplexed `mode: cdc` export of `engine` with an optional `columns_from: grants`.
    fn multiplex_cfg(engine: &str, url: &str, grants: bool) -> Config {
        let policy = if grants {
            "  columns_from: grants\n"
        } else {
            ""
        };
        cfg_from(&format!(
            "source:\n  type: {engine}\n  url: \"{url}\"\n{policy}exports:\n  - name: cdc\n    \
             tables: [orders, customers]\n    mode: cdc\n    format: parquet\n    \
             cdc: {{ checkpoint: ./cdc.ckpt }}\n    destination: {{ type: local, path: /tmp/x }}\n"
        ))
    }

    type UnitRow = (Option<String>, String, Option<String>);

    fn units_of(config: &Config, url: &str) -> Result<Vec<UnitRow>> {
        let units = resolved_units(
            config,
            &config.exports[0],
            url,
            None,
            std::path::Path::new("."),
            None,
            OnUnresolvedCapture::Degrade,
        )?;
        Ok(units
            .into_iter()
            .map(|u| (u.table, u.query, u.relation))
            .collect())
    }

    /// Each multiplexed table is one unit probed by `SELECT *`, its relation the table itself.
    #[test]
    fn the_resolved_units_of_a_multiplexed_export_are_its_tables() {
        let config = multiplex_cfg("postgres", "postgresql://h/d", false);
        let units = units_of(&config, "postgresql://h/d").unwrap();
        let unit = |t: &str| {
            (
                Some(t.to_string()),
                format!("SELECT * FROM {t}"),
                Some(t.to_string()),
            )
        };
        assert_eq!(units, vec![unit("orders"), unit("customers")]);
    }

    /// Under `columns_from: grants` the unit queries come from the server's grants, so a source that cannot be read stops the report.
    #[test]
    fn a_grants_report_reads_the_grants_from_the_source_and_stops_when_it_cannot() {
        for (engine, url) in [("mysql", DEAD_MYSQL), ("oracle", DEAD_ORACLE)] {
            let config = multiplex_cfg(engine, url, true);
            let err = units_of(&config, url)
                .err()
                .unwrap_or_else(|| panic!("{engine}: units resolved without a source"));
            assert!(!format!("{err:#}").is_empty(), "{engine}");
            let off = units_of(&multiplex_cfg(engine, url, false), url).unwrap();
            assert_eq!(off.len(), 2, "{engine}: no policy, no source read");
        }
    }

    /// Each engine that enforces grants reads them from its own server; any other engine reads none.
    #[test]
    fn the_grants_of_a_report_are_read_by_the_engine_that_enforces_them() {
        let tables = vec!["orders".to_string()];
        let read = |t: SourceType, url: &str| readable_by_table(t, url, None, &tables);
        read(SourceType::Mysql, DEAD_MYSQL).expect_err("mysql reads its server");
        #[cfg(feature = "oracle")]
        read(SourceType::Oracle, DEAD_ORACLE).expect_err("oracle reads its server");
        read(SourceType::Postgres, "postgresql://u:p@127.0.0.1:1/app")
            .expect_err("postgres reads its server");
        read(SourceType::Mssql, "sqlserver://u:p@127.0.0.1:1/app")
            .expect_err("mssql reads its server");
        let none = read(SourceType::Mongo, "postgresql://u:p@127.0.0.1:1/app").unwrap();
        assert!(none.is_empty(), "mongo reads no grants");
    }

    /// A source whose result columns are the given `(name, native type, default type)` and whose describe names the ones typed finer than a microsecond.
    struct Described {
        columns: Vec<(&'static str, &'static str, RivetType)>,
        finer: Vec<String>,
        describe_fails: bool,
    }

    impl Described {
        fn of(columns: &[(&'static str, &'static str, RivetType)], finer: &[&str]) -> Self {
            Described {
                columns: columns.to_vec(),
                finer: finer.iter().map(|c| c.to_string()).collect(),
                describe_fails: false,
            }
        }
    }

    impl crate::source::Source for Described {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        fn query_scalar(&mut self, _sql: &str) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            overrides: &ColumnOverrides,
        ) -> anyhow::Result<Vec<crate::types::TypeMapping>> {
            Ok(self
                .columns
                .iter()
                .map(|(name, native, default)| {
                    crate::types::TypeMapping::from_source(
                        &crate::types::SourceColumn::simple(*name, *native, true),
                        overrides
                            .get(*name)
                            .cloned()
                            .unwrap_or_else(|| default.clone()),
                    )
                })
                .collect())
        }
        fn finer_than_microsecond_in_query(&mut self, _query: &str) -> anyhow::Result<Vec<String>> {
            if self.describe_fails {
                anyhow::bail!("describe refused");
            }
            Ok(self.finer.clone())
        }
    }

    fn timestamp(unit: crate::types::TimeUnit, tz: bool) -> RivetType {
        RivetType::Timestamp {
            unit,
            timezone: tz.then(|| "UTC".into()),
        }
    }

    /// A time override that keeps fewer fraction digits than its result column holds reads `lossy` on the batch and the stream probe, decided per result column of the query: an alias is lossy, a CAST to a coarser type is not.
    #[test]
    fn a_time_override_narrowing_a_result_column_is_lossy_on_batch_and_stream() {
        use crate::types::TimeUnit::{Microsecond, Millisecond, Nanosecond};
        let ns = || timestamp(Nanosecond, false);
        let verdict = |source: &mut Described, stream: bool, ov: &[(&str, RivetType)]| {
            let overrides: ColumnOverrides =
                ov.iter().map(|(c, t)| (c.to_string(), t.clone())).collect();
            probe_mappings(source, stream, "q", &overrides)
                .unwrap()
                .into_iter()
                .map(|m| (m.column_name, m.fidelity, m.warnings))
                .collect::<Vec<_>>()
        };
        for stream in [false, true] {
            let mut aliased = Described::of(&[("x", "datetime2", ns())], &["x"]);
            let got = verdict(
                &mut aliased,
                stream,
                &[("x", timestamp(Microsecond, false))],
            );
            assert_eq!(got[0].1, TypeFidelity::Lossy, "alias, stream={stream}");
            assert_eq!(
                got[0].2.len(),
                1,
                "one warning, stream={stream}: {:?}",
                got[0].2
            );

            let mut cast =
                Described::of(&[("d7", "datetime2", timestamp(Millisecond, false))], &[]);
            let got = verdict(&mut cast, stream, &[("d7", timestamp(Microsecond, false))]);
            assert_eq!(got[0].1, TypeFidelity::Exact, "cast, stream={stream}");
            assert!(got[0].2.is_empty(), "{:?}", got[0].2);

            let mut zoned = Described::of(
                &[("z", "datetimeoffset", timestamp(Nanosecond, true))],
                &["z"],
            );
            let got = verdict(&mut zoned, stream, &[("z", timestamp(Microsecond, true))]);
            assert_eq!(got[0].1, TypeFidelity::Lossy, "tz, stream={stream}");

            let mut kept = Described::of(
                &[
                    ("a", "datetime2", ns()),
                    ("b", "datetime2", ns()),
                    ("c", "datetime2", ns()),
                ],
                &["a", "b"],
            );
            let got = verdict(
                &mut kept,
                stream,
                &[
                    ("a", timestamp(Nanosecond, false)),
                    ("b", RivetType::String),
                    ("c", timestamp(Microsecond, false)),
                ],
            );
            assert_eq!(
                got.iter().map(|g| g.1).collect::<Vec<_>>(),
                vec![TypeFidelity::Exact; 3],
                "a nanosecond override, a text override and a column the source holds at microseconds are not narrowed, stream={stream}"
            );
        }
    }

    /// The reason a narrowing time override carries is pinned, and names a remedy that works on each engine that can hold such a column.
    #[test]
    fn a_narrowing_time_override_names_its_cause_and_a_remedy_that_works() {
        use crate::types::TimeUnit::{Microsecond, Nanosecond};
        let reason = override_narrows(
            &timestamp(Nanosecond, false),
            &timestamp(Microsecond, false),
            true,
        )
        .expect("narrowing");
        assert_eq!(
            reason,
            "the source column holds more than 6 fraction digits of a second and this override keeps 6 \
             — the finer digits are truncated at run; this is lossy, not exact. To keep them, drop the \
             override on SQL Server (`datetime2(7)` and `datetimeoffset(7)` keep nanoseconds by default) \
             or declare `timestamp_ns` / `timestamp_tz_ns` on Oracle"
        );
        for kept in [
            timestamp(Nanosecond, false),
            RivetType::String,
            RivetType::Int64,
        ] {
            assert_eq!(
                override_narrows(&timestamp(Nanosecond, false), &kept, true),
                None
            );
        }
        assert_eq!(
            override_narrows(
                &timestamp(Nanosecond, false),
                &timestamp(Microsecond, false),
                false
            ),
            None
        );
    }

    /// The describe is read only when an override keeps at most a microsecond, and a describe that fails is said in a warning, never read as `exact` in silence.
    #[test]
    fn the_describe_is_read_only_for_a_time_override_and_its_failure_is_said() {
        use crate::types::TimeUnit::{Microsecond, Nanosecond};
        let ns = || timestamp(Nanosecond, false);
        let probe = |source: &mut Described, ov: (&str, RivetType)| {
            let overrides: ColumnOverrides = [(ov.0.to_string(), ov.1)].into_iter().collect();
            probe_mappings(source, false, "q", &overrides).unwrap()
        };
        let mut asked = Described::of(&[("d", "datetime2", ns())], &["d"]);
        asked.describe_fails = true;
        for ov in [("d", RivetType::String), ("d", ns())] {
            let got = probe(&mut asked, ov);
            assert!(got[0].warnings.is_empty(), "{:?}", got[0].warnings);
        }
        let got = probe(&mut asked, ("d", timestamp(Microsecond, false)));
        assert_eq!(got[0].fidelity, TypeFidelity::Exact);
        assert!(
            got[0].warnings.len() == 1 && got[0].warnings[0].contains("describe refused"),
            "{:?}",
            got[0].warnings
        );
    }
}
