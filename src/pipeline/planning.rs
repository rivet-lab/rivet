//! The decisions `run` takes before its first write, taken by `check`, `doctor` and `plan` through the same functions.

use std::collections::HashMap;
use std::path::Path;

use crate::config::{Config, ExportConfig, ExportMode};
use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::plan::validate::Diagnostic;
use crate::source::Source;

use super::job::{
    log_plan_diagnostics, plan_open_columns, settle_columns_are_temporal,
    unexpanded_partition_error,
};
use super::run_set::RunSet;

/// The run-only switches that change what a plan decides.
#[derive(Clone, Copy, Default)]
pub(crate) struct PlanFlags {
    pub validate: bool,
    pub reconcile: bool,
    pub resume: bool,
    pub strict: bool,
}

/// One batch export as run would plan it: its plan and the non-refusing diagnostics.
pub(crate) struct PlannedExport {
    pub plan: ResolvedRunPlan,
    pub diags: Vec<Diagnostic>,
}

/// The batch plan of one export, with a missing relation coded and a rejected plan refused: the one build `run` executes.
pub(crate) fn plan_batch_export(
    probe: Option<&mut dyn Source>,
    config: &Config,
    export: &ExportConfig,
    config_dir: &Path,
    flags: PlanFlags,
    params: Option<&HashMap<String, String>>,
) -> Result<PlannedExport> {
    let mut probe = probe;
    let plan = crate::plan::build::build_plan_on(
        probe.as_deref_mut().map(|s| s as &mut dyn Source),
        config,
        export,
        config_dir,
        flags.validate,
        flags.reconcile,
        flags.resume,
        params,
    )
    .map_err(|e| {
        crate::source::code_a_missing_relation(
            e,
            &export.name,
            export.table.as_deref(),
            export.query.as_deref(),
        )
    })?;
    refuse_text_the_stream_renders_otherwise(
        config,
        export,
        &plan,
        probe,
        crate::plan::build::connect_source,
    )?;
    let diags = crate::plan::validate_plan(&plan);
    if let Some(err) = log_plan_diagnostics(&plan.export_name, &diags) {
        return Err(err);
    }
    Ok(PlannedExport { plan, diags })
}

/// Refuses a CDC baseline leg that declares `text` over a column the server renders as text, which the change stream renders in its own form into the same `__changes` log; a source it cannot read is admitted with a warning.
fn refuse_text_the_stream_renders_otherwise(
    config: &Config,
    export: &ExportConfig,
    plan: &ResolvedRunPlan,
    probe: Option<&mut dyn Source>,
    connect: impl FnOnce(&crate::config::SourceConfig) -> Result<Box<dyn Source>>,
) -> Result<()> {
    use crate::types::override_table::{Engine, server_text_declared};
    let Some(parent) = export.snapshot_parent.as_deref() else {
        return Ok(());
    };
    if seeded!("cdc-leg-text-declaration", true, false) {
        return Ok(());
    }
    if plan.column_overrides.is_empty()
        || config.source.source_type != crate::config::SourceType::Postgres
    {
        return Ok(());
    }
    let mut own;
    let src: &mut dyn Source = match probe {
        Some(probe) => probe,
        None => match connect(&config.source) {
            Ok(connected) => {
                own = connected;
                own.as_mut()
            }
            Err(e) => {
                warn_text_declaration_unchecked(parent, "connect", &e);
                return Ok(());
            }
        },
    };
    let detected = match src.type_mappings(&plan.base_query, &crate::types::ColumnOverrides::new())
    {
        Ok(detected) => detected,
        Err(e) => {
            warn_text_declaration_unchecked(parent, "describe", &e);
            return Ok(());
        }
    };
    let Some(declared) = server_text_declared(Engine::Postgres, &detected, &plan.column_overrides)
    else {
        return Ok(());
    };
    let (column, native) = (&declared.column_name, &declared.source_native_type);
    crate::rivet_bail!(
        crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
        "export '{parent}': column '{column}' is declared text over a `{native}` column, which the \
         baseline leg would deliver as the server's own text while the change stream renders the \
         same cell in its own form, so one `__changes` log would hold two spellings of it; no row \
         was delivered. Remove `{column}` from `columns:`: the baseline and the stream then \
         both deliver the `{native}` column's own type."
    )
}

/// The warning for a baseline leg admitted because the source could not be read to check its text declarations.
fn warn_text_declaration_unchecked(parent: &str, step: &str, cause: &anyhow::Error) {
    log::warn!(
        "export '{parent}': the check of its `columns:` text declarations against the source did not run, the {step} failed: {}; the baseline leg is admitted unchecked, and a date, time, timestamp or array column declared `text` there would be delivered as the server's text while the change stream renders it in its own form",
        crate::redact::redact_error(cause)
    );
}

/// The refusals the source's own catalog decides before a write: an override the wire type cannot take, a missing relation, a strict or settle column.
pub(crate) fn admit_columns(
    src: &mut dyn Source,
    config: &Config,
    plan: &ResolvedRunPlan,
    export: &ExportConfig,
    config_dir: &Path,
    params: Option<&HashMap<String, String>>,
    strict: bool,
) -> Result<()> {
    crate::preflight::type_report::refuse_override_wire_mismatch(
        config,
        export,
        &plan.column_overrides,
        config_dir,
        params,
    )?;
    let mappings = match crate::preflight::type_report::probe_mappings(
        src,
        crate::preflight::type_report::is_stream(export),
        &plan.base_query,
        &plan.column_overrides,
    ) {
        Ok(m) => m,
        Err(e) => {
            let e = crate::source::code_a_missing_relation(
                e,
                &export.name,
                export.table.as_deref(),
                export.query.as_deref(),
            );
            return match crate::error::error_code(&e) {
                Some(_) => Err(e),
                None => Ok(()),
            };
        }
    };
    settle_columns_are_temporal(plan, Some(&mappings))?;
    plan_open_columns(plan, &mut Some(mappings), strict)
}

/// The exports `config` runs for `export_name` with every `partition_by` expanded, as `run` selects them.
pub(crate) fn select_exports(
    config: &Config,
    export_name: Option<&str>,
    config_dir: &Path,
    params: Option<&HashMap<String, String>>,
) -> Result<Vec<ExportConfig>> {
    RunSet::select(config, export_name)?.in_process(&config.source, config_dir, params)
}

/// One batch export, or one baseline leg of a CDC export, planned and then admitted against the shared source connection.
fn plan_and_admit(
    src: &mut std::result::Result<Box<dyn Source>, String>,
    config: &Config,
    export: &ExportConfig,
    config_dir: &Path,
    flags: PlanFlags,
    params: Option<&HashMap<String, String>>,
) -> Result<PlannedExport> {
    plan_batch_export(
        src.as_deref_mut().ok().map(|s| s as &mut dyn Source),
        config,
        export,
        config_dir,
        flags,
        params,
    )
    .and_then(|p| match src.as_deref_mut() {
        Ok(s) => {
            admit_columns(s, config, &p.plan, export, config_dir, params, flags.strict).map(|()| p)
        }
        Err(_)
            if p.plan.column_overrides.is_empty()
                || seeded!("check-unreachable-override-admitted", true, false) =>
        {
            Ok(p)
        }
        Err(cause) => Err(crate::error::coded(
            crate::error::codes::SOURCE_CONNECT_FAILED,
            format!(
                "export '{}' was not admitted: the source could not be reached, so its `columns:` overrides were not checked against the catalog: {cause}",
                export.name
            ),
        )),
    })
}

/// Every selected batch export, and with `cdc_baseline` every pending baseline leg of a selected CDC export, planned and admitted as `run` would, in run order, stopping before the first write.
pub(crate) fn plan_run(
    config: &Config,
    config_dir: &Path,
    export_name: Option<&str>,
    params: Option<&HashMap<String, String>>,
    flags: PlanFlags,
    cdc_baseline: bool,
) -> Result<Vec<(String, Result<PlannedExport>)>> {
    let exports = select_exports(config, export_name, config_dir, params)?;
    let mut src = match crate::source::create_source(&config.source) {
        Ok(src) => Ok(src),
        Err(e) if config.source.columns_from_grants() => {
            return Err(super::attach_connect_hint(e, &config.source));
        }
        Err(e) => Err(format!("{e:#}")),
    };
    let mut planned = Vec::new();
    for export in &exports {
        if export.mode != ExportMode::Cdc {
            let one = plan_and_admit(&mut src, config, export, config_dir, flags, params);
            planned.push((export.name.clone(), one));
            continue;
        }
        if !cdc_baseline || seeded!("check-skips-cdc-baseline-leg", true, false) {
            continue;
        }
        match super::cdc_job::pending_snapshot_legs(config, export) {
            Ok(legs) => {
                for leg in legs {
                    let one = plan_and_admit(&mut src, config, &leg, config_dir, flags, params);
                    planned.push((leg.name.clone(), one));
                }
            }
            Err(e) => planned.push((export.name.clone(), Err(e))),
        }
    }
    Ok(planned)
}

/// The refusal `rivet plan` raises for an export that cannot be sealed: a plan holding the `{partition}` token.
pub(crate) fn refuse_to_seal(plan: &ResolvedRunPlan) -> Result<()> {
    unexpanded_partition_error(plan).map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A source that answers the column probe with one Lossy column.
    struct LossySource;

    impl Source for LossySource {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> Result<()> {
            Ok(())
        }
        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>> {
            Ok(None)
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            use crate::types::{RivetType, SourceColumn, TypeFidelity, TypeMapping};
            let mut m = TypeMapping::from_source(
                &SourceColumn::simple("ts", "timestamp(9)", true),
                RivetType::String,
            );
            m.fidelity = TypeFidelity::Lossy;
            Ok(vec![m])
        }
    }

    /// A source that answers the column probe with the mappings it was given.
    struct ScriptedSource(Vec<crate::types::TypeMapping>);

    impl Source for ScriptedSource {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> Result<()> {
            Ok(())
        }
        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>> {
            Ok(None)
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            Ok(self.0.clone())
        }
    }

    fn one_export_config(source_type: &str, extra: &str) -> Config {
        Config::from_yaml(&format!(
            "source: {{ type: {source_type}, url: \"{source_type}://u:p@127.0.0.1:1/db\"{extra} }}\n\
             exports:\n\
             \x20 - name: e\n    table: t\n    mode: full\n    format: parquet\n\
             \x20   destination: {{ type: local, path: ./out }}\n"
        ))
        .expect("the fixture config loads")
    }

    /// The catalog admission refuses a Lossy column under `--strict` and admits it otherwise.
    #[test]
    fn admit_columns_refuses_a_lossy_column_only_under_strict() {
        let config = one_export_config("postgres", "");
        let plan = crate::plan::test_support::test_plan();
        let admit = |strict: bool| {
            admit_columns(
                &mut LossySource,
                &config,
                &plan,
                &config.exports[0],
                Path::new("."),
                None,
                strict,
            )
        };
        assert!(admit(false).is_ok());
        let err = admit(true).unwrap_err();
        assert!(format!("{err:#}").contains("ts"), "{err:#}");
    }

    /// The admission refuses, before any read, what `run` refuses by code: a column no Arrow type carries and a column the format cannot write; and admits the same columns where the run writes them.
    #[test]
    fn admit_columns_refuses_unmapped_and_unwritable_columns_and_admits_the_rest() {
        use crate::config::FormatType;
        use crate::types::{RivetType, SourceColumn, TypeMapping};
        let col = |name: &str, t: RivetType| {
            TypeMapping::from_source(&SourceColumn::simple(name, "native", true), t)
        };
        let unmapped = || {
            col(
                "net",
                RivetType::Unsupported {
                    native_type: "cidr".into(),
                    reason: "no arrow type".into(),
                },
            )
        };
        let wide = || {
            col(
                "n",
                RivetType::List {
                    inner: Box::new(RivetType::List {
                        inner: Box::new(RivetType::String),
                    }),
                },
            )
        };
        let config = one_export_config("postgres", "");
        let admit = |m: TypeMapping, format: FormatType, strict: bool| {
            let mut plan = crate::plan::test_support::test_plan();
            plan.format = format;
            admit_columns(
                &mut ScriptedSource(vec![m]),
                &config,
                &plan,
                &config.exports[0],
                Path::new("."),
                None,
                strict,
            )
        };
        for strict in [false, true] {
            for format in [FormatType::Parquet, FormatType::Csv] {
                let err = admit(unmapped(), format, strict).unwrap_err();
                assert_eq!(
                    crate::error::error_code(&err),
                    Some("RIVET_TYPE_UNSAFE_MAPPING")
                );
                assert!(
                    format!("{err:#}").contains("1 column(s) need a `columns:` declaration"),
                    "{err:#}"
                );
            }
            let err = admit(wide(), FormatType::Csv, strict).unwrap_err();
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED")
            );
            assert!(admit(wide(), FormatType::Parquet, strict).is_ok());
            assert!(admit(col("id", RivetType::Int64), FormatType::Csv, strict).is_ok());
        }
    }

    /// A source that cannot connect stops a `columns_from: grants` plan, refuses an export whose `columns:` it could not admit, and lets an export with no override go on.
    #[test]
    fn an_unreachable_source_refuses_an_unadmitted_override_and_not_a_plain_plan() {
        let dir = Path::new(".");
        let grants = one_export_config("mysql", ", columns_from: grants");
        let err = plan_run(&grants, dir, None, None, PlanFlags::default(), false)
            .err()
            .expect("a grants plan needs its catalog");
        assert!(format!("{err:#}").contains("127.0.0.1"), "{err:#}");
        let plain = one_export_config("mysql", "");
        let planned = plan_run(&plain, dir, None, None, PlanFlags::default(), false).unwrap();
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].0, "e");
        assert!(planned[0].1.is_ok());
        let mut over = plain.clone();
        over.exports[0].columns = [("id".to_string(), "int64".to_string())].into();
        let mut planned = plan_run(&over, dir, None, None, PlanFlags::default(), false).unwrap();
        let err = planned
            .remove(0)
            .1
            .err()
            .expect("an override is not a pass");
        crate::error::assert_coded(&err, "RIVET_SOURCE_CONNECT_FAILED", 2);
        assert!(format!("{err:#}").contains("127.0.0.1"), "{err:#}");
    }

    /// Sealing refuses a destination holding `{partition}` by code and passes an expanded one.
    #[test]
    fn refuse_to_seal_refuses_the_partition_token_and_passes_an_expanded_plan() {
        let plan = crate::plan::test_support::test_plan();
        assert!(refuse_to_seal(&plan).is_ok());
        for in_prefix in [false, true] {
            let mut p = plan.clone();
            if in_prefix {
                p.destination.prefix = Some("x/{partition}".into());
            } else {
                p.destination.path = Some("./out/{partition}".into());
            }
            let err = refuse_to_seal(&p).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_PLAN_PARTITION_TOKEN_UNEXPANDED", 1);
        }
    }

    fn declared_text_plan(column: &str) -> ResolvedRunPlan {
        let mut plan = crate::plan::test_support::test_plan();
        plan.column_overrides = [(column.to_string(), crate::types::RivetType::String)].into();
        plan
    }

    fn connect_to(
        native: &str,
        rivet_type: crate::types::RivetType,
    ) -> impl FnOnce(&crate::config::SourceConfig) -> Result<Box<dyn Source>> + '_ {
        move |_| {
            let column = crate::types::SourceColumn::simple("d", native, true);
            let mapping = crate::types::TypeMapping::from_source(&column, rivet_type);
            Ok(Box::new(ScriptedSource(vec![mapping])) as Box<dyn Source>)
        }
    }

    fn stream_leg(config: &Config) -> ExportConfig {
        let mut export = config.exports[0].clone();
        export.snapshot_parent = Some("parent".into());
        export
    }

    /// A CDC baseline leg declaring text over a PostgreSQL date is refused by code with the column, the native type and the remedy.
    #[test]
    fn a_baseline_leg_declaring_text_over_a_postgres_date_is_refused_with_the_remedy() {
        let config = one_export_config("postgres", "");
        let err = refuse_text_the_stream_renders_otherwise(
            &config,
            &stream_leg(&config),
            &declared_text_plan("d"),
            None,
            connect_to("date", crate::types::RivetType::Date),
        )
        .expect_err("a server-rendered date declared text");
        assert_eq!(
            crate::error::code_of(&err).map(|c| c.id),
            Some(crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH.id)
        );
        let text = format!("{err:#}");
        assert!(
            text.contains("export 'parent': column 'd' is declared text over a `date` column"),
            "{text}"
        );
        assert!(
            text.contains("no row was delivered. Remove `d` from `columns:`"),
            "{text}"
        );
    }

    fn run(
        config: &Config,
        export: &ExportConfig,
        plan: &ResolvedRunPlan,
        connect: impl FnOnce(&crate::config::SourceConfig) -> Result<Box<dyn Source>>,
    ) -> Result<()> {
        refuse_text_the_stream_renders_otherwise(config, export, plan, None, connect)
    }

    /// A source whose catalog describe fails with a connection string in the error.
    struct DescribeFails;

    impl Source for DescribeFails {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> Result<()> {
            Ok(())
        }
        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>> {
            Ok(None)
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            anyhow::bail!("describe refused at postgres://u:describe_pw@127.0.0.1:1/db")
        }
    }

    type ConnectFn = Box<dyn FnOnce(&crate::config::SourceConfig) -> Result<Box<dyn Source>>>;

    fn named_leg(config: &Config, parent: &str) -> ExportConfig {
        let mut export = stream_leg(config);
        export.snapshot_parent = Some(parent.into());
        export
    }

    fn warnings_of(parent: &str) -> Vec<String> {
        crate::pipeline::run::run_tail_tests::install_warn_capture();
        crate::pipeline::run::run_tail_tests::captured_warnings_mentioning(&format!(
            "export '{parent}'"
        ))
    }

    fn unreachable(_: &crate::config::SourceConfig) -> Result<Box<dyn Source>> {
        anyhow::bail!("connect refused at postgres://u:connect_pw@127.0.0.1:1/db")
    }

    /// A baseline leg whose source cannot be reached is admitted with one warning naming the export, the step, the masked cause and that the check did not run.
    #[test]
    fn an_unreachable_source_admits_the_baseline_leg_with_one_masked_warning() {
        crate::pipeline::run::run_tail_tests::install_warn_capture();
        let pg = one_export_config("postgres", "");
        let leg = named_leg(&pg, "warn_connect");
        assert!(run(&pg, &leg, &declared_text_plan("d"), unreachable).is_ok());
        let said = warnings_of("warn_connect");
        assert_eq!(said.len(), 1, "{said:?}");
        assert!(
            said[0].contains("the check of its `columns:` text declarations against the source did not run, the connect failed: connect refused at"),
            "{said:?}"
        );
        assert!(
            said[0].contains("the baseline leg is admitted unchecked"),
            "{said:?}"
        );
        assert!(!said[0].contains("connect_pw"), "{said:?}");
    }

    /// A catalog describe that fails admits the leg with one warning naming the describe step and masking the cause.
    #[test]
    fn a_failed_describe_admits_the_baseline_leg_with_one_masked_warning() {
        crate::pipeline::run::run_tail_tests::install_warn_capture();
        let pg = one_export_config("postgres", "");
        let leg = named_leg(&pg, "warn_describe");
        let admitted = run(&pg, &leg, &declared_text_plan("d"), |_| {
            Ok(Box::new(DescribeFails) as Box<dyn Source>)
        });
        assert!(admitted.is_ok());
        let said = warnings_of("warn_describe");
        assert_eq!(said.len(), 1, "{said:?}");
        assert!(
            said[0].contains("did not run, the describe failed: describe refused at"),
            "{said:?}"
        );
        assert!(!said[0].contains("describe_pw"), "{said:?}");
    }

    /// The connection the caller already holds answers the check, so no second connection is opened, and a failure on it warns the same way.
    #[test]
    fn the_callers_probe_answers_the_check_without_a_second_connection() {
        crate::pipeline::run::run_tail_tests::install_warn_capture();
        let pg = one_export_config("postgres", "");
        let column = crate::types::SourceColumn::simple("d", "date", true);
        let mapping =
            crate::types::TypeMapping::from_source(&column, crate::types::RivetType::Date);
        let mut probe = ScriptedSource(vec![mapping]);
        let err = refuse_text_the_stream_renders_otherwise(
            &pg,
            &named_leg(&pg, "probe_refuses"),
            &declared_text_plan("d"),
            Some(&mut probe),
            |_| panic!("a held probe must not open a second connection"),
        )
        .expect_err("a date declared text, read on the held probe");
        assert!(format!("{err:#}").contains("export 'probe_refuses': column 'd'"));
        let admitted = refuse_text_the_stream_renders_otherwise(
            &pg,
            &named_leg(&pg, "probe_warns"),
            &declared_text_plan("d"),
            Some(&mut DescribeFails),
            |_| panic!("a held probe must not open a second connection"),
        );
        assert!(admitted.is_ok());
        let said = warnings_of("probe_warns");
        assert_eq!(said.len(), 1, "{said:?}");
        assert!(said[0].contains("the describe failed"), "{said:?}");
    }

    /// Where the check has nothing to read, or reads and finds nothing, an unreachable source and a clean read stay silent.
    #[test]
    fn the_inputs_next_to_an_unchecked_leg_stay_silent() {
        crate::pipeline::run::run_tail_tests::install_warn_capture();
        let pg = one_export_config("postgres", "");
        let mysql = one_export_config("mysql", "");
        let cases: [(&str, &Config, ExportConfig, ResolvedRunPlan); 4] = [
            (
                "silent_no_parent",
                &pg,
                pg.exports[0].clone(),
                declared_text_plan("d"),
            ),
            (
                "silent_no_declaration",
                &pg,
                named_leg(&pg, "silent_no_declaration"),
                crate::plan::test_support::test_plan(),
            ),
            (
                "silent_not_postgres",
                &mysql,
                named_leg(&mysql, "silent_not_postgres"),
                declared_text_plan("d"),
            ),
            (
                "silent_clean_read",
                &pg,
                named_leg(&pg, "silent_clean_read"),
                declared_text_plan("other"),
            ),
        ];
        for (name, config, export, plan) in cases {
            let connect: ConnectFn = if name == "silent_clean_read" {
                Box::new(|_| Ok(Box::new(ScriptedSource(Vec::new())) as Box<dyn Source>))
            } else {
                Box::new(move |_| anyhow::bail!("connect refused for {name}"))
            };
            assert!(run(config, &export, &plan, connect).is_ok(), "{name}");
            let said = crate::pipeline::run::run_tail_tests::captured_warnings_mentioning(name);
            assert!(said.is_empty(), "{name}: {said:?}");
        }
    }

    /// The same declaration is admitted where the leg is not a baseline, nothing is declared, the source is not PostgreSQL, the column is not server-rendered text or the source cannot be reached.
    #[test]
    fn the_inputs_next_to_a_refused_baseline_declaration_are_admitted() {
        let date = || connect_to("date", crate::types::RivetType::Date);
        let pg = one_export_config("postgres", "");
        assert!(
            run(&pg, &pg.exports[0], &declared_text_plan("d"), date()).is_ok(),
            "no snapshot parent"
        );
        assert!(
            run(
                &pg,
                &stream_leg(&pg),
                &crate::plan::test_support::test_plan(),
                date(),
            )
            .is_ok(),
            "no declared column"
        );
        assert!(
            run(
                &pg,
                &stream_leg(&pg),
                &declared_text_plan("d"),
                connect_to("int4", crate::types::RivetType::Int32),
            )
            .is_ok(),
            "a column the server does not render as text"
        );
        assert!(
            run(&pg, &stream_leg(&pg), &declared_text_plan("other"), date()).is_ok(),
            "a declaration over another column"
        );
        assert!(
            run(&pg, &stream_leg(&pg), &declared_text_plan("d"), |_| {
                anyhow::bail!("unreachable")
            })
            .is_ok(),
            "an unreachable source"
        );
        let mysql = one_export_config("mysql", "");
        assert!(
            run(
                &mysql,
                &stream_leg(&mysql),
                &declared_text_plan("d"),
                date(),
            )
            .is_ok(),
            "a source that is not PostgreSQL"
        );
    }
}
