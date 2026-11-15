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
    let plan = crate::plan::build::build_plan_on(
        probe,
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
    let diags = crate::plan::validate_plan(&plan);
    if let Some(err) = log_plan_diagnostics(&plan.export_name, &diags) {
        return Err(err);
    }
    Ok(PlannedExport { plan, diags })
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
    plan_open_columns(&plan.export_name, &mut Some(mappings), strict)
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

/// Every selected batch export planned and admitted as `run` would, in run order, stopping before the first write.
pub(crate) fn plan_run(
    config: &Config,
    config_dir: &Path,
    export_name: Option<&str>,
    params: Option<&HashMap<String, String>>,
    flags: PlanFlags,
) -> Result<Vec<(String, Result<PlannedExport>)>> {
    let exports = select_exports(config, export_name, config_dir, params)?;
    let mut src = match crate::source::create_source(&config.source) {
        Ok(src) => Some(src),
        Err(e) if config.source.columns_from_grants() => {
            return Err(super::attach_connect_hint(e, &config.source));
        }
        Err(_) => None,
    };
    let mut planned = Vec::new();
    for export in exports.iter().filter(|e| e.mode != ExportMode::Cdc) {
        let one = plan_batch_export(
            src.as_deref_mut().map(|s| s as &mut dyn Source),
            config,
            export,
            config_dir,
            flags,
            params,
        )
        .and_then(|p| match src.as_deref_mut() {
            Some(s) => admit_columns(s, config, &p.plan, export, config_dir, params, flags.strict)
                .map(|()| p),
            None => Ok(p),
        });
        planned.push((export.name.clone(), one));
    }
    Ok(planned)
}

/// The refusal `rivet plan` raises for an export that cannot be sealed: a plan holding the `{partition}` token.
pub(crate) fn refuse_to_seal(plan: &ResolvedRunPlan) -> Result<()> {
    unexpanded_partition_error(plan).map_or(Ok(()), Err)
}
