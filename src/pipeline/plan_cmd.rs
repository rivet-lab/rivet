//! **`rivet plan`** — build and display a `PlanArtifact` without executing.

use std::collections::HashMap;
use std::path::Path;

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::plan::{
    ComputedPlanData, ExportRecommendation, ExtractionStrategy, PlanArtifact, PlanDiagnostics,
    PlanPrioritizationSnapshot, PrioritizationInputs, build_plan,
    campaign::recommend_campaign,
    inputs::{PrioritizationHints, build_prioritization_inputs},
    recommend::recommend_export,
    validate::{DiagnosticLevel, validate_plan},
};
use crate::state::StateStore;
use crate::{preflight, source};

use super::chunked::{chunk_plan_fingerprint, detect_and_generate_chunks};
use super::run_set::RunSet;

/// Output format for `rivet plan`.
pub enum PlanOutputFormat {
    /// Human-readable summary printed to stdout; no file written.
    Pretty,
    /// Pretty-printed JSON written to the given path (or stdout if `None`).
    Json(Option<String>),
}

/// `--annotate-waves` packs a schedule across the WHOLE config, so it is a category error when scoped to a single `--export` (packing one export alone rewrites its wave to 1.
fn refuse_annotate_scoped_to_export(annotate_waves: bool, has_export: bool) -> bool {
    annotate_waves && has_export
}

/// Entry point for `rivet plan`.
fn plannable_exports(config: &Config, export_name: Option<&str>) -> Result<Vec<ExportConfig>> {
    let selected = RunSet::select(config, export_name)?;
    if export_name.is_some() {
        return Ok(selected.declared().to_vec());
    }
    let kept: std::collections::HashSet<&str> = selected
        .declared()
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    let recipes: std::collections::HashSet<String> = config
        .exports
        .iter()
        .filter(|e| !kept.contains(e.name.as_str()))
        .map(|e| e.name.clone())
        .collect();
    let mut plannable = Vec::new();
    for e in &config.exports {
        match crate::config::batch_plan_skip_reason(e, &recipes) {
            Some(why) => log::warn!("plan: skipped '{}' — {why}", e.name),
            None => plannable.push(e.clone()),
        }
    }
    if plannable.is_empty() {
        crate::rivet_bail!(
            crate::error::codes::PLAN_NOTHING_TO_PLAN,
            "nothing to plan: every export in the config is a CDC stream or a backfill \
             recipe (see the skips above) — run them with `rivet run`"
        );
    }
    Ok(plannable)
}

pub fn run_plan_command(
    config_path: &str,
    export_name: Option<&str>,
    params: Option<&HashMap<String, String>>,
    format: PlanOutputFormat,
    annotate_waves: bool,
    date: Option<chrono::NaiveDate>,
) -> Result<()> {
    if refuse_annotate_scoped_to_export(annotate_waves, export_name.is_some()) {
        crate::rivet_bail!(
            crate::error::codes::CLI_OPTION_UNSUPPORTED,
            "--annotate-waves schedules the WHOLE config (each export's wave is relative to              the others) and cannot be scoped to --export: packing one export alone would              rewrite its wave to 1. Drop --export to annotate the whole config, or drop              --annotate-waves to plan just this export."
        );
    }
    let config = Config::load_with_params(config_path, params)?;
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));

    let exports = plannable_exports(&config, export_name)?;

    let state_path = config_dir.join(".rivet_state.db");
    let state = StateStore::open(state_path.to_str().unwrap_or(".rivet_state.db"))?;

    let mut built: Vec<(PlanArtifact, PrioritizationInputs, ExportRecommendation)> = Vec::new();

    for export in &exports {
        built.push(build_plan_artifact(
            &config,
            export,
            config_path,
            config_dir,
            params,
            &state,
            date,
        )?);
    }

    let campaign_opt = if built.len() > 1 {
        let pairs: Vec<_> = built
            .iter()
            .map(|(_, i, r)| (i.clone(), r.clone()))
            .collect();
        Some(recommend_campaign(pairs))
    } else {
        None
    };

    let multi_export = built.len() > 1;

    let mut artifacts: Vec<PlanArtifact> = Vec::with_capacity(built.len());
    for (mut artifact, _inputs, standalone_rec) in built {
        let snap = if let Some(ref camp) = campaign_opt {
            let rec = camp
                .ordered_exports
                .iter()
                .find(|e| e.export_name == artifact.export_name)
                .cloned()
                .unwrap_or_else(|| standalone_rec.clone());
            PlanPrioritizationSnapshot {
                export_recommendation: rec,
                campaign: Some(camp.clone()),
            }
        } else {
            PlanPrioritizationSnapshot {
                export_recommendation: standalone_rec,
                campaign: None,
            }
        };
        artifact.prioritization = Some(snap);
        artifacts.push(artifact);
    }

    let recs: Vec<(String, u32, bool)> = artifacts
        .iter()
        .filter_map(|a| {
            a.prioritization.as_ref().map(|p| {
                let rec = &p.export_recommendation;
                let parallel_safe =
                    rec.cost_class == crate::plan::CostClass::Low && !rec.isolate_on_source;
                (a.export_name.clone(), rec.recommended_wave, parallel_safe)
            })
        })
        .collect();
    let cost_of: std::collections::HashMap<String, crate::plan::CostClass> = artifacts
        .iter()
        .filter_map(|a| {
            a.prioritization
                .as_ref()
                .map(|p| (a.export_name.clone(), p.export_recommendation.cost_class))
        })
        .collect();
    let isolate_of: std::collections::HashSet<String> = artifacts
        .iter()
        .filter_map(|a| {
            a.prioritization.as_ref().and_then(|p| {
                p.export_recommendation
                    .isolate_on_source
                    .then(|| a.export_name.clone())
            })
        })
        .collect();
    let recs = if annotate_waves {
        repack_from_history(recs, &cost_of, &isolate_of, &state)
    } else {
        recs
    };
    let fields = fields_to_write(&recs, annotate_waves);
    let written = if fields.is_empty() {
        std::collections::HashSet::new()
    } else {
        write_plan_fields_to_config(config_path, &fields)?
    };
    match plan_write_report(written.len(), annotate_waves, recs.len()) {
        PlanWriteReport::Wrote(n) => log::warn!(
            "plan --annotate-waves: (over)wrote wave/parallel_safe on {} export(s) in {}",
            n,
            config_path,
        ),
        PlanWriteReport::ReadOnly(n) => log::info!(
            "plan: read-only — scheduled {} export(s) but left {} unchanged; \
             rerun with --annotate-waves to write wave/parallel_safe into it",
            n,
            config_path,
        ),
        PlanWriteReport::Silent => {}
    }
    let unmatched: Vec<&String> = fields.keys().filter(|n| !written.contains(*n)).collect();
    if !unmatched.is_empty() {
        log::warn!(
            "plan: {} export(s) had recommendations but NO matching `- name:` line in {} — \
             nothing was written for them: {:?}",
            unmatched.len(),
            config_path,
            unmatched
        );
    }

    let safe_of = effective_parallel_safe(&recs, &config, &fields, &written);
    let wave_of = effective_wave(&recs, &config, &fields, &written);
    if pool_estimate_is_printable(&format, artifacts.len()) {
        print_pool_estimate(&artifacts, &safe_of, &state);
    }

    emit_artifacts(&artifacts, &format, multi_export, config_path, &wave_of)?;

    Ok(())
}

/// Derive a distinct per-export output path from the `--output` value when more than one export is being planned.
fn per_export_output_path(base: &str, export_name: &str) -> String {
    let sanitized: String = export_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let path = Path::new(base);
    let parent = path.parent();
    let stem = path.file_stem().and_then(|s| s.to_str());
    let ext = path.extension().and_then(|s| s.to_str());

    let file_name = match (stem, ext) {
        (Some(stem), Some(ext)) => format!("{stem}.{sanitized}.{ext}"),
        (Some(stem), None) => format!("{stem}.{sanitized}"),
        (None, _) => format!("{base}.{sanitized}"),
    };

    match parent {
        Some(dir) if !dir.as_os_str().is_empty() => {
            dir.join(file_name).to_string_lossy().into_owned()
        }
        _ => file_name,
    }
}

/// What `rivet plan` says about a `partition_by` export: its artifact is a preview that `apply` refuses.
fn partitioned_preview_note(export: &crate::config::ExportConfig) -> Option<String> {
    export.partition_by.as_ref().map(|_| {
        format!(
            "plan: '{}' is a `partition_by` export: its plan is a preview of the whole table, and \
             `rivet apply <plan-file>` refuses it — the per-partition exports exist only when the \
             config runs (`rivet apply <config.yaml>` or `rivet run`)",
            export.name
        )
    })
}

/// The failure `rivet plan` ends with when its preflight could not read the source: no artifact is sealed.
fn source_unreadable(export: &str, cause: anyhow::Error) -> anyhow::Error {
    cause.context(crate::error::CodedError::new(
        crate::error::codes::PLAN_SOURCE_UNREADABLE,
        format!(
            "plan '{export}': the source could not be read, so no plan was written (`rivet apply` \
             would fail on it)"
        ),
    ))
}

fn build_plan_artifact(
    config: &Config,
    export: &crate::config::ExportConfig,
    config_path: &str,
    config_dir: &Path,
    params: Option<&HashMap<String, String>>,
    state: &StateStore,
    date: Option<chrono::NaiveDate>,
) -> Result<(PlanArtifact, PrioritizationInputs, ExportRecommendation)> {
    let mut plan = build_plan(config, export, config_dir, false, false, false, params)?;
    if let Some(note) = partitioned_preview_note(export) {
        log::warn!("{note}");
    }

    let validate_diags = validate_plan(&plan);
    let mut validate_warnings: Vec<String> = Vec::new();
    for d in &validate_diags {
        match d.level {
            DiagnosticLevel::Rejected => {
                crate::rivet_bail!(
                    crate::error::codes::PLAN_REJECTED,
                    "[{}] {}",
                    d.rule,
                    d.message
                );
            }
            DiagnosticLevel::Warning | DiagnosticLevel::Degraded => {
                validate_warnings.push(format!("[{}] {}", d.rule, d.message));
            }
        }
    }

    let planned = crate::plan::build::export_as_planned(export, &plan.strategy);
    let (computed, plan_diagnostics, hints) = match preflight::get_export_diagnostic(
        config, &planned,
    ) {
        Ok(mut diag) => {
            crate::preflight::overlay_measured_rows(
                &mut diag,
                &planned,
                config.source.source_type,
                state,
            );
            let mut warnings: Vec<String> =
                diag.warnings.iter().map(|w| w.message.clone()).collect();
            warnings.extend(validate_warnings);
            if warnings.is_empty() && !matches!(diag.verdict, preflight::HealthVerdict::Efficient) {
                if let Some(s) = diag.suggestion.clone() {
                    warnings.push(s);
                } else {
                    warnings.push(format!(
                        "verdict {} but preflight collected no specific warnings — review `rivet check` output for context",
                        diag.verdict
                    ));
                }
            }
            let strategy_rationale = crate::plan::explain_strategy(&diag, &plan.strategy, export);
            let plan_diagnostics = PlanDiagnostics {
                verdict: diag.verdict.to_string(),
                warnings,
                recommended_profile: diag.recommended_profile.to_string(),
                strategy_rationale,
            };
            let row_is_measured = diag
                .row_source
                .as_deref()
                .is_some_and(|s| s.starts_with("measured"));
            let computed = compute_plan_data(&plan, diag.row_estimate, row_is_measured, state)?;
            let hints = PrioritizationHints {
                incremental_uses_index: diag.uses_index,
                cursor_range_observed: diag.cursor_min.is_some() && diag.cursor_max.is_some(),
            };
            (computed, plan_diagnostics, hints)
        }
        Err(e) => return Err(source_unreadable(&export.name, e)),
    };

    let fingerprint = match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => chunk_plan_fingerprint(
            &plan.base_query,
            &cp.column,
            cp.chunk_size,
            cp.chunk_count,
            cp.by_days,
        ),
        _ => String::new(),
    };

    let history = match state.get_metrics(Some(&export.name), 20) {
        Ok(metrics) => Some(crate::plan::HistorySnapshot::summarize(&metrics)),
        Err(e) => {
            log::warn!(
                "plan '{}': history lookup failed; proceeding without historical refinement: {:#}",
                export.name,
                e
            );
            None
        }
    };

    let inputs =
        build_prioritization_inputs(export, &plan, &computed, &plan_diagnostics, hints, history);
    let recommendation = recommend_export(&inputs, &plan_diagnostics);

    let ctx = match date {
        Some(d) => {
            crate::destination::placeholder::PlaceholderContext::for_date(d, export.family())
        }
        None => crate::destination::placeholder::PlaceholderContext::keeping_date(export.family()),
    };
    (plan.destination, plan.destination_written) =
        crate::destination::placeholder::resolve_destination(export.destination.clone(), &ctx);

    let mut artifact = PlanArtifact::new(
        plan.export_name.clone(),
        plan.strategy.mode_label().to_string(),
        fingerprint,
        plan,
        computed,
        plan_diagnostics,
    );

    artifact.config_path = Some(
        Path::new(config_path)
            .canonicalize()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| config_path.to_string()),
    );

    Ok((artifact, inputs, recommendation))
}

/// Below this span the catalog is not allowed to override it.
const CATALOG_OVERRIDE_MIN_SPAN: i64 = 10_000;

/// The row estimate a chunked plan reports, from the two available signals.
fn chunked_row_estimate(
    key_span: Option<i64>,
    catalog: Option<i64>,
    measured: bool,
    span_is_days: bool,
) -> Option<i64> {
    if span_is_days {
        return catalog;
    }
    if measured {
        return catalog.or(key_span);
    }
    match (key_span, catalog) {
        (Some(span), Some(cat)) if span >= CATALOG_OVERRIDE_MIN_SPAN => Some(span.max(cat)),
        (Some(span), Some(_)) => Some(span),
        (a, b) => a.or(b),
    }
}

/// The chunked key SPAN itself: how many ordinals the detector's first..last boundaries cover, INCLUSIVE, floored at zero.
fn chunked_key_span(first_lo: i64, last_hi: i64) -> i64 {
    crate::config::quantity::Knob::inclusive_count(first_lo, last_hi)
}

/// Compute the `ComputedPlanData` portion of the artifact.
fn compute_plan_data(
    plan: &crate::plan::ResolvedRunPlan,
    row_estimate: Option<i64>,
    row_is_measured: bool,
    state: &StateStore,
) -> Result<ComputedPlanData> {
    match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => {
            let mut src = source::create_source(&plan.source)?;
            let chunk_ranges = detect_and_generate_chunks(
                &mut *src,
                &plan.base_query,
                &cp.column,
                cp.chunk_size,
                cp.chunk_count,
                &plan.export_name,
                cp.by_days,
                plan.source.source_type,
            )?;
            let chunk_count = chunk_ranges.len();
            let chunked_estimate = chunk_ranges
                .first()
                .zip(chunk_ranges.last())
                .map(|(first, last)| chunked_key_span(first.0, last.1));
            let span_is_days = cp.by_days.is_some();
            Ok(ComputedPlanData {
                chunk_ranges,
                chunk_count,
                cursor_snapshot: None,
                row_estimate: chunked_row_estimate(
                    chunked_estimate,
                    row_estimate,
                    row_is_measured,
                    span_is_days,
                ),
            })
        }

        ExtractionStrategy::Incremental(_) => {
            state.refuse_respelled_source(&plan.progress_key())?;
            let cursor_snapshot = state
                .get(&plan.export_name, &plan.source.state_key())?
                .last_cursor_value;
            Ok(ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot,
                row_estimate,
            })
        }

        ExtractionStrategy::Snapshot
        | ExtractionStrategy::TimeWindow { .. }
        | ExtractionStrategy::Keyset(_) => Ok(ComputedPlanData {
            chunk_ranges: vec![],
            chunk_count: 0,
            cursor_snapshot: None,
            row_estimate,
        }),
    }
}

/// Print the pool scheduler's predicted makespan (#166) beside the wave plan, from the SAME predictor `apply --pool` schedules with ([`super::pool::predict_items`]).
fn pool_estimate_is_printable(format: &PlanOutputFormat, exports: usize) -> bool {
    exports > 1 && !matches!(format, PlanOutputFormat::Json(None))
}

fn print_pool_estimate(
    artifacts: &[PlanArtifact],
    safe_of: &std::collections::HashMap<String, bool>,
    state: &StateStore,
) {
    let predicted = super::pool::predict_items(
        state,
        artifacts.iter().map(|a| {
            (
                a.export_name.as_str(),
                safe_of.get(&a.export_name).copied().unwrap_or(false),
            )
        }),
        &std::collections::HashMap::new(),
    );
    let items: Vec<super::pool::PoolItem> = predicted.iter().map(|(i, _)| i.clone()).collect();
    let classified: Vec<super::pool::PredictedFrom> =
        predicted.iter().map(|(_, f)| f.clone()).collect();
    let (measured, attempt_n, placeholder_n) = super::pool::classification_counts(&classified);
    let estimated = attempt_n + placeholder_n;
    let total: f64 = items.iter().map(|i| i.predicted_secs).sum();
    println!(
        "\n  Pool scheduler ({measured} measured, {estimated} estimated of {} export(s)):",
        predicted.len(),
    );
    println!("    sequential: {:.0} min", total / 60.0);
    for m in [2usize, 4, 6] {
        let mk = super::pool::predicted_makespan_secs(&items, m);
        let floor = super::pool::makespan_floor_secs(&items, m);
        println!(
            "    pool M={m}: {:.0} min  (floor max(longest,total/M) = {:.0})",
            mk / 60.0,
            floor / 60.0
        );
    }
    if let Some(hedge) = super::pool::lower_bound_hedge(attempt_n, placeholder_n) {
        println!("    {hedge}");
    }
    println!(
        "    (a bounded work-stealing pool; --annotate-waves packs the wave \
         alternative — see #166)"
    );
}

fn emit_artifacts(
    artifacts: &[PlanArtifact],
    format: &PlanOutputFormat,
    multi_export: bool,
    config_path: &str,
    wave_of: &HashMap<String, Option<u32>>,
) -> Result<()> {
    match format {
        PlanOutputFormat::Pretty => {
            if multi_export {
                print_compact_summary(artifacts, config_path, wave_of);
            } else {
                for artifact in artifacts {
                    artifact.print_summary();
                }
            }
        }
        PlanOutputFormat::Json(None) => {
            if artifacts.len() > 1 {
                println!("{}", serde_json::to_string_pretty(artifacts)?);
            } else if let Some(artifact) = artifacts.first() {
                println!("{}", artifact.to_json_pretty()?);
            }
        }
        PlanOutputFormat::Json(Some(path)) => {
            for artifact in artifacts {
                let json = artifact.to_json_pretty()?;
                let out_path = if multi_export {
                    per_export_output_path(path, &artifact.export_name)
                } else {
                    path.clone()
                };
                std::fs::write(&out_path, &json).map_err(|e| {
                    crate::error::coded(
                        crate::error::codes::CLI_OUTPUT_UNWRITABLE,
                        format!("cannot write plan file '{}': {}", out_path, e),
                    )
                })?;
                println!("Plan written to: {}", out_path);
            }
        }
    }
    Ok(())
}

/// Compact one-line-per-export table for a multi-export plan, sorted by the `wave:` apply reads (then by descending score, then name).
fn print_compact_summary(
    artifacts: &[PlanArtifact],
    config_path: &str,
    wave_of: &HashMap<String, Option<u32>>,
) {
    let wave = |a: &PlanArtifact| wave_of.get(&a.export_name).copied().flatten();
    let key = |a: &PlanArtifact| {
        let score = a
            .prioritization
            .as_ref()
            .map_or(0, |p| p.export_recommendation.priority_score);
        (wave(a).unwrap_or(u32::MAX), score)
    };
    let mut order: Vec<&PlanArtifact> = artifacts.iter().collect();
    order.sort_by(|a, b| {
        let (wa, sa) = key(a);
        let (wb, sb) = key(b);
        wa.cmp(&wb)
            .then(sb.cmp(&sa))
            .then(a.export_name.cmp(&b.export_name))
    });
    let name_w = order
        .iter()
        .map(|a| a.export_name.chars().count())
        .max()
        .unwrap_or(6)
        .clamp(6, 32);

    println!();
    let waves: Vec<Option<u32>> = artifacts.iter().map(wave).collect();
    println!("{}", apply_order_line(config_path, &waves));
    println!();
    println!("{}", PlanArtifact::summary_header(name_w));
    for a in &order {
        println!("{}", a.summary_line(name_w, wave(a)));
    }
    println!();
    println!("  Full detail for one export:  rivet plan -c <config> --export <name>");
}

/// Per-export YAML fields that `plan` records in place (`wave`, `parallel_safe`), keyed by export name → ordered `(field, value)` pairs.
type ExportFields = HashMap<String, Vec<(&'static str, String)>>;

/// Replace the cost model's `recommended_wave` with waves PACKED from measured run history (`--annotate-waves` only).
fn repack_from_history(
    recs: Vec<(String, u32, bool)>,
    cost_of: &std::collections::HashMap<String, crate::plan::CostClass>,
    isolate_of: &std::collections::HashSet<String>,
    state: &crate::state::StateStore,
) -> Vec<(String, u32, bool)> {
    let (items, measured_n) = history_pack_items(&recs, cost_of, state);
    let waves = crate::plan::waves::pack(&items, 4096);
    log::warn!(
        "plan --annotate-waves: packed {} export(s) into {} wave(s) from run history \
         ({} measured, {} estimated — an estimate orders the first run only)",
        items.len(),
        waves.len(),
        measured_n,
        items.len() - measured_n
    );
    let mut wave_of: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    for w in &waves {
        for m in &w.members {
            wave_of.insert(m.clone(), w.number);
        }
    }
    recs.into_iter()
        .map(|(name, rec_wave, _)| {
            let w = wave_of.get(&name).copied().unwrap_or(rec_wave);
            let ps = !isolate_of.contains(&name);
            (name, w, ps)
        })
        .collect()
}

/// The packer's input, plus the count the operator-facing label reports: `(items, measured_n)`.
fn history_pack_items(
    recs: &[(String, u32, bool)],
    cost_of: &std::collections::HashMap<String, crate::plan::CostClass>,
    state: &crate::state::StateStore,
) -> (Vec<crate::plan::waves::PackItem>, usize) {
    let mut measured_n = 0usize;
    let items: Vec<crate::plan::waves::PackItem> = recs
        .iter()
        .map(|(name, rec_wave, _)| {
            let tier = *rec_wave;
            let (from, peak_rss) = super::pool::predict_secs_and_rss(state, name);
            if matches!(from, super::pool::PredictedFrom::Measured(_)) {
                measured_n += 1;
            }
            let (secs, rss) = (from.secs(), peak_rss.unwrap_or(150));
            crate::plan::waves::PackItem {
                name: name.clone(),
                tier,
                cost_class: cost_of
                    .get(name)
                    .copied()
                    .unwrap_or(crate::plan::CostClass::Medium),
                predicted_secs: secs,
                peak_rss_mb: rss,
            }
        })
        .collect();
    (items, measured_n)
}

/// Which of the plan's recommendations get WRITTEN into the operator's config.
fn fields_to_write(recs: &[(String, u32, bool)], annotate_waves: bool) -> ExportFields {
    if !annotate_waves {
        return HashMap::new();
    }
    recs.iter()
        .map(|(name, wave, parallel_safe)| {
            (
                name.clone(),
                vec![
                    ("wave", wave.to_string()),
                    ("parallel_safe", parallel_safe.to_string()),
                ],
            )
        })
        .collect()
}

/// What `run_plan_command` should tell the operator about config writes.
#[derive(Debug, PartialEq, Eq)]
enum PlanWriteReport {
    /// `--annotate-waves` (over)wrote N exports — warn, config was mutated.
    Wrote(usize),
    /// Read-only: N exports were scheduled but the config was left untouched.
    ReadOnly(usize),
    /// Nothing to say: an annotate run whose writes all failed to match a `- name:` line (the separate `unmatched` warning covers it), or an empty plan.
    Silent,
}

/// Decide the write-report from the outcome.
fn plan_write_report(written: usize, annotate_waves: bool, recs: usize) -> PlanWriteReport {
    if written > 0 {
        PlanWriteReport::Wrote(written)
    } else if !annotate_waves && recs > 0 {
        PlanWriteReport::ReadOnly(recs)
    } else {
        PlanWriteReport::Silent
    }
}

/// What `apply --pool` will read for `parallel_safe` on each export AFTER this plan run finished writing.
fn effective_parallel_safe(
    recs: &[(String, u32, bool)],
    config: &Config,
    fields: &ExportFields,
    written: &std::collections::HashSet<String>,
) -> HashMap<String, bool> {
    recs.iter()
        .map(|(name, _, rec_safe)| {
            let wrote_safe = written.contains(name)
                && fields
                    .get(name)
                    .is_some_and(|items| items.iter().any(|(k, _)| *k == "parallel_safe"));
            let effective = if wrote_safe {
                *rec_safe
            } else {
                config
                    .exports
                    .iter()
                    .find(|e| &e.name == name)
                    .and_then(|e| e.parallel_safe)
                    .unwrap_or(false)
            };
            (name.clone(), effective)
        })
        .collect()
}

/// The `wave:` each export carries on disk after this run.
fn effective_wave(
    recs: &[(String, u32, bool)],
    config: &Config,
    fields: &ExportFields,
    written: &std::collections::HashSet<String>,
) -> HashMap<String, Option<u32>> {
    recs.iter()
        .map(|(name, rec_wave, _)| {
            let wrote = written.contains(name)
                && fields
                    .get(name)
                    .is_some_and(|items| items.iter().any(|(k, _)| *k == "wave"));
            let on_disk = if wrote {
                Some(*rec_wave)
            } else {
                config
                    .exports
                    .iter()
                    .find(|e| &e.name == name)
                    .and_then(|e| e.wave)
            };
            (name.clone(), on_disk)
        })
        .collect()
}

/// The table's headline: what `rivet apply` does with these `wave:` values.
fn apply_order_line(config_path: &str, waves: &[Option<u32>]) -> String {
    let n = waves.len();
    let unset = waves.iter().filter(|w| w.is_none()).count();
    if unset == n {
        format!(
            "  Plan: {n} exports — {config_path} sets no `wave:`, so `rivet apply {config_path}` \
             runs them as one unscheduled group (Rec = the cost model's advice; \
             `--annotate-waves` writes a schedule)"
        )
    } else if unset == 0 {
        format!(
            "  Plan: {n} exports — `rivet apply {config_path}` runs them by `wave:` (lowest first)"
        )
    } else {
        format!(
            "  Plan: {n} exports — `rivet apply {config_path}` runs them by `wave:` (lowest \
             first); the {unset} with no `wave:` run last as one unscheduled group"
        )
    }
}

fn write_plan_fields_to_config(
    config_path: &str,
    fields: &ExportFields,
) -> Result<std::collections::HashSet<String>> {
    if fields.is_empty() {
        return Ok(std::collections::HashSet::new());
    }
    let original = std::fs::read_to_string(config_path).map_err(|e| {
        crate::error::coded(
            crate::error::codes::CONFIG_FILE_UNREADABLE,
            format!(
                "cannot read config '{}' to record plan fields: {}",
                config_path, e
            ),
        )
    })?;
    let (updated, written) = apply_field_annotations(&original, fields);
    if updated != original {
        std::fs::write(config_path, updated).map_err(|e| {
            crate::error::coded(
                crate::error::codes::CLI_OUTPUT_UNWRITABLE,
                format!(
                    "cannot write plan fields to config '{}': {}",
                    config_path, e
                ),
            )
        })?;
    }
    Ok(written)
}

/// Pure core of [`write_plan_fields_to_config`] (text in → text out, unit-tested).
fn apply_field_annotations(
    yaml: &str,
    fields: &ExportFields,
) -> (String, std::collections::HashSet<String>) {
    let mut out = String::with_capacity(yaml.len() + 64);
    let mut written: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut current: Option<(String, usize, usize)> = None;

    for line in yaml.split_inclusive('\n') {
        let content = line.strip_suffix('\n').unwrap_or(line);

        if let Some((dash_indent, name)) = parse_export_name(content) {
            out.push_str(line);
            let field_indent = dash_indent + 2;
            if let Some(items) = fields.get(&name) {
                for (key, value) in items {
                    out.push_str(&" ".repeat(field_indent));
                    out.push_str(&format!("{key}: {value}\n"));
                }
                written.insert(name.clone());
            }
            current = Some((name, dash_indent, field_indent));
            continue;
        }

        if let Some((name, dash_indent, field_indent)) = current.as_ref() {
            let trimmed = content.trim_start();
            let indent = content.len() - trimmed.len();
            let blank_or_comment = trimmed.is_empty() || trimmed.starts_with('#');
            if !blank_or_comment && indent <= *dash_indent {
                current = None;
            } else if indent == *field_indent
                && let Some(items) = fields.get(name)
                && items
                    .iter()
                    .any(|(key, _)| trimmed.starts_with(&format!("{key}:")))
            {
                continue;
            }
        }
        out.push_str(line);
    }
    (out, written)
}

/// Parse a `<indent>- name: <name>` export list item → `(indent_of_dash, name)`.
fn parse_export_name(line: &str) -> Option<(usize, String)> {
    let trimmed = line.trim_start();
    let dash_indent = line.len() - trimmed.len();
    let rest = trimmed.strip_prefix("- ")?;
    let value = rest.trim_start().strip_prefix("name:")?;
    let value = value.split(" #").next().unwrap_or(value);
    let name = value
        .trim()
        .trim_matches(|c: char| c == '"' || c == '\'')
        .to_string();
    (!name.is_empty()).then_some((dash_indent, name))
}

#[cfg(test)]
mod tests {
    use super::per_export_output_path;
    use std::path::Path;

    use super::{
        ExportFields, PlanWriteReport, apply_field_annotations, apply_order_line,
        effective_parallel_safe, effective_wave, fields_to_write, history_pack_items,
        plan_write_report, refuse_annotate_scoped_to_export, repack_from_history,
    };

    /// `plan` says a `partition_by` export's artifact is a preview `apply` refuses, and says nothing otherwise.
    #[test]
    fn plan_notes_that_a_partitioned_exports_artifact_cannot_be_applied() {
        let cfg = crate::config::Config::from_yaml(
            "source: { type: postgres, url: \"postgresql://u:p@127.0.0.1:1/db\" }\n\
             exports:\n\
             \x20 - name: events\n    table: events\n    mode: full\n    format: parquet\n\
             \x20   partition_by: created_at\n\
             \x20   destination: { type: local, path: \"./out/{partition}\" }\n\
             \x20 - name: plain\n    table: plain\n    mode: full\n    format: parquet\n\
             \x20   destination: { type: local, path: ./out/plain }\n",
        )
        .expect("the fixture config loads");
        assert_eq!(
            super::partitioned_preview_note(&cfg.exports[0]).as_deref(),
            Some(
                "plan: 'events' is a `partition_by` export: its plan is a preview of the whole \
                 table, and `rivet apply <plan-file>` refuses it — the per-partition exports exist \
                 only when the config runs (`rivet apply <config.yaml>` or `rivet run`)"
            )
        );
        assert_eq!(super::partitioned_preview_note(&cfg.exports[1]), None);
    }

    /// The write-report the operator sees, per outcome.
    #[test]
    fn plan_write_report_maps_outcome_to_the_right_operator_message() {
        assert_eq!(plan_write_report(3, true, 3), PlanWriteReport::Wrote(3));
        assert_eq!(plan_write_report(1, false, 5), PlanWriteReport::Wrote(1));
        assert_eq!(plan_write_report(0, false, 4), PlanWriteReport::ReadOnly(4));
        assert_eq!(plan_write_report(0, true, 4), PlanWriteReport::Silent);
        assert_eq!(plan_write_report(0, false, 0), PlanWriteReport::Silent);
        assert_eq!(plan_write_report(0, true, 0), PlanWriteReport::Silent);
    }

    /// The chunked key span is INCLUSIVE of both boundaries, and floored at 0.
    #[test]
    fn chunked_key_span_counts_both_boundaries_and_never_goes_negative() {
        use super::chunked_key_span;
        assert_eq!(chunked_key_span(1, 30), 30);
        assert_eq!(chunked_key_span(100, 100), 1, "one ordinal is one row");
        assert_eq!(chunked_key_span(-5, 5), 11);
        assert_eq!(chunked_key_span(950_000_000, 1_290_000_000), 340_000_001);
        assert_eq!(chunked_key_span(10, 5), 0);
        assert_eq!(chunked_key_span(i64::MIN, i64::MAX), i64::MAX);
    }

    /// The "N measured, M estimated" label counts MEASUREMENTS.
    #[test]
    fn the_packer_counts_only_successes_as_measured() {
        let state = crate::state::StateStore::open_in_memory().expect("state");
        let rec = |name: &str, run: &str, ms: i64, status: &str| {
            state
                .record_metric_full(&crate::state::MetricRow {
                    export_name: name.to_string(),
                    run_id: run.to_string(),
                    duration_ms: ms,
                    total_rows: 10,
                    peak_rss_mb: Some(120),
                    status: status.to_string(),
                    files_produced: 1,
                    bytes_written: 100,
                    ..Default::default()
                })
                .expect("record");
        };
        rec("ok_a", "a1", 900_000, "success");
        rec("ok_b", "b1", 600_000, "success");
        rec("never", "n1", 300_000, "failed");
        let recs: Vec<(String, u32, bool)> = ["ok_a", "ok_b", "never", "fresh"]
            .iter()
            .map(|n| ((*n).to_string(), 2u32, false))
            .collect();
        let cost_of = recs
            .iter()
            .map(|(n, _, _)| (n.clone(), crate::plan::CostClass::High))
            .collect();
        let (items, measured_n) = history_pack_items(&recs, &cost_of, &state);
        assert_eq!(items.len(), 4, "fixture: every rec becomes a pack item");
        assert_eq!(
            measured_n, 2,
            "only the two exports with a SUCCESS are measurements; the \
             failed-attempt floor and the history-less export are estimates"
        );
    }

    /// The refuse-guard fires ONLY when BOTH `--annotate-waves` and `--export` are set.
    #[test]
    fn refuse_annotate_scoped_to_export_only_when_both_set() {
        assert!(
            refuse_annotate_scoped_to_export(true, true),
            "both set → refuse"
        );
        assert!(
            !refuse_annotate_scoped_to_export(true, false),
            "annotate whole config → allow"
        );
        assert!(
            !refuse_annotate_scoped_to_export(false, true),
            "plain --export → allow (|| would wrongly refuse this)"
        );
        assert!(
            !refuse_annotate_scoped_to_export(false, false),
            "neither → allow"
        );
    }

    /// The packer's duration must come from the REAL producer.
    #[test]
    fn repack_reads_durations_from_the_state_stores_recorder() {
        let state = crate::state::StateStore::open_in_memory().expect("state");
        for (name, ms) in [("slowpoke", 90_000i64), ("quickie", 1_000i64)] {
            state
                .record_metric_full(&crate::state::MetricRow {
                    export_name: name.to_string(),
                    run_id: format!("{name}_run"),
                    duration_ms: ms,
                    total_rows: 10,
                    peak_rss_mb: Some(120),
                    status: "success".to_string(),
                    error_message: None,
                    tuning_profile: None,
                    format: None,
                    mode: None,
                    files_produced: 1,
                    bytes_written: 100,
                    retries: 0,
                    validated: None,
                    schema_changed: None,
                    ..Default::default()
                })
                .expect("record");
        }
        state
            .record_metric_full(&crate::state::MetricRow {
                export_name: "slowpoke".to_string(),
                run_id: "slowpoke_failed".to_string(),
                duration_ms: 5,
                total_rows: 0,
                peak_rss_mb: Some(50),
                status: "failed".to_string(),
                error_message: Some("boom".to_string()),
                tuning_profile: None,
                format: None,
                mode: None,
                files_produced: 0,
                bytes_written: 0,
                retries: 0,
                validated: None,
                schema_changed: None,
                ..Default::default()
            })
            .expect("record failed");

        let recs = vec![
            ("slowpoke".to_string(), 2, false),
            ("quickie".to_string(), 2, false),
            ("newcomer".to_string(), 2, false),
        ];
        let cost_of = recs
            .iter()
            .map(|(n, _, _)| (n.clone(), crate::plan::CostClass::High))
            .collect();

        let packed = repack_from_history(recs, &cost_of, &std::collections::HashSet::new(), &state);
        let wave = |n: &str| packed.iter().find(|(m, _, _)| m == n).unwrap().1;
        assert!(
            wave("slowpoke") < wave("newcomer") || wave("slowpoke") == wave("newcomer"),
            "measured 90s must not sort below a 5s placeholder"
        );
        assert_eq!(wave("slowpoke"), 1, "the measured-heaviest opens the tier");
        assert!(packed.iter().all(|(_, _, ps)| *ps));
        let w1: Vec<&str> = packed
            .iter()
            .filter(|(_, w, _): &&(String, u32, bool)| *w == 1)
            .map(|(n, _, _)| n.as_str())
            .collect();
        assert!(w1.contains(&"slowpoke"), "w1={w1:?}");
    }

    /// `plan --annotate-waves` and `apply --pool` must not disagree about a CHRONICALLY-FAILING export.
    #[test]
    fn repack_measures_an_export_that_has_failed_past_the_recent_window() {
        let state = crate::state::StateStore::open_in_memory().expect("state");
        let rec = |name: &str, run: &str, ms: i64, status: &str| {
            state
                .record_metric_full(&crate::state::MetricRow {
                    export_name: name.to_string(),
                    run_id: run.to_string(),
                    duration_ms: ms,
                    total_rows: 10,
                    peak_rss_mb: Some(120),
                    status: status.to_string(),
                    files_produced: 1,
                    bytes_written: 100,
                    ..Default::default()
                })
                .expect("record");
        };
        rec("chronic", "chronic_ok", 900_000, "success");
        for i in 0..30 {
            rec("chronic", &format!("chronic_bad{i}"), 60_000, "failed");
        }
        rec("steady", "steady_ok", 600_000, "success");
        rec("mid", "mid_ok", 300_000, "success");
        rec("tiny", "tiny_ok", 10_000, "success");

        let recs: Vec<(String, u32, bool)> = ["chronic", "steady", "mid", "tiny"]
            .iter()
            .map(|n| ((*n).to_string(), 2u32, false))
            .collect();
        let cost_of = recs
            .iter()
            .map(|(n, _, _)| (n.clone(), crate::plan::CostClass::High))
            .collect();
        let packed = repack_from_history(recs, &cost_of, &std::collections::HashSet::new(), &state);
        let wave = |n: &str| packed.iter().find(|(m, _, _)| m == n).unwrap().1;
        assert_eq!(
            wave("chronic"),
            1,
            "the 900 s success is still this export's prediction, 30 failures later"
        );
        assert_eq!(wave("steady"), 1, "fixture: K=2 pairs the two heaviest");
        assert_eq!((wave("mid"), wave("tiny")), (2, 2), "…and the two lightest");
    }

    /// The NEVER-succeeded export: `--annotate-waves` must schedule it from the same failed-attempt tier `apply --pool` uses, and must charge the wave budget its real peak RSS.
    #[test]
    fn repack_predicts_an_export_that_has_never_succeeded_from_its_failed_attempts() {
        let state = crate::state::StateStore::open_in_memory().expect("state");
        let rec = |name: &str, run: &str, ms: i64, rss: i64, status: &str| {
            state
                .record_metric_full(&crate::state::MetricRow {
                    export_name: name.to_string(),
                    run_id: run.to_string(),
                    duration_ms: ms,
                    total_rows: 10,
                    peak_rss_mb: Some(rss),
                    status: status.to_string(),
                    files_produced: 1,
                    bytes_written: 100,
                    ..Default::default()
                })
                .expect("record");
        };
        rec("doomed", "d1", 1_200_000, 900, "failed");
        rec("doomed", "d2", 3_600_000, 3000, "interrupted");
        rec("doomed", "d3", 600_000, 200, "failed");
        rec("steady", "s_ok", 1_200_000, 2000, "success");
        rec("mid", "m_ok", 600_000, 100, "success");
        rec("tiny", "t_ok", 10_000, 100, "success");

        let recs: Vec<(String, u32, bool)> = ["doomed", "steady", "mid", "tiny"]
            .iter()
            .map(|n| ((*n).to_string(), 2u32, false))
            .collect();
        let cost_of = recs
            .iter()
            .map(|(n, _, _)| (n.clone(), crate::plan::CostClass::Medium))
            .collect();
        let packed = repack_from_history(recs, &cost_of, &std::collections::HashSet::new(), &state);
        let wave = |n: &str| packed.iter().find(|(m, _, _)| m == n).unwrap().1;
        assert_eq!(
            wave("doomed"),
            1,
            "an hour-long failed attempt is a FLOOR on the real duration — it must \
             open the tier, not sort below a 10 s export at the 5 s placeholder"
        );
        assert_eq!(
            (wave("steady"), wave("mid"), wave("tiny")),
            (2, 2, 2),
            "3000 MB + 2000 MB exceeds the 4096 MB wave budget: the failed \
             attempt's peak RSS must split the wave, not be discarded as 150 MB"
        );
    }

    /// An isolate_on_source export must come out parallel_safe:FALSE even under packing (bug-hunt round 2).
    #[test]
    fn repack_keeps_isolate_on_source_out_of_the_concurrent_batch() {
        let state = crate::state::StateStore::open_in_memory().expect("state");
        for n in ["lonely", "buddy"] {
            state
                .record_metric_full(&crate::state::MetricRow {
                    export_name: n.to_string(),
                    run_id: format!("{n}_r"),
                    duration_ms: 1000,
                    total_rows: 10,
                    peak_rss_mb: Some(100),
                    status: "success".to_string(),
                    error_message: None,
                    tuning_profile: None,
                    format: None,
                    mode: None,
                    files_produced: 1,
                    bytes_written: 100,
                    retries: 0,
                    validated: None,
                    schema_changed: None,
                    ..Default::default()
                })
                .expect("rec");
        }
        let recs = vec![
            ("lonely".to_string(), 4, false),
            ("buddy".to_string(), 4, true),
        ];
        let cost_of = recs
            .iter()
            .map(|(n, _, _)| (n.clone(), crate::plan::CostClass::Low))
            .collect();
        let mut isolate = std::collections::HashSet::new();
        isolate.insert("lonely".to_string());
        let packed = repack_from_history(recs, &cost_of, &isolate, &state);
        let ps = |n: &str| packed.iter().find(|(m, _, _)| m == n).unwrap().2;
        assert!(
            !ps("lonely"),
            "isolate_on_source export must be parallel_safe:false"
        );
        assert!(
            ps("buddy"),
            "a normal packed export stays parallel_safe:true"
        );
    }

    /// --annotate-waves + --export is a category error.
    #[test]
    fn annotate_waves_refuses_to_be_scoped_to_one_export() {
        let err = super::run_plan_command(
            "/nonexistent/rivet.yaml",
            Some("just_me"),
            None,
            super::PlanOutputFormat::Pretty,
            true,
            None,
        )
        .expect_err("--annotate-waves + --export must refuse");
        let msg = format!("{err:#}");
        crate::error::assert_coded(&err, "RIVET_CLI_OPTION_UNSUPPORTED", 1);
        assert!(
            msg.contains("--annotate-waves") && msg.contains("--export"),
            "the refusal must name both flags: {msg}"
        );
    }

    /// The plan preview and apply --pool now share ONE predictor ([`crate::pipeline::pool::predict_items`]).
    #[test]
    fn plan_is_read_only_without_annotate_and_packs_the_whole_config_with_it() {
        let source = crate::config::SourceConfig {
            source_type: crate::config::SourceType::Postgres,
            url: Some("postgresql://localhost/test".into()),
            url_env: None,
            url_file: None,
            url_secret: None,
            host: None,
            port: None,
            user: None,
            password: None,
            password_env: None,
            password_secret: None,
            database: None,
            environment: None,
            tuning: None,
            tls: None,
            mongo: None,
            columns_from: None,
        };
        let mut cfg = crate::config::Config {
            source,
            exports: vec![
                crate::config::sample_export("hand_tuned"),
                crate::config::sample_export("half_set"),
                crate::config::sample_export("blank"),
            ],
            notifications: None,
            parallel_exports: false,
            parallel_export_processes: false,
            load: None,
        };
        cfg.exports[0].wave = Some(7);
        cfg.exports[0].parallel_safe = Some(false);
        cfg.exports[1].wave = Some(7);
        let _ = &cfg;
        let recs = vec![
            ("hand_tuned".to_string(), 2, true),
            ("half_set".to_string(), 2, true),
            ("blank".to_string(), 2, true),
        ];

        let fields = fields_to_write(&recs, false);
        assert!(
            fields.is_empty(),
            "plain `rivet plan` is read-only: it must write NOTHING, not even \
             fill a blank export's wave/parallel_safe; got {fields:?}"
        );

        let fields = fields_to_write(&recs, true);
        assert_eq!(fields.len(), 3, "the flag packs every export");
        for name in ["hand_tuned", "half_set", "blank"] {
            let keys: Vec<&str> = fields[name].iter().map(|(k, _)| *k).collect();
            assert_eq!(
                keys,
                vec!["wave", "parallel_safe"],
                "--annotate-waves writes both fields on {name}"
            );
        }
    }

    /// The preview and the schedule must be ONE number.
    #[test]
    fn pool_preview_models_the_parallel_safe_apply_will_read_not_the_recommendation() {
        let source = crate::config::SourceConfig {
            source_type: crate::config::SourceType::Postgres,
            url: Some("postgresql://localhost/test".into()),
            url_env: None,
            url_file: None,
            url_secret: None,
            host: None,
            port: None,
            user: None,
            password: None,
            password_env: None,
            password_secret: None,
            database: None,
            environment: None,
            tuning: None,
            tls: None,
            mongo: None,
            columns_from: None,
        };
        let mut cfg = crate::config::Config {
            source,
            exports: vec![
                crate::config::sample_export("hand_tuned"),
                crate::config::sample_export("blank"),
                crate::config::sample_export("unmatched"),
            ],
            notifications: None,
            parallel_exports: false,
            parallel_export_processes: false,
            load: None,
        };
        cfg.exports[0].wave = Some(7);
        cfg.exports[0].parallel_safe = Some(false);
        let recs = vec![
            ("hand_tuned".to_string(), 2, true),
            ("blank".to_string(), 2, true),
            ("unmatched".to_string(), 2, true),
        ];
        let fields = fields_to_write(&recs, true);
        assert!(
            fields.contains_key("unmatched"),
            "the fixture must ATTEMPT the write it then fails to match"
        );
        let written: std::collections::HashSet<String> =
            ["blank".to_string()].into_iter().collect();

        let safe = effective_parallel_safe(&recs, &cfg, &fields, &written);
        assert_eq!(
            safe.get("hand_tuned"),
            Some(&false),
            "the write never landed on disk, so apply reads the config's flag — not the rec"
        );
        assert_eq!(
            safe.get("blank"),
            Some(&true),
            "a field whose write DID land reads back as the rec"
        );
        assert_eq!(
            safe.get("unmatched"),
            Some(&false),
            "nothing landed on disk, so apply's `unwrap_or(false)` is the truth"
        );

        let wave = effective_wave(&recs, &cfg, &fields, &written);
        assert_eq!(
            wave.get("hand_tuned"),
            Some(&Some(7)),
            "apply reads the config's wave"
        );
        assert_eq!(
            wave.get("blank"),
            Some(&Some(2)),
            "the landed write is on disk"
        );
        assert_eq!(
            wave.get("unmatched"),
            Some(&None),
            "no wave on disk: unscheduled"
        );
    }

    #[test]
    fn the_plan_headline_says_what_apply_does_with_the_waves_on_disk() {
        assert_eq!(
            apply_order_line("r.yaml", &[None, None]),
            "  Plan: 2 exports — r.yaml sets no `wave:`, so `rivet apply r.yaml` runs them as \
             one unscheduled group (Rec = the cost model's advice; `--annotate-waves` writes a \
             schedule)"
        );
        assert_eq!(
            apply_order_line("r.yaml", &[Some(1), Some(3)]),
            "  Plan: 2 exports — `rivet apply r.yaml` runs them by `wave:` (lowest first)"
        );
        assert_eq!(
            apply_order_line("r.yaml", &[Some(1), None, None]),
            "  Plan: 3 exports — `rivet apply r.yaml` runs them by `wave:` (lowest first); the \
             2 with no `wave:` run last as one unscheduled group"
        );
    }

    fn wave_fields(pairs: &[(&str, u32)]) -> ExportFields {
        pairs
            .iter()
            .map(|(n, w)| (n.to_string(), vec![("wave", w.to_string())]))
            .collect()
    }

    /// Inserts `wave:` right after each `- name:`, replaces a stale one, and leaves comments / other fields / unlisted exports untouched.
    #[test]
    fn wave_annotations_insert_replace_and_preserve() {
        let yaml = "exports:\n  - name: orders   # the orders table\n    mode: incremental\n    wave: 9\n    cursor_column: updated_at\n  - name: events\n    mode: full\ndestination:\n  type: local\n";
        let (out, written) =
            apply_field_annotations(yaml, &wave_fields(&[("orders", 2), ("events", 1)]));
        assert_eq!(written.len(), 2, "both named exports matched");
        assert!(
            out.contains("- name: orders   # the orders table\n    wave: 2\n"),
            "{out}"
        );
        assert!(
            !out.contains("wave: 9"),
            "stale wave must be dropped:\n{out}"
        );
        assert!(out.contains("- name: events\n    wave: 1\n"), "{out}");
        assert!(out.contains("cursor_column: updated_at"));
        assert!(out.contains("destination:\n  type: local"));
    }

    /// An export whose name isn't in the map is left byte-identical.
    #[test]
    fn wave_annotations_leave_unlisted_export_untouched() {
        let yaml = "exports:\n  - name: orders\n    mode: full\n";
        let (out, written) = apply_field_annotations(yaml, &wave_fields(&[("other", 1)]));
        assert!(
            written.is_empty(),
            "a name absent from the YAML writes nothing"
        );
        assert_eq!(out, yaml);
    }

    /// Regression (audit #4): two distinct exports under one `--output FILE` must derive two distinct paths, so neither is silently overwritten.
    #[test]
    fn per_export_paths_are_distinct() {
        let a = per_export_output_path("/tmp/plan.json", "orders");
        let b = per_export_output_path("/tmp/plan.json", "users");
        assert_ne!(a, b, "distinct exports must not collide on one path");
        assert_eq!(a, "/tmp/plan.orders.json");
        assert_eq!(b, "/tmp/plan.users.json");
    }

    /// The original extension is preserved so `rivet apply` (and any `*.json` glob) still find the per-export files.
    #[test]
    fn per_export_path_keeps_json_extension() {
        let p = per_export_output_path("/tmp/plan.json", "orders");
        assert_eq!(
            Path::new(&p).extension().and_then(|e| e.to_str()),
            Some("json"),
            "per-export file must keep the .json extension"
        );
    }

    /// An export name with no extension on the base still gets a distinct, non-colliding suffix rather than overwriting the base path.
    #[test]
    fn per_export_path_without_extension_appends_name() {
        let p = per_export_output_path("/tmp/plan", "orders");
        assert_eq!(p, "/tmp/plan.orders");
    }

    /// A hostile export name (path separators, dots) must not escape the directory or mangle the extension.
    #[test]
    fn per_export_path_sanitizes_unsafe_export_name() {
        let p = per_export_output_path("/tmp/plan.json", "../../etc/passwd");
        assert!(
            !p.contains(".."),
            "sanitized path must not contain a `..` traversal: {p}"
        );
        let file = Path::new(&p)
            .file_name()
            .and_then(|f| f.to_str())
            .expect("derived path has a file name");
        assert!(
            !file.contains('/') && !file.contains('\\'),
            "sanitized file name must not contain a path separator: {file}"
        );
        assert_eq!(
            Path::new(&p).parent().and_then(|d| d.to_str()),
            Some("/tmp"),
            "derived path must stay in the requested directory"
        );
        assert_eq!(
            Path::new(&p).extension().and_then(|e| e.to_str()),
            Some("json"),
            "extension must survive sanitization"
        );
    }

    /// The pool advisory must never share STDOUT with the machine document.
    #[test]
    fn the_pool_advisory_never_shares_stdout_with_the_json_document() {
        use super::{PlanOutputFormat, pool_estimate_is_printable};
        assert!(!pool_estimate_is_printable(
            &PlanOutputFormat::Json(None),
            2
        ));
        assert!(pool_estimate_is_printable(
            &PlanOutputFormat::Json(Some("plan.json".to_string())),
            2
        ));
        assert!(pool_estimate_is_printable(&PlanOutputFormat::Pretty, 2));
        assert!(!pool_estimate_is_printable(&PlanOutputFormat::Pretty, 1));
    }

    /// Field find (2026-08-13 pool dogfood): a chunked export on a NON-UNIQUE key (a versioned table, ~2.5 rows per key value) has `catalog > span`, and the old span-always-wins rule under-reported ~831M rows as the ~333M key span.
    #[test]
    fn chunked_row_estimate_prefers_catalog_when_key_is_provably_non_unique() {
        use super::chunked_row_estimate;
        assert_eq!(
            chunked_row_estimate(Some(333_000_000), Some(831_000_000), false, false),
            Some(831_000_000),
        );
        assert_eq!(
            chunked_row_estimate(Some(342_000_000), Some(520_000), false, false),
            Some(342_000_000),
        );
        assert_eq!(
            chunked_row_estimate(Some(342_000_000), Some(520_000), true, false),
            Some(520_000),
        );
        assert_eq!(
            chunked_row_estimate(Some(30), Some(1130), false, false),
            Some(30)
        );
        assert_eq!(chunked_row_estimate(Some(42), None, false, false), Some(42));
        assert_eq!(chunked_row_estimate(None, Some(42), false, false), Some(42));
        assert_eq!(chunked_row_estimate(None, None, false, false), None);
    }

    /// `chunk_by_days` ordinals are DAYS, so the span is not a row figure at all.
    #[test]
    fn a_by_days_span_is_a_day_count_and_never_a_row_estimate() {
        use super::chunked_row_estimate;
        assert_eq!(
            chunked_row_estimate(Some(1095), Some(831_000_000), false, true),
            Some(831_000_000),
        );
        assert_eq!(
            chunked_row_estimate(Some(20_000), Some(4_000), false, true),
            Some(4_000),
        );
        assert_eq!(
            chunked_row_estimate(Some(1095), Some(520_000), true, true),
            Some(520_000),
        );
        assert_eq!(chunked_row_estimate(Some(1095), None, false, true), None);
    }
    /// `plan` over a source it cannot read ends with a code, the cause, and the cause's exit class.
    #[test]
    fn a_plan_over_an_unreadable_source_fails_by_code_with_the_causes_exit_class() {
        let missing = super::source_unreadable(
            "orders",
            anyhow::anyhow!("preflight: relation \"orders\" does not exist (SQLSTATE 42P01)"),
        );
        assert_eq!(
            format!("{missing:#}"),
            "plan 'orders': the source could not be read, so no plan was written (`rivet apply` \
             would fail on it): preflight: relation \"orders\" does not exist (SQLSTATE 42P01)"
        );
        assert_eq!(
            crate::error::error_code(&missing),
            Some("RIVET_PLAN_SOURCE_UNREADABLE")
        );
        assert_eq!(crate::error::classify_exit(&missing), 1);
        let dropped =
            super::source_unreadable("orders", anyhow::anyhow!("connection reset by peer"));
        assert_eq!(crate::error::classify_exit(&dropped), 2);
    }

    /// A dash and an underscore are kept as written, so two exports that differ only there keep two files.
    #[test]
    fn per_export_path_keeps_a_dash_and_an_underscore_as_written() {
        assert_eq!(
            per_export_output_path("/tmp/plan.json", "daily-orders_v2"),
            "/tmp/plan.daily-orders_v2.json"
        );
        assert_ne!(
            per_export_output_path("plan.json", "a-b"),
            per_export_output_path("plan.json", "a_b")
        );
        assert_eq!(
            per_export_output_path("plan.json", "orders"),
            "plan.orders.json"
        );
    }

    /// A config of three exports whose `wave`/`parallel_safe` differ from each other and from `recs`.
    fn three_exports() -> (crate::config::Config, Vec<(String, u32, bool)>) {
        let mut cfg = crate::config::Config::from_yaml(
            "source: { type: postgres, url: \"postgresql://u:p@127.0.0.1:1/db\" }\n\
             exports:\n\
             \x20 - name: first\n    table: first\n    mode: full\n    format: parquet\n\
             \x20   destination: { type: local, path: ./out/first }\n\
             \x20 - name: second\n    table: second\n    mode: full\n    format: parquet\n\
             \x20   destination: { type: local, path: ./out/second }\n\
             \x20 - name: third\n    table: third\n    mode: full\n    format: parquet\n\
             \x20   destination: { type: local, path: ./out/third }\n",
        )
        .expect("the fixture config loads");
        (cfg.exports[0].wave, cfg.exports[0].parallel_safe) = (Some(1), Some(true));
        (cfg.exports[1].wave, cfg.exports[1].parallel_safe) = (Some(9), Some(false));
        let recs = vec![
            ("first".to_string(), 4, false),
            ("second".to_string(), 5, true),
            ("third".to_string(), 6, true),
        ];
        (cfg, recs)
    }

    /// With nothing written each export reads its own config value, not a neighbour's and not the recommendation.
    #[test]
    fn an_unwritten_export_reads_its_own_config_value() {
        let (cfg, recs) = three_exports();
        let (fields, written) = (ExportFields::new(), std::collections::HashSet::new());
        let safe = effective_parallel_safe(&recs, &cfg, &fields, &written);
        assert_eq!(
            (safe["first"], safe["second"], safe["third"]),
            (true, false, false)
        );
        let wave = effective_wave(&recs, &cfg, &fields, &written);
        assert_eq!(
            (wave["first"], wave["second"], wave["third"]),
            (Some(1), Some(9), None)
        );
    }

    /// Only the field a write carried is on disk afterwards.
    #[test]
    fn a_write_puts_on_disk_only_the_field_it_carried() {
        let (cfg, recs) = three_exports();
        let written: std::collections::HashSet<String> =
            ["third".to_string()].into_iter().collect();
        let only = |field: &'static str, value: &str| -> ExportFields {
            [("third".to_string(), vec![(field, value.to_string())])]
                .into_iter()
                .collect()
        };
        let wave_only = only("wave", "6");
        assert!(!effective_parallel_safe(&recs, &cfg, &wave_only, &written)["third"]);
        assert_eq!(
            effective_wave(&recs, &cfg, &wave_only, &written)["third"],
            Some(6)
        );
        let safe_only = only("parallel_safe", "true");
        assert!(effective_parallel_safe(&recs, &cfg, &safe_only, &written)["third"]);
        assert_eq!(
            effective_wave(&recs, &cfg, &safe_only, &written)["third"],
            None
        );
    }

    /// The annotations land in the config file itself, and the names returned are the ones that matched a `- name:` line.
    #[test]
    fn the_annotations_are_written_into_the_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rivet.yaml");
        std::fs::write(&path, "exports:\n  - name: orders\n    mode: full\n").unwrap();
        let config = path.to_str().unwrap();
        let written =
            super::write_plan_fields_to_config(config, &wave_fields(&[("orders", 3), ("gone", 1)]))
                .unwrap();
        assert_eq!(
            written.into_iter().collect::<Vec<_>>(),
            ["orders".to_string()]
        );
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "exports:\n  - name: orders\n    wave: 3\n    mode: full\n"
        );
        let missing = dir.path().join("absent.yaml");
        let err = super::write_plan_fields_to_config(
            missing.to_str().unwrap(),
            &wave_fields(&[("orders", 3)]),
        )
        .unwrap_err();
        assert!(
            err.to_string().starts_with("cannot read config '"),
            "{err:#}"
        );
    }

    /// The field lands two columns past the dash at any list indent.
    #[test]
    fn wave_annotations_follow_the_indent_of_the_list() {
        let annotated =
            |yaml: &str| apply_field_annotations(yaml, &wave_fields(&[("orders", 2)])).0;
        assert_eq!(
            annotated("exports:\n- name: orders\n  wave: 9\n  mode: full\n"),
            "exports:\n- name: orders\n  wave: 2\n  mode: full\n"
        );
        assert_eq!(
            annotated("exports:\n      - name: orders\n        wave: 9\n        mode: full\n"),
            "exports:\n      - name: orders\n        wave: 2\n        mode: full\n"
        );
    }

    /// A blank line or a comment at any indent does not end the export, so a stale field after it is still replaced.
    #[test]
    fn a_blank_line_or_a_comment_does_not_end_the_export_block() {
        let yaml = "exports:\n  - name: orders\n\n# tuned by hand\n    wave: 9\n    mode: full\n\
                    other:\n    wave: 5\n";
        let (out, written) = apply_field_annotations(yaml, &wave_fields(&[("orders", 2)]));
        assert_eq!(written.len(), 1);
        assert_eq!(
            out,
            "exports:\n  - name: orders\n    wave: 2\n\n# tuned by hand\n    mode: full\n\
             other:\n    wave: 5\n"
        );
    }

    /// A quoted export name is read as its bare name, in either quote style and before a trailing comment.
    #[test]
    fn a_quoted_export_name_is_read_as_its_bare_name() {
        let parsed = |line: &str| super::parse_export_name(line);
        assert_eq!(
            parsed("  - name: \"orders\""),
            Some((2, "orders".to_string()))
        );
        assert_eq!(
            parsed("- name: 'daily orders'  # nightly"),
            Some((0, "daily orders".to_string()))
        );
        assert_eq!(parsed("  - name: \"\""), None);
        assert_eq!(parsed("    name: orders"), None);
        let yaml = "exports:\n  - name: \"orders\"\n    mode: full\n";
        let (out, written) = apply_field_annotations(yaml, &wave_fields(&[("orders", 2)]));
        assert_eq!(written.len(), 1);
        assert_eq!(
            out,
            "exports:\n  - name: \"orders\"\n    wave: 2\n    mode: full\n"
        );
    }

    #[test]
    fn a_config_with_nothing_to_plan_and_an_unknown_export_are_refused_by_code() {
        use crate::pipeline::plan_cmd::plannable_exports;
        let config = crate::config::Config::from_yaml(
            "source: { type: postgres, url: \"postgresql://u:p@127.0.0.1:1/db\" }\n\
             exports:\n\
             \x20 - name: orders\n    mode: cdc\n    table: orders\n    format: parquet\n\
             \x20   cdc: { checkpoint: /tmp/ck, backfill: [orders_base] }\n\
             \x20   destination: { type: local, path: ./out/orders }\n\
             \x20 - name: orders_base\n    table: orders\n    mode: full\n    format: parquet\n\
             \x20   destination: { type: local, path: ./out/orders_base }\n",
        )
        .unwrap();
        let err = plannable_exports(&config, None).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_PLAN_NOTHING_TO_PLAN", 1);
        let err = plannable_exports(&config, Some("nope")).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_CLI_EXPORT_UNKNOWN", 1);
        assert_eq!(
            plannable_exports(&config, Some("orders_base"))
                .unwrap()
                .len(),
            1
        );
    }
}
