//! Schema-drift detection + baseline persistence.

use crate::config::SchemaDriftPolicy;
use crate::error::{Result, SchemaDriftError};
use crate::journal::RunEvent;
use crate::plan::ResolvedRunPlan;
use crate::state::{
    PendingBaselines, SchemaChange, SchemaColumn, StateStore, schema_change_between,
};

use super::summary::RunSummary;

/// Adapter — single mode: columns from the sink's resolved (data-derived) schema.
pub(super) fn check_from_sink_schema(
    state: &StateStore,
    export_name: &str,
    sink_schema: &arrow::datatypes::Schema,
    policy: SchemaDriftPolicy,
    summary: &mut RunSummary,
) -> Result<()> {
    let columns = crate::state::arrow_schema_to_columns(sink_schema);
    if columns.is_empty() {
        summary.schema_changed.get_or_insert(false);
        return Ok(());
    }
    check_and_persist(state, export_name, &columns, policy, summary)
}

/// Adapter — chunked mode: columns from a scan-free `type_mappings` probe, run **pre-chunk** so `on_schema_drift.
pub(super) fn check_from_type_mappings(
    src: &mut dyn crate::source::Source,
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
) -> Result<()> {
    if seeded!("chunked-drift-gate-skipped", true, false) {
        summary.schema_changed.get_or_insert(false);
        return Ok(());
    }
    let mappings = match src.type_mappings(&plan.base_query, &plan.column_overrides) {
        Ok(m) => m,
        Err(e) => {
            log::warn!(
                "export '{}': could not resolve schema for drift check (skipping): {e:#}",
                plan.export_name
            );
            summary.schema_changed.get_or_insert(false);
            return Ok(());
        }
    };
    let fields: Vec<arrow::datatypes::Field> = mappings
        .iter()
        .filter_map(crate::types::build_arrow_field)
        .collect();
    if fields.is_empty() {
        summary.schema_changed.get_or_insert(false);
        return Ok(());
    }
    let columns = crate::state::arrow_schema_to_columns(&arrow::datatypes::Schema::new(fields));
    check_and_persist(
        state,
        &plan.export_name,
        &columns,
        plan.schema_drift_policy,
        summary,
    )
}

/// Adapter — CDC: one captured table's probed columns, judged before any change is read.
pub(super) fn check_from_cdc_mappings(
    state: &StateStore,
    key: &str,
    mappings: &[crate::types::TypeMapping],
    policy: SchemaDriftPolicy,
    pending: &mut PendingBaselines,
) -> Result<()> {
    let fields: Vec<arrow::datatypes::Field> = mappings
        .iter()
        .filter_map(crate::types::build_arrow_field)
        .collect();
    if fields.is_empty() {
        return Ok(());
    }
    migrate_baseline(state, pending, key, mappings, AbsentColumns::Adopt);
    let columns = crate::state::arrow_schema_to_columns(&arrow::datatypes::Schema::new(fields));
    judge(
        state,
        pending,
        key,
        &columns,
        policy,
        &mut RunSummary::default(),
    )
}

/// Whether a column the baseline never recorded may be adopted into it: only a CDC baseline omitted an unbuildable column, a batch baseline did not.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum AbsentColumns {
    Adopt,
    Drift,
}

/// Hold the baseline of `key` with every column now delivered as server text rewritten, so no run reads that as drift.
pub(super) fn migrate_baseline(
    state: &StateStore,
    pending: &mut PendingBaselines,
    key: &str,
    mappings: &[crate::types::TypeMapping],
    absent: AbsentColumns,
) {
    if seeded!("upgrade-baseline-not-migrated", true, false) {
        return;
    }
    match pending.effective(state, key) {
        Ok(Some(stored)) => {
            if let Some(migrated) = migrate_upgrade_labels(&stored, mappings, absent) {
                pending.set(key, migrated);
            }
        }
        Ok(None) => {}
        Err(e) => log::warn!("schema drift: could not read the baseline of '{key}': {e:#}"),
    }
}

/// True when `new` differs from its baseline entry only because rivet now labels it server text: absent, the old Arrow type, or the native name a run that refused it recorded.
pub(super) fn is_upgrade_label_migration(
    old: Option<&SchemaColumn>,
    new: &crate::types::TypeMapping,
) -> bool {
    use crate::types::{Delivery, TextForm, rivet_type_to_arrow};
    new.delivery == Delivery::Text(TextForm::ServerText)
        && (old.map(|c| c.data_type.clone())
            == rivet_type_to_arrow(&new.rivet_type).map(|dt| format!("{dt:?}"))
            || old.is_some_and(|c| c.data_type == new.source_native_type))
}

/// The baseline with every upgrade-label migration applied (one info line each), or `None` when nothing moved.
fn migrate_upgrade_labels(
    stored: &[SchemaColumn],
    mappings: &[crate::types::TypeMapping],
    absent: AbsentColumns,
) -> Option<Vec<SchemaColumn>> {
    let mut out = stored.to_vec();
    let mut moved = false;
    for m in mappings {
        let at = out.iter().position(|c| c.name == m.column_name);
        if at.is_none()
            && absent == AbsentColumns::Drift
            && seeded!("batch-absent-column-adopted", false, true)
        {
            continue;
        }
        if !is_upgrade_label_migration(at.map(|i| &out[i]), m) {
            continue;
        }
        let Some(field) = crate::types::build_arrow_field(m) else {
            continue;
        };
        let col = SchemaColumn {
            name: m.column_name.clone(),
            data_type: format!("{:?}", field.data_type()),
        };
        log::info!(
            "schema drift: column '{}' ({}) is now delivered as server text; the baseline \
             did not record it that way, so it is updated without reporting drift",
            m.column_name,
            m.source_native_type
        );
        match at {
            Some(i) => out[i] = col,
            None => out.push(col),
        }
        moved = true;
    }
    moved.then_some(out)
}

/// Deep core (private): judge `columns` for `export_name` and hold any new baseline in the summary, stored only when the run ends clean.
fn check_and_persist(
    state: &StateStore,
    export_name: &str,
    columns: &[SchemaColumn],
    policy: SchemaDriftPolicy,
    summary: &mut RunSummary,
) -> Result<()> {
    let mut pending = std::mem::take(&mut summary.pending_baselines);
    let verdict = judge(state, &mut pending, export_name, columns, policy, summary);
    summary.pending_baselines = pending;
    verdict
}

/// Judge `columns` and hold the new baseline in `pending`, stored only when the run ends clean.
fn judge(
    state: &StateStore,
    pending: &mut PendingBaselines,
    export_name: &str,
    columns: &[SchemaColumn],
    policy: SchemaDriftPolicy,
    summary: &mut RunSummary,
) -> Result<()> {
    let verdict = hold_or_refuse(state, pending, export_name, columns, policy, summary);
    if seeded!("schema-baseline-before-refusal", true, false) {
        pending.commit(state);
    }
    verdict
}

/// The lines that name what a schema change added, removed and retyped.
fn drift_lines(change: &SchemaChange) -> Vec<String> {
    let mut lines = Vec::new();
    if !change.added.is_empty() {
        lines.push(format!("  added: {}", change.added.join(", ")));
    }
    if !change.removed.is_empty() {
        lines.push(format!("  removed: {}", change.removed.join(", ")));
    }
    for (col, old, new) in &change.type_changed {
        lines.push(format!("  type changed: {col} ({old} → {new})"));
    }
    lines
}

/// Detect drift of `columns` against the baseline of `export_name` and act per `policy`.
fn hold_or_refuse(
    state: &StateStore,
    pending: &mut PendingBaselines,
    export_name: &str,
    columns: &[SchemaColumn],
    policy: SchemaDriftPolicy,
    summary: &mut RunSummary,
) -> Result<()> {
    let stored = match pending.effective(state, export_name) {
        Ok(Some(stored)) => stored,
        Ok(None) => {
            pending.set(export_name, columns.to_vec());
            summary.schema_changed = Some(false);
            return Ok(());
        }
        Err(e) => {
            log::warn!("schema tracking error: {e:#}");
            summary.schema_changed.get_or_insert(false);
            return Ok(());
        }
    };
    let Some(change) = schema_change_between(&stored, columns) else {
        summary.schema_changed = Some(false);
        return Ok(());
    };
    summary.schema_changed = Some(true);
    summary.journal.record(RunEvent::SchemaChanged {
        added: change.added.clone(),
        removed: change.removed.clone(),
        type_changed: change.type_changed.clone(),
    });
    match policy {
        SchemaDriftPolicy::Continue => pending.set(export_name, columns.to_vec()),
        SchemaDriftPolicy::Warn => {
            log::warn!("export '{export_name}': schema changed!");
            drift_lines(&change)
                .iter()
                .for_each(|line| log::warn!("{line}"));
            pending.set(export_name, columns.to_vec());
        }
        SchemaDriftPolicy::Fail => {
            log::error!(
                "export '{export_name}': schema drift detected — aborting (on_schema_drift: fail)"
            );
            drift_lines(&change)
                .iter()
                .for_each(|line| log::error!("{line}"));
            return Err(SchemaDriftError::new(format!(
                "schema drift detected for export '{export_name}': \
                 {} column(s) added, {} removed, {} retyped — \
                 set `on_schema_drift: warn` to accept, or fix the schema mismatch",
                change.added.len(),
                change.removed.len(),
                change.type_changed.len()
            ))
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ty: &str) -> SchemaColumn {
        SchemaColumn {
            name: name.into(),
            data_type: ty.into(),
        }
    }
    fn summary() -> RunSummary {
        RunSummary::stub_for_testing("run-1", "orders")
    }

    fn mapping(name: &str, native: &str, t: crate::types::RivetType) -> crate::types::TypeMapping {
        crate::types::TypeMapping::from_source(
            &crate::types::SourceColumn::simple(name, native, true),
            t,
        )
    }

    fn server_text(
        name: &str,
        native: &str,
        t: crate::types::RivetType,
    ) -> crate::types::TypeMapping {
        mapping(name, native, t).with_text(crate::types::TextForm::ServerText)
    }

    fn bare_numeric() -> crate::types::RivetType {
        crate::types::RivetType::Unsupported {
            native_type: "numeric".into(),
            reason: "no precision".into(),
        }
    }

    fn array_of(inner: crate::types::RivetType) -> crate::types::RivetType {
        crate::types::RivetType::List {
            inner: Box::new(inner),
        }
    }

    /// The baseline a refused run left, taken by `migrate_baseline` under `fail`; a typed column and a genuinely retyped one are not.
    #[test]
    fn a_baseline_a_refused_run_left_is_migrated_for_every_batch_mode_too() {
        let st = StateStore::open_in_memory().unwrap();
        st.store_schema(
            "k",
            &[
                col("id", "Int64"),
                col("n", "numeric"),
                col("a", "_numeric"),
                col("v", "Decimal128(10, 2)"),
            ],
        )
        .unwrap();
        let cols = vec![
            mapping("id", "bigint", crate::types::RivetType::Int64),
            server_text("n", "numeric", bare_numeric()),
            server_text("a", "_numeric", array_of(bare_numeric())),
            mapping(
                "v",
                "numeric(10,2)",
                crate::types::RivetType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            ),
        ];
        let mut pending = PendingBaselines::default();
        migrate_baseline(&st, &mut pending, "k", &cols, AbsentColumns::Drift);
        assert_eq!(
            st.get_stored_schema("k").unwrap().unwrap().len(),
            4,
            "the migration is held, not stored"
        );
        pending.commit(&st);
        let stored = st.get_stored_schema("k").unwrap().unwrap();
        assert_eq!(
            stored,
            vec![
                col("id", "Int64"),
                col("n", "Utf8"),
                col("a", "Utf8"),
                col("v", "Decimal128(10, 2)")
            ]
        );
        let again = st.get_stored_schema("k").unwrap();
        let mut second = PendingBaselines::default();
        migrate_baseline(&st, &mut second, "k", &cols, AbsentColumns::Drift);
        second.commit(&st);
        assert_eq!(st.get_stored_schema("k").unwrap(), again, "idempotent");
    }

    /// A column the batch baseline never recorded is drift, not a label migration; a CDC baseline adopts it.
    #[test]
    fn a_column_absent_from_a_batch_baseline_is_left_to_the_drift_check() {
        let cols = vec![
            mapping("id", "bigint", crate::types::RivetType::Int64),
            server_text(
                "x",
                "inet",
                crate::types::RivetType::Unsupported {
                    native_type: "inet".into(),
                    reason: "no arrow type".into(),
                },
            ),
            server_text("n", "numeric", bare_numeric()),
        ];
        let batch = StateStore::open_in_memory().unwrap();
        batch.store_schema("k", &[col("id", "Int64")]).unwrap();
        let mut held = PendingBaselines::default();
        migrate_baseline(&batch, &mut held, "k", &cols, AbsentColumns::Drift);
        held.commit(&batch);
        assert_eq!(
            batch.get_stored_schema("k").unwrap().unwrap(),
            vec![col("id", "Int64")]
        );
        let cdc = StateStore::open_in_memory().unwrap();
        cdc.store_schema("k", &[col("id", "Int64")]).unwrap();
        let mut held = PendingBaselines::default();
        migrate_baseline(&cdc, &mut held, "k", &cols, AbsentColumns::Adopt);
        held.commit(&cdc);
        assert_eq!(
            cdc.get_stored_schema("k").unwrap().unwrap(),
            vec![col("id", "Int64"), col("x", "Utf8"), col("n", "Utf8")]
        );
    }

    fn date_list() -> crate::types::RivetType {
        crate::types::RivetType::List {
            inner: Box::new(crate::types::RivetType::Date),
        }
    }

    /// The label migration covers exactly what 0.30.0 recorded for an unbuildable column, nothing else.
    #[test]
    fn upgrade_label_migration_matches_only_what_the_old_baseline_recorded() {
        let n = server_text("n", "numeric", bare_numeric());
        assert!(
            is_upgrade_label_migration(None, &n),
            "0.30.0 recorded nothing"
        );
        assert!(!is_upgrade_label_migration(Some(&col("n", "Utf8")), &n));
        assert!(!is_upgrade_label_migration(Some(&col("n", "Int64")), &n));

        let d = server_text("d", "date[]", date_list());
        let old_list = format!(
            "{:?}",
            crate::types::rivet_type_to_arrow(&date_list()).unwrap()
        );
        assert_eq!(
            old_list, "List(Field { data_type: Date32, nullable: true })",
            "what 0.30.0 stored, measured with its release binary"
        );
        assert!(is_upgrade_label_migration(Some(&col("d", &old_list)), &d));
        assert!(
            !is_upgrade_label_migration(None, &d),
            "a List column was recorded"
        );

        assert!(
            is_upgrade_label_migration(Some(&col("n", "numeric")), &n),
            "a refused run recorded the native name"
        );
        let na = server_text("a", "_numeric", array_of(bare_numeric()));
        assert!(is_upgrade_label_migration(Some(&col("a", "_numeric")), &na));
        assert!(!is_upgrade_label_migration(Some(&col("a", "numeric")), &na));
        let still = mapping("n", "numeric", bare_numeric());
        assert!(
            !is_upgrade_label_migration(Some(&col("n", "numeric")), &still),
            "a column that is still not delivered as server text is not migrated"
        );

        let native = mapping("v", "bigint", crate::types::RivetType::Int64);
        assert!(
            !is_upgrade_label_migration(None, &native),
            "a new native column drifts"
        );
        let text = mapping("s", "text", crate::types::RivetType::String);
        assert!(!is_upgrade_label_migration(None, &text));
    }

    /// Under `fail`, a 0.30.0 baseline takes the server-text column silently; a genuinely new column still fails.
    #[test]
    fn cdc_gate_migrates_an_old_baseline_silently_and_still_fails_a_new_column() {
        let st = StateStore::open_in_memory().unwrap();
        let old_list = format!(
            "{:?}",
            crate::types::rivet_type_to_arrow(&date_list()).unwrap()
        );
        st.store_schema("k", &[col("id", "Int64"), col("d", &old_list)])
            .unwrap();
        let mut cols = vec![
            mapping("id", "bigint", crate::types::RivetType::Int64),
            server_text("n", "numeric", bare_numeric()),
            server_text("d", "date[]", date_list()),
        ];
        let mut pending = PendingBaselines::default();
        check_from_cdc_mappings(&st, "k", &cols, SchemaDriftPolicy::Fail, &mut pending).unwrap();
        pending.commit(&st);
        let stored = st.get_stored_schema("k").unwrap().unwrap();
        assert!(stored.contains(&col("n", "Utf8")), "{stored:?}");
        assert!(stored.contains(&col("d", "Utf8")), "{stored:?}");

        cols.push(mapping("extra", "bigint", crate::types::RivetType::Int64));
        let err = check_from_cdc_mappings(&st, "k", &cols, SchemaDriftPolicy::Fail, &mut pending)
            .expect_err("a genuinely new column still drifts");
        assert!(
            format!("{err:#}").contains("schema drift detected"),
            "{err:#}"
        );
    }

    #[test]
    fn the_drift_lines_name_each_kind_of_change_that_happened_and_no_other() {
        let change = |added: &[&str], removed: &[&str], retyped: bool| SchemaChange {
            added: added.iter().map(|s| s.to_string()).collect(),
            removed: removed.iter().map(|s| s.to_string()).collect(),
            type_changed: if retyped {
                vec![("v".into(), "Int64".into(), "Utf8".into())]
            } else {
                Vec::new()
            },
        };
        assert!(drift_lines(&change(&[], &[], false)).is_empty());
        assert_eq!(
            drift_lines(&change(&["a", "b"], &[], false)),
            vec!["  added: a, b"]
        );
        assert_eq!(
            drift_lines(&change(&[], &["c"], false)),
            vec!["  removed: c"]
        );
        assert_eq!(
            drift_lines(&change(&["a"], &["c"], true)),
            vec![
                "  added: a",
                "  removed: c",
                "  type changed: v (Int64 → Utf8)"
            ]
        );
    }

    fn ends_clean(st: &StateStore, s: &mut RunSummary) {
        s.pending_baselines.commit(st);
    }

    #[test]
    fn first_run_establishes_baseline_no_drift() {
        let st = StateStore::open_in_memory().unwrap();
        let mut s = summary();
        let cols = vec![col("id", "Int64"), col("name", "Utf8")];
        check_and_persist(&st, "orders", &cols, SchemaDriftPolicy::Fail, &mut s).unwrap();
        assert_eq!(s.schema_changed, Some(false));
        assert!(!s.pending_baselines.is_empty(), "the baseline is held");
        assert_eq!(
            st.get_stored_schema("orders").unwrap(),
            None,
            "a first baseline is held until the run ends clean"
        );
        ends_clean(&st, &mut s);
        assert_eq!(st.get_stored_schema("orders").unwrap(), Some(cols));
    }

    #[test]
    fn drift_under_fail_returns_err_and_flags_change() {
        let st = StateStore::open_in_memory().unwrap();
        let v1 = vec![col("id", "Int64")];
        let mut s1 = summary();
        check_and_persist(&st, "orders", &v1, SchemaDriftPolicy::Fail, &mut s1).unwrap();
        ends_clean(&st, &mut s1);
        let v2 = vec![col("id", "Int64"), col("email", "Utf8")];
        let mut s2 = summary();
        let err = check_and_persist(&st, "orders", &v2, SchemaDriftPolicy::Fail, &mut s2)
            .expect_err("fail policy must abort on drift");
        assert!(
            format!("{err:#}").contains("schema drift detected"),
            "{err:#}"
        );
        assert_eq!(s2.schema_changed, Some(true));
        assert!(s2.pending_baselines.is_empty(), "a refusal holds nothing");
    }

    #[test]
    fn drift_under_warn_stores_new_baseline_and_continues() {
        let st = StateStore::open_in_memory().unwrap();
        let v1 = vec![col("id", "Int64")];
        let mut s1 = summary();
        check_and_persist(&st, "orders", &v1, SchemaDriftPolicy::Warn, &mut s1).unwrap();
        ends_clean(&st, &mut s1);
        let v2 = vec![col("id", "Int64"), col("email", "Utf8")];
        let mut s2 = summary();
        check_and_persist(&st, "orders", &v2, SchemaDriftPolicy::Warn, &mut s2).unwrap();
        assert_eq!(s2.schema_changed, Some(true));
        ends_clean(&st, &mut s2);
        let mut s3 = summary();
        check_and_persist(&st, "orders", &v2, SchemaDriftPolicy::Warn, &mut s3).unwrap();
        assert_eq!(s3.schema_changed, Some(false));
    }

    /// A run that is refused after the check leaves the stored baseline as it found it, whatever the policy.
    #[test]
    fn a_run_that_never_ends_clean_leaves_the_stored_baseline_untouched() {
        let v1 = vec![col("id", "Int64"), col("v", "Int64")];
        let v2 = vec![col("id", "Int64"), col("v", "Int64"), col("w", "Int64")];
        for policy in [SchemaDriftPolicy::Warn, SchemaDriftPolicy::Continue] {
            let st = StateStore::open_in_memory().unwrap();
            st.store_schema("orders", &v1).unwrap();
            let mut s = summary();
            check_and_persist(&st, "orders", &v2, policy, &mut s).unwrap();
            assert_eq!(s.schema_changed, Some(true));
            assert_eq!(st.get_stored_schema("orders").unwrap().unwrap(), v1);
            let mut retry = summary();
            check_and_persist(&st, "orders", &v2, policy, &mut retry).unwrap();
            assert_eq!(
                retry.schema_changed,
                Some(true),
                "the retry of a refused run reports the same drift again"
            );
        }
    }

    /// Within one run a second check judges against the baseline the first one held, as it did when the first stored it.
    #[test]
    fn a_second_check_in_the_run_judges_against_the_baseline_the_first_held() {
        let st = StateStore::open_in_memory().unwrap();
        st.store_schema("k", &[col("id", "Int64")]).unwrap();
        let v2 = vec![col("id", "Int64"), col("w", "Int64")];
        let mut s = summary();
        check_and_persist(&st, "k", &v2, SchemaDriftPolicy::Warn, &mut s).unwrap();
        assert_eq!(s.schema_changed, Some(true));
        check_and_persist(&st, "k", &v2, SchemaDriftPolicy::Warn, &mut s).unwrap();
        assert_eq!(s.schema_changed, Some(false));
        let v3 = vec![col("id", "Int64"), col("w", "Utf8")];
        assert!(check_and_persist(&st, "k", &v3, SchemaDriftPolicy::Fail, &mut s).is_err());
        assert_eq!(
            s.pending_baselines.effective(&st, "k").unwrap().unwrap(),
            v2,
            "a refusal does not move what the run holds"
        );
    }

    /// A zero-row run must not DESTROY the baseline.
    #[test]
    fn a_zero_row_run_neither_wipes_the_baseline_nor_invents_drift() {
        use arrow::datatypes::{DataType, Field, Schema};
        let st = StateStore::open_in_memory().unwrap();
        let real = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        let mut first = summary();
        check_from_sink_schema(&st, "orders", &real, SchemaDriftPolicy::Warn, &mut first).unwrap();
        ends_clean(&st, &mut first);

        let empty = Schema::empty();
        let mut s_empty = summary();
        check_from_sink_schema(&st, "orders", &empty, SchemaDriftPolicy::Warn, &mut s_empty)
            .unwrap();
        assert_eq!(
            s_empty.schema_changed,
            Some(false),
            "an empty schema is the absence of evidence, not drift"
        );
        assert_eq!(
            st.get_stored_schema("orders").unwrap().map(|c| c.len()),
            Some(2),
            "the zero-row run must not overwrite the baseline with []"
        );

        let mut s_next = summary();
        check_from_sink_schema(&st, "orders", &real, SchemaDriftPolicy::Warn, &mut s_next).unwrap();
        assert_eq!(
            s_next.schema_changed,
            Some(false),
            "the baseline survived, so the next real run is drift-free"
        );
    }

    /// The same shape under `fail`: a scheduled run with nothing to export must not ABORT the export.
    #[test]
    fn a_zero_row_run_does_not_abort_under_on_schema_drift_fail() {
        use arrow::datatypes::{DataType, Field, Schema};
        let st = StateStore::open_in_memory().unwrap();
        let real = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let mut first = summary();
        check_from_sink_schema(&st, "orders", &real, SchemaDriftPolicy::Fail, &mut first).unwrap();
        ends_clean(&st, &mut first);

        let empty = Schema::empty();
        let mut s = summary();
        check_from_sink_schema(&st, "orders", &empty, SchemaDriftPolicy::Fail, &mut s)
            .expect("a run with no rows is not schema drift and must not abort");
        assert_eq!(s.schema_changed, Some(false));
    }
}
