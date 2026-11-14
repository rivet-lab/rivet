//! The `TargetLoader` seam and the first live loader — **BigQuery**.
//!
//! OSS decides *what* a column becomes in the warehouse (`TargetColumnSpec`,
//! via `ExportTarget::resolve_table`). This module executes that plan against
//! a live warehouse.
//!
//! ## BigQuery load model — one free path
//!
//! Batch-loading Parquet from GCS is **free** in BigQuery (load jobs use the
//! ingestion slot pool, not query slots). The loader declares each column's
//! native `target_type` **inline in the `LOAD DATA` statement**, e.g.
//!
//! ```sql
//! LOAD DATA OVERWRITE `p.d.t` (id INT64, json_col JSON, dt_col DATETIME)
//! PARTITION BY d FROM FILES (format = 'PARQUET', uris = [...]);
//! ```
//!
//! With the schema declared, BigQuery **coerces the Parquet to native types on
//! load** — JSON, DATETIME (wall-clock), TIME, NUMERIC, … all land natively,
//! **for free** (a load job, not a query). No autoload-then-CTAS recovery is
//! needed. Verified live against a full MySQL type matrix: every column loaded
//! natively with `total_bytes_billed = 0`.
//!
//! (This corrects an earlier premise — the OSS resolver's `cast_sql` recovery
//! assumes a *bare* autoload rejects native types; declaring the schema in the
//! `LOAD DATA` statement itself coerces them for free. The one exception is a
//! *value* transform like UUID `bytes → TO_HEX(hex)`, which a type declaration
//! cannot perform; such a column lands as its declared type and may need a
//! downstream transform.)
//!
//! Idempotent under Rivet's at-least-once file delivery: `LOAD DATA OVERWRITE`
//! reproduces the same table on a retry.
//!
//! ## Two BigQuery limits this respects
//!
//! - `PARTITION BY` / `CLUSTER BY` apply **only when the table is created**;
//!   an overwrite must repeat them, and BigQuery refuses a changed spec — so an
//!   existing table's shape is read from `tables.get` and compared before the
//!   load (`table_shape_conflict`). Clustering is capped at 4 columns. Partition
//!   options (`partition_expiration_days`, `require_partition_filter`) are set
//!   at creation and changed in place with `ALTER TABLE SET OPTIONS`, since an
//!   overwrite that declares different ones is refused too (verified 2026-09-12).
//! - A single load *or* query job may modify at most **4,000 partitions**. The
//!   driver estimates the partitions a load touches from the Parquet footers
//!   before the job runs (`partition_budget`); BigQuery's own refusal is the
//!   backstop, surfaced as an actionable error.
//!
//! ## Cost attribution via job labels
//!
//! Every BigQuery job the loader creates is labeled so its cost is
//! automatically attributable: `managed_by:rivet`, `rivet_op:<load|count>`,
//! `rivet_table:<table>`, `rivet_run:<id>` (the load-run correlation id, when
//! set).
//! The batch ops are free load/metadata jobs (`total_bytes_billed = 0`); the
//! CDC path adds billed `merge` / `compact` ops on the same `run_sql(sql, op,
//! table)` seam, so a billed dedup step shows on its own cost line (see
//! `docs/cdc-bigquery-load.md`). The labels flow into
//! `INFORMATION_SCHEMA.JOBS` and the billing export, so cost per
//! operation/table is one query:
//!
//! ```sql
//! SELECT
//!   (SELECT value FROM UNNEST(labels) WHERE key = 'rivet_run')   AS run,
//!   (SELECT value FROM UNNEST(labels) WHERE key = 'rivet_op')    AS op,
//!   (SELECT value FROM UNNEST(labels) WHERE key = 'rivet_table') AS tbl,
//!   COUNT(*)                              AS jobs,
//!   SUM(total_bytes_billed)               AS bytes_billed,
//!   SUM(total_bytes_billed) / POW(1024, 4) * 6.25 AS est_usd  -- ~$6.25/TiB on-demand
//! FROM `region-us`.INFORMATION_SCHEMA.JOBS
//! WHERE EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key = 'managed_by' AND value = 'rivet')
//! GROUP BY run, op, tbl ORDER BY run, bytes_billed DESC;
//! ```
//!
//! Transport is BigQuery's REST API, in process (`bq_rest`) — `jobs.insert`
//! plus a poll, on the blocking `reqwest` client. Auth comes from the SAME ADC
//! seam the GCS destination signs with (`gcp_auth`), so a laptop
//! with `gcloud auth application-default login`, a container with a
//! service-account key file in `GOOGLE_APPLICATION_CREDENTIALS`, and a CI box
//! with a token all work without the Google Cloud SDK on PATH — and all three
//! run the job as the identity the operator configured, which is the property
//! that matters (a load acting as a different principal has an audit trail
//! that is fiction). The remaining shape rivet cannot mint in process
//! (`external_account` / workload identity, which needs an STS exchange) falls
//! back to `gcloud auth print-access-token` — a TOKEN, not the transport; see
//! `bq_rest::mint_token_via_gcloud_cli`.

use super::TargetLoader;
use super::bq_rest::BigQueryApi;
use crate::load::plan::{Clustering, Granularity, PartitionForm, PartitionKey, TablePartition};
use crate::types::target::{BqType, TargetColumnSpec, TargetType};
use anyhow::{Context as _, Result};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

mod ddl;
mod shape;
#[cfg(test)]
mod tests;

use ddl::*;
use shape::*;

/// Maximum clustering columns BigQuery allows.
pub(crate) const MAX_CLUSTER_COLUMNS: usize = 4;

/// Loads Rivet Parquet into a BigQuery dataset over the REST API.
#[derive(Debug, Clone)]
pub struct BigQueryLoader {
    pub project: String,
    pub dataset: String,
    /// The resolved `load.partition` of the table the load writes.
    pub partition: Option<TablePartition>,
    /// Up to 4 clustering columns, and whether the config wrote them.
    pub clustering: Clustering,
    /// Load-run correlation id, emitted as the automatic `rivet_run:<id>` job label so every job of one `rivet load` invocation shares a run key.
    pub run_id: Option<String>,
    /// The destination the Parquet is staged under.
    pub destination: Option<crate::config::DestinationConfig>,
    /// The CDC layout: under `BaseAndBuffer`, `<table>__changes` is a per-cycle buffer.
    pub layout: crate::load::plan::CdcLayout,
    /// The REST client, built on first use and shared by every clone.
    api: Arc<OnceLock<BigQueryApi>>,
    /// Columns loaded under their Parquet name and renamed after, as (file name, warehouse name).
    renames: Vec<crate::load::plan::Rename>,
    /// Where the client records every job it finishes.
    journal: crate::load::result::JobJournal,
}

impl BigQueryLoader {
    pub fn new(project: impl Into<String>, dataset: impl Into<String>) -> Self {
        Self {
            project: project.into(),
            dataset: dataset.into(),
            partition: None,
            clustering: Clustering::Auto(Vec::new()),
            run_id: None,
            layout: crate::load::plan::CdcLayout::LogAndView,
            destination: None,
            api: Arc::new(OnceLock::new()),
            renames: Vec::new(),
            journal: Default::default(),
        }
    }

    /// Record every job this loader runs in `journal`.
    pub fn journaled(mut self, journal: crate::load::result::JobJournal) -> Self {
        self.journal = journal;
        self
    }

    /// Partition the table the load creates.
    pub fn partition(mut self, partition: TablePartition) -> Self {
        self.partition = Some(partition);
        self
    }

    /// The `PARTITION BY` expression, when the load partitions.
    fn partition_expr(&self) -> Option<&str> {
        self.partition.as_ref().map(|p| p.expr.as_str())
    }

    /// Set the load-run correlation id, emitted as the `rivet_run` job label.
    pub fn staged_under(mut self, dest: crate::config::DestinationConfig) -> Self {
        self.destination = Some(dest);
        self
    }

    /// Load these columns under their Parquet name and rename them to the warehouse name.
    pub fn renamed(mut self, renames: Vec<crate::load::plan::Rename>) -> Self {
        self.renames = renames;
        self
    }

    /// Append `batches` to `changes` one job at a time.
    fn append_renamed(
        &self,
        changes: &str,
        specs: &[TargetColumnSpec],
        batches: &[Vec<String>],
        partition: Option<&TablePartition>,
    ) -> Result<()> {
        let staging = format!("{changes}__staging");
        let staging_fqtn = self.fqtn(&staging);
        let schema = build_file_schema(specs, &self.renames);
        let partition_expr = partition.map(|p| p.expr.as_str());
        let drop = format!("DROP TABLE IF EXISTS `{staging_fqtn}`;");
        for batch in batches {
            self.run_sql(&drop, "load", changes)?;
            let load = build_load_data_sql(
                &staging_fqtn,
                true,
                &schema,
                partition_expr,
                self.cluster_by(),
                None,
                batch,
            );
            self.run_sql(&load, "load", changes)?;
            if let Some(rename) = build_rename_columns_sql(&staging_fqtn, &self.renames) {
                self.run_sql(&rename, "load", changes)?;
            }
            self.api()?.copy_append(
                &self.dataset,
                &staging,
                changes,
                &self.labels("load", changes),
            )?;
        }
        self.run_sql(&drop, "load", changes)
    }

    /// The CDC layout the load writes (see the field).
    pub fn layout(mut self, layout: crate::load::plan::CdcLayout) -> Self {
        self.layout = layout;
        self
    }

    /// The load jobs `uris` need under the TARGET's partition.
    fn batches(
        &self,
        uris: &[String],
        partition: Option<&TablePartition>,
    ) -> Result<Vec<Vec<String>>> {
        let keyed = partition.filter(|p| p.key.column().is_some());
        match self.destination.as_ref().zip(keyed) {
            Some((dest, partition)) => {
                let store = crate::load::open_store(dest)?;
                crate::load::partition_budget::plan_load_batches(&store, uris, partition)
            }
            None => Ok(vec![uris.to_vec()]),
        }
    }

    pub fn run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = Some(id.into());
        self
    }

    /// Cluster the table the load creates on `columns`, as a written clustering.
    #[cfg(test)]
    pub fn clustered_on(mut self, columns: Vec<String>) -> Self {
        self.clustering = Clustering::Written(columns);
        self
    }

    /// The clustering columns.
    pub(crate) fn cluster_by(&self) -> &[String] {
        self.clustering.columns()
    }

    /// The REST client, built once per loader (and shared with its clones).
    fn api(&self) -> Result<&BigQueryApi> {
        if let Some(api) = self.api.get() {
            return Ok(api);
        }
        let credential = match &self.destination {
            Some(dest) => crate::config::DestinationCredential::choose(dest, "destination")?,
            None => crate::config::DestinationCredential::Ambient,
        };
        let built = BigQueryApi::for_dataset(&self.project, &self.dataset, &credential)?
            .journaled(self.journal.clone());
        let _ = self.api.set(built);
        Ok(self.api.get().expect("the client was just set"))
    }

    /// The automatic + user labels for a job, keyed for `configuration.labels`.
    fn labels(&self, op: &str, table: &str) -> BTreeMap<String, String> {
        build_labels(op, table, self.run_id.as_deref())
    }

    /// Run a SQL statement (free `LOAD DATA` load job or a billed CTAS/query), tagged with `rivet_op:<op>` + `rivet_table:<table>` for cost attribution.
    fn run_sql(&self, sql: &str, op: &str, table: &str) -> Result<()> {
        self.api()?
            .run_query(sql, &self.labels(op, table))
            .map_err(augment_partition_limit)
            .map(|_job_id| ())
    }

    /// Rows in `table`: table metadata for a table, a `COUNT(*)` for a view.
    fn count_rows(&self, table: &str) -> Result<u64> {
        let api = self.api()?;
        match api.table_num_rows(&self.dataset, table)? {
            Some(rows) => Ok(rows),
            None => api.run_query_scalar(
                &format!("SELECT COUNT(*) AS n FROM `{}`", self.fqtn(table)),
                &self.labels("load", table),
            ),
        }
    }

    /// Refuse a clustering list BigQuery would reject or that is not a plain identifier.
    fn check_cluster_by(&self) -> Result<()> {
        check_cluster_columns(self.cluster_by())
    }

    /// The partitioning, clustering and partition options of `table` from `tables.get`, or `None` when it is no base table.
    fn existing_shape(&self, table: &str) -> Result<Option<TableShape>> {
        Ok(self
            .api()?
            .table_metadata(&self.dataset, table)?
            .as_ref()
            .map(parse_table_shape))
    }
}

impl TargetLoader for BigQueryLoader {
    fn changes_has_prior_changes(&self, table: &str) -> Result<bool> {
        let fqtn = self.fqtn(&format!("{table}__changes"));
        let sql = format!(
            "SELECT COUNT(*) AS n FROM (SELECT 1 FROM `{fqtn}` WHERE __op IS NOT NULL LIMIT 1)"
        );
        match self
            .api()?
            .run_query_scalar(&sql, &self.labels("load", table))
        {
            Ok(n) => Ok(n > 0),
            Err(e) if format!("{e:#}").contains("Not found") => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn fqtn(&self, table: &str) -> String {
        format!("{}.{}.{}", self.project, self.dataset, table)
    }

    fn object_kind(&self, table: &str) -> Result<super::ObjectKind> {
        self.api()?.object_kind(&self.dataset, table)
    }

    fn column_overlap(&self, table: &str, names: &[&str]) -> Result<(u64, u64)> {
        let (total, matched) = build_column_overlap_sql(&self.project, &self.dataset, table, names);
        let (api, labels) = (self.api()?, self.labels("load", table));
        Ok((
            api.run_query_scalar(&total, &labels)?,
            api.run_query_scalar(&matched, &labels)?,
        ))
    }

    fn row_count(&self, table: &str) -> Result<u64> {
        self.count_rows(table)
    }

    fn adopt_as_changelog(&self, table: &str) -> Result<()> {
        let src = self.fqtn(table);
        let changes = self.fqtn(&format!("{table}__changes"));
        let shape = self.existing_shape(table)?.unwrap_or_default();
        for sql in build_adoption_sql(&src, table, &changes, &shape) {
            self.run_sql(&sql, "load", table)?;
        }
        if shape.require_partition_filter {
            eprintln!(
                "  note: `{changes}` does not require a partition filter, unlike `{src}` did — \
                 the current-state view reads all of it"
            );
        }
        if shape.expires_load_dates() {
            eprintln!(
                "  note: `{changes}` keeps its load-date partitions without expiry — expiring them \
                 would drop rows that never changed from the current-state view"
            );
        }
        if keeps_own_clustering(&shape, self.cluster_by(), self.clustering.is_written()) {
            eprintln!(
                "  note: `{changes}` keeps the clustering `{src}` had ({}); a written `cluster_by` \
                 would re-cluster it",
                shape.cluster.join(", ")
            );
        }
        Ok(())
    }

    fn shape(&self) -> Option<&dyn super::ShapeControl> {
        Some(self)
    }

    fn materialize(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        uris: &[String],
        _expected_rows: Option<u64>,
    ) -> Result<u64> {
        self.check_cluster_by()?;
        let target = self.fqtn(table);
        let schema = build_file_schema(specs, &self.renames);

        let existing = self.existing_shape(table)?;
        let options = creation_options(existing.is_none(), self.partition.as_ref());
        let cluster = table_clustering(&self.clustering, existing.as_ref());
        check_cluster_columns(cluster)?;
        if uris.is_empty() {
            eprintln!("{}", empty_run_note(&target, existing.is_some()));
            let sql = build_empty_table_sql(
                &target,
                existing.is_some(),
                &build_schema(specs),
                self.partition_expr(),
                cluster,
                options.as_deref(),
            );
            self.run_sql(&sql, "load", table)?;
            return self.count_rows(table);
        }
        let batches = self.batches(uris, self.partition.as_ref())?;
        if loads_directly(&self.renames, &batches) {
            let sql = build_load_data_sql(
                &target,
                true,
                &schema,
                self.partition_expr(),
                cluster,
                options.as_deref(),
                uris,
            );
            self.run_sql(&sql, "load", table)?;
            if let Some(alter) = options_drift(&target, existing.as_ref(), self.partition.as_ref())
            {
                self.run_sql(&alter, "load", table)?;
                eprintln!("  note: `{target}` partition options changed: {alter}");
            }
            return self.count_rows(table);
        }
        let staging = format!("{table}__staging");
        let staging_fqtn = self.fqtn(&staging);
        eprintln!(
            "  {target}: {} files in {} load jobs (≤ {} partitions each) via `{staging_fqtn}`",
            uris.len(),
            batches.len(),
            crate::load::partition_budget::MAX_PARTITIONS_PER_JOB
        );
        self.run_sql(
            &format!("DROP TABLE IF EXISTS `{staging_fqtn}`;"),
            "load",
            table,
        )?;
        let fresh = creation_options(true, self.partition.as_ref());
        if let Some((first, rest)) = batches.split_first() {
            let sql = build_load_data_sql(
                &staging_fqtn,
                true,
                &schema,
                self.partition_expr(),
                cluster,
                fresh.as_deref(),
                first,
            );
            self.run_sql(&sql, "load", table)?;
            for batch in rest {
                let sql =
                    build_load_data_sql(&staging_fqtn, false, &schema, None, &[], None, batch);
                self.run_sql(&sql, "load", table)?;
            }
        }
        if let Some(rename) = build_rename_columns_sql(&staging_fqtn, &self.renames) {
            self.run_sql(&rename, "load", table)?;
        }
        self.run_sql(&build_clone_sql(&target, &staging_fqtn), "load", table)?;
        self.run_sql(&format!("DROP TABLE `{staging_fqtn}`;"), "load", table)?;
        self.count_rows(table)
    }

    fn append_changelog(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        uris: &[String],
        _pk: &[String],
    ) -> Result<u64> {
        use crate::load::cdc::Warehouse;
        self.check_cluster_by()?;
        let mut full = crate::load::cdc::meta_column_specs(Warehouse::BigQuery);
        full.extend(
            specs
                .iter()
                .filter(|s| !is_meta_column(&s.column_name))
                .cloned(),
        );
        let schema = build_schema(&full);

        let changes = format!("{table}__changes");
        let changes_fqtn = self.fqtn(&changes);

        let log_partition_decl = if self.layout.log_is_disposable() {
            None
        } else {
            self.partition.as_ref()
        };
        let existed = self
            .api()?
            .table_metadata(&self.dataset, &changes)?
            .is_some();
        let create = build_create_changes_sql(
            &changes_fqtn,
            &schema,
            log_partition_decl,
            self.cluster_by(),
        );
        self.run_sql(&create, "load", &changes)?;

        let alter = if existed {
            build_alter_add_columns_sql(&changes_fqtn, &full)
        } else {
            None
        };
        if let Some(alter) = alter {
            self.run_sql(&alter, "load", &changes)?;
        }
        let log_partition = self.partition.as_ref().map(changelog_partition);
        let settle = if self.layout.log_is_disposable() {
            None
        } else {
            options_drift(
                &changes_fqtn,
                self.existing_shape(&changes)?.as_ref(),
                log_partition.as_ref(),
            )
        };
        if let Some(alter) = settle {
            self.run_sql(&alter, "load", &changes)?;
            eprintln!("  note: `{changes_fqtn}` partition options changed: {alter}");
        }

        let before = self.count_rows(&changes)?;
        if uris.is_empty() {
            return Ok(0);
        }
        let batches = self.batches(uris, log_partition_decl)?;
        if let [_, _, ..] = batches.as_slice() {
            eprintln!(
                "  {changes_fqtn}: {} files in {} append jobs (≤ {} partitions each)",
                uris.len(),
                batches.len(),
                crate::load::partition_budget::MAX_PARTITIONS_PER_JOB
            );
        }
        if self.renames.is_empty() {
            for batch in &batches {
                let load =
                    build_load_data_sql(&changes_fqtn, false, &schema, None, &[], None, batch);
                self.run_sql(&load, "load", &changes)?;
            }
        } else {
            self.append_renamed(&changes, &full, &batches, log_partition_decl)?;
        }
        let after = self.count_rows(&changes)?;
        Ok(after.saturating_sub(before))
    }

    fn flag_absent_from_buffer(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        pk: &[String],
    ) -> Result<u64> {
        self.append_changelog(table, specs, &[], pk)?;
        let changes = format!("{table}__changes");
        let columns: Vec<&str> = specs
            .iter()
            .map(|s| s.column_name.as_str())
            .filter(|c| !is_meta_column(c))
            .collect();
        let keys: Vec<&str> = pk.iter().map(String::as_str).collect();
        let sql = crate::load::cdc::absent_as_deleted_sql(
            crate::load::cdc::Warehouse::BigQuery,
            &self.fqtn(table),
            &self.fqtn(&changes),
            &columns,
            &keys,
        );
        let before = self.count_rows(&changes)?;
        self.run_sql(&sql, "load", &changes)?;
        Ok(self.count_rows(&changes)?.saturating_sub(before))
    }

    fn warehouse(&self) -> crate::load::cdc::Warehouse {
        crate::load::cdc::Warehouse::BigQuery
    }

    fn compact(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        pk: &[String],
        order: crate::load::cdc::CompactOrder,
    ) -> Result<crate::load::CompactReport> {
        use crate::load::cdc::{
            CompactArm, CompactProbe, compact_arm, compact_probe_sql, compact_script_sql,
            plan_compact_merges,
        };
        let base = self.fqtn(table);
        let changes = format!("{table}__changes");
        let changes_fqtn = self.fqtn(&changes);
        let api = crate::load::before_write(self.api())?;

        let key = self.partition.as_ref().map(|p| &p.key);
        let arm = compact_arm(key);

        let merging = format!("{table}__changes__merging");
        let merging_fqtn = self.fqtn(&merging);
        let leftover =
            crate::load::before_write(api.table_metadata(&self.dataset, &merging))?.is_some();
        let buffer = crate::load::before_write(api.table_metadata(&self.dataset, &changes))?;
        if nothing_to_compact(buffer.is_some(), leftover) {
            return Ok(crate::load::CompactReport {
                base,
                changes_rows: 0,
                merge_jobs: 0,
                had_buffer: false,
            });
        }
        if let Some(alter) = build_alter_add_columns_sql(&base, specs) {
            self.run_sql(&alter, "merge", table)?;
        }
        let mut recovered_rows = 0u64;
        let mut recovered_jobs = 0usize;
        if leftover {
            eprintln!(
                "  note: `{merging_fqtn}` is left over from a compaction that did not complete \
                 (its job died, or its MERGE failed — see that compact's error) — merging it \
                 before this run's buffer"
            );
            let script = compact_script_sql(
                &base,
                &merging_fqtn,
                None,
                specs,
                pk,
                order.clone(),
                arm.recovery_day_column(),
            );
            let row = api.run_query_first_row(&script, &self.labels("merge", table))?;
            let (rows, jobs) = compact_summary(&row)?;
            recovered_rows = rows;
            recovered_jobs = jobs;
        }

        let Some(buffer) = buffer else {
            return Ok(crate::load::CompactReport {
                base,
                changes_rows: 0,
                merge_jobs: 0,
                had_buffer: false,
            }
            .with_recovered(recovered_rows, recovered_jobs));
        };
        if buffer.get("numRows").and_then(serde_json::Value::as_str) == Some("0") {
            self.run_sql(&format!("DROP TABLE `{changes_fqtn}`;"), "merge", table)?;
            return Ok(crate::load::CompactReport {
                base,
                changes_rows: 0,
                merge_jobs: 0,
                had_buffer: true,
            }
            .with_recovered(recovered_rows, recovered_jobs));
        }
        crate::test_hook::maybe_panic_at("compact_before_merge");
        if let CompactArm::Scripted { day_column } = arm {
            let script = compact_script_sql(
                &base,
                &merging_fqtn,
                Some(crate::load::cdc::CompactRename {
                    from_fqtn: &changes_fqtn,
                    to_bare: &merging,
                }),
                specs,
                pk,
                order,
                day_column,
            );
            let row = api.run_query_first_row(&script, &self.labels("merge", table))?;
            let (changes_rows, merge_jobs) = compact_summary(&row)?;
            crate::test_hook::maybe_panic_at("compact_after_merge");
            return Ok(crate::load::CompactReport {
                base,
                changes_rows,
                merge_jobs,
                had_buffer: true,
            }
            .with_recovered(recovered_rows, recovered_jobs));
        }
        let part_col = key.and_then(PartitionKey::column);
        let time_type = matches!(key, Some(PartitionKey::Time { .. })).then(|| {
            part_col.map_or(BqType::Timestamp.into(), |c| {
                crate::load::cdc::column_type_of(specs, c)
            })
        });
        let probe = compact_probe_sql(
            &changes_fqtn,
            &base,
            pk,
            part_col,
            time_type.as_ref(),
            crate::load::cdc::trusts_buffer_days(&order),
        );
        let row = self
            .api()?
            .run_query_first_row(&probe, &self.labels("merge", table))?;
        let cell = |i: usize| row.get(i).cloned().flatten().unwrap_or_default();
        let probe = CompactProbe {
            rows: cell(0).parse().unwrap_or(0),
            lo: cell(1),
            hi: cell(2),
            nulls: cell(3).parse().unwrap_or(0),
            in_lo: cell(4),
            in_hi: cell(5),
        };
        let changes_rows = probe.rows;
        let merges = plan_compact_merges(&base, &changes_fqtn, specs, pk, order, key, &probe)?;
        for sql in &merges {
            self.run_sql(sql, "merge", table)?;
        }
        crate::test_hook::maybe_panic_at("compact_after_merge");
        self.run_sql(&format!("DROP TABLE `{changes_fqtn}`;"), "merge", table)?;
        Ok(crate::load::CompactReport {
            base,
            changes_rows,
            merge_jobs: merges.len(),
            had_buffer: true,
        }
        .with_recovered(recovered_rows, recovered_jobs))
    }

    fn create_view(&self, table: &str, view_sql: &str) -> Result<()> {
        self.run_sql(view_sql, "load", table)?;
        Ok(())
    }
}

impl BigQueryLoader {
    /// How `object` differs from the declared partition/clustering, or `None`.
    fn drift_of(&self, object: &str) -> Result<Option<super::ChangelogDrift>> {
        let declared_cluster = self.clustering.is_written().then_some(self.cluster_by());
        Ok(self.existing_shape(object)?.and_then(|shape| {
            classify_drift(
                &shape,
                self.partition.as_ref().map(|p| &p.key),
                declared_cluster,
            )
        }))
    }
}

impl super::ShapeControl for BigQueryLoader {
    fn table_shape_conflict(&self, table: &str) -> Result<Option<String>> {
        let want = self.partition.as_ref().map(|p| &p.key);
        let written = self.clustering.is_written().then_some(self.cluster_by());
        Ok(self
            .existing_shape(table)?
            .and_then(|shape| shape_conflict(&shape, want, written)))
    }

    fn changelog_drift(&self, table: &str) -> Result<Option<super::ChangelogDrift>> {
        self.drift_of(&format!("{table}__changes"))
    }

    fn adoption_drift(&self, table: &str) -> Result<Option<super::ChangelogDrift>> {
        self.drift_of(table)
    }

    fn recluster_changelog(&self, table: &str) -> Result<()> {
        let changes = format!("{table}__changes");
        self.api()?.patch_table(
            &self.dataset,
            &changes,
            &clustering_patch(self.cluster_by()),
        )?;
        Ok(())
    }

    fn rebuild_changelog(&self, table: &str) -> Result<()> {
        let changes = format!("{table}__changes");
        let rebuild = format!("{changes}__rebuild");
        let old = format!("{changes}__old");
        let api = self.api()?;
        let meta = api
            .table_metadata(&self.dataset, &changes)?
            .ok_or_else(|| {
                crate::error::coded(
                    crate::error::codes::LOAD_WAREHOUSE_OBJECT_NOT_FOUND,
                    format!("`{}` is not a table", self.fqtn(&changes)),
                )
            })?;
        let props = parse_table_props(&meta);
        let row_policies = match api.row_access_policy_count(&self.dataset, &changes) {
            Ok(n) => n,
            Err(e) => {
                eprintln!(
                    "  warning: could not list the row access policies of `{}` ({e:#}) — a \
                     rebuilt log carries none; check by hand",
                    self.fqtn(&changes)
                );
                0
            }
        };
        if let Some(why) = rebuild_policy_refusal(&self.fqtn(&changes), &props, row_policies) {
            return Err(super::refused(
                crate::error::codes::LOAD_CHANGELOG_PARTITION_CHANGED,
                why,
            ));
        }
        let before = self.count_rows(&changes)?;
        let copy = build_rebuild_copy_sql(
            &self.fqtn(&rebuild),
            &self.fqtn(&changes),
            self.partition.as_ref(),
            self.cluster_by(),
            &props,
        );
        self.run_sql(&copy, "load", table)?;
        let after = self.count_rows(&rebuild)?;
        if after != before {
            crate::rivet_bail!(
                crate::error::codes::LOAD_COPY_COUNT_MISMATCH,
                "`{}` holds {after} rows but `{}` holds {before} — the copy is left for \
                 inspection, nothing was swapped",
                self.fqtn(&rebuild),
                self.fqtn(&changes)
            );
        }
        for sql in build_rebuild_swap_sql(
            &self.fqtn(&changes),
            &self.fqtn(&rebuild),
            &self.fqtn(&old),
            &changes,
            &old,
        ) {
            self.run_sql(&sql, "load", table)?;
        }
        Ok(())
    }

    fn rebuild_leftovers(&self, table: &str) -> Result<Vec<String>> {
        if self.layout.log_is_disposable() {
            return Ok(Vec::new());
        }
        let changes = format!("{table}__changes");
        let names = [format!("{changes}__rebuild"), format!("{changes}__old")];
        let sql = build_leftovers_sql(&self.project, &self.dataset, &names);
        let code = self
            .api()?
            .run_query_scalar(&sql, &self.labels("load", table))?;
        Ok(leftover_names(code, &names)
            .iter()
            .map(|n| self.fqtn(n))
            .collect())
    }
}

/// The `PARTITION BY` expression for a `partition:` form, from the column's BigQuery type (ADR-0034 D3).
pub(crate) fn partition_expr(
    export: &str,
    form: &PartitionForm,
    column_type: &dyn Fn(&str) -> Result<TargetType>,
) -> Result<(PartitionKey, String)> {
    Ok(match form {
        PartitionForm::Column {
            column,
            granularity,
        } => {
            let t = column_type(column)?;
            let g = granularity.as_sql();
            let expr = match (&t, granularity) {
                (TargetType::BigQuery(BqType::Timestamp), _) => {
                    format!("TIMESTAMP_TRUNC(`{column}`, {g})")
                }
                (TargetType::BigQuery(BqType::DateTime), _) => {
                    format!("DATETIME_TRUNC(`{column}`, {g})")
                }
                (TargetType::BigQuery(BqType::Date), Granularity::Day) => format!("`{column}`"),
                (TargetType::BigQuery(BqType::Date), Granularity::Hour) => crate::rivet_bail!(
                    crate::error::codes::CONFIG_LOAD_PARTITION_UNSUPPORTED,
                    "export `{export}`: `{column}` is a DATE, which has no hours — partition it \
                     by day, month or year"
                ),
                (TargetType::BigQuery(BqType::Date), _) => {
                    format!("DATE_TRUNC(`{column}`, {g})")
                }
                _ => crate::rivet_bail!(
                    crate::error::codes::CONFIG_LOAD_PARTITION_UNSUPPORTED,
                    "export `{export}`: cannot partition on `{column}` ({}); BigQuery partitions \
                     a DATE, DATETIME or TIMESTAMP column by time, or an INT64 column with `range`",
                    crate::load::plan::base_type(&t)
                ),
            };
            (
                PartitionKey::Time {
                    column: Some(column.clone()),
                    granularity: *granularity,
                },
                expr,
            )
        }
        PartitionForm::Range {
            column,
            start,
            end,
            interval,
        } => {
            let t = column_type(column)?;
            if t != TargetType::BigQuery(BqType::Int64) {
                crate::rivet_bail!(
                    crate::error::codes::CONFIG_LOAD_PARTITION_UNSUPPORTED,
                    "export `{export}`: `range` partitions an INT64 column, and `{column}` is {}",
                    crate::load::plan::base_type(&t)
                );
            }
            (
                PartitionKey::Range {
                    column: column.clone(),
                    start: *start,
                    end: *end,
                    interval: *interval,
                },
                format!("RANGE_BUCKET(`{column}`, GENERATE_ARRAY({start}, {end}, {interval}))"),
            )
        }
        PartitionForm::Ingestion(g) => {
            let expr = match g {
                Granularity::Day => "_PARTITIONDATE".to_string(),
                _ => format!("TIMESTAMP_TRUNC(_PARTITIONTIME, {})", g.as_sql()),
            };
            (
                PartitionKey::Time {
                    column: None,
                    granularity: *g,
                },
                expr,
            )
        }
    })
}

/// Whether BigQuery can cluster a column of this native type.
pub(crate) fn clusterable(target_type: &TargetType) -> bool {
    matches!(
        target_type,
        TargetType::BigQuery(
            BqType::BigNumeric
                | BqType::Bool
                | BqType::Date
                | BqType::DateTime
                | BqType::Int64
                | BqType::Numeric
                | BqType::String
                | BqType::Timestamp
        )
    )
}

/// Whether a compaction has nothing to merge: no live buffer and no leftover of a merge that died.
fn nothing_to_compact(buffer: bool, leftover: bool) -> bool {
    !buffer && !leftover
}

/// The `(changes_rows, merge_jobs)` row the compaction script ends with.
fn compact_summary(row: &[Option<String>]) -> Result<(u64, usize)> {
    let cell = |i: usize, what: &str| -> Result<u64> {
        row.get(i)
            .cloned()
            .flatten()
            .and_then(|s| s.parse().ok())
            .with_context(|| {
                format!(
                    "compaction ran (buffer dropped) but its summary row had no readable \
                     {what}: {row:?}"
                )
            })
    };
    Ok((cell(0, "changes_rows")?, cell(1, "merge_jobs")? as usize))
}

fn is_meta_column(name: &str) -> bool {
    crate::load::cdc::is_meta_column(name)
}

/// The job's label SET: the automatic `managed_by:rivet` / `rivet_op:<op>` / `rivet_table:<table>` labels, plus `rivet_run:<id>` when a run id is set.
fn build_labels(op: &str, table: &str, run_id: Option<&str>) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::from([
        ("managed_by".to_string(), "rivet".to_string()),
        ("rivet_op".to_string(), sanitize_label(op)),
        ("rivet_table".to_string(), sanitize_label(table)),
    ]);
    if let Some(id) = run_id {
        labels.insert("rivet_run".to_string(), sanitize_label(id));
    }
    labels
}

/// Coerce a string into BigQuery's label charset.
fn sanitize_label(s: &str) -> String {
    let mapped: String = s
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if mapped.is_empty() {
        return "unnamed".to_string();
    }
    if mapped == s && mapped.len() <= MAX_LABEL_LEN {
        return mapped;
    }
    let digest = format!("{:08x}", xxhash_rust::xxh3::xxh3_64(s.as_bytes()) as u32);
    let mut out: String = mapped
        .chars()
        .take(MAX_LABEL_LEN - 1 - digest.len())
        .collect();
    out.push('-');
    out.push_str(&digest);
    out
}

/// BigQuery's cap on a label value.
const MAX_LABEL_LEN: usize = 63;

/// Turn BigQuery's partition-quota failure into an actionable error.
pub(crate) fn augment_partition_limit(e: anyhow::Error) -> anyhow::Error {
    let s = e.to_string().to_lowercase();
    if s.contains("partition")
        && (s.contains("4000") || s.contains("quota") || s.contains("exceed"))
    {
        return e.context(
            "BigQuery caps a single load/query job at 4,000 modified partitions — split the \
             Parquet URIs into batches whose partition span is <= 4,000 (e.g. load by date range)",
        );
    }
    e
}
