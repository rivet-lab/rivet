//! The SQL the BigQuery loader runs: `LOAD DATA`, the change-log DDL, the adoption and rebuild scripts, and the catalog probes.

use super::*;
use crate::load::plan::{Rename, file_name};

/// `CREATE TABLE IF NOT EXISTS` for the change log, partitioned as the load declares and clustered on `cluster_by` (capped at BigQuery's 4 clustering columns.
pub(super) fn build_create_changes_sql(
    fqtn: &str,
    schema: &str,
    partition: Option<&TablePartition>,
    cluster_by: &[String],
) -> String {
    let cluster: Vec<String> = cluster_by
        .iter()
        .take(MAX_CLUSTER_COLUMNS)
        .cloned()
        .collect();
    let options = partition.and_then(changelog_options_sql);
    format!(
        "CREATE TABLE IF NOT EXISTS `{fqtn}` (\n{schema}\n){};",
        table_shape_clauses(
            partition.map(|p| p.expr.as_str()),
            &cluster,
            options.as_deref()
        )
    )
}

/// The billed copy of the change log into `rebuild_fqtn`, shaped as the load declares and carrying the table properties a `CREATE TABLE … AS SELECT` would otherwise drop.
pub(super) fn build_rebuild_copy_sql(
    rebuild_fqtn: &str,
    changes_fqtn: &str,
    partition: Option<&TablePartition>,
    cluster_by: &[String],
    props: &TableProps,
) -> String {
    let mut options: Vec<String> = partition
        .and_then(changelog_options_sql)
        .into_iter()
        .collect();
    options.extend(table_props_options(props));
    let options = (!options.is_empty()).then(|| options.join(", "));
    format!(
        "CREATE TABLE `{rebuild_fqtn}`{}\nAS SELECT * FROM `{changes_fqtn}`;",
        table_shape_clauses(
            partition.map(|p| p.expr.as_str()),
            cluster_by,
            options.as_deref()
        )
    )
}

/// A double-quoted SQL string literal.
pub(super) fn sql_string(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Swap the rebuilt log in: the old one steps aside, the copy takes the name, the old one is dropped.
pub(super) fn build_rebuild_swap_sql(
    changes_fqtn: &str,
    rebuild_fqtn: &str,
    old_fqtn: &str,
    changes_name: &str,
    old_name: &str,
) -> Vec<String> {
    vec![
        format!("ALTER TABLE `{changes_fqtn}` RENAME TO {old_name};"),
        format!("ALTER TABLE `{rebuild_fqtn}` RENAME TO {changes_name};"),
        format!("DROP TABLE `{old_fqtn}`;"),
    ]
}

/// Probe whose scalar has bit `i` set when `names[i]` exists in the dataset.
pub(super) fn build_leftovers_sql(project: &str, dataset: &str, names: &[String]) -> String {
    let terms = names
        .iter()
        .enumerate()
        .map(|(i, n)| format!("{} * COUNTIF(table_name = '{n}')", 1u64 << i))
        .collect::<Vec<_>>()
        .join(" + ");
    format!("SELECT {terms} AS n FROM `{project}.{dataset}`.INFORMATION_SCHEMA.TABLES")
}

/// The names whose bit is set in the leftovers probe's scalar.
pub(super) fn leftover_names(code: u64, names: &[String]) -> Vec<String> {
    names
        .iter()
        .enumerate()
        .filter(|(i, _)| code & (1 << i) != 0)
        .map(|(_, n)| n.clone())
        .collect()
}

/// Probes counting all columns of `table` and those among `names`.
pub(super) fn build_column_overlap_sql(
    project: &str,
    dataset: &str,
    table: &str,
    names: &[&str],
) -> (String, String) {
    let from = format!(
        "FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table}'"
    );
    let list = if names.is_empty() {
        "''".to_string()
    } else {
        names
            .iter()
            .map(|n| format!("'{}'", n.to_lowercase()))
            .collect::<Vec<_>>()
            .join(", ")
    };
    (
        format!("SELECT COUNT(*) AS n {from}"),
        format!("SELECT COUNT(*) AS n {from} AND LOWER(column_name) IN ({list})"),
    )
}

/// Turn the full-load table into `<table>__changes` without a query.
pub(super) fn build_adoption_sql(
    src_fqtn: &str,
    table: &str,
    changes_fqtn: &str,
    shape: &TableShape,
) -> Vec<String> {
    let meta = crate::load::cdc::meta_column_specs(crate::load::cdc::Warehouse::BigQuery);
    let mut out = vec![format!(
        "ALTER TABLE `{src_fqtn}` RENAME TO {table}__changes;"
    )];
    out.extend(build_alter_add_columns_sql(changes_fqtn, &meta));
    if shape.require_partition_filter {
        out.push(format!(
            "ALTER TABLE `{changes_fqtn}` SET OPTIONS(require_partition_filter = false);"
        ));
    }
    if shape.expires_load_dates() {
        out.push(format!(
            "ALTER TABLE `{changes_fqtn}` SET OPTIONS(partition_expiration_days = NULL);"
        ));
    }
    out
}

/// Bring an EXISTING table's schema up to the declared one by ADDING what is missing.
pub(super) fn build_alter_add_columns_sql(
    fqtn: &str,
    specs: &[TargetColumnSpec],
) -> Option<String> {
    if specs.is_empty() {
        return None;
    }
    let adds = specs
        .iter()
        .map(|s| {
            format!(
                "ADD COLUMN IF NOT EXISTS `{}` {}",
                s.column_name, s.target_type
            )
        })
        .collect::<Vec<_>>()
        .join(",\n  ");
    Some(format!("ALTER TABLE `{fqtn}`\n  {adds};"))
}

/// `PARTITION BY … / CLUSTER BY … / OPTIONS(…)` clauses (empty when unset).
pub(super) fn table_shape_clauses(
    partition_expr: Option<&str>,
    cluster_by: &[String],
    options: Option<&str>,
) -> String {
    let mut s = String::new();
    if let Some(expr) = partition_expr {
        s.push_str(&format!("\nPARTITION BY {expr}"));
    }
    if !cluster_by.is_empty() {
        let quoted: Vec<String> = cluster_by.iter().map(|c| format!("`{c}`")).collect();
        s.push_str(&format!("\nCLUSTER BY {}", quoted.join(", ")));
    }
    if let Some(opts) = options {
        s.push_str(&format!("\nOPTIONS({opts})"));
    }
    s
}

/// Replace `target` with a zero-copy clone of `staging`.
pub(super) fn build_clone_sql(target: &str, staging: &str) -> String {
    format!("CREATE OR REPLACE TABLE `{target}` CLONE `{staging}`;")
}

/// A `FROM FILES(...)` Parquet source list.
pub(super) fn from_files(uris: &[String]) -> String {
    let list = uris
        .iter()
        .map(|u| format!("    '{u}'"))
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        "FROM FILES (\n  format = 'PARQUET',\n  enable_list_inference = true,\n  uris = [\n{list}\n  ]\n)"
    )
}

/// The BigQuery column schema declared inline in LOAD DATA, from each spec's native `target_type`.
pub(super) fn build_schema(specs: &[TargetColumnSpec]) -> String {
    specs
        .iter()
        .map(|s| format!("  `{}` {}", s.column_name, s.target_type))
        .collect::<Vec<_>>()
        .join(",\n")
}

/// [`build_schema`] under the Parquet's own column names, for a load whose `renames` map file names to warehouse names.
pub(super) fn build_file_schema(specs: &[TargetColumnSpec], renames: &[Rename]) -> String {
    let file_specs: Vec<TargetColumnSpec> = specs
        .iter()
        .map(|s| TargetColumnSpec {
            column_name: file_name(renames, &s.column_name).to_string(),
            ..s.clone()
        })
        .collect();
    build_schema(&file_specs)
}

/// One metadata-only statement renaming each file-named column to its warehouse name.
pub(super) fn build_rename_columns_sql(fqtn: &str, renames: &[Rename]) -> Option<String> {
    let clauses = renames
        .iter()
        .map(|(file, latin)| format!("RENAME COLUMN `{file}` TO `{latin}`"))
        .collect::<Vec<_>>();
    (!clauses.is_empty()).then(|| format!("ALTER TABLE `{fqtn}` {};", clauses.join(", ")))
}

/// Whether a load goes straight into its target: nothing to rename and at most one job.
pub(super) fn loads_directly(renames: &[Rename], batches: &[Vec<String>]) -> bool {
    renames.is_empty() && batches.len() <= 1
}

/// The free statement that leaves `fqtn` empty: `TRUNCATE` when it exists, else `CREATE` with the load's shape.
pub(super) fn build_empty_table_sql(
    fqtn: &str,
    exists: bool,
    schema: &str,
    partition_expr: Option<&str>,
    cluster_by: &[String],
    options: Option<&str>,
) -> String {
    if exists {
        return format!("TRUNCATE TABLE `{fqtn}`;");
    }
    let clauses = table_shape_clauses(partition_expr, cluster_by, options);
    format!("CREATE TABLE `{fqtn}` (\n{schema}\n){clauses};")
}

/// What a load of a run that exported no rows says it did to `fqtn`.
pub(super) fn empty_run_note(fqtn: &str, exists: bool) -> String {
    match exists {
        true => format!("  note: the newest run exported 0 rows — `{fqtn}` is emptied to match"),
        false => format!("  note: the newest run exported 0 rows — `{fqtn}` is created empty"),
    }
}

/// A free `LOAD DATA` batch-load statement declaring the native `schema`, so BigQuery coerces the Parquet to native types on load.
pub(super) fn build_load_data_sql(
    fqtn: &str,
    overwrite: bool,
    schema: &str,
    partition_expr: Option<&str>,
    cluster_by: &[String],
    options: Option<&str>,
    uris: &[String],
) -> String {
    let kw = if overwrite { "OVERWRITE" } else { "INTO" };
    let clauses = table_shape_clauses(partition_expr, cluster_by, options);
    format!(
        "LOAD DATA {kw} `{fqtn}` (\n{schema}\n){clauses}\n{};",
        from_files(uris)
    )
}
