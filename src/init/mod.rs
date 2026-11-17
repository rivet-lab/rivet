mod artifact;
mod candidates;
/// Offline strategy-decision replay harness — test-only (no runtime caller).
#[cfg(test)]
mod catalog_replay;
mod density;
mod mongo;
mod mssql;
mod mysql;
#[cfg(feature = "oracle")]
mod oracle;
mod postgres;
mod yaml_scaffold;

pub(crate) use artifact::{
    ChunkCandidate, CursorCandidate, CursorCandidateReason, DiscoveryArtifact, TableDiscovery,
};

use crate::error::Result;

/// Column metadata fetched from information_schema.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_primary_key: bool,
    /// `true` when the column can be NULL.
    pub is_nullable: bool,
    /// Filled for NUMERIC / DECIMAL columns from `information_schema.columns`.
    pub numeric_precision: Option<u32>,
    pub numeric_scale: Option<u32>,
    /// `true` when the column is backed by an index (PK, unique, or secondary).
    #[serde(default)]
    pub is_indexed: bool,
    /// Set when the keyset seam refuses the type the engine delivers this column as.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub not_keyset: bool,
    /// Set by an engine whose run refuses this column as an incremental cursor although its type name reads as one.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub not_cursor: bool,
    /// 1-based place in the source primary key, read from the source the way `rivet run` records it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_position: Option<u32>,
}

/// Table metadata used to generate the config scaffold and discovery artifact.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TableInfo {
    pub schema: String,
    pub table: String,
    pub row_estimate: i64,
    /// Approximate physical size in bytes (heap + indexes), if available.
    pub total_bytes: Option<i64>,
    pub columns: Vec<ColumnInfo>,
    /// First-run density probe (#148): when present, `row_estimate` above HAS BEEN REPLACED by the sampled figure (the catalog's original lives in `probe.catalog_rows`), so every downstream decision stands on the measured number automatically.
    #[serde(skip, default)]
    pub density: Option<crate::init::density::DensityProbe>,
    /// The SQL Server CDC capture instance `cdc.change_tables` holds for this table (the newest when two), read at init time.
    #[serde(skip, default)]
    pub cdc_capture_instance: Option<String>,
    /// Why Oracle LogMiner cannot capture this table, read at init time; a CDC scaffold leaves it out.
    #[serde(skip, default)]
    pub cdc_unmineable: Option<String>,
}

impl TableInfo {
    /// The primary key columns in key order; empty when the source key was not read.
    pub(crate) fn source_key(&self) -> Vec<&str> {
        let mut key: Vec<(u32, &str)> = self
            .columns
            .iter()
            .filter_map(|c| c.key_position.map(|p| (p, c.name.as_str())))
            .collect();
        key.sort_unstable();
        key.into_iter().map(|(_, n)| n).collect()
    }

    /// The range-chunk key: the integer PK, else the first INDEXED integer column (an unindexed one makes every window a full scan).
    pub(crate) fn best_chunk_column(&self) -> Option<&str> {
        self.columns
            .iter()
            .find(|c| c.is_primary_key && is_integer_type(&c.data_type))
            .or_else(|| {
                self.columns
                    .iter()
                    .find(|c| c.is_indexed && is_integer_type(&c.data_type))
            })
            .map(|c| c.name.as_str())
    }

    /// The single-column primary key, if the table has exactly one PK column.
    pub(crate) fn single_pk_column(&self) -> Option<&str> {
        let mut pks = self.columns.iter().filter(|c| c.is_primary_key);
        let first = pks.next()?;
        if pks.next().is_some() {
            return None;
        }
        Some(first.name.as_str())
    }

    /// Best candidate for `cursor_column`: prefer updated_at, then created_at, then any timestamp.
    pub(crate) fn chosen_cursor_column(&self) -> Option<String> {
        crate::init::candidates::cursor_candidates(self)
            .into_iter()
            .find(|c| !is_coarse_stamp_type(&c.data_type))
            .map(|c| c.column)
    }

    /// [`chosen_cursor_column`] RESTRICTED to a timestamp column.
    pub(crate) fn chosen_time_column(&self) -> Option<String> {
        crate::init::candidates::cursor_candidates(self)
            .into_iter()
            .find(|c| is_timestamp_type(&c.data_type))
            .map(|c| c.column)
    }

    /// Best candidate for the warehouse PARTITION column.
    pub(crate) fn best_partition_column(&self) -> Option<&str> {
        let ts: Vec<&ColumnInfo> = self
            .columns
            .iter()
            .filter(|c| is_timestamp_type(&c.data_type) && !is_tombstone_stamp(&c.name))
            .collect();
        ts.iter()
            .find(|c| is_creation_stamp(&c.name))
            .or_else(|| ts.iter().find(|c| !is_mutation_stamp(&c.name)))
            .or_else(|| ts.first())
            .map(|c| c.name.as_str())
    }

    pub(crate) fn best_cursor_column(&self) -> Option<&str> {
        let ts_cols: Vec<&ColumnInfo> = self
            .columns
            .iter()
            .filter(|c| {
                is_timestamp_type(&c.data_type)
                    && !c.not_cursor
                    && !is_tombstone_stamp(&c.name)
                    && !is_coarse_stamp_type(&c.data_type)
            })
            .collect();
        ts_cols
            .iter()
            .find(|c| is_mutation_stamp(&c.name))
            .or_else(|| ts_cols.iter().find(|c| c.name == "created_at"))
            .or_else(|| ts_cols.first())
            .map(|c| c.name.as_str())
    }

    /// The single-column PK when the keyset seam admits its delivered type.
    pub(crate) fn keysettable_pk_column(&self) -> Option<&str> {
        let pk = self.single_pk_column()?;
        let col = self.columns.iter().find(|c| c.name == pk)?;
        (!col.not_keyset).then_some(pk)
    }

    /// Whether the table has any key `chunked` can page by: an integer column or a keysettable PK.
    pub(crate) fn has_chunk_key(&self) -> bool {
        self.best_chunk_column().is_some() || self.keysettable_pk_column().is_some()
    }

    /// Suggest extraction mode based on row count and available columns.
    pub(crate) fn suggest_mode(&self) -> &'static str {
        if self.row_estimate > 100_000 {
            if self.has_chunk_key() {
                return "chunked";
            }
            if self.best_cursor_column().is_some() {
                return "incremental";
            }
        }
        "full"
    }

    /// One-line rationale for [`suggest_mode`]'s choice.
    pub(crate) fn mode_rationale(&self, mode: &str) -> String {
        match mode {
            "chunked" => {
                let why = if self.row_estimate > 100_000 {
                    "≥ 100K threshold and"
                } else {
                    "below the 100K threshold, but pageable because"
                };
                let base = format!(
                    "auto: ~{} rows {why} chunk column '{}' is available",
                    fmt_row_estimate(self.row_estimate),
                    self.best_chunk_column()
                        .or_else(|| self.keysettable_pk_column())
                        .unwrap_or("id"),
                );
                match self.best_cursor_column().filter(|c| is_mutation_stamp(c)) {
                    Some(cursor) => format!(
                        "{base}. NOTE: chunked re-reads the whole table each run — for scheduled \
                         re-runs, `mode: incremental` on '{cursor}' pulls only changed rows"
                    ),
                    None => base,
                }
            }
            "incremental" => {
                let on = match self.chosen_cursor_column() {
                    Some(cursor) => format!("incremental on '{cursor}'"),
                    None => "incremental — set cursor_column manually (no timestamp column \
                             detected)"
                        .to_string(),
                };
                match self.overridden_from(mode) {
                    Some(auto) => format!(
                        "~{} rows {}; auto would pick `{auto}`, written as {on}",
                        fmt_row_estimate(self.row_estimate),
                        threshold_phrase(self.row_estimate),
                    ),
                    None => format!(
                        "auto: ~{} rows ≥ 100K threshold; chunk column missing, falling back to {on}",
                        fmt_row_estimate(self.row_estimate),
                    ),
                }
            }
            "full" => {
                if let Some(auto) = self.overridden_from(mode) {
                    format!(
                        "~{} rows {}; auto would pick `{auto}`, written as a full scan",
                        fmt_row_estimate(self.row_estimate),
                        threshold_phrase(self.row_estimate),
                    )
                } else if self.row_estimate > 100_000 {
                    format!(
                        "auto: ~{} rows ≥ 100K but no chunk key or cursor column available — full scan",
                        fmt_row_estimate(self.row_estimate),
                    )
                } else {
                    format!(
                        "auto: ~{} rows below 100K chunked threshold",
                        fmt_row_estimate(self.row_estimate),
                    )
                }
            }
            _ => format!("mode={mode}"),
        }
    }

    /// `suggest_mode`'s pick when `mode` differs from it (a `--mode` override or a recipe), else `None`.
    pub(crate) fn overridden_from(&self, mode: &str) -> Option<&'static str> {
        let auto = self.suggest_mode();
        (auto != mode).then_some(auto)
    }

    /// Default chunk_size scaled by the row estimate.
    pub(crate) fn suggest_chunk_size(&self) -> u64 {
        match self.row_estimate {
            r if r < 1_000_000 => 100_000,
            r if r < 10_000_000 => 250_000,
            r if r < 100_000_000 => 1_000_000,
            _ => 2_500_000,
        }
    }

    /// Average bytes per row, when the catalog gave us `total_bytes`.
    pub(crate) fn avg_row_bytes(&self) -> Option<i64> {
        let probe_corrected = matches!(
            &self.density,
            Some(p) if matches!(
                p.method,
                crate::init::density::EstimateMethod::Probed
                    | crate::init::density::EstimateMethod::Counted
            )
        );
        match self.total_bytes {
            Some(b) if self.row_estimate > 0 && !probe_corrected => Some(b / self.row_estimate),
            _ => None,
        }
    }

    /// Enumerate ranked cursor candidates with structured reasons.
    pub(crate) fn cursor_candidates(&self) -> Vec<CursorCandidate> {
        candidates::cursor_candidates(self)
    }

    /// Enumerate chunk candidates (integer-typed columns, PK preferred).
    pub(crate) fn chunk_candidates(&self) -> Vec<ChunkCandidate> {
        candidates::chunk_candidates(self)
    }
}

/// Where `rows` sits against the 100K chunked threshold, as rationale text.
fn threshold_phrase(rows: i64) -> &'static str {
    if rows > 100_000 {
        "≥ 100K threshold"
    } else {
        "below the 100K threshold"
    }
}

/// Render a row estimate as a short human-readable string for inline YAML comments (`"1.0M"`, `"10K"`, `"950"`).
fn fmt_row_estimate(rows: i64) -> String {
    if rows >= 1_000_000 {
        format!("{:.1}M", rows as f64 / 1_000_000.0)
    } else if rows >= 1_000 {
        format!("{}K", rows / 1_000)
    } else {
        rows.to_string()
    }
}

fn is_integer_type(t: &str) -> bool {
    let t = t.to_lowercase();
    matches!(
        t.as_str(),
        "int"
            | "integer"
            | "bigint"
            | "int4"
            | "int8"
            | "int2"
            | "smallint"
            | "serial"
            | "bigserial"
            | "smallserial"
            | "tinyint"
            | "mediumint"
    )
}

/// Whether the type holds only a day or a minute.
pub(crate) fn is_coarse_stamp_type(t: &str) -> bool {
    let t = t.to_lowercase();
    t == "date" || t.contains("smalldatetime")
}

fn is_timestamp_type(t: &str) -> bool {
    let t = t.to_lowercase();
    t.contains("timestamp") || t.contains("datetime") || t == "date"
}

/// A column name folded for the stamp predicates.
fn stamp_key(name: &str) -> String {
    name.chars()
        .filter(|c| *c != '_')
        .flat_map(char::to_lowercase)
        .collect()
}

/// Whether the column name means "when the row came to BE".
pub(crate) fn is_creation_stamp(name: &str) -> bool {
    matches!(
        stamp_key(name).as_str(),
        "createdat"
            | "madeat"
            | "occurredat"
            | "createdon"
            | "createddate"
            | "creationdate"
            | "datecreated"
            | "createdtime"
    )
}

/// Whether the column name means "this stamp moves when the row CHANGES".
fn is_mutation_stamp(name: &str) -> bool {
    matches!(
        stamp_key(name).as_str(),
        "updatedat"
            | "modifiedat"
            | "changedat"
            | "lastupdated"
            | "lastmodified"
            | "lastchanged"
            | "lastupdate"
            | "updatedon"
            | "modifiedon"
            | "changedon"
            | "updatedate"
            | "modifieddate"
            | "datemodified"
            | "dateupdated"
            | "modifiedtime"
            | "updatedtime"
    )
}

/// Whether the column name marks a soft delete: NULL on every live row, so it can never be a cursor.
pub(crate) fn is_tombstone_stamp(name: &str) -> bool {
    matches!(
        stamp_key(name).as_str(),
        "deletedat" | "deletedon" | "deleteddate" | "removedat" | "archivedat" | "purgedat"
    )
}

/// `SELECT *` over the identifiers `parts` (schema, table), quoted for `source`.
pub(super) fn select_all(source: crate::config::SourceType, parts: &[&str]) -> String {
    let quoted: Vec<String> = parts
        .iter()
        .map(|p| crate::sql::quote_ident(source, p))
        .collect();
    format!("SELECT * FROM {}", quoted.join("."))
}

/// A table read from the catalog, with the cause its type probe failed when the user cannot SELECT it.
pub(super) type Introspected = (TableInfo, Option<String>);

/// `schema.table` of `info`, or the bare table when it has no schema.
fn table_label(info: &TableInfo) -> String {
    if info.schema.is_empty() {
        info.table.clone()
    } else {
        format!("{}.{}", info.schema, info.table)
    }
}

/// The stop for a table whose column types could not be read: coded, with the cause and the remedy.
fn keyset_probe_context(cause: &str, table: &str) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::INIT_TABLE_UNREADABLE,
        format!(
            "init: reading the column types of {table} to choose its keyset key failed: {cause}; grant SELECT on {table} or leave it out of init"
        ),
    )
}

/// The table of a single-table init, or the coded stop when its column types could not be read.
fn require_readable((info, unreadable): Introspected) -> Result<TableInfo> {
    match unreadable {
        Some(cause) => Err(keyset_probe_context(&cause, &table_label(&info))),
        None => Ok(info),
    }
}

/// The warning for a table a schema scan keeps without a keyset key because its column types could not be read.
fn unreadable_table_warning(table: &str, cause: &str) -> String {
    format!(
        "rivet: WARN [{}] init: reading the column types of {table} failed: {cause}; {table} is scaffolded without a keyset key; grant SELECT on {table} or leave it out of init\n",
        crate::error::codes::INIT_TABLE_UNREADABLE.id
    )
}

/// The stop for a schema scan in which no table could be read.
fn nothing_readable(tables: &[String]) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::INIT_TABLE_UNREADABLE,
        format!(
            "init: the column types of none of the {} table(s) in the scan could be read: {}; grant SELECT on them or point --schema at a schema the user can read",
            tables.len(),
            tables.join(", ")
        ),
    )
}

/// Mark the keyset refusals from the probe's types; when the probe failed mark every column and return the cause.
pub(super) fn probe_keyset_refusals(
    source: crate::config::SourceType,
    columns: &mut [ColumnInfo],
    probe: Result<Vec<crate::types::TypeMapping>>,
    declared_wider: impl FnOnce() -> Result<Vec<(String, crate::source::keyset_key::Wider)>>,
) -> Result<Option<String>> {
    match probe {
        Ok(mappings) => {
            let wider = declared_wider()?;
            mark_keyset_refusals(source, columns, &mappings, &wider);
            Ok(None)
        }
        Err(e) => {
            columns.iter_mut().for_each(|c| c.not_keyset = true);
            Ok(Some(format!("{e:#}")))
        }
    }
}

/// Set `not_keyset` on every column the keyset seam refuses, from the types the engine delivers for the table.
pub(super) fn mark_keyset_refusals(
    source: crate::config::SourceType,
    columns: &mut [ColumnInfo],
    mappings: &[crate::types::TypeMapping],
    declared_wider: &[(String, crate::source::keyset_key::Wider)],
) {
    for c in columns {
        c.not_keyset = seeded!(
            "keyset-key-init-mirror",
            mappings.iter().any(|m| {
                m.column_name == c.name
                    && matches!(m.rivet_type, crate::types::RivetType::Decimal { .. })
            }),
            crate::source::keyset_key::column_refusal(
                source,
                mappings,
                declared_wider,
                crate::source::keyset_key::KeyShape::Single,
                &c.name,
            )
            .is_some()
        );
    }
}

pub(super) fn source_type(source_url: &str) -> Result<&'static str> {
    source_type_of(source_url).map(crate::config::SourceType::label)
}

/// Refuse a forced `--mode` the source cannot run, in the config loader's own words.
fn refuse_unsupported_forced_mode(source_url: &str, mode: Option<&str>) -> Result<()> {
    match (source_type_of(source_url), mode) {
        (Ok(st), Some(m)) if !st.is_sql() && !matches!(m, "full" | "cdc") => crate::rivet_bail!(
            crate::error::codes::CONFIG_SOURCE_MODE_UNSUPPORTED,
            "init: --mode {m}: {}",
            crate::config::non_sql_mode_refusal(st, m)
        ),
        _ => Ok(()),
    }
}

/// The engine the source URL's scheme names ([`crate::config::SourceType::from_url_scheme`]).
pub(super) fn source_type_of(source_url: &str) -> Result<crate::config::SourceType> {
    let Some(t) = crate::config::SourceType::from_url_scheme(source_url) else {
        crate::rivet_bail!(
            crate::error::codes::CONFIG_SOURCE_URL_INVALID,
            "Unsupported source URL scheme. Expected postgresql://, mysql://, sqlserver://, mongodb://, or oracle://, got: {}",
            source_url
        )
    };
    Ok(t)
}

/// Default SQL Server schema when the user passes a bare table name.
fn mssql_table_schema(parsed_schema: &str) -> String {
    if parsed_schema == "public" {
        "dbo".to_string()
    } else {
        parsed_schema.to_string()
    }
}

/// Whole-schema include/exclude filtering for `rivet init` (L1).
#[derive(Debug, Clone, Default)]
pub struct TableFilter {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

impl TableFilter {
    /// `true` when `name` survives the filter (kept in the scaffold).
    pub(super) fn matches(&self, name: &str) -> bool {
        if self.exclude.iter().any(|g| glob_match(g, name)) {
            return false;
        }
        self.include.is_empty() || self.include.iter().any(|g| glob_match(g, name))
    }

    /// [`matches`](Self::matches) ignoring case, for Oracle, where an unquoted name folds upper-case.
    #[cfg_attr(not(feature = "oracle"), allow(dead_code))]
    pub(super) fn matches_folded(&self, name: &str) -> bool {
        let up = |v: &[String]| v.iter().map(|g| g.to_uppercase()).collect();
        TableFilter {
            include: up(&self.include),
            exclude: up(&self.exclude),
        }
        .matches(&name.to_uppercase())
    }
}

/// Minimal shell-style glob over the whole `name` (anchored at both ends).
fn glob_match(pattern: &str, name: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = name.chars().collect();
    let (mut p, mut t) = (0usize, 0usize);
    let (mut star, mut star_t) = (None, 0usize);
    while t < txt.len() {
        if p < pat.len() && (pat[p] == '?' || pat[p] == txt[t]) {
            p += 1;
            t += 1;
        } else if p < pat.len() && pat[p] == '*' {
            star = Some(p);
            star_t = t;
            p += 1;
        } else if let Some(sp) = star {
            p = sp + 1;
            star_t += 1;
            t = star_t;
        } else {
            return false;
        }
    }
    while p < pat.len() && pat[p] == '*' {
        p += 1;
    }
    p == pat.len()
}

/// Output format for `rivet init` (Epic B).
#[derive(Debug, Clone, Copy)]
pub enum InitFormat {
    /// YAML scaffold (default — backward compatible).
    Yaml,
    /// JSON discovery artifact for machine consumers (`rivet init --discover`).
    DiscoveryJson,
}

/// Optional cloud destination for YAML scaffolds (`--gcs-bucket` / `--s3-bucket` on `rivet init`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InitYamlDestination {
    pub gcs_bucket: Option<String>,
    /// If set, scaffold includes `credentials_file:`; if `None`, use ADC (no key line in YAML).
    pub gcs_credentials_file: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    /// `--bigquery-project` / `--bigquery-dataset`.
    pub bigquery_project: Option<String>,
    pub bigquery_dataset: Option<String>,
    /// `--clickhouse-url` / `--clickhouse-database`: the same, for a ClickHouse load.
    pub clickhouse_url: Option<crate::secret::Secret>,
    pub clickhouse_database: Option<String>,
    pub clickhouse_user: Option<String>,
    /// `--snowflake-*`: the same, for a Snowflake load (all five, or none).
    pub snowflake: Option<InitSnowflakeTarget>,
    /// The `[a-z0-9_]` stem of the `-o` file a CDC scaffold names its slot and `server_id` from, so two configs on one cluster never share them.
    pub cdc_name_stem: Option<String>,
    /// `--azure-container` / `--azure-account` / `--azure-endpoint`: an Azure Blob destination.
    pub azure: Option<InitAzureTarget>,
}

/// The Azure Blob container a scaffold stages in, its storage account, and an explicit endpoint (Azurite's `http://127.0.0.1:10000/devstoreaccount1`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InitAzureTarget {
    pub container: String,
    pub account: String,
    pub endpoint: Option<String>,
}

/// The values a Snowflake `load:` block needs — every one but the role is required by the loader.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InitSnowflakeTarget {
    /// The account identifier: `myorg-myaccount`, or a locator such as `xy12345.us-east-1`.
    pub account: String,
    pub user: String,
    /// The unencrypted PKCS#8 private key file.
    pub private_key_path: String,
    /// The role the load runs under; `None` uses the user's default.
    pub role: Option<String>,
    pub warehouse: String,
    pub database: String,
    pub schema: String,
    /// A pre-created GCS `STORAGE INTEGRATION` granting Snowflake read on the bucket.
    pub storage_integration: String,
}

impl InitYamlDestination {
    /// The partition granularity init guesses for the named warehouse and its reason; `None` without one.
    pub(crate) fn partition_guess(&self) -> Option<(&'static str, &'static str)> {
        if self.clickhouse_url.is_some() {
            Some((
                "month",
                "ClickHouse advises month or coarser; one INSERT block may touch at most 100 partitions",
            ))
        } else if self.snowflake.is_some() {
            Some((
                "day",
                "Snowflake has no partitions: this is a leading DATE_TRUNC clustering key, which \
                 turns on billed Automatic Clustering; drop it to cluster by the key alone",
            ))
        } else if self.bigquery_project.is_some() {
            Some((
                "day",
                "day holds ~4,000 partitions (11 years); use month for a longer history",
            ))
        } else {
            None
        }
    }

    pub fn validate(&self) -> Result<()> {
        let buckets = [
            self.gcs_bucket.is_some(),
            self.s3_bucket.is_some(),
            self.azure.is_some(),
        ];
        if buckets.iter().filter(|b| **b).count() > 1 {
            crate::rivet_bail!(
                crate::error::codes::CLI_OPTION_UNSUPPORTED,
                "use at most one of --gcs-bucket, --s3-bucket and --azure-container"
            );
        }
        Ok(())
    }

    /// Whether the scaffold stages in a bucket or container rather than a local directory.
    pub(crate) fn is_bucket(&self) -> bool {
        self.gcs_bucket.is_some() || self.s3_bucket.is_some() || self.azure.is_some()
    }
}

/// Entry point for `rivet init`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceProvenance {
    /// `--source <url>`: typed inline.
    Inline,
    /// `--source-env <NAME>`: write `url_env: NAME`.
    Env(String),
    /// `--source-file <PATH>`: write `url_file: PATH`.
    File(String),
}

#[allow(clippy::too_many_arguments)]
pub fn init(
    source_url: &str,
    provenance: &SourceProvenance,
    table: Option<&str>,
    schema: Option<&str>,
    output: Option<&str>,
    format: InitFormat,
    yaml_destination: InitYamlDestination,
    filter: &TableFilter,
    mode_override: Option<&str>,
    tls: Option<&crate::config::TlsConfig>,
) -> Result<()> {
    yaml_destination.validate()?;
    let mut yaml_destination = yaml_destination;
    if matches!(format, InitFormat::Yaml) {
        refuse_unsupported_forced_mode(source_url, mode_override)?;
        yaml_destination.cdc_name_stem = seeded!(
            "init-cdc-stream-names-shared",
            output.and(None),
            output.and_then(yaml_scaffold::output_stem)
        );
    }
    let (scaffold, snapshots) = match format {
        InitFormat::Yaml => init_yaml(
            tls,
            source_url,
            provenance,
            table,
            schema,
            &yaml_destination,
            filter,
            mode_override,
        )?,
        InitFormat::DiscoveryJson => {
            if yaml_destination != InitYamlDestination::default() {
                eprintln!(
                    "rivet: note: --gcs-bucket / --s3-bucket are ignored for --discover (JSON has no destination)"
                );
            }
            if mode_override.is_some() {
                eprintln!(
                    "rivet: note: --mode is ignored for --discover (the artifact reports a suggested_mode, not a chosen one); run without --discover to scaffold a YAML with that mode"
                );
            }
            (
                yaml_scaffold::Scaffold {
                    text: init_discovery_json(tls, source_url, table, schema, filter)?,
                    decisions: Default::default(),
                },
                Vec::new(),
            )
        }
    };
    let yaml_scaffold::Scaffold { text, decisions } = scaffold;
    let text = yaml_scaffold::for_source(text, source_type(source_url).unwrap_or_default(), || {
        pg_in_recovery(source_url, tls)
    });

    if matches!(format, InitFormat::Yaml) {
        refuse_nothing_exportable(&decisions, snapshots.len())?;
    }
    if let Some(notice) = cursor_notice(&decisions, mode_override, table.is_none()) {
        eprintln!("{notice}");
    }
    let runnable = decisions.needs_cursor.is_empty();
    eprint!("{}", marked_exports_warning(&decisions));

    match output {
        Some(path) => {
            require_explicit_state(path, crate::state::StateStore::names_a_server())?;
            refuse_cdc_names_taken(path, &text)?;
            write_config_output(path, &text)?;
            record_strategy_snapshots(path, &snapshots);
            record_primary_keys(path, source_url, tls, &snapshots);
            let label_written = match format {
                InitFormat::Yaml => "Config",
                InitFormat::DiscoveryJson => "Discovery artifact",
            };
            eprintln!("{label_written} written to {path}");
            if let Some(notice) = decimal_notice(&decisions) {
                eprintln!("{notice}");
            }
            if matches!(format, InitFormat::Yaml) && runnable {
                eprint!(
                    "{}{}",
                    next_steps_block(path, provenance, mode_override, &decisions),
                    no_load_note(&decisions)
                );
            }
        }
        None => {
            print!("{text}");
            if matches!(format, InitFormat::Yaml) && runnable {
                eprint!(
                    "{}{}",
                    next_steps_block("rivet.yaml", provenance, mode_override, &decisions),
                    no_load_note(&decisions)
                );
            }
        }
    }

    Ok(())
}

/// Refuse a scaffold that left out every table it found, naming each table with its reason.
fn refuse_nothing_exportable(d: &yaml_scaffold::ScaffoldDecisions, tables: usize) -> Result<()> {
    if tables == 0 || d.skipped.len() < tables {
        return Ok(());
    }
    crate::rivet_bail!(
        crate::error::codes::INIT_NOTHING_EXPORTABLE,
        "init left out every table it found ({}), so the config would hold no export; nothing was \
         written:\n  {}",
        d.skipped.join(", "),
        d.skip_reasons.join("\n  ")
    );
}

/// The next-steps line for a scaffold that left its `load:` block out because the loader refuses it.
fn no_load_note(d: &yaml_scaffold::ScaffoldDecisions) -> &'static str {
    if d.load_refused {
        "\nNo `load:` block: `rivet load` cannot take this source's CDC stream yet (the reason \
         is in the file). `rivet run` writes its Parquet; load those parts with your own tooling.\n"
    } else {
        ""
    }
}

/// The friendly "do this next" ladder printed after a YAML scaffold.
fn next_steps_block(
    path: &str,
    provenance: &SourceProvenance,
    mode: Option<&str>,
    d: &yaml_scaffold::ScaffoldDecisions,
) -> String {
    let yaml_scaffold::ScaffoldDecisions {
        has_baseline,
        has_load,
        compacts: has_compact,
        has_delta,
        ..
    } = *d;
    let mut s = String::from("\nNext steps:\n");
    if matches!(provenance, SourceProvenance::Inline) {
        s.push_str(
            "  0. export DATABASE_URL='<your-url>'    # the scaffold reads this (URL kept out of the file)\n",
        );
    }
    s.push_str(&format!(
        "  1. rivet doctor -c {path}            # test source + destination auth\n  \
         2. rivet check  -c {path}            # column-type & schema report\n  \
         3. rivet run    -c {path} --validate # export, then verify row counts\n"
    ));
    if mode == Some("cdc") && has_baseline {
        s.push_str(&format!(
            "\nThe first run anchors the stream and reads every table's baseline (through its \
             recipe under `backfill: auto`, whole under `initial: snapshot`); each later run \
             captures only the changes since. Put it on a schedule:\n  \
             rivet run   -c {path}                    # bounded (until_current) — safe to repeat\n"
        ));
    } else if mode == Some("cdc") {
        s.push_str(&format!(
            "\nThis stream captures CHANGES ONLY from its anchor on — the baseline is yours: add \
             `cdc.initial: snapshot`, or a batch export of the table plus `cdc.backfill: auto`. \
             Then put it on a schedule:\n  \
             rivet run   -c {path}                    # bounded (until_current) — safe to repeat\n"
        ));
    } else {
        s.push_str(&format!(
            "\nOr seal a reviewable plan, then apply it (runs many tables by priority wave):\n  \
             rivet plan  -c {path}                    # review the schedule (read-only)\n  \
             rivet plan  -c {path} --annotate-waves   # write wave:/parallel_safe: into the config\n  \
             rivet apply {path}                       # runs wave-by-wave (parallel where safe)\n"
        ));
    }
    if has_load && has_compact {
        s.push_str(&format!(
            "\nThen the warehouse half of the cycle (review `load:` first — its values are guesses):\n  \
             rivet load    -c {path}                  # Parquet -> the base, or the buffer on later runs\n  \
             rivet compact -c {path}                  # merge the buffer into the base and drop it\n"
        ));
    } else if has_load && has_delta {
        s.push_str(&format!(
            "\nThen the warehouse half (review `load:` first — its values are guesses):\n  \
             rivet load    -c {path}                  # a full export OVERWRITES its table; a cdc or \
             incremental one appends to <table>__changes behind the <table> view\n"
        ));
    } else if has_load {
        s.push_str(&format!(
            "\nThen the warehouse half (review `load:` first — its values are guesses):\n  \
             rivet load    -c {path}                  # Parquet -> the table; each load OVERWRITES it\n"
        ));
    }
    s
}

/// The `--table` introspection found no columns for `qualified`: not there, or not readable.
pub(super) fn table_not_found(qualified: &str) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::INIT_TABLE_NOT_FOUND,
        format!(
            "Table '{qualified}' not found or has no columns. \
             Check the table name and that the user has SELECT privilege."
        ),
    )
}

/// The "nothing to scaffold" error, filter-aware.
fn no_tables_error(filter: &TableFilter) -> anyhow::Error {
    if !filter.include.is_empty() || !filter.exclude.is_empty() {
        crate::error::coded(
            crate::error::codes::INIT_NO_TABLES,
            "No tables/views matched the --include/--exclude globs (or the schema is empty). \
             Check the globs — the same source without filters may discover tables — and also \
             verify --schema and privileges.",
        )
    } else {
        crate::error::coded(
            crate::error::codes::INIT_NO_TABLES,
            "No tables or views found (check --schema and privileges)",
        )
    }
}

/// Where a CDC export of `cfg` (a config in `dir`) keeps its stream: its checkpoint file and its destination.
fn cdc_places(
    cfg: &crate::config::Config,
    dir: &std::path::Path,
) -> Vec<(String, Option<std::path::PathBuf>, String)> {
    let canon =
        |p: std::path::PathBuf| crate::source::cdc::checkpoint_identity::canonical_place(&p);
    cfg.exports
        .iter()
        .filter(|e| e.cdc.is_some())
        .map(|e| {
            let ckpt = e
                .cdc
                .as_ref()
                .and_then(|c| c.checkpoint.as_deref())
                .map(|raw| canon(crate::source::cdc::resolve_checkpoint(raw, dir)));
            let d = &e.destination;
            let place = match &d.path {
                Some(p) => canon(dir.join(p)).display().to_string(),
                None => format!(
                    "{:?}://{}/{}",
                    d.destination_type,
                    d.bucket.as_deref().unwrap_or(""),
                    d.prefix.as_deref().unwrap_or("").trim_end_matches('/')
                ),
            };
            (e.name.clone(), ckpt, place)
        })
        .collect()
}

/// Refuse a scaffold whose CDC checkpoint already exists, or whose checkpoint or destination another config in the same directory names.
fn refuse_cdc_names_taken(path: &str, text: &str) -> Result<()> {
    let Ok(mine) = crate::config::Config::from_yaml(text) else {
        return Ok(());
    };
    let out = std::path::Path::new(path);
    let dir = out
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let taken = |what: String| {
        crate::error::coded(
            crate::error::codes::INIT_CDC_NAME_TAKEN,
            format!(
                "init: {what}. Nothing was written. Write the config under another `-o` name: \
                 `-o <other>.yaml` names its checkpoint and destination after the file"
            ),
        )
    };
    let ours = cdc_places(&mine, dir);
    let siblings = std::fs::read_dir(dir).into_iter().flatten().flatten();
    for entry in siblings {
        let other = entry.path();
        let yaml = other.extension().is_some_and(|x| x == "yaml" || x == "yml");
        if !yaml || other.file_name() == out.file_name() {
            continue;
        }
        let Ok(cfg) = crate::config::Config::load(&other.display().to_string()) else {
            continue;
        };
        for (theirs, ckpt, place) in cdc_places(&cfg, dir) {
            for (export, my_ckpt, my_place) in &ours {
                if my_ckpt.is_some() && my_ckpt == &ckpt {
                    return Err(taken(format!(
                        "export '{export}' would share its CDC checkpoint with export '{theirs}' \
                         of '{}'",
                        other.display()
                    )));
                }
                if my_place == &place {
                    return Err(taken(format!(
                        "export '{export}' would share its CDC destination `{place}` with export \
                         '{theirs}' of '{}'",
                        other.display()
                    )));
                }
            }
        }
    }
    for (export, ckpt, _) in &ours {
        if let Some(c) = ckpt.as_ref().filter(|c| c.exists()) {
            let owner = crate::source::cdc::checkpoint_identity::owner_of(c).map_or_else(
                || "a stream of an earlier config".to_string(),
                |o| {
                    let gone = match std::path::Path::new(&o.config).exists() {
                        true => "",
                        false => ", moved or deleted since",
                    };
                    format!("export '{}' of '{}'{gone}", o.export, o.config)
                },
            );
            return Err(taken(format!(
                "export '{export}' would keep its CDC position in '{}', which already exists: \
                 the checkpoint of {owner}",
                c.display()
            )));
        }
    }
    Ok(())
}

/// Write the scaffold to `-o <path>`, naming the path + cause on failure.
fn write_config_output(path: &str, text: &str) -> Result<()> {
    if std::path::Path::new(path).exists() {
        crate::rivet_bail!(
            crate::error::codes::INIT_CONFIG_EXISTS,
            "init: '{path}' already exists — refusing to overwrite a config that may \
             carry hand edits. Move it aside, or write elsewhere with `-o <new-path>`."
        );
    }
    std::fs::write(path, text).map_err(|e| {
        crate::error::coded(crate::error::codes::CLI_OUTPUT_UNWRITABLE, format!(
            "init: could not write config to '{path}': {e} \
             (check the parent directory exists and is writable; `-o` takes a file path, not a directory)"
        ))
    })
}

/// Resolve the effective schema for a single `--table`, honoring `--schema`.
fn resolve_single_table_schema<'a>(
    table: &'a str,
    schema_flag: Option<&str>,
) -> Result<(Option<String>, &'a str)> {
    let (parsed_schema, name) = yaml_scaffold::parse_table(table);
    let has_embedded = table.contains('.');
    match schema_flag.map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) if has_embedded && s != parsed_schema => crate::rivet_bail!(
            crate::error::codes::CLI_OPTION_UNSUPPORTED,
            "init: --table '{table}' already names schema '{parsed_schema}', but --schema is \
             '{s}' — remove one so they agree"
        ),
        Some(s) if !has_embedded => Ok((Some(s.to_string()), name)),
        _ => Ok((has_embedded.then_some(parsed_schema), name)),
    }
}

/// Introspect one `--table` (single-table init), honoring `--schema`.
fn introspect_single_table(
    tls: Option<&crate::config::TlsConfig>,
    source_url: &str,
    table: &str,
    schema_flag: Option<&str>,
    #[cfg_attr(not(feature = "oracle"), allow(unused_variables))] cdc: bool,
) -> Result<TableInfo> {
    let (eff_schema, table_name) = resolve_single_table_schema(table, schema_flag)?;
    Ok(match source_type(source_url)? {
        "postgres" => {
            let mut client = postgres::connect(source_url, tls)?;
            let mut info = require_readable(postgres::introspect(
                &mut client,
                eff_schema.as_deref().unwrap_or("public"),
                table_name,
            )?)?;
            postgres::density_probe(&mut client, &mut info);
            info
        }
        "mysql" => {
            let mut conn = mysql::connect(source_url, tls)?;
            if let Some(db) = eff_schema.as_deref() {
                if let Some(url_db) = mysql::resolve_database_for_listing(source_url, None).ok()
                    && url_db != db
                {
                    crate::rivet_bail!(
                        crate::error::codes::CLI_OPTION_UNSUPPORTED,
                        "init: --schema '{db}' selects a different MySQL database than the source \
                         URL ('{url_db}') — the generated config connects via the URL and would \
                         export the URL's database, not '{db}'. Put the database in the URL instead \
                         (mysql://…/{db})."
                    );
                }
                mysql::use_database(&mut conn, db)?;
            }
            let mut info = require_readable(mysql::introspect(&mut conn, table_name)?)?;
            mysql::density_probe(&mut conn, &mut info);
            info
        }
        "mssql" => {
            let mut conn = mssql::connect(source_url, tls)?;
            let mut info = require_readable(mssql::introspect(
                &mut conn,
                &mssql_table_schema(eff_schema.as_deref().unwrap_or("public")),
                table_name,
            )?)?;
            mark_mssql_catalog_exact(&mut info);
            info
        }
        #[cfg(feature = "oracle")]
        "oracle" => {
            let mut conn = oracle::connect(source_url, tls)?;
            let owner = oracle::resolve_schema(&mut conn, eff_schema.as_deref())?;
            require_readable(oracle::introspect(
                &mut conn,
                &owner,
                &crate::sql::oracle_catalog_name(table_name),
                cdc,
            )?)?
        }
        "mongo" => {
            reject_mongo_schema(schema_flag)?;
            let conn = mongo::connect(source_url, tls)?;
            mongo::introspect(&conn, table)?
        }
        _ => unreachable!(),
    })
}

/// MongoDB has no schema namespace (collections live directly in a database), so an explicit `--schema` cannot be honoured.
fn reject_mongo_schema(schema_flag: Option<&str>) -> Result<()> {
    if schema_flag.map(str::trim).is_some_and(|s| !s.is_empty()) {
        crate::rivet_bail!(
            crate::error::codes::CLI_OPTION_UNSUPPORTED,
            "init: --schema is not supported for MongoDB — a collection has no schema \
             namespace. Put the database in the connection URL instead (mongodb://…/<db>)."
        );
    }
    Ok(())
}

/// Whether the PostgreSQL server at `source_url` answers that it is in recovery (a standby).
fn pg_in_recovery(source_url: &str, tls: Option<&crate::config::TlsConfig>) -> bool {
    let probe = crate::source::postgres::connect_client(source_url, tls).and_then(|mut c| {
        Ok(c.query_one("SELECT pg_is_in_recovery()", &[])?
            .get::<_, bool>(0))
    });
    seeded!(
        "init-bounds-a-drain-on-a-standby",
        probe.is_err(),
        probe.unwrap_or(false)
    )
}

#[allow(clippy::too_many_arguments)]
fn init_yaml(
    tls: Option<&crate::config::TlsConfig>,
    source_url: &str,
    provenance: &SourceProvenance,
    table: Option<&str>,
    schema: Option<&str>,
    dest: &InitYamlDestination,
    filter: &TableFilter,
    mode_override: Option<&str>,
) -> Result<(yaml_scaffold::Scaffold, Vec<crate::state::StrategySnapshot>)> {
    if let Some(t) = table {
        let info = introspect_single_table(
            tls,
            source_url,
            t,
            schema,
            probes_mineability(mode_override),
        )?;
        let mut infos = vec![info];
        read_source_keys(&mut infos, source_url, dest, mode_override, |kind| {
            crate::source::connect(kind, source_url, tls, None)
        });
        let info = &infos[0];
        let snaps = vec![snapshot_of(info, mode_override, source_url)];
        let scaffold =
            yaml_scaffold::scaffold_table(info, source_url, provenance, dest, mode_override, tls)?;
        return Ok((scaffold, snaps));
    }
    let mut infos = introspect_all(
        tls,
        source_url,
        schema,
        filter,
        probes_mineability(mode_override),
    )?;
    if infos.is_empty() {
        return Err(no_tables_error(filter));
    }
    read_source_keys(&mut infos, source_url, dest, mode_override, |kind| {
        crate::source::connect(kind, source_url, tls, None)
    });
    let label = schema_scope_label(source_url, schema, infos.len())?;
    let scaffold = yaml_scaffold::scaffold_schema(
        &infos,
        source_url,
        provenance,
        &label,
        dest,
        mode_override,
        tls,
    )?;
    let snaps = infos
        .iter()
        .map(|i| snapshot_of(i, mode_override, source_url))
        .collect();
    Ok((scaffold, snaps))
}

/// The decision plus the EVIDENCE it was made from, for the state store.
fn mark_mssql_catalog_exact(info: &mut TableInfo) {
    let method = if info.row_estimate > 0 {
        crate::init::density::EstimateMethod::CatalogExact
    } else {
        crate::init::density::EstimateMethod::Unverified
    };
    info.density = Some(crate::init::density::DensityProbe {
        rows: info.row_estimate,
        density: 0.0,
        method,
        catalog_rows: info.row_estimate,
        k: 0,
        w: 0,
    });
}

/// The stderr line naming every export init could not give a cursor, when there is one.
fn cursor_notice(
    d: &yaml_scaffold::ScaffoldDecisions,
    mode: Option<&str>,
    whole_schema: bool,
) -> Option<String> {
    (!d.needs_cursor.is_empty())
        .then(|| cursor_missing_message(&d.needs_cursor, mode, whole_schema))
}

/// The stderr note for a scaffold that wrote the default `decimal(38,18)`, when it did.
fn decimal_notice(d: &yaml_scaffold::ScaffoldDecisions) -> Option<String> {
    d.decimal_review.then(|| {
        format!(
            "rivet: note: YAML uses default decimal(38,18) for column(s) with NUMERIC without (p,s) in the DDL — search for `{}` under columns: and fix before production.",
            yaml_scaffold::INIT_DECIMAL_REVIEW_MARKER
        )
    })
}

/// The line naming every export init could not give a cursor, with the ways out that apply to this invocation.
fn cursor_missing_message(names: &[String], mode: Option<&str>, whole_schema: bool) -> String {
    let key = if mode == Some("time_window") {
        "time_column"
    } else {
        "cursor_column"
    };
    let way_out = if whole_schema {
        format!(
            ", or re-run init excluding them (`--exclude {}`)",
            names.join(" ")
        )
    } else {
        ", or re-run init with another `--mode`".to_string()
    };
    format!(
        "rivet: {} export(s) have no timestamp column, so `{}` cannot give them a `{key}:` — \
         the config will NOT load until you set one for each (search for `{}`){way_out}: {}",
        names.len(),
        mode.unwrap_or("this mode"),
        yaml_scaffold::INIT_CURSOR_REVIEW_MARKER,
        names.join(", "),
    )
}

/// One line per informational decision, naming every export it was made for.
fn marked_exports_warning(d: &yaml_scaffold::ScaffoldDecisions) -> String {
    let mut out = String::new();
    if !d.skipped.is_empty() {
        out.push_str(&format!(
            "rivet: {} table(s) were left out of the config — see the `# SKIPPED` comments \
             for why: {}\n",
            d.skipped.len(),
            d.skipped.join(", ")
        ));
    }
    for (names, what) in [
        (
            &d.insert_only,
            "use a cursor that does not change on UPDATE: updated rows are never re-exported \
             (set `cursor_column:` to an updated_at-style column, or use `mode: cdc`)",
        ),
        (
            &d.no_chunk_key,
            "have no integer column or keysettable primary key, so they were written as \
             `mode: full` instead of `chunked`",
        ),
    ] {
        if !names.is_empty() {
            out.push_str(&format!(
                "rivet: {} export(s) {what}: {}\n",
                names.len(),
                names.join(", ")
            ));
        }
    }
    out
}

fn snapshot_of(
    info: &TableInfo,
    mode_override: Option<&str>,
    source_url: &str,
) -> crate::state::StrategySnapshot {
    let keyset_form =
        source_type(source_url).is_ok_and(|st| yaml_scaffold::table_form_ok(info, st));
    let d = yaml_scaffold::decided_strategy(info, mode_override, keyset_form);
    crate::state::StrategySnapshot {
        export_name: info.table.clone(),
        source_schema: (!info.schema.is_empty()).then(|| info.schema.clone()),
        source_table: info.table.clone(),
        row_estimate: info
            .density
            .as_ref()
            .map(|p| p.rows)
            .or((info.row_estimate > 0).then_some(info.row_estimate)),
        total_bytes: info.total_bytes,
        avg_row_bytes: info.avg_row_bytes(),
        chosen_mode: d.mode,
        strategy_kind: Some(d.kind.to_string()),
        key_column: d.key_column,
        chunk_size: d.chunk_size,
        catalog_rows: info.density.as_ref().map(|p| p.catalog_rows),
        density: info.density.as_ref().filter(|p| p.k > 0).map(|p| p.density),
        estimate_method: info.density.as_ref().map(|p| p.method.label().to_string()),
        probe_k: info
            .density
            .as_ref()
            .filter(|p| p.k > 0)
            .map(|p| p.k as i64),
        probe_w: info.density.as_ref().filter(|p| p.w > 0).map(|p| p.w),
    }
}

/// Whether the scaffold asks the source what its CDC capture can mine: only `--mode cdc` does.
fn probes_mineability(mode: Option<&str>) -> bool {
    mode == Some("cdc")
}

fn init_discovery_json(
    tls: Option<&crate::config::TlsConfig>,
    source_url: &str,
    table: Option<&str>,
    schema: Option<&str>,
    filter: &TableFilter,
) -> Result<String> {
    let (infos, scope) = if let Some(t) = table {
        let info = introspect_single_table(tls, source_url, t, schema, false)?;
        let scope = match source_type(source_url)? {
            "postgres" => format!("table \"{}\".\"{}\"", info.schema, info.table),
            "mysql" => format!("table `{}`", info.table),
            "mssql" => format!("table [{}].[{}]", info.schema, info.table),
            "oracle" => format!("table \"{}\".\"{}\"", info.schema, info.table),
            "mongo" => format!("collection {}", info.table),
            _ => unreachable!(),
        };
        (vec![info], scope)
    } else {
        let infos = introspect_all(tls, source_url, schema, filter, false)?;
        if infos.is_empty() {
            return Err(no_tables_error(filter));
        }
        let label = schema_scope_label(source_url, schema, infos.len())?;
        (infos, label)
    };

    let st = source_type(source_url)?;
    let tables: Vec<TableDiscovery> = infos.iter().map(table_discovery).collect();

    let artifact = DiscoveryArtifact {
        rivet_version: env!("CARGO_PKG_VERSION").to_string(),
        source_type: st.to_string(),
        scope,
        tls_mode: tls.map(|t| t.mode.to_string()),
        tables,
    };
    artifact.to_json_pretty()
}

fn table_discovery(info: &TableInfo) -> TableDiscovery {
    let cursor_candidates = info.cursor_candidates();
    let chunk_candidates = info.chunk_candidates();
    let fallback = candidates::suggest_cursor_fallback(info);

    let mut notes: Vec<String> = Vec::new();
    if fallback.is_some() {
        notes.push(
            "Primary cursor candidate is nullable; consider \
             `incremental_cursor_mode: coalesce` with the suggested fallback (ADR-0007)."
                .into(),
        );
    }
    if info.columns.is_empty() {
        notes.push("No columns visible — check SELECT privileges.".into());
    }

    TableDiscovery {
        schema: info.schema.clone(),
        table: info.table.clone(),
        row_estimate: info.row_estimate,
        total_bytes: info.total_bytes,
        suggested_mode: info.suggest_mode().to_string(),
        cursor_candidates,
        suggested_cursor_fallback_column: fallback,
        chunk_candidates,
        notes,
    }
}

/// Classify one table of a schema scan: keep it (warning when its types were unreadable), skip a vanished one, abort on any other error.
fn scan_step(res: Result<Introspected>, unreadable: &mut Vec<String>) -> Result<Option<TableInfo>> {
    match res {
        Ok((info, None)) => Ok(Some(info)),
        Ok((info, Some(cause))) => {
            let label = table_label(&info);
            eprint!("{}", unreadable_table_warning(&label, &cause));
            unreadable.push(label);
            Ok(Some(info))
        }
        Err(e) if e.to_string().contains("not found or has no columns") => Ok(None),
        Err(e) => Err(e),
    }
}

/// Every table of the scan; stops (`RIVET_INIT_TABLE_UNREADABLE`) only when none of them could be read.
fn introspect_all(
    tls: Option<&crate::config::TlsConfig>,
    source_url: &str,
    schema: Option<&str>,
    filter: &TableFilter,
    cdc: bool,
) -> Result<Vec<TableInfo>> {
    let mut unreadable = Vec::new();
    let out = scan_tables(tls, source_url, schema, filter, cdc, &mut unreadable)?;
    refuse_unreadable_scan(out.len(), &unreadable)?;
    Ok(out)
}

/// The stop for a scan that found tables and could read the column types of none of them.
fn refuse_unreadable_scan(scanned: usize, unreadable: &[String]) -> Result<()> {
    if scanned > 0 && unreadable.len() == scanned {
        return Err(nothing_readable(unreadable));
    }
    Ok(())
}

fn scan_tables(
    tls: Option<&crate::config::TlsConfig>,
    source_url: &str,
    schema: Option<&str>,
    filter: &TableFilter,
    #[cfg_attr(not(feature = "oracle"), allow(unused_variables))] cdc: bool,
    unreadable: &mut Vec<String>,
) -> Result<Vec<TableInfo>> {
    match source_type(source_url)? {
        "postgres" => {
            let sch = schema
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("public");
            let mut client = postgres::connect(source_url, tls)?;
            let names = retain_filtered(
                catalog_order(postgres::list_tables(&mut client, sch)?),
                filter,
            );
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                if let Some(mut info) =
                    scan_step(postgres::introspect(&mut client, sch, &n), unreadable)?
                {
                    postgres::density_probe(&mut client, &mut info);
                    out.push(info)
                }
            }
            Ok(out)
        }
        "mysql" => {
            let db = mysql::resolve_database_for_listing(source_url, schema)?;
            if schema.map(str::trim).is_some_and(|s| !s.is_empty())
                && let Some(url_db) = mysql::resolve_database_for_listing(source_url, None).ok()
                && url_db != db
            {
                crate::rivet_bail!(
                    crate::error::codes::CLI_OPTION_UNSUPPORTED,
                    "init: --schema '{db}' selects a different MySQL database than the source URL \
                     ('{url_db}') — `rivet init`/`run` connect via the URL and would list and export \
                     the URL's database, not '{db}'. Put the database in the URL instead (mysql://…/{db})."
                );
            }
            let mut conn = mysql::connect(source_url, tls)?;
            let names = retain_filtered(catalog_order(mysql::list_tables(&mut conn, &db)?), filter);
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                if let Some(mut info) = scan_step(mysql::introspect(&mut conn, &n), unreadable)? {
                    mysql::density_probe(&mut conn, &mut info);
                    out.push(info);
                }
            }
            Ok(out)
        }
        "mssql" => {
            let sch = schema
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("dbo");
            let mut conn = mssql::connect(source_url, tls)?;
            let names = retain_filtered(catalog_order(mssql::list_tables(&mut conn, sch)?), filter);
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                if let Some(mut info) =
                    scan_step(mssql::introspect(&mut conn, sch, &n), unreadable)?
                {
                    mark_mssql_catalog_exact(&mut info);
                    out.push(info)
                }
            }
            Ok(out)
        }
        #[cfg(feature = "oracle")]
        "oracle" => {
            let mut conn = oracle::connect(source_url, tls)?;
            let owner = oracle::resolve_schema(&mut conn, schema)?;
            let mut names = catalog_order(oracle::list_tables(&mut conn, &owner)?);
            names.retain(|n| filter.matches_folded(n));
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                if let Some(info) =
                    scan_step(oracle::introspect(&mut conn, &owner, &n, cdc), unreadable)?
                {
                    out.push(info)
                }
            }
            Ok(out)
        }
        "mongo" => {
            reject_mongo_schema(schema)?;
            let conn = mongo::connect(source_url, tls)?;
            let names = retain_filtered(catalog_order(mongo::list_tables(&conn)?), filter);
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                out.push(mongo::introspect(&conn, &n)?);
            }
            Ok(out)
        }
        _ => unreachable!(),
    }
}

/// The tables a catalog listed, in rivet's own order (byte order of the name).
fn catalog_order(mut names: Vec<String>) -> Vec<String> {
    names.sort_unstable();
    names
}

/// Keep only the discovered table names the `--include` / `--exclude` globs admit (L1).
fn retain_filtered(mut names: Vec<String>, filter: &TableFilter) -> Vec<String> {
    names.retain(|n| filter.matches(n));
    names
}

fn schema_scope_label(source_url: &str, schema: Option<&str>, n: usize) -> Result<String> {
    let kind = source_type(source_url)?;
    let obj = if n == 1 { "object" } else { "objects" };
    Ok(match kind {
        "postgres" => {
            let sch = schema
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("public");
            format!("PostgreSQL schema \"{sch}\" ({n} {obj})")
        }
        "mysql" => {
            let db = mysql::resolve_database_for_listing(source_url, schema)?;
            format!("MySQL database \"{db}\" ({n} {obj})")
        }
        "mssql" => {
            let sch = schema
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("dbo");
            format!("SQL Server schema \"{sch}\" ({n} {obj})")
        }
        "oracle" => {
            let owner = schema
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("(current schema)");
            format!("Oracle schema \"{owner}\" ({n} {obj})")
        }
        "mongo" => {
            let db = source_url
                .split_once("://")
                .and_then(|(_, rest)| rest.split('/').nth(1))
                .map(|seg| seg.split('?').next().unwrap_or(seg))
                .filter(|s| !s.is_empty())
                .unwrap_or("(default)");
            format!("MongoDB database \"{db}\" ({n} {obj})")
        }
        _ => unreachable!(),
    })
}

/// A `RIVET_STATE_URL` the operator set must answer before `init` writes a config for it.
fn require_explicit_state(config_path: &str, server_named: bool) -> Result<()> {
    if server_named {
        crate::state::StateStore::probe(config_path).map_err(|e| {
            e.context(format!(
                "init: RIVET_STATE_URL does not answer, so `{config_path}` was not written"
            ))
        })?;
    }
    Ok(())
}

/// Persist the shape each strategy was chosen from, into the state store the runs will use.
fn record_strategy_snapshots(config_path: &str, snapshots: &[crate::state::StrategySnapshot]) {
    if snapshots.is_empty() {
        return;
    }
    let store = match crate::state::StateStore::open(config_path) {
        Ok(s) => s,
        Err(e) => {
            log::debug!("init: strategy snapshot skipped (state store unavailable): {e:#}");
            return;
        }
    };
    for s in snapshots {
        if let Err(e) = store.record_strategy_snapshot(s) {
            log::debug!(
                "init: strategy snapshot for '{}' not written (informational): {e:#}",
                s.export_name
            );
        }
    }
}

/// Record each scaffolded export's source primary key in key order, so `rivet load` has one for a `query:` export, which `rivet run` cannot read a key from.
fn record_primary_keys(
    config_path: &str,
    source_url: &str,
    tls: Option<&crate::config::TlsConfig>,
    snapshots: &[crate::state::StrategySnapshot],
) {
    if snapshots.is_empty() {
        return;
    }
    let kind = match source_type_of(source_url) {
        Ok(k) => k,
        Err(e) => {
            log::debug!("init: primary keys skipped (unreadable source url): {e:#}");
            return;
        }
    };
    let store = match crate::state::StateStore::open(config_path) {
        Ok(s) => s,
        Err(e) => {
            log::warn!("init: source primary keys not recorded (state store unavailable): {e:#}");
            return;
        }
    };
    let mut src = match crate::source::connect(kind, source_url, tls, None) {
        Ok(s) => s,
        Err(e) => {
            log::warn!("init: source primary keys not recorded (source unreachable): {e:#}");
            return;
        }
    };
    let source_key = crate::config::source_state_key(kind, source_url);
    let mut failed: Vec<&str> = Vec::new();
    for s in snapshots {
        let relation = relation_for_key(&kind, s.source_schema.as_deref(), &s.source_table);
        match src.primary_key(&relation) {
            Ok(Some(pk)) => {
                if let Err(e) = store.record_primary_key(&s.export_name, None, &source_key, &pk) {
                    log::debug!(
                        "init: primary key for '{}' not written: {e:#}",
                        s.export_name
                    );
                    failed.push(&s.export_name);
                }
            }
            Ok(None) => {}
            Err(e) => {
                log::debug!(
                    "init: primary key for '{}' unreadable: {e:#}",
                    s.export_name
                );
                failed.push(&s.export_name);
            }
        }
    }
    if !failed.is_empty() {
        log::warn!(
            "init: source primary keys not recorded for {} of {} export(s) — `rivet load` \
             will refuse those with `pk: auto` until you declare `pk:` in their `load:` \
             block: {}",
            failed.len(),
            snapshots.len(),
            failed.join(", ")
        );
    }
}

/// Does any table of this scaffold become an incremental `query:` export loaded into a warehouse, so its key is worth reading?
fn source_keys_wanted(
    infos: &[TableInfo],
    dest: &InitYamlDestination,
    mode_override: Option<&str>,
) -> bool {
    dest.partition_guess().is_some() && infos.iter().any(|i| key_wanted(i, mode_override))
}

/// Is this table's export an incremental one?
fn key_wanted(info: &TableInfo, mode_override: Option<&str>) -> bool {
    mode_override.unwrap_or_else(|| info.suggest_mode()) == "incremental"
}

/// Stamp each wanted table's columns with their key position from `fetch`.
fn record_source_keys(
    infos: &mut [TableInfo],
    mode_override: Option<&str>,
    mut fetch: impl FnMut(&TableInfo) -> Result<Option<Vec<String>>>,
) -> Vec<String> {
    let mut unread = Vec::new();
    for info in infos.iter_mut().filter(|i| key_wanted(i, mode_override)) {
        match fetch(info) {
            Ok(Some(key)) => {
                for (place, name) in key.iter().enumerate() {
                    if let Some(c) = info.columns.iter_mut().find(|c| &c.name == name) {
                        c.key_position = u32::try_from(place + 1).ok();
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                log::debug!("init: primary key for '{}' unreadable: {e:#}", info.table);
                unread.push(info.table.clone());
            }
        }
    }
    unread
}

/// The warning that names the tables whose key could not be read, `None` when every read worked.
fn unread_keys_notice(unread: &[String]) -> Option<String> {
    (!unread.is_empty()).then(|| {
        format!(
            "init: source primary key not read for {} table(s) — their incremental exports carry no `load: pk:`; declare `pk: [..]` in the export's `load:` block before `rivet load`: {}",
            unread.len(),
            unread.join(", ")
        )
    })
}

/// Read the source primary key, in key order, of every table whose export will be an incremental `query:` loaded into a warehouse.
fn read_source_keys(
    infos: &mut [TableInfo],
    source_url: &str,
    dest: &InitYamlDestination,
    mode_override: Option<&str>,
    connect: impl FnOnce(crate::config::SourceType) -> Result<Box<dyn crate::source::Source>>,
) {
    if !source_keys_wanted(infos, dest, mode_override) {
        return;
    }
    let kind = match source_type_of(source_url) {
        Ok(k) => k,
        Err(e) => {
            log::debug!("init: primary keys not read (unreadable source url): {e:#}");
            return;
        }
    };
    let mut src = match connect(kind) {
        Ok(s) => s,
        Err(e) => {
            log::warn!("init: source primary keys not read (source unreachable): {e:#}");
            return;
        }
    };
    let unread = record_source_keys(infos, mode_override, |info| {
        let schema = (!info.schema.is_empty()).then_some(info.schema.as_str());
        src.primary_key(&relation_for_key(&kind, schema, &info.table))
    });
    if let Some(notice) = unread_keys_notice(&unread) {
        log::warn!("{notice}");
    }
}

/// `schema.table` quoted the way the engine's catalog lookup reads it.
fn relation_for_key(
    source_type: &crate::config::SourceType,
    schema: Option<&str>,
    table: &str,
) -> String {
    let quote = |part: &str| match source_type {
        crate::config::SourceType::Mysql => format!("`{}`", part.replace('`', "``")),
        crate::config::SourceType::Mssql => format!("[{}]", part.replace(']', "]]")),
        _ => format!("\"{}\"", part.replace('"', "\"\"")),
    };
    match schema {
        Some(s) => format!("{}.{}", quote(s), quote(table)),
        None => quote(table),
    }
}

#[cfg(test)]
mod tests {
    /// A source that answers `primary_key` from a fixed map and records the relations it was asked about.
    struct KeySource {
        asked: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl crate::source::Source for KeySource {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> Result<()> {
            unreachable!()
        }

        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>> {
            Ok(None)
        }

        fn type_mappings(
            &mut self,
            _query: &str,
            _column_overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            unreachable!()
        }

        fn primary_key(&mut self, table: &str) -> Result<Option<Vec<String>>> {
            self.asked.lock().unwrap().push(table.to_string());
            Ok(Some(vec!["b".into()]))
        }
    }

    #[test]
    fn the_key_read_goes_through_the_engines_quoted_relation_and_stamps_the_table() {
        let wh = InitYamlDestination {
            bigquery_project: Some("p".into()),
            bigquery_dataset: Some("d".into()),
            ..Default::default()
        };
        let table = |schema: &str| TableInfo {
            schema: schema.into(),
            table: "t".into(),
            ..make_table(1, vec![col("a", "int", false), col("b", "int", false)])
        };
        let asked = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut infos = vec![table("s"), table("")];
        let log = asked.clone();
        read_source_keys(
            &mut infos,
            "mysql://u:p@h/db",
            &wh,
            Some("incremental"),
            move |_| Ok(Box::new(KeySource { asked: log })),
        );
        assert_eq!(*asked.lock().unwrap(), vec!["`s`.`t`", "`t`"]);
        assert_eq!(infos[0].columns[1].key_position, Some(1));
        assert_eq!(infos[1].columns[1].key_position, Some(1));
    }

    #[test]
    fn nothing_is_connected_when_no_key_is_wanted_and_a_dead_source_stamps_nothing() {
        let wh = InitYamlDestination {
            bigquery_project: Some("p".into()),
            bigquery_dataset: Some("d".into()),
            ..Default::default()
        };
        let mut infos = vec![make_table(1, vec![col("b", "int", false)])];
        read_source_keys(&mut infos, "mysql://u:p@h/db", &wh, Some("full"), |_| {
            panic!("no connection for a table that is not incremental")
        });
        read_source_keys(
            &mut infos,
            "mysql://u:p@h/db",
            &wh,
            Some("incremental"),
            |_| Err(anyhow::anyhow!("unreachable")),
        );
        read_source_keys(&mut infos, "not a url", &wh, Some("incremental"), |_| {
            panic!("an unreadable url never connects")
        });
        assert_eq!(infos[0].columns[0].key_position, None);
    }

    #[test]
    fn the_unread_key_notice_names_the_tables_and_is_silent_when_all_were_read() {
        assert_eq!(unread_keys_notice(&[]), None);
        let n = unread_keys_notice(&["a".into(), "b".into()]).unwrap();
        assert!(n.contains("2 table(s)") && n.ends_with(": a, b"), "{n}");
    }

    #[test]
    fn source_keys_are_read_for_incremental_tables_into_a_warehouse_only() {
        let wh = InitYamlDestination {
            bigquery_project: Some("p".into()),
            bigquery_dataset: Some("d".into()),
            ..Default::default()
        };
        let t = make_table(1, vec![]);
        assert!(source_keys_wanted(
            std::slice::from_ref(&t),
            &wh,
            Some("incremental")
        ));
        assert!(!source_keys_wanted(
            std::slice::from_ref(&t),
            &wh,
            Some("full")
        ));
        assert!(!source_keys_wanted(
            std::slice::from_ref(&t),
            &InitYamlDestination::default(),
            Some("incremental")
        ));
        assert!(!source_keys_wanted(&[], &wh, Some("incremental")));
    }

    #[test]
    fn a_read_key_stamps_its_columns_in_key_order_and_a_failed_read_is_named() {
        let table = |n: &str| TableInfo {
            table: n.into(),
            ..make_table(
                1,
                vec![
                    col("a", "int", false),
                    col("b", "int", false),
                    col("c", "int", false),
                ],
            )
        };
        let mut infos = vec![table("ok"), table("none"), table("bad"), table("denied")];
        let unread = record_source_keys(&mut infos, Some("incremental"), |i| {
            match i.table.as_str() {
                "ok" => Ok(Some(vec!["c".into(), "a".into()])),
                "none" => Ok(None),
                _ => Err(anyhow::anyhow!("denied")),
            }
        });
        assert_eq!(unread, vec!["bad".to_string(), "denied".to_string()]);
        let pos = |i: &TableInfo| i.columns.iter().map(|c| c.key_position).collect::<Vec<_>>();
        assert_eq!(pos(&infos[0]), vec![Some(2), None, Some(1)]);
        assert_eq!(pos(&infos[1]), vec![None, None, None]);
        let mut full = vec![table("x")];
        record_source_keys(&mut full, Some("full"), |_| panic!("not asked"));
    }

    #[test]
    fn only_a_cdc_scaffold_probes_mineability() {
        assert!(probes_mineability(Some("cdc")));
        assert!(!probes_mineability(None));
        assert!(!probes_mineability(Some("full")));
        assert!(!probes_mineability(Some("incremental")));
    }

    #[test]
    fn a_discovery_of_an_unreachable_source_is_an_error_not_an_empty_artifact() {
        let url = "postgresql://rivet:rivet@127.0.0.1:1/rivet";
        let one = init_discovery_json(None, url, Some("t"), None, &TableFilter::default());
        assert!(one.is_err(), "{one:?}");
        let all = init_discovery_json(None, url, None, None, &TableFilter::default());
        assert!(all.is_err(), "{all:?}");
    }
    use super::yaml_scaffold::ScaffoldDecisions;

    /// Recording primary keys must not re-load and re-VALIDATE the config.
    #[test]
    fn recording_primary_keys_does_not_validate_the_whole_config() {
        let src = include_str!("mod.rs");
        let body = src
            .split("fn record_primary_keys(")
            .nth(1)
            .expect("record_primary_keys still exists")
            .split("\nfn ")
            .next()
            .expect("its body");

        assert!(
            !body.contains("Config::load("),
            "record_primary_keys must not VALIDATE the config to record keys — one \
             invalid export then costs every export its key. The source type comes \
             from the URL (`source_type_of`), as everywhere else in init."
        );
        assert!(
            body.contains("source::connect("),
            "it should take the source by TYPE, not by a config it had to load"
        );
        assert!(
            body.contains("failed.push"),
            "one table rivet cannot read a key for must not cost the others theirs — \
             collect the offenders and name them ALL, since reporting only the first \
             makes finding N of them take N init runs"
        );
    }

    /// Every table the scaffold leaves out is named, in config order, from the decision that left it out.
    #[test]
    fn every_skipped_table_is_named_from_the_decision() {
        let named = |n: &str| TableInfo {
            cdc_capture_instance: None,
            cdc_unmineable: None,
            table: n.into(),
            ..make_table(100, vec![col("id", "bigint", true)])
        };
        let decided = |url: &str, mode: Option<&str>, tables: &[&str]| {
            let infos: Vec<TableInfo> = tables.iter().map(|n| named(n)).collect();
            yaml_scaffold::scaffold_schema(
                &infos,
                url,
                &SourceProvenance::Inline,
                "db",
                &InitYamlDestination::default(),
                mode,
                None,
            )
            .unwrap()
            .decisions
            .skipped
        };
        assert_eq!(
            decided(
                "postgresql://u:p@h/db",
                Some("cdc"),
                &["Orders", "kept", "Items"]
            ),
            vec!["Orders".to_string(), "Items".to_string()]
        );
        assert_eq!(
            decided("mongodb://u:p@h/db", None, &["user-events", "kept"]),
            vec!["user-events".to_string()]
        );
        assert!(decided("postgresql://u:p@h/db", None, &["a", "b"]).is_empty());
    }

    /// Each informational decision prints one line naming every export it covers; none prints nothing.
    #[test]
    fn marked_exports_warning_names_every_export_per_decision() {
        assert_eq!(marked_exports_warning(&ScaffoldDecisions::default()), "");
        let only_chunk = marked_exports_warning(&ScaffoldDecisions {
            no_chunk_key: vec!["k".into()],
            ..Default::default()
        });
        assert_eq!(
            only_chunk,
            "rivet: 1 export(s) have no integer column or keysettable primary key, so they were \
             written as `mode: full` instead of `chunked`: k\n"
        );
        let all = marked_exports_warning(&ScaffoldDecisions {
            skipped: vec!["Orders".into(), "Items".into()],
            insert_only: vec!["a".into(), "b".into()],
            no_chunk_key: vec!["k".into()],
            ..Default::default()
        });
        assert_eq!(
            all,
            "rivet: 2 table(s) were left out of the config — see the `# SKIPPED` comments for \
             why: Orders, Items\n\
             rivet: 2 export(s) use a cursor that does not change on UPDATE: updated rows are \
             never re-exported (set `cursor_column:` to an updated_at-style column, or use \
             `mode: cdc`): a, b\n"
                .to_string()
                + &only_chunk
        );
    }

    /// A server that does not answer is not a standby.
    #[test]
    fn a_server_that_does_not_answer_is_not_in_recovery() {
        assert!(!pg_in_recovery(
            "postgresql://rivet:rivet@127.0.0.1:1/rivet",
            None
        ));
    }

    fn schema_scaffold(infos: &[TableInfo], mode: Option<&str>) -> yaml_scaffold::Scaffold {
        yaml_scaffold::scaffold_schema(
            infos,
            "postgresql://u:p@h/db",
            &SourceProvenance::Inline,
            "db",
            &InitYamlDestination::default(),
            mode,
            None,
        )
        .unwrap()
    }

    /// A schema-wide init names the cursor-less exports on stderr, and says nothing when every export has a cursor.
    #[test]
    fn a_schema_init_prints_the_cursor_notice_only_when_an_export_lacks_a_cursor() {
        let table = |n: &str, ty: &str| TableInfo {
            cdc_capture_instance: None,
            cdc_unmineable: None,
            table: n.into(),
            ..make_table(100, vec![col("c", ty, false)])
        };
        let lacking = schema_scaffold(
            &[
                table("stamped", "timestamp"),
                table("bare_a", "text"),
                table("bare_b", "text"),
            ],
            Some("incremental"),
        );
        let notice = cursor_notice(&lacking.decisions, Some("incremental"), true)
            .expect("two exports lack a cursor");
        assert!(
            notice.starts_with("rivet: 2 export(s) have no timestamp column")
                && notice.ends_with(": bare_a, bare_b"),
            "{notice}"
        );
        let clean = schema_scaffold(&[table("stamped", "timestamp")], Some("incremental"));
        assert_eq!(
            cursor_notice(&clean.decisions, Some("incremental"), true),
            None
        );
    }

    /// A schema-wide init that wrote the default decimal(38,18) says so on stderr; one that did not stays quiet.
    #[test]
    fn a_schema_init_prints_the_decimal_note_only_when_it_wrote_the_default_decimal() {
        let plain = TableInfo {
            cdc_capture_instance: None,
            cdc_unmineable: None,
            table: "plain".into(),
            ..make_table(100, vec![col("id", "bigint", true)])
        };
        let ledger = TableInfo {
            cdc_capture_instance: None,
            cdc_unmineable: None,
            table: "ledger".into(),
            ..make_table(
                100,
                vec![col("id", "bigint", true), col("amount", "numeric", false)],
            )
        };
        let with_default = schema_scaffold(&[plain.clone(), ledger], None);
        assert!(
            with_default.text.contains("decimal(38,18)"),
            "{}",
            with_default.text
        );
        let note = decimal_notice(&with_default.decisions).expect("the default was written");
        assert!(
            note.starts_with("rivet: note: YAML uses default decimal(38,18)")
                && note.contains("search for `# REVIEW:`"),
            "{note}"
        );
        let without = schema_scaffold(&[plain], None);
        assert_eq!(decimal_notice(&without.decisions), None);
    }

    #[test]
    fn the_cursor_message_offers_exclude_only_to_a_whole_schema_init() {
        let names = vec!["a".to_string(), "b".to_string()];
        let schema = cursor_missing_message(&names, Some("incremental"), true);
        assert!(schema.contains("(`--exclude a b`)"), "{schema}");
        let one = cursor_missing_message(&names[..1], Some("time_window"), false);
        assert!(
            !one.contains("--exclude"),
            "`--exclude` is ignored with `--table`: {one}"
        );
        assert!(
            one.contains("`time_column:`") && one.contains("another `--mode`"),
            "{one}"
        );
    }

    #[test]
    fn exports_needing_a_cursor_names_every_one_of_them() {
        let stamped = |n: &str| TableInfo {
            cdc_capture_instance: None,
            cdc_unmineable: None,
            table: n.into(),
            ..make_table(100, vec![col("updated_at", "timestamp", false)])
        };
        let unstamped = |n: &str| TableInfo {
            cdc_capture_instance: None,
            cdc_unmineable: None,
            table: n.into(),
            ..make_table(100, vec![col("email", "text", false)])
        };
        let needs_cursor = |mode: &str, infos: &[TableInfo]| {
            yaml_scaffold::scaffold_schema(
                infos,
                "postgresql://u:p@h/db",
                &SourceProvenance::Inline,
                "db",
                &InitYamlDestination::default(),
                Some(mode),
                None,
            )
            .unwrap()
            .decisions
            .needs_cursor
        };
        let infos = [
            stamped("good_one"),
            unstamped("no_stamp_a"),
            stamped("also_good"),
            unstamped("no_stamp_b"),
        ];
        for mode in ["incremental", "time_window"] {
            assert_eq!(
                needs_cursor(mode, &infos),
                vec!["no_stamp_a".to_string(), "no_stamp_b".to_string()],
                "{mode}: every offender, in file order — naming only the first is what made \
                 finding three take three init runs"
            );
        }
        assert!(
            needs_cursor("incremental", &infos[..1]).is_empty(),
            "a scaffold with a cursor for every export reports nothing"
        );
    }

    /// The URL is the one source of truth for the engine, in both spellings.
    #[test]
    fn source_type_of_agrees_with_the_string_form_it_mirrors() {
        use crate::config::SourceType;
        for (url, want_str, want_enum) in [
            ("postgresql://h/db", "postgres", SourceType::Postgres),
            ("postgres://h/db", "postgres", SourceType::Postgres),
            ("mysql://h/db", "mysql", SourceType::Mysql),
            ("sqlserver://h/db", "mssql", SourceType::Mssql),
            ("mssql://h/db", "mssql", SourceType::Mssql),
            ("mongodb://h/db", "mongo", SourceType::Mongo),
        ] {
            assert_eq!(super::source_type(url).unwrap(), want_str, "{url}");
            assert_eq!(super::source_type_of(url).unwrap(), want_enum, "{url}");
        }
        assert!(
            super::source_type_of("redis://h").is_err(),
            "an unsupported scheme must not silently resolve to an engine"
        );
    }

    /// SQL Server's temporal types are `datetime2`, `datetimeoffset` and `smalldatetime`.
    #[test]
    fn every_sql_server_temporal_type_is_a_timestamp_type() {
        for t in [
            "datetime2",
            "DATETIME2",
            "datetimeoffset",
            "smalldatetime",
            "datetime",
            "timestamp",
            "timestamptz",
            "date",
        ] {
            assert!(
                super::is_timestamp_type(t),
                "{t} must be a cursor candidate"
            );
        }
        assert!(!super::is_timestamp_type("rowversion"));
        for t in ["bigint", "int", "varchar", "uniqueidentifier", "time"] {
            assert!(!super::is_timestamp_type(t), "{t} must not be");
        }
    }

    /// `next_steps_block` is a pure string builder reached only via the init command's `eprint!` (live/CLI-only), so its `-> String::new()` / `"xyzzy"` stubs survive `cargo mutants -- --lib --bins`.
    #[test]
    fn next_steps_block_for_cdc_schedules_the_run_not_a_plan() {
        let s = super::next_steps_block(
            "rivet.yaml",
            &super::SourceProvenance::Env("X".into()),
            Some("cdc"),
            &ScaffoldDecisions {
                has_baseline: true,
                ..Default::default()
            },
        );
        assert!(s.contains("rivet doctor -c rivet.yaml"), "block:\n{s}");
        assert!(
            !s.contains("--annotate-waves") && !s.contains("rivet apply"),
            "a cdc config has no batch plan to seal; block:\n{s}"
        );
        assert!(
            s.contains("rivet run   -c rivet.yaml") && s.contains("through its recipe"),
            "block:\n{s}"
        );
        let s = super::next_steps_block(
            "rivet.yaml",
            &super::SourceProvenance::Env("X".into()),
            Some("cdc"),
            &ScaffoldDecisions::default(),
        );
        assert!(
            s.contains("CHANGES ONLY") && !s.contains("through its recipe"),
            "a capture-only scaffold must say so; block:\n{s}"
        );
        assert!(s.contains("cdc.backfill: auto") && s.contains("cdc.initial: snapshot"));
    }

    /// The terminal block must agree with the file it describes.
    #[test]
    fn next_steps_block_prescribes_compact_only_for_a_base_and_buffer_scaffold() {
        let block = |has_compact: bool| {
            super::next_steps_block(
                "rivet.yaml",
                &super::SourceProvenance::Env("X".into()),
                None,
                &ScaffoldDecisions {
                    has_load: true,
                    compacts: has_compact,
                    ..Default::default()
                },
            )
        };
        let compacting = block(true);
        assert!(
            compacting.contains("rivet load    -c rivet.yaml")
                && compacting.contains("rivet compact -c rivet.yaml"),
            "block:\n{compacting}"
        );
        let overwriting = block(false);
        assert!(
            overwriting.contains("rivet load    -c rivet.yaml")
                && overwriting.contains("OVERWRITES")
                && !overwriting.contains("rivet compact"),
            "block:\n{overwriting}"
        );
        let appending = super::next_steps_block(
            "rivet.yaml",
            &super::SourceProvenance::Env("X".into()),
            Some("cdc"),
            &ScaffoldDecisions {
                has_load: true,
                has_delta: true,
                ..Default::default()
            },
        );
        assert!(
            appending.contains("appends to <table>__changes")
                && !appending.contains("rivet compact"),
            "block:\n{appending}"
        );
        let no_load = super::next_steps_block(
            "rivet.yaml",
            &super::SourceProvenance::Env("X".into()),
            Some("cdc"),
            &ScaffoldDecisions {
                has_delta: true,
                ..Default::default()
            },
        );
        assert!(
            !no_load.contains("rivet load"),
            "no load block, no load step:\n{no_load}"
        );
    }

    #[test]
    fn next_steps_block_shows_read_only_plan_then_annotate() {
        let s = super::next_steps_block(
            "rivet.yaml",
            &super::SourceProvenance::Env("X".into()),
            None,
            &ScaffoldDecisions::default(),
        );
        assert!(s.contains("rivet doctor -c rivet.yaml"), "block:\n{s}");
        assert!(
            s.contains("rivet run    -c rivet.yaml --validate"),
            "block:\n{s}"
        );
        assert!(
            s.contains("rivet plan  -c rivet.yaml") && s.contains("read-only"),
            "next-steps must present plain `rivet plan` as read-only; block:\n{s}"
        );
        assert!(
            s.contains("--annotate-waves") && s.contains("write wave:/parallel_safe:"),
            "next-steps must show --annotate-waves as the config-writing step; block:\n{s}"
        );
        assert!(
            !s.contains("export DATABASE_URL"),
            "Env provenance must not print the Inline-only export line; block:\n{s}"
        );
    }

    /// A vanished table is skipped, any other error aborts the scan, a table passes through.
    #[test]
    fn scan_step_skips_only_the_vanished_table_shape() {
        let vanished = Err(anyhow::anyhow!(
            "Table 'qa_broken_v' not found or has no columns. Check the table name \
             and that the user has SELECT privilege."
        ));
        assert!(
            matches!(scan_step(vanished, &mut Vec::new()), Ok(None)),
            "a listed-but-vanished table (dropped mid-scan, or a broken view) must be \
             SKIPPED — aborting on it is how one stale view failed every schema-wide init"
        );

        let real = Err(anyhow::anyhow!("connection refused (os error 61)"));
        assert!(
            scan_step(real, &mut Vec::new()).is_err(),
            "any other error must ABORT the scan — swallowing a connection drop emits a \
             silently truncated scaffold that reads as a small schema"
        );

        let info = TableInfo {
            cdc_capture_instance: None,
            cdc_unmineable: None,
            schema: "s".into(),
            table: "t".into(),
            row_estimate: 1,
            total_bytes: None,
            columns: Vec::new(),
            density: None,
        };
        let mut unreadable = Vec::new();
        assert!(
            matches!(scan_step(Ok((info, None)), &mut unreadable), Ok(Some(i)) if i.table == "t")
        );
        assert!(unreadable.is_empty());
    }

    use super::*;

    fn col(name: &str, ty: &str, pk: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: ty.to_string(),
            is_primary_key: pk,
            is_nullable: false,
            numeric_precision: None,
            numeric_scale: None,
            is_indexed: false,
            ..Default::default()
        }
    }

    /// #199: the density probe's key must be INDEXED.
    #[test]
    fn best_chunk_column_requires_an_index() {
        let c = |n: &str, ty: &str, pk: bool, idx: bool| ColumnInfo {
            name: n.into(),
            data_type: ty.into(),
            is_primary_key: pk,
            is_nullable: false,
            numeric_precision: None,
            numeric_scale: None,
            is_indexed: idx,
            ..Default::default()
        };
        let t = make_table(1, vec![c("id", "bigint", true, true)]);
        assert_eq!(t.best_chunk_column(), Some("id"));
        let t = make_table(
            1,
            vec![c("uid", "uuid", true, true), c("qty", "int", false, false)],
        );
        assert_eq!(
            t.best_chunk_column(),
            None,
            "an unindexed integer is never the chunk key"
        );
        let t = make_table(
            1,
            vec![
                c("uid", "uuid", true, true),
                c("ref_id", "bigint", false, true),
            ],
        );
        assert_eq!(t.best_chunk_column(), Some("ref_id"));
    }

    fn make_table(rows: i64, cols: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            cdc_capture_instance: None,
            cdc_unmineable: None,
            density: None,
            schema: "public".to_string(),
            table: "orders".to_string(),
            row_estimate: rows,
            total_bytes: None,
            columns: cols,
        }
    }

    fn scan_table(name: &str, rows: i64, columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            table: name.into(),
            ..make_table(rows, columns)
        }
    }

    fn indexed(name: &str, ty: &str) -> ColumnInfo {
        ColumnInfo {
            is_indexed: true,
            ..col(name, ty, false)
        }
    }

    fn readable_scan_tables() -> Vec<TableInfo> {
        vec![
            scan_table(
                "a_orders",
                5_000_000,
                vec![
                    col("id", "bigint", true),
                    col("created_at", "timestamp", false),
                    col("status", "text", false),
                ],
            ),
            scan_table(
                "b_events",
                150_000,
                vec![col("id", "uuid", true), col("payload", "text", false)],
            ),
            scan_table(
                "c_small",
                100,
                vec![col("code", "varchar", true), col("note", "text", false)],
            ),
            scan_table(
                "d_wide",
                150_000,
                vec![
                    col("sku", "varchar", true),
                    indexed("shard", "int"),
                    col("title", "text", false),
                ],
            ),
        ]
    }

    fn locked_small_table() -> TableInfo {
        scan_table(
            "y_locked_small",
            100,
            vec![col("code", "varchar", true), col("note", "text", false)],
        )
    }

    fn locked_big_table() -> TableInfo {
        scan_table(
            "z_locked_big",
            150_000,
            vec![col("id", "uuid", true), col("payload", "text", false)],
        )
    }

    /// #148 (roast 2026-08-09): avg_row_bytes must return None once the density probe CORRECTED row_estimate.
    #[test]
    fn avg_row_bytes_none_when_a_probe_corrected_the_rows() {
        use crate::init::density::{DensityProbe, EstimateMethod};
        let mut t = make_table(148_000, vec![]);
        t.total_bytes = Some(4_000_000);
        t.density = Some(DensityProbe {
            rows: 148_000,
            density: 3.0,
            method: EstimateMethod::Probed,
            catalog_rows: 1000,
            k: 50,
            w: 10_000,
        });
        assert_eq!(
            t.avg_row_bytes(),
            None,
            "corrected rows + frozen bytes = unknown per-row"
        );

        let mut t2 = make_table(1000, vec![]);
        t2.total_bytes = Some(4_000_000);
        assert_eq!(t2.avg_row_bytes(), Some(4000));
        let mut t3 = make_table(0, vec![]);
        t3.total_bytes = Some(4_000_000);
        assert_eq!(t3.avg_row_bytes(), None);
    }

    /// #148 (roast 2026-08-09): mark_mssql_catalog_exact must NOT stamp catalog-exact on a 0 (a VIEW or stats-unavailable table yields 0.
    #[test]
    fn mssql_catalog_exact_only_for_a_positive_count() {
        use crate::init::density::EstimateMethod;
        let mut base = make_table(500_000, vec![]);
        mark_mssql_catalog_exact(&mut base);
        assert_eq!(
            base.density.as_ref().unwrap().method,
            EstimateMethod::CatalogExact,
            "a positive sys.partitions count IS catalog-exact"
        );
        let mut view = make_table(0, vec![]);
        mark_mssql_catalog_exact(&mut view);
        assert_eq!(
            view.density.as_ref().unwrap().method,
            EstimateMethod::Unverified,
            "0 from a view / no-stats is UNKNOWN, not an audited exact 0"
        );
    }

    /// A3/A2 (roast 2026-08-09): where best_cursor_column (name-preference) and chosen_cursor_column (scored) DIVERGE, every user-facing surface that names the cursor must name the CHOSEN one.
    #[test]
    fn mode_rationale_names_the_chosen_cursor_not_the_name_preferred_one() {
        let nullable_ts = |name: &str| ColumnInfo {
            name: name.into(),
            data_type: "timestamp".into(),
            is_primary_key: false,
            is_nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            is_indexed: false,
            ..Default::default()
        };
        let info = make_table(
            500_000,
            vec![
                col("payload", "text", false),
                nullable_ts("updated_at"),
                col("modified_at", "timestamp", false),
            ],
        );
        assert_eq!(info.best_cursor_column(), Some("updated_at"));
        assert_eq!(info.chosen_cursor_column().as_deref(), Some("modified_at"));
        let r = info.mode_rationale("incremental");
        assert!(
            r.contains("modified_at"),
            "must name the chosen cursor: {r}"
        );
        assert!(
            !r.contains("updated_at"),
            "must NOT name the name-preferred phantom: {r}"
        );
    }

    #[test]
    fn suggest_chunked_for_large_table_with_int_pk() {
        let info = make_table(
            5_000_000,
            vec![
                col("id", "bigint", true),
                col("created_at", "timestamp", false),
            ],
        );
        assert_eq!(info.suggest_mode(), "chunked");
        assert_eq!(info.best_chunk_column(), Some("id"));
    }

    #[test]
    fn chunked_rationale_hints_incremental_when_cursor_column_exists() {
        let info = make_table(
            655_000,
            vec![
                col("warranty_id", "bigint", true),
                col("update_date", "timestamp", false),
            ],
        );
        assert_eq!(info.suggest_mode(), "chunked");
        let r = info.mode_rationale("chunked");
        assert!(
            r.contains("mode: incremental"),
            "expected an incremental hint: {r}"
        );
        assert!(
            r.contains("update_date"),
            "should name the cursor column: {r}"
        );
    }

    #[test]
    fn chunked_rationale_no_incremental_hint_without_cursor_column() {
        let info = make_table(5_000_000, vec![col("id", "bigint", true)]);
        assert_eq!(info.suggest_mode(), "chunked");
        let r = info.mode_rationale("chunked");
        assert!(
            !r.contains("mode: incremental"),
            "no cursor column → no hint: {r}"
        );
    }

    #[test]
    fn chunked_rationale_does_not_claim_a_threshold_it_did_not_cross() {
        let small = make_table(200, vec![col("id", "bigint", true)]);
        assert_eq!(
            small.suggest_mode(),
            "full",
            "suggest_mode would not pick chunked here — only recipe_mode does"
        );
        let r = small.mode_rationale("chunked");
        assert!(
            !r.contains("≥ 100K threshold"),
            "a 200-row table must not claim it crossed the threshold: {r}"
        );
        assert!(
            r.contains("below the 100K threshold") && r.contains("pageable"),
            "it must name the real reason — a usable key, not the row count: {r}"
        );
    }

    #[test]
    fn full_rationale_distinguishes_below_threshold_from_no_key_at_scale() {
        let small = make_table(500, vec![col("id", "bigint", true)]);
        assert_eq!(small.suggest_mode(), "full");
        assert!(
            small.mode_rationale("full").contains("below 100K"),
            "small table keeps the below-threshold message"
        );
        let big_keyless = make_table(150_000, vec![col("label", "text", false)]);
        assert_eq!(big_keyless.suggest_mode(), "full");
        let r = big_keyless.mode_rationale("full");
        assert!(
            !r.contains("below 100K"),
            "150K must not say below 100K: {r}"
        );
        assert!(r.contains("no chunk key"), "must name the real reason: {r}");
    }

    #[test]
    fn a_forced_mode_rationale_names_the_override_not_the_auto_reasons() {
        let small_keyed = make_table(
            900,
            vec![
                col("id", "bigint", true),
                col("updated_at", "timestamp", false),
            ],
        );
        assert_eq!(
            small_keyed.mode_rationale("incremental"),
            "~900 rows below the 100K threshold; auto would pick `full`, written as \
             incremental on 'updated_at'"
        );
        let small_no_cursor = make_table(900, vec![col("label", "text", false)]);
        assert_eq!(
            small_no_cursor.mode_rationale("incremental"),
            "~900 rows below the 100K threshold; auto would pick `full`, written as \
             incremental — set cursor_column manually (no timestamp column detected)"
        );
        let big_keyed = make_table(500_000, vec![col("id", "bigint", true)]);
        assert_eq!(
            big_keyed.mode_rationale("full"),
            "~500K rows ≥ 100K threshold; auto would pick `chunked`, written as a full scan"
        );
        let big_cursor_only = make_table(500_000, vec![col("updated_at", "timestamp", false)]);
        assert_eq!(
            big_cursor_only.mode_rationale("incremental"),
            "auto: ~500K rows ≥ 100K threshold; chunk column missing, falling back to \
             incremental on 'updated_at'"
        );
    }

    #[test]
    fn suggest_incremental_when_no_int_pk() {
        let info = make_table(500_000, vec![col("updated_at", "timestamp", false)]);
        assert_eq!(info.suggest_mode(), "incremental");
        assert_eq!(info.best_cursor_column(), Some("updated_at"));
    }

    #[test]
    fn suggest_full_for_small_table() {
        let info = make_table(500, vec![col("id", "bigint", true)]);
        assert_eq!(info.suggest_mode(), "full");
    }

    #[test]
    fn cursor_column_prefers_updated_at() {
        let info = make_table(
            0,
            vec![
                col("created_at", "timestamp", false),
                col("updated_at", "timestamp", false),
            ],
        );
        assert_eq!(info.best_cursor_column(), Some("updated_at"));
    }

    /// The partition key is the date the rows are ABOUT, so it must skip EVERY mutation stamp, not just the two spelled `updated_at`/`modified_at`.
    #[test]
    fn partition_column_skips_a_mutation_stamp_spelled_otherwise() {
        let info = make_table(
            0,
            vec![
                col("changed_at", "timestamp", false),
                col("event_date", "date", false),
            ],
        );
        assert_eq!(info.best_partition_column(), Some("event_date"));
    }

    /// SQL Server's house convention is PascalCase: `CreatedDate` / `ModifiedDate`.
    #[test]
    fn pascal_case_stamps_are_recognised() {
        let info = make_table(
            0,
            vec![
                col("BusinessEntityID", "bigint", true),
                col("CreatedDate", "datetime2", false),
                col("ModifiedDate", "datetime2", false),
            ],
        );
        assert_eq!(info.best_cursor_column(), Some("ModifiedDate"));
        assert_eq!(info.best_partition_column(), Some("CreatedDate"));
        assert!(
            super::is_mutation_stamp("LAST_UPDATED") && super::is_creation_stamp("DateCreated")
        );
    }

    /// Among several business dates the creation stamp wins, whatever its position.
    #[test]
    fn partition_prefers_the_creation_stamp_over_an_earlier_business_date() {
        let info = make_table(
            0,
            vec![
                col("shipped_at", "timestamp", false),
                col("created_at", "timestamp", false),
            ],
        );
        assert_eq!(info.best_partition_column(), Some("created_at"));
    }

    #[test]
    fn parse_table_with_schema() {
        let (s, t) = yaml_scaffold::parse_table("my_schema.orders");
        assert_eq!(s, "my_schema");
        assert_eq!(t, "orders");
    }

    #[test]
    fn parse_table_without_schema_defaults_to_public() {
        let (s, t) = yaml_scaffold::parse_table("orders");
        assert_eq!(s, "public");
        assert_eq!(t, "orders");
    }

    /// `SELECT *` quotes every part of the name the way its engine quotes identifiers.
    #[test]
    fn select_all_quotes_each_part_for_its_engine() {
        use crate::config::SourceType;
        for (source, expected) in [
            (SourceType::Postgres, "SELECT * FROM \"s\".\"t\""),
            (SourceType::Mysql, "SELECT * FROM `s`.`t`"),
            (SourceType::Mssql, "SELECT * FROM [s].[t]"),
            (SourceType::Oracle, "SELECT * FROM \"s\".\"t\""),
        ] {
            assert_eq!(select_all(source, &["s", "t"]), expected, "{source:?}");
        }
        assert_eq!(select_all(SourceType::Mysql, &["t"]), "SELECT * FROM `t`");
    }

    /// A table of exactly 100000 rows is not large; one more is.
    #[test]
    fn a_table_is_large_for_chunking_one_row_past_100000() {
        let columns = || vec![col("id", "bigint", true)];
        assert_eq!(make_table(100_000, columns()).suggest_mode(), "full");
        assert_eq!(make_table(100_001, columns()).suggest_mode(), "chunked");
    }

    /// A single-table init stops on a table whose types could not be read, coded, naming the table, the cause and the remedy.
    #[test]
    fn a_named_table_whose_types_cannot_be_read_stops_init_with_the_cause_and_the_remedy() {
        let table = || scan_table("t", 10, vec![col("id", "bigint", true)]);
        let e = require_readable((
            TableInfo {
                schema: "s".into(),
                ..table()
            },
            Some("permission denied for table t".into()),
        ))
        .unwrap_err();
        crate::error::assert_coded(&e, "RIVET_INIT_TABLE_UNREADABLE", 1);
        assert_eq!(
            format!("{e:#}"),
            "init: reading the column types of s.t to choose its keyset key failed: permission denied for table t; grant SELECT on s.t or leave it out of init"
        );
        let bare = require_readable((
            TableInfo {
                schema: String::new(),
                ..table()
            },
            Some("denied".into()),
        ))
        .unwrap_err();
        assert!(
            format!("{bare:#}").contains("column types of t to choose"),
            "{bare:#}"
        );
        assert_eq!(require_readable((table(), None)).unwrap().table, "t");
    }

    /// A schema scan keeps an unreadable table, records it, and warns in coded words with the table, the cause and the remedy.
    #[test]
    fn a_scan_keeps_an_unreadable_table_and_names_it_in_a_coded_warning() {
        let mut unreadable = Vec::new();
        let kept = scan_step(
            Ok((locked_small_table(), Some("permission denied".into()))),
            &mut unreadable,
        )
        .unwrap();
        assert_eq!(kept.unwrap().table, "y_locked_small");
        assert_eq!(unreadable, vec!["public.y_locked_small".to_string()]);
        assert_eq!(
            unreadable_table_warning("public.y_locked_small", "permission denied"),
            "rivet: WARN [RIVET_INIT_TABLE_UNREADABLE] init: reading the column types of public.y_locked_small failed: permission denied; public.y_locked_small is scaffolded without a keyset key; grant SELECT on public.y_locked_small or leave it out of init\n"
        );
    }

    /// A scan stops only when it found tables and could read none of them.
    #[test]
    fn a_scan_stops_only_when_no_table_could_be_read() {
        let names = |n: &[&str]| n.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let e = refuse_unreadable_scan(2, &names(&["public.a", "public.b"])).unwrap_err();
        crate::error::assert_coded(&e, "RIVET_INIT_TABLE_UNREADABLE", 1);
        assert_eq!(
            format!("{e:#}"),
            "init: the column types of none of the 2 table(s) in the scan could be read: public.a, public.b; grant SELECT on them or point --schema at a schema the user can read"
        );
        assert!(refuse_unreadable_scan(2, &names(&["public.a"])).is_ok());
        assert!(refuse_unreadable_scan(2, &[]).is_ok());
        assert!(refuse_unreadable_scan(0, &[]).is_ok());
    }

    /// A failed probe marks every column `not_keyset` and returns the cause; a good probe returns none.
    #[test]
    fn a_failed_probe_marks_every_column_not_keyset_and_returns_the_cause() {
        let mut columns = vec![col("id", "bigint", true), col("n", "text", false)];
        let cause = probe_keyset_refusals(
            crate::config::SourceType::Postgres,
            &mut columns,
            Err(anyhow::anyhow!("permission denied")),
            || unreachable!("the catalog is not asked when the probe failed"),
        )
        .unwrap();
        assert_eq!(cause.as_deref(), Some("permission denied"));
        assert!(columns.iter().all(|c| c.not_keyset));
        let asked = std::cell::Cell::new(false);
        let mut readable = vec![col("id", "bigint", true)];
        let cause = probe_keyset_refusals(
            crate::config::SourceType::Postgres,
            &mut readable,
            Ok(Vec::new()),
            || {
                asked.set(true);
                Ok(Vec::new())
            },
        )
        .unwrap();
        assert!(cause.is_none() && asked.get());
    }

    /// Readable tables of a scan that also holds unreadable ones scaffold byte for byte as main scaffolded them.
    #[test]
    fn readable_tables_scaffold_as_main_did_beside_unreadable_ones() {
        let main_readable = include_str!("fixtures/scan_readable_as_main.yaml");
        let mut readable = readable_scan_tables();
        readable.push(locked_small_table());
        assert_eq!(schema_scaffold(&readable, None).text, main_readable);

        let mut scan = readable_scan_tables();
        for mut t in [locked_small_table(), locked_big_table()] {
            let cause = probe_keyset_refusals(
                crate::config::SourceType::Postgres,
                &mut t.columns,
                Err(anyhow::anyhow!("permission denied")),
                || Ok(Vec::new()),
            )
            .unwrap();
            assert!(cause.is_some());
            scan.push(t);
        }
        let text = schema_scaffold(&scan, None).text;
        let tail = text
            .strip_prefix(main_readable)
            .expect("every readable export and the small unreadable one are main's bytes");
        assert!(
            tail.contains("name: z_locked_big") && tail.contains("mode: full"),
            "{tail}"
        );
        assert!(!tail.contains("chunk_by_key"), "{tail}");
    }

    /// Init marks a column `not_keyset` exactly when the planner's `admit_keyset_keys` refuses it, for every type on every engine.
    #[test]
    fn init_and_the_planner_refuse_the_same_keyset_keys() {
        use crate::config::SourceType;
        use crate::source::TableIntrospection;
        use crate::source::keyset_key::Wider;
        use crate::source::keyset_key::samples::{mapping_of, rivet_type_samples};
        for source in [
            SourceType::Postgres,
            SourceType::Mysql,
            SourceType::Mssql,
            SourceType::Oracle,
        ] {
            for fact in [
                None,
                Some(Wider::FinerTimestamp),
                Some(Wider::InexactNumber),
            ] {
                let mappings: Vec<_> = rivet_type_samples()
                    .into_iter()
                    .enumerate()
                    .map(|(i, t)| {
                        let mut m = mapping_of(t);
                        m.column_name = format!("c{i}");
                        m
                    })
                    .collect();
                let names: Vec<String> = mappings.iter().map(|m| m.column_name.clone()).collect();
                let finer_list: Vec<(String, Wider)> = fact
                    .map(|w| names.iter().map(|n| (n.clone(), w)).collect())
                    .unwrap_or_default();
                let mut columns: Vec<ColumnInfo> = names
                    .iter()
                    .map(|n| ColumnInfo {
                        name: n.clone(),
                        ..Default::default()
                    })
                    .collect();
                mark_keyset_refusals(source, &mut columns, &mappings, &finer_list);
                let mut planner = TableIntrospection {
                    keyset_keys: names.clone(),
                    declared_wider: finer_list.clone(),
                    ..Default::default()
                };
                planner.admit_keyset_keys(source, &mappings);
                for (c, m) in columns.iter().zip(&mappings) {
                    assert_eq!(
                        !c.not_keyset,
                        planner.is_usable_keyset_key(&c.name),
                        "{source:?} fact={fact:?}: init and the planner disagree on {:?}",
                        m.rivet_type
                    );
                }
                assert!(
                    columns.iter().any(|c| c.not_keyset),
                    "{source:?} fact={fact:?}: the sweep must hold refused types"
                );
                assert!(
                    source == SourceType::Oracle && fact.is_some()
                        || columns.iter().any(|c| !c.not_keyset),
                    "{source:?} fact={fact:?}: the sweep must hold admitted types"
                );
            }
        }
    }

    #[test]
    fn init_keysets_a_large_non_int_pk_but_never_a_decimal_pk() {
        let uuid_pk = make_table(
            150_000,
            vec![col("id", "uuid", true), col("n", "int", false)],
        );
        assert_eq!(uuid_pk.suggest_mode(), "chunked");
        assert_eq!(uuid_pk.keysettable_pk_column(), Some("id"));

        let str_pk = make_table(
            150_000,
            vec![col("code", "varchar", true), col("n", "int", false)],
        );
        assert_eq!(str_pk.suggest_mode(), "chunked");
        assert_eq!(str_pk.keysettable_pk_column(), Some("code"));

        let dec_pk = make_table(
            150_000,
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: "decimal".into(),
                    is_primary_key: true,
                    is_nullable: false,
                    numeric_precision: Some(20),
                    numeric_scale: Some(0),
                    is_indexed: false,
                    not_keyset: true,
                    ..Default::default()
                },
                col("name", "text", false),
            ],
        );
        assert_eq!(
            dec_pk.keysettable_pk_column(),
            None,
            "decimal PK not keysettable"
        );
        assert_eq!(
            dec_pk.suggest_mode(),
            "full",
            "decimal PK (no int col) stays full"
        );

        let small_uuid = make_table(
            50_000,
            vec![col("id", "uuid", true), col("n", "int", false)],
        );
        assert_eq!(small_uuid.suggest_mode(), "full");
    }

    /// The snapshot's decision must be the one the YAML actually carries.
    #[test]
    fn decided_strategy_agrees_with_the_generated_yaml() {
        let cases: Vec<(&str, TableInfo)> = vec![
            (
                "single int PK, large → keyset",
                make_table(
                    2_000_000,
                    vec![col("id", "bigint", true), col("name", "text", false)],
                ),
            ),
            (
                "no single PK, int column, large → range",
                make_table(
                    2_000_000,
                    vec![col("order_id", "bigint", false), col("name", "text", false)],
                ),
            ),
            (
                "small → full",
                make_table(10, vec![col("id", "bigint", true)]),
            ),
        ];
        for (label, info) in cases {
            let d = yaml_scaffold::decided_strategy(&info, None, true);
            let yaml = yaml_scaffold::generate_config(
                &info,
                "postgresql://localhost/db",
                &super::SourceProvenance::Inline,
                &Default::default(),
                None,
                None,
            )
            .expect("scaffold");
            let yaml: String = yaml
                .lines()
                .filter(|l| !l.trim_start().starts_with('#'))
                .collect::<Vec<_>>()
                .join("\n");

            assert!(
                yaml.contains(&format!("mode: {}", d.mode)),
                "{label}: snapshot says mode {:?}, YAML says otherwise:\n{yaml}",
                d.mode
            );
            let (expect, forbid) = match d.kind {
                "keyset" => ("chunk_by_key:", Some("chunk_column:")),
                "range" => ("chunk_column:", Some("chunk_by_key:")),
                _ => ("mode:", None),
            };
            assert!(
                yaml.contains(expect),
                "{label}: snapshot kind {:?} implies `{expect}` in the YAML:\n{yaml}",
                d.kind
            );
            if let Some(f) = forbid {
                assert!(
                    !yaml.contains(f),
                    "{label}: snapshot kind {:?} but the YAML also carries `{f}`:\n{yaml}",
                    d.kind
                );
            }
            if let Some(k) = &d.key_column {
                assert!(
                    yaml.contains(k),
                    "{label}: snapshot names key {k:?}, absent from the YAML:\n{yaml}"
                );
            }
        }
    }

    #[test]
    fn generate_config_chunked_single_pk_uses_keyset() {
        let info = make_table(
            2_000_000,
            vec![col("id", "bigint", true), col("name", "text", false)],
        );
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &InitYamlDestination::default(),
            None,
            None,
        )
        .unwrap();
        assert!(yaml.contains("mode: chunked"), "got:\n{yaml}");
        assert!(yaml.contains("chunk_by_key: id"), "got:\n{yaml}");
        assert!(
            !yaml.contains("\n    chunk_column:"),
            "PK must keyset, not range-chunk:\n{yaml}"
        );
        assert!(
            !yaml.contains("\n    parallel:"),
            "keyset must not emit parallel:\n{yaml}"
        );
        assert!(
            yaml.contains("\n    chunk_checkpoint: true"),
            "keyset must default chunk_checkpoint on (safe crash-recovery):\n{yaml}"
        );
        assert!(
            yaml.contains("# keyset_incremental: true"),
            "append-only incremental must be a commented opt-in:\n{yaml}"
        );
        assert!(yaml.contains("table: orders"), "got:\n{yaml}");
        assert!(
            !yaml.contains("query:"),
            "keyset must use table:, not query:\n{yaml}"
        );
        assert!(yaml.contains("meta_columns:"), "got:\n{yaml}");
    }

    #[test]
    fn generate_config_chunked_no_single_pk_uses_range_column() {
        let info = make_table(
            2_000_000,
            vec![
                ColumnInfo {
                    is_indexed: true,
                    ..col("region_id", "int", false)
                },
                col("name", "text", false),
            ],
        );
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &InitYamlDestination::default(),
            None,
            None,
        )
        .unwrap();
        assert!(yaml.contains("mode: chunked"), "got:\n{yaml}");
        assert!(yaml.contains("chunk_column: region_id"), "got:\n{yaml}");
        assert!(
            !yaml.contains("chunk_by_key:"),
            "no PK → range, not keyset:\n{yaml}"
        );
        assert!(yaml.contains("chunk_checkpoint: true"), "got:\n{yaml}");
    }

    #[test]
    fn single_pk_column_detects_single_composite_and_none() {
        let one = make_table(1, vec![col("id", "bigint", true), col("n", "text", false)]);
        assert_eq!(one.single_pk_column(), Some("id"));
        let composite = make_table(1, vec![col("a", "bigint", true), col("b", "bigint", true)]);
        assert_eq!(composite.single_pk_column(), None);
        let none = make_table(1, vec![col("x", "int", false)]);
        assert_eq!(none.single_pk_column(), None);
        let uuid = make_table(1, vec![col("id", "uuid", true)]);
        assert_eq!(uuid.single_pk_column(), Some("id"));
        assert_eq!(uuid.best_chunk_column(), None);
    }

    #[test]
    fn generate_config_incremental_contains_cursor() {
        let info = make_table(200_000, vec![col("updated_at", "timestamp", false)]);
        let yaml = yaml_scaffold::generate_config(
            &info,
            "mysql://localhost/db",
            &super::SourceProvenance::Inline,
            &InitYamlDestination::default(),
            None,
            None,
        )
        .unwrap();
        assert!(yaml.contains("mode: incremental"), "got:\n{yaml}");
        assert!(yaml.contains("cursor_column: updated_at"), "got:\n{yaml}");
        assert!(yaml.contains("row_hash: true"), "got:\n{yaml}");
    }

    #[test]
    fn generate_schema_config_emits_multiple_exports() {
        let a = TableInfo {
            cdc_capture_instance: None,
            cdc_unmineable: None,
            density: None,
            schema: "public".to_string(),
            table: "users".to_string(),
            row_estimate: 100,
            total_bytes: None,
            columns: vec![col("id", "bigint", true)],
        };
        let b = TableInfo {
            cdc_capture_instance: None,
            cdc_unmineable: None,
            density: None,
            schema: "public".to_string(),
            table: "orders".to_string(),
            row_estimate: 200,
            total_bytes: None,
            columns: vec![col("id", "bigint", true), col("user_id", "int", false)],
        };
        let yaml = yaml_scaffold::generate_schema_config(
            &[a, b],
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            r#"schema "public" (2)"#,
            &InitYamlDestination::default(),
            None,
            None,
        )
        .unwrap();
        assert!(yaml.contains("  - name: users"), "got:\n{yaml}");
        assert!(yaml.contains("  - name: orders"), "got:\n{yaml}");
        assert!(
            yaml.contains("    table: users") || yaml.contains("FROM users"),
            "expected users reference; got:\n{yaml}"
        );
        assert!(
            yaml.contains("    table: orders") || yaml.contains("FROM orders"),
            "expected orders reference; got:\n{yaml}"
        );
        assert_eq!(yaml.matches("meta_columns:").count(), 2, "got:\n{yaml}");
    }

    #[test]
    fn generate_config_gcs_bucket_sets_per_table_prefix_without_credentials_file() {
        let info = make_table(
            2_000_000,
            vec![col("id", "bigint", true), col("name", "text", false)],
        );
        let dest = InitYamlDestination {
            gcs_bucket: Some("my-bucket".to_string()),
            gcs_credentials_file: None,
            s3_bucket: None,
            s3_region: None,
            bigquery_project: None,
            bigquery_dataset: None,
            clickhouse_url: None,
            clickhouse_database: None,
            clickhouse_user: None,
            snowflake: None,
            cdc_name_stem: None,
            azure: None,
        };
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
            None,
        )
        .unwrap();
        assert!(yaml.contains("type: gcs"), "got:\n{yaml}");
        assert!(yaml.contains("bucket: my-bucket"), "got:\n{yaml}");
        assert!(yaml.contains("prefix: exports/orders/"), "got:\n{yaml}");
        assert!(
            !yaml.contains("credentials_file"),
            "ADC / env key: YAML should omit credentials_file, got:\n{yaml}"
        );
    }

    fn scaffolded(info: &TableInfo, mode: Option<&str>) -> yaml_scaffold::Scaffold {
        yaml_scaffold::scaffold_table(
            info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &InitYamlDestination::default(),
            mode,
            None,
        )
        .unwrap()
    }

    fn scaffold(info: &TableInfo, mode: Option<&str>) -> String {
        scaffolded(info, mode).text
    }

    #[test]
    fn forced_chunked_on_a_keyless_table_is_written_as_full_not_a_phantom_id() {
        let mut deleted_at = col("deleted_at", "timestamp", false);
        deleted_at.is_nullable = true;
        let info = make_table(
            100,
            vec![
                col("email", "text", false),
                deleted_at,
                col("payload", "text", false),
            ],
        );
        let yaml = scaffold(&info, Some("chunked"));
        assert!(yaml.contains("    mode: full"), "got:\n{yaml}");
        assert!(!yaml.contains("chunk_column"), "got:\n{yaml}");
        assert_eq!(
            scaffolded(&info, Some("chunked")).decisions.no_chunk_key,
            vec!["orders".to_string()]
        );
        let d = yaml_scaffold::decided_strategy(&info, Some("chunked"), true);
        assert_eq!(
            (d.mode.as_str(), d.kind),
            ("full", "full"),
            "snapshot agrees with the YAML"
        );
    }

    #[test]
    fn a_table_that_cannot_use_the_table_form_is_recorded_as_the_yaml_writes_it() {
        let snapshot_kind = |info: &TableInfo| {
            snapshot_of(info, Some("chunked"), "postgresql://localhost/db")
                .strategy_kind
                .unwrap()
        };
        let mut uuid_only =
            make_table(100, vec![col("uid", "uuid", true), col("v", "text", false)]);
        uuid_only.table = "Orders".into();
        let yaml = scaffold(&uuid_only, Some("chunked"));
        assert!(
            yaml.contains("    mode: full"),
            "no table: form, no keyset: {yaml}"
        );
        assert_eq!(snapshot_kind(&uuid_only), "full");

        let mut with_int = make_table(
            100,
            vec![
                col("uid", "uuid", true),
                ColumnInfo {
                    is_indexed: true,
                    ..col("n", "bigint", false)
                },
            ],
        );
        with_int.table = "Orders".into();
        let yaml = scaffold(&with_int, Some("chunked"));
        assert!(yaml.contains("chunk_column: n"), "{yaml}");
        assert_eq!(snapshot_kind(&with_int), "range");
    }

    #[test]
    fn a_soft_delete_stamp_is_never_a_cursor_or_a_partition_key() {
        let mut deleted_at = col("deleted_at", "timestamp", false);
        deleted_at.is_nullable = true;
        let only_tombstone = make_table(100, vec![col("email", "text", false), deleted_at.clone()]);
        assert_eq!(only_tombstone.chosen_cursor_column(), None);
        assert_eq!(only_tombstone.best_cursor_column(), None);
        assert_eq!(only_tombstone.best_partition_column(), None);
        let with_created = make_table(100, vec![deleted_at, col("created_at", "timestamp", false)]);
        assert_eq!(
            with_created.chosen_cursor_column().as_deref(),
            Some("created_at")
        );
        assert_eq!(with_created.best_partition_column(), Some("created_at"));
    }

    #[test]
    fn an_incremental_cursor_that_misses_updates_is_marked_insert_only() {
        let by_id = make_table(
            100,
            vec![col("id", "bigint", true), col("v", "text", false)],
        );
        let yaml = scaffold(&by_id, Some("incremental"));
        assert_eq!(
            scaffolded(&by_id, Some("incremental"))
                .decisions
                .insert_only,
            vec!["orders".to_string()],
            "got:\n{yaml}"
        );
        let by_stamp = make_table(
            100,
            vec![
                col("id", "bigint", true),
                col("updated_at", "timestamp", false),
            ],
        );
        let yaml = scaffold(&by_stamp, Some("incremental"));
        assert!(
            scaffolded(&by_stamp, Some("incremental"))
                .decisions
                .insert_only
                .is_empty(),
            "got:\n{yaml}"
        );
    }

    #[test]
    fn generate_config_gcs_adds_credentials_file_when_set() {
        let info = make_table(100, vec![col("id", "bigint", true)]);
        let dest = InitYamlDestination {
            gcs_bucket: Some("b".to_string()),
            gcs_credentials_file: Some("/path/sa.json".to_string()),
            s3_bucket: None,
            s3_region: None,
            bigquery_project: None,
            bigquery_dataset: None,
            clickhouse_url: None,
            clickhouse_database: None,
            clickhouse_user: None,
            snowflake: None,
            cdc_name_stem: None,
            azure: None,
        };
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
            None,
        )
        .unwrap();
        assert!(
            yaml.contains("credentials_file: /path/sa.json"),
            "got:\n{yaml}"
        );
    }

    #[test]
    fn generate_config_s3_bucket_with_region_emits_full_destination() {
        let info = make_table(
            2_000_000,
            vec![col("id", "bigint", true), col("name", "text", false)],
        );
        let dest = InitYamlDestination {
            gcs_bucket: None,
            gcs_credentials_file: None,
            s3_bucket: Some("my-s3-bucket".to_string()),
            s3_region: Some("eu-central-1".to_string()),
            bigquery_project: None,
            bigquery_dataset: None,
            clickhouse_url: None,
            clickhouse_database: None,
            clickhouse_user: None,
            snowflake: None,
            cdc_name_stem: None,
            azure: None,
        };
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
            None,
        )
        .unwrap();
        assert!(yaml.contains("type: s3"), "got:\n{yaml}");
        assert!(yaml.contains("bucket: my-s3-bucket"), "got:\n{yaml}");
        assert!(yaml.contains("prefix: exports/orders/"), "got:\n{yaml}");
        assert!(yaml.contains("region: eu-central-1"), "got:\n{yaml}");
        assert!(
            !yaml.contains("type: local"),
            "should not fall back to local:\n{yaml}"
        );
        assert!(
            !yaml.contains("type: gcs"),
            "must not mix s3 with gcs lines:\n{yaml}"
        );
    }

    #[test]
    fn generate_config_s3_without_region_omits_region_line() {
        let info = make_table(100, vec![col("id", "bigint", true)]);
        let dest = InitYamlDestination {
            gcs_bucket: None,
            gcs_credentials_file: None,
            s3_bucket: Some("b".to_string()),
            s3_region: None,
            bigquery_project: None,
            bigquery_dataset: None,
            clickhouse_url: None,
            clickhouse_database: None,
            clickhouse_user: None,
            snowflake: None,
            cdc_name_stem: None,
            azure: None,
        };
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
            None,
        )
        .unwrap();
        assert!(yaml.contains("type: s3"), "got:\n{yaml}");
        assert!(!yaml.contains("region:"), "got:\n{yaml}");
    }

    #[test]
    fn validate_rejects_both_buckets_set() {
        let dest = InitYamlDestination {
            gcs_bucket: Some("g".into()),
            gcs_credentials_file: None,
            s3_bucket: Some("s".into()),
            s3_region: None,
            bigquery_project: None,
            bigquery_dataset: None,
            clickhouse_url: None,
            clickhouse_database: None,
            clickhouse_user: None,
            snowflake: None,
            cdc_name_stem: None,
            azure: None,
        };
        let err = dest.validate().expect_err("conflict must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("--gcs-bucket") && msg.contains("--s3-bucket"),
            "expected conflict diagnostic, got: {msg}"
        );
    }

    /// `--azure-container` writes an Azure destination that names the container, the account and the env var of the key, never a key, and the endpoint only when one was given.
    #[test]
    fn an_azure_scaffold_names_its_container_account_and_key_env_and_no_secret() {
        let info = make_table(
            2_000_000,
            vec![col("id", "bigint", true), col("name", "text", false)],
        );
        let at = |endpoint: Option<&str>| InitYamlDestination {
            azure: Some(InitAzureTarget {
                container: "rivet-c".into(),
                account: "acct".into(),
                endpoint: endpoint.map(str::to_string),
            }),
            ..Default::default()
        };
        let scaffold = |dest: &InitYamlDestination| {
            let yaml = yaml_scaffold::generate_config(
                &info,
                "postgresql://localhost/db",
                &super::SourceProvenance::Inline,
                dest,
                None,
                None,
            )
            .unwrap();
            let cfg =
                crate::config::Config::from_yaml(&yaml).unwrap_or_else(|e| panic!("{e:#}\n{yaml}"));
            (cfg.exports[0].destination.clone(), yaml)
        };
        let (d, yaml) = scaffold(&at(Some("http://127.0.0.1:10000/devstoreaccount1")));
        assert_eq!(
            d.destination_type,
            crate::config::DestinationType::Azure,
            "{yaml}"
        );
        assert_eq!(
            (
                d.bucket.as_deref(),
                d.prefix.as_deref(),
                d.account_name.as_deref(),
                d.account_key_env.as_deref(),
                d.sas_token_env.as_deref(),
                d.endpoint.as_deref(),
            ),
            (
                Some("rivet-c"),
                Some("exports/orders/"),
                Some("acct"),
                Some("AZURE_STORAGE_KEY"),
                None,
                Some("http://127.0.0.1:10000/devstoreaccount1"),
            ),
            "{yaml}"
        );
        assert!(!d.allow_anonymous, "{yaml}");
        let (d, yaml) = scaffold(&at(None));
        assert_eq!(d.endpoint, None, "derived from the account: {yaml}");
    }

    /// Each destination kind alone is a bucket scaffold; none is a local one.
    #[test]
    fn each_bucket_flag_alone_makes_a_bucket_scaffold() {
        let azure = InitYamlDestination {
            azure: Some(InitAzureTarget {
                container: "c".into(),
                account: "a".into(),
                endpoint: None,
            }),
            ..Default::default()
        };
        let gcs = InitYamlDestination {
            gcs_bucket: Some("g".into()),
            ..Default::default()
        };
        let s3 = InitYamlDestination {
            s3_bucket: Some("s".into()),
            ..Default::default()
        };
        assert_eq!(
            [&azure, &gcs, &s3, &InitYamlDestination::default()].map(|d| d.is_bucket()),
            [true, true, true, false]
        );
    }

    #[test]
    fn validate_refuses_azure_beside_another_bucket() {
        let azure = Some(InitAzureTarget {
            container: "c".into(),
            account: "a".into(),
            endpoint: None,
        });
        for dest in [
            InitYamlDestination {
                gcs_bucket: Some("g".into()),
                azure: azure.clone(),
                ..Default::default()
            },
            InitYamlDestination {
                s3_bucket: Some("s".into()),
                azure: azure.clone(),
                ..Default::default()
            },
        ] {
            let err = dest.validate().expect_err("two destinations");
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_CLI_OPTION_UNSUPPORTED")
            );
            assert!(format!("{err}").contains("--azure-container"), "{err}");
        }
        InitYamlDestination {
            azure,
            ..Default::default()
        }
        .validate()
        .unwrap();
    }

    /// Two listings of one catalog in different orders scaffold one order.
    #[test]
    fn a_catalog_listing_is_ordered_by_rivet_whatever_order_it_came_in() {
        let names = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let want = names(&["Orders", "audit", "orders", "users"]);
        for listed in [
            names(&["users", "orders", "audit", "Orders"]),
            names(&["audit", "Orders", "users", "orders"]),
            names(&["orders", "users", "Orders", "audit"]),
        ] {
            assert_eq!(catalog_order(listed), want);
        }
    }

    #[test]
    fn validate_accepts_neither_or_one_bucket() {
        InitYamlDestination::default().validate().unwrap();
        InitYamlDestination {
            gcs_bucket: Some("g".into()),
            ..Default::default()
        }
        .validate()
        .unwrap();
        InitYamlDestination {
            s3_bucket: Some("s".into()),
            ..Default::default()
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn yaml_quoting_is_noop_for_safe_values() {
        for v in ["bucket-1", "my_table", "exports/orders/", "us-east-1"] {
            assert_eq!(
                yaml_scaffold::yaml_quote_if_needed(v),
                v,
                "unexpected quoting for {v:?}"
            );
        }
    }

    #[test]
    fn yaml_quoting_handles_risky_values() {
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("true"), "\"true\"");
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("NO"), "\"NO\"");
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("null"), "\"null\"");
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("123"), "\"123\"");
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("1.5"), "\"1.5\"");
        assert_eq!(
            yaml_scaffold::yaml_quote_if_needed("-leading-dash"),
            "\"-leading-dash\""
        );
        assert_eq!(
            yaml_scaffold::yaml_quote_if_needed("foo: bar"),
            "\"foo: bar\""
        );
        assert_eq!(
            yaml_scaffold::yaml_quote_if_needed("foo #bar"),
            "\"foo #bar\""
        );
        assert_eq!(
            yaml_scaffold::yaml_quote_if_needed(" leading"),
            "\" leading\""
        );
        assert_eq!(yaml_scaffold::yaml_quote_if_needed(""), "\"\"");
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("a\"b"), "\"a\\\"b\"");
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("c\\d"), "\"c\\\\d\"");
    }

    #[test]
    fn generate_config_quotes_risky_bucket_name() {
        let info = make_table(100, vec![col("id", "bigint", true)]);
        let dest = InitYamlDestination {
            gcs_bucket: Some("true".to_string()),
            ..Default::default()
        };
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
            None,
        )
        .unwrap();
        assert!(
            yaml.contains("bucket: \"true\""),
            "risky bucket name must be quoted, got:\n{yaml}"
        );
        let parsed: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&yaml).expect("YAML must remain valid");
        let bucket = &parsed["exports"][0]["destination"]["bucket"];
        assert_eq!(
            bucket.as_str(),
            Some("true"),
            "bucket must deserialize as a string, got: {bucket:?}"
        );
    }

    #[test]
    fn generate_config_quotes_credentials_path_with_spaces() {
        let info = make_table(100, vec![col("id", "bigint", true)]);
        let dest = InitYamlDestination {
            gcs_bucket: Some("b".into()),
            gcs_credentials_file: Some("/path with spaces/sa.json".into()),
            ..Default::default()
        };
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
            None,
        )
        .unwrap();
        let parsed: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&yaml).expect("YAML must remain valid");
        assert_eq!(
            parsed["exports"][0]["destination"]["credentials_file"].as_str(),
            Some("/path with spaces/sa.json"),
        );
    }

    #[test]
    fn generate_config_full_yaml_round_trips() {
        let info = make_table(
            2_000_000,
            vec![col("id", "bigint", true), col("name", "text", false)],
        );
        let dest = InitYamlDestination {
            s3_bucket: Some("ok-bucket".into()),
            s3_region: Some("eu-west-1".into()),
            ..Default::default()
        };
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
            None,
        )
        .unwrap();
        let parsed: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&yaml).expect("scaffold must be valid YAML");
        let exp = &parsed["exports"][0];
        assert_eq!(exp["name"].as_str(), Some("orders"));
        assert_eq!(exp["mode"].as_str(), Some("chunked"));
        assert_eq!(exp["chunk_by_key"].as_str(), Some("id"));
        assert_eq!(exp["table"].as_str(), Some("orders"));
        assert!(exp["chunk_column"].is_null());
        assert_eq!(exp["chunk_checkpoint"].as_bool(), Some(true));
        assert!(exp["keyset_incremental"].is_null());
        assert_eq!(exp["meta_columns"]["row_hash"].as_bool(), Some(true));
        assert_eq!(exp["destination"]["type"].as_str(), Some("s3"));
        assert_eq!(exp["destination"]["bucket"].as_str(), Some("ok-bucket"));
        assert_eq!(exp["destination"]["region"].as_str(), Some("eu-west-1"));
        assert_eq!(
            exp["destination"]["prefix"].as_str(),
            Some("exports/orders/")
        );
    }

    #[test]
    fn source_type_recognizes_sqlserver_and_mssql_schemes() {
        assert_eq!(
            source_type("sqlserver://sa:p@host:1433/db").unwrap(),
            "mssql"
        );
        assert_eq!(source_type("mssql://sa:p@host:1433/db").unwrap(), "mssql");
    }

    #[test]
    fn oracle_schema_scope_label_names_the_owner_or_the_current_schema() {
        let url = "oracle://u:p@h:1521/SVC";
        assert_eq!(
            schema_scope_label(url, Some("RIVET"), 2).unwrap(),
            "Oracle schema \"RIVET\" (2 objects)"
        );
        assert_eq!(
            schema_scope_label(url, Some(" "), 1).unwrap(),
            "Oracle schema \"(current schema)\" (1 object)"
        );
    }

    #[test]
    fn source_type_unsupported_scheme_names_sqlserver() {
        let err = source_type("db2://host/db").expect_err("db2 is unsupported");
        let msg = format!("{err}");
        assert!(
            msg.contains("sqlserver://"),
            "error must list sqlserver:// as accepted, got: {msg}"
        );
    }

    #[test]
    fn mssql_table_schema_defaults_public_placeholder_to_dbo() {
        assert_eq!(mssql_table_schema("public"), "dbo");
        assert_eq!(mssql_table_schema("sales"), "sales");
    }

    #[test]
    fn init_accepts_oracle_cdc_and_still_refuses_what_the_loader_refuses() {
        let ora = "oracle://rivet:rivet@h:1521/FREEPDB1";
        assert!(refuse_unsupported_forced_mode(ora, Some("cdc")).is_ok());
        assert!(refuse_unsupported_forced_mode(ora, Some("chunked")).is_ok());
        assert!(refuse_unsupported_forced_mode("mongodb://h/db", Some("chunked")).is_err());
    }

    #[test]
    fn an_oracle_include_glob_matches_whatever_case_it_is_typed_in() {
        let f = TableFilter {
            include: vec!["bh2_sk_i*".into()],
            exclude: vec!["*_tmp".into()],
        };
        assert!(f.matches_folded("BH2_SK_IOT"));
        assert!(f.matches_folded("bh2_sk_Iot"));
        assert!(!f.matches_folded("BH2_SK_IOT_TMP"));
        assert!(!f.matches_folded("BH2_SK_NUM"));
        assert!(!f.matches("BH2_SK_IOT"), "the plain matcher stays exact");
    }

    #[test]
    fn a_forced_mode_mongo_cannot_run_is_refused_in_the_loaders_words() {
        for m in ["incremental", "chunked", "time_window"] {
            let err = super::refuse_unsupported_forced_mode("mongodb://h/db", Some(m))
                .unwrap_err()
                .to_string();
            assert_eq!(
                err,
                format!(
                    "init: --mode {m}: source type 'Mongo' supports `mode: full` (batch) and \
                     `mode: cdc` (change streams) (got `mode: {m}`). MongoDB has no SQL, so \
                     chunked / incremental / keyset / time-window are not available; every \
                     document exports as `_id` + a `document` JSON column."
                )
            );
        }
        for m in [None, Some("full"), Some("cdc")] {
            assert!(super::refuse_unsupported_forced_mode("mongodb://h/db", m).is_ok());
        }
        assert!(
            super::refuse_unsupported_forced_mode("postgresql://h/db", Some("chunked")).is_ok()
        );
    }

    #[test]
    fn reject_mongo_schema_refuses_explicit_schema_but_allows_absent() {
        assert!(super::reject_mongo_schema(Some("mydb")).is_err());
        assert!(super::reject_mongo_schema(Some("  ")).is_ok());
        assert!(super::reject_mongo_schema(None).is_ok());
        let err = super::reject_mongo_schema(Some("mydb"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("not supported for MongoDB"), "got: {err}");
    }

    #[test]
    fn resolve_single_table_schema_honors_schema_flag_and_detects_conflict() {
        assert_eq!(
            resolve_single_table_schema("users", Some("myschema")).unwrap(),
            (Some("myschema".to_string()), "users")
        );
        assert_eq!(
            resolve_single_table_schema("users", None).unwrap(),
            (None, "users")
        );
        assert_eq!(
            resolve_single_table_schema("sales.orders", Some("sales")).unwrap(),
            (Some("sales".to_string()), "orders")
        );
        assert_eq!(
            resolve_single_table_schema("sales.orders", None).unwrap(),
            (Some("sales".to_string()), "orders")
        );
        let err = resolve_single_table_schema("sales.orders", Some("other"))
            .expect_err("conflicting schema must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("already names schema 'sales'") && msg.contains("--schema is 'other'"),
            "conflict must name both schemas: {msg}"
        );
        assert_eq!(
            resolve_single_table_schema("sales.orders", Some("  ")).unwrap(),
            (Some("sales".to_string()), "orders")
        );
    }

    #[test]
    fn no_tables_error_points_at_the_filter_when_globs_are_active() {
        let filtered = no_tables_error(&TableFilter {
            include: vec!["zzz_nomatch_*".into()],
            exclude: vec![],
        });
        assert!(
            format!("{filtered}").contains("--include/--exclude"),
            "filtered-out error must name the globs: {filtered}"
        );
        let exclude_all = no_tables_error(&TableFilter {
            include: vec![],
            exclude: vec!["*".into()],
        });
        assert!(format!("{exclude_all}").contains("--include/--exclude"));
        let unfiltered = no_tables_error(&TableFilter::default());
        assert!(
            format!("{unfiltered}").contains("check --schema and privileges"),
            "unfiltered error keeps the schema/privileges hint: {unfiltered}"
        );
    }

    #[test]
    fn init_stops_carry_their_codes() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let err = write_config_output(f.path().to_str().unwrap(), "source: {}\n").unwrap_err();
        crate::error::assert_coded(&err, "RIVET_INIT_CONFIG_EXISTS", 5);
        let err = no_tables_error(&TableFilter::default());
        crate::error::assert_coded(&err, "RIVET_INIT_NO_TABLES", 1);
        let err = table_not_found("public.orders");
        crate::error::assert_coded(&err, "RIVET_INIT_TABLE_NOT_FOUND", 1);
        assert!(
            err.to_string()
                .starts_with("Table 'public.orders' not found")
        );
    }

    #[test]
    fn write_config_output_names_the_path_and_op_on_failure() {
        let err = write_config_output("no_such_dir_xyz/sub/cfg.yaml", "source: {}\n")
            .expect_err("a missing parent dir must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("no_such_dir_xyz/sub/cfg.yaml"),
            "must name the path: {msg}"
        );
        assert!(
            msg.contains("could not write config"),
            "must name the operation: {msg}"
        );
    }

    #[test]
    fn glob_match_literal_star_and_question() {
        assert!(glob_match("orders", "orders"));
        assert!(!glob_match("orders", "orders2"));
        assert!(glob_match("bench_*", "bench_users"));
        assert!(glob_match("bench_*", "bench_"));
        assert!(!glob_match("bench_*", "orders"));
        assert!(glob_match("*_tmp", "orders_tmp"));
        assert!(glob_match("a*c", "abc"));
        assert!(glob_match("a*c", "ac"));
        assert!(!glob_match("a*c", "ab"));
        assert!(glob_match("order?", "orders"));
        assert!(!glob_match("order?", "order"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
        assert!(glob_match("*bench*", "x_bench_y"));
        assert!(!glob_match("*bench*", "orders"));
    }

    #[test]
    fn table_filter_empty_keeps_everything() {
        let f = TableFilter::default();
        assert!(f.matches("orders"));
        assert!(f.matches("bench_users"));
    }

    #[test]
    fn table_filter_include_only_keeps_matches() {
        let f = TableFilter {
            include: vec!["orders".into()],
            exclude: vec![],
        };
        assert!(f.matches("orders"));
        assert!(!f.matches("users"));
        assert!(!f.matches("bench_orders"));
    }

    #[test]
    fn table_filter_exclude_drops_matches() {
        let f = TableFilter {
            include: vec![],
            exclude: vec!["bench_*".into()],
        };
        assert!(f.matches("orders"));
        assert!(!f.matches("bench_users"));
    }

    #[test]
    fn table_filter_exclude_wins_over_include() {
        let f = TableFilter {
            include: vec!["*".into()],
            exclude: vec!["bench_*".into()],
        };
        assert!(f.matches("orders"));
        assert!(!f.matches("bench_users"));
    }

    #[test]
    fn table_discovery_surfaces_cursor_and_coalesce_hint() {
        let info = make_table(
            300_000,
            vec![
                col("key", "uuid", false),
                col("created_at", "timestamp", false),
                ColumnInfo {
                    name: "updated_at".into(),
                    data_type: "timestamp".into(),
                    is_primary_key: false,
                    is_nullable: true,
                    numeric_precision: None,
                    numeric_scale: None,
                    is_indexed: false,
                    ..Default::default()
                },
            ],
        );
        let td = table_discovery(&info);
        assert_eq!(td.suggested_mode, "incremental");
        assert_eq!(td.cursor_candidates[0].column, "updated_at");
        assert_eq!(
            td.suggested_cursor_fallback_column,
            Some("created_at".into())
        );
        assert!(
            td.notes.iter().any(|n| n.contains("coalesce")),
            "expected coalesce hint, got: {:?}",
            td.notes
        );
    }
}

#[cfg(test)]
mod explicit_state_tests {
    /// A named state that does not answer stops `init` before the config is written; without one nothing is asked.
    #[test]
    fn init_asks_a_named_state_only() {
        let gone = "/nonexistent-rivet-init-dir/c.yaml";
        if !crate::state::StateStore::names_a_server() {
            assert!(super::require_explicit_state(gone, true).is_err());
        }
        super::require_explicit_state(gone, false).unwrap();
    }
}

#[cfg(test)]
mod cdc_names_taken_tests {
    /// A one-table MySQL CDC config whose checkpoint is `ckpt` and whose destination is `out`.
    fn cdc_yaml(ckpt: &str, out: &str) -> String {
        format!(
            "source:\n  type: mysql\n  url: \"mysql://u:p@localhost/app\"\nexports:\n  - name: orders_cdc\n    \
             table: orders\n    mode: cdc\n    format: parquet\n    cdc:\n      checkpoint: {ckpt}\n      \
             server_id: 4300\n    destination: {{ type: local, path: {out} }}\n"
        )
    }

    /// A scaffold is refused, writing nothing, when its checkpoint exists or a sibling config names its checkpoint or its destination.
    #[test]
    fn a_scaffold_naming_another_streams_checkpoint_or_destination_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let at = |f: &str| dir.path().join(f).display().to_string();
        let mine = cdc_yaml("./cdc/b.ckpt", "./output/b/");
        assert!(super::refuse_cdc_names_taken(&at("b.yaml"), &mine).is_ok());
        std::fs::write(at("a.yaml"), cdc_yaml("./cdc/b.ckpt", "./output/a/")).unwrap();
        let shared_ckpt = super::refuse_cdc_names_taken(&at("b.yaml"), &mine).unwrap_err();
        std::fs::write(at("a.yaml"), cdc_yaml("./cdc/a.ckpt", "./output/b/")).unwrap();
        let shared_dest = super::refuse_cdc_names_taken(&at("b.yaml"), &mine).unwrap_err();
        std::fs::write(at("a.yaml"), cdc_yaml("./cdc/a.ckpt", "./output/a/")).unwrap();
        assert!(super::refuse_cdc_names_taken(&at("b.yaml"), &mine).is_ok());
        std::fs::write(at("c.yml"), cdc_yaml("./cdc/b.ckpt", "./output/c/")).unwrap();
        let yml = super::refuse_cdc_names_taken(&at("b.yaml"), &mine).unwrap_err();
        assert!(format!("{yml:#}").contains("c.yml"), "{yml:#}");
        std::fs::remove_file(at("c.yml")).unwrap();
        std::fs::write(at("c.txt"), cdc_yaml("./cdc/b.ckpt", "./output/c/")).unwrap();
        std::fs::write(at("b.yaml"), &mine).unwrap();
        assert!(super::refuse_cdc_names_taken(&at("b.yaml"), &mine).is_ok());
        std::fs::remove_file(at("b.yaml")).unwrap();
        std::fs::create_dir_all(dir.path().join("cdc")).unwrap();
        std::fs::write(dir.path().join("cdc/b.ckpt"), "{}").unwrap();
        let exists = super::refuse_cdc_names_taken(&at("b.yaml"), &mine).unwrap_err();
        for (err, says) in [
            (shared_ckpt, "share its CDC checkpoint"),
            (shared_dest, "share its CDC destination"),
            (exists, "already exists"),
        ] {
            crate::error::assert_coded(&err, "RIVET_INIT_CDC_NAME_TAKEN", 5);
            assert!(format!("{err:#}").contains(says), "{err:#}");
        }
    }
}
