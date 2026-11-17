mod anchor;
mod cdc;
pub(crate) mod credential_file;
pub mod cursor;
pub(crate) mod destination;
pub(crate) mod destination_credential;
mod export;
mod format;
mod gcp_secret;
mod lints;
pub mod load;
mod notifications;
pub(crate) mod quantity;
pub mod resolve;
pub mod schema;
pub(crate) mod snowflake_account;
mod source;
pub(crate) mod url;

pub use cdc::*;
pub use cursor::IncrementalCursorMode;
pub use destination::*;
pub(crate) use destination_credential::DestinationCredential;
pub use export::*;
pub use format::*;
pub use notifications::*;
#[allow(unused_imports)]
pub(crate) use resolve::{literal_parts, resolve_env_vars};
pub use resolve::{parse_file_size, resolve_vars};
pub use schema::generate_config_schema_pretty;
pub use source::*;

use quantity::Knob;
use schemars::JsonSchema;
use serde::Deserialize;

/// Top-level Rivet configuration root.
///
/// Operators write this struct as YAML (typically `rivet.yaml`).  The
/// `JsonSchema` derive is the source of truth for the `schemas/rivet.schema.json`
/// artifact and the `rivet schema config` command's output (v0.7.3 P0).
#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub source: SourceConfig,
    pub exports: Vec<ExportConfig>,
    #[serde(default)]
    pub notifications: Option<NotificationsConfig>,
    /// Same as `rivet run --parallel-exports`: the exports run concurrently, at most 16 at once; a CDC export run alone also takes its pending baseline snapshots at most 16 at once.
    #[serde(default)]
    pub parallel_exports: bool,
    #[serde(default)]
    pub parallel_export_processes: bool,
    /// The warehouse **load** target — consumed by `rivet load`, so ONE config drives
    /// both the export and the downstream load. The extraction commands validate it
    /// (a malformed block fails `rivet check` before an extract runs) and otherwise
    /// ignore it: it shapes the load, not the extract.
    #[serde(default)]
    pub load: Option<load::LoadSection>,
}

/// Two configured `tables:` entries that can name ONE relation, if any.
pub(crate) fn overlapping_table_pair(ts: &[String]) -> Option<(String, String)> {
    let leaf = |t: &str| {
        t.rsplit_once('.')
            .map_or(t.to_string(), |(_, l)| l.to_string())
    };
    for (i, a) in ts.iter().enumerate() {
        for b in ts.iter().skip(i + 1) {
            let (qa, qb) = (a.contains('.'), b.contains('.'));
            if qa != qb && leaf(a) == leaf(b) {
                return Some((a.clone(), b.clone()));
            }
        }
    }
    None
}

/// Two `tables:` entries whose per-table directories are one directory on a case-insensitive filesystem, if any.
pub(crate) fn case_colliding_table_pair(ts: &[String]) -> Option<(String, String)> {
    let mut first_by_folded = std::collections::HashMap::new();
    for t in ts {
        if let Some(prev) = first_by_folded.insert(t.to_lowercase(), t)
            && prev != t
        {
            return Some((prev.clone(), t.clone()));
        }
    }
    None
}

impl Config {
    pub fn load(path: &str) -> crate::error::Result<Self> {
        Self::load_with_params(path, None)
    }

    pub fn load_with_params(
        path: &str,
        params: Option<&std::collections::HashMap<String, String>>,
    ) -> crate::error::Result<Self> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                crate::error::coded(
                    crate::error::codes::CONFIG_FILE_UNREADABLE,
                    format!(
                        "config file '{}' not found.\n  Hint: check the path, or run `rivet init` to generate one.",
                        path
                    ),
                )
            } else {
                crate::error::coded(
                    crate::error::codes::CONFIG_FILE_UNREADABLE,
                    format!("cannot read config file '{}': {}", path, e),
                )
            }
        })?;
        resolve::warn_unused_params(&contents, params);
        let resolved = resolve_vars(&contents, params)?;
        let mut config =
            Self::from_yaml(&resolved).map_err(|e| e.context(format!("config file '{}'", path)))?;
        config.record_queries_as_written(&contents);
        if seeded!("relative-destination-path-follows-the-cwd", false, true) {
            config.anchor_paths(path, std::env::current_dir().ok())?;
        }
        if seeded!("named-credential-falls-back", false, true) {
            config.anchor_files(path, std::env::current_dir().ok())?;
        }
        Ok(config)
    }

    /// Anchor every relative file a config names (`url_file`, `tls.ca_file`, the load target's key and CA, each export's `credentials_file`) to the config file's directory through the one `Anchor`.
    fn anchor_files(
        &mut self,
        config_path: &str,
        cwd: Option<std::path::PathBuf>,
    ) -> crate::error::Result<()> {
        let Some(anchor) = anchor::Anchor::of(config_path, cwd) else {
            return Ok(());
        };
        if seeded!("url-file-follows-the-cwd", false, true) {
            if let Some(written) = self.source.url_file.clone() {
                self.source.url_file = Some(anchor.file("source: url_file", &written)?);
            }
            if let Some(tls) = &mut self.source.tls
                && let Some(written) = tls.ca_file.clone()
            {
                tls.ca_file = Some(anchor.file("source: tls.ca_file", &written)?);
            }
            if let Some(load) = &mut self.load {
                match &mut load.target {
                    load::LoadTarget::Snowflake {
                        private_key_path, ..
                    } => {
                        *private_key_path =
                            anchor.file("load: private_key_path", private_key_path)?;
                    }
                    load::LoadTarget::Clickhouse {
                        ca_file: Some(ca_file),
                        ..
                    } => {
                        *ca_file = anchor.file("load: ca_file", ca_file)?;
                    }
                    _ => {}
                }
            }
        }
        for export in &mut self.exports {
            let Some(written) = export.destination.credentials_file.clone() else {
                continue;
            };
            let field = format!("export '{}': credentials_file", export.name);
            let anchored = anchor.file(&field, &written)?;
            credential_file::google_key(&field, &anchored)?;
            export.destination.credentials_file = Some(anchored);
        }
        Ok(())
    }

    /// Anchor every relative local destination path to the config file's directory.
    fn anchor_paths(
        &mut self,
        config_path: &str,
        cwd: Option<std::path::PathBuf>,
    ) -> crate::error::Result<()> {
        let Some(dir) = std::path::absolute(config_path)
            .ok()
            .and_then(|p| p.parent().map(destination::normalized))
        else {
            return Ok(());
        };
        let cwd = cwd.filter(|c| *c != dir);
        for export in &mut self.exports {
            let Some(written) = export.destination.anchor(&dir) else {
                continue;
            };
            let Some(cwd) = &cwd else { continue };
            let old = destination::literal_root(&cwd.join(&written));
            let new = destination::literal_root(std::path::Path::new(
                export
                    .destination
                    .path
                    .as_deref()
                    .or(export.destination.prefix.as_deref())
                    .unwrap_or(""),
            ));
            if destination::relative_path_moved(&export.name, &old, &new) {
                crate::rivet_bail!(
                    crate::error::codes::DEST_RELATIVE_PATH_MOVED,
                    "export '{}': `destination.path: {written}` is relative and names {} (beside the config) since 0.32, while {} (under the working directory, where a release before 0.32 wrote it) holds this export's files and {} holds none: move the files to {}, or make `destination.path` absolute",
                    export.name,
                    new.display(),
                    old.display(),
                    new.display(),
                    new.display()
                );
            }
        }
        Ok(())
    }

    /// Record each export's inline `query:` as the file spells it, placeholders in place, where the unresolved document parses.
    fn record_queries_as_written(&mut self, unresolved: &str) {
        let doc = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&resolve::masked(unresolved));
        let written = doc
            .ok()
            .and_then(|d| d.get("exports")?.as_sequence().cloned());
        for (export, written) in self.exports.iter_mut().zip(written.iter().flatten()) {
            let query = written.get("query").and_then(|q| q.as_str());
            export.query_as_written = query.map(resolve::unmasked);
        }
    }

    pub fn from_yaml(yaml: &str) -> crate::error::Result<Self> {
        if let Err(e) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(yaml) {
            let m = e.to_string();
            if let Some(hint) = Self::unquoted_template_brace_hint(yaml, &m) {
                return Err(lints::yaml_invalid(format!("{e}\n  {hint}")));
            }
            if let Some(hint) = Self::tab_indent_hint(yaml, &m) {
                return Err(lints::yaml_invalid(format!("{e}\n  {hint}")));
            }
            return Err(lints::yaml_invalid(m));
        }
        Self::check_misplaced_tuning_fields(yaml)?;
        Self::check_csv_compression(yaml)?;
        Self::check_tls_mode_downgrade(yaml)?;
        Self::check_snowflake_connection(yaml)?;
        let config: Config = serde_yaml_ng::from_str(yaml).map_err(|e| {
            if let Some(hint) = Self::unquoted_template_brace_hint(yaml, &e.to_string()) {
                lints::yaml_invalid(format!("{e}\n  {hint}"))
            } else {
                lints::enhance_parse_error(e)
            }
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Detect the unquoted-`{partition}` (or `{date}`, …) template footgun and return an actionable quoting hint, or `None` when the error is unrelated.
    fn tab_indent_hint(yaml: &str, err_msg: &str) -> Option<String> {
        if !err_msg.contains("tab character") {
            return None;
        }
        let line_no: usize = err_msg
            .split_once("line ")
            .and_then(|(_, rest)| rest.split([' ', ',']).next())
            .and_then(|n| n.parse().ok())?;
        let line = yaml.lines().nth(line_no.checked_sub(1)?)?;
        let leading = &line[..line.len() - line.trim_start().len()];
        leading.contains('\t').then(|| {
            format!(
                "line {line_no} is indented with a TAB — YAML requires spaces. Replace the tab(s) with spaces."
            )
        })
    }

    fn unquoted_template_brace_hint(yaml: &str, err_msg: &str) -> Option<String> {
        const FLOW_SYMPTOMS: &[&str] = &[
            "did not find expected ',' or '}'",
            "while parsing a flow mapping",
            "invalid type: map, expected a string",
            "did not find expected key",
        ];
        if !FLOW_SYMPTOMS.iter().any(|s| err_msg.contains(s)) {
            return None;
        }
        if !yaml.lines().any(line_has_unquoted_brace_value) {
            return None;
        }
        Some(
            "a YAML value containing { } (such as {partition} or {date}) must be quoted, \
             e.g. prefix: \"exports/{partition}/\""
                .to_string(),
        )
    }

    /// Reject `format: csv` paired with an explicitly-requested compression codec (Finding #10).
    fn check_csv_compression(yaml: &str) -> crate::error::Result<()> {
        let root: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml)?;
        let Some(exports) = root.get("exports").and_then(|e| e.as_sequence()) else {
            return Ok(());
        };
        for export in exports {
            if export.get("format").and_then(|f| f.as_str()) != Some("csv") {
                continue;
            }
            let name = export
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("<unnamed>");

            if let Some(codec) = export.get("compression").and_then(|c| c.as_str())
                && let Some(ct) = CompressionType::from_label(codec)
                && !format::compression_supported(FormatType::Csv, ct)
            {
                crate::config_bail!(
                    crate::error::codes::CONFIG_CSV_COMPRESSION_UNSUPPORTED,
                    "export '{}': CSV output does not support compression: {}. \
                     CSV has no compression encoder, so the codec would be silently dropped \
                     while the manifest records it.\n  \
                     Hint: use `format: parquet` for compression, or set `compression: none`.",
                    name,
                    codec,
                );
            }

            if let Some(profile) = export.get("compression_profile").and_then(|c| c.as_str())
                && profile != CompressionProfile::None.label()
            {
                crate::config_bail!(
                    crate::error::codes::CONFIG_CSV_COMPRESSION_UNSUPPORTED,
                    "export '{}': CSV output does not support compression_profile: {} \
                     (it resolves to a compression codec the CSV writer cannot apply).\n  \
                     Hint: use `format: parquet` for compression, or set `compression_profile: none`.",
                    name,
                    profile,
                );
            }
        }
        Ok(())
    }

    /// V13: reject a `source.tls` block that pairs an *explicitly chosen* enforced `mode:` with a verification-disabling danger knob (`accept_invalid_certs` / `accept_invalid_hostnames`).
    fn check_tls_mode_downgrade(yaml: &str) -> crate::error::Result<()> {
        let root: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml)?;
        let Some(tls) = root.get("source").and_then(|s| s.get("tls")) else {
            return Ok(());
        };

        let Some(mode) = tls.get("mode").and_then(|m| m.as_str()) else {
            return Ok(());
        };
        if mode == "disable" {
            return Ok(());
        }

        let knob = if tls
            .get("accept_invalid_certs")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            Some("accept_invalid_certs")
        } else if tls
            .get("accept_invalid_hostnames")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            Some("accept_invalid_hostnames")
        } else {
            None
        };

        if let Some(knob) = knob {
            crate::config_bail!(
                crate::error::codes::CONFIG_TLS_VERIFICATION_DOWNGRADE,
                "source.tls: {} disables certificate verification, silently downgrading the \
                 chosen `mode: {}` to trust-anything (MITM exposure — credentials and rows \
                 readable/forgeable on the wire).\n  \
                 Hint: drop the danger knob and trust a private CA with `tls.ca_file: <pem>`; \
                 only use a danger knob for a loopback self-signed dev container, and then omit \
                 the explicit `mode:` so the contradiction is gone.",
                knob,
                mode,
            );
        }
        Ok(())
    }

    /// Refuse a Snowflake `load:` that still names a `snow` CLI `connection:`, with the fields that replace it.
    fn check_snowflake_connection(yaml: &str) -> crate::error::Result<()> {
        if seeded!(
            "snowflake-connection-line-ignored-at-config-load",
            !yaml.is_empty(),
            false
        ) {
            return Ok(());
        }
        let root: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml)?;
        let Some(load) = root.get("load") else {
            return Ok(());
        };
        if load.get("target").and_then(|t| t.as_str()) != Some("snowflake") {
            return Ok(());
        }
        let Some(connection) = load.get("connection") else {
            return Ok(());
        };
        let was = connection
            .as_str()
            .map(|c| format!(" (here `{}`)", c.escape_default()))
            .unwrap_or_default();
        crate::config_bail!(
            crate::error::codes::CONFIG_LOAD_BLOCK_INVALID,
            "`load.connection`{was} named a `snow` CLI connection, and rivet no longer reads \
             it: it talks to Snowflake itself and signs in with a key pair. Replace the line \
             with these under `load:`:\n\n    \
             account: myorg-myaccount\n    \
             user: RIVET_USER\n    \
             private_key_path: /path/to/rsa_key.p8\n    \
             role: RIVET_ROLE\n\n  \
             `account` is your account identifier (or a locator such as `xy12345.us-east-1`), \
             `user` has the public half of the key registered (`ALTER USER ... SET \
             RSA_PUBLIC_KEY=...`), the key file is an unencrypted PKCS#8 PEM, and `role` is \
             optional. The env vars RIVET_SNOWFLAKE_ACCOUNT, RIVET_SNOWFLAKE_USER, \
             RIVET_SNOWFLAKE_KEY (the key file) and RIVET_SNOWFLAKE_ROLE override these four fields."
        );
    }

    /// Detect tuning-related fields placed directly under `source:` or an `exports[]` entry instead of inside the `tuning:` sub-key.
    fn check_misplaced_tuning_fields(yaml: &str) -> crate::error::Result<()> {
        const TUNING_FIELDS: &[&str] = crate::config::TUNING_FIELD_NAMES;

        let root: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml)?;

        if let Some(source) = root.get("source") {
            let misplaced: Vec<&str> = TUNING_FIELDS
                .iter()
                .copied()
                .filter(|&f| source.get(f).is_some())
                .collect();
            if !misplaced.is_empty() {
                crate::config_bail!(
                    crate::error::codes::CONFIG_YAML_INVALID,
                    "source: field(s) [{}] belong under 'source.tuning:', not directly under 'source:'. \
                     Example:\n  source:\n    tuning:\n      {}: <value>",
                    misplaced.join(", "),
                    misplaced[0],
                );
            }
        }

        if let Some(exports) = root.get("exports").and_then(|e| e.as_sequence()) {
            for (i, export) in exports.iter().enumerate() {
                let name = export
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("<unnamed>");
                let misplaced: Vec<&str> = TUNING_FIELDS
                    .iter()
                    .copied()
                    .filter(|&f| export.get(f).is_some())
                    .collect();
                if !misplaced.is_empty() {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_YAML_INVALID,
                        "export '{}' (index {}): field(s) [{}] belong under 'exports[].tuning:', \
                         not directly in the export. Example:\n  exports:\n    - name: {}\n      tuning:\n        {}: <value>",
                        name,
                        i,
                        misplaced.join(", "),
                        name,
                        misplaced[0],
                    );
                }
            }
        }

        Ok(())
    }

    /// Reject a config before any plan/connect step.
    fn validate(&self) -> crate::error::Result<()> {
        self.validate_exports_list()?;
        self.validate_source_connection()?;
        self.validate_columns_from()?;
        self.validate_non_sql_source_modes()?;
        for export in &self.exports {
            self.validate_export(export)?;
        }
        self.validate_outbound_urls_are_not_remote_plaintext()?;
        self.validate_cdc_resource_conflicts()?;
        self.validate_csv_exports_are_not_loaded()?;
        self.validate_load_overrides()?;
        self.validate_layout_has_a_compacting_warehouse()?;
        seeded!(
            "snowflake-cdc-base-without-baseline-accepted",
            {
                let _ = self.validate_a_default_base_has_a_baseline();
            },
            self.validate_a_default_base_has_a_baseline()?
        );
        self.validate_cdc_has_a_loadable_target()?;
        Ok(())
    }

    /// A ClickHouse `load.url` or a literal Slack `webhook_url` that is `http://` on a remote host is refused when the config is read.
    fn validate_outbound_urls_are_not_remote_plaintext(&self) -> crate::error::Result<()> {
        if let Some(load::LoadTarget::Clickhouse {
            url,
            allow_plaintext,
            ..
        }) = self.load.as_ref().map(|l| &l.target)
        {
            url::require_https_clickhouse_url(url, *allow_plaintext)?;
        }
        if let Some(url) = self
            .notifications
            .as_ref()
            .and_then(|n| n.slack.as_ref())
            .and_then(|s| s.webhook_url.as_ref())
        {
            url::require_https_webhook(url)?;
        }
        Ok(())
    }

    /// A CDC export whose stream the `load:` target cannot take (Oracle anywhere) is refused when the config is read, before a run extracts a stream the load would then refuse.
    fn validate_cdc_has_a_loadable_target(&self) -> crate::error::Result<()> {
        let refusal = self
            .load
            .as_ref()
            .and_then(|l| l.target.cdc_refusal(self.source.source_type));
        let cdc = self.exports.iter().find(|e| e.mode == ExportMode::Cdc);
        if let (Some(why), Some(e)) = (refusal, cdc) {
            crate::config_bail!(
                crate::error::codes::CONFIG_SOURCE_MODE_UNSUPPORTED,
                "export '{}': {}",
                e.name,
                why
            );
        }
        Ok(())
    }

    /// `layout: base_buffer` promises a `rivet compact`, which ClickHouse has none of.
    fn validate_layout_has_a_compacting_warehouse(&self) -> crate::error::Result<()> {
        use crate::config::load::LayoutChoice;
        let Some(load) = &self.load else {
            return Ok(());
        };
        if load.target.compacts() {
            return Ok(());
        }
        let base = |l: Option<LayoutChoice>| l == Some(LayoutChoice::BaseBuffer);
        let written = if base(load.layout) {
            Some("`load.layout`".to_string())
        } else {
            self.exports.iter().find_map(|e| {
                let o = e.load.as_ref()?;
                if base(o.layout) {
                    return Some(format!("export '{}': `load.layout`", e.name));
                }
                o.tables
                    .iter()
                    .find(|(_, t)| base(t.layout))
                    .map(|(t, _)| format!("export '{}': `load.tables.{t}.layout`", e.name))
            })
        };
        if let Some(site) = written {
            crate::config_bail!(
                crate::error::codes::CONFIG_LOAD_BLOCK_INVALID,
                "{site}: `base_buffer` needs `target: bigquery` or `target: snowflake` — \
                 `rivet compact` does not run on ClickHouse, so the base would never take its \
                 buffer. Drop the key to keep the changelog and its dedup view"
            );
        }
        Ok(())
    }

    /// Where a base is the default layout, a `mode.
    fn validate_a_default_base_has_a_baseline(&self) -> crate::error::Result<()> {
        use crate::config::load::{BaseSupport, LayoutChoice};
        let Some(load) = &self.load else {
            return Ok(());
        };
        if load.target.base_support() != BaseSupport::Default {
            return Ok(());
        }
        for e in self.exports.iter().filter(|e| e.mode == ExportMode::Cdc) {
            if e.cdc.as_ref().is_some_and(|c| c.has_baseline()) {
                continue;
            }
            let units: Vec<Option<&str>> = match e.multiplex_tables() {
                Some(tables) => tables.iter().map(|t| Some(t.as_str())).collect(),
                None => vec![None],
            };
            for unit in units {
                if load.overlay(e, unit).layout == Some(LayoutChoice::LogView) {
                    continue;
                }
                let site = match unit {
                    Some(t) => format!("export '{}' (table '{t}')", e.name),
                    None => format!("export '{}'", e.name),
                };
                crate::config_bail!(
                    crate::error::codes::CONFIG_LOAD_BLOCK_INVALID,
                    "{site}: on `target: {}` a `mode: cdc` export keeps a base table and a buffer, \
                     and this stream names nothing to build the base from — neither \
                     `cdc.backfill:` nor `cdc.initial: snapshot` — so the base would start empty \
                     and miss every row the stream never changes. Either add `cdc.backfill:` (or \
                     `cdc.initial: snapshot`) to build the base from a snapshot of the table, or \
                     write `load.layout: log_view` to keep the change log and its current-state \
                     view",
                    load.target.name()
                );
            }
        }
        Ok(())
    }

    /// A per-export `load:` override needs the top-level block it overrides.
    fn validate_load_overrides(&self) -> crate::error::Result<()> {
        for e in &self.exports {
            let Some(o) = &e.load else { continue };
            if o.tables.is_empty() {
                continue;
            }
            let Some(captured) = e.multiplex_tables() else {
                crate::config_bail!(
                    crate::error::codes::CONFIG_LOAD_BLOCK_INVALID,
                    "export '{}': `load.tables:` is for a `mode: cdc` export with `tables:` — \
                     this export has one unit, so its `load:` block already IS the table's",
                    e.name
                );
            };
            for (t, sub) in &o.tables {
                if !captured.iter().any(|c| c == t) {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_LOAD_BLOCK_INVALID,
                        "export '{}': `load.tables.{t}` names a table this export does not \
                         capture (tables: [{}])",
                        e.name,
                        captured.join(", ")
                    );
                }
                if !sub.tables.is_empty() {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_LOAD_BLOCK_INVALID,
                        "export '{}': `load.tables.{t}.tables` — a per-table block takes no \
                         `tables:` of its own",
                        e.name
                    );
                }
            }
        }
        if self.load.is_some() {
            return Ok(());
        }
        if let Some(e) = self.exports.iter().find(|e| e.load.is_some()) {
            crate::config_bail!(
                crate::error::codes::CONFIG_LOAD_BLOCK_INVALID,
                "export '{}' has a `load:` override but the config has no top-level `load:` \
                 block — the warehouse (`target`, `project`/`dataset`, …) is declared once, at \
                 the top level; the export's block only overrides `pk`, `cluster_by`, \
                 `partition`, `cleanup_source`, `gc_orphans` and `allow_source_drift`",
                e.name
            );
        }
        Ok(())
    }

    /// `columns_from: grants` on an engine that does not enforce it is refused for every mode, SQL or not.
    fn validate_columns_from(&self) -> crate::error::Result<()> {
        if let Some(why) = source::columns_from_refusal(&self.source) {
            crate::config_bail!(crate::error::codes::CONFIG_SOURCE_MODE_UNSUPPORTED, "{why}");
        }
        Ok(())
    }

    /// Non-SQL sources (MongoDB) support only `mode: full` today.
    fn validate_non_sql_source_modes(&self) -> crate::error::Result<()> {
        if self.source.source_type.is_sql() {
            return Ok(());
        }
        for e in &self.exports {
            if !matches!(e.mode, ExportMode::Full | ExportMode::Cdc) {
                crate::config_bail!(
                    crate::error::codes::CONFIG_SOURCE_MODE_UNSUPPORTED,
                    "export '{}': {}",
                    e.name,
                    non_sql_mode_refusal(self.source.source_type, e.mode.as_str())
                );
            }
            if e.partition_by.is_some() {
                crate::config_bail!(
                    crate::error::codes::CONFIG_SOURCE_MODE_UNSUPPORTED,
                    "export '{}': partition_by is not supported on a {:?} source — it builds SQL \
                     min/max/NULL probes over the partition column. Drop `partition_by:` and \
                     export the collection whole.",
                    e.name,
                    self.source.source_type
                );
            }
            if e.parallel > 1 && self.source.mongo.as_ref().is_some_and(|m| m.resume) {
                crate::config_bail!(
                    crate::error::codes::CONFIG_SOURCE_MODE_UNSUPPORTED,
                    "export '{}': `source.mongo.resume: true` cannot be combined with \
                     `parallel: {}` — the parallel `_id`-range fan-out keeps no keyset \
                     checkpoint, so resume would be silently ignored. Drop `parallel` \
                     to keep cross-run resume, or drop `resume` to keep the fan-out.",
                    e.name,
                    e.parallel
                );
            }
        }
        if let Some(page) = self.source.mongo.as_ref().and_then(|m| m.page_size) {
            Knob::PAGE_SIZE.validate_source(page)?;
        }
        if let Some(m) = self.source.mongo.as_ref()
            && m.resume
            && m.page_size.is_none()
        {
            crate::config_bail!(
                crate::error::codes::CONFIG_SOURCE_MODE_UNSUPPORTED,
                "`source.mongo.resume: true` requires `source.mongo.page_size` (keyset paging) — \
                 the full-scan path (no `page_size`) reads no checkpoint, so resume is silently \
                 ignored and the whole collection re-reads every run. Set `page_size:` to enable \
                 resume, or drop `resume`.",
            );
        }
        Ok(())
    }

    /// `format: csv` × a `load:` block is REFUSED.
    fn validate_csv_exports_are_not_loaded(&self) -> crate::error::Result<()> {
        if self.load.is_none() && self.exports.iter().all(|e| e.load.is_none()) {
            return Ok(());
        }
        for e in &self.exports {
            let loaded = self.load.is_some() || e.load.is_some();
            if loaded && e.format == crate::config::FormatType::Csv {
                crate::config_bail!(
                    crate::error::codes::CONFIG_CSV_LOAD_UNSUPPORTED,
                    "export '{}': `format: csv` cannot feed a `load:` block — the \
                     warehouse loaders read parquet parts only, so this load would \
                     report \"nothing to load\" and exit 0 while the CSV data sits \
                     unloaded forever. Use `format: parquet` for loaded exports, or \
                     drop the load block for this one.",
                    e.name
                );
            }
        }
        Ok(())
    }

    /// Whole-config shape: at least one export, names unique.
    fn validate_exports_list(&self) -> crate::error::Result<()> {
        if self.exports.is_empty() {
            crate::config_bail!(
                crate::error::codes::CONFIG_NO_EXPORTS,
                "exports: at least one export must be defined (got empty list)"
            );
        }

        let mut seen: std::collections::HashSet<&str> =
            std::collections::HashSet::with_capacity(self.exports.len());
        for e in &self.exports {
            if !seen.insert(e.name.as_str()) {
                crate::config_bail!(
                    crate::error::codes::CONFIG_DUPLICATE_EXPORT,
                    "exports: duplicate export name '{}' (each export must have a unique name; state is keyed by name)",
                    e.name
                );
            }
        }
        Ok(())
    }

    /// Source connection block: exactly one connection method, well-formed, and the source-level tuning that is shared by every export.
    fn validate_source_connection(&self) -> crate::error::Result<()> {
        if let Some(t) = &self.source.tuning
            && t.batch_size.is_some()
            && t.batch_size_memory_mb.is_some()
        {
            crate::config_bail!(
                crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
                "tuning: batch_size and batch_size_memory_mb are mutually exclusive. \
                 Prefer batch_size_memory_mb (rivet sizes the batch to a memory budget, \
                 adapting to row width); set batch_size only to pin an exact row count."
            );
        }

        if !self.source.has_url_fields() && !self.source.has_structured_fields() {
            crate::config_bail!(
                crate::error::codes::CONFIG_SOURCE_CONNECTION_INVALID,
                "source: no connection method configured. Add one of:\n  url_env: DATABASE_URL                          (URL from env var — recommended)\n  url: 'postgresql://user:pass@host:5432/db'      (inline — not recommended for committed configs)\n  url_file: /etc/rivet/source.url                 (URL from file — rotation-friendly)\n  url_secret: projects/<p>/secrets/<s>            (URL from Google Secret Manager)\n  host/user/database/...                          (structured fields under `source:`)"
            );
        }

        self.source.check_url_scheme_offline()?;

        if self.source.has_url_fields() {
            let url_count = [
                self.source.url.is_some(),
                self.source.url_env.is_some(),
                self.source.url_file.is_some(),
                self.source.url_secret.is_some(),
            ]
            .iter()
            .filter(|set| **set)
            .count();
            if url_count > 1 {
                crate::config_bail!(
                    crate::error::codes::CONFIG_SOURCE_CONNECTION_INVALID,
                    "source: specify exactly one of 'url', 'url_env', 'url_file' or 'url_secret' (got {} set).\n  Hint: pick one — `url_env` is recommended so credentials never enter the YAML.",
                    url_count
                );
            }
        }

        if self.source.has_url_fields() && self.source.has_structured_fields() {
            return Err(source::mixed_connection_methods());
        }

        if self.source.has_structured_fields() {
            for (field, set) in [
                ("host", self.source.host.is_some()),
                ("user", self.source.user.is_some()),
                ("database", self.source.database.is_some()),
            ] {
                if !set {
                    return Err(source::structured_field_missing(field));
                }
            }
            let passwords = [
                self.source.password.is_some(),
                self.source.password_env.is_some(),
                self.source.password_secret.is_some(),
            ];
            if passwords.iter().filter(|set| **set).count() > 1 {
                crate::config_bail!(
                    crate::error::codes::CONFIG_SOURCE_CONNECTION_INVALID,
                    "source: specify one of 'password', 'password_env' or 'password_secret', not both.\n  Hint: prefer `password_env: DB_PASSWORD` so credentials never enter the YAML."
                );
            }
        }
        Ok(())
    }

    /// Per-export rules: effective tuning, query source, `query_file` SecOps, destination auth, compression, and the mode/chunk matrix.
    fn validate_export(&self, export: &ExportConfig) -> crate::error::Result<()> {
        if export.chunk_dense {
            crate::config_bail!(
                crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                "export '{}': `chunk_dense` was removed: it re-numbered rows per chunk and \
                 skipped or duplicated rows under concurrent writes. Use `chunk_by_key: \
                 <unique key>` (keyset), or `chunk_column:` range chunking on an integer key.",
                export.name
            );
        }
        self.validate_export_names(export)?;
        self.validate_export_partition(export)?;
        self.validate_export_sizing(export)?;
        self.validate_export_source(export)?;
        self.validate_export_destination(export)?;
        self.validate_export_output(export)?;
        self.validate_export_mode(export)?;
        self.validate_export_mode_knobs(export)?;
        self.validate_export_column_overrides(export)?;
        self.validate_export_cdc(export)?;
        self.validate_export_chunk_by_days(export)?;
        Ok(())
    }

    /// Export and table names are safe path segments; a declared row-hash set fits one table.
    fn validate_export_names(&self, export: &ExportConfig) -> crate::error::Result<()> {
        if !is_filename_safe_name(&export.name) {
            crate::config_bail!(
                crate::error::codes::CONFIG_PATH_UNSAFE,
                "export name '{}' is not filename-safe: it must not be absolute, contain \
                 '/', '\\', '..', a NUL, be longer than 200 bytes, or start with '.' (the name is used in output paths \
                 and state keys). Use a plain identifier like `orders` or `daily_events`.",
                export.name.escape_default(),
            );
        }

        for t in export.tables.iter().flatten() {
            if !is_filename_safe_name(t) {
                crate::config_bail!(
                    crate::error::codes::CONFIG_PATH_UNSAFE,
                    "export '{}': tables entry '{}' is not filename-safe: it must not contain \
                     '/', '\\', '..', a NUL, or start with '.' (each table becomes a destination \
                     path segment).",
                    export.name,
                    t.escape_default(),
                );
            }
        }

        export.meta_columns.row_hash.validate(&export.name)?;

        if export.meta_columns.row_hash.declared().is_some() && export.tables.is_some() {
            crate::config_bail!(
                crate::error::codes::CONFIG_ROW_HASH_INVALID,
                "export '{}': meta_columns.row_hash names one table's columns, so it cannot \
                 apply to a multi-table `tables:` stream. Give each table its own export \
                 (with `table:`), or use `row_hash: true` to cover every column of each.",
                export.name
            );
        }
        Ok(())
    }

    /// `partition_by` static rules and prefix shapes.
    fn validate_export_partition(&self, export: &ExportConfig) -> crate::error::Result<()> {
        for field in [
            ("prefix", export.destination.prefix.as_deref()),
            ("path", export.destination.path.as_deref()),
        ] {
            let (name, Some(v)) = field else { continue };
            if v.contains('\0') || (name == "path" && v.trim().is_empty()) {
                crate::config_bail!(
                    crate::error::codes::CONFIG_PATH_UNSAFE,
                    "export '{}': destination {name} '{}' is empty or holds a NUL byte — name a \
                     directory, for example `path: ./out`.",
                    export.name,
                    v.escape_default()
                );
            }
            if v.split(['/', '\\']).any(|seg| seg == "..") {
                crate::config_bail!(
                    crate::error::codes::CONFIG_PATH_UNSAFE,
                    "export '{}': destination {name} '{}' contains a `..` segment — the \
                     objects would land OUTSIDE the configured location and every later \
                     load/list would read an empty prefix as 'up to date'.",
                    export.name,
                    v.escape_default()
                );
            }
            if name == "prefix" && v.contains("://") {
                crate::config_bail!(
                    crate::error::codes::CONFIG_PATH_UNSAFE,
                    "export '{}': destination prefix '{}' embeds a URI scheme — `prefix:` \
                     is bucket-relative (the bucket comes from `bucket:`); a pasted \
                     `gs://...` doubles into a key the load layer never finds.",
                    export.name,
                    v.escape_default()
                );
            }
        }
        if let Some(col) = export.partition_by.as_deref() {
            if col.trim().is_empty() {
                crate::config_bail!(
                    crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                    "export '{}': partition_by must name a column",
                    export.name
                );
            }
            if !is_filename_safe_name(col) {
                crate::config_bail!(
                    crate::error::codes::CONFIG_PATH_UNSAFE,
                    "export '{}': partition_by column '{}' is not filename-safe: it must not \
                     contain '/', '\\', '..', a NUL, or start with '.' (it becomes a `col=value` \
                     destination path segment).",
                    export.name,
                    col.escape_default(),
                );
            }
            if matches!(export.mode, ExportMode::TimeWindow | ExportMode::Cdc) {
                crate::config_bail!(
                    crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                    "export '{}': partition_by is not compatible with `mode: {:?}` — it is a batch \
                     output-layout feature; partition a full/chunked/incremental export instead.",
                    export.name,
                    export.mode,
                );
            }
            if export.load.is_some() || self.load.is_some() {
                crate::config_bail!(
                    crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                    "export '{}': partition_by is not compatible with a `load:` block — a \
                     partitioned export writes one manifest per partition sub-prefix, but the \
                     warehouse loader would load only a single partition (and `cleanup_source` \
                     would then wipe the rest). To load it, remove `partition_by:` from this \
                     export; to keep the partitioned layout, remove the `load:` block — the \
                     export then stays Parquet-only, since `rivet load` has no per-partition mode.",
                    export.name,
                );
            }
            if export.chunk_by_key.is_some() {
                crate::config_bail!(
                    crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                    "export '{}': partition_by is not compatible with chunk_by_key — keyset seek \
                     pagination over a partition bucket (a subquery around the table) is not \
                     supported. Use a range `chunk_column`, a smaller `partition_granularity`, \
                     or `mode: full`.",
                    export.name
                );
            }
            let has_token = export
                .destination
                .path
                .as_deref()
                .is_some_and(|s| s.contains("{partition}"))
                || export
                    .destination
                    .prefix
                    .as_deref()
                    .is_some_and(|s| s.contains("{partition}"));
            if !has_token {
                crate::config_bail!(
                    crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                    "export '{}': partition_by requires a '{{partition}}' token in destination.path \
                     or destination.prefix (otherwise every partition would overwrite the same prefix).",
                    export.name
                );
            }
        }
        Ok(())
    }

    /// Chunk-size and memory-budget knobs do not contradict each other.
    fn validate_export_sizing(&self, export: &ExportConfig) -> crate::error::Result<()> {
        if export.chunk_size_memory_mb.is_some() && export.chunk_size != default_chunk_size() {
            crate::config_bail!(
                crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
                "export '{}': chunk_size and chunk_size_memory_mb are mutually exclusive — \
                 chunk_size_memory_mb derives the window size from a memory budget, so an explicit \
                 chunk_size would be silently ignored. Set one or the other.",
                export.name
            );
        }

        for t in [self.source.tuning.as_ref(), export.tuning.as_ref()]
            .into_iter()
            .flatten()
        {
            if let Some(n) = t.batch_size {
                Knob::BATCH_SIZE.validate_export(&export.name, n)?;
            }
        }

        if let Some(q) = &export.quality {
            Knob::ratios(&format!("export '{}'", export.name), &q.null_ratio_max)?;
        }

        let merged =
            crate::tuning::merge_tuning_config(self.source.tuning.as_ref(), export.tuning.as_ref());
        if let Some(t) = merged
            && t.batch_size.is_some()
            && t.batch_size_memory_mb.is_some()
        {
            crate::config_bail!(
                crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
                "export '{}': effective tuning has both batch_size and batch_size_memory_mb (mutually exclusive)",
                export.name
            );
        }
        if let Some(et) = &export.tuning
            && et.batch_size.is_some()
            && et.batch_size_memory_mb.is_some()
        {
            crate::config_bail!(
                crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
                "export '{}': tuning.batch_size and tuning.batch_size_memory_mb are mutually exclusive",
                export.name
            );
        }
        Ok(())
    }

    /// Exactly one source selector (`table`/`tables`/`query`/`query_file`), each well-formed.
    fn validate_export_source(&self, export: &ExportConfig) -> crate::error::Result<()> {
        if export.table.is_some() && export.tables.is_some() {
            crate::config_bail!(
                crate::error::codes::CONFIG_EXPORT_SOURCE_INVALID,
                "export '{}': `table:` and `tables:` are mutually exclusive — use \
                 `tables: [a, b]` for a multi-table stream",
                export.name
            );
        }

        let set_count = [
            export.query.is_some(),
            export.query_file.is_some(),
            export.table.is_some(),
            export.tables.is_some(),
        ]
        .iter()
        .filter(|b| **b)
        .count();
        if set_count == 0 {
            crate::config_bail!(
                crate::error::codes::CONFIG_EXPORT_SOURCE_INVALID,
                "export '{}': specify exactly one of 'query', 'query_file', 'table', or 'tables'. \
                 Use table: <name> for a whole table (enables PK auto-chunking); \
                 query: \"SELECT …\" for an inline one-liner; \
                 query_file: <path> for SQL you keep in version control.",
                export.name
            );
        }
        if set_count > 1 {
            crate::config_bail!(
                crate::error::codes::CONFIG_EXPORT_SOURCE_INVALID,
                "export '{}': specify exactly one of 'query', 'query_file', 'table', or 'tables' (got {} set)",
                export.name,
                set_count
            );
        }
        if let Some(file) = &export.query_file {
            export::check_query_file_path(&export.name, file)?;
        }
        Ok(())
    }

    /// Destination endpoint, path and credentials are safe and complete.
    fn validate_export_destination(&self, export: &ExportConfig) -> crate::error::Result<()> {
        if matches!(
            export.destination.destination_type,
            DestinationType::S3 | DestinationType::Gcs | DestinationType::Azure
        ) && let Some(endpoint) = &export.destination.endpoint
        {
            let loopback = url::request_host(endpoint).is_some_and(|host| is_loopback_host(&host));
            if !loopback && !export.destination.allow_anonymous {
                crate::config_bail!(
                    crate::error::codes::CONFIG_DESTINATION_ENDPOINT_UNSAFE,
                    "export '{}': destination.endpoint '{}' points at a non-loopback host. \
                     A custom endpoint redirects every upload there — committing one is a \
                     data-exfiltration / cleartext-credential risk.\n  \
                     Hint: drop `endpoint:` to use the provider default, point it at a \
                     loopback emulator (e.g. http://127.0.0.1:9000 with allow_anonymous: true \
                     for Minio/Azurite), or set `allow_anonymous: true` for an anonymous \
                     emulator.",
                    export.name,
                    endpoint,
                );
            }
        }

        DestinationCredential::choose(&export.destination, &format!("export '{}'", export.name))?;

        Ok(())
    }

    /// Output file size and compression level are in range.
    fn validate_export_output(&self, export: &ExportConfig) -> crate::error::Result<()> {
        if let Some(ref size_str) = export.max_file_size {
            parse_file_size(size_str).map_err(|_| {
                crate::error::coded(
                    crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
                    format!(
                        "export '{}': invalid max_file_size '{}'",
                        export.name, size_str
                    ),
                )
            })?;
        }

        if let Some(level) = export.compression_level {
            match export.compression {
                CompressionType::Zstd => {
                    if !(1..=22).contains(&level) {
                        crate::config_bail!(
                            crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
                            "export '{}': zstd compression_level must be 1..22, got {}",
                            export.name,
                            level
                        );
                    }
                }
                CompressionType::Gzip => {
                    if level > 9 {
                        crate::config_bail!(
                            crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
                            "export '{}': gzip compression_level must be 0..9, got {}",
                            export.name,
                            level
                        );
                    }
                }
                _ => {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
                        "export '{}': compression_level is only supported for zstd and gzip",
                        export.name
                    );
                }
            }
        }
        Ok(())
    }

    /// Each mode's own requirements (and `settle`, an incremental-only knob).
    fn validate_export_mode(&self, export: &ExportConfig) -> crate::error::Result<()> {
        if export.settle.is_some() && export.mode != ExportMode::Incremental {
            crate::config_bail!(
                crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                "export '{}': `settle` requires `mode: incremental` — it bounds the incremental \
                 cursor query and is silently ignored in `mode: {:?}`.\n  \
                 Hint: set `mode: incremental` with a `cursor_column`, or remove `settle`.",
                export.name,
                export.mode,
            );
        }

        match export.mode {
            ExportMode::Incremental => {
                if export.cursor_column.is_none() {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                        "export '{}': incremental mode requires cursor_column",
                        export.name
                    );
                }
                if let Some(settle) = &export.settle {
                    settle.after_secs().map_err(|e| {
                        crate::error::coded(
                            crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
                            format!("export '{}': {e:#}", export.name),
                        )
                    })?;
                    if settle
                        .column
                        .as_deref()
                        .is_some_and(|c| c.trim().is_empty())
                    {
                        crate::config_bail!(
                            crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                            "export '{}': settle.column is empty — name a date/timestamp \
                             column, or omit it to settle on the cursor",
                            export.name
                        );
                    }
                }
                match export.incremental_cursor_mode {
                    IncrementalCursorMode::Coalesce => {
                        if export.cursor_fallback_column.is_none() {
                            crate::config_bail!(
                                crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                                "export '{}': incremental_cursor_mode: coalesce requires cursor_fallback_column",
                                export.name
                            );
                        }
                    }
                    IncrementalCursorMode::SingleColumn => {
                        if export.cursor_fallback_column.is_some() {
                            crate::config_bail!(
                                crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                                "export '{}': cursor_fallback_column is only valid with incremental_cursor_mode: coalesce",
                                export.name
                            );
                        }
                    }
                }
            }
            ExportMode::Chunked => {
                let has_strategy = export.chunk_column.is_some()
                    || export.chunk_by_key.is_some()
                    || export.chunk_count.is_some()
                    || export.chunk_by_days.is_some();
                if !has_strategy && export.table.is_none() {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                        "export '{}': chunked mode needs a chunking strategy. Pick one:\n  \
                         chunk_column: <int col>    range chunks on an integer column (most common)\n  \
                         chunk_by_key: <unique col>  keyset pagination when there's no integer PK\n  \
                         chunk_count: <N>            split the range into N equal chunks\n  \
                         chunk_by_days: <D>          time-bucketed chunks (needs a date/timestamp column)\n  \
                         Or use the `table:` shortcut on a single table — rivet auto-resolves the column from the primary key.",
                        export.name
                    );
                }
                Knob::CHUNK_SIZE.validate_export(&export.name, export.chunk_size)?;
                if export.parallel == 0 {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                        "export '{}': chunked mode requires parallel >= 1 (got 0)",
                        export.name
                    );
                }
                if let Some(count) = export.chunk_count {
                    Knob::CHUNK_COUNT.validate_export(&export.name, count)?;
                }
                if export.chunk_count.is_some() && export.chunk_by_days.is_some() {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                        "export '{}': chunk_count and chunk_by_days are mutually exclusive. \
                         Use chunk_count: N to split an integer range into N chunks; \
                         use chunk_by_days: D to bucket a date/timestamp column by D-day windows.",
                        export.name
                    );
                }
            }
            ExportMode::TimeWindow => {
                if export.time_column.is_none() {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                        "export '{}': time_window mode requires time_column",
                        export.name
                    );
                }
                if export.days_window.is_none() {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                        "export '{}': time_window mode requires days_window",
                        export.name
                    );
                }
            }
            ExportMode::Full => {}
            ExportMode::Cdc => self.validate_export_cdc_mode(export)?,
        }

        Ok(())
    }

    /// Knobs that only apply to another mode are refused, not silently ignored.
    fn validate_export_mode_knobs(&self, export: &ExportConfig) -> crate::error::Result<()> {
        if export.tables.is_some() && export.mode != ExportMode::Cdc {
            crate::config_bail!(
                crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                "export '{}': `tables:` is only valid with `mode: cdc` (batch exports \
                 are one query/table per export)",
                export.name
            );
        }

        if export.mode != ExportMode::Chunked {
            let offending = if export.chunk_column.is_some() {
                Some("chunk_column")
            } else if export.chunk_by_key.is_some() {
                Some("chunk_by_key")
            } else if export.chunk_count.is_some() {
                Some("chunk_count")
            } else if export.chunk_size_memory_mb.is_some() {
                Some("chunk_size_memory_mb")
            } else if export.chunk_max_attempts.is_some() {
                Some("chunk_max_attempts")
            } else if export.chunk_checkpoint {
                Some("chunk_checkpoint")
            } else if export.keyset_incremental {
                Some("keyset_incremental")
            } else if export.chunk_size != default_chunk_size() {
                Some("chunk_size")
            } else {
                None
            };
            if let Some(knob) = offending {
                crate::config_bail!(
                    crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                    "export '{}': `{}` requires `mode: chunked` — it is silently ignored in \
                     `mode: {:?}`, which runs a single unbounded snapshot over the whole table \
                     instead of chunking (the source-pressure footgun chunked mode prevents).\n  \
                     Hint: add `mode: chunked`, or remove the `{}` setting.",
                    export.name,
                    knob,
                    export.mode,
                    knob,
                );
            }
        }

        if export.keyset_incremental && export.chunk_by_key.is_none() {
            crate::config_bail!(
                crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                "export '{}': `keyset_incremental: true` requires `chunk_by_key:` (it continues \
                 from the last exported key) — it has no effect on a range `chunk_column` export.\n  \
                 Hint: set `chunk_by_key: <indexed unique key>`, or remove `keyset_incremental`.",
                export.name,
            );
        }

        self.validate_cdc_block_needs_cdc_mode(export)?;
        Ok(())
    }

    /// Table-qualified `columns:` override keys name a captured table.
    fn validate_export_column_overrides(&self, export: &ExportConfig) -> crate::error::Result<()> {
        {
            for key in export.columns.keys() {
                let Some((tbl, _col)) = key.split_once('.') else {
                    continue;
                };
                if export.query.is_some() || export.query_file.is_some() {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_COLUMN_OVERRIDE_TABLE_UNKNOWN,
                        "export '{}': qualified column override '{key}' needs a table-shaped \
                         export (`table:`/`tables:`), not a query",
                        export.name
                    );
                }
                let bare = |t: &str| t.rsplit('.').next().unwrap_or(t).to_string();
                let captures = export
                    .table
                    .as_deref()
                    .map(bare)
                    .into_iter()
                    .chain(export.tables.iter().flatten().map(|t| bare(t)))
                    .any(|t| t == tbl);
                if !captures {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_COLUMN_OVERRIDE_TABLE_UNKNOWN,
                        "export '{}': column override '{key}' names table '{tbl}', which this \
                         export does not capture — fix the table name or use a bare column key \
                         to apply it to every captured table",
                        export.name
                    );
                }
            }
        }
        Ok(())
    }

    /// `chunk_by_days` is a chunked-mode knob with a positive value.
    fn validate_export_chunk_by_days(&self, export: &ExportConfig) -> crate::error::Result<()> {
        if let Some(days) = export.chunk_by_days {
            if export.mode != ExportMode::Chunked {
                crate::config_bail!(
                    crate::error::codes::CONFIG_MODE_SETTING_INVALID,
                    "export '{}': chunk_by_days requires mode: chunked",
                    export.name
                );
            }
            if days == 0 {
                crate::config_bail!(
                    crate::error::codes::CONFIG_CHUNK_BY_DAYS_INVALID,
                    "export '{}': chunk_by_days must be at least 1",
                    export.name
                );
            }
        }
        Ok(())
    }
}

/// True when a single YAML line carries a mapping value (text after `key:`) that contains a `{` outside of any quotes.
fn line_has_unquoted_brace_value(line: &str) -> bool {
    if line.trim_start().starts_with('#') {
        return false;
    }
    let bytes = line.as_bytes();
    let mut sep = None;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b':' && (i + 1 == bytes.len() || bytes[i + 1].is_ascii_whitespace()) {
            sep = Some(i + 1);
            break;
        }
        i += 1;
    }
    let Some(value_start) = sep else {
        return false;
    };
    let value = line[value_start..].trim_start();
    if value.is_empty() || value.starts_with('#') {
        return false;
    }

    let mut in_single = false;
    let mut in_double = false;
    let vbytes = value.as_bytes();
    for (j, &c) in vbytes.iter().enumerate() {
        match c {
            b'\'' if !in_double => in_single = !in_single,
            b'"' if !in_single => in_double = !in_double,
            b'{' if !in_single && !in_double => {
                if j > 0 && vbytes[j - 1] == b'$' {
                    continue;
                }
                return true;
            }
            _ => {}
        }
    }
    false
}

/// True when `host` names the local machine.
fn is_loopback_host(host: &str) -> bool {
    if host == "localhost" {
        return true;
    }
    let h = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);
    h.parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

/// The longest export or table name: a part file name adds a timestamp and extension and stays under the 255-byte file name limit.
const MAX_NAME_BYTES: usize = 200;

/// True when `name` is filename-safe: rejects path-traversal (`..`), absolute or slash-bearing names (`/`, `\`), a leading `.` (hidden / current-dir), and embedded NULs.
fn is_filename_safe_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_BYTES
        && !name.starts_with('.')
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains("..")
        && !name.contains('\0')
}

/// Why a non-SQL source refuses a mode other than `full` / `cdc` — shared by the loader and `rivet init`.
pub(crate) fn non_sql_mode_refusal(source: SourceType, mode: &str) -> String {
    format!(
        "source type '{source:?}' supports `mode: full` (batch) and `mode: cdc` (change streams) \
         (got `mode: {mode}`). MongoDB has no SQL, so chunked / incremental / keyset / \
         time-window are not available; every document exports as `_id` + a `document` JSON \
         column."
    )
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod audit_csv_compression {
    //! Finding #10: `format: csv` + a compression codec is a silent no-op (the file stays uncompressed but the manifest records the codec).
    use super::*;

    fn yaml(format: &str, compression_line: &str) -> String {
        format!(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: {format}\n\
             {compression_line}    destination:\n      type: local\n      path: ./out\n"
        )
    }

    /// A MongoDB `mode: cdc` export under a ClickHouse `load:` is accepted, as a batch one is (ADR-0035 CH7).
    #[test]
    fn a_mongo_cdc_export_under_a_clickhouse_load_is_accepted_as_a_batch_one_is() {
        let cfg = |mode: &str, extra: &str| {
            format!(
                "source:\n  type: mongo\n  url: \"mongodb://localhost:27018/db\"\n\
                 load: {{ target: clickhouse, url: \"http://localhost:8123\", database: d, \
                 user: u, password_env: CH_PW }}\n\
                 exports:\n  - name: orders\n    table: orders\n    mode: {mode}\n{extra}\
                 \x20   format: parquet\n    destination: {{ type: gcs, bucket: b, prefix: \"p/\" }}\n"
            )
        };
        assert!(
            Config::from_yaml(&cfg("cdc", "    cdc: { checkpoint: ./c.ckpt }\n")).is_ok(),
            "a CDC export loads"
        );
        assert!(
            Config::from_yaml(&cfg("full", "")).is_ok(),
            "a batch export loads"
        );
    }

    /// An Oracle `mode: cdc` export under any `load:` is refused when the config is read.
    #[test]
    fn an_oracle_cdc_export_under_a_load_is_refused_and_batch_is_not() {
        let cfg = |mode: &str, extra: &str| {
            format!(
                "source:\n  type: oracle\n  url: \"oracle://u:p@localhost:1521/FREEPDB1\"\n\
                 load: {{ target: bigquery, project: p, dataset: d }}\n\
                 exports:\n  - name: orders\n    table: orders\n    mode: {mode}\n{extra}\
                 \x20   format: parquet\n    destination: {{ type: gcs, bucket: b, prefix: \"p/\" }}\n"
            )
        };
        let err =
            Config::from_yaml(&cfg("cdc", "    cdc: { checkpoint: ./c.ckpt }\n")).unwrap_err();
        assert!(
            format!("{err:#}").contains(crate::config::load::ORACLE_CDC_NOT_LOADABLE),
            "{err:#}"
        );
        assert!(
            Config::from_yaml(&cfg("full", "")).is_ok(),
            "a batch export loads"
        );
    }

    #[test]
    fn audit_csv_compression_is_rejected() {
        let err = Config::from_yaml(&yaml("csv", "    compression: gzip\n")).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("CSV output does not support compression") && msg.contains("gzip"),
            "csv+gzip must be rejected with an actionable message; got: {msg}"
        );
        assert!(
            msg.contains("parquet") && msg.contains("none"),
            "message must point to the real options (parquet / none); got: {msg}"
        );

        Config::from_yaml(&yaml("parquet", "    compression: gzip\n"))
            .expect("parquet+gzip must validate");
        Config::from_yaml(&yaml("csv", "    compression: none\n")).expect("csv+none must validate");
    }

    #[test]
    fn audit_csv_every_real_codec_is_rejected() {
        for codec in ["zstd", "snappy", "gzip", "lz4"] {
            let err = Config::from_yaml(&yaml("csv", &format!("    compression: {codec}\n")))
                .unwrap_err();
            let msg = format!("{err:#}");
            assert!(
                msg.contains("CSV output does not support compression") && msg.contains(codec),
                "csv+{codec} must be rejected; got: {msg}"
            );
        }
    }

    #[test]
    fn audit_csv_compression_profile_is_rejected() {
        for profile in ["fast", "balanced", "compact"] {
            let err = Config::from_yaml(&yaml(
                "csv",
                &format!("    compression_profile: {profile}\n"),
            ))
            .unwrap_err();
            let msg = format!("{err:#}");
            assert!(
                msg.contains("CSV output does not support compression_profile")
                    && msg.contains(profile),
                "csv+profile {profile} must be rejected; got: {msg}"
            );
        }
        Config::from_yaml(&yaml("csv", "    compression_profile: none\n"))
            .expect("csv + compression_profile: none must validate");
    }

    #[test]
    fn audit_csv_default_compression_still_validates() {
        Config::from_yaml(&yaml("csv", "")).expect("bare format: csv must validate");
    }

    #[test]
    fn audit_compression_supported_predicate() {
        for ct in [
            CompressionType::Zstd,
            CompressionType::Snappy,
            CompressionType::Gzip,
            CompressionType::Lz4,
            CompressionType::None,
        ] {
            assert!(compression_supported(FormatType::Parquet, ct));
        }
        assert!(compression_supported(
            FormatType::Csv,
            CompressionType::None
        ));
        for ct in [
            CompressionType::Zstd,
            CompressionType::Snappy,
            CompressionType::Gzip,
            CompressionType::Lz4,
        ] {
            assert!(
                !compression_supported(FormatType::Csv, ct),
                "CSV must not claim to support {}",
                ct.label()
            );
        }
    }
}

#[cfg(test)]
mod row_hash_config {
    //! A DECLARED `meta_columns.row_hash` names ONE table's columns.
    use super::*;

    fn yaml(export_body: &str) -> String {
        format!(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: t\n    format: parquet\n    destination:\n      type: gcs\n      bucket: b\n      prefix: \"t/\"\n{export_body}"
        )
    }

    #[test]
    fn single_table_declared_set_is_accepted() {
        let cfg = Config::from_yaml(&yaml(
            "    table: orders\n    meta_columns:\n      row_hash: [id, status, updated_at]\n",
        ))
        .expect("a single-table declared row_hash must parse");
        assert_eq!(
            cfg.exports[0].meta_columns.row_hash.declared(),
            Some(["id", "status", "updated_at"].map(String::from).as_slice())
        );
    }

    /// Tables differing only by case share one directory on a case-insensitive local filesystem, so a local multi-table stream refuses them.
    #[test]
    fn a_local_stream_refuses_tables_that_differ_only_by_case() {
        let cfg = |dest: &str| {
            format!(
                "source:\n  type: mysql\n  url: \"mysql://localhost/test\"\n\
                 exports:\n  - name: cdc\n    mode: cdc\n    tables: [HuntQ, other, huntq]\n\
                 \x20   format: parquet\n    cdc:\n      checkpoint: /tmp/ck\n    destination:\n{dest}"
            )
        };
        let err = Config::from_yaml(&cfg("      type: local\n      path: ./out\n"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("'HuntQ' and 'huntq'") && err.contains("letter case"),
            "a local stream must refuse case-colliding tables by name: {err}"
        );
        Config::from_yaml(&cfg(
            "      type: gcs\n      bucket: b\n      prefix: \"t/\"\n",
        ))
        .expect("object keys are case-sensitive, so a cloud stream keeps both tables");
        assert_eq!(case_colliding_table_pair(&["a".into(), "b".into()]), None);
    }

    #[test]
    fn multi_table_stream_refuses_a_declared_set() {
        let err = Config::from_yaml(&yaml(
            "    mode: cdc\n    tables: [orders, customers]\n    cdc:\n      checkpoint: /tmp/ck\n\
             \x20   meta_columns:\n      row_hash: [id, status]\n",
        ))
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("multi-table") && err.contains("row_hash"),
            "a `tables:` stream must be refused at config-load, not mid-stream: {err}"
        );
    }

    /// …but `true` is not a per-table claim, so it must still be allowed.
    #[test]
    fn multi_table_stream_still_accepts_row_hash_true() {
        Config::from_yaml(&yaml(
            "    mode: cdc\n    tables: [orders, customers]\n    cdc:\n      checkpoint: /tmp/ck\n\
             \x20   meta_columns:\n      row_hash: true\n",
        ))
        .expect("`row_hash: true` covers whatever table the row came from");
    }

    #[test]
    fn empty_column_set_is_refused() {
        let err = Config::from_yaml(&yaml(
            "    table: orders\n    meta_columns:\n      row_hash: []\n",
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("attests nothing"), "{err}");
    }
}

#[cfg(test)]
mod reserved_load_extension {
    //! A downstream first-party loader carries its warehouse target in a top-level `load:` block so ONE config drives export + load.
    use super::*;

    fn loaded_yaml(load: &str) -> String {
        format!(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n    destination:\n      type: gcs\n      bucket: b\n      prefix: \"t/\"\n{load}"
        )
    }

    #[test]
    fn a_top_level_load_block_is_typed_when_the_config_is_read() {
        let cfg = Config::from_yaml(&loaded_yaml(
            "load:\n  target: bigquery\n  project: p\n  dataset: d\n  cleanup_source: true\n",
        ))
        .expect("a well-formed `load:` block parses");
        let load = cfg.load.expect("typed");
        assert_eq!(load.target.name(), "bigquery");
        assert!(load.cleanup_source);
    }

    #[test]
    fn a_malformed_load_block_fails_the_config_not_the_load() {
        let err = Config::from_yaml(&loaded_yaml(
            "load:\n  project: p\n  dataset: d\n  cleanup_source: true\n",
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("target"), "a block without a target: {err}");
        let err = Config::from_yaml(&loaded_yaml(
            "load:\n  target: bigquery\n  project: p\n  dataset: d\n  partition: { column: ts, granularity: week }\n",
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("week"), "a bad partition granularity: {err}");
        let err = Config::from_yaml(&loaded_yaml(
            "load:\n  target: bigquery\n  project: p\n  dataset: d\n  gc_orphan: true\n",
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("gc_orphan"), "a typo'd key: {err}");
    }

    #[test]
    fn a_per_export_load_override_needs_the_top_level_block() {
        let yaml = "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n    load: { pk: [id] }\n    destination:\n      type: gcs\n      bucket: b\n      prefix: \"t/\"\n";
        let err = Config::from_yaml(yaml).unwrap_err().to_string();
        assert!(
            err.contains("no top-level `load:` block"),
            "an override with nothing to override: {err}"
        );
    }

    /// `load.tables:` is a per-table layer of a multiplex stream.
    #[test]
    fn per_table_load_overrides_name_captured_tables_of_a_multiplex_stream() {
        let cfg = |export_body: &str| {
            format!(
                "source:\n  type: mysql\n  url: \"mysql://localhost/test\"\nexports:\n{export_body}\
                 load:\n  target: bigquery\n  project: p\n  dataset: d\n"
            )
        };
        let stream = |load: &str| {
            cfg(&format!(
                "  - name: cdc\n    tables: [orders, customers]\n    mode: cdc\n    format: parquet\n    \
                 cdc: {{ checkpoint: ./c.ckpt }}\n    destination: {{ type: gcs, bucket: b, prefix: cdc/ }}\n    \
                 load: {load}\n"
            ))
        };
        Config::from_yaml(&stream("{ tables: { customers: { partition: none } } }"))
            .expect("a captured table may carry its own block");
        let err = Config::from_yaml(&stream("{ tables: { payments: { partition: none } } }"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("does not capture") && err.contains("payments"),
            "{err}"
        );
        let err = Config::from_yaml(&stream(
            "{ tables: { customers: { tables: { orders: { pk: [id] } } } } }",
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("takes no `tables:` of its own"), "{err}");
        let single = cfg(
            "  - name: t\n    table: t\n    mode: full\n    format: parquet\n    \
             destination: { type: gcs, bucket: b, prefix: t/ }\n    load: { tables: { t: { pk: [id] } } }\n",
        );
        let err = Config::from_yaml(&single).unwrap_err().to_string();
        assert!(err.contains("has one unit"), "{err}");
    }

    /// On MongoDB a dotted collection name is LITERAL.
    #[test]
    fn a_mongo_backfill_recipe_must_name_the_collection_exactly() {
        let cfg = |recipe_table: &str| {
            format!(
                "source:\n  type: mongo\n  url: \"mongodb://localhost/app\"\nexports:\n\
                 \x20 - name: base\n    table: {recipe_table}\n    mode: full\n    format: parquet\n    \
                 destination: {{ type: gcs, bucket: b, prefix: base/ }}\n\
                 \x20 - name: stream\n    table: events\n    mode: cdc\n    format: parquet\n    \
                 cdc: {{ backfill: auto, checkpoint: ./c.ckpt }}\n    \
                 destination: {{ type: gcs, bucket: b, prefix: cdc/ }}\n"
            )
        };
        Config::from_yaml(&cfg("events")).expect("the exact collection pairs");
        let err = Config::from_yaml(&cfg("audit.events"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("no export reading table 'events'"), "{err}");
        let err = Config::from_yaml(
            &cfg("public.events")
                .replace("table: events", "table: public.events")
                .replace(
                    "table: public.events\n    mode: full",
                    "table: events\n    mode: full",
                ),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("collection name is literal") && err.contains("public.events"),
            "{err}"
        );
    }

    /// A recipe is skipped by `plan` and `check`, so a bad `columns:` type or table shortcut on it used to surface only when the leg ran.
    #[test]
    fn a_backfill_recipes_read_is_validated_at_config_load() {
        let cfg = |cols: &str| {
            format!(
                "source:\n  type: postgres\n  url: \"postgresql://localhost/app\"\nexports:\n\
                 \x20 - name: base\n    table: orders\n    mode: full\n    format: parquet\n{cols}    \
                 destination: {{ type: gcs, bucket: b, prefix: base/ }}\n\
                 \x20 - name: stream\n    table: orders\n    mode: cdc\n    format: parquet\n    \
                 cdc: {{ backfill: auto }}\n    \
                 destination: {{ type: gcs, bucket: b, prefix: cdc/ }}\n"
            )
        };
        Config::from_yaml(&cfg("    columns: { qty: int32 }\n")).expect("a valid recipe");
        let err = Config::from_yaml(&cfg("    columns: { qty: \"decimal (10, 2)\" }\n"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("base") && err.contains("qty"), "{err}");
    }

    /// `backfill:` with no value is a typed key with nothing said.
    #[test]
    fn an_empty_backfill_key_is_refused_not_read_as_absent() {
        let yaml = "source:\n  type: postgres\n  url: \"postgresql://localhost/app\"\nexports:\n\
                    \x20 - name: stream\n    table: orders\n    mode: cdc\n    format: parquet\n    \
                    cdc:\n      backfill:\n    \
                    destination: { type: gcs, bucket: b, prefix: cdc/ }\n";
        let err = Config::from_yaml(yaml).unwrap_err().to_string();
        assert!(
            err.contains("`cdc.backfill` must be `auto` or a list"),
            "{err}"
        );
        let err = Config::from_yaml(&yaml.replace("backfill:\n", "backfill: atuo\n"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`cdc.backfill` must be `auto` or a list"),
            "{err}"
        );
    }

    /// Two captured tables with one LEAF (`sales.orders`, `archive.orders`) route fine.
    #[test]
    fn two_captured_tables_with_one_leaf_cannot_carry_recipe_column_types() {
        let cfg = |sales_cols: &str| {
            format!(
                "source:\n  type: postgres\n  url: \"postgresql://localhost/app\"\nexports:\n\
                 \x20 - name: sales\n    table: sales.orders\n    mode: full\n    format: parquet\n{sales_cols}    \
                 destination: {{ type: gcs, bucket: b, prefix: sales/ }}\n\
                 \x20 - name: archive\n    table: archive.orders\n    mode: full\n    format: parquet\n    \
                 destination: {{ type: gcs, bucket: b, prefix: archive/ }}\n\
                 \x20 - name: stream\n    tables: [sales.orders, archive.orders]\n    mode: cdc\n    format: parquet\n    \
                 cdc: {{ backfill: auto }}\n    destination: {{ type: gcs, bucket: b, prefix: cdc/ }}\n"
            )
        };
        Config::from_yaml(&cfg("")).expect("untyped recipes over two same-leaf tables pair fine");
        let err = Config::from_yaml(&cfg("    columns: { amount: \"decimal(18,2)\" }\n"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("share the leaf name")
                && err.contains("sales.orders")
                && err.contains("archive.orders"),
            "{err}"
        );
    }

    /// A column typed one way by the recipe and another by the stream is refused at CONFIG LOAD.
    #[test]
    fn a_recipe_and_its_stream_typing_one_column_apart_is_refused_at_load() {
        let cfg = |stream_cols: &str| {
            format!(
                "source:\n  type: postgres\n  url: \"postgresql://localhost/app\"\nexports:\n\
                 \x20 - name: orders_baseline\n    table: orders\n    mode: full\n    format: parquet\n    \
                 columns: {{ amount: \"decimal(18,2)\" }}\n    \
                 destination: {{ type: gcs, bucket: b, prefix: base/ }}\n\
                 \x20 - name: stream\n    tables: [orders]\n    mode: cdc\n    format: parquet\n{stream_cols}    \
                 cdc: {{ backfill: auto }}\n    destination: {{ type: gcs, bucket: b, prefix: cdc/ }}\n"
            )
        };
        Config::from_yaml(&cfg("")).expect("a recipe typing alone is fine");
        Config::from_yaml(&cfg(
            "    columns: { \"orders.amount\": \"numeric(18,2)\" }\n",
        ))
        .expect("the same type in another spelling is no conflict");
        let err = Config::from_yaml(&cfg(
            "    columns: { \"orders.amount\": \"decimal(11,4)\" }\n",
        ))
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("one column cannot have two types") && err.contains("'amount'"),
            "{err}"
        );
    }
}

#[cfg(test)]
mod audit_unquoted_template_brace {
    //! yaml-hint: an unquoted `{partition}` (or `{date}`) in a path/prefix value trips serde_yaml_ng's flow-mapping parser with a cryptic message that gives no clue the brace needs quoting.
    use super::*;

    /// A full, otherwise-valid config whose `prefix:` value is whatever the caller passes verbatim (quoted or not).
    fn yaml_with_prefix(prefix_value: &str) -> String {
        format!(
            "source:\n\
             \x20 type: postgres\n\
             \x20 url: \"postgresql://localhost/test\"\n\
             exports:\n\
             \x20 - name: t\n\
             \x20   query: \"SELECT 1\"\n\
             \x20   format: parquet\n\
             \x20   partition_by: created_date\n\
             \x20   destination:\n\
             \x20     type: local\n\
             \x20     path: ./out\n\
             \x20     prefix: {prefix_value}\n"
        )
    }

    const HINT_FRAGMENT: &str =
        "a YAML value containing { } (such as {partition} or {date}) must be quoted";

    /// Every combination the partition_by doc names as incompatible is refused at load, and the doc names each refusal.
    #[test]
    fn partition_by_doc_names_every_combination_config_load_refuses() {
        let schema = serde_json::to_value(schemars::schema_for!(crate::config::ExportConfig))
            .unwrap()["properties"]["partition_by"]["description"]
            .as_str()
            .unwrap()
            .replace('\n', " ");
        let base = |extra: &str, top: &str| {
            format!(
                "source: {{ type: postgres, url: \"postgresql://u:p@h/d\" }}\n\
                 exports:\n  - name: t1\n    table: t1\n    format: parquet\n    \
                 partition_by: created_at\n    destination: {{ type: local, path: \"./out/{{partition}}/\" }}\n\
                 {extra}{top}"
            )
        };
        let cases = [
            (
                "`mode: time_window`",
                base(
                    "    mode: time_window\n    time_column: created_at\n    days_window: 1\n",
                    "",
                ),
            ),
            ("`mode: cdc`", base("    mode: cdc\n", "")),
            ("`chunk_by_key`", base("    chunk_by_key: id\n", "")),
            (
                "a `load:` block (per-export or top-level)",
                base("", "load: { target: bigquery, project: p, dataset: d }\n"),
            ),
        ];
        assert!(
            Config::from_yaml(&base("", "")).is_ok(),
            "the plain form must load"
        );
        for (token, yaml) in cases {
            assert!(schema.contains(token), "partition_by doc must name {token}");
            let err = format!("{:#}", Config::from_yaml(&yaml).unwrap_err());
            assert!(err.contains("partition_by"), "{token}: {err}");
        }
        assert!(
            schema.contains("or a MongoDB source"),
            "partition_by doc must name the MongoDB refusal"
        );
    }

    #[test]
    fn top_level_load_with_partition_by_is_rejected() {
        let yaml = r#"
source: { type: postgres, url: "postgresql://rivet:rivet@127.0.0.1:5432/rivet" }
exports:
  - name: t1
    query: "SELECT 1 as id"
    format: parquet
    partition_by: id
    destination: { type: local, path: "./out/{partition}/" }
load: { target: bigquery, project: p, dataset: d }
"#;
        let err = Config::from_yaml(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("partition_by is not compatible with a `load:` block"),
            "a top-level `load:` + partition_by must be rejected: {err}"
        );
        assert!(
            err.to_string().ends_with(
                "To load it, remove `partition_by:` from this export; to keep the partitioned \
                 layout, remove the `load:` block — the export then stays Parquet-only, since \
                 `rivet load` has no per-partition mode."
            ),
            "the remedy must name only what works: {err}"
        );

        let ok_yaml = r#"
source: { type: postgres, url: "postgresql://rivet:rivet@127.0.0.1:5432/rivet" }
exports:
  - name: t1
    query: "SELECT 1 as id"
    format: parquet
    partition_by: id
    destination: { type: local, path: "./out/{partition}/" }
"#;
        assert!(
            Config::from_yaml(ok_yaml).is_ok(),
            "partition_by without any load: must stay accepted"
        );
    }

    #[test]
    fn bare_partition_token_gets_quoting_hint() {
        let err = Config::from_yaml(&yaml_with_prefix("{partition}")).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains(HINT_FRAGMENT),
            "bare {{partition}} must carry the quoting hint; got: {msg}"
        );
        assert!(
            msg.contains("invalid type: map") || msg.contains("line"),
            "the original parser error must be kept; got: {msg}"
        );
    }

    #[test]
    fn trailing_text_after_brace_gets_quoting_hint() {
        let err = Config::from_yaml(&yaml_with_prefix("{date}/{partition}/")).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains(HINT_FRAGMENT),
            "{{date}}/{{partition}}/ must carry the quoting hint; got: {msg}"
        );
    }

    #[test]
    fn unclosed_brace_gets_quoting_hint() {
        let err = Config::from_yaml(&yaml_with_prefix("{partition")).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains(HINT_FRAGMENT),
            "unclosed brace must carry the quoting hint; got: {msg}"
        );
    }

    #[test]
    fn quoted_brace_value_loads_ok() {
        let cfg = Config::from_yaml(&yaml_with_prefix("\"exports/{partition}/\""))
            .expect("quoted {partition} prefix must load");
        assert_eq!(
            cfg.exports[0].destination.prefix.as_deref(),
            Some("exports/{partition}/")
        );
    }

    #[test]
    fn config_without_braces_is_untouched() {
        let yaml = "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
                    exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n\
                    \x20   destination:\n      type: local\n      path: ./out\n      prefix: exports/data/\n";
        Config::from_yaml(yaml).expect("a brace-free prefix must load");
    }

    #[test]
    fn unquoted_brace_value_is_detected() {
        assert!(line_has_unquoted_brace_value("    prefix: {partition}"));
        assert!(line_has_unquoted_brace_value("      path: {date}/out"));
        assert!(line_has_unquoted_brace_value("prefix: {partition"));
    }

    #[test]
    fn quoted_brace_value_is_not_flagged() {
        assert!(!line_has_unquoted_brace_value(
            "    prefix: \"exports/{partition}/\""
        ));
        assert!(!line_has_unquoted_brace_value("    prefix: 'data/{date}/'"));
    }

    #[test]
    fn env_placeholder_and_plain_values_are_not_flagged() {
        assert!(!line_has_unquoted_brace_value("    url: ${DATABASE_URL}"));
        assert!(!line_has_unquoted_brace_value("    path: ./out"));
        assert!(!line_has_unquoted_brace_value("  # prefix: {partition}"));
        assert!(!line_has_unquoted_brace_value("    prefix:"));
    }
}

#[cfg(test)]
mod sec_config_validation_regression {
    //! Regression edge-cases that pin the *compat boundaries* of the config-validation security fixes.
    use super::*;

    /// A full, otherwise-valid config whose single export's `destination:` block is whatever the caller passes verbatim.
    fn yaml_with_destination(dest_block: &str) -> String {
        format!(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n\
             {dest_block}"
        )
    }

    #[test]
    fn request_host_parses_forms() {
        let host = |url: &str| url::request_host(url);
        assert_eq!(
            host("https://attacker.example.com").as_deref(),
            Some("attacker.example.com")
        );
        assert_eq!(
            host("http://127.0.0.1:10000/devstoreaccount1").as_deref(),
            Some("127.0.0.1")
        );
        assert_eq!(
            host("http://user:pass@127.0.0.1:9000").as_deref(),
            Some("127.0.0.1")
        );
        assert_eq!(host("http://[::1]:9000").as_deref(), Some("::1"));
        assert_eq!(host("not-a-url"), None);
        assert_eq!(host("://nohost"), None);
    }

    #[test]
    fn loopback_host_classification() {
        for h in ["127.0.0.1", "127.0.0.53", "localhost", "::1"] {
            assert!(is_loopback_host(h), "{h} must be loopback");
        }
        for h in ["attacker.example.com", "evil.com", "10.0.0.1", "::2"] {
            assert!(!is_loopback_host(h), "{h} must be remote");
        }
    }

    #[test]
    fn loopback_endpoint_without_allow_anonymous_still_accepted() {
        let cfg = yaml_with_destination(
            "    destination:\n      type: s3\n      bucket: b\n      region: us-east-1\n\
             \x20     endpoint: http://127.0.0.1:9000\n      access_key_env: AK\n      secret_key_env: SK\n",
        );
        Config::from_yaml(&cfg).expect("loopback endpoint with creds must stay accepted");
    }

    #[test]
    fn remote_https_endpoint_with_allow_anonymous_is_the_only_remote_escape() {
        let cfg = yaml_with_destination(
            "    destination:\n      type: gcs\n      bucket: b\n\
             \x20     endpoint: https://emulator.example.com\n      allow_anonymous: true\n",
        );
        Config::from_yaml(&cfg).expect("remote endpoint + allow_anonymous opt-in must be accepted");
    }

    #[test]
    fn absolute_local_path_is_allowed() {
        let cfg =
            yaml_with_destination("    destination:\n      type: local\n      path: /output\n");
        Config::from_yaml(&cfg).expect("absolute local path (Docker mount) must validate");
    }

    #[test]
    fn dotdot_in_local_prefix_is_rejected() {
        let cfg = yaml_with_destination(
            "    destination:\n      type: local\n      path: ./out\n      prefix: a/../b\n",
        );
        let err = Config::from_yaml(&cfg).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("prefix") && msg.contains(".."),
            "a '..' in the local prefix must be rejected naming prefix/..; got: {msg}"
        );
    }

    #[test]
    fn tls_danger_knob_without_explicit_mode_still_accepted() {
        let yaml = "source:\n  type: mssql\n  url: \"sqlserver://sa:pw@127.0.0.1:1433/db\"\n  \
                    tls:\n    accept_invalid_certs: true\n\
                    exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n    \
                    destination:\n      type: local\n      path: ./out\n";
        Config::from_yaml(yaml)
            .expect("dev-container default-mode + accept_invalid_certs must stay accepted");
    }

    #[test]
    fn tls_explicit_verify_ca_plus_invalid_hostnames_rejected() {
        let yaml = "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n  \
                    tls:\n    mode: verify-ca\n    accept_invalid_hostnames: true\n\
                    exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n    \
                    destination:\n      type: local\n      path: ./out\n";
        let err = Config::from_yaml(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("accept_invalid_hostnames") && msg.contains("verify-ca"),
            "explicit verify-ca + accept_invalid_hostnames must be rejected; got: {msg}"
        );
    }

    #[test]
    fn tls_explicit_disable_with_knob_is_not_flagged() {
        let yaml = "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n  \
                    tls:\n    mode: disable\n    accept_invalid_certs: true\n\
                    exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n    \
                    destination:\n      type: local\n      path: ./out\n";
        Config::from_yaml(yaml).expect("mode: disable + knob is a no-op and must validate");
    }

    #[test]
    fn filename_safe_name_boundaries() {
        for ok in ["t", "orders", "daily_events", "v2-2024", "name.with.dots"] {
            assert!(is_filename_safe_name(ok), "{ok:?} must be accepted");
        }
        for bad in [
            "",
            "..",
            "../x",
            "/abs",
            "sub/dir",
            "back\\slash",
            ".hidden",
            "with\u{0000}nul",
        ] {
            assert!(!is_filename_safe_name(bad), "{bad:?} must be rejected");
        }
    }
}

#[cfg(test)]
mod sec_config_validation {
    //! RED security tests for config-load validation gaps (cluster: config-validation).
    use super::*;

    /// A full, otherwise-valid config whose single export's `destination:` block is whatever the caller passes verbatim.
    fn yaml_with_destination(dest_block: &str) -> String {
        format!(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n\
             {dest_block}"
        )
    }

    #[test]
    fn sec_s3_custom_endpoint_rejected() {
        let cfg = yaml_with_destination(
            "    destination:\n      type: s3\n      bucket: my-bucket\n      region: us-east-1\n\
             \x20     endpoint: https://attacker.example.com\n",
        );
        let res = Config::from_yaml(&cfg);
        assert!(
            res.is_err(),
            "a non-loopback custom S3 endpoint (https://attacker.example.com) must be \
             rejected at config-load (data-exfiltration target); got Ok"
        );
        let msg = format!("{:#}", res.unwrap_err());
        assert!(
            msg.contains("endpoint"),
            "rejection must name the offending 'endpoint' field; got: {msg}"
        );
    }

    #[test]
    fn sec_http_endpoint_rejected() {
        let cfg = yaml_with_destination(
            "    destination:\n      type: s3\n      bucket: my-bucket\n      region: us-east-1\n\
             \x20     endpoint: http://evil.com\n",
        );
        let res = Config::from_yaml(&cfg);
        assert!(
            res.is_err(),
            "a plaintext http:// endpoint to a remote host (http://evil.com) must be \
             rejected at config-load (cleartext credentials + data); got Ok"
        );
        let msg = format!("{:#}", res.unwrap_err());
        assert!(
            msg.contains("endpoint") || msg.to_lowercase().contains("http"),
            "rejection must name the endpoint / cleartext problem; got: {msg}"
        );
    }

    #[test]
    fn a_backslash_or_tab_does_not_make_a_remote_endpoint_loopback() {
        for endpoint in [
            r#""http://evil.com\\@127.0.0.1:9000""#,
            r#""ht\ttp://evil.com\\@127.0.0.1:9000""#,
            r#""http://192.168.1.5:80\\@127.0.0.1:9000""#,
            r#""http://192.168.1.5:pw\\@127.0.0.1:9000""#,
        ] {
            let cfg = yaml_with_destination(&format!(
                "    destination:\n      type: s3\n      bucket: my-bucket\n      region: us-east-1\n\
                 \x20     endpoint: {endpoint}\n"
            ));
            let e = Config::from_yaml(&cfg).expect_err(endpoint);
            assert_eq!(
                crate::error::error_code(&e),
                Some("RIVET_CONFIG_DESTINATION_ENDPOINT_UNSAFE"),
                "{endpoint}: {e:#}"
            );
        }
    }

    #[test]
    fn sec_loopback_endpoint_still_accepted_guard() {
        let cfg = yaml_with_destination(
            "    destination:\n      type: s3\n      bucket: my-bucket\n      region: us-east-1\n\
             \x20     endpoint: http://127.0.0.1:9000\n      allow_anonymous: true\n",
        );
        Config::from_yaml(&cfg)
            .expect("a loopback emulator endpoint with allow_anonymous must stay accepted");
    }

    #[test]
    fn sec_export_name_traversal_rejected() {
        for bad in ["../../../etc/x", "/abs/x", "sub/dir", "with\u{0000}nul"] {
            let name_yaml = serde_json::to_string(bad).expect("encode name");
            let cfg = format!(
                "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
                 exports:\n  - name: {name_yaml}\n    query: \"SELECT 1\"\n    format: parquet\n\
                 \x20   destination:\n      type: local\n      path: ./out\n"
            );
            let res = Config::from_yaml(&cfg);
            assert!(
                res.is_err(),
                "export name {bad:?} (traversal/absolute/slash/NUL) must be rejected at \
                 config-load; got Ok"
            );
            let msg = format!("{:#}", res.unwrap_err());
            assert!(
                msg.contains("name"),
                "rejection of name {bad:?} must name the offending 'name' field; got: {msg}"
            );
        }
    }

    #[test]
    fn sec_export_name_normal_still_accepted_guard() {
        let cfg = yaml_with_destination("    destination:\n      type: local\n      path: ./out\n");
        Config::from_yaml(&cfg).expect("a normal export name ('t') must stay accepted");
    }

    #[test]
    fn sec_local_dest_path_traversal_rejected() {
        let cfg = yaml_with_destination(
            "    destination:\n      type: local\n      path: ../../../../tmp/x\n",
        );
        let res = Config::from_yaml(&cfg);
        assert!(
            res.is_err(),
            "a local destination path containing '..' (../../../../tmp/x) must be rejected \
             at config-load (writes outside the output tree); got Ok"
        );
        let msg = format!("{:#}", res.unwrap_err());
        assert!(
            msg.contains("path") || msg.contains(".."),
            "rejection must name the offending 'path' / traversal; got: {msg}"
        );
    }

    #[test]
    fn sec_accept_invalid_certs_warns() {
        let cfg = yaml_with_destination("    destination:\n      type: local\n      path: ./out\n");
        let cfg = cfg.replace(
            "  url: \"postgresql://localhost/test\"\n",
            "  url: \"postgresql://localhost/test\"\n  tls:\n    mode: verify-full\n    accept_invalid_certs: true\n",
        );
        let res = Config::from_yaml(&cfg);
        assert!(
            res.is_err(),
            "tls mode: verify-full with accept_invalid_certs: true is a silent security \
             downgrade and must be loudly surfaced (error) at config-load; got Ok"
        );
        let msg = format!("{:#}", res.unwrap_err());
        assert!(
            msg.contains("accept_invalid_certs") || msg.to_lowercase().contains("verify"),
            "the surfaced error must name the dangerous knob / mode contradiction; got: {msg}"
        );
    }

    /// `rollover: 0` and `rollover_memory_mb: 0` are REFUSED at validation.
    #[test]
    fn a_traversal_or_scheme_prefix_is_refused_at_validate() {
        for (prefix, needle) in [
            ("exports/../escape/", ".."),
            ("gs://bh6-bucket/inner/", "URI scheme"),
        ] {
            let yaml = format!(
                "source:\n  type: postgres\n  url: postgres://x/x\nexports:\n  - name: e1\n    \
                 query: SELECT 1\n    destination:\n      type: gcs\n      bucket: b\n      \
                 prefix: \"{prefix}\"\n    format: parquet\n"
            );
            let err = Config::from_yaml(&yaml).expect_err(prefix).to_string();
            assert!(err.contains(needle), "{prefix}: {err}");
        }
        let ok = "source:\n  type: postgres\n  url: postgres://x/x\nexports:\n  - name: e1\n    \
                  query: SELECT 1\n    destination:\n      type: gcs\n      bucket: b\n      \
                  prefix: exports/v1.2/\n    format: parquet\n";
        Config::from_yaml(ok).expect("a version-dotted prefix is not a traversal");
    }

    #[test]
    fn a_csv_export_under_a_load_block_is_refused_loudly() {
        let base = r#"
source:
  type: postgres
  url: postgresql://u:p@127.0.0.1/db
LOAD_BLOCK
exports:
  - name: t
    table: public.t
    format: FORMAT
    destination: {type: local, path: /tmp/out/}
"#;
        let cfg = base
            .replace(
                "LOAD_BLOCK",
                "load: {target: bigquery, project: p, dataset: d}",
            )
            .replace("FORMAT", "csv");
        let err = Config::from_yaml(&cfg).expect_err("csv + load").to_string();
        assert!(
            err.contains("cannot feed a `load:` block"),
            "must name the interaction and the fix: {err}"
        );
        Config::from_yaml(
            &base
                .replace(
                    "LOAD_BLOCK",
                    "load: {target: bigquery, project: p, dataset: d}",
                )
                .replace("FORMAT", "parquet"),
        )
        .expect("parquet + load must validate");
        Config::from_yaml(&base.replace("LOAD_BLOCK", "").replace("FORMAT", "csv"))
            .expect("csv without load must validate");
    }

    /// Two CDC checkpoints that RESOLVE to one path are one conflict, whatever their spelling.
    #[test]
    fn equivalent_checkpoint_spellings_are_one_conflict() {
        let cfg = r#"
source:
  type: mssql
  url: sqlserver://u:p@127.0.0.1/db
exports:
  - name: a
    table: dbo.a
    mode: cdc
    format: parquet
    cdc: {capture_instance: dbo_a, checkpoint: "./cdc/x.ckpt"}
    destination: {type: local, path: /tmp/a/}
  - name: b
    table: dbo.b
    mode: cdc
    format: parquet
    cdc: {capture_instance: dbo_b, checkpoint: "cdc/x.ckpt"}
    destination: {type: local, path: /tmp/b/}
"#;
        let err = Config::from_yaml(cfg)
            .expect_err("one file, two spellings")
            .to_string();
        assert!(
            err.contains("same checkpoint path"),
            "the guard must compare RESOLVED paths, as its own doc promises: {err}"
        );
    }

    #[test]
    fn cdc_rollover_zero_is_refused_not_a_file_explosion() {
        let base = r#"
source:
  type: postgres
  url: postgresql://u:p@127.0.0.1/db
exports:
  - name: t
    table: public.t
    mode: cdc
    format: parquet
    cdc:
      ROLLOVER_LINE
    destination: {type: local, path: /tmp/out/}
"#;
        for (line, needle) in [
            ("rollover: 0", "rollover must be >= 1"),
            ("rollover_memory_mb: 0", "rollover_memory_mb must be >= 1"),
            ("rollover_memory_mb: 999999999999999999", "overflows"),
            ("rollover_memory_mb: 18014398509481984", "overflows"),
            ("rollover_memory_mb: 17592186044416", "overflows"),
        ] {
            let yaml = base.replace("ROLLOVER_LINE", line);
            let err = Config::from_yaml(&yaml).expect_err(line).to_string();
            assert!(
                err.contains(needle),
                "`{line}` must be refused with a message naming the fix — got: {err}"
            );
        }
    }
}

/// Every field of [`crate::tuning::TuningConfig`], for the misplaced-field detector.
pub(crate) const TUNING_FIELD_NAMES: &[&str] = &[
    "profile",
    "batch_size",
    "batch_size_memory_mb",
    "throttle_ms",
    "statement_timeout_s",
    "max_retries",
    "retry_backoff_ms",
    "lock_timeout_s",
    "memory_threshold_mb",
    "max_batch_memory_mb",
    "on_batch_memory_exceeded",
    "adaptive",
    "min_parallel",
    "max_value_mb",
];
