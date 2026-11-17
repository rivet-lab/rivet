use super::HealthVerdict;
use crate::config::quantity::Knob;
use crate::config::{ExportConfig, ExportMode};

/// B1: Human-readable strategy name derived from mode + config.
pub(crate) fn derive_strategy(export: &ExportConfig) -> String {
    derive_strategy_resolved(export, None)
}

/// As [`derive_strategy`] but told the engine-introspected single-int PK the planner would auto-resolve an unset chunked `chunk_column` to (`build_plan`).
pub(crate) fn derive_strategy_resolved(export: &ExportConfig, auto_pk: Option<&str>) -> String {
    if let Some(key) = export.chunk_by_key.as_ref() {
        if export.parallel > 1 {
            return format!(
                "keyset-parallel({}, size={}, p={})",
                key, export.chunk_size, export.parallel
            );
        }
        return format!("keyset({}, size={})", key, export.chunk_size);
    }
    match export.mode {
        ExportMode::Full => {
            if export.parallel > 1 {
                format!("full-parallel({})", export.parallel)
            } else {
                "full-scan".to_string()
            }
        }
        ExportMode::Incremental => {
            let col = export.cursor_column.as_deref().unwrap_or("?");
            format!("incremental({})", col)
        }
        ExportMode::Chunked => {
            let col = export
                .chunk_column
                .as_deref()
                .or(auto_resolved_chunk_pk(export, auto_pk))
                .unwrap_or("?");
            if let Some(days) = export.chunk_by_days {
                if export.parallel > 1 {
                    format!(
                        "date-chunked-parallel({}, {}d, p={})",
                        col, days, export.parallel
                    )
                } else {
                    format!("date-chunked({}, {}d)", col, days)
                }
            } else if export.parallel > 1 {
                format!(
                    "chunked-parallel({}, size={}, p={})",
                    col, export.chunk_size, export.parallel
                )
            } else {
                format!("chunked({}, size={})", col, export.chunk_size)
            }
        }
        ExportMode::TimeWindow => {
            let col = export.time_column.as_deref().unwrap_or("?");
            let days = export.days_window.unwrap_or(0);
            format!("time-window({}, {}d)", col, days)
        }
        ExportMode::Cdc => "cdc".to_string(),
    }
}

/// The column the run actually SEEKS / RANGES / WINDOWS on.
pub(crate) fn preflight_range_col(export: &ExportConfig) -> Option<&str> {
    export
        .chunk_column
        .as_deref()
        .or(export.chunk_key_lead())
        .or(export.cursor_column.as_deref())
        .or(export.time_column.as_deref())
}

/// [`preflight_range_col`] plus the planner's AUTO-RESOLUTION.
pub(crate) fn preflight_range_col_resolved<'a>(
    export: &'a ExportConfig,
    auto_pk: Option<&'a str>,
) -> Option<&'a str> {
    preflight_range_col(export).or(auto_resolved_chunk_pk(export, auto_pk))
}

/// The single-int PK a chunked export auto-resolves to.
fn auto_resolved_chunk_pk<'a>(
    export: &'a ExportConfig,
    auto_pk: Option<&'a str>,
) -> Option<&'a str> {
    if should_auto_resolve_chunk_pk(export) {
        auto_pk
    } else {
        None
    }
}

/// Does `build_plan` auto-resolve this export's chunk column to the single-integer PK?
pub(crate) fn should_auto_resolve_chunk_pk(export: &ExportConfig) -> bool {
    export.mode == ExportMode::Chunked
        && export.chunk_column.is_none()
        && export.chunk_by_key.is_none()
}

/// Does this strategy read on an indexed key the catalog probe should confirm?
pub(crate) fn strategy_probes_index(export: &ExportConfig) -> bool {
    matches!(
        export.mode,
        ExportMode::Chunked | ExportMode::Incremental | ExportMode::TimeWindow
    ) || export.chunk_by_key.is_some()
}

/// The base relation the catalog probes key on, for the PG/MySQL diagnostics.
pub(crate) fn preflight_base_table<'a>(
    export: &'a ExportConfig,
    base_query: &'a str,
) -> Option<std::borrow::Cow<'a, str>> {
    export
        .table
        .as_deref()
        .map(std::borrow::Cow::Borrowed)
        .or_else(|| super::postgres::table_from_simple_query(base_query))
}

/// Whether a single-table query returns every row of its table (no filter, grouping, set, join or row cap).
pub(crate) fn reads_every_row(query: &str) -> bool {
    let q = format!(
        " {} ",
        query
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    );
    ![
        " where ",
        " group by ",
        " having ",
        " union ",
        " intersect ",
        " except ",
        " minus ",
        " join ",
        " distinct ",
        " top ",
        " top(",
        " offset ",
        " fetch ",
        " connect by ",
        " sample",
        " tablesample",
        " partition (",
        " partition(",
        " subpartition",
        " apply ",
        " pivot ",
        " pivot(",
        " unpivot",
        " as of ",
        " for system_time",
        "@",
    ]
    .iter()
    .any(|kw| q.contains(kw))
}

/// `base_table` when the query reads all of it, so catalog stats and bare-table MIN/MAX describe the export.
pub(crate) fn whole_table_relation<'a>(
    base_table: Option<&'a str>,
    base_query: &str,
) -> Option<&'a str> {
    base_table.filter(|_| reads_every_row(base_query))
}

/// The table an engine runs its single-int-PK probe against.
pub(crate) fn auto_pk_probe_target<'a>(
    export: &ExportConfig,
    base_table: Option<&'a str>,
) -> Option<&'a str> {
    if should_auto_resolve_chunk_pk(export) {
        base_table
    } else {
        None
    }
}

/// The `(table, column)` an engine runs its leading-key index probe against.
pub(crate) fn index_probe_target<'a>(
    export: &'a ExportConfig,
    auto_pk: Option<&'a str>,
    base_table: Option<&'a str>,
) -> Option<(&'a str, &'a str)> {
    if !strategy_probes_index(export) {
        return None;
    }
    let col = preflight_range_col_resolved(export, auto_pk)?;
    Some((base_table?, col))
}

/// #149: measured beats declared — and the report SAYS which it stands on.
pub(crate) const ACTUALS_STALE_DAYS: i64 = 14;

pub(crate) fn choose_row_estimate(
    catalog: Option<i64>,
    measured: Option<(i64, chrono::DateTime<chrono::Utc>)>,
    now: chrono::DateTime<chrono::Utc>,
) -> (Option<i64>, String) {
    match (catalog, measured) {
        (_, Some((rows, at))) => {
            let stale = (now - at).num_days() > ACTUALS_STALE_DAYS;
            let diverges = catalog.is_some_and(|c| {
                let (lo, hi) = (rows.min(c), rows.max(c));
                lo > 0 && hi / lo >= 2 || (lo == 0 && hi > 0)
            });
            if stale && diverges {
                (
                    catalog,
                    format!(
                        "catalog estimate — a measured figure from {} was set aside as stale \
                         (>{}d old) and >2× divergent",
                        at.format("%Y-%m-%d"),
                        ACTUALS_STALE_DAYS
                    ),
                )
            } else {
                (Some(rows), format!("measured {}", at.format("%Y-%m-%d")))
            }
        }
        (c, None) => (c, "catalog estimate".to_string()),
    }
}

/// Overlay the state store's freshest successful actual onto a diagnostic.
fn measured_this_source(
    m: &crate::state::ExportMetric,
    export: &ExportConfig,
    source: crate::config::SourceType,
    state: &crate::state::StateStore,
) -> bool {
    if m.source_type.as_deref() != Some(source.ledger_label().as_str()) {
        return false;
    }
    let want = crate::manifest::destination_uri_for_manifest(&export.destination);
    match m.run_id.as_deref().map(|id| state.run_prefix_of(id)) {
        Some(Ok(Some(prefix))) => prefix == want,
        _ => true,
    }
}

pub(crate) fn overlay_measured_rows(
    diag: &mut super::ExportDiagnostic,
    export: &ExportConfig,
    source: crate::config::SourceType,
    state: &crate::state::StateStore,
) {
    if !is_whole_table_mode(&diag.mode) {
        diag.row_source = Some("catalog estimate".to_string());
        return;
    }
    let measured = state
        .get_metrics(Some(&diag.export_name), 25)
        .ok()
        .and_then(|ms| {
            ms.into_iter()
                .find(|m| m.status == "success" && measured_this_source(m, export, source, state))
                .and_then(|m| {
                    chrono::DateTime::parse_from_rfc3339(&m.run_at)
                        .ok()
                        .map(|t| (m.total_rows, t.with_timezone(&chrono::Utc)))
                })
        });
    let before = diag.row_estimate;
    let (est, label) = choose_row_estimate(diag.row_estimate, measured, chrono::Utc::now());
    diag.row_estimate = est;
    diag.row_source = Some(label);

    if est != before && est.is_some() {
        diag.verdict = export_verdict(export, est, diag.uses_index, diag.avg_row_bytes);
        diag.recommended_profile = recommend_profile(est, diag.uses_index, export);
        diag.recommended_parallel = recommend_parallelism(export, est, diag.uses_index);
        if diag.suggestion.is_some() {
            diag.suggestion = build_suggestion(&diag.verdict, est, diag.uses_index, export);
        }
        diag.warnings = collect_warnings(
            export,
            est,
            diag.avg_row_bytes,
            diag.chunk_min.as_deref(),
            diag.chunk_max.as_deref(),
            diag.db_max_connections,
        );
    }
}

/// Does this diagnostic's mode read the WHOLE table every run (so a measured `total_rows` IS the table size)?
pub(crate) fn is_whole_table_mode(mode: &str) -> bool {
    mode == "full" || mode.starts_with("chunked (column")
}

/// The operator-facing mode line shown in the `rivet check` diagnostic.
pub(crate) fn diagnose_mode_str(export: &ExportConfig) -> String {
    if let Some(key) = export.chunk_by_key.as_ref() {
        if export.parallel > 1 {
            return format!(
                "keyset (key: {}, size: {}, parallel: {})",
                key, export.chunk_size, export.parallel
            );
        }
        return format!("keyset (key: {}, size: {})", key, export.chunk_size);
    }
    match export.mode {
        ExportMode::Full => "full".to_string(),
        ExportMode::Incremental => format!(
            "incremental (cursor: {})",
            export.cursor_column.as_deref().unwrap_or("?")
        ),
        ExportMode::Chunked => format!(
            "chunked (column: {}, size: {})",
            export.chunk_column.as_deref().unwrap_or("?"),
            export.chunk_size
        ),
        ExportMode::TimeWindow => format!(
            "time_window (column: {}, days: {})",
            export.time_column.as_deref().unwrap_or("?"),
            export.days_window.unwrap_or(0)
        ),
        ExportMode::Cdc => "cdc (change data capture)".to_string(),
    }
}

/// Resolve the base query the runner will issue, for the preflight probes.
pub(crate) fn resolve_preflight_base_query(export: &ExportConfig) -> String {
    match export.resolve_query(std::path::Path::new(""), None) {
        Ok(q) => q,
        Err(e) => {
            log::debug!(
                "preflight: base-query resolution failed for export '{}': {e}",
                export.name
            );
            export
                .query
                .clone()
                .unwrap_or_else(|| "SELECT 1".to_string())
        }
    }
}

/// B2: Recommend tuning profile based on row estimate and index usage.
pub(crate) fn recommend_profile(
    row_estimate: Option<i64>,
    uses_index: bool,
    export: &ExportConfig,
) -> &'static str {
    let rows = row_estimate.unwrap_or(0);
    match (uses_index, rows) {
        (true, r) if r <= 1_000_000 => "fast",
        (true, r) if r <= 10_000_000 => "balanced",
        (true, _) => "safe",
        (false, r) if r <= 100_000 => {
            if export.parallel > 1 {
                "safe"
            } else {
                "balanced"
            }
        }
        (false, r) if r <= 1_000_000 => "balanced",
        (false, _) => "safe",
    }
}

/// Key-range sparsity for chunked `BETWEEN` windows.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChunkSparsityInfo {
    pub is_sparse: bool,
    pub density: f64,
    pub range_span: i64,
    pub logical_windows: i64,
    pub row_count: i64,
}

pub(crate) fn chunk_sparsity_from_counts(
    row_count: i64,
    min_i: i64,
    max_i: i64,
    chunk_size: usize,
) -> ChunkSparsityInfo {
    let range_span = Knob::range_span(min_i, max_i);
    let density = row_count as f64 / range_span as f64;
    let logical_windows = Knob::logical_windows(range_span, chunk_size);
    let is_sparse = row_count > 0 && density < 0.1 && logical_windows > 10;
    ChunkSparsityInfo {
        is_sparse,
        density,
        range_span,
        logical_windows,
        row_count,
    }
}

/// B3: Detect sparse range risk for chunked mode.
pub(crate) fn check_sparse_range(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    cursor_min: Option<&str>,
    cursor_max: Option<&str>,
) -> Option<String> {
    if export.mode != ExportMode::Chunked {
        return None;
    }
    if export.chunk_by_key.is_some() {
        return None;
    }
    if export.chunk_by_days.is_some() {
        return None;
    }
    if export.chunk_count.is_some() {
        return None;
    }
    let rows = row_estimate.unwrap_or(0);
    if rows == 0 {
        return None;
    }

    let (min_val, max_val) = match (cursor_min, cursor_max) {
        (Some(a), Some(b)) => (a, b),
        _ => return None,
    };

    let min_i: i64 = min_val.parse().ok()?;
    let max_i: i64 = max_val.parse().ok()?;
    let info = chunk_sparsity_from_counts(rows, min_i, max_i, export.chunk_size);
    if !info.is_sparse {
        return None;
    }

    let empty_pct = ((1.0 - info.density).clamp(0.0, 1.0) * 100.0) as u32;
    Some(format!(
        "Sparse key range: ~{}% of chunk windows will be empty (range {}..{}, ~{} rows). \
         Switch to keyset pagination (`chunk_by_key: <unique key>`) — it pages by ROWS and is \
         immune to a sparse/gappy key — or use `chunk_count: N`, or incremental mode.",
        empty_pct, min_val, max_val, rows
    ))
}

/// A single chunk query scanning more than this holds one statement.
const MAX_CHUNK_SCAN_BYTES: i64 = 256 * 1024 * 1024;

/// B3b (dual of [`check_sparse_range`]): warn when `chunk_size` makes a single chunk query scan *too many* bytes.
pub(crate) fn check_oversized_chunk(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    avg_row_bytes: Option<i64>,
    cursor_min: Option<&str>,
    cursor_max: Option<&str>,
) -> Option<String> {
    if export.mode != ExportMode::Chunked || export.chunk_by_days.is_some() {
        return None;
    }
    let rows = row_estimate?;
    let bytes_per_row = avg_row_bytes?;
    if rows <= 0 || bytes_per_row <= 0 || export.chunk_size == 0 {
        return None;
    }

    let rows_per_chunk: i64 = if export.chunk_by_key.is_some() {
        Knob::saturating_i64(export.chunk_size)
    } else {
        let min_i: i64 = cursor_min?.parse().ok()?;
        let max_i: i64 = cursor_max?.parse().ok()?;
        let info = chunk_sparsity_from_counts(rows, min_i, max_i, export.chunk_size);
        (info.density * export.chunk_size as f64).round() as i64
    };
    let rows_per_chunk = rows_per_chunk.clamp(1, rows);

    let bytes_per_chunk = rows_per_chunk.saturating_mul(bytes_per_row);
    if bytes_per_chunk <= MAX_CHUNK_SCAN_BYTES {
        return None;
    }

    let suggested = Knob::saturating_i64(export.chunk_size)
        .saturating_mul(MAX_CHUNK_SCAN_BYTES)
        .saturating_div(bytes_per_chunk)
        .max(1);
    let mb = |b: i64| b / (1024 * 1024);
    Some(format!(
        "Heavy chunk: chunk_size={} makes each chunk query scan ~{} MB (~{} rows × ~{} B/row), \
         holding one statement — and its snapshot/locks — open that long (the source-harm a \
         smaller chunk avoids). Consider chunk_size around {} to keep each chunk under ~{} MB.",
        export.chunk_size,
        mb(bytes_per_chunk),
        rows_per_chunk,
        bytes_per_row,
        suggested,
        mb(MAX_CHUNK_SCAN_BYTES),
    ))
}

/// B4: Warn about dense surrogate sort cost when query uses ROW_NUMBER.
pub(crate) fn check_dense_surrogate_cost(export: &ExportConfig) -> Option<String> {
    let query = export.query.as_deref().unwrap_or("");
    let q_upper = query.to_uppercase();
    if export.mode == ExportMode::Chunked && q_upper.contains("ROW_NUMBER") {
        Some(
            "Dense surrogate (ROW_NUMBER) requires a global sort -- this adds CPU and I/O cost \
             proportional to the full result set. For very large or hot tables, consider \
             incremental mode on an indexed cursor column, or a precomputed dense key."
                .to_string(),
        )
    } else {
        None
    }
}

/// Connections reserved for non-worker use (monitoring, admin, failover).
const CONNECTION_HEADROOM: u32 = 3;

/// Warn when configured parallelism meets or exceeds the DB's max_connections limit.
pub(crate) fn check_connection_limit(
    parallel: usize,
    db_max_connections: Option<u32>,
) -> Option<String> {
    if parallel <= 1 {
        return None;
    }
    match db_max_connections {
        None => Some(
            "Could not fetch DB max_connections — connection limit check skipped. \
             Ask your DBA to verify your user has access to DB server variables, \
             then verify your parallel setting manually."
                .to_string(),
        ),
        Some(max_conn) if parallel as u32 >= max_conn => Some(format!(
            "parallel={parallel} meets or exceeds DB max_connections={max_conn} — \
             workers will compete for connections and some may fail. \
             Reduce parallel to at most {} (leave headroom for other connections).",
            max_conn.saturating_sub(CONNECTION_HEADROOM).max(1),
        )),
        _ => None,
    }
}

/// B5: Warn about parallel memory risk.
pub(crate) fn check_parallel_memory_risk(
    export: &ExportConfig,
    row_estimate: Option<i64>,
) -> Option<String> {
    if export.parallel <= 1 {
        return None;
    }
    let rows = row_estimate.unwrap_or(0);
    if rows > 5_000_000 {
        Some(format!(
            "Parallel={} on ~{}M rows: each worker buffers batch_size rows in memory. \
             With wide rows this can cause high RSS. Monitor with memory_threshold_mb \
             or reduce parallel/batch_size.",
            export.parallel,
            rows / 1_000_000,
        ))
    } else {
        None
    }
}

/// L6: Recommend parallelism level.
pub(crate) fn recommend_parallelism(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    uses_index: bool,
) -> (u32, &'static str) {
    if export.chunk_by_key.is_some() {
        return match export.parallel {
            n if n > 1 => (
                n as u32,
                "keyset fans row-percentile-range workers — each seeks a disjoint slice",
            ),
            _ => (
                1,
                "keyset is sequential by default — set `parallel: N` to fan out N seek workers",
            ),
        };
    }
    if export.mode != ExportMode::Chunked {
        return (1, "only chunked mode benefits from parallelism");
    }

    let rows = row_estimate.unwrap_or(0);

    if rows < 50_000 {
        return (1, "dataset too small to benefit from parallelism");
    }

    if !uses_index && rows > 5_000_000 {
        return (1, "no index — parallel scans would multiply source load");
    }

    if !uses_index {
        return (
            2,
            "no index — conservative parallelism to limit source impact",
        );
    }

    match rows {
        r if r < 500_000 => (2, "moderate dataset — 2 workers sufficient"),
        r if r < 5_000_000 => (4, "large dataset with index support"),
        _ => (
            4,
            "very large dataset — cap at 4 to control memory; increase with memory_threshold_mb monitoring",
        ),
    }
}

/// Collect all warnings (B3-B6) for an export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Severity {
    Low,
    Medium,
    High,
}

impl Severity {
    /// Upper-case label for the text render (`[HIGH]`).
    pub(crate) fn label(self) -> &'static str {
        match self {
            Severity::Low => "LOW",
            Severity::Medium => "MEDIUM",
            Severity::High => "HIGH",
        }
    }
}

/// A preflight `check` warning with its severity.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct Warning {
    pub severity: Severity,
    pub message: String,
}

impl Warning {
    pub(crate) fn new(severity: Severity, message: String) -> Self {
        Self { severity, message }
    }
}

/// A chunked/keyset export with no `chunk_checkpoint.
pub(crate) fn check_missing_checkpoint(
    export: &ExportConfig,
    row_estimate: Option<i64>,
) -> Option<String> {
    let chunked_or_keyset = export.mode == ExportMode::Chunked || export.chunk_by_key.is_some();
    if !chunked_or_keyset || export.chunk_checkpoint || export.keyset_incremental {
        return None;
    }
    if row_estimate.is_some_and(|r| r < SMALL_TABLE_ROW_THRESHOLD) {
        return None;
    }
    Some(
        "No chunk_checkpoint: a crashed or interrupted run re-reads the whole table \
         from the start (no resume). Add `chunk_checkpoint: true` for crash-recovery — \
         safe to enable on any table: it does NOT change your clean re-run semantics (a \
         clean re-run still does a full pass and picks up updates). Recommended for large \
         tables or unreliable links. (Keyset append-only incremental is the separate \
         `keyset_incremental` opt-in.)"
            .to_string(),
    )
}

pub(super) fn collect_warnings(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    avg_row_bytes: Option<i64>,
    chunk_min: Option<&str>,
    chunk_max: Option<&str>,
    db_max_connections: Option<u32>,
) -> Vec<Warning> {
    [
        check_connection_limit(export.parallel, db_max_connections)
            .map(|m| Warning::new(Severity::High, m)),
        check_sparse_range(export, row_estimate, chunk_min, chunk_max)
            .map(|m| Warning::new(Severity::Medium, m)),
        check_oversized_chunk(export, row_estimate, avg_row_bytes, chunk_min, chunk_max)
            .map(|m| Warning::new(Severity::Medium, m)),
        check_dense_surrogate_cost(export).map(|m| Warning::new(Severity::Low, m)),
        check_parallel_memory_risk(export, row_estimate).map(|m| Warning::new(Severity::High, m)),
        check_missing_checkpoint(export, row_estimate).map(|m| Warning::new(Severity::Medium, m)),
    ]
    .into_iter()
    .flatten()
    .collect()
}

pub(crate) use crate::plan::explain::SMALL_TABLE_ROW_THRESHOLD;

/// The verdict for `export`: the table-scan rules of [`compute_verdict`], except a `mode.
pub(crate) fn export_verdict(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    uses_index: bool,
    avg_row_bytes: Option<i64>,
) -> HealthVerdict {
    if export.mode == ExportMode::Cdc {
        return HealthVerdict::Efficient;
    }
    compute_verdict(
        row_estimate,
        uses_index,
        export.cursor_column.is_some(),
        avg_row_bytes,
        export.parallel,
    )
}

pub(crate) fn compute_verdict(
    row_estimate: Option<i64>,
    uses_index: bool,
    has_cursor: bool,
    avg_row_bytes: Option<i64>,
    parallel: usize,
) -> HealthVerdict {
    let rows = row_estimate.unwrap_or(0);

    match (uses_index, has_cursor, rows) {
        (true, true, r) if r <= 10_000_000 => HealthVerdict::Efficient,
        (true, true, _) => HealthVerdict::Acceptable,
        (true, false, _) => HealthVerdict::Acceptable,
        (false, _, r) if row_estimate.is_some() && r < SMALL_TABLE_ROW_THRESHOLD => {
            HealthVerdict::Acceptable
        }
        (false, _, r) if r <= 1_000_000 => HealthVerdict::Degraded,
        (false, true, r) if r <= 50_000_000 => HealthVerdict::Degraded,
        (false, _, _) => match avg_row_bytes {
            Some(b)
                if crate::tuning::memory::estimate_peak_rss_mb(parallel.max(1), b)
                    <= crate::tuning::memory::DEFAULT_MEM_BUDGET_MB =>
            {
                HealthVerdict::Degraded
            }
            _ => HealthVerdict::Unsafe,
        },
    }
}

pub(crate) fn build_suggestion(
    verdict: &HealthVerdict,
    row_estimate: Option<i64>,
    uses_index: bool,
    export: &ExportConfig,
) -> Option<String> {
    let rows = row_estimate.unwrap_or(0);

    match verdict {
        HealthVerdict::Efficient => None,
        HealthVerdict::Acceptable => {
            if rows > 10_000_000 {
                let mut msg = format!("Large dataset (~{}M rows).", rows / 1_000_000);
                match export.mode {
                    ExportMode::Full => {
                        msg.push_str(" Switch to incremental mode with an indexed cursor column to avoid re-reading unchanged rows.");
                    }
                    ExportMode::Chunked if export.parallel <= 1 => {
                        msg.push_str(" Add parallel > 1 to speed up chunked extraction.");
                    }
                    _ => {
                        msg.push_str(" Use 'safe' tuning profile to limit database impact.");
                    }
                }
                Some(msg)
            } else {
                None
            }
        }
        HealthVerdict::Degraded => {
            let mut parts = Vec::new();
            if !uses_index {
                parts.push("No index detected -- full table scan.".to_string());
            }
            match export.mode {
                ExportMode::Full if export.cursor_column.is_none() => {
                    parts.push(
                        "Add an indexed cursor column and switch to incremental mode.".to_string(),
                    );
                }
                ExportMode::Chunked if !uses_index => {
                    let col = export
                        .chunk_column
                        .as_deref()
                        .or(export.chunk_key_lead())
                        .unwrap_or("chunk_column");
                    parts.push(format!(
                        "Create an index on '{}' to speed up range scans.",
                        col
                    ));
                }
                ExportMode::TimeWindow if !uses_index => {
                    let col = export.time_column.as_deref().unwrap_or("time_column");
                    parts.push(format!(
                        "Create an index on '{}' for efficient time-window filtering.",
                        col
                    ));
                }
                _ => {
                    if export.cursor_column.is_none() && !matches!(export.mode, ExportMode::Chunked)
                    {
                        parts.push(
                            "Consider adding a cursor column for incremental mode.".to_string(),
                        );
                    }
                }
            }
            parts.push("Use 'safe' tuning profile to limit database impact.".to_string());
            Some(parts.join(" "))
        }
        HealthVerdict::Unsafe => {
            let mut parts = vec![format!(
                "~{}M row scan without index support.",
                rows / 1_000_000
            )];
            match export.mode {
                ExportMode::Full => {
                    parts.push("Add an indexed cursor column and use incremental mode to avoid full re-reads.".to_string());
                }
                ExportMode::Chunked => {
                    let col = export
                        .chunk_column
                        .as_deref()
                        .or(export.chunk_key_lead())
                        .unwrap_or("chunk_column");
                    let fix = if export.chunk_by_key.is_some() {
                        "Create an index on '{col}' (keyset needs a unique index), or use mode: full for one snapshot scan."
                    } else {
                        "Create an index on '{col}'. Consider reducing chunk_size or adding parallel workers."
                    };
                    parts.push(fix.replace("{col}", col));
                }
                ExportMode::TimeWindow => {
                    let col = export.time_column.as_deref().unwrap_or("time_column");
                    parts.push(format!(
                        "Create an index on '{}'. Reduce days_window if possible.",
                        col
                    ));
                }
                ExportMode::Incremental => {
                    let col = export.cursor_column.as_deref().unwrap_or("cursor_column");
                    parts.push(format!("Create an index on '{}'.", col));
                }
                ExportMode::Cdc => {}
            }
            parts.push("Use 'safe' tuning profile. Extract during off-peak hours.".to_string());
            Some(parts.join(" "))
        }
    }
}

/// What one engine's catalog/plan probes ANSWERED about an export.
pub(crate) struct ProbeFacts {
    /// The single-integer PK the planner would auto-resolve an unset chunked `chunk_column` to (`build_plan`).
    pub auto_pk: Option<String>,
    /// Scan-free row estimate (PG EXPLAIN `rows=`, MySQL EXPLAIN `rows`, MSSQL `sys.partitions`).
    pub row_estimate: Option<i64>,
    /// Average bytes per row, where the engine has a figure worth trusting.
    pub avg_row_bytes: Option<i64>,
    /// MIN/MAX of the incremental key expression or the range column, as text.
    pub range_min: Option<String>,
    pub range_max: Option<String>,
    /// Human-readable access-path description from the query plan (`Seq Scan`, `range using PRIMARY`).
    pub scan_type: Option<String>,
    /// Did the plan for the BASE query use an index?
    pub plan_uses_index: bool,
    /// Is the read column the leading key of an index on the base table, per the CATALOG?
    pub catalog_index: Option<bool>,
    /// The server's configured connection cap, for the parallelism check.
    pub db_max_connections: Option<u32>,
}

/// Turn one engine's [`ProbeFacts`] into the export's [`ExportDiagnostic`].
pub(crate) fn assemble_diagnostic(
    export: &ExportConfig,
    facts: ProbeFacts,
) -> super::ExportDiagnostic {
    let ProbeFacts {
        auto_pk,
        row_estimate,
        avg_row_bytes,
        range_min,
        range_max,
        scan_type,
        plan_uses_index,
        catalog_index,
        db_max_connections,
    } = facts;

    let uses_index = match catalog_index {
        Some(true) => true,
        Some(false) | None => plan_uses_index,
    };

    let strategy = derive_strategy_resolved(export, auto_pk.as_deref());
    let verdict = export_verdict(export, row_estimate, uses_index, avg_row_bytes);
    let recommended_profile = recommend_profile(row_estimate, uses_index, export);
    let recommended_parallel = recommend_parallelism(export, row_estimate, uses_index);
    let warnings = collect_warnings(
        export,
        row_estimate,
        avg_row_bytes,
        range_min.as_deref(),
        range_max.as_deref(),
        db_max_connections,
    );
    let suggestion = build_suggestion(&verdict, row_estimate, uses_index, export);

    super::ExportDiagnostic {
        row_source: None,
        export_name: export.name.clone(),
        strategy,
        mode: diagnose_mode_str(export),
        cursor_column: export.cursor_column.clone(),
        row_estimate,
        avg_row_bytes,
        cursor_min: range_min.clone(),
        cursor_max: range_max.clone(),
        scan_type,
        uses_index,
        verdict,
        recommended_profile,
        recommended_parallel,
        warnings,
        suggestion,
        chunk_min: range_min,
        chunk_max: range_max,
        db_max_connections,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn only_a_whole_table_read_may_borrow_the_tables_stats() {
        use crate::preflight::table_from_simple_query;
        let whole = |q: &str| {
            let t = table_from_simple_query(q).map(|c| c.into_owned());
            whole_table_relation(t.as_deref(), q).map(str::to_string)
        };
        for q in [
            "SELECT * FROM dbo.orders",
            "SELECT id, amount FROM dbo.orders",
            "SELECT [id], [amount]\nFROM [dbo].[orders]",
            "SELECT \"ID\", \"NAME\"\nFROM \"RIVET\".\"ORDERS\"",
        ] {
            assert!(whole(q).is_some(), "reads every row: {q}");
        }
        for q in [
            "SELECT id, amount FROM dbo.orders WHERE tenant_id = 7",
            "SELECT TOP 100 * FROM dbo.orders",
            "SELECT TOP(100) id FROM dbo.orders",
            "SELECT DISTINCT id FROM dbo.orders",
            "SELECT id FROM dbo.orders ORDER BY id OFFSET 10 ROWS",
            "SELECT a FROM t FETCH FIRST 5 ROWS ONLY",
            "SELECT id FROM dbo.orders TABLESAMPLE (10 PERCENT)",
            "SELECT id, count(*) FROM dbo.orders GROUP BY id",
            "SELECT * FROM sales PARTITION (p2024)",
            "SELECT * FROM sales SUBPARTITION (sp1)",
            "SELECT o.* FROM dbo.orders o CROSS APPLY dbo.f(o.id) x",
            "SELECT o.* FROM dbo.orders o OUTER APPLY dbo.f(o.id) x",
            "SELECT * FROM sales AS OF TIMESTAMP SYSTIMESTAMP",
            "SELECT * FROM dbo.orders FOR SYSTEM_TIME ALL",
            "SELECT * FROM sales@remote_db",
        ] {
            assert!(
                table_from_simple_query(q).is_some(),
                "probe relation still resolves: {q}"
            );
            assert_eq!(whole(q), None, "a subset must not borrow table stats: {q}");
        }
    }

    /// Round-10: the runtime sparse warning names `chunk_count.
    #[test]
    fn a_chunk_count_config_is_exempt_from_the_sparse_alarm() {
        let mut e = crate::config::sample_export("sparse");
        e.mode = crate::config::ExportMode::Chunked;
        e.chunk_column = Some("id".into());
        e.chunk_size = 100_000;
        assert!(
            check_sparse_range(&e, Some(1000), Some("1"), Some("1000000000")).is_some(),
            "fixture must be sparse without the knob, or this test grades nothing"
        );
        e.chunk_count = Some(8);
        assert_eq!(
            check_sparse_range(&e, Some(1000), Some("1"), Some("1000000000")),
            None,
            "the operator chose the window count — nothing to warn about"
        );
    }

    use super::*;
    use crate::config::ExportConfig;

    fn cfg(extra_yaml: &str) -> ExportConfig {
        let yaml = format!(
            "name: test\nformat: parquet\ndestination:\n  type: local\n  path: /tmp\n{extra_yaml}"
        );
        serde_yaml_ng::from_str(&yaml).expect("parse ExportConfig")
    }

    #[test]
    fn resolved_range_col_and_strategy_model_the_auto_chunk_pk() {
        let e = cfg("mode: chunked\ntable: orders\n");
        assert_eq!(preflight_range_col(&e), None);
        assert_eq!(
            derive_strategy(&e),
            format!("chunked(?, size={})", e.chunk_size)
        );
        assert_eq!(preflight_range_col_resolved(&e, Some("id")), Some("id"));
        assert_eq!(
            derive_strategy_resolved(&e, Some("id")),
            format!("chunked(id, size={})", e.chunk_size)
        );
        let e2 = cfg("mode: chunked\ntable: orders\nchunk_column: created_at\n");
        assert_eq!(
            preflight_range_col_resolved(&e2, Some("id")),
            Some("created_at")
        );
        let e3 = cfg("mode: incremental\ncursor_column: updated_at\n");
        assert_eq!(
            preflight_range_col_resolved(&e3, Some("id")),
            Some("updated_at")
        );
    }

    #[test]
    fn derive_strategy_full_scan() {
        assert_eq!(derive_strategy(&cfg("mode: full\n")), "full-scan");
    }

    #[test]
    fn derive_strategy_full_parallel() {
        let e = cfg("mode: full\nparallel: 4\n");
        assert_eq!(derive_strategy(&e), "full-parallel(4)");
    }

    #[test]
    fn derive_strategy_incremental() {
        let e = cfg("mode: incremental\ncursor_column: updated_at\n");
        assert_eq!(derive_strategy(&e), "incremental(updated_at)");
    }

    #[test]
    fn derive_strategy_chunked_by_size() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 50000\n");
        assert_eq!(derive_strategy(&e), "chunked(id, size=50000)");
    }

    #[test]
    fn derive_strategy_date_chunked() {
        let e = cfg("mode: chunked\nchunk_column: created_at\nchunk_by_days: 7\n");
        assert_eq!(derive_strategy(&e), "date-chunked(created_at, 7d)");
    }

    #[test]
    fn choose_row_estimate_rule_both_directions() {
        use chrono::{Duration, Utc};
        let now = Utc::now();
        let fresh = Some((835_000_000i64, now - Duration::days(2)));
        let stale = Some((835_000_000i64, now - Duration::days(30)));
        let (est, src) = choose_row_estimate(Some(332_000_000), fresh, now);
        assert_eq!(est, Some(835_000_000));
        assert!(src.starts_with("measured "), "{src}");
        let (est, src) = choose_row_estimate(Some(332_000_000), stale, now);
        assert_eq!(est, Some(332_000_000));
        assert!(src.contains("set aside as stale"), "{src}");
        let (est, src) = choose_row_estimate(Some(800_000_000), stale, now);
        assert_eq!(est, Some(835_000_000));
        assert!(src.starts_with("measured "), "{src}");
        let (est, src) = choose_row_estimate(Some(100), None, now);
        assert_eq!(est, Some(100));
        assert_eq!(src, "catalog estimate");
    }

    fn scope_diag() -> super::super::ExportDiagnostic {
        super::super::ExportDiagnostic {
            export_name: "rivet_type_matrix".into(),
            strategy: "full-scan".into(),
            mode: "full".into(),
            cursor_column: None,
            row_estimate: None,
            row_source: None,
            avg_row_bytes: None,
            cursor_min: None,
            cursor_max: None,
            scan_type: None,
            uses_index: false,
            verdict: super::super::HealthVerdict::Degraded,
            warnings: Vec::new(),
            recommended_profile: "balanced",
            recommended_parallel: (1, "test"),
            suggestion: None,
            chunk_min: None,
            chunk_max: None,
            db_max_connections: None,
        }
    }

    fn scope_state(engine: &str, run_prefix: Option<&str>) -> crate::state::StateStore {
        let state = crate::state::StateStore::open_in_memory().unwrap();
        state
            .record_metric_full(&crate::state::MetricRow {
                export_name: "rivet_type_matrix".into(),
                run_id: "foreign".into(),
                total_rows: 4,
                status: "success".into(),
                source_type: Some(engine.into()),
                ..Default::default()
            })
            .unwrap();
        if let Some(prefix) = run_prefix {
            state
                .begin_run(
                    "foreign",
                    "rivet_type_matrix",
                    prefix,
                    "2026-09-23T14:00:00Z",
                )
                .unwrap();
        }
        state
    }

    /// A same-named run of ANOTHER engine on a shared state store is not this export's measurement.
    #[test]
    fn overlay_ignores_a_same_named_run_of_another_engine() {
        let export = cfg("table: rivet_type_matrix\nmode: full\n");
        let mut diag = scope_diag();
        overlay_measured_rows(
            &mut diag,
            &export,
            crate::config::SourceType::Mssql,
            &scope_state("postgres", None),
        );
        assert_eq!(
            diag.row_estimate, None,
            "a postgres run must not size an mssql table: {:?}",
            diag.row_source
        );
    }

    /// A same-engine run the ledger records under ANOTHER destination is another config's measurement.
    #[test]
    fn overlay_ignores_a_same_named_run_into_another_destination() {
        let export = cfg("table: rivet_type_matrix\nmode: full\n");
        let mut diag = scope_diag();
        let state = scope_state("mssql", Some("file:///elsewhere/rivet_type_matrix/"));
        overlay_measured_rows(&mut diag, &export, crate::config::SourceType::Mssql, &state);
        assert_eq!(diag.row_estimate, None, "{:?}", diag.row_source);
    }

    /// The same engine into the same destination is this export's own measurement.
    #[test]
    fn overlay_uses_the_run_of_the_same_engine_and_destination() {
        let export = cfg("table: rivet_type_matrix\nmode: full\n");
        let want = crate::manifest::destination_uri_for_manifest(&export.destination);
        let mut diag = scope_diag();
        overlay_measured_rows(
            &mut diag,
            &export,
            crate::config::SourceType::Mssql,
            &scope_state("mssql", Some(&want)),
        );
        assert_eq!(diag.row_estimate, Some(4));
    }

    /// The overlay through a REAL state store (RED against a catalog-only path).
    #[test]
    fn overlay_swaps_catalog_for_the_recorded_actual_and_says_so() {
        let state = crate::state::StateStore::open_in_memory().unwrap();
        state
            .record_metric_full(&crate::state::MetricRow {
                source_type: Some("postgres".into()),
                export_name: "versioned_giant".into(),
                run_id: "r1".into(),
                total_rows: 835_700_000,
                status: "success".into(),
                ..Default::default()
            })
            .unwrap();
        let mut diag = super::super::ExportDiagnostic {
            export_name: "versioned_giant".into(),
            strategy: "chunked(id, size=100000)".into(),
            mode: "chunked (column: id, size: 100000)".into(),
            cursor_column: None,
            row_estimate: Some(331_966_196),
            row_source: None,
            avg_row_bytes: None,
            cursor_min: None,
            cursor_max: None,
            scan_type: None,
            uses_index: true,
            verdict: super::super::HealthVerdict::Efficient,
            warnings: Vec::new(),
            recommended_profile: "balanced",
            recommended_parallel: (1, "test"),
            suggestion: None,
            chunk_min: None,
            chunk_max: None,
            db_max_connections: None,
        };
        let export = cfg("table: t\nmode: chunked\nchunk_column: id\nchunk_size: 100000\n");
        diag.recommended_profile = "fast";
        overlay_measured_rows(
            &mut diag,
            &export,
            crate::config::SourceType::Postgres,
            &state,
        );
        assert_eq!(diag.row_estimate, Some(835_700_000), "measured must win");
        assert_ne!(
            diag.recommended_profile, "fast",
            "verdict/profile must be recomputed from the measured estimate, not the catalog one"
        );
        assert!(
            diag.row_source
                .as_deref()
                .unwrap_or("")
                .starts_with("measured "),
            "the report must SAY it stands on the measurement: {:?}",
            diag.row_source
        );
    }

    /// #202 (roast 2026-08-10): the overlay recomputes verdict/profile but left diag.warnings on the CATALOG estimate.
    #[test]
    fn overlay_recomputes_row_scaled_warnings_from_the_measure() {
        let state = crate::state::StateStore::open_in_memory().unwrap();
        state
            .record_metric_full(&crate::state::MetricRow {
                source_type: Some("postgres".into()),
                export_name: "big_chunked".into(),
                run_id: "r1".into(),
                total_rows: 10_000_000,
                status: "success".into(),
                ..Default::default()
            })
            .unwrap();
        let export =
            cfg("table: t\nmode: chunked\nchunk_column: id\nchunk_size: 100000\nparallel: 4\n");
        let mut diag = super::super::ExportDiagnostic {
            export_name: "big_chunked".into(),
            strategy: "chunked(id, size=100000)".into(),
            mode: "chunked (column: id, size: 100000)".into(),
            cursor_column: None,
            row_estimate: Some(1000),
            row_source: None,
            avg_row_bytes: None,
            cursor_min: None,
            cursor_max: None,
            scan_type: None,
            uses_index: true,
            verdict: super::super::HealthVerdict::Efficient,
            warnings: Vec::new(),
            recommended_profile: "fast",
            recommended_parallel: (4, "test"),
            suggestion: None,
            chunk_min: None,
            chunk_max: None,
            db_max_connections: None,
        };
        overlay_measured_rows(
            &mut diag,
            &export,
            crate::config::SourceType::Postgres,
            &state,
        );
        assert_eq!(diag.row_estimate, Some(10_000_000));
        assert!(
            diag.warnings
                .iter()
                .any(|w| w.message.contains("Parallel=4")),
            "the parallel-memory-risk warning must be recomputed from the measured 10M rows: {:?}",
            diag.warnings.iter().map(|w| &w.message).collect::<Vec<_>>()
        );
    }

    /// #1 (roast 2026-08-10): Mongo emits suggestion.
    #[test]
    fn overlay_keeps_a_none_suggestion_none_for_mongo() {
        let state = crate::state::StateStore::open_in_memory().unwrap();
        state
            .record_metric_full(&crate::state::MetricRow {
                source_type: Some("postgres".into()),
                export_name: "mongo_coll".into(),
                run_id: "r1".into(),
                total_rows: 12_000_000,
                status: "success".into(),
                ..Default::default()
            })
            .unwrap();
        let mut diag = super::super::ExportDiagnostic {
            export_name: "mongo_coll".into(),
            strategy: "full".into(),
            mode: "full".into(),
            cursor_column: None,
            row_estimate: Some(5_000_000),
            row_source: None,
            avg_row_bytes: None,
            cursor_min: None,
            cursor_max: None,
            scan_type: None,
            uses_index: false,
            verdict: super::super::HealthVerdict::Degraded,
            warnings: Vec::new(),
            recommended_profile: "safe",
            recommended_parallel: (1, "test"),
            suggestion: None,
            chunk_min: None,
            chunk_max: None,
            db_max_connections: None,
        };
        let export = cfg("table: coll\nmode: full\n");
        overlay_measured_rows(
            &mut diag,
            &export,
            crate::config::SourceType::Postgres,
            &state,
        );
        assert_eq!(
            diag.row_estimate,
            Some(12_000_000),
            "measured overlay applied"
        );
        assert_eq!(
            diag.suggestion, None,
            "a None suggestion (Mongo) must stay None through the recompute"
        );
    }

    /// #149 mode-blindness (roast 2026-08-09): an INCREMENTAL run's total_rows is the per-run DELTA, not the table.
    #[test]
    fn overlay_does_not_measure_an_incremental_delta_as_the_table() {
        assert!(is_whole_table_mode("full"));
        assert!(is_whole_table_mode("chunked (column: id, size: 100000)"));
        assert!(!is_whole_table_mode("incremental (cursor: updated_at)"));
        assert!(!is_whole_table_mode("keyset (key: id, size: 50000)"));

        let state = crate::state::StateStore::open_in_memory().unwrap();
        state
            .record_metric_full(&crate::state::MetricRow {
                source_type: Some("postgres".into()),
                export_name: "big_incremental".into(),
                run_id: "r1".into(),
                total_rows: 0,
                status: "success".into(),
                ..Default::default()
            })
            .unwrap();
        let mut diag = super::super::ExportDiagnostic {
            export_name: "big_incremental".into(),
            strategy: "incremental".into(),
            mode: "incremental (cursor: updated_at)".into(),
            cursor_column: Some("updated_at".into()),
            row_estimate: Some(300_000_000),
            row_source: None,
            avg_row_bytes: None,
            cursor_min: None,
            cursor_max: None,
            scan_type: None,
            uses_index: true,
            verdict: super::super::HealthVerdict::Efficient,
            warnings: Vec::new(),
            recommended_profile: "balanced",
            recommended_parallel: (1, "test"),
            suggestion: None,
            chunk_min: None,
            chunk_max: None,
            db_max_connections: None,
        };
        let export = cfg("table: t\nmode: incremental\ncursor_column: updated_at\n");
        overlay_measured_rows(
            &mut diag,
            &export,
            crate::config::SourceType::Postgres,
            &state,
        );
        assert_eq!(
            diag.row_estimate,
            Some(300_000_000),
            "an incremental delta must NOT overwrite the table estimate"
        );
        assert_eq!(diag.row_source.as_deref(), Some("catalog estimate"));
    }

    #[test]
    fn preflight_range_col_resolves_every_read_strategy() {
        assert_eq!(
            preflight_range_col(&cfg("mode: chunked\nchunk_column: id\n")),
            Some("id"),
            "range chunk reads on chunk_column"
        );
        assert_eq!(
            preflight_range_col(&cfg("mode: chunked\nchunk_by_key: uuid\n")),
            Some("uuid"),
            "keyset reads on chunk_by_key"
        );
        assert_eq!(
            preflight_range_col(&cfg("mode: incremental\ncursor_column: updated_at\n")),
            Some("updated_at"),
            "incremental reads on cursor_column"
        );
        assert_eq!(
            preflight_range_col(&cfg("mode: time_window\ntime_column: ts\ndays_window: 7\n")),
            Some("ts"),
            "time_window reads on time_column — the arm the roast found missing"
        );
    }

    #[test]
    fn strategy_probes_index_gates_every_read_strategy_but_full() {
        assert!(
            strategy_probes_index(&cfg("mode: time_window\ntime_column: ts\ndays_window: 7\n")),
            "time_window issues WHERE ts >= window_start on a btree — must probe the index"
        );
        assert!(strategy_probes_index(&cfg(
            "mode: chunked\nchunk_by_key: id\n"
        )));
        assert!(strategy_probes_index(&cfg(
            "mode: chunked\nchunk_column: id\n"
        )));
        assert!(strategy_probes_index(&cfg(
            "mode: incremental\ncursor_column: c\n"
        )));
        assert!(
            !strategy_probes_index(&cfg("mode: full\n")),
            "a plain full scan reads no indexed key — must NOT probe"
        );
    }

    fn facts() -> ProbeFacts {
        ProbeFacts {
            auto_pk: None,
            row_estimate: Some(1_000),
            avg_row_bytes: None,
            range_min: None,
            range_max: None,
            scan_type: None,
            plan_uses_index: false,
            catalog_index: None,
            db_max_connections: None,
        }
    }

    #[test]
    fn index_probe_target_names_the_column_every_read_strategy_reads_on() {
        let t = Some("orders");
        assert_eq!(
            index_probe_target(&cfg("mode: chunked\nchunk_column: id\n"), None, t),
            Some(("orders", "id"))
        );
        assert_eq!(
            index_probe_target(&cfg("mode: chunked\nchunk_by_key: uuid\n"), None, t),
            Some(("orders", "uuid")),
            "keyset probes its seek key, not the absent chunk_column"
        );
        assert_eq!(
            index_probe_target(
                &cfg("mode: time_window\ntime_column: ts\ndays_window: 7\n"),
                None,
                t
            ),
            Some(("orders", "ts"))
        );
        assert_eq!(
            index_probe_target(&cfg("mode: chunked\n"), Some("id"), t),
            Some(("orders", "id")),
            "an auto-resolved chunk PK is the column the run ranges on — probe THAT"
        );
        assert_eq!(
            index_probe_target(&cfg("mode: full\n"), None, t),
            None,
            "a plain full scan reads no indexed key — no probe to run"
        );
        assert_eq!(
            index_probe_target(&cfg("mode: chunked\nchunk_column: id\n"), None, None),
            None,
            "an unresolvable base relation must not be guessed at"
        );
    }

    #[test]
    fn auto_pk_probe_target_fires_only_where_the_planner_auto_resolves() {
        assert_eq!(
            auto_pk_probe_target(&cfg("mode: chunked\n"), Some("orders")),
            Some("orders")
        );
        assert_eq!(
            auto_pk_probe_target(&cfg("mode: chunked\nchunk_column: id\n"), Some("orders")),
            None,
            "an explicit chunk_column is what the planner ranges on — no PK probe"
        );
        assert_eq!(
            auto_pk_probe_target(&cfg("mode: full\n"), Some("orders")),
            None
        );
        assert_eq!(auto_pk_probe_target(&cfg("mode: chunked\n"), None), None);
    }

    #[test]
    fn assemble_diagnostic_lets_a_catalog_yes_outrank_the_base_plan_hint() {
        let e = cfg("mode: chunked\nchunk_column: id\n");
        let d = assemble_diagnostic(
            &e,
            ProbeFacts {
                plan_uses_index: false,
                catalog_index: Some(true),
                ..facts()
            },
        );
        assert!(d.uses_index, "a catalog YES must beat the base-query plan");

        let d = assemble_diagnostic(
            &e,
            ProbeFacts {
                plan_uses_index: true,
                catalog_index: Some(false),
                ..facts()
            },
        );
        assert!(d.uses_index);

        for hint in [true, false] {
            let d = assemble_diagnostic(
                &e,
                ProbeFacts {
                    plan_uses_index: hint,
                    catalog_index: None,
                    ..facts()
                },
            );
            assert_eq!(d.uses_index, hint, "no catalog answer keeps the plan hint");
        }
    }

    #[test]
    fn assemble_diagnostic_labels_the_auto_resolved_pk_not_a_question_mark() {
        let d = assemble_diagnostic(
            &cfg("mode: chunked\nchunk_size: 50000\n"),
            ProbeFacts {
                auto_pk: Some("id".to_string()),
                ..facts()
            },
        );
        assert_eq!(d.strategy, "chunked(id, size=50000)");
        assert!(
            !d.strategy.contains('?'),
            "the `?` placeholder is the diagnostic-bypass tell"
        );
    }

    /// A `mode: cdc` export reads the transaction LOG, not the table: no scan, no index probe, no cursor.
    #[test]
    fn assemble_diagnostic_does_not_grade_a_cdc_stream_as_a_table_scan() {
        for rows in [Some(500_000), Some(5_000_000), Some(500_000_000), None] {
            let d = assemble_diagnostic(
                &cfg("mode: cdc\n"),
                ProbeFacts {
                    row_estimate: rows,
                    plan_uses_index: false,
                    catalog_index: Some(false),
                    ..facts()
                },
            );
            assert_eq!(d.strategy, "cdc");
            assert!(
                matches!(
                    d.verdict,
                    HealthVerdict::Efficient | HealthVerdict::Acceptable
                ),
                "rows={rows:?}: a log reader is not a table scan, got {}",
                d.verdict
            );
            assert!(
                d.suggestion
                    .as_deref()
                    .is_none_or(|s| !s.contains("cursor")),
                "rows={rows:?}: no cursor advice for a log reader: {:?}",
                d.suggestion
            );
        }
    }

    /// The skeleton is only de-triplicated while every engine ROUTES through it.
    #[test]
    fn every_sql_engine_diagnostic_routes_through_the_assembler() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/preflight");
        for engine in ["postgres.rs", "mysql.rs", "mssql.rs"] {
            let src = std::fs::read_to_string(dir.join(engine)).expect("read engine diagnostic");
            let code = src
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                code.contains("assemble_diagnostic("),
                "{engine} must build its diagnostic through the shared assembler"
            );
            assert!(
                !code.contains("ExportDiagnostic {"),
                "{engine} constructs an ExportDiagnostic itself — the assembly \
                 policy (index override, strategy label, warnings) is shared and \
                 must not be re-stated per engine"
            );
        }
    }

    #[test]
    fn derive_strategy_keyset_names_the_seek_key_not_a_question_mark() {
        let e = cfg("mode: chunked\nchunk_by_key: id\nchunk_size: 250000\n");
        assert_eq!(derive_strategy(&e), "keyset(id, size=250000)");
        assert!(
            !derive_strategy(&e).contains('?'),
            "keyset must never render the chunk_column '?' placeholder"
        );
    }

    #[test]
    fn diagnose_mode_str_renders_keyset_with_its_key() {
        assert_eq!(
            diagnose_mode_str(&cfg(
                "mode: chunked\nchunk_by_key: ref_id\nchunk_size: 100000\n"
            )),
            "keyset (key: ref_id, size: 100000)"
        );
    }

    #[test]
    fn diagnose_mode_str_renders_each_mode() {
        assert_eq!(diagnose_mode_str(&cfg("mode: full\n")), "full");
        assert_eq!(
            diagnose_mode_str(&cfg("mode: incremental\ncursor_column: updated_at\n")),
            "incremental (cursor: updated_at)"
        );
        assert_eq!(
            diagnose_mode_str(&cfg(
                "mode: chunked\nchunk_column: id\nchunk_size: 100000\n"
            )),
            "chunked (column: id, size: 100000)"
        );
    }

    #[test]
    fn resolve_preflight_base_query_table_shortcut_and_inline() {
        assert_eq!(
            resolve_preflight_base_query(&cfg("mode: full\ntable: orders\n")),
            "SELECT * FROM orders"
        );
        assert_eq!(
            resolve_preflight_base_query(&cfg("mode: full\nquery: \"SELECT id FROM t WHERE x\"\n")),
            "SELECT id FROM t WHERE x"
        );
    }

    #[test]
    fn recommend_profile_indexed_small_is_fast() {
        let e = cfg("");
        assert_eq!(recommend_profile(Some(500_000), true, &e), "fast");
    }

    #[test]
    fn recommend_profile_indexed_medium_is_balanced() {
        let e = cfg("");
        assert_eq!(recommend_profile(Some(5_000_000), true, &e), "balanced");
    }

    #[test]
    fn recommend_profile_indexed_large_is_safe() {
        let e = cfg("");
        assert_eq!(recommend_profile(Some(15_000_000), true, &e), "safe");
    }

    #[test]
    fn recommend_profile_no_index_small_is_balanced() {
        let e = cfg("");
        assert_eq!(recommend_profile(Some(50_000), false, &e), "balanced");
    }

    #[test]
    fn recommend_profile_no_index_large_is_safe() {
        let e = cfg("");
        assert_eq!(recommend_profile(Some(5_000_000), false, &e), "safe");
    }

    #[test]
    fn sparsity_over_the_full_i64_range_saturates_instead_of_overflowing() {
        let info = chunk_sparsity_from_counts(10, i64::MIN, i64::MAX, 100_000);
        assert_eq!(info.range_span, i64::MAX);
        assert!(info.is_sparse);
        let huge = chunk_sparsity_from_counts(10, i64::MIN, i64::MAX, usize::MAX);
        assert_eq!(huge.logical_windows, 1);
    }

    #[test]
    fn chunk_sparsity_dense_range_not_sparse() {
        let info = chunk_sparsity_from_counts(10_000, 0, 10_000, 100);
        assert!(!info.is_sparse);
        assert!(info.density > 0.9);
    }

    #[test]
    fn chunk_sparsity_very_sparse_range_is_sparse() {
        let info = chunk_sparsity_from_counts(100, 0, 1_000_000, 50_000);
        assert!(
            info.is_sparse,
            "expected sparse: density={:.6}",
            info.density
        );
        assert!(info.density < 0.1);
        assert!(info.logical_windows > 10);
    }

    #[test]
    fn chunk_sparsity_zero_rows_never_sparse() {
        let info = chunk_sparsity_from_counts(0, 0, 1_000_000, 100);
        assert!(!info.is_sparse);
    }

    #[test]
    fn check_sparse_range_non_chunked_mode_returns_none() {
        let e = cfg("mode: full\n");
        assert!(check_sparse_range(&e, Some(100), Some("1"), Some("1000000")).is_none());
    }

    #[test]
    fn check_sparse_range_chunk_by_days_returns_none() {
        let e = cfg("mode: chunked\nchunk_column: created_at\nchunk_by_days: 7\n");
        assert!(check_sparse_range(&e, Some(100), Some("1"), Some("1000000")).is_none());
    }

    #[test]
    fn check_sparse_range_sparse_returns_warning() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 100000\n");
        let w = check_sparse_range(&e, Some(100), Some("1"), Some("5000000"));
        assert!(w.is_some(), "expected sparse warning");
        let msg = w.unwrap();
        assert!(msg.contains("Sparse"), "got: {msg}");
    }

    #[test]
    fn check_sparse_range_dense_range_returns_none() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 100\n");
        assert!(check_sparse_range(&e, Some(10_000), Some("0"), Some("10000")).is_none());
    }

    #[test]
    fn check_sparse_range_keyset_is_immune_even_on_a_sparse_span() {
        let e = cfg("mode: chunked\nchunk_by_key: id\nchunk_size: 100000\n");
        assert!(
            check_sparse_range(&e, Some(100), Some("1"), Some("5000000")).is_none(),
            "keyset must be immune to the sparse-range warning"
        );
    }

    #[test]
    fn recommend_parallelism_keyset_default_is_sequential_with_the_escape_hint() {
        let e = cfg("mode: chunked\nchunk_by_key: id\nchunk_size: 250000\n");
        let (level, reason) = recommend_parallelism(&e, Some(1_000_000), true);
        assert_eq!(level, 1, "keyset default must be a single worker: {reason}");
        assert!(
            reason.contains("parallel: N"),
            "must point at the parallel escape: {reason}"
        );
    }

    #[test]
    fn recommend_parallelism_keyset_honours_configured_parallel() {
        let e = cfg("mode: chunked\nchunk_by_key: id\nchunk_size: 250000\nparallel: 4\n");
        let (level, reason) = recommend_parallelism(&e, Some(1_000_000), true);
        assert_eq!(
            level, 4,
            "keyset must honour the configured parallel: {reason}"
        );
        assert!(reason.contains("row-percentile"), "got: {reason}");
    }

    #[test]
    fn check_oversized_chunk_non_chunked_returns_none() {
        let e = cfg("mode: full\n");
        assert!(
            check_oversized_chunk(&e, Some(10_000_000), Some(500), Some("0"), Some("9999999"))
                .is_none()
        );
    }

    #[test]
    fn check_oversized_chunk_by_days_returns_none() {
        let e = cfg("mode: chunked\nchunk_column: created_at\nchunk_by_days: 7\n");
        assert!(
            check_oversized_chunk(&e, Some(10_000_000), Some(500), Some("0"), Some("9999999"))
                .is_none()
        );
    }

    #[test]
    fn check_oversized_chunk_no_row_estimate_returns_none() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 1000000\n");
        assert!(check_oversized_chunk(&e, None, Some(500), Some("0"), Some("9999999")).is_none());
    }

    #[test]
    fn check_oversized_chunk_no_width_returns_none() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 1000000\n");
        assert!(
            check_oversized_chunk(&e, Some(10_000_000), None, Some("0"), Some("9999999")).is_none()
        );
    }

    #[test]
    fn check_oversized_chunk_small_chunk_returns_none() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 100000\n");
        let w = check_oversized_chunk(&e, Some(10_000_000), Some(500), Some("0"), Some("10000000"));
        assert!(w.is_none(), "48 MB/chunk is under budget: {w:?}");
    }

    #[test]
    fn check_oversized_chunk_heavy_range_chunk_warns_and_suggests_smaller() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 1000000\n");
        let w = check_oversized_chunk(&e, Some(10_000_000), Some(500), Some("0"), Some("10000000"))
            .expect("heavy chunk must warn");
        assert!(w.contains("Heavy chunk"), "got: {w}");
        assert!(
            w.contains("chunk_size=1000000"),
            "names the current size: {w}"
        );
        assert!(
            w.contains("chunk_size around"),
            "advises a smaller size: {w}"
        );
    }

    #[test]
    fn check_oversized_chunk_low_cardinality_range_key_does_not_phantom_warn() {
        let e = cfg("mode: chunked\nchunk_column: amount\nchunk_size: 100000\n");
        let w = check_oversized_chunk(&e, Some(150_000), Some(15), Some("0"), Some("99"));
        assert!(
            w.is_none(),
            "one chunk holds the whole 150K-row table (~2 MB) — no phantom heavy warning: {w:?}"
        );
    }

    #[test]
    fn check_oversized_chunk_keyset_chunk_larger_than_table_does_not_phantom_warn() {
        let e = cfg("mode: chunked\nchunk_by_key: id\nchunk_size: 700000\n");
        let w = check_oversized_chunk(&e, Some(50_000), Some(500), None, None);
        assert!(
            w.is_none(),
            "a chunk_size past the row count still scans only the 50K rows present (~24 MB): {w:?}"
        );
    }

    #[test]
    fn check_oversized_chunk_keyset_uses_chunk_size_directly() {
        let e = cfg("mode: chunked\nchunk_by_key: id\nchunk_size: 2000000\n");
        let w = check_oversized_chunk(&e, Some(50_000_000), Some(200), None, None)
            .expect("keyset heavy chunk must warn even without a key range");
        assert!(w.contains("Heavy chunk"), "got: {w}");
    }

    #[test]
    fn check_dense_surrogate_row_number_in_query_returns_warning() {
        let e = cfg(
            "mode: chunked\nchunk_column: id\nquery: \"SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t\"\n",
        );
        assert!(check_dense_surrogate_cost(&e).is_some());
    }

    #[test]
    fn check_dense_surrogate_normal_chunked_returns_none() {
        let e = cfg("mode: chunked\nchunk_column: id\n");
        assert!(check_dense_surrogate_cost(&e).is_none());
    }

    #[test]
    fn check_missing_checkpoint_keyset_without_checkpoint_warns() {
        let e = cfg("mode: chunked\nchunk_by_key: id\nchunk_size: 250000\n");
        let w = check_missing_checkpoint(&e, Some(1_000_000));
        assert!(w.is_some(), "expected a checkpoint nudge");
        let msg = w.unwrap();
        assert!(msg.contains("chunk_checkpoint"), "names the fix");
        assert!(
            !msg.contains("incremental-by-key") && !msg.to_lowercase().contains("omit"),
            "must not advise the removed incremental semantics: {msg}"
        );
        assert!(
            msg.contains("does NOT change") || msg.contains("full pass"),
            "must reassure that clean re-run semantics are unchanged: {msg}"
        );
    }

    #[test]
    fn check_missing_checkpoint_range_chunk_unknown_size_warns() {
        let e = cfg("mode: chunked\nchunk_column: ref_id\nchunk_size: 250000\n");
        assert!(check_missing_checkpoint(&e, None).is_some());
    }

    #[test]
    fn check_missing_checkpoint_present_is_silent() {
        let e = cfg("mode: chunked\nchunk_by_key: id\nchunk_checkpoint: true\n");
        assert!(check_missing_checkpoint(&e, Some(5_000_000)).is_none());
    }

    #[test]
    fn check_missing_checkpoint_keyset_incremental_implies_checkpoint_is_silent() {
        let e = cfg("mode: chunked\nchunk_by_key: id\nkeyset_incremental: true\n");
        assert!(check_missing_checkpoint(&e, Some(5_000_000)).is_none());
    }

    #[test]
    fn check_missing_checkpoint_known_small_table_is_silent() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 1000\n");
        assert!(check_missing_checkpoint(&e, Some(5_000)).is_none());
    }

    #[test]
    fn check_missing_checkpoint_non_chunked_is_silent() {
        let e = cfg("mode: full\n");
        assert!(check_missing_checkpoint(&e, Some(10_000_000)).is_none());
    }

    #[test]
    fn check_connection_limit_parallel_one_returns_none() {
        assert!(check_connection_limit(1, Some(100)).is_none());
    }

    #[test]
    fn check_connection_limit_unknown_max_conn_returns_warning() {
        let w = check_connection_limit(4, None).unwrap();
        assert!(
            w.contains("max_connections") || w.contains("limit check"),
            "got: {w}"
        );
    }

    #[test]
    fn check_connection_limit_exceeds_max_conn_warns() {
        let w = check_connection_limit(10, Some(8)).unwrap();
        assert!(w.contains("10") && w.contains("8"), "got: {w}");
    }

    #[test]
    fn check_connection_limit_within_limit_returns_none() {
        assert!(check_connection_limit(4, Some(100)).is_none());
    }

    #[test]
    fn check_parallel_memory_risk_parallel_one_returns_none() {
        let e = cfg("parallel: 1\n");
        assert!(check_parallel_memory_risk(&e, Some(10_000_000)).is_none());
    }

    #[test]
    fn check_parallel_memory_risk_small_rows_returns_none() {
        let e = cfg("parallel: 4\n");
        assert!(check_parallel_memory_risk(&e, Some(100_000)).is_none());
    }

    #[test]
    fn check_parallel_memory_risk_large_rows_returns_warning() {
        let e = cfg("parallel: 4\n");
        let w = check_parallel_memory_risk(&e, Some(6_000_000)).unwrap();
        assert!(
            w.contains("Parallel=4") || w.contains("arallel"),
            "got: {w}"
        );
    }

    #[test]
    fn collect_warnings_assembles_applicable_checks_in_registry_order() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 100000\nparallel: 20\n");
        let warnings = collect_warnings(
            &e,
            Some(10_000_000),
            None,
            Some("1"),
            Some("1000000000"),
            Some(20),
        );
        assert_eq!(warnings.len(), 4, "four checks apply: {warnings:?}");
        assert!(
            warnings[0].message.contains("max_connections")
                && warnings[0].severity == Severity::High,
            "connection-limit is first, High: {warnings:?}"
        );
        assert!(
            warnings[1].message.contains("Sparse") && warnings[1].severity == Severity::Medium,
            "sparse-range is second, Medium: {warnings:?}"
        );
        assert!(
            warnings[2].message.contains("memory") && warnings[2].severity == Severity::High,
            "parallel-memory is third, High: {warnings:?}"
        );
        assert!(
            warnings[3].message.contains("chunk_checkpoint")
                && warnings[3].severity == Severity::Medium,
            "missing-checkpoint is last, Medium: {warnings:?}"
        );
    }

    #[test]
    fn compute_verdict_indexed_cursor_small_is_efficient() {
        assert!(matches!(
            compute_verdict(Some(1_000_000), true, true, None, 1),
            HealthVerdict::Efficient
        ));
    }

    #[test]
    fn compute_verdict_indexed_no_cursor_small_is_acceptable() {
        assert!(matches!(
            compute_verdict(Some(1_000_000), true, false, None, 1),
            HealthVerdict::Acceptable
        ));
    }

    #[test]
    fn compute_verdict_no_index_small_is_degraded() {
        assert!(matches!(
            compute_verdict(Some(500_000), false, true, None, 1),
            HealthVerdict::Degraded
        ));
    }

    #[test]
    fn compute_verdict_no_index_very_large_no_width_is_unsafe() {
        assert!(matches!(
            compute_verdict(Some(5_000_000), false, false, None, 1),
            HealthVerdict::Unsafe
        ));
    }

    #[test]
    fn compute_verdict_no_index_large_narrow_within_budget_is_degraded() {
        assert!(matches!(
            compute_verdict(Some(10_000_000), false, false, Some(100), 4),
            HealthVerdict::Degraded
        ));
    }

    #[test]
    fn compute_verdict_no_index_wide_high_parallel_over_budget_is_unsafe() {
        assert!(matches!(
            compute_verdict(Some(10_000_000), false, false, Some(20_000), 24),
            HealthVerdict::Unsafe
        ));
    }
}
