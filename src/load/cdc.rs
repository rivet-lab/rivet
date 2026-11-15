//! CDC current-state dedup view.
//!
//! rivet CDC appends a change log to `<table>__changes` (free `LOAD DATA` /
//! billed `COPY`); a **view** collapses it to current state. The collapse is one
//! `ROW_NUMBER` window that keeps the latest change per PK:
//!
//! ```sql
//! ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <total change order> DESC) = 1
//! ```
//!
//! A deleted row is kept as a **tombstone**, not dropped: the winning change's
//! `__op` becomes a boolean `__is_deleted` column, so the row survives with its
//! last-known values and an auditable delete flag — a delete is never a silent
//! disappearance. Live current state is `WHERE NOT __is_deleted`.
//!
//! The **total change order** is `(__pos, __seq)`:
//! - `__pos` is the commit position — it orders changes *across* transactions,
//!   but every change in one transaction shares it (verified live on all three
//!   engines: 8000 updates of one PK in one transaction → a single `__pos`).
//! - `__seq` (OSS `TxnSeq`) is the intra-transaction ordinal — it breaks that
//!   tie. Without it the dedup picked an arbitrary row (live: `counter = 1` for
//!   a row whose committed value was `8000`).
//!
//! `__pos` is a JSON string whose shape is per source engine, so its parse (the
//! part before `__seq`) is engine-specific — see [`SourceEngine`]. The parse
//! functions are also *warehouse*-specific ([`Warehouse`]): BigQuery reads JSON
//! with `JSON_VALUE`, Snowflake with `PARSE_JSON(...):path`.

use crate::source::cdc::order;
use crate::types::target::{BqType, ChType, SfType, TargetColumnSpec, TargetStatus, TargetType};

/// The source engine a change log came from — selects how `__pos` is parsed into a sortable key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceEngine {
    /// `{"file":"binlog.000047","pos":10840633}` — order by file, then numeric pos.
    MySql,
    /// `{"lsn":"3D/484A4908"}` — hex `hi/lo`; zero-pad each half to fixed width so a lexical compare equals a numeric one (raw `"9" > "10"` otherwise).
    Postgres,
    /// `{"lsn":"0000002d000000d80194"}` — fixed-width hex; lexical == numeric.
    SqlServer,
    /// `{"_data":"826A4E0001...","ct":N,"ord":N}`.
    Mongo,
}

impl SourceEngine {
    /// The source type this change log came from.
    pub fn source_type(self) -> crate::config::SourceType {
        use crate::config::SourceType;
        match self {
            Self::MySql => SourceType::Mysql,
            Self::Postgres => SourceType::Postgres,
            Self::SqlServer => SourceType::Mssql,
            Self::Mongo => SourceType::Mongo,
        }
    }
}

/// The warehouse the view is defined in — selects the JSON-parse dialect and the `SELECT * EXCEPT/EXCLUDE` keyword.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Warehouse {
    BigQuery,
    Snowflake,
    ClickHouse,
}

impl Warehouse {
    /// The warehouse's name in operator-facing notes.
    pub fn label(self) -> &'static str {
        match self {
            Warehouse::BigQuery => "BigQuery",
            Warehouse::Snowflake => "Snowflake",
            Warehouse::ClickHouse => "ClickHouse",
        }
    }

    /// The `SELECT *`-minus-columns keyword: BigQuery and ClickHouse spell it `EXCEPT`, Snowflake `EXCLUDE`.
    fn except_keyword(self) -> &'static str {
        match self {
            Warehouse::BigQuery | Warehouse::ClickHouse => "EXCEPT",
            Warehouse::Snowflake => "EXCLUDE",
        }
    }

    /// Quote a `project.dataset.table` / `db.schema.table` identifier for this warehouse.
    pub(crate) fn quote_fqtn(self, fqtn: &str) -> String {
        match self {
            Warehouse::BigQuery => format!("`{fqtn}`"),
            Warehouse::Snowflake => fqtn.to_string(),
            Warehouse::ClickHouse => fqtn
                .split('.')
                .map(|p| format!("`{p}`"))
                .collect::<Vec<_>>()
                .join("."),
        }
    }

    /// Quote a single column identifier for the view's `PARTITION BY`/`ORDER BY`.
    pub(crate) fn quote_ident(self, col: &str) -> String {
        match self {
            Warehouse::BigQuery | Warehouse::ClickHouse => format!("`{col}`"),
            Warehouse::Snowflake => col.to_string(),
        }
    }
}

/// What decides the WINNER when a key appears more than once in a compaction buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactOrder {
    Cdc(SourceEngine),
    Cursor(String),
}

impl From<SourceEngine> for CompactOrder {
    fn from(e: SourceEngine) -> Self {
        CompactOrder::Cdc(e)
    }
}

impl CompactOrder {
    /// The complete `ORDER BY` clause of the winner-picking `ROW_NUMBER`.
    pub(crate) fn order_by(&self, warehouse: Warehouse) -> String {
        match self {
            CompactOrder::Cdc(engine) => {
                let baseline_last = (seeded!(
                    "snowflake-compact-ranks-baseline-first",
                    false,
                    warehouse == Warehouse::Snowflake
                ))
                .then(|| "__pos IS NOT NULL".to_string());
                baseline_last
                    .into_iter()
                    .chain(engine.order_exprs(warehouse))
                    .map(|e| format!("{e} DESC"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
            CompactOrder::Cursor(column) => cursor_order_by(warehouse, column),
        }
    }
}

impl SourceEngine {
    /// This engine's position order.
    pub(crate) fn order_row(self) -> &'static order::Row {
        match self {
            SourceEngine::MySql => &order::MYSQL,
            SourceEngine::Postgres => &order::POSTGRES,
            SourceEngine::SqlServer => &order::SQL_SERVER,
            SourceEngine::Mongo => &order::MONGO,
        }
    }

    /// The `ORDER BY` expressions (most-significant first) that totally-order the change log for this engine in `warehouse`'s SQL dialect.
    pub(crate) fn order_exprs(self, warehouse: Warehouse) -> Vec<String> {
        let pos = match warehouse {
            Warehouse::BigQuery => self.order_row().sql(order::Dialect::BigQuery),
            Warehouse::Snowflake => self.order_row().sql(order::Dialect::Snowflake),
            Warehouse::ClickHouse => vec!["__ver".into()],
        };
        pos.into_iter()
            .chain(std::iter::once("__seq".to_string()))
            .collect()
    }
}

/// The three CDC meta columns rivet's change log carries, typed for `warehouse`.
pub(crate) fn is_meta_column(name: &str) -> bool {
    matches!(name, "__op" | "__pos" | "__seq")
}

pub fn meta_column_specs(warehouse: Warehouse) -> Vec<TargetColumnSpec> {
    let (str_ty, int_ty): (TargetType, TargetType) = match warehouse {
        Warehouse::BigQuery => (BqType::String.into(), BqType::Int64.into()),
        Warehouse::Snowflake => (SfType::Varchar.into(), SfType::Integer.into()),
        Warehouse::ClickHouse => (ChType::String.into(), ChType::Int64.into()),
    };
    ["__op", "__pos"]
        .into_iter()
        .map(|name| meta_spec(name, str_ty.clone()))
        .chain(std::iter::once(meta_spec("__seq", int_ty)))
        .collect()
}

fn meta_spec(name: &str, ty: TargetType) -> TargetColumnSpec {
    TargetColumnSpec {
        column_name: name.into(),
        autoload_type: ty.clone(),
        target_type: ty,
        status: TargetStatus::Ok,
        note: None,
        cast_sql: None,
    }
}

pub use crate::enrich::DELETE_FLAG_COLUMN;

/// Every name rivet OWNS in a changelog or base+buffer table.
pub(crate) fn is_reserved_column(name: &str) -> bool {
    is_meta_column(name) || name == DELETE_FLAG_COLUMN
}

/// The base table's delete flag as a column spec, appended to the source columns of a base-and-buffer load (the baseline Parquet carries it as `false`).
pub fn flag_spec(warehouse: Warehouse) -> TargetColumnSpec {
    let ty = match warehouse {
        Warehouse::BigQuery => BqType::Bool.into(),
        Warehouse::Snowflake => SfType::Boolean.into(),
        Warehouse::ClickHouse => ChType::Bool.into(),
    };
    meta_spec(DELETE_FLAG_COLUMN, ty)
}

/// A half-open window on the partition column, as typed SQL literals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeBound {
    pub column: String,
    pub lo: String,
    pub hi_exclusive: String,
    pub all_lo: String,
    pub all_hi_exclusive: String,
}

/// `__rivet_t.k = __rivet_s.k AND …` over the key columns.
fn key_join(pk: &[&str]) -> String {
    key_equality(Warehouse::BigQuery, "__rivet_t", "__rivet_s", pk)
}

/// `left.k = right.k AND …` over the key columns, quoted for `warehouse`.
fn key_equality(warehouse: Warehouse, left: &str, right: &str, pk: &[&str]) -> String {
    pk.iter()
        .map(|k| {
            let k = warehouse.quote_ident(k);
            format!("{left}.{k} = {right}.{k}")
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// The tombstones a re-baseline buffers: every live base row whose key the buffer holds no row for, with the base's last values and `__op = 'delete'`.
pub fn absent_as_deleted_sql(
    warehouse: Warehouse,
    base_fqtn: &str,
    changes_fqtn: &str,
    columns: &[&str],
    pk: &[&str],
) -> String {
    let q = |c: &str| warehouse.quote_ident(c);
    let cols = columns.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
    let picked = columns
        .iter()
        .map(|c| format!("__rivet_t.{}", q(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let (op, flag) = (q("__op"), q(DELETE_FLAG_COLUMN));
    let (base, changes) = (
        warehouse.quote_fqtn(base_fqtn),
        warehouse.quote_fqtn(changes_fqtn),
    );
    format!(
        "INSERT INTO {changes} ({cols}, {op})\n\
         SELECT {picked}, 'delete'\n\
         FROM {base} AS __rivet_t\n\
         WHERE NOT IFNULL(__rivet_t.{flag}, FALSE)\n\
         \x20 AND NOT EXISTS (SELECT 1 FROM {changes} AS __rivet_s WHERE {join})",
        join = key_equality(warehouse, "__rivet_t", "__rivet_s", pk)
    )
}

/// The partition column over every row a compaction can touch, as one column `v`.
fn touched_values_sql(
    changes_fqtn: &str,
    base_fqtn: &str,
    pk: &[&str],
    column: &str,
    as_date: Option<&TargetType>,
) -> String {
    let (own, base) = match as_date {
        Some(ty) => (
            date_of(&format!("`{column}`"), ty),
            date_of(&format!("__rivet_t.`{column}`"), ty),
        ),
        None => (format!("`{column}`"), format!("__rivet_t.`{column}`")),
    };
    format!(
        "SELECT {own} AS v FROM `{changes_fqtn}` UNION ALL SELECT {base} FROM `{base_fqtn}` AS __rivet_t \
         WHERE EXISTS (SELECT 1 FROM `{changes_fqtn}` AS __rivet_s WHERE {})",
        key_join(pk)
    )
}

/// Script-variable budget for keys: BigQuery refuses a variable over 1,048,576 bytes (measured.
const KEY_ARRAY_BYTES: u64 = 700_000;

/// The script-variable element type for a key column's warehouse type.
fn key_array_type(target_type: &TargetType) -> Option<&'static str> {
    let TargetType::BigQuery(t) = target_type else {
        return None;
    };
    match t {
        BqType::Int64 => Some("INT64"),
        BqType::String => Some("STRING"),
        BqType::Numeric => Some("NUMERIC"),
        BqType::BigNumeric => Some("BIGNUMERIC"),
        BqType::Date => Some("DATE"),
        BqType::DateTime => Some("DATETIME"),
        BqType::Timestamp => Some("TIMESTAMP"),
        BqType::Bool
        | BqType::Float64
        | BqType::Time
        | BqType::Bytes
        | BqType::Json
        | BqType::Array(_) => None,
    }
}

/// Script statements that set `days` to the partition days a compaction touches, reading the base only for the updated/deleted keys whose row the buffer's own days do not contain (a key whose partition value changed, or a delete that carries no partition value).
fn touched_days_script(
    changes_fqtn: &str,
    base_fqtn: &str,
    pk: &[&str],
    column: &str,
    column_type: &TargetType,
    key0: Option<(&str, &str)>,
) -> String {
    let own = date_of(&format!("`{column}`"), column_type);
    let base_day = date_of(&format!("__rivet_t.`{column}`"), column_type);
    let join = key_join(pk);
    let unmatched = format!(
        "FROM `{changes_fqtn}` AS __rivet_s WHERE IFNULL(__rivet_s.__op, '') != 'insert' AND NOT EXISTS \
         (SELECT 1 FROM `{base_fqtn}` AS __rivet_t WHERE {join} AND \
         ({base_day} IN UNNEST(days) OR __rivet_t.`{column}` IS NULL))"
    );
    let add_days = |filter: &str| {
        format!(
            "SET days = ARRAY(SELECT DISTINCT d FROM UNNEST(ARRAY_CONCAT(days, (SELECT \
             IFNULL(ARRAY_AGG(DISTINCT {base_day} IGNORE NULLS), []) FROM `{base_fqtn}` AS __rivet_t \
             WHERE {filter}))) AS d);"
        )
    };
    let scan_all = add_days(&format!(
        "EXISTS (SELECT 1 FROM `{changes_fqtn}` AS __rivet_s WHERE {join} AND IFNULL(__rivet_s.__op, '') != 'insert')"
    ));
    let lookup = match key0 {
        Some((k, _)) => format!(
            "SET moved_bytes = (SELECT IFNULL(SUM(BYTE_LENGTH(CAST(__rivet_s.`{k}` AS STRING)) + 16), 0) {unmatched});\n\
             IF moved_bytes > 0 THEN\n\
             \x20 IF moved_bytes <= {KEY_ARRAY_BYTES} THEN\n\
             \x20   SET moved = (SELECT IFNULL(ARRAY_AGG(DISTINCT __rivet_s.`{k}` IGNORE NULLS), []) {unmatched});\n\
             \x20   {}\n\
             \x20 ELSE\n\
             \x20   {scan_all}\n\
             \x20 END IF;\n\
             END IF;",
            add_days(&format!(
                "__rivet_t.`{k}` IN UNNEST(moved) AND EXISTS (SELECT 1 FROM `{changes_fqtn}` AS __rivet_s WHERE {join})"
            ))
        ),
        None => format!("IF EXISTS (SELECT 1 {unmatched}) THEN\n\x20 {scan_all}\nEND IF;"),
    };
    format!(
        "SET days = (SELECT IFNULL(ARRAY_AGG(DISTINCT {own} IGNORE NULLS), []) FROM `{changes_fqtn}`);\n{lookup}"
    )
}

/// Whether a compaction takes its partitions from the buffer alone.
pub fn trusts_buffer_days(order: &CompactOrder) -> bool {
    matches!(order, CompactOrder::Cdc(SourceEngine::MySql))
}

/// The partition column as a `DATE`.
fn date_of(col_sql: &str, target_type: &TargetType) -> String {
    if *target_type == TargetType::BigQuery(BqType::Timestamp) {
        format!("DATE({col_sql}, 'UTC')")
    } else {
        format!("DATE({col_sql})")
    }
}

/// The partition column's warehouse type, `TIMESTAMP` when the specs do not name it.
pub(crate) fn column_type_of(specs: &[TargetColumnSpec], col: &str) -> TargetType {
    specs
        .iter()
        .find(|s| s.column_name == col)
        .map_or(TargetType::BigQuery(BqType::Timestamp), |s| {
            s.target_type.clone()
        })
}

/// `(rows, min, max, null_count)` — rows and NULLs of the buffer, min/max of the partition column over everything the compaction touches ([`touched_values_sql`]).
pub fn compact_probe_sql(
    changes_fqtn: &str,
    base_fqtn: &str,
    pk: &[String],
    partition_col: Option<&str>,
    time_type: Option<&TargetType>,
    buffer_only: bool,
) -> String {
    let Some(c) = partition_col else {
        return format!(
            "SELECT COUNT(*) AS n, '' AS lo, '' AS hi, 0 AS null_keys FROM `{changes_fqtn}`"
        );
    };
    let pk_refs: Vec<&str> = pk.iter().map(String::as_str).collect();
    let values = if buffer_only {
        let own = time_type.map_or_else(|| format!("`{c}`"), |ty| date_of(&format!("`{c}`"), ty));
        format!("SELECT {own} AS v FROM `{changes_fqtn}`")
    } else {
        touched_values_sql(changes_fqtn, base_fqtn, &pk_refs, c, time_type)
    };
    let inside = if time_type.is_some() {
        let (from, until) = PARTITIONED_DAYS;
        let v = format!("IF(v >= DATE '{from}' AND v < DATE '{until}', v, NULL)");
        format!(
            ", IFNULL(CAST(MIN({v}) AS STRING), '{NOTHING_INSIDE}') AS in_lo, \
             IFNULL(CAST(MAX({v}) AS STRING), '{NOTHING_INSIDE}') AS in_hi"
        )
    } else {
        String::new()
    };
    format!(
        "SELECT (SELECT COUNT(*) FROM `{changes_fqtn}`) AS n, \
         IFNULL(CAST(MIN(v) AS STRING), '') AS lo, IFNULL(CAST(MAX(v) AS STRING), '') AS hi, \
         (SELECT COUNTIF(`{c}` IS NULL) FROM `{changes_fqtn}`) AS null_keys{inside} \
         FROM ({values})"
    )
}

/// The days BigQuery's time-unit partitions cover, `[from, until)`.
pub const PARTITIONED_DAYS: (&str, &str) = ("1960-01-01", "2160-01-01");

/// The probe's `in_lo`/`in_hi` when no value lies inside [`PARTITIONED_DAYS`].
pub const NOTHING_INSIDE: &str = "none";

/// Split `[lo, hi]` (inclusive dates) into half-open day windows of at most `step_days`.
pub fn day_windows(
    lo: chrono::NaiveDate,
    hi: chrono::NaiveDate,
    step_days: i64,
) -> Vec<(chrono::NaiveDate, chrono::NaiveDate)> {
    let mut out = Vec::new();
    let mut start = lo;
    let end = hi + chrono::Duration::days(1);
    while start < end {
        let next = (start + chrono::Duration::days(step_days)).min(end);
        out.push((start, next));
        start = next;
    }
    out
}

/// A `date` as a literal of the partition column's BigQuery type.
pub fn time_literal(target_type: &TargetType, date: chrono::NaiveDate) -> String {
    let d = date.format("%Y-%m-%d");
    match target_type {
        TargetType::BigQuery(BqType::Date) => format!("DATE '{d}'"),
        TargetType::BigQuery(BqType::DateTime) => format!("DATETIME '{d}T00:00:00'"),
        _ => format!("TIMESTAMP '{d} 00:00:00+00'"),
    }
}

/// The compaction MERGE: the latest change per key in `changes` (the buffer) is upserted into `base`.
#[allow(clippy::too_many_arguments)]
pub fn compact_merge_sql(
    base_fqtn: &str,
    changes_fqtn: &str,
    columns: &[&str],
    pk: &[&str],
    order: impl Into<CompactOrder>,
    bound: Option<&RangeBound>,
    nulls_only: Option<&str>,
    deleted_flag: bool,
) -> String {
    let filter = match (bound, nulls_only) {
        (Some(b), _) => MergeFilter::Range(b.clone()),
        (None, Some(c)) => MergeFilter::NullKeys(c.to_string()),
        (None, None) => MergeFilter::All,
    };
    compact_merge_filtered_sql(
        base_fqtn,
        changes_fqtn,
        columns,
        pk,
        &order.into(),
        &filter,
        deleted_flag,
    )
}

/// Which of the buffer's WINNERS one MERGE takes, and the matching constant predicate on the base so BigQuery prunes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeFilter {
    All,
    Range(RangeBound),
    /// A day-partitioned base: the winners in `DATE(column) IN UNNEST(chunk)`, the base bounded by `all`.
    Days {
        column: String,
        column_type: TargetType,
        chunk: String,
        all: String,
    },
    NullKeys(String),
}

impl MergeFilter {
    /// The predicate on a row of `alias` — no alias for the buffer's own columns, `T` for the base.
    fn predicate(&self, alias: &str) -> String {
        let base = !alias.is_empty();
        let q = |c: &str| {
            if base {
                format!("{alias}.`{c}`")
            } else {
                format!("`{c}`")
            }
        };
        let upper = |c: &str, hi: &str| {
            if hi.is_empty() {
                String::new()
            } else {
                format!(" AND {c} < {hi}")
            }
        };
        match self {
            MergeFilter::All => String::new(),
            MergeFilter::Range(b) if base => format!(
                " AND ({c} >= {lo}{hi} OR {c} IS NULL)",
                c = q(&b.column),
                lo = b.all_lo,
                hi = upper(&q(&b.column), &b.all_hi_exclusive)
            ),
            MergeFilter::Range(b) => format!(
                " AND {c} >= {lo}{hi}",
                c = q(&b.column),
                lo = b.lo,
                hi = upper(&q(&b.column), &b.hi_exclusive)
            ),
            MergeFilter::Days {
                column,
                column_type,
                all,
                ..
            } if base => format!(
                " AND ({d} IN UNNEST({all}) OR {c} IS NULL)",
                d = date_of(&q(column), column_type),
                c = q(column)
            ),
            MergeFilter::Days {
                column,
                column_type,
                chunk,
                ..
            } => format!(
                " AND {} IN UNNEST({chunk})",
                date_of(&q(column), column_type)
            ),
            MergeFilter::NullKeys(c) => format!(" AND {} IS NULL", q(c)),
        }
    }
}

/// The latest row per key of `changes_fqtn` under `order`, with the ranking column dropped and `filter` (a leading ` AND …` or empty) on the winners.
fn winners_sql(
    warehouse: Warehouse,
    changes_fqtn: &str,
    pk: &[&str],
    order: &CompactOrder,
    filter: &str,
) -> String {
    let partition = quote_partition(warehouse, pk);
    let order = order.order_by(warehouse);
    let except = warehouse.except_keyword();
    let changes = warehouse.quote_fqtn(changes_fqtn);
    format!(
        "SELECT * {except} (__rn) FROM (\n\
         \x20   SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order}) AS __rn\n\
         \x20   FROM {changes}\n\
         \x20 ) WHERE __rn = 1{filter}"
    )
}

/// The one transaction that folds a spent buffer into its base on a warehouse that replaces rows instead of merging them (Snowflake).
pub(crate) fn replace_compact_sql(
    warehouse: Warehouse,
    base_fqtn: &str,
    changes_fqtn: &str,
    columns: &[&str],
    pk: &[&str],
    order: &CompactOrder,
    deleted_flag: bool,
) -> String {
    let winners = winners_sql(warehouse, changes_fqtn, pk, order, "");
    let base = warehouse.quote_fqtn(base_fqtn);
    let target = base_fqtn.rsplit('.').next().unwrap_or(base_fqtn);
    let on = key_equality(warehouse, target, "__rivet_s", pk);
    let q = |c: &str| warehouse.quote_ident(c);
    let flag = |rendered: String| deleted_flag.then_some(rendered);
    let insert_cols = columns
        .iter()
        .map(|c| q(c))
        .chain(flag(q(DELETE_FLAG_COLUMN)))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_vals = columns
        .iter()
        .map(|c| format!("__rivet_s.{}", q(c)))
        .chain(flag("FALSE".to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    let upsert = "COALESCE(__rivet_s.__op, '') <> 'delete'";
    let replaced = if deleted_flag {
        format!("{on} AND {upsert}")
    } else {
        on.clone()
    };
    let tombstone = if deleted_flag {
        format!(
            "UPDATE {base} SET {flag_col} = TRUE FROM (\n  {winners}\n) AS __rivet_s \
             WHERE {on} AND __rivet_s.__op = 'delete';\n",
            flag_col = q(DELETE_FLAG_COLUMN)
        )
    } else {
        String::new()
    };
    format!(
        "BEGIN;\n\
         DELETE FROM {base} USING (\n  {winners}\n) AS __rivet_s WHERE {replaced};\n\
         INSERT INTO {base} ({insert_cols})\n\
         SELECT {insert_vals} FROM (\n  {winners}\n) AS __rivet_s WHERE {upsert};\n\
         {tombstone}COMMIT;\n"
    )
}

/// [`compact_merge_sql`] over a [`MergeFilter`].
pub fn compact_merge_filtered_sql(
    base_fqtn: &str,
    changes_fqtn: &str,
    columns: &[&str],
    pk: &[&str],
    order: &CompactOrder,
    filter: &MergeFilter,
    deleted_flag: bool,
) -> String {
    let wh = Warehouse::BigQuery;
    let winners = winners_sql(wh, changes_fqtn, pk, order, &filter.predicate(""));
    let on_keys = key_join(pk);
    let on_bound = match filter {
        MergeFilter::NullKeys(_) => String::new(),
        other => other.predicate("__rivet_t"),
    };
    let flag = |rendered: String| deleted_flag.then_some(rendered);
    let set = columns
        .iter()
        .map(|c| format!("`{c}` = __rivet_s.`{c}`"))
        .chain(flag(format!("`{DELETE_FLAG_COLUMN}` = FALSE")))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_cols = columns
        .iter()
        .map(|c| format!("`{c}`"))
        .chain(flag(format!("`{DELETE_FLAG_COLUMN}`")))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_vals = columns
        .iter()
        .map(|c| format!("__rivet_s.`{c}`"))
        .chain(flag("FALSE".to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    let tombstone = if deleted_flag {
        format!(
            "WHEN MATCHED AND __rivet_s.__op = 'delete' THEN UPDATE SET `{DELETE_FLAG_COLUMN}` = TRUE\n"
        )
    } else {
        "WHEN MATCHED AND __rivet_s.__op = 'delete' THEN DELETE\n".to_string()
    };
    format!(
        "MERGE `{base_fqtn}` AS __rivet_t\n\
         USING (\n\
         \x20 {winners}\n\
         ) AS __rivet_s\n\
         ON {on_keys}{on_bound}\n\
         {tombstone}WHEN MATCHED THEN UPDATE SET {set}\n\
         WHEN NOT MATCHED AND COALESCE(__rivet_s.__op, '') != 'delete' THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )
}

/// Build the current-state dedup view over a `<table>__changes` log for `warehouse`.
pub fn dedup_view_sql(
    warehouse: Warehouse,
    view_fqtn: &str,
    changes_fqtn: &str,
    pk: &[&str],
    engine: SourceEngine,
) -> String {
    let partition = quote_partition(warehouse, pk);
    let order = std::iter::once("__pos IS NOT NULL".to_string())
        .chain(engine.order_exprs(warehouse))
        .map(|e| format!("{e} DESC"))
        .collect::<Vec<_>>()
        .join(", ");
    build_dedup_view(
        warehouse,
        view_fqtn,
        changes_fqtn,
        &partition,
        &order,
        "COALESCE(__op = 'delete', FALSE)",
    )
}

/// The ClickHouse current-state view: `FINAL` across partitions, pinned against a profile (ADR-0035 CH5, CH8).
pub(crate) fn clickhouse_final_view(view_fqtn: &str, changes_fqtn: &str) -> String {
    let wh = Warehouse::ClickHouse;
    format!(
        "CREATE OR REPLACE VIEW {view} AS\n\
         SELECT * EXCEPT (__op, __pos, __seq, __ver),\n\
         \x20      ifNull(__op = 'delete', false) AS {flag}\n\
         FROM {changes} FINAL\n\
         SETTINGS do_not_merge_across_partitions_select_final = 0",
        view = wh.quote_fqtn(view_fqtn),
        changes = wh.quote_fqtn(changes_fqtn),
        flag = DELETE_FLAG_COLUMN,
    )
}

/// Quote each PK column for `warehouse` and join for a `PARTITION BY`.
fn quote_partition(warehouse: Warehouse, pk: &[&str]) -> String {
    pk.iter()
        .map(|c| warehouse.quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The shared current-state view envelope: keep the winning row per PK (`ROW_NUMBER … WHERE __rn = 1`), drop the meta columns from `*`, and project `delete_flag` into [`DELETE_FLAG_COLUMN`].
fn build_dedup_view(
    warehouse: Warehouse,
    view_fqtn: &str,
    changes_fqtn: &str,
    partition: &str,
    order_by: &str,
    delete_flag: &str,
) -> String {
    format!(
        "CREATE OR REPLACE VIEW {view} AS\n\
         SELECT * {except} (__op, __pos, __seq, __rn),\n\
         \x20      {delete_flag} AS {flag}\n\
         FROM (\n\
         \x20 SELECT *, ROW_NUMBER() OVER (\n\
         \x20   PARTITION BY {partition}\n\
         \x20   ORDER BY {order_by}\n\
         \x20 ) AS __rn\n\
         \x20 FROM {changes}\n\
         )\n\
         WHERE __rn = 1;",
        view = warehouse.quote_fqtn(view_fqtn),
        changes = warehouse.quote_fqtn(changes_fqtn),
        except = warehouse.except_keyword(),
        flag = DELETE_FLAG_COLUMN,
    )
}

/// Build the current-state dedup view for an **incremental** load's change log.
fn cursor_order_by(warehouse: Warehouse, cursor_column: &str) -> String {
    let cur = warehouse.quote_ident(cursor_column);
    format!("{cur} IS NOT NULL DESC, {cur} DESC")
}

pub fn inc_dedup_view_sql(
    warehouse: Warehouse,
    view_fqtn: &str,
    changes_fqtn: &str,
    pk: &[&str],
    cursor_column: &str,
) -> String {
    let partition = quote_partition(warehouse, pk);
    let order = cursor_order_by(warehouse, cursor_column);
    build_dedup_view(
        warehouse,
        view_fqtn,
        changes_fqtn,
        &partition,
        &order,
        "FALSE",
    )
}

/// The buffer's range probe answered bounds the compaction cannot read as its key's type.
fn unreadable_range(base: &str, col: &str, lo: &str, hi: &str) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::LOAD_WAREHOUSE_ANSWER_UNREADABLE,
        format!("compact `{base}`: cannot read the buffer's `{col}` range ({lo:?}..{hi:?})"),
    )
}
#[cfg(test)]
mod tests {
    /// The re-baseline's tombstones: each live base row with no buffer row for its key, under its last values.
    #[test]
    fn the_rebaseline_tombstones_are_the_live_base_rows_the_buffer_does_not_hold() {
        let sql = super::absent_as_deleted_sql(
            Warehouse::BigQuery,
            "p.d.t",
            "p.d.t__changes",
            &["id", "v"],
            &["id"],
        );
        assert_eq!(
            sql,
            "INSERT INTO `p.d.t__changes` (`id`, `v`, `__op`)\n\
             SELECT __rivet_t.`id`, __rivet_t.`v`, 'delete'\n\
             FROM `p.d.t` AS __rivet_t\n\
             WHERE NOT IFNULL(__rivet_t.`__is_deleted`, FALSE)\n\
             \x20 AND NOT EXISTS (SELECT 1 FROM `p.d.t__changes` AS __rivet_s WHERE __rivet_t.`id` = __rivet_s.`id`)"
        );
    }

    use super::*;

    const WAREHOUSES: [Warehouse; 2] = [Warehouse::BigQuery, Warehouse::Snowflake];
    const ENGINES: [SourceEngine; 4] = [
        SourceEngine::MySql,
        SourceEngine::Postgres,
        SourceEngine::SqlServer,
        SourceEngine::Mongo,
    ];

    #[test]
    fn every_warehouse_and_engine_orders_by_seq_last_and_drops_meta_columns() {
        for wh in WAREHOUSES {
            for engine in ENGINES {
                let sql = dedup_view_sql(wh, "p.d.orders", "p.d.orders__changes", &["id"], engine);
                assert!(sql.contains("__seq DESC"), "{wh:?}/{engine:?}: {sql}");
                let order = sql.split("ORDER BY").nth(1).unwrap();
                let seq_at = order.find("__seq DESC").unwrap();
                let pos_at = order.find("__pos").unwrap();
                assert!(
                    pos_at < seq_at,
                    "{wh:?}/{engine:?}: __pos must sort before __seq"
                );
                assert!(sql.contains("WHERE __rn = 1;"), "{wh:?}/{engine:?}: {sql}");
                assert!(
                    !sql.contains("!= 'delete'"),
                    "{wh:?}/{engine:?}: deletes must NOT be dropped"
                );
                assert!(
                    sql.contains("COALESCE(__op = 'delete', FALSE) AS __is_deleted"),
                    "{wh:?}/{engine:?}"
                );
                assert!(
                    sql.contains(&format!("PARTITION BY {}", wh.quote_ident("id"))),
                    "{wh:?}/{engine:?}"
                );
            }
        }
    }

    #[test]
    fn bigquery_uses_json_value_and_except() {
        let sql = dedup_view_sql(Warehouse::BigQuery, "v", "c", &["id"], SourceEngine::MySql);
        assert!(sql.contains(
            "CAST(REGEXP_EXTRACT(JSON_VALUE(__pos,'$.file'), r'[0-9]+$') AS INT64) DESC"
        ));
        assert!(sql.contains("CAST(JSON_VALUE(__pos,'$.pos') AS INT64) DESC"));
        assert!(sql.contains("EXCEPT (__op, __pos, __seq, __rn)"));
    }

    #[test]
    fn snowflake_uses_parse_json_and_exclude() {
        let sql = dedup_view_sql(Warehouse::Snowflake, "v", "c", &["id"], SourceEngine::MySql);
        assert!(
            sql.contains(
                "TO_NUMBER(REGEXP_SUBSTR(PARSE_JSON(__pos):file::string, '[0-9]+$')) DESC"
            )
        );
        assert!(sql.contains("PARSE_JSON(__pos):pos::integer DESC"));
        assert!(sql.contains("EXCLUDE (__op, __pos, __seq, __rn)"));
    }

    #[test]
    fn postgres_zero_pads_each_lsn_half_per_dialect() {
        let bq = dedup_view_sql(
            Warehouse::BigQuery,
            "v",
            "c",
            &["id"],
            SourceEngine::Postgres,
        );
        assert!(bq.contains("[OFFSET(0)],8,'0')"));
        assert!(bq.contains("[OFFSET(1)],8,'0')"));
        let sf = dedup_view_sql(
            Warehouse::Snowflake,
            "v",
            "c",
            &["id"],
            SourceEngine::Postgres,
        );
        assert!(sf.contains("SPLIT_PART(PARSE_JSON(__pos):lsn::string,'/',1)"));
        assert!(sf.contains("SPLIT_PART(PARSE_JSON(__pos):lsn::string,'/',2)"));
    }

    #[test]
    fn sqlserver_uses_fixed_width_lsn_directly() {
        let bq = dedup_view_sql(
            Warehouse::BigQuery,
            "v",
            "c",
            &["id"],
            SourceEngine::SqlServer,
        );
        assert!(bq.contains("JSON_VALUE(__pos,'$.lsn') DESC, __seq DESC"));
        let sf = dedup_view_sql(
            Warehouse::Snowflake,
            "v",
            "c",
            &["id"],
            SourceEngine::SqlServer,
        );
        assert!(sf.contains("PARSE_JSON(__pos):lsn::string DESC, __seq DESC"));
    }

    #[test]
    fn mongo_orders_by_cluster_time_ordinal_then_token_and_partitions_by_id() {
        let bq = dedup_view_sql(Warehouse::BigQuery, "v", "c", &["_id"], SourceEngine::Mongo);
        assert!(bq.contains(
            "ORDER BY __pos IS NOT NULL DESC, \
COALESCE(CAST(JSON_VALUE(__pos,'$.ct') AS NUMERIC), -1) DESC, \
COALESCE(CAST(JSON_VALUE(__pos,'$.ord') AS NUMERIC), -1) DESC, \
JSON_VALUE(__pos,'$._data') DESC, __seq DESC"
        ));
        assert!(bq.contains("PARTITION BY `_id`"));
        let sf = dedup_view_sql(
            Warehouse::Snowflake,
            "v",
            "c",
            &["_id"],
            SourceEngine::Mongo,
        );
        assert!(sf.contains(
            "ORDER BY __pos IS NOT NULL DESC, \
COALESCE(PARSE_JSON(__pos):ct::NUMBER(38,0), -1) DESC, \
COALESCE(PARSE_JSON(__pos):ord::NUMBER(38,0), -1) DESC, \
PARSE_JSON(__pos):_data::string DESC, __seq DESC"
        ));
        assert!(sf.contains("COALESCE(__op = 'delete', FALSE) AS __is_deleted"));
    }

    #[test]
    fn mongo_compact_ranks_by_the_same_order_as_the_view() {
        let order = |w| CompactOrder::Cdc(SourceEngine::Mongo).order_by(w);
        assert_eq!(
            order(Warehouse::BigQuery),
            "COALESCE(CAST(JSON_VALUE(__pos,'$.ct') AS NUMERIC), -1) DESC, \
COALESCE(CAST(JSON_VALUE(__pos,'$.ord') AS NUMERIC), -1) DESC, \
JSON_VALUE(__pos,'$._data') DESC, __seq DESC"
        );
        assert_eq!(
            order(Warehouse::Snowflake),
            "__pos IS NOT NULL DESC, \
COALESCE(PARSE_JSON(__pos):ct::NUMBER(38,0), -1) DESC, \
COALESCE(PARSE_JSON(__pos):ord::NUMBER(38,0), -1) DESC, \
PARSE_JSON(__pos):_data::string DESC, __seq DESC"
        );
    }

    #[test]
    fn snapshot_backfill_rows_are_live_and_rank_oldest_on_every_dialect() {
        for wh in WAREHOUSES {
            for engine in ENGINES {
                let sql = dedup_view_sql(wh, "p.d.t", "p.d.t__changes", &["id"], engine);
                assert!(
                    sql.contains("COALESCE(__op = 'delete', FALSE) AS __is_deleted"),
                    "{wh:?}/{engine:?}: NULL __op must read as not-deleted (live): {sql}"
                );
                let order = sql.split("ORDER BY").nth(1).unwrap();
                assert!(
                    order.contains("__pos IS NOT NULL DESC"),
                    "{wh:?}/{engine:?}: NULL __pos must be ranked, not left to engine default: {sql}"
                );
                let guard_at = order.find("__pos IS NOT NULL DESC").unwrap();
                let parse_at = order.find(if wh == Warehouse::BigQuery {
                    "JSON_VALUE"
                } else {
                    "PARSE_JSON"
                });
                if let Some(parse_at) = parse_at {
                    assert!(
                        guard_at < parse_at,
                        "{wh:?}/{engine:?}: null-rank guard must precede the __pos parse: {sql}"
                    );
                }
            }
        }
    }

    #[test]
    fn identifiers_are_backticked_for_bigquery_and_bare_for_snowflake() {
        let bq = dedup_view_sql(
            Warehouse::BigQuery,
            "p.d.orders",
            "p.d.orders__changes",
            &["id"],
            SourceEngine::MySql,
        );
        assert!(bq.contains("VIEW `p.d.orders` AS"));
        assert!(bq.contains("FROM `p.d.orders__changes`"));
        let sf = dedup_view_sql(
            Warehouse::Snowflake,
            "db.sc.orders",
            "db.sc.orders__changes",
            &["id"],
            SourceEngine::MySql,
        );
        assert!(!sf.contains('`'), "snowflake view must not back-tick: {sf}");
        assert!(sf.contains("VIEW db.sc.orders AS"));
        assert!(sf.contains("FROM db.sc.orders__changes"));
    }

    #[test]
    fn inc_dedup_view_orders_by_cursor_and_never_tombstones_on_every_dialect() {
        for wh in WAREHOUSES {
            let sql = inc_dedup_view_sql(
                wh,
                "p.d.orders",
                "p.d.orders__changes",
                &["id"],
                "updated_at",
            );
            assert!(
                sql.contains(&format!("PARTITION BY {}", wh.quote_ident("id"))),
                "{wh:?}: {sql}"
            );
            let cur = wh.quote_ident("updated_at");
            assert!(
                sql.contains(&format!("ORDER BY {cur} IS NOT NULL DESC, {cur} DESC")),
                "{wh:?}: cursor order must guard NULL-baseline first: {sql}"
            );
            assert!(sql.contains("FALSE AS __is_deleted"), "{wh:?}: {sql}");
            assert!(
                !sql.contains("'delete'"),
                "{wh:?}: no CDC delete logic: {sql}"
            );
            let kw = match wh {
                Warehouse::BigQuery | Warehouse::ClickHouse => "EXCEPT",
                Warehouse::Snowflake => "EXCLUDE",
            };
            assert!(
                sql.contains(&format!("{kw} (__op, __pos, __seq, __rn)")),
                "{wh:?}: drops the (reused) CDC meta columns: {sql}"
            );
        }
    }

    #[test]
    fn inc_dedup_view_quotes_identifiers_per_dialect() {
        let bq = inc_dedup_view_sql(
            Warehouse::BigQuery,
            "p.d.o",
            "p.d.o__changes",
            &["id"],
            "ts",
        );
        assert!(bq.contains("VIEW `p.d.o` AS"));
        assert!(bq.contains("FROM `p.d.o__changes`"));
        let sf = inc_dedup_view_sql(
            Warehouse::Snowflake,
            "db.sc.o",
            "db.sc.o__changes",
            &["id"],
            "ts",
        );
        assert!(!sf.contains('`'), "snowflake bare identifiers: {sf}");
        assert!(sf.contains("VIEW db.sc.o AS"));
    }

    #[test]
    fn composite_primary_key_partitions_by_all_columns() {
        let sql = dedup_view_sql(
            Warehouse::BigQuery,
            "v",
            "c",
            &["tenant", "id"],
            SourceEngine::MySql,
        );
        assert!(sql.contains("PARTITION BY `tenant`, `id`"));
    }

    #[test]
    fn identifiers_are_quoted_per_dialect_so_a_reserved_word_column_is_safe() {
        let bq = inc_dedup_view_sql(Warehouse::BigQuery, "v", "c", &["order"], "end");
        assert!(bq.contains("PARTITION BY `order`"), "{bq}");
        assert!(
            bq.contains("ORDER BY `end` IS NOT NULL DESC, `end` DESC"),
            "{bq}"
        );
        let sf = inc_dedup_view_sql(Warehouse::Snowflake, "v", "c", &["order"], "end");
        assert!(sf.contains("PARTITION BY order"), "{sf}");
        assert!(
            sf.contains("ORDER BY end IS NOT NULL DESC, end DESC"),
            "{sf}"
        );
        let cdc = dedup_view_sql(
            Warehouse::BigQuery,
            "v",
            "c",
            &["a", "b"],
            SourceEngine::MySql,
        );
        assert!(cdc.contains("PARTITION BY `a`, `b`"), "{cdc}");
    }

    /// ClickHouse's current-state view reads the collapsing log with FINAL and flags deletes.
    #[test]
    fn the_clickhouse_view_reads_final_and_flags_deletes() {
        let sql = clickhouse_final_view("d.orders", "d.orders__changes");
        assert_eq!(
            sql,
            "CREATE OR REPLACE VIEW `d`.`orders` AS\nSELECT * EXCEPT (__op, __pos, __seq, __ver),\n       \
             ifNull(__op = 'delete', false) AS __is_deleted\nFROM `d`.`orders__changes` FINAL\n\
             SETTINGS do_not_merge_across_partitions_select_final = 0"
        );
        assert_eq!(
            [
                Warehouse::BigQuery,
                Warehouse::Snowflake,
                Warehouse::ClickHouse
            ]
            .map(Warehouse::label),
            ["BigQuery", "Snowflake", "ClickHouse"]
        );
    }

    #[test]
    fn meta_column_specs_are_typed_per_warehouse_and_ordered() {
        let bq = meta_column_specs(Warehouse::BigQuery);
        let names: Vec<&str> = bq.iter().map(|s| s.column_name.as_str()).collect();
        assert_eq!(
            names,
            ["__op", "__pos", "__seq"],
            "meta columns lead the schema, in order"
        );
        assert_eq!(bq[0].target_type, "STRING");
        assert_eq!(bq[2].target_type, "INT64");
        let sf = meta_column_specs(Warehouse::Snowflake);
        assert_eq!(sf[1].target_type, "VARCHAR");
        assert_eq!(sf[2].target_type, "INTEGER");
    }
}

/// What the compaction probe read from the buffer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CompactProbe {
    pub rows: u64,
    pub lo: String,
    pub hi: String,
    pub nulls: u64,
    /// A time key's min/max inside [`PARTITIONED_DAYS`], or [`NOTHING_INSIDE`].
    pub in_lo: String,
    pub in_hi: String,
}

/// What every compaction MERGE is built from.
pub(crate) fn merge_inputs<'a>(
    specs: &'a [TargetColumnSpec],
    pk: &'a [String],
) -> (Vec<&'a str>, Vec<&'a str>, bool) {
    let columns = specs
        .iter()
        .map(|s| s.column_name.as_str())
        .filter(|c| !is_meta_column(c) && *c != DELETE_FLAG_COLUMN)
        .collect();
    let pk_refs = pk.iter().map(String::as_str).collect();
    let deleted_flag = specs.iter().any(|s| s.column_name == DELETE_FLAG_COLUMN);
    (columns, pk_refs, deleted_flag)
}

/// The MERGE statements one compaction runs, decided from the probe alone.
pub fn plan_compact_merges(
    base: &str,
    changes_fqtn: &str,
    specs: &[TargetColumnSpec],
    pk: &[String],
    order: impl Into<CompactOrder>,
    key: Option<&crate::load::plan::PartitionKey>,
    probe: &CompactProbe,
) -> anyhow::Result<Vec<String>> {
    use crate::load::plan::{Granularity, PartitionKey};
    let order = order.into();
    if probe.rows == 0 {
        return Ok(Vec::new());
    }
    let (columns, pk_refs, deleted_flag) = merge_inputs(specs, pk);
    let merge = |bound: Option<&RangeBound>, nulls_only: Option<&str>| {
        compact_merge_sql(
            base,
            changes_fqtn,
            &columns,
            &pk_refs,
            order.clone(),
            bound,
            nulls_only,
            deleted_flag,
        )
    };
    let part_col = key.and_then(PartitionKey::column);
    let mut merges = Vec::new();
    match (key, part_col) {
        (Some(PartitionKey::Time { granularity, .. }), Some(col)) if !probe.lo.is_empty() => {
            let ty = &column_type_of(specs, col);
            let step = match granularity {
                Granularity::Hour => 166,
                Granularity::Day => 4000,
                Granularity::Month => 4000 * 28,
                Granularity::Year => 4000 * 365,
            };
            let parse = |d: &str| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d");
            let (Ok(lo_d), Ok(hi_d)) = (parse(&probe.lo), parse(&probe.hi)) else {
                return Err(unreadable_range(base, col, &probe.lo, &probe.hi));
            };
            let upper = |d: chrono::NaiveDate| {
                if chrono::Datelike::year(&d) > 9999 {
                    String::new()
                } else {
                    time_literal(ty, d)
                }
            };
            let all_lo = time_literal(ty, lo_d);
            let past_hi = hi_d + chrono::Duration::days(1);
            let all_hi = upper(past_hi);
            let (Ok(from_d), Ok(until_d)) = (parse(PARTITIONED_DAYS.0), parse(PARTITIONED_DAYS.1))
            else {
                unreachable!("PARTITIONED_DAYS are dates");
            };
            let mut windows = Vec::new();
            if lo_d < from_d {
                windows.push((lo_d, past_hi.min(from_d)));
            }
            let inside = match (parse(&probe.in_lo), parse(&probe.in_hi)) {
                (Ok(a), Ok(b)) => Some((a, b)),
                _ if probe.in_lo == NOTHING_INSIDE && probe.in_hi == NOTHING_INSIDE => None,
                _ => {
                    let (a, b) = (
                        lo_d.max(from_d),
                        hi_d.min(until_d - chrono::Duration::days(1)),
                    );
                    (a <= b).then_some((a, b))
                }
            };
            if let Some((a, b)) = inside {
                windows.extend(day_windows(a, b, step));
            }
            if hi_d >= until_d {
                windows.push((lo_d.max(until_d), past_hi));
            }
            for (from, to) in windows {
                let bound = RangeBound {
                    column: col.to_string(),
                    lo: time_literal(ty, from),
                    hi_exclusive: upper(to),
                    all_lo: all_lo.clone(),
                    all_hi_exclusive: all_hi.clone(),
                };
                merges.push(merge(Some(&bound), None));
            }
            if probe.nulls > 0 {
                merges.push(merge(None, Some(col)));
            }
        }
        (
            Some(PartitionKey::Range {
                interval,
                start,
                end,
                ..
            }),
            Some(col),
        ) if !probe.lo.is_empty() => {
            let (Ok(lo_i), Ok(hi_i)) = (probe.lo.parse::<i64>(), probe.hi.parse::<i64>()) else {
                return Err(unreadable_range(base, col, &probe.lo, &probe.hi));
            };
            if lo_i < *start || hi_i >= *end {
                merges.push(merge(None, None));
                return Ok(merges);
            }
            let step = interval.saturating_mul(4000).max(1);
            let all_hi = hi_i
                .checked_add(1)
                .map_or_else(String::new, |h| h.to_string());
            let mut from = lo_i;
            loop {
                let to = from.checked_add(step);
                let bound = RangeBound {
                    column: col.to_string(),
                    lo: from.to_string(),
                    hi_exclusive: to.map_or_else(String::new, |t| t.to_string()),
                    all_lo: lo_i.to_string(),
                    all_hi_exclusive: all_hi.clone(),
                };
                merges.push(merge(Some(&bound), None));
                match to {
                    Some(t) if t <= hi_i => from = t,
                    _ => break,
                }
            }
            if probe.nulls > 0 {
                merges.push(merge(None, Some(col)));
            }
        }
        _ => merges.push(merge(None, None)),
    }
    Ok(merges)
}

/// Partitions one MERGE statement may touch (BigQuery's cap per DML statement).
pub const MERGE_PARTITION_CAP: usize = 4000;

/// Rename the LIVE buffer out of the way as the script's first act.
#[derive(Clone, Copy)]
pub struct CompactRename<'a> {
    /// The live buffer, fully qualified — what the script renames AWAY.
    pub from_fqtn: &'a str,
    /// The bare table name it becomes.
    pub to_bare: &'a str,
}

/// The `ALTER TABLE … RENAME TO …` line, or nothing on the recovery pass.
fn rename_stmt(rename: Option<CompactRename<'_>>) -> String {
    rename.map_or_else(String::new, |r| {
        format!("ALTER TABLE `{}` RENAME TO `{}`;\n", r.from_fqtn, r.to_bare)
    })
}

/// ONE multi-statement job that compacts a base whose partition key is a DAY column.
pub fn compact_script_sql(
    base_fqtn: &str,
    changes_fqtn: &str,
    rename: Option<CompactRename<'_>>,
    specs: &[TargetColumnSpec],
    pk: &[String],
    order: impl Into<CompactOrder>,
    day_column: Option<&str>,
) -> String {
    let order = order.into();
    let buffer_only = trusts_buffer_days(&order);
    let rename = rename_stmt(rename);
    let (columns, pk_refs, deleted_flag) = merge_inputs(specs, pk);
    let merge = |filter: &MergeFilter| {
        compact_merge_filtered_sql(
            base_fqtn,
            changes_fqtn,
            &columns,
            &pk_refs,
            &order,
            filter,
            deleted_flag,
        )
    };
    let Some(col) = day_column else {
        return format!(
            "DECLARE n INT64 DEFAULT 0;\n\
             {rename}\
             SET n = (SELECT COUNT(*) FROM `{changes_fqtn}`);\n\
             IF n > 0 THEN\n{merge_all}\nEND IF;\n\
             DROP TABLE `{changes_fqtn}`;\n\
             SELECT n AS changes_rows, IF(n > 0, 1, 0) AS merge_jobs;",
            merge_all = merge(&MergeFilter::All)
        );
    };
    let ty = &column_type_of(specs, col);
    let by_days = merge(&MergeFilter::Days {
        column: col.to_string(),
        column_type: ty.clone(),
        chunk: "chunk".to_string(),
        all: "days".to_string(),
    });
    let null_keys = merge(&MergeFilter::NullKeys(col.to_string()));
    let key0 = pk_refs.first().filter(|_| !buffer_only).and_then(|k| {
        specs
            .iter()
            .find(|s| s.column_name == *k)
            .and_then(|s| key_array_type(&s.target_type))
            .map(|t| (*k, t))
    });
    let key_decls = key0.map_or_else(String::new, |(_, t)| {
        format!("DECLARE moved_bytes INT64 DEFAULT 0;\nDECLARE moved ARRAY<{t}> DEFAULT [];\n")
    });
    let touched = if buffer_only {
        format!(
            "SET days = (SELECT IFNULL(ARRAY_AGG(DISTINCT {} IGNORE NULLS), []) FROM `{changes_fqtn}`);",
            date_of(&format!("`{col}`"), ty)
        )
    } else {
        touched_days_script(changes_fqtn, base_fqtn, &pk_refs, col, ty, key0)
    };
    format!(
        "DECLARE n INT64 DEFAULT 0;\n\
         DECLARE null_keys INT64 DEFAULT 0;\n\
         DECLARE days ARRAY<DATE> DEFAULT [];\n\
         DECLARE chunk ARRAY<DATE>;\n\
         DECLARE i INT64 DEFAULT 0;\n\
         DECLARE jobs INT64 DEFAULT 0;\n\
         {key_decls}\
         {rename}\
         SET (n, null_keys) = (SELECT AS STRUCT COUNT(*), COUNTIF(`{col}` IS NULL) FROM `{changes_fqtn}`);\n\
         {touched}\n\
         WHILE i < ARRAY_LENGTH(days) DO\n\
         \x20 SET chunk = ARRAY(SELECT d FROM UNNEST(days) AS d WITH OFFSET AS o WHERE o >= i AND o < i + {cap});\n\
         {by_days}\n\
         \x20 SET i = i + {cap};\n\
         \x20 SET jobs = jobs + 1;\n\
         END WHILE;\n\
         IF null_keys > 0 THEN\n{null_keys_merge}\n\x20 SET jobs = jobs + 1;\nEND IF;\n\
         DROP TABLE `{changes_fqtn}`;\n\
         SELECT n AS changes_rows, jobs AS merge_jobs;",
        cap = MERGE_PARTITION_CAP,
        null_keys_merge = null_keys,
    )
}

/// Which compaction a base's partition key gets: one scripted job, or windowed MERGEs then a DROP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactArm<'a> {
    /// One scripted job; `day_column` is the DATE-pruned column, `None` for an unbounded MERGE.
    Scripted { day_column: Option<&'a str> },
    /// Hour/month/year or integer-range keys: probe, one MERGE per window, then DROP.
    Windowed,
}

impl<'a> CompactArm<'a> {
    /// The day column a recovered leftover's script prunes by; unbounded off the scripted arm.
    pub fn recovery_day_column(self) -> Option<&'a str> {
        match self {
            CompactArm::Scripted { day_column } => day_column,
            CompactArm::Windowed => None,
        }
    }
}

/// Picks the compaction arm for a base partitioned on `key` (unpartitioned or a load date prunes nothing).
pub fn compact_arm(key: Option<&crate::load::plan::PartitionKey>) -> CompactArm<'_> {
    use crate::load::plan::{Granularity, PartitionKey};
    match key {
        None => CompactArm::Scripted { day_column: None },
        Some(k) if k.is_load_date() => CompactArm::Scripted { day_column: None },
        Some(PartitionKey::Time {
            column,
            granularity: Granularity::Day,
        }) => CompactArm::Scripted {
            day_column: column.as_deref(),
        },
        Some(_) => CompactArm::Windowed,
    }
}

#[cfg(test)]
mod compact_tests {
    use super::*;
    use crate::load::plan::{Granularity, PartitionKey};

    #[test]
    fn a_key_type_gets_an_array_element_type_only_when_in_unnest_covers_it() {
        for (t, want) in [
            (BqType::Int64, Some("INT64")),
            (BqType::String, Some("STRING")),
            (BqType::Numeric, Some("NUMERIC")),
            (BqType::BigNumeric, Some("BIGNUMERIC")),
            (BqType::Timestamp, Some("TIMESTAMP")),
            (BqType::Date, Some("DATE")),
            (BqType::DateTime, Some("DATETIME")),
            (BqType::Bool, None),
            (BqType::Float64, None),
            (BqType::Time, None),
            (BqType::Bytes, None),
            (BqType::Json, None),
            (BqType::Array(Box::new(BqType::Int64)), None),
        ] {
            assert_eq!(key_array_type(&t.clone().into()), want, "{t}");
        }
        assert_eq!(
            key_array_type(&SfType::NumberPs(38, 0).into()),
            None,
            "only a BigQuery type declares a BigQuery script variable"
        );
        let s = compact_script_sql(
            "p.d.t",
            "p.d.t__changes",
            None,
            &[
                meta_spec("id", BqType::Bytes.into()),
                meta_spec("v", BqType::Int64.into()),
                meta_spec("created_at", BqType::DateTime.into()),
            ],
            &["id".to_string()],
            SourceEngine::Postgres,
            Some("created_at"),
        );
        assert!(
            !s.contains("moved") && s.contains("IF EXISTS (SELECT 1 FROM `p.d.t__changes` AS __rivet_s WHERE IFNULL(__rivet_s.__op, '') != 'insert'"),
            "a key an array cannot hold still looks moved keys up, by the semi-join: {s}"
        );
    }

    #[test]
    fn compact_arm_scripts_day_and_unprunable_keys_and_windows_the_rest() {
        let time = |c: Option<&str>, g| PartitionKey::Time {
            column: c.map(str::to_string),
            granularity: g,
        };
        let scripted = |d| CompactArm::Scripted { day_column: d };
        assert_eq!(compact_arm(None), scripted(None));
        assert_eq!(
            compact_arm(Some(&time(None, Granularity::Hour))),
            scripted(None),
            "load time"
        );
        assert_eq!(
            compact_arm(Some(&time(
                Some(crate::enrich::COL_EXPORTED_AT),
                Granularity::Month
            ))),
            scripted(None),
            "extraction stamp"
        );
        assert_eq!(
            compact_arm(Some(&time(Some("ts"), Granularity::Day))),
            scripted(Some("ts"))
        );
        for g in [Granularity::Hour, Granularity::Month, Granularity::Year] {
            assert_eq!(
                compact_arm(Some(&time(Some("ts"), g))),
                CompactArm::Windowed
            );
        }
        let range = PartitionKey::Range {
            column: "b".into(),
            start: 0,
            end: 100,
            interval: 10,
        };
        assert_eq!(compact_arm(Some(&range)), CompactArm::Windowed);
    }

    #[test]
    fn a_recovered_leftover_merges_unbounded_off_the_scripted_arm() {
        assert_eq!(CompactArm::Windowed.recovery_day_column(), None);
        assert_eq!(
            CompactArm::Scripted {
                day_column: Some("ts")
            }
            .recovery_day_column(),
            Some("ts")
        );
    }

    /// The script renames the LIVE buffer away first, and never drops it by name.
    #[test]
    fn the_compaction_script_renames_the_live_buffer_away_before_it_merges() {
        let script = |rename| {
            compact_script_sql(
                "p.d.t",
                "p.d.t__changes__merging",
                rename,
                &specs(),
                &["id".to_string()],
                SourceEngine::MySql,
                Some("created_at"),
            )
        };
        let renaming = script(Some(CompactRename {
            from_fqtn: "p.d.t__changes",
            to_bare: "t__changes__merging",
        }));

        assert!(
            renaming.contains("ALTER TABLE `p.d.t__changes` RENAME TO `t__changes__merging`;"),
            "the live buffer is renamed away: {renaming}"
        );
        assert_eq!(
            renaming.matches("`p.d.t__changes`").count(),
            1,
            "the LIVE name may appear only in the rename — every later statement must \
             work on the merging table, or an abandoned job still destroys a buffer a \
             concurrent load has refilled: {renaming}"
        );
        assert!(
            renaming.contains("DROP TABLE `p.d.t__changes__merging`;"),
            "and the DROP takes the renamed table: {renaming}"
        );
        let alter = renaming.find("ALTER TABLE").expect("the rename");
        for stmt in ["SET (n, null_keys)", "MERGE", "DROP TABLE"] {
            assert!(
                renaming.find(stmt).expect(stmt) > alter,
                "`{stmt}` must come after the rename: {renaming}"
            );
        }

        let recovering = script(None);
        assert!(
            !recovering.contains("ALTER TABLE"),
            "a recovery pass merges an ALREADY-renamed table: {recovering}"
        );
        assert!(
            recovering.contains("DROP TABLE `p.d.t__changes__merging`;"),
            "and still drops it when done: {recovering}"
        );
    }

    /// The day-key script: one job — probe, chunked `IN UNNEST(chunk)` MERGEs (≤ 4,000 days each, the base pruned to exactly those days), the NULL-keyed pass, the DROP, and the `(changes_rows, merge_jobs)` row last.
    #[test]
    fn the_compaction_script_probes_merges_by_day_list_and_drops_in_one_job() {
        let s = compact_script_sql(
            "p.d.t",
            "p.d.t__changes",
            None,
            &specs(),
            &["id".to_string()],
            SourceEngine::Postgres,
            Some("created_at"),
        );
        assert!(
            s.contains(
                "SET (n, null_keys) = (SELECT AS STRUCT COUNT(*), COUNTIF(`created_at` IS NULL) FROM `p.d.t__changes`);"
            ),
            "ONE count statement over the buffer — every statement that reads a table is \
             billed a 10 MB floor: {s}"
        );
        assert!(
            s.contains(
                "SET days = (SELECT IFNULL(ARRAY_AGG(DISTINCT DATE(`created_at`) IGNORE NULLS), []) FROM `p.d.t__changes`);"
            ),
            "the days start as the buffer's own: {s}"
        );
        let unmatched = "FROM `p.d.t__changes` AS __rivet_s WHERE IFNULL(__rivet_s.__op, '') != 'insert' AND NOT EXISTS \
                         (SELECT 1 FROM `p.d.t` AS __rivet_t WHERE __rivet_t.`id` = __rivet_s.`id` AND \
                         (DATE(__rivet_t.`created_at`) IN UNNEST(days) OR __rivet_t.`created_at` IS NULL))";
        assert!(
            s.contains(&format!(
                "SET moved_bytes = (SELECT IFNULL(SUM(BYTE_LENGTH(CAST(__rivet_s.`id` AS STRING)) + 16), 0) {unmatched});"
            )),
            "only an updated/deleted key whose base row lies outside the buffer's days is looked up, \
             and the check reads the base pruned to those days: {s}"
        );
        assert!(
            s.contains("DECLARE moved ARRAY<INT64> DEFAULT [];")
                && s.contains("IF moved_bytes <= 700000 THEN")
                && s.contains("WHERE __rivet_t.`id` IN UNNEST(moved) AND EXISTS"),
            "a small moved set is found through the key's clustering, never a base scan: {s}"
        );
        assert!(
            s.contains(
                "WHERE EXISTS (SELECT 1 FROM `p.d.t__changes` AS __rivet_s WHERE __rivet_t.`id` = __rivet_s.`id` AND IFNULL(__rivet_s.__op, '') != 'insert')"
            ),
            "a moved set too large for a variable falls back to the semi-join: {s}"
        );
        assert!(s.contains("WHILE i < ARRAY_LENGTH(days) DO"), "{s}");
        assert!(s.contains("WHERE o >= i AND o < i + 4000"), "{s}");
        assert!(
            s.contains(") WHERE __rn = 1 AND DATE(`created_at`) IN UNNEST(chunk)")
                && s.contains(
                    "ON __rivet_t.`id` = __rivet_s.`id` AND (DATE(__rivet_t.`created_at`) IN UNNEST(days) OR __rivet_t.`created_at` IS NULL)"
                ),
            "winners filtered by the chunk, the base pruned to EVERY touched day plus the \
             NULL partition — never to the chunk alone: {s}"
        );
        assert!(
            s.contains("IF null_keys > 0 THEN")
                && s.contains(") WHERE __rn = 1 AND `created_at` IS NULL")
                && s.contains(
                    "ON __rivet_t.`id` = __rivet_s.`id`\nWHEN MATCHED AND __rivet_s.__op = 'delete'"
                ),
            "NULL-keyed winners merge by key alone: {s}"
        );
        let drop = s.find("DROP TABLE `p.d.t__changes`;").expect("the drop");
        let last_merge = s.rfind("MERGE `p.d.t`").expect("a merge");
        assert!(last_merge < drop, "every MERGE precedes the DROP: {s}");
        assert!(
            s.trim_end()
                .ends_with("SELECT n AS changes_rows, jobs AS merge_jobs;"),
            "{s}"
        );

        let own = compact_script_sql(
            "p.d.t",
            "p.d.t__changes",
            None,
            &specs(),
            &["id".to_string()],
            SourceEngine::MySql,
            Some("created_at"),
        );
        assert!(
            own.contains(
                "SET days = (SELECT IFNULL(ARRAY_AGG(DISTINCT DATE(`created_at`) IGNORE NULLS), []) FROM `p.d.t__changes`);\nWHILE"
            ) && !own.contains("moved")
                && own.matches("FROM `p.d.t` AS __rivet_t").count() == 0,
            "a MySQL stream refuses a partition move, so its days are the buffer's alone and \
             only the MERGE reads the base: {own}"
        );

        let plain = compact_script_sql(
            "p.d.t",
            "p.d.t__changes",
            None,
            &specs(),
            &["id".to_string()],
            SourceEngine::MySql,
            None,
        );
        assert!(
            plain.contains("IF n > 0 THEN\nMERGE `p.d.t` AS __rivet_t"),
            "{plain}"
        );
        assert!(
            plain.contains("ON __rivet_t.`id` = __rivet_s.`id`\n"),
            "unbounded: {plain}"
        );
        assert!(!plain.contains("UNNEST"), "{plain}");
        assert!(
            plain.ends_with("SELECT n AS changes_rows, IF(n > 0, 1, 0) AS merge_jobs;"),
            "{plain}"
        );

        let utc = compact_script_sql(
            "p.d.t",
            "p.d.t__changes",
            None,
            &[
                meta_spec("id", BqType::Int64.into()),
                meta_spec("created_at", BqType::Timestamp.into()),
            ],
            &["id".to_string()],
            SourceEngine::Postgres,
            Some("created_at"),
        );
        assert!(
            utc.contains("SET days = (SELECT IFNULL(ARRAY_AGG(DISTINCT DATE(`created_at`, 'UTC') IGNORE NULLS), []) FROM `p.d.t__changes`);")
                && utc.contains("(DATE(__rivet_t.`created_at`, 'UTC') IN UNNEST(days) OR __rivet_t.`created_at` IS NULL))")
                && utc.contains(") WHERE __rn = 1 AND DATE(`created_at`, 'UTC') IN UNNEST(chunk)")
                && utc.contains("ON __rivet_t.`id` = __rivet_s.`id` AND (DATE(__rivet_t.`created_at`, 'UTC') IN UNNEST(days) OR __rivet_t.`created_at` IS NULL)"),
            "{utc}"
        );

        let hostile = compact_script_sql(
            "p.d.my-orders",
            "p.d.my-orders__changes",
            None,
            &[
                meta_spec("order", BqType::Int64.into()),
                meta_spec("created_at", BqType::Date.into()),
            ],
            &["order".to_string()],
            SourceEngine::Postgres,
            Some("created_at"),
        );
        assert!(
            hostile.contains("MERGE `p.d.my-orders` AS __rivet_t"),
            "{hostile}"
        );
        assert!(
            hostile.contains("PARTITION BY `order` ORDER BY"),
            "{hostile}"
        );
        assert!(
            hostile.contains(
                "ON __rivet_t.`order` = __rivet_s.`order` AND (DATE(__rivet_t.`created_at`)"
            ),
            "{hostile}"
        );
        assert!(
            hostile.contains("DROP TABLE `p.d.my-orders__changes`;"),
            "{hostile}"
        );
        assert!(
            !hostile.contains(" order "),
            "never a bare reserved word: {hostile}"
        );
    }

    /// A probe that does not say where its values lie inside the partitioned days.
    fn probe(rows: u64, lo: &str, hi: &str, nulls: u64) -> CompactProbe {
        CompactProbe {
            rows,
            lo: lo.into(),
            hi: hi.into(),
            nulls,
            in_lo: String::new(),
            in_hi: String::new(),
        }
    }

    fn specs() -> Vec<TargetColumnSpec> {
        vec![
            meta_spec("id", BqType::Int64.into()),
            meta_spec("v", BqType::Int64.into()),
            meta_spec("created_at", BqType::DateTime.into()),
            meta_spec("__is_deleted", BqType::Bool.into()),
        ]
    }

    fn plan(key: Option<&PartitionKey>, p: &CompactProbe) -> Vec<String> {
        plan_compact_merges(
            "p.d.t",
            "p.d.t__changes",
            &specs(),
            &["id".to_string()],
            SourceEngine::MySql,
            key,
            p,
        )
        .expect("a plan")
    }

    /// An empty buffer plans nothing; no key plans one unbounded MERGE.
    #[test]
    fn the_compaction_plan_follows_the_probe_and_the_key() {
        let day = PartitionKey::Time {
            column: Some("created_at".into()),
            granularity: Granularity::Day,
        };
        assert!(
            plan(Some(&day), &probe(0, "", "", 0)).is_empty(),
            "nothing to merge"
        );
        let all_null_keys = plan(Some(&day), &probe(3, "", "", 3));
        assert_eq!(
            all_null_keys.len(),
            1,
            "a key with no range (every row's key NULL) merges once, unbounded"
        );
        assert!(
            all_null_keys[0].contains("ON __rivet_t.`id` = __rivet_s.`id`\n"),
            "{}",
            all_null_keys[0]
        );
        let hour = PartitionKey::Time {
            column: Some("created_at".into()),
            granularity: Granularity::Hour,
        };
        assert_eq!(
            plan(Some(&hour), &probe(4, "2024-01-01", "2024-07-18", 0)).len(),
            2,
            "hourly partitions: 4,000 of them are 166 days, so 200 days take two windows"
        );
        let month = PartitionKey::Time {
            column: Some("created_at".into()),
            granularity: Granularity::Month,
        };
        assert_eq!(
            plan(Some(&month), &probe(4, "1990-01-01", "2026-09-17", 0)).len(),
            1,
            "monthly partitions never reach the cap: one window"
        );

        let none = plan(None, &probe(7, "", "", 0));
        assert_eq!(none.len(), 1);
        assert!(
            none[0].contains("ON __rivet_t.`id` = __rivet_s.`id`\n"),
            "unbounded: {}",
            none[0]
        );
        assert!(
            none[0].contains("SET `id` = __rivet_s.`id`, `v` = __rivet_s.`v`, `created_at` = __rivet_s.`created_at`, `__is_deleted` = FALSE"),
            "the flag is set, never copied from the buffer: {}",
            none[0]
        );

        let two = plan(Some(&day), &probe(9, "2000-01-01", "2013-09-08", 0));
        assert_eq!(two.len(), 2, "5,000 days → two windows of ≤ 4,000");
        assert!(
            two[0].contains("WHERE __rn = 1 AND `created_at` >= DATETIME '2000-01-01T00:00:00' AND `created_at` < DATETIME '2010-12-14T00:00:00'"),
            "the winners of the first window: {}",
            two[0]
        );
        assert!(
            two[1].contains("WHERE __rn = 1 AND `created_at` >= DATETIME '2010-12-14T00:00:00'"),
            "{}",
            two[1]
        );
        let base_bound = "ON __rivet_t.`id` = __rivet_s.`id` AND (__rivet_t.`created_at` >= DATETIME '2000-01-01T00:00:00' AND __rivet_t.`created_at` < DATETIME '2013-09-09T00:00:00' OR __rivet_t.`created_at` IS NULL)";
        assert!(
            two[0].contains(base_bound) && two[1].contains(base_bound),
            "{}\n{}",
            two[0],
            two[1]
        );

        let with_nulls = plan(Some(&day), &probe(9, "2024-01-01", "2024-01-02", 3));
        assert_eq!(with_nulls.len(), 2, "one window + the NULL set");
        assert!(
            with_nulls[1].contains("WHERE __rn = 1 AND `created_at` IS NULL"),
            "{}",
            with_nulls[1]
        );

        let range = PartitionKey::Range {
            column: "bucket".into(),
            start: 0,
            end: 1_000_000,
            interval: 10,
        };
        let stepped = plan(Some(&range), &probe(5, "0", "45000", 1));
        assert_eq!(
            stepped.len(),
            3,
            "45,001 keys at 10 × 4,000 per job → two windows + NULLs"
        );
        assert!(
            stepped[0].contains("`bucket` >= 0 AND `bucket` < 40000"),
            "{}",
            stepped[0]
        );
        assert!(
            stepped[1].contains("`bucket` >= 40000 AND `bucket` < 80000"),
            "{}",
            stepped[1]
        );

        let stray = plan(Some(&range), &probe(5, "0", "4000000000", 0));
        assert_eq!(stray.len(), 1, "{stray:?}");
        assert!(
            stray[0].contains("ON __rivet_t.`id` = __rivet_s.`id`\n"),
            "{}",
            stray[0]
        );

        let eot = plan(Some(&month), &probe(2, "9999-01-01", "9999-12-31", 0));
        assert_eq!(eot.len(), 1, "{eot:?}");
        assert!(
            !eot[0].contains("10000")
                && eot[0].contains("WHERE __rn = 1 AND `created_at` >= DATETIME '9999-01-01T00:00:00'\n")
                && eot[0].contains("ON __rivet_t.`id` = __rivet_s.`id` AND (__rivet_t.`created_at` >= DATETIME '9999-01-01T00:00:00' OR __rivet_t.`created_at` IS NULL)"),
            "no upper bound past the last day: {}",
            eot[0]
        );

        let long_month = plan(Some(&month), &probe(4, "1600-01-01", "2000-01-01", 0));
        assert_eq!(long_month.len(), 2, "146,000 days at 112,000 per window");
        assert_eq!(plan(Some(&range), &probe(5, "0", "45000", 0)).len(), 2);
        let range_nulls = plan(Some(&range), &probe(2, "", "", 2));
        assert_eq!(range_nulls.len(), 1);
        assert!(
            range_nulls[0].contains("ON __rivet_t.`id` = __rivet_s.`id`\n"),
            "{}",
            range_nulls[0]
        );

        let err = plan_compact_merges(
            "p.d.t",
            "p.d.t__changes",
            &specs(),
            &["id".to_string()],
            SourceEngine::MySql,
            Some(&day),
            &probe(1, "not-a-date", "x", 0),
        )
        .unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_LOAD_WAREHOUSE_ANSWER_UNREADABLE")
        );
        let err = err.to_string();
        assert!(
            err.contains("cannot read the buffer's `created_at` range"),
            "{err}"
        );
        let err = plan_compact_merges(
            "p.d.t",
            "p.d.t__changes",
            &specs(),
            &["id".to_string()],
            SourceEngine::MySql,
            Some(&range),
            &probe(1, "low", "high", 0),
        )
        .unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_LOAD_WAREHOUSE_ANSWER_UNREADABLE")
        );
    }

    /// A time key's value outside the partitioned days sits in the one unpartitioned partition.
    #[test]
    fn a_time_key_outside_the_partitioned_days_is_one_window_per_side() {
        let hour = PartitionKey::Time {
            column: Some("created_at".into()),
            granularity: Granularity::Hour,
        };
        let probe = |lo: &str, hi: &str, in_lo: &str, in_hi: &str| CompactProbe {
            rows: 2,
            lo: lo.into(),
            hi: hi.into(),
            nulls: 0,
            in_lo: in_lo.into(),
            in_hi: in_hi.into(),
        };
        let eot = plan(
            Some(&hour),
            &probe("2026-10-01", "9999-12-31", "2026-10-01", "2026-10-15"),
        );
        assert_eq!(eot.len(), 2, "the days inside and the sentinel");
        assert!(
            eot[0].contains("WHERE __rn = 1 AND `created_at` >= DATETIME '2026-10-01T00:00:00' AND `created_at` < DATETIME '2026-10-16T00:00:00'")
                && eot[1].contains("WHERE __rn = 1 AND `created_at` >= DATETIME '2160-01-01T00:00:00'\n"),
            "{eot:?}"
        );
        let outside = plan(
            Some(&hour),
            &probe("0001-01-01", "9999-12-31", "none", "none"),
        );
        assert_eq!(outside.len(), 2, "nothing inside: one window per side");
        assert!(
            outside[0].contains("`created_at` >= DATETIME '0001-01-01T00:00:00' AND `created_at` < DATETIME '1960-01-01T00:00:00'")
                && outside[1].contains("`created_at` >= DATETIME '2160-01-01T00:00:00'\n"),
            "{outside:?}"
        );
        let below = plan(
            Some(&hour),
            &probe("1900-01-01", "2024-07-18", "2024-01-01", "2024-07-18"),
        );
        assert_eq!(below.len(), 3, "one window below, 200 days inside in two");
        for merge in &outside {
            assert!(
                merge.contains("ON __rivet_t.`id` = __rivet_s.`id` AND (__rivet_t.`created_at` >= DATETIME '0001-01-01T00:00:00' OR __rivet_t.`created_at` IS NULL)"),
                "the base stays bounded by the whole touched span: {merge}"
            );
        }
        let half = plan(Some(&hour), &probe("2026-10-01", "9999-12-31", "none", ""));
        assert_eq!(
            half.len(),
            295,
            "one side read is not a reading: the clamped span"
        );
        let unread = plan(Some(&hour), &probe("2026-10-01", "9999-12-31", "", ""));
        assert_eq!(
            unread.len(),
            295,
            "48,669 days in 166-day steps, then the sentinel"
        );
        assert!(
            unread[293].contains("AND `created_at` < DATETIME '2160-01-01T00:00:00'")
                && unread[294].contains("`created_at` >= DATETIME '2160-01-01T00:00:00'\n"),
            "{}\n{}",
            unread[293],
            unread[294]
        );
    }

    fn bound(lo: &str, hi: &str) -> RangeBound {
        RangeBound {
            column: "created_at".into(),
            lo: lo.into(),
            hi_exclusive: hi.into(),
            all_lo: lo.into(),
            all_hi_exclusive: hi.into(),
        }
    }

    /// The MERGE upserts the latest change per key, flags a tombstone instead of deleting, un-flags a re-insert, never inserts a delete.
    #[test]
    fn compact_merge_flags_deletes_and_prunes_both_sides_by_constants() {
        let sql = compact_merge_sql(
            "p.d.orders",
            "p.d.orders__changes",
            &["id", "v", "created_at"],
            &["id"],
            SourceEngine::MySql,
            Some(&bound("DATE '2000-01-01'", "DATE '2010-12-14'")),
            None,
            true,
        );
        assert!(sql.starts_with("MERGE `p.d.orders` AS __rivet_t"), "{sql}");
        assert!(sql.contains("PARTITION BY `id` ORDER BY"), "{sql}");
        assert!(
            sql.contains("FROM `p.d.orders__changes`\n  ) WHERE __rn = 1 AND `created_at` >= DATE '2000-01-01' AND `created_at` < DATE '2010-12-14'"),
            "the buffer side is bounded AFTER ranking, on the winner: {sql}"
        );
        assert!(
            sql.contains("ON __rivet_t.`id` = __rivet_s.`id` AND (__rivet_t.`created_at` >= DATE '2000-01-01' AND __rivet_t.`created_at` < DATE '2010-12-14' OR __rivet_t.`created_at` IS NULL)"),
            "the base side is bounded by the same constants, plus its NULL partition: {sql}"
        );
        assert!(
            sql.contains(
                "WHEN MATCHED AND __rivet_s.__op = 'delete' THEN UPDATE SET `__is_deleted` = TRUE"
            ),
            "a tombstone flags, never deletes: {sql}"
        );
        assert!(
            sql.contains("WHEN MATCHED THEN UPDATE SET `id` = __rivet_s.`id`, `v` = __rivet_s.`v`, `created_at` = __rivet_s.`created_at`, `__is_deleted` = FALSE"),
            "an update refreshes the values and un-flags: {sql}"
        );
        assert!(
            sql.contains("WHEN NOT MATCHED AND COALESCE(__rivet_s.__op, '') != 'delete' THEN INSERT (`id`, `v`, `created_at`, `__is_deleted`) VALUES (__rivet_s.`id`, __rivet_s.`v`, __rivet_s.`created_at`, FALSE)"),
            "an insert lands live; a delete of an unknown key inserts nothing: {sql}"
        );
        assert!(
            !sql.contains("THEN DELETE"),
            "the warehouse deletes nothing: {sql}"
        );
        assert!(
            sql.contains("JSON_VALUE(__pos,'$.file')") && sql.contains("__seq DESC"),
            "{sql}"
        );
    }

    /// Rows whose partition column is NULL (a tombstone with a minimal before-image) merge on their own, unpruned, so a bounded pass cannot skip them.
    #[test]
    fn compact_merge_of_null_partition_rows_is_unbounded() {
        let sql = compact_merge_sql(
            "p.d.t",
            "p.d.t__changes",
            &["id"],
            &["id"],
            SourceEngine::Postgres,
            None,
            Some("created_at"),
            true,
        );
        assert!(
            sql.contains("WHERE __rn = 1 AND `created_at` IS NULL"),
            "the NULL set is chosen among the WINNERS, so a key whose latest change is \
             dated never merges here as well: {sql}"
        );
        assert!(
            sql.contains("ON __rivet_t.`id` = __rivet_s.`id`\n"),
            "no bound on the base: {sql}"
        );
    }

    /// Windows of at most `step` days cover `[lo, hi]` exactly once each.
    #[test]
    fn day_windows_cover_the_span_once_in_cap_sized_steps() {
        let lo = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let hi = lo + chrono::Duration::days(4999);
        let w = day_windows(lo, hi, 4000);
        assert_eq!(w.len(), 2);
        assert_eq!(w[0].0, lo);
        assert_eq!(w[0].1, lo + chrono::Duration::days(4000));
        assert_eq!(w[1].0, w[0].1, "windows abut");
        assert_eq!(
            w[1].1,
            hi + chrono::Duration::days(1),
            "the last is half-open past hi"
        );
        assert_eq!(
            day_windows(lo, lo, 4000),
            vec![(lo, lo + chrono::Duration::days(1))]
        );
    }

    #[test]
    fn time_literals_follow_the_columns_type() {
        let d = chrono::NaiveDate::from_ymd_opt(2026, 9, 17).unwrap();
        assert_eq!(time_literal(&BqType::Date.into(), d), "DATE '2026-09-17'");
        assert_eq!(
            time_literal(&BqType::DateTime.into(), d),
            "DATETIME '2026-09-17T00:00:00'"
        );
        assert_eq!(
            time_literal(&BqType::Timestamp.into(), d),
            "TIMESTAMP '2026-09-17 00:00:00+00'"
        );
    }

    #[test]
    fn the_probe_normalises_time_keys_to_dates_and_reads_ranges_as_is() {
        let pk = ["id".to_string()];
        let t = compact_probe_sql(
            "p.d.t__changes",
            "p.d.t",
            &pk,
            Some("created_at"),
            Some(&BqType::DateTime.into()),
            false,
        );
        assert!(
            t.contains("SELECT DATE(`created_at`) AS v FROM `p.d.t__changes` UNION ALL SELECT DATE(__rivet_t.`created_at`) FROM `p.d.t` AS __rivet_t WHERE EXISTS (SELECT 1 FROM `p.d.t__changes` AS __rivet_s WHERE __rivet_t.`id` = __rivet_s.`id`)")
                && t.contains("MIN(v)")
                && t.contains("(SELECT COUNTIF(`created_at` IS NULL) FROM `p.d.t__changes`) AS null_keys"),
            "the range covers the base's rows of the buffer's keys, the NULL count is the buffer's: {t}"
        );
        assert!(
            t.contains("IFNULL(CAST(MIN(IF(v >= DATE '1960-01-01' AND v < DATE '2160-01-01', v, NULL)) AS STRING), 'none') AS in_lo")
                && t.contains("MAX(IF(v >= DATE '1960-01-01' AND v < DATE '2160-01-01', v, NULL)) AS STRING), 'none') AS in_hi"),
            "a time key reads the span inside the partitioned days: {t}"
        );
        let ts = compact_probe_sql(
            "p.d.t__changes",
            "p.d.t",
            &pk,
            Some("created_at"),
            Some(&BqType::Timestamp.into()),
            false,
        );
        assert!(
            ts.contains("SELECT DATE(`created_at`, 'UTC') AS v FROM `p.d.t__changes` UNION ALL SELECT DATE(__rivet_t.`created_at`, 'UTC') FROM `p.d.t` AS __rivet_t"),
            "{ts}"
        );
        let r = compact_probe_sql("p.d.t__changes", "p.d.t", &pk, Some("bucket"), None, false);
        assert!(
            r.contains("SELECT `bucket` AS v FROM `p.d.t__changes` UNION ALL SELECT __rivet_t.`bucket` FROM `p.d.t` AS __rivet_t")
                && !r.contains("DATE(")
                && !r.contains("in_lo"),
            "{r}"
        );
        let n = compact_probe_sql("p.d.t__changes", "p.d.t", &pk, None, None, false);
        assert!(
            n.contains("'' AS lo") && n.contains("0 AS null_keys"),
            "{n}"
        );
        let own = compact_probe_sql(
            "p.d.t__changes",
            "p.d.t",
            &pk,
            Some("created_at"),
            Some(&BqType::Timestamp.into()),
            true,
        );
        assert!(
            own.contains("FROM (SELECT DATE(`created_at`, 'UTC') AS v FROM `p.d.t__changes`)")
                && !own.contains("`p.d.t` AS __rivet_t"),
            "a stream that refuses a partition move ranges over the buffer alone: {own}"
        );
    }
}

#[cfg(test)]
mod compact_column_tests {
    use super::*;
    use crate::load::plan::{Granularity, PartitionKey};

    /// A probe that does not say where its values lie inside the partitioned days.
    fn probe(rows: u64, lo: &str, hi: &str, nulls: u64) -> CompactProbe {
        CompactProbe {
            rows,
            lo: lo.into(),
            hi: hi.into(),
            nulls,
            in_lo: String::new(),
            in_hi: String::new(),
        }
    }

    /// A YEAR key steps 4,000 x 365 days, so the two partitioned centuries are ONE window.
    #[test]
    fn a_yearly_key_plans_one_window_for_the_partitioned_centuries() {
        let year = PartitionKey::Time {
            column: Some("created_at".into()),
            granularity: Granularity::Year,
        };
        let specs = vec![
            meta_spec("id", BqType::Int64.into()),
            meta_spec("created_at", BqType::DateTime.into()),
            flag_spec(Warehouse::BigQuery),
        ];
        let plans = plan_compact_merges(
            "p.d.t",
            "p.d.t__changes",
            &specs,
            &["id".to_string()],
            SourceEngine::MySql,
            Some(&year),
            &probe(4, "1960-01-01", "2159-12-31", 0),
        )
        .expect("a plan");
        assert_eq!(plans.len(), 1, "73,049 days at 1,460,000 per window");
        assert!(
            plans[0].contains("__rivet_t.`created_at` >= DATETIME '1960-01-01T00:00:00'")
                && plans[0].contains("__rivet_t.`created_at` < DATETIME '2160-01-01T00:00:00'"),
            "the one window covers the whole span: {}",
            plans[0]
        );
    }

    /// The scripted MERGE carries the DATA columns only.
    #[test]
    fn the_compaction_script_merges_data_columns_only() {
        let mut specs = meta_column_specs(Warehouse::BigQuery);
        specs.push(meta_spec("id", BqType::Int64.into()));
        specs.push(meta_spec("v", BqType::Int64.into()));
        specs.push(flag_spec(Warehouse::BigQuery));
        let s = compact_script_sql(
            "p.d.t",
            "p.d.t__changes",
            None,
            &specs,
            &["id".to_string()],
            SourceEngine::MySql,
            None,
        );
        assert!(
            s.contains("SET `id` = __rivet_s.`id`, `v` = __rivet_s.`v`, `__is_deleted` = FALSE"),
            "data columns, then the flag the merge sets: {s}"
        );
        assert!(
            s.contains(
                "INSERT (`id`, `v`, `__is_deleted`) VALUES (__rivet_s.`id`, __rivet_s.`v`, FALSE)"
            ),
            "{s}"
        );
        for meta in ["__op", "__pos", "__seq"] {
            assert!(
                !s.contains(&format!("`{meta}` = __rivet_s.`{meta}`")),
                "the buffer's {meta} must not land in the base: {s}"
            );
        }
        assert!(
            !s.contains("`__is_deleted` = __rivet_s.`__is_deleted`"),
            "the flag is the merge's decision, not a copied column: {s}"
        );
    }
}

#[cfg(test)]
mod compact_order_tests {
    use super::*;

    /// The compaction and the current-state view must rank ONE key the same way, or a compacted base and a view over the same rows disagree about which is latest.
    #[test]
    fn the_cursor_order_is_the_one_the_incremental_view_ranks_by() {
        let order = CompactOrder::Cursor("updated_at".into()).order_by(Warehouse::BigQuery);
        assert_eq!(order, "`updated_at` IS NOT NULL DESC, `updated_at` DESC");
        let view = inc_dedup_view_sql(
            Warehouse::BigQuery,
            "p.d.t",
            "p.d.t__changes",
            &["id"],
            "updated_at",
        );
        assert!(
            view.contains(&order),
            "the view must rank by the same order: {view}"
        );
        let cdc = CompactOrder::Cdc(SourceEngine::MySql).order_by(Warehouse::BigQuery);
        assert!(
            cdc.contains("__pos") && cdc.ends_with("DESC"),
            "a stream ranks by its log position: {cdc}"
        );
        assert_eq!(
            CompactOrder::from(SourceEngine::MySql).order_by(Warehouse::BigQuery),
            cdc
        );
    }
}

#[cfg(test)]
mod compact_flag_tests {
    use super::*;

    /// A query-based export cannot express a DELETE, so its base need not carry `__is_deleted`.
    #[test]
    fn a_base_without_the_delete_flag_merges_without_naming_it() {
        let data = vec![
            meta_spec("id", BqType::Int64.into()),
            meta_spec("v", BqType::Int64.into()),
        ];
        let mut flagged = data.clone();
        flagged.push(flag_spec(Warehouse::BigQuery));
        let build = |specs: &[TargetColumnSpec]| {
            compact_script_sql(
                "p.d.t",
                "p.d.t__changes",
                None,
                specs,
                &["id".to_string()],
                SourceEngine::MySql,
                None,
            )
        };

        let with = build(&flagged);
        assert!(
            with.contains("`__is_deleted` = FALSE") && with.contains("`__is_deleted` = TRUE"),
            "a stream's base keeps the flag and its tombstone arm: {with}"
        );

        let without = build(&data);
        assert!(
            !without.contains("__is_deleted"),
            "the column is named nowhere when the base has none: {without}"
        );
        assert!(
            without.contains("WHEN MATCHED AND __rivet_s.__op = 'delete' THEN DELETE\n")
                && !without.contains("THEN UPDATE SET `__is_deleted`"),
            "no flag, no tombstone to write — a delete DELETES, never falls through to the \
             upsert with its key-only image: {without}"
        );
        assert!(
            without.contains(
                "WHEN MATCHED THEN UPDATE SET `id` = __rivet_s.`id`, `v` = __rivet_s.`v`\n"
            ) && without.contains("INSERT (`id`, `v`) VALUES (__rivet_s.`id`, __rivet_s.`v`)"),
            "the data columns still upsert: {without}"
        );
    }
}

#[cfg(test)]
mod replace_compact_tests {
    use super::*;

    fn script(deleted_flag: bool) -> String {
        replace_compact_sql(
            Warehouse::Snowflake,
            "DB.SC.orders",
            "DB.SC.orders__changes__merging",
            &["id", "v"],
            &["id"],
            &CompactOrder::Cdc(SourceEngine::MySql),
            deleted_flag,
        )
    }

    fn at(sql: &str, needle: &str) -> usize {
        sql.find(needle)
            .unwrap_or_else(|| panic!("`{needle}` missing from:\n{sql}"))
    }

    /// The replacement is ONE transaction, in the order delete, insert, tombstone, commit.
    #[test]
    fn the_replacement_is_one_transaction_in_a_fixed_order() {
        let s = script(true);
        let order = [
            at(&s, "BEGIN;"),
            at(&s, "DELETE FROM DB.SC.orders USING"),
            at(&s, "INSERT INTO DB.SC.orders"),
            at(&s, "UPDATE DB.SC.orders SET __is_deleted = TRUE"),
            at(&s, "COMMIT;"),
        ];
        assert!(order.windows(2).all(|w| w[0] < w[1]), "{s}");
        assert_eq!(s.matches("BEGIN;").count(), 1, "{s}");
        assert!(
            !s.contains("MERGE"),
            "Snowflake replaces, it does not merge: {s}"
        );
    }

    /// With the flag a tombstone flags its base row and neither deletes nor inserts it.
    #[test]
    fn with_the_flag_a_tombstone_flags_its_row_and_is_never_inserted() {
        let s = script(true);
        assert!(
            s.contains(
                "WHERE orders.id = __rivet_s.id AND COALESCE(__rivet_s.__op, '') <> 'delete';"
            ),
            "only non-delete winners replace a base row: {s}"
        );
        assert!(
            s.contains("FALSE) FROM") || s.contains(", FALSE FROM"),
            "an inserted row is live: {s}"
        );
        assert!(
            s.contains("WHERE orders.id = __rivet_s.id AND __rivet_s.__op = 'delete';"),
            "the tombstone flags the row it finds and inserts nothing: {s}"
        );
        assert!(
            s.contains("INSERT INTO DB.SC.orders (id, v, __is_deleted)"),
            "{s}"
        );
    }

    /// Without the flag a delete winner deletes its base row, and no tombstone statement exists.
    #[test]
    fn without_the_flag_a_delete_deletes_and_names_no_flag_column() {
        let s = script(false);
        assert!(!s.contains("__is_deleted"), "{s}");
        assert!(!s.contains("UPDATE"), "{s}");
        assert!(
            s.contains("AS __rivet_s WHERE orders.id = __rivet_s.id;"),
            "every winner, deletes included, removes its base row: {s}"
        );
        assert!(
            s.contains("INSERT INTO DB.SC.orders (id, v)")
                && s.contains("WHERE COALESCE(__rivet_s.__op, '') <> 'delete';"),
            "a delete winner is not inserted: {s}"
        );
    }

    /// The winner of a key is the latest by log position; Snowflake sorts NULLs first on DESC.
    #[test]
    fn the_winner_ranks_a_missing_position_last_on_snowflake_only() {
        let s = script(true);
        assert!(
            s.contains("ROW_NUMBER() OVER (PARTITION BY id ORDER BY __pos IS NOT NULL DESC"),
            "{s}"
        );
        assert!(!s.contains("QUALIFY"), "{s}");
        let bq = CompactOrder::Cdc(SourceEngine::MySql).order_by(Warehouse::BigQuery);
        assert!(
            !bq.contains("IS NOT NULL"),
            "BigQuery's order is unchanged: {bq}"
        );
    }
}
