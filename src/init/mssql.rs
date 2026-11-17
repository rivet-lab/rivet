//! SQL Server introspection for `rivet init` (parity with [`super::postgres`] / [`super::mysql`]).

use crate::config::SourceType;
use crate::error::Result;
use crate::source::Source;
use crate::source::mssql::MssqlSource;

use crate::types::ColumnOverrides;

use super::{ColumnInfo, TableInfo};

/// Record (row) separator folded into the aggregated scalar — ASCII 30 (RS).
const RS: char = '\u{1e}';
/// Field separator within one aggregated record — ASCII 31 (US).
const US: char = '\u{1f}';

/// Open the one connection shared across the whole init run (`list_tables` plus every per-table `introspect`.
pub(super) fn connect(url: &str, tls: Option<&crate::config::TlsConfig>) -> Result<MssqlSource> {
    MssqlSource::connect_with_tls(url, tls)
}

/// Base tables and views in a SQL Server schema (`information_schema.TABLES`), without the system objects SQL Server ships (`IsMSShipped`, e.g.
pub(super) fn list_tables(
    conn: &mut dyn crate::source::Source,
    schema: &str,
) -> Result<Vec<String>> {
    let shipped = seeded!("mssql-shipped-tables-scaffolded", ">= 0", "= 0");
    let sql = format!(
        "SELECT STRING_AGG(CONVERT(nvarchar(max), TABLE_NAME), CHAR(30)) WITHIN GROUP (ORDER BY TABLE_NAME) \
         FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = N'{}' AND TABLE_TYPE IN ('BASE TABLE', 'VIEW') \
         AND OBJECTPROPERTY(OBJECT_ID(QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)), 'IsMSShipped') {shipped}",
        schema.replace('\'', "''"),
    );
    let names = conn
        .query_scalar(&sql)?
        .map(|s| {
            s.split(RS)
                .filter(|n| !n.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Ok(names)
}

pub(super) fn introspect(
    conn: &mut MssqlSource,
    schema: &str,
    table: &str,
) -> Result<super::Introspected> {
    let schema_lit = schema.replace('\'', "''");
    let table_lit = table.replace('\'', "''");

    let count_sql = format!(
        "SELECT SUM(p.rows) FROM sys.partitions p \
         JOIN sys.objects o ON o.object_id = p.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE s.name = N'{schema_lit}' AND o.name = N'{table_lit}' AND p.index_id IN (0,1)",
    );
    let row_estimate = conn
        .query_scalar(&count_sql)?
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
        .max(0);

    let columns_sql = format!(
        "SELECT STRING_AGG(CONVERT(nvarchar(max), \
             CONCAT( \
                 c.COLUMN_NAME, CHAR(31), \
                 c.DATA_TYPE, CHAR(31), \
                 CASE WHEN pk.COLUMN_NAME IS NULL THEN '0' ELSE '1' END, CHAR(31), \
                 c.IS_NULLABLE, CHAR(31), \
                 ISNULL(CONVERT(varchar(12), c.NUMERIC_PRECISION), ''), CHAR(31), \
                 ISNULL(CONVERT(varchar(12), c.NUMERIC_SCALE), ''), CHAR(31), \
                 CASE WHEN ix.name IS NULL THEN '0' ELSE '1' END \
             )), CHAR(30)) WITHIN GROUP (ORDER BY c.ORDINAL_POSITION) \
         FROM information_schema.COLUMNS c \
         LEFT JOIN ( \
             SELECT ku.COLUMN_NAME \
             FROM information_schema.TABLE_CONSTRAINTS tc \
             JOIN information_schema.KEY_COLUMN_USAGE ku \
                 ON ku.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
                 AND ku.TABLE_SCHEMA = tc.TABLE_SCHEMA \
                 AND ku.TABLE_NAME = tc.TABLE_NAME \
             WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
                 AND tc.TABLE_SCHEMA = N'{schema_lit}' AND tc.TABLE_NAME = N'{table_lit}' \
         ) pk ON pk.COLUMN_NAME = c.COLUMN_NAME \
         LEFT JOIN ( \
             SELECT DISTINCT sc.name \
             FROM sys.index_columns ic \
             JOIN sys.columns sc ON sc.object_id = ic.object_id AND sc.column_id = ic.column_id \
             WHERE ic.key_ordinal = 1 \
                 AND ic.object_id = OBJECT_ID(QUOTENAME(N'{schema_lit}') + N'.' + QUOTENAME(N'{table_lit}')) \
         ) ix ON ix.name = c.COLUMN_NAME \
         WHERE c.TABLE_SCHEMA = N'{schema_lit}' AND c.TABLE_NAME = N'{table_lit}'",
    );

    let cdc_capture_instance = conn
        .query_scalar(&format!(
            "SELECT TOP 1 capture_instance FROM cdc.change_tables \
             WHERE source_object_id = OBJECT_ID(QUOTENAME(N'{schema_lit}') + N'.' + QUOTENAME(N'{table_lit}')) \
             ORDER BY create_date DESC, capture_instance"
        ))
        .ok()
        .flatten();

    let agg = conn.query_scalar(&columns_sql)?;
    let mut columns: Vec<ColumnInfo> = agg.as_deref().map(parse_columns_agg).unwrap_or_default();

    if columns.is_empty() {
        return Err(super::table_not_found(&format!("{schema}.{table}")));
    }
    let probe = conn.type_mappings(
        &super::select_all(SourceType::Mssql, &[schema, table]),
        &ColumnOverrides::default(),
    );
    let unreadable = super::probe_keyset_refusals(SourceType::Mssql, &mut columns, probe, || {
        crate::source::mssql::finer_than_microsecond_columns(conn, schema, table)
    })?;

    let info = TableInfo {
        cdc_capture_instance,
        cdc_unmineable: None,
        density: None,
        schema: schema.to_string(),
        table: table.to_string(),
        row_estimate,
        total_bytes: None,
        columns,
    };
    Ok((info, unreadable))
}

/// Split the `RS`/`US`-delimited column aggregate back into [`ColumnInfo`]s.
fn parse_columns_agg(agg: &str) -> Vec<ColumnInfo> {
    agg.split(RS)
        .filter(|rec| !rec.is_empty())
        .filter_map(|rec| {
            let f: Vec<&str> = rec.split(US).collect();
            if f.len() != 7 {
                return None;
            }
            Some(ColumnInfo {
                is_indexed: f[2] == "1" || f[6] == "1",
                name: f[0].to_string(),
                data_type: catalog_type(f[1]),
                is_primary_key: f[2] == "1",
                is_nullable: f[3].eq_ignore_ascii_case("YES"),
                numeric_precision: f[4].parse::<u32>().ok(),
                numeric_scale: f[5].parse::<u32>().ok(),
                ..Default::default()
            })
        })
        .collect()
}

/// SQL Server's catalog spells its 8-byte row-version counter `timestamp`.
fn catalog_type(data_type: &str) -> String {
    if data_type.eq_ignore_ascii_case("timestamp") {
        "rowversion".to_string()
    } else {
        data_type.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `DATA_TYPE = 'timestamp'` on SQL Server is `rowversion`, never a time.
    #[test]
    fn a_rowversion_column_is_never_read_as_a_timestamp() {
        let agg = format!(
            "id{US}bigint{US}1{US}NO{US}19{US}0{US}1{RS}rv{US}timestamp{US}0{US}NO{US}{US}{US}0{RS}\
             seen_at{US}datetime2{US}0{US}YES{US}{US}{US}0"
        );
        let cols = parse_columns_agg(&agg);
        let ty = |n: &str| cols.iter().find(|c| c.name == n).unwrap().data_type.clone();
        assert_eq!(ty("rv"), "rowversion");
        assert!(!super::super::is_timestamp_type(&ty("rv")));
        assert!(super::super::is_timestamp_type(&ty("seen_at")));
    }

    /// Guard the whole init-introspection SQL against Msg 9829.
    #[test]
    fn every_string_agg_uses_nvarchar_max_to_survive_wide_tables() {
        let src = include_str!("mssql.rs");
        let production = src.split("#[cfg(test)]").next().unwrap();
        for (i, line) in production.lines().enumerate() {
            if line.contains("STRING_AGG(") && !line.trim_start().starts_with("//") {
                assert!(
                    line.contains("CONVERT(nvarchar(max)"),
                    "line {}: STRING_AGG without CONVERT(nvarchar(max)) hits Msg 9829 on a wide \
                     table:\n{}",
                    i + 1,
                    line.trim()
                );
            }
        }
    }

    /// Build one `US`-joined column record (mirrors the server-side CONCAT).
    fn rec(fields: &[&str]) -> String {
        fields.join(&US.to_string())
    }

    /// Join column records with the `RS` separator (mirrors STRING_AGG).
    fn agg(records: &[String]) -> String {
        records.join(&RS.to_string())
    }

    #[test]
    fn parse_columns_agg_round_trips_fields() {
        let s = agg(&[
            rec(&["id", "bigint", "1", "NO", "", "", "1"]),
            rec(&["amount", "decimal", "0", "NO", "12", "2", "1"]),
            rec(&["note", "nvarchar", "0", "YES", "", "", "0"]),
        ]);
        let cols = parse_columns_agg(&s);
        assert_eq!(cols.len(), 3);

        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].data_type, "bigint");
        assert!(cols[0].is_primary_key);
        assert!(!cols[0].is_nullable);
        assert_eq!(cols[0].numeric_precision, None);
        assert_eq!(cols[0].numeric_scale, None);

        assert_eq!(cols[1].name, "amount");
        assert!(!cols[1].is_primary_key);
        assert_eq!(cols[1].numeric_precision, Some(12));
        assert_eq!(cols[1].numeric_scale, Some(2));
        assert!(
            cols[1].is_indexed,
            "a non-PK column that leads an index is indexed"
        );
        assert!(!cols[2].is_indexed);

        assert_eq!(cols[2].name, "note");
        assert!(cols[2].is_nullable);
    }

    #[test]
    fn parse_columns_agg_skips_malformed_record() {
        let s = agg(&[
            rec(&["id", "bigint", "1", "NO", "", "", "1"]),
            rec(&["broken", "rec"]),
        ]);
        let cols = parse_columns_agg(&s);
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "id");
    }

    #[test]
    fn parse_columns_agg_empty_is_empty() {
        assert!(parse_columns_agg("").is_empty());
    }
}

#[cfg(test)]
mod list_tables_tests {
    /// A source that answers the listing with `names` joined by `RS` and keeps the SQL it was asked.
    struct Listing(Option<String>, Vec<String>);

    impl crate::source::Source for Listing {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
        fn query_scalar(&mut self, sql: &str) -> crate::error::Result<Option<String>> {
            self.1.push(sql.to_string());
            Ok(self.0.clone())
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _column_overrides: &crate::types::ColumnOverrides,
        ) -> crate::error::Result<Vec<crate::types::TypeMapping>> {
            unreachable!()
        }
    }

    /// The listing asks for the schema's tables and views without the shipped ones, and returns every name the server sent.
    #[test]
    fn the_listing_leaves_out_shipped_objects_and_returns_every_name() {
        let rs = super::RS.to_string();
        let mut src = Listing(Some(["orders", "o'brien", ""].join(&rs)), Vec::new());
        let names = super::list_tables(&mut src, "sa'les").unwrap();
        assert_eq!(names, vec!["orders".to_string(), "o'brien".to_string()]);
        let sql = &src.1[0];
        assert!(sql.contains("'IsMSShipped') = 0"), "{sql}");
        assert!(sql.contains("TABLE_SCHEMA = N'sa''les'"), "{sql}");
        let mut empty = Listing(None, Vec::new());
        assert!(super::list_tables(&mut empty, "dbo").unwrap().is_empty());
    }
}
