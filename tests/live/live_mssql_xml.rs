//! SQL Server `xml` columns are delivered as the server's own text, batch and CDC.

use crate::common::*;

const COLS: &str = "id INT PRIMARY KEY, doc XML";

const NO_ORACLE: &str = "xml: the DuckDB mssql scanner reads xml as text of its own; the oracle here is sqlcmd's CAST to nvarchar(max)";

/// The value expressions inserted per id; `None` is SQL NULL.
fn docs() -> Vec<(i64, Option<&'static str>)> {
    vec![
        (1, Some("CAST(N'<a/>' AS xml)")),
        (
            2,
            Some("CAST(N'<r a=\"1\" b=''x\"y''><c>h\u{e9}llo \u{2713} \u{1F600}</c></r>' AS xml)"),
        ),
        (3, Some("CAST(N'<t>&lt;&amp;&gt;&quot;</t>' AS xml)")),
        (4, None),
        (5, Some("CAST(N'<a/><b>two</b>' AS xml)")),
        (6, Some("CAST(N'<a><![CDATA[x<y]]></a>' AS xml)")),
        (7, Some("CAST(N'<a><!-- c --><?pi x?></a>' AS xml)")),
        (
            8,
            Some("CAST(N'<a xml:space=\"preserve\">  x  </a>' AS xml)"),
        ),
        (9, Some("CAST(N'<a>  x  </a>' AS xml)")),
        (
            10,
            Some(
                "CAST(N'<a>' + REPLICATE(CAST(N'\u{e9}' AS nvarchar(max)), 1500) + N'</a>' AS xml)",
            ),
        ),
        (11, Some("CAST(N'<a>x&#13;&#10;y</a>' AS xml)")),
    ]
}

/// One INSERT per id in `ids`.
fn inserts(table: &str, ids: std::ops::RangeInclusive<i64>) -> Vec<String> {
    docs()
        .into_iter()
        .filter(|(id, _)| ids.contains(id))
        .map(|(id, expr)| {
            format!(
                "INSERT INTO dbo.{table} (id, doc) VALUES ({id}, {})",
                expr.unwrap_or("NULL")
            )
        })
        .collect()
}

/// The source's own text per id as `id|lowercase hex of the UTF-16 bytes of CAST(doc AS nvarchar(max))`, `NULL` for SQL NULL.
fn source_rendering(container: &str, table: &str) -> Vec<String> {
    let lines = sqlcmd_lines(
        container,
        &format!(
            "SELECT CONCAT(id, '|0|NULL') FROM dbo.{table} WHERE doc IS NULL \
             UNION ALL \
             SELECT CONCAT(t.id, '|', n.n, '|', SUBSTRING(t.h, (n.n - 1) * 200 + 1, 200)) \
             FROM (SELECT id, LOWER(CONVERT(varchar(max), CAST(CAST(doc AS nvarchar(max)) AS varbinary(max)), 2)) h \
                   FROM dbo.{table} WHERE doc IS NOT NULL) t \
             JOIN (SELECT TOP 100 ROW_NUMBER() OVER (ORDER BY (SELECT 1)) n FROM sys.all_columns) n \
               ON (n.n - 1) * 200 < LEN(t.h)"
        ),
    );
    let mut by_id: std::collections::BTreeMap<i64, Vec<(i64, String)>> = Default::default();
    for l in lines.iter().filter(|l| !l.is_empty()) {
        let mut p = l.splitn(3, '|');
        let id = p.next().unwrap().parse().expect("oracle id");
        let n = p.next().unwrap().parse().expect("oracle chunk");
        by_id
            .entry(id)
            .or_default()
            .push((n, p.next().unwrap().to_string()));
    }
    by_id
        .into_iter()
        .map(|(id, mut chunks)| {
            chunks.sort();
            let hex: String = chunks.into_iter().map(|(_, h)| h).collect();
            format!("{id}|{hex}")
        })
        .collect()
}

/// What rivet wrote in `dir`, in the same `id|hex` shape, ordered by id.
fn delivered(dir: &std::path::Path) -> Vec<String> {
    let ids = dir_parquet_ids(dir);
    let docs = dir_parquet_display(dir, "doc");
    assert_eq!(ids.len(), docs.len(), "one doc cell per id in {dir:?}");
    let mut rows: Vec<(i64, Option<String>)> = ids.into_iter().zip(docs).collect();
    rows.sort();
    rows.into_iter()
        .map(|(id, doc)| {
            let text = doc.map_or_else(
                || "NULL".to_string(),
                |t| {
                    t.encode_utf16()
                        .flat_map(u16::to_le_bytes)
                        .map(|b| format!("{b:02x}"))
                        .collect()
                },
            );
            format!("{id}|{text}")
        })
        .collect()
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_batch_export_delivers_xml_columns_as_the_source_renders_them() {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_xml");
    mssql_exec(&format!("CREATE TABLE dbo.{table} ({COLS})"));
    let _guard = MssqlTable::adopt(table.clone());
    for sql in inserts(&table, 1..=11) {
        mssql_exec(&sql);
    }
    let want = source_rendering("rivet-mssql-1", &table);
    assert_eq!(want.len(), 11, "oracle rows: {want:?}");
    let rig = Rig::mssql_batch(&table).no_oracle(NO_ORACLE);
    rig.run_ok();
    assert_eq!(delivered(&rig.out_dir()), want);
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_cdc_baseline_and_stream_deliver_the_same_xml_text_as_the_source_renders() {
    let mut sc = CdcScenario::mssql_with("mssql_cdc_xml", COLS, |r, _| {
        r.cdc("initial: snapshot").no_oracle(NO_ORACLE)
    });
    let table = sc.table.clone();
    for sql in inserts(&table, 1..=6) {
        sc.sql(&sql);
    }
    sc.settle();
    sc.rig.run_ok();
    for sql in inserts(&table, 7..=11) {
        sc.sql(&sql);
    }
    sc.settle();
    sc.rig.run_ok();
    let want = source_rendering("rivet-mssql-cdc-1", &table);
    assert_eq!(want.len(), 11, "oracle rows: {want:?}");
    let out = sc.rig.out_dir();
    let mut got = delivered(&out.join("snapshot"));
    assert_eq!(got.len(), 6, "the baseline holds ids 1..=6: {got:?}");
    got.extend(delivered(&out));
    assert_eq!(got, want);
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_cdc_stream_without_a_baseline_delivers_xml_text_as_the_source_renders() {
    let mut sc = CdcScenario::mssql_with("mssql_cdc_xml_s", COLS, |r, _| r.no_oracle(NO_ORACLE));
    sc.rig.run_ok();
    let table = sc.table.clone();
    for sql in inserts(&table, 1..=11) {
        sc.sql(&sql);
    }
    sc.settle();
    sc.rig.run_ok();
    let want = source_rendering("rivet-mssql-cdc-1", &table);
    assert_eq!(want.len(), 11, "oracle rows: {want:?}");
    assert_eq!(delivered(&sc.rig.out_dir()), want);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_declared_text_column_over_xml_delivers_the_same_text_as_the_undeclared_one() {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_xml_decl");
    mssql_exec(&format!("CREATE TABLE dbo.{table} ({COLS})"));
    let _guard = MssqlTable::adopt(table.clone());
    for sql in inserts(&table, 1..=11) {
        mssql_exec(&sql);
    }
    let want = source_rendering("rivet-mssql-1", &table);
    let rig = Rig::mssql_batch(&table)
        .export_line("columns: {doc: text}")
        .no_oracle(NO_ORACLE);
    rig.run_ok();
    assert_eq!(delivered(&rig.out_dir()), want);
}
