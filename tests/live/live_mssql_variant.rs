//! SQL Server `sql_variant` and CLR UDT columns are delivered as text, batch and CDC.
//!
//! tiberius 0.12 panicked (`unimplemented!`) on a `sql_variant` cell and batch had to
//! refuse the column up front; CDC had no guard at all. The oracle is the server's own
//! rendering through `sqlcmd`: `CONVERT(nvarchar(4000), v, 121)` (ISO for temporals)
//! for the variant, and the lowercase hex of `CAST(x AS varbinary(max))` for
//! `hierarchyid` / `geometry` / `geography`. Also pins the one tiberius 0.13 default
//! rivet overrides: no 30 s bound on a server round-trip.

use crate::common::*;
use crate::live_value_refusal_text_remedy_load::{ChDb, clickhouse_rows_json, json_text};

const COLS: &str = "id INT PRIMARY KEY, v sql_variant, h hierarchyid, g geometry, gg geography";

/// One INSERT per row: an int, an nvarchar, a datetime2(7), a decimal, a datetimeoffset, NULLs.
fn inserts(table: &str) -> Vec<String> {
    [
        "1, CAST(42 AS int), '/1/2/', geometry::STGeomFromText('POINT(1 2)', 0), \
         geography::Point(47.65, -122.34, 4326)",
        "2, CAST(N'héllo ✓' AS nvarchar(20)), NULL, NULL, NULL",
        "3, CAST('2024-02-29 12:34:56.1234567' AS datetime2(7)), '/', NULL, NULL",
        "4, CAST(12.50 AS decimal(10,2)), NULL, NULL, NULL",
        "5, CAST('2024-02-29 12:00:00.5 +05:30' AS datetimeoffset(1)), NULL, NULL, NULL",
        "6, NULL, NULL, NULL, NULL",
    ]
    .iter()
    .map(|v| format!("INSERT INTO dbo.{table} (id, v, h, g, gg) VALUES ({v})"))
    .collect()
}

/// The source's own text for every row, as `id|v|h|g|gg` with `NULL` for SQL NULL.
fn source_rendering(container: &str, table: &str) -> Vec<String> {
    let hex = |c: &str| format!("LOWER(CONVERT(varchar(max), CAST({c} AS varbinary(max)), 2))");
    sqlcmd_lines(
        container,
        &format!(
            "SELECT CONCAT(id, '|', ISNULL(CONVERT(nvarchar(4000), v, 121), N'NULL'), '|', \
             ISNULL({}, 'NULL'), '|', ISNULL({}, 'NULL'), '|', ISNULL({}, 'NULL')) \
             FROM dbo.{table} ORDER BY id",
            hex("h"),
            hex("g"),
            hex("gg")
        ),
    )
}

/// What rivet wrote, read back by DuckDB, in the same `id|v|h|g|gg` shape.
fn delivered(dir: &std::path::Path) -> Vec<String> {
    let c = stage_for_duckdb(dir);
    let v = duckdb_run_sql_json(&format!(
        "SELECT id, v, h, g, gg FROM read_parquet('{c}/**/*.parquet') ORDER BY id"
    ));
    v["rows"]
        .as_array()
        .expect("duckdb rows")
        .iter()
        .map(|r| {
            (0..5)
                .map(|i| r[i].as_str().unwrap_or("NULL").to_string())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_batch_export_delivers_sql_variant_and_udt_columns_as_the_source_renders_them() {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_variant");
    mssql_exec(&format!("CREATE TABLE dbo.{table} ({COLS})"));
    let _guard = MssqlTable::adopt(table.clone());
    for sql in inserts(&table) {
        mssql_exec(&sql);
    }
    let want = source_rendering("rivet-mssql-1", &table);
    assert_eq!(want.len(), 6, "oracle rows: {want:?}");
    let rig = Rig::mssql_batch(&table)
        .no_oracle("sql_variant / CLR UDT: the DuckDB mssql scanner renders ToString()/WKB where rivet delivers the wire bytes; no shared rendering to compare");
    rig.run_ok();
    assert_eq!(delivered(&rig.out_dir()), want);
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_cdc_capture_delivers_sql_variant_and_udt_columns_as_the_source_renders_them() {
    let mut sc = CdcScenario::mssql_with("mssql_cdc_variant", COLS, |r, _| {
        r.no_oracle("sql_variant / CLR UDT: the DuckDB mssql scanner renders ToString()/WKB where rivet delivers the wire bytes; no shared rendering to compare")
    });
    sc.rig.run_ok(); // pin
    let table = sc.table.clone();
    for sql in inserts(&table) {
        sc.sql(&sql);
    }
    sc.settle();
    sc.rig.run_ok();
    let want = source_rendering("rivet-mssql-cdc-1", &table);
    assert_eq!(want.len(), 6, "oracle rows: {want:?}");
    assert_eq!(delivered(&sc.rig.out_dir()), want);
}

const GEO_COLS: &str = "id INT PRIMARY KEY, g geometry, gg geography";

/// One INSERT per row: SRID 0 and 4326, NULL, empty, curve types, and a Z/M point.
fn geo_inserts(table: &str) -> Vec<String> {
    [
        "1, geometry::STGeomFromText('POINT(1 2)', 0), geography::Point(47.65, -122.34, 4326)",
        "2, geometry::STGeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))', 4326), \
         geography::STGeomFromText('LINESTRING(-122.36 47.656, -122.343 47.656)', 4326)",
        "3, NULL, NULL",
        "4, geometry::STGeomFromText('POINT EMPTY', 0), \
         geography::STGeomFromText('GEOMETRYCOLLECTION EMPTY', 4326)",
        "5, geometry::STGeomFromText('CIRCULARSTRING(0 0, 1 1, 2 0)', 0), \
         geography::STGeomFromText('CIRCULARSTRING(-122.3 47.6, -122.2 47.7, -122.1 47.6)', 4326)",
        "6, geometry::STGeomFromText('COMPOUNDCURVE((0 0,1 1),CIRCULARSTRING(1 1,2 0,3 1))', 0), NULL",
        "7, geometry::STGeomFromText('CURVEPOLYGON(CIRCULARSTRING(0 0,4 0,4 4,0 4,0 0))', 0), NULL",
        "8, geometry::STGeomFromText('POINT(1 2 3 4)', 0), \
         geography::STGeomFromText('POINT(1 2 3 4)', 4326)",
    ]
    .iter()
    .map(|v| format!("INSERT INTO dbo.{table} (id, g, gg) VALUES ({v})"))
    .collect()
}

/// The server's rendering of `expr` per row, read in 200-character slices because sqlcmd truncates a long line.
fn geo_slices(container: &str, table: &str, expr: &str) -> std::collections::BTreeMap<i64, String> {
    let lines = sqlcmd_lines(
        container,
        &format!(
            "WITH n AS (SELECT TOP 60 ROW_NUMBER() OVER (ORDER BY (SELECT 1)) - 1 AS k FROM sys.all_objects) \
             SELECT CONCAT(t.id, '|', n.k, '|', CASE WHEN {expr} IS NULL THEN CASE WHEN n.k = 0 THEN 'NULL' ELSE '' END \
             ELSE SUBSTRING({expr}, n.k * 200 + 1, 200) END) FROM dbo.{table} t CROSS JOIN n \
             WHERE n.k = 0 OR n.k * 200 < ISNULL(LEN({expr}), 0) ORDER BY t.id, n.k"
        ),
    );
    let mut out = std::collections::BTreeMap::new();
    for line in lines {
        let mut f = line.splitn(3, '|');
        let id: i64 = f.next().expect("id").trim().parse().expect("numeric id");
        f.next().expect("slice index");
        out.entry(id)
            .or_insert_with(String::new)
            .push_str(f.next().unwrap_or(""));
    }
    out
}

/// The server's rendering of `a` and `b` per row as `id|a|b`, with `NULL` for SQL NULL.
fn geo_source(container: &str, table: &str, a: &str, b: &str) -> Vec<String> {
    let (a, b) = (
        geo_slices(container, table, a),
        geo_slices(container, table, b),
    );
    a.iter()
        .map(|(id, x)| format!("{id}|{x}|{}", b[id]))
        .collect()
}

/// The lowercase hex of the CLR serialization of column `c`.
fn clr_hex(c: &str) -> String {
    format!("LOWER(CONVERT(varchar(max), CAST({c} AS varbinary(max)), 2))")
}

/// What rivet wrote under `dir`, read back through the parquet reader as `id|a|b`, `NULL` for a null, bytes as lowercase hex.
fn geo_delivered(dir: &std::path::Path, a: &str, b: &str) -> Vec<String> {
    use arrow::util::display::array_value_to_string;
    let cell = |col: &arrow::array::ArrayRef, i: usize| {
        if col.is_null(i) {
            "NULL".to_string()
        } else {
            array_value_to_string(col, i)
                .expect("render a cell")
                .to_lowercase()
        }
    };
    let mut rows: Vec<(i64, String)> = Vec::new();
    for part in files_under(dir)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
    {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(part).expect("open part"),
        )
        .expect("parquet part")
        .build()
        .expect("parquet reader");
        for batch in reader {
            let batch = batch.expect("record batch");
            let id = batch.column_by_name("id").expect("id column");
            let ids =
                arrow::compute::cast(id, &arrow::datatypes::DataType::Int64).expect("id as i64");
            let ids = ids
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("i64 ids");
            let (ca, cb) = (batch.column_by_name(a), batch.column_by_name(b));
            let (ca, cb) = (ca.expect("first column"), cb.expect("second column"));
            for i in 0..batch.num_rows() {
                rows.push((
                    ids.value(i),
                    format!("{}|{}|{}", ids.value(i), cell(ca, i), cell(cb, i)),
                ));
            }
        }
    }
    rows.sort();
    rows.into_iter().map(|(_, line)| line).collect()
}

/// The `rivet.text_form` of `col` in every part under `dir` that holds the column.
fn text_forms(dir: &std::path::Path, col: &str) -> Vec<Option<String>> {
    files_under(dir)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .filter_map(|p| {
            let b = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                std::fs::File::open(p).expect("open part"),
            )
            .expect("parquet part");
            let f = b.schema().field_with_name(col).ok()?.clone();
            Some(f.metadata().get("rivet.text_form").cloned())
        })
        .collect()
}

/// Every part holding `col` labels it `hex_bytes`, and at least one does.
fn assert_hex_bytes_labelled(dir: &std::path::Path, col: &str) {
    let forms = text_forms(dir, col);
    assert!(
        !forms.is_empty() && forms.iter().all(|f| f.as_deref() == Some("hex_bytes")),
        "{col}: rivet.text_form per part: {forms:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_batch_export_labels_geometry_and_geography_hex_bytes_and_keeps_the_clr_hex_of_every_value() {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_geo");
    mssql_exec(&format!("CREATE TABLE dbo.{table} ({GEO_COLS})"));
    let _guard = MssqlTable::adopt(table.clone());
    for sql in geo_inserts(&table) {
        mssql_exec(&sql);
    }
    let want = geo_source("rivet-mssql-1", &table, &clr_hex("g"), &clr_hex("gg"));
    assert_eq!(want.len(), 8, "oracle rows: {want:?}");
    let rig = Rig::mssql_batch(&table);
    rig.run_ok();
    let dir = rig.out_dir();
    assert_eq!(geo_delivered(&dir, "g", "gg"), want);
    assert_hex_bytes_labelled(&dir, "g");
    assert_hex_bytes_labelled(&dir, "gg");
    assert_eq!(text_forms(&dir, "id"), vec![None]);
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_cdc_capture_labels_geometry_and_geography_hex_bytes_and_keeps_the_clr_hex_of_every_value() {
    let mut sc = CdcScenario::mssql_with("mssql_cdc_geo", GEO_COLS, |r, _| r);
    sc.rig.run_ok();
    let table = sc.table.clone();
    for sql in geo_inserts(&table) {
        sc.sql(&sql);
    }
    sc.settle();
    sc.rig.run_ok();
    let want = geo_source("rivet-mssql-cdc-1", &table, &clr_hex("g"), &clr_hex("gg"));
    assert_eq!(want.len(), 8, "oracle rows: {want:?}");
    let dir = sc.rig.out_dir();
    assert_eq!(geo_delivered(&dir, "g", "gg"), want);
    assert_hex_bytes_labelled(&dir, "g");
    assert_hex_bytes_labelled(&dir, "gg");
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_query_with_stasbinary_and_stsrid_delivers_wkb_and_the_srid_not_the_clr_hex() {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_geo_wkb");
    mssql_exec(&format!("CREATE TABLE dbo.{table} ({GEO_COLS})"));
    let _guard = MssqlTable::adopt(table.clone());
    for sql in geo_inserts(&table) {
        mssql_exec(&sql);
    }
    let wkb = |c: &str| format!("LOWER(CONVERT(varchar(max), {c}.STAsBinary(), 2))");
    let want = geo_source("rivet-mssql-1", &table, &wkb("g"), &wkb("gg"));
    let clr = geo_source("rivet-mssql-1", &table, &clr_hex("g"), &clr_hex("gg"));
    assert_eq!(want.len(), 8, "oracle rows: {want:?}");
    assert_ne!(want, clr, "the WKB is not the CLR hex");
    let rig = Rig::mssql_batch(&table).query(&format!(
        "SELECT id, g.STAsBinary() AS g_wkb, gg.STAsBinary() AS gg_wkb FROM dbo.{table}"
    ));
    rig.run_ok();
    assert_eq!(geo_delivered(&rig.out_dir(), "g_wkb", "gg_wkb"), want);
    let srid = |c: &str| format!("CAST({c}.STSrid AS varchar(10))");
    let want_srid = geo_source("rivet-mssql-1", &table, &srid("g"), &srid("gg"));
    let rig = Rig::mssql_batch(&table).query(&format!(
        "SELECT id, g.STSrid AS g_srid, gg.STSrid AS gg_srid FROM dbo.{table}"
    ));
    rig.run_ok();
    assert_eq!(
        geo_delivered(&rig.out_dir(), "g_srid", "gg_srid"),
        want_srid
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_batch_export_labels_hierarchyid_hex_bytes_and_keeps_the_clr_hex_of_every_value() {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_hier");
    mssql_exec(&format!(
        "CREATE TABLE dbo.{table} (id INT PRIMARY KEY, h hierarchyid, hh hierarchyid)"
    ));
    let _guard = MssqlTable::adopt(table.clone());
    for row in [
        "1, hierarchyid::Parse('/'), hierarchyid::Parse('/1/')",
        "2, hierarchyid::Parse('/1/2.3/'), hierarchyid::Parse('/4/5/6/')",
        "3, NULL, NULL",
    ] {
        mssql_exec(&format!("INSERT INTO dbo.{table} VALUES ({row})"));
    }
    let want = geo_source("rivet-mssql-1", &table, &clr_hex("h"), &clr_hex("hh"));
    assert_eq!(want.len(), 3, "oracle rows: {want:?}");
    let rig = Rig::mssql_batch(&table);
    rig.run_ok();
    let dir = rig.out_dir();
    assert_eq!(geo_delivered(&dir, "h", "hh"), want);
    assert_hex_bytes_labelled(&dir, "h");
    assert_hex_bytes_labelled(&dir, "hh");
}

/// Runs a chunked export keyed on a hierarchyid PK of `rows` rows and asserts the refusal: exit 1, its code, its text, no part written.
fn assert_hierarchyid_key_refused(rows: usize, chunk_size: Option<usize>) {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_hier_key");
    mssql_exec(&format!(
        "CREATE TABLE dbo.{table} (h hierarchyid NOT NULL PRIMARY KEY, v INT NOT NULL)"
    ));
    let _guard = MssqlTable::adopt(table.clone());
    for i in 0..rows {
        let path = if i == 0 {
            "/".to_string()
        } else {
            format!("/{i}/")
        };
        mssql_exec(&format!(
            "INSERT INTO dbo.{table} VALUES (hierarchyid::Parse('{path}'), {i})"
        ));
    }
    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir = tmp.path().join("out");
    let mut rig = Rig::mssql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: h")
        .dest_path(out_dir.clone());
    if let Some(n) = chunk_size {
        rig = rig.export_line(&format!("chunk_size: {n}"));
    }
    let out = rig.run_args(&[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(1),
        "a hierarchyid key must be refused: {stderr}"
    );
    assert!(
        stderr.contains("RIVET_PLAN_CHUNK_KEY_UNUSABLE")
            && stderr.contains(
                "Column 'h' is delivered as text, which does not sort as the stored value"
            ),
        "stderr: {stderr}"
    );
    let parts = files_under(&out_dir)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .count();
    assert_eq!(
        parts, 0,
        "the refusal comes before the first page is written"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_hierarchyid_primary_key_is_refused_as_a_keyset_key_before_the_first_write() {
    assert_hierarchyid_key_refused(6, Some(2));
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_hierarchyid_primary_key_on_a_table_smaller_than_chunk_size_is_refused_too() {
    assert_hierarchyid_key_refused(3, Some(100));
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_columns_text_declaration_on_a_geometry_column_still_delivers_the_clr_hex_unlabelled() {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_geo_decl");
    mssql_exec(&format!("CREATE TABLE dbo.{table} ({GEO_COLS})"));
    let _guard = MssqlTable::adopt(table.clone());
    for sql in geo_inserts(&table) {
        mssql_exec(&sql);
    }
    let want = geo_source("rivet-mssql-1", &table, &clr_hex("g"), &clr_hex("gg"));
    let rig = Rig::mssql_batch(&table).export_line("columns: {g: text, gg: text}");
    rig.run_ok();
    let dir = rig.out_dir();
    assert_eq!(geo_delivered(&dir, "g", "gg"), want);
    assert_eq!(text_forms(&dir, "g"), vec![None]);
}

#[test]
#[ignore = "live: requires docker compose mssql + RIVET_PREV_RELEASE_BIN"]
fn a_geometry_column_a_previous_release_wrote_is_not_drift_under_fail_and_keeps_its_values() {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_geo_up");
    mssql_exec(&format!("CREATE TABLE dbo.{table} ({GEO_COLS})"));
    let _guard = MssqlTable::adopt(table.clone());
    for sql in geo_inserts(&table) {
        mssql_exec(&sql);
    }
    let want = geo_source("rivet-mssql-1", &table, &clr_hex("g"), &clr_hex("gg"));
    let rig = Rig::mssql_batch(&table)
        .no_oracle("the first run is the previous release, whose parts carry no text form; the ledger grades the hex_bytes label this release adds, so the cell asserts both runs itself")
        .export_line("on_schema_drift: fail");
    let Some(old) = rig.as_previous_release() else {
        return;
    };
    let before = old.run();
    assert!(
        before.status.success(),
        "{}",
        String::from_utf8_lossy(&before.stderr)
    );
    let dir = rig.out_dir();
    assert_eq!(geo_delivered(&dir, "g", "gg"), want);
    for part in files_under(&dir) {
        std::fs::remove_file(part).expect("clear the previous release's parts");
    }
    let mine = rig.run();
    let err = String::from_utf8_lossy(&mine.stderr);
    assert_eq!(mine.status.code(), Some(0), "{err}");
    assert!(!err.contains("type changed"), "{err}");
    assert!(!err.contains("schema changed"), "{err}");
    assert_eq!(geo_delivered(&dir, "g", "gg"), want);
    assert_hex_bytes_labelled(&dir, "g");
    assert_hex_bytes_labelled(&dir, "gg");
}

const GEO_WAREHOUSE_BUCKET: &str = "rivet-qa-clickhouse-load";
const GEO_CH_PASSWORD_ENV: &str = "RIVET_TEST_CH_PASSWORD";

fn geo_table() -> (String, MssqlTable) {
    let table = unique_name("mssql_geo_wh");
    mssql_exec(&format!("CREATE TABLE dbo.{table} ({GEO_COLS})"));
    let guard = MssqlTable::adopt(table.clone());
    for sql in geo_inserts(&table) {
        mssql_exec(&sql);
    }
    (table, guard)
}

fn id_pipe(rows: &[Vec<Option<String>>]) -> Vec<String> {
    rows.iter()
        .map(|r| {
            r.iter()
                .map(|c| c.clone().unwrap_or_else(|| "NULL".to_string()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mssql"]
fn a_labelled_geometry_column_loads_into_clickhouse_and_unhex_recovers_the_clr_bytes() {
    require_alive(LiveService::Mssql);
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(GEO_WAREHOUSE_BUCKET);
    let (table, _guard) = geo_table();
    let want = geo_source("rivet-mssql-1", &table, &clr_hex("g"), &clr_hex("gg"));
    let db = ChDb::new();
    let rig = Rig::mssql_batch(&table)
        .dest_gcs(
            GEO_WAREHOUSE_BUCKET,
            &unique_name("geoch"),
            FAKE_GCS_ENDPOINT,
        )
        .top_line(&format!(
            "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {}, \
             user: {CLICKHOUSE_USER}, password_env: {GEO_CH_PASSWORD_ENV}, pk: [id] }}",
            db.0
        ));
    rig.run_ok();
    rig.load_ok(&[], &[(GEO_CH_PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
    let read = |g: &str, gg: &str| {
        id_pipe(&clickhouse_rows_json(&format!(
            "SELECT id, ifNull({g}, 'NULL'), ifNull({gg}, 'NULL') FROM {}.{table} ORDER BY id FORMAT JSONCompact",
            db.0
        )))
    };
    assert_eq!(read("g", "gg"), want, "clickhouse: the text as delivered");
    assert_eq!(
        read("lower(hex(unhex(g)))", "lower(hex(unhex(gg)))"),
        want,
        "clickhouse: unhex(x) recovers the CLR bytes"
    );
}

#[test]
#[ignore = "live: requires BigQuery creds + mssql"]
fn a_labelled_geometry_column_loads_into_bigquery_and_from_hex_recovers_the_clr_bytes() {
    let Some(bq) = BqLive::from_env("geo_bq") else {
        return;
    };
    require_alive(LiveService::Mssql);
    let (table, _guard) = geo_table();
    let want = geo_source("rivet-mssql-1", &table, &clr_hex("g"), &clr_hex("gg"));
    let _cleanup = bq.cleanup(&[&table]);
    let rig = Rig::mssql_batch(&table)
        .dest_gcs_live(&bq.bucket, &format!("{}/{table}", bq.prefix))
        .top_line(&bq.load_line(""));
    rig.run_ok();
    rig.load_ok(&[], &[]);
    let read = |g: &str, gg: &str| {
        let rows = bq.read_bq_rows(&format!(
            "SELECT id, IFNULL({g}, 'NULL') AS a, IFNULL({gg}, 'NULL') AS b FROM `{}.{}.{table}` ORDER BY id",
            bq.project, bq.dataset
        ));
        id_pipe(
            &rows
                .iter()
                .map(|r| vec![json_text(&r["id"]), json_text(&r["a"]), json_text(&r["b"])])
                .collect::<Vec<_>>(),
        )
    };
    assert_eq!(read("g", "gg"), want, "bigquery: the text as delivered");
    assert_eq!(
        read("LOWER(TO_HEX(FROM_HEX(g)))", "LOWER(TO_HEX(FROM_HEX(gg)))"),
        want,
        "bigquery: FROM_HEX(x) recovers the CLR bytes"
    );
}

/// A read held behind a schema lock for over 30 s completes. It does not go red when
/// `command_timeout(None)` is removed: tiberius 0.13's 30 s default did not fire while the
/// server held the lock (measured 2026-09-30), so this guards the outcome, not that setting.
#[test]
#[ignore = "live+gate-only: requires docker compose mssql; quarantined from CI, where the reader was not blocked on 3 of 4 runs (cause open)"]
fn a_read_blocked_longer_than_thirty_seconds_still_completes() {
    require_alive(LiveService::Mssql);
    let table = format!("rivet.dbo.{}", unique_name("mssql_slow"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL);\nINSERT INTO {table} VALUES (1,1);"
    ));
    let _guard = MssqlTable::adopt(table.clone());
    let rig = Rig::mssql_batch("mssql_slow")
        .query(&format!("SELECT id, v FROM {table}"))
        .source_line("tuning:")
        .source_line("  lock_timeout_s: 0")
        .source_line("  statement_timeout_s: 0")
        .source_line("  max_retries: 0");
    // A schema-modification lock blocks every reader, snapshot isolation included.
    let tx = format!(
        "BEGIN TRAN; ALTER TABLE {table} ADD blocker INT NULL;\nWAITFOR DELAY '00:01:30';\nROLLBACK;"
    );
    // Resolved before the lock: OBJECT_ID itself waits on the schema lock it would look for.
    let oid = mssql_query_i64(&format!("SELECT OBJECT_ID('{table}')"));
    let writer = std::thread::spawn(move || mssql_exec(&tx));
    let t0 = std::time::Instant::now();
    while mssql_query_i64(&format!(
        "SELECT COUNT(*) FROM sys.dm_tran_locks WHERE request_mode = 'Sch-M' \
         AND resource_associated_entity_id = {oid}"
    )) == 0
    {
        assert!(
            t0.elapsed().as_secs() < 5,
            "fixture: the writer never took the schema lock"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let started = std::time::Instant::now();
    let said = rig.run_ok_capture();
    let took = started.elapsed();
    writer.join().expect("writer thread");
    assert!(
        took.as_secs() >= 31,
        "the read was not blocked past tiberius' 30 s default ({took:?}), so this proves nothing:\n{said}"
    );
    assert_eq!(duckdb_dir_parquet_i64(&rig.out_dir(), "v"), vec![1]);
}
