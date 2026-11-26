//! MySQL spatial columns are delivered as the lowercase hex of standard WKB, batch parquet and csv.
//!
//! The oracle is the server's own `HEX(ST_AsBinary(col))` read through a second client, never
//! rivet's counters. A non-zero SRID is refused per value with its coded remedy.

use crate::common::*;
use mysql::prelude::Queryable as _;

const COLS: &str = "id INT PRIMARY KEY, g GEOMETRY, p POINT, l LINESTRING, pg POLYGON, gc GEOMETRYCOLLECTION, mp MULTIPOINT";
const SPATIAL: [&str; 6] = ["g", "p", "l", "pg", "gc", "mp"];

fn create_and_seed(table: &str) -> MysqlTable {
    let mut c = mysql_connect();
    c.query_drop(format!("CREATE TABLE {table} ({COLS})"))
        .unwrap();
    let guard = MysqlTable::adopt(table.to_string());
    for v in [
        "1, ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('LINESTRING(0 0,1 1,2 3)'), \
         ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'), ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))'), \
         ST_GeomFromText('MULTIPOINT((1 2),(3 4))')",
        "2, ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'), NULL, NULL, NULL, ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'), NULL",
        "3, ST_GeomFromText('MULTIPOINT((9 9))'), NULL, NULL, NULL, NULL, NULL",
        "4, NULL, NULL, NULL, NULL, NULL, NULL",
    ] {
        c.query_drop(format!("INSERT INTO {table} VALUES ({v})"))
            .unwrap();
    }
    guard
}

/// The server's own standard-WKB hex per row as `[id, g, p, l, pg, gc, mp]`, `None` for SQL NULL.
fn source_wkb_hex(table: &str) -> Vec<Vec<Option<String>>> {
    let mut c = mysql_connect();
    let cols = SPATIAL
        .iter()
        .map(|s| format!("LOWER(HEX(ST_AsBinary({s})))"))
        .collect::<Vec<_>>()
        .join(", ");
    let rows: Vec<mysql::Row> = c
        .query(format!("SELECT id, {cols} FROM {table} ORDER BY id"))
        .unwrap();
    rows.into_iter()
        .map(|mut r| {
            (0..=SPATIAL.len())
                .map(|i| r.take::<Option<String>, _>(i).unwrap())
                .collect()
        })
        .collect()
}

fn delivered_parquet(dir: &std::path::Path) -> Vec<Vec<Option<String>>> {
    use arrow::array::{Array, AsArray};
    let mut out = Vec::new();
    for b in read_all_parts(dir) {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_primitive::<arrow::datatypes::Int32Type>()
            .clone();
        for i in 0..b.num_rows() {
            let mut row = vec![Some(ids.value(i).to_string())];
            for s in SPATIAL {
                let col = b.column_by_name(s).unwrap().as_string::<i32>();
                row.push((!col.is_null(i)).then(|| col.value(i).to_string()));
            }
            out.push(row);
        }
    }
    out.sort_by_key(|r| r[0].clone().unwrap().parse::<i64>().unwrap());
    out
}

fn read_csv(dir: &std::path::Path) -> Vec<Vec<Option<String>>> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "csv"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no csv part in {}", dir.display());
    let mut out: Vec<Vec<Option<String>>> = Vec::new();
    for f in files {
        for line in std::fs::read_to_string(f).unwrap().lines().skip(1) {
            out.push(
                line.split(',')
                    .map(|v| (!v.is_empty()).then(|| v.to_string()))
                    .collect(),
            );
        }
    }
    out.sort_by_key(|r| r[0].clone().unwrap().parse::<i64>().unwrap());
    out
}

fn rig(table: &str) -> Rig {
    Rig::mysql_batch(table)
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_batch_parquet_export_delivers_every_spatial_type_as_the_servers_wkb_hex() {
    require_alive(LiveService::Mysql);
    let table = unique_name("mysql_spatial");
    let _guard = create_and_seed(&table);
    let want = source_wkb_hex(&table);
    assert_eq!(want.len(), 4, "oracle rows: {want:?}");
    let rig = rig(&table);
    rig.run_ok();
    assert_eq!(delivered_parquet(&rig.out_dir()), want);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_batch_csv_export_delivers_every_spatial_type_as_the_servers_wkb_hex() {
    require_alive(LiveService::Mysql);
    let table = unique_name("mysql_spatial_csv");
    let _guard = create_and_seed(&table);
    let want = source_wkb_hex(&table);
    let rig = rig(&table).with_format("csv");
    rig.run_ok();
    assert_eq!(read_csv(&rig.out_dir()), want);
}

fn parquet_parts(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .map(|d| {
            d.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
                .count()
        })
        .unwrap_or(0)
}

fn seed_srid(table: &str) -> MysqlTable {
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, g GEOMETRY)"
    ))
    .unwrap();
    let guard = MysqlTable::adopt(table.to_string());
    for v in [
        "1, ST_GeomFromText('POINT(1 2)')",
        "2, ST_GeomFromText('POINT(1 2)', 4326)",
    ] {
        c.query_drop(format!("INSERT INTO {table} VALUES ({v})"))
            .unwrap();
    }
    guard
}

/// A value with an SRID other than 0 is refused by code naming the column and both remedies, and nothing is written.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_spatial_value_with_an_srid_is_refused_by_code_with_its_remedies() {
    require_alive(LiveService::Mysql);
    let table = unique_name("mysql_spatial_srid");
    let _guard = seed_srid(&table);
    let refused = rig(&table);
    let run = refused.run();
    let err = String::from_utf8_lossy(&run.stderr);
    assert_eq!(run.status.code(), Some(5), "{err}");
    assert!(err.contains("RIVET_SOURCE_VALUE_UNREPRESENTABLE"), "{err}");
    assert!(
        err.contains(
            "mysql: column `g`: a spatial value has SRID 4326, which the hex_wkb form cannot carry"
        ),
        "{err}"
    );
    assert!(
        err.contains("Declare the column `binary` in `columns:` to deliver MySQL's internal bytes, or select ST_AsBinary(col) and ST_SRID(col) in the export query"),
        "{err}"
    );
    assert_eq!(
        parquet_parts(&refused.out_dir()),
        0,
        "a refused run wrote a part"
    );
}

/// The refusal's remedies deliver: `binary` writes the internal bytes the server holds, and `ST_AsBinary(col)` with `ST_SRID(col)` writes the server's WKB and SRID.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn the_remedies_of_the_srid_refusal_deliver_what_the_server_holds() {
    require_alive(LiveService::Mysql);
    let table = unique_name("mysql_spatial_srid_fix");
    let _guard = seed_srid(&table);
    let held: Vec<(i32, String, String, u32)> = mysql_connect()
        .query(format!(
            "SELECT id, HEX(g), LOWER(HEX(ST_AsBinary(g))), ST_SRID(g) FROM {table} ORDER BY id"
        ))
        .unwrap();
    let binary = rig(&table)
        .no_oracle(
            "spatial: a binary override delivers the internal bytes the test reads as HEX(g)",
        )
        .export_line("columns: {g: binary}");
    binary.run_ok();
    let got = dir_parquet_display(&binary.out_dir(), "g");
    let want: Vec<_> = held.iter().map(|r| Some(r.1.clone())).collect();
    let got: Vec<_> = got
        .into_iter()
        .map(|v| v.map(|v| v.to_ascii_uppercase()))
        .collect();
    assert_eq!(got, want, "`binary` delivers the internal bytes");
    let name = unique_name("mysql_spatial_srid_query");
    let query = Rig::mysql_batch(&name)
        .no_oracle("spatial: the oracle is the server's ST_AsBinary and ST_SRID read by this test")
        .query(&format!(
            "SELECT id, ST_AsBinary(g) AS wkb, ST_SRID(g) AS srid FROM {table}"
        ));
    query.run_ok();
    let wkb: Vec<_> = dir_parquet_display(&query.out_dir(), "wkb")
        .into_iter()
        .map(|v| v.map(|v| v.to_ascii_lowercase()))
        .collect();
    assert_eq!(
        wkb,
        held.iter().map(|r| Some(r.2.clone())).collect::<Vec<_>>()
    );
    let srid = dir_parquet_display(&query.out_dir(), "srid");
    assert_eq!(
        srid,
        held.iter()
            .map(|r| Some(r.3.to_string()))
            .collect::<Vec<_>>()
    );
}

fn cdc_source_wkb_hex(table: &str) -> std::collections::BTreeMap<i64, Vec<Option<String>>> {
    let cols = SPATIAL
        .iter()
        .map(|s| format!("LOWER(HEX(ST_AsBinary({s})))"))
        .collect::<Vec<_>>()
        .join(", ");
    let rows: Vec<mysql::Row> = cdc_conn()
        .query(format!("SELECT id, {cols} FROM {table}"))
        .unwrap();
    rows.into_iter()
        .map(|mut r| {
            let id: i64 = r.take(0).unwrap();
            (
                id,
                (1..=SPATIAL.len())
                    .map(|i| r.take::<Option<String>, _>(i).unwrap())
                    .collect(),
            )
        })
        .collect()
}

type SpatialCells = std::collections::BTreeMap<i64, Vec<Vec<Option<String>>>>;

/// Every spatial cell the parts under `dir` hold by id, with the `rivet.text_form` of each spatial column of each part.
fn parquet_cdc_cells(dir: &std::path::Path) -> (SpatialCells, Vec<Option<String>>) {
    use arrow::array::{Array, AsArray};
    let mut cells = SpatialCells::default();
    let mut forms = Vec::new();
    let mut dirs: Vec<_> = files_under(dir)
        .into_iter()
        .filter(|f| f.extension().is_some_and(|x| x == "parquet"))
        .filter_map(|f| f.parent().map(std::path::Path::to_path_buf))
        .collect();
    dirs.sort();
    dirs.dedup();
    for d in dirs {
        for b in read_all_parts(&d) {
            if b.column_by_name("g").is_none() {
                continue;
            }
            let schema = b.schema();
            forms = SPATIAL
                .iter()
                .map(|s| {
                    schema
                        .field_with_name(s)
                        .unwrap()
                        .metadata()
                        .get("rivet.text_form")
                        .cloned()
                })
                .collect();
            let ids = arrow::compute::cast(
                b.column_by_name("id").unwrap(),
                &arrow::datatypes::DataType::Int64,
            )
            .unwrap();
            let ids = ids.as_primitive::<arrow::datatypes::Int64Type>().clone();
            for i in 0..b.num_rows() {
                let row = SPATIAL
                    .iter()
                    .map(|s| {
                        let c = b.column_by_name(s).unwrap().as_string::<i32>();
                        (!c.is_null(i)).then(|| c.value(i).to_string())
                    })
                    .collect();
                cells.entry(ids.value(i)).or_default().push(row);
            }
        }
    }
    (cells, forms)
}

/// CDC delivers every spatial type as the same standard-WKB hex the batch does, from the snapshot leg and from the binlog.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_cdc_run_delivers_every_spatial_type_as_the_servers_wkb_hex_from_snapshot_and_binlog() {
    let table = unique_name("mysql_spatial_cdc");
    let _guard = MysqlCdcTable(table.clone());
    let mut c = cdc_conn();
    c.query_drop(format!("CREATE TABLE {table} ({COLS})"))
        .unwrap();
    let insert = |c: &mut mysql::PooledConn, rows: &[&str]| {
        for v in rows {
            c.query_drop(format!("INSERT INTO {table} VALUES ({v})"))
                .unwrap();
        }
    };
    let point = "ST_GeomFromText('POINT(1 2)')";
    insert(
        &mut c,
        &[&format!(
            "1, {point}, {point}, ST_GeomFromText('LINESTRING(0 0,1 1,2 3)'), ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'), ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))'), ST_GeomFromText('MULTIPOINT((1 2),(3 4))')"
        )],
    );
    let rig = Rig::mysql_cdc(&table).cdc("initial: snapshot");
    rig.run_ok();
    insert(
        &mut c,
        &[
            "2, ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'), NULL, NULL, NULL, ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'), NULL",
            "3, ST_GeomFromText('MULTIPOINT((9 9))'), NULL, NULL, NULL, NULL, NULL",
            "4, NULL, NULL, NULL, NULL, NULL, NULL",
        ],
    );
    rig.run_ok();
    let want = cdc_source_wkb_hex(&table);
    let (got, forms) = parquet_cdc_cells(&rig.out_dir());
    assert_eq!(forms, vec![Some("hex_wkb".to_string()); SPATIAL.len()]);
    for (id, cells) in &got {
        for cell in cells {
            assert_eq!(cell, &want[id], "id {id}");
        }
    }
    assert_eq!(got.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
}

/// A stream refuses an SRID value by code with the stream's remedy, and delivers nothing past it.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_cdc_stream_refuses_a_spatial_value_with_an_srid_by_code_with_the_streams_remedy() {
    let table = unique_name("mysql_spatial_cdc_srid");
    let _guard = MysqlCdcTable(table.clone());
    let mut c = cdc_conn();
    c.query_drop(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, g GEOMETRY)"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} VALUES (1, ST_GeomFromText('POINT(1 2)'))"
    ))
    .unwrap();
    let rig = Rig::mysql_cdc(&table).cdc("initial: snapshot");
    rig.run_ok();
    c.query_drop(format!(
        "INSERT INTO {table} VALUES (2, ST_GeomFromText('POINT(3 4)', 4326))"
    ))
    .unwrap();
    let said = rig.run_expect_fail();
    assert!(
        said.contains("RIVET_SOURCE_VALUE_UNREPRESENTABLE"),
        "{said}"
    );
    assert!(
        said.contains("a spatial value has SRID 4326, which the hex_wkb form cannot carry"),
        "{said}"
    );
    assert!(
        said.contains("A stream has no query to select ST_AsBinary(col) in: declare the column `binary` in `columns:`"),
        "{said}"
    );
    assert!(!said.contains("export query"), "{said}");
}

/// The stream's remedy delivers: `binary` carries the internal bytes the server holds, from the snapshot and from the binlog.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn the_stream_remedy_of_the_srid_refusal_delivers_what_the_server_holds() {
    let table = unique_name("mysql_spatial_cdc_srid_fix");
    let _guard = MysqlCdcTable(table.clone());
    let mut c = cdc_conn();
    c.query_drop(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, g GEOMETRY)"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} VALUES (1, ST_GeomFromText('POINT(1 2)', 4326))"
    ))
    .unwrap();
    let rig = Rig::mysql_cdc(&table)
        .cdc("initial: snapshot")
        .export_line("columns: {g: binary}")
        .no_oracle("spatial: the oracle is the server's HEX(g) read by this test");
    rig.run_ok();
    c.query_drop(format!(
        "INSERT INTO {table} VALUES (2, ST_GeomFromText('POINT(3 4)', 4326))"
    ))
    .unwrap();
    rig.run_ok();
    let held: Vec<(i64, String)> = c
        .query(format!("SELECT id, HEX(g) FROM {table} ORDER BY id"))
        .unwrap();
    let batches = delivered_batches(&rig);
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        held.len()
    );
    assert_eq!(geometry_by_id(&batches), held.into_iter().collect());
}

fn delivered_batches(rig: &Rig) -> Vec<arrow::record_batch::RecordBatch> {
    files_under(&rig.out_dir())
        .into_iter()
        .filter(|f| f.extension().is_some_and(|x| x == "parquet"))
        .filter_map(|f| f.parent().map(std::path::Path::to_path_buf))
        .collect::<std::collections::BTreeSet<_>>()
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect()
}

fn geometry_by_id(
    batches: &[arrow::record_batch::RecordBatch],
) -> std::collections::BTreeMap<i64, String> {
    let mut got = std::collections::BTreeMap::new();
    for b in batches {
        let ids = arrow::compute::cast(
            b.column_by_name("id").unwrap(),
            &arrow::datatypes::DataType::Int64,
        )
        .unwrap();
        let ids = arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(&ids).clone();
        let g = b.column_by_name("g").unwrap();
        for i in 0..b.num_rows() {
            got.insert(
                ids.value(i),
                arrow::util::display::array_value_to_string(g, i)
                    .unwrap()
                    .to_ascii_uppercase(),
            );
        }
    }
    got
}

fn upgrade_over_a_refused_baseline(policy: &str) {
    require_alive(LiveService::Mysql);
    let table = unique_name(&format!("mysql_spatial_up_{policy}"));
    let _guard = create_and_seed(&table);
    let want: Vec<_> = source_wkb_hex(&table)
        .into_iter()
        .map(|r| r[1].clone())
        .collect();
    let rig = Rig::mysql_batch(&table)
        .query(&format!("SELECT id, g FROM {table}"))
        .export_line(&format!("on_schema_drift: {policy}"));
    let Some(old) = rig.as_previous_release() else {
        return;
    };
    let refused = old.run();
    assert!(
        !refused.status.success(),
        "the previous release refuses a spatial column"
    );
    let recorded = "SELECT COUNT(*) FROM export_schema WHERE export_name = '{export}' AND columns_json LIKE '%\"name\":\"g\",\"type\":\"unknown\"%'";
    assert_eq!(
        rig.state_count(recorded),
        1,
        "the refused run recorded the `unknown` baseline"
    );
    let mine = rig.run();
    let err = String::from_utf8_lossy(&mine.stderr);
    assert_eq!(mine.status.code(), Some(0), "{err}");
    assert!(!err.contains("type changed"), "{err}");
    assert!(!err.contains("schema changed"), "{err}");
    let got: Vec<_> = dir_parquet_display(&rig.out_dir(), "g")
        .into_iter()
        .collect();
    assert_eq!(got, want, "the upgraded run delivers the server's WKB hex");
    let again = rig.run();
    assert_eq!(
        again.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&again.stderr)
    );
}

/// The baseline a previous release recorded for a refused spatial column is not drift under `fail` once the column is delivered.
#[test]
#[ignore = "live: requires docker compose mysql + RIVET_PREV_RELEASE_BIN"]
fn a_spatial_column_a_previous_release_refused_is_not_drift_under_fail() {
    upgrade_over_a_refused_baseline("fail");
}

/// The same under the default `warn`: no drift warning is raised for the upgrade.
#[test]
#[ignore = "live: requires docker compose mysql + RIVET_PREV_RELEASE_BIN"]
fn a_spatial_column_a_previous_release_refused_is_not_drift_under_warn() {
    upgrade_over_a_refused_baseline("warn");
}

const CDC_DRIFT_FAIL: &str = "on_schema_drift: fail";

/// A spatial column added to a stream is drift under `fail`: exit 4 naming the column, the baseline unchanged.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_spatial_column_added_to_a_cdc_stream_is_drift_under_fail() {
    let table = unique_name("mysql_spatial_cdc_add");
    let _guard = MysqlCdcTable(table.clone());
    let mut c = cdc_conn();
    c.query_drop(format!("CREATE TABLE {table} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let rig = Rig::mysql_cdc(&table).export_line(CDC_DRIFT_FAIL);
    rig.run_ok();
    c.query_drop(format!("ALTER TABLE {table} ADD COLUMN g GEOMETRY"))
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} VALUES (1, 1, ST_GeomFromText('POINT(1 2)'))"
    ))
    .unwrap();
    let said = rig.run_expect_fail();
    assert!(said.contains("added: g"), "{said}");
    assert_eq!(rig.run().status.code(), Some(4), "{said}");
    let recorded = "SELECT COUNT(*) FROM export_schema WHERE export_name = '{export}' AND columns_json LIKE '%\"name\":\"g\"%'";
    assert_eq!(rig.state_count(recorded), 0, "the baseline did not adopt g");
}

/// Under `warn` the same added column is reported and delivered as the server's WKB hex.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_spatial_column_added_to_a_cdc_stream_is_reported_and_delivered_under_warn() {
    let table = unique_name("mysql_spatial_cdc_addw");
    let _guard = MysqlCdcTable(table.clone());
    let mut c = cdc_conn();
    c.query_drop(format!("CREATE TABLE {table} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let rig = Rig::mysql_cdc(&table).export_line("on_schema_drift: warn");
    rig.run_ok();
    c.query_drop(format!("ALTER TABLE {table} ADD COLUMN g GEOMETRY"))
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} VALUES (1, 1, ST_GeomFromText('POINT(1 2)'))"
    ))
    .unwrap();
    let out = rig.run();
    let said = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(0), "{said}");
    assert!(said.contains("added: g"), "{said}");
    let batches = delivered_batches(&rig);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(geometry_by_id(&batches), server_geometry(&mut c, &table));
}

/// A CDC baseline a previous release wrote already holds the spatial column as `Utf8`, so this build reads no drift in it under `fail`.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + RIVET_PREV_RELEASE_BIN"]
fn a_cdc_baseline_a_previous_release_wrote_for_a_spatial_column_is_not_drift_under_fail() {
    let table = unique_name("mysql_spatial_cdc_old");
    let _guard = MysqlCdcTable(table.clone());
    let mut c = cdc_conn();
    c.query_drop(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, v INT, g GEOMETRY)"
    ))
    .unwrap();
    let rig = Rig::mysql_cdc(&table).export_line(CDC_DRIFT_FAIL);
    let Some(old) = rig.as_previous_release() else {
        return;
    };
    let anchored = old.run();
    assert_eq!(
        anchored.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&anchored.stderr)
    );
    let held = "SELECT COUNT(*) FROM export_schema WHERE export_name = '{export}' AND columns_json LIKE '%\"name\":\"g\",\"type\":\"Utf8\"%'";
    assert_eq!(
        rig.state_count(held),
        1,
        "the previous release recorded g as Utf8"
    );
    c.query_drop(format!(
        "INSERT INTO {table} VALUES (1, 1, ST_GeomFromText('POINT(1 2)'))"
    ))
    .unwrap();
    let mine = rig.run();
    let said = String::from_utf8_lossy(&mine.stderr);
    assert_eq!(mine.status.code(), Some(0), "{said}");
    assert!(!said.contains("schema drift"), "{said}");
    let batches = delivered_batches(&rig);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(geometry_by_id(&batches), server_geometry(&mut c, &table));
}

/// A row a previous release streamed for a spatial column keeps its form `\x` + hex of the server's SRID and WKB bytes; the row this build streams after it is the WKB hex, with no drift under `fail`.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + RIVET_PREV_RELEASE_BIN"]
fn rows_a_previous_release_streamed_for_a_spatial_column_keep_their_form_and_the_next_stream_row_is_wkb_hex()
 {
    let table = unique_name("mysql_spatial_cdc_mixed");
    let _guard = MysqlCdcTable(table.clone());
    let mut c = cdc_conn();
    c.query_drop(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, v INT, g GEOMETRY)"
    ))
    .unwrap();
    let rig = Rig::mysql_cdc(&table)
        .no_oracle(
            "spatial: the previous release streams the server's SRID and WKB bytes as `\\x` text, which the oracle reads as a disagreement with the source",
        )
        .export_line(CDC_DRIFT_FAIL);
    let Some(old) = rig.as_previous_release() else {
        return;
    };
    let anchored = old.run();
    assert_eq!(
        anchored.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&anchored.stderr)
    );
    c.query_drop(format!(
        "INSERT INTO {table} VALUES (1, 1, ST_GeomFromText('POINT(1 2)'))"
    ))
    .unwrap();
    let streamed_old = old.run();
    assert_eq!(
        streamed_old.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&streamed_old.stderr)
    );
    c.query_drop(format!(
        "INSERT INTO {table} VALUES (2, 2, ST_GeomFromText('LINESTRING(0 0,1 1)'))"
    ))
    .unwrap();
    let mine = rig.run();
    let said = String::from_utf8_lossy(&mine.stderr);
    assert_eq!(mine.status.code(), Some(0), "{said}");
    assert!(!said.contains("schema drift"), "{said}");
    let wkb = server_geometry(&mut c, &table);
    let internal: String = c
        .query_first(format!("SELECT HEX(g) FROM {table} WHERE id = 1"))
        .unwrap()
        .unwrap();
    let batches = delivered_batches(&rig);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    let got = geometry_by_id(&batches);
    assert_eq!(
        got.get(&1),
        Some(&format!("\\X{internal}")),
        "the row the previous release streamed keeps its form"
    );
    assert_eq!(
        got.get(&2),
        wkb.get(&2),
        "the row this build streams is the WKB hex"
    );
    assert_eq!(got.len(), 2);
}

fn server_geometry(
    c: &mut impl mysql::prelude::Queryable,
    table: &str,
) -> std::collections::BTreeMap<i64, String> {
    let rows: Vec<(i64, String)> = c
        .query(format!(
            "SELECT id, HEX(ST_AsBinary(g)) FROM {table} ORDER BY id"
        ))
        .unwrap();
    rows.into_iter().collect()
}

fn ring_wkt(points: usize) -> String {
    let ring: Vec<String> = (0..points)
        .map(|i| format!("{} {}", i, (i * i) % 7))
        .collect();
    format!("POLYGON(({},{}))", ring.join(","), ring[0])
}

fn seed_shapes(table: &str, big_points: usize) -> MysqlTable {
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, mls MULTILINESTRING, mpg MULTIPOLYGON, g GEOMETRY)"
    ))
    .unwrap();
    let guard = MysqlTable::adopt(table.to_string());
    for v in [
        "1, ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3,4 5))'), \
         ST_GeomFromText('MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0)),((10 10,12 10,12 12,10 10)))'), \
         ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2),GEOMETRYCOLLECTION(LINESTRING(0 0,1 1),POINT(3 3)))')"
            .to_string(),
        "2, NULL, NULL, ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')".to_string(),
        format!(
            "3, NULL, NULL, ST_GeomFromText('{}')",
            ring_wkt(big_points)
        ),
        "4, NULL, NULL, NULL".to_string(),
    ] {
        c.query_drop(format!("INSERT INTO {table} VALUES ({v})"))
            .unwrap();
    }
    guard
}

fn shapes_from_the_server(table: &str, col: &str) -> Vec<Option<String>> {
    mysql_connect()
        .query(format!(
            "SELECT LOWER(HEX(ST_AsBinary({col}))) FROM {table} ORDER BY id"
        ))
        .unwrap()
}

/// MULTILINESTRING, MULTIPOLYGON, an empty and a nested collection, a polygon of 640 KB and NULL are delivered as the server's ST_AsBinary hex.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn the_multi_types_a_large_polygon_empty_and_nested_collections_and_null_are_delivered_as_the_servers_wkb_hex()
 {
    require_alive(LiveService::Mysql);
    let table = unique_name("mysql_spatial_shapes");
    let _guard = seed_shapes(&table, 40_000);
    let rig = rig(&table).query(&format!("SELECT id, mls, mpg, g FROM {table}"));
    rig.run_ok();
    for col in ["mls", "mpg", "g"] {
        let want = shapes_from_the_server(&table, col);
        assert_eq!(want.len(), 4, "{col}: {want:?}");
        assert_eq!(dir_parquet_display(&rig.out_dir(), col), want, "{col}");
    }
    let big = shapes_from_the_server(&table, "g")[2].clone().unwrap();
    assert!(
        big.len() > 1_200_000,
        "a 640 KB polygon holds {} hex digits",
        big.len()
    );
}

/// Under `max_value_mb: 1` a 640 KB geometry is delivered like a `binary` column, whose hex is twice the ceiling, and one over the ceiling is refused by code.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_spatial_value_is_held_to_the_value_ceiling_on_its_bytes_like_a_binary_column() {
    require_alive(LiveService::Mysql);
    let held = unique_name("mysql_spatial_ceiling_ok");
    let _g1 = seed_shapes(&held, 40_000);
    let ok = rig(&held)
        .query(&format!("SELECT id, g FROM {held}"))
        .export_line("tuning: { max_value_mb: 1 }");
    ok.run_ok();
    assert_eq!(
        dir_parquet_display(&ok.out_dir(), "g"),
        shapes_from_the_server(&held, "g")
    );
    let over = unique_name("mysql_spatial_ceiling_over");
    let _g2 = seed_shapes(&over, 80_000);
    let refused = rig(&over)
        .query(&format!("SELECT id, g FROM {over}"))
        .export_line("tuning: { max_value_mb: 1 }");
    let run = refused.run();
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(err.contains("RIVET_SOURCE_VALUE_TOO_LARGE"), "{err}");
    assert!(err.contains("column 'g' has a single value of"), "{err}");
}

/// A json override of a GEOMETRYCOLLECTION column is refused by exit 5 with its coded error, naming the column.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_json_override_of_a_geometrycollection_is_refused_by_code_with_exit_5() {
    require_alive(LiveService::Mysql);
    let table = unique_name("mysql_spatial_json_gc");
    let _guard = create_and_seed(&table);
    let rig = rig(&table).export_line("columns: {gc: json}");
    let out = rig.run();
    assert_refused(
        &out,
        Refused::by_code("RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("`gc`"),
        "the refusal names the column: {stderr}"
    );
}
