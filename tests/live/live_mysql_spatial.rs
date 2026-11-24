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

fn delivered_csv(dir: &std::path::Path) -> Vec<Vec<Option<String>>> {
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
        .no_oracle("spatial: the oracle is the server's HEX(ST_AsBinary(col)) read by this test")
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
    assert_eq!(delivered_csv(&rig.out_dir()), want);
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
    let binary = rig(&table).export_line("columns: {g: binary}");
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
fn cdc_delivered(dir: &std::path::Path) -> (SpatialCells, Vec<Option<String>>) {
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
    let (got, forms) = cdc_delivered(&rig.out_dir());
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
    let rig = Rig::mysql_cdc(&table)
        .cdc("initial: snapshot")
        .no_oracle("spatial: the oracle is the server's ST_AsBinary read by this test");
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
    let mut got = std::collections::BTreeMap::new();
    for b in files_under(&rig.out_dir())
        .into_iter()
        .filter(|f| f.extension().is_some_and(|x| x == "parquet"))
        .filter_map(|f| f.parent().map(std::path::Path::to_path_buf))
        .collect::<std::collections::BTreeSet<_>>()
        .iter()
        .flat_map(|d| read_all_parts(d))
    {
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
    assert_eq!(got, held.into_iter().collect());
}
