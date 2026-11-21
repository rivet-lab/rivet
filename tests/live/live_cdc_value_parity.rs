//! Batch and CDC deliver the same value for the same source cell.
//! The oracle is the source read through the simple-query protocol (the server's own output text), never rivet's counters.
//!
//! ```text
//! cargo nextest run --test live_suite --run-ignored only -E 'test(/^live_cdc_value_parity::/)'
//! ```

use crate::common::*;
use arrow::array::{Array, Float32Array, Float64Array, StringArray};
use postgres::NoTls;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Text,
    F32,
    F64,
}

#[derive(Clone)]
struct Col {
    name: String,
    ddl: String,
    kind: Kind,
    lits: Vec<String>,
}

fn col(name: &str, ddl: &str, kind: Kind, lits: &[&str]) -> Col {
    Col {
        name: name.to_string(),
        ddl: ddl.to_string(),
        kind,
        lits: lits.iter().map(|s| s.to_string()).collect(),
    }
}

fn text(name: &str, ddl: &str, lits: &[&str]) -> Col {
    col(name, ddl, Kind::Text, lits)
}

/// The rows of `cols`: one per literal position, then one all-NULL row.
fn rows_of(cols: &[Col]) -> Vec<String> {
    let n = cols.iter().map(|c| c.lits.len()).max().unwrap_or(0);
    let mut rows: Vec<String> = (0..n)
        .map(|i| {
            let vals: Vec<&str> = cols
                .iter()
                .map(|c| c.lits.get(i).map_or("NULL", String::as_str))
                .collect();
            format!("({}, {})", i + 1, vals.join(", "))
        })
        .collect();
    rows.push(format!(
        "({}, {})",
        n + 1,
        vec!["NULL"; cols.len()].join(", ")
    ));
    rows
}

fn ddl_of(cols: &[Col]) -> String {
    let body: Vec<String> = cols
        .iter()
        .map(|c| format!("{} {}", c.name, c.ddl))
        .collect();
    format!("id BIGINT PRIMARY KEY, {}", body.join(", "))
}

fn insert_of(table: &str, cols: &[Col]) -> String {
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    format!(
        "INSERT INTO {table} (id, {}) VALUES {}",
        names.join(", "),
        rows_of(cols).join(", ")
    )
}

/// Every selected column of the table as the server prints it, by id.
fn source_text(url: &str, table: &str, cols: &[Col]) -> Vec<Vec<Option<String>>> {
    let mut pg = postgres::Client::connect(url, NoTls).expect("connect the oracle session");
    pg.simple_query("SET datestyle = 'ISO, MDY'; SET TimeZone = 'UTC'; SET extra_float_digits = 3")
        .expect("the oracle session settings");
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    pg.simple_query(&format!(
        "SELECT id, {} FROM {table} ORDER BY id",
        names.join(", ")
    ))
    .expect("the source's rows")
    .into_iter()
    .filter_map(|m| match m {
        postgres::SimpleQueryMessage::Row(r) => {
            Some((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect())
        }
        _ => None,
    })
    .collect()
}

/// The comparable form of a source text cell of `kind`.
fn source_form(kind: Kind, v: &str) -> String {
    match kind {
        Kind::Text => v.to_string(),
        Kind::F64 => format!(
            "f64:{:016x}",
            norm64(v.parse::<f64>().expect("a float text"))
        ),
        Kind::F32 => format!(
            "f64:{:016x}",
            norm64(v.parse::<f32>().expect("a float text") as f64)
        ),
    }
}

fn norm64(v: f64) -> u64 {
    if v.is_nan() {
        f64::NAN.to_bits()
    } else {
        v.to_bits()
    }
}

/// Every delivered cell by id, in `cols` order, as a comparable string or NULL; only rows whose `__op` is in `ops` when given.
fn delivered(
    dir: &std::path::Path,
    cols: &[Col],
    ops: Option<&[&str]>,
) -> std::collections::BTreeMap<i64, Vec<Option<String>>> {
    let mut out = std::collections::BTreeMap::new();
    for b in read_all_parts(dir) {
        let schema = b.schema();
        let at = |name: &str| {
            schema.index_of(name).unwrap_or_else(|_| {
                panic!("column {name} is not delivered under {}", dir.display())
            })
        };
        let ids = arrow::compute::cast(b.column(at("id")), &arrow::datatypes::DataType::Int64)
            .expect("the id as int64");
        let ids = ids
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let ops_col = ops.map(|_| b.column(at("__op")).clone());
        for r in 0..b.num_rows() {
            if let (Some(o), Some(keep)) = (&ops_col, ops) {
                let op = arrow::util::display::array_value_to_string(o, r).unwrap();
                if !keep.contains(&op.as_str()) {
                    continue;
                }
            }
            let row =
                cols.iter()
                    .map(|c| {
                        let a = b.column(at(&c.name));
                        if a.is_null(r) {
                            return None;
                        }
                        Some(match c.kind {
                            Kind::Text => match a.as_any().downcast_ref::<StringArray>() {
                                Some(s) => s.value(r).to_string(),
                                None => format!(
                                    "<{} {}>",
                                    a.data_type(),
                                    arrow::util::display::array_value_to_string(a, r).unwrap()
                                ),
                            },
                            Kind::F64 => format!(
                                "f64:{:016x}",
                                norm64(a.as_any().downcast_ref::<Float64Array>().unwrap().value(r))
                            ),
                            Kind::F32 => format!(
                                "f64:{:016x}",
                                norm64(a.as_any().downcast_ref::<Float32Array>().unwrap().value(r)
                                    as f64)
                            ),
                        })
                    })
                    .collect();
            out.insert(ids.value(r), row);
        }
    }
    out
}

/// One line per column and mode whose delivered cells differ from the source's text, or a refusal that stopped the mode.
fn differences(
    mode: &str,
    cols: &[Col],
    want: &[Vec<Option<String>>],
    got: &std::collections::BTreeMap<i64, Vec<Option<String>>>,
) -> Vec<String> {
    let mut bad = Vec::new();
    for (c, spec) in cols.iter().enumerate() {
        for w in want {
            let id: i64 = w[0].as_deref().unwrap().parse().unwrap();
            let src = w[c + 1].as_deref().map(|v| source_form(spec.kind, v));
            let seen = got.get(&id).map(|r| r[c].clone());
            if seen != Some(src.clone()) {
                bad.push(format!(
                    "{mode}: {} ({}) id {id}: source {:?} delivered {:?}",
                    spec.name, spec.ddl, src, seen
                ));
            }
        }
    }
    bad
}

fn pg_families() -> Vec<Col> {
    let mut cols: Vec<Col> = crate::live_pg_server_text::FAMILIES
        .iter()
        .filter(|f| !f.1.starts_with("bit") && !f.1.starts_with("varbit"))
        .map(|f| text(f.0, f.1, &[f.2, f.3, f.4]))
        .collect();
    cols.extend([
        text(
            "x_inet",
            "inet",
            &[
                "'10.1.2.3'",
                "'10.1.2.3/32'",
                "'2001:db8::1/128'",
                "'::1'",
                "'10.1.2.0/24'",
                "'0.0.0.0/0'",
            ],
        ),
        text(
            "x_cidr",
            "cidr",
            &[
                "'10.0.0.0/8'",
                "'2001:db8::/32'",
                "'10.1.2.3/32'",
                "'::/0'",
                "'::1/128'",
                "'192.168.1.0/24'",
            ],
        ),
        text(
            "x_mac",
            "macaddr",
            &[
                "'08:00:2b:01:02:03'",
                "'ff:ff:ff:ff:ff:ff'",
                "'00:00:00:00:00:00'",
            ],
        ),
        text(
            "x_inets",
            "inet[]",
            &[
                "'{10.0.0.1,10.0.0.0/8}'",
                "'{}'",
                "'{NULL}'",
                "'{::1,\"2001:db8::/32\"}'",
                "'{10.1.2.3/32,2001:db8::1/128}'",
                "'{NULL,10.0.0.1,NULL}'",
            ],
        ),
        text(
            "x_cidrs",
            "cidr[]",
            &[
                "'{10.0.0.0/8,::/0}'",
                "'{}'",
                "'{NULL}'",
                "'{10.1.2.3/32}'",
                "'{2001:db8::/32,NULL}'",
            ],
        ),
        text(
            "x_macs",
            "macaddr[]",
            &[
                "'{08:00:2b:01:02:03,ff:ff:ff:ff:ff:ff}'",
                "'{}'",
                "'{NULL}'",
            ],
        ),
        text(
            "x_drs",
            "daterange[]",
            &[
                "'{\"[2026-01-01,2026-02-01)\",empty}'",
                "'{}'",
                "'{NULL}'",
                "'{\"[2026-03-01,infinity)\",\"(,2020-01-01]\"}'",
            ],
        ),
        text(
            "x_trs",
            "tstzrange[]",
            &[
                "'{\"[2026-01-01 00:00:00+00,2026-01-02 00:00:00+00)\",empty}'",
                "'{}'",
                "'{NULL}'",
            ],
        ),
        text(
            "x_pts",
            "point[]",
            &["'{\"(1,2)\",\"(3,4)\"}'", "'{}'", "'{NULL,\"(0,0)\"}'"],
        ),
        text(
            "x_money",
            "money",
            &[
                "'1234.50'::money",
                "'-0.05'::money",
                "'0'::money",
                "'1234567.89'::money",
            ],
        ),
        text(
            "x_moneys",
            "money[]",
            &["'{1.50,2.25}'", "'{}'", "'{NULL,1234.50}'"],
        ),
        text(
            "x_xml",
            "xml",
            &["'<a b=\"1\">it''s</a>'", "'<r>a b</r>'", "'<p>&amp;</p>'"],
        ),
        text(
            "x_tsv",
            "tsvector",
            &["'it''s:1 b:2'::tsvector", "to_tsvector('simple', 'a b a')"],
        ),
    ]);
    cols
}

fn pg_bits() -> Vec<Col> {
    vec![
        text("b5", "bit(5)", &["B'10110'", "B'00000'", "B'11111'"]),
        text(
            "bv",
            "varbit",
            &["B'101'", "B''", "B'1111000011110000'", "B'0'"],
        ),
        text("b1", "bit(1)", &["B'1'", "B'0'"]),
        text(
            "b5s",
            "bit(5)[]",
            &["'{10110,00000}'", "'{}'", "'{NULL,11111}'"],
        ),
        text("bvs", "varbit[]", &["'{101,\"\"}'", "'{}'", "'{NULL,1}'"]),
    ]
}

fn pg_floats() -> Vec<Col> {
    vec![
        col(
            "f8",
            "double precision",
            Kind::F64,
            &[
                "'Infinity'",
                "'-Infinity'",
                "'NaN'",
                "'-0'",
                "1.7976931348623157e308",
                "5e-324",
            ],
        ),
        col(
            "f4",
            "real",
            Kind::F32,
            &[
                "'Infinity'",
                "'-Infinity'",
                "'NaN'",
                "'-0'",
                "3.4028235e38",
                "1e-45",
            ],
        ),
    ]
}

/// A table of `cols` in the CDC database, an empty snapshot, then the rows through the stream and a batch run over the final table: the differences of both modes against the source.
fn pg_batch_and_stream_differences(label: &str, cols: &[Col]) -> Vec<String> {
    let table = unique_name(label);
    let slot = unique_name(&format!("{label}_slot"));
    let mut pg = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect the cdc stand");
    pg.batch_execute(&format!("CREATE TABLE {table} ({})", ddl_of(cols)))
        .unwrap_or_else(|e| panic!("create {table}: {e}"));
    let _table = PgTable::adopt_on(POSTGRES_CDC_URL, table.clone());
    let _slot = Slot::new(slot.clone());
    let cdc = Rig::pg_cdc(&table, &slot).cdc_line("initial: snapshot");
    let snapshot = cdc.run();
    pg.batch_execute(&insert_of(&table, cols))
        .unwrap_or_else(|e| panic!("insert into {table}: {e}"));
    let batch = Rig::pg_batch(&table)
        .export_named(&format!("{table}_batch"))
        .source_url(POSTGRES_CDC_URL);
    let want = source_text(POSTGRES_CDC_URL, &table, cols);
    modes_against_source(cols, &want, &batch, &cdc, snapshot, |_| {})
}

/// The differences of a batch run and a stream run against the source's text; `settle` is called with the change rows to wait for before the stream reads.
fn modes_against_source(
    cols: &[Col],
    want: &[Vec<Option<String>>],
    batch: &Rig,
    cdc: &Rig,
    snapshot: std::process::Output,
    settle: impl Fn(i64),
) -> Vec<String> {
    let mut bad = Vec::new();
    let out = batch.run();
    if out.status.success() {
        let got = delivered(&batch.out_dir(), cols, None);
        bad.extend(differences("batch", cols, want, &got));
    } else {
        bad.push(format!("batch refused: {}", refusal_line(&out.stderr)));
    }
    settle(want.len() as i64);
    let out = if snapshot.status.success() {
        cdc.run()
    } else {
        snapshot
    };
    if out.status.success() {
        let got = delivered(&cdc.out_dir(), cols, Some(&["insert"]));
        bad.extend(differences("cdc", cols, want, &got));
    } else {
        bad.push(format!("cdc refused: {}", refusal_line(&out.stderr)));
    }
    bad
}

/// The coded error line of a run's stderr, cut for a table row.
fn refusal_line(stderr: &[u8]) -> String {
    String::from_utf8_lossy(stderr)
        .lines()
        .find(|l| l.contains("Error: ["))
        .map_or_else(String::new, |l| l.chars().take(260).collect())
}

/// The differences of every column of `cols`, each tried alone so one refusal does not hide the others.
fn per_column(label: &str, cols: &[Col]) -> Vec<String> {
    let indexed: Vec<(usize, &Col)> = cols.iter().enumerate().collect();
    indexed
        .chunks(6)
        .flat_map(|chunk| {
            std::thread::scope(|s| {
                let handles: Vec<_> = chunk
                    .iter()
                    .map(|(i, c)| {
                        s.spawn(move || {
                            pg_batch_and_stream_differences(&format!("{label}{i}"), &[(*c).clone()])
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .flat_map(|h| h.join().expect("a column probe"))
                    .collect::<Vec<_>>()
            })
        })
        .collect()
}

fn assert_alike(what: &str, bad: Vec<String>) {
    assert!(
        bad.is_empty(),
        "{what}: {} differences between the source's text and the delivered cells:\n{}",
        bad.len(),
        bad.join("\n")
    );
}

#[test]
#[ignore = "live: requires docker compose postgres-cdc (wal_level=logical)"]
fn postgres_batch_and_cdc_deliver_every_text_type_alike() {
    assert_alike("postgres text types", per_column("vp_text", &pg_families()));
}

#[test]
#[ignore = "live: requires docker compose postgres-cdc (wal_level=logical)"]
fn postgres_batch_and_cdc_deliver_bit_strings_alike() {
    assert_alike("postgres bit strings", per_column("vp_bits", &pg_bits()));
}

#[test]
#[ignore = "live: requires docker compose postgres-cdc (wal_level=logical)"]
fn postgres_batch_and_cdc_deliver_float_specials_alike() {
    assert_alike(
        "postgres float specials",
        per_column("vp_float", &pg_floats()),
    );
}

/// The float columns MySQL and SQL Server can hold: finite extremes and negative zero.
fn sql_floats(f8: &str, f4: &str, tiny8: &str, tiny4: &str, max4: &str) -> Vec<Col> {
    vec![
        col(
            "f8",
            f8,
            Kind::F64,
            &["1.7976931348623157e308", "-0e0", tiny8, "-1.5e300"],
        ),
        col("f4", f4, Kind::F32, &[max4, "-0e0", tiny4, "-1.5e30"]),
    ]
}

/// The non-finite float spellings each engine is asked to store.
const NON_FINITE: [&str; 4] = ["'Infinity'", "'-Infinity'", "'NaN'", "1e309"];

/// The text of a float source column holds a finite number.
fn assert_finite_source(engine: &str, want: &[Vec<Option<String>>]) {
    for row in want {
        for cell in row.iter().skip(1).flatten() {
            assert!(
                cell.parse::<f64>().is_ok_and(f64::is_finite),
                "{engine} stored a non-finite float `{cell}`"
            );
        }
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn mysql_batch_and_cdc_deliver_float_extremes_alike_and_the_engine_stores_no_infinity() {
    assert_alike("mysql floats", mysql_float_differences());
}

/// The differences of a mysql float table between batch, stream and the source text.
fn mysql_float_differences() -> Vec<String> {
    use mysql::prelude::Queryable;
    let cols = sql_floats(
        "double",
        "float",
        "2.2250738585072014e-308",
        "1.17549435e-38",
        "3.4028234e38",
    );
    let table = unique_name("vp_my_float");
    let mut conn = cdc_conn();
    conn.query_drop(format!(
        "CREATE TABLE {table} ({}) ENGINE=InnoDB",
        ddl_of(&cols)
    ))
    .unwrap();
    let _t = MysqlCdcTable(table.clone());
    let cdc = Rig::mysql_cdc(&table).cdc_line("initial: snapshot");
    let snapshot = cdc.run();
    for (i, lit) in NON_FINITE.iter().enumerate() {
        let attempt = conn.query_drop(format!(
            "INSERT INTO {table} (id, f8, f4) VALUES ({}, {lit}, {lit})",
            100 + i
        ));
        assert!(attempt.is_err(), "mysql accepted {lit} into a float");
    }
    conn.query_drop(insert_of(&table, &cols)).unwrap();
    let batch = Rig::mysql_batch(&table)
        .export_named(&format!("{table}_batch"))
        .source_url(MYSQL_CDC_URL);
    let want: Vec<Vec<Option<String>>> = conn
        .query_map(
            format!("SELECT id, CAST(f8 AS CHAR), CAST(f4 AS CHAR) FROM {table} ORDER BY id"),
            |(id, a, b): (i64, Option<String>, Option<String>)| vec![Some(id.to_string()), a, b],
        )
        .unwrap();
    assert_finite_source("mysql", &want);
    modes_against_source(&cols, &want, &batch, &cdc, snapshot, |_| {})
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_batch_and_cdc_deliver_float_extremes_alike_and_the_engine_stores_no_infinity() {
    let _serial = cross_process_serial("mssql_cdc");
    assert_alike("mssql floats", mssql_float_differences());
}

/// The differences of a mssql float table between batch, stream and the source text.
fn mssql_float_differences() -> Vec<String> {
    let cols = sql_floats(
        "float",
        "real",
        "2.2250738585072014e-308",
        "1.17549435e-38",
        "3.4028235e38",
    );
    let table = unique_name("vp_ms_float");
    let ci = format!("dbo_{table}");
    let ddl = ddl_of(&cols).replace("id BIGINT", "id INT");
    mssql_cdc_exec(&format!("CREATE TABLE dbo.{table} ({ddl})"));
    let _t = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    enable_cdc(&table, &ci);
    let cdc = Rig::mssql_cdc(&table, &ci).cdc_line("initial: snapshot");
    let snapshot = cdc.run();
    for (i, lit) in NON_FINITE.iter().enumerate() {
        mssql_cdc_try_exec(&format!(
            "INSERT INTO dbo.{table} (id, f8, f4) VALUES ({}, {lit}, {lit})",
            100 + i
        ));
    }
    assert_eq!(
        mssql_cdc_query_i64(&format!("SELECT COUNT(*) FROM dbo.{table}")),
        0,
        "sql server accepted a non-finite float"
    );
    mssql_cdc_exec(&insert_of(&format!("dbo.{table}"), &cols));
    let batch = Rig::mssql_batch(&table)
        .export_named(&format!("{table}_batch"))
        .source_url(MSSQL_CDC_URL);
    let want: Vec<Vec<Option<String>>> = mssql_cdc_query_strings(&format!(
        "SELECT CONCAT(id, '|', COALESCE(CONVERT(varchar(64), f8, 3), '~'), '|', \
         COALESCE(CONVERT(varchar(64), f4, 3), '~')) FROM dbo.{table} ORDER BY id"
    ))
    .iter()
    .map(|l| {
        l.split('|')
            .map(|c| (c != "~").then(|| c.to_string()))
            .collect()
    })
    .collect();
    assert_finite_source("mssql", &want);
    modes_against_source(&cols, &want, &batch, &cdc, snapshot, |rows| {
        wait_for_capture(&ci, rows)
    })
}

#[cfg(feature = "oracle")]
/// What a run left: the coded refusal, or the delivered cells of `col` as text.
fn outcome(out: &std::process::Output, dir: &std::path::Path, col: &str, cdc: bool) -> String {
    if !out.status.success() {
        let line = refusal_line(&out.stderr);
        let code = line
            .split(['[', ']'])
            .nth(1)
            .map_or("refused without a code", |c| c);
        return format!("refused {code}");
    }
    let mut cells = Vec::new();
    for b in read_all_parts(dir) {
        let schema = b.schema();
        let Ok(at) = schema.index_of(col) else {
            return format!("column {col} not delivered");
        };
        let op = schema.index_of("__op").ok();
        for r in 0..b.num_rows() {
            if cdc && let Some(o) = op {
                let kind = arrow::util::display::array_value_to_string(b.column(o), r).unwrap();
                if kind != "insert" {
                    continue;
                }
            }
            cells.push(arrow::util::display::array_value_to_string(b.column(at), r).unwrap());
        }
    }
    format!("value {}", cells.join("|"))
}

#[cfg(feature = "oracle")]
/// One Oracle column holding `literal`, delivered by a batch run and by the stream: both outcomes as text.
fn oracle_outcomes(ddl: &str, literal: &str) -> (String, String, String) {
    let t = crate::live_cdc_oracledb::cdc_table(
        "ora_vp",
        &format!("id NUMBER(10) PRIMARY KEY, v {ddl}"),
    );
    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let cdc_dir = d.path().join("cdc");
    std::fs::create_dir_all(&cdc_dir).unwrap();
    let cdc = Rig::oracle_cdc(t.name())
        .checkpoint_path(ckpt)
        .dest_path(cdc_dir.clone());
    cdc.run_ok();
    ora_exec(&format!("INSERT INTO {} VALUES (1, {literal})", t.name()));
    let source = ora_text_rows(&format!("SELECT TO_CHAR(v) FROM {}", t.name()))
        .first()
        .and_then(|r| r[0].clone())
        .unwrap_or_default();
    let batch = Rig::oracle_batch(t.name());
    let b = batch.run();
    let c = cdc.run();
    (
        source,
        outcome(&b, &batch.out_dir(), "V", false),
        outcome(&c, &cdc_dir, "V", true),
    )
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_batch_and_cdc_deliver_number_and_float_specials_alike() {
    let _serial = cross_process_serial("oracle_cdc");
    let cases: [(&str, &str); 11] = [
        ("NUMBER", "-1E125"),
        ("FLOAT(10)", "9.9999999999999999999999999999999999999E125"),
        ("FLOAT(10)", "-9.9999999999999999999999999999999999999E125"),
        ("NUMBER", "123.456"),
        ("BINARY_DOUBLE", "BINARY_DOUBLE_INFINITY"),
        ("BINARY_DOUBLE", "-BINARY_DOUBLE_INFINITY"),
        ("BINARY_DOUBLE", "BINARY_DOUBLE_NAN"),
        ("BINARY_DOUBLE", "-0.0"),
        ("BINARY_FLOAT", "BINARY_FLOAT_INFINITY"),
        ("BINARY_FLOAT", "BINARY_FLOAT_NAN"),
        ("BINARY_FLOAT", "-0.0"),
    ];
    let mut bad = Vec::new();
    for (ddl, literal) in cases {
        let (source, batch, cdc) = oracle_outcomes(ddl, literal);
        println!("ORACLE-PROBE {ddl} {literal} | source {source:?} | batch {batch} | cdc {cdc}");
        if batch != cdc {
            bad.push(format!(
                "{ddl} {literal}: source {source:?} batch {batch} cdc {cdc}"
            ));
        }
    }
    assert_alike("oracle number and float specials", bad);
    let (source, batch, cdc) = oracle_outcomes("NUMBER", "1E125");
    assert_eq!(
        (batch.as_str(), cdc.as_str()),
        (
            "refused RIVET_SOURCE_VALUE_UNREPRESENTABLE",
            format!("value 1{}", "0".repeat(125)).as_str()
        ),
        "exactly 1E125 (source {source:?}): batch cannot tell it from infinity, the stream reads the exact value"
    );
}
