//! A PostgreSQL column type with no Arrow mapping is delivered as the server's own text, in every batch mode.
//! The oracle is the source read through the simple-query protocol, which returns each value as the type's output function prints it.
//!
//! ```text
//! cargo nextest run --test live_suite --run-ignored only -E 'test(/^live_pg_server_text::/)'
//! ```

use crate::common::*;
use arrow::array::{Array, StringArray};

const ENGINE: SqlEngine = SqlEngine::Pg;

pub(crate) const FAMILIES: &[(&str, &str, &str, &str, &str)] = &[
    (
        "inet",
        "inet",
        "'10.1.2.3'",
        "'10.1.2.0/24'",
        "'2001:db8::1'",
    ),
    (
        "cidr",
        "cidr",
        "'10.0.0.0/8'",
        "'2001:db8::/32'",
        "'192.168.1.0/24'",
    ),
    (
        "macaddr",
        "macaddr",
        "'08:00:2b:01:02:03'",
        "'aa:bb:cc:dd:ee:ff'",
        "'01:02:03:04:05:06'",
    ),
    (
        "macaddr8",
        "macaddr8",
        "'08:00:2b:01:02:03:04:05'",
        "'aa:bb:cc:dd:ee:ff:00:11'",
        "'01:02:03:04:05:06:07:08'",
    ),
    ("bit", "bit(5)", "B'10110'", "B'00000'", "B'11111'"),
    ("varbit", "varbit", "B'101'", "B''", "B'1111000011110000'"),
    (
        "money",
        "money",
        "'1234.50'::money",
        "'-0.05'::money",
        "'92233720368547758.07'::money",
    ),
    ("qchar", "\"char\"", "'a'", "'Z'", "'~'"),
    (
        "timetz",
        "timetz",
        "'12:34:56.789012+05:30'",
        "'00:00:00+00'",
        "'23:59:59.999999-12'",
    ),
    (
        "xml",
        "xml",
        "'<a b=\"1\">x</a>'",
        "'<r/>'",
        "'<p>\u{e9}\u{4e2d}</p>'",
    ),
    (
        "tsvector",
        "tsvector",
        "'a:1 b:2'::tsvector",
        "'x'::tsvector",
        "to_tsvector('simple', 'the quick fox')",
    ),
    (
        "tsquery",
        "tsquery",
        "'a & b'::tsquery",
        "'x | y'::tsquery",
        "'!z'::tsquery",
    ),
    (
        "pt",
        "point",
        "'(1.5,-2.25)'",
        "'(0,0)'",
        "'(1e300,-1e-300)'",
    ),
    ("ln", "line", "'{1,-1,0}'", "'{0,1,-5}'", "'{3,4,5}'"),
    (
        "ls",
        "lseg",
        "'[(0,0),(1,1)]'",
        "'[(1.5,2.5),(3,4)]'",
        "'[(0,0),(0,0)]'",
    ),
    (
        "bx",
        "box",
        "'(1,1),(0,0)'",
        "'(5,5),(2,2)'",
        "'(0.1,0.2),(0.3,0.4)'",
    ),
    (
        "ph",
        "path",
        "'[(0,0),(1,1),(2,0)]'",
        "'((0,0),(1,1))'",
        "'((1,2))'",
    ),
    (
        "pg",
        "polygon",
        "'((0,0),(1,0),(1,1))'",
        "'((0,0),(2,0),(2,2),(0,2))'",
        "'((1,1),(2,2),(3,1))'",
    ),
    (
        "ci",
        "circle",
        "'<(1,2),3>'",
        "'<(0,0),0>'",
        "'<(0.5,0.25),1.5>'",
    ),
    ("ir", "int4range", "'[1,10)'", "'empty'", "'(,5]'"),
    (
        "lr",
        "int8range",
        "'[1,9223372036854775806]'",
        "'[5,6)'",
        "'(,)'",
    ),
    ("nr", "numrange", "'[1.5,2.5)'", "'(0,1]'", "'[,]'"),
    (
        "dr",
        "daterange",
        "'[2026-01-01,2026-02-01)'",
        "'empty'",
        "'[2026-03-01,)'",
    ),
    (
        "tr",
        "tsrange",
        "'[2026-01-01 00:00:00,2026-01-02 12:30:45.123456)'",
        "'empty'",
        "'(,2026-05-05 00:00:00]'",
    ),
    (
        "tz",
        "tstzrange",
        "'[2026-01-01 00:00:00+00,2026-01-02 00:00:00+00)'",
        "'empty'",
        "'[2026-01-01 05:00:00+05,)'",
    ),
    (
        "mr",
        "int4multirange",
        "'{[1,3),[5,8)}'",
        "'{}'",
        "'{(,2],[9,)}'",
    ),
    (
        "rc",
        "regclass",
        "'pg_class'::regclass",
        "'pg_type'::regclass",
        "'pg_namespace'::regclass",
    ),
    (
        "rt",
        "regtype",
        "'integer'::regtype",
        "'text'::regtype",
        "'timestamptz'::regtype",
    ),
    ("xi", "xid", "'1'::xid", "'4294967295'::xid", "'3'::xid"),
    (
        "lsn",
        "pg_lsn",
        "'0/16B3748'",
        "'0/0'",
        "'FFFFFFFF/FFFFFFFF'",
    ),
    ("tid", "tid", "'(0,1)'", "'(4294967295,65535)'", "'(7,3)'"),
    (
        "inets",
        "inet[]",
        "'{10.0.0.1,10.0.0.0/8}'",
        "'{}'",
        "'{::1,NULL}'",
    ),
    (
        "pts",
        "point[]",
        "'{\"(1,2)\",\"(3,4)\"}'",
        "'{}'",
        "'{NULL}'",
    ),
    (
        "irs",
        "int4range[]",
        "'{\"[1,3)\",empty}'",
        "'{}'",
        "'{NULL}'",
    ),
    (
        "moneys",
        "money[]",
        "'{1.50,2.25}'",
        "'{}'",
        "'{NULL,0.01}'",
    ),
    ("num", "numeric", "1.50", "'NaN'", "-0.000001234"),
    (
        "numw",
        "numeric",
        "123456789012345678901234567890.123456789012",
        "'-Infinity'",
        "0",
    ),
    (
        "nums",
        "numeric[]",
        "'{1.50,-0.000001234}'",
        "'{}'",
        "'{NULL,123456789012345678901234567890.123456789012}'",
    ),
];

/// The table's DDL columns and the INSERT of three populated rows and one all-NULL row.
fn fixture(composite: &str) -> (String, String) {
    let mut ddl = String::from("id BIGINT PRIMARY KEY");
    for (name, ty, ..) in FAMILIES {
        ddl += &format!(", {name} {ty}");
    }
    ddl += &format!(", comp {composite}");
    let names: Vec<&str> = FAMILIES.iter().map(|f| f.0).collect();
    let cols = format!("id, {}, comp", names.join(", "));
    let populated = |id: i64, at: usize, comp: &str| {
        let values: Vec<&str> = FAMILIES.iter().map(|f| [f.2, f.3, f.4][at]).collect();
        format!("({id}, {}, {comp})", values.join(", "))
    };
    let nulls = vec!["NULL"; FAMILIES.len()].join(", ");
    let rows = [
        populated(1, 0, "ROW(1, 'x')"),
        populated(2, 1, "ROW(2, 'a b')"),
        populated(3, 2, "ROW(3, NULL)"),
        format!("(4, {nulls}, NULL)"),
    ];
    (
        ddl,
        format!("INSERT INTO {{t}} ({cols}) VALUES {}", rows.join(", ")),
    )
}

/// A user type dropped with the guard.
struct OwnType(String);

impl Drop for OwnType {
    fn drop(&mut self) {
        ENGINE.exec(&format!("DROP TYPE IF EXISTS {} CASCADE", self.0));
    }
}

/// Every column of `table` as the server prints it, by id, read in the simple-query protocol.
fn source_text(table: &str) -> Vec<Vec<Option<String>>> {
    let mut pg = pg_connect();
    pg.simple_query("SET datestyle = 'ISO, MDY'; SET TimeZone = 'UTC'; SET extra_float_digits = 3")
        .expect("the oracle session settings");
    pg.simple_query(&format!("SELECT * FROM {table} ORDER BY id"))
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

/// The delivered parts' column names and every cell as a string or NULL, by id.
fn delivered(rig: &Rig) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    let batches = read_all_parts(&rig.out_dir());
    let schema = batches.first().expect("a delivered part").schema();
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|c| match c.as_any().downcast_ref::<StringArray>() {
                        Some(s) => (!s.is_null(row)).then(|| s.value(row).to_string()),
                        None => (!c.is_null(row))
                            .then(|| arrow::util::display::array_value_to_string(c, row).unwrap()),
                    })
                    .collect(),
            );
        }
    }
    rows.sort_by_key(|r| r[0].clone().unwrap().parse::<i64>().unwrap());
    let names = schema.fields().iter().map(|f| f.name().clone()).collect();
    (names, rows)
}

/// One export of the table in `mode` delivers every column as the server's text, every value equal to the source's.
fn the_table_of_unmapped_types_is_delivered_as_server_text(mode: &str) {
    let composite = OwnType(unique_name("srvtxt_c"));
    ENGINE.exec(&format!(
        "CREATE TYPE {} AS (n INTEGER, s TEXT)",
        composite.0
    ));
    let (ddl, insert) = fixture(&composite.0);
    let (table, _guard) = ENGINE.create("srvtxt", &ddl);
    ENGINE.exec(&insert.replace("{t}", &table));
    let rig = ENGINE.rig(&table);
    let rig = match mode {
        "full" => rig,
        "keyset" => ENGINE.staged(rig, "chunked", &["chunk_by_key: id", "chunk_size: 2"]),
        "range" => ENGINE.staged(rig, "chunked", &["chunk_column: id", "chunk_size: 2"]),
        "incremental" => ENGINE.staged(rig, "incremental", &["cursor_column: id"]),
        other => panic!("no batch mode `{other}`"),
    };
    let out = rig.run();
    let said = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        out.status.success(),
        "the {mode} export of unmapped PostgreSQL types must deliver:\n{said}"
    );
    let (names, got) = delivered(&rig);
    let want = source_text(&table);
    assert_eq!(got.len(), want.len(), "{mode}: the row count");
    for (c, name) in names.iter().enumerate() {
        for (g, w) in got.iter().zip(&want) {
            assert_eq!(
                g[c], w[c],
                "{mode}: column `{name}` of id {:?} delivered {:?} where the server prints {:?}",
                g[0], g[c], w[c]
            );
        }
    }
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_table_of_every_unmapped_type_is_delivered_as_server_text_full_postgres() {
    the_table_of_unmapped_types_is_delivered_as_server_text("full");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_table_of_every_unmapped_type_is_delivered_as_server_text_keyset_postgres() {
    the_table_of_unmapped_types_is_delivered_as_server_text("keyset");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_table_of_every_unmapped_type_is_delivered_as_server_text_range_postgres() {
    the_table_of_unmapped_types_is_delivered_as_server_text("range");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_table_of_every_unmapped_type_is_delivered_as_server_text_incremental_postgres() {
    the_table_of_unmapped_types_is_delivered_as_server_text("incremental");
}

/// A `tsvector` column under a `text` override is delivered as the server's own text, never reshaped.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_tsvector_under_a_text_override_is_the_servers_text_postgres() {
    let (table, _guard) = ENGINE.create("tsv_ovr", "id BIGINT PRIMARY KEY, t tsvector");
    ENGINE.exec(&format!(
        "INSERT INTO {table} (id, t) VALUES (1, 'a:1 b:2'::tsvector), (2, 'a b c'), \
         (3, to_tsvector('simple', 'the quick fox')), (4, 'x:3A,4B'::tsvector), (5, NULL), (6, $q$'a b c'$q$)"
    ));
    let rig = ENGINE.rig(&table).export_line("columns: {t: text}");
    let out = rig.run();
    assert!(
        out.status.success(),
        "a tsvector under `columns: text` is delivered:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let (_, got) = delivered(&rig);
    let want = source_text(&table);
    assert_eq!(got.len(), want.len(), "the row count");
    for (g, w) in got.iter().zip(&want) {
        assert_eq!(
            g[1], w[1],
            "id {:?} delivered {:?} where the server prints {:?}",
            g[0], g[1], w[1]
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_numeric_the_catalog_types_keeps_its_decimal_and_a_bare_one_is_server_text_postgres() {
    let (table, _guard) = ENGINE.create(
        "num_typed",
        "id BIGINT PRIMARY KEY, hinted NUMERIC(10,2), bare NUMERIC",
    );
    ENGINE.exec(&format!(
        "INSERT INTO {table} VALUES (1, 1.50, 2.5000000000), (2, -99999999.99, 'NaN'), (3, NULL, NULL)"
    ));
    let rig = ENGINE.rig(&table);
    let out = rig.run();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let schema = read_all_parts(&rig.out_dir())[0].schema();
    let kind = |c: &str| schema.field_with_name(c).unwrap().data_type().clone();
    assert_eq!(
        kind("hinted"),
        arrow::datatypes::DataType::Decimal128(10, 2)
    );
    assert_eq!(kind("bare"), arrow::datatypes::DataType::Utf8);
    let (names, got) = delivered(&rig);
    let want = source_text(&table);
    let bare = names.iter().position(|n| n == "bare").unwrap();
    for (g, w) in got.iter().zip(&want) {
        assert_eq!(g[bare], w[bare], "bare of id {:?}", g[0]);
    }
}

/// The values whose display scale is the part of them a plain decimal text loses.
const NUMERIC_SCALES: &str = "(1, 1.50), (2, 2.5000000000), (3, 0.00), (4, 100), (5, 1000.000), \
     (6, -0.000001234), (7, 12345678901234567890.1200), (8, 'NaN'), (9, NULL), (10, 0.10)";

/// A bare `numeric` under a `text` override is the server's own text, every display scale kept.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_bare_numeric_under_a_text_override_keeps_its_display_scale_postgres() {
    let (table, _guard) = ENGINE.create("num_scale_text", "id BIGINT PRIMARY KEY, n NUMERIC");
    ENGINE.exec(&format!("INSERT INTO {table} VALUES {NUMERIC_SCALES}"));
    let rig = ENGINE.rig(&table).export_line("columns: {n: text}");
    let out = rig.run();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let (_, got) = delivered(&rig);
    let want = source_text(&table);
    assert_eq!(got.len(), want.len(), "the row count");
    for (g, w) in got.iter().zip(&want) {
        assert_eq!(
            g[1], w[1],
            "id {:?} delivered {:?} where the server prints {:?}",
            g[0], g[1], w[1]
        );
    }
}

/// A `numeric(p,s)` under a `text` override is the server's own text.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_typed_numeric_under_a_text_override_is_the_servers_text_postgres() {
    let (table, _guard) = ENGINE.create("num_typed_text", "id BIGINT PRIMARY KEY, n NUMERIC(18,4)");
    ENGINE.exec(&format!(
        "INSERT INTO {table} VALUES (1, 1.50), (2, 0), (3, -99999999999999.9999), (4, NULL)"
    ));
    let rig = ENGINE.rig(&table).export_line("columns: {n: text}");
    let out = rig.run();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let (_, got) = delivered(&rig);
    let want = source_text(&table);
    for (g, w) in got.iter().zip(&want) {
        assert_eq!(g[1], w[1], "id {:?}", g[0]);
    }
}

/// A `numeric(p,s)` read as a native decimal, declared or autodetected, delivers each value at the column's scale.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_typed_numeric_read_as_a_decimal_is_the_value_at_the_column_scale_postgres() {
    let (table, _guard) = ENGINE.create(
        "num_dec_ovr",
        "id BIGINT PRIMARY KEY, n NUMERIC(18,4), m NUMERIC(18,4)",
    );
    ENGINE.exec(&format!(
        "INSERT INTO {table} VALUES (1, 1.50, 1.50), (2, 2.5000, 2.5000), (3, 0.00, 0.00), \
         (4, 100, 100), (5, -0.0012, -0.0012), (6, NULL, NULL)"
    ));
    let rig = ENGINE
        .rig(&table)
        .export_line("columns: {n: \"decimal(20,6)\"}");
    let out = rig.run();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let (names, got) = delivered(&rig);
    let col = |c: &str| names.iter().position(|n| n == c).unwrap();
    let want_n = [
        Some("1.500000"),
        Some("2.500000"),
        Some("0.000000"),
        Some("100.000000"),
        Some("-0.001200"),
        None,
    ];
    let want_m = [
        Some("1.5000"),
        Some("2.5000"),
        Some("0.0000"),
        Some("100.0000"),
        Some("-0.0012"),
        None,
    ];
    for (i, g) in got.iter().enumerate() {
        assert_eq!(g[col("n")].as_deref(), want_n[i], "declared, id {:?}", g[0]);
        assert_eq!(
            g[col("m")].as_deref(),
            want_m[i],
            "autodetected, id {:?}",
            g[0]
        );
    }
}

/// The `export_schema` row a run of the previous release that refused a bare `numeric` and a `numeric[]` left behind: the native names.
const BASELINE_A_REFUSED_RUN_LEFT: &str = r#"[{"name":"id","type":"Int64"},{"name":"b","type":"numeric"},{"name":"a","type":"_numeric"},{"name":"v","type":"Decimal128(10, 2)"}]"#;

/// Overwrite the table's drift baseline with `columns_json` and return how many rows it touched.
fn set_baseline(rig: &Rig, columns_json: &str) -> usize {
    let db = rig.config_path().parent().unwrap().join(".rivet_state.db");
    rusqlite::Connection::open(db)
        .expect("the state db")
        .execute(
            "UPDATE export_schema SET columns_json = ?1 WHERE export_name = ?2",
            [columns_json, rig.export_name()],
        )
        .expect("the baseline update")
}

/// After an upgrade from a refused run, `mode` under `policy_line` delivers the rows once and reads no drift in the columns the baseline named natively.
fn a_baseline_a_refused_run_left_is_taken_without_drift(mode: &str, policy_line: Option<&str>) {
    let (table, _guard) = ENGINE.create(
        "num_upg",
        "id BIGINT PRIMARY KEY, b NUMERIC, a NUMERIC[], v NUMERIC(10,2)",
    );
    let rig = ENGINE.rig(&table);
    let rig = match mode {
        "full" => rig,
        "keyset" => ENGINE.staged(rig, "chunked", &["chunk_by_key: id", "chunk_size: 2"]),
        "range" => ENGINE.staged(rig, "chunked", &["chunk_column: id", "chunk_size: 2"]),
        "incremental" => ENGINE.staged(rig, "incremental", &["cursor_column: id"]),
        other => panic!("no batch mode `{other}`"),
    };
    let rig = match policy_line {
        Some(line) => rig.export_line(line),
        None => rig,
    };
    rig.run_ok();
    assert_eq!(
        set_baseline(&rig, BASELINE_A_REFUSED_RUN_LEFT),
        1,
        "the previous release's baseline is in place"
    );
    ENGINE.exec(&format!(
        "INSERT INTO {table} VALUES (1, 1.5, '{{2.5,NULL}}', 3.25), (2, 'NaN', '{{}}', NULL), (3, NULL, NULL, -1.00)"
    ));
    let out = rig.run();
    let said = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        out.status.success(),
        "{mode} after the upgrade must deliver:\n{said}"
    );
    assert!(
        !said.contains("type changed") && !said.contains("schema drift"),
        "{mode}: the upgrade is not drift:\n{said}"
    );
    let (_, got) = delivered(&rig);
    let ids: Vec<_> = got.iter().map(|r| r[0].clone().unwrap()).collect();
    assert_eq!(ids, ["1", "2", "3"], "{mode}: every row exactly once");
    let again = rig.run();
    assert!(
        again.status.success(),
        "{mode}: the run after it: {}",
        String::from_utf8_lossy(&again.stderr)
    );
    if mode == "incremental" {
        let (_, after) = delivered(&rig);
        assert_eq!(after.len(), 3, "incremental: no row twice");
    }
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_upgraded_full_export_under_fail_reads_no_drift_postgres() {
    a_baseline_a_refused_run_left_is_taken_without_drift("full", Some("on_schema_drift: fail"));
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_upgraded_keyset_export_under_fail_reads_no_drift_postgres() {
    a_baseline_a_refused_run_left_is_taken_without_drift("keyset", Some("on_schema_drift: fail"));
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_upgraded_range_export_under_fail_reads_no_drift_postgres() {
    a_baseline_a_refused_run_left_is_taken_without_drift("range", Some("on_schema_drift: fail"));
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_upgraded_incremental_export_under_fail_reads_no_drift_postgres() {
    a_baseline_a_refused_run_left_is_taken_without_drift(
        "incremental",
        Some("on_schema_drift: fail"),
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_upgraded_range_export_under_the_default_policy_reads_no_drift_postgres() {
    a_baseline_a_refused_run_left_is_taken_without_drift("range", None);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_upgraded_incremental_export_under_the_default_policy_reads_no_drift_postgres() {
    a_baseline_a_refused_run_left_is_taken_without_drift("incremental", None);
}

/// After a clean first run, a column of source type `ddl` added to the table is drift: `fail` refuses with exit 4 naming it, any other policy reports it as added.
fn a_column_added_after_the_baseline_is_drift(
    mode: &str,
    policy_line: Option<&str>,
    ddl: &str,
    fails: bool,
) {
    let (table, _guard) = ENGINE.create("added_col", "id BIGINT PRIMARY KEY, v INT");
    let rig = ENGINE.rig(&table);
    let rig = match mode {
        "full" => rig,
        "keyset" => ENGINE.staged(rig, "chunked", &["chunk_by_key: id", "chunk_size: 2"]),
        "range" => ENGINE.staged(rig, "chunked", &["chunk_column: id", "chunk_size: 2"]),
        "incremental" => ENGINE.staged(rig, "incremental", &["cursor_column: id"]),
        other => panic!("no batch mode `{other}`"),
    };
    let rig = match policy_line {
        Some(line) => rig.export_line(line),
        None => rig,
    };
    let rig = rig.a_failed_run_may_leave(
        &[Leftover::OrphanPart, Leftover::FileLog],
        "the drift gate fails the run after its parts are written",
    );
    ENGINE.exec(&format!("INSERT INTO {table} VALUES (1, 10), (2, 20)"));
    rig.run_ok();
    ENGINE.exec(&format!("ALTER TABLE {table} ADD COLUMN x {ddl}"));
    ENGINE.exec(&format!("INSERT INTO {table} VALUES (3, 30, NULL)"));
    let out = rig.run();
    let said = String::from_utf8_lossy(&out.stderr).to_string();
    if fails {
        assert_eq!(
            out.status.code(),
            Some(4),
            "{mode} {ddl}: a column added after the baseline is drift under `fail`:\n{said}"
        );
        assert!(
            said.contains("1 column(s) added, 0 removed, 0 retyped"),
            "{mode} {ddl}: the refusal counts the added column:\n{said}"
        );
    } else {
        assert!(out.status.success(), "{mode} {ddl}:\n{said}");
        assert!(
            said.contains("schema changed!") && said.contains("added: x"),
            "{mode} {ddl}: the added column is reported:\n{said}"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_added_inet_column_is_drift_under_fail_full_postgres() {
    a_column_added_after_the_baseline_is_drift("full", Some("on_schema_drift: fail"), "inet", true);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_added_tsvector_column_is_drift_under_fail_full_postgres() {
    a_column_added_after_the_baseline_is_drift(
        "full",
        Some("on_schema_drift: fail"),
        "tsvector",
        true,
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_added_bare_numeric_column_is_drift_under_fail_full_postgres() {
    a_column_added_after_the_baseline_is_drift(
        "full",
        Some("on_schema_drift: fail"),
        "numeric",
        true,
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_added_inet_column_is_drift_under_fail_keyset_postgres() {
    a_column_added_after_the_baseline_is_drift(
        "keyset",
        Some("on_schema_drift: fail"),
        "inet",
        true,
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_added_inet_column_is_drift_under_fail_range_postgres() {
    a_column_added_after_the_baseline_is_drift(
        "range",
        Some("on_schema_drift: fail"),
        "inet",
        true,
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_added_inet_column_is_drift_under_fail_incremental_postgres() {
    a_column_added_after_the_baseline_is_drift(
        "incremental",
        Some("on_schema_drift: fail"),
        "inet",
        true,
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_added_inet_column_is_reported_under_the_default_policy_incremental_postgres() {
    a_column_added_after_the_baseline_is_drift("incremental", None, "inet", false);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_added_tsvector_column_is_reported_under_the_default_policy_incremental_postgres() {
    a_column_added_after_the_baseline_is_drift("incremental", None, "tsvector", false);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_added_bare_numeric_column_is_reported_under_the_default_policy_incremental_postgres() {
    a_column_added_after_the_baseline_is_drift("incremental", None, "numeric", false);
}
