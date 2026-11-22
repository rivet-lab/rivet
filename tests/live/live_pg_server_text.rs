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

/// A `tsvector` column under a `text` override is delivered as the server's own text or refused by code, never reshaped.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_tsvector_under_a_text_override_is_the_servers_text_or_refused_by_code_postgres() {
    let (table, _guard) = ENGINE.create("tsv_ovr", "id BIGINT PRIMARY KEY, t tsvector");
    ENGINE.exec(&format!(
        "INSERT INTO {table} (id, t) VALUES (1, 'a:1 b:2'::tsvector), (2, 'a b c'), \
         (3, to_tsvector('simple', 'the quick fox')), (4, 'x:3A,4B'::tsvector), (5, NULL), (6, $q$'a b c'$q$)"
    ));
    let rig = ENGINE.rig(&table).export_line("columns: {t: text}");
    let out = rig.run();
    let said = String::from_utf8_lossy(&out.stderr).to_string();
    if !out.status.success() {
        assert!(said.contains("RIVET_"), "a refusal is coded:\n{said}");
        return;
    }
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
