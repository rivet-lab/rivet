//! An integer column under a `float4`/`float8` override is delivered as the same numbers, refused by code, or never written as NULL.

use crate::common::*;

const NOT_EXACT: &str = "RIVET_SOURCE_VALUE_UNREPRESENTABLE";
const WIRE: &str = "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH";

/// The source values of the `i` and `j` columns; the last row is NULL.
const VALUES: [&str; 4] = ["7", "0", "-5", "NULL"];

fn floats(of: &[&str]) -> Vec<Option<f64>> {
    of.iter()
        .map(|v| (*v != "NULL").then(|| v.parse::<i128>().unwrap() as f64))
        .collect()
}

fn id_floats(n: usize) -> Vec<Option<f64>> {
    (1..=n).map(|i| Some(i as f64)).collect()
}

fn insert_rows(table: &str, values: &[&str]) -> String {
    let rows = values
        .iter()
        .enumerate()
        .map(|(k, v)| format!("({}, {v}, {v})", k + 1))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {table} (id, i, j) VALUES {rows}")
}

fn mysql_table(ty: &str, values: &[&str]) -> (String, MysqlTable) {
    use mysql::prelude::Queryable;
    let table = unique_name("ovf_my");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, i {ty} NULL, j {ty} NULL)"
    ))
    .unwrap();
    let guard = MysqlTable::adopt(table.clone());
    c.query_drop(insert_rows(&table, values)).unwrap();
    (table, guard)
}

fn mysql_rig(table: &str, columns: &str) -> Rig {
    Rig::mysql_batch(table)
        .no_oracle("the subject is the float value of an integer column under an override, graded against the inserted integers")
        .export_line(&format!("columns: {{{columns}}}"))
}

/// MySQL INT columns, the primary key among them, under float4/float8 are delivered as the inserted numbers, NULL only where the source is NULL.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_int_column_declared_float_is_delivered_with_its_values_mysql() {
    require_alive(LiveService::Mysql);
    let (table, _guard) = mysql_table("INT", &VALUES);
    let rig = mysql_rig(&table, "id: float8, i: float8, j: float4");
    rig.run_ok();
    let out = rig.out_dir();
    assert_eq!(dir_parquet_floats(&out, "id"), id_floats(4), "id float8");
    assert_eq!(dir_parquet_floats(&out, "i"), floats(&VALUES), "i float8");
    assert_eq!(dir_parquet_floats(&out, "j"), floats(&VALUES), "j float4");
    assert_eq!(
        parquet_column_type(&out, "j"),
        arrow::datatypes::DataType::Float32
    );
}

/// One integer type under a float override: the largest value the float holds is delivered, the next one up is refused by code before any part is written.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_integer_a_float_cannot_hold_exactly_is_refused_never_rounded_mysql() {
    require_alive(LiveService::Mysql);
    let cases: [(&str, &str, &[&str], &[&str]); 4] = [
        (
            "INT",
            "float4",
            &["16777216", "-16777216"],
            &["1", "16777217"],
        ),
        ("INT", "float4", &["16777216"], &["-16777217"]),
        (
            "BIGINT",
            "float8",
            &["9007199254740992", "-9007199254740992"],
            &["1", "9007199254740993"],
        ),
        (
            "BIGINT UNSIGNED",
            "float8",
            &["9007199254740992"],
            &["18446744073709551615"],
        ),
    ];
    for (ty, ovr, holds, past) in cases {
        let (table, _guard) = mysql_table(ty, holds);
        let rig = mysql_rig(&table, &format!("i: {ovr}, j: {ovr}"));
        rig.run_ok();
        assert_eq!(
            dir_parquet_floats(&rig.out_dir(), "i"),
            floats(holds),
            "{ty} as {ovr}"
        );
        let (table, _guard) = mysql_table(ty, past);
        let rig = mysql_rig(&table, &format!("i: {ovr}, j: {ovr}"));
        let out = rig.run();
        assert_refused(&out, Refused::by_code(NOT_EXACT, 5));
        let said = String::from_utf8_lossy(&out.stderr).into_owned();
        assert!(
            said.contains("Declare `"),
            "{ty} as {ovr} names a remedy: {said}"
        );
        assert_eq!(
            files_with_extension(&rig.out_dir(), "parquet").len(),
            0,
            "{ty} as {ovr}: nothing written before the refusal"
        );
    }
}

/// A value of a kind the declared type's converter does not read is refused by code, never written as NULL.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_value_the_declared_type_does_not_read_is_refused_never_nulled_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    for (ty, value, ovr) in [
        ("DOUBLE", "1.5", "int8"),
        ("DOUBLE", "1.5", "int4"),
        ("DOUBLE", "1.5", "bool"),
        ("TIME", "'10:20:30'", "string"),
        ("DOUBLE", "1.5", "binary"),
    ] {
        let table = unique_name("ovf_my_kind");
        let mut c = mysql_connect();
        c.query_drop(format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY, i {ty} NULL, j {ty} NULL)"
        ))
        .unwrap();
        let _guard = MysqlTable::adopt(table.clone());
        c.query_drop(insert_rows(&table, &[value])).unwrap();
        let rig = mysql_rig(&table, &format!("i: {ovr}, j: {ovr}"));
        assert_refused(&rig.run(), Refused::by_code(WIRE, 1));
        assert_eq!(
            files_with_extension(&rig.out_dir(), "parquet").len(),
            0,
            "{ty} as {ovr}: nothing written"
        );
    }
}

/// Text under an override that parses, and text that does not, are read as on every release: the value, and NULL for the unparsable one.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn text_and_decimal_under_a_float_or_int_override_read_as_before_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("ovf_my_text");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, t VARCHAR(20) NULL, d DECIMAL(10,2) NULL, f DOUBLE NULL, r FLOAT NULL)"
    ))
    .unwrap();
    let _guard = MysqlTable::adopt(table.clone());
    c.query_drop(format!(
        "INSERT INTO {table} VALUES (1, '1.5', 2.50, 3.25, 0.5), (2, 'abc', -7.00, -0.125, -2.0), (3, NULL, NULL, NULL, NULL)"
    ))
    .unwrap();
    let rig = mysql_rig(&table, "t: float8, d: float8, f: float4, r: float8");
    rig.run_ok();
    let out = rig.out_dir();
    assert_eq!(
        dir_parquet_floats(&out, "t"),
        vec![Some(1.5), None, None],
        "text as float8"
    );
    assert_eq!(
        dir_parquet_floats(&out, "d"),
        vec![Some(2.5), Some(-7.0), None],
        "decimal as float8"
    );
    assert_eq!(
        dir_parquet_floats(&out, "f"),
        vec![Some(3.25), Some(-0.125), None],
        "double as float4"
    );
    assert_eq!(
        dir_parquet_floats(&out, "r"),
        vec![Some(0.5), Some(-2.0), None],
        "float as float8"
    );
}

/// PostgreSQL refuses an int4 column declared float8 at `check` and at `run` with the wire code.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_int_column_declared_float8_is_refused_by_check_and_run_postgres() {
    require_alive(LiveService::Postgres);
    let name = unique_name("ovf_pg");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&name)
        .query("SELECT 1 AS id, 7::int4 AS i")
        .mode("full")
        .export_line("columns:")
        .export_line("  i: float8")
        .dest_path(out.path().to_path_buf());
    let want = Refused::by_code(WIRE, 1);
    assert_refused(&rig.run_args(&["--export", &name]), want);
    assert_refused(&rig.cli(&["check", "--export", &name]), want);
}

/// Oracle converts an INTEGER column under float4/float8 and delivers the numbers.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_integer_column_declared_float_is_delivered_with_its_values_oracle() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create("ovf_or", "id INTEGER PRIMARY KEY, i INTEGER, j INTEGER");
    ora_exec(&insert_rows(t.name(), &VALUES));
    let rig = Rig::oracle_batch(t.name())
        .no_oracle("the subject is the float value of an integer column under an override, graded against the inserted integers")
        .export_line("columns: {I: float8, J: float4}");
    rig.run_ok();
    let out = rig.out_dir();
    assert_eq!(dir_parquet_floats(&out, "i"), floats(&VALUES), "i float8");
    assert_eq!(dir_parquet_floats(&out, "j"), floats(&VALUES), "j float4");
}

#[derive(Clone, Copy, Debug)]
enum Eng {
    Mysql,
    Postgres,
    Mssql,
    Oracle,
}

/// A source type of the case, spelled per engine.
#[derive(Clone, Copy)]
enum Kind {
    Int,
    Smallint,
    Bigint,
    Double,
}

impl Eng {
    fn ty(self, k: Kind) -> &'static str {
        match (self, k) {
            (Eng::Oracle, Kind::Int) => "NUMBER(10)",
            (Eng::Oracle, Kind::Smallint) => "NUMBER(5)",
            (Eng::Oracle, Kind::Bigint) => "NUMBER(19)",
            (Eng::Oracle, Kind::Double) => "BINARY_DOUBLE",
            (Eng::Postgres, Kind::Double) => "DOUBLE PRECISION",
            (Eng::Mssql, Kind::Double) => "FLOAT",
            (_, Kind::Int) => "INT",
            (_, Kind::Smallint) => "SMALLINT",
            (_, Kind::Bigint) => "BIGINT",
            (_, Kind::Double) => "DOUBLE",
        }
    }

    fn col(self, c: &str) -> String {
        match self {
            Eng::Oracle => c.to_uppercase(),
            _ => c.to_string(),
        }
    }

    fn live(self) -> LiveService {
        match self {
            Eng::Mysql => LiveService::Mysql,
            Eng::Postgres => LiveService::Postgres,
            Eng::Mssql => LiveService::Mssql,
            Eng::Oracle => LiveService::Oracle,
        }
    }

    fn rig(self, table: &str) -> Rig {
        match self {
            Eng::Mysql => Rig::mysql_batch(table),
            Eng::Postgres => Rig::pg_batch(table),
            Eng::Mssql => Rig::mssql_batch(table),
            Eng::Oracle => Rig::oracle_batch(table),
        }
    }

    fn table(self, k: Kind, values: &[&str]) -> (String, Box<dyn std::any::Any>) {
        let ty = self.ty(k);
        let rows = values
            .iter()
            .enumerate()
            .map(|(k, v)| format!("({}, {v})", k + 1))
            .collect::<Vec<_>>()
            .join(", ");
        if let Eng::Oracle = self {
            let t = OracleTable::create("ovf_x", &format!("id INTEGER PRIMARY KEY, v {ty}"));
            ora_exec(&format!("INSERT INTO {} (id, v) VALUES {rows}", t.name()));
            return (t.name().to_string(), Box::new(t));
        }
        let table = unique_name("ovf_x");
        let ddl = format!("CREATE TABLE {table} (id INT PRIMARY KEY, v {ty} NULL)");
        let dml = format!("INSERT INTO {table} (id, v) VALUES {rows}");
        let guard: Box<dyn std::any::Any> = match self {
            Eng::Mysql => {
                use mysql::prelude::Queryable;
                let mut c = mysql_connect();
                c.query_drop(ddl).unwrap();
                let g = MysqlTable::adopt(table.clone());
                c.query_drop(dml).unwrap();
                Box::new(g)
            }
            Eng::Postgres => {
                let mut c = pg_connect();
                c.batch_execute(&ddl).unwrap();
                let g = PgTable::adopt(table.clone());
                c.batch_execute(&dml).unwrap();
                Box::new(g)
            }
            _ => {
                mssql_exec(&ddl);
                let g = MssqlTable::adopt(table.clone());
                mssql_exec(&dml);
                Box::new(g)
            }
        };
        (table, guard)
    }
}

/// One override on a column: source type, values, the declared type, and whether the override sits on the primary key.
struct Case {
    label: &'static str,
    kind: Kind,
    values: &'static [&'static str],
    ovr: &'static str,
    pk: bool,
}

const INTS: &[&str] = &["7", "0", "-5", "NULL"];
const FLAGS: &[&str] = &["1", "0", "7", "NULL"];
const DOUBLES: &[&str] = &["3.0", "-2.0", "0.0", "NULL"];

const CASES: &[Case] = &[
    Case {
        label: "int>float8",
        kind: Kind::Int,
        values: INTS,
        ovr: "float8",
        pk: false,
    },
    Case {
        label: "int>float4",
        kind: Kind::Int,
        values: INTS,
        ovr: "float4",
        pk: false,
    },
    Case {
        label: "smallint>float8",
        kind: Kind::Smallint,
        values: INTS,
        ovr: "float8",
        pk: false,
    },
    Case {
        label: "bigint>float8",
        kind: Kind::Bigint,
        values: INTS,
        ovr: "float8",
        pk: false,
    },
    Case {
        label: "pk int>float8",
        kind: Kind::Int,
        values: INTS,
        ovr: "float8",
        pk: true,
    },
    Case {
        label: "int>int8",
        kind: Kind::Int,
        values: INTS,
        ovr: "int8",
        pk: false,
    },
    Case {
        label: "int>int2",
        kind: Kind::Int,
        values: INTS,
        ovr: "int2",
        pk: false,
    },
    Case {
        label: "smallint>int4",
        kind: Kind::Smallint,
        values: INTS,
        ovr: "int4",
        pk: false,
    },
    Case {
        label: "int>bool",
        kind: Kind::Int,
        values: FLAGS,
        ovr: "bool",
        pk: false,
    },
    Case {
        label: "int>text",
        kind: Kind::Int,
        values: INTS,
        ovr: "text",
        pk: false,
    },
    Case {
        label: "double>int8",
        kind: Kind::Double,
        values: DOUBLES,
        ovr: "int8",
        pk: false,
    },
    Case {
        label: "double>int4",
        kind: Kind::Double,
        values: DOUBLES,
        ovr: "int4",
        pk: false,
    },
    Case {
        label: "double>float4",
        kind: Kind::Double,
        values: DOUBLES,
        ovr: "float4",
        pk: false,
    },
    Case {
        label: "double>bool",
        kind: Kind::Double,
        values: DOUBLES,
        ovr: "bool",
        pk: false,
    },
];

/// Whether the delivered cell is what the source holds, read as the declared type.
fn cell_matches(ovr: &str, src: &str, got: &Option<String>) -> bool {
    if src == "NULL" {
        return got.is_none();
    }
    let Some(got) = got else { return false };
    let n: f64 = src.parse().unwrap();
    match ovr {
        "bool" => *got == (n != 0.0).to_string(),
        "text" => got == src,
        _ => got.parse::<f64>().is_ok_and(|g| g == n),
    }
}

/// What a run of `case` did: delivered the source, refused by code, or something else, read back from the parquet it wrote.
fn outcome(eng: Eng, case: &Case) -> String {
    let (table, _guard) = eng.table(case.kind, case.values);
    let col = eng.col(if case.pk { "id" } else { "v" });
    let rig = eng
        .rig(&table)
        .no_oracle("the subject is the value a declared override delivers, graded against the inserted rows")
        .export_line(&format!("columns: {{{col}: {}}}", case.ovr));
    let out = rig.run();
    let parts = files_with_extension(&rig.out_dir(), "parquet").len();
    if out.status.success() {
        let got = dir_parquet_display(&rig.out_dir(), &col);
        let want: Vec<String> = if case.pk {
            (1..=case.values.len()).map(|i| i.to_string()).collect()
        } else {
            case.values.iter().map(|v| v.to_string()).collect()
        };
        let ok = got.len() == want.len()
            && got
                .iter()
                .zip(&want)
                .all(|(g, w)| cell_matches(case.ovr, w, g));
        return if ok {
            "delivered".to_string()
        } else {
            format!("WRONG {got:?}")
        };
    }
    let said = String::from_utf8_lossy(&out.stderr);
    let code = said
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .find(|w| w.starts_with("RIVET_"))
        .unwrap_or("uncoded");
    let left = if parts > 0 { " parts left" } else { "" };
    format!(
        "refused {code} (exit {}){left}",
        out.status.code().unwrap_or(-1)
    )
}

const D: &str = "delivered";
const W: &str = "refused RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH (exit 1)";
const Q: &str = "refused RIVET_SOURCE_QUERY_FAILED (exit 1)";
const R: &str = "refused";

/// Pinned outcome per case in `CASES` order: MySQL, PostgreSQL, SQL Server, Oracle.
const EXPECTED: [[&str; 4]; 14] = [
    [D, W, R, D],
    [D, W, R, D],
    [D, W, R, D],
    [D, W, R, D],
    [D, W, R, D],
    [D, D, R, D],
    [D, D, R, D],
    [D, D, R, D],
    [D, W, R, Q],
    [D, W, W, D],
    [W, W, R, Q],
    [W, W, R, Q],
    [D, W, R, Q],
    [W, W, R, Q],
];

/// A pin is exact, except `refused` which accepts any coded refusal that left no part behind.
fn matches_pin(pin: &str, got: &str) -> bool {
    if pin == R {
        got.starts_with("refused RIVET_") && !got.contains("parts left")
    } else {
        got == pin
    }
}

fn matrix(eng: Eng) {
    require_alive(eng.live());
    let col = match eng {
        Eng::Mysql => 0,
        Eng::Postgres => 1,
        Eng::Mssql => 2,
        Eng::Oracle => 3,
    };
    assert_eq!(CASES.len(), EXPECTED.len(), "every case has a pin");
    let mut bad = Vec::new();
    for (case, pins) in CASES.iter().zip(EXPECTED) {
        let got = outcome(eng, case);
        eprintln!("MATRIX {eng:?} | {} | {got}", case.label);
        if !matches_pin(pins[col], &got) {
            bad.push(format!("{}: got {got}, pinned {}", case.label, pins[col]));
        }
    }
    assert!(
        bad.is_empty(),
        "{eng:?}: a declared override must deliver the source or refuse by code, as pinned:\n{}",
        bad.join("\n")
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_declared_override_delivers_the_source_or_refuses_by_code_mysql() {
    matrix(Eng::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_declared_override_delivers_the_source_or_refuses_by_code_postgres() {
    matrix(Eng::Postgres);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_declared_override_delivers_the_source_or_refuses_by_code_mssql() {
    matrix(Eng::Mssql);
}

#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_declared_override_delivers_the_source_or_refuses_by_code_oracle() {
    matrix(Eng::Oracle);
}

/// MongoDB has no per-field typing: a `columns:` override on a numeric field is refused by code or leaves the document untouched, never a NULL column.
#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_columns_override_on_a_numeric_field_never_nulls_it_mongo() {
    require_alive(LiveService::Mongo);
    let db = unique_name("ovf_mg");
    let m = MongoTest::connect(27017, &db);
    m.drop_collection("c");
    m.insert_many(
        "c",
        vec![
            mongodb::bson::doc! { "_id": 1, "v": 7_i32 },
            mongodb::bson::doc! { "_id": 2, "v": 0_i32 },
        ],
    );
    let rig = Rig::mongo_batch("c")
        .source_url(&MongoTest::url(27017, &db))
        .no_oracle("the subject is a columns override on a numeric Mongo field, graded against the inserted documents")
        .export_line("columns: {v: float8}");
    let out = rig.run();
    let said = String::from_utf8_lossy(&out.stderr).into_owned();
    eprintln!(
        "MATRIX Mongo | v>float8 | exit {:?} | {}",
        out.status.code(),
        said.lines().last().unwrap_or("")
    );
    if out.status.success() {
        let docs = dir_parquet_display(&rig.out_dir(), "document");
        eprintln!("MATRIX Mongo | v>float8 | documents {docs:?}");
        assert!(
            docs.iter()
                .all(|d| d.as_deref().is_some_and(|d| d.contains("\"v\"")))
        );
    } else {
        assert!(said.contains("RIVET_"), "{said}");
    }
    m.drop_database();
}
