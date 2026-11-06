//! Keyset key type and precision (docs/chunking-matrix.yaml, keyset_key_type_admission and
//! keyset_sub_microsecond_key): the planner accepts a keyset key only when the cursor reader can
//! advance it, so a key it cannot advance is refused by code before any read.
//!
//! Oracles: the exit and `Error:` line of the real binary, the parts under the destination, and the
//! ids the destination holds against the ids the source holds.

use crate::common::*;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Decimal128Type, Int32Type, Int64Type};

const UNUSABLE: Refused = Refused::by_code("RIVET_PLAN_CHUNK_KEY_UNUSABLE", 1);
const COMMANDS: [&str; 3] = ["check", "plan", "run"];
const SMALL_PAGE: i64 = 4;
const ONE_CHUNK: i64 = 100_000;

/// A key type of a neighbour cell: label, column type, literal of row `i`.
type KeyKind = (&'static str, &'static str, fn(i64) -> String);

/// One key shape: the column type, the literal of row `i`, and where the key sits in the table.
#[derive(Clone, Copy)]
struct Case {
    label: &'static str,
    key_ddl: &'static str,
    key_value: fn(i64) -> String,
    primary: bool,
    rows: i64,
    overrides: Option<&'static str>,
    composite: bool,
}

const ENUM_MEMBERS: [&str; 12] = [
    "m09", "m01", "m10", "m02", "m11", "m03", "m12", "m04", "m05", "m06", "m07", "m08",
];

const MYSQL_ENUM: &str =
    "ENUM('m09','m01','m10','m02','m11','m03','m12','m04','m05','m06','m07','m08')";

/// The member of `ENUM_MEMBERS` that row `i` holds, quoted.
fn enum_member(i: i64) -> String {
    format!("'{}'", ENUM_MEMBERS[(i - 1) as usize])
}

const MYSQL_BINARY16: Case = Case {
    label: "MySQL BINARY(16) primary key",
    key_ddl: "BINARY(16)",
    key_value: |i| format!("UNHEX(LPAD(HEX({i}), 32, '0'))"),
    primary: true,
    rows: 12,
    overrides: None,
    composite: false,
};
const MYSQL_TIME: Case = Case {
    label: "MySQL TIME unique key",
    key_ddl: "TIME",
    key_value: |i| format!("SEC_TO_TIME({i} * 60)"),
    primary: false,
    rows: 12,
    overrides: None,
    composite: false,
};
const MYSQL_TINYINT1: Case = Case {
    label: "MySQL TINYINT(1) unique key",
    key_ddl: "TINYINT(1)",
    key_value: |i| i.to_string(),
    primary: false,
    rows: 12,
    overrides: None,
    composite: false,
};
const MYSQL_ENUM_KEY: Case = Case {
    label: "MySQL ENUM unique key",
    key_ddl: MYSQL_ENUM,
    key_value: enum_member,
    primary: false,
    rows: 12,
    overrides: None,
    composite: false,
};
const PG_BYTEA: Case = Case {
    label: "PostgreSQL bytea unique key",
    key_ddl: "BYTEA",
    key_value: |i| format!("decode(lpad(to_hex({i}), 16, '0'), 'hex')"),
    primary: false,
    rows: 12,
    overrides: None,
    composite: false,
};
const PG_TIME: Case = Case {
    label: "PostgreSQL time unique key",
    key_ddl: "TIME",
    key_value: |i| format!("(TIME '00:00:00' + {i} * INTERVAL '1 minute')"),
    primary: false,
    rows: 12,
    overrides: None,
    composite: false,
};
const MSSQL_VARBINARY: Case = Case {
    label: "SQL Server varbinary unique key",
    key_ddl: "VARBINARY(8)",
    key_value: |i| format!("CONVERT(VARBINARY(8), {i})"),
    primary: false,
    rows: 12,
    overrides: None,
    composite: false,
};
const MSSQL_TIME: Case = Case {
    label: "SQL Server time unique key",
    key_ddl: "TIME",
    key_value: |i| format!("DATEADD(MINUTE, {i}, CAST('00:00:00' AS TIME))"),
    primary: false,
    rows: 12,
    overrides: None,
    composite: false,
};
const MSSQL_DT2_7_DISTINCT_US: Case = Case {
    label: "SQL Server datetime2(7) key, a tick past each microsecond",
    key_ddl: "DATETIME2(7)",
    key_value: |i| {
        format!(
            "DATEADD(NANOSECOND, {i} * 1000 + 700, CAST('2024-01-01 00:00:01' AS DATETIME2(7)))"
        )
    },
    primary: false,
    rows: 10,
    overrides: None,
    composite: false,
};
const MSSQL_DT2_7_ONE_US: Case = Case {
    label: "SQL Server datetime2(7) key, nine ticks inside one microsecond",
    key_ddl: "DATETIME2(7)",
    key_value: |i| {
        format!("DATEADD(NANOSECOND, {i} * 100, CAST('2024-01-01 00:00:01' AS DATETIME2(7)))")
    },
    primary: false,
    rows: 9,
    overrides: None,
    composite: false,
};
const MSSQL_DTO_7: Case = Case {
    label: "SQL Server datetimeoffset(7) key, a tick past each microsecond",
    key_ddl: "DATETIMEOFFSET(7)",
    key_value: |i| {
        format!(
            "DATEADD(NANOSECOND, {i} * 1000 + 700, CAST('2024-01-01 00:00:01 +00:00' AS DATETIMEOFFSET(7)))"
        )
    },
    primary: false,
    rows: 10,
    overrides: None,
    composite: false,
};
const MSSQL_DT2_7_NS_OVERRIDE: Case = Case {
    label: "SQL Server datetime2(7) key under the timestamp_ns override",
    key_ddl: "DATETIME2(7)",
    key_value: |i| {
        format!(
            "DATEADD(NANOSECOND, {i} * 1000 + 700, CAST('2024-01-01 00:00:01' AS DATETIME2(7)))"
        )
    },
    primary: false,
    rows: 10,
    overrides: Some("columns: {k: timestamp_ns}"),
    composite: false,
};
const MSSQL_ALIAS_DT2_7: Case = Case {
    label: "SQL Server alias type over datetime2(7), a tick past each microsecond",
    key_ddl: "dbo.rivet_ks_ts7",
    ..MSSQL_DT2_7_DISTINCT_US
};

/// The alias case over its own type `name`, so cells that run in parallel never share a server type.
fn alias_dt2_7(name: &'static str) -> Case {
    Case {
        key_ddl: name,
        ..MSSQL_ALIAS_DT2_7
    }
}
const PG_DOMAIN_BYTEA: Case = Case {
    label: "PostgreSQL domain over bytea unique key",
    key_ddl: "rivet_ks_bytea",
    ..PG_BYTEA
};
#[cfg(feature = "oracle")]
const ORACLE_TS9: Case = Case {
    label: "Oracle TIMESTAMP(9) unique key",
    key_ddl: "TIMESTAMP(9)",
    key_value: |i| format!("TIMESTAMP '2024-01-01 00:00:00' + NUMTODSINTERVAL({i}, 'SECOND')"),
    primary: false,
    rows: 10,
    overrides: None,
    composite: false,
};

#[cfg(feature = "oracle")]
const ORACLE_BINARY_DOUBLE: Case = Case {
    label: "Oracle BINARY_DOUBLE unique key",
    key_ddl: "BINARY_DOUBLE",
    key_value: |i| format!("{i}.5d"),
    primary: false,
    rows: 10,
    overrides: None,
    composite: false,
};
#[cfg(feature = "oracle")]
const ORACLE_SCALED_NUMBER: Case = Case {
    label: "Oracle NUMBER(12,2) unique key",
    key_ddl: "NUMBER(12,2)",
    key_value: |i| format!("{i} + 0.25"),
    primary: false,
    rows: 10,
    overrides: None,
    composite: false,
};
#[cfg(feature = "oracle")]
const ORACLE_FLOAT: Case = Case {
    label: "Oracle FLOAT unique key",
    key_ddl: "FLOAT",
    key_value: |i| i.to_string(),
    primary: false,
    rows: 10,
    overrides: None,
    composite: false,
};
#[cfg(feature = "oracle")]
const ORACLE_NUMBER_STAR_SCALE: Case = Case {
    label: "Oracle NUMBER(*,2) unique key",
    key_ddl: "NUMBER(*,2)",
    key_value: |i| format!("{i} + 0.25"),
    primary: false,
    rows: 10,
    overrides: None,
    composite: false,
};
#[cfg(feature = "oracle")]
const ORACLE_NEGATIVE_SCALE_NUMBER: Case = Case {
    label: "Oracle NUMBER(5,-2) unique key",
    key_ddl: "NUMBER(5,-2)",
    key_value: |i| format!("{i} * 100"),
    primary: false,
    rows: 10,
    overrides: None,
    composite: false,
};
#[cfg(feature = "oracle")]
const ORACLE_NEGATIVE_SCALE_38: Case = Case {
    label: "Oracle NUMBER(38,-5) unique key",
    key_ddl: "NUMBER(38,-5)",
    key_value: |i| format!("{i} * 100000"),
    primary: false,
    rows: 10,
    overrides: None,
    composite: false,
};
#[cfg(feature = "oracle")]
const ORACLE_SCALE_PAST_38: Case = Case {
    label: "Oracle NUMBER(38,39) unique key",
    key_ddl: "NUMBER(38,39)",
    key_value: |i| format!("{i}E-39"),
    primary: false,
    rows: 10,
    overrides: None,
    composite: false,
};
#[cfg(feature = "oracle")]
const ORACLE_LTZ: Case = Case {
    label: "Oracle TIMESTAMP(6) WITH LOCAL TIME ZONE unique key",
    key_ddl: "TIMESTAMP(6) WITH LOCAL TIME ZONE",
    key_value: |i| format!("TIMESTAMP '2024-01-01 00:00:00' + NUMTODSINTERVAL({i}, 'SECOND')"),
    primary: false,
    rows: 10,
    overrides: None,
    composite: false,
};

/// The lead column of a composite key `(g, k)`, spelled as the engine folds it.
fn lead(engine: SqlEngine) -> &'static str {
    match engine.folds_upper() {
        true => "G",
        false => "g",
    }
}

/// The key columns and their config spelling: Oracle folds bare names up, the rest keep them.
fn names(engine: SqlEngine) -> (&'static str, &'static str) {
    match engine.folds_upper() {
        true => ("ID", "K"),
        false => ("id", "k"),
    }
}

/// A fresh `(k, id)` table for `case` with its rows inserted, and its drop guard.
fn seeded(engine: SqlEngine, case: &Case, tag: &str) -> (String, Box<dyn std::any::Any>) {
    engine.alive();
    let (id, k) = names(engine);
    let id_ddl = match engine.folds_upper() {
        true => "NUMBER(18)",
        false => engine.int64(),
    };
    let g = lead(engine);
    let columns = match (case.composite, case.primary) {
        (true, _) => format!(
            "{id} {id_ddl} NOT NULL, {g} {id_ddl} NOT NULL, {k} {} NOT NULL, PRIMARY KEY ({g}, {k})",
            case.key_ddl
        ),
        (false, true) => format!(
            "{k} {} NOT NULL PRIMARY KEY, {id} {id_ddl} NOT NULL",
            case.key_ddl
        ),
        (false, false) => format!(
            "{id} {id_ddl} NOT NULL PRIMARY KEY, {k} {} NOT NULL UNIQUE",
            case.key_ddl
        ),
    };
    let (table, guard) = engine.create(tag, &columns);
    for i in 1..=case.rows {
        let insert = match case.composite {
            true => format!(
                "INSERT INTO {table} ({id}, {g}, {k}) VALUES ({i}, 1, {})",
                (case.key_value)(i)
            ),
            false => format!(
                "INSERT INTO {table} ({id}, {k}) VALUES ({i}, {})",
                (case.key_value)(i)
            ),
        };
        engine.exec(&insert);
    }
    engine.refresh_row_estimate(&table);
    (table, guard)
}

/// A chunked export of `table` keyed on `case`, paged by `chunk_size`.
fn keyset_rig(engine: SqlEngine, table: &str, case: &Case, chunk_size: i64) -> Rig {
    let (_, k) = names(engine);
    let mut rig = engine.rig(table).mode("chunked");
    if case.composite {
        rig = rig.export_line(&format!("chunk_by_key: [{}, {k}]", lead(engine)));
    } else if !case.primary {
        rig = rig.export_line(&format!("chunk_by_key: {k}"));
    }
    if let Some(line) = case.overrides {
        rig = rig.export_line(line);
    }
    rig.export_line(&format!("chunk_size: {chunk_size}"))
}

/// The sorted ids the parts under `dir` hold, the column named in either case.
fn delivered_ids(dir: &std::path::Path) -> Vec<i64> {
    let mut ids = Vec::new();
    for batch in read_all_parts(dir) {
        let schema = batch.schema();
        let at = schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case("id"))
            .expect("an id column");
        let column = batch.column(at);
        match column.data_type() {
            DataType::Int32 => ids.extend(
                column
                    .as_primitive::<Int32Type>()
                    .iter()
                    .flatten()
                    .map(i64::from),
            ),
            DataType::Int64 => ids.extend(column.as_primitive::<Int64Type>().iter().flatten()),
            DataType::Decimal128(_, 0) => ids.extend(
                column
                    .as_primitive::<Decimal128Type>()
                    .iter()
                    .flatten()
                    .map(|v| v as i64),
            ),
            other => panic!("the id column is {other:?}"),
        }
    }
    ids.sort();
    ids
}

/// A column type alias (SQL Server `CREATE TYPE`, PostgreSQL `CREATE DOMAIN`) dropped with this guard.
struct AliasType {
    engine: SqlEngine,
    drop_sql: String,
}

impl AliasType {
    /// Create `name` over `base`, replacing a leftover of the same name.
    fn create(engine: SqlEngine, name: &str, base: &str) -> Self {
        let (drop_sql, create_sql) = match engine {
            SqlEngine::Mssql => (
                format!("DROP TYPE IF EXISTS dbo.{name}"),
                format!("CREATE TYPE dbo.{name} FROM {base}"),
            ),
            SqlEngine::Pg => (
                format!("DROP DOMAIN IF EXISTS {name}"),
                format!("CREATE DOMAIN {name} AS {base}"),
            ),
            _ => panic!("this engine has no column type alias"),
        };
        engine.exec(&drop_sql);
        engine.exec(&create_sql);
        AliasType { engine, drop_sql }
    }
}

impl Drop for AliasType {
    fn drop(&mut self) {
        let (engine, sql) = (self.engine, self.drop_sql.clone());
        if std::panic::catch_unwind(move || engine.exec(&sql)).is_err() {
            eprintln!("could not drop the alias type: {}", self.drop_sql);
        }
    }
}

/// Run `check`; a panic in it is recorded in `gaps` under `label`, its first line only.
fn probe(gaps: &mut Vec<String>, label: &str, check: impl FnOnce()) {
    if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(check)) {
        let said = panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        gaps.push(format!("{label}: {}", said.lines().next().unwrap_or("")));
    }
}

/// What a run that was not refused delivered: the part rows and the distinct ids among them.
fn delivered_note(rig: &Rig) -> String {
    let ids = delivered_ids(&rig.out_dir());
    let mut distinct = ids.clone();
    distinct.dedup();
    format!(
        "{} row(s) delivered, {} distinct id(s)",
        ids.len(),
        distinct.len()
    )
}

/// Every command refuses the key by code, and the run writes nothing.
fn the_key_is_refused(engine: SqlEngine, case: &Case, chunk_size: i64, what: &str) {
    let (table, _guard) = seeded(engine, case, "ks_kt");
    let rig = keyset_rig(engine, &table, case, chunk_size);
    let mut gaps = Vec::new();
    for command in COMMANDS {
        probe(&mut gaps, command, || {
            let out = match command {
                "run" => rig.run(),
                other => rig.cli(&[other]),
            };
            assert_refused(&out, UNUSABLE);
        });
    }
    let parts = files_with_extension(&rig.out_dir(), "parquet").len();
    if parts > 0 {
        gaps.push(format!(
            "run: {parts} part(s) written before the refusal; {}",
            delivered_note(&rig)
        ));
    }
    assert!(
        gaps.is_empty(),
        "keyset key {} {what}: not refused by RIVET_PLAN_CHUNK_KEY_UNUSABLE on every command\n{}",
        case.label,
        gaps.join("\n")
    );
}

/// The key is refused while the table spans several pages.
fn refused_over_several_pages(engine: SqlEngine, case: Case) {
    the_key_is_refused(engine, &case, SMALL_PAGE, "over several pages");
}

/// The key is refused although the whole table fits one chunk.
fn refused_on_a_table_that_fits_one_chunk(engine: SqlEngine, case: Case) {
    let case = Case { rows: 5, ..case };
    the_key_is_refused(engine, &case, ONE_CHUNK, "on a table that fits one chunk");
}

/// Every key type the cursor reads delivers the source's ids once over several pages.
fn key_types_the_cursor_reads_deliver_the_seeded_ids(engine: SqlEngine, keys: &[KeyKind]) {
    key_types_deliver_the_seeded_ids_over(engine, keys, 12, &[]);
}

/// Every key type in `keys` delivers the source's `rows` ids once, with the export lines `extra` added.
fn key_types_deliver_the_seeded_ids_over(
    engine: SqlEngine,
    keys: &[KeyKind],
    rows: i64,
    extra: &[&str],
) {
    for (label, ddl, value) in keys {
        let case = Case {
            label,
            key_ddl: ddl,
            key_value: *value,
            primary: false,
            rows,
            overrides: None,
            composite: false,
        };
        let (table, _guard) = seeded(engine, &case, "ks_ok");
        let rig = extra.iter().fold(
            keyset_rig(engine, &table, &case, SMALL_PAGE),
            |rig, line| rig.export_line(line),
        );
        let out = rig.run();
        assert!(
            out.status.success(),
            "keyset key {label}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            delivered_ids(&rig.out_dir()),
            (1..=case.rows).collect::<Vec<_>>(),
            "keyset key {label}: the destination holds the source's ids once"
        );
    }
}

/// A key the planner refuses today is still refused, with the same code.
#[cfg(feature = "oracle")]
fn a_refused_key_stays_refused(engine: SqlEngine, case: Case) {
    let (table, _guard) = seeded(engine, &case, "ks_ref");
    let rig = keyset_rig(engine, &table, &case, SMALL_PAGE);
    for command in COMMANDS {
        let out = match command {
            "run" => rig.run(),
            other => rig.cli(&[other]),
        };
        assert_refused(&out, UNUSABLE);
    }
    assert_eq!(files_with_extension(&rig.out_dir(), "parquet").len(), 0);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn keyset_key_is_refused_mysql_binary16_primary_key() {
    refused_over_several_pages(SqlEngine::Mysql, MYSQL_BINARY16);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn keyset_key_is_refused_mysql_time_unique_key() {
    refused_over_several_pages(SqlEngine::Mysql, MYSQL_TIME);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn keyset_key_is_refused_mysql_tinyint1_unique_key() {
    refused_over_several_pages(SqlEngine::Mysql, MYSQL_TINYINT1);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn keyset_key_is_refused_mysql_enum_unique_key() {
    refused_over_several_pages(SqlEngine::Mysql, MYSQL_ENUM_KEY);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_key_is_refused_postgres_bytea_unique_key() {
    refused_over_several_pages(SqlEngine::Pg, PG_BYTEA);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_key_is_refused_postgres_time_unique_key() {
    refused_over_several_pages(SqlEngine::Pg, PG_TIME);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_key_is_refused_mssql_varbinary_unique_key() {
    refused_over_several_pages(SqlEngine::Mssql, MSSQL_VARBINARY);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_key_is_refused_mssql_time_unique_key() {
    refused_over_several_pages(SqlEngine::Mssql, MSSQL_TIME);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_key_is_refused_mssql_datetime2_7_distinct_microseconds() {
    refused_over_several_pages(SqlEngine::Mssql, MSSQL_DT2_7_DISTINCT_US);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_key_is_refused_mssql_datetime2_7_inside_one_microsecond() {
    refused_over_several_pages(SqlEngine::Mssql, MSSQL_DT2_7_ONE_US);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_key_is_refused_mssql_datetimeoffset_7_distinct_microseconds() {
    refused_over_several_pages(SqlEngine::Mssql, MSSQL_DTO_7);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn keyset_key_is_refused_on_a_one_chunk_table_mysql_binary16_primary_key() {
    refused_on_a_table_that_fits_one_chunk(SqlEngine::Mysql, MYSQL_BINARY16);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_key_is_refused_on_a_one_chunk_table_postgres_bytea_unique_key() {
    refused_on_a_table_that_fits_one_chunk(SqlEngine::Pg, PG_BYTEA);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_key_is_refused_on_a_one_chunk_table_mssql_datetime2_7_key() {
    refused_on_a_table_that_fits_one_chunk(SqlEngine::Mssql, MSSQL_DT2_7_DISTINCT_US);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_key_is_refused_mssql_alias_type_over_datetime2_7() {
    let _alias = AliasType::create(SqlEngine::Mssql, "rivet_ks_ts7_pages", "DATETIME2(7)");
    refused_over_several_pages(SqlEngine::Mssql, alias_dt2_7("dbo.rivet_ks_ts7_pages"));
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_key_is_refused_on_a_one_chunk_table_mssql_alias_type_over_datetime2_7() {
    let _alias = AliasType::create(SqlEngine::Mssql, "rivet_ks_ts7_one", "DATETIME2(7)");
    refused_on_a_table_that_fits_one_chunk(SqlEngine::Mssql, alias_dt2_7("dbo.rivet_ks_ts7_one"));
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_key_is_refused_postgres_domain_over_bytea() {
    let _alias = AliasType::create(SqlEngine::Pg, "rivet_ks_bytea", "BYTEA");
    refused_over_several_pages(SqlEngine::Pg, PG_DOMAIN_BYTEA);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn an_alias_type_over_datetime2_6_keyset_key_delivers_the_seeded_ids_once_mssql() {
    let _alias = AliasType::create(SqlEngine::Mssql, "rivet_ks_ts6", "DATETIME2(6)");
    key_types_the_cursor_reads_deliver_the_seeded_ids(
        SqlEngine::Mssql,
        &[(
            "SQL Server alias type over datetime2(6)",
            "dbo.rivet_ks_ts6",
            |i| format!("DATEADD(MICROSECOND, {i}, CAST('2024-01-01' AS DATETIME2(6)))"),
        )],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_domain_over_text_keyset_key_delivers_the_seeded_ids_once_postgres() {
    let _alias = AliasType::create(SqlEngine::Pg, "rivet_ks_text", "TEXT");
    key_types_the_cursor_reads_deliver_the_seeded_ids(
        SqlEngine::Pg,
        &[("PostgreSQL domain over text", "rivet_ks_text", |i| {
            format!("'k{i:03}'")
        })],
    );
}

/// `rivet init --table` scaffolds a keyset key for a primary key the planner admits and none for one it refuses.
fn init_scaffolds_a_keyset_key_exactly_when_the_planner_admits_it(
    engine: SqlEngine,
    refused: Case,
    admitted: Case,
) {
    let (_, k) = names(engine);
    let scaffold = |case: &Case| {
        let case = Case {
            primary: true,
            ..*case
        };
        let (table, guard) = seeded(engine, &case, "ks_init");
        let cfg = InitConfig::generate(engine.url(), &["--table", &table, "--mode", "chunked"]);
        assert!(
            cfg.init.status.success(),
            "init --table {table} ({}): {}",
            case.label,
            String::from_utf8_lossy(&cfg.init.stderr)
        );
        let ran = cfg.run_after(&["check"], &[], Pairing::SameRefusal).run;
        assert!(
            ran.status.success(),
            "the config init wrote for {} does not run (exit {:?})\n{}",
            case.label,
            ran.status.code(),
            String::from_utf8_lossy(&ran.stderr)
        );
        (cfg.yaml(), guard)
    };
    let (yaml, _guard) = scaffold(&admitted);
    assert!(
        yaml.contains(&format!("chunk_by_key: {k}")),
        "init scaffolds the key the planner admits ({}):\n{yaml}",
        admitted.label
    );
    let (yaml, _guard) = scaffold(&refused);
    assert!(
        !yaml.contains("chunk_by_key:") && yaml.contains("NOTE: no chunk key"),
        "init scaffolds no keyset key for the key the planner refuses ({}):\n{yaml}",
        refused.label
    );
}

const MYSQL_VARCHAR16: Case = Case {
    label: "MySQL VARCHAR(16) primary key",
    key_ddl: "VARCHAR(16)",
    key_value: |i| format!("'k{i:03}'"),
    primary: true,
    rows: 12,
    overrides: None,
    composite: false,
};
const PG_TEXT: Case = Case {
    label: "PostgreSQL text primary key",
    key_ddl: "TEXT",
    key_value: |i| format!("'k{i:03}'"),
    primary: true,
    rows: 12,
    overrides: None,
    composite: false,
};
const MSSQL_DT2_6: Case = Case {
    label: "SQL Server datetime2(6) primary key",
    key_ddl: "DATETIME2(6)",
    key_value: |i| format!("DATEADD(MICROSECOND, {i}, CAST('2024-01-01' AS DATETIME2(6)))"),
    primary: true,
    rows: 12,
    overrides: None,
    composite: false,
};

#[test]
#[ignore = "live: requires docker compose mssql"]
fn init_scaffolds_no_keyset_key_for_a_datetime2_7_primary_key_mssql() {
    init_scaffolds_a_keyset_key_exactly_when_the_planner_admits_it(
        SqlEngine::Mssql,
        MSSQL_DT2_7_DISTINCT_US,
        MSSQL_DT2_6,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn init_scaffolds_no_keyset_key_for_an_alias_type_over_datetime2_7_primary_key_mssql() {
    let _alias = AliasType::create(SqlEngine::Mssql, "rivet_ks_ts7_init", "DATETIME2(7)");
    init_scaffolds_a_keyset_key_exactly_when_the_planner_admits_it(
        SqlEngine::Mssql,
        alias_dt2_7("dbo.rivet_ks_ts7_init"),
        MSSQL_DT2_6,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn init_scaffolds_no_keyset_key_for_a_binary16_primary_key_mysql() {
    init_scaffolds_a_keyset_key_exactly_when_the_planner_admits_it(
        SqlEngine::Mysql,
        Case {
            primary: true,
            ..MYSQL_BINARY16
        },
        MYSQL_VARCHAR16,
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_scaffolds_no_keyset_key_for_a_bytea_primary_key_postgres() {
    init_scaffolds_a_keyset_key_exactly_when_the_planner_admits_it(
        SqlEngine::Pg,
        Case {
            primary: true,
            ..PG_BYTEA
        },
        PG_TEXT,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn keyset_key_types_the_cursor_reads_deliver_the_seeded_ids_mysql() {
    key_types_the_cursor_reads_deliver_the_seeded_ids(
        SqlEngine::Mysql,
        &[
            ("MySQL BIGINT", "BIGINT", |i| i.to_string()),
            ("MySQL VARCHAR(16)", "VARCHAR(16)", |i| format!("'k{i:03}'")),
            ("MySQL DATETIME(6)", "DATETIME(6)", |i| {
                format!("DATE_ADD(TIMESTAMP('2024-01-01 00:00:00'), INTERVAL {i} MICROSECOND)")
            }),
            ("MySQL DATE", "DATE", |i| {
                format!("DATE_ADD('2024-01-01', INTERVAL {i} DAY)")
            }),
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_key_types_the_cursor_reads_deliver_the_seeded_ids_postgres() {
    key_types_the_cursor_reads_deliver_the_seeded_ids(
        SqlEngine::Pg,
        &[
            ("PostgreSQL bigint", "BIGINT", |i| i.to_string()),
            ("PostgreSQL text", "TEXT", |i| format!("'k{i:03}'")),
            ("PostgreSQL timestamp(6)", "TIMESTAMP(6)", |i| {
                format!("TIMESTAMP '2024-01-01 00:00:00' + {i} * INTERVAL '1 microsecond'")
            }),
            ("PostgreSQL date", "DATE", |i| {
                format!("DATE '2024-01-01' + {i}")
            }),
            ("PostgreSQL uuid", "UUID", |i| {
                format!("('00000000-0000-0000-0000-' || lpad(to_hex({i}), 12, '0'))::uuid")
            }),
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_key_types_the_cursor_reads_deliver_the_seeded_ids_mssql() {
    key_types_the_cursor_reads_deliver_the_seeded_ids(
        SqlEngine::Mssql,
        &[
            ("SQL Server bigint", "BIGINT", |i| i.to_string()),
            ("SQL Server varchar(16)", "VARCHAR(16)", |i| {
                format!("'k{i:03}'")
            }),
            ("SQL Server datetime2(6)", "DATETIME2(6)", |i| {
                format!("DATEADD(MICROSECOND, {i}, CAST('2024-01-01' AS DATETIME2(6)))")
            }),
            ("SQL Server date", "DATE", |i| {
                format!("DATEADD(DAY, {i}, CAST('2024-01-01' AS DATE))")
            }),
        ],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn keyset_key_types_the_cursor_reads_deliver_the_seeded_ids_oracle() {
    key_types_the_cursor_reads_deliver_the_seeded_ids(
        SqlEngine::Oracle,
        &[
            ("Oracle NUMBER(18)", "NUMBER(18)", |i| i.to_string()),
            ("Oracle NUMBER(38)", "NUMBER(38)", |i| i.to_string()),
            ("Oracle VARCHAR2(16)", "VARCHAR2(16)", |i| {
                format!("'k{i:03}'")
            }),
            ("Oracle NVARCHAR2(16)", "NVARCHAR2(16)", |i| {
                format!("'k{i:03}'")
            }),
            ("Oracle CHAR(8)", "CHAR(8)", |i| format!("'k{i:03}'")),
            ("Oracle DATE", "DATE", |i| {
                format!("DATE '2024-01-01' + {i}")
            }),
            ("Oracle TIMESTAMP(6)", "TIMESTAMP(6)", |i| {
                format!(
                    "TIMESTAMP '2024-01-01 00:00:00' + NUMTODSINTERVAL({i} / 1000000, 'SECOND')"
                )
            }),
        ],
    );
}

/// The bare `NUMBER` keys a seek must page: integers, fractions across zero, 35 digits, and both ends of the exponent range.
#[cfg(feature = "oracle")]
const ORACLE_BARE_NUMBER_KEYS: [KeyKind; 5] = [
    ("Oracle bare NUMBER", "NUMBER", |i| i.to_string()),
    ("Oracle bare NUMBER fractions across zero", "NUMBER", |i| {
        format!("{i} - 6.5")
    }),
    ("Oracle bare NUMBER of 35 digits", "NUMBER", |i| {
        format!("{i} * 100000000000000000000000000000 + {i} / 8")
    }),
    ("Oracle bare NUMBER of a large exponent", "NUMBER", |i| {
        format!("{i}E100")
    }),
    ("Oracle bare NUMBER of a small exponent", "NUMBER", |i| {
        format!("{i}E-100")
    }),
];

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_bare_number_keyset_key_delivers_the_seeded_ids_once_oracle() {
    key_types_the_cursor_reads_deliver_the_seeded_ids(SqlEngine::Oracle, &ORACLE_BARE_NUMBER_KEYS);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_bare_number_keyset_key_delivers_the_seeded_ids_once_in_parallel_oracle() {
    key_types_deliver_the_seeded_ids_over(
        SqlEngine::Oracle,
        &ORACLE_BARE_NUMBER_KEYS,
        40,
        &["parallel: 3"],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_datetime2_7_key_under_the_timestamp_ns_override_delivers_the_seeded_ids_once_mssql() {
    let case = MSSQL_DT2_7_NS_OVERRIDE;
    let (table, _guard) = seeded(SqlEngine::Mssql, &case, "ks_ns");
    let rig = keyset_rig(SqlEngine::Mssql, &table, &case, SMALL_PAGE).no_oracle(
        "the oracle reads a datetime2(7) source at microsecond precision, so every 700 ns tick the run delivers differs from its source row",
    );
    let out = rig.run();
    assert!(
        out.status.success(),
        "keyset key {}: {}",
        case.label,
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        delivered_ids(&rig.out_dir()),
        (1..=case.rows).collect::<Vec<_>>(),
        "keyset key {}: every row once",
        case.label
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_timestamp_9_keyset_key_stays_refused_oracle() {
    a_refused_key_stays_refused(SqlEngine::Oracle, ORACLE_TS9);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_binary_double_a_float_a_scaled_number_and_a_local_time_zone_keyset_key_stay_refused_oracle() {
    for case in [
        ORACLE_BINARY_DOUBLE,
        ORACLE_SCALED_NUMBER,
        ORACLE_NUMBER_STAR_SCALE,
        ORACLE_FLOAT,
        ORACLE_NEGATIVE_SCALE_NUMBER,
        ORACLE_NEGATIVE_SCALE_38,
        ORACLE_SCALE_PAST_38,
        ORACLE_LTZ,
    ] {
        a_refused_key_stays_refused(SqlEngine::Oracle, case);
    }
}

/// The check output names `phrase` and not `absent`, so the refusal states the cause and the remedy that holds.
#[cfg(feature = "oracle")]
fn the_refusal_says(engine: SqlEngine, case: &Case, phrase: &str, absent: &str) {
    let (table, _guard) = seeded(engine, case, "ks_say");
    let out = keyset_rig(engine, &table, case, SMALL_PAGE).cli(&["check"]);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(text.contains(phrase), "{}: {text}", case.label);
    assert!(!text.contains(absent), "{}: {text}", case.label);
}

#[cfg(feature = "oracle")]
const ORACLE_TS9_NS: Case = Case {
    label: "Oracle TIMESTAMP(9) unique key under the timestamp_ns override",
    overrides: Some("columns: {K: timestamp_ns}"),
    ..ORACLE_TS9
};
#[cfg(feature = "oracle")]
const ORACLE_TS9_COMPOSITE: Case = Case {
    label: "Oracle TIMESTAMP(9) as the second column of a composite key",
    composite: true,
    ..ORACLE_TS9
};
#[cfg(feature = "oracle")]
const ORACLE_TS9_COMPOSITE_NS: Case = Case {
    label: "Oracle TIMESTAMP(9) as the second column of a composite key under the timestamp_ns override",
    composite: true,
    ..ORACLE_TS9_NS
};
#[cfg(feature = "oracle")]
const ORACLE_FLOAT_COMPOSITE: Case = Case {
    label: "Oracle FLOAT as the second column of a composite key",
    composite: true,
    ..ORACLE_FLOAT
};
#[cfg(feature = "oracle")]
const ORACLE_SCALED_NUMBER_INT64_COMPOSITE: Case = Case {
    label: "Oracle NUMBER(12,2) of whole numbers under an int64 override as the second column of a composite key",
    key_value: |i| i.to_string(),
    overrides: Some("columns: {K: int64}"),
    composite: true,
    ..ORACLE_SCALED_NUMBER
};
#[cfg(feature = "oracle")]
const ORACLE_SCALED_NUMBER_STRING_COMPOSITE: Case = Case {
    label: "Oracle NUMBER(12,2) of fractions under a string override as the second column of a composite key",
    overrides: Some("columns: {K: string}"),
    composite: true,
    ..ORACLE_SCALED_NUMBER
};
const PG_JSONB: Case = Case {
    label: "PostgreSQL jsonb unique key",
    key_ddl: "JSONB",
    key_value: |i| {
        format!("'{{\"a\": {{\"b\": [{i}, null, true]}}, \"u\": \"\u{e9}\u{4e2d}{i}\"}}'::jsonb")
    },
    primary: false,
    rows: 12,
    overrides: None,
    composite: false,
};

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_timestamp_9_keyset_key_under_the_timestamp_ns_override_stays_refused_oracle() {
    the_key_is_refused(
        SqlEngine::Oracle,
        &ORACLE_TS9_NS,
        SMALL_PAGE,
        "under the override",
    );
    the_refusal_says(
        SqlEngine::Oracle,
        &ORACLE_TS9_NS,
        "whatever type `columns:` delivers it as",
        "needs the `timestamp_ns`",
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_timestamp_9_composite_keyset_key_is_refused_oracle() {
    for case in [ORACLE_TS9_COMPOSITE, ORACLE_TS9_COMPOSITE_NS] {
        the_key_is_refused(SqlEngine::Oracle, &case, SMALL_PAGE, "as a composite key");
    }
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_scaled_number_a_float_and_a_negative_scale_keyset_key_stay_refused_under_a_columns_override_oracle()
 {
    for base in [
        ORACLE_SCALED_NUMBER,
        ORACLE_NUMBER_STAR_SCALE,
        ORACLE_FLOAT,
        ORACLE_NEGATIVE_SCALE_NUMBER,
    ] {
        for line in ["columns: {K: int64}", "columns: {K: string}"] {
            let case = Case {
                overrides: Some(line),
                ..base
            };
            the_key_is_refused(SqlEngine::Oracle, &case, SMALL_PAGE, line);
        }
    }
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_float_and_a_scaled_number_composite_keyset_key_deliver_the_seeded_ids_once_oracle() {
    for case in [
        ORACLE_FLOAT_COMPOSITE,
        ORACLE_SCALED_NUMBER_INT64_COMPOSITE,
        ORACLE_SCALED_NUMBER_STRING_COMPOSITE,
    ] {
        let (table, _guard) = seeded(SqlEngine::Oracle, &case, "ks_cmp");
        let rig = keyset_rig(SqlEngine::Oracle, &table, &case, SMALL_PAGE);
        let out = rig.run();
        assert!(
            out.status.success(),
            "keyset key {}: {}",
            case.label,
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            delivered_ids(&rig.out_dir()),
            (1..=case.rows).collect::<Vec<_>>(),
            "keyset key {}: every row once",
            case.label
        );
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_jsonb_keyset_key_delivers_the_seeded_ids_once_postgres() {
    for case in [
        PG_JSONB,
        Case {
            composite: true,
            ..PG_JSONB
        },
    ] {
        let (table, _guard) = seeded(SqlEngine::Pg, &case, "ks_jb");
        let rig = keyset_rig(SqlEngine::Pg, &table, &case, SMALL_PAGE);
        let out = rig.run();
        assert!(
            out.status.success(),
            "keyset key {} (composite {}): {}",
            case.label,
            case.composite,
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            delivered_ids(&rig.out_dir()),
            (1..=case.rows).collect::<Vec<_>>(),
            "keyset key {} (composite {}): every row once",
            case.label,
            case.composite
        );
    }
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_datetime2_7_composite_keyset_key_is_refused_mssql() {
    refused_over_several_pages(
        SqlEngine::Mssql,
        Case {
            label: "SQL Server datetime2(7) as the second column of a composite key",
            composite: true,
            ..MSSQL_DT2_7_DISTINCT_US
        },
    );
}
