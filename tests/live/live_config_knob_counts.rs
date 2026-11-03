//! Count, span and ratio knobs (docs/config-validation-matrix.yaml, HCF-1/3/4/8): a value the runtime cannot
//! hold is refused by code where the config or the flags are read, and a key span wider than `i64` plans
//! every window. A cell asserts the correct behaviour; one that fails today is an `open_defect_*` cell
//! acknowledged in dev/release_oracle/known_red.py.
//!
//! Oracles: the exit and `Error:` line of the real binary, and the ids the destination holds against the
//! ids the source holds.

use crate::common::*;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Decimal128Type, Int64Type};

const ROWS: i64 = 5;
const OUT_OF_RANGE: Refused = Refused::by_code("RIVET_CONFIG_VALUE_OUT_OF_RANGE", 1);
const MODE_SETTING: Refused = Refused::by_code("RIVET_CONFIG_MODE_SETTING_INVALID", 1);
const ROLLOVER_INVALID: Refused = Refused::by_code("RIVET_CONFIG_CDC_ROLLOVER_INVALID", 1);
const PAST_I64: [&str; 2] = ["9223372036854775808", "18446744073709551615"];
const WITHIN_I64: [&str; 3] = ["1", "3", "4611686018427387904"];
const COMMANDS: [&str; 3] = ["validate", "plan", "run"];

#[derive(Clone, Copy, Debug)]
enum Shape {
    Query,
    TableCheckpoint,
}

const SHAPES: [Shape; 2] = [Shape::Query, Shape::TableCheckpoint];

/// A fresh `(id, v)` table whose key has the integer type range chunking takes on `engine`, and its drop guard.
fn keyed_table(engine: SqlEngine, tag: &str, wide_key: bool) -> (String, Box<dyn std::any::Any>) {
    engine.alive();
    let key = match (wide_key, engine.folds_upper()) {
        (false, true) => "NUMBER(18)",
        _ => engine.int64(),
    };
    let (id, v, int) = (engine.col("id"), engine.col("v"), engine.int64());
    engine.create(tag, &format!("{id} {key} PRIMARY KEY, {v} {int} NOT NULL"))
}

/// Insert one row per id of `ids` with `v = 1`.
fn insert_ids(engine: SqlEngine, table: &str, ids: &[i64]) {
    let (id, v) = (engine.col("id"), engine.col("v"));
    for id_value in ids {
        engine.exec(&format!(
            "INSERT INTO {table} ({id}, {v}) VALUES ({id_value}, 1)"
        ));
    }
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

/// A chunked export of `table` over `id`, in `shape`, with `knob_line` as its sizing line.
fn chunked(engine: SqlEngine, table: &str, shape: Shape, knob_line: &str) -> Rig {
    let rig = engine
        .rig(table)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line(knob_line);
    match shape {
        Shape::Query => rig.query(&format!("SELECT * FROM {table}")),
        Shape::TableCheckpoint => rig.export_line("chunk_checkpoint: true"),
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

/// What the invocation said in one stable line: its `Error:` line, else the line after its panic header, else its exit.
fn said(out: &std::process::Output) -> String {
    let err = String::from_utf8_lossy(&out.stderr);
    let mut lines = err.lines();
    let found = lines.clone().find(|l| l.starts_with("Error:")).or_else(|| {
        lines
            .find(|l| l.contains("panicked at"))
            .and_then(|_| lines.next())
    });
    let said = found.unwrap_or("no error line");
    format!(
        "exit {:?}: {}",
        out.status.code(),
        &said[..said.len().min(160)]
    )
}

/// Panic with the count of failed probes on the first line and each failure under it.
fn no_gaps(gaps: Vec<String>, probes: usize, what: &str) {
    assert!(
        gaps.is_empty(),
        "{} of {probes} inputs {what}\n{}",
        gaps.len(),
        gaps.join("\n")
    );
}

/// `run` for the command `run`, else the subcommand of that name.
fn invoke(rig: &Rig, command: &str) -> std::process::Output {
    match command {
        "run" => rig.run(),
        other => rig.cli(&[other]),
    }
}

/// A `chunk_size` no `i64` window holds is refused by every command that reads it, on both shapes.
fn chunk_size_past_i64_is_refused(engine: SqlEngine) {
    let (table, _guard) = keyed_table(engine, "hk_cs", false);
    insert_ids(engine, &table, &(1..=ROWS).collect::<Vec<_>>());
    let (mut gaps, mut probes) = (Vec::new(), 0);
    for shape in SHAPES {
        for size in PAST_I64 {
            let rig = chunked(engine, &table, shape, &format!("chunk_size: {size}"));
            for command in COMMANDS {
                probes += 1;
                probe(
                    &mut gaps,
                    &format!("chunk_size {size} {shape:?} {command}"),
                    || assert_refused(&invoke(&rig, command), OUT_OF_RANGE),
                );
            }
        }
    }
    no_gaps(
        gaps,
        probes,
        "are not refused with RIVET_CONFIG_VALUE_OUT_OF_RANGE",
    );
}

/// A `chunk_size` an `i64` window holds delivers every row, and `0` keeps its own refusal.
fn chunk_size_within_i64_delivers_the_source_ids(engine: SqlEngine) {
    let (table, _guard) = keyed_table(engine, "hk_csok", false);
    let ids: Vec<i64> = (1..=ROWS).collect();
    insert_ids(engine, &table, &ids);
    for shape in SHAPES {
        for size in WITHIN_I64 {
            let rig = chunked(engine, &table, shape, &format!("chunk_size: {size}"));
            let out = rig.run();
            assert!(
                out.status.success(),
                "chunk_size {size} {shape:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            assert_eq!(
                delivered_ids(&rig.out_dir()),
                ids,
                "chunk_size {size} {shape:?}: the destination holds the source's ids"
            );
        }
        let rig = chunked(engine, &table, shape, "chunk_size: 0");
        for command in COMMANDS {
            assert_refused(&invoke(&rig, command), MODE_SETTING);
        }
    }
}

/// Windows over a key range as wide as `i64`, sized by `chunk_count` or by a `chunk_size` near `i64::MAX`, plan and deliver every row.
fn wide_key_windows_plan_and_deliver(engine: SqlEngine) {
    let (table, _guard) = keyed_table(engine, "hk_wide", true);
    let ids = [i64::MIN + 1, -1, 0, 1, i64::MAX];
    insert_ids(engine, &table, &ids);
    let (mut gaps, mut probes) = (Vec::new(), 0);
    for knob in [
        "chunk_count: 1",
        "chunk_count: 2",
        "chunk_count: 4",
        "chunk_size: 9223372036854775807",
        "chunk_size: 4611686018427387904",
    ] {
        let rig = chunked(engine, &table, Shape::Query, knob).export_line("parallel: 1");
        probes += 2;
        probe(&mut gaps, &format!("{knob} plan"), || {
            let out = rig.cli(&["plan"]);
            assert!(out.status.success(), "{}", said(&out));
        });
        probe(&mut gaps, &format!("{knob} run"), || {
            let out = rig.run();
            assert!(out.status.success(), "{}", said(&out));
            assert_eq!(delivered_ids(&rig.out_dir()), ids.to_vec());
        });
    }
    no_gaps(
        gaps,
        probes,
        "do not plan and deliver a key range as wide as i64",
    );
}

/// A `chunk_count` over a narrow range delivers every row in that many parts, and `0` keeps its own refusal.
fn chunk_count_over_a_narrow_range_is_unchanged(engine: SqlEngine) {
    let (table, _guard) = keyed_table(engine, "hk_ccn", false);
    let ids: Vec<i64> = (1..=ROWS).collect();
    insert_ids(engine, &table, &ids);
    let rig = chunked(engine, &table, Shape::Query, "chunk_count: 2");
    let out = rig.run();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(delivered_ids(&rig.out_dir()), ids);
    let parts = std::fs::read_dir(rig.out_dir())
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        .count();
    assert_eq!(parts, 2, "two windows, two parts");
    let zero = chunked(engine, &table, Shape::Query, "chunk_count: 0");
    assert_refused(
        &zero.cli(&["validate"]),
        Refused::by_code("RIVET_CONFIG_CHUNK_COUNT_INVALID", 1),
    );
}

#[test]
#[ignore = "live+gate-only: open defect HCF-1; closes with fix/config-knob-quantity-counts"]
fn open_defect_chunk_size_past_i64_is_refused_postgres() {
    chunk_size_past_i64_is_refused(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: open defect HCF-1; closes with fix/config-knob-quantity-counts"]
fn open_defect_chunk_size_past_i64_is_refused_mysql() {
    chunk_size_past_i64_is_refused(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: open defect HCF-1; closes with fix/config-knob-quantity-counts"]
fn open_defect_chunk_size_past_i64_is_refused_mssql() {
    chunk_size_past_i64_is_refused(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: open defect HCF-1; closes with fix/config-knob-quantity-counts"]
fn open_defect_chunk_size_past_i64_is_refused_oracle() {
    chunk_size_past_i64_is_refused(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunk_size_within_i64_delivers_the_source_ids_postgres() {
    chunk_size_within_i64_delivers_the_source_ids(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn chunk_size_within_i64_delivers_the_source_ids_mysql() {
    chunk_size_within_i64_delivers_the_source_ids(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn chunk_size_within_i64_delivers_the_source_ids_mssql() {
    chunk_size_within_i64_delivers_the_source_ids(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn chunk_size_within_i64_delivers_the_source_ids_oracle() {
    chunk_size_within_i64_delivers_the_source_ids(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: open defect HCF-8; closes with fix/config-knob-quantity-counts"]
fn open_defect_wide_key_windows_plan_and_deliver_postgres() {
    wide_key_windows_plan_and_deliver(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: open defect HCF-8; closes with fix/config-knob-quantity-counts"]
fn open_defect_wide_key_windows_plan_and_deliver_mysql() {
    wide_key_windows_plan_and_deliver(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: open defect HCF-8; closes with fix/config-knob-quantity-counts"]
fn open_defect_wide_key_windows_plan_and_deliver_mssql() {
    wide_key_windows_plan_and_deliver(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: open defect HCF-8; closes with fix/config-knob-quantity-counts"]
fn open_defect_wide_key_windows_plan_and_deliver_oracle() {
    wide_key_windows_plan_and_deliver(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunk_count_over_a_narrow_range_is_unchanged_postgres() {
    chunk_count_over_a_narrow_range_is_unchanged(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn chunk_count_over_a_narrow_range_is_unchanged_mysql() {
    chunk_count_over_a_narrow_range_is_unchanged(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn chunk_count_over_a_narrow_range_is_unchanged_mssql() {
    chunk_count_over_a_narrow_range_is_unchanged(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn chunk_count_over_a_narrow_range_is_unchanged_oracle() {
    chunk_count_over_a_narrow_range_is_unchanged(SqlEngine::Oracle);
}

const CDC_COLUMNS: &str = "id BIGINT PRIMARY KEY, v BIGINT";

type Shape2 = fn(Rig, &str) -> Rig;

fn pg_stream(shape: Shape2) -> CdcScenario {
    CdcScenario::pg_with("hk_ro", CDC_COLUMNS, shape)
}

fn mysql_stream(shape: Shape2) -> CdcScenario {
    CdcScenario::mysql_with("hk_ro", CDC_COLUMNS, shape)
}

fn mssql_stream(shape: Shape2) -> CdcScenario {
    CdcScenario::mssql_with("hk_ro", CDC_COLUMNS, move |r, t| {
        shape(r.repoint(&format!("dbo.{t}")), t)
    })
}

fn mongo_stream(shape: Shape2) -> CdcScenario {
    CdcScenario::mongo_with("hk_ro", shape)
}

#[cfg(feature = "oracle")]
fn oracle_stream(shape: Shape2) -> CdcScenario {
    CdcScenario::oracle_with("hk_ro", shape)
}

fn plain(rig: Rig, _table: &str) -> Rig {
    rig
}

fn rollover_zero(rig: Rig, _table: &str) -> Rig {
    rig.cdc("rollover: 0")
}

/// `rivet cdc --rollover 0` is refused as `cdc.rollover: 0` is, before the stream is read.
fn cdc_rollover_zero_is_refused_on_the_flag(scenario: CdcScenario) {
    let out = scenario.rig.cli_cdc_raw(true, &["--rollover", "0"]);
    assert_refused(&out, ROLLOVER_INVALID);
}

/// `cdc.rollover: 0` stays refused in the config, by `validate` and by `run`.
fn cdc_rollover_in_the_config_is_unchanged(scenario: CdcScenario) {
    assert_refused(&scenario.rig.cli(&["validate"]), ROLLOVER_INVALID);
    assert_refused(&scenario.rig.run(), ROLLOVER_INVALID);
}

#[test]
#[ignore = "live+gate-only: open defect HCF-4; closes with fix/config-knob-quantity-counts"]
fn open_defect_cdc_rollover_zero_is_refused_on_the_flag_postgres() {
    cdc_rollover_zero_is_refused_on_the_flag(pg_stream(plain));
}

#[test]
#[ignore = "live+gate-only: open defect HCF-4; closes with fix/config-knob-quantity-counts"]
fn open_defect_cdc_rollover_zero_is_refused_on_the_flag_mysql() {
    cdc_rollover_zero_is_refused_on_the_flag(mysql_stream(plain));
}

#[test]
#[ignore = "live+gate-only: open defect HCF-4; closes with fix/config-knob-quantity-counts"]
fn open_defect_cdc_rollover_zero_is_refused_on_the_flag_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    cdc_rollover_zero_is_refused_on_the_flag(mssql_stream(plain));
}

#[test]
#[ignore = "live+gate-only: open defect HCF-4; closes with fix/config-knob-quantity-counts"]
fn open_defect_cdc_rollover_zero_is_refused_on_the_flag_mongo() {
    cdc_rollover_zero_is_refused_on_the_flag(mongo_stream(plain));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: open defect HCF-4; closes with fix/config-knob-quantity-counts"]
fn open_defect_cdc_rollover_zero_is_refused_on_the_flag_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    cdc_rollover_zero_is_refused_on_the_flag(oracle_stream(plain));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn cdc_rollover_in_the_config_is_unchanged_postgres() {
    cdc_rollover_in_the_config_is_unchanged(pg_stream(rollover_zero));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_rollover_in_the_config_is_unchanged_mysql() {
    cdc_rollover_in_the_config_is_unchanged(mysql_stream(rollover_zero));
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn cdc_rollover_in_the_config_is_unchanged_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    cdc_rollover_in_the_config_is_unchanged(mssql_stream(rollover_zero));
}

#[test]
#[ignore = "live: requires docker compose mongo-rs"]
fn cdc_rollover_in_the_config_is_unchanged_mongo() {
    cdc_rollover_in_the_config_is_unchanged(mongo_stream(rollover_zero));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn cdc_rollover_in_the_config_is_unchanged_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    cdc_rollover_in_the_config_is_unchanged(oracle_stream(rollover_zero));
}

/// A fresh database on the replica-set MongoDB holding `n` int-`_id` documents in `t`: its URL, handle and drop guard.
fn mongo_collection(tag: &str, n: i64) -> (String, MongoTest, MongoDbGuard) {
    require_alive(LiveService::MongoRs);
    let port = mongo_rs_port();
    let db = unique_name(tag);
    let guard = MongoDbGuard {
        port,
        db: db.clone(),
    };
    let m = MongoTest::connect(port, &db);
    m.seed_int_id("t", n);
    (MongoTest::url(port, &db), m, guard)
}

#[test]
#[ignore = "live+gate-only: open defect HCF-4; closes with fix/config-knob-quantity-counts"]
fn open_defect_mongo_page_size_zero_is_refused() {
    let (url, _m, _guard) = mongo_collection("hk_ps", ROWS);
    let rig = Rig::mongo_batch("t").source_url(&url).mongo("page_size: 0");
    let (mut gaps, mut probes) = (Vec::new(), 0);
    for command in COMMANDS {
        probes += 1;
        probe(&mut gaps, &format!("page_size 0 {command}"), || {
            assert_refused(&invoke(&rig, command), OUT_OF_RANGE)
        });
    }
    no_gaps(
        gaps,
        probes,
        "are not refused with RIVET_CONFIG_VALUE_OUT_OF_RANGE",
    );
}

#[test]
#[ignore = "live: requires docker compose mongo-rs"]
fn mongo_page_size_above_zero_delivers_every_document() {
    let (url, _m, _guard) = mongo_collection("hk_psok", ROWS);
    for page in [1, 2, 1000] {
        let rig = Rig::mongo_batch("t")
            .source_url(&url)
            .mongo(&format!("page_size: {page}"));
        let out = rig.run();
        assert!(
            out.status.success(),
            "page_size {page}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let rows: usize = read_all_parts(&rig.out_dir())
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, ROWS as usize, "page_size {page}");
    }
}

/// The `quality:` block of a rig holding `null_ratio_max` for `column` at `ratio` (YAML text).
fn with_null_ratio(rig: Rig, column: &str, ratio: &str) -> Rig {
    rig.export_line("quality:")
        .export_line("  null_ratio_max:")
        .export_line(&format!("    {column}: {ratio}"))
}

type QualitySource = (&'static str, Box<dyn Fn() -> Rig>);

/// Per engine: a column an export of it produces, a constructor of a batch rig over a table of rows, and the drop guards.
fn quality_sources() -> (Vec<QualitySource>, Vec<Box<dyn std::any::Any>>) {
    let (mut rigs, mut guards): (Vec<QualitySource>, Vec<Box<dyn std::any::Any>>) =
        (Vec::new(), Vec::new());
    #[cfg(feature = "oracle")]
    let engines = [
        SqlEngine::Pg,
        SqlEngine::Mysql,
        SqlEngine::Mssql,
        SqlEngine::Oracle,
    ];
    #[cfg(not(feature = "oracle"))]
    let engines = [SqlEngine::Pg, SqlEngine::Mysql, SqlEngine::Mssql];
    for engine in engines {
        let (table, guard) = keyed_table(engine, "hk_q", false);
        insert_ids(engine, &table, &(1..=ROWS).collect::<Vec<_>>());
        guards.push(guard);
        rigs.push(("v", Box::new(move || engine.rig(&table))));
    }
    let (url, mongo, guard) = mongo_collection("hk_q", ROWS);
    guards.push(Box::new(mongo));
    guards.push(Box::new(guard));
    rigs.push((
        "_id",
        Box::new(move || Rig::mongo_batch("t").source_url(&url)),
    ));
    (rigs, guards)
}

#[test]
#[ignore = "live+gate-only: open defect HCF-3; closes with fix/config-knob-quantity-counts"]
fn open_defect_null_ratio_max_outside_the_unit_interval_is_refused() {
    let (rigs, _guards) = quality_sources();
    let (mut gaps, mut probes) = (Vec::new(), 0);
    for (column, make) in &rigs {
        for ratio in [".nan", ".inf", "-.inf", "5", "1.5", "-0.1"] {
            let rig = with_null_ratio(make(), column, ratio);
            for command in ["validate", "run"] {
                probes += 1;
                probe(
                    &mut gaps,
                    &format!("null_ratio_max {ratio} {command}"),
                    || assert_refused(&invoke(&rig, command), OUT_OF_RANGE),
                );
            }
        }
    }
    no_gaps(
        gaps,
        probes,
        "are not refused with RIVET_CONFIG_VALUE_OUT_OF_RANGE",
    );
}

#[test]
#[ignore = "live: requires the SQL stand and mongo-rs"]
fn null_ratio_max_inside_the_unit_interval_is_accepted() {
    let (rigs, _guards) = quality_sources();
    for (column, make) in &rigs {
        for ratio in ["0", "0.5", "1"] {
            let rig = with_null_ratio(make(), column, ratio);
            let out = rig.run();
            assert!(
                out.status.success(),
                "null_ratio_max {ratio}: {}",
                said(&out)
            );
            let rows: usize = read_all_parts(&rig.out_dir())
                .iter()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(rows, ROWS as usize, "null_ratio_max {ratio}");
        }
    }
}
