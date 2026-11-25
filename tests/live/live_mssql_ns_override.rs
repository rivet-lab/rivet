//! A SQL Server value a `timestamp_ns` override cannot hold is refused by code, never written as NULL.

use crate::common::*;

/// One column type under its override: the in-range values, and the values past the nanosecond range.
struct Case {
    ty: &'static str,
    ovr: &'static str,
    plain: &'static str,
    in_range: &'static [&'static str],
    past: &'static [&'static str],
}

const CASES: &[Case] = &[
    Case {
        ty: "DATETIME2(7)",
        ovr: "timestamp_ns",
        plain: "'2024-02-29 12:34:56.123'",
        in_range: &[
            "'1677-09-21 00:12:43.1452242'",
            "'2024-02-29 12:34:56.1234567'",
            "'2262-04-11 23:47:16.8547758'",
        ],
        past: &["'1600-01-01'", "'2300-01-01'", "'9999-12-31 23:59:59.997'"],
    },
    Case {
        ty: "DATETIME",
        ovr: "timestamp_ns",
        plain: "'2024-02-29 12:34:56.123'",
        in_range: &[
            "'1753-01-01'",
            "'2024-02-29 12:34:56.997'",
            "'2262-04-11 12:00:00'",
        ],
        past: &["'2300-01-01'", "'9999-12-31 23:59:59.997'"],
    },
];

/// A table `id, v` holding `values` then a NULL row; returns the guard.
fn table_of(ty: &str, values: &[&str]) -> (String, MssqlTable) {
    let table = unique_name("mssql_nsovr");
    mssql_exec(&format!(
        "CREATE TABLE dbo.{table} (id INT PRIMARY KEY, v {ty} NULL)"
    ));
    let guard = MssqlTable::adopt(table.clone());
    let rows = values
        .iter()
        .enumerate()
        .map(|(i, v)| format!("({}, CAST({v} AS {ty}))", i + 1))
        .chain([format!("({}, NULL)", values.len() + 1)])
        .collect::<Vec<_>>()
        .join(", ");
    mssql_exec(&format!("INSERT INTO dbo.{table} (id, v) VALUES {rows}"));
    (table, guard)
}

fn rig_for(table: &str, ovr: &str) -> Rig {
    Rig::mssql_batch(table)
        .no_oracle("the subject is the override's nanosecond reading, graded below by the server's DATEDIFF_BIG and DATEPART against the parquet nanosecond values")
        .export_line(&format!("columns: {{v: {ovr}}}"))
}

/// A value past 1677-09-21..2262-04-11 under the override is refused by code before any part is written; each remedy the text names delivers the source and a wrong one is walked.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_value_past_the_nanosecond_range_is_refused_and_never_written_as_null() {
    require_alive(LiveService::Mssql);
    for case in CASES {
        for past in case.past {
            let (table, _guard) = table_of(case.ty, &[case.plain, past]);
            let mut rig =
                Rig::mssql_batch(&table).export_line(&format!("columns: {{v: {}}}", case.ovr));
            let ctx = format!("{} {past}", case.ty);
            let first = rig.run();
            assert_refused(
                &first,
                Refused::by_code("RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5),
            );
            assert_eq!(
                files_with_extension(&rig.out_dir(), "parquet").len(),
                0,
                "{ctx}: nothing is written before the refusal"
            );
            let table_q = table.clone();
            let datetime = case.ty == "DATETIME";
            let said = rig.refuses_twice_and_walks_out(
                &["run"],
                &[],
                Refused::by_code("RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5),
                vec![
                    Remedy::wrong(
                        "empties the destination and runs again",
                        Then::Refuses(Refused::by_code("RIVET_SOURCE_VALUE_UNREPRESENTABLE", 5)),
                        |r| {
                            for f in std::fs::read_dir(r.out_dir()).expect("the destination").flatten() {
                                let _ = std::fs::remove_file(f.path());
                            }
                        },
                    ),
                    Remedy::new(
                        "Declare `v: text` under `columns:` and rivet delivers the server's own text for every row",
                        Then::DeliversTheSource,
                        move |r| {
                            r.rebuilt(|rig| {
                                ungraded_when_datetime(rig.restage("full", &["columns: {v: text}"]), datetime)
                            })
                        },
                    ),
                    Remedy::new("Declare the column `timestamp`", Then::DeliversTheSource, |r| {
                        r.rebuilt(|rig| rig.restage("full", &["columns: {v: timestamp}"]))
                    }),
                    Remedy::new("CAST it in the export's `query:`", Then::DeliversTheSource, move |r| {
                        r.rebuilt(|rig| {
                            rig.restage("full", &[]).query(&format!(
                                "SELECT id, CAST(v AS DATETIME2(6)) AS v FROM dbo.{table_q} ORDER BY id"
                            ))
                        })
                    }),
                ],
            );
            assert!(said.contains("column `v` holds "), "{ctx}: {said}");
        }
    }
}

/// A table of in-range values and a NULL delivers every value at its 100 ns tick and the NULL as NULL.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn in_range_values_under_the_override_keep_every_digit_and_null_stays_null() {
    require_alive(LiveService::Mssql);
    for case in CASES {
        let (table, _guard) = table_of(case.ty, case.in_range);
        let rig = rig_for(&table, case.ovr);
        rig.run_ok();
        let want = sqlcmd_lines(
            "rivet-mssql-1",
            &format!(
                "SELECT CONCAT(id, '|', ISNULL(CAST(CAST(DATEDIFF_BIG(SECOND, CAST('1970-01-01' AS DATETIME2(7)), v) AS DECIMAL(38,0)) * 1000000000 \
                 + DATEPART(NANOSECOND, v) AS varchar(40)), 'NULL')) FROM dbo.{table} ORDER BY id"
            ),
        );
        let got: Vec<String> = dir_parquet_id_and_nanos(&rig.out_dir(), "v")
            .into_iter()
            .map(|(id, v)| format!("{id}|{}", v.map_or("NULL".to_string(), |n| n.to_string())))
            .collect();
        assert_eq!(got.len(), case.in_range.len() + 1, "{}: {got:?}", case.ty);
        assert_eq!(got, want, "{}", case.ty);
    }
}

/// A temporal column type with the values a nanosecond timestamp cannot hold and the ones it can.
struct TextCase {
    ty: &'static str,
    style: u8,
    values: &'static [&'static str],
}

const TEXT_CASES: &[TextCase] = &[
    TextCase {
        ty: "DATETIME2(7)",
        style: 121,
        values: &[
            "'0001-01-01 00:00:00.1234567'",
            "'1600-01-01 00:00:00.0000001'",
            "'2024-02-29 12:34:56.1234567'",
            "'2300-01-01 00:00:00'",
            "'9999-12-31 23:59:59.9999999'",
        ],
    },
    TextCase {
        ty: "DATETIMEOFFSET(7)",
        style: 121,
        values: &[
            "'0001-01-01 00:00:00.1234567 +00:00'",
            "'1600-01-01 00:00:00.0000001 +05:30'",
            "'2024-02-29 12:34:56.1234567 +02:00'",
            "'2300-01-01 00:00:00 -08:00'",
            "'9999-12-31 23:59:59.9999999 +00:00'",
        ],
    },
    TextCase {
        ty: "DATETIME2(3)",
        style: 121,
        values: &["'0001-01-01 00:00:00.123'", "'9999-12-31 23:59:59.999'"],
    },
    TextCase {
        ty: "DATETIME",
        style: 121,
        values: &[
            "'1753-01-01'",
            "'2024-02-29 12:34:56.997'",
            "'9999-12-31 23:59:59.997'",
        ],
    },
    TextCase {
        ty: "SMALLDATETIME",
        style: 120,
        values: &["'1900-01-01'", "'2024-02-29 12:34'", "'2079-06-06 23:59'"],
    },
];

/// The rig with its oracle off for a DATETIME column, whose ledger text goes through DATETIME2(7) while the server's own text is the three-digit form.
fn ungraded_when_datetime(rig: Rig, datetime: bool) -> Rig {
    if datetime {
        rig.no_oracle("the ledger renders DATETIME through DATETIME2(7) (.1233333) while the server's own text is .123; graded against CONVERT in a_temporal_column_declared_text_is_delivered_as_the_servers_text")
    } else {
        rig
    }
}

/// A temporal column declared `text` delivers the server's own `CONVERT(varchar, v, style)` text (style 121, and 120 for SMALLDATETIME, whose precision is the minute) for every value, the year 0001 and 9999 included, and NULL as NULL.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_temporal_column_declared_text_is_delivered_as_the_servers_text() {
    require_alive(LiveService::Mssql);
    for case in TEXT_CASES {
        let (table, _guard) = table_of(case.ty, case.values);
        let rig = ungraded_when_datetime(
            Rig::mssql_batch(&table).export_line("columns: {v: text}"),
            case.ty == "DATETIME",
        );
        rig.run_ok();
        let want = sqlcmd_lines(
            "rivet-mssql-1",
            &format!(
                "SELECT ISNULL(CONVERT(varchar(40), v, {}), 'NULL') FROM dbo.{table} ORDER BY id",
                case.style
            ),
        );
        let got: Vec<String> = dir_parquet_display(&rig.out_dir(), "v")
            .into_iter()
            .map(|v| v.unwrap_or_else(|| "NULL".to_string()))
            .collect();
        assert_eq!(
            parquet_column_type(&rig.out_dir(), "v"),
            arrow::datatypes::DataType::Utf8,
            "{}",
            case.ty
        );
        assert_eq!(
            got, want,
            "{}: delivered text vs the server CONVERT text",
            case.ty
        );
    }
}

/// A temporal column declared `text` loads into `kind` and a second client reads back the server's own text, years 0001 and 9999 included.
fn text_loads_into(kind: WarehouseKind, case: &TextCase) {
    require_alive(LiveService::Mssql);
    let Some(mut w) = Warehouse::of(kind, "mssqltxt") else {
        return;
    };
    let (table, _guard) = table_of(case.ty, case.values);
    w.adopt(&table);
    let rig = w.shape(
        ungraded_when_datetime(
            Rig::mssql_batch(&table).export_line("columns: {v: text}"),
            case.ty == "DATETIME",
        ),
        "id",
    );
    rig.run_ok();
    w.load(&rig);
    let want = sqlcmd_lines(
        "rivet-mssql-1",
        &format!(
            "SELECT ISNULL(CONVERT(varchar(40), v, {}), 'NULL') FROM dbo.{table} ORDER BY id",
            case.style
        ),
    );
    let got: Vec<String> = w
        .texts(&table, "id", "v")
        .into_iter()
        .map(|(_, v)| v.unwrap_or_else(|| "NULL".to_string()))
        .collect();
    assert_eq!(
        got, want,
        "{} into {kind:?}: read back vs the server text",
        case.ty
    );
}

fn text_loads_every_case_into(kind: WarehouseKind) {
    for case in TEXT_CASES {
        text_loads_into(kind, case);
    }
}

#[test]
#[ignore = "live: requires docker compose mssql + clickhouse + fake-gcs"]
fn a_temporal_declared_text_loads_into_clickhouse_as_the_servers_text() {
    text_loads_every_case_into(WarehouseKind::ClickHouse);
}

#[test]
#[ignore = "live: requires docker compose mssql + BigQuery creds"]
fn a_temporal_declared_text_loads_into_bigquery_as_the_servers_text() {
    text_loads_every_case_into(WarehouseKind::BigQuery);
}

#[test]
#[ignore = "live: requires docker compose mssql + Snowflake creds"]
fn a_temporal_declared_text_loads_into_snowflake_as_the_servers_text() {
    text_loads_every_case_into(WarehouseKind::Snowflake);
}

const WIRE: Refused<'static> = Refused::by_code("RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);

const CDC_TEXT_TYPES: &[(&str, &str, &str, &str)] = &[
    (
        "DATETIME2(7)",
        "datetime2",
        "'2300-01-01 00:00:00.000001'",
        "timestamp",
    ),
    (
        "DATETIMEOFFSET(7)",
        "datetimeoffset",
        "'2300-01-01 00:00:00.000001 +02:00'",
        "timestamp_tz",
    ),
    (
        "DATETIME",
        "datetime or smalldatetime",
        "'2000-01-01 00:00:00.003'",
        "",
    ),
    (
        "SMALLDATETIME",
        "datetime or smalldatetime",
        "'2024-02-29 12:34'",
        "",
    ),
];

fn cdc_scenario(
    label: &str,
    ty: &str,
    literal: &str,
    shape: impl FnOnce(Rig) -> Rig,
) -> CdcScenario {
    let mut scn =
        CdcScenario::mssql_with(label, &format!("id BIGINT PRIMARY KEY, v {ty}"), |r, t| {
            shape(r.repoint(&format!("dbo.{t}")))
        });
    let t = scn.table.clone();
    scn.sql(&format!(
        "INSERT INTO dbo.{t} (id, v) VALUES (1, {literal})"
    ));
    scn.settle();
    scn
}

fn declared_text(rig: Rig) -> Rig {
    rig.export_line("columns:").export_line("  v: \"text\"")
}

/// The run, `doctor` and both `check` spellings refuse `rig`'s export by `WIRE`; returns the run's stderr.
fn refused_alike(rig: &Rig, name: &str) -> String {
    let run = rig.run_args(&["--export", name]);
    assert_refused(&run, WIRE);
    assert_refused(&rig.cli(&["check", "--export", name]), WIRE);
    assert_refused(
        &rig.cli(&["check", "--export", name, "--type-report"]),
        WIRE,
    );
    assert_refused(&rig.cli(&["doctor"]), WIRE);
    String::from_utf8_lossy(&run.stderr).into_owned()
}

/// A CDC baseline leg declared `text` over a date-time is refused by the run, `doctor` and both `check` spellings, naming the column, the type and the remedy, and the remedy delivers the baseline and the stream as the column's own type.
#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_cdc_baseline_declared_text_over_a_date_time_is_refused_and_its_remedy_delivers() {
    let _serial = cross_process_serial("mssql_cdc");
    for (ty, native, literal, remedy) in CDC_TEXT_TYPES {
        let scn = cdc_scenario("cdctxt_ms", ty, literal, |r| {
            declared_text(r.cdc_line("initial: snapshot"))
        });
        let name = scn.rig.export_name().to_string();
        let mut rig = scn.rig;
        let said = refused_alike(&rig, &name);
        assert!(
            said.contains(&format!(
                "column 'v' is declared text over a `{native}` column, which the baseline leg would deliver as the server's own text while the change stream renders the same cell in its own form"
            )),
            "{ty}: {said}"
        );
        assert!(
            said.contains("no row was delivered. Remove `v` from `columns:` and the baseline and the stream both deliver")
                && said.contains("declare `v: timestamp` (or `timestamp_tz` for a datetimeoffset) instead"),
            "{ty}: {said}"
        );
        assert_eq!(
            files_with_extension(&rig.out_dir(), "parquet").len(),
            0,
            "{ty}: a refused baseline delivered a part"
        );
        if remedy.is_empty() {
            rig.replace_export_line("columns:", "");
            rig.replace_export_line("  v:", "");
        } else {
            rig.replace_export_line("  v:", &format!("  v: \"{remedy}\""));
        }
        rig.run_ok();
        assert_eq!(
            dir_parquet_display(&rig.out_dir().join("snapshot"), "id").len(),
            1,
            "{ty}: the baseline after the remedy"
        );
    }
}

/// A stream-only export declared `text` over a `datetime2(7)` or `datetimeoffset(7)` keeps delivering the change stream's own text, as before the baseline leg was guarded.
#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_stream_only_export_declared_text_over_a_date_time_keeps_the_streams_text() {
    let _serial = cross_process_serial("mssql_cdc");
    for (ty, literal, want) in [
        (
            "DATETIME2(7)",
            "'2300-01-01 00:00:00.0000001'",
            "2300-01-01 00:00:00.000000100",
        ),
        (
            "DATETIMEOFFSET(7)",
            "'2300-01-01 00:00:00.0000001 +02:00'",
            "2299-12-31 22:00:00.000000100",
        ),
    ] {
        let scn = cdc_scenario("cdcstm_ms", ty, literal, declared_text);
        scn.rig.run_ok();
        assert_eq!(
            dir_parquet_display(&scn.rig.out_dir(), "v"),
            vec![Some(want.to_string())],
            "{ty}"
        );
    }
}

/// A CDC baseline leg declared `text` over a text column, or `timestamp_ns` over a `datetime2(7)`, is delivered as before.
#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_cdc_baseline_declared_over_a_text_column_or_a_nanosecond_timestamp_is_delivered() {
    let _serial = cross_process_serial("mssql_cdc");
    for (ty, literal, declared, want) in [
        ("NVARCHAR(40)", "N'abc'", "text", "abc"),
        (
            "DATETIME2(7)",
            "'2024-02-29 12:34:56.1234567'",
            "timestamp_ns",
            "2024-02-29T12:34:56.123456700",
        ),
    ] {
        let scn = cdc_scenario("cdcnbr_ms", ty, literal, |r| {
            r.cdc_line("initial: snapshot")
                .export_line("columns:")
                .export_line(&format!("  v: \"{declared}\""))
        });
        scn.rig.run_ok();
        assert_eq!(
            dir_parquet_display(&scn.rig.out_dir().join("snapshot"), "v"),
            vec![Some(want.to_string())],
            "{ty} {declared}"
        );
    }
}

/// A CDC baseline leg declared `text` over a `date` is still refused by the batch table's own message, not the baseline guard's.
#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_cdc_baseline_declared_text_over_a_date_keeps_the_batch_tables_refusal() {
    let _serial = cross_process_serial("mssql_cdc");
    let scn = cdc_scenario("cdcdate_ms", "DATE", "'2024-02-29'", |r| {
        declared_text(r.cdc_line("initial: snapshot"))
    });
    let name = scn.rig.export_name().to_string();
    let said = refused_alike(&scn.rig, &name);
    assert!(!said.contains("the baseline leg would deliver"), "{said}");
}
