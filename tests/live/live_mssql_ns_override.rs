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
                                let rig = rig.restage("full", &["columns: {v: text}"]);
                                if datetime {
                                    rig.no_oracle("the ledger renders DATETIME through DATETIME2(7) (.1233333) while the server's own text is .123; graded against CONVERT in a_temporal_column_declared_text_is_delivered_as_the_servers_text")
                                } else {
                                    rig
                                }
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

/// A temporal column declared `text` delivers the server's own `CONVERT(varchar, v, style)` text (style 121, and 120 for SMALLDATETIME, whose precision is the minute) for every value, the year 0001 and 9999 included, and NULL as NULL.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_temporal_column_declared_text_is_delivered_as_the_servers_text() {
    require_alive(LiveService::Mssql);
    for case in TEXT_CASES {
        let (table, _guard) = table_of(case.ty, case.values);
        let rig = Rig::mssql_batch(&table)
            .no_oracle("graded below against the server's own CONVERT text")
            .export_line("columns: {v: text}");
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

/// A `datetime2(7)` / `datetimeoffset(7)` column declared `text` loads into `kind` and a second client reads back the server's own text, years 0001 and 9999 included.
fn text_loads_into(kind: WarehouseKind, case: &TextCase) {
    require_alive(LiveService::Mssql);
    let Some(mut w) = Warehouse::of(kind, "mssqltxt") else {
        return;
    };
    let (table, _guard) = table_of(case.ty, case.values);
    w.adopt(&table);
    let rig = w.shape(
        Rig::mssql_batch(&table)
            .no_oracle("graded below by the warehouse's own read of the delivered text against the server's CONVERT text")
            .export_line("columns: {v: text}"),
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
    for case in &TEXT_CASES[..2] {
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
