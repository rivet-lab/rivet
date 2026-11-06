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
        .no_oracle("the subject is the override's nanosecond reading, graded below by the server's DATEDIFF_BIG and DATEPART against epoch_ns()")
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
        let c = stage_for_duckdb(&rig.out_dir());
        let got: Vec<String> = duckdb_run_sql_json(&format!(
            "SELECT id, epoch_ns(v) FROM read_parquet('{c}/**/*.parquet') ORDER BY id"
        ))["rows"]
            .as_array()
            .expect("duckdb rows")
            .iter()
            .map(|r| {
                format!(
                    "{}|{}",
                    r[0].as_str().unwrap(),
                    r[1].as_str().filter(|s| *s != "None").unwrap_or("NULL")
                )
            })
            .collect();
        assert_eq!(got.len(), case.in_range.len() + 1, "{}: {got:?}", case.ty);
        assert_eq!(got, want, "{}", case.ty);
    }
}
