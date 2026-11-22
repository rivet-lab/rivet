//! A `columns:` time override that keeps fewer fraction digits than a result column holds is reported lossy on batch and on CDC, decided per result column of the query.
//! The oracle for delivered rows is Arrow's rendering of the parquet the run wrote.

use crate::common::*;
use crate::live_int_to_float_override::readback_only;

/// Arrow's rendering of `cols` for every row under `out`, in file and row order.
pub fn delivered_rows(out: &std::path::Path, cols: &[&str]) -> Vec<Vec<Option<String>>> {
    let columns: Vec<Vec<Option<String>>> =
        cols.iter().map(|c| dir_parquet_display(out, c)).collect();
    let n = columns.first().map_or(0, Vec::len);
    (0..n)
        .map(|i| columns.iter().map(|c| c[i].clone()).collect())
        .collect()
}

/// Asserts `out` holds exactly `n` rows and each renders `values` in `cols`.
pub fn assert_delivered(
    out: &std::path::Path,
    cols: &[&str],
    values: &[&str],
    n: usize,
    ctx: &str,
) {
    let row: Vec<Option<String>> = values.iter().map(|v| Some(v.to_string())).collect();
    assert_eq!(
        delivered_rows(out, cols),
        vec![row; n],
        "{ctx}: delivered rows of {cols:?}"
    );
}

/// One batch case: name, optional query, declaration, verdict, delivered column, delivered value.
pub type BatchCase<'a> = (&'a str, Option<String>, &'a str, Verdict, &'a str, &'a str);

/// One CDC case: label, columns, declaration, delivered column, verdict, inserted value, delivered value.
pub type CdcCase<'a> = (
    &'a str,
    &'a str,
    &'a str,
    &'a str,
    Verdict,
    &'a str,
    &'a str,
);

/// The verdict a declared column draws: lossy (warns by default, refused by `--strict`) or exact (runs under both).
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Verdict {
    Lossy,
    Exact,
}

/// Asserts the `--strict` run of `rig` refuses for a lossy verdict and runs for an exact one.
pub fn strict_verdict(rig: &Rig, want: Verdict, ctx: &str) {
    let strict = rig.run_args(&["--strict"]);
    let err = String::from_utf8_lossy(&strict.stderr).to_string();
    match want {
        Verdict::Lossy => {
            assert!(
                !strict.status.success(),
                "{ctx}: --strict must refuse:\n{err}"
            );
            assert!(err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{ctx}: {err}");
            assert!(err.contains("strict mode"), "{ctx}: {err}");
            assert!(err.contains("fidelity=lossy"), "{ctx}: {err}");
        }
        Verdict::Exact => {
            assert!(strict.status.success(), "{ctx}: --strict must run:\n{err}");
            assert!(!err.contains("fidelity=lossy"), "{ctx}: {err}");
        }
    }
}

/// Asserts `check --type-report --json` labels the column `col` with the verdict `want`.
pub fn check_verdict(rig: &Rig, want: Verdict, col: &str, ctx: &str) {
    let name = rig.export_name().to_string();
    let o = rig.cli(&["check", "--export", &name, "--type-report", "--json"]);
    let stdout = String::from_utf8_lossy(&o.stdout).to_string();
    let stderr = String::from_utf8_lossy(&o.stderr).to_string();
    let json: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("{ctx}: check json ({e}):\n{stdout}\n{stderr}"));
    let fidelity = json["columns"]
        .as_array()
        .and_then(|cols| {
            cols.iter().find(|c| {
                c["column"]
                    .as_str()
                    .is_some_and(|n| n.eq_ignore_ascii_case(col))
            })
        })
        .and_then(|c| c["fidelity"].as_str())
        .unwrap_or_else(|| panic!("{ctx}: no `{col}` in the type report:\n{stdout}\n{stderr}"))
        .to_string();
    let expect = match want {
        Verdict::Lossy => "lossy",
        Verdict::Exact => "exact",
    };
    assert_eq!(fidelity, expect, "{ctx}: check --type-report for `{col}`");
}

/// Asserts the default-policy run of `rig` exits 0.
pub fn lax_runs(rig: &Rig, ctx: &str) {
    let lax = rig.run_args(&[]);
    assert!(
        lax.status.success(),
        "{ctx}: the default policy runs:\n{}",
        String::from_utf8_lossy(&lax.stderr)
    );
}

/// Grades a fresh rig from `make` for `want` under `check`, `--strict` and the default policy; returns the destination of the default run.
pub fn graded(make: &dyn Fn() -> Rig, want: Verdict, col: &str, ctx: &str) -> std::path::PathBuf {
    check_verdict(&make(), want, col, ctx);
    strict_verdict(&make(), want, ctx);
    let rig = make();
    lax_runs(&rig, ctx);
    rig.out_dir()
}

/// Rows a pair of runs (`--strict`, then default) leaves: the refused strict run writes none.
pub fn delivered(want: Verdict) -> usize {
    match want {
        Verdict::Lossy => 1,
        Verdict::Exact => 2,
    }
}

const DT2_FINE: &str = "2024-01-02 03:04:05.1234567";

/// `id, d7 datetime2(7), d3 datetime2(3), o7 datetimeoffset(7)`, one row.
fn mssql_table() -> (String, MssqlTable) {
    let table = unique_name("nso_ms");
    mssql_exec(&format!(
        "CREATE TABLE dbo.{table} (id INT PRIMARY KEY, d7 DATETIME2(7), d3 DATETIME2(3), o7 DATETIMEOFFSET(7))"
    ));
    let guard = MssqlTable::adopt(table.clone());
    mssql_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, '{DT2_FINE}', '2024-01-02 03:04:05.123', '{DT2_FINE} +00:00')"
    ));
    (table, guard)
}

/// A SQL Server batch override is graded per result column: the table, an alias, a join and a CAST to a coarser type each draw the verdict their own column deserves, and the rows are what the verdict says.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_mssql_batch_time_override_is_graded_by_the_result_column_it_lands_on() {
    require_alive(LiveService::Mssql);
    let (t, _guard) = mssql_table();
    let cases: &[BatchCase] = &[
        (
            "table d7",
            None,
            "d7: timestamp",
            Verdict::Lossy,
            "d7",
            "2024-01-02T03:04:05.123456",
        ),
        (
            "table o7",
            None,
            "o7: timestamp_tz",
            Verdict::Lossy,
            "o7",
            "2024-01-02T03:04:05.123456+00:00",
        ),
        (
            "table d7 ns",
            None,
            "d7: timestamp_ns",
            Verdict::Exact,
            "d7",
            "2024-01-02T03:04:05.123456700",
        ),
        (
            "table o7 ns",
            None,
            "o7: timestamp_tz_ns",
            Verdict::Exact,
            "o7",
            "2024-01-02T03:04:05.123456700+00:00",
        ),
        (
            "table d3",
            None,
            "d3: timestamp",
            Verdict::Exact,
            "d3",
            "2024-01-02T03:04:05.123",
        ),
        (
            "cast",
            Some(format!(
                "SELECT id, CAST(d7 AS datetime2(3)) AS d7 FROM dbo.{t}"
            )),
            "d7: timestamp",
            Verdict::Exact,
            "d7",
            "2024-01-02T03:04:05.123",
        ),
        (
            "alias",
            Some(format!("SELECT id, d7 AS x FROM dbo.{t}")),
            "x: timestamp",
            Verdict::Lossy,
            "x",
            "2024-01-02T03:04:05.123456",
        ),
        (
            "join",
            Some(format!(
                "SELECT a.id, b.d7 AS d7 FROM dbo.{t} a JOIN dbo.{t} b ON a.id = b.id"
            )),
            "d7: timestamp",
            Verdict::Lossy,
            "d7",
            "2024-01-02T03:04:05.123456",
        ),
    ];
    let mut failed = Vec::new();
    for (n, (name, query, decl, want, col, value)) in cases.iter().enumerate() {
        let case = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let out = std::env::temp_dir().join(format!("{t}_{n}"));
            let _ = std::fs::remove_dir_all(&out);
            std::fs::create_dir_all(&out).unwrap();
            let make = || {
                let mut rig = Rig::mssql_batch(&t).dest_path(out.clone());
                if let Some(q) = query {
                    rig = rig.query(q);
                }
                let rig = rig.export_line(&format!("columns: {{{decl}}}"));
                match want {
                    Verdict::Lossy => readback_only(rig),
                    Verdict::Exact => rig,
                }
            };
            let out = graded(&make, *want, col, name);
            assert_delivered(&out, &["id", col], &["1", value], delivered(*want), name);
            let _ = std::fs::remove_dir_all(&out);
        }));
        if let Err(e) = case {
            let msg = e
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            failed.push(format!("{name}: {msg}"));
        }
    }
    assert_eq!(failed, Vec::<String>::new(), "cases failed");
}

/// A SQL Server CDC override is graded as the batch one: `datetime2(7)` and `datetimeoffset(7)` under a microsecond type are lossy, under their nanosecond type and over `datetime2(3)` exact; the change row holds what the verdict says.
#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_mssql_cdc_time_override_is_graded_as_the_batch_override() {
    let _serial = cross_process_serial("mssql_cdc");
    let cases: &[(&str, &str, Verdict, &str, &str)] = &[
        (
            "nso_cdc_ms7",
            "d7: timestamp",
            Verdict::Lossy,
            "d7",
            "2024-01-02T03:04:05.123456",
        ),
        (
            "nso_cdc_msn",
            "d7: timestamp_ns",
            Verdict::Exact,
            "d7",
            "2024-01-02T03:04:05.123456700",
        ),
        (
            "nso_cdc_ms3",
            "d3: timestamp",
            Verdict::Exact,
            "d3",
            "2024-01-02T03:04:05.123",
        ),
        (
            "nso_cdc_mso7",
            "o7: timestamp_tz",
            Verdict::Lossy,
            "o7",
            "2024-01-02T03:04:05.123456+00:00",
        ),
        (
            "nso_cdc_mson",
            "o7: timestamp_tz_ns",
            Verdict::Exact,
            "o7",
            "2024-01-02T03:04:05.123456700+00:00",
        ),
    ];
    for (label, decl, want, col, value) in cases {
        let decl = decl.to_string();
        let mut scn = CdcScenario::mssql_with(
            label,
            "id INT PRIMARY KEY, d7 DATETIME2(7), d3 DATETIME2(3), o7 DATETIMEOFFSET(7)",
            |r, _| {
                let r = r.export_line(&format!("columns: {{{decl}}}"));
                match want {
                    Verdict::Lossy => readback_only(r),
                    Verdict::Exact => r,
                }
            },
        );
        let out = scn.rig.out_dir();
        std::fs::create_dir_all(&out).unwrap();
        check_verdict(&scn.rig, *want, col, label);
        strict_verdict(&scn.rig, *want, label);
        lax_runs(&scn.rig, label);
        let table = scn.table.clone();
        scn.sql(&format!(
            "INSERT INTO dbo.{table} VALUES (1, '{DT2_FINE}', '2024-01-02 03:04:05.123', '{DT2_FINE} +00:00')"
        ));
        scn.settle();
        scn.rig.run_ok();
        assert_eq!(cdc_id_ops(&out), vec![(1, "insert".to_string())], "{label}");
        assert_delivered(
            &out,
            &["__op", "id", col],
            &["insert", "1", value],
            1,
            label,
        );
    }
}
