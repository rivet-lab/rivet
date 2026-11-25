//! The type policy runs in the run (ADR-0038 CP6): a lossy or unsupported column warns by
//! default and refuses under `--strict` before any data is read, with the verdict and code
//! `rivet check --type-report --strict` gives. The oracle for delivered rows is DuckDB.
#![cfg(feature = "oracle")]

use crate::common::*;

/// Parquet files anywhere under `dir`.
fn parquet_parts(dir: &std::path::Path) -> usize {
    walkdir_files(dir)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .count()
}

/// Every file under `dir`, recursively (an absent dir has none).
fn walkdir_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walkdir_files(&p));
        } else {
            out.push(p);
        }
    }
    out
}

/// An Oracle table with one sub-microsecond `TIMESTAMP(9)` value in row 1.
fn oracle_ts9_table() -> OracleTable {
    let t = OracleTable::create("tp_ts9", "id NUMBER(10) PRIMARY KEY, ts9 TIMESTAMP(9)");
    ora_exec(&format!(
        "INSERT INTO {} VALUES (1, TIMESTAMP '2024-01-02 03:04:05.123456789')",
        t.name()
    ));
    t
}

/// An undeclared Oracle CDC `TIMESTAMP(9)` is delivered as nanosecond text with or without `--strict`; the declared `timestamp` still delivers microseconds.
#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_timestamp9_is_delivered_as_text_undeclared_and_a_declared_timestamp_delivers_the_row()
{
    let _serial = cross_process_serial("oracle_cdc");
    let t = OracleTable::create("tp_cts9", "id NUMBER(10) PRIMARY KEY, ts9 TIMESTAMP(9)");
    ora_exec(&format!(
        "ALTER TABLE {} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        t.name()
    ));
    ora_exec(&format!("GRANT SELECT ON {} TO c##rivetcdc", t.name()));
    let table = t.name().to_string();
    let leg = |declared: bool| {
        let d = tempfile::tempdir().unwrap();
        let out = d.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let ckpt = d.path().join("cdc.ckpt");
        let table = table.clone();
        let rig = move || {
            let r = Rig::oracle_cdc(&table)
                .checkpoint_path(ckpt.clone())
                .dest_path(out.clone());
            match declared {
                true => r.export_line("columns: {TS9: timestamp}"),
                false => r,
            }
        };
        (d, rig)
    };
    let insert = |id: u32| {
        ora_exec(&format!(
            "INSERT INTO {} VALUES ({id}, TIMESTAMP '2024-01-02 03:04:05.123456789')",
            t.name()
        ));
    };

    let count = |dir: &std::path::Path, filter: &str| {
        duckdb_dir_scalar(&dir.join("out"), "count(*)", Some(filter))
    };
    let (undeclared_dir, undeclared) = leg(false);
    undeclared().run_ok();
    insert(1);
    for (args, id) in [(&[][..], 1), (&["--strict"][..], 2)] {
        if id == 2 {
            insert(2);
        }
        let run = undeclared().run_args(args);
        let err = String::from_utf8_lossy(&run.stderr);
        assert!(run.status.success(), "{args:?} undeclared:\n{err}");
        assert!(!err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{err}");
        assert_eq!(
            count(
                undeclared_dir.path(),
                &format!("__op = 'insert' AND ID = {id} AND TS9 = '2024-01-02T03:04:05.123456789'")
            ),
            1,
            "{args:?}: the undeclared timestamp(9) keeps every digit as text"
        );
    }

    let (declared_dir, declared) = leg(true);
    declared().run_ok();
    insert(3);
    declared().run_ok();
    assert_eq!(
        count(
            declared_dir.path(),
            "__op = 'insert' AND ID = 3 AND TS9 = TIMESTAMP '2024-01-02 03:04:05.123456'"
        ),
        1,
        "the declared timestamp keeps microseconds"
    );
}

/// An undeclared Oracle batch `TIMESTAMP(9)` is delivered as nanosecond text under every policy; declared `timestamp` it is delivered lossy without `--strict` and refused under it; declared `timestamp_ns` it runs under both.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn oracle_batch_timestamp9_is_delivered_as_text_undeclared_under_every_policy() {
    require_alive(LiveService::Oracle);
    let t = oracle_ts9_table();
    let d = tempfile::tempdir().unwrap();

    for (n, args) in [&["--strict"][..], &[][..]].into_iter().enumerate() {
        let out = d.path().join(format!("undeclared{n}"));
        let run = Rig::oracle_batch(t.name())
            .dest_path(out.clone())
            .run_args(args);
        let err = String::from_utf8_lossy(&run.stderr);
        assert!(run.status.success(), "{args:?} undeclared:\n{err}");
        assert!(!err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{err}");
        crate::live_narrowing_override::assert_delivered(
            &out,
            &["ID", "TS9"],
            &["1", "2024-01-02T03:04:05.123456789"],
            1,
            "the undeclared timestamp(9) keeps every digit as text",
        );
    }

    let declared = |n: usize, decl: &str| {
        let out = d.path().join(format!("declared{n}"));
        let rig = Rig::oracle_batch(t.name())
            .dest_path(out.clone())
            .export_line(decl);
        (rig, out)
    };

    let (lossy, lossy_out) = declared(0, "columns: {TS9: timestamp}");
    let run = lossy.run_args(&[]);
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(run.status.success(), "declared timestamp:\n{err}");
    assert!(
        err.contains("column 'TS9'") && err.contains("fidelity=lossy"),
        "{err}"
    );
    crate::live_narrowing_override::assert_delivered(
        &lossy_out,
        &["ID", "TS9"],
        &["1", "2024-01-02T03:04:05.123456"],
        1,
        "the declared timestamp keeps microseconds",
    );

    let (strict, strict_out) = declared(1, "columns: {TS9: timestamp}");
    let run = strict.run_args(&["--strict"]);
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(
        !run.status.success(),
        "declared timestamp under --strict:\n{err}"
    );
    assert!(err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{err}");
    assert!(err.contains("strict mode"), "{err}");
    assert!(
        err.contains("column 'TS9'") && err.contains("fidelity=lossy"),
        "{err}"
    );
    assert_eq!(parquet_parts(&strict_out), 0, "a refused run wrote a part");

    for (n, args) in [&["--strict"][..], &[][..]].into_iter().enumerate() {
        let (exact, exact_out) = declared(2 + n, "columns: {TS9: timestamp_ns}");
        let run = exact.run_args(args);
        let err = String::from_utf8_lossy(&run.stderr);
        assert!(
            run.status.success(),
            "{args:?} declared timestamp_ns:\n{err}"
        );
        assert!(!err.contains("fidelity=lossy"), "{err}");
        crate::live_narrowing_override::assert_delivered(
            &exact_out,
            &["ID", "TS9"],
            &["1", "2024-01-02T03:04:05.123456789"],
            1,
            "the declared timestamp_ns keeps every digit",
        );
    }
}

/// `check --strict` and `run --strict` agree on an undeclared TIMESTAMP(9): both pass and the run delivers the text, and the declared lossy `timestamp` is refused by both with one code.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn check_strict_and_run_strict_agree_on_an_undeclared_timestamp_9_and_refuse_a_declared_lossy_one()
{
    require_alive(LiveService::Oracle);
    let t = oracle_ts9_table();
    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    let rig = Rig::oracle_batch(t.name()).dest_path(out.clone());

    let c = rig.run_after(
        &["check", "--type-report", "--strict"],
        &["--strict"],
        Pairing::SameRefusal,
    );
    assert!(
        c.green,
        "check --strict:\n{}\nrun --strict:\n{}",
        String::from_utf8_lossy(&c.preflight.stderr),
        String::from_utf8_lossy(&c.run.stderr)
    );
    crate::live_narrowing_override::assert_delivered(
        &out,
        &["ID", "TS9"],
        &["1", "2024-01-02T03:04:05.123456789"],
        1,
        "run --strict delivers the text",
    );

    let lossy_out = d.path().join("lossy");
    let lossy = Rig::oracle_batch(t.name())
        .dest_path(lossy_out.clone())
        .export_line("columns: {TS9: timestamp}")
        .run_after(
            &["check", "--type-report", "--strict"],
            &["--strict"],
            Pairing::SameRefusal,
        );
    assert!(!lossy.green, "a declared lossy timestamp under --strict");
    for (who, err) in [
        ("check", &lossy.preflight.stderr),
        ("run", &lossy.run.stderr),
    ] {
        let err = String::from_utf8_lossy(err);
        assert!(err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{who}: {err}");
        assert!(err.contains("column 'TS9'"), "{who}: {err}");
    }
    assert_eq!(parquet_parts(&lossy_out), 0, "run --strict wrote a part");
}

/// A PostgreSQL json[], jsonb[], interval[], oid[] and enum[] column is delivered as a list of its elements, NULL elements kept.
#[test]
#[ignore = "live: requires docker compose postgres + duckdb"]
fn a_postgres_json_interval_oid_enum_array_is_delivered_as_a_list_of_its_elements() {
    require_alive(LiveService::Postgres);
    let table = unique_name("tp_arr_wire");
    let ty = format!("{table}_st");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TYPE {ty} AS ENUM ('active', 'pending');
         CREATE TABLE {table} (id INT PRIMARY KEY, a_json JSON[], a_jsonb JSONB[], a_interval INTERVAL[], a_oid OID[], a_enum {ty}[]);
         INSERT INTO {table} VALUES (1, ARRAY['{{\"a\":1}}'::json, NULL], ARRAY['{{\"a\":1}}'::jsonb, NULL],
            ARRAY['1 day'::interval, '2 hours'::interval, NULL], ARRAY[1::oid, 4294967295::oid, NULL],
            ARRAY['active'::{ty}, 'pending', NULL])"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table.clone());
    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    let run = Rig::pg_batch(&table)
        .dest_path(out.clone())
        .no_oracle(
            "the cell reads every list element back through DuckDB; the matrix grades these types",
        )
        .run_args(&[]);
    c.batch_execute(&format!("DROP TABLE {table}; DROP TYPE {ty}"))
        .unwrap();
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(run.status.success(), "{err}");
    let filter = "a_json[1] = '{\"a\":1}' AND len(a_json) = 2 AND a_json[2] IS NULL \
        AND a_jsonb[1] = '{\"a\": 1}' AND len(a_jsonb) = 2 \
        AND a_interval[1] = 'P1D' AND a_interval[2] = 'PT2H' AND len(a_interval) = 3 \
        AND a_oid[1] = 1 AND a_oid[2] = 4294967295 AND len(a_oid) = 3 \
        AND a_enum[1] = 'active' AND a_enum[2] = 'pending' AND len(a_enum) = 3";
    assert_eq!(duckdb_dir_scalar(&out, "count(*)", Some(filter)), 1);
}

/// A two-dimensional jsonb[] keeps its refusal naming the multi-dimensional cause, the column and the cast to text.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_postgres_two_dimensional_jsonb_array_keeps_the_multi_dimensional_refusal() {
    require_alive(LiveService::Postgres);
    let table = unique_name("tp_arr_2d");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, grid JSONB[]);
         INSERT INTO {table} VALUES (1, ARRAY[ARRAY['1'::jsonb,'2'::jsonb],ARRAY['3'::jsonb,'4'::jsonb]])"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table.clone());
    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    let run = Rig::pg_batch(&table).dest_path(out.clone()).run_args(&[]);
    let err = String::from_utf8_lossy(&run.stderr).to_lowercase();
    assert!(!run.status.success(), "{err}");
    assert!(
        err.contains("multi-dimensional") && err.contains("grid") && err.contains("::text"),
        "{err}"
    );
    assert_eq!(parquet_parts(&out), 0, "a refused run wrote a part");
}

/// An Oracle table of `columns` with `rows` inserted, exported by `query` (the whole table when `None`) under `out`.
fn oracle_export(
    columns: &str,
    rows: &[&str],
    query: Option<&str>,
    out: &std::path::Path,
) -> (std::process::Output, OracleTable) {
    let t = OracleTable::create("tp_wire", columns);
    for r in rows {
        ora_exec(&format!("INSERT INTO {} VALUES ({r})", t.name()));
    }
    let mut rig = Rig::oracle_batch(t.name()).dest_path(out.to_path_buf());
    if let Some(q) = query {
        rig = rig.query(&q.replace("{t}", t.name()));
    }
    (rig.run_args(&[]), t)
}

/// An Oracle NUMBER infinity or an exact 1E125 is refused with a code, the column and the `TO_CHAR` remedy, which then delivers the server's own text; finite values are delivered as before.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_oracle_number_infinity_is_refused_by_code_and_to_char_delivers_the_server_text() {
    require_alive(LiveService::Oracle);
    let d = tempfile::tempdir().unwrap();
    let cols = "id NUMBER(10) PRIMARY KEY, v FLOAT(10), n NUMBER";
    let inf = "1, 9.9999999999999999999999999999999999999E125, 1";
    let exact = "2, 1, 1E125";
    let fin = "3, 1.5, 123.456";
    for (n, row) in [inf, exact].into_iter().enumerate() {
        let out = d.path().join(format!("refused{n}"));
        let (run, _t) = oracle_export(cols, &[row], Some("SELECT id, v, n FROM {t}"), &out);
        let err = String::from_utf8_lossy(&run.stderr);
        assert_eq!(run.status.code(), Some(5), "{err}");
        assert!(err.contains("RIVET_SOURCE_VALUE_UNREPRESENTABLE"), "{err}");
        let col = if n == 0 { "V" } else { "N" };
        assert!(err.contains(&format!("column `{col}`")), "{err}");
        assert!(err.contains(&format!("`TO_CHAR({col}) AS {col}`")), "{err}");
        assert_eq!(parquet_parts(&out), 0, "a refused run wrote a part");
    }
    let out = d.path().join("remedy");
    let (run, _t) = oracle_export(
        cols,
        &[inf, exact.replace("2,", "4,").as_str(), fin],
        Some("SELECT id, TO_CHAR(v) AS v, TO_CHAR(n) AS n FROM {t}"),
        &out,
    );
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let filter = "(\"ID\" = 1 AND \"V\" = '~') OR (\"ID\" = 4 AND \"N\" LIKE '1.0%E+125') OR (\"ID\" = 3 AND \"V\" = '1.5' AND \"N\" = '123.456')";
    assert_eq!(duckdb_dir_scalar(&out, "count(*)", Some(filter)), 3);
}

/// An Oracle NUMBER infinity read through a declared `float64` column is refused by code, and a finite value in the same column is delivered.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_oracle_number_infinity_in_a_float64_override_is_refused_and_finite_is_delivered() {
    require_alive(LiveService::Oracle);
    let d = tempfile::tempdir().unwrap();
    let run_with = |tag: &str, row: &str| {
        let t = OracleTable::create("tp_f64", "id NUMBER(10) PRIMARY KEY, n FLOAT(10)");
        ora_exec(&format!("INSERT INTO {} VALUES ({row})", t.name()));
        let out = d.path().join(tag);
        let run = Rig::oracle_batch(t.name())
            .no_oracle("the column carries a declared `float64` override whose own rendering is this test's subject")
            .export_line("columns: {N: float64}")
            .dest_path(out.clone())
            .run_args(&[]);
        (run, out)
    };
    let (run, out) = run_with("inf", "1, 9.9999999999999999999999999999999999999E125");
    let err = String::from_utf8_lossy(&run.stderr);
    assert_eq!(run.status.code(), Some(5), "{err}");
    assert!(err.contains("RIVET_SOURCE_VALUE_UNREPRESENTABLE"), "{err}");
    assert!(err.contains("column `N`"), "{err}");
    assert_eq!(parquet_parts(&out), 0, "a refused run wrote a part");
    let (run, out) = run_with("fin", "2, 123.5");
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(parquet_parts(&out), 1);
}

/// An Oracle INTERVAL DAY TO SECOND with a sub-microsecond fraction is refused with a code, the column and the `TO_CHAR` remedy, which delivers the server's text.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_oracle_interval_with_nanoseconds_is_refused_by_code_and_to_char_delivers_the_text() {
    require_alive(LiveService::Oracle);
    let d = tempfile::tempdir().unwrap();
    let cols = "id NUMBER(10) PRIMARY KEY, iv INTERVAL DAY(9) TO SECOND(9)";
    let ns = "1, INTERVAL '1 02:03:04.000000005' DAY(9) TO SECOND(9)";
    let out = d.path().join("refused");
    let (run, _t) = oracle_export(cols, &[ns], None, &out);
    let err = String::from_utf8_lossy(&run.stderr);
    assert_eq!(run.status.code(), Some(5), "{err}");
    assert!(err.contains("RIVET_SOURCE_VALUE_UNREPRESENTABLE"), "{err}");
    assert!(
        err.contains("column `IV`") && err.contains("(5 ns)"),
        "{err}"
    );
    assert!(err.contains("`TO_CHAR(IV) AS IV`"), "{err}");
    assert_eq!(parquet_parts(&out), 0, "a refused run wrote a part");
    let out = d.path().join("remedy");
    let (run, _t) = oracle_export(
        cols,
        &[ns],
        Some("SELECT id, TO_CHAR(iv) AS iv FROM {t}"),
        &out,
    );
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(
        duckdb_dir_scalar(
            &out,
            "count(*)",
            Some("\"IV\" = '+000000001 02:03:04.000000005'")
        ),
        1
    );
}

/// Finite Oracle NUMBER and FLOAT values (also the largest 38-digit one) and a microsecond-exact INTERVAL DAY TO SECOND are delivered unchanged.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn finite_oracle_numbers_and_a_microsecond_interval_are_delivered_unchanged() {
    require_alive(LiveService::Oracle);
    let d = tempfile::tempdir().unwrap();
    let cols = "id NUMBER(10) PRIMARY KEY, v FLOAT(10), n NUMBER";
    let fin = "3, 1.5, 123.456";
    let out = d.path().join("finite");
    let (run, _t) = oracle_export(
        cols,
        &[
            fin,
            "5, 1.5E124, 9.9999999999999999999999999999999999999E125",
        ],
        None,
        &out,
    );
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(duckdb_dir_scalar(&out, "count(*)", None), 2);
    let cols = "id NUMBER(10) PRIMARY KEY, iv INTERVAL DAY(9) TO SECOND(9)";
    let us = "2, INTERVAL '1 02:03:04.123456000' DAY(9) TO SECOND(9)";
    let out = d.path().join("micro");
    let (run, _t) = oracle_export(cols, &[us], None, &out);
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(
        duckdb_dir_scalar(&out, "count(*)", Some("\"IV\" = 'P1DT2H3M4.123456S'")),
        1
    );
}
