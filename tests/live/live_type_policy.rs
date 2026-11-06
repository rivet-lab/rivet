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

/// An undeclared Oracle CDC `TIMESTAMP(9)` is refused with or without `--strict`, before any part or checkpoint move; the declared `timestamp` delivers the row.
#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_timestamp9_is_refused_until_declared_and_a_declared_timestamp_delivers_the_row() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = OracleTable::create("tp_cts9", "id NUMBER(10) PRIMARY KEY, ts9 TIMESTAMP(9)");
    ora_exec(&format!(
        "ALTER TABLE {} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        t.name()
    ));
    ora_exec(&format!("GRANT SELECT ON {} TO c##rivetcdc", t.name()));
    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let rig = |declared: bool| {
        let r = Rig::oracle_cdc(t.name())
            .checkpoint_path(ckpt.clone())
            .dest_path(out.clone());
        match declared {
            true => r.export_line("columns: {TS9: timestamp}"),
            false => r,
        }
    };
    let refused = |args: &[&str]| {
        let run = rig(false).run_args(args);
        let err = String::from_utf8_lossy(&run.stderr).to_string();
        assert!(!run.status.success(), "{args:?} must refuse:\n{err}");
        assert!(err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{err}");
        assert!(
            err.contains("column 'TS9'") && err.contains("`TS9: timestamp`"),
            "the refusal names the column and the declaration CDC can read:\n{err}"
        );
        assert!(!err.contains("strict mode"), "{err}");
    };

    rig(true).run_ok();
    let anchored = std::fs::read(&ckpt).expect("the declared run anchors");
    ora_exec(&format!(
        "INSERT INTO {} VALUES (1, TIMESTAMP '2024-01-02 03:04:05.123456789')",
        t.name()
    ));
    let parts_before = parquet_parts(&out);

    refused(&[]);
    refused(&["--strict"]);
    assert_eq!(
        parquet_parts(&out),
        parts_before,
        "a refused run wrote a part"
    );
    assert_eq!(
        std::fs::read(&ckpt).unwrap(),
        anchored,
        "a refused run moved the checkpoint"
    );

    rig(true).run_ok();
    assert_eq!(
        duckdb_dir_scalar(
            &out,
            "count(*)",
            Some("__op = 'insert' AND ID = 1 AND TS9 = TIMESTAMP '2024-01-02 03:04:05.123456'")
        ),
        1,
        "the declared timestamp keeps microseconds"
    );
}

/// An undeclared Oracle batch `TIMESTAMP(9)` is refused with or without `--strict` before any part; declared `timestamp`, it runs under both.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn oracle_batch_timestamp9_is_refused_until_declared_under_every_policy() {
    require_alive(LiveService::Oracle);
    let t = oracle_ts9_table();
    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    let rig = Rig::oracle_batch(t.name()).dest_path(out.clone());

    for args in [&["--strict"][..], &[][..]] {
        let run = rig.run_args(args);
        let err = String::from_utf8_lossy(&run.stderr);
        assert!(!run.status.success(), "{args:?} must refuse:\n{err}");
        assert!(err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{err}");
        assert!(err.contains("column 'TS9'"), "{err}");
        assert_eq!(parquet_parts(&out), 0, "a refused run wrote a part");
    }

    for (n, args) in [&["--strict"][..], &[][..]].into_iter().enumerate() {
        let declared_out = d.path().join(format!("declared{n}"));
        let declared = Rig::oracle_batch(t.name())
            .dest_path(declared_out.clone())
            .export_line("columns: {TS9: timestamp}");
        let run = declared.run_args(args);
        let err = String::from_utf8_lossy(&run.stderr);
        assert!(run.status.success(), "{args:?} declared:\n{err}");
        assert!(!err.contains("fidelity=lossy"), "{err}");
        assert_eq!(
            duckdb_dir_scalar(&declared_out, "count(*)", Some("\"ID\" = 1")),
            1
        );
    }
}

#[test]
#[ignore = "live: requires docker compose oracle"]
fn check_strict_and_run_strict_refuse_the_same_config_with_the_same_code() {
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
    let check_err = String::from_utf8_lossy(&c.preflight.stderr);
    let run_err = String::from_utf8_lossy(&c.run.stderr);
    assert!(
        !c.preflight.status.success(),
        "check --strict:\n{check_err}"
    );
    for (who, err) in [("check", &check_err), ("run", &run_err)] {
        assert!(err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{who}: {err}");
        assert!(err.contains("column 'TS9'"), "{who}: {err}");
    }
    assert_eq!(parquet_parts(&out), 0, "run --strict wrote a part");

    let lax = rig.run_after(&["check", "--type-report"], &[], Pairing::SameRefusal);
    assert!(
        !lax.green,
        "an undeclared TIMESTAMP(9) is refused without --strict too:\n{}",
        String::from_utf8_lossy(&lax.preflight.stderr)
    );
    for err in [&lax.preflight.stderr, &lax.run.stderr] {
        let err = String::from_utf8_lossy(err);
        assert!(err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{err}");
        assert!(!err.contains("strict mode"), "{err}");
    }
    assert_eq!(parquet_parts(&out), 0, "the lax run wrote a part");
}
