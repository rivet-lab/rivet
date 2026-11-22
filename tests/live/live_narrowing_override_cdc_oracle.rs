//! The Oracle CDC side of `live_narrowing_override`: the stream grades a `timestamp` override over `TIMESTAMP(9)` as the batch run does.

use crate::common::*;
use crate::live_narrowing_override::*;

/// An Oracle CDC declared `timestamp` over `TIMESTAMP(9)` is lossy and refused by `--strict`, over `TIMESTAMP(3)` exact; the change row holds what the verdict says.
#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn an_oracle_cdc_time_override_is_graded_as_the_batch_override() {
    let _serial = cross_process_serial("oracle_cdc");
    let cases: &[(&str, &str, &str, Verdict, &str)] = &[
        (
            "nso_cor9",
            "id NUMBER(10) PRIMARY KEY, ts9 TIMESTAMP(9)",
            "TS9: timestamp",
            Verdict::Lossy,
            "TS9 = TIMESTAMP '2024-01-02 03:04:05.123456'",
        ),
        (
            "nso_cor3",
            "id NUMBER(10) PRIMARY KEY, ts3 TIMESTAMP(3)",
            "TS3: timestamp",
            Verdict::Exact,
            "TS3 = TIMESTAMP '2024-01-02 03:04:05.123'",
        ),
    ];
    for (label, cols, decl, want, row) in cases {
        let t = OracleTable::create(label, cols);
        ora_exec(&format!(
            "ALTER TABLE {} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
            t.name()
        ));
        ora_exec(&format!("GRANT SELECT ON {} TO c##rivetcdc", t.name()));
        let d = tempfile::tempdir().unwrap();
        let out = d.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let rig = Rig::oracle_cdc(t.name())
            .checkpoint_path(d.path().join("cdc.ckpt"))
            .dest_path(out.clone())
            .export_line(&format!("columns: {{{decl}}}"));
        check_verdict(&rig, *want, col_of(row), label);
        strict_verdict(&rig, *want, label);
        lax_runs(&rig, label);
        let value = if *want == Verdict::Lossy {
            "TIMESTAMP '2024-01-02 03:04:05.123456789'"
        } else {
            "TIMESTAMP '2024-01-02 03:04:05.123'"
        };
        ora_exec(&format!("INSERT INTO {} VALUES (1, {value})", t.name()));
        rig.run_ok();
        assert_eq!(
            crate::live_narrowing_override::count_where(
                &out,
                &format!("__op = 'insert' AND ID = 1 AND {row}")
            ),
            1,
            "{label}: delivered change row"
        );
    }
}
