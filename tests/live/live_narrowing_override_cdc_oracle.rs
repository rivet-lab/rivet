//! The Oracle CDC side of `live_narrowing_override`: the stream grades a time override over `TIMESTAMP(9)` and `TIMESTAMP(9) WITH TIME ZONE` as the batch run does.

use crate::common::*;
use crate::live_narrowing_override::*;

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

/// An Oracle CDC declared `timestamp` / `timestamp_tz` over `TIMESTAMP(9)` [WITH TIME ZONE] is lossy and refused by `--strict`, `timestamp` over `TIMESTAMP(3)` exact; the change row holds the microseconds the verdict says.
#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn an_oracle_cdc_time_override_is_graded_as_the_batch_override() {
    let _serial = cross_process_serial("oracle_cdc");
    let cases: &[CdcCase] = &[
        (
            "nso_cor9",
            "id NUMBER(10) PRIMARY KEY, ts9 TIMESTAMP(9)",
            "TS9: timestamp",
            "TS9",
            Verdict::Lossy,
            "TIMESTAMP '2024-01-02 03:04:05.123456789'",
            "2024-01-02T03:04:05.123456",
        ),
        (
            "nso_cor3",
            "id NUMBER(10) PRIMARY KEY, ts3 TIMESTAMP(3)",
            "TS3: timestamp",
            "TS3",
            Verdict::Exact,
            "TIMESTAMP '2024-01-02 03:04:05.123'",
            "2024-01-02T03:04:05.123",
        ),
        (
            "nso_cortz9",
            "id NUMBER(10) PRIMARY KEY, tz9 TIMESTAMP(9) WITH TIME ZONE",
            "TZ9: timestamp_tz",
            "TZ9",
            Verdict::Lossy,
            "TIMESTAMP '2024-01-02 03:04:05.123456789 +00:00'",
            "2024-01-02T03:04:05.123456Z",
        ),
    ];
    for (label, cols, decl, col, want, value, shown) in cases {
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
        check_verdict(&rig, *want, col, label);
        strict_verdict(&rig, *want, label);
        lax_runs(&rig, label);
        ora_exec(&format!("INSERT INTO {} VALUES (1, {value})", t.name()));
        rig.run_ok();
        assert_eq!(total_parquet_rows(&out), 1, "{label}");
        assert_delivered(
            &out,
            &["__op", "ID", col],
            &["insert", "1", shown],
            1,
            label,
        );
    }
}
