//! The Oracle side of `live_narrowing_override`: a `columns:` time override over `TIMESTAMP(7..9)` is graded per result column on batch and on CDC.

use crate::common::*;
use crate::live_narrowing_override::*;

/// `id NUMBER(10) PK, ts9 TIMESTAMP(9), ts3 TIMESTAMP(3)`, one row.
fn table() -> OracleTable {
    let t = OracleTable::create(
        "nso_or",
        "id NUMBER(10) PRIMARY KEY, ts9 TIMESTAMP(9), ts3 TIMESTAMP(3)",
    );
    ora_exec(&format!(
        "INSERT INTO {} VALUES (1, TIMESTAMP '2024-01-02 03:04:05.123456789', TIMESTAMP '2024-01-02 03:04:05.123')",
        t.name()
    ));
    t
}

/// An Oracle batch override is graded per result column: the table, an alias, a join and a CAST to a coarser type each draw the verdict their own column deserves.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_oracle_batch_time_override_is_graded_by_the_result_column_it_lands_on() {
    require_alive(LiveService::Oracle);
    let t = table();
    let n = t.name();
    let cases: &[BatchCase] = &[
        (
            "table",
            None,
            "TS9: timestamp",
            Verdict::Lossy,
            "TS9",
            "2024-01-02T03:04:05.123456",
        ),
        (
            "table ns",
            None,
            "TS9: timestamp_ns",
            Verdict::Exact,
            "TS9",
            "2024-01-02T03:04:05.123456789",
        ),
        (
            "table ts3",
            None,
            "TS9: timestamp_ns, TS3: timestamp",
            Verdict::Exact,
            "TS3",
            "2024-01-02T03:04:05.123",
        ),
        (
            "cast",
            Some(format!(
                "SELECT ID, CAST(TS9 AS TIMESTAMP(3)) AS TS9 FROM {n}"
            )),
            "TS9: timestamp",
            Verdict::Exact,
            "TS9",
            "2024-01-02T03:04:05.123",
        ),
        (
            "alias",
            Some(format!("SELECT ID, TS9 AS X FROM {n}")),
            "X: timestamp",
            Verdict::Lossy,
            "X",
            "2024-01-02T03:04:05.123456",
        ),
        (
            "join",
            Some(format!(
                "SELECT a.ID, b.TS9 FROM {n} a JOIN {n} b ON a.ID = b.ID"
            )),
            "TS9: timestamp",
            Verdict::Lossy,
            "TS9",
            "2024-01-02T03:04:05.123456",
        ),
    ];
    for (i, (name, query, decl, want, col, value)) in cases.iter().enumerate() {
        let out = std::env::temp_dir().join(format!("{n}_{i}"));
        let _ = std::fs::remove_dir_all(&out);
        std::fs::create_dir_all(&out).unwrap();
        let make = || {
            let mut rig = Rig::oracle_batch(n).dest_path(out.clone());
            if let Some(q) = query {
                rig = rig.query(q);
            }
            rig.export_line(&format!("columns: {{{decl}}}"))
        };
        let out = graded(&make, *want, col, name);
        assert_delivered(&out, &["ID", col], &["1", value], delivered(*want), name);
        let _ = std::fs::remove_dir_all(&out);
    }
    for decl in ["TS9: date", "TS3: date"] {
        let rig = Rig::oracle_batch(n).export_line(&format!("columns: {{{decl}}}"));
        refused_on_check_and_run(&rig, "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", decl);
    }
}
