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
    let cases: &[(&str, Option<String>, &str, Verdict, &str)] = &[
        (
            "table",
            None,
            "TS9: timestamp",
            Verdict::Lossy,
            "TS9 = TIMESTAMP '2024-01-02 03:04:05.123456'",
        ),
        (
            "table ns",
            None,
            "TS9: timestamp_ns",
            Verdict::Exact,
            "TS9 = TIMESTAMP_NS '2024-01-02 03:04:05.123456789'",
        ),
        (
            "table ts3",
            None,
            "TS9: timestamp_ns, TS3: timestamp",
            Verdict::Exact,
            "TS3 = TIMESTAMP '2024-01-02 03:04:05.123'",
        ),
        (
            "cast",
            Some(format!(
                "SELECT ID, CAST(TS9 AS TIMESTAMP(3)) AS TS9 FROM {n}"
            )),
            "TS9: timestamp",
            Verdict::Exact,
            "TS9 = TIMESTAMP '2024-01-02 03:04:05.123'",
        ),
        (
            "alias",
            Some(format!("SELECT ID, TS9 AS X FROM {n}")),
            "X: timestamp",
            Verdict::Lossy,
            "X = TIMESTAMP '2024-01-02 03:04:05.123456'",
        ),
        (
            "join",
            Some(format!(
                "SELECT a.ID, b.TS9 FROM {n} a JOIN {n} b ON a.ID = b.ID"
            )),
            "TS9: timestamp",
            Verdict::Lossy,
            "TS9 = TIMESTAMP '2024-01-02 03:04:05.123456'",
        ),
    ];
    for (i, (name, query, decl, want, row)) in cases.iter().enumerate() {
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
        let out = graded(&make, *want, col_of(row), name);
        assert_eq!(
            count_where(&out, &format!("ID = 1 AND {row}")),
            delivered(*want),
            "{name}: delivered row"
        );
        let _ = std::fs::remove_dir_all(&out);
    }
}
