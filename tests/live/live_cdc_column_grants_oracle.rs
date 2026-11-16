//! Live `source.columns_from: grants` (Oracle): the account's readable columns are asked of the server, and a table it may not read is refused by code.

use std::path::Path;

use super::live_cdc_oracledb::cdc_table;
use crate::common::*;

const SECRET: &str = "SECRET-";
const BOUNDARY_TEXT: &str = "columns_from: grants - batch reads are enforced by the server (a column the \
account may not SELECT is never selected). The Oracle change stream asks LogMiner for the readable columns \
only, so a hidden column's value is never fetched by rivet's own query; that is data minimisation, not \
access control: the capture account's LogMiner views and the redo itself still carry every column, so \
restrict the capture account if the values must not be readable with another client. Oracle has no \
column-level SELECT privilege: a table is readable whole or not at all, so the readable set of a table you \
may SELECT is every column and a table you may not SELECT is refused.";

/// `table(id, name, pass INVISIBLE)` the capture user may SELECT, holding `(1,'ann',SECRET)`, `(2,'bob',SECRET)`, `(3,'cid',SECRET)`.
fn invisible_table(label: &str, columns: &str) -> OracleTable {
    let t = cdc_table(label, columns);
    for (id, name) in [(1, "ann"), (2, "bob"), (3, "cid")] {
        ora_exec(&format!(
            "INSERT INTO {} (id, name, pass) VALUES ({id}, '{name}', '{SECRET}{id}')",
            t.name()
        ));
    }
    t
}

const HIDDEN_PASS: &str =
    "id NUMBER(18) PRIMARY KEY, name VARCHAR2(20), pass VARCHAR2(20) INVISIBLE";

/// Every directory under `dir` that holds a parquet part, at any depth.
fn parquet_dirs(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut dirs: Vec<_> = files_under(dir)
        .into_iter()
        .filter(|f| f.extension().is_some_and(|x| x == "parquet"))
        .filter_map(|f| f.parent().map(Path::to_path_buf))
        .collect();
    dirs.sort();
    dirs.dedup();
    dirs
}

/// Every part under `dir`, at any depth.
fn parts_under(dir: &Path) -> Vec<arrow::record_batch::RecordBatch> {
    parquet_dirs(dir)
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect()
}

/// Every column name and every rendered cell of `parts`.
fn names_and_cells(parts: &[arrow::record_batch::RecordBatch]) -> (Vec<String>, Vec<String>) {
    let (mut names, mut cells) = (Vec::new(), Vec::new());
    for b in parts {
        for (field, col) in b.schema().fields().iter().zip(b.columns()) {
            names.push(field.name().clone());
            for i in 0..b.num_rows() {
                cells.push(arrow::util::display::array_value_to_string(col, i).unwrap());
            }
        }
    }
    (names, cells)
}

/// `(op, id, name)` of every change in `parts`, in capture order.
fn changes(parts: &[arrow::record_batch::RecordBatch]) -> Vec<(String, i64, String)> {
    let mut out = Vec::new();
    for b in parts.iter().filter(|b| b.column_by_name("__op").is_some()) {
        let s = |n: &str, i: usize| {
            arrow::util::display::array_value_to_string(b.column_by_name(n).unwrap(), i).unwrap()
        };
        for i in 0..b.num_rows() {
            let seq: i64 = s("__seq", i).parse().unwrap();
            out.push((
                seq,
                (s("__op", i), s("ID", i).parse().unwrap(), s("NAME", i)),
            ));
        }
    }
    out.sort_by_key(|(seq, _)| *seq);
    out.into_iter().map(|(_, c)| c).collect()
}

/// No hidden column by name and no hidden value in `parts`; returns the delivered cells.
fn assert_no_hidden(parts: &[arrow::record_batch::RecordBatch], what: &str) -> Vec<String> {
    let (names, cells) = names_and_cells(parts);
    assert!(
        !names.iter().any(|n| n.eq_ignore_ascii_case("pass")),
        "{what}: `pass` is a column of the delivered parts: {names:?}"
    );
    assert!(
        !cells.iter().any(|v| v.contains(SECRET)),
        "{what}: a hidden value was delivered: {cells:?}"
    );
    cells
}

/// The one text a run said, stdout and stderr.
fn said_by(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// The text of every statement the server cached that reads `RIVET.<table>`, asked of `v$sql` as `SYSTEM`.
fn statements_reading(table: &str) -> Vec<String> {
    let conn = ora_system_conn();
    let cursor = conn
        .query(
            &format!(
                "SELECT sql_text FROM v$sql WHERE upper(sql_text) LIKE 'SELECT %' \
                 AND upper(sql_text) LIKE '%\"RIVET\".\"{table}\"%' \
                 AND upper(sql_text) NOT LIKE '%V$SQL%'"
            ),
            &[],
        )
        .unwrap();
    cursor
        .map(|row| {
            row.unwrap()
                .get::<Option<String>>(0)
                .unwrap()
                .unwrap_or_default()
        })
        .collect()
}

/// A table `RIVET.<name>` the capture user holds `UPDATE (name)` on and no `SELECT`: visible in the catalog, denied on read.
fn unreadable_table(label: &str) -> OracleTable {
    let t = OracleTable::create(label, "id NUMBER(18) PRIMARY KEY, name VARCHAR2(20)");
    ora_exec(&format!(
        "ALTER TABLE {} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        t.name()
    ));
    ora_exec(&format!(
        "INSERT INTO {} (id, name) VALUES (1, 'ann')",
        t.name()
    ));
    ora_exec(&format!(
        "GRANT UPDATE (name) ON {} TO c##rivetcdc",
        t.name()
    ));
    t
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn an_invisible_column_is_never_delivered_by_name_or_value_in_cdc_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = invisible_table("ogr_cdc", HIDDEN_PASS);
    let d = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_cdc(t.name())
        .checkpoint_path(d.path().join("cdc.ckpt"))
        .dest_path(d.path().join("out"))
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    let check = rig.cli(&["check"]);
    assert!(
        check.status.success(),
        "check under the policy:\n{}",
        said_by(&check)
    );
    rig.run_ok();
    for sql in [
        format!(
            "UPDATE {} SET pass = '{SECRET}a', name = 'ann2' WHERE id = 1",
            t.name()
        ),
        format!(
            "INSERT INTO {} (id, name, pass) VALUES (4, 'dee', '{SECRET}ins')",
            t.name()
        ),
        format!("DELETE FROM {} WHERE id = 2", t.name()),
    ] {
        ora_exec(&sql);
    }
    rig.run_ok();

    let parts = parts_under(&d.path().join("out"));
    let cells = assert_no_hidden(&parts, "cdc");
    assert!(
        ["ann", "bob", "cid", "ann2", "dee"]
            .iter()
            .all(|n| cells.iter().any(|v| v == n)),
        "the snapshot and the change parts were both read: {cells:?}"
    );
    assert_eq!(
        changes(&parts),
        vec![
            ("update".into(), 1, "ann2".into()),
            ("insert".into(), 4, "dee".into()),
            ("delete".into(), 2, "bob".into()),
        ],
        "every change is delivered with the readable columns only"
    );
    let seen = statements_reading(t.name());
    assert!(
        seen.iter().any(|s| s.contains("\"ID\", \"NAME\" FROM")),
        "the server saw the snapshot read the readable columns by name: {seen:?}"
    );
    assert!(
        !seen.iter().any(|s| s.to_uppercase().contains("PASS")),
        "no statement rivet sent names the invisible column: {seen:?}"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn an_invisible_column_is_never_read_in_every_batch_mode_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = invisible_table("ogr_bat", HIDDEN_PASS);
    for (mode, lines) in [
        ("full", &[][..]),
        ("chunked", &["chunk_by_key: ID", "chunk_size: 1"][..]),
        ("incremental", &["cursor_column: ID"][..]),
    ] {
        let rig = Rig::oracle_cdc(t.name())
            .source_line("columns_from: grants")
            .into_full_batch()
            .restage(mode, lines);
        let check = rig.cli(&["check"]);
        assert!(
            check.status.success(),
            "{mode}: check under the policy:\n{}",
            said_by(&check)
        );
        rig.run_ok();
        let cells = assert_no_hidden(&parts_under(&rig.out_dir()), mode);
        for name in ["ann", "bob", "cid"] {
            assert!(
                cells.iter().any(|v| v == name),
                "{mode}: every row is read: {cells:?}"
            );
        }
    }
    let seen = statements_reading(t.name());
    assert!(
        seen.iter().any(|s| s.contains("\"ID\", \"NAME\" FROM")),
        "the server saw the read name the readable columns: {seen:?}"
    );
    assert!(
        !seen.iter().any(|s| s.to_uppercase().contains("PASS")),
        "no statement rivet sent names the invisible column: {seen:?}"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn a_table_the_account_may_read_whole_is_delivered_whole_under_the_policy_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = invisible_table(
        "ogr_whole",
        "id NUMBER(18) PRIMARY KEY, name VARCHAR2(20), pass VARCHAR2(20)",
    );
    let d = tempfile::tempdir().unwrap();
    let delivered = |policy: bool, leg: &str| {
        let mut rig = Rig::oracle_cdc(t.name())
            .checkpoint_path(d.path().join(format!("{leg}.ckpt")))
            .dest_path(d.path().join(leg))
            .cdc("initial: snapshot");
        if policy {
            rig = rig.source_line("columns_from: grants");
        }
        rig.run_ok();
        ora_exec(&format!(
            "UPDATE {} SET pass = '{SECRET}u{leg}' WHERE id = 1",
            t.name()
        ));
        rig.run_ok();
        let read: Vec<_> = parquet_dirs(&d.path().join(leg))
            .iter()
            .flat_map(|p| read_all_parts(p))
            .collect();
        let (mut names, mut cells) = names_and_cells(&read);
        names.sort();
        cells.sort();
        (names, cells)
    };
    let (off_names, off_cells) = delivered(false, "off");
    let (on_names, on_cells) = delivered(true, "on");
    assert!(
        on_names.iter().any(|n| n == "PASS")
            && on_cells.iter().any(|v| v == &format!("{SECRET}uon")),
        "an account that may read `pass` is delivered it under the policy: {on_names:?} {on_cells:?}"
    );
    assert_eq!(
        on_names, off_names,
        "the policy changes no delivered column of a whole-readable table"
    );
    assert_eq!(
        on_cells.len(),
        off_cells.len(),
        "the policy changes no delivered row of a whole-readable table"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn a_table_the_account_may_not_read_is_refused_by_code_in_batch_cdc_and_check_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = unreadable_table("ogr_deny");
    let d = tempfile::tempdir().unwrap();

    let off = Rig::oracle_cdc(t.name())
        .into_full_batch()
        .dest_path(d.path().join("off"))
        .run();
    assert_refused(&off, Refused::by_code("RIVET_SOURCE_QUERY_FAILED", 1));
    assert!(
        said_by(&off).contains("ORA-41900"),
        "without the policy the server's own refusal stands:\n{}",
        said_by(&off)
    );

    let batch = Rig::oracle_cdc(t.name())
        .source_line("columns_from: grants")
        .into_full_batch()
        .dest_path(d.path().join("batch"));
    let cdc = Rig::oracle_cdc(t.name())
        .source_line("columns_from: grants")
        .checkpoint_path(d.path().join("cdc.ckpt"))
        .dest_path(d.path().join("cdc"))
        .cdc("initial: snapshot");
    for (what, out) in [
        ("batch run", batch.run()),
        ("batch check", batch.cli(&["check"])),
        ("cdc run", cdc.run()),
        ("cdc check", cdc.cli(&["check"])),
    ] {
        assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
        let said = said_by(&out);
        assert!(
            said.contains("found no column this account may SELECT")
                && said.contains("GRANT SELECT on the table to the account"),
            "{what}: the refusal names the cause and the remedy:\n{said}"
        );
    }
    assert_refused_before_any_write(&d.path().join("cdc"), &d.path().join("cdc.ckpt"));
    assert!(
        parquet_dirs(&d.path().join("batch")).is_empty(),
        "a refused batch writes nothing"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn an_unreadable_key_is_refused_by_code_before_the_anchor_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = invisible_table(
        "ogr_key",
        "id NUMBER(18) INVISIBLE PRIMARY KEY, name VARCHAR2(20), pass VARCHAR2(20)",
    );
    let d = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_cdc(t.name())
        .checkpoint_path(d.path().join("cdc.ckpt"))
        .dest_path(d.path().join("out"))
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    let out = rig.run();
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
    assert!(
        said_by(&out).contains("key column `ID`") && said_by(&out).contains("`load.pk`"),
        "an unreadable key is named with its remedy:\n{}",
        said_by(&out)
    );
    assert_refused_before_any_write(&d.path().join("out"), &d.path().join("cdc.ckpt"));
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn a_hidden_cursor_or_chunk_column_is_refused_by_code_before_the_read_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = invisible_table("ogr_cur", HIDDEN_PASS);
    for (what, mode, lines) in [
        ("cursor_column", "incremental", &["cursor_column: PASS"][..]),
        (
            "chunk_by_key",
            "chunked",
            &["chunk_by_key: PASS", "chunk_size: 1"][..],
        ),
    ] {
        let rig = Rig::oracle_cdc(t.name())
            .source_line("columns_from: grants")
            .into_full_batch()
            .restage(mode, lines);
        let out = rig.run();
        let said = said_by(&out);
        assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
        assert!(
            said.contains("`PASS`") && said.contains(what),
            "{what}: the refusal names the column and the setting:\n{said}"
        );
        assert_refused_before_any_write(&rig.out_dir(), &rig.checkpoint());
    }
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn the_policy_says_what_it_does_not_guarantee_at_check_and_at_the_start_of_a_run_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = invisible_table("ogr_say", HIDDEN_PASS);
    let d = tempfile::tempdir().unwrap();
    let on = Rig::oracle_cdc(t.name())
        .checkpoint_path(d.path().join("cdc.ckpt"))
        .dest_path(d.path().join("out"))
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    let check = on.cli(&["check"]);
    assert!(check.status.success(), "{}", said_by(&check));
    assert!(
        String::from_utf8_lossy(&check.stdout).contains(BOUNDARY_TEXT),
        "`rivet check` states the boundary of the policy:\n{}",
        said_by(&check)
    );
    let run = on.run_args_env(&["--export", t.name()], &[("RUST_LOG", "info")]);
    assert!(run.status.success(), "{}", said_by(&run));
    assert!(
        said_by(&run).contains(BOUNDARY_TEXT),
        "a change-stream run says it at its start:\n{}",
        said_by(&run)
    );
    let off = Rig::oracle_cdc(t.name()).cdc("initial: snapshot");
    assert!(
        !said_by(&off.cli(&["check"])).contains(BOUNDARY_TEXT),
        "a config without the policy says nothing about it"
    );
}
