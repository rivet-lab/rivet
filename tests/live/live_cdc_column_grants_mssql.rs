//! Live `source.columns_from: grants` (SQL Server): a column-grant login delivers only the columns it may SELECT.

use std::path::Path;

use crate::common::*;

const SECRET: &str = "SECRET-";
const PW: &str = "Cg_Passw0rd1";
const BOUNDARY_TEXT: &str = "columns_from: grants - batch reads are enforced by the server (a column the \
account may not SELECT is never selected). The SQL Server change stream is enforced by the server too: \
the capture instance's change function returns nothing to an account that may not SELECT every column \
the instance captures, so rivet refuses an instance wider than the account's grants and reads one made \
of exactly the columns it may SELECT (sys.sp_cdc_enable_table @captured_column_list). A grant change \
is seen when rivet reads each batch of the change table, not when it happens: the stream is then refused \
and nothing of that batch is delivered, but a REVOKE undone before the next read is not seen.";

/// A login that may SELECT only `columns` of `dbo.table`; dropped with it.
struct GrantLogin(String);

impl GrantLogin {
    /// A login with `grant` on the table, the documented `cdc` schema read and the performance-state read `rivet check` probes.
    fn with(grant: &str) -> Self {
        let name = unique_name("cg_u");
        mssql_cdc_exec(&format!(
            "CREATE LOGIN [{name}] WITH PASSWORD = '{PW}', CHECK_POLICY = OFF; \
             CREATE USER [{name}] FOR LOGIN [{name}]; \
             {grant} TO [{name}]; \
             GRANT SELECT ON SCHEMA::cdc TO [{name}]; \
             GRANT VIEW DATABASE PERFORMANCE STATE TO [{name}];"
        ));
        Self(name)
    }

    /// A login that may `SELECT` only `columns` of `table`.
    fn new(table: &str, columns: &str) -> Self {
        Self::with(&format!("GRANT SELECT ON dbo.{table} ({columns})"))
    }

    /// As `new`, with `SELECT` on the whole table.
    fn whole_table(table: &str) -> Self {
        Self::with(&format!("GRANT SELECT ON dbo.{table}"))
    }

    fn url(&self) -> String {
        MSSQL_CDC_URL.replace("sa:Rivet_Passw0rd!@", &format!("{}:{PW}@", self.0))
    }
}

impl Drop for GrantLogin {
    fn drop(&mut self) {
        mssql_cdc_try_exec(&format!(
            "IF USER_ID(N'{n}') IS NOT NULL DROP USER [{n}]; \
             IF SUSER_ID(N'{n}') IS NOT NULL DROP LOGIN [{n}];",
            n = self.0
        ));
    }
}

/// `table(id INT PRIMARY KEY, name, pass)` holding `(1,'ann',SECRET)`, `(2,'bob',SECRET)`, `(3,'cid',SECRET)`.
fn seeded_table(label: &str) -> String {
    let table = unique_name(label);
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} (id INT PRIMARY KEY, name VARCHAR(20), pass VARCHAR(20)); \
         INSERT INTO dbo.{table} VALUES (1, 'ann', '{SECRET}1'), (2, 'bob', '{SECRET}2'), \
         (3, 'cid', '{SECRET}3');"
    ));
    table
}

/// Capture instance `ci` of `dbo.table` over `columns` (`None`: every column), with no gating role.
fn capture(table: &str, ci: &str, columns: Option<&str>) {
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    let list = columns
        .map(|c| format!(", @captured_column_list = N'{c}'"))
        .unwrap_or_default();
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'{table}', \
         @role_name = NULL, @capture_instance = N'{ci}'{list};"
    ));
    wait_for_a_max_lsn();
}

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
                (s("__op", i), s("id", i).parse().unwrap(), s("name", i)),
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

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_column_grant_login_delivers_no_hidden_column_by_name_or_value_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_cdc");
    let ci = format!("dbo_{table}");
    capture(&table, &ci, Some("id, name"));
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::new(&table, "id, name");
    let rig = Rig::mssql_cdc(&table, &ci)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");

    let check = rig.cli(&["check"]);
    assert!(
        check.status.success(),
        "check under a column grant:\n{}",
        said_by(&check)
    );
    rig.run_ok();
    for sql in [
        format!("UPDATE dbo.{table} SET pass = '{SECRET}a', name = 'ann2' WHERE id = 1"),
        format!("INSERT INTO dbo.{table} VALUES (4, 'dee', '{SECRET}ins')"),
        format!("DELETE FROM dbo.{table} WHERE id = 2"),
    ] {
        mssql_cdc_exec(&sql);
    }
    wait_for_capture(&ci, 3);
    rig.run_ok();

    let parts = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect::<Vec<_>>();
    let (names, cells) = names_and_cells(&parts);
    assert!(
        !names.iter().any(|n| n.eq_ignore_ascii_case("pass")),
        "`pass` is a column of the delivered parts: {names:?}"
    );
    assert!(
        !cells.iter().any(|v| v.contains(SECRET)),
        "a hidden value was delivered: {cells:?}"
    );
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
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_column_grant_login_is_refused_for_a_capture_wider_than_its_grant_or_a_missing_key_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_wide");
    let ci = format!("dbo_{table}");
    capture(&table, &ci, None);
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::new(&table, "id, name");

    let out = Rig::mssql_cdc(&table, &ci)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants")
        .run();
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_PREREQUISITE", 1));
    let said = said_by(&out);
    assert!(
        said.contains("`pass`") && said.contains("@captured_column_list"),
        "the over-capture names its forbidden column and the remedy:\n{said}"
    );

    let ci2 = format!("{ci}_ro");
    capture(&table, &ci2, Some("id, name"));
    let _second = MssqlCdcTable {
        table: table.clone(),
        ci: ci2.clone(),
    };
    let rig = Rig::mssql_cdc(&table, &ci2)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    rig.run_ok();
    assert_no_hidden(
        &parquet_dirs(&rig.out_dir())
            .iter()
            .flat_map(|d| read_all_parts(d))
            .collect::<Vec<_>>(),
        "the remedy",
    );

    let keyless = GrantLogin::new(&table, "name");
    let out = Rig::mssql_cdc(&table, &ci2)
        .source_url(&keyless.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants")
        .run();
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
    assert!(
        said_by(&out).contains("`id`"),
        "an unreadable key is named:\n{}",
        said_by(&out)
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_column_grant_login_reads_only_its_columns_in_every_batch_mode_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_batch");
    let ci = format!("dbo_{table}");
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::new(&table, "id, name");
    for (mode, lines) in [
        ("full", &[][..]),
        ("chunked", &["chunk_by_key: id", "chunk_size: 1"][..]),
        ("incremental", &["cursor_column: id"][..]),
    ] {
        let rig = Rig::mssql_cdc(&table, &ci)
            .source_url(&user.url())
            .source_line("columns_from: grants")
            .into_full_batch()
            .restage(mode, lines);
        let check = rig.cli(&["check"]);
        assert!(
            check.status.success(),
            "{mode}: check under a column grant:\n{}",
            said_by(&check)
        );
        rig.run_ok();
        let cells = assert_no_hidden(
            &parquet_dirs(&rig.out_dir())
                .iter()
                .flat_map(|d| read_all_parts(d))
                .collect::<Vec<_>>(),
            mode,
        );
        for name in ["ann", "bob", "cid"] {
            assert!(
                cells.iter().any(|v| v == name),
                "{mode}: every row is read: {cells:?}"
            );
        }
    }
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_login_that_may_read_every_column_delivers_every_column_under_the_policy_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_whole");
    let ci = format!("dbo_{table}");
    capture(&table, &ci, None);
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::whole_table(&table);
    let rig = Rig::mssql_cdc(&table, &ci)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    rig.run_ok();
    mssql_cdc_exec(&format!(
        "UPDATE dbo.{table} SET pass = '{SECRET}u' WHERE id = 1"
    ));
    wait_for_capture(&ci, 1);
    rig.run_ok();
    let (names, cells) = names_and_cells(
        &parquet_dirs(&rig.out_dir())
            .iter()
            .flat_map(|d| read_all_parts(d))
            .collect::<Vec<_>>(),
    );
    assert!(
        names.iter().any(|n| n == "pass") && cells.iter().any(|v| v == &format!("{SECRET}u")),
        "a login that may read `pass` is delivered it: {names:?} {cells:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_column_revoked_between_runs_is_refused_by_the_next_run_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_rev");
    let ci = format!("dbo_{table}");
    capture(&table, &ci, Some("id, name"));
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::new(&table, "id, name");
    let rig = Rig::mssql_cdc(&table, &ci)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    rig.run_ok();

    mssql_cdc_exec(&format!(
        "REVOKE SELECT ON dbo.{table} (name) FROM [{}];",
        user.0
    ));
    mssql_cdc_exec(&format!(
        "UPDATE dbo.{table} SET name = 'ann2' WHERE id = 1"
    ));
    wait_for_capture(&ci, 1);
    let out = rig.run();
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_PREREQUISITE", 1));
    let (_, cells) = names_and_cells(
        &parquet_dirs(&rig.out_dir())
            .iter()
            .flat_map(|d| read_all_parts(d))
            .collect::<Vec<_>>(),
    );
    assert!(
        !cells.iter().any(|v| v == "ann2"),
        "a column revoked since the last run is not delivered: {cells:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_column_grant_login_without_the_policy_is_refused_by_the_server_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_nopol");
    let ci = format!("dbo_{table}");
    capture(&table, &ci, Some("id, name"));
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::new(&table, "id, name");
    let out = Rig::mssql_cdc(&table, &ci)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .run();
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_QUERY_FAILED", 1));
    assert!(
        said_by(&out).contains("code: 230"),
        "without `columns_from: grants` the login's `SELECT *` is refused by the server:\n{}",
        said_by(&out)
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_partial_capture_without_the_policy_is_refused_as_before_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_part");
    let ci = format!("dbo_{table}");
    capture(&table, &ci, Some("id, name"));
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let out = Rig::mssql_cdc(&table, &ci).run();
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_PREREQUISITE", 1));
    assert!(
        said_by(&out).contains("do not carry every column of their table")
            && said_by(&out).contains("(2 of 3 columns)"),
        "the refusal of a partial capture is unchanged:\n{}",
        said_by(&out)
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn the_policy_says_what_it_does_not_guarantee_at_check_and_at_the_start_of_a_run_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_say");
    let ci = format!("dbo_{table}");
    capture(&table, &ci, Some("id, name"));
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::new(&table, "id, name");
    let on = Rig::mssql_cdc(&table, &ci)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    let check = on.cli(&["check"]);
    assert!(check.status.success(), "{}", said_by(&check));
    assert!(
        String::from_utf8_lossy(&check.stdout).contains(BOUNDARY_TEXT),
        "`rivet check` states the boundary of the policy:\n{}",
        said_by(&check)
    );
    let run = on.run_args_env(&["--export", &table], &[("RUST_LOG", "info")]);
    assert!(run.status.success(), "{}", said_by(&run));
    assert!(
        said_by(&run).contains(BOUNDARY_TEXT),
        "a change-stream run says it at its start:\n{}",
        said_by(&run)
    );
    let off = Rig::mssql_cdc(&table, &ci).cdc("initial: snapshot");
    assert!(
        !said_by(&off.cli(&["check"])).contains(BOUNDARY_TEXT),
        "a config without the policy says nothing about it"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_hidden_cursor_or_chunk_column_is_refused_by_code_before_the_read_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_cur");
    let ci = format!("dbo_{table}");
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::new(&table, "id, name");
    for (what, mode, lines) in [
        ("cursor_column", "incremental", &["cursor_column: pass"][..]),
        (
            "chunk_by_key",
            "chunked",
            &["chunk_by_key: pass", "chunk_size: 1"][..],
        ),
    ] {
        let rig = Rig::mssql_cdc(&table, &ci)
            .source_url(&user.url())
            .source_line("columns_from: grants")
            .into_full_batch()
            .restage(mode, lines);
        let out = rig.run();
        let said = said_by(&out);
        assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
        assert!(
            said.contains("`pass`") && said.contains(what),
            "{what}: the refusal names the column and the setting:\n{said}"
        );
        assert_refused_before_any_write(&rig.out_dir(), &rig.checkpoint());
    }
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_denied_connect_under_the_policy_carries_the_connect_hint_in_every_batch_mode_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_auth");
    let ci = format!("dbo_{table}");
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::new(&table, "id, name");
    let denied = user.url().replace(&format!(":{PW}@"), ":wrong@");
    for (mode, lines) in [
        ("full", &[][..]),
        ("chunked", &["chunk_by_key: id", "chunk_size: 1"][..]),
    ] {
        let rig = Rig::mssql_cdc(&table, &ci)
            .source_url(&denied)
            .source_line("columns_from: grants")
            .into_full_batch()
            .restage(mode, lines);
        for cmd in ["run", "check"] {
            let out = rig.cli(&[cmd]);
            assert!(
                !out.status.success(),
                "{mode} {cmd}: a wrong password fails"
            );
            assert!(
                said_by(&out).contains("Verify the SQL login/password"),
                "{mode} {cmd}: the denied connect carries its hint:\n{}",
                said_by(&out)
            );
        }
    }
}

/// Wait until the capture job has written `inserts` inserts of `ci` to its change table.
fn wait_for_inserts_captured(ci: &str, inserts: i64) {
    for _ in 0..120 {
        if mssql_cdc_query_i64(&format!(
            "SELECT COUNT(*) FROM cdc.{ci}_CT WHERE __$operation = 2"
        )) >= inserts
        {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    panic!("capture job did not write an insert to cdc.{ci}_CT in 60s");
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_revoke_during_a_run_is_refused_by_code_and_delivers_nothing_of_the_column_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_mid");
    let ci = format!("dbo_{table}");
    capture(&table, &ci, Some("id, name"));
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::new(&table, "id, name");
    let rig = Rig::mssql_cdc(&table, &ci)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    rig.run_ok();
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (4, 'dee', '{SECRET}ins')"
    ));
    wait_for_inserts_captured(&ci, 1);
    let out = crate::live_cdc_column_grants::run_parked_at(&rig, "cdc_after_open", 3000, || {
        mssql_cdc_exec(&format!(
            "REVOKE SELECT ON dbo.{table} (name) FROM [{}];",
            user.0
        ));
    });
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_GRANTS_CHANGED", 2));
    assert!(
        said_by(&out).contains("lost `name`"),
        "the refusal names the lost column:\n{}",
        said_by(&out)
    );
    let parts = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect::<Vec<_>>();
    assert_no_hidden(&parts, "after the refusal");
    assert!(
        changes(&parts).is_empty(),
        "nothing of the refused batch was delivered: {:?}",
        changes(&parts)
    );
    let again = rig.run();
    assert_refused(&again, Refused::by_code("RIVET_SOURCE_CDC_PREREQUISITE", 1));
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn an_update_of_hidden_columns_only_delivers_no_row_and_the_next_change_is_delivered_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_hid");
    let ci = format!("dbo_{table}");
    capture(&table, &ci, Some("id, name"));
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::new(&table, "id, name");
    let rig = Rig::mssql_cdc(&table, &ci)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    rig.run_ok();
    mssql_cdc_exec(&format!(
        "UPDATE dbo.{table} SET pass = '{SECRET}moved' WHERE id = 1"
    ));
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (5, 'eve', '{SECRET}ins')"
    ));
    wait_for_inserts_captured(&ci, 1);
    rig.run_ok();
    let parts = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect::<Vec<_>>();
    assert_no_hidden(&parts, "after a hidden-only update");
    assert_eq!(
        changes(&parts),
        vec![("insert".into(), 5, "eve".into())],
        "a change of hidden columns only is no change, and the next change is delivered"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_revoke_between_two_batches_of_a_run_is_refused_by_code_and_the_second_batch_stays_unread_mssql()
 {
    let _serial = cross_process_serial("mssql_cdc");
    let table = seeded_table("cg_two");
    let ci = format!("dbo_{table}");
    capture(&table, &ci, Some("id, name"));
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let user = GrantLogin::new(&table, "id, name");
    let rig = Rig::mssql_cdc(&table, &ci)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .cdc("rollover: 1")
        .source_line("columns_from: grants")
        .a_failed_run_may_leave(
            &[
                Leftover::OrphanPart,
                Leftover::FileLog,
                Leftover::CdcFlush,
                Leftover::CdcCheckpoint,
            ],
            "the stop comes at the grant change: the batch committed before it stays, and the checkpoint follows it",
        );
    rig.run_ok();
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (4, 'dee', '{SECRET}a')"
    ));
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (5, 'eve', '{SECRET}b')"
    ));
    wait_for_inserts_captured(&ci, 2);
    let out = crate::live_cdc_column_grants::run_parked_at(
        &rig,
        "cdc_after_parts_before_ack",
        8000,
        || {
            mssql_cdc_exec(&format!(
                "REVOKE SELECT ON dbo.{table} (name) FROM [{}];",
                user.0
            ));
        },
    );
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_GRANTS_CHANGED", 2));
    assert!(
        said_by(&out).contains("lost `name`"),
        "the refusal names the lost column:\n{}",
        said_by(&out)
    );
    let parts = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect::<Vec<_>>();
    assert_no_hidden(&parts, "after the refusal");
    assert_eq!(
        changes(&parts),
        vec![("insert".into(), 4, "dee".into())],
        "the batch read before the revoke stays and the batch after it is not read"
    );
}
