//! Live `source.columns_from: grants` (MySQL): a column-grant account delivers only
//! the columns it may SELECT — by name and by value, in batch reads, baselines and the stream.

use std::path::Path;

use crate::common::*;
use mysql::prelude::Queryable as _;

const SECRET: &str = "SECRET-";

fn root_url() -> String {
    MYSQL_CDC_URL.replace("rivet:rivet@", "root:rivet@")
}

fn root() -> mysql::Conn {
    mysql::Conn::new(mysql::Opts::from_url(&root_url()).unwrap()).expect("connect as root")
}

/// A replication account that may SELECT only `columns` of `table`.
struct GrantUser(String);

impl GrantUser {
    fn new(table: &str, columns: &str) -> Self {
        let user = Self::select_only(table, columns);
        root()
            .query_drop(format!(
                "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'",
                user.0
            ))
            .unwrap();
        user
    }

    /// An account that may SELECT only `columns` of `table`, with no replication grant.
    fn select_only(table: &str, columns: &str) -> Self {
        let user = unique_name("cg_u");
        let mut c = root();
        c.query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
            .unwrap();
        c.query_drop(format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'"))
            .unwrap();
        c.query_drop(format!(
            "GRANT SELECT ({columns}) ON rivet.{table} TO '{user}'@'%'"
        ))
        .unwrap();
        Self(user)
    }

    fn url(&self) -> String {
        MYSQL_CDC_URL.replace("rivet:rivet@", &format!("{}:pw@", self.0))
    }
}

impl Drop for GrantUser {
    fn drop(&mut self) {
        let _ = root().query_drop(format!("DROP USER IF EXISTS '{}'@'%'", self.0));
    }
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

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_grant_account_delivers_no_hidden_column_by_name_or_value_mysql() {
    let table = unique_name("cg");
    let _table = MysqlCdcTable(table.clone());
    let mut c = root();
    c.query_drop(format!(
        "CREATE TABLE rivet.{table} (id INT PRIMARY KEY, name VARCHAR(20), pass VARCHAR(20))"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO rivet.{table} VALUES (1, 'ann', '{SECRET}snap')"
    ))
    .unwrap();
    let user = GrantUser::new(&table, "id, name");
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");

    let check = rig.cli(&["check"]);
    assert!(
        check.status.success(),
        "check under a column grant:\n{}",
        String::from_utf8_lossy(&check.stderr)
    );
    rig.run_ok();
    for sql in [
        format!("UPDATE rivet.{table} SET pass = '{SECRET}a' WHERE id = 1"),
        format!("UPDATE rivet.{table} SET pass = '{SECRET}b', name = 'ann2' WHERE id = 1"),
        format!("INSERT INTO rivet.{table} VALUES (2, 'bob', '{SECRET}ins')"),
        format!("ALTER TABLE rivet.{table} ADD COLUMN ssn VARCHAR(20)"),
        format!("INSERT INTO rivet.{table} VALUES (3, 'cid', '{SECRET}c', '{SECRET}ssn')"),
        format!("UPDATE rivet.{table} SET ssn = '{SECRET}ssn2' WHERE id = 3"),
        format!("DELETE FROM rivet.{table} WHERE id = 2"),
    ] {
        c.query_drop(sql).unwrap();
    }
    rig.run_ok();

    let parts: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    let (names, cells) = names_and_cells(&parts);
    for hidden in ["pass", "ssn"] {
        assert!(
            !names.iter().any(|n| n.eq_ignore_ascii_case(hidden)),
            "`{hidden}` is a column of the delivered parts: {names:?}"
        );
    }
    assert!(
        !cells.iter().any(|v| v.contains(SECRET)),
        "a hidden value was delivered: {cells:?}"
    );
    assert!(
        cells.iter().any(|v| v == "ann") && cells.iter().any(|v| v == "ann2"),
        "the snapshot and the change parts were both read: {cells:?}"
    );
    assert!(
        names.iter().any(|n| n == "__op"),
        "the change parts were read: {names:?}"
    );
    assert_eq!(
        changes(&parts),
        vec![
            ("update".into(), 1, "ann2".into()),
            ("insert".into(), 2, "bob".into()),
            ("insert".into(), 3, "cid".into()),
            ("delete".into(), 2, "bob".into()),
        ],
        "an update of hidden columns only is not delivered; every other change is"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_grant_account_is_refused_without_the_policy_or_without_the_key_mysql() {
    let table = unique_name("cg_key");
    let _table = MysqlCdcTable(table.clone());
    root()
        .query_drop(format!(
            "CREATE TABLE rivet.{table} (id INT PRIMARY KEY, name VARCHAR(20), pass VARCHAR(20))"
        ))
        .unwrap();
    root()
        .query_drop(format!("INSERT INTO rivet.{table} VALUES (1, 'ann', 'p')"))
        .unwrap();

    let readable = GrantUser::new(&table, "id, name");
    let said = Rig::mysql_cdc(&table)
        .source_url(&readable.url())
        .cdc("initial: snapshot")
        .run_expect_fail();
    assert!(
        said.contains("1142"),
        "without `columns_from: grants` the account's `SELECT *` is refused by the server:\n{said}"
    );

    let keyless = GrantUser::new(&table, "name");
    let said = Rig::mysql_cdc(&table)
        .source_url(&keyless.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants")
        .run_expect_fail();
    assert!(
        said.contains("RIVET_SOURCE_COLUMNS_UNREADABLE") && said.contains("`id`"),
        "an unreadable key is refused by code and named:\n{said}"
    );
}

/// `table` holding `(1,'ann',SECRET)`, `(2,'bob',SECRET)`, `(3,'cid',SECRET)`, dropped on Drop.
fn seeded(label: &str) -> (String, MysqlCdcTable) {
    let table = unique_name(label);
    let guard = MysqlCdcTable(table.clone());
    let mut c = root();
    c.query_drop(format!(
        "CREATE TABLE rivet.{table} (id INT PRIMARY KEY, name VARCHAR(20), pass VARCHAR(20))"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO rivet.{table} VALUES (1, 'ann', '{SECRET}1'), (2, 'bob', '{SECRET}2'), \
         (3, 'cid', '{SECRET}3')"
    ))
    .unwrap();
    (table, guard)
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

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_grant_account_reads_only_its_columns_in_every_batch_mode_mysql() {
    let (table, _table) = seeded("cg_batch");
    let user = GrantUser::select_only(&table, "id, name");
    for (mode, lines) in [
        ("full", &[][..]),
        ("chunked", &["chunk_by_key: id", "chunk_size: 1"][..]),
        ("incremental", &["cursor_column: id"][..]),
    ] {
        let rig = Rig::mysql_cdc(&table)
            .source_url(&user.url())
            .source_line("columns_from: grants")
            .into_full_batch()
            .restage(mode, lines);
        let check = rig.cli(&["check"]);
        assert!(
            check.status.success(),
            "{mode}: check under a column grant:\n{}",
            String::from_utf8_lossy(&check.stderr)
        );
        rig.run_ok();
        let parts: Vec<_> = parquet_dirs(&rig.out_dir())
            .iter()
            .flat_map(|d| read_all_parts(d))
            .collect();
        let cells = assert_no_hidden(&parts, mode);
        for name in ["ann", "bob", "cid"] {
            assert!(
                cells.iter().any(|v| v == name),
                "{mode}: every row is read: {cells:?}"
            );
        }
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_grant_account_backfills_through_its_recipes_without_a_hidden_column_mysql() {
    let (table, _table) = seeded("cg_bf");
    let user = GrantUser::new(&table, "id, name");
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .cdc("backfill: auto")
        .also_batch_export("baseline", &table, "chunked")
        .also_export_line("chunk_by_key: id")
        .also_export_line("chunk_size: 1");
    rig.run_ok();
    root()
        .query_drop(format!(
            "UPDATE rivet.{table} SET name = 'ann2', pass = '{SECRET}u' WHERE id = 1"
        ))
        .unwrap();
    rig.run_ok();

    let parts: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    let cells = assert_no_hidden(&parts, "backfill");
    for name in ["ann", "bob", "cid", "ann2"] {
        assert!(
            cells.iter().any(|v| v == name),
            "the baseline and the change are both delivered: {cells:?}"
        );
    }
}

/// An open continuous run, a grant change written as `revoke(table, user)`, a row after it.
fn a_revoke_stops_an_open_run_before_the_column_leaves(
    label: &str,
    revoke: impl Fn(&str, &str) -> String,
) {
    let (table, _table) = seeded(label);
    let user = GrantUser::new(&table, "id, name, pass");
    let mut rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .cdc("initial: snapshot")
        .cdc("rollover: 3")
        .cdc("max_events: 4")
        .a_failed_run_may_leave(
            &[
                Leftover::OrphanPart,
                Leftover::FileLog,
                Leftover::CdcFlush,
                Leftover::CdcCheckpoint,
            ],
            "the stop comes at the grant change: the span committed before it stays, and the checkpoint follows it",
        );
    rig.run_ok();
    let mut c = root();
    c.query_drop(format!(
        "INSERT INTO rivet.{table} VALUES (4, 'dan', 'open'), (5, 'eli', 'open'), (6, 'fay', 'open')"
    ))
    .unwrap();
    rig.replace_cdc_line("until_current", "until_current: false");
    let marker = rig.out_dir().join("..").join("revoke.paused");
    let marker_env = marker.display().to_string();
    let mut child = rig.spawn_args_env(
        &[],
        &[
            ("RIVET_TEST_PAUSE_AT", "cdc_after_parts_before_ack:20000"),
            ("RIVET_TEST_PAUSE_MARKER", &marker_env),
        ],
    );
    let opened = std::time::Instant::now();
    while !marker.is_file() {
        if let Ok(Some(_)) = child.try_wait() {
            let out = child.wait_with_output().unwrap();
            panic!(
                "the open run exited before it parked:\n{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        if opened.elapsed() > std::time::Duration::from_secs(30) {
            let _ = child.kill();
            let out = child.wait_with_output().unwrap();
            panic!(
                "the open run never parked after its first part:\n{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    c.query_drop(revoke(&table, &user.0)).unwrap();
    c.query_drop(format!(
        "INSERT INTO rivet.{table} VALUES (7, 'eve', 'REVOKED-7')"
    ))
    .unwrap();
    let out = child.wait_with_output().expect("reap the open run");
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.code() == Some(2) && said.contains("RIVET_SOURCE_CDC_GRANTS_CHANGED"),
        "a revoke inside the open run stops it by code, exit 2:\n{said}"
    );

    rig.replace_cdc_line("until_current", "until_current: true");
    rig.run_ok();
    let parts: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    let (_, cells) = names_and_cells(&parts);
    assert!(
        !cells.iter().any(|v| v.contains("REVOKED-")),
        "a value written after the revoke was delivered: {cells:?}"
    );
    assert!(
        cells.iter().any(|v| v == "eve") && cells.iter().any(|v| v == "dan"),
        "the rows on both sides of the revoke are delivered: {cells:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_revoke_during_a_continuous_run_stops_it_before_the_column_leaves_mysql() {
    a_revoke_stops_an_open_run_before_the_column_leaves("cg_rev", |table, user| {
        format!("REVOKE SELECT (pass) ON rivet.{table} FROM '{user}'@'%'")
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_revoke_written_as_an_executable_comment_stops_an_open_run_too_mysql() {
    a_revoke_stops_an_open_run_before_the_column_leaves("cg_revx", |table, user| {
        format!("/*!80000 REVOKE SELECT (pass) ON rivet.{table} FROM '{user}'@'%' */")
    });
}

/// Column names of every part under `rig`'s output, in part order, without rivet's own columns.
fn delivered_columns(rig: &Rig) -> Vec<Vec<String>> {
    parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .filter(|n| !n.starts_with("__"))
                .collect()
        })
        .collect()
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_full_grant_account_delivers_the_columns_select_star_does_mysql() {
    let table = unique_name("cg_inv");
    let _table = MysqlCdcTable(table.clone());
    let mut c = root();
    c.query_drop(format!(
        "CREATE TABLE rivet.{table} (id INT PRIMARY KEY, inv VARCHAR(20) INVISIBLE, \
         name VARCHAR(20), gen VARCHAR(40) AS (CONCAT(name, '-g')) VIRTUAL)"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO rivet.{table} (id, name, inv) VALUES (1, 'ann', 'INV-1')"
    ))
    .unwrap();
    let user = GrantUser::new(&table, "id, name, inv, gen");
    let want = ["id", "name", "gen"].map(String::from).to_vec();

    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .into_full_batch()
        .restage("full", &[]);
    rig.run_ok();
    assert_eq!(delivered_columns(&rig), vec![want.clone()], "batch");

    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .cdc("initial: snapshot");
    rig.run_ok();
    c.query_drop(format!(
        "INSERT INTO rivet.{table} (id, name, inv) VALUES (2, 'bob', 'INV-2')"
    ))
    .unwrap();
    rig.run_ok();
    let parts = delivered_columns(&rig);
    assert!(parts.len() >= 2, "a snapshot and a change part: {parts:?}");
    for part in parts {
        assert_eq!(part, want, "cdc");
    }
    let all: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    let (_, cells) = names_and_cells(&all);
    for value in ["ann-g", "bob-g"] {
        assert!(
            cells.iter().any(|v| v == value),
            "each value stays under its own column: {cells:?}"
        );
    }
    assert!(!cells.iter().any(|v| v.starts_with("INV-")), "{cells:?}");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_denied_connect_under_the_policy_carries_the_connect_hint_in_every_batch_mode_mysql() {
    let (table, _table) = seeded("cg_auth");
    let user = GrantUser::select_only(&table, "id, name");
    let denied = user.url().replace(":pw@", ":wrong@");
    for (mode, lines) in [
        ("full", &[][..]),
        ("chunked", &["chunk_by_key: id", "chunk_size: 1"][..]),
    ] {
        let rig = Rig::mysql_cdc(&table)
            .source_url(&denied)
            .source_line("columns_from: grants")
            .into_full_batch()
            .restage(mode, lines);
        for cmd in ["run", "check"] {
            let out = rig.cli(&[cmd]);
            let err = String::from_utf8_lossy(&out.stderr);
            assert!(
                !out.status.success(),
                "{mode} {cmd}: a wrong password fails"
            );
            assert!(
                err.contains("Verify the user/password"),
                "{mode} {cmd}: the denied connect carries its hint:\n{err}"
            );
        }
    }
}

/// A table `(id, bio TEXT, name, pass)` holding three rows, dropped on Drop.
fn seeded_with_blob(label: &str) -> (String, MysqlCdcTable) {
    let table = unique_name(label);
    let guard = MysqlCdcTable(table.clone());
    let mut c = root();
    c.query_drop(format!(
        "CREATE TABLE rivet.{table} (id INT PRIMARY KEY, bio TEXT, name VARCHAR(20), pass VARCHAR(20))"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO rivet.{table} VALUES (1, 'b1', 'ann', '{SECRET}1'), (2, 'b2', 'bob', '{SECRET}2'), \
         (3, 'b3', 'cid', '{SECRET}3')"
    ))
    .unwrap();
    (table, guard)
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_partial_row_image_is_refused_before_it_is_projected_mysql() {
    // `blob`: the table also holds a TEXT column before `name`, which NOBLOB leaves out of an image.
    type Case = (&'static str, &'static str, bool, fn(&str) -> String);
    let cases: [Case; 4] = [
        ("MINIMAL", "insert", false, |t| {
            format!("INSERT INTO rivet.{t} (id, pass) VALUES (9, '{SECRET}ins')")
        }),
        (
            "MINIMAL",
            "update of a visible and a hidden column",
            false,
            |t| format!("UPDATE rivet.{t} SET name = 'bob2', pass = '{SECRET}upd' WHERE id = 2"),
        ),
        ("MINIMAL", "update of a hidden column", false, |t| {
            format!("UPDATE rivet.{t} SET pass = '{SECRET}upd' WHERE id = 2")
        }),
        ("NOBLOB", "update", true, |t| {
            format!("UPDATE rivet.{t} SET name = 'bob2', pass = '{SECRET}upd' WHERE id = 2")
        }),
    ];
    for (image, what, blob, write) in cases {
        let (table, _table) = if blob {
            seeded_with_blob("cg_part")
        } else {
            seeded("cg_part")
        };
        let user = GrantUser::new(&table, "id, name");
        let rig = Rig::mysql_cdc(&table)
            .source_url(&user.url())
            .cdc("initial: snapshot")
            .source_line("columns_from: grants");
        rig.run_ok();
        let mut writer = root();
        writer
            .query_drop(format!("SET SESSION binlog_row_image = '{image}'"))
            .unwrap();
        writer.query_drop(write(&table)).unwrap();
        let out = rig.run();
        assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_PREREQUISITE", 1));
        let said = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            said.contains("a PARTIAL image") && said.contains("binlog_row_image = FULL"),
            "{image} {what}: the refusal names the cause and the remedy:\n{said}"
        );
        let parts: Vec<_> = parquet_dirs(&rig.out_dir())
            .iter()
            .flat_map(|d| read_all_parts(d))
            .collect();
        assert_no_hidden(&parts, &format!("{image} {what}"));
        assert_eq!(
            changes(&parts),
            vec![],
            "{image} {what}: a refused image delivers no change, and never overwrites a key"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_partial_image_of_an_uncaptured_table_and_a_full_one_of_a_captured_table_are_delivered_as_before_mysql()
 {
    let (table, _table) = seeded("cg_nb");
    let (other, _other) = seeded("cg_nb_other");
    let user = GrantUser::new(&table, "id, name");
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    rig.run_ok();
    let mut writer = root();
    writer
        .query_drop("SET SESSION binlog_row_image = 'MINIMAL'")
        .unwrap();
    writer
        .query_drop(format!(
            "UPDATE rivet.{other} SET pass = '{SECRET}other' WHERE id = 1"
        ))
        .unwrap();
    writer
        .query_drop("SET SESSION binlog_row_image = 'FULL'")
        .unwrap();
    writer
        .query_drop(format!(
            "UPDATE rivet.{table} SET name = 'ann2', pass = '{SECRET}full' WHERE id = 1"
        ))
        .unwrap();
    rig.run_ok();
    let parts: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    assert_no_hidden(&parts, "uncaptured partial image");
    assert_eq!(changes(&parts), vec![("update".into(), 1, "ann2".into())]);
}

/// A database `name`, dropped on Drop.
struct OtherDb(String);

impl Drop for OtherDb {
    fn drop(&mut self) {
        let _ = root().query_drop(format!("DROP DATABASE IF EXISTS `{}`", self.0));
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_qualified_table_does_not_capture_a_table_named_by_its_qualified_string_in_another_database_mysql()
 {
    let table = unique_name("cg_q");
    let _table = MysqlCdcTable(table.clone());
    let other = OtherDb(unique_name("cg_qdb"));
    let mut c = root();
    c.query_drop(format!(
        "CREATE TABLE rivet.{table} (id INT PRIMARY KEY, name VARCHAR(20))"
    ))
    .unwrap();
    c.query_drop(format!("CREATE DATABASE `{}`", other.0))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE `{}`.`rivet.{table}` (id INT PRIMARY KEY, name VARCHAR(20))",
        other.0
    ))
    .unwrap();
    c.query_drop(format!("INSERT INTO rivet.{table} VALUES (1, 'ann')"))
        .unwrap();
    let rig = Rig::mysql_cdc(&format!("rivet.{table}")).cdc("initial: snapshot");
    rig.run_ok();
    c.query_drop(format!(
        "INSERT INTO `{}`.`rivet.{table}` VALUES (50, 'FOREIGN-ROW')",
        other.0
    ))
    .unwrap();
    c.query_drop(format!("INSERT INTO rivet.{table} VALUES (2, 'bob')"))
        .unwrap();
    rig.run_ok();
    let parts: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    let (_, cells) = names_and_cells(&parts);
    assert!(
        !cells.iter().any(|v| v.contains("FOREIGN")),
        "a row of another database's table reached the export: {cells:?}"
    );
    assert_eq!(changes(&parts), vec![("insert".into(), 2, "bob".into())]);
}

/// Every command that loads the config refuses `columns_from: grants` off MySQL, by code, before it
/// connects or writes: a PostgreSQL, SQL Server, Oracle or MongoDB source would deliver every column.
fn columns_from_grants_is_refused_by_every_command(rigs: Vec<Rig>) {
    for rig in rigs {
        let rig = rig.source_line("columns_from: grants");
        for cmd in [
            &["check"][..],
            &["run"],
            &["plan"],
            &["doctor"],
            &["validate"],
            &["load"],
            &["state", "show"],
        ] {
            let out = rig.cli(cmd);
            assert_refused(
                &out,
                Refused::by_code("RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED", 1),
            );
            let said = format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            assert!(
                said.contains("`source.columns_from: grants` is supported on MySQL sources only")
                    && said.contains("would read and deliver every column")
                    && said.contains("Remove `columns_from:` from `source:`"),
                "{cmd:?}: the refusal names the cause and the remedy:\n{said}"
            );
        }
        assert!(
            parquet_dirs(&rig.out_dir()).is_empty(),
            "a refused config writes nothing"
        );
    }
}

#[test]
#[ignore = "live: needs only the rig harness env; the refusal comes before any connection"]
fn columns_from_grants_is_refused_by_every_command_postgres() {
    columns_from_grants_is_refused_by_every_command(vec![
        Rig::pg_batch("orders"),
        Rig::pg_cdc("orders", "cg_slot"),
    ]);
}

#[test]
#[ignore = "live: needs only the rig harness env; the refusal comes before any connection"]
fn columns_from_grants_is_refused_by_every_command_mssql() {
    columns_from_grants_is_refused_by_every_command(vec![
        Rig::mssql_batch("orders"),
        Rig::mssql_cdc("orders", "dbo_orders"),
    ]);
}

#[test]
#[ignore = "live: needs only the rig harness env; the refusal comes before any connection"]
fn columns_from_grants_is_refused_by_every_command_oracle() {
    columns_from_grants_is_refused_by_every_command(vec![
        Rig::oracle_batch("orders"),
        Rig::oracle_cdc("orders"),
    ]);
}

#[test]
#[ignore = "live: needs only the rig harness env; the refusal comes before any connection"]
fn columns_from_grants_is_refused_by_every_command_mongo() {
    columns_from_grants_is_refused_by_every_command(vec![
        Rig::mongo_batch("orders").source_url(&MongoTest::url(27018, "cg_refused")),
        Rig::mongo_cdc("orders").source_url(&MongoTest::url(27018, "cg_refused")),
    ]);
}
