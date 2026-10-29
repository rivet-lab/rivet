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

    /// An account that may SELECT the whole of `table` (every column, by table privilege), plus replication.
    fn whole_table(table: &str) -> Self {
        let user = unique_name("cg_u");
        let mut c = root();
        c.query_drop(format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'"))
            .unwrap();
        c.query_drop(format!("GRANT SELECT ON rivet.{table} TO '{user}'@'%'"))
            .unwrap();
        c.query_drop(format!(
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%'"
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

/// A continuous run parked after its first part, with what `act` did to the source meanwhile.
struct OpenRun {
    user: GrantUser,
    rig: Rig,
    /// What the run said when it ended (after `act` and the rows it wrote).
    out: std::process::Output,
    _table: MysqlCdcTable,
}

impl OpenRun {
    fn said(&self) -> String {
        format!(
            "{}{}",
            String::from_utf8_lossy(&self.out.stdout),
            String::from_utf8_lossy(&self.out.stderr)
        )
    }
}

/// Park `rig`'s next run at `point` for `ms`, call `during` once the run is parked, and reap the run.
fn run_parked_at(rig: &Rig, point: &str, ms: u64, during: impl FnOnce()) -> std::process::Output {
    let marker = rig.out_dir().join("..").join(format!("{point}.paused"));
    let marker_env = marker.display().to_string();
    let at = format!("{point}:{ms}");
    let mut child = rig.spawn_args_env(
        &[],
        &[
            ("RIVET_TEST_PAUSE_AT", &at),
            ("RIVET_TEST_PAUSE_MARKER", &marker_env),
        ],
    );
    let opened = std::time::Instant::now();
    while !marker.is_file() {
        if let Ok(Some(_)) = child.try_wait() {
            let out = child.wait_with_output().unwrap();
            panic!(
                "the run exited before it parked at {point}:\n{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        if opened.elapsed() > std::time::Duration::from_secs(60) {
            let _ = child.kill();
            let out = child.wait_with_output().unwrap();
            panic!(
                "the run never parked at {point}:\n{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    during();
    child.wait_with_output().expect("reap the parked run")
}

/// An open continuous run over `seeded(label)` (account granted `id, name, pass`): a first part
/// is made durable, the run parks, `act` writes to the source, and the run ends at `max_events`.
fn an_open_run_with(label: &str, max_events: u32, act: impl FnOnce(&str, &GrantUser)) -> OpenRun {
    an_open_run_as(label, max_events, false, act)
}

/// As [`an_open_run_with`]; `whole_table` makes the account hold SELECT on the whole table.
fn an_open_run_as(
    label: &str,
    max_events: u32,
    whole_table: bool,
    act: impl FnOnce(&str, &GrantUser),
) -> OpenRun {
    let (table, _table) = seeded(label);
    let user = if whole_table {
        GrantUser::whole_table(&table)
    } else {
        GrantUser::new(&table, "id, name, pass")
    };
    let mut rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .cdc("initial: snapshot")
        .cdc("rollover: 3")
        .cdc(&format!("max_events: {max_events}"))
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
    root()
        .query_drop(format!(
            "INSERT INTO rivet.{table} VALUES (4, 'dan', 'open'), (5, 'eli', 'open'), (6, 'fay', 'open')"
        ))
        .unwrap();
    rig.replace_cdc_line("until_current", "until_current: false");
    let out = run_parked_at(&rig, "cdc_after_parts_before_ack", 20000, || {
        act(&table, &user)
    });
    OpenRun {
        user,
        rig,
        out,
        _table,
    }
}

/// An open continuous run, a grant change written as `revoke(table, user)`, a row after it.
fn a_revoke_stops_an_open_run_before_the_column_leaves(
    label: &str,
    revoke: impl Fn(&str, &str) -> String,
) {
    let mut run = an_open_run_with(label, 4, |table, user| {
        let mut c = root();
        c.query_drop(revoke(table, &user.0)).unwrap();
        c.query_drop(format!(
            "INSERT INTO rivet.{table} VALUES (7, 'eve', 'REVOKED-7')"
        ))
        .unwrap();
    });
    let said = run.said();
    assert!(
        run.out.status.code() == Some(2) && said.contains("RIVET_SOURCE_CDC_GRANTS_CHANGED"),
        "a revoke inside the open run stops it by code, exit 2:\n{said}"
    );

    run.rig
        .replace_cdc_line("until_current", "until_current: true");
    run.rig.run_ok();
    let parts: Vec<_> = parquet_dirs(&run.rig.out_dir())
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

/// The binlog `(file, position)` at the end of the LAST commit after `start` that wrote `table`; the oracle for "the checkpoint reached this transaction".
fn commit_end_after(start: &(String, u64), table: &str) -> (String, u64) {
    let mut c = root();
    let events: Vec<(u64, String, u64, String)> = c
        .query_map(
            format!("SHOW BINLOG EVENTS IN '{}' FROM {}", start.0, start.1),
            |r: mysql::Row| {
                (
                    r.get::<u64, _>(1).unwrap_or(0),
                    r.get::<String, _>(2).unwrap_or_default(),
                    r.get::<u64, _>(4).unwrap_or(0),
                    r.get::<String, _>(5).unwrap_or_default(),
                )
            },
        )
        .unwrap();
    let (mut wrote, mut last) = (false, None);
    for (_, kind, end, info) in events {
        wrote |= kind == "Table_map" && info.contains(&format!("(rivet.{table})"));
        if kind == "Xid" {
            if wrote {
                last = Some((start.0.clone(), end));
            }
            wrote = false;
        }
    }
    last.unwrap_or_else(|| panic!("no commit that wrote `{table}` after {start:?}"))
}

/// The server's binlog `(file, position)` now.
fn binlog_now() -> (String, u64) {
    let mut c = root();
    let row: mysql::Row = c
        .query_first("SHOW BINARY LOG STATUS")
        .or_else(|_| c.query_first("SHOW MASTER STATUS"))
        .expect("binlog status")
        .expect("binlog enabled");
    (row.get(0).unwrap(), row.get(1).unwrap())
}

/// The `(file, pos)` the rig's checkpoint file holds.
fn checkpoint_at(rig: &Rig) -> (String, u64) {
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(rig.checkpoint()).expect("the checkpoint"))
            .expect("checkpoint json");
    (
        json["file"].as_str().expect("file").to_string(),
        json["pos"].as_u64().expect("pos"),
    )
}

/// A run under the policy anchored and baselined, then `writes` committed one by one: the
/// checkpoint of the next run must reach the end of the last commit that wrote `watch`.
fn the_checkpoint_follows_commits_of(
    what: &str,
    watch: impl Fn(&str, &str) -> String,
    writes: impl Fn(&str, &str) -> Vec<String>,
) -> Vec<(String, i64, String)> {
    let (table, _table) = seeded("cg_adv");
    let (other, _other) = seeded("cg_adv_o");
    let user = GrantUser::new(&table, "id, name");
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    rig.run_ok();
    let start = binlog_now();
    let mut c = root();
    for sql in writes(&table, &other) {
        c.query_drop(sql).unwrap();
    }
    let want = commit_end_after(&start, &watch(&table, &other));
    rig.run_ok();
    let got = checkpoint_at(&rig);
    assert!(
        got >= want,
        "{what}: the checkpoint {got:?} stopped before the end of the last commit {want:?}"
    );
    let parts: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    assert_no_hidden(&parts, what);
    changes(&parts)
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn an_update_of_hidden_columns_only_advances_the_checkpoint_mysql() {
    let delivered = the_checkpoint_follows_commits_of(
        "hidden-only update",
        |t, _| t.to_string(),
        |t, _| {
            vec![format!(
                "UPDATE rivet.{t} SET pass = '{SECRET}h' WHERE id = 1"
            )]
        },
    );
    assert_eq!(
        delivered,
        vec![],
        "an update of hidden columns only is not delivered"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn an_update_of_an_uncaptured_table_advances_the_checkpoint_under_the_policy_mysql() {
    let delivered = the_checkpoint_follows_commits_of(
        "uncaptured update",
        |_, o| o.to_string(),
        |_, o| {
            vec![format!(
                "UPDATE rivet.{o} SET pass = '{SECRET}o', name = 'x' WHERE id = 1"
            )]
        },
    );
    assert_eq!(
        delivered,
        vec![],
        "another table's change is not this export's"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_visible_update_and_a_mixed_transaction_advance_the_checkpoint_as_before_mysql() {
    let delivered = the_checkpoint_follows_commits_of(
        "visible update",
        |t, _| t.to_string(),
        |t, o| {
            vec![
                format!("UPDATE rivet.{t} SET name = 'ann2' WHERE id = 1"),
                format!(
                    "BEGIN; UPDATE rivet.{t} SET pass = '{SECRET}m' WHERE id = 2; \
                     UPDATE rivet.{o} SET pass = '{SECRET}m' WHERE id = 2; \
                     UPDATE rivet.{t} SET name = 'bob2' WHERE id = 2; COMMIT"
                ),
            ]
        },
    );
    assert_eq!(
        delivered,
        vec![
            ("update".into(), 1, "ann2".into()),
            ("update".into(), 2, "bob2".into())
        ]
    );
}

/// The three-row `seeded` table, an account granted `id, name`, a run that anchored and baselined.
fn anchored_under_the_policy(label: &str) -> (String, MysqlCdcTable, GrantUser, Rig) {
    let (table, guard) = seeded(label);
    let user = GrantUser::new(&table, "id, name");
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    rig.run_ok();
    (table, guard, user, rig)
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_renamed_in_the_backlog_is_refused_not_silently_dropped_mysql() {
    let (table, _guard, user, rig) = anchored_under_the_policy("cg_ren");
    let mut rig = rig.a_failed_run_may_leave(
        &[Leftover::ResumePoint],
        "the run observes the readable columns at its start, and a rename leaves the old column ungranted: that observed schema is stored before the refusal at the statement (known: src/state/schema.rs stores the baseline of a failed run)",
    );
    let mut c = root();
    for sql in [
        format!("UPDATE rivet.{table} SET name = 'ann-before' WHERE id = 1"),
        format!("ALTER TABLE rivet.{table} RENAME COLUMN name TO nm"),
        format!("UPDATE rivet.{table} SET nm = 'ann-after' WHERE id = 1"),
    ] {
        c.query_drop(sql).unwrap();
    }
    let before = checkpoint_at(&rig);
    let out = rig.run();
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_UNDECODABLE", 5));
    assert!(
        said.contains(&table) && said.contains("RENAME") && said.contains("Re-baseline the stream"),
        "the refusal names the table, the statement and the remedy:\n{said}"
    );
    assert_eq!(
        checkpoint_at(&rig),
        before,
        "a refused span does not move the checkpoint"
    );
    let parts: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    assert_eq!(changes(&parts), vec![], "a refused span delivers no change");

    c.query_drop(format!(
        "GRANT SELECT (nm) ON rivet.{table} TO '{}'@'%'",
        user.0
    ))
    .unwrap();
    follow_rebaseline_remedy(&mut rig, true);
    let parts: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    let (names, cells) = names_and_cells(&parts);
    assert!(
        names.iter().any(|n| n == "nm") && cells.iter().any(|v| v == "ann-after"),
        "the re-baselined stream delivers the column under its new name: {names:?} {cells:?}"
    );
    assert_no_hidden(&parts, "after the re-baseline");
}

/// What a run delivered, as `(column names, sorted cells)` of every column that is not rivet's own.
fn delivered(rig: &Rig) -> (Vec<String>, Vec<String>) {
    delivered_under(&rig.out_dir())
}

/// The non-`__` column names and the sorted cells of every part under `dir`.
fn delivered_under(dir: &Path) -> (Vec<String>, Vec<String>) {
    let (mut names, mut cells) = (Vec::new(), Vec::new());
    for b in parquet_dirs(dir).iter().flat_map(|d| read_all_parts(d)) {
        for (field, col) in b.schema().fields().iter().zip(b.columns()) {
            if field.name().starts_with("__") {
                continue;
            }
            names.push(field.name().clone());
            for i in 0..b.num_rows() {
                cells.push(arrow::util::display::array_value_to_string(col, i).unwrap());
            }
        }
    }
    names.sort();
    names.dedup();
    cells.sort();
    (names, cells)
}

/// `(name, city)` rows `ann`, `bob` under a generated invisible primary key `my_row_id`.
fn gipk_table(label: &str) -> (String, MysqlCdcTable) {
    let table = unique_name(label);
    let guard = MysqlCdcTable(table.clone());
    let mut c = root();
    c.query_drop("SET SESSION sql_generate_invisible_primary_key = ON")
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE rivet.{table} (name VARCHAR(20), city VARCHAR(20))"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO rivet.{table} VALUES ('ann', 'rome'), ('bob', 'oslo')"
    ))
    .unwrap();
    (table, guard)
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_table_with_a_generated_invisible_key_is_read_as_without_the_policy_mysql() {
    let batch = |policy: bool| {
        let (table, _guard) = gipk_table("cg_gipkb");
        let user = GrantUser::whole_table(&table);
        let mut rig = Rig::mysql_cdc(&table)
            .source_url(&user.url())
            .into_full_batch()
            .restage("full", &[]);
        if policy {
            rig = rig.source_line("columns_from: grants");
        }
        rig.run_ok();
        let parts: Vec<_> = parquet_dirs(&rig.out_dir())
            .iter()
            .flat_map(|d| read_all_parts(d))
            .collect();
        assert_no_hidden(&parts, "gipk batch");
        delivered(&rig)
    };
    assert_eq!(
        batch(false),
        batch(true),
        "batch: the policy over an account with the table's whole SELECT delivers what `SELECT *` does"
    );
    let stream = |policy: bool| {
        let (table, _guard) = gipk_table("cg_gipks");
        let user = GrantUser::whole_table(&table);
        let mut rig = Rig::mysql_cdc(&table)
            .source_url(&user.url())
            .cdc("initial: snapshot")
            .no_oracle(
                "the oracle keys a CDC delivery by the table's primary key, which is the invisible `my_row_id` the stream does not carry with or without the policy; the cell compares the two deliveries itself",
            );
        if policy {
            rig = rig.source_line("columns_from: grants");
        }
        rig.run_ok();
        let mut w = root();
        w.query_drop(format!("INSERT INTO rivet.{table} VALUES ('cid', 'kyiv')"))
            .unwrap();
        w.query_drop(format!(
            "UPDATE rivet.{table} SET city = 'rome2' WHERE name = 'ann'"
        ))
        .unwrap();
        rig.run_ok();
        let parts: Vec<_> = parquet_dirs(&rig.out_dir())
            .iter()
            .flat_map(|d| read_all_parts(d))
            .collect();
        assert_no_hidden(&parts, "gipk stream");
        delivered(&rig)
    };
    assert_eq!(
        stream(false),
        stream(true),
        "cdc: the policy over an account with the table's whole SELECT delivers what the stream does without it"
    );
}

/// What `out` said on stdout and stderr.
fn said_by(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_hidden_cursor_or_chunk_column_is_refused_by_code_before_the_read_mysql() {
    let (table, _table) = seeded("cg_cur");
    let user = GrantUser::select_only(&table, "id, name");
    for (what, mode, lines) in [
        ("cursor_column", "incremental", &["cursor_column: pass"][..]),
        (
            "chunk_by_key",
            "chunked",
            &["chunk_by_key: pass", "chunk_size: 1"][..],
        ),
    ] {
        let rig = Rig::mysql_cdc(&table)
            .source_url(&user.url())
            .source_line("columns_from: grants")
            .into_full_batch()
            .restage(mode, lines);
        let out = rig.run();
        let said = said_by(&out);
        assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
        assert!(
            said.contains("`pass`") && said.contains(what) && !said.contains("1054"),
            "{what}: the refusal names the column and the setting, not the server's error:\n{said}"
        );
        assert_refused_before_any_write(&rig.out_dir(), &rig.checkpoint());
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_visible_cursor_or_chunk_column_is_read_as_before_mysql() {
    let (table, _table) = seeded("cg_cur_ok");
    let user = GrantUser::select_only(&table, "id, name");
    for (mode, lines) in [
        ("incremental", &["cursor_column: id"][..]),
        ("chunked", &["chunk_by_key: id", "chunk_size: 1"][..]),
    ] {
        let rig = Rig::mysql_cdc(&table)
            .source_url(&user.url())
            .source_line("columns_from: grants")
            .into_full_batch()
            .restage(mode, lines);
        rig.run_ok();
        let parts: Vec<_> = parquet_dirs(&rig.out_dir())
            .iter()
            .flat_map(|d| read_all_parts(d))
            .collect();
        assert_no_hidden(&parts, "visible cursor or chunk");
        let (names, cells) = delivered(&rig);
        assert_eq!(names, vec!["id".to_string(), "name".to_string()], "{mode}");
        assert!(
            ["ann", "bob", "cid"]
                .iter()
                .all(|n| cells.contains(&n.to_string()))
        );
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_sealed_plan_applied_after_a_revoke_is_refused_by_code_with_the_replan_remedy_mysql() {
    let (table, _table) = seeded("cg_plan");
    let user = GrantUser::new(&table, "id, name, pass");
    let url_var = "RIVET_CG_PLAN_URL";
    let envs = [(url_var, user.url())];
    let env_refs: Vec<(&str, &str)> = envs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let rig = Rig::mysql_cdc(&table)
        .source_url_env(url_var)
        .source_line("columns_from: grants")
        .into_full_batch()
        .restage("full", &[]);
    let plan = rig.out_dir().with_file_name("plan.json");
    let planned = rig.plan_json_env(&plan, &[], &env_refs);
    assert!(planned.status.success(), "plan: {}", said_by(&planned));
    root()
        .query_drop(format!(
            "REVOKE SELECT (pass) ON rivet.{table} FROM '{}'@'%'",
            user.0
        ))
        .unwrap();
    let out = rig.apply_env(&plan, &[], &env_refs);
    let said = said_by(&out);
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
    assert!(
        said.contains("column 'pass'") && said.contains("rivet plan") && !said.contains("1143"),
        "the refusal names the column and the remedy (plan again), not the server's error:\n{said}"
    );
    let written = files_under(&rig.out_dir());
    assert!(
        written
            .iter()
            .all(|f| f.extension().is_some_and(|e| e == "json")),
        "a run refused at its read delivers no part and no success marker (a failed manifest is its record): {written:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_plan_and_its_apply_under_the_policy_read_the_readable_columns_mysql() {
    let (table, _table) = seeded("cg_plan_ok");
    let user = GrantUser::select_only(&table, "id, name");
    let url_var = "RIVET_CG_PLAN_URL";
    let envs = [(url_var, user.url())];
    let env_refs: Vec<(&str, &str)> = envs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let rig = Rig::mysql_cdc(&table)
        .source_url_env(url_var)
        .source_line("columns_from: grants")
        .into_full_batch()
        .restage("full", &[]);
    let plan = rig.out_dir().with_file_name("plan.json");
    let planned = rig.plan_json_env(&plan, &[], &env_refs);
    assert!(planned.status.success(), "plan: {}", said_by(&planned));
    let out = rig.apply_env(&plan, &[], &env_refs);
    assert!(out.status.success(), "apply: {}", said_by(&out));
    let (names, cells) = delivered(&rig);
    assert_eq!(names, vec!["id".to_string(), "name".to_string()]);
    assert!(!cells.iter().any(|v| v.contains(SECRET)), "{cells:?}");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_time_window_and_a_partitioned_export_under_the_policy_read_the_readable_columns_mysql() {
    let table = unique_name("cg_win");
    let _table = MysqlCdcTable(table.clone());
    let mut c = root();
    c.query_drop(format!(
        "CREATE TABLE rivet.{table} (id INT PRIMARY KEY, name VARCHAR(20), pass VARCHAR(20), \
         created_at DATETIME)"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO rivet.{table} VALUES (1, 'ann', '{SECRET}1', NOW()), \
         (2, 'bob', '{SECRET}2', NOW() - INTERVAL 2 DAY)"
    ))
    .unwrap();
    let user = GrantUser::select_only(&table, "id, name, created_at");
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .into_full_batch()
        .restage(
            "time_window",
            &["time_column: created_at", "days_window: 1"],
        );
    rig.run_ok();
    let parts: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    assert_no_hidden(&parts, "time_window");
    let (names, cells) = delivered(&rig);
    assert!(!names.iter().any(|n| n == "pass"), "{names:?}");
    assert!(!cells.iter().any(|v| v.contains(SECRET)), "{cells:?}");
    assert!(
        cells.iter().any(|v| v == "ann") && !cells.iter().any(|v| v == "bob"),
        "the window cuts by the hidden-or-not time column, the columns are the readable ones: {cells:?}"
    );

    let root_dir = tempfile::tempdir().unwrap();
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .into_full_batch()
        .restage(
            "full",
            &["partition_by: created_at", "partition_granularity: day"],
        )
        .dest_path(root_dir.path().join("{partition}"));
    let applied = rig.apply_env(&rig.config_path(), &[], &[]);
    assert!(
        applied.status.success(),
        "partitioned apply: {}",
        said_by(&applied)
    );
    let (names, cells) = delivered_under(root_dir.path());
    assert!(!names.iter().any(|n| n == "pass"), "{names:?}");
    assert!(!cells.iter().any(|v| v.contains(SECRET)), "{cells:?}");
    assert!(
        cells.iter().any(|v| v == "ann") && cells.iter().any(|v| v == "bob"),
        "{cells:?}"
    );
    let partitions = std::fs::read_dir(root_dir.path())
        .unwrap()
        .flatten()
        .filter(|e| e.path().is_dir())
        .count();
    assert!(partitions >= 2, "one directory per day: {partitions}");
}

/// A run over `seeded(label)` (account granted `granted`) parked just before the stream opens,
/// with the snapshot leg already read; `act` changes the grants and writes a row meanwhile.
/// The policy is one value per run: the run is refused by code (exit 2), no part carries
/// columns another part lacks, and a revoked value is never delivered.
fn the_policy_is_one_value_per_run(
    label: &str,
    granted: &str,
    point: &str,
    act: impl FnOnce(&str, &GrantUser),
) {
    let (table, _table) = seeded(label);
    let user = GrantUser::new(&table, granted);
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .cdc("initial: snapshot")
        .no_oracle(
            "the stop comes at the grant change after the snapshot leg finished: the leg is a complete delivery under its own prefix, with its marker, manifest and journal rows, which a failed run may not leave by the oracle's contract; the cell reads the parts itself",
        );
    let out = run_parked_at(&rig, point, 4000, || act(&table, &user));
    let said = said_by(&out);
    assert!(
        out.status.code() == Some(2) && said.contains("RIVET_SOURCE_CDC_GRANTS_CHANGED"),
        "a grant change between the two reads of one run is refused by code, exit 2:\n{said}"
    );
    let sets = delivered_columns(&rig);
    assert!(
        sets.windows(2).all(|w| {
            let (mut a, mut b) = (w[0].clone(), w[1].clone());
            a.sort();
            b.sort();
            a == b
        }),
        "the snapshot leg and the stream carry different columns in one run: {sets:?}\n{said}"
    );
    let (_, cells) = delivered(&rig);
    assert!(
        !cells.iter().any(|v| v.contains("REVOKED-")),
        "a value written after the revoke was delivered: {cells:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_grant_between_the_snapshot_and_the_stream_open_never_mixes_columns_in_one_run_mysql() {
    the_policy_is_one_value_per_run(
        "cg_once_a",
        "id, name",
        "cdc_before_stream_open",
        |table, user| {
            let mut c = root();
            c.query_drop(format!(
                "GRANT SELECT (pass) ON rivet.{table} TO '{}'@'%'",
                user.0
            ))
            .unwrap();
            c.query_drop(format!(
                "UPDATE rivet.{table} SET name = 'ann2', pass = 'GRANTED-1' WHERE id = 1"
            ))
            .unwrap();
        },
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_revoke_between_the_snapshot_and_the_stream_open_never_mixes_columns_in_one_run_mysql() {
    the_policy_is_one_value_per_run(
        "cg_once_b",
        "id, name, pass",
        "cdc_before_stream_open",
        |table, user| {
            let mut c = root();
            c.query_drop(format!(
                "REVOKE SELECT (pass) ON rivet.{table} FROM '{}'@'%'",
                user.0
            ))
            .unwrap();
            c.query_drop(format!(
                "UPDATE rivet.{table} SET name = 'ann2', pass = 'REVOKED-1' WHERE id = 1"
            ))
            .unwrap();
        },
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_grant_between_the_snapshot_read_and_the_anchor_never_mixes_columns_in_one_run_mysql() {
    the_policy_is_one_value_per_run(
        "cg_once_c",
        "id, name",
        "cdc_before_anchor",
        |table, user| {
            let mut c = root();
            c.query_drop(format!(
                "GRANT SELECT (pass) ON rivet.{table} TO '{}'@'%'",
                user.0
            ))
            .unwrap();
            c.query_drop(format!(
                "UPDATE rivet.{table} SET name = 'ann2', pass = 'GRANTED-1' WHERE id = 1"
            ))
            .unwrap();
        },
    );
}

/// `table` `(id PK, code UNIQUE, name, pass)` holding two rows, dropped on Drop.
fn seeded_with_unique_code(label: &str) -> (String, MysqlCdcTable) {
    let table = unique_name(label);
    let guard = MysqlCdcTable(table.clone());
    let mut c = root();
    c.query_drop(format!(
        "CREATE TABLE rivet.{table} (id INT PRIMARY KEY, code VARCHAR(10) NOT NULL UNIQUE, \
         name VARCHAR(20), pass VARCHAR(20))"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO rivet.{table} VALUES (1, 'c1', 'ann', '{SECRET}1'), (2, 'c2', 'bob', '{SECRET}2')"
    ))
    .unwrap();
    (table, guard)
}

const DECLARED_CODE_KEY: &str = "load: { target: bigquery, project: p, dataset: d, pk: [code] }";

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_declared_key_the_account_reads_is_accepted_while_the_primary_key_is_hidden_mysql() {
    let (table, _table) = seeded_with_unique_code("cg_dk_ok");
    let user = GrantUser::new(&table, "code, name");
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .top_line(DECLARED_CODE_KEY)
        .cdc("initial: snapshot");
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
    assert_no_hidden(&parts, "declared key");
    let (names, cells) = delivered(&rig);
    assert_eq!(names, vec!["code".to_string(), "name".to_string()]);
    assert!(cells.iter().any(|v| v == "ann2"), "{cells:?}");
    assert!(!cells.iter().any(|v| v.contains(SECRET)), "{cells:?}");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_declared_key_the_account_cannot_read_is_refused_by_code_before_anything_is_written_mysql() {
    let (table, _table) = seeded_with_unique_code("cg_dk_no");
    let user = GrantUser::new(&table, "id, name");
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .top_line(DECLARED_CODE_KEY)
        .cdc("initial: snapshot");
    let out = rig.run();
    let said = said_by(&out);
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
    assert!(
        said.contains("key column `code` (the `load.pk` of this export) is not readable under `columns_from: grants`")
            && said.contains("GRANT SELECT on it")
            && said.contains("nothing was read or written")
            && !said.contains("which the table does not have"),
        "the refusal names the key, where it came from and the remedy:\n{said}"
    );
    assert_refused_before_any_write(&rig.out_dir(), &rig.checkpoint());
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_primary_key_the_account_cannot_read_is_refused_by_code_with_the_remedy_mysql() {
    let (table, _table) = seeded_with_unique_code("cg_pk_no");
    let user = GrantUser::new(&table, "code, name");
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .cdc("initial: snapshot");
    let out = rig.run();
    let said = said_by(&out);
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
    assert!(
        said.contains(
            "key column `id` (the primary key) is not readable under `columns_from: grants`"
        ) && said.contains("GRANT SELECT on it")
            && said.contains("`load.pk`"),
        "the refusal names the key, where it came from and both remedies:\n{said}"
    );
    assert_refused_before_any_write(&rig.out_dir(), &rig.checkpoint());
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_the_account_may_update_but_not_select_is_not_delivered_mysql() {
    let (table, _table) = seeded("cg_upd");
    let user = GrantUser::new(&table, "id, name");
    root()
        .query_drop(format!(
            "GRANT UPDATE (pass) ON rivet.{table} TO '{}'@'%'",
            user.0
        ))
        .unwrap();
    let ungraded = |r: Rig| {
        r.no_oracle("the oracle lists the source's columns as the account and is refused on the UPDATE-only `pass`, the column this cell proves is never read")
    };
    let batch = ungraded(
        Rig::mysql_cdc(&table)
            .source_url(&user.url())
            .source_line("columns_from: grants")
            .into_full_batch(),
    );
    batch.run_ok();
    let (names, cells) = delivered(&batch);
    assert_eq!(names, vec!["id".to_string(), "name".to_string()], "batch");
    assert!(
        !cells.iter().any(|v| v.contains(SECRET)),
        "batch: {cells:?}"
    );

    let rig = ungraded(
        Rig::mysql_cdc(&table)
            .source_url(&user.url())
            .source_line("columns_from: grants")
            .cdc("initial: snapshot"),
    );
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
    assert_no_hidden(&parts, "update-only column cdc");
    let (names, cells) = delivered(&rig);
    assert_eq!(names, vec!["id".to_string(), "name".to_string()], "cdc");
    assert!(cells.iter().any(|v| v == "ann2"), "{cells:?}");
    assert!(!cells.iter().any(|v| v.contains(SECRET)), "cdc: {cells:?}");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_revoke_between_the_stream_open_and_the_schema_read_is_refused_by_code_mysql() {
    let (table, _table) = seeded("cg_open");
    let user = GrantUser::new(&table, "id, name, pass");
    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .a_failed_run_may_leave(
            &[Leftover::CdcCheckpoint],
            "the first run's anchor is written when the stream opens; the refusal comes after it",
        );
    let out = run_parked_at(&rig, "cdc_after_open", 3000, || {
        root()
            .query_drop(format!(
                "REVOKE SELECT (pass) ON rivet.{table} FROM '{}'@'%'",
                user.0
            ))
            .unwrap();
    });
    let said = said_by(&out);
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_GRANTS_CHANGED", 2));
    assert!(
        !said.contains("1143"),
        "the server's error is not the refusal:\n{said}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_schema_change_under_a_table_grant_is_refused_naming_the_column_that_appeared_mysql() {
    let run = an_open_run_as("cg_msg", 4, true, |table, _user| {
        let mut c = root();
        c.query_drop(format!(
            "ALTER TABLE rivet.{table} ADD COLUMN extra VARCHAR(20) DEFAULT 'x'"
        ))
        .unwrap();
        c.query_drop(format!(
            "INSERT INTO rivet.{table} VALUES (7, 'eve', 'p7', 'e7')"
        ))
        .unwrap();
        c.query_drop("CREATE USER IF NOT EXISTS 'cg_unrelated'@'%' IDENTIFIED BY 'x'")
            .unwrap();
        c.query_drop(format!(
            "INSERT INTO rivet.{table} VALUES (8, 'fay', 'p8', 'e8')"
        ))
        .unwrap();
    });
    root()
        .query_drop("DROP USER IF EXISTS 'cg_unrelated'@'%'")
        .unwrap();
    let said = run.said();
    assert!(
        run.out.status.code() == Some(2) && said.contains("RIVET_SOURCE_CDC_GRANTS_CHANGED"),
        "refused by code, exit 2:\n{said}"
    );
    assert!(
        said.contains("gained `extra`")
            && said.contains("a schema change under a table-level grant")
            && !said.contains("(a grant change)"),
        "the message says what changed:\n{said}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn privilege_statements_in_the_backlog_cost_the_server_a_bounded_number_of_connections_mysql() {
    let connections = |label: &str, statements: u32| -> u64 {
        let run = an_open_run_with(label, 4, |table, _user| {
            let mut c = root();
            for i in 0..statements {
                c.query_drop(format!("CREATE USER IF NOT EXISTS '{label}_{i}'@'%'"))
                    .unwrap();
            }
            c.query_drop(format!("INSERT INTO rivet.{table} VALUES (7, 'eve', 'p7')"))
                .unwrap();
        });
        let mut c = root();
        for i in 0..statements {
            c.query_drop(format!("DROP USER IF EXISTS '{label}_{i}'@'%'"))
                .unwrap();
        }
        let opened: Option<u64> = c
            .exec_first(
                "SELECT CAST(SUM(TOTAL_CONNECTIONS) AS UNSIGNED) FROM performance_schema.accounts \
                 WHERE USER = ?",
                (run.user.0.as_str(),),
            )
            .unwrap();
        opened.unwrap_or(0)
    };
    let few = connections("cg_conn_a", 2);
    let many = connections("cg_conn_b", 40);
    assert!(
        many <= few + 1,
        "40 privilege statements opened {many} connections as the run's account, 2 opened {few}: the cost of a statement is a connection"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_bare_table_name_is_read_from_the_same_relation_with_and_without_the_policy_mysql() {
    let table = unique_name("cg_bare");
    let other = OtherDb(unique_name("cg_baredb"));
    let mut c = root();
    c.query_drop(format!("CREATE DATABASE `{}`", other.0))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE `{}`.`{table}` (id INT PRIMARY KEY, name VARCHAR(20))",
        other.0
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO `{}`.`{table}` VALUES (1, 'FOREIGN-ROW')",
        other.0
    ))
    .unwrap();
    let rig = |policy: bool| {
        let mut r = Rig::mysql_cdc(&table).source_url(&root_url());
        if policy {
            r = r.source_line("columns_from: grants");
        }
        r.into_full_batch()
            .no_oracle("the relation under test lives in a second database; the oracle reads the url's database only")
    };
    let (plain, policy) = (rig(false), rig(true));
    let (plain_out, policy_out) = (plain.run(), policy.run());
    let (plain_said, policy_said) = (said_by(&plain_out), said_by(&policy_out));
    assert_refused(
        &plain_out,
        Refused::by_code("RIVET_SOURCE_TABLE_NOT_FOUND", 1),
    );
    assert_refused(
        &policy_out,
        Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1),
    );
    assert!(
        !policy_said.contains("FOREIGN") && !plain_said.contains("FOREIGN"),
        "the policy read a relation the plain run cannot:\nwithout:\n{plain_said}\nwith:\n{policy_said}"
    );
    assert!(
        !files_under(&policy.out_dir())
            .iter()
            .any(|f| std::fs::read_to_string(f).is_ok_and(|t| t.contains("FOREIGN"))),
        "the policy delivered the other database's rows"
    );
}

const BOUNDARY_TEXT: &str = "columns_from: grants - batch reads are enforced by the server (a column the \
account may not SELECT is never selected). The MySQL change stream is a client-side projection: the binlog \
carries every column of a changed row to this process, which drops the unreadable ones before anything is \
written. That is data minimisation, not access control: restrict the replication account itself if the values \
must not reach this host.";

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn the_policy_says_what_it_does_not_guarantee_at_check_and_at_the_start_of_a_run_mysql() {
    let (table, _table) = seeded("cg_say");
    let user = GrantUser::new(&table, "id, name");
    let on = Rig::mysql_cdc(&table)
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
    let off = Rig::mysql_cdc(&table).cdc("initial: snapshot");
    let check = off.cli(&["check"]);
    assert!(
        !said_by(&check).contains("data minimisation"),
        "a config without the policy says nothing about it:\n{}",
        said_by(&check)
    );
}

/// One DDL met in the backlog: the writes before and after it, then what the run exited with, delivered and checkpointed.
fn ddl_in_the_backlog(policy: bool, ddl: &str) -> String {
    let (table, _table) = seeded("cg_ddl");
    let user = GrantUser::new(&table, "id, name");
    let mut rig = Rig::mysql_cdc(&table).cdc("initial: snapshot");
    if ddl.contains("RENAME COLUMN") && !policy {
        rig = rig.no_oracle(
            "a rename met in a backlog is delivered as two columns, the old name for the rows before it and the new one after; the oracle reads the one column the source has now",
        );
    }
    if policy {
        rig = rig
            .source_url(&user.url())
            .source_line("columns_from: grants");
    }
    if policy && ddl.contains("RENAME COLUMN") {
        rig = rig.a_failed_run_may_leave(
            &[Leftover::ResumePoint],
            "the run observes the readable columns at its start, and a rename leaves the old column ungranted: that observed schema is stored before the refusal at the statement (known: src/state/schema.rs stores the baseline of a failed run)",
        );
    }
    rig.run_ok();
    let start = binlog_now();
    let mut c = root();
    c.query_drop(format!(
        "UPDATE rivet.{table} SET name = 'ann-before' WHERE id = 1"
    ))
    .unwrap();
    c.query_drop(ddl.replace("{t}", &table)).unwrap();
    let col = if ddl.contains("RENAME COLUMN name") {
        "nm"
    } else {
        "name"
    };
    c.query_drop(format!(
        "UPDATE rivet.{table} SET {col} = 'ann-after' WHERE id = 1"
    ))
    .unwrap();
    let end = commit_end_after(&start, &table);
    let out = rig.run();
    let (names, cells) = delivered(&rig);
    format!(
        "exit={} code={} advanced={} names={names:?} cells={cells:?}",
        out.status.code().unwrap_or(-1),
        said_by(&out)
            .split("[RIVET_")
            .nth(1)
            .and_then(|s| s.split(']').next())
            .unwrap_or("-"),
        checkpoint_at(&rig) >= end
    )
}

/// The string `ddl_in_the_backlog` gives for a delivery of the three updates' rows and the columns `names` (policy off: a root account).
fn the_ddl_run(exit: i32, code: &str, advanced: bool, names: &str, cells: &str) -> String {
    format!("exit={exit} code={code} advanced={advanced} names={names} cells={cells}")
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn ddl_in_the_backlog_is_met_as_before_mysql() {
    // Captured on origin/main a130fd97 (policy off) and on be1a8310 (policy on) before the fix.
    let id_cells = r#"["1", "1", "1", "2", "3""#;
    let ann = r#""ann", "ann-after", "ann-before", "bob", "cid"]"#;
    let off: [(&str, String); 4] = [
        (
            "ALTER TABLE rivet.{t} ADD COLUMN nick VARCHAR(10) NULL",
            the_ddl_run(
                0,
                "-",
                true,
                r#"["id", "name", "nick", "pass"]"#,
                &format!(
                    r#"["", "", "1", "1", "1", "2", "3", "SECRET-1", "SECRET-1", "SECRET-1", "SECRET-2", "SECRET-3", {ann}"#
                ),
            ),
        ),
        (
            "ALTER TABLE rivet.{t} DROP COLUMN pass",
            the_ddl_run(
                0,
                "-",
                true,
                r#"["id", "name", "pass"]"#,
                &format!(r#"{id_cells}, "SECRET-1", "SECRET-2", "SECRET-3", {ann}"#),
            ),
        ),
        (
            "ALTER TABLE rivet.{t} RENAME COLUMN name TO nm",
            the_ddl_run(
                0,
                "-",
                true,
                r#"["id", "name", "nm", "pass"]"#,
                &format!(
                    r#"{id_cells}, "SECRET-1", "SECRET-1", "SECRET-1", "SECRET-2", "SECRET-3", {ann}"#
                ),
            ),
        ),
        (
            "ALTER TABLE rivet.{t} RENAME COLUMN pass TO pw",
            the_ddl_run(
                0,
                "-",
                true,
                r#"["id", "name", "pass", "pw"]"#,
                &format!(
                    r#"{id_cells}, "SECRET-1", "SECRET-1", "SECRET-1", "SECRET-2", "SECRET-3", {ann}"#
                ),
            ),
        ),
    ];
    for (ddl, want) in off {
        assert_eq!(ddl_in_the_backlog(false, ddl), want, "policy off: {ddl}");
    }
    // The policy on: a column added or dropped is met as before the rename refusal.
    let visible = the_ddl_run(
        0,
        "-",
        true,
        r#"["id", "name"]"#,
        &format!(r#"{id_cells}, {ann}"#),
    );
    for ddl in [
        "ALTER TABLE rivet.{t} ADD COLUMN nick VARCHAR(10) NULL",
        "ALTER TABLE rivet.{t} DROP COLUMN pass",
    ] {
        assert_eq!(ddl_in_the_backlog(true, ddl), visible, "policy on: {ddl}");
    }
    assert_eq!(
        ddl_in_the_backlog(true, "ALTER TABLE rivet.{t} RENAME COLUMN pass TO pw"),
        the_ddl_run(
            5,
            "SOURCE_CDC_UNDECODABLE",
            false,
            r#"["id", "name"]"#,
            r#"["1", "2", "3", "ann", "bob", "cid"]"#
        ),
        "a hidden column's rename is refused too: the backlog cannot tell it from a delivered one"
    );
}
