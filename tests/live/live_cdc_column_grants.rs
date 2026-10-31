//! Live `source.columns_from: grants` (MySQL): a column-grant account delivers only the columns it may SELECT.

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

/// A replication account that may SELECT only `columns` of `table`; the role it holds, if any, is dropped with it.
struct GrantUser(String, Option<String>);

impl GrantUser {
    fn new(table: &str, columns: &str) -> Self {
        Self::new_in("rivet", table, columns)
    }

    /// As `new`, for a table of the database `db`.
    fn new_in(db: &str, table: &str, columns: &str) -> Self {
        let user = Self::select_only_in(db, table, columns);
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
        Self::select_only_in("rivet", table, columns)
    }

    /// As `select_only`, for a table of the database `db`.
    fn select_only_in(db: &str, table: &str, columns: &str) -> Self {
        let user = unique_name("cg_u");
        let mut c = root();
        c.query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
            .unwrap();
        c.query_drop(format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'"))
            .unwrap();
        c.query_drop(format!(
            "GRANT SELECT ({columns}) ON {db}.{table} TO '{user}'@'%'"
        ))
        .unwrap();
        Self(user, None)
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
        Self(user, None)
    }

    /// An account that may SELECT every column of `table` by a database-level grant, plus replication.
    fn database_level(table: &str) -> Self {
        let user = Self::select_only(table, "id");
        let mut c = root();
        c.query_drop(format!(
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'",
            user.0
        ))
        .unwrap();
        c.query_drop(format!("GRANT SELECT ON rivet.* TO '{}'@'%'", user.0))
            .unwrap();
        user
    }

    /// An account that may SELECT `id, name` of `table` itself and `pass` only through a role that is its default role, plus replication.
    fn with_default_role(table: &str) -> Self {
        let mut user = Self::new(table, "id, name");
        let role = unique_name("cg_r");
        let mut c = root();
        c.query_drop(format!("CREATE ROLE '{role}'")).unwrap();
        c.query_drop(format!(
            "GRANT SELECT (id, name, pass) ON rivet.{table} TO '{role}'"
        ))
        .unwrap();
        c.query_drop(format!("GRANT '{role}' TO '{}'@'%'", user.0))
            .unwrap();
        c.query_drop(format!("SET DEFAULT ROLE '{role}' TO '{}'@'%'", user.0))
            .unwrap();
        user.1 = Some(role);
        user
    }

    fn url(&self) -> String {
        MYSQL_CDC_URL.replace("rivet:rivet@", &format!("{}:pw@", self.0))
    }
}

impl Drop for GrantUser {
    fn drop(&mut self) {
        let _ = root().query_drop(format!("DROP USER IF EXISTS '{}'@'%'", self.0));
        if let Some(role) = &self.1 {
            let _ = root().query_drop(format!("DROP ROLE IF EXISTS '{role}'"));
        }
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
        format!("INSERT INTO rivet.{table} VALUES (3, 'cid', '{SECRET}c')"),
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
    assert!(
        !names.iter().any(|n| n.eq_ignore_ascii_case("pass")),
        "`pass` is a column of the delivered parts: {names:?}"
    );
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
    let out = Rig::mysql_cdc(&table)
        .source_url(&readable.url())
        .cdc("initial: snapshot")
        .run();
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_QUERY_FAILED", 1));
    assert!(
        said_by(&out).contains("1142"),
        "without `columns_from: grants` the account's `SELECT *` is refused by the server:\n{}",
        said_by(&out)
    );

    let keyless = GrantUser::new(&table, "name");
    let out = Rig::mysql_cdc(&table)
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

/// `table` holding `(1,'ann',SECRET)`, `(2,'bob',SECRET)`, `(3,'cid',SECRET)`, dropped on Drop.
fn seeded(label: &str) -> (String, MysqlCdcTable) {
    let table = unique_name(label);
    let guard = MysqlCdcTable(table.clone());
    seed_rows("rivet", &table);
    (table, guard)
}

/// Creates `db.table` with the three seeded rows.
fn seed_rows(db: &str, table: &str) {
    let mut c = root();
    c.query_drop(format!(
        "CREATE TABLE {db}.{table} (id INT PRIMARY KEY, name VARCHAR(20), pass VARCHAR(20))"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO {db}.{table} VALUES (1, 'ann', '{SECRET}1'), (2, 'bob', '{SECRET}2'), \
         (3, 'cid', '{SECRET}3')"
    ))
    .unwrap();
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
    run_parked_through(rig, point, ms, vec![Box::new(during)])
}

/// As [`run_parked_at`] for a run that parks at `point` once per closure.
fn run_parked_through<'a>(
    rig: &Rig,
    point: &str,
    ms: u64,
    during: Vec<Box<dyn FnOnce() + 'a>>,
) -> std::process::Output {
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
    for act in during {
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
        std::fs::remove_file(&marker).expect("clear the park marker");
        act();
    }
    child.wait_with_output().expect("reap the parked run")
}

/// The open run stopped before the statement it met: by the table name in it, or by the re-read of the grants a foreign statement of the shared log called for first.
fn a_run_stopped_at_a_statement_naming_the_table(run: &OpenRun) {
    let said = run.said();
    if said.contains("[RIVET_SOURCE_CDC_GRANTS_CHANGED]") {
        assert_refused(
            &run.out,
            Refused::by_code("RIVET_SOURCE_CDC_GRANTS_CHANGED", 2),
        );
        return;
    }
    assert_refused(
        &run.out,
        Refused::by_code("RIVET_SOURCE_CDC_UNDECODABLE", 5),
    );
    assert!(
        said.contains("names the captured table") && said.contains("Re-baseline the stream"),
        "the refusal names the table and the remedy:\n{said}"
    );
}

/// An open continuous run over `seeded(label)`: `act` writes to the source while it parks, then it ends at `max_events`.
fn an_open_run_with(label: &str, max_events: u32, act: impl FnOnce(&str, &GrantUser)) -> OpenRun {
    an_open_run_as(label, max_events, false, act)
}

/// As [`an_open_run_with`]; `whole_table` makes the account hold SELECT on the whole table.
fn an_open_run_as<'a>(
    label: &str,
    max_events: u32,
    whole_table: bool,
    act: impl FnOnce(&str, &GrantUser) + 'a,
) -> OpenRun {
    an_open_run_through(
        label,
        max_events,
        |table| {
            if whole_table {
                GrantUser::whole_table(table)
            } else {
                GrantUser::new(table, "id, name, pass")
            }
        },
        vec![Box::new(act)],
    )
}

/// What the source does while an open run is parked: called with the table and the account.
type Act<'a> = Box<dyn FnOnce(&str, &GrantUser) + 'a>;

/// An open continuous run of `account(table)`: `acts[i]` writes to the source during its i-th park.
fn an_open_run_through<'a>(
    label: &str,
    max_events: u32,
    account: impl FnOnce(&str) -> GrantUser,
    acts: Vec<Act<'a>>,
) -> OpenRun {
    let (table, _table) = seeded(label);
    let user = account(&table);
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
    let during: Vec<Box<dyn FnOnce() + '_>> = acts
        .into_iter()
        .map(|act| {
            let (table, user) = (&table, &user);
            Box::new(move || act(table, user)) as Box<dyn FnOnce() + '_>
        })
        .collect();
    let out = run_parked_through(&rig, "cdc_after_parts_before_ack", 8000, during);
    OpenRun {
        user,
        rig,
        out,
        _table,
    }
}

/// The run ended at a grant change; the rest of the backlog, read on its own, holds no value written after the change.
fn nothing_written_after_the_change_leaves(run: &mut OpenRun) {
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

/// An open continuous run, a grant change written as `revoke(table, user)`, a row after it.
fn a_revoke_naming_the_table_is_refused_by_name_before_the_column_leaves(
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
    a_run_stopped_at_a_statement_naming_the_table(&run);
    let parts: Vec<_> = parquet_dirs(&run.rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    let (_, cells) = names_and_cells(&parts);
    assert!(
        !cells.iter().any(|v| v.contains("REVOKED-")),
        "a value written after the revoke was delivered: {cells:?}"
    );
    run.rig
        .replace_cdc_line("until_current", "until_current: true");
    follow_rebaseline_remedy(&mut run.rig, true);
    let (names, cells) = delivered(&run.rig);
    assert!(
        names.iter().all(|n| n == "id" || n == "name") && cells.iter().any(|v| v == "eve"),
        "the re-baseline reads the account's columns as they are now: {names:?} {cells:?}"
    );
}

/// An open run that re-read the grants once, then `change` the account's grants and one row the changed grants hide.
fn a_login_time_grant_change_after_an_earlier_re_read_stops_an_open_run(
    label: &str,
    account: impl FnOnce(&str) -> GrantUser,
    change: fn(&str, &str) -> Vec<String>,
) {
    let mut run = an_open_run_through(
        label,
        7,
        account,
        vec![
            Box::new(|table, _| {
                let mut c = root();
                c.query_drop("FLUSH PRIVILEGES").unwrap();
                c.query_drop(format!(
                    "INSERT INTO rivet.{table} VALUES (7, 'gus', 'open'), (8, 'hal', 'open'), (9, 'ivy', 'open')"
                ))
                .unwrap();
            }),
            Box::new(move |table, user| {
                let mut c = root();
                for q in change(table, &user.0) {
                    c.query_drop(q).unwrap();
                }
                c.query_drop(format!(
                    "INSERT INTO rivet.{table} VALUES (10, 'eve', 'REVOKED-10')"
                ))
                .unwrap();
            }),
        ],
    );
    assert_refused(
        &run.out,
        Refused::by_code("RIVET_SOURCE_CDC_GRANTS_CHANGED", 2),
    );
    nothing_written_after_the_change_leaves(&mut run);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_database_level_revoke_after_an_earlier_re_read_stops_an_open_run_mysql() {
    a_login_time_grant_change_after_an_earlier_re_read_stops_an_open_run(
        "cg_dbl",
        |table| {
            let user = GrantUser::database_level(table);
            root()
                .query_drop(format!(
                    "GRANT SELECT (id, name) ON rivet.{table} TO '{}'@'%'",
                    user.0
                ))
                .unwrap();
            user
        },
        |_, user| vec![format!("REVOKE SELECT ON rivet.* FROM '{user}'@'%'")],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_default_role_taken_away_after_an_earlier_re_read_stops_an_open_run_mysql() {
    a_login_time_grant_change_after_an_earlier_re_read_stops_an_open_run(
        "cg_drl",
        GrantUser::with_default_role,
        |_, user| vec![format!("SET DEFAULT ROLE NONE TO '{user}'@'%'")],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_revoke_during_a_continuous_run_is_refused_by_name_before_the_column_leaves_mysql() {
    a_revoke_naming_the_table_is_refused_by_name_before_the_column_leaves(
        "cg_rev",
        |table, user| format!("REVOKE SELECT (pass) ON rivet.{table} FROM '{user}'@'%'"),
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_revoke_behind_a_line_comment_in_an_executable_comment_is_refused_by_name_mysql() {
    a_revoke_naming_the_table_is_refused_by_name_before_the_column_leaves(
        "cg_revl",
        |table, user| {
            format!("/*!80000 -- note\nREVOKE SELECT (pass) ON rivet.{table} FROM '{user}'@'%' */")
        },
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_revoke_written_as_an_executable_comment_is_refused_by_name_mysql() {
    a_revoke_naming_the_table_is_refused_by_name_before_the_column_leaves(
        "cg_revx",
        |table, user| {
            format!("/*!80000 REVOKE SELECT (pass) ON rivet.{table} FROM '{user}'@'%' */")
        },
    );
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
            said.contains("a PARTIAL image")
                && said.contains("binlog_row_image = FULL")
                && said.contains(REBASELINE_REMEDY)
                && !said.contains("a delay rather than a loss"),
            "{image} {what}: the refusal names the cause and the re-baseline that gets past it, not a re-run that meets the same image again:\n{said}"
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
        writer
            .query_drop("SET SESSION binlog_row_image = 'FULL'")
            .unwrap();
        let mut rig = rig;
        follow_rebaseline_remedy(&mut rig, true);
        let (names, mut cells) = delivered(&rig);
        let mut expected: Vec<String> = root()
            .query::<(i64, Option<String>), _>(format!("SELECT id, name FROM rivet.{table}"))
            .unwrap()
            .into_iter()
            .flat_map(|(id, name)| [id.to_string(), name.unwrap_or_default()])
            .collect();
        cells.sort();
        expected.sort();
        assert_eq!(
            cells, expected,
            "{image} {what}: the remedy the refusal names re-baselines the stream onto the source as it is now"
        );
        assert!(
            names.iter().all(|n| n == "id" || n == "name"),
            "{image} {what}: only the readable columns after the re-baseline: {names:?}"
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

/// Every command that loads the config refuses `columns_from: grants` off MySQL, by code, before it connects or writes.
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

/// The binlog `(file, position)` at the end of the last commit after `start` that wrote `table`.
fn commit_end_after(start: &(String, u64), table: &str) -> (String, u64) {
    let mut c = root();
    let events: Vec<(u64, String, u64, Vec<u8>)> = c
        .query_map(
            format!("SHOW BINLOG EVENTS IN '{}' FROM {}", start.0, start.1),
            |r: mysql::Row| {
                (
                    r.get::<u64, _>(1).unwrap_or(0),
                    r.get::<String, _>(2).unwrap_or_default(),
                    r.get::<u64, _>(4).unwrap_or(0),
                    r.get::<Vec<u8>, _>(5).unwrap_or_default(),
                )
            },
        )
        .unwrap();
    let (mut wrote, mut last) = (false, None);
    for (_, kind, end, info) in events {
        wrote |= kind == "Table_map"
            && String::from_utf8_lossy(&info).contains(&format!("(rivet.{table})"));
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

/// True (and a skip is recorded) unless the stand is ours alone: the checkpoint cells compare the checkpoint with the exact end of the log.
fn the_stand_is_shared() -> bool {
    let shared = std::env::var("RIVET_TEST_EXCLUSIVE").is_err();
    if shared {
        skip_live(
            "any commit of another writer moves the end of the log the checkpoint is compared with — run with RIVET_TEST_EXCLUSIVE=1 and --test-threads=1",
        );
    }
    shared
}

/// A run under the policy, then `writes` committed one by one: the next checkpoint is the end of the last commit.
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
    let end = binlog_now();
    let want = commit_end_after(&start, &watch(&table, &other));
    assert_eq!(
        want, end,
        "{what}: a commit of another writer followed the cell's last one; the cell needs the stand to itself"
    );
    rig.run_ok();
    assert_eq!(
        checkpoint_at(&rig),
        want,
        "{what}: the checkpoint is not exactly the end of the last commit"
    );
    let parts: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    assert_no_hidden(&parts, what);
    changes(&parts)
}

#[test]
#[ignore = "live+exclusive: the checkpoint is compared with the exact end of the log — set RIVET_TEST_EXCLUSIVE=1"]
fn an_update_of_hidden_columns_only_advances_the_checkpoint_mysql() {
    if the_stand_is_shared() {
        return;
    }
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
#[ignore = "live+exclusive: the checkpoint is compared with the exact end of the log — set RIVET_TEST_EXCLUSIVE=1"]
fn an_update_of_an_uncaptured_table_advances_the_checkpoint_under_the_policy_mysql() {
    if the_stand_is_shared() {
        return;
    }
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
#[ignore = "live+exclusive: the checkpoint is compared with the exact end of the log — set RIVET_TEST_EXCLUSIVE=1"]
fn a_visible_update_and_a_mixed_transaction_advance_the_checkpoint_as_before_mysql() {
    if the_stand_is_shared() {
        return;
    }
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

/// `steps` committed in the backlog after the anchored run of `rig`: the run is refused by `code`, its message holds every part of `said`, the checkpoint stays and no change is delivered; `mend` repairs what the remedy needs, the re-baseline is followed and `reads` grades what it delivers.
fn a_refused_backlog_is_got_past_by_the_remedy(
    rig: Rig,
    steps: Vec<String>,
    code: &str,
    said_parts: &[String],
    mend: impl FnOnce(),
    reads: impl FnOnce(&Rig),
) {
    let said_parts: Vec<String> = said_parts
        .iter()
        .cloned()
        .chain(["Re-baseline the stream".to_string()])
        .collect();
    a_stopped_backlog_is_got_past_by_the_remedy(
        rig,
        steps,
        Refused::by_code(code, 5),
        &said_parts,
        mend,
        reads,
    );
}

/// As `a_refused_backlog_is_got_past_by_the_remedy` for a stop `refused` names, whose message holds exactly `said_parts`.
fn a_stopped_backlog_is_got_past_by_the_remedy(
    rig: Rig,
    steps: Vec<String>,
    refused: Refused,
    said_parts: &[String],
    mend: impl FnOnce(),
    reads: impl FnOnce(&Rig),
) {
    let mut rig = rig.a_failed_run_may_leave(
        &[Leftover::ResumePoint],
        "the run observes the readable columns at its start: that observed schema is stored before the refusal at the statement (known: src/state/schema.rs stores the baseline of a failed run)",
    );
    let mut c = root();
    for sql in steps {
        c.query_drop(sql).unwrap();
    }
    let before = checkpoint_at(&rig);
    let out = rig.run();
    let said = said_by(&out);
    assert_refused(&out, refused);
    for part in said_parts {
        assert!(said.contains(part), "the refusal lacks {part:?}:\n{said}");
    }
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
    mend();
    follow_rebaseline_remedy(&mut rig, true);
    reads(&rig);
}

/// A column renamed in the backlog by `rename(table)`: the run is refused by code and the re-baseline delivers the column under its new name.
fn a_column_renamed_in_the_backlog_is_refused_not_silently_dropped(
    label: &str,
    rename: fn(&str) -> Vec<String>,
) {
    let (table, _guard, user, rig) = anchored_under_the_policy(label);
    let steps = std::iter::once(UPDATED_BEFORE.replace("{t}", &table))
        .chain(rename(&table))
        .chain([UPDATED_AFTER_AS_NM.replace("{t}", &table)])
        .collect();
    a_refused_backlog_is_got_past_by_the_remedy(
        rig,
        steps,
        "RIVET_SOURCE_CDC_UNDECODABLE",
        &[table.clone(), "RENAME".to_string()],
        || {
            root()
                .query_drop(format!(
                    "GRANT SELECT (nm) ON rivet.{table} TO '{}'@'%'",
                    user.0
                ))
                .unwrap();
        },
        |rig| {
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
        },
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_renamed_in_the_backlog_is_refused_not_silently_dropped_mysql() {
    a_column_renamed_in_the_backlog_is_refused_not_silently_dropped("cg_ren", |t| {
        vec![format!("ALTER TABLE rivet.{t} RENAME COLUMN name TO nm")]
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_renamed_in_an_executable_comment_is_refused_not_silently_dropped_mysql() {
    a_column_renamed_in_the_backlog_is_refused_not_silently_dropped("cg_renx", |t| {
        vec![format!(
            "/*!80000 ALTER TABLE rivet.{t} RENAME COLUMN name TO nm */"
        )]
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_renamed_after_a_dash_comment_is_refused_not_silently_dropped_mysql() {
    a_column_renamed_in_the_backlog_is_refused_not_silently_dropped("cg_rend", |t| {
        vec![format!(
            "ALTER TABLE rivet.{t} -- c\nRENAME COLUMN name TO nm"
        )]
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_renamed_after_a_hash_comment_is_refused_not_silently_dropped_mysql() {
    a_column_renamed_in_the_backlog_is_refused_not_silently_dropped("cg_renh", |t| {
        vec![format!(
            "ALTER TABLE rivet.{t} # c\nRENAME COLUMN name TO nm"
        )]
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_renamed_beside_a_comment_in_an_executable_comment_is_refused_mysql() {
    a_column_renamed_in_the_backlog_is_refused_not_silently_dropped("cg_renn", |t| {
        vec![format!(
            "/*!80000 ALTER TABLE rivet.{t} /* c */ RENAME COLUMN name TO nm */"
        )]
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_renamed_with_ansi_quoted_names_is_refused_mysql() {
    a_column_renamed_in_the_backlog_is_refused_not_silently_dropped("cg_rena", |t| {
        vec![
            "SET SESSION sql_mode = 'ANSI_QUOTES'".to_string(),
            format!("ALTER TABLE \"rivet\".\"{t}\" RENAME COLUMN \"name\" TO \"nm\""),
        ]
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_changed_with_ansi_quoted_names_is_refused_mysql() {
    a_column_renamed_in_the_backlog_is_refused_not_silently_dropped("cg_renc", |t| {
        vec![
            "SET SESSION sql_mode = 'ANSI_QUOTES'".to_string(),
            format!("ALTER TABLE rivet.{t} CHANGE \"name\" \"nm\" VARCHAR(20)"),
        ]
    });
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
        said.contains("column 'pass'")
            && said.contains("rivet plan")
            && !said.contains("ERROR 1143"),
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

/// A table of `rows` rows `(id, name, pass)`, every `pass` a `SECRET-` value, dropped on Drop.
fn big_table(label: &str, rows: u32) -> (String, MysqlCdcTable) {
    let table = unique_name(label);
    let guard = MysqlCdcTable(table.clone());
    let mut c = root();
    c.query_drop(format!(
        "CREATE TABLE rivet.{table} (id INT PRIMARY KEY, name VARCHAR(20), pass VARCHAR(20))"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO rivet.{table} \
         WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < {rows}) \
         SELECT n, CONCAT('n', n), CONCAT('{SECRET}', n) FROM s"
    ))
    .unwrap();
    (table, guard)
}

/// A first run over a throttled snapshot of a big table; `act` runs while the snapshot is read.
fn the_policy_is_one_value_per_run(
    label: &str,
    granted: &str,
    two_tables: bool,
    act: impl FnOnce(&str, &GrantUser),
) {
    let (first, _first) = big_table(label, 60);
    let (second, _second) = seeded(&format!("{label}_b"));
    let user = GrantUser::new(&first, granted);
    root()
        .query_drop(format!(
            "GRANT SELECT ({granted}) ON rivet.{second} TO '{}'@'%'",
            user.0
        ))
        .unwrap();
    let tables: Vec<&str> = if two_tables {
        vec![&first, &second]
    } else {
        vec![&first]
    };
    let rig = Rig::mysql_cdc(&first)
        .tables(&tables)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .source_line("tuning:")
        .source_line("  batch_size: 10")
        .source_line("  throttle_ms: 1500")
        .cdc("initial: snapshot")
        .no_oracle(
            "the stop comes at the grant change after a snapshot leg finished: the leg is a complete delivery under its own prefix, with its marker, manifest and journal rows, which a failed run may not leave by the oracle's contract; the cell reads the parts itself",
        );
    let mut child = rig.spawn_args_env(&[], &[]);
    let opened = std::time::Instant::now();
    let mut c = root();
    while c
        .exec_first::<u64, _, _>(
            "SELECT COUNT(*) FROM performance_schema.processlist WHERE USER = ? AND TIME >= 1",
            (user.0.as_str(),),
        )
        .unwrap()
        .unwrap_or(0)
        == 0
    {
        if let Ok(Some(_)) = child.try_wait() {
            let out = child.wait_with_output().unwrap();
            panic!(
                "the run ended before it read the first table:\n{}",
                said_by(&out)
            );
        }
        assert!(
            opened.elapsed() < std::time::Duration::from_secs(60),
            "the run never sat in a snapshot read"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    act(if two_tables { &second } else { &first }, &user);
    let out = child.wait_with_output().expect("reap the run");
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_GRANTS_CHANGED", 2));
    for dir in parquet_dirs(&rig.out_dir()) {
        let sets: Vec<Vec<String>> = read_all_parts(&dir)
            .iter()
            .map(|b| {
                let mut names: Vec<String> = b
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .filter(|n| !n.starts_with("__"))
                    .collect();
                names.sort();
                names
            })
            .collect();
        assert!(
            sets.windows(2).all(|w| w[0] == w[1]),
            "{}: one table's parts carry different columns in one run: {sets:?}\n{}",
            dir.display(),
            said_by(&out)
        );
    }
    let (_, cells) = delivered(&rig);
    assert!(
        !cells.iter().any(|v| v.contains("REVOKED-")),
        "a value written after the revoke was delivered: {cells:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_grant_while_the_snapshot_is_read_never_mixes_columns_in_one_run_mysql() {
    the_policy_is_one_value_per_run("cg_once_a", "id, name", true, |last, user| {
        let mut c = root();
        c.query_drop(format!(
            "GRANT SELECT (pass) ON rivet.{last} TO '{}'@'%'",
            user.0
        ))
        .unwrap();
        c.query_drop(format!(
            "UPDATE rivet.{last} SET name = 'ann2', pass = 'GRANTED-1' WHERE id = 1"
        ))
        .unwrap();
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_revoke_while_the_last_snapshot_is_read_never_mixes_columns_in_one_run_mysql() {
    the_policy_is_one_value_per_run("cg_once_b", "id, name, pass", false, |last, user| {
        let mut c = root();
        c.query_drop(format!(
            "REVOKE SELECT (pass) ON rivet.{last} FROM '{}'@'%'",
            user.0
        ))
        .unwrap();
        c.query_drop(format!(
            "UPDATE rivet.{last} SET name = 'ann2', pass = 'REVOKED-1' WHERE id = 1"
        ))
        .unwrap();
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_revoke_before_a_later_snapshot_is_read_is_a_grant_change_not_a_stale_plan_mysql() {
    the_policy_is_one_value_per_run("cg_once_c", "id, name, pass", true, |last, user| {
        let mut c = root();
        c.query_drop(format!(
            "REVOKE SELECT (pass) ON rivet.{last} FROM '{}'@'%'",
            user.0
        ))
        .unwrap();
    });
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
        .census_key("code")
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
    let batch = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
        .into_full_batch();
    batch.run_ok();
    let (names, cells) = delivered(&batch);
    assert_eq!(names, vec!["id".to_string(), "name".to_string()], "batch");
    assert!(
        !cells.iter().any(|v| v.contains(SECRET)),
        "batch: {cells:?}"
    );

    let rig = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .source_line("columns_from: grants")
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
        !said.contains("ERROR 1143"),
        "the server's error is not the refusal:\n{said}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_schema_change_under_a_table_grant_is_refused_by_the_table_name_with_the_remedy_mysql() {
    let run = an_open_run_as("cg_msg", 5, true, |table, _user| {
        let mut c = root();
        c.query_drop(format!(
            "ALTER TABLE rivet.{table} ADD COLUMN extra VARCHAR(20) DEFAULT 'x'"
        ))
        .unwrap();
        c.query_drop(format!(
            "INSERT INTO rivet.{table} VALUES (7, 'eve', 'p7', 'e7')"
        ))
        .unwrap();
    });
    a_run_stopped_at_a_statement_naming_the_table(&run);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_database_level_grant_that_adds_a_column_is_refused_naming_the_column_that_appeared_mysql() {
    let run = an_open_run_through(
        "cg_gain",
        4,
        |table| GrantUser::new(table, "id, name"),
        vec![Box::new(|table, user| {
            let mut c = root();
            c.query_drop(format!("GRANT SELECT ON rivet.* TO '{}'@'%'", user.0))
                .unwrap();
            c.query_drop(format!(
                "INSERT INTO rivet.{table} VALUES (7, 'eve', 'GAINED-7')"
            ))
            .unwrap();
        })],
    );
    let said = run.said();
    assert_refused(
        &run.out,
        Refused::by_code("RIVET_SOURCE_CDC_GRANTS_CHANGED", 2),
    );
    assert!(
        said.contains("gained `pass`") && said.contains("a grant change, or a schema change"),
        "the message says what changed:\n{said}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn privilege_statements_in_the_backlog_cost_the_server_a_bounded_number_of_connections_mysql() {
    let connections = |label: &str, statements: u32, privilege: bool| -> u64 {
        let statement = |i: u32| {
            if privilege {
                format!("CREATE USER IF NOT EXISTS '{label}_{i}'@'%'")
            } else {
                format!("CREATE TABLE IF NOT EXISTS rivet.{label}_{i} (id INT)")
            }
        };
        let undo = |i: u32| {
            if privilege {
                format!("DROP USER IF EXISTS '{label}_{i}'@'%'")
            } else {
                format!("DROP TABLE IF EXISTS rivet.{label}_{i}")
            }
        };
        let run = an_open_run_with(label, 4, |table, _user| {
            let mut c = root();
            for i in 0..statements {
                c.query_drop(statement(i)).unwrap();
            }
            c.query_drop(format!("INSERT INTO rivet.{table} VALUES (7, 'eve', 'p7')"))
                .unwrap();
        });
        let mut c = root();
        for i in 0..statements {
            c.query_drop(undo(i)).unwrap();
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
    let few = connections("cg_conn_a", 2, true);
    let many = connections("cg_conn_b", 40, true);
    let plain = connections("cg_conn_c", 40, false);
    assert!(
        many <= few + 1,
        "40 privilege statements opened {many} connections as the run's account, 2 opened {few}: the cost of a statement is a connection"
    );
    assert!(
        plain <= few + 1,
        "40 statements that change no privilege opened {plain} connections as the run's account, 2 privilege statements opened {few}: only a privilege statement is re-read"
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
    let (names, cells) = delivered(&policy);
    assert!(
        names.is_empty() && cells.is_empty(),
        "the policy delivered the other database's rows: {names:?} {cells:?}"
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

/// The columns `rivet check --target bigquery --json` types for the rig's export, or the refusal it ended with.
fn checked_columns(rig: &Rig) -> Result<Vec<String>, std::process::Output> {
    let out = rig.cli(&["check", "--target", "bigquery", "--json"]);
    if !out.status.success() {
        return Err(out);
    }
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let first = stdout
        .lines()
        .find(|l| l.starts_with('{'))
        .unwrap_or_else(|| panic!("a type report on stdout:\n{stdout}"));
    let report: serde_json::Value = serde_json::from_str(first).unwrap();
    Ok(report["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["column"].as_str().unwrap().to_string())
        .collect())
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn the_type_report_probes_the_columns_of_the_policy_mysql() {
    let (table, _table) = seeded("cg_chk");
    let user = GrantUser::new(&table, "id, name");
    let on = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    assert_eq!(
        checked_columns(&on).map_err(|o| said_by(&o)),
        Ok(vec!["id".to_string(), "name".to_string()]),
        "under the policy the type report types the columns the account reads, and runs"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn the_type_report_of_a_mysql_source_without_the_policy_is_as_on_main_mysql() {
    let (table, _table) = seeded("cg_chk_off");
    let user = GrantUser::new(&table, "id, name");
    let off = Rig::mysql_cdc(&table)
        .source_url(&user.url())
        .cdc("initial: snapshot");
    let denied =
        checked_columns(&off).expect_err("without the policy the account's SELECT * is denied");
    assert_refused(&denied, Refused::by_code("RIVET_SOURCE_QUERY_FAILED", 1));
    assert!(
        said_by(&denied).contains("SELECT command denied"),
        "a column-restricted account without the policy is refused as before:\n{}",
        said_by(&denied)
    );
    let root_off = Rig::mysql_cdc(&table).cdc("initial: snapshot");
    assert_eq!(
        checked_columns(&root_off).map_err(|o| said_by(&o)),
        Ok(vec![
            "id".to_string(),
            "name".to_string(),
            "pass".to_string()
        ]),
        "a root account without the policy types every column, as before"
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
    let refused = the_ddl_run(
        5,
        "SOURCE_CDC_UNDECODABLE",
        false,
        r#"["id", "name"]"#,
        r#"["1", "2", "3", "ann", "bob", "cid"]"#,
    );
    for ddl in [
        "ALTER TABLE rivet.{t} ADD COLUMN nick VARCHAR(10) NULL",
        "ALTER TABLE rivet.{t} DROP COLUMN pass",
        "ALTER TABLE rivet.{t} RENAME COLUMN pass TO pw",
    ] {
        assert_eq!(
            ddl_in_the_backlog(true, ddl),
            refused,
            "policy on: a statement naming the captured table is refused whatever it changes: {ddl}"
        );
    }
}

/// `(op, id, name or nm)` of every change in `parts`, in capture order.
fn named_changes(parts: &[arrow::record_batch::RecordBatch]) -> Vec<(String, i64, String)> {
    let mut out = Vec::new();
    for b in parts.iter().filter(|b| b.column_by_name("__op").is_some()) {
        let s = |n: &str, i: usize| {
            arrow::util::display::array_value_to_string(b.column_by_name(n).unwrap(), i).unwrap()
        };
        let name = if b.column_by_name("name").is_some() {
            "name"
        } else {
            "nm"
        };
        for i in 0..b.num_rows() {
            let seq: i64 = s("__seq", i).parse().unwrap();
            out.push((seq, (s("__op", i), s("id", i).parse().unwrap(), s(name, i))));
        }
    }
    out.sort_by_key(|(seq, _)| *seq);
    out.into_iter().map(|(_, c)| c).collect()
}

/// What one run met after `steps` were committed in the backlog: how it ended, whether the checkpoint moved, the changes it delivered and what it said.
struct Backlog {
    exit: i32,
    code: String,
    moved: bool,
    said: String,
    changes: Vec<(String, i64, String)>,
    rows_kept: i64,
}

impl Backlog {
    /// The verdict as one line.
    fn line(&self) -> String {
        format!(
            "exit={} code={} moved={} changes={:?}",
            self.exit, self.code, self.moved, self.changes
        )
    }
}

/// A table name that neither is nor contains `table`, and that `table` does not contain.
fn uncaptured_beside(table: &str) -> String {
    format!(
        "zz{}",
        table.split_once('_').map_or(table, |(_, rest)| rest)
    )
}

const REFUSED_UNDECODABLE: &str = "exit=5 code=SOURCE_CDC_UNDECODABLE moved=false changes=[]";
const REFUSED_TRUNCATED: &str = "exit=5 code=SOURCE_CDC_TRUNCATED moved=false changes=[]";
const RECREATED: &str = "CREATE TABLE rivet.{t} LIKE rivet.{t}_old";
const RECREATED_PLAIN: &str =
    "CREATE TABLE rivet.{t} (id INT PRIMARY KEY, name VARCHAR(20), pass VARCHAR(20))";
const BOTH_DELIVERED: &str = "exit=0 code=- moved=true changes=[(\"update\", 1, \"ann-before\"), (\"update\", 1, \"ann-after\")]";

const BEFORE_DELIVERED: &str = "exit=0 code=- moved=true changes=[(\"update\", 1, \"ann-before\")]";
const UPDATED_BEFORE: &str = "UPDATE rivet.{t} SET name = 'ann-before' WHERE id = 1";
const UPDATED_AFTER: &str = "UPDATE rivet.{t} SET name = 'ann-after' WHERE id = 1";
const UPDATED_AFTER_AS_NM: &str = "UPDATE rivet.{t} SET nm = 'ann-after' WHERE id = 1";
const GHOST_WITH_NM: &str =
    "CREATE TABLE rivet.{t}_new (id INT PRIMARY KEY, nm VARCHAR(20), pass VARCHAR(20))";
const GHOST_COPY: &str =
    "INSERT INTO rivet.{t}_new (id, nm, pass) SELECT id, name, pass FROM rivet.{t}";
const SWAP: &str = "RENAME TABLE rivet.{t} TO rivet.{t}_old, rivet.{t}_new TO rivet.{t}";

/// The seeded table `{t}` (an anchored run first, with or without `columns_from: grants`), `steps` committed on one root session, then one run; `{u}` is a name no captured table contains, `{t}_old`, `{t}_new`, `{u}_old` and `{u}_new` are dropped with it.
fn statements_in_the_backlog(label: &str, policy: bool, steps: &[&str]) -> Backlog {
    statements_in_the_backlog_spelled(label, policy, None, steps)
}

/// Another spelling of a table name that a server folding identifier case resolves to the same table.
#[derive(Clone, Copy)]
enum Spelling {
    Kelvin,
    DottedI,
    Upper,
}

impl Spelling {
    /// `table` with its first `k` as the Kelvin sign, its first `i` as the dotted capital I, or in capitals.
    fn of(self, table: &str) -> String {
        match self {
            Spelling::Kelvin => table.replacen('k', "\u{212a}", 1),
            Spelling::DottedI => table.replacen('i', "\u{130}", 1),
            Spelling::Upper => table.to_uppercase(),
        }
    }
}

/// Whether the server folds identifier case (`lower_case_table_names` other than 0).
fn the_server_folds_names() -> bool {
    let said: Option<i64> = root()
        .query_first("SELECT @@global.lower_case_table_names")
        .unwrap();
    said.unwrap() != 0
}

/// As `statements_in_the_backlog`; `{S}` in a step is the table name in `spelling`, a table of its own, created before the anchored run, when the server keeps case.
fn statements_in_the_backlog_spelled(
    label: &str,
    policy: bool,
    spelling: Option<Spelling>,
    steps: &[&str],
) -> Backlog {
    let (table, _table) = seeded(label);
    let spelled = spelling.map(|how| how.of(&table));
    let _spelled_aside = spelled.as_ref().map(|s| MysqlCdcTable(format!("`{s}`")));
    if let Some(s) = spelled.as_ref().filter(|_| !the_server_folds_names()) {
        seed_rows("rivet", &format!("`{s}`"));
    }
    let other = uncaptured_beside(&table);
    let _aside = (
        MysqlCdcTable(format!("{table}_old")),
        MysqlCdcTable(format!("{table}_new")),
        MysqlCdcTable(other.clone()),
        MysqlCdcTable(format!("{other}_old")),
        MysqlCdcTable(format!("{other}_new")),
    );
    let user = GrantUser::new(&table, "id, name");
    let mut rig = Rig::mysql_cdc(&table)
        .cdc("initial: snapshot")
        .a_failed_run_may_leave(
            &[Leftover::ResumePoint],
            "the run observes the readable columns at its start: that observed schema is stored before the refusal at the statement (known: src/state/schema.rs stores the baseline of a failed run)",
        )
        .no_oracle("the cell grades the changes the run delivered against the statements it committed, not the final table");
    if policy {
        rig = rig
            .source_url(&user.url())
            .source_line("columns_from: grants");
    }
    rig.run_ok();
    let before = checkpoint_at(&rig);
    let mut c = root();
    for sql in steps {
        c.query_drop(
            sql.replace("{t}", &table)
                .replace("{u}", &other)
                .replace("{S}", spelled.as_deref().unwrap_or("")),
        )
        .unwrap();
    }
    let out = rig.run();
    let parts: Vec<_> = parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect();
    let said = said_by(&out);
    Backlog {
        exit: out.status.code().unwrap_or(-1),
        code: said
            .split("[RIVET_")
            .nth(1)
            .and_then(|s| s.split(']').next())
            .unwrap_or("-")
            .to_string(),
        moved: checkpoint_at(&rig) != before,
        said,
        changes: named_changes(&parts),
        rows_kept: c
            .query_first(format!("SELECT COUNT(*) FROM rivet.{table}"))
            .ok()
            .flatten()
            .unwrap_or(-1),
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_table_swap_of_a_captured_table_is_refused_under_the_policy_mysql() {
    let run = statements_in_the_backlog(
        "cg_swap",
        true,
        &[
            UPDATED_BEFORE,
            GHOST_WITH_NM,
            GHOST_COPY,
            SWAP,
            UPDATED_AFTER_AS_NM,
        ],
    );
    assert_eq!(
        run.line(),
        "exit=5 code=SOURCE_CDC_UNDECODABLE moved=false changes=[]",
        "{}",
        run.said
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_table_swap_without_the_policy_is_met_as_before_mysql() {
    let run = statements_in_the_backlog(
        "cg_swap_off",
        false,
        &[
            UPDATED_BEFORE,
            GHOST_WITH_NM,
            GHOST_COPY,
            SWAP,
            UPDATED_AFTER_AS_NM,
        ],
    );
    assert_eq!(run.line(), BOTH_DELIVERED, "{}", run.said);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_rename_of_a_table_the_run_does_not_capture_is_met_as_before_under_the_policy_mysql() {
    let run = statements_in_the_backlog(
        "cg_swap_other",
        true,
        &[
            UPDATED_BEFORE,
            "CREATE TABLE rivet.{u}_new (id INT PRIMARY KEY)",
            "RENAME TABLE rivet.{u}_new TO rivet.{u}_old",
            "ALTER TABLE rivet.{u}_old RENAME TO rivet.{u}_new",
            UPDATED_AFTER,
        ],
    );
    assert_eq!(run.line(), BOTH_DELIVERED, "{}", run.said);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn an_alter_of_an_uncaptured_table_is_met_as_before_under_the_policy_mysql() {
    let run = statements_in_the_backlog(
        "cg_swap_idx",
        true,
        &[
            UPDATED_BEFORE,
            "CREATE TABLE rivet.{u} (id INT PRIMARY KEY, name VARCHAR(20))",
            "ALTER TABLE rivet.{u} ADD INDEX ix_name (name), ADD COLUMN `rename` INT",
            "ALTER TABLE rivet.{u} RENAME INDEX ix_name TO ix_other",
            "ALTER TABLE rivet.{u} COMMENT = 'rename to somewhere'",
            UPDATED_AFTER,
        ],
    );
    assert_eq!(run.line(), BOTH_DELIVERED, "{}", run.said);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_non_ascii_comment_on_an_uncaptured_table_is_met_as_before_under_the_policy_mysql() {
    let cjk = "\u{4e00}".repeat(64);
    let cyrillic = "\u{0446}".repeat(64);
    let created = format!(
        "CREATE TABLE rivet.{{u}} (id INT PRIMARY KEY, name VARCHAR(20)) COMMENT = '{cjk}'"
    );
    let altered = format!("ALTER TABLE rivet.{{u}} COMMENT = '{cyrillic}'");
    let run = statements_in_the_backlog(
        "cg_cjk",
        true,
        &[UPDATED_BEFORE, &created, &altered, UPDATED_AFTER],
    );
    assert_eq!(run.line(), BOTH_DELIVERED, "{}", run.said);
}

/// Every case `(policy, steps, verdict line)` run on its own seeded table, the verdicts compared in one assertion.
fn backlogs_end_as(label: &str, cases: &[(bool, &[&str], &str)]) {
    backlogs_end_as_saying(label, cases, "");
}

/// As `backlogs_end_as`, and every run's message holds `said_part`.
fn backlogs_end_as_saying(label: &str, cases: &[(bool, &[&str], &str)], said_part: &str) {
    let runs: Vec<_> = cases
        .iter()
        .enumerate()
        .map(|(i, (policy, steps, _))| {
            (
                steps,
                statements_in_the_backlog(&format!("{label}{i}"), *policy, steps),
            )
        })
        .collect();
    let got: Vec<String> = runs
        .iter()
        .map(|(steps, run)| format!("{}: {}", steps.join(" ; "), run.line()))
        .collect();
    let want: Vec<String> = cases
        .iter()
        .map(|(_, steps, want)| format!("{}: {want}", steps.join(" ; ")))
        .collect();
    assert_eq!(got, want);
    for (steps, run) in &runs {
        assert!(
            run.said.contains(said_part),
            "{}: the message lacks {said_part:?}: {}",
            steps.join(" ; "),
            run.said
        );
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_rename_of_a_captured_table_is_refused_in_every_form_under_the_policy_mysql() {
    let forms: [&[&str]; 9] = [
        &["ALTER TABLE rivet.{t} RENAME TO rivet.{t}_old", RECREATED],
        &["ALTER TABLE rivet.{t} RENAME AS rivet.{t}_old", RECREATED],
        &[
            "ALTER TABLE rivet.{t} ADD COLUMN k INT, RENAME rivet.{t}_old",
            RECREATED,
        ],
        &["RENAME TABLE rivet.{t} TO rivet.{t}_old", RECREATED],
        &["RENAME TABLE {t} TO {t}_old", RECREATED],
        &["RENAME TABLE `rivet`.`{t}` TO `rivet`.`{t}_old`", RECREATED],
        &[
            "/*!80000 RENAME TABLE rivet.{t} TO rivet.{t}_old */",
            RECREATED,
        ],
        &["RENAME TABLE rivet.{t} -- c\n TO rivet.{t}_old", RECREATED],
        &[
            "SET SESSION sql_mode = 'ANSI_QUOTES'",
            "RENAME TABLE \"rivet\".\"{t}\" TO \"rivet\".\"{t}_old\"",
            RECREATED,
        ],
    ];
    let cases: Vec<_> = forms
        .iter()
        .map(|f| (true, *f, REFUSED_UNDECODABLE))
        .collect();
    backlogs_end_as_saying("cg_rnf", &cases, "was renamed (RENAME TABLE");
    backlogs_end_as(
        "cg_rns",
        &[(
            true,
            &[
                "CREATE TABLE rivet.{t}_new (id INT PRIMARY KEY)",
                "RENAME TABLE rivet.{t}_new TO rivet.{t}_old, rivet.{t} TO rivet.{t}_new",
                "CREATE TABLE rivet.{t} LIKE rivet.{t}_old",
            ],
            REFUSED_UNDECODABLE,
        )],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_renamed_after_a_backslash_literal_under_no_backslash_escapes_is_refused_mysql() {
    backlogs_end_as_saying(
        "cg_nbe",
        &[(
            true,
            &[
                "SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'",
                UPDATED_BEFORE,
                "ALTER TABLE rivet.{t} COMMENT = 'x\\', RENAME COLUMN name TO nm",
                UPDATED_AFTER_AS_NM,
            ],
            REFUSED_UNDECODABLE,
        )],
        "had a column renamed",
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_renamed_after_a_backslash_name_under_ansi_quotes_is_refused_mysql() {
    backlogs_end_as_saying(
        "cg_ans",
        &[(
            true,
            &[
                "SET SESSION sql_mode = 'ANSI_QUOTES'",
                UPDATED_BEFORE,
                "ALTER TABLE rivet.{t} ADD COLUMN \"x\\\" INT, RENAME COLUMN name TO nm",
                UPDATED_AFTER_AS_NM,
            ],
            REFUSED_UNDECODABLE,
        )],
        "had a column renamed",
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn statements_whose_quotes_do_not_hide_a_rename_are_met_as_before_under_the_policy_mysql() {
    let no_backslash = "SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'";
    let ansi = "SET SESSION sql_mode = 'ANSI_QUOTES'";
    backlogs_end_as(
        "cg_qnb",
        &[
            (
                true,
                &[
                    no_backslash,
                    UPDATED_BEFORE,
                    "CREATE TABLE rivet.{u} (id INT PRIMARY KEY)",
                    "ALTER TABLE rivet.{u} COMMENT = 'x\\', ADD COLUMN extra INT",
                    UPDATED_AFTER,
                ],
                BOTH_DELIVERED,
            ),
            (
                true,
                &[
                    no_backslash,
                    UPDATED_BEFORE,
                    "CREATE TABLE rivet.{u} (id INT PRIMARY KEY)",
                    "ALTER TABLE rivet.{u} COMMENT = 'rename column name to nm'",
                    UPDATED_AFTER,
                ],
                BOTH_DELIVERED,
            ),
            (
                true,
                &[
                    UPDATED_BEFORE,
                    "ALTER TABLE rivet.{t} COMMENT = 'it\\'s', RENAME COLUMN name TO nm",
                    UPDATED_AFTER_AS_NM,
                ],
                REFUSED_UNDECODABLE,
            ),
            (
                true,
                &[
                    UPDATED_BEFORE,
                    "ALTER TABLE rivet.{t} COMMENT = \"a\\\"b\", RENAME COLUMN name TO nm",
                    UPDATED_AFTER_AS_NM,
                ],
                REFUSED_UNDECODABLE,
            ),
            (
                true,
                &[
                    ansi,
                    UPDATED_BEFORE,
                    "CREATE TABLE rivet.{u} (id INT PRIMARY KEY)",
                    "ALTER TABLE rivet.{u} ADD COLUMN \"x\\\" INT, ADD COLUMN extra INT",
                    UPDATED_AFTER,
                ],
                BOTH_DELIVERED,
            ),
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_truncate_drop_or_statement_write_with_double_quoted_names_is_refused_under_ansi_quotes_mysql()
{
    let ansi = "SET SESSION sql_mode = 'ANSI_QUOTES'";
    let statement = "SET SESSION binlog_format = 'STATEMENT'";
    backlogs_end_as(
        "cg_anq",
        &[
            (
                false,
                &[ansi, "TRUNCATE \"rivet\".\"{t}\""],
                REFUSED_TRUNCATED,
            ),
            (false, &[ansi, "TRUNCATE TABLE \"{t}\""], REFUSED_TRUNCATED),
            (
                false,
                &[
                    ansi,
                    "DROP TABLE IF EXISTS \"rivet\".\"{t}\"",
                    RECREATED_PLAIN,
                ],
                REFUSED_TRUNCATED,
            ),
            (
                false,
                &[
                    ansi,
                    statement,
                    "UPDATE \"rivet\".\"{t}\" SET name = 'stmt' WHERE id = 2",
                ],
                REFUSED_UNDECODABLE,
            ),
            (
                false,
                &[
                    ansi,
                    statement,
                    "INSERT INTO \"rivet\".\"{t}\" (id, name, pass) VALUES (9, 'q', 'r')",
                ],
                REFUSED_UNDECODABLE,
            ),
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_truncate_drop_or_statement_write_is_met_as_before_without_double_quoted_names_mysql() {
    let ansi = "SET SESSION sql_mode = 'ANSI_QUOTES'";
    let statement = "SET SESSION binlog_format = 'STATEMENT'";
    backlogs_end_as(
        "cg_anp",
        &[
            (false, &["TRUNCATE rivet.{t}"], REFUSED_TRUNCATED),
            (false, &["TRUNCATE TABLE `rivet`.`{t}`"], REFUSED_TRUNCATED),
            (
                false,
                &["DROP TABLE IF EXISTS `{t}`", RECREATED_PLAIN],
                REFUSED_TRUNCATED,
            ),
            (
                false,
                &[statement, "UPDATE rivet.{t} SET name = 'stmt' WHERE id = 2"],
                REFUSED_UNDECODABLE,
            ),
            (
                false,
                &[
                    statement,
                    "INSERT INTO `rivet`.`{t}` (id, name, pass) VALUES (9, 'q', 'r')",
                ],
                REFUSED_UNDECODABLE,
            ),
            (
                false,
                &[
                    UPDATED_BEFORE,
                    "CREATE TABLE rivet.{t}_old (id INT PRIMARY KEY)",
                    ansi,
                    "TRUNCATE \"rivet\".\"{t}_old\"",
                    "DROP TABLE \"rivet\".\"{t}_old\"",
                    UPDATED_AFTER,
                ],
                BOTH_DELIVERED,
            ),
        ],
    );
}

/// A statement naming the captured table, committed in the backlog by `statements(table)`: the run is refused by code naming the table and the re-baseline, and the re-baseline gets past it.
fn a_statement_naming_the_table_is_refused_and_the_remedy_gets_past_it(
    label: &str,
    statements: fn(&str) -> Vec<String>,
) {
    let (table, _guard, _user, rig) = anchored_under_the_policy(label);
    let steps = std::iter::once(UPDATED_BEFORE.replace("{t}", &table))
        .chain(statements(&table))
        .chain([UPDATED_AFTER.replace("{t}", &table)])
        .collect();
    a_refused_backlog_is_got_past_by_the_remedy(
        rig,
        steps,
        "RIVET_SOURCE_CDC_UNDECODABLE",
        &[format!("names the captured table `{table}`")],
        || {},
        |rig| {
            let (names, cells) = delivered(rig);
            assert!(
                cells.iter().any(|v| v == "ann-after")
                    && names.iter().all(|n| n == "id" || n == "name"),
                "the re-baselined stream reads the table as it is now: {names:?} {cells:?}"
            );
        },
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn an_add_index_on_a_captured_table_is_refused_naming_the_table_and_the_remedy_mysql() {
    a_statement_naming_the_table_is_refused_and_the_remedy_gets_past_it("cg_idx", |t| {
        vec![format!("ALTER TABLE rivet.{t} ADD INDEX ix_name (name)")]
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_statement_spelling_the_table_name_in_another_case_is_refused_mysql() {
    a_statement_naming_the_table_is_refused_and_the_remedy_gets_past_it("cg_case", |t| {
        let upper = t.to_uppercase();
        vec![
            format!("CREATE TABLE rivet.{upper}_UP (id INT)"),
            format!("DROP TABLE rivet.{upper}_UP"),
        ]
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn an_add_column_on_a_captured_table_is_refused_naming_the_table_and_the_remedy_mysql() {
    a_statement_naming_the_table_is_refused_and_the_remedy_gets_past_it("cg_addc", |t| {
        vec![format!("ALTER TABLE rivet.{t} ADD COLUMN nick VARCHAR(10)")]
    });
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_grant_on_an_uncaptured_table_does_not_stop_the_stream_mysql() {
    let run = statements_in_the_backlog(
        "cg_unrel",
        true,
        &[
            UPDATED_BEFORE,
            "CREATE TABLE rivet.{u} (id INT PRIMARY KEY)",
            "CREATE USER IF NOT EXISTS 'cg_unrel_u'@'%' IDENTIFIED BY 'x'",
            "GRANT SELECT ON rivet.{u} TO 'cg_unrel_u'@'%'",
            "REVOKE SELECT ON rivet.{u} FROM 'cg_unrel_u'@'%'",
            "DROP USER 'cg_unrel_u'@'%'",
            UPDATED_AFTER,
        ],
    );
    assert_eq!(run.line(), BOTH_DELIVERED, "{}", run.said);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_name_beginning_like_a_captured_table_is_refused_naming_the_captured_table_mysql() {
    let run = statements_in_the_backlog(
        "cg_f4m",
        true,
        &[
            "SET NAMES latin1",
            UPDATED_BEFORE,
            RAW_F4_CREATE,
            RUN_RAW[0],
            RUN_RAW[1],
            UPDATED_AFTER,
        ],
    );
    assert_eq!(run.line(), REFUSED_UNDECODABLE, "{}", run.said);
    assert!(
        run.said.contains("names the captured table `cg_f4m_")
            && run.said.contains("Re-baseline the stream"),
        "the over-stop names the captured table and the remedy:\n{}",
        run.said
    );
}

const RAW_F4_CREATE: &str =
    "SET @s = CONCAT('CREATE TABLE rivet.{t}', 0xE8, 'le (id INT PRIMARY KEY)')";
const RUN_RAW: [&str; 2] = ["PREPARE cg_raw FROM @s", "EXECUTE cg_raw"];
const STATEMENT_FORMAT: &str = "SET SESSION binlog_format = 'STATEMENT'";
const OTHER_TABLE: &str = "CREATE TABLE rivet.{u} (id INT PRIMARY KEY)";
const UPDATED_AFTER_AS_PW: &str = "UPDATE rivet.{t} SET pw = 'ann-after' WHERE id = 1";

fn raw(pieces: &str) -> String {
    format!("SET @s = CONCAT({pieces})")
}

fn leak(text: impl Into<String>) -> &'static str {
    Box::leak(text.into().into_boxed_str())
}

type Case = (bool, Vec<&'static str>, &'static str);

fn end_as(label: &str, built: &[Case]) {
    let cases: Vec<(bool, &[&str], &str)> =
        built.iter().map(|(p, s, w)| (*p, &s[..], *w)).collect();
    backlogs_end_as(label, &cases);
}

/// A backlog case that runs `before` after the character set `set`, then the statement `pieces`, then `tail`.
fn lexed_case(
    policy: bool,
    set: &str,
    before: &[&'static str],
    pieces: &str,
    tail: &[&'static str],
    want: &'static str,
) -> Case {
    let mut steps: Vec<&'static str> = vec![leak(set), UPDATED_BEFORE];
    steps.extend_from_slice(before);
    steps.push(leak(raw(pieces)));
    steps.extend_from_slice(&RUN_RAW);
    steps.extend_from_slice(tail);
    (policy, steps, want)
}

/// Leaves a statement no run can identify in the binlog: the nextest override `threads-required = "num-test-threads"` runs it alone.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_control_byte_after_a_dash_opens_a_comment_that_hides_a_statement_write_mysql() {
    let delete = "'DELETE rivet.{u} --', 0x01, '\n, rivet.{t} FROM rivet.{t} JOIN rivet.{u} ON rivet.{t}.id = rivet.{u}.id'";
    let before = [STATEMENT_FORMAT, OTHER_TABLE];
    end_as(
        "cg_ctl_w",
        &[
            lexed_case(
                true,
                "SET NAMES utf8mb4",
                &before,
                delete,
                &[],
                REFUSED_UNDECODABLE,
            ),
            lexed_case(
                true,
                "SET NAMES latin1",
                &before,
                delete,
                &[],
                REFUSED_UNDECODABLE,
            ),
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_change_after_a_comment_opened_by_a_control_byte_is_matched_to_its_own_column_mysql() {
    let change = "'ALTER TABLE rivet.{t} CHANGE id id INT --', 0x01, ', CHANGE name name VARCHAR(20) /*\n, CHANGE pass pw VARCHAR(20) COMMENT ''*/ -- '''";
    end_as(
        "cg_ctl_c",
        &[
            lexed_case(
                true,
                "SET NAMES utf8mb4",
                &[],
                change,
                &[UPDATED_AFTER_AS_PW],
                REFUSED_UNDECODABLE,
            ),
            lexed_case(
                true,
                "SET NAMES latin1",
                &[],
                change,
                &[UPDATED_AFTER_AS_PW],
                REFUSED_UNDECODABLE,
            ),
            lexed_case(
                true,
                "SET NAMES utf8mb4",
                &[],
                "'ALTER TABLE rivet.{t} CHANGE id id INT, CHANGE name name VARCHAR(20), CHANGE pass pw VARCHAR(20)'",
                &[UPDATED_AFTER_AS_PW],
                REFUSED_UNDECODABLE,
            ),
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_blank_byte_the_dash_comment_swallowed_does_not_hide_the_next_line_in_latin1_mysql() {
    let change = "'ALTER TABLE rivet.{t} CHANGE id id INT --', 0xA0, ', CHANGE name name VARCHAR(20)\n, CHANGE', 0xA0, 'pass pw VARCHAR(20)'";
    end_as(
        "cg_nbsp_c",
        &[lexed_case(
            true,
            "SET NAMES latin1",
            &[],
            change,
            &[UPDATED_AFTER_AS_PW],
            REFUSED_UNDECODABLE,
        )],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_name_beginning_like_a_captured_table_and_continuing_with_a_letter_byte_is_refused_as_that_table_mysql()
 {
    let case = |set: &str| -> Case {
        let mut steps: Vec<&'static str> = vec![leak(set), UPDATED_BEFORE];
        for sql in [
            "'CREATE TABLE rivet.{t}', 0xE8, 'le (id INT PRIMARY KEY)'",
            "'RENAME TABLE rivet.{t}', 0xE8, 'le TO rivet.{t}', 0xE8, 'le_old'",
            "'TRUNCATE rivet.{t}', 0xE8, 'le_old'",
            "'DROP TABLE rivet.{t}', 0xE8, 'le_old'",
        ] {
            steps.push(leak(raw(sql)));
            steps.extend_from_slice(&RUN_RAW);
        }
        steps.push(UPDATED_AFTER);
        (true, steps, REFUSED_UNDECODABLE)
    };
    end_as(
        "cg_f4",
        &[
            case("SET NAMES latin1"),
            case("SET NAMES cp1250"),
            case("SET NAMES cp1251"),
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn an_uncaptured_table_renamed_under_a_non_ascii_name_is_met_as_before_in_a_client_character_set_mysql()
 {
    let mut steps: Vec<&'static str> = vec!["SET NAMES latin1", UPDATED_BEFORE];
    for sql in [
        "'CREATE TABLE rivet.{u}', 0xE8, 'le (id INT PRIMARY KEY)'",
        "'RENAME TABLE rivet.{u}', 0xE8, 'le TO rivet.{u}', 0xE8, 'le_old'",
        "'DROP TABLE rivet.{u}', 0xE8, 'le_old'",
    ] {
        steps.push(leak(raw(sql)));
        steps.extend_from_slice(&RUN_RAW);
    }
    steps.push(UPDATED_AFTER);
    end_as("cg_f4u", &[(true, steps, BOTH_DELIVERED)]);
}

/// One backlog naming the captured table in another spelling: how the run ends on a server that folds identifier case and on one that keeps it.
struct Spelled {
    policy: bool,
    how: Spelling,
    steps: &'static [&'static str],
    folds: &'static str,
    keeps: &'static str,
    rows_when_folded: i64,
    says: &'static str,
}

/// Every case on its own seeded table, the verdicts and the rows the captured table keeps compared in one assertion; on a server that folds, the message holds `says`.
fn spellings_end_as(label: &str, cases: &[Spelled]) {
    let folds = the_server_folds_names();
    let runs: Vec<_> = cases
        .iter()
        .enumerate()
        .map(|(i, c)| {
            (
                c,
                statements_in_the_backlog_spelled(
                    &format!("{label}{i}"),
                    c.policy,
                    Some(c.how),
                    c.steps,
                ),
            )
        })
        .collect();
    let got: Vec<String> = runs
        .iter()
        .map(|(c, r)| {
            format!(
                "policy={} {}: {} rows_kept={}",
                c.policy,
                c.steps.join(" ; "),
                r.line(),
                r.rows_kept
            )
        })
        .collect();
    let want: Vec<String> = cases
        .iter()
        .map(|c| {
            format!(
                "policy={} {}: {} rows_kept={}",
                c.policy,
                c.steps.join(" ; "),
                if folds { c.folds } else { c.keeps },
                if folds { c.rows_when_folded } else { 3 }
            )
        })
        .collect();
    assert_eq!(got, want, "lower_case_table_names folds: {folds}");
    for (c, r) in runs.iter().filter(|_| folds) {
        assert!(
            r.said.contains(c.says),
            "{}: the message lacks {:?}: {}",
            c.steps.join(" ; "),
            c.says,
            r.said
        );
    }
}

const TRUNCATE_SPELLED: [&str; 3] = [UPDATED_BEFORE, "TRUNCATE TABLE rivet.`{S}`", UPDATED_AFTER];

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_truncate_spelled_with_a_character_the_server_folds_is_refused_under_the_policy_mysql() {
    let case = |how, keeps| Spelled {
        policy: true,
        how,
        steps: &TRUNCATE_SPELLED,
        folds: REFUSED_UNDECODABLE,
        keeps,
        rows_when_folded: 0,
        says: "names the captured table",
    };
    spellings_end_as(
        "cg_kin",
        &[
            case(Spelling::Kelvin, BOTH_DELIVERED),
            case(Spelling::DottedI, BOTH_DELIVERED),
            case(Spelling::Upper, REFUSED_UNDECODABLE),
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_column_renamed_or_swapped_under_a_folded_spelling_is_refused_mysql() {
    let case = |steps| Spelled {
        policy: true,
        how: Spelling::Kelvin,
        steps,
        folds: REFUSED_UNDECODABLE,
        keeps: BOTH_DELIVERED,
        rows_when_folded: 3,
        says: "names the captured table",
    };
    spellings_end_as(
        "cg_kre",
        &[
            Spelled {
                keeps: BEFORE_DELIVERED,
                ..case(&[
                    UPDATED_BEFORE,
                    "ALTER TABLE rivet.`{S}` RENAME COLUMN name TO nm",
                ])
            },
            case(&[
                UPDATED_BEFORE,
                "ALTER TABLE rivet.`{S}` RENAME COLUMN name TO pass, RENAME COLUMN pass TO name",
                UPDATED_AFTER,
            ]),
        ],
    );
}

/// A TRUNCATE of `{t}` with each `letter` written as `bytes` in the client character set `set`, on a table of its own when the server keeps case.
fn multibyte_truncate_case(set: &str, letter: char, bytes: &str, folds: bool) -> Case {
    let name = format!("REPLACE('{{t}}', '{letter}', 0x{bytes})");
    let mut steps: Vec<&'static str> = vec![leak(set), UPDATED_BEFORE];
    if !folds {
        steps.push(leak(raw(&format!(
            "'CREATE TABLE rivet.', {name}, ' (id INT PRIMARY KEY)'"
        ))));
        steps.extend_from_slice(&RUN_RAW);
    }
    steps.push(leak(raw(&format!("'TRUNCATE TABLE rivet.', {name}"))));
    steps.extend_from_slice(&RUN_RAW);
    if !folds {
        steps.push(leak(raw(&format!("'DROP TABLE rivet.', {name}"))));
        steps.extend_from_slice(&RUN_RAW);
    }
    steps.push(UPDATED_AFTER);
    let want = if folds {
        REFUSED_UNDECODABLE
    } else {
        BOTH_DELIVERED
    };
    (true, steps, want)
}

/// Leaves a statement no run can identify in the binlog: the nextest override `threads-required = "num-test-threads"` runs it alone.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_truncate_spelled_with_a_multibyte_character_the_server_folds_is_refused_under_the_policy_mysql()
 {
    let folds = the_server_folds_names();
    let cases = [
        multibyte_truncate_case("SET NAMES gb18030", 'k', "8136BF32", folds),
        multibyte_truncate_case("SET NAMES gb18030", 'i', "81309032", folds),
        multibyte_truncate_case("SET NAMES ujis", 'i', "8FAAC4", folds),
    ];
    end_as("cg_mbki", &cases);
}

/// Leaves a statement no run can identify in the binlog: the nextest override `threads-required = "num-test-threads"` runs it alone.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn an_uncaptured_table_renamed_under_a_two_byte_name_whose_trail_is_a_backtick_is_met_as_before_mysql()
 {
    let mut steps: Vec<&'static str> = vec!["SET NAMES gbk", UPDATED_BEFORE];
    for sql in [
        "'CREATE TABLE rivet.{u}x', 0x8160, 'y (id INT PRIMARY KEY)'",
        "'RENAME TABLE rivet.{u}x', 0x8160, 'y TO rivet.{u}z', 0x8160",
        "'DROP TABLE rivet.{u}z', 0x8160",
    ] {
        steps.push(leak(raw(sql)));
        steps.extend_from_slice(&RUN_RAW);
    }
    steps.push(UPDATED_AFTER);
    end_as("cg_gbk", &[(true, steps, BOTH_DELIVERED)]);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_database_dropped_with_a_captured_table_is_met_as_before_by_the_unreadable_columns_stop_mysql()
{
    let db = OtherDb(unique_name("cg_dbd"));
    let table = unique_name("cg_dbt");
    root()
        .query_drop(format!("CREATE DATABASE `{}`", db.0))
        .unwrap();
    seed_rows(&db.0, &table);
    let (decoy, _decoy) = seeded("cg_dbc");
    let user = GrantUser::new_in(&db.0, &table, "id, name");
    root()
        .query_drop(format!(
            "GRANT SELECT (id) ON rivet.{decoy} TO '{}'@'%'",
            user.0
        ))
        .unwrap();
    let rig = Rig::mysql_cdc(&format!("{}.{table}", db.0))
        .source_url(&user.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants");
    rig.run_ok();
    let steps = vec![
        format!(
            "UPDATE {}.{table} SET name = 'ann-before' WHERE id = 1",
            db.0
        ),
        format!("DROP DATABASE `{}`", db.0),
    ];
    a_stopped_backlog_is_got_past_by_the_remedy(
        rig,
        steps,
        Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1),
        &[],
        || {
            root()
                .query_drop(format!("CREATE DATABASE `{}`", db.0))
                .unwrap();
            seed_rows(&db.0, &table);
            root()
                .query_drop(format!(
                    "GRANT SELECT (id, name) ON {}.{table} TO '{}'@'%'",
                    db.0, user.0
                ))
                .unwrap();
        },
        |rig| {
            let parts: Vec<_> = parquet_dirs(&rig.out_dir())
                .iter()
                .flat_map(|d| read_all_parts(d))
                .collect();
            assert!(!parts.is_empty(), "the re-baselined stream delivers parts");
            let (names, cells) = delivered(rig);
            assert!(
                cells.iter().any(|v| v == "ann") && names.iter().all(|n| n == "id" || n == "name"),
                "the re-baselined stream reads the re-created table: {names:?} {cells:?}"
            );
        },
    );
}
