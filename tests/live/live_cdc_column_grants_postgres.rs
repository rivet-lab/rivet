//! Live `source.columns_from: grants` (PostgreSQL): a column-grant role delivers only the columns it may SELECT.

use std::path::Path;

use crate::common::*;

const SECRET: &str = "SECRET-";

fn root() -> postgres::Client {
    postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("connect as the superuser")
}

/// A replication role that may SELECT only `columns` of `table`; dropped with the guard.
struct GrantRole(String);

impl GrantRole {
    fn new(table: &str, columns: &str) -> Self {
        let role = unique_name("cg_r");
        root()
            .batch_execute(&format!(
                "CREATE ROLE {role} LOGIN REPLICATION PASSWORD 'pw'; \
                 GRANT SELECT ({columns}) ON {table} TO {role}"
            ))
            .unwrap();
        Self(role)
    }

    fn on_the_table(table: &str) -> Self {
        let role = unique_name("cg_r");
        root()
            .batch_execute(&format!(
                "CREATE ROLE {role} LOGIN REPLICATION PASSWORD 'pw'; \
                 GRANT SELECT ON {table} TO {role}"
            ))
            .unwrap();
        Self(role)
    }

    fn url(&self) -> String {
        POSTGRES_CDC_URL.replace("rivet:rivet@", &format!("{}:pw@", self.0))
    }
}

impl Drop for GrantRole {
    fn drop(&mut self) {
        let _ = root().batch_execute(&format!(
            "DROP OWNED BY {role}; DROP ROLE IF EXISTS {role}",
            role = self.0
        ));
    }
}

/// `table(id, name, pass)` holding `(1,'ann',SECRET)`, `(2,'bob',SECRET)`, `(3,'cid',SECRET)`.
fn seeded(label: &str, identity: &str) -> (String, PgTable) {
    let table = unique_name(label);
    root()
        .batch_execute(&format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT, pass TEXT); \
             ALTER TABLE {table} REPLICA IDENTITY {identity}; \
             INSERT INTO {table} VALUES (1, 'ann', '{SECRET}1'), (2, 'bob', '{SECRET}2'), (3, 'cid', '{SECRET}3')"
        ))
        .unwrap();
    let guard = PgTable::adopt_on(POSTGRES_CDC_URL, table.clone());
    (table, guard)
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

/// The server's current WAL position.
fn wal_position() -> String {
    root()
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0)
}

/// Whether the server's `confirmed_flush_lsn` of `slot` is past `before`.
fn confirmed_flush_lsn_moved_past(slot: &str, before: &str) -> bool {
    root()
        .query_one(
            "SELECT confirmed_flush_lsn > $1::text::pg_lsn FROM pg_replication_slots WHERE slot_name = $2",
            &[&before, &slot],
        )
        .unwrap()
        .get(0)
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

/// A snapshot-then-stream rig on `role` with `columns_from: grants`; graded by the cell, since the oracle reads the source with this account.
fn grant_rig(table: &str, slot: &str, role: &GrantRole) -> Rig {
    Rig::pg_cdc(table, slot)
        .source_url(&role.url())
        .cdc("initial: snapshot")
        .source_line("columns_from: grants")
        .no_oracle("the oracle reads the source through the rig's account, which may not SELECT the hidden column; the cell grades the delivery against the values it seeded")
}

type Change = (String, i64, String);

/// A run under a column grant on `identity`: the snapshot and the changes are delivered without the hidden column, and the changes are `expected`.
fn a_column_grant_role_delivers_only_what_it_may_read(identity: &str, expected: Vec<Change>) {
    let (table, _table) = seeded("cg_pg", identity);
    let slot = unique_name("cg_pg_slot");
    let _slot = Slot::new(slot.clone());
    let role = GrantRole::new(&table, "id, name");
    let rig = grant_rig(&table, &slot, &role);
    let check = rig.cli(&["check"]);
    assert!(
        check.status.success(),
        "check under a column grant:\n{}",
        said_by(&check)
    );
    rig.run_ok();
    let mut c = root();
    for sql in [
        format!("UPDATE {table} SET pass = '{SECRET}a' WHERE id = 1"),
        format!("UPDATE {table} SET pass = '{SECRET}b', name = 'ann2' WHERE id = 1"),
        format!("INSERT INTO {table} VALUES (4, 'dee', '{SECRET}ins')"),
        format!("DELETE FROM {table} WHERE id = 2"),
    ] {
        c.batch_execute(&sql).unwrap();
    }
    rig.run_ok();

    let parts = parts_under(&rig.out_dir());
    let cells = assert_no_hidden(&parts, "cdc");
    assert!(
        ["ann", "bob", "cid", "ann2", "dee"]
            .iter()
            .all(|n| cells.iter().any(|v| v == n)),
        "the snapshot and the change parts were both read: {cells:?}"
    );
    assert_eq!(changes(&parts), expected);
}

fn change(op: &str, id: i64, name: &str) -> Change {
    (op.into(), id, name.into())
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc (wal_level=logical)"]
fn a_column_grant_role_delivers_no_hidden_column_by_name_or_value_in_cdc_postgres() {
    a_column_grant_role_delivers_only_what_it_may_read(
        "DEFAULT",
        vec![
            change("update", 1, "ann"),
            change("update", 1, "ann2"),
            change("insert", 4, "dee"),
            change("delete", 2, ""),
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc (wal_level=logical)"]
fn an_update_of_hidden_columns_only_is_not_delivered_under_a_full_row_image_in_cdc_postgres() {
    a_column_grant_role_delivers_only_what_it_may_read(
        "FULL",
        vec![
            change("update", 1, "ann2"),
            change("insert", 4, "dee"),
            change("delete", 2, "bob"),
        ],
    );
}

/// `(op, id)` of every change in `parts`, in capture order.
fn op_ids(parts: &[arrow::record_batch::RecordBatch]) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    for b in parts.iter().filter(|b| b.column_by_name("__op").is_some()) {
        let s = |n: &str, i: usize| {
            arrow::util::display::array_value_to_string(b.column_by_name(n).unwrap(), i).unwrap()
        };
        for i in 0..b.num_rows() {
            out.push((
                s("__seq", i).parse::<i64>().unwrap(),
                (s("__op", i), s("id", i).parse().unwrap()),
            ));
        }
    }
    out.sort_by_key(|(seq, _)| *seq);
    out.into_iter().map(|(_, c)| c).collect()
}

/// Park `rig`'s next run after its stream opened, call `during` once it is parked, and reap the run.
fn run_parked_after_open(rig: &Rig, during: impl FnOnce()) -> std::process::Output {
    let marker = rig.out_dir().join("..").join("cdc_after_open.paused");
    let marker_env = marker.display().to_string();
    let mut child = rig.spawn_args_env(
        &[],
        &[
            ("RIVET_TEST_PAUSE_AT", "cdc_after_open:3000"),
            ("RIVET_TEST_PAUSE_MARKER", &marker_env),
        ],
    );
    let opened = std::time::Instant::now();
    while !marker.is_file() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "the run exited before it parked:\n{}",
            child.stderr_so_far()
        );
        assert!(
            opened.elapsed() < std::time::Duration::from_secs(60),
            "the run never parked"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    std::fs::remove_file(&marker).expect("clear the park marker");
    during();
    child.wait_with_output().expect("reap the parked run")
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc (wal_level=logical)"]
fn a_revoke_during_a_run_is_refused_by_code_and_the_next_run_delivers_the_row_without_the_column_in_cdc_postgres()
 {
    let (table, _table) = seeded("cg_pg_rv", "DEFAULT");
    let slot = unique_name("cg_pg_rv_slot");
    let _slot = Slot::new(slot.clone());
    let role = GrantRole::new(&table, "id, name");
    let rig = grant_rig(&table, &slot, &role);
    rig.run_ok();
    let before = wal_position();
    root()
        .batch_execute(&format!(
            "INSERT INTO {table} VALUES (4, 'dee', '{SECRET}ins')"
        ))
        .unwrap();
    let out = run_parked_after_open(&rig, || {
        root()
            .batch_execute(&format!("REVOKE SELECT (name) ON {table} FROM {}", role.0))
            .unwrap();
    });
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_GRANTS_CHANGED", 2));
    assert!(
        said_by(&out).contains("lost `name`"),
        "the refusal names the lost column:\n{}",
        said_by(&out)
    );
    let parts = parts_under(&rig.out_dir());
    assert_no_hidden(&parts, "after the refusal");
    assert!(
        op_ids(&parts).is_empty(),
        "nothing of the refused batch was delivered: {:?}",
        op_ids(&parts)
    );
    rig.run_ok();
    let parts = parts_under(&rig.out_dir());
    let cells = assert_no_hidden(&parts, "after the re-run");
    assert!(
        confirmed_flush_lsn_moved_past(&slot, &before),
        "the re-run consumed the refused batch"
    );
    assert_eq!(op_ids(&parts), [("insert".to_string(), 4)]);
    assert!(
        !cells.iter().any(|v| v == "dee"),
        "the revoked column is not delivered by the re-run: {cells:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc (wal_level=logical)"]
fn a_key_the_role_cannot_read_is_refused_by_code_before_anything_is_written_in_cdc_postgres() {
    let (table, _table) = seeded("cg_pg_key", "DEFAULT");
    let slot = unique_name("cg_pg_key_slot");
    let _slot = Slot::new(slot.clone());
    let role = GrantRole::new(&table, "name, pass");
    let rig = grant_rig(&table, &slot, &role);
    let out = rig.run_with_envs(&[]);
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
    assert!(
        parquet_dirs(&rig.out_dir()).is_empty(),
        "nothing was written"
    );
    let held = root()
        .query(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap();
    assert_eq!(
        held.len(),
        0,
        "the refusal came before the slot was created"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc (wal_level=logical)"]
fn a_role_that_may_read_every_column_delivers_what_the_run_without_the_policy_does_in_cdc_postgres()
{
    let delivered = |policy: bool| {
        let (table, _table) = seeded("cg_pg_all", "FULL");
        let slot = unique_name("cg_pg_all_slot");
        let _slot = Slot::new(slot.clone());
        let role = GrantRole::on_the_table(&table);
        let mut rig = Rig::pg_cdc(&table, &slot)
            .source_url(&role.url())
            .cdc("initial: snapshot");
        if policy {
            rig = rig.source_line("columns_from: grants");
        }
        rig.run_ok();
        let before = wal_position();
        let mut c = root();
        c.batch_execute(&format!(
            "UPDATE {table} SET pass = '{SECRET}a' WHERE id = 1; \
             UPDATE {table} SET name = 'ann2' WHERE id = 1; \
             DELETE FROM {table} WHERE id = 2"
        ))
        .unwrap();
        rig.run_ok();
        assert!(
            confirmed_flush_lsn_moved_past(&slot, &before),
            "the changes were consumed"
        );
        let parts = parts_under(&rig.out_dir());
        let (mut names, _) = names_and_cells(&parts);
        names.sort();
        names.dedup();
        let mut cells: Vec<String> = Vec::new();
        for b in &parts {
            for n in ["id", "name", "pass"] {
                if let Some(col) = b.column_by_name(n) {
                    for i in 0..b.num_rows() {
                        cells.push(format!(
                            "{n}={}",
                            arrow::util::display::array_value_to_string(col, i).unwrap()
                        ));
                    }
                }
            }
        }
        cells.sort();
        (names, cells, op_ids(&parts))
    };
    let without = delivered(false);
    let with = delivered(true);
    assert!(
        with.0.iter().any(|n| n == "pass"),
        "a table-level grant reads every column: {:?}",
        with.0
    );
    assert_eq!(
        with, without,
        "the policy changes nothing for a full reader"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc (wal_level=logical)"]
fn a_spilled_transaction_delivers_no_hidden_column_in_cdc_postgres() {
    let (table, _table) = seeded("cg_pg_sp", "DEFAULT");
    let slot = unique_name("cg_pg_sp_slot");
    let _slot = Slot::new(slot.clone());
    let role = GrantRole::new(&table, "id, name");
    let rig = grant_rig(&table, &slot, &role);
    rig.run_ok();
    let before = wal_position();
    root()
        .batch_execute(&format!(
            "INSERT INTO {table} SELECT g, 'n' || g, '{SECRET}' || g FROM generate_series(10, 21) g"
        ))
        .unwrap();
    let out = rig.run_with_envs(&[("RIVET_CDC_MAX_TX_ROWS", "4"), ("RIVET_CDC_SPILL_DIR", "1")]);
    assert!(out.status.success(), "the run:\n{}", said_by(&out));
    assert!(
        said_by(&out).contains(" from disk ("),
        "the transaction was spilled and replayed:\n{}",
        said_by(&out)
    );
    let parts = parts_under(&rig.out_dir());
    let cells = assert_no_hidden(&parts, "spill");
    let ids: Vec<i64> = op_ids(&parts).into_iter().map(|(_, id)| id).collect();
    assert_eq!(ids, (10..=21).collect::<Vec<i64>>());
    assert!(cells.iter().any(|v| v == "n15"), "{cells:?}");
    assert!(
        confirmed_flush_lsn_moved_past(&slot, &before),
        "the spilled transaction was consumed"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc (wal_level=logical)"]
fn an_update_of_hidden_columns_only_advances_the_slot_in_cdc_postgres() {
    let (table, _table) = seeded("cg_pg_adv", "FULL");
    let slot = unique_name("cg_pg_adv_slot");
    let _slot = Slot::new(slot.clone());
    let role = GrantRole::new(&table, "id, name");
    let rig = grant_rig(&table, &slot, &role);
    rig.run_ok();
    let mut c = root();
    let before: String = c
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);
    c.batch_execute(&format!(
        "UPDATE {table} SET pass = '{SECRET}x' WHERE id = 1"
    ))
    .unwrap();
    rig.run_ok();
    let moved: bool = c
        .query_one(
            "SELECT confirmed_flush_lsn > $1::text::pg_lsn FROM pg_replication_slots WHERE slot_name = $2",
            &[&before, &slot],
        )
        .unwrap()
        .get(0);
    assert!(moved, "the slot moved past the update of a hidden column");
    assert!(op_ids(&parts_under(&rig.out_dir())).is_empty());
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc (wal_level=logical)"]
fn an_unchanged_toast_hidden_column_poisons_nothing_in_cdc_postgres() {
    let table = unique_name("cg_pg_toast");
    root()
        .batch_execute(&format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT, pass TEXT); \
             ALTER TABLE {table} ALTER COLUMN pass SET STORAGE EXTERNAL; \
             INSERT INTO {table} VALUES (1, 'ann', '{SECRET}' || repeat(md5(random()::text), 200))"
        ))
        .unwrap();
    let _table = PgTable::adopt_on(POSTGRES_CDC_URL, table.clone());
    let slot = unique_name("cg_pg_slot");
    let _slot = Slot::new(slot.clone());
    let role = GrantRole::new(&table, "id, name");
    let rig = grant_rig(&table, &slot, &role);
    rig.run_ok();
    let before = wal_position();
    root()
        .batch_execute(&format!("UPDATE {table} SET name = 'ann2' WHERE id = 1"))
        .unwrap();
    rig.run_ok();
    assert!(
        confirmed_flush_lsn_moved_past(&slot, &before),
        "the update was consumed"
    );
    let parts = parts_under(&rig.out_dir());
    assert_no_hidden(&parts, "cdc");
    assert_eq!(changes(&parts), vec![change("update", 1, "ann2")]);
}
