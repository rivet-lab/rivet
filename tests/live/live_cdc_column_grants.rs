//! Live `cdc.columns_from: grants` (MySQL): a column-grant account delivers only
//! the columns it may SELECT — by name and by value, in the snapshot and the stream.

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
        let user = unique_name("cg_u");
        let mut c = root();
        c.query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
            .unwrap();
        c.query_drop(format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'"))
            .unwrap();
        c.query_drop(format!(
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%'"
        ))
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
        .cdc("columns_from: grants");

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
        .cdc("columns_from: grants")
        .run_expect_fail();
    assert!(
        said.contains("RIVET_SOURCE_CDC_COLUMNS_UNREADABLE") && said.contains("`id`"),
        "an unreadable key is refused by code and named:\n{said}"
    );
}
