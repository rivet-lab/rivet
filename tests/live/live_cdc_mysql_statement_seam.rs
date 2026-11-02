//! Live MySQL CDC without `columns_from: grants`: a statement that removes a captured table's rows without row events stops the stream, spelled any way the server resolves to the table.

use std::path::Path;

use crate::common::*;
use mysql::prelude::Queryable as _;

const STAND_VAR: &str = "RIVET_TEST_MYSQL_STMT_URL";
const KELVIN: &str = "\u{212A}";
const DOTTED_I: &str = "\u{0130}";

/// The stand under test: `RIVET_TEST_MYSQL_STMT_URL` when set, else the CDC server (keeps case) or the gate's folding `mysql-stmt` stand.
fn base_url(folding: bool) -> String {
    std::env::var(STAND_VAR).unwrap_or_else(|_| {
        if folding {
            gate_profile_url("mysql-stmt").to_string()
        } else {
            MYSQL_CDC_URL
                .replace("rivet:rivet@", "root:rivet@")
                .replace("/rivet", "")
        }
    })
}

fn admin(base: &str) -> mysql::Conn {
    mysql::Conn::new(mysql::Opts::from_url(base).unwrap()).unwrap_or_else(|e| {
        panic!("live test requires the stand at {base} ({e}); a folding one: docker compose --profile mysql-stmt up -d --wait mysql-stmt-84-lctn1")
    })
}

fn rig_base_url(base: &str) -> String {
    if !base.contains("root@") {
        return base.to_string();
    }
    let mut c = admin(base);
    c.query_drop("CREATE USER IF NOT EXISTS 'seam_rig'@'%' IDENTIFIED BY 'rivet'")
        .unwrap();
    c.query_drop("GRANT ALL ON *.* TO 'seam_rig'@'%'").unwrap();
    base.replace("root@", "seam_rig:rivet@")
}

/// A database of its own on the stand under test, dropped with the value.
struct Seam {
    base: String,
    db: String,
    lctn: i64,
}

impl Seam {
    fn new(label: &str) -> Self {
        Self::on(base_url(false), label)
    }

    fn folding(label: &str) -> Self {
        Self::on(base_url(true), label)
    }

    fn on(base: String, label: &str) -> Self {
        let db = unique_name(label).to_lowercase();
        let mut c = admin(&base);
        c.query_drop(format!("CREATE DATABASE `{db}`")).unwrap();
        let lctn: i64 = c
            .query_first("SELECT @@global.lower_case_table_names")
            .unwrap()
            .unwrap();
        for ddl in [
            "CREATE TABLE kinds (id INT PRIMARY KEY, v INT)",
            "CREATE TABLE other (id INT PRIMARY KEY, v INT)",
            "CREATE TABLE swap (id INT PRIMARY KEY, v INT)",
            "CREATE TABLE parts (id INT NOT NULL, v INT, PRIMARY KEY (id)) PARTITION BY RANGE (id) \
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (100), \
             PARTITION pmax VALUES LESS THAN MAXVALUE)",
            "CREATE TABLE otherparts (id INT NOT NULL, v INT, PRIMARY KEY (id)) PARTITION BY RANGE (id) \
             (PARTITION p0 VALUES LESS THAN (10), PARTITION pmax VALUES LESS THAN MAXVALUE)",
        ] {
            c.query_drop(format!("USE `{db}`")).unwrap();
            c.query_drop(ddl).unwrap();
        }
        Self { base, db, lctn }
    }

    fn conn(&self) -> mysql::Conn {
        let mut c = admin(&self.base);
        c.query_drop("SET NAMES utf8mb4").unwrap();
        c.query_drop(format!("USE `{}`", self.db)).unwrap();
        c
    }

    fn url(&self) -> String {
        format!("{}/{}", rig_base_url(&self.base), self.db)
    }

    fn rig(&self, table: &str, dir: &Path) -> Rig {
        Rig::mysql_cdc(&format!("{}.{table}", self.db))
            .source_url(&self.url())
            .checkpoint_path(dir.join(format!("{table}.ckpt")))
            .dest_path(dir.join(format!("out_{table}")))
    }

    fn folds(&self) -> bool {
        self.lctn != 0
    }
}

/// A database spelled in another case than the captured one, dropped with the value.
struct Shadow {
    base: String,
    name: String,
}

impl Shadow {
    fn new(base: &str, name: &str) -> Self {
        let mut c = admin(base);
        c.query_drop(format!("DROP DATABASE IF EXISTS `{name}`"))
            .unwrap();
        c.query_drop(format!("CREATE DATABASE `{name}`")).unwrap();
        c.query_drop(format!("USE `{name}`")).unwrap();
        for t in [
            "KINDS".to_string(),
            format!("{KELVIN}inds"),
            "kinds".to_string(),
        ] {
            c.query_drop(format!("CREATE TABLE `{t}` (id INT PRIMARY KEY, v INT)"))
                .unwrap();
        }
        Self {
            base: base.to_string(),
            name: name.to_string(),
        }
    }
}

impl Drop for Shadow {
    fn drop(&mut self) {
        let _ = admin(&self.base).query_drop(format!("DROP DATABASE IF EXISTS `{}`", self.name));
    }
}

impl Drop for Seam {
    fn drop(&mut self) {
        let _ = admin(&self.base).query_drop(format!("DROP DATABASE IF EXISTS `{}`", self.db));
    }
}

fn delivered_ids(rig: &Rig) -> Vec<i64> {
    let mut ids: Vec<i64> = read_cdc_changes(&rig.out_dir())
        .iter()
        .map(|c| c.id)
        .collect();
    ids.sort_unstable();
    ids
}

/// The statements `steps` run after the baseline, then one run that must stop with `code` and exit 5.
fn stops(seam: &Seam, table: &str, rows: &str, steps: &[String], code: &str) {
    stops_then(seam, table, rows, steps, code, |_| {});
}

/// [`stops`], then `after` runs on the rig that was stopped.
fn stops_then(
    seam: &Seam,
    table: &str,
    rows: &str,
    steps: &[String],
    code: &str,
    after: impl FnOnce(&mut Rig),
) {
    let d = tempfile::tempdir().unwrap();
    let mut rig = seam.rig(table, d.path());
    rig.run_ok();
    let mut c = seam.conn();
    c.query_drop(format!("INSERT INTO `{table}` VALUES {rows}"))
        .unwrap();
    for s in steps {
        c.query_drop(s).unwrap();
    }
    let left: i64 = c
        .query_first(format!("SELECT count(*) FROM `{table}`"))
        .ok()
        .flatten()
        .unwrap_or(-1);
    let out = rig.run();
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !out.status.success(),
        "the run exited 0 with {steps:?} in the log; the source now holds {left} rows of `{table}` \
         (-1 when the table is gone), the delivered stream holds inserts of ids {:?} and nothing that retracts them",
        delivered_ids(&rig)
    );
    assert_eq!(out.status.code(), Some(5), "{steps:?}: {said}");
    assert!(
        said.contains(code),
        "{steps:?} must be refused as {code}: {said}"
    );
    assert!(
        said.contains(REBASELINE_REMEDY),
        "{steps:?} must name the re-baseline: {said}"
    );
    after(&mut rig);
}

/// The statements `steps` touch only tables the export does not capture: the run delivers the captured table's own change.
fn passes(seam: &Seam, table: &str, steps: &[String]) {
    let d = tempfile::tempdir().unwrap();
    let rig = seam.rig(table, d.path());
    rig.run_ok();
    let mut c = seam.conn();
    c.query_drop(format!("INSERT INTO `{table}` VALUES (1,10),(2,20)"))
        .unwrap();
    for s in steps {
        c.query_drop(s).unwrap();
    }
    rig.run_ok();
    assert_eq!(
        delivered_ids(&rig),
        vec![1, 2],
        "{steps:?}: the captured table's own rows are delivered"
    );
}

fn recreated_database(db: &str, drop: &str) -> Vec<String> {
    vec![
        drop.to_string(),
        format!("CREATE DATABASE `{db}`"),
        format!("USE `{db}`"),
        CREATE_KINDS.to_string(),
    ]
}

fn needs_a_folding_server(seam: &Seam) -> bool {
    if !seam.folds() {
        skip_live(&format!(
            "lower_case_table_names=0: another spelling is another table; {STAND_VAR} points at a stand that keeps case"
        ));
    }
    seam.folds()
}

const CREATE_KINDS: &str = "CREATE TABLE kinds (id INT PRIMARY KEY, v INT)";
const TRUNCATED: &str = "RIVET_SOURCE_CDC_TRUNCATED";
const UNDECODABLE: &str = "RIVET_SOURCE_CDC_UNDECODABLE";

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc, or RIVET_TEST_MYSQL_STMT_URL at a statement-truth stand"]
fn a_truncate_spelled_in_another_case_stops_the_stream_on_a_folding_server_mysql() {
    let s = Seam::folding("seam_tc");
    if !needs_a_folding_server(&s) {
        return;
    }
    let db = s.db.to_uppercase();
    stops(
        &s,
        "kinds",
        "(1,10),(2,20)",
        &[format!("TRUNCATE TABLE `{db}`.`KINDS`")],
        TRUNCATED,
    );
    let t = Seam::folding("seam_tu");
    stops(
        &t,
        "kinds",
        "(1,10),(2,20)",
        &["TRUNCATE KINDS".to_string()],
        TRUNCATED,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc, or RIVET_TEST_MYSQL_STMT_URL at a statement-truth stand"]
fn a_truncate_spelled_with_a_character_the_server_folds_stops_the_stream_mysql() {
    let s = Seam::folding("seam_tk");
    if !needs_a_folding_server(&s) {
        return;
    }
    stops(
        &s,
        "kinds",
        "(1,10),(2,20)",
        &[format!("TRUNCATE TABLE `{}`.`{KELVIN}inds`", s.db)],
        TRUNCATED,
    );
    let t = Seam::folding("seam_ti");
    stops(
        &t,
        "kinds",
        "(1,10),(2,20)",
        &[format!("TRUNCATE TABLE `{}`.`k{DOTTED_I}nds`", t.db)],
        TRUNCATED,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc, or RIVET_TEST_MYSQL_STMT_URL at a statement-truth stand"]
fn a_drop_spelled_in_another_case_stops_the_stream_on_a_folding_server_mysql() {
    let s = Seam::folding("seam_dc");
    if !needs_a_folding_server(&s) {
        return;
    }
    stops(
        &s,
        "kinds",
        "(1,10),(2,20)",
        &[
            format!("DROP TABLE `{}`.`KINDS`", s.db.to_uppercase()),
            CREATE_KINDS.to_string(),
        ],
        TRUNCATED,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc, or RIVET_TEST_MYSQL_STMT_URL at a statement-truth stand"]
fn a_statement_logged_write_in_another_case_stops_the_stream_on_a_folding_server_mysql() {
    let s = Seam::folding("seam_wc");
    if !needs_a_folding_server(&s) {
        return;
    }
    stops(
        &s,
        "kinds",
        "(1,10),(2,20)",
        &[
            "SET SESSION binlog_format = 'STATEMENT'".to_string(),
            "DELETE FROM KINDS WHERE id = 1".to_string(),
        ],
        UNDECODABLE,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc, or RIVET_TEST_MYSQL_STMT_URL at a statement-truth stand"]
fn a_database_dropped_with_a_captured_table_stops_the_stream_mysql() {
    for (label, verb) in [("seam_d1", "DATABASE"), ("seam_d2", "SCHEMA")] {
        let s = Seam::new(label);
        stops(
            &s,
            "kinds",
            "(1,10),(2,20)",
            &recreated_database(&s.db, &format!("DROP {verb} `{}`", s.db)),
            TRUNCATED,
        );
    }
    let s = Seam::folding("seam_d3");
    if s.folds() {
        stops(
            &s,
            "kinds",
            "(1,10),(2,20)",
            &recreated_database(
                &s.db,
                &format!("DROP DATABASE IF EXISTS `{}`", s.db.to_uppercase()),
            ),
            TRUNCATED,
        );
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc, or RIVET_TEST_MYSQL_STMT_URL at a statement-truth stand"]
fn a_partition_removed_from_a_captured_table_stops_the_stream_mysql() {
    for (label, op) in [
        ("seam_p1", "TRUNCATE PARTITION p0"),
        ("seam_p2", "DROP PARTITION p0"),
        ("seam_p3", "EXCHANGE PARTITION p0 WITH TABLE swap"),
        (
            "seam_p4",
            "EXCHANGE PARTITION p0 WITH TABLE swap WITHOUT VALIDATION",
        ),
    ] {
        let s = Seam::new(label);
        stops(
            &s,
            "parts",
            "(1,10),(2,20)",
            &[format!("ALTER TABLE parts {op}")],
            TRUNCATED,
        );
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc, or RIVET_TEST_MYSQL_STMT_URL at a statement-truth stand"]
fn a_partitioned_table_exchanged_with_a_captured_table_stops_the_stream_mysql() {
    let s = Seam::new("seam_px");
    stops(
        &s,
        "swap",
        "(1,10),(2,20)",
        &["ALTER TABLE parts EXCHANGE PARTITION p0 WITH TABLE swap".to_string()],
        TRUNCATED,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc, or RIVET_TEST_MYSQL_STMT_URL at a statement-truth stand"]
fn the_same_statements_on_tables_the_export_does_not_capture_leave_the_stream_running_mysql() {
    for s in [Seam::new("seam_n1"), Seam::folding("seam_n5")] {
        let mut steps = vec![
            "TRUNCATE TABLE other".to_string(),
            "ALTER TABLE otherparts TRUNCATE PARTITION p0".to_string(),
            "ALTER TABLE otherparts DROP PARTITION p0".to_string(),
            "ALTER TABLE parts EXCHANGE PARTITION p0 WITH TABLE swap".to_string(),
            "DROP TABLE swap".to_string(),
        ];
        if s.folds() {
            steps.push(format!("TRUNCATE TABLE `{}`.`OTHER`", s.db.to_uppercase()));
        }
        passes(&s, "kinds", &steps);
    }
    let t = Seam::new("seam_n2");
    let other = Seam::new("seam_n3");
    let _ = &other;
    passes(&t, "kinds", &[format!("DROP DATABASE `{}`", other.db)]);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc, or RIVET_TEST_MYSQL_STMT_URL at a statement-truth stand"]
fn on_a_server_that_keeps_case_a_database_spelled_in_another_case_is_another_database_and_leaves_the_stream_running_mysql()
 {
    let s = Seam::new("seam_n4");
    if s.folds() {
        skip_live(&format!(
            "lower_case_table_names={}: another spelling is the same table; point {STAND_VAR} at a case-sensitive stand",
            s.lctn
        ));
        return;
    }
    let shadow = Shadow::new(&s.base, &s.db.to_uppercase());
    passes(
        &s,
        "kinds",
        &[
            format!("TRUNCATE TABLE `{}`.`KINDS`", shadow.name),
            format!("TRUNCATE TABLE `{}`.`{KELVIN}inds`", shadow.name),
            format!("TRUNCATE TABLE `{}`.`kinds`", shadow.name),
            format!("DROP DATABASE `{}`", shadow.name),
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc, or RIVET_TEST_MYSQL_STMT_URL at a statement-truth stand"]
fn the_rebaseline_a_partition_stop_prints_delivers_the_rows_the_source_holds_mysql() {
    let s = Seam::new("seam_rb");
    stops_then(
        &s,
        "parts",
        "(1,10),(2,20)",
        &[
            "ALTER TABLE parts TRUNCATE PARTITION p0".to_string(),
            "INSERT INTO parts VALUES (50,500)".to_string(),
        ],
        TRUNCATED,
        |rig| {
            follow_rebaseline_remedy(rig, false);
        },
    );
}
