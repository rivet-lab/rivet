//! Live MySQL CDC cells that `RESET MASTER` their own server: `docker compose --profile cdc up -d mysql-reset duckdb`.

use crate::common::*;
use mysql::prelude::Queryable;

/// A root connection to the server these cells may reset.
fn reset_root() -> mysql::PooledConn {
    mysql::Pool::new(MYSQL_RESET_ROOT_URL)
        .expect("mysql pool")
        .get_conn()
        .expect("mysql conn")
}

/// A fresh table on the reset server and a CDC rig over it, anchored by one pin run.
fn pinned(root: &mut mysql::PooledConn, label: &str) -> (String, Rig) {
    let tbl = unique_name(label);
    root.query_drop(format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT); FLUSH BINARY LOGS; FLUSH BINARY LOGS"
    ))
    .expect("create");
    let rig = Rig::mysql_cdc(&tbl)
        .source_url(MYSQL_RESET_URL)
        .duckdb_oracle();
    rig.run_ok();
    (tbl, rig)
}

/// With GTID off, `RESET MASTER` until the checkpoint's file name exists again: the resume is refused, never read from an offset in another file.
#[test]
#[ignore = "live: requires the mysql-reset service (:3317) and duckdb"]
fn a_checkpoint_whose_binlog_name_returns_after_reset_master_is_refused() {
    require_alive(LiveService::MysqlReset);
    let _serial = cross_process_serial("mysql_reset");
    let mut root = reset_root();
    let (tbl, rig) = pinned(&mut root, "rivet_cdc_renum");
    root.query_drop(format!("INSERT INTO {tbl} VALUES (1, 1), (2, 2)"))
        .expect("insert");
    rig.run_ok();
    let ckpt = std::fs::read_to_string(rig.checkpoint()).expect("checkpoint");
    let file = serde_json::from_str::<serde_json::Value>(&ckpt).expect("json")["file"]
        .as_str()
        .expect("file")
        .to_string();
    let ordinal: u32 = file.rsplit('.').next().unwrap().parse().unwrap();
    root.query_drop("RESET MASTER").expect("reset master");
    for _ in 1..ordinal {
        root.query_drop("FLUSH BINARY LOGS").expect("flush");
    }
    root.query_drop(format!("INSERT INTO {tbl} VALUES (101, 1), (102, 2)"))
        .expect("same-sized transaction after reset");
    root.query_drop(format!("INSERT INTO {tbl} VALUES (103, 3)"))
        .expect("insert after reset");
    let out = rig.run_with_envs(&[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() && out.status.code() == Some(5),
        "a resume into a renumbered binlog must stop with exit 5: {:?}\n{stderr}",
        out.status.code()
    );
    assert!(
        stderr.contains("RIVET_SOURCE_CDC_FOREIGN_CHECKPOINT"),
        "the refusal is coded: {stderr}"
    );
    assert_eq!(
        std::fs::read_to_string(rig.checkpoint()).expect("checkpoint"),
        ckpt,
        "a refused resume leaves the checkpoint where it was"
    );
    rig.assert_complete("id", 2, "only the changes before the reset are delivered");
    let _ = root.query_drop(format!("DROP TABLE IF EXISTS {tbl}"));
}

/// A checkpoint written before rivet recorded a commit fingerprint resumes on its coordinates and says so.
#[test]
#[ignore = "live: requires the mysql-reset service (:3317) and duckdb"]
fn a_checkpoint_without_a_commit_fingerprint_resumes_with_a_warning() {
    require_alive(LiveService::MysqlReset);
    let _serial = cross_process_serial("mysql_reset");
    let mut root = reset_root();
    let (tbl, rig) = pinned(&mut root, "rivet_cdc_nofp");
    root.query_drop(format!("INSERT INTO {tbl} VALUES (1, 1), (2, 2)"))
        .expect("insert");
    rig.run_ok();
    let mut ckpt: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(rig.checkpoint()).expect("checkpoint"))
            .expect("json");
    assert!(
        ckpt["commit"]["xid"].is_u64(),
        "a checkpoint at a commit records its fingerprint: {ckpt}"
    );
    ckpt.as_object_mut().expect("object").remove("commit");
    std::fs::write(rig.checkpoint(), ckpt.to_string()).expect("write old-shape checkpoint");
    root.query_drop(format!("INSERT INTO {tbl} VALUES (3, 3)"))
        .expect("insert");
    let said = rig.run_ok_capture();
    assert!(
        said.contains("records no commit fingerprint"),
        "an old checkpoint resumes with a warning: {said}"
    );
    rig.assert_complete(
        "id",
        3,
        "every change is delivered once across the old checkpoint",
    );
    let _ = root.query_drop(format!("DROP TABLE IF EXISTS {tbl}"));
}

/// A commit fingerprint that lands on an event whose text is not UTF-8 is a coded refusal, never a panic.
#[test]
#[ignore = "live: requires the mysql-reset service (:3317) and duckdb"]
fn a_fingerprint_on_an_event_whose_text_is_not_utf8_is_refused_by_code() {
    require_alive(LiveService::MysqlReset);
    let _serial = cross_process_serial("mysql_reset");
    let mut root = reset_root();
    let (tbl, rig) = pinned(&mut root, "rivet_cdc_bin");
    root.query_drop(format!("INSERT INTO {tbl} VALUES (1, 1)"))
        .expect("insert");
    rig.run_ok();
    root.query_drop(format!("ALTER TABLE {tbl} ADD COLUMN b VARBINARY(4)"))
        .expect("alter");
    let file: String = serde_json::from_str::<serde_json::Value>(
        &std::fs::read_to_string(rig.checkpoint()).expect("checkpoint"),
    )
    .expect("json")["file"]
        .as_str()
        .expect("file")
        .to_string();
    root.query_drop("SET SESSION binlog_format = 'STATEMENT'")
        .expect("statement format");
    root.exec_drop(
        format!("INSERT INTO {tbl} VALUES (2, 2, ?)"),
        (vec![0xffu8, 0xfe, 0x80, 0xc3],),
    )
    .expect("a statement whose logged text is not UTF-8");
    let events: Vec<(u64, Vec<u8>, u64, Vec<u8>)> = root
        .query_map(
            format!("SHOW BINLOG EVENTS IN '{file}'"),
            |(_, pos, kind, _, end, info): (Vec<u8>, u64, Vec<u8>, u64, u64, Vec<u8>)| {
                (pos, kind, end, info)
            },
        )
        .expect("binlog events");
    let (at, _, end, _) = events
        .into_iter()
        .find(|(_, kind, _, info)| kind == b"Query" && std::str::from_utf8(info).is_err())
        .expect("the statement is logged with its raw bytes");
    let mut ckpt: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(rig.checkpoint()).expect("checkpoint"))
            .expect("json");
    ckpt["pos"] = serde_json::json!(end);
    ckpt["commit"] = serde_json::json!({"at": at, "xid": 1});
    std::fs::write(rig.checkpoint(), ckpt.to_string()).expect("write checkpoint");
    let out = rig.run_with_envs(&[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() && out.status.code() == Some(5),
        "a fingerprint on another event must stop with exit 5, not a panic: {:?}\n{stderr}",
        out.status.code()
    );
    assert!(
        stderr.contains("RIVET_SOURCE_CDC_FOREIGN_CHECKPOINT") && !stderr.contains("panicked"),
        "the refusal is coded: {stderr}"
    );
    rig.assert_complete("id", 1, "nothing past the refused checkpoint is delivered");
    let _ = root.query_drop(format!("DROP TABLE IF EXISTS {tbl}"));
}
