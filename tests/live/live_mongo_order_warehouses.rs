//! MongoDB CDC in BigQuery and Snowflake is ranked by `(ct, ord)` then `_data`: crafted logs whose resume tokens disagree with the stream order, graded against the source.

use crate::common::*;

const PORT: u16 = 27018;

enum Wh {
    Bq(BqLive),
    Sf(SfLive),
}

impl Wh {
    fn bq(label: &str) -> Option<Self> {
        BqLive::from_env(label).map(Wh::Bq)
    }

    fn sf(label: &str) -> Option<Self> {
        SfLive::from_env(label).map(Wh::Sf)
    }

    fn shaped(&self, rig: Rig, table: &str, base_buffer: bool) -> Rig {
        let rig = if base_buffer {
            rig.cdc("backfill: auto")
                .also_batch_export("baseline", table, "full")
        } else {
            rig
        };
        let layout = if base_buffer {
            "base_buffer"
        } else {
            "log_view"
        };
        let extra = format!(", pk: [_id], layout: {layout}");
        match self {
            Wh::Bq(b) => rig
                .dest_gcs_live(&b.bucket, &b.prefix)
                .top_line(&b.load_line(&extra)),
            Wh::Sf(s) => rig
                .dest_gcs_live(&s.bucket, &s.prefix)
                .top_line(&s.load_line(&extra)),
        }
    }

    fn fq(&self, table: &str) -> String {
        match self {
            Wh::Bq(b) => format!("`{}.{}.{table}`", b.project, b.dataset),
            Wh::Sf(s) => s.fq(table),
        }
    }

    fn exec(&self, sql: &str) {
        match self {
            Wh::Bq(b) => b.exec(sql),
            Wh::Sf(s) => s.exec(sql),
        }
    }

    fn guard(&self, table: &str) -> Box<dyn std::any::Any> {
        match self {
            Wh::Bq(b) => Box::new(b.cleanup(&[table, &format!("{table}__changes")])),
            Wh::Sf(s) => Box::new(s.cleanup(&[table])),
        }
    }

    fn live_pairs(&self, relation: &str) -> Vec<(i64, i64)> {
        let to_i64 = |v: &serde_json::Value| -> i64 {
            match v {
                serde_json::Value::String(s) => s.parse().expect("a number"),
                other => other.as_i64().expect("a number"),
            }
        };
        let mut rows: Vec<(i64, i64)> = match self {
            Wh::Bq(b) => b
                .read_bq_rows(&format!(
                    "SELECT CAST(_id AS INT64) AS k, CAST(JSON_VALUE(document, '$.v') AS INT64) AS v \
                     FROM {} WHERE NOT __is_deleted",
                    self.fq(relation)
                ))
                .iter()
                .map(|r| (to_i64(&r["k"]), to_i64(&r["v"])))
                .collect(),
            Wh::Sf(s) => s
                .read(&format!(
                    "SELECT CAST(_id AS INT), CAST(document:v AS INT) FROM {} WHERE NOT __is_deleted",
                    self.fq(relation)
                ))
                .iter()
                .map(|r| (to_i64(&r[0]), to_i64(&r[1])))
                .collect(),
        };
        rows.sort_unstable();
        rows
    }

    fn max_ord(&self, changes: &str) -> i64 {
        match self {
            Wh::Bq(b) => b
                .read_bq_rows(&format!(
                    "SELECT MAX(CAST(JSON_VALUE(__pos, '$.ord') AS INT64)) AS n FROM {} WHERE __op IS NOT NULL",
                    self.fq(changes)
                ))[0]["n"]
                .as_str()
                .expect("n")
                .parse()
                .expect("ord"),
            Wh::Sf(s) => {
                let r = &s.read(&format!(
                    "SELECT MAX(PARSE_JSON(__pos):ord::integer) FROM {} WHERE __op IS NOT NULL",
                    self.fq(changes)
                ))[0][0];
                r.as_i64()
                    .or_else(|| r.as_str().and_then(|x| x.parse().ok()))
                    .expect("ord")
            }
        }
    }

    fn count(&self, sql_where: &str, relation: &str) -> i64 {
        let sql = format!(
            "SELECT COUNT(*) AS n FROM {} WHERE {sql_where}",
            self.fq(relation)
        );
        match self {
            Wh::Bq(b) => b.read_bq_rows(&sql)[0]["n"]
                .as_str()
                .expect("n")
                .parse()
                .expect("count"),
            Wh::Sf(s) => {
                let r = &s.read(&sql)[0][0];
                r.as_i64()
                    .or_else(|| r.as_str().and_then(|x| x.parse().ok()))
                    .expect("count")
            }
        }
    }
}

fn run_ok_load(rig: &Rig) -> String {
    let out = rig.cli(&["load"]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "rivet load failed:\n{said}");
    said
}

fn compact_ok(rig: &Rig) -> String {
    let out = rig.cli(&["compact"]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "rivet compact failed:\n{said}");
    said
}

const U64_MAX: &str = "18446744073709551615";
const U64_BELOW_MAX: &str = "18446744073709551614";

struct Crafted {
    id: i64,
    op: &'static str,
    pos: String,
}

fn pos(data: &str, stamp: Option<(&str, u32)>) -> String {
    match stamp {
        Some((ct, ord)) => format!(r#"{{"_data":"{data}","rt":"{data}","ct":{ct},"ord":{ord}}}"#),
        None => format!(r#"{{"_data":"{data}","rt":"{data}"}}"#),
    }
}

/// Per key an insert and an update (a delete for the last) whose tokens run against the stream order.
fn crafted_log() -> Vec<Crafted> {
    let c = |id, op, pos| Crafted { id, op, pos };
    vec![
        c(11, "insert", pos("FF", Some(("100", 0)))),
        c(11, "update", pos("00", Some(("200", 0)))),
        c(12, "insert", pos("0A", Some(("100", 9)))),
        c(12, "update", pos("0B", Some(("100", 10)))),
        c(13, "insert", pos("FF", Some((U64_BELOW_MAX, 0)))),
        c(13, "update", pos("00", Some((U64_MAX, 0)))),
        c(14, "insert", pos("FF", None)),
        c(14, "update", pos("00", Some(("1", 0)))),
        c(15, "insert", pos("0A", None)),
        c(15, "update", pos("0B", None)),
        c(16, "insert", pos("FF", Some(("100", 1)))),
        c(16, "update", pos("00", Some(("100", 2)))),
        c(17, "insert", pos("FF", Some(("100", 0)))),
        c(17, "delete", pos("00", Some(("200", 0)))),
    ]
}

/// Insert and update keys 11..=16 and insert then delete key 17, in that order.
fn churn(scn: &mut CdcScenario) {
    for id in 11..=17 {
        scn.insert(id);
    }
    for id in 11..=16 {
        scn.update(id);
    }
    scn.delete(17);
    scn.settle();
}

fn craft(wh: &Wh, changes: &str) {
    let log = crafted_log();
    for c in &log {
        wh.exec(&format!(
            "UPDATE {} SET __pos = '{}' WHERE CAST(_id AS STRING) = '{}' AND __op = '{}'",
            wh.fq(changes),
            c.pos,
            c.id,
            c.op
        ));
    }
    let crafted = log
        .iter()
        .map(|c| format!("'{}'", c.pos))
        .collect::<Vec<_>>()
        .join(", ");
    assert_eq!(
        wh.count(&format!("__pos IN ({crafted})"), changes),
        log.len() as i64,
        "every crafted position landed on exactly one change row"
    );
}

fn source_from(scn: &mut CdcScenario, first: i64) -> Vec<(i64, i64)> {
    scn.id_v_pairs()
        .into_iter()
        .filter(|(id, _)| *id >= first)
        .collect()
}

fn crafted_view_cell(wh: Wh, label: &str) {
    require_alive(LiveService::MongoRs);
    let coll = unique_name(label);
    let _guard = wh.guard(&coll);
    let mut scn = CdcScenario::mongo_collection_with(label, &coll, |r, t| wh.shaped(r, t, false));
    scn.rig.run_ok();
    churn(&mut scn);
    scn.rig.run_ok();
    run_ok_load(&scn.rig);
    let changes = format!("{coll}__changes");
    craft(&wh, &changes);
    let want = source_from(&mut scn, 11);
    assert_eq!(
        want.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![11, 12, 13, 14, 15, 16]
    );
    assert_eq!(
        wh.live_pairs(&coll),
        want,
        "the view ranks each key's rows by (ct, ord), then token"
    );
}

fn crafted_compact_cell(wh: Wh, label: &str) {
    require_alive(LiveService::MongoRs);
    let coll = unique_name(label);
    let _guard = wh.guard(&coll);
    let mut scn = CdcScenario::mongo_collection_with(label, &coll, |r, t| wh.shaped(r, t, true));
    scn.insert(1);
    scn.insert(2);
    scn.settle();
    scn.rig.run_ok();
    let said = run_ok_load(&scn.rig);
    assert!(said.contains("layout=base+buffer"), "{said}");
    compact_ok(&scn.rig);
    churn(&mut scn);
    scn.rig.run_ok();
    run_ok_load(&scn.rig);
    let changes = format!("{coll}__changes");
    craft(&wh, &changes);
    let said = compact_ok(&scn.rig);
    assert!(said.contains("COMPACT OK"), "{said}");
    let want = scn.id_v_pairs();
    let mut live = wh.live_pairs(&coll);
    live.sort_unstable();
    assert_eq!(
        live, want,
        "the compact ranks each key's rows by (ct, ord), then token"
    );
}

#[test]
#[ignore = "live: requires mongo-rs + BigQuery creds"]
fn a_mongo_log_whose_tokens_run_against_its_stamps_ranks_by_the_stamps_in_the_view_bigquery() {
    let Some(wh) = Wh::bq("mgo_vw_bq") else {
        return;
    };
    crafted_view_cell(wh, "mgo_vw_bq");
}

#[test]
#[ignore = "live: requires mongo-rs + Snowflake creds"]
fn a_mongo_log_whose_tokens_run_against_its_stamps_ranks_by_the_stamps_in_the_view_snowflake() {
    let Some(wh) = Wh::sf("mgo_vw_sf") else {
        return;
    };
    crafted_view_cell(wh, "mgo_vw_sf");
}

#[test]
#[ignore = "live: requires mongo-rs + BigQuery creds"]
fn a_mongo_log_whose_tokens_run_against_its_stamps_ranks_by_the_stamps_in_the_compact_bigquery() {
    let Some(wh) = Wh::bq("mgo_cp_bq") else {
        return;
    };
    crafted_compact_cell(wh, "mgo_cp_bq");
}

#[test]
#[ignore = "live: requires mongo-rs + Snowflake creds"]
fn a_mongo_log_whose_tokens_run_against_its_stamps_ranks_by_the_stamps_in_the_compact_snowflake() {
    let Some(wh) = Wh::sf("mgo_cp_sf") else {
        return;
    };
    crafted_compact_cell(wh, "mgo_cp_sf");
}

/// One transaction (one clusterTime) writes a key twelve times, so its ordinal passes 9; the view equals Mongo.
fn tie_cell(wh: Wh, label: &str) {
    require_alive(LiveService::MongoRs);
    let coll = unique_name(label);
    let db = unique_name(label);
    let _guard = wh.guard(&coll);
    let _db = MongoDbGuard {
        port: PORT,
        db: db.clone(),
    };
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection(&coll);
    let rig = wh.shaped(
        Rig::mongo_cdc(&coll).source_url(&MongoTest::url(PORT, &db)),
        &coll,
        false,
    );
    rig.run_ok();
    let values: Vec<String> = (1..=12).map(|i| i.to_string()).collect();
    let ops: Vec<(i64, &str, &str)> = values.iter().map(|v| (1, "v", v.as_str())).collect();
    m.txn_updates(&coll, &ops);
    rig.run_ok();
    run_ok_load(&rig);
    assert_eq!(
        wh.max_ord(&format!("{coll}__changes")),
        11,
        "twelve events of one clusterTime, ordinals 0..=11"
    );
    let want: Vec<(i64, i64)> = m
        .current_state_i64(&coll, "v")
        .into_iter()
        .map(|(id, v)| (id, v.parse().expect("v")))
        .collect();
    assert_eq!(want, vec![(1, 12)]);
    assert_eq!(wh.live_pairs(&coll), want);
}

#[test]
#[ignore = "live: requires mongo-rs + BigQuery creds"]
fn a_mongo_transaction_of_twelve_events_loads_in_source_order_bigquery() {
    let Some(wh) = Wh::bq("mgo_tie_bq") else {
        return;
    };
    tie_cell(wh, "mgo_tie_bq");
}

#[test]
#[ignore = "live: requires mongo-rs + Snowflake creds"]
fn a_mongo_transaction_of_twelve_events_loads_in_source_order_snowflake() {
    let Some(wh) = Wh::sf("mgo_tie_sf") else {
        return;
    };
    tie_cell(wh, "mgo_tie_sf");
}
