//! Live `source.columns_from: grants` (MongoDB): an account that reads through a `$project` view delivers only the view's fields.

use std::path::Path;

use mongodb::bson::{Document, doc};

use crate::common::*;

const SECRET: &str = "SECRET-";
const ROOT: (&str, &str) = ("root", "rootpass");
const PASSWORD: &str = "viewpw";

/// A role, a user and a view of `base` in `db`, created as root and dropped with the fixture.
struct ViewAccount {
    admin: MongoTest,
    db: String,
    base: String,
    user: String,
    role: String,
}

impl ViewAccount {
    fn admin_url(db: &str) -> String {
        mongo_auth_rs_url(ROOT.0, ROOT.1, db)
    }

    fn new(db: &str, base: &str) -> Self {
        require_alive(LiveService::MongoAuthRs);
        let admin = MongoTest::connect_url(&Self::admin_url(db), db);
        let account = Self {
            admin,
            db: db.to_string(),
            base: base.to_string(),
            user: unique_name("vu"),
            role: unique_name("vr"),
        };
        account.admin.drop_database();
        account
    }

    fn on_admin(&self, command: Document) {
        self.admin
            .run_command_on("admin", command.clone())
            .unwrap_or_else(|e| panic!("{command:?}: {e}"));
    }

    fn create_view(&self, name: &str, pipeline: Vec<Document>) {
        self.admin
            .run_command_on(
                &self.db,
                doc! { "create": name, "viewOn": &self.base, "pipeline": pipeline },
            )
            .unwrap_or_else(|e| panic!("create view {name}: {e}"));
    }

    fn privilege(&self, collection: &str, actions: &[&str]) -> Document {
        doc! {
            "resource": { "db": &self.db, "collection": collection },
            "actions": actions.to_vec(),
        }
    }

    fn grant(&self, privileges: Vec<Document>) {
        self.on_admin(doc! { "createRole": &self.role, "privileges": privileges, "roles": [] });
        self.on_admin(doc! {
            "createUser": &self.user,
            "pwd": PASSWORD,
            "roles": [{ "role": &self.role, "db": "admin" }],
        });
    }

    fn view_only(&self, view: &str) {
        self.grant(vec![
            self.privilege(view, &["find"]),
            self.privilege("", &["listCollections"]),
        ]);
    }

    fn view_and_stream(&self, view: &str) {
        self.grant(vec![
            self.privilege(view, &["find"]),
            self.privilege("", &["listCollections", "find", "changeStream"]),
        ]);
    }

    fn url(&self) -> String {
        mongo_auth_rs_url(&self.user, PASSWORD, &self.db)
    }
}

impl Drop for ViewAccount {
    fn drop(&mut self) {
        let _ = self
            .admin
            .run_command_on("admin", doc! { "dropUser": &self.user });
        let _ = self
            .admin
            .run_command_on("admin", doc! { "dropRole": &self.role });
        self.admin.drop_database();
    }
}

fn project(fields: &[&str]) -> Document {
    let mut stage = Document::new();
    for f in fields {
        stage.insert(*f, 1);
    }
    doc! { "$project": stage }
}

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

fn delivered_parts(rig: &Rig) -> Vec<arrow::record_batch::RecordBatch> {
    parquet_dirs(&rig.out_dir())
        .iter()
        .flat_map(|d| read_all_parts(d))
        .collect()
}

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

/// `(op, _id, document)` of every change in `parts`, in capture order.
fn changes(parts: &[arrow::record_batch::RecordBatch]) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for b in parts.iter().filter(|b| b.column_by_name("__op").is_some()) {
        let s = |n: &str, i: usize| {
            arrow::util::display::array_value_to_string(b.column_by_name(n).unwrap(), i).unwrap()
        };
        for i in 0..b.num_rows() {
            let seq: i64 = s("__seq", i).parse().unwrap();
            out.push((seq, (s("__op", i), s("_id", i), s("document", i))));
        }
    }
    out.sort_by_key(|(seq, _)| *seq);
    out.into_iter().map(|(_, c)| c).collect()
}

fn said_by(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn seed_people(account: &ViewAccount) {
    account.admin.insert_many(
        &account.base,
        vec![
            doc! { "_id": 1_i64, "name": "ann", "pass": format!("{SECRET}one") },
            doc! { "_id": 2_i64, "name": "bob", "pass": format!("{SECRET}two") },
        ],
    );
}

fn batch_rig(account: &ViewAccount) -> Rig {
    Rig::mongo_batch(&account.base)
        .source_url(&account.url())
        .source_line("columns_from: grants")
        .no_oracle(
            "the oracle reads the base collection as the view-only account, which the server refuses; the cell reads the delivered parts against the source ids itself",
        )
}

fn cdc_rig(account: &ViewAccount) -> Rig {
    Rig::mongo_cdc(&account.base)
        .source_url(&account.url())
        .source_line("columns_from: grants")
        .cdc("initial: snapshot")
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn a_view_account_delivers_no_hidden_field_by_name_or_value_in_batch_mongo() {
    let account = ViewAccount::new(&unique_name("cgb"), "people");
    seed_people(&account);
    account.create_view("people_public", vec![project(&["name"])]);
    account.view_only("people_public");

    let rig = batch_rig(&account);
    let check = rig.cli(&["check"]);
    assert!(
        check.status.success(),
        "check under a view grant:\n{}",
        said_by(&check)
    );
    let said = said_by(&check);
    assert!(
        said.contains("MongoDB has no column privileges"),
        "check says what the policy guarantees on MongoDB:\n{said}"
    );
    rig.run_ok();

    let (names, cells) = names_and_cells(&delivered_parts(&rig));
    assert!(
        !cells
            .iter()
            .any(|c| c.contains(SECRET) || c.contains("pass")),
        "a hidden field reached the delivered bytes: {cells:?}"
    );
    assert!(
        !names.iter().any(|n| n == "pass"),
        "`pass` is a delivered column: {names:?}"
    );
    let source_ids = account.admin.ids(&account.base);
    for id in &source_ids {
        assert!(
            cells.iter().any(|c| c.contains(&format!("\"_id\":{id}"))
                || c.contains(&format!("\"_id\": {id}"))
                || c == id),
            "source _id {id} is not delivered: {cells:?}"
        );
    }
    assert!(
        cells.iter().any(|c| c.contains("ann")) && cells.iter().any(|c| c.contains("bob")),
        "the view's field is delivered: {cells:?}"
    );
}

/// One refusal of a batch run under `columns_from: grants`: its code, its words, and that nothing was written.
fn refused_batch(account: &ViewAccount, url: &str, words: &[&str]) {
    let rig = Rig::mongo_batch(&account.base)
        .source_url(url)
        .source_line("columns_from: grants");
    let out = rig.cli(&["run"]);
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
    let said = said_by(&out);
    for w in words {
        assert!(said.contains(w), "the refusal names `{w}`:\n{said}");
    }
    assert!(
        said.contains("Create a view of the collection"),
        "the refusal names the remedy:\n{said}"
    );
    assert!(
        parquet_dirs(&rig.out_dir()).is_empty(),
        "a refused run writes nothing"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn a_batch_read_is_refused_when_the_account_can_read_the_collection_itself_mongo() {
    let account = ViewAccount::new(&unique_name("cgr"), "people");
    seed_people(&account);
    account.create_view("people_public", vec![project(&["name"])]);
    account.grant(vec![
        account.privilege("people_public", &["find"]),
        account.privilege("people", &["find"]),
        account.privilege("", &["listCollections"]),
    ]);
    refused_batch(
        &account,
        &account.url(),
        &[
            "would not be enforced by the server",
            "find on the collection itself",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn a_batch_read_is_refused_when_the_account_holds_a_database_wide_read_mongo() {
    let account = ViewAccount::new(&unique_name("cgw"), "people");
    seed_people(&account);
    account.create_view("people_public", vec![project(&["name"])]);
    account.view_and_stream("people_public");
    refused_batch(
        &account,
        &account.url(),
        &[
            "would not be enforced by the server",
            "find and changeStream",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn a_batch_read_is_refused_when_no_view_exposes_the_collection_mongo() {
    let account = ViewAccount::new(&unique_name("cgn"), "people");
    seed_people(&account);
    account.grant(vec![account.privilege("", &["listCollections"])]);
    refused_batch(
        &account,
        &account.url(),
        &["found no view of this collection"],
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn a_batch_read_is_refused_when_two_views_expose_the_collection_mongo() {
    let account = ViewAccount::new(&unique_name("cgt"), "people");
    seed_people(&account);
    account.create_view("people_a", vec![project(&["name"])]);
    account.create_view("people_b", vec![project(&["name"])]);
    account.view_only("people_a");
    refused_batch(&account, &account.url(), &["people_a", "people_b"]);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn a_batch_read_is_refused_when_the_view_hides_the_key_mongo() {
    let account = ViewAccount::new(&unique_name("cgk"), "people");
    seed_people(&account);
    account.create_view(
        "people_public",
        vec![doc! { "$project": { "_id": 0, "name": 1 } }],
    );
    account.view_only("people_public");
    refused_batch(&account, &account.url(), &["_id"]);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn a_batch_read_is_refused_when_the_view_is_not_a_single_project_mongo() {
    for (label, pipeline) in [
        ("exclusion", vec![doc! { "$project": { "pass": 0 } }]),
        (
            "two stages",
            vec![doc! { "$match": { "name": "ann" } }, project(&["name"])],
        ),
        (
            "an expression",
            vec![doc! { "$project": { "name": { "$toUpper": "$name" } } }],
        ),
    ] {
        let account = ViewAccount::new(&unique_name("cgs"), "people");
        seed_people(&account);
        account.create_view("people_public", pipeline);
        account.view_only("people_public");
        println!("view shape: {label}");
        refused_batch(&account, &account.url(), &["`$project`"]);
    }
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn a_batch_read_is_refused_without_credentials_mongo() {
    require_alive(LiveService::Mongo);
    let db = unique_name("cgu");
    let m = MongoTest::connect(27017, &db);
    let _guard = MongoDbGuard {
        port: 27017,
        db: db.clone(),
    };
    m.insert_many(
        "people",
        vec![doc! { "_id": 1_i64, "name": "ann", "pass": "p" }],
    );
    m.run_command_on(
        &db,
        doc! { "create": "people_public", "viewOn": "people", "pipeline": [project(&["name"])] },
    )
    .unwrap();
    let rig = Rig::mongo_batch("people")
        .source_url(&MongoTest::url(27017, &db))
        .source_line("columns_from: grants");
    let out = rig.cli(&["run"]);
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_COLUMNS_UNREADABLE", 1));
    assert!(
        said_by(&out).contains("needs an authenticated account"),
        "the refusal names its cause:\n{}",
        said_by(&out)
    );
    assert!(parquet_dirs(&rig.out_dir()).is_empty());
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn a_batch_without_the_policy_reads_every_field_as_before_mongo() {
    let account = ViewAccount::new(&unique_name("cgp"), "people");
    seed_people(&account);
    let rig = Rig::mongo_batch(&account.base).source_url(&ViewAccount::admin_url(&account.db));
    rig.run_ok();
    let (_, cells) = names_and_cells(&delivered_parts(&rig));
    assert!(
        cells.iter().any(|c| c.contains(&format!("{SECRET}one")))
            && cells.iter().any(|c| c.contains(&format!("{SECRET}two"))),
        "without the policy every field is delivered: {cells:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn a_view_account_delivers_no_hidden_field_in_cdc_mongo() {
    let account = ViewAccount::new(&unique_name("cgc"), "people");
    seed_people(&account);
    account.create_view("people_public", vec![project(&["name", "tags"])]);
    account.view_and_stream("people_public");
    let pre_images = account.admin.enable_pre_images("people");

    let rig = cdc_rig(&account);
    let check = rig.cli(&["check"]);
    assert!(check.status.success(), "check:\n{}", said_by(&check));
    rig.run_ok();

    account
        .admin
        .upsert_fields("people", 1, doc! { "pass": format!("{SECRET}a") });
    account.admin.upsert_fields(
        "people",
        1,
        doc! { "pass": format!("{SECRET}b"), "name": "ann2" },
    );
    account.admin.upsert_fields(
        "people",
        3,
        doc! { "name": "cid", "pass": { "deep": format!("{SECRET}c") }, "tags": ["x"] },
    );
    account
        .admin
        .upsert_fields("people", 3, doc! { "pass.deep": format!("{SECRET}d") });
    account.admin.unset_field("people", 2, "pass");
    account.admin.delete_one("people", 2);
    rig.run_ok();

    let parts = delivered_parts(&rig);
    let (names, cells) = names_and_cells(&parts);
    assert!(
        !cells
            .iter()
            .any(|c| c.contains(SECRET) || c.contains("pass")),
        "a hidden field reached the delivered bytes: {cells:?}"
    );
    assert!(
        cells.iter().any(|c| c.contains("ann")) && cells.iter().any(|c| c.contains("ann2")),
        "the snapshot and the change parts were both read: {cells:?}"
    );
    assert!(names.iter().any(|n| n == "__op"), "change parts: {names:?}");
    let got = changes(&parts);
    let ops: Vec<(&str, &str)> = got
        .iter()
        .map(|(o, i, _)| (o.as_str(), i.as_str()))
        .collect();
    assert_eq!(
        ops,
        vec![("update", "1"), ("insert", "3"), ("delete", "2")],
        "an update of hidden fields only is not delivered; every other change is: {got:?}"
    );
    if pre_images {
        let delete = got.last().unwrap();
        assert!(
            delete.2.contains("bob") && !delete.2.contains("pass"),
            "the delete's pre-image carries the view's fields only: {delete:?}"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn an_update_of_hidden_fields_only_advances_the_checkpoint_mongo() {
    let account = ViewAccount::new(&unique_name("cgh"), "people");
    seed_people(&account);
    account.create_view("people_public", vec![project(&["name"])]);
    account.view_and_stream("people_public");

    let rig = cdc_rig(&account);
    rig.run_ok();
    let pinned = std::fs::read(rig.checkpoint()).unwrap();
    let before = delivered_parts(&rig).len();

    account
        .admin
        .upsert_fields("people", 1, doc! { "pass": format!("{SECRET}x") });
    rig.run_ok();

    assert_ne!(
        pinned,
        std::fs::read(rig.checkpoint()).unwrap(),
        "the checkpoint must move past an event that shows nothing"
    );
    let parts = delivered_parts(&rig);
    assert_eq!(
        parts.len(),
        before,
        "a hidden-only update is delivered as no row"
    );
    assert!(
        changes(&parts).is_empty(),
        "no change row: {:?}",
        changes(&parts)
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn a_cdc_run_is_refused_when_no_view_exposes_the_collection_mongo() {
    let account = ViewAccount::new(&unique_name("cgq"), "people");
    seed_people(&account);
    account.grant(vec![
        account.privilege("", &["listCollections", "find", "changeStream"]),
    ]);
    let rig = cdc_rig(&account);
    for (cmd, code) in [
        (&["run"][..], "RIVET_SOURCE_COLUMNS_UNREADABLE"),
        (&["doctor"][..], "RIVET_CLI_PREFLIGHT_FAILED"),
    ] {
        let out = rig.cli(cmd);
        assert_refused(&out, Refused::by_code(code, 1));
        assert!(
            said_by(&out).contains("found no view of this collection"),
            "{cmd:?}:\n{}",
            said_by(&out)
        );
    }
    assert!(
        !rig.checkpoint().exists(),
        "a refused run pins no checkpoint"
    );
    assert!(parquet_dirs(&rig.out_dir()).is_empty());
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth-rs"]
fn a_cdc_stream_without_the_policy_delivers_every_field_as_before_mongo() {
    let account = ViewAccount::new(&unique_name("cgn2"), "people");
    seed_people(&account);
    let rig = Rig::mongo_cdc(&account.base)
        .source_url(&ViewAccount::admin_url(&account.db))
        .cdc("initial: snapshot");
    rig.run_ok();
    account
        .admin
        .upsert_fields("people", 1, doc! { "pass": format!("{SECRET}x") });
    rig.run_ok();
    let got = changes(&delivered_parts(&rig));
    assert_eq!(got.len(), 1, "{got:?}");
    assert!(
        got[0].2.contains(&format!("{SECRET}x")),
        "every field is delivered without the policy: {got:?}"
    );
}
