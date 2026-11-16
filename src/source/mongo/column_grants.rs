//! `source.columns_from: grants` on MongoDB: the fields a view of the collection exposes, asked of the server.

use std::collections::{BTreeMap, HashMap};

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};

use super::MongoSession;
use crate::error::{Result, codes};
use crate::source::cdc::ReadableColumns;

/// What `columns_from: grants` guarantees on MongoDB, said once at `rivet check` and at the start of a change-stream run.
pub(crate) const BOUNDARY: &str = "columns_from: grants - MongoDB has no column privileges: the fields an \
account may read are those of a view of the collection (a single `$project` stage). A batch read goes \
through that view, and is refused unless the account holds no find or changeStream on the collection \
itself, so the server never hands it a hidden field. A change stream cannot be opened on a view: it watches \
the database, so the account holds find and changeStream on the collection and the server-side pipeline \
that removes hidden fields from each event is data minimisation, not access control: restrict the account \
itself if the values must not reach this host. A delete carries its pre-image from MongoDB 6.0 with pre- and post-images enabled on the collection, and only its key before.";

/// What every refusal of this policy asks of the operator.
const REMEDY: &str = "Create a view of the collection whose only stage is a `$project` listing the fields \
to deliver (db.createView('<view>', '<collection>', [{$project: {<field>: 1}}])), keep `_id` in it, and let \
the account read through the view: `find` on the view and `listCollections` on the database. A batch run \
also needs the account to hold no `find` and no `changeStream` on the collection itself; a CDC run holds \
them database-wide and delivers only the view's fields";

/// The privilege actions that let an account read a collection's documents.
const READ_ACTIONS: [&str; 3] = ["find", "changeStream", "anyAction"];

/// The refusal that says `why` about `db.collection` under `columns_from: grants`.
fn refusal(db: &str, collection: &str, why: &str) -> anyhow::Error {
    crate::error::coded(
        codes::SOURCE_COLUMNS_UNREADABLE,
        format!(
            "mongodb: `{db}.{collection}`: `columns_from: grants` {why}; nothing was read or \
             written. {REMEDY}"
        ),
    )
}

/// Whether `v` keeps a field in a `$project` stage.
fn includes(v: &Bson) -> Option<bool> {
    v.as_bool()
        .or_else(|| v.as_i32().map(|i| i != 0))
        .or_else(|| v.as_i64().map(|i| i != 0))
        .or_else(|| v.as_f64().map(|d| d != 0.0))
}

/// The fields a view whose pipeline is `pipeline` exposes, `_id` first, or why it exposes no fixed list.
pub(crate) fn view_fields(pipeline: &[Document]) -> std::result::Result<Vec<String>, String> {
    let [stage] = pipeline else {
        return Err(format!(
            "its view has {} pipeline stages, and only a single `$project` stage names the fields it exposes",
            pipeline.len()
        ));
    };
    let Ok(project) = stage.get_document("$project") else {
        return Err("its view's only stage is not a `$project`".to_string());
    };
    let mut fields = vec!["_id".to_string()];
    for (key, value) in project {
        let Some(keep) = includes(value) else {
            return Err(format!(
                "its view projects `{key}` with an expression, and only `field: 1` lists a field"
            ));
        };
        if key == "_id" {
            if !keep {
                return Err(
                    "its view leaves out `_id`, and a document cannot be delivered without its key"
                        .to_string(),
                );
            }
            continue;
        }
        if !keep {
            return Err(format!(
                "its view excludes `{key}`, and only a view that lists the fields it keeps (`field: 1`) names what is delivered"
            ));
        }
        if key.is_empty() || key.starts_with('$') || key.contains('.') {
            return Err(format!(
                "its view projects the path `{key}`, and only top-level field names (no `.`, no leading `$`) can be delivered"
            ));
        }
        fields.push(key.clone());
    }
    Ok(fields)
}

/// The view a listing row describes: its name and the fields it exposes.
fn exposed_by(db: &str, base: &str, listed: &[Document]) -> Result<(String, Vec<String>)> {
    let mut named: Vec<&str> = listed
        .iter()
        .filter_map(|d| d.get_str("name").ok())
        .collect();
    named.sort_unstable();
    let [only] = listed else {
        if listed.is_empty() {
            return Err(refusal(
                db,
                base,
                "found no view of this collection that the account can list",
            ));
        }
        return Err(refusal(
            db,
            base,
            &format!(
                "found {} views of this collection ({}), and cannot tell which one the account is meant to read through",
                listed.len(),
                named.join(", ")
            ),
        ));
    };
    let view = only.get_str("name").unwrap_or_default().to_string();
    let bare = !view.contains('.')
        && crate::sql::strip_select_star_from(&format!("SELECT * FROM {view}")) == Some(&view);
    if !bare {
        return Err(refusal(
            db,
            base,
            &format!(
                "found the view `{view}`, whose name is not a plain identifier (letters, digits and `_`, not starting with a digit)"
            ),
        ));
    }
    let pipeline: Vec<Document> = only
        .get_document("options")
        .ok()
        .and_then(|o| o.get_array("pipeline").ok())
        .map(|p| {
            p.iter()
                .filter_map(|s| s.as_document().cloned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let fields = view_fields(&pipeline)
        .map_err(|why| refusal(db, base, &format!("view `{view}`: {why}")))?;
    Ok((view, fields))
}

/// The columns an account may read of `base` and the `SELECT` of the view that exposes them.
pub(crate) fn readable_columns_of_view(
    session: &MongoSession,
    base: &str,
) -> Result<ReadableColumns> {
    let db = session.db();
    let listed = session.block_on(async {
        let mut cursor = session
            .client()
            .database(db)
            .list_collections()
            .filter(doc! { "options.viewOn": base })
            .await?
            .with_type::<Document>();
        let mut rows = Vec::new();
        while let Some(row) = cursor.try_next().await? {
            rows.push(row);
        }
        Ok::<_, mongodb::error::Error>(rows)
    });
    let listed = listed.map_err(|e| {
        if super::not_allowed(&e) {
            refusal(
                db,
                base,
                "could not list the views of this database: the account lacks `listCollections` on it",
            )
        } else {
            anyhow::Error::new(e).context(format!(
                "listing the views of MongoDB collection `{db}.{base}` failed"
            ))
        }
    })?;
    let (view, columns) = exposed_by(db, base, &listed)?;
    Ok(ReadableColumns {
        select: format!("SELECT * FROM {view}"),
        columns,
    })
}

/// Whether the privileges `auth` lists let the account read `db.collection`, and by which actions.
pub(crate) fn privileges_reaching(auth: &Document, db: &str, collection: &str) -> Vec<String> {
    let privileges = auth
        .get_array("authenticatedUserPrivileges")
        .cloned()
        .unwrap_or_default();
    let mut reaching: Vec<String> = Vec::new();
    for privilege in privileges.iter().filter_map(Bson::as_document) {
        let Ok(resource) = privilege.get_document("resource") else {
            continue;
        };
        let covers = resource.get_bool("anyResource").unwrap_or(false)
            || {
                let (rdb, rcoll) = (
                    resource.get_str("db").ok(),
                    resource.get_str("collection").ok(),
                );
                matches!((rdb, rcoll), (Some(d), Some(c)) if (d.is_empty() || d == db) && (c.is_empty() || c == collection))
            };
        if !covers {
            continue;
        }
        for action in privilege
            .get_array("actions")
            .into_iter()
            .flatten()
            .filter_map(Bson::as_str)
        {
            if READ_ACTIONS.contains(&action) && !reaching.iter().any(|a| a == action) {
                reaching.push(action.to_string());
            }
        }
    }
    reaching.sort_by_key(|a| READ_ACTIONS.iter().position(|r| r == a));
    reaching
}

/// The refusal a batch read owes when the account is unauthenticated or can read `base` itself.
fn refuse_unless_view_only(db: &str, base: &str, auth: &Document) -> Result<()> {
    let users = auth.get_array("authenticatedUsers").map_or(0, Vec::len);
    if users == 0 {
        return Err(refusal(
            db,
            base,
            "needs an authenticated account: this connection holds no credentials, so the server restricts nothing it reads",
        ));
    }
    let reaching = privileges_reaching(auth, db, base);
    if seeded!(
        "mongo-grants-base-readable-accepted",
        true,
        reaching.is_empty()
    ) {
        return Ok(());
    }
    Err(refusal(
        db,
        base,
        &format!(
            "would not be enforced by the server: the account holds {} on the collection itself, so it can read the hidden fields without the view",
            reaching.join(" and ")
        ),
    ))
}

/// The `authInfo` of the connection, with the privileges it holds.
fn auth_info(session: &MongoSession) -> Result<Document> {
    let reply = session
        .block_on(async {
            session
                .client()
                .database(session.db())
                .run_command(doc! { "connectionStatus": 1, "showPrivileges": true })
                .await
        })
        .map_err(|e| {
            anyhow::Error::new(e).context("reading the privileges of the MongoDB connection failed")
        })?;
    Ok(reply.get_document("authInfo").cloned().unwrap_or_default())
}

/// What a batch read of `base` may read: the view's fields, once the account is shown to reach only the view.
pub(crate) fn readable_columns_for_batch(
    session: &MongoSession,
    base: &str,
) -> Result<ReadableColumns> {
    let readable = readable_columns_of_view(session, base)?;
    refuse_unless_view_only(session.db(), base, &auth_info(session)?)?;
    Ok(readable)
}

/// The view of each of `tables` a change stream filters its events by, resolved once before the anchor.
pub(crate) fn resolve_for_tables(
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    tables: &[String],
) -> Result<HashMap<String, ReadableColumns>> {
    let session = MongoSession::connect(url, tls)?;
    tables
        .iter()
        .map(|t| Ok((t.clone(), readable_columns_of_view(&session, t)?)))
        .collect()
}

/// The `$literal` list of fields an event of `collection` may carry.
fn allowed_fields(grants: &BTreeMap<&str, &[String]>) -> Document {
    let branches: Vec<Bson> = grants
        .iter()
        .map(|(coll, fields)| {
            Bson::Document(doc! {
                "case": { "$eq": ["$ns.coll", *coll] },
                "then": { "$literal": Bson::Array(fields.iter().map(|f| Bson::String(f.clone())).collect()) },
            })
        })
        .collect();
    doc! { "$switch": { "branches": branches, "default": { "$literal": Bson::Array(Vec::new()) } } }
}

/// `$let allowed` around `body`, where `allowed` is the fields the event's collection may deliver.
fn with_allowed(grants: &BTreeMap<&str, &[String]>, body: Document) -> Bson {
    Bson::Document(doc! { "$let": { "vars": { "allowed": allowed_fields(grants) }, "in": body } })
}

/// `$cond` that applies `filtered` to `path` when it is an object and leaves it as it is otherwise.
fn when_object(path: &str, filtered: Document) -> Document {
    doc! { "$cond": [{ "$eq": [{ "$type": path }, "object"] }, filtered, path] }
}

/// The keys of the object at `path` whose first segment is allowed, as an object.
fn kept_keys(path: &str) -> Document {
    doc! { "$arrayToObject": { "$filter": {
        "input": { "$objectToArray": path },
        "as": "f",
        "cond": { "$in": [{ "$arrayElemAt": [{ "$split": ["$$f.k", "."] }, 0] }, "$$allowed"] },
    } } }
}

/// The change-stream pipeline that keeps events of `grants`' collections and removes every field outside each one's list.
pub(crate) fn event_pipeline(grants: &HashMap<String, ReadableColumns>) -> Vec<Document> {
    let by_name: BTreeMap<&str, &[String]> = grants
        .iter()
        .map(|(t, r)| (t.as_str(), r.columns.as_slice()))
        .collect();
    let tables: Vec<Bson> = by_name
        .keys()
        .map(|t| Bson::String((*t).to_string()))
        .collect();
    let image = |path: &str| {
        with_allowed(
            &by_name,
            when_object(
                path,
                doc! { "$arrayToObject": { "$filter": {
                    "input": { "$objectToArray": path },
                    "as": "f",
                    "cond": { "$in": ["$$f.k", "$$allowed"] },
                } } },
            ),
        )
    };
    let described = with_allowed(
        &by_name,
        when_object(
            "$updateDescription",
            doc! {
                "updatedFields": kept_keys("$updateDescription.updatedFields"),
                "removedFields": { "$filter": {
                    "input": "$updateDescription.removedFields",
                    "as": "r",
                    "cond": { "$in": [{ "$arrayElemAt": [{ "$split": ["$$r", "."] }, 0] }, "$$allowed"] },
                } },
                "truncatedArrays": { "$filter": {
                    "input": "$updateDescription.truncatedArrays",
                    "as": "t",
                    "cond": { "$in": [{ "$arrayElemAt": [{ "$split": ["$$t.field", "."] }, 0] }, "$$allowed"] },
                } },
            },
        ),
    );
    vec![
        doc! { "$match": { "$or": [
            { "ns.coll": { "$in": tables } },
            { "operationType": { "$in": ["dropDatabase", "invalidate"] } },
        ] } },
        doc! { "$project": {
            "_id": 1, "operationType": 1, "clusterTime": 1, "ns": 1, "to": 1, "documentKey": 1,
            "fullDocument": 1, "fullDocumentBeforeChange": 1, "updateDescription": 1,
        } },
        doc! { "$addFields": {
            "documentKey": when_object("$documentKey", doc! { "_id": "$documentKey._id" }),
            "fullDocument": image("$fullDocument"),
            "fullDocumentBeforeChange": image("$fullDocumentBeforeChange"),
            "updateDescription": described,
        } },
    ]
}

/// The pipeline a change stream opens with: none without grants.
pub(crate) fn stream_pipeline(
    grants: Option<&HashMap<String, ReadableColumns>>,
) -> Option<Vec<Document>> {
    grants
        .filter(|_| !seeded!("mongo-cdc-pipeline-unprojected", true, false))
        .map(event_pipeline)
}

/// The collections a stream filters, by name.
pub(crate) fn projected_collections(
    grants: Option<&HashMap<String, ReadableColumns>>,
) -> Vec<String> {
    grants.map_or_else(Vec::new, |g| g.keys().cloned().collect())
}

/// Whether an event is an update of `collection` that leaves nothing this run may deliver.
pub(crate) fn is_hidden_only_update(
    projected: &[String],
    collection: Option<&str>,
    op: &mongodb::change_stream::event::OperationType,
    description: Option<&mongodb::change_stream::event::UpdateDescription>,
) -> bool {
    collection.is_some_and(|c| projected.iter().any(|p| p == c))
        && update_shows_nothing(op, description)
}

/// Whether an update the server already filtered changes nothing this run may deliver.
pub(crate) fn update_shows_nothing(
    op: &mongodb::change_stream::event::OperationType,
    description: Option<&mongodb::change_stream::event::UpdateDescription>,
) -> bool {
    matches!(op, mongodb::change_stream::event::OperationType::Update)
        && description.is_some_and(|d| {
            d.updated_fields.is_empty()
                && d.removed_fields.is_empty()
                && d.truncated_arrays.as_ref().is_none_or(Vec::is_empty)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(body: Document) -> Vec<Document> {
        vec![doc! { "$project": body }]
    }

    #[test]
    fn only_an_empty_update_of_a_projected_collection_is_hidden_only() {
        use mongodb::change_stream::event::{OperationType, UpdateDescription};
        let described = |fields: Document| {
            mongodb::bson::from_document::<UpdateDescription>(
                doc! { "updatedFields": fields, "removedFields": [] },
            )
            .unwrap()
        };
        let empty = described(Document::new());
        let shown = described(doc! { "name": "x" });
        let projected = vec!["people".to_string()];
        let hidden = |c, op: &OperationType, d: &UpdateDescription| {
            is_hidden_only_update(&projected, c, op, Some(d))
        };
        assert!(hidden(Some("people"), &OperationType::Update, &empty));
        assert!(!hidden(Some("other"), &OperationType::Update, &empty));
        assert!(!hidden(None, &OperationType::Update, &empty));
        assert!(!hidden(Some("people"), &OperationType::Update, &shown));
        assert!(!hidden(Some("people"), &OperationType::Replace, &empty));
    }

    #[test]
    fn a_project_view_exposes_its_listed_fields_and_the_key_first() {
        assert_eq!(
            view_fields(&project(doc! { "name": 1, "n": true })).unwrap(),
            ["_id", "name", "n"]
        );
        assert_eq!(
            view_fields(&project(doc! { "_id": 1, "name": 1 })).unwrap(),
            ["_id", "name"]
        );
    }

    #[test]
    fn a_projection_value_keeps_a_field_when_it_is_true_or_a_non_zero_number() {
        for (on, off) in [
            (Bson::Boolean(true), Bson::Boolean(false)),
            (Bson::Int32(1), Bson::Int32(0)),
            (Bson::Int64(2), Bson::Int64(0)),
            (Bson::Double(0.5), Bson::Double(0.0)),
        ] {
            assert_eq!(includes(&on), Some(true), "{on:?}");
            assert_eq!(includes(&off), Some(false), "{off:?}");
        }
        assert_eq!(includes(&Bson::String("$b".into())), None);
    }

    #[test]
    fn a_view_that_names_no_fixed_list_of_fields_is_refused_with_the_cause() {
        for (pipeline, said) in [
            (Vec::new(), "has 0 pipeline stages"),
            (
                vec![
                    doc! { "$match": { "a": 1 } },
                    doc! { "$project": { "a": 1 } },
                ],
                "has 2 pipeline stages",
            ),
            (vec![doc! { "$match": { "a": 1 } }], "is not a `$project`"),
            (project(doc! { "_id": 0, "a": 1 }), "leaves out `_id`"),
            (project(doc! { "secret": 0 }), "excludes `secret`"),
            (project(doc! { "a": "$b" }), "with an expression"),
            (project(doc! { "a.b": 1 }), "the path `a.b`"),
            (project(doc! { "$x": 1 }), "the path `$x`"),
        ] {
            let why = view_fields(&pipeline).unwrap_err();
            assert!(why.contains(said), "{pipeline:?}: {why}");
        }
    }

    fn privilege(db: &str, coll: &str, actions: &[&str]) -> Bson {
        Bson::Document(doc! {
            "resource": { "db": db, "collection": coll },
            "actions": actions.iter().map(|a| Bson::String((*a).into())).collect::<Vec<_>>(),
        })
    }

    #[test]
    fn a_privilege_reaches_the_collection_by_collection_database_or_any_resource() {
        let auth = |p: Vec<Bson>| doc! { "authenticatedUserPrivileges": p };
        let reach = |p: Vec<Bson>| privileges_reaching(&auth(p), "cg", "orders");
        assert_eq!(reach(vec![privilege("cg", "orders", &["find"])]), ["find"]);
        assert_eq!(
            reach(vec![privilege("cg", "", &["find", "changeStream"])]),
            ["find", "changeStream"]
        );
        assert_eq!(reach(vec![privilege("", "orders", &["find"])]), ["find"]);
        assert_eq!(
            reach(vec![privilege("", "", &["anyAction"])]),
            ["anyAction"]
        );
        assert_eq!(
            reach(vec![Bson::Document(
                doc! { "resource": { "anyResource": true }, "actions": ["find"] }
            )]),
            ["find"]
        );
        for held in [
            privilege("cg", "orders_v", &["find"]),
            privilege("cg", "other", &["find", "changeStream"]),
            privilege("elsewhere", "", &["find"]),
            privilege("cg", "", &["listCollections", "insert"]),
            Bson::Document(doc! { "resource": { "cluster": true }, "actions": ["find"] }),
        ] {
            assert!(reach(vec![held.clone()]).is_empty(), "{held:?}");
        }
    }

    #[test]
    fn a_batch_read_needs_an_authenticated_account_that_cannot_read_the_collection() {
        let view_only = doc! {
            "authenticatedUsers": [{ "user": "u", "db": "admin" }],
            "authenticatedUserPrivileges": [privilege("cg", "orders_v", &["find"])],
        };
        refuse_unless_view_only("cg", "orders", &view_only).unwrap();
        let anonymous = doc! { "authenticatedUsers": [], "authenticatedUserPrivileges": [] };
        let said = format!(
            "{:#}",
            refuse_unless_view_only("cg", "orders", &anonymous).unwrap_err()
        );
        assert!(said.contains("needs an authenticated account"), "{said}");
        let leaky = doc! {
            "authenticatedUsers": [{ "user": "u", "db": "admin" }],
            "authenticatedUserPrivileges": [privilege("cg", "", &["find", "changeStream"])],
        };
        let err = refuse_unless_view_only("cg", "orders", &leaky).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some(codes::SOURCE_COLUMNS_UNREADABLE.id)
        );
        let said = format!("{err:#}");
        assert!(
            said.contains("holds find and changeStream on the collection itself")
                && said.contains("db.createView"),
            "{said}"
        );
    }

    fn view(name: &str, project: Document) -> Document {
        doc! { "name": name, "type": "view", "options": { "viewOn": "orders", "pipeline": [{ "$project": project }] } }
    }

    #[test]
    fn exactly_one_plain_named_view_decides_the_fields() {
        let (name, fields) =
            exposed_by("cg", "orders", &[view("orders_v", doc! { "name": 1 })]).unwrap();
        assert_eq!(
            (name.as_str(), fields),
            ("orders_v", vec!["_id".to_string(), "name".to_string()])
        );
        for (listed, wanted) in [
            (vec![], "found no view"),
            (
                vec![view("b_v", doc! { "a": 1 }), view("a_v", doc! { "a": 1 })],
                "found 2 views of this collection (a_v, b_v)",
            ),
            (vec![view("a.b", doc! { "a": 1 })], "not a plain identifier"),
            (vec![view("1v", doc! { "a": 1 })], "not a plain identifier"),
            (
                vec![view("v", doc! { "a": "$b" })],
                "view `v`: its view projects `a` with an expression",
            ),
        ] {
            let said = format!("{:#}", exposed_by("cg", "orders", &listed).unwrap_err());
            assert!(said.contains(wanted), "{said}");
        }
    }
}
