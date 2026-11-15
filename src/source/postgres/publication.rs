//! The publication a `pgoutput` stream reads: created when absent, verified when present, never altered silently.

use std::collections::BTreeMap;

use postgres::Client;

use crate::error::Result;
use crate::source::postgres::cdc::search_path_relation;

/// The columns a publication carries per configured table; a table absent from the list carries all of its columns.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ColumnList(BTreeMap<String, Vec<String>>);

impl ColumnList {
    /// A list from `(configured table name, columns)` pairs.
    pub(crate) fn from_pairs<I, S>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (S, Vec<String>)>,
        S: Into<String>,
    {
        Self(pairs.into_iter().map(|(t, c)| (t.into(), c)).collect())
    }

    /// The columns configured for `table`, if any.
    pub(crate) fn get(&self, table: &str) -> Option<&[String]> {
        self.0.get(table).map(Vec::as_slice)
    }
}

/// One table of a publication as the catalog spells it, with its column list when it has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PubTable {
    pub schema: String,
    pub table: String,
    pub columns: Option<Vec<String>>,
}

/// What `pg_publication` and `pg_publication_rel` hold for one publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedPublication {
    pub all_tables: bool,
    pub schema_members: u32,
    pub publishes_all_operations: bool,
    pub tables: Vec<PubTable>,
}

/// Whether an existing publication is the one the stream needs, and if not, what differs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PublicationVerdict {
    Match,
    Drift(Vec<String>),
}

/// The first PostgreSQL major version whose publications carry column lists.
const COLUMN_LISTS_FROM: i32 = 150_000;

/// The quoted `"schema"."table" ("a","b")` member list of a publication.
fn table_clause(tables: &[PubTable]) -> String {
    use crate::config::SourceType::Postgres;
    use crate::sql::quote_ident;
    tables
        .iter()
        .map(|t| {
            let mut s = format!(
                "{}.{}",
                quote_ident(Postgres, &t.schema),
                quote_ident(Postgres, &t.table)
            );
            if let Some(cols) = &t.columns {
                let list: Vec<String> = cols.iter().map(|c| quote_ident(Postgres, c)).collect();
                s.push_str(&format!(" ({})", list.join(", ")));
            }
            s
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The `CREATE PUBLICATION` statement for `tables`.
pub(crate) fn create_statement(name: &str, tables: &[PubTable]) -> String {
    format!(
        "CREATE PUBLICATION {} FOR TABLE {}",
        crate::sql::quote_ident(crate::config::SourceType::Postgres, name),
        table_clause(tables)
    )
}

/// The statements that bring a drifted publication to `tables`.
pub(crate) fn repair_statements(name: &str, tables: &[PubTable]) -> Vec<String> {
    let q = crate::sql::quote_ident(crate::config::SourceType::Postgres, name);
    vec![
        format!("ALTER PUBLICATION {q} SET TABLE {}", table_clause(tables)),
        format!("ALTER PUBLICATION {q} SET (publish = 'insert, update, delete, truncate')"),
    ]
}

fn sorted(cols: &[String]) -> Vec<&str> {
    let mut v: Vec<&str> = cols.iter().map(String::as_str).collect();
    v.sort_unstable();
    v
}

/// Compare what the server holds with what the stream needs; every difference is named.
pub(crate) fn publication_verdict(
    desired: &[PubTable],
    observed: &ObservedPublication,
) -> PublicationVerdict {
    let mut why = Vec::new();
    if observed.all_tables {
        why.push("it is FOR ALL TABLES".to_string());
    }
    if observed.schema_members > 0 {
        why.push("it has FOR TABLES IN SCHEMA members".to_string());
    }
    if !observed.publishes_all_operations {
        why.push("it does not publish insert, update, delete and truncate".to_string());
    }
    for want in desired {
        match observed
            .tables
            .iter()
            .find(|o| o.schema == want.schema && o.table == want.table)
        {
            None => why.push(format!("it lacks table {}.{}", want.schema, want.table)),
            Some(have) => {
                let same = match (&want.columns, &have.columns) {
                    (None, None) => true,
                    (Some(w), Some(h)) => sorted(w) == sorted(h),
                    _ => false,
                };
                if !same {
                    why.push(format!(
                        "table {}.{} carries {} where the stream needs {}",
                        want.schema,
                        want.table,
                        describe(&have.columns),
                        describe(&want.columns)
                    ));
                }
            }
        }
    }
    for have in &observed.tables {
        if !desired
            .iter()
            .any(|w| w.schema == have.schema && w.table == have.table)
        {
            why.push(format!(
                "it holds table {}.{} that no export of this slot reads",
                have.schema, have.table
            ));
        }
    }
    if why.is_empty() {
        PublicationVerdict::Match
    } else {
        PublicationVerdict::Drift(why)
    }
}

fn describe(cols: &Option<Vec<String>>) -> String {
    match cols {
        None => "all columns".to_string(),
        Some(c) => format!("columns ({})", c.join(", ")),
    }
}

/// The refusal text for a publication that differs from the stream's need, with the exact repair.
pub(crate) fn drift_refusal(name: &str, why: &[String], tables: &[PubTable]) -> String {
    format!(
        "pg cdc: publication '{name}' exists but differs from what this stream reads: {}. \
         rivet does not alter a publication it did not just create. Apply `{}` (and `{}` when it does not publish every operation), then re-run.",
        why.join("; "),
        repair_statements(name, tables)[0],
        repair_statements(name, tables)[1],
    )
}

/// The server's major version as `server_version_num`.
fn server_version_num(client: &mut Client) -> Result<i32> {
    let v: String = client
        .query_one("SELECT current_setting('server_version_num')", &[])?
        .get(0);
    Ok(v.parse().unwrap_or(0))
}

/// Read one publication from the catalog; `None` when it does not exist.
fn observe(client: &mut Client, name: &str, version: i32) -> Result<Option<ObservedPublication>> {
    let Some(head) = client.query_opt(
        "SELECT puballtables, pubinsert AND pubupdate AND pubdelete AND pubtruncate \
           FROM pg_publication WHERE pubname = $1",
        &[&name],
    )?
    else {
        return Ok(None);
    };
    let (columns_sql, schemas_sql) = if version >= COLUMN_LISTS_FROM {
        (
            "CASE WHEN r.prattrs IS NULL THEN NULL ELSE ARRAY(SELECT a.attname::text FROM pg_attribute a \
              WHERE a.attrelid = c.oid AND a.attnum = ANY(r.prattrs::int2[]) ORDER BY a.attnum) END",
            "(SELECT count(*) FROM pg_publication_namespace pn JOIN pg_publication p ON p.oid = pn.pnpubid WHERE p.pubname = $1)::int4",
        )
    } else {
        ("NULL::text[]", "0::int4")
    };
    let rows = client.query(
        &format!(
            "SELECT n.nspname::text, c.relname::text, {columns_sql} \
               FROM pg_publication_rel r \
               JOIN pg_publication p ON p.oid = r.prpubid \
               JOIN pg_class c ON c.oid = r.prrelid \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
              WHERE p.pubname = $1 ORDER BY 1, 2"
        ),
        &[&name],
    )?;
    let schema_members: i32 = client
        .query_one(&format!("SELECT {schemas_sql}"), &[&name])?
        .get(0);
    Ok(Some(ObservedPublication {
        all_tables: head.get(0),
        schema_members: u32::try_from(schema_members).unwrap_or(0),
        publishes_all_operations: head.get(1),
        tables: rows
            .iter()
            .map(|r| PubTable {
                schema: r.get(0),
                table: r.get(1),
                columns: r.get(2),
            })
            .collect(),
    }))
}

/// Resolve configured table names to the catalog's spelling, attaching the column list each has.
fn resolve_tables(
    client: &mut Client,
    tables: &[String],
    columns: Option<&ColumnList>,
) -> Result<Vec<PubTable>> {
    let mut out: Vec<PubTable> = Vec::new();
    for cfg in tables {
        let Some((schema, table)) = search_path_relation(client, cfg)? else {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
                "pg cdc: configured table '{cfg}' does not resolve to a base table, so the publication cannot name it. \
                 Set `table:` to `schema.table` as the catalog spells it."
            );
        };
        let cols = columns.and_then(|c| c.get(cfg)).map(<[String]>::to_vec);
        if let Some(c) = &cols
            && c.is_empty()
        {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_PREREQUISITE,
                "pg cdc: the column list for '{cfg}' is empty, and a publication cannot carry a table with no columns. \
                 Grant SELECT on at least one column of '{cfg}', or leave the table out of the export."
            );
        }
        if !out.iter().any(|t| t.schema == schema && t.table == table) {
            out.push(PubTable {
                schema,
                table,
                columns: cols,
            });
        }
    }
    Ok(out)
}

/// Create the publication `name` over `tables` when absent; verify it when present. The one place a column list enters.
pub(crate) fn ensure_publication(
    client: &mut Client,
    name: &str,
    tables: &[String],
    columns: Option<&ColumnList>,
) -> Result<()> {
    let version = server_version_num(client)?;
    if columns.is_some() && version < COLUMN_LISTS_FROM {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            "pg cdc: column lists in a publication need PostgreSQL 15 or newer, and this server is {version}. \
             Upgrade the server, or remove `columns_from: grants` from the export."
        );
    }
    let desired = resolve_tables(client, tables, columns)?;
    if desired.is_empty() {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
            "pg cdc: publication '{name}' has no table to carry. Set `table:` on the export."
        );
    }
    if observe(client, name, version)?.is_none() {
        match client.batch_execute(&create_statement(name, &desired)) {
            Ok(()) => return Ok(()),
            Err(e) if e.code() == Some(&postgres::error::SqlState::DUPLICATE_OBJECT) => {}
            Err(e) => return Err(e.into()),
        }
    }
    let observed = observe(client, name, version)?.ok_or_else(|| {
        crate::error::coded(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            format!(
                "pg cdc: publication '{name}' disappeared while it was being verified. Re-run; if it keeps vanishing, another session is dropping it."
            ),
        )
    })?;
    match publication_verdict(&desired, &observed) {
        PublicationVerdict::Match => Ok(()),
        PublicationVerdict::Drift(why) => {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_PREREQUISITE,
                "{}",
                drift_refusal(name, &why, &desired)
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(schema: &str, table: &str, cols: Option<&[&str]>) -> PubTable {
        PubTable {
            schema: schema.into(),
            table: table.into(),
            columns: cols.map(|c| c.iter().map(|s| s.to_string()).collect()),
        }
    }

    fn seen(tables: Vec<PubTable>) -> ObservedPublication {
        ObservedPublication {
            all_tables: false,
            schema_members: 0,
            publishes_all_operations: true,
            tables,
        }
    }

    /// The same table set and the same columns in another order match.
    #[test]
    fn the_same_members_match_whatever_their_order() {
        let want = [t("public", "a", Some(&["x", "y"])), t("public", "b", None)];
        let have = seen(vec![
            t("public", "b", None),
            t("public", "a", Some(&["y", "x"])),
        ]);
        assert_eq!(publication_verdict(&want, &have), PublicationVerdict::Match);
    }

    /// Each kind of difference is named: a missing table, an extra one, a column list, a flag, FOR ALL TABLES, schema members.
    #[test]
    fn every_kind_of_drift_is_named() {
        let want = [t("public", "a", Some(&["x"]))];
        let mut have = seen(vec![t("public", "a", None), t("public", "z", None)]);
        have.all_tables = true;
        have.schema_members = 1;
        have.publishes_all_operations = false;
        let PublicationVerdict::Drift(why) = publication_verdict(&want, &have) else {
            panic!("a drifted publication must not match");
        };
        let all = why.join("|");
        for needle in [
            "FOR ALL TABLES",
            "FOR TABLES IN SCHEMA",
            "does not publish insert, update, delete and truncate",
            "table public.a carries all columns where the stream needs columns (x)",
            "holds table public.z",
        ] {
            assert!(all.contains(needle), "{needle} missing from {all}");
        }
        let lacking = publication_verdict(&want, &seen(vec![]));
        assert_eq!(
            lacking,
            PublicationVerdict::Drift(vec!["it lacks table public.a".into()])
        );
    }

    /// The statements quote every identifier and carry the column list.
    #[test]
    fn the_statements_quote_names_and_carry_the_column_list() {
        let tables = [
            t("my\"s", "Ord", Some(&["id", "Na\"me"])),
            t("public", "b", None),
        ];
        assert_eq!(
            create_statement("p\"1", &tables),
            "CREATE PUBLICATION \"p\"\"1\" FOR TABLE \"my\"\"s\".\"Ord\" (\"id\", \"Na\"\"me\"), \"public\".\"b\""
        );
        assert_eq!(
            repair_statements("p", &tables)[0],
            "ALTER PUBLICATION \"p\" SET TABLE \"my\"\"s\".\"Ord\" (\"id\", \"Na\"\"me\"), \"public\".\"b\""
        );
    }

    /// The refusal names its cause and both repair statements.
    #[test]
    fn the_drift_refusal_names_the_cause_and_the_repair() {
        let tables = [t("public", "a", None)];
        let text = drift_refusal("pub1", &["it lacks table public.a".into()], &tables);
        assert!(text.contains("publication 'pub1' exists but differs"));
        assert!(text.contains("it lacks table public.a"));
        assert!(text.contains("ALTER PUBLICATION \"pub1\" SET TABLE \"public\".\"a\""));
        assert!(text.contains(
            "ALTER PUBLICATION \"pub1\" SET (publish = 'insert, update, delete, truncate')"
        ));
    }

    /// A column list answers by the configured name, and a table without one carries all columns.
    #[test]
    fn a_column_list_is_looked_up_by_configured_name() {
        let l = ColumnList::from_pairs([("public.a", vec!["x".to_string()])]);
        assert_eq!(l.get("public.a"), Some(&["x".to_string()][..]));
        assert_eq!(l.get("public.b"), None);
    }

    const STANDS: [&str; 2] = [
        "postgresql://rivet:rivet@127.0.0.1:5434/rivet",
        "postgresql://rivet:rivet@127.0.0.1:5618/rivet",
    ];

    fn repair_in(err: &anyhow::Error, which: &str) -> String {
        let text = format!("{err:#}");
        let at = text
            .find(which)
            .unwrap_or_else(|| panic!("{which} in {text}"));
        let rest = &text[at..];
        let start = rest.find('`').expect("opening tick") + 1;
        let end = start + rest[start..].find('`').expect("closing tick");
        rest[start..end].to_string()
    }

    /// Live: a publication is created when absent, accepted when equal, refused with a working repair when it drifted.
    #[test]
    #[ignore = "live: requires docker compose postgres-cdc (:5434) and pg18-cdc (:5618)"]
    fn live_a_publication_is_created_verified_and_a_drift_is_repaired_by_the_message() {
        for url in STANDS {
            let mut c = Client::connect(url, postgres::NoTls).unwrap();
            let tag = format!(
                "{}_{}",
                std::process::id(),
                url.rsplit(':').next().unwrap().len()
            );
            let (a, b, p) = (
                format!("pgo_pub_a_{tag}"),
                format!("pgo_pub_b_{tag}"),
                format!("pgo_pub_{tag}"),
            );
            c.batch_execute(&format!(
                "DROP PUBLICATION IF EXISTS {p}; DROP TABLE IF EXISTS {a}, {b}; \
                 CREATE TABLE {a} (id int PRIMARY KEY, secret text); CREATE TABLE {b} (id int PRIMARY KEY)"
            ))
            .unwrap();
            let tables = vec![a.clone()];
            ensure_publication(&mut c, &p, &tables, None).expect("created");
            ensure_publication(&mut c, &p, &tables, None).expect("verified");
            let held: i64 = c
                .query_one("SELECT count(*) FROM pg_publication_tables WHERE pubname = $1 AND tablename = $2", &[&p, &a])
                .unwrap()
                .get(0);
            assert_eq!(held, 1, "{url}");

            c.batch_execute(&format!("ALTER PUBLICATION {p} ADD TABLE {b}"))
                .unwrap();
            let err =
                ensure_publication(&mut c, &p, &tables, None).expect_err("an extra table is drift");
            assert_eq!(
                crate::error::code_of(&err).map(|c| c.id),
                Some("RIVET_SOURCE_CDC_PREREQUISITE")
            );
            assert!(
                format!("{err:#}").contains(&format!("holds table public.{b}")),
                "{err:#}"
            );
            c.batch_execute(&repair_in(&err, "Apply `"))
                .expect("the named repair runs");
            ensure_publication(&mut c, &p, &tables, None).expect("repaired");

            c.batch_execute(&format!("ALTER PUBLICATION {p} SET (publish = 'insert')"))
                .unwrap();
            let err = ensure_publication(&mut c, &p, &tables, None)
                .expect_err("a missing operation is drift");
            assert!(
                format!("{err:#}").contains("does not publish insert, update, delete and truncate")
            );
            c.batch_execute(&repair_in(&err, "(and `"))
                .expect("the flag repair runs");
            ensure_publication(&mut c, &p, &tables, None).expect("repaired flags");

            let missing = ensure_publication(&mut c, &p, &["pgo_no_such_table".to_string()], None)
                .expect_err("an unresolved table");
            assert_eq!(
                crate::error::code_of(&missing).map(|c| c.id),
                Some("RIVET_SOURCE_CDC_TABLE_UNRESOLVED")
            );

            let version: i32 = c
                .query_one("SELECT current_setting('server_version_num')::int", &[])
                .unwrap()
                .get(0);
            let cols = ColumnList::from_pairs([(a.clone(), vec!["id".to_string()])]);
            let q = format!("{p}_cols");
            c.batch_execute(&format!("DROP PUBLICATION IF EXISTS {q}"))
                .unwrap();
            if version >= COLUMN_LISTS_FROM {
                ensure_publication(&mut c, &q, &tables, Some(&cols))
                    .expect("created with a column list");
                ensure_publication(&mut c, &q, &tables, Some(&cols))
                    .expect("verified with a column list");
                let listed: Vec<String> = c
                    .query_one(
                        "SELECT attnames::text[] FROM pg_publication_tables WHERE pubname = $1",
                        &[&q],
                    )
                    .unwrap()
                    .get(0);
                assert_eq!(
                    listed,
                    vec!["id".to_string()],
                    "{url}: the server holds the list, secret is not published"
                );
                let err = ensure_publication(&mut c, &q, &tables, None)
                    .expect_err("a narrower list than needed");
                assert!(
                    format!("{err:#}")
                        .contains("carries columns (id) where the stream needs all columns"),
                    "{err:#}"
                );
                c.batch_execute(&repair_in(&err, "Apply `"))
                    .expect("repair");
                ensure_publication(&mut c, &q, &tables, None).expect("repaired");
            } else {
                let err = ensure_publication(&mut c, &q, &tables, Some(&cols))
                    .expect_err("PG14 has no column lists");
                assert!(format!("{err:#}").contains("need PostgreSQL 15"), "{err:#}");
            }
            c.batch_execute(&format!("DROP PUBLICATION IF EXISTS {p}; DROP PUBLICATION IF EXISTS {q}; DROP TABLE {a}, {b}")).unwrap();
        }
    }
}
