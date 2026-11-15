//! Resolved capture identity — ONE answer to "which relation does this `table:` string mean", instead of six.

use crate::error::Result;

/// One relation the catalog reports for a configured name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogMatch {
    pub schema: String,
    pub table: String,
    /// Can this relation actually CARRY a change event?
    pub capturable: bool,
    /// What the catalog calls it — `BASE TABLE`, `VIEW`, a PostgreSQL `relkind`.
    pub kind: String,
}

/// A configured `table:` resolved to exactly one relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedTable {
    /// The string the operator wrote.
    pub configured: String,
    /// The catalog's own spelling — what wire events will carry.
    pub schema: String,
    pub table: String,
    /// Set when the configured name ALSO matched relations that cannot carry an event.
    pub note: Option<String>,
}

/// Resolve one configured `table:` against what the catalog reported for it.
pub(crate) fn resolve_captured_table(
    configured: &str,
    matches: &[CatalogMatch],
) -> Result<CapturedTable> {
    let mut distinct: Vec<&CatalogMatch> = Vec::new();
    for m in matches {
        if !distinct.iter().any(|d| **d == *m) {
            distinct.push(m);
        }
    }

    let (live, inert): (Vec<&CatalogMatch>, Vec<&CatalogMatch>) =
        distinct.iter().partition(|m| m.capturable);
    let note_excluding = |elected: Option<&CatalogMatch>| -> String {
        let twins: Vec<String> = inert
            .iter()
            .filter(|m| elected.is_none_or(|e| ***m != *e))
            .map(|m| format!("{}.{} [{}]", m.schema, m.table, m.kind))
            .collect();
        if twins.is_empty() {
            String::new()
        } else {
            format!(
                " (the name also matches {}, which cannot carry a change event — if that \
                 is the one you meant, capture is not possible for it)",
                twins.join(", ")
            )
        }
    };
    let note = note_excluding(None);
    if live.is_empty() && distinct.len() > 1 {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
            "cdc: `{configured}` matches {} relations and NONE of them can carry a change \
             event under that name{note}. Qualify it (`schema.table`) to say which one \
             you mean — that resolves to a single relation, and the engine then names \
             what is possible for THAT kind (a partitioned parent is captured by its \
             leaves or snapshotted with `mode: full`; a view is captured through the \
             base table it reads).",
            distinct.len()
        );
    }
    let distinct: Vec<&CatalogMatch> = if live.is_empty() { distinct } else { live };

    match distinct.as_slice() {
        [] => crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
            "cdc: `{configured}` matches no relation the source reports. Capture would \
             open a stream and route nothing — every event dropped by the routing \
             filter, silently. Check the name, and qualify it (`schema.table`) if it \
             lives outside the connection's default schema.{note}"
        ),
        [one] => {
            let note = note_excluding(Some(one));
            Ok(CapturedTable {
                configured: configured.to_string(),
                schema: one.schema.clone(),
                table: one.table.clone(),
                note: (!note.is_empty()).then(|| {
                    format!(
                        "`{configured}` resolves to {}.{}, but{note}",
                        one.schema, one.table
                    )
                }),
            })
        }
        many => {
            let listing = many
                .iter()
                .map(|m| {
                    if m.kind.is_empty() {
                        format!("{}.{}", m.schema, m.table)
                    } else {
                        format!("{}.{} [{}]", m.schema, m.table, m.kind)
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_TABLE_UNRESOLVED,
                "cdc: `{configured}` could mean {} different relations ({listing}) — the \
                 engine's own parser and rivet's byte-exact routing do not agree on one, \
                 or more than one schema holds the name. All of them would land in this \
                 export, and because images are matched by column NAME, a foreign row is \
                 written under THIS table's names when the arity matches, or as an \
                 all-NULL row when it does not. Neither is recoverable from the output. \
                 Qualify it (`schema.{configured}`, quoted as the catalog spells it) to \
                 capture the one you mean.{note}",
                many.len()
            )
        }
    }
}

/// Does a config `table:` entry match an event's identity?
pub(crate) fn table_matches(
    engine: super::CdcEngine,
    cfg: &str,
    schema: &str,
    table: &str,
) -> bool {
    if engine == super::CdcEngine::Mongo {
        return cfg == table;
    }
    if seeded!(
        "qualified-table-matches-a-literal-dotted-name",
        cfg == table,
        false
    ) {
        return true;
    }
    match cfg.split_once('.') {
        Some((cs, ct)) => cs == schema && ct == table,
        None => seeded!(
            "qualified-table-matches-a-literal-dotted-name",
            false,
            cfg == table
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::cdc::CdcEngine;

    fn m(schema: &str, table: &str) -> CatalogMatch {
        CatalogMatch {
            schema: schema.into(),
            table: table.into(),
            kind: "BASE TABLE".into(),
            capturable: true,
        }
    }

    fn k(schema: &str, table: &str, kind: &str) -> CatalogMatch {
        CatalogMatch {
            schema: schema.into(),
            table: table.into(),
            kind: kind.into(),
            capturable: kind == "BASE TABLE",
        }
    }

    /// A twin that cannot CARRY an event is context, not a collision.
    #[test]
    fn a_twin_that_cannot_carry_an_event_is_a_note_not_an_ambiguity() {
        let got = resolve_captured_table(
            "vv",
            &[k("rivet", "vv", "VIEW"), k("other_db", "vv", "BASE TABLE")],
        )
        .expect("only ONE candidate can carry an event, so there is no ambiguity");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("other_db", "vv"),
            "the capturable relation wins — the view emits nothing and cannot put \
             rows in this export"
        );
        let note = got.note.expect("the view must be reported, not swallowed");
        assert!(
            note.contains("rivet.vv [VIEW]") && note.contains("cannot carry a change event"),
            "name it and say why it was skipped, because it may be what they meant: \
             {note}"
        );

        let err = resolve_captured_table(
            "vv",
            &[k("a", "vv", "BASE TABLE"), k("b", "vv", "BASE TABLE")],
        )
        .expect_err("two relations that can each carry events IS an ambiguity");
        assert!(err.to_string().contains("could mean"));
    }

    /// No capturable candidate at all: the name matches only relations that emit nothing.
    #[test]
    fn a_name_matching_only_inert_relations_still_resolves_for_the_classifier() {
        let got = resolve_captured_table("vv", &[k("rivet", "vv", "VIEW")])
            .expect("one candidate, inert — classification refuses it, not resolution");
        assert_eq!((got.schema.as_str(), got.table.as_str()), ("rivet", "vv"));
    }

    /// TWO inert candidates and no live one.
    #[test]
    fn two_inert_candidates_are_refused_for_the_right_reason() {
        let err = resolve_captured_table("vv", &[k("a", "vv", "VIEW"), k("b", "vv", "VIEW")])
            .expect_err("nothing capturable, and two of them — capture is impossible");
        let msg = err.to_string();
        assert!(
            msg.contains("cannot carry a change event"),
            "say the TRUE reason — none of these emits anything — rather than the \
             ambiguity story about rows landing in the export: {msg}"
        );
        assert!(
            !msg.contains("would land in this export"),
            "and do not claim rows would land when none can: {msg}"
        );
    }

    /// The single-inert case's NOTE must not name the relation it just elected.
    #[test]
    fn a_lone_inert_match_gets_no_note_about_itself() {
        let got = resolve_captured_table("orders", &[k("public", "orders", "p")])
            .expect("one candidate, inert — classification owns the refusal");
        assert_eq!(
            got.note, None,
            "the only match IS the elected relation, so there is no twin to warn \
             about — a note naming it on both sides of `but` is noise the operator \
             has to decode before discarding. Got: {:?}",
            got.note
        );
    }

    /// The all-inert refusal must not claim capture is impossible for the NAME.
    #[test]
    fn the_all_inert_refusal_points_at_qualification_not_at_impossibility() {
        let err = resolve_captured_table(
            "orders",
            &[k("public", "orders", "p"), k("rep", "orders", "v")],
        )
        .expect_err("nothing capturable under this name as configured");
        let msg = err.to_string();
        assert!(
            !msg.contains("Capture is not possible for this name"),
            "capture IS possible for a partitioned parent — by its leaves, or with \
             `mode: full`. Stating the opposite sends an operator away from a \
             workable config: {msg}"
        );
        assert!(
            msg.contains("qualify") || msg.contains("schema.table"),
            "hand over the one action that reaches the engine's specific advice — \
             qualifying the name resolves to ONE relation and the classifier then \
             names what to do about it: {msg}"
        );
    }

    /// The ordinary case, and the one that must not regress.
    #[test]
    fn one_match_resolves_to_the_catalog_spelling_and_keeps_the_configured_string() {
        let got = resolve_captured_table("orders", &[m("public", "orders")])
            .expect("one match is the ordinary case");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("public", "orders")
        );
        assert_eq!(
            got.configured, "orders",
            "the operator's string is the destination sub-prefix — resolving must not \
             move an existing output path"
        );
    }

    /// The FOLD case: the operator writes `mixedcase`, the catalog spells it `MixedCase`, and wire events carry the catalog's spelling.
    #[test]
    fn resolution_takes_the_catalogs_case_not_the_configured_one() {
        let got = resolve_captured_table("mixedcase", &[m("public", "MixedCase")])
            .expect("the catalog found it");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("public", "MixedCase")
        );
        assert_eq!(got.configured, "mixedcase");
    }

    /// ONE relation reported through SEVERAL rules is one relation.
    #[test]
    fn duplicate_reports_of_the_same_relation_do_not_make_an_ambiguity() {
        let got = resolve_captured_table(
            "public.orders",
            &[m("public", "orders"), m("public", "orders")],
        )
        .expect("the same relation twice is not two relations");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("public", "orders")
        );
    }

    /// MANY is a refusal, always — no precedence.
    #[test]
    fn two_distinct_relations_are_refused_naming_both_whatever_their_shapes() {
        for (cfg, pair) in [
            ("orders", [m("public", "orders"), m("archive", "orders")]),
            (
                "MixedCase",
                [m("public", "MixedCase"), m("public", "mixedcase")],
            ),
            ("events.v2", [m("app", "events.v2"), m("events", "v2")]),
        ] {
            let err = resolve_captured_table(cfg, &pair)
                .expect_err("two distinct relations means the config does not name one");
            let msg = err.to_string();
            let a = format!("{}.{}", pair[0].schema, pair[0].table);
            let b = format!("{}.{}", pair[1].schema, pair[1].table);
            assert!(
                msg.contains(&a) && msg.contains(&b),
                "`{cfg}`: name BOTH — an operator qualifies only if they know which \
                 one they meant: {msg}"
            );
            assert!(msg.contains("Qualify"), "hand over the fix: {msg}");
        }
    }

    /// Nothing matched.
    #[test]
    fn a_name_the_catalog_does_not_know_is_a_resolution_failure() {
        let err = resolve_captured_table("ghost", &[]).expect_err("no relation, no capture");
        assert!(err.to_string().contains("ghost"));
    }

    /// A dotted MongoDB collection resolves WHOLE.
    #[test]
    fn a_dotted_collection_name_resolves_whole_when_it_is_the_only_reading() {
        let got = resolve_captured_table("events.v2", &[m("app", "events.v2")])
            .expect("one reading, one relation");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("app", "events.v2")
        );
    }

    #[test]
    fn table_matches_handles_bare_and_qualified_configs() {
        assert!(
            table_matches(CdcEngine::Postgres, "orders", "public", "orders"),
            "bare matches any schema"
        );
        assert!(
            table_matches(CdcEngine::Postgres, "public.orders", "public", "orders"),
            "qualified matches"
        );
        assert!(
            !table_matches(CdcEngine::Postgres, "audit.orders", "public", "orders"),
            "wrong schema differs"
        );
        assert!(
            !table_matches(CdcEngine::Postgres, "orders", "public", "users"),
            "different table differs"
        );
    }

    /// Mongo has NO schema qualifier, so the `schema.table` split arm must not run there.
    #[test]
    fn a_mongo_dotted_name_never_splits_into_a_schema_qualifier() {
        assert!(
            table_matches(CdcEngine::Mongo, "shopdb.orders", "shopdb", "shopdb.orders"),
            "the collection LITERALLY named `shopdb.orders` is the only reading"
        );
        assert!(
            !table_matches(CdcEngine::Mongo, "shopdb.orders", "shopdb", "orders"),
            "the sibling collection `orders` must NOT also match — that is the \
             interleave: two collections into one destination, counts intact"
        );
        assert!(table_matches(
            CdcEngine::Postgres,
            "public.orders",
            "public",
            "orders"
        ));
    }

    #[test]
    fn roast_dotted_collection_name_routes_by_full_name() {
        assert!(table_matches(
            CdcEngine::Mongo,
            "my.data",
            "shopdb",
            "my.data"
        ));
        assert!(!table_matches(
            CdcEngine::Mongo,
            "my.data",
            "shopdb",
            "my.other"
        ));
    }
    #[test]
    fn a_qualified_sql_config_never_matches_a_table_literally_named_by_it() {
        for engine in [
            CdcEngine::Postgres,
            CdcEngine::Mysql,
            CdcEngine::Mssql,
            CdcEngine::Oracle,
        ] {
            assert!(
                !table_matches(engine, "shop.orders", "other", "shop.orders"),
                "{engine:?}: `shop.orders` is schema `shop` + table `orders`, not a table named `shop.orders`"
            );
            assert!(
                table_matches(engine, "shop.orders", "shop", "orders"),
                "{engine:?}"
            );
            assert!(
                !table_matches(engine, "shop.orders", "shop", "other"),
                "{engine:?}"
            );
            assert!(
                !table_matches(engine, "shop.orders", "other", "orders"),
                "{engine:?}"
            );
            assert!(
                table_matches(engine, "orders", "any", "orders"),
                "{engine:?}: bare names are unchanged"
            );
            assert!(
                !table_matches(engine, "orders", "any", "orders2"),
                "{engine:?}"
            );
        }
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    fn rel(schema: &str, table: &str, kind: &str) -> CatalogMatch {
        CatalogMatch {
            schema: schema.into(),
            table: table.into(),
            kind: kind.into(),
            capturable: kind == "BASE TABLE",
        }
    }

    /// No match, two live matches, and two inert matches: three messages, one code (usage, exit 1).
    #[test]
    fn a_cdc_table_that_names_no_single_base_table_is_refused_by_code() {
        for matches in [
            vec![],
            vec![rel("a", "t", "BASE TABLE"), rel("b", "t", "BASE TABLE")],
            vec![rel("a", "t", "VIEW"), rel("b", "t", "VIEW")],
        ] {
            let err = resolve_captured_table("t", &matches).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_TABLE_UNRESOLVED", 1);
        }
    }
}
