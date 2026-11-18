//! `source.columns_from: grants`, engine-neutral: the row projection, the key-readability check and the denial coding.

use std::collections::HashMap;

use crate::config::SourceType;
use crate::error::Result;
use crate::source::cdc::ReadableColumns;
use crate::sql::quote_ident;

/// How an engine compares two column names (MySQL folds ASCII case).
pub(crate) type NamesMatch = fn(&str, &str) -> bool;

/// The server's text when `err` is an engine's typed "not yours to read" verdict.
pub(crate) fn denied_by_server(err: &anyhow::Error) -> Option<String> {
    super::mysql::column_grants::denied_by_server(err)
}

/// `SELECT` of `columns` from `qualifier.table`, every name quoted for `engine`.
pub(crate) fn projection_sql(
    engine: SourceType,
    qualifier: Option<&str>,
    table: &str,
    columns: &[String],
) -> String {
    let quote = |name: &str| quote_ident(engine, name);
    let cols: Vec<String> = columns.iter().map(|c| quote(c)).collect();
    let rel = match qualifier {
        Some(q) => format!("{}.{}", quote(q), quote(table)),
        None => quote(table),
    };
    format!("SELECT {} FROM {rel}", cols.join(", "))
}

/// The positions in `names` of the columns in `readable`, names compared by `same`.
pub(crate) fn kept_positions(
    names: &[String],
    readable: &[String],
    same: NamesMatch,
) -> Vec<usize> {
    names
        .iter()
        .enumerate()
        .filter(|(_, n)| readable.iter().any(|r| same(r, n)))
        .map(|(i, _)| i)
        .collect()
}

/// A projected update whose images are equal and fully decoded: only a hidden column changed.
pub(crate) fn changes_only_hidden<T: PartialEq>(
    projected: bool,
    before: Option<&(Vec<T>, Vec<usize>)>,
    after: Option<&(Vec<T>, Vec<usize>)>,
) -> bool {
    projected
        && matches!((before, after), (Some((b, bb)), Some((a, ab))) if bb.is_empty() && ab.is_empty() && b == a)
}

/// A present image that does not carry one value per column of the table map.
pub(crate) fn is_partial<T>(full: usize, row: Option<&(Vec<T>, Vec<usize>)>) -> bool {
    row.is_some_and(|(vals, _)| vals.len() != full)
}

/// `row` cut to `keep`, its undecodable positions renumbered into the cut row.
pub(crate) fn project_row<T>(
    (vals, bad): (Vec<T>, Vec<usize>),
    keep: &[usize],
) -> (Vec<T>, Vec<usize>) {
    let bad = keep
        .iter()
        .enumerate()
        .filter(|(_, k)| bad.contains(k))
        .map(|(i, _)| i)
        .collect();
    let mut slots: Vec<Option<T>> = vals.into_iter().map(Some).collect();
    let vals = keep
        .iter()
        .filter_map(|&k| slots.get_mut(k).and_then(Option::take))
        .collect();
    (vals, bad)
}

/// The catalog reads the key-readability check asks of an engine, per configured table.
pub(crate) trait KeyCatalog {
    /// The table as the refusal names it (`db.table`).
    fn relation(&self, configured: &str) -> Result<String>;
    /// The primary key of the table in key order; empty for a table without one.
    fn primary_key(&mut self, configured: &str) -> Result<Vec<String>>;
    /// Whether the server's typed verdict refuses the account a read of `column`; any other server answer is `false`.
    fn denies(&mut self, configured: &str, column: &str) -> Result<bool>;
}

/// Refuse a table whose key (its `load.pk` or its primary key) has a column the account may not read.
pub(crate) fn require_readable_keys(
    engine: &str,
    catalog: &mut impl KeyCatalog,
    configured: &[String],
    readable: &HashMap<String, ReadableColumns>,
    declared: &HashMap<String, Vec<String>>,
    same: NamesMatch,
) -> Result<()> {
    for cfg in configured {
        let rel = catalog.relation(cfg)?;
        let (key, from) = match declared.get(cfg) {
            Some(k) => (k.clone(), "the `load.pk` of this export"),
            None => (catalog.primary_key(cfg)?, "the primary key"),
        };
        let shown = &readable[cfg].columns;
        for k in key.iter().filter(|k| !shown.iter().any(|c| same(c, k))) {
            if catalog.denies(cfg, k)?
                && seeded!("column-grants-unreadable-key-accepted", false, true)
            {
                crate::rivet_bail!(
                    crate::error::codes::SOURCE_COLUMNS_UNREADABLE,
                    "{engine}: `{rel}`: key column `{k}` ({from}) is not readable under \
                     `columns_from: grants`, so changes to a row could not be told apart; \
                     nothing was read or written. GRANT SELECT on it, or name a readable \
                     unique key in `load.pk`"
                );
            }
        }
    }
    Ok(())
}

/// Code an export error under `columns_from: grants`: a refused column is COLUMNS_UNREADABLE for a batch, GRANTS_CHANGED for a stream leg.
pub(crate) fn code_a_revoked_column(
    err: anyhow::Error,
    export: &str,
    grants_on: bool,
    cdc_leg: bool,
) -> anyhow::Error {
    if seeded!(
        "column-grants-revoked-column-uncoded",
        true,
        !grants_on || crate::error::code_of(&err).is_some()
    ) {
        return err;
    }
    let Some(said) = denied_by_server(&err) else {
        return err;
    };
    if cdc_leg {
        return crate::error::coded(
            crate::error::codes::SOURCE_CDC_GRANTS_CHANGED,
            format!(
                "export '{export}': the account may no longer SELECT a column this snapshot reads \
                 under `columns_from: grants` (the server said: {said}). The grants changed after \
                 this run read them; the snapshot of this table was not completed and no change \
                 stream was opened. Re-run: the run then reads the columns as they are now"
            ),
        );
    }
    crate::error::coded(
        crate::error::codes::SOURCE_COLUMNS_UNREADABLE,
        format!(
            "export '{export}': the account may no longer SELECT a column this run reads under \
             `columns_from: grants` (the server said: {said}). The grants changed after the plan \
             or the settings named that column; nothing of this run was delivered. Run `rivet plan` \
             again so the plan reads the columns the account may SELECT now, or GRANT SELECT on it"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::mysql::column_grants::server_said;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn an_identifier_is_quoted_for_any_text() {
        assert_eq!(quote_ident(SourceType::Mysql, "a`b"), "`a``b`");
        assert_eq!(
            projection_sql(
                SourceType::Mysql,
                Some("d b"),
                "t",
                &s(&["id", "x`y", "ім'я"]) // allow-cyrillic
            ),
            "SELECT `id`, `x``y`, `ім'я` FROM `d b`.`t`" // allow-cyrillic
        );
        assert_eq!(
            projection_sql(SourceType::Mysql, None, "t", &s(&["id"])),
            "SELECT `id` FROM `t`"
        );
        assert_eq!(
            projection_sql(SourceType::Postgres, Some("a\"b"), "t", &s(&["id"])),
            "SELECT \"id\" FROM \"a\"\"b\".\"t\""
        );
    }

    #[test]
    fn a_row_keeps_only_readable_columns_and_their_own_poison() {
        let names = s(&["id", "secret", "Name", "pass"]);
        let keep = kept_positions(&names, &s(&["ID", "name"]), str::eq_ignore_ascii_case);
        assert_eq!(keep, vec![0, 2]);
        assert_eq!(
            kept_positions(&names, &s(&["ID", "name"]), |a, b| a == b),
            Vec::<usize>::new(),
            "the engine's own comparison decides"
        );
        let row = (vec!["1", "tok", "ann", "pw"], vec![1, 2]);
        assert_eq!(project_row(row, &keep), (vec!["1", "ann"], vec![1]));
        assert_eq!(
            project_row((vec!["1"], vec![]), &[]),
            (Vec::<&str>::new(), vec![])
        );
    }

    #[test]
    fn only_an_equal_fully_decoded_update_is_hidden_only() {
        let row = |v: &[&'static str], bad: &[usize]| (v.to_vec(), bad.to_vec());
        let same = row(&["1", "ann"], &[]);
        assert!(changes_only_hidden(true, Some(&same), Some(&same)));
        assert!(
            !changes_only_hidden(false, Some(&same), Some(&same)),
            "no projection, no hiding"
        );
        assert!(!changes_only_hidden(
            true,
            Some(&same),
            Some(&row(&["1", "bob"], &[]))
        ));
        assert!(
            !changes_only_hidden(true, None, Some(&same)),
            "an insert is delivered"
        );
        assert!(
            !changes_only_hidden(true, Some(&same), None),
            "a delete is delivered"
        );
        let poisoned = row(&["1", "ann"], &[1]);
        assert!(!changes_only_hidden(true, Some(&poisoned), Some(&poisoned)));
        assert!(!changes_only_hidden(true, Some(&same), Some(&poisoned)));
        assert!(
            !changes_only_hidden(true, Some(&row(&["1"], &[])), Some(&same)),
            "a thin before image proves nothing"
        );
    }

    #[test]
    fn an_image_is_partial_when_it_holds_fewer_values_than_the_table_has_columns() {
        let full = (vec![1, 2, 3], vec![]);
        let minimal = (vec![1], vec![]);
        assert!(!is_partial(3, Some(&full)));
        assert!(is_partial(3, Some(&minimal)));
        assert!(
            !is_partial::<i32>(3, None),
            "an absent image is not a partial one"
        );
        assert!(
            is_partial(3, Some(&(vec![1, 2, 3, 4], vec![]))),
            "more values than names is not whole either"
        );
    }

    #[test]
    fn a_denied_read_of_a_snapshot_leg_is_a_grant_change_with_the_rerun_remedy() {
        let code = crate::error::codes::SOURCE_CDC_GRANTS_CHANGED;
        let coded = code_a_revoked_column(server_said(1143), "orders", true, true);
        assert_eq!(crate::error::code_of(&coded), Some(code));
        let text = format!("{coded:#}");
        assert!(
            text.contains("orders") && text.contains("Re-run") && !text.contains("rivet plan"),
            "{text}"
        );
        let other = code_a_revoked_column(server_said(1054), "orders", true, true);
        assert!(crate::error::code_of(&other).is_none());
        let off = code_a_revoked_column(server_said(1143), "orders", false, true);
        assert!(crate::error::code_of(&off).is_none());
    }

    #[test]
    fn a_denied_read_under_the_policy_is_coded_with_the_replan_remedy_and_nothing_else_is() {
        let code = crate::error::codes::SOURCE_COLUMNS_UNREADABLE;
        let coded = code_a_revoked_column(server_said(1143), "orders", true, false);
        assert_eq!(crate::error::code_of(&coded), Some(code));
        let text = format!("{coded:#}");
        assert!(
            text.contains("orders") && text.contains("rivet plan") && text.contains("GRANT SELECT"),
            "{text}"
        );
        assert!(
            !text.contains("1143"),
            "the raw driver error does not stay in the chain: {text}"
        );
        let off = code_a_revoked_column(server_said(1143), "orders", false, false);
        assert!(
            crate::error::code_of(&off).is_none(),
            "no policy, no recoding"
        );
        let other = code_a_revoked_column(server_said(1054), "orders", true, false);
        assert!(crate::error::code_of(&other).is_none());
        let already = crate::error::coded(crate::error::codes::SOURCE_CDC_GRANTS_CHANGED, "moved");
        let kept = code_a_revoked_column(already, "orders", true, false);
        assert_eq!(
            crate::error::code_of(&kept),
            Some(crate::error::codes::SOURCE_CDC_GRANTS_CHANGED),
            "a coded error keeps its code"
        );
    }

    struct Catalog {
        key: Vec<String>,
        denied: Vec<&'static str>,
        asked: Vec<String>,
    }

    impl KeyCatalog for Catalog {
        fn relation(&self, configured: &str) -> Result<String> {
            Ok(format!("db.{configured}"))
        }

        fn primary_key(&mut self, _configured: &str) -> Result<Vec<String>> {
            Ok(self.key.clone())
        }

        fn denies(&mut self, _configured: &str, column: &str) -> Result<bool> {
            self.asked.push(column.to_string());
            Ok(self.denied.contains(&column))
        }
    }

    fn shown(columns: &[&str]) -> HashMap<String, ReadableColumns> {
        let columns = s(columns);
        let select = projection_sql(SourceType::Mysql, Some("db"), "t", &columns);
        HashMap::from([("t".to_string(), ReadableColumns { columns, select })])
    }

    fn check(
        catalog: &mut Catalog,
        readable: &[&str],
        declared: &HashMap<String, Vec<String>>,
    ) -> Result<()> {
        require_readable_keys(
            "mysql",
            catalog,
            &s(&["t"]),
            &shown(readable),
            declared,
            str::eq_ignore_ascii_case,
        )
    }

    #[test]
    fn a_key_column_the_account_may_not_read_is_refused_naming_its_source() {
        let mut c = Catalog {
            key: s(&["id"]),
            denied: vec!["id"],
            asked: vec![],
        };
        let none = HashMap::new();
        let e = check(&mut c, &["name"], &none).expect_err("an unreadable primary key");
        assert_eq!(
            crate::error::code_of(&e),
            Some(crate::error::codes::SOURCE_COLUMNS_UNREADABLE)
        );
        let text = format!("{e:#}");
        assert!(
            text.contains("mysql: `db.t`: key column `id` (the primary key)")
                && text.contains("GRANT SELECT"),
            "{text}"
        );
        let declared = HashMap::from([("t".to_string(), s(&["id"]))]);
        let e = check(&mut c, &["name"], &declared).expect_err("an unreadable declared key");
        assert!(format!("{e:#}").contains("(the `load.pk` of this export)"));
    }

    #[test]
    fn a_readable_key_is_not_asked_of_the_server_and_a_non_denial_passes() {
        let mut c = Catalog {
            key: s(&["ID"]),
            denied: vec![],
            asked: vec![],
        };
        check(&mut c, &["id", "name"], &HashMap::new()).expect("a readable key");
        assert!(c.asked.is_empty(), "{:?}", c.asked);
        check(&mut c, &["name"], &HashMap::new()).expect("a server answer that is no denial");
        assert_eq!(c.asked, s(&["ID"]));
    }

    #[test]
    fn a_declared_key_is_used_without_reading_the_primary_key() {
        struct NoCatalogKey;
        impl KeyCatalog for NoCatalogKey {
            fn relation(&self, _: &str) -> Result<String> {
                Ok("db.t".into())
            }
            fn primary_key(&mut self, _: &str) -> Result<Vec<String>> {
                panic!("the primary key must not be read when `load.pk` names the key")
            }
            fn denies(&mut self, _: &str, _: &str) -> Result<bool> {
                Ok(false)
            }
        }
        let declared = HashMap::from([("t".to_string(), s(&["id"]))]);
        require_readable_keys(
            "mysql",
            &mut NoCatalogKey,
            &s(&["t"]),
            &shown(&["id"]),
            &declared,
            str::eq_ignore_ascii_case,
        )
        .expect("a declared readable key");
    }
}
