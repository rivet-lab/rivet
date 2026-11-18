//! The server's type label for a type OID, domains read as their base type.

use std::collections::HashMap;

use postgres::Client;

use crate::error::Result;

/// One `pg_type` row: the OID, its `format_type` label, and the base OID when it is a domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TypeRow {
    pub oid: u32,
    pub label: String,
    pub domain_base: Option<u32>,
}

/// Every type OID of one database, to the label a value of that type decodes under.
#[derive(Debug, Default)]
pub(crate) struct TypeLabels {
    rows: HashMap<u32, TypeRow>,
}

/// A domain chain longer than this is a cycle or a catalog this reader does not understand.
const MAX_DOMAIN_DEPTH: usize = 32;

impl TypeLabels {
    /// The labels of `rows`.
    pub(crate) fn from_rows(rows: impl IntoIterator<Item = TypeRow>) -> Self {
        Self {
            rows: rows.into_iter().map(|r| (r.oid, r)).collect(),
        }
    }

    /// Read every type of the connected database.
    pub(crate) fn load(client: &mut Client) -> Result<Self> {
        let rows = client.query(
            "SELECT oid, format_type(oid, NULL), \
                    CASE WHEN typtype = 'd' THEN typbasetype END \
               FROM pg_type",
            &[],
        )?;
        Ok(Self::from_rows(rows.iter().map(|r| TypeRow {
            oid: r.get(0),
            label: r.get(1),
            domain_base: r.get(2),
        })))
    }

    /// The label of `oid` with domains followed to their base; `None` for an OID the catalog does not hold.
    pub(crate) fn label(&self, oid: u32) -> Option<&str> {
        let mut row = self.rows.get(&oid)?;
        for _ in 0..MAX_DOMAIN_DEPTH {
            match row.domain_base {
                None => return Some(&row.label),
                Some(base) => row = self.rows.get(&base)?,
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(oid: u32, label: &str, domain_base: Option<u32>) -> TypeRow {
        TypeRow {
            oid,
            label: label.to_string(),
            domain_base,
        }
    }

    /// A plain type keeps its label; a domain, and a domain over a domain, read as the base type's label.
    #[test]
    fn a_domain_chain_reads_as_its_base_types_label() {
        let t = TypeLabels::from_rows([
            row(23, "integer", None),
            row(16400, "app.pint", Some(23)),
            row(16401, "app.pint2", Some(16400)),
            row(16402, "app.mood", None),
        ]);
        assert_eq!(t.label(23), Some("integer"));
        assert_eq!(t.label(16400), Some("integer"));
        assert_eq!(t.label(16401), Some("integer"));
        assert_eq!(t.label(16402), Some("app.mood"));
    }

    /// An OID the catalog lacks, a domain whose base is missing, and a domain cycle are all `None`, never a guess.
    #[test]
    fn an_unknown_oid_a_missing_base_and_a_cycle_have_no_label() {
        let t = TypeLabels::from_rows([
            row(16400, "d1", Some(99)),
            row(16410, "c1", Some(16411)),
            row(16411, "c2", Some(16410)),
        ]);
        assert_eq!(t.label(7), None);
        assert_eq!(t.label(16400), None);
        assert_eq!(t.label(16410), None);
    }
}
