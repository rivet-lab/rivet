//! What a change position's order is, per source engine, as data.
//!
//! One [`Row`] per engine lists its ordered [`Part`]s. Validate compares them in Rust, the
//! BigQuery and Snowflake dedup views render them as SQL, and the ClickHouse `__ver` packs
//! them into an integer. Readers that stay outside, each on its own input type:
//! PostgreSQL `parse_lsn` (resume bound, `u32` halves), the Oracle `Scns` compare (two
//! parsed `u64`s) and the Mongo idle-poll compare (`_data` strings from the live token).

use serde_json::Value as Json;

/// One comparable component of a position.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Val {
    Num(u64),
    Text(String),
}

/// A position as its ordered components; the derived order is the engine's log order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Key(pub(crate) Vec<Val>);

/// How one field of a position is read and ordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartKind {
    /// A binlog file name, ordered by its numeric suffix.
    BinlogOrdinal,
    /// A JSON unsigned integer.
    Number,
    /// A decimal integer written as a JSON string.
    DecimalText,
    /// A PostgreSQL `hi/lo` LSN, two hex halves.
    PgLsn,
    /// Fixed-width hex text, lexical order equals numeric.
    FixedHex,
    /// Variable-length order-preserving text; no integer packing exists.
    KeyString,
}

/// A named field of the position JSON and how it orders.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Part {
    pub field: &'static str,
    pub kind: PartKind,
}

/// The SQL dialects that order a log by `ROW_NUMBER` over text expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dialect {
    BigQuery,
    Snowflake,
}

/// How a position's JSON shape selects its row.
#[derive(Debug, Clone, Copy)]
enum Sniff {
    Str(&'static str),
    StrWithKey(&'static str, &'static str),
    StrContaining(&'static str, char),
}

/// When the ClickHouse version expression takes this row's arm.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ChWhen {
    HasField(&'static str),
    Contains(&'static str, &'static str),
    Otherwise,
}

/// How a row's parts pack into the 128-bit position half of `__ver`.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ChPack {
    Single(Part),
    Pair(Part, Part),
}

/// A row's contribution to the ClickHouse version expression.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChArm {
    pub when: ChWhen,
    pub pack: ChPack,
}

/// One engine's position order.
#[derive(Debug)]
pub(crate) struct Row {
    sniff: Sniff,
    pub parts: &'static [Part],
    pub clickhouse: Option<ChArm>,
}

const FILE: Part = Part {
    field: "file",
    kind: PartKind::BinlogOrdinal,
};
const POS: Part = Part {
    field: "pos",
    kind: PartKind::Number,
};
const PG_LSN: Part = Part {
    field: "lsn",
    kind: PartKind::PgLsn,
};
const MSSQL_LSN: Part = Part {
    field: "lsn",
    kind: PartKind::FixedHex,
};

/// MySQL `{file, pos}`.
pub(crate) const MYSQL: Row = Row {
    sniff: Sniff::StrWithKey("file", "pos"),
    parts: &[FILE, POS],
    clickhouse: Some(ChArm {
        when: ChWhen::HasField("file"),
        pack: ChPack::Pair(FILE, POS),
    }),
};

/// Oracle `{low_water, commit_scn}`, ordered by the commit SCN; not loadable.
pub(crate) const ORACLE: Row = Row {
    sniff: Sniff::Str("commit_scn"),
    parts: &[Part {
        field: "commit_scn",
        kind: PartKind::DecimalText,
    }],
    clickhouse: None,
};

/// Mongo `{_data, rt}`, ordered by the resume token keystring.
pub(crate) const MONGO: Row = Row {
    sniff: Sniff::Str("_data"),
    parts: &[Part {
        field: "_data",
        kind: PartKind::KeyString,
    }],
    clickhouse: None,
};

/// PostgreSQL `{lsn: "hi/lo"}`.
pub(crate) const POSTGRES: Row = Row {
    sniff: Sniff::StrContaining("lsn", '/'),
    parts: &[PG_LSN],
    clickhouse: Some(ChArm {
        when: ChWhen::Contains("lsn", "/"),
        pack: ChPack::Single(PG_LSN),
    }),
};

/// SQL Server `{lsn: <fixed-width hex>}`.
pub(crate) const SQL_SERVER: Row = Row {
    sniff: Sniff::Str("lsn"),
    parts: &[MSSQL_LSN],
    clickhouse: Some(ChArm {
        when: ChWhen::Otherwise,
        pack: ChPack::Single(MSSQL_LSN),
    }),
};

/// Every row, in the order a position's shape is matched.
pub(crate) const ROWS: [&Row; 5] = [&MYSQL, &ORACLE, &MONGO, &POSTGRES, &SQL_SERVER];

/// The numeric suffix of a binlog file name (`mysql-bin.000042` is 42).
pub(crate) fn binlog_ordinal(name: &str) -> Option<u64> {
    name.rsplit_once('.')?.1.parse().ok()
}

/// A PostgreSQL `hi/lo` LSN as one number; a low half wider than 32 bits is malformed.
fn pg_lsn(lsn: &str) -> Option<u64> {
    let (hi, lo) = lsn.split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    if lo >> 32 != 0 {
        return None;
    }
    Some((hi << 32) + lo)
}

/// The position's order key, matching its shape to a row; `None` when no row reads it.
pub(crate) fn key(v: &Json) -> Option<Key> {
    ROWS.iter().find(|r| r.sniff.matches(v))?.key(v)
}

impl Sniff {
    /// Whether `v` has this shape.
    fn matches(self, v: &Json) -> bool {
        let text = |f: &str| v.get(f).and_then(Json::as_str);
        match self {
            Sniff::Str(f) => text(f).is_some(),
            Sniff::StrWithKey(f, k) => text(f).is_some() && v.get(k).is_some(),
            Sniff::StrContaining(f, c) => text(f).is_some_and(|s| s.contains(c)),
        }
    }
}

impl Row {
    /// The order key of a position of this row's shape.
    pub(crate) fn key(&self, v: &Json) -> Option<Key> {
        self.parts
            .iter()
            .map(|p| p.kind.value(v.get(p.field)?))
            .collect::<Option<Vec<_>>>()
            .map(Key)
    }

    /// The first part's raw text, as the bounded drains compare it.
    pub(crate) fn first_text(&self, v: &Json) -> Option<String> {
        let first = self.parts.first()?;
        v.get(first.field)?.as_str().map(String::from)
    }

    /// The `ORDER BY` expressions for the dedup view, most significant first.
    pub(crate) fn sql(&self, dialect: Dialect) -> Vec<String> {
        self.parts
            .iter()
            .flat_map(|p| p.kind.sql(dialect, p.field))
            .collect()
    }
}

impl PartKind {
    /// The comparable value of a field.
    fn value(self, v: &Json) -> Option<Val> {
        match self {
            PartKind::BinlogOrdinal => {
                let name = v.as_str()?;
                Some(match binlog_ordinal(name) {
                    Some(ord) => Val::Num(ord),
                    None => Val::Text(name.to_string()),
                })
            }
            PartKind::Number => v.as_u64().map(Val::Num),
            PartKind::DecimalText => v.as_str()?.parse().ok().map(Val::Num),
            PartKind::PgLsn => pg_lsn(v.as_str()?).map(Val::Num),
            PartKind::FixedHex | PartKind::KeyString => v.as_str().map(|s| Val::Text(s.into())),
        }
    }

    /// The SQL expressions ordering this field in a dialect.
    fn sql(self, dialect: Dialect, f: &str) -> Vec<String> {
        use Dialect::{BigQuery, Snowflake};
        match (dialect, self) {
            (BigQuery, PartKind::BinlogOrdinal) => vec![format!(
                "CAST(REGEXP_EXTRACT(JSON_VALUE(__pos,'$.{f}'), r'[0-9]+$') AS INT64)"
            )],
            (Snowflake, PartKind::BinlogOrdinal) => vec![format!(
                "TO_NUMBER(REGEXP_SUBSTR(PARSE_JSON(__pos):{f}::string, '[0-9]+$'))"
            )],
            (BigQuery, PartKind::Number | PartKind::DecimalText) => {
                vec![format!("CAST(JSON_VALUE(__pos,'$.{f}') AS INT64)")]
            }
            (Snowflake, PartKind::Number | PartKind::DecimalText) => {
                vec![format!("PARSE_JSON(__pos):{f}::integer")]
            }
            (BigQuery, PartKind::PgLsn) => (0..2)
                .map(|i| format!("LPAD(SPLIT(JSON_VALUE(__pos,'$.{f}'),'/')[OFFSET({i})],8,'0')"))
                .collect(),
            (Snowflake, PartKind::PgLsn) => (1..=2)
                .map(|i| format!("LPAD(SPLIT_PART(PARSE_JSON(__pos):{f}::string,'/',{i}),8,'0')"))
                .collect(),
            (BigQuery, PartKind::FixedHex | PartKind::KeyString) => {
                vec![format!("JSON_VALUE(__pos,'$.{f}')")]
            }
            (Snowflake, PartKind::FixedHex | PartKind::KeyString) => {
                vec![format!("PARSE_JSON(__pos):{f}::string")]
            }
        }
    }

    /// The ClickHouse `UInt128` expression for this field, or `None` when it has no integer form.
    fn clickhouse(self, f: &str) -> Option<String> {
        let text = format!("JSONExtractString(ifNull(__pos, ''), '{f}')");
        match self {
            PartKind::BinlogOrdinal => Some(format!(
                "toUInt128(toUInt64OrZero(extract({text}, '[0-9]+$')))"
            )),
            PartKind::Number => Some(format!(
                "toUInt128(JSONExtractUInt(ifNull(__pos, ''), '{f}'))"
            )),
            PartKind::PgLsn => Some(format!(
                "toUInt128(reinterpretAsUInt64(reverse(unhex(concat(\
leftPad(splitByChar('/', {text})[1], 8, '0'), \
leftPad(splitByChar('/', {text})[2], 8, '0'))))))"
            )),
            PartKind::FixedHex => Some(format!(
                "reinterpretAsUInt128(reverse(unhex(leftPad({text}, 32, '0'))))"
            )),
            PartKind::DecimalText | PartKind::KeyString => None,
        }
    }
}

impl ChArm {
    /// The arm's `UInt128` value, or `None` when a part has no integer form.
    fn value(self) -> Option<String> {
        match self.pack {
            ChPack::Single(p) => p.kind.clickhouse(p.field),
            ChPack::Pair(hi, lo) => Some(format!(
                "bitOr(bitShiftLeft({}, 64), {})",
                hi.kind.clickhouse(hi.field)?,
                lo.kind.clickhouse(lo.field)?
            )),
        }
    }

    /// The field whose presence marks this arm's shape.
    fn marker(self) -> &'static str {
        match self.when {
            ChWhen::HasField(f) | ChWhen::Contains(f, _) => f,
            ChWhen::Otherwise => match self.pack {
                ChPack::Single(p) | ChPack::Pair(p, _) => p.field,
            },
        }
    }
}

/// The ClickHouse change log's version: `(source position << 128) | (__seq + 1)`, the
/// position decoded from the `__pos` JSON's own shape; an unknown shape fails the insert (ADR-0035 CH3).
pub(crate) fn clickhouse_version_expr() -> String {
    let arms: Vec<(ChArm, String)> = ROWS
        .iter()
        .filter_map(|r| r.clickhouse)
        .filter_map(|a| Some((a, a.value()?)))
        .collect();
    let mut cases = String::new();
    for (arm, value) in &arms {
        match arm.when {
            ChWhen::HasField(f) => {
                cases += &format!("JSONHas(ifNull(__pos, ''), '{f}'), {value}, ");
            }
            ChWhen::Contains(f, needle) => {
                cases += &format!(
                    "position(JSONExtractString(ifNull(__pos, ''), '{f}'), '{needle}') > 0, \
{value}, "
                );
            }
            ChWhen::Otherwise => cases += value,
        }
    }
    let mut markers: Vec<&str> = Vec::new();
    for (arm, _) in &arms {
        if !markers.contains(&arm.marker()) {
            markers.push(arm.marker());
        }
    }
    let known = markers
        .iter()
        .map(|f| format!("JSONHas(ifNull(__pos, ''), '{f}')"))
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "bitOr(bitShiftLeft(toUInt256(multiIf(__pos IS NULL, toUInt128(0), {cases})), 128), \
toUInt256(ifNull(__seq, -1) + 1)) + toUInt256(throwIf(\
__pos IS NOT NULL AND NOT ({known}), \
'rivet: unrecognised __pos shape — no source position to version this row by (ADR-0035 CH3)'))"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const VERSION_EXPR: &str = concat!(
        "bitOr(bitShiftLeft(toUInt256(multiIf(__pos IS NULL, toUInt128(0), JSONHas(ifNull(__pos",
        ", ''), 'file'), bitOr(bitShiftLeft(toUInt128(toUInt64OrZero(extract(JSONExtractString(",
        "ifNull(__pos, ''), 'file'), '[0-9]+$'))), 64), toUInt128(JSONExtractUInt(ifNull(__pos,",
        " ''), 'pos'))), position(JSONExtractString(ifNull(__pos, ''), 'lsn'), '/') > 0, toUInt",
        "128(reinterpretAsUInt64(reverse(unhex(concat(leftPad(splitByChar('/', JSONExtractStrin",
        "g(ifNull(__pos, ''), 'lsn'))[1], 8, '0'), leftPad(splitByChar('/', JSONExtractString(i",
        "fNull(__pos, ''), 'lsn'))[2], 8, '0')))))), reinterpretAsUInt128(reverse(unhex(leftPad",
        "(JSONExtractString(ifNull(__pos, ''), 'lsn'), 32, '0')))))), 128), toUInt256(ifNull(__",
        "seq, -1) + 1)) + toUInt256(throwIf(__pos IS NOT NULL AND NOT (JSONHas(ifNull(__pos, ''",
        "), 'file') OR JSONHas(ifNull(__pos, ''), 'lsn')), 'rivet: unrecognised __pos shape — n",
        "o source position to version this row by (ADR-0035 CH3)'))",
    );

    fn k(s: &str) -> Option<Key> {
        key(&serde_json::from_str::<Json>(s).unwrap())
    }

    #[test]
    fn binlog_ordinal_parses_the_numeric_suffix() {
        assert_eq!(binlog_ordinal("mysql-bin.000042"), Some(42));
        assert_eq!(binlog_ordinal("binlog.1000000"), Some(1_000_000));
        assert_eq!(binlog_ordinal("my.replica.000007"), Some(7));
        assert_eq!(binlog_ordinal("no-suffix"), None);
        assert_eq!(binlog_ordinal("binlog.notanum"), None);
    }

    #[test]
    fn the_clickhouse_version_is_the_pinned_expression() {
        assert_eq!(clickhouse_version_expr(), VERSION_EXPR);
    }

    #[test]
    fn every_clickhouse_arm_has_an_integer_form_and_otherwise_is_last() {
        let arms: Vec<ChArm> = ROWS.iter().filter_map(|r| r.clickhouse).collect();
        assert_eq!(arms.len(), 3);
        assert!(arms.iter().all(|a| a.value().is_some()));
        let otherwise: Vec<usize> = arms
            .iter()
            .enumerate()
            .filter(|(_, a)| matches!(a.when, ChWhen::Otherwise))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(otherwise, vec![arms.len() - 1]);
    }

    #[test]
    fn a_row_without_an_integer_form_has_no_clickhouse_arm() {
        assert!(MONGO.clickhouse.is_none() && ORACLE.clickhouse.is_none());
        assert!(PartKind::KeyString.clickhouse("_data").is_none());
        assert!(PartKind::DecimalText.clickhouse("commit_scn").is_none());
    }

    #[test]
    fn each_row_renders_its_order_for_both_dialects() {
        let sql = |r: &Row, d| r.sql(d);
        assert_eq!(
            sql(&MYSQL, Dialect::BigQuery),
            [
                "CAST(REGEXP_EXTRACT(JSON_VALUE(__pos,'$.file'), r'[0-9]+$') AS INT64)",
                "CAST(JSON_VALUE(__pos,'$.pos') AS INT64)"
            ]
        );
        assert_eq!(
            sql(&MYSQL, Dialect::Snowflake),
            [
                "TO_NUMBER(REGEXP_SUBSTR(PARSE_JSON(__pos):file::string, '[0-9]+$'))",
                "PARSE_JSON(__pos):pos::integer"
            ]
        );
        assert_eq!(
            sql(&POSTGRES, Dialect::BigQuery),
            [
                "LPAD(SPLIT(JSON_VALUE(__pos,'$.lsn'),'/')[OFFSET(0)],8,'0')",
                "LPAD(SPLIT(JSON_VALUE(__pos,'$.lsn'),'/')[OFFSET(1)],8,'0')"
            ]
        );
        assert_eq!(
            sql(&POSTGRES, Dialect::Snowflake),
            [
                "LPAD(SPLIT_PART(PARSE_JSON(__pos):lsn::string,'/',1),8,'0')",
                "LPAD(SPLIT_PART(PARSE_JSON(__pos):lsn::string,'/',2),8,'0')"
            ]
        );
        assert_eq!(
            sql(&SQL_SERVER, Dialect::BigQuery),
            ["JSON_VALUE(__pos,'$.lsn')"]
        );
        assert_eq!(
            sql(&SQL_SERVER, Dialect::Snowflake),
            ["PARSE_JSON(__pos):lsn::string"]
        );
        assert_eq!(
            sql(&MONGO, Dialect::BigQuery),
            ["JSON_VALUE(__pos,'$._data')"]
        );
        assert_eq!(
            sql(&MONGO, Dialect::Snowflake),
            ["PARSE_JSON(__pos):_data::string"]
        );
        assert_eq!(
            sql(&ORACLE, Dialect::BigQuery),
            ["CAST(JSON_VALUE(__pos,'$.commit_scn') AS INT64)"]
        );
        assert_eq!(
            sql(&ORACLE, Dialect::Snowflake),
            ["PARSE_JSON(__pos):commit_scn::integer"]
        );
    }

    #[test]
    fn a_position_matches_the_first_row_that_reads_its_shape() {
        let mysql = r#"{"file":"binlog.000007","pos":4,"server_uuid":"u"}"#;
        assert_eq!(k(mysql), Some(Key(vec![Val::Num(7), Val::Num(4)])));
        assert_eq!(
            k(r#"{"low_water":"5","commit_scn":"9"}"#),
            Some(Key(vec![Val::Num(9)]))
        );
        assert_eq!(
            k(r#"{"_data":"826A","rt":"0e"}"#),
            Some(Key(vec![Val::Text("826A".into())]))
        );
        assert_eq!(
            k(r#"{"lsn":"3C/1A"}"#),
            Some(Key(vec![Val::Num(0x3C_0000_001A)]))
        );
        assert_eq!(
            k(r#"{"lsn":"0000002a000001f80003","pinned":true}"#),
            Some(Key(vec![Val::Text("0000002a000001f80003".into())]))
        );
        assert_eq!(k(r#"{"nope":1}"#), None);
        assert_eq!(k("[]"), None);
    }

    #[test]
    fn a_matched_shape_with_an_unreadable_part_is_unparseable_not_re_matched() {
        assert_eq!(k(r#"{"file":"binlog.000001","pos":"4","lsn":"0/1"}"#), None);
        assert_eq!(
            k(r#"{"file":"binlog.000001","lsn":"0/1"}"#),
            Some(Key(vec![Val::Num(1)]))
        );
        assert_eq!(k(r#"{"commit_scn":"x","_data":"82"}"#), None);
        assert_eq!(
            k(r#"{"commit_scn":9,"_data":"82"}"#),
            Some(Key(vec![Val::Text("82".into())]))
        );
        assert_eq!(k(r#"{"lsn":"1/1FFFFFFFFF"}"#), None);
        assert_eq!(k(r#"{"lsn":"zz/1"}"#), None);
        assert_eq!(k(r#"{"lsn":"1/zz"}"#), None);
    }

    #[test]
    fn a_binlog_name_without_a_suffix_keeps_its_name() {
        assert_eq!(
            k(r#"{"file":"binlog","pos":1}"#),
            Some(Key(vec![Val::Text("binlog".into()), Val::Num(1)]))
        );
        assert_ne!(
            k(r#"{"file":"binlog","pos":1}"#),
            k(r#"{"file":"binlog.000001","pos":1}"#)
        );
    }

    #[test]
    fn numbers_compare_as_numbers_and_keystrings_as_text() {
        let scn = |n: &str| k(&format!(r#"{{"commit_scn":"{n}"}}"#));
        assert!(scn("999") < scn("1000"));
        assert!(k(r#"{"lsn":"0/FFFFFFFF"}"#) < k(r#"{"lsn":"1/0"}"#));
        assert!(k(r#"{"lsn":"3C/9"}"#) < k(r#"{"lsn":"3C/1A"}"#));
        assert!(
            k(r#"{"file":"binlog.999999","pos":9}"#) < k(r#"{"file":"binlog.1000000","pos":1}"#)
        );
        assert!(k(r#"{"_data":"8264AA"}"#) < k(r#"{"_data":"8264AB"}"#));
    }

    #[test]
    fn the_first_text_is_the_raw_first_part() {
        let v = serde_json::json!({"_data": "82AB", "rt": "0e"});
        assert_eq!(MONGO.first_text(&v), Some("82AB".to_string()));
        assert_eq!(MONGO.first_text(&serde_json::json!({"_data": 5})), None);
        assert_eq!(MONGO.first_text(&serde_json::json!({})), None);
    }
}
