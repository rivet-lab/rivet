//! Guard: the "Types by kind" table in docs/type-mapping.md matches `CommonKind` and the engines' own tables.

use super::kind::CommonKind;
use super::override_table::Engine;

const DOC: &str = include_str!("../../docs/type-mapping.md");
const KIND_SOURCE: &str = include_str!("kind.rs");

const ALL: [CommonKind; 16] = [
    CommonKind::Bool,
    CommonKind::I16,
    CommonKind::I32,
    CommonKind::I64,
    CommonKind::F32,
    CommonKind::F64,
    CommonKind::Dec,
    CommonKind::Date,
    CommonKind::Naive,
    CommonKind::Utc,
    CommonKind::Text,
    CommonKind::Binary,
    CommonKind::Json,
    CommonKind::Time,
    CommonKind::Uuid,
    CommonKind::Interval,
];

/// The name the doc table gives a kind in its first column.
fn doc_name(kind: CommonKind) -> &'static str {
    match kind {
        CommonKind::Bool => "bool",
        CommonKind::I16 => "int16",
        CommonKind::I32 => "int32",
        CommonKind::I64 => "int64",
        CommonKind::F32 => "float32",
        CommonKind::F64 => "float64",
        CommonKind::Dec => "decimal",
        CommonKind::Date => "date",
        CommonKind::Naive => "timestamp",
        CommonKind::Utc => "timestamp_tz",
        CommonKind::Text => "text",
        CommonKind::Binary => "binary",
        CommonKind::Json => "json",
        CommonKind::Time => "time",
        CommonKind::Uuid => "uuid",
        CommonKind::Interval => "interval",
    }
}

/// The type name a kind is delivered as, without its parameters.
fn delivered_name(kind: CommonKind) -> String {
    match kind.rivet_type() {
        Some(t) => t.label().split('(').next().unwrap_or_default().to_string(),
        None => "decimal".to_string(),
    }
}

/// One row of the doc table: kind, delivered-as and one cell per engine column.
struct Row {
    kind: String,
    delivered: String,
    cells: Vec<String>,
}

/// The rows of the table under the "Types by kind" heading of `doc`.
fn rows(doc: &str) -> Result<Vec<Row>, String> {
    let body = doc
        .split("\n## Types by kind\n")
        .nth(1)
        .ok_or("no `## Types by kind` heading")?;
    let mut rows = Vec::new();
    for line in body
        .lines()
        .skip_while(|l| !l.starts_with('|'))
        .take_while(|l| l.starts_with('|'))
        .skip(2)
    {
        let cells: Vec<String> = line
            .trim()
            .trim_matches('|')
            .split('|')
            .map(|c| c.trim().to_string())
            .collect();
        if cells.len() != 6 {
            return Err(format!("a row has {} cells, not 6: {line}", cells.len()));
        }
        rows.push(Row {
            kind: cells[0].clone(),
            delivered: cells[1].trim_matches('`').to_string(),
            cells: cells[2..].to_vec(),
        });
    }
    Ok(rows)
}

/// Fails unless `doc` lists every common kind once, in code order, with the type the code delivers it as.
fn check_kinds(doc: &str) -> Result<(), String> {
    let rows = rows(doc)?;
    let doc_kinds: Vec<&str> = rows.iter().map(|r| r.kind.as_str()).collect();
    let code_kinds: Vec<&str> = ALL.iter().map(|k| doc_name(*k)).collect();
    if doc_kinds != code_kinds {
        return Err(format!(
            "doc kinds {doc_kinds:?} differ from code {code_kinds:?}"
        ));
    }
    for (row, kind) in rows.iter().zip(ALL) {
        let doc_delivered = row.delivered.split('(').next().unwrap_or_default();
        if doc_delivered != delivered_name(kind) {
            return Err(format!(
                "{kind:?} is delivered as `{}` in the doc, `{}` in code",
                row.delivered,
                delivered_name(kind)
            ));
        }
    }
    Ok(())
}

/// Fails unless the doc's column for `engine` is `none` for exactly the kinds not in `seen`.
fn check_engine(doc: &str, engine: Engine, seen: &[CommonKind]) -> Result<(), String> {
    let column = match engine {
        Engine::Postgres => 0,
        Engine::Mysql => 1,
        Engine::Mssql => 2,
        Engine::Oracle => 3,
        Engine::Mongo => return Err("Mongo has no column in the by-kind table".into()),
    };
    for (row, kind) in rows(doc)?.iter().zip(ALL) {
        let cell = &row.cells[column];
        let in_doc = !cell.starts_with("none");
        let in_code = seen.contains(&kind);
        if in_doc != in_code {
            return Err(format!(
                "{engine:?} {kind:?}: the doc says `{cell}`, the engine table {} it",
                if in_code { "reads" } else { "never reads" }
            ));
        }
    }
    Ok(())
}

/// Fails unless the doc's column for `engine` matches the kinds the engine's own table reads.
pub(crate) fn assert_doc_column_matches(engine: Engine, kinds: &[CommonKind]) {
    check_kinds(DOC).unwrap_or_else(|e| panic!("docs/type-mapping.md `Types by kind`: {e}"));
    check_engine(DOC, engine, kinds)
        .unwrap_or_else(|e| panic!("docs/type-mapping.md `Types by kind`: {e}"));
}

/// The variant names of `CommonKind` as written in its source.
fn enum_variants(source: &str) -> Vec<String> {
    let body = source
        .split("pub(crate) enum CommonKind {")
        .nth(1)
        .and_then(|rest| rest.split('}').next())
        .unwrap_or_default();
    body.lines()
        .map(|l| l.trim().trim_end_matches(',').to_string())
        .filter(|l| l.chars().next().is_some_and(|c| c.is_ascii_uppercase()))
        .collect()
}

#[test]
fn the_doc_table_lists_the_common_kinds_in_code_order_with_their_delivered_type() {
    check_kinds(DOC).unwrap();
}

#[test]
fn the_kind_list_this_guard_walks_is_every_variant_of_the_enum() {
    let mut walked: Vec<String> = ALL.iter().map(|k| format!("{k:?}")).collect();
    let mut declared = enum_variants(KIND_SOURCE);
    walked.sort();
    declared.sort();
    assert_eq!(walked, declared);
}

#[test]
fn a_doc_table_with_a_kind_renamed_dropped_or_swapped_is_refused() {
    let renamed = DOC.replace("| uuid | `uuid` |", "| guid | `uuid` |");
    assert!(check_kinds(&renamed).is_err());
    let a = "| time | `time(µs)` |";
    let b = "| uuid | `uuid` |";
    let swapped = DOC.replace(a, "@@").replace(b, a).replace("@@", b);
    assert!(check_kinds(&swapped).is_err());
    let names_swapped = DOC
        .replace("| timestamp |", "@@")
        .replace("| timestamp_tz |", "| timestamp |")
        .replace("@@", "| timestamp_tz |");
    assert!(check_kinds(&names_swapped).is_err());
    let dropped: String = DOC
        .lines()
        .filter(|l| !l.starts_with("| interval |"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(check_kinds(&dropped).is_err());
}

#[test]
fn a_doc_table_naming_another_delivered_type_is_refused() {
    let wrong = DOC.replace("| interval | `interval` |", "| interval | `string` |");
    assert!(check_kinds(&wrong).is_err());
}

#[test]
fn a_doc_column_that_claims_or_denies_a_kind_the_engine_does_not_is_refused() {
    use CommonKind::*;
    let oracle = [
        Bool, I32, I64, F32, F64, Dec, Naive, Utc, Text, Binary, Json, Interval,
    ];
    check_engine(DOC, Engine::Oracle, &oracle).unwrap();
    let mut claims_time = oracle.to_vec();
    claims_time.push(Time);
    assert!(check_engine(DOC, Engine::Oracle, &claims_time).is_err());
    let denies: Vec<CommonKind> = oracle.into_iter().filter(|k| *k != Interval).collect();
    assert!(check_engine(DOC, Engine::Oracle, &denies).is_err());
}
