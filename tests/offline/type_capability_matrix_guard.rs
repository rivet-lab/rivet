//! Drift guard for `docs/type-capability-matrix.yaml` (ADR-0038 CP9): its dimensions are
//! derived from the product enums, never typed in.

use std::collections::BTreeSet;

use rivet::types::target::{ExportTarget, TargetInput, TargetStatus};
use rivet::types::{RivetType, TimeUnit, derive_fidelity, rivet_type_to_arrow};
use serde_yaml_ng::Value;

use super::chunking_matrix_guard::{enum_variants, source_engine_variants};

const LEDGER: &str = "docs/type-capability-matrix.yaml";
const DELIVERY_RS: &str = "src/types/delivery.rs";
/// The `render.canon` values the parity driver implements, read from the driver's own canon module.
#[allow(dead_code)]
#[path = "../common/canon.rs"]
mod canon;
use canon::CANONS;

/// `IsoTimestampNanos` -> `iso_timestamp_nanos`, the label rule `TextForm::label` follows.
fn snake(ident: &str) -> String {
    let mut out = String::new();
    for (i, c) in ident.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// The ledger parsed from the repo root.
fn ledger() -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(LEDGER);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {LEDGER}: {e}"));
    serde_yaml_ng::from_str(&text).unwrap_or_else(|e| panic!("parse {LEDGER}: {e}"))
}

/// The string keys of a YAML mapping at `v[key]`.
fn keys(v: &Value, key: &str) -> BTreeSet<String> {
    v.get(key)
        .and_then(Value::as_mapping)
        .unwrap_or_else(|| panic!("`{key}:` is missing or not a mapping"))
        .keys()
        .map(|k| k.as_str().expect("string key").to_string())
        .collect()
}

#[test]
fn every_text_form_has_exactly_one_ledger_entry_with_a_recovery_per_target() {
    let derived: BTreeSet<String> = enum_variants(DELIVERY_RS, "TextForm")
        .iter()
        .map(|v| snake(v))
        .collect();
    assert!(
        derived.len() >= 14 && derived.contains("decimal_plain") && derived.contains("server_text"),
        "TextForm parse produced {derived:?}"
    );
    let doc = ledger();
    let declared = keys(&doc, "forms");
    let missing: Vec<_> = derived.difference(&declared).collect();
    let unknown: Vec<_> = declared.difference(&derived).collect();
    assert!(
        missing.is_empty() && unknown.is_empty(),
        "{LEDGER} forms: out of sync with TextForm — missing {missing:?}, no such form {unknown:?}"
    );

    let targets: BTreeSet<String> = enum_variants("src/types/target.rs", "ExportTarget")
        .iter()
        .map(|v| v.to_ascii_lowercase())
        .collect();
    let source =
        std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(DELIVERY_RS))
            .expect("read delivery.rs");
    for form in &declared {
        let entry = &doc["forms"][form.as_str()];
        let renderer = entry["renderer"]
            .as_str()
            .unwrap_or_else(|| panic!("forms.{form}.renderer missing"));
        assert!(
            renderer == "engine-produced" || source.contains(&format!("pub fn {renderer}(")),
            "forms.{form}.renderer names `{renderer}`, which is not a pub fn in {DELIVERY_RS}"
        );
        assert_eq!(
            keys(entry, "recover"),
            targets,
            "forms.{form}.recover must name exactly the ExportTarget variants"
        );
    }
}

#[test]
fn every_source_engine_has_one_rows_list() {
    let engines: BTreeSet<String> = source_engine_variants().into_iter().collect();
    let doc = ledger();
    assert_eq!(
        keys(&doc, "engines"),
        engines,
        "{LEDGER} engines: must list exactly the SourceType variants"
    );
    for engine in &engines {
        let mut sections = keys(&doc["engines"], engine);
        sections.remove("setup");
        sections.remove("renders");
        assert_eq!(
            sections,
            BTreeSet::from(["rows".to_string()]),
            "engines.{engine} must have exactly `rows:` (plus an optional setup: and renders:)"
        );
    }
}

/// The rows of one engine, keyed by native type.
fn rows<'a>(doc: &'a Value, engine: &str) -> Vec<(String, &'a Value)> {
    doc["engines"][engine]["rows"]
        .as_sequence()
        .unwrap_or_else(|| panic!("engines.{engine}.rows must be a list of rows"))
        .iter()
        .map(|r| {
            let native = r["native_type"]
                .as_str()
                .unwrap_or_else(|| panic!("engines.{engine}.rows: a row has no native_type"));
            (native.to_string(), r)
        })
        .collect()
}

/// Every error in the engine rows of `doc`: samples, markers and their today keys, delivery spellings, renders.
fn row_violations(doc: &Value) -> Vec<String> {
    use arrow_schema::extension::{ExtensionType, Json, Uuid};
    let forms = keys(doc, "forms");
    let extensions = [<Uuid as ExtensionType>::NAME, <Json as ExtensionType>::NAME];
    let valid_delivery = |d: &str| {
        forms.contains(d)
            || extensions.contains(&d)
            || d.strip_prefix("List(")
                .and_then(|e| e.strip_suffix(')'))
                .is_some_and(|e| extensions.contains(&e))
            || d.parse::<arrow_schema::DataType>()
                .is_ok_and(|t| t.to_string() == d)
    };
    let mut bad = Vec::new();
    for engine in keys(doc, "engines") {
        let renders = &doc["engines"][engine.as_str()]["renders"];
        for (d, render) in renders.as_mapping().into_iter().flatten() {
            let d = d.as_str().unwrap_or("");
            let at = format!("engines.{engine}.renders[{d}]");
            if !valid_delivery(d) {
                bad.push(format!(
                    "{at}: `{d}` is no Arrow type, canonical extension or TextForm label"
                ));
            }
            bad.extend(render_violations(&engine, &at, render));
        }
        let (mut seen, mut arrow_rows) = (BTreeSet::new(), Vec::new());
        let engine_rows = rows(doc, &engine);
        let delivers = engine_rows.iter().any(|(_, r)| !r["delivery"].is_null());
        for (native, r) in engine_rows {
            let at = format!("engines.{engine}.rows[{native}]");
            if !seen.insert(native.clone()) {
                bad.push(format!("{at}: listed twice"));
            }
            let matrix_only =
                r["delivery"].is_null() && r["edges"].as_sequence().is_some_and(|e| !e.is_empty());
            if !matrix_only && r["sample"].as_sequence().is_none_or(|s| s.is_empty()) {
                bad.push(format!("{at}: no sample"));
            }
            let d = r["delivery"].as_str().unwrap_or("");
            if !matrix_only && !valid_delivery(d) {
                bad.push(format!(
                    "{at}: delivery `{d}` is no Arrow type, canonical extension or TextForm \
                     label (a refused type declares its ADR target with known_defect and \
                     refuses; a row with `edges` and no delivery is matrix-only)"
                ));
            }
            bad.extend(edge_violations(&at, r));
            bad.extend(cdc_violations(&at, r));
            bad.extend(csv_violations(&at, r));
            bad.extend(na_violations(&at, r, delivers));
            for key in r.as_mapping().into_iter().flat_map(|m| m.keys()) {
                let key = key.as_str().unwrap_or("");
                if !ROW_KEYS.contains(&key) {
                    bad.push(format!("{at}: `{key}:` is not a row field"));
                }
            }
            let kd = &r["known_defect"];
            if let Some(why) = kd.as_str()
                && !(why.contains("ADR-") && why.contains("step"))
            {
                bad.push(format!(
                    "{at}: known_defect must name the ADR and the step that fixes it"
                ));
            }
            if !kd.is_null() {
                match r["today_delivery"].as_str() {
                    None => bad.push(format!(
                        "{at}: known_defect needs `today_delivery:`, the type rivet ships today"
                    )),
                    Some(t) if !valid_delivery(t) => bad.push(format!(
                        "{at}: today_delivery `{t}` is no Arrow type, canonical extension or TextForm label"
                    )),
                    _ => {}
                }
            }
            for (key, val) in [
                ("today_render", &r["today_render"]),
                ("today_delivery", &r["today_delivery"]),
                ("defect_samples", &r["defect_samples"]),
            ] {
                if !val.is_null() && kd.is_null() {
                    bad.push(format!("{at}: `{key}:` belongs beside `known_defect:`"));
                }
            }
            bad.extend(leg_shape_violations(&at, r));
            let samples: Vec<&str> = r["sample"]
                .as_sequence()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            let leg_samples = LEGS
                .iter()
                .map(|(k, _)| (format!("{k}.defect_samples"), &r[*k]["defect_samples"]));
            for (key, list) in std::iter::once(("defect_samples".to_string(), &r["defect_samples"]))
                .chain(leg_samples)
            {
                for d in list.as_sequence().into_iter().flatten() {
                    let d = d.as_str().unwrap_or("");
                    if d != "NULL" && !samples.contains(&d) {
                        bad.push(format!(
                            "{at}: {key} names `{d}`, neither a sample nor NULL"
                        ));
                    }
                }
            }
            if r["refuses"].is_string() && !r["delivery"].is_null() && kd.is_null() {
                bad.push(format!(
                    "{at}: `refuses` on a row with a `delivery` is a known_defect row's batch \
                     behaviour; refusal is not an ADR target"
                ));
            }
            for key in ["render", "today_render"] {
                bad.extend(render_violations(&engine, &at, &r[key]));
            }
            let duck = if r["today_render"].is_null() {
                r["render"]["duck"].as_str().or(renders[d]["duck"].as_str())
            } else {
                r["today_render"]["duck"].as_str()
            };
            if duck == Some("arrow") {
                arrow_rows.push(native);
            }
        }
        if arrow_rows.len() > usize::from(engine == "postgres") {
            bad.push(format!(
                "engines.{engine}: `duck: arrow` (Arrow's display instead of DuckDB) is for one \
                 postgres row at most, here {arrow_rows:?}"
            ));
        }
    }
    bad
}

/// Every render field in `render` the parity driver does not read for `engine`.
fn render_violations(engine: &str, at: &str, render: &Value) -> Vec<String> {
    let mut bad = Vec::new();
    for (k, v) in render.as_mapping().into_iter().flatten() {
        let (k, v) = (k.as_str().unwrap_or(""), v.as_str().unwrap_or(""));
        let known = match k {
            "source" => engine == "oracle",
            "server" => engine != "oracle",
            "duck" => true,
            "canon" => CANONS.contains(&v),
            _ => false,
        };
        if !known {
            bad.push(format!(
                "{at}: render `{k}: {v}` is not one the parity driver reads"
            ));
        }
    }
    bad
}

/// The type a delivery label names: a TextForm label and every text-valued column is a string, `arrow.json`/`arrow.uuid` are their kinds, anything else an Arrow display.
fn rivet_type_of(forms: &BTreeSet<String>, d: &str) -> RivetType {
    use arrow_schema::{DataType, TimeUnit as Unit};
    let unit = |u: &Unit| match u {
        Unit::Second => TimeUnit::Second,
        Unit::Millisecond => TimeUnit::Millisecond,
        Unit::Microsecond => TimeUnit::Microsecond,
        Unit::Nanosecond => TimeUnit::Nanosecond,
    };
    if forms.contains(d) {
        return RivetType::String;
    }
    match d {
        "arrow.json" => return RivetType::Json,
        "arrow.uuid" => return RivetType::Uuid,
        _ => {}
    }
    if let Some(inner) = d.strip_prefix("List(").and_then(|e| e.strip_suffix(')')) {
        return RivetType::List {
            inner: Box::new(rivet_type_of(forms, inner)),
        };
    }
    let t = match d
        .parse::<DataType>()
        .unwrap_or_else(|e| panic!("delivery `{d}`: {e}"))
    {
        DataType::Boolean => RivetType::Bool,
        DataType::Int16 => RivetType::Int16,
        DataType::Int32 => RivetType::Int32,
        DataType::Int64 => RivetType::Int64,
        DataType::UInt64 => RivetType::UInt64,
        DataType::Float32 => RivetType::Float32,
        DataType::Float64 => RivetType::Float64,
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            RivetType::Decimal { precision, scale }
        }
        DataType::Date32 => RivetType::Date,
        DataType::Time64(u) => RivetType::Time { unit: unit(&u) },
        DataType::Timestamp(u, tz) => RivetType::Timestamp {
            unit: unit(&u),
            timezone: tz.map(|z| z.to_string()),
        },
        DataType::Utf8 => RivetType::String,
        DataType::Binary => RivetType::Binary,
        other => panic!("delivery `{d}` ({other}) has no RivetType"),
    };
    assert_eq!(
        rivet_type_to_arrow(&t).map(|a| a.to_string()).as_deref(),
        Some(d),
        "delivery `{d}` does not round-trip through RivetType"
    );
    t
}

/// The code a warehouse load refuses a column no type exists for.
const LOAD_UNMAPPED: &str = "refuse:RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED";

/// The column type `rivet load` builds in `leg` for a column delivered as `d`, as the ledger spells it: the resolver's own type, `Nullable` around a ClickHouse scalar, ClickHouse's own name for `Decimal64(s)`.
fn expected_leg(leg: &str, forms: &BTreeSet<String>, d: &str) -> String {
    let rt = rivet_type_of(forms, d);
    let target = match leg {
        "bigquery" => ExportTarget::BigQuery,
        "snowflake" => ExportTarget::Snowflake,
        "clickhouse" => ExportTarget::ClickHouse,
        other => panic!("no warehouse leg `{other}`"),
    };
    let spec = target.resolve_column(TargetInput {
        column_name: "c",
        rivet_type: &rt,
        arrow_type: None,
        fidelity: derive_fidelity(&rt),
    });
    if spec.status == TargetStatus::Fail {
        return LOAD_UNMAPPED.to_string();
    }
    let t = spec.target_type.to_string();
    if leg != "clickhouse" {
        return t;
    }
    let t = canonical_decimal64(&t).replace("JSON", "String");
    if t.starts_with("Array(") {
        t
    } else {
        format!("Nullable({t})")
    }
}

/// `Decimal64(s)` spelled the way `system.columns` prints it, `Decimal(18, s)`, also inside an Array.
fn canonical_decimal64(t: &str) -> String {
    match t.split_once("Decimal64(") {
        Some((head, tail)) => {
            let (s, rest) = tail.split_once(')').expect("Decimal64 closes");
            format!("{head}Decimal(18, {s}){}", canonical_decimal64(rest))
        }
        None => t.to_string(),
    }
}

/// Every delivering row whose `clickhouse`, `bigquery` or `snowflake` leg is absent or names a type the resolver does not build for its delivery (and, on a `known_defect` row, for its `today_delivery`).
fn leg_violations(doc: &Value) -> Vec<String> {
    let forms = keys(doc, "forms");
    let mut bad = Vec::new();
    for engine in keys(doc, "engines") {
        for (native, r) in rows(doc, &engine) {
            let Some(d) = r["delivery"].as_str() else {
                continue;
            };
            for (leg, name) in LEGS {
                let at = format!("engines.{engine}.rows[{native}].{leg}");
                let want = expected_leg(leg, &forms, d);
                let Some(got) = r[leg]["type"].as_str() else {
                    bad.push(format!(
                        "{at}: a delivering row names its {name} type, `rivet load` builds `{want}`"
                    ));
                    continue;
                };
                if got != want {
                    bad.push(format!("{at}.type: `{got}`, `rivet load` builds `{want}`"));
                }
                if r["known_defect"].is_null() || !r[leg]["defect"].is_null() {
                    continue;
                }
                let Some(td) = r["today_delivery"].as_str() else {
                    continue;
                };
                let want = expected_leg(leg, &forms, td);
                match r[leg]["today"].as_str() {
                    Some(today) if today == want => {}
                    Some(today) => bad.push(format!(
                        "{at}.today: `{today}`, `rivet load` builds `{want}`"
                    )),
                    None => bad.push(format!("{at}.today: missing, `rivet load` builds `{want}`")),
                }
            }
        }
    }
    bad
}

/// Rows per engine; a ledger may grow past these, never fall below them.
const ROW_FLOOR: [(&str, usize); 5] = [
    // ratchet-pin: type-ledger-rows (review 2027-02-01) sum min
    ("postgres", 101),
    ("mysql", 80),
    ("mssql", 55),
    ("oracle", 57),
    ("mongo", 20),
]; // ratchet-pin: end

#[test]
fn every_ledger_row_has_a_sample_and_a_real_delivery() {
    let doc = ledger();
    for (engine, floor) in ROW_FLOOR {
        let got = rows(&doc, engine).len();
        assert!(
            got >= floor,
            "engines.{engine}.rows lost rows: {got}, the floor is {floor}"
        );
    }
    let mut bad = row_violations(&doc);
    bad.extend(leg_violations(&doc));
    assert!(bad.is_empty(), "{LEDGER}:\n{}", bad.join("\n"));
}

/// The rows the parity driver expects to miss their ADR target, as `engine:native`
/// (` (ClickHouse)` for a clickhouse.defect). Named and marked are the same set: a fix
/// deletes its line here, and a new defect is fixed, not added here.
const KNOWN_DEFECTS: &[&str] = &[
    // ratchet-pin: type-known-defects (review 2027-02-01) strings
    "postgres:INET",
    "postgres:CIDR",
]; // ratchet-pin: end

/// A row that never delivers carries `na:` dated and without `delivery`; an engine with no delivering row needs it on every row.
fn na_violations(at: &str, r: &Value, engine_delivers: bool) -> Vec<String> {
    let mut bad = Vec::new();
    match r["na"].as_str() {
        None if !r["na"].is_null() => bad.push(format!("{at}: `na:` is a string")),
        None if !engine_delivers => bad.push(format!(
            "{at}: the engine delivers no row, so every row needs a dated `na:` reason"
        )),
        None => {}
        Some(why) => {
            let dated = why.split_once(": ").is_some_and(|(d, rest)| {
                !rest.is_empty()
                    && d.len() == 10
                    && d.bytes().enumerate().all(|(i, b)| {
                        if i == 4 || i == 7 {
                            b == b'-'
                        } else {
                            b.is_ascii_digit()
                        }
                    })
            });
            if !dated {
                bad.push(format!(
                    "{at}: `na:` starts with a `YYYY-MM-DD: ` date and gives a reason"
                ));
            }
            if !r["delivery"].is_null() {
                bad.push(format!("{at}: `na:` belongs to a row without `delivery`"));
            }
        }
    }
    bad
}

/// The fields a ledger row may carry.
const ROW_KEYS: [&str; 18] = [
    "native_type",
    "sample",
    "delivery",
    "render",
    "known_defect",
    "today_render",
    "today_delivery",
    "defect_samples",
    "clickhouse",
    "bigquery",
    "snowflake",
    "edges",
    "refuses",
    "cdc",
    "cdc_reason",
    "csv",
    "csv_reason",
    "na",
];

/// The fields of an edge written as a mapping: its literal and a verdict per stage or target.
const EDGE_KEYS: [&str; 6] = ["v", "x", "cdc", "bigquery", "snowflake", "clickhouse"];

/// Every error in a row's `edges:` and `refuses:`: the shape only, the verdict grammar is `dev/release_oracle/type_edges.py`'s.
fn edge_violations(at: &str, r: &Value) -> Vec<String> {
    let mut bad = Vec::new();
    let text = |v: &Value| v.as_str().is_some_and(|s| !s.trim().is_empty());
    if !r["refuses"].is_null() && !text(&r["refuses"]) {
        bad.push(format!("{at}: `refuses:` is a verdict string"));
    }
    let edges = &r["edges"];
    if edges.is_null() {
        return bad;
    }
    let Some(list) = edges.as_sequence() else {
        return vec![format!("{at}: `edges:` is a list")];
    };
    let mut seen = BTreeSet::new();
    for (i, e) in list.iter().enumerate() {
        let lit = match e {
            Value::String(s) => Some(s.as_str()),
            Value::Mapping(m) => {
                for (k, v) in m {
                    let k = k.as_str().unwrap_or("");
                    if !EDGE_KEYS.contains(&k) {
                        bad.push(format!("{at}: edges[{i}] `{k}:` is not an edge field"));
                    } else if !text(v) {
                        bad.push(format!("{at}: edges[{i}] `{k}:` is a non-empty string"));
                    }
                }
                e["v"].as_str()
            }
            _ => None,
        };
        match lit {
            Some(l) if !l.trim().is_empty() => {
                if !seen.insert(l) {
                    bad.push(format!("{at}: edges[{i}] `{l}` is listed twice"));
                }
            }
            _ => bad.push(format!(
                "{at}: edges[{i}] is a literal or a mapping with `v:`"
            )),
        }
    }
    bad
}

/// Every error in a row's `cdc:` verdict: a delivering row has one, `exact` or a registered `refuse:CODE` with a dated `cdc_reason`; the grammar of the rest is `dev/release_oracle/type_edges.py`'s.
fn cdc_violations(at: &str, r: &Value) -> Vec<String> {
    let mut bad = Vec::new();
    let reason = r["cdc_reason"].as_str();
    if r["delivery"].is_null() {
        if !r["cdc"].is_null() || reason.is_some() {
            bad.push(format!(
                "{at}: `cdc:` belongs to a row with `delivery`; a matrix-only row has no CDC parity"
            ));
        }
        return bad;
    }
    let Some(verdict) = r["cdc"].as_str() else {
        return vec![format!(
            "{at}: a row with `delivery` needs `cdc:`, `exact` (as batch) or `refuse:CODE` with a dated `cdc_reason:`"
        )];
    };
    match verdict.strip_prefix("refuse:") {
        None if verdict == "exact" => {
            if reason.is_some() {
                bad.push(format!("{at}: `cdc_reason:` belongs beside `cdc: refuse:CODE`"));
            }
        }
        None => bad.push(format!(
            "{at}: `cdc: {verdict}` is `exact` or `refuse:CODE` (a form belongs to an edge, a known defect to known_defect)"
        )),
        Some(coded) => {
            let code = coded.strip_suffix("@extract").unwrap_or(coded);
            if !rivet::error::codes::ALL.iter().any(|c| c.id == code) {
                bad.push(format!(
                    "{at}: `cdc: {verdict}` names `{code}`, no registered code (rivet::error::codes::ALL)"
                ));
            }
            let dated = reason.is_some_and(|w| {
                let d = w.as_bytes();
                d.len() > 11
                    && d[..10].iter().enumerate().all(|(i, b)| {
                        if i == 4 || i == 7 {
                            *b == b'-'
                        } else {
                            b.is_ascii_digit()
                        }
                    })
                    && d[10] == b':'
            });
            if !dated {
                bad.push(format!(
                    "{at}: `cdc: {verdict}` needs `cdc_reason: \"YYYY-MM-DD: why\"`"
                ));
            }
        }
    }
    bad
}

/// Whether a delivery label is an Arrow type CSV writes as rendered text: a flat list, a wide decimal or a nanosecond timestamp.
fn csv_text_delivery(delivery: &str) -> bool {
    delivery.starts_with("List(")
        || delivery.starts_with("Decimal256(")
        || delivery.starts_with("Timestamp(ns")
}

/// Every error in a row's `csv:` decision: a row whose delivery CSV writes as text carries `csv: text` and a dated `csv_reason`, and no other row does.
fn csv_violations(at: &str, r: &Value) -> Vec<String> {
    let mut bad = Vec::new();
    let text_row = r["delivery"].as_str().is_some_and(csv_text_delivery);
    match (r["csv"].as_str(), text_row) {
        (Some("text"), true) => {
            let dated = r["csv_reason"].as_str().is_some_and(|w| {
                let d = w.as_bytes();
                d.len() > 11
                    && d[..10].iter().enumerate().all(|(i, b)| if i == 4 || i == 7 { *b == b'-' } else { b.is_ascii_digit() })
                    && d[10] == b':'
            });
            if !dated {
                bad.push(format!("{at}: `csv: text` needs `csv_reason: \"YYYY-MM-DD: why\"`"));
            }
        }
        (None, true) => bad.push(format!(
            "{at}: a list, wide decimal or nanosecond timestamp row needs `csv: text` and a dated `csv_reason:`, the decision CSV makes for it"
        )),
        (Some(other), _) if other != "text" => bad.push(format!("{at}: `csv: {other}` is `text`")),
        (Some(_), false) => bad.push(format!("{at}: `csv:` belongs to a list, wide decimal or nanosecond timestamp row")),
        (None, false) if !r["csv_reason"].is_null() => bad.push(format!("{at}: `csv_reason:` belongs beside `csv: text`")),
        _ => {}
    }
    bad
}

/// The warehouse legs of a row: ledger key and the name `KNOWN_DEFECTS` spells.
const LEGS: [(&str, &str); 3] = [
    ("clickhouse", "ClickHouse"),
    ("bigquery", "BigQuery"),
    ("snowflake", "Snowflake"),
];

/// The fields of a row's warehouse leg map.
const LEG_KEYS: [&str; 4] = ["type", "today", "defect", "defect_samples"];

/// Every error in the shape of a row's warehouse legs: fields, `type`, markers and `today`.
fn leg_shape_violations(at: &str, r: &Value) -> Vec<String> {
    let mut bad = Vec::new();
    for (key, name) in LEGS {
        let leg = &r[key];
        for field in leg.as_mapping().into_iter().flat_map(|m| m.keys()) {
            let field = field.as_str().unwrap_or("");
            if !LEG_KEYS.contains(&field) {
                bad.push(format!("{at}: `{key}.{field}:` is not a {name} field"));
            }
        }
        if !leg.is_null() && leg["type"].is_null() {
            bad.push(format!("{at}: `{key}:` names no `type:`"));
        }
        let defect = &leg["defect"];
        if let Some(why) = defect.as_str()
            && !(why.contains("ADR-") && why.contains("step"))
        {
            bad.push(format!(
                "{at}: {key}.defect must name the ADR and the step that fixes it"
            ));
        }
        let marked = !r["known_defect"].is_null() || !defect.is_null();
        match (marked, leg["today"].is_null()) {
            (true, true) if !leg.is_null() => bad.push(format!(
                "{at}: a marked row with a {name} type needs `{key}.today:`"
            )),
            (false, false) => bad.push(format!("{at}: `{key}.today:` belongs beside a marker")),
            _ => {}
        }
        if !leg["defect_samples"].is_null() && defect.is_null() {
            bad.push(format!(
                "{at}: `{key}.defect_samples:` belongs beside `{key}.defect:`"
            ));
        }
    }
    bad
}

/// Every marked row of `doc` as `KNOWN_DEFECTS` spells it.
fn marked(doc: &Value) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for e in keys(doc, "engines") {
        for (n, r) in rows(doc, &e) {
            if !r["known_defect"].is_null() {
                out.insert(format!("{e}:{n}"));
            }
            for (key, name) in LEGS {
                if !r[key]["defect"].is_null() {
                    out.insert(format!("{e}:{n} ({name})"));
                }
            }
        }
    }
    out
}

/// Every marker of `doc` that `KNOWN_DEFECTS` does not name, and every name with no marker left.
fn known_defect_violations(doc: &Value) -> Vec<String> {
    let named: BTreeSet<String> = KNOWN_DEFECTS.iter().map(|s| s.to_string()).collect();
    let marked = marked(doc);
    marked
        .symmetric_difference(&named)
        .map(|m| {
            if marked.contains(m) {
                format!("{m}: a new known defect, blessed into the ledger instead of fixed")
            } else {
                format!("{m}: fixed, its marker is gone — delete its KNOWN_DEFECTS line")
            }
        })
        .collect()
}

#[test]
fn known_defect_rows_only_shrink() {
    let bad = known_defect_violations(&ledger());
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

/// The mapping of `engine`'s row `native`, for a test to edit.
fn row_mut<'a>(doc: &'a mut Value, engine: &str, native: &str) -> &'a mut serde_yaml_ng::Mapping {
    doc["engines"][engine]["rows"]
        .as_sequence_mut()
        .unwrap()
        .iter_mut()
        .find(|r| r["native_type"] == native)
        .and_then(Value::as_mapping_mut)
        .unwrap_or_else(|| panic!("no {engine} row {native}"))
}

#[test]
fn a_known_defect_moved_to_another_row_is_refused() {
    let mut doc = ledger();
    let marker = row_mut(&mut doc, "postgres", "INET")
        .remove("known_defect")
        .unwrap();
    row_mut(&mut doc, "mysql", "DATETIME(6)").insert("known_defect".into(), marker);
    assert_eq!(
        known_defect_violations(&doc),
        [
            "mysql:DATETIME(6): a new known defect, blessed into the ledger instead of fixed",
            "postgres:INET: fixed, its marker is gone — delete its KNOWN_DEFECTS line"
        ],
        "a marker moved to another row kept the count and went unnoticed"
    );
}

#[test]
fn a_fixed_defect_left_in_known_defects_is_refused() {
    let mut doc = ledger();
    row_mut(&mut doc, "postgres", "INET")
        .remove("known_defect")
        .unwrap();
    assert_eq!(
        known_defect_violations(&doc),
        ["postgres:INET: fixed, its marker is gone — delete its KNOWN_DEFECTS line"]
    );
}

#[test]
fn a_known_defect_without_today_delivery_is_refused() {
    let mut doc = ledger();
    row_mut(&mut doc, "postgres", "INET")
        .remove("today_delivery")
        .unwrap();
    row_mut(&mut doc, "postgres", "UUID[]")
        .get_mut("clickhouse")
        .and_then(Value::as_mapping_mut)
        .unwrap()
        .remove("today")
        .unwrap();
    let bad = row_violations(&doc);
    for want in ["needs `today_delivery:`", "needs `clickhouse.today:`"] {
        assert!(bad.iter().any(|b| b.contains(want)), "{want}: {bad:?}");
    }
}

/// The leg map `leg` of `engine`'s row `native`, for a test to edit.
fn leg_mut<'a>(
    doc: &'a mut Value,
    engine: &str,
    native: &str,
    leg: &str,
) -> &'a mut serde_yaml_ng::Mapping {
    row_mut(doc, engine, native)
        .get_mut(leg)
        .and_then(Value::as_mapping_mut)
        .unwrap_or_else(|| panic!("no {engine} row {native} leg {leg}"))
}

#[test]
fn a_warehouse_type_the_resolver_does_not_build_is_refused() {
    for (engine, native, leg) in [
        ("mssql", "BIGINT", "bigquery"),
        ("postgres", "TEXT", "snowflake"),
        ("mysql", "VARCHAR(200)", "clickhouse"),
        ("oracle", "NUMBER(18)", "clickhouse"),
    ] {
        let mut doc = ledger();
        leg_mut(&mut doc, engine, native, leg).insert("type".into(), "BLOB".into());
        let bad = leg_violations(&doc);
        let at = format!("engines.{engine}.rows[{native}].{leg}.type: `BLOB`");
        assert!(bad.iter().any(|b| b.starts_with(&at)), "{at}: {bad:?}");
    }
}

#[test]
fn a_delivering_row_without_a_warehouse_leg_is_refused() {
    for (engine, native, leg, name) in [
        ("mysql", "VARCHAR(200)", "snowflake", "Snowflake"),
        ("oracle", "NUMBER(18)", "clickhouse", "ClickHouse"),
        ("postgres", "NUMERIC", "bigquery", "BigQuery"),
    ] {
        let mut doc = ledger();
        row_mut(&mut doc, engine, native).remove(leg).unwrap();
        let bad = leg_violations(&doc);
        let want = format!(
            "engines.{engine}.rows[{native}].{leg}: a delivering row names its {name} type"
        );
        assert!(bad.iter().any(|b| b.starts_with(&want)), "{want}: {bad:?}");
    }
}

#[test]
fn a_leg_today_the_resolver_does_not_build_is_refused() {
    let mut doc = ledger();
    leg_mut(&mut doc, "postgres", "CIDR", "bigquery").insert("today".into(), "INT64".into());
    leg_mut(&mut doc, "postgres", "INET", "snowflake")
        .remove("today")
        .unwrap();
    let bad = leg_violations(&doc);
    for want in [
        "engines.postgres.rows[CIDR].bigquery.today: `INT64`, `rivet load` builds `STRING`",
        "engines.postgres.rows[INET].snowflake.today: missing, `rivet load` builds `TEXT`",
    ] {
        assert!(bad.iter().any(|b| b == want), "{want}: {bad:?}");
    }
}

#[test]
fn a_leg_today_without_a_marker_or_an_unknown_leg_field_is_refused() {
    let mut doc = ledger();
    let leg = leg_mut(&mut doc, "mssql", "BIGINT", "bigquery");
    leg.insert("today".into(), "INT64".into());
    leg.insert("note".into(), "x".into());
    let bad = row_violations(&doc);
    for want in [
        "`bigquery.today:` belongs beside a marker",
        "`bigquery.note:` is not a BigQuery field",
    ] {
        assert!(bad.iter().any(|b| b.contains(want)), "{want}: {bad:?}");
    }
}

/// A list of a canonical extension is a delivery label; a list of anything else is not.
#[test]
fn a_matrix_only_row_without_edges_is_refused() {
    let mut doc = ledger();
    row_mut(&mut doc, "mongo", "int").remove("edges").unwrap();
    let bad = row_violations(&doc);
    assert!(
        bad.iter()
            .any(|b| b.contains("engines.mongo.rows[int]") && b.contains("no sample")),
        "{bad:?}"
    );
}

#[test]
fn an_edge_listed_twice_or_with_an_unknown_key_is_refused() {
    let mut doc = ledger();
    let row = row_mut(&mut doc, "postgres", "TEXT");
    let edges = row
        .get_mut("edges")
        .and_then(Value::as_sequence_mut)
        .unwrap();
    let first = edges[0].clone();
    edges.push(first);
    let mut typo = serde_yaml_ng::Mapping::new();
    typo.insert("v".into(), "'x'".into());
    typo.insert("bigquerry".into(), "null".into());
    edges.push(Value::Mapping(typo));
    let bad = row_violations(&doc);
    for want in ["is listed twice", "`bigquerry:` is not an edge field"] {
        assert!(bad.iter().any(|b| b.contains(want)), "{want}: {bad:?}");
    }
}

#[test]
fn a_delivering_row_without_a_cdc_verdict_is_refused() {
    let mut doc = ledger();
    row_mut(&mut doc, "postgres", "TEXT").remove("cdc").unwrap();
    let bad = row_violations(&doc);
    assert!(
        bad.iter()
            .any(|b| b.contains("rows[TEXT]") && b.contains("needs `cdc:`")),
        "{bad:?}"
    );
}

#[test]
fn a_cdc_verdict_off_the_grammar_or_without_its_reason_is_refused() {
    let coded = "refuse:RIVET_TYPE_UNSAFE_MAPPING";
    for (verdict, reason, want) in [
        ("form:x", None, "is `exact` or `refuse:CODE`"),
        ("NAMED!exact", None, "is `exact` or `refuse:CODE`"),
        ("exact ", None, "is `exact` or `refuse:CODE`"),
        (
            "refuse:RIVET_NO_SUCH_CODE",
            Some("2026-11-21: why"),
            "no registered code",
        ),
        (
            "refuse:UNCODED",
            Some("2026-11-21: why"),
            "no registered code",
        ),
        (coded, None, "needs `cdc_reason:"),
        (coded, Some("why, undated"), "needs `cdc_reason:"),
        (coded, Some("2026-11-21 why"), "needs `cdc_reason:"),
        (
            "exact",
            Some("2026-11-21: why"),
            "belongs beside `cdc: refuse:CODE`",
        ),
    ] {
        let mut doc = ledger();
        let row = row_mut(&mut doc, "postgres", "TEXT");
        row.insert("cdc".into(), verdict.into());
        if let Some(r) = reason {
            row.insert("cdc_reason".into(), r.into());
        }
        let bad = row_violations(&doc);
        assert!(
            bad.iter()
                .any(|b| b.contains("rows[TEXT]") && b.contains(want)),
            "{verdict} / {reason:?}: {bad:?}"
        );
    }
    let mut doc = ledger();
    let row = row_mut(&mut doc, "postgres", "TEXT");
    row.insert("cdc".into(), coded.into());
    row.insert(
        "cdc_reason".into(),
        "2026-11-21: the stand cannot decode it".into(),
    );
    assert!(
        !row_violations(&doc)
            .iter()
            .any(|b| b.contains("rows[TEXT]")),
        "a registered code with a dated reason is a CDC verdict"
    );
}

#[test]
fn a_row_of_an_engine_that_never_delivers_needs_a_dated_na_reason() {
    let mut doc = ledger();
    row_mut(&mut doc, "mongo", "int").remove("na").unwrap();
    let bad = row_violations(&doc);
    assert!(
        bad.iter()
            .any(|b| b.contains("rows[int]") && b.contains("needs a dated `na:` reason")),
        "{bad:?}"
    );
    row_mut(&mut doc, "mongo", "int").insert("na".into(), "no date".into());
    let bad = row_violations(&doc);
    assert!(
        bad.iter()
            .any(|b| b.contains("rows[int]") && b.contains("starts with a `YYYY-MM-DD: ` date")),
        "{bad:?}"
    );
}

#[test]
fn an_na_reason_on_a_delivering_row_is_refused() {
    let mut doc = ledger();
    row_mut(&mut doc, "postgres", "TEXT").insert("na".into(), "2026-11-22: x".into());
    let bad = row_violations(&doc);
    assert!(
        bad.iter()
            .any(|b| b.contains("rows[TEXT]") && b.contains("belongs to a row without `delivery`")),
        "{bad:?}"
    );
}

#[test]
fn a_matrix_only_row_with_a_cdc_verdict_is_refused() {
    let mut doc = ledger();
    row_mut(&mut doc, "mongo", "int").insert("cdc".into(), "exact".into());
    let bad = row_violations(&doc);
    assert!(
        bad.iter()
            .any(|b| b.contains("rows[int]") && b.contains("matrix-only row has no CDC parity")),
        "{bad:?}"
    );
}

#[test]
fn a_list_delivery_label_names_a_canonical_extension_only() {
    for (label, ok) in [
        ("List(arrow.uuid)", true),
        ("List(arrow.json)", true),
        ("List(arrow.bogus)", false),
        ("List(arrow.uuid", false),
    ] {
        let mut doc = ledger();
        row_mut(&mut doc, "postgres", "UUID[]").insert("delivery".into(), label.into());
        let bad: Vec<String> = row_violations(&doc)
            .into_iter()
            .filter(|b| b.contains("rows[UUID[]]: delivery"))
            .collect();
        assert_eq!(bad.is_empty(), ok, "{label}: {bad:?}");
    }
}

/// The column types of `fn type_table` in the Oracle CDC suite, the one hand list the ledger did not replace.
fn oracle_type_table_types() -> Vec<String> {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/live/live_cdc_oracledb.rs");
    let src = std::fs::read_to_string(path).expect("read live_cdc_oracledb.rs");
    let body = &src[src.find("fn type_table()").expect("fn type_table")..];
    // cdc_table("<prefix>", "<columns>"): the columns are the second string literal.
    let ddl: String = body
        .split('"')
        .nth(3)
        .expect("type_table's column list")
        .split("\\\n")
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(" ");
    let (mut cols, mut depth, mut cur) = (Vec::new(), 0, String::new());
    for ch in ddl.chars() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                cols.push(std::mem::take(&mut cur));
                continue;
            }
            _ => {}
        }
        cur.push(ch);
    }
    cols.push(cur);
    cols.iter()
        .filter(|c| !c.contains("PRIMARY KEY"))
        .map(|c| {
            c.trim()
                .split_once(' ')
                .expect("name type")
                .1
                .trim()
                .to_uppercase()
        })
        .collect()
}

#[test]
fn the_oracle_cdc_type_table_is_covered_by_ledger_rows() {
    let doc = ledger();
    let declared: BTreeSet<String> = rows(&doc, "oracle")
        .into_iter()
        .map(|(n, _)| n.to_uppercase())
        .collect();
    let types = oracle_type_table_types();
    assert!(types.len() >= 15, "type_table parse produced {types:?}");
    let missing: Vec<_> = types.iter().filter(|t| !declared.contains(*t)).collect();
    assert!(
        missing.is_empty(),
        "live_cdc_oracledb.rs::type_table uses types with no oracle ledger row: {missing:?}"
    );
}

#[test]
fn duck_arrow_is_one_postgres_row_at_most() {
    let arrow = || -> Value { serde_yaml_ng::from_str("{duck: arrow}").unwrap() };
    let mut doc = ledger();
    row_mut(&mut doc, "mysql", "DOUBLE").insert("render".into(), arrow());
    row_mut(&mut doc, "postgres", "NUMERIC(18,2)").insert("render".into(), arrow());
    let bad: Vec<String> = row_violations(&doc)
        .into_iter()
        .filter(|b| b.contains("duck: arrow"))
        .collect();
    assert_eq!(bad.len(), 2, "{bad:?}");
    assert!(bad[0].starts_with("engines.mysql:") && bad[1].starts_with("engines.postgres:"));
}

const FIDELITY: &str = "docs/cdc-type-fidelity-matrix.yaml";

/// Every disagreement between the ledger's known_defect rows and the `known_defect:` lists of the CDC fidelity matrix.
fn fidelity_violations(ledger: &Value, fidelity: &Value) -> Vec<String> {
    let mut named = BTreeSet::new();
    let mut bad = Vec::new();
    for sc in fidelity["scenarios"].as_sequence().into_iter().flatten() {
        for (engine, cell) in sc.as_mapping().into_iter().flatten() {
            for n in cell["known_defect"].as_sequence().into_iter().flatten() {
                let key = format!(
                    "{}:{}",
                    engine.as_str().unwrap_or(""),
                    n.as_str().unwrap_or("")
                );
                if !named.insert(key.clone()) {
                    bad.push(format!("{FIDELITY}: {key} is named in two cells"));
                }
            }
        }
    }
    let marked: BTreeSet<String> = keys(ledger, "engines")
        .iter()
        .flat_map(|e| {
            rows(ledger, e)
                .into_iter()
                .filter(|(_, r)| !r["known_defect"].is_null())
                .map(move |(n, _)| format!("{e}:{n}"))
        })
        .collect();
    for k in marked.difference(&named) {
        bad.push(format!(
            "{k}: a ledger known_defect no {FIDELITY} cell names"
        ));
    }
    for k in named.difference(&marked) {
        bad.push(format!(
            "{k}: {FIDELITY} names a known_defect the ledger does not mark"
        ));
    }
    bad
}

/// The CDC fidelity matrix parsed from the repo root.
fn fidelity() -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(FIDELITY);
    serde_yaml_ng::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn the_cdc_fidelity_matrix_names_exactly_the_ledger_known_defects() {
    let bad = fidelity_violations(&ledger(), &fidelity());
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

#[test]
fn a_known_defect_fixed_in_the_ledger_only_is_refused() {
    let mut doc = ledger();
    row_mut(&mut doc, "postgres", "INET")
        .remove("known_defect")
        .unwrap();
    assert_eq!(
        fidelity_violations(&doc, &fidelity()),
        [format!(
            "postgres:INET: {FIDELITY} names a known_defect the ledger does not mark"
        )]
    );
}

#[test]
fn a_csv_text_row_without_its_decision_or_a_stray_one_is_refused() {
    let mut doc = ledger();
    row_mut(&mut doc, "postgres", "TEXT[]")
        .remove("csv")
        .unwrap();
    row_mut(&mut doc, "postgres", "TEXT").insert("csv".into(), "text".into());
    row_mut(&mut doc, "postgres", "INTEGER[]")
        .insert("csv_reason".into(), "2026-11-23 undated".into());
    row_mut(&mut doc, "postgres", "DATE[]").insert("csv".into(), "refuse".into());
    let bad = row_violations(&doc);
    for (row, want) in [
        ("TEXT[]", "needs `csv: text`"),
        ("TEXT", "`csv:` belongs to a list"),
        ("INTEGER[]", "needs `csv_reason:"),
        ("DATE[]", "is `text`"),
    ] {
        assert!(
            bad.iter()
                .any(|b| b.contains(&format!("rows[{row}]")) && b.contains(want)),
            "{row}: {bad:?}"
        );
    }
}

#[test]
fn every_list_wide_decimal_and_nanosecond_row_of_the_ledger_carries_its_csv_decision() {
    let doc = ledger();
    let mut deciding = 0;
    for engine in ["postgres", "mysql", "mssql", "oracle", "mongo"] {
        for (native, r) in rows(&doc, engine) {
            if r["delivery"].as_str().is_some_and(csv_text_delivery) {
                assert_eq!(r["csv"].as_str(), Some("text"), "{engine} {native}");
                deciding += 1;
            }
        }
    }
    assert_eq!(
        deciding, 12,
        "ten PostgreSQL rows, DATETIME2, DATETIMEOFFSET"
    );
}
