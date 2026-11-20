//! Offline characterization of what each source engine does per native type.

use super::override_table::{Engine, override_is_read};
use super::{Delivery, RivetType, TimeUnit, TypeMapping};

const KNOWN_RED: &str = "dev/release_oracle/known_red.py";

/// One table row: the case name, the mapping the engine produced, and the line pinned for it.
pub(crate) struct Case {
    pub name: String,
    pub mapping: TypeMapping,
    pub expect: &'static str,
}

/// Build a case from a name, the engine's mapping and its pinned line.
pub(crate) fn case(name: &str, mapping: TypeMapping, expect: &'static str) -> Case {
    Case {
        name: name.to_string(),
        mapping,
        expect,
    }
}

/// The override targets every source type is probed with, one or two per override-table kind.
fn probes() -> Vec<RivetType> {
    let ts = |unit, tz: Option<&str>| RivetType::Timestamp {
        unit,
        timezone: tz.map(str::to_string),
    };
    vec![
        RivetType::Bool,
        RivetType::Int16,
        RivetType::Int32,
        RivetType::Int64,
        RivetType::UInt64,
        RivetType::Float32,
        RivetType::Float64,
        RivetType::Decimal {
            precision: 18,
            scale: 2,
        },
        RivetType::Decimal {
            precision: 50,
            scale: 2,
        },
        RivetType::Date,
        RivetType::Time {
            unit: TimeUnit::Microsecond,
        },
        ts(TimeUnit::Microsecond, None),
        ts(TimeUnit::Microsecond, Some("UTC")),
        ts(TimeUnit::Nanosecond, None),
        ts(TimeUnit::Nanosecond, Some("UTC")),
        RivetType::String,
        RivetType::Binary,
        RivetType::Json,
        RivetType::Uuid,
        RivetType::Enum,
        RivetType::Interval,
        RivetType::List {
            inner: Box::new(RivetType::Int32),
        },
        RivetType::Unsupported {
            native_type: "x".into(),
            reason: "x".into(),
        },
    ]
}

/// The probes `engine` reads as an override of a column its mapper typed as `m`.
fn reads(engine: Engine, m: &TypeMapping) -> String {
    let read: Vec<String> = probes()
        .iter()
        .filter(|p| override_is_read(engine, &m.source_native_type, &m.rivet_type, p))
        .map(RivetType::label)
        .collect();
    if read.is_empty() {
        "-".to_string()
    } else {
        read.join(",")
    }
}

/// The delivery word of a mapping: `native`, `text:<form>`, or `unsupported` when it has no Arrow type.
fn delivery(m: &TypeMapping) -> String {
    match (&m.arrow_type, m.delivery) {
        (None, _) => "unsupported".to_string(),
        (Some(_), Delivery::Native) => "native".to_string(),
        (Some(_), Delivery::Text(form)) => format!("text:{}", form.label()),
    }
}

/// The pinned line of a mapping: type, Arrow type, delivery, fidelity, and the override pairs read.
pub(crate) fn line(engine: Engine, m: &TypeMapping) -> String {
    format!(
        "{} | {} | {} | {} | reads {}",
        m.rivet_type.label(),
        m.arrow_type
            .as_ref()
            .map_or_else(|| "-".to_string(), |a| a.to_string()),
        delivery(m),
        m.fidelity.label(),
        reads(engine, m),
    )
}

/// The known-red classes a pinned line names, or a panic on a mark shape it does not understand.
fn marks(case: &str, expect: &str) -> (String, Vec<String>) {
    let mut parts = expect.split(" # ");
    let core = parts.next().unwrap_or_default().to_string();
    let mut red = Vec::new();
    for mark in parts {
        match mark.strip_prefix("red:") {
            Some(names) if !names.is_empty() => red.extend(names.split(',').map(str::to_string)),
            _ if mark == "under-change" => {}
            _ => panic!(
                "{case}: unreadable mark `{mark}`; a mark is `red:CLASS[,CLASS]` or `under-change`"
            ),
        }
    }
    (core, red)
}

/// The rows whose line or ledger mark differs from the pinned table; empty when the table holds.
pub(crate) fn wrong_rows(engine: Engine, cases: &[Case]) -> Vec<String> {
    assert!(
        !cases.is_empty(),
        "{engine:?}: an empty table grades nothing"
    );
    let ledger =
        std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(KNOWN_RED))
            .unwrap_or_else(|e| panic!("{KNOWN_RED} is unreadable: {e}"));
    let mut wrong = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for c in cases {
        assert!(
            seen.insert(c.name.clone()),
            "{engine:?}: `{}` is pinned twice",
            c.name
        );
        let (core, red) = marks(&c.name, c.expect);
        let got = line(engine, &c.mapping);
        if got != core {
            wrong.push(format!("{:<28} want {core}\n{:<28} got  {got}", c.name, ""));
        }
        for class in red {
            if !ledger.contains(&format!("DEFECT_{class}")) {
                wrong.push(format!(
                    "{:<28} marks red:{class}, which {KNOWN_RED} does not ledger",
                    c.name
                ));
            }
        }
    }
    wrong
}

/// The failure text for a table with `wrong` differing rows out of `rows`.
pub(crate) fn table_message(engine: Engine, rows: usize, wrong: &[String]) -> String {
    format!(
        "{engine:?}: {} of {rows} rows differ from the pinned table:\n{}",
        wrong.len(),
        wrong.join("\n")
    )
}
