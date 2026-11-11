//! A value a decoder cannot read must be refused, never written as NULL or a default.
//! Every shape below turns "could not read it" into "there was nothing there"; each occurrence
//! under `src/source/` is a line of the ledger, and a new one needs its own line.

use crate::site_ledger::{enclosing_fn, hold, number};

/// The shapes that swap an unreadable value for NULL or nothing.
const SHAPES: &[&str] = &[
    "=> RivetValue::Null",
    "map_or(RivetValue::Null",
    "unwrap_or(RivetValue::Null)",
    "_ => b.append_null()",
    ".ok().flatten()",
    "map_or(V::Null",
    "Err(_) => b.append_null()",
];

fn rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read src/source").flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

#[test]
fn silent_degrade_shapes_are_each_listed() {
    let mut files = Vec::new();
    rust_files(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/source"),
        &mut files,
    );
    files.sort();
    let mut ids = Vec::new();
    for f in &files {
        let text = std::fs::read_to_string(f).unwrap();
        let rel = f
            .strip_prefix(env!("CARGO_MANIFEST_DIR"))
            .unwrap_or(f)
            .display()
            .to_string();
        let mut hits: Vec<(usize, &str)> = SHAPES
            .iter()
            .flat_map(|s| text.match_indices(s).map(move |(at, _)| (at, *s)))
            .collect();
        hits.sort();
        ids.extend(
            hits.into_iter()
                .map(|(at, s)| format!("{rel}::{}: {s}", enclosing_fn(&text, at))),
        );
    }
    hold(
        "silent-degrade-shapes",
        &number(ids),
        "a value the decoder cannot read must be refused (rivet_bail! with SOURCE_VALUE_UNREPRESENTABLE or SOURCE_CDC_CELL_UNSUPPORTED), not written as NULL. A genuine SQL NULL arm is fine: write it as an explicit NULL match instead of a catch-all",
    );
}
