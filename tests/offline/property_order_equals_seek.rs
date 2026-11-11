//! order_equals_seek: every engine of the test layer has key-type rows and every seeking mode is graded, read from the code.

use std::path::PathBuf;

fn read(rel: &str) -> String {
    std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel)).unwrap()
}

fn variants(text: &str, header: &str) -> Vec<String> {
    let body = text
        .split(header)
        .nth(1)
        .expect("enum header")
        .split("\n}")
        .next()
        .unwrap();
    body.lines()
        .map(str::trim)
        .filter(|l| l.ends_with(',') && !l.starts_with("#") && !l.starts_with("//"))
        .map(|l| l.trim_end_matches(',').to_string())
        .collect()
}

#[test]
fn every_engine_has_key_types_and_is_graded() {
    let engines = variants(&read("tests/common/sql_engine.rs"), "pub enum SqlEngine {");
    assert!(
        engines.len() >= 4,
        "SqlEngine variants not read: {engines:?}"
    );
    let live = read("tests/live/property_order_equals_seek.rs");
    let key_types = live
        .split("fn key_types")
        .nth(1)
        .unwrap()
        .split("/// The `INSERT`")
        .next()
        .unwrap();
    for e in &engines {
        assert!(
            key_types.contains(&format!("SqlEngine::{e} =>")),
            "SqlEngine::{e} has no key-type row in key_types: a new engine is a new row"
        );
        assert!(
            live.contains(&format!("grade(SqlEngine::{e})")),
            "SqlEngine::{e} is never graded: its cell would never run"
        );
    }
}

#[test]
fn every_mode_is_graded_and_every_engine_has_a_live_test() {
    let live = read("tests/live/property_order_equals_seek.rs");
    let modes = variants(&live, "pub(crate) enum Mode {");
    assert!(modes.len() >= 4, "Mode variants not read: {modes:?}");
    let list = live
        .split("pub(crate) const MODES")
        .nth(1)
        .unwrap()
        .split("];")
        .next()
        .unwrap();
    for m in &modes {
        assert!(
            list.contains(&format!("Mode::{m},")),
            "Mode::{m} is not in MODES"
        );
    }
    for e in ["postgres", "mysql", "mssql", "oracle"] {
        assert!(
            live.contains(&format!("fn order_equals_seek_{e}()")),
            "no live test for {e}"
        );
    }
}

#[test]
fn the_make_target_and_the_live_module_exist() {
    assert!(read("Makefile").contains("property-order-equals-seek:"));
    assert!(read("tests/live_suite.rs").contains("mod property_order_equals_seek;"));
}
