//! Tests for placeholder/env-var resolution and `parse_file_size`.

use super::*;

#[test]
fn resolve_vars_multiple_vars_in_one_string() {
    let mut params = std::collections::HashMap::new();
    params.insert("A".into(), "1".into());
    params.insert("B".into(), "2".into());
    let resolved = resolve_vars("${A}-${B}-end", Some(&params)).unwrap();
    assert_eq!(resolved, "1-2-end");
}

#[test]
fn resolve_vars_missing_closing_brace_stops() {
    let resolved = resolve_vars("before${NO_CLOSE after", None).unwrap();
    assert_eq!(resolved, "before${NO_CLOSE after");
}

#[test]
fn resolve_vars_empty_var_name() {
    let resolved = resolve_vars("x=${}y", None).unwrap();
    assert_eq!(resolved, "x=y");
}

#[test]
fn resolve_vars_no_vars_passthrough() {
    let resolved = resolve_vars("plain text, no dollars", None).unwrap();
    assert_eq!(resolved, "plain text, no dollars");
}

#[test]
fn resolve_vars_adjacent_vars() {
    let mut params = std::collections::HashMap::new();
    params.insert("X".into(), "hello".into());
    params.insert("Y".into(), "world".into());
    let resolved = resolve_vars("${X}${Y}", Some(&params)).unwrap();
    assert_eq!(resolved, "helloworld");
}

#[test]
fn resolve_vars_dollar_without_brace_ignored() {
    let resolved = resolve_vars("price is $5", None).unwrap();
    assert_eq!(resolved, "price is $5");
}

#[test]
fn resolve_vars_value_containing_dollar_does_not_recurse() {
    let mut params = std::collections::HashMap::new();
    params.insert("RAW".into(), "${NOT_A_VAR}".into());
    let resolved = resolve_vars("x=${RAW}", Some(&params)).unwrap();
    assert_eq!(resolved, "x=${NOT_A_VAR}");
}

#[test]
fn parse_file_size_fractional() {
    assert_eq!(
        parse_file_size("1.5GB").unwrap(),
        (1.5 * 1024.0 * 1024.0 * 1024.0) as u64
    );
    assert_eq!(
        parse_file_size("0.5MB").unwrap(),
        (0.5 * 1024.0 * 1024.0) as u64
    );
}

#[test]
fn parse_file_size_whitespace() {
    assert_eq!(parse_file_size("  512 MB  ").unwrap(), 512 * 1024 * 1024);
}

#[test]
fn parse_file_size_lowercase() {
    assert_eq!(parse_file_size("256mb").unwrap(), 256 * 1024 * 1024);
    assert_eq!(parse_file_size("1gb").unwrap(), 1024 * 1024 * 1024);
}

#[test]
fn parse_file_size_invalid_errors() {
    assert!(parse_file_size("abc").is_err());
    assert!(parse_file_size("MB").is_err());
}

#[test]
fn parse_file_size_zero_is_rejected() {
    assert!(parse_file_size("0MB").is_err());
    assert!(parse_file_size("0").is_err());
    assert!(parse_file_size("-5MB").is_err());
}

#[test]
fn resolve_vars_unicode_value_preserved_exactly() {
    let mut params = std::collections::HashMap::new();
    params.insert("GREETING".into(), "Γεια σου 🌍".into());
    let got = resolve_vars("msg=${GREETING}!", Some(&params)).unwrap();
    assert_eq!(got, "msg=Γεια σου 🌍!");
}

#[test]
fn resolve_vars_value_with_quotes_newlines_braces_passes_through() {
    let mut params = std::collections::HashMap::new();
    params.insert(
        "NASTY".into(),
        "a \"quote\" and a\nnewline and a {brace}".into(),
    );
    let got = resolve_vars("v=[${NASTY}]", Some(&params)).unwrap();
    assert_eq!(got, "v=[a \"quote\" and a\nnewline and a {brace}]");
}

/// Substituting a long value costs CPU in proportion to the value, so no step rescans the text.
#[test]
fn resolve_vars_long_value_terminates_quickly() {
    use crate::test_budget::{SCALE, SMALL, linear_cost};
    let params = |bytes: usize| {
        let mut params = std::collections::HashMap::new();
        params.insert("BIG".to_string(), "a".repeat(bytes));
        params
    };
    let (small, large) = linear_cost(
        "resolve_vars",
        params(SMALL),
        params(SMALL * SCALE),
        |params| resolve_vars("x=${BIG}y", Some(params)).unwrap(),
    );
    assert_eq!(small.len(), 2 + SMALL + 1);
    assert_eq!(large.len(), 2 + SMALL * SCALE + 1);
    assert!(large.starts_with("x=") && large.ends_with('y'));
}

#[test]
fn resolve_vars_repeated_placeholder_uses_value_each_time() {
    let mut params = std::collections::HashMap::new();
    params.insert("A".into(), "alpha".into());
    params.insert("B".into(), "beta".into());
    let got = resolve_vars("${A}${A}-${B}-${A}", Some(&params)).unwrap();
    assert_eq!(got, "alphaalpha-beta-alpha");
}

#[test]
fn parse_file_size_rejects_empty_and_garbage() {
    assert!(parse_file_size("").is_err());
    assert!(parse_file_size("not-a-size").is_err());
    assert!(parse_file_size("MB").is_err());
    assert!(parse_file_size("GB 1").is_err());
}

#[test]
fn parse_file_size_boundary_exact_kb() {
    assert_eq!(parse_file_size("1023").unwrap(), 1023);
    assert_eq!(parse_file_size("1024").unwrap(), 1024);
    assert_eq!(parse_file_size("1KB").unwrap(), 1024);
}

#[test]
fn parse_file_size_does_not_panic_on_huge_inputs_and_is_deterministic() {
    let inputs = [
        "1000000000000000000000000GB",
        "1e308GB",
        "999999999999999999999999999B",
        "1.7976931348623157e308MB",
    ];
    for s in inputs {
        let a = parse_file_size(s).is_ok();
        let b = parse_file_size(s).is_ok();
        assert_eq!(a, b, "deterministic for input {s:?}");
    }
}

#[test]
fn parse_file_size_negative_values_are_not_huge_positive() {
    match parse_file_size("-1GB").ok() {
        None => {}
        Some(v) => assert_eq!(v, 0, "negative must coerce to 0 if accepted, got {v}"),
    }
}
