//! Lint passes that turn serde's unknown-field errors into a one-line "Did you mean" hint.

/// Wrap a serde_yaml_ng parse error with a "Did you mean ...?" hint when the unknown field has a close-enough lexical match in the expected-fields list embedded in the error message.
pub fn enhance_parse_error(err: serde_yaml_ng::Error) -> anyhow::Error {
    let raw = err.to_string();
    if let Some(hint) = did_you_mean_from_message(&raw) {
        return yaml_invalid(format!("{raw}\n  Did you mean `{hint}`?"));
    }
    if let Some(hint) = missing_field_hint(&raw) {
        return yaml_invalid(format!("{raw}\n  Hint: {hint}"));
    }
    yaml_invalid(raw)
}

/// A YAML parse or shape error, coded `RIVET_CONFIG_YAML_INVALID`.
pub(crate) fn yaml_invalid(message: impl Into<String>) -> anyhow::Error {
    crate::error::coded(crate::error::codes::CONFIG_YAML_INVALID, message)
}

/// A remediation line for serde's `missing field \`x\`` errors on the few top-level fields a newcomer is most likely to omit.
fn missing_field_hint(msg: &str) -> Option<&'static str> {
    let i = msg.find("missing field `")? + "missing field `".len();
    let field = msg[i..].split('`').next()?;
    match field {
        "source" => Some(
            "every config needs a `source:` block (type + url/url_env). Run `rivet init` to scaffold one.",
        ),
        "exports" => Some(
            "every config needs an `exports:` list with at least one export. Run `rivet init` to scaffold one.",
        ),
        "name" => Some("each export needs a `name:` (used in output filenames and state keys)."),
        "query" => {
            Some("each export needs a `query:` — or the `table:` shortcut — to select rows.")
        }
        _ => None,
    }
}

/// Parse `"unknown field `xxx`, expected one of `a`, `b`, ..."` and return the best lexical match for `xxx` from the expected list, or `None` if the message doesn't fit the shape or nothing is close enough.
fn did_you_mean_from_message(msg: &str) -> Option<String> {
    let (unknown, expected) = parse_unknown_field_message(msg)?;
    closest_match(&unknown, &expected)
}

/// Extract `(unknown_field, expected_fields)` from a serde unknown- field error string.
fn parse_unknown_field_message(msg: &str) -> Option<(String, Vec<String>)> {
    let start = msg.find("unknown field `")? + "unknown field `".len();
    let rest = &msg[start..];
    let unknown_end = rest.find('`')?;
    let unknown = rest[..unknown_end].to_string();

    let after_unknown = &rest[unknown_end + 1..];
    let expect_idx = after_unknown.find("expected ")?;
    let after_expected = &after_unknown[expect_idx + "expected ".len()..];
    let after_expected = after_expected
        .strip_prefix("one of ")
        .unwrap_or(after_expected);

    let mut expected: Vec<String> = Vec::new();
    let mut cursor = after_expected;
    while let Some(open) = cursor.find('`') {
        let after_open = &cursor[open + 1..];
        let close = after_open.find('`')?;
        expected.push(after_open[..close].to_string());
        cursor = &after_open[close + 1..];
    }
    if expected.is_empty() {
        return None;
    }
    Some((unknown, expected))
}

/// Find the closest match for `needle` in `candidates` by case-insensitive Levenshtein distance, with one shortcut.
fn closest_match(needle: &str, candidates: &[String]) -> Option<String> {
    let needle_lower = needle.to_ascii_lowercase();

    let mut best: Option<(usize, &String)> = None;
    for c in candidates {
        let d = levenshtein(&needle_lower, &c.to_ascii_lowercase());
        match best {
            Some((bd, _)) if d >= bd => {}
            _ => best = Some((d, c)),
        }
    }
    if let Some((dist, hit)) = best {
        let longer = needle.len().max(hit.len());
        let threshold = (longer / 3).max(2);
        if dist <= threshold {
            return Some(hit.clone());
        }
    }

    const SUBSTR_MIN_LEN: usize = 3;
    let mut substring_hit: Option<&String> = None;
    for c in candidates {
        if c.len() < SUBSTR_MIN_LEN {
            continue;
        }
        let cl = c.to_ascii_lowercase();
        if needle_lower.contains(&cl) || cl.contains(&needle_lower) {
            substring_hit = match substring_hit {
                Some(prev) if prev.len() >= c.len() => Some(prev),
                _ => Some(c),
            };
        }
    }
    substring_hit.cloned()
}

/// Classic Wagner–Fischer Levenshtein distance over ASCII / byte- aligned strings.
fn levenshtein(a: &str, b: &str) -> usize {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr: Vec<usize> = vec![0; b.len() + 1];
    for (i, &ai) in a.iter().enumerate() {
        curr[0] = i + 1;
        for (j, &bj) in b.iter().enumerate() {
            let cost = if ai == bj { 0 } else { 1 };
            curr[j + 1] = (curr[j] + 1).min(prev[j + 1] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levenshtein_identical_is_zero() {
        assert_eq!(levenshtein("password", "password"), 0);
    }

    #[test]
    fn levenshtein_single_typo_is_one() {
        assert_eq!(levenshtein("paswword", "password"), 1);
    }

    #[test]
    fn levenshtein_insert_is_one() {
        assert_eq!(levenshtein("acccess_key_env", "access_key_env"), 1);
    }

    #[test]
    fn closest_match_picks_obvious_typo() {
        let candidates = vec![
            "access_key_env".into(),
            "secret_key_env".into(),
            "bucket".into(),
        ];
        assert_eq!(
            closest_match("acccess_key_env", &candidates).as_deref(),
            Some("access_key_env"),
        );
    }

    #[test]
    fn closest_match_returns_none_when_too_far() {
        let candidates = vec!["a".into(), "b".into(), "c".into()];
        assert!(closest_match("totally_unrelated", &candidates).is_none());
    }

    #[test]
    fn closest_match_is_case_insensitive() {
        let candidates = vec!["bucket".into()];
        assert_eq!(
            closest_match("BUCKET", &candidates).as_deref(),
            Some("bucket"),
        );
    }

    #[test]
    fn closest_match_prefers_near_typo_over_longer_substring() {
        let candidates = vec![
            "source".into(),
            "exports".into(),
            "parallel_exports".into(),
            "parallel_export_processes".into(),
        ];
        assert_eq!(
            closest_match("export", &candidates).as_deref(),
            Some("exports"),
        );
    }

    #[test]
    fn closest_match_substring_fallback_still_works() {
        let candidates = vec!["bucket".into(), "prefix".into()];
        assert_eq!(
            closest_match("bucket_name", &candidates).as_deref(),
            Some("bucket"),
        );
    }

    #[test]
    fn parses_serde_unknown_field_shape() {
        let msg = "unknown field `azure_container`, expected one of `bucket`, `prefix`, `path`";
        let (unknown, expected) = parse_unknown_field_message(msg).unwrap();
        assert_eq!(unknown, "azure_container");
        assert_eq!(expected, vec!["bucket", "prefix", "path"]);
    }

    #[test]
    fn parses_with_trailing_location() {
        let msg = "unknown field `foo`, expected one of `a`, `b` at line 12 column 5";
        let (unknown, expected) = parse_unknown_field_message(msg).unwrap();
        assert_eq!(unknown, "foo");
        assert_eq!(expected, vec!["a", "b"]);
    }

    #[test]
    fn returns_none_for_non_matching_message() {
        let msg = "invalid type: integer `42`, expected a string at line 3 column 5";
        assert!(parse_unknown_field_message(msg).is_none());
    }

    #[test]
    fn end_to_end_typo_suggestion() {
        let msg = "unknown field `acccess_key_env`, expected one of `bucket`, `access_key_env`, `secret_key_env` at line 14 column 5";
        assert_eq!(
            did_you_mean_from_message(msg).as_deref(),
            Some("access_key_env"),
        );
    }

    #[test]
    fn no_suggestion_when_nothing_is_close() {
        let msg =
            "unknown field `flux_capacitor`, expected one of `bucket`, `prefix` at line 1 column 1";
        assert!(did_you_mean_from_message(msg).is_none());
    }
}
