/// Replaces `${VAR}` patterns with values from `params` (if provided) or environment variables.
pub fn resolve_vars(
    input: &str,
    params: Option<&std::collections::HashMap<String, String>>,
) -> crate::error::Result<String> {
    substitute(input, |var_name| {
        let value = if let Some(v) = params.and_then(|p| p.get(var_name)) {
            v.clone()
        } else {
            match std::env::var(var_name) {
                Ok(v) => v,
                Err(_) => crate::config_bail!(
                    crate::error::codes::CONFIG_SECRET_UNSET,
                    "'${{{var_name}}}' in the config is unresolved — set it with \
                     `--param {var_name}=<value>` (`-p`, which wins over the environment) or export \
                     the environment variable '{var_name}'. (A missing value would silently become \
                     an empty string, so rivet refuses.)"
                ),
            }
        };

        if value.contains('\0') {
            crate::config_bail!(
                crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
                "value for '${{{var_name}}}' contains a NUL byte; refusing to substitute it \
                 (check the parameter/environment source)"
            );
        }
        Ok(value)
    })
}

/// `input` with every `${name}` replaced by `value_of(name)`, the one scan of the placeholder grammar.
fn substitute(
    input: &str,
    mut value_of: impl FnMut(&str) -> crate::error::Result<String>,
) -> crate::error::Result<String> {
    let mut result = input.to_string();
    let mut search_from = 0;
    while let Some(rel_start) = result[search_from..].find("${") {
        let start = search_from + rel_start;
        let Some(rel_end) = result[start..].find('}') else {
            break;
        };
        let end = start + rel_end;
        let var_name = &result[start + 2..end];
        let value = if var_name.is_empty() {
            String::new()
        } else {
            value_of(var_name)?
        };
        result = format!("{}{}{}", &result[..start], value, &result[end + 1..]);
        search_from = start + value.len();
    }
    Ok(result)
}

/// The marks that stand for the `${` and the `}` of a placeholder in [`masked`] text.
const MASK: (char, char) = ('\u{E000}', '\u{E001}');

/// `input` with every `${name}` held between two private-use marks.
pub(super) fn masked(input: &str) -> String {
    substitute(input, |name| Ok(format!("{}{name}{}", MASK.0, MASK.1))).unwrap_or_default()
}

/// A value read from a [`masked`] document, its placeholders spelled `${name}` again.
pub(super) fn unmasked(value: &str) -> String {
    value.replace(MASK.0, "${").replace(MASK.1, "}")
}

/// The text of `template` around its `${name}` placeholders, in order; `None` when it holds none.
pub(crate) fn literal_parts(template: &str) -> Option<Vec<String>> {
    let cut = substitute(template, |_| Ok(MASK.0.to_string())).ok()?;
    let parts = cut.split(MASK.0).map(str::to_string);
    cut.contains(MASK.0).then(|| parts.collect())
}

/// Convenience wrapper: resolve `${VAR}` from environment only.
pub fn resolve_env_vars(input: &str) -> crate::error::Result<String> {
    resolve_vars(input, None)
}

/// Return the names of `--param key=value` entries whose `${key}` placeholder does not appear in `haystack`.
pub fn find_unused_params(
    haystack: &str,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Vec<String> {
    let Some(p) = params else {
        return Vec::new();
    };
    let mut unused: Vec<String> = p
        .keys()
        .filter(|k| !haystack.contains(&format!("${{{k}}}")))
        .cloned()
        .collect();
    unused.sort();
    unused
}

/// F10 (0.7.5 audit): warn loudly when `--param key=value` was passed but `${key}` never appears anywhere the resolver searched.
pub fn warn_unused_params(
    haystack: &str,
    params: Option<&std::collections::HashMap<String, String>>,
) {
    for key in find_unused_params(haystack, params) {
        log::warn!(
            "--param '{}' was not referenced by any `${{{}}}` placeholder in the config — \
             check the parameter name (case-sensitive) or remove the unused --param",
            key,
            key
        );
    }
}

/// Parse a human-readable file size like "512MB", "1GB", "100KB" into bytes.
pub fn parse_file_size(s: &str) -> crate::error::Result<u64> {
    let s = s.trim().to_uppercase();
    let (num, multiplier) = if let Some(n) = s.strip_suffix("GB") {
        (n.trim(), 1024u64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("MB") {
        (n.trim(), 1024u64 * 1024)
    } else if let Some(n) = s.strip_suffix("KB") {
        (n.trim(), 1024u64)
    } else if let Some(n) = s.strip_suffix('B') {
        (n.trim(), 1u64)
    } else {
        (s.as_str(), 1u64)
    };
    let value: f64 = num.parse().map_err(|_| {
        crate::error::coded(
            crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
            format!(
                "invalid file size: '{}' — expected a number with an optional unit \
                 B/KB/MB/GB (e.g. '512MB', '1.5GB', or a bare byte count like '1048576'); \
                 a fractional value is allowed and units are binary (KB = 1024 bytes)",
                s
            ),
        )
    })?;
    let bytes = (value * multiplier as f64) as u64;
    if bytes == 0 {
        crate::config_bail!(
            crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE,
            "invalid file size: '{}' — must be at least 1 byte; a zero, negative, or \
             sub-byte rotation threshold would split the output after every row",
            s
        );
    }
    Ok(bytes)
}

pub(crate) fn format_bytes(b: u64) -> String {
    if b >= 1_073_741_824 {
        format!("{:.1} GB", b as f64 / 1_073_741_824.0)
    } else if b >= 1_048_576 {
        format!("{:.1} MB", b as f64 / 1_048_576.0)
    } else if b >= 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else {
        format!("{} B", b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn no_placeholders_returned_verbatim() {
        assert_eq!(resolve_vars("SELECT 1", None).unwrap(), "SELECT 1");
    }

    #[test]
    fn empty_string_returned_verbatim() {
        assert_eq!(resolve_vars("", None).unwrap(), "");
    }

    #[test]
    fn param_substitutes_placeholder() {
        let mut p = HashMap::new();
        p.insert("TABLE".into(), "orders".into());
        let result = resolve_vars("SELECT * FROM ${TABLE}", Some(&p)).unwrap();
        assert_eq!(result, "SELECT * FROM orders");
    }

    #[test]
    fn param_takes_precedence_over_env() {
        unsafe { std::env::set_var("RIVET_TEST_OVERRIDE_VAR", "from_env") };
        let mut p = HashMap::new();
        p.insert("RIVET_TEST_OVERRIDE_VAR".into(), "from_param".into());
        let result = resolve_vars("${RIVET_TEST_OVERRIDE_VAR}", Some(&p)).unwrap();
        unsafe { std::env::remove_var("RIVET_TEST_OVERRIDE_VAR") };
        assert_eq!(result, "from_param");
    }

    #[test]
    fn multiple_placeholders_all_substituted() {
        let mut p = HashMap::new();
        p.insert("A".into(), "hello".into());
        p.insert("B".into(), "world".into());
        let result = resolve_vars("${A} ${B}", Some(&p)).unwrap();
        assert_eq!(result, "hello world");
    }

    #[test]
    fn sec_param_value_with_nul_rejected() {
        let mut p = HashMap::new();
        p.insert("x".into(), "1\0injected".into());
        let err = resolve_vars("${x}", Some(&p)).expect_err("a NUL value must be rejected");
        assert!(err.to_string().contains("x"), "must name the param: {err}");
        assert!(
            !err.to_string().contains("injected"),
            "must not echo the value: {err}"
        );
    }

    #[test]
    fn sec_param_value_newline_passes_through_guard() {
        let mut p = HashMap::new();
        p.insert("frag".into(), "a\nb".into());
        let result = resolve_vars("X=${frag}", Some(&p)).unwrap();
        assert_eq!(result, "X=a\nb");
    }

    #[test]
    fn sec_normal_param_value_substitutes_fine_guard() {
        let mut p = HashMap::new();
        p.insert("id_min".into(), "100".into());
        let result = resolve_vars("WHERE id >= ${id_min}", Some(&p)).unwrap();
        assert_eq!(result, "WHERE id >= 100");
    }

    #[test]
    fn sec_normal_param_value_with_spaces_and_quotes_substitutes_fine_guard() {
        let mut p = HashMap::new();
        p.insert("filter".into(), "name = 'o''brien'".into());
        let result = resolve_vars("WHERE ${filter}", Some(&p)).unwrap();
        assert_eq!(result, "WHERE name = 'o''brien'");
    }

    #[test]
    fn env_var_substituted_when_set() {
        unsafe { std::env::set_var("RIVET_TEST_RESOLVE_VAR", "secret123") };
        let result = resolve_vars("pass=${RIVET_TEST_RESOLVE_VAR}", None).unwrap();
        unsafe { std::env::remove_var("RIVET_TEST_RESOLVE_VAR") };
        assert_eq!(result, "pass=secret123");
    }

    #[test]
    fn missing_env_var_returns_error() {
        unsafe { std::env::remove_var("RIVET_DEFINITELY_NOT_SET_VAR_XYZ") };
        let err = resolve_vars("${RIVET_DEFINITELY_NOT_SET_VAR_XYZ}", None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("RIVET_DEFINITELY_NOT_SET_VAR_XYZ"),
            "got: {msg}"
        );
    }

    #[test]
    fn empty_placeholder_expands_to_empty_string() {
        let result = resolve_vars("pre${}post", None).unwrap();
        assert_eq!(result, "prepost");
    }

    /// A masked document keeps every placeholder, names it again when unmasked, and holds no brace a flow collection would end a scalar at.
    #[test]
    fn a_masked_placeholder_holds_no_brace_and_is_spelled_again_when_unmasked() {
        let written = "q: [${A}, 'x ${B} y'] ${UNCLOSED";
        let hidden = masked(written);
        assert_eq!(
            hidden,
            "q: [\u{E000}A\u{E001}, 'x \u{E000}B\u{E001} y'] ${UNCLOSED"
        );
        assert_eq!(unmasked(&hidden), written);
        assert_eq!(masked("pre${}post"), "prepost");
        assert_eq!(unmasked("plain"), "plain");
    }

    #[test]
    fn literal_parts_are_the_text_around_each_placeholder() {
        let parts = |t: &str| literal_parts(t);
        let own = |p: &[&str]| Some(p.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        assert_eq!(
            parts("where a = ${x} and b = '${y}'"),
            own(&["where a = ", " and b = '", "'"])
        );
        assert_eq!(parts("${x}"), own(&["", ""]));
        assert_eq!(parts("where a = 1"), None);
        assert_eq!(parts("pre${}post ${UNCLOSED"), None);
    }

    #[test]
    fn unclosed_placeholder_left_as_is() {
        let result = resolve_vars("${UNCLOSED", None).unwrap();
        assert_eq!(result, "${UNCLOSED");
    }

    #[test]
    fn find_unused_params_returns_empty_when_no_params() {
        assert!(find_unused_params("SELECT 1", None).is_empty());
    }

    #[test]
    fn find_unused_params_used_key_not_flagged() {
        let mut p = HashMap::new();
        p.insert("max_id".into(), "20".into());
        let unused = find_unused_params("SELECT * FROM t WHERE id <= ${max_id}", Some(&p));
        assert!(unused.is_empty(), "got: {unused:?}");
    }

    #[test]
    fn find_unused_params_unused_key_flagged_once() {
        let mut p = HashMap::new();
        p.insert("typo_id".into(), "20".into());
        let unused = find_unused_params("SELECT * FROM t WHERE id <= ${max_id}", Some(&p));
        assert_eq!(unused, vec!["typo_id".to_string()]);
    }

    #[test]
    fn find_unused_params_mixed_used_and_unused() {
        let mut p = HashMap::new();
        p.insert("col".into(), "id".into());
        p.insert("typo".into(), "x".into());
        let unused = find_unused_params("SELECT ${col} FROM t", Some(&p));
        assert_eq!(unused, vec!["typo".to_string()]);
    }

    #[test]
    fn find_unused_params_partial_match_does_not_count() {
        let mut p = HashMap::new();
        p.insert("max".into(), "20".into());
        let unused = find_unused_params("SELECT * FROM t WHERE id <= ${max_id}", Some(&p));
        assert_eq!(unused, vec!["max".to_string()]);
    }

    #[test]
    fn resolve_env_vars_reads_env() {
        unsafe { std::env::set_var("RIVET_TEST_ENV_WRAPPER", "wrapped") };
        let result = resolve_env_vars("v=${RIVET_TEST_ENV_WRAPPER}").unwrap();
        unsafe { std::env::remove_var("RIVET_TEST_ENV_WRAPPER") };
        assert_eq!(result, "v=wrapped");
    }

    #[test]
    fn parse_1gb() {
        assert_eq!(parse_file_size("1GB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_512mb() {
        assert_eq!(parse_file_size("512MB").unwrap(), 512 * 1024 * 1024);
    }

    #[test]
    fn parse_100kb() {
        assert_eq!(parse_file_size("100KB").unwrap(), 100 * 1024);
    }

    #[test]
    fn parse_bytes_suffix() {
        assert_eq!(parse_file_size("2048B").unwrap(), 2048);
    }

    #[test]
    fn parse_no_suffix_treated_as_bytes() {
        assert_eq!(parse_file_size("4096").unwrap(), 4096);
    }

    #[test]
    fn parse_whitespace_trimmed() {
        assert_eq!(parse_file_size("  256MB  ").unwrap(), 256 * 1024 * 1024);
    }

    #[test]
    fn parse_lowercase_accepted() {
        assert_eq!(parse_file_size("1gb").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_invalid_returns_error() {
        assert!(parse_file_size("notanumber").is_err());
    }

    #[test]
    fn parse_invalid_error_names_accepted_units() {
        let err = parse_file_size("banana").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("B/KB/MB/GB"), "got: {msg}");
        assert!(msg.contains("fractional"), "got: {msg}");
        assert!(msg.contains("1024"), "got: {msg}");
    }

    #[test]
    fn parse_zero_and_negative_rejected_not_coerced_to_zero() {
        for bad in ["0", "0B", "0MB", "-5MB", "-1", "-0.5GB"] {
            let err = parse_file_size(bad).unwrap_err();
            assert!(
                err.to_string().contains("at least 1 byte"),
                "'{bad}' must be rejected as non-positive, got: {err}"
            );
        }
        assert_eq!(parse_file_size("1B").unwrap(), 1);
    }

    #[test]
    fn parse_sub_byte_fraction_rejected_not_truncated_to_zero() {
        for bad in ["0.5B", "0.9", "0.0005KB", "0.4"] {
            let err = parse_file_size(bad).unwrap_err();
            assert!(
                err.to_string().contains("at least 1 byte"),
                "'{bad}' truncates to 0 bytes and must be rejected, got: {err}"
            );
        }
        assert_eq!(parse_file_size("1.9B").unwrap(), 1);
        assert_eq!(parse_file_size("1.5KB").unwrap(), 1536);
    }
}

#[cfg(test)]
mod format_bytes_tests {
    use super::format_bytes;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
        assert_eq!(format_bytes(2_684_354_560), "2.5 GB");
    }

    #[test]
    fn format_bytes_boundary_values() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1025), "1.0 KB");
        assert_eq!(format_bytes(1_048_575), "1024.0 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_823), "1024.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
    }
}
