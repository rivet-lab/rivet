//! The count, span and ratio knobs: what each accepts and what each refuses, by code, through the config loader.

use rivet::config::Config;

fn export(source: &str, body: &str) -> String {
    format!(
        "source:\n  type: postgres\n  url: \"postgresql://u:p@localhost:1/db\"\n{source}exports:\n  - name: t\n{body}    format: parquet\n    destination: {{ type: local, path: ./out }}\n"
    )
}

fn chunked(knob: &str) -> String {
    export(
        "",
        &format!(
            "    query: \"SELECT id FROM t\"\n    mode: chunked\n    chunk_column: id\n    {knob}\n"
        ),
    )
}

fn cdc(knob: &str) -> String {
    export(
        "",
        &format!(
            "    mode: cdc\n    table: public.t\n    cdc: {{ checkpoint: ./c.ckpt, {knob} }}\n"
        ),
    )
}

fn quality(value: &str) -> String {
    export(
        "",
        &format!(
            "    query: \"SELECT id, a FROM t\"\n    quality: {{ null_ratio_max: {{ a: {value} }} }}\n"
        ),
    )
}

fn mongo(page: &str) -> String {
    "source:\n  type: mongo\n  url: \"mongodb://u:p@localhost:1/db\"\n  mongo: { page_size: PAGE }\nexports:\n  - name: t\n    table: c\n    format: parquet\n    destination: { type: local, path: ./out }\n"
        .replace("PAGE", page)
}

fn code_of(yaml: &str) -> Option<&'static str> {
    Config::from_yaml(yaml)
        .err()
        .map(|e| rivet::error::error_code(&e).expect("a coded refusal"))
}

#[test]
fn knob_values_inside_their_range_load_as_before() {
    let ok = [
        chunked("chunk_size: 1"),
        chunked("chunk_size: 3"),
        chunked("chunk_size: 100000"),
        chunked("chunk_size: 9223372036854775807"),
        chunked("chunk_count: 1"),
        chunked("chunk_count: 4"),
        chunked("chunk_count: 1000000"),
        cdc("rollover: 1"),
        cdc("rollover: 100000"),
        cdc("rollover: 9223372036854775807"),
        quality("0.0"),
        quality("0.5"),
        quality("1.0"),
        mongo("1"),
        mongo("1000"),
    ];
    for yaml in &ok {
        assert_eq!(code_of(yaml), None, "{yaml}");
    }
}

#[test]
fn a_full_mode_export_never_meets_the_chunk_size_range() {
    let yaml = export(
        "",
        "    query: \"SELECT id FROM t\"\n    mode: full\n    chunk_size: 9223372036854775808\n",
    );
    assert_ne!(code_of(&yaml), Some("RIVET_CONFIG_VALUE_OUT_OF_RANGE"));
}

#[test]
fn a_zero_keeps_the_code_it_had_and_a_new_bound_has_one_code() {
    let cases = [
        (
            chunked("chunk_size: 0"),
            "RIVET_CONFIG_MODE_SETTING_INVALID",
        ),
        (
            chunked("chunk_count: 0"),
            "RIVET_CONFIG_CHUNK_COUNT_INVALID",
        ),
        (cdc("rollover: 0"), "RIVET_CONFIG_CDC_ROLLOVER_INVALID"),
        (mongo("0"), "RIVET_CONFIG_VALUE_OUT_OF_RANGE"),
        (
            chunked("chunk_size: 9223372036854775808"),
            "RIVET_CONFIG_VALUE_OUT_OF_RANGE",
        ),
        (
            chunked("chunk_count: 18446744073709551615"),
            "RIVET_CONFIG_VALUE_OUT_OF_RANGE",
        ),
        (
            cdc("rollover: 9223372036854775808"),
            "RIVET_CONFIG_VALUE_OUT_OF_RANGE",
        ),
        (
            mongo("9223372036854775808"),
            "RIVET_CONFIG_VALUE_OUT_OF_RANGE",
        ),
        (quality("1.5"), "RIVET_CONFIG_VALUE_OUT_OF_RANGE"),
        (quality("-0.1"), "RIVET_CONFIG_VALUE_OUT_OF_RANGE"),
        (quality(".nan"), "RIVET_CONFIG_VALUE_OUT_OF_RANGE"),
        (quality(".inf"), "RIVET_CONFIG_VALUE_OUT_OF_RANGE"),
    ];
    for (yaml, code) in &cases {
        assert_eq!(code_of(yaml), Some(*code), "{yaml}");
    }
}
