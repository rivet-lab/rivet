//! Shrink-only ceilings for the sites that read a credential out of its `Secret`.

use std::path::Path;

// Ceilings frozen at 2026-11-10 (fix/secret-newtype): each is a connect or request site that sends the credential. A raise names the new site in its PR body.
// ratchet-pin: secret-expose-sites (review 2027-02-01) count src/ \.expose\(\)
const CEILINGS: &[(&str, usize)] = &[
    ("src/bin/rivet-mcp.rs", 2),
    ("src/bin/seed/copy_pg.rs", 4),
    ("src/bin/seed/fast.rs", 2),
    ("src/bin/seed/insert.rs", 4),
    ("src/bin/seed/main.rs", 1),
    ("src/bin/seed/mssql.rs", 2),
    ("src/cli/dispatch.rs", 1),
    ("src/config/gcp_secret.rs", 1),
    ("src/config/source.rs", 9),
    ("src/destination/azure.rs", 2),
    ("src/destination/s3.rs", 3),
    ("src/gcp_auth.rs", 10),
    ("src/init/yaml_scaffold.rs", 1),
    ("src/load/bq_rest.rs", 1),
    ("src/load/clickhouse.rs", 3),
    ("src/load/mod.rs", 1),
    ("src/load/snowflake_rest.rs", 4),
    ("src/notify.rs", 1),
    ("src/pipeline/apply_cmd.rs", 1),
    ("src/pipeline/cdc/mod.rs", 1),
    ("src/pipeline/cdc_job.rs", 5),
    ("src/pipeline/cdc_release.rs", 2),
    ("src/pipeline/incarnation.rs", 1),
    ("src/pipeline/mongo_parallel.rs", 2),
    ("src/plan/build.rs", 2),
    ("src/preflight/cdc_health.rs", 1),
    ("src/preflight/mod.rs", 2),
    ("src/preflight/type_report.rs", 3),
    ("src/source/cdc/mod.rs", 1),
    ("src/source/mssql/mod.rs", 1),
    ("src/source/oracle/mod.rs", 1),
    ("src/state/copy.rs", 5),
    ("src/state/load_lease.rs", 1),
    ("src/state/mod.rs", 5),
];

fn rust_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read src dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The `.expose()` and `Secret::expose` sites of `path` outside its test module.
fn expose_sites(path: &Path) -> usize {
    let text = std::fs::read_to_string(path).expect("read source file");
    let code = [
        "\n#[cfg(test)]\nmod ",
        "\n#[cfg(test)]\npub mod ",
        "\n#[cfg(test)]\npub(crate) mod ",
    ]
    .iter()
    .filter_map(|marker| text.find(marker))
    .min()
    .map_or(text.as_str(), |end| &text[..end]);
    code.matches(".expose()").count() + code.matches("Secret::expose").count()
}

#[test]
fn a_secret_is_read_out_only_at_the_listed_sites() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_files(&root.join("src"), &mut files);
    let mut over = Vec::new();
    for file in files {
        let rel = file
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "src/secret.rs" || rel.contains("/tests/") || rel.ends_with("/tests.rs") {
            continue;
        }
        let ceiling = CEILINGS
            .iter()
            .find(|(p, _)| *p == rel)
            .map_or(0, |(_, n)| *n);
        let found = expose_sites(&file);
        if found > ceiling {
            over.push(format!("{rel}: {found} .expose() sites, ceiling {ceiling}"));
        }
    }
    assert_eq!(
        over,
        Vec::<String>::new(),
        "a credential is read out of its Secret beyond the listed sites"
    );
}

#[test]
fn a_zeroized_text_credential_is_a_secret_everywhere_outside_its_module() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_files(&root.join("src"), &mut files);
    let raw: Vec<String> = files
        .iter()
        .filter(|f| !f.ends_with("src/secret.rs"))
        .filter(|f| {
            let text = std::fs::read_to_string(f).expect("read source file");
            text.lines()
                .any(|l| !l.trim_start().starts_with("//") && l.contains("Zeroizing<String>"))
        })
        .map(|f| f.strip_prefix(root).unwrap().display().to_string())
        .collect();
    assert_eq!(
        raw,
        Vec::<String>::new(),
        "a text credential is held as Zeroizing<String> instead of a Secret"
    );
}
