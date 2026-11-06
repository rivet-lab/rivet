//! Every stand image is pinned `repo[:tag]@sha256:<64 hex>` in both compose files, the gate matrix and the `*IMAGE` constants of dev/.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use regex::Regex;

const COMPOSE_FILES: &[&str] = &["docker-compose.yaml", "dev/stand/docker-compose.yaml"];
const MATRIX: &str = "dev/release-oracle/matrix.yaml";

/// Repos pinned by digest alone: `YYYY-MM-DD: why no version tag names the digest; closes when ...`.
const DIGEST_ONLY: &[(&str, &str)] = &[
    (
        "cgr.dev/chainguard/minio",
        "2026-11-06: the registry publishes only `latest` for this image; closes when it publishes version tags",
    ),
    (
        "edoburu/pgbouncer",
        "2026-11-06: no published tag resolves to the build the gate host runs; closes when a version tag carries that digest or the build is replaced",
    ),
    (
        "mcr.microsoft.com/azure-storage/azurite",
        "2026-11-06: the 3.36.0 tag was rebuilt after the gate host pulled; closes when a tag carries that digest or the build is replaced",
    ),
];

#[derive(Debug, PartialEq)]
struct Ref {
    repo: String,
    tag: Option<String>,
    digest: String,
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(rel: &str) -> String {
    let p = root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// The ref a compose image string resolves to with every `${VAR:-default}` taken at its default.
fn expand_defaults(image: &str) -> Result<String, String> {
    let var = Regex::new(r"\$\{[A-Za-z_][A-Za-z0-9_]*:-([^}]*)\}").unwrap();
    let out = var.replace_all(image, "$1").into_owned();
    if out.contains('$') {
        return Err(format!(
            "`{image}` has a variable without a default, so no pin can be read from it"
        ));
    }
    Ok(out)
}

/// Parses `repo[:tag]@sha256:<64 lowercase hex>`; the error says what is missing.
fn parse_ref(image: &str) -> Result<Ref, String> {
    let (name, digest) = image
        .split_once("@sha256:")
        .ok_or_else(|| format!("`{image}` has no `@sha256:` digest"))?;
    if digest.len() != 64 || !digest.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')) {
        return Err(format!(
            "`{image}`: the digest is not 64 lowercase hex characters"
        ));
    }
    let last = name.rsplit('/').next().unwrap_or(name);
    let (repo, tag) = match last.split_once(':') {
        Some((_, t)) if t.is_empty() || t == "latest" => {
            return Err(format!("`{image}`: tag `{t}` names no version"));
        }
        Some((_, t)) => (
            name[..name.len() - t.len() - 1].to_string(),
            Some(t.to_string()),
        ),
        None => (name.to_string(), None),
    };
    if repo.is_empty() {
        return Err(format!("`{image}` names no repository"));
    }
    Ok(Ref {
        repo,
        tag,
        digest: digest.to_string(),
    })
}

fn compose_images(rel: &str) -> Vec<(String, String)> {
    let doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&read(rel)).unwrap_or_else(|e| panic!("{rel} is not YAML: {e}"));
    let services = doc
        .get("services")
        .and_then(|s| s.as_mapping())
        .unwrap_or_else(|| panic!("{rel} has no services"));
    services
        .iter()
        .filter_map(|(name, svc)| {
            let image = svc.get("image")?.as_str()?;
            Some((
                format!("{rel} service {}", name.as_str()?),
                image.to_string(),
            ))
        })
        .collect()
}

fn matrix_images() -> Vec<(String, String)> {
    let doc: serde_yaml_ng::Value = serde_yaml_ng::from_str(&read(MATRIX))
        .unwrap_or_else(|e| panic!("{MATRIX} is not YAML: {e}"));
    let engines = doc
        .get("engines")
        .and_then(|e| e.as_mapping())
        .unwrap_or_else(|| panic!("{MATRIX} has no engines"));
    let mut out = Vec::new();
    for (engine, spec) in engines {
        for v in spec
            .get("versions")
            .and_then(|v| v.as_sequence())
            .into_iter()
            .flatten()
        {
            let image = v
                .get("image")
                .and_then(|i| i.as_str())
                .unwrap_or_else(|| panic!("{MATRIX}: a version of {engine:?} has no image"));
            out.push((
                format!(
                    "{MATRIX} {} {}",
                    engine.as_str().unwrap_or("?"),
                    v.get("tag").and_then(|t| t.as_str()).unwrap_or("?")
                ),
                image.to_string(),
            ));
        }
    }
    out
}

fn py_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display())) {
        let p = entry.unwrap().path();
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if p.is_dir() {
            if !matches!(name, "__pycache__" | ".venv" | "node_modules" | "target") {
                py_files(&p, out);
            }
        } else if name.ends_with(".py") {
            out.push(p);
        }
    }
}

/// Every module-level `<NAME>IMAGE = "<ref>"` in dev/**/*.py.
fn python_images() -> Vec<(String, String)> {
    let re = Regex::new(r#"(?m)^[A-Z][A-Z0-9_]*IMAGE\s*=\s*"([^"]+)""#).unwrap();
    let mut files = Vec::new();
    py_files(&root().join("dev"), &mut files);
    files.sort();
    let mut out = Vec::new();
    for f in files {
        let text = std::fs::read_to_string(&f).unwrap_or_default();
        for c in re.captures_iter(&text) {
            let rel = f.strip_prefix(root()).unwrap().display().to_string();
            out.push((
                format!("{rel} {}", &c[0][..c[0].find('=').unwrap()].trim()),
                c[1].to_string(),
            ));
        }
    }
    out
}

/// Every pin problem across the given `(where, image)` pairs: unpinned refs, tagless refs off the ledger, one tag with two digests, stale ledger rows.
fn problems(images: &[(String, String)], ledger: &[(&str, &str)]) -> Vec<String> {
    let mut bad = Vec::new();
    let mut digests: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    let mut tagless: BTreeSet<String> = BTreeSet::new();
    for (at, image) in images {
        let parsed = expand_defaults(image).and_then(|e| parse_ref(&e));
        match parsed {
            Err(e) => bad.push(format!("{at}: {e}")),
            Ok(r) => match r.tag {
                Some(tag) => {
                    digests.entry((r.repo, tag)).or_default().insert(r.digest);
                }
                None => {
                    if !ledger.iter().any(|(repo, _)| *repo == r.repo) {
                        bad.push(format!(
                            "{at}: `{image}` is pinned by digest alone and `{}` has no row in DIGEST_ONLY",
                            r.repo
                        ));
                    }
                    tagless.insert(r.repo);
                }
            },
        }
    }
    for ((repo, tag), set) in &digests {
        if set.len() > 1 {
            bad.push(format!(
                "{repo}:{tag} is pinned to {} different digests: {set:?}",
                set.len()
            ));
        }
    }
    let dated = Regex::new(r"^\d{4}-\d{2}-\d{2}: .+").unwrap();
    for (repo, why) in ledger {
        if !dated.is_match(why) {
            bad.push(format!(
                "DIGEST_ONLY row `{repo}` has no `YYYY-MM-DD: reason`"
            ));
        }
        if !tagless.contains(*repo) {
            bad.push(format!(
                "DIGEST_ONLY row `{repo}` is stale: no image is pinned by digest alone under it"
            ));
        }
    }
    bad
}

fn all_images() -> Vec<(String, String)> {
    let mut all = Vec::new();
    for f in COMPOSE_FILES {
        let found = compose_images(f);
        assert!(
            !found.is_empty(),
            "{f} yields no images: the read is vacuous"
        );
        all.extend(found);
    }
    let matrix = matrix_images();
    assert!(
        !matrix.is_empty(),
        "{MATRIX} yields no images: the read is vacuous"
    );
    all.extend(matrix);
    let py = python_images();
    assert!(
        !py.is_empty(),
        "dev/**/*.py yields no `*IMAGE` constant: the read is vacuous"
    );
    all.extend(py);
    all
}

#[test]
fn every_stand_image_is_pinned_by_digest() {
    let bad = problems(&all_images(), DIGEST_ONLY);
    assert_eq!(
        bad,
        Vec::<String>::new(),
        "stand images off the digest pin; pin as `tag@sha256:<index digest>` of the image the gate host runs, \
         and list a repo that publishes no version tag in DIGEST_ONLY with a dated reason"
    );
}

#[test]
fn the_pin_reader_refuses_what_it_does_not_understand() {
    let d = "a".repeat(64);
    assert_eq!(
        parse_ref(&format!("mysql:8.0.46@sha256:{d}")).unwrap(),
        Ref {
            repo: "mysql".into(),
            tag: Some("8.0.46".into()),
            digest: d.clone()
        }
    );
    assert_eq!(
        parse_ref(&format!("registry:5000/team/app:1.2@sha256:{d}"))
            .unwrap()
            .repo,
        "registry:5000/team/app"
    );
    assert!(
        parse_ref(&format!("cgr.dev/x/minio@sha256:{d}"))
            .unwrap()
            .tag
            .is_none()
    );
    for bad in [
        "mysql:8.0".to_string(),
        format!("mysql:latest@sha256:{d}"),
        format!("mysql:@sha256:{d}"),
        format!("mysql:8.0@sha256:{}", &d[..63]),
        format!("mysql:8.0@sha256:{}", "A".repeat(64)),
        format!("@sha256:{d}"),
    ] {
        assert!(parse_ref(&bad).is_err(), "{bad} must be refused");
    }
    assert!(
        expand_defaults("mongo:${V}").is_err(),
        "a variable without a default has no pin"
    );
    assert_eq!(
        expand_defaults(&format!("mongo:${{V:-7@sha256:{d}}}")).unwrap(),
        format!("mongo:7@sha256:{d}")
    );
}

#[test]
fn the_ledger_and_the_consistency_check_bite() {
    let d = "b".repeat(64);
    let e = "c".repeat(64);
    let at = |s: &str| s.to_string();
    let two = vec![
        (at("a"), format!("mysql:8.0@sha256:{d}")),
        (at("b"), format!("mysql:8.0@sha256:{e}")),
    ];
    assert!(
        problems(&two, &[])
            .iter()
            .any(|p| p.contains("2 different digests"))
    );
    let bare = vec![(at("a"), format!("x/y@sha256:{d}"))];
    assert!(
        problems(&bare, &[])
            .iter()
            .any(|p| p.contains("no row in DIGEST_ONLY"))
    );
    assert!(problems(&bare, &[("x/y", "2026-11-06: why")]).is_empty());
    assert!(
        problems(&bare, &[("x/y", "why")])
            .iter()
            .any(|p| p.contains("YYYY-MM-DD"))
    );
    let tagged = vec![(at("a"), format!("x/y:1@sha256:{d}"))];
    assert!(
        problems(&tagged, &[("x/y", "2026-11-06: why")])
            .iter()
            .any(|p| p.contains("stale"))
    );
}
