//! The "Exercised by" column of docs/reference/compatibility.md claims no more than the compose files and the nightly workflow start.

use std::collections::BTreeMap;
use std::path::PathBuf;

use regex::Regex;

const DOC: &str = "docs/reference/compatibility.md";
const COMPOSE: &str = "docker-compose.yaml";
const STAND: &str = "dev/stand/docker-compose.yaml";
const NIGHTLY: &str = ".github/workflows/nightly-live.yml";

#[derive(Debug, Clone, Copy, PartialEq)]
enum Claim {
    ReleaseGate,
    Nightly,
    Manual,
    NotExercised,
}

#[derive(Debug)]
struct Row {
    engine: String,
    version: String,
    claim: Claim,
}

#[derive(Debug)]
struct Service {
    image: String,
    legacy: bool,
}

fn read(rel: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// The claim a status cell opens with; an unknown opening is an error, never a default.
fn claim_of(status: &str) -> Result<Claim, String> {
    let s = status.replace("**", "");
    let s = s.trim();
    for (word, claim) in [
        ("Release gate", Claim::ReleaseGate),
        ("Nightly", Claim::Nightly),
        ("Manual", Claim::Manual),
        ("Not exercised", Claim::NotExercised),
    ] {
        if s.starts_with(word) {
            return Ok(claim);
        }
    }
    Err(format!(
        "status `{status}` opens with none of: Release gate, Nightly, Manual, Not exercised"
    ))
}

/// The rows of the "Supported versions" table for the engines compose can start.
fn rows(doc: &str) -> Result<Vec<Row>, String> {
    let start = doc
        .find("## Supported versions")
        .ok_or("no `## Supported versions` section")?;
    let mut out = Vec::new();
    for line in doc[start..].lines().skip(1) {
        if line.starts_with("## ") || line.starts_with("\"Primary target\"") {
            break;
        }
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.len() < 5
            || cells[1] == "Engine"
            || cells[1].starts_with('-')
            || cells[1].is_empty()
        {
            continue;
        }
        if !["PostgreSQL", "MySQL", "SQL Server", "MongoDB"].contains(&cells[1]) {
            continue;
        }
        out.push(Row {
            engine: cells[1].to_string(),
            version: cells[2].to_string(),
            claim: claim_of(cells[3])?,
        });
    }
    if out.is_empty() {
        return Err("the table holds no PostgreSQL / MySQL / SQL Server / MongoDB row".into());
    }
    Ok(out)
}

/// Every service of a compose file with the image it resolves to and whether it sits under the `legacy` profile.
fn services(compose: &str) -> Vec<Service> {
    let anchor = Regex::new(r"&(img-[\w-]+)\s+(\S+)").unwrap();
    let var = Regex::new(r"\$\{[A-Za-z_][A-Za-z0-9_]*:-([^}]*)\}").unwrap();
    let anchors: BTreeMap<String, String> = anchor
        .captures_iter(compose)
        .map(|c| (c[1].to_string(), var.replace_all(&c[2], "$1").into_owned()))
        .collect();
    let head = Regex::new(r"^  [A-Za-z0-9_-]+:\s*$").unwrap();
    let image = Regex::new(r"^    image:\s*(\S+)").unwrap();
    let in_services = compose.find("\nservices:").expect("a `services:` section");
    let mut out: Vec<Service> = Vec::new();
    let mut open = false;
    for line in compose[in_services..].lines().skip(1) {
        if line.starts_with("volumes:") || line.starts_with("networks:") {
            break;
        }
        if head.is_match(line) {
            out.push(Service {
                image: String::new(),
                legacy: false,
            });
            open = true;
            continue;
        }
        if !open {
            continue;
        }
        let svc = out.last_mut().unwrap();
        if let Some(c) = image.captures(line) {
            let raw = c[1].to_string();
            svc.image = match raw.strip_prefix('*') {
                Some(name) => anchors.get(name).cloned().unwrap_or_default(),
                None => var.replace_all(&raw, "$1").into_owned(),
            };
        }
        if line.trim_start().starts_with("profiles:") && line.contains("legacy") {
            svc.legacy = true;
        }
    }
    out.into_iter().filter(|s| !s.image.is_empty()).collect()
}

/// Whether an image names the engine at the version (`postgres:16.14@...` is PostgreSQL 16).
fn runs(image: &str, engine: &str, version: &str) -> bool {
    let rest = match engine {
        "PostgreSQL" => image.strip_prefix("postgres:"),
        "MySQL" => image.strip_prefix("mysql:"),
        "SQL Server" => image.strip_prefix("mcr.microsoft.com/mssql/server:"),
        "MongoDB" => image.strip_prefix("mongo:"),
        other => panic!("no image repo for `{other}`"),
    };
    let Some(tag) = rest else { return false };
    tag == version
        || [".", "-", "@"]
            .iter()
            .any(|sep| tag.starts_with(&format!("{version}{sep}")))
}

/// Every claim the table makes that the compose files and the nightly workflow do not back; empty when it holds.
fn lies(doc: &str, compose: &str, stand: &str, nightly: &str) -> Vec<String> {
    let rows = match rows(doc) {
        Ok(r) => r,
        Err(e) => return vec![e],
    };
    let main = services(compose);
    let manual = services(stand);
    let matrix = Regex::new(r"mongo_version:\s*\[([^\]]*)\]").unwrap();
    let nightly_mongo: Vec<String> = matrix
        .captures(nightly)
        .map(|c| {
            c[1].split(',')
                .map(|v| v.trim().trim_matches('"').to_string())
                .collect()
        })
        .unwrap_or_default();
    let mut out = Vec::new();
    for r in &rows {
        let gate = main
            .iter()
            .any(|s| !s.legacy && runs(&s.image, &r.engine, &r.version));
        let any = main.iter().any(|s| runs(&s.image, &r.engine, &r.version))
            || manual.iter().any(|s| runs(&s.image, &r.engine, &r.version));
        let nightly_ok = r.engine == "MongoDB" && nightly_mongo.contains(&r.version);
        let name = format!("{} {}", r.engine, r.version);
        match r.claim {
            Claim::ReleaseGate if !gate => out.push(format!(
                "{name}: claims the release gate, but no compose service outside the `legacy` profile runs it"
            )),
            Claim::Nightly if !nightly_ok => out.push(format!(
                "{name}: claims the nightly matrix, but {NIGHTLY} `mongo_version` does not list it"
            )),
            Claim::Manual if !any => out.push(format!(
                "{name}: claims a manual matrix, but neither {COMPOSE} nor {STAND} pins an image for it"
            )),
            Claim::NotExercised if any || nightly_ok => out.push(format!(
                "{name}: says Not exercised, but a compose file or the nightly matrix starts it; name what runs it"
            )),
            _ => {}
        }
    }
    out
}

#[test]
fn every_version_claim_is_backed_by_what_the_stand_starts() {
    let l = lies(&read(DOC), &read(COMPOSE), &read(STAND), &read(NIGHTLY));
    assert_eq!(
        l,
        Vec::<String>::new(),
        "{DOC} claims more than the stand starts"
    );
}

#[test]
fn a_claim_the_stand_does_not_back_is_named() {
    let (doc, compose, stand, nightly) = (read(DOC), read(COMPOSE), read(STAND), read(NIGHTLY));
    let gated = doc.replace(
        "| PostgreSQL |       18 | Manual (`dev/stand`) |",
        "| PostgreSQL |       18 | Release gate |",
    );
    assert_ne!(gated, doc, "the PostgreSQL 18 row moved");
    let l = lies(&gated, &compose, &stand, &nightly);
    assert!(
        l.iter()
            .any(|m| m.starts_with("PostgreSQL 18: claims the release gate")),
        "{l:?}"
    );
    let hidden = doc.replace(
        "| PostgreSQL |       12 | Manual (`legacy` profile) |",
        "| PostgreSQL |       12 | Not exercised |",
    );
    assert_ne!(hidden, doc, "the PostgreSQL 12 row moved");
    let l = lies(&hidden, &compose, &stand, &nightly);
    assert!(
        l.iter()
            .any(|m| m.starts_with("PostgreSQL 12: says Not exercised")),
        "{l:?}"
    );
    let nightly_less = doc.replace(
        "| MongoDB    |      6.0 | Nightly (CI matrix) |",
        "| MongoDB    |      6.1 | Nightly (CI matrix) |",
    );
    let l = lies(&nightly_less, &compose, &stand, &nightly);
    assert!(
        l.iter()
            .any(|m| m.starts_with("MongoDB 6.1: claims the nightly")),
        "{l:?}"
    );
    let unknown = doc.replace("| Manual (`dev/stand`) |", "| Supported |");
    assert_eq!(
        lies(&unknown, &compose, &stand, &nightly).len(),
        1,
        "a status that opens with no known claim is an error, not a pass"
    );
}
