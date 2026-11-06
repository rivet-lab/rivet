//! Stand port guards: every published port binds loopback, and every `LiveService` port is a compose mapping.

use regex::Regex;
use serde::Deserialize;
use serde_yaml_ng::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const LOOPBACK: &str = "127.0.0.1";

/// One compose port mapping: the service, the host IP it binds, the host port and the original spelling.
struct Publish {
    service: String,
    host_ip: Option<String>,
    host_port: Option<u16>,
    spec: String,
}

/// Parse a short-syntax mapping `[ip:]host:container[/proto]`; a `${VAR:-x}` is read as an opaque token.
fn parse_short(service: &str, spec: &str) -> Publish {
    let flat = Regex::new(r"\$\{[^}]*\}").unwrap().replace_all(spec, "X");
    let body = flat.split('/').next().unwrap_or("");
    let parts: Vec<&str> = body.split(':').collect();
    let host_ip = (parts.len() >= 3).then(|| parts[..parts.len() - 2].join(":"));
    let host_port = (parts.len() >= 2)
        .then(|| parts[parts.len() - 2].parse::<u16>().ok())
        .flatten();
    Publish {
        service: service.to_string(),
        host_ip,
        host_port,
        spec: spec.to_string(),
    }
}

/// Read one entry of a service's `ports:` list, short string or long map form.
fn parse_entry(service: &str, entry: &Value) -> Publish {
    match entry {
        Value::String(s) => parse_short(service, s),
        Value::Number(n) => parse_short(service, &n.to_string()),
        Value::Mapping(m) => {
            let get = |k: &str| m.get(Value::String(k.to_string()));
            let text = |v: Option<&Value>| match v {
                Some(Value::String(s)) => Some(s.clone()),
                Some(Value::Number(n)) => Some(n.to_string()),
                _ => None,
            };
            Publish {
                service: service.to_string(),
                host_ip: text(get("host_ip")),
                host_port: text(get("published")).and_then(|p| p.parse().ok()),
                spec: format!("{entry:?}"),
            }
        }
        other => panic!("{service}: unreadable ports entry {other:?}"),
    }
}

/// Every port mapping of every service in a compose file.
fn compose_publishes(path: &Path) -> Vec<Publish> {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {}: {e} (run from the repo root)", path.display()));
    let mut out = Vec::new();
    for document in serde_yaml_ng::Deserializer::from_str(&text) {
        let mut doc = Value::deserialize(document)
            .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
        doc.apply_merge()
            .unwrap_or_else(|e| panic!("merge keys in {}: {e}", path.display()));
        let Some(Value::Mapping(services)) = doc.get("services") else {
            continue;
        };
        for (name, svc) in services {
            let name = name.as_str().unwrap_or("?");
            if let Some(Value::Sequence(ports)) = svc.get("ports") {
                out.extend(ports.iter().map(|e| parse_entry(name, e)));
            }
        }
    }
    out
}

/// Every repo file passing `keep`, skipping dot-directories (except `.github`), `target*`, symlinks and vendored trees.
fn repo_files(dir: &Path, keep: &dyn Fn(&Path) -> bool, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_symlink() {
            continue;
        }
        if kind.is_dir() {
            let skip = (name.starts_with('.') && name != ".github")
                || name.starts_with("target")
                || matches!(name.as_str(), "node_modules" | "__pycache__" | "venv");
            if !skip {
                repo_files(&path, keep, out);
            }
        } else if keep(&path) {
            out.push(path);
        }
    }
}

/// The compose files in the repo, found by name so a new one is graded without a list.
fn compose_files() -> Vec<PathBuf> {
    let mut out = Vec::new();
    repo_files(
        Path::new("."),
        &|p| {
            let n = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            (n.starts_with("docker-compose") || n.starts_with("compose."))
                && (n.ends_with(".yaml") || n.ends_with(".yml"))
        },
        &mut out,
    );
    out.sort();
    out
}

/// Spellings of a `docker run` publish that do not name the loopback host IP.
fn unbound_runtime_publishes(text: &str) -> Vec<String> {
    let argv = Regex::new(r#""(?:-p|--publish)"\s*,\s*f?"([0-9{\[$][^"]*)""#).unwrap();
    let cli =
        Regex::new(r"docker (?:run|create)[^\n]*?\s(?:-p|--publish)[ =]([^\s\x22']+)").unwrap();
    let all = Regex::new(r#""--publish-all"|"-P"\s*,\s*"--"#).unwrap();
    let mut out: Vec<String> = argv
        .captures_iter(text)
        .chain(cli.captures_iter(text))
        .map(|c| c[1].to_string())
        .filter(|spec| !spec.starts_with(&format!("{LOOPBACK}:")))
        .collect();
    out.extend(all.find_iter(text).map(|m| m.as_str().to_string()));
    out
}

#[test]
fn every_compose_port_binds_loopback() {
    let files = compose_files();
    let mut offenders = Vec::new();
    for file in &files {
        for p in compose_publishes(file) {
            if p.host_ip.as_deref() != Some(LOOPBACK) {
                offenders.push(format!("{}: {} `{}`", file.display(), p.service, p.spec));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "compose ports published without the {LOOPBACK} host IP (reachable from the LAN): {offenders:#?}"
    );
}

#[test]
fn the_compose_discovery_is_not_truncated() {
    let files = compose_files();
    let count = |path: &str| -> usize {
        files
            .iter()
            .filter(|f| **f == Path::new(path))
            .map(|f| compose_publishes(f).len())
            .sum()
    };
    assert!(count("./docker-compose.yaml") >= 56, "{files:?}");
    assert!(count("./dev/stand/docker-compose.yaml") >= 18, "{files:?}");
    assert!(
        count("./docs/recipes/airflow/docker-compose.2.10.yaml") >= 2,
        "{files:?}"
    );
}

#[test]
fn the_compose_port_parser_reads_every_spelling() {
    let cases = [
        ("5432:5432", None, Some(5432)),
        ("127.0.0.1:5432:5432", Some("127.0.0.1"), Some(5432)),
        ("0.0.0.0:80:80/tcp", Some("0.0.0.0"), Some(80)),
        ("[::1]:80:80", Some("[::1]"), Some(80)),
        ("${HOST_PORT:-27018}:27017", None, None),
        (
            "127.0.0.1:${HOST_PORT:-27018}:27017",
            Some("127.0.0.1"),
            None,
        ),
        ("8080", None, None),
    ];
    for (spec, ip, port) in cases {
        let p = parse_short("svc", spec);
        assert_eq!((p.host_ip.as_deref(), p.host_port), (ip, port), "{spec}");
    }
    let long: Value =
        serde_yaml_ng::from_str("{target: 80, published: 8080, host_ip: 127.0.0.1}").unwrap();
    let p = parse_entry("svc", &long);
    assert_eq!(
        (p.host_ip.as_deref(), p.host_port),
        (Some(LOOPBACK), Some(8080))
    );
}

#[test]
fn every_runtime_publish_binds_loopback() {
    let mut files = Vec::new();
    repo_files(
        Path::new("."),
        &|p| {
            let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            matches!(ext, "py" | "rs" | "sh" | "yml") || name == "Makefile"
        },
        &mut files,
    );
    let me = Path::new("tests/offline/live_service_ports_guard.rs");
    let mut offenders = Vec::new();
    for file in files.iter().filter(|f| !f.ends_with(me)) {
        let Ok(text) = std::fs::read_to_string(file) else {
            continue;
        };
        for spec in unbound_runtime_publishes(&text) {
            offenders.push(format!("{}: {spec}", file.display()));
        }
    }
    assert!(
        offenders.is_empty(),
        "`docker run` publishes without the {LOOPBACK} host IP (use shell.loopback_publish): {offenders:#?}"
    );
}

#[test]
fn the_runtime_publish_scan_reads_every_spelling() {
    let bad = [
        r#"["-p", f"{port}:5432"]"#,
        r#"[ "-p",
            "5432:5432" ]"#,
        r#"sh("docker", "run", "--publish", "3306:3306")"#,
        "docker run -d -p 5432:5432 postgres",
        r#"["--publish-all"]"#,
    ];
    for text in bad {
        assert_eq!(unbound_runtime_publishes(text).len(), 1, "{text}");
    }
    let good = [
        r#"["-p", f"127.0.0.1:{host_port}:{container_port}"]"#,
        r#""-p",
            "127.0.0.1::27017","#,
        "docker run -d -p 127.0.0.1:5432:5432 postgres",
    ];
    for text in good {
        assert!(unbound_runtime_publishes(text).is_empty(), "{text}");
    }
}

/// Host ports mapped by every compose file.
fn compose_host_ports() -> BTreeSet<u16> {
    compose_files()
        .iter()
        .flat_map(|f| compose_publishes(f))
        .filter_map(|p| p.host_port)
        .collect()
}

/// Every `(name, port)` the `LiveService` enum claims, parsed from the source of `tests/common/env.rs`.
fn live_service_ports() -> Vec<(String, u16)> {
    let text = std::fs::read_to_string("tests/common/env.rs").expect("read tests/common/env.rs");
    let addr_fn = text
        .split("fn addr(self)")
        .nth(1)
        .expect("env.rs must contain LiveService::addr");
    let mut out = Vec::new();
    for arm in addr_fn.split("LiveService::").skip(1) {
        let Some(name) = arm.split(|c: char| !c.is_alphanumeric()).next() else {
            continue;
        };
        if let Some(rest) = arm.split("\"127.0.0.1\",").nth(1)
            && let Some(port) = rest
                .split(|c: char| !c.is_ascii_digit())
                .find(|s| !s.is_empty())
                .and_then(|s| s.parse::<u16>().ok())
        {
            out.push((name.to_string(), port));
        }
        if arm.contains("\n    }\n") {
            break;
        }
    }
    out
}

#[test]
fn every_live_service_port_is_mapped_by_a_compose_file() {
    let mapped = compose_host_ports();
    let services = live_service_ports();
    let orphans: Vec<&(String, u16)> = services
        .iter()
        .filter(|(_, p)| *p != 0 && !mapped.contains(p))
        .collect();
    assert!(
        orphans.is_empty(),
        "LiveService arms claim ports no compose file maps: {orphans:?}. Either the \
         service moved in docker-compose.yaml and env.rs was not updated (every \
         require_alive for it now probes a dead port and reports the service down), or \
         the arm was added ahead of the compose service (its tests can never run). \
         Hand-drifted twins here have produced false 'verified' claims before."
    );
}

#[test]
fn the_port_parse_is_not_truncated() {
    let services = live_service_ports();
    assert!(
        services.len() >= 15,
        "parsed only {} LiveService port arms from env.rs — the guard above is grading a \
         truncated list: {services:?}",
        services.len()
    );
    for known in ["Postgres", "Mysql", "MssqlGovernor", "Minio"] {
        assert!(
            services.iter().any(|(n, _)| n == known),
            "parser lost `{known}`; parsed: {services:?}"
        );
    }
}
