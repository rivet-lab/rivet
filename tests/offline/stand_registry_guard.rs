//! dev/stand/registry.yaml is the one list of stand names; a hard-coded copy must agree with it or not exist.

#[path = "../common/env.rs"]
mod env;
#[path = "../common/registry.rs"]
mod registry;

use std::path::Path;

/// Every file under `dir` with one of `exts`, recursively, skipping build and scratch trees.
fn files(dir: &Path, exts: &[&str], out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        let name = e.file_name().to_string_lossy().into_owned();
        if p.is_dir() {
            if !matches!(
                name.as_str(),
                "target" | ".live-tmp" | "__pycache__" | "node_modules"
            ) {
                files(&p, exts, out);
            }
        } else if exts.iter().any(|x| name.ends_with(x)) {
            out.push(p);
        }
    }
}

#[test]
fn env_rs_endpoints_are_the_registry_endpoints() {
    let pairs = [
        ("postgres", env::POSTGRES_URL),
        ("postgres_cdc", env::POSTGRES_CDC_URL),
        ("mysql", env::MYSQL_URL),
        ("mysql_cdc", env::MYSQL_CDC_URL),
        ("mssql", env::MSSQL_URL),
        ("mssql_cdc", env::MSSQL_CDC_URL),
        ("mongo", env::MONGO_URL),
        ("mongo_rs", env::MONGO_RS_URL),
        ("mongo_shard", env::MONGO_SHARD_URL),
    ];
    for (source, hard_coded) in pairs {
        assert_eq!(
            registry::stand_url(source),
            hard_coded,
            "tests/common/env.rs disagrees with dev/stand/registry.yaml for `{source}`"
        );
    }
    for (profile, hard_coded) in [
        ("shard", env::MONGO_SHARD_URL),
        ("tz", env::CLICKHOUSE_TZ_HTTP_URL),
    ] {
        assert_eq!(
            registry::gate_profile_url(profile),
            hard_coded,
            "tests/common/env.rs disagrees with dev/stand/registry.yaml for gate profile `{profile}`"
        );
    }
    assert!(
        registry::gate_profile_url("tz").ends_with(&format!(":{}", env::CLICKHOUSE_TZ_PORT)),
        "CLICKHOUSE_TZ_PORT disagrees with dev/stand/registry.yaml for gate profile `tz`"
    );
    assert!(
        registry::stand_url("mongo_shard").contains(&format!(":{}/", env::MONGO_SHARD_PORT)),
        "MONGO_SHARD_PORT disagrees with dev/stand/registry.yaml for `mongo_shard`"
    );
}

/// Every `(profile, service, port)` a live service of tests/common/env.rs asks the operator to start with `docker compose --profile`.
fn declared_profile_services(env_rs: &str) -> Vec<(String, String, u16)> {
    let consts: std::collections::HashMap<String, u16> =
        regex::Regex::new(r"const (\w+): u16 = (\d+);")
            .unwrap()
            .captures_iter(env_rs)
            .map(|c| (c[1].to_string(), c[2].parse().unwrap()))
            .collect();
    regex::Regex::new(
        r#""127\.0\.0\.1",\s*(\w+),\s*"[^"]*docker compose --profile ([\w-]+) up -d (?:--wait )?([\w-]+)""#,
    )
    .unwrap()
    .captures_iter(env_rs)
    .map(|c| {
        let port = c[1].parse().unwrap_or_else(|_| consts[&c[1]]);
        (c[2].to_string(), c[3].to_string(), port)
    })
    .collect()
}

/// The gate profiles whose compose profile and services name `service`, with the port their URL probes.
fn gate_ports_of(profile: &str, service: &str) -> Vec<u16> {
    let profiles = registry::stand()["gate_profiles"].as_mapping().unwrap();
    profiles
        .iter()
        .filter(|(name, spec)| {
            spec["profile"].as_str().unwrap_or(name.as_str().unwrap()) == profile
                && spec["services"]
                    .as_sequence()
                    .unwrap()
                    .iter()
                    .any(|s| s.as_str() == Some(service))
        })
        .map(|(_, spec)| {
            let url = spec["url"].as_str().unwrap();
            let rest = url.rsplit('@').next().unwrap().split("//").last().unwrap();
            let host_port = rest.split(['/', '?']).next().unwrap();
            host_port.rsplit(':').next().unwrap().parse().unwrap()
        })
        .collect()
}

#[test]
fn every_profile_service_a_live_test_needs_is_a_gate_profile_probing_its_port() {
    let declared = declared_profile_services(include_str!("../common/env.rs"));
    assert!(
        declared.len() >= 8,
        "the declared-service scan found only {declared:?}"
    );
    let missing: Vec<String> = declared
        .into_iter()
        .filter(|(profile, service, port)| !gate_ports_of(profile, service).contains(port))
        .map(|(profile, service, port)| format!("--profile {profile} {service} (port {port})"))
        .collect();
    assert!(
        missing.is_empty(),
        "tests/common/env.rs asks for these stands but no gate_profiles entry in dev/stand/registry.yaml starts them and probes their port, so the gate leaves their cells NOT GRADED: {missing:?}"
    );
}

#[test]
fn no_bigquery_dataset_name_is_spelled_outside_the_registry() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut all = Vec::new();
    for dir in ["tests", "dev"] {
        files(&root.join(dir), &[".rs", ".py", ".sh"], &mut all);
    }
    all.push(root.join("Makefile"));
    let spelled = regex::Regex::new(
        r#"["'=]\s*(rivet_(e2e|matrix|blessed[a-z_]*|same_[a-z_]+|partner_[a-z_]+|tmp_[a-z_]*))\b"#,
    )
    .unwrap();
    let mut offenders = Vec::new();
    for f in all
        .into_iter()
        .filter(|f| !f.ends_with("dev/pytools/registry.py"))
    {
        let text = std::fs::read_to_string(&f).unwrap_or_default();
        for (n, line) in text.lines().enumerate() {
            for c in spelled.captures_iter(line) {
                offenders.push(format!(
                    "{}:{}: {}",
                    f.strip_prefix(root).unwrap().display(),
                    n + 1,
                    &c[1]
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "BigQuery dataset names belong in dev/stand/registry.yaml (read them via tests/common/registry.rs or \
         dev/pytools/registry.py):\n{}",
        offenders.join("\n")
    );
}

#[test]
fn no_warehouse_project_or_bucket_is_spelled_outside_the_registry() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut all = Vec::new();
    for dir in ["tests", "dev", ".github"] {
        files(&root.join(dir), &[".rs", ".py", ".sh", ".yml"], &mut all);
    }
    all.push(root.join("Makefile"));
    let names = [
        registry::stand()["bigquery"]["project"].as_str().unwrap(),
        registry::stand()["gcs"]["bucket"].as_str().unwrap(),
    ];
    let offenders: Vec<String> = all
        .iter()
        .filter(|f| !f.ends_with("dev/pytools/registry.py"))
        .flat_map(|f| {
            let text = std::fs::read_to_string(f).unwrap_or_default();
            text.lines()
                .enumerate()
                .filter(|(_, l)| {
                    names.iter().any(|n| {
                        ['"', '\'', '=']
                            .iter()
                            .any(|q| l.contains(&format!("{q}{n}")))
                    })
                })
                .map(|(n, _)| format!("{}:{}", f.strip_prefix(root).unwrap().display(), n + 1))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "the warehouse project and bucket belong in dev/stand/registry.yaml (read them via \
         tests/common/registry.rs `warehouse` or dev/pytools/registry.py `warehouse`):\n{}",
        offenders.join("\n")
    );
}

#[test]
fn the_harness_names_no_endpoint_of_its_own() {
    let text =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/harness/mod.rs"))
            .unwrap();
    assert!(
        !text.contains("127.0.0.1:") && !text.contains("\"rivet_bench\""),
        "tests/harness/mod.rs must read hosts, ports and databases from dev/stand/registry.yaml"
    );
}

#[test]
fn the_python_harness_runs_the_pinned_duckdb_never_a_binary_on_path() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut all = Vec::new();
    files(&root.join("dev"), &[".py"], &mut all);
    let bare = regex::Regex::new(r#"[\[(,]\s*"duckdb"\s*,|which\(\s*"duckdb"\s*\)"#).unwrap();
    let offenders: Vec<String> = all
        .iter()
        .filter(|f| !f.ends_with("dev/pytools/duckcli.py"))
        .flat_map(|f| {
            let text = std::fs::read_to_string(f).unwrap_or_default();
            text.lines()
                .enumerate()
                .filter(|(_, l)| bare.is_match(l))
                .map(|(n, _)| format!("{}:{}", f.strip_prefix(root).unwrap().display(), n + 1))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "run DuckDB through dev/pytools/duckcli.py (`*DUCKDB`, the uv-pinned package), not the `duckdb` on PATH:\n{}",
        offenders.join("\n")
    );
}
