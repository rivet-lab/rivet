//! The Rust seeder is gone: fixtures are native files under `seeds/` applied by `dev/pytools/seed_fixture.py`.

use std::path::{Path, PathBuf};
use std::process::Command;

const GONE: &[&str] = &[
    "src/bin/seed",
    "--bin seed",
    "dev-seed",
    "RIVET_SEED_I_KNOW",
    "target/release/seed",
    "target/debug/seed",
];
const HISTORY: &[&str] = &[
    "CHANGELOG.md",
    "changelog.d/",
    "tests/offline/no_rust_seeder_guard.rs",
];

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn tracked() -> Vec<String> {
    let out = Command::new("git")
        .args(["ls-files", "-z"])
        .current_dir(root())
        .output()
        .expect("git is a prerequisite of this repo's offline guards");
    assert!(out.status.success(), "git ls-files failed");
    String::from_utf8_lossy(&out.stdout)
        .split('\0')
        .filter(|p| !p.is_empty())
        .map(str::to_owned)
        .collect()
}

/// `path: needle` for every file whose text still names a removed seeder part, history files excepted.
fn offenders(files: &[(String, String)]) -> Vec<String> {
    let mut out = Vec::new();
    for (path, text) in files {
        if HISTORY.iter().any(|h| path.starts_with(h)) {
            continue;
        }
        for needle in GONE {
            if text.contains(needle) {
                out.push(format!("{path}: {needle}"));
            }
        }
        if path == "Cargo.toml" && text.lines().any(|l| l.trim() == "name = \"seed\"") {
            out.push(format!("{path}: [[bin]] seed"));
        }
    }
    out
}

#[test]
fn the_seeder_directory_is_not_there() {
    assert!(
        !Path::new(&root()).join("src/bin/seed").exists(),
        "src/bin/seed is back: fixtures are native files (seeds/fixture/<engine>.sql) applied by dev/pytools/seed_fixture.py"
    );
}

#[test]
fn no_tracked_file_names_the_rust_seeder() {
    let files: Vec<(String, String)> = tracked()
        .into_iter()
        .filter_map(|p| {
            std::fs::read_to_string(root().join(&p))
                .ok()
                .map(|t| (p, t))
        })
        .collect();
    assert!(files.len() > 100, "the walk read {} files", files.len());
    assert_eq!(offenders(&files), Vec::<String>::new());
}

#[test]
fn the_check_names_each_removed_part_and_spares_history() {
    let f = |p: &str, t: &str| (p.to_string(), t.to_string());
    let found = offenders(&[
        f("Makefile", "\tcargo build --bin seed --features dev-seed\n"),
        f("Cargo.toml", "[[bin]]\nname = \"seed\"\n"),
        f("x.yml", "RIVET_SEED_I_KNOW=1 target/release/seed\n"),
        f("CHANGELOG.md", "the `dev-seed` feature\n"),
        f("changelog.d/a.md", "src/bin/seed\n"),
        f("seeds/fixture/postgres.sql", "-- native\n"),
    ]);
    assert_eq!(
        found,
        vec![
            "Makefile: --bin seed",
            "Makefile: dev-seed",
            "Cargo.toml: [[bin]] seed",
            "x.yml: RIVET_SEED_I_KNOW",
            "x.yml: target/release/seed",
        ]
    );
}
