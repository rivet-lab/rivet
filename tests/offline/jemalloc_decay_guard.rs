//! The shipped binary hands freed pages back to the OS after 250 ms.

#![cfg(feature = "jemalloc")]

use std::process::Command;

const RIVET_BIN: &str = env!("CARGO_BIN_EXE_rivet");

/// The `opt` section jemalloc prints at exit for a run of `rivet --version`.
fn jemalloc_opts() -> String {
    let out = Command::new(RIVET_BIN)
        .arg("--version")
        .env("_RJEM_MALLOC_CONF", "stats_print:true,stats_print_opts:J")
        .env_remove("MALLOC_CONF")
        .output()
        .expect("failed to spawn rivet binary");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// The release binary purges dirty pages after 250 ms.
#[test]
fn the_binary_purges_freed_pages_after_250_ms() {
    let report = jemalloc_opts();
    assert!(
        report.contains("\"dirty_decay_ms\":250"),
        "jemalloc's dirty page decay is not 250 ms in the shipped binary: {}",
        report.chars().take(600).collect::<String>()
    );
}

/// The operator's own `_RJEM_MALLOC_CONF` still wins over the compiled-in default.
#[test]
fn an_operator_setting_overrides_the_compiled_decay() {
    let out = Command::new(RIVET_BIN)
        .arg("--version")
        .env(
            "_RJEM_MALLOC_CONF",
            "dirty_decay_ms:7000,stats_print:true,stats_print_opts:J",
        )
        .output()
        .expect("failed to spawn rivet binary");
    let report = String::from_utf8_lossy(&out.stderr);
    assert!(
        report.contains("\"dirty_decay_ms\":7000"),
        "the operator's decay was not honoured: {}",
        report.chars().take(600).collect::<String>()
    );
}
