//! The cloud legs of the property matrices, graded offline from recorded server answers.

use std::path::PathBuf;
use std::process::{Command, Output};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn oracle(args: &[&str], work: &std::path::Path) -> Output {
    Command::new("uv")
        .args([
            "run",
            "--frozen",
            "--quiet",
            "python",
            "-m",
            "dev.release_oracle.cloud_legs",
        ])
        .args(args)
        .env("RIVET_BIN_OVERRIDE", env!("CARGO_BIN_EXE_rivet"))
        .env("RIVET_ORACLE_WORK", work)
        .current_dir(root())
        .output()
        .expect("spawn `uv run`: install uv, the oracle is pinned by uv.lock")
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn the_python_oracle_passes_its_own_self_test() {
    let work = tempfile::tempdir().unwrap();
    let out = oracle(&["--self-test"], work.path());
    assert!(
        out.status.success() && text(&out).contains("cloud_legs self-test ok"),
        "{}",
        text(&out)
    );
}

#[test]
fn the_offline_tier_grades_the_recorded_answers_and_names_what_it_cannot() {
    let work = tempfile::tempdir().unwrap();
    let out = oracle(
        &[
            "--tier",
            "offline",
            "--work",
            work.path().join("w").to_str().unwrap(),
        ],
        work.path(),
    );
    let seen = text(&out);
    assert!(out.status.success(), "{seen}");
    for line in [
        "no_silent_null_cloud summary tier=offline PASS=2 RED=0 NOT_GRADED=3",
        "no_secret_in_output_cloud summary tier=offline PASS=3 RED=1 NOT_GRADED=6",
        "check_equals_run_cloud summary tier=offline PASS=1 RED=1 NOT_GRADED=2",
        "no_debris_after_stop_cloud summary tier=offline PASS=2 RED=0 NOT_GRADED=3",
    ] {
        assert!(
            seen.lines().any(|l| l == line),
            "missing `{line}` in:\n{seen}"
        );
    }
}
