//! The no_secret_in_output matrix's oracle proves itself against a planted credential.

use std::path::PathBuf;

/// Run the matrix module from the repo root.
fn matrix(args: &[&str]) -> std::process::Output {
    std::process::Command::new("python3")
        .args(["-m", "dev.release_oracle.property_no_secret_in_output"])
        .args(args)
        .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")))
        .output()
        .expect("python3 is a prerequisite of every gate script in this repo")
}

#[test]
fn the_matrix_oracle_catches_a_planted_credential() {
    let out = matrix(&["--self-test"]);
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "no_secret_in_output self-test: ok",
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
