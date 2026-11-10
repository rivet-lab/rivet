//! The no-silent-NULL property matrix grades its own grader offline; its live cells run through `make property-no-silent-null`.

use std::process::{Command, Output};

fn run(module: &str) -> Output {
    Command::new("python3")
        .args(["-m", module, "--self-test"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap_or_else(|e| panic!("run `python3 -m {module} --self-test`: {e}; python3 is a prerequisite, not a reason to skip"))
}

/// Every failure class fires on its exact harm and every `RivetType` variant has a sample family.
#[test]
fn the_no_silent_null_matrix_passes_its_own_self_test() {
    let out = run("dev.release_oracle.property_no_silent_null");
    assert_eq!(
        out.status.code(),
        Some(0),
        "no_silent_null fails its self-test:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The grants-CDC axis grades hidden, NULL and shape-differing columns.
#[test]
fn the_no_silent_null_grants_axis_passes_its_own_self_test() {
    let out = run("dev.release_oracle.property_no_silent_null_grants");
    assert_eq!(
        out.status.code(),
        Some(0),
        "no_silent_null_grants fails its self-test:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The change-stream axis reads its legs from the product and grades NULL, changed and missing change rows.
#[test]
fn the_no_silent_null_cdc_axis_passes_its_own_self_test() {
    let out = run("dev.release_oracle.property_no_silent_null_cdc");
    assert_eq!(
        out.status.code(),
        Some(0),
        "no_silent_null_cdc fails its self-test:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The load axis reads its targets from `LoadTarget` and grades a NULL read back from the warehouse.
#[test]
fn the_no_silent_null_load_axis_passes_its_own_self_test() {
    let out = run("dev.release_oracle.property_no_silent_null_load");
    assert_eq!(
        out.status.code(),
        Some(0),
        "no_silent_null_load fails its self-test:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
