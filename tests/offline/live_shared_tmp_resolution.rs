//! The directory tests stage into is the one the running containers mount at /work, never a guess from the checkout.

use crate::stand_registry_guard::env::{WorkMount, resolve_shared_tmp};
use std::path::Path;

fn mounted(dir: &Path) -> WorkMount {
    Ok(Some(dir.display().to_string()))
}

fn checkout() -> &'static Path {
    Path::new("/nonexistent/worktree/tests/.live-tmp")
}

#[test]
fn the_mount_source_wins_over_the_checkout_the_test_runs_from() {
    let stand = tempfile::tempdir().unwrap();
    let seen = [
        ("rivet-duckdb", mounted(stand.path())),
        ("rivet-clickhouse", mounted(stand.path())),
    ];
    assert_eq!(resolve_shared_tmp(&seen, checkout()).unwrap(), stand.path());
}

#[test]
fn a_stand_without_clickhouse_still_resolves() {
    let stand = tempfile::tempdir().unwrap();
    let seen = [
        ("rivet-duckdb", mounted(stand.path())),
        ("rivet-clickhouse", Ok(None)),
    ];
    assert_eq!(resolve_shared_tmp(&seen, checkout()).unwrap(), stand.path());
}

#[test]
fn containers_mounting_two_directories_are_refused_naming_both() {
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let seen = [
        ("rivet-duckdb", mounted(a.path())),
        ("rivet-clickhouse", mounted(b.path())),
    ];
    let e = resolve_shared_tmp(&seen, checkout()).unwrap_err();
    assert!(
        e.contains(&a.path().display().to_string()) && e.contains(&b.path().display().to_string()),
        "{e}"
    );
}

#[test]
fn no_running_container_is_refused_naming_the_checkout_path() {
    let seen = [("rivet-duckdb", Ok(None)), ("rivet-clickhouse", Ok(None))];
    let e = resolve_shared_tmp(&seen, checkout()).unwrap_err();
    assert!(e.contains("/nonexistent/worktree/tests/.live-tmp"), "{e}");
}

#[test]
fn a_docker_that_cannot_answer_is_refused_not_guessed() {
    let seen = [
        (
            "rivet-duckdb",
            Err("Cannot connect to the Docker daemon".to_string()),
        ),
        ("rivet-clickhouse", Ok(None)),
    ];
    let e = resolve_shared_tmp(&seen, checkout()).unwrap_err();
    assert!(
        e.contains("Cannot connect") && e.contains("/nonexistent/worktree"),
        "{e}"
    );
}

#[test]
fn a_mount_this_process_cannot_write_is_refused_naming_both_paths() {
    let gone = Path::new("/nonexistent/stand/tests/.live-tmp");
    let seen = [("rivet-duckdb", mounted(gone))];
    let e = resolve_shared_tmp(&seen, checkout()).unwrap_err();
    assert!(
        e.contains("/nonexistent/stand/tests/.live-tmp")
            && e.contains("/nonexistent/worktree/tests/.live-tmp"),
        "{e}"
    );
}
