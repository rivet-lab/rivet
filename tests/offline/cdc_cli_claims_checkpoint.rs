//! `rivet cdc` claims its `--checkpoint` through the seam a `mode: cdc` run claims through (offline, dead source URLs).

use std::path::Path;
use std::process::Command;

const RIVET_BIN: &str = env!("CARGO_BIN_EXE_rivet");
const SHARED: &str = "RIVET_SOURCE_CDC_CHECKPOINT_SHARED";

/// One CDC engine: its source type, a dead URL, and the table spelling its config takes.
struct Engine {
    ty: &'static str,
    url: &'static str,
    table: &'static str,
    capture_instance: Option<&'static str>,
}

/// Every engine whose `rivet cdc` arm exists in this build.
fn engines() -> Vec<Engine> {
    #[cfg_attr(not(feature = "oracle"), allow(unused_mut))]
    let mut v = vec![
        Engine {
            ty: "mysql",
            url: "mysql://u@127.0.0.1:1/app",
            table: "app.orders",
            capture_instance: None,
        },
        Engine {
            ty: "postgres",
            url: "postgresql://u@127.0.0.1:1/app",
            table: "public.orders",
            capture_instance: None,
        },
        Engine {
            ty: "mssql",
            url: "sqlserver://u@127.0.0.1:1/app",
            table: "dbo.orders",
            capture_instance: Some("dbo_orders"),
        },
        Engine {
            ty: "mongo",
            url: "mongodb://u@127.0.0.1:1/app?serverSelectionTimeoutMS=500",
            table: "orders",
            capture_instance: None,
        },
    ];
    #[cfg(feature = "oracle")]
    v.push(Engine {
        ty: "oracle",
        url: "oracle://u:p@127.0.0.1:1/ORCL",
        table: "APP.ORDERS",
        capture_instance: None,
    });
    v
}

/// The `rivet` command with every inherited `RIVET_*` stripped.
fn rivet(dir: &Path, args: &[&str]) -> (i32, String) {
    let mut cmd = Command::new(RIVET_BIN);
    cmd.args(args).current_dir(dir);
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("RIVET_") {
            cmd.env_remove(&k);
        }
    }
    let out = cmd.output().expect("spawn rivet");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// A `mode: cdc` config in `dir/<file>` naming `ckpt` for export `export`.
fn write_config(dir: &Path, file: &str, e: &Engine, export: &str, ckpt: &str) {
    let tls = if e.ty == "mysql" {
        ""
    } else {
        "  tls: { mode: disable }\n"
    };
    let ci = e
        .capture_instance
        .map_or(String::new(), |c| format!("      capture_instance: {c}\n"));
    let yaml = format!(
        "source:\n  type: {}\n  url: \"{}\"\n{tls}exports:\n  - name: {export}\n    table: {}\n    mode: cdc\n    \
         format: parquet\n    cdc:\n      checkpoint: {ckpt}\n{ci}    destination: {{ type: local, path: ./out_{export} }}\n",
        e.ty, e.url, e.table
    );
    std::fs::write(dir.join(file), yaml).unwrap();
}

/// `rivet cdc --source <url> --checkpoint <ckpt> --table <table>`.
fn cli_cdc(dir: &Path, e: &Engine, ckpt: &str) -> (i32, String) {
    let mut args = vec![
        "cdc",
        "--source",
        e.url,
        "--checkpoint",
        ckpt,
        "--table",
        e.table,
    ];
    if let Some(ci) = e.capture_instance {
        args.extend(["--capture-instance", ci]);
    }
    rivet(dir, &args)
}

/// A checkpoint a `mode: cdc` export owns is refused to `rivet cdc` by code, before the source is dialled; the file lost does not change it.
#[test]
fn rivet_cdc_is_refused_a_checkpoint_an_export_owns_on_every_engine() {
    let mut bad = Vec::new();
    for e in engines() {
        let dir = tempfile::tempdir().unwrap();
        write_config(dir.path(), "a.yaml", &e, "orders_a", "./o.ckpt");
        let (_, ran) = rivet(dir.path(), &["run", "-c", "a.yaml"]);
        assert!(
            dir.path().join("o.ckpt.owner").exists(),
            "{}: the run claims its checkpoint:\n{ran}",
            e.ty
        );
        for lost in [false, true] {
            if lost {
                std::fs::write(dir.path().join("o.ckpt"), "{}").unwrap();
                std::fs::remove_file(dir.path().join("o.ckpt")).unwrap();
            }
            let (rc, err) = cli_cdc(dir.path(), &e, "./o.ckpt");
            if rc != 5 || !err.contains(&format!("[{SHARED}]")) || !err.contains("Nothing was read")
            {
                bad.push(format!(
                    "{} (checkpoint file lost: {lost}): exit {rc}: {}",
                    e.ty,
                    err.lines().next().unwrap_or("")
                ));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "`rivet cdc` must be refused an export's checkpoint by code:\n{}",
        bad.join("\n")
    );
}

/// The text of the neighbour refusal (an export reading an export's checkpoint) as `main` printed it.
fn export_to_export_text(ckpt: &str, a: &Path, b: &Path) -> String {
    format!(
        "cdc: checkpoint '{ckpt}' belongs to export 'orders_a' of config '{}', which still reads it; export 'orders_b' of config '{}' would continue from where that one stopped, and each stream would skip the changes the other read. Nothing was read. Give this export a `cdc.checkpoint` of its own (`rivet init -o` names it after the config file) and keep this file: it is the other export's position. Give it `cdc.initial: snapshot` if it has none, so its first run on the new checkpoint re-reads every table, and move any file it wrote before out of its destination first: those may miss the changes the other stream read. If this config replaces that one (an edited copy of it), delete or rename that config, or point its `cdc.checkpoint` elsewhere, and run this one again",
        a.display(),
        b.display()
    )
}

/// Unchanged: an export refused an export's checkpoint keeps the exact refusal it had.
#[test]
fn an_export_refused_an_exports_checkpoint_keeps_its_text() {
    let e = &engines()[0];
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    for (f, n) in [("a.yaml", "orders_a"), ("b.yaml", "orders_b")] {
        write_config(dir.path(), f, e, n, "./o.ckpt");
    }
    rivet(dir.path(), &["run", "-c", "a.yaml"]);
    let (rc, err) = rivet(dir.path(), &["run", "-c", "b.yaml"]);
    let want = export_to_export_text("./o.ckpt", &root.join("a.yaml"), &root.join("b.yaml"));
    assert!(
        rc == 5 && err.contains(&format!("[{SHARED}] {want}")),
        "exit {rc}:\n{err}"
    );
}

/// Unchanged: a checkpoint nobody owns, or a stale owner, or none at all, is read by `rivet cdc` as before.
#[test]
fn rivet_cdc_reads_an_unowned_checkpoint_and_claims_it_for_itself() {
    for e in engines() {
        let dir = tempfile::tempdir().unwrap();
        let owner = dir.path().join("o.ckpt.owner");
        for _ in 0..2 {
            let (rc, err) = cli_cdc(dir.path(), &e, "./o.ckpt");
            assert!(
                rc != 5 && !err.contains(SHARED),
                "{}: an unowned checkpoint is read (exit {rc}):\n{err}",
                e.ty
            );
        }
        let held = std::fs::read_to_string(&owner).expect("the command records itself");
        assert_eq!(held, r#"{"config":"","export":"rivet cdc"}"#, "{}", e.ty);
        std::fs::write(&owner, r#"{"config":"/nowhere/gone.yaml","export":"x"}"#).unwrap();
        let (rc, err) = cli_cdc(dir.path(), &e, "./o.ckpt");
        assert!(
            rc != 5 && !err.contains(SHARED),
            "{}: a stale owner is replaced (exit {rc}):\n{err}",
            e.ty
        );
        let mut no_ckpt = vec!["cdc", "--source", e.url, "--table", e.table];
        if let Some(ci) = e.capture_instance {
            no_ckpt.extend(["--capture-instance", ci]);
        }
        let other = tempfile::tempdir().unwrap();
        let (rc, err) = rivet(other.path(), &no_ckpt);
        assert!(
            rc != 5 && !err.contains(SHARED),
            "{}: no checkpoint, nothing to claim (exit {rc}):\n{err}",
            e.ty
        );
        assert_eq!(
            std::fs::read_dir(other.path()).unwrap().count(),
            0,
            "{}: no checkpoint, no record",
            e.ty
        );
    }
}

/// A checkpoint a `rivet cdc` command holds is refused to an export by code; deleting the record hands it over.
#[test]
fn an_export_is_refused_a_checkpoint_a_rivet_cdc_command_holds() {
    let mut bad = Vec::new();
    for e in engines() {
        let dir = tempfile::tempdir().unwrap();
        write_config(dir.path(), "a.yaml", &e, "orders_a", "./o.ckpt");
        cli_cdc(dir.path(), &e, "./o.ckpt");
        let (rc, err) = rivet(dir.path(), &["run", "-c", "a.yaml"]);
        if rc != 5
            || !err.contains(&format!("[{SHARED}]"))
            || !err.contains("belongs to a `rivet cdc` command")
        {
            bad.push(format!(
                "{}: exit {rc}: {}",
                e.ty,
                err.lines().next().unwrap_or("")
            ));
        }
        std::fs::remove_file(dir.path().join("o.ckpt.owner")).unwrap();
        let (rc, err) = rivet(dir.path(), &["run", "-c", "a.yaml"]);
        if rc == 5 || err.contains(SHARED) {
            bad.push(format!(
                "{}: the record deleted, the export still refused: {err}",
                e.ty
            ));
        }
    }
    assert_eq!(bad, Vec::<String>::new());
}
