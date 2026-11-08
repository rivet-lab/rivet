//! A password written as a query pair of `RIVET_STATE_URL` reaches no output: the state the
//! `doctor` line names, the note `state copy` prints, and every command that fails on it.

use crate::common::*;

const SECRET: &str = "Qp7ZkStateSecretX9";

/// A login role with its own password and every right on the given scratch schemas; dropped on `Drop`.
struct StateRole(String);

impl StateRole {
    fn create(schemas: &[&ScratchStateSchema]) -> Self {
        let role = unique_name("rivet_sq");
        let mut admin = schemas[0].client();
        admin
            .batch_execute(&format!(
                "DROP ROLE IF EXISTS {role}; CREATE ROLE {role} LOGIN PASSWORD '{SECRET}'"
            ))
            .expect("create role");
        for schema in schemas {
            admin
                .batch_execute(&format!("GRANT ALL ON SCHEMA {} TO {role}", schema.name))
                .expect("grant");
        }
        Self(role)
    }

    /// A state URL whose password is a query pair, in the spelling `key`.
    fn url(&self, key: &str, schema: &ScratchStateSchema) -> String {
        format!(
            "postgresql://{}@127.0.0.1:5433/postgres?{key}={SECRET}&options=-csearch_path%3D{}",
            self.0, schema.name
        )
    }
}

impl Drop for StateRole {
    fn drop(&mut self) {
        let _ = pg_connect().batch_execute(&format!(
            "DROP OWNED BY {0}; DROP ROLE IF EXISTS {0}",
            self.0
        ));
    }
}

fn said(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// A state URL that answers, with the password a query pair, is named by `doctor` and `state copy` with the password masked, and a config `init` writes against it holds none.
#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn a_query_pair_password_in_the_state_url_reaches_no_output() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(20);
    let (probed, target) = (
        ScratchStateSchema::new("rivet_sq_probe"),
        ScratchStateSchema::new("rivet_sq_copy"),
    );
    let role = StateRole::create(&[&probed, &target]);
    let rig = Rig::pg_batch(table.name());
    rig.run_ok();

    let mut leaked: Vec<String> = Vec::new();
    for key in ["password", "sslpassword", "pass%77ord"] {
        let url = role.url(key, &probed);
        let mut outputs = vec![
            (
                "check",
                rig.cli_env(&["check"], &[("RIVET_STATE_URL", &url)]),
            ),
            (
                "doctor",
                rig.cli_env(&["doctor"], &[("RIVET_STATE_URL", &url)]),
            ),
            (
                "doctor --json",
                rig.cli_env(&["doctor", "--json"], &[("RIVET_STATE_URL", &url)]),
            ),
        ];
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("init.yaml");
        outputs.push((
            "init",
            run_rivet_env(
                &[
                    "init",
                    "--source",
                    POSTGRES_URL,
                    "--table",
                    table.name(),
                    "-o",
                    cfg.to_str().unwrap(),
                ],
                &[("RIVET_STATE_URL", &url)],
            ),
        ));
        for (name, out) in &outputs {
            if key == "password" && !out.status.success() {
                leaked.push(format!("{key}: `{name}` failed: {}", said(out)));
            }
            if said(out).contains(SECRET) {
                leaked.push(format!("{key}: {name}: {}", said(out)));
            }
        }
        if key == "password" {
            let doctor = said(&outputs[1].1);
            if !doctor.contains("[OK]  State (postgres ") || !doctor.contains("password=***") {
                leaked.push(format!(
                    "doctor does not name the state with the password masked: {doctor}"
                ));
            }
        }
    }

    let copy_url = role.url("password", &target);
    let copied = rig.cli(&["state", "copy", "--to", &copy_url]);
    assert!(copied.status.success(), "{}", said(&copied));
    assert!(
        said(&copied).contains("Run every command of this config with RIVET_STATE_URL="),
        "{}",
        said(&copied)
    );
    if said(&copied).contains(SECRET) {
        leaked.push(format!("state copy: {}", said(&copied)));
    }
    let again = rig.cli(&["state", "copy", "--to", &copy_url]);
    assert!(!again.status.success(), "{}", said(&again));
    if said(&again).contains(SECRET) {
        leaked.push(format!(
            "state copy again (target not empty): {}",
            said(&again)
        ));
    }
    assert_eq!(
        leaked,
        Vec::<String>::new(),
        "STATE URL QUERY PASSWORD LEAKED into {} output(s):\n  {}",
        leaked.len(),
        leaked.join("\n  ")
    );
}

/// The same URL at a state that does not answer: every command that names it fails without the password.
#[test]
#[ignore = "live: requires postgres"]
fn a_query_pair_password_in_a_dead_state_url_reaches_no_output() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5);
    let rig = Rig::pg_batch(table.name());
    rig.run_ok();
    let mut leaked: Vec<String> = Vec::new();
    for key in ["password", "PASSWORD", "sslpassword", "pwd", "pass%77ord"] {
        let url =
            format!("postgresql://rivet@127.0.0.1:1/rivet_state?sslmode=disable&{key}={SECRET}");
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("init.yaml");
        let outputs = [
            (
                "check",
                rig.cli_env(&["check"], &[("RIVET_STATE_URL", &url)]),
            ),
            (
                "doctor",
                rig.cli_env(&["doctor"], &[("RIVET_STATE_URL", &url)]),
            ),
            (
                "doctor --json",
                rig.cli_env(&["doctor", "--json"], &[("RIVET_STATE_URL", &url)]),
            ),
            (
                "init",
                run_rivet_env(
                    &[
                        "init",
                        "--source",
                        POSTGRES_URL,
                        "--table",
                        table.name(),
                        "-o",
                        cfg.to_str().unwrap(),
                    ],
                    &[("RIVET_STATE_URL", &url)],
                ),
            ),
            ("state copy", rig.cli(&["state", "copy", "--to", &url])),
        ];
        for (name, out) in &outputs {
            assert!(!said(out).trim().is_empty(), "{key}: `{name}` said nothing");
            if said(out).contains(SECRET) {
                leaked.push(format!("{key}: {name}"));
            }
        }
    }
    assert_eq!(
        leaked,
        Vec::<String>::new(),
        "STATE URL QUERY PASSWORD LEAKED into {} output(s):\n  {}",
        leaked.len(),
        leaked.join("\n  ")
    );
}

/// A source URL whose password is a query pair, at a server that answers: `check`, `doctor` and `plan` name the source without the password.
#[test]
#[ignore = "live: requires postgres"]
fn a_query_pair_password_in_the_source_url_reaches_no_output() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5);
    let role = unique_name("rivet_sq_src");
    pg_connect()
        .batch_execute(&format!(
            "DROP ROLE IF EXISTS {role}; CREATE ROLE {role} LOGIN PASSWORD '{SECRET}'; GRANT SELECT ON {} TO {role}",
            table.name()
        ))
        .expect("create source role");
    let dir = tempfile::tempdir().unwrap();
    let mut leaked: Vec<String> = Vec::new();
    for key in ["password", "PASSWORD", "pass%77ord"] {
        let cfg = dir.path().join("c.yaml");
        let url =
            format!("postgresql://{role}@127.0.0.1:5432/rivet?{key}={SECRET}&application_name=sq");
        std::fs::write(
            &cfg,
            format!(
                "source:\n  type: postgres\n  url: \"{url}\"\nexports:\n  - name: t\n    query: \"SELECT * FROM {}\"\n    mode: full\n    format: parquet\n    destination: {{ type: local, path: {} }}\n",
                table.name(),
                dir.path().join("out").display()
            ),
        )
        .unwrap();
        let c = cfg.to_str().unwrap();
        for (name, args) in [
            ("check", vec!["check", "-c", c]),
            ("doctor", vec!["doctor", "-c", c]),
            ("doctor --json", vec!["doctor", "-c", c, "--json"]),
            ("plan", vec!["plan", "-c", c, "--format", "json"]),
            ("run", vec!["run", "-c", c]),
        ] {
            let out = run_rivet_env(&args, &[]);
            if said(&out).contains(SECRET) {
                leaked.push(format!("{key}: {name}"));
            }
        }
    }
    let _ =
        pg_connect().batch_execute(&format!("DROP OWNED BY {role}; DROP ROLE IF EXISTS {role}"));
    assert_eq!(
        leaked,
        Vec::<String>::new(),
        "SOURCE URL QUERY PASSWORD LEAKED into {} output(s):\n  {}",
        leaked.len(),
        leaked.join("\n  ")
    );
}
