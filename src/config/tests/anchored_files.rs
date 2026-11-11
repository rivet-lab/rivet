use super::*;

const SNOWFLAKE: &str = "load: { target: snowflake, account: acct, user: U, private_key_path: ./k.p8, warehouse: w, database: d, schema: s, storage_integration: si }";
const CLICKHOUSE: &str = "load: { target: clickhouse, url: \"https://h:8443\", database: d, user: u, password_env: PW, ca_file: ./ca.pem }";

fn config_with(dir: &std::path::Path, source_extra: &str, load: &str) -> String {
    let path = dir.join("rivet.yaml");
    std::fs::write(
        &path,
        format!(
            "source:\n  type: postgres\n{source_extra}\n{load}\nexports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n    destination: {{ type: gcs, bucket: b, prefix: p/ }}\n"
        ),
    )
    .unwrap();
    path.display().to_string()
}

fn parse(path: &str) -> Config {
    Config::from_yaml(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn beside(dir: &tempfile::TempDir, name: &str) -> String {
    super::super::destination::normalized(&dir.path().join(name))
        .display()
        .to_string()
}

/// A relative `url_file` names the file beside the config, whatever directory rivet starts from.
#[test]
fn a_relative_url_file_is_read_beside_the_config() {
    let conf = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    std::fs::write(conf.path().join("db.url"), "postgresql://beside/db").unwrap();
    std::fs::write(cwd.path().join("db.url"), "postgresql://under-cwd/db").unwrap();
    let path = config_with(conf.path(), "  url_file: ./db.url", "");
    let mut cfg = parse(&path);
    cfg.anchor_files(&path, Some(cwd.path().to_path_buf()))
        .unwrap();
    assert_eq!(cfg.source.url_file, Some(beside(&conf, "db.url")));
    assert_eq!(cfg.source.resolve_url().unwrap(), "postgresql://beside/db");
}

/// An absolute `url_file`, and a relative one run from the config's own directory, resolve as they did.
#[test]
fn an_absolute_url_file_and_a_run_from_the_config_directory_resolve_as_before() {
    let conf = tempfile::tempdir().unwrap();
    let abs = conf.path().join("abs.url");
    std::fs::write(&abs, "postgresql://abs/db").unwrap();
    let path = config_with(conf.path(), &format!("  url_file: {}", abs.display()), "");
    let mut cfg = parse(&path);
    cfg.anchor_files(&path, Some(conf.path().to_path_buf()))
        .unwrap();
    assert_eq!(cfg.source.url_file, Some(abs.display().to_string()));
    std::fs::write(conf.path().join("db.url"), "postgresql://beside/db").unwrap();
    let path = config_with(conf.path(), "  url_file: db.url", "");
    let mut cfg = parse(&path);
    cfg.anchor_files(&path, Some(conf.path().to_path_buf()))
        .unwrap();
    assert_eq!(cfg.source.url_file, Some(beside(&conf, "db.url")));
}

/// A relative file only the working directory holds is refused naming both places, for every path field the seam covers.
#[test]
fn a_file_only_the_working_directory_holds_is_refused_naming_both_places() {
    for (source_extra, load, file) in [
        ("  url_file: ./db.url", "", "db.url"),
        (
            "  url: \"postgresql://h/d\"\n  tls: { mode: verify-full, ca_file: ./ca.pem }",
            "",
            "ca.pem",
        ),
        ("  url: \"postgresql://h/d\"", SNOWFLAKE, "k.p8"),
        ("  url: \"postgresql://h/d\"", CLICKHOUSE, "ca.pem"),
    ] {
        let conf = tempfile::tempdir().unwrap();
        let cwd = tempfile::tempdir().unwrap();
        std::fs::write(cwd.path().join(file), "x").unwrap();
        let path = config_with(conf.path(), source_extra, load);
        let mut cfg = parse(&path);
        let err = cfg
            .anchor_files(&path, Some(cwd.path().to_path_buf()))
            .unwrap_err();
        crate::error::assert_coded(&err, "RIVET_CONFIG_SECRET_UNSET", 1);
        let shown = format!("{err:#}");
        assert!(
            shown.contains(&beside(&conf, file))
                && shown.contains(&cwd.path().join(file).display().to_string())
                && shown.contains("move the file to"),
            "{file}: {shown}"
        );
    }
}

/// Each remaining path field is anchored beside the config and read from there, a file under the working directory notwithstanding.
#[test]
fn every_path_field_is_anchored_beside_the_config() {
    let tls = "  url: \"postgresql://h/d\"\n  tls: { mode: verify-full, ca_file: ./ca.pem }";
    let conf = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    for file in ["ca.pem", "k.p8"] {
        std::fs::write(conf.path().join(file), "x").unwrap();
        std::fs::write(cwd.path().join(file), "y").unwrap();
    }
    let at = |src: &str, load: &str| {
        let path = config_with(conf.path(), src, load);
        let mut cfg = parse(&path);
        cfg.anchor_files(&path, Some(cwd.path().to_path_buf()))
            .unwrap();
        cfg
    };
    let cfg = at(tls, "");
    assert_eq!(
        cfg.source.tls.unwrap().ca_file,
        Some(beside(&conf, "ca.pem"))
    );
    let cfg = at("  url: \"postgresql://h/d\"", SNOWFLAKE);
    let Some(load::LoadSection {
        target: load::LoadTarget::Snowflake {
            private_key_path, ..
        },
        ..
    }) = cfg.load
    else {
        panic!("a snowflake load");
    };
    assert_eq!(private_key_path, beside(&conf, "k.p8"));
    let cfg = at("  url: \"postgresql://h/d\"", CLICKHOUSE);
    let Some(load::LoadSection {
        target: load::LoadTarget::Clickhouse { ca_file, .. },
        ..
    }) = cfg.load
    else {
        panic!("a clickhouse load");
    };
    assert_eq!(ca_file, Some(beside(&conf, "ca.pem")));
}

/// A relative path whose file neither place holds is anchored and left to the reader to refuse, naming the anchored place.
#[test]
fn a_relative_file_neither_place_holds_names_the_anchored_place() {
    let conf = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let path = config_with(conf.path(), "  url_file: ./absent.url", "");
    let mut cfg = parse(&path);
    cfg.anchor_files(&path, Some(cwd.path().to_path_buf()))
        .unwrap();
    let err = cfg.source.resolve_url().unwrap_err();
    crate::error::assert_coded(&err, "RIVET_CONFIG_SECRET_UNSET", 1);
    assert!(
        format!("{err:#}").contains(&beside(&conf, "absent.url")),
        "{err:#}"
    );
}
