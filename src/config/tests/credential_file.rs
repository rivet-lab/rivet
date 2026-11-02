use super::*;

fn config_beside(dir: &std::path::Path, credentials_file: &str) -> String {
    let path = dir.join("rivet.yaml");
    std::fs::write(
        &path,
        format!(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\nexports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n    destination:\n      type: gcs\n      bucket: b\n      credentials_file: {credentials_file}\n"
        ),
    )
    .unwrap();
    path.display().to_string()
}

/// A relative `credentials_file` is read beside the config, whatever directory rivet starts from.
#[test]
fn a_relative_credentials_file_is_read_beside_the_config() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("k.json"),
        br#"{"client_email":"a@b.c","private_key":"k"}"#,
    )
    .unwrap();
    let cfg = Config::load(&config_beside(dir.path(), "./k.json")).unwrap();
    let got = cfg.exports[0].destination.credentials_file.clone().unwrap();
    assert!(
        std::path::Path::new(&got).is_absolute() && got.ends_with("k.json"),
        "{got}"
    );
}

/// A named `credentials_file` that is no key stops the load by code, naming the field and the path.
#[test]
fn a_named_credentials_file_that_is_no_key_stops_the_load() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("empty.json"), b"").unwrap();
    std::fs::write(dir.path().join("broken.json"), b"{\"type\":").unwrap();
    std::fs::create_dir(dir.path().join("keydir")).unwrap();
    for name in ["empty.json", "broken.json", "keydir", "empty.json/k.json"] {
        let err = Config::load(&config_beside(dir.path(), &format!("./{name}"))).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_DEST_CREDENTIALS_INVALID", 1);
        let shown = format!("{err:#}");
        assert!(
            shown.contains("credentials_file") && shown.contains(name),
            "{name}: {shown}"
        );
    }
}

/// A key that only the working directory holds is refused with both places named, never read from there.
#[test]
fn a_key_only_the_working_directory_holds_is_refused_naming_both_places() {
    let conf = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    std::fs::write(
        cwd.path().join("k.json"),
        br#"{"client_email":"a@b.c","private_key":"k"}"#,
    )
    .unwrap();
    let path = config_beside(conf.path(), "./k.json");
    let mut cfg = Config::from_yaml(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let err = cfg
        .anchor_credential_files(&path, Some(cwd.path().to_path_buf()))
        .unwrap_err();
    crate::error::assert_coded(&err, "RIVET_CONFIG_SECRET_UNSET", 1);
    let shown = format!("{err:#}");
    assert!(
        shown.contains(&conf.path().join("k.json").display().to_string())
            && shown.contains(&cwd.path().join("k.json").display().to_string()),
        "{shown}"
    );
}

/// An absolute `credentials_file` is read where it is written, and a usable key passes unchanged.
#[test]
fn an_absolute_credentials_file_is_kept_as_written() {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("k.json");
    std::fs::write(&key, br#"{"client_email":"a@b.c","private_key":"k"}"#).unwrap();
    let cfg = Config::load(&config_beside(dir.path(), &key.display().to_string())).unwrap();
    assert_eq!(
        cfg.exports[0].destination.credentials_file.as_deref(),
        key.to_str()
    );
}
