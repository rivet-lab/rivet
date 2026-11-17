use std::path::{Component, Path, PathBuf};

use crate::config::DestinationConfig;
use crate::error::Result;

pub struct LocalDestination {
    base_path: String,
}

impl LocalDestination {
    pub fn new(config: &DestinationConfig) -> Result<Self> {
        let base_path = config
            .path
            .clone()
            .or_else(|| config.prefix.clone())
            .unwrap_or_else(|| ".".to_string());
        Ok(Self { base_path })
    }

    /// Join an attacker-influenceable `key` under `base_path` without letting it escape the destination root (CWE-22).
    fn safe_join(&self, key: &str) -> Result<PathBuf> {
        if key.contains('\0') {
            crate::rivet_bail!(
                crate::error::codes::CONFIG_PATH_UNSAFE,
                "destination key contains a NUL byte: {key:?}"
            );
        }
        let rel = Path::new(key);
        if rel.is_absolute() {
            crate::rivet_bail!(
                crate::error::codes::CONFIG_PATH_UNSAFE,
                "destination key must be relative, got absolute path: {key:?}"
            );
        }
        for component in rel.components() {
            match component {
                Component::Normal(_) | Component::CurDir => {}
                Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                    crate::rivet_bail!(
                        crate::error::codes::CONFIG_PATH_UNSAFE,
                        "destination key escapes the destination root: {key:?}"
                    );
                }
            }
        }
        Ok(Path::new(&self.base_path).join(rel))
    }
}

/// Copy `src` into the staging path `tmp`, creating it with `O_EXCL` so a pre-planted file or symlink at the temp name causes `EEXIST` rather than a silent follow that writes through the link (CWE-367 / symlink race).
fn stage_copy(src: &Path, tmp: &Path) -> Result<()> {
    let mut out = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(tmp)?;
    let mut input = std::fs::File::open(src)?;
    std::io::copy(&mut input, &mut out)?;
    Ok(())
}

/// The staging file name for one write of `file_name`.
fn staging_name(file_name: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    static STARTUP: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    let stamp = STARTUP.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0)
    });
    format!(
        ".{file_name}.{}-{stamp:x}.{}.tmp",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// Renames `src` to `dst`; a cross-device rename falls back to copy then remove, any other error is returned.
fn rename_or_copy(
    src: &Path,
    dst: &Path,
    rename: impl FnOnce(&Path, &Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    match rename(src, dst) {
        Ok(()) => Ok(()),
        Err(e) if e.raw_os_error() == Some(libc::EXDEV) => {
            std::fs::copy(src, dst)?;
            std::fs::remove_file(src)
        }
        Err(e) => Err(e),
    }
}

/// An idempotent delete: a key that is already gone counts as removed.
fn absent_is_removed(r: std::io::Result<()>) -> std::io::Result<()> {
    match r {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

impl super::Destination for LocalDestination {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<super::WriteOutcome> {
        let target = self.safe_join(remote_key)?;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file_name = target
            .file_name()
            .ok_or_else(|| {
                crate::error::coded(
                    crate::error::codes::INTERNAL_DEST_INVARIANT,
                    format!("destination key has no file name: {remote_key}"),
                )
            })?
            .to_string_lossy()
            .into_owned();
        let tmp = target.with_file_name(staging_name(&file_name));
        if let Err(e) = stage_copy(local_path, &tmp) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        crate::test_hook::maybe_block_at("before_commit_rename");
        if let Err(e) = std::fs::rename(&tmp, &target) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e.into());
        }
        log::info!("wrote {}", target.display());
        Ok(super::WriteOutcome::opaque())
    }

    fn capabilities(&self) -> super::DestinationCapabilities {
        super::DestinationCapabilities {
            commit_protocol: super::WriteCommitProtocol::Atomic,
            idempotent_overwrite: true,
            retry_safe: true,
            partial_write_risk: false,
        }
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<super::ObjectMeta>> {
        let root = self.safe_join(prefix)?;
        if !root.exists() {
            return Ok(Vec::new());
        }
        let base = Path::new(&self.base_path);
        let mut out = Vec::new();
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            let meta = std::fs::metadata(&dir)?;
            if meta.is_file() {
                let rel = dir
                    .strip_prefix(base)
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| dir.to_string_lossy().into_owned());
                out.push(super::ObjectMeta {
                    key: rel,
                    size_bytes: meta.len(),
                    content_md5: None,
                });
                continue;
            }
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                let path = entry.path();
                let ftype = entry.file_type()?;
                if ftype.is_dir() {
                    stack.push(path);
                } else if ftype.is_file() {
                    let m = entry.metadata()?;
                    let rel = path
                        .strip_prefix(base)
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_else(|_| path.to_string_lossy().into_owned());
                    out.push(super::ObjectMeta {
                        key: rel,
                        size_bytes: m.len(),
                        content_md5: None,
                    });
                }
            }
        }
        Ok(out)
    }

    fn read(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.safe_join(key)?;
        Ok(std::fs::read(path)?)
    }

    fn head(&self, key: &str) -> Result<Option<super::ObjectMeta>> {
        let path = self.safe_join(key)?;
        match std::fs::metadata(&path) {
            Ok(m) if m.is_file() => Ok(Some(super::ObjectMeta {
                key: key.to_string(),
                size_bytes: m.len(),
                content_md5: None,
            })),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn remove(&self, key: &str) -> Result<()> {
        Ok(absent_is_removed(std::fs::remove_file(
            self.safe_join(key)?,
        ))?)
    }

    fn r#move(&self, from: &str, to: &str) -> Result<()> {
        let src = self.safe_join(from)?;
        let dst = self.safe_join(to)?;
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(rename_or_copy(&src, &dst, |a, b| std::fs::rename(a, b))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DestinationType;
    use crate::destination::{Destination, WriteCommitProtocol};
    use std::io::Write;

    fn dest() -> LocalDestination {
        LocalDestination {
            base_path: ".".into(),
        }
    }

    /// ADR-0004: local temp-then-rename is Atomic.
    #[test]
    fn local_commit_protocol_is_atomic() {
        let caps = dest().capabilities();
        assert_eq!(caps.commit_protocol, WriteCommitProtocol::Atomic);
    }

    /// Writing the same key twice replaces the file; no orphaned artifact is left.
    #[test]
    fn local_idempotent_overwrite() {
        assert!(dest().capabilities().idempotent_overwrite);
    }

    /// Temp-then-rename means a killed write leaves nothing at the final key, so the local destination is retry-safe with no partial-write risk.
    #[test]
    fn local_retry_safe_no_partial_write_risk() {
        let caps = dest().capabilities();
        assert!(
            caps.retry_safe,
            "temp-then-rename is retry-safe (no artifact at the final key on failure)"
        );
        assert!(
            !caps.partial_write_risk,
            "rename is atomic on the same filesystem; no partial file at the final key"
        );
    }

    fn dest_at(base_path: &std::path::Path) -> LocalDestination {
        LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(base_path.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap()
    }

    fn source_file_with(bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(bytes).unwrap();
        f.flush().unwrap();
        f
    }

    /// Writing into a nested key must auto-create every missing parent directory.
    #[test]
    fn write_auto_creates_nested_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let src = source_file_with(b"hello\n");

        dest.write(src.path(), "a/b/c/payload.csv").unwrap();

        let target = dir.path().join("a/b/c/payload.csv");
        assert!(target.exists(), "nested target must exist");
        assert_eq!(std::fs::read(&target).unwrap(), b"hello\n");
    }

    /// Writing twice to the same key must produce a deterministic final state (second payload wins).
    #[test]
    fn writing_same_key_twice_replaces_content_deterministically() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());

        let first = source_file_with(b"first run\n");
        dest.write(first.path(), "data.csv").unwrap();

        let second = source_file_with(b"second run\n");
        dest.write(second.path(), "data.csv").unwrap();

        let target = dir.path().join("data.csv");
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"second run\n",
            "second write must replace the first; no stale content"
        );
    }

    /// Writing into a read-only directory must fail fast with an Err, not panic and not silently succeed.
    #[cfg(unix)]
    #[test]
    fn write_to_readonly_directory_returns_error_not_panic() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let readonly = dir.path().join("readonly");
        std::fs::create_dir(&readonly).unwrap();
        let mut perms = std::fs::metadata(&readonly).unwrap().permissions();
        perms.set_mode(0o500);
        std::fs::set_permissions(&readonly, perms).unwrap();

        let dest = dest_at(&readonly);
        let src = source_file_with(b"data");

        let result = dest.write(src.path(), "out.csv");

        let mut restore = std::fs::metadata(&readonly).unwrap().permissions();
        restore.set_mode(0o700);
        let _ = std::fs::set_permissions(&readonly, restore);

        assert!(
            result.is_err(),
            "writing into a read-only directory must return Err, not succeed"
        );
    }

    /// When the resolved destination path refers to an existing *file* (not a directory), `create_dir_all` on its parent succeeds but `fs::copy` then fails.
    #[test]
    fn write_when_base_path_points_at_a_file_returns_error() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let dest = dest_at(f.path());
        let src = source_file_with(b"data");

        let result = dest.write(src.path(), "nested/out.csv");
        assert!(
            result.is_err(),
            "writing under a file-typed base_path must fail cleanly"
        );
    }

    /// A missing source file is an Err, not a panic.
    #[test]
    fn write_with_nonexistent_source_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());

        let missing = dir.path().join("does_not_exist.csv");
        let result = dest.write(&missing, "out.csv");
        assert!(result.is_err(), "missing source must surface as Err");
    }

    /// The `remote_key` may contain characters that are legal on POSIX file systems but unusual (spaces, unicode).
    #[test]
    fn write_preserves_unusual_but_legal_key_characters() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let src = source_file_with(b"payload");

        let key = "with space/🚀 αρχείο.csv";
        dest.write(src.path(), key).unwrap();

        assert!(
            dir.path().join(key).exists(),
            "unicode-and-space key must be preserved verbatim"
        );
    }

    /// A successful write must leave exactly the final file.
    #[test]
    fn write_leaves_no_temp_file_behind_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let src = source_file_with(b"full payload\n");

        dest.write(src.path(), "nested/part-000001.parquet")
            .unwrap();

        let parent = dir.path().join("nested");
        let entries: Vec<_> = std::fs::read_dir(&parent)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            entries,
            vec!["part-000001.parquet".to_string()],
            "only the final file may remain; no .tmp sibling"
        );
        assert_eq!(
            std::fs::read(parent.join("part-000001.parquet")).unwrap(),
            b"full payload\n"
        );
    }

    /// A failed write must leave nothing at the final key.
    #[test]
    fn failed_write_leaves_no_file_at_final_key() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());

        let missing = dir.path().join("vanished-source.csv");
        let result = dest.write(&missing, "out/data.csv");
        assert!(result.is_err());

        let parent = dir.path().join("out");
        assert!(
            !parent.join("data.csv").exists(),
            "no file at the final key"
        );
        let leftovers: Vec<_> = std::fs::read_dir(&parent)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            leftovers.is_empty(),
            "no temp artifact may survive a failed write: {leftovers:?}"
        );
    }

    /// A stale temp left by a crashed previous run must not poison the next attempt.
    #[test]
    fn stale_temp_from_crashed_run_does_not_affect_the_next_write() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let stale = dir.path().join(".data.csv.tmp");
        std::fs::write(&stale, b"truncated garb").unwrap();

        let src = source_file_with(b"clean payload\n");
        dest.write(src.path(), "data.csv").unwrap();

        assert_eq!(
            std::fs::read(dir.path().join("data.csv")).unwrap(),
            b"clean payload\n",
            "the write must land whatever a crashed predecessor left behind"
        );
        let temps: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert_eq!(
            temps,
            vec![".data.csv.tmp".to_string()],
            "this write leaves no temp of its own, and the stale one is untouched \
             (dot-prefixed and .tmp-suffixed, so a FUTURE sweep could find it — \
             none exists today; see the doc above)"
        );
    }

    /// The staging name is unique per write, which is what makes concurrent writers into one prefix safe.
    #[test]
    fn two_writes_of_one_key_never_share_a_staging_name() {
        let a = staging_name("manifest.json");
        let b = staging_name("manifest.json");
        assert_ne!(
            a, b,
            "two stagings of the same key must not collide, or one writer unlinks the other's \
             file and the loser's rename fails with ENOENT — reported by the run as a non-fatal \
             manifest write failure while its data sits on the prefix unnamed by any manifest"
        );
        for n in [&a, &b] {
            assert!(
                n.starts_with(".manifest.json."),
                "still a recognisable sibling: {n}"
            );
            assert!(n.ends_with(".tmp"), "still sweepable: {n}");
        }
    }

    #[test]
    fn list_prefix_returns_files_with_relative_keys_and_sizes() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::write(dir.path().join("a.txt"), b"abc").unwrap();
        std::fs::create_dir_all(dir.path().join("nested/sub")).unwrap();
        std::fs::write(dir.path().join("nested/b.txt"), b"hello").unwrap();
        std::fs::write(dir.path().join("nested/sub/c.bin"), b"\0\0\0\0").unwrap();

        let mut listed = dest.list_prefix("").unwrap();
        listed.sort_by(|x, y| x.key.cmp(&y.key));
        let names: Vec<_> = listed.iter().map(|m| m.key.clone()).collect();
        assert_eq!(
            names,
            vec![
                "a.txt".to_string(),
                "nested/b.txt".to_string(),
                "nested/sub/c.bin".to_string(),
            ]
        );
        let sizes: Vec<_> = listed.iter().map(|m| m.size_bytes).collect();
        assert_eq!(sizes, vec![3u64, 5u64, 4u64]);
    }

    #[test]
    fn list_prefix_scopes_to_subdirectory() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::write(dir.path().join("top.txt"), b"x").unwrap();
        std::fs::create_dir_all(dir.path().join("only_me")).unwrap();
        std::fs::write(dir.path().join("only_me/a.parquet"), b"yy").unwrap();
        std::fs::write(dir.path().join("only_me/b.parquet"), b"zzz").unwrap();

        let listed = dest.list_prefix("only_me").unwrap();
        let names: std::collections::HashSet<_> = listed.iter().map(|m| m.key.clone()).collect();
        assert_eq!(
            names,
            ["only_me/a.parquet", "only_me/b.parquet"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
    }

    #[test]
    fn list_prefix_missing_returns_empty_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let listed = dest.list_prefix("does_not_exist").unwrap();
        assert!(listed.is_empty());
    }

    #[test]
    fn read_round_trips_bytes_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let payload: &[u8] = b"manifest body goes here\n";
        std::fs::write(dir.path().join("manifest.json"), payload).unwrap();
        let got = dest.read("manifest.json").unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn head_returns_some_for_existing_file_with_correct_size() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::write(dir.path().join("part-000001.parquet"), [42u8; 1234]).unwrap();
        let m = dest.head("part-000001.parquet").unwrap().unwrap();
        assert_eq!(m.key, "part-000001.parquet");
        assert_eq!(m.size_bytes, 1234);
    }

    #[test]
    fn remove_deletes_the_key_and_an_absent_key_is_ok() {
        use std::io::{Error, ErrorKind};
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::write(dir.path().join("_SUCCESS"), b"x").unwrap();
        dest.remove("_SUCCESS").unwrap();
        assert!(!dir.path().join("_SUCCESS").exists());
        dest.remove("_SUCCESS").unwrap();
        assert!(super::absent_is_removed(Err(Error::from(ErrorKind::PermissionDenied))).is_err());
    }

    #[test]
    fn head_returns_none_for_absent_file_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        assert!(dest.head("missing.txt").unwrap().is_none());
    }

    #[test]
    fn head_returns_none_for_directory_not_file() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::create_dir_all(dir.path().join("subdir")).unwrap();
        assert!(dest.head("subdir").unwrap().is_none());
    }

    #[test]
    fn read_returns_err_on_missing_key() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        assert!(dest.read("nope.json").is_err());
    }

    #[test]
    fn move_relocates_file_and_creates_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::write(dir.path().join("part-000001.parquet"), b"payload").unwrap();

        dest.r#move(
            "part-000001.parquet",
            "_quarantine/run_xyz/part-000001.parquet",
        )
        .unwrap();

        assert!(!dir.path().join("part-000001.parquet").exists());
        let body = std::fs::read(
            dir.path()
                .join("_quarantine")
                .join("run_xyz")
                .join("part-000001.parquet"),
        )
        .unwrap();
        assert_eq!(body, b"payload");
    }

    #[test]
    fn move_returns_err_on_missing_source() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let result = dest.r#move("absent.parquet", "_quarantine/r/absent.parquet");
        assert!(
            result.is_err(),
            "moving a non-existent file must surface as Err so the caller logs it"
        );
    }

    #[test]
    fn move_overwrites_existing_destination_object() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::write(dir.path().join("a"), b"new").unwrap();
        std::fs::create_dir_all(dir.path().join("_quarantine/r")).unwrap();
        std::fs::write(dir.path().join("_quarantine/r/a"), b"old").unwrap();

        dest.r#move("a", "_quarantine/r/a").unwrap();
        assert!(!dir.path().join("a").exists());
        let body = std::fs::read(dir.path().join("_quarantine/r/a")).unwrap();
        assert_eq!(body, b"new", "rename overwrites target");
    }

    /// A cross-device rename error falls back to copy then remove, leaving the bytes at the destination only.
    #[test]
    fn cross_device_rename_falls_back_to_copy_then_remove() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = (dir.path().join("src"), dir.path().join("dst"));
        std::fs::write(&src, b"payload").unwrap();

        rename_or_copy(&src, &dst, |_, _| {
            Err(std::io::Error::from_raw_os_error(libc::EXDEV))
        })
        .unwrap();

        assert_eq!(std::fs::read(&dst).unwrap(), b"payload");
        assert!(!src.exists(), "the source is removed after the copy");
    }

    /// A rename error other than a cross-device one is returned as it came, with no copy made.
    #[test]
    fn non_cross_device_rename_error_is_returned_without_a_copy() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = (dir.path().join("src"), dir.path().join("dst"));
        std::fs::write(&src, b"payload").unwrap();

        let err = rename_or_copy(&src, &dst, |_, _| {
            Err(std::io::Error::from_raw_os_error(libc::EACCES))
        })
        .unwrap_err();

        assert_eq!(err.raw_os_error(), Some(libc::EACCES));
        assert!(src.exists(), "the source is untouched");
        assert!(
            !dst.exists(),
            "no copy is made for a non cross-device error"
        );
    }

    /// SEC-RED V14: `remote_key` is joined onto `base_path` via `Path::join` with no traversal containment, so a key containing `../` (or an absolute path) escapes the destination directory and lets an attacker-influenced key write/move/read arbitrary files on the host.
    #[test]
    fn sec_local_write_rejects_traversal_key() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base");
        std::fs::create_dir_all(&base).unwrap();
        let dest = dest_at(&base);

        let escaped_rel = dir.path().join("escape.parquet");
        let src = source_file_with(b"pwned-relative");
        let result = dest.write(src.path(), "../escape.parquet");
        assert!(
            result.is_err(),
            "write must REFUSE a `../` traversal key (V14), got Ok"
        );
        assert!(
            !escaped_rel.exists(),
            "write must NOT create a file outside base_path: {}",
            escaped_rel.display()
        );

        let escaped_abs = dir.path().join("abs-escape.parquet");
        let abs_key = escaped_abs.to_string_lossy().into_owned();
        let src_abs = source_file_with(b"pwned-absolute");
        let result_abs = dest.write(src_abs.path(), &abs_key);
        assert!(
            result_abs.is_err(),
            "write must REFUSE an absolute key that escapes base_path (V14), got Ok"
        );
        assert!(
            !escaped_abs.exists(),
            "write must NOT create a file at an absolute escaped path: {}",
            escaped_abs.display()
        );

        let secret = dir.path().join("secret.txt");
        std::fs::write(&secret, b"top-secret").unwrap();
        let read_res = dest.read("../secret.txt");
        assert!(
            read_res.is_err(),
            "read must REFUSE a `../` traversal key (V14); leaking host files"
        );

        std::fs::write(base.join("inside.parquet"), b"payload").unwrap();
        let move_escaped = dir.path().join("moved-out.parquet");
        let move_res = dest.r#move("inside.parquet", "../moved-out.parquet");
        assert!(
            move_res.is_err(),
            "r#move must REFUSE a `../` traversal destination key (V14), got Ok"
        );
        assert!(
            !move_escaped.exists(),
            "r#move must NOT relocate a file outside base_path: {}",
            move_escaped.display()
        );
    }

    /// V14: a `..` buried mid-path (not just a leading one) and a NUL byte are equally refused.
    #[test]
    fn sec_local_rejects_buried_traversal_and_nul_keys() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base");
        std::fs::create_dir_all(&base).unwrap();
        let dest = dest_at(&base);
        let src = source_file_with(b"x");

        assert!(
            dest.write(src.path(), "ok/../../escape.parquet").is_err(),
            "a `..` component anywhere in the key must be refused"
        );
        assert!(
            dest.write(src.path(), "bad\0name.parquet").is_err(),
            "a NUL byte in the key must be refused"
        );
        dest.write(src.path(), "ver..1/data.parquet").unwrap();
        assert!(base.join("ver..1/data.parquet").exists());
    }

    /// V16 (CWE-367): the deterministic staging temp is created with `O_EXCL`.
    #[cfg(unix)]
    #[test]
    fn sec_write_does_not_follow_symlink_at_staging_temp() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());

        let victim = dir.path().join("victim.txt");
        std::fs::write(&victim, b"original").unwrap();

        let planted = dir.path().join(".data.csv.tmp");
        std::os::unix::fs::symlink(&victim, &planted).unwrap();

        let src = source_file_with(b"clean payload\n");
        dest.write(src.path(), "data.csv").unwrap();

        assert_eq!(
            std::fs::read(&victim).unwrap(),
            b"original",
            "no staged write may follow a planted symlink into the victim"
        );
        assert_eq!(
            std::fs::read(dir.path().join("data.csv")).unwrap(),
            b"clean payload\n",
            "the real key still gets the payload via a fresh regular temp"
        );
        assert!(
            dir.path()
                .join("data.csv")
                .symlink_metadata()
                .unwrap()
                .file_type()
                .is_file(),
            "the final key is a regular file, never a link"
        );
    }

    /// Pins the shipped OPT-6 temp-then-rename contract that ADR-0004 + semantics.md document.
    #[test]
    fn local_is_retry_safe_with_no_partial_write() {
        let caps = dest().capabilities();
        assert!(caps.retry_safe, "local temp-then-rename must be retry_safe");
        assert!(
            !caps.partial_write_risk,
            "local temp-then-rename must have no partial_write_risk"
        );
    }
}
